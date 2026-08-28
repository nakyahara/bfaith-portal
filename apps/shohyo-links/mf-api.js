/**
 * shohyo-links — MFクラウド会計APIクライアント (Phase 2)
 * OAuth 2.0 (authorization code / BASIC) + 仕訳・証憑・明細のラッパー。
 * 仕様: https://developers.api-accounting.moneyforward.com/ (openapi v3)
 * トークンは DATA_DIR/shohyo-links.db の mf_tokens に保存 (共有ドライブには置かない)。
 */
import { getShohyoDB } from './db.js';

const AUTH_BASE = 'https://api.biz.moneyforward.com';
const API_BASE = 'https://api-accounting.moneyforward.com';
export const MF_SCOPES = [
  'mfc/accounting/offices.read',
  'mfc/accounting/journal.read',
  'mfc/accounting/voucher.write',
  'mfc/accounting/transaction.read',
  'mfc/accounting/trade_partners.read',
].join(' ');

function clientConfig() {
  const id = process.env.MF_CLIENT_ID || '';
  const secret = process.env.MF_CLIENT_SECRET || '';
  const redirect = process.env.MF_REDIRECT_URI || 'https://bfaith-portal.onrender.com/apps/shohyo-links/mf/callback';
  return { id, secret, redirect, configured: Boolean(id && secret) };
}

export function mfConfigured() {
  return clientConfig().configured;
}

// ---- トークン永続化 (単一事業者前提の1行テーブル) ----

function ensureTokenTable() {
  const db = getShohyoDB();
  db.exec(`CREATE TABLE IF NOT EXISTS mf_tokens (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  return db;
}

export function loadTokens() {
  const row = ensureTokenTable().prepare('SELECT payload FROM mf_tokens WHERE id = 1').get();
  return row ? JSON.parse(row.payload) : null;
}

export function saveTokens(tokens) {
  ensureTokenTable().prepare(`INSERT INTO mf_tokens (id, payload, updated_at) VALUES (1, @payload, @updated_at)
    ON CONFLICT(id) DO UPDATE SET payload = @payload, updated_at = @updated_at`)
    .run({ payload: JSON.stringify(tokens), updated_at: new Date().toISOString() });
}

export function clearTokens() {
  ensureTokenTable().prepare('DELETE FROM mf_tokens WHERE id = 1').run();
}

// ---- OAuth ----

export function authorizeUrl(state) {
  const { id, redirect } = clientConfig();
  const q = new URLSearchParams({
    client_id: id, redirect_uri: redirect, response_type: 'code', scope: MF_SCOPES, state,
  });
  return `${AUTH_BASE}/authorize?${q}`;
}

async function tokenRequest(params) {
  const { id, secret } = clientConfig();
  const res = await fetch(`${AUTH_BASE}/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: 'Basic ' + Buffer.from(`${id}:${secret}`).toString('base64'),
    },
    body: new URLSearchParams(params).toString(),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`mf_token_${body.error || res.status}`);
    err.detail = body;
    throw err;
  }
  return body;
}

export async function exchangeCode(code) {
  const { redirect } = clientConfig();
  const body = await tokenRequest({ grant_type: 'authorization_code', code, redirect_uri: redirect });
  const tokens = normalizeTokens(body);
  saveTokens(tokens);
  return tokens;
}

function normalizeTokens(body) {
  return {
    access_token: body.access_token,
    refresh_token: body.refresh_token,
    scope: body.scope,
    expires_at: Date.now() + (Number(body.expires_in) || 3600) * 1000,
  };
}

// MFは refresh のたびに refresh_token をローテーションするため、同時にrefreshすると
// 片方のトークンが無効化される。プロセス内で1本に束ねる (Renderは単一プロセス)。
let refreshInFlight = null;

async function ensureAccessToken() {
  const tokens = loadTokens();
  if (!tokens) throw new Error('mf_not_connected');
  if (Date.now() < tokens.expires_at - 60_000) return tokens.access_token;
  if (!refreshInFlight) {
    refreshInFlight = (async () => {
      const body = await tokenRequest({ grant_type: 'refresh_token', refresh_token: tokens.refresh_token });
      const next = normalizeTokens(body);
      if (!next.refresh_token) next.refresh_token = tokens.refresh_token; // refresh_tokenが返らない実装への保険
      saveTokens(next);
      return next.access_token;
    })().finally(() => { refreshInFlight = null; });
  }
  return refreshInFlight;
}

/** 接続解除時にMF側でもトークンを失効させる (RFC 7009 /revoke)。失敗しても解除は続行する */
export async function revokeTokens() {
  const tokens = loadTokens();
  if (!tokens?.refresh_token) return { revoked: false, reason: 'no_token' };
  const { id, secret } = clientConfig();
  try {
    const res = await fetch(`${AUTH_BASE}/revoke`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: 'Basic ' + Buffer.from(`${id}:${secret}`).toString('base64'),
      },
      body: new URLSearchParams({ token: tokens.refresh_token, token_type_hint: 'refresh_token' }).toString(),
    });
    return { revoked: res.ok, status: res.status };
  } catch (e) {
    console.error('[shohyo-links] mf revoke', e.message);
    return { revoked: false, reason: e.message };
  }
}

// ---- APIラッパー ----

// MFのRate Limiter: 1アクセストークンあたり 3リクエスト/秒 (developers /rate_limiter)。
// 呼び出しを直列化し最低350ms間隔を空ける + 429は Retry-After に従って最大2回リトライする。
const MIN_INTERVAL_MS = 350;
let lastCallAt = 0;
let queue = Promise.resolve();

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function throttled(fn) {
  const run = queue.then(async () => {
    const wait = lastCallAt + MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await sleep(wait);
    lastCallAt = Date.now();
    return fn();
  });
  queue = run.catch(() => {}); // 失敗しても後続の待ち行列は止めない
  return run;
}

async function apiFetch(path, { method = 'GET', body } = {}) {
  for (let attempt = 0; ; attempt++) {
    const token = await ensureAccessToken();
    const res = await throttled(() => fetch(`${API_BASE}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(body ? { 'Content-Type': 'application/json' } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    }));
    if (res.status === 429 && attempt < 2) {
      const retryAfter = Number(res.headers.get('retry-after'));
      await sleep(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1));
      continue;
    }
    if (res.status === 204) return null;
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      const err = new Error(`mf_api_${res.status}`);
      err.detail = json;
      err.status = res.status;
      throw err;
    }
    return json;
  }
}

export async function currentOffice() {
  return apiFetch('/api/v3/offices');
}

/** 期間内の仕訳を全ページ取得 */
export async function getJournals(startDate, endDate) {
  const journals = [];
  const perPage = 500;
  for (let page = 1; page <= 40; page++) {
    const res = await apiFetch(`/api/v3/journals?start_date=${startDate}&end_date=${endDate}&page=${page}&per_page=${perPage}`);
    const items = res?.journals || [];
    journals.push(...items);
    if (items.length < perPage) break;
  }
  return journals;
}

// MFが返すIDは URLエンコード済み ('%2B' 等)。URLSearchParams が再エンコードするので一度戻す
const rawId = (id) => { try { return decodeURIComponent(String(id)); } catch { return String(id); } };

/** 連携サービス (カード・銀行口座) の一覧 */
export async function getConnectedAccounts() {
  const res = await apiFetch('/api/v3/connected_accounts');
  return res?.connected_accounts || [];
}

/**
 * 期間内の明細 (連携サービスから入力の1行) を全ページ取得。
 * statuses = journalizing_status の配列 (none=未仕訳 / registered=仕訳済み ...)。省略で全件
 */
export async function getTransactions(startDate, endDate, { statuses = null, accountId = null } = {}) {
  const out = [];
  const perPage = 500;
  for (let page = 1; page <= 40; page++) {
    const q = new URLSearchParams({ start_date: startDate, end_date: endDate, page: String(page), per_page: String(perPage), order: 'asc' });
    if (accountId) q.set('connected_account_id', rawId(accountId));
    for (const st of statuses || []) q.append('journalizing_statuses', st);
    const res = await apiFetch(`/api/v3/transactions?${q}`);
    const items = res?.transactions || [];
    out.push(...items);
    if (items.length < perPage) break;
  }
  return out;
}

/** 明細IDに紐づく仕訳を引く (transaction_ids は最大50件/回) */
export async function getJournalsByTransactionIds(ids, startDate, endDate) {
  const out = [];
  for (let i = 0; i < ids.length; i += 50) {
    const q = new URLSearchParams({ start_date: startDate, end_date: endDate, per_page: '500' });
    for (const id of ids.slice(i, i + 50)) q.append('transaction_ids', rawId(id));
    const res = await apiFetch(`/api/v3/journals?${q}`);
    out.push(...(res?.journals || []));
  }
  return out;
}

/** 証憑を保存する。journalId が null なら未添付のままBoxに保存される */
export async function postVoucher(journalId, fileName, fileDataBase64) {
  return apiFetch('/api/v3/vouchers', {
    method: 'POST',
    body: { journal_id: journalId, voucher_files: [{ file_name: fileName, file_data: fileDataBase64 }] },
  });
}

// ---- 支払い先マスタとの突合 ----

/** 全角→半角・カナ正規化・大文字化・空白類除去 (NFKCで大部分をカバー) */
export function normalizeText(s) {
  return String(s || '').normalize('NFKC').toUpperCase().replace(/[\s　・･]/g, '');
}

// 単語トークンとして一般的すぎて突合キーにしない語
const KEY_STOPWORDS = new Set(['MONTHLY', 'CHARGE', 'SUBSCR', 'SUBSCRIPTION', 'ONLINE', 'JAPAN', 'TOKYO', 'CLOUD', 'SERVICE', 'STORE']);

/** 支払い先名から突合キーを作る: 全体 + 括弧の内外セグメント + 単語トークン */
export function vendorKeys(name) {
  const keys = new Set();
  // 丸数字プレフィックスはNFKCで数字化される前に除去する
  const raw = String(name || '').replace(/^[①-⑳㉑-㉟㊱-㊿]/, '');
  const norm = normalizeText(raw);
  if (norm.length >= 3) keys.add(norm);
  // 括弧区切りのセグメント (正規化後は空白が消えているため括弧のみで分割)
  for (const seg of norm.split(/[（）()【】\[\]]/)) {
    if (seg.length >= 3) keys.add(seg);
  }
  // 空白区切りの単語トークン (FONDESK MONTHLY CHARGE → FONDESK)。汎用語と短語は除外
  for (const tok of raw.split(/[（）()【】\[\]\s　・･]/)) {
    const t = normalizeText(tok);
    if (t.length >= 5 && !KEY_STOPWORDS.has(t) && !/^\d+$/.test(t)) keys.add(t);
  }
  return [...keys];
}

/**
 * 仕訳を表示・絞り込み用に要約する (スキーマ差異に耐えるベストエフォート)
 * amount=借方合計 / accounts=勘定科目 / partners=取引先 / remarks=摘要
 * 勘定科目を分けて返すのは、画面側で「証憑が要らない仕訳 (売上計上・棚卸など)」を
 * 勘定科目チップで畳めるようにするため。
 */
export function journalDigest(j) {
  const accounts = [];
  const partners = [];
  const remarks = [];
  let amount = 0;
  const push = (arr, v) => { if (typeof v === 'string' && v.trim() && !arr.includes(v)) arr.push(v); };
  const side = (s) => {
    if (!s || typeof s !== 'object') return;
    push(accounts, s.account_name);
    push(accounts, s.sub_account_name);
    push(partners, s.trade_partner_name);
  };
  for (const b of Array.isArray(j.branches) ? j.branches : []) {
    side(b.debitor);
    side(b.creditor);
    push(remarks, b.remark);
    const v = Number(b.debitor?.value);
    if (Number.isFinite(v)) amount += v;
  }
  if (!accounts.length && !partners.length) {
    // branches が想定と違う形で来た場合の保険 (再帰でそれらしいキーを拾う)
    const walk = (v) => {
      if (Array.isArray(v)) return v.forEach(walk);
      if (!v || typeof v !== 'object') return;
      side(v);
      if (!amount && typeof v.value === 'number') amount = v.value;
      Object.values(v).forEach(w => { if (w && typeof w === 'object') walk(w); });
    };
    walk(j);
  }
  return { amount, accounts, partners, remarks };
}

/** 仕訳オブジェクトから突合対象の文字列 (摘要・取引先名・content等) を再帰収集 */
export function journalTexts(journal) {
  const out = [];
  const walk = (v) => {
    if (typeof v === 'string') { if (v.length >= 2 && !/^[\d\-:TZ.%=+/]+$/.test(v)) out.push(v); }
    else if (Array.isArray(v)) v.forEach(walk);
    else if (v && typeof v === 'object') {
      for (const [k, val] of Object.entries(v)) {
        if (/name|memo|content|remark|description/i.test(k)) walk(val);
        else if (typeof val !== 'string') walk(val);
      }
    }
  };
  walk(journal);
  return out;
}

/** 仕訳と支払い先マスタを部分一致で突合。マッチした vendor の配列を返す */
export function matchVendors(journal, vendors) {
  const text = normalizeText(journalTexts(journal).join('|'));
  if (!text) return [];
  return vendors.filter(v => (v._keys || (v._keys = vendorKeys(v.name))).some(k => text.includes(k)));
}
