/**
 * shohyo-links — MF仕訳用 証憑リンク集
 * UI: views/index.html (自己完結)。API: /api/links CRUD ({ ok, result } / { ok, error } 形式)。
 * 認証は server.js の requireAppAccess('shohyo-links') に委譲。
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { listLinks, createLink, updateLink, deleteLink } from './db.js';
import {
  mfConfigured, authorizeUrl, exchangeCode, loadTokens, clearTokens,
  currentOffice, getJournals, postVoucher, matchVendors, revokeTokens, journalDigest,
  getConnectedAccounts, getTransactions, getJournalsByTransactionIds, missingScopes, mfErrorText,
} from './mf-api.js';
import {
  addToInbox, getInbox, listInbox, countByStatus, readFile, updateInboxMeta, setMatch, setStatus,
  listAttachLog, autoAttachEnabled, setSetting, getSetting, decodeBase64Strict, INBOX_STATUSES, TX_ID_MAX, transactionOwners,
} from './inbox.js';
import { runInboxMatch, attachWithClaim } from './attach-job.js';
import { parseVoucherFileName, isValidDate, matchVoucher } from './matcher.js';
import { extractEnabled } from './extract.js';
import { ingestVoucher, applyExtraction } from './ingest.js';
import { runGdriveInbox, gdriveInboxEnabled, gdriveInboxUrl } from './gdrive-inbox.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + query);
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

router.get('/api/links', (req, res) => {
  try {
    res.json({ ok: true, result: listLinks() });
  } catch (e) {
    console.error('[shohyo-links] list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.post('/api/links', (req, res) => {
  try {
    res.json({ ok: true, result: createLink(req.body || {}) });
  } catch (e) {
    if (e.message === 'name_required') return res.status(400).json({ ok: false, error: 'name_required' });
    console.error('[shohyo-links] create', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.patch('/api/links/:id', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'bad_id' });
  try {
    const row = updateLink(id, req.body || {});
    if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: row });
  } catch (e) {
    if (e.message === 'name_required') return res.status(400).json({ ok: false, error: 'name_required' });
    console.error('[shohyo-links] update', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.delete('/api/links/:id', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'bad_id' });
  try {
    if (!deleteLink(id)) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: { deleted: id } });
  } catch (e) {
    console.error('[shohyo-links] delete', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ---- MF会計API連携 (Phase 2) ----

router.get('/mf', (req, res) => {
  // 末尾スラッシュ (/mf/) だと画面内の相対fetch (api/mf/...) が 1階層ずれるため /mf に寄せる
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) {
    return res.redirect(308, `${req.baseUrl}/mf` + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  }
  res.sendFile(path.join(__dirname, 'views', 'mf.html'));
});

// OAuth開始: MFの認可画面へリダイレクト
router.get('/mf/connect', (req, res) => {
  if (!mfConfigured()) return res.status(500).send('MF_CLIENT_ID / MF_CLIENT_SECRET が未設定です (Render環境変数)');
  const state = crypto.randomBytes(16).toString('hex');
  req.session.mfOauthState = state;
  res.redirect(authorizeUrl(state));
});

// OAuthコールバック
router.get('/mf/callback', async (req, res) => {
  // 相対リダイレクト ('mf?...') は /apps/shohyo-links/mf/callback を基準に解決され /mf/mf になるため絶対パスで返す
  const backToMf = (q) => res.redirect(`${req.baseUrl}/mf${q}`);
  try {
    const { code, state, error } = req.query;
    if (error) return backToMf('?error=' + encodeURIComponent(String(error)));
    if (!code || !state || state !== req.session.mfOauthState) {
      return backToMf('?error=state_mismatch');
    }
    delete req.session.mfOauthState;
    await exchangeCode(String(code));
    backToMf('?connected=1');
  } catch (e) {
    console.error('[shohyo-links] mf callback', e.message, e.detail || '');
    backToMf('?error=' + encodeURIComponent(e.message));
  }
});

router.get('/api/mf/status', async (req, res) => {
  try {
    if (!mfConfigured()) return res.json({ ok: true, result: { configured: false, connected: false } });
    if (!loadTokens()) return res.json({ ok: true, result: { configured: true, connected: false } });
    let office = null;
    try { office = await currentOffice(); } catch (e) {
      if (e.message === 'mf_not_connected' || String(e.message).includes('invalid_grant')) {
        return res.json({ ok: true, result: { configured: true, connected: false, reason: 'token_expired' } });
      }
      throw e;
    }
    res.json({ ok: true, result: { configured: true, connected: true, office, missing_scopes: missingScopes() } });
  } catch (e) {
    console.error('[shohyo-links] mf status', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/mf/disconnect', async (req, res) => {
  // MF側でも失効させてから手元のトークンを消す (残しておくと第三者が使える状態が続く)
  const revoke = await revokeTokens();
  clearTokens();
  res.json({ ok: true, result: { disconnected: true, ...revoke } });
});

// 期間内の仕訳を取得し、証憑未添付を支払い先マスタと突合して返す
router.get('/api/mf/unattached', async (req, res) => {
  try {
    const { start, end, all } = req.query;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(start)) || !/^\d{4}-\d{2}-\d{2}$/.test(String(end))) {
      return res.status(400).json({ ok: false, error: 'bad_period' });
    }
    const vendors = listLinks();
    const journals = await getJournals(String(start), String(end));
    const rows = journals
      .filter(j => all === '1' || !(j.voucher_file_ids || []).length)
      .map(j => ({
        id: j.id,
        number: j.number,
        date: j.transaction_date,
        memo: j.memo || '',
        vouchers: (j.voucher_file_ids || []).length,
        ...journalDigest(j),
        vendors: matchVendors(j, vendors).map(v => ({
          id: v.id, name: v.name, url: v.url, storage_path: v.storage_path, fetch_method: v.fetch_method,
        })),
      }));
    res.json({ ok: true, result: { total: journals.length, rows } });
  } catch (e) {
    console.error('[shohyo-links] mf unattached', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message, detail: mfErrorText(e) });
  }
});

// MFの「連携サービスから入力」と同じ単位 = 連携サービス (カード/口座) ごとの明細。
// 経理の実作業はこの画面で起きているため、仕訳ではなく明細を主役にする。
router.get('/mf/transactions', (req, res) => {
  // 画面内の相対パスは '../' 基準 (末尾スラッシュが付くと階層がずれるので /mf/transactions に寄せる)
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) {
    return res.redirect(308, `${req.baseUrl}/mf/transactions` + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  }
  res.sendFile(path.join(__dirname, 'views', 'mf-transactions.html'));
});

// MFが明細取得と一緒に証憑も自動取得してくれる連携サービス (公式一覧 2025-12)。ここは人が取りに行かない
const AUTO_VOUCHER_SERVICES = ['UPSIDER', 'PRESIDENT CARD', 'Amazon.co.jp', 'Amazonビジネス', 'MISUMI', 'Yahoo!ショッピング', '楽天市場', 'ビジネスカード'];
const isAutoVoucher = (name) => AUTO_VOUCHER_SERVICES.some(k => String(name || '').includes(k));

// 期間内の明細を連携サービスごとにまとめて返す。仕訳済みの明細には仕訳ID・証憑数を付ける
router.get('/api/mf/transactions', async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(start)) || !/^\d{4}-\d{2}-\d{2}$/.test(String(end))) {
      return res.status(400).json({ ok: false, error: 'bad_period' });
    }
    const vendors = listLinks();
    // 連携サービス名は connected_account.read が要る。旧スコープで接続したままだと403 → 名前なしで続行し再接続を促す
    let accountsWarning = null;
    const [accounts, txs] = await Promise.all([
      getConnectedAccounts().catch((e) => {
        if (e.status !== 403) throw e;
        accountsWarning = 'reconnect_for_scope';
        return [];
      }),
      getTransactions(String(start), String(end)),
    ]);

    // 仕訳済み明細 → 仕訳 (証憑は仕訳に付くので、添付有無は仕訳側が正)
    const registered = txs.filter(t => t.journalizing_status !== 'none' && t.journalizing_status !== 'excluded');
    const journals = registered.length ? await getJournalsByTransactionIds(registered.map(t => t.id), String(start), String(end)) : [];
    const journalByTx = new Map();
    for (const j of journals) if (j.transaction_id) journalByTx.set(j.transaction_id, j);

    // 受け箱の状態を明細に重ねる (この画面だけで「証憑が揃っているか」が分かるように)
    const inboxByTx = new Map();
    for (const v of listInbox({ limit: 2000 })) {
      const tag = { id: v.id, status: v.status, file_name: v.file_name };
      if (v.match_tx_id && ['waiting_registration', 'proposed', 'attaching', 'attached', 'needs_check'].includes(v.status)) {
        inboxByTx.set(v.match_tx_id, tag);
      } else if (v.status === 'ambiguous') {
        for (const c of v.candidates || []) if (!inboxByTx.has(c.tx_id)) inboxByTx.set(c.tx_id, { ...tag, candidate: true });
      }
    }

    const byAccount = new Map();
    for (const a of accounts) {
      byAccount.set(a.id, { id: a.id, name: a.name, auto_voucher: isAutoVoucher(a.name), mf_url: getSetting(mfUrlKey_(a.id), ''), rows: [] });
    }
    for (const t of txs) {
      if (!byAccount.has(t.connected_account_id)) {
        byAccount.set(t.connected_account_id, { id: t.connected_account_id, name: `連携サービス ${byAccount.size + 1}`, auto_voucher: false, rows: [] });
      }
      const j = journalByTx.get(t.id) || null;
      const voucherCount = j ? (j.voucher_file_ids || []).length : (t.voucher_file_ids || []).length;
      byAccount.get(t.connected_account_id).rows.push({
        id: t.id,
        date: t.date,
        value: t.value,
        side: t.side,
        content: t.content || '',
        memo: t.memo || '',
        status: t.journalizing_status,
        journal_id: j ? j.id : null,
        journal_number: j ? j.number : null,
        accounts: j ? journalDigest(j).accounts : [],
        vouchers: voucherCount,
        inbox: inboxByTx.get(t.id) || null,
        vendors: matchVendors({ content: t.content, memo: t.memo }, vendors).map(v => ({
          id: v.id, name: v.name, url: v.url, storage_path: v.storage_path, fetch_method: v.fetch_method,
        })),
      });
    }
    const result = [...byAccount.values()].map(a => {
      const expense = a.rows.filter(r => r.side === 'EXPENSE' && r.status !== 'excluded');
      return {
        ...a,
        counts: {
          total: a.rows.length,
          unregistered: a.rows.filter(r => r.status === 'none').length,
          need_voucher: expense.filter(r => r.status !== 'none' && !r.vouchers).length,
          attached: expense.filter(r => r.vouchers > 0).length,
          // 受け箱に証憑が来ていて、MFで「登録」すれば貼れる明細
          ready: a.rows.filter(r => r.status === 'none' && r.inbox && !r.inbox.candidate && r.inbox.status !== 'attached').length,
          // 支出でまだ証憑が無い (受け箱にも無い) 明細 = 取りに行く対象。MFが証憑を自動取得するサービスは除く
          missing: a.auto_voucher ? 0 : expense.filter(r => !r.vouchers && !r.inbox).length,
        },
      };
    }).sort((x, y) => y.counts.unregistered - x.counts.unregistered || y.counts.need_voucher - x.counts.need_voucher || y.counts.total - x.counts.total);
    res.json({ ok: true, result: { accounts: result, total: txs.length, warning: accountsWarning } });
  } catch (e) {
    console.error('[shohyo-links] mf transactions', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message, detail: mfErrorText(e) });
  }
});

// ---- 証憑の受け箱 (Phase 3 ③④: 突合 + 提案/自動添付) ----

router.get('/mf/inbox', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) return res.redirect(308, `${req.baseUrl}/mf/inbox` + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'mf-inbox.html'));
});

const inboxId_ = (v) => { const n = Number(v); return Number.isSafeInteger(n) && n > 0 ? n : null; };
const actorOf_ = (req) => req.session?.user?.email || req.session?.email || req.session?.user?.name || 'user';
// 自動添付のON/OFF (お金に関わる設定) は admin だけ
function requireAdmin_(req, res, next) {
  if (req.session?.role !== 'admin') return res.status(403).json({ ok: false, error: 'admin_only' });
  next();
}

router.get('/api/inbox', (req, res) => {
  try {
    const status = req.query.status ? String(req.query.status) : null;
    if (status && !INBOX_STATUSES.includes(status)) return res.status(400).json({ ok: false, error: 'bad_status' });
    res.json({ ok: true, result: {
      rows: listInbox({ status }), counts: countByStatus(), auto_attach: autoAttachEnabled(), is_admin: req.session?.role === 'admin',
      ai_enabled: extractEnabled(), gdrive: { enabled: gdriveInboxEnabled(), url: gdriveInboxUrl() },
    } });
  } catch (e) {
    console.error('[shohyo-links] inbox list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// 受け箱に入れる (画面のアップロード / ロボット / メール転送)。JSON+base64。
// 種別は中身 (magic bytes) で決め、申告された mime/拡張子は使わない。ファイル名規約から日付・金額・支払先を補完
router.post('/api/inbox', async (req, res) => {
  try {
    const { file_name, file_data, source, vendor_id, vendor_name, doc_date, amount, note } = req.body || {};
    if (typeof file_name !== 'string' || !file_name.trim() || !file_data) return res.status(400).json({ ok: false, error: 'file_required' });
    if (file_name.length > 255) return res.status(400).json({ ok: false, error: 'file_name_too_long' });
    if (typeof file_data !== 'string' || decodedSize_(file_data) > 5 * 1024 * 1024) return res.status(413).json({ ok: false, error: 'file_too_large_5mb' });
    const buffer = decodeBase64Strict(file_data);
    if (!buffer) return res.status(400).json({ ok: false, error: 'bad_base64' });
    if (doc_date !== undefined && doc_date !== '' && !isValidDate(doc_date)) return res.status(400).json({ ok: false, error: 'bad_date' });
    // 保存 → 日付・金額・支払先が揃っていなければ中身を読む (PDFはルール→AI、画像はAI)。読めなくてもアップロードは成功
    const { row, duplicate, extracted } = await ingestVoucher(buffer, { file_name, source, vendor_id, vendor_name, doc_date, amount, note });
    res.json({ ok: true, result: { row, duplicate, extracted } });
  } catch (e) {
    if (e.message === 'unsupported_file') return res.status(415).json({ ok: false, error: 'unsupported_file' });
    if (['bad_amount', 'bad_vendor_id', 'bad_date'].includes(e.message)) return res.status(400).json({ ok: false, error: e.message });
    console.error('[shohyo-links] inbox add', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 読み取りをやり直す (直し忘れ・AI鍵を後から入れた等)。空の項目だけ埋める
router.post('/api/inbox/:id/extract', async (req, res) => {
  const id = inboxId_(req.params.id);
  const row = id ? getInbox(id) : null;
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (['attached', 'attaching', 'needs_check'].includes(row.status)) return res.status(409).json({ ok: false, error: 'already_attached' });
  try {
    const extracted = await applyExtraction(row, readFile(row));
    res.json({ ok: true, result: { row: getInbox(id), extracted, ai_enabled: extractEnabled() } });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/inbox/log', (req, res) => {
  res.json({ ok: true, result: listAttachLog(200) });
});

router.get('/api/inbox/:id/file', (req, res) => {
  const id = inboxId_(req.params.id);
  const row = id ? getInbox(id) : null;
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  try {
    const buf = readFile(row);
    res.setHeader('Content-Type', row.mime || 'application/octet-stream');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Content-Disposition', "inline; filename*=UTF-8''" + encodeURIComponent(row.file_name));
    res.send(buf);
  } catch (e) {
    console.error('[shohyo-links] inbox file', id, e.message);
    res.status(500).json({ ok: false, error: e.message === 'file_corrupted' ? 'file_corrupted' : 'file_missing' });
  }
});

router.patch('/api/inbox/:id', (req, res) => {
  const id = inboxId_(req.params.id);
  if (!id) return res.status(400).json({ ok: false, error: 'bad_id' });
  try {
    const row = updateInboxMeta(id, req.body || {});
    if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: row });
  } catch (e) {
    const code = { already_attached: 409, bad_date: 400, bad_amount: 400, bad_vendor_id: 400 }[e.message] || 500;
    res.status(code).json({ ok: false, error: e.message });
  }
});

// 人の操作: 提案を承認して貼る / 候補 (tx_id) を指定して貼る。
// 貼れる相手は「サーバーが突合で出した候補」だけ。任意の仕訳IDは受け付けない (Codexレビュー High-5)。
// 添付直前にMFから明細と仕訳を引き直し、金額・日付・支払先が今も合うこと・他の証憑に取られていないこと・
// 仕訳に既に証憑が無いことを再検証する (古い候補に貼らない・Codex 2巡目 #4)。証憑ありは force=true (人の確認) で許可
router.post('/api/inbox/:id/attach', async (req, res) => {
  const id = inboxId_(req.params.id);
  const row = id ? getInbox(id) : null;
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (['attached', 'attaching', 'needs_check'].includes(row.status)) return res.status(409).json({ ok: false, error: 'already_attached' });
  try {
    const bodyTx = req.body?.tx_id;
    if (bodyTx !== undefined && (typeof bodyTx !== 'string' || bodyTx.length > TX_ID_MAX)) return res.status(400).json({ ok: false, error: 'bad_tx_id' });
    const txId = bodyTx || row.match_tx_id;
    if (!txId) return res.status(400).json({ ok: false, error: 'target_required' });
    const candidates = row.candidates || [];
    const cand = candidates.find(c => c.tx_id === txId) || (row.match_tx_id === txId ? { tx_id: txId, date: row.doc_date } : null);
    if (!cand) return res.status(400).json({ ok: false, error: 'not_a_candidate' });
    const owners = transactionOwners();
    if ([...(owners.get(txId) || [])].some(o => o !== row.id)) return res.status(409).json({ ok: false, error: 'taken_by_other' });

    // MFから引き直す (候補の取引日 ±10日)
    const anchor = isValidDate(cand.date) ? cand.date : (isValidDate(row.doc_date) ? row.doc_date : new Date().toISOString().slice(0, 10));
    const from = shiftDate_(anchor, -10), to = shiftDate_(anchor, 10);
    const tx = (await getTransactions(from, to)).find(t => t.id === txId);
    if (!tx) return res.status(409).json({ ok: false, error: 'transaction_gone' });
    const recheck = matchVoucher(row, [tx], listLinks());
    if (recheck.kind !== 'unique') return res.status(409).json({ ok: false, error: 'candidate_stale', reason: recheck.reason });
    const journals = await getJournalsByTransactionIds([txId], from, to);
    const j = journals.find(x => x.transaction_id === txId);
    if (!j) return res.status(409).json({ ok: false, error: 'journal_not_registered' });
    if ((j.voucher_file_ids || []).length > 0 && req.body?.force !== true) {
      return res.status(409).json({ ok: false, error: 'journal_has_voucher', vouchers: j.voucher_file_ids.length });
    }
    // 既に証憑がある仕訳へ人の確認で追加した場合は、後から分かるよう台帳に残す
    const forced = req.body?.force === true && (j.voucher_file_ids || []).length > 0;
    const r = await attachWithClaim(row, j, {
      tx_id: txId, mode: 'manual', actor: actorOf_(req),
      reason: `${bodyTx ? 'picked' : 'approved'}${forced ? '+force(証憑ありの仕訳へ追加)' : ''}`,
    });
    if (!r.ok) {
      if (r.error === 'claim_failed') return res.status(409).json({ ok: false, error: 'already_attached' });
      if (r.error === 'needs_check') return res.status(502).json({ ok: false, error: 'needs_check' });
      return res.status(r.error === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: r.error });
    }
    res.json({ ok: true, result: getInbox(id) });
  } catch (e) {
    console.error('[shohyo-links] inbox attach', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message, detail: mfErrorText(e) });
  }
});

router.post('/api/inbox/:id/exclude', (req, res) => {
  const id = inboxId_(req.params.id);
  const row = id ? getInbox(id) : null;
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (row.status === 'attached' || row.status === 'attaching') return res.status(409).json({ ok: false, error: 'already_attached' });
  res.json({ ok: true, result: setStatus(row.id, 'excluded') });
});

// 戻す: 除外 / 要確認 (MFで未添付だったと人が確認した) を再照合の対象に戻す
router.post('/api/inbox/:id/reopen', (req, res) => {
  const id = inboxId_(req.params.id);
  const row = id ? getInbox(id) : null;
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (row.status === 'attached' || row.status === 'attaching') return res.status(409).json({ ok: false, error: 'already_attached' });
  res.json({ ok: true, result: setStatus(row.id, 'new') });
});

// 今すぐ照合 (cron を待たない)。先にGドライブの受け箱を拾ってから突合。attach は設定 (自動添付ON) に従う
router.post('/api/inbox/run', async (req, res) => {
  try {
    let gdrive = null;
    if (gdriveInboxEnabled()) {
      try { gdrive = await runGdriveInbox(); } catch (e) { gdrive = { ok: false, errors: [String(e.message).slice(0, 120)] }; }
    }
    const r = await runInboxMatch({ attach: true, actor: actorOf_(req) });
    r.gdrive = gdrive;
    if (r.error) return res.status(r.error === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: r.error, result: r });
    res.json({ ok: true, result: r });
  } catch (e) {
    console.error('[shohyo-links] inbox run', e.message, e.detail || '');
    // MFのエラー本文も返す (画面のトーストに出す。mf_api_400 だけでは原因が分からない)
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message, detail: mfErrorText(e) });
  }
});

router.post('/api/inbox/settings', requireAdmin_, (req, res) => {
  const v = req.body?.auto_attach;
  if (v !== true && v !== false) return res.status(400).json({ ok: false, error: 'bad_value' });
  setSetting('auto_attach', v ? '1' : '0');
  console.log(`[shohyo-links] auto_attach -> ${v ? 'ON' : 'OFF'} by ${actorOf_(req)}`);
  res.json({ ok: true, result: { auto_attach: v } });
});

function shiftDate_(ymd, days) {
  const d = new Date(ymd + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

// MF「連携サービスから入力 (通帳・カード他)」の、カードで絞ったURLを連携サービスごとに覚える。
// URL中の search_form[asset_acts][account_id_hash] はMF画面専用のハッシュで、APIの連携サービスIDから作れない。
// なので人が1回だけ「MFでカードを選んで検索したURL」を貼る (2026-08-28 中原さん)
const MF_TJ_PREFIX = 'https://accounting.moneyforward.com/transaction_journals';
const mfUrlKey_ = (accountId) => `mf_url:${String(accountId).slice(0, 200)}`;

router.post('/api/mf/accounts/:id/url', (req, res) => {
  const accountId = String(req.params.id || '');
  if (!accountId || accountId.length > 200) return res.status(400).json({ ok: false, error: 'bad_id' });
  const url = String(req.body?.url || '').trim();
  if (url === '') { setSetting(mfUrlKey_(accountId), ''); return res.json({ ok: true, result: { url: '' } }); }
  if (!url.startsWith(MF_TJ_PREFIX) || url.length > 1000 || /[\s<>"']/.test(url)) {
    return res.status(400).json({ ok: false, error: 'bad_url' });
  }
  setSetting(mfUrlKey_(accountId), url);
  res.json({ ok: true, result: { url } });
});

// 証憑ファイルを仕訳に添付 (journal_id 無しならBoxへ未添付保存)
router.post('/api/mf/attach', async (req, res) => {
  try {
    const { journal_id, file_name, file_data } = req.body || {};
    if (!file_name || !file_data) return res.status(400).json({ ok: false, error: 'file_required' });
    // MF証憑API仕様 (developers /specs/vouchers): 1件5MB・名称255文字・1仕訳あたり5件まで
    if (String(file_name).length > 255) return res.status(400).json({ ok: false, error: 'file_name_too_long' });
    if (decodedSize_(String(file_data)) > 5 * 1024 * 1024) return res.status(413).json({ ok: false, error: 'file_too_large_5mb' });
    const result = await postVoucher(journal_id || null, String(file_name), String(file_data));
    res.json({ ok: true, result });
  } catch (e) {
    console.error('[shohyo-links] mf attach', e.message, e.detail || '');
    const status = e.message === 'mf_not_connected' ? 401 : (e.status === 413 ? 413 : 500);
    res.status(status).json({ ok: false, error: e.message });
  }
});

/** base64文字列から元ファイルのバイト数を求める (パディング考慮) */
function decodedSize_(b64) {
  const s = b64.replace(/=+$/, '');
  return Math.floor(s.length * 3 / 4);
}

export default router;
