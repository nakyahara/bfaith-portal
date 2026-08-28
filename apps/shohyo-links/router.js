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
  getConnectedAccounts, getTransactions, getJournalsByTransactionIds, missingScopes,
} from './mf-api.js';
import {
  addToInbox, getInbox, listInbox, countByStatus, readFile, updateInboxMeta, setMatch, markAttached, setStatus,
  listAttachLog, autoAttachEnabled, setSetting,
} from './inbox.js';
import { runInboxMatch } from './attach-job.js';
import { parseVoucherFileName } from './matcher.js';

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
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message });
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

    const byAccount = new Map();
    for (const a of accounts) {
      byAccount.set(a.id, { id: a.id, name: a.name, auto_voucher: isAutoVoucher(a.name), rows: [] });
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
        },
      };
    }).sort((x, y) => y.counts.unregistered - x.counts.unregistered || y.counts.need_voucher - x.counts.need_voucher || y.counts.total - x.counts.total);
    res.json({ ok: true, result: { accounts: result, total: txs.length, warning: accountsWarning } });
  } catch (e) {
    console.error('[shohyo-links] mf transactions', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message });
  }
});

// ---- 証憑の受け箱 (Phase 3 ③④: 突合 + 提案/自動添付) ----

router.get('/mf/inbox', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) return res.redirect(308, `${req.baseUrl}/mf/inbox` + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'mf-inbox.html'));
});

router.get('/api/inbox', (req, res) => {
  try {
    const status = req.query.status ? String(req.query.status) : null;
    res.json({ ok: true, result: { rows: listInbox({ status }), counts: countByStatus(), auto_attach: autoAttachEnabled() } });
  } catch (e) {
    console.error('[shohyo-links] inbox list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// 受け箱に入れる (画面のアップロード / ロボット / メール転送)。JSON+base64。ファイル名規約から日付・金額・支払先を補完
router.post('/api/inbox', (req, res) => {
  try {
    const { file_name, file_data, mime, source, vendor_id, vendor_name, doc_date, amount, note } = req.body || {};
    if (!file_name || !file_data) return res.status(400).json({ ok: false, error: 'file_required' });
    if (String(file_name).length > 255) return res.status(400).json({ ok: false, error: 'file_name_too_long' });
    if (decodedSize_(String(file_data)) > 5 * 1024 * 1024) return res.status(413).json({ ok: false, error: 'file_too_large_5mb' });
    const parsed = parseVoucherFileName(file_name) || {};
    const { row, duplicate } = addToInbox(Buffer.from(String(file_data), 'base64'), {
      file_name, mime, source: source || 'upload', vendor_id,
      vendor_name: vendor_name ?? parsed.vendor_name, doc_date: doc_date ?? parsed.doc_date, amount: amount ?? parsed.amount, note,
    });
    res.json({ ok: true, result: { row, duplicate } });
  } catch (e) {
    console.error('[shohyo-links] inbox add', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/inbox/:id/file', (req, res) => {
  const row = getInbox(Number(req.params.id));
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  try {
    res.setHeader('Content-Type', row.mime || 'application/octet-stream');
    res.setHeader('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(row.file_name)}`);
    res.send(readFile(row));
  } catch (e) {
    res.status(500).json({ ok: false, error: 'file_missing' });
  }
});

router.patch('/api/inbox/:id', (req, res) => {
  try {
    const row = updateInboxMeta(Number(req.params.id), req.body || {});
    if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: row });
  } catch (e) {
    res.status(e.message === 'already_attached' ? 409 : 500).json({ ok: false, error: e.message });
  }
});

// 人の操作: 提案を承認して貼る / 候補 (tx_id) を指定して貼る / 任意の仕訳IDに貼る
router.post('/api/inbox/:id/attach', async (req, res) => {
  const id = Number(req.params.id);
  const row = getInbox(id);
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (row.status === 'attached') return res.status(409).json({ ok: false, error: 'already_attached' });
  try {
    let journalId = String(req.body?.journal_id || row.match_journal_id || '');
    let journalNumber = req.body?.journal_number ?? row.match_journal_number ?? null;
    let txId = String(req.body?.tx_id || row.match_tx_id || '');
    if (!journalId) {
      // 候補の明細から仕訳を引く (登録済みでないと貼れない)
      if (!txId) return res.status(400).json({ ok: false, error: 'target_required' });
      const [j] = await getJournalsByTransactionIds([txId], row.doc_date ? shiftDate_(row.doc_date, -10) : shiftDate_(new Date().toISOString().slice(0, 10), -60), shiftDate_(new Date().toISOString().slice(0, 10), 1));
      if (!j) return res.status(409).json({ ok: false, error: 'journal_not_registered' });
      journalId = j.id; journalNumber = j.number;
    }
    const r = await postVoucher(journalId, row.file_name, readFile(row).toString('base64'));
    const fileId = r?.voucher_file_ids?.[0]?.file_id || '';
    const actor = req.session?.user?.email || req.session?.user?.name || 'user';
    const out = markAttached(id, { journal_id: journalId, journal_number: journalNumber, tx_id: txId, mf_file_id: fileId, mode: 'manual', actor, reason: req.body?.reason || 'approved' });
    res.json({ ok: true, result: out });
  } catch (e) {
    console.error('[shohyo-links] inbox attach', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message });
  }
});

router.post('/api/inbox/:id/exclude', (req, res) => {
  const row = getInbox(Number(req.params.id));
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (row.status === 'attached') return res.status(409).json({ ok: false, error: 'already_attached' });
  res.json({ ok: true, result: setStatus(row.id, 'excluded') });
});

router.post('/api/inbox/:id/reopen', (req, res) => {
  const row = getInbox(Number(req.params.id));
  if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
  if (row.status === 'attached') return res.status(409).json({ ok: false, error: 'already_attached' });
  setMatch(row.id, { status: 'new', candidates: [] });
  res.json({ ok: true, result: getInbox(row.id) });
});

// 今すぐ照合 (cron を待たない)。attach は設定 (自動添付ON) に従う
router.post('/api/inbox/run', async (req, res) => {
  try {
    const r = await runInboxMatch({ attach: true, actor: 'manual' });
    res.status(r.error === 'mf_not_connected' ? 401 : 200).json({ ok: r.ok || Boolean(r.error), result: r, ...(r.error ? { error: r.error } : {}) });
  } catch (e) {
    console.error('[shohyo-links] inbox run', e.message, e.detail || '');
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/inbox/log', (req, res) => {
  res.json({ ok: true, result: listAttachLog(200) });
});

router.post('/api/inbox/settings', (req, res) => {
  const on = req.body?.auto_attach === true || req.body?.auto_attach === '1' || req.body?.auto_attach === 1;
  setSetting('auto_attach', on ? '1' : '0');
  console.log(`[shohyo-links] auto_attach -> ${on ? 'ON' : 'OFF'} by ${req.session?.user?.email || 'user'}`);
  res.json({ ok: true, result: { auto_attach: on } });
});

function shiftDate_(ymd, days) {
  const d = new Date(ymd + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

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
