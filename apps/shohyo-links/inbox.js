/**
 * shohyo-links — 証憑の受け箱 (voucher_inbox)
 *
 * 「証憑はここに入れれば、あとはロボットがMFの仕訳に貼る」の台帳。
 * ファイル実体は DATA_DIR/shohyo-vouchers/<sha256>.<ext> (正本はMF側のBox。ここは貼るまでの待機所)。
 * DBには絶対パスを持たず sha256 と拡張子から毎回組み立てる (DB内のパスを信用しない・Codexレビュー #8)。
 *
 * status の遷移:
 *   new → (突合) → proposed            … 一意に決まった (提案モードでは人が承認、自動モードでは即添付)
 *                → waiting_registration … 相手の明細がまだ未仕訳 (登録されたら次の周期で貼る)
 *                → ambiguous / no_match … 人が候補から選ぶ
 *                → attaching            … 添付の確保中 (リース。POST前に必ずここを通る。明細IDごとに1件だけ)
 *                → attached             … MFに貼れた (mf_file_id を持つ)
 *                → needs_check          … POST後に結果が確定できなかった (MF側に貼れている可能性)。**自動で再送しない**。
 *                                         人がMF画面を見て「戻す」(未添付だった) か「除外」(貼れていた) を選ぶ
 *   人の操作: excluded (貼らない) / 候補から選んで attach / 戻す
 *
 * 排他的な所有 (同じ明細に2つの証憑を貼らない) は attaching/attached だけ。proposed/waiting は毎周期
 * まとめて再評価するので、同じ明細を指す証憑が複数あれば全部 ambiguous になる (先着順にしない・Codex 2巡目 #3)
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { getShohyoDB } from './db.js';
import { isValidDate } from './matcher.js';

export const INBOX_STATUSES = ['new', 'proposed', 'waiting_registration', 'ambiguous', 'no_match', 'attaching', 'attached', 'needs_check', 'excluded', 'error'];
// 毎周期の突合対象 (needs_check は含めない = 自動再送しない)
const OPEN_STATUSES = ['new', 'proposed', 'waiting_registration', 'ambiguous', 'no_match', 'error'];
// 添付を確保できる状態
const CLAIMABLE = ['new', 'proposed', 'waiting_registration', 'ambiguous', 'no_match', 'error'];
const MIME_BY_EXT = { pdf: 'application/pdf', jpg: 'image/jpeg', png: 'image/png' };
const AMOUNT_MAX = 1_000_000_000;
export const TX_ID_MAX = 200;

let ready = false;
function db() {
  const d = getShohyoDB();
  if (!ready) {
    d.exec(`CREATE TABLE IF NOT EXISTS voucher_inbox (
      id                   INTEGER PRIMARY KEY AUTOINCREMENT,
      sha256               TEXT NOT NULL UNIQUE,
      file_name            TEXT NOT NULL,
      mime                 TEXT NOT NULL DEFAULT '',
      size                 INTEGER NOT NULL DEFAULT 0,
      ext                  TEXT NOT NULL DEFAULT 'pdf',
      source               TEXT NOT NULL DEFAULT 'upload',
      vendor_id            INTEGER,
      vendor_name          TEXT NOT NULL DEFAULT '',
      doc_date             TEXT NOT NULL DEFAULT '',
      amount               INTEGER,
      note                 TEXT NOT NULL DEFAULT '',
      status               TEXT NOT NULL DEFAULT 'new',
      match_tx_id          TEXT NOT NULL DEFAULT '',
      match_journal_id     TEXT NOT NULL DEFAULT '',
      match_journal_number INTEGER,
      match_strength       TEXT NOT NULL DEFAULT '',
      match_reason         TEXT NOT NULL DEFAULT '',
      candidates_json      TEXT NOT NULL DEFAULT '[]',
      lease_token          TEXT NOT NULL DEFAULT '',
      lease_prev_status    TEXT NOT NULL DEFAULT '',
      mf_file_id           TEXT NOT NULL DEFAULT '',
      attached_at          TEXT NOT NULL DEFAULT '',
      last_checked_at      TEXT NOT NULL DEFAULT '',
      error                TEXT NOT NULL DEFAULT '',
      created_at           TEXT NOT NULL,
      updated_at           TEXT NOT NULL
    )`);
    d.exec('CREATE INDEX IF NOT EXISTS idx_voucher_inbox_status ON voucher_inbox(status)');
    // 読み取りの経緯 (なぜ読めた/読めなかったか)。既存DBにも冪等に足す
    if (!d.prepare('PRAGMA table_info(voucher_inbox)').all().some(c => c.name === 'extract_note')) {
      d.exec("ALTER TABLE voucher_inbox ADD COLUMN extract_note TEXT NOT NULL DEFAULT ''");
    }
    // 相手の仕訳に既に付いている証憑の数。0より大きい行に貼ると二重添付になるので、画面で警告しボタンを変える
    if (!d.prepare('PRAGMA table_info(voucher_inbox)').all().some(c => c.name === 'match_journal_vouchers')) {
      d.exec('ALTER TABLE voucher_inbox ADD COLUMN match_journal_vouchers INTEGER NOT NULL DEFAULT 0');
    }
    // #1044 以前の行は reason 文字列に「(仕訳に証憑あり)」を連結していた。新カラムへ移して
    // 「照合し直すまで警告が出ない」状態を無くす (該当行が無くなれば何もしないので冪等)
    backfillJournalVouchers(d);
    // 明細ごとに 確保中/添付済み は1件だけ (二重POSTをDBで止める。確保の時点で効く)
    d.exec('DROP INDEX IF EXISTS uq_voucher_inbox_attached_tx');
    d.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_voucher_inbox_owned_tx ON voucher_inbox(match_tx_id)
      WHERE status IN ('attaching', 'attached') AND match_tx_id != ''`);
    d.exec(`CREATE TABLE IF NOT EXISTS voucher_attach_log (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      inbox_id       INTEGER NOT NULL,
      journal_id     TEXT NOT NULL,
      journal_number INTEGER,
      tx_id          TEXT NOT NULL DEFAULT '',
      mf_file_id     TEXT NOT NULL DEFAULT '',
      mode           TEXT NOT NULL,
      actor          TEXT NOT NULL DEFAULT '',
      reason         TEXT NOT NULL DEFAULT '',
      created_at     TEXT NOT NULL
    )`);
    d.exec(`CREATE TABLE IF NOT EXISTS shohyo_settings (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )`);
    ready = true;
  }
  return d;
}

export function storageDir() {
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const dir = path.join(dataDir, 'shohyo-vouchers');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

/** 実体の中身で種別を決める (申告された mime/拡張子は信用しない)。対応外は null */
export function sniffKind(buffer) {
  if (buffer.length >= 5 && buffer.subarray(0, 5).toString('latin1') === '%PDF-') return { ext: 'pdf', mime: 'application/pdf' };
  if (buffer.length >= 3 && buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) return { ext: 'jpg', mime: 'image/jpeg' };
  if (buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]))) return { ext: 'png', mime: 'image/png' };
  return null;
}

/** base64 を厳密に復号する (壊れた/空の文字列は null) */
export function decodeBase64Strict(s) {
  const str = String(s || '').replace(/\s+/g, '');
  if (!str || str.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(str)) return null;
  const buf = Buffer.from(str, 'base64');
  return buf.length ? buf : null;
}

// ---- 設定 (自動添付ON/OFF = 提案モード) ----

export function getSetting(key, fallback = '') {
  const row = db().prepare('SELECT value FROM shohyo_settings WHERE key = ?').get(key);
  return row ? row.value : fallback;
}

export function setSetting(key, value) {
  db().prepare('INSERT INTO shohyo_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value')
    .run(key, String(value));
}

/** 自動添付 (strong一致を人の承認なしで貼る)。既定 OFF = 提案モード */
export function autoAttachEnabled() {
  return getSetting('auto_attach', '0') === '1';
}

// ---- 入力の正規化 (明示された不正値は throw、未指定は null) ----

export function normAmount(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error('bad_amount');
  const r = Math.round(n);
  if (!Number.isSafeInteger(r) || r <= 0 || r > AMOUNT_MAX) throw new Error('bad_amount');
  return r;
}
export function normVendorId(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  if (!Number.isSafeInteger(n) || n <= 0) throw new Error('bad_vendor_id');
  if (!getShohyoDB().prepare('SELECT 1 FROM vendor_links WHERE id = ?').get(n)) throw new Error('bad_vendor_id');
  return n;
}
const str = (v, max) => (typeof v === 'string' || typeof v === 'number') ? String(v).trim().slice(0, max) : '';

// ---- 受け入れ ----

/**
 * ファイルを受け箱に入れる。同じ内容 (sha256) は二重登録しない (既存行を返す・duplicate=true)
 * @param {Buffer} buffer
 * @param {{ file_name, source?, vendor_id?, vendor_name?, doc_date?, amount?, note? }} meta
 * @throws unsupported_file … PDF/JPEG/PNG 以外 / bad_amount / bad_vendor_id / bad_date
 */
export function addToInbox(buffer, meta) {
  const kind = sniffKind(buffer);
  if (!kind) throw new Error('unsupported_file');
  if (meta.doc_date !== undefined && meta.doc_date !== null && meta.doc_date !== '' && !isValidDate(meta.doc_date)) throw new Error('bad_date');
  const amount = normAmount(meta.amount);
  const vendorId = normVendorId(meta.vendor_id);
  const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');
  const d = db();
  const existing = d.prepare('SELECT * FROM voucher_inbox WHERE sha256 = ?').get(sha256);
  if (existing) return { row: hydrate(existing), duplicate: true };

  // 一時ファイル → rename で原子的に置く。INSERT が競合 (同時アップロード) したら相手の行が正
  const dir = storageDir();
  const finalPath = path.join(dir, `${sha256}.${kind.ext}`);
  const tmpPath = path.join(dir, `.${sha256}.${process.pid}.${Date.now()}.tmp`);
  fs.writeFileSync(tmpPath, buffer);
  try {
    fs.renameSync(tmpPath, finalPath);
  } catch (e) {
    try { fs.unlinkSync(tmpPath); } catch { /* 既に無い */ }
    throw e;
  }
  const now = new Date().toISOString();
  const res = d.prepare(`INSERT INTO voucher_inbox
    (sha256, file_name, mime, size, ext, source, vendor_id, vendor_name, doc_date, amount, note, status, created_at, updated_at)
    VALUES (@sha256, @file_name, @mime, @size, @ext, @source, @vendor_id, @vendor_name, @doc_date, @amount, @note, 'new', @now, @now)
    ON CONFLICT(sha256) DO NOTHING`)
    .run({
      sha256, file_name: str(meta.file_name, 255) || `voucher.${kind.ext}`, mime: kind.mime, size: buffer.length, ext: kind.ext,
      source: ['upload', 'robot', 'mail', 'gdrive'].includes(meta.source) ? meta.source : 'upload',
      vendor_id: vendorId, vendor_name: str(meta.vendor_name, 200),
      doc_date: isValidDate(meta.doc_date) ? meta.doc_date : '',
      amount, note: str(meta.note, 500), now,
    });
  if (res.changes === 0) {
    return { row: hydrate(d.prepare('SELECT * FROM voucher_inbox WHERE sha256 = ?').get(sha256)), duplicate: true };
  }
  return { row: getInbox(res.lastInsertRowid), duplicate: false };
}

export function getInbox(id) {
  const row = db().prepare('SELECT * FROM voucher_inbox WHERE id = ?').get(id);
  return row ? hydrate(row) : null;
}

function hydrate(row) {
  let candidates = [];
  try { candidates = JSON.parse(row.candidates_json || '[]'); } catch { /* 壊れていても一覧は出す */ }
  const { lease_token, lease_prev_status, ...rest } = row;
  return { ...rest, candidates };
}

export function listInbox({ status = null, limit = 500 } = {}) {
  const rows = status
    ? db().prepare('SELECT * FROM voucher_inbox WHERE status = ? ORDER BY doc_date DESC, id DESC LIMIT ?').all(status, limit)
    : db().prepare('SELECT * FROM voucher_inbox ORDER BY doc_date DESC, id DESC LIMIT ?').all(limit);
  return rows.map(hydrate);
}

export function listOpenInbox() {
  const q = OPEN_STATUSES.map(() => '?').join(',');
  return db().prepare(`SELECT * FROM voucher_inbox WHERE status IN (${q}) ORDER BY id`).all(...OPEN_STATUSES).map(hydrate);
}

export function countByStatus() {
  const out = Object.fromEntries(INBOX_STATUSES.map(s => [s, 0]));
  for (const r of db().prepare('SELECT status, COUNT(*) AS c FROM voucher_inbox GROUP BY status').all()) out[r.status] = r.c;
  return out;
}

/** 保存ディレクトリ配下の sha256.ext だけを読む。読んだ内容の sha256 も検証する (差し替え・破損を貼らない) */
export function readFile(row) {
  if (!/^[0-9a-f]{64}$/.test(row.sha256) || !MIME_BY_EXT[row.ext]) throw new Error('bad_row');
  const dir = storageDir();
  const p = path.resolve(dir, `${row.sha256}.${row.ext}`);
  if (path.dirname(p) !== path.resolve(dir)) throw new Error('bad_path');
  const buf = fs.readFileSync(p);
  if (crypto.createHash('sha256').update(buf).digest('hex') !== row.sha256) throw new Error('file_corrupted');
  return buf;
}

/** 人が直せる項目 (日付・金額・支払先・メモ)。直したら突合をやり直すため status は new に戻す */
export function updateInboxMeta(id, body) {
  const cur = getInbox(id);
  if (!cur) return null;
  if (cur.status === 'attached' || cur.status === 'attaching' || cur.status === 'needs_check') throw new Error('already_attached');
  if (body.doc_date !== undefined && body.doc_date !== '' && body.doc_date !== null && !isValidDate(body.doc_date)) throw new Error('bad_date');
  const data = {
    vendor_id: body.vendor_id === undefined ? cur.vendor_id : normVendorId(body.vendor_id),
    vendor_name: body.vendor_name === undefined ? cur.vendor_name : str(body.vendor_name, 200),
    doc_date: body.doc_date === undefined ? cur.doc_date : (isValidDate(body.doc_date) ? body.doc_date : ''),
    amount: body.amount === undefined ? cur.amount : normAmount(body.amount),
    note: body.note === undefined ? cur.note : str(body.note, 500),
  };
  db().prepare(`UPDATE voucher_inbox SET vendor_id=@vendor_id, vendor_name=@vendor_name, doc_date=@doc_date, amount=@amount, note=@note,
    status='new', match_tx_id='', match_journal_id='', match_journal_number=NULL, match_strength='', match_reason='', candidates_json='[]',
    match_journal_vouchers=0,
    updated_at=@now WHERE id=@id`).run({ ...data, now: new Date().toISOString(), id });
  return getInbox(id);
}

/**
 * 旧形式 (match_reason に「(仕訳に証憑あり)」を連結していた頃) の行を match_journal_vouchers へ移す。
 * 件数までは分からないので 1 件として扱う (画面の警告を出すのが目的)。冪等。
 * @returns 移した行数
 */
export function backfillJournalVouchers(d = db()) {
  return d.prepare(`UPDATE voucher_inbox SET match_journal_vouchers = 1,
    match_reason = replace(match_reason, ' (仕訳に証憑あり)', '')
    WHERE match_reason LIKE '%(仕訳に証憑あり)%'`).run().changes;
}

export function setMatch(id, { status, tx_id = '', journal_id = '', journal_number = null, strength = '', reason = '', candidates = [], journal_vouchers = 0 }) {
  if (!INBOX_STATUSES.includes(status)) throw new Error('bad_status');
  // 添付の確保中・添付済み・要確認は突合結果で上書きしない
  db().prepare(`UPDATE voucher_inbox SET status=@status, match_tx_id=@tx_id, match_journal_id=@journal_id, match_journal_number=@journal_number,
    match_strength=@strength, match_reason=@reason, match_journal_vouchers=@journal_vouchers, candidates_json=@candidates,
    last_checked_at=@now, error='', updated_at=@now
    WHERE id=@id AND status NOT IN ('attaching','attached','needs_check')`)
    .run({ status, tx_id, journal_id, journal_number, strength, reason, journal_vouchers: Number(journal_vouchers) || 0,
      candidates: JSON.stringify(candidates).slice(0, 20000), now: new Date().toISOString(), id });
}

// ---- 添付の確保 (リース) ----
// POST /vouchers の前に必ず claim する。inbox 行と明細IDの両方を確保する:
//   行 = status が CLAIMABLE のときだけ attaching に (同じ証憑を2回貼らない)
//   明細 = uq_voucher_inbox_owned_tx により attaching/attached で同じ tx_id は1件だけ (同じ明細に2つ貼らない)
// 確保できた実行者だけが貼る → cron と手動、ダブルクリック、別証憑の同時承認が重なっても1回

export function claimForAttach(id, txId) {
  if (!txId || String(txId).length > TX_ID_MAX) return null;
  const token = crypto.randomBytes(12).toString('hex');
  const q = CLAIMABLE.map(() => '?').join(',');
  try {
    const res = db().prepare(`UPDATE voucher_inbox SET lease_prev_status = status, status='attaching', lease_token=?, match_tx_id=?, updated_at=?
      WHERE id=? AND status IN (${q})`).run(token, String(txId), new Date().toISOString(), id, ...CLAIMABLE);
    return res.changes === 1 ? token : null;
  } catch (e) {
    if (/UNIQUE/i.test(e.message)) return null; // 同じ明細を他の証憑が確保済み/添付済み
    throw e;
  }
}

/** 確保したが「MFに送っていない」ことが確かなとき: 元の状態に戻す (エラー文言があれば error) */
export function releaseClaim(id, token, errorMessage = '') {
  const now = new Date().toISOString();
  db().prepare(`UPDATE voucher_inbox SET status = CASE WHEN ? != '' THEN 'error' ELSE lease_prev_status END,
    error=?, lease_token='', lease_prev_status='', last_checked_at=?, updated_at=? WHERE id=? AND status='attaching' AND lease_token=?`)
    .run(errorMessage, String(errorMessage).slice(0, 500), now, now, id, token);
}

/** POST後に結果が確定できないとき: needs_check (自動再送しない・人がMF画面で確認) */
export function markNeedsCheck(id, token, message) {
  const now = new Date().toISOString();
  db().prepare(`UPDATE voucher_inbox SET status='needs_check', error=?, lease_token='', lease_prev_status='', last_checked_at=?, updated_at=?
    WHERE id=? AND status='attaching' AND lease_token=?`).run(String(message).slice(0, 500), now, now, id, token);
}

/** 確保トークン付きで添付済みにする (CAS)。他者に確保を奪われていたら false */
export function markAttached(id, token, { journal_id, journal_number = null, tx_id = '', mf_file_id, mode, actor = '', reason = '' }) {
  const now = new Date().toISOString();
  const d = db();
  let ok = false;
  const tx = d.transaction(() => {
    const res = d.prepare(`UPDATE voucher_inbox SET status='attached', match_journal_id=@journal_id, match_journal_number=@journal_number, match_tx_id=@tx_id,
      mf_file_id=@mf_file_id, attached_at=@now, error='', lease_token='', lease_prev_status='', updated_at=@now
      WHERE id=@id AND status='attaching' AND lease_token=@token`)
      .run({ journal_id, journal_number, tx_id, mf_file_id, now, id, token });
    if (res.changes !== 1) return;
    d.prepare(`INSERT INTO voucher_attach_log (inbox_id, journal_id, journal_number, tx_id, mf_file_id, mode, actor, reason, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(id, journal_id, journal_number, tx_id, mf_file_id, mode, actor, reason, now);
    ok = true;
  });
  tx();
  return ok;
}

/** 起動時: 前回のプロセスが attaching のまま落ちた行を needs_check に (MF側に貼れている可能性があるため再送しない) */
export function recoverStaleClaims() {
  const now = new Date().toISOString();
  const res = db().prepare(`UPDATE voucher_inbox SET status='needs_check', error='前回の添付処理が途中で止まりました。MFの仕訳に貼れているか確認し、貼れていれば「除外」、貼れていなければ「戻す」を押してください',
    lease_token='', lease_prev_status='', updated_at=? WHERE status='attaching'`).run(now);
  return res.changes;
}

/** 人の操作で状態を変える。attaching/attached からは変えない。needs_check からは new (戻す) と excluded だけ */
export function setExtractNote(id, note) {
  db().prepare('UPDATE voucher_inbox SET extract_note=?, updated_at=? WHERE id=?').run(String(note || '').slice(0, 500), new Date().toISOString(), id);
}

export function setStatus(id, status) {
  if (!INBOX_STATUSES.includes(status)) throw new Error('bad_status');
  db().prepare(`UPDATE voucher_inbox SET status=?, match_tx_id = CASE WHEN ? = 'new' THEN '' ELSE match_tx_id END, error='', updated_at=?
    WHERE id=? AND status NOT IN ('attaching','attached') AND (status != 'needs_check' OR ? IN ('new','excluded'))`)
    .run(status, status, new Date().toISOString(), id, status);
  return getInbox(id);
}

export function listAttachLog(limit = 200) {
  return db().prepare(`SELECT l.*, i.file_name, i.vendor_name, i.amount, i.doc_date FROM voucher_attach_log l
    JOIN voucher_inbox i ON i.id = l.inbox_id ORDER BY l.id DESC LIMIT ?`).all(limit);
}

/** 排他的に所有されている明細 (確保中/添付済み/要確認) と、その所有者 inbox_id の集合 */
export function transactionOwners() {
  const owners = new Map();
  for (const r of db().prepare(`SELECT id, match_tx_id FROM voucher_inbox
    WHERE match_tx_id != '' AND status IN ('attaching','attached','needs_check')`).all()) {
    if (!owners.has(r.match_tx_id)) owners.set(r.match_tx_id, new Set());
    owners.get(r.match_tx_id).add(r.id);
  }
  return owners;
}
