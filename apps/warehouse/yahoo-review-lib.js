/**
 * yahoo-review-lib.js — Yahoo!ショッピング 商品レビューチェックツール ZIP/CSV の取込 (P2-Y PR-Y-A)
 *
 * らくらくーぽん Yahoo 版置換 (『らくらくーぽんYahoo版_置換_要件設計_20260827.md』§Y1)。
 * 楽天版 (rakuten-review-lib.js) と同じ「incoming → UPSERT → 低評価キュー → 削除検知」だが、
 * Yahoo 固有の差分:
 *   - 入力はストクリの「ダウンロード」= ZIP (中に cp932 CSV 1本)。列 = 評価日 / 評価点数 / 商品名 /
 *     商品コード / 注文ID / コメントタイトル / コメント内容 / 動画本数 / 画像枚数 / いいね数
 *   - レビュー詳細 URL のような一意キーが無い → review_identity = sha256(注文ID + 商品コード) を個体、
 *     revision_hash = sha256(identity + 評価日 + ★ + 正規化タイトル + 正規化本文 + 動画数 + 画像数) を版とする
 *     (Codex 設計R1/R2: 版キーを個体キーにしない・revision PK は (identity, hash))
 *   - 同一 identity が 1 ファイルに 2 行以上 = 同一注文×商品の複数レビュー (可否は未実測) → **fail-closed**:
 *     その identity は fact に入れず fact_yahoo_review_conflicts へ隔離 (planner の has_review にも使わない)、件数を通知 (Codex 設計R3 High)
 *   - 削除検知は「検証済みの 90 日全量スナップショット」だけを材料にする (ファイル名の窓マーカー
 *     `_d<from>_<to>_` が必須。無いファイル=手動DLでは削除検知しない)。2 回連続不在で is_deleted=1 (監査用途)
 *   - planner (campaign-lib、MALL_TABLES.yahoo.reviews) が楽天と同じ列名で読めるよう
 *     review_url (= 'yahoo:' + identity) / posted_at ('YYYY-MM-DD 00:00:00') / date_jst / order_number /
 *     rating / first_seen_at / is_deleted を持つ
 *   - PII: レビュー本文は公開情報だが mirror には送らない。注文ID は業務データとして保持 (設計 §2.2)
 */
import crypto from 'node:crypto';
import AdmZip from 'adm-zip';
import { parseCsvBuffer } from './rakuten-ads-rpp-lib.js';

export const YAHOO_REVIEW_SOURCE = 'yahoo-review';
export const HEADER_COLS = ['評価日', '評価点数', '商品名', '商品コード', '注文ID', 'コメントタイトル', 'コメント内容', '動画本数', '画像枚数', 'いいね数'];
const ZIP_MAX_BYTES = 20 * 1024 * 1024;
const CSV_MAX_BYTES = 50 * 1024 * 1024;
const BODY_MAX_CHARS = 20000;

const trimS = (v) => String(v ?? '').trim();

export function ensureYahooReviewTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_reviews (
    review_identity  TEXT PRIMARY KEY,
    review_url       TEXT NOT NULL UNIQUE,
    order_number     TEXT NOT NULL,
    product_code     TEXT NOT NULL,
    item_name        TEXT,
    rating           INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    posted_at        TEXT NOT NULL,
    date_jst         TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    title            TEXT,
    body             TEXT,
    video_count      INTEGER NOT NULL DEFAULT 0,
    image_count      INTEGER NOT NULL DEFAULT 0,
    like_count       INTEGER NOT NULL DEFAULT 0,
    revision_hash    TEXT NOT NULL,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    is_deleted       INTEGER NOT NULL DEFAULT 0,
    miss_count       INTEGER NOT NULL DEFAULT 0,
    last_missed_on   TEXT,
    source_file      TEXT,
    import_id        INTEGER,
    imported_at      TEXT NOT NULL,
    updated_at       TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fyr_order ON fact_yahoo_reviews(order_number)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fyr_date ON fact_yahoo_reviews(date_jst)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fyr_first_seen ON fact_yahoo_reviews(first_seen_at)`);
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_review_revisions (
    review_identity TEXT NOT NULL,
    revision_hash   TEXT NOT NULL,
    observed_at     TEXT NOT NULL,
    rating          INTEGER NOT NULL,
    date_jst        TEXT NOT NULL,
    is_deleted      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (review_identity, revision_hash)
  )`);
  // identity 衝突 (同一注文×商品に内容の違う行が複数) は fact に入れず隔離する (fail-closed、Codex Y-A R2 High 4:
  // fact に置くと planner の has_review 判定に使われてしまう)。解消 (次の全量で 1 行に戻る) したら fact へ戻す
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_review_conflicts (
    review_identity TEXT PRIMARY KEY,
    order_number    TEXT NOT NULL,
    product_code    TEXT NOT NULL,
    item_name       TEXT,
    rows_seen       INTEGER NOT NULL,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    source_file     TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS raw_yahoo_review_import_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at TEXT NOT NULL,
    source      TEXT NOT NULL,
    file_name   TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    date_from   TEXT,
    date_to     TEXT,
    window_from TEXT,
    window_to   TEXT,
    row_count   INTEGER,
    inserted    INTEGER,
    updated     INTEGER,
    status      TEXT NOT NULL CHECK (status IN ('ok','duplicate','error')),
    message     TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_yril_sha ON raw_yahoo_review_import_log(file_sha256, status)`);
  // 検証済み全量スナップショット台帳 (Codex Y-A R1 High: ファイル名マーカーは誰でも付けられるので信頼境界にしない。
  // downloader が「画面の件数 = 行数・窓内・ヘッダ一致」を確認した ZIP の sha256 をここに記録し、
  // importer は sha256 が一致し窓も一致するファイルだけを削除検知の材料にする)
  db.exec(`CREATE TABLE IF NOT EXISTS yahoo_review_snapshots (
    file_sha256  TEXT PRIMARY KEY,
    window_from  TEXT NOT NULL,
    window_to    TEXT NOT NULL,
    screen_count INTEGER NOT NULL,
    verified_at  TEXT NOT NULL
  )`);
  // 低評価通知キュー (取込 tx 内で enqueue、送信 2xx で削除)。kind = first (初回観測) / transition (★3以上→★2以下)
  db.exec(`CREATE TABLE IF NOT EXISTS yahoo_review_low_notify_queue (
    review_identity TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('first','transition','conflict')),
    item_name       TEXT,
    product_code    TEXT,
    rating          INTEGER,
    date_jst        TEXT,
    queued_at       TEXT NOT NULL,
    PRIMARY KEY (review_identity, kind)
  )`);
}

// ─── identity / revision ───
export function reviewIdentityFor(orderNumber, productCode) {
  return crypto.createHash('sha256').update(`yahoo:${trimS(orderNumber)}|${trimS(productCode)}`, 'utf8').digest('hex').slice(0, 32);
}
export function normalizeText(s) {
  return String(s ?? '').replace(/\x00/g, '').replace(/\r\n?/g, '\n').replace(/[ \t]+/g, ' ').trim().slice(0, BODY_MAX_CHARS);
}
export function revisionHashFor(rec) {
  const s = JSON.stringify([rec.review_identity, rec.date_jst, rec.rating, normalizeText(rec.title), normalizeText(rec.body), rec.video_count, rec.image_count]);
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex').slice(0, 24);
}

/** 画面の件数と突合する行数 = 通常レコード + 衝突 identity の実行数 (衝突は identity 数ではなく行数で数える — Codex Y-A R3 High) */
export function countedRows(prepared) {
  return prepared.records.length + prepared.conflicts.reduce((n, c) => n + (c.rows_seen || 1), 0);
}

/** downloader が検証済みの全量スナップショットを登録する (importer の削除検知の信頼境界) */
export function recordVerifiedSnapshot(db, { sha256, from, to, screenCount, nowIso = new Date().toISOString() }) {
  db.prepare(`INSERT OR REPLACE INTO yahoo_review_snapshots (file_sha256, window_from, window_to, screen_count, verified_at) VALUES (?, ?, ?, ?, ?)`)
    .run(sha256, from, to, screenCount, nowIso);
}
export function getVerifiedSnapshot(db, sha256) {
  return db.prepare(`SELECT * FROM yahoo_review_snapshots WHERE file_sha256 = ?`).get(sha256) || null;
}

/** ファイル名の要求窓マーカー `_d<from>_<to>_` (downloader の命名契約) */
export function parseWindowMarker(name) {
  const m = String(name).match(/_d(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})[_.]/);
  if (!m) return null;
  const [, from, to] = m;
  if (from > to) return null;
  return { from, to };
}

/** 'YYYYMMDD' → 'YYYY-MM-DD' (実在日チェック込み) */
function normalizeYmd(s) {
  const m = String(s).trim().match(/^(\d{4})(\d{2})(\d{2})$/) || String(s).trim().match(/^(\d{4})[/-](\d{2})[/-](\d{2})$/);
  if (!m) return null;
  const iso = `${m[1]}-${m[2]}-${m[3]}`;
  const d = new Date(`${iso}T00:00:00Z`);
  return Number.isFinite(d.getTime()) && d.toISOString().slice(0, 10) === iso ? iso : null;
}

/** ZIP → CSV バッファ (1 エントリのみ・path traversal / zip bomb ガード) */
export function extractReviewCsvFromZip(buffer) {
  if (buffer.length > ZIP_MAX_BYTES) throw new Error(`ZIP が大きすぎる (${buffer.length} bytes > ${ZIP_MAX_BYTES})`);
  let zip;
  try { zip = new AdmZip(buffer); } catch (e) { throw new Error(`ZIP として読めない: ${e.message}`); }
  const entries = zip.getEntries().filter((e) => !e.isDirectory);
  if (entries.length !== 1) throw new Error(`ZIP のエントリ数が ${entries.length} (1 本のみ許可)`);
  const e = entries[0];
  // adm-zip は entryName を正規化するため raw 名で検査する (path traversal を黙って直さない)
  const name = e.rawEntryName ? e.rawEntryName.toString('utf8') : e.entryName;
  if (/(^|[\\/])\.\.([\\/]|$)/.test(name) || /^[\\/]/.test(name) || /^[A-Za-z]:/.test(name)) throw new Error(`ZIP エントリ名が不正 (${name})`);
  if (!/\.csv$/i.test(name)) throw new Error(`ZIP の中身が CSV ではない (${name})`);
  const declared = e.header.size;
  if (declared > CSV_MAX_BYTES) throw new Error(`CSV の宣言サイズが大きすぎる (${declared})`);
  const data = e.getData();
  if (data.length > CSV_MAX_BYTES) throw new Error(`CSV が大きすぎる (${data.length})`);
  return { name, data };
}

/**
 * ZIP (or CSV) を解釈して取込レコードにする。DL 直後の検証にも使う。
 * @returns {ok, records, conflicts, dateFrom, dateTo, warnings, csvName} | {ok:false, error}
 */
export function prepareYahooReviewFile(name, buffer) {
  let csvName = name, csv = buffer;
  if (/\.zip$/i.test(name) || (buffer.length >= 4 && buffer[0] === 0x50 && buffer[1] === 0x4b)) {
    try { ({ name: csvName, data: csv } = extractReviewCsvFromZip(buffer)); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  let rows;
  try { rows = parseCsvBuffer(csv); } catch (e) { return { ok: false, error: `CSVパース失敗: ${e.message}` }; }
  if (rows.length === 0) return { ok: false, error: '空ファイル' };
  const header = rows[0].map((h) => trimS(h).replace(/^\uFEFF/, ''));
  const missing = HEADER_COLS.filter((c) => !header.includes(c));
  if (missing.length > 0) return { ok: false, error: `ヘッダ不一致 (欠落: ${missing.join(',')})。商品レビューチェックツール以外の CSV の可能性` };
  const idx = Object.fromEntries(HEADER_COLS.map((c) => [c, header.indexOf(c)]));

  const warnings = [];
  const byId = new Map();
  const conflictIds = new Set();
  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i];
    if (cells.length === 1 && trimS(cells[0]) === '') continue; // 末尾空行
    if (cells.length !== header.length) return { ok: false, error: `行${i + 1}: 列数不一致 (${cells.length} != ${header.length})` };
    const dateJst = normalizeYmd(cells[idx['評価日']]);
    if (!dateJst) return { ok: false, error: `行${i + 1}: 評価日が不正 (${trimS(cells[idx['評価日']])})` };
    const rating = Number(trimS(cells[idx['評価点数']]));
    if (!Number.isInteger(rating) || rating < 1 || rating > 5) return { ok: false, error: `行${i + 1}: 評価点数が不正 (${trimS(cells[idx['評価点数']])})` };
    const orderNumber = trimS(cells[idx['注文ID']]);
    const productCode = trimS(cells[idx['商品コード']]);
    if (!orderNumber || !/^[A-Za-z0-9_-]+$/.test(orderNumber)) return { ok: false, error: `行${i + 1}: 注文ID が不正 (${orderNumber.slice(0, 40)})` };
    if (!productCode) return { ok: false, error: `行${i + 1}: 商品コードが空` };
    const toInt = (v) => { const n = Number(trimS(v) || 0); return Number.isInteger(n) && n >= 0 ? n : 0; };
    const rec = {
      review_identity: reviewIdentityFor(orderNumber, productCode),
      order_number: orderNumber,
      product_code: productCode,
      item_name: trimS(cells[idx['商品名']]) || null,
      rating,
      date_jst: dateJst,
      posted_at: `${dateJst} 00:00:00`,
      title: normalizeText(cells[idx['コメントタイトル']]) || null,
      body: normalizeText(cells[idx['コメント内容']]),
      video_count: toInt(cells[idx['動画本数']]),
      image_count: toInt(cells[idx['画像枚数']]),
      like_count: toInt(cells[idx['いいね数']]),
    };
    rec.review_url = `yahoo:${rec.review_identity}`;
    rec.revision_hash = revisionHashFor(rec);
    const prev = byId.get(rec.review_identity);
    if (prev) {
      if (prev.revision_hash === rec.revision_hash) { warnings.push(`完全同一行の重複をマージ: ${orderNumber}/${productCode}`); continue; }
      conflictIds.add(rec.review_identity); // 同一注文×商品に内容の違う行 = 複数レビュー (fail-closed)
      continue;
    }
    byId.set(rec.review_identity, rec);
  }
  const conflicts = [...conflictIds].map((id) => ({ ...byId.get(id), rows_seen: rows.slice(1).filter((c) => c.length === header.length && reviewIdentityFor(trimS(c[idx['注文ID']]), trimS(c[idx['商品コード']])) === id).length }));
  for (const id of conflictIds) byId.delete(id);
  const records = [...byId.values()];
  const dates = records.map((r) => r.date_jst).sort();
  return {
    ok: true,
    label: `Yahooレビュー (${records.length}件${conflicts.length ? `、衝突 ${conflicts.length}` : ''})`,
    records, conflicts, csvName,
    dateFrom: dates[0] || null, dateTo: dates[dates.length - 1] || null,
    warnings,
  };
}

// ─── 取込 (1 ファイル = 1 トランザクション) ───
/**
 * @returns {status:'ok'|'duplicate'|'error', results:[...], newLowRatings:[...]}
 * 削除検知: 窓マーカーがあり、かつ opts.fullSnapshot=true (downloader が「画面の件数 = 行数」を検証済み)
 * のときだけ、窓内 (date_jst が from..to) の既存 identity が現れなければ miss+1、2 回連続で is_deleted=1。
 */
export function importYahooReviewFile(db, { name, buffer, sha256, source = 'incoming', nowIso = null, fullSnapshot = null }) {
  const now = nowIso || new Date().toISOString();
  // 観測窓 = 台帳 (検証済み) > ファイル名マーカー > なし。冪等判定は (sha256, 窓) で行う
  // (Codex Y-A R3 High: 0件スナップショットは本文が毎回同一で sha256 が同じになる → 窓が違えば別の観測として取り込む)
  const verified = getVerifiedSnapshot(db, sha256);
  const marker = parseWindowMarker(name);
  const window = verified ? { from: verified.window_from, to: verified.window_to } : (fullSnapshot === true ? marker : null);
  const obsWindow = window || marker || null;
  const dup = db.prepare(`SELECT id FROM raw_yahoo_review_import_log WHERE file_sha256 = ? AND status = 'ok' AND COALESCE(window_from, '') = ? AND COALESCE(window_to, '') = ?`)
    .get(sha256, obsWindow?.from || '', obsWindow?.to || '');
  if (dup) {
    db.prepare(`INSERT INTO raw_yahoo_review_import_log (imported_at, source, file_name, file_sha256, status, message)
                VALUES (?, ?, ?, ?, 'duplicate', 'same sha256 already imported')`).run(now, source, name, sha256);
    return { status: 'duplicate', results: [{ file: name, ok: false, duplicate: true, error: '取込済み (sha256一致)' }], newLowRatings: [] };
  }
  const prepared = prepareYahooReviewFile(name, buffer);
  if (!prepared.ok) {
    db.prepare(`INSERT INTO raw_yahoo_review_import_log (imported_at, source, file_name, file_sha256, status, message)
                VALUES (?, ?, ?, ?, 'error', ?)`).run(now, source, name, sha256, prepared.error.slice(0, 500));
    return { status: 'error', results: [{ file: name, ok: false, error: prepared.error }], newLowRatings: [] };
  }
  // 削除検知の材料 = 台帳 (yahoo_review_snapshots) に sha256 が登録され、その窓と一致するファイルだけ。
  // ファイル名のマーカーだけでは検知しない (手動DL・部分CSVで正規レビューを削除扱いにしない)。
  // fullSnapshot=true はテスト用の明示オーバーライド (窓はマーカーから取る)
  const detectDeletion = !!window && (fullSnapshot === false ? false : true);
  const snapshotNote = detectDeletion ? '' : (marker ? ' (削除検知なし: 検証済みスナップショット未登録)' : ' (削除検知なし)');

  const selectStmt = db.prepare(`SELECT review_identity, revision_hash, rating, is_deleted FROM fact_yahoo_reviews WHERE review_identity = ?`);
  const selectConflictStmt = db.prepare(`SELECT review_identity FROM fact_yahoo_review_conflicts WHERE review_identity = ?`);
  const upsertConflictStmt = db.prepare(`INSERT INTO fact_yahoo_review_conflicts (review_identity, order_number, product_code, item_name, rows_seen, first_seen_at, last_seen_at, source_file)
    VALUES (@review_identity, @order_number, @product_code, @item_name, @rows_seen, @now, @now, @source_file)
    ON CONFLICT(review_identity) DO UPDATE SET rows_seen = excluded.rows_seen, last_seen_at = excluded.last_seen_at, source_file = excluded.source_file, item_name = excluded.item_name`);
  const deleteFactStmt = db.prepare(`DELETE FROM fact_yahoo_reviews WHERE review_identity = ?`);
  const deleteConflictStmt = db.prepare(`DELETE FROM fact_yahoo_review_conflicts WHERE review_identity = ?`);
  const insertStmt = db.prepare(`
    INSERT INTO fact_yahoo_reviews (
      review_identity, review_url, order_number, product_code, item_name, rating, posted_at, date_jst, title, body,
      video_count, image_count, like_count, revision_hash, first_seen_at, last_seen_at, is_deleted, miss_count,
      source_file, import_id, imported_at, updated_at
    ) VALUES (
      @review_identity, @review_url, @order_number, @product_code, @item_name, @rating, @posted_at, @date_jst, @title, @body,
      @video_count, @image_count, @like_count, @revision_hash, @now, @now, 0, 0,
      @source_file, @import_id, @now, @now
    )`);
  const updateStmt = db.prepare(`
    UPDATE fact_yahoo_reviews SET
      item_name = @item_name, rating = @rating, posted_at = @posted_at, date_jst = @date_jst, title = @title, body = @body,
      video_count = @video_count, image_count = @image_count, like_count = @like_count, revision_hash = @revision_hash,
      last_seen_at = @now, is_deleted = 0, miss_count = 0, source_file = @source_file, import_id = @import_id, updated_at = @now
    WHERE review_identity = @review_identity`);
  const touchStmt = db.prepare(`UPDATE fact_yahoo_reviews SET last_seen_at = ?, is_deleted = 0, miss_count = 0, like_count = ?, updated_at = ? WHERE review_identity = ?`);
  const revisionStmt = db.prepare(`INSERT OR IGNORE INTO fact_yahoo_review_revisions (review_identity, revision_hash, observed_at, rating, date_jst, is_deleted) VALUES (?, ?, ?, ?, ?, ?)`);
  const enqueueLowStmt = db.prepare(`INSERT OR IGNORE INTO yahoo_review_low_notify_queue (review_identity, kind, item_name, product_code, rating, date_jst, queued_at) VALUES (?, ?, ?, ?, ?, ?, ?)`);
  const logStmt = db.prepare(`
    INSERT INTO raw_yahoo_review_import_log (imported_at, source, file_name, file_sha256, date_from, date_to, window_from, window_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', ?)`);
  const missStmt = db.prepare(`UPDATE fact_yahoo_reviews SET miss_count = miss_count + 1, last_missed_on = ?, updated_at = ? WHERE review_identity = ?`);
  const markDeletedStmt = db.prepare(`UPDATE fact_yahoo_reviews SET is_deleted = 1, miss_count = miss_count + 1, last_missed_on = ?, updated_at = ? WHERE review_identity = ?`);
  const activeInWindowStmt = db.prepare(`
    SELECT review_identity, revision_hash, rating, date_jst, miss_count, last_missed_on FROM fact_yahoo_reviews
     WHERE is_deleted = 0 AND date_jst >= ? AND date_jst <= ?`);

  if (verified && verified.screen_count !== countedRows(prepared)) {
    const msg = `検証済みスナップショットの件数 ${verified.screen_count} と行数 ${countedRows(prepared)} が一致しない`;
    db.prepare(`INSERT INTO raw_yahoo_review_import_log (imported_at, source, file_name, file_sha256, status, message) VALUES (?, ?, ?, ?, 'error', ?)`).run(now, source, name, sha256, msg);
    return { status: 'error', results: [{ file: name, ok: false, error: msg }], newLowRatings: [] };
  }
  const tx = db.transaction(() => {
    const log = logStmt.run(now, source, name, sha256, prepared.dateFrom, prepared.dateTo, obsWindow?.from || null, obsWindow?.to || null, countedRows(prepared), 0, 0, '');
    const importId = Number(log.lastInsertRowid);
    let inserted = 0, updated = 0, unchanged = 0, missed = 0, deleted = 0, resolvedConflicts = 0, conflictHeld = 0;
    const newLow = [];
    const seen = new Set();
    for (const rec of prepared.records) {
      seen.add(rec.review_identity);
      const prev = selectStmt.get(rec.review_identity);
      const params = { ...rec, now, source_file: name, import_id: importId };
      // 衝突解消 (1 行に戻った) は検証済み全量スナップショットのときだけ認める (Codex Y-A R3 High: 部分CSVで片方だけ
      // 来ただけで fact に復帰させない)。未検証ファイルでは衝突 identity を取り込まない (隔離のまま)
      if (selectConflictStmt.get(rec.review_identity)) {
        if (!detectDeletion) { conflictHeld++; continue; }
        deleteConflictStmt.run(rec.review_identity); resolvedConflicts++;
      }
      if (!prev) {
        insertStmt.run(params);
        revisionStmt.run(rec.review_identity, rec.revision_hash, now, rec.rating, rec.date_jst, 0);
        inserted++;
        if (rec.rating <= 2) { enqueueLowStmt.run(rec.review_identity, 'first', rec.item_name, rec.product_code, rec.rating, rec.date_jst, now); newLow.push({ identity: rec.review_identity, rating: rec.rating }); }
      } else if (prev.revision_hash !== rec.revision_hash) {
        updateStmt.run(params);
        revisionStmt.run(rec.review_identity, rec.revision_hash, now, rec.rating, rec.date_jst, 0);
        updated++;
        if (rec.rating <= 2 && prev.rating >= 3) { enqueueLowStmt.run(rec.review_identity, 'transition', rec.item_name, rec.product_code, rec.rating, rec.date_jst, now); newLow.push({ identity: rec.review_identity, rating: rec.rating, transition: true }); }
      } else {
        touchStmt.run(now, rec.like_count, now, rec.review_identity);
        unchanged++;
      }
    }
    // 衝突 identity: fact から外して隔離表へ (既存の通常行があれば削除 = planner の has_review にも使わない)。
    // 「新規の衝突」と「通常→衝突への遷移」の両方を通知 (Codex Y-A R2 High 3)
    let conflictsNew = 0;
    for (const rec of prepared.conflicts) {
      seen.add(rec.review_identity);
      const wasNormal = !!selectStmt.get(rec.review_identity);
      const wasConflict = !!selectConflictStmt.get(rec.review_identity);
      if (wasNormal) { deleteFactStmt.run(rec.review_identity); revisionStmt.run(rec.review_identity, `conflict:${rec.revision_hash}`, now, rec.rating, rec.date_jst, 0); }
      upsertConflictStmt.run({ review_identity: rec.review_identity, order_number: rec.order_number, product_code: rec.product_code, item_name: rec.item_name, rows_seen: rec.rows_seen, now, source_file: name });
      if (!wasConflict) { enqueueLowStmt.run(rec.review_identity, 'conflict', rec.item_name, rec.product_code, rec.rating, rec.date_jst, now); conflictsNew++; }
    }
    if (detectDeletion) {
      // 同日ガードは JST 暦日で (Codex Y-A R2 High 2: UTC 日付だと JST 同日の 2 回目を別日に数えて誤削除する)
      const today = new Date(Date.parse(now) + 9 * 3600 * 1000).toISOString().slice(0, 10);
      for (const row of activeInWindowStmt.all(window.from, window.to)) {
        if (seen.has(row.review_identity)) continue;
        if (row.last_missed_on === today) continue; // 同日 2 回の取込で 2 回数えない
        if (row.miss_count + 1 >= 2) {
          markDeletedStmt.run(today, now, row.review_identity);
          revisionStmt.run(row.review_identity, `deleted:${row.revision_hash}`, now, row.rating, row.date_jst, 1);
          deleted++;
        } else {
          missStmt.run(today, now, row.review_identity);
          missed++;
        }
      }
    }
    db.prepare(`UPDATE raw_yahoo_review_import_log SET inserted = ?, updated = ?, message = ? WHERE id = ?`)
      .run(inserted, updated, `unchanged=${unchanged} conflicts=${prepared.conflicts.length} (new ${conflictsNew}, resolved ${resolvedConflicts}, held ${conflictHeld}) missed=${missed} deleted=${deleted}${snapshotNote}`, importId);
    return { inserted, updated, unchanged, missed, deleted, newLow, conflictsNew, resolvedConflicts, conflictHeld };
  });
  const r = tx();
  return {
    status: 'ok',
    results: [{ file: name, ok: true, label: prepared.label, inserted: r.inserted, updated: r.updated, unchanged: r.unchanged, missed: r.missed, deleted: r.deleted,
      conflicts: prepared.conflicts.length, conflictsNew: r.conflictsNew, resolvedConflicts: r.resolvedConflicts, conflictHeld: r.conflictHeld, date_from: prepared.dateFrom, date_to: prepared.dateTo, warnings: prepared.warnings }],
    newLowRatings: r.newLow,
  };
}
