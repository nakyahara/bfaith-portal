/**
 * rakuten-review-lib.js — RMSレビューチェックツールCSV の取込ライブラリ (mall-csv-fetcher P2 PR-A)
 *
 * らくらくーぽん置換 (AI_reference『らくらくーぽん置換_P2_要件設計_20260716.md』) の C1。
 *
 * CSV仕様 (2026-07-16 実測、review.rms.rakuten.co.jp/search/csv/):
 *   - CP932、全列ダブルクォート。ヘッダ1行目 (前置行なし)
 *   - 列: レビュータイプ / 商品名 / レビュー詳細URL / 評価 / 投稿時間 / タイトル /
 *         レビュー本文 / フラグ / 注文番号 / 未対応フラグ
 *   - レビュー詳細URLがほぼ一意 (稀に完全同一行の重複が出る → マージ)
 *   - 投稿時間 = "YYYY/M/D H:MM:SS" (JST)
 *   - 出力は新着順で ≈1.2MB 上限に切られる → downloader 側が窓分割で全量化。
 *     取込側は「行数上限ちょうど」を検知できないため window メタで検証しない (原本は processed/ に残る)
 *
 * 設計 (Codex R1/R2 反映):
 *   - current 表 = UPSERT (最新状態)。編集検知 = source_hash 変化時のみ revision append
 *   - revision 表に本文・タイトルは持たない (PII 最小化。本文は current のみ、mirror へは送らない)
 *   - 削除検知 (is_deleted) は PR-A では立てない (2回連続不在ルールは PR-C で実装)
 *   - ファイル内で同一URL: 完全同一行 → 1件にマージ (警告)、内容が違う → ファイル単位エラー
 *   - 低評価 (★1-2) の「新規に現れた」レビューを取込結果として返す → CLI が GChat 通知
 */
import crypto from 'node:crypto';
import { parseCsvBuffer } from './rakuten-ads-rpp-lib.js';

const trimS = v => String(v == null ? '' : v).trim();

export const REVIEW_SOURCE = 'rakuten-review';

// ─── DDL ───
export function ensureRakutenReviewTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_reviews (
    review_url    TEXT PRIMARY KEY,
    review_type   TEXT NOT NULL CHECK (review_type IN ('item','shop')),
    item_id       INTEGER,
    item_name     TEXT,
    rating        INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    posted_at     TEXT NOT NULL,
    date_jst      TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    title         TEXT,
    body          TEXT,
    flag          TEXT,
    order_number  TEXT,
    todo_flag     TEXT,
    source_hash   TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0,
    source_file   TEXT,
    import_id     INTEGER,
    imported_at   TEXT NOT NULL,
    updated_at    TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frr_date ON fact_rakuten_reviews(date_jst)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frr_order ON fact_rakuten_reviews(order_number)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frr_item ON fact_rakuten_reviews(item_id, date_jst)`);

  // 編集履歴 (hash 変化時のみ append。本文・タイトルは持たない — Codex R2)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_review_revisions (
    review_url   TEXT NOT NULL,
    observed_at  TEXT NOT NULL,
    source_hash  TEXT NOT NULL,
    rating       INTEGER NOT NULL,
    posted_at    TEXT NOT NULL,
    is_deleted   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (review_url, observed_at)
  )`);

  // 取込ログ (raw_rakuten_data_import_log と同型)
  db.exec(`CREATE TABLE IF NOT EXISTS raw_rakuten_review_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    shop_code    TEXT NOT NULL DEFAULT 'b-faith',
    source       TEXT NOT NULL,
    file_name    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL,
    date_from    TEXT,
    date_to      TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    updated      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ok','error','skipped','duplicate')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rrril_sha ON raw_rakuten_review_import_log(file_sha256, status)`);
}

// ─── パース ───
const HEADER_COLS = ['レビュータイプ', '商品名', 'レビュー詳細URL', '評価', '投稿時間',
  'タイトル', 'レビュー本文', 'フラグ', '注文番号', '未対応フラグ'];

/** "2026/7/6 9:07:30" → { postedAt: '2026-07-06 09:07:30', dateJst: '2026-07-06' } */
function normalizePostedAt(v) {
  const m = trimS(v).match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  const [, y, mo, d, h, mi, s] = m;
  const mm = String(+mo).padStart(2, '0');
  const dd = String(+d).padStart(2, '0');
  const hh = String(+h).padStart(2, '0');
  const date = `${y}-${mm}-${dd}`;
  // 実在日チェック
  const dt = new Date(`${date}T00:00:00Z`);
  if (isNaN(dt.getTime()) || dt.toISOString().slice(0, 10) !== date) return null;
  return { postedAt: `${date} ${hh}:${mi}:${s}`, dateJst: date };
}

/** item レビューURL https://review.rakuten.co.jp/item/1/<shop>_<itemId>/... → itemId */
function extractItemId(url) {
  const m = url.match(/\/item\/\d+\/\d+_(\d+)\//);
  return m ? Number(m[1]) : null;
}

/** レビューCSVを検証つきでレコード配列にする。downloader の DL 後検証と取込の両方から使う。
 *  @returns {ok, records, dateFrom, dateTo, warnings} | {ok:false, error} */
export function prepareReviewFile(name, buffer) {
  let rows;
  try {
    rows = parseCsvBuffer(buffer);
  } catch (e) {
    return { ok: false, error: `CSVパース失敗: ${e.message}` };
  }
  if (rows.length === 0) return { ok: false, error: '空ファイル' };

  const header = rows[0].map(trimS);
  const missing = HEADER_COLS.filter(c => !header.includes(c));
  if (missing.length > 0) {
    return { ok: false, error: `レビューCSVのヘッダ不一致 (欠落: ${missing.join(',')})。レビューチェックツール以外のCSVの可能性` };
  }
  const idx = Object.fromEntries(HEADER_COLS.map(c => [c, header.indexOf(c)]));

  const warnings = [];
  const byUrl = new Map();
  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i];
    if (cells.length !== header.length) {
      return { ok: false, error: `行${i + 1}: 列数不一致 (${cells.length} != ${header.length})` };
    }
    const typeRaw = trimS(cells[idx['レビュータイプ']]);
    const reviewType = typeRaw === '商品レビュー' ? 'item' : typeRaw === 'ショップレビュー' ? 'shop' : null;
    if (!reviewType) return { ok: false, error: `行${i + 1}: 未知のレビュータイプ「${typeRaw}」` };

    const reviewUrl = trimS(cells[idx['レビュー詳細URL']]);
    if (!/^https:\/\/review\.rakuten\.co\.jp\//.test(reviewUrl)) {
      return { ok: false, error: `行${i + 1}: レビュー詳細URLが不正 (${reviewUrl.slice(0, 60)})` };
    }
    const rating = Number(trimS(cells[idx['評価']]));
    if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
      return { ok: false, error: `行${i + 1}: 評価が不正 (${trimS(cells[idx['評価']])})` };
    }
    const ts = normalizePostedAt(cells[idx['投稿時間']]);
    if (!ts) return { ok: false, error: `行${i + 1}: 投稿時間が不正 (${trimS(cells[idx['投稿時間']])})` };

    // 商品レビューは item_id をURLから必ず取れることを要求 (取れない=URL形式変更。
    // mirror 側 contract が item=正整数を検証するため、ここで先に fail-fast して failed/ に落とす)
    const itemId = reviewType === 'item' ? extractItemId(reviewUrl) : null;
    if (reviewType === 'item' && !itemId) {
      return { ok: false, error: `行${i + 1}: 商品レビューURLから item_id を抽出できない (${reviewUrl.slice(0, 80)})。URL形式変更の疑い` };
    }
    const rec = {
      review_url: reviewUrl,
      review_type: reviewType,
      item_id: itemId,
      item_name: trimS(cells[idx['商品名']]) || null,
      rating,
      posted_at: ts.postedAt,
      date_jst: ts.dateJst,
      title: trimS(cells[idx['タイトル']]) || null,
      body: String(cells[idx['レビュー本文']] ?? ''),
      flag: trimS(cells[idx['フラグ']]) || null,
      order_number: trimS(cells[idx['注文番号']]) || null,
      todo_flag: trimS(cells[idx['未対応フラグ']]) || null,
    };
    rec.source_hash = hashReviewContent(rec);

    const prev = byUrl.get(reviewUrl);
    if (prev) {
      // 完全同一行 (楽天側の出力揺れで実在、2026-07-16 実測) はマージ。内容が違えばエラー
      if (prev.source_hash === rec.source_hash && prev.posted_at === rec.posted_at) {
        warnings.push(`完全同一行の重複をマージ: ${reviewUrl}`);
        continue;
      }
      return { ok: false, error: `同一レビューURLで内容が異なる行: ${reviewUrl} (黙って上書きしない)` };
    }
    byUrl.set(reviewUrl, rec);
  }

  const records = [...byUrl.values()];
  const dates = records.map(r => r.date_jst).sort();
  return {
    ok: true,
    label: `レビュー (${records.length}件)`,
    records,
    dateFrom: dates[0] || null,
    dateTo: dates[dates.length - 1] || null,
    warnings,
  };
}

/** 編集検知用の内容ハッシュ (last_seen 等の管理列は含めない — 冪等ハッシュに volatile を混ぜない) */
export function hashReviewContent(rec) {
  const s = JSON.stringify([rec.rating, rec.posted_at, rec.title, rec.body, rec.order_number, rec.todo_flag, rec.item_name]);
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex').slice(0, 24);
}

// ─── 取込 (1ファイル = 1トランザクション) ───
/**
 * @returns {status:'ok'|'duplicate'|'error', results:[...], newLowRatings:[...]}
 *   newLowRatings = 今回のファイルで「新規に現れた」★2以下 (GChat通知用、本文は含めない)
 */
export function importReviewFile(db, { name, buffer, sha256, source = 'incoming' }) {
  const now = new Date().toISOString();

  const dup = db.prepare(`
    SELECT id FROM raw_rakuten_review_import_log WHERE file_sha256 = ? AND status = 'ok'
  `).get(sha256);
  if (dup) {
    db.prepare(`
      INSERT INTO raw_rakuten_review_import_log (imported_at, source, file_name, file_sha256, status, message)
      VALUES (?, ?, ?, ?, 'duplicate', 'same sha256 already imported')
    `).run(now, source, name, sha256);
    return { status: 'duplicate', results: [{ file: name, ok: false, duplicate: true, error: '取込済み (sha256一致)' }], newLowRatings: [] };
  }

  const prepared = prepareReviewFile(name, buffer);
  if (!prepared.ok) {
    db.prepare(`
      INSERT INTO raw_rakuten_review_import_log (imported_at, source, file_name, file_sha256, status, message)
      VALUES (?, ?, ?, ?, 'error', ?)
    `).run(now, source, name, sha256, prepared.error.slice(0, 500));
    return { status: 'error', results: [{ file: name, ok: false, error: prepared.error }], newLowRatings: [] };
  }

  const selectStmt = db.prepare(`SELECT review_url, source_hash FROM fact_rakuten_reviews WHERE review_url = ?`);
  const insertStmt = db.prepare(`
    INSERT INTO fact_rakuten_reviews (
      review_url, review_type, item_id, item_name, rating, posted_at, date_jst,
      title, body, flag, order_number, todo_flag, source_hash,
      first_seen_at, last_seen_at, is_deleted, source_file, import_id, imported_at, updated_at
    ) VALUES (
      @review_url, @review_type, @item_id, @item_name, @rating, @posted_at, @date_jst,
      @title, @body, @flag, @order_number, @todo_flag, @source_hash,
      @now, @now, 0, @source_file, @import_id, @now, @now
    )
  `);
  const updateStmt = db.prepare(`
    UPDATE fact_rakuten_reviews SET
      review_type = @review_type, item_id = @item_id, item_name = @item_name,
      rating = @rating, posted_at = @posted_at, date_jst = @date_jst,
      title = @title, body = @body, flag = @flag, order_number = @order_number,
      todo_flag = @todo_flag, source_hash = @source_hash,
      last_seen_at = @now, is_deleted = 0, source_file = @source_file,
      import_id = @import_id, updated_at = @now
    WHERE review_url = @review_url
  `);
  const touchStmt = db.prepare(`UPDATE fact_rakuten_reviews SET last_seen_at = ?, is_deleted = 0 WHERE review_url = ?`);
  const revisionStmt = db.prepare(`
    INSERT OR IGNORE INTO fact_rakuten_review_revisions (review_url, observed_at, source_hash, rating, posted_at, is_deleted)
    VALUES (?, ?, ?, ?, ?, 0)
  `);
  const logStmt = db.prepare(`
    INSERT INTO raw_rakuten_review_import_log (
      imported_at, source, file_name, file_sha256, date_from, date_to,
      row_count, inserted, updated, status, message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', ?)
  `);

  const newLowRatings = [];
  let inserted = 0, updated = 0, unchanged = 0;
  const tx = db.transaction(() => {
    const logInfo = logStmt.run(
      now, source, name, sha256, prepared.dateFrom, prepared.dateTo,
      prepared.records.length, 0, 0, prepared.warnings.slice(0, 3).join(' / ')
    );
    const importId = logInfo.lastInsertRowid;

    for (const rec of prepared.records) {
      const existing = selectStmt.get(rec.review_url);
      const params = { ...rec, now, source_file: name, import_id: importId };
      if (!existing) {
        insertStmt.run(params);
        revisionStmt.run(rec.review_url, now, rec.source_hash, rec.rating, rec.posted_at);
        inserted++;
        if (rec.rating <= 2) {
          newLowRatings.push({
            review_url: rec.review_url, review_type: rec.review_type,
            item_name: rec.item_name, rating: rec.rating,
            posted_at: rec.posted_at, order_number: rec.order_number,
          });
        }
      } else if (existing.source_hash !== rec.source_hash) {
        updateStmt.run(params);
        revisionStmt.run(rec.review_url, now, rec.source_hash, rec.rating, rec.posted_at);
        updated++;
      } else {
        touchStmt.run(now, rec.review_url);
        unchanged++;
      }
    }
    db.prepare(`UPDATE raw_rakuten_review_import_log SET inserted = ?, updated = ? WHERE id = ?`)
      .run(inserted, updated, importId);
  });

  try {
    tx();
  } catch (e) {
    db.prepare(`
      INSERT INTO raw_rakuten_review_import_log (imported_at, source, file_name, file_sha256, status, message)
      VALUES (?, ?, ?, ?, 'error', ?)
    `).run(now, source, name, sha256, String(e.message).slice(0, 500));
    return { status: 'error', results: [{ file: name, ok: false, error: e.message }], newLowRatings: [] };
  }

  return {
    status: 'ok',
    results: [{
      file: name, ok: true, label: prepared.label, rows: prepared.records.length,
      inserted, updated, unchanged,
      date_from: prepared.dateFrom, date_to: prepared.dateTo,
      warnings: prepared.warnings,
    }],
    newLowRatings,
  };
}
