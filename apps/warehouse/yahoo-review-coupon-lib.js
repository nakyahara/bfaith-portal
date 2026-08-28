/**
 * yahoo-review-coupon-lib.js — レビュー投稿のお礼クーポン (月次・非表示・定率5%) の台帳と純粋ロジック (P2-Y PR-Y-C3)
 *
 * Yahoo にはクーポン発行 API が無いため、ストアクリエイター Pro の画面操作で作る
 * (既存の `scripts/mall-csv-fetcher/yahoo-coupon-rotate.mjs` と同じ手法。あちらは公開クーポンの定期入れ替え)。
 * 画面操作は **非冪等** なので、設計書 v0.4 §Y3 の状態機械で「二重発行より未発行を選ぶ」:
 *
 *   planned ─(枠を予約)→ submitting ─(画面操作を1回だけ)→ 一覧を op-id で照合
 *        ├─ ちょうど1件 → issued (詳細URLを読み取って保存)
 *        └─ 0件/複数/応答不明 → reconcile_required (**以後は照合のみ。作成は自動で再試行しない**)
 *                                   24時間解決しなければ manual_intervention として人へ
 *
 * op-id = クーポンの説明文の末尾に埋め込む機械可読な識別子。「同月のクーポンが無いこと」は
 * 手動作成や vendor のクーポンと衝突するため識別条件に使わない (Codex 設計R1)。
 *
 * 送信側 (PR-Y-C4) は status='issued' かつ coupon_start <= now < coupon_end の行の URL だけを使う。
 */

export const COUPON_DISCOUNT_RATIO = 5;          // 定率 5% (vendor 現行と同じ)
export const COUPON_TITLE = '雑貨イズムYahoo!ショッピング店で次回購入に使える5％割引クーポン'; // vendor の実タイトルと完全一致 (％は全角)
export const COUPON_START_HOUR = '00';
export const COUPON_END_HOUR = '23';
export const OP_ID_PREFIX = 'RVW';
// コピー元クーポンに要求する条件 (一覧の hidden input。実測 2026-08-27: vendor の月次クーポン)
export const SOURCE_DISCOUNT_TYPE = '2';   // 2 = 定率割引
// 発行フォームに要求する値 (2026-08-28 に vendor のクーポンを実測)
export const EXPECTED_FORM = Object.freeze({
  DiscountType: '2',      // 定率
  DiscountRatio: '5',     // 5%
  DispFlg: '0',           // 公開範囲 = 非表示 (URL を知っている人だけが獲得できる)
  ItemDesignation: '3',   // ストア内全商品
  Combine: '0',           // 他クーポンとの併用不可
  nDayFlg: '0',           // 期間指定 (獲得日起点ではない)
});
/** コピー元として使える行か (定率5%であること)。違えば作成しない = 定額や別条件のクーポンを増殖させない */
export function isUsableCopySource(row) {
  return !!row && String(row.discountType) === SOURCE_DISCOUNT_TYPE && String(row.discountRatio) === String(COUPON_DISCOUNT_RATIO);
}

export function ensureYahooCouponLedger(db) {
  const migrate = (t, col, ddl) => {
    const has = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(t);
    if (!has) return;
    const cols = db.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name);
    if (!cols.includes(col)) db.exec(`ALTER TABLE ${t} ADD COLUMN ${ddl}`);
  };
  db.exec(`CREATE TABLE IF NOT EXISTS yahoo_campaign_coupons (
    month        TEXT PRIMARY KEY CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    status       TEXT NOT NULL CHECK (status IN ('planned','submitting','reconcile_required','issued','manual_intervention','abandoned')),
    operation_id TEXT NOT NULL UNIQUE,
    coupon_id    TEXT UNIQUE,
    coupon_url   TEXT,
    coupon_start TEXT NOT NULL,
    coupon_end   TEXT NOT NULL,
    reserved_at  TEXT NOT NULL,
    submitted_at TEXT,
    reconcile_since TEXT,
    issued_at    TEXT,
    note         TEXT,
    updated_at   TEXT NOT NULL
  )`);
  migrate('yahoo_campaign_coupons', 'reconcile_since', 'reconcile_since TEXT');
}

/** 'YYYY-MM' の妥当性 */
export function isValidMonth(m) {
  return /^\d{4}-(0[1-9]|1[0-2])$/.test(String(m || ''));
}

/** JST 暦日 'YYYY/MM/DD' (Yahoo のフォーム表記) */
function ymdSlash(y, m, d) {
  return `${y}/${String(m).padStart(2, '0')}/${String(d).padStart(2, '0')}`;
}
function lastDayOf(year, month1) {
  return new Date(Date.UTC(year, month1, 0)).getUTCDate(); // month1=1..12 → その月の日数
}

/**
 * 月次クーポンの掲載期間を決める。vendor と同じ「月初発行・翌月末まで有効」。
 * 実行が月の途中なら開始は当日 (Yahoo は過去日を開始にできない)。
 * @returns { startYmd, endYmd, startHour, endHour, spanDays } (YYYY/MM/DD)
 */
export function monthlyCouponPeriod(month, nowIso = new Date().toISOString()) {
  if (!isValidMonth(month)) throw new Error(`月の形式が不正 (${month})`);
  const [y, m] = month.split('-').map(Number);
  const jstNow = new Date(Date.parse(nowIso) + 9 * 3600 * 1000);
  const today = { y: jstNow.getUTCFullYear(), m: jstNow.getUTCMonth() + 1, d: jstNow.getUTCDate() };
  // 開始 = 月初。ただし当月かつ既に月初を過ぎているなら当日 (過去日は Yahoo が受け付けない)
  let start = { y, m, d: 1 };
  if (y === today.y && m === today.m && today.d > 1) start = today;
  if (new Date(Date.UTC(y, m - 1, 1)) < new Date(Date.UTC(today.y, today.m - 1, today.d)) && !(y === today.y && m === today.m)) {
    throw new Error(`過去の月には発行できない (${month})`);
  }
  // 終了 = 翌月末
  const endMonth = m === 12 ? 1 : m + 1;
  const endYear = m === 12 ? y + 1 : y;
  const end = { y: endYear, m: endMonth, d: lastDayOf(endYear, endMonth) };
  const spanDays = Math.round((Date.UTC(end.y, end.m - 1, end.d) - Date.UTC(start.y, start.m - 1, start.d)) / 86400000);
  if (spanDays > 90) throw new Error(`公開期間が90日を超過 (${spanDays}日、Yahoo制約は90日以内)`);
  if (spanDays < 1) throw new Error(`期間が不正 (${spanDays}日)`);
  return {
    startYmd: ymdSlash(start.y, start.m, start.d),
    endYmd: ymdSlash(end.y, end.m, end.d),
    startHour: COUPON_START_HOUR, endHour: COUPON_END_HOUR, spanDays,
  };
}

/** 一意な操作ID (説明文に埋め込んで一覧で照合する)。英数のみ = 画面・URL で壊れない */
export function makeOperationId(month, rand = null) {
  const r = rand || Math.random().toString(36).slice(2, 8).toUpperCase();
  return `${OP_ID_PREFIX}-${month.replace('-', '')}-${r}`;
}

/** 説明文 (末尾に op-id)。顧客に見えるが、管理番号として無害な体裁にする */
export function couponDescription(operationId) {
  return `レビューをご投稿いただいたお客様への感謝クーポンです。ストア内全商品にご利用いただけます。（管理番号 ${operationId}）`;
}

/** 一覧行 (yahoo-coupon-rotate の fetchCouponRows 形式) から op-id 一致を探す */
export function findByOperationId(rows, operationId) {
  return rows.filter((r) => String(r.description || r.title || '').includes(operationId)
    || String(r.opId || '') === operationId);
}

/** 獲得URLの妥当性 (Yahoo のクーポンページであること) */
export function isValidCouponUrl(url) {
  return /^https:\/\/shopping\.yahoo\.co\.jp\/coupon\/[A-Za-z0-9_-]+\/[A-Za-z0-9]{16,}\/?$/.test(String(url || ''));
}

// ─── 台帳操作 (すべて単一トランザクション) ───

export function getCouponRow(db, month) {
  return db.prepare(`SELECT * FROM yahoo_campaign_coupons WHERE month = ?`).get(month) || null;
}

/**
 * 月の枠を予約する (planned)。既に行があれば null (状態は呼び出し側が見る)。
 * @returns {null | {operationId, period}}
 */
export function reserveMonth(db, { month, period, operationId, nowIso = new Date().toISOString() }) {
  const tx = db.transaction(() => {
    if (getCouponRow(db, month)) return null;
    db.prepare(`INSERT INTO yahoo_campaign_coupons
      (month, status, operation_id, coupon_start, coupon_end, reserved_at, updated_at)
      VALUES (?, 'planned', ?, ?, ?, ?, ?)`)
      .run(month, operationId, `${period.startYmd} ${period.startHour}:00`, `${period.endYmd} ${period.endHour}:00`, nowIso, nowIso);
    return { operationId, period };
  });
  return tx.immediate();
}

/** planned → submitting (画面操作の直前に commit する。以後 status で自動再作成を止める) */
export function markSubmitting(db, month, nowIso = new Date().toISOString()) {
  const r = db.prepare(`UPDATE yahoo_campaign_coupons SET status = 'submitting', submitted_at = ?, updated_at = ?
                         WHERE month = ? AND status = 'planned'`).run(nowIso, nowIso, month);
  return r.changes === 1;
}

/** 照合できた → issued (URL は検証済みのものだけ) */
export function markIssued(db, { month, couponId, couponUrl, nowIso = new Date().toISOString() }) {
  if (!isValidCouponUrl(couponUrl)) throw new Error(`獲得URLが不正 (${String(couponUrl).slice(0, 60)})`);
  if (!couponUrlMatchesId(couponUrl, couponId)) throw new Error(`獲得URLがクーポンIDと一致しない (id=${String(couponId).slice(0, 24)})`);
  const r = db.prepare(`UPDATE yahoo_campaign_coupons SET status = 'issued', coupon_id = ?, coupon_url = ?, issued_at = ?, note = NULL, updated_at = ?
                         WHERE month = ? AND status IN ('submitting','reconcile_required')`)
    .run(couponId, couponUrl, nowIso, nowIso, month);
  return r.changes === 1;
}

/** 獲得URLの末尾がそのクーポンIDであること (別クーポンの URL を掴んでいないか — Codex Y-C3 R1 Medium) */
export function couponUrlMatchesId(url, couponId) {
  if (!isValidCouponUrl(url) || !/^[A-Za-z0-9]{16,}$/.test(String(couponId || ''))) return false;
  return String(url).replace(/\/$/, '').endsWith(`/${couponId}`);
}

/** 結果不明・0件・複数 → reconcile_required (作成は再試行しない) */
export function markReconcileRequired(db, { month, note, nowIso = new Date().toISOString() }) {
  // reconcile_since は「最初に reconcile_required になった時刻」を保つ (Codex Y-C3 R1 High:
  // updated_at を基準にすると、毎回の照合で更新され続けて 24 時間のエスカレーションが永久に発火しない)
  const r = db.prepare(`UPDATE yahoo_campaign_coupons
       SET status = 'reconcile_required', note = ?, updated_at = ?,
           reconcile_since = COALESCE(reconcile_since, ?)
     WHERE month = ? AND status IN ('submitting','reconcile_required')`)
    .run(String(note || '').slice(0, 300), nowIso, nowIso, month);
  return r.changes === 1;
}

/** 24時間解決しない reconcile_required を人手対応へ上げる */
export function escalateStale(db, { hours = 24, nowIso = new Date().toISOString() } = {}) {
  const cutoff = new Date(Date.parse(nowIso) - hours * 3600 * 1000).toISOString();
  const rows = db.prepare(`SELECT month FROM yahoo_campaign_coupons WHERE status = 'reconcile_required' AND COALESCE(reconcile_since, updated_at) < ?`).all(cutoff);
  for (const r of rows) {
    db.prepare(`UPDATE yahoo_campaign_coupons SET status = 'manual_intervention', updated_at = ? WHERE month = ? AND status = 'reconcile_required'`)
      .run(nowIso, r.month);
  }
  return rows.map((r) => r.month);
}

/** 送信側が使えるクーポン (issued かつ期間内) */
export function usableCouponFor(db, nowIso = new Date().toISOString()) {
  const rows = db.prepare(`SELECT * FROM yahoo_campaign_coupons WHERE status = 'issued' ORDER BY month DESC`).all();
  const now = Date.parse(nowIso);
  for (const r of rows) {
    const s = Date.parse(`${String(r.coupon_start).replace(/\//g, '-').replace(' ', 'T')}:00+09:00`);
    const e = Date.parse(`${String(r.coupon_end).replace(/\//g, '-').replace(' ', 'T')}:59+09:00`);
    if (Number.isFinite(s) && Number.isFinite(e) && s <= now && now < e && isValidCouponUrl(r.coupon_url)) return r;
  }
  return null;
}
