/**
 * 商品別 想定利益 — 世代の内容ハッシュ (送信側・受信側で共通)
 *
 * 🚨 ここは**1か所にしかない**。送信側と受信側で別々に書くと、
 *    並び順や対象列がわずかに違うだけで、正常な世代が永久に公開できなくなる。
 *    実際 R5 で「JS の localeCompare」と「SQLite の ORDER BY (BINARY)」が
 *    食い違い、SKU に大文字小文字が混ざると必ず不一致になる状態だった:
 *      JS      : rakuten1a , rakuten1B
 *      SQLite  : rakuten1B , rakuten1a
 *
 * 対象列 (Codex R5-2):
 *   利益額だけでなく、**順位・対象集合・失効判定を変える列すべて**を含める。
 *   expense_scope_version が書き換えられると FBA が自社出荷のランキングに混入し、
 *   *_valid_until が書き換えられると表示時の失効判定が変わる。
 */
import crypto from 'crypto';

/** ハッシュに含める列。ここに無い列は「変えても検出できない」ことになる */
export const HASH_COLUMNS = [
  // 同一性
  'mall', 'shop_id', 'mall_item_key', 'ne_code', 'fulfillment',
  // 金額 (符号を決める)
  'expected_profit', 'expected_margin_rate',
  'revenue_ex_tax', 'cost_ex_tax', 'shipping_total_ex_tax', 'fba_fee_ex_tax', 'fee_total_ex_tax',
  // 対象集合・順位を決める
  'expense_scope_version', 'rank_eligible', 'calculation_status', 'listing_status',
  // 表示時の失効判定を変える
  'listing_enum_valid_until', 'price_valid_until', 'fee_valid_until',
  'cost_valid_until', 'shipping_master_valid_until',
  // 入力ごとの状態 (適格判定に使う)
  'listing_enum_status', 'price_status', 'fee_status', 'cost_status',
  'shipping_master_status', 'shipping_revenue_status', 'scenario_fit',
];

/**
 * 行の並び順。
 * 🚨 JS 側で明示的に決める。SQLite の ORDER BY に頼らない
 *    (照合順序が環境や列定義で変わるため)。
 *    比較は「コードポイント順」に固定する (localeCompare は使わない)
 */
export function sortKey(row) {
  return `${row.mall}${row.shop_id}${row.mall_item_key}`;
}

function byCodePoint(a, b) {
  const ak = sortKey(a);
  const bk = sortKey(b);
  if (ak < bk) return -1;
  if (ak > bk) return 1;
  return 0;
}

/** 行を正規化する (undefined と null を同じ扱いにし、列の順序も固定する) */
export function normalizeRow(row) {
  const out = {};
  for (const col of HASH_COLUMNS) {
    const v = row[col];
    out[col] = v === undefined ? null : v;
  }
  return out;
}

/**
 * 行の配列から内容ハッシュを作る。
 * 送信側 (メモリ上の行) も受信側 (DB から読んだ行) も、必ずこの関数を通す。
 */
export function hashRows(rows) {
  const h = crypto.createHash('sha256');
  for (const r of [...rows].sort(byCodePoint)) {
    h.update(JSON.stringify(normalizeRow(r)));
    h.update('\n');
  }
  return h.digest('hex');
}

/** DB に保存済みの世代からハッシュを作る (並び替えは JS 側で行う) */
export function hashGeneration(db, generationId) {
  const cols = HASH_COLUMNS.join(', ');
  const rows = db.prepare(
    `SELECT ${cols} FROM mart_listing_expected_profit WHERE generation_id = ?`).all(generationId);
  return hashRows(rows);
}
