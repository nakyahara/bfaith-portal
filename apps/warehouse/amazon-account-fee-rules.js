/**
 * amazon-account-fee-rules.js — Amazon のアカウント単位の手数料の分け方 (1 か所)。
 *   SQLite の月の手数料の build (rebuild-amazon-account-fees.js・SQL) と、Company DB へ財務を送る送り手 (apps/company-db/push/amazon-finance.mjs・JS) の両方が使う。
 *   2026-09-29 (F2b-2) に rebuild-amazon-account-fees.js から出した (中身は変えていない)。
 */

// transaction_type → fee_type mapping (2026-07-06 実データの distinct から作成)
// 🚨 2026-09-28: Amazon が決済の取引の名前を変えていた (7 月から保管料・長期保管料、6 月から返送料) のに古い名前しか拾わず、
//   7〜9 月の保管料 (月 30〜66 万円)・長期保管料 (月 約 10 万円)・返送料が 0 = アカウント全体の利益が月 40〜80 万円多く出ていた。
//   新しい名前を足した + 分けられない SKU なしの取引が出たら最後の行を ⚠️ にする (daily-sync で「全部 OK」に数えない = 次に名前が変わったら気づく)
export const FEE_TYPE_RULES = [
  // [fee_type, 完全一致の名前, 前方一致の名前]
  ['storage', ['Storage Fee', 'Storage Fee - Correction', 'Storage Fee - Reversal'], ['FBA Inventory Storage Fee']],   // 2026-07〜 FBA Inventory Storage Fee
  ['long_term_storage', ['StorageRenewalBilling'], ['FBA Long Term Storage Fee']],                                        // 2026-07〜 FBA Long Term Storage Fee
  ['removal', ['RemovalComplete'], ['FBA Removal Order']],                                                                // 2026-06〜 FBA Removal Order: Return Fee
  ['inbound_defect', [], ['Inbound Defect Fee']],
  ['low_inventory', [], []],   // '%LowInventory%' / '%Low-Inventory%' (LIKE)
  ['subscription', ['Subscription Fee'], []],
  // 🆕 2026-09-28: Easy Ship の配送料 (注文ごと・SKU なし)。今までどこにも入れておらず、Amazon 分析の確定利益が月 130〜230 万円多く出ていた
  //   (カスタム経費も 0 件・管理会計は代表指示 2026-09-01 で「Easy Ship運賃」として数えている)。金額は Amazon の符号・税込のまま (保管料と同じ)。
  //   金額の列は月で違う (古い月 = other-amount / 新しい月 = item-related-fee-amount) → 両方を足す
  ['easy_ship', ['Amazon Easy Ship Charges'], []],
  // 🆕 2026-09-29: 手数料の調整・払いすぎた手数料の返還 (Amazon からの戻り = 正。中原さん「3」)。今まで NOT_ACCOUNT_FEE でどこにも入れていなかった
  //   (SKU のある行は日次の財務の reversal_reimbursement に入る = ここは SKU なしの行だけ = 二重にならない)。月の最終利益では ÷1.1 (ほかの手数料と同じ)
  ['other_account_fee', ['Fee Adjustment', 'Overpaid Fees Adjustment'], []],
];
// アカウント単位の手数料に入れない SKU なしの取引 (今までも入れていない。これ以外の SKU なしの取引が出たら ⚠️)
//   預かり金の出し入れ (Current / Previous Reserve = 相殺) / 調整 (Goodwill・Retrocharge・ServiceFee・BuyerRecharge)
//   (Easy Ship の料金は 2026-09-28 から easy_ship・手数料の調整 (Fee Adjustment / Overpaid) は 2026-09-29 から other_account_fee として入れる)
export const NOT_ACCOUNT_FEE = ['Current Reserve Amount', 'Previous Reserve Amount Balance', 'Goodwill Concession',
  'Order_Retrocharge', 'Refund_Retrocharge', 'ServiceFee', 'BuyerRecharge'];
// 確かめた名前 (本番の決済に出た名前・2026-09-28)。前方一致で拾ったがここに無い名前 = 金額は入れた上で ⚠️ (人が確かめてここに足す。Codex #1515 R1)
//   Inbound Defect Fee… / LowInventory は最初 (2026-07-06) から名前の揺れを前提にした型 = 型ごと確かめ済み
export const CONFIRMED_NAMES = ['Storage Fee', 'Storage Fee - Correction', 'Storage Fee - Reversal', 'FBA Inventory Storage Fee',
  'StorageRenewalBilling', 'FBA Long Term Storage Fee', 'RemovalComplete', 'FBA Removal Order: Return Fee', 'Subscription Fee', 'Amazon Easy Ship Charges',
  'Fee Adjustment', 'Overpaid Fees Adjustment'];

// ── JS の判定 (SQL と同じ決め) ──
// 🚨 SQLite の LIKE は ASCII の大文字小文字を区別しない / IN (完全一致) は区別する。SQL の CASE は FEE_TYPE_RULES の順に最初に当たったもの
const asciiLower = (s) => String(s).replace(/[A-Z]/g, (c) => c.toLowerCase());
const likePrefixJs = (tx, p) => asciiLower(tx).startsWith(asciiLower(p));
const isLowInventory = (tx) => asciiLower(tx).includes('lowinventory') || asciiLower(tx).includes('low-inventory');
const matchRule = (tx, exact, prefix) => exact.includes(tx) || prefix.some((p) => likePrefixJs(tx, p));

/** SKU なしの行の手数料の種類 (SQL の FEE_FILTER_SQL と FEE_CASE_SQL と同じ)。手数料でなければ null */
export function classifyAccountFee(tx) {
  if (tx == null) return null;
  const isFee = FEE_TYPE_RULES.some(([t, e, p]) => (t === 'low_inventory' ? isLowInventory(tx) : matchRule(tx, e, p)));
  if (!isFee) return null;
  for (const [t, e, p] of FEE_TYPE_RULES) {
    if (t === 'low_inventory' ? isLowInventory(tx) : matchRule(tx, e, p)) return t;
  }
  return 'other_account_fee';   // SQL の ELSE (ここには来ない)
}
