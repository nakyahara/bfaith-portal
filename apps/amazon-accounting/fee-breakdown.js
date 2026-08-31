/**
 * 手数料内訳（説明別）の判定ルール — 純粋関数（DB・express 依存なし。テストから直接 import する）
 *
 * SKUを持たない手数料行を CSV「説明」列で判別し、その行の「合計」列を合算してセグメント表の右端に列として見せる。
 * 実データ (2026-06/07) で判明: 説明の文言は月で変わる (Easy Ship は 6月 2種の別名 / 7月 統合名)、
 * 金額が入る列も月で変わる (手数料 / トランザクション他 / FBA手数料 / その他) → 部分一致 + 合計列の合算が唯一安定。
 * 判定は上から順に排他 (「FBA長期在庫保管手数料」は「在庫保管手数料」を含むので長期を先に)。
 * 既存の各列・合計には既に含まれている金額の抜き出しであり、集計値は一切変えない (表示の追加のみ)。
 * 要件定義: システム設計\Amazon売上集計_手数料内訳表示_要件定義_20260831.md
 */

// 判定ルール（正規化後の説明に対して上から順に排他）
export const FEE_BREAKDOWN_RULES = [
  { label: 'FBA長期在庫保管手数料', test: d => d.includes('長期在庫保管手数料') },
  { label: 'FBA在庫保管手数料',     test: d => d.includes('在庫保管手数料') },
  // Easy Ship は「含む」の部分一致（ユーザー決定 2026-08-31 Q1）。どの説明が入ったかは「SKUなし行の説明別一覧」で確認できる
  { label: 'Amazon Easy Ship料金',  test: d => d.includes('easy ship') },
];

// 表示順（セグメント表・CSV の右端に追加する列）
export const FEE_COLUMNS = ['Amazon Easy Ship料金', 'FBA在庫保管手数料', 'FBA長期在庫保管手数料'];

/**
 * 説明の正規化: NFKC（全角英数・全角空白・NBSP → 半角/通常空白）→ 連続空白を1つに → 前後空白と
 * 末尾のコロン (取り消し/訂正行は "FBA在庫保管手数料:" と付く) を除去 → 小文字化
 */
export function normalizeFeeDesc(desc) {
  return String(desc || '')
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/[:：\s]+$/, '')
    .toLowerCase();
}

/**
 * 行がどの手数料内訳に該当するかをラベルで返す（該当なしは null）。
 * SKU空欄行のみ対象 (注文・返金行の説明は商品名なので誤判定を避ける)。振込み (解決方法 'skip') は既存集計と同様に対象外。
 */
export function classifyFeeRow(row) {
  if (!row || row.解決方法 === 'skip') return null;
  if (row.sku) return null;
  const d = normalizeFeeDesc(row.説明);
  if (!d) return null;
  const rule = FEE_BREAKDOWN_RULES.find(r => r.test(d));
  return rule ? rule.label : null;
}

/**
 * 表示用の合計: CSVの「合計」から手数料内訳3列を差し引いた額（2026-09-01 代表指示: 3列を「その他」と「合計」の間に置き、合計からその分を引く）。
 * 保存データ (by_segment) の 合計 は CSV の合計のまま（税率別集計・MF・管理会計と整合）。内訳3列 + netTotal = 合計。
 * 内訳キーが無い行（本機能より前に確定した月）はそのまま 合計 を返す。
 */
export function netTotal(row, feeCols = FEE_COLUMNS) {
  if (!row) return 0;
  return (Number(row['合計']) || 0) - feeCols.reduce((s, c) => s + (Number(row[c]) || 0), 0);
}
