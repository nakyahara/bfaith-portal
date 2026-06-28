/**
 * 仕入れ先向け売れ筋共有 — CSV 生成（社内DL・公開DL 共通）
 *
 * 商品サマリ行 + その出品(ページ)別明細行を 1 ファイルに出力。
 * UTF-8 BOM 付き（Excel 文字化け回避）。CSV インジェクション対策あり。
 */

export function csvCell(s) {
  let v = String(s ?? '');
  // 先頭の空白/タブ/改行を挟んだ後の =,+,-,@ も Excel 等で式評価されるため ' を前置
  // (例: " =cmd()", "\t=HYPERLINK(...)" は素朴な /^[=+\-@]/ をすり抜ける)
  if (/^[\s]*[=+\-@]/.test(v)) v = `'${v}`;
  return /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}

// 行を 1 本化。全セルを必ず csvCell に通す（フィールド単位のエスケープ漏れを構造的に防ぐ）。
function row(cells) {
  return cells.map(csvCell).join(',');
}

export function buildCsv(report, mallLabels) {
  const header = ['種別', 'モール', '商品コード/出品ID', '名称', 'FBA', '販売件数', 'ピース数', '売上(税込)', '最終販売日'];
  const lines = [row(header)];
  const products = report.products || [];
  for (const p of products) {
    lines.push(row([
      '商品', '合計', p.ne_code, p.product_name, '',
      '', p.pieces, p.sales, p.lastSold || '',
    ]));
    for (const L of (p.listings || [])) {
      lines.push(row([
        '出品', (mallLabels[L.mall] || L.mall), L.listingId, L.listingName,
        L.mall === 'amazon' ? (L.is_fba ? 'FBA' : 'FBM') : '',
        L.sold, L.pieces, L.sales, '',
      ]));
    }
  }
  return '﻿' + lines.join('\r\n') + '\r\n';
}
