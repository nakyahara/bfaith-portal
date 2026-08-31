/**
 * Amazon ペイメントレポートCSVの解析と集計 — 純粋関数（DB・express 依存なし。テストから直接 import する）
 *
 *   parsePaymentCsvText(text) : CSVテキスト → 行オブジェクト配列 (ヘッダー行の動的検出・カンマ区切り金額の数値化)
 *   aggregate(resolvedRows)   : SKU解決済み行 → 税率別 / セグメント別 / 除外 / MF税込 / 手数料内訳 / SKUなし行一覧
 *
 * 集計ロジックの正本は システム設計\Amazon売上集計ツール_引き継ぎ.md、手数料内訳は fee-breakdown.js を参照。
 */
import { FEE_COLUMNS, classifyFeeRow } from './fee-breakdown.js';

// ─── ペイメントレポートCSVの解析 ───
// 先頭のメタ行数は Amazon 側で増減するため、"日付/時間" と "SKU" を含む行をヘッダーとして動的に検出する。
// 戻り値: { headerIdx, rows }。headerIdx < 0 はヘッダー未検出 (rows は空)。
// ※ /upload と fee-breakdown.test.mjs の両方がこの関数を使う (テストと本番でパーサを共有する)
export function parsePaymentCsvText(text) {
  const lines = text.split(/\r?\n/);

  // 列ヘッダー行（"日付/時間" と "SKU" を含む行）を検出して、その次行から読み始める
  // ※Amazonがメタ行を増減してもヘッダー検出で吸収できるよう動的に判定
  let headerIdx = -1;
  for (let i = 0; i < Math.min(lines.length, 30); i++) {
    if (lines[i].includes('日付/時間') && lines[i].includes('SKU')) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) return { headerIdx: -1, rows: [] };

  const num = v => { const n = parseFloat((v || '').replace(/"/g, '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };
  const clean = v => (v || '').replace(/^"|"$/g, '').trim();

  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    // CSV行をパース（ダブルクォート対応）
    const cols = [];
    let current = '', inQ = false;
    for (let j = 0; j < line.length; j++) {
      const ch = line[j];
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cols.push(current); current = ''; }
      else current += ch;
    }
    cols.push(current);

    const date = clean(cols[0]);
    if (!date) continue;
    // 日付から時刻部分を除去
    const dateOnly = date.replace(/ .+$/, '');

    rows.push({
      日付: dateOnly,
      決済番号: clean(cols[1]),
      トランザクション種類: clean(cols[2]),
      注文番号: clean(cols[3]),
      sku: clean(cols[4]).toLowerCase(),
      説明: clean(cols[5]),
      数量: parseInt(clean(cols[6])) || 0,
      商品売上: num(cols[13]),
      商品の売上税: num(cols[14]),
      配送料: num(cols[15]),
      配送料の税金: num(cols[16]),
      ギフト包装手数料: num(cols[17]),
      ギフト包装の税金: num(cols[18]),
      Amazonポイント費用: num(cols[19]),
      プロモーション割引額: num(cols[20]),
      プロモーション割引の税金: num(cols[21]),
      手数料: num(cols[23]),
      FBA手数料: num(cols[24]),
      トランザクション他: num(cols[25]),
      その他: num(cols[26]),
      合計: num(cols[27]),
    });
  }
  return { headerIdx, rows };
}

// ─── 集計 ───

export function aggregate(resolvedRows) {
  const columns = ['商品売上', '商品の売上税', '配送料', '配送料の税金',
    'ギフト包装手数料', 'ギフト包装の税金', 'Amazonポイント費用',
    'プロモーション割引額', 'プロモーション割引の税金', '手数料', 'FBA手数料',
    'トランザクション他', 'その他', '合計'];

  function emptyRow(withFee) {
    const r = {};
    columns.forEach(c => r[c] = 0);
    // 手数料内訳 (合計列の抜き出し。上の各列に含まれる金額の内訳であり加算しない) はセグメント別だけに持たせる。
    // 税率別・除外(輸出)には生やさない (DB の by_tax / excluded JSON に無関係なキーが乗るのを防ぐ)
    if (withFee) FEE_COLUMNS.forEach(c => r[c] = 0);
    r.原価合計 = 0;
    r.行数 = 0;
    return r;
  }

  function addRow(target, row) {
    columns.forEach(c => target[c] += row[c] || 0);
    const feeKey = classifyFeeRow(row);
    if (feeKey && Object.prototype.hasOwnProperty.call(target, feeKey)) target[feeKey] += row.合計 || 0;
    target.原価合計 += (row.原価 || 0) * (row.数量 || 1);
    target.行数++;
  }

  // 税率別
  const byTax = { '10': emptyRow(), '8': emptyRow() };

  // セグメント別（1〜3 + other。4=輸出は除外）
  const bySegment = { '1': emptyRow(true), '2': emptyRow(true), '3': emptyRow(true), 'other': emptyRow(true) };

  // 除外セグメント（4=輸出）
  const excluded = { '4': emptyRow() };

  // 「その他/未分類」に入った行の明細を記録
  const otherDetails = new Map();
  // SKUなし行の説明別一覧 (トランザクションの種類 × 説明 原文)。手数料内訳の根拠と、Amazonが文言を変えた月の検知用
  const noSkuDetails = new Map();

  for (const row of resolvedRows) {
    if (row.解決方法 === 'skip') continue; // 振込み

    if (!row.sku) {
      const nk = (row.トランザクション種類 || '') + '||' + (row.説明 || '');
      const ne = noSkuDetails.get(nk) || {
        トランザクション種類: row.トランザクション種類 || '',
        説明: row.説明 || '',
        判定: classifyFeeRow(row) || '',
        行数: 0,
        合計: 0,
      };
      ne.行数++;
      ne.合計 += row.合計 || 0;
      noSkuDetails.set(nk, ne);
    }

    // 税率別（税率未登録は集計しない）
    if (row.税率 === 10 || row.税率 === 8) {
      addRow(byTax[String(row.税率)], row);
    } else if (row.解決方法 === 'no_sku' || row.解決方法 === 'unresolved' || row.解決方法 === 'adjustment_no_master') {
      // SKUなし・未解決・調整(照合対象外)は10%扱い（手数料/弁償金等は税率情報がない）
      addRow(byTax['10'], row);
    }
    // 税率未登録（商品は解決済みだが税率がない）→ 税率別集計に含めない

    // セグメント別
    const segKey = row.売上分類 ? String(row.売上分類) : 'other';

    if (excluded[segKey]) {
      // 4=輸出 → 除外集計
      addRow(excluded[segKey], row);
    } else if (bySegment[segKey]) {
      addRow(bySegment[segKey], row);
    } else {
      addRow(bySegment['other'], row);
    }

    // 「その他」に入った行の明細を記録
    if (!row.売上分類 && !excluded[segKey]) {
      const detailKey = row.商品コード || row.sku || '_no_sku_' + (row.トランザクション種類 || '');
      const existing = otherDetails.get(detailKey) || {
        sku: row.sku || '',
        商品コード: row.商品コード || '',
        商品名: row.説明 || '',
        トランザクション種類: row.トランザクション種類 || '',
        解決方法: row.解決方法,
        商品売上: 0,
        合計: 0,
        数量: 0,
        count: 0,
      };
      existing.商品売上 += row.商品売上 || 0;
      existing.合計 += row.合計 || 0;
      existing.数量 += row.数量 || 0;
      existing.count++;
      otherDetails.set(detailKey, existing);
    }
  }

  // MF連携用 税込み集計行
  const t10 = byTax['10'];
  const t8 = byTax['8'];
  const mfColumns = ['商品売上(10%)', '商品売上(8%)', '配送料', 'ギフト包装手数料',
    'Amazonポイントの費用', 'プロモーション割引額', '手数料', 'FBA手数料',
    'トランザクションに関するその他の手数料+その他', '合計', '端数調整'];
  const mfRow = {
    '商品売上(10%)': t10['商品売上'] + t10['商品の売上税'],
    '商品売上(8%)': t8['商品売上'] + t8['商品の売上税'],
    '配送料': t10['配送料'] + t10['配送料の税金'] + t8['配送料'] + t8['配送料の税金'],
    'ギフト包装手数料': t10['ギフト包装手数料'] + t10['ギフト包装の税金'] + t8['ギフト包装手数料'] + t8['ギフト包装の税金'],
    'Amazonポイントの費用': t10['Amazonポイント費用'] + t8['Amazonポイント費用'],
    'プロモーション割引額': t10['プロモーション割引額'] + t10['プロモーション割引の税金'] + t8['プロモーション割引額'] + t8['プロモーション割引の税金'],
    '手数料': t10['手数料'] + t8['手数料'],
    'FBA手数料': t10['FBA手数料'] + t8['FBA手数料'],
    'トランザクションに関するその他の手数料+その他': t10['トランザクション他'] + t10['その他'] + t8['トランザクション他'] + t8['その他'],
    '合計': t10['合計'] + t8['合計'],
  };
  // 端数調整 = 合計 - 他全列の合計
  const mfSubtotal = mfRow['商品売上(10%)'] + mfRow['商品売上(8%)'] + mfRow['配送料']
    + mfRow['ギフト包装手数料'] + mfRow['Amazonポイントの費用'] + mfRow['プロモーション割引額']
    + mfRow['手数料'] + mfRow['FBA手数料'] + mfRow['トランザクションに関するその他の手数料+その他'];
  mfRow['端数調整'] = mfRow['合計'] - mfSubtotal;

  return {
    byTax,
    bySegment,
    excluded,
    otherDetails: [...otherDetails.values()].sort((a, b) => Math.abs(b.商品売上) - Math.abs(a.商品売上)),
    noSkuDetails: [...noSkuDetails.values()].sort((a, b) => Math.abs(b.合計) - Math.abs(a.合計)),
    columns,
    feeColumns: FEE_COLUMNS,
    mfRow,
    mfColumns,
  };
}
