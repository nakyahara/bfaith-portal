/**
 * 倉庫CSV (ロジザード在庫CSV) のパース+検証 (総点検 P0-1)
 *
 * 従来は素朴な split(',') + フェイルオープンな文字コード判定で、
 * 商品名にカンマを含む行の列ズレや誤ファイルアップロードが
 * 無警告で倉庫在庫の全置換 (=全推奨計算の汚染) に直結していた。
 * RFC4180 パーサ (picking-csv.js) を流用し、ヘッダ・行単位で検証する。
 */
import { parseCsv, decodeCsvBuffer } from './picking-csv.js';

/**
 * @param {Buffer} buffer アップロードされたCSVのバッファ
 * @returns {{ items: object[], invalidRows: {line:number, reason:string}[] } | { error: string }}
 */
export function parseWarehouseCsv(buffer) {
  const { text, encoding } = decodeCsvBuffer(buffer);
  const rows = parseCsv(text).filter(r => r.some(c => c && c.trim() !== ''));
  if (rows.length < 2) return { error: 'CSVが空です (ヘッダ+データ行が必要)' };

  const headers = rows[0].map(h => h.trim());
  // 必須ヘッダ: いずれかのグループが1つも無ければロジザード在庫CSVではないと判断して中止
  const requiredGroups = [
    ['商品ID', '商品コード'],
    ['在庫数(引当数を含む)', '在庫数'],
  ];
  const missing = requiredGroups.filter(g => !g.some(h => headers.includes(h)));
  if (missing.length > 0) {
    return { error: `必須ヘッダが見つかりません: ${missing.map(g => g.join(' または ')).join(' / ')}。` +
      `ロジザードの在庫CSVか、文字コードが正しいか確認してください (判定: ${encoding})` };
  }

  // 数値セル: "1,234" のカンマ区切りを許容し、整数以外は不正行扱い
  const toInt = (s) => {
    if (s === '' || s == null) return null;
    const n = Number(String(s).replace(/,/g, '').trim());
    return Number.isInteger(n) ? n : NaN;
  };

  const items = [];
  const invalidRows = [];
  for (let i = 1; i < rows.length; i++) {
    const cols = rows[i];
    const obj = {};
    headers.forEach((h, j) => { obj[h] = (cols[j] ?? '').trim(); });

    const code = obj['商品ID'] || obj['商品コード'] || '';
    if (!code) { invalidRows.push({ line: i + 1, reason: '商品IDが空' }); continue; }
    if (cols.length !== headers.length) {
      invalidRows.push({ line: i + 1, reason: `列数不一致 (ヘッダ${headers.length}列/行${cols.length}列)` });
      continue;
    }

    const qty = toInt(obj['在庫数(引当数を含む)'] ?? obj['在庫数']);
    const reserved = toInt(obj['引当数']) ?? 0;
    if (qty == null || Number.isNaN(qty) || qty < 0) { invalidRows.push({ line: i + 1, reason: `在庫数が不正 (${obj['在庫数(引当数を含む)'] ?? obj['在庫数'] ?? ''})` }); continue; }
    if (Number.isNaN(reserved) || reserved < 0) { invalidRows.push({ line: i + 1, reason: `引当数が不正 (${obj['引当数']})` }); continue; }
    if (reserved > qty) { invalidRows.push({ line: i + 1, reason: `引当数(${reserved})が在庫数(${qty})を超過` }); continue; }

    // ロジザードCSV実データ列名に対応
    const block = obj['ブロック略称'] || '';
    const location = obj['ロケ'] || obj['ロケーション'] || '';

    // 最終入荷日: YYYYMMDD → YYYY-MM-DD に正規化
    const rawNyuka = obj['最終入荷日'] || '';
    const lastArrivalDate = rawNyuka.length === 8
      ? `${rawNyuka.slice(0,4)}-${rawNyuka.slice(4,6)}-${rawNyuka.slice(6,8)}`
      : rawNyuka;

    const blockAllocOrder = toInt(obj['ブロック引当順']);
    items.push({
      logizard_code: code,
      product_name: obj['商品名'] || '',
      location: location,
      block: block,
      quantity: qty,
      reserved: reserved,
      available_qty: qty - reserved,
      expiry_date: obj['有効期限'] || '',
      lot_no: obj['ロット'] || '',
      barcode: obj['バーコード'] || '',
      is_y_location: (block === 'YYY' || location.toUpperCase().startsWith('Y')) ? 1 : 0,
      last_arrival_date: lastArrivalDate,
      location_biz_type: obj['ロケ業務区分'] || '',
      block_alloc_order: (blockAllocOrder == null || Number.isNaN(blockAllocOrder)) ? 9999 : blockAllocOrder,
      biz_priority: obj['指定した業態を優先して取り置く'] || '',
    });
  }

  if (items.length === 0) {
    return { error: `有効な行が0件です (不正行${invalidRows.length}件)。ファイル内容を確認してください` };
  }
  const total = items.length + invalidRows.length;
  if (invalidRows.length / total > 0.1) {
    const samples = invalidRows.slice(0, 5).map(r => `${r.line}行目: ${r.reason}`).join(' / ');
    return { error: `不正行が${invalidRows.length}/${total}件 (10%超) のため中止しました。例: ${samples}` };
  }
  return { items, invalidRows };
}
