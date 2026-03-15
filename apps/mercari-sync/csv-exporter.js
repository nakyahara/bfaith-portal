/**
 * メルカリShops 一括登録CSV出力モジュール
 * Shift_JIS (cp932) エンコーディング対応
 */
import iconv from 'iconv-lite';

const MAX_IMAGES = 20;
const MAX_SKUS = 10;
export const MAX_ROWS_PER_CSV = 1000;

export function buildCsvColumns() {
  const columns = [];
  for (let i = 1; i <= MAX_IMAGES; i++) columns.push(`商品画像名_${i}`);
  columns.push('商品名', '商品説明');
  for (let i = 1; i <= MAX_SKUS; i++) {
    columns.push(`SKU${i}_種類`, `SKU${i}_在庫数`, `SKU${i}_商品管理コード`, `SKU${i}_JANコード`);
  }
  columns.push('ブランドID', '販売価格', 'カテゴリID', '商品の状態');
  columns.push('配送方法', '発送元の地域', '発送までの日数');
  columns.push('商品ステータス', '配送料の負担');
  columns.push('送料ID', 'メルカリBiz配送_クール区分');
  return columns;
}

function escapeCsvField(value) {
  const s = String(value ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

export function writeCsv(rows) {
  const columns = buildCsvColumns();
  const lines = [];

  // Header
  lines.push(columns.map(escapeCsvField).join(','));

  // Rows
  for (const row of rows) {
    const line = columns.map(col => escapeCsvField(row[col] ?? '')).join(',');
    lines.push(line);
  }

  const csvText = lines.join('\r\n') + '\r\n';
  // Encode to Shift_JIS (cp932)
  return iconv.encode(csvText, 'cp932');
}

function decodeCsvContent(buffer) {
  // Try cp932 first, then UTF-8
  for (const encoding of ['cp932', 'utf-8']) {
    try {
      const text = iconv.decode(buffer, encoding);
      // Basic validity check
      if (text.includes(',') || text.includes('\n')) return text;
    } catch { /* try next */ }
  }
  return null;
}

function parseCsvText(text) {
  // Simple CSV parser that handles quoted fields
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };

  const parseRow = (line) => {
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
        else if (ch === '"') { inQuotes = false; }
        else { current += ch; }
      } else {
        if (ch === '"') { inQuotes = true; }
        else if (ch === ',') { fields.push(current); current = ''; }
        else { current += ch; }
      }
    }
    fields.push(current);
    return fields;
  };

  const headers = parseRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const fields = parseRow(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => { obj[h] = fields[idx] || ''; });
    rows.push(obj);
  }
  return { headers, rows };
}

export function parseMercariExportCsv(buffer) {
  const text = decodeCsvContent(buffer);
  if (!text) return new Set();

  const { rows } = parseCsvText(text);
  const codes = new Set();
  for (const row of rows) {
    for (let i = 1; i <= MAX_SKUS; i++) {
      const code = (row[`SKU${i}_商品管理コード`] || '').trim();
      if (code) codes.add(code);
    }
  }
  return codes;
}

export function parseGenericCsvForCodes(buffer, columnName = '') {
  const codes = new Set();
  const messages = [];

  const text = decodeCsvContent(buffer);
  if (!text) return { codes, messages: ['ファイルのエンコーディングを認識できません'] };

  const { headers, rows } = parseCsvText(text);
  if (headers.length === 0) return { codes, messages: ['CSVのヘッダー行が見つかりません'] };

  let targetColumns = [];

  if (columnName) {
    if (headers.includes(columnName)) {
      targetColumns = [columnName];
    } else {
      return { codes, messages: [`指定された列名 '${columnName}' がCSVに見つかりません`, `利用可能な列名: ${headers.join(', ')}`] };
    }
  } else {
    // メルカリ形式チェック
    const skuCols = [];
    for (let i = 1; i <= MAX_SKUS; i++) {
      if (headers.includes(`SKU${i}_商品管理コード`)) skuCols.push(`SKU${i}_商品管理コード`);
    }
    if (skuCols.length) {
      targetColumns = skuCols;
      messages.push(`メルカリShops CSV形式を検出（${skuCols.length}列）`);
    } else {
      const candidates = ['商品管理コード', '商品コード', '管理番号', '商品管理番号', 'item_code', 'sku_code', 'product_code', 'SKU管理番号'];
      for (const cand of candidates) {
        if (headers.includes(cand)) {
          targetColumns = [cand];
          messages.push(`列名 '${cand}' を自動検出しました`);
          break;
        }
      }
    }

    if (targetColumns.length === 0) {
      return { codes, messages: ['商品コードを含む列を自動検出できませんでした', `利用可能な列名: ${headers.join(', ')}`, '列名を指定してアップロードし直してください'] };
    }
  }

  for (const row of rows) {
    for (const col of targetColumns) {
      const code = (row[col] || '').trim();
      if (code) codes.add(code);
    }
  }
  messages.push(`${codes.size}件の商品コードを抽出しました（${rows.length}行から）`);
  return { codes, messages };
}
