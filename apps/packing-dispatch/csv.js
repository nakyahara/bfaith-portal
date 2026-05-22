/**
 * NE→ロジザード CSV のコーデック (Shift-JIS / RFC4180)。
 *  - 全フィールド引用・カンマ/改行/二重引用符を含むセルに対応する厳密パーサ
 *  - ヘッダは fail-closed 検証 (B3): 列数とアンカー列名が一致しなければ取込拒否
 *  - 再出力は元の全列をそのまま保持し、指定列だけ差し替えて Shift-JIS で返す
 */
import iconv from 'iconv-lite';
import { EXPECTED_COL_COUNT } from './db.js';

const HEADER_ANCHORS = {
  1: 'ショップ名', 2: '注文番号', 6: '配送先都道府県', 17: '配送会社id',
  34: '配送方法', 55: '品番', 63: '受注数', 76: 'ne配送会社id',
  91: '自動梱包機使用', 92: '配送方法別ヤマト請求顧客コード',
};

// RFC4180 パーサ。CRLF/LF 混在・引用内改行・"" エスケープに対応。
export function parseCsv(text) {
  const rows = [];
  let row = [], field = '', inQuotes = false, i = 0;
  const n = text.length;
  while (i < n) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ',') { row.push(field); field = ''; i++; continue; }
    if (c === '\r') { i++; continue; }
    if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
    field += c; i++;
  }
  // 末尾フィールド/行 (最終行に改行が無い場合)
  if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
  return rows;
}

export function decodeCp932(buffer) {
  return iconv.decode(buffer, 'Shift_JIS');
}

/**
 * @returns { ok, header, rows, errors[] }  rows はヘッダを除くデータ行(配列の配列)
 */
export function parseNeCsv(buffer) {
  const text = decodeCp932(buffer);
  const all = parseCsv(text);
  const errors = [];
  if (all.length === 0) return { ok: false, errors: ['CSV が空です'], header: [], rows: [] };
  const header = all[0];

  if (header.length !== EXPECTED_COL_COUNT) {
    errors.push(`列数が想定外です (期待 ${EXPECTED_COL_COUNT} 列 / 実際 ${header.length} 列)。NE の出力形式が変わった可能性があります。`);
  }
  for (const [idx, name] of Object.entries(HEADER_ANCHORS)) {
    const actual = header[idx];
    if (actual !== name) {
      errors.push(`列${idx} は「${name}」のはずですが「${actual ?? '(無し)'}」でした。`);
    }
  }
  // データ行の列数チェック (壊れ行検出)
  const rows = all.slice(1).filter((r) => !(r.length === 1 && r[0] === '')); // 末尾空行除外
  const badRows = [];
  rows.forEach((r, i) => { if (r.length !== header.length) badRows.push(i + 2); });
  if (badRows.length) {
    errors.push(`列数が揃わない行があります (行 ${badRows.slice(0, 5).join(', ')}${badRows.length > 5 ? ' …' : ''})。`);
  }
  return { ok: errors.length === 0, errors, header, rows };
}

// セルの CSV エスケープ (全フィールド引用・式インジェクション無害化)
function cell(v) {
  let s = String(v == null ? '' : v);
  return '"' + s.replace(/"/g, '""') + '"';
}

/**
 * header + rows(配列の配列) を Shift-JIS の CSV バッファにする。CRLF 区切り。
 */
export function buildNeCsv(header, rows) {
  const lines = [header.map(cell).join(',')];
  for (const r of rows) lines.push(r.map(cell).join(','));
  const text = lines.join('\r\n') + '\r\n';
  return iconv.encode(text, 'Shift_JIS');
}
