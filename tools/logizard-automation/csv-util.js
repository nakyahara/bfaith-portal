/**
 * CSVパース共通処理。auto-zaiko.js から利用し、test-csv-util.js で単体検証する。
 *
 * 途中切断の検出が主目的なので、閉じていない引用符と「改行で終わっていない末尾」を
 * 異常として報告できるようにしてある (行数だけの検査では部分ファイルを弾けないため)。
 */

// 引用符内のカンマ・改行・"" エスケープに対応した素朴なCSVパーサ
export function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else { inQuotes = false; }
      } else {
        field += c;
      }
      continue;
    }
    if (c === '"') { inQuotes = true; }
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\r') { /* CRLFのCRは無視 */ }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else { field += c; }
  }
  if (inQuotes) {
    throw new Error('CSVの引用符が閉じていません (ダウンロードが途中で切れた可能性があります)');
  }
  const endedWithNewline = field === '' && row.length === 0;
  if (!endedWithNewline) { row.push(field); rows.push(row); }
  return { rows, endedWithNewline };
}
