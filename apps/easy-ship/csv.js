/**
 * 依存なしのCSVパーサ/シリアライザ (RFC4180準拠)。
 * easy-ship-helper リポジトリ packages/shared/src/csv.ts から移植 (Codexレビュー済み)。
 * - 不正な構文 (閉じ忘れ・フィールド途中の引用符・閉じ引用符後の余分な文字) は
 *   行番号付きの CsvParseError を投げる (黙って別解釈しない)
 */
export class CsvParseError extends Error {
  constructor(line, detail) {
    super(`CSVの${line}行目が不正です: ${detail}`);
    this.line = line;
  }
}

export function parseCsv(text) {
  const bom = String.fromCharCode(0xfeff);
  const s = text.startsWith(bom) ? text.slice(1) : text;
  const rows = [];
  let row = [];
  let field = '';
  let line = 1;
  let state = 'fieldStart'; // fieldStart | unquoted | quoted | afterQuote

  const endField = () => {
    row.push(field);
    field = '';
    state = 'fieldStart';
  };
  const endRow = () => {
    endField();
    rows.push(row);
    row = [];
  };

  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (state === 'quoted') {
      if (c === '"') {
        if (s[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          state = 'afterQuote';
        }
      } else {
        if (c === '\n') line++;
        field += c;
      }
      continue;
    }
    if (c === ',') {
      endField();
      continue;
    }
    if (c === '\r' || c === '\n') {
      if (c === '\r' && s[i + 1] === '\n') i++;
      endRow();
      line++;
      continue;
    }
    if (state === 'afterQuote') {
      throw new CsvParseError(line, '閉じ引用符の後に余分な文字があります');
    }
    if (c === '"') {
      if (state === 'fieldStart') {
        state = 'quoted';
        continue;
      }
      throw new CsvParseError(
        line,
        'フィールドの途中に引用符があります。フィールド全体を引用符で囲んでください',
      );
    }
    field += c;
    state = 'unquoted';
  }

  if (state === 'quoted') throw new CsvParseError(line, '引用符が閉じられていません');
  if (field !== '' || row.length > 0 || state === 'afterQuote') endRow();
  return rows.filter((r) => !(r.length === 1 && (r[0] ?? '').trim() === ''));
}

function toCsvValue(v) {
  return /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}

export function toCsv(rows) {
  return rows.map((r) => r.map(toCsvValue).join(',')).join('\r\n') + '\r\n';
}

/**
 * Excelで開く用途向けの数式インジェクション対策。
 * 先頭 (空白を挟んでも) が = + - @ のセルに ' を前置して数式評価を防ぐ。
 * 再インポート用のエクスポートには適用しないこと (データが変わるため)。
 */
export function sanitizeExcelCell(v) {
  return /^\s*[=+\-@]/.test(v) || /^[\t\r]/.test(v) ? `'${v}` : v;
}

/** CSVの is_active 列: true/false/1/0/空(=true)。不正値は null を返す */
export function parseCsvBool(raw) {
  const v = String(raw ?? '').trim().toLowerCase();
  if (v === '' || v === 'true' || v === '1') return true;
  if (v === 'false' || v === '0') return false;
  return null;
}

export const CSV_COLUMNS = [
  'sku',
  'package_size_code',
  'package_size_label',
  'amazon_option_value',
  'is_active',
  'note',
];
