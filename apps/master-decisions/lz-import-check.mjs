/**
 * lz-import-check.mjs — ロジザードの毎日の商品マスタの取込の「押す前」と「押した後」の決まり (純粋・マスタ正本切替 ③c-1b-2b-1a)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 契約 v3」(K1・G・v1 §6)。
 *   validateImportCsv  取り込む CSV の確かめ (見出しが DAILY と完全一致・5 列・CSV として正しい・商品ID の重複なし (文字でも小文字でも)・文字が戻る)
 *   parseImportResult  結果の画面の文字 → 総件数・処理件数・処理不要件数・エラー件数 (カンマ付きも読む。一般的な「完了」の文言は使わない)
 *   judgeImportResult  成功 = 総件数 = CSV の行数・処理件数 + 処理不要件数 = 総件数・エラー件数 0 → imported_unverified /
 *                      件数が合わない・エラー > 0 → partial / 結果が無い・2 つ以上・読めない → unknown
 *   buildLosslessCsv   試験・戻しの資料の CSV を作る (5 列を独立に持つ・文字を「?」に落とさない・作った後に読み直して承認した値と文字の単位で一致) (K1)
 *                      毎日の生成器 (lz-csv.mjs buildLzCsv) = 商品名をふりがなにも入れる・一部の文字を「?」にする は使わない
 */
import iconv from 'iconv-lite';
import { parseCsvBytes } from './lz-compare.mjs';
import { DAILY } from './lz-csv.mjs';

const dec = (b) => iconv.decode(Buffer.from(b), 'cp932');

/**
 * 取り込む CSV の確かめ (押す前・K2 の照らし直しの前提)
 * @returns {{ ok: boolean, reason: string|null, rows: number, table: string[][] }}   table = 見出しを除く行 (各 5 つの文字)
 */
export function validateImportCsv(buf) {
  const bad = (reason, extra = {}) => ({ ok: false, reason, rows: 0, table: [], ...extra });
  const b = Buffer.from(buf || []);
  if (!b.length) return bad('import_csv_empty');
  const P = parseCsvBytes(b);
  if (P.shape.bom) return bad('import_csv_bom');
  if (P.shape.unterminated || P.shape.bare_quote || P.shape.after_quote) return bad('import_csv_broken');
  if (P.shape.lf || P.shape.cr) return bad('import_csv_newline');   // 行の終わりは CRLF だけ (毎日の CSV と同じ)
  // 文字が戻る = バイト → 文字 → バイト で同じ (読めないバイトを「それらしい文字」にして比べない)
  for (const r of P.records) for (const c of r.cells) if (!iconv.encode(dec(c), 'cp932').equals(Buffer.from(c))) return bad('import_csv_encoding');
  const head = (P.records[0] || { cells: [] }).cells.map(dec);
  if (head.length !== DAILY.header.length || head.some((h, i) => h !== DAILY.header[i])) return bad('import_csv_header');
  const body = P.records.slice(1);
  if (!body.length) return bad('import_csv_no_rows');
  if (body.some((r) => r.cells.length !== DAILY.header.length)) return bad('import_csv_row_width');
  const table = body.map((r) => r.cells.map(dec));
  if (table.some((r) => r.some((c) => /[\u0000-\u001f\u007f]/.test(c)))) return bad('import_csv_control');   // セルの中の改行・制御文字
  const seen = new Set(), seenLower = new Set();
  for (const [i, row] of table.entries()) {
    const id = row[0];
    if (id.trim() === '') return bad('import_csv_blank_id', { at: i + 2 });
    if (seen.has(id)) return bad('import_csv_duplicate_id', { at: i + 2 });
    if (seenLower.has(id.toLowerCase())) return bad('import_csv_duplicate_id_case', { at: i + 2 });   // 大文字小文字だけ違う 2 行 (K8: 重複の禁止はそのまま)
    seen.add(id); seenLower.add(id.toLowerCase());
  }
  return { ok: true, reason: null, rows: table.length, table };
}

// 数の後に 数字・小数点・カンマが続かない (「1.5」を 1 と読まない。Codex #1519 R1 High)
const NUM = '([0-9][0-9,]*)(?![0-9.,．])';
const LABELS = Object.freeze(['総件数', '処理件数', '処理不要件数', 'エラー件数']);
// 1 つの結果の表示の中だけで読む (次の「インポート結果」をまたがない = 呼び手が表示ごとに切ってから渡す)
const ONE_RESULT_RE = new RegExp(`^インポート結果[\\s\\S]{0,300}?総件数\\s*[:：]\\s*${NUM}[\\s\\S]{0,80}?処理件数\\s*[:：]\\s*${NUM}[\\s\\S]{0,80}?処理不要件数\\s*[:：]\\s*${NUM}[\\s\\S]{0,80}?エラー件数\\s*[:：]\\s*${NUM}`);
const toInt = (s) => {
  if (!/^[0-9]{1,3}(,[0-9]{3})*$|^[0-9]+$/.test(s)) return null;   // カンマは 3 桁ごとだけ
  const n = Number(s.replace(/,/g, ''));
  return Number.isSafeInteger(n) ? n : null;
};

/**
 * 結果の画面の文字を読む。呼び手は「今回押した後に新しく出た結果の表示」だけを渡す (C・K6)
 * @returns {{ found: boolean, reason: string|null, total?: number, processed?: number, noop?: number, errors?: number, text?: string }}
 */
export function parseImportResult(text) {
  const t = String(text ?? '');
  // 先に「インポート結果」の表示の数を数える: 0 = 無い / 2 つ以上 = どれが今回か分からない (読めない表示 + 読める表示 も。Codex #1519 R1 High)
  const starts = [...t.matchAll(/インポート結果/g)].map((m) => m.index);
  if (!starts.length) return { found: false, reason: 'result_missing' };
  if (starts.length > 1) return { found: false, reason: 'result_ambiguous' };
  const seg = t.slice(starts[0]);
  // 見出しが 2 回出る = 表示が混ざっている
  const labelCount = (l) => seg.split(l).length - 1;
  if (LABELS.some((l) => labelCount(l) > 1)) return { found: false, reason: 'result_ambiguous' };
  const m = seg.match(ONE_RESULT_RE);
  if (!m) return { found: false, reason: 'result_unreadable' };
  const [total, processed, noop, errors] = [m[1], m[2], m[3], m[4]].map(toInt);
  if ([total, processed, noop, errors].some((n) => n == null)) return { found: false, reason: 'result_bad_number' };
  return { found: true, reason: null, total, processed, noop, errors, text: m[0].replace(/\s+/g, ' ').slice(0, 300) };
}

/**
 * 結果の読みから、ポータルの行き先を決める (v2 §4・v1 §6)
 * @returns {{ to: 'imported_unverified'|'partial'|'unknown', why: string }}
 */
export function judgeImportResult(parsed, csvRows) {
  if (!Number.isSafeInteger(csvRows) || csvRows < 1) throw new Error('csvRows (取り込んだ CSV の行数) が要る');
  if (!parsed || !parsed.found) return { to: 'unknown', why: (parsed && parsed.reason) || 'result_missing' };
  const { total, processed, noop, errors } = parsed;
  if (errors > 0) return { to: 'partial', why: `errors_${errors}` };
  if (total !== csvRows) return { to: 'partial', why: `total_${total}_rows_${csvRows}` };
  if (processed + noop !== total) return { to: 'partial', why: `processed_${processed}_noop_${noop}_total_${total}` };
  return { to: 'imported_unverified', why: 'counts_match' };
}

/** 1 つのセルを文字を落とさずに Shift_JIS (CP932) に。戻せない文字・制御文字 = 例外 (K1) */
export function encodeCellLossless(text) {
  const t = String(text);
  if (/[\u0000-\u001f\u007f]/.test(t)) throw new Error(`制御文字は書けない: ${JSON.stringify(t).slice(0, 60)}`);
  const bytes = iconv.encode(t, 'cp932');
  if (dec(bytes) !== t) throw new Error(`Shift_JIS に戻せない文字がある: ${JSON.stringify(t).slice(0, 60)}`);
  if (!/[",]/.test(t)) return bytes;
  const out = [0x22];
  for (const x of bytes) { out.push(x); if (x === 0x22) out.push(0x22); }
  out.push(0x22);
  return Buffer.from(out);
}

/**
 * 試験・戻しの資料の CSV を作る (見出し = DAILY・5 列・CRLF・最後の改行なし = 毎日の CSV と同じ形)。
 * 作った後に validateImportCsv で読み直し、各セルが渡した値と文字の単位で一致することを確かめる (違う = 例外)。
 * @param {string[][]} rows  各行 = [形式/型番, 商品名, ふりがな, 仕入単価, 取引先id] (5 列を独立に持つ)
 * @returns {{ bytes: Buffer, rows: number }}
 */
export function buildLosslessCsv(rows) {
  if (!Array.isArray(rows) || !rows.length) throw new Error('行が無い');
  for (const r of rows) if (!Array.isArray(r) || r.length !== DAILY.header.length || r.some((c) => typeof c !== 'string')) throw new Error('各行は 5 つの文字');
  const line = (cells) => {
    const parts = [];
    cells.forEach((c, i) => { if (i) parts.push(Buffer.from([0x2c])); parts.push(encodeCellLossless(c)); });
    return Buffer.concat(parts);
  };
  const crlf = Buffer.from([0x0d, 0x0a]);
  const lines = [line(DAILY.header), ...rows.map(line)];
  const bytes = Buffer.concat(lines.flatMap((l, i) => (i ? [crlf, l] : [l])));
  const v = validateImportCsv(bytes);
  if (!v.ok) throw new Error(`作った CSV が確かめを通らない: ${v.reason}`);
  if (v.rows !== rows.length || v.table.some((r, i) => r.some((c, j) => c !== rows[i][j]))) throw new Error('作った CSV を読み直すと値が違う');
  return { bytes, rows: v.rows };
}
