/**
 * FBA納品ピッキング準備 — CSV コーデック
 *
 * 設計根拠 (Codex レビュー #6/#10):
 *  - RFC4180 厳密パーサ (引用内カンマ/改行/"" エスケープ対応)。商品名にカンマ・括弧が入るため
 *    素朴な split(',') は使わない (apps/packing-dispatch/csv.js と同等の実装)。
 *  - 入力エンコーディングは UTF-8(BOM有/無) と CP932(Shift_JIS) を自動判定。Amazon の納品プラン
 *    CSV は UTF-8/SJIS のどちらもあり得る。ロジザード/バーコードは SJIS。判定は文字化け(U+FFFD)
 *    の少なさ + 日本語文字数のスコアリング (GAS readCsvAuto_ と同方針)。
 *  - 出力 (P-touch ラベル CSV) は Shift_JIS・CRLF を維持 (P-touch Editor の文字化け防止)。
 */
import iconv from 'iconv-lite';

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

/**
 * Buffer を UTF-8(BOM) / UTF-8 / CP932 から自動判定してデコード。
 * @returns {{ text: string, encoding: string }}
 */
export function decodeCsvBuffer(buffer) {
  if (!buffer || buffer.length === 0) return { text: '', encoding: 'empty' };
  // UTF-8 BOM
  if (buffer.length >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
    return { text: buffer.slice(3).toString('utf8'), encoding: 'utf-8-bom' };
  }
  const candidates = [
    { enc: 'utf-8', text: buffer.toString('utf8') },
    { enc: 'cp932', text: iconv.decode(buffer, 'Shift_JIS') },
  ];
  let best = candidates[0], bestScore = -Infinity;
  for (const cand of candidates) {
    const bad = (cand.text.match(/�/g) || []).length;          // 文字化け(置換文字)
    const jp = (cand.text.match(/[一-龥ぁ-んァ-ヶー]/g) || []).length; // 日本語文字
    const score = jp - bad * 200;
    if (score > bestScore) { best = cand; bestScore = score; }
  }
  return { text: best.text, encoding: best.enc };
}

/**
 * CSV セルのエスケープ。
 * @param {*} v 値
 * @param {boolean} guardFormula true の場合、先頭が = + - @ 等のセルに ' を前置して
 *   表計算アプリの式インジェクションを無害化する (Excel で開く運用向け)。
 *   P-touch ラベル CSV は false (P-touch Editor は式評価せず、' 前置は印字を壊すため)。
 */
export function csvCell(v, guardFormula = false) {
  let s = String(v == null ? '' : v);
  if (guardFormula && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return '"' + s.replace(/"/g, '""') + '"';
}

/**
 * 行配列(配列の配列) → Shift_JIS・CRLF の CSV Buffer。
 * @param {string[][]} rows 全行(ヘッダ含む)
 * @param {{ guardFormula?: boolean }} opts
 */
export function buildShiftJisCsv(rows, opts = {}) {
  const guard = !!opts.guardFormula;
  const text = rows.map(r => r.map(c => csvCell(c, guard)).join(',')).join('\r\n') + '\r\n';
  return iconv.encode(text, 'Shift_JIS');
}
