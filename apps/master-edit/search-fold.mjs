/**
 * search-fold.mjs — マスタの入力の検索で「同じものとして当てる」決まり (一覧の名前・Amazon SKU の対応の名前)
 *
 *   NFKC (半角カナ → 全角カナ・全角英数 → 半角英数・濁点の合成) → 小文字 → ひらがな → カタカナ
 *   例: 「はちみつ」「ハチミツ」「ﾊﾁﾐﾂ」は同じ・「ＡＢＣ」「abc」「ABC」は同じ。
 *   長音 (ー と -)・小さい文字 (ァ と ア)・濁点の有無は変えない (別の字のまま)。
 *
 * 検索語は JS の foldSearch で、DB の列は foldSql の式で、同じ決まりにそろえて LIKE で比べる。
 * 画面の JS (public/me-shell.js の fold) も同じ決まり = 試験 (test-master-edit-ui) で両方が同じ答えになることを確かめる。
 * DB は PostgreSQL 13 以降の normalize(…, NFKC) を使う (Company DB は 16 以上・PGlite は 18)。7,377 件なら index なしで足りる (式の index は要らない)
 */
const HIRA = [];
const KATA = [];
for (let c = 0x3041; c <= 0x3096; c++) { HIRA.push(String.fromCharCode(c)); KATA.push(String.fromCharCode(c + 0x60)); }
HIRA.push('ゝ', 'ゞ'); KATA.push('ヽ', 'ヾ');   // ゝゞ → ヽヾ
/** translate の 2 つ目・3 つ目に渡す字の並び (同じ長さ・同じ順) */
export const HIRA_CHARS = HIRA.join('');
export const KATA_CHARS = KATA.join('');

/** 検索語を決まった形に */
export function foldSearch(s) {
  return String(s ?? '').normalize('NFKC').toLowerCase()
    .replace(/[ぁ-ゖゝゞ]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 0x60));
}

/** LIKE の % と _ と \ を文字として探す形 (既定の escape = \)・前後に % */
export const likeOf = (s) => `%${String(s).replace(/[\\%_]/g, (c) => `\\${c}`)}%`;

/**
 * DB の列を同じ決まりにする式。params に HIRA_CHARS・KATA_CHARS を足して、その $番号を使う。
 * 戻り値 = (列の式) => SQL の式
 */
export function foldSql(params) {
  params.push(HIRA_CHARS); const h = params.length;
  params.push(KATA_CHARS); const k = params.length;
  return (expr) => `translate(lower(normalize(${expr}, NFKC)), $${h}, $${k})`;
}
