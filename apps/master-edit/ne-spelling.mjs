/**
 * ne-spelling.mjs — 外へ出す CSV の商品コードを、NE から来た商品なら NE の書き方で出す (10/16 中原さん)
 *
 * なぜ: 夜間ロードは NE の取得の商品コード (mirror_products.商品コード = 小文字にして保存) を core.skus.code に入れる (apps/company-db/load/sources.mjs)。
 *   NE の商品コードは大文字・小文字を区別する = 小文字のまま NE やロジザードに上げると、別の商品として入るおそれ
 *   (2026-10-13 に小文字の商品マスタ CSV が NE に予約アップされ、u60e-05 などで書き方が 2 つになった)。
 * 決まり (書き方を推測しない):
 *   - 新商品の入口で作った商品 (登録の始まり = new_entry) = core.skus.code (打った書き方) のまま・注意なし
 *   - NE から来た商品 (backfill / ne_discovered / 登録の行が無い) で、NE の元のコード (0041 ops.master_ne_codes の kind = product) が ok = NE の書き方 (ne_code)
 *   - NE から来た商品で ok でない (collided / invalid / 記録に無い / 読めない) = core.skus.code のまま + 注意 (理由) = 確かめずに上げないように
 * 読むのは DB の関数 ops.ne_reg_ne_codes (画面のロール master_edit に ops.master_ne_codes の select は無い = 0053)。
 *   関数は 1 回 1,000 コードまで = 1,000 ずつに分けて、1 つの SQL で全部の塊を呼ぶ (往復は権限の確かめと合わせて 2 回)
 */

/** ops.ne_reg_ne_codes が 1 回で読むコードの数 (0053 の関数の上限と同じ) */
export const NE_CODES_CHUNK = 1000;

/** 注意の言葉 (CSV の「商品コードの注意」) */
export const SPELLING_NOTES = Object.freeze({
  collided: 'NE に大文字・小文字だけ違う書き方が 2 つ以上 (NE の書き方が決まらない)',
  invalid: 'NE の書き方が使えない (使えない文字・壊れた記録)',
  missing: 'NE の書き方が分からない (NE の元のコードの記録に無い)',
  unreadable: 'NE の書き方を読めない (Company DB の関数を使えない)',
});

/**
 * NE の元のコード (kind = product) を読む。戻り値 = Map<code_norm, { state, ne_code }> / null = 読めない (関数が無い・実行の権限が無い)
 * @param {{ query: Function }} db
 * @param {string[]} norms  core.skus.code_norm
 */
export async function readNeSpellings(db, norms) {
  const want = [...new Set((norms || []).filter((x) => typeof x === 'string' && x))];
  const map = new Map();
  if (!want.length) return map;
  const ok = (await db.query(`select coalesce(pg_catalog.has_function_privilege(to_regprocedure('ops.ne_reg_ne_codes(text[])'), 'EXECUTE'), false) as ok`)).rows[0].ok === true;
  if (!ok) return null;
  const rows = (await db.query(`select ops.ne_reg_ne_codes(g.codes) as r
      from (select array_agg(u.c order by u.c) as codes
              from (select t.c, (row_number() over (order by t.c) - 1) / ${NE_CODES_CHUNK} as grp from unnest($1::text[]) as t(c)) u
             group by u.grp) g`, [want])).rows;
  for (const { r } of rows) {
    for (const e of (r && r.entries) || []) if (e.kind === 'product') map.set(e.code_norm, { state: e.state, ne_code: e.ne_code ?? null });
  }
  return map;
}

/**
 * 外へ出す商品コードと注意 (純粋)。origin = 登録の始まり (null = 登録の行が無い = NE から来た商品として扱う)
 * @param {{ code: string, code_norm: string, origin?: string|null }} sku
 * @param {Map<string, { state, ne_code }>|null} spell  readNeSpellings の結果 (null = 読めない)
 * @returns {{ code: string, note: string }}
 */
export function exportCode({ code, code_norm: norm, origin = null }, spell) {
  if (origin === 'new_entry') return { code, note: '' };
  if (!spell) return { code, note: SPELLING_NOTES.unreadable };
  const e = spell.get(norm);
  if (!e) return { code, note: SPELLING_NOTES.missing };
  // ok = 書き方が 1 つ。小文字にすると同じ商品のコードのときだけ使う (0041 の CHECK と同じ。違えば使わない)
  if (e.state === 'ok' && typeof e.ne_code === 'string' && e.ne_code && e.ne_code.toLowerCase() === norm) return { code: e.ne_code, note: '' };
  return { code, note: e.state === 'collided' ? SPELLING_NOTES.collided : SPELLING_NOTES.invalid };
}
