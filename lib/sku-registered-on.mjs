/**
 * sku-registered-on.mjs — 商品の登録日 (core.skus.registered_on / registered_on_source。0057) の共通の部品
 *
 * 決まり (0057 の頭の説明が正):
 *   ne         = NE の商品マスタの作成日 (夜間ロードが 商品管理リストの公開 snapshot の 登録日 から入れる)
 *   portal     = ポータルの新商品の登録で作った日 (登録の関数 ops.register_new_sku が作った取引の JST の今日を明示して入れる。列の既定値は無い = 空)
 *   first_seen = 夜間ロードが NE で初めて見て作った日 (NE の作成日が無いセット・例外)
 *   一度入ったら変えない (DB の trigger)。空 = 分からない
 * 使う所: apps/company-db/load/sources.mjs (NE の作成日を読む)・engine.mjs (空の行だけ埋める)・apps/master-edit (一覧の列・並べ替え・単品の画面)
 */

/** 出どころ → 画面の言葉 */
export const REGISTERED_ON_SOURCES = Object.freeze({ ne: 'NE の作成日', portal: 'ポータルで登録', first_seen: '夜間ロードで初めて見た日' });

/** 受け付ける最も古い日 (0057 の CHECK と同じ) */
export const REGISTERED_ON_MIN = '2000-01-01';

/**
 * NE の作成日 (例 '2026-10-03 11:18:17' / '2026/10/3 11:18:17' / '2026-10-03') → 'YYYY-MM-DD' (JST の暦日のまま。NE の時刻は日本の時刻)。
 * 読めない・無い日付 (2/30・13 月)・2000-01-01 より前・today (JST 'YYYY-MM-DD') より先 = null (入れない = 翌晩以降にもう一度見る)
 */
export function parseNeCreationDate(raw, today = null) {
  if (raw == null) return null;
  const m = /^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:$|[\sT])/.exec(String(raw));
  if (!m) return null;
  const y = Number(m[1]); const mo = Number(m[2]); const d = Number(m[3]);
  const probe = new Date(Date.UTC(y, mo - 1, d));
  if (probe.getUTCFullYear() !== y || probe.getUTCMonth() !== mo - 1 || probe.getUTCDate() !== d) return null;
  const iso = `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
  if (iso < REGISTERED_ON_MIN) return null;
  if (today && iso > today) return null;
  return iso;
}

/** core.skus に 0057 の列があるか (0057 の前の DB でも画面・ロードを止めない) */
export async function hasRegisteredOn(db) {
  // pg_attribute を直接 (information_schema.columns は権限の判定つきの view で重い = 一覧を開くたびに呼ぶので)
  return (await db.query(`select count(*)::int as n from pg_catalog.pg_attribute
     where attrelid = pg_catalog.to_regclass('core.skus') and attname in ('registered_on', 'registered_on_source') and attnum > 0 and not attisdropped`)).rows[0].n === 2;
}

/** 一覧の並べ替え ('' = コード順 = 今までどおり) */
export const LIST_SORTS = Object.freeze({ '': 'コード順', reg_desc: '登録日の新しい順' });

/**
 * 並べ替えの ORDER BY (alias = core.skus の別名)。登録日が無い DB (0057 の前) はコード順。
 * 登録日の新しい順 = 空 (分からない) は最後・同じ日はコード順
 */
export function listOrderBy(sort, { alias = 's', hasColumn = true } = {}) {
  if (sort === 'reg_desc' && hasColumn) return `${alias}.registered_on desc nulls last, ${alias}.code_norm`;
  return `${alias}.code_norm`;
}
