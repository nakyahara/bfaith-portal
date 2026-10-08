/**
 * list-columns.mjs — 商品・セットの一覧 (画面 A) の列の決まり (10/8 中原さん「列を選んで人ごとに保存・見出しで並び替え・仕入先の列」= PR1)
 *
 * 列の id = URL の並び (?sort=<id>&dir=asc|desc)・人ごとの列の設定 (view-prefs.mjs) の両方で使う名前。
 *   cell = 一覧の td の data-col (前からの名前を変えない = 試験・画面の JS がそのまま読める)。無ければ id と同じ
 *   sort = 並べられる列の種類 (見出しの「押すと ○○順」の言葉)。無い = 並べられない (FBA・売れた数・注文残・対応が必要 = 全件を読まないと並べられない)
 *   fixed = 外せない・動かせない列 (コード = いつも左端・横に送っても残る)
 * 並べ方そのもの (SQL / JS) は read.mjs の listSkus。この表は名前と言葉だけ (サーバー・画面の JS が同じ物を使う)
 */

/** 並びの種類 → [昇順の言葉, 降順の言葉] (見出しの title・表の上の「並び: …」) */
export const SORT_WORDS = Object.freeze({
  text: ['あいうえお・ABC 順', '逆の順'],
  code: ['0→9・A→Z の順', '9→0・Z→A の順'],
  date: ['古い順', '新しい順'],
  num: ['小さい順', '大きい順'],
  kind: ['単品 → セット → 例外', '例外 → セット → 単品'],
  state: ['取扱中 → 中止', '中止 → 取扱中'],
  sup: ['仕入先コード順', '仕入先コードの逆の順'],
});

export const LIST_COLUMNS = Object.freeze([
  { id: 'code', label: 'コード', sort: 'code', fixed: true, width: 'width:130px' },
  { id: 'name', label: '名前', sort: 'text', width: 'min-width:260px' },
  { id: 'kind', label: '区分', sort: 'kind', width: 'width:74px', th: 'th-kind' },
  { id: 'state', label: '状態', sort: 'state', width: 'width:150px' },
  { id: 'reg', label: '登録日', sort: 'date', width: 'width:96px' },
  { id: 'price', label: '売価', sort: 'num', num: true, width: 'width:84px' },
  { id: 'cost', label: '原価', sort: 'num', num: true, width: 'width:112px' },
  { id: 'tax', label: '税', sort: 'num', num: true, width: 'width:74px' },
  { id: 'profit', label: '利益', sub: '1 個・参考', sort: 'num', num: true, width: 'width:92px', th: 'th-profit' },
  { id: 'rate', label: '利益率', sort: 'num', num: true, width: 'width:74px', th: 'th-profit-rate', cell: 'profit-rate' },
  { id: 'sales_class', label: '売上分類', sort: 'num', width: 'width:120px' },
  { id: 'stock', label: '在庫', sort: 'num', num: true, ref: true, width: 'width:86px', th: 'th-stock' },
  { id: 'sup', label: '仕入先', sort: 'sup', isNew: true, width: 'min-width:150px', th: 'th-sup' },
  { id: 'fba', label: 'FBA (JP)', num: true, ref: true, width: 'width:92px', th: 'th-fba' },
  { id: 'sold', label: '売れた 7日/30日', num: true, ref: true, width: 'width:96px', th: 'th-sales', cell: 'sales' },
  { id: 'po', label: '注文残', num: true, ref: true, width: 'width:76px', po: true },
  { id: 'flags', label: '対応が必要', width: 'width:150px' },
].map((c) => Object.freeze({ ...c, cell: c.cell || c.id })));

export const COLUMN_IDS = Object.freeze(LIST_COLUMNS.map((c) => c.id));
export const COLUMN_BY_ID = Object.freeze(Object.fromEntries(LIST_COLUMNS.map((c) => [c.id, c])));
/** 並べられる列 (URL の ?sort= で受ける名前)。これ以外は使わない (= コード順) */
export const SORTABLE = Object.freeze(LIST_COLUMNS.filter((c) => c.sort).map((c) => c.id));
const has = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

/**
 * いつもの列 (設定を保存していない人・初期に戻す) = 10/8 までの一覧と同じ列 (出す・並びとも)。仕入先は新しい列 = 出さない (名前の下の小さい字のまま)。
 * 🚨 見本 (masterlist_mock) は FBA・売れた数・注文残・対応が必要を出さない形だったが、保存していない全員の一覧から列が消えないように今の列のまま
 */
export const DEFAULT_VIEW = Object.freeze({
  order: Object.freeze([...COLUMN_IDS]),
  shown: Object.freeze(COLUMN_IDS.filter((id) => id !== 'sup')),
});

/**
 * 古い URL の並び (10/8 まで) → { sort, dir }。'' = コード順。
 *   reg_desc = 登録日の新しい順・kind = 区分の順・profit_asc = 利益の少ない順・rate_asc = 利益率の低い順
 */
export const LEGACY_SORTS = Object.freeze({
  '': { sort: '', dir: '' },
  reg_desc: { sort: 'reg', dir: 'desc' },
  profit_asc: { sort: 'profit', dir: '' },
  rate_asc: { sort: 'rate', dir: '' },
});

/**
 * URL の sort・dir → 決まった形 { sort: 列の id か '' (= コード順), dir: 'desc' か '' (= 昇順) }。
 * 知らない列・並べられない列 (fba など)・形の違う値 = コード順 (SQL に入れない)。コードの昇順 = 両方 '' (URL に付けない)
 */
export function normalizeSort(sortRaw, dirRaw) {
  const s = typeof sortRaw === 'string' ? sortRaw : '';
  const d = typeof dirRaw === 'string' ? dirRaw : '';
  if (has(LEGACY_SORTS, s)) {
    const l = LEGACY_SORTS[s];
    // 古い名前で向きが決まっているもの (reg_desc など) はその向き。'' (コード順) だけは dir=desc を受ける
    if (s === '' && d === 'desc') return { sort: '', dir: 'desc' };
    return { sort: l.sort, dir: l.dir };
  }
  if (!SORTABLE.includes(s)) return { sort: '', dir: '' };
  const dir = d === 'desc' ? 'desc' : '';
  if (s === 'code') return { sort: '', dir };
  return { sort: s, dir };
}
/** 決まった形の並び → 列の id ('' = code) */
export const sortColumnOf = (sort) => sort || 'code';

/** 列の設定の形を確かめる (人が送ってきた物)。誤り = Error (message = 画面に出す言葉)。戻り値 = { order, shown } (足りない列は後ろに出さない形で足す) */
export class ViewPrefsInputError extends Error {}
export function parseViewInput(body) {
  const bad = (m) => { throw new ViewPrefsInputError(m); };
  if (!body || typeof body !== 'object' || Array.isArray(body)) bad('列の設定の形が違います');
  for (const k of Object.keys(body)) if (k !== 'order' && k !== 'shown') bad(`知らない項目 ${String(k).slice(0, 40)} は受けません`);
  const list = (v, what) => {
    if (!Array.isArray(v)) bad(`${what} は列の名前の並びにしてください`);
    if (v.length > COLUMN_IDS.length) bad(`${what} の列が多すぎます (${COLUMN_IDS.length} まで)`);
    const seen = new Set();
    for (const x of v) {
      if (typeof x !== 'string' || !has(COLUMN_BY_ID, x)) bad(`知らない列 ${String(typeof x === 'string' ? x : typeof x).slice(0, 40)} は受けません`);
      if (seen.has(x)) bad(`${what} に同じ列 ${x} が 2 回あります`);
      seen.add(x);
    }
    return v;
  };
  const order = list(body.order, '並び');
  const shown = list(body.shown, '出す列');
  if (order[0] !== 'code') bad('コードの列はいつも左端です (動かせません)');
  if (!shown.includes('code')) bad('コードの列は外せません');
  return normalizeView({ order, shown });
}

/**
 * 保存してあった列の設定を今の列に合わせる (後から列を足した・消した): 知らない列は捨てる・無い列は後ろに足す (出さない)・コードはいつも左端で出す。
 * 読めない形 = null (いつもの列)
 */
export function normalizeView(v) {
  if (!v || !Array.isArray(v.order) || !Array.isArray(v.shown)) return null;
  const order = [];
  for (const x of v.order) if (typeof x === 'string' && has(COLUMN_BY_ID, x) && !order.includes(x)) order.push(x);
  for (const id of COLUMN_IDS) if (!order.includes(id)) order.push(id);
  const ordered = ['code', ...order.filter((x) => x !== 'code')];
  const shownSet = new Set(v.shown.filter((x) => typeof x === 'string' && has(COLUMN_BY_ID, x)));
  shownSet.add('code');
  return { order: ordered, shown: ordered.filter((x) => shownSet.has(x)) };
}
export const sameView = (a, b) => !!a && !!b && a.order.join(',') === b.order.join(',') && a.shown.join(',') === b.shown.join(',');
