/**
 * lib/amazon-sku-asin-map.js — Amazon の SKU → ASIN の対応 (1 か所・Render の写しから作る)
 *
 * なぜ要るか (2026-10-03・F4 の調べ・Codex R-F4-1 Medium):
 *   日次の財務 (mirror_amazon_finance_sku_daily) の asin_norm は miniPC の build で **常に空**
 *   (sql/amazon/build_f_amazon_finance_sku_daily_v1.sql の `'' AS asin_norm`)。それで
 *     - amazon-dashboard の広告の割り振りで、ASIN の粒度の広告の行が SKU に結びつかず、全部「配れない広告費」の按分に回っていた
 *     - site-products の asinFinance・supplier-sales の ASIN の欄はいつも空だった
 *   → 財務の fact の鍵に ASIN を戻すのではなく、**SKU → ASIN の専用のマップ** をここで 1 つ作って、上の 3 つで使う
 *     (Company DB の財務にも ASIN は無い = F4-4 で読み手を替えても、このマップはそのまま使える)。
 *
 * 出どころ (Render の写しにある SKU と ASIN の対はこの 2 つだけ):
 *   1. mirror_amazon_sku_fees (SKU ごとに 1 行・今の対)
 *        miniPC の amazon_sku_fees。asin = その SKU の最新の注文 (raw_sp_orders) の ASIN → 無ければ前のキャッシュ。
 *        ASIN が変わると取り直す (fetch-amazon-fees.js の asin_changed)。「見た日」= fetched_at (UTC) の JST の日
 *   2. mirror_amazon_price_snapshot_daily (日 × SKU・対の履歴)
 *        miniPC の fact_amazon_price_snapshot。対象の SKU と ASIN は 1 の表から取っている。「見た日」= date_jst の最大
 *   (m_sku_master に ASIN は無い・raw_sp_orders の asin は写しに無い・広告の SKU の行は ASIN を捨てている = 使えない)
 *
 * 決まり:
 *   - 鍵の形: SKU = trim + 小文字 (財務の seller_sku・広告の target と同じ)・ASIN = trim + 大文字 (英数字 10 文字だけ・それ以外は捨てる)
 *   - **いつの時点の対応か**:
 *       今の ASIN (表示用・1 つ) = 「見た日」がいちばん新しい対。同じ日なら sku_fees を先 (price_snapshot の ASIN は sku_fees から作るため)、
 *       それでも同じなら ASIN の文字の順 → SKU の文字の順 (compareAsinHits・SQL の返す順に依らず毎回同じ。商品に SKU が複数ある site-products も同じ順で選ぶ)
 *       ASIN → SKU の群 (広告の割り振り用) = **一度でも対になった SKU 全部** (期間で絞らない)。
 *       SKU の ASIN が途中で変わっても、前の ASIN の広告も今の ASIN の広告もその SKU に届く。
 *       価格の写しは始まりより前の日が無いため、期間で絞ると古い期間の広告が結びつかなくなる
 *   - **1 対多**: 1 つの ASIN に SKU が 2 つ以上 (FBA と自社発送・プライスターの pr_ の SKU など) はふつうにある →
 *       広告の割り振りは、その期間に財務の行がある SKU の間で売上の比 (負の売上は 0 として数える・全部 0 なら等分)。
 *       1 つの SKU に ASIN が 2 つ以上 (出品の付け替え) は珍しい → 表示は今の 1 つ・割り振りはどちらの ASIN の広告も受ける
 *   - **二重に配らない**: SKU の群は SKU ごとに 1 回 (2 つの出どころで同じ対が見えても 1 回)。ASIN の粒度の広告の 1 行は群の中で 1 回だけ配る
 *       (割合の合計 = 1)。同じ費用が SKU の粒度と ASIN の粒度の両方に出ることは無い (取込で SKU を先にしている・fetch-amazon-ads.js)
 *   - **fail-soft**: 出どころの表が読めないときはその出どころだけ飛ばし degraded に名前を残す (画面を落とさない)。
 *       使う所は応答の degradedLookups に出す (asinMapDiagnostics)・degraded のマップは 30 秒しかキャッシュしない
 *
 * 🚨 日次の財務の asin_norm は使わない (常に空・Company DB にも無い)。
 */

/** 出どころ。rank が小さいほど同じ日のとき先 */
export const SKU_ASIN_SOURCES = Object.freeze([
  Object.freeze({
    key: 'sku_fees',
    rank: 0,
    sql: `SELECT LOWER(TRIM(seller_sku)) AS sku, UPPER(TRIM(asin)) AS asin,
                 COALESCE(date(fetched_at, '+9 hours'), '') AS seen
          FROM mirror_amazon_sku_fees
          WHERE seller_sku IS NOT NULL AND TRIM(seller_sku) <> '' AND asin IS NOT NULL AND TRIM(asin) <> ''
          ORDER BY 1, 2, 3`,
  }),
  Object.freeze({
    key: 'price_snapshot',
    rank: 1,
    sql: `SELECT LOWER(TRIM(seller_sku)) AS sku, UPPER(TRIM(asin)) AS asin, MAX(date_jst) AS seen
          FROM mirror_amazon_price_snapshot_daily
          WHERE TRIM(seller_sku) <> '' AND asin IS NOT NULL AND TRIM(asin) <> ''
          GROUP BY LOWER(TRIM(seller_sku)), UPPER(TRIM(asin))
          ORDER BY 1, 2`,
  }),
]);
const RANK = Object.fromEntries(SKU_ASIN_SOURCES.map((s) => [s.key, s.rank]));

/** ASIN の形 (trim + 大文字の後) = 英数字 10 文字。空・空白・記号 (N/A など)・短い / 長い壊れた値 (NULL・NONE など) は捨てる (Codex #1604 R1 Low) */
export const ASIN_RE = /^[A-Z0-9]{10}$/;

/** 出どころ → 応答の診断 degradedLookups の名前 (site-products の前からの名前。dashboard・supplier-sales も同じ名前で出す) */
export const DEGRADED_LOOKUP_NAME = Object.freeze({ price_snapshot: 'asinPrice', sku_fees: 'asinFees' });

/** 出どころの順位 (小さいほど同じ日のとき先)。知らない出どころは最後 */
export const sourceRank = (source) => (Object.hasOwn(RANK, source) ? RANK[source] : Number.MAX_SAFE_INTEGER);

/**
 * 「今の ASIN」の候補 ({ asin, seen, source, sku }) の全順序 (負 = a が先)。
 *   見た日が新しい → 出どころの順位 (手数料の写しが先) → ASIN の文字の順 → SKU の文字の順
 *   = 入力の順 (SQL の返す順) に依らず毎回同じものを選ぶ (Codex #1604 R1 Medium: 同じ日・複数 SKU)
 */
export function compareAsinHits(a, b) {
  const sa = a.seen || '', sb = b.seen || '';
  if (sa !== sb) return sa > sb ? -1 : 1;
  const ra = sourceRank(a.source), rb = sourceRank(b.source);
  if (ra !== rb) return ra - rb;
  if (a.asin !== b.asin) return a.asin < b.asin ? -1 : 1;
  const ka = a.sku || '', kb = b.sku || '';
  return ka === kb ? 0 : ka < kb ? -1 : 1;
}

export const normSku = (v) => String(v ?? '').trim().toLowerCase();
export const normAsin = (v) => String(v ?? '').trim().toUpperCase();

/**
 * 観測 ([{ sku, asin, seen, source }]) からマップを作る (DB を読まない・試験用にも出す)。
 * @returns {{ asinBySku: Map<string, {asin, seen, source, sku}>, skusByAsin: Map<string, Set<string>>, asinsBySku: Map<string, Set<string>>,
 *            stats: { skus, asins, pairs, skusWithManyAsins, asinsWithManySkus, dropped }, degraded: string[] }}
 *   asinBySku のキー = 小文字の SKU・値の asin = 大文字。skusByAsin のキー = 小文字の ASIN (広告の target と同じ形)
 */
export function buildSkuAsinMap(observations, degraded = []) {
  const asinBySku = new Map();
  const skusByAsin = new Map();
  const asinsBySku = new Map();
  let dropped = 0;
  let pairs = 0;
  for (const o of observations || []) {
    const sku = normSku(o?.sku);
    const asin = normAsin(o?.asin);
    if (!sku || !ASIN_RE.test(asin) || !Object.hasOwn(RANK, o?.source)) { dropped++; continue; }
    const seen = typeof o.seen === 'string' ? o.seen : '';
    const a = asin.toLowerCase();
    if (!asinsBySku.has(sku)) asinsBySku.set(sku, new Set());
    if (!asinsBySku.get(sku).has(a)) pairs++;
    asinsBySku.get(sku).add(a);
    if (!skusByAsin.has(a)) skusByAsin.set(a, new Set());
    skusByAsin.get(a).add(sku);
    const cur = asinBySku.get(sku);
    const hit = { asin, seen, source: o.source, sku };
    if (!cur || compareAsinHits(hit, cur) < 0) asinBySku.set(sku, hit);
  }
  let skusWithManyAsins = 0;
  for (const s of asinsBySku.values()) if (s.size > 1) skusWithManyAsins++;
  let asinsWithManySkus = 0;
  for (const s of skusByAsin.values()) if (s.size > 1) asinsWithManySkus++;
  return {
    asinBySku, skusByAsin, asinsBySku,
    stats: { skus: asinsBySku.size, asins: skusByAsin.size, pairs, skusWithManyAsins, asinsWithManySkus, dropped },
    degraded: [...degraded],
  };
}

/** 写しの 2 つの表を読んでマップを作る (キャッシュなし)。表が無い・読めない出どころは飛ばして degraded に */
export function readSkuAsinMap(db) {
  const obs = [];
  const degraded = [];
  for (const src of SKU_ASIN_SOURCES) {
    try {
      for (const r of db.prepare(src.sql).all()) obs.push({ sku: r.sku, asin: r.asin, seen: r.seen ?? '', source: src.key });
    } catch (e) {
      degraded.push(src.key);
      console.warn(`[amazon-sku-asin-map] ${src.key} を読めない (この出どころは飛ばす):`, e.message);
    }
  }
  return buildSkuAsinMap(obs, degraded);
}

// 接続ごとのキャッシュ (amazon-dashboard の getNameMap と同じ 10 分)。写しの更新は夜 1 回なので 10 分の遅れは問題にならない
export const CACHE_TTL_MS = 10 * 60 * 1000;
// 出どころが読めなかった (degraded) マップは短くだけ持つ (Codex #1604 R1 Medium)。
//   10 分持つと、表が戻っても 10 分は ASIN の広告が「配れない分」に回ったまま。30 秒 = 1 つの画面の読み込み (診断タブは月ごとに何度も割り振る) の間だけ使い回す
export const DEGRADED_CACHE_TTL_MS = 30 * 1000;
const _cacheByDb = new WeakMap();

/**
 * マップを返す。opts.cache = false なら毎回読む (site-products のように取引の中で読む所・試験)。
 * opts.now = 今の時刻 (ms・試験でキャッシュの失効を確かめる用。既定 Date.now())
 */
export function loadSkuAsinMap(db, opts = {}) {
  if (opts.cache === false) return readSkuAsinMap(db);
  const now = Number.isFinite(opts.now) ? opts.now : Date.now();
  const hit = _cacheByDb.get(db);
  if (hit) {
    const ttl = hit.map.degraded.length ? DEGRADED_CACHE_TTL_MS : CACHE_TTL_MS;
    if (now - hit.at >= 0 && now - hit.at < ttl) return hit.map;
  }
  const map = readSkuAsinMap(db);
  _cacheByDb.set(db, { at: now, map });
  return map;
}

/**
 * 応答に載せる診断 (site-products と同じ形の degradedLookups + マップの数)。
 *   degradedLookups = 読めなかった出どころ (asinPrice / asinFees)。空でなければ ASIN の広告の一部が「配れない分」に回り、ASIN の欄が空になり得る
 *   asinMap = { skus, asins, pairs, skusWithManyAsins (付け替えた SKU), asinsWithManySkus (1 ASIN に複数 SKU = 多対多の数), dropped }
 *   マップが無い (読んでいない) ときは degradedLookups = []・asinMap = null
 */
export function asinMapDiagnostics(map) {
  if (!map) return { degradedLookups: [], asinMap: null };
  return {
    degradedLookups: map.degraded.map((s) => DEGRADED_LOOKUP_NAME[s] || s),
    asinMap: { ...map.stats },
  };
}

/** キャッシュを捨てる (試験用) */
export function clearSkuAsinMapCache(db) {
  if (db) _cacheByDb.delete(db);
}

/** SKU の今の ASIN (大文字)。無ければ '' */
export function currentAsin(map, sku) {
  return map?.asinBySku.get(normSku(sku))?.asin || '';
}

/** SKU と一度でも対になった ASIN (小文字の Set)。無ければ空の Set */
export function asinsOfSku(map, sku) {
  return map?.asinsBySku.get(normSku(sku)) || new Set();
}
