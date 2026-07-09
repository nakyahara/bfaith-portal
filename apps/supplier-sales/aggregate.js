/**
 * 仕入れ先向け売れ筋共有 — 集計ロジック (Phase 2: 実出荷ピース数 + 出品明細 + 期間選択)
 *
 * 全モール（Amazon / 楽天 / Yahoo! / au PAY / LINEギフト / Qoo10）の
 * SKU 日次売上マート（mirror_*_finance_sku_daily）を NE 商品コードに名寄せし、
 * 仕入先コード単位で「指定期間の販売ピース数・売上」を商品別 + 出品ページ別に集計する。
 *
 * ── 数え方の確定仕様（中原さん確定 2026-06-28）──────────────────────────
 *  【ピース数】= 実出荷ピース数。3個セットが1件売れたら、その構成商品を「3個」として数える。
 *     - Amazon: seller_sku を mirror_sku_resolved で構成品 (ne_code × quantity) に展開し、
 *               ピース数 = 出品の販売数 × quantity。
 *     - 他モール: fact 行の ne_code がセット商品 (mirror_set_components に存在) なら構成品へ展開、
 *               単品ならそのまま。ピース数 = 出品の販売数 × 構成数量。
 *     - 注: 他モールで「3個入り出品」が NE 上は単品コード直結 (セット未登録) の場合、
 *           構成数量を知る術がないため 1出品=1ピース として数える（データ登録依存の限界）。
 *  【売上】= その出品ページが実際に売れた金額（税込）。複数商品が混ざるセットの場合のみ、
 *     構成品の「標準売価 × 構成数量」比で按分（二重計上回避）。単品/同一商品nパックは全額。
 *     ※ 原価は一切扱わない（仕入先非開示）。
 *
 *  【集計対象日】稼働中モール（14日以内に同期のあるモール）全てが確定済みの日 - 2日（D-2）まで。
 *     直近日はモール側の確定遅延で揺れるため締める。
 *  【FBA/FBM】Amazon は fba_fulfillment_jpy / fba_storage_jpy の発生有無で出品単位に判別（推定）。
 */

export const MALL_LABELS = {
  amazon: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo!',
  aupay: 'au PAY', linegift: 'LINEギフト', qoo10: 'Qoo10',
};

// 非 Amazon モールの日次売上 fact 定義。key = 出品識別子、sales = 売上(税込)列。
const NON_AMAZON_MALLS = [
  { mall: 'rakuten',  table: 'mirror_rakuten_finance_sku_daily',  key: 'rakuten_code',  sales: 'gross_sales_jpy_incl' },
  { mall: 'yahoo',    table: 'mirror_yahoo_finance_sku_daily',    key: 'yahoo_sku_key', sales: 'gross_sales_jpy_incl' },
  { mall: 'aupay',    table: 'mirror_aupay_finance_sku_daily',    key: 'aupay_sku_key', sales: 'gross_sales_jpy_incl' },
  { mall: 'linegift', table: 'mirror_linegift_finance_sku_daily', key: 'sku_code',      sales: 'gross_sales_jpy_incl' },
  { mall: 'qoo10',    table: 'mirror_qoo10_finance_sku_daily',    key: 'sku_code',      sales: 'customer_paid_jpy_incl' },
];

const PRESETS = { '7d': 7, '30d': 30, '90d': 90 };
const YMD = /^\d{4}-\d{2}-\d{2}$/;
// custom 期間の最大日数（未認証公開口からの過大スキャン=DoS を防ぐ上限）
const MAX_CUSTOM_DAYS = 366;

// ── 日付ユーティリティ ─────────────────────────────────────────
function shiftDate(ymd, days) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}
function daysBetween(a, b) {
  return Math.round((new Date(`${b}T00:00:00Z`) - new Date(`${a}T00:00:00Z`)) / 86400000);
}

// 各モール fact の最新 date_jst（データのある table のみ）。
function tableMaxDates(db) {
  const tables = ['mirror_amazon_finance_sku_daily', ...NON_AMAZON_MALLS.map(c => c.table)];
  const maxes = [];
  for (const t of tables) {
    const row = db.prepare(`SELECT MAX(date_jst) AS d FROM ${t}`).get();
    if (row && row.d) maxes.push(row.d);
  }
  return maxes;
}

/**
 * 確定対象日 cutoff を求める。= 稼働中モール（globalMax から14日以内）MAX の最小 - 2日。
 * 遅延モールのゼロ過少集計を防ぎ、停止モールが cutoff を過去へ引きずるのも防ぐ。
 */
export function confirmedCutoff(db) {
  const maxes = tableMaxDates(db);
  if (!maxes.length) return null;
  const globalMax = maxes.reduce((a, b) => (b > a ? b : a));
  const active = maxes.filter(m => daysBetween(m, globalMax) <= 14);
  const confirmedAcross = active.reduce((a, b) => (b < a ? b : a));
  return { latest: globalMax, confirmedAcross, cutoff: shiftDate(confirmedAcross, -2) };
}

/**
 * 期間を解決する。preset(7d/30d/90d) または custom(start,end)。
 * end は cutoff（確定対象日）でクランプ。prev は同じ長さの直前期間。
 */
export function resolvePeriod(db, opts = {}) {
  const c = confirmedCutoff(db);
  if (!c) return null;
  const cutoff = c.cutoff;
  let start, end, label, period;
  if (opts.period === 'custom' && YMD.test(opts.start || '') && YMD.test(opts.end || '')) {
    period = 'custom';
    start = opts.start; end = opts.end;
    if (end > cutoff) end = cutoff;
    if (start > end) start = end;
    // 下限クランプ: end から最大 MAX_CUSTOM_DAYS 日まで（過大範囲スキャン防止）
    const minStart = shiftDate(end, -(MAX_CUSTOM_DAYS - 1));
    if (start < minStart) start = minStart;
    label = `${start} 〜 ${end}`;
  } else {
    const n = PRESETS[opts.period] || 30;
    period = PRESETS[opts.period] ? opts.period : '30d';
    end = cutoff;
    start = shiftDate(cutoff, -(n - 1));
    label = `直近${n}日`;
  }
  const len = daysBetween(start, end) + 1;
  return {
    period, start, end, label, len, cutoff,
    prevEnd: shiftDate(start, -1),
    prevStart: shiftDate(start, -len),
    latest: c.latest, confirmedAcross: c.confirmedAcross,
  };
}

// ── 名寄せ・展開マップ ─────────────────────────────────────────
// 商品マスタ全件 Map は按分の重み計算で他仕入先の構成品価格も要るため全件持つが、
// 毎リクエスト全件ロードを避けるため短 TTL でキャッシュ（mirror_products は日次更新）。
let _prodCache = null;
let _prodCacheAt = 0;
const PROD_CACHE_TTL_MS = 60_000;

function loadProductMap(db) {
  const now = Date.now();
  if (_prodCache && (now - _prodCacheAt) < PROD_CACHE_TTL_MS) return _prodCache;
  const m = new Map(); // 商品コード -> { name, price, supplier }
  for (const r of db.prepare('SELECT 商品コード c, 商品名 n, 標準売価 p, 仕入先コード s FROM mirror_products').all()) {
    m.set(r.c, { name: r.n || '', price: (r.p == null ? 0 : r.p), supplier: r.s || '' });
  }
  _prodCache = m;
  _prodCacheAt = now;
  return m;
}

// セット商品コード -> [{code, qty}]（対象仕入先の構成品を含むセットのみ）
function loadSetMap(db, supplier) {
  const m = new Map();
  // 監査M-7: SKU比較は両辺LOWER(TRIM)で正規化 (INV-13、Yahoo PR #94事故の再発防止)
  const rows = db.prepare(`
    SELECT セット商品コード sc, 構成商品コード cc, 数量 q
    FROM mirror_set_components
    WHERE LOWER(TRIM(セット商品コード)) IN (
      SELECT LOWER(TRIM(セット商品コード)) FROM mirror_set_components
      WHERE LOWER(TRIM(構成商品コード)) IN (SELECT LOWER(TRIM(商品コード)) FROM mirror_products WHERE 仕入先コード = @s)
    )`).all({ s: supplier });
  for (const r of rows) {
    if (!m.has(r.sc)) m.set(r.sc, []);
    m.get(r.sc).push({ code: r.cc, qty: r.q || 1 });
  }
  return m;
}

// Amazon seller_sku -> [{code, qty}]（対象仕入先の構成品を含む seller_sku のみ）
function loadAmazonMap(db, supplier) {
  const m = new Map();
  const rows = db.prepare(`
    SELECT seller_sku k, ne_code c, quantity q
    FROM mirror_sku_resolved
    WHERE LOWER(TRIM(seller_sku)) IN (
      SELECT LOWER(TRIM(seller_sku)) FROM mirror_sku_resolved
      WHERE LOWER(TRIM(ne_code)) IN (SELECT LOWER(TRIM(商品コード)) FROM mirror_products WHERE 仕入先コード = @s)
    )`).all({ s: supplier });
  for (const r of rows) {
    if (!m.has(r.k)) m.set(r.k, []);
    m.get(r.k).push({ code: r.c, qty: r.q || 1 });
  }
  return m;
}

/**
 * 指定仕入先の期間レポート。
 * @returns { period, products:[{ne_code,name,pieces,sales,prevPieces,prevSales,lastSold,
 *            listings:[{mall,listingId,listingName,asin,is_fba,sold,pieces,sales}]}], totals }
 */
export function getSupplierReport(db, supplierCode, opts = {}) {
  // 確定(精算)期間。finance マートが空だと null になり得るが、速報(注文ベース)は
  // それと独立に出すため、P が null でも処理を続け、確定パートだけスキップする。
  const P = resolvePeriod(db, opts);

  const prod = loadProductMap(db);
  const setMap = loadSetMap(db, supplierCode);
  const amzMap = loadAmazonMap(db, supplierCode);

  // 商品(構成品=対象仕入先)ごとのアキュムレータ
  const out = new Map();
  const getProd = (code) => {
    let p = out.get(code);
    if (!p) {
      p = {
        ne_code: code, name: (prod.get(code)?.name) || '',
        pieces: 0, sales: 0, prevPieces: 0, prevSales: 0, lastSold: null,
        listings: new Map(),
      };
      out.set(code, p);
    }
    return p;
  };

  // 1出品行を構成品に展開し、対象仕入先分を計上する。
  function attribute({ mall, listingId, listingName, asin, fbaFee, date, u, sales, comps }) {
    const inWin = date >= P.start && date <= P.end;
    const inPrev = date >= P.prevStart && date <= P.prevEnd;
    if (!inWin && !inPrev) return;
    // 売上按分の重み = 標準売価 × 構成数量。ただし 1 つでも価格欠損(0/NULL)の構成品が
    // あると価格按分では欠損品の取り分が 0 になり過小計上になるため、その場合は
    // 数量按分（構成数量比）にフォールバックして全構成品へ確実に配賦する。
    const allPriced = comps.every(c => (prod.get(c.code)?.price || 0) > 0);
    const weights = comps.map(c => allPriced ? (prod.get(c.code).price * c.qty) : c.qty);
    const wsum = weights.reduce((a, b) => a + b, 0);
    for (let i = 0; i < comps.length; i++) {
      const c = comps[i];
      const info = prod.get(c.code);
      if (!info || info.supplier !== supplierCode) continue; // 対象仕入先の構成品のみ
      const share = wsum > 0 ? weights[i] / wsum : 1 / comps.length;
      const pieces = u * c.qty;
      const sAlloc = sales * share;
      const p = getProd(c.code);
      if (inWin) {
        p.pieces += pieces; p.sales += sAlloc;
        if (u > 0 && (!p.lastSold || date > p.lastSold)) p.lastSold = date;
        const lk = `${mall}|${listingId}`;
        let L = p.listings.get(lk);
        if (!L) {
          L = { mall, listingId, listingName: listingName || '', asin: asin || '', is_fba: false, sold: 0, pieces: 0, sales: 0 };
          p.listings.set(lk, L);
        }
        L.sold += u; L.pieces += pieces; L.sales += sAlloc;
        if (fbaFee > 0) L.is_fba = true;
        if (listingName && !L.listingName) L.listingName = listingName;
      }
      if (inPrev) { p.prevPieces += pieces; p.prevSales += sAlloc; }
    }
  }

  // 確定(精算ベース)パート。期間 P が無い(finance マート空)場合はスキップ＝速報のみ返す。
  if (P) {
    const params = { s: supplierCode, start: P.prevStart, end: P.end };

    // Amazon: seller_sku を構成品へ展開
    const amzRows = db.prepare(`
      SELECT date_jst d, seller_sku k, asin_norm asin, product_name name,
             CAST(units_net_sold AS REAL) u, sales_principal_jpy sales,
             (fba_fulfillment_jpy + fba_storage_jpy) fbaFee
      FROM mirror_amazon_finance_sku_daily
      WHERE date_jst BETWEEN @start AND @end
        AND LOWER(TRIM(seller_sku)) IN (
          SELECT LOWER(TRIM(seller_sku)) FROM mirror_sku_resolved
          WHERE LOWER(TRIM(ne_code)) IN (SELECT LOWER(TRIM(商品コード)) FROM mirror_products WHERE 仕入先コード = @s))
    `).all(params);
    for (const r of amzRows) {
      const comps = amzMap.get(r.k);
      if (!comps || !comps.length) continue;
      attribute({ mall: 'amazon', listingId: r.k, listingName: r.name, asin: r.asin, fbaFee: r.fbaFee, date: r.d, u: r.u, sales: r.sales, comps });
    }

    // 非 Amazon: fact の ne_code がセットなら展開、単品ならそのまま
    for (const c of NON_AMAZON_MALLS) {
      const rows = db.prepare(`
        SELECT date_jst d, ${c.key} k, ne_code ne, product_name name,
               CAST(units_net_sold AS REAL) u, ${c.sales} sales
        FROM ${c.table}
        WHERE date_jst BETWEEN @start AND @end
          AND ne_code IS NOT NULL AND trim(ne_code) <> ''
          AND (LOWER(TRIM(ne_code)) IN (SELECT LOWER(TRIM(商品コード)) FROM mirror_products WHERE 仕入先コード = @s)
               OR LOWER(TRIM(ne_code)) IN (SELECT LOWER(TRIM(セット商品コード)) FROM mirror_set_components
                              WHERE LOWER(TRIM(構成商品コード)) IN (SELECT LOWER(TRIM(商品コード)) FROM mirror_products WHERE 仕入先コード = @s)))
      `).all(params);
      for (const r of rows) {
        const comps = setMap.get(r.ne) || [{ code: r.ne, qty: 1 }];
        attribute({ mall: c.mall, listingId: r.k, listingName: r.name, asin: '', fbaFee: 0, date: r.d, u: r.u, sales: r.sales, comps });
      }
    }
  }

  // 速報（注文ベース・モール別・毎朝更新）をモール別マート mirror から取得しマージ。
  // 確定(精算ベース,期間可変)と速報(注文ベース,固定7/30日)は基準が違うため列を分けて持つ。
  const sokuho = loadSokuho(db, supplierCode);

  const financePart = (p) => ({
    pieces: round1(p.pieces),
    sales: Math.round(p.sales),
    prevPieces: round1(p.prevPieces),
    prevSales: round1(p.prevSales),
    avgPerDay: P ? round2(p.pieces / P.len) : 0,
    lastSold: p.lastSold,
    listings: [...p.listings.values()]
      .map(L => ({ ...L, pieces: round1(L.pieces), sales: Math.round(L.sales) }))
      .sort((a, b) => b.pieces - a.pieces || b.sales - a.sales),
  });
  const emptyFinance = { pieces: 0, sales: 0, prevPieces: 0, prevSales: 0, avgPerDay: 0, lastSold: null, listings: [] };
  const emptySokuho = { name: '', s7: 0, s30: 0, byMall: new Map() };

  // 確定(finance)と速報(モール別マート)の商品コード和集合
  const allNe = new Set([...out.keys(), ...sokuho.byNe.keys()]);
  const products = [...allNe]
    .map(ne => {
      const f = out.get(ne);
      const sk = sokuho.byNe.get(ne) || emptySokuho;
      const fin = f ? financePart(f) : emptyFinance;
      // 速報モール別内訳（数量0は除外、30日降順）
      const sokuhoMalls = [...(sk.byMall ? sk.byMall.values() : [])]
        .filter(m => m.q7 || m.q30)
        .sort((a, b) => b.q30 - a.q30 || b.q7 - a.q7);
      return {
        ne_code: ne,
        product_name: (f && f.name) || sk.name || '',
        ...fin,
        // 速報（注文ベース）。sokuhoMalls にモール別内訳（楽天/Yahoo/Amazon FBA/FBM…、卸は除外）
        sokuho7: sk.s7 || 0,
        sokuho30: sk.s30 || 0,
        sokuhoMalls,
      };
    })
    .filter(p => p.pieces !== 0 || p.sales !== 0 || p.sokuho30 !== 0 || p.sokuho7 !== 0)
    // 速報30日(今売れてるか)を主、確定売上を従にソート
    .sort((a, b) => b.sokuho30 - a.sokuho30 || b.sales - a.sales || b.pieces - a.pieces);

  const totals = {
    productCount: products.length,
    pieces: round1(products.reduce((a, p) => a + p.pieces, 0)),
    sales: products.reduce((a, p) => a + p.sales, 0),
    prevPieces: round1(products.reduce((a, p) => a + p.prevPieces, 0)),
    prevSales: products.reduce((a, p) => a + p.prevSales, 0),
    sokuho7: products.reduce((a, p) => a + p.sokuho7, 0),
    sokuho30: products.reduce((a, p) => a + p.sokuho30, 0),
  };

  return { period: P, sokuho: { asOf: sokuho.asOf, status: sokuho.status }, products, totals };
}

// 速報モール別の表示ラベルとグルーピング。
//   卸(wholesale)等は仕入先に見せない → SOKUHO_EXCLUDE で完全除外(合計にも入れない)。
//   ラベルに無いモール(rakuma/yahoo_auction/mysmartstore/dshopping 等)は「その他」に集約。
const SOKUHO_MALL_LABELS = {
  rakuten: '楽天', yahoo: 'Yahoo!', aupay: 'au PAY', qoo10: 'Qoo10', mercari: 'メルカリ',
  linegift: 'LINEギフト', amazon_fba: 'Amazon FBA', amazon_fbm: 'Amazon FBM',
};
const SOKUHO_EXCLUDE = new Set(['wholesale']);
// 表示順（中原さん指定）: Amazon(FBA→FBM) → 楽天 → Yahoo! → au PAY → Qoo10 → メルカリ → LINEギフト → その他
const SOKUHO_MALL_ORDER = ['amazon_fba', 'amazon_fbm', 'rakuten', 'yahoo', 'aupay', 'qoo10', 'mercari', 'linegift', 'other'];

// 速報モール別マトリクスの「常時表示する固定列」（売上0でも列を出す＝au PAY等が消えない）。
// 'other'(その他) は固定列に含めず、実データがある時だけ末尾に追加する（views 側で処理）。
export const SOKUHO_MALL_COLUMNS = SOKUHO_MALL_ORDER
  .filter(k => k !== 'other')
  .map(k => ({ key: k, label: SOKUHO_MALL_LABELS[k] }));

// 速報モール別マート(mirror_f_sales_velocity_by_product_mall)から仕入先別の速報販売数を取得。
// 注文ベース・毎朝更新・モール別(楽天/Yahoo/Amazon FBA/FBM…)。卸は除外、未定義モールは「その他」。
// 注意:
//  - rows と as_of は db.transaction で一括読み(mirror 同期 DELETE→INSERT との競合回避)。
//  - マート未生成/未移行(テーブルなし)でも throw でレポート全体を500にしないよう try/catch で空縮退。
function loadSokuho(db, supplier) {
  try {
    return db.transaction(() => {
      const rows = db.prepare(`
        SELECT v.商品コード c, p.商品名 n, v.mall mall, v.qty_7d q7, v.qty_30d q30, v.as_of_date asof
        FROM mirror_f_sales_velocity_by_product_mall v
        JOIN mirror_products p ON p.商品コード = LOWER(TRIM(v.商品コード))
        WHERE p.仕入先コード = @s
      `).all({ s: supplier });
      const byNe = new Map();
      let asOf = null;
      for (const r of rows) {
        if (SOKUHO_EXCLUDE.has(r.mall)) continue; // 卸など除外（合計にも含めない）
        if (r.asof && (!asOf || r.asof > asOf)) asOf = r.asof;
        const key = SOKUHO_MALL_LABELS[r.mall] ? r.mall : 'other';
        const label = SOKUHO_MALL_LABELS[r.mall] || 'その他';
        let e = byNe.get(r.c);
        if (!e) { e = { name: r.n || '', s7: 0, s30: 0, byMall: new Map() }; byNe.set(r.c, e); }
        e.s7 += r.q7 || 0; e.s30 += r.q30 || 0;
        let m = e.byMall.get(key);
        if (!m) { m = { key, label, q7: 0, q30: 0, order: SOKUHO_MALL_ORDER.indexOf(key) }; e.byMall.set(key, m); }
        m.q7 += r.q7 || 0; m.q30 += r.q30 || 0;
      }
      return { asOf, status: null, byNe };
    })();
  } catch (e) {
    console.error('[supplier-sales] loadSokuho skipped:', e.message);
    return { asOf: null, status: null, byNe: new Map() };
  }
}

function round1(n) { return Math.round(n * 10) / 10; }
function round2(n) { return Math.round(n * 100) / 100; }

/**
 * 確定(精算ベース)の日次明細を返す。日付 × モール × 出品 の粒度。
 *   = 「いつ・どのモールで・何個・いくら」の確定データ（CSV用）。
 *   finance マート(date_jst 粒度)由来。原価は出さない。期間 P 内のみ(prev は含めない)。
 *   Amazon は seller_sku→仕入先解決＋FBA/FBM判別、他モールは fact の ne_code。
 * @returns { period, rows:[{date, mall, is_fba, listingId, asin, ne_code, product_name, units, sales}] }
 */
export function getSupplierDailyDetail(db, supplierCode, opts = {}) {
  const P = resolvePeriod(db, opts);
  if (!P) return { period: null, rows: [] };
  const prod = loadProductMap(db);
  const amzMap = loadAmazonMap(db, supplierCode);
  const setMap = loadSetMap(db, supplierCode);
  const params = { s: supplierCode, start: P.start, end: P.end };
  const rows = [];

  // 1出品行を構成品に展開し、対象仕入先の構成品だけを按分して日次行に出す。
  //   数量=出品販売数×構成数量(実出荷ピース)、売上=標準売価×数量比で按分(混在セットで
  //   他仕入先分を載せない=サマリと同一ロジック)。価格欠損時は数量按分にフォールバック。
  function emit({ date, mall, is_fba, listingId, asin, comps, u, sales }) {
    if (!comps || !comps.length) return;
    const allPriced = comps.every(c => (prod.get(c.code)?.price || 0) > 0);
    const weights = comps.map(c => allPriced ? (prod.get(c.code).price * c.qty) : c.qty);
    const wsum = weights.reduce((a, b) => a + b, 0);
    for (let i = 0; i < comps.length; i++) {
      const c = comps[i];
      const info = prod.get(c.code);
      if (!info || info.supplier !== supplierCode) continue; // 対象仕入先の構成品のみ
      const share = wsum > 0 ? weights[i] / wsum : 1 / comps.length;
      const pieces = u * c.qty;
      const sAlloc = sales * share;
      if (!pieces && !sAlloc) continue;
      rows.push({
        date, mall, is_fba, listingId, asin,
        ne_code: c.code, product_name: info.name || '',
        units: round1(pieces), sales: Math.round(sAlloc),
      });
    }
  }

  // Amazon: 日次 × seller_sku（FBA/FBM 判別、構成品展開）
  const amz = db.prepare(`
    SELECT date_jst, seller_sku, asin_norm, product_name,
           CAST(units_net_sold AS REAL) u, sales_principal_jpy sales,
           (fba_fulfillment_jpy + fba_storage_jpy) fbaFee
    FROM mirror_amazon_finance_sku_daily
    WHERE date_jst BETWEEN @start AND @end
      AND seller_sku IN (
        SELECT seller_sku FROM mirror_sku_resolved
        WHERE ne_code IN (SELECT 商品コード FROM mirror_products WHERE 仕入先コード = @s))
  `).all(params);
  for (const r of amz) {
    emit({ date: r.date_jst, mall: 'amazon', is_fba: r.fbaFee > 0, listingId: r.seller_sku, asin: r.asin_norm || '', comps: amzMap.get(r.seller_sku), u: r.u, sales: r.sales });
  }

  // 非 Amazon: 日次 × 出品（fact の ne_code、セットは構成品展開）
  for (const c of NON_AMAZON_MALLS) {
    const rs = db.prepare(`
      SELECT date_jst, ${c.key} k, ne_code, product_name,
             CAST(units_net_sold AS REAL) u, ${c.sales} sales
      FROM ${c.table}
      WHERE date_jst BETWEEN @start AND @end
        AND ne_code IS NOT NULL AND trim(ne_code) <> ''
        AND (ne_code IN (SELECT 商品コード FROM mirror_products WHERE 仕入先コード = @s)
             OR ne_code IN (SELECT セット商品コード FROM mirror_set_components
                            WHERE 構成商品コード IN (SELECT 商品コード FROM mirror_products WHERE 仕入先コード = @s)))
    `).all(params);
    for (const r of rs) {
      const comps = setMap.get(r.ne_code) || [{ code: r.ne_code, qty: 1 }];
      emit({ date: r.date_jst, mall: c.mall, is_fba: false, listingId: r.k, asin: '', comps, u: r.u, sales: r.sales });
    }
  }

  // 日付降順 → モール → 出品
  rows.sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : 0)
    || (a.mall < b.mall ? -1 : a.mall > b.mall ? 1 : 0)
    || (a.listingId < b.listingId ? -1 : 1));
  return { period: P, rows };
}

/**
 * 社内向け: SKU 名寄せの未解決率（直近30日・全社）。仕入先別が過少表示でないかの健全性チェック。
 * 公開側には出さない。
 */
export function getUnresolvedStats(db) {
  const c = confirmedCutoff(db);
  if (!c) return null;
  const p = { w30start: shiftDate(c.cutoff, -29), cutoff: c.cutoff };

  // Amazon: seller_sku が mirror_sku_resolved で解決できない売上を未解決とみなす
  const amz = db.prepare(`
    SELECT SUM(a.sales_principal_jpy) AS total,
           SUM(CASE WHEN r.seller_sku IS NULL THEN a.sales_principal_jpy ELSE 0 END) AS unresolved
    FROM mirror_amazon_finance_sku_daily a
    LEFT JOIN (SELECT DISTINCT seller_sku FROM mirror_sku_resolved) r ON r.seller_sku = a.seller_sku
    WHERE a.date_jst BETWEEN @w30start AND @cutoff
  `).get(p);

  let total = amz.total || 0;
  let unresolved = amz.unresolved || 0;
  for (const c2 of NON_AMAZON_MALLS) {
    const r = db.prepare(`
      SELECT SUM(${c2.sales}) AS total,
             SUM(CASE WHEN ne_code IS NULL OR trim(ne_code) = '' THEN ${c2.sales} ELSE 0 END) AS unresolved
      FROM ${c2.table}
      WHERE date_jst BETWEEN @w30start AND @cutoff
    `).get(p);
    total += r.total || 0;
    unresolved += r.unresolved || 0;
  }

  const rate = total > 0 ? unresolved / total : 0;
  return {
    cutoff: c.cutoff,
    totalSales30: Math.round(total),
    unresolvedSales30: Math.round(unresolved),
    unresolvedRate: rate,
    level: rate > 0.10 ? 'block' : rate > 0.05 ? 'warn' : rate > 0.01 ? 'notice' : 'ok',
  };
}
