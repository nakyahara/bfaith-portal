/**
 * 米国FBA在庫一覧の組み立て (純粋関数・DB も通信も持たない = 試験で丸ごと確かめる)。
 *
 * 入力 = miniPC GET /service-api/fba/us/reports/latest の中身 (apps/warehouse/fba-us-reports-store.js)
 *   { last_attempt, latest: { business_date, reports: { restock: { ok, fetched_at, rows }, planning: {...} } }, file_errors }
 *   rows はレポートの行そのまま (列名 → 文字列)。
 *
 * 🚨 値が取れなかったものは 0 にしない (設計方針 §8 H1)。
 *   列がレポートに無い / 値が空 / 数字でない は null にして、画面は「—」と理由を出す。
 *   列名は日本版の正規化 (sp-api-reports.js normalizeRestockRow / normalizePlanningRow) と同じ候補を、大文字小文字を無視して探す。
 */

// 項目 → レポートの列名の候補。日本版の正規化と同じもの (+ 米国の表記ゆれに備えて大文字小文字は無視)
export const RESTOCK_FIELDS = {
  sku: ['Merchant SKU', 'sku', 'merchant-sku'],
  asin: ['ASIN', 'asin'],
  fnsku: ['FNSKU', 'fnsku'],
  product_name: ['Product Name', 'product-name', '商品名'],
  available: ['Available', 'available', '在庫にある'],
  working: ['Working', 'working', '進行中'],
  shipped: ['Shipped', 'shipped', '出荷済み'],
  receiving: ['Receiving', 'receiving', '受領中'],
  fc_transfer: ['FC Transfer', 'fc-transfer', 'FC移管中'],
  fc_processing: ['FC Processing', 'fc-processing', '入出荷作業中 - FC処理中'],
  customer_order: ['Customer Order', 'customer-order', '入出荷作業中 - 出荷待ち'],
  unfulfillable: ['Unfulfillable', 'unfulfillable', '販売不可'],
  sold_30d: ['Units Sold Last 30 Days', 'units-sold-last-30-days', '過去30日間に販売されたユニット数'],
  amazon_reco_qty: ['Recommended replenishment qty', 'recommended-replenishment-qty', '推奨される在庫補充数'],
  amazon_reco_date: ['Recommended ship date', 'recommended-ship-date', '推奨発送日'],
  alert: ['Alert', 'alert', '警告'],
  price: ['Price', 'price', '価格'],
  days_of_supply: ['Days of Supply at Amazon Fulfillment Network', 'days-of-supply-at-amazon-fulfillment-network'],
};
export const PLANNING_FIELDS = {
  sku: ['sku', 'merchant-sku'],
  asin: ['asin'],
  fnsku: ['fnsku'],
  product_name: ['product-name'],
  sold_7d: ['units-shipped-t7'],
  sold_30d: ['units-shipped-t30'],
  sold_90d: ['units-shipped-t90'],
  price: ['your-price'],
};
// 画面で使う列。1 行にも無ければ「列名が変わった」の帯を出す (参考だけの列 = Amazon の在庫日数・警告などは帯にしない)
const RESTOCK_REQUIRED = ['sku', 'fnsku', 'available', 'working', 'shipped', 'receiving', 'fc_transfer', 'fc_processing', 'customer_order', 'unfulfillable', 'sold_30d', 'amazon_reco_qty'];
const PLANNING_REQUIRED = ['sku', 'sold_7d', 'sold_30d', 'sold_90d'];
const TEXT_FIELDS = new Set(['sku', 'asin', 'fnsku', 'product_name', 'amazon_reco_date', 'alert']);
const STALE_HOURS = 36;   // 毎朝 07:00 の取得が 1 回抜けたら古い扱い (日本の影の下書きの関所と同じ幅)

const keyOf = (s) => String(s).trim().toLowerCase();

/** 1 行を「小文字の列名 → 値」に。候補の列が行に無いのか、あるが空なのかを分けて返す */
function reader(raw) {
  const m = new Map();
  for (const [k, v] of Object.entries(raw || {})) m.set(keyOf(k), v);
  return (aliases) => {
    for (const a of aliases) {
      const k = keyOf(a);
      if (m.has(k)) return { present: true, value: m.get(k) };
    }
    return { present: false, value: undefined };
  };
}

/** 数字の列: 列なし・空・数字でない は null (理由つき)。カンマ区切り ("1,234") は許す */
function numCell(got) {
  if (!got.present) return { v: null, why: 'no_column' };
  const s = got.value == null ? '' : String(got.value).trim();
  if (s === '') return { v: null, why: 'empty' };
  const n = Number(s.replace(/,/g, ''));
  return Number.isFinite(n) ? { v: n, why: null } : { v: null, why: 'not_number' };
}

function parseRow(raw, fields) {
  const get = reader(raw);
  const out = {}; const why = {};
  for (const [f, aliases] of Object.entries(fields)) {
    const got = get(aliases);
    if (TEXT_FIELDS.has(f)) {
      const s = got.present && got.value != null ? String(got.value).trim() : '';
      out[f] = s === '' ? null : s;
    } else {
      const c = numCell(got);
      out[f] = c.v;
      if (c.why) why[f] = c.why;
    }
  }
  return { v: out, why };
}

/** レポート全体で、1 行にも列が無かった項目 (= 列名が変わった・その列が来ない疑い) */
function missingColumns(rows, fields, required) {
  const readers = rows.map(reader);
  return required.filter((f) => !readers.some((get) => get(fields[f]).present));
}

const hoursBetween = (a, b) => (Date.parse(b) - Date.parse(a)) / 3600000;

/**
 * @param {object} payload  miniPC の応答 (ok: true を除いた中身)
 * @param {object} opts
 * @param {(skus: string[]) => Map<string, {route: 'master'|'product_code'|'none'|'unknown', components: {ne_code: string, qty: number}[], name?: string, error?: string}>} opts.resolveSkus
 *        SKU (小文字) → 自社の商品コードへの結びつき
 * @param {Date} [opts.now]
 */
export function buildUsInventoryView(payload, { resolveSkus = () => new Map(), now = new Date() } = {}) {
  const latest = payload && payload.latest;
  const restock = latest && latest.reports && latest.reports.restock;
  const planning = latest && latest.reports && latest.reports.planning;
  const restockRows = (restock && Array.isArray(restock.rows)) ? restock.rows : [];
  const planningRows = (planning && Array.isArray(planning.rows)) ? planning.rows : [];

  // SKU ごとに RESTOCK と PLANNING を合わせる (SKU の大文字小文字は無視して突き合わせ、表示は RESTOCK の表記)
  const bySku = new Map();
  const dupSkus = [];
  const add = (rows, fields, side) => {
    for (const raw of rows) {
      const p = parseRow(raw, fields);
      if (!p.v.sku) continue;
      const k = keyOf(p.v.sku);
      if (!bySku.has(k)) bySku.set(k, { key: k, sku: p.v.sku, restock: null, planning: null });
      const e = bySku.get(k);
      if (e[side]) { dupSkus.push(p.v.sku); continue; }   // 同じレポートに同じ SKU が 2 行 = 後の行は使わない
      e[side] = p;
    }
  };
  add(restockRows, RESTOCK_FIELDS, 'restock');
  add(planningRows, PLANNING_FIELDS, 'planning');

  const mapping = resolveSkus([...bySku.keys()]) || new Map();
  const rows = [...bySku.values()].map((e) => {
    const r = e.restock ? e.restock.v : {};
    const p = e.planning ? e.planning.v : {};
    const unknown = [];   // この行で取れなかった数字の項目 (画面の「—」の理由)
    if (!e.restock) unknown.push('RESTOCK にこの SKU の行なし (在庫の内訳・30日販売が分からない)');
    if (!e.planning) unknown.push('PLANNING にこの SKU の行なし');
    const pick = (src, f, label) => {
      if (!src) return null;
      const why = src.why[f];
      if (why) unknown.push(`${label}: ${why === 'no_column' ? '列なし' : why === 'empty' ? '空' : '数字でない'}`);
      return src.v[f] ?? null;
    };
    const inv = {
      available: pick(e.restock, 'available', '販売可能'),
      working: pick(e.restock, 'working', '準備中'),
      shipped: pick(e.restock, 'shipped', '輸送中'),
      receiving: pick(e.restock, 'receiving', '受領中'),
      fc_transfer: pick(e.restock, 'fc_transfer', 'FC移管中'),
      fc_processing: pick(e.restock, 'fc_processing', 'FC処理中'),
      customer_order: pick(e.restock, 'customer_order', '出荷待ち'),
      unfulfillable: pick(e.restock, 'unfulfillable', '販売不可'),
    };
    const sold30Restock = pick(e.restock, 'sold_30d', '30日販売(RESTOCK)');
    const sold30Planning = e.planning ? pick(e.planning, 'sold_30d', '30日販売(PLANNING)') : null;
    // 日本の計算と同じく RESTOCK を先に使い、無ければ PLANNING (設計方針 §8 H1 の指摘 = 日本は RESTOCK 優先)
    const sold30 = sold30Restock ?? sold30Planning;
    const parts = [inv.available, inv.working, inv.shipped, inv.receiving];
    const onHand = parts.some((x) => x == null) ? null : parts.reduce((a, b) => a + b, 0);
    const daily = sold30 == null ? null : sold30 / 30;
    const coverDays = onHand == null || daily == null ? null : daily > 0 ? Math.round(onHand / daily) : null;
    const m = mapping.get(e.key) || { route: 'unknown', components: [] };
    return {
      sku: e.sku,
      product_name: r.product_name || p.product_name || null,
      asin: r.asin || p.asin || null,
      fnsku: r.fnsku || p.fnsku || null,
      in_restock: !!e.restock, in_planning: !!e.planning,
      ...inv,
      on_hand: onHand,                       // 販売可能 + 準備中 + 輸送中 + 受領中 (どれかが取れなければ null)
      sold_30d: sold30,
      sold_30d_source: sold30Restock != null ? 'restock' : sold30Planning != null ? 'planning' : null,
      sold_30d_restock: sold30Restock, sold_30d_planning: sold30Planning,
      sold_7d: e.planning ? pick(e.planning, 'sold_7d', '7日販売') : null,
      sold_90d: e.planning ? pick(e.planning, 'sold_90d', '90日販売') : null,
      cover_days: coverDays,                  // on_hand ÷ (30日販売 ÷ 30)。売れていなければ null
      amazon_reco_qty: pick(e.restock, 'amazon_reco_qty', 'Amazon推奨数'),
      amazon_reco_date: r.amazon_reco_date || null,
      alert: r.alert || null,
      price_usd: r.price ?? p.price ?? null,
      days_of_supply_amazon: e.restock ? e.restock.v.days_of_supply ?? null : null,
      mapping: m,
      unknown,
    };
  }).sort((a, b) => (b.sold_30d ?? -1) - (a.sold_30d ?? -1) || a.sku.localeCompare(b.sku));

  // 取得の状態 (画面の上の帯)
  const warnings = [];
  const restockAt = restock && restock.ok ? restock.fetched_at : null;
  const planningAt = planning && planning.ok ? planning.fetched_at : null;
  const ageHours = restockAt ? hoursBetween(restockAt, now.toISOString()) : null;
  if (!latest) warnings.push({ level: 'error', text: '米国のレポートがまだ 1 回も保存されていません (毎朝 07:00 の取得のあとに出ます)' });
  else {
    if (!restockAt) warnings.push({ level: 'error', text: `RESTOCK (在庫の内訳・30日販売) が取れていません: ${restock ? restock.error || '不明' : 'レポートなし'}` });
    else if (ageHours > STALE_HOURS) warnings.push({ level: 'error', text: `RESTOCK が古い (${Math.floor(ageHours)} 時間前の取得)。毎朝の取得が止まっている可能性があります` });
    if (!planningAt) warnings.push({ level: 'warn', text: `PLANNING (7日・90日販売) が取れていません: ${planning ? planning.error || '不明' : 'レポートなし'}` });
  }
  const la = payload && payload.last_attempt;
  // 最後の取得で失敗したレポートが、表に出ている分より新しい = 表は前の回の分 (同じ日の再実行で PLANNING だけ失敗した回も。Codex PR1 R1 Medium 2)
  const newerThan = (at) => !at || Date.parse(la.attempted_at) > Date.parse(at);
  if (la && la.reports) {
    if (la.save_error) {
      warnings.push({ level: 'error', text: `最新の取得 (${la.business_date}) はレポートを取れたのに保存できませんでした: ${la.save_error}。下の表は前に保存できた分です` });
    } else {
      if (!(la.reports.restock && la.reports.restock.ok) && newerThan(restockAt)) {
        warnings.push({ level: 'error', text: `最新の取得 (${la.business_date}) で RESTOCK が失敗しています: ${la.error || (la.reports.restock && la.reports.restock.error) || '不明'}。在庫の内訳・30日販売は前に取れた分${restockAt ? ` (${restockAt} の取得)` : ''}です` });
      }
      if (!(la.reports.planning && la.reports.planning.ok) && planningAt && newerThan(planningAt)) {
        warnings.push({ level: 'warn', text: `最新の取得 (${la.business_date}) で PLANNING が失敗しています: ${la.error || (la.reports.planning && la.reports.planning.error) || '不明'}。7日・90日販売は前に取れた分 (${planningAt} の取得) です` });
      }
    }
  }
  for (const fe of (payload && payload.file_errors) || []) warnings.push({ level: 'warn', text: `保存ファイルを読めませんでした (${fe.file}): ${fe.error}` });
  const restockMissing = restockRows.length ? missingColumns(restockRows, RESTOCK_FIELDS, RESTOCK_REQUIRED) : [];
  const planningMissing = planningRows.length ? missingColumns(planningRows, PLANNING_FIELDS, PLANNING_REQUIRED) : [];
  if (restockMissing.length) warnings.push({ level: 'warn', text: `RESTOCK に見つからない列: ${restockMissing.join(', ')} (列名が変わった可能性。この項目は「—」になります)` });
  if (planningMissing.length) warnings.push({ level: 'warn', text: `PLANNING に見つからない列: ${planningMissing.join(', ')}` });
  if (dupSkus.length) warnings.push({ level: 'warn', text: `同じレポートに同じ SKU が 2 行: ${[...new Set(dupSkus)].join(', ')} (2 行目は使っていません)` });
  const unmapped = rows.filter((r) => r.mapping.route === 'none').map((r) => r.sku);
  if (unmapped.length) warnings.push({ level: 'warn', text: `自社の商品コードに結びつかない SKU が ${unmapped.length} 件: ${unmapped.join(', ')} (マスタ登録で SKU を登録してください)` });
  const direct = rows.filter((r) => r.mapping.route === 'product_code').map((r) => r.sku);
  if (direct.length) warnings.push({ level: 'info', text: `SKU マスタに未登録で、商品コードと同じ文字列なので 1 個として結びつけた SKU: ${direct.join(', ')} (セットでなければそのままで可。SKU マスタに登録すると確実になります)` });
  const unresolved = rows.filter((r) => r.mapping.route === 'unknown');
  if (unresolved.length) warnings.push({ level: 'error', text: `商品コードへの結びつきを調べられませんでした: ${(unresolved[0].mapping.error) || '理由不明'}` });

  // 合計: 1 SKU でも分からなければ合計も分からない (null)。何 SKU 分からないかを別に返す (Codex PR1 R1 Medium 1 = 欠けを 0 として足していた)
  const unknownCount = (fs) => rows.filter((r) => fs.some((f) => r[f] == null)).length;
  const sum = (...fs) => (unknownCount(fs) > 0 ? null : rows.reduce((a, r) => a + fs.reduce((b, f) => b + r[f], 0), 0));
  return {
    business_date: latest ? latest.business_date : null,
    restock_fetched_at: restockAt,
    planning_fetched_at: planningAt,
    last_attempt: la ? { business_date: la.business_date, attempted_at: la.attempted_at, restock_ok: !!(la.reports && la.reports.restock && la.reports.restock.ok), planning_ok: !!(la.reports && la.reports.planning && la.reports.planning.ok), error: la.error || null } : null,
    warnings,
    totals: {
      skus: rows.length,
      selling_skus: unknownCount(['sold_30d']) > 0 ? null : rows.filter((r) => r.sold_30d > 0).length,
      available: sum('available'),
      inbound: sum('working', 'shipped', 'receiving'),
      sold_30d: sum('sold_30d'),
    },
    // 合計が null のとき、分からない SKU の数 (画面は「—」+「n SKU 不明」)
    totals_unknown: {
      selling_skus: unknownCount(['sold_30d']),
      available: unknownCount(['available']),
      inbound: unknownCount(['working', 'shipped', 'receiving']),
      sold_30d: unknownCount(['sold_30d']),
    },
    rows,
  };
}
