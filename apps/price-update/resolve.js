/**
 * price-update / 出品引き当て (要件定義 v1.0 F1)
 *
 * NE商品コードから「どのモールのどの出品コードか」を引き当てる。読むのは mirror だけで、
 * ライブ確認 (実在するか・今いくらか) は live-price.js が API で行う。
 *
 * ★信頼度モデル (要件 F1)。更新できるのは confirmed だけ:
 *   confirmed  … 確定マスタ or カタログAPIで実在確認済み (= ライブ価格が取れた行)
 *   rule       … 規則推定 (出品コード = NEコード 等)。候補表示のみ
 *   sales      … 販売実績からの推定 (実績はあるが今の出品と結びつくか未確認)
 *   unresolved … 引き当てできない
 *
 * ここでは mirror 由来はすべて rule 以下に置く。「マスタにある = 今も出品されている」ではないため、
 * confirmed への昇格は live-price.js の実在確認を通った行だけに限る (fail-closed)。
 *
 * 🚨「未出品」と言い切れるのは全出品カタログ照合で否定を証明できる時だけ (要件 F1)。
 *    それ以外は「未解決」として出す。
 */

/** 商品コードの照合キー (SKU は LOWER(TRIM()) が家ルール) */
export function normCode(v) {
  return String(v == null ? '' : v).trim().toLowerCase();
}

/** mirror にまだ無いテーブル (PR1 未マージの環境など) を fail-soft で扱う */
function tableExists(db, name) {
  try {
    return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
  } catch {
    return false;
  }
}

function placeholders(n) {
  return new Array(n).fill('?').join(',');
}

/** 入力された NEコード群の商品行 (mirror_products) を引く */
export function loadProducts(db, codes) {
  const keys = [...new Set(codes.map(normCode).filter(Boolean))];
  if (keys.length === 0) return new Map();
  const rows = db.prepare(`
    SELECT 商品コード AS code, 商品名 AS name, 商品区分 AS kind, 取扱区分 AS handling,
           標準売価 AS listPrice, 原価 AS cost, 原価状態 AS costStatus, 送料 AS shipping,
           消費税率 AS taxRate, セット構成品数 AS setCount
      FROM mirror_products
     WHERE LOWER(TRIM(商品コード)) IN (${placeholders(keys.length)})
  `).all(...keys);
  return new Map(rows.map((r) => [normCode(r.code), r]));
}

/**
 * 入力コードを構成に含むセット商品を探す (要件 F1「セット展開の逆引き」)。
 * @returns {Array<{setCode:string, viaCode:string, qty:number}>}
 */
export function findSetsContaining(db, codes) {
  const keys = [...new Set(codes.map(normCode).filter(Boolean))];
  if (keys.length === 0) return [];
  const rows = db.prepare(`
    SELECT セット商品コード AS setCode, 構成商品コード AS viaCode, 数量 AS qty
      FROM mirror_set_components
     WHERE LOWER(TRIM(構成商品コード)) IN (${placeholders(keys.length)})
     ORDER BY セット商品コード
  `).all(...keys);
  return rows.map((r) => ({ setCode: r.setCode, viaCode: r.viaCode, qty: r.qty || 1 }));
}

/**
 * セット商品の原価を構成品から再計算 (要件 F3)。
 * 構成品の原価が1つでも欠けたら null (欠けた分を 0 円として足し込まない)。
 * @param {Map<string, number|null>} costOverrides 値上げ後の原価 (NEコード → 税抜原価)
 */
export function setCostOf(db, setCode, costOverrides = new Map()) {
  const comps = db.prepare(`
    SELECT 構成商品コード AS code, 数量 AS qty, 構成商品原価 AS cost
      FROM mirror_set_components WHERE LOWER(TRIM(セット商品コード)) = ?
  `).all(normCode(setCode));
  if (comps.length === 0) return { cost: null, missing: [], components: [] };
  const missing = [];
  let total = 0;
  const components = [];
  for (const c of comps) {
    const key = normCode(c.code);
    const override = costOverrides.has(key) ? costOverrides.get(key) : undefined;
    const cost = override !== undefined && override !== null ? Number(override)
      : (c.cost == null ? null : Number(c.cost));
    const qty = Number(c.qty) || 1;
    components.push({ code: c.code, qty, cost, overridden: override !== undefined && override !== null });
    if (cost == null || !Number.isFinite(cost)) { missing.push(c.code); continue; }
    total += cost * qty;
  }
  return { cost: missing.length > 0 ? null : total, missing, components };
}

/**
 * 楽天: mirror_rakuten_sku_map (AM/AL/W の逆引き)。
 *
 * ★AM/AL/W は「別々の出品」ではなく **同じ 1 SKU の別名** (2026-08-30 実機で確認):
 *   AM = merchantDefinedSkuId (システム連携用SKU番号) / AL = variants のキー (SKU管理番号) /
 *   W  = itemNumber (商品番号。これが manageNumber になる)
 *   例) ne_code=0726-001802-bk → AL=360 / AM=0726-001802-bk / W=0726-001802 で、
 *       実体は「manageNumber=0726-001802 の variant 360」1つだけ。
 *
 * ここで別名ごとに行を作ると、同じ SKU が 3 行に見えるうえ、AL/AM は manageNumber として
 * 引けないので全部「見つかりません」になる。→ **1 SKU = 1 行**にまとめ、別名は候補として持つ。
 * どの manageNumber / variant かの確定は live-price.js が API で行う。
 */
function resolveRakuten(db, code) {
  const rows = db.prepare(`
    SELECT rakuten_code AS listingCode, source
      FROM mirror_rakuten_sku_map WHERE LOWER(TRIM(ne_code)) = ? ORDER BY source, rakuten_code
  `).all(normCode(code));
  if (rows.length === 0) return [];
  const aliases = rows.map((r) => r.listingCode);
  // 表示は W (商品番号 = manageNumber) を優先。無ければ最初の別名 (ライブ取得後に実際の管理番号へ差し替わる)
  const w = rows.find((r) => r.source === 'w');
  return [{
    mall: 'rakuten',
    listingCode: w ? w.listingCode : aliases[0],
    aliases,
    skuCode: null,
    confidence: 'rule',
    source: `mirror_rakuten_sku_map(${rows.map((r) => r.source).join('/')})`,
  }];
}

/** Yahoo: mirror_yahoo_sku_map (手動map) + 「出品コード = NEコード」規則 */
function resolveYahoo(db, code) {
  const out = [];
  if (tableExists(db, 'mirror_yahoo_sku_map')) {
    for (const r of db.prepare(`
      SELECT yahoo_key AS listingCode FROM mirror_yahoo_sku_map
       WHERE LOWER(TRIM(ne_code)) = ? ORDER BY yahoo_key
    `).all(normCode(code))) {
      out.push({ mall: 'yahoo', listingCode: r.listingCode, skuCode: null, confidence: 'rule', source: 'mirror_yahoo_sku_map' });
    }
  }
  if (!out.some((x) => normCode(x.listingCode) === normCode(code))) {
    out.push({ mall: 'yahoo', listingCode: code, skuCode: null, confidence: 'rule', source: '規則 (出品コード=NEコード)' });
  }
  return out;
}

/** au PAY: mirror_aupay_sku_map + 「item_code = NEコード」規則。価格は出さない (要件 F2) */
function resolveAupay(db, code) {
  const out = [];
  if (tableExists(db, 'mirror_aupay_sku_map')) {
    for (const r of db.prepare(`
      SELECT aupay_key AS listingCode FROM mirror_aupay_sku_map
       WHERE LOWER(TRIM(ne_code)) = ? ORDER BY aupay_key
    `).all(normCode(code))) {
      out.push({ mall: 'aupay', listingCode: r.listingCode, skuCode: null, confidence: 'rule', source: 'mirror_aupay_sku_map' });
    }
  }
  if (!out.some((x) => normCode(x.listingCode) === normCode(code))) {
    out.push({ mall: 'aupay', listingCode: code, skuCode: null, confidence: 'rule', source: '規則 (item_code=NEコード)' });
  }
  return out;
}

/** Qoo10: m_qoo10_items.seller_code (販売者商品コード) */
function resolveQoo10(db, code) {
  if (!tableExists(db, 'mirror_qoo10_items')) return [];
  return db.prepare(`
    SELECT item_no AS listingCode, item_name AS itemName FROM mirror_qoo10_items
     WHERE LOWER(TRIM(seller_code)) = ? ORDER BY item_no
  `).all(normCode(code)).map((r) => ({
    mall: 'qoo10', listingCode: r.listingCode, skuCode: null,
    confidence: 'rule', source: 'mirror_qoo10_items', itemName: r.itemName,
  }));
}

/** Amazon: 表示のみ (更新対象外)。seller_sku は mirror_sku_resolved 経由 */
function resolveAmazon(db, code) {
  return db.prepare(`
    SELECT seller_sku AS listingCode, source, quantity FROM mirror_sku_resolved
     WHERE LOWER(TRIM(ne_code)) = ? ORDER BY seller_sku
  `).all(normCode(code)).map((r) => ({
    mall: 'amazon',
    listingCode: r.listingCode,
    skuCode: null,
    // Amazon は更新しないので信頼度は表示用。master 由来だけ rule、自動解決は sales 相当
    confidence: r.source === 'master' ? 'rule' : 'sales',
    source: `mirror_sku_resolved(${r.source})`,
    quantity: r.quantity,
  }));
}

export const MALLS = ['rakuten', 'yahoo', 'amazon', 'aupay', 'qoo10'];
/** API で価格を更新できる (予定の) モール。M1 は読むだけ */
export const UPDATABLE_MALLS = ['rakuten', 'yahoo'];

/**
 * 1 NEコードの全モール出品候補。
 *
 * ★候補が1つも無いモールは行を消さず「未解決」として出す (要件 F1)。
 *   行が消えると「そのモールには出品していない」と読めてしまうが、それを言い切れるのは
 *   全出品カタログと照合して否定できた時だけ。引き当てられなかっただけなら、そう表示する。
 */
export function resolveListings(db, code) {
  const byMall = {
    rakuten: resolveRakuten(db, code),
    yahoo: resolveYahoo(db, code),
    amazon: resolveAmazon(db, code),
    aupay: resolveAupay(db, code),
    qoo10: resolveQoo10(db, code),
  };
  const out = [];
  for (const mall of MALLS) {
    const found = byMall[mall] || [];
    if (found.length > 0) { out.push(...found); continue; }
    out.push({
      mall,
      listingCode: null,
      skuCode: null,
      confidence: 'unresolved',
      source: '出品コードが見つかりませんでした (出品が無いとは限りません)',
    });
  }
  return out;
}

/** 商品ページ / 管理画面の URL (要件 F5) */
export function listingUrl(mall, listingCode, extra = {}) {
  const c = String(listingCode || '');
  if (!c) return null;
  switch (mall) {
    case 'rakuten': return `https://item.rakuten.co.jp/b-faith/${encodeURIComponent(c)}/`;
    case 'yahoo': return `https://store.shopping.yahoo.co.jp/b-faith01/${encodeURIComponent(c)}.html`;
    case 'amazon': return extra.asin ? `https://www.amazon.co.jp/dp/${encodeURIComponent(extra.asin)}` : null;
    // auPAY / Qoo10 は商品ページを組み立てず管理画面へ送る (出品コードから公開URLを機械的に作れない)
    case 'aupay': return 'https://manager.wowma.jp/';
    case 'qoo10': return 'https://qsm.qoo10.jp/';
    default: return null;
  }
}

/**
 * 画面に出す行を組み立てる。
 *
 * @param db mirror DB ハンドル
 * @param {string[]} inputCodes 入力された NEコード
 * @param {object} opts
 * @param {Map<string,number>} [opts.costOverrides] 新原価 (税抜) の入力 NEコード→円
 * @returns {{targets:Array, unknownCodes:string[]}}
 *   targets[i] = { neCode, name, rowKind, viaCode, cost, costSource, taxRate, shipping, listings:[...] }
 */
export function buildTargets(db, inputCodes, opts = {}) {
  const costOverrides = opts.costOverrides instanceof Map ? opts.costOverrides : new Map();
  const codes = [...new Set(inputCodes.map((c) => String(c || '').trim()).filter(Boolean))];
  const products = loadProducts(db, codes);
  const unknownCodes = codes.filter((c) => !products.has(normCode(c)));

  const targets = [];
  const seen = new Set();

  const pushTarget = (code, rowKind, viaCode) => {
    const key = normCode(code);
    if (!key || seen.has(key)) return;
    const p = products.get(key) || loadProducts(db, [code]).get(key);
    if (!p) return;
    seen.add(key);
    const override = costOverrides.has(key) ? costOverrides.get(key) : null;
    let cost = override != null ? override : (p.cost == null ? null : Number(p.cost));
    let costSource = override != null ? '入力された新原価' : 'm_products.原価';
    let setInfo = null;
    if (rowKind === 'set') {
      setInfo = setCostOf(db, code, costOverrides);
      if (setInfo.cost != null) {
        cost = setInfo.cost;
        costSource = '構成品から再計算';
      } else if (override == null) {
        cost = null;
        costSource = `構成品の原価が未登録 (${setInfo.missing.join(', ')})`;
      }
    }
    targets.push({
      neCode: p.code,
      name: p.name,
      rowKind,
      viaCode: viaCode || null,
      kind: p.kind,
      handling: p.handling,
      cost,
      costSource,
      taxRate: p.taxRate == null ? null : Number(p.taxRate),
      shipping: p.shipping == null ? null : Number(p.shipping),
      listPrice: p.listPrice == null ? null : Number(p.listPrice),
      setComponents: setInfo ? setInfo.components : null,
      listings: resolveListings(db, p.code),
    });
  };

  for (const c of codes) pushTarget(c, 'single', null);
  // セットは初期未選択で一覧に足す (要件 決定事項#3)
  for (const s of findSetsContaining(db, codes)) pushTarget(s.setCode, 'set', s.viaCode);

  return { targets, unknownCodes };
}
