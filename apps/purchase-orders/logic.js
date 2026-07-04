/**
 * purchase-orders 発注計算エンジン
 *
 * 旧スプレッドシート「発注対象商品」の数式を完全移植する:
 *   在庫月数 L      = (総在庫数_引当なし + 注残数) / 30日販売数合計   (販売0なら0)
 *   在庫定数 O      = M<=1→0.5, 1<M<=2→1, 2<M<=3→2, M>3→3   (M=推奨保有月数)
 *   目標月数 P      = M + O
 *   発注対象        = 取扱区分='取扱中' かつ 0 < L <= M
 *   推奨発注量      = lots = (P-L)*V/N を N(発注ロット単位)で丸め
 *                     lots > 1 → ROUND(lots)*N / lots <= 1 → ROUNDUP(lots)*N (最低1ロット)
 *   掘り起こし対象  = L = 0 (在庫+注残ゼロ、または30日販売ゼロ)
 *     ※旧シートは取扱中止も掘り起こしに含めていたが、本アプリは「取扱中」のみに絞る (ノイズ除去)
 *
 * データソース: mirror_pml_snapshot_rows (published run) + po_* マスタ。
 */
import { getDB, normSupplierCode, normProductCode } from './db.js';

const PML_COLS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '売上分類',
  '総在庫数_引当なし', '注残数', '販売数7日_合計', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '最終仕入日',
];

const num = v => {
  if (v == null || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

/** 在庫定数 (推奨保有月数に応じた上乗せバッファ月数) — 旧シート IFS を忠実移植 */
export function stockConstant(m) {
  if (m <= 1) return 0.5;
  if (m <= 2) return 1;
  if (m <= 3) return 2;
  return 3;
}

/** 1商品の発注判定。旧シートの1行分。 */
export function computeProduct(r) {
  const S = num(r['総在庫数_引当なし']);
  const B = num(r['注残数']);
  const V = num(r['販売数30日_合計']);
  const M = num(r['推奨保有月数']);
  const N = num(r['発注ロット単位']);
  const active = String(r['取扱区分'] || '') === '取扱中';
  const stockMonths = V > 0 ? (S + B) / V : 0; // L
  const isTarget = active && stockMonths > 0 && stockMonths <= M;
  let recQty = null;
  if (isTarget && N > 0) {
    const P = M + stockConstant(M);
    const lots = ((P - stockMonths) * V) / N;
    recQty = lots > 1 ? Math.round(lots) * N : Math.ceil(lots) * N;
  }
  return {
    code: r['商品コード'],
    key: normProductCode(r['商品コード']),
    name: r['商品名'] || '',
    supplierCode: normSupplierCode(r['仕入先']),
    active,
    salesClass: r['売上分類'] == null ? '' : String(r['売上分類']),
    stock: S,
    backOrder: B,
    sales30: V,
    sales7: num(r['販売数7日_合計']),
    lot: N,
    holdMonths: M,
    price: num(r['売価']),
    cost: num(r['原価']),
    lastPurchase: r['最終仕入日'] || '',
    stockMonths,
    isTarget,
    recQty,
    isHorikoshi: active && stockMonths === 0,
  };
}

/** published PML を読む (pub+rows を1トランザクションで) */
export function loadPml() {
  const db = getDB();
  return db.transaction(() => {
    const pub = db.prepare('SELECT * FROM mirror_pml_published WHERE id=1').get();
    if (!pub) return { pub: null, rows: [] };
    const rows = db.prepare(
      `SELECT ${PML_COLS.join(', ')} FROM mirror_pml_snapshot_rows WHERE run_id=?`
    ).all(pub.run_id);
    return { pub, rows };
  })();
}

/** マスタ一式をロード */
export function loadMasters() {
  const db = getDB();
  const suppliers = new Map();
  for (const s of db.prepare('SELECT * FROM po_suppliers').all()) {
    suppliers.set(normSupplierCode(s.supplier_code), s);
  }
  const conditions = db.prepare('SELECT * FROM po_order_conditions ORDER BY condition_id').all();
  const materialGroups = new Map();
  for (const g of db.prepare('SELECT * FROM po_material_groups').all()) {
    materialGroups.set(g.group_id, g);
  }
  const attrs = new Map();
  for (const a of db.prepare('SELECT * FROM po_product_attrs').all()) {
    attrs.set(a.product_key, a);
  }
  return { suppliers, conditions, materialGroups, attrs };
}

/** 直近 issuedDays 日以内に発注確定済みの商品 → { product_key: {orderId, issuedAt, qty} } */
export function loadRecentIssued(issuedDays = 14) {
  const db = getDB();
  const since = new Date(Date.now() - issuedDays * 86400000).toISOString();
  const rows = db.prepare(`
    SELECT i.product_key, i.qty, o.id AS order_id, o.issued_at
    FROM po_order_items i JOIN po_orders o ON o.id = i.order_id
    WHERE o.status = 'issued' AND o.issued_at >= ?
    ORDER BY o.issued_at ASC
  `).all(since);
  const map = new Map();
  for (const r of rows) map.set(r.product_key, { orderId: r.order_id, issuedAt: r.issued_at, qty: r.qty });
  return map;
}

/**
 * 全体計算。仕入先別に 要発注 / ついで買い候補 / 掘り起こし を仕分けする。
 * 戻り値の products は PML 全行の computeProduct 結果 (attrs 情報を付与済み)。
 */
export function computeAll() {
  // PML・マスタ・直近発注を1つの read transaction で読む (途中の書き込みと混在させない、Codex R2 Low)
  const db = getDB();
  const { pub, rows, masters, recentIssued } = db.transaction(() => ({
    ...loadPml(), masters: loadMasters(), recentIssued: loadRecentIssued(),
  }))();
  const products = [];
  const bySupplier = new Map();
  for (const r of rows) {
    const p = computeProduct(r);
    const a = masters.attrs.get(p.key);
    p.conditionId = a ? (a.condition_id || '') : '';
    p.materialGroupId = a ? (a.material_group_id || '') : '';
    p.capacityPerUnit = a ? (a.capacity_per_unit || null) : null;
    p.caseLot = a ? (a.case_lot || null) : null;
    const ri = recentIssued.get(p.key);
    p.recentIssued = ri || null;
    products.push(p);
    if (!p.supplierCode) continue;
    let g = bySupplier.get(p.supplierCode);
    if (!g) {
      const sm = masters.suppliers.get(p.supplierCode);
      g = {
        code: p.supplierCode,
        name: sm ? sm.name : '',
        memo: sm ? (sm.order_memo || '') : '',
        targets: [], candidates: [], horikoshi: [],
      };
      bySupplier.set(p.supplierCode, g);
    }
    if (p.isTarget) g.targets.push(p);
    else if (p.isHorikoshi) g.horikoshi.push(p);
    else if (p.active && p.stockMonths > 0) g.candidates.push(p);
  }
  for (const g of bySupplier.values()) {
    g.targets.sort((a, b) => a.stockMonths - b.stockMonths);
    g.candidates.sort((a, b) => a.stockMonths - b.stockMonths);
    g.horikoshi.sort((a, b) => (b.lastPurchase || '').localeCompare(a.lastPurchase || ''));
    g.estAmount = Math.round(g.targets.reduce((s, p) => s + (p.recQty || 0) * p.cost, 0));
  }
  return { pub, products, bySupplier, masters };
}

/**
 * 発注条件の充足評価 (自動判定は 数量[個] / 金額[円] のみ。他タイプは表示専用)。
 * items: [{key, qty, cost}] — カート内容 (qty=発注数量, cost=単価原価)
 */
export function evaluateCondition(cond, memberKeys, items) {
  const inGroup = items.filter(i => memberKeys.has(i.key));
  const sumQty = inGroup.reduce((s, i) => s + i.qty, 0);
  const sumAmount = Math.round(inGroup.reduce((s, i) => s + i.qty * (i.cost || 0), 0));
  let auto = null; // null=自動判定不可
  if (cond.condition_type === '数量' && (cond.unit === '個' || !cond.unit)) {
    auto = { current: sumQty, required: cond.condition_value, met: sumQty >= cond.condition_value, kind: 'qty' };
  } else if (cond.condition_type === '金額') {
    auto = { current: sumAmount, required: cond.condition_value, met: sumAmount >= cond.condition_value, kind: 'amount' };
  }
  return { sumQty, sumAmount, auto };
}
