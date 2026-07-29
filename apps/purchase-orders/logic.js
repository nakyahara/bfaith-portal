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
 *   掘り起こし対象  = 取扱中 かつ 在庫0 かつ 注残0 (中原さん定義 2026-07-14:
 *     他社の値下げで価格が合わず仕入を控えた商品を、一定期間後に再販できるか調べるためのステータス)
 *     ※旧シートは取扱中止も掘り起こしに含めていたが、本アプリは「取扱中」のみに絞る (ノイズ除去)
 *     ※在庫があって販売0の商品は掘り起こしではない → ついで買い候補の末尾に回す
 *   セット商品 (商品区分='セット') は対象外 = 一覧にも出さない (在庫・発注は構成品側で管理)
 *
 * データソース: mirror_pml_snapshot_rows (published run) + po_* マスタ。
 */
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { getSetting } from './ledger.js';

const PML_COLS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '商品区分', '売上分類',
  '総在庫数_引当なし', 'FBA在庫数', '注残数', '販売数7日_合計', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '最終仕入日', '登録日',
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

/** 1商品の発注判定。旧シートの1行分。backOrderOverride を渡すとNE CSVの注残数の代わりに使う (アプリ台帳の残数) */
export function computeProduct(r, backOrderOverride) {
  const S = num(r['総在庫数_引当なし']);
  const B = backOrderOverride != null ? backOrderOverride : num(r['注残数']);
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
    // 掘り起こし = 取扱中かつ在庫0・注残0 (仕入を控えた商品の再販調査。販売0でも在庫があれば対象外)
    isHorikoshi: active && S === 0 && B === 0,
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

/** NE商品マスタCSVオーバーレイをロード */
export function loadNeOverlay() {
  const db = getDB();
  const meta = db.prepare('SELECT * FROM po_ne_overlay_meta WHERE id=1').get() || null;
  const rows = new Map();
  if (meta) {
    for (const r of db.prepare('SELECT * FROM po_ne_overlay_rows').all()) rows.set(r.product_key, r);
  }
  return { meta, rows };
}

/**
 * published PML に NEオーバーレイ (日中手動DLのNE商品マスタCSV) をマージして返す。
 * 上書き対象: 取扱区分 / 仕入先 / 原価 / 売価 / 発注ロット単位 / 最終仕入日 / 注残数(=NE発注残数) /
 *             総在庫数_引当なし (= NE在庫数 + PML.FBA在庫数)
 * 据え置き: FBA在庫数・販売数・推奨保有月数 (PMLのまま)。
 * overlay が朝のNE同期より古い場合は自動で無視する (applied=false)。
 */
export function loadPmlMerged() {
  const db = getDB();
  return db.transaction(() => {
    const { pub, rows } = loadPml();
    const ov = loadNeOverlay();
    let overlay = null;
    let applied = false;
    if (ov.meta) {
      applied = true;
      if (pub && pub.src_ne_products_synced_at) {
        const a = Date.parse(ov.meta.uploaded_at);
        const b = Date.parse(pub.src_ne_products_synced_at);
        if (!Number.isNaN(a) && !Number.isNaN(b) && a < b) applied = false; // 朝同期の方が新しい
      }
      overlay = { uploaded_at: ov.meta.uploaded_at, row_count: ov.meta.row_count, filename: ov.meta.filename, source: ov.meta.source || 'ne', applied, mergedCount: 0 };
    }
    const merged = !applied ? rows : rows.map(r => {
      const o = ov.rows.get(normProductCode(r['商品コード']));
      if (!o) return r;
      overlay.mergedCount++;
      return {
        ...r,
        '取扱区分': o.取扱区分 != null && o.取扱区分 !== '' ? o.取扱区分 : r['取扱区分'],
        '仕入先': o.仕入先コード != null && o.仕入先コード !== '' ? o.仕入先コード : r['仕入先'],
        '原価': o.原価 != null ? o.原価 : r['原価'],
        '売価': o.売価 != null ? o.売価 : r['売価'],
        '発注ロット単位': o.発注ロット単位 != null ? o.発注ロット単位 : r['発注ロット単位'],
        '最終仕入日': o.最終仕入日 != null && o.最終仕入日 !== '' ? o.最終仕入日 : r['最終仕入日'],
        '注残数': o.発注残数 != null ? o.発注残数 : r['注残数'],
        '総在庫数_引当なし': o.在庫数 != null ? o.在庫数 + num(r['FBA在庫数']) : r['総在庫数_引当なし'],
      };
    });
    return { pub, rows: merged, overlay };
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
  const selectable = new Map();
  for (const s of db.prepare('SELECT * FROM po_selectable_products').all()) {
    selectable.set(s.product_key, s);
  }
  return { suppliers, conditions, materialGroups, attrs, selectable };
}

/** 選べるセット構成商品の既定最低在庫 (min_stock 未設定時) */
export const SELECTABLE_DEFAULT_MIN = 10;

/** 直近 issuedDays 日以内に発注確定済みの商品 → { product_key: {orderId, issuedAt, qty} } */
export function loadRecentIssued(issuedDays = 14) {
  const db = getDB();
  const since = new Date(Date.now() - issuedDays * 86400000).toISOString();
  // 移行PO (NE発注残初期取込) は除外: その数量はNE由来でPMLの注残数に反映済みのため、
  // 「発注済み (NE反映待ち)」バッジを付けると二重カウントになる
  const rows = db.prepare(`
    SELECT i.product_key, i.qty, o.id AS order_id, o.issued_at
    FROM po_order_items i JOIN po_orders o ON o.id = i.order_id
    WHERE o.status = 'issued' AND o.issued_at >= ? AND (o.origin IS NULL OR o.origin <> 'migration')
    ORDER BY o.issued_at ASC
  `).all(since);
  const map = new Map();
  for (const r of rows) map.set(r.product_key, { orderId: r.order_id, issuedAt: r.issued_at, qty: r.qty });
  return map;
}

/**
 * アプリ台帳の商品別発注残 (残数>0のオープン発注のみ、移行PO含む) → Map(product_key → 残数計)。
 * NE CSVの「発注残数」の代わりに要発注判定へ使う: 発注確定した瞬間に注残へ反映され、
 * NE CSV/朝同期の取込を待たずに要発注から消える (二重発注防止)。
 */
export function loadLedgerBackorders() {
  const db = getDB();
  // 集計ロジックは v_ledger_backorder_by_product に一本化 (PML正本ビュー v_pml_rows_authoritative・
  // 整合性検査と同一定義。tracked の正は issued_at >= tracking_started_at、Codex サイクルR1 High / SSoT化 2026-07-13)
  const map = new Map();
  for (const r of db.prepare('SELECT product_key, backorder_qty FROM v_ledger_backorder_by_product').all()) {
    map.set(r.product_key, r.backorder_qty);
  }
  return map;
}

/**
 * 全体計算。仕入先別に 要発注 / ついで買い候補 / 掘り起こし を仕分けする。
 * 戻り値の products は PML 全行の computeProduct 結果 (attrs 情報を付与済み)。
 * 注残はアプリ台帳 (loadLedgerBackorders) を正とする — NE CSVの発注残数は使わない。
 */
export function computeAll() {
  // PML(+NEオーバーレイ)・マスタ・直近発注を1つの read transaction で読む (途中の書き込みと混在させない、Codex R2 Low)
  const db = getDB();
  const { pub, rows, overlay, masters, recentIssued, ledgerZan, useLedgerZan } = db.transaction(() => ({
    ...loadPmlMerged(), masters: loadMasters(), recentIssued: loadRecentIssued(), ledgerZan: loadLedgerBackorders(),
    // 既定 'app' = アプリ台帳。設定 backorder_source='ne' で旧挙動 (NE CSVの発注残数) へ戻せる
    useLedgerZan: getSetting('backorder_source') !== 'ne',
  }))();
  const products = [];
  const bySupplier = new Map();
  for (const r of rows) {
    // セット商品は発注・在庫管理の対象外 (在庫は構成品側)。全商品情報にも出さない (中原さん 2026-07-14)
    if (String(r['商品区分'] || '').trim() === 'セット') continue;
    const p = computeProduct(r, useLedgerZan ? (ledgerZan.get(normProductCode(r['商品コード'])) || 0) : undefined);
    const a = masters.attrs.get(p.key);
    p.conditionId = a ? (a.condition_id || '') : '';
    p.materialGroupId = a ? (a.material_group_id || '') : '';
    p.capacityPerUnit = a ? (a.capacity_per_unit || null) : null;
    p.caseGroup = a ? (a.case_group || '') : '';
    p.caseLot = a ? (a.case_lot || null) : null;
    const ri = recentIssued.get(p.key);
    p.recentIssued = ri || null;
    // 選べる◯種セット構成商品: 在庫を切らさない前提のため、在庫+注残 が最低在庫以下なら要発注扱い
    const sel = masters.selectable.get(p.key);
    const selMin = sel ? (sel.min_stock != null ? sel.min_stock : SELECTABLE_DEFAULT_MIN) : null;
    p.selectableLow = (sel && p.active && (p.stock + p.backOrder) <= selMin)
      ? { sets: sel.set_names || '', minStock: selMin } : null;
    // 通常式で推奨発注が出ない場合 (販売30日=0等) は「最低在庫の2倍まで補充」をロット切上げで推奨 (Codex P8-1)
    if (p.selectableLow && !p.recQty) {
      const lot = p.lot > 0 ? p.lot : 1;
      const need = Math.max(0, selMin * 2 - (p.stock + p.backOrder));
      p.recQty = need > 0 ? Math.ceil(need / lot) * lot : lot;
    }
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
    if (p.isTarget || p.selectableLow) g.targets.push(p);
    else if (p.isHorikoshi) g.horikoshi.push(p);
    // 在庫あり販売0 (stockMonths=0) もついで買い候補に含める (掘り起こしから外れた死に筋の受け皿。
    // どのリストにも載らないとワークスペース検索・カート追加から消えてしまう)
    else if (p.active) g.candidates.push(p);
  }
  for (const g of bySupplier.values()) {
    g.targets.sort((a, b) => a.stockMonths - b.stockMonths);
    // 販売0 (stockMonths=0) は末尾へ。それ以外は在庫月数の少ない順
    g.candidates.sort((a, b) => ((a.stockMonths === 0) - (b.stockMonths === 0)) || a.stockMonths - b.stockMonths);
    g.horikoshi.sort((a, b) => (b.lastPurchase || '').localeCompare(a.lastPurchase || ''));
    g.estAmount = Math.round(g.targets.reduce((s, p) => s + (p.recQty || 0) * p.cost, 0));
  }
  return { pub, overlay, products, bySupplier, masters };
}

/**
 * 発注条件の充足評価。旧GAS参照ツール (発注条件マスタ) の全条件タイプを自動判定する。
 *   数量[個]           Σ数量 ≧ 条件値
 *   金額[円]           Σ(数量×原価) ≧ 条件値
 *   上限               Σ数量 ≦ 条件値 (仕入先の出荷制限。例: びわこふきん300枚まで)
 *   数量[ケース]       Σ(数量÷ケース入数) ≧ 条件値。ケース入数 = attrsのケースロット、無ければ発注ロット単位
 *   ケース入数かつ金額  金額下限 + ケースグループごとに Σ数量がケースロットの倍数 (ジョンズブレンド:
 *                      同グループ内は香り違いを混載できるが、ケース入数ちょうどで発注する)
 *   ロット倍率 n倍     各商品の発注数が (発注ロット単位×n) の倍数 (パシーマ: 2ロット単位で発注)
 *   ロット倍率以上 n倍  各商品の発注数が (発注ロット単位×n) 以上 (ノアテック: 最低2ロットから)
 * items: [{key, qty, cost, lot?, caseLot?, caseGroup?}] — 発注内容 (lot=発注ロット単位)。
 * lot/ケース情報が無い商品は判定不能として auto.unknown / auto.noSize に件数・キーを載せ、met は下げない
 * (データ不備で発注を止めない。バッジで人間に知らせる)。
 */
export function evaluateCondition(cond, memberKeys, items) {
  const inGroup = items.filter(i => memberKeys.has(i.key));
  const sumQty = inGroup.reduce((s, i) => s + i.qty, 0);
  const sumAmount = Math.round(inGroup.reduce((s, i) => s + i.qty * (i.cost || 0), 0));
  const active = inGroup.filter(i => i.qty > 0);
  let auto = null; // null=自動判定不可 (未知の条件タイプ → 条件文を表示して人間が判断)
  if (cond.condition_type === '数量' && (cond.unit === '個' || !cond.unit)) {
    auto = { current: sumQty, required: cond.condition_value, met: sumQty >= cond.condition_value, kind: 'qty' };
  } else if (cond.condition_type === '数量' && cond.unit === 'ケース') {
    let cases = 0; const noSize = [];
    for (const i of active) {
      const size = i.caseLot || i.lot || 0;
      if (size > 0) cases += i.qty / size;
      else noSize.push(i.key);
    }
    cases = Math.round(cases * 100) / 100;
    auto = { current: cases, required: cond.condition_value, met: cases >= cond.condition_value, kind: 'cases', noSize };
  } else if (cond.condition_type === '金額') {
    auto = { current: sumAmount, required: cond.condition_value, met: sumAmount >= cond.condition_value, kind: 'amount' };
  } else if (cond.condition_type === '上限') {
    auto = { current: sumQty, required: cond.condition_value, met: sumQty <= cond.condition_value, kind: 'cap' };
  } else if (cond.condition_type === 'ケース入数かつ金額') {
    // ケースグループごとに合算 (グループ無しの商品は単品でケースロット判定)
    const byGroup = new Map(); const misaligned = []; const noInfo = [];
    for (const i of active) {
      if (i.caseGroup) {
        let g = byGroup.get(i.caseGroup);
        if (!g) { g = { qty: 0, lot: 0 }; byGroup.set(i.caseGroup, g); }
        g.qty += i.qty;
        if (i.caseLot > 0) g.lot = Math.max(g.lot, i.caseLot);
      } else if (i.caseLot > 0) {
        if (i.qty % i.caseLot !== 0) misaligned.push({ group: i.key, qty: i.qty, lot: i.caseLot });
      } else noInfo.push(i.key);
    }
    for (const [name, g] of byGroup) {
      if (g.lot > 0) { if (g.qty % g.lot !== 0) misaligned.push({ group: name, qty: g.qty, lot: g.lot }); }
      else noInfo.push(name);
    }
    const amountMet = sumAmount >= cond.condition_value;
    auto = { current: sumAmount, required: cond.condition_value, met: amountMet && misaligned.length === 0, kind: 'caseAmount', amountMet, misaligned, noInfo };
  } else if (cond.condition_type === 'ロット倍率') {
    const off = []; const unknown = [];
    for (const i of active) {
      const step = (i.lot || 0) * cond.condition_value;
      if (!(step > 0)) { unknown.push(i.key); continue; }
      if (i.qty % step !== 0) off.push({ key: i.key, qty: i.qty, step });
    }
    auto = { current: sumQty, required: cond.condition_value, met: off.length === 0, kind: 'lotMultiple', off, unknown };
  } else if (cond.condition_type === 'ロット倍率以上') {
    const off = []; const unknown = [];
    for (const i of active) {
      const min = (i.lot || 0) * cond.condition_value;
      if (!(min > 0)) { unknown.push(i.key); continue; }
      if (i.qty < min) off.push({ key: i.key, qty: i.qty, min });
    }
    auto = { current: sumQty, required: cond.condition_value, met: off.length === 0, kind: 'lotMin', off, unknown };
  }
  return { sumQty, sumAmount, auto };
}
