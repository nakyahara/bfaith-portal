/**
 * aggregator.js — 月末棚卸しの金額集計コア
 *
 * 数量 × 税抜原価 で在庫金額を計算する。原価は mirror_products から引く。
 * Amazon SKU は mirror_sku_resolved (master優先 + sku_map fallback) で NE商品コードに変換し、
 * セット商品は mirror_set_components で構成品に展開する。
 *
 * env INVENTORY_MONTHLY_USE_LEGACY_SKU_MAP=1 で旧 mirror_sku_map 直参照に戻せる escape hatch あり。
 *
 * 入力:
 *   - fbaRows: [{ seller_sku, fba_warehouse, fba_inbound, product_name, asin }]
 *   - ownRows: [{ 商品コード, 在庫数, 商品名 }]
 *   - usFbaRows: [{ seller_sku, fba_warehouse, fba_inbound, ... }] | null
 *       米国版発注推奨レポートCSVをparseRestockReportした結果。指定時はこちらを優先。
 *   - usFbaAmount: 数値（米国FBA在庫金額・直接入力）。usFbaRowsが無い時のみ使用。
 *   - pendingRows: [{ supplier_name, amount, note }]
 *
 * 出力:
 *   {
 *     totals: { fba_warehouse, fba_inbound, own_warehouse, fba_us, pending, total },
 *     details: [{ category, seller_sku, 商品コード, 商品名, 数量, 原価, 金額, 原価状態 }],
 *     warnings: { unmappedSkus, unknownProducts, missingCost }
 *   }
 */
import { getDB } from './db.js';

function buildLookups(db) {
  // mirror_products: 商品コード(小文字) → { 原価, 原価状態, 商品名 }
  const products = new Map();
  for (const p of db.prepare(
    'SELECT 商品コード, 商品名, 原価, 原価状態 FROM mirror_products'
  ).all()) {
    products.set((p.商品コード || '').toLowerCase(), p);
  }

  // SKU解決マップ: seller_sku(小文字) → [{ ne_code, 数量 }]
  // 既定: mirror_sku_resolved (master優先 + sku_map fallback)
  // env で旧 mirror_sku_map 直参照に戻せる escape hatch あり
  const useLegacy = process.env.INVENTORY_MONTHLY_USE_LEGACY_SKU_MAP === '1';
  const skuMapRows = useLegacy
    ? db.prepare('SELECT seller_sku, ne_code, 数量 FROM mirror_sku_map').all()
    // mirror_sku_resolved は quantity カラム(英語)。エイリアスで 数量 に揃える
    : db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
  const skuMap = new Map();
  for (const s of skuMapRows) {
    const key = (s.seller_sku || '').toLowerCase();
    if (!skuMap.has(key)) skuMap.set(key, []);
    skuMap.get(key).push({ ne_code: (s.ne_code || '').toLowerCase(), 数量: s.数量 || 1 });
  }

  // mirror_set_components: セット商品コード(小文字) → [{ 構成商品コード, 数量, 構成商品原価 }]
  const setComponents = new Map();
  for (const c of db.prepare(
    'SELECT セット商品コード, 構成商品コード, 数量, 構成商品原価 FROM mirror_set_components'
  ).all()) {
    const key = (c.セット商品コード || '').toLowerCase();
    if (!setComponents.has(key)) setComponents.set(key, []);
    setComponents.get(key).push({
      構成商品コード: (c.構成商品コード || '').toLowerCase(),
      数量: c.数量 || 1,
      構成商品原価: c.構成商品原価,
    });
  }

  return { products, skuMap, setComponents };
}

/**
 * 商品コード（NE商品コード）の単価原価を解決する。
 * 解決順序:
 *   1) mirror_products に原価が登録されていればそれを使う
 *   2) mirror_set_components が存在すれば構成品の原価合計から算出
 *      （mirror_products に親 SKU レコードが無くても集計する）
 *   3) どちらも無ければ 0 + ステータスで警告化
 */
function resolveCostByNeCode(neCode, lookups) {
  const code = (neCode || '').toLowerCase();
  if (!code) return { cost: 0, status: 'NO_CODE', name: '' };

  const product = lookups.products.get(code);

  // 1) 単品で原価が登録されていれば優先で使う
  if (product && product.原価 != null && (product.原価状態 === 'COMPLETE' || product.原価状態 === 'OVERRIDDEN')) {
    return { cost: Number(product.原価) || 0, status: product.原価状態, name: product.商品名 || '' };
  }

  // 2) セット商品: mirror_products に親レコードが無くても、構成品があれば集計
  const components = lookups.setComponents.get(code);
  if (components && components.length > 0) {
    let sum = 0;
    let allOk = true;
    for (const comp of components) {
      if (comp.構成商品原価 != null) {
        // set_components の構成商品原価は登録時のスナップショット値。null でなければ採用。
        sum += Number(comp.構成商品原価) * (comp.数量 || 1);
      } else {
        // mirror_products からの解決時は原価が登録されている (COMPLETE/OVERRIDDEN) ものだけ採用。
        // 原価=0 でも 原価状態が MISSING/PARTIAL のままだと「未登録の0」を有効値として扱って
        // しまい、セット全体が静かに 0 円扱いになる事故を起こすので明示的に弾く。
        const inner = lookups.products.get(comp.構成商品コード);
        if (inner && inner.原価 != null
            && (inner.原価状態 === 'COMPLETE' || inner.原価状態 === 'OVERRIDDEN')) {
          sum += Number(inner.原価) * (comp.数量 || 1);
        } else {
          allOk = false;
        }
      }
    }
    return { cost: sum, status: allOk ? 'COMPLETE_SET' : 'PARTIAL_SET', name: (product && product.商品名) || '' };
  }

  // 3) どちらにも該当しない
  if (!product) return { cost: 0, status: 'NOT_IN_MASTER', name: '' };
  return { cost: 0, status: product.原価状態 || 'MISSING', name: product.商品名 || '' };
}

/**
 * Amazon SKU 1件分の在庫金額（数量×単価原価）を計算する。
 * sku_map が複数ヒット（セット販売SKU）の場合は ne_code 毎に展開して合計。
 */
// 「原価が解決できなかった」または「部分的にしか取れなかった」状態。警告の対象。
//   - MISSING / PARTIAL: mirror_products.原価状態 由来
//   - PARTIAL_SET: 構成品の原価が一部欠落しているセット
//   - NOT_IN_MASTER: mirror_products にも mirror_set_components にも無い
// マスタに原価=0 が COMPLETE/OVERRIDDEN として登録されている商品（販促品など、
// 意図的に0円にしているもの）は警告対象にしない。
const INCOMPLETE_COST_STATUSES = new Set(['MISSING', 'PARTIAL', 'PARTIAL_SET', 'NOT_IN_MASTER']);

function valueAmazonRow(seller_sku, qty, lookups, warnings) {
  if (qty <= 0) return { value: 0, lines: [] };
  const skuKey = (seller_sku || '').toLowerCase();
  const mappings = lookups.skuMap.get(skuKey);
  if (!mappings || mappings.length === 0) {
    warnings.unmappedSkus.push(seller_sku);
    return { value: 0, lines: [{ ne_code: null, qty, cost: 0, status: 'UNMAPPED_SKU', name: '' }] };
  }
  let total = 0;
  const lines = [];
  for (const m of mappings) {
    const r = resolveCostByNeCode(m.ne_code, lookups);
    const lineQty = qty * (m.数量 || 1);
    const lineValue = lineQty * r.cost;
    if (r.status === 'NOT_IN_MASTER') warnings.unknownProducts.push(m.ne_code);
    // 原価未登録/部分欠落のステータス時のみ警告。COMPLETE で原価=0 は意図された0円。
    if (INCOMPLETE_COST_STATUSES.has(r.status)) {
      warnings.missingCost.push(`${seller_sku} → ${m.ne_code}${r.status === 'PARTIAL_SET' ? ' (部分原価)' : ''}`);
    }
    total += lineValue;
    lines.push({ ne_code: m.ne_code, qty: lineQty, cost: r.cost, status: r.status, name: r.name });
  }
  return { value: total, lines };
}

export function aggregateInventory({ fbaRows = [], ownRows = [], usFbaRows = null, usFbaAmount = 0, pendingRows = [] }) {
  const db = getDB();
  const lookups = buildLookups(db);

  const warnings = { unmappedSkus: [], unknownProducts: [], missingCost: [] };
  const details = [];
  let fbaWarehouseTotal = 0;
  let fbaInboundTotal = 0;
  let ownWarehouseTotal = 0;

  // 1) FBA倉庫内 / FBA輸送中
  for (const row of fbaRows) {
    if (row.fba_warehouse > 0) {
      const r = valueAmazonRow(row.seller_sku, row.fba_warehouse, lookups, warnings);
      fbaWarehouseTotal += r.value;
      for (const l of r.lines) {
        details.push({
          category: 'fba_warehouse',
          seller_sku: row.seller_sku,
          商品コード: l.ne_code,
          商品名: l.name || row.product_name || '',
          数量: l.qty,
          原価: l.cost,
          金額: l.qty * l.cost,
          原価状態: l.status,
        });
      }
    }
    if (row.fba_inbound > 0) {
      const r = valueAmazonRow(row.seller_sku, row.fba_inbound, lookups, warnings);
      fbaInboundTotal += r.value;
      for (const l of r.lines) {
        details.push({
          category: 'fba_inbound',
          seller_sku: row.seller_sku,
          商品コード: l.ne_code,
          商品名: l.name || row.product_name || '',
          数量: l.qty,
          原価: l.cost,
          金額: l.qty * l.cost,
          原価状態: l.status,
        });
      }
    }
  }

  // 2) 自社倉庫
  for (const row of ownRows) {
    const r = resolveCostByNeCode(row.商品コード, lookups);
    if (r.status === 'NOT_IN_MASTER') warnings.unknownProducts.push(row.商品コード);
    if (INCOMPLETE_COST_STATUSES.has(r.status)) {
      warnings.missingCost.push(`${row.商品コード}${r.status === 'PARTIAL_SET' ? ' (部分原価)' : ''}`);
    }
    const value = row.在庫数 * r.cost;
    ownWarehouseTotal += value;
    details.push({
      category: 'own_warehouse',
      seller_sku: null,
      商品コード: row.商品コード,
      商品名: r.name || row.商品名 || '',
      数量: row.在庫数,
      原価: r.cost,
      金額: value,
      原価状態: r.status,
    });
  }

  // 3) 米国FBA: CSVが渡されたらJP同様にSKU解決→原価マスタで円換算する。
  //    無ければ Phase 1 互換で usFbaAmount を直接金額として採用する。
  //    原価は JP 用の mirror_products.原価 をそのまま流用する（米国独自の原価マスタは未整備）。
  let fbaUsTotal = 0;
  if (Array.isArray(usFbaRows)) {
    for (const row of usFbaRows) {
      // 米国も「倉庫内 + 輸送中」の合計数量を fba_us として一本化集計する
      // （JP は倉庫内/輸送中で分けているが、米国は内訳を持たない既存スキーマに合わせる）
      const totalQty = (row.fba_warehouse || 0) + (row.fba_inbound || 0);
      if (totalQty <= 0) continue;
      const r = valueAmazonRow(row.seller_sku, totalQty, lookups, warnings);
      fbaUsTotal += r.value;
      for (const l of r.lines) {
        details.push({
          category: 'fba_us',
          seller_sku: row.seller_sku,
          商品コード: l.ne_code,
          商品名: l.name || row.product_name || '',
          数量: l.qty,
          原価: l.cost,
          金額: l.qty * l.cost,
          原価状態: l.status,
        });
      }
    }
  } else {
    fbaUsTotal = Number(usFbaAmount) || 0;
  }

  // 4) 発注後未着 はシンプルに金額のみ
  const pendingTotal = pendingRows.reduce((s, p) => s + (Number(p.amount) || 0), 0);

  const totals = {
    fba_warehouse: Math.round(fbaWarehouseTotal),
    fba_inbound: Math.round(fbaInboundTotal),
    own_warehouse: Math.round(ownWarehouseTotal),
    fba_us: Math.round(fbaUsTotal),
    pending: Math.round(pendingTotal),
    total: Math.round(fbaWarehouseTotal + fbaInboundTotal + ownWarehouseTotal + fbaUsTotal + pendingTotal),
  };

  // 警告は重複除去
  warnings.unmappedSkus = [...new Set(warnings.unmappedSkus)];
  warnings.unknownProducts = [...new Set(warnings.unknownProducts)];
  warnings.missingCost = [...new Set(warnings.missingCost)];

  return { totals, details, warnings };
}

/**
 * mirror_inv_daily_summary / mirror_inv_daily_detail から指定日の集計を再構成する。
 * CSV アップロード経路の代替: 毎朝 sync された mirror データを使うことで CSV 取得不要にする。
 *
 * 同じ商品×同じ日付なら CSV 経路と数値は一致する想定 (どちらも同じ原価マスタを使う)。
 *
 * 入力:
 *   - snapshot_date: 'YYYY-MM-DD'
 *   - pendingRows: [{ supplier_name, amount, note }] (発注後未着は手動入力のみ)
 * 出力: aggregateInventory() と同じ形 { totals, details, warnings }
 */
const MIRROR_STATUS_TO_LEGACY = {
  ok: 'COMPLETE',
  cost_missing: 'MISSING',
  ne_missing: 'NOT_IN_MASTER',
};
const MIRROR_CATEGORY_TO_LEGACY = {
  fba_us_warehouse: 'fba_us', // 月末ツール UI は fba_us 単一カテゴリで集計表示
  fba_us_inbound: 'fba_us',
};

export function aggregateFromMirror({ snapshot_date, pendingRows = [] }) {
  const db = getDB();

  // 1) サマリ → totals
  const summaryRows = db.prepare(
    `SELECT category, total_value, source_status
     FROM mirror_inv_daily_summary WHERE business_date = ?`
  ).all(snapshot_date);
  if (summaryRows.length === 0) {
    throw new Error(`${snapshot_date} の在庫スナップショットが mirror に存在しません (sync 待ち or 別日付を指定)`);
  }

  // [Codex R1 #1] source_status チェック: no_source / failed の日は保存・プレビュー共に拒否
  // 「正常な 0 円在庫」として履歴保存される事故を防ぐ
  const fatalRows = summaryRows.filter(r => r.source_status === 'no_source' || r.source_status === 'failed');
  if (fatalRows.length > 0) {
    const cats = fatalRows.map(r => `${r.category}=${r.source_status}`).join(', ');
    throw new Error(`${snapshot_date} の在庫データは不完全です (${cats})。SP-API 取得失敗 or sync 未完了の可能性があるので、cron 完了を待つか別日付を指定してください。`);
  }

  // [Codex R1 #2 / R2 #1] summary/detail 世代不一致チェック
  // sync は summary / detail を別 payload で送るため「summary は新、detail は古い」状態が一瞬発生し得る
  // 検出条件: total_qty > 0 の category は detail にも行が必要
  //   - total_qty=0 (例: fba_us_inbound qty=0) なら detail 行は生成されないので check 不要
  //   - total_qty>0 で source_status='partial'/cost_missing のみでも detail に SKU 行は生成される
  const detailCats = new Set(
    db.prepare(`SELECT DISTINCT category FROM mirror_inv_daily_detail WHERE business_date = ?`)
      .all(snapshot_date).map(r => r.category)
  );
  const missingDetailCats = summaryRows.filter(r => Number(r.total_qty || 0) > 0 && !detailCats.has(r.category));
  if (missingDetailCats.length > 0) {
    throw new Error(`${snapshot_date} の明細 (mirror_inv_daily_detail) が summary と一致しません (${missingDetailCats.map(r => r.category).join(',')} の detail 未着)。次回 sync 完了まで待ってください。`);
  }

  const byCat = Object.fromEntries(summaryRows.map(r => [r.category, r]));
  const v = (cat) => Number(byCat[cat]?.total_value || 0);
  const fbaUsSummary = v('fba_us_warehouse') + v('fba_us_inbound');

  // [Codex R2 #3] CSV経路は「生値合計→最後に1回 round」なので mirror経路もそれに合わせる
  // (カテゴリ毎に round してから足すと小数原価で 1 円ズレる)
  const pendingTotal = pendingRows.reduce((s, p) => s + (Number(p.amount) || 0), 0);
  const rawTotal = v('fba_warehouse') + v('fba_inbound') + v('own_warehouse') + fbaUsSummary + pendingTotal;
  const totals = {
    fba_warehouse: Math.round(v('fba_warehouse')),
    fba_inbound: Math.round(v('fba_inbound')),
    own_warehouse: Math.round(v('own_warehouse')),
    fba_us: Math.round(fbaUsSummary),
    pending: Math.round(pendingTotal),
    total: Math.round(rawTotal),
  };

  // 2) 明細 → details (mirror_inv_daily_detail はすでにセット展開済 + 原価適用済)
  const detailRows = db.prepare(`
    SELECT category, source_item_code, ne_code,
           COALESCE(product_name, source_product_name, '') AS product_name,
           qty, unit_cost, total_value, cost_status, resolution_method
    FROM mirror_inv_daily_detail
    WHERE business_date = ?
    ORDER BY category, ne_code
  `).all(snapshot_date);

  const details = detailRows.map(d => {
    let 原価状態 = MIRROR_STATUS_TO_LEGACY[d.cost_status] || d.cost_status || 'UNKNOWN';
    if (d.resolution_method === 'unresolved') 原価状態 = 'UNMAPPED_SKU';
    return {
      category: MIRROR_CATEGORY_TO_LEGACY[d.category] || d.category,
      seller_sku: (d.category === 'own_warehouse') ? null : d.source_item_code,
      商品コード: d.ne_code,
      商品名: d.product_name,
      数量: d.qty,
      原価: d.unit_cost ?? 0,
      金額: d.total_value ?? 0,
      原価状態,
    };
  });

  // 3) 警告 (CSV 経路と同じキー名で再構築 → UI ダウンロードボタンが流用できる)
  const warnings = {
    unmappedSkus: [...new Set(
      details.filter(d => d.原価状態 === 'UNMAPPED_SKU' && d.seller_sku).map(d => d.seller_sku)
    )],
    unknownProducts: [...new Set(
      details.filter(d => d.原価状態 === 'NOT_IN_MASTER' && d.商品コード).map(d => d.商品コード)
    )],
    missingCost: [...new Set(
      details
        .filter(d => INCOMPLETE_COST_STATUSES.has(d.原価状態))
        .map(d => d.seller_sku ? `${d.seller_sku} → ${d.商品コード}` : d.商品コード)
    )],
    // partial: 集計成功だが unresolved/cost_missing を含む。保存は許可するが UI で警告表示
    partialCategories: summaryRows.filter(r => r.source_status === 'partial').map(r => r.category),
  };

  return { totals, details, warnings };
}

/** 集計結果を inv_snapshot* に保存。同一日が既存なら上書きする。 */
export function saveSnapshot({ snapshot_date, result, pendingRows = [], note = '' }) {
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  const txn = db.transaction(() => {
    // 共有 mirror DB は PRAGMA foreign_keys が ON ではない前提のため、
    // ON DELETE CASCADE に頼らず子テーブル明細を明示的に消す。
    // （上書き対象の snapshot_id を先に拾って子→親の順で削除する）
    const stale = db.prepare('SELECT id FROM inv_snapshot WHERE snapshot_date = ?').all(snapshot_date);
    const delDetail = db.prepare('DELETE FROM inv_snapshot_detail WHERE snapshot_id = ?');
    const delPending = db.prepare('DELETE FROM inv_snapshot_pending WHERE snapshot_id = ?');
    for (const row of stale) {
      delDetail.run(row.id);
      delPending.run(row.id);
    }
    db.prepare('DELETE FROM inv_snapshot WHERE snapshot_date = ?').run(snapshot_date);
    const info = db.prepare(`
      INSERT INTO inv_snapshot (snapshot_date, fba_warehouse, fba_inbound, own_warehouse, fba_us, pending_orders, total, note, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      snapshot_date,
      result.totals.fba_warehouse,
      result.totals.fba_inbound,
      result.totals.own_warehouse,
      result.totals.fba_us,
      result.totals.pending,
      result.totals.total,
      note || null,
      now,
    );
    const snapshotId = info.lastInsertRowid;
    const insDetail = db.prepare(`
      INSERT INTO inv_snapshot_detail (snapshot_id, category, seller_sku, 商品コード, 商品名, 数量, 原価, 金額, 原価状態)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const d of result.details) {
      insDetail.run(snapshotId, d.category, d.seller_sku, d.商品コード, d.商品名, d.数量, d.原価, d.金額, d.原価状態);
    }
    const insPending = db.prepare(`
      INSERT INTO inv_snapshot_pending (snapshot_id, supplier_name, amount, note) VALUES (?, ?, ?, ?)
    `);
    for (const p of pendingRows) {
      if (p.supplier_name) insPending.run(snapshotId, p.supplier_name, Number(p.amount) || 0, p.note || null);
    }
    return snapshotId;
  });

  return txn();
}

export function listSnapshots() {
  const db = getDB();
  return db.prepare('SELECT * FROM inv_snapshot ORDER BY snapshot_date DESC').all();
}

export function getSnapshot(id) {
  const db = getDB();
  const summary = db.prepare('SELECT * FROM inv_snapshot WHERE id = ?').get(id);
  if (!summary) return null;
  const details = db.prepare('SELECT * FROM inv_snapshot_detail WHERE snapshot_id = ?').all(id);
  const pending = db.prepare('SELECT * FROM inv_snapshot_pending WHERE snapshot_id = ?').all(id);
  return { summary, details, pending };
}
