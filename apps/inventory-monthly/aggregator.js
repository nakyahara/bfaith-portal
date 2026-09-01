/**
 * aggregator.js — 月末棚卸しの金額集計コア
 *
 * 数量 × 税抜原価 で在庫金額を計算する。原価は mirror_products から引く。
 * Amazon SKU は mirror_sku_resolved (master only) で NE商品コードに変換し、
 * セット商品は mirror_set_components で構成品に展開する。
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
  // mirror_sku_resolved は quantity カラム(英語)。エイリアスで 数量 に揃える
  const skuMapRows = db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
  const skuMap = new Map();
  for (const s of skuMapRows) {
    const key = (s.seller_sku || '').toLowerCase();
    if (!skuMap.has(key)) skuMap.set(key, []);
    // 数量検証: NULL→1扱い、0/負数/非整数→1 fallback (棚卸しは 1 個でも数えたい)
    const rawQty = s.数量;
    const validQty = (rawQty == null) ? 1
      : (Number.isInteger(rawQty) && rawQty > 0) ? rawQty : 1;
    skuMap.get(key).push({ ne_code: (s.ne_code || '').toLowerCase(), 数量: validQty });
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

export function aggregateInventory({ fbaRows = [], ownRows = [], usFbaRows = null, usFbaAmount = 0, pendingRows = [], usFbaInbound = 0, manualAdjustment = 0, manualAdjustmentNote = '' }) {
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

  // 5) 米国FBA在庫輸送中（手動金額・税抜）。実額なのでマイナス・非有限値(NaN/Infinity)は弾く。
  //    ③が米国CSV(usFbaRows)経由のときは輸送中が既に fba_us に含まれているため、
  //    手入力⑤を加えると二重計上になる。UIでも0を促しているが、ここでも強制0にして事故を防ぐ。
  const fbaUsInboundRaw = Number(usFbaInbound);
  let fbaUsInboundTotal = Number.isFinite(fbaUsInboundRaw) ? Math.max(0, fbaUsInboundRaw) : 0;
  if (Array.isArray(usFbaRows)) fbaUsInboundTotal = 0;
  // 6) 手動調整在庫金額（符号付き＝マイナス可）。非有限値は 0 に丸める。
  const manualAdjustmentRaw = Number(manualAdjustment);
  const manualAdjustmentTotal = Number.isFinite(manualAdjustmentRaw) ? manualAdjustmentRaw : 0;

  const totals = {
    fba_warehouse: Math.round(fbaWarehouseTotal),
    fba_inbound: Math.round(fbaInboundTotal),
    own_warehouse: Math.round(ownWarehouseTotal),
    fba_us: Math.round(fbaUsTotal),
    fba_us_inbound: Math.round(fbaUsInboundTotal),
    pending: Math.round(pendingTotal),
    manual_adjustment: Math.round(manualAdjustmentTotal),
    manual_adjustment_note: manualAdjustmentNote || '',
    total: Math.round(fbaWarehouseTotal + fbaInboundTotal + ownWarehouseTotal + fbaUsTotal + fbaUsInboundTotal + pendingTotal + manualAdjustmentTotal),
  };

  // 警告は重複除去
  warnings.unmappedSkus = [...new Set(warnings.unmappedSkus)];
  warnings.unknownProducts = [...new Set(warnings.unknownProducts)];
  warnings.missingCost = [...new Set(warnings.missingCost)];

  return { totals, details, warnings };
}

/**
 * mirror_inv_daily_summary / mirror_inv_daily_detail から月末在庫を再構成する。
 * CSV アップロード経路の代替: 毎朝 sync された mirror データを使うことで CSV 取得不要にする。
 *
 * ⭐ 断面の定義 (2026-08 変更): snapshot_date (例 7/31) の月末在庫は
 *    「翌日朝 (business_date = snapshot_date + 1日、例 8/1 朝7時) の取得断面」から作る。
 *    business_date は毎朝の取得時点の日付なので、snapshot_date 当日の business_date を読むと
 *    月末最終日の出荷・入荷が反映されない (実質前日末)。旧 CSV 手動運用も
 *    「月初にCSVをDL = 前日末の状態」だったので、この定義が旧運用と一致する。
 *    厳密には「翌朝観測値を用いた月末期末推計」(深夜0時〜朝7時の在庫移動は軽微として補正なし)。
 *
 * 入力:
 *   - snapshot_date: 'YYYY-MM-DD' (実在日であること)
 *   - pendingRows: [{ supplier_name, amount, note }] (発注後未着は手動入力のみ)
 * 出力: aggregateInventory() と同じ形 { totals, details, warnings } + source_business_date
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

// mirror 経路で必須のカテゴリ。summary に行自体が無いカテゴリを 0 円として
// 静かに保存しないためのガード (source_status チェックは「存在する行」にしか効かない)
const REQUIRED_MIRROR_CATEGORIES = ['own_warehouse', 'fba_warehouse', 'fba_inbound', 'fba_us_warehouse', 'fba_us_inbound'];

/** 'YYYY-MM-DD' が実在日か検証 (2026-02-31 のような日付を弾く) */
export function isValidIsoDate(iso) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso || '')) return false;
  const [y, m, d] = iso.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

/** 実在日 'YYYY-MM-DD' に n 日足す (UTC 演算なので月跨ぎ・年跨ぎ・うるう年安全) */
export function addDaysIso(iso, n) {
  const [y, m, d] = iso.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d + n));
  return dt.toISOString().slice(0, 10);
}

export function aggregateFromMirror({ snapshot_date, pendingRows = [] }) {
  const db = getDB();

  if (!isValidIsoDate(snapshot_date)) {
    const err = new Error(`棚卸し基準日が不正です: ${snapshot_date}`);
    err.statusCode = 400;
    throw err;
  }
  // 読み取る断面 = 翌日朝の business_date (ヘッダコメント参照)
  const source_business_date = addDaysIso(snapshot_date, 1);

  // 1) summary / detail の読み取り + 整合性検査。
  // 単一の読取トランザクションで囲み、検査と明細取得の間に sync の書き込みが
  // 挟まって別世代のデータを読む余地 (TOCTOU) を塞ぐ (Codex R2 High)。
  // ※ better-sqlite3 は同期実行なので同一プロセス内では元々割り込み不可。
  //   これは将来の複数プロセス化・非同期化への防御。
  const readSnapshot = db.transaction(() => {
    const summaryRows = db.prepare(
      `SELECT category, total_qty, total_value, source_status
       FROM mirror_inv_daily_summary WHERE business_date = ?`
    ).all(source_business_date);
    if (summaryRows.length === 0) {
      const latest = db.prepare(`SELECT MAX(business_date) AS d FROM mirror_inv_daily_summary`).get()?.d || 'なし';
      const err = new Error(
        `${snapshot_date} の月末在庫は翌朝 ${source_business_date} の同期データから作成しますが、` +
        `mirror に business_date=${source_business_date} がありません (mirror 最新=${latest})。` +
        `翌朝の daily-sync 完了後に再実行してください。`
      );
      err.statusCode = 409; // source 未着 = 再試行で解消し得る
      throw err;
    }

    // [Codex R1 #1] source_status チェック: no_source / failed の日は保存・プレビュー共に拒否
    // 「正常な 0 円在庫」として履歴保存される事故を防ぐ
    const fatalRows = summaryRows.filter(r => r.source_status === 'no_source' || r.source_status === 'failed');
    if (fatalRows.length > 0) {
      const cats = fatalRows.map(r => `${r.category}=${r.source_status}`).join(', ');
      const err = new Error(`${source_business_date} 朝の在庫データは不完全です (${cats})。SP-API 取得失敗 or sync 未完了の可能性があるので、cron 完了を待つか別日付を指定してください。`);
      err.statusCode = 409;
      throw err;
    }

    // 必須カテゴリの存在チェック: summary 行自体が欠けている場合は fail-closed
    const presentCats = new Set(summaryRows.map(r => r.category));
    const absentCats = REQUIRED_MIRROR_CATEGORIES.filter(c => !presentCats.has(c));
    if (absentCats.length > 0) {
      const err = new Error(`${source_business_date} 朝の summary に必須カテゴリがありません (${absentCats.join(', ')})。sync 未完了の可能性があります。`);
      err.statusCode = 409;
      throw err;
    }

    // [Codex R1 #2 / R2 #1] summary/detail 世代不一致チェック
    // sync は summary / detail を別 payload で送るため「summary は新、detail は古い」状態が一瞬発生し得る。
    // 3層で突合する:
    //   a) 数量: total_qty vs SUM(qty) の厳密一致 (total_qty=0 カテゴリも検査 — Codex R1 #3)
    //   b) 金額: total_value vs SUM(total_value) の一致 (許容誤差1円 = 浮動小数の加算順差のみ許す)。
    //      数量が偶然一致しても原価・SKU構成だけ変わった世代ズレを検出する (Codex R2 High)
    //   c) detail 内の snapshot_run_id 混在: 同一 business_date に複数世代の行が残っている状態を検出
    // 完全な保証は summary 側にも snapshot_run_id を持たせて照合すること (miniPC + mirror の
    // スキーマ変更が必要なため別PR。summary には現状 run_id 列が無い)。
    const detailAgg = new Map(
      db.prepare(`SELECT category, SUM(qty) AS q, SUM(total_value) AS v FROM mirror_inv_daily_detail WHERE business_date = ? GROUP BY category`)
        .all(source_business_date).map(r => [r.category, { q: Number(r.q), v: Number(r.v || 0) }])
    );
    const mismatchCats = summaryRows
      .filter(r => {
        const agg = detailAgg.get(r.category);
        if ((agg?.q ?? 0) !== Number(r.total_qty || 0)) return true;
        return Math.abs((agg?.v ?? 0) - Number(r.total_value || 0)) > 1;
      })
      .map(r => {
        const agg = detailAgg.get(r.category);
        return `${r.category} (summary: qty=${r.total_qty}/value=${Math.round(r.total_value || 0)}, detail: qty=${agg?.q ?? 'なし'}/value=${Math.round(agg?.v || 0)})`;
      });
    if (mismatchCats.length > 0) {
      const err = new Error(`${source_business_date} 朝の明細 (mirror_inv_daily_detail) が summary と一致しません: ${mismatchCats.join(', ')}。次回 sync 完了まで待ってください。`);
      err.statusCode = 409;
      throw err;
    }
    const runIdCount = db.prepare(
      `SELECT COUNT(DISTINCT snapshot_run_id) AS n FROM mirror_inv_daily_detail WHERE business_date = ? AND snapshot_run_id IS NOT NULL`
    ).get(source_business_date)?.n ?? 0;
    if (runIdCount > 1) {
      const err = new Error(`${source_business_date} 朝の明細に複数世代 (snapshot_run_id ${runIdCount}種) が混在しています。次回 sync 完了まで待ってください。`);
      err.statusCode = 409;
      throw err;
    }

    const detailRows = db.prepare(`
      SELECT category, source_item_code, ne_code,
             COALESCE(product_name, source_product_name, '') AS product_name,
             qty, unit_cost, total_value, cost_status, resolution_method
      FROM mirror_inv_daily_detail
      WHERE business_date = ?
      ORDER BY category, ne_code
    `).all(source_business_date);

    return { summaryRows, detailRows };
  });
  const { summaryRows, detailRows } = readSnapshot();

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

  // 2) 明細 → details (mirror_inv_daily_detail はすでにセット展開済 + 原価適用済。
  //    detailRows は readSnapshot トランザクション内で summary と同時に取得済み)
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

  return { totals, details, warnings, source_business_date };
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
      INSERT INTO inv_snapshot (snapshot_date, fba_warehouse, fba_inbound, own_warehouse, fba_us, fba_us_inbound, pending_orders, manual_adjustment, manual_adjustment_note, total, note, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      snapshot_date,
      result.totals.fba_warehouse,
      result.totals.fba_inbound,
      result.totals.own_warehouse,
      result.totals.fba_us,
      result.totals.fba_us_inbound || 0,
      result.totals.pending,
      result.totals.manual_adjustment || 0,
      result.totals.manual_adjustment_note || null,
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

// ───────── 原価未登録 (0円計上) 明細の可視化と手入力反映 ─────────
//
// 月末保存 (cron / CSV) の時点で原価が解決できなかった明細は 0 円で計上されている。
// これは合計を過小にする「見えない穴」なので、履歴詳細ページで
//   1) 目立つアラート + 対象商品の一覧 (検索せずに分かる)
//   2) その場で原価を手入力 → 明細・カテゴリ列・合計へ反映
// できるようにする。マスタ (miniPC m_products) の修正は別途必要 (翌月も 0 円になる) ので、
// 手入力値は inv_snapshot_cost_fix に残して翌月の同じ商品に「前回入力値」として提示する。

// 「原価が取れていない」状態。COMPLETE / OVERRIDDEN で原価=0 はマスタ上意図した 0 円なので含めない。
export const UNRESOLVED_COST_STATUSES = new Set([...INCOMPLETE_COST_STATUSES, 'UNMAPPED_SKU', 'UNKNOWN']);
// 手入力で原価を入れた明細の状態。再編集は許すが「未解決」には数えない。
export const MANUAL_FIX_STATUS = 'MANUAL_FIX';

export const COST_STATUS_LABEL = {
  UNMAPPED_SKU: 'Amazon SKU → NE商品コード未マップ (SKUマスタ未登録)',
  NOT_IN_MASTER: '商品マスタに無い商品コード',
  MISSING: '原価未登録',
  PARTIAL: '原価が一部未登録',
  PARTIAL_SET: 'セット構成品の一部が原価未登録',
  UNKNOWN: '原価状態不明',
  MANUAL_FIX: '手入力済',
};

/** 明細行 → 商品単位のキー。商品コードがあればそれ、無ければ Amazon SKU。 */
export function costItemKey(row) {
  const code = (row.商品コード || '').trim();
  if (code) return 'code:' + code.toLowerCase();
  const sku = (row.seller_sku || '').trim();
  if (sku) return 'sku:' + sku.toLowerCase();
  return null;
}

/**
 * 保存済み snapshot の明細のうち「原価が取れず 0 円計上」の行と「手入力済」の行を
 * 商品単位 (costItemKey) にまとめて返す。
 *
 * 戻り値:
 *   items: [{ key, 商品コード, seller_sku, 商品名, 原価状態,
 *             原価 (手入力済なら現在値。未解決は null),
 *             current_value (現在計上されている金額。未解決でも PARTIAL_SET 等は 0 でないことがある),
 *             rows: [{ id, category, 数量 }], total_qty, prev_cost, prev_snapshot_date }]
 *          原価状態は未解決系を優先 (同一商品で未解決行と手入力行が混在したら未解決扱い)。
 *          数量 0 以下の行は対象外 (金額に影響しない)。
 *   unresolved_count: 未解決の商品数 (= アラート件数)
 *   unresolved_qty:   未解決の合計数量
 *   fixed_count:      手入力済の商品数
 *   zero_registered:  マスタで原価=0 として登録済み (COMPLETE/OVERRIDDEN) の明細行数。参考表示用
 */
export function listUnresolvedCostItems(snapshotId) {
  const db = getDB();
  const snap = db.prepare('SELECT snapshot_date FROM inv_snapshot WHERE id = ?').get(snapshotId);
  const rows = db.prepare(
    `SELECT id, category, seller_sku, 商品コード, 商品名, 数量, 原価, 金額, 原価状態
     FROM inv_snapshot_detail WHERE snapshot_id = ? AND 数量 > 0 ORDER BY category, 商品コード, seller_sku`
  ).all(snapshotId);

  const byKey = new Map();
  let zero_registered = 0;
  for (const r of rows) {
    const status = r.原価状態 || 'UNKNOWN';
    const unresolved = UNRESOLVED_COST_STATUSES.has(status);
    const fixed = status === MANUAL_FIX_STATUS;
    if (!unresolved && !fixed) {
      if (Number(r.原価 || 0) === 0) zero_registered++;
      continue;
    }
    const key = costItemKey(r);
    if (!key) continue;
    if (!byKey.has(key)) {
      byKey.set(key, {
        key,
        商品コード: r.商品コード || null,
        seller_sku: r.seller_sku || null,
        商品名: r.商品名 || '',
        原価状態: status,
        原価: fixed ? Number(r.原価 || 0) : null,
        current_value: 0,
        rows: [],
        total_qty: 0,
        prev_cost: null,
        prev_snapshot_date: null,
      });
    }
    const item = byKey.get(key);
    if (!item.商品名 && r.商品名) item.商品名 = r.商品名;
    if (!item.seller_sku && r.seller_sku) item.seller_sku = r.seller_sku;
    // 未解決を優先: 手入力済と混在していたら未解決として数える
    if (unresolved && item.原価状態 === MANUAL_FIX_STATUS) { item.原価状態 = status; item.原価 = null; }
    item.rows.push({ id: r.id, category: r.category, 数量: Number(r.数量 || 0) });
    item.total_qty += Number(r.数量 || 0);
    item.current_value += Number(r.金額 || 0);
  }

  // 過去の手入力値 (同じ商品がマスタ未登録のまま翌月も出たときの提示用)。
  // この snapshot より前の基準日、または同じ基準日の旧世代 (CSV 再保存で作り直した場合) から引く。
  // 後の月の値を「前回」として出さない (Codex R1 #10)。
  const prevStmt = db.prepare(`
    SELECT 原価, snapshot_date
    FROM inv_snapshot_cost_fix
    WHERE item_key = ? AND snapshot_date <= ? AND snapshot_id != ?
    ORDER BY snapshot_date DESC, id DESC LIMIT 1
  `);
  for (const item of byKey.values()) {
    const prev = snap ? prevStmt.get(item.key, snap.snapshot_date, snapshotId) : null;
    if (prev) { item.prev_cost = Number(prev.原価); item.prev_snapshot_date = prev.snapshot_date; }
  }

  const items = [...byKey.values()].sort((a, b) => {
    // 未解決 → 手入力済 の順、同順位は数量の多い順 (金額影響が大きいものを上に)
    const au = a.原価状態 === MANUAL_FIX_STATUS ? 1 : 0;
    const bu = b.原価状態 === MANUAL_FIX_STATUS ? 1 : 0;
    if (au !== bu) return au - bu;
    return b.total_qty - a.total_qty;
  });
  const unresolvedItems = items.filter(i => i.原価状態 !== MANUAL_FIX_STATUS);
  return {
    items,
    unresolved_count: unresolvedItems.length,
    unresolved_qty: unresolvedItems.reduce((s, i) => s + i.total_qty, 0),
    fixed_count: items.length - unresolvedItems.length,
    zero_registered,
  };
}

/** 明細 category → inv_snapshot の金額列。ここに無い category は合計へ反映しない (現状は全て対応済) */
const CATEGORY_COLUMN = {
  fba_warehouse: 'fba_warehouse',
  fba_inbound: 'fba_inbound',
  own_warehouse: 'own_warehouse',
  fba_us: 'fba_us',
};

/**
 * 手入力した原価を明細へ反映し、カテゴリ列・合計を再計算する。
 *
 * fixes: [{ key: 'code:xxx' | 'sku:xxx', cost: 数値 (0以上), expected_qty?: 画面表示時の合計数量 }]
 * 更新対象 = 同じ key の明細のうち 原価状態 が未解決系 or 手入力済 の行だけ
 * (マスタから原価が取れている行は触らない)。
 *   - expected_qty を渡すと DB 上の対象数量と照合し、違えば statusCode=409 で失敗する
 *     (古い画面から保存して想定外の行に適用するのを防ぐ。Codex R1 #5)
 *   - 対象行が全て手入力済で同じ原価なら no-op (再送で監査記録が増えない。Codex R1 #3)
 *   - 金額列の無い category の行が対象に含まれたら全体を失敗させる (明細だけ変わって合計に乗らない事故防止)
 *
 * カテゴリ列は「保存済みの列 + 差分 (数量×新原価 − 旧金額)」で更新する。
 * 明細 SUM から作り直すと mirror 経路の summary 値 (±1円許容) と食い違い得るため差分方式。
 * 列は整数で保存するので、小数原価を何度も再編集すると丸めが積み上がって ±1円ずれ得る
 * (棚卸し金額の精度としては無視できる範囲。厳密に合わせたければ月末保存をやり直す)。
 * total は pending 編集 endpoint と同じく「カテゴリ列 + 未着 + 手動調整」から再構成する。
 *
 * 戻り値: { updated_items, updated_rows, totals: {…inv_snapshot の金額列…}, unresolved_count }
 */
export function applyCostFixes(snapshotId, fixes, { created_by = null } = {}) {
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const editableStatuses = [...UNRESOLVED_COST_STATUSES, MANUAL_FIX_STATUS];
  const placeholders = editableStatuses.map(() => '?').join(',');

  const txn = db.transaction(() => {
    const snap = db.prepare(
      'SELECT id, snapshot_date, fba_warehouse, fba_inbound, own_warehouse, fba_us, fba_us_inbound, pending_orders, manual_adjustment, note FROM inv_snapshot WHERE id = ?'
    ).get(snapshotId);
    if (!snap) return null;

    const selectRows = db.prepare(
      `SELECT id, category, seller_sku, 商品コード, 商品名, 数量, 原価, 金額, 原価状態
       FROM inv_snapshot_detail
       WHERE snapshot_id = ? AND 数量 > 0 AND (原価状態 IN (${placeholders}) OR 原価状態 IS NULL)`
    );
    const updRow = db.prepare('UPDATE inv_snapshot_detail SET 原価 = ?, 金額 = ?, 原価状態 = ? WHERE id = ?');
    const insFix = db.prepare(
      `INSERT INTO inv_snapshot_cost_fix (snapshot_id, snapshot_date, item_key, 商品コード, seller_sku, 商品名, 原価, rows_updated, delta_value, created_by, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );

    // 対象行を key ごとに束ねる (1回の SELECT で済ませる)
    const editable = selectRows.all(snapshotId, ...editableStatuses);
    const byKey = new Map();
    for (const r of editable) {
      const k = costItemKey(r);
      if (!k) continue;
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(r);
    }

    const deltaByCol = {};
    let updated_items = 0;
    let updated_rows = 0;
    for (const f of fixes) {
      const rows = byKey.get(f.key);
      if (!rows || rows.length === 0) continue; // 既にマスタで解決済み等 → 無視 (エラーにしない)
      const dbQty = rows.reduce((s, r) => s + Number(r.数量 || 0), 0);
      if (f.expected_qty != null && Number(f.expected_qty) !== dbQty) {
        const err = new Error(`${f.key} の対象数量が画面表示時 (${f.expected_qty}) と DB (${dbQty}) で異なります。ページを再読み込みしてから入力し直してください。`);
        err.statusCode = 409;
        throw err;
      }
      const unknownCat = rows.find(r => !CATEGORY_COLUMN[r.category]);
      if (unknownCat) {
        const err = new Error(`${f.key} に金額列へ反映できない区分 (${unknownCat.category}) の明細があります。`);
        err.statusCode = 500;
        throw err;
      }
      // 全行が既に同じ原価で手入力済なら no-op
      if (rows.every(r => r.原価状態 === MANUAL_FIX_STATUS && Number(r.原価) === f.cost)) continue;
      let delta = 0;
      for (const r of rows) {
        const newValue = Number(r.数量 || 0) * f.cost;
        const d = newValue - Number(r.金額 || 0);
        updRow.run(f.cost, newValue, MANUAL_FIX_STATUS, r.id);
        const col = CATEGORY_COLUMN[r.category];
        deltaByCol[col] = (deltaByCol[col] || 0) + d;
        delta += d;
        updated_rows++;
      }
      const head = rows[0];
      insFix.run(snapshotId, snap.snapshot_date, f.key, head.商品コード || null, head.seller_sku || null, head.商品名 || '', f.cost, rows.length, delta, created_by, now);
      updated_items++;
    }
    if (updated_items === 0) return { updated_items: 0, updated_rows: 0, totals: null, snapshot_date: snap.snapshot_date };

    const cols = {
      fba_warehouse: Math.round((snap.fba_warehouse || 0) + (deltaByCol.fba_warehouse || 0)),
      fba_inbound: Math.round((snap.fba_inbound || 0) + (deltaByCol.fba_inbound || 0)),
      own_warehouse: Math.round((snap.own_warehouse || 0) + (deltaByCol.own_warehouse || 0)),
      fba_us: Math.round((snap.fba_us || 0) + (deltaByCol.fba_us || 0)),
    };
    const total = cols.fba_warehouse + cols.fba_inbound + cols.own_warehouse + cols.fba_us
      + (snap.fba_us_inbound || 0) + (snap.pending_orders || 0) + (snap.manual_adjustment || 0);
    // note には「手入力あり」の目印を 1 つだけ残す (再編集のたびに増やさない。詳細は inv_snapshot_cost_fix)
    const fixedItems = db.prepare(
      `SELECT COUNT(DISTINCT item_key) AS n FROM inv_snapshot_cost_fix WHERE snapshot_id = ?`
    ).get(snapshotId).n;
    const noteTag = `[原価手入力 ${now.slice(0, 10)}: ${fixedItems}商品]`;
    const baseNote = (snap.note || '').replace(/\s*\[原価手入力 [^\]]*\]/g, '').trim();
    const note = baseNote ? `${baseNote} ${noteTag}` : noteTag;
    db.prepare(
      'UPDATE inv_snapshot SET fba_warehouse = ?, fba_inbound = ?, own_warehouse = ?, fba_us = ?, total = ?, note = ? WHERE id = ?'
    ).run(cols.fba_warehouse, cols.fba_inbound, cols.own_warehouse, cols.fba_us, total, note, snapshotId);
    return {
      updated_items, updated_rows,
      totals: { ...cols, fba_us_inbound: snap.fba_us_inbound || 0, pending_orders: snap.pending_orders || 0, manual_adjustment: snap.manual_adjustment || 0, total },
      snapshot_date: snap.snapshot_date,
    };
  });

  const result = txn();
  if (!result) return null;
  result.unresolved_count = listUnresolvedCostItems(snapshotId).unresolved_count;
  return result;
}

/**
 * 履歴一覧用: snapshot ごとの「原価未登録 (0円計上) 商品数」を一括で返す。
 * 戻り値: Map<snapshot_id, { unresolved_count, fixed_count }>
 */
export function countUnresolvedCostBySnapshot() {
  const db = getDB();
  const unresolvedList = [...UNRESOLVED_COST_STATUSES];
  const ph = unresolvedList.map(() => '?').join(',');
  // costItemKey() と同じ規則: 商品コード優先、無ければ seller_sku、両方空なら NULL (数えない)
  const keyExpr = `COALESCE(NULLIF('code:' || LOWER(TRIM(COALESCE(商品コード, ''))), 'code:'), NULLIF('sku:' || LOWER(TRIM(COALESCE(seller_sku, ''))), 'sku:'))`;
  const rows = db.prepare(`
    SELECT snapshot_id,
           COUNT(DISTINCT CASE WHEN 原価状態 IN (${ph}) OR 原価状態 IS NULL THEN ${keyExpr} END) AS unresolved_count,
           COUNT(DISTINCT CASE WHEN 原価状態 = ? THEN ${keyExpr} END) AS fixed_count
    FROM inv_snapshot_detail
    WHERE 数量 > 0
    GROUP BY snapshot_id
  `).all(...unresolvedList, MANUAL_FIX_STATUS);
  return new Map(rows.map(r => [r.snapshot_id, { unresolved_count: r.unresolved_count, fixed_count: r.fixed_count }]));
}
