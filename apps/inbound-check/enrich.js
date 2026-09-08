/**
 * 入荷受付チェック — いろは行きの明細に載せる参照データ (商品マスタ・仕入先・30日販売数・フリー在庫・作業仕様)
 *
 * 旧 Notion 作業カードの送信 (notion-sync.js) の中にあった純粋な集計処理を、
 * Notion の運用廃止 (2026-09-05) に伴う削除 (2026-09-09) のときにここへ移した。
 * Notion とは無関係で、いろは在庫化アプリ (iroha-work/service.js) が現役で使っている。
 */
function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 1回分の参照データをまとめて引く。
 * ⚠1行ずつ LOWER(TRIM(...)) で照合するとインデックスが効かず全表スキャン×行数になるので、
 *   30日分・対象表ごとに1クエリで Map を作る (mirror_sales_daily は数百万行になり得る)。
 */
export function buildEnrichContext(db) {
  const ctx = { products: new Map(), suppliers: new Map(), sales30: new Map(), freeStock: new Map(), workMaster: new Map() };
  if (tableExists(db, 'mirror_products')) {
    for (const r of db.prepare('SELECT 商品コード AS code, 仕入先コード AS sup, 取扱区分 AS handling FROM mirror_products').all()) {
      const k = String(r.code || '').trim().toLowerCase();
      if (k) ctx.products.set(k, { supplierCode: r.sup, handling: r.handling });
    }
  }
  if (tableExists(db, 'po_suppliers')) {
    // ⚠列名は name (supplier_name ではない — 2026-09-02 本番で no such column。
    //   正=apps/purchase-orders/db.js initPurchaseOrders。テストも本物の init でテーブルを作る)
    for (const r of db.prepare('SELECT supplier_code, name FROM po_suppliers').all()) {
      ctx.suppliers.set(String(r.supplier_code), r.name);
    }
  }
  if (tableExists(db, 'mirror_sales_daily')) {
    // GAS は販売実績シートの「30日販売数合計」を使っていた。ここは自社ミラーの直近30日合計
    const rows = db.prepare(`SELECT LOWER(TRIM(商品コード)) AS k, SUM(数量) AS q
      FROM mirror_sales_daily
      WHERE データ種別 = 'by_product' AND 日付 >= date('now', '-30 day')
      GROUP BY LOWER(TRIM(商品コード))`).all();
    for (const r of rows) if (r.k) ctx.sales30.set(r.k, Number(r.q) || 0);
  }
  if (tableExists(db, 'mirror_logizard_stock')) {
    // フリー在庫 = 在庫数 − 引当数。GAS の zenzaiko.csv は品質を見ていなかったが、
    // 不良品在庫は外部に預けられる数ではないので良品に絞る (意図的な改善)
    const rows = db.prepare(`SELECT LOWER(TRIM(商品ID)) AS k, SUM(在庫数 - 引当数) AS free
      FROM mirror_logizard_stock WHERE 品質区分名 = '良品'
      GROUP BY LOWER(TRIM(商品ID))`).all();
    for (const r of rows) if (r.k) ctx.freeStock.set(r.k, Number(r.free) || 0);
  }
  if (tableExists(db, 'f_iroha_work_master')) {
    // いろは作業仕様 (資材・収納容器・容器あたり数量・工程数・備考)。旧シートの置き換え (PR2)
    for (const r of db.prepare('SELECT * FROM f_iroha_work_master').all()) ctx.workMaster.set(r.code_key, r);
  }
  return ctx;
}

/** 外部出しOK数 (GAS calcExternalAllowance_ と同じ式: フリー在庫 − ceil(30日販売数/30×14日)) */
export function calcExternal(sales30, freeStock) {
  const r = { sales30: sales30 ?? null, freeStock: freeStock ?? null, externalOk: null };
  if (r.sales30 == null && r.freeStock == null) return r;
  const s = r.sales30 ?? 0;
  if (r.freeStock != null) {
    const keep = Math.ceil((s / 30) * 14);
    r.externalOk = Math.max(r.freeStock - keep, 0);
  }
  return r;
}
