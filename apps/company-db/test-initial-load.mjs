/**
 * test-initial-load.mjs — Company DB 初期ロードの受入試験 (SQLite の fixture → plan → PGlite の Postgres)
 *
 * 本物の Render の SQLite と Postgres は要らない。fixture は本番の CREATE TABLE と同じ列名で作る (列名の取り違えは
 * fail-closed だと全拒否・0 件で静かに死ぬ教訓 → 列名は各アプリの db.js からコピー)。
 * 使い方: node apps/company-db/test-initial-load.mjs
 *
 * 固定する契約 (Codex PR-B R1 の高 1〜10 / 中 11〜15):
 *   1 再観測は「最新と同じ内容」だけ落とす (A→B→A は 3 行残る)      2 複数個パックの JAN・重量は商品に付けない (listing の属性)
 *   3 fba_sku_attrs と Sheet は別の出どころ (違えば conflict)        4 正規化衝突で落とした SKU は以降どこにも使わない (隔離)
 *   5 JAN の取り合いは誰にも付けず conflict (処理順に依存しない)      6 全区分で 予定 = 投入 + 既存同 + skip
 *   7 完全に読めた出品/セットの構成は plan に合わせる (manual は残す)  8 ASIN は出どころの優先順で 1 つ
 *   9 物理属性は内容が同じ再送を入れない・観測時刻は出どころの時刻   10 ロジザードは rank 0 だけ jan
 *   11 楽天の AM 二重は束ねない  12 店舗キーは安定した定数  13 router は 202 + /status  15 ?sync_key は受けない
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { buildPlanFromRender, mapSkuKind, mapHandling, mapTaxRate, mapCost, parseContent, isJan, toIso, singleUnitCode, SHOP_CODES } from './load/sources.mjs';
import { runInitialLoad, reportToMarkdown, ASIN_SOURCE_PRIORITY } from './load/engine.mjs';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
const quiet = () => {};

// ── fixture: Render の DATA_DIR を一時ディレクトリに再現 ──
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-load-'));
{
  const m = new Database(path.join(dataDir, 'warehouse-mirror.db'));
  m.exec(`
    CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT, 在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER, 代表商品コード TEXT, updated_at TEXT NOT NULL);
    CREATE TABLE mirror_set_components (セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, 構成商品名 TEXT, 構成商品原価 REAL, updated_at TEXT NOT NULL, PRIMARY KEY (セット商品コード, 構成商品コード));
    CREATE TABLE mirror_sku_master (seller_sku TEXT NOT NULL PRIMARY KEY, 商品名 TEXT, source_created_at TEXT, source_updated_at TEXT, synced_at TEXT NOT NULL);
    CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, 商品名 TEXT, source_updated_at TEXT, sort_order INTEGER NOT NULL DEFAULT 0, synced_at TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code));
    CREATE TABLE mirror_rakuten_sku_map (rakuten_code TEXT PRIMARY KEY, ne_code TEXT NOT NULL, source TEXT NOT NULL, updated_at TEXT NOT NULL, manage_number TEXT);
    CREATE TABLE mirror_qoo10_items (item_no TEXT PRIMARY KEY, seller_code_raw TEXT, seller_code TEXT, item_name TEXT, brand TEXT, attr_date_jst TEXT, imported_at TEXT, source_run_id TEXT NOT NULL, source_row_hash TEXT NOT NULL, synced_at TEXT NOT NULL);
    CREATE TABLE mirror_amazon_sku_fees (seller_sku TEXT PRIMARY KEY, asin TEXT, fulfillment_channel TEXT, total_fee REAL, fetched_at TEXT NOT NULL);
    CREATE TABLE product_drafts (id INTEGER PRIMARY KEY AUTOINCREMENT, ne_code TEXT NOT NULL UNIQUE, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', official_url TEXT, price INTEGER, jan_code TEXT, asin TEXT, amazon_url TEXT, own_brand INTEGER NOT NULL DEFAULT 0, has_variation INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '');
    CREATE TABLE draft_page_info (draft_id INTEGER PRIMARY KEY, product_type TEXT NOT NULL DEFAULT 'general', brand_name TEXT, content_volume TEXT, size_text TEXT, ingredients TEXT, usage_notes TEXT, origin_type TEXT, origin_country TEXT, category_label TEXT, seller_name TEXT, importer_name TEXT, food_name TEXT, food_ingredients TEXT, food_expiry TEXT, food_storage TEXT, updated_at TEXT NOT NULL DEFAULT '');
    CREATE TABLE draft_sku_jans (draft_id INTEGER NOT NULL, sku_code TEXT NOT NULL, jan_code TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY (draft_id, sku_code));
    CREATE TABLE f_inbound_check_barcode_master (barcode TEXT PRIMARY KEY, code_key TEXT NOT NULL, product_id TEXT NOT NULL, product_name TEXT, barcode_type TEXT NOT NULL, kubun TEXT, rank INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, updated_by TEXT);
    CREATE TABLE f_inbound_info (code_key TEXT PRIMARY KEY, 商品コード TEXT NOT NULL, 商品名 TEXT, 入数 INTEGER, source TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT NOT NULL, order_memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, send_method TEXT, lead_days INTEGER);
    CREATE TABLE po_vendor_code_map (supplier_code TEXT NOT NULL, product_key TEXT NOT NULL, product_code TEXT NOT NULL, vendor_code TEXT NOT NULL, updated_at TEXT NOT NULL, qty_per_unit REAL, PRIMARY KEY (supplier_code, product_key));
    CREATE TABLE supplier_share_master (仕入先コード TEXT PRIMARY KEY, 表示名 TEXT NOT NULL, memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
  `);
  const ins = (sql, rowsArr) => { const st = m.prepare(sql); for (const r of rowsArr) st.run(...r); };
  // 商品: 単品 4 (abc001 / abc002 8% / abc003 廃番 / ＡＢＣ004 全角=abc004 と衝突) + セット 1 + 例外 1 + 区分不明 1
  ins('insert into mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 仕入先コード, 売上分類, 代表商品コード, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?)', [
    ['abc001', 'テスト商品1', '単品', '取扱中', 380.4, 'NE', 'COMPLETE', 0.1, 'STANDARD_10', '0001', 3, 'abc001', 'x'],
    ['abc002', 'テスト商品2 (軽減)', '単品', '取扱中', 250, 'NE', 'COMPLETE', 0.08, 'REDUCED_8', '0001', 3, 'abc001', 'x'],   // 代表 = abc001 (バリエーション子)
    ['abc003', '廃番商品', '単品', '廃番', null, '不明', 'MISSING', 0.1, null, '0002', 1, null, 'x'],
    ['abc004', '衝突する商品', '単品', '取扱中', 100, 'NE', 'COMPLETE', 0.1, 'STANDARD_10', null, 3, null, 'x'],
    ['ＡＢＣ004', '全角の重複', '単品', '取扱中', 100, 'NE', 'COMPLETE', 0.1, 'STANDARD_10', null, 3, null, 'x'],
    ['abc001set3', 'テスト商品1 3個セット', 'セット', '取扱中', 1141.2, 'セット計算', 'COMPLETE', 0.1, 'STANDARD_10', null, 3, null, 'x'],
    ['exc999', '例外原価', '例外', '取扱中', 999, '例外', 'OVERRIDDEN', null, null, null, null, null, 'x'],
    ['weird', '区分が変', '謎', '取扱中', null, null, 'MISSING', null, null, null, null, null, 'x'],
  ]);
  ins('insert into mirror_set_components values (?,?,?,?,?,?)', [['abc001set3', 'abc001', 3, 'テスト商品1', 380, 'x'], ['abc001set3', 'nosuch', 1, null, null, 'x']]);
  ins('insert into mirror_sku_master values (?,?,?,?,?)', [['pr_abc001-3', 'テスト商品1 3個', null, null, 'x'], ['pr_ABC001', 'テスト商品1', null, null, 'x'], ['pr_bundle', 'バンドル', null, null, 'x'], ['pr_abc001-2pk', 'テスト商品1 2個パック', null, null, 'x']]);
  ins('insert into mirror_sku_resolved (seller_sku, ne_code, quantity, source, sort_order, synced_at) values (?,?,?,?,?,?)', [
    ['pr_abc001-3', 'abc001set3', 1, 'master', 0, 'x'], ['pr_ABC001', 'abc001', 1, 'master', 0, 'x'], ['pr_bundle', 'abc001', 1, 'master', 0, 'x'], ['pr_bundle', 'abc002', 2, 'master', 1, 'x'],
    ['pr_abc001-2pk', 'abc001', 2, 'master', 0, 'x'],                                                                                                 // 複数個パック (同じ単品 × 2)
  ]);
  ins('insert into mirror_amazon_sku_fees (seller_sku, asin, fetched_at) values (?,?,?)', [['pr_abc001', 'B000AAA001', 'x'], ['pr_abc001-3', 'B000AAA003', 'x']]);
  ins('insert into mirror_rakuten_sku_map values (?,?,?,?,?)', [
    ['abc001-am', 'abc001', 'am', 'x', 'item-abc001'], ['abc001-al', 'abc001', 'al', 'x', 'item-abc001'], ['abc001', 'abc001', 'w', 'x', 'item-abc001'],   // 同じ SKU の 3 別名
    ['abc002-am1', 'abc002', 'am', 'x', 'item-abc002'], ['abc002-am2', 'abc002', 'am', 'x', 'item-abc002'], ['abc002', 'abc002', 'w', 'x', 'item-abc002'],   // AM が 2 つ = 束ねない
    ['ghost', 'nosuch', 'w', 'x', 'item-ghost'],                                                                                            // NE コードが無い
  ]);
  ins('insert into mirror_qoo10_items (item_no, seller_code, item_name, brand, source_run_id, source_row_hash, synced_at) values (?,?,?,?,?,?,?)', [['700001', 'abc001', 'テスト商品1 Qoo10', 'Qブランド', 'r', 'h', 'x']]);
  ins("insert into product_drafts (ne_code, name, status, jan_code, updated_at) values (?,?,?,?,?)", [['abc001', 'テスト商品1', 'listed', '4900000000011', '2026-09-01 10:00:00'], ['abc002', 'テスト商品2', 'listed', '4900000000028', '2026-09-01 10:00:00'], ['excluded1', '除外', 'excluded', '4900000000099', '']]);
  ins('insert into draft_page_info (draft_id, product_type, brand_name, content_volume, ingredients, usage_notes, seller_name, updated_at) values (?,?,?,?,?,?,?,?)', [[1, 'cosmetics', 'テストブランド', '100ml', '水、グリセリン', '目に入らないように', '株式会社テスト', '2026-09-02 10:00:00']]);
  ins('insert into draft_sku_jans values (?,?,?,?)', [[2, 'abc002', '4900000000028', 'x']]);
  ins('insert into f_inbound_check_barcode_master (barcode, code_key, product_id, barcode_type, rank, updated_at) values (?,?,?,?,?,?)', [
    ['4900000000011', 'abc001', 'abc001', 'jan', 0, 'x'], ['4900000000035', 'abc003', 'abc003', 'jan', 0, 'x'], ['X00FNSKU1', 'abc001', 'abc001', 'fnsku', 0, 'x'],
    ['4900000000099', 'abc002', 'abc002', 'jan', 0, 'x'],   // abc002 の JAN が product_hub (…028) と食い違う
    ['4900000000042', 'abc001', 'abc001', 'jan', 1, 'x'],   // abc001 の副バーコード (rank 1) = 採用しない
  ]);
  ins('insert into f_inbound_info (code_key, 商品コード, 入数, source, created_at, updated_at) values (?,?,?,?,?,?)', [['abc001', 'abc001', 12, 'excel', 'x', 'x']]);
  ins('insert into po_suppliers (supplier_code, name, created_at, updated_at, send_method, lead_days) values (?,?,?,?,?,?)', [['0001', 'AMC', 'x', 'x', 'fax', 10]]);
  ins('insert into po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at, qty_per_unit) values (?,?,?,?,?,?)', [['0001', 'abc001', 'abc001', 'AMC-001', 'x', 12]]);
  ins('insert into supplier_share_master values (?,?,?,?,?)', [['0002', '仕入先B', null, 'x', 'x']]);
  m.close();

  const f = new Database(path.join(dataDir, 'fba.db'));
  f.exec('CREATE TABLE sku_mapping (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_sku TEXT NOT NULL UNIQUE, asin TEXT, product_name TEXT, ne_code TEXT, logizard_code TEXT, fnsku TEXT, jan TEXT, is_set INTEGER DEFAULT 0, set_components TEXT, per_unit_volume REAL, storage_type TEXT, updated_at TEXT); CREATE TABLE fba_sku_attrs (amazon_sku TEXT PRIMARY KEY, asin TEXT, fnsku TEXT, source TEXT, updated_at TEXT);');
  const fi = f.prepare('insert into sku_mapping (amazon_sku, asin, ne_code, fnsku, jan, is_set) values (?,?,?,?,?,?)');
  fi.run('pr_abc001', 'B000AAA001', 'abc001', 'X00FNSKU1', '4900000000011', 0);
  fi.run('pr_abc001-3', 'B000DIFF03', 'abc001set3', 'X00FNSKU3', null, 1);                   // ASIN が fees / attrs と食い違う
  fi.run('pr_sheetonly', 'B000SHEET1', 'abc002', 'X00FNSKU2', '4900000000028', 0);          // Sheet だけにある
  fi.run('pr_abc001-2pk', 'B000AAA002', 'abc001', 'X00FNSKU4', '4900000000022', 0);         // 複数個パックの JAN (単品の JAN ではない)
  const fa = f.prepare('insert into fba_sku_attrs (amazon_sku, asin, fnsku, source, updated_at) values (?,?,?,?,?)');
  fa.run('pr_abc001-3', 'B000AAA003', 'X00FNSKU3', 'sheet_backfill', '2026-09-01 10:00:00');   // attrs は fees と同じ (Sheet が古い)
  fa.run('pr_ABC001', null, 'X00FNSKU1', 'planning', '2026-09-02 10:00:00');
  f.close();

  const r = new Database(path.join(dataDir, 'rakuten-yahoo-sync.db'));
  r.exec("CREATE TABLE yahoo_registered_items (item_code TEXT PRIMARY KEY, yahoo_item_code TEXT NOT NULL, has_sub_code INTEGER NOT NULL DEFAULT 0, last_seen_at TEXT NOT NULL, source TEXT NOT NULL); CREATE TABLE notion_overrides (rakuten_manage_number TEXT PRIMARY KEY, yahoo_title TEXT, yahoo_jan TEXT, source_hash TEXT NOT NULL, notion_page_id TEXT NOT NULL UNIQUE, synced_at TEXT NOT NULL DEFAULT '');");
  r.prepare("insert into yahoo_registered_items values (?,?,?,?,?)").run('abc001', 'abc001', 0, 'x', 'yahoo_existing');
  r.prepare("insert into yahoo_registered_items values (?,?,?,?,?)").run('unknown-y', 'unknown-y', 0, 'x', 'yahoo_existing');
  r.prepare("insert into notion_overrides (rakuten_manage_number, yahoo_jan, source_hash, notion_page_id) values (?,?,?,?)").run('item-abc002', '4900000000028', 'h', 'p1');
  r.close();

  const p = new Database(path.join(dataDir, 'postage.db'));
  p.exec("CREATE TABLE pm_skus (sku_code TEXT PRIMARY KEY, display_name TEXT, unit_weight_g REAL, thickness_mm REAL, default_material_code TEXT, material_source TEXT, weight_source TEXT, note TEXT, updated_at TEXT NOT NULL DEFAULT '', updated_by TEXT);");
  p.prepare("insert into pm_skus (sku_code, unit_weight_g, thickness_mm, weight_source, updated_at) values (?,?,?,?,?)").run('abc001', 320, 25, 'measured', '2026-09-05T00:00:00Z');
  p.close();

  const b = new Database(path.join(dataDir, 'fba-box.db'));
  b.exec("CREATE TABLE fbx_weight_refs (fnsku TEXT PRIMARY KEY, asin TEXT, weight_g REAL, raw_value TEXT, source TEXT NOT NULL DEFAULT 'sp_api_package', status TEXT NOT NULL, error_message TEXT, fetched_at TEXT NOT NULL); CREATE TABLE fbx_weight_current (fnsku TEXT PRIMARY KEY, unit_g REAL NOT NULL, source TEXT NOT NULL, basis_id INTEGER, sample_qty INTEGER, updated_at TEXT NOT NULL);");
  b.prepare("insert into fbx_weight_refs (fnsku, asin, weight_g, status, fetched_at) values (?,?,?,?,?)").run('X00FNSKU1', 'B000AAA001', 300, 'ok', '2026-09-01T00:00:00.000Z');
  b.prepare("insert into fbx_weight_refs (fnsku, asin, weight_g, status, fetched_at) values (?,?,?,?,?)").run('X00FNSKU4', 'B000AAA002', 650, 'ok', '2026-09-01T00:00:00.000Z');   // 2 個パックの重量 = 単品に付けない
  b.close();

  const st = new Database(path.join(dataDir, 'staff.db'));
  st.exec("CREATE TABLE staff (id INTEGER PRIMARY KEY AUTOINCREMENT, staff_no TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, short_name TEXT, kana TEXT, kind TEXT, portal_email TEXT, joined_on TEXT, left_on TEXT, active INTEGER NOT NULL DEFAULT 1, sort INTEGER NOT NULL DEFAULT 0, note TEXT, version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);");
  st.prepare("insert into staff (staff_no, display_name, kind, portal_email, active, created_at, updated_at) values (?,?,?,?,?,?,?)").run('001', '中原 大輔', 'employee', 'd@example.com', 1, 'x', 'x');
  st.prepare("insert into staff (staff_no, display_name, kind, portal_email, active, created_at, updated_at) values (?,?,?,?,?,?,?)").run('i01', 'いろは職員', 'iroha', null, 1, 'x', 'x');
  st.close();
}

console.log('出どころの読み方 (純関数)');
t('商品区分 / 取扱区分 / 税率 / 原価の読み替え', () => {
  assert.equal(mapSkuKind('単品'), 'single'); assert.equal(mapSkuKind('セット'), 'set'); assert.equal(mapSkuKind('例外'), 'exception'); assert.equal(mapSkuKind('謎'), null);
  assert.equal(mapHandling('取扱中'), 'active'); assert.equal(mapHandling('廃番'), 'discontinued'); assert.equal(mapHandling(''), 'unknown');
  assert.equal(mapTaxRate(0.08), 0.08); assert.equal(mapTaxRate(0.1), 0.10); assert.equal(mapTaxRate(0), null); assert.equal(mapTaxRate(null), null);
  assert.deepEqual(mapCost({ 原価状態: 'COMPLETE', 原価: 380.4, 原価ソース: 'NE' }), { jpy: 380.4, source: 'ne', status: 'COMPLETE' });
  assert.equal(mapCost({ 原価状態: 'PARTIAL', 原価: null }), null);
  assert.equal(mapCost({ 原価状態: 'MISSING', 原価: 5 }), null);          // MISSING は原価があっても採らない
  assert.ok(isJan('4900000000011')); assert.ok(!isJan('490000000001'));
});
t('[16] 内容量の読み取り: 日本語の単位 (\\b が効かない) と 英字単位の切れ目', () => {
  assert.deepEqual(parseContent('100ml'), { num: 100, unit: 'ml' }); assert.deepEqual(parseContent('1.5 L'), { num: 1.5, unit: 'L' }); assert.equal(parseContent('たっぷり'), null);
  assert.deepEqual(parseContent('12個入'), { num: 12, unit: '個' });
  assert.deepEqual(parseContent('3本セット'), { num: 3, unit: '本' });
  assert.deepEqual(parseContent('500ml×2'), { num: 500, unit: 'ml' });
  assert.equal(parseContent('100mlx'), null);                                // 'mlx' は単位ではない
  assert.deepEqual(parseContent('1,000g'), { num: 1000, unit: 'g' });
});
t('[9] 出どころの時刻の読み方 / 単品 1 個の判定', () => {
  assert.equal(toIso('2026-09-01 10:00:00'), '2026-09-01T01:00:00.000Z');   // SQLite localtime = JST
  assert.equal(toIso('2026-09-01T00:00:00.000Z'), '2026-09-01T00:00:00.000Z');
  assert.equal(toIso('x'), null); assert.equal(toIso(''), null); assert.equal(toIso(null), null);
  assert.equal(singleUnitCode([{ code: 'a', qty: 1 }]), 'a');
  assert.equal(singleUnitCode([{ code: 'a', qty: 2 }]), null);              // 複数個パック
  assert.equal(singleUnitCode([{ code: 'a', qty: 1 }, { code: 'b', qty: 1 }]), null);
  assert.equal(singleUnitCode([]), null);
  assert.deepEqual(ASIN_SOURCE_PRIORITY, ['listing_report', 'fba_sku_attrs', 'fba_sheet_import', 'amazon_fees']);
});

console.log('\nplan (SQLite → 素の配列)');
const plan = buildPlanFromRender({ dataDir, log: quiet });
t('[!] skus: 区分不明は落として記録、全角の重複はそのまま plan に (engine が正規化衝突で skip する)', () => {
  assert.equal(plan.skus.length, 7);
  assert.ok(plan.sources.skipped.some((x) => x.code === 'weird'));
  const a1 = plan.skus.find((s) => s.code === 'abc001');
  assert.deepEqual(a1.cost, { jpy: 380.4, source: 'ne', status: 'COMPLETE' });
  assert.equal(plan.skus.find((s) => s.code === 'abc002').taxRate, 0.08);
  assert.equal(plan.skus.find((s) => s.code === 'abc003').cost, null);
  assert.equal(plan.skus.find((s) => s.code === 'abc003').handling, 'discontinued');
});
t('[3][8][12] Amazon: 対応表 + Sheet だけの SKU、ASIN/FNSKU の候補は出どころ別 (attrs / Sheet / fees)、店舗キーは定数', () => {
  const amz = plan.listings.filter((l) => l.mall === 'amazon');
  assert.equal(amz.length, 5);                                            // pr_abc001-3, pr_ABC001, pr_bundle, pr_abc001-2pk, pr_sheetonly
  assert.ok(amz.every((l) => l.shopCode === SHOP_CODES.amazon && l.shopCode === 'main@A1VC38T7YXB528'));
  const l3 = amz.find((l) => l.listingCode === 'pr_abc001-3');
  assert.deepEqual(l3.asinCandidates.map((c) => [c.source, c.asin]), [['fba_sku_attrs', 'B000AAA003'], ['fba_sheet_import', 'B000DIFF03'], ['amazon_fees', 'B000AAA003']]);
  assert.deepEqual(l3.fnskuCandidates.map((c) => c.fnsku), ['X00FNSKU3', 'X00FNSKU3']);
  const bundle = amz.find((l) => l.listingCode === 'pr_bundle');
  assert.equal(bundle.components.length, 2);
  assert.ok(!plan.observations.some((o) => o.sourceRef === 'sku_mapping:pr_bundle'));    // 2 SKU の出品に JAN を付けない
  assert.ok(plan.observations.some((o) => o.skuCode === 'abc001' && o.attribute === 'jan' && o.source === 'fba_sheet_import' && o.scope === 'item'));
});
t('[2] 複数個パック (構成 1 行・qty=2) の JAN と重量は商品ではなく listing の属性 (scope listing)', () => {
  const jan = plan.observations.filter((o) => o.sourceRef === 'sku_mapping:pr_abc001-2pk');
  assert.equal(jan.length, 1);
  assert.deepEqual(jan[0].listingRef, { mall: 'amazon', shopCode: SHOP_CODES.amazon, listingCode: 'pr_abc001-2pk' });
  assert.equal(jan[0].scope, 'listing'); assert.equal(jan[0].skuCode, undefined);
  assert.ok(!plan.observations.some((o) => o.skuCode === 'abc001' && o.attribute === 'jan' && o.valueText === '4900000000022'));
  const w = plan.observations.filter((o) => o.sourceRef === 'fbx_weight_refs:X00FNSKU4');
  assert.equal(w.length, 1); assert.equal(w[0].scope, 'listing'); assert.equal(w[0].valueNum, 650);
  assert.ok(!plan.physicals.some((p) => p.weightG === 650));
  assert.equal(plan.physicals.filter((p) => p.skuCode === 'abc001').length, 2);            // pm_skus 実測 320 + fbx catalog 300 (単品 1 個の出品経由)
  assert.equal(plan.physicals.find((p) => p.sourceRef === 'fbx_weight_refs:X00FNSKU1').observedAt, '2026-09-01T00:00:00.000Z');   // 出どころの時刻
  assert.equal(plan.physicals.find((p) => p.sourceRef === 'pm_skus').observedAt, '2026-09-05T00:00:00.000Z');
  assert.equal(plan.observations.find((o) => o.sourceRef === 'draft_page_info:1' && o.attribute === 'brand').observedAt, '2026-09-02T01:00:00.000Z');
});
t('[11][12] 楽天: AM > AL > W の別名を 1 listing にまとめる。AM が 2 つ以上のグループは束ねず行ごとに', () => {
  const rk = plan.listings.filter((l) => l.mall === 'rakuten');
  assert.equal(rk.length, 5);                                             // abc001 (3 別名→1), abc002 (AM 2 + W → 3), ghost
  assert.ok(rk.every((l) => l.shopCode === 'main'));
  const a = rk.find((l) => l.mallItemId === 'item-abc001');
  assert.equal(a.listingCode, 'abc001-am');
  assert.deepEqual(a.externalIds.map((x) => x.kind), ['system_sku_number', 'sku_manage_number', 'item_number']);
  const b = rk.filter((l) => l.mallItemId === 'item-abc002');
  assert.deepEqual(b.map((l) => l.listingCode).sort(), ['abc002', 'abc002-am1', 'abc002-am2']);
  assert.ok(b.every((l) => l.externalIds.length === 1));
  assert.deepEqual(plan.sources.rakuten_alias_ambiguous, [{ manage_number: 'item-abc002', ne_code: 'abc002', am: 2 }]);
});
t('[10] Yahoo / Qoo10 / Notion JAN / ロジザード JAN (rank 0 だけ jan) / product-hub / 入数 / 仕入先 / 人', () => {
  assert.equal(plan.listings.filter((l) => l.mall === 'yahoo').length, 2);
  assert.equal(plan.listings.filter((l) => l.mall === 'qoo10').length, 1);
  assert.ok(plan.observations.some((o) => o.source === 'notion_import' && o.skuCode === 'abc002' && o.valueText === '4900000000028'));
  assert.ok(plan.observations.some((o) => o.source === 'logizard' && o.skuCode === 'abc002' && o.valueText === '4900000000099' && o.attribute === 'jan'));
  assert.ok(plan.observations.some((o) => o.source === 'logizard' && o.skuCode === 'abc001' && o.valueText === '4900000000042' && o.attribute === 'jan_secondary'));
  assert.ok(!plan.observations.some((o) => o.attribute === 'jan' && o.valueText === '4900000000042'));
  assert.ok(plan.observations.some((o) => o.source === 'product_hub' && o.attribute === 'brand' && o.valueText === 'テストブランド'));
  assert.ok(plan.observations.some((o) => o.source === 'qoo10' && o.attribute === 'brand'));
  assert.ok(plan.observations.some((o) => o.attribute === 'net_content' && o.valueNum === 100 && o.unit === 'ml'));
  assert.ok(plan.observations.some((o) => o.source === 'inbound_info' && o.attribute === 'unit_count' && o.valueNum === 12));
  assert.equal(plan.compliance.length, 1);
  assert.deepEqual(plan.suppliers.map((s) => s.code).sort(), ['0001', '0002']);
  assert.ok(plan.supplierSkus.some((x) => x.supplierCode === '0001' && x.skuCode === 'abc001' && x.vendorCode === 'AMC-001' && x.stockUnitsPerOrderUnit === 12));
  assert.equal(plan.workers.length, 2);
  assert.equal(plan.workers.find((w) => w.staffNo === 'i01').companyId, 2);
});

console.log('\nengine (plan → Postgres)');
const pglite = new PGlite();
const db = pgliteAdapter(pglite);
const q = async (sql, params) => (await db.query(sql, params)).rows;
await applyMigrations(db, { log: quiet });
const TABLES = ['core.skus', 'core.products', 'core.sku_components', 'core.listings', 'core.listing_components', 'core.catalog_items', 'core.external_ids', 'core.product_attribute_observations', 'core.attribute_resolutions', 'core.sku_costs', 'core.product_physicals', 'core.product_compliance', 'core.suppliers', 'core.supplier_skus', 'core.workers', 'ops.ingest_runs'];
const counts = async () => Object.fromEntries(await Promise.all(TABLES.map(async (t2) => [t2, (await q(`select count(*)::int as n from ${t2}`))[0].n])));
const jan = async (code) => (await q("select e.external_value from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product' where s.code = $1 and e.id_kind = 'jan' and e.valid_to is null", [code]))[0]?.external_value;
const balanced = (r) => { for (const [k, v] of Object.entries(r.sections)) assert.equal(v.expected, v.applied + v.same + v.skipped.length, `${k} が釣り合わない`); };

let report;
await ta('[!] dry-run は全部やってから巻き戻す (表は空のまま、report は出る)', async () => {
  report = await runInitialLoad(db, plan, { dryRun: true, log: quiet, runId: 'load_test_dry' });
  assert.equal(report.ok, true);
  assert.equal((await q('select count(*)::int as n from core.skus'))[0].n, 0);
  assert.equal((await q('select count(*)::int as n from ops.ingest_runs'))[0].n, 0);
  assert.equal(report.summary.skus.applied, 6);
  assert.equal(report.summary.skus.skipped, 1);                              // ＡＢＣ004 が abc004 と衝突
});

await ta('[6] 本適用: 全区分で 予定 = 投入 + 既存同 + skip (fail-close の物差し) と、各表の件数', async () => {
  report = await runInitialLoad(db, plan, { log: quiet, runId: 'load_test_1' });
  assert.equal(report.ok, true);
  balanced(report);
  const s = report.summary;
  assert.deepEqual(Object.keys(s), ['skus', 'products', 'set_components', 'sku_costs', 'suppliers', 'supplier_skus', 'listings', 'listing_components', 'catalog_items', 'listing_asin_links', 'listing_external_ids', 'ne_codes', 'observations', 'resolutions', 'jan', 'physicals', 'compliance', 'workers']);
  assert.equal(s.skus.applied, 6);
  assert.equal(s.products.applied, 4);                                       // 単品 4 (abc001/abc002/abc003/abc004)
  assert.equal(s.set_components.applied, 1); assert.equal(s.set_components.skipped, 1);   // nosuch
  assert.equal(s.sku_costs.applied, 5);                                      // abc001/abc002/abc004/set/exc (abc003 は MISSING なので無し、全角の重複は skip)
  assert.equal(s.listings.applied, 5 + 5 + 2 + 1);
  assert.equal(s.listing_components.applied, 12); assert.equal(s.listing_components.skipped, 2);   // ghost→nosuch, unknown-y
  assert.equal(s.catalog_items.applied, 4);                                  // 一意 ASIN: B000AAA001 / B000AAA003 / B000SHEET1 / B000AAA002 (pr_bundle は ASIN 無し)
  assert.equal(s.listing_asin_links.applied, 4);                             // pr_ABC001 / pr_abc001-3 / pr_sheetonly / pr_abc001-2pk
  assert.equal(s.ne_codes.applied, 6);
  assert.equal((await q("select count(*)::int as n from core.skus where sku_kind = 'single' and product_id is not null"))[0].n, 4);
  const parent = (await q("select p.parent_product_id is not null as has_parent from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc002'"))[0];
  assert.equal(parent.has_parent, true);                                      // 代表商品コード = abc001
  assert.equal(Number((await q("select cost_jpy from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'abc001' and c.valid_to is null"))[0].cost_jpy), 380);   // 四捨五入
  assert.equal((await q("select count(*)::int as n from ops.ingest_runs where ingest_run_id = 'load_test_1' and status = 'success'"))[0].n, 1);
  assert.deepEqual((await q("select distinct shop_code from core.listings where mall = 'amazon'")).map((r) => r.shop_code), ['main@A1VC38T7YXB528']);
});

await ta('[8][3] ASIN は catalog_items 経由、出どころの優先順 (attrs > Sheet > fees) で 1 つ。食い違いは conflict、両方は付けない', async () => {
  const r = (await q("select ci.asin from core.listings l join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id where l.listing_code = 'pr_abc001-3'"))[0];
  assert.equal(r.asin, 'B000AAA003');
  const c = report.conflicts.find((x) => x.kind === 'asin' && x.listing === 'pr_abc001-3');
  assert.deepEqual(c.values, { fba_sku_attrs: 'B000AAA003', fba_sheet_import: 'B000DIFF03', amazon_fees: 'B000AAA003' });
  assert.equal(c.adopted, 'B000AAA003');
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'asin'"))[0].n, 0);
  const fn = await q("select external_value from core.external_ids where id_kind = 'fnsku' and valid_to is null order by 1");
  assert.deepEqual(fn.map((x) => x.external_value), ['X00FNSKU1', 'X00FNSKU2', 'X00FNSKU3', 'X00FNSKU4']);
  assert.ok(!report.conflicts.some((x) => x.kind === 'fnsku'));            // attrs と Sheet の FNSKU は同じ
});

await ta('[11] 楽天の別名 3 つは 1 listing + 外部 ID 3 つ。AM 二重は listing 3 つ。NE コードの無い出品は listing は残り、構成は未解決として report', async () => {
  const ids = await q("select id_kind, external_value from core.external_ids e join core.listings l on l.listing_id = e.entity_id and e.entity_type = 'listing' where l.mall = 'rakuten' and l.mall_item_id = 'item-abc001' order by 1");
  assert.deepEqual(ids.map((x) => x.id_kind), ['item_number', 'sku_manage_number', 'system_sku_number']);
  assert.equal((await q("select count(*)::int as n from core.listings where mall = 'rakuten' and mall_item_id = 'item-abc002'"))[0].n, 3);
  assert.ok(report.unresolved.listing_components.some((u) => u.listing === 'ghost' && u.code === 'nosuch'));
  assert.ok(report.unresolved.listing_components.some((u) => u.mall === 'yahoo' && u.code === 'unknown-y'));
  assert.equal((await q("select count(*)::int as n from core.listings where mall = 'rakuten' and listing_code = 'ghost'"))[0].n, 1);
});

await ta('[10][2] JAN: 観測は全部残り、規則 v1 (product_hub > ne > logizard > …) で 1 つ採用。不一致は conflict、副バーコードとパックの JAN は採用されない', async () => {
  assert.equal(await jan('abc001'), '4900000000011');
  assert.equal(await jan('abc002'), '4900000000028');                       // product_hub が logizard (…099) に勝つ
  assert.equal(await jan('abc003'), '4900000000035');                       // ロジザードだけでも採用
  assert.ok(report.conflicts.some((c) => c.kind === 'jan' && c.values.logizard === '4900000000099' && c.adopted === '4900000000028'));
  const nObs = (await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan' and entity_type = 'product'"))[0].n;
  assert.ok(nObs >= 8, String(nObs));
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan_secondary'"))[0].n, 1);
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan' and entity_type = 'listing' and packaging_scope = 'listing' and value_text = '4900000000022'"))[0].n, 1);
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and external_value in ('4900000000022', '4900000000042')"))[0].n, 0);
  const res = (await q("select o.source_system from core.attribute_resolutions r join core.product_attribute_observations o on o.observation_id = r.resolved_observation_id join core.skus s on s.product_id = r.entity_id where s.code = 'abc002' and r.attribute = 'jan'"))[0];
  assert.equal(res.source_system, 'product_hub');
});

await ta('[9] ブランド・内容量は products に、入数 (inbound_info) は規則が無いので観測だけ、重量は実測が有効で観測時刻は出どころの時刻', async () => {
  const p = (await q("select p.brand, p.net_content, p.net_content_uom, p.unit_count from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc001'"))[0];
  assert.equal(p.brand, 'テストブランド');                                     // product_hub > qoo10
  assert.equal(Number(p.net_content), 100); assert.equal(p.net_content_uom, 'ml');
  assert.equal(p.unit_count, null);
  assert.ok(report.conflicts.some((c) => c.kind === 'brand' && c.values.qoo10 === 'Qブランド'));
  const w = (await q("select weight_g, source_system, is_measured, observed_at::text as at from core.product_physicals ph join core.skus s on s.product_id = ph.product_id where s.code = 'abc001' and ph.scope = 'package' and ph.is_effective"))[0];
  assert.deepEqual([w.weight_g, w.source_system, w.is_measured], [320, 'measured', true]);
  assert.match(w.at, /^2026-09-05/);
  assert.equal((await q("select count(*)::int as n from core.product_physicals ph join core.skus s on s.product_id = ph.product_id where s.code = 'abc001'"))[0].n, 2);
  const c = (await q("select ingredients, distributor from core.product_compliance pc join core.skus s on s.product_id = pc.product_id where s.code = 'abc001'"))[0];
  assert.equal(c.distributor, '株式会社テスト');
  const v = (await q("select jan, asin, brand, package_weight_g, dq_flags, listing_count from mart.v_product_360 where sku_code = 'abc001'"))[0];
  assert.equal(v.jan, '4900000000011'); assert.equal(v.package_weight_g, 320);
  // 単品 1 個の出品 (pr_ABC001) の ASIN は B000AAA001。2 個パック (pr_abc001-2pk) は B000AAA002 で、商品の ASIN ではない
  const singleAsin = await q("select ci.asin from core.listings l join core.listing_components lc on lc.listing_id = l.listing_id join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id join core.skus s on s.sku_id = lc.sku_id where s.code = 'abc001' and lc.qty = 1 and l.mall = 'amazon' and (select count(*) from core.listing_components x where x.listing_id = l.listing_id) = 1");
  assert.deepEqual(singleAsin.map((r) => r.asin), ['B000AAA001']);
  // 🚨 v_product_360.asin は今 max(ci.asin) で全出品から拾う (0007) ので、複数個パックの ASIN が勝ち得る → 0009 で単品出品に限定する (PR-C の前提)。ここでは値を固定しない
  assert.ok(['B000AAA001', 'B000AAA002'].includes(v.asin), v.asin);
  assert.ok(v.dq_flags.includes('unit_count') && v.dq_flags.includes('genre'));
  assert.ok(Number(v.listing_count) >= 4);                                   // amazon 3 + rakuten 1 + yahoo 1 + qoo10 1 (pr_bundle / 2pk 込み)
});

await ta('仕入先・人', async () => {
  assert.equal((await q('select count(*)::int as n from core.suppliers'))[0].n, 2);
  const ss = (await q("select vendor_code, stock_units_per_order_unit from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where s.code = 'abc001'"))[0];
  assert.equal(ss.vendor_code, 'AMC-001'); assert.equal(ss.stock_units_per_order_unit, 12);
  assert.equal((await q("select count(*)::int as n from core.workers where company_id = 2"))[0].n, 1);
});

await ta('[1][9] もう一度流しても増えない (冪等。観測・物理属性も「最新と同じ内容」は入れない)。原価が変わったら有効期間を付け替える', async () => {
  const before = await counts();
  const r2 = await runInitialLoad(db, plan, { log: quiet, runId: 'load_test_2' });
  assert.equal(r2.ok, true); balanced(r2);
  const after = await counts();
  for (const [t2, n2] of Object.entries(before)) assert.equal(after[t2], t2 === 'ops.ingest_runs' ? n2 + 1 : n2, t2);
  assert.equal(r2.summary.observations.applied, 0); assert.ok(r2.summary.observations.same > 10);
  assert.equal(r2.summary.physicals.applied, 0); assert.equal(r2.summary.physicals.same, 2);
  assert.equal(r2.summary.jan.applied, 0); assert.equal(r2.summary.jan.same, 3);
  // 原価の変更
  const plan2 = structuredClone(plan); plan2.skus.find((s) => s.code === 'abc001').cost.jpy = 400;
  await runInitialLoad(db, plan2, { log: quiet, runId: 'load_test_3' });
  const costs = await q("select cost_jpy, valid_to is null as active from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'abc001' order by sku_cost_id");
  assert.deepEqual(costs.map((c) => [Number(c.cost_jpy), c.active]), [[380, false], [400, true]]);
});

await ta('[1] 再観測 A→B→A は 3 行残る (内容ベースのキーで落とさない)。同じ値の再送は増えない', async () => {
  const cnt = async () => (await q("select count(*)::int as n from core.product_attribute_observations o join core.skus s on s.product_id = o.entity_id and o.entity_type = 'product' where s.code = 'abc001' and o.attribute = 'brand' and o.source_system = 'qoo10'"))[0].n;
  assert.equal(await cnt(), 1);
  const pB = structuredClone(plan); pB.observations.find((o) => o.source === 'qoo10' && o.attribute === 'brand').valueText = 'Qブランド2';
  const rB = await runInitialLoad(db, pB, { log: quiet, runId: 'load_test_obs_b' }); balanced(rB);
  assert.equal(await cnt(), 2);
  const rA = await runInitialLoad(db, plan, { log: quiet, runId: 'load_test_obs_a' }); balanced(rA);
  assert.equal(await cnt(), 3);                                              // A→B→A
  const rA2 = await runInitialLoad(db, plan, { log: quiet, runId: 'load_test_obs_a2' }); balanced(rA2);
  assert.equal(await cnt(), 3);                                              // 同じ値の再送は増えない
  const p = (await q("select brand from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc001'"))[0];
  assert.equal(p.brand, 'テストブランド');                                     // product_hub が上なので変わらない
});

await ta('[5] JAN の取り合い: 2 つの product が同じ JAN を要求したら誰にも付けず conflict。既に持っている方はそのまま、解決結果も書かない', async () => {
  const pC = structuredClone(plan);
  pC.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000011', source: 'product_hub', sourceRef: 'test:contend', observedAt: new Date().toISOString() });
  const rC = await runInitialLoad(db, pC, { log: quiet, runId: 'load_test_contend' }); balanced(rC);
  const c = rC.conflicts.find((x) => x.kind === 'jan_contended');
  assert.ok(c && c.value === '4900000000011' && c.product_ids.length === 2, JSON.stringify(c));
  assert.equal(await jan('abc001'), '4900000000011');
  assert.equal(await jan('abc004'), undefined);
  assert.equal((await q("select count(*)::int as n from core.attribute_resolutions r join core.skus s on s.product_id = r.entity_id where s.code = 'abc004' and r.attribute = 'jan'"))[0].n, 0);
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and external_value = '4900000000011' and valid_to is null"))[0].n, 1);
  // 取り合いが解消 (abc004 の JAN が別の値になった) すれば付く
  const pD = structuredClone(plan);
  pD.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000044', source: 'product_hub', sourceRef: 'test:ok', observedAt: new Date().toISOString() });
  const rD = await runInitialLoad(db, pD, { log: quiet, runId: 'load_test_contend2' }); balanced(rD);
  assert.equal(await jan('abc004'), '4900000000044');
  assert.ok(!rD.conflicts.some((x) => x.kind === 'jan_contended'));
});

await ta('[4] 正規化衝突で落とした SKU (ＡＢＣ004) は観測・構成・出品のどこにも使わない (abc004 に混ざらない)', async () => {
  const pI = structuredClone(plan);
  pI.observations.push({ skuCode: 'ＡＢＣ004', attribute: 'brand', scope: 'item', valueText: '混入ブランド', source: 'product_hub', sourceRef: 'test:iso', observedAt: new Date().toISOString() });
  pI.setComponents.push({ parentCode: 'abc001set3', childCode: 'ＡＢＣ004', qty: 1, source: 'ne' });
  pI.listings.push({ mall: 'yahoo', shopCode: 'main', listingCode: 'iso-y', status: 'active', components: [{ code: 'ＡＢＣ004', qty: 1, resolution: 'exact' }] });
  const rI = await runInitialLoad(db, pI, { log: quiet, runId: 'load_test_iso' }); balanced(rI);
  assert.ok(rI.sections.observations.skipped.some((s) => s.code === 'ＡＢＣ004'));
  assert.ok(rI.sections.set_components.skipped.some((s) => s.child === 'ＡＢＣ004'));
  assert.ok(rI.sections.listing_components.skipped.some((s) => s.code === 'ＡＢＣ004'));
  const p = (await q("select brand from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc004'"))[0];
  assert.equal(p.brand, null);
  assert.equal((await q("select count(*)::int as n from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id where l.listing_code = 'iso-y'"))[0].n, 0);
  // skip があった親 (abc001set3) の既存構成は消されない (完全に読めていないので触らない)
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 1);
});

await ta('[7] 構成の訂正: 完全に読めた出品の plan に無い構成行は消える。人が手で確定した行 (manual) は残して conflict', async () => {
  const lid = Number((await q("select listing_id from core.listings where listing_code = 'pr_bundle'"))[0].listing_id);
  const sid2 = Number((await q("select sku_id from core.skus where code = 'abc002'"))[0].sku_id);
  const cnt = async () => (await q('select count(*)::int as n from core.listing_components where listing_id = $1', [lid]))[0].n;
  assert.equal(await cnt(), 2);
  const pF = structuredClone(plan); pF.listings.find((l) => l.listingCode === 'pr_bundle').components = [{ code: 'abc001', qty: 1, resolution: 'imported' }];
  const rF = await runInitialLoad(db, pF, { log: quiet, runId: 'load_test_fix' }); balanced(rF);
  assert.equal(await cnt(), 1);
  assert.ok(rF.sections.listing_components.notes.some((n) => n === 'stale removed: 1'));
  // 人が手で足した行は plan に無くても残る
  await db.query("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type, resolved_by_id) values (1, $1, $2, 2, 'manual', 'human', 'test')", [lid, sid2]);
  const rG = await runInitialLoad(db, pF, { log: quiet, runId: 'load_test_fix2' }); balanced(rG);
  assert.equal(await cnt(), 2);
  assert.ok(rG.conflicts.some((c) => c.kind === 'listing_component_manual_kept' && c.listing_id === lid && c.sku_id === sid2));
  // 元の plan (abc002 qty 2 imported) を流しても manual は上書きされない (= 既存を尊重 = same に数える)
  const rH = await runInitialLoad(db, plan, { log: quiet, runId: 'load_test_fix3' }); balanced(rH);
  assert.equal((await q('select resolution from core.listing_components where listing_id = $1 and sku_id = $2', [lid, sid2]))[0].resolution, 'manual');
  assert.equal(rH.summary.listing_components.same, 1);
  // セット構成も同じ (plan から子を外す → 消える)
  const pS = structuredClone(plan); pS.setComponents = pS.setComponents.filter((c) => c.childCode !== 'nosuch');   // skip が無い = 完全に読めた
  pS.setComponents.push({ parentCode: 'abc001set3', childCode: 'abc002', qty: 1, source: 'ne' });
  const rS = await runInitialLoad(db, pS, { log: quiet, runId: 'load_test_set' }); balanced(rS);
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 2);
  const pS2 = structuredClone(pS); pS2.setComponents = pS2.setComponents.filter((c) => c.childCode !== 'abc002');
  const rS2 = await runInitialLoad(db, pS2, { log: quiet, runId: 'load_test_set2' }); balanced(rS2);
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 1);
});

await ta('[14] SQL のエラー (CHECK 違反) が途中で起きたら全部巻き戻す (どの表も増えない・ingest_runs にも残らない)', async () => {
  const before = await counts();
  const pE = structuredClone(plan);
  pE.listings[pE.listings.length - 1].status = 'bogus';                     // listings.status の CHECK 違反 (skus の後・listing の途中)
  pE.skus.push({ code: 'newsku_rollback', name: '巻き戻る', kind: 'single', handling: 'active' });
  let err;
  try { await runInitialLoad(db, pE, { log: quiet, runId: 'load_test_err' }); } catch (e) { err = e; }
  assert.ok(err, '例外が出るはず');
  assert.equal(err.report.ok, false); assert.equal(err.report.run_id, 'load_test_err');
  assert.match(err.report.error, /check|status/i);
  assert.deepEqual(await counts(), before);
  assert.equal((await q("select count(*)::int as n from core.skus where code = 'newsku_rollback'"))[0].n, 0);
  // 接続はそのまま使える (トランザクションが閉じている)
  assert.equal((await q('select 1 as ok'))[0].ok, 1);
});

await ta('[6] 予定 ≠ 投入 なら巻き戻す (fail-close): 重複は skip に数えられて釣り合う。report の md に区分の表が出る', async () => {
  const bad = structuredClone(plan);
  bad.setComponents.push({ parentCode: 'abc001set3', childCode: 'abc001', qty: 3, source: 'ne' });   // 重複 → skip に数えられて釣り合う (OK)
  const r = await runInitialLoad(db, bad, { log: quiet, runId: 'load_test_4' }); balanced(r);
  assert.equal(r.summary.set_components.skipped, 2);
  const md = reportToMarkdown(r);
  assert.match(md, /set_components \| 3 \| /);
  assert.match(md, /jan_contended|不一致/);
});

console.log('\nrouter (202 + /status、ヘッダ認証だけ)');
await ta('[13][15] POST /load は 202 で run_id を返し、/status に実行中→完了が出る。?sync_key= は 401', async () => {
  const express = (await import('express')).default;
  const http = await import('node:http');
  const routerMod = await import('./router.mjs');
  process.env.MIRROR_SYNC_KEY = 'test-key';
  process.env.DATA_DIR = dataDir;
  process.env.COMPANY_DB_URL = 'postgres://nobody:nothing@127.0.0.1:1/none';   // 繋がらない = 失敗が /status と latest.json に残る
  const app = express(); app.use('/apps/company-db/sync', routerMod.default);
  const server = http.createServer(app); await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
  const call = async (p, opt = {}) => { const res = await fetch(base + p, opt); return { status: res.status, body: await res.json() }; };
  try {
    assert.equal((await call('/status')).status, 401);
    assert.equal((await call('/status?sync_key=test-key')).status, 401);                          // クエリでは通さない
    assert.equal((await call('/status?counts=0', { headers: { 'x-sync-key': 'test-key' } })).status, 200);
    const started = await call('/load', { method: 'POST', headers: { 'x-sync-key': 'test-key' } });
    assert.equal(started.status, 202); assert.ok(started.body.run_id); assert.equal(started.body.dry_run, true);
    const st0 = await call('/status?counts=0', { headers: { 'x-sync-key': 'test-key' } });
    assert.ok(st0.body.current === null || st0.body.current.run_id === started.body.run_id);
    for (let i = 0; i < 100 && routerMod.state.current; i++) await new Promise((r) => setTimeout(r, 100));
    assert.equal(routerMod.state.current, null, '終わっているはず');
    const st1 = await call('/status?counts=0', { headers: { 'x-sync-key': 'test-key' } });
    assert.equal(st1.body.last.run_id, started.body.run_id);
    assert.equal(st1.body.last.status, 'failed');                                                  // Postgres に繋がらない
    assert.equal(st1.body.latest.run_id, started.body.run_id);                                     // 初期失敗でも latest.json に残る
    assert.equal(st1.body.latest.ok, false); assert.match(st1.body.latest.error, /connect/);
    assert.ok(fs.existsSync(path.join(dataDir, 'company-db', `load-${started.body.run_id}.md`)));
  } finally { server.close(); delete process.env.MIRROR_SYNC_KEY; delete process.env.COMPANY_DB_URL; delete process.env.DATA_DIR; }
});

await pglite.close();
fs.rmSync(dataDir, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
