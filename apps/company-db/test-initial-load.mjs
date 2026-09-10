/**
 * test-initial-load.mjs — Company DB 初期ロードの受入試験 (SQLite の fixture → plan → PGlite の Postgres)
 *
 * 本物の Render の SQLite と Postgres は要らない。fixture は本番の CREATE TABLE と同じ列名で作る (列名の取り違えは
 * fail-closed だと全拒否・0 件で静かに死ぬ教訓 → 列名は各アプリの db.js からコピー)。
 * 使い方: node apps/company-db/test-initial-load.mjs
 *
 * 固定する契約 (Codex PR-B R1 高 1〜10 / 中 11〜15、R2 H1〜H7 / M1〜M5):
 *   1 再送は「同じ出どころ・参照・内容・時刻」だけ落とす (時刻の無い入力は最新と同じ内容)。A→B→A は 3 行残る
 *   2 複数個パック・セット×1 の JAN・重量は商品に付けない (listing の属性)   3 fba_sku_attrs と Sheet は別の出どころ、FNSKU の明示解除は既存を閉じる
 *   4 正規化衝突で落とした SKU / 出品 (listingRef 経由も) は以降どこにも使わない   5 外部 ID の移動計画は固定点 (連鎖・循環・取り合い・manual)
 *   6 全区分で 予定 (除外前) = 投入 + 既存同 + skip。合わなければ LOAD_UNBALANCED で全巻き戻し
 *   7 完全に読めた (非空・skip 無し) 出品/セットの構成だけ plan に合わせる (manual は残す・数量が違えば conflict + skip)
 *   8 ASIN は出どころの優先順で 1 つ   9 物理属性は内容 + 時刻で再送判定   10 ロジザードは rank 0 だけ jan
 *   11 楽天の AM 二重は束ねない  12 店舗キーは定数  13 router は 202 + /status + 再起動後の interrupted  15 ?sync_key は受けない
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { buildPlanFromRender, mapSkuKind, mapHandling, mapTaxRate, mapCost, parseContent, isJan, toIso, singleUnitCode, SHOP_CODES } from './load/sources.mjs';
import { runInitialLoad, reportToMarkdown, variationGroupName, ASIN_SOURCE_PRIORITY } from './load/engine.mjs';

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
    // 代表商品コードが実在しない名札 (D-24 = A)。jersey = 色違い 3 つ (2 つ取扱中)、melgroup = 【】が無い名前
    ['jersey-bk', 'ジャージ補修シート 【ブラック(黒)】_白ビ袋', '単品', '取扱中', null, null, 'MISSING', 0.1, null, null, null, 'jersey', 'x'],
    ['jersey-nv', 'ジャージ補修シート 【ネイビー(紺)】_白ビ袋', '単品', '取扱中', null, null, 'MISSING', 0.1, null, null, null, 'jersey', 'x'],
    ['jersey-wh', 'ジャージ 補修シート 【ホワイト(白)】_長3封', '単品', '廃番', null, null, 'MISSING', 0.1, null, null, null, 'jersey', 'x'],
    ['mel01', 'メルカリ訳アリ品01', '単品', '取扱中', null, null, 'MISSING', 0.1, null, null, null, 'melgroup', 'x'],
    ['mel02', 'メルカリ訳アリ品02', '単品', '取扱中', null, null, 'MISSING', 0.1, null, null, null, 'melgroup', 'x'],
    ['setchild', 'セットを代表に指す商品', '単品', '取扱中', null, null, 'MISSING', 0.1, null, null, null, 'abc001set3', 'x'],   // 代表がセット SKU → 名札にしない
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
  ins('insert into draft_sku_jans values (?,?,?,?)', [[2, 'abc002', '4900000000028', '2026-09-01 10:00:00']]);
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
  fi.run('pr_abc001-3', 'B000DIFF03', 'abc001set3', 'X00FNSKU3', '4900000000033', 1);        // ASIN が fees / attrs と食い違う。JAN はセット × 1 の出品のもの (商品には付けない)
  fi.run('pr_sheetonly', 'B000SHEET1', 'abc002', 'X00FNSKU2', '4900000000028', 0);          // Sheet だけにある。FNSKU は attrs (planning) が別の値
  fi.run('pr_abc001-2pk', 'B000AAA002', 'abc001', 'X00FNSKU4', '4900000000022', 0);         // 複数個パックの JAN (単品の JAN ではない)
  const fa = f.prepare('insert into fba_sku_attrs (amazon_sku, asin, fnsku, source, updated_at) values (?,?,?,?,?)');
  fa.run('pr_abc001-3', 'B000AAA003', 'X00FNSKU3', 'sheet_backfill', '2026-09-01 10:00:00');   // attrs は fees と同じ (Sheet が古い)
  fa.run('pr_ABC001', null, 'X00FNSKU1', 'planning', '2026-09-02 10:00:00');
  fa.run('pr_sheetonly', null, 'X00FNSKU9', 'planning', '2026-09-03 10:00:00');              // attrs が Sheet の FNSKU を退ける (X00FNSKU2 は不採用)
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
  const bw = b.prepare("insert into fbx_weight_refs (fnsku, asin, weight_g, status, fetched_at) values (?,?,?,?,?)");
  bw.run('X00FNSKU1', 'B000AAA001', 300, 'ok', '2026-09-01T00:00:00.000Z');
  bw.run('X00FNSKU4', 'B000AAA002', 650, 'ok', '2026-09-01T00:00:00.000Z');   // 2 個パックの重量 = 単品に付けない
  bw.run('X00FNSKU3', 'B000AAA003', 900, 'ok', '2026-09-01T00:00:00.000Z');   // セット × 1 の重量 = 単品に付けない
  bw.run('X00FNSKU2', 'B000SHEET1', 500, 'ok', '2026-09-01T00:00:00.000Z');   // 不採用の FNSKU (attrs が X00FNSKU9 に変えた) の重量 = 付けない
  b.prepare("insert into fbx_weight_current (fnsku, unit_g, source, updated_at) values (?,?,?,?)").run('X00FNSKU1', 310, 'catalog', 'x');   // 時刻が読めない ('x') → 時刻の無い観測として扱う (毎回増えない)
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
t('[9][M3] 出どころの時刻の読み方 / 単品 1 個の判定 (SKU 種別も見る)', () => {
  assert.equal(toIso('2026-09-01 10:00:00'), '2026-09-01T01:00:00.000Z');   // SQLite localtime = JST
  assert.equal(toIso('2026-09-01T00:00:00.000Z'), '2026-09-01T00:00:00.000Z');
  assert.equal(toIso('x'), null); assert.equal(toIso(''), null); assert.equal(toIso(null), null);
  assert.equal(singleUnitCode([{ code: 'a', qty: 1 }]), 'a');
  assert.equal(singleUnitCode([{ code: 'a', qty: 2 }]), null);              // 複数個パック
  assert.equal(singleUnitCode([{ code: 'a', qty: 1 }, { code: 'b', qty: 1 }]), null);
  assert.equal(singleUnitCode([]), null);
  assert.equal(singleUnitCode([{ code: 'set1', qty: 1 }], (c) => c !== 'set1'), null);   // セット SKU × 1 は単品ではない
  assert.equal(singleUnitCode([{ code: 'a', qty: 1 }], (c) => c === 'a'), 'a');
  assert.deepEqual(ASIN_SOURCE_PRIORITY, ['listing_report', 'fba_sku_attrs', 'fba_sheet_import', 'amazon_fees']);
});

console.log('\nplan (SQLite → 素の配列)');
const plan = buildPlanFromRender({ dataDir, log: quiet });
t('[D-24] バリエーションのまとまり: 代表コードごとに 1 グループ、名前は子の商品名から', () => {
  const g = new Map(plan.variationGroups.map((x) => [x.code, x]));
  assert.deepEqual([...g.keys()].sort(), ['abc001', 'abc001set3', 'jersey', 'melgroup']);
  assert.equal(g.get('jersey').name, 'ジャージ補修シート');           // 【 の前が 2 件以上同じ (「ジャージ 補修シート」は 1 件なので負ける)
  assert.deepEqual(g.get('jersey').childCodes, ['jersey-bk', 'jersey-nv', 'jersey-wh']);
  assert.equal(g.get('jersey').status, 'active');                       // 取扱中の子がある
  assert.equal(g.get('melgroup').name, 'メルカリ訳アリ品');             // 【 が無い → 最長共通接頭辞から末尾の数字を削る
  assert.equal(g.get('abc001').childCodes.length, 1);                   // abc002 の代表 = 実在する単品 abc001
  assert.equal(plan.sources.variation_groups, 4); assert.equal(plan.sources.variation_children, 7);
  // 名前の付け方 (純関数)
  assert.equal(variationGroupName(['AB 【黒】', 'AB 【白】'], 'rep'), 'AB');
  assert.equal(variationGroupName(['A 【黒】', 'A 【白】'], 'rep'), 'rep');   // 1 文字の接頭辞は名前にしない
  assert.equal(variationGroupName(['ポケモン ワッペン【カビゴン】_長3封', 'ポケモン ワッペン【ピカチュウ】_長3封'], 'rep'), 'ポケモン ワッペン');
  assert.equal(variationGroupName(['メガネずれ落ち防止ロック 【L】', 'メガネずれ落ち防止ロック 【M】', 'メガネ ずれ落ち防止ロック 【S】'], 'rep'), 'メガネずれ落ち防止ロック');
  assert.equal(variationGroupName(['あ01', 'あ02'], 'rep'), 'rep');     // 共通部分が 2 文字未満 → 代表コード
  assert.equal(variationGroupName([], 'rep'), 'rep');
  assert.equal(variationGroupName(['単独商品 【黒】'], 'rep'), '単独商品');
});
t('[!] skus: 区分不明は落として記録、全角の重複はそのまま plan に (engine が正規化衝突で skip する)', () => {
  assert.equal(plan.skus.length, 13);
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
  const so = amz.find((l) => l.listingCode === 'pr_sheetonly');
  assert.deepEqual(so.fnskuCandidates.map((c) => [c.source, c.fnsku]), [['fba_sku_attrs', 'X00FNSKU9'], ['fba_sheet_import', 'X00FNSKU2']]);
  const bundle = amz.find((l) => l.listingCode === 'pr_bundle');
  assert.equal(bundle.components.length, 2);
  assert.ok(!plan.observations.some((o) => o.sourceRef === 'sku_mapping:pr_bundle'));    // 2 SKU の出品に JAN を付けない
  assert.ok(plan.observations.some((o) => o.skuCode === 'abc001' && o.attribute === 'jan' && o.source === 'fba_sheet_import' && o.scope === 'item' && o.observedAt === null));   // Sheet に時刻は無い
});
t('[2][M3][H7] 複数個パック・セット×1 の JAN と重量は listing の属性。不採用 FNSKU の重量は付けない。観測時刻は出どころの時刻', () => {
  const jan = plan.observations.filter((o) => o.sourceRef === 'sku_mapping:pr_abc001-2pk');
  assert.equal(jan.length, 1);
  assert.deepEqual(jan[0].listingRef, { mall: 'amazon', shopCode: SHOP_CODES.amazon, listingCode: 'pr_abc001-2pk' });
  assert.equal(jan[0].scope, 'listing'); assert.equal(jan[0].skuCode, undefined);
  assert.ok(!plan.observations.some((o) => o.skuCode === 'abc001' && o.attribute === 'jan' && o.valueText === '4900000000022'));
  const setJan = plan.observations.filter((o) => o.sourceRef === 'sku_mapping:pr_abc001-3');
  assert.equal(setJan.length, 1); assert.equal(setJan[0].scope, 'listing'); assert.equal(setJan[0].listingRef.listingCode, 'pr_abc001-3');   // セット SKU × 1
  assert.ok(!plan.observations.some((o) => o.skuCode && o.valueText === '4900000000033'));
  const w = plan.observations.filter((o) => o.sourceRef === 'fbx_weight_refs:X00FNSKU4');
  assert.equal(w.length, 1); assert.equal(w[0].scope, 'listing'); assert.equal(w[0].valueNum, 650);
  const w3 = plan.observations.filter((o) => o.sourceRef === 'fbx_weight_refs:X00FNSKU3');
  assert.equal(w3.length, 1); assert.equal(w3[0].scope, 'listing'); assert.equal(w3[0].valueNum, 900);
  assert.ok(!plan.physicals.some((p) => p.weightG === 650 || p.weightG === 900));
  assert.ok(!plan.physicals.some((p) => p.weightG === 500) && !plan.observations.some((o) => o.valueNum === 500));   // 不採用の X00FNSKU2
  assert.equal(plan.physicals.filter((p) => p.skuCode === 'abc001').length, 3);            // pm_skus 実測 320 + fbx catalog 300 + fbx current 310 (単品 1 個の出品経由)
  assert.equal(plan.physicals.find((p) => p.sourceRef === 'fbx_weight_refs:X00FNSKU1').observedAt, '2026-09-01T00:00:00.000Z');   // 出どころの時刻
  assert.equal(plan.physicals.find((p) => p.sourceRef === 'fbx_weight_current:X00FNSKU1').observedAt, null);                        // 読めない時刻は null
  assert.deepEqual(plan.physicals.find((p) => p.sourceRef === 'fbx_weight_refs:X00FNSKU1').via, { fnsku: 'X00FNSKU1', listing: { mall: 'amazon', shopCode: SHOP_CODES.amazon, listingCode: 'pr_ABC001' } });
  assert.ok(plan.observations.filter((o) => o.attribute === 'package_weight_g' && /fbx_/.test(o.sourceRef)).every((o) => o.via?.fnsku));
  assert.equal(plan.physicals.find((p) => p.sourceRef === 'pm_skus').observedAt, '2026-09-05T00:00:00.000Z');
  assert.ok(plan.listings.filter((l) => l.mall === 'amazon').every((l) => l.fnskuCleared === false));
  assert.equal(plan.observations.find((o) => o.sourceRef === 'draft_page_info:1' && o.attribute === 'brand').observedAt, '2026-09-02T01:00:00.000Z');
  assert.equal(plan.observations.find((o) => o.source === 'logizard' && o.valueText === '4900000000035').observedAt, null);        // 'x' は時刻でない → null
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
const pidOf = async (code) => Number((await q('select product_id from core.skus where code = $1', [code]))[0].product_id);
const balanced = (r) => { for (const [k, v] of Object.entries(r.sections)) assert.equal(v.expected, v.applied + v.same + v.skipped.length, `${k} が釣り合わない`); };
const obsAt = (o) => ({ ...o, observedAt: o.observedAt ?? null });
const run = async (p, runId) => { const r = await runInitialLoad(db, p, { log: quiet, runId }); balanced(r); return r; };

let report;
await ta('[!] dry-run は全部やってから巻き戻す (表は空のまま、report は出る)', async () => {
  report = await runInitialLoad(db, plan, { dryRun: true, log: quiet, runId: 'load_test_dry' });
  assert.equal(report.ok, true);
  assert.equal((await q('select count(*)::int as n from core.skus'))[0].n, 0);
  assert.equal((await q('select count(*)::int as n from ops.ingest_runs'))[0].n, 0);
  assert.equal(report.summary.skus.applied, 12);
  assert.equal(report.summary.skus.skipped, 1);                              // ＡＢＣ004 が abc004 と衝突
});

await ta('[6] 本適用: 全区分で 予定 = 投入 + 既存同 + skip (fail-close の物差し) と、各表の件数', async () => {
  report = await run(plan, 'load_test_1');
  assert.equal(report.ok, true);
  const s = report.summary;
  assert.deepEqual(Object.keys(s), ['skus', 'products', 'variation_groups', 'variation_parents', 'set_components', 'sku_costs', 'suppliers', 'supplier_skus', 'listings', 'listing_components', 'catalog_items', 'listing_asin_links', 'listing_external_ids', 'fnsku_clears', 'ne_codes', 'observations', 'jan', 'resolutions', 'future_revocations', 'physicals', 'compliance', 'workers']);
  assert.equal(s.skus.applied, 12);
  assert.equal(s.products.applied, 10);                                      // 単品 10 (abc001〜004 / jersey 3 / mel 2 / setchild)
  assert.equal(s.set_components.applied, 1); assert.equal(s.set_components.skipped, 1);   // nosuch
  assert.equal(s.sku_costs.applied, 5);                                      // abc001/abc002/abc004/set/exc (abc003 は MISSING なので無し、全角の重複は skip)
  assert.equal(s.listings.applied, 5 + 5 + 2 + 1);
  assert.equal(s.listing_components.applied, 12); assert.equal(s.listing_components.skipped, 2);   // ghost→nosuch, unknown-y
  assert.equal(s.catalog_items.applied, 4);                                  // 一意 ASIN: B000AAA001 / B000AAA003 / B000SHEET1 / B000AAA002 (pr_bundle は ASIN 無し)
  assert.equal(s.listing_asin_links.applied, 4);                             // pr_ABC001 / pr_abc001-3 / pr_sheetonly / pr_abc001-2pk
  assert.equal(s.ne_codes.applied, 12);
  assert.equal(s.jan.expected, 3); assert.equal(s.jan.applied, 3);           // abc001 / abc002 / abc003
  assert.equal(s.resolutions.expected, 5); assert.equal(s.resolutions.applied, 5);   // abc001 jan/brand/net_content, abc002 jan, abc003 jan (unit_count は規則が無い)
  assert.equal((await q("select count(*)::int as n from core.skus where sku_kind = 'single' and product_id is not null"))[0].n, 10);
  const parent = (await q("select p.parent_product_id is not null as has_parent from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc002'"))[0];
  assert.equal(parent.has_parent, true);                                      // 代表商品コード = abc001
  assert.equal(Number((await q("select cost_jpy from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'abc001' and c.valid_to is null"))[0].cost_jpy), 380);   // 四捨五入
  assert.equal((await q("select count(*)::int as n from ops.ingest_runs where ingest_run_id = 'load_test_1' and status = 'success'"))[0].n, 1);
  assert.deepEqual((await q("select distinct shop_code from core.listings where mall = 'amazon'")).map((r) => r.shop_code), ['main@A1VC38T7YXB528']);
});

await ta('[D-24] 名札の product が作られ、色違いが束ねられる。実在する単品を代表に指すならそれを親に。セット SKU を指すなら名札にしない (conflict)', async () => {
  const s = report.summary;
  assert.equal(s.variation_groups.expected, 4);
  assert.equal(s.variation_groups.applied, 2);                                // jersey / melgroup を新規に作る
  assert.equal(s.variation_groups.same, 1);                                   // abc001 は実在する単品 → そのまま親に
  assert.equal(s.variation_groups.skipped, 1);                                // abc001set3 (セット) は名札にしない
  assert.ok(report.conflicts.some((c) => c.kind === 'variation_parent_not_single' && c.representative === 'abc001set3' && c.sku_kind === 'set'));
  assert.equal(s.variation_parents.expected, 7);                              // jersey 3 + mel 2 + abc002 + setchild
  assert.equal(s.variation_parents.applied, 6); assert.equal(s.variation_parents.skipped, 1);   // setchild は親が決まらない
  assert.ok(report.unresolved.variation_parent.some((u) => u.code === 'setchild'));
  // 名札 product は SKU を持たない (買えない)。子はその下に
  const g = (await q("select product_id, name, status from core.products where display_code = 'jersey'"))[0];
  assert.equal(g.name, 'ジャージ補修シート'); assert.equal(g.status, 'active');
  assert.equal((await q('select count(*)::int as n from core.skus where product_id = $1', [g.product_id]))[0].n, 0);
  const kids = await q("select s.code, p.status from core.products p join core.skus s on s.product_id = p.product_id where p.parent_product_id = $1 order by 1", [g.product_id]);
  assert.deepEqual(kids.map((r) => r.code), ['jersey-bk', 'jersey-nv', 'jersey-wh']);
  assert.deepEqual(kids.map((r) => r.status), ['active', 'active', 'discontinued']);   // 子の取扱区分はそのまま
  const mel = (await q("select product_id, name, status from core.products where display_code = 'melgroup'"))[0];
  assert.equal(mel.name, 'メルカリ訳アリ品');
  assert.equal((await q('select count(*)::int as n from core.products where parent_product_id = $1', [mel.product_id]))[0].n, 2);
  // まとまりは mart.v_product_360 に出ない (SKU 起点なので買える商品だけ)
  assert.equal((await q("select count(*)::int as n from mart.v_product_360 where sku_code in ('jersey', 'melgroup')"))[0].n, 0);
  assert.equal((await q("select count(*)::int as n from mart.v_product_360 where sku_code = 'jersey-bk'"))[0].n, 1);
});

await ta('[D-24][R1-1/3] 親子が循環するなら、その辺は全部付けない (2 つの循環・3 つの循環。入力順で変わらない)', async () => {
  const pid1 = await pidOf('abc001'); const pid2 = await pidOf('abc002'); const pid3 = await pidOf('abc003'); const pid4 = await pidOf('abc004');
  const parentOf = async (pid) => (await q('select parent_product_id from core.products where product_id = $1', [pid]))[0].parent_product_id;
  assert.equal(Number(await parentOf(pid2)), pid1); assert.equal(await parentOf(pid1), null);
  // (a) 既存の親が無い 2 つの循環: abc004 の親 = abc003、abc003 の親 = abc004 → どちらも付けない
  const pAB = structuredClone(plan);
  pAB.variationGroups = [
    { code: 'abc003', name: 'X', childCodes: ['abc004'], status: 'active' },
    { code: 'abc004', name: 'Y', childCodes: ['abc003'], status: 'active' },
  ];
  const rAB = await run(pAB, 'load_test_vg_loop2');
  assert.equal(rAB.conflicts.filter((c) => c.kind === 'variation_parent_loop').length, 2);
  assert.equal(await parentOf(pid3), null); assert.equal(await parentOf(pid4), null);
  // 逆順でも同じ (入力順に依存しない)
  const pBA = structuredClone(pAB); pBA.variationGroups.reverse();
  const rBA = await run(pBA, 'load_test_vg_loop2r');
  assert.equal(rBA.conflicts.filter((c) => c.kind === 'variation_parent_loop').length, 2);
  assert.equal(await parentOf(pid3), null); assert.equal(await parentOf(pid4), null);
  // (b) 3 つの循環 abc003→abc002→abc004→abc003 も全部落ちる (50 件で打ち切らない)
  const p3 = structuredClone(plan);
  p3.variationGroups = [
    { code: 'abc002', name: 'A', childCodes: ['abc003'], status: 'active' },
    { code: 'abc004', name: 'B', childCodes: ['abc002'], status: 'active' },
    { code: 'abc003', name: 'C', childCodes: ['abc004'], status: 'active' },
  ];
  const r3 = await run(p3, 'load_test_vg_loop3');
  assert.equal(r3.conflicts.filter((c) => c.kind === 'variation_parent_loop').length, 3);
  assert.equal(await parentOf(pid3), null); assert.equal(await parentOf(pid4), null);
  assert.equal(Number(await parentOf(pid2)), pid1);                           // 既存はそのまま
  // (c) 循環でない鎖は通る: abc003 の親 = abc002 (abc002 の親は abc001) = 3 段
  const pChain = structuredClone(plan);
  pChain.variationGroups = [...plan.variationGroups, { code: 'abc002', name: 'A', childCodes: ['abc003'], status: 'active' }];
  await run(pChain, 'load_test_vg_chain');
  assert.equal(Number(await parentOf(pid3)), pid2);
  await db.query('update core.products set parent_product_id = null where product_id = $1', [pid3]);
});

await ta('[D-24][R1-2/5] 隔離した代表コードは使わない。同じ run に正規化衝突する代表コードが来ても名札を二重に作らない', async () => {
  const pid3 = await pidOf('abc003');
  // (a) 代表コードが 'ＡＢＣ004' (正規化衝突で落とした表記) → 触らない (abc004 の product に混ぜない)
  const pIso = structuredClone(plan);
  pIso.variationGroups = [...plan.variationGroups, { code: 'ＡＢＣ004', name: '混入', childCodes: ['abc003'], status: 'active' }];
  const rIso = await run(pIso, 'load_test_vg_iso');
  assert.ok(rIso.sections.variation_groups.skipped.some((x) => x.code === 'ＡＢＣ004' && /正規化衝突で落とした表記/.test(x.reason)));
  assert.equal((await q('select parent_product_id from core.products where product_id = $1', [pid3]))[0].parent_product_id, null);
  // (b) 同じ run に 'dupgroup' と 'DUPGROUP' → 2 番目は skip、名札 product は 1 つだけ
  const pDup = structuredClone(plan);
  pDup.variationGroups = [
    { code: 'dupgroup', name: 'まとまり1', childCodes: ['abc003'], status: 'active' },
    { code: 'DUPGROUP', name: 'まとまり2', childCodes: ['abc004'], status: 'active' },
  ];
  const rDup = await run(pDup, 'load_test_vg_dup');
  assert.ok(rDup.sections.variation_groups.skipped.some((x) => x.code === 'DUPGROUP' && /正規化衝突/.test(x.reason)));
  assert.equal((await q("select count(*)::int as n from core.products where core.norm_code(display_code) = 'dupgroup'"))[0].n, 1);
  // 2 回目も増えない
  const rDup2 = await run(pDup, 'load_test_vg_dup2');
  assert.equal((await q("select count(*)::int as n from core.products where core.norm_code(display_code) = 'dupgroup'"))[0].n, 1);
  assert.equal(rDup2.summary.variation_groups.applied, 0);
  assert.equal((await q("select count(*)::int as n from core.products p where p.parent_product_id in (select product_id from core.products where core.norm_code(display_code) = 'dupgroup')"))[0].n, 1);   // 子は abc003 だけ (DUPGROUP の子 abc004 は付かない)
  await db.query("update core.products set parent_product_id = null where company_id = 1 and parent_product_id in (select product_id from core.products where core.norm_code(display_code) = 'dupgroup' and company_id = 1)");
  await db.query("delete from core.products where core.norm_code(display_code) = 'dupgroup' and company_id = 1");
});

await ta('[D-24][R1-4] 名札の名前は作ったとき 1 回だけ (人が直しても機械が書き戻さない)。状態は子の取扱区分から毎回決まる', async () => {
  // 人が名前を直す → 次のロードで戻らない
  await db.query("update core.products set name = '人が直したまとまり名' where display_code = 'jersey' and company_id = 1");
  const rH = await run(plan, 'load_test_vg_human');
  assert.equal((await q("select name from core.products where display_code = 'jersey'"))[0].name, '人が直したまとまり名');
  assert.equal(rH.summary.variation_groups.applied, 0);
  // 出どころ側の名前が変わっても、既にある名札の名前は触らない
  const pRen = structuredClone(plan); pRen.variationGroups.find((x) => x.code === 'jersey').name = 'ジャージ補修シート (改)';
  await run(pRen, 'load_test_vg_rename');
  assert.equal((await q("select name from core.products where display_code = 'jersey'"))[0].name, '人が直したまとまり名');
  await db.query("update core.products set name = 'ジャージ補修シート' where display_code = 'jersey' and company_id = 1");
  // 状態は業務データ (子に取扱中があるか) なので毎回決まる。人が draft にしても戻る
  await db.query("update core.products set status = 'draft' where display_code = 'jersey' and company_id = 1");
  const rS = await run(plan, 'load_test_vg_status');
  assert.equal((await q("select status from core.products where display_code = 'jersey'"))[0].status, 'active');
  assert.equal(rS.summary.variation_groups.applied, 1);
  // 子が全部 取扱中でなくなったら まとまりも discontinued
  const pOff = structuredClone(plan);
  for (const c of ['jersey-bk', 'jersey-nv']) pOff.skus.find((x) => x.code === c).handling = 'discontinued';
  await run(pOff, 'load_test_vg_off');
  assert.equal((await q("select status from core.products where display_code = 'jersey'"))[0].status, 'discontinued');
  await run(plan, 'load_test_vg_on');
  assert.equal((await q("select status from core.products where display_code = 'jersey'"))[0].status, 'active');
});

await ta('[8][3] ASIN は catalog_items 経由、出どころの優先順 (attrs > Sheet > fees) で 1 つ。食い違いは conflict、両方は付けない。FNSKU も同じ', async () => {
  const r = (await q("select ci.asin from core.listings l join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id where l.listing_code = 'pr_abc001-3'"))[0];
  assert.equal(r.asin, 'B000AAA003');
  const c = report.conflicts.find((x) => x.kind === 'asin' && x.listing === 'pr_abc001-3');
  assert.deepEqual(c.values, { fba_sku_attrs: 'B000AAA003', fba_sheet_import: 'B000DIFF03', amazon_fees: 'B000AAA003' });
  assert.equal(c.adopted, 'B000AAA003');
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'asin'"))[0].n, 0);
  const fn = await q("select external_value from core.external_ids where id_kind = 'fnsku' and valid_to is null order by 1");
  assert.deepEqual(fn.map((x) => x.external_value), ['X00FNSKU1', 'X00FNSKU3', 'X00FNSKU4', 'X00FNSKU9']);   // pr_sheetonly は attrs の X00FNSKU9 (Sheet の X00FNSKU2 は不採用)
  const fc = report.conflicts.find((x) => x.kind === 'fnsku' && x.listing === 'pr_sheetonly');
  assert.deepEqual(fc.values, { fba_sku_attrs: 'X00FNSKU9', fba_sheet_import: 'X00FNSKU2' }); assert.equal(fc.adopted, 'X00FNSKU9');
});

await ta('[11] 楽天の別名 3 つは 1 listing + 外部 ID 3 つ。AM 二重は listing 3 つ。NE コードの無い出品は listing は残り、構成は未解決として report', async () => {
  const ids = await q("select id_kind, external_value from core.external_ids e join core.listings l on l.listing_id = e.entity_id and e.entity_type = 'listing' where l.mall = 'rakuten' and l.mall_item_id = 'item-abc001' order by 1");
  assert.deepEqual(ids.map((x) => x.id_kind), ['item_number', 'sku_manage_number', 'system_sku_number']);
  assert.equal((await q("select count(*)::int as n from core.listings where mall = 'rakuten' and mall_item_id = 'item-abc002'"))[0].n, 3);
  assert.ok(report.unresolved.listing_components.some((u) => u.listing === 'ghost' && u.code === 'nosuch'));
  assert.ok(report.unresolved.listing_components.some((u) => u.mall === 'yahoo' && u.code === 'unknown-y'));
  assert.equal((await q("select count(*)::int as n from core.listings where mall = 'rakuten' and listing_code = 'ghost'"))[0].n, 1);
});

await ta('[10][2][M2] JAN: 観測は全部残り、規則 v1 (product_hub > ne > logizard > …) で 1 つ採用。不一致は出どころ:参照で conflict。副バーコード・パック・セットの JAN は採用されない', async () => {
  assert.equal(await jan('abc001'), '4900000000011');
  assert.equal(await jan('abc002'), '4900000000028');                       // product_hub が logizard (…099) に勝つ
  assert.equal(await jan('abc003'), '4900000000035');                       // ロジザードだけでも採用
  const c = report.conflicts.find((x) => x.kind === 'jan' && x.adopted === '4900000000028');
  assert.ok(c, 'jan conflict');
  assert.equal(c.values['logizard:barcode_master:rank0'], '4900000000099'); assert.equal(c.adopted_source, 'product_hub');
  const nObs = (await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan' and entity_type = 'product'"))[0].n;
  assert.ok(nObs >= 8, String(nObs));
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan_secondary'"))[0].n, 1);
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where attribute = 'jan' and entity_type = 'listing' and packaging_scope = 'listing' and value_text in ('4900000000022', '4900000000033')"))[0].n, 2);
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and external_value in ('4900000000022', '4900000000033', '4900000000042')"))[0].n, 0);
  const res = (await q("select o.source_system from core.attribute_resolutions r join core.product_attribute_observations o on o.observation_id = r.resolved_observation_id join core.skus s on s.product_id = r.entity_id where s.code = 'abc002' and r.attribute = 'jan'"))[0];
  assert.equal(res.source_system, 'product_hub');
  // 時刻の無い観測はロード時刻で入る (null では入らない)
  assert.equal((await q('select count(*)::int as n from core.product_attribute_observations where observed_at is null'))[0].n, 0);
});

await ta('[9] ブランド・内容量は products に、入数 (inbound_info) は規則が無いので観測だけ、重量は実測が有効で観測時刻は出どころの時刻', async () => {
  const p = (await q("select p.brand, p.net_content, p.net_content_uom, p.unit_count from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc001'"))[0];
  assert.equal(p.brand, 'テストブランド');                                     // product_hub > qoo10
  assert.equal(Number(p.net_content), 100); assert.equal(p.net_content_uom, 'ml');
  assert.equal(p.unit_count, null);
  assert.ok(report.conflicts.some((c) => c.kind === 'brand' && c.values['qoo10:qoo10:700001'] === 'Qブランド' && c.adopted === 'テストブランド'));
  const w = (await q("select weight_g, source_system, is_measured, observed_at::text as at from core.product_physicals ph join core.skus s on s.product_id = ph.product_id where s.code = 'abc001' and ph.scope = 'package' and ph.is_effective"))[0];
  assert.deepEqual([w.weight_g, w.source_system, w.is_measured], [320, 'measured', true]);
  assert.match(w.at, /^2026-09-05/);
  assert.equal((await q("select count(*)::int as n from core.product_physicals ph join core.skus s on s.product_id = ph.product_id where s.code = 'abc001'"))[0].n, 3);
  assert.equal((await q('select count(*)::int as n from core.product_physicals where observed_at is null'))[0].n, 0);   // 時刻の無い行はロード時刻で入る
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

await ta('[1][9] もう一度流しても増えない (冪等。観測・物理属性の再送は入れない)。原価が変わったら有効期間を付け替える', async () => {
  const before = await counts();
  const r2 = await run(plan, 'load_test_2');
  const after = await counts();
  for (const [t2, n2] of Object.entries(before)) assert.equal(after[t2], t2 === 'ops.ingest_runs' ? n2 + 1 : n2, t2);
  assert.equal(r2.summary.observations.applied, 0); assert.ok(r2.summary.observations.same > 10);
  assert.equal(r2.summary.physicals.applied, 0); assert.equal(r2.summary.physicals.same, 3);   // 時刻の無い fbx_weight_current も「最新と同じ内容」で再送
  assert.equal(r2.summary.jan.applied, 0); assert.equal(r2.summary.jan.same, 3);
  assert.equal(r2.summary.listing_external_ids.applied, 0); assert.equal(r2.summary.listing_external_ids.skipped, 0);
  // 原価の変更
  const plan2 = structuredClone(plan); plan2.skus.find((s) => s.code === 'abc001').cost.jpy = 400;
  await run(plan2, 'load_test_3');
  const costs = await q("select cost_jpy, valid_to is null as active from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'abc001' order by sku_cost_id");
  assert.deepEqual(costs.map((c) => [Number(c.cost_jpy), c.active]), [[380, false], [400, true]]);
});

await ta('[1] 時刻の無い観測: A→B→A は 3 行残る。同じ値の再送は増えない', async () => {
  const cnt = async () => (await q("select count(*)::int as n from core.product_attribute_observations o join core.skus s on s.product_id = o.entity_id and o.entity_type = 'product' where s.code = 'abc001' and o.attribute = 'brand' and o.source_system = 'qoo10'"))[0].n;
  assert.equal(await cnt(), 1);
  const pB = structuredClone(plan); pB.observations.find((o) => o.source === 'qoo10' && o.attribute === 'brand').valueText = 'Qブランド2';
  await run(pB, 'load_test_obs_b');
  assert.equal(await cnt(), 2);
  await run(plan, 'load_test_obs_a');
  assert.equal(await cnt(), 3);                                              // A→B→A
  await run(plan, 'load_test_obs_a2');
  assert.equal(await cnt(), 3);                                              // 同じ値の再送は増えない
  const p = (await q("select brand from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc001'"))[0];
  assert.equal(p.brand, 'テストブランド');                                     // product_hub が上なので変わらない
});

await ta('[H5] 時刻のある観測: 同じ内容・同じ時刻は再送、同じ内容でも新しい時刻は新しい観測 (採用順に効く)。古い観測の再送は増えない', async () => {
  const pid = await pidOf('abc002');
  const cnt = async () => (await q("select count(*)::int as n from core.product_attribute_observations where entity_type = 'product' and entity_id = $1 and attribute = 'jan' and source_system = 'product_hub'", [pid]))[0].n;
  const base = await cnt();                                                  // product_drafts:2 + draft_sku_jans:2 (2026-09-01)
  // 同じ product_hub の別の参照 (draft_sku_jans:2) が B (…099) を 9/03 に観測 → 同優先度なので新しい B が採用される
  const pB = structuredClone(plan);
  pB.observations.find((o) => o.sourceRef === 'draft_sku_jans:2').valueText = '4900000000099'; pB.observations.find((o) => o.sourceRef === 'draft_sku_jans:2').observedAt = '2026-09-03T00:00:00.000Z';
  await run(pB, 'load_test_t1');
  assert.equal(await cnt(), base + 1);
  assert.equal(await jan('abc002'), '4900000000099');
  assert.ok((await q("select 1 from core.attribute_resolutions r where r.entity_id = $1 and r.attribute = 'jan'", [pid])).length === 1);
  // 同じ内容 A (…028) を product_drafts:2 が 9/04 に観測し直した → 新しい観測として入り、A が採用に戻る
  const pA = structuredClone(pB);
  pA.observations.find((o) => o.sourceRef === 'product_drafts:2').observedAt = '2026-09-04T00:00:00.000Z';
  await run(pA, 'load_test_t2');
  assert.equal(await cnt(), base + 2);
  assert.equal(await jan('abc002'), '4900000000028');
  // 同じ plan (古い時刻の行 = 完全一致) をもう一度 → 増えない
  const r3 = await run(pA, 'load_test_t3');
  assert.equal(await cnt(), base + 2);
  assert.equal(r3.summary.observations.applied, 0);
  // 元の plan に戻す (…028 at 9/01 は既にある = 再送) → 増えない・採用は最新 (9/04 の A) のまま
  await run(plan, 'load_test_t4');
  assert.equal(await cnt(), base + 2);
  assert.equal(await jan('abc002'), '4900000000028');
});

await ta('[5][H1][M1] JAN の取り合い: 新しい JAN を 2 つの product が要求 → 誰にも付けず jan_contended、予定は除外前の件数。既に持っている値を別の product が要求 → jan_taken', async () => {
  const pid3 = await pidOf('abc003'); const pid4 = await pidOf('abc004');
  const pC = structuredClone(plan);
  pC.observations.push({ skuCode: 'abc003', attribute: 'jan', scope: 'item', valueText: '4900000000077', source: 'product_hub', sourceRef: 'test:c1', observedAt: '2026-09-06T00:00:00.000Z' });
  pC.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000077', source: 'product_hub', sourceRef: 'test:c2', observedAt: '2026-09-06T00:00:00.000Z' });
  const rC = await run(pC, 'load_test_contend');
  const c = rC.conflicts.find((x) => x.kind === 'jan_contended');
  assert.ok(c && c.value === '4900000000077' && c.entities.length === 2, JSON.stringify(c));
  assert.equal(await jan('abc003'), '4900000000035');                        // 元の JAN のまま (取り合いに負けた要求では閉じない)
  assert.equal(await jan('abc004'), undefined);
  assert.equal(rC.summary.jan.expected, 4); assert.equal(rC.summary.jan.skipped, 2); assert.equal(rC.summary.jan.same, 2);   // 予定は除外前
  assert.ok(rC.sections.resolutions.skipped.some((s) => s.product_id === pid3 && s.attribute === 'jan'));
  assert.ok(rC.sections.resolutions.skipped.some((s) => s.product_id === pid4 && s.attribute === 'jan'));
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and external_value = '4900000000077'"))[0].n, 0);
  // 既に abc001 が持つ …011 を abc004 が要求 → taken (abc001 はそのまま)
  const pT = structuredClone(plan);
  pT.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000011', source: 'product_hub', sourceRef: 'test:t', observedAt: '2026-09-06T00:00:00.000Z' });
  const rT = await run(pT, 'load_test_taken');
  assert.ok(rT.conflicts.some((x) => x.kind === 'jan_taken' && x.wanted_by.id === pid4 && x.value === '4900000000011'));
  assert.equal(await jan('abc001'), '4900000000011'); assert.equal(await jan('abc004'), undefined);
  // 取り合いの観測 (…077) は append-only で残っているので、相手 (abc004) が退いた今は abc003 が …077 を取る (…035 は閉じる)
  assert.equal(await jan('abc003'), '4900000000077');
  assert.ok(rT.conflicts.some((x) => x.kind === 'jan_replaced' && x.old === '4900000000035'));
  // 取り合いが解消 (abc004 の JAN が別の値になった) すれば付く
  const pD = structuredClone(plan);
  pD.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000044', source: 'product_hub', sourceRef: 'test:ok', observedAt: '2026-09-07T00:00:00.000Z' });
  const rD = await run(pD, 'load_test_contend2');
  assert.equal(await jan('abc004'), '4900000000044');
  assert.ok(!rD.conflicts.some((x) => x.kind === 'jan_contended' || x.kind === 'jan_taken'));
  assert.equal(await jan('abc003'), '4900000000077');
});

await ta('[H1] 外部 ID の移動計画: 入れ替え (循環) は通る。移れない持ち主に連なる要求は taken で止まり、全ロードは失敗しない', async () => {
  const pid1 = await pidOf('abc001'); const pid2 = await pidOf('abc002'); const pid3 = await pidOf('abc003'); const pid4 = await pidOf('abc004');
  // 入れ替え: abc001 → …028、abc002 → …011 (どちらも相手が持っている値)
  const pS = structuredClone(plan);
  for (const o of pS.observations) {
    if (o.sourceRef === 'product_drafts:1') { o.valueText = '4900000000028'; o.observedAt = '2026-09-08T00:00:00.000Z'; }
    if (o.sourceRef === 'product_drafts:2' || o.sourceRef === 'draft_sku_jans:2') { o.valueText = '4900000000011'; o.observedAt = '2026-09-08T00:00:00.000Z'; }
  }
  const closedBefore = (await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and valid_to is not null"))[0].n;
  const rS = await run(pS, 'load_test_swap');
  assert.equal(await jan('abc001'), '4900000000028'); assert.equal(await jan('abc002'), '4900000000011');
  assert.equal(rS.conflicts.filter((x) => x.kind === 'jan_replaced').length, 2);
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and valid_to is not null"))[0].n, closedBefore + 2);
  // 戻す (9/09 の観測) → また入れ替わる
  const pS2 = structuredClone(plan);
  for (const o of pS2.observations) if (['product_drafts:1', 'product_drafts:2', 'draft_sku_jans:2'].includes(o.sourceRef)) o.observedAt = '2026-09-09T00:00:00.000Z';
  await run(pS2, 'load_test_swap_back');
  assert.equal(await jan('abc001'), '4900000000011'); assert.equal(await jan('abc002'), '4900000000028');
  // 連鎖: abc004 → …077 (abc003 が持つ)、abc003 → …011 (abc001 が持つ)、abc001 は動かない → abc003 は taken → abc004 も taken (abc003 が手放さない)。例外にならない
  const pCh = structuredClone(pS2);
  pCh.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000077', source: 'product_hub', sourceRef: 'test:ch4', observedAt: '2026-09-09T02:00:00.000Z' });
  pCh.observations.push({ skuCode: 'abc003', attribute: 'jan', scope: 'item', valueText: '4900000000011', source: 'product_hub', sourceRef: 'test:ch3', observedAt: '2026-09-09T02:00:00.000Z' });
  const rCh = await run(pCh, 'load_test_chain');
  assert.equal(rCh.ok, true);
  assert.deepEqual(rCh.conflicts.filter((x) => x.kind === 'jan_taken').map((x) => [x.wanted_by.id, x.held_by.id]).sort(), [[pid3, pid1], [pid4, pid3]].sort());
  assert.equal(await jan('abc001'), '4900000000011'); assert.equal(await jan('abc003'), '4900000000077'); assert.equal(await jan('abc004'), '4900000000044');
  assert.equal((await q("select count(*)::int as n from core.external_ids where id_kind = 'jan' and valid_to is not null"))[0].n, closedBefore + 4);   // 連鎖では何も閉じない
  assert.ok([pid1, pid2, pid3, pid4].every(Number.isFinite));
  // 人が付けた JAN (manual) があるエンティティには、別の値を自動で付けない (abc004 の最新の観測は …077 = 取り合い中。manual …088 があるので manual_kept)
  await db.query("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id) values (1, 'product', $1, 'jan', 'jan', '4900000000088', 'manual', 'human', 'test')", [pid4]);
  const rM = await run(pS2, 'load_test_manual_jan');
  assert.ok(rM.conflicts.some((x) => x.kind === 'jan_manual_kept' && x.entity.id === pid4 && x.manual === '4900000000088' && x.wanted === '4900000000077'), JSON.stringify(rM.conflicts.filter((x) => /jan/.test(x.kind))));
  assert.deepEqual((await q("select external_value from core.external_ids where entity_type = 'product' and entity_id = $1 and id_kind = 'jan' and valid_to is null order by 1", [pid4])).map((r) => r.external_value), ['4900000000044', '4900000000088']);   // 自動の …044 も閉じない
  // 既に持っている …044 を新しい観測で要求し直す → same (manual と併存したまま)
  const pM = structuredClone(pS2);
  pM.observations.push({ skuCode: 'abc004', attribute: 'jan', scope: 'item', valueText: '4900000000044', source: 'product_hub', sourceRef: 'test:ok', observedAt: '2026-09-09T03:00:00.000Z' });
  const rM2 = await run(pM, 'load_test_manual_jan2');
  assert.ok(!rM2.conflicts.some((x) => x.kind === 'jan_manual_kept'));
  assert.equal(rM2.summary.jan.same, 3);   // abc001 / abc002 / abc004 (…044)。abc003 は最新の観測 …011 (ch3) を要求し続けて taken (skip)
  assert.deepEqual((await q("select external_value from core.external_ids where entity_type = 'product' and entity_id = $1 and id_kind = 'jan' and valid_to is null order by 1", [pid4])).map((r) => r.external_value), ['4900000000044', '4900000000088']);
  await db.query("update core.external_ids set valid_to = now() where entity_id = $1 and external_value = '4900000000088'", [pid4]);
});

await ta('[4][H2] 正規化衝突で落とした SKU (ＡＢＣ004) と出品 (ＡＢＣ001) は観測・構成・出品・listingRef のどこにも使わない', async () => {
  const pI = structuredClone(plan);
  pI.observations.push({ skuCode: 'ＡＢＣ004', attribute: 'brand', scope: 'item', valueText: '混入ブランド', source: 'product_hub', sourceRef: 'test:iso', observedAt: null });
  pI.setComponents.push({ parentCode: 'abc001set3', childCode: 'ＡＢＣ004', qty: 1, source: 'ne' });
  pI.listings.push({ mall: 'yahoo', shopCode: 'main', listingCode: 'iso-y', status: 'active', components: [{ code: 'ＡＢＣ004', qty: 1, resolution: 'exact' }] });
  pI.listings.push({ mall: 'yahoo', shopCode: 'main', listingCode: 'ＡＢＣ001', status: 'active', components: [] });   // 既存の yahoo abc001 と正規化衝突
  pI.observations.push({ listingRef: { mall: 'yahoo', shopCode: 'main', listingCode: 'ＡＢＣ001' }, attribute: 'jan', scope: 'listing', valueText: '4900000000066', source: 'fba_sheet_import', sourceRef: 'test:isoref', observedAt: null });
  const rI = await run(pI, 'load_test_iso');
  assert.ok(rI.sections.observations.skipped.some((s) => s.code === 'ＡＢＣ004'));
  assert.ok(rI.sections.observations.skipped.some((s) => s.listing === 'ＡＢＣ001'));
  assert.ok(rI.sections.listings.skipped.some((s) => s.code === 'ＡＢＣ001'));
  assert.ok(rI.sections.set_components.skipped.some((s) => s.child === 'ＡＢＣ004'));
  assert.ok(rI.sections.listing_components.skipped.some((s) => s.code === 'ＡＢＣ004'));
  const p = (await q("select brand from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'abc004'"))[0];
  assert.equal(p.brand, null);
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where value_text = '4900000000066'"))[0].n, 0);
  assert.equal((await q("select count(*)::int as n from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id where l.listing_code = 'iso-y'"))[0].n, 0);
  // skip があった親 (abc001set3) の既存構成は消されない (完全に読めていないので触らない)
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 1);
});

await ta('[7][H3][H4] 構成の訂正: 完全に読めた (非空・skip 無し) 出品だけ plan に合わせる。重複 skip・空・manual の数量違いでは消さない', async () => {
  const lid = Number((await q("select listing_id from core.listings where listing_code = 'pr_bundle'"))[0].listing_id);
  const sid2 = Number((await q("select sku_id from core.skus where code = 'abc002'"))[0].sku_id);
  const cnt = async () => (await q('select count(*)::int as n from core.listing_components where listing_id = $1', [lid]))[0].n;
  assert.equal(await cnt(), 2);
  // 重複 (A+A) は不完全 → 消さない
  const pDup = structuredClone(plan); pDup.listings.find((l) => l.listingCode === 'pr_bundle').components = [{ code: 'abc001', qty: 1, resolution: 'imported' }, { code: 'abc001', qty: 1, resolution: 'imported' }];
  const rDup = await run(pDup, 'load_test_dup');
  assert.equal(await cnt(), 2); assert.equal(rDup.summary.listing_components.skipped, 3);
  // 空 (読めなかった・未解決) → 消さない
  const pEmpty = structuredClone(plan); pEmpty.listings.find((l) => l.listingCode === 'pr_bundle').components = [];
  await run(pEmpty, 'load_test_empty');
  assert.equal(await cnt(), 2);
  // 完全に読めた (abc001 だけ) → abc002 が消える
  const pF = structuredClone(plan); pF.listings.find((l) => l.listingCode === 'pr_bundle').components = [{ code: 'abc001', qty: 1, resolution: 'imported' }];
  const rF = await run(pF, 'load_test_fix');
  assert.equal(await cnt(), 1);
  assert.ok(rF.sections.listing_components.notes.some((n) => n === 'stale removed: 1'));
  // 人が手で足した行は plan に無くても残る
  await db.query("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type, resolved_by_id) values (1, $1, $2, 2, 'manual', 'human', 'test')", [lid, sid2]);
  const rG = await run(pF, 'load_test_fix2');
  assert.equal(await cnt(), 2);
  assert.ok(rG.conflicts.some((c) => c.kind === 'listing_component_manual_kept' && c.listing_id === lid && c.sku_id === sid2));
  // 元の plan (abc002 qty 2 imported) を流しても manual は上書きされない (数量が同じ = same)
  const rH = await run(plan, 'load_test_fix3');
  assert.equal((await q('select resolution, qty from core.listing_components where listing_id = $1 and sku_id = $2', [lid, sid2]))[0].resolution, 'manual');
  assert.equal(rH.summary.listing_components.same, 1); assert.ok(!rH.conflicts.some((c) => c.kind === 'listing_component_manual_mismatch'));
  // manual と数量が違う → skip + conflict (manual の 2 のまま)。skip があるので他の行も消さない
  const pQ = structuredClone(plan); pQ.listings.find((l) => l.listingCode === 'pr_bundle').components = [{ code: 'abc002', qty: 3, resolution: 'imported' }];
  const rQ = await run(pQ, 'load_test_manual_qty');
  assert.ok(rQ.conflicts.some((c) => c.kind === 'listing_component_manual_mismatch' && c.manual_qty === 2 && c.plan_qty === 3));
  assert.equal(rQ.summary.listing_components.same, 0); assert.ok(rQ.sections.listing_components.skipped.some((s) => s.code === 'abc002' && /manual/.test(s.reason)));
  assert.equal(Number((await q('select qty from core.listing_components where listing_id = $1 and sku_id = $2', [lid, sid2]))[0].qty), 2);
  assert.equal(await cnt(), 2);
  // セット構成も同じ (plan から子を外す → 消える。manual の数量違いは conflict)
  const pS = structuredClone(plan); pS.setComponents = pS.setComponents.filter((c) => c.childCode !== 'nosuch');   // skip が無い = 完全に読めた
  pS.setComponents.push({ parentCode: 'abc001set3', childCode: 'abc002', qty: 1, source: 'ne' });
  await run(pS, 'load_test_set');
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 2);
  const pS2 = structuredClone(pS); pS2.setComponents = pS2.setComponents.filter((c) => c.childCode !== 'abc002');
  await run(pS2, 'load_test_set2');
  assert.equal((await q("select count(*)::int as n from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id where s.code = 'abc001set3'"))[0].n, 1);
  const setId = Number((await q("select sku_id from core.skus where code = 'abc001set3'"))[0].sku_id); const sid1 = Number((await q("select sku_id from core.skus where code = 'abc001'"))[0].sku_id);
  await db.query("update core.sku_components set source = 'manual', qty = 5 where parent_sku_id = $1 and child_sku_id = $2", [setId, sid1]);
  const rSm = await run(pS2, 'load_test_set_manual');
  assert.ok(rSm.conflicts.some((c) => c.kind === 'set_component_manual_mismatch' && c.manual_qty === 5 && c.plan_qty === 3));
  assert.equal(Number((await q('select qty from core.sku_components where parent_sku_id = $1 and child_sku_id = $2', [setId, sid1]))[0].qty), 5);
  await db.query("update core.sku_components set source = 'ne', qty = 3 where parent_sku_id = $1 and child_sku_id = $2", [setId, sid1]);
});

await ta('[H1-R3] 外部 ID: 既に持つ値 (same) と新しい値を同時に要求しても、持つ値は閉じない・他へ手放さない。再実行で変わらない', async () => {
  const lid = Number((await q("select listing_id from core.listings where mall = 'rakuten' and listing_code = 'abc001-am'"))[0].listing_id);
  const active = async () => (await q("select external_value from core.external_ids where entity_type = 'listing' and entity_id = $1 and id_kind = 'item_number' and valid_to is null order by 1", [lid])).map((r) => r.external_value);
  assert.deepEqual(await active(), ['abc001']);
  const pA = structuredClone(plan); pA.listings.find((l) => l.mall === 'rakuten' && l.listingCode === 'abc001-am').externalIds.push({ system: 'rakuten', kind: 'item_number', value: 'abc001-alias2' });
  const rA = await run(pA, 'load_test_same_ok');
  assert.deepEqual(await active(), ['abc001', 'abc001-alias2']);                             // a (same) は閉じない、b は付く
  assert.ok(!rA.conflicts.some((c) => c.kind === 'listing_external_id_replaced'));
  const rA2 = await run(pA, 'load_test_same_ok2');
  assert.deepEqual(await active(), ['abc001', 'abc001-alias2']); assert.equal(rA2.summary.listing_external_ids.applied, 0);
  // 別の出品が同じ値 'abc001' を要求 → 持ち主 (same で保持) は手放さない → taken
  const pB = structuredClone(pA); pB.listings.find((l) => l.mall === 'rakuten' && l.listingCode === 'abc002-am1').externalIds.push({ system: 'rakuten', kind: 'item_number', value: 'abc001' });
  const rB = await run(pB, 'load_test_same_taken');
  assert.ok(rB.conflicts.some((c) => c.kind === 'listing_external_id_taken' && c.value === 'abc001' && c.held_by.id === lid));
  assert.deepEqual(await active(), ['abc001', 'abc001-alias2']);
  await run(plan, 'load_test_same_back');   // 元の plan (alias2 の要求が無い) → alias2 は「要求が無い」だけなので残る (閉じるのは新規に通る要求があるときだけ)
  assert.deepEqual(await active(), ['abc001', 'abc001-alias2']);
  await db.query("update core.external_ids set valid_to = now() where entity_id = $1 and external_value = 'abc001-alias2'", [lid]);
});

await ta('[H7-R3] DB 側で FNSKU が manual に守られて付かなかったとき、その FNSKU の重量は商品に付けない', async () => {
  const lid = Number((await q("select listing_id from core.listings where listing_code = 'pr_sheetonly'"))[0].listing_id);
  await db.query("update core.external_ids set resolution = 'manual' where entity_type = 'listing' and entity_id = $1 and id_kind = 'fnsku' and valid_to is null", [lid]);   // X00FNSKU9 を人が確定
  const pW = structuredClone(plan);
  const l = pW.listings.find((x) => x.listingCode === 'pr_sheetonly'); l.fnskuCandidates = [{ fnsku: 'X00FNSKU2', source: 'fba_sheet_import' }];   // 入力は X00FNSKU2 に変わった
  const via = { fnsku: 'X00FNSKU2', listing: { mall: 'amazon', shopCode: SHOP_CODES.amazon, listingCode: 'pr_sheetonly' } };
  pW.physicals.push({ skuCode: 'abc002', scope: 'package', weightG: 500, source: 'amazon_catalog', sourceRef: 'fbx_weight_refs:X00FNSKU2', isMeasured: false, observedAt: '2026-09-01T00:00:00.000Z', via });
  pW.observations.push({ skuCode: 'abc002', attribute: 'package_weight_g', scope: 'package', valueNum: 500, unit: 'g', source: 'amazon_catalog', sourceRef: 'fbx_weight_refs:X00FNSKU2', observedAt: '2026-09-01T00:00:00.000Z', via });
  const rW = await run(pW, 'load_test_via');
  assert.ok(rW.conflicts.some((c) => c.kind === 'listing_external_id_manual_kept' && c.entity.id === lid && c.wanted === 'X00FNSKU2'));
  assert.ok(rW.sections.physicals.skipped.some((s) => s.code === 'abc002' && /X00FNSKU2/.test(s.reason)));
  assert.ok(rW.sections.observations.skipped.some((s) => s.code === 'abc002' && /X00FNSKU2/.test(s.reason)));
  assert.equal((await q('select count(*)::int as n from core.product_physicals where weight_g = 500'))[0].n, 0);
  assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where value_num = 500 and attribute = 'package_weight_g'"))[0].n, 0);
  // manual の FNSKU (X00FNSKU9) の重量なら付く (出品に実際に付いている)
  const pV = structuredClone(plan);
  const via9 = { fnsku: 'X00FNSKU9', listing: via.listing };
  pV.physicals.push({ skuCode: 'abc002', scope: 'package', weightG: 480, source: 'amazon_catalog', sourceRef: 'fbx_weight_refs:X00FNSKU9', isMeasured: false, observedAt: '2026-09-01T00:00:00.000Z', via: via9 });
  await run(pV, 'load_test_via_ok');
  assert.equal((await q('select count(*)::int as n from core.product_physicals where weight_g = 480'))[0].n, 1);
  await db.query("update core.external_ids set resolution = 'imported' where entity_type = 'listing' and entity_id = $1 and id_kind = 'fnsku' and valid_to is null", [lid]);
});

await ta('[H6] FNSKU の明示的な解除 (attrs が空にした) は既存の自動付与を閉じる。入力欠落 (候補なし・解除なし) では閉じない', async () => {
  const lid = Number((await q("select listing_id from core.listings where listing_code = 'pr_ABC001'"))[0].listing_id);
  const active = async () => (await q("select external_value from core.external_ids where entity_type = 'listing' and entity_id = $1 and id_kind = 'fnsku' and valid_to is null", [lid])).map((r) => r.external_value);
  assert.deepEqual(await active(), ['X00FNSKU1']);
  const pNo = structuredClone(plan); const l = pNo.listings.find((x) => x.listingCode === 'pr_ABC001'); l.fnskuCandidates = [];   // 欠落 (解除ではない)
  await run(pNo, 'load_test_fnsku_missing');
  assert.deepEqual(await active(), ['X00FNSKU1']);
  const pClr = structuredClone(plan); const l2 = pClr.listings.find((x) => x.listingCode === 'pr_ABC001'); l2.fnskuCandidates = []; l2.fnskuCleared = true;
  const rClr = await run(pClr, 'load_test_fnsku_clear');
  assert.deepEqual(await active(), []);
  assert.equal(rClr.summary.fnsku_clears.expected, 1); assert.equal(rClr.summary.fnsku_clears.applied, 1);
  assert.ok(rClr.conflicts.some((c) => c.kind === 'fnsku_cleared' && c.listing_id === lid && c.old[0] === 'X00FNSKU1'));
  const rClr2 = await run(pClr, 'load_test_fnsku_clear2');   // もう一度 → 閉じるものが無い = same
  assert.equal(rClr2.summary.fnsku_clears.same, 1);
  await run(plan, 'load_test_fnsku_back');                    // 元に戻す → 新しい有効行
  assert.deepEqual(await active(), ['X00FNSKU1']);
  assert.equal((await q("select count(*)::int as n from core.external_ids where entity_type = 'listing' and entity_id = $1 and id_kind = 'fnsku'", [lid]))[0].n, 2);
  // 🚨 結合: SQLite (fba_sku_attrs の planning 行) で FNSKU を空にする → plan を作り直す → 解除が engine まで届く (Codex R3-2: フラグを plan に直接置く試験では見つからなかった)
  const fdb = new Database(path.join(dataDir, 'fba.db'));
  fdb.prepare("update fba_sku_attrs set fnsku = null, updated_at = '2026-09-08 10:00:00' where amazon_sku = 'pr_ABC001'").run(); fdb.close();
  const planCleared = buildPlanFromRender({ dataDir, log: quiet });
  const lc = planCleared.listings.find((x) => x.listingCode === 'pr_ABC001');
  assert.equal(lc.fnskuCleared, true); assert.deepEqual(lc.fnskuCandidates, []);
  assert.ok(planCleared.sources.fnsku_cleared_by_attrs.includes('pr_ABC001'));
  assert.ok(!planCleared.physicals.some((p) => p.via?.fnsku === 'X00FNSKU1'));   // 付かない FNSKU の重量は plan にも出ない
  const rInt = await run(planCleared, 'load_test_fnsku_clear_int');
  assert.equal(rInt.summary.fnsku_clears.applied, 1);
  assert.deepEqual(await active(), []);
  const fdb2 = new Database(path.join(dataDir, 'fba.db'));
  fdb2.prepare("update fba_sku_attrs set fnsku = 'X00FNSKU1', updated_at = '2026-09-02 10:00:00' where amazon_sku = 'pr_ABC001'").run(); fdb2.close();
  await run(buildPlanFromRender({ dataDir, log: quiet }), 'load_test_fnsku_clear_int_back');
  assert.deepEqual(await active(), ['X00FNSKU1']);
});

await ta('[R4-1] FNSKU の解除と別の出品への移管が同じ plan で 1 回で完結する (解除を移動判定より先に)', async () => {
  const lidA = Number((await q("select listing_id from core.listings where listing_code = 'pr_ABC001'"))[0].listing_id);
  const lidB = Number((await q("select listing_id from core.listings where listing_code = 'pr_bundle'"))[0].listing_id);
  const active = async (lid) => (await q("select external_value from core.external_ids where entity_type = 'listing' and entity_id = $1 and id_kind = 'fnsku' and valid_to is null", [lid])).map((r) => r.external_value);
  assert.deepEqual(await active(lidA), ['X00FNSKU1']); assert.deepEqual(await active(lidB), []);
  const pMv = structuredClone(plan);
  const a = pMv.listings.find((x) => x.listingCode === 'pr_ABC001'); a.fnskuCandidates = []; a.fnskuCleared = true;
  const b = pMv.listings.find((x) => x.listingCode === 'pr_bundle'); b.fnskuCandidates = [{ fnsku: 'X00FNSKU1', source: 'fba_sheet_import' }];
  const rMv = await run(pMv, 'load_test_move');
  assert.deepEqual(await active(lidA), []); assert.deepEqual(await active(lidB), ['X00FNSKU1']);   // 1 回で移る
  assert.ok(!rMv.conflicts.some((c) => c.kind === 'listing_external_id_taken'));
  assert.equal(rMv.summary.fnsku_clears.applied, 1); assert.equal(rMv.summary.listing_external_ids.skipped, 0);
  const rMv2 = await run(pMv, 'load_test_move2');   // 再実行で不変
  assert.deepEqual(await active(lidA), []); assert.deepEqual(await active(lidB), ['X00FNSKU1']);
  assert.equal(rMv2.summary.listing_external_ids.applied, 0); assert.equal(rMv2.summary.fnsku_clears.same, 1);
  await run(plan, 'load_test_move_back');   // 元に戻す (A が要求、B は要求なし → B の行は残る = 要求が無いだけでは閉じない → A は taken)
  assert.deepEqual(await active(lidB), ['X00FNSKU1']); assert.deepEqual(await active(lidA), []);
  await db.query("update core.external_ids set valid_to = now() where entity_id = $1 and id_kind = 'fnsku' and valid_to is null", [lidB]);
  await run(plan, 'load_test_move_back2');
  assert.deepEqual(await active(lidA), ['X00FNSKU1']);
});

await ta('[R4-2] 未来の観測時刻は理由つきで入れない。既に未来の行があっても「最新」に数えないので、時刻の無い再送が毎回増えない', async () => {
  const pid = await pidOf('abc001');
  const cntPhy = async () => (await q("select count(*)::int as n from core.product_physicals where product_id = $1 and source_ref = 'fbx_weight_current:X00FNSKU1'", [pid]))[0].n;
  const before = await cntPhy();
  // 入力が未来 → skip
  const pF = structuredClone(plan);
  pF.physicals.push({ skuCode: 'abc001', scope: 'package', weightG: 999, source: 'measured', sourceRef: 'pm_skus', isMeasured: true, observedAt: '2031-01-01T00:00:00.000Z' });
  pF.observations.push({ skuCode: 'abc001', attribute: 'brand', scope: 'item', valueText: '未来ブランド', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: '2031-01-01T00:00:00.000Z' });
  const rF = await run(pF, 'load_test_future_in');
  assert.ok(rF.sections.physicals.skipped.some((s) => /未来/.test(s.reason)));
  assert.ok(rF.sections.observations.skipped.some((s) => /未来/.test(s.reason)));
  assert.equal((await q('select count(*)::int as n from core.product_physicals where weight_g = 999'))[0].n, 0);
  assert.equal((await q("select brand from core.products where product_id = $1", [pid]))[0].brand, 'テストブランド');
  // DB に未来の行がある (別の書き手が入れた想定) → 時刻の無い 310g は「最新 (未来を除く) と同じ内容」で再送 = 増えない。有効行にもならない
  await db.query("insert into core.product_physicals (company_id, product_id, scope, source_system, source_ref, weight_g, is_measured, observed_at, created_by_type, created_by_id) values (1, $1, 'package', 'amazon_catalog', 'fbx_weight_current:X00FNSKU1', 100, false, '2031-01-01T00:00:00Z', 'system', 'test')", [pid]);
  await db.query("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_num, value_unit, raw_text, source_system, source_ref, observed_at, content_hash) values ('test:future', 'product', $1, 'package_weight_g', 'package', 100, 'g', '100', 'amazon_catalog', 'fbx_weight_current:X00FNSKU1', '2031-01-01T00:00:00Z', 'x')", [pid]);
  const r1 = await run(plan, 'load_test_future_db1');
  const r2 = await run(plan, 'load_test_future_db2');
  assert.equal(await cntPhy(), before + 1);   // 未来の 100g だけ増えた。310g は増えない
  assert.equal(r1.summary.physicals.applied, 0); assert.equal(r2.summary.physicals.applied, 0);
  assert.equal(r1.summary.observations.applied, 0); assert.equal(r2.summary.observations.applied, 0);
  const eff = (await q("select weight_g from core.product_physicals where product_id = $1 and scope = 'package' and is_effective", [pid]))[0];
  assert.equal(eff.weight_g, 320);   // 未来の 100g は有効行にならない
  await db.query("update core.product_physicals set is_effective = false where product_id = $1 and observed_at > now()", [pid]);   // 念のため
});

await ta('[R5-1] 5 分以内の未来行 (許容範囲) が最新に居座っても、時刻の無い再送は毎回増えない (比較対象はロード時刻以前の行だけ)', async () => {
  const pid = await pidOf('abc001');
  const soon = new Date(Date.now() + 2 * 60 * 1000).toISOString();   // +2 分 = 入力としては許容される未来
  await db.query("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, source_ref, observed_at, content_hash) values ('test:soon', 'product', $1, 'brand', 'item', 'Q未来', 'Q未来', 'qoo10', 'qoo10:700002', $2, 'y')", [pid, soon]);
  await db.query("insert into core.product_physicals (company_id, product_id, scope, source_system, source_ref, weight_g, is_measured, observed_at, created_by_type, created_by_id) values (1, $1, 'package', 'amazon_catalog', 'fbx_weight_current:X00FNSKU1', 100, false, $2, 'system', 'test')", [pid, soon]);
  const cntObs = async () => (await q("select count(*)::int as n from core.product_attribute_observations where entity_id = $1 and source_ref = 'qoo10:700002'", [pid]))[0].n;
  const cntPhy = async () => (await q("select count(*)::int as n from core.product_physicals where product_id = $1 and source_ref = 'fbx_weight_current:X00FNSKU1'", [pid]))[0].n;
  const pS = structuredClone(plan);
  pS.observations.push({ skuCode: 'abc001', attribute: 'brand', scope: 'item', valueText: 'Qブランド', source: 'qoo10', sourceRef: 'qoo10:700002', observedAt: null });   // 時刻なし・内容は未来行と違う
  const o0 = await cntObs(); const p0 = await cntPhy();
  const r1 = await run(pS, 'load_test_soon1');
  assert.equal(await cntObs(), o0 + 1);                                       // 初回はロード時刻で入る (ロード時刻以前の行が無い)
  assert.equal(await cntPhy(), p0);                                           // 310g は既存の最新 (ロード時刻以前) と同じ内容 → 再送
  const r2 = await run(pS, 'load_test_soon2');
  const r3 = await run(pS, 'load_test_soon3');
  assert.equal(await cntObs(), o0 + 1); assert.equal(await cntPhy(), p0);     // 2 回目以降は増えない
  assert.equal(r2.summary.observations.applied, 0); assert.equal(r3.summary.observations.applied, 0);
  assert.equal(r1.summary.physicals.applied, 0); assert.equal(r2.summary.physicals.applied, 0);
  await db.query("update core.product_physicals set is_effective = false where product_id = $1 and observed_at > now()", [pid]);
});

await ta('[R5-2] 既に採用されている未来由来の解決・有効行は、その回で解除される (置き換え候補があれば置き換え、無ければ解決を消して列も空に)', async () => {
  const pid1 = await pidOf('abc001'); const pid3 = await pidOf('abc003');
  // (a) 置き換え候補あり: abc001 の brand が未来の観測で採用されている → plan の product_hub (9/02) に置き換わる
  const oid = Number((await q("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, source_ref, observed_at, content_hash) values ('test:fut-brand', 'product', $1, 'brand', 'item', '未来ブランド', '未来ブランド', 'product_hub', 'draft_page_info:1', '2031-01-01T00:00:00Z', 'z') returning observation_id", [pid1]))[0].observation_id);
  await db.query("update core.attribute_resolutions set resolved_observation_id = $2, resolved_at = now() where entity_type = 'product' and entity_id = $1 and attribute = 'brand' and packaging_scope = 'item'", [pid1, oid]);
  await db.query("update core.products set brand = '未来ブランド' where product_id = $1", [pid1]);
  // (b) 置き換え候補なし: abc001 の manufacturer が未来の観測で採用されている (plan に manufacturer は無い) → 解決を消し、列も空に
  const oid2 = Number((await q("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, source_ref, observed_at, content_hash) values ('test:fut-mfr', 'product', $1, 'manufacturer', 'item', 'MFR未来', 'MFR未来', 'product_hub', 'draft_page_info:1', '2031-01-01T00:00:00Z', 'w') returning observation_id", [pid1]))[0].observation_id);
  await db.query("insert into core.attribute_resolutions (entity_type, entity_id, attribute, packaging_scope, resolved_observation_id, rule_version) values ('product', $1, 'manufacturer', 'item', $2, 'v1')", [pid1, oid2]);
  await db.query("update core.products set manufacturer = 'MFR未来' where product_id = $1", [pid1]);
  // (c) 有効行: abc003 (plan に物理属性なし・既存の物理属性もなし) に未来の有効行がある → 解除される
  await db.query("insert into core.product_physicals (company_id, product_id, scope, source_system, source_ref, weight_g, is_measured, observed_at, is_effective, created_by_type, created_by_id) values (1, $1, 'package', 'amazon_catalog', 'test:fut', 100, false, '2031-01-01T00:00:00Z', true, 'system', 'test')", [pid3]);
  const rR = await run(plan, 'load_test_revoke');
  assert.equal(rR.summary.future_revocations.expected, 2); assert.equal(rR.summary.future_revocations.same, 1); assert.equal(rR.summary.future_revocations.applied, 1);
  const p = (await q('select brand, manufacturer from core.products where product_id = $1', [pid1]))[0];
  assert.equal(p.brand, 'テストブランド'); assert.equal(p.manufacturer, null);
  assert.equal((await q("select count(*)::int as n from core.attribute_resolutions where entity_id = $1 and attribute = 'manufacturer'", [pid1]))[0].n, 0);
  assert.ok(rR.conflicts.some((c) => c.kind === 'resolution_future_revoked' && c.product_id === pid1 && c.attribute === 'manufacturer'));
  assert.equal((await q("select count(*)::int as n from core.product_physicals where product_id = $1 and is_effective", [pid3]))[0].n, 0);
  assert.ok(rR.conflicts.some((c) => c.kind === 'physical_future_revoked' && c.product_id === pid3));
  const rR2 = await run(plan, 'load_test_revoke2');   // 再実行: もう解除するものは無い
  assert.equal(rR2.summary.future_revocations.expected, 0);
  assert.equal((await q('select manufacturer from core.products where product_id = $1', [pid1]))[0].manufacturer, null);
});

await ta('[M4-R3] running.json: 開始記録を書けなければ始めない。終了記録を書けなければ running.json を残す (interrupted として見える)', async () => {
  const { runLoadOnce, readRunning } = await import('./load/run-initial-load.mjs');
  const url = 'postgres://nobody:nothing@127.0.0.1:1/none';
  // (a) outDir の親がファイル → mkdir 失敗 → 始めない
  const blocker = path.join(dataDir, 'notadir'); fs.writeFileSync(blocker, 'x');
  let err;
  try { await runLoadOnce({ dataDir, url, outDir: path.join(blocker, 'company-db'), log: quiet, runId: 'load_test_nomark' }); } catch (e) { err = e; }
  assert.ok(err && err.code === 'RUNNING_MARK_FAILED', String(err && err.code));
  // (b) latest.json がディレクトリ → 終了記録を書けない → running.json は残る
  const dir2 = path.join(dataDir, 'out2'); fs.mkdirSync(path.join(dir2, 'latest.json'), { recursive: true });
  err = null;
  try { await runLoadOnce({ dataDir, url, outDir: dir2, log: quiet, runId: 'load_test_nowrite' }); } catch (e) { err = e; }
  assert.ok(err && /connect/.test(err.report.error));
  assert.equal(readRunning(dir2)?.run_id, 'load_test_nowrite');
  // (c) 普通に終われば消える
  const dir3 = path.join(dataDir, 'out3');
  err = null;
  try { await runLoadOnce({ dataDir, url, outDir: dir3, log: quiet, runId: 'load_test_ok' }); } catch (e) { err = e; }
  assert.ok(err); assert.equal(readRunning(dir3), null); assert.ok(fs.existsSync(path.join(dir3, 'latest.json')));
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

await ta('[6][M5] 予定 ≠ 投入 なら LOAD_UNBALANCED で全部巻き戻す (adapter が 1 行黙って落とした場合)', async () => {
  const before = await counts();
  const dbBad = { exec: (t2) => db.exec(t2), query: async (sql, params) => { const r = await db.query(sql, params); return /insert into core\.workers/.test(sql) ? { ...r, rows: r.rows.slice(1) } : r; } };
  const pE = structuredClone(plan); pE.skus.push({ code: 'newsku_unbalanced', name: '巻き戻る', kind: 'single', handling: 'active' });
  let err;
  try { await runInitialLoad(dbBad, pE, { log: quiet, runId: 'load_test_unbalanced' }); } catch (e) { err = e; }
  assert.ok(err, '例外が出るはず'); assert.equal(err.code, 'LOAD_UNBALANCED'); assert.equal(err.section, 'workers');
  assert.match(err.report.error, /workers: 予定 2 ≠ 投入 1/);
  assert.deepEqual(await counts(), before);
  assert.equal((await q("select count(*)::int as n from core.skus where code = 'newsku_unbalanced'"))[0].n, 0);
  // 重複は skip に数えられて釣り合う (正常)。report の md に区分の表が出る
  const bad = structuredClone(plan);
  bad.setComponents.push({ parentCode: 'abc001set3', childCode: 'abc001', qty: 3, source: 'ne' });
  const r = await run(bad, 'load_test_4');
  assert.equal(r.summary.set_components.skipped, 2);
  const md = reportToMarkdown(r);
  assert.match(md, /set_components \| 3 \| /);
  assert.match(md, /不一致/);
});

console.log('\nrouter (202 + /status、ヘッダ認証だけ、再起動後の interrupted)');
await ta('[13][15][M4] POST /load は 202 で run_id を返し、/status に実行中→完了が出る。?sync_key= は 401。途中で死んだ記録 (running.json) は interrupted に出る', async () => {
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
  const H = { headers: { 'x-sync-key': 'test-key' } };
  try {
    assert.equal((await call('/status')).status, 401);
    assert.equal((await call('/status?sync_key=test-key')).status, 401);                          // クエリでは通さない
    assert.equal((await call('/status?counts=0', H)).status, 200);
    // 途中で死んだ記録を置いておく → /status の interrupted に出る、POST の previous_interrupted に出る
    fs.mkdirSync(path.join(dataDir, 'company-db'), { recursive: true });
    fs.writeFileSync(path.join(dataDir, 'company-db', 'running.json'), JSON.stringify({ run_id: 'load_dead', dry_run: false, started_at: '2026-09-09T00:00:00.000Z' }));
    const st = await call('/status?counts=0', H);
    assert.equal(st.body.interrupted.run_id, 'load_dead');
    // 同時に 2 本 → 1 本だけ 202、もう 1 本は 409 (単一飛行)
    const both = await Promise.all([call('/load', { method: 'POST', ...H }), call('/load', { method: 'POST', ...H })]);
    assert.deepEqual(both.map((x) => x.status).sort(), [202, 409], JSON.stringify(both.map((x) => x.status)));
    const started = both.find((x) => x.status === 202); const dup = both.find((x) => x.status === 409);
    assert.ok(started.body.run_id); assert.equal(started.body.dry_run, true);
    assert.equal(started.body.previous_interrupted.run_id, 'load_dead');
    assert.equal(dup.body.run_id, started.body.run_id);
    for (let i = 0; i < 100 && routerMod.getLoadState().current; i++) await new Promise((r) => setTimeout(r, 100));
    assert.equal(routerMod.getLoadState().current, null, '終わっているはず');
    const st1 = await call('/status?counts=0', H);
    assert.equal(st1.body.last.run_id, started.body.run_id);
    assert.equal(st1.body.last.status, 'failed');                                                  // Postgres に繋がらない
    assert.equal(st1.body.latest.run_id, started.body.run_id);                                     // 初期失敗でも latest.json に残る
    assert.equal(st1.body.latest.ok, false); assert.match(st1.body.latest.error, /connect/);
    assert.equal(st1.body.interrupted, null);                                                      // 終わったので running.json は消えている
    assert.ok(!fs.existsSync(path.join(dataDir, 'company-db', 'running.json')));
    assert.ok(fs.existsSync(path.join(dataDir, 'company-db', `load-${started.body.run_id}.md`)));
    // [R4-3] latest.json が壊れていても (ディレクトリ) interrupted は出る
    fs.rmSync(path.join(dataDir, 'company-db', 'latest.json')); fs.mkdirSync(path.join(dataDir, 'company-db', 'latest.json'));
    fs.writeFileSync(path.join(dataDir, 'company-db', 'running.json'), JSON.stringify({ run_id: 'load_dead2', dry_run: false, started_at: '2026-09-09T00:00:00.000Z' }));
    const st2 = await call('/status?counts=0', H);
    assert.equal(st2.status, 200); assert.ok(st2.body.latest_error, 'latest_error'); assert.equal(st2.body.interrupted.run_id, 'load_dead2');
    // [R5-3] 壊れた running.json は「記録なし」にしない → interrupted_error
    fs.writeFileSync(path.join(dataDir, 'company-db', 'running.json'), '{not json');
    const st3 = await call('/status?counts=0', H);
    assert.equal(st3.status, 200); assert.equal(st3.body.interrupted, null); assert.match(st3.body.interrupted_error, /running\.json/);
    fs.rmSync(path.join(dataDir, 'company-db', 'latest.json'), { recursive: true }); fs.rmSync(path.join(dataDir, 'company-db', 'running.json'));
    // [B2] report の一覧と明細 (run_id の形以外は 400、無ければ 404、md も取れる)
    const lst = await call('/reports', H);
    assert.equal(lst.status, 200); assert.ok(lst.body.reports.some((r) => r.run_id === started.body.run_id && r.ok === false));
    const rep = await call(`/report/${started.body.run_id}`, H);
    assert.equal(rep.status, 200); assert.equal(rep.body.run_id, started.body.run_id); assert.match(rep.body.error, /connect/);
    const mdRes = await fetch(base + `/report/${started.body.run_id}?format=md`, H);
    assert.equal(mdRes.status, 200); assert.match(await mdRes.text(), /Company DB 初期ロード/);
    assert.equal((await call('/report/..%2Flatest', H)).status, 400);
    assert.equal((await call('/report/latest', H)).status, 400);
    assert.equal((await call('/report/load_000000000000000_000000', H)).status, 404);
    assert.equal((await fetch(base + `/report/${started.body.run_id}`)).status, 401);
  } finally { server.close(); delete process.env.MIRROR_SYNC_KEY; delete process.env.COMPANY_DB_URL; delete process.env.DATA_DIR; }
});

await ta('[B2] remote-load の base URL: RENDER_MIRROR_URL の末尾パス (/apps/mirror) を落として origin だけ。RENDER_PORTAL_URL が別ホストなら止める (publish.js と同じ)', async () => {
  const { baseOrigin, judgeRun } = await import('../../scripts/company-db/remote-load.mjs');
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror' }), 'https://portal.example.com');
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror', RENDER_PORTAL_URL: 'https://portal.example.com/' }), 'https://portal.example.com');
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror', RENDER_PORTAL_URL: 'https://evil.example.com/' }), '');   // 別ホストへは鍵を送らない = 止める
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror', RENDER_PORTAL_URL: 'http://portal.example.com/' }), '');
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror', RENDER_PORTAL_URL: 'not a url' }), '');                    // 指定があるのに読めない → mirror に落ちず止める
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'https://portal.example.com/apps/mirror', RENDER_PORTAL_URL: '   ' }), '');                          // 空白だけも「指定がある」→ 止める (syncBaseUrl と同じ)
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'http://portal.example.com/apps/mirror' }), '');                                                    // https 以外は使わない
  assert.equal(baseOrigin({ RENDER_MIRROR_URL: 'not a url' }), '');
  assert.equal(baseOrigin({}), '');
  // wait の判定: その run の完了と成功だけを OK にする
  const id = 'load_202609100022291_3eeaf1';
  assert.deepEqual(judgeRun({ current: { run_id: id } }, id).done, false);
  assert.equal(judgeRun({ current: null, last: { run_id: id, status: 'done' } }, id).ok, true);
  assert.equal(judgeRun({ current: null, last: { run_id: id, status: 'failed', error: 'x' } }, id).ok, false);
  assert.equal(judgeRun({ current: null, last: null, latest: { run_id: id, ok: true } }, id).ok, true);                   // 再起動後は latest.json で判定
  assert.equal(judgeRun({ current: null, last: null, latest: { run_id: id, ok: false } }, id).ok, false);
  assert.equal(judgeRun({ current: null, last: null, latest: { run_id: id, ok: true }, interrupted: { run_id: id, committed: null } }, id).ok, false);   // 中断 = 結果不明
  assert.equal(judgeRun({ current: null, last: { run_id: 'load_000000000000000_000000', status: 'done' }, latest: { run_id: 'load_000000000000000_000000', ok: true } }, id).ok, false);   // 別の run しか無い
  assert.equal(judgeRun({ current: null, last: { run_id: 'load_000000000000000_000000', status: 'done' }, latest: { run_id: id, ok: true } }, id).ok, false);   // last が別の run なら latest には落ちない (R2)
  assert.equal(judgeRun({ current: null, last: null, latest: null }, id).ok, false);
  assert.equal(judgeRun({ current: null, latest: { run_id: id, ok: true } }, null).ok, true);
  assert.equal(judgeRun({ current: null, last: { run_id: 'load_000000000000000_000000', status: 'failed' }, latest: { run_id: id, ok: true } }, null).ok, false);   // 省略時も last が正 (R2)
  assert.equal(judgeRun({ current: null, last: { run_id: id, status: 'failed' }, latest: { run_id: id, ok: true } }, null).ok, false);
  assert.equal(judgeRun({ current: { run_id: 'load_000000000000000_000000' }, latest: { run_id: id, ok: true } }, id).ok, true);   // 別の run が動いていても、その run は終わって成功 (last 無し = 再起動後)
  assert.equal(judgeRun({ current: { run_id: 'load_000000000000000_000000' }, latest: { run_id: id, ok: true } }, null).done, false);   // 省略時は動いている run を待つ
  assert.equal(judgeRun({ current: null, interrupted: null, interrupted_error: 'running.json が壊れている', last: null, latest: { run_id: id, ok: true } }, null).ok, false);   // 中断の有無が分からない = 結果不明 (R3)
  assert.equal(judgeRun({ current: null, interrupted: null, interrupted_error: 'x', last: null, latest: { run_id: id, ok: true } }, id).ok, false);
});

await pglite.close();
fs.rmSync(dataDir, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
