/**
 * FBA在庫補充 smoke テスト (総点検 P0-1〜P0-3)
 *
 * 実行: node apps/fba-replenishment/smoke.mjs
 * 一時DB (DATA_DIR=os tmp) を使うため本番データに影響しない。
 *
 * 検証対象:
 *  - P0-1: parseWarehouseCsv (RFC4180・ヘッダ/行検証・不正行率ガード)
 *  - P0-2: セット品の有効期限制限のセット数換算 (構成品qty考慮) + 上限クリップ
 *  - P0-3: data_quality (未マッピング / set_components不正 / PLANNING欠落) の可視化
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import iconv from 'iconv-lite';

// db.js は import 時に DATA_DIR を定数化するため、env 設定後に動的 import する
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-smoke-'));
process.env.FBA_SKU_MAPPING_SOURCE = 'sheet';

const db = await import('./db.js');
const { parseWarehouseCsv } = await import('./warehouse-csv.js');

let pass = 0, fail = 0;
function assert(cond, name, detail = '') {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${detail ? ' — ' + detail : ''}`); }
}

// ===== P0-1: parseWarehouseCsv =====
console.log('--- P0-1: 倉庫CSVパース+検証 ---');
{
  // 商品名に引用符付きカンマ → 列ズレしない
  const csv = '商品ID,商品名,ロケ,在庫数,引当数,有効期限\n' +
    'abc1,"ネジ,ボルトセット",A-01,10,2,\n';
  const r = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  assert(!r.error && r.items.length === 1, '引用符内カンマの行が列ズレしない', r.error);
  assert(r.items?.[0]?.product_name === 'ネジ,ボルトセット', '商品名にカンマが保持される');
  assert(r.items?.[0]?.available_qty === 8, 'available_qty = 在庫数-引当数');
}
{
  // Shift_JIS デコード
  const csv = '商品ID,商品名,ロケ,在庫数\nabc2,テスト商品,B-01,5\n';
  const r = parseWarehouseCsv(iconv.encode(csv, 'Shift_JIS'));
  assert(!r.error && r.items?.[0]?.product_name === 'テスト商品', 'Shift_JIS CSVをデコードできる', r.error);
}
{
  // 必須ヘッダ欠落 (誤ファイル) → 全体エラーで中止
  const csv = 'SKU,数量\nx,1\n';
  const r = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  assert(!!r.error, '必須ヘッダ欠落はエラーで中止 (在庫全消え防止)');
}
{
  // 数値不正・負数・引当超過行はスキップ (少数なら全体は成功)
  const rows = ['商品ID,商品名,在庫数,引当数'];
  for (let i = 0; i < 50; i++) rows.push(`ok${i},商品${i},10,0`);
  rows.push('bad1,不正,abc,0');    // 在庫数が数値でない
  rows.push('bad2,負数,-5,0');     // 負数
  rows.push('bad3,超過,5,9');      // 引当>在庫
  rows.push(',ID空,10,0');         // 商品ID空
  const r = parseWarehouseCsv(Buffer.from(rows.join('\n'), 'utf8'));
  assert(!r.error && r.items?.length === 50, '不正行のみスキップし正常行は取込', r.error);
  assert(r.invalidRows?.length === 4, `不正行4件を検出 (実際: ${r.invalidRows?.length})`);
}
{
  // 不正行率 >10% → 全体中止
  const rows = ['商品ID,商品名,在庫数'];
  for (let i = 0; i < 5; i++) rows.push(`ok${i},商品,10`);
  for (let i = 0; i < 5; i++) rows.push(`bad${i},商品,xx`);
  const r = parseWarehouseCsv(Buffer.from(rows.join('\n'), 'utf8'));
  assert(!!r.error, '不正行率10%超は全体エラーで中止');
}
{
  // カンマ区切り数値 "1,234" を許容
  const csv = '商品ID,商品名,在庫数\nabc3,大量,"1,234"\n';
  const r = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  assert(!r.error && r.items?.[0]?.quantity === 1234, 'カンマ区切り数値を1234として取込', r.error);
}
{
  // JS Number構文 (指数・16進) は不正行扱い
  const rows = ['商品ID,商品名,在庫数'];
  for (let i = 0; i < 20; i++) rows.push(`ok${i},正常,10`);
  rows.push('bad1,指数,1e6');
  const r = parseWarehouseCsv(Buffer.from(rows.join('\n'), 'utf8'));
  assert(!r.error && r.items?.length === 20 && r.invalidRows?.length === 1, '指数表記1e6は不正行扱い', r.error);
}
{
  // 閉じない引用符 → 全体エラー
  const csv = '商品ID,商品名,在庫数\nabc4,"壊れた引用符,10\nabc5,次の行,20\n';
  const r = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  assert(!!r.error, '閉じない引用符は全体エラーで中止');
}
{
  // 有効期限の表記ゆれ正規化
  const csv = '商品ID,商品名,在庫数,有効期限\na1,商品,10,20270101\na2,商品,10,2027/1/1\na3,商品,10,2027-01-01\n';
  const r = parseWarehouseCsv(Buffer.from(csv, 'utf8'));
  const exps = (r.items || []).map(i => i.expiry_date);
  assert(exps.every(e => e === '2027-01-01'), `有効期限をYYYY-MM-DDに正規化 (実際${JSON.stringify(exps)})`);
}

// ===== P0-2/P0-3: 計算エンジン =====
console.log('--- P0-2/P0-3: セット品期限換算 + data_quality ---');
await db.initDb();

db.upsertSkuMappings([
  {
    amazon_sku: 'SET-A', product_name: '2+1セット', ne_code: 'comp1', logizard_code: 'comp1',
    is_set: true, set_components: [{ ne_code: 'comp1', qty: 2 }, { ne_code: 'comp2', qty: 1 }],
  },
  { amazon_sku: 'plain-b', product_name: '単品B', ne_code: 'plainb', logizard_code: 'plainb' },
  // set_components が不正JSON (文字列を渡すと '"...ではない..."' が保存され配列にならない)
  { amazon_sku: 'BROKEN-C', product_name: '構成不正', ne_code: 'brokenc', logizard_code: 'brokenc', set_components: 'this is not an array' },
  // 構成品qtyが負数 → 構造検証で推奨停止
  { amazon_sku: 'NEGQTY-D', product_name: 'qty不正', ne_code: 'comp1', logizard_code: 'comp1', is_set: true, set_components: [{ ne_code: 'comp1', qty: -2 }] },
]);

db.saveRestockLatest([
  { amazon_sku: 'SET-A', product_name: '2+1セット', fba_available: 0, units_sold_30d: 60, amazon_recommended_qty: null },
  { amazon_sku: 'plain-b', product_name: '単品B', fba_available: 0, units_sold_30d: 90, amazon_recommended_qty: null },
  { amazon_sku: 'BROKEN-C', product_name: '構成不正', fba_available: 0, units_sold_30d: 30, amazon_recommended_qty: null },
  { amazon_sku: 'NEGQTY-D', product_name: 'qty不正', fba_available: 0, units_sold_30d: 30, amazon_recommended_qty: null },
  // 未マッピング×実績あり (30日販売10) → active = 本物の納品漏れ候補 → 警告対象
  { amazon_sku: 'UNMAPPED-SOLD', product_name: '未マッピング(売れてる)', fba_available: 0, units_sold_30d: 10, amazon_recommended_qty: null },
  // 未マッピング×FBA在庫あり → active
  { amazon_sku: 'UNMAPPED-STOCK', product_name: '未マッピング(在庫あり)', fba_available: 5, units_sold_30d: 0, amazon_recommended_qty: null },
  // 未マッピング×入荷中のみ → active
  { amazon_sku: 'UNMAPPED-INBOUND', product_name: '未マッピング(入荷中)', fba_available: 0, fba_inbound_shipped: 3, units_sold_30d: 0, amazon_recommended_qty: null },
  // 未マッピング×Amazon推奨あり (生きている出品) → active
  { amazon_sku: 'UNMAPPED-RECO', product_name: '未マッピング(Amazon推奨)', fba_available: 0, units_sold_30d: 0, amazon_recommended_qty: 20 },
  // 未マッピング×他CHのみ販売 (FBA実績ゼロ) → active。Amazon投入漏れの検知経路
  { amazon_sku: 'UNMAPPED-NONFBA', product_name: '未マッピング(他CHで販売中)', fba_available: 0, units_sold_30d: 0, amazon_recommended_qty: null },
  // 未マッピング×準備中がAPI側にだけある (レポートWorking=0) → active。RESTOCKだけ見ると取りこぼす
  { amazon_sku: 'UNMAPPED-APIWORK', product_name: '未マッピング(API準備中)', fba_available: 0, fba_inbound_working: 0, units_sold_30d: 0, amazon_recommended_qty: null },
  // 未マッピング×実績ゼロ → inactive = バリエーション登録専用等。警告にせず静かに件数表示
  { amazon_sku: 'UNMAPPED-VARIATION', product_name: '未マッピング(未使用)', fba_available: 0, units_sold_30d: 0, amazon_recommended_qty: null },
]);

// 他CH売上スナップショット (sku_mapping とは別テーブル → 未マッピングSKUでも実績を引ける)
db.saveNonFbaSalesSnapshot([
  { amazon_sku: 'UNMAPPED-NONFBA', non_fba_sales_7d: 5, non_fba_sales_30d: 25 },
]);

db.replaceWarehouseInventory([
  // comp1: 期限なし100個 (2個/セット → 50セット分)
  { logizard_code: 'comp1', product_name: '構成品1', location: 'A-01', quantity: 100, reserved: 0, available_qty: 100, expiry_date: '', block_alloc_order: 1 },
  // comp2: 期限違いで10個ずつ (1個/セット)。同一期限で送れるのは10セットのみ
  { logizard_code: 'comp2', product_name: '構成品2', location: 'B-01', quantity: 10, reserved: 0, available_qty: 10, expiry_date: '2027-01-01', block_alloc_order: 1 },
  { logizard_code: 'comp2', product_name: '構成品2', location: 'B-02', quantity: 10, reserved: 0, available_qty: 10, expiry_date: '2027-06-01', block_alloc_order: 2 },
  // plainb: 期限なし30個
  { logizard_code: 'plainb', product_name: '単品B', location: 'C-01', quantity: 30, reserved: 0, available_qty: 30, expiry_date: '', block_alloc_order: 1 },
]);

const { generateRecommendations } = await import('./calculation-engine.js');
// inboundWorkingOverride = Inbound API のリアルタイム準備中。B-4検証のためキーの case をあえて崩す:
//  - 'unmapped-apiwork' : 未マッピング判定側の case 差
//  - 'PLAIN-B' / 'plain-b' : 本体推奨計算の case 差 + 正規化衝突 (最大値=12が採用されるべき)
const result = generateRecommendations(false, { 'unmapped-apiwork': 7, 'PLAIN-B': 12, 'plain-b': 0 });

assert(Array.isArray(result.items) && result.items.length === 4,
  `不正set_componentsがあっても全SKU生成が止まらない (items=${result.items?.length})`);

// 構成不正SKUは推奨停止 (単品扱いの誤数量を出さない)
for (const sku of ['BROKEN-C', 'NEGQTY-D']) {
  const item = result.items.find(i => i.amazon_sku === sku);
  assert(item?.invalid_mapping === true && item?.recommended_qty === 0,
    `${sku}: 構成不正は推奨停止 (invalid_mapping=${item?.invalid_mapping}, rec=${item?.recommended_qty})`);
  assert((item?.alerts || []).some(a => a.type === 'invalid_mapping'), `${sku}: 推奨停止アラートが付く`);
}

const setA = result.items.find(i => i.amazon_sku === 'SET-A');
assert(!!setA, 'SET-A が生成される');
if (setA) {
  // 従来バグ: 期限は先頭構成品(comp1)しか見ず、comp2の期限もセット数換算もされなかった
  assert(setA.warehouse_available === 20, `セット可能数=ボトルネック構成品基準 (期待20, 実際${setA.warehouse_available})`);
  assert(setA.expiry_limited === true, 'セット2番目構成品の期限制限を検知 (従来は見落とし)');
  assert(setA.recommended_qty === 10, `期限制限がセット数換算される (期待10, 実際${setA.recommended_qty})`);
  assert(setA.expiry_date === '2027-01-01', `代表期限=引当順先頭の期限 (実際${setA.expiry_date})`);
  assert(setA.is_expiry_managed === true, '期限管理判定が全構成品を見る');
}

const plainB = result.items.find(i => i.amazon_sku === 'plain-b');
// B-4: 本体の推奨計算でも override を case 非依存で参照し、正規化衝突は最大値を採用 (過小評価=二重推奨を防ぐ)
assert(plainB?.fba_inbound_working === 12 && plainB?.fba_inbound_working_source === 'API',
  `本体計算もoverrideをcase非依存+衝突は最大値で参照 (期待12/API, 実際${plainB?.fba_inbound_working}/${plainB?.fba_inbound_working_source})`);
assert(plainB?.effective_fba_stock === 12, `準備中が実質FBA在庫に乗る (期待12, 実際${plainB?.effective_fba_stock})`);

for (const i of result.items) {
  if (typeof i.adjusted_qty === 'number' && typeof i.warehouse_available === 'number') {
    assert(i.adjusted_qty <= i.warehouse_available, `${i.amazon_sku}: 補正後数量が倉庫可能数を超えない (${i.adjusted_qty} ≤ ${i.warehouse_available})`);
  }
}

const dq = result.data_quality;
// 未マッピングは稼働実績で仕分け: active=要対応 (警告バナー) / inactive=未使用SKU (静かな情報表示)
const activeSkus = (dq?.unmapped_active || []).map(u => u.sku).sort();
const expectedActive = ['UNMAPPED-APIWORK', 'UNMAPPED-INBOUND', 'UNMAPPED-NONFBA', 'UNMAPPED-RECO', 'UNMAPPED-SOLD', 'UNMAPPED-STOCK'];
assert(dq?.unmapped_active_count === 6 && JSON.stringify(activeSkus) === JSON.stringify(expectedActive),
  `未マッピング×実績あり(販売/在庫/入荷中/Amazon推奨/他CH)のみactive (実際${JSON.stringify(activeSkus)})`);
assert(dq?.unmapped_inactive_count === 1 && dq?.unmapped_inactive_skus?.[0] === 'UNMAPPED-VARIATION',
  `未マッピング×実績ゼロはinactive=警告にしない (実際${JSON.stringify(dq?.unmapped_inactive_skus)})`);
assert(dq?.unmapped_active?.[0]?.units_sold_30d !== undefined,
  'active側は判断材料 (30日販売/FBA在庫/入荷中) を持つ');
// B-4: override は case 差があっても効く (レポートWorking=0でもAPI側7個を拾ってactive)
const apiWork = dq?.unmapped_active?.find(u => u.sku === 'UNMAPPED-APIWORK');
assert(apiWork?.fba_inbound === 7, `準備中はAPI値をcase非依存で参照 (期待7, 実際${apiWork?.fba_inbound})`);
// 他CH販売のみのSKUも実績として拾う (Amazon投入漏れの検知経路)
const nonFba = dq?.unmapped_active?.find(u => u.sku === 'UNMAPPED-NONFBA');
assert(nonFba?.non_fba_sales_30d === 25, `他CH30日販売をactive判定に使う (期待25, 実際${nonFba?.non_fba_sales_30d})`);
assert(dq?.invalid_mapping_count === 2 && dq?.invalid_mappings?.some(m => m.sku === 'BROKEN-C') && dq?.invalid_mappings?.some(m => m.sku === 'NEGQTY-D'),
  `set_components不正がmetaで返る (実際${JSON.stringify(dq?.invalid_mappings)})`);
assert(dq?.planning_missing_count === 11, `PLANNING欠落件数 (期待11=restock全行, 実際${dq?.planning_missing_count})`);

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail > 0 ? 1 : 0;
