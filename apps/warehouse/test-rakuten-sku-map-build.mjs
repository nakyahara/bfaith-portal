/**
 * test-rakuten-sku-map-build.mjs — 楽天 SKU マップの組み立ての検証
 *
 * ここが緩むと「色違いの商品ページにたどり着けない」(価格一括改定 2026-09-01 発覚) が再発する。
 * 楽天には接続せず、all-skus の応答を模した配列で組み立てだけを試す。
 *
 * 実行: node apps/warehouse/test-rakuten-sku-map-build.mjs
 */
import { buildMappings, resolveSku, INVALID_AL } from './rakuten-sku-map-build.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const productMap = new Map();
for (const c of ['0726-001802-BK', '0726-001802-BE', '0726-001802-GR', '0726-001588', 'nursewatch-pk']) {
  productMap.set(c.toLowerCase(), c);
}

// 実物と同じ形 (all-skus): 12 色が同じ itemNumber / manageNumber を共有し、AL は連番、AM は色つきコード
const sku = (color, al) => ({
  itemNumber: '0726-001802', manageNumber: '0726-001802',
  skuManageNumber: String(al), systemSkuNumber: `0726-001802-${color}`,
});

console.log('\n── 1 SKU の解決 (AM → AL → W) ──');
{
  eq(resolveSku(sku('BK', 360), productMap), { ne_code: '0726-001802-BK', resolution: 'am' }, 'AM で当たる (正本表記で返す)');
  eq(resolveSku({ itemNumber: '0726-001588', skuManageNumber: 'normal-inventory', systemSkuNumber: '' }, productMap),
    { ne_code: '0726-001588', resolution: 'w' }, '無意味な AL は飛ばして W で当たる');
  ok(INVALID_AL.has('normal-inventory'), 'normal-inventory は無意味な AL');
  eq(resolveSku({ itemNumber: 'unknown', skuManageNumber: '999', systemSkuNumber: 'x' }, productMap), null, 'どれも当たらなければ null');
}

console.log('\n── ★カラバリ: W 行を持てない色にも manage_number が入る ──');
{
  const skus = [sku('BK', 360), sku('BE', 366), sku('GR', 368)];
  const { mappings, resolvedCount, unresolvedCount, withoutManageNumber } = buildMappings(skus, productMap);
  eq([resolvedCount, unresolvedCount, withoutManageNumber], [3, 0, 0], '3 SKU とも解決');

  // W 行は 1 商品に 1 つ (主キー)。先に来た色のもの
  const wRows = [...mappings].filter(([, v]) => v.source === 'w');
  eq(wRows.length, 1, 'W (商品番号) の行は 1 つだけ');
  eq(wRows[0][1].ne_code, '0726-001802-BK', 'W 行は最初の色に付く');

  // ★BE / GR は W 行を持たないが、AM / AL の行に manage_number がある
  for (const color of ['be', 'gr']) {
    const am = mappings.get(`0726-001802-${color}`);
    eq([am?.source, am?.manage_number], ['am', '0726-001802'], `★${color.toUpperCase()} の AM 行に商品管理番号がある`);
  }
  eq(mappings.get('366')?.manage_number, '0726-001802', '★BE の AL 行 (366) にも商品管理番号がある');
  eq(mappings.get('366')?.ne_code, '0726-001802-BE', 'AL 366 は BE に紐づく');
  eq(mappings.get('0726-001802')?.manage_number, '0726-001802', 'W 行にも入る');

  // 全行に manage_number がある = どの行から引いても商品ページに届く
  ok([...mappings.values()].every((v) => v.manage_number === '0726-001802'), '★全行が同じ商品管理番号を指す');
}

console.log('\n── 優先順: 同じコードに複数 SKU が当たったら AM > AL > W ──');
{
  // 変な例だが起こりうる: ある SKU の AL が、別 SKU の AM と同じ文字列
  const skus = [
    { itemNumber: 'nursewatch', manageNumber: 'nursewatch', skuManageNumber: 'nursewatch-pk', systemSkuNumber: '' },
    { itemNumber: 'nursewatch', manageNumber: 'nursewatch', skuManageNumber: '386', systemSkuNumber: 'nursewatch-pk' },
  ];
  const { mappings } = buildMappings(skus, productMap);
  eq(mappings.get('nursewatch-pk')?.source, 'am', 'AM が AL に勝つ');
}

console.log('\n── manageNumber が無い応答 (古いキャッシュ等) でも落ちない ──');
{
  const { mappings, withoutManageNumber } = buildMappings([{ ...sku('BK', 360), manageNumber: undefined }], productMap);
  eq(mappings.get('360')?.manage_number, null, 'manage_number は null (空文字にしない)');
  eq(withoutManageNumber, 1, '無かった件数を数える (ログで気づけるように)');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
