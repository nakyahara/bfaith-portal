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

console.log('\n── 1 SKU の解決: AM → 空欄なら W (中原さん 2026-09-09。AL は使わない) ──');
{
  eq(resolveSku(sku('BK', 360), productMap), { ne_code: '0726-001802-BK', resolution: 'am' }, 'AM で当たる (正本表記で返す)');
  eq(resolveSku({ itemNumber: '0726-001588', skuManageNumber: 'normal-inventory', systemSkuNumber: '' }, productMap),
    { ne_code: '0726-001588', resolution: 'w' }, 'AM が空欄なら W (商品番号) で当たる');
  ok(INVALID_AL.has('normal-inventory'), 'normal-inventory は無意味な AL');
  eq(resolveSku({ itemNumber: 'unknown', skuManageNumber: '999', systemSkuNumber: 'x' }, productMap),
    { ne_code: null, reason: 'am_unmatched' }, 'AM が NE に無ければ未解決 (理由つき)');
  eq(resolveSku({ itemNumber: 'unknown', skuManageNumber: '999', systemSkuNumber: '' }, productMap),
    { ne_code: null, reason: 'w_unmatched' }, 'AM も W も当たらなければ未解決 (理由つき)');
}

console.log('\n── 🚨 AL (SKU管理番号) で商品を決めない (2026-09-09 の実害: treemuddler200) ──');
{
  // 実物の形: 商品ページ treemuddler200 / 商品番号 treemuddler100-2 / AL は商品管理番号と同じ文字列。
  // NE 側には **別商品** の treemuddler200 (原価 330) が実在していて、そちらの原価が使われていた
  const pm = new Map([['treemuddler100-2', 'treemuddler100-2'], ['treemuddler200', 'treemuddler200']]);
  const target = {
    manageNumber: 'treemuddler200', itemNumber: 'treemuddler100-2',
    skuManageNumber: 'treemuddler200', systemSkuNumber: '',
  };

  eq(resolveSku(target, pm), { ne_code: 'treemuddler100-2', resolution: 'w' },
    '🚨 AM が空欄なら商品番号で紐づく (AL が別商品と同名でも、そちらへ行かない)');

  const { mappings } = buildMappings([target], pm);
  eq(mappings.get('treemuddler200')?.ne_code, 'treemuddler100-2',
    '🚨 AL からも引けるが、指す先は商品番号で決めた NE 品番');
  eq(mappings.get('treemuddler100-2')?.ne_code, 'treemuddler100-2', '商品番号からも同じ NE 品番');
}

console.log('\n── AM が入っているのに当たらないとき、W へ落とさない ──');
{
  // 🚨 ルールの条件は「AM が空欄なら」。「当たらなければ」で落とすと、また別商品を拾う
  const pm = new Map([['w-code', 'w-code']]);
  eq(resolveSku({ itemNumber: 'w-code', skuManageNumber: 'x', systemSkuNumber: 'am-not-in-ne' }, pm),
    { ne_code: null, reason: 'am_unmatched' }, 'AM 不一致は未解決 (W で拾い直さない)');
  // 同じ SKU で AM を空にすると W で当たる = 分岐が本当に「AM の有無」で決まっている
  eq(resolveSku({ itemNumber: 'w-code', skuManageNumber: 'x', systemSkuNumber: '' }, pm),
    { ne_code: 'w-code', resolution: 'w' }, 'AM を空にすれば W で当たる');
}

console.log('\n── 🚨 AM 有りと AM 空欄が同じページに混ざっても、商品番号の行が別 SKU を指さない (Codex P1) ──');
{
  // 商品ページ page-w に 2 SKU: 片方は AM で別商品に紐づく / 片方は AM 空欄で商品番号に紐づく。
  // W (商品番号) の行は 1 つしか作れないので、AM 側の答えが入ると
  // AM 空欄の出品が **別 SKU の原価**を引く。しかも同順位は先勝ちなので取得順で変わる
  const pm = new Map([['am-red', 'am-red'], ['page-w', 'page-w']]);
  const withAm = { manageNumber: 'page', itemNumber: 'page-w', skuManageNumber: '001', systemSkuNumber: 'am-red' };
  const noAm   = { manageNumber: 'page', itemNumber: 'page-w', skuManageNumber: '002', systemSkuNumber: '' };

  for (const [label, skus] of [['AM 有りが先', [withAm, noAm]], ['AM 空欄が先', [noAm, withAm]]]) {
    const { mappings } = buildMappings(skus, pm);
    eq(mappings.get('page-w')?.ne_code, 'page-w', `🚨 ${label}: 商品番号の行は商品番号で決めた方 (取得順で変わらない)`);
  }

  // どの SKU も商品番号で決めていないページでは、これまでどおり 1 行入る
  // (楽天の注文明細は商品番号で突き合わせるので、行ごと消してはいけない)
  const { mappings: onlyAm } = buildMappings([withAm], pm);
  eq(onlyAm.get('page-w')?.ne_code, 'am-red', '商品番号で決めた SKU が無ければ、これまでどおり 1 行入る');
}

console.log('\n── 当たらなかった理由を数える (ルールを厳しくした影響が見えるように) ──');
{
  const pm = new Map([['ok-am', 'ok-am']]);
  const { resolvedCount, unresolvedCount, byResolution, unresolvedByReason } = buildMappings([
    { manageNumber: 'p1', itemNumber: 'w1', skuManageNumber: 'a1', systemSkuNumber: 'ok-am' },
    { manageNumber: 'p2', itemNumber: 'w2', skuManageNumber: 'a2', systemSkuNumber: 'no-such-am' },
    { manageNumber: 'p3', itemNumber: 'w3', skuManageNumber: 'a3', systemSkuNumber: '' },
  ], pm);
  eq([resolvedCount, unresolvedCount], [1, 2], '1 件解決・2 件未解決');
  eq(byResolution, { am: 1 }, 'どのコードで決めたかの内訳');
  eq(unresolvedByReason, { am_unmatched: 1, w_unmatched: 1 }, '未解決の理由の内訳');
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

console.log('\n── 索引の優先順: 同じコードから引けるものが複数あれば AM > AL > W ──');
{
  // 変な例だが起こりうる: ある SKU の AL が、別 SKU の AM と同じ文字列
  const skus = [
    { itemNumber: 'nursewatch-pk', manageNumber: 'nursewatch', skuManageNumber: 'nursewatch-pk', systemSkuNumber: '' },
    { itemNumber: 'nursewatch', manageNumber: 'nursewatch', skuManageNumber: '386', systemSkuNumber: 'nursewatch-pk' },
  ];
  const { mappings } = buildMappings(skus, productMap);
  eq(mappings.get('nursewatch-pk')?.source, 'am', 'AM の行が AL の行に勝つ (索引としての優先順)');
}

console.log('\n── manageNumber が無い応答 (古いキャッシュ等) でも落ちない ──');
{
  const { mappings, withoutManageNumber } = buildMappings([{ ...sku('BK', 360), manageNumber: undefined }], productMap);
  eq(mappings.get('360')?.manage_number, null, 'manage_number は null (空文字にしない)');
  eq(withoutManageNumber, 1, '無かった件数を数える (ログで気づけるように)');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
