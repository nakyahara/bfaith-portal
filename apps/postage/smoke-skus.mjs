/**
 * 商品マスタ (検索・状態・その場判定) のテスト。
 *   node apps/postage/smoke-skus.mjs
 *
 * 一時ディレクトリで動かすので本番には触れない。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'postage-skus-'));
process.env.DATA_DIR = TMP;
process.env.POSTAGE_WAREHOUSE_DB = path.join(TMP, 'nonexistent.db');   // 出荷実績は使わない

let pass = 0, fail = 0;
const t = (name, fn) => {
  try { fn(); pass++; console.log(`  ok   ${name}`); }
  catch (e) { fail++; console.log(`  FAIL ${name}\n       ${e.message}`); }
};
const eq = (a, b, what) => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what || ''} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
};

const { initPostageDB, getDB, closePostageDB } = await import('./db.js');
const { searchSkus, countByStatus, previewOne, skuStatus } = await import('./skus.js');
const { buildContext } = await import('./coverage.js');
initPostageDB();
const db = getDB();

// 茶封筒だけ「実測済み」にする (未実測では何も確定しないため)
db.prepare('UPDATE pm_materials SET dims_verified=1 WHERE material_code=?').run('chabuto');

const put = (sku, name, w, th, mat) => db.prepare(`
  INSERT INTO pm_skus (sku_code, display_name, unit_weight_g, thickness_mm, default_material_code, updated_at)
  VALUES (?,?,?,?,?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run(sku, name, w, th, mat);

put('a-ready', '揃っている商品_長3封', 15, 1, 'chabuto');
put('b-noweight', '重さ待ち_長3封', null, 1, 'chabuto');
put('c-nothick', '厚み待ち_長3封', 15, null, 'chabuto');
put('d-nomat', '資材未定の商品', 15, 1, null);
put('e-dims', '外寸未測定の資材_梱機プ', 30, 20, 'shiropuchi');
put('f_under', 'アンダースコアを含む_長3封', 15, 1, 'chabuto');
put('g-allnull', '何も入っていない商品', null, null, null);

console.log('■ 状態の判定');
t('全部揃っていれば ready', () => eq(skuStatus({ default_material_code: 'chabuto', unit_weight_g: 1, thickness_mm: 1 }), 'ready'));
t('資材が無ければ no_material (重さ厚みが揃っていても)', () =>
  eq(skuStatus({ default_material_code: null, unit_weight_g: 1, thickness_mm: 1 }), 'no_material'));
t('重さが無ければ no_weight', () =>
  eq(skuStatus({ default_material_code: 'chabuto', unit_weight_g: null, thickness_mm: 1 }), 'no_weight'));
t('厚みが無ければ no_thickness', () =>
  eq(skuStatus({ default_material_code: 'chabuto', unit_weight_g: 1, thickness_mm: null }), 'no_thickness'));

console.log('\n■ 件数');
t('状態ごとの件数が重複せず合計と合う', () => {
  const c = countByStatus();
  eq(c.total, 7);
  eq(c.no_material + c.no_weight + c.no_thickness + c.ready, c.total, '足すと総数');
  eq(c.no_material, 2); eq(c.ready, 3);
  eq(c.incomplete, c.total - c.ready);
});

console.log('\n■ 検索');
t('タブの件数と一覧の件数が必ず一致する (全部空の商品も含めて)', () => {
  const c = countByStatus();
  for (const k of ['no_material', 'no_weight', 'no_thickness', 'ready', 'incomplete']) {
    eq(searchSkus({ filter: k }).total, c[k], `${k} のタブ件数と一覧件数`);
  }
  eq(searchSkus({ filter: 'all' }).total, c.total);
});
t('全部空の商品は「資材が未定」に1回だけ出る (重複して数えない)', () => {
  eq(searchSkus({ filter: 'no_material' }).rows.some((r) => r.sku_code === 'g-allnull'), true);
  eq(searchSkus({ filter: 'no_weight' }).rows.some((r) => r.sku_code === 'g-allnull'), false);
});
t('未完了だけに絞れる', () => {
  const r = searchSkus({ filter: 'incomplete' });
  eq(r.total, 4);
  eq(r.rows.some((x) => x.sku_code === 'a-ready'), false);
});
t('揃っているものだけに絞れる', () => {
  // 商品側が揃っている、という意味。資材の外寸が未測定 (e-dims) はここに入る。
  // その商品が実際に確定できるかは右端のプレビューが答える
  eq(searchSkus({ filter: 'ready' }).total, 3);
});
t('商品側が揃っていても資材が未実測なら確定はしない (状態とプレビューは別物)', () => {
  eq(skuStatus(searchSkus({ q: 'e-dims' }).rows[0]), 'ready');
  eq(previewOne('e-dims', buildContext()).ok, false);
});
t('商品コードの部分一致', () => {
  eq(searchSkus({ q: 'noweight' }).rows.map((r) => r.sku_code), ['b-noweight']);
});
t('商品名でも探せる', () => {
  eq(searchSkus({ q: '厚み待ち' }).rows.map((r) => r.sku_code), ['c-nothick']);
});
t('アンダースコアを含む検索が全件に当たらない (LIKE のワイルドカードを打ち消す)', () => {
  const r = searchSkus({ q: 'f_under' });
  eq(r.total, 1, 'f_under だけに当たる');
});
t('% を入れても全件に当たらない', () => {
  eq(searchSkus({ q: '%' }).total, 0);
});
t('未完了が先、揃っているものが後に並ぶ', () => {
  const r = searchSkus({ filter: 'all' });
  eq(r.rows[0].sku_code, 'd-nomat', '資材未定が先頭');
  eq(r.rows[r.rows.length - 1].default_material_code !== null, true);
});
t('件数の上限を超える指定は丸められる', () => {
  eq(searchSkus({ limit: 9999 }).limit, 200);
  eq(searchSkus({ limit: -5 }).limit, 50, '負数は既定に戻す');
  eq(searchSkus({ limit: 0 }).limit, 50, '0 も既定に戻す');
});

console.log('\n■ その場の判定プレビュー');
const ctx = buildContext();
t('揃っていれば料金が出る (15 + 5 + 0.5 = 20.5g・厚さ1mm → 定形110円)', () => {
  const p = previewOne('a-ready', ctx);
  eq(p.ok, true);
  eq(/定形 50g以内/.test(p.text) && /110円/.test(p.text), true, p.text);
  eq(/20.5g/.test(p.sub), true, p.sub);
});
t('重さが無ければ理由つきで不明', () => {
  const p = previewOne('b-noweight', ctx);
  eq(p.ok, false); eq(/重さ未登録/.test(p.text), true, p.text);
});
t('資材が未定なら不明', () => {
  const p = previewOne('d-nomat', ctx);
  eq(p.ok, false); eq(/資材が決まらない/.test(p.text), true, p.text);
});
t('資材の外寸が未測定なら不明 (重さが揃っていても)', () => {
  const p = previewOne('e-dims', ctx);
  eq(p.ok, false); eq(/外寸が未測定/.test(p.text), true, p.text);
});
t('マスタに無い商品コードでも落ちない', () => {
  const p = previewOne('zzz-none', ctx);
  eq(p.ok, false); eq(/マスタに無い/.test(p.text), true, p.text);
});

closePostageDB();
try { fs.rmSync(TMP, { recursive: true, force: true }); }
catch { console.log(`  (一時ディレクトリを消せませんでした: ${TMP})`); }
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
