/**
 * Render → miniPC クライアント (remote-sources.js) の応答検証のテスト。
 * 通信はしない。純関数 (parseProductsResponse / parseRmsItemResponse) だけを叩く。
 *
 * ここで守りたいこと: miniPC からの応答が欠けている・形が違うときに、
 * 黙って空の素材で展開へ進まないこと (2026-08-08 の「Renderで黙って劣化」事故の再発防止)。
 */
import path from 'path';
import { pathToFileURL } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );
const { parseProductsResponse, parseRmsItemResponse, remoteConfigured } =
  await import(pathToFileURL(path.join(repo, 'apps/select-set/remote-sources.js')));

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};
const mustThrow = (fn, n) => {
  try { fn(); ok(false, n); } catch { ok(true, n); }
};

console.log('=== 接続設定の判定 ===');
delete process.env.CF_ACCESS_CLIENT_ID;
delete process.env.CF_ACCESS_CLIENT_SECRET;
delete process.env.WAREHOUSE_SERVICE_TOKEN;
ok(!remoteConfigured(), '3つの env が無ければ未設定と判定');
process.env.CF_ACCESS_CLIENT_ID = 'a';
process.env.CF_ACCESS_CLIENT_SECRET = 'b';
ok(!remoteConfigured(), '一部だけでは未設定と判定');
process.env.WAREHOUSE_SERVICE_TOKEN = 'c';
ok(remoteConfigured(), '3つ揃えば設定済みと判定');

console.log('\n=== 商品マスタ応答の検証 ===');
const goodProducts = {
  ok: true,
  count: 2,
  products: [
    { code: 'ae-rose10', name: 'ローズ', available: 5, status: '取扱中' },
    { code: 'sabon10', name: 'サボン', available: 0, status: '取扱中' },
  ],
};
const parsed = parseProductsResponse(goodProducts);
ok(parsed.length === 2 && parsed[0].code === 'ae-rose10' && parsed[0].available === 5, '正常な応答を整形できる');
ok(parseProductsResponse({ ok: true, products: [{ code: 'x', available: 'abc' }] })[0].available === 0,
  '在庫が数値でなければ 0 として扱う (欠損で例外にはしない)');
mustThrow(() => parseProductsResponse(null), '中身が無ければ投げる');
mustThrow(() => parseProductsResponse({ ok: false, products: [] }), 'ok=false は投げる');
mustThrow(() => parseProductsResponse({ ok: true, products: [] }), '🚨 商品0件は投げる (空の素材で展開に進まない)');
mustThrow(() => parseProductsResponse({ ok: true, products: [{ name: 'コード無し' }] }), '有効な行が無ければ投げる');
mustThrow(() => parseProductsResponse({ ok: true, products: 'html...' }), '配列でなければ投げる');

console.log('\n=== RMS応答の検証 ===');
const opts = [{ displayName: '1本目', selections: [{ displayValue: 'ローズ_ae-rose10' }] }];
ok(parseRmsItemResponse({ manageNumber: 'selectae10-5', customizationOptions: opts }).length === 1,
  'customizationOptions を取り出せる');
ok(parseRmsItemResponse({ manageNumber: 'x', title: 'セットでない商品' }).length === 0,
  '選択肢の無い商品は空配列 (エラーではない)');
mustThrow(() => parseRmsItemResponse(null), '中身が無ければ投げる');
mustThrow(() => parseRmsItemResponse({ error: 'RMS_API_ERROR', message: 'ng' }),
  '🚨 エラー応答を「選択肢なし」と誤解しない');
mustThrow(() => parseRmsItemResponse('<html>...</html>'), '文字列 (HTML等) は投げる');

console.log(`\n合計: ${pass} pass / ${fail} NG`);
process.exit(fail === 0 ? 0 : 1);
