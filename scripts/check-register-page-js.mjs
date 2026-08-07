/**
 * check-register-page-js.mjs — register 画面に埋め込まれたクライアント JS の構文チェック
 *
 * apps/warehouse/router.js は画面 HTML をテンプレートリテラルで組み立てるため、
 * `node --check` では <script> の中身 (= ただの文字列) が検査されない。
 * 壊れた JS を入れると画面のボタンが全滅するが CI もサーバ起動も素通りする。
 * (product-hub #701 で同種の事故があったため、warehouse 側にも同じ網を張る)
 *
 * 実行: node scripts/check-register-page-js.mjs
 */
import fs from 'node:fs';
import vm from 'node:vm';

const SRC = 'apps/warehouse/router.js';
const src = fs.readFileSync(SRC, 'utf8');
const blocks = [...src.matchAll(/<script>([\s\S]*?)<\/script>/g)];

if (blocks.length === 0) {
  console.error(`❌ ${SRC} に <script> ブロックが見つかりません (抽出パターンが古い?)`);
  process.exit(1);
}

let bad = 0;
blocks.forEach((m, i) => {
  // テンプレートリテラル内のエスケープを、実際にブラウザへ出る形へ戻す
  let code = m[1]
    .replace(/\\`/g, '`')
    .replace(/\\\$/g, '$')
    .replace(/\\\\/g, '\\');
  // サーバ側の ${...} 埋め込みは実行時に値が入るのでダミーに置換
  code = code.replace(/\$\{[^{}]*\}/g, '0');
  try {
    new vm.Script(code);
    console.log(`✅ script#${i} 構文OK (${code.length}文字)`);
  } catch (e) {
    bad++;
    console.log(`❌ script#${i} ${e.message}`);
  }
});

console.log(bad === 0 ? '\n✅ 画面JS 全ブロック構文OK' : `\n❌ ${bad}ブロックNG`);
process.exit(bad === 0 ? 0 : 1);
