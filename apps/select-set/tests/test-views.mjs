/**
 * views/*.ejs に埋め込んだクライアントJSの構文チェック。
 * (product-hub #701 の再発防止と同じ仕掛け: 生改行の混入などで画面のボタンが全滅するのを
 *  コミット前に検知する。vm.Script はコンパイルのみで実行はしない)
 */
import fs from 'fs';
import path from 'path';
import vm from 'vm';
import { fileURLToPath } from 'url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const views = path.join(dir, '..', 'views');

let pass = 0;
let fail = 0;
for (const f of fs.readdirSync(views).filter((n) => n.endsWith('.ejs'))) {
  const src = fs.readFileSync(path.join(views, f), 'utf8');
  const blocks = [...src.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
  if (!blocks.length) continue;
  const js = blocks.join('\n').replace(/<%[-=]?[\s\S]*?%>/g, '0');
  try {
    new vm.Script(js, { filename: f });
    pass++;
    console.log(`  ok - client-js syntax ${f}`);
  } catch (e) {
    fail++;
    console.log(`  NG - client-js syntax ${f}: ${e.message}`);
  }
}
console.log(`\n合計: ${pass} pass / ${fail} NG`);
process.exit(fail === 0 ? 0 : 1);
