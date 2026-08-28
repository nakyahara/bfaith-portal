/**
 * test-views.mjs — 画面テンプレート (EJS) の中の JavaScript が壊れていないか
 *
 * 🚨 EJS を render できても、その中の <script> が構文エラーなら画面の操作は**全部**死ぬ。
 *    render テストだけでは素通りするので、ここで実際に構文解析する。
 *    実際に踏んだ事故: 文字列リテラルの中に実改行が入り (`split('` + 改行 + `')`)、
 *    端末登録・出力先の保存・失効ボタンがすべて動かなくなった (2026-08-28)。
 *    heredoc/sed 経由で `\n` を書くと実文字に化ける → Write ツールか node で書くこと。
 *
 * 実行: node apps/packing/tests/test-views.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'views');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

console.log('── EJS の <script> が JavaScript として成立するか ──');
for (const file of fs.readdirSync(dir).filter((f) => f.endsWith('.ejs'))) {
  const src = fs.readFileSync(path.join(dir, file), 'utf8');

  // 開始タグと終了タグの数が合わない = JSコメント内に終了タグを書いた等で早期に閉じている
  const opens = (src.match(/<script(?![a-zA-Z])/g) || []).length;
  const closes = (src.match(/<\/script\s*>/g) || []).length;
  ok(opens === closes, `${file}: script タグの開閉が一致 (${opens}/${closes})`);

  for (const [i, m] of [...src.matchAll(/<script(?![a-zA-Z])[^>]*>([\s\S]*?)<\/script\s*>/g)].entries()) {
    // EJS の埋め込みは値が入る前提なので、構文解析できる形のダミーに置き換える
    const js = m[1]
      .replace(/<%[-=]?([\s\S]*?)%>/g, '0')
      .replace(/^\s*\/\/.*$/gm, '');
    if (!js.trim()) continue;
    let err = null;
    try { new vm.Script(js, { filename: `${file}#script${i + 1}` }); } catch (e) { err = e; }
    ok(!err, `${file}: script${i + 1} が構文エラーでない${err ? ` — ${err.message}` : ''}`);
  }
}

console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
