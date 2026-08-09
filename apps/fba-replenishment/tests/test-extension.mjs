/**
 * Chrome拡張 (tools/fba-fukutsu-helper) の静的チェック。
 * ブラウザ無しで回せる範囲だけ: JSの構文、manifest の整合性、配布zipに入れてはいけないもの。
 * 拡張を読み込んで初めて分かる不備は実機確認で見る。
 *   node apps/fba-replenishment/tests/test-extension.mjs
 */
import fs from 'fs';
import path from 'path';
import vm from 'vm';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );
const dir = path.join(repo, 'tools', 'fba-fukutsu-helper');

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};

// --- 構文 ---
for (const f of ['content.js', 'fukutsu-csv.js', 'page-parse.js']) {
  try {
    new vm.Script(fs.readFileSync(path.join(dir, f), 'utf8'), { filename: f });
    ok(true, `構文 ${f}`);
  } catch (e) {
    ok(false, `構文 ${f}: ${e.message}`);
  }
}

// --- manifest ---
const mf = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'));
ok(mf.manifest_version === 3, 'manifest_version は 3');
ok(/^\d+\.\d+\.\d+$/.test(mf.version || ''), `version が x.y.z 形式 (${mf.version})`);
ok((mf.description || '').length > 0 && mf.description.length <= 132,
  `description は132文字以内 (${(mf.description || '').length}文字)`);

// ⭐この拡張は通信しない。権限が増えていたら設計が崩れているサイン
ok(!mf.permissions || mf.permissions.length === 0, '⭐permissions は空 (通信も保存もしない)');
ok(!mf.host_permissions || mf.host_permissions.length === 0, '⭐host_permissions は空');
ok(!mf.background, 'background (Service Worker) を持たない');

const matches = mf.content_scripts?.[0]?.matches ?? [];
ok(matches.some((m) => m.includes('sellercentral.amazon.co.jp')), 'Seller Central に差し込む');
ok(matches.every((m) => /sellercentral/.test(m)), '対象を Seller Central 以外へ広げていない');

const js = mf.content_scripts?.[0]?.js ?? [];
ok(js.indexOf('fukutsu-csv.js') < js.indexOf('content.js') && js.indexOf('page-parse.js') < js.indexOf('content.js'),
  'content.js より先に依存ファイルを読み込む');
for (const f of js.concat(mf.content_scripts?.[0]?.css ?? [])) {
  ok(fs.existsSync(path.join(dir, f)), `参照ファイルが存在する: ${f}`);
}

// --- アイコン (ウェブストア提出に必須) ---
ok(mf.icons && mf.icons['128'], 'icons に128pxがある (ストア提出に必須)');
for (const [size, rel] of Object.entries(mf.icons || {})) {
  const p = path.join(dir, rel);
  ok(fs.existsSync(p) && fs.statSync(p).size > 0, `アイコン実体がある: ${rel} (${size}px)`);
}

// --- 配布zipに入れてはいけないもの ---
// router.js の除外リストと、実際に置いてある開発用ファイルが食い違わないようにする
const routerSrc = fs.readFileSync(path.join(repo, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
for (const f of ['make-icons.mjs', 'STORE_LISTING.md']) {
  ok(fs.existsSync(path.join(dir, f)), `開発用ファイルが存在する: ${f}`);
  ok(routerSrc.includes(`'${f}'`), `⭐配布zipの除外リストに入っている: ${f}`);
}

console.log(`\n${pass} 件通過 / ${fail} 件失敗`);
if (fail) process.exit(1);
