/**
 * Chrome拡張 (tools/ne-select-set-helper) の静的チェック。
 * ブラウザ無しで回せる範囲だけ: JSの構文と manifest の整合性。
 * 拡張を読み込んで初めて分かる不備 (セレクタ違い等) は実機確認で見る。
 */
import fs from 'fs';
import path from 'path';
import vm from 'vm';
import { fileURLToPath } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );
const dir = path.join(repo, 'tools', 'ne-select-set-helper');

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};

for (const f of ['content.js', 'sw.js', 'options.js']) {
  const p = path.join(dir, f);
  try {
    new vm.Script(fs.readFileSync(p, 'utf8'), { filename: f });
    ok(true, `構文 ${f}`);
  } catch (e) {
    ok(false, `構文 ${f}: ${e.message}`);
  }
}

const mf = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'));
ok(mf.manifest_version === 3, 'manifest_version は 3');
ok(mf.content_scripts?.[0]?.matches?.some((m) => m.includes('main.next-engine.com/Userjyuchu')),
  'NEの受注伝票画面にだけ差し込む (対象を広げすぎない)');
ok(mf.host_permissions?.some((h) => h.includes('wh.bfaith-wh.uk')), 'ポータル (miniPC) への接続許可がある');
ok(!mf.permissions?.includes('tabs') && !mf.permissions?.includes('<all_urls>'),
  '余計な権限を持たない');
for (const f of [...(mf.content_scripts?.[0]?.js || []), ...(mf.content_scripts?.[0]?.css || []),
  mf.background?.service_worker, mf.options_page]) {
  if (!f) continue;
  ok(fs.existsSync(path.join(dir, f)), `manifest が参照する ${f} が存在する`);
}

// content.js が実機で確認したセレクタを使っているか (ここが変わると黙って動かなくなるので固定する)
const content = fs.readFileSync(path.join(dir, 'content.js'), 'utf8');
for (const sel of ['search_order_line_result', 'meisai_assist_input_syohin_code',
  'meisai_assist_input_jyuchu_su', 'meisai_assist_input_btn']) {
  ok(content.includes(sel), `実測セレクタ ${sel} を参照している`);
}
ok(/再計算/.test(content), '保存時の再計算ダイアログについて警告を出している');

// 接続先 (miniPC) は Cloudflare Access の後ろにあるので、素の fetch では 302 される。
// ブラウザのログインセッションを乗せるのが前提なので、そこが外れたら気づけるようにする
const sw = fs.readFileSync(path.join(dir, 'sw.js'), 'utf8');
ok(/credentials:\s*'include'/.test(sw), 'Cloudflare Access を通すため credentials:include で叩いている');
ok(/cloudflareaccess/.test(sw), 'CF Accessへ飛ばされたことを検知して案内している');

// Codexレビュー (2026-08-08) で潰した事故シナリオが、コードから消えていないことを固定する
ok(/findTargetRow/.test(content), '元行を商品コードだけで探さない (同じセットが複数行あるとき用)');
ok(/r\.op === t\.op/.test(content), '商品opと受注数まで見て元行を一意に特定している');
ok(/waitForAdded/.test(content), '固定sleepではなく行が増えるまで待っている');
ok(/diffRows/.test(content), '追加前後の差分で「今回増えた行」を見ている (既存行を成果に数えない)');
ok(/parseQuantity/.test(content), '受注数を勝手に1に丸めない');
ok(/一致しません|missing/.test(content), '追加された明細を指示内容と照合している');
ok(/保存しないでください/.test(content), '失敗時は保存させない強い警告を出している');
ok(/let applying/.test(content), '同時実行を止めるロックがある');

// Chromeウェブストアに出すのに必要なもの
ok(!!mf.icons && ['16', '32', '48', '128'].every((k) => mf.icons[k]), 'ストア提出に必要なアイコンが揃っている');
for (const f of Object.values(mf.icons || {})) {
  ok(fs.existsSync(path.join(dir, f)), `アイコン ${f} が存在する`);
}
ok(/^\d+\.\d+\.\d+$/.test(mf.version), 'version がストアの形式 (x.y.z)');
ok((mf.description || '').length > 0 && (mf.description || '').length <= 132, '説明が132文字以内 (ストアの上限)');
ok(fs.existsSync(path.join(dir, 'STORE_LISTING.md')), '提出用の文面がある');

console.log(`\n合計: ${pass} pass / ${fail} NG`);
process.exit(fail === 0 ? 0 : 1);
