/**
 * Chrome拡張の判定ロジックのテスト。
 * content.js をサンドボックスで読み込み、公開している純粋な部品だけを直接叩く。
 *
 * Codexレビュー2巡目 (2026-08-08) の指摘:
 *   「関数名や文字列の存在を見るだけの静的テストでは、
 *     『既存行を追加結果として数えてしまう』問題を検出できない」
 * ここはその再発防止。実DOMは使わず、行の配列で突き合わせる。
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
const src = fs.readFileSync(path.join(repo, 'tools/ne-select-set-helper/content.js'), 'utf8');

// content.js は受注伝票画面でだけ動くので、別のパスにしておけば init() は何もせず抜ける
const sandbox = {
  location: { pathname: '/other', search: '' },
  document: { getElementById: () => null, querySelector: () => null },
  chrome: { runtime: { sendMessage: () => {} } },
  setTimeout,
  clearTimeout,
  console,
  Event: class {},
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(src, sandbox, { filename: 'content.js' });

const I = sandbox.__bfSelectSetInternals;

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};
const eq = (a, b, n) => ok(JSON.stringify(a) === JSON.stringify(b),
  `${n}${JSON.stringify(a) === JSON.stringify(b) ? '' : `  期待=${JSON.stringify(b)} 実際=${JSON.stringify(a)}`}`);

ok(!!I, 'content.js から判定ロジックを取り出せる');

console.log('=== 受注数の読み取り (補正しない) ===');
for (const [input, want] of [['3', 3], [' 2 ', 2], ['1', 1], ['999', 999]]) {
  eq(I.parseQuantity(input), want, `「${input}」→ ${want}`);
}
for (const input of ['0', '', '1.5', '12abc', 'あ', '-1', '1000', null, undefined, '１']) {
  eq(I.parseQuantity(input), null, `「${input}」は読み取り失敗として null (勝手に1にしない)`);
}

console.log('\n=== 増えた行の抽出 (差分で見る) ===');
const A1 = { code: 'A', quantity: 1 };
const B1 = { code: 'B', quantity: 1 };
const X1 = { code: 'X', quantity: 1 };
eq(I.diffRows([A1], [A1, B1]).added, ['B×1'], '増えた行だけを返す');
eq(I.diffRows([A1], [A1]).added, [], '変化なしなら空');
eq(I.diffRows([A1, B1], [A1]).removed, ['B×1'], '減った行も分かる');
eq(I.diffRows([A1], [A1, A1]).added, ['A×1'], '同じ内容が増えた場合も1件として数える');

console.log('\n=== 🚨 既存行を「今回の成果」として数えない (Codex2巡目の指摘) ===');
// 追加前から A×1 があり、期待は A×1 と B×1。NEが誤って X×1 と B×1 を入れた場合
const before = [{ code: 'SET', quantity: 1 }, A1];
const after = [{ code: 'SET', quantity: 1 }, A1, X1, B1];
const added = I.diffRows(before, after).added;
eq(added.sort(), ['B×1', 'X×1'], '増えたのは X と B (元から在った A は含めない)');
const cmp = I.compareAdded(added, [A1, B1]);
ok(!cmp.ok, '🚨 行数が合っていても「A は元から在った / X は余分」で不一致になる');
eq(cmp.missing, ['A×1'], '足りない行が分かる');
eq(cmp.extra, ['X×1'], '余分な行が分かる');

console.log('\n=== 指示どおりに増えたときだけ成功 ===');
ok(I.compareAdded(I.diffRows(before, [...before, B1, A1]).added, [A1, B1]).ok,
  '指示した2行がちょうど増えていれば成功');
ok(I.compareAdded(['S×1', 'S×1'], [{ code: 'S', quantity: 1 }, { code: 'S', quantity: 1 }]).ok,
  '同じ商品を2行入れる場合も (同じ香りを2本選んだ注文) 正しく数える');
ok(!I.compareAdded(['S×1'], [{ code: 'S', quantity: 1 }, { code: 'S', quantity: 1 }]).ok,
  '2行必要なのに1行しか増えていなければ失敗');
ok(!I.compareAdded(['S×1', 'S×1', 'S×1'], [{ code: 'S', quantity: 1 }, { code: 'S', quantity: 1 }]).ok,
  '余分に増えていても失敗');
ok(!I.compareAdded(['S×2'], [{ code: 'S', quantity: 1 }]).ok, '数量が違えば失敗');

console.log('\n=== 🚨 追加が落ち着くまで待つ (Codex3巡目の指摘) ===');
// NEが段階的に行を足す場合、期待行数に達した瞬間に照合すると
// 後から現れる余分な行を見逃す。中身が一定時間変わらないことを条件にする。
const SET = { code: 'SET', quantity: 1 };
const base = [SET];

{
  const res = await I.waitForStableAdded(base, 2, {
    quietMs: 100, timeoutMs: 3000, readCurrent: () => [SET, A1, B1],
  });
  ok(res.stable, '変化がなければ安定と判定する');
  eq(res.added.sort(), ['A×1', 'B×1'], '増えた行を返す');
}

{
  // 期待の2行が先に出て、そのあと余分な3行目が遅れて追加されるケース
  let calls = 0;
  const res = await I.waitForStableAdded(base, 2, {
    quietMs: 400,
    timeoutMs: 5000,
    readCurrent: () => (++calls <= 2 ? [SET, A1, B1] : [SET, A1, B1, X1]),
  });
  ok(res.stable, '最終的には安定する');
  eq(res.added.sort(), ['A×1', 'B×1', 'X×1'], '🚨 遅れて増えた余分な行も拾う');
  ok(!I.compareAdded(res.added, [A1, B1]).ok, '余分があるので照合は失敗する (成功表示にしない)');
}

{
  // ずっと増え続けて落ち着かないケース
  let n = 0;
  const res = await I.waitForStableAdded(base, 2, {
    quietMs: 400,
    timeoutMs: 1200,
    readCurrent: () => [SET, ...Array.from({ length: ++n }, (_, i) => ({ code: 'R' + i, quantity: 1 }))],
  });
  ok(!res.stable, '変化し続けるなら安定しないと判定する (成功にしない)');
}

console.log(`\n合計: ${pass} pass / ${fail} NG`);
process.exit(fail === 0 ? 0 : 1);
