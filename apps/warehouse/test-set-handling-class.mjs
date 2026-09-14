/**
 * test-set-handling-class.mjs — セット商品の取扱区分を構成品から引き継ぐテスト
 *
 * 対象:
 *   rebuild-m-products.js  … resolveSetHandlingClass / セット行への適用
 *
 * 背景 (2026-09-14 中原さん指示):
 *   セットを構成する商品コードが 取扱中止 / ﾒｰｶｰ取扱中止 になっていたら、
 *   セット自体も 取扱中止 / ﾒｰｶｰ取扱中止 の扱いにする。
 *   これまでは NE のセット自身の値 (多くは 取扱中 のまま) だけを見ていたので、
 *   もう組めないセットが m_products で「取扱中」に見えていた。
 *   構成品で 取扱中止 と ﾒｰｶｰ取扱中止 が混ざったら ﾒｰｶｰ取扱中止 を優先する (中原さん決定)。
 *
 * 実行: node apps/warehouse/test-set-handling-class.mjs
 * 本番 DB には触れない (一時 DATA_DIR に専用 warehouse.db を作り、終了時に削除)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'set-handling-class-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const {
  resolveSetHandlingClass, rebuildMProducts,
  HANDLING_ACTIVE, HANDLING_STOPPED, HANDLING_MAKER_STOPPED,
} = await import('./rebuild-m-products.js');
// 🚨 取扱中を表す値は想定利益側の正本 (query.js) と突き合わせる。文字列を写すと片方だけ変えた日に気づけない
const { HANDLING_ACTIVE: EP_HANDLING_ACTIVE } = await import('../expected-profit/query.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(a === b, `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// ───────────────────────── 1. resolveSetHandlingClass (単体) ─────────────────────────
console.log('\n── resolveSetHandlingClass ──');
{
  const C = (handlingClass, componentExists = true) => ({ handlingClass, componentExists });
  const ACT = HANDLING_ACTIVE, STOP = HANDLING_STOPPED, MAKER = HANDLING_MAKER_STOPPED;

  eq(ACT, EP_HANDLING_ACTIVE, '取扱中 の値が expected-profit/query.js の HANDLING_ACTIVE と同じ');
  eq(resolveSetHandlingClass(ACT, [C(ACT), C(ACT)]), ACT, '構成品すべて取扱中 → 取扱中');
  eq(resolveSetHandlingClass(ACT, [C(ACT), C(STOP)]), STOP, '1つでも取扱中止 → 取扱中止');
  eq(resolveSetHandlingClass(ACT, [C(MAKER), C(ACT)]), MAKER, '1つでもﾒｰｶｰ取扱中止 → ﾒｰｶｰ取扱中止');
  eq(resolveSetHandlingClass(ACT, [C(STOP), C(MAKER)]), MAKER, '取扱中止とﾒｰｶｰ取扱中止の混在 → ﾒｰｶｰ取扱中止');
  eq(resolveSetHandlingClass(ACT, [C(MAKER), C(STOP)]), MAKER, '並び順が逆でも ﾒｰｶｰ取扱中止');

  // NE のセット自身が止まっていれば、その値を残す (人が決めた止め方を上書きしない)
  eq(resolveSetHandlingClass(STOP, [C(MAKER)]), STOP, 'NE のセットが取扱中止 → 構成品がﾒｰｶｰでも取扱中止のまま');
  eq(resolveSetHandlingClass(MAKER, [C(ACT)]), MAKER, 'NE のセットがﾒｰｶｰ取扱中止 → そのまま');

  // NE にセットの行が無い (null) / 空 は 取扱中 として扱い、構成品を見る
  eq(resolveSetHandlingClass(null, [C(STOP)]), STOP, 'セットが NE に無い + 構成品が取扱中止 → 取扱中止');
  eq(resolveSetHandlingClass(undefined, [C(ACT)]), ACT, 'セットが NE に無い + 構成品が取扱中 → 取扱中 (従来どおり)');
  eq(resolveSetHandlingClass('', []), ACT, '構成品 0 件 → 取扱中 (従来どおり)');
  eq(resolveSetHandlingClass(ACT, null), ACT, '構成品が配列でない → 取扱中');

  // 分からない構成品だけではセットを止めない
  eq(resolveSetHandlingClass(ACT, [C(STOP, false)]), ACT, '構成品が NE に無い → 止めない');
  eq(resolveSetHandlingClass(ACT, [C(null), C('')]), ACT, '構成品の取扱区分が空 → 止めない');
  eq(resolveSetHandlingClass(ACT, [C(null), C(STOP)]), STOP, '空が混ざっても、止まった構成品があれば止める');

  // 空白: 比べるときだけ除く。NE のセット自身の値を返すときは元の値のまま (Codex R1)
  eq(resolveSetHandlingClass(' 取扱中 ', [C(' 取扱中止 ')]), STOP, '空白付きの構成品の値も 取扱中止 と読む');
  eq(resolveSetHandlingClass(' 取扱中止 ', [C(ACT)]), ' 取扱中止 ', 'NE のセットの値は空白ごとそのまま返す');
  eq(resolveSetHandlingClass(' 取扱中 ', [C(ACT)]), ' 取扱中 ', '引き継がないときも NE の値を書き換えない');
  eq(resolveSetHandlingClass('   ', [C(ACT)]), ACT, '空白だけの値は未登録と同じ → 取扱中');
  eq(resolveSetHandlingClass('   ', [C(MAKER)]), MAKER, '空白だけの値は未登録と同じ → 構成品から引き継ぐ');

  // 知らない値が増えても取りこぼさない (どれを採るかは並べ替えで決める = 毎回同じ)
  eq(resolveSetHandlingClass(ACT, [C('廃番')]), '廃番', '知らない止め方 → その値');
  eq(resolveSetHandlingClass(ACT, [C('廃番'), C(STOP)]), STOP, '知らない値より 取扱中止 を優先');
  eq(resolveSetHandlingClass(ACT, [C('b-停止'), C('a-停止')]), 'a-停止', '知らない値どうしは並べ替えの先頭 (毎回同じ結果)');
}

// ───────────────────────── 2. rebuild でのセットへの適用 ─────────────────────────
console.log('\n── rebuildMProducts: セット取扱区分の引き継ぎ ──');

await initDB();
const db = getDB();
const NOW = '2026-09-14 12:00:00';

const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products
  (商品コード, 商品名, 原価, 売価, 取扱区分, 在庫数, 引当数, 消費税率, 作成日, synced_at)
  VALUES (?, ?, 100, 300, ?, 0, 0, 10, '2026-01-01', ?)`);
const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products
  (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at)
  VALUES (?, ?, 900, ?, 1, ?)`);

// 品質ゲート (総件数 3,000件未満は反映中止) を通すためのダミー単品
db.transaction(() => {
  for (let i = 0; i < 3200; i++) insNe.run(`filler-${i}`, `ダミー${i}`, '取扱中', NOW);
})();

// 構成品 (単品)
insNe.run('comp-ok', '取扱中の商品', '取扱中', NOW);
insNe.run('comp-ok2', '取扱中の商品2', '取扱中', NOW);
insNe.run('comp-stop', '取扱中止の商品', '取扱中止', NOW);
insNe.run('comp-maker', 'ﾒｰｶｰ取扱中止の商品', 'ﾒｰｶｰ取扱中止', NOW);
insNe.run('comp-blank', '取扱区分が空の商品', '', NOW);

const addSet = (code, name, neStatus, comps) => {
  if (neStatus !== undefined) insNe.run(code, name, neStatus, NOW);
  for (const c of comps) insSet.run(code, name, c, NOW);
};
addSet('set-all-ok', '全部取扱中', '取扱中', ['comp-ok', 'comp-ok2']);
addSet('set-stop', '取扱中止を含む', '取扱中', ['comp-ok', 'comp-stop']);
addSet('set-maker', 'ﾒｰｶｰ取扱中止を含む', '取扱中', ['comp-ok', 'comp-maker']);
addSet('set-mixed', '両方を含む', '取扱中', ['comp-stop', 'comp-maker']);
addSet('set-ne-own', 'NE で取扱中止', '取扱中止', ['comp-maker']);
addSet('set-no-ne-row', 'NE にセットの行が無い', undefined, ['comp-stop']);
addSet('set-orphan', '構成品が NE に無い', '取扱中', ['comp-nai']);
addSet('set-blank', '構成品の取扱区分が空', '取扱中', ['comp-blank']);
// 大文字小文字違いの構成品コード (raw_ne_products とは COLLATE NOCASE で結ぶ)
addSet('set-case', '構成品コードが大文字', '取扱中', ['COMP-MAKER']);
// ネストセット: 親 → 子セット → 取扱中止の単品。取扱区分は直接の構成品しか見ないので、
// 子セットは止まるが親セットは取扱中のまま残る (制限として固定する。本番のネストセットは 0 件)
addSet('set-nest-child', '子セット', '取扱中', ['comp-stop']);
addSet('set-nest-parent', '親セット', '取扱中', ['set-nest-child']);

const result = await rebuildMProducts();
ok(result.ok, `rebuild が成功する (${result.total}件)`);

const getMp = db.prepare('SELECT 商品区分, 取扱区分 FROM m_products WHERE 商品コード = ?');
eq(getMp.get('set-all-ok')?.取扱区分, '取扱中', 'set-all-ok: 構成品すべて取扱中 → 取扱中');
eq(getMp.get('set-stop')?.取扱区分, '取扱中止', 'set-stop: 取扱中止を引き継ぐ');
eq(getMp.get('set-maker')?.取扱区分, 'ﾒｰｶｰ取扱中止', 'set-maker: ﾒｰｶｰ取扱中止を引き継ぐ');
eq(getMp.get('set-mixed')?.取扱区分, 'ﾒｰｶｰ取扱中止', 'set-mixed: 混在 → ﾒｰｶｰ取扱中止');
eq(getMp.get('set-ne-own')?.取扱区分, '取扱中止', 'set-ne-own: NE のセット自身の値を残す');
eq(getMp.get('set-no-ne-row')?.取扱区分, '取扱中止', 'set-no-ne-row: NE にセットが無くても構成品から引き継ぐ');
eq(getMp.get('set-orphan')?.取扱区分, '取扱中', 'set-orphan: 構成品が NE に無い → 止めない');
eq(getMp.get('set-blank')?.取扱区分, '取扱中', 'set-blank: 構成品の取扱区分が空 → 止めない');
eq(getMp.get('set-case')?.取扱区分, 'ﾒｰｶｰ取扱中止', 'set-case: 大文字の構成品コードでも引き継ぐ');
eq(getMp.get('set-stop')?.商品区分, 'セット', 'セットとして投入されている');
// 単品はこれまでどおり NE の値のまま
eq(getMp.get('comp-stop')?.取扱区分, '取扱中止', '単品の取扱中止はそのまま');
eq(getMp.get('comp-ok')?.取扱区分, '取扱中', '単品の取扱中はそのまま');

eq(getMp.get('set-nest-child')?.取扱区分, '取扱中止', 'set-nest-child: 直接の構成品から引き継ぐ');
eq(getMp.get('set-nest-parent')?.取扱区分, '取扱中', 'set-nest-parent: 孫の構成品は見ない (制限。品質チェックが警告する)');
ok(result.checks.some(c => /ネストセット（構成品がセット）: 1件/.test(c) && c.includes('取扱区分は直接の構成品しか見ない')),
  '品質チェックがネストセットと取扱区分の制限を警告する');

// 引き継いだ件数 = set-stop / set-maker / set-mixed / set-no-ne-row / set-case / set-nest-child の 6 件
const line = result.log.find(l => l.includes('取扱区分を構成品から引き継ぎ')) || '';
ok(/取扱区分を構成品から引き継ぎ: 6件/.test(line), `ログに引き継いだ件数が出る (${line})`);
// 例は最大 5 件 (セットコード順)。先頭の set-case が入る
ok(line.includes('set-case=ﾒｰｶｰ取扱中止'), 'ログに例 (セットコード=取扱区分) が出る');
ok(result.log.some(l => /売上分類を構成品から導出: \d+件/.test(l)), '売上分類のログは従来どおり出る');

// 構成品が取扱中に戻れば、次の再構築でセットも戻る (m_products の中だけで決めていて、NE の値は書き換えていない)
db.prepare("UPDATE raw_ne_products SET 取扱区分 = '取扱中' WHERE 商品コード = 'comp-stop'").run();
const again = await rebuildMProducts();
ok(again.ok, '2 回目の rebuild が成功する');
eq(getMp.get('set-stop')?.取扱区分, '取扱中', '構成品が取扱中に戻ればセットも取扱中に戻る');
eq(getMp.get('set-maker')?.取扱区分, 'ﾒｰｶｰ取扱中止', '止まったままの構成品のセットは止まったまま');
eq(db.prepare("SELECT 取扱区分 FROM raw_ne_products WHERE 商品コード = 'set-stop'").get()?.取扱区分, '取扱中',
  'NE 由来の raw 値 (セット自身) は書き換えていない');

// ───────────────────────── 結果 ─────────────────────────
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });

console.log(`\n${failed === 0 ? '✅ 全テスト成功' : `❌ ${failed}件 失敗`}`);
process.exit(failed === 0 ? 0 : 1);
