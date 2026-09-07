/**
 * test-record-guard.mjs — 「送れる行が0の履歴」を作らせない / 理由を必ず言う
 *
 * ★守りたい事故 (2026-09-07 実際に起きた):
 *   `lightbluetb-100` の履歴が LINEギフト1行だけででき、価格は1件も送られていないのに
 *   現場は「価格改定した」と思っていた。原因は行のチェック忘れ。
 *   チェックの無い行は記録されず、手動更新モールの行だけは自動で足されるので、
 *   **送れる行が0の履歴が黙って作れてしまう**。
 *
 * 実行: node apps/price-update/test-record-guard.mjs
 */
import {
  isAutoRecordedManualRow, chooseRowsToRecord, summarizeRecord, noTargetReasonOf, MANUAL_ONLY_MESSAGE,
} from './record-guard.js';
import { MALL_LABELS, MALL_CAPABILITIES } from './mall-capabilities.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

/** 画面から来る行 (プレビュー行) の最小形 */
const row = (over = {}) => ({
  mall: 'rakuten', listingCode: 'abc-001', confidence: 'confirmed', manual: false,
  selected: false, newPrice: null, evaluation: { canUpdate: false, blocks: [], warns: [] }, ...over,
});
const sendable = (over = {}) => row({ selected: true, newPrice: 1200, evaluation: { canUpdate: true, blocks: [], warns: [] }, ...over });
const linegift = (over = {}) => row({ mall: 'linegift', manual: true, evaluation: null, ...over });

console.log('\n── 記録される行の選び分け ──');
{
  ok(chooseRowsToRecord([row(), row()]).length === 0, 'チェックの無い行は記録されない');
  ok(chooseRowsToRecord([sendable()]).length === 1, 'チェックした行は記録される');
  ok(chooseRowsToRecord([linegift()]).length === 1, '★手動更新モールの行はチェック無しでも記録される');
  ok(chooseRowsToRecord([linegift({ confidence: 'unresolved' })]).length === 0,
    '引き当てできなかった手動行は足さない (チェックのしようがない)');
  ok(chooseRowsToRecord([linegift({ listingCode: null })]).length === 0, '出品コードが無い手動行は足さない');
  ok(!isAutoRecordedManualRow(row()), '更新できるモールの行は自動では足さない');
}

console.log('\n── 送れる行が0 (チェック忘れ) を見つける ──');
{
  const only = summarizeRecord([row({ newPrice: 1200 }), linegift()]);
  ok(only.chosen.length === 1 && only.manual.length === 1, '売価を入れてもチェックが無ければ、記録は手動行1件だけ');
  ok(only.manualOnly === true, '★送れる行が0だと分かる (これを記録の前に知らせる)');

  const mixed = summarizeRecord([sendable(), linegift()]);
  ok(mixed.manualOnly === false, 'チェックした行があれば止めない');
  ok(mixed.sendable.length === 1, '送れる行を数えられる');

  // ⛔ で止まっている行は「選んではいる」= 黙って死ぬケースではない (画面に数が出ている)
  const blocked = summarizeRecord([row({ selected: true, newPrice: 1 }), linegift()]);
  ok(blocked.manualOnly === false, '⛔ の行を選んでいる場合は manualOnly にしない (画面に理由が出ている)');
  ok(blocked.sendable.length === 0, 'それでも送れる行は0と数える');

  ok(summarizeRecord([]).manualOnly === false, '1行も無いときは manualOnly にしない (別の文言で断る)');
  ok(MANUAL_ONLY_MESSAGE.includes('チェック'), 'サーバ側の断り文句が「チェック」に触れている');
}

console.log('\n── 履歴側: なぜ送る行が無いのかを必ず言う ──');
{
  const op = (over = {}) => ({ mall: 'rakuten', initial_state: 'previewed', state: 'previewed', ...over });

  const manualOnly = noTargetReasonOf([op({ mall: 'linegift', initial_state: 'manual_required', state: 'manual_done' })]);
  ok(manualOnly.includes('LINEギフト'), '★モール名を日本語で出す (生の linegift を見せない)');
  ok(manualOnly.includes('1件も送られていません'), '★送られていないことを言い切る (ここが今回の申告の核心)');
  ok(manualOnly.includes('チェック'), 'どうすれば送れるのかを書く');

  const blocked = noTargetReasonOf([op({ initial_state: 'blocked_preview', state: 'blocked_preview' }), op({ mall: 'amazon', initial_state: 'manual_required', state: 'manual_required' })]);
  ok(blocked.includes('ガードで止まっています'), 'ガードで止まった行は数えて言う');
  ok(blocked.includes('Amazon'), '手動更新モールの行も内訳に出す');

  const done = noTargetReasonOf([op({ state: 'confirmed' })]);
  ok(done.includes('すでに送信を終えています'), '送信済みだけの履歴はそう言う');

  ok(noTargetReasonOf([]).includes('行がありません'), '空の履歴でも文言が出る');
  ok(typeof noTargetReasonOf(null) === 'string', '壊れた入力でも落ちない');
}

console.log('\n── モール名の正本 ──');
{
  ok(MALL_LABELS.linegift === 'LINEギフト', 'LINEギフトの名前がある');
  ok(Object.keys(MALL_LABELS).length === Object.keys(MALL_CAPABILITIES).length,
    '★すべてのモールに日本語名がある (足したモールが英字のまま出ない)');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
