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
  isAutoRecordedManualRow, chooseRowsToRecord, summarizeRecord,
  noSendableReasonOf, noSendableMessageOf, NO_SENDABLE_MESSAGES, noTargetReasonOf,
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

console.log('\n── 送れる行が0 を見つける (記録の前に知らせる) ──');
{
  const reasonOf = (rows) => noSendableReasonOf(summarizeRecord(rows));

  const only = summarizeRecord([row({ newPrice: 1200 }), linegift()]);
  ok(only.chosen.length === 1 && only.manual.length === 1, '売価を入れてもチェックが無ければ、記録は手動行1件だけ');
  ok(reasonOf([row({ newPrice: 1200 }), linegift()]) === 'manual_only', '★チェック忘れ = manual_only');

  const mixed = summarizeRecord([sendable(), linegift()]);
  ok(reasonOf([sendable(), linegift()]) === null, '送れる行があれば止めない');
  ok(mixed.sendable.length === 1, '送れる行を数えられる');

  // ★⛔ の行だけを選んだ履歴も「1件も送られない」。ここを素通りさせない (Codex R1 高)
  ok(reasonOf([row({ selected: true, newPrice: 1 }), linegift()]) === 'all_blocked',
    '★⛔ の行だけを選んだ場合も止める (all_blocked)');
  ok(reasonOf([row({ selected: true })]) === 'all_blocked', '★売価が未入力の行だけでも止める');
  ok(reasonOf([sendable(), row({ selected: true, newPrice: 1 })]) === null,
    '送れる行が1つでもあれば、ほかが ⛔ でも止めない');

  ok(reasonOf([]) === null, '1行も無いときはここでは止めない (別の文言で断る)');
  ok(NO_SENDABLE_MESSAGES.manual_only.includes('チェック'), 'チェック忘れの文言が「チェック」に触れている');
  ok(NO_SENDABLE_MESSAGES.all_blocked.includes('判定'), '⛔ の文言が「判定」の列に案内している');
  ok(noSendableMessageOf('all_blocked').includes('もう一度押してください'), 'サーバの断りに、通す道が書いてある');
  ok(noSendableMessageOf('しらない理由').includes('送れる行がありません'), '知らない理由でも文言が出る');
}

console.log('\n── 履歴側: なぜ送る行が無いのかを必ず言う ──');
{
  const op = (over = {}) => ({ mall: 'rakuten', initial_state: 'previewed', state: 'previewed', ...over });

  const manualOnly = noTargetReasonOf([op({ mall: 'linegift', initial_state: 'manual_required', state: 'manual_done' })]);
  ok(manualOnly.includes('LINEギフト'), '★モール名を日本語で出す (生の linegift を見せない)');
  ok(manualOnly.includes('1件も送られていません'), '★送られていないことを言い切る (ここが今回の申告の核心)');
  ok(manualOnly.includes('チェック'), 'どうすれば送れるのかを書く');

  const blocked = noTargetReasonOf([
    op({ initial_state: 'blocked_preview', state: 'blocked_preview' }),
    op({ mall: 'amazon', initial_state: 'manual_required', state: 'manual_required' }),
  ]);
  ok(blocked.includes('ガードで止まっています'), 'ガードで止まった行は数えて言う');
  ok(blocked.includes('Amazon'), '手動更新モールの行も内訳に出す');

  // ★「previewed 以外 = 送信済み」とまとめない (Codex R1 高)。結果不明を送信済みと読ませない
  const done = noTargetReasonOf([op({ state: 'confirmed' })]);
  ok(done.includes('更新済み'), '送って変わったことも確かめた行はそう言う');

  // ★noop は書き込んでいない (execute.js)。送信済みと混ぜない (Codex R2 中)
  const noop = noTargetReasonOf([op({ state: 'noop' })]);
  ok(noop.includes('もともと同じ価格') && noop.includes('書き込んでいません'), '★noop を「送信済み」と言わない');

  const unknown = noTargetReasonOf([op({ state: 'unknown' })]);
  ok(!unknown.includes('更新済み'), '★結果が不明な行を「送信済み」と言わない');
  ok(unknown.includes('結果が不明') && unknown.includes('モールの画面で実際の価格'),
    '★結果が不明ならモールの画面を見るよう言う (自動で送り直さない運用に合わせる)');
  ok(noTargetReasonOf([op({ state: 'executing' })]).includes('送信中'), '送信中の行も不明側に入れる');

  // ★failed は「送る前に弾かれた」と「送った後の照合が通らなかった」が混ざる (Codex R2 高)。
  //   後者では価格が変わっているので、「送られていません」と言い切ってはいけない
  const failed = noTargetReasonOf([op({ state: 'failed' })]);
  ok(!failed.includes('送られていません'), '★failed を「送っていない」と言い切らない');
  ok(failed.includes('モールの画面で実際の価格'), '★failed でもモールの画面で確かめるよう言う');

  const notSent = noTargetReasonOf([op({ state: 'conflict' }), op({ state: 'skipped' })]);
  ok(notSent.includes('送られていません'), '価格の食い違い・停止は「送られていない」と言う');
  ok(!notSent.includes('更新済み'), '送られていない行を送信済みに混ぜない');

  ok(noTargetReasonOf([op({ state: 'みたことない状態' })]).includes('どれにも当てはまらない'),
    '知らない状態でも黙って送信済み扱いにしない');

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
