/**
 * evaluate.js + jobs-registry の検証 (依存なし・オフラインで完結)
 * 実行: node apps/jobs-monitor/test-evaluate.js
 */
import {
  evaluateEntry, evaluateAll, needsImmediateAlert, realertIntervalMs,
  buildDigest, removeByDeadlineMs, lastAnchorMs, fmtAge, todayJst,
} from './evaluate.js';
import { JOBS_REGISTRY, validateRegistry } from '../../config/jobs-registry.mjs';

let pass = 0;
const fails = [];
function eq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { pass++; } else { fails.push(`${label}: 期待 ${e} / 実際 ${a}`); }
}
function ok(cond, label) { if (cond) pass++; else fails.push(label); }

const H = 3600 * 1000;
const D = 24 * H;
/** JSTの日時 → UTCミリ秒 */
const jst = (y, mo, d, h = 0, mi = 0) => Date.UTC(y, mo - 1, d, h, mi) - 9 * H;

// ── 台帳そのものの検証 ──
eq(validateRegistry(), [], '実台帳がバリデーションを通る');
const VALID_SJ = {
  id: 'ok-job', type: 'scheduled_job', importance: 'P1', owner: 'o', purpose: 'p', runbook: 'r',
  where: 'w', schedule: 's', anchor_hour_jst: 7, anchor_minute_jst: 0, grace_hours: 6, lifecycle: 'permanent',
};
eq(validateRegistry([VALID_SJ]), [], '正しいscheduled_jobは通る');
ok(validateRegistry([{ ...VALID_SJ, id: 'A B' }]).length > 0, '不正idを弾く');
ok(validateRegistry([VALID_SJ, VALID_SJ]).some((e) => /重複/.test(e)), 'id重複を検出');
ok(validateRegistry([{ ...VALID_SJ, anchor_hour_jst: undefined }]).some((e) => /anchor_hour_jst/.test(e)), 'anchor欠落を検出');
ok(validateRegistry([{ ...VALID_SJ, anchor_hour_jst: 24 }]).some((e) => /anchor_hour_jst/.test(e)), 'anchor範囲外を検出');
ok(validateRegistry([{ ...VALID_SJ, anchor_minute_jst: 60 }]).some((e) => /anchor_minute_jst/.test(e)), 'minute範囲外を検出');
ok(validateRegistry([{ ...VALID_SJ, grace_hours: 30 }]).some((e) => /grace_hours/.test(e)), 'grace>=24を弾く (次アンカー跨ぎ)');
ok(validateRegistry([{ ...VALID_SJ, importance: 'TMP' }]).some((e) => /TMP/.test(e)), 'scheduled_jobのTMPを弾く');
ok(validateRegistry([{ ...VALID_SJ, where: undefined }]).some((e) => /where/.test(e)), 'where欠落を検出');
const VALID_HO = {
  id: 'ho-1', type: 'human_obligation', importance: 'P1', owner: 'o', purpose: 'p', runbook: 'r',
  where: 'w', schedule: 's', period_hours: 28 * 24, warn_days: 5, lifecycle: 'permanent',
};
eq(validateRegistry([VALID_HO]), [], '正しいhuman_obligationは通る');
ok(validateRegistry([{ ...VALID_HO, warn_days: -1 }]).some((e) => /warn_days/.test(e)), '負のwarn_daysを弾く');
const VALID_TA = {
  id: 'ta-1', type: 'temporary_asset', importance: 'TMP', owner: 'o', purpose: 'p', runbook: 'r',
  where: 'w', remove_by: '2026-09-01', lifecycle: 'temporary',
};
eq(validateRegistry([VALID_TA]), [], '正しいtemporary_assetは通る');
ok(validateRegistry([{ ...VALID_TA, remove_by: '2026-99-99' }]).some((e) => /remove_by/.test(e)), '実在しない日付を弾く');
ok(validateRegistry([{ ...VALID_TA, lifecycle: 'permanent' }]).some((e) => /lifecycle/.test(e)), '一時物のlifecycle不整合を弾く');

// ── lastAnchorMs ──
eq(lastAnchorMs(jst(2026, 8, 1, 12, 0), 7, 0), jst(2026, 8, 1, 7, 0), '12:00から見た直近07:00は今日');
eq(lastAnchorMs(jst(2026, 8, 1, 6, 59), 7, 0), jst(2026, 7, 31, 7, 0), '06:59から見た直近07:00は昨日');
eq(lastAnchorMs(jst(2026, 8, 1, 7, 0), 7, 0), jst(2026, 8, 1, 7, 0), 'ちょうど07:00は今日 (過去扱い)');
eq(lastAnchorMs(jst(2026, 8, 1, 9, 40), 9, 45), jst(2026, 7, 31, 9, 45), '分も考慮する (09:40→昨日09:45)');
eq(lastAnchorMs(jst(2027, 1, 1, 0, 30), 14, 0), jst(2026, 12, 31, 14, 0), '年跨ぎ');

// ── scheduled_job (アンカー方式: 締切 = 直近アンカー + grace) ──
const SJ = { id: 'j1', type: 'scheduled_job', importance: 'P1', purpose: 'p', runbook: 'r', anchor_hour_jst: 7, anchor_minute_jst: 0, grace_hours: 6 };
const SEEN_OLD = { firstSeenAtMs: jst(2026, 7, 1) }; // 1ヶ月前から監視
// 今日07:00のアンカー、13:00が締切
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 8, 1, 8, 24) }, jst(2026, 8, 1, 12, 0)).status, 'ok', '今朝成功→ok');
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 7, 31, 8, 0) }, jst(2026, 8, 1, 12, 59)).status, 'ok', '昨日成功でも締切(13:00)前ならまだok');
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 7, 31, 8, 0) }, jst(2026, 8, 1, 13, 1)).status, 'late', '締切13:01を過ぎたらlate');
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 7, 31, 8, 0) }, jst(2026, 8, 1, 13, 0)).status, 'late', '締切ちょうど13:00でlate (境界は>=)');
// ⭐ドリフトしない: 昨日「遅れて」13:59に成功しても、今日の締切は13:00のまま
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 7, 31, 13, 59) }, jst(2026, 8, 1, 13, 1)).status, 'late', '前日の遅れ成功で締切がドリフトしない (Codex R1 high)');
// アンカー後に成功していれば当然ok
eq(evaluateEntry(SJ, { ...SEEN_OLD, lastOkAtMs: jst(2026, 8, 1, 7, 30) }, jst(2026, 8, 1, 13, 1)).status, 'ok', '今日のアンカー後に成功→ok');

// ── 初回ping未着の昇格 (Codex R1 high) ──
// 監視を始めたばかり (アンカー前) は uninitialized
eq(evaluateEntry(SJ, { firstSeenAtMs: jst(2026, 8, 1, 10, 0) }, jst(2026, 8, 1, 12, 0)).status, 'uninitialized', '監視開始直後はuninitialized');
// 監視開始後のアンカーを跨いで締切も過ぎたのに ping が無い → late (配線忘れ・ジョブ死)
const rNoPing = evaluateEntry(SJ, { firstSeenAtMs: jst(2026, 8, 1, 5, 0) }, jst(2026, 8, 1, 13, 1));
eq(rNoPing.status, 'late', 'ping未着でも初回締切超過でlateに昇格');
ok(/配線/.test(rNoPing.detail), '配線忘れの可能性を案内');
// 監視開始前のアンカーには責任を持たない (デプロイ直後の誤報防止)
eq(evaluateEntry(SJ, { firstSeenAtMs: jst(2026, 8, 1, 13, 30) }, jst(2026, 8, 1, 14, 0)).status, 'uninitialized', '監視開始前のアンカーでは鳴らさない');

// ── human_obligation ──
const NOW = jst(2026, 8, 1, 12, 0);
const HO = { id: 'h1', type: 'human_obligation', importance: 'P1', purpose: 'p', runbook: 'r', period_hours: 28 * 24, warn_days: 5 };
eq(evaluateEntry(HO, { ...SEEN_OLD, lastOkAtMs: NOW - 1 * D }, NOW).status, 'ok', '昨日実施なら余裕');
eq(evaluateEntry(HO, { ...SEEN_OLD, lastOkAtMs: NOW - 24 * D }, NOW).status, 'due_soon', '残り4日はdue_soon');
eq(evaluateEntry(HO, { ...SEEN_OLD, lastOkAtMs: NOW - 22 * D }, NOW).status, 'ok', '残り6日はまだok');
eq(evaluateEntry(HO, { ...SEEN_OLD, lastOkAtMs: NOW - 29 * D }, NOW).status, 'overdue', '期限超過はoverdue');
// 未初期化でも firstSeen 起点で昇格する (永遠に無音にならない)
eq(evaluateEntry(HO, { firstSeenAtMs: NOW - 1 * D }, NOW).status, 'uninitialized', '記録なし+期限遠い→uninitialized');
eq(evaluateEntry(HO, { firstSeenAtMs: NOW - 24 * D }, NOW).status, 'due_soon', '記録なしでもfirstSeen起点でdue_soonへ');
eq(evaluateEntry(HO, { firstSeenAtMs: NOW - 29 * D }, NOW).status, 'overdue', '記録なしでもfirstSeen起点でoverdueへ');

// ── temporary_asset ──
const TA = { id: 't1', type: 'temporary_asset', importance: 'TMP', purpose: 'p', runbook: 'r', remove_by: '2026-09-01' };
eq(evaluateEntry(TA, null, NOW).status, 'ok', '撤去期限前はok');
eq(evaluateEntry(TA, null, jst(2026, 9, 2, 0, 30)).status, 'remove_due', '9/2にはremove_due');
eq(evaluateEntry(TA, null, jst(2026, 9, 1, 21, 0)).status, 'ok', '期限日9/1のJST21時はまだok');
eq(evaluateEntry({ ...TA, remove_by: '2026-02-30' }, null, NOW).status, 'remove_due', '実在しない日付は要対応として出す');
ok(removeByDeadlineMs('2026-09-01') !== null, 'remove_byをパースできる');
eq(removeByDeadlineMs('2026-02-30'), null, '実在しない日はnull');

// ── 即時通知の対象 ──
ok(needsImmediateAlert({ importance: 'P1', status: 'late' }), 'P1のlateは即時');
ok(needsImmediateAlert({ importance: 'P2', status: 'overdue' }), 'P2のoverdueは即時');
ok(!needsImmediateAlert({ importance: 'P3', status: 'late' }), 'P3は即時にしない (朝サマリ)');
ok(!needsImmediateAlert({ importance: 'P1', status: 'due_soon' }), 'due_soonは即時にしない');
ok(!needsImmediateAlert({ importance: 'P1', status: 'uninitialized' }), 'uninitializedは即時にしない');
eq(realertIntervalMs('P1'), 6 * H, 'P1の再通知は6時間');
eq(realertIntervalMs('P2'), 24 * H, 'P2の再通知は24時間');

// ── digest ──
const dAllOk = buildDigest([{ id: 'a', importance: 'P1', status: 'ok', detail: '' }], [], jst(2026, 8, 1, 8, 50));
ok(/✅ すべて正常/.test(dAllOk), '全部okなら1行サマリ');
const dMix = buildDigest([
  { id: 'a', importance: 'P1', status: 'late', detail: '3日止まってる', runbook: 'logsを見る' },
  { id: 'b', importance: 'P2', status: 'due_soon', detail: 'あと3日', runbook: '再認可する' },
  { id: 'c', importance: 'TMP', status: 'remove_due', detail: '期限切れ', runbook: '消す' },
], ['mystery-job'], jst(2026, 8, 1, 8, 50));
ok(/🔴 \*a\*/.test(dMix), 'lateは🔴で出る');
ok(/🟡 \*b\*/.test(dMix), 'due_soonは🟡で出る');
ok(/🟠 \*c\*/.test(dMix), 'remove_dueは🟠で出る');
ok(/mystery-job/.test(dMix), '未登録pingを警告する');
ok(dMix.indexOf('🔴') < dMix.indexOf('🟠') && dMix.indexOf('🟠') < dMix.indexOf('🟡'), '深刻な順に並ぶ');
// 見出しの日付はJST暦日。配信時刻の 08:50 JST は UTC では前日 23:50 なので、
// UTC由来の日付を使うと毎朝ちょうど1日前が出てしまう (2026-08-02 に実際に発覚)
ok(/\(2026-08-02\)/.test(buildDigest([], [], jst(2026, 8, 2, 8, 50))), '見出しは配信時点のJST暦日 (08:50でも当日)');
ok(/\(2026-08-01\)/.test(buildDigest([], [], jst(2026, 8, 1, 0, 5))), '見出しはJST深夜0:05でも当日');

// ── ユーティリティ ──
eq(fmtAge(3 * D + 5 * H), '3日', '3日+5時間→3日');
eq(fmtAge(5 * H), '5時間', '5時間');
eq(fmtAge(10 * 60 * 1000), '1時間', '1時間未満は切り上げて1時間');
eq(todayJst(jst(2026, 8, 1, 0, 10)), '2026-08-01', 'todayJst JST深夜');
eq(todayJst(Date.UTC(2026, 6, 31, 15, 30)), '2026-08-01', 'todayJst UTC15:30はJST翌日');

// ── 実台帳のサンプル評価 (整合性の煙テスト) ──
const seenAll = {};
for (const e of JOBS_REGISTRY) seenAll[e.id] = { firstSeenAtMs: NOW };
const smoke = evaluateAll(JOBS_REGISTRY, seenAll, NOW);
eq(smoke.length, JOBS_REGISTRY.length, '実台帳を全件評価できる');
ok(smoke.filter((r) => r.type === 'temporary_asset').every((r) => r.status === 'ok'), '一時物は期限前ならok');
ok(smoke.filter((r) => r.type !== 'temporary_asset').every((r) => r.status === 'uninitialized'), '監視開始直後は全部uninitialized (誤報しない)');

// ── 結果 ──
if (fails.length) {
  console.error(`❌ ${fails.length} 件失敗 / ${pass} 件成功`);
  for (const f of fails) console.error(`  - ${f}`);
  process.exit(1);
}
console.log(`✅ 全 ${pass} 件パス`);
