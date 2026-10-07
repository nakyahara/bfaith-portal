#!/usr/bin/env node
/**
 * test-company-db-migrate-lock-watch.mjs — migrate の lock の 45 分の見張り (scripts/company-db/migrate-lock-watch.mjs)・
 *   見張りつきで migrate を流す 1 つのコマンド (scripts/company-db/migrate-watched.mjs)・runner の見張りの関門 (migrate.mjs の LOCK_WATCH_REQUIRED) の試験
 *   (設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 v3.9「見張り」・v3.12「45 分の見張り」・PR #1638 Codex R1)
 *
 * 固定する契約:
 *   U0 既定 = 45 分 (runner の MIGRATE_LOCK_ALERT_MINUTES)・60 秒ごと・--url を受けない・--interval-sec / --alert-min は --dry-run の時だけ
 *   U1〜U4・U6〜U10 見張りの数え方と知らせ (時計を差し替え)
 *   U5 1 つのコマンドの道 (runSupervised) = 起動の見回りで lock が無い → その後の lock は「前の見回りから」数える (「見張りを始めた時から」= 遅く鳴る は起きない)。
 *      起動し直した見張り (since) は migrate を始めた時刻から数える。単独の関数の呼び出しで始めた時にもう持たれていた時だけ「始めた時から」(CLI は単独で起動しない)
 *   T1 終わりの知らせ (✅・8 時間の ❌) は届くまで数回送り直し、届かなければ exit 3
 *   S1 起動の知らせが届かない (有効な https の URL・応答 404 = sendJobsChat が false) → ready を返さない・exit 4 / 届けば ready の後に見張る・親の done で終わる
 *   S2 始める前から別の migrate が lock を持っている → held (知らせは送らない・exit 1)
 *   O1 1 つのコマンドの順番 (差し替え): ready の前に migrate を始めない・ready が来ない / held / 時間切れ = migrate を始めない・見張りが死ねば知らせて起動し直す (since = migrate を始めた時刻)・
 *      起動し直せなければ exit 3・migrate の失敗は exit 1・見張りの終わりの知らせが届かなければ exit 3
 *   R1〜R4 本物の PG 18.4 (権限の無い役割から別の役割の lock の pid は見え backend_start は見えない・別の DB は数えない・runner の lock の間に鳴る・読み手は lock を取らない)
 *   🆕 Codex R2:
 *   T2 持ち主が替わった時の A の「外れた」が届かなければ、B が外れた後に exit 3 (届けば 0)
 *   P1〜P3 親が死んだ (IPC が切れた): lock が無ければ知らせて終わる / lock があれば知らせて外れるまで見張る (45 分の知らせも) /
 *      ready から 10 分 lock が現れない = 知らせて exit 5 / lock の無い状態にも 8 時間の上限
 *   O5 見張りが exit 5 で終わった = 起動し直さない・exit 3 / O6 見張り・migrate を起動できない (例外) = 文書の exit code / O7 run の nonce を見張り (起動し直しも) と migrate に同じに渡す
 *   R5 heartbeat の契約 (本物の PG): 見張りの読み手が見回りのたびに application_name に nonce と server の epoch を書く・runner は同じ DB・同じ nonce・120 秒以内だけ通す
 *      (nonce が無い・違う run・idle の接続 (heartbeat の無い名前)・古い heartbeat (止まった見張り)・未来の epoch・別の DB = 通らない)
 *   G1 runner の関門は本物の PG の CIC の道で既定で必須 (opts を渡さない migrateWithLock の直の呼び出しでも・supportsConcurrentIndex を false にした本物の PG の adapter でも
 *      heartbeat が無ければ LOCK_WATCH_REQUIRED・何も作らない・記録しない・lock が残らない)・新しい heartbeat があれば流れる・見張りが止まって古くなれば次の file で止まる
 *   G2 外す道は PGlite の adapter (lockWatchExempt) だけ = pgAdapter は持たない・runner に外す opts は無い・heartbeat を書く関数を試験の外で使うのは見張りだけ (grep の縛り)
 *   E3 本物の子のプロセス: 親を kill → 見張りは IPC の切れを見て、lock が無ければ知らせて exit 0 / lock があれば外れるまで見張って ✅
 *   L1 fork / spawn を起動できない (error event) → 未処理の誤りで落ちずに exit 1 に変わる
 *   🆕 Codex R3:
 *   P4 親と migrate がほぼ同時に消えた (held → none と親の切れが同じ見回り) → 親の死を先に知らせる (外れたことも同じ文に・45 分の知らせを出していたかも)・exit 0 /
 *      その知らせが 5 回とも届かない → exit 3
 *   P5 親が切れた時に lock が残っていた ⚠️ が 5 回とも届かない → 覚えて、外れた後に exit 3 (届けば 0)
 *   G3 本物の PG の adapter で CONCURRENTLY を外す道 (取引の中) も各文の直前に heartbeat を確かめる = 1 文目の後に heartbeat が古くなると 2 文目の前で止まり、
 *      取引ごと巻き戻して記録しない (1 文目の index も残らない・lock も残らない) / heartbeat が新しいままなら 2 文とも作って記録する
 *      (取引の中の pg_stat_activity は写し = pg_stat_clear_snapshot で捨ててから読む。捨てなければ 1 文目の前の新しい heartbeat が見えたままで通ってしまう)
 *   E1 本物の子のプロセス: migrate-watched の本体 + 見張りの CLI (--supervised --dry-run) + migrate.mjs の CLI = 起動の知らせ → migrate → ⚠️ → ✅ → exit 0
 *   E2 本物の子のプロセス: 起動の知らせが届かない (届かない https) → migrate を始めない (記録の表も無い)
 *   C1 CLI: 見張りは単独で起動しない・--url / --alert-min (dry-run でない) は exit 2 / migrate-watched は送り先・接続先が無い・--url・--dry-run で exit 2
 *   C2 見張りの CLI --dry-run (単独) が本物の PG で鳴る文を出し、外れたら exit 0・password を出さない
 * 使い方: node scripts/test-company-db-migrate-lock-watch.mjs   (npm run test:company-db にも入っている)
 */
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs, runLockWatch, runSupervised, readLockHolder, pgHolderReader, alertText, writeLockWatchHeartbeat, DEFAULTS, EXIT, WATCH_APPLICATION_NAME } from './company-db/migrate-lock-watch.mjs';
import { runWatchedMigrate, forkWatcher, spawnMigrate, newRunNonce, parseArgs as parseWatchedArgs } from './company-db/migrate-watched.mjs';
import { openPgClient, pgAdapter, pgliteAdapter, withMigrateLock, migrateWithLock, describeLockHolder, buildIndexExpect, readIndexAttrs, assertLockWatchFresh, MIGRATE_LOCK_NAME, MIGRATE_LOCK_ALERT_MINUTES, MIGRATE_LOCK_WATCH_APPLICATION_NAME, LOCK_WATCH_NONCE_ENV, LOCK_WATCH_HEARTBEAT_MAX_AGE_SEC } from './company-db/migrate.mjs';
import { sendJobsChat } from './logizard-import/notify-jobs.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const WATCH_CLI = path.join(ROOT, 'scripts', 'company-db', 'migrate-lock-watch.mjs');
const WATCHED_CLI = path.join(ROOT, 'scripts', 'company-db', 'migrate-watched.mjs');
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const MIN = 60000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * 時計を差し替えた見張り。timeline(tMs) = その時刻 (起動からの ms) の読みの結果 (null・持ち主・Error)。
 * sendOk(i, text) = i 回目の送りが届くか。戻り = { result, sends: [{ at, text, ok }], logs }
 */
async function sim(timeline, opts = {}, sendOk = () => true) {
  let clock = 0;
  const sends = [], logs = [];
  const result = await runLockWatch({
    now: () => clock,
    sleep: async (ms) => { clock += ms; },
    readHolder: async () => { const r = timeline(clock); if (r instanceof Error) throw r; return r; },
    send: async (text) => { const okk = sendOk(sends.length, text); sends.push({ at: clock, text, ok: okk }); return okk; },
    log: (m) => logs.push(m),
    dbName: 'testdb',
    ...opts,
  });
  return { result, sends, logs };
}
const H = (pid, extra = {}) => ({ pid, applicationName: 'company-db-migrate', usename: 'deployer', startVisible: false, heldMs: null, phase: null, relation: null, ...extra });

console.log('— 見張り (時計を差し替え) —');
await t('U0 既定 = 45 分 (runner の定数)・60 秒ごと・--url を受けない・--interval-sec / --alert-min は --dry-run の時だけ', async () => {
  assert.equal(DEFAULTS.alertMin, 45);
  assert.equal(DEFAULTS.alertMin, MIGRATE_LOCK_ALERT_MINUTES);
  assert.equal(DEFAULTS.intervalSec, 60);
  assert.equal(WATCH_APPLICATION_NAME, MIGRATE_LOCK_WATCH_APPLICATION_NAME);
  const a = parseArgs(['--supervised']);
  assert.deepEqual([a.alertMin, a.intervalSec, a.dryRun, a.supervised, a.since], [45, 60, false, true, null]);
  assert.equal(parseArgs(['--supervised', '--since', '1791000000000']).since, 1791000000000);
  assert.equal(parseArgs(['--alert-min', '0.05', '--interval-sec', '1', '--dry-run']).alertMin, 0.05);
  assert.throws(() => parseArgs(['--url', 'postgres://u:p@h/d']), /--url は受けない/);
  assert.throws(() => parseArgs(['--supervised', '--alert-min', '30']), /--dry-run の時だけ/);
  assert.throws(() => parseArgs(['--supervised', '--interval-sec', '30']), /--dry-run の時だけ/);
  for (const bad of [['--alert-min', '0', '--dry-run'], ['--alert-min', '--dry-run'], ['--interval-sec', 'x', '--dry-run'], ['--interval-sec', '0.5', '--dry-run'], ['--what'], ['--since', 'x']]) assert.throws(() => parseArgs(bad), undefined, bad.join(' '));
  assert.deepEqual(parseWatchedArgs(['--to', '0061']), { to: '0061' });
  for (const bad of [['--url', 'postgres://x'], ['--dry-run'], ['--list'], ['--to', '61'], ['--x']]) assert.throws(() => parseWatchedArgs(bad), undefined, bad.join(' '));
});
await t('U1 (単独の関数) lock が一度も現れない → 待ちの時間 (30 分) で終わる・送らない・exit 0', async () => {
  const r = await sim(() => null);
  assert.deepEqual([r.result.outcome, r.result.code, r.sends.length], ['never_seen', 0, 0]);
});
await t('U2 45 分より前に外れる → 送らない・exit 0', async () => {
  const r = await sim((c) => (c >= 2 * MIN && c < 40 * MIN ? H(101) : null));
  assert.deepEqual([r.result.outcome, r.result.code, r.sends.length], ['released', 0, 0]);
});
await t('U3 backend_start が見える → 接続から数え、45 分を超えた最初の見回りで 1 回・60 分ごとにもう一度・外れたら「外れた」', async () => {
  const r = await sim((c) => (c < 130 * MIN ? H(202, { startVisible: true, heldMs: c + 5 * MIN, phase: 'building index: scanning table', relation: 'core.listings' }) : null));
  assert.deepEqual([r.result.outcome, r.result.code], ['released', 0]);
  assert.deepEqual(r.sends.map((s) => s.at / MIN), [40, 100, 130]);
  assert.ok(r.sends[0].text.includes(`migrate の lock (${MIGRATE_LOCK_NAME}) を 45 分持っている (45 分を超えた)`), r.sends[0].text);
  assert.match(r.sends[0].text, /pid 202 \(company-db-migrate・deployer\)/);
  assert.match(r.sends[0].text, /接続から \(backend_start\)/);
  assert.match(r.sends[0].text, /core\.listings の building index: scanning table/);
  assert.match(r.sends[0].text, /pg_cancel_backend/);
  assert.match(r.sends[1].text, /まだ外れていない/);
  assert.ok(r.sends[2].text.startsWith('✅ ') && r.sends[2].text.includes(`lock (${MIGRATE_LOCK_NAME}) が外れた: pid 202`), r.sends[2].text);
});
await t('U4 backend_start が見えない・直前の見回りで lock が無かった → その見回りから数える (見えた時刻から数えるより 1 回早い)', async () => {
  const r = await sim((c) => (c >= 10.5 * MIN && c < 70 * MIN ? H(303) : null));
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.deepEqual(alerts.map((s) => s.at / MIN), [55]);
  assert.match(alerts[0].text, /前の見回りで lock が無かった時から/);
  assert.ok(r.sends.at(-1).text.startsWith('✅'));
});
await t('U5 1 つのコマンドの道 = 起動の見回りで lock が無い → 次の見回りで見えた lock は「前の見回りから」(遅く鳴らない) / 起動し直し (since) = migrate を始めた時刻から / 単独の関数で始めた時にもう持たれていた時だけ「始めた時から」', async () => {
  // runSupervised: 起動の見回り (時刻 0) は無し → ready → 親が migrate を始め、0.5 分に lock → 1 分の見回りで見える → 0 分から数える = 45 分で鳴る
  let clock = 0; const sends = [], ipc = []; let done = false;
  const reader = { dbName: async () => 'cdb', read: async () => (done && clock >= 50 * MIN ? null : clock >= 0.5 * MIN && clock < 50 * MIN ? H(501) : null) };
  const r = await runSupervised({ reader, send: async (x) => { sends.push({ at: clock, x }); return true; }, ipcSend: (m) => ipc.push({ at: clock, m, sendsBefore: sends.length }), isParentDone: () => done, now: () => clock, sleep: async (ms) => { clock += ms; if (clock >= 50 * MIN) done = true; } });
  assert.deepEqual([r.code, r.outcome], [0, 'released']);
  assert.deepEqual(ipc.map((x) => [x.m.type, x.sendsBefore]), [['ready', 1]]);   // 起動の知らせ (🟢) が届いた後に ready
  assert.match(sends[0].x, /^🟢 .*見張りを始めた/);
  const alerts = sends.filter((s) => s.x.startsWith('⚠️'));
  assert.deepEqual(alerts.map((s) => s.at / MIN), [45]);
  assert.match(alerts[0].x, /前の見回りで lock が無かった時から/);
  assert.doesNotMatch(alerts[0].x, /実際はもっと長い/);
  // 起動し直し: 見張りが死んで 20 分後に起動し直した (since = 10 分前に migrate を始めた)。lock は起動の見回りからもう持たれている = since から数える
  clock = 20 * MIN; const sends2 = [], ipc2 = [];
  const r2 = await runSupervised({ reader: { dbName: async () => 'cdb', read: async () => (clock < 40 * MIN ? H(502) : null) }, sinceMs: 10 * MIN, send: async (x) => { sends2.push({ at: clock, x }); return true; }, ipcSend: (m) => ipc2.push(m), isParentDone: () => false, now: () => clock, sleep: async (ms) => { clock += ms; } });
  assert.deepEqual(ipc2.map((m) => m.type), ['ready']);
  assert.match(sends2[0].x, /^🟡 .*起動し直した/);
  const a2 = sends2.filter((s) => s.x.startsWith('⚠️'));
  assert.equal(a2.length, 0, '10 分から数えて 40 分で外れた = 鳴らない');
  assert.equal(r2.outcome, 'released');
  // 単独の関数で、始めた時にもう持たれていた (CLI は単独で起動しない = 本番では起きない)
  const r3 = await sim((c) => (c < 50 * MIN ? H(404) : null));
  const a3 = r3.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.deepEqual(a3.map((s) => s.at / MIN), [45]);
  assert.match(a3[0].text, /実際はもっと長い/);
});
await t('U6 送れなかった ⚠️ は次の見回りで送り直す (届いた時だけ知らせた扱い)', async () => {
  const r = await sim((c) => (c < 60 * MIN ? H(505, { startVisible: true, heldMs: c }) : null), {}, (i) => i >= 2);
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.deepEqual(alerts.map((s) => [s.at / MIN, s.ok]), [[45, false], [46, false], [47, true]]);
  assert.ok(r.sends.at(-1).text.startsWith('✅'));
});
await t('T1 終わりの知らせ (✅・8 時間の ❌) = 届くまで数回 (5 回・30 秒おき) 送り直し、届けば exit 0 / 1 のまま・届かなければ exit 3', async () => {
  const held = (c) => (c < 60 * MIN ? H(601, { startVisible: true, heldMs: c }) : null);
  // ✅ が 2 回届かず 3 回目で届く
  let r = await sim(held, {}, (i, x) => !(x.startsWith('✅') && i < 3));
  const ends = r.sends.filter((s) => s.text.startsWith('✅'));
  assert.deepEqual(ends.map((s) => [s.at / MIN, s.ok]), [[60, false], [60.5, false], [61, true]]);
  assert.deepEqual([r.result.code, r.result.outcome, r.result.notifyFailed], [0, 'released', false]);
  // ✅ が 5 回とも届かない → exit 3
  r = await sim(held, {}, (i, x) => !x.startsWith('✅'));
  assert.equal(r.sends.filter((s) => s.text.startsWith('✅')).length, 5);
  assert.deepEqual([r.result.code, r.result.outcome, r.result.notifyFailed], [EXIT.NOTIFY_END_FAILED, 'released', true]);
  // 8 時間の ❌ が届かない → exit 3 (届けば 1)
  r = await sim(() => H(602), { maxHours: 2 }, (i, x) => !x.includes('打ち切る'));
  assert.equal(r.sends.filter((s) => s.text.includes('打ち切る')).length, 5);
  assert.deepEqual([r.result.code, r.result.outcome], [EXIT.NOTIFY_END_FAILED, 'max_hours']);
  r = await sim(() => H(603), { maxHours: 2 });
  assert.deepEqual([r.result.code, r.result.outcome], [EXIT.FAIL, 'max_hours']);
  assert.deepEqual(r.sends.map((s) => s.at / MIN), [45, 105, 120]);
  assert.match(r.sends.at(-1).text, /見張りを打ち切る \(2 時間\): lock はまだ pid 603/);
  // 鳴る前に外れた = 終わりの知らせは無い = 送り先が壊れていても exit 0
  r = await sim((c) => (c < 10 * MIN ? H(604) : null), {}, () => false);
  assert.deepEqual([r.result.code, r.sends.length], [0, 0]);
});
await t('U7 DB を 3 回続けて読めない → 「見張れていない」を 1 回だけ・読めなかった見回りは「lock が無かった」と数えない', async () => {
  const r = await sim((c) => (c < 10 * MIN ? null : c < 20 * MIN ? new Error('connection refused') : c < 70 * MIN ? H(606) : null));
  const fails = r.sends.filter((s) => /読めない/.test(s.text));
  assert.deepEqual(fails.map((s) => s.at / MIN), [12]);
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.deepEqual(alerts.map((s) => s.at / MIN), [54]);
});
await t('U9 持ち主が替わった (A → B) → A は「外れた」・B を続けて見張り、B は直前の見回りから数える', async () => {
  const r = await sim((c) => (c < 50 * MIN ? H(801) : c < 120 * MIN ? H(802) : null));
  assert.deepEqual(r.sends.map((s) => [s.at / MIN, s.text.slice(0, 1), /pid (\d+)/.exec(s.text)[1]]), [[45, '⚠', '801'], [50, '✅', '801'], [94, '⚠', '802'], [120, '✅', '802']]);
});
await t('U10 知らせの文に接続文字列・password を含まない', async () => {
  const txt = alertText({ h: H(9), elapsedMs: 46 * MIN, countFrom: 'prev_poll', dbName: 'cdb', alertMin: 45, repeat: false });
  assert.doesNotMatch(txt, /postgres:\/\/|password/i);
  assert.match(txt, /46 分持っている/);
});

await t('T2 (Codex R2 Medium 1) 持ち主が替わった時 (A の解放と B の取得が見回りの間) に A の「外れた」が 5 回とも届かない → B が外れた後に exit 3 / 届けば exit 0', async () => {
  const tl = (c) => (c < 50 * MIN ? H(811) : c < 60 * MIN ? H(812) : null);
  let r = await sim(tl, {}, (i, x) => !(x.startsWith('✅') && x.includes('pid 811')));
  assert.equal(r.sends.filter((s) => s.text.startsWith('✅') && s.text.includes('pid 811')).length, 5);
  assert.ok(r.sends.every((s) => !s.text.includes('pid 812') || !s.text.startsWith('✅')), 'B は鳴っていない = B の「外れた」は無い');
  assert.deepEqual([r.result.code, r.result.outcome, r.result.notifyFailed], [EXIT.NOTIFY_END_FAILED, 'released', true]);
  r = await sim(tl);
  assert.deepEqual([r.result.code, r.result.notifyFailed], [0, false]);
});
await t('P1 (Codex R2 Medium 2) 親が死んだ (IPC が切れた)・lock が無い → 知らせて終わる (exit 0) / 知らせが届かなければ exit 3', async () => {
  let gone = false;
  let r = await sim((c) => { if (c >= 3 * MIN) gone = true; return null; }, { supervised: true, isParentGone: () => gone });
  assert.deepEqual([r.result.outcome, r.result.code], ['parent_gone', 0]);
  assert.deepEqual(r.sends.map((s) => s.at / MIN), [3]);
  assert.match(r.sends[0].text, /親 \(migrate-watched\) が途中で終わった.*lock は今は無い = 見張りを終える/);
  gone = false;
  r = await sim((c) => { if (c >= 3 * MIN) gone = true; return null; }, { supervised: true, isParentGone: () => gone }, () => false);
  assert.deepEqual([r.result.outcome, r.result.code], ['parent_gone', EXIT.NOTIFY_END_FAILED]);
});
await t('P2 (Codex R2 Medium 2) 親が死んだ・lock がある → 1 回知らせて外れるまで見張る (45 分の ⚠️ も出す)・外れたら ✅ で exit 0', async () => {
  let gone = false;
  const r = await sim((c) => { if (c >= 5 * MIN) gone = true; return c >= 1 * MIN && c < 70 * MIN ? H(821) : null; }, { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } });
  assert.deepEqual(r.sends.map((s) => [s.at / MIN, s.text.startsWith('✅') ? '✅' : s.text.startsWith('⚠️') ? '⚠️' : '?']), [[5, '⚠️'], [45, '⚠️'], [70, '✅']]);
  assert.match(r.sends[0].text, /親 \(migrate-watched\) が途中で終わった.*pid 821 .*が持っている = 外れるまで見張る/);
  assert.match(r.sends[1].text, /45 分持っている/);
  assert.deepEqual([r.result.outcome, r.result.code], ['released', 0]);
});
await t('P3 (Codex R2 Medium 2) ready から 10 分 lock が一度も現れない (migrate が始まらない) → 知らせて exit 5 / lock の無い状態にも 8 時間の上限 (exit 1)', async () => {
  let r = await sim(() => null, { supervised: true });
  assert.deepEqual([r.result.outcome, r.result.code, r.sends.map((s) => s.at / MIN)], ['no_start', EXIT.NO_START, [10]]);
  assert.match(r.sends[0].text, /migrate が始まらない: 見張りの ready から 10 分 lock が現れない/);
  r = await sim(() => null, { supervised: true, noStartMin: 600, maxHours: 2 });
  assert.deepEqual([r.result.outcome, r.result.code], ['max_hours', EXIT.FAIL]);
  assert.match(r.sends.at(-1).text, /打ち切る \(2 時間\): lock は無い/);
});

await t('P4 (Codex R3 Medium 1) 親と migrate がほぼ同時に消えた (held → none と親の切れが同じ見回り) → 親の死を先に知らせる (外れたことも同じ文に)・exit 0 / 5 回とも届かない → exit 3', async () => {
  // 鳴る前 (10 分で消える)
  let gone = false;
  const tl = (until) => (c) => { if (c >= until * MIN) { gone = true; return null; } return c >= 1 * MIN ? H(831) : null; };
  let r = await sim(tl(10), { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } });
  assert.deepEqual([r.result.outcome, r.result.code, r.sends.length], ['parent_gone', 0, 1]);
  assert.match(r.sends[0].text, /親 \(migrate-watched\) が途中で終わった.*lock は今は無い \(pid 831 .*がおおよそ 10 分持っていた後に外れた\) = 見張りを終える/);
  assert.doesNotMatch(r.sends[0].text, /45 分の知らせを出していた/);
  // 鳴った後 (50 分で消える) = 「45 分の知らせを出していた」も同じ文に
  gone = false;
  r = await sim(tl(50), { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } });
  assert.deepEqual([r.result.outcome, r.result.code], ['parent_gone', 0]);
  assert.deepEqual(r.sends.map((x) => x.at / MIN), [45, 50]);
  assert.match(r.sends[1].text, /親 \(migrate-watched\) が途中で終わった.*45 分の知らせを出していた/);
  // その知らせが 5 回とも届かない → exit 3
  gone = false;
  r = await sim(tl(10), { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } }, () => false);
  assert.deepEqual([r.result.outcome, r.result.code, r.sends.length], ['parent_gone', EXIT.NOTIFY_END_FAILED, 5]);
});
await t('P5 (Codex R3 Medium 1) 親が切れた時に lock が残っていた ⚠️ が 5 回とも届かない → 覚えて、外れた後に exit 3 / 届けば exit 0', async () => {
  let gone = false;
  const tl = (c) => { if (c >= 5 * MIN) gone = true; return c >= 1 * MIN && c < 20 * MIN ? H(841) : null; };
  let r = await sim(tl, { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } }, (i, x) => !x.includes('外れるまで見張る'));
  assert.equal(r.sends.filter((x) => x.text.includes('外れるまで見張る')).length, 5);
  assert.deepEqual([r.result.outcome, r.result.code, r.result.notifyFailed], ['released', EXIT.NOTIFY_END_FAILED, true]);
  gone = false;
  r = await sim(tl, { supervised: true, isParentGone: () => gone, priorPoll: { atMs: 0, pid: null } });
  assert.deepEqual([r.result.outcome, r.result.code], ['released', 0]);
});

console.log('— 起動の知らせと親の下の見張り —');
const HOOK_ENV = { GCHAT_WEBHOOK_JOBS: 'https://chat.googleapis.com/v1/spaces/TEST/messages?key=k&token=t' };
await t('S1 起動の知らせが届かない (有効な https の URL・応答 404 = sendJobsChat が false) → ready を返さない・exit 4 / 届けば ready → 親の done で終わる', async () => {
  const calls = [];
  const send404 = (x) => sendJobsChat(x, { env: HOOK_ENV, fetchImpl: async (u, init) => { calls.push(JSON.parse(init.body).text); return { ok: false, status: 404 }; } });
  assert.equal(await send404('x'), false);
  calls.length = 0;
  const ipc = [];
  const r = await runSupervised({ reader: { dbName: async () => 'cdb', read: async () => null }, send: send404, ipcSend: (m) => ipc.push(m), isParentDone: () => false, sleep: async () => {} });
  assert.deepEqual([r.code, r.outcome, ipc.length, calls.length], [EXIT.NOTIFY_START_FAILED, 'start_notify_failed', 0, 3]);
  assert.match(calls[0], /^🟢/);
  // 届く (200) → ready → 親の done → lock が無い = 終わる
  let done = false; const ipc2 = [];
  const send200 = (x) => sendJobsChat(x, { env: HOOK_ENV, fetchImpl: async () => ({ ok: true, status: 200 }) });
  const r2 = await runSupervised({ reader: { dbName: async () => 'cdb', read: async () => null }, send: send200, ipcSend: (m) => { ipc2.push(m); }, isParentDone: () => done, sleep: async () => { done = true; } });
  assert.deepEqual([r2.code, r2.outcome, ipc2.map((m) => m.type)], [0, 'parent_done', ['ready']]);
});
await t('S2 始める前から別の migrate が lock を持っている → held (知らせは送らない・exit 1)', async () => {
  const ipc = [], sends = [];
  const r = await runSupervised({ reader: { dbName: async () => 'cdb', read: async () => H(901) }, send: async (x) => { sends.push(x); return true; }, ipcSend: (m) => ipc.push(m), isParentDone: () => false, sleep: async () => {} });
  assert.deepEqual([r.code, r.outcome, ipc.map((m) => m.type), ipc[0].pid, sends.length], [EXIT.FAIL, 'held_at_start', ['held'], 901, 0]);
});

console.log('— 1 つのコマンドの順番 (差し替え) —');
/** 差し替えの子のプロセス。plan = { ready: 'ready'|'held'|'exit'|'never', exitAfterReady: number|null (migrate の間に死ぬ = その code), doneExit: code } */
function fakes(plans, migrate = { code: 0 }) {
  const ev = [];
  let wi = 0;
  const deferred = () => { let r; const p = new Promise((x) => { r = x; }); return { p, r }; };
  const mig = deferred();
  const nonces = [];
  const startWatcher = ({ since, nonce }) => {
    const plan = plans[Math.min(wi, plans.length - 1)]; const i = ++wi;
    nonces.push(['watcher', nonce]);
    if (plan.ready === 'throw') throw new Error('spawn EPERM (試験)');
    ev.push(`watcher${i} start since=${since == null ? '-' : 'set'}`);
    const ready = deferred(), exited = deferred();
    setTimeout(() => {
      if (plan.ready === 'ready') { ev.push(`watcher${i} ready`); ready.r({ type: 'ready' }); }
      else if (plan.ready === 'held') { ready.r({ type: 'held', pid: 7 }); setTimeout(() => exited.r(1), 5); }
      else if (plan.ready === 'exit') exited.r(plan.code ?? 4);
      if (plan.ready === 'ready' && plan.exitAfterReady != null) setTimeout(() => { ev.push(`watcher${i} died`); exited.r(plan.exitAfterReady); }, 10);
    }, 5);
    return { ready: ready.p, exited: exited.p, done: () => { ev.push(`watcher${i} done`); setTimeout(() => exited.r(plan.doneExit ?? 0), 5); }, kill: () => { ev.push(`watcher${i} kill`); exited.r(1); } };
  };
  const startMigrate = ({ nonce }) => { nonces.push(['migrate', nonce]); if (migrate.throw) throw new Error('spawn ENOENT (試験)'); ev.push('migrate start'); setTimeout(() => { ev.push('migrate exit'); mig.r(migrate.code); }, migrate.ms ?? 60); return { exited: mig.p }; };
  const notes = [];
  return { ev, notes, nonces, deps: { startWatcher, startMigrate, notify: async (x) => { notes.push(x); return true; }, readyTimeoutMs: 200 } };
}
await t('O1 ready の後だけ migrate を始める・migrate が終われば done → 見張りの exit 0 で exit 0', async () => {
  const f = fakes([{ ready: 'ready' }]);
  const r = await runWatchedMigrate(f.deps);
  assert.deepEqual(r, { code: 0, reason: 'OK', migrateCode: 0, watcherCode: 0, restarts: 0 });
  assert.deepEqual(f.ev, ['watcher1 start since=-', 'watcher1 ready', 'migrate start', 'migrate exit', 'watcher1 done']);
});
await t('O2 ready が来ない (起動の知らせが届かない = exit 4) / held / 時間切れ → migrate を始めない (exit 1)', async () => {
  for (const [plan, re] of [[{ ready: 'exit', code: 4 }, /起動の知らせが GChat に届かない/], [{ ready: 'held' }, /別の migrate が lock を持っている/], [{ ready: 'never' }, /ready を返さない/]]) {
    const f = fakes([plan]);
    const r = await runWatchedMigrate(f.deps);
    assert.deepEqual([r.code, r.reason, f.ev.includes('migrate start')], [1, 'WATCH_NOT_READY', false], JSON.stringify(plan));
    assert.match(r.why, re);
  }
});
await t('O3 migrate の間に見張りが死んだ → 知らせて、migrate を始めた時刻 (since) で起動し直す・続けて exit 0 / 起動し直せない → 知らせて exit 3 (migrate は止めない)', async () => {
  let f = fakes([{ ready: 'ready', exitAfterReady: 1 }, { ready: 'ready' }], { code: 0, ms: 120 });
  let r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, r.restarts], [0, 'OK', 1]);
  assert.deepEqual(f.ev.filter((x) => /start|died|done/.test(x)), ['watcher1 start since=-', 'migrate start', 'watcher1 died', 'watcher2 start since=set', 'watcher2 done']);
  assert.match(f.notes[0], /見張りが migrate の間に止まった \(exit 1\) = 起動し直す \(1\/3\)/);
  f = fakes([{ ready: 'ready', exitAfterReady: 1 }, { ready: 'exit', code: 4 }], { code: 0, ms: 120 });
  r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, r.restarts, r.migrateCode], [3, 'WATCH_LOST', 3, 0]);
  assert.match(f.notes.at(-1), /3 回起動し直せなかった = 45 分の見張りが効いていない/);
  assert.ok(!f.ev.some((x) => /migrate kill/.test(x)));
});
await t('O4 migrate が失敗 → exit 1 / 見張りの終わりの知らせが届かない (見張り exit 3) → exit 3', async () => {
  let f = fakes([{ ready: 'ready' }], { code: 1 });
  let r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason], [1, 'MIGRATE_FAILED']);
  f = fakes([{ ready: 'ready', doneExit: 3 }]);
  r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, r.watcherCode], [3, 'WATCH_FAILED', 3]);
});
await t('O5 (Codex R2 Medium 2) 見張りが exit 5 (ready から 10 分 lock が現れない) で終わった → 起動し直さない・migrate の終わりを待って exit 3', async () => {
  const f = fakes([{ ready: 'ready', exitAfterReady: EXIT.NO_START }, { ready: 'ready' }], { code: 0, ms: 120 });
  const r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, r.restarts, r.migrateCode], [3, 'WATCH_LOST', 0, 0]);
  assert.equal(f.ev.filter((x) => /watcher2/.test(x)).length, 0);
});
await t('O6 (Codex R2 Low) 見張りを起動できない (例外) → migrate を始めない (exit 1) / migrate を起動できない (例外) → exit 1', async () => {
  let f = fakes([{ ready: 'throw' }]);
  let r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, f.ev.includes('migrate start')], [1, 'WATCH_NOT_READY', false]);
  assert.match(r.why, /見張りを起動できない \(spawn EPERM/);
  f = fakes([{ ready: 'ready' }], { throw: true });
  r = await runWatchedMigrate(f.deps);
  assert.deepEqual([r.code, r.reason, r.migrateCode], [1, 'MIGRATE_FAILED', 1]);
});
await t('O7 (Codex R2 High) run の nonce (16 桁の hex) を見張り・起動し直した見張り・migrate に同じに渡す・run ごとに違う', async () => {
  const f = fakes([{ ready: 'ready', exitAfterReady: 1 }, { ready: 'ready' }], { code: 0, ms: 120 });
  await runWatchedMigrate(f.deps);
  assert.deepEqual(f.nonces.map((x) => x[0]), ['watcher', 'migrate', 'watcher']);
  assert.match(f.nonces[0][1], /^[0-9a-f]{16}$/);
  assert.equal(new Set(f.nonces.map((x) => x[1])).size, 1);
  const g = fakes([{ ready: 'ready' }]);
  await runWatchedMigrate(g.deps);
  assert.notEqual(g.nonces[0][1], f.nonces[0][1]);
  assert.match(newRunNonce(), /^[0-9a-f]{16}$/);
  assert.ok(`${MIGRATE_LOCK_WATCH_APPLICATION_NAME}:${newRunNonce()}:9999999999`.length <= 63, 'application_name は 63 バイトまで');
});

// ─── 本物の PG (embedded-postgres) ───
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の見張りの試験を流せない (飛ばさない)。リポジトリで npm ci');
  process.exit(1);
}
const clusterDir = path.join(os.tmpdir(), `cdb-migrate-lock-watch-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new loaded.EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
const suUrl = (db = 'postgres') => `postgres://postgres:${SU_PW}@127.0.0.1:${port}/${db}`;
const hex = crypto.randomBytes(4).toString('hex');
const WATCHER = `w45_watch_${hex}`, RUNNER = `w45_run_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const roleUrl = (role, db = 'postgres') => `postgres://${role}:${PW}@127.0.0.1:${port}/${db}`;
const quiet = () => {};
const BIG_DISK = async () => ({ ok: true, capacityBytes: 100 * 1024 ** 3, usedBytes: 1024 ** 3 });
const tmpDirs = [];
const mkDir = (files) => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'd60w45-mig-')); tmpDirs.push(d); for (const [n, x] of Object.entries(files)) fs.writeFileSync(path.join(d, n), x); return d; };
const BASE = `create schema app;
create table app.t (id bigint primary key, a text not null, b int not null);
insert into app.t select g, 'x' || g, g % 100 from generate_series(1, 2000) g;
analyze app.t;
`;
const CI_SQL = `-- migrate:concurrent-index
create index concurrently if not exists t_b_idx on app.t (b);
`;
// 試験の子のプロセスに本物の送り先・本番の接続を渡さない (リポジトリ直下の .env は cwd = tmp で読まない)
const cleanEnv = (extra = {}) => { const e = { ...process.env }; for (const k of ['GCHAT_WEBHOOK_JOBS', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_URL', 'RENDER_API_KEY', 'CDB_RENDER_PG_RESOURCE_ID', LOCK_WATCH_NONCE_ENV]) delete e[k]; return { ...e, ...extra }; };
let cleanupFailed = false;
console.log('— 本物の PG (embedded-postgres ' + PINNED_EMBEDDED_PG + ' / ' + loaded.from + ') —');
await cluster.initialise();
await cluster.start();
const conns = [];
const open = async (url, extra) => { const c = await openPgClient(url, extra); c.on('error', () => {}); conns.push(c); return c; };
try {
  const su = await open(suUrl());
  // watcher = 照会用の役割の代わり (LOGIN だけ・pg_read_all_stats なし) / runner = migrate を流す役割の代わり (deployer)
  await su.query(`create role ${WATCHER} login password '${PW}'`);
  await su.query(`create role ${RUNNER} login password '${PW}' createdb`);
  await su.query('create database w45_other');
  let dbSeq = 0;
  const newDb = async () => { const n = `w45_db_${++dbSeq}`; await su.query(`create database ${n} owner ${RUNNER}`); return n; };

  await t('R1 権限の無い役割から: 別の役割の runner (withMigrateLock) が持つ lock の pid・application_name は見え、backend_start は見えない / 同じ役割なら見える / describeLockHolder と同じ pid', async () => {
    const run = await open(roleUrl(RUNNER));
    const runPid = (await run.query('select pg_backend_pid() as p')).rows[0].p;
    const w = await open(roleUrl(WATCHER));
    const same = await open(roleUrl(RUNNER));
    assert.equal(await readLockHolder(w), null);
    let release; const held = new Promise((r) => { release = r; });
    let inside; const entered = new Promise((r) => { inside = r; });
    const p = withMigrateLock(pgAdapter(run), async () => { inside(); await held; return 'done'; });
    await entered;
    try {
      const hw = await readLockHolder(w);
      assert.deepEqual([hw.pid, hw.applicationName, hw.startVisible, hw.heldMs], [runPid, 'company-db-migrate', false, null]);
      const hs = await readLockHolder(same);
      assert.equal(hs.pid, runPid); assert.equal(hs.startVisible, true); assert.ok(hs.heldMs >= 0 && hs.heldMs < 60000, `heldMs = ${hs.heldMs}`);
      assert.deepEqual((await describeLockHolder(pgAdapter(su))).map((x) => x.pid), [runPid]);
    } finally { release(); }
    assert.equal(await p, 'done');
    assert.equal(await readLockHolder(w), null);
  });

  await t('R2 別の DB で同じ鍵の lock を持っていても数えない', async () => {
    const other = await open(suUrl('w45_other'));
    await other.query('select pg_advisory_lock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]);
    assert.equal(await readLockHolder(await open(roleUrl(WATCHER))), null);
    assert.ok(await readLockHolder(await open(roleUrl(WATCHER, 'w45_other'))));
    await other.query('select pg_advisory_unlock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]);
  });

  await t('R3 本物の PG: runner の withMigrateLock が持つ間に鳴り (前の見回りから数える)、外れたら「外れた」を送って終わる', async () => {
    const run = await open(roleUrl(RUNNER));
    const w = await open(roleUrl(WATCHER));
    const sends = [];
    const watch = runLockWatch({ readHolder: () => readLockHolder(w), send: async (x) => { sends.push(x); return true; }, dbName: 'postgres', intervalSec: 0.2, alertMin: 1.5 / 60, repeatMin: 60, waitStartMin: 1, maxHours: 1 });
    await sleep(600);
    await withMigrateLock(pgAdapter(run), () => sleep(3000));
    const r = await watch;
    assert.deepEqual([r.outcome, r.code, sends.length], ['released', 0, 2], sends.join('\n---\n'));
    assert.ok(sends[0].startsWith(`⚠️ Company DB の migrate の lock (${MIGRATE_LOCK_NAME})`), sends[0]);
    assert.match(sends[0], /前の見回りで lock が無かった時から/);
    assert.match(sends[1], /^✅/);
  });

  await t('R4 見張りの読み手は lock を取らない・application_name で見える・同じ設定 (default_transaction_read_only) の接続では書けない', async () => {
    const reader = pgHolderReader(roleUrl(WATCHER));
    assert.equal(await reader.dbName(), 'postgres');
    assert.equal(await reader.read(), null);
    assert.equal((await describeLockHolder(pgAdapter(su))).length, 0);
    assert.equal((await su.query('select pid from pg_stat_activity where application_name = $1', [WATCH_APPLICATION_NAME])).rows.length, 1);
    assert.equal((await su.query(`select count(*)::int as n from pg_locks l join pg_stat_activity a on a.pid = l.pid where a.application_name = $1 and l.locktype = 'advisory'`, [WATCH_APPLICATION_NAME])).rows[0].n, 0);
    await reader.close();
    const w = await open(roleUrl(WATCHER));
    await w.query('set default_transaction_read_only = on');
    await assert.rejects(w.query('create temp table w45_x (a int)'), /read-only/);
  });

  // ─── runner の関門 (LOCK_WATCH_REQUIRED) ───
  const dbX = await newDb();
  const cx = await open(roleUrl(RUNNER, dbX));
  await migrateWithLock(pgAdapter(cx), { dir: mkDir({ '0001_base.sql': BASE }), log: quiet });
  await cx.query('create index concurrently if not exists t_b_idx on app.t (b)');
  const EXPECT = await buildIndexExpect(pgAdapter(cx), { version: '0002', name: 'idx', file: '0002_idx.sql', text: CI_SQL, concurrentIndex: true });
  const ciDir = () => mkDir({ '0001_base.sql': BASE, '0002_idx.sql': CI_SQL, '0002_idx.expect.json': JSON.stringify(EXPECT, null, 2) });
  const versions = async (c) => (await c.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);

  /** 本物の見張りと同じ heartbeat を出す試験の接続 (writeLockWatchHeartbeat = 見張りの読み手と同じ関数)。stop で止める */
  const startHb = async (url, nonce, everyMs = 1000) => {
    const c = await open(url);
    await writeLockWatchHeartbeat(c, nonce);
    const tm = setInterval(() => { writeLockWatchHeartbeat(c, nonce).catch(() => {}); }, everyMs);
    return { c, stop: async () => { clearInterval(tm); try { await c.end(); } catch { /* */ } } };
  };
  const setAppName = (c, name) => c.query("select pg_catalog.set_config('application_name', $1, false)", [name]);
  const nowEpoch = async () => Number((await su.query('select floor(extract(epoch from clock_timestamp()))::bigint as e')).rows[0].e);

  await t('R5 heartbeat の契約: 見張りの読み手は見回りのたびに application_name に nonce と epoch を書く・runner は同じ DB・同じ nonce・120 秒以内だけ通す', async () => {
    const dbH = await newDb();
    const run = pgAdapter(await open(roleUrl(RUNNER, dbH)));
    const nonce = newRunNonce(), other = newRunNonce();
    const reader = pgHolderReader(roleUrl(WATCHER, dbH), { nonce });
    try {
      await assert.rejects(assertLockWatchFresh(run, { nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED' && /この run の見張りの接続が同じ DB に無い/.test(e.message));
      await reader.dbName();   // 接続しただけ (見回りをしていない = heartbeat の無い名前の idle の接続)
      assert.equal((await su.query('select count(*)::int as n from pg_stat_activity where application_name = $1 and datname = $2', [WATCH_APPLICATION_NAME, dbH])).rows[0].n, 1);
      await assert.rejects(assertLockWatchFresh(run, { nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');
      await reader.read();     // 見回り 1 回 = heartbeat
      const names = (await su.query('select application_name as a from pg_stat_activity where datname = $1 and application_name like $2', [dbH, WATCH_APPLICATION_NAME + ':%'])).rows.map((x) => x.a);
      assert.equal(names.length, 1); assert.match(names[0], new RegExp('^' + WATCH_APPLICATION_NAME + ':' + nonce + ':\\d{10}$'));
      assert.equal((await assertLockWatchFresh(run, { nonce })).fresh, 1);
      await assert.rejects(assertLockWatchFresh(run, { nonce: other }), (e) => e.code === 'LOCK_WATCH_REQUIRED');   // 別の run
      await assert.rejects(assertLockWatchFresh(run, { nonce: null }), (e) => /nonce .*が無い/.test(e.message));
      await assert.rejects(assertLockWatchFresh(run, { nonce: 'XYZ' }), (e) => /形が違う/.test(e.message));
      await assert.rejects(assertLockWatchFresh(pgAdapter(await open(roleUrl(RUNNER, 'w45_other'))), { nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');   // 別の DB
      // env の nonce (migrate-watched が子に渡す形)
      process.env[LOCK_WATCH_NONCE_ENV] = nonce;
      try { assert.equal((await assertLockWatchFresh(run)).fresh, 1); } finally { delete process.env[LOCK_WATCH_NONCE_ENV]; }
      // 止まった見張り = heartbeat が 121 秒前のまま / 未来の epoch (6 秒先) = 通らない・120 秒ちょうどは通る
      const fake = await open(roleUrl(WATCHER, dbH));
      await reader.close();
      const e0 = await nowEpoch();
      await setAppName(fake, `${WATCH_APPLICATION_NAME}:${nonce}:${e0 - LOCK_WATCH_HEARTBEAT_MAX_AGE_SEC - 1}`);
      await assert.rejects(assertLockWatchFresh(run, { nonce }), (e) => /heartbeat が 12[1-3] 秒前で古い/.test(e.message));
      await setAppName(fake, `${WATCH_APPLICATION_NAME}:${nonce}:${e0 + 30}`);
      await assert.rejects(assertLockWatchFresh(run, { nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');
      await setAppName(fake, `${WATCH_APPLICATION_NAME}:${nonce}:${(await nowEpoch()) - 100}`);
      assert.equal((await assertLockWatchFresh(run, { nonce })).fresh, 1);
      await setAppName(fake, `${WATCH_APPLICATION_NAME}:${nonce}:x${e0}`);   // 形が違う名前は数えない (cast の誤りで落ちない)
      await assert.rejects(assertLockWatchFresh(run, { nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');
    } finally { await reader.close(); }
  });

  await t('G1 runner の関門は本物の PG の CIC の道で既定で必須: opts の無い migrateWithLock の直の呼び出し・supportsConcurrentIndex を false にした本物の PG の adapter・別の run の heartbeat = LOCK_WATCH_REQUIRED (何も作らない・記録しない・lock が残らない) / 新しい heartbeat なら流れる / 見張りが止まって古くなれば次の file で止まる', async () => {
    const nonce = newRunNonce();
    // ① heartbeat が無い (opts を渡さない直の呼び出し)
    let dbG = await newDb();
    let c = await open(roleUrl(RUNNER, dbG));
    await assert.rejects(migrateWithLock(pgAdapter(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED' && /0002_idx\.sql/.test(e.message) && /migrate-watched\.mjs/.test(e.message));
    assert.deepEqual(await versions(c), ['0001']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_b_idx'), null);
    assert.deepEqual(await describeLockHolder(pgAdapter(su)), []);
    // ② 本物の PG の adapter で CONCURRENTLY を外す道 (supportsConcurrentIndex = false) も同じ
    await assert.rejects(migrateWithLock({ ...pgAdapter(c), supportsConcurrentIndex: false }, { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');
    assert.deepEqual(await versions(c), ['0001']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_b_idx'), null);
    // ③ 別の run の heartbeat は数えない
    const hbOther = await startHb(roleUrl(WATCHER, dbG), newRunNonce());
    try {
      await assert.rejects(migrateWithLock(pgAdapter(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED');
    } finally { await hbOther.stop(); }
    // ④ この run の新しい heartbeat (本物の見張りの読み手) → 流れる
    const reader = pgHolderReader(roleUrl(WATCHER, dbG), { nonce });
    await reader.read();
    try {
      const r = await migrateWithLock(pgAdapter(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce });
      assert.deepEqual(r.applied, ['0002']);
      assert.ok((await readIndexAttrs(pgAdapter(c), 'app', 't_b_idx')).valid);
    } finally { await reader.close(); }
    // ⑤ 見張りが止まった (heartbeat が古い) → 次の file で止まる・前の file は記録済みのまま
    dbG = await newDb();
    c = await open(roleUrl(RUNNER, dbG));
    const stuck = await open(roleUrl(WATCHER, dbG));
    await setAppName(stuck, `${WATCH_APPLICATION_NAME}:${nonce}:${(await nowEpoch()) - 300}`);
    await assert.rejects(migrateWithLock(pgAdapter(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce }), (e) => e.code === 'LOCK_WATCH_REQUIRED' && /古い/.test(e.message));
    assert.deepEqual(await versions(c), ['0001']);
    assert.deepEqual(await describeLockHolder(pgAdapter(su)), []);
  });
  await t('G2 外す道は PGlite の adapter (lockWatchExempt) だけ: pgAdapter は持たない・runner に外す opts は無い (requireLockWatch・cliRunOptions は無い)・heartbeat を書く関数を試験の外で呼ぶのは見張りだけ', async () => {
    assert.equal(pgAdapter(su).lockWatchExempt, undefined);
    const { PGlite } = await import('@electric-sql/pglite');
    const lite = new PGlite();
    try { assert.equal(pgliteAdapter(lite).lockWatchExempt, true); } finally { await lite.close(); }
    const src = fs.readFileSync(path.join(ROOT, 'scripts', 'company-db', 'migrate.mjs'), 'utf8');
    assert.equal((src.match(/lockWatchExempt: true/g) || []).length, 1, 'lockWatchExempt: true は pgliteAdapter の 1 か所だけ');
    assert.ok(/export function pgliteAdapter[\s\S]{0,600}lockWatchExempt: true/.test(src));
    assert.ok(!/requireLockWatch|cliRunOptions|skipLockWatch|lockWatchOptional/.test(src), '外す opts の名前が無い');
    // 取引の外の CIC の各文の前 (1)・取引の中の道の前 (1)・取引の中の道の各文の前 (1・inTx) = 3 か所 (🆕 Codex R3 M2)
    assert.equal((src.match(/await assertLockWatchFresh\(db, \{ nonce: opts\.lockWatchNonce \?\? process\.env\[LOCK_WATCH_NONCE_ENV\], label: f\.file \}\)/g) || []).length, 1, '取引の外の CIC の各文の前');
    assert.equal((src.match(/if \(lockWatch\) await assertLockWatchFresh\(db, \{ nonce: lockWatch\.nonce, label: f\.file \}\)/g) || []).length, 1, '取引の中の道の前');
    assert.equal((src.match(/if \(lockWatch\) await assertLockWatchFresh\(db, \{ nonce: lockWatch\.nonce, label: f\.file, inTx: true \}\)/g) || []).length, 1, '取引の中の道の各文の前');
    assert.ok(/const lockWatch = db\.lockWatchExempt === true \? null : \{ nonce: opts\.lockWatchNonce \?\? process\.env\[LOCK_WATCH_NONCE_ENV\] \}/.test(src), '外すのは lockWatchExempt だけ');
    const hits = [];
    const walk = (d) => { for (const e of fs.readdirSync(d, { withFileTypes: true })) { if (['node_modules', '.git'].includes(e.name)) continue; const q = path.join(d, e.name); if (e.isDirectory()) walk(q); else if (/\.(m?js)$/.test(e.name) && !/^test-/.test(e.name) && fs.readFileSync(q, 'utf8').includes('writeLockWatchHeartbeat')) hits.push(path.relative(ROOT, q).replace(/\\/g, '/')); } };
    for (const d of ['apps', 'lib', 'scripts', 'config']) walk(path.join(ROOT, d));
    assert.deepEqual(hits, ['scripts/company-db/migrate-lock-watch.mjs']);
  });

  await t('G3 (Codex R3 Medium 2) 本物の PG の adapter で CONCURRENTLY を外す道 (取引の中) も各文の直前に heartbeat を確かめる: 1 文目の後に古くなると 2 文目の前で止まり、取引ごと巻き戻して記録しない / 新しいままなら 2 文とも作る', async () => {
    const CI2_SQL = '-- migrate:concurrent-index\ncreate index concurrently if not exists t_b2_idx on app.t (b);\ncreate index concurrently if not exists t_a2_idx on app.t (a);\n';
    await cx.query('create index concurrently if not exists t_b2_idx on app.t (b)');
    await cx.query('create index concurrently if not exists t_a2_idx on app.t (a)');
    const EXPECT2 = await buildIndexExpect(pgAdapter(cx), { version: '0002', name: 'idx2', file: '0002_idx2.sql', text: CI2_SQL, concurrentIndex: true });
    const dir2 = () => mkDir({ '0001_base.sql': BASE, '0002_idx2.sql': CI2_SQL, '0002_idx2.expect.json': JSON.stringify(EXPECT2, null, 2) });
    const nonce = newRunNonce();
    /** 1 文目 (t_b2_idx) を流した直後に heartbeat を古くする adapter (取引の中の道) */
    const staleAfterFirst = (c, hb) => {
      const base = pgAdapter(c); const seen = [];
      return { seen, adapter: { ...base, supportsConcurrentIndex: false, exec: async (q) => {
        const r = await base.exec(q);
        if (/^\s*create\s+index\s+if\s+not\s+exists\s+t_b2_idx/i.test(q)) { seen.push('b2'); await setAppName(hb, `${WATCH_APPLICATION_NAME}:${nonce}:${(await nowEpoch()) - LOCK_WATCH_HEARTBEAT_MAX_AGE_SEC - 30}`); }
        if (/^\s*create\s+index\s+if\s+not\s+exists\s+t_a2_idx/i.test(q)) seen.push('a2');
        return r;
      } } };
    };
    // ① 1 文目の後に古くなる → 2 文目の前で止まる
    let dbT = await newDb();
    let c = await open(roleUrl(RUNNER, dbT));
    let hb = await open(roleUrl(WATCHER, dbT));
    await writeLockWatchHeartbeat(hb, nonce);   // 新しい heartbeat (1 回だけ・見回りは続けない = 止まった見張り)
    const x = staleAfterFirst(c, hb);
    await assert.rejects(migrateWithLock(x.adapter, { dir: dir2(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce }),
      (e) => e.code === 'LOCK_WATCH_REQUIRED' && /0002_idx2\.sql/.test(e.message) && /古い/.test(e.message) && /巻き戻した/.test(e.message));
    assert.deepEqual(x.seen, ['b2'], '2 文目を流していない');
    assert.deepEqual(await versions(c), ['0001']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_b2_idx'), null, '1 文目の index も巻き戻った');
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a2_idx'), null);
    assert.deepEqual(await describeLockHolder(pgAdapter(su)), []);
    // ② heartbeat が新しいまま (本物の見張りの読み手で見回る) → 2 文とも作って記録
    dbT = await newDb();
    c = await open(roleUrl(RUNNER, dbT));
    const reader = pgHolderReader(roleUrl(WATCHER, dbT), { nonce });
    await reader.read();
    try {
      const r = await migrateWithLock({ ...pgAdapter(c), supportsConcurrentIndex: false }, { dir: dir2(), log: quiet, readDiskMetrics: BIG_DISK, lockWatchNonce: nonce });
      assert.deepEqual(r.applied, ['0001', '0002']);
      assert.ok((await readIndexAttrs(pgAdapter(c), 'app', 't_b2_idx')).valid);
      assert.ok((await readIndexAttrs(pgAdapter(c), 'app', 't_a2_idx')).valid);
    } finally { await reader.close(); }
    await cx.query('drop index concurrently if exists app.t_b2_idx');
    await cx.query('drop index concurrently if exists app.t_a2_idx');
  });

  // ─── 本物の子のプロセス (migrate-watched の本体 + 見張りの CLI + migrate.mjs の CLI) ───
  await t('E1 本物の子のプロセス: 起動の知らせ (dry-run で画面) → ready の後に migrate (この run の heartbeat がある) → ⚠️ → ✅ → exit 0', async () => {
    const dbE = await newDb();
    const su2 = await open(suUrl(dbE));
    const dir = mkDir({ '0001_base.sql': BASE, '0002_slow.sql': 'select pg_sleep(5);\n' });
    let watchOut = '', migOut = '', readyAt = null, migStartAt = null, freshAtStart = null;
    const nonce = newRunNonce();
    const r = await runWatchedMigrate({
      nonce,
      startWatcher: ({ since, nonce: n }) => {
        const w = forkWatcher({ since, nonce: n, extraArgs: ['--dry-run', '--interval-sec', '1', '--alert-min', String(2 / 60)], env: cleanEnv({ COMPANY_DB_WATCH_URL: roleUrl(WATCHER, dbE) }), stdio: 'pipe' });
        w.child.stdout.on('data', (d) => { watchOut += d; }); w.child.stderr.on('data', (d) => { watchOut += d; });
        w.ready.then(() => { readyAt = Date.now(); });
        return w;
      },
      startMigrate: ({ nonce: n }) => {
        migStartAt = Date.now();
        freshAtStart = assertLockWatchFresh(pgAdapter(su2), { nonce: n }).then((x) => x.fresh, (e) => e.message);
        const m = spawnMigrate({ args: ['--dir', dir], nonce: n, env: cleanEnv({ COMPANY_DB_URL: roleUrl(RUNNER, dbE) }), stdio: 'pipe' });
        m.child.stdout.on('data', (d) => { migOut += d; }); m.child.stderr.on('data', (d) => { migOut += d; });
        return m;
      },
      notify: async () => { throw new Error('親の知らせは呼ばれないはず'); },
    });
    assert.deepEqual([r.code, r.reason, r.migrateCode, r.watcherCode], [0, 'OK', 0, 0], watchOut + '\n' + migOut);
    assert.ok(readyAt && migStartAt && readyAt <= migStartAt, 'ready の前に migrate を始めた');
    assert.equal(await freshAtStart, 1, 'migrate を始めた時にこの run の heartbeat がある');
    assert.match(watchOut, /\(dry-run・送らない\)\n🟢 Company DB の migrate の lock の見張りを始めた/);
    assert.match(watchOut, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の lock/);
    assert.match(watchOut, /前の見回りで lock が無かった時から/);
    assert.match(watchOut, /\(dry-run・送らない\)\n✅/);
    assert.match(migOut, /applied=2/);
    assert.doesNotMatch(watchOut + migOut, new RegExp(PW));
  });
  await t('E2 本物の子のプロセス: 起動の知らせが届かない (届かない https の送り先) → 見張りは exit 4・migrate を始めない (記録の表も無い)', async () => {
    const dbE = await newDb();
    let started = false, watchOut = '';
    const r = await runWatchedMigrate({
      startWatcher: ({ since, nonce }) => { const w = forkWatcher({ since, nonce, env: cleanEnv({ COMPANY_DB_WATCH_URL: roleUrl(WATCHER, dbE), GCHAT_WEBHOOK_JOBS: 'https://127.0.0.1:9/never' }), stdio: 'pipe' }); w.child.stdout.on('data', (d) => { watchOut += d; }); w.child.stderr.on('data', (d) => { watchOut += d; }); return w; },
      startMigrate: () => { started = true; throw new Error('migrate を始めた'); },
      notify: async () => true,
    });
    assert.deepEqual([r.code, r.reason, started], [1, 'WATCH_NOT_READY', false], watchOut);
    assert.match(r.why, /exit 4 = 起動の知らせが GChat に届かない/);
    const c = await open(roleUrl(RUNNER, dbE));
    assert.equal((await c.query(`select to_regclass('ops.schema_migrations') as t`)).rows[0].t, null);
  });

  await t('E3 (Codex R2 Medium 2) 本物の子のプロセス: 親を kill → 見張りは IPC の切れを見て、lock が無ければ知らせて exit 0 / lock があれば知らせて外れるまで見張り ✅ で exit 0', async () => {
    const dbP = await newDb();
    const parentSrc = `import { forkWatcher } from ${JSON.stringify(pathToFileURL(WATCHED_CLI).href)};
const w = forkWatcher({ nonce: process.env.T_NONCE, extraArgs: ['--dry-run', '--interval-sec', '1', '--alert-min', String(2 / 60)] });
w.ready.then((m) => console.log('READY ' + m.type + ' ' + w.pid));
setInterval(() => {}, 1000);`;
    const parentFile = path.join(mkDir({}), 'd60w45-parent.mjs');
    fs.writeFileSync(parentFile, parentSrc);
    const runCase = async (holdLock) => {
      const parent = spawn(process.execPath, [parentFile], { cwd: os.tmpdir(), env: cleanEnv({ COMPANY_DB_WATCH_URL: roleUrl(WATCHER, dbP), T_NONCE: newRunNonce() }), stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '';
      parent.stdout.on('data', (d) => { out += d; }); parent.stderr.on('data', (d) => { out += d; });
      const end = Date.now() + 30000;
      while (!/READY ready (\d+)/.test(out) && Date.now() < end) await sleep(100);
      const wpid = Number((/READY ready (\d+)/.exec(out) || [])[1]);
      assert.ok(wpid, out);
      let holder = null;
      if (holdLock) { holder = await open(roleUrl(RUNNER, dbP)); await holder.query('select pg_advisory_lock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]); await sleep(1500); }
      parent.kill();   // Windows = TerminateProcess。見張りは detached = 親の job に入らず残る・IPC が切れる (detached でなければ一緒に終わる = 前の実験)
      const alive = () => { try { process.kill(wpid, 0); return true; } catch { return false; } };
      if (holdLock) {
        await sleep(4000);
        assert.ok(alive(), '親が死んでも lock がある間は見張りを続ける');
        await holder.query('select pg_advisory_unlock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]);
      }
      const end2 = Date.now() + 30000;
      while (alive() && Date.now() < end2) await sleep(200);
      await sleep(300);
      assert.ok(!alive(), '見張りが終わらない\n' + out);
      return out;
    };
    let out = await runCase(false);
    assert.match(out, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の見張りの親 \(migrate-watched\) が途中で終わった.*lock は今は無い = 見張りを終える/);
    assert.match(out, /終わり: parent_gone/);
    out = await runCase(true);
    assert.match(out, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の見張りの親 \(migrate-watched\) が途中で終わった.*が持っている = 外れるまで見張る/);
    assert.match(out, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の lock/);
    assert.match(out, /\(dry-run・送らない\)\n✅/);
    assert.match(out, /終わり: released/);
  });
  await t('L1 (Codex R2 Low) fork / spawn を起動できない (error event) → 未処理の誤りで落ちずに exit 1 に変わる', async () => {
    const logs = [];
    const w = forkWatcher({ nonce: newRunNonce(), execPath: path.join(os.tmpdir(), 'd60w45-no-such-node.exe'), stdio: 'pipe', log: (m) => logs.push(m) });
    assert.equal(await Promise.race([w.exited, sleep(15000).then(() => 'timeout')]), 1);
    const m = spawnMigrate({ nonce: newRunNonce(), execPath: path.join(os.tmpdir(), 'd60w45-no-such-node.exe'), stdio: 'pipe', log: (x) => logs.push(x) });
    assert.equal(await Promise.race([m.exited, sleep(15000).then(() => 'timeout')]), 1);
    assert.equal(logs.length, 2, logs.join('\n'));
    assert.match(logs[0], /見張り のプロセスを起動できない/); assert.match(logs[1], /migrate のプロセスを起動できない/);
    // runWatchedMigrate の中で起動できない見張り = migrate を始めない
    let started = false;
    const r = await runWatchedMigrate({ startWatcher: ({ since, nonce }) => forkWatcher({ since, nonce, execPath: path.join(os.tmpdir(), 'd60w45-no-such-node.exe'), stdio: 'pipe', log: () => {} }), startMigrate: () => { started = true; return { exited: Promise.resolve(0) }; }, notify: async () => true });
    assert.deepEqual([r.code, r.reason, started], [1, 'WATCH_NOT_READY', false]);
  });
  await t('C1 CLI: 見張りは単独で起動しない・--url・--alert-min (dry-run でない) は exit 2 / migrate-watched は送り先・接続先が無い・--url・--dry-run で exit 2', async () => {
    const env = cleanEnv({ COMPANY_DB_WATCH_URL: 'postgres://nobody:x@127.0.0.1:1/none', COMPANY_DB_URL: 'postgres://nobody:x@127.0.0.1:1/none', GCHAT_WEBHOOK_JOBS: 'https://127.0.0.1:9/never' });
    const run = (cli, args, e = env) => spawnSync(process.execPath, [cli, ...args], { cwd: os.tmpdir(), env: e, encoding: 'utf8', timeout: 60000 });
    let r = run(WATCH_CLI, []);
    assert.equal(r.status, 2, r.stdout + r.stderr); assert.match(r.stderr, /単独では起動しない/);
    r = run(WATCH_CLI, ['--supervised']);   // IPC が無い
    assert.equal(r.status, 2, r.stdout + r.stderr); assert.match(r.stderr, /単独では起動しない/);
    for (const args of [['--url', 'postgres://a:b@c/d'], ['--alert-min', '90'], ['--supervised', '--interval-sec', '3600']]) { r = run(WATCH_CLI, args); assert.equal(r.status, 2, args.join(' ') + r.stderr); }
    r = run(WATCH_CLI, ['--dry-run'], cleanEnv());
    assert.equal(r.status, 2); assert.match(r.stderr, /COMPANY_DB_WATCH_URL/);
    r = run(WATCH_CLI, ['--dry-run'], cleanEnv({ COMPANY_DB_WATCH_URL: 'postgres://nobody:x@127.0.0.1:1/none', [LOCK_WATCH_NONCE_ENV]: 'not-hex' }));
    assert.equal(r.status, 2); assert.match(r.stderr, /形が違う/);
    // 親の下 (IPC あり) で nonce が無い = exit 2
    const w = forkWatcher({ nonce: 'bad', env: cleanEnv({ COMPANY_DB_WATCH_URL: 'postgres://nobody:x@127.0.0.1:1/none', GCHAT_WEBHOOK_JOBS: 'https://127.0.0.1:9/never' }), stdio: 'pipe' });
    let wout = ''; w.child.stderr.on('data', (d) => { wout += d; });
    assert.equal(await w.exited, 2, wout); assert.match(wout, /nonce/);
    r = run(WATCHED_CLI, [], cleanEnv({ COMPANY_DB_URL: 'postgres://x@h/d', COMPANY_DB_WATCH_URL: 'postgres://x@h/d' }));
    assert.equal(r.status, 2, r.stdout + r.stderr); assert.match(r.stderr, /GCHAT_WEBHOOK_JOBS/);
    r = run(WATCHED_CLI, [], cleanEnv({ GCHAT_WEBHOOK_JOBS: 'https://127.0.0.1:9/never' }));
    assert.equal(r.status, 2); assert.match(r.stderr, /COMPANY_DB_URL・COMPANY_DB_WATCH_URL/);
    for (const args of [['--url', 'postgres://a:b@c/d'], ['--dry-run'], ['--list']]) { r = run(WATCHED_CLI, args); assert.equal(r.status, 2, args.join(' ') + r.stderr); }
  });

  await t('C2 見張りの CLI --dry-run (単独): 本物の PG で鳴る文を画面に出し、外れたら exit 0・password を出さない', async () => {
    const child = spawn(process.execPath, [WATCH_CLI, '--dry-run', '--interval-sec', '1', '--alert-min', String(2 / 60)], { cwd: os.tmpdir(), env: cleanEnv({ COMPANY_DB_WATCH_URL: roleUrl(WATCHER) }) });
    let out = '';
    child.stdout.on('data', (d) => { out += d; }); child.stderr.on('data', (d) => { out += d; });
    const exited = new Promise((r) => child.on('exit', (code) => r(code)));
    await sleep(2500);
    await withMigrateLock(pgAdapter(await open(roleUrl(RUNNER))), () => sleep(5000));
    const code = await Promise.race([exited, sleep(30000).then(() => 'timeout')]);
    if (code === 'timeout') child.kill();
    assert.equal(code, 0, out);
    assert.match(out, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の lock/);
    assert.match(out, /\(dry-run・送らない\)\n✅/);
    assert.match(out, /終わり: released/);
    assert.doesNotMatch(out, new RegExp(PW));
  });
} finally {
  for (const c of conns) { try { await c.end(); } catch { /* */ } }
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await sleep(500); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* */ } }
}
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng || cleanupFailed ? 1 : 0;
setTimeout(() => process.exit(process.exitCode), 10000).unref();
