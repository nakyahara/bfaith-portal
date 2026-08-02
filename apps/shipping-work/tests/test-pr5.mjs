/**
 * PR5 (休憩控除・異常候補一覧・集計・設定) のテスト。
 *   node test-pr5.mjs [repo-root]
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

// 引数省略時はこのファイルの位置からリポジトリルートを解決する
const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sw-pr5-'));
process.env.DATA_DIR = tmp;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/db.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/shipping-work/service.js')));
dbMod.initShippingWorkDB();
const db = dbMod.getDB();

let pass = 0, fail = 0;
const ok = (c, n) => { if (c) { pass++; console.log(`  ok - ${n}`); } else { fail++; console.log(`  NG - ${n}`); } };
function expectErr(fn, status, name) {
  try { fn(); fail++; console.log(`  NG - ${name} (エラーにならなかった)`); }
  catch (e) {
    if (e instanceof svc.SwError && e.status === status) { pass++; console.log(`  ok - ${name} [${e.status} ${e.message}]`); }
    else { fail++; console.log(`  NG - ${name} (got ${e.status ?? e.code}: ${e.message})`); }
  }
}
const uuid = () => crypto.randomUUID();
const W = 'w@b-faith.biz';
const ADMIN = 'admin@b-faith.biz';
const today = dbMod.jstToday();

function reset() {
  for (const t of ['sw_print_attempts', 'sw_print_jobs', 'sw_pauses', 'sw_mistakes',
    'sw_status_events', 'sw_operations', 'sw_audit_logs', 'sw_sessions', 'sw_batches']) {
    db.prepare(`DELETE FROM ${t}`).run();
  }
  db.prepare('DELETE FROM sw_settings').run();
}
const mk = (o) => dbMod.createBatch(
  { packingMethod: 'manual', bunrui: 'tanpin', docUrl: `https://drive.google.com/d/${o.shippingNo}`, ...o }, ADMIN);
/** 完了済みセッションを作り、時刻・時間を直接調整する (テスト用)。 */
function doneSession(batchId, worker, { startUtc, endUtc, activeSec, flags = [] }) {
  const r = svc.startProcess('picking', batchId, worker, uuid());
  svc.completeProcess('picking', r.session_id, worker, uuid());
  db.prepare('UPDATE sw_sessions SET requested_at=?, ended_at=?, active_sec=?, flags_json=? WHERE id=?')
    .run(startUtc, endUtc, activeSec, JSON.stringify(flags), r.session_id);
  return r.session_id;
}

console.log('# 1. 設定の保存と検証');
reset();
{
  expectErr(() => svc.saveSetting('unknown_key', '1', ADMIN), 400, '不明なキーは 400');
  expectErr(() => svc.saveSetting('anomaly_too_long_minutes', 'abc', ADMIN), 400, '整数以外は 400');
  expectErr(() => svc.saveSetting('anomaly_too_long_minutes', '0', ADMIN), 400, '範囲外は 400');
  svc.saveSetting('anomaly_too_long_minutes', '90', ADMIN);
  ok(db.prepare("SELECT value FROM sw_settings WHERE key='anomaly_too_long_minutes'").get().value === '90', '閾値を保存できる');
  expectErr(() => svc.saveSetting('break_periods', '12:00-11:00', ADMIN), 400, '終了<=開始の休憩帯は 400');
  expectErr(() => svc.saveSetting('break_periods', '25:00-26:00', ADMIN), 400, '不正な時刻は 400');
  svc.saveSetting('break_periods', '12:00-13:00, 15:00-15:15', ADMIN);
  ok(JSON.stringify(svc.getBreakPeriods()) === '["12:00-13:00","15:00-15:15"]', '休憩帯を保存できる (複数)');
  svc.saveSetting('break_periods', '', ADMIN);
  ok(svc.getBreakPeriods().length === 0, '空にできる');
  const audits = db.prepare("SELECT COUNT(*) c FROM sw_audit_logs WHERE action='admin_save_setting'").get().c;
  ok(audits === 3, '設定変更が監査ログに残る');
}

console.log('# 2. 休憩の自動控除 (要件§10.4)');
reset();
{
  // completeProcess は now を内部取得するため、requested_at を「now - 2時間」に置いた上で完了し、
  // 休憩帯を「now の90分前〜30分前」(1時間) に設定して控除を確かめる
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  const now = Date.now();
  const twoHoursAgo = new Date(now - 2 * 3600_000).toISOString().slice(0, 19) + 'Z';
  const fmtJst = (ms) => new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(ms));
  const bStart = fmtJst(now - 90 * 60_000);
  const bEnd = fmtJst(now - 30 * 60_000);
  svc.saveSetting('break_periods', `${bStart}-${bEnd}`, ADMIN);

  const r2 = svc.startProcess('picking', id, W, uuid());
  db.prepare('UPDATE sw_sessions SET requested_at=? WHERE id=?').run(twoHoursAgo, r2.session_id);
  const c = svc.completeProcess('picking', r2.session_id, W, uuid());
  // 経過2時間 − 休憩1時間 = 実作業約1時間
  ok(c.workSec > 3500 && c.workSec < 3700, `⭐休憩1時間が自動控除される (実作業 ${c.workSec}秒 ≒ 1時間)`);
  const s = db.prepare('SELECT * FROM sw_sessions WHERE id=?').get(r2.session_id);
  ok(JSON.parse(s.flags_json).includes('over_break'), '⭐休憩またぎフラグが付く');
  ok(s.active_sec === c.workSec, 'active_sec にも控除後の値が入る');
}
reset();
{
  // 休憩と保留が重なっても二重控除しない
  const now = Date.now();
  const fmtJst = (ms) => new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(ms));
  // 休憩帯 = 90分前〜30分前 (60分)
  svc.saveSetting('break_periods', `${fmtJst(now - 90 * 60_000)}-${fmtJst(now - 30 * 60_000)}`, ADMIN);
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  const r = svc.startProcess('picking', id, W, uuid());
  const twoHoursAgo = new Date(now - 2 * 3600_000).toISOString().slice(0, 19) + 'Z';
  db.prepare('UPDATE sw_sessions SET requested_at=? WHERE id=?').run(twoHoursAgo, r.session_id);
  // 保留 80分前〜50分前 (30分。全部休憩帯の中)
  svc.pauseProcess('picking', r.session_id, W, 'pk1', '', uuid());
  svc.resumeProcess('picking', r.session_id, W, uuid());
  db.prepare('UPDATE sw_pauses SET paused_at=?, resumed_at=? WHERE session_id=?')
    .run(new Date(now - 80 * 60_000).toISOString().slice(0, 19) + 'Z',
         new Date(now - 50 * 60_000).toISOString().slice(0, 19) + 'Z', r.session_id);
  const c = svc.completeProcess('picking', r.session_id, W, uuid());
  // 経過120分 − 保留30分 − 休憩(60分のうち保留と重ならない30分) = 60分
  ok(c.workSec > 3500 && c.workSec < 3700,
    `⭐保留と休憩の重なりは二重控除しない (実作業 ${c.workSec}秒 ≒ 60分)`);
}

console.log('# 2b. 日をまたぐセッションの休憩控除 (Codex round2 low)');
reset();
{
  // 26時間前に開始したセッション (必ずJSTの日を跨ぐ)。休憩帯は now の90〜30分前 (60分)。
  // 帯は暦日ごとに展開されるので、昨日と今日の2回分 = 120分が控除される
  const now = Date.now();
  const fmtJst = (ms) => new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(ms));
  svc.saveSetting('break_periods', `${fmtJst(now - 90 * 60_000)}-${fmtJst(now - 30 * 60_000)}`, ADMIN);
  const id = mk({ workDate: today, shippingNo: 's09', slipCount: 10 });
  const r = svc.startProcess('picking', id, W, uuid());
  const start = new Date(now - 26 * 3600_000).toISOString().slice(0, 19) + 'Z';
  db.prepare('UPDATE sw_sessions SET requested_at=? WHERE id=?').run(start, r.session_id);
  const c = svc.completeProcess('picking', r.session_id, W, uuid());
  const expected = 26 * 3600 - 2 * 3600;  // 経過26h − 休憩(60分×2日分)
  ok(Math.abs(c.workSec - expected) < 120,
    `⭐日をまたぐと休憩帯が暦日ごとに控除される (実作業 ${c.workSec}秒 ≒ ${expected}秒)`);
}

console.log('# 3. 異常候補一覧');
reset();
{
  const nowIso = (min) => new Date(Date.now() - min * 60_000).toISOString().slice(0, 19) + 'Z';
  const a = mk({ workDate: today, shippingNo: 's01', slipCount: 5 });
  const b = mk({ workDate: today, shippingNo: 's02', slipCount: 5 });
  const c = mk({ workDate: today, shippingNo: 's03', slipCount: 5 });
  doneSession(a, W, { startUtc: nowIso(10), endUtc: nowIso(9), activeSec: 30, flags: ['too_short'] });
  doneSession(b, W, { startUtc: nowIso(300), endUtc: nowIso(5), activeSec: 17000, flags: ['too_long'] });
  doneSession(c, W, { startUtc: nowIso(20), endUtc: nowIso(8), activeSec: 700, flags: [] });  // フラグなし
  const list = svc.listAnomalies(today, today);
  ok(list.length === 2, 'フラグ付きだけが一覧に出る (2件)');
  ok(list.every((s) => Array.isArray(s.flags) && s.flags.length > 0), 'フラグが配列で付く');
  // 除外判定すると invalid が表示に反映される (一覧からは消えない = 判定履歴が見える)
  svc.adminJudgeSession(list[0].id, 'invalid', ADMIN, '成績調整の疑い', uuid());
  const after = svc.listAnomalies(today, today);
  ok(after.find((s) => s.id === list[0].id)?.validity === 'invalid', '判定結果が一覧に反映される');
}

console.log('# 4. 集計 (要件§10.2)');
reset();
{
  const nowIso = (min) => new Date(Date.now() - min * 60_000).toISOString().slice(0, 19) + 'Z';
  // W: 単品2バッチ (10伝票 600秒 / 20伝票 1200秒) + アソート1バッチ (10伝票 900秒)
  const a = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  const b = mk({ workDate: today, shippingNo: 's02', slipCount: 20 });
  const c = mk({ workDate: today, shippingNo: 's03', bunrui: 'assort', slipCount: 10 });
  doneSession(a, W, { startUtc: nowIso(60), endUtc: nowIso(50), activeSec: 600 });
  doneSession(b, W, { startUtc: nowIso(45), endUtc: nowIso(25), activeSec: 1200 });
  doneSession(c, W, { startUtc: nowIso(20), endUtc: nowIso(5), activeSec: 900 });
  const rows = svc.getStats(today, today);
  const tanpin = rows.find((r) => r.worker === W && r.process === 'picking' && r.bunrui === 'tanpin');
  const assort = rows.find((r) => r.worker === W && r.process === 'picking' && r.bunrui === 'assort');
  ok(!!tanpin && !!assort, '発送分類別に分かれる');
  ok(tanpin.batches === 2 && tanpin.slips === 30 && tanpin.activeSec === 1800, '単品: 2バッチ/30伝票/1800秒');
  ok(tanpin.slipsPerHour === 60.0, '伝票/時 = 30伝票÷0.5時間 = 60');
  ok(tanpin.secPerSlipAvg === 60.0, '秒/伝票 平均 = 1800/30 = 60');
  ok(tanpin.secPerSlipMedian === 60.0, '秒/伝票 中央値 = (60+60)/2 = 60');
  ok(assort.batches === 1 && assort.secPerSlipAvg === 90.0, 'アソート: 90秒/伝票');
  // 除外判定した計測は集計から外れる
  const sid = db.prepare('SELECT id FROM sw_sessions WHERE batch_id=?').get(a).id;
  svc.adminJudgeSession(sid, 'invalid', ADMIN, '誤計測', uuid());
  const rows2 = svc.getStats(today, today);
  const tanpin2 = rows2.find((r) => r.worker === W && r.bunrui === 'tanpin');
  ok(tanpin2.batches === 1 && tanpin2.activeSec === 1200, '⭐除外した計測は集計から外れる');
}
reset();
{
  // 完了訂正の継続セッションはバッチ数を重複カウントしない・時間は合算
  const id = mk({ workDate: today, shippingNo: 's01', slipCount: 10 });
  const r = svc.startProcess('picking', id, W, uuid());
  svc.completeProcess('picking', r.session_id, W, uuid());
  const cor = svc.correctCompletion('picking', r.session_id, W, 'work_remained', '', uuid());
  svc.completeProcess('picking', cor.session_id, W, uuid());
  db.prepare('UPDATE sw_sessions SET active_sec=300 WHERE id=?').run(r.session_id);
  db.prepare('UPDATE sw_sessions SET active_sec=120 WHERE id=?').run(cor.session_id);
  const rows = svc.getStats(today, today);
  const g = rows.find((x) => x.worker === W);
  ok(g.batches === 1 && g.activeSec === 420, '⭐訂正の継続はバッチ1・時間は合算 (300+120)');
}

console.log('# 4b. Codexレビュー指摘の回帰');
reset();
{
  // 集計に日付軸がある (要件は 作業者×日×工程×発送分類)
  const nowIso = (min) => new Date(Date.now() - min * 60_000).toISOString().slice(0, 19) + 'Z';
  const yesterdayUtc = (min) => new Date(Date.now() - 24 * 3600_000 - min * 60_000).toISOString().slice(0, 19) + 'Z';
  const yesterday = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo' })
    .format(new Date(Date.now() - 24 * 3600_000));
  const a = mk({ workDate: yesterday, shippingNo: 's01', slipCount: 10 });
  const b = mk({ workDate: today, shippingNo: 's02', slipCount: 10 });
  doneSession(a, W, { startUtc: yesterdayUtc(60), endUtc: yesterdayUtc(30), activeSec: 600 });
  doneSession(b, W, { startUtc: nowIso(60), endUtc: nowIso(30), activeSec: 900 });
  const rows = svc.getStats(yesterday, today);
  ok(rows.length === 2, '⭐複数日は日付別の行に分かれる (1行に合算しない)');
  ok(rows[0].date === yesterday && rows[1].date === today, '各行に日付が付く (昇順)');
  ok(rows[0].activeSec === 600 && rows[1].activeSec === 900, '日ごとの時間が正しい');

  // 休憩帯の重複は保存できない (二重控除の防止)
  expectErr(() => svc.saveSetting('break_periods', '12:00-13:00, 12:30-13:30', ADMIN), 400,
    '⭐重なる休憩帯は 400');
  expectErr(() => svc.saveSetting('break_periods', '12:00-13:00, 12:00-13:00', ADMIN), 400,
    '同一帯の二重登録も 400');
  expectErr(() => svc.saveSetting('break_periods',
    Array.from({ length: 11 }, (_, i) => `${String(i).padStart(2, '0')}:00-${String(i).padStart(2, '0')}:30`).join(','),
    ADMIN), 400, '11個以上は 400');
  svc.saveSetting('break_periods', '15:00-15:15, 12:00-13:00', ADMIN);
  ok(JSON.stringify(svc.getBreakPeriods()) === '["12:00-13:00","15:00-15:15"]',
    '重ならない複数帯はソートされて保存される');
}

console.log('# 5. マスタの有効/無効');
reset();
{
  expectErr(() => svc.toggleMaster('shipping_no', 'nope', false, ADMIN), 404, '存在しないマスタは 404');
  svc.toggleMaster('shipping_no', 's45', false, ADMIN);
  ok(db.prepare("SELECT active FROM sw_masters WHERE kind='shipping_no' AND code='s45'").get().active === 0, '無効化できる');
  ok(!dbMod.listMasters('shipping_no').some((m) => m.code === 's45'), '無効化すると選択肢から消える');
  ok(svc.toggleMaster('shipping_no', 's45', false, ADMIN).noop === true, '同じ状態への切替は noop');
  svc.toggleMaster('shipping_no', 's45', true, ADMIN);
  ok(dbMod.listMasters('shipping_no').some((m) => m.code === 's45'), '再有効化できる');
  ok(db.prepare("SELECT COUNT(*) c FROM sw_audit_logs WHERE action='admin_toggle_master'").get().c === 2, '切替が監査ログに残る');
  const kinds = svc.listAllMasters();
  ok(kinds.has('shipping_no') && kinds.get('shipping_no').length === 46, '設定画面用の全マスタ一覧が取れる');
}

console.log(`\n結果: ${pass} passed / ${fail} failed`);
db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windowsのロックは無視 */ }
process.exit(fail === 0 ? 0 : 1);
