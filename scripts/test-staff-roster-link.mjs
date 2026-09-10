/**
 * スタッフマスタ 共通化 (2026-09-10) — テスト
 *
 * 実行: node scripts/test-staff-roster-link.mjs   (DATA_DIR 未指定時は一時ディレクトリ)
 * 検証: 役割 iroha / 職員PIN (設定・照合・ロック・秘密列を出さない) / 名簿の世代 (roster_rev) / 番号の自動採番 /
 *       /export は「いろはだけの人」を出さない /
 *       鏡 (roster-link): 写す・旧名簿の移行 (名前一致で紐付け・新規・PIN の持ち越し)・追加・有効無効・紐付け直し
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import crypto from 'crypto';
import express from 'express';
import Database from 'better-sqlite3';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'staff-link-test-'));
process.env.STAFF_EXPORT_TOKEN = 'test-token';
const staff = await import('../apps/staff/db.js');
const link = await import('../apps/staff/roster-link.js');
const { default: staffRouter } = await import('../apps/staff/router.js');
const {
  getStaffDB, listStaff, getStaff, getStaffByNo, createStaff, setStaffRoles, setStaffActive, setStaffPin, verifyStaffPin,
  _clearStaffPinFails, getRosterRev, nextGeneratedStaffNo, nameKey, STAFF_ROLES, IROHA_ROLE, listAudit,
} = staff;
const { ensureMirrorColumns, syncRoster, migrateLegacyRoster, addRosterWorker, setRosterWorkerActive, relinkRosterWorker, listStaffForLink, registerMirror } = link;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
console.log('DATA_DIR =', process.env.DATA_DIR);
getStaffDB();

// 鏡の表 (FBA箱詰めの fbx_workers と同じ DDL)。アプリの DB を丸ごと立てずに roster-link だけを試す
const MIRROR_DDL = (name) => `CREATE TABLE ${name} (
  id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL,
  worker_type TEXT NOT NULL CHECK (worker_type IN ('member','staff')),
  active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)), sort_order INTEGER NOT NULL DEFAULT 0,
  pin_hash TEXT, pin_salt TEXT, pin_fails INTEGER NOT NULL DEFAULT 0, pin_lock_until TEXT,
  created_at TEXT NOT NULL, created_by TEXT)`;
const app1 = new Database(':memory:'); app1.exec(MIRROR_DDL('fbx_workers'));
const app2 = new Database(':memory:'); app2.exec(MIRROR_DDL('f_iroha_workers'));
const st1 = { rev: null }, st2 = { rev: null };
registerMirror(app1, 'fbx_workers', st1);
registerMirror(app2, 'f_iroha_workers', st2);
const rows = (db, t) => db.prepare(`SELECT * FROM ${t} ORDER BY id`).all();

console.log('\n[1] 役割 iroha / 職員PIN');
{
  ok(STAFF_ROLES.includes('iroha') && IROHA_ROLE === 'iroha', '役割に iroha がある');
  const emp = createStaff({ staff_no: 'T-EMP', display_name: '職員 テスト', kind: 'employee' }, 't');
  const user = createStaff({ staff_no: 'T-USR', display_name: '利用者 テスト', kind: 'iroha' }, 't');
  ok(setStaffRoles(emp.id, ['office', 'iroha'], 't').ok && getStaff(emp.id).roles.join(',') === 'iroha,office', '役割 iroha を付けられる');
  ok(listStaff({ role: 'iroha' }).some(s => s.id === emp.id), 'listStaff({role:iroha}) で引ける');
  ok(setStaffPin(user.id, '1234', 't').error === 'not_staff', '利用者 (kind=iroha) には PIN を設定できない');
  ok(setStaffPin(emp.id, '12', 't').error === 'bad_pin', '桁数チェック');
  ok(setStaffPin(999999, '1234', 't').error === 'not_found', '存在しない id');
  const rev0 = getRosterRev();
  ok(setStaffPin(emp.id, '4649', 't').ok === true, '職員に PIN 設定');
  ok(getRosterRev() === rev0 + 1, 'PIN 設定で名簿の世代が進む (鏡が pin_set を写すため)');
  const s = getStaff(emp.id);
  ok(s.pin_set === 1 && !!s.pin_set_at, 'pin_set フラグと設定時刻');
  ok(!('pin_hash' in s) && !('pin_salt' in s), '🚨 getStaff は pin_hash / pin_salt を出さない');
  ok(!listStaff({ includeInactive: true }).some(x => 'pin_hash' in x), 'listStaff も出さない');
  ok(!JSON.stringify(listAudit(emp.id)).includes('pin_hash'), '監査 JSON にも入らない');
  ok(verifyStaffPin(emp.id, '4649').ok === true, '正しい PIN は通る');
  ok(verifyStaffPin(emp.id, '0000').error === 'pin_invalid', '間違いは弾く');
  ok(verifyStaffPin(emp.id, '').error === 'pin_required', '空は pin_required');
  ok(verifyStaffPin(user.id, '4649').error === 'pin_required', '利用者は照合の対象外');
  const revBefore = getRosterRev();
  for (let i = 0; i < 5; i++) verifyStaffPin(emp.id, '9999');
  ok(verifyStaffPin(emp.id, '4649').error === 'pin_locked', '5 回失敗でロック (正しい PIN でも通さない)');
  ok(getRosterRev() === revBefore, '照合 (失敗カウンタ) では世代を進めない');
  _clearStaffPinFails();
  ok(verifyStaffPin(emp.id, '4649').ok === true, 'ロック解除後は通る');
  // 利用者に PIN が乗る経路を塞ぐ (Codex #1301 R1 Medium)
  ok(staff.importStaffPinHash(user.id, { pinHash: 'x', pinSalt: 'staff-pin:y' }, 't').error === 'not_staff', '持ち越しでも利用者には PIN を入れない');
  const emp2 = createStaff({ staff_no: 'T-EMP2', display_name: '区分変更 テスト', kind: 'part_time' }, 't');
  setStaffPin(emp2.id, '5555', 't');
  ok(getStaff(emp2.id).pin_set === 1, '設定できる');
  const u = staff.updateStaff(emp2.id, { kind: 'iroha' }, 't', getStaff(emp2.id).version);
  ok(u.ok && getStaff(emp2.id).pin_set === 0 && verifyStaffPin(emp2.id, '5555').error === 'pin_required', '区分を いろは (利用者) に変えたら PIN は消える');
}

console.log('\n[2] 名簿の世代 / 番号の自動採番 / 照合キー');
{
  const r0 = getRosterRev();
  const x = createStaff({ staff_no: 'T-REV', display_name: '世代 テスト' }, 't');
  ok(getRosterRev() === r0 + 1, '追加で +1');
  setStaffRoles(x.id, ['iroha'], 't');
  ok(getRosterRev() === r0 + 2, '役割の変更で +1');
  setStaffRoles(x.id, ['iroha'], 't');
  ok(getRosterRev() === r0 + 2, '同じ役割を入れ直しても進まない');
  setStaffActive(x.id, false, 't', { expectVersion: getStaff(x.id).version });
  ok(getRosterRev() === r0 + 3, '無効化で +1');
  ok(nextGeneratedStaffNo() === 'IROHA-001', '自動採番の最初は IROHA-001');
  createStaff({ staff_no: 'IROHA-001', display_name: '採番 1' }, 't');
  createStaff({ staff_no: 'IROHA-007', display_name: '採番 7' }, 't');
  ok(nextGeneratedStaffNo() === 'IROHA-008', '最大 +1 (歯抜けは埋めない)');
  ok(nameKey('田中　太郎') === nameKey('田中 太郎') && nameKey('田中 太郎') === nameKey('田中太郎'), '照合キーは空白 (全角/半角) を無視');
  ok(nameKey('Ｔａｎａｋａ') === nameKey('Tanaka'), 'NFKC で寄せる');
  ok(nameKey('田中') !== nameKey('田中 太郎'), '部分一致はしない');
}

console.log('\n[3] /export は「いろはの利用者 (kind=iroha)」と「役割が iroha だけの人」を出さない');
{
  const app = express();
  app.use((req, _res, next) => { req.session = null; next(); });
  app.use('/apps/staff', express.json(), staffRouter);
  const server = http.createServer(app);
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const port = server.address().port;
  const irohaOnly = createStaff({ staff_no: 'T-IO', display_name: 'いろはだけ', kind: 'iroha' }, 't');
  setStaffRoles(irohaOnly.id, ['iroha'], 't');
  const both = createStaff({ staff_no: 'T-BOTH', display_name: '倉庫といろは' }, 't');
  setStaffRoles(both.id, ['warehouse', 'iroha'], 't');
  const none = createStaff({ staff_no: 'T-NONE', display_name: '役割なし' }, 't');
  setStaffRoles(none.id, [], 't');
  const staffIrohaOnly = createStaff({ staff_no: 'T-SIO', display_name: '職員いろはだけ', kind: 'employee' }, 't');
  setStaffRoles(staffIrohaOnly.id, ['iroha'], 't');
  const r = await fetch(`http://127.0.0.1:${port}/apps/staff/export`, { headers: { Authorization: 'Bearer test-token' } });
  const j = await r.json();
  const nos = new Set(j.staff.map(s => s.staff_no));
  ok(r.status === 200 && j.ok, 'export が取れる');
  ok(!nos.has('T-IO'), '利用者 (kind=iroha) は出ない (miniPC に関係なく、名前を外に出さない)');
  ok(!nos.has('T-SIO'), '役割が いろは だけの職員も出ない (取込側が同名の未紐付け作業者に紐付けて無効にするため — Codex R1 High#1)');
  ok(nos.has('T-BOTH'), '倉庫の役割も持つ人は出る (roles に iroha も載る)');
  ok(nos.has('T-NONE'), '役割の無い人は今までどおり出る (取込側が無効にする)');
  ok(nos.has('0001'), '既存の 13 名は出る');
  ok(!JSON.stringify(j).includes('pin_hash'), 'export にも pin_hash は出ない');
  server.close();
}

console.log('\n[4] 鏡: 写す (syncRoster)');
{
  ensureMirrorColumns(app1, 'fbx_workers');
  ensureMirrorColumns(app1, 'fbx_workers');   // 2 回呼んでも壊れない
  const cols = new Set(app1.prepare('PRAGMA table_info(fbx_workers)').all().map(c => c.name));
  ok(cols.has('staff_id') && cols.has('pin_set'), 'staff_id / pin_set 列が足される');
  const r1 = syncRoster(app1, 'fbx_workers', st1);
  const m = rows(app1, 'fbx_workers');
  ok(r1.synced && m.length === 4, `役割 iroha を持つ有効な人だけ写る (${m.length} 人: 職員テスト・いろはだけ・倉庫といろは・職員いろはだけ)`);
  const emp = m.find(x => x.display_name === '職員 テスト');
  const usr = m.find(x => x.display_name === 'いろはだけ');
  ok(emp && emp.worker_type === 'staff' && emp.pin_set === 1 && emp.active === 1, '社員 → 職員 / pin_set=1');
  ok(usr && usr.worker_type === 'member' && usr.pin_set === 0, 'kind=iroha → 利用者');
  ok(!m.some(x => x.display_name === '利用者 テスト'), '役割 iroha が無い人は写らない');
  ok(syncRoster(app1, 'fbx_workers', st1).synced === false, '世代が同じなら何もしない');
  // 名前を変える → 追従
  const s = getStaff(emp.staff_id);
  staff.updateStaff(s.id, { short_name: 'しょくいん' }, 't', s.version);
  syncRoster(app1, 'fbx_workers', st1);
  ok(rows(app1, 'fbx_workers').find(x => x.id === emp.id).display_name === 'しょくいん', '短い表記が入ると名前タップの名前も変わる');
  // 役割を外す → 無効 (行は残る)
  setStaffRoles(s.id, ['office'], 't');
  syncRoster(app1, 'fbx_workers', st1);
  ok(rows(app1, 'fbx_workers').find(x => x.id === emp.id).active === 0, '役割 iroha を外すと無効になる (行は残る = 履歴を守る)');
  setStaffRoles(s.id, ['office', 'iroha'], 't');
  syncRoster(app1, 'fbx_workers', st1);
  ok(rows(app1, 'fbx_workers').find(x => x.id === emp.id).active === 1, '付け直すと有効に戻る (同じ行 = id が変わらない)');
}

console.log('\n[5] 旧名簿の移行 (migrateLegacyRoster)');
{
  const legacy = (db, t, name, type, extra = {}) => {
    const now = new Date().toISOString();
    return Number(db.prepare(`INSERT INTO ${t} (display_name, worker_type, active, pin_hash, pin_salt, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?, 'legacy')`)
      .run(name, type, extra.active ?? 1, extra.pin_hash ?? null, extra.pin_salt ?? null, now).lastInsertRowid);
  };
  // 旧方式の PIN ハッシュ (fba-box のまま: scrypt(pin, `fbx-pin:${salt}`))
  const salt = crypto.randomBytes(16).toString('hex');
  const oldHash = crypto.scryptSync('2468', `fbx-pin:${salt}`, 32).toString('hex');
  const lTanaka = legacy(app1, 'fbx_workers', 'たなか', 'member');                       // 一致なし → 新規 (kind=iroha)
  const lHoshi = legacy(app1, 'fbx_workers', '星立夏', 'staff', { pin_hash: oldHash, pin_salt: salt });   // 既存社員「星 立夏」に空白違いで一致 + PIN 持ち越し
  const lNakahara = legacy(app1, 'fbx_workers', '中原 大輔', 'member');                  // 利用者は社員に一致させない → 新規
  const lQuit = legacy(app1, 'fbx_workers', 'やめたひと', 'member', { active: 0 });      // 無効 → 新規だが無効・役割なし
  const lSameAsUser = legacy(app1, 'fbx_workers', 'いろはだけ', 'staff', { pin_hash: oldHash, pin_salt: salt });   // 利用者「いろはだけ」(kind=iroha) と同名の職員 → 利用者に紐付けない
  const mig = migrateLegacyRoster(app1, 'fbx_workers', { saltPrefix: 'fbx-pin:', appLabel: 'FBA箱詰め' });
  ok(mig.linked.length === 1 && mig.linked[0].localId === lHoshi && mig.linked[0].staffNo === '20250901', '「星立夏」→ 既存の 星 立夏 (20250901) に紐付け (空白無視)');
  ok(mig.linked[0].pin === 'carried', 'PIN をハッシュのまま持ち越した');
  ok(getStaffByNo('20250901').roles.includes('iroha'), '紐付けた既存社員に役割 iroha が付く (倉庫の役割は残る)');
  ok(verifyStaffPin(getStaffByNo('20250901').id, '2468').ok === true, '🚨 持ち越した旧方式の PIN がそのまま通る');
  ok(mig.created.length === 4, `一致しない 4 人は新規 (${mig.created.map(c => c.name).join('・')})`);
  const sameAsUser = getStaff(mig.created.find(c => c.localId === lSameAsUser).staffId);
  ok(sameAsUser.kind === null && sameAsUser.id !== getStaffByNo('T-IO').id && sameAsUser.pin_set === 1, '職員は同名の利用者 (kind=iroha) に紐付けず新規 (PIN は新しい行に)');
  ok(getStaffByNo('T-IO').pin_set === 0, '利用者の行に PIN は乗らない');
  const tanaka = getStaff(mig.created.find(c => c.localId === lTanaka).staffId);
  ok(tanaka.kind === 'iroha' && /^IROHA-\d{3}$/.test(tanaka.staff_no) && tanaka.roles.join(',') === 'iroha', 'たなか: kind=iroha・自動採番・役割 iroha');
  const nak = getStaff(mig.created.find(c => c.localId === lNakahara).staffId);
  ok(nak.id !== getStaffByNo('0001').id && nak.kind === 'iroha', '利用者「中原 大輔」は社員 0001 に紐付けず別の人として作る');
  const quit = getStaff(mig.created.find(c => c.localId === lQuit).staffId);
  ok(quit.active === 0 && quit.roles.length === 0, '無効だった人は無効・役割なしで作る (鏡で有効に戻らない)');
  const after = rows(app1, 'fbx_workers');
  ok(after.every(x => x.staff_id != null), '全行に staff_id が入った');
  ok(after.every(x => x.pin_hash == null && x.pin_salt == null), '旧 PIN 列は消した (秘密を残さない)');
  const again = migrateLegacyRoster(app1, 'fbx_workers', { saltPrefix: 'fbx-pin:', appLabel: 'FBA箱詰め' });
  ok(again.linked.length === 0 && again.created.length === 0, '2 回目は何もしない (冪等)');
  syncRoster(app1, 'fbx_workers', st1, { force: true });
  ok(rows(app1, 'fbx_workers').find(x => x.id === lQuit).active === 0 && rows(app1, 'fbx_workers').find(x => x.id === lTanaka).active === 1, '写したあとも 無効/有効 が保たれる');

  // もう一つのアプリ (いろは在庫化) に同じ「たなか」がいる → 同じ人に紐付く (2 アプリで 1 人)
  ensureMirrorColumns(app2, 'f_iroha_workers');
  const lTanaka2 = legacy(app2, 'f_iroha_workers', 'たなか', 'member');
  const lHoshi2 = legacy(app2, 'f_iroha_workers', '星 立夏', 'staff', { pin_hash: crypto.scryptSync('1111', `iroha-pin:zz`, 32).toString('hex'), pin_salt: 'zz' });
  const mig2 = migrateLegacyRoster(app2, 'f_iroha_workers', { saltPrefix: 'iroha-pin:', appLabel: 'いろは在庫化' });
  ok(mig2.linked.length === 2 && mig2.linked.find(x => x.localId === lTanaka2).staffId === tanaka.id, '2 つ目のアプリの「たなか」は 1 つ目が作った利用者に紐付く');
  ok(mig2.linked.find(x => x.localId === lHoshi2).pin === 'kept-existing', '既に PIN がある人の PIN は上書きしない (先に共通化した方が正)');
  ok(verifyStaffPin(getStaffByNo('20250901').id, '2468').ok && verifyStaffPin(getStaffByNo('20250901').id, '1111').error === 'pin_invalid', '星 立夏 の PIN は FBA箱詰め側の 2468 のまま');
}

console.log('\n[6] 追加 / 有効無効 (アプリから)');
{
  const a = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'さとう', workerType: 'member', actor: 'ipad', appLabel: 'FBA箱詰め' });
  ok(a.ok && a.id > 0 && /^IROHA-\d{3}$/.test(a.staffNo), `追加 → 鏡の id と自動採番 (${a.staffNo})`);
  const sato = getStaff(a.staffId);
  ok(sato.kind === 'iroha' && sato.roles.join(',') === 'iroha' && sato.note.includes('FBA箱詰め'), 'スタッフマスタに kind=iroha・役割 iroha で入る');
  ok(addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'さとう', workerType: 'member', actor: 'ipad', appLabel: 'FBA箱詰め' }).error === 'duplicate', '同名 (有効) は重複');
  ok(addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'x', workerType: 'boss', actor: 'ipad', appLabel: 'FBA箱詰め' }).error === 'bad_type', '区分は member/staff');
  const b = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'しょくいん2', workerType: 'staff', actor: 'ipad', appLabel: 'FBA箱詰め' });
  ok(getStaff(b.staffId).kind === null, '職員として追加した人の区分は空 (管理画面で決める)');
  // 2 つ目のアプリにも同じ人が出る
  syncRoster(app2, 'f_iroha_workers', st2);
  ok(rows(app2, 'f_iroha_workers').some(x => x.staff_id === a.staffId && x.display_name === 'さとう'), '片方で足すともう片方にも出る');
  // 無効 = 役割を外すだけ
  const off = setRosterWorkerActive(app1, 'fbx_workers', st1, a.id, false, 'ipad');
  ok(off.ok && rows(app1, 'fbx_workers').find(x => x.id === a.id).active === 0, '無効にできる');
  ok(getStaff(a.staffId).active === 1 && getStaff(a.staffId).roles.length === 0, 'スタッフマスタでは有効のまま (役割 iroha だけ外れる)');
  syncRoster(app2, 'f_iroha_workers', st2);
  ok(rows(app2, 'f_iroha_workers').find(x => x.staff_id === a.staffId).active === 0, 'もう片方のアプリでも無効になる');
  ok(setRosterWorkerActive(app1, 'fbx_workers', st1, a.id, true, 'ipad').ok && rows(app1, 'fbx_workers').find(x => x.id === a.id).active === 1, '有効に戻せる');
  // スタッフマスタで退職 → アプリから有効にはできない
  setStaffActive(a.staffId, false, 'admin', { expectVersion: getStaff(a.staffId).version });
  syncRoster(app1, 'fbx_workers', st1);
  ok(rows(app1, 'fbx_workers').find(x => x.id === a.id).active === 0, '退職 (無効) は鏡にも効く');
  ok(setRosterWorkerActive(app1, 'fbx_workers', st1, a.id, true, 'ipad').error === 'retired', 'アプリからは戻せない (スタッフマスタで戻す)');
  ok(setRosterWorkerActive(app1, 'fbx_workers', st1, 99999, true, 'ipad').error === 'not_found', '存在しない id');
}

console.log('\n[7] 紐付け直し (relinkRosterWorker)');
{
  // 移行で新規に作った「みやけ」(自動採番・PIN あり) を、既存社員「三宅 晴菜」(20240801) に付け替える
  const l = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'みやけ', workerType: 'staff', actor: 'ipad', appLabel: 'FBA箱詰め' });
  setStaffPin(l.staffId, '3333', 'ipad');
  syncRoster(app1, 'fbx_workers', st1);
  const target = getStaffByNo('20240801');
  ok(!target.pin_set && !target.roles.includes('iroha'), '付け替え先は PIN なし・役割 iroha なし');
  const r = relinkRosterWorker(app1, 'fbx_workers', st1, { localId: l.id, staffId: target.id, actor: 'admin' });
  ok(r.ok && r.pin === 'carried', '付け替えできる + PIN を引き継ぐ');
  const local = rows(app1, 'fbx_workers').find(x => x.id === l.id);
  ok(local.staff_id === target.id && local.display_name === '三宅 晴菜' && local.active === 1, '鏡の行は同じ id のまま、名前が正式表記に');
  // もう片方のアプリの鏡も一緒に付け替わる (片方だけ直すと、もう片方でその人が消える — Codex R1 High#2)
  ok(r.alsoRelinked.some(x => x.table === 'f_iroha_workers'), 'いろは在庫化の鏡も付け替えた');
  const local2 = rows(app2, 'f_iroha_workers').find(x => x.staff_id === target.id);
  ok(local2 && local2.active === 1 && local2.display_name === '三宅 晴菜', 'いろは在庫化でもその人は有効なまま (消えない)');
  ok(!rows(app2, 'f_iroha_workers').some(x => x.staff_id === l.staffId && x.active === 1), '元の行を指す有効な行はどちらの鏡にも残らない');
  const t2 = getStaffByNo('20240801');
  ok(t2.roles.includes('iroha') && t2.roles.includes('warehouse') && t2.pin_set === 1, '先に役割 iroha が付き (倉庫は残る)、PIN も付く');
  ok(verifyStaffPin(t2.id, '3333').ok === true, '引き継いだ PIN が通る');
  const old = getStaff(l.staffId);
  ok(old.roles.length === 0 && old.active === 0, '元の自動採番の行は役割なし・無効 (重複の後始末)');
  ok(relinkRosterWorker(app1, 'fbx_workers', st1, { localId: l.id, staffId: target.id, actor: 'admin' }).unchanged === true, '同じ先なら何もしない');
  const other = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'ほかのひと', workerType: 'member', actor: 'ipad', appLabel: 'FBA箱詰め' });
  ok(relinkRosterWorker(app1, 'fbx_workers', st1, { localId: other.id, staffId: target.id, actor: 'admin' }).error === 'already_linked', '既に別の行が紐付いている人には付け替えられない');
  ok(relinkRosterWorker(app1, 'fbx_workers', st1, { localId: other.id, staffId: 99999, actor: 'admin' }).error === 'not_found', '存在しない staff');
  // 他のアプリの鏡で付け替えられないとき: その鏡は触らず、元の行の役割も外さない
  const cA = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'こんふりくとA', workerType: 'member', actor: 'ipad', appLabel: 'FBA箱詰め' });
  const cB = addRosterWorker(app1, 'fbx_workers', st1, { displayName: 'こんふりくとB', workerType: 'member', actor: 'ipad', appLabel: 'FBA箱詰め' });
  const X = createStaff({ staff_no: 'T-X', display_name: 'ターゲットX', kind: 'iroha' }, 't');
  syncRoster(app2, 'f_iroha_workers', st2);
  const bRow2 = rows(app2, 'f_iroha_workers').find(x => x.staff_id === cB.staffId);
  app2.prepare('UPDATE f_iroha_workers SET staff_id = ? WHERE id = ?').run(X.id, bRow2.id);   // いろは在庫化側では X が既に B に紐付いている状態を作る
  const rc = relinkRosterWorker(app1, 'fbx_workers', st1, { localId: cA.id, staffId: X.id, actor: 'admin' });
  ok(rc.ok && rc.conflicts.length === 1 && rc.conflicts[0].table === 'f_iroha_workers', `付け替えたが衝突を返す: ${rc.message}`);
  ok(rows(app1, 'fbx_workers').find(x => x.id === cA.id).staff_id === X.id, 'この鏡は付け替わっている');
  const aRow2 = rows(app2, 'f_iroha_workers').find(x => x.staff_id === cA.staffId);
  ok(!!aRow2, 'いろは在庫化の鏡は元の行を指したまま (触らない)');
  const oldA = getStaff(cA.staffId);
  ok(oldA.roles.includes('iroha') && oldA.active === 1, '元の行の役割 iroha は外さない (まだ使っている鏡がある)');
  ok(aRow2.active === 1, 'いろは在庫化でその人は消えない');
  ok(listStaffForLink().some(s => s.id === old.id && s.active === 0), '紐付け直しの選択肢には無効な人も出る (いまの先が無効でも表示できるように)');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exit(fail ? 1 : 0);
