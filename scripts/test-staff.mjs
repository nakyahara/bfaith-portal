/**
 * スタッフマスタ (apps/staff) — テスト
 *
 * 実行: node scripts/test-staff.mjs   (DATA_DIR 未指定時は一時ディレクトリ)
 * 検証: 初期データ seed (13名・冪等) / 追加・重複番号拒否・入力検証 / 更新 (楽観ロック) / 無効化・再有効化 (削除しない) /
 *       監査履歴 / 名前タップ候補 (短い表記優先) / inbound-check からの参照
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'staff-test-'));
const staff = await import('../apps/staff/db.js');
const { getStaffDB, listStaff, getStaff, getStaffByNo, createStaff, updateStaff, setStaffActive, setStaffRoles, listAudit, listTapCandidates, tapName, seedInitialStaff, STAFF_KINDS, STAFF_ROLES } = staff;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const throwsWith = (fn, re, l) => { try { fn(); ok(false, `${l} (例外なし)`); } catch (e) { ok(re.test(e.message), `${l}: ${e.message}`); } };

console.log('DATA_DIR =', process.env.DATA_DIR);
getStaffDB();

console.log('\n[1] 初期データ');
{
  const all = listStaff({ includeInactive: true });
  ok(all.length === 13, `seed 13名 (${all.length})`);
  ok(all[0].staff_no === '0001' && all[0].display_name === '中原 大輔' && all[0].sort === 10, '1件目 0001 中原 大輔 (seed 順 = sort)');
  ok(getStaffByNo('20250901').display_name === '星 立夏' && getStaffByNo('20250901').joined_on === '2025-09-01', 'YYYYMMDD 番号 → 入社日に写す');
  ok(getStaffByNo('0003').joined_on === null, '0001〜0003 は入社日なし');
  ok(seedInitialStaff().seeded === 0 && listStaff({ includeInactive: true }).length === 13, 'seed は冪等 (2回目は0件)');
  ok(listAudit(all[0].id).some(a => a.action === 'seed'), '監査に seed が残る');
  // 役割 (どの現場の名前タップに出すか)
  ok(getStaffByNo('0003').roles.join(',') === 'office', '谷川 泰仁 = 事務 (seed)');
  ok(getStaffByNo('20240901').roles.join(',') === 'office' && getStaffByNo('20241001').roles.join(',') === 'office', '田中 美祐・高島 和美 = 事務');
  ok(getStaffByNo('20250901').roles.join(',') === 'warehouse', '星 立夏 = 倉庫');
  ok(getStaffByNo('0001').roles.join(',') === 'office,warehouse', '中原 大輔 = 倉庫+事務');
  ok(listStaff({ role: 'warehouse' }).length === 10 && listStaff({ role: 'office' }).length === 4, '倉庫10名 / 事務4名');
}

console.log('\n[2] 追加・検証');
{
  const s = createStaff({ staff_no: '20260901', display_name: 'テスト 太郎', short_name: 'テスト', kind: 'part_time', joined_on: '2026-09-01', portal_email: 'Test@Example.com' }, 'admin@example.com');
  ok(s.id > 0 && s.staff_no === '20260901' && s.portal_email === 'test@example.com' && s.sort === 140, '追加 (mail 小文字化・sort は末尾+10)');
  throwsWith(() => createStaff({ staff_no: '20260901', display_name: '別人' }, 'x'), /既に使われています/, '番号重複を拒否');
  throwsWith(() => createStaff({ staff_no: '', display_name: 'x' }, 'x'), /必須/, '番号必須');
  throwsWith(() => createStaff({ staff_no: 'abc', display_name: '' }, 'x'), /必須/, '名前必須');
  throwsWith(() => createStaff({ staff_no: 'x', display_name: 'y', kind: 'boss' }, 'x'), /区分/, '区分の値域');
  throwsWith(() => createStaff({ staff_no: 'x', display_name: 'y', joined_on: '2026/09/01' }, 'x'), /YYYY-MM-DD/, '日付形式');
  throwsWith(() => createStaff({ staff_no: '番号', display_name: 'y' }, 'x'), /英数字/, '番号は英数字');
  ok(STAFF_KINDS.length === 5, '区分 5種');
}

console.log('\n[3] 更新 (楽観ロック)・無効化');
{
  const s = getStaffByNo('20260901');
  const r1 = updateStaff(s.id, { short_name: 'テス', note: 'メモ' }, 'admin@example.com', s.version);
  ok(r1.ok && r1.staff.version === s.version + 1 && r1.staff.short_name === 'テス' && r1.staff.display_name === s.display_name, '一部更新 (他列は不変・version+1)');
  const r2 = updateStaff(s.id, { note: '古い画面' }, 'x', s.version);
  ok(!r2.ok && r2.error === 'conflict' && r2.current.note === 'メモ', '古い version = conflict');
  throwsWith(() => updateStaff(s.id, { staff_no: '0001' }, 'x', r1.staff.version), /既に使われています/, '番号の付け替え重複を拒否');
  const r3 = updateStaff(s.id, {}, 'x', r1.staff.version);
  ok(r3.ok && r3.changed === false, '変更なしは何もしない');
  ok(updateStaff(9999, { note: 'x' }, 'x', 1).error === 'not_found', '存在しない id');
  for (const bad of [undefined, null, 0, -1, 1.5, '2', NaN]) ok(updateStaff(s.id, { note: 'x' }, 'x', bad).error === 'bad_request', `expectVersion=${String(bad)} は bad_request`);
  ok(setStaffActive(s.id, false, 'x', {}).error === 'bad_request', 'active: expectVersion 必須');
  ok(setStaffActive(s.id, 'false', 'x', { expectVersion: r1.staff.version }).error === 'bad_request', "active: 'false' (文字列) は拒否");
  ok(setStaffActive(s.id, false, 'x', { expectVersion: r1.staff.version, leftOn: '2026/09/01' }).error === 'bad_request', 'active: 退職日の形式検証');
  ok(setStaffActive(s.id, false, 'x', { expectVersion: r1.staff.version, leftOn: '2026-02-30' }).error === 'bad_request', 'active: 実在しない退職日');
  ok(setStaffActive(s.id, false, 'x', { expectVersion: 1 }).error === 'conflict', 'active: 古い version = conflict');
  const d = setStaffActive(s.id, false, 'admin@example.com', { expectVersion: r1.staff.version });
  ok(d.ok && d.staff.active === 0 && /^\d{4}-\d{2}-\d{2}$/.test(d.staff.left_on), '無効化 = active 0 + 退職日 (削除しない)');
  ok(listStaff().length === 13 && listStaff({ includeInactive: true }).length === 14, '有効一覧から消えるが行は残る');
  ok(updateStaff(s.id, { note: '無効化の後' }, 'x', r1.staff.version).error === 'conflict', '無効化と通常編集の競合 (古い version) = conflict');
  const a = setStaffActive(s.id, true, 'admin@example.com', { expectVersion: d.staff.version });
  ok(a.ok && a.staff.active === 1 && a.staff.left_on === null, '再有効化で退職日クリア');
  const au = listAudit(s.id);
  ok(au.map(x => x.action).join(',') === 'reactivate,deactivate,update,create', `監査履歴 (${au.map(x => x.action).join(',')})`);
  ok(getStaff(s.id).version === a.staff.version, 'getStaff の version 一致');
  const dbx = getStaffDB();
  throwsWith(() => dbx.prepare('DELETE FROM staff_audit WHERE staff_id = ?').run(s.id), /append-only/, '監査表の DELETE はトリガで拒否');
  throwsWith(() => dbx.prepare("UPDATE staff_audit SET actor = 'x' WHERE staff_id = ?").run(s.id), /append-only/, '監査表の UPDATE はトリガで拒否');
  // 部分 seed: 1名を消した状態で seed → その1名だけ補完・他は不変
  const edited = updateStaff(getStaffByNo('0003').id, { note: '編集済み' }, 'x', getStaffByNo('0003').version);
  dbx.pragma('foreign_keys = OFF');   // 監査行が FK で参照しているのでテストでは一時的に外して行を消す
  dbx.prepare("DELETE FROM staff WHERE staff_no = '20260701'").run();
  dbx.pragma('foreign_keys = ON');
  const sd = seedInitialStaff(dbx);
  ok(sd.seeded === 1 && getStaffByNo('20260701') && getStaffByNo('0003').note === '編集済み' && getStaffByNo('0003').version === edited.staff.version, '部分 seed: 不足分だけ補完・既存行は上書きしない');
}

console.log('\n[4] 名前タップ候補');
{
  const c = listTapCandidates();
  ok(c.every(x => x.roles.includes('warehouse')), '名前タップの候補は既定で倉庫作業の人だけ (事務は出ない)');
  ok(!c.some(x => x.display_name === '谷川 泰仁'), '事務担当は候補に出ない');
  ok(c[0].name === '中原 大輔', '倉庫の人は出る (短い表記優先)');
  ok(listTapCandidates({ role: null }).length > c.length, 'role:null で全員を取れる');
  ok(tapName({ short_name: '  ', display_name: 'A' }) === 'A', '短い表記が空白なら正式表記');
  ok(c.every(x => Number.isInteger(x.staff_id) && x.staff_no), 'staff_id / staff_no を持つ');
  // 役割の付け外し
  const t2 = getStaffByNo('0003');
  const rr = setStaffRoles(t2.id, ['warehouse'], 'admin@example.com');
  ok(rr.ok && getStaffByNo('0003').roles.join(',') === 'warehouse', '事務 → 倉庫 に変更');
  ok(listTapCandidates().some(x => x.display_name === '谷川 泰仁'), '倉庫にすると候補に出る');
  ok(setStaffRoles(t2.id, ['office'], 'x').ok && !listTapCandidates().some(x => x.display_name === '谷川 泰仁'), '事務に戻すと候補から消える');
  ok(setStaffRoles(t2.id, ['boss'], 'x').error === 'bad_request', '不正な役割は拒否');
  ok(setStaffRoles(t2.id, [], 'x').ok && getStaffByNo('0003').roles.length === 0, '役割ゼロも許す (どの現場にも出さない)');
  ok(setStaffRoles(99999, ['office'], 'x').error === 'not_found', '存在しない id');
  ok(listAudit(t2.id).some(a => a.action === 'update'), '役割変更が監査に残る');
  ok(setStaffRoles(t2.id, ['office'], 'x').ok, '後片付け (事務に戻す)');
  // seed は画面で外した役割を復活させない
  const before = getStaffByNo('20250901').roles.join(',');
  setStaffRoles(getStaffByNo('20250901').id, ['office'], 'x');
  seedInitialStaff();
  ok(getStaffByNo('20250901').roles.join(',') === 'office', 'seed 再実行で役割を上書きしない');
  setStaffRoles(getStaffByNo('20250901').id, ['warehouse'], 'x');
  ok(getStaffByNo('20250901').roles.join(',') === before, '後片付け');
}

console.log('\n[5] inbound-check からの参照');
{
  const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
  initMirrorDB();
  const ic = await import('../apps/inbound-check/db.js');
  const ws = ic.listWorkers();
  const cand = listTapCandidates();
  ok(ws.length === cand.length && ws[0].code === '0001' && ws[0].name === '中原 大輔', `inbound-check.listWorkers = スタッフマスタの倉庫作業者 (${ws.length}名)`);
  ok(!ws.some(w => w.code === '0003'), '事務担当 (谷川 泰仁) は入荷受付チェックの名前タップにも出ない');
  const w = ic.getWorker('20250901');
  ok(w && w.name === '星 立夏' && w.active === 1 && Number.isInteger(w.staff_id), 'getWorker(管理番号)');
  ok(ic.getWorker('nope') === null, '不明な番号は null');
  const db = ic.getDB();
  ok(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='f_inbound_check_workers'").get(), '旧 f_inbound_check_workers は無い');
  ok(db.prepare('PRAGMA table_info(f_inbound_check_events)').all().some(c => c.name === 'staff_id'), 'events に staff_id 列');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
