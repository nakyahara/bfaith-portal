/**
 * スタッフマスタ同期 (miniPC picking ← Render apps/staff) — テスト
 *
 * 実行: node scripts/test-picking-staff-sync.mjs   (DATA_DIR 未指定時は一時ディレクトリ)
 * 検証: migration v12 / 名前一致の初回紐付け / 新規追加 / 改名・無効化の追従 / staff未登録は触らない /
 *       fail-closed (0件・激減・取得失敗) / staff_id での再照合 / 同期状態の記録
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pk-staff-test-'));
const { initPickingDB, getDB, listWorkers, addWorker, setWorkerActive } = await import('../apps/picking/db.js');
const { applyStaffExport, syncStaff, getStaffSyncState, fetchStaffExport, isStaffSyncConfigured } = await import('../apps/picking/staff-sync.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const staff = (id, no, name, extra = {}) => ({ id, staff_no: no, display_name: name, short_name: null, kind: null, portal_email: null, active: 1, sort: id * 10, updated_at: '2026-09-01T00:00:00Z', version: 1, roles: ['warehouse'], ...extra });
const payload = (rows, generated = '2026-09-01T01:00:00Z') => ({ ok: true, generated_at: generated, staff: rows });
const workerByName = n => listWorkers(true).find(w => w.name === n);

console.log('DATA_DIR =', process.env.DATA_DIR);
initPickingDB();
const db = getDB();

console.log('\n[1] migration v12');
{
  ok(db.pragma('user_version', { simple: true }) === 12, 'user_version = 12');
  const cols = db.prepare('PRAGMA table_info(pk_workers)').all().map(c => c.name);
  ok(['staff_id', 'staff_no', 'source'].every(c => cols.includes(c)), 'pk_workers に staff_id / staff_no / source');
  ok(!!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pk_staff_sync_state'").get(), 'pk_staff_sync_state テーブル');
  ok(!!db.prepare("SELECT 1 FROM sqlite_master WHERE type='index' AND name='uq_pk_workers_staff'").get(), 'staff_id の一意索引');
}

console.log('\n[2] 初回同期 (名前一致で紐付け + 新規追加)');
{
  // 既に picking で使われている作業者。⭐実データは**空白なし**の表記で、スタッフマスタは空白あり
  // (2026-09-01 の実障害: 完全一致の照合で紐付かず二重登録された)
  addWorker('星立夏');
  addWorker('田中美祐');
  addWorker('派遣 太郎');            // スタッフマスタに無い人 (触ってはいけない)
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'),
    staff(2, '20250901', '星 立夏'),
    staff(3, '20240901', '田中 美祐'),
  ]));
  ok(r.ok && r.linked === 2 && r.added === 1, `空白の有無を無視して紐付け2・追加1 (linked=${r.linked} added=${r.added})`);
  ok(r.renamed === 0, '空白の有無だけの違いでは改名しない (現場の表記=過去実績と繋がったまま)');
  ok(workerByName('星立夏').staff_id === 2 && workerByName('星立夏').staff_no === '20250901', '「星立夏」に staff_id を紐付け (現場の表記は据え置き)');
  ok(listWorkers(true).length === 4, '二重登録しない (4名: 星立夏・田中美祐・派遣 太郎・中原 大輔)');
  ok(workerByName('中原 大輔').source === 'staff' && workerByName('中原 大輔').active === 1, '新規スタッフを追加');
  ok(workerByName('派遣 太郎').staff_id === null && workerByName('派遣 太郎').source === 'local', 'スタッフマスタに無い人は触らない (local のまま)');
  ok(r.warnings.some(w => w.includes('派遣 太郎')), `未登録者が警告に出る (${r.warnings.join(' / ')})`);
  ok(listWorkers(true).length === 4, '合計4名');
  const st = getStaffSyncState();
  ok(st.ok === 1 && st.staff_count === 3 && st.generated_at === '2026-09-01T01:00:00Z', '同期状態を記録');
}

console.log('\n[3] 2回目 (staff_id で再照合・改名・無効化)');
{
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'),
    staff(2, '20250901', '星 立夏', { short_name: '星さん' }),      // 短い表記を変更
    staff(3, '20240901', '田中 美祐', { active: 0 }),               // 退職
    staff(4, '20260701', '有國 陽'),                                 // 新規
  ], '2026-09-02T01:00:00Z'));
  ok(r.ok && r.linked === 0, '2回目は名前一致に頼らない (linked=0)');
  ok(r.renamed === 1 && workerByName('星さん')?.staff_id === 2, `実質的な改名 (短い表記) には追従する (renamed=${r.renamed})`);
  ok(listWorkers(true).find(w => w.staff_id === 3)?.name === '田中美祐', '空白違いのままの人は改名されない');
  ok(r.deactivated === 1 && listWorkers(true).find(w => w.staff_id === 3).active === 0, '退職者を無効化');
  ok(r.added === 1 && workerByName('有國 陽'), '新規スタッフを追加');
  ok(listWorkers().every(w => w.name !== '田中美祐' && w.name !== '田中 美祐'), '無効化された人は名前タップに出ない');
  ok(workerByName('派遣 太郎').active === 1, 'スタッフマスタに無い人は有効のまま (勝手に消さない)');
  ok(listWorkers(true).length === 5, '行は消えない (5名)');
}

console.log('\n[4] 退職者の再入社・code の不変');
{
  const before = listWorkers(true).find(w => w.staff_id === 3);
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }),
    staff(3, '20240901', '田中 美祐'),   // 復帰
    staff(4, '20260701', '有國 陽'),
  ], '2026-09-03T01:00:00Z'));
  const after = listWorkers(true).find(w => w.staff_id === 3);
  ok(r.ok && after.active === 1 && after.code === before.code, `再有効化しても code は不変 (${after.code})`);
  ok(r.added === 0, '同じ人を二重に追加しない');
}

console.log('\n[5] 退職者だけの新規は生やさない');
{
  const n = listWorkers(true).length;
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }),
    staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
    staff(9, '19990101', '大昔 退職', { active: 0 }),
  ], '2026-09-04T01:00:00Z'));
  ok(r.ok && r.added === 0 && listWorkers(true).length === n, '無効なスタッフは pk_workers に追加しない');
}

console.log('\n[5b] 役割 (事務担当は名前タップに出さない)');
{
  const n = listWorkers(true).length;
  // 事務の人は追加されない
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }),
    staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
    staff(20, '0003', '谷川 泰仁', { roles: ['office'] }),
    staff(21, '20241001', '高島 和美', { roles: ['office'] }),
  ], '2026-09-06T00:00:00Z'));
  ok(r.ok && r.added === 0 && listWorkers(true).length === n, '事務だけの役割の人は pk_workers に追加しない');
  ok(!listWorkers().some(w => w.name === '谷川 泰仁'), '事務担当は名前タップに出ない');
  // 倉庫にいた人が事務に移ったら無効化される
  const r2 = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん', roles: ['office'] }),
    staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
  ], '2026-09-07T00:00:00Z'));
  ok(r2.ok && r2.deactivated === 1 && listWorkers(true).find(w => w.staff_id === 2)?.active === 0,
    '倉庫 → 事務 に変わった人は無効化 (名前タップから消える)');
  // 戻せば復活
  const r3 = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }),
    staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
  ], '2026-09-08T00:00:00Z'));
  ok(r3.ok && listWorkers(true).find(w => w.staff_id === 2)?.active === 1, '倉庫に戻せば復活');
  // roles が無い古い export は全員 倉庫扱い (現場の名前タップが空にならない)
  const r4 = applyStaffExport({ ok: true, generated_at: '2026-09-09T00:00:00Z', staff: [
    { id: 1, staff_no: '0001', display_name: '中原 大輔', active: 1, sort: 10 },
    { id: 2, staff_no: '20250901', display_name: '星 立夏', short_name: '星さん', active: 1, sort: 100 },
    { id: 3, staff_no: '20240901', display_name: '田中 美祐', active: 1, sort: 30 },
    { id: 4, staff_no: '20260701', display_name: '有國 陽', active: 1, sort: 40 },
  ] });
  ok(r4.ok && r4.deactivated === 0, 'roles が無い export は全員 倉庫とみなす (fail-open)');
}

console.log('\n[6] fail-closed');
{
  const n = listWorkers(true).length;
  const zero = applyStaffExport(payload([], '2026-09-15T01:00:00Z'));
  ok(!zero.ok && zero.skipped && listWorkers(true).length === n, '0件は適用しない');
  const half = applyStaffExport(payload([staff(1, '0001', '中原 大輔')], '2026-09-15T02:00:00Z'));
  ok(!half.ok && half.skipped && /半分未満/.test(half.error) && listWorkers(true).length === n, '有効数が前回成功時の半分未満なら適用しない');
  ok(/前回 4 名/.test(half.error), `判定基準は前回成功時の有効スタッフ数 (local を含めない): ${half.error}`);
  ok(listWorkers().length >= 4, '前回の作業者が保たれている');
  const st = getStaffSyncState();
  ok(st.ok === 0 && /半分未満/.test(st.error), '失敗理由が状態に残る');
}

console.log('\n[6b] 入力検証 (1件でも壊れていたら全体を拒否)');
{
  const n = listWorkers(true).length;
  const base = [staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽')];
  const cases = [
    [[...base, { ...staff(5, '5', 'x'), id: 'abc' }], /id が正の整数でない/, 'id が文字列'],
    [[...base, { ...staff(5, '5', 'x'), id: -1 }], /id が正の整数でない/, 'id が負数'],
    [[...base, staff(1, '90001', '別人')], /id 1 が重複/, 'id 重複'],
    [[...base, staff(5, '0001', '番号かぶり')], /staff_no 0001 が重複/, 'staff_no 重複'],
    [[...base, { ...staff(5, '', 'x') }], /staff_no が空/, 'staff_no 空'],
    [[...base, { ...staff(5, '5', '') }], /display_name が空/, 'display_name 空'],
    [[...base, staff(5, '90005', '有國 陽')], /同じ表示名が複数/, 'export 側に同名が2人'],
  ];
  for (const [rows, re, label] of cases) {
    const r = applyStaffExport(payload(rows, '2026-09-16T01:00:00Z'));
    ok(!r.ok && re.test(r.error) && listWorkers(true).length === n, `${label} → 全体を拒否 (${r.error?.slice(0, 40)})`);
  }
}

console.log('\n[6c] identity conflict (staff DB 再作成で id 再採番)');
{
  const n = listWorkers(true).length;
  const r = applyStaffExport(payload([
    staff(1, '99999', '中原 大輔'),   // id 1 は 0001 のはずが別番号で来た
    staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
  ], '2026-09-17T01:00:00Z'));
  ok(!r.ok && /対応が食い違って/.test(r.error) && listWorkers(true).length === n, `staff_no の食い違いは全体を拒否 (${r.error?.slice(0, 50)})`);
}

console.log('\n[6d] 同名の取り違え防止');
{
  addWorker('二重 名前');
  addWorker('二重 名前');   // pk_workers に同名が2行 (未紐付け)
  const before = listWorkers(true).length;
  const r = applyStaffExport(payload([
    staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }),
    staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽'),
    staff(7, '20261001', '二重 名前'),
  ], '2026-09-18T01:00:00Z'));
  ok(r.ok, '他の人の同期は進む');
  ok(r.added === 0 && listWorkers(true).length === before, '曖昧な名前の人は紐付けも追加もしない');
  ok(r.warnings.some(w => /紐付けを保留/.test(w)), `保留を警告に出す (${r.warnings.find(w => /保留/.test(w)) || 'なし'})`);
  ok(listWorkers(true).filter(w => w.name === '二重 名前').every(w => w.staff_id === null), '同名2行はどちらも紐付かない');
}

console.log('\n[6e] 世代の巻き戻り');
{
  const okr = applyStaffExport(payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽')], '2026-09-20T00:00:00Z'));
  ok(okr.ok, '新しい世代は適用');
  const old = applyStaffExport(payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '別名' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽')], '2026-09-19T00:00:00Z'));
  ok(!old.ok && /より古いため/.test(old.error), '古い世代の後着は拒否');
  ok(listWorkers(true).some(w => w.name === '星さん'), '巻き戻らない');
}

console.log('\n[6f] 紐付け済みが export から消えた');
{
  const r = applyStaffExport(payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐')], '2026-09-21T00:00:00Z'));
  ok(r.ok && r.warnings.some(w => /スタッフマスタから消えて/.test(w)), `消えた紐付け済みを強く警告 (${r.warnings.find(w => /消えて/.test(w)) || 'なし'})`);
  ok(listWorkers(true).find(w => w.staff_id === 4).active === 1, '勝手に無効化はしない (人が判断)');
}

console.log('\n[7] 取得 (fetch) 経路');
{
  ok(isStaffSyncConfigured() === false, 'env 未設定なら未構成');
  const r0 = await syncStaff();
  ok(!r0.ok && /未設定/.test(r0.error), 'env 未設定なら skip');
  process.env.STAFF_EXPORT_TOKEN = 'test-token';
  process.env.STAFF_SYNC_URL = 'https://example.test';
  let seen = null;
  const fakeOk = async (url, opt) => { seen = { url, auth: opt.headers.Authorization }; return { ok: true, status: 200, json: async () => payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽')], '2026-09-25T00:00:00Z') }; };
  const r = await syncStaff({ fetchFn: fakeOk });
  ok(r.ok && seen.url === 'https://example.test/apps/staff/export' && seen.auth === 'Bearer test-token', 'URL と Bearer ヘッダ');
  const r404 = await syncStaff({ fetchFn: async () => ({ ok: false, status: 404 }) });
  ok(!r404.ok && /Render 側の STAFF_EXPORT_TOKEN/.test(r404.error), '404 は Render 側 env 未設定として説明');
  const r401 = await syncStaff({ fetchFn: async () => ({ ok: false, status: 401 }) });
  ok(!r401.ok && /HTTP 401/.test(r401.error), '401 はそのまま報告');
  const rBad = await syncStaff({ fetchFn: async () => ({ ok: true, status: 200, json: async () => ({ ok: true }) }) });
  ok(!rBad.ok && /形式が不正/.test(rBad.error), '応答の形式検証');
  const rNet = await syncStaff({ fetchFn: async () => { throw new Error('ECONNREFUSED'); } });
  ok(!rNet.ok && /取得に失敗/.test(rNet.error) && listWorkers().length >= 4, '通信失敗でも作業者は消えない');
  // 同時実行 (手動 + poller) は1回にまとめる
  let calls = 0;
  const slow = async () => { calls++; await new Promise(r => setTimeout(r, 50)); return { ok: true, status: 200, json: async () => payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐')], '2026-09-30T00:00:00Z') }; };
  const [a, b] = await Promise.all([syncStaff({ fetchFn: slow }), syncStaff({ fetchFn: slow })]);
  ok(calls === 1 && a.ok && b.ok && a === b, `同時実行は1回にまとまる (fetch ${calls}回)`);
  delete process.env.STAFF_EXPORT_TOKEN; delete process.env.STAFF_SYNC_URL;
}

console.log('\n[7b] 同期状態の記録');
{
  const st = getStaffSyncState();
  ok(st.active_staff_count > 0 && st.last_generated_at === '2026-09-30T00:00:00Z', '成功時に判定基準 (有効数・世代) を進める');
  process.env.STAFF_EXPORT_TOKEN = 'test-token';
  await syncStaff({ fetchFn: async () => ({ ok: false, status: 500 }) });
  const st2 = getStaffSyncState();
  ok(st2.ok === 0 && st2.active_staff_count === st.active_staff_count && st2.last_generated_at === st.last_generated_at,
    '失敗しても判定基準は据え置き (基準が緩まない)');
  delete process.env.STAFF_EXPORT_TOKEN;
}

console.log('\n[8] 手で無効化した人を同期が復活させるか (仕様確認)');
{
  process.env.STAFF_EXPORT_TOKEN = 'test-token';
  const w = listWorkers(true).find(w => w.staff_id === 4);
  setWorkerActive(w.code, 0);
  ok(listWorkers(true).find(x => x.code === w.code).active === 0, '画面から無効化');
  applyStaffExport(payload([staff(1, '0001', '中原 大輔'), staff(2, '20250901', '星 立夏', { short_name: '星さん' }), staff(3, '20240901', '田中 美祐'), staff(4, '20260701', '有國 陽')], '2026-10-01T00:00:00Z'));
  ok(listWorkers(true).find(x => x.code === w.code).active === 1, 'スタッフマスタが有効なら次の同期で復活する (正本はスタッフマスタ)');
  delete process.env.STAFF_EXPORT_TOKEN;
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
