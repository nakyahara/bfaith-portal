/**
 * FBA箱詰め記録 (apps/fba-box) — router 層のテスト (HTTP 経由・権限境界)
 * 実行: node scripts/test-fba-box-router.mjs
 *
 * 検証: 未登録端末の拒否 / 登録コード→端末Cookie / 名簿ゲート (bootstrap → 職員PIN) /
 *       箱の取消は職員のみ / 箱札・まとめ / 本社: 出荷前チェック → Excel出力 → DL → STAアップ済み / 資材は管理者のみ
 * セッションは x-test-session ヘッダで模擬 (admin / user / なし)。picking-prep には触れない (run は db 直接作成)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import express from 'express';

process.env.RENDER = '';
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-box-router-'));
process.env.DATA_DIR = tmp;

const db = await import('../apps/fba-box/db.js');
const svc = await import('../apps/fba-box/service.js');
const xl = await import('../apps/fba-box/excel.js');
db._openForTest(path.join(tmp, 'router.db'));
const { default: router, _setPickingSource } = await import('../apps/fba-box/router.js');

const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  if (s === 'admin') req.session = { authenticated: true, email: 'admin@test', role: 'admin', allowedApps: '*', destroy: (cb) => cb() };
  else if (s === 'user') req.session = { authenticated: true, email: 'user@test', role: 'user', allowedApps: ['fba-box'], destroy: (cb) => cb() };
  else req.session = null;
  next();
});
app.use('/apps/fba-box', express.json({ limit: '256kb' }), router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const PORT = server.address().port;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const BASE = `${ORIGIN}/apps/fba-box`;

let passed = 0, failed = 0;
function t(name, fn) {
  return Promise.resolve().then(fn).then(() => { passed++; console.log(`  ✅ ${name}`); })
    .catch((e) => { failed++; console.error(`  ❌ ${name}\n     ${e.message}`); });
}
let deviceCookie = null;
async function call(method, url, { body, session, device = true, origin = true, raw = false } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (session) headers['x-test-session'] = session;
  if (device && deviceCookie) headers.Cookie = deviceCookie;
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  if (raw) return r;
  let j = null;
  try { j = await r.json(); } catch { /* HTML 等 */ }
  return { status: r.status, j, r };
}

console.log('■ アクセス制御');
await t('未登録端末: /api は 401、画面は /enroll へ', async () => {
  assert.equal((await call('GET', '/api/runs')).status, 401);
  const r = await call('GET', '/', { raw: true });
  assert.equal(r.status, 302);
  assert.ok(r.headers.get('location').endsWith('/apps/fba-box/enroll'));
});
await t('登録コードは管理者のみ発行でき、Origin なしは 403', async () => {
  assert.equal((await call('POST', '/admin/enroll-codes', { body: { label: 'iPad' }, session: 'user' })).status, 403);
  assert.equal((await call('POST', '/admin/enroll-codes', { body: { label: 'iPad' }, session: 'admin', origin: false })).status, 403);
});
let code = null;
await t('管理者が発行 → 端末が /enroll/redeem で Cookie を得る', async () => {
  const c = await call('POST', '/admin/enroll-codes', { body: { label: 'テストiPad' }, session: 'admin' });
  assert.equal(c.j.ok, true);
  code = c.j.code;
  const r = await call('POST', '/enroll/redeem', { body: { code }, raw: true });
  assert.equal(r.status, 200);
  const sc = r.headers.get('set-cookie');
  assert.ok(sc && sc.includes('fbx_device='), 'set-cookie に fbx_device');
  deviceCookie = sc.split(';')[0];
  assert.equal((await call('GET', '/api/runs')).status, 200);
});

console.log('■ 名簿 (bootstrap → 職員PIN)');
let staffId = null, memberId = null;
await t('PIN持ち職員が 0 人 = bootstrap: 端末から PIN なしで職員を追加し PIN を設定できる', async () => {
  const r0 = await call('GET', '/api/roster');
  assert.equal(r0.j.bootstrap, true);
  const a = await call('POST', '/api/workers', { body: { display_name: 'しょくいん', worker_type: 'staff' } });
  assert.equal(a.j.ok, true, JSON.stringify(a.j));
  staffId = a.j.id;
  const p = await call('POST', `/api/workers/${staffId}/pin`, { body: { pin: '2468' } });
  assert.equal(p.j.ok, true, JSON.stringify(p.j));
  assert.equal((await call('GET', '/api/roster')).j.bootstrap, false);
});
await t('bootstrap 終了後: 認証なしは 403、間違い PIN は 403、正しい職員PIN で追加できる', async () => {
  const noAuth = await call('POST', '/api/workers', { body: { display_name: 'りようしゃ', worker_type: 'member' } });
  assert.equal(noAuth.status, 403); assert.equal(noAuth.j.error, 'staff_required');
  const bad = await call('POST', '/api/workers', { body: { display_name: 'りようしゃ', worker_type: 'member', auth_worker_id: staffId, auth_pin: '0000' } });
  assert.equal(bad.status, 403); assert.equal(bad.j.error, 'pin_invalid');
  const okr = await call('POST', '/api/workers', { body: { display_name: 'りようしゃ', worker_type: 'member', auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(okr.j.ok, true, JSON.stringify(okr.j));
  memberId = okr.j.id;
});
await t('PIN を持つ最後の職員は端末から無効にできない (409) / セッションなら可', async () => {
  const r = await call('POST', `/api/workers/${staffId}/active`, { body: { active: false, auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(r.status, 409); assert.equal(r.j.error, 'last_staff');
  const r2 = await call('POST', `/api/workers/${memberId}/active`, { body: { active: false, auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(r2.j.ok, true);
  const r3 = await call('POST', `/api/workers/${memberId}/active`, { body: { active: true }, session: 'user', device: false });
  assert.equal(r3.j.ok, true);
});
await t('セッションで最後の PIN 職員を無効にしても端末が bootstrap (無ゲート) に戻らない', async () => {
  const off = await call('POST', `/api/workers/${staffId}/active`, { body: { active: false }, session: 'admin', device: false });
  assert.equal(off.j.ok, true);
  assert.equal((await call('GET', '/api/roster')).j.bootstrap, false);
  const add = await call('POST', '/api/workers', { body: { display_name: 'のっとり', worker_type: 'staff' } });
  assert.equal(add.status, 403);
  const on = await call('POST', `/api/workers/${staffId}/active`, { body: { active: true }, session: 'admin', device: false });
  assert.equal(on.j.ok, true);
});
await t('名簿の変更操作が監査に残る (worker_add / pin_set / worker_active)', async () => {
  const actions = new Set(db.listEvents(100).map((e) => e.action));
  for (const a of ['worker_add', 'pin_set', 'worker_active']) assert.ok(actions.has(a), a);
});

console.log('■ 納品回 → 箱 → 取消 → 出荷前チェック → Excel出力');
const fixture = path.resolve('scripts/fixtures/fba-box/packlist_v1.1_2sku_15box.xlsx');
const ing = await xl.ingestPacklist(fs.readFileSync(fixture), 'packlist_router.xlsx');
assert.equal(ing.ok, true, JSON.stringify(ing).slice(0, 300));
const plan = [{ slotId: 'p1', sheet: 'P1_通常', label: '通常', rows: ing.parsed.sheets[0].skuRows.map((r, i) => ({ no: i + 1, fnsku: r.fnsku, productName: r.productName, qty: String(r.plannedQty) })) }];
const m = svc.matchWorkbook(ing.parsed, plan);
const created = db.createRun({ sourceRunId: 90, deliveryDate: '2026-09-12', title: '9/12 納品分', matchSummary: svc.summarizeMatch(m),
  excelFile: { originalName: 'packlist_router.xlsx', storedPath: ing.storedPath, sha256: ing.sha256, fingerprint: ing.parsed.fingerprint, metadata: ing.parsed.metadata },
  groups: m.groups, createdBy: 'admin@test' });
const runId = created.runId;
let groupId = null, rows = [];
await t('有効化は本社 (セッション) のみ。端末からは 403', async () => {
  assert.equal((await call('POST', `/admin/runs/${runId}/activate`)).status, 403);
  const r = await call('POST', `/admin/runs/${runId}/activate`, { session: 'user', device: false });
  assert.equal(r.j.ok, true);
  const st = await call('GET', `/api/state?run=${runId}`);
  assert.equal(st.j.ok, true);
  groupId = st.j.groups[0].id;
  rows = [...st.j.rows].sort((a, b) => a.excel_row - b.excel_row);
  assert.equal(st.j.exportState.latest, null);
});
let box1 = null, box2 = null;
await t('端末: 箱を作って割当 (worker 必須・request_id 冪等)', async () => {
  assert.equal((await call('POST', '/api/boxes', { body: { pack_group_id: groupId, material_code: 'box140' } })).status, 400);
  box1 = (await call('POST', '/api/boxes', { body: { pack_group_id: groupId, material_code: 'box140', worker_id: memberId } })).j;
  assert.equal(box1.ok, true);
  box2 = (await call('POST', '/api/boxes', { body: { pack_group_id: groupId, material_code: 'box140', worker_id: memberId } })).j;
  for (const [i, r] of rows.entries()) {
    const p = await call('POST', '/api/placements', { body: { run_id: runId, row_id: r.id, box_id: box1.boxId, qty: r.planned_qty, worker_id: memberId, request_id: `rq${i}` } });
    assert.equal(p.j.ok, true, JSON.stringify(p.j));
  }
  const again = await call('POST', '/api/placements', { body: { run_id: runId, row_id: rows[0].id, box_id: box1.boxId, qty: rows[0].planned_qty, worker_id: memberId, request_id: 'rq0' } });
  assert.equal(again.j.already, true);
});
/** 確認した人の3列 (placement_id は API に出さないので DB を直接見る) */
const cwCols = (rowId) => db.getDB().prepare(
  'SELECT check_worker, check_worker_source, check_worker_placement_id FROM fbx_row_work WHERE row_id = ?').get(rowId) || {};
await t('確認した人: 投入で自動記録され、intent なしの更新 (旧画面の自動POST) は client_outdated', async () => {
  const st = await call('GET', `/api/state?run=${runId}`);
  const r = st.j.rows.find((x) => x.id === rows[0].id);
  assert.equal(r.check_worker, 'りようしゃ');          // 投入と同じトランザクションで入っている
  assert.equal(r.check_worker_source, 'auto');
  const before = cwCols(r.id);
  assert.ok(before.check_worker_placement_id > 0, '由来の投入を持つ');
  // 旧画面が投入後に送ってくる形 (intent なし) → 拒否。通すと自動で決めた担当を上書きしてしまう
  for (const name of ['しょくいん', 'りようしゃ']) {   // 別名でも同名でも拒否 (同名だと source だけ manual に化ける)
    const stale = await call('POST', `/api/rows/${r.id}/workers`, { body: { worker_id: staffId, check_worker: name } });
    assert.equal(stale.status, 409);
    assert.equal(stale.j.error, 'client_outdated');
    assert.deepEqual(cwCols(r.id), before, `intent なし (${name}) では3列とも変わらない`);
  }
  // ラベル貼り担当だけの更新は intent なしでも通る (旧画面は送らない・本社の互換のため)。担当の3列は動かない
  assert.equal((await call('POST', `/api/rows/${r.id}/workers`, { body: { worker_id: staffId, label_worker: 'たなか' } })).j.ok, true);
  assert.deepEqual(cwCols(r.id), before, 'label_worker だけの更新でも3列は変わらない');
  // 新画面が人の選択として送る形 (intent=manual) は通る → 以後は投入に連動しない (由来の投入を持たない)
  const pick = await call('POST', `/api/rows/${r.id}/workers`, { body: { worker_id: staffId, check_worker: 'しょくいん', check_worker_intent: 'manual' } });
  assert.equal(pick.j.ok, true, JSON.stringify(pick.j));
  const after = (await call('GET', `/api/state?run=${runId}`)).j.rows.find((x) => x.id === r.id);
  assert.equal(after.check_worker, 'しょくいん');
  assert.equal(after.check_worker_source, 'manual');
  assert.equal(cwCols(r.id).check_worker_placement_id, null);
});
await t('数を直す: POST /api/placements/:id/adjust (自端末の直近は利用者でも可)。直した後に元の数で入れ直せる', async () => {
  const st = await call('GET', `/api/state?run=${runId}`);
  const p = st.j.placements.find((x) => x.row_id === rows[0].id);
  const adj = await call('POST', `/api/placements/${p.id}/adjust`, { body: { worker_id: memberId, qty: p.qty - 1, request_id: 'adj-r1' } });
  assert.equal(adj.j.ok, true, JSON.stringify(adj.j));
  assert.equal(adj.j.to, p.qty - 1);
  const back = await call('POST', `/api/placements/${adj.j.placementId}/adjust`, { body: { worker_id: memberId, qty: p.qty, request_id: 'adj-r2' } });
  assert.equal(back.j.ok, true, JSON.stringify(back.j));
  assert.equal((await call('GET', `/api/state?run=${runId}`)).j.rows.find((r) => r.id === rows[0].id).placed, rows[0].planned_qty);
});
await t('箱の取消: 利用者は 403 / 職員PIN + 中身ありは 409 / 空箱は ok / 取消箱は state に void で残る', async () => {
  const r1 = await call('POST', `/api/boxes/${box2.boxId}/void`, { body: { worker_id: memberId, reason: 'x' } });
  assert.equal(r1.status, 403);
  const r2 = await call('POST', `/api/boxes/${box1.boxId}/void`, { body: { worker_id: staffId, pin: '2468', reason: '余り' } });
  assert.equal(r2.status, 409); assert.equal(r2.j.error, 'not_empty');
  const r3 = await call('POST', `/api/boxes/${box2.boxId}/void`, { body: { worker_id: staffId, pin: '2468', reason: '余り' } });
  assert.equal(r3.j.ok, true, JSON.stringify(r3.j));
  const st = await call('GET', `/api/state?run=${runId}`);
  assert.equal(st.j.boxes.find((b) => b.id === box2.boxId).status, 'void');
  assert.equal(st.j.boxes.find((b) => b.id === box1.boxId).amazon_name, 'P1 - B1');
});
await t('まとめ (readiness) と箱札は端末から見られる', async () => {
  const rd = await call('GET', `/api/readiness?run=${runId}`);
  assert.equal(rd.j.ok, true);
  assert.equal(rd.j.readiness.ok, false);
  assert.ok(rd.j.readiness.blockers.some((b) => b.code === 'open_boxes'));
  const pr = await call('GET', `/print/boxes?run=${runId}`, { raw: true });
  assert.equal(pr.status, 200);
  const html = await pr.text();
  assert.ok(html.includes(box1.boxCode) && !html.includes(box2.boxCode), '取消箱は箱札に出ない');
});
await t('本社: 出荷前チェック (開いた箱でブロック) → 箱を閉じる → 出力 → DL → STAアップ済み', async () => {
  const r0 = await call('GET', `/admin/runs/${runId}/readiness`, { session: 'user', device: false });
  assert.equal(r0.j.readiness.ok, false);
  const ex0 = await call('POST', `/admin/runs/${runId}/exports`, { session: 'user', device: false });
  assert.equal(ex0.status, 409); assert.equal(ex0.j.error, 'not_ready');
  const c = await call('POST', `/api/boxes/${box1.boxId}/close`, { body: { worker_id: memberId, measured_kg: 6.5, closed_reason: 'items_done' } });
  assert.equal(c.j.ok, true);
  const r1 = await call('GET', `/admin/runs/${runId}/readiness`, { session: 'user', device: false });
  assert.equal(r1.j.readiness.ok, true, JSON.stringify(r1.j.readiness.blockers));
  const ex = await call('POST', `/admin/runs/${runId}/exports`, { session: 'user', device: false });
  assert.equal(ex.j.ok, true, JSON.stringify(ex.j));
  assert.equal(ex.j.stale, false);
  const dl = await fetch(ORIGIN + ex.j.downloadUrl, { headers: { 'x-test-session': 'user' } });
  assert.equal(dl.status, 200);
  assert.ok(dl.headers.get('content-type').includes('spreadsheetml'));
  assert.ok(dl.headers.get('content-disposition').includes('packlist_router.xlsx'));
  const buf = Buffer.from(await dl.arrayBuffer());
  assert.equal(buf.subarray(0, 2).toString(), 'PK');
  const devDl = await call('GET', `/admin/exports/${ex.j.exportId}/download`, { raw: true });   // 端末からは不可 (GET 画面系はログインへ)
  assert.equal(devDl.status, 302);
  assert.ok(devDl.headers.get('location').endsWith('/login'));
  const sta = await call('POST', `/admin/runs/${runId}/sta-uploaded`, { body: { export_id: ex.j.exportId }, session: 'user', device: false });
  assert.equal(sta.j.ok, true, JSON.stringify(sta.j));
  assert.equal(db.getRun(runId).status, 'done');
});
await t('資材の編集は管理者のみ (user は 403)', async () => {
  assert.equal((await call('POST', '/admin/materials', { body: { code: 'box120', name: '120' }, session: 'user', device: false })).status, 403);
  const r = await call('POST', '/admin/materials', { body: { code: 'box120', name: '120サイズ', width_cm: 40, length_cm: 30, height_cm: 25 }, session: 'admin', device: false });
  assert.equal(r.j.ok, true, JSON.stringify(r.j));
});
await t('管理画面 (admin.ejs) が描画できる', async () => {
  const r = await fetch(`${BASE}/admin`, { headers: { 'x-test-session': 'admin' } });
  assert.equal(r.status, 200);
  const html = await r.text();
  assert.ok(html.includes('Excel出力') && html.includes('資材'));
});

console.log('■ PR2.5: picking 実行 → iPad から作業開始 → Excel 後付け (HTTP)');
const pkRows = ing.parsed.sheets[0].skuRows.map((r, i) => ({ no: i + 1, sku: r.sku, fnsku: r.fnsku, productName: '商品' + i, qty: String(r.plannedQty) }));
const pickingMem = [{ id: 501, delivery_date: '2026-09-25', run_at: '2026-09-03 10:00', plan_sheet_count: 1,
  result: JSON.stringify({ planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: pkRows }] }) }];
// 502 = 直近一覧に出ない古い実行 (getPickingRun では読めるが getPickingRuns には無い)
const pickingOld = { id: 502, delivery_date: '2026-08-01', run_at: '2026-08-01 10:00', plan_sheet_count: 1, result: pickingMem[0].result };
_setPickingSource(async () => ({ getPickingRuns: () => pickingMem, getPickingRun: (id) => [...pickingMem, pickingOld].find((r) => r.id === Number(id)) || null }));
let pkRunId = null;
await t('GET /api/runs に「まだ始めていない picking 実行」が出る', async () => {
  const r = await call('GET', '/api/runs');
  assert.equal(r.j.ok, true);
  const p = r.j.pickingRuns.find((x) => x.id === 501);
  assert.ok(p); assert.equal(p.boxRun, null); assert.equal(p.deliveryDate, '2026-09-25');
});
await t('端末から POST /api/runs/from-picking → active な納品回。二度目は already', async () => {
  const r = await call('POST', '/api/runs/from-picking', { body: { source_run_id: 501 } });
  assert.equal(r.j.ok, true, JSON.stringify(r.j)); assert.equal(r.j.created, true);
  pkRunId = r.j.runId;
  const again = await call('POST', '/api/runs/from-picking', { body: { source_run_id: 501 } });
  assert.equal(again.j.already, true); assert.equal(again.j.runId, pkRunId);
  assert.equal((await call('POST', '/api/runs/from-picking', { body: { source_run_id: 999 } })).status, 403);   // 一覧に無い
  // 一覧に出ない古い実行は端末からは 403、本社 (セッション) なら作れる
  const old = await call('POST', '/api/runs/from-picking', { body: { source_run_id: 502 } });
  assert.equal(old.status, 403); assert.equal(old.j.error, 'not_recent');
  const oldAdmin = await call('POST', '/admin/runs/from-picking', { body: { source_run_id: 502 }, session: 'user', device: false });
  assert.equal(oldAdmin.j.ok, true, JSON.stringify(oldAdmin.j));
  const list = await call('GET', '/api/runs');
  assert.equal(list.j.pickingRuns.find((x) => x.id === 501).boxRun.id, pkRunId);
  const st = await call('GET', `/api/state?run=${pkRunId}`);
  assert.equal(st.j.run.status, 'active'); assert.equal(st.j.groups[0].display_name, '通常'); assert.equal(st.j.rows.length, 2);
});
await t('作業を終える: 利用者は 403 / 職員PIN + 未投入あり → 409 incomplete (一覧) / acknowledge で done', async () => {
  const st = await call('GET', `/api/state?run=${pkRunId}`);
  const gid = st.j.groups[0].id;
  const bx = (await call('POST', '/api/boxes', { body: { pack_group_id: gid, material_code: 'box140', worker_id: memberId } })).j;
  const row = st.j.rows[0];
  assert.equal((await call('POST', '/api/placements', { body: { run_id: pkRunId, row_id: row.id, box_id: bx.boxId, qty: 1, worker_id: memberId, request_id: 'fin-1' } })).j.ok, true);
  assert.equal((await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: memberId } })).status, 403);
  // 送る数の修正は職員のみ
  assert.equal((await call('POST', `/api/rows/${row.id}/send-qty`, { body: { worker_id: memberId, send_qty: 2 } })).status, 403);
  const sq = await call('POST', `/api/rows/${row.id}/send-qty`, { body: { worker_id: staffId, pin: '2468', send_qty: 2, reason: 'stock_short' } });
  assert.equal(sq.j.ok, true, JSON.stringify(sq.j)); assert.equal(sq.j.shortage, row.planned_qty - 2);
  const open = await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: staffId, pin: '2468' } });
  assert.equal(open.status, 409); assert.equal(open.j.error, 'open_boxes');
  assert.equal((await call('POST', `/api/boxes/${bx.boxId}/close`, { body: { worker_id: memberId, measured_kg: 1.2 } })).j.ok, true);
  const inc = await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: staffId, pin: '2468' } });
  assert.equal(inc.status, 409); assert.equal(inc.j.error, 'incomplete'); assert.equal(inc.j.rows.length, 2);
  assert.equal(db.getRun(pkRunId).status, 'active');
  const done = await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: staffId, pin: '2468', acknowledge: true } });
  assert.equal(done.j.ok, true, JSON.stringify(done.j));
  assert.equal(done.j.notShipped, 2);
  assert.equal(db.getRun(pkRunId).status, 'done');
});
// 以降の添付テストは active な回で行う (done の回にも添付はできるが、作業中の回で確認する)
{
  const r = await call('POST', '/admin/runs/from-picking', { body: { source_run_id: 502 }, session: 'user', device: false });
  assert.equal(r.j.already, true);
  pkRunId = r.j.runId;
}
await t('本社: POST /admin/runs/:id/excel (multipart) で Excel を添付 → 突合結果、readiness の no_excel が消える', async () => {
  const before = await call('GET', `/admin/runs/${pkRunId}/readiness`, { session: 'user', device: false });
  assert.ok(before.j.readiness.blockers.some((b) => b.code === 'no_excel'));
  const fd = new FormData();
  fd.append('excel', new Blob([fs.readFileSync(fixture)]), 'packlist_attach.xlsx');
  const r = await fetch(`${BASE}/admin/runs/${pkRunId}/excel`, { method: 'POST', body: fd, headers: { 'x-test-session': 'user', Origin: ORIGIN } });
  const j = await r.json();
  assert.equal(j.ok, true, JSON.stringify(j));
  assert.equal(j.groups[0].matched, 2);
  const after = await call('GET', `/admin/runs/${pkRunId}/readiness`, { session: 'user', device: false });
  assert.ok(!after.j.readiness.blockers.some((b) => b.code === 'no_excel'));
  assert.equal(after.j.readiness.groups[0].excelAttached, true);
  assert.equal((await call('POST', `/admin/runs/${pkRunId}/excel`, { session: 'user', device: false })).status, 400);   // ファイルなし
});

console.log('■ PR3: 重量補助 (実測の登録・上限は職員の承認)');
const wRun = db.createRunFromPicking({ pickingRun: { id: 700, delivery_date: '2026-10-05' }, planSheets: [
  { slotId: 'p1', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, fnsku: 'X0RTW00001', productName: '重さテスト商品', qty: '10' }] },
], createdBy: 'test' });
const wState = db.getRunState(wRun.runId);
const wGid = wState.groups[0].id, wRowId = wState.rows[0].id;

await t('端末: 実測「10個で2050g」を登録 → 採用値が実測になる。履歴が読めて、取り消すと戻る', async () => {
  assert.equal((await call('POST', '/api/weights', { body: { fnsku: 'X0RTW00001', sample_qty: 0, total_g: 100, worker_id: memberId } })).status, 400);
  const ok = await call('POST', '/api/weights', { body: { fnsku: 'X0RTW00001', sample_qty: 10, total_g: 2050, worker_id: memberId, run_id: wRun.runId } });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
  assert.equal(ok.j.unitG, 205);
  const st = await call('GET', `/api/state?run=${wRun.runId}`);
  assert.equal(st.j.weights.X0RTW00001.unitG, 205);
  assert.equal(st.j.weights.X0RTW00001.source, 'measured');
  assert.deepEqual(st.j.weightLimits, { targetG: 28000, limitG: 30000, snapshotted: true });
  const hist = await call('GET', '/api/weights?fnsku=X0RTW00001');
  assert.equal(hist.j.measurements.length, 1);
  assert.equal(hist.j.rules.limit_g, 30000);
  // 別の回を名乗る取消は職員へ回す (単重は全回共通のマスタ)
  assert.equal((await call('POST', `/api/weights/${ok.j.id}/revoke`, { body: { worker_id: memberId } })).status, 403);
  assert.equal((await call('POST', `/api/weights/${ok.j.id}/revoke`, { body: { worker_id: memberId, run_id: wRun.runId } })).status, 200);
  assert.equal((await call('POST', `/api/weights/${ok.j.id}/revoke`, { body: { worker_id: memberId, run_id: wRun.runId } })).status, 409);
  // 取り消したので単重は未登録に戻る = 推定から外れる
  const st2 = await call('GET', `/api/state?run=${wRun.runId}`);
  assert.equal(st2.j.weights.X0RTW00001, undefined);
});

await t('箱クローズ: 上限超えは 409 over_limit → 職員PINの承認 (override) を添えれば閉じられる', async () => {
  const bx = await call('POST', '/api/boxes', { body: { pack_group_id: wGid, material_code: 'box140', worker_id: memberId } });
  assert.equal(bx.status, 200, JSON.stringify(bx.j));
  const pl = await call('POST', '/api/placements', { body: { run_id: wRun.runId, row_id: wRowId, box_id: bx.j.boxId, qty: 10, worker_id: memberId, request_id: 'rw1' } });
  assert.equal(pl.status, 200, JSON.stringify(pl.j));
  const ng = await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31 } });
  assert.equal(ng.status, 409);
  assert.equal(ng.j.error, 'over_limit');
  assert.equal(db.getBox(bx.j.boxId).status, 'open');
  // override は職員PINが要る (利用者が自分で押しても通らない)
  assert.equal((await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31, override: true } })).status, 403);
  // 一般のポータルセッションでも承認にはならない (Codex PR3 #1: hasSessionAccess は職員である保証がない)
  assert.equal((await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31, override: true }, session: 'user', device: false })).status, 403);
  assert.equal((await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31, override: true, auth_worker_id: staffId, auth_pin: '0000' } })).status, 403);
  db._clearPinFails();
  const ok = await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31, override: true, auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
  assert.equal(ok.j.overLimit, true);
  assert.equal(ok.j.overTarget, true);
  assert.equal(db.getBox(bx.j.boxId).measured_weight_kg, 31);
  assert.equal(db.getBox(bx.j.boxId).limit_override_by, 'しょくいん');
});

await t('実測の登録は納品回と商品の対応を検証する (別の回の商品・存在しない FNSKU は 409)', async () => {
  assert.equal((await call('POST', '/api/weights', { body: { fnsku: 'X0RTW00001', sample_qty: 1, total_g: 10, worker_id: memberId } })).status, 400, 'run_id なし');
  const bad = await call('POST', '/api/weights', { body: { fnsku: 'X0NOSUCH01', sample_qty: 1, total_g: 10, run_id: wRun.runId, worker_id: memberId } });
  assert.equal(bad.status, 409);
  assert.equal(bad.j.error, 'not_in_run');
});

await t('過去回・別回の重さの取消は、一般のポータルセッションだけでは通らない (管理者 or 職員PIN)', async () => {
  const m = await call('POST', '/api/weights', { body: { fnsku: 'X0RTW00001', sample_qty: 5, total_g: 500, run_id: wRun.runId, worker_id: memberId } });
  assert.equal(m.status, 200, JSON.stringify(m.j));
  assert.equal((await call('POST', `/api/weights/${m.j.id}/revoke`, { body: { worker_id: memberId, run_id: 999999 } })).status, 403, '別の回を名乗る取消');
  assert.equal((await call('POST', `/api/weights/${m.j.id}/revoke`,
    { body: { worker_id: memberId, run_id: 999999, as_staff: true }, session: 'user', device: false })).status, 403, '一般セッション + as_staff だけでは通らない');
  db._clearPinFails();
  const ok = await call('POST', `/api/weights/${m.j.id}/revoke`,
    { body: { worker_id: memberId, run_id: 999999, as_staff: true, auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
});

await t('本社: 商品ごとの単重一覧 / ルール変更は管理者のみ・目標>上限は 400', async () => {
  const wl = await call('GET', `/admin/runs/${wRun.runId}/weights`, { session: 'user', device: false });
  assert.equal(wl.status, 200);
  assert.equal(wl.j.weights.length, 1);
  assert.equal(wl.j.weights[0].fnsku, 'X0RTW00001');
  assert.equal(wl.j.runLimits.limitG, 30000);
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000 }, session: 'user', device: false })).status, 403);
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 31000, limit_g: 30000 }, session: 'admin', device: false })).status, 400);
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 27000, limit_g: 29000 }, session: 'admin', device: false })).status, 200);
  assert.equal(db.getWeightRules().limit_g, 29000);
  await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000 }, session: 'admin', device: false });
});

server.close();
console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
