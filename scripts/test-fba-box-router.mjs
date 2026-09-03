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
const { default: router } = await import('../apps/fba-box/router.js');

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

server.close();
console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
