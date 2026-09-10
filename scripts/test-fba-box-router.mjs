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
const notify = await import('../apps/fba-box/notify.js');
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
  // 応答喪失後の再送でも、新規成功と同じ形 (確認した人と由来) を返す — 画面が通信断のときだけ
  // 「誰が確認した人になったか」を知らせられない、を防ぐ (Codex R4 medium#1)
  assert.equal(again.j.checkWorker, 'りようしゃ');
  assert.equal(again.j.checkWorkerSource, 'auto');
  assert.equal(again.j.placementId > 0, true);
});
/** 確認した人の3列 (placement_id は API に出さないので DB を直接見る) */
const cwCols = (rowId) => db.getDB().prepare(
  'SELECT check_worker, check_worker_source, check_worker_placement_id FROM fbx_row_work WHERE row_id = ?').get(rowId) || {};
await t('応答喪失後の送り直しは、作業者が無効になっていても前回の結果を返す (PQ-R2 high#2)', async () => {
  const r0 = rows[0];
  // 残数を1つ空けてから、一時的な作業者で1個入れる (箱は既存の box1 を使う = 後始末を増やさない)
  const st = await call('GET', `/api/state?run=${runId}`);
  const mine = st.j.placements.find((x) => x.row_id === r0.id);
  assert.equal((await call('POST', `/api/placements/${mine.id}/revoke`, { body: { worker_id: memberId } })).j.ok, true);
  const tmp = await call('POST', '/api/workers', { body: { display_name: 'いちじ', worker_type: 'member', auth_worker_id: staffId, auth_pin: '2468' } });
  assert.equal(tmp.j.ok, true, JSON.stringify(tmp.j));
  const p = { run_id: runId, row_id: r0.id, box_id: box1.boxId, qty: 1, worker_id: tmp.j.id, request_id: 'lost-1' };
  const first = await call('POST', '/api/placements', { body: p });
  assert.equal(first.j.ok, true, JSON.stringify(first.j));
  // ここで応答が失われた体。その間に職員がこの作業者を無効にした
  assert.equal((await call('POST', `/api/workers/${tmp.j.id}/active`, { body: { active: false, auth_worker_id: staffId, auth_pin: '2468' } })).j.ok, true);
  assert.equal((await call('POST', '/api/placements', { body: Object.assign({}, p, { request_id: 'new-1' }) })).j.error, 'worker_required', '新規は今までどおり止める');
  const again = await call('POST', '/api/placements', { body: p });   // 同じ request_id で送り直す
  assert.equal(again.status, 200, JSON.stringify(again.j));
  assert.equal(again.j.already, true, '登録済みなので前回の結果を返す (入れ直しを促さない)');
  assert.equal(again.j.placementId, first.j.placementId);
  assert.equal(again.j.checkWorker, first.j.checkWorker);
  const conflict = await call('POST', '/api/placements', { body: Object.assign({}, p, { qty: 2 }) });
  assert.equal(conflict.status, 409);
  assert.equal(conflict.j.error, 'idempotency_conflict', '内容が違う同じ操作IDは今までどおり弾く');
  // 期限を空文字で送っても、同じ内容の再送は conflict にならない (PQ-R3 medium#1)
  const e1 = await call('POST', '/api/placements', { body: Object.assign({}, p, { request_id: 'exp-1', expiry: '', worker_id: memberId }) });
  assert.equal(e1.j.ok, true, JSON.stringify(e1.j));
  const e2 = await call('POST', '/api/placements', { body: Object.assign({}, p, { request_id: 'exp-1', expiry: '', worker_id: memberId }) });
  assert.equal(e2.j.already, true, JSON.stringify(e2.j));
  const e3 = await call('POST', '/api/placements', { body: Object.assign({}, p, { request_id: 'exp-1', worker_id: memberId }) });   // expiry 省略
  assert.equal(e3.j.already, true, '空文字と未指定は同じ内容として扱う');
  // 取消済みの再送は「記録できています」と返さない (PQ-R3 high#5)
  assert.equal((await call('POST', `/api/placements/${e1.j.placementId}/revoke`, { body: { worker_id: memberId } })).j.ok, true);
  const afterRevoke = await call('POST', '/api/placements', { body: Object.assign({}, p, { request_id: 'exp-1', expiry: '', worker_id: memberId }) });
  assert.equal(afterRevoke.status, 409);
  assert.equal(afterRevoke.j.error, 'placement_revoked', JSON.stringify(afterRevoke.j));
  // 後始末: 一時の投入を消して、元の投入を戻す
  assert.equal((await call('POST', `/api/placements/${first.j.placementId}/revoke`, { body: { worker_id: memberId } })).j.ok, true);
  const back = await call('POST', '/api/placements', { body: { run_id: runId, row_id: r0.id, box_id: mine.box_id, qty: mine.qty, worker_id: memberId, request_id: 'restore-1' } });
  assert.equal(back.j.ok, true, JSON.stringify(back.j));
});

await t('職員の本人確認だけの API: PIN が違えば通らない・通れば監査に残る (PQ-R3 high#4)', async () => {
  assert.equal((await call('POST', '/api/staff/verify', { body: { auth_worker_id: staffId, auth_pin: '0000', purpose: 'discard_broken_pending' } })).status, 403);
  assert.equal((await call('POST', '/api/staff/verify', { body: { auth_worker_id: memberId, auth_pin: '2468', purpose: 'x' } })).status, 403, '利用者は通さない');
  const okv = await call('POST', '/api/staff/verify', { body: { auth_worker_id: staffId, auth_pin: '2468', purpose: 'discard_broken_pending', detail: '{"reason":"unknown_shape"}' } });
  assert.equal(okv.j.ok, true, JSON.stringify(okv.j));
  assert.equal(okv.j.approvedBy, 'しょくいん');
  assert.ok(db.listEvents(5).some((e) => e.action === 'staff_verify' && JSON.parse(e.payload || '{}').purpose === 'discard_broken_pending'), '監査に残る');
});

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
await t('投入の送信キュー (place-queue.js) が作業画面と同じゲートの内側で配信される', async () => {
  const r = await call('GET', '/place-queue.js', { raw: true });
  assert.equal(r.status, 200);
  assert.ok((r.headers.get('content-type') || '').includes('javascript'), r.headers.get('content-type'));
  const js = await r.text();
  assert.ok(js.includes('createPlaceQueue'), 'window.createPlaceQueue を出している');
  // 作業画面が実際にこの URL を読んでいる (パスを変えたら気づけるように)
  const page = await (await call('GET', '/', { raw: true })).text();
  assert.ok(page.includes('/apps/fba-box/place-queue.js'), '作業画面が読み込んでいる');
  // 端末未登録なら画面と同じく /enroll へ (JS だけ素通しにしない)
  const anon = await fetch(`${BASE}/place-queue.js`, { redirect: 'manual' });
  assert.equal(anon.status, 302);
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
  // 完了したら本社の Google Chat へ (中原さん 2026-09-10)。本物には投げない
  delete process.env.PUBLIC_BASE_URL;
  process.env[notify.WEBHOOK_ENV] = 'https://chat.example/fba-box';
  const sent = [];
  notify.setNotifySender(async (url, text) => { sent.push({ url, text }); });
  const done = await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: staffId, pin: '2468', acknowledge: true } });
  assert.equal(done.j.ok, true, JSON.stringify(done.j));
  assert.equal(done.j.notShipped, 2);
  assert.equal(db.getRun(pkRunId).status, 'done');
  await new Promise((r) => setTimeout(r, 150));   // 通知は応答を待たずに送る (outbox → notify-outbox.js)
  assert.equal(db.listNotifyOutbox(pkRunId)[0].status, 'sent', '完了と同じトランザクションで積んだ送信待ちが sent になる');
  assert.equal(sent.length, 1, '完了で 1 回だけ送る');
  assert.equal(sent[0].url, 'https://chat.example/fba-box');
  assert.ok(sent[0].text.includes('FBA箱詰めが終わりました'), sent[0].text);
  assert.ok(sent[0].text.includes(`<https://bfaith-portal.onrender.com/apps/fba-box/admin/runs/${pkRunId}/report|`), 'リンクは本社向けまとめ・Host ヘッダー (127.0.0.1) からは作らない: ' + sent[0].text);
  assert.ok(sent[0].text.includes('⚠ 予定と違う商品'), '送る数を減らした行・入れなかった行があるので');
  assert.ok(sent[0].text.includes('しょくいん'), '終えた人');
  const nev = db.listEvents(200, pkRunId).find((e) => e.action === 'notify_run_done');
  assert.ok(nev && nev.ok, '送れたことを履歴に残す: ' + JSON.stringify(nev));
  // 二度押し (already) では送らない
  assert.equal((await call('POST', `/api/runs/${pkRunId}/finish`, { body: { worker_id: staffId, pin: '2468', acknowledge: true } })).j.already, true);
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(sent.length, 1, '二度押しでは送らない');
  notify.setNotifySender(null);
  delete process.env[notify.WEBHOOK_ENV];
});
await t('本社: 完了通知のリンク先 GET /admin/runs/:id/report — セッションで開ける / 予定と違う行は赤 / 端末 Cookie だけでは /login へ', async () => {
  const r = await call('GET', `/admin/runs/${pkRunId}/report`, { session: 'user', device: false, raw: true });
  assert.equal(r.status, 200);
  const html = await r.text();
  assert.ok(html.includes('送り状') && html.includes('箱ラベル'), '送り状・箱ラベル用');
  assert.ok((html.match(/<tr class="alert">/g) || []).length >= 1, '予定と違う行に赤');
  assert.ok(html.includes('Amazon の箱') && html.includes('kg'), '箱の一覧');
  assert.ok(html.includes('id="tsv"'), 'コピー用のデータ');
  const noSess = await call('GET', `/admin/runs/${pkRunId}/report`, { raw: true });
  assert.equal(noSess.status, 302, '端末 Cookie だけ (ポータル未ログイン) では見られない');
  assert.equal(noSess.headers.get('location'), '/login');
  assert.equal((await call('GET', '/admin/runs/999999/report', { session: 'user', device: false, raw: true })).status, 404);
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

await t('箱クローズ: 上限超えは 409 over_limit → override を添えれば閉じられる (PIN は要らない・決めた人が残る)', async () => {
  const bx = await call('POST', '/api/boxes', { body: { pack_group_id: wGid, material_code: 'box140', worker_id: memberId } });
  assert.equal(bx.status, 200, JSON.stringify(bx.j));
  const pl = await call('POST', '/api/placements', { body: { run_id: wRun.runId, row_id: wRowId, box_id: bx.j.boxId, qty: 10, worker_id: memberId, request_id: 'rw1' } });
  assert.equal(pl.status, 200, JSON.stringify(pl.j));
  // 🚨 必ず二段階。override を付けない1回目は断る (「うっかり閉じた」を作らない)
  const ng = await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31 } });
  assert.equal(ng.status, 409);
  assert.equal(ng.j.error, 'over_limit');
  assert.equal(db.getBox(bx.j.boxId).status, 'open');
  // 職員PIN は要らない (中原さん 2026-09-10: 職員がいないと箱が閉じられず作業が止まるため)。
  // そのかわり「このまま閉じる」を選んだ作業者の名前を必ず残す
  const ok = await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { worker_id: memberId, measured_kg: 31, override: true } });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
  assert.equal(ok.j.overLimit, true);
  assert.equal(ok.j.overTarget, true);
  assert.equal(db.getBox(bx.j.boxId).measured_weight_kg, 31);
  assert.equal(db.getBox(bx.j.boxId).limit_override_by, 'りようしゃ', '決めた人 = 閉じた作業者本人');
  assert.ok(db.getBox(bx.j.boxId).limit_override_at);
  // 作業者が分からない送信は今までどおり通さない (誰が決めたか残らないため)
  assert.equal((await call('POST', `/api/boxes/${bx.j.boxId}/close`, { body: { measured_kg: 31, override: true } })).status, 400);
  // 本社の出荷前チェックには必ず出る (最後の歯止め)
  const rd = await call('GET', `/api/readiness?run=${wRun.runId}`);
  const w = (rd.j.readiness.warnings || []).find((x) => x.code === 'over_weight_limit');
  assert.ok(w, JSON.stringify((rd.j.readiness.warnings || []).map((x) => x.code)));
  assert.equal(w.boxes[0].approvedBy, 'りようしゃ');
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

console.log('■ 積み方区分 (土台・重い): iPad の API / 本社の設定・取込 / 重いの基準 / 管理画面');
const pcRun = db.createRunFromPicking({ pickingRun: { id: 800, delivery_date: '2026-10-20' }, planSheets: [
  { slotId: 'p1', sheet: 'P1_通常', label: '通常', rows: [
    { no: 1, sku: 'SKU-PC-HEAVY', fnsku: 'X0RPC00001', productName: '重い商品', qty: '3' },
    { no: 2, sku: 'SKU-PC-FLAT', fnsku: 'X0RPC00002', productName: '平らな商品', qty: '3' },
  ] },
], createdBy: 'test' });
db.upsertWeightRef({ fnsku: 'X0RPC00001', asin: 'B0RPC1', weightG: 800, raw: '0.80', status: 'ok' });

await t('端末: POST /api/packing-class — 作業者必須・run_id 必須・回にない商品は 409・不正値 400・オリジン検査。付けると state.packing に出る', async () => {
  assert.equal((await call('POST', '/api/packing-class', { body: { fnsku: 'X0RPC00002', class: 'base' } })).status, 400, '作業者なし');
  assert.equal((await call('POST', '/api/packing-class', { body: { fnsku: 'X0RPC00002', class: 'base', worker_id: memberId } })).status, 400, 'run_id なし');
  const nir = await call('POST', '/api/packing-class', { body: { fnsku: 'X0NOSUCH02', class: 'base', worker_id: memberId, run_id: pcRun.runId } });
  assert.equal(nir.status, 409); assert.equal(nir.j.error, 'not_in_run');
  assert.equal((await call('POST', '/api/packing-class', { body: { fnsku: 'X0RPC00002', class: 'top', worker_id: memberId, run_id: pcRun.runId } })).status, 400, '不正値');
  assert.equal((await call('POST', '/api/packing-class', { body: { fnsku: 'X0RPC00002', class: 'base', worker_id: memberId, run_id: pcRun.runId }, origin: false })).status, 403, 'オリジン検査');
  const ok = await call('POST', '/api/packing-class', { body: { fnsku: 'X0RPC00002', class: 'base', worker_id: memberId, run_id: pcRun.runId } });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
  assert.equal(ok.j.effective.cls, 'base'); assert.equal(ok.j.effective.source, 'manual');
  const st = await call('GET', `/api/state?run=${pcRun.runId}`);
  assert.equal(st.j.packing.X0RPC00002.cls, 'base');
  assert.equal(st.j.packing.X0RPC00001.cls, null, '基準が未設定なので重いは自動で付かない');
  assert.deepEqual(st.j.packingRules, { heavyMinG: null });
});

await t('本社: 重いの基準を 500g にすると 800g の商品が自動で「重い」。基準の変更は管理者のみ。キー無しの保存は基準を引き継ぐ', async () => {
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: 500 }, session: 'user', device: false })).status, 403);
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: -5 }, session: 'admin', device: false })).status, 400);
  const ok = await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: 500 }, session: 'admin', device: false });
  assert.equal(ok.status, 200, JSON.stringify(ok.j)); assert.equal(ok.j.heavyMinG, 500);
  const st = await call('GET', `/api/state?run=${pcRun.runId}`);
  assert.deepEqual([st.j.packing.X0RPC00001.cls, st.j.packing.X0RPC00001.source], ['heavy', 'weight']);
  assert.deepEqual(st.j.packingRules, { heavyMinG: 500 });
  await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000 }, session: 'admin', device: false });
  assert.equal(db.getWeightRules().heavy_min_g, 500, 'キー無し = 引き継ぐ');
  assert.equal((await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: null }, session: 'admin', device: false })).j.heavyMinG, null, 'null = 止める');
  await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: 500 }, session: 'admin', device: false });
});

await t('本社: POST /admin/packing-class は一般セッションで可 (回なし)。端末Cookieだけでは不可。「通常」で自動の重いを止める', async () => {
  assert.equal((await call('POST', '/admin/packing-class', { body: { fnsku: 'X0RPC00001', class: 'normal' } })).status, 403, '端末Cookieだけ');
  const ok = await call('POST', '/admin/packing-class', { body: { fnsku: 'X0RPC00001', class: 'normal' }, session: 'user', device: false });
  assert.equal(ok.status, 200, JSON.stringify(ok.j));
  assert.deepEqual([ok.j.effective.cls, ok.j.effective.source, ok.j.effective.updatedBy], ['normal', 'manual', 'session:user@test']);
  assert.equal((await call('POST', '/admin/packing-class', { body: { fnsku: '', class: 'normal' }, session: 'user', device: false })).status, 400);
  const st = await call('GET', `/api/state?run=${pcRun.runId}`);
  assert.equal(st.j.packing.X0RPC00001.cls, 'normal');
});

await t('本社: 画像・重さの一覧 (GET /admin/runs/:id/images) に積み方 (有効値) が載る', async () => {
  const img = await import('../apps/fba-box/images.js');
  img._setAttrsSource(async () => []);
  img._setFetcher(async () => ({ ok: true, result: { image: null, dimensions: { weight: '-' } } }));
  const r = await call('GET', `/admin/runs/${pcRun.runId}/images`, { session: 'user', device: false });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  const heavy = r.j.items.find((x) => x.fnsku === 'X0RPC00001'), flat = r.j.items.find((x) => x.fnsku === 'X0RPC00002');
  assert.deepEqual([heavy.packing.cls, heavy.packing.source], ['normal', 'manual']);
  assert.deepEqual([flat.packing.cls, flat.packing.source], ['base', 'manual']);
});

await t('本社: GET /admin (管理画面) が描画され、積み方の欄と最近の変更 (端末で付けた商品) が出る', async () => {
  const r = await call('GET', '/admin', { session: 'admin', device: false, raw: true });
  assert.equal(r.status, 200);
  const html = await r.text();
  assert.ok(html.includes('積み方 (土台・重い)'));
  assert.ok(html.includes('X0RPC00002'), '最近変わった積み方');
  assert.ok(html.includes('id="wr-heavy"'));
  assert.ok(html.includes('id="pk-import"'), '管理者には取込ボタン');
  const u = await call('GET', '/admin', { session: 'user', device: false, raw: true });
  assert.equal((await u.text()).includes('id="pk-import"'), false, '一般セッションには取込ボタンを出さない');
});

await t('本社: 土台シートからの取込 (管理者のみ)。SKU 属性 → picking 実行 → この DB の行 の順で FNSKU を引き、1つに決まるものだけ入れる', async () => {
  _setPickingSource(async () => ({
    getPickingRuns: () => [{ id: 900 }],
    getPickingRun: (id) => (Number(id) === 900 ? { id: 900, result: JSON.stringify({ planSheets: [{ rows: [{ sku: 'sku-from-picking', fnsku: 'X0IMP00002' }] }] }) } : null),
    getFbaSkuAttrs: () => [
      { amazon_sku: 'sku-from-attrs', asin: 'B0A', fnsku: 'X0IMP00001' },
      { amazon_sku: 'sku-multi', asin: 'B0M', fnsku: 'X0IMP00003' }, { amazon_sku: 'sku-multi', asin: 'B0M', fnsku: 'X0IMP00004' },
    ],
    getDodaiMaster: () => [{ sku: 'sku-from-attrs' }, { sku: 'sku-from-picking' }, { sku: 'sku-pc-flat' }, { sku: 'sku-multi' }, { sku: 'sku-none' }],
  }));
  assert.equal((await call('POST', '/admin/packing-class/import-dodai', { session: 'user', device: false })).status, 403);
  const r = await call('POST', '/admin/packing-class/import-dodai', { session: 'admin', device: false });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  assert.equal(r.j.total, 5);
  assert.equal(r.j.imported, 2, 'attrs 由来 + picking 由来');
  assert.equal(r.j.keptManual, 1, 'sku-pc-flat は端末で付けた manual を保持 (この DB の行から大文字小文字を問わず解決)');
  assert.deepEqual(r.j.unresolved, ['sku-none']);
  assert.equal(r.j.ambiguous.length, 1); assert.equal(r.j.ambiguous[0].sku, 'sku-multi');
  assert.equal(db.getProductFlags(['X0IMP00001'])[0].source, 'sheet_import');
  assert.equal(db.getProductFlags(['X0IMP00002'])[0].seller_sku, 'sku-from-picking');
  _setPickingSource(async () => ({ getPickingRuns: () => [], getPickingRun: () => null, getFbaSkuAttrs: () => [], getDodaiMaster: () => [] }));
  assert.equal((await call('POST', '/admin/packing-class/import-dodai', { session: 'admin', device: false })).status, 409, '空のマスタは取り込まない');
  _setPickingSource(async () => { throw new Error('fba.db down'); });
  assert.equal((await call('POST', '/admin/packing-class/import-dodai', { session: 'admin', device: false })).status, 502);
});
await call('POST', '/admin/weight-rules', { body: { target_g: 28000, limit_g: 30000, heavy_min_g: null }, session: 'admin', device: false });

await t('本社の「完了にする」でも本社の Google Chat へ知らせる (終えた人 = ポータルの人・リンクは Host から作らない)', async () => {
  delete process.env.PUBLIC_BASE_URL;
  process.env[notify.WEBHOOK_ENV] = 'https://chat.example/fba-box';
  const sent = [];
  notify.setNotifySender(async (url, text) => { sent.push(text); });
  const c = db.createRunFromPicking({ pickingRun: { id: 7771, delivery_date: '2026-09-30' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-adm', fnsku: 'X0ADM00001', productName: '本社で完了', qty: '2' }] }], createdBy: 't' });
  const r = await call('POST', `/admin/runs/${c.runId}/finish`, { body: { acknowledge: true }, session: 'user', device: false });
  assert.equal(r.j.ok, true, JSON.stringify(r.j));
  await new Promise((res) => setTimeout(res, 150));
  assert.equal(sent.length, 1, JSON.stringify(db.listNotifyOutbox(c.runId)));
  assert.ok(sent[0].includes('user@test'), '終えた人 = ポータルの人: ' + sent[0]);
  assert.ok(sent[0].includes(`<https://bfaith-portal.onrender.com/apps/fba-box/admin/runs/${c.runId}/report|`), sent[0]);
  notify.setNotifySender(null);
  delete process.env[notify.WEBHOOK_ENV];
});

server.close();
console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
