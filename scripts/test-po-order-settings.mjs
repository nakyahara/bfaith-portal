/**
 * test-po-order-settings.mjs — 発注の設定を書く 1 つの部品 (apps/purchase-orders/order-settings.js)・発注ロットの出どころ (logic.js)・
 *   NE のロットを一度だけ写す script (apps/purchase-orders/scripts/copy-ne-order-lot.mjs) (10/9 中原さん)
 *
 * 発注アプリの SQLite = 一時の DATA_DIR の warehouse-mirror.db (本番の DB に触らない)。発注アプリの router を HTTP 越しにも通す。
 * 固定する契約:
 *   S 先に読んだ値の確かめ: 印 (seen) が無い = 428・違う = 409 stale (何も書かない)・'overwrite' は新商品の登録 (master-edit:new) だけ・
 *     列の値の印 (seen.fields・発注画面のグループの紐付け)・同じミリ秒に 2 回書いても印は進む
 *   W 書き込み: 1 つの取引 (新しいグループ → 商品の行 → 記録)・中身が違う同じ ID = 409・代表の仕入先でない発注条件 = 400 (変えるときだけ)・
 *     dry-run は何も残さない・変わらない = 書かない (記録も増えない)・記録 = だれ・どの画面・前と後・request_id
 *   E 入口が 2 つ (発注アプリのマスタ管理の API・マスタの入力の部品の呼び方) で同じ行・同じ記録・お互いの変更で 409
 *   A マスタ管理の一覧: 発注ロットの列・印・発注ロットだけの行は「未紐付け」・NE 登録待ち (マスタの入力で作った・商品管理リストに無い)・
 *     紐付けの削除は発注ロットを残す・グループの削除も印・使われているグループは消さない
 *   L 発注ロットの出どころ: 写す前 (ne) = 今までどおり NE の値・写した後 (app) = 発注アプリの order_lot だけ (無い = すすめる数が出ない)
 *   C 写しの script: dry-run は何も書かない・分け方 (行を作る・埋める・同じ・発注アプリを残す・NE が空・おかしい・重なり)・止まる商品 (Codex #1674 R1 High 2・3)・
 *     --apply は ne のときだけ・--reapply は件数の確かめつき (R1 Medium 2)・
 *     --apply は 1 つの取引で写して app に切り替える・もう一度流しても同じ・--diff・--back-to-ne・DATA_DIR が無い = 止める
 *   U 未紐付けタブの知らせ (写した後): 発注ロットが空の取扱中・NE と違う
 * 使い方: node scripts/test-po-order-settings.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'po-order-settings-');
process.env.DATA_DIR = SCRATCH;
const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const imp = (p) => import(pathToFileURL(path.join(WORK, p)).href);
const killer = setTimeout(() => { console.error('時間切れ (120 秒)'); process.exit(3); }, 120e3);
killer.unref();

const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB();
(await imp('lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
const { getDB } = await imp('apps/purchase-orders/db.js');
const logic = await imp('apps/purchase-orders/logic.js');
const OS = await imp('apps/purchase-orders/order-settings.js');
const ledger = await imp('apps/purchase-orders/ledger.js');
const COPY = await imp('apps/purchase-orders/scripts/copy-ne-order-lot.mjs');
const router = (await imp('apps/purchase-orders/router.js')).default;
const express = (await import('express')).default;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e && e.stack || e}`); process.exitCode = 1; } }

const db = getDB();
const T0 = new Date(Date.now() - 86400e3).toISOString();
db.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, synced_at) VALUES (1, 'run_t', 'ok', ?, ?)`).run(T0.slice(0, 10), T0);
const insRow = db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 商品区分, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価)
  VALUES ('run_t', ?, ?, ?, ?, ?, ?, 0, 10, ?, ?, ?, 500, 200)`);
// 在庫 0・30 日で 100 売れる・推奨保有 1.5 か月 = 要発注 (目標 2.5 か月 = 250 個 → ロットで丸める)
insRow.run('lot-a', 'ロット 100 の商品', '0001', '取扱中', '', 0, 100, 100, 1.5);
insRow.run('lot-zero', 'NE のロットが 0', '0001', '取扱中', '', 0, 100, 0, 1.5);
insRow.run('lot-c', 'グループがある商品', '0001', '取扱中', '', 0, 100, 6, 1.5);
insRow.run('lot-keep', '発注アプリに値がある商品', '0001', '取扱中', '', 0, 100, 12, 1.5);
insRow.run('Dup-X', '重なり (大文字)', '0001', '取扱中', '', 0, 0, 10, 1.5);
insRow.run('dup-x', '重なり (小文字)', '0001', '取扱中止', '', 0, 0, 20, 1.5);
insRow.run('set-1', 'セット', '0001', '取扱中', 'セット', 0, 0, 5, 1.5);
insRow.run('old-item', '取扱中止で NE が空', '0001', '取扱中止', '', 0, 0, null, 1.5);
// R1 High 3: 重なりは「正の整数 / 空・0 / おかしい」に分けてから比べる (12 と 0・12 と -1 も重なり / おかしい)
insRow.run('dz', '重なり: 取扱中は 0', '0001', '取扱中', '', 0, 100, 0, 1.5);
insRow.run('DZ', '重なり: 取扱中止は 12', '0001', '取扱中止', '', 0, 0, 12, 1.5);
insRow.run('neg', '重なり: 12', '0001', '取扱中', '', 0, 100, 12, 1.5);
insRow.run('NEG', '重なり: -1', '0001', '取扱中止', '', 0, 0, -1, 1.5);
insRow.run('frac', '小数のロット (今は使えている)', '0001', '取扱中', '', 0, 100, 1.5, 1.5);
insRow.run('frac-off', '小数のロット (取扱中止)', '0001', '取扱中止', '', 0, 0, 1.5, 1.5);
insRow.run('twin', '重なり: 同じ 8', '0001', '取扱中', '', 0, 100, 8, 1.5);
insRow.run('TWIN', '重なり: 同じ 8', '0001', '取扱中止', '', 0, 0, 8, 1.5);
const now = () => new Date().toISOString();
db.prepare(`INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at) VALUES ('c1','1','条件 1','金額',30000,'円',?,?)`).run(T0, T0);
db.prepare(`INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at) VALUES ('c2','2','ほかの仕入先','金額',10000,'円',?,?)`).run(T0, T0);
db.prepare(`INSERT INTO po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) VALUES ('m1','原料 1',100,'kg',?,?)`).run(T0, T0);
db.prepare(`INSERT INTO po_product_attrs (product_key, product_code, condition_id, created_at, updated_at) VALUES ('lot-c','lot-c','c1',?,?)`).run(T0, T0);
db.prepare(`INSERT INTO po_product_attrs (product_key, product_code, order_lot, created_at, updated_at) VALUES ('lot-keep','lot-keep',24,?,?)`).run(T0, T0);

const attrs = (key) => db.prepare('SELECT * FROM po_product_attrs WHERE product_key=?').get(key) || null;
const audits = (resource) => db.prepare('SELECT * FROM po_audit_log WHERE resource=? ORDER BY id').all(resource).map((r) => ({ ...r, detail: JSON.parse(r.detail_json) }));
const counts = () => Object.fromEntries(['po_product_attrs', 'po_order_conditions', 'po_material_groups', 'po_audit_log'].map((t) => [t, db.prepare(`SELECT COUNT(*) AS n FROM ${t}`).get().n]));
const status = (fn) => { try { fn(); return 'ok'; } catch (e) { return `${e.status} ${e.reason}`; } };

// 発注アプリの router (ログインした人 = naka@test)
const app = express();
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', allowedApps: '*' }; next(); });
app.use('/apps/purchase-orders', express.json({ limit: '1mb' }), router);
const server = http.createServer(app);
server.keepAliveTimeout = 120e3;   // 写しの script (spawnSync) の間に keep-alive の接続を閉じない (閉じた接続を fetch が使うと fetch failed)
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/purchase-orders`;
const j = async (p, opt = {}) => {
  const r = await fetch(base + p, { ...opt, headers: { 'Content-Type': 'application/json', ...(opt.headers || {}) }, body: opt.body === undefined ? undefined : JSON.stringify(opt.body) });
  return { status: r.status, body: await r.json().catch(() => null) };
};

console.log('発注の設定の部品 (order-settings.js)');
await ta('[S1] 印 (seen): 無い = 428・行が無いのに印あり / 行があるのに null = 409・overwrite は無い (validate-only は dry-run だけ)・列の値の印は 1 件以上で変える列を全部', async () => {
  const w = (seen, via = 'po-admin', patch = { order_lot: 5 }) => status(() => OS.writeOrderSettings({ code: 'new-1', patch, seen, actor: 'naka@test', via }));
  const c0 = counts();
  assert.equal(w(undefined), '428 seen_required');
  assert.equal(w('overwrite'), '428 seen_required', 'overwrite は新商品の登録だけ');
  assert.equal(w('overwrite', 'master-edit:sku'), '428 seen_required');
  assert.equal(w({ updated_at: '2020-01-01T00:00:00.000Z' }), '409 stale');
  assert.deepEqual(counts(), c0, '断ったのに書いた');
  assert.equal(w({ updated_at: null }), 'ok');
  assert.equal(w({ updated_at: null }, 'po-admin', { order_lot: 6 }), '409 stale', '行が出来た後に「まだ無い」の印');
  const u = attrs('new-1').updated_at;
  assert.equal(w({ updated_at: u }, 'po-admin', { order_lot: 6 }), 'ok');
  assert.notEqual(attrs('new-1').updated_at, u, '印が進まない');
  // 同じミリ秒に続けて書いても印は進む (先に読んだ値の確かめを見逃さない)
  const u2 = attrs('new-1').updated_at;
  assert.equal(w({ updated_at: u2 }, 'po-admin', { order_lot: 7 }), 'ok');
  assert.ok(attrs('new-1').updated_at > u2);
  // 列の値の印 (発注画面のグループの紐付け)
  assert.equal(w({ fields: { condition_id: 'c1' } }, 'po-bind', { condition_id: 'c1' }), '409 stale', '画面は c1 と思っていたが実は空');
  assert.equal(w({ fields: { condition_id: null } }, 'po-bind', { condition_id: 'c1' }), 'ok');
  assert.equal(w({ fields: { bogus: 1 } }, 'po-bind', { condition_id: 'c1' }), '400 invalid_input');
  // R1 Medium 1: 空の fields・変える列が fields に無い = 断る (別の画面の変更を上書きしない)
  const c1 = counts();
  assert.equal(w({ fields: {} }, 'po-bind', { condition_id: null }), '400 invalid_input');
  assert.equal(w({ fields: { material_group_id: null } }, 'po-bind', { condition_id: null }), '400 invalid_input');
  assert.equal(w({ fields: { condition_id: 'c1' } }, 'po-bind', { condition_id: null, material_group_id: 'm1' }), '400 invalid_input');
  assert.deepEqual(counts(), c1);
  // R1 High 1: 'overwrite' (印を見ない) はもう無い = どの画面も 428。validate-only は確かめ (dry-run) だけ
  assert.equal(w('overwrite', 'master-edit:new', { order_lot: 9 }), '428 seen_required');
  assert.equal(w('validate-only', 'master-edit:new', { order_lot: 9 }), '428 seen_required');
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-1', patch: { order_lot: 9 }, seen: 'validate-only', actor: 'x', via: 'master-edit:new' }, { dryRun: true })), 'ok');
  assert.deepEqual(counts(), c1);
  assert.deepEqual([attrs('new-1').order_lot, attrs('new-1').condition_id], [7, 'c1']);
});

await ta('[W1] 書き込み: 新しいグループ + 商品の行 + 記録を 1 つの取引で・dry-run は何も残さない・同じ中身のグループはそのまま使う・違う中身 = 409・仕入先・変わらない = 書かない', async () => {
  const input = (over = {}) => ({ code: 'New-2', patch: { order_lot: '１２', capacity_per_unit: '100', case_lot: '' }, newCondition: { condition_id: 'c-new', display_name: '新しい条件', condition_type: '数量', unit: 'ケース', condition_value: '3' },
    newMaterial: { group_id: 'm-new', name: '新しい原料', min_order_qty: '', unit: '' }, seen: { updated_at: null }, supplierCode: '0001', actor: 'naka@test', via: 'master-edit:new', requestId: 'req-1', ...over });
  const c0 = counts();
  const dry = OS.writeOrderSettings(input(), { dryRun: true });
  assert.deepEqual([dry.dryRun, dry.changed, dry.created], [true, true, { condition: true, material: true }]);
  assert.deepEqual(counts(), c0, 'dry-run が書いた');
  const r = OS.writeOrderSettings(input());
  assert.deepEqual([r.changed, r.created, r.row.product_code, r.row.order_lot, r.row.capacity_per_unit, r.row.condition_id, r.row.material_group_id, r.row.created_via],
    [true, { condition: true, material: true }, 'New-2', 12, 100, 'c-new', 'm-new', 'master-edit:new']);
  assert.deepEqual(db.prepare("SELECT supplier_code, condition_type, unit, condition_value FROM po_order_conditions WHERE condition_id='c-new'").get(), { supplier_code: '1', condition_type: '数量', unit: 'ケース', condition_value: 3 });
  const a = audits('attrs:new-2');
  assert.deepEqual([a.length, a[0].actor, a[0].actor_type, a[0].action, a[0].request_id, a[0].detail.via, a[0].detail.before, a[0].detail.after.order_lot], [1, 'naka@test', 'user', 'po_attrs_write', 'req-1', 'master-edit:new', null, 12]);
  assert.equal(audits('condition:c-new')[0].detail.via, 'master-edit:new');
  // やり直し (同じ中身) = グループは作らない・行も変わらない = 記録も増えない
  const again = OS.writeOrderSettings(input({ seen: { updated_at: attrs('new-2').updated_at } }));
  assert.deepEqual([again.changed, again.created], [false, { condition: false, material: false }]);
  assert.equal(audits('attrs:new-2').length, 1);
  // 同じ ID で中身が違う = 409 (取引ごと巻き戻る)
  const c1 = counts();
  assert.equal(status(() => OS.writeOrderSettings(input({ code: 'new-3', newCondition: { condition_id: 'c-new', display_name: '違う', condition_type: '金額', unit: '円', condition_value: '1' } }))), '409 group_id_taken');
  assert.equal(status(() => OS.writeOrderSettings(input({ code: 'new-3', newCondition: null, newMaterial: { group_id: 'm-new', name: '違う' } }))), '409 group_id_taken');
  assert.deepEqual(counts(), c1);
  // 代表の仕入先でない発注条件 = 400 (変えるときだけ・仕入先が無い = 400)。マスタ管理 (supplierCode なし) は見ない
  assert.equal(status(() => OS.writeOrderSettings(input({ code: 'new-3', newCondition: null, newMaterial: null, patch: { condition_id: 'c2' } }))), '400 invalid_input');
  assert.equal(status(() => OS.writeOrderSettings(input({ code: 'new-3', newCondition: null, newMaterial: null, patch: { condition_id: 'c1' }, supplierCode: null }))), '400 invalid_input');
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: { condition_id: 'c2' }, seen: { updated_at: null }, actor: 'x', via: 'po-admin' })), 'ok');
  // その行のほかの列だけ直す (仕入先の違う c2 はそのまま = 止めない)
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: { order_lot: 4, condition_id: 'c2' }, seen: { updated_at: attrs('new-3').updated_at }, supplierCode: '1', actor: 'x', via: 'master-edit:sku' })), 'ok');
  // 形: 0・小数・負・文字のロット / 知らない列 / 知らない画面
  for (const v of ['0', '1.5', '-1', 'abc', 2000000]) assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: { order_lot: v }, seen: 'overwrite', actor: 'x', via: 'master-edit:new' })), '400 invalid_input', String(v));
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: { bogus: 1 }, seen: 'overwrite', actor: 'x', via: 'master-edit:new' })), '400 invalid_input');
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: {}, seen: 'overwrite', actor: 'x', via: 'somewhere' })), '400 invalid_input');
  assert.equal(status(() => OS.writeOrderSettings({ code: 'new-3', patch: { condition_id: 'a — b' }, seen: 'overwrite', actor: 'x', via: 'master-edit:new' })), '400 invalid_input');
});

await ta('[R1] 新商品の登録の ②: 行がまだ無いことを確かめる・request_id + 商品で 1 回だけ記録・やり直しは前の結果 (その後に直した値を戻さない)・ほかの画面が先に作った = 409', async () => {
  const reg = (over = {}) => OS.writeRegistrationOrderSettings({ code: 'Reg-1', patch: { order_lot: '5', condition_id: 'c1' }, supplierCode: '0001', actor: 'naka@test', requestId: 'rq-1', ...over });
  assert.equal(OS.registrationOrderDone('rq-1'), false);
  const r1 = reg();
  assert.deepEqual([r1.ok, r1.changed, r1.replayed, attrs('reg-1').order_lot, attrs('reg-1').created_via], [true, true, false, 5, 'master-edit:new']);
  assert.equal(OS.registrationOrderDone('rq-1'), true);
  assert.equal(audits('attrs:reg-1').at(-1).request_id, 'rq-1');
  // その後に発注アプリのマスタ管理が直す → 同じ request_id のやり直し = 前の結果を返すだけ (8 を 5 に戻さない)
  OS.writeOrderSettings({ code: 'reg-1', patch: { order_lot: 8 }, seen: { updated_at: attrs('reg-1').updated_at }, actor: 'other@test', via: 'po-admin' });
  const n0 = audits('attrs:reg-1').length;
  const again = reg();
  assert.deepEqual([again.ok, again.replayed, again.changed, attrs('reg-1').order_lot, audits('attrs:reg-1').length], [true, true, true, 8, n0]);
  // 同じ request_id で違う中身・違う商品 = 409
  assert.equal(status(() => reg({ patch: { order_lot: '6' } })), '409 request_id_reused');
  assert.equal(status(() => reg({ code: 'reg-x' })), '409 request_id_reused');
  // ほかの画面がもう行を作っていた (② の前・② が落ちた後) = 409 stale・上書きしない・記録しない (やり直しても 409)
  OS.writeOrderSettings({ code: 'reg-2', patch: { order_lot: 3 }, seen: { updated_at: null }, actor: 'other@test', via: 'po-admin' });
  assert.equal(status(() => reg({ code: 'reg-2', requestId: 'rq-2' })), '409 stale');
  assert.equal(status(() => reg({ code: 'reg-2', requestId: 'rq-2' })), '409 stale');
  assert.deepEqual([attrs('reg-2').order_lot, OS.registrationOrderDone('rq-2')], [3, false]);
  // request_id が無い = 400
  assert.equal(status(() => reg({ code: 'reg-3', requestId: '' })), '400 invalid_input');
});

console.log('\n入口が 2 つ (発注アプリのマスタ管理 / マスタの入力)');
await ta('[E1] 発注アプリのマスタ管理の API も同じ部品: 印が要る (無い = 428)・マスタの入力が先に直した = 409・発注画面の紐付けも・記録の via が分かれる', async () => {
  let r = await j('/api/masters/attrs', { method: 'POST', body: { product_code: 'lot-a', order_lot: '100', condition_id: '', material_group_id: '' } });
  assert.deepEqual([r.status, r.body.reason], [428, 'seen_required']);
  r = await j('/api/masters/attrs', { method: 'POST', body: { product_code: 'lot-a', order_lot: '100', condition_id: 'c1', seen: { updated_at: null } } });
  assert.equal(r.status, 200, JSON.stringify(r.body));
  const opened = r.body.row.updated_at;   // マスタ管理の画面を開いた (この印)
  // マスタの入力 (商品の画面) が先に直す
  OS.writeOrderSettings({ code: 'lot-a', patch: { order_lot: 50 }, seen: { updated_at: opened }, supplierCode: '0001', actor: 'other@test', via: 'master-edit:sku' });
  r = await j('/api/masters/attrs', { method: 'POST', body: { product_code: 'lot-a', order_lot: '100', condition_id: 'c1', seen: { updated_at: opened } } });
  assert.deepEqual([r.status, r.body.reason, r.body.current.order_lot], [409, 'stale', 50]);
  assert.equal(attrs('lot-a').order_lot, 50, 'マスタの入力の値を上書きした');
  // 発注画面の紐付け: 画面が見ていた紐付けと違う = 409
  r = await j('/api/attrs/bind', { method: 'POST', body: { product_code: 'lot-a', condition_id: '', seen: { fields: { condition_id: null } } } });
  assert.deepEqual([r.status, r.body.reason], [409, 'stale']);
  r = await j('/api/attrs/bind', { method: 'POST', body: { product_code: 'lot-a', condition_id: '' } });
  assert.equal(r.status, 428);
  r = await j('/api/attrs/bind', { method: 'POST', body: { product_code: 'lot-a', condition_id: '', seen: { fields: { condition_id: 'c1' } } } });
  assert.equal(r.status, 200);
  assert.deepEqual([attrs('lot-a').condition_id, attrs('lot-a').order_lot], [null, 50], '紐付けを外しても発注ロットはそのまま');
  assert.deepEqual(audits('attrs:lot-a').map((x) => [x.actor, x.detail.via]), [['naka@test', 'po-admin'], ['other@test', 'master-edit:sku'], ['naka@test', 'po-bind']]);
  // 前の画面 (発注ロットの列を知らない) が送っても発注ロットは消さない
  r = await j('/api/masters/attrs', { method: 'POST', body: { product_code: 'lot-a', condition_id: 'c1', seen: { updated_at: attrs('lot-a').updated_at } } });
  assert.equal(r.status, 200);
  assert.deepEqual([attrs('lot-a').order_lot, attrs('lot-a').condition_id], [50, 'c1']);
});

await ta('[A1] マスタ管理の一覧: 発注ロット・印・未紐付け (発注ロットだけ)・NE 登録待ち・紐付けの削除は発注ロットを残す・グループも印・使われているグループは消さない', async () => {
  OS.writeRegistrationOrderSettings({ code: 'Brand-New-1', patch: { order_lot: 3 }, supplierCode: '0001', actor: 'naka@test', requestId: 'a1-reg' });
  OS.writeOrderSettings({ code: 'gone-code', patch: { order_lot: 3, case_group: 'G' }, seen: { updated_at: null }, actor: 'naka@test', via: 'po-admin' });
  let r = await j('/api/masters/attrs');
  const by = Object.fromEntries(r.body.rows.map((x) => [x.product_key, x]));
  assert.deepEqual([by['lot-keep'].order_lot, by['lot-keep'].linked, typeof by['lot-keep'].updated_at], [24, false, 'string'], '発注ロットだけの行は未紐付け');
  assert.deepEqual([by['lot-zero'].linked, by['lot-zero'].updated_at], [false, null]);
  assert.deepEqual([by['brand-new-1'].pmlMissing, by['brand-new-1'].neWaiting], [true, true], 'マスタの入力で作った・商品管理リストに無い = NE 登録待ち');
  assert.deepEqual([by['gone-code'].pmlMissing, by['gone-code'].neWaiting], [true, false], 'コード改廃 = PML外のまま');
  assert.ok(!by['old-item'], '取扱中止で紐付けの無い商品は出さない');
  const admin = await (await fetch(base + '/admin')).text();
  for (const w of ["{ k: 'order_lot', l: '発注ロット", 'var SEEN_TABS = { attrs: 1, conditions: 1, materials: 1 };', "b2.seen = seenOfRow(tr);", "'?seen=' + encodeURIComponent", 'NE 登録待ち', "if (j.reason === 'stale')", "j.lotSource === 'app'"]) {
    assert.ok(admin.includes(w), `マスタ管理の画面に ${w} が無い`);
  }
  // 紐付けの削除: 印が要る・発注ロットは残す (発注ロットも無ければ行ごと)
  r = await j('/api/masters/attrs/lot-a', { method: 'DELETE' });
  assert.deepEqual([r.status, r.body.reason], [409, 'stale']);
  r = await j('/api/masters/attrs/lot-a?seen=' + encodeURIComponent(attrs('lot-a').updated_at), { method: 'DELETE' });
  assert.deepEqual([r.status, r.body.kept, attrs('lot-a').order_lot, attrs('lot-a').condition_id], [200, true, 50, null]);
  r = await j('/api/masters/attrs/lot-c?seen=' + encodeURIComponent(attrs('lot-c').updated_at), { method: 'DELETE' });
  assert.deepEqual([r.status, r.body.deleted, attrs('lot-c')], [200, 1, null]);
  // グループ: 印・使われているグループは 400
  r = await j('/api/masters/conditions', { method: 'POST', body: { condition_id: 'c1', supplier_code: '1', display_name: '条件 1 改', condition_type: '金額', condition_value: 40000, unit: '円', seen: { updated_at: '2020-01-01T00:00:00.000Z' } } });
  assert.deepEqual([r.status, r.body.reason], [409, 'stale']);
  const cu = db.prepare("SELECT updated_at FROM po_order_conditions WHERE condition_id='c1'").get().updated_at;
  r = await j('/api/masters/conditions', { method: 'POST', body: { condition_id: 'c1', supplier_code: '1', display_name: '条件 1 改', condition_type: '金額', condition_value: 40000, unit: '円', seen: { updated_at: cu } } });
  assert.equal(r.status, 200);
  assert.equal(audits('condition:c1').at(-1).detail.after.display_name, '条件 1 改');
  r = await j('/api/masters/conditions', { method: 'POST', body: { condition_id: 'c1', supplier_code: '1', display_name: 'x', condition_type: '金額', condition_value: 1, unit: '円', seen: { updated_at: null } } });
  assert.deepEqual([r.status, r.body.reason], [409, 'stale'], '追加 (まだ無いこと) なのにもうある');
  const used = db.prepare("SELECT updated_at FROM po_order_conditions WHERE condition_id='c-new'").get().updated_at;
  r = await j('/api/masters/conditions/c-new?seen=' + encodeURIComponent(used), { method: 'DELETE' });
  assert.deepEqual([r.status, r.body.reason], [400, 'in_use']);
});

console.log('\n発注ロットの出どころ (logic.js)');
await ta('[L1] 写す前 (ne) = NE の値・写した後 (app) = 発注アプリの order_lot だけ (無い = すすめる数が出ない・NE の値は使わない)', async () => {
  assert.equal(logic.withOrderLot({ '発注ロット単位': 100 }, { order_lot: 7 }, 'ne')['発注ロット単位'], 100);
  assert.equal(logic.withOrderLot({ '発注ロット単位': 100 }, { order_lot: 7 }, 'app')['発注ロット単位'], 7);
  assert.equal(logic.withOrderLot({ '発注ロット単位': 100 }, null, 'app')['発注ロット単位'], null);
  let r = logic.computeAll();
  const p = (code) => r.products.find((x) => x.code === code);
  assert.equal(r.lotSource, 'ne');
  // lot-a: 目標 2.5 か月 × 100 = 250 個 / NE のロット 100 → 2.5 ロット → 3 ロット = 300
  assert.deepEqual([p('lot-a').lot, p('lot-a').recQty, p('lot-keep').lot], [100, 300, 12]);
  ledger.setSetting('order_lot_source', 'app', { actorType: 'migration', actor: 'test' });
  r = logic.computeAll();
  assert.equal(r.lotSource, 'app');
  // 発注アプリ: lot-a = 50 (マスタの入力で直した) → 250/50 = 5 ロット = 250 / lot-keep = 24 / lot-zero = 無い = すすめる数なし
  assert.deepEqual([p('lot-a').lot, p('lot-a').recQty, p('lot-keep').lot, p('lot-keep').recQty], [50, 250, 24, 240]);
  assert.deepEqual([p('lot-zero').lot, p('lot-zero').recQty, p('lot-zero').isTarget], [0, null, true], '発注アプリに値が無い = すすめる数が出ない (要発注には出る)');
  assert.throws(() => ledger.setSetting('order_lot_source', 'x', { actorType: 'migration', actor: 'test' }), /ne\/app/);
  ledger.setSetting('order_lot_source', 'ne', { actorType: 'migration', actor: 'test' });
});

console.log('\nNE のロットを写す script (copy-ne-order-lot.mjs)');
const SCRIPT = path.join(WORK, 'apps/purchase-orders/scripts/copy-ne-order-lot.mjs');
const cli = (args, env = {}) => {
  const r = spawnSync(process.execPath, [SCRIPT, ...args], { env: { ...process.env, ...env }, encoding: 'utf8', timeout: 60e3 });
  // --json の答え = 「{」だけの行から後 (前に DB の初期化のログが出る)
  const lines = String(r.stdout || '').split(/\r?\n/);
  const at = lines.findIndex((l) => l === '{');
  return { code: r.status, out: r.stdout, err: r.stderr, json: (() => { try { return JSON.parse(lines.slice(at).join('\n')); } catch { return null; } })() };
};
await ta('[C1] 分け方 (planCopy): 行を作る・埋める・同じ・発注アプリを残す・NE が空・重なり (12 と 0 も)・おかしい (負・小数)・止まる商品・セットは見ない', async () => {
  const { normProductCode } = await imp('apps/purchase-orders/db.js');
  const plan = COPY.planCopy(db, { normProductCode, pmlRows: logic.loadPml().rows });
  const keys = (k) => plan[k].map((x) => x.key).sort();
  // lot-a = 発注アプリ 50 / NE 100 → 残す・lot-keep = 24 / 12 → 残す・lot-c = 行なし (消した) → 作る・lot-zero = 行あり発注ロットなし? (無い) → NE 0 = 空
  assert.deepEqual(keys('conflict_keep_app'), ['lot-a', 'lot-keep']);
  assert.deepEqual(keys('insert'), ['lot-c', 'twin']);
  assert.deepEqual(keys('ne_empty'), ['lot-zero', 'old-item']);
  // R1 High 3: 12 と 0 = 重なり・12 と -1 / 小数 = おかしい (正の値だけで比べない)
  assert.deepEqual(keys('dup_conflict'), ['dup-x', 'dz']);
  assert.deepEqual(plan.dup_conflict.find((x) => x.key === 'dup-x').ne_values.sort(), ['10', '20']);
  assert.deepEqual(plan.dup_conflict.find((x) => x.key === 'dz').ne_values.sort(), ['12', '空']);
  assert.deepEqual(keys('ne_bad'), ['frac', 'frac-off', 'neg']);
  // R1 High 2: 取扱中・発注アプリが空・今は NE の値ですすめる数が出ている = 止まる商品 (取扱中止の frac-off は止めない)
  assert.deepEqual(keys('blockers'), ['dup-x', 'dz', 'frac', 'neg']);
  assert.ok(!Object.values(plan).flat().some((x) => x && x.key === 'set-1'), 'セットを写そうとした');
  assert.equal(plan.neEmptyActive, 1);
  // 発注アプリに行がある (グループ) が発注ロットが空 = 埋める
  OS.writeOrderSettings({ code: 'lot-c', patch: { condition_id: 'c1' }, seen: { updated_at: null }, actor: 'naka@test', via: 'po-admin' });
  const plan2 = COPY.planCopy(db, { normProductCode, pmlRows: logic.loadPml().rows });
  assert.deepEqual([plan2.fill.map((x) => x.key), plan2.insert.map((x) => x.key)], [['lot-c'], ['twin']]);
});

await ta('[C2] CLI: DATA_DIR が無い = 止める・dry-run は何も書かない・止まる商品があれば --apply は何も書かない・解いたら写して app・もう app = --apply は何もしない・--reapply は件数の確かめつき・--diff・--back-to-ne', async () => {
  const noDir = cli([], { DATA_DIR: '' });
  assert.equal(noDir.code, 2, noDir.err);
  assert.match(noDir.err, /DATA_DIR が設定されていません/);
  const c0 = counts();
  const dry = cli(['--json']);
  assert.equal(dry.code, 0, dry.err);
  assert.deepEqual([dry.json.mode, dry.json.fill, dry.json.insert, dry.json.conflict_keep_app, dry.json.dup_conflict, dry.json.ne_empty, dry.json.ne_bad, dry.json.blockers], ['dry-run', 1, 1, 2, 2, 2, 3, 4]);
  assert.deepEqual(counts(), c0, 'dry-run が書いた');
  assert.equal(ledger.getSetting('order_lot_source') === 'app', false);
  const human = cli([]);
  assert.match(human.out, /dry-run \(何も書いていません\)/); assert.match(human.out, /lot-a {2}発注アプリ=50 {2}NE=100/);
  assert.match(human.out, /止まる商品[^\n]*4 件/);
  // R1 High 2: 止まる商品がある = 書く前に全体を止める (何も書かない・app にしない)
  const blocked = cli(['--apply', '--json']);
  assert.equal(blocked.code, 3, blocked.out + blocked.err);
  assert.deepEqual([blocked.json.ok, blocked.json.blocked.map((x) => x.key).sort()], [false, ['dup-x', 'dz', 'frac', 'neg']]);
  assert.deepEqual(counts(), c0, '止めたのに書いた');
  assert.equal(ledger.getSetting('order_lot_source') === 'app', false);
  // 人が発注アプリに値を入れた商品 = 解いた (通す)
  for (const [code, lot] of [['Dup-X', 10], ['dz', 5], ['frac', 2], ['neg', 12]]) OS.writeOrderSettings({ code, patch: { order_lot: lot }, seen: { updated_at: null }, actor: 'naka@test', via: 'po-admin' });
  const c1 = counts();
  const ap = cli(['--apply', '--json']);
  assert.equal(ap.code, 0, ap.err);
  assert.deepEqual([ap.json.ok, ap.json.written, ap.json.source_after], [true, 2, 'app']);
  assert.equal(attrs('twin').order_lot, 8);
  // R1 Medium 2: もう app = --apply は何も変えずに終わる (写した後に増えた商品を NE から埋めない)
  insRow.run('late-new', '写した後に増えた商品', '0001', '取扱中', '', 0, 100, 4, 1.5);
  const c2 = counts();
  const twice = cli(['--apply', '--json']);
  assert.deepEqual([twice.code, twice.json.ok, twice.json.skipped], [0, true, 'already_app'], twice.out + twice.err);
  assert.deepEqual([counts(), attrs('late-new')], [c2, null]);
  // 写し直し = --reapply (確かめの数が要る): 数なし = 書かずに数を出す / 違う数 = 止める / 同じ数 = 書く (app のまま)
  const pre = cli(['--reapply', '--json']);
  assert.deepEqual([pre.code, pre.json.preview, pre.json.to_write], [0, true, 1], pre.out + pre.err);
  assert.deepEqual(counts(), c2);
  const wrong = cli(['--reapply', '--confirm=2', '--json']);
  assert.equal(wrong.code, 2, wrong.out + wrong.err);
  assert.deepEqual(counts(), c2);
  const re = cli(['--reapply', '--confirm=1', '--json']);
  assert.deepEqual([re.code, re.json.ok, re.json.written, attrs('late-new').order_lot, ledger.getSetting('order_lot_source')], [0, true, 1, 4, 'app'], re.out + re.err);
  assert.ok(c1);
  assert.equal(ledger.getSetting('order_lot_source'), 'app');
  assert.deepEqual([attrs('lot-c').order_lot, attrs('lot-a').order_lot, attrs('lot-keep').order_lot], [6, 50, 24], '発注アプリの値を NE で上書きした');
  const au = audits('attrs:lot-c').at(-1);
  assert.deepEqual([au.actor_type, au.actor, au.action, au.detail.after.order_lot], ['migration', 'copy-ne-order-lot', 'po_order_lot_copy', 6]);
  assert.deepEqual(audits('setting:order_lot_source').filter((x) => x.action === 'po_order_lot_copy_done').map((x) => [x.actor, x.detail.fill]), [['copy-ne-order-lot', 1]]);
  const again = cli(['--json']);
  assert.deepEqual([again.json.fill, again.json.insert, again.json.same, again.json.blockers], [0, 0, 3, 0]);
  const diff = cli(['--diff', '--json']);
  assert.deepEqual([diff.json.source, diff.json.missing, diff.json.diff], ['app', 1, 3]);   // 空 = lot-zero / 違う = lot-a・lot-keep・frac
  const back = cli(['--back-to-ne', '--json']);
  assert.deepEqual([back.json.before, back.json.after], ['app', 'ne']);
  assert.equal(ledger.getSetting('order_lot_source'), 'ne');
  assert.equal(attrs('lot-c').order_lot, 6, '戻しても写した値は消さない');
  ledger.setSetting('order_lot_source', 'app', { actorType: 'migration', actor: 'test' });
});

await ta('[U1] 未紐付けタブの知らせ (写した後): 発注ロットが空の取扱中・NE と違う', async () => {
  const r = await j('/api/attrs/unlinked?days=0');
  assert.equal(r.body.lotSource, 'app');
  assert.deepEqual(r.body.lotMissing.map((x) => x.code).sort(), ['lot-zero']);
  assert.deepEqual(r.body.lotDiff.map((x) => [x.code, x.app, x.ne]).sort(), [['frac', 2, 1.5], ['lot-a', 50, 100], ['lot-keep', 24, 12]]);
  assert.ok(r.body.rows.some((x) => x.code === 'lot-keep'), '発注ロットだけの行は未紐付けのまま');
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
