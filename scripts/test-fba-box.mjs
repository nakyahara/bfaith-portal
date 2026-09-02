/**
 * FBA箱詰め記録 (apps/fba-box) — DB層 + 突合ロジックのテスト
 * 実行: node scripts/test-fba-box.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.RENDER = '';   // ローカルテスト (DATA_DIR ガードを踏まない)

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-box-test-'));
const dbFile = path.join(tmp, 'test.db');

const db = await import('../apps/fba-box/db.js');
const svc = await import('../apps/fba-box/service.js');
db._openForTest(dbFile);

let passed = 0, failed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ✅ ${name}`); }
  catch (e) { failed++; console.error(`  ❌ ${name}\n     ${e.message}`); }
}

// ───────── 突合 (service) ─────────
console.log('■ 突合ロジック');

const planSheets = [
  { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
    { no: 1, fnsku: 'X0001AAA01', productName: 'ロジン松やに', qty: '45' },
    { no: 2, fnsku: 'X0001BBB02', productName: 'アロマオイル', qty: '16' },
    { no: 3, fnsku: 'X0001EEE05', productName: 'Excelに無い商品', qty: '9' },
  ] },
  { slotId: 'p2_normal', sheet: 'P2_通常', label: '通常プラン2', rows: [
    { no: 1, fnsku: 'X0001AAA01', productName: 'ロジン松やに', qty: '10' },
  ] },
];
const sheetInfo = {
  sheetName: '輸送箱の梱包情報', packingGroupId: 'pg-test-1', packingGroupLabel: '梱包グループ：1',
  totalBoxes: { row: 3, col: 13, value: 5 }, maxBoxColumns: 15, headerRow: 5,
  headers: { SKU: 1, FNSKU: 5 }, boxColumns: { 1: 13 }, boxNameRow: 9, dimRows: { weight: 10 },
  skuRows: [
    { row: 6, sku: 'sku-a', asin: 'B000000001', fnsku: 'X0001AAA01', excelId: 'pk1', productName: 'ロジン', plannedQty: 45 },
    { row: 7, sku: 'sku-b', asin: 'B000000002', fnsku: 'X0001BBB02', excelId: 'pk2', productName: 'アロマ', plannedQty: 20 },
    { row: 8, sku: 'sku-c', asin: 'B000000003', fnsku: 'X0001CCC03', excelId: 'pk3', productName: 'プラン外', plannedQty: 5 },
  ],
};

t('FNSKU一致+数量一致 = matched (複数候補は数量で一意化)', () => {
  const idx = svc.buildPickingIndex(planSheets);
  const m = svc.matchSheet(sheetInfo, idx);
  assert.equal(m.ok, true);
  const a = m.rows.find(r => r.fnsku === 'X0001AAA01');
  assert.equal(a.matchState, 'matched');
  assert.equal(a.planNo, '通常_1');
});
t('数量不一致 = qty_mismatch (作成は通す・警告)', () => {
  const m = svc.matchSheet(sheetInfo, svc.buildPickingIndex(planSheets));
  const b = m.rows.find(r => r.fnsku === 'X0001BBB02');
  assert.equal(b.matchState, 'qty_mismatch');
  assert.equal(b.planNo, '通常_2');
  assert.ok(m.issues.some(i => i.kind === 'qty_mismatch'));
});
t('picking側に無い = excel_only', () => {
  const m = svc.matchSheet(sheetInfo, svc.buildPickingIndex(planSheets));
  const c = m.rows.find(r => r.fnsku === 'X0001CCC03');
  assert.equal(c.matchState, 'excel_only');
  assert.equal(c.planNo, null);
});
t('Excel内の識別キー重複 = ブロック (ok:false)', () => {
  const dupSheet = { ...sheetInfo, skuRows: [...sheetInfo.skuRows, { ...sheetInfo.skuRows[0], row: 9 }] };
  const m = svc.matchSheet(dupSheet, svc.buildPickingIndex(planSheets));
  assert.equal(m.ok, false);
  assert.ok(m.blocking.some(i => i.kind === 'duplicate_identity'));
});
t('同一FNSKUで別SKUの2行も重複ブロック (突合キーと同じ規則)', () => {
  const dupSheet = { ...sheetInfo, skuRows: [...sheetInfo.skuRows, { ...sheetInfo.skuRows[0], row: 9, sku: 'sku-a2', asin: 'B999999999' }] };
  const m = svc.matchSheet(dupSheet, svc.buildPickingIndex(planSheets));
  assert.equal(m.ok, false);
  assert.ok(m.blocking.some(i => i.kind === 'duplicate_identity' && i.identity === 'fnsku:X0001AAA01'));
});
t('同一FNSKU複数候補で数量も曖昧 = plan_noなし+ambiguous警告', () => {
  const amb = { ...sheetInfo, skuRows: [{ row: 6, sku: 'sku-a', asin: 'B1', fnsku: 'X0001AAA01', plannedQty: 7, productName: 'x' }] };
  const m = svc.matchSheet(amb, svc.buildPickingIndex(planSheets));
  assert.equal(m.rows[0].planNo, null);
  assert.ok(m.issues.some(i => i.kind === 'ambiguous'));
});
t('matchWorkbook: pickingにあってExcelに無い = picking_only 警告', () => {
  const wb = { sheets: [sheetInfo] };
  const m = svc.matchWorkbook(wb, planSheets);
  assert.ok(m.issues.some(i => i.kind === 'picking_only' && i.fnsku === 'X0001EEE05'));
});
t('shortNameForSpeech: 【】<>内を除去', () => {
  assert.equal(svc.shortNameForSpeech('【水溶性】 アロマオイル10ml <チャック付き>'), 'アロマオイル10ml');
});

// ───────── run 作成〜割当 (db) ─────────
console.log('■ 納品回・割当');

function makeRun(sourceRunId = 61) {
  const wb = { sheets: [sheetInfo] };
  const m = svc.matchWorkbook(wb, planSheets);
  return db.createRun({
    sourceRunId, deliveryDate: '2026-09-05', title: '9/5 納品分',
    matchSummary: svc.summarizeMatch(m),
    excelFile: { originalName: 'pack.xlsx', storedPath: '/tmp/x.xlsx', sha256: 'a'.repeat(64), fingerprint: 'd337e046bbf029c1', metadata: {} },
    groups: m.groups, createdBy: 'test@b-faith.biz',
  });
}

const created = makeRun();
t('createRun 成功', () => assert.equal(created.ok, true));
t('同じ picking 実行の二重作成は duplicate_run', () => {
  const again = makeRun();
  assert.equal(again.ok, false);
  assert.equal(again.error, 'duplicate_run');
});

const runId = created.runId;
const state0 = db.getRunState(runId);
const groupId = state0.groups[0].id;
const rowA = state0.rows.find(r => r.fnsku === 'X0001AAA01');
const rowB = state0.rows.find(r => r.fnsku === 'X0001BBB02');
const wStaff = db.addWorker({ displayName: 'しょくいん', workerType: 'staff', actor: 't' });
const wMember = db.addWorker({ displayName: 'りようしゃ', workerType: 'member', actor: 't' });
db.setWorkerPin(wStaff.id, '1234', 't');
const staff = db.getWorker(wStaff.id);
const member = db.getWorker(wMember.id);

t('setup 中は箱を作れない (run_not_active)', () => {
  const r = db.createBox({ packGroupId: groupId, materialCode: 'box140', worker: member });
  assert.equal(r.error, 'run_not_active');
});
t('activateRun で作業開始できる', () => {
  assert.equal(db.activateRun(runId, 't').ok, true);
  assert.equal(db.getRun(runId).status, 'active');
});

const box1 = db.createBox({ packGroupId: groupId, materialCode: 'box140', worker: member });
t('createBox: 箱コードは グループ名-B連番', () => {
  assert.equal(box1.ok, true);
  assert.equal(box1.boxNo, 1);
  assert.equal(box1.boxCode, 'G1-B1');
});
t('createBox: 不正資材は拒否', () => {
  assert.equal(db.createBox({ packGroupId: groupId, materialCode: 'nope', worker: member }).error, 'bad_material');
});

const p1 = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 20, worker: member, deviceKey: 'dev:1', deviceLabel: 'iPad1', requestId: 'r1' });
t('addPlacement 成功 (box_seq=1)', () => {
  assert.equal(p1.ok, true);
  assert.equal(p1.boxSeq, 1);
  assert.equal(p1.placed, 20);
});
t('冪等性: 同じ device_key+request_id+同内容 は already で同結果', () => {
  const again = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 20, worker: member, deviceKey: 'dev:1', deviceLabel: 'iPad1', requestId: 'r1' });
  assert.equal(again.ok, true);
  assert.equal(again.already, true);
  assert.equal(db.getRunState(runId).rows.find(r => r.id === rowA.id).placed, 20);
});
t('冪等性: 同じキーで内容が違えば idempotency_conflict', () => {
  const bad = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 3, worker: member, deviceKey: 'dev:1', requestId: 'r1' });
  assert.equal(bad.ok, false);
  assert.equal(bad.error, 'idempotency_conflict');
});
t('残数超過は over_qty で拒否', () => {
  const r = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 26, worker: member, deviceKey: 'dev:1', requestId: 'r2' });
  assert.equal(r.error, 'over_qty');
});
t('数量は正の整数のみ', () => {
  assert.equal(db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1.5, worker: member, deviceKey: 'dev:1', requestId: 'r3' }).error, 'bad_qty');
  assert.equal(db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 0, worker: member, deviceKey: 'dev:1', requestId: 'r4' }).error, 'bad_qty');
});

t('期限: 同一行に別期限はブロック・同一はOK・省略は引き継ぎ', () => {
  const e1 = db.addPlacement({ runId, rowId: rowB.id, boxId: box1.boxId, qty: 5, expiry: '2028-06-24', worker: member, deviceKey: 'dev:1', requestId: 'e1' });
  assert.equal(e1.ok, true);
  const e2 = db.addPlacement({ runId, rowId: rowB.id, boxId: box1.boxId, qty: 5, expiry: '2029-01-01', worker: member, deviceKey: 'dev:1', requestId: 'e2' });
  assert.equal(e2.error, 'expiry_conflict');
  const e3 = db.addPlacement({ runId, rowId: rowB.id, boxId: box1.boxId, qty: 5, worker: member, deviceKey: 'dev:1', requestId: 'e3' });
  assert.equal(e3.ok, true);
  assert.equal(e3.expiry, '2028-06-24');
});
t('期限: 過去日・不正値は拒否', () => {
  assert.equal(db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, expiry: '2020-01-01', worker: member, deviceKey: 'dev:1', requestId: 'e4' }).error, 'past_expiry');
  assert.equal(db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, expiry: '2028-13-01', worker: member, deviceKey: 'dev:1', requestId: 'e5' }).error, 'bad_expiry');
});

t('layer は manual として記録される', () => {
  const r = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 5, layer: 'bottom', worker: member, deviceKey: 'dev:1', requestId: 'l1' });
  assert.equal(r.ok, true);
  const st = db.getRunState(runId);
  const p = st.placements.find(x => x.id === r.placementId);
  assert.equal(p.placement_layer, 'bottom');
  assert.equal(p.layer_source, 'manual');
});

// 取消
t('取消: 自端末の直近は利用者でも可・数が戻る', () => {
  const r = db.revokePlacement({ placementId: p1.placementId, worker: member, deviceKey: 'dev:1' });
  assert.equal(r.ok, true);
  assert.equal(db.getRunState(runId).rows.find(x => x.id === rowA.id).placed, 5);
});
t('取消: 他端末からは staff_required', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 3, worker: member, deviceKey: 'dev:1', requestId: 'v1' });
  const r = db.revokePlacement({ placementId: p.placementId, worker: member, deviceKey: 'dev:2' });
  assert.equal(r.error, 'staff_required');
});
t('取消: 職員は理由必須・理由付きで成功', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 2, worker: member, deviceKey: 'dev:1', requestId: 'v2' });
  assert.equal(db.revokePlacement({ placementId: p.placementId, byStaff: true, worker: staff, deviceKey: 'dev:2' }).error, 'reason_required');
  assert.equal(db.revokePlacement({ placementId: p.placementId, byStaff: true, reason: '誤入力', worker: staff, deviceKey: 'dev:2' }).ok, true);
});
t('box_seq は取消後も再利用しない', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 's1' });
  const seqs = db.getDB().prepare('SELECT box_seq FROM fbx_placements WHERE box_id = ? ORDER BY box_seq').all(box1.boxId).map(x => x.box_seq);
  assert.equal(new Set(seqs).size, seqs.length);
  assert.equal(p.boxSeq, Math.max(...seqs));
});

// 箱クローズ
t('closeBox: 実測kg必須・不正値拒否', () => {
  assert.equal(db.closeBox({ boxId: box1.boxId, measuredKg: 0, worker: staff }).error, 'bad_weight');
  assert.equal(db.closeBox({ boxId: box1.boxId, measuredKg: 'x', worker: staff }).error, 'bad_weight');
});
t('closeBox 成功 → 以後の割当は box_closed', () => {
  const r = db.closeBox({ boxId: box1.boxId, measuredKg: 12.4, closedReason: 'items_done', cushionLevel: 'little', worker: staff });
  assert.equal(r.ok, true);
  const add = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 'c1' });
  assert.equal(add.error, 'box_closed');
});
t('閉じた箱の割当取消は職員のみ', () => {
  const st = db.getRunState(runId);
  const p = st.placements.find(x => x.box_id === box1.boxId);
  assert.equal(db.revokePlacement({ placementId: p.id, worker: member, deviceKey: 'dev:1' }).error, 'staff_required');
});
t('reopenBox: 理由必須・実測がクリアされ再クローズ要', () => {
  assert.equal(db.reopenBox({ boxId: box1.boxId, worker: staff }).error, 'reason_required');
  const r = db.reopenBox({ boxId: box1.boxId, reason: '詰め直し', worker: staff });
  assert.equal(r.ok, true);
  const b = db.getRunState(runId).boxes.find(x => x.id === box1.boxId);
  assert.equal(b.status, 'open');
  assert.equal(b.measured_weight_kg, null);
  assert.equal(b.reopen_count, 1);
});
t('空箱はクローズできない', () => {
  const b2 = db.createBox({ packGroupId: groupId, materialCode: 'box160', worker: member });
  assert.equal(db.closeBox({ boxId: b2.boxId, measuredKg: 3, worker: staff }).error, 'empty_box');
});
t('箱数はテンプレ上限まで', () => {
  let last = null;
  for (let i = 0; i < 20; i++) last = db.createBox({ packGroupId: groupId, materialCode: 'box140', worker: member });
  assert.equal(last.error, 'box_limit');
});

// 行メタ
t('setRowWorkers: 片方だけ更新できる', () => {
  db.setRowWorkers({ rowId: rowA.id, labelWorker: 'たなか', worker: member });
  db.setRowWorkers({ rowId: rowA.id, checkWorker: 'さとう', worker: member });
  const r = db.getRunState(runId).rows.find(x => x.id === rowA.id);
  assert.equal(r.label_worker, 'たなか');
  assert.equal(r.check_worker, 'さとう');
});
t('setRowShortage: 残数超は拒否・正常は記録・clearで消える', () => {
  const r0 = db.getRunState(runId).rows.find(x => x.id === rowA.id);
  const remaining = r0.planned_qty - r0.placed;
  assert.equal(db.setRowShortage({ rowId: rowA.id, shortageQty: remaining + 1, reason: 'missing', worker: staff }).error, 'bad_qty');
  assert.equal(db.setRowShortage({ rowId: rowA.id, shortageQty: 2, reason: 'damaged', worker: staff }).ok, true);
  assert.equal(db.getRunState(runId).rows.find(x => x.id === rowA.id).shortage_qty, 2);
  assert.equal(db.clearRowShortage({ rowId: rowA.id, worker: staff }).ok, true);
  assert.equal(db.getRunState(runId).rows.find(x => x.id === rowA.id).shortage_qty, null);
});

t('不足確定後は placed+shortage を超える割当が over_qty', () => {
  db.setRowShortage({ rowId: rowA.id, shortageQty: 2, reason: 'missing', worker: staff });
  const r0 = db.getRunState(runId).rows.find(x => x.id === rowA.id);
  const rem = r0.planned_qty - r0.placed - r0.shortage_qty;
  const over = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: rem + 1, worker: member, deviceKey: 'dev:1', requestId: 'sh1' });
  assert.equal(over.error, 'over_qty');
  db.clearRowShortage({ rowId: rowA.id, worker: staff });
});

// run 完了ガード
t('setRunStatus done: 開いた箱があれば拒否', () => {
  const r = db.setRunStatus(runId, 'done', 't');
  assert.equal(r.error, 'open_boxes');
});
t('setRunStatus done: 全行の投入+不足=予定 でなければ rows_incomplete、揃えば完了できる', () => {
  // 別 run で完了までの正常系を検証
  const m = svc.matchWorkbook({ sheets: [sheetInfo] }, planSheets);
  const c2 = db.createRun({
    sourceRunId: 62, deliveryDate: '2026-09-06', title: '9/6 納品分',
    matchSummary: svc.summarizeMatch(m),
    excelFile: { originalName: 'p.xlsx', storedPath: '/tmp/y.xlsx', sha256: 'b'.repeat(64), fingerprint: 'd337e046bbf029c1', metadata: {} },
    groups: m.groups, createdBy: 't',
  });
  assert.equal(c2.ok, true);
  db.activateRun(c2.runId, 't');
  const st2 = db.getRunState(c2.runId);
  const g2 = st2.groups[0].id;
  const bx = db.createBox({ packGroupId: g2, materialCode: 'box140', worker: member });
  // 1行だけ入れて他は未処理 → rows_incomplete
  const rA = st2.rows[0];
  db.addPlacement({ runId: c2.runId, rowId: rA.id, boxId: bx.boxId, qty: rA.planned_qty, worker: member, deviceKey: 'dev:9', requestId: 'd1' });
  db.closeBox({ boxId: bx.boxId, measuredKg: 8, worker: staff });
  assert.equal(db.setRunStatus(c2.runId, 'done', 't').error, 'rows_incomplete');
  // 残り2行を「全量不足」で確定 → 完了できる
  for (const row of st2.rows.slice(1)) {
    db.setRowShortage({ rowId: row.id, shortageQty: row.planned_qty, reason: 'hq_order', worker: staff });
  }
  assert.equal(db.setRunStatus(c2.runId, 'done', 't').ok, true);
});
t('閉じた箱の layer 変更は職員のみ', () => {
  const st = db.getRunState(runId);
  const closedBoxIds = new Set(st.boxes.filter(b => b.status === 'closed').map(b => b.id));
  const p = st.placements.find(x => closedBoxIds.has(x.box_id));
  if (!p) { // box1 は再オープン済みのため、ここで一度閉じ直して検証
    db.closeBox({ boxId: box1.boxId, measuredKg: 9, worker: staff });
  }
  const st2 = db.getRunState(runId);
  const closed2 = new Set(st2.boxes.filter(b => b.status === 'closed').map(b => b.id));
  const p2 = st2.placements.find(x => closed2.has(x.box_id));
  assert.ok(p2, '閉じた箱に割当があるはず');
  assert.equal(db.setPlacementLayer({ placementId: p2.id, layer: 'top', worker: member }).error, 'staff_required');
  assert.equal(db.setPlacementLayer({ placementId: p2.id, layer: 'top', byStaff: true, worker: staff }).ok, true);
  db.reopenBox({ boxId: p2.box_id, reason: 'テスト後始末', worker: staff });
});

// PIN
t('職員PIN: 正しい/間違い/ロック', () => {
  assert.equal(db.verifyWorkerPin(staff.id, '1234').ok, true);
  assert.equal(db.verifyWorkerPin(staff.id, '9999').error, 'pin_invalid');
  for (let i = 0; i < 5; i++) db.verifyWorkerPin(staff.id, '0000');
  assert.equal(db.verifyWorkerPin(staff.id, '1234').error, 'pin_locked');
  db._clearPinFails();
  assert.equal(db.verifyWorkerPin(staff.id, '1234').ok, true);
});
t('利用者にPINは設定できない', () => {
  assert.equal(db.setWorkerPin(member.id, '1234', 't').error, 'not_staff');
});

// 端末・登録コード
t('登録コード: 発行→引換→再利用拒否', () => {
  const c = db.createEnrollCode('テストiPad', 't');
  const r1 = db.redeemEnrollCode(c.code);
  assert.equal(r1.ok, true);
  assert.ok(db.verifyDevice(r1.token));
  assert.equal(db.redeemEnrollCode(c.code).error, 'used');
});
t('新コード発行で旧未使用コードは無効', () => {
  const c1 = db.createEnrollCode('1台目', 't');
  db.createEnrollCode('2台目', 't');
  assert.equal(db.redeemEnrollCode(c1.code).error, 'expired');
});

// 監査イベント
t('主要操作が fbx_events に残っている', () => {
  const actions = new Set(db.listEvents(500).map(e => e.action));
  for (const a of ['run_create', 'run_activate', 'box_create', 'box_close', 'box_reopen', 'placement_add', 'placement_revoke', 'row_shortage']) {
    assert.ok(actions.has(a), `missing event: ${a}`);
  }
});

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
