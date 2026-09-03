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
process.env.DATA_DIR = tmp;   // excel.js の隔離保存先 (EXCEL_DIR/EXPORT_DIR) を一時ディレクトリに

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

// ───────── PR2: 名簿ゲート・資材・箱取消・出荷前チェック・Excel出力 (ゴールデンファイル) ─────────
console.log('■ PR2: 名簿・資材');

t('countStaffWithPin: 有効な PIN 職員の数 (無効化で減る)', () => {
  assert.equal(db.countStaffWithPin(), 1);
  db.setWorkerActive(staff.id, false);
  assert.equal(db.countStaffWithPin(), 0);
  db.setWorkerActive(staff.id, true);
  assert.equal(db.countStaffWithPin(), 1);
});
t('upsertMaterial: 検証 (コード・数値) と外寸の保存', () => {
  assert.equal(db.upsertMaterial({ code: 'bad code!', name: 'x', actor: 't' }).error, 'bad_code');
  assert.equal(db.upsertMaterial({ code: 'box140', name: '', actor: 't' }).error, 'bad_name');
  assert.equal(db.upsertMaterial({ code: 'box140', name: '140', widthCm: -1, actor: 't' }).error, 'bad_number');
  assert.equal(db.upsertMaterial({ code: 'BOX140', name: '140サイズ段ボール', tareG: 900, widthCm: 45, lengthCm: 35, heightCm: 30.5, sort: 1, actor: 't' }).ok, true);
  const m = db.listMaterials().find((x) => x.code === 'box140');
  assert.equal(m.width_cm, 45); assert.equal(m.height_cm, 30.5); assert.equal(m.tare_g, 900);
  assert.equal(db.upsertMaterial({ code: 'box999', name: '一時', active: false, actor: 't' }).ok, true);
  assert.equal(db.listMaterials().some((x) => x.code === 'box999'), false);
  assert.equal(db.listMaterials(true).some((x) => x.code === 'box999'), true);
});

console.log('■ PR2: 実物テンプレ取込 → 箱詰め → 箱取消 → チェック → Excel出力 (python)');
const xl = await import('../apps/fba-box/excel.js');
const FIX = path.resolve('scripts/fixtures/fba-box');
const fixture2 = path.join(FIX, 'packlist_v1.1_4sku_19box_expiry.xlsx');
const ing = await xl.ingestPacklist(fs.readFileSync(fixture2), 'packlist_test.xlsx');
t('実物テンプレ (4SKU/19箱列) を python で取込できる・箱名を拾う', () => {
  assert.equal(ing.ok, true, JSON.stringify(ing).slice(0, 400));
  assert.equal(ing.parsed.fingerprint, 'd337e046bbf029c1');
  assert.equal(ing.parsed.sheets[0].boxNames['3'], 'P1 - B3');
  assert.equal(ing.parsed.sheets[0].skuRows.length, 4);
});
// picking 側は Excel と同じ FNSKU/数量で作る (全行 matched)
const realPlan = [{ slotId: 'p1', sheet: 'P1_通常', label: '通常',
  rows: ing.parsed.sheets[0].skuRows.map((r, i) => ({ no: i + 1, fnsku: r.fnsku, productName: r.productName, qty: String(r.plannedQty) })) }];
const m3 = svc.matchWorkbook(ing.parsed, realPlan);
const c3 = db.createRun({
  sourceRunId: 70, deliveryDate: '2026-09-10', title: '9/10 納品分', matchSummary: svc.summarizeMatch(m3),
  excelFile: { originalName: 'packlist_test.xlsx', storedPath: ing.storedPath, sha256: ing.sha256, fingerprint: ing.parsed.fingerprint, metadata: ing.parsed.metadata },
  groups: m3.groups, createdBy: 't',
});
db.activateRun(c3.runId, 't');
const run3 = c3.runId;
const s3 = db.getRunState(run3);
const g3 = s3.groups[0].id;
const rows3 = [...s3.rows].sort((a, b) => a.excel_row - b.excel_row);   // 予定 5, 5, 1, 30
const mkBox = (mat) => db.createBox({ packGroupId: g3, materialCode: mat, worker: member });
const b1 = mkBox('box140'), b2 = mkBox('box160'), b3 = mkBox('box140'), b4 = mkBox('box140');
const put = (row, box, qty, rid) => db.addPlacement({ runId: run3, rowId: row.id, boxId: box.boxId, qty, worker: member, deviceKey: 'dev:7', deviceLabel: 'iPad7', requestId: rid });
t('全行を投入できる (行0→B1 / 行1→B1+B2 / 行2→B2 / 行3→B4)', () => {
  assert.equal(put(rows3[0], b1, 5, 'x1').ok, true);
  assert.equal(put(rows3[1], b1, 3, 'x2').ok, true);
  assert.equal(put(rows3[1], b2, 2, 'x3').ok, true);
  assert.equal(put(rows3[2], b2, 1, 'x4').ok, true);
  assert.equal(put(rows3[3], b4, 30, 'x5').ok, true);
});
t('readiness: 開いた箱・空箱がブロッカー', () => {
  const r = db.exportReadiness(run3);
  assert.equal(r.ok, false);
  const codes = r.blockers.map((b) => b.code);
  assert.ok(codes.includes('open_boxes'), codes.join());
  assert.ok(codes.includes('empty_boxes'), codes.join());
  assert.ok(!codes.includes('rows_incomplete'), codes.join());
});
t('voidBox: 中身ありは not_empty / 理由必須 / 空なら ok / 取消後は割当不可・二重は already', () => {
  assert.equal(db.voidBox({ boxId: b1.boxId, reason: 'x', worker: staff }).error, 'not_empty');
  assert.equal(db.voidBox({ boxId: b3.boxId, worker: staff }).error, 'reason_required');
  assert.equal(db.voidBox({ boxId: b3.boxId, reason: '箱が余った', worker: staff }).ok, true);
  assert.equal(put(rows3[0], b3, 1, 'x6').error, 'box_void');
  assert.equal(db.voidBox({ boxId: b3.boxId, reason: 'again', worker: staff }).already, true);
  assert.equal(db.closeBox({ boxId: b3.boxId, measuredKg: 1, worker: staff }).error, 'box_void');
});
t('Amazon 箱番号: 取消した箱を飛ばして詰める (B4 → 3番目) / 取消箱は null', () => {
  const st = db.getRunState(run3);
  const byId = new Map(st.boxes.map((b) => [b.id, b]));
  assert.equal(byId.get(b1.boxId).amazon_box_no, 1);
  assert.equal(byId.get(b2.boxId).amazon_box_no, 2);
  assert.equal(byId.get(b3.boxId).amazon_box_no, null);
  assert.equal(byId.get(b4.boxId).amazon_box_no, 3);
  assert.equal(byId.get(b4.boxId).amazon_name, 'P1 - B3');
  assert.equal(byId.get(b4.boxId).box_no, 4);
});
t('createBox: 取消後も box_no は再利用しない', () => {
  const b5 = mkBox('box140');
  assert.equal(b5.boxNo, 5);
  assert.equal(db.voidBox({ boxId: b5.boxId, reason: '試験', worker: staff }).ok, true);
});
t('closeBox 3箱 → readiness ok。警告 = 欠番 / 外寸なし (box160) / 確認担当なし', () => {
  assert.equal(db.closeBox({ boxId: b1.boxId, measuredKg: 12.4, closedReason: 'items_done', worker: staff }).ok, true);
  assert.equal(db.closeBox({ boxId: b2.boxId, measuredKg: 8, worker: staff }).ok, true);
  assert.equal(db.closeBox({ boxId: b4.boxId, measuredKg: 20.25, worker: staff }).ok, true);
  const r = db.exportReadiness(run3);
  assert.equal(r.ok, true, JSON.stringify(r.blockers));
  const w = r.warnings.map((x) => x.code);
  assert.ok(w.includes('box_gap'), w.join());
  assert.ok(w.includes('no_dims'), w.join());
  assert.ok(w.includes('unchecked_rows'), w.join());
  assert.equal(r.groups[0].boxes.length, 3);
  assert.equal(r.groups[0].boxes[2].amazonName, 'P1 - B3');
  assert.equal(r.expiries.length, 0);
});
const payload = db.buildExportPayload(run3);
t('buildExportPayload: 箱数1 + 数量5 + 重量3 + 外寸(140のみ)6 = 15セル、0 の箱は空欄のまま', () => {
  assert.equal(payload.ok, true, JSON.stringify(payload).slice(0, 300));
  const cells = payload.sheets[0].cells;
  const kinds = cells.reduce((a, c) => { a[c.kind] = (a[c.kind] || 0) + 1; return a; }, {});
  assert.deepEqual(kinds, { total_boxes: 1, qty: 5, weight: 3, width: 2, length: 2, height: 2 });
  const st = ing.parsed.sheets[0];
  assert.deepEqual(cells[0], { row: st.totalBoxes.row, col: st.totalBoxes.col, value: 3, kind: 'total_boxes' });
  const q = cells.filter((c) => c.kind === 'qty');
  // 行3 (予定30) は Amazon 3番目の箱 = boxColumns['3'] の列
  assert.ok(q.some((c) => c.row === rows3[3].excel_row && c.col === st.boxColumns['3'] && c.value === 30));
  assert.ok(q.some((c) => c.row === rows3[1].excel_row && c.col === st.boxColumns['1'] && c.value === 3));
  assert.ok(q.some((c) => c.row === rows3[1].excel_row && c.col === st.boxColumns['2'] && c.value === 2));
  assert.ok(cells.some((c) => c.kind === 'weight' && c.row === st.dimRows.weight && c.col === st.boxColumns['3'] && c.value === 20.25));
  assert.ok(cells.some((c) => c.kind === 'height' && c.col === st.boxColumns['1'] && c.value === 30.5));
  assert.equal(payload.snapshot.groups[0].boxes[2].contents[0].qty, 30);
});
const w3 = await xl.writePacklist({ templatePath: ing.storedPath, sheets: payload.sheets, fileTag: 'test' });
t('writePacklist (ゴールデン): 原本非破壊で書けて独自検算を通る (再読込一致・他エントリ byte 一致・fingerprint 不変)', () => {
  assert.equal(w3.ok, true, JSON.stringify(w3).slice(0, 400));
  assert.equal(w3.written, 15);
  assert.equal(w3.verify.cellsChecked, 15);
  assert.equal(w3.verify.fingerprint, 'd337e046bbf029c1');
  assert.deepEqual(w3.verify.changedEntries, ['xl/worksheets/sheet2.xml']);
  assert.ok(fs.existsSync(w3.outputPath));
  assert.notEqual(fs.readFileSync(w3.outputPath).length, 0);
});
const fixture1 = path.join(FIX, 'packlist_v1.1_2sku_15box.xlsx');
const ing1 = await xl.ingestPacklist(fs.readFileSync(fixture1), 'packlist_test1.xlsx');
const w1 = ing1.ok ? await xl.writePacklist({ templatePath: ing1.storedPath, fileTag: 'test1', sheets: [{ sheetName: ing1.parsed.sheets[0].sheetName, cells: [
  { row: ing1.parsed.sheets[0].totalBoxes.row, col: ing1.parsed.sheets[0].totalBoxes.col, value: 1, kind: 'total_boxes' },
  { row: ing1.parsed.sheets[0].skuRows[0].row, col: ing1.parsed.sheets[0].boxColumns['1'], value: 3, kind: 'qty' },
  { row: ing1.parsed.sheets[0].dimRows.weight, col: ing1.parsed.sheets[0].boxColumns['1'], value: 4.2, kind: 'weight' },
] }] }) : null;
t('writePacklist (ゴールデン2件目: 2SKU/15箱列・箱名行が9行目) も通る', () => {
  assert.equal(ing1.ok, true, JSON.stringify(ing1).slice(0, 300));
  assert.equal(w1.ok, true, JSON.stringify(w1).slice(0, 400));
  assert.equal(w1.verify.cellsChecked, 3);
});
const wBad = await xl.writePacklist({ templatePath: ing1.storedPath, fileTag: 'bad', sheets: [{ sheetName: ing1.parsed.sheets[0].sheetName, cells: [
  { row: ing1.parsed.sheets[0].skuRows[0].row, col: ing1.parsed.sheets[0].headers['輸送箱の数'], value: 3, kind: 'qty' } ] }] });
t('writePacklist: 数式セル (輸送箱の数 = SUM) への書込は拒否', () => {
  assert.equal(wBad.ok, false);
  assert.equal(wBad.error, 'formula_cell');
});
const wMissing = await xl.writePacklist({ templatePath: path.join(tmp, 'nope.xlsx'), sheets: [] });
t('writePacklist: 原本が無ければ template_missing', () => {
  assert.equal(wMissing.error, 'template_missing');
});

const ex1 = db.recordExport({ runId: run3, excelFileId: payload.excelFile.id, dataVersion: payload.snapshot.dataVersion, fileName: 'packlist_test.xlsx',
  storedPath: w3.outputPath, sha256: w3.sha256, snapshot: payload.snapshot, verify: w3.verify, createdBy: 't' });
t('recordExport → listExports (最新・stale でない) / getExport でスナップショット復元', () => {
  assert.equal(ex1.ok, true);
  assert.equal(ex1.stale, false);
  const list = db.listExports(run3);
  assert.equal(list.length, 1);
  assert.equal(list[0].stale, 0);
  assert.equal(db.getExport(ex1.exportId).snapshot.groups[0].totalBoxes, 3);
  assert.equal(db.getRunState(run3).exportState.stale, false);
});
t('版管理: 出力後の変更 (箱の開け直し→閉じ直し) で旧版になり、旧版の STAアップ記録は拒否', () => {
  db.reopenBox({ boxId: b2.boxId, reason: '詰め直し', worker: staff });
  db.closeBox({ boxId: b2.boxId, measuredKg: 8.1, worker: staff });
  assert.equal(db.listExports(run3)[0].stale, 1);
  assert.equal(db.getRunState(run3).exportState.stale, true);
  assert.equal(db.exportReadiness(run3).warnings.some((w) => w.code === 'stale_export'), true);
  assert.equal(db.markStaUploaded({ runId: run3, exportId: ex1.exportId, actor: 't' }).error, 'stale_export');
});
t('再出力 → STAアップ済み記録 → 納品回 done・data_version は動かない (setRunStatus も動かさない)', () => {
  const p2 = db.buildExportPayload(run3);
  assert.equal(p2.ok, true);
  const ex2 = db.recordExport({ runId: run3, excelFileId: p2.excelFile.id, dataVersion: p2.snapshot.dataVersion, fileName: 'packlist_test.xlsx',
    storedPath: w3.outputPath, sha256: w3.sha256, snapshot: p2.snapshot, verify: null, createdBy: 't' });
  assert.equal(ex2.stale, false);
  const before = db.getRun(run3).data_version;
  assert.equal(db.markStaUploaded({ runId: run3, exportId: ex2.exportId, actor: 't' }).ok, true);
  const run = db.getRun(run3);
  assert.equal(run.status, 'done');
  assert.equal(run.sta_export_id, ex2.exportId);
  assert.ok(run.sta_uploaded_at);
  assert.equal(run.data_version, before);
  assert.equal(db.listExports(run3).find((e) => e.id === ex2.exportId).sta_uploaded, 1);
  // done 後も readiness は出力可 (再DL用)、iPad からの割当は不可
  assert.equal(db.exportReadiness(run3).ok, true);
  assert.equal(put(rows3[0], b1, 1, 'x9').error, 'run_not_active');
});
t('PR2 の操作が fbx_events に残っている', () => {
  const actions = new Set(db.listEvents(1000).map((e) => e.action));
  for (const a of ['box_void', 'excel_export', 'run_sta_uploaded', 'material_upsert']) assert.ok(actions.has(a), `missing event: ${a}`);
});

console.log('■ PR2: fbx_boxes の void 移行');
const Database = (await import('better-sqlite3')).default;
const mdb = new Database(path.join(tmp, 'migrate.db'));
mdb.pragma('journal_mode = WAL'); mdb.pragma('foreign_keys = ON');
db.createTables(mdb);
// PR1 時点のスキーマ (status CHECK が open/closed のみ) に戻してデータを入れる
mdb.pragma('foreign_keys = OFF');
mdb.exec(`DROP TABLE fbx_boxes;
  CREATE TABLE fbx_boxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, pack_group_id INTEGER NOT NULL REFERENCES fbx_pack_groups(id),
    box_no INTEGER NOT NULL CHECK (box_no >= 1), box_code TEXT NOT NULL, material_code TEXT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed')),
    measured_weight_kg REAL CHECK (measured_weight_kg IS NULL OR measured_weight_kg > 0),
    closed_at TEXT, closed_by TEXT, closed_reason TEXT,
    cushion_level TEXT CHECK (cushion_level IS NULL OR cushion_level IN ('none','little','much')),
    reopen_count INTEGER NOT NULL DEFAULT 0, created_by TEXT, created_at TEXT NOT NULL, UNIQUE(pack_group_id, box_no));`);
mdb.pragma('foreign_keys = ON');
mdb.exec(`INSERT INTO fbx_runs (id, source_run_id, title, status, created_at) VALUES (1, 1, 'r', 'active', 'now');
  INSERT INTO fbx_excel_files (id, run_id, stored_path, sha256, fingerprint, uploaded_at) VALUES (1, 1, '/x', 'h', 'f', 'now');
  INSERT INTO fbx_pack_groups (id, run_id, excel_file_id, sheet_name, packing_group_id, display_name) VALUES (1, 1, 1, 's', 'pg1', 'G1');
  INSERT INTO fbx_rows (id, run_id, pack_group_id, excel_row, seller_sku, fnsku, planned_qty) VALUES (1, 1, 1, 6, 'sku', 'X1', 5);
  INSERT INTO fbx_boxes (id, pack_group_id, box_no, box_code, status, measured_weight_kg, closed_at, created_at) VALUES (1, 1, 1, 'G1-B1', 'closed', 3.5, 'now', 'now');
  INSERT INTO fbx_boxes (id, pack_group_id, box_no, box_code, created_at) VALUES (2, 1, 2, 'G1-B2', 'now');
  INSERT INTO fbx_placements (run_id, row_id, box_id, qty, box_seq, device_key, request_id, created_at) VALUES (1, 1, 1, 5, 1, 'd', 'r', 'now');`);
t('PR1 スキーマの fbx_boxes を void 対応へ再構築: データ保持・FK 健全・冪等', () => {
  assert.equal(/'void'/.test(mdb.prepare(`SELECT sql FROM sqlite_master WHERE name = 'fbx_boxes'`).get().sql), false);
  db.createTables(mdb);
  const sql = mdb.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fbx_boxes'`).get().sql;
  assert.ok(sql.includes("'void'"));
  assert.ok(sql.includes('voided_at'));
  assert.equal(mdb.prepare('SELECT COUNT(*) c FROM fbx_boxes').get().c, 2);
  const b = mdb.prepare('SELECT * FROM fbx_boxes WHERE id = 1').get();
  assert.equal(b.status, 'closed'); assert.equal(b.measured_weight_kg, 3.5); assert.equal(b.box_code, 'G1-B1');
  assert.equal(mdb.prepare('SELECT box_id FROM fbx_placements').get().box_id, 1);
  assert.deepEqual(mdb.prepare('PRAGMA foreign_key_check').all(), []);
  assert.equal(mdb.pragma('foreign_keys', { simple: true }), 1);
  assert.equal(mdb.prepare('SELECT COUNT(*) c FROM sqlite_master WHERE name = ?').get('fbx_boxes_new').c, 0);
  db.createTables(mdb);   // 2回目は何もしない
  assert.equal(mdb.prepare('SELECT COUNT(*) c FROM fbx_boxes').get().c, 2);
  // 移行後の表で void が使える
  mdb.prepare(`UPDATE fbx_boxes SET status = 'void', voided_at = 'now' WHERE id = 2`).run();
  assert.equal(mdb.prepare('SELECT status FROM fbx_boxes WHERE id = 2').get().status, 'void');
});
mdb.close();

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
