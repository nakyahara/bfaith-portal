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
t('matchExcelSheetsToGroups: 全体最適の一対一 (貪欲だと衝突する例) / 同点は曖昧 / 重なり無しは unmatched / 1×1 は重なり 0 でも対応', () => {
  const sheet = (name, fn) => ({ sheetName: name, skuRows: fn.map((f) => ({ fnsku: f })) });
  // S1: G1=2/G2=1, S2: G1=3/G2=2 → 最適は S1→G2, S2→G1 (合計4)。貪欲 (S1→G1) だと S2 が衝突する
  const groups = [{ id: 1, name: 'G1', fnskus: ['A', 'B', 'C', 'D'] }, { id: 2, name: 'G2', fnskus: ['A', 'E', 'F'] }];
  const m = svc.matchExcelSheetsToGroups([sheet('S1', ['A', 'B', 'E']), sheet('S2', ['A', 'C', 'D', 'F'])], groups);
  assert.equal(m.ok, true, JSON.stringify(m));
  assert.deepEqual(m.assignments.map((a) => [a.sheetName, a.groupId]), [['S1', 2], ['S2', 1]]);
  assert.deepEqual(m.unassignedGroups, []);
  const amb = svc.matchExcelSheetsToGroups([sheet('S1', ['A']), sheet('S2', ['A'])], groups);
  assert.equal(amb.ok, false); assert.ok(amb.issues.some((i) => i.kind === 'ambiguous'));
  const un = svc.matchExcelSheetsToGroups([sheet('S1', ['Z'])], groups);
  assert.equal(un.ok, false); assert.ok(un.issues.some((i) => i.kind === 'unmatched_sheet'));
  const one = svc.matchExcelSheetsToGroups([sheet('S1', ['Z'])], [groups[0]]);
  assert.equal(one.ok, true); assert.equal(one.assignments[0].overlap, 0);
  const partial = svc.matchExcelSheetsToGroups([sheet('S1', ['E', 'F'])], groups);
  assert.equal(partial.ok, true); assert.deepEqual(partial.unassignedGroups, [1]);
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
t('createBox: 箱コードは グループ名-連番 (B は付けない)', () => {
  assert.equal(box1.ok, true);
  assert.equal(box1.boxNo, 1);
  assert.equal(box1.boxCode, 'G1-1');
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

// 取消 (入力ミスの訂正は PIN 不要 — 中原さん 9/3)
t('取消: 自端末の記録は誰でも取り消せる・数が戻る', () => {
  const r = db.revokePlacement({ placementId: p1.placementId, worker: member, deviceKey: 'dev:1' });
  assert.equal(r.ok, true);
  assert.equal(db.getRunState(runId).rows.find(x => x.id === rowA.id).placed, 5);
});
t('取消: 他の端末の記録・時間が経った記録も PIN なしで取り消せる (監査には残る)', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 3, worker: member, deviceKey: 'dev:1', requestId: 'v1' });
  db.getDB().prepare(`UPDATE fbx_placements SET created_at = '2020-01-01T00:00:00.000Z' WHERE id = ?`).run(p.placementId);
  const r = db.revokePlacement({ placementId: p.placementId, worker: member, deviceKey: 'dev:2' });
  assert.equal(r.ok, true, JSON.stringify(r));
  const ev = db.listEvents(5).find((e) => e.action === 'placement_revoke');
  assert.equal(JSON.parse(ev.payload).otherDevice, true);
});
t('取消: 職員として明示的に行う場合だけ理由が要る', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 2, worker: member, deviceKey: 'dev:1', requestId: 'v2' });
  assert.equal(db.revokePlacement({ placementId: p.placementId, byStaff: true, worker: staff, deviceKey: 'dev:2' }).error, 'reason_required');
  assert.equal(db.revokePlacement({ placementId: p.placementId, byStaff: true, reason: '誤入力', worker: staff, deviceKey: 'dev:2' }).ok, true);
});
t('adjustPlacement: 間違えた数を直す = 取消 + 入れ直し (同じ箱)。0 は取消だけ。残数超なら取消ごと戻す。PIN 不要 (他端末・閉じた箱でも)', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 4, worker: member, deviceKey: 'dev:1', requestId: 'adj0' });
  const before = db.getRunState(runId).rows.find((x) => x.id === rowA.id).placed;
  const r = db.adjustPlacement({ placementId: p.placementId, qty: 2, worker: member, deviceKey: 'dev:1', deviceLabel: 'iPad1', requestId: 'adj1' });
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.equal(r.from, 4); assert.equal(r.to, 2);
  assert.equal(db.getRunState(runId).rows.find((x) => x.id === rowA.id).placed, before - 2);
  const st = db.getRunState(runId);
  assert.equal(st.placements.find((x) => x.id === r.placementId).qty, 2);
  assert.equal(st.placements.some((x) => x.id === p.placementId), false, '元の記録は取消済み');
  // 残数を超える数には直せない → 元の記録も戻る
  const bad = db.adjustPlacement({ placementId: r.placementId, qty: 999, worker: member, deviceKey: 'dev:1', requestId: 'adj2' });
  assert.equal(bad.error, 'over_qty');
  assert.equal(db.getRunState(runId).placements.find((x) => x.id === r.placementId).revoked_at, null);
  assert.equal(db.getRunState(runId).rows.find((x) => x.id === rowA.id).placed, before - 2);
  // 他端末からでも PIN なしで直せる
  const st2 = db.adjustPlacement({ placementId: r.placementId, qty: 1, worker: member, deviceKey: 'dev:2', requestId: 'adj4' });
  assert.equal(st2.ok, true, JSON.stringify(st2));
  // 閉じた箱の記録も直せる → 箱が自動で開き実測重量が消える (量り直し)
  db.closeBox({ boxId: box1.boxId, measuredKg: 9.9, worker: staff });
  const cl = db.adjustPlacement({ placementId: st2.placementId, qty: 2, worker: member, deviceKey: 'dev:1', requestId: 'adj-c2' });
  assert.equal(cl.ok, true, JSON.stringify(cl)); assert.equal(cl.boxReopened, true);
  const bx1 = db.getRunState(runId).boxes.find((x) => x.id === box1.boxId);
  assert.equal(bx1.status, 'open'); assert.equal(bx1.measured_weight_kg, null);
  assert.equal(db.getRunState(runId).placements.find((x) => x.id === cl.placementId).box_id, box1.boxId);
  // 再送 (通信断のリトライ) は同じ結果を返す — 二重に直さない
  const again = db.adjustPlacement({ placementId: st2.placementId, qty: 2, worker: member, deviceKey: 'dev:1', requestId: 'adj-c2' });
  assert.equal(again.ok, true); assert.equal(again.already, true); assert.equal(again.placementId, cl.placementId);
  // 0 = 取消だけ。その再送も成功で返る
  const z = db.adjustPlacement({ placementId: cl.placementId, qty: 0, worker: member, deviceKey: 'dev:1', requestId: 'adj5' });
  assert.equal(z.ok, true); assert.equal(z.placementId, null); assert.equal(z.from, 2);
  const zAgain = db.adjustPlacement({ placementId: cl.placementId, qty: 0, worker: member, deviceKey: 'dev:1', requestId: 'adj5' });
  assert.equal(zAgain.ok, true); assert.equal(zAgain.already, true);
  assert.equal(db.getRunState(runId).rows.find((x) => x.id === rowA.id).placed, before - 4);
  assert.ok(db.listEvents(30).some((e) => e.action === 'placement_adjust'));
});
t('box_seq は取消後も再利用しない', () => {
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 's1' });
  const seqs = db.getDB().prepare('SELECT box_seq FROM fbx_placements WHERE box_id = ? ORDER BY box_seq').all(box1.boxId).map(x => x.box_seq);
  assert.equal(new Set(seqs).size, seqs.length);
  assert.equal(p.boxSeq, Math.max(...seqs));
});

// 箱クローズ
t('closeBox: 読み合わせ中に中身が変わっていたら閉じさせない (box_changed)', () => {
  const v0 = db.getBox(box1.boxId).content_version;
  const p = db.addPlacement({ runId, rowId: rowA.id, boxId: box1.boxId, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 'cv1' });
  assert.equal(p.ok, true);
  const v1 = db.getBox(box1.boxId).content_version;
  assert.equal(v1, v0 + 1, '割当で版が上がる');
  const stale = db.closeBox({ boxId: box1.boxId, measuredKg: 5, worker: staff, expectedContentVersion: v0 });
  assert.equal(stale.error, 'box_changed');
  assert.equal(stale.contentVersion, v1);
  db.revokePlacement({ placementId: p.placementId, worker: member, deviceKey: 'dev:1' });
  assert.equal(db.getBox(box1.boxId).content_version, v1 + 1, '取消でも版が上がる');
  // 版を合わせれば閉じられる
  const v2 = db.getBox(box1.boxId).content_version;
  assert.equal(db.closeBox({ boxId: box1.boxId, measuredKg: 5, worker: staff, expectedContentVersion: v2 }).ok, true);
  db.reopenBox({ boxId: box1.boxId, reason: 'テスト戻し', worker: staff });
});
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
t('閉じた箱の割当も PIN なしで取り消せる → 箱が開いて実測重量が消える (量り直し)', () => {
  const st = db.getRunState(runId);
  const p = st.placements.find(x => x.box_id === box1.boxId);
  const before = st.rows.find(x => x.id === p.row_id).placed;
  assert.equal(st.boxes.find(x => x.id === box1.boxId).status, 'closed');
  const r = db.revokePlacement({ placementId: p.id, worker: member, deviceKey: 'dev:1' });
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.equal(r.boxReopened, true);
  const b = db.getRunState(runId).boxes.find(x => x.id === box1.boxId);
  assert.equal(b.status, 'open'); assert.equal(b.measured_weight_kg, null);
  assert.ok(db.listEvents(5).some(e => e.action === 'box_reopen' && JSON.parse(e.payload || '{}').auto === true));
  // 戻す (以降のテストの前提 = 中身のある閉じた箱 を保つ)
  db.addPlacement({ runId, rowId: p.row_id, boxId: p.box_id, qty: p.qty, expiry: p.expiry, worker: member, deviceKey: 'dev:1', requestId: 'restore-' + p.id });
  assert.equal(db.getRunState(runId).rows.find(x => x.id === p.row_id).placed, before);
  assert.equal(db.closeBox({ boxId: box1.boxId, measuredKg: 12.4, worker: staff }).ok, true);
});
t('reopenBox: 理由必須・実測がクリアされ再クローズ要', () => {
  const before = db.getRunState(runId).boxes.find(x => x.id === box1.boxId).reopen_count;
  assert.equal(db.reopenBox({ boxId: box1.boxId, worker: staff }).error, 'reason_required');
  const r = db.reopenBox({ boxId: box1.boxId, reason: '詰め直し', worker: staff });
  assert.equal(r.ok, true);
  const b = db.getRunState(runId).boxes.find(x => x.id === box1.boxId);
  assert.equal(b.status, 'open');
  assert.equal(b.measured_weight_kg, null);
  assert.equal(b.reopen_count, before + 1);
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
t('buildExportPayload: 箱数1 + 数量5 + 重量3 + 外寸(140のみ)6 = 15セル、それ以外の入力セル (4行+4寸法行)×19列 は clear', () => {
  assert.equal(payload.ok, true, JSON.stringify(payload).slice(0, 300));
  assert.equal(payload.exports.length, 1);
  const cells = payload.exports[0].sheets[0].cells;
  const kinds = cells.reduce((a, c) => { a[c.kind] = (a[c.kind] || 0) + 1; return a; }, {});
  assert.deepEqual(kinds, { total_boxes: 1, qty: 5, weight: 3, width: 2, length: 2, height: 2, clear: 8 * 19 - 14 });
  assert.equal(new Set(cells.map((c) => `${c.row}|${c.col}`)).size, cells.length, '同一セルの二重指定なし');
  const st = ing.parsed.sheets[0];
  assert.deepEqual(cells[0], { row: st.totalBoxes.row, col: st.totalBoxes.col, value: 3, kind: 'total_boxes' });
  const q = cells.filter((c) => c.kind === 'qty');
  // 行3 (予定30) は Amazon 3番目の箱 = boxColumns['3'] の列
  assert.ok(q.some((c) => c.row === rows3[3].excel_row && c.col === st.boxColumns['3'] && c.value === 30));
  assert.ok(q.some((c) => c.row === rows3[1].excel_row && c.col === st.boxColumns['1'] && c.value === 3));
  assert.ok(q.some((c) => c.row === rows3[1].excel_row && c.col === st.boxColumns['2'] && c.value === 2));
  assert.ok(cells.some((c) => c.kind === 'weight' && c.row === st.dimRows.weight && c.col === st.boxColumns['3'] && c.value === 20.25));
  assert.ok(cells.some((c) => c.kind === 'height' && c.col === st.boxColumns['1'] && c.value === 30.5));
  assert.equal(payload.exports[0].snapshot.groups[0].boxes[2].contents[0].qty, 30);
});
const w3 = await xl.writePacklist({ templatePath: ing.storedPath, sheets: payload.exports[0].sheets, fileTag: 'test' });
t('writePacklist (ゴールデン): 原本非破壊で書けて独自検算を通る (再読込一致・他エントリ byte 一致・fingerprint 不変)', () => {
  assert.equal(w3.ok, true, JSON.stringify(w3).slice(0, 400));
  assert.equal(w3.written, 15);
  assert.equal(w3.cleared, 8 * 19 - 14);
  assert.equal(w3.verify.cellsChecked, payload.exports[0].sheets[0].cells.length);
  assert.equal(w3.verify.fingerprint, 'd337e046bbf029c1');
  assert.deepEqual(w3.verify.changedEntries, ['xl/worksheets/sheet2.xml']);
  assert.ok(fs.existsSync(w3.outputPath));
  assert.notEqual(fs.readFileSync(w3.outputPath).length, 0);
});
const ingFilled = await xl.ingestPacklist(fs.readFileSync(w3.outputPath), 'filled.xlsx');
t('記入済み (出力済み) ファイルの再アップロードは prefilled_template で拒否', () => {
  assert.equal(ingFilled.ok, false);
  assert.equal(ingFilled.error, 'prefilled_template');
});
// 記入済み原本に対しても clear で古い値が消えることを確認 (取込ガードを迂回した二重防御の検証):
// 出力済みファイルを原本にして「数量 1 セルだけ」を書くと、他の数量・寸法は空になる
const wClear = await xl.writePacklist({ templatePath: w3.outputPath, fileTag: 'clear', sheets: [{ sheetName: payload.exports[0].sheets[0].sheetName,
  cells: payload.exports[0].sheets[0].cells.map((c) => (c.kind === 'qty' && c.value === 30 ? c : (c.kind === 'total_boxes' ? c : { row: c.row, col: c.col, value: null, kind: 'clear' }))) }] });
t('clear: 原本に残った値を空にできる (再読込で None)', () => {
  assert.equal(wClear.ok, true, JSON.stringify(wClear).slice(0, 400));
  assert.equal(wClear.written, 2);
  assert.equal(wClear.cleared, payload.exports[0].sheets[0].cells.length - 2);
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

const ex1 = db.recordExport({ runId: run3, excelFileId: payload.exports[0].excelFile.id, dataVersion: payload.exports[0].snapshot.dataVersion, fileName: 'packlist_test.xlsx',
  storedPath: w3.outputPath, sha256: w3.sha256, snapshot: payload.exports[0].snapshot, verify: w3.verify, createdBy: 't' });
t('recordExport → listExports (最新・stale でない) / getExport でスナップショット復元', () => {
  assert.equal(ex1.ok, true);
  assert.equal(ex1.stale, false);
  const list = db.listExports(run3);
  assert.equal(list.length, 1);
  assert.equal(list[0].stale, 0);
  assert.equal(db.getExport(ex1.exportId).snapshot.groups[0].totalBoxes, 3);
  assert.equal(db.getRunState(run3).exportState.stale, false);
});
t('資材の外寸変更 → その資材の箱を持つ未アップの納品回の版が進む (旧版になる)。名前だけの変更では進まない', () => {
  const v0 = db.getRun(run3).data_version;
  const r1 = db.upsertMaterial({ code: 'box140', name: '140サイズ段ボール (改)', tareG: 900, widthCm: 45, lengthCm: 35, heightCm: 30.5, sort: 1, actor: 't' });
  assert.deepEqual(r1.bumpedRuns, []);
  assert.equal(db.getRun(run3).data_version, v0);
  const r2 = db.upsertMaterial({ code: 'box140', name: '140サイズ段ボール', tareG: 900, widthCm: 46, lengthCm: 35, heightCm: 30.5, sort: 1, actor: 't' });
  // box140 の箱を持つ未アップの回 (この試験では run1 / c2 / run3) が全て対象。run3 は必ず含まれる
  assert.ok(r2.bumpedRuns.includes(run3), JSON.stringify(r2.bumpedRuns));
  assert.equal(db.getRun(run3).data_version, v0 + 1);
  assert.equal(db.listExports(run3)[0].stale, 1);
  assert.equal(db.markStaUploaded({ runId: run3, exportId: ex1.exportId, actor: 't' }).error, 'stale_export');
  // 新しい出力の外寸は 46 になる
  const p = db.buildExportPayload(run3);
  assert.ok(p.exports[0].sheets[0].cells.some((c) => c.kind === 'width' && c.value === 46));
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
  const ex2 = db.recordExport({ runId: run3, excelFileId: p2.exports[0].excelFile.id, dataVersion: p2.exports[0].snapshot.dataVersion, fileName: 'packlist_test.xlsx',
    storedPath: w3.outputPath, sha256: w3.sha256, snapshot: p2.exports[0].snapshot, verify: null, createdBy: 't' });
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
  // STAアップ済みの記録は上書きしない: 同じ版は冪等、別の出力は already_uploaded
  assert.equal(db.markStaUploaded({ runId: run3, exportId: ex2.exportId, actor: 't' }).already, true);
  const ex3 = db.recordExport({ runId: run3, excelFileId: p2.exports[0].excelFile.id, dataVersion: p2.exports[0].snapshot.dataVersion, fileName: 'x.xlsx',
    storedPath: w3.outputPath, sha256: w3.sha256, snapshot: p2.exports[0].snapshot, verify: null, createdBy: 't' });
  assert.equal(db.markStaUploaded({ runId: run3, exportId: ex3.exportId, actor: 't' }).error, 'already_uploaded');
  assert.equal(db.getRun(run3).sta_export_id, ex2.exportId);
  // STAアップ済みの回は資材の外寸を変えても版が進まない (アップ済み Excel の版を守る)
  const vDone = db.getRun(run3).data_version;
  db.upsertMaterial({ code: 'box140', name: '140サイズ段ボール', tareG: 900, widthCm: 47, lengthCm: 35, heightCm: 30.5, sort: 1, actor: 't' });
  assert.equal(db.getRun(run3).data_version, vDone);
});
t('名簿 bootstrap は一度 PIN が設定されたら閉じたまま (職員が全員無効でも戻らない)', () => {
  assert.equal(db.isRosterBootstrap(), false);
  db.setWorkerActive(staff.id, false);
  assert.equal(db.countStaffWithPin(), 0);
  assert.equal(db.isRosterBootstrap(), false);
  db.setWorkerActive(staff.id, true);
});
t('PR2 の操作が fbx_events に残っている', () => {
  const actions = new Set(db.listEvents(1000).map((e) => e.action));
  for (const a of ['box_void', 'excel_export', 'run_sta_uploaded', 'material_upsert']) assert.ok(actions.has(a), `missing event: ${a}`);
});

console.log('■ PR2.5: picking 実行から納品回 → 箱詰め → Excel 後付け');
const f1rows = ing1.parsed.sheets[0].skuRows;   // 2 SKU (3, 3)
const f2rows = ing.parsed.sheets[0].skuRows;    // 4 SKU (5, 5, 1, 30)
const pkSheets = [
  { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
    { no: 1, sku: f1rows[0].sku, fnsku: f1rows[0].fnsku, productName: '商品A', qty: String(f1rows[0].plannedQty) },
    { no: 2, sku: f1rows[1].sku, fnsku: f1rows[1].fnsku, productName: '商品B', qty: String(f1rows[1].plannedQty + 1) },   // Excel と数量差
    { no: 3, sku: 'fake-sku', fnsku: 'X0FAKE00001', productName: 'Excelに無い商品', qty: '2' },                            // picking_only
  ] },
  // 4 つ目の SKU を落とす → Excel 添付で excel_only として増える
  { slotId: 'p2_normal', sheet: 'P2_通常', label: '通常プラン2', rows: f2rows.slice(0, 3).map((r, i) => ({ no: i + 1, sku: r.sku, fnsku: r.fnsku, productName: '商品' + i, qty: String(r.plannedQty) })) },
];
const pr = db.createRunFromPicking({ pickingRun: { id: 200, delivery_date: '2026-09-20', run_at: '2026-09-03 10:00' }, planSheets: pkSheets, createdBy: 'picking@test' });
t('createRunFromPicking: すぐ active・グループ=プラン別シート・行は pending・SKU あり・Excel なし', () => {
  assert.equal(pr.ok, true, JSON.stringify(pr));
  assert.equal(pr.created, true);
  const st = db.getRunState(pr.runId);
  assert.equal(st.run.status, 'active');
  assert.equal(st.run.title, '2026-09-20 納品分');
  assert.equal(st.groups.length, 2);
  assert.equal(st.groups[0].display_name, '通常');
  assert.equal(st.groups[0].excel_file_id, null);
  assert.equal(st.groups[0].source_slot_id, 'p1_normal');
  assert.equal(st.rows.length, 6);
  assert.ok(st.rows.every((r) => r.match_state === 'pending' && r.excel_row === null));
  assert.equal(st.rows.find((r) => r.plan_no === '通常_1').seller_sku, f1rows[0].sku);
  assert.equal(st.excelFiles.length, 0);
});
t('同じ picking 実行はもう一度作らない (already) / getRunBySource', () => {
  const again = db.createRunFromPicking({ pickingRun: { id: 200 }, planSheets: pkSheets, createdBy: 't' });
  assert.equal(again.already, true);
  assert.equal(again.runId, pr.runId);
  assert.equal(db.getRunBySource(200).id, pr.runId);
  assert.equal(db.getRunBySource(999), null);
  assert.equal(db.createRunFromPicking({ pickingRun: { id: 201 }, planSheets: [], createdBy: 't' }).error, 'no_rows');
});
const st4 = db.getRunState(pr.runId);
const g1 = st4.groups[0].id, g2 = st4.groups[1].id;
const rowsOf = (gid) => db.getRunState(pr.runId).rows.filter((r) => r.pack_group_id === gid).sort((a, b) => a.id - b.id);
let fakePlacement = null;
t('Excel なしでも箱を作って割当できる (箱コード = ラベル-B連番)。readiness は no_excel でブロック', () => {
  const bx = db.createBox({ packGroupId: g1, materialCode: 'box140', worker: member });
  assert.equal(bx.boxCode, '通常-1');
  const rA = rowsOf(g1)[0];
  assert.equal(db.addPlacement({ runId: pr.runId, rowId: rA.id, boxId: bx.boxId, qty: 3, worker: member, deviceKey: 'dev:8', requestId: 'pk1' }).ok, true);
  // Excel 添付前は「Excel に無い商品」も入れられてしまう (pending) → 添付後に picking_only_placed で止まる (下で検証)
  const fake = rowsOf(g1).find((r) => r.fnsku === 'X0FAKE00001');
  fakePlacement = db.addPlacement({ runId: pr.runId, rowId: fake.id, boxId: bx.boxId, qty: 1, worker: member, deviceKey: 'dev:8', requestId: 'pk-fake' });
  assert.equal(fakePlacement.ok, true);
  const rd = db.exportReadiness(pr.runId);
  assert.ok(rd.blockers.some((b) => b.code === 'no_excel'));
  assert.equal(rd.groups[0].excelAttached, false);
});
const at1 = db.attachExcelToRun({ runId: pr.runId, parsed: ing1.parsed, file: { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
t('attachExcelToRun (P1): FNSKU の重なりでグループ1に対応。matched / qty_mismatch (Excel が正) / picking_only を分類', () => {
  assert.equal(at1.ok, true, JSON.stringify(at1));
  assert.equal(at1.groups.length, 1);
  assert.equal(at1.groups[0].groupId, g1);
  assert.equal(at1.groups[0].matched, 1);
  assert.equal(at1.groups[0].qty_mismatch, 1);
  assert.equal(at1.groups[0].picking_only, 1);
  assert.deepEqual(at1.unassignedGroups, [g2]);
  const rows = rowsOf(g1);
  const rB = rows.find((r) => r.fnsku === f1rows[1].fnsku);
  assert.equal(rB.match_state, 'qty_mismatch');
  assert.equal(rB.planned_qty, f1rows[1].plannedQty);
  assert.equal(rB.picking_qty, f1rows[1].plannedQty + 1);
  assert.equal(rows.find((r) => r.fnsku === 'X0FAKE00001').match_state, 'picking_only');
  assert.ok(rows.every((r) => (r.match_state === 'picking_only' ? r.excel_row === null : r.excel_row > 0)));
  const st = db.getRunState(pr.runId);
  assert.equal(st.groups[0].excel_file_id, at1.excelFileId);
  assert.equal(st.groups[0].packing_group_id, ing1.parsed.sheets[0].packingGroupId);
  assert.equal(st.groups[0].excel_sheet_name, ing1.parsed.sheets[0].sheetName);
  assert.equal(st.groups[0].max_box_columns, 15);
  assert.equal(st.groups[1].excel_file_id, null);
  assert.ok(db.exportReadiness(pr.runId).blockers.some((b) => b.code === 'no_excel'));
});
const at2 = db.attachExcelToRun({ runId: pr.runId, parsed: ing.parsed, file: { originalName: 'p2.xlsx', storedPath: ing.storedPath, sha256: ing.sha256 }, actor: 't' });
t('attachExcelToRun (P2): 2 ファイル目はグループ2へ。Excel にだけある商品は excel_only (origin=excel) で行が増える', () => {
  assert.equal(at2.ok, true, JSON.stringify(at2));
  assert.equal(at2.groups[0].groupId, g2);
  assert.equal(at2.groups[0].matched, 3);
  assert.equal(at2.groups[0].excel_only, 1);
  assert.deepEqual(at2.unassignedGroups, []);
  assert.equal(rowsOf(g2).length, 4);
  const eo = rowsOf(g2).find((r) => r.match_state === 'excel_only');
  assert.ok(eo && eo.excel_row > 0 && eo.origin === 'excel');
  assert.ok(rowsOf(g1).every((r) => r.origin === 'picking'));
  assert.equal(db.getRunState(pr.runId).excelFiles.length, 2);
  assert.ok(!db.exportReadiness(pr.runId).blockers.some((b) => b.code === 'no_excel'));
});
t('添付の拒否: Excel の予定数 < 投入+不足 (over_placed)。取消すれば添付できる', () => {
  // 別の納品回: 行A を予定 5 で作って 5 個投入 → Excel (予定 3) を添付 → 拒否
  const c = db.createRunFromPicking({ pickingRun: { id: 300, delivery_date: '2026-09-21' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: f1rows[0].sku, fnsku: f1rows[0].fnsku, productName: 'A', qty: '5' }, { no: 2, sku: f1rows[1].sku, fnsku: f1rows[1].fnsku, productName: 'B', qty: '3' }] },
  ], createdBy: 't' });
  const st = db.getRunState(c.runId);
  const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
  const rA = st.rows.find((r) => r.plan_no === '通常_1');
  const p = db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bx.boxId, qty: 5, worker: member, deviceKey: 'dev:9', requestId: 'op1' });
  assert.equal(p.ok, true);
  const bad = db.attachExcelToRun({ runId: c.runId, parsed: ing1.parsed, file: { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
  assert.equal(bad.ok, false); assert.equal(bad.error, 'attach_conflict');
  assert.ok(bad.conflicts.some((x) => x.kind === 'over_placed' && x.placed === 5 && x.excelQty === 3));
  assert.equal(db.getRunState(c.runId).excelFiles.length, 0, '拒否時はファイル記録を残さない');
  db.revokePlacement({ placementId: p.placementId, byStaff: true, reason: '多すぎ', worker: staff, deviceKey: 'dev:9' });
  const okr = db.attachExcelToRun({ runId: c.runId, parsed: ing1.parsed, file: { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
  assert.equal(okr.ok, true, JSON.stringify(okr));
  assert.equal(db.getRunState(c.runId).rows.find((r) => r.id === rA.id).planned_qty, 3);
});
t('再添付で消えた excel_only 行: 記録なしは削除 / 取消済み記録だけなら retired / 投入ありは拒否 (excel_only_placed)', () => {
  // picking 側に 1 SKU だけの回に fixture1 (2 SKU) を添付 → 2 つ目が excel_only
  const c = db.createRunFromPicking({ pickingRun: { id: 301, delivery_date: '2026-09-22' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: f1rows[0].sku, fnsku: f1rows[0].fnsku, productName: 'A', qty: '3' }] },
  ], createdBy: 't' });
  const file = { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 };
  assert.equal(db.attachExcelToRun({ runId: c.runId, parsed: ing1.parsed, file, actor: 't' }).ok, true);
  const rows1 = db.getRunState(c.runId).rows;
  const eo = rows1.find((r) => r.match_state === 'excel_only');
  assert.ok(eo && eo.origin === 'excel');
  // 2 つ目の SKU を落とした Excel で再添付 → 記録が無いので削除
  const parsedMinus = JSON.parse(JSON.stringify(ing1.parsed));
  parsedMinus.sheets[0].skuRows = parsedMinus.sheets[0].skuRows.filter((r) => r.fnsku !== eo.fnsku);
  const re1 = db.attachExcelToRun({ runId: c.runId, parsed: parsedMinus, file, actor: 't' });
  assert.equal(re1.ok, true, JSON.stringify(re1));
  assert.equal(re1.groups[0].retired, 1);
  assert.equal(db.getRunState(c.runId).rows.some((r) => r.id === eo.id), false, '記録なしの excel_only 行は消える');
  // もう一度フル Excel を添付 → excel_only が再生成される → 投入して取消 → 落とした Excel で再添付 → retired (FK で残す)
  assert.equal(db.attachExcelToRun({ runId: c.runId, parsed: ing1.parsed, file, actor: 't' }).ok, true);
  const st = db.getRunState(c.runId);
  const eo2 = st.rows.find((r) => r.match_state === 'excel_only');
  const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
  const p = db.addPlacement({ runId: c.runId, rowId: eo2.id, boxId: bx.boxId, qty: 1, worker: member, deviceKey: 'dev:9', requestId: 'eo1' });
  assert.equal(p.ok, true);
  const blocked = db.attachExcelToRun({ runId: c.runId, parsed: parsedMinus, file, actor: 't' });
  assert.equal(blocked.error, 'attach_conflict');
  assert.ok(blocked.conflicts.some((x) => x.kind === 'excel_only_placed'));
  db.revokePlacement({ placementId: p.placementId, byStaff: true, reason: 'x', worker: staff, deviceKey: 'dev:9' });
  const re2 = db.attachExcelToRun({ runId: c.runId, parsed: parsedMinus, file, actor: 't' });
  assert.equal(re2.ok, true, JSON.stringify(re2));
  const retired = db.getRunState(c.runId).rows.find((r) => r.id === eo2.id);
  assert.equal(retired.match_state, 'retired');
  assert.equal(retired.excel_row, null);
  // retired 行への投入 (差し替え直後の古い画面) は拒否
  assert.equal(db.addPlacement({ runId: c.runId, rowId: eo2.id, boxId: bx.boxId, qty: 1, worker: member, deviceKey: 'dev:9', requestId: 'eo3' }).error, 'row_excluded');
  // retired は完了判定から外れる: 行A を 3 入れて閉じれば done にできる
  const rA = db.getRunState(c.runId).rows.find((r) => r.origin === 'picking');
  db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bx.boxId, qty: 3, worker: member, deviceKey: 'dev:9', requestId: 'eo2' });
  db.closeBox({ boxId: bx.boxId, measuredKg: 2, worker: staff });
  assert.equal(db.exportReadiness(c.runId).ok, true, JSON.stringify(db.exportReadiness(c.runId).blockers));
  const pl = db.buildExportPayload(c.runId);
  assert.equal(pl.exports[0].sheets[0].cells.filter((x) => x.kind === 'qty').length, 1, 'retired 行は書かない');
});
t('Excel に無い商品 (picking_only): 添付前の投入は出力ブロック → 取消で解消。添付後は投入・担当・不足の更新を拒否 (row_excluded)。完了判定からは外れる', () => {
  const fake = rowsOf(g1).find((r) => r.match_state === 'picking_only');
  assert.equal(fake.placed, 1);
  assert.ok(db.exportReadiness(pr.runId).blockers.some((b) => b.code === 'picking_only_placed'));
  const bx = db.getRunState(pr.runId).boxes.find((b) => b.pack_group_id === g1);
  // 古い画面からの投入 (競合) は DB 層で拒否 (Codex PR2.5 R2)
  const p = db.addPlacement({ runId: pr.runId, rowId: fake.id, boxId: bx.id, qty: 1, worker: member, deviceKey: 'dev:8', requestId: 'pk2' });
  assert.equal(p.error, 'row_excluded');
  assert.equal(db.setRowShortage({ rowId: fake.id, shortageQty: 1, reason: 'missing', worker: staff }).error, 'row_excluded');
  assert.equal(db.setRowWorkers({ rowId: fake.id, labelWorker: 'x', worker: member }).error, 'row_excluded');
  assert.equal(db.clearRowShortage({ rowId: fake.id, worker: staff }).error, 'row_excluded');
  assert.equal(db.revokePlacement({ placementId: fakePlacement.placementId, byStaff: true, reason: 'Excelに無い', worker: staff, deviceKey: 'dev:8' }).ok, true);
  const rd = db.exportReadiness(pr.runId);
  assert.ok(!rd.blockers.some((b) => b.code === 'picking_only_placed'));
  const inc = rd.blockers.find((b) => b.code === 'rows_incomplete');
  assert.ok(inc && !inc.rows.some((r) => r.id === fake.id));
});
t('再添付 (差し替え): 同じ Excel をもう一度添付しても行の対応は保たれ、グループは新しいファイルを指す', () => {
  const at1b = db.attachExcelToRun({ runId: pr.runId, parsed: ing1.parsed, file: { originalName: 'p1-again.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
  assert.equal(at1b.ok, true, JSON.stringify(at1b));
  assert.equal(at1b.groups[0].groupId, g1);
  assert.equal(at1b.groups[0].matched, 1);
  assert.equal(db.getRunState(pr.runId).groups[0].excel_file_id, at1b.excelFileId);
  assert.equal(db.getRunState(pr.runId).excelFiles.length, 3);
});
// 全行投入 → 閉じる → 出力
{
  const bx1 = db.getRunState(pr.runId).boxes.find((b) => b.pack_group_id === g1);
  const rB = rowsOf(g1).find((r) => r.fnsku === f1rows[1].fnsku);
  db.addPlacement({ runId: pr.runId, rowId: rB.id, boxId: bx1.id, qty: rB.planned_qty, worker: member, deviceKey: 'dev:8', requestId: 'pk3' });
  const bx2 = db.createBox({ packGroupId: g2, materialCode: 'box140', worker: member });
  for (const [i, r] of rowsOf(g2).entries()) {
    db.addPlacement({ runId: pr.runId, rowId: r.id, boxId: bx2.boxId, qty: r.planned_qty, worker: member, deviceKey: 'dev:8', requestId: 'pk4-' + i });
  }
  db.closeBox({ boxId: bx1.id, measuredKg: 4, worker: staff });
  db.closeBox({ boxId: bx2.boxId, measuredKg: 9, worker: staff });
}
const payloadPk = db.buildExportPayload(pr.runId);
const wPk = payloadPk.ok ? await Promise.all(payloadPk.exports.map((ex, i) => xl.writePacklist({ templatePath: ex.excelFile.stored_path, sheets: ex.sheets, fileTag: 'pk' + i }))) : [];
t('全行投入 → 出力は添付ファイルごと (2 出力)。シート名は Excel 側・行は Excel の行番号・両方とも検算を通る', () => {
  assert.equal(payloadPk.ok, true, JSON.stringify(payloadPk).slice(0, 400));
  assert.equal(payloadPk.exports.length, 2);
  const names = payloadPk.exports.map((e) => e.excelFile.original_name).sort();
  assert.deepEqual(names, ['p1-again.xlsx', 'p2.xlsx']);
  for (const ex of payloadPk.exports) {
    assert.equal(ex.sheets.length, 1);
    assert.equal(ex.sheets[0].sheetName, ing1.parsed.sheets[0].sheetName);
    assert.equal(ex.sheets[0].cells[0].kind, 'total_boxes');
    assert.equal(ex.sheets[0].cells[0].value, 1);
  }
  const p1 = payloadPk.exports.find((e) => e.excelFile.original_name === 'p1-again.xlsx');
  assert.equal(p1.sheets[0].cells.filter((c) => c.kind === 'qty').length, 2);   // picking_only は書かない
  assert.ok(wPk.every((w) => w.ok), JSON.stringify(wPk.map((w) => w.error || 'ok')));
  assert.equal(wPk.length, 2);
});
t('複数 Excel の STA アップ済み: 1 ファイルでは納品回は完了しない → 全ファイルで done。同一ファイルの別出力は拒否', () => {
  const rec = db.recordExportBatch({ runId: pr.runId, createdBy: 't', items: payloadPk.exports.map((ex, i) => ({
    excelFileId: ex.excelFile.id, dataVersion: ex.snapshot.dataVersion, fileName: ex.excelFile.original_name, storedPath: wPk[i].outputPath, sha256: wPk[i].sha256, snapshot: ex.snapshot, verify: wPk[i].verify })) });
  assert.equal(rec.ok, true, JSON.stringify(rec));
  assert.equal(rec.exportIds.length, 2);
  assert.equal(rec.stale, false);
  assert.equal(db.recordExportBatch({ runId: pr.runId, createdBy: 't', items: [{ excelFileId: 1, dataVersion: 1 }, { excelFileId: 2, dataVersion: 2 }] }).error, 'version_mismatch');
  const m1 = db.markStaUploaded({ runId: pr.runId, exportId: rec.exportIds[0], actor: 't' });
  assert.equal(m1.ok, true, JSON.stringify(m1));
  assert.equal(m1.runDone, false);
  assert.equal(m1.remaining.length, 1);
  assert.equal(db.getRun(pr.runId).status, 'active');
  assert.equal(db.getRun(pr.runId).sta_uploaded_at, null);
  assert.ok(db.exportReadiness(pr.runId).warnings.some((w) => w.code === 'sta_partial'));
  assert.equal(db.listExports(pr.runId).find((e) => e.id === rec.exportIds[0]).sta_uploaded, 1);
  // 同じファイルの別出力 (同じ版) を記録しようとすると拒否
  const dup = db.recordExport({ runId: pr.runId, excelFileId: payloadPk.exports[0].excelFile.id, dataVersion: payloadPk.exports[0].snapshot.dataVersion, fileName: 'dup.xlsx', storedPath: wPk[0].outputPath, sha256: wPk[0].sha256, snapshot: payloadPk.exports[0].snapshot, verify: null, createdBy: 't' });
  assert.equal(db.markStaUploaded({ runId: pr.runId, exportId: dup.exportId, actor: 't' }).error, 'already_uploaded');
  // アップ済みファイルのグループへの再添付は拒否
  const reat = db.attachExcelToRun({ runId: pr.runId, parsed: payloadPk.exports[0].excelFile.original_name === 'p1-again.xlsx' ? ing1.parsed : ing.parsed, file: { originalName: 'x.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
  assert.equal(reat.error, 'file_uploaded');
  const m2 = db.markStaUploaded({ runId: pr.runId, exportId: rec.exportIds[1], actor: 't' });
  assert.equal(m2.ok, true, JSON.stringify(m2));
  assert.equal(m2.runDone, true);
  assert.equal(db.getRun(pr.runId).status, 'done');
  assert.ok(db.getRun(pr.runId).sta_uploaded_at);
  assert.equal(db.markStaUploaded({ runId: pr.runId, exportId: rec.exportIds[1], actor: 't' }).already, true);
});

console.log('■ 作業を終える (全部入らなくても完了) / 商品画像');
{
  const c = db.createRunFromPicking({ pickingRun: { id: 400, delivery_date: '2026-09-23' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
      { no: 1, sku: 'sku-f1', fnsku: 'X0FIN00001', productName: '入れた商品', qty: '3' },
      { no: 2, sku: 'sku-f2', fnsku: 'X0FIN00002', productName: '破損で入れない商品', qty: '4' },
    ] },
  ], createdBy: 't' });
  const st = db.getRunState(c.runId);
  const gid = st.groups[0].id;
  const rA = st.rows.find((r) => r.plan_no === '通常_1'), rB = st.rows.find((r) => r.plan_no === '通常_2');
  const bx = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });
  const bxEmpty = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });
  db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bx.boxId, qty: 3, worker: member, deviceKey: 'dev:f', requestId: 'fin1' });
  t('finishRun: 中身のある開いた箱があれば open_boxes', () => {
    const r = db.finishRun({ runId: c.runId, worker: staff, deviceLabel: 'iPad' });
    assert.equal(r.error, 'open_boxes');
    assert.deepEqual(r.boxes.map((b) => b.code), ['通常-1']);
  });
  db.closeBox({ boxId: bx.boxId, measuredKg: 2.5, worker: staff });
  t('finishRun: 未投入が残っていれば acknowledge なしは incomplete (アラート用の一覧)。状態は変わらない', () => {
    const r = db.finishRun({ runId: c.runId, worker: staff, deviceLabel: 'iPad' });
    assert.equal(r.error, 'incomplete');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].fnsku, 'X0FIN00002'); assert.equal(r.rows[0].remaining, 4);
    assert.equal(db.getRun(c.runId).status, 'active');
    assert.equal(db.getRunState(c.runId).boxes.find((b) => b.id === bxEmpty.boxId).status, 'open');
  });
  t('finishRun (acknowledge): 残りは「今回は納品しない」の不足で確定・空箱は取消・run は done。出荷前チェックは通る', () => {
    const r = db.finishRun({ runId: c.runId, acknowledge: true, worker: staff, deviceLabel: 'iPad' });
    assert.equal(r.ok, true, JSON.stringify(r));
    assert.equal(r.notShipped, 1);
    assert.deepEqual(r.voidedBoxes, ['通常-2']);
    const st2 = db.getRunState(c.runId);
    assert.equal(st2.run.status, 'done');
    const b = st2.rows.find((x) => x.id === rB.id);
    assert.equal(b.shortage_qty, 4); assert.equal(b.shortage_reason, 'not_shipped');
    assert.equal(st2.boxes.find((x) => x.id === bxEmpty.boxId).status, 'void');
    assert.equal(db.finishRun({ runId: c.runId, acknowledge: true, worker: staff }).already, true);
    // Excel を後から添付しても出力できる (数量 = 入れた分だけ)
    const rd = db.exportReadiness(c.runId);
    assert.ok(!rd.blockers.some((x) => x.code === 'rows_incomplete'), JSON.stringify(rd.blockers));
    assert.ok(rd.warnings.some((x) => x.code === 'shortage_rows'));
    const ev = db.listEvents(50).filter((e) => e.run_id === c.runId).map((e) => e.action);
    assert.ok(ev.includes('run_done') && ev.includes('row_shortage') && ev.includes('box_void'));
  });
  t('finishRun: 既存の不足 (破損 2) に残りを足すとき理由を上書きせず内訳を持つ / 投入超過は over_planned で拒否', () => {
    const c5 = db.createRunFromPicking({ pickingRun: { id: 405, delivery_date: '2026-09-26' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
      { no: 1, fnsku: 'X0MIX00001', productName: '混在', qty: '10' }, { no: 2, fnsku: 'X0MIX00002', productName: '超過', qty: '2' }] }], createdBy: 't' });
    const st = db.getRunState(c5.runId);
    const r1 = st.rows.find((x) => x.fnsku === 'X0MIX00001'), r2 = st.rows.find((x) => x.fnsku === 'X0MIX00002');
    const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c5.runId, rowId: r1.id, boxId: bx.boxId, qty: 5, worker: member, deviceKey: 'dev:m', requestId: 'mx1' });
    db.setRowShortage({ rowId: r1.id, shortageQty: 2, reason: 'damaged', worker: staff });
    db.addPlacement({ runId: c5.runId, rowId: r2.id, boxId: bx.boxId, qty: 2, worker: member, deviceKey: 'dev:m', requestId: 'mx2' });
    db.closeBox({ boxId: bx.boxId, measuredKg: 3, worker: staff });
    // 超過を DB 直接で作る (不変条件違反のシミュレーション)
    db.getDB().prepare('UPDATE fbx_rows SET planned_qty = 1 WHERE id = ?').run(r2.id);
    const over = db.finishRun({ runId: c5.runId, acknowledge: true, worker: staff });
    assert.equal(over.error, 'over_planned'); assert.equal(over.rows[0].fnsku, 'X0MIX00002');
    db.getDB().prepare('UPDATE fbx_rows SET planned_qty = 2 WHERE id = ?').run(r2.id);
    const fin = db.finishRun({ runId: c5.runId, acknowledge: true, worker: staff });
    assert.equal(fin.ok, true, JSON.stringify(fin));
    const row = db.getRunState(c5.runId).rows.find((x) => x.id === r1.id);
    assert.equal(row.shortage_qty, 5);
    assert.equal(row.shortage_reason, 'damaged');
    assert.deepEqual(JSON.parse(row.shortage_detail), [{ reason: 'damaged', qty: 2 }, { reason: 'not_shipped', qty: 3 }]);
    const w = db.exportReadiness(c5.runId).warnings.find((x) => x.code === 'shortage_rows');
    assert.equal(w.rows.find((x) => x.id === r1.id).reasonJa, '破損 2 + 今回は納品しない 3');
  });
  t('done 後に Excel を添付して予定が増減しても不足を再計算して出力できる (Excel 未添付で完了 → 後添付)', () => {
    // fixture1 (A=3, B=3) に対し picking では A=2 (少なめ), B=3 → A を 2 入れて完了 → 添付で A の予定が 3 になる
    const c6 = db.createRunFromPicking({ pickingRun: { id: 406, delivery_date: '2026-09-27' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
      { no: 1, sku: f1rows[0].sku, fnsku: f1rows[0].fnsku, productName: 'A', qty: '2' }, { no: 2, sku: f1rows[1].sku, fnsku: f1rows[1].fnsku, productName: 'B', qty: '3' }] }], createdBy: 't' });
    const st = db.getRunState(c6.runId);
    const rA = st.rows.find((x) => x.fnsku === f1rows[0].fnsku), rB = st.rows.find((x) => x.fnsku === f1rows[1].fnsku);
    const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c6.runId, rowId: rA.id, boxId: bx.boxId, qty: 2, worker: member, deviceKey: 'dev:d', requestId: 'dn1' });
    db.addPlacement({ runId: c6.runId, rowId: rB.id, boxId: bx.boxId, qty: 1, worker: member, deviceKey: 'dev:d', requestId: 'dn2' });
    db.closeBox({ boxId: bx.boxId, measuredKg: 2, worker: staff });
    assert.equal(db.finishRun({ runId: c6.runId, acknowledge: true, worker: staff }).ok, true);   // B は残り 2 → not_shipped 2
    const at = db.attachExcelToRun({ runId: c6.runId, parsed: ing1.parsed, file: { originalName: 'late.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
    assert.equal(at.ok, true, JSON.stringify(at));
    assert.ok(at.warnings.some((w) => w.kind === 'shortage_recomputed' && w.fnsku === f1rows[0].fnsku && w.shortageTo === 1), JSON.stringify(at.warnings));
    const rows = db.getRunState(c6.runId).rows;
    assert.equal(rows.find((x) => x.id === rA.id).shortage_qty, 1);           // 予定 3 - 投入 2
    assert.equal(rows.find((x) => x.id === rB.id).shortage_qty, 2);           // 変わらず
    const rd = db.exportReadiness(c6.runId);
    assert.equal(rd.ok, true, JSON.stringify(rd.blockers));
    assert.equal(db.buildExportPayload(c6.runId).ok, true);
  });
  t('shortageBreakdownFor: 増分は not_shipped へ、減分は末尾から削る、1 件なら detail は無し', () => {
    const a = db.shortageBreakdownFor({ shortage: 2, reason: 'damaged', detail: null }, 5);
    assert.equal(a.reason, 'damaged'); assert.deepEqual(JSON.parse(a.detail), [{ reason: 'damaged', qty: 2 }, { reason: 'not_shipped', qty: 3 }]);
    const b = db.shortageBreakdownFor({ shortage: 5, reason: 'damaged', detail: a.detail }, 6);
    assert.deepEqual(JSON.parse(b.detail), [{ reason: 'damaged', qty: 2 }, { reason: 'not_shipped', qty: 4 }]);
    const c = db.shortageBreakdownFor({ shortage: 5, reason: 'damaged', detail: a.detail }, 1);
    assert.equal(c.reason, 'damaged'); assert.equal(c.detail, null);
    const z = db.shortageBreakdownFor({ shortage: 5, reason: 'damaged', detail: a.detail }, 0);
    assert.equal(z.reason, null); assert.equal(z.detail, null);
    const n = db.shortageBreakdownFor({ shortage: 0, reason: null, detail: null }, 3);
    assert.equal(n.reason, 'not_shipped'); assert.equal(n.detail, null);
  });
  t('done 後の添付で予定が減る: 投入 ≤ 予定なら拒否せず不足を縮める (内訳も末尾から)。作業中の回でも不足が予定を超えれば縮める', () => {
    // fixture2 の 4 SKU。picking では 4 つ目 (Excel 30) を 40 にし、5 だけ入れて完了 → 不足 35 (not_shipped) → 添付で予定 30 → 不足 25
    const rowsPk = f2rows.map((r, i) => ({ no: i + 1, sku: r.sku, fnsku: r.fnsku, productName: 'p' + i, qty: String(i === 3 ? 40 : r.plannedQty) }));
    const c7 = db.createRunFromPicking({ pickingRun: { id: 407, delivery_date: '2026-09-28' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: rowsPk }], createdBy: 't' });
    const st = db.getRunState(c7.runId);
    const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
    for (const [i, r] of st.rows.entries()) {
      db.addPlacement({ runId: c7.runId, rowId: r.id, boxId: bx.boxId, qty: i === 3 ? 5 : r.planned_qty, worker: member, deviceKey: 'dev:e', requestId: 'dec' + i });
    }
    const r4 = st.rows[3];
    db.setRowShortage({ rowId: r4.id, shortageQty: 2, reason: 'damaged', worker: staff });   // 破損 2 を先に
    db.closeBox({ boxId: bx.boxId, measuredKg: 4, worker: staff });
    assert.equal(db.finishRun({ runId: c7.runId, acknowledge: true, worker: staff }).ok, true);   // 残り 33 → 破損 2 + not_shipped 33 = 35
    assert.equal(db.getRunState(c7.runId).rows.find((x) => x.id === r4.id).shortage_qty, 35);
    const at = db.attachExcelToRun({ runId: c7.runId, parsed: ing.parsed, file: { originalName: 'dec.xlsx', storedPath: ing.storedPath, sha256: ing.sha256 }, actor: 't' });
    assert.equal(at.ok, true, JSON.stringify(at));
    const row = db.getRunState(c7.runId).rows.find((x) => x.id === r4.id);
    assert.equal(row.planned_qty, 30); assert.equal(row.shortage_qty, 25);
    assert.deepEqual(JSON.parse(row.shortage_detail), [{ reason: 'damaged', qty: 2 }, { reason: 'not_shipped', qty: 23 }]);
    assert.equal(db.exportReadiness(c7.runId).ok, true, JSON.stringify(db.exportReadiness(c7.runId).blockers));
    // 作業中の回: 予定 40 (picking) → 送る数 10 (不足 30) にしてから Excel (予定 30) を添付 → 不足は 30 → 25 に縮む (投入 5)
    const c8 = db.createRunFromPicking({ pickingRun: { id: 408, delivery_date: '2026-09-29' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: rowsPk }], createdBy: 't' });
    const st8 = db.getRunState(c8.runId);
    const bx8 = db.createBox({ packGroupId: st8.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c8.runId, rowId: st8.rows[3].id, boxId: bx8.boxId, qty: 5, worker: member, deviceKey: 'dev:e', requestId: 'act1' });
    assert.equal(db.setRowSendQty({ rowId: st8.rows[3].id, sendQty: 10, worker: staff }).ok, true);   // 不足 30
    const at8 = db.attachExcelToRun({ runId: c8.runId, parsed: ing.parsed, file: { originalName: 'act.xlsx', storedPath: ing.storedPath, sha256: ing.sha256 }, actor: 't' });
    assert.equal(at8.ok, true, JSON.stringify(at8));
    const row8 = db.getRunState(c8.runId).rows.find((x) => x.id === st8.rows[3].id);
    assert.equal(row8.planned_qty, 30); assert.equal(row8.shortage_qty, 25); assert.equal(row8.shortage_reason, 'stock_short');
    assert.equal(db.getRun(c8.runId).status, 'active');
  });
  t('setRowShortage: 理由 not_shipped が使える', () => {
    const c2 = db.createRunFromPicking({ pickingRun: { id: 401 }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, fnsku: 'X0FIN00003', productName: 'x', qty: '2' }] }], createdBy: 't' });
    const row = db.getRunState(c2.runId).rows[0];
    assert.equal(db.setRowShortage({ rowId: row.id, shortageQty: 2, reason: 'not_shipped', worker: staff }).ok, true);
  });
  t('setRowSendQty: 予定 30 → 送る数 25 = 不足 5 (在庫が少ない)。予定超・投入未満は拒否。予定に戻すと不足が消える。readiness に修正前→後', () => {
    const c4 = db.createRunFromPicking({ pickingRun: { id: 402, delivery_date: '2026-09-24' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 's', fnsku: 'X0SEND0001', productName: '在庫少', qty: '30' }] }], createdBy: 't' });
    const st = db.getRunState(c4.runId);
    const row = st.rows[0];
    const bx = db.createBox({ packGroupId: st.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c4.runId, rowId: row.id, boxId: bx.boxId, qty: 20, worker: member, deviceKey: 'dev:s', requestId: 'sq1' });
    assert.equal(db.setRowSendQty({ rowId: row.id, sendQty: 31, worker: staff }).error, 'bad_qty');
    assert.equal(db.setRowSendQty({ rowId: row.id, sendQty: 19, worker: staff }).error, 'bad_qty');
    assert.equal(db.setRowSendQty({ rowId: row.id, sendQty: 25, reason: 'nope', worker: staff }).error, 'bad_reason');
    const r = db.setRowSendQty({ rowId: row.id, sendQty: 25, worker: staff, deviceLabel: 'iPad' });
    assert.equal(r.ok, true, JSON.stringify(r));
    assert.equal(r.shortage, 5); assert.equal(r.from, 30);
    const after = db.getRunState(c4.runId).rows[0];
    assert.equal(after.shortage_qty, 5); assert.equal(after.shortage_reason, 'stock_short');
    // 残り = 25 - 20 = 5 → 5 入れれば完了できる
    assert.equal(db.addPlacement({ runId: c4.runId, rowId: row.id, boxId: bx.boxId, qty: 6, worker: member, deviceKey: 'dev:s', requestId: 'sq2' }).error, 'over_qty');
    assert.equal(db.addPlacement({ runId: c4.runId, rowId: row.id, boxId: bx.boxId, qty: 5, worker: member, deviceKey: 'dev:s', requestId: 'sq3' }).ok, true);
    const w = db.exportReadiness(c4.runId).warnings.find((x) => x.code === 'shortage_rows');
    assert.ok(w && w.rows[0].planned === 30 && w.rows[0].sendQty === 25 && w.rows[0].reasonJa === '在庫が少ない', JSON.stringify(w));
    assert.ok(db.listEvents(20).some((e) => e.action === 'row_send_qty'));
    // 投入 25 のまま予定 (30) に戻す → 不足が消え、残り 5 になる
    assert.equal(db.setRowSendQty({ rowId: row.id, sendQty: 30, worker: staff }).ok, true);
    assert.equal(db.getRunState(c4.runId).rows[0].shortage_qty, null);
  });
  // 商品画像: キャッシュ + images.js (fetcher / 属性源を差し替え)
  const img = await import('../apps/fba-box/images.js');
  t('listRowsNeedingCatalog / upsertProductImage / getRunState の image_url / URL 検証', () => {
    const need = db.listRowsNeedingCatalog(c.runId);
    assert.deepEqual(need.map((x) => x.fnsku).sort(), ['X0FIN00001', 'X0FIN00002']);
    db.upsertProductImage({ fnsku: 'X0FIN00001', asin: 'B0TEST0001', url: 'https://m.media-amazon.com/images/I/test.jpg', status: 'ok' });
    // PR3: 画像が入っても参考単重がまだなら、その商品はまだ取得対象 (1回の呼び出しで両方埋める)
    assert.deepEqual(db.listRowsNeedingCatalog(c.runId).map((x) => x.fnsku).sort(), ['X0FIN00001', 'X0FIN00002']);
    db.upsertWeightRef({ fnsku: 'X0FIN00001', asin: 'B0TEST0001', weightG: 250, raw: '0.25', status: 'ok' });
    assert.deepEqual(db.listRowsNeedingCatalog(c.runId).map((x) => x.fnsku), ['X0FIN00002']);
    assert.equal(db.getRunState(c.runId).rows.find((r) => r.fnsku === 'X0FIN00001').image_url, 'https://m.media-amazon.com/images/I/test.jpg');
    db.upsertProductImage({ fnsku: 'X0FIN00002', asin: null, url: null, status: 'error' });
    db.upsertWeightRef({ fnsku: 'X0FIN00002', asin: null, weightG: null, status: 'error' });
    assert.deepEqual(db.listRowsNeedingCatalog(c.runId), [], 'error は翌日まで再試行しない');
    assert.deepEqual(db.listRowsNeedingCatalog(c.runId, { retryAfterMs: 0 }).map((x) => x.fnsku), ['X0FIN00002']);
    // 行の asin がキャッシュの asin と違えば (Excel 差し替えで別商品) 取り直す
    db.getDB().prepare('UPDATE fbx_rows SET asin = ? WHERE fnsku = ?').run('B0OTHER001', 'X0FIN00001');
    assert.deepEqual(db.listRowsNeedingCatalog(c.runId).map((x) => x.fnsku), ['X0FIN00001']);
    db.getDB().prepare('UPDATE fbx_rows SET asin = NULL WHERE fnsku = ?').run('X0FIN00001');
    // miniPC の実物の応答は { ok, result: { image } }。包み方とキー名の取り違えで全商品「画像なし」になった 2026-09-03 の再発防止
    assert.equal(img.pickImageUrl({ ok: true, result: { asin: 'B0', image: 'https://m.media-amazon.com/images/I/r.jpg' } }), 'https://m.media-amazon.com/images/I/r.jpg');
    assert.equal(img.pickImageUrl({ ok: true, result: { asin: 'B0', image: '' } }), null);
    assert.equal(img.pickImageUrl({ ok: true, asin: 'B0', image: 'https://m.media-amazon.com/images/I/x.jpg' }), 'https://m.media-amazon.com/images/I/x.jpg');
    assert.equal(img.pickImageUrl({ ok: true, mainImage: 'https://m.media-amazon.com/images/I/y.jpg' }), 'https://m.media-amazon.com/images/I/y.jpg');
    assert.equal(img.pickImageUrl({ ok: true, image: '' }), null);
    assert.equal(img.pickImageUrl(null), null);
    assert.equal(img.sanitizeImageUrl('https://m.media-amazon.com/images/I/a.jpg'), 'https://m.media-amazon.com/images/I/a.jpg');
    assert.equal(img.sanitizeImageUrl('http://m.media-amazon.com/images/I/a.jpg'), null);
    assert.equal(img.sanitizeImageUrl('https://evil.example.com/a.jpg'), null);
    assert.equal(img.sanitizeImageUrl('javascript:alert(1)'), null);
  });
  const calls = [];
  img._setAttrsSource(async () => [{ amazon_sku: 'sku-f2', asin: 'B0TEST0002', fnsku: 'X0FIN00002' }, { amazon_sku: 'sku-x', asin: 'B0TEST0003', fnsku: 'X0FIN00003' }]);
  // 応答は miniPC の実物と同じ形 { ok, result: { image, dimensions: { weight: kg文字列 } } }
  img._setFetcher(async (asin) => {
    calls.push(asin);
    if (asin === 'B0TEST0003') throw new Error('boom');
    return asin === 'B0TEST0002'
      ? { ok: true, result: { image: 'https://m.media-amazon.com/images/I/2.jpg', dimensions: { weight: '0.03' } } }
      : { ok: true, result: { image: null, dimensions: { weight: '-' } } };
  });
  db.upsertProductImage({ fnsku: 'X0FIN00002', asin: null, url: null, status: 'error' });
  db.getDB().prepare(`UPDATE fbx_product_images SET fetched_at = '2020-01-01T00:00:00.000Z' WHERE fnsku = 'X0FIN00002'`).run();
  process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';   // configured 扱い (fetcher は差し替え済みなので外には出ない)
  const notConf0 = process.env.WAREHOUSE_SERVICE_TOKEN;
  const res1 = await img.ensureRunCatalog(c.runId, { force: true });
  const again = await img.ensureRunCatalog(c.runId);
  t('ensureRunCatalog: FNSKU/SKU → ASIN を引いて画像と参考単重を1回で取得。スロットルで連続実行は skip', () => {
    assert.ok(notConf0);
    assert.equal(res1.total, 1, JSON.stringify(res1));
    assert.equal(res1.fetched, 1);
    assert.equal(res1.weighed, 1, '同じ応答から単重も取る');
    assert.equal(db.getRunState(c.runId).weights.X0FIN00002.unitG, 30, '0.03kg → 30g');
    assert.equal(db.getRunState(c.runId).weights.X0FIN00002.source, 'catalog');
    assert.deepEqual(calls, ['B0TEST0002'], '画像と単重で2回叩かない');
    assert.equal(db.getRunState(c.runId).rows.find((r) => r.fnsku === 'X0FIN00002').image_url, 'https://m.media-amazon.com/images/I/2.jpg');
    assert.equal(again.skipped, 'throttled');
  });
  const c3 = db.getRunBySource(401);
  const res3 = await img.ensureRunCatalog(c3.id, { force: true });
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  const notConf = await img.ensureRunCatalog(c3.id);
  t('ensureRunCatalog: 取得失敗は error として記録 (作業は止めない)。未設定なら skip', () => {
    assert.equal(res3.failed, 1, JSON.stringify(res3));
    assert.equal(db.getDB().prepare(`SELECT status FROM fbx_product_images WHERE fnsku = 'X0FIN00003'`).get().status, 'error');
    assert.equal(db.getDB().prepare(`SELECT status FROM fbx_weight_refs WHERE fnsku = 'X0FIN00003'`).get().status, 'error');
    assert.equal(notConf.skipped, 'not_configured');
  });
  // 「今すぐ取り直す」を取得中に押しても in_flight で弾かれず、終わるのを待ってから実行する (9/3 実機で in_flight 表示)
  img._resetImageState();
  process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';
  db.getDB().prepare(`DELETE FROM fbx_product_images WHERE fnsku IN ('X0FIN00001','X0FIN00002')`).run();
  img._setAttrsSource(async () => [{ amazon_sku: 'sku-f1', asin: 'B0SLOW0001', fnsku: 'X0FIN00001' }, { amazon_sku: 'sku-f2', asin: 'B0SLOW0002', fnsku: 'X0FIN00002' }]);
  let slowCalls = 0;
  img._setFetcher(async () => { slowCalls++; await new Promise((r) => setTimeout(r, 200)); return { ok: true, result: { image: 'https://m.media-amazon.com/images/I/slow.jpg', dimensions: { weight: '0.10' } } }; });
  const [r1, r2] = await Promise.all([img.ensureRunCatalog(c.runId, { force: true }), img.ensureRunCatalog(c.runId, { force: true })]);
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  t('ensureRunCatalog: 取得中に「今すぐ取り直す」を押しても in_flight で弾かず、待ってから実行する', () => {
    assert.equal(r1.skipped, undefined, JSON.stringify(r1));
    assert.equal(r2.skipped, undefined, JSON.stringify(r2));
    assert.equal(r1.fetched + r2.fetched, 2, '2 商品を取得 (二重取得しない)');
    assert.equal(slowCalls, 2);
    assert.equal(db.getRunState(c.runId).rows.filter((x) => x.image_url).length >= 1, true);
  });
  img._resetImageState();
}

console.log('■ PR3: 重量補助 (参考単重・実測・推定・上限)');
{
  const imgW = await import('../apps/fba-box/images.js');
  t('pickPackageWeightG: miniPC の kg 文字列 → 1個あたり g。"-"・0・大きすぎる値は「単重なし」', () => {
    // 実物の応答 (2026-09-06 miniPC で確認): dimensions.weight はポンド由来の kg 文字列 (小数2桁)
    assert.equal(imgW.pickPackageWeightG({ ok: true, result: { dimensions: { weight: '0.02' } } }).g, 20);
    assert.equal(imgW.pickPackageWeightG({ ok: true, result: { dimensions: { weight: '1.5' } } }).g, 1500);
    assert.equal(imgW.pickPackageWeightG({ dimensions: { weight: '0.25' } }).g, 250, 'result の入れ子でなくても読む');
    assert.equal(imgW.pickPackageWeightG({ result: { dimensions: { weight: '-' } } }).g, null);
    assert.equal(imgW.pickPackageWeightG({ result: { dimensions: { weight: '0' } } }).g, null);
    assert.equal(imgW.pickPackageWeightG({ result: { dimensions: { weight: '250' } } }).g, null, '1個250kg = 単位取り違えの保険');
    assert.equal(imgW.pickPackageWeightG({ result: {} }).g, null);
    assert.equal(imgW.pickPackageWeightG(null).g, null);
  });

  const mkSheets = (rows) => [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows }];
  const c = db.createRunFromPicking({ pickingRun: { id: 500, delivery_date: '2026-10-01' }, planSheets: mkSheets([
    { no: 1, sku: 'sku-w1', fnsku: 'X0WGT00001', productName: '重さのわかる商品', qty: '25' },
    { no: 2, sku: 'sku-w2', fnsku: 'X0WGT00002', productName: '重さ不明の商品', qty: '4' },
  ]), createdBy: 't' });
  const st0 = db.getRunState(c.runId);
  const gid = st0.groups[0].id;
  const rA = st0.rows.find((r) => r.fnsku === 'X0WGT00001'), rB = st0.rows.find((r) => r.fnsku === 'X0WGT00002');
  const estOf = (boxId) => db.getRunState(c.runId).boxes.find((b) => b.id === boxId).est;

  t('納品回の開始時に重量ルールを焼き付ける (目標28kg / 上限30kg)', () => {
    const run = db.getRun(c.runId);
    assert.equal(run.weight_target_g, 28000);
    assert.equal(run.weight_limit_g, 30000);
    assert.deepEqual(db.getRunState(c.runId).weightLimits, { targetG: 28000, limitG: 30000, snapshotted: true });
  });

  t('参考単重 → 採用値 (catalog)。実測「10個で2050g」を入れると実測が勝ち、取り消すと参考値に戻る', () => {
    db.upsertWeightRef({ fnsku: 'X0WGT00001', asin: 'B0W1', weightG: 200, raw: '0.20', status: 'ok' });
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.unitG, 200);
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.source, 'catalog');
    const m = db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 10, totalG: 2050, worker: member, deviceLabel: 'iPad', runId: c.runId });
    assert.equal(m.ok, true);
    assert.equal(m.unitG, 205, 'まとめて量って個数で割る');
    let cur = db.getRunState(c.runId).weights.X0WGT00001;
    assert.equal(cur.unitG, 205);
    assert.equal(cur.source, 'measured');
    assert.equal(cur.sampleQty, 10);
    assert.equal(db.revokeWeightMeasurement({ id: m.id, worker: staff }).ok, true);
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.unitG, 200, '取り消したら参考値に戻る');
    assert.equal(db.revokeWeightMeasurement({ id: m.id, worker: staff }).error, 'already_revoked');
    assert.equal(db.listWeightMeasurements('X0WGT00001').length, 1, '取消も履歴には残す (逆算分析の生データ)');
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 0, totalG: 100, worker: member }).error, 'bad_qty');
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 5, totalG: -1, worker: member }).error, 'bad_weight');
    assert.equal(db.addWeightMeasurement({ fnsku: '', sampleQty: 5, totalG: 100, worker: member }).error, 'bad_fnsku');
  });

  const bx = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });   // 自重 900g
  db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bx.boxId, qty: 10, worker: member, deviceKey: 'dev:w', requestId: 'w1' });

  t('箱の推定 = Σ(数量×採用単重) + 資材の自重。単重不明の商品は足さず欠損数で返す', () => {
    let e = estOf(bx.boxId);
    assert.equal(e.estG, 2900, '10個×200g + 箱900g');
    assert.equal(e.unknownQty, 0);
    assert.equal(e.tareKnown, true);
    assert.equal(e.complete, true, '「あと約N個」を出してよい状態');
    db.addPlacement({ runId: c.runId, rowId: rB.id, boxId: bx.boxId, qty: 4, worker: member, deviceKey: 'dev:w', requestId: 'w2' });
    e = estOf(bx.boxId);
    assert.equal(e.estG, 2900, '単重不明の商品は推定に足さない');
    assert.equal(e.unknownQty, 4, '欠損数を必ずセットで返す');
    assert.equal(e.complete, false);
    assert.equal(e.unknownItems[0].fnsku, 'X0WGT00002');
  });

  t('資材の自重が未登録の箱は tareKnown=false (推定を鵜呑みにさせない)', () => {
    const bx2 = db.createBox({ packGroupId: gid, materialCode: 'other', worker: member });   // tare_g NULL
    db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bx2.boxId, qty: 5, worker: member, deviceKey: 'dev:w', requestId: 'w3' });
    const e = estOf(bx2.boxId);
    assert.equal(e.estG, 1000, '中身だけ (5個×200g)');
    assert.equal(e.tareKnown, false);
    assert.equal(e.complete, false);
  });

  t('上限30kg超えは作業者だけでは閉じられない → 職員の承認で閉じられる。閉じた時点の推定を残す', () => {
    const ng = db.closeBox({ boxId: bx.boxId, measuredKg: 31.2, worker: member, deviceLabel: 'iPad' });
    assert.equal(ng.error, 'over_limit');
    assert.equal(ng.limitKg, 30);
    assert.equal(db.getBox(bx.boxId).status, 'open', '断ったときは閉じない');
    const ok = db.closeBox({ boxId: bx.boxId, measuredKg: 31.2, worker: member, deviceLabel: 'iPad', staffApproved: true, approvedBy: '職員A' });
    assert.equal(ok.ok, true);
    assert.equal(ok.overLimit, true);
    assert.equal(ok.overTarget, true);
    assert.equal(ok.hint, null, '単重不明があるうちは乖離ヒントを出さない');
    const b = db.getBox(bx.boxId);
    assert.equal(b.est_weight_g_at_close, 2900);
    assert.equal(b.est_unknown_qty_at_close, 4);
    assert.equal(b.tare_g_at_close, 900);
    assert.equal(b.limit_override_by, '職員A', '承認者は箱そのものにも残す (出荷前チェックで探せるように)');
    assert.ok(b.limit_override_at);
    const ev = db.listEvents(30, c.runId).find((e) => e.action === 'box_close');
    assert.equal(JSON.parse(ev.payload).overLimit.approvedBy, '職員A');
  });

  t('実測が推定と大きく違うと乖離ヒント (500g以上 かつ 5%以上)。近ければ黙る', () => {
    const bxOk = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bxOk.boxId, qty: 5, worker: member, deviceKey: 'dev:w', requestId: 'w4' });
    assert.equal(estOf(bxOk.boxId).estG, 1900);
    const near = db.closeBox({ boxId: bxOk.boxId, measuredKg: 2.0, worker: member });   // 差 100g
    assert.equal(near.ok, true);
    assert.equal(near.hint, null);
    const bxNg = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bxNg.boxId, qty: 5, worker: member, deviceKey: 'dev:w', requestId: 'w5' });
    const far = db.closeBox({ boxId: bxNg.boxId, measuredKg: 5, worker: member });      // 差 3100g
    assert.equal(far.ok, true);
    assert.ok(far.hint, '数量か単重が怪しいと知らせる');
    assert.equal(far.hint.estG, 1900);
    assert.ok(far.hint.message.includes('3.1kg'));
  });

  t('出荷前チェック: 上限を超えて閉じた箱は警告に出る (Amazon 側で受入不可・追加料金の可能性)', () => {
    const rd = db.exportReadiness(c.runId);
    const w = rd.warnings.find((x) => x.code === 'over_weight_limit');
    assert.ok(w, JSON.stringify(rd.warnings.map((x) => x.code)));
    assert.equal(w.boxes.length, 1);
    assert.equal(w.boxes[0].weightKg, 31.2);
    assert.equal(w.boxes[0].approvedBy, '職員A');
  });

  t('ルールの変更は作業中の回には効かない (開始時のスナップショット)。目標>上限は拒否', () => {
    assert.equal(db.setWeightRules({ targetG: 30000, limitG: 20000, actor: 'admin' }).error, 'bad_value');
    assert.equal(db.setWeightRules({ targetG: 20000, limitG: 22000, actor: 'admin' }).ok, true);
    assert.equal(db.getRun(c.runId).weight_limit_g, 30000, '作業中の回は動かない');
    assert.equal(db.closeBox({ boxId: db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member }).boxId, measuredKg: 25, worker: member }).error, 'empty_box');
    const c2 = db.createRunFromPicking({ pickingRun: { id: 501, delivery_date: '2026-10-02' }, planSheets: mkSheets([
      { no: 1, sku: 'sku-w9', fnsku: 'X0WGT00009', productName: '新しい回の商品', qty: '2' },
    ]), createdBy: 't' });
    assert.equal(db.getRun(c2.runId).weight_limit_g, 22000, 'これから始める回は新しいルール');
    db.setWeightRules({ targetG: 28000, limitG: 30000, actor: 'admin' });
    assert.equal(db.getWeightRules().limit_g, 30000);
  });

  t('実測の登録は「作業中の納品回に実在する商品」だけ受ける (Codex PR3 #6: 打ち間違いを全回のマスタに入れない)', () => {
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 1, totalG: 10, worker: member }).error, 'run_required');
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 1, totalG: 10, runId: 999999, worker: member }).error, 'run_required');
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0NOTHERE1', sampleQty: 1, totalG: 10, runId: c.runId, worker: member }).error, 'not_in_run');
    const doneRun = db.getRunBySource(400);
    assert.equal(db.addWeightMeasurement({ fnsku: 'X0FIN00001', sampleQty: 1, totalG: 10, runId: doneRun.id, worker: member }).error, 'run_not_active');
  });

  t('既に始まっている納品回にも、デプロイ時のマイグレーションでルールを焼き付ける (Codex PR3 #2)', () => {
    db.getDB().prepare('UPDATE fbx_runs SET weight_target_g = NULL, weight_limit_g = NULL WHERE id = ?').run(c.runId);
    assert.equal(db.getRun(c.runId).weight_limit_g, null);
    db.createTables();   // = デプロイ後の起動
    const run = db.getRun(c.runId);
    assert.equal(run.weight_target_g, 28000);
    assert.equal(run.weight_limit_g, 30000);
  });

  t('listRunWeights: 商品ごとの採用値・参考値・実測件数 (本社が「不明が何点か」を見る)', () => {
    const list = db.listRunWeights(c.runId);
    const a = list.find((x) => x.fnsku === 'X0WGT00001'), b = list.find((x) => x.fnsku === 'X0WGT00002');
    assert.equal(a.unit_g, 200);
    assert.equal(a.source, 'catalog');
    assert.equal(a.ref_g, 200);
    assert.equal(a.meas_count, 0, '取り消した実測は数えない');
    assert.equal(b.unit_g, null, '単重不明');
  });

  t('参考値が未取得なら、実測があってもカタログ取得の対象に残す (Codex PR3 #4: 実測を取り消したとき単重不明に落とさない)', () => {
    db.upsertProductImage({ fnsku: 'X0WGT00002', asin: 'B0W2', url: 'https://m.media-amazon.com/images/I/w2.jpg', status: 'ok' });
    const m = db.addWeightMeasurement({ fnsku: 'X0WGT00002', sampleQty: 2, totalG: 100, runId: c.runId, worker: member });
    assert.equal(m.ok, true);
    assert.equal(db.getRunState(c.runId).weights.X0WGT00002.unitG, 50);
    assert.ok(db.listRowsNeedingCatalog(c.runId).some((x) => x.fnsku === 'X0WGT00002'),
      '画像あり + 実測あり でも参考値が無ければ取りに行く');
    db.revokeWeightMeasurement({ id: m.id, worker: staff });
  });

  t('rebuildWeightCurrent: 採用値が壊れていても起動時に作り直す (Codex PR3 #5)', () => {
    db.getDB().prepare(`UPDATE fbx_weight_current SET unit_g = 99999, source = 'catalog' WHERE fnsku = 'X0WGT00001'`).run();
    db.getDB().prepare(`DELETE FROM fbx_weight_current WHERE fnsku = 'X0WGT00002'`).run();
    const n = db.rebuildWeightCurrent();
    assert.ok(n >= 2);
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.unitG, 200, '参考値から作り直す');
    assert.equal(db.getRunState(c.runId).weights.X0WGT00002, undefined, '元データが無い商品は採用値も持たない');
  });
}

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

console.log('■ PR2.5: fbx_pack_groups / fbx_rows の Excel 後付け移行');
const mdb2 = new Database(path.join(tmp, 'migrate2.db'));
mdb2.pragma('journal_mode = WAL'); mdb2.pragma('foreign_keys = ON');
db.createTables(mdb2);
mdb2.pragma('foreign_keys = OFF');
mdb2.exec(`DROP TABLE fbx_rows; DROP TABLE fbx_pack_groups;
  CREATE TABLE fbx_pack_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL REFERENCES fbx_runs(id),
    excel_file_id INTEGER NOT NULL REFERENCES fbx_excel_files(id), sheet_name TEXT NOT NULL, packing_group_id TEXT NOT NULL,
    display_name TEXT NOT NULL, box_count_hint INTEGER, max_box_columns INTEGER, structure_json TEXT, UNIQUE(run_id, packing_group_id));
  CREATE TABLE fbx_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL REFERENCES fbx_runs(id),
    pack_group_id INTEGER NOT NULL REFERENCES fbx_pack_groups(id), excel_row INTEGER NOT NULL, seller_sku TEXT NOT NULL, asin TEXT,
    fnsku TEXT NOT NULL, excel_id TEXT, product_name TEXT, planned_qty INTEGER NOT NULL CHECK (planned_qty >= 0), plan_no TEXT,
    source_slot_id TEXT, picking_row_no INTEGER, picking_qty INTEGER,
    match_state TEXT NOT NULL DEFAULT 'matched' CHECK (match_state IN ('matched','qty_mismatch','excel_only')),
    requires_expiry INTEGER CHECK (requires_expiry IN (0,1)), UNIQUE(pack_group_id, excel_row));
  CREATE INDEX IF NOT EXISTS idx_fbx_rows_run ON fbx_rows(run_id);`);
mdb2.pragma('foreign_keys = ON');
mdb2.exec(`INSERT INTO fbx_runs (id, source_run_id, title, status, created_at) VALUES (1, 1, 'r', 'active', 'now');
  INSERT INTO fbx_excel_files (id, run_id, stored_path, sha256, fingerprint, uploaded_at) VALUES (1, 1, '/x', 'h', 'f', 'now');
  INSERT INTO fbx_pack_groups (id, run_id, excel_file_id, sheet_name, packing_group_id, display_name, max_box_columns) VALUES (1, 1, 1, '輸送箱の梱包情報', 'pg1', 'G1', 15);
  INSERT INTO fbx_rows (id, run_id, pack_group_id, excel_row, seller_sku, fnsku, planned_qty, match_state) VALUES (1, 1, 1, 6, 'sku', 'X1', 5, 'qty_mismatch');
  INSERT INTO fbx_boxes (id, pack_group_id, box_no, box_code, created_at) VALUES (1, 1, 1, 'G1-B1', 'now');
  INSERT INTO fbx_placements (run_id, row_id, box_id, qty, box_seq, device_key, request_id, created_at) VALUES (1, 1, 1, 5, 1, 'd', 'r', 'now');`);
t('PR2 スキーマの pack_groups / rows を再構築: NULL 許容・pending/picking_only・excel_sheet_name 補完・FK 健全・冪等', () => {
  db.createTables(mdb2);
  const gcols = new Set(mdb2.prepare('PRAGMA table_info(fbx_pack_groups)').all().map((c) => c.name));
  assert.ok(gcols.has('source_slot_id') && gcols.has('excel_sheet_name'));
  assert.equal(mdb2.prepare('PRAGMA table_info(fbx_pack_groups)').all().find((c) => c.name === 'excel_file_id').notnull, 0);
  assert.ok(mdb2.prepare(`SELECT sql FROM sqlite_master WHERE name = 'fbx_rows'`).get().sql.includes("'pending'"));
  const g = mdb2.prepare('SELECT * FROM fbx_pack_groups WHERE id = 1').get();
  assert.equal(g.excel_sheet_name, '輸送箱の梱包情報'); assert.equal(g.excel_file_id, 1); assert.equal(g.max_box_columns, 15);
  const r = mdb2.prepare('SELECT * FROM fbx_rows WHERE id = 1').get();
  assert.equal(r.match_state, 'qty_mismatch'); assert.equal(r.excel_row, 6); assert.equal(r.origin, 'excel');
  assert.ok(mdb2.prepare(`SELECT sql FROM sqlite_master WHERE name = 'fbx_rows'`).get().sql.includes("'retired'"));
  assert.ok(new Set(mdb2.prepare('PRAGMA table_info(fbx_exports)').all().map((c) => c.name)).has('sta_uploaded_at'));
  assert.deepEqual(mdb2.prepare('PRAGMA foreign_key_check').all(), []);
  assert.equal(mdb2.prepare(`SELECT COUNT(*) c FROM sqlite_master WHERE name IN ('fbx_rows_new','fbx_pack_groups_new')`).get().c, 0);
  assert.equal(mdb2.prepare(`SELECT COUNT(*) c FROM sqlite_master WHERE type = 'index' AND name = 'idx_fbx_rows_run'`).get().c, 1);
  db.createTables(mdb2);   // 2回目は何もしない
  // 新スキーマで Excel なしの行が入る
  mdb2.prepare(`INSERT INTO fbx_pack_groups (run_id, sheet_name, display_name, source_slot_id) VALUES (1, 'P1_通常', '通常', 'p1_normal')`).run();
  mdb2.prepare(`INSERT INTO fbx_rows (run_id, pack_group_id, fnsku, planned_qty, match_state) VALUES (1, 2, 'X2', 3, 'pending')`).run();
  assert.equal(mdb2.prepare('SELECT COUNT(*) c FROM fbx_rows').get().c, 2);
});
mdb2.close();

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
