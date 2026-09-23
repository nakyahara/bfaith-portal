import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
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
const report = await import('../apps/fba-box/report.js');
const notify = await import('../apps/fba-box/notify.js');
db._openForTest(dbFile);

let passed = 0, failed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ✅ ${name}`); }
  catch (e) { failed++; console.error(`  ❌ ${name}\n     ${e.message}`); }
}
/** 非同期の試験 (await して失敗を拾う。t() に async を渡すと失敗が握りつぶされる) */
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ✅ ${name}`); }
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
// ─── 確認した人 (R1: high#1 上書き / high#2 取消後に残る / medium#1 通信断で欠落) ───
const cwBox = db.createBox({ packGroupId: groupId, materialCode: 'box140', worker: member });
const wOther = db.getWorker(db.addWorker({ displayName: 'べつのひと', workerType: 'member', actor: 't' }).id);
/**
 * 不変条件 (Codex R2 medium#3 / R3 medium#2): source='auto' の確認した人 =
 * その行に残っている一番古い有効な (名前つき) 投入をした人。名前・由来・由来の投入の3列を
 * **必ず**照合する (名前が入っていれば通す、にすると auto→manual の誤遷移を見逃す)
 */
function checkWorkerCols(rowId) {
  return db.getDB().prepare('SELECT check_worker, check_worker_source, check_worker_placement_id FROM fbx_row_work WHERE row_id = ?').get(rowId) || {};
}
function assertAutoCheck(rowId, label) {
  const rw = checkWorkerCols(rowId);
  const p = db.getDB().prepare(`SELECT id, worker_name FROM fbx_placements
    WHERE row_id = ? AND revoked_at IS NULL AND worker_name IS NOT NULL AND worker_name != ''
    ORDER BY id LIMIT 1`).get(rowId);
  assert.equal(rw.check_worker ?? null, p ? p.worker_name : null, `${label}: 確認した人`);
  assert.equal(rw.check_worker_source ?? null, p ? 'auto' : null, `${label}: 由来`);
  assert.equal(rw.check_worker_placement_id ?? null, p ? p.id : null, `${label}: 由来の投入`);
}
/** 人が選んだ名前 (manual) / 移行前からある値 (source NULL) は投入に連動しない = 由来の投入を持たない */
function assertPinnedCheck(rowId, name, source, label) {
  const rw = checkWorkerCols(rowId);
  assert.equal(rw.check_worker ?? null, name, `${label}: 確認した人`);
  assert.equal(rw.check_worker_source ?? null, source, `${label}: 由来`);
  assert.equal(rw.check_worker_placement_id ?? null, null, `${label}: 由来の投入 (持たない)`);
}
const cwRow = () => db.getRunState(runId).rows.find(x => x.id === rowB.id);
const cwPlacements = () => db.getRunState(runId).placements.filter(x => x.row_id === rowB.id).sort((a, b) => a.id - b.id);

t('確認した人: 投入と同じトランザクションで自動で入る (画面からの別POSTではない)', () => {
  assert.equal(cwRow().check_worker, 'りようしゃ');       // 最初の投入で自動記録された
  assert.equal(cwRow().check_worker_source, 'auto');
  assertAutoCheck(rowB.id, '初期状態');
});
t('確認した人: あとから別の端末・別の人が入れても、先に入れた人を上書きしない (high#1)', () => {
  const add = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-1' });
  assert.equal(add.ok, true, JSON.stringify(add));
  assert.equal(add.checkWorker, 'りようしゃ');
  assertAutoCheck(rowB.id, '別の人が追加投入');
});
t('確認した人: 一番古い投入が消えると、残った投入をした人へ入れ替わる (high#2)', () => {
  for (const p of cwPlacements().filter(x => x.worker_name === 'りようしゃ')) {
    assert.equal(db.revokePlacement({ placementId: p.id, worker: member, deviceKey: 'dev:1' }).ok, true);
    assertAutoCheck(rowB.id, '取消の途中');
  }
  assert.equal(cwRow().check_worker, 'べつのひと');       // 残ったのは wOther の投入だけ
  assertAutoCheck(rowB.id, 'りようしゃの投入を全部取消');
});
t('確認した人: 投入を全部取り消すと消える。入れ直せば入れた人が入る (high#2)', () => {
  for (const p of cwPlacements()) {
    assert.equal(db.revokePlacement({ placementId: p.id, worker: member, deviceKey: 'dev:1' }).ok, true);
  }
  assert.equal(cwRow().check_worker, null);
  assert.equal(cwRow().check_worker_source, null);
  assertAutoCheck(rowB.id, '全部取消');
  const re = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 2, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-2' });
  assert.equal(re.ok, true, JSON.stringify(re));
  assert.equal(re.checkWorker, 'べつのひと');
  assertAutoCheck(rowB.id, '入れ直し');
});
t('確認した人: 数を直す (取消+入れ直しを1トランザクション) でも由来が追随する', () => {
  const p0 = cwPlacements()[0];
  const adj = db.adjustPlacement({ placementId: p0.id, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 'cw-adj' });
  assert.equal(adj.ok, true, JSON.stringify(adj));
  assert.equal(cwRow().check_worker, 'りようしゃ');       // 入れ直したのは member
  assertAutoCheck(rowB.id, '数を直したあと');
  assert.equal(db.adjustPlacement({ placementId: cwPlacements()[0].id, qty: 0, worker: member, deviceKey: 'dev:1', requestId: 'cw-adj0' }).ok, true);
  assert.equal(cwRow().check_worker, null);              // 0 に直す = 取消だけ → 有効な投入が無い
  assertAutoCheck(rowB.id, '0 に直したあと');
});
t('確認した人: 再送 (応答喪失) の冪等応答も、新規成功と同じ形で確認した人と由来を返す', () => {
  const first = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-idem' });
  assert.equal(first.ok, true, JSON.stringify(first));
  const again = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-idem' });
  assert.equal(again.already, true);
  assert.equal(again.placementId, first.placementId);          // 二重登録しない
  assert.equal(again.checkWorker, first.checkWorker);
  assert.equal(again.checkWorkerSource, 'auto');
  // 人が指名したあとの再送は、その名前と由来 manual を返す (画面が「先に入れた」と断定しないため)
  assert.equal(db.setRowWorkers({ rowId: rowB.id, checkWorker: 'さとう', worker: staff }).ok, true);
  const again2 = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-idem' });
  assert.equal(again2.checkWorker, 'さとう');
  assert.equal(again2.checkWorkerSource, 'manual');
  // 後始末
  assert.equal(db.setRowWorkers({ rowId: rowB.id, checkWorker: null, worker: staff }).ok, true);
  assert.equal(db.revokePlacement({ placementId: first.placementId, worker: member, deviceKey: 'dev:1' }).ok, true);
  assertAutoCheck(rowB.id, '冪等応答のテストのあと');
});
t('確認した人: 人が選んだ名前は投入でも取消でも動かない。消せば自動に戻る', () => {
  assert.equal(db.setRowWorkers({ rowId: rowB.id, checkWorker: 'さとう', worker: staff }).ok, true);
  assert.equal(db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: member, deviceKey: 'dev:1', requestId: 'cw-3' }).ok, true);
  assertPinnedCheck(rowB.id, 'さとう', 'manual', '人が選んだ直後に投入');
  for (const p of cwPlacements()) {
    assert.equal(db.revokePlacement({ placementId: p.id, worker: member, deviceKey: 'dev:1' }).ok, true);
  }
  assertPinnedCheck(rowB.id, 'さとう', 'manual', '全部取り消しても残る');
  assert.equal(db.setRowWorkers({ rowId: rowB.id, checkWorker: null, worker: staff }).ok, true);
  assertPinnedCheck(rowB.id, null, null, '消したあと');
});
t('確認した人: 移行前からある値 (source なし) は自動では消さない・書き換えない', () => {
  // 本番には、この変更より前に画面から書き込まれた check_worker が source NULL で残っている。
  // 人が選んだのか自動なのか区別できない → 触らない側に倒す (勝手に消えるほうが事故)
  db.getDB().prepare('UPDATE fbx_row_work SET check_worker = ?, check_worker_source = NULL, check_worker_placement_id = NULL WHERE row_id = ?')
    .run('きゅうデータ', rowB.id);
  const add = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-legacy' });
  assert.equal(add.ok, true, JSON.stringify(add));
  // 応答契約: 移行前データでも新規成功と冪等再送で同じ形 (source は NULL のまま。Codex R5 low#1)
  assert.equal(add.checkWorker, 'きゅうデータ');
  assert.equal(add.checkWorkerSource, null);
  const addAgain = db.addPlacement({ runId, rowId: rowB.id, boxId: cwBox.boxId, qty: 1, worker: wOther, deviceKey: 'dev:9', requestId: 'cw-legacy' });
  assert.equal(addAgain.already, true);
  assert.equal(addAgain.checkWorker, 'きゅうデータ');
  assert.equal(addAgain.checkWorkerSource, null);
  assertPinnedCheck(rowB.id, 'きゅうデータ', null, '移行前の値がある行に投入');
  assert.equal(db.revokePlacement({ placementId: add.placementId, worker: member, deviceKey: 'dev:1' }).ok, true);
  assertPinnedCheck(rowB.id, 'きゅうデータ', null, '移行前の値がある行の投入を取消');
  // 戻す (以降のテストの前提 = rowB は りようしゃ が期限 2028-06-24 で 10個 box1 に入れた状態)。
  // 公開APIだけで戻す — 3列を手で書くと不変条件を壊した状態を後続テストへ渡してしまう
  assert.equal(db.setRowWorkers({ rowId: rowB.id, checkWorker: null, worker: staff }).ok, true);
  assert.equal(db.addPlacement({ runId, rowId: rowB.id, boxId: box1.boxId, qty: 5, expiry: '2028-06-24', worker: member, deviceKey: 'dev:1', requestId: 'cw-restore-1' }).ok, true);
  assert.equal(db.addPlacement({ runId, rowId: rowB.id, boxId: box1.boxId, qty: 5, worker: member, deviceKey: 'dev:1', requestId: 'cw-restore-2' }).ok, true);
  assert.equal(cwRow().placed, 10);
  assert.equal(cwRow().check_worker, 'りようしゃ');
  assertAutoCheck(rowB.id, '後始末のあと');
  assert.equal(db.voidBox({ boxId: cwBox.boxId, reason: 'テストの後始末', worker: staff }).ok, true);
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
t('closeBox 3箱 → readiness ok。警告 = 欠番 / 外寸なし (box160)。確認担当は自動で入るので警告なし', () => {
  assert.equal(db.closeBox({ boxId: b1.boxId, measuredKg: 12.4, closedReason: 'items_done', worker: staff }).ok, true);
  assert.equal(db.closeBox({ boxId: b2.boxId, measuredKg: 8, worker: staff }).ok, true);
  assert.equal(db.closeBox({ boxId: b4.boxId, measuredKg: 20.25, worker: staff }).ok, true);
  const r = db.exportReadiness(run3);
  assert.equal(r.ok, true, JSON.stringify(r.blockers));
  const w = r.warnings.map((x) => x.code);
  assert.ok(w.includes('box_gap'), w.join());
  assert.ok(w.includes('no_dims'), w.join());
  // 入れた行の確認した人は投入と同じトランザクションで自動で入る (Codex PR2.6-R1) →
  // unchecked_rows は「1個も入れていない行」にだけ残る警告になった
  assert.ok(!w.includes('unchecked_rows'), w.join());
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
  // FBA 側の印 (fbx_meta) が無くても、スタッフマスタ側の「PIN を一度でも設定した」で閉じたまま
  // (PIN をスタッフマスタ画面や いろは在庫化 から設定した場合 — Codex #1301 R2 Medium)
  db.getDB().prepare("DELETE FROM fbx_meta WHERE key = 'roster_bootstrap_done'").run();
  assert.equal(db.isRosterBootstrap(), false, 'staff.db の pin_ever_set で閉じる');
  db.getDB().prepare("INSERT INTO fbx_meta (key, value) VALUES ('roster_bootstrap_done', '1')").run();
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
  // retired 行は「確認担当が未記録」の警告にも数えない (iPad に出ず担当も付けられない = 消せない警告になる。
  // Codex R2 medium#1 / R3 low#1)。普通の未記録行は今までどおり数える
  db.setRowWorkers({ rowId: rA.id, checkWorker: null, worker: staff });
  const un = db.exportReadiness(c.runId).warnings.find((w) => w.code === 'unchecked_rows');
  assert.ok(un, '未記録の通常行があるので警告は出る');
  assert.ok(un.rows.some((x) => x.id === rA.id), '通常行は数える');
  assert.ok(!un.rows.some((x) => x.id === eo2.id), 'retired 行は数えない');
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
  t('本社向けまとめ (buildRunReport): 予定と違う商品に印・箱の重さと外寸・Amazon の箱番号・完了通知の本文', () => {
    const rep = report.buildRunReport(c.runId);
    assert.equal(rep.run.status, 'done');
    assert.equal(rep.totals.boxes, 1, '取消した空箱は数えない');
    assert.equal(rep.totals.weightKg, 2.5);
    assert.equal(rep.totals.planned, 7); assert.equal(rep.totals.placed, 3);
    assert.equal(rep.totals.diffRows, 1);
    const rows = rep.groups[0].rows;
    const a = rows.find((r) => r.fnsku === 'X0FIN00001'), b = rows.find((r) => r.fnsku === 'X0FIN00002');
    assert.equal(a.alert, false); assert.equal(a.diff, 0); assert.deepEqual(a.inBoxes.map((x) => [x.code, x.qty]), [['通常-1', 3]]);
    assert.equal(b.alert, true); assert.equal(b.diff, -4); assert.equal(b.placed, 0);
    assert.ok(b.reasonJa && b.reasonJa.includes('今回は納品しない'), b.reasonJa);
    assert.equal(rep.groups[0].boxes.length, 1, '取消した箱 (通常-2) は出さない');
    const box = rep.groups[0].boxes[0];
    assert.equal(box.code, '通常-1'); assert.equal(box.amazonName, 'B1'); assert.equal(box.weightKg, 2.5); assert.equal(box.overLimit, false);
    const m = db.listMaterials(true).find((x) => x.code === 'box140');
    assert.equal(box.material, m.name, '資材の名前 (140サイズ段ボール) = 送り状のサイズ');
    if (m.width_cm > 0) { assert.ok(box.dims); assert.equal(box.sum3, Math.round((m.width_cm + m.length_cm + m.height_cm) * 10) / 10); }
    else assert.equal(box.dims, null, '外寸が未登録なら出さない (推測しない)');
    assert.deepEqual(box.contents.map((x) => [x.fnsku, x.qty]), [['X0FIN00001', 3]]);
    const text = report.runDoneText(rep, { link: 'https://example.test/apps/fba-box/admin/runs/1/report', doneBy: 'しょくいん', at: new Date('2026-09-12T06:40:00Z') });
    assert.ok(text.includes('FBA箱詰めが終わりました') && text.includes(rep.run.title), text);
    assert.ok(text.includes('箱 1 箱') && text.includes('2.5 kg') && text.includes('商品 1 種類 3 個'), text);
    assert.ok(text.includes('⚠ 予定と違う商品 1 件'), text);
    assert.ok(text.includes('しょくいん') && text.includes('9/12 15:40'), text);
    assert.ok(text.includes('<https://example.test/apps/fba-box/admin/runs/1/report|'), 'Google Chat のリンク書式');
    // いちばん上の 3 つ (中原さん 2026-09-18): ①プラン×区分の箱の数 ②数量の変更・キャンセル ③期限
    assert.deepEqual(rep.planBoxes.kinds, ['通常', '危険物', '大型'], '大型2 は使った回だけ');
    assert.deepEqual(rep.planBoxes.plans.map((p) => p.plan), ['P1']);
    const cells = Object.fromEntries(rep.planBoxes.plans[0].cells.map((x) => [x.kind, x]));
    assert.equal(cells['通常'].exists, true); assert.equal(cells['通常'].boxes, 1, '取消した空箱は数えない'); assert.equal(cells['通常'].weightKg, 2.5); assert.equal(cells['通常'].qty, 3);
    assert.equal(cells['危険物'].exists, false, 'この回に無い区分'); assert.equal(cells['危険物'].boxes, 0);
    assert.equal(rep.planBoxes.total.boxes, 1);
    assert.deepEqual(rep.changes.map((x) => [x.group, x.planNo, x.fnsku, x.action, x.planned, x.placed]), [['P1_通常', '通常_2', 'X0FIN00002', 'cancel', 4, 0]], '予定どおりの商品は出さない');
    assert.ok(rep.changes[0].reasonJa.includes('今回は納品しない'));
    assert.equal(rep.pendingRows, 0, '完了した回に「まだ決まっていない」は無い');
    assert.ok(rep.tsv.chg && rep.tsv.chg.includes('キャンセル'), 'コピー用');
  });
  {
    // 期限・作業中の回 (未投入は「差」として赤・理由は「未投入」)
    const c9 = db.createRunFromPicking({ pickingRun: { id: 409, delivery_date: '2026-09-24' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-e1', fnsku: 'X0EXP00001', productName: '期限のある商品', qty: '5' }] },
    ], createdBy: 't' });
    const st9 = db.getRunState(c9.runId);
    const b9 = db.createBox({ packGroupId: st9.groups[0].id, materialCode: 'box140', worker: member });
    db.createBox({ packGroupId: st9.groups[0].id, materialCode: 'box140', worker: member });   // 中身の無い箱
    db.addPlacement({ runId: c9.runId, rowId: st9.rows[0].id, boxId: b9.boxId, qty: 2, expiry: '2027-03-31', worker: member, deviceKey: 'dev:e', requestId: 'exp9a' });
    db.addPlacement({ runId: c9.runId, rowId: st9.rows[0].id, boxId: b9.boxId, qty: 1, expiry: '2027-03-31', worker: member, deviceKey: 'dev:e', requestId: 'exp9b' });
    t('本社向けまとめ: 期限は行ごと・箱ごとにまとめる / 作業中の回は未投入を差として出す / 中身の無い箱は出さない', () => {
      const rep = report.buildRunReport(c9.runId);
      assert.equal(rep.run.status, 'active');
      const r = rep.groups[0].rows[0];
      assert.equal(r.placed, 3); assert.equal(r.remaining, 2); assert.equal(r.diff, -2); assert.equal(r.alert, true); assert.equal(r.reasonJa, null);
      assert.deepEqual(r.expiries, [{ expiry: '2027-03-31', qty: 3 }]);
      assert.deepEqual(rep.expiries.map((e) => [e.group, e.planNo, e.fnsku, e.expiry, e.qty]), [['P1_通常', '通常_1', 'X0EXP00001', '2027-03-31', 3]]);
      // 作業中の回: まだ入れ終わっていない商品は「数量の変更・キャンセル」に混ぜない (減らすかどうかが決まっていない) → 件数だけ
      assert.deepEqual(rep.changes, []); assert.equal(rep.pendingRows, 1);
      assert.equal(rep.groups[0].boxes.length, 1, '中身の無い箱は送らないので出さない');
      assert.deepEqual(rep.groups[0].boxes[0].contents.map((x) => [x.expiry, x.qty]), [['2027-03-31', 3]]);
      assert.equal(rep.totals.openBoxes, 1, 'まだ閉じていない箱'); assert.equal(rep.totals.noWeight, 1);
      assert.equal(report.buildRunReport(999999), null);
    });
  }
  {
    // 通知 (Google Chat): 未設定なら送らない・失敗しても throw しない
    const saved = process.env[notify.WEBHOOK_ENV];
    delete process.env[notify.WEBHOOK_ENV];
    const n0 = await notify.notifyHq('x');
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const got = [];
    notify.setNotifySender(async (url, text) => { got.push({ url, text }); });
    const n1 = await notify.notifyHq('こんにちは');
    notify.setNotifySender(async () => { throw new Error('boom 500'); });
    const n2 = await notify.notifyHq('こんにちは');
    const n3 = await notify.notifyHq('');
    notify.setNotifySender(null);
    if (saved === undefined) delete process.env[notify.WEBHOOK_ENV]; else process.env[notify.WEBHOOK_ENV] = saved;
    t('完了通知: webhook 未設定なら送らない / 設定があれば送る / 送信失敗・空文字も throw せず理由を返す', () => {
      assert.deepEqual(n0, { sent: false, reason: 'no_webhook' });
      assert.deepEqual(n1, { sent: true }); assert.equal(got[0].url, 'https://chat.example/hook'); assert.equal(got[0].text, 'こんにちは');
      assert.equal(n2.sent, false); assert.ok(n2.reason.includes('boom'));
      assert.deepEqual(n3, { sent: false, reason: 'empty' });
    });
  }
  {
    // 完了通知の送信待ち (outbox) — Codex PR #1307 R1 P1: 完了と同じトランザクションで積む / 送る / 再試行 / 未設定は skipped / 同時に呼んでも 1 通 / 持ったまま落ちた札は 5 分で取り直す
    const ob = await import('../apps/fba-box/notify-outbox.js');
    const savedHook = process.env[notify.WEBHOOK_ENV], savedBase = process.env.PUBLIC_BASE_URL;
    delete process.env.PUBLIC_BASE_URL;
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const got = [];
    let fail = false;
    notify.setNotifySender(async (url, text) => { if (fail) throw new Error('chat 503'); got.push(text); });
    const mkRun = (id, fnsku) => db.createRunFromPicking({ pickingRun: { id, delivery_date: '2026-09-25' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-' + id, fnsku, productName: '通知の試験 ' + id, qty: '1' }] }], createdBy: 't' });
    const row0 = db.listNotifyOutbox(c.runId);
    await ob.drainNotifyOutbox();
    const afterSend = db.listNotifyOutbox(c.runId);
    db.finishRun({ runId: c.runId, acknowledge: true, worker: staff });   // 二度押し (already)
    const afterAgain = db.listNotifyOutbox(c.runId);
    const c10 = mkRun(410, 'X0OUT00010');
    db.enqueueRunDoneNotify(c10.runId, 'てすと');
    fail = true;
    await ob.drainNotifyOutbox();
    const retry = db.listNotifyOutbox(c10.runId)[0];
    await ob.drainNotifyOutbox();   // まだ時刻前なので送らない
    const gotBefore = got.length;
    fail = false;
    db.getDB().prepare('UPDATE fbx_notify_outbox SET next_try_at = ? WHERE id = ?').run(new Date(Date.now() - 1000).toISOString(), retry.id);
    await Promise.all([ob.drainNotifyOutbox(), ob.drainNotifyOutbox()]);   // 同時に呼んでも 1 通
    const retried = db.listNotifyOutbox(c10.runId)[0];
    const c12 = mkRun(412, 'X0OUT00012');
    db.enqueueRunDoneNotify(c12.runId, 'x');
    const j12 = db.listNotifyOutbox(c12.runId)[0];
    db.getDB().prepare('UPDATE fbx_notify_outbox SET claimed_at = ?, claim_token = ? WHERE id = ?').run(new Date(Date.now() - 60 * 1000).toISOString(), 'dead', j12.id);
    await ob.drainNotifyOutbox();
    const held = db.listNotifyOutbox(c12.runId)[0];
    db.getDB().prepare('UPDATE fbx_notify_outbox SET claimed_at = ? WHERE id = ?').run(new Date(Date.now() - 10 * 60 * 1000).toISOString(), j12.id);
    await ob.drainNotifyOutbox();
    const reclaimed = db.listNotifyOutbox(c12.runId)[0];
    const c11 = mkRun(411, 'X0OUT00011');
    db.enqueueRunDoneNotify(c11.runId, 'x');
    delete process.env[notify.WEBHOOK_ENV];
    await ob.drainNotifyOutbox();
    const skipped = db.listNotifyOutbox(c11.runId)[0];
    // まとめを作れない一時的な失敗 (SQLite の busy 等) も再試行する。回が無いときだけ打ち切る (Codex PR #1307 R2 P1)
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const c14 = mkRun(414, 'X0OUT00014');
    db.enqueueRunDoneNotify(c14.runId, 'x');
    ob._setReportBuilderForTest(() => { throw new Error('SQLITE_BUSY: database is locked'); });
    await ob.drainNotifyOutbox();
    const buildRetry = db.listNotifyOutbox(c14.runId)[0];
    ob._setReportBuilderForTest(null);
    db.getDB().prepare('UPDATE fbx_notify_outbox SET next_try_at = ? WHERE id = ?').run(new Date(Date.now() - 1000).toISOString(), buildRetry.id);
    await ob.drainNotifyOutbox();
    const buildRecovered = db.listNotifyOutbox(c14.runId)[0];
    const c15 = mkRun(415, 'X0OUT00015');
    db.enqueueRunDoneNotify(c15.runId, 'x');
    ob._setReportBuilderForTest(() => null);   // 回が見つからない = 何度やっても送れない
    await ob.drainNotifyOutbox();
    const notFound = db.listNotifyOutbox(c15.runId)[0];
    ob._setReportBuilderForTest(null);
    // 8 回続けて送れなければ打ち切り、9 回目は送らない (Codex PR #1307 R3)
    const c16 = mkRun(416, 'X0OUT00016');
    db.enqueueRunDoneNotify(c16.runId, 'x');
    const j16 = db.listNotifyOutbox(c16.runId)[0];
    let calls = 0;
    notify.setNotifySender(async () => { calls++; throw new Error('chat down'); });
    for (let i = 0; i < ob.MAX_ATTEMPTS + 1; i++) {
      db.getDB().prepare('UPDATE fbx_notify_outbox SET next_try_at = ? WHERE id = ?').run(new Date(Date.now() - 1000).toISOString(), j16.id);
      await ob.drainNotifyOutbox();
    }
    const gaveUp = db.listNotifyOutbox(c16.runId)[0];
    const callsAtGiveUp = calls;
    delete process.env[notify.WEBHOOK_ENV];
    ob._stopNotifyOutboxForTest();
    notify.setNotifySender(null);
    if (savedHook === undefined) delete process.env[notify.WEBHOOK_ENV]; else process.env[notify.WEBHOOK_ENV] = savedHook;
    if (savedBase !== undefined) process.env.PUBLIC_BASE_URL = savedBase;
    t('完了通知の送信待ち: 完了で 1 件積み、送ると sent / リンクは Host ではなく本番のアドレス / 二度押しでは積まない', () => {
      assert.equal(row0.length, 1); assert.equal(row0[0].status, 'pending'); assert.equal(row0[0].done_by, staff.display_name);
      assert.equal(afterSend[0].status, 'sent'); assert.ok(afterSend[0].sent_at);
      assert.ok(got[0].includes('FBA箱詰めが終わりました'), got[0]);
      assert.ok(got[0].includes(`<https://bfaith-portal.onrender.com/apps/fba-box/admin/runs/${c.runId}/report|`), got[0]);
      assert.equal(afterAgain.length, 1, '二度押しでは積まない');
    });
    t('完了通知の送信待ち: 送れなければ間隔を空けて再試行 / 時刻前は送らない / 同時に呼んでも 1 通 / 持ったまま落ちた札は 5 分で取り直す / 未設定は skipped', () => {
      assert.equal(retry.status, 'pending'); assert.equal(retry.attempts, 1); assert.ok(retry.last_error.includes('503'), retry.last_error);
      assert.ok(Date.parse(retry.next_try_at) > Date.now(), '次は少し後');
      assert.equal(gotBefore, 1, '時刻前は送らない');
      assert.equal(retried.status, 'sent'); assert.equal(got.filter((x) => x.includes(`/admin/runs/${c10.runId}/report`)).length, 1, '同時に呼んでも 1 通');
      assert.equal(held.status, 'pending', '1 分前に持たれたままの札は取らない');
      assert.equal(reclaimed.status, 'sent', '5 分を過ぎた札は取り直して送る');
      assert.equal(skipped.status, 'skipped'); assert.equal(skipped.last_error, 'no_webhook');
      const ev = db.listEvents(500).filter((e) => e.action === 'notify_run_done');
      assert.ok(ev.some((e) => e.run_id === c10.runId && !e.ok) && ev.some((e) => e.run_id === c10.runId && e.ok), '再試行と送れたことを履歴に残す');
    });
    t('完了通知の送信待ち: まとめを作れない一時的な失敗も再試行し、直れば送る / 回が無いときだけ打ち切る (Codex R2 P1)', () => {
      assert.equal(buildRetry.status, 'pending', '即打ち切らない'); assert.equal(buildRetry.attempts, 1);
      assert.ok(buildRetry.last_error.includes('SQLITE_BUSY'), buildRetry.last_error);
      assert.ok(Date.parse(buildRetry.next_try_at) > Date.now() - 1000 * 60 * 60, '次の時刻が入っている');
      assert.equal(buildRecovered.status, 'sent', '直ったら送る');
      assert.equal(notFound.status, 'failed'); assert.equal(notFound.last_error, 'run_not_found');
    });
    t('完了通知の送信待ち: 8 回続けて送れなければ打ち切り、9 回目は送らない (Codex R3)', () => {
      assert.equal(gaveUp.status, 'failed'); assert.equal(gaveUp.attempts, ob.MAX_ATTEMPTS);
      assert.ok(gaveUp.last_error.includes('chat down'), gaveUp.last_error);
      assert.equal(callsAtGiveUp, ob.MAX_ATTEMPTS, `送ろうとした回数 = ${callsAtGiveUp} (${ob.MAX_ATTEMPTS} のはず)`);
    });
  }
  {
    // 完了した回の知らせを「もう一度送る」(中原さん 2026-09-17「再送信とか」)
    const ob = await import('../apps/fba-box/notify-outbox.js');
    const savedHook = process.env[notify.WEBHOOK_ENV], savedBase = process.env.PUBLIC_BASE_URL;
    delete process.env.PUBLIC_BASE_URL;
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const got = [];
    notify.setNotifySender(async (url, text) => { got.push(text); });
    const mkRun = (id) => db.createRunFromPicking({ pickingRun: { id, delivery_date: '2026-09-26' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-rs' + id, fnsku: 'X0RS' + String(id).padStart(6, '0'), productName: '再送の試験 ' + id, qty: '1' }] }], createdBy: 't' });
    const mkDone = (id) => { const r = mkRun(id); const f = db.finishRun({ runId: r.runId, acknowledge: true, worker: staff }); if (!f.ok) throw new Error(JSON.stringify(f)); return r.runId; };
    const textsOf = (runId) => got.filter((x) => x.includes(`/admin/runs/${runId}/report|`));
    // ① 届いた回をもう一度 → 【再送】付きで 1 通。二度押しは 1 通にまとめる
    const r1 = mkDone(420);
    await ob.drainNotifyOutbox();
    const first = db.getRunDoneNotify(r1);
    // 完了の知らせが届いてから 3 分以上たった回 (届いた直後は待たせる — 下の too_soon のテストで見る)
    const aged = (runId) => { const t0 = new Date(Date.now() - db.RESEND_COOLDOWN_MS - 1000).toISOString(); db.getDB().prepare('UPDATE fbx_notify_outbox SET sent_at = ?, resent_at = NULL WHERE run_id = ?').run(t0, runId); };
    aged(r1);
    const re1 = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000001' });
    const afterReset = db.getRunDoneNotify(r1);
    const re1b = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000002' });
    await ob.drainNotifyOutbox();
    const resent = db.getRunDoneNotify(r1);
    const texts1 = textsOf(r1);
    // 送れて sent になったあとに応答だけ消え、同じ操作で押し直した (Codex #1350 R1 #2) → 1 回のまま。
    // 🚨 操作 A (rq-1) の応答が消えている間に別の操作 B (rq-2) を受け付けていても、A の押し直しは A として照合する (R2 #1)
    const re1c = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000002' });
    const re1cA = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000001' });
    await ob.drainNotifyOutbox();
    const textsAfterReplay = textsOf(r1).length;
    // 届いた直後に別の操作 → too_soon (Codex #1350 R1 #3)。3 分たてば送れる
    const re1d = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000003' });
    const cooling = db.getRunDoneNotify(r1);
    const old = new Date(Date.now() - db.RESEND_COOLDOWN_MS - 1000).toISOString();
    db.getDB().prepare('UPDATE fbx_notify_outbox SET sent_at = ?, resent_at = ? WHERE run_id = ?').run(old, old, r1);
    const cooled = db.getRunDoneNotify(r1);
    // 3 分たったあとに A・B を押し直しても、まだ 1 回のまま (R2 #1: 上書きされた A が通って 3 通目になっていた)
    const lateA = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000001' });
    const lateB = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000002' });
    const refusedRetry = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000003' });   // 断った操作は残さない → 待てば通る
    db.getDB().prepare('UPDATE fbx_notify_outbox SET sent_at = ?, resent_at = ? WHERE run_id = ?').run(old, old, r1);
    const re1e = db.resendRunDoneNotify({ runId: r1, requestedBy: 'いろはiPad', requestId: 'rq-000004' });
    await ob.drainNotifyOutbox();
    const textsAfterCool = textsOf(r1).length;
    // ② いま送っている最中 (持ち札が生きている) は断る / 札が切れていれば今すぐ送る
    const r2 = mkDone(421);
    const j2 = db.listNotifyOutbox(r2)[0];
    db.getDB().prepare('UPDATE fbx_notify_outbox SET claimed_at = ?, claim_token = ? WHERE id = ?').run(new Date(Date.now() - 30 * 1000).toISOString(), 'live', j2.id);
    const sendingView = db.getRunDoneNotify(r2);
    const re2 = db.resendRunDoneNotify({ runId: r2, requestedBy: 'x', requestId: 'rq-000021' });
    const stillHeld = db.listNotifyOutbox(r2)[0];
    db.getDB().prepare('UPDATE fbx_notify_outbox SET claimed_at = ?, attempts = 3 WHERE id = ?').run(new Date(Date.now() - 10 * 60 * 1000).toISOString(), j2.id);
    const re2b = db.resendRunDoneNotify({ runId: r2, requestedBy: 'x', requestId: 'rq-000022' });
    const bumped = db.listNotifyOutbox(r2)[0];
    await ob.drainNotifyOutbox();
    const afterStale = db.getRunDoneNotify(r2);
    // ③ 通知先が未設定で送らなかった回 → 設定したあと送ると、初めて届く 1 通なので【再送】は付けない
    delete process.env[notify.WEBHOOK_ENV];
    const r3 = mkDone(422);
    await ob.drainNotifyOutbox();
    const skippedView = db.getRunDoneNotify(r3);
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const re3 = db.resendRunDoneNotify({ runId: r3, requestedBy: '本社 中原', requestId: 'rq-000031' });
    await ob.drainNotifyOutbox();
    const afterSkipped = db.getRunDoneNotify(r3);
    const texts3 = textsOf(r3);
    // ④ 8 回で打ち切った回 (failed) も戻せる
    const r4 = mkDone(423);
    db.getDB().prepare(`UPDATE fbx_notify_outbox SET status = 'failed', attempts = 8, last_error = 'chat down' WHERE run_id = ?`).run(r4);
    const re4 = db.resendRunDoneNotify({ runId: r4, requestedBy: 'x', requestId: 'rq-000041' });
    const failedReset = db.listNotifyOutbox(r4)[0];
    await ob.drainNotifyOutbox();
    const afterFailed = db.getRunDoneNotify(r4);
    // ⑤ 知らせが積まれていない完了回 (STA アップ済みで完了・通知ができる前に完了) → ここで積む。終えた人と時刻は完了の記録から
    const r5 = mkRun(424).runId;
    // STA アップ済みの記録で完了した回 (markStaUploaded と同じ書き方。この経路は知らせを積まない)
    db.getDB().prepare("UPDATE fbx_runs SET status = 'done', done_at = ? WHERE id = ?").run('2026-09-16T01:02:03.000Z', r5);
    db.logEvent({ runId: r5, action: 'run_sta_uploaded', targetType: 'run', targetId: r5, deviceLabel: 'session:hq@test', ok: true });
    const none5 = db.getRunDoneNotify(r5);
    const re5 = db.resendRunDoneNotify({ runId: r5, requestedBy: 'y', requestId: 'rq-000051' });
    const row5 = db.listNotifyOutbox(r5)[0];
    await ob.drainNotifyOutbox();
    const after5 = db.getRunDoneNotify(r5);
    const texts5 = textsOf(r5);
    // ⑥ 完了していない回・無い回
    const re6 = db.resendRunDoneNotify({ runId: mkRun(425).runId, requestedBy: 'x', requestId: 'rq-000061' });
    const re7 = db.resendRunDoneNotify({ runId: 999999, requestedBy: 'x', requestId: 'rq-000071' });
    // ⑦ 送信処理が一覧を取ったあと、前の知らせを送っている間に「もう一度送る」で回数を戻した (Codex #1350 R1 #1)
    //    → 持った直後の行で送るので、1 回の失敗では打ち切らない
    const rA = mkDone(426), rB = mkDone(427);
    db.getDB().prepare('UPDATE fbx_notify_outbox SET attempts = ? WHERE run_id = ?').run(ob.MAX_ATTEMPTS - 1, rB);
    let releaseA;
    const gateA = new Promise((r) => { releaseA = r; });
    let aStarted = false;
    notify.setNotifySender(async (url, text) => {
      if (text.includes(`/admin/runs/${rA}/report|`)) { aStarted = true; await gateA; got.push(text); return; }
      if (text.includes(`/admin/runs/${rB}/report|`)) throw new Error('chat 503');
      got.push(text);
    });
    const racing = ob.drainNotifyOutbox();
    for (let i = 0; i < 50 && !aStarted; i++) await new Promise((r) => setImmediate(r));
    const raceStarted = aStarted;
    const reB = db.resendRunDoneNotify({ runId: rB, requestedBy: 'x', requestId: 'rq-000081' });
    releaseA();
    await racing;
    const afterRace = db.listNotifyOutbox(rB)[0];
    notify.setNotifySender(async (url, text) => { got.push(text); });
    // ⑧ 全部の回で 10 分に 10 回まで (Codex #1350 R1 #3)。まだ送信待ちを今すぐ送るのは 1 通のままなので数えない
    const countRecent = () => db.getDB().prepare(`SELECT COUNT(*) c FROM fbx_notify_resend_requests WHERE created_at >= ? AND queued IN ('reset','inserted')`)
      .get(new Date(Date.now() - db.RESEND_WINDOW_MS).toISOString()).c;
    const fillFrom = countRecent();
    for (let i = fillFrom; i < db.RESEND_MAX; i++) {
      db.getDB().prepare(`INSERT INTO fbx_notify_resend_requests (run_id, request_id, queued, created_at) VALUES (?, ?, 'reset', ?)`).run(r1, 'synthetic-' + i, new Date().toISOString());
    }
    const plan = db.getDB().prepare(`EXPLAIN QUERY PLAN SELECT COUNT(*) c FROM fbx_notify_resend_requests WHERE created_at >= ? AND queued IN ('reset','inserted')`).all('x').map((r) => r.detail).join(' / ');
    delete process.env[notify.WEBHOOK_ENV];
    const r9 = mkDone(428);   // 未設定で送らなかった回
    await ob.drainNotifyOutbox();
    process.env[notify.WEBHOOK_ENV] = 'https://chat.example/hook';
    const tooMany = db.resendRunDoneNotify({ runId: r9, requestedBy: 'x', requestId: 'rq-000091' });
    const r10 = mkDone(429);   // まだ送信待ち
    const pendingOk = db.resendRunDoneNotify({ runId: r10, requestedBy: 'x', requestId: 'rq-000101' });
    db.getDB().prepare(`DELETE FROM fbx_notify_resend_requests WHERE request_id LIKE 'synthetic-%'`).run();
    await ob.drainNotifyOutbox();
    ob._stopNotifyOutboxForTest();
    notify.setNotifySender(null);
    if (savedHook === undefined) delete process.env[notify.WEBHOOK_ENV]; else process.env[notify.WEBHOOK_ENV] = savedHook;
    if (savedBase !== undefined) process.env.PUBLIC_BASE_URL = savedBase;
    t('もう一度送る: 届いた回は送信待ちに戻して【再送】付きで 1 通 / 二度押しは 1 通 / 押した人を書く', () => {
      assert.equal(first.status, 'sent'); assert.equal(first.sent_count, 1);
      assert.deepEqual(re1, { ok: true, queued: 'reset' });
      assert.equal(afterReset.status, 'pending'); assert.equal(afterReset.attempts, 0); assert.equal(afterReset.resent_by, 'いろはiPad');
      assert.deepEqual(re1b, { ok: true, queued: 'pending' }, '二度押しはまだ送信待ち → 積み直さない');
      assert.equal(resent.status, 'sent'); assert.equal(resent.sent_count, 2);
      assert.equal(texts1.length, 2, `二度押ししても再送は 1 通 (届いた数 ${texts1.length})`);
      assert.ok(!texts1[0].startsWith('【再送】'), '最初の知らせには付けない');
      assert.ok(texts1[1].startsWith('【再送】📦'), texts1[1]);
      assert.ok(texts1[1].includes('もう一度送った人: いろはiPad'), texts1[1]);
      assert.ok(texts1[1].includes(`終えた人: ${staff.display_name}`), '終えた人は最初の完了のまま: ' + texts1[1]);
      const ev = db.listEvents(1000, r1).filter((e) => e.action === 'notify_resend');
      assert.equal(ev.length, 4, '押したことを履歴に残す (同じ操作の押し直し・断った分は数えない): ' + ev.map((e) => e.payload).join(' | '));
    });
    t('もう一度送る: 送れたあとに応答が消えて同じ操作で押し直しても 1 回 / 届いた直後は待つ / 待てば送れる (Codex #1350 R1 #2 #3)', () => {
      assert.deepEqual(re1c, { ok: true, queued: 'already' });
      assert.deepEqual(re1cA, { ok: true, queued: 'already' }, 'あとの操作 B に上書きされず、前の操作 A も照合できる (R2 #1)');
      assert.equal(textsAfterReplay, 2, '押し直しでは送らない');
      assert.deepEqual(lateA, { ok: true, queued: 'already' }, '3 分たっても A の押し直しは 1 回のまま');
      assert.deepEqual(lateB, { ok: true, queued: 'already' });
      assert.equal(re1d.ok, false); assert.equal(re1d.error, 'too_soon'); assert.ok(re1d.retryAt && re1d.message.includes('から'), re1d.message);
      assert.ok(cooling.cooldown_until, '画面にも待ちの時刻を出せる'); assert.equal(cooled.cooldown_until, null);
      assert.deepEqual(refusedRetry, { ok: true, queued: 'reset' }, '断った操作 (too_soon) は残さないので、待てば同じ操作で送れる');
      assert.deepEqual(re1e, { ok: true, queued: 'pending' }, 'rq-3 がまだ送信待ちなので、rq-4 はそこにまとめる');
      assert.equal(textsAfterCool, 3, '待ってからの新しい操作は 1 通だけ (rq-3 と rq-4 で 1 通)');
    });
    t('もう一度送る: 送っている最中は断る (戻すと 2 通になる) / 持ち札が切れていれば今すぐ送る', () => {
      assert.equal(sendingView.sending, true);
      assert.equal(re2.ok, false); assert.equal(re2.error, 'sending');
      assert.equal(stillHeld.claim_token, 'live', '断ったときは何も変えない');
      assert.deepEqual(re2b, { ok: true, queued: 'pending' });
      assert.equal(bumped.attempts, 0, '回数を数え直す'); assert.equal(bumped.claim_token, null);
      assert.equal(afterStale.status, 'sent'); assert.equal(afterStale.sending, false);
    });
    t('もう一度送る: 未設定で送らなかった回は、設定後の初めての 1 通に【再送】を付けない / 打ち切った回も戻せる', () => {
      assert.equal(skippedView.status, 'skipped'); assert.equal(skippedView.sent_count, 0);
      assert.deepEqual(re3, { ok: true, queued: 'reset' });
      assert.equal(afterSkipped.status, 'sent'); assert.equal(afterSkipped.sent_count, 1);
      assert.equal(texts3.length, 1); assert.ok(texts3[0].startsWith('📦'), texts3[0]);
      assert.deepEqual(re4, { ok: true, queued: 'reset' });
      assert.equal(failedReset.status, 'pending'); assert.equal(failedReset.attempts, 0); assert.equal(failedReset.last_error, null);
      assert.equal(afterFailed.status, 'sent');
    });
    t('もう一度送る: 知らせが積まれていない完了回はここで積む (終えた人・時刻は完了の記録から) / 完了していない回・無い回は断る', () => {
      assert.equal(none5, null);
      assert.deepEqual(re5, { ok: true, queued: 'inserted' });
      assert.equal(row5.done_by, 'session:hq@test'); assert.equal(row5.created_at, db.getRun(r5).done_at);
      assert.equal(after5.status, 'sent'); assert.equal(texts5.length, 1); assert.ok(texts5[0].startsWith('📦'), '初めての 1 通: ' + texts5[0]);
      assert.equal(re6.ok, false); assert.equal(re6.error, 'not_done');
      assert.equal(re7.ok, false); assert.equal(re7.error, 'not_found');
    });
    t('もう一度送る: 送信処理が一覧を取ったあとで回数を戻しても、持った直後の行で送る (1 回の失敗で打ち切らない — Codex #1350 R1 #1)', () => {
      assert.equal(raceStarted, true, '前の知らせを送っている間に押した');
      assert.deepEqual(reB, { ok: true, queued: 'pending' });
      assert.equal(afterRace.status, 'pending', JSON.stringify(afterRace)); assert.equal(afterRace.attempts, 1);
    });
    t('もう一度送る: 全部の回で 10 分に 10 回まで (too_many)。まだ送信待ちを今すぐ送るのは数えない (Codex #1350 R1 #3)', () => {
      assert.equal(tooMany.ok, false); assert.equal(tooMany.error, 'too_many');
      assert.ok(plan.includes('idx_fbx_notify_resend_requests_at'), '数えるのは受け付けた操作の表を時刻の索引で (監査ログ全体を走査しない — R2 #2): ' + plan);
      assert.deepEqual(pendingOk, { ok: true, queued: 'pending' });
    });
  }
  {
    // もう一度送る: 列を足す移行。もう送れている行は sent_count = 1 にそろえる (次に送ると【再送】と書ける)。2 回目の起動では数え直さない
    const legacyFile = path.join(tmp, 'legacy-outbox.db');
    const Database = (await import('better-sqlite3')).default;
    const L = new Database(legacyFile);
    L.exec(`CREATE TABLE fbx_notify_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('run_done')), done_by TEXT,
      created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','skipped','failed')), attempts INTEGER NOT NULL DEFAULT 0,
      next_try_at TEXT NOT NULL, claimed_at TEXT, claim_token TEXT, sent_at TEXT, last_error TEXT, UNIQUE(run_id, kind))`);
    const ins = L.prepare(`INSERT INTO fbx_notify_outbox (run_id, kind, created_at, status, attempts, next_try_at, sent_at) VALUES (?, 'run_done', '2026-09-13T00:00:00.000Z', ?, 1, '2026-09-13T00:00:00.000Z', ?)`);
    ins.run(1, 'sent', '2026-09-13T00:00:05.000Z'); ins.run(2, 'skipped', null);
    L.close();
    db._openForTest(legacyFile);
    const migrated = db.getDB().prepare('SELECT run_id, sent_count, resent_by FROM fbx_notify_outbox ORDER BY run_id').all();
    const hasRequests = !!db.getDB().prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fbx_notify_resend_requests'").get();
    db.getDB().prepare('UPDATE fbx_notify_outbox SET sent_count = 3 WHERE run_id = 1').run();
    db._openForTest(legacyFile);
    const reopened = db.getDB().prepare('SELECT sent_count FROM fbx_notify_outbox WHERE run_id = 1').get().sent_count;
    db._openForTest(dbFile);
    t('もう一度送る: 既存の送信待ちに列を足し、送れている行だけ sent_count = 1 / 2 回目の起動では数え直さない', () => {
      assert.deepEqual(migrated.map((r) => ({ ...r })), [{ run_id: 1, sent_count: 1, resent_by: null }, { run_id: 2, sent_count: 0, resent_by: null }]);
      assert.equal(hasRequests, true, '受け付けた操作の表もできる');
      assert.equal(reopened, 3);
    });
  }
  t('表計算へのコピー: 式として動く値に \' を付け、タブ・改行は空白に (Codex PR #1307 R1 P2)', () => {
    assert.equal(report.tsvCell('=HYPERLINK("x")'), '\'=HYPERLINK("x")');
    assert.equal(report.tsvCell('+81'), "'+81"); assert.equal(report.tsvCell('-abc'), "'-abc"); assert.equal(report.tsvCell('@x'), "'@x");
    assert.equal(report.tsvCell('a\tb\r\nc'), 'a b c');
    assert.equal(report.tsvCell(-2), '-2'); assert.equal(report.tsvCell(null), ''); assert.equal(report.tsvCell('G1-1'), 'G1-1');
    assert.equal(report.tsvCell(' =HYPERLINK("x")'), "' =HYPERLINK(\"x\")", '先頭の空白のあとの = も式になりうる (Codex R2 P2)');
    assert.equal(report.tsvCell('　+81'), "'　+81", '全角空白のあとも'); assert.equal(report.tsvCell('\t=1'), "' =1", 'タブは空白にしてから見る');
    assert.equal(report.tsvCell('a =b'), 'a =b', '途中の = はそのまま (先頭だけが式になる)');
    const tsv = report.reportTsv({ groups: [{ id: 7, boxes: [{ amazonName: 'P1 - B1', code: 'G1-1', material: '=cmd', weightKg: 12.4, dims: null, qty: 3 }] }],
      changes: [{ group: 'P1_通常', planNo: '通常_2', fnsku: 'X0', sku: '-sku', name: '=cmd', planned: 4, placed: 0, actionJa: 'キャンセル (送りません)', reasonJa: null }],
      expiries: [{ group: 'P1_通常', fnsku: 'X0', sku: '-sku', name: 'a\tb', expiry: '2027-03', qty: 2 }] });
    assert.equal(tsv.box7.split('\n')[1], "P1 - B1\tG1-1\t'=cmd\t12.4\t\t\t\t3");
    assert.equal(tsv.exp.split('\n')[0], 'FNSKU\tSKU\t商品\t期限\t個数\tプラン', '期限: もとの列の順は変えず、プランはうしろに足す');
    assert.equal(tsv.exp.split('\n')[1], "X0\t'-sku\ta b\t2027-03\t2\tP1_通常");
    assert.equal(tsv.chg.split('\n')[1], "P1_通常\t通常_2\tX0\t'-sku\t'=cmd\t4\t0\tキャンセル (送りません)\t", '数量の変更・キャンセルの一覧も同じ守り');
    assert.equal(report.reportTsv({ groups: [], changes: [], expiries: [] }).chg, undefined, '無ければ鍵ごと出さない');
  });
  {
    // プラン外の商品が入っているとき: 予定と比べる数に混ぜない (Codex PR #1307 R1 P2)
    const c13 = db.createRunFromPicking({ pickingRun: { id: 413, delivery_date: '2026-09-26' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-x1', fnsku: 'X0EXT00001', productName: 'プラン外になる商品', qty: '2' }] }], createdBy: 't' });
    const s13 = db.getRunState(c13.runId);
    const b13 = db.createBox({ packGroupId: s13.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c13.runId, rowId: s13.rows[0].id, boxId: b13.boxId, qty: 2, worker: member, deviceKey: 'dev:x', requestId: 'x13' });
    db.getDB().prepare("UPDATE fbx_rows SET match_state = 'picking_only' WHERE id = ?").run(s13.rows[0].id);   // Excel を付けたらプランに無かった体
    t('本社向けまとめ: プラン外の商品は「予定と比べる数」に混ぜず、別に出す', () => {
      const rp = report.buildRunReport(c13.runId);
      assert.equal(rp.totals.planned, 0); assert.equal(rp.totals.placedInPlan, 0); assert.equal(rp.totals.placedExtra, 2); assert.equal(rp.totals.placed, 2);
      const r = rp.groups[0].rows[0]; assert.equal(r.alert, true); assert.ok(r.note.includes('STA のプラン'), r.note);
      const text = report.runDoneText(rp, { link: 'https://x/r', doneBy: 'x' });
      assert.ok(text.includes('🟥 STA のプランに無い商品が 2 個'), text);
      assert.deepEqual(rp.changes.map((x) => [x.action, x.placed]), [['extra', 2]], 'プランに無いのに箱に入っている商品も、直すものとして上に出す');
    });
  }
  {
    // いちばん上の ① プラン × 区分 ごとの箱の数 / ② 数量の変更 (中原さん 2026-09-18)
    const c18 = db.createRunFromPicking({ pickingRun: { id: 418, delivery_date: '2026-09-27' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, fnsku: 'X0PLN00001', productName: '通常の商品', qty: '5' }, { no: 2, fnsku: 'X0PLN00002', productName: '予定どおりの商品', qty: '1' }] },
      { slotId: 'p1_danger', sheet: 'P1_危険物', label: '危険', rows: [{ no: 1, fnsku: 'X0PLN00003', productName: '危険物の商品', qty: '2' }] },
      { slotId: 'p2_large2', sheet: 'P2_大型2', label: '大型2プラン2', rows: [{ no: 1, fnsku: 'X0PLN00004', productName: '大型の商品', qty: '1' }] },
      { slotId: 'zzz', sheet: 'てきとうな名前', label: 'その他', rows: [{ no: 1, fnsku: 'X0PLN00005', productName: '区分を読めないグループ', qty: '1' }] },
    ], createdBy: 't' });
    const s18 = db.getRunState(c18.runId);
    const gOf = (sheet) => s18.groups.find((g) => g.sheet_name === sheet);
    const rOf = (fnsku) => s18.rows.find((r) => r.fnsku === fnsku);
    const put = (sheet, fnsku, qty, kg, id) => {
      const b = db.createBox({ packGroupId: gOf(sheet).id, materialCode: 'box140', worker: member });
      const p = db.addPlacement({ runId: c18.runId, rowId: rOf(fnsku).id, boxId: b.boxId, qty, worker: member, deviceKey: 'dev:p', requestId: id });
      assert.equal(p.ok, true, JSON.stringify(p));
      if (kg != null) db.closeBox({ boxId: b.boxId, measuredKg: kg, worker: staff });
      return b;
    };
    put('P1_通常', 'X0PLN00001', 2, 4.2, 'p18a'); put('P1_通常', 'X0PLN00001', 1, 3.1, 'p18b'); put('P1_通常', 'X0PLN00002', 1, null, 'p18c');
    put('P2_大型2', 'X0PLN00004', 1, 9, 'p18d'); put('てきとうな名前', 'X0PLN00005', 1, 1.5, 'p18e');
    db.createBox({ packGroupId: gOf('P1_危険物').id, materialCode: 'box140', worker: member });   // 中身の無い箱 = 数えない
    db.setRowShortage({ rowId: rOf('X0PLN00001').id, shortageQty: 2, reason: 'damaged', worker: staff });   // 5 個の予定 → 3 個で確定
    t('本社向けまとめ ①: プラン × 区分 (通常・危険物・大型) ごとの箱の数 / 大型2 は使った回だけ列を出す / 区分を読めないグループは別の行', () => {
      const pb = report.buildRunReport(c18.runId).planBoxes;
      assert.deepEqual(pb.kinds, ['通常', '危険物', '大型', '大型2']);
      assert.deepEqual(pb.plans.map((p) => p.plan), ['P1', 'P2']);
      const cell = (plan, kind) => pb.plans.find((p) => p.plan === plan).cells.find((x) => x.kind === kind);
      assert.deepEqual([cell('P1', '通常').boxes, cell('P1', '通常').weightKg, cell('P1', '通常').qty, cell('P1', '通常').noWeight], [3, 7.3, 4, 1]);
      assert.deepEqual([cell('P1', '危険物').exists, cell('P1', '危険物').boxes], [true, 0], 'この回にあるが、送る箱が無い (空の箱は数えない)');
      assert.equal(cell('P1', '大型').exists, false); assert.equal(cell('P2', '通常').exists, false);
      assert.deepEqual([cell('P2', '大型2').boxes, cell('P2', '大型2').weightKg], [1, 9]);
      assert.deepEqual(pb.plans.map((p) => p.boxes), [3, 1], 'プランごとの合計');
      assert.deepEqual(pb.kindTotals.map((x) => [x.kind, x.boxes]), [['通常', 3], ['危険物', 0], ['大型', 0], ['大型2', 1]]);
      assert.deepEqual(pb.others.map((o) => [o.name, o.boxes]), [['てきとうな名前', 1]], '推測で区分に入れない');
      assert.equal(pb.total.boxes, 5, '合計は区分を読めないグループの箱も含む');
    });
    t('本社向けまとめ ②: 不足で確定した商品は「数量を 5 → 3 に変更」/ まだ入れていない商品 (作業中) は件数だけ', () => {
      const rp = report.buildRunReport(c18.runId);
      assert.deepEqual(rp.changes.map((x) => [x.fnsku, x.action, x.planned, x.placed, x.actionJa]), [['X0PLN00001', 'qty', 5, 3, '数量を 5 → 3 に変更']]);
      assert.equal(rp.pendingRows, 1, '危険物の商品 (まだ 1 個も入れていない)');
    });
    t('planKindOf: スロット ID が正。無ければシート名 (P1_危険 も 危険物) / どちらも読めなければ null', () => {
      assert.deepEqual(report.planKindOf({ source_slot_id: 'p2_large', sheet_name: 'なんでも' }), { plan: 'P2', kind: '大型' });
      assert.deepEqual(report.planKindOf({ source_slot_id: 'p1_large2', sheet_name: '' }), { plan: 'P1', kind: '大型2' });
      assert.deepEqual(report.planKindOf({ source_slot_id: 'p1', sheet_name: 'P1_危険' }), { plan: 'P1', kind: '危険物' });
      assert.deepEqual(report.planKindOf({ source_slot_id: null, sheet_name: 'P2_通常' }), { plan: 'P2', kind: '通常' });
      assert.equal(report.planKindOf({ source_slot_id: null, sheet_name: '通常' }), null);
      assert.equal(report.planKindOf({}), null);
    });
  }
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

  // 片方だけの再取得で、取れている側を巻き添えにしない (Codex PR3 R2#1)
  img._resetImageState();
  process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';
  db.upsertProductImage({ fnsku: 'X0FIN00001', asin: 'B0KEEP0001', url: 'https://m.media-amazon.com/images/I/keep.jpg', status: 'ok' });
  db.upsertWeightRef({ fnsku: 'X0FIN00001', asin: 'B0KEEP0001', weightG: null, status: 'none', error: 'Amazon に梱包重量の登録がありません' });
  db.getDB().prepare(`UPDATE fbx_weight_refs SET fetched_at = '2020-01-01T00:00:00.000Z' WHERE fnsku = 'X0FIN00001'`).run();
  db.upsertProductImage({ fnsku: 'X0FIN00002', asin: 'B0KEEP0002', url: 'https://m.media-amazon.com/images/I/keep2.jpg', status: 'ok' });
  db.upsertWeightRef({ fnsku: 'X0FIN00002', asin: 'B0KEEP0002', weightG: 30, raw: '0.03', status: 'ok' });
  img._setAttrsSource(async () => [{ amazon_sku: 'sku-f1', asin: 'B0KEEP0001', fnsku: 'X0FIN00001' }]);
  img._setFetcher(async () => { throw new Error('timeout'); });
  const keep = await img.ensureRunCatalog(c.runId, { force: true });
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  t('単重の再取得が失敗しても、取れている画像は壊さない (Codex PR3 R2#1)', () => {
    assert.equal(keep.total, 1, '単重だけが欠けている 1 商品が対象 (両方 ok の商品は対象外)');
    assert.equal(keep.failed, 1, JSON.stringify(keep));
    assert.equal(db.getDB().prepare(`SELECT status FROM fbx_product_images WHERE fnsku = 'X0FIN00001'`).get().status, 'ok', '画像は ok のまま');
    assert.equal(db.getDB().prepare(`SELECT image_url FROM fbx_product_images WHERE fnsku = 'X0FIN00001'`).get().image_url,
      'https://m.media-amazon.com/images/I/keep.jpg');
    assert.equal(db.getDB().prepare(`SELECT status FROM fbx_weight_refs WHERE fnsku = 'X0FIN00001'`).get().status, 'error');
  });

  // 呼び出しが成功しても、更新するのは欠けている側だけ (Codex PR3 R3#1)
  img._resetImageState();
  process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';
  img._setFetcher(async () => ({ ok: true, result: { image: null, dimensions: { weight: '0.05' } } }));
  const partial = await img.ensureRunCatalog(c.runId, { force: true });
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  t('成功した応答に画像が無くても、既に取れている画像は消さない (Codex PR3 R3#1)', () => {
    assert.equal(partial.total, 1, '単重だけが欠けている 1 商品が対象');
    assert.equal(partial.weighed, 1);
    assert.equal(partial.none, 0, '画像側は今回の更新対象ではないので数えない');
    assert.equal(db.getDB().prepare(`SELECT image_url FROM fbx_product_images WHERE fnsku = 'X0FIN00001'`).get().image_url,
      'https://m.media-amazon.com/images/I/keep.jpg');
    assert.equal(db.getRunState(c.runId).weights.X0FIN00001.unitG, 50, '単重側は更新される (0.05kg → 50g)');
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
    { no: 1, sku: 'sku-w1', fnsku: 'X0WGT00001', productName: '重さのわかる商品', qty: '30' },
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
    assert.equal(db.revokeWeightMeasurement({ id: m.id, runId: c.runId, worker: member }).ok, true, '同じ回で登録した記録は作業者が取り消せる');
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.unitG, 200, '取り消したら参考値に戻る');
    assert.equal(db.revokeWeightMeasurement({ id: m.id, runId: c.runId, worker: member }).error, 'already_revoked');
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
    db.revokeWeightMeasurement({ id: m.id, runId: c.runId, worker: member });
  });

  t('rebuildWeightCurrent: 壊れた値・孤児行のどちらも起動時に直す (Codex PR3 R1#5 / R2#2)', () => {
    db.getDB().prepare(`UPDATE fbx_weight_current SET unit_g = 99999, source = 'catalog' WHERE fnsku = 'X0WGT00001'`).run();
    db.getDB().prepare(`DELETE FROM fbx_weight_current WHERE fnsku = 'X0WGT00002'`).run();
    // 元データがどこにも無い孤児行 (projection にだけ残ってしまったもの)
    db.getDB().prepare(`INSERT INTO fbx_weight_current (fnsku, unit_g, source, updated_at) VALUES ('X0ORPHAN01', 123, 'catalog', ?)`).run(new Date().toISOString());
    const n = db.rebuildWeightCurrent();
    assert.ok(n >= 3);
    assert.equal(db.getRunState(c.runId).weights.X0WGT00001.unitG, 200, '参考値から作り直す');
    assert.equal(db.getRunState(c.runId).weights.X0WGT00002, undefined, '元データが無い商品は採用値も持たない');
    assert.equal(db.getDB().prepare(`SELECT COUNT(*) c FROM fbx_weight_current WHERE fnsku = 'X0ORPHAN01'`).get().c, 0, '孤児行は消える');
  });

  t('実測の取消: 別の回・終わった回の記録は職員のみ (単重は全回共通のマスタ — Codex PR3 R2#4)', () => {
    const mine = db.addWeightMeasurement({ fnsku: 'X0WGT00001', sampleQty: 4, totalG: 800, runId: c.runId, worker: member });
    assert.equal(mine.ok, true);
    const doneRun = db.getRunBySource(400);
    const old = db.getDB().prepare(`INSERT INTO fbx_weight_measurements (fnsku, sample_qty, total_g, unit_g, method, run_id, measured_at)
      VALUES ('X0WGT00001', 1, 111, 111, 'scale', ?, ?)`).run(doneRun.id, new Date().toISOString());
    const oldId = Number(old.lastInsertRowid);
    assert.equal(db.revokeWeightMeasurement({ id: oldId, runId: c.runId, worker: member }).error, 'staff_required');
    assert.equal(db.revokeWeightMeasurement({ id: mine.id, runId: 999999, worker: member }).error, 'staff_required', '別の回を名乗っても通さない');
    assert.equal(db.revokeWeightMeasurement({ id: oldId, byStaff: true, worker: staff }).ok, true, '職員なら過去回も取り消せる');
    assert.equal(db.revokeWeightMeasurement({ id: mine.id, runId: c.runId, worker: member }).ok, true);
  });

  t('箱を開け直すと上限超えの承認も消える (Codex PR3 R2#3)', () => {
    const bxR = db.createBox({ packGroupId: gid, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c.runId, rowId: rA.id, boxId: bxR.boxId, qty: 1, worker: member, deviceKey: 'dev:w', requestId: 'w9' });
    assert.equal(db.closeBox({ boxId: bxR.boxId, measuredKg: 31, worker: member, staffApproved: true, approvedBy: '職員B' }).ok, true);
    assert.equal(db.getBox(bxR.boxId).limit_override_by, '職員B');
    assert.equal(db.reopenBox({ boxId: bxR.boxId, reason: '詰め直し', worker: staff }).ok, true);
    const after = db.getBox(bxR.boxId);
    assert.equal(after.limit_override_by, null);
    assert.equal(after.limit_override_at, null);
    assert.equal(after.est_weight_g_at_close, null);
  });
}

console.log('■ 積み方区分 (土台・重い): 有効値 = 手動 > 重さから自動 > 未設定 / 取込 / 履歴');
{
  const c = db.createRunFromPicking({ pickingRun: { id: 600, delivery_date: '2026-10-10' }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
    { no: 1, sku: 'sku-pk-heavy', fnsku: 'X0PCK00001', productName: '1kg の粉', qty: '5' },
    { no: 2, sku: 'sku-pk-light', fnsku: 'X0PCK00002', productName: '10ml オイル', qty: '5' },
    { no: 3, sku: 'sku-pk-noweight', fnsku: 'X0PCK00003', productName: '重さ不明', qty: '5' },
  ] }], createdBy: 't' });
  db.upsertWeightRef({ fnsku: 'X0PCK00001', asin: 'B0PK1', weightG: 1000, raw: '1.00', status: 'ok' });
  db.upsertWeightRef({ fnsku: 'X0PCK00002', asin: 'B0PK2', weightG: 30, raw: '0.03', status: 'ok' });

  t('基準が未設定なら「重い」は自動で付かない (渡した FNSKU は全部 cls:null で返る = has() で判定させない)', () => {
    assert.equal(db.getWeightRules().heavy_min_g, null);
    const m = db.effectivePackingClass(['X0PCK00001', 'x0pck00002', 'X0PCK00003', '']);
    assert.equal(m.size, 3);
    assert.equal(m.get('X0PCK00001').cls, null);
    assert.equal(m.get('X0PCK00001').unitG, 1000);
    assert.equal(m.get('X0PCK00002').cls, null, '小文字で渡しても正規化');
    assert.equal(m.get('X0PCK00003').unitG, null);
    assert.equal(db.effectivePackingClass([]).size, 0);
  });
  t('setWeightRules: 重いの基準を入れる / キー省略は引き継ぐ / null で止める / 不正値は bad_value', () => {
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: 500, actor: 't' }).heavyMinG, 500);
    assert.equal(db.getWeightRules().heavy_min_g, 500);
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, actor: 't' }).heavyMinG, 500, 'キーを渡さなければ現行値を引き継ぐ');
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: -1, actor: 't' }).error, 'bad_value');
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: 'abc', actor: 't' }).error, 'bad_value');
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: 200000, actor: 't' }).error, 'bad_value');
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: null, actor: 't' }).heavyMinG, null);
    assert.equal(db.getWeightRules().heavy_min_g, null);
    assert.equal(db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: '500', actor: 't' }).heavyMinG, 500);
  });
  t('基準 500g: 1kg は自動で「重い」(source=weight)、30g と単重不明は未設定。getRunState.packing にも出る。自動は保存しない', () => {
    const m = db.effectivePackingClass(['X0PCK00001', 'X0PCK00002', 'X0PCK00003']);
    assert.deepEqual([m.get('X0PCK00001').cls, m.get('X0PCK00001').source], ['heavy', 'weight']);
    assert.equal(m.get('X0PCK00002').cls, null);
    assert.equal(m.get('X0PCK00003').cls, null);
    const st = db.getRunState(c.runId);
    assert.equal(st.packing.X0PCK00001.cls, 'heavy');
    assert.equal(st.packing.X0PCK00003.cls, null);
    assert.equal(st.packing.X0PCK00003.heavyMinG, 500);
    assert.deepEqual(st.packingRules, { heavyMinG: 500 });
    assert.equal(db.getDB().prepare('SELECT COUNT(*) c FROM fbx_product_flags').get().c, 0, '自動の重いは保存しない (派生)');
  });
  t('実測がちょうど基準と同じなら「重い」(≥)。実測を取り消して参考値 (30g) に戻れば外れる', () => {
    const m = db.addWeightMeasurement({ fnsku: 'X0PCK00002', sampleQty: 2, totalG: 1000, worker: member, runId: c.runId });
    assert.equal(m.unitG, 500);
    assert.equal(db.effectivePackingClass(['X0PCK00002']).get('X0PCK00002').cls, 'heavy');
    db.revokeWeightMeasurement({ id: m.id, runId: c.runId, worker: member });
    assert.equal(db.effectivePackingClass(['X0PCK00002']).get('X0PCK00002').cls, null);
  });
  t('setPackingClass: 検証 (FNSKU 空 / 不正値 / 回なし / 回が active でない / 回にない商品)', () => {
    assert.equal(db.setPackingClass({ fnsku: '', cls: 'base', worker: member }).error, 'bad_fnsku');
    assert.equal(db.setPackingClass({ fnsku: 'X0PCK00002', cls: 'top', worker: member }).error, 'bad_class');
    assert.equal(db.setPackingClass({ fnsku: 'X0PCK00002', cls: 'base', runId: 999999, worker: member }).error, 'run_required');
    assert.equal(db.setPackingClass({ fnsku: 'X0NOSUCH99', cls: 'base', runId: c.runId, worker: member }).error, 'not_in_run');
    const c2 = db.createRunFromPicking({ pickingRun: { id: 601 }, planSheets: [{ slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 's', fnsku: 'X0PCK00009', productName: 'x', qty: '1' }] }], createdBy: 't', activate: false });
    assert.equal(db.setPackingClass({ fnsku: 'X0PCK00009', cls: 'base', runId: c2.runId, worker: member }).error, 'run_not_active');
    assert.equal(db.getDB().prepare('SELECT COUNT(*) c FROM fbx_product_flags').get().c, 0, '弾いたものは何も書かない');
  });
  t('現場が「土台」を付ける → manual が効く。回の行から seller_sku を補完し、履歴が残る。同じ値の再設定は履歴を増やさない', () => {
    const r = db.setPackingClass({ fnsku: 'x0pck00002', cls: 'base', runId: c.runId, worker: member, deviceLabel: 'iPad1' });
    assert.equal(r.ok, true);
    assert.deepEqual([r.effective.cls, r.effective.source, r.effective.updatedBy], ['base', 'manual', 'りようしゃ']);
    const row = db.getProductFlags(['X0PCK00002'])[0];
    assert.equal(row.seller_sku, 'sku-pk-light', '回の行から SKU を補完');
    assert.equal(row.source, 'manual');
    assert.equal(row.device_label, 'iPad1');
    const again = db.setPackingClass({ fnsku: 'X0PCK00002', cls: 'base', runId: c.runId, worker: member });
    assert.equal(again.unchanged, true);
    const hist = db.listPackingClassChanges(10).filter((h) => h.fnsku === 'X0PCK00002');
    assert.equal(hist.length, 1);
    assert.deepEqual([hist[0].from, hist[0].to, hist[0].by, hist[0].productName, hist[0].runId], [null, 'base', 'りようしゃ', '10ml オイル', c.runId]);
  });
  t('手動の「通常」は自動の「重い」を止める。未設定に戻す (null) と自動の重いに戻る', () => {
    assert.equal(db.setPackingClass({ fnsku: 'X0PCK00001', cls: 'normal', runId: c.runId, worker: member }).effective.cls, 'normal');
    assert.equal(db.effectivePackingClass(['X0PCK00001']).get('X0PCK00001').source, 'manual');
    const back = db.setPackingClass({ fnsku: 'X0PCK00001', cls: null, runId: c.runId, worker: member });
    assert.equal(back.ok, true);
    assert.deepEqual([back.effective.cls, back.effective.source], ['heavy', 'weight']);
    const hist = db.listPackingClassChanges(10).filter((h) => h.fnsku === 'X0PCK00001');
    assert.deepEqual(hist.map((h) => [h.from, h.to]), [['normal', null], [null, 'normal']]);
  });
  t('管理画面からは回なしで付けられる (runId 省略。誰が = session ラベル)', () => {
    const r = db.setPackingClass({ fnsku: 'X0ANY00001', cls: 'base', deviceLabel: 'session:hq@test' });
    assert.equal(r.ok, true);
    assert.equal(db.getProductFlags(['X0ANY00001'])[0].updated_by, 'session:hq@test');
    assert.equal(db.getProductFlags().length, 3, '全件 (X0PCK00001 の NULL 行も含む)');
  });
  t('土台シートからの取込: 1つに決まる SKU だけ base/sheet_import。手動は保持、0件・複数件は一覧、再実行は冪等', () => {
    const idx = { 'sku-pk-noweight': ['X0PCK00003'], 'sku-pk-light': ['X0PCK00002'], 'sku-multi': ['X0M0000001', 'X0M0000002'], 'sku-new': ['X0NEW00001'] };
    const r1 = db.importPackingClassFromSkus({ skus: ['sku-pk-noweight', 'sku-pk-light', 'sku-multi', 'sku-unknown', 'sku-new', 'sku-new', ''], fnskusOf: (sk) => idx[sk] || [], actor: 'session:hq@test' });
    assert.equal(r1.total, 5, '重複と空は数えない');
    assert.equal(r1.imported, 2, 'noweight と new');
    assert.equal(r1.keptManual, 1, 'light は手動 (base) を保持');
    assert.deepEqual(r1.unresolved, ['sku-unknown']);
    assert.deepEqual(r1.ambiguous, [{ sku: 'sku-multi', fnskus: ['X0M0000001', 'X0M0000002'] }]);
    const f3 = db.getProductFlags(['X0PCK00003'])[0];
    assert.deepEqual([f3.packing_class, f3.source, f3.seller_sku, f3.updated_by], ['base', 'sheet_import', 'sku-pk-noweight', 'session:hq@test']);
    assert.equal(db.effectivePackingClass(['X0PCK00003']).get('X0PCK00003').source, 'sheet_import');
    const r2 = db.importPackingClassFromSkus({ skus: ['sku-pk-noweight', 'sku-new'], fnskusOf: (sk) => idx[sk] || [], actor: 'session:hq@test' });
    assert.deepEqual([r2.imported, r2.unchanged], [0, 2], '再実行は何もしない');
    assert.equal(db.listPackingClassChanges(50).filter((h) => h.via === 'sheet_import').length, 2, '取込の履歴は初回の 2 件だけ');
    // 取込後に現場が上書き → manual になり、次の取込では保持される
    db.setPackingClass({ fnsku: 'X0PCK00003', cls: 'normal', runId: c.runId, worker: member });
    const r3 = db.importPackingClassFromSkus({ skus: ['sku-pk-noweight'], fnskusOf: (sk) => idx[sk] || [] });
    assert.equal(r3.keptManual, 1);
    assert.equal(db.effectivePackingClass(['X0PCK00003']).get('X0PCK00003').cls, 'normal');
  });
  t('listKnownSkuFnsku: この DB の行から seller_sku↔fnsku を返す (取込の解決に使う)', () => {
    const known = db.listKnownSkuFnsku();
    assert.ok(known.some((k) => k.seller_sku === 'sku-pk-heavy' && k.fnsku === 'X0PCK00001'));
  });
  // 後続のテストに影響しないよう基準を戻す
  db.setWeightRules({ targetG: 28000, limitG: 30000, heavyMinG: null, actor: 't' });
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

// ───────── PR2.6: fbx_row_work に確認担当の由来を足す移行 (Codex R2 medium#3) ─────────
console.log('■ PR2.6: fbx_row_work の由来列 ALTER 移行');
const mdb3 = new Database(path.join(tmp, 'migrate-rowwork.db'));
mdb3.pragma('journal_mode = WAL'); mdb3.pragma('foreign_keys = ON');
db.createTables(mdb3);
// この変更より前の fbx_row_work (由来の2列が無い) に戻して、稼働中の回のデータを入れる
mdb3.pragma('foreign_keys = OFF');
mdb3.exec(`DROP TABLE fbx_row_work;
  CREATE TABLE fbx_row_work (
    row_id INTEGER PRIMARY KEY REFERENCES fbx_rows(id),
    label_worker TEXT, check_worker TEXT,
    shortage_qty INTEGER CHECK (shortage_qty IS NULL OR shortage_qty > 0),
    shortage_reason TEXT, shortage_by TEXT, shortage_detail TEXT, updated_at TEXT);`);
mdb3.pragma('foreign_keys = ON');
mdb3.exec(`INSERT INTO fbx_runs (id, source_run_id, title, status, created_at) VALUES (1, 1, 'r', 'active', 'now');
  INSERT INTO fbx_excel_files (id, run_id, stored_path, sha256, fingerprint, uploaded_at) VALUES (1, 1, '/x', 'h', 'f', 'now');
  INSERT INTO fbx_pack_groups (id, run_id, excel_file_id, sheet_name, packing_group_id, display_name) VALUES (1, 1, 1, 's', 'pg1', 'G1');
  INSERT INTO fbx_rows (id, run_id, pack_group_id, excel_row, seller_sku, fnsku, planned_qty) VALUES (1, 1, 1, 6, 'sku', 'X1', 5);
  INSERT INTO fbx_row_work (row_id, label_worker, check_worker, shortage_qty, updated_at) VALUES (1, 'たなか', 'さとう', 2, 'then');`);
t('稼働中の回のある DB に由来の2列を足す: 既存値は保持され source は NULL (= 触らない印)', () => {
  assert.equal(new Set(mdb3.prepare('PRAGMA table_info(fbx_row_work)').all().map((c) => c.name)).has('check_worker_source'), false);
  db.createTables(mdb3);
  const cols = new Set(mdb3.prepare('PRAGMA table_info(fbx_row_work)').all().map((c) => c.name));
  assert.ok(cols.has('check_worker_source')); assert.ok(cols.has('check_worker_placement_id'));
  const rw = mdb3.prepare('SELECT * FROM fbx_row_work WHERE row_id = 1').get();
  assert.equal(rw.check_worker, 'さとう');        // 稼働中の回の記録は消えない
  assert.equal(rw.label_worker, 'たなか');
  assert.equal(rw.shortage_qty, 2);
  assert.equal(rw.check_worker_source, null);     // NULL = 由来不明 → 自動では動かさない
  assert.equal(rw.updated_at, 'then');            // ALTER だけ = 既存行を書き直さない
  assert.deepEqual(mdb3.prepare('PRAGMA foreign_key_check').all(), []);
  db.createTables(mdb3);                          // 2回目は何もしない (冪等)
  assert.equal(mdb3.prepare('SELECT COUNT(*) c FROM fbx_row_work').get().c, 1);
});
mdb3.close();

console.log('■ 積み方区分: 既存 DB への ALTER (heavy_min_g) と新テーブル (fbx_product_flags) の追加');
const mdb4 = new Database(path.join(tmp, 'migrate-packing.db'));
mdb4.pragma('journal_mode = WAL'); mdb4.pragma('foreign_keys = ON');
db.createTables(mdb4);
// この変更より前の形 (重いの基準の列が無い・商品フラグの表が無い) に戻し、本社が決めたルール行を入れる
mdb4.exec(`DROP TABLE fbx_product_flags;
  DROP TABLE fbx_weight_rules;
  CREATE TABLE fbx_weight_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, target_g INTEGER NOT NULL CHECK (target_g > 0),
    limit_g INTEGER NOT NULL CHECK (limit_g > 0), effective_from TEXT NOT NULL, updated_by TEXT, updated_at TEXT NOT NULL);
  INSERT INTO fbx_weight_rules (target_g, limit_g, effective_from, updated_by, updated_at) VALUES (27000, 29000, '2026-09-01T00:00:00.000Z', 'hq', '2026-09-01T00:00:00.000Z');`);
t('稼働中の DB に heavy_min_g 列と fbx_product_flags を足す: 既存ルール行は保持され基準は NULL (= 自動では付けない)', () => {
  assert.equal(new Set(mdb4.prepare('PRAGMA table_info(fbx_weight_rules)').all().map((c) => c.name)).has('heavy_min_g'), false);
  db.createTables(mdb4);
  assert.ok(new Set(mdb4.prepare('PRAGMA table_info(fbx_weight_rules)').all().map((c) => c.name)).has('heavy_min_g'));
  const r = db.getWeightRules(mdb4);
  assert.equal(r.limit_g, 29000); assert.equal(r.heavy_min_g, null);
  assert.ok(mdb4.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='fbx_product_flags'`).get());
  assert.equal(db.effectivePackingClass(['X1'], mdb4).get('X1').cls, null);
  db.createTables(mdb4);   // 冪等
  assert.equal(mdb4.prepare('SELECT COUNT(*) c FROM fbx_weight_rules').get().c, 1);
});
mdb4.close();

// ───────── 期限管理商品の判定 (中原さん 2026-09-18) ─────────
console.log('■ 期限管理商品の判定: 正本 (ロジザード商品マスタ) を引いて、期限の入力欄を出す商品を決める');
{
  const { expiryManagedByCode } = await import('../apps/warehouse-mirror/expiry-managed.js');
  // mirror DB の代わり (入荷受付チェックと同じ 2 つの表だけ作る)
  const mir = new Database(path.join(tmp, 'mirror-like.db'));
  mir.exec(`CREATE TABLE f_inbound_check_product_flags (code_key TEXT PRIMARY KEY, expiry_managed INTEGER NOT NULL,
      source TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT);
    CREATE TABLE mirror_logizard_stock (商品ID TEXT NOT NULL, 有効期限 TEXT, 在庫数 INTEGER NOT NULL);
    INSERT INTO f_inbound_check_product_flags VALUES ('food01', 1, 'logizard', 'x', NULL), ('tool01', 0, 'logizard', 'x', NULL),
      ('hand01', 1, 'manual', 'x', NULL), ('odd01', 0, 'logizard', 'x', NULL);
    INSERT INTO mirror_logizard_stock VALUES ('FOOD01', '2027-03-31', 5), ('tool01', '', 3), ('odd01', '2027-06-30', 2),
      ('gone01', '2027-01-31', 0), ('stockonly', '2028-01-31', 4);`);
  t('判定: マスタが「あり」→ 期限管理 / 「無し」→ 期限管理でない / 手で直した分もそのまま', () => {
    const m = expiryManagedByCode(['food01', 'tool01', 'hand01'], mir);
    assert.deepEqual([m.get('food01').managed, m.get('food01').source], [true, 'stock']);
    assert.deepEqual([m.get('tool01').managed, m.get('tool01').source], [false, 'logizard']);
    assert.deepEqual([m.get('hand01').managed, m.get('hand01').source], [true, 'manual']);
  });
  t('🚨 マスタに無い商品は「期限管理でない」ではなく「分からない」(null) — 黙って入力欄を消さない', () => {
    const m = expiryManagedByCode(['NOPE01', 'gone01'], mir);
    assert.deepEqual([m.get('nope01').managed, m.get('nope01').source], [null, 'unknown']);
    assert.equal(m.get('gone01').managed, null, '在庫が 0 の行の期限では決めない (入荷受付チェックと同じ)');
    assert.equal(expiryManagedByCode([], mir).size, 0);
    assert.equal(expiryManagedByCode(['x'], new Database(':memory:')).get('x').managed, null, '表がまだ無い mirror でも false に倒さない');
  });
  t('🚨 設定が「無し」でも実物 (在庫) に期限が入っていれば期限管理に倒す / マスタに無くても在庫に期限があれば期限管理', () => {
    const m = expiryManagedByCode(['odd01', 'stockonly'], mir);
    assert.deepEqual([m.get('odd01').managed, m.get('odd01').source], [true, 'stock']);
    assert.deepEqual([m.get('stockonly').managed, m.get('stockonly').source], [true, 'stock']);
  });
  mir.close();

  const expiry = await import('../apps/fba-box/expiry.js');
  t('セット商品 (構成品が複数) の決め方: 1つでも期限管理なら期限管理 / 分からないが混じれば分からない', () => {
    assert.deepEqual(expiry.decideForCodes([{ managed: false, source: 'logizard' }, { managed: true, source: 'stock' }]), { requires: 1, source: 'stock' });
    assert.deepEqual(expiry.decideForCodes([{ managed: false, source: 'logizard' }, { managed: null, source: 'unknown' }]), { requires: null, source: 'unknown' });
    assert.deepEqual(expiry.decideForCodes([{ managed: false, source: 'logizard' }, { managed: false, source: 'manual' }]), { requires: 0, source: 'manual' });
    assert.deepEqual(expiry.decideForCodes([]), { requires: null, source: 'no_code' }, '商品コードにたどり着けない = 分からない');
  });

  const c18 = db.createRunFromPicking({ pickingRun: { id: 430, delivery_date: '2026-09-30' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
      { no: 1, sku: 'sku-food', fnsku: 'X0EXPMGD01', productName: '期限管理の商品', qty: '4' },
      { no: 2, sku: 'sku-tool', fnsku: 'X0EXPNON01', productName: '期限管理でない商品', qty: '2' },
      { no: 3, fnsku: 'X0EXPFNS01', productName: 'SKU は FNSKU から引く商品', qty: '1' },
      { no: 4, sku: 'sku-none', fnsku: 'X0EXPUNK01', productName: '紐付けが無い商品', qty: '1' },
    ] }], createdBy: 't' });
  expiry._setExpirySource(async () => ({
    skuToCodes: (skus) => new Map(skus.filter((s) => s !== 'sku-none').map((s) => [s, [{ 'sku-food': 'food01', 'sku-tool': 'tool01', 'sku-byfnsku': 'hand01' }[s]]])),
    expiryManagedByCode: (codes) => new Map(codes.map((c) => [c, { food01: { managed: true, source: 'logizard' },
      tool01: { managed: false, source: 'logizard' }, hand01: { managed: true, source: 'manual' } }[c] || { managed: null, source: 'unknown' }])),
    fbaSkuAttrs: () => [{ amazon_sku: 'sku-byfnsku', fnsku: 'X0EXPFNS01' }],
  }));
  await ta('納品回の行に判定を焼く: 行の SKU / FNSKU から引いた SKU のどちらでも引ける。引けない行は「分からない」のまま', async () => {
    const r = await expiry.ensureRunExpiryFlags(c18.runId);
    assert.deepEqual([r.ok, r.checked, r.managed, r.notManaged, r.unknown], [true, 4, 2, 1, 1], JSON.stringify(r));
    const rows = db.getRunState(c18.runId).rows;
    const by = (fn) => rows.find((x) => x.fnsku === fn);
    assert.deepEqual([by('X0EXPMGD01').requires_expiry, by('X0EXPMGD01').expiry_source], [1, 'logizard']);
    assert.deepEqual([by('X0EXPNON01').requires_expiry, by('X0EXPNON01').expiry_source], [0, 'logizard']);
    assert.deepEqual([by('X0EXPFNS01').requires_expiry, by('X0EXPFNS01').expiry_source], [1, 'manual'], 'FNSKU → SKU → 商品コード');
    assert.deepEqual([by('X0EXPUNK01').requires_expiry, by('X0EXPUNK01').expiry_source], [null, 'unknown'],
      'SKU はあるが商品コードを引けない = 分からない (no_code = SKU そのものが無い行)');
    assert.deepEqual(expiry.expirySummary(c18.runId),
      { managed: 2, notManaged: 1, unknown: 1, unresolved: 0, bySource: { logizard: 2, manual: 1, unknown: 1 } });
  });
  await ta('🚨 商品コードを引けない SKU を黙って落とさない (Codex R1 #1: 落とすと残りだけで 0 に確定して期限欄が消える)', async () => {
    const c21 = db.createRunFromPicking({ pickingRun: { id: 433, delivery_date: '2026-10-03' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
        { no: 1, sku: 'sku-unresolved', fnsku: 'X0EXPMIX01', productName: '行の SKU は引けず、FNSKU 側の SKU は「管理でない」', qty: '1' },
        { no: 2, sku: 'sku-unresolved', fnsku: 'X0EXPMIX02', productName: '行の SKU は引けず、FNSKU 側の SKU は「管理」', qty: '1' },
      ] }], createdBy: 't' });
    expiry._setExpirySource(async () => ({
      skuToCodes: (skus) => new Map(skus.filter((s) => s !== 'sku-unresolved').map((s) => [s, [s === 'sku-mgd' ? 'food01' : 'tool01']])),
      expiryManagedByCode: (codes) => new Map(codes.map((c) => [c, c === 'food01' ? { managed: true, source: 'logizard' } : { managed: false, source: 'logizard' }])),
      fbaSkuAttrs: () => [{ amazon_sku: 'sku-non', fnsku: 'X0EXPMIX01' }, { amazon_sku: 'sku-mgd', fnsku: 'X0EXPMIX02' }],
    }));
    await expiry.ensureRunExpiryFlags(c21.runId);
    const rows = db.getRunState(c21.runId).rows;
    const by = (fn) => rows.find((x) => x.fnsku === fn);
    assert.deepEqual([by('X0EXPMIX01').requires_expiry, by('X0EXPMIX01').expiry_source], [null, 'unknown'],
      '引けない SKU が混じっていたら「分からない」(0 にしない)');
    assert.equal(by('X0EXPMIX02').requires_expiry, 1, '1 つでも「期限管理」と分かれば期限管理 (安全側)');
  });
  await ta('2 回目は判定済みの行を見ない (force で見直す)', async () => {
    assert.equal((await expiry.ensureRunExpiryFlags(c18.runId)).checked, 0);
    assert.equal((await expiry.ensureRunExpiryFlags(c18.runId, { force: true })).checked, 4);
  });
  await ta('🚨 SKU 属性 (FNSKU→SKU) が読めないときは何も焼かない (Codex R2 #2: 焼くと 0 に確定し、直っても焼き直さない)', async () => {
    const c23 = db.createRunFromPicking({ pickingRun: { id: 435, delivery_date: '2026-10-05' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
        { no: 1, sku: 'sku-non', fnsku: 'X0EXPATT01', productName: '行の SKU は管理でない・FNSKU 側に管理の SKU がある', qty: '1' }] }], createdBy: 't' });
    const codes = { 'sku-non': 'tool01', 'sku-mgd': 'food01' };
    const mk = (fbaSkuAttrs) => ({
      skuToCodes: (skus) => new Map(skus.map((s) => [s, [codes[s]]])),
      expiryManagedByCode: (cs) => new Map(cs.map((c) => [c, c === 'food01' ? { managed: true, source: 'logizard' } : { managed: false, source: 'logizard' }])),
      fbaSkuAttrs,
    });
    expiry._setExpirySource(async () => mk(() => { throw new Error('fba.db down'); }));
    const bad = await expiry.ensureRunExpiryFlags(c23.runId);
    assert.equal(bad.ok, false); assert.equal(bad.skipped, 1); assert.ok(bad.error.includes('sku属性'), bad.error);
    assert.deepEqual(db.getRunState(c23.runId).rows.map((r) => [r.requires_expiry, r.expiry_source]), [[null, null]], '焼かない = まだ判定していないまま');
    expiry._setExpirySource(async () => mk(() => [{ amazon_sku: 'sku-mgd', fnsku: 'X0EXPATT01' }]));
    const good = await expiry.ensureRunExpiryFlags(c23.runId);
    assert.equal(good.checked, 1, '直ったら次に開いたときに焼き直す');
    assert.equal(db.getRunState(c23.runId).rows[0].requires_expiry, 1, 'FNSKU 側の SKU で期限管理と分かる');
  });
  await ta('🚨 Excel 添付で SKU が入った・変わった行は判定をやり直す (Codex R1 #2: 古い判定のままだと期限欄がたたまれたまま)', async () => {
    // picking で SKU 無し → 判定できない (no_code) → Excel 添付で期限管理商品の SKU が入る
    const c22 = db.createRunFromPicking({ pickingRun: { id: 434, delivery_date: '2026-10-04' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
        { no: 1, fnsku: f1rows[0].fnsku, productName: 'A', qty: '5' }, { no: 2, fnsku: f1rows[1].fnsku, productName: 'B', qty: '3' }] }], createdBy: 't' });
    expiry._setExpirySource(async () => ({
      skuToCodes: (skus) => new Map(skus.map((s) => [s, ['food01']])),
      expiryManagedByCode: (codes) => new Map(codes.map((c) => [c, { managed: true, source: 'logizard' }])),
      fbaSkuAttrs: () => [],
    }));
    await expiry.ensureRunExpiryFlags(c22.runId);
    assert.deepEqual(db.getRunState(c22.runId).rows.map((r) => [r.requires_expiry, r.expiry_source]), [[null, 'no_code'], [null, 'no_code']],
      'SKU が無いので判定できない');
    const att = db.attachExcelToRun({ runId: c22.runId, parsed: ing1.parsed, file: { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' });
    assert.equal(att.ok, true, JSON.stringify(att));
    assert.deepEqual(db.getRunState(c22.runId).rows.map((r) => r.expiry_source), [null, null], 'SKU が入った行は「まだ判定していない」に戻る');
    await expiry.ensureRunExpiryFlags(c22.runId);
    assert.deepEqual(db.getRunState(c22.runId).rows.map((r) => r.requires_expiry), [1, 1], '焼き直すと期限管理になる');
    // 同じ Excel をもう一度添付しても、SKU が変わっていない行は判定を消さない (毎回まっさらにしない)
    assert.equal(db.attachExcelToRun({ runId: c22.runId, parsed: ing1.parsed, file: { originalName: 'p1.xlsx', storedPath: ing1.storedPath, sha256: ing1.sha256 }, actor: 't' }).ok, true);
    assert.deepEqual(db.getRunState(c22.runId).rows.map((r) => r.expiry_source), ['logizard', 'logizard']);
  });
  await ta('🚨 mirror が読めなくても納品回は止まらない (全部「分からない」= いままでどおり期限欄が出る)', async () => {
    const c19 = db.createRunFromPicking({ pickingRun: { id: 431, delivery_date: '2026-10-01' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [{ no: 1, sku: 'sku-food', fnsku: 'X0EXPERR01', productName: 'mirror が落ちている', qty: '1' }] }], createdBy: 't' });
    expiry._setExpirySource(async () => { throw new Error('mirror なし'); });
    const r = await expiry.ensureRunExpiryFlags(c19.runId);
    assert.equal(r.ok, false); assert.ok(r.error.includes('mirror'));
    const row = db.getRunState(c19.runId).rows[0];
    assert.deepEqual([row.requires_expiry, row.expiry_source], [null, null], '判定していない印 (次に開いたときもう一度やる)');
    assert.equal(expiry.expirySummary(c19.runId).unresolved, 1);
  });

  // 出荷前チェック: 期限管理商品なのに期限が空
  {
    expiry._setExpirySource(async () => ({
      skuToCodes: (skus) => new Map(skus.map((s) => [s, ['food01']])),
      expiryManagedByCode: (codes) => new Map(codes.map((c) => [c, { managed: true, source: 'logizard' }])),
      fbaSkuAttrs: () => [],
    }));
    const c20 = db.createRunFromPicking({ pickingRun: { id: 432, delivery_date: '2026-10-02' }, planSheets: [
      { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
        { no: 1, sku: 'sku-a', fnsku: 'X0EXPCHK01', productName: '期限を入れた商品', qty: '2' },
        { no: 2, sku: 'sku-b', fnsku: 'X0EXPCHK02', productName: '期限を入れ忘れた商品', qty: '2' }] }], createdBy: 't' });
    await expiry.ensureRunExpiryFlags(c20.runId);
    const s20 = db.getRunState(c20.runId);
    const b20 = db.createBox({ packGroupId: s20.groups[0].id, materialCode: 'box140', worker: member });
    db.addPlacement({ runId: c20.runId, rowId: s20.rows[0].id, boxId: b20.boxId, qty: 2, expiry: '2027-05-31', worker: member, deviceKey: 'dev:x', requestId: 'ex20a' });
    db.addPlacement({ runId: c20.runId, rowId: s20.rows[1].id, boxId: b20.boxId, qty: 2, worker: member, deviceKey: 'dev:x', requestId: 'ex20b' });
    t('出荷前チェック: 期限管理商品なのに期限が空だと警告 (止めない — 本社が STA に入れる前に気づくため)', () => {
      const w = db.exportReadiness(c20.runId).warnings.find((x) => x.code === 'expiry_missing');
      assert.ok(w, '警告が出る');
      assert.deepEqual(w.rows.map((r) => r.fnsku), ['X0EXPCHK02'], '期限を入れた商品は出さない');
      assert.ok(!db.exportReadiness(c20.runId).blockers.some((x) => x.code === 'expiry_missing'), 'blocker にはしない');
    });
    t('本社向け一覧: 期限管理商品なのに期限が空を ③ に出す', () => {
      const rep = report.buildRunReport(c20.runId);
      assert.deepEqual(rep.expiryMissing.map((m) => [m.fnsku, m.placed]), [['X0EXPCHK02', 2]]);
      assert.deepEqual(rep.expiries.map((e) => e.fnsku), ['X0EXPCHK01']);
      assert.equal(report.buildRunReport(c18.runId).expiryMissing.length, 0, '箱に入れていない商品は出さない');
    });
  }
  expiry._setExpirySource(null);
}

console.log('■ 1 つの商品を複数の箱へ分けて入れる (中原さん 2026-09-23: 商品番号 → 何番の箱に何個)');
{
  const cs = db.createRunFromPicking({ pickingRun: { id: 923, delivery_date: '2026-09-30' }, planSheets: [
    { slotId: 'p1_normal', sheet: 'P1_通常', label: '通常', rows: [
      { no: 1, fnsku: 'X0SPLIT001', productName: '分ける商品', qty: '20' },
      { no: 2, fnsku: 'X0SPLIT002', productName: 'ほかの商品', qty: '4' }] },
    { slotId: 'p1_danger', sheet: 'P1_危険物', label: '危険', rows: [{ no: 1, fnsku: 'X0SPLIT003', productName: '危険物', qty: '2' }] }], createdBy: 't' });
  const ss = db.getRunState(cs.runId);
  const [gN, gD] = ss.groups;
  const rS = ss.rows.find((r) => r.fnsku === 'X0SPLIT001');
  const bx1 = db.createBox({ packGroupId: gN.id, materialCode: 'box140', worker: member });
  const bx2 = db.createBox({ packGroupId: gN.id, materialCode: 'box140', worker: member });
  const bx3 = db.createBox({ packGroupId: gN.id, materialCode: 'box140', worker: member });
  const bxD = db.createBox({ packGroupId: gD.id, materialCode: 'box140', worker: member });
  const base = { runId: cs.runId, rowId: rS.id, worker: member, deviceKey: 'dev:split923', deviceLabel: 'iPadS' };
  const liveOf = () => db.getRunState(cs.runId).placements.filter((p) => p.row_id === rS.id && !p.revoked_at);

  t('分けて入れる: 1 回で 2 箱に記録 (1 箱目 12 / 2 箱目 5)。操作ID は rid / rid#2・箱ごとに box_seq', () => {
    const r = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 12 }, { boxId: bx2.boxId, qty: 5 }], requestId: 'sp1' });
    assert.equal(r.ok, true, JSON.stringify(r));
    assert.equal(r.placed, 17);
    assert.deepEqual(r.placements.map((p) => [p.boxId, p.qty, p.boxSeq]), [[bx1.boxId, 12, 1], [bx2.boxId, 5, 1]]);
    const rows = db.getDB().prepare("SELECT request_id, box_id, qty FROM fbx_placements WHERE device_key = 'dev:split923' ORDER BY id").all();
    assert.deepEqual(rows.map((x) => x.request_id), ['sp1', 'sp1#2']);
    assert.equal(r.checkWorker, member.display_name, '確認した人は 1 回だけ自動で入る');
  });
  t('分けて入れる: 同じ操作の送り直しは前の結果を返す (二重に記録しない)。内容が違えば 409', () => {
    const again = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 12 }, { boxId: bx2.boxId, qty: 5 }], requestId: 'sp1' });
    assert.equal(again.ok, true); assert.equal(again.already, true);
    assert.deepEqual(again.placements.map((p) => p.qty), [12, 5]);
    assert.equal(liveOf().length, 2);
    const rp = db.replayPlacement({ deviceKey: 'dev:split923', requestId: 'sp1', runId: cs.runId, rowId: rS.id, splits: [{ boxId: bx1.boxId, qty: 12 }, { boxId: bx2.boxId, qty: 5 }] });
    assert.equal(rp.ok, true); assert.equal(rp.already, true, '作業者の検証より先に返す引き当ても分けた操作を見つける');
    const diff = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 12 }, { boxId: bx2.boxId, qty: 4 }], requestId: 'sp1' });
    assert.equal(diff.error, 'idempotency_conflict');
    assert.equal(db.addPlacement({ ...base, boxId: bx1.boxId, qty: 12, requestId: 'sp1' }).error, 'idempotency_conflict', '1 箱の送信に同じ操作IDを使い回しても通さない');
    assert.equal(db.addPlacement({ ...base, boxId: bx1.boxId, qty: 1, requestId: 'sp1#2' }).error, 'idempotency_conflict', '2 つ目の操作IDを別の操作で使えない');
  });
  t('分けて入れる: 1 つでもだめな箱があれば 1 つも入れない (閉じた箱・別グループ・同じ箱 2 回・合計が残りを超える)', () => {
    db.getDB().prepare("UPDATE fbx_boxes SET status = 'closed' WHERE id = ?").run(bx3.boxId);
    const closed = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 1 }, { boxId: bx3.boxId, qty: 1 }], requestId: 'sp2' });
    assert.equal(closed.error, 'box_closed'); assert.ok(closed.message.includes('G1-3') || closed.message.includes(':'), closed.message);
    db.getDB().prepare("UPDATE fbx_boxes SET status = 'open' WHERE id = ?").run(bx3.boxId);
    assert.equal(db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 1 }, { boxId: bxD.boxId, qty: 1 }], requestId: 'sp3' }).error, 'wrong_group');
    assert.equal(db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 1 }, { boxId: bx1.boxId, qty: 1 }], requestId: 'sp4' }).error, 'bad_request');
    const over = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 2 }, { boxId: bx3.boxId, qty: 2 }], requestId: 'sp5' });
    assert.equal(over.error, 'over_qty', '残り 3 に合計 4 は入らない'); assert.ok(over.message.includes('残りは 3 個'), over.message);
    assert.equal(db.addPlacement({ ...base, splits: [], requestId: 'sp6' }).error, 'bad_qty');
    assert.equal(db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 0 }], requestId: 'sp7' }).error, 'bad_qty');
    assert.equal(liveOf().reduce((a, p) => a + p.qty, 0), 17, 'どれも入っていない');
  });
  t('分けて入れる: 期限は全部の箱に同じ期限で入る。1 箱だけのときはいままでの 1 件の投入と同じ操作ID・ハッシュ', () => {
    const r = db.addPlacement({ ...base, splits: [{ boxId: bx3.boxId, qty: 3 }], requestId: 'sp8', expiry: '2029-02' });
    assert.equal(r.ok, true, JSON.stringify(r));
    const same = db.addPlacement({ ...base, boxId: bx3.boxId, qty: 3, requestId: 'sp8', expiry: '2029-02' });
    assert.equal(same.already, true, '古い画面 (box_id + qty) の送り直しと同じ操作として扱う');
    const r2 = db.getRunState(cs.runId);
    assert.equal(r2.rows.find((x) => x.id === rS.id).placed, 20);
  });
  t('分けて入れた記録の 1 つを取り消したあとの送り直しは「記録できています」と言わない', () => {
    const p2 = db.getDB().prepare("SELECT id FROM fbx_placements WHERE device_key = 'dev:split923' AND request_id = 'sp1#2'").get();
    assert.equal(db.revokePlacement({ placementId: p2.id, worker: member, deviceKey: 'dev:split923' }).ok, true);
    const again = db.addPlacement({ ...base, splits: [{ boxId: bx1.boxId, qty: 12 }, { boxId: bx2.boxId, qty: 5 }], requestId: 'sp1' });
    assert.equal(again.error, 'placement_revoked');
    // 一部だけ取り消し = 残っている箱の分も文に出す (Codex PR #1421 R1 #2: 全部入っていないと思って入れ直させない)
    assert.equal(again.partial, true);
    assert.deepEqual(again.placements.map((p) => [p.qty, p.revoked]), [[12, false], [5, true]]);
    assert.ok(again.message.includes('残っている: ') && again.message.includes('12個') && again.message.includes('取り消し済み: '), again.message);
  });
  t('分けて入れた回の本社向け一覧: 商品の「入れた箱」に 2 箱とも出る', () => {
    const rep = report.buildRunReport(cs.runId);
    const row = rep.groups.flatMap((g) => g.rows).find((r) => r.fnsku === 'X0SPLIT001');
    assert.deepEqual(row.inBoxes.map((b) => b.qty), [12, 3]);
  });
  t('まとめて取り消す (revokePlacements): 全部戻るか 1 つも戻らない・押し直しても同じ・違う商品は混ぜない (Codex PR #1421 R1 #1)', () => {
    const rB = ss.rows.find((r) => r.fnsku === 'X0SPLIT002');
    const add = db.addPlacement({ runId: cs.runId, rowId: rB.id, worker: member, deviceKey: 'dev:split923', splits: [{ boxId: bx1.boxId, qty: 1 }, { boxId: bx2.boxId, qty: 2 }], requestId: 'rb1' });
    assert.equal(add.ok, true, JSON.stringify(add));
    const ids = add.placements.map((p) => p.placementId);
    const mixed = db.revokePlacements({ placementIds: [ids[0], liveOf()[0].id], worker: member, deviceKey: 'dev:split923' });
    assert.equal(mixed.error, 'bad_request', '違う商品の記録は一緒に取り消さない');
    assert.equal(db.revokePlacements({ placementIds: [ids[0], 999999], worker: member }).error, 'not_found');
    assert.equal(db.revokePlacements({ placementIds: [], worker: member }).error, 'bad_request');
    const liveB = () => db.getRunState(cs.runId).placements.filter((p) => p.row_id === rB.id && !p.revoked_at).length;
    assert.equal(liveB(), 2, '断られた呼び出しでは 1 つも戻っていない');
    const r1 = db.revokePlacements({ placementIds: ids, worker: member, deviceKey: 'dev:split923' });
    assert.equal(r1.ok, true); assert.equal(r1.revoked, 2); assert.equal(liveB(), 0);
    const r2 = db.revokePlacements({ placementIds: ids, worker: member, deviceKey: 'dev:split923' });
    assert.equal(r2.ok, true, '押し直し (応答が失われた後) も成功'); assert.equal(r2.already, 2);
  });
  t('数を直す (adjustPlacement) の送り直し: 同じ修正だけ前の結果を返す。数が違えば 409・直した記録が取り消されていれば「直せています」と言わない (Codex PR #1421 R1 #7)', () => {
    const rB = ss.rows.find((r) => r.fnsku === 'X0SPLIT002');
    const dk = 'dev:adj923';
    const base0 = db.addPlacement({ runId: cs.runId, rowId: rB.id, boxId: bx1.boxId, qty: 3, worker: member, deviceKey: dk, requestId: 'adj-base' });
    assert.equal(base0.ok, true, JSON.stringify(base0));
    const a1 = db.adjustPlacement({ placementId: base0.placementId, qty: 2, worker: member, deviceKey: dk, requestId: 'adjA' });
    assert.equal(a1.ok, true, JSON.stringify(a1));
    const again = db.adjustPlacement({ placementId: base0.placementId, qty: 2, worker: member, deviceKey: dk, requestId: 'adjA' });
    assert.equal(again.already, true, '同じ修正の送り直しは前の結果');
    assert.equal(again.placementId, a1.placementId);
    const diff = db.adjustPlacement({ placementId: base0.placementId, qty: 1, worker: member, deviceKey: dk, requestId: 'adjA' });
    assert.equal(diff.error, 'idempotency_conflict', '同じ操作IDで数が違う修正は成功扱いにしない');
    const base1 = db.addPlacement({ runId: cs.runId, rowId: rB.id, boxId: bx2.boxId, qty: 1, worker: member, deviceKey: dk, requestId: 'adj-base2' });
    assert.equal(db.adjustPlacement({ placementId: base1.placementId, qty: 2, worker: member, deviceKey: dk, requestId: 'adjA' }).error, 'idempotency_conflict', '別の記録の修正に同じ操作IDを使っても成功扱いにしない');
    assert.equal(db.revokePlacement({ placementId: a1.placementId, worker: member, deviceKey: dk }).ok, true);
    assert.equal(db.adjustPlacement({ placementId: base0.placementId, qty: 2, worker: member, deviceKey: dk, requestId: 'adjA' }).error, 'placement_revoked',
      '直した記録がそのあと取り消されていれば「直せています」と言わない');
    assert.equal(db.getRunState(cs.runId).placements.filter((p) => p.row_id === rB.id && !p.revoked_at).reduce((a, p) => a + p.qty, 0), 1, '二重に直していない (残りは箱 2 の 1 個だけ)');
  });
  t('数を直す: ふつうの投入の操作IDでは「同じ修正」にならない / 別の操作で直された記録への 0 は成功にしない / 送り直しでも箱を開けたことを返す (Codex PR #1424 R1)', () => {
    const rB = ss.rows.find((r) => r.fnsku === 'X0SPLIT002');
    const dk = 'dev:adj924';
    const liveB = () => db.getRunState(cs.runId).placements.filter((p) => p.row_id === rB.id && !p.revoked_at);
    liveB().forEach((p) => db.revokePlacement({ placementId: p.id, worker: member, deviceKey: dk }));
    // #2: 取消済みの記録 A への修正に、同じ商品・同じ箱のふつうの投入 B の操作IDと数を使う → 成功にしない
    const a = db.addPlacement({ runId: cs.runId, rowId: rB.id, boxId: bx1.boxId, qty: 1, worker: member, deviceKey: dk, requestId: 'n-a' });
    db.revokePlacement({ placementId: a.placementId, worker: member, deviceKey: dk });
    const b = db.addPlacement({ runId: cs.runId, rowId: rB.id, boxId: bx1.boxId, qty: 2, worker: member, deviceKey: dk, requestId: 'n-b' });
    assert.equal(db.adjustPlacement({ placementId: a.placementId, qty: 2, worker: member, deviceKey: dk, requestId: 'n-b' }).error, 'idempotency_conflict');
    // #3: 端末 X が 2→1 に直したあと、古い画面の端末 Y が元の記録を 0 に → 成功にしない (1 個は残っている)
    const fix = db.adjustPlacement({ placementId: b.placementId, qty: 1, worker: member, deviceKey: 'dev:X', requestId: 'fx1' });
    assert.equal(fix.ok, true, JSON.stringify(fix));
    const stale = db.adjustPlacement({ placementId: b.placementId, qty: 0, worker: member, deviceKey: 'dev:Y', requestId: 'fy1' });
    assert.equal(stale.ok, false); assert.equal(stale.error, 'revoked', JSON.stringify(stale));
    // 同じ端末・同じ操作の 0 の送り直しは成功
    const z1 = db.adjustPlacement({ placementId: fix.placementId, qty: 0, worker: member, deviceKey: dk, requestId: 'z1' });
    assert.equal(z1.ok, true);
    const z1again = db.adjustPlacement({ placementId: fix.placementId, qty: 0, worker: member, deviceKey: dk, requestId: 'z1' });
    assert.equal(z1again.ok, true); assert.equal(z1again.already, true);
    assert.equal(db.adjustPlacement({ placementId: fix.placementId, qty: 0, worker: member, deviceKey: dk, requestId: 'z2' }).error, 'revoked', '別の操作の 0 は成功にしない');
    // #4: 閉じた箱の記録を直した → 送り直しでも boxReopened を返す
    const c = db.addPlacement({ runId: cs.runId, rowId: rB.id, boxId: bx2.boxId, qty: 2, worker: member, deviceKey: dk, requestId: 'n-c' });
    db.getDB().prepare("UPDATE fbx_boxes SET status = 'closed' WHERE id = ?").run(bx2.boxId);
    const r1 = db.adjustPlacement({ placementId: c.placementId, qty: 1, worker: member, deviceKey: dk, requestId: 'rc1' });
    assert.equal(r1.boxReopened, true);
    const r1again = db.adjustPlacement({ placementId: c.placementId, qty: 1, worker: member, deviceKey: dk, requestId: 'rc1' });
    assert.equal(r1again.already, true); assert.equal(r1again.boxReopened, true, '送り直しでも量り直しを案内できる');
  });
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
