/**
 * tracking-runner.js の純粋関数 (JST日付 / 通知本文) のテスト。
 *   node apps/fba-replenishment/tests/test-tracking-runner.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-runner-test-'));
// SP-APIの疎通はしない。missingEnv を通すためだけのダミー
for (const k of ['SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET', 'SP_API_REFRESH_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']) {
  process.env[k] = process.env[k] || 'dummy-for-test';
}

const { jstYmd, formatSummary, runTrackingJob, partitionShipments } = await import('../tracking-runner.js');
const store = await import('../tracking-store.js');
const { _internal, summarizeShipment, checkDeadline, trackingSlotCount, describePutError } = await import('../tracking-service.js');

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('tracking-runner');

t('JST日付: UTC深夜でも日本の日付になる (toISOStringの罠)', () => {
  assert.equal(jstYmd(new Date('2026-08-07T13:00:00Z')), '20260807'); // 22:00 JST
  assert.equal(jstYmd(new Date('2026-08-07T15:30:00Z')), '20260808'); // 翌日 00:30 JST
  assert.equal(jstYmd(new Date('2026-08-06T23:00:00Z')), '20260807');
});

const base = { runId: 'r', commit: true, severity: 'ok', openShipments: 0, expectYmd: '20260807', blocked: [], excluded: [], skipped: [], registered: [], failed: [], note: [] };

t('中断時は「登録は一切していません」と明記する', () => {
  const s = formatSummary({ ...base, severity: 'blocked', blocked: ['個口合計 5 と輸送箱 6 が一致しません'] });
  assert.match(s, /🚨/);
  assert.match(s, /登録は一切していません/);
  assert.match(s, /個口合計 5 と輸送箱 6/);
  assert.match(s, /翌日 Seller Central 画面から手入力/);
});

t('成功時は納品ごとに箱数と照合方法を出す', () => {
  const s = formatSummary({ ...base, severity: 'ok', registered: [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', countBoxes: 5, matchedBy: '納品番号' }] });
  assert.match(s, /✅/);
  assert.match(s, /FBA15GGL5J2X \(HND2\) 5箱  照合=納品番号/);
});

t('期限切れは「画面から手入力してください」を添える', () => {
  const s = formatSummary({ ...base, severity: 'warn', failed: [{ shipmentConfirmationId: 'FBA-A', error: '編集期限を過ぎています', needsManual: true }] });
  assert.match(s, /⚠️/);
  assert.match(s, /画面から手入力してください/);
});

t('🚨除外は黙って捨てず件数を必ず出す', () => {
  const s = formatSummary({ ...base, excluded: [{ fcCode: 'AMRC', 件数: 4, reason: 'FBA以外' }] });
  assert.match(s, /除外 4件/);
  assert.match(s, /AMRC/);
});

t('🚨納品が無い日 (CSV未設置) は情報どまり — 毎晩の誤警報を出さない', () => {
  const s = formatSummary({ ...base, severity: 'info', note: ['本日の出荷実績CSVはまだ置かれていません。FBA納品が無い日なら正常です'] });
  assert.match(s, /^ℹ️/);
  assert.doesNotMatch(s, /🚨/);
  assert.doesNotMatch(s, /登録は一切していません/);
});

t('対象の納品が無いだけなら警告どまり (中断にしない)', () => {
  const s = formatSummary({ ...base, severity: 'warn', note: ['追跡番号の未登録な納品が見つかりません'] });
  assert.match(s, /^⚠️/);
  assert.doesNotMatch(s, /🚨/);
});

t('箱数不一致は中断 (severityで見分ける)', () => {
  const s = formatSummary({ ...base, severity: 'blocked', blocked: ['個口合計 5 と輸送箱 6 が一致しません'] });
  assert.match(s, /^🚨/);
  assert.match(s, /登録は一切していません/);
});

t('🚨投入待ちの納品が無い日は情報どまり — 置きっぱなしCSVで赤を出さない', () => {
  const s = formatSummary({ ...base, severity: 'info', openShipments: 0,
    note: ['追跡番号の投入を待っている納品はありません (納品が無い日、すでに画面で入力済み、またはラベル未発行)'] });
  assert.match(s, /^ℹ️/);
  assert.doesNotMatch(s, /🚨/);
  assert.doesNotMatch(s, /登録は一切していません/);
});

t('🚨投入待ちがあるのにCSVが古いときは赤 + 対象を並べる', () => {
  const s = formatSummary({ ...base, severity: 'blocked', openShipments: 2, blocked: [
    'CSVの出荷日が期待 (20260808) と一致しません → 20260807:19件。古いCSVが残っていないか、出力時の日付指定を確認してください',
    '追跡番号待ちの納品: FBA15GGL5J2X(HND2) 5箱 / FBA15GGLDVMG(XHD4) 14箱',
  ] });
  assert.match(s, /^🚨/);
  assert.match(s, /FBA15GGL5J2X\(HND2\) 5箱/);
});

t('🚨投入待ちがあるのにCSVが無いときは、何をすべきか書く', () => {
  const s = formatSummary({ ...base, severity: 'blocked', openShipments: 1, blocked: [
    '追跡番号待ちの納品が 1件あるのに、出荷実績CSVが取得できません: ファイルが見つかりません',
    '対象: FBA15GGL5J2X(HND2) 5箱',
    'iS-2の「出荷実績印刷・CSV出力」から本日分を fukutsu_tuiseki.csv として保存してください',
  ] });
  assert.match(s, /出荷実績印刷・CSV出力/);
  assert.match(s, /fukutsu_tuiseki\.csv/);
});

t('プレビューはタイトルで分かる', () => {
  assert.match(formatSummary({ ...base, commit: false }), /追跡番号\(プレビュー\)/);
  assert.doesNotMatch(formatSummary({ ...base, commit: true }), /プレビュー/);
});

// ── 2巡目レビューの修正 ──────────────────────────────
t('🚨受理されたか断定できない例外は indeterminate にする (再送で二重投入しない)', () => {
  const c = _internal.classifyPutError;
  // Amazonが明確に拒否したもの = 確定失敗
  assert.equal(c({ code: 'BadRequest', message: 'is not in a state where tracking details can be provided' }).indeterminate, false);
  assert.equal(c({ code: 'InvalidInput', message: 'validation error detected' }).indeterminate, false);
  // 通信起因 = 受理された可能性がある
  assert.equal(c({ code: 'ETIMEDOUT', message: 'socket hang up' }).indeterminate, true);
  assert.equal(c({ code: 'ECONNRESET', message: 'read ECONNRESET' }).indeterminate, true);
  assert.equal(c(new Error('Internal Server Error')).indeterminate, true);
  assert.equal(c(undefined).indeterminate, true);
});

// ── 2026-08-11 実測の修正: 期限切れガードの素通り ──────────────────
// 8/10分をJST 10時に --commit したら、checkDeadline が要約オブジェクト (datesなし) を
// 受け取って「期限の情報が取れませんでした→ok」と素通りし、Amazonの
// "not in a state..." エラーで3件失敗した。要約が期限の生データを保持し、
// runner の checkDeadline(ship, now) が期限切れを検出できることを固定する。
t('🚨要約は期限の生データを保持する (checkDeadlineの再評価が素通りしない)', () => {
  // 8/11 に実際に返ってきた getShipment の形 (FBA15GFZRWWR)
  const raw = {
    shipmentConfirmationId: 'FBA15GFZRWWR',
    status: 'READY_TO_SHIP',
    destination: { warehouseId: 'XJW1' },
    dates: { readyToShipWindow: { start: '2026-08-10T14:59Z', end: '2026-08-10T14:59Z' } },
    selectedDeliveryWindow: { editableUntil: '2026-08-11T00:00Z', startDate: '2026-08-11T00:00Z', endDate: '2026-08-14T00:00Z' },
  };
  const ship = summarizeShipment({ inboundPlanId: 'p1', name: 'n' }, { shipmentId: 's1' }, raw, [{ boxId: 'B1' }]);
  // 22:00 JST (出荷当日) = 期限内 → 投入してよい
  assert.equal(checkDeadline(ship, new Date('2026-08-10T13:00:00Z')).ok, true);
  // 翌朝 10:00 JST = 両方の期限切れ → ガードで止まる (Amazonのエラーまで行かせない)
  const dl = checkDeadline(ship, new Date('2026-08-11T01:00:00Z'));
  assert.equal(dl.ok, false);
  assert.match(dl.note, /画面からは入力できます/);
  assert.equal(dl.expired.length, 2);
});

// 🚨2026-08-12 22:00 の本番で実際に起きた形。READY_TO_SHIP なのに spdTrackingItems が
// 「箱数ぶんの trackingId 空エントリ」で返り、hasTracking=true と誤認 →
// 納品が照合対象から外れ、CSV行が宙に浮いて「対応する納品が見つかりません」で全件中断した。
t('🚨trackingIdが空のエントリは「登録済み」と数えない (8/12の全件中断の原因)', () => {
  const raw = {
    shipmentConfirmationId: 'FBA15GG6BGX1',
    status: 'READY_TO_SHIP',
    destination: { warehouseId: 'XJE2' },
    trackingDetails: { spdTrackingDetail: { spdTrackingItems: [
      { boxId: 'FBA15GG6BGX1U000001' }, // trackingId 無し
      { boxId: 'FBA15GG6BGX1U000002', trackingId: '' },
      { boxId: 'FBA15GG6BGX1U000003', trackingId: '  ' },
    ] } },
  };
  const ship = summarizeShipment({ inboundPlanId: 'p1', name: '' }, { shipmentId: 's1' }, raw, [{ boxId: 'B1' }]);
  assert.equal(ship.hasTracking, false);
  // 1件でも実値が入っていれば従来どおり「登録済み」
  raw.trackingDetails.spdTrackingDetail.spdTrackingItems[0].trackingId = '66393873615';
  const ship2 = summarizeShipment({ inboundPlanId: 'p1', name: '' }, { shipmentId: 's1' }, raw, [{ boxId: 'B1' }]);
  assert.equal(ship2.hasTracking, true);
});

t('期限の情報が無い納品は緩い側に倒す (ok=true。最終判断はAmazon)', () => {
  const ship = summarizeShipment({ inboundPlanId: 'p1', name: '' }, { shipmentId: 's1' },
    { shipmentConfirmationId: 'FBA-X', status: 'READY_TO_SHIP', destination: { warehouseId: 'HND2' } }, []);
  assert.equal(ship.dates, null);
  assert.equal(checkDeadline(ship).ok, true);
});

// ── 記入欄 (spdTrackingItems の枠) の有無で投入可否が決まる — 2026-08-31 実測 ──
const mkShip = (id, { slots = 1, boxes = 1, status = 'READY_TO_SHIP', rtsEnd = null, tracked = false } = {}) => {
  const items = Array.from({ length: slots }, (_, i) => ({ boxId: `${id}U${i}`, ...(tracked ? { trackingId: '66393873615' } : {}) }));
  const raw = {
    shipmentConfirmationId: id, status, destination: { warehouseId: 'HND2' },
    trackingDetails: slots ? { spdTrackingDetail: { spdTrackingItems: items } } : {},
    ...(rtsEnd ? { dates: { readyToShipWindow: { start: rtsEnd, end: rtsEnd } } } : {}),
  };
  return summarizeShipment({ inboundPlanId: 'p1', name: '' }, { shipmentId: 's-' + id }, raw,
    Array.from({ length: boxes }, (_, i) => ({ boxId: `${id}U${i}` })));
};

t('🚨記入欄の数を数える (これがPUTの可否そのもの)', () => {
  assert.equal(trackingSlotCount({ trackingDetails: { spdTrackingDetail: { spdTrackingItems: [{ boxId: 'B1' }, { boxId: 'B2' }] } } }), 2);
  assert.equal(trackingSlotCount({ trackingDetails: {} }), 0);
  assert.equal(trackingSlotCount({}), 0);
});

t('🚨記入欄が空でも「欄がある」なら投入できる (欄あり=putReady)', () => {
  const s = mkShip('FBA-SLOT', { slots: 3, boxes: 3 });
  assert.equal(s.slots, 3);
  assert.equal(s.putReady, true);
  assert.equal(s.hasTracking, false, '空欄は「登録済み」ではない');
});

t('🚨記入欄が無い納品は投入対象から外す (何度投げても BadRequest になるだけ)', () => {
  const ng = mkShip('FBA-NOSLOT', { slots: 0, boxes: 4 });
  assert.equal(ng.putReady, false);
  const { actionable, outOfReach, todo } = partitionShipments([], [ng]);
  assert.equal(todo.length, 0);
  assert.equal(actionable.length, 0);
  assert.equal(outOfReach.length, 1);
  assert.match(outOfReach[0].skipReason, /記入欄/);
});

t('🚨記入欄と輸送箱が食い違う納品は送らない (一部の箱が宙に浮く)', () => {
  const ng = mkShip('FBA-PARTIAL', { slots: 1, boxes: 3 });
  assert.equal(ng.putReady, false);
  assert.match(ng.notReadyReason, /記入欄 1個 と輸送箱 3個 が一致しません/);
  const { todo, outOfReach } = partitionShipments([], [ng]);
  assert.equal(todo.length, 0);
  assert.match(outOfReach[0].skipReason, /一致しません/);
});

t('🚨到着済みなのに追跡番号が無い納品を黙って消さない (240箱を3週間見落とした原因)', () => {
  const late = mkShip('FBA-LATE', { slots: 0, boxes: 19, status: 'RECEIVING' });
  const { todo, outOfReach } = partitionShipments([], [], new Date(), [late]);
  assert.equal(todo.length, 0);
  assert.equal(outOfReach.length, 1);
  assert.equal(outOfReach[0].tooLate, true);
  assert.match(outOfReach[0].skipReason, /RECEIVING.*未登録のまま到着/);
});

t('🚨期限切れは照合より前に外す (残すとCSV行が宙に浮いてその日の全件が中断する)', () => {
  const now = new Date('2026-08-28T13:00:00Z');
  const dead = mkShip('FBA-OLD', { slots: 1, rtsEnd: '2026-08-27T14:59Z' });
  const live = mkShip('FBA-NEW', { slots: 1, rtsEnd: '2026-08-28T14:59Z' });
  const { actionable, outOfReach, todo } = partitionShipments([dead, live], [], now);
  assert.deepEqual(todo.map((s) => s.shipmentConfirmationId), ['FBA-NEW']);
  assert.deepEqual(outOfReach.map((s) => s.shipmentConfirmationId), ['FBA-OLD']);
  assert.ok(actionable.every((s) => s.shipmentConfirmationId !== 'FBA-OLD'));
});

t('登録済みの納品は照合には回すが、投入対象には数えない (CSV行を宙に浮かせないため)', () => {
  const done = mkShip('FBA-DONE', { slots: 1, tracked: true });
  const { actionable, todo, outOfReach } = partitionShipments([done], []);
  assert.equal(actionable.length, 1, '照合には渡す');
  assert.equal(todo.length, 0, '投入はしない');
  assert.equal(outOfReach.length, 0);
});

t('🚨投入できない納品が残る日は緑にしない (放置すると期限を過ぎて箱が迷子になる)', () => {
  const s = formatSummary({
    ...base, severity: 'warn', registered: [],
    outOfReach: [{ shipmentConfirmationId: 'FBA-A', fcCode: 'XJE1', boxes: 45, reason: 'Amazon側に追跡番号の記入欄がありません (Seller Central で出荷確定の操作が必要)' }],
  });
  assert.match(s, /⚠️/);
  assert.match(s, /画面から入力してください 1件 \(45箱\)/);
  assert.match(s, /FBA-A \(XJE1\) 45箱/);
  assert.match(s, /記入欄/);
});

t('🚨PUT失敗はcode・HTTPステータス・request IDまで残す (同じ文言の別原因を切り分けるため)', () => {
  const d = describePutError({
    code: 'BadRequest', statusCode: 400, message: 'ERROR: Shipment sh1 is not in a state where tracking details can be provided.',
    headers: { 'x-amzn-RequestId': 'abc-123' }, details: 'detail text',
  });
  assert.equal(d.code, 'BadRequest');
  assert.equal(d.httpStatus, 400);
  assert.equal(d.requestId, 'abc-123');
  assert.match(d.message, /not in a state/);
  // 手がかりが何も無い例外でも message だけは必ず残る
  assert.equal(describePutError(new Error('socket hang up')).message, 'socket hang up');
});

await (async () => {
  const name = '🚨投入記録に読めない行があれば中断する (pendingを見失って再送しない)';
  fs.appendFileSync(store.storePath(), '{壊れたJSON' + String.fromCharCode(10), 'utf-8');
  const s = await runTrackingJob({ file: 'これは読まれない.csv' });
  assert.equal(s.severity, 'blocked', JSON.stringify(s));
  assert.ok(s.blocked.some((b) => b.includes('投入記録に読めない行があります')), s.blocked.join(' / '));
  fs.writeFileSync(store.storePath(), '', 'utf-8');
  pass++; console.log(`  ok  ${name}`);
})();

await (async () => {
  const name = 'ロックは実行後に解放される (次の実行が止まらない)';
  const s = await runTrackingJob({ file: 'これは読まれない.csv' });
  assert.ok(s.severity, JSON.stringify(s));
  assert.equal(store.acquireLock('after').ok, true, '前の実行のロックが残っていない');
  store.releaseLock('after');
  pass++; console.log(`  ok  ${name}`);
})();

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });

console.log(`\n${pass} 件すべて通過`);
