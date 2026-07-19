/**
 * shipping-log スモークテスト — 出荷実績ログ取込 API
 *
 * 検証内容:
 * Test 1: ensureSchema で sl_shipping_slips + trigger が作成される
 * Test 2: ingestFolderSlips が INSERT し、再送 (別run・別日=日跨ぎ) は ignored (slip_no 冪等)
 * Test 3: 内容不一致 (mgmt_no が双方非NULLで異なる) は conflict、新規側 null は idempotent
 * Test 4: append-only trigger — UPDATE / DELETE が RAISE(ABORT) で拒否される
 * Test 5: HTTP 認証 — env 未設定 503 / token 無し 401 / 不一致 403 / 一致 200 (fail-closed)
 * Test 6: バリデーション — rows 空 / ship_date 形式・実在日 / slip_no 形式 → 400
 * Test 7: conflict 時に HTTP 409 + conflicts 明細
 * Test 8: GET /recent は MIRROR_READ_TOKEN (x-read-token)。未設定503 / 不一致401 / 一致200
 * Test 9: sl_picking_batches スキーマ + append-only trigger
 * Test 10: ingestPickingBatches — INSERT / 冪等再送 / 内容不一致 conflict
 * Test 11: POST /ingest-picking — 認証 / 検算バリデーション / 409
 * Test 12: GET /recent-picking / GET /export (read token + 範囲検証)
 * Test 13: GAS parsePickingText_ — 実物OCRテキスト (2026-07-19 出荷_01) で総合計抽出、
 *          連結トークンの一意分割検算、曖昧・形式外は throw
 *
 * 実行: node scripts/test-shipping-log.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';

// DATA_DIR はモジュール読込時にキャプチャされるため import 前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipping-log-test-'));
process.env.DATA_DIR = tmpDir;

const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { ensureSchema, ingestFolderSlips, ingestPickingBatches, recentSlips, recentPicking } = await import('../apps/shipping-log/db.js');
const { default: shippingLogRouter } = await import('../apps/shipping-log/router.js');

let failed = 0;
function ok(msg) { console.log(`[OK] ${msg}`); }
function fail(msg) { console.error(`[FAIL] ${msg}`); failed++; }
function expectEq(actual, expected, msg) {
  if (actual === expected) ok(`${msg} = ${JSON.stringify(actual)}`);
  else fail(`${msg}: 期待=${JSON.stringify(expected)} 実際=${JSON.stringify(actual)}`);
}

// ── Test 1: スキーマ作成 ──
ensureSchema();
const db = getMirrorDB();
const table = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='sl_shipping_slips'`).get();
table ? ok('sl_shipping_slips 作成') : fail('sl_shipping_slips 未作成');
const triggers = db.prepare(`SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_sl_slips%'`).all();
expectEq(triggers.length, 2, 'append-only trigger 数');

// ── Test 2: 取込 + 冪等再送 (日跨ぎ含む) ──
const basePayload = {
  runId: '20260718-181600', folderName: '出荷_01', shipDate: '2026-07-18', extractedAt: '2026-07-18 18:16:00',
  rows: [
    { slip_no: 'SP00110324384', mgmt_no: '1498337', mall_order_no: null, source_file: '納品書_1.pdf' },
    { slip_no: 'SP00110324772', mgmt_no: '1498416', mall_order_no: 'AES250-3936214-7150201', source_file: '納品書_1.pdf' },
  ],
};
const r1 = ingestFolderSlips(basePayload);
expectEq(r1.inserted, 2, '初回 inserted');
expectEq(r1.conflicts.length, 0, '初回 conflicts');
// 翌日再実行 (削除失敗→翌日再送シナリオ): ship_date が変わっても slip_no で冪等
const r2 = ingestFolderSlips({ ...basePayload, runId: '20260719-181600', shipDate: '2026-07-19' });
expectEq(r2.inserted, 0, '日跨ぎ再送 inserted');
expectEq(r2.ignored, 2, '日跨ぎ再送 ignored');
expectEq(r2.conflicts.length, 0, '日跨ぎ再送 conflicts');
// ignored_details に既存行の出所 (同一フォルダ再送と判別できる情報) が返る (Codex R4)
expectEq(r2.ignored_details.length, 2, '日跨ぎ再送 ignored_details 件数');
expectEq(r2.ignored_details[0]?.folder_name, '出荷_01', 'ignored_details 出所フォルダ');
// 別フォルダから同じ slip_no を送る (OCR誤認シナリオ) → ignored だが出所が異なる
const rX = ingestFolderSlips({ ...basePayload, runId: 'rX', folderName: '出荷_09', rows: [
  { slip_no: 'SP00110324384', mgmt_no: null },
] });
expectEq(rX.ignored, 1, '別フォルダ誤認 ignored');
expectEq(rX.ignored_details[0]?.folder_name, '出荷_01', '別フォルダ誤認の出所 (GAS側で削除見送り判定に使う)');
// 初回の ship_date が保持されている (append-only)
expectEq(db.prepare(`SELECT ship_date FROM sl_shipping_slips WHERE slip_no='SP00110324384'`).get().ship_date,
  '2026-07-18', '初回 ship_date 保持');

// ── Test 3: conflict 判定 ──
const r3 = ingestFolderSlips({ ...basePayload, runId: 'r3', rows: [
  { slip_no: 'SP00110324384', mgmt_no: '9999999' },          // 内容不一致 → conflict
  { slip_no: 'SP00110324772', mgmt_no: null },               // 新規側 null → idempotent
] });
expectEq(r3.conflicts.length, 1, 'conflict 件数');
expectEq(r3.conflicts[0]?.slip_no, 'SP00110324384', 'conflict slip_no');
expectEq(r3.ignored, 1, 'null側 idempotent');
// 既存NULL・新規非NULL → append-only では保存できない新情報なので conflict (Codex R2 medium)
const r3b = ingestFolderSlips({ ...basePayload, runId: 'r3b', rows: [
  { slip_no: 'SP00110324384', mgmt_no: '1498337', mall_order_no: 'AES111-1111111-1111111' },
] });
expectEq(r3b.conflicts.length, 1, '既存NULL→新規非NULL conflict');

// ── Test 4: append-only trigger ──
try {
  db.prepare(`UPDATE sl_shipping_slips SET mgmt_no='x' WHERE slip_no='SP00110324384'`).run();
  fail('UPDATE が通ってしまった');
} catch (e) { /(UPDATE forbidden)/.test(e.message) ? ok('UPDATE 拒否') : fail(`UPDATE 拒否だが想定外メッセージ: ${e.message}`); }
try {
  db.prepare(`DELETE FROM sl_shipping_slips WHERE slip_no='SP00110324384'`).run();
  fail('DELETE が通ってしまった');
} catch (e) { /(DELETE forbidden)/.test(e.message) ? ok('DELETE 拒否') : fail(`DELETE 拒否だが想定外メッセージ: ${e.message}`); }

// ── Test 5-8: HTTP 層 ──
const app = express();
app.use('/apps/shipping-log/api', express.json({ limit: '2mb' }), shippingLogRouter);
const server = app.listen(0, '127.0.0.1');
await new Promise((r) => server.on('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/shipping-log/api`;

async function post(pathName, body, token) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const res = await fetch(base + pathName, { method: 'POST', headers, body: JSON.stringify(body) });
  return { status: res.status, body: await res.json() };
}

const TOKEN = 'test-token-shipping-log';
const READ_TOKEN = 'test-read-token';
const httpPayload = {
  run_id: '20260718-181600', folder: '出荷_02', ship_date: '2026-07-18',
  rows: [{ slip_no: 'SP00110399999', mgmt_no: '1498999' }],
};

delete process.env.SHIPPING_LOG_INGEST_TOKEN;
expectEq((await post('/ingest', httpPayload, TOKEN)).status, 503, 'env 未設定 → 503');
process.env.SHIPPING_LOG_INGEST_TOKEN = TOKEN;
expectEq((await post('/ingest', httpPayload, null)).status, 401, 'token 無し → 401');
expectEq((await post('/ingest', httpPayload, 'wrong-token-xxxxxxxxxxxx')).status, 403, 'token 不一致 → 403');
const okRes = await post('/ingest', httpPayload, TOKEN);
expectEq(okRes.status, 200, 'token 一致 → 200');
expectEq(okRes.body.inserted, 1, 'HTTP経由 inserted');
expectEq(okRes.body.total, 1, 'HTTP経由 total');

// バリデーション
expectEq((await post('/ingest', { ...httpPayload, rows: [] }, TOKEN)).status, 400, 'rows 空 → 400');
expectEq((await post('/ingest', { ...httpPayload, ship_date: '2026/07/18' }, TOKEN)).status, 400, 'ship_date 形式不正 → 400');
expectEq((await post('/ingest', { ...httpPayload, ship_date: '2026-02-31' }, TOKEN)).status, 400, 'ship_date 実在しない日 → 400');
expectEq((await post('/ingest', { ...httpPayload, rows: [{ mgmt_no: '1' }] }, TOKEN)).status, 400, 'slip_no 欠落 → 400');
expectEq((await post('/ingest', { ...httpPayload, rows: [{ slip_no: 'INVALID123' }] }, TOKEN)).status, 400, 'slip_no 形式不正 → 400');

// conflict → 409
const conflictRes = await post('/ingest', { ...httpPayload, run_id: 'r-conflict',
  rows: [{ slip_no: 'SP00110399999', mgmt_no: '7777777' }] }, TOKEN);
expectEq(conflictRes.status, 409, '内容不一致 → 409');
expectEq(conflictRes.body.conflicts?.length, 1, '409 conflicts 明細');

// GET /recent は read token (x-read-token)
delete process.env.MIRROR_READ_TOKEN;
expectEq((await fetch(`${base}/recent`)).status, 503, 'read token env 未設定 → 503');
process.env.MIRROR_READ_TOKEN = READ_TOKEN;
expectEq((await fetch(`${base}/recent`, { headers: { 'x-read-token': 'wrong' } })).status, 401, 'read token 不一致 → 401');
const recentRes = await fetch(`${base}/recent?limit=10`, { headers: { 'x-read-token': READ_TOKEN } });
const recentBody = await recentRes.json();
expectEq(recentRes.status, 200, 'GET /recent → 200');
expectEq(recentBody.rows.length, 3, 'recent 行数');
// ingest token では /recent は読めない (権限分離)
expectEq((await fetch(`${base}/recent`, { headers: { authorization: `Bearer ${TOKEN}` } })).status, 401, 'ingest token では recent 不可');

// DB層 recentSlips も確認
expectEq(recentSlips(10).length, 3, 'recentSlips 行数');

// ── Test 9: sl_picking_batches スキーマ ──
const pickTable = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='sl_picking_batches'`).get();
pickTable ? ok('sl_picking_batches 作成') : fail('sl_picking_batches 未作成');
const pickTriggers = db.prepare(`SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_sl_picking%'`).all();
expectEq(pickTriggers.length, 2, 'picking append-only trigger 数');

// ── Test 10: ingestPickingBatches 冪等 / conflict ──
const pickPayload = {
  runId: '20260719-181600', folderName: '出荷_01', shipDate: '2026-07-19', extractedAt: '2026-07-19 18:16:00',
  rows: [{ source_file: 'ピッキングリスト_1.pdf', total_qty: 67, pages: 3, page_totals: [35, 31, 1],
           work_date_on_list: '2026-07-19', printed_at: '2026-07-19 10:46:41' }],
};
const p1 = ingestPickingBatches(pickPayload);
expectEq(p1.inserted, 1, 'picking 初回 inserted');
const p2 = ingestPickingBatches({ ...pickPayload, runId: 'retry' });
expectEq(p2.inserted, 0, 'picking 再送 inserted');
expectEq(p2.ignored, 1, 'picking 再送 ignored');
const p3 = ingestPickingBatches({ ...pickPayload,
  rows: [{ ...pickPayload.rows[0], total_qty: 68, page_totals: [35, 32, 1] }] });
expectEq(p3.conflicts.length, 1, 'picking 内容不一致 conflict');
// 別日は別バッチ扱い (PK に ship_date を含む — 同名フォルダの使い回し対応)
const p4 = ingestPickingBatches({ ...pickPayload, shipDate: '2026-07-20' });
expectEq(p4.inserted, 1, 'picking 別日は新規行');
try {
  db.prepare(`UPDATE sl_picking_batches SET total_qty=1 WHERE folder_name='出荷_01'`).run();
  fail('picking UPDATE が通ってしまった');
} catch (e) { /(UPDATE forbidden)/.test(e.message) ? ok('picking UPDATE 拒否') : fail(`picking UPDATE 想定外: ${e.message}`); }

// ── Test 11: POST /ingest-picking ──
const httpPickPayload = {
  run_id: 'r-pick', folder: '出荷_02', ship_date: '2026-07-19',
  rows: [{ source_file: 'ピッキングリスト_2.pdf', total_qty: 12, pages: 2, page_totals: [7, 5] }],
};
expectEq((await post('/ingest-picking', httpPickPayload, null)).status, 401, 'picking token 無し → 401');
const pickOk = await post('/ingest-picking', httpPickPayload, TOKEN);
expectEq(pickOk.status, 200, 'picking token 一致 → 200');
expectEq(pickOk.body.inserted, 1, 'picking HTTP経由 inserted');
// 検算・形式バリデーション
expectEq((await post('/ingest-picking', { ...httpPickPayload,
  rows: [{ source_file: 'x.pdf', total_qty: 12, pages: 2, page_totals: [7, 4] }] }, TOKEN)).status, 400, '検算不一致 → 400');
expectEq((await post('/ingest-picking', { ...httpPickPayload,
  rows: [{ source_file: 'x.pdf', total_qty: 12, pages: 3, page_totals: [7, 5] }] }, TOKEN)).status, 400, 'pages と page_totals 数不一致 → 400');
expectEq((await post('/ingest-picking', { ...httpPickPayload,
  rows: [{ source_file: 'x.pdf', total_qty: 0, pages: 1, page_totals: [0] }] }, TOKEN)).status, 400, 'total_qty=0 → 400');
expectEq((await post('/ingest-picking', { ...httpPickPayload, rows: [
  { source_file: 'dup.pdf', total_qty: 1, pages: 1, page_totals: [1] },
  { source_file: 'dup.pdf', total_qty: 1, pages: 1, page_totals: [1] },
] }, TOKEN)).status, 400, 'source_file 重複 → 400');
// 内容不一致 → 409
const pickConflict = await post('/ingest-picking', { ...httpPickPayload, run_id: 'r-pick2',
  rows: [{ source_file: 'ピッキングリスト_2.pdf', total_qty: 13, pages: 2, page_totals: [7, 6] }] }, TOKEN);
expectEq(pickConflict.status, 409, 'picking 内容不一致 → 409');

// ── Test 12: GET /recent-picking + /export ──
const rp = await fetch(`${base}/recent-picking?limit=10`, { headers: { 'x-read-token': READ_TOKEN } });
expectEq(rp.status, 200, 'GET /recent-picking → 200');
expectEq((await rp.json()).rows.length, 3, 'recent-picking 行数');
expectEq(recentPicking(10).length, 3, 'recentPicking 行数');
expectEq((await fetch(`${base}/export?from=2026-07-19&to=2026-07-19`)).status, 401, 'export token 無し → 401');
const ex = await fetch(`${base}/export?from=2026-07-18&to=2026-07-19`, { headers: { 'x-read-token': READ_TOKEN } });
expectEq(ex.status, 200, 'GET /export → 200');
const exBody = await ex.json();
expectEq(exBody.slips.length, 3, 'export slips 件数');
expectEq(exBody.picking.length, 2, 'export picking 件数 (7/18-19)');
expectEq((await fetch(`${base}/export?from=2026-07-19&to=2026-07-18`, { headers: { 'x-read-token': READ_TOKEN } })).status,
  400, 'export from>to → 400');
expectEq((await fetch(`${base}/export?from=2026-01-01&to=2026-07-19`, { headers: { 'x-read-token': READ_TOKEN } })).status,
  400, 'export 範囲超過 → 400');

// ── Test 13: GAS parsePickingText_ (実物OCRテキスト) ──
// .gs を Node で評価して実物の関数を取り出す (コピーでなく本物をテストする)。
// 評価対象はリポジトリ内の自作ファイルのみ = import と同じ信頼レベル (外部入力は混入しない)。
// GAS API (DriveApp等) はトップレベルでは呼ばれないので読み込みだけなら安全。
const gasSrc = fs.readFileSync(new URL('./gas/shipping-trash-ingest.gs', import.meta.url), 'utf-8');
const gasFns = new Function('Drive',
  `${gasSrc}\n; return { parsePickingText_: parsePickingText_, dedupeByContent_: dedupeByContent_ };`);
const { parsePickingText_: parsePickingText } = gasFns(undefined);

// 2026-07-19 出荷_01 の実物OCR抜粋: 総合計67、ページ内合計35/31/1 が「6735」「6731」「671」で出る。
// バーコード (220000001等)・数量連結blob (42222622221611)・ロケーション断片 (003等) が混在
const realText = [
  'Page：ブロック 003-009-01 003-010-01 トータルピッキングリスト 出力日時：',
  '作業日 2026/07/19 業務区分 通販 担当者 荷主名 B-Faith株式会社 数量 残数',
  '／ 1 3 2026/07/19 10:46:41 No.1 6735 良品feelsc 220000002 richbathp-sl 良品220000001',
  '2028/10/08 牛のひづめ 【ノーマル 2個入り】 総合計 ページ内合計 42222622221611 280 332 258 26108',
  'Page：ブロック ／ 2 3 2026/07/19 10:46:41 No.1 6731 良品as-sa5 887755 490487200 総合計 ページ内合計 111112113111214 30790621 58736',
  'Page：ブロック 良品 P3FD 008-019-05 5209 1 10 トータルピッキングリスト 3 ／ 3 出力日時： 2026/07/19 10:46:41 No.1 671',
  '担当者 moumouhc-co 490487200 総合計 ページ内合計',
].join('\n');
const parsed = parsePickingText(realText);
expectEq(parsed.totalQty, 67, '実物OCR 総合計');
expectEq(parsed.pages, 3, '実物OCR ページ数');
expectEq(JSON.stringify(parsed.pageTotals.slice().sort((a, b) => a - b)), '[1,31,35]', '実物OCR ページ内合計');
expectEq(parsed.workDate, '2026-07-19', '実物OCR 作業日 (最頻出日付)');
expectEq(parsed.printedAt, '2026-07-19 10:46:41', '実物OCR 出力日時');

// 1ページもの: 総合計12+ページ内合計12 → 「1212」
const single = parsePickingText('トータルピッキングリスト 作業日 2026/07/19 ページ内合計 1212 総合計');
expectEq(single.totalQty, 12, '1ページ 総合計');
expectEq(JSON.stringify(single.pageTotals), '[12]', '1ページ ページ内合計');

// 『ページ内合計』が無い (別書式PDF) → throw
try { parsePickingText('納品書 SP00110324384'); fail('形式外が通ってしまった'); }
catch (e) { /ピッキングリスト形式でない/.test(e.message) ? ok('形式外 throw') : fail(`形式外 想定外: ${e.message}`); }

// 検算の通る分割が無い (合計トークン欠け) → throw
try { parsePickingText('ページ内合計 ページ内合計 9999 12'); fail('検算不能が通ってしまった'); }
catch (e) { /特定できず/.test(e.message) ? ok('検算不能 throw') : fail(`検算不能 想定外: ${e.message}`); }

// ── Test 14: GAS dedupeByContent_ — md5一致のみ複製扱い / md5不明は除外しない ──
const md5map = { id1: 'aaa', id2: 'aaa', id3: 'bbb' }; // id4 は md5 取得不可
const driveMock = { Files: { get: (id) => { if (!md5map[id]) throw new Error('no md5'); return { md5Checksum: md5map[id] }; } } };
const { dedupeByContent_: dedupe } = gasFns(driveMock);
const mkFile = (id, name) => ({ getId: () => id, getName: () => name, getSize: () => 100 });
expectEq(dedupe([mkFile('id1', 'a.pdf'), mkFile('id2', 'a.pdf')]).length, 1, '同名同md5 → 1つに集約');
expectEq(dedupe([mkFile('id1', 'a.pdf'), mkFile('id3', 'a.pdf')]).length, 2, '同名同サイズ異md5 → 両方残す');
expectEq(dedupe([mkFile('id1', 'a.pdf'), mkFile('id4', 'a.pdf')]).length, 2, 'md5不明 → 複製扱いしない');

await new Promise((r) => server.close(r));
try { db.close(); } catch { /* teardown */ }
console.log(failed === 0 ? '\n✅ 全テスト PASS' : `\n❌ ${failed} 件失敗`);
process.exitCode = failed === 0 ? 0 : 1;
