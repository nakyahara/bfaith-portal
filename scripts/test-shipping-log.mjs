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
 * Test 13: GAS parsePickingText_ — GAS実機OCR (ラベル直後型) で総合計抽出+検算、
 *          連結型/検算不一致/形式外はすべて throw (フォールバック無し=偽解防止)
 * Test 14: GAS dedupeByContent_ — md5一致のみ複製扱い、md5不明は除外しない
 * Test 15: Notion担当者 (notion-staff.js) — buildStaffRows の設定検証 fail-closed /
 *          正規化 / 重複タイブレーク、upsertStaffRows の COALESCE 保持、
 *          POST /sync-staff / GET /staff-props / GET /recent-staff の認証と状態
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
// 連結型 (別経路抽出の形式) はGASでは出ないため不採用 — 全文数字走査の偽解リスクを
// 避けるべく throw して隔離に回す (Codex high ×2 の結論)
try { parsePickingText(realText); fail('連結型が通ってしまった (フォールバック廃止済みのはず)'); }
catch (e) { /特定できず/.test(e.message) ? ok('連結型OCR → throw (隔離へ)') : fail(`連結型 想定外: ${e.message}`); }

// GAS Docs変換OCRの実出力 (2026-07-19、debugPickingRawText 実機ログ抜粋):
// ラベルと値が分離して「総合計\n67\nページ内合計\n35」の形で出る (戦略A)
const gasOcrText = [
  'トータルピッキングリスト 出力日時： ', 'Page2026/07/19 10:46:41 1 3', '',
  '作業日 ', '2026/07/19', '業務区分 ', '通販', '荷主名 ', 'No.1B-Faith株式会社 ', '倉庫名 ', 'B-Faith',
  '担当者', '', '総合計', '67', 'ページ内合計', '35', '',
  'ブロック', 'ロケーション ', '商品ID', '数量 ', '残数', 'バーコード',
  'P3FA', '003-009-01 ', 'feelsc', '良品', '4', '280', '2200000029829',
  'フィールソークール 20g 【ペパーミントの香り】',
  'P3FB', '003-005-04 ', 'ushi-hidume-2', '良品', '1', '8', 'X000TGXUNJ', '2028/10/08',
  '牛のひづめ 【ノーマル 2個入り】_K-44',
  'トータルピッキングリスト 出力日時： ', 'Page2026/07/19 10:46:41 2 3',
  '作業日 ', '2026/07/19', '担当者', '', '総合計', '67', 'ページ内合計', '31', '',
  'P3FD', '006-008-03 ', 'nemunemask', '良品', '12', '587', '4530212204027',
  'トータルピッキングリスト 出力日時： ', 'Page2026/07/19 10:46:41 3 3',
  '作業日 ', '2026/07/19', '担当者', '', '総合計', '67', 'ページ内合計', '1', '',
  'P3FD ', '008-019-05 ', 'moumouhc-co', '良品 ', '1 ', '10', '4904872005209 ',
].join('\n');
const gasParsed = parsePickingText(gasOcrText);
expectEq(gasParsed.totalQty, 67, 'GAS実OCR(ラベル直後型) 総合計');
expectEq(JSON.stringify(gasParsed.pageTotals), '[35,31,1]', 'GAS実OCR(ラベル直後型) ページ内合計');
expectEq(gasParsed.workDate, '2026-07-19', 'GAS実OCR(ラベル直後型) 作業日');
expectEq(gasParsed.printedAt, '2026-07-19 10:46:41', 'GAS実OCR(ラベル直後型) 出力日時');
// ラベル直後型を認識したのに検算が合わない → 戦略Bに迂回せず即throw (Codex high:
// Bは全文走査なので無関係な数字 71/76 が「総合計7=1+6」の偽解を作りうる)
try {
  parsePickingText('総合計 67 ページ内合計 35 総合計 68 ページ内合計 31 余白 71 76');
  fail('総合計不一致が通ってしまった');
} catch (e) { /総合計がページ間で不一致/.test(e.message) ? ok('総合計不一致→B迂回せずthrow') : fail(`総合計不一致 想定外: ${e.message}`); }
try {
  parsePickingText('総合計\n67\nページ内合計\n35\n総合計\n67\nページ内合計\n31');
  fail('総和不一致が通ってしまった');
} catch (e) { /検算不一致/.test(e.message) ? ok('総和不一致→throw') : fail(`総和不一致 想定外: ${e.message}`); }
// カンマ入り等の想定外数値形式は不成立扱い (「6,7」→67 と読む寛容さは偽解の温床)
try {
  parsePickingText('総合計 6,7 ページ内合計 3,5 総合計 6,7 ページ内合計 3,2');
  fail('カンマ入り数値が通ってしまった');
} catch (e) { /特定できず/.test(e.message) ? ok('カンマ入り数値→throw') : fail(`カンマ入り 想定外: ${e.message}`); }

// 1ページもの (ラベル直後型)
const single = parsePickingText('トータルピッキングリスト 作業日 2026/07/19 総合計 12 ページ内合計 12');
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

// ── Test 15: Notion担当者スナップショット ──
const { buildStaffRows, normalizeBatchNo, lookbackSinceIso } = await import('../apps/shipping-log/notion-staff.js');

// 取得窓はJST暦日の00:00起点 (境界日の部分取得→置換欠損を防ぐ)。
// 19:30 JST (=10:30 UTC) 実行・7日窓 → 最古対象日=7/13 (JST) の00:00 = 7/12 15:00 UTC
expectEq(lookbackSinceIso(7, new Date('2026-07-19T10:30:00Z')), '2026-07-12T15:00:00.000Z',
  'lookback窓はJST暦日00:00起点');
expectEq(lookbackSinceIso(1, new Date('2026-07-19T10:30:00Z')), '2026-07-18T15:00:00.000Z',
  'lookback=1は当日JST 00:00から');
const { upsertStaffRows, recentStaff: recentStaffFn } = await import('../apps/shipping-log/db.js');

expectEq(normalizeBatchNo('出荷_01'), '出荷_01', 'batch正規化 そのまま');
expectEq(normalizeBatchNo('1'), '出荷_01', 'batch正規化 数字のみ');
expectEq(normalizeBatchNo('出荷12'), '出荷_12', 'batch正規化 アンダースコアなし');
expectEq(normalizeBatchNo('メモ'), null, 'batch正規化 数字なし→null');
expectEq(normalizeBatchNo('棚卸 1'), null, 'batch正規化 無関係接頭辞→null');
expectEq(normalizeBatchNo('返品_02'), null, 'batch正規化 別業務カード→null');
expectEq(normalizeBatchNo('0'), null, 'batch正規化 0→null');
// 実DB (スタッフ用デイリー業務) で観測した表記
expectEq(normalizeBatchNo('出荷_1　パフ済'), '出荷_01', 'batch正規化 後ろメモつき (実DB)');
expectEq(normalizeBatchNo('緊急在庫'), null, 'batch正規化 緊急在庫カード→null');
expectEq(normalizeBatchNo('I will. ありがとうございました。'), null, 'batch正規化 チャット様テキスト→null');

const mkPage = (id, edited, props) => ({ id, last_edited_time: edited, properties: props });
const pTitle = (s) => ({ type: 'title', title: [{ plain_text: s }] });
const pPeople = (names) => ({ type: 'people', people: names.map((n) => ({ name: n })) });
const pDate = (d) => ({ type: 'date', date: d ? { start: d } : null });
const cfgBase = { propBatch: '出荷No', propPacker: '梱包', propPicker: null, propDate: null, propStatus: null };

// 設定不備は fail-closed
expectEq(!!buildStaffRows([mkPage('p1', '', { 出荷No: pTitle('1') })], { ...cfgBase, propPacker: null }, '2026-07-19').skipped,
  true, 'PACKER未設定 → skipped');
expectEq(!!buildStaffRows([mkPage('p1', '', { 出荷No: pTitle('1') })], { ...cfgBase, propPacker: 'タイプミス' }, '2026-07-19').skipped,
  true, 'プロパティ不存在 → skipped');
// 正常系 (日付プロパティなし = カード使い回し運用 → 当日割当)
const staffBuilt = buildStaffRows([
  mkPage('p1', '2026-07-19T09:00:00Z', { 出荷No: pTitle('1'), 梱包: pPeople(['山田']) }),
  mkPage('p2', '2026-07-19T09:00:00Z', { 出荷No: pTitle('02'), 梱包: pPeople(['佐藤', '鈴木']) }),
  mkPage('p3', '2026-07-19T09:00:00Z', { 出荷No: pTitle('メモ'), 梱包: pPeople([]) }),
], cfgBase, '2026-07-19');
expectEq(staffBuilt.rows?.length, 2, 'staff rows 件数');
expectEq(staffBuilt.noBatch, 1, 'staff 出荷No不明カウント');
expectEq(staffBuilt.rows?.find((r) => r.batch_no === '出荷_02')?.packer, '佐藤、鈴木', 'people 複数名連結');
// 日付プロパティなし + 同一バッチ重複 = 日別カードDBの疑い → skipped
expectEq(!!buildStaffRows([
  mkPage('p1', '2026-07-19T09:00:00Z', { 出荷No: pTitle('1'), 梱包: pPeople(['山田']) }),
  mkPage('p2', '2026-07-19T10:00:00Z', { 出荷No: pTitle('01'), 梱包: pPeople(['佐藤']) }),
], cfgBase, '2026-07-19').skipped, true, 'DATE未設定+重複 → skipped');
// DATE設定時: 日付ありは重複を最終更新でタイブレーク、日付なしカードは除外
const cfgDate = { ...cfgBase, propDate: '作業日' };
const datedBuilt = buildStaffRows([
  mkPage('p1', '2026-07-19T09:00:00Z', { 出荷No: pTitle('1'), 梱包: pPeople(['山田']), 作業日: pDate('2026-07-18') }),
  mkPage('p2', '2026-07-19T10:00:00Z', { 出荷No: pTitle('1'), 梱包: pPeople(['佐藤']), 作業日: pDate('2026-07-18') }),
  mkPage('p3', '2026-07-19T10:00:00Z', { 出荷No: pTitle('2'), 梱包: pPeople(['田中']), 作業日: pDate(null) }),
], cfgDate, '2026-07-19');
expectEq(datedBuilt.rows?.length, 1, 'DATE設定 rows 件数');
expectEq(datedBuilt.rows?.[0]?.packer, '佐藤', '重複は最終更新が新しい方を採用');
expectEq(datedBuilt.noDate, 1, '日付なしカードは除外カウント');
// created_time 型の作業日 (実DB「作成日時」): UTC→JST変換。7/18 23:30 UTC = 7/19 08:30 JST
const pSelect = (s) => ({ type: 'select', select: s ? { name: s } : null });
const pCreated = (iso) => ({ type: 'created_time', created_time: iso });
const createdBuilt = buildStaffRows([
  mkPage('p1', '2026-07-19T09:00:00Z', { 名前: pTitle('出荷_1　パフ済'), 梱包担当者: pSelect('星'), 作成日時: pCreated('2026-07-18T23:30:00.000Z') }),
], { propBatch: '名前', propPacker: '梱包担当者', propPicker: null, propDate: '作成日時', propStatus: null }, '2026-07-19');
expectEq(createdBuilt.rows?.length, 1, 'created_time DATE rows 件数');
expectEq(createdBuilt.rows?.[0]?.work_date, '2026-07-19', 'created_time はJST変換 (UTC 7/18 23:30→JST 7/19)');
expectEq(createdBuilt.rows?.[0]?.batch_no, '出荷_01', '実DB表記の名前→batch正規化');
expectEq(createdBuilt.rows?.[0]?.packer, '星', 'select型の梱包担当者');

// upsert: COALESCE で取得済み担当者を NULL で潰さない
upsertStaffRows([{ work_date: '2026-07-19', batch_no: '出荷_01', packer: '山田', picker: null,
  card_status: null, notion_page_id: 'p1', props_summary: '{}' }]);
upsertStaffRows([{ work_date: '2026-07-19', batch_no: '出荷_01', packer: null, picker: '鈴木',
  card_status: null, notion_page_id: 'p1', props_summary: '{}' }]);
const staffRow = db.prepare(`SELECT packer, picker FROM sl_batch_staff WHERE batch_no='出荷_01'`).get();
expectEq(staffRow.packer, '山田', 'COALESCE packer保持');
expectEq(staffRow.picker, '鈴木', 'COALESCE picker追記');
// 日別カード運用 (preserveNonNull=false): 作業日単位の置換 = 空欄訂正・カード削除も反映
upsertStaffRows([{ work_date: '2026-07-19', batch_no: '出荷_01', packer: null, picker: null,
  card_status: null, notion_page_id: 'p1', props_summary: '{}' }], { preserveNonNull: false });
expectEq(db.prepare(`SELECT packer FROM sl_batch_staff WHERE work_date='2026-07-19' AND batch_no='出荷_01'`).get().packer,
  null, '日別カード運用は空欄訂正を反映');
upsertStaffRows([{ work_date: '2026-07-20', batch_no: '出荷_05', packer: 'A', picker: null,
  card_status: null, notion_page_id: 'p5', props_summary: '{}' }], { preserveNonNull: false });
upsertStaffRows([{ work_date: '2026-07-20', batch_no: '出荷_06', packer: 'B', picker: null,
  card_status: null, notion_page_id: 'p6', props_summary: '{}' }], { preserveNonNull: false });
expectEq(db.prepare(`SELECT COUNT(*) c FROM sl_batch_staff WHERE work_date='2026-07-20'`).get().c,
  1, '日別カード運用は作業日単位で置換 (消えたカードの行が残らない)');
// 突合VIEW: 7/18伝票はstaff無し(null)、staffは7/19なのでJOINされない→dateを合わせて確認
upsertStaffRows([{ work_date: '2026-07-18', batch_no: '出荷_01', packer: '中村', picker: null,
  card_status: null, notion_page_id: 'p9', props_summary: '{}' }]);
const joined = db.prepare(`SELECT packer FROM v_sl_slips_staff WHERE slip_no='SP00110324384'`).get();
expectEq(joined.packer, '中村', 'v_sl_slips_staff 伝票×担当者JOIN');
// 日別カード置換 (7/19・7/20) は取得結果に無い過去日 (7/18) に触らない
upsertStaffRows([{ work_date: '2026-07-20', batch_no: '出荷_06', packer: 'B', picker: null,
  card_status: null, notion_page_id: 'p6', props_summary: '{}' }], { preserveNonNull: false });
expectEq(db.prepare(`SELECT COUNT(*) c FROM sl_batch_staff WHERE work_date='2026-07-18'`).get().c,
  1, '取得結果に無い過去日は保持 (アーカイブされても履歴は残る)');

// HTTP: 認証と状態
delete process.env.NOTION_TOKEN;
expectEq((await post('/sync-staff', {}, null)).status, 401, 'sync-staff token 無し → 401');
const syncRes = await post('/sync-staff', {}, TOKEN);
expectEq(syncRes.status, 200, 'sync-staff (NOTION_TOKEN未設定) → 200');
expectEq(!!syncRes.body.skipped, true, 'sync-staff skipped 明示');
expectEq((await fetch(`${base}/staff-props`)).status, 401, 'staff-props token 無し → 401');
expectEq((await fetch(`${base}/staff-props`, { headers: { 'x-read-token': READ_TOKEN } })).status, 503,
  'staff-props (NOTION_TOKEN未設定) → 503');
const rs = await fetch(`${base}/recent-staff?limit=10`, { headers: { 'x-read-token': READ_TOKEN } });
expectEq(rs.status, 200, 'GET /recent-staff → 200');
expectEq((await rs.json()).rows.length, 3, 'recent-staff 行数');
expectEq(recentStaffFn(10).length, 3, 'recentStaff 行数');

await new Promise((r) => server.close(r));
try { db.close(); } catch { /* teardown */ }
console.log(failed === 0 ? '\n✅ 全テスト PASS' : `\n❌ ${failed} 件失敗`);
process.exitCode = failed === 0 ? 0 : 1;
