/**
 * 入荷受付チェック (apps/inbound-check) — E2E テスト (DB 層 + CSV パーサ)
 *
 * 実行: node scripts/test-inbound-check.mjs [入荷状況照会CSVのパス]
 *   - DATA_DIR 未指定時は一時ディレクトリを作って使う
 *   - CSV パスを渡すと実ファイル (CA04001_*.csv) でパースを検証。省略時は合成 CSV のみ
 *
 * 検証項目:
 *   1. CSV パーサ: 必須列 / 列数不一致 / 数値 / 0件正常 / 明細キー
 *   2. 取込: active 1件・supersede・同一ハッシュ拒否・古い生成時刻拒否・0件取込
 *   3. 消し込み: 条件付き UPDATE (二重確認=conflict / version 不一致=conflict / stale_batch / 取消の reverted_event_id)
 *   4. 表示結合: f_inbound_info の入数・いろは / mirror_logizard_stock の P3F 優先・集約 (明細が増えない)
 *   5. 端末・作業者 (スタッフマスタ参照)・履歴CSV・保持期間の掃除
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import assert from 'assert';
import iconv from 'iconv-lite';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'inbound-check-test-'));
}
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const csvMod = await import('../apps/inbound-check/csv.js');
const dbMod = await import('../apps/inbound-check/db.js');
const { parseInboundCsv } = csvMod;
const {
  getDB, importCsv, getActiveBatch, getState, listEvents, eventsCsv, cleanupOld,
  applyQuantityEvents, listQuantityEvents, finalizeLine, reopenLine, listDestinations, createTables, quantitySum,
  createDevice, verifyDevice, revokeDevice, listDevices, listWorkers, getWorker, productInfoMap, workDateJst,
} = dbMod;

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}
function throwsWith(fn, re, label) {
  try { fn(); ok(false, `${label} (例外なし)`); } catch (e) { ok(re.test(e.message), `${label}: ${e.message}`); }
}

const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '荷主入荷NO', '入荷予定日', '入荷受付日', '入荷確定日',
  '取引先ID', '取引先名', '業務区分名', '商品ID', '商品名', '品質区分名', 'ロケーション', '予定数', '受付数', '検品数', '作成日時', '更新日時', 'バーコード', '備考'];
function makeCsv(rows, { header = HEADER } = {}) {
  const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const lines = [header.map(q).join(',')];
  for (const r of rows) lines.push(header.map(h => q(r[h])).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'cp932');
}
const row = (ar, no, pid, qty, extra = {}) => ({
  入荷管理番号: ar, 入荷管理行番号: no, 入荷管理詳細行番号: 1, ステータス: '受付済', 入荷予定日: '20260831', 入荷受付日: '20260831',
  取引先ID: '0002', 取引先名: 'BF', 業務区分名: '通常入荷', 商品ID: pid, 商品名: `商品 ${pid}`, 品質区分名: '良品',
  予定数: qty, 受付数: qty, 作成日時: '20260831182105', 更新日時: '20260831220311', バーコード: '4500000000000', ...extra,
});

console.log('DATA_DIR =', process.env.DATA_DIR);
initMirrorDB();
// 旧作業者表に行がある状態で初期化 → DROP されない (migration guard)
{
  const { getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
  const m = getMirrorDB();
  m.exec("CREATE TABLE IF NOT EXISTS f_inbound_check_workers (code TEXT PRIMARY KEY, name TEXT NOT NULL, sort INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1)");
  m.prepare("INSERT INTO f_inbound_check_workers (code, name) VALUES ('w01', '先に登録した人')").run();
  getDB();
  ok(!!m.prepare("SELECT 1 FROM sqlite_master WHERE name='f_inbound_check_workers'").get(), '旧作業者表に行があれば DROP しない');
  m.exec('DELETE FROM f_inbound_check_workers');
  const { createTables } = await import('../apps/inbound-check/db.js');
  createTables(m);
  ok(!m.prepare("SELECT 1 FROM sqlite_master WHERE name='f_inbound_check_workers'").get(), '0 行になれば DROP される');
}
const db = getDB();

// ─── 1. CSV パーサ ───
console.log('\n[1] CSV パーサ');
{
  const p = parseInboundCsv(makeCsv([row('AR1', 1, 'abcDEF', 100), row('AR1', 2, 'x2', 12), row('AR2', 1, 'x3', 6)]));
  ok(p.rows.length === 3, '3行パース');
  ok(p.rows[0].line_key === 'AR1|1|1' && p.rows[0].code_key === 'abcdef', 'line_key と code_key (小文字)');
  ok(p.rows[0].planned_date === '2026-08-31' && p.rows[0].created_at === '2026-08-31T18:21:05+09:00', '日付・日時の変換');
  ok(p.rows[2].seq === 3, 'seq = CSV 行順');
  const e = parseInboundCsv(makeCsv([]));
  ok(e.rows.length === 0, 'ヘッダのみ = 0件で正常');
  throwsWith(() => parseInboundCsv(makeCsv([], { header: HEADER.filter(h => h !== '予定数') })), /必須列/, '必須列欠落を拒否');
  const bad = Buffer.concat([makeCsv([row('AR1', 1, 'a', 1)]), iconv.encode('"AR9","1"\r\n', 'cp932')]);
  throwsWith(() => parseInboundCsv(bad), /列数/, '列数不一致を拒否');
  throwsWith(() => parseInboundCsv(makeCsv([row('AR1', 1, 'a', 'abc')])), /整数/, '予定数が数値でない');
  throwsWith(() => parseInboundCsv(makeCsv([row('AR1', 1, 'a', 1), row('AR1', 1, 'b', 1)])), /重複/, '明細キー重複を拒否');
  throwsWith(() => parseInboundCsv(makeCsv([row('', 1, 'a', 1)])), /入荷管理番号/, 'AR 空を拒否');
  throwsWith(() => parseInboundCsv(Buffer.alloc(0)), /空/, '0バイトを拒否');
  throwsWith(() => parseInboundCsv(makeCsv([row('AR1', 1, 'a', 1, { 入荷予定日: '20261399' })])), /日付/, '実在しない日付を拒否');
  throwsWith(() => parseInboundCsv(makeCsv([row('AR1', 1, 'a', '99999999999999999')])), /大きすぎ/, '安全整数超を拒否');
  throwsWith(() => parseInboundCsv(makeCsv([], { header: [...HEADER, '予定数'] })), /重複/, '重複ヘッダを拒否');
  throwsWith(() => parseInboundCsv(makeCsv([], { header: [...HEADER, ''] })), /空です/, '空ヘッダを拒否');
  throwsWith(() => parseInboundCsv(Buffer.concat([makeCsv([row('AR1', 1, 'a', 1)]), Buffer.from([0x82, 0xff, 0x0d, 0x0a])])), /Shift-JIS/, '壊れた CP932 を拒否');
  const p2 = parseInboundCsv(makeCsv([row('AR1', 1, 'a', 5, { 入荷管理詳細行番号: '' , 受付数: '' })]));
  ok(p2.rows[0].detail_no === 1 && p2.rows[0].received_qty === null, '詳細行番号 空→1 / 受付数 空→null');
}
const realPath = process.argv[2];
if (realPath && fs.existsSync(realPath)) {
  const p = parseInboundCsv(fs.readFileSync(realPath));
  ok(p.header.length === 58, `実CSV: 58列 (${p.header.length})`);
  ok(p.rows.length === 16 && p.rows[0].ar_no === 'AR00110005164', `実CSV: 16行 / AR00110005164 (${p.rows.length}行)`);
  ok(p.rows[0].product_id === 'asahilabo15g' && p.rows[0].planned_qty === 100, '実CSV: 1行目 asahilabo15g 100');
  ok(p.rows[12].product_name.includes('アットアロマ'), '実CSV: 日本語商品名 (CP932) が読める');
}

// ─── 2. 取込 ───
console.log('\n[2] 取込');
const csvA = makeCsv([row('AR1', 1, 'abcDEF', 100), row('AR1', 2, 'x2', 12), row('AR2', 1, 'x3', 6)]);
{
  const r = importCsv(csvA, { fileName: 'a.csv', source: 'manual_upload', actor: 'tester', generatedAt: '2026-09-01T00:00:00Z' });
  ok(r.ok && r.rowCount === 3 && r.slipCount === 2, `取込 A: 3行/2伝票 (${JSON.stringify(r.ok)})`);
  ok(getActiveBatch().id === r.batch.id && getActiveBatch().status === 'active', 'active バッチ');
  const dup = importCsv(csvA, { fileName: 'a2.csv', source: 'manual_upload', generatedAt: '2026-09-01T01:00:00Z' });
  ok(!dup.ok && dup.error === 'duplicate_file', '同一ハッシュを拒否');
  const older = importCsv(makeCsv([row('AR9', 1, 'z', 1)]), { generatedAt: '2026-08-31T00:00:00Z' });
  ok(!older.ok && older.error === 'older_file' && getActiveBatch().id === r.batch.id, '生成時刻が古いファイルを拒否 (active は据え置き)');
  const badcsv = importCsv(Buffer.from('garbage'), { generatedAt: '2026-09-02T00:00:00Z' });
  ok(!badcsv.ok && badcsv.error === 'bad_csv' && getActiveBatch().id === r.batch.id, '壊れたCSVを拒否 (active は据え置き)');
  const st = getState();
  ok(st.slips.length === 2 && st.lines.length === 3 && st.totals.checked === 0, 'state: 2伝票 3行 未確認');
  ok(st.lines.every(l => l.check_status === 'unchecked' && l.version === 1), '全行 unchecked / version 1');
  const logs = db.prepare('SELECT ok, message FROM f_inbound_check_import_log ORDER BY id').all();
  ok(logs.length === 4 && logs[0].ok === 1 && logs.slice(1).every(l => l.ok === 0), '取込ログに成功1・失敗3');
}

// ─── 3. 確定 (finalize) と やり直し (reopen) ───
console.log('\n[3] 確定・やり直し');
const FILL = id => ({ client_event_id: id });
{
  const b = getActiveBatch();
  // 「全部あり」= 残り (予定 − 見つけた) を1イベント足してから exact 確定する
  const r1 = finalizeLine({ batchId: b.id, lineKey: 'AR1|1|1', expectVersion: 1, expectQuantityVersion: 1,
    result: 'exact', mode: 'fill_remaining', fillEvent: FILL('ev-fill-ar1-1'),
    worker: '山田', staffId: 7, deviceId: 1, deviceLabel: 'iPad1' });
  ok(r1.ok && r1.state.status === 'checked' && r1.state.version === 2 && r1.state.found_qty === 100 && r1.state.finalized_result === 'exact',
    '全部あり → checked v2 / 100個 / exact');
  const r2 = finalizeLine({ batchId: b.id, lineKey: 'AR1|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', mode: 'current', worker: '佐藤' });
  ok(!r2.ok && r2.error === 'finalized' && r2.current.checked_by === '山田', '確定済みへの確定 = finalized + 現在状態');
  for (const bad of [undefined, null, 0, -1, 1.5, NaN, '2']) {
    const rb = finalizeLine({ batchId: b.id, lineKey: 'AR1|2|1', expectVersion: bad, expectQuantityVersion: 1, result: 'exact', worker: '佐藤' });
    ok(!rb.ok && rb.error === 'bad_request', `expectVersion=${String(bad)} は bad_request`);
  }
  const r3 = finalizeLine({ batchId: b.id, lineKey: 'AR1|2|1', expectVersion: 99, expectQuantityVersion: 1, result: 'exact', mode: 'current', worker: '佐藤' });
  ok(!r3.ok && r3.error === 'conflict', 'version 不一致 = conflict');
  const r4 = reopenLine({ batchId: b.id, lineKey: 'AR1|2|1', expectVersion: 1, worker: '佐藤' });
  ok(!r4.ok && r4.error === 'conflict', '未確認行のやり直し = conflict');
  const r5 = finalizeLine({ batchId: b.id, lineKey: 'NOPE', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', worker: 'x' });
  ok(!r5.ok && r5.error === 'not_found', '存在しない明細 = not_found');
  const r6 = finalizeLine({ batchId: b.id, lineKey: 'AR2|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', worker: '' });
  ok(!r6.ok && r6.error === 'bad_request', '作業者なし = bad_request');
  const r7 = finalizeLine({ batchId: b.id, lineKey: 'AR2|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', mode: 'current', worker: '山田' });
  ok(!r7.ok && r7.error === 'result_mismatch', '0個なのに exact = result_mismatch (人が選んだ意味と実数が食い違ったまま確定させない)');
  const st = getState();
  ok(st.totals.checked === 1 && st.slips.find(s => s.ar_no === 'AR1').checked_count === 1, 'state に確認数が反映');

  // やり直し = 確認だけ外し、数えた数は残す
  const un = reopenLine({ batchId: b.id, lineKey: 'AR1|1|1', expectVersion: 2, worker: '山田' });
  ok(un.ok && un.state.status === 'unchecked' && un.state.version === 3 && un.state.checked_by === null, 'やり直す → unchecked v3');
  ok(un.state.found_qty === 100 && un.state.finalized_result === null, 'やり直しても数量100は残る (誤タップで記録が消えない)');
  const ev = listEvents(b.id);
  ok(ev.length === 2 && ev[1].action === 'uncheck' && ev[1].reverted_event_id === ev[0].id, 'やり直しイベントが確認イベントを指す');
  ok(ev[0].staff_id === 7 && ev[0].result === 'exact' && ev[0].found_qty === 100 && ev[0].planned_qty_snapshot === 100,
    'events に staff_id / result / 確定時点の数量スナップショットが残る');
  // 数量が残っているので、再確定は fill 不要 (mode='current')
  ok(finalizeLine({ batchId: b.id, lineKey: 'AR1|1|1', expectVersion: 3, expectQuantityVersion: 2, result: 'exact', mode: 'current', worker: '山田' }).ok,
    '再確定 (version 3 / 数量そのまま) OK');
  ok(finalizeLine({ batchId: b.id, lineKey: 'AR2|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', mode: 'fill_remaining', fillEvent: FILL('ev-fill-ar2-1'), worker: '山田' }).ok, 'AR2 確認');
  ok(getState().totals.checked === 2, '確認数 2');
  const csv = eventsCsv(b.id);
  ok(csv.charCodeAt(0) === 0xFEFF && csv.split('\r\n').length >= 5 && csv.includes('打ち消した確認ID'), '履歴CSV (BOM + 4イベント)');
}

// ─── 3b. 数量 (部分確認) ───
// 要件定義 v1.3 §11。加算イベント / 冪等ID / 訂正 / 不足確定 / 台帳の実数
console.log('\n[3b] 数量 (部分確認)');
{
  const b = getActiveBatch();
  const K = 'AR1|2|1';                     // 予定 12
  const add = (id, q, kind = 'box', u = 4) => ({ client_event_id: id, action: 'add', quantity: q, input_kind: kind, unit_size: u });

  let x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 1, events: [add('ev-q-0001', 4)], worker: '山田', packQty: 4 });
  ok(x.ok && x.state.found_qty === 4 && x.state.quantity_version === 2, '＋1箱 (4入り) → 4個 / quantity_version=2');
  ok(getState().lines.find(l => l.line_key === K).quantity_relation === 'shortage', '4/12 は shortage (= 画面では「一部」)');
  ok(getState().totals.partial === 1, 'totals.partial = 1 (partial は status ではなく found_qty から導出)');

  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 2, events: [add('ev-q-0001', 4)], worker: '山田' });
  ok(x.ok && x.replayed && x.state.found_qty === 4, '同じ client_event_id の再送は二重加算しない (応答だけ失われた時の押し直し)');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 2, events: [add('ev-q-0001', 8)], worker: '山田' });
  ok(!x.ok && x.error === 'idempotency_conflict', '同じ操作IDで違う内容 = idempotency_conflict');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 1, events: [add('ev-q-0002', 4)], worker: '佐藤' });
  ok(!x.ok && x.error === 'conflict' && x.current.found_qty === 4, '古い quantity_version = conflict + 現在値');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 2, events: [add('ev-q-0002', 4)], worker: '佐藤' });
  ok(x.ok && x.state.found_qty === 8, '別の人の加算は足し算になる (絶対値の上書きではない)');

  const evs = listQuantityEvents(b.id, K);
  ok(evs.length === 2 && evs[0].quantity === 4 && evs[0].reversed === 0, '数量イベント履歴 2件・未打消');
  // 訂正 = 打ち消し + 新しい入数での加算 (元イベントを replaces_event_seq で指す)
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 3, worker: '山田', events: [
    { client_event_id: 'ev-q-0003', action: 'reversal', quantity: 4, input_kind: 'correction', reverses_event_seq: evs[0].event_seq },
    { client_event_id: 'ev-q-0004', action: 'add', quantity: 6, input_kind: 'correction', unit_size: 6, replaces_event_seq: evs[0].event_seq },
  ] });
  ok(x.ok && x.state.found_qty === 10, '入数の訂正 (4入り→6入り) → 10個');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 4, worker: '山田',
    events: [{ client_event_id: 'ev-q-0005', action: 'reversal', quantity: 4, input_kind: 'correction', reverses_event_seq: evs[0].event_seq }] });
  ok(!x.ok && x.error === 'already_reversed', '同じ加算の二重打ち消し = already_reversed');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 4, worker: '山田',
    events: [{ client_event_id: 'ev-q-0006', action: 'add', quantity: 6, input_kind: 'correction', replaces_event_seq: evs[0].event_seq }] });
  ok(!x.ok && x.error === 'bad_request', '訂正の add だけを単独で送るのは拒否 (打ち消しと同時に送る)');

  // 不足のまま確定 → 台帳には「実数」が残る
  let f = finalizeLine({ batchId: b.id, lineKey: K, expectVersion: 1, expectQuantityVersion: 4, result: 'exact', mode: 'current', worker: '山田' });
  ok(!f.ok && f.error === 'result_mismatch', '10/12 で exact は result_mismatch');
  f = finalizeLine({ batchId: b.id, lineKey: K, expectVersion: 1, expectQuantityVersion: 4, result: 'shortage', mode: 'current',
    worker: '山田', clientOperationId: 'op-short-0001', decide: () => ({ ok: true, destination: 'iroha', decidedFrom: 'chosen' }) });
  ok(f.ok && f.state.finalized_result === 'shortage' && f.state.found_qty === 10, '不足のまま確定 → checked / shortage / 10個');
  const dests = listDestinations({ destination: 'iroha' }).filter(d => !d.cancelled_at);
  ok(dests.length === 1 && dests[0].actual_qty === 10 && dests[0].planned_qty === 12,
    '行き先台帳に実数10 (予定12ではなく、実際にいろはへ送る数が残る)');
  ok(getState().totals.toIroha === 1 && getState().totals.toIrohaQty === 10, 'totals のいろは件数・個数は台帳から数える');
  const again = finalizeLine({ batchId: b.id, lineKey: K, expectVersion: 1, expectQuantityVersion: 4, result: 'shortage', worker: '山田', clientOperationId: 'op-short-0001' });
  ok(again.ok && again.replayed && listDestinations({ destination: 'iroha' }).filter(d => !d.cancelled_at).length === 1,
    '確定の再送は台帳を増やさない (client_operation_id で冪等)');
  x = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: 4, events: [add('ev-q-0007', 2)], worker: '山田' });
  ok(!x.ok && x.error === 'finalized', '確定済みの行には数を足せない (先にやり直す)');

  // 打ち消しで合計が負にならない
  const ro = reopenLine({ batchId: b.id, lineKey: K, expectVersion: 2, worker: '山田' });
  ok(ro.ok && ro.state.found_qty === 10, 'やり直しても数量は残る');
  ok(listDestinations({ destination: 'iroha' }).filter(d => !d.cancelled_at).length === 0, 'やり直しで行き先台帳が取り消される (二重計上を防ぐ)');
  const all = listQuantityEvents(b.id, K);
  const live = all.filter(e => e.action === 'add' && !e.reversed);
  let qv = ro.state.quantity_version, seq = 0;
  for (const e of live) {
    const rr = applyQuantityEvents({ batchId: b.id, lineKey: K, expectQuantityVersion: qv, worker: '山田',
      events: [{ client_event_id: `ev-q-undo-${++seq}`, action: 'reversal', quantity: e.quantity, input_kind: 'correction', reverses_event_seq: e.event_seq }] });
    qv = rr.state.quantity_version;
  }
  ok(getState().lines.find(l => l.line_key === K).found_qty === 0, '全部打ち消すと 0 に戻る (行は消さず reversal を積む)');
  // 最後に確定しておく ([2b] が「前回確認済み」を見るため)
  ok(finalizeLine({ batchId: b.id, lineKey: K, expectVersion: 3, expectQuantityVersion: qv, result: 'exact', mode: 'fill_remaining',
    fillEvent: FILL('ev-fill-ar12-1'), worker: '=HYPERLINK("x")' }).ok, '残りも全部あり → exact 確定');
  ok(eventsCsv(b.id).includes("'=HYPERLINK"), '履歴CSV: 先頭 = の値はアポストロフィで無害化');
}

// ─── 新バッチ: 同日は引き継ぎ / 翌日はリセット・旧 batch は stale ───
// ⚠miniPC は 08:40 と 11:45 の1日2回取り込む。同日の2回目で午前中の確認が消えると現場が二度手間になる
console.log('\n[2b] 新バッチ (同日引き継ぎ / 翌日リセット)');
{
  const old = getActiveBatch();
  // AR1|2|1 は予定数を 12 → 20 に変える (数えたものが違うので引き継がない)
  const csvB = makeCsv([row('AR1', 1, 'abcDEF', 100), row('AR1', 2, 'x2', 20), row('AR3', 1, 'new', 3)]);
  const r = importCsv(csvB, { fileName: 'b.csv', source: 'manual_upload', generatedAt: '2026-09-02T00:00:00Z' });
  ok(r.ok && getActiveBatch().id === r.batch.id, '取込 B が active');
  ok(db.prepare("SELECT COUNT(*) c FROM f_inbound_check_batches WHERE status='active'").get().c === 1, 'active は常に1件');
  ok(getActiveBatch().work_date === workDateJst(), '新バッチに work_date (JST) が入る');
  const st = getState();
  const l1 = st.lines.find(l => l.line_key === 'AR1|1|1');
  ok(l1.check_status === 'checked' && l1.checked_by === '山田', '同日の再取込: 明細キー・商品・予定数が同じ行は確認を引き継ぐ');
  ok(r.carriedOver === 1, '取込結果に引き継ぎ件数 (carriedOver)');
  const l2 = st.lines.find(l => l.line_key === 'AR1|2|1');
  ok(l2.check_status === 'unchecked' && !!l2.prev_checked, '予定数が変わった明細は引き継がず未確認 (前回確認は参考表示)');
  ok(st.lines.find(l => l.line_key === 'AR3|1|1').check_status === 'unchecked', '新しい明細は未確認');
  const stale = finalizeLine({ batchId: old.id, lineKey: 'AR1|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact', worker: '山田' });
  ok(!stale.ok && stale.error === 'stale_batch' && stale.activeBatchId === r.batch.id, '旧 batch_id の操作 = stale_batch');
  // 翌日の取込 (active の work_date を1日戻して再現) は引き継がない = 要件 §2 確定事項⑤ 毎朝リセット
  db.prepare("UPDATE f_inbound_check_batches SET work_date = date(work_date, '-1 day') WHERE id = ?").run(r.batch.id);
  const rc = importCsv(makeCsv([row('AR1', 1, 'abcDEF', 100), row('AR3', 1, 'new', 3)]), { fileName: 'c.csv', generatedAt: '2026-09-02T06:00:00Z' });
  ok(rc.ok && rc.carriedOver === 0, '翌日の取込は引き継がない');
  const st2 = getState();
  ok(st2.lines.every(l => l.check_status === 'unchecked'), '翌日は全行 unchecked (毎朝リセット)');
  ok(st2.lines.find(l => l.line_key === 'AR1|1|1').prev_checked.by === '山田', '前回確認済みの参考表示');
  ok(getActiveBatch().data_max_at === '2026-08-31T22:03:11+09:00', 'data_max_at = 明細の更新日時の最大');
  const olderData = importCsv(makeCsv([row('AR7', 1, 'q', 1, { 更新日時: '20260830090000', 作成日時: '20260830090000' })]), { fileName: 'old-data.csv', generatedAt: '2026-09-09T00:00:00Z' });
  ok(!olderData.ok && olderData.error === 'older_file' && /明細/.test(olderData.message), '生成時刻が新しくても明細時刻が古いCSVは拒否 (File.lastModified を信用しない)');
  const z = importCsv(makeCsv([]), { fileName: 'zero.csv', generatedAt: '2026-09-03T00:00:00Z' });
  ok(z.ok && z.rowCount === 0 && getState().lines.length === 0 && getState().batch.id === z.batch.id, '0件CSVも取り込める (一覧は空)');
}

// ─── 4. 表示結合 ───
console.log('\n[4] 入数・行き先・ピックロケ');
{
  const now = new Date().toISOString();
  db.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, 直接ピックロケ保管, BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES ('abcdef', 'abcDEF', '商品', 10, '要', '有り', 'ケース', '無し', 'manual', ?, ?)`).run(now, now);
  db.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES ('x2', 'x2', '商品2', NULL, '有り', 'manual', ?, ?)`).run(now, now);
  const ins = db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, ブロック略称, ロケ, 在庫数, 引当数, captured_at, synced_at) VALUES (?, ?, ?, ?, 0, ?, ?)`);
  ins.run('abcdef', 'P3FB', '004-014-04', 2, now, now);
  ins.run('abcdef', 'P3FB', '004-014-04', 3, now, now);   // 同ロケ別行 (有効期限違い等) → 集約
  ins.run('abcdef', 'P3FD', '001-001-01', 9, now, now);   // 在庫多い → 先頭
  ins.run('abcdef', 'R1FA', '001-001-01', 100, now, now); // いろは棟 = ピックロケではない
  ins.run('abcdef', 'P3FC', '002-002-02', 0, now, now);   // 在庫0 → 除外
  ins.run('x2', 'R1FA', '002-002-02', 5, now, now);       // P3F なし → 保管
  const m = productInfoMap(['abcDEF', 'x2', 'nothing']);
  const a = m.get('abcdef');
  ok(a.info && a.info.irisu === 10 && a.info.iroha === '無し', 'f_inbound_info 結合 (大文字コードでも code_key で引ける)');
  ok(a.loc_source === 'pick' && a.pick_locs.map(x => x.loc).join(',') === 'P3FD-001-001-01,P3FB-004-014-04', `P3F 優先・在庫数順・同ロケ集約 (${a.pick_locs.map(x => x.loc + ':' + x.qty).join(',')})`);
  ok(a.pick_locs[1].qty === 5, '同ロケの在庫が合算 (2+3)');
  const x = m.get('x2');
  ok(x.loc_source === 'storage' && x.other_locs[0].loc === 'R1FA-002-002-02' && x.info.irisu === null, 'P3F なし → 保管ロケ / 入数 NULL');
  ok(m.get('nothing').loc_source === 'none' && m.get('nothing').info === null, '未登録商品 = none');
  const csvC = makeCsv([row('AR5', 1, 'abcDEF', 106), row('AR5', 2, 'abcDEF', 4), row('AR5', 3, 'x2', 1)]);
  importCsv(csvC, { fileName: 'c.csv', generatedAt: '2026-09-04T00:00:00Z' });
  const st = getState();
  ok(st.lines.length === 3, '複数ロケがあっても明細行は増えない (JOIN 増殖なし)');
  ok(st.lines[0].info.irisu === 10 && st.lines[0].pick_locs.length === 2 && st.lines[1].pick_locs.length === 2, '同一商品の2行に同じ補助情報');
}

// ─── 5. 端末・作業者・掃除 ───
console.log('\n[5] 端末・作業者・掃除');
{
  const { token: tok, id: devId } = createDevice('入荷iPad1', 'admin@example.com');
  ok(Number.isInteger(devId) && devId > 0, 'createDevice は id も返す');
  const d = verifyDevice(tok);
  ok(d && d.label === '入荷iPad1', '端末登録 → 検証OK');
  ok(verifyDevice('bogus') === null, '不正トークン = null');
  ok(revokeDevice(d.id) && verifyDevice(tok) === null, '失効後は検証NG');
  ok(listDevices().length === 1 && listDevices()[0].revoked_at, '端末一覧に失効が見える');
  const ws = listWorkers();
  ok(ws.length === 10 && ws[0].code === '0001' && ws[0].name === '中原 大輔', `listWorkers = スタッフマスタの倉庫作業者 (${ws.length}名・事務3名は出ない)`);
  ok(!ws.some(w => w.code === '0003'), '事務担当 (谷川 泰仁) は名前タップに出ない');
  const gw = getWorker('20250901');
  ok(gw && gw.name === '星 立夏' && Number.isInteger(gw.staff_id), 'getWorker(管理番号) → 名前 + staff_id');
  ok(getWorker('w01') === null, '旧コード w01 は存在しない');
  ok(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='f_inbound_check_workers'").get(), '旧作業者表は DROP 済み');
  // 保持期間: 古い superseded バッチを消す (active は消さない)
  db.prepare("UPDATE f_inbound_check_batches SET imported_at = '2020-01-01T00:00:00.000Z' WHERE status = 'superseded'").run();
  db.prepare("UPDATE f_inbound_check_import_log SET at = '2020-01-01T00:00:00.000Z'").run();
  const c = cleanupOld(db);
  ok(c.batches >= 1 && db.prepare('SELECT COUNT(*) c FROM f_inbound_check_batches').get().c === 1, `古いバッチを削除 (${c.batches}) / active は残る`);
  ok(db.prepare('SELECT COUNT(*) c FROM f_inbound_check_events').get().c === 0 && db.prepare('SELECT COUNT(*) c FROM f_inbound_check_lines WHERE batch_id NOT IN (SELECT id FROM f_inbound_check_batches)').get().c === 0, '子行も CASCADE で消える');
  ok(c.logs >= 1, `取込ログの掃除 (${c.logs})`);
}

// ─── 9. 移行 (既存の確認済みを数量つきに引き上げる) ───
// ⭐**バックフィルまでが1セット**。ここが抜けると、本番の当日分が全部「0 / 106個」に見えて
//   現場が数え直す羽目になり、さらに同日の再取込で数量が消える (要件定義 v1.3 §11.8)
// ─── 9. 移行 (既存の確認済みを数量つきに引き上げる) ───
// ⭐**バックフィルまでが1セット**。ここが抜けると、本番の当日分が全部「0 / 106個」に見えて
//   現場が数え直す羽目になり、さらに同日の再取込で数量が消える (要件定義 v1.3 §11.8)
console.log('\n[9] 移行 (バックフィル)');
{
  // 数量を入れた行がある状態で「数量の列が無かった頃の DB」を作り直す
  const rm = importCsv(makeCsv([row('AR9', 1, 'mig1', 50), row('AR9', 2, 'mig2', 8)]), { fileName: 'mig.csv', generatedAt: '2026-09-20T00:00:00Z' });
  const bm = rm.batch;
  finalizeLine({ batchId: bm.id, lineKey: 'AR9|1|1', expectVersion: 1, expectQuantityVersion: 1, result: 'exact',
    mode: 'fill_remaining', fillEvent: { client_event_id: 'ev-mig-0001' }, worker: '山田' });
  ok(getState().lines.find(l => l.line_key === 'AR9|1|1').found_qty === 50, '移行前の準備: 50個で確定');

  // 旧スキーマを再現する (列を落とし、数量イベントを消す)
  db.exec('DELETE FROM f_inbound_check_quantity_events');
  for (const c of ['found_qty', 'quantity_version', 'quantity_work_date', 'finalized_result', 'destination_id', 'current_pack_qty']) {
    db.exec(`ALTER TABLE f_inbound_check_line_state DROP COLUMN ${c}`);
  }
  ok(!db.prepare('PRAGMA table_info(f_inbound_check_line_state)').all().map(c => c.name).includes('found_qty'), '旧スキーマ (found_qty 無し) を再現');

  createTables(db);   // ← ここで移行が走る

  const cols1 = db.prepare('PRAGMA table_info(f_inbound_check_line_state)').all().map(c => c.name);
  ok(cols1.includes('found_qty') && cols1.includes('quantity_version') && cols1.includes('finalized_result'), '列が足される');
  const s1 = db.prepare("SELECT * FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = 'AR9|1|1'").get(bm.id);
  ok(s1.found_qty === 50 && s1.finalized_result === 'exact', '既存の確認済みは found_qty = 予定数 / exact で埋まる (0個にならない)');
  const s2 = db.prepare("SELECT * FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = 'AR9|2|1'").get(bm.id);
  ok(s2.found_qty === 0 && s2.finalized_result === null, '未確認の行は 0 のまま');
  const synth = db.prepare('SELECT * FROM f_inbound_check_quantity_events WHERE client_event_id = ?').get(`backfill:${bm.id}:AR9|1|1`);
  ok(synth && synth.quantity === 50 && synth.input_kind === 'backfill' && synth.worker === '山田',
    '合成の加算イベントも作られる (これが無いと同日の再取込で数量が消える)');
  ok(quantitySum(db, bm.work_date, 'AR9|1|1', 'mig1') === 50, 'イベント集計も 50 になる');

  createTables(db);   // 2回流しても増えない (冪等)
  ok(db.prepare('SELECT COUNT(*) c FROM f_inbound_check_quantity_events WHERE client_event_id = ?').get(`backfill:${bm.id}:AR9|1|1`).c === 1,
    '移行を再実行しても合成イベントは増えない (冪等)');

  // 同日の再取込で、移行した数量がそのまま引き継がれる
  const rm2 = importCsv(makeCsv([row('AR9', 1, 'mig1', 50), row('AR9', 2, 'mig2', 8), row('AR9', 3, 'mig3', 5)]), { fileName: 'mig2.csv', generatedAt: '2026-09-20T02:00:00Z' });
  const lm = getState().lines.find(x => x.line_key === 'AR9|1|1');
  ok(rm2.ok && lm.check_status === 'checked' && lm.found_qty === 50, '移行後の同日再取込でも確認と数量が残る');
}

console.log('\n[13] 完了一覧 (棚入れ・確認用)');
{
  const { listCompletedSlips, completedSlipsCsv } = await import('../apps/inbound-check/db.js');
  // 台帳に直接入れて検証する (確認の経路そのものは [8]〜[12] で検証済み)
  const today = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo' }).format(new Date());
  const ins = db.prepare(`INSERT INTO f_inbound_check_destinations
    (batch_id, line_key, ar_no, product_id, product_name, planned_qty, actual_qty, destination, decided_from,
     worker, decided_at, work_date, code_key, expiry_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  const at = new Date().toISOString();
  db.prepare('DELETE FROM f_inbound_check_destinations').run();
  ins.run(9001, 'ARX|1|1', 'ARX', 'p1', '商品1', 10, 10, 'bfaith', 'master', '山田', at, today, 'p1', null);
  ins.run(9001, 'ARX|2|1', 'ARX', 'p2', '商品2', 20, 18, 'bfaith', 'master', '山田', at, today, 'p2', '2027-06-30');
  ins.run(9001, 'ARY|1|1', 'ARY', 'p3', '商品3', 5, 5, 'iroha', 'chosen', '星', at, today, 'p3', '2027-06');

  const slips = listCompletedSlips({ days: 2 });
  ok(slips.length === 2, `完了した伝票が2件 (${slips.length})`);
  const x = slips.find(s => s.ar_no === 'ARX');
  ok(x && x.lines.length === 2 && x.done === true, 'ARX は2行で完了');
  ok(x.expiry_count === 1, '期限のある行数を数える');
  ok(x.lines[0].expiry_date === '2027-06-30', '期限のある行を先に出す (棚入れで先に片付ける)');
  ok(x.lines[0].actual_qty === 18 && x.lines[0].planned_qty === 20, '予定と実数の両方を持つ');
  const y = slips.find(s => s.ar_no === 'ARY');
  ok(y.iroha_count === 1 && y.lines[0].destination === 'iroha', 'いろは行きの件数と行き先');

  // 取り消した行は出さない
  db.prepare("UPDATE f_inbound_check_destinations SET cancelled_at = ? WHERE line_key = 'ARY|1|1'").run(at);
  ok(!listCompletedSlips({ days: 2 }).some(s => s.ar_no === 'ARY'), '取り消した行は完了一覧に出ない');

  // AR で絞れる (作業画面の「📋 一覧」から来たとき)
  const only = listCompletedSlips({ arNo: 'ARX' });
  ok(only.length === 1 && only[0].ar_no === 'ARX', 'AR で絞り込める');

  // 同じ明細を確認し直したら最新だけを出す
  ins.run(9001, 'ARX|2|1', 'ARX', 'p2', '商品2', 20, 20, 'bfaith', 'master', '星', at, today, 'p2', '2028-01-31');
  const again = listCompletedSlips({ arNo: 'ARX' })[0];
  ok(again.lines.length === 2, 'やり直しても行が増えない');
  ok(again.lines.find(l => l.product_id === 'p2').expiry_date === '2028-01-31', 'やり直したら最新の期限を出す');

  const csv = completedSlipsCsv({ days: 2 });
  ok(csv.charCodeAt(0) === 0xFEFF, 'CSV は BOM つき');
  ok(/有効期限/.test(csv) && /2028-01-31/.test(csv), 'CSV に有効期限が入る');
  ok(/商品1/.test(csv) && /商品2/.test(csv), 'CSV に明細が入る');
}

console.log('\n[14] 入庫情報の選択肢 (プルダウン)');
{
  const { fieldOptions } = await import('../apps/inbound-check/db.js');
  const now2 = new Date().toISOString();
  db.prepare(`INSERT OR REPLACE INTO f_inbound_info
    (code_key, 商品コード, 商品名, BF保管荷姿, source, created_at, updated_at)
    VALUES ('optx', 'optX', '選択肢テスト', '特注ケース', 'manual', ?, ?)`).run(now2, now2);
  const o = fieldOptions();
  ok(Array.isArray(o.BF保管荷姿) && o.BF保管荷姿.includes('特注ケース'), '入庫情報に入った新しい表記が選択肢に並ぶ (専用の表は作らない)');
  ok(o.BF保管荷姿.includes('そのまま'), '土台の選択肢は必ず含む (1件も無い列でも空にしない)');
  ok(o.いろは在庫化作業有無.includes('有り') && o.いろは在庫化作業有無.includes('無し'), 'いろはの選択肢');
  ok(!o.BF保管荷姿.includes('－'), 'いろは=有り の印 (－) は選択肢に出さない');
  ok(new Set(o.BF保管荷姿).size === o.BF保管荷姿.length, '同じ表記が二重に並ばない');
  // よく使われている順 (件数の多い表記が上)
  for (const c of ['o1', 'o2', 'o3']) {
    db.prepare(`INSERT OR REPLACE INTO f_inbound_info (code_key, 商品コード, 商品名, BF保管荷姿, source, created_at, updated_at)
      VALUES (?, ?, '順序テスト', 'よく使う', 'manual', ?, ?)`).run(c, c, now2, now2);
  }
  ok(fieldOptions().BF保管荷姿[0] === 'よく使う', '件数の多い表記が先頭に来る');
}

console.log('\n[PR-B] いろは行きの確定 → 在庫化アプリのタスクを同時に作る / やり直し・再取込 → 取消');
{
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const IW = await import('../apps/iroha-work/db.js');
  const { createTaskForDestination } = await import('../apps/iroha-work/task-intake.js');
  const taskOf = (destId) => TD.getTaskByDestination(destId);
  const destOf = (id) => db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = ?').get(id);
  const lineState = (key) => getState().lines.find(l => l.line_key === key);
  const destIdOf = (key) => db.prepare('SELECT destination_id FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?').get(getActiveBatch().id, key)?.destination_id ?? null;
  db.prepare(`INSERT OR REPLACE INTO f_iroha_work_master (code_key, 商品コード, material_code, storage_container, units_per_container, process_count, note, version, updated_at)
    VALUES ('task-a', 'TASK-A', 'D-8', '透明袋', 180, 2, 'メモ', 3, '2026-09-03T00:00:00Z')`).run();
  const imp = importCsv(makeCsv([row('AR9', 1, 'TASK-A', 6, { 商品名: 'タスク商品A', バーコード: '4599999999991' }), row('AR9', 2, 'TASK-B', 3), row('AR9', 3, 'TASK-C', 2)]),
    { fileName: 'prb.csv', source: 'manual_upload', actor: 'tester', generatedAt: '2027-01-01T00:00:00Z' });
  ok(imp.ok, '前提: 新しい取込');
  const b = getActiveBatch();
  const decideIroha = () => ({ ok: true, destination: 'iroha', decidedFrom: 'chosen', expiryDate: '2027-03' });
  const fin = (key, extra) => finalizeLine({ batchId: b.id, lineKey: key, expectVersion: 1, expectQuantityVersion: 1, result: 'exact', mode: 'fill_remaining', worker: '山田', ...extra });
  const f1 = fin('AR9|1|1', { fillEvent: FILL('ev-prb-1'), clientOperationId: 'op-prb-0001', decide: decideIroha });
  ok(f1.ok && destIdOf('AR9|1|1'), '前提: いろは行きで確定');
  const d1 = destOf(destIdOf('AR9|1|1'));
  const t1 = taskOf(d1.id);
  ok(t1 && t1.status === 'not_started' && t1.facility_code == null && t1.notion_page_id == null,
    '確定と同時にタスクができる (未着手・拠点は未定・Notion ページなし)');
  ok(t1.product_code === 'TASK-A' && t1.product_name === 'タスク商品A' && t1.qty === 6 && t1.ar_no === 'AR9' && t1.barcode === '4599999999991'
    && t1.expiry === '2027-03' && t1.arrival_date === d1.work_date, 'タスクに商品コード・名前・数量 (実数)・入荷管理番号・バーコード・有効期限・入庫日が載る');
  const snap = JSON.parse(t1.master_snapshot);
  ok(snap.material_code === 'D-8' && snap.storage_container === '透明袋' && snap.units_per_container === 180 && snap.process_count === 2 && snap.version === 3, '作業仕様のスナップショット (作成時点の値)');
  const payload = JSON.parse(t1.payload);
  ok(payload['入庫日'] === d1.work_date && payload['作業拠点'] === 'いろは' && payload.destination_id === d1.id && payload.source === 'inbound_check', 'payload は Notion 時代と同じキー名');
  ok(t1.created_by === 'inbound:山田' && db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_created' AND task_id = ?").get(t1.id).c === 1, '誰が確定したかと履歴が残る');
  const again = fin('AR9|1|1', { fillEvent: FILL('ev-prb-1'), clientOperationId: 'op-prb-0001', decide: decideIroha });
  ok(again.ok && again.replayed && db.prepare('SELECT COUNT(*) c FROM f_iroha_tasks WHERE destination_id = ?').get(d1.id).c === 1, '再送 (同じ操作ID) でタスクは増えない');
  ok(createTaskForDestination(destOf(d1.id)).action === 'exists', '同じ行き先からもう一度作ろうとしても既存を返す (冪等)');
  const f2 = fin('AR9|2|1', { fillEvent: FILL('ev-prb-2'), decide: () => ({ ok: true, destination: 'bfaith', decidedFrom: 'chosen' }) });
  ok(f2.ok && destIdOf('AR9|2|1') && !taskOf(destIdOf('AR9|2|1')), 'B-Faith 行きはタスクにならない');
  const f3 = fin('AR9|3|1', { fillEvent: FILL('ev-prb-3'), worker: '鈴木', decide: decideIroha });
  const d3Id = destIdOf('AR9|3|1');
  const t3 = taskOf(d3Id);
  ok(t3 && t3.master_snapshot == null && t3.product_code === 'TASK-C' && t3.created_by === 'inbound:鈴木', '作業仕様が無い商品も未着手で作られる (スナップショット無し)');

  // やり直し (未着手・実績なし) → 自動で取消
  const s1 = lineState('AR9|1|1');
  ok(reopenLine({ batchId: b.id, lineKey: 'AR9|1|1', expectVersion: s1.version, expectQuantityVersion: s1.quantity_version, worker: '山田', clientOperationId: 'op-prb-ro1' }).ok, '前提: やり直し');
  const t1b = taskOf(d1.id);
  ok(t1b.status === 'closed' && t1b.close_reason === 'cancelled' && t1b.cancellation_source === 'inbound_reversal', 'やり直すと未着手のタスクは自動で取消 (終了:取消)');
  // 着手後のやり直し → 自動取消せず要確認
  const w = IW.addIrohaWorker({ displayName: 'プラビー', workerType: 'member', actor: 'test' });
  const s3 = IW.startSession({ taskId: t3.id, worker: IW.getIrohaWorker(w.id) });
  ok(s3.ok, '前提: 別のタスクで作業を開始');
  const ls3 = lineState('AR9|3|1');
  ok(reopenLine({ batchId: b.id, lineKey: 'AR9|3|1', expectVersion: ls3.version, expectQuantityVersion: ls3.quantity_version, worker: '鈴木' }).ok, '前提: 着手後にやり直し');
  const t3b = taskOf(d3Id);
  ok(t3b.status !== 'closed' && t3b.cancellation_requested_at && t3b.cancellation_source === 'inbound_reversal', '着手後のやり直しは自動取消せず要確認 (cancellation_requested_at)');
  IW.stopSession({ taskId: t3.id, workerId: w.id, sessionId: s3.sessionId, reason: 'done' });
  // 再確認 → 新しい行き先 = 新しいタスク (前のは取消のまま)
  const s1b = lineState('AR9|1|1');
  const f1c = finalizeLine({ batchId: b.id, lineKey: 'AR9|1|1', expectVersion: s1b.version, expectQuantityVersion: s1b.quantity_version, result: 'exact', mode: 'current', worker: '山田', decide: decideIroha });
  const d1cId = destIdOf('AR9|1|1');
  ok(f1c.ok && d1cId && d1cId !== d1.id && taskOf(d1cId)?.status === 'not_started' && taskOf(d1.id).status === 'closed', '再確認すると新しいタスク (前のタスクは取消のまま)');
  // 再取込で確認を引き継げない行 (予定数が変わった) → 行き先が取り消され、タスクも取消
  const imp2 = importCsv(makeCsv([row('AR9', 1, 'TASK-A', 7), row('AR9', 2, 'TASK-B', 3), row('AR9', 3, 'TASK-C', 2)]), { fileName: 'prb2.csv', generatedAt: '2027-01-01T01:00:00Z' });
  ok(imp2.ok, '前提: 予定数が変わった再取込');
  const t1c = taskOf(d1cId);
  ok(t1c.status === 'closed' && t1c.close_reason === 'cancelled' && t1c.cancellation_source === 'inbound_import', '再取込で引き継げなかった行のタスクは取消 (inbound_import)');
  // 再取込で行ごと消えた確認済み行 → 行き先を取消 (line_removed)・タスクも取消 (Codex PR-B R1 #1)
  const b2 = getActiveBatch();
  const f3b = finalizeLine({ batchId: b2.id, lineKey: 'AR9|3|1', expectVersion: lineState('AR9|3|1').version, expectQuantityVersion: lineState('AR9|3|1').quantity_version, result: 'exact', mode: 'current', worker: '鈴木', decide: decideIroha });
  ok(f3b.ok && taskOf(destIdOf('AR9|3|1'))?.status === 'not_started', '前提: 消える予定の行を確定 (タスクあり)');
  const d3bId = destIdOf('AR9|3|1');
  const imp3 = importCsv(makeCsv([row('AR9', 1, 'TASK-A', 7), row('AR9', 2, 'TASK-B', 3)]), { fileName: 'prb3.csv', generatedAt: '2027-01-01T02:00:00Z' });
  ok(imp3.ok && /消えた明細の行き先 1件を取消/.test(db.prepare('SELECT message FROM f_inbound_check_import_log ORDER BY id DESC LIMIT 1').get().message), '前提: 行 3 が無い CSV を再取込 (ログに件数)');
  ok(destOf(d3bId).cancelled_at && destOf(d3bId).cancel_reason === 'line_removed', '消えた明細の行き先は取消 (line_removed)');
  const t3c = taskOf(d3bId);
  ok(t3c.status === 'closed' && t3c.close_reason === 'cancelled' && t3c.cancellation_source === 'inbound_import', '消えた明細のタスクも取消');
  ok(destOf(destIdOf('AR9|2|1')).cancelled_at == null, '残った行 (B-Faith 行き) はそのまま');
  // 既に取消済みの行き先が carry に残っていても「取消 N 件」に数えない (Codex PR-B R2 Low)
  const b3 = getActiveBatch();
  const f2b = finalizeLine({ batchId: b3.id, lineKey: 'AR9|1|1', expectVersion: lineState('AR9|1|1').version, expectQuantityVersion: lineState('AR9|1|1').quantity_version, result: 'shortage', mode: 'current', worker: '山田', decide: decideIroha });
  ok(f2b.ok, '前提: 行 1 を確定 (7 予定 / 6 実数 = 不足)');
  const dPre = destIdOf('AR9|1|1');
  db.prepare("UPDATE f_inbound_check_destinations SET cancelled_at = ?, cancelled_by = 'manual', cancel_reason = 'reopen' WHERE id = ?").run(new Date().toISOString(), dPre);
  const imp4 = importCsv(makeCsv([row('AR9', 2, 'TASK-B', 3)]), { fileName: 'prb4.csv', generatedAt: '2027-01-01T03:00:00Z' });
  ok(imp4.ok && !/消えた明細/.test(db.prepare('SELECT message FROM f_inbound_check_import_log ORDER BY id DESC LIMIT 1').get().message), '既に取消済みなら「消えた明細の取消」に数えない');
  ok(destOf(dPre).cancel_reason === 'reopen', '取消理由も上書きしない');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
