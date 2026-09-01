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
  getDB, importCsv, getActiveBatch, getState, applyCheck, listEvents, eventsCsv, cleanupOld,
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

// ─── 3. 消し込み ───
console.log('\n[3] 消し込み');
{
  const b = getActiveBatch();
  const r1 = applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'check', expectVersion: 1, worker: '山田', staffId: 7, deviceId: 1, deviceLabel: 'iPad1' });
  ok(r1.ok && r1.state.status === 'checked' && r1.state.version === 2 && r1.state.checked_by === '山田', '確認 → checked v2');
  const r2 = applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'check', expectVersion: 1, worker: '佐藤' });
  ok(!r2.ok && r2.error === 'conflict' && r2.current.checked_by === '山田', '同時確認 (古い version) = conflict + 現在状態');
  for (const bad of [undefined, null, 0, -1, 1.5, NaN, '2']) {
    const rb = applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'check', expectVersion: bad, worker: '佐藤' });
    ok(!rb.ok && rb.error === 'bad_request', `expectVersion=${String(bad)} は bad_request`);
  }
  const r3 = applyCheck({ batchId: b.id, lineKey: 'AR1|2|1', action: 'check', worker: '佐藤', expectVersion: 99 });
  ok(!r3.ok && r3.error === 'conflict', 'version 不一致 = conflict');
  const r4 = applyCheck({ batchId: b.id, lineKey: 'AR1|2|1', action: 'uncheck', worker: '佐藤', expectVersion: 1 });
  ok(!r4.ok && r4.error === 'conflict', '未確認行の取消 = conflict');
  const r5 = applyCheck({ batchId: b.id, lineKey: 'NOPE', action: 'check', worker: 'x', expectVersion: 1 });
  ok(!r5.ok && r5.error === 'not_found', '存在しない明細 = not_found');
  const r6 = applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'check', worker: '', expectVersion: 2 });
  ok(!r6.ok && r6.error === 'bad_request', '作業者なし = bad_request');
  const st = getState();
  ok(st.totals.checked === 1 && st.slips.find(s => s.ar_no === 'AR1').checked_count === 1, 'state に確認数が反映');
  const un = applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'uncheck', worker: '山田', expectVersion: 2 });
  ok(un.ok && un.state.status === 'unchecked' && un.state.version === 3 && un.state.checked_by === null, '取消 → unchecked v3');
  const ev = listEvents(b.id);
  ok(ev.length === 2 && ev[1].action === 'uncheck' && ev[1].reverted_event_id === ev[0].id, '取消イベントが確認イベントを指す');
  ok(ev[0].staff_id === 7 && ev[1].staff_id === null, 'events.staff_id (確認=7 / 取消は未指定=null)');
  ok(applyCheck({ batchId: b.id, lineKey: 'AR1|1|1', action: 'check', worker: '山田', expectVersion: 3 }).ok, '再確認 (version 3) OK');
  ok(applyCheck({ batchId: b.id, lineKey: 'AR2|1|1', action: 'check', worker: '山田', expectVersion: 1 }).ok, 'AR2 確認');
  ok(getState().totals.checked === 2, '確認数 2');
  const csv = eventsCsv(b.id);
  ok(csv.charCodeAt(0) === 0xFEFF && csv.split('\r\n').length >= 5 && csv.includes('打ち消した確認ID'), '履歴CSV (BOM + 4イベント)');
  applyCheck({ batchId: b.id, lineKey: 'AR1|2|1', action: 'check', worker: '=HYPERLINK("x")', expectVersion: 1 });
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
  const stale = applyCheck({ batchId: old.id, lineKey: 'AR1|1|1', action: 'check', worker: '山田', expectVersion: 1 });
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

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
