/**
 * 有効期限の先入力 (pending_expiry) のテスト — 2026-09-02 中原さん要望
 * 「詳細の期限管理のところで期限を入れられるように。入れてあれば確認時に聞かなくていい」
 *
 * 実行: node scripts/test-inbound-check-pending-expiry.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-pe-test-'));
}

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB, setPendingExpiry, pendingExpiryFor, getState, reopenLine, workDateJst } = await import('../apps/inbound-check/db.js');

const db = getDB();
db.prepare(`INSERT INTO f_inbound_check_batches (id, source, file_name, file_hash, csv_generated_at, row_count, slip_count, imported_at, status)
  VALUES (1, 'manual_upload', 't.csv', 'h1', '2026-09-02T00:00:00Z', 2, 1, '2026-09-02T00:00:00Z', 'active')`).run();
db.prepare(`INSERT INTO f_inbound_check_slips (batch_id, ar_no, line_count, seq) VALUES (1, 'AR001', 2, 1)`).run();
const insLine = db.prepare(`INSERT INTO f_inbound_check_lines
  (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, planned_qty, seq)
  VALUES (1, ?, 'AR001', 1, 1, ?, ?, ?, 10, ?)`);
insLine.run('L1', 'PROD-A', 'prod-a', '商品A', 1);
insLine.run('L2', 'PROD-B', 'prod-b', '商品B', 2);
const insState = db.prepare(`INSERT INTO f_inbound_check_line_state (batch_id, line_key, status) VALUES (1, ?, ?)`);
insState.run('L1', 'unchecked');
insState.run('L2', 'checked');

console.log('[1] 先入力の保存と取得');
{
  const r = setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: '2026-12' });
  ok(r.ok && r.pending_expiry === '2026-12', '年月だけでも保存できる');
  ok(pendingExpiryFor(1, 'L1') === '2026-12', 'pendingExpiryFor で読める (確認時にここを見る)');
  const r2 = setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: '2027-01-15' });
  ok(r2.ok && pendingExpiryFor(1, 'L1') === '2027-01-15', '上書きできる (後勝ち)');
  const r3 = setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: null });
  ok(r3.ok && pendingExpiryFor(1, 'L1') === null, 'null で消せる');
}

console.log('[2] ガード');
{
  const r = setPendingExpiry({ batchId: 1, lineKey: 'L2', expiryDate: '2026-12' });
  ok(r.ok === false && r.error === 'finalized', '確認済みの行には書けない (やり直してから)');
  const r2 = setPendingExpiry({ batchId: 99, lineKey: 'L1', expiryDate: '2026-12' });
  ok(r2.ok === false && r2.error === 'stale_batch', '古い batch_id は stale_batch');
  const r3 = setPendingExpiry({ batchId: 1, lineKey: 'NOPE', expiryDate: '2026-12' });
  ok(r3.ok === false && r3.error === 'not_found', '無い明細は not_found');
}

console.log('[3] getState に pending_expiry が載る');
{
  setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: '2026-12' });
  const s = getState();
  const l1 = s.lines.find(l => l.line_key === 'L1');
  ok(l1 && l1.pending_expiry === '2026-12', '一覧の行に pending_expiry が出る (タグ表示・確認スキップ判定に使う)');
}

console.log('[4] やり直すと確定に使った期限が先入力に戻る (Codex #1116 Med-3)');
{
  const di = db.prepare(`INSERT INTO f_inbound_check_destinations
    (batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at, expiry_date, work_date, code_key, actual_qty)
    VALUES (1, 'L2', 'AR001', 'PROD-B', '商品B', 10, 'iroha', 'master', 'テスト', ?, '2026-11', ?, 'prod-b', 10)`)
    .run(new Date().toISOString(), workDateJst());
  db.prepare('UPDATE f_inbound_check_line_state SET destination_id = ? WHERE batch_id = 1 AND line_key = ?')
    .run(Number(di.lastInsertRowid), 'L2');
  const r = reopenLine({ batchId: 1, lineKey: 'L2', expectVersion: 1, worker: 'テスト' });
  ok(r.ok === true, 'やり直しできる');
  ok(pendingExpiryFor(1, 'L2') === '2026-11', '確定時の期限が pending に戻る (タグ📅入力済として見える)');
}

console.log('[5] 前日の一覧には先入力できない (day_stale ガード — Codex #1116 Med-4)');
{
  db.prepare("UPDATE f_inbound_check_batches SET work_date = '2000-01-01' WHERE id = 1").run();
  const r = setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: '2026-12' });
  ok(r.ok === false && r.error === 'stale_work_date', '前日バッチは stale_work_date');
  db.prepare('UPDATE f_inbound_check_batches SET work_date = ? WHERE id = 1').run(workDateJst());
  const r2 = setPendingExpiry({ batchId: 1, lineKey: 'L1', expiryDate: '2026-12' });
  ok(r2.ok === true, '当日の一覧なら書ける');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail === 0 ? 0 : 1;
