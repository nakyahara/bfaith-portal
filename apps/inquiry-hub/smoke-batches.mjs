// 🧺一括操作のバッチ記録・取り消し (batches.js + 組み込み先 + admin UI) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-batches.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-batches-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-batches-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { bulkUpdateInquiries } = await import('./queries.js');
const { listBulkBatches, getBulkBatch, revertBulkBatch } = await import('./batches.js');
const { createFolder } = await import('./folders.js');
const { createLabel } = await import('./labels.js');
const { applyRuleToExistingMails } = await import('./mail-rules.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// テストデータ
db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','テスト店','info@example.com')`).run();
const shopId = db.prepare('SELECT id FROM shops').get().id;
const mkInq = (ext, status, subject = `件名 ${ext}`) => db.prepare(`INSERT INTO inquiries
    (channel_type, shop_id, external_inquiry_id, subject, customer_identifier, internal_status, is_unread, received_at, last_message_at)
  VALUES ('email', ?, ?, ?, 'c@example.com', ?, 1, '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')`)
  .run(shopId, ext, subject, status).lastInsertRowid;
const a = mkInq('b-a', 'open');
const b = mkInq('b-b', 'in_progress');
const c = mkInq('b-c', 'open');
const folder = createFolder('整理待ち', 'tester');
const label = createLabel('自動通知', '#64748b', 'tester');

// ─── 1. 記録 (一括操作 → バッチが残る) ───
console.log('1. 記録');
let batch1;
{
  const r = bulkUpdateInquiries([a, b, c], { status: 'done', folderId: folder.id }, { actorId: 'tester', source: 'bulk' });
  check('一括操作がバッチIDを返す', r.updated === 3 && Number.isInteger(r.batchId), JSON.stringify(r));
  batch1 = getBulkBatch(r.batchId);
  check('バッチ行 (実行者/種別/件数)', batch1.actor === 'tester' && batch1.source === 'bulk'
    && batch1.target_count === 3 && batch1.changed_count === 3 && !batch1.reverted_at);
  const items = db.prepare('SELECT * FROM bulk_batch_items WHERE batch_id = ? ORDER BY inquiry_id').all(batch1.id);
  const itA = items.find(i => i.inquiry_id === a);
  check('itemsに変更前後 (status+completed_at+folder)', items.length === 3
    && JSON.parse(itA.before_json).internal_status === 'open'
    && JSON.parse(itA.before_json).completed_at === null
    && JSON.parse(itA.after_json).internal_status === 'done'
    && JSON.parse(itA.after_json).folder_id === folder.id, JSON.stringify(itA));
  // 変更0件ならバッチを作らない
  const r0 = bulkUpdateInquiries([a], { status: 'done' }, { actorId: 'tester' });
  check('変更0件はバッチなし', r0.updated === 0 && r0.batchId === null);
}

// ─── 2. 取り消し (後からの手動変更は上書きしない) ───
console.log('2. 取り消し');
{
  // b は取り消し前に人が手で「対応中」に戻した (バッチのafter=doneと不一致になる)
  db.prepare("UPDATE inquiries SET internal_status = 'in_progress' WHERE id = ?").run(b);
  const r = revertBulkBatch(batch1.id, 'tester2');
  check('取り消し実行 (2件復元+1件は部分復元)', r.ok && r.reverted >= 2, JSON.stringify(r));
  const rowA = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(a);
  check('a: 状態・completed_at・フォルダが元に戻る', rowA.internal_status === 'open'
    && rowA.completed_at === null && rowA.folder_id === null, JSON.stringify(rowA));
  const rowB = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(b);
  check('b: 手で変えた状態は上書きしない (フォルダだけ戻る)', rowB.internal_status === 'in_progress'
    && rowB.folder_id === null, JSON.stringify(rowB));
  check('バッチはreverted_at付き', !!getBulkBatch(batch1.id).reverted_at);
  const again = revertBulkBatch(batch1.id, 'tester2');
  check('再取り消しは成功扱いで何もしない (冪等)', again.alreadyReverted === true
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(a).internal_status === 'open');
  const logs = db.prepare("SELECT COUNT(*) AS c FROM inquiry_activity_logs WHERE action_type = 'bulk_revert'").get().c;
  check('取り消しの監査ログが残る', logs >= 2);
}

// ─── 3. メールルール一括適用の記録・取り消し ───
console.log('3. ルール一括適用');
{
  const d = mkInq('b-d', 'open', '自動配信のお知らせ');
  const e = mkInq('b-e', 'done', '自動配信のお知らせ');
  const r = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '自動配信' }],
    { matchMode: 'all', apply: true, actorId: 'tester', action: 'import_done', labelId: label.id });
  check('ルール適用もバッチを返す', r.completed === 1 && r.labeled === 2 && Number.isInteger(r.batchId), JSON.stringify(r));
  const bt = getBulkBatch(r.batchId);
  check('種別=rule_apply・条件も保存', bt.source === 'rule_apply'
    && JSON.parse(bt.filter_json).conditions[0].value === '自動配信', bt.filter_json);
  const rv = revertBulkBatch(r.batchId, 'tester');
  const rowD = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(d);
  const rowE = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(e);
  check('取り消しで d=openに戻り両方のラベルが外れる', rv.reverted === 2
    && rowD.internal_status === 'open' && rowD.is_unread === 1 && rowD.label_id === null
    && rowE.internal_status === 'done' && rowE.label_id === null, JSON.stringify({ rowD, rowE, rv }));
  // dry-run はバッチを作らない
  const dry = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '自動配信' }],
    { matchMode: 'all', apply: false, action: 'import_done', labelId: label.id });
  check('dry-runはバッチなし', dry.batchId === undefined || dry.batchId === null, JSON.stringify(dry));
}

// ─── 4. HTTP (bulk API がbatchIdを返す + admin表示 + revert API) ───
console.log('4. HTTP');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jp = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) })
    .then(r => r.json().then(j => ({ status: r.status, j })));

  const f = mkInq('b-f', 'open');
  const rb = await jp('/api/inquiries/bulk', { ids: [f], ops: { status: 'done' } });
  check('bulk APIがbatchIdを返す', rb.status === 200 && Number.isInteger(rb.j.batchId), JSON.stringify(rb.j));
  const rf = await jp('/api/inquiries/bulk-by-filter', { filter: { view: 'done', q: '件名 b-f' }, ops: { isUnread: false } });
  check('bulk-by-filter APIもbatchIdを返す (source=bulk_filter)', rf.status === 200 && Number.isInteger(rf.j.batchId)
    && getBulkBatch(rf.j.batchId).source === 'bulk_filter', JSON.stringify(rf.j));

  const adminHtml = await (await fetch(base + '/admin')).text();
  check('運用管理に一括操作の履歴カード', adminHtml.includes('一括操作の履歴') && adminHtml.includes('このバッチを取り消す'));
  check('取り消し済みバッチは表示が変わる', adminHtml.includes('取り消し済み'));

  const rv = await jp(`/api/bulk-batches/${rb.j.batchId}/revert`, {});
  check('revert API', rv.status === 200 && rv.j.reverted === 1
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(f).internal_status === 'open', JSON.stringify(rv.j));
  check('revert API再送は成功扱い', (await jp(`/api/bulk-batches/${rb.j.batchId}/revert`, {})).j.alreadyReverted === true);
  check('存在しないバッチは400', (await jp('/api/bulk-batches/999999/revert', {})).status === 400);

  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
