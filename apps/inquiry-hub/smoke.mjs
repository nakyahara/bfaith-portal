// inquiry-hub 機能スモーク: スキーマ作成 → fixture投入 → queries/制約/操作ログを検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initInquiryHubDB, getDB, logActivity, toUtcIso } from './db.js';
import { listInquiries, listFilterOptions, getInquiryDetail, likeEsc, PAGE_SIZE } from './queries.js';

const T = s => toUtcIso(s); // fixture はJSTで書き、保存は正準形式 (UTC 'YYYY-MM-DDTHH:MM:SSZ')

// 本番 DB 誤実行ガード 1: DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-smoke-data)');
  process.exit(2);
}

// 本番 DB 誤実行ガード 2: marker file 方式。smoke 生成マーカーのない既存 DB は削除しない
const dbPath = path.join(process.env.DATA_DIR, 'inquiry-hub.db');
const markerPath = path.join(process.env.DATA_DIR, '.inquiry-hub-smoke-db');
if (fs.existsSync(dbPath) && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の inquiry-hub.db があります (smoke 生成マーカーなし)。実 DB の可能性があるため中断します');
  process.exit(2);
}
for (const suffix of ['', '-wal', '-shm']) { try { fs.rmSync(dbPath + suffix); } catch {} }

initInquiryHubDB();
const db = getDB();
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
fs.writeFileSync(markerPath, `created by apps/inquiry-hub/smoke.mjs at ${new Date().toISOString()}\n`);

let passed = 0, failed = 0;
function check(name, cond, detail) {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
}
function throws(name, fn, msgPart) {
  try { fn(); check(name, false, '例外が発生しませんでした'); }
  catch (e) { check(name, !msgPart || String(e.message).includes(msgPart), `期待(${msgPart}) 実際(${e.message})`); }
}

// ─── 1. スキーマ ───
console.log('1. スキーマ作成');
const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(r => r.name);
for (const t of ['shops', 'inquiries', 'inquiry_messages', 'inquiry_attachments', 'outbox_replies',
  'sync_state', 'sync_errors', 'inquiry_activity_logs', 'internal_notes', 'reply_templates',
  'ai_jobs', 'ai_drafts', 'ai_runs']) {
  check(`table ${t}`, tables.includes(t));
}
check('PRAGMA foreign_keys=ON', db.pragma('foreign_keys', { simple: true }) === 1);
check('PRAGMA journal_mode=WAL', db.pragma('journal_mode', { simple: true }) === 'wal');

// ─── 2. fixture ───
console.log('2. fixture 投入');
const shopEmail = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール窓口','support@test.jp')").run().lastInsertRowid;
const shopRakuten = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','rk-shop')").run().lastInsertRowid;

const insInq = db.prepare(`INSERT INTO inquiries
  (channel_type, shop_id, external_inquiry_id, customer_name, customer_identifier, subject,
   internal_status, assigned_user_id, order_number, product_code, product_name, is_unread, needs_attention, ai_needed, received_at, last_message_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
const inq1 = insInq.run('email', shopEmail, 'th-001', '検索太郎', 'taro@example.com', '配送について',
  'open', null, 'ORD-100', 'sku-alpha', 'アルファ商品', 1, 0, 0, T('2026-07-10T09:00:00+09:00'), T('2026-07-10T09:00:00+09:00')).lastInsertRowid;
const inq2 = insInq.run('rakuten', shopRakuten, 'rk-001', '楽天花子', 'mask-01', '100%オーガニックですか_特殊記号',
  'in_progress', 'user-a', 'ORD-200', 'sku-beta', 'ベータ商品', 0, 1, 1, T('2026-07-12T10:00:00+09:00'), T('2026-07-13T08:00:00+09:00')).lastInsertRowid;
const inq3 = insInq.run('email', shopEmail, 'th-002', '完了次郎', 'jiro@example.com', '解決済みの件',
  'done', 'user-b', null, null, null, 0, 0, 0, T('2026-07-01T09:00:00+09:00'), T('2026-07-02T09:00:00+09:00')).lastInsertRowid;

const insMsg = db.prepare(`INSERT INTO inquiry_messages
  (inquiry_id, external_message_id, sender_type, sender_name, message_body_text, is_incoming, received_at)
  VALUES (?,?,?,?,?,?,?)`);
insMsg.run(inq1, 'm-001', 'customer', '検索太郎', '荷物はいつ届きますか。ユニーク検索語カモノハシ を含む本文。', 1, T('2026-07-10T09:00:00+09:00'));
insMsg.run(inq2, 'm-101', 'customer', '楽天花子', '成分について教えてください。', 1, T('2026-07-12T10:00:00+09:00'));
insMsg.run(inq2, 'm-102', 'shop', '店舗', '天然由来成分100%です。', 0, T('2026-07-13T08:00:00+09:00'));
insMsg.run(inq3, 'm-201', 'customer', '完了次郎', '解決しました。ありがとうございました。', 1, T('2026-07-01T09:00:00+09:00'));
check('fixture 投入', db.prepare('SELECT COUNT(*) AS c FROM inquiries').get().c === 3);

// ─── 3. 制約 (設計書§6/§8.2) ───
console.log('3. 制約検証');
throws('チケット重複 UNIQUE(channel,shop,external_id)', () =>
  insInq.run('email', shopEmail, 'th-001', 'x', 'x', 'x', 'open', null, null, null, null, 1, 0, 0, T('2026-07-10T09:00:00+09:00'), null), 'UNIQUE');
throws('メッセージ重複 UNIQUE(inquiry,external_message_id)', () =>
  insMsg.run(inq1, 'm-001', 'customer', 'x', 'x', 1, T('2026-07-10T09:01:00+09:00')), 'UNIQUE');
throws('FK: 存在しない inquiry_id へのメッセージ', () =>
  insMsg.run(99999, 'm-999', 'customer', 'x', 'x', 1, T('2026-07-10T09:00:00+09:00')), 'FOREIGN KEY');
throws('CHECK: 不正な internal_status', () =>
  db.prepare("UPDATE inquiries SET internal_status='bogus' WHERE id=?").run(inq1), 'CHECK');
throws('CHECK: 不正な channel_type (shops)', () =>
  db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('amazon','x','y')").run(), 'CHECK');
throws('FK: channel_type と shop の不一致 (email shop に楽天チケット)', () =>
  insInq.run('rakuten', shopEmail, 'rk-bad', 'x', 'x', 'x', 'open', null, null, null, null, 1, 0, 0, T('2026-07-10T09:00:00+09:00'), null), 'FOREIGN KEY');
throws('CHECK: ai_needed 値域外', () =>
  db.prepare('UPDATE inquiries SET ai_needed = 9 WHERE id = ?').run(inq1), 'CHECK');
{
  const insAtt = db.prepare(`INSERT INTO inquiry_attachments (inquiry_message_id, external_attachment_id, file_name) VALUES (?,?,?)`);
  const msg1 = db.prepare('SELECT id FROM inquiry_messages WHERE inquiry_id = ? LIMIT 1').get(inq1).id;
  insAtt.run(msg1, 'att-1', 'a.jpg');
  throws('添付重複 UNIQUE(message,external_attachment_id)', () => insAtt.run(msg1, 'att-1', 'a.jpg'), 'UNIQUE');
  throws('添付 external_attachment_id NOT NULL', () => insAtt.run(msg1, null, 'b.jpg'), 'NOT NULL');
}
check('toUtcIso 正準形式', toUtcIso('2026-07-10T09:00:00+09:00') === '2026-07-10T00:00:00Z');
check('toUtcIso number入力', toUtcIso(Date.parse('2026-07-10T00:00:00Z')) === '2026-07-10T00:00:00Z');
check('DB DEFAULT が正準形式', /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(
  db.prepare('SELECT created_at FROM inquiries WHERE id = ?').get(inq1).created_at));
throws('outbox: client_operation_id UNIQUE', () => {
  const ins = db.prepare(`INSERT INTO outbox_replies (inquiry_id, channel_type, client_operation_id, body_text, created_by, base_conversation_rev)
    VALUES (?,?,?,?,?,?)`);
  ins.run(inq1, 'email', 'op-1', 'test', 'smoke', 0);
  ins.run(inq1, 'email', 'op-1', 'test', 'smoke', 0);
}, 'UNIQUE');

// ─── 4. 一覧クエリ ───
console.log('4. 一覧クエリ');
check('全件 (archived除外)', listInquiries({}).total === 3);
check('並び順 = 最終更新降順', listInquiries({}).rows[0].id === inq2);
check('status フィルタ', listInquiries({ status: 'open' }).total === 1);
check('不正 status は無視', listInquiries({ status: 'bogus' }).total === 3);
check('channel フィルタ', listInquiries({ channel: 'email' }).total === 2);
check('shop フィルタ', listInquiries({ shop: String(shopRakuten) }).total === 1);
check('担当=未割当', listInquiries({ assigned: 'none' }).total === 1);
check('担当=user-a', listInquiries({ assigned: 'user-a' }).total === 1);
check('未読のみ', listInquiries({ unread: '1' }).total === 1);
check('要確認のみ', listInquiries({ attention: '1' }).total === 1);
check('AIフラグあり', listInquiries({ ai: '1' }).total === 1);
check('期間 from', listInquiries({ from: '2026-07-11' }).total === 1);
check('期間 to (当日終日含む)', listInquiries({ to: '2026-07-10' }).total === 2);
check('検索: 顧客名', listInquiries({ q: '検索太郎' }).total === 1);
check('検索: 注文番号', listInquiries({ q: 'ORD-200' }).total === 1);
check('検索: 本文 (メッセージ横断)', listInquiries({ q: 'カモノハシ' }).total === 1);
check('検索: LIKE特殊文字がリテラル扱い (% は0件)', listInquiries({ q: '99%' }).total === 0);
check('検索: _ を含む語の完全リテラル一致', listInquiries({ q: '100%オーガニックですか_特殊記号' }).total === 1);
check('検索: ヒットなし', listInquiries({ q: '存在しない語ゼブラ' }).total === 0);
check('likeEsc', likeEsc('a%b_c\\d') === 'a\\%b\\_c\\\\d');
check('msg_count 付与', listInquiries({ q: 'ORD-200' }).rows[0].msg_count === 2);
check('ページング: page=2 は空', listInquiries({ page: '2' }).rows.length === 0 && PAGE_SIZE === 50);

// 論理削除は一覧から消える
db.prepare('UPDATE inquiries SET is_archived = 1 WHERE id = ?').run(inq3);
check('is_archived=1 は一覧から除外', listInquiries({}).total === 2);
db.prepare('UPDATE inquiries SET is_archived = 0 WHERE id = ?').run(inq3);

// ─── 5. フィルタ用マスタ・詳細 ───
console.log('5. フィルタ用マスタ・詳細');
const fo = listFilterOptions();
check('shops 2件', fo.shops.length === 2);
check('assignees', fo.assignees.length === 2 && fo.assignees.includes('user-a'));
check('countMap', fo.countMap.open === 1 && fo.countMap.done === 1);

const detail = getInquiryDetail(inq2);
check('詳細: inquiry', detail && detail.inquiry.subject.includes('オーガニック'));
check('詳細: messages 昇順', detail.messages.length === 2 && detail.messages[0].external_message_id === 'm-101');
check('詳細: 存在しないid は null', getInquiryDetail(99999) === null);
check('詳細: 非整数は null', getInquiryDetail(NaN) === null);

// ─── 6. 操作ログ・メモ ───
console.log('6. 操作ログ・メモ');
logActivity(inq1, { userId: 'smoke-user', actionType: 'status_change', before: { status: 'open' }, after: { status: 'in_progress' } });
const log = db.prepare('SELECT * FROM inquiry_activity_logs WHERE inquiry_id = ? ORDER BY id DESC LIMIT 1').get(inq1);
check('logActivity 構造化記録', log && log.action_type === 'status_change'
  && JSON.parse(log.before_json).status === 'open' && JSON.parse(log.after_json).status === 'in_progress');
db.prepare('INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?,?,?)').run(inq1, 'smoke-user', 'テストメモ');
check('メモが詳細に載る', getInquiryDetail(inq1).notes.length === 1);

// ─── 結果 ───
console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
