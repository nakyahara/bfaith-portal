// inquiry-hub 機能スモーク: スキーマ作成 → fixture投入 → queries/制約/操作ログを検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initInquiryHubDB, getDB, logActivity, toUtcIso } from './db.js';
import { listInquiries, listFilterOptions, getInquiryDetail, likeEsc, PAGE_SIZE, STATUSES, INBOX_STATUSES } from './queries.js';
import { parseCsv, importTemplatesCsv, importQaCsv, listTemplates, listQa } from './templates.js';
import { purgeDemo } from './scripts/purge-demo.mjs';
import { setResolverForTest } from './mx-check.js';

// 返信APIの宛先ドメイン事前確認 (mx-check.js) で実DNSを引かせない (常に「受け取れる」扱い)
setResolverForTest({
  resolveMx: async (d) => [{ exchange: `mail.${d}`, priority: 10 }],
  resolve4: async () => [],
  resolve6: async () => [],
});

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
console.log('4. 一覧クエリ (ビュー横断=all で従来の絞り込みを検証)');
check('全件 (archived除外)', listInquiries({ view: 'all' }).total === 3);
check('並び順 = 最終更新降順', listInquiries({ view: 'all' }).rows[0].id === inq2);
check('status フィルタ', listInquiries({ view: 'all', status: 'open' }).total === 1);
check('不正 status は無視', listInquiries({ view: 'all', status: 'bogus' }).total === 3);
check('channel フィルタ', listInquiries({ view: 'all', channel: 'email' }).total === 2);
check('shop フィルタ', listInquiries({ view: 'all', shop: String(shopRakuten) }).total === 1);
check('担当=未割当', listInquiries({ view: 'all', assigned: 'none' }).total === 1);
check('担当=user-a', listInquiries({ view: 'all', assigned: 'user-a' }).total === 1);
check('未読のみ', listInquiries({ view: 'all', unread: '1' }).total === 1);
check('要確認のみ', listInquiries({ view: 'all', attention: '1' }).total === 1);
check('AIフラグあり', listInquiries({ view: 'all', ai: '1' }).total === 1);
check('期間 from', listInquiries({ view: 'all', from: '2026-07-11' }).total === 1);
check('期間 to (当日終日含む)', listInquiries({ view: 'all', to: '2026-07-10' }).total === 2);
check('検索: 顧客名', listInquiries({ view: 'all', q: '検索太郎' }).total === 1);
check('検索: 注文番号', listInquiries({ view: 'all', q: 'ORD-200' }).total === 1);
check('検索: 本文 (メッセージ横断)', listInquiries({ view: 'all', q: 'カモノハシ' }).total === 1);
check('検索: LIKE特殊文字がリテラル扱い (% は0件)', listInquiries({ view: 'all', q: '99%' }).total === 0);
check('検索: _ を含む語の完全リテラル一致', listInquiries({ view: 'all', q: '100%オーガニックですか_特殊記号' }).total === 1);
check('検索: ヒットなし', listInquiries({ view: 'all', q: '存在しない語ゼブラ' }).total === 0);
// 上部タブ = チャネルグループ (mail=email / mall=rakuten+yahoo)
{
  const { CHANNEL_GROUPS, countInboxByGroup } = await import('./queries.js');
  check('グループ定義: mail=email / mall=rakuten+yahoo',
    JSON.stringify(CHANNEL_GROUPS.mail.channels) === JSON.stringify(['email'])
    && JSON.stringify(CHANNEL_GROUPS.mall.channels) === JSON.stringify(['rakuten', 'yahoo']));
  check('group=mail は email のみ', listInquiries({ view: 'all', group: 'mail' }).total === 2);
  check('group=mall は 楽天+Yahoo!', listInquiries({ view: 'all', group: 'mall' }).total === 1);
  check('不正な group は無視', listInquiries({ view: 'all', group: 'bogus' }).total === 3);
  check('group と channel はAND (mall×email=0件)', listInquiries({ view: 'all', group: 'mall', channel: 'email' }).total === 0);
  // 新着件数: inq1=email open / inq2=rakuten in_progress / inq3=email done (doneは新着に入らない)
  const c = countInboxByGroup();
  check('countInboxByGroup: mail=1 mall=1 all=2', c.mail === 1 && c.mall === 1 && c.all === 2);

  // 状態タブ用: いま見ている文脈での ビュー別件数 (2026-08-17)
  const { countViewsInContext } = await import('./queries.js');
  const all = countViewsInContext({});
  check('countViewsInContext (絞り込みなし): inbox=2 done=1 all=3',
    all.inbox === 2 && all.done === 1 && all.all === 3, JSON.stringify(all));
  const mall = countViewsInContext({ group: 'mall' });
  check('countViewsInContext (mall): 楽天1件のみ', mall.all === 1 && mall.inbox === 1 && mall.done === 0);
  const mail = countViewsInContext({ group: 'mail' });
  check('countViewsInContext (mail): 新着1+完了1', mail.inbox === 1 && mail.done === 1 && mail.all === 2);
}

// 一括操作 (2026-08-17 スタッフ要望: 一覧のチェックボックスからまとめて変更)
{
  const { bulkUpdateInquiries, BULK_MAX } = await import('./queries.js');
  const shopB = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','一括店','bulk@b-faith.biz')").run().lastInsertRowid;
  const fid = Number(db.prepare("INSERT INTO inquiry_folders (name) VALUES ('一括テスト')").run().lastInsertRowid);
  const mk = (ext, status = 'open') => db.prepare(`INSERT INTO inquiries
      (channel_type, shop_id, external_inquiry_id, subject, internal_status, is_unread, received_at, conversation_rev)
    VALUES ('email', ?, ?, '一括', ?, 1, ?, 1)`).run(shopB, ext, status, T('2026-08-17T10:00:00+09:00')).lastInsertRowid;
  const b1 = mk('bk1'), b2 = mk('bk2'), b3 = mk('bk3', 'done');

  const r1 = bulkUpdateInquiries([b1, b2], { status: 'done' }, { actorId: 'tester' });
  check('一括: 状態変更 2件', r1.updated === 2 && r1.skipped === 0
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(b1).internal_status === 'done');
  check('一括: done で completed_at が入る', !!db.prepare('SELECT completed_at FROM inquiries WHERE id = ?').get(b1).completed_at);
  check('一括: 変更不要な件は skipped (冪等)', bulkUpdateInquiries([b1, b3], { status: 'done' }).updated === 0);

  const r2 = bulkUpdateInquiries([b1, b2, b3], { folderId: fid, assigned: 'staff@b-faith.biz', isUnread: false }, { actorId: 'tester' });
  check('一括: フォルダ+担当+既読をまとめて', r2.updated === 3
    && db.prepare('SELECT folder_id, assigned_user_id, is_unread FROM inquiries WHERE id = ?').get(b2).folder_id === fid);
  check('一括: 担当が入る', db.prepare('SELECT assigned_user_id FROM inquiries WHERE id = ?').get(b2).assigned_user_id === 'staff@b-faith.biz');
  check('一括: 既読になる', db.prepare('SELECT is_unread FROM inquiries WHERE id = ?').get(b2).is_unread === 0);
  check('一括: フォルダをnullで未分類へ戻せる',
    bulkUpdateInquiries([b1], { folderId: null }).updated === 1
    && db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(b1).folder_id === null);
  check('一括: 操作ログが1件ずつ残る', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id = ? AND user_id = 'tester'").get(b1).c >= 2);

  // 拒否系
  const bad = (fn) => { try { fn(); return null; } catch (e) { return e.message; } };
  check('一括: 対象なしは拒否', bad(() => bulkUpdateInquiries([], { status: 'done' }))?.includes('選択されていません'));
  check('一括: 変更内容なしは拒否', bad(() => bulkUpdateInquiries([b1], {}))?.includes('変更内容'));
  check('一括: 不正ステータスは拒否', bad(() => bulkUpdateInquiries([b1], { status: 'bogus' }))?.includes('不正なステータス'));
  check('一括: 存在しないフォルダは拒否', bad(() => bulkUpdateInquiries([b1], { folderId: 99999 }))?.includes('フォルダが存在しません'));
  check(`一括: 上限${BULK_MAX}件を超えたら拒否`,
    bad(() => bulkUpdateInquiries(Array.from({ length: BULK_MAX + 1 }, (_, i) => i + 1), { status: 'done' }))?.includes('までです'));
  check('一括: アーカイブ済みは対象外', (() => {
    db.prepare('UPDATE inquiries SET is_archived = 1 WHERE id = ?').run(b3);
    const r = bulkUpdateInquiries([b3], { status: 'open' });
    db.prepare('UPDATE inquiries SET is_archived = 0 WHERE id = ?').run(b3);
    return r.updated === 0 && r.skipped === 1;
  })());

  // 「この条件の全件を選択」(2026-08-20 中原さん要望: ページ50件では足りない → フィルタ全件)
  const { listInquiryIdsByFilter } = await import('./queries.js');
  {
    db.prepare('UPDATE inquiries SET internal_status = ?, folder_id = NULL WHERE id IN (?,?)').run('open', b1, b2);
    db.prepare('UPDATE inquiries SET internal_status = ? WHERE id = ?').run('done', b3);
    const ids = listInquiryIdsByFilter({ view: 'inbox', shop: String(shopB) });
    check('全件選択: 一覧と同じ条件で全IDを返す (新着=2件)', ids.length === 2 && ids.includes(b1) && ids.includes(b2));
    check('全件選択: 完了ビューは1件', listInquiryIdsByFilter({ view: 'done', shop: String(shopB) }).length === 1);
    check('全件選択: 検索語も効く', listInquiryIdsByFilter({ view: 'inbox', shop: String(shopB), q: '一括' }).length === 2
      && listInquiryIdsByFilter({ view: 'inbox', shop: String(shopB), q: '存在しない語XYZ' }).length === 0);
    check('全件選択: 上限超過は拒否', bad(() => listInquiryIdsByFilter({ view: 'inbox' }, { max: 1 }))?.includes('多すぎます'));
    const rAll = bulkUpdateInquiries(ids, { status: 'done' }, { actorId: 'tester', maxItems: 3000 });
    check('全件選択→一括完了 (maxItems拡張)', rAll.updated === 2
      && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(b1).internal_status === 'done');
  }

  // 後片付け (バッチ記録は inquiries をFK参照するので先に消す。本番は物理削除しないためテストのみの都合)
  db.prepare('DELETE FROM bulk_batch_items WHERE inquiry_id IN (?,?,?)').run(b1, b2, b3);
  db.prepare('DELETE FROM bulk_batches WHERE id NOT IN (SELECT DISTINCT batch_id FROM bulk_batch_items)').run();
  db.prepare('DELETE FROM inquiry_activity_logs WHERE inquiry_id IN (?,?,?)').run(b1, b2, b3);
  db.prepare('DELETE FROM inquiries WHERE shop_id = ?').run(shopB);
  db.prepare('DELETE FROM shops WHERE id = ?').run(shopB);
  db.prepare('DELETE FROM inquiry_folders WHERE id = ?').run(fid);
}
check('likeEsc', likeEsc('a%b_c\\d') === 'a\\%b\\_c\\\\d');
check('msg_count 付与', listInquiries({ view: 'all', q: 'ORD-200' }).rows[0].msg_count === 2);
check('ページング: page=2 は空', listInquiries({ view: 'all', page: '2' }).rows.length === 0 && PAGE_SIZE === 50);

// 論理削除は一覧から消える
db.prepare('UPDATE inquiries SET is_archived = 1 WHERE id = ?').run(inq3);
check('is_archived=1 は一覧から除外', listInquiries({ view: 'all' }).total === 2);
db.prepare('UPDATE inquiries SET is_archived = 0 WHERE id = ?').run(inq3);

// ─── 4b. 受信箱ビュー (2026-07-25: 送信済みはTOPから外し、返事が来たら受信へ戻る) ───
console.log('4b. 受信箱ビュー');
{
  const { countByView, VIEWS, DEFAULT_VIEW } = await import('./queries.js');
  const shopV = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','V店','v@b-faith.biz')").run().lastInsertRowid;
  const mkV = (ext, status = 'open') => db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, internal_status, received_at, last_message_at, conversation_rev)
    VALUES ('email', ?, ?, ?, ?, ?, ?, 1)`).run(shopV, ext, ext, status, T('2026-07-20T10:00:00'), T('2026-07-20T10:00:00')).lastInsertRowid;
  const addMsg = (id, incoming, at) => db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
    VALUES (?,?,?,?,?,?)`).run(id, `${id}-${at}`, incoming ? 'customer' : 'shop', 'x', incoming ? 1 : 0, T(at));

  const vNew = mkV('v-new');        // 顧客からの新着のみ → 受信トレイ
  addMsg(vNew, 1, '2026-07-20T10:00:00');
  const vReplied = mkV('v-replied'); // 顧客→こちらが返信 → 送信済み (ボールは相手)
  addMsg(vReplied, 1, '2026-07-20T10:00:00');
  addMsg(vReplied, 0, '2026-07-20T11:00:00');
  const vDone = mkV('v-done', 'done'); // 完了 → 対応済み
  addMsg(vDone, 1, '2026-07-20T10:00:00');
  const vEmpty = mkV('v-empty');    // メッセージ0件 → 取りこぼさず受信トレイ

  // ステータスはメールディーラー準拠 (新着 / 返信処理中 / 完了)。ビューはステータスで振り分く
  db.prepare("UPDATE inquiries SET internal_status = 'waiting_reply' WHERE id = ?").run(vReplied);
  const ids = v => listInquiries({ view: v, shop: String(shopV) }).rows.map(r => r.id).sort();
  check('新着: open のもの (返信処理中・完了は出ない)', JSON.stringify(ids('inbox')) === JSON.stringify([vNew, vEmpty].sort()));
  check('返信処理中: waiting_reply のもの', JSON.stringify(ids('sent')) === JSON.stringify([vReplied]));
  check('完了: done のみ', JSON.stringify(ids('done')) === JSON.stringify([vDone]));
  check('すべて: 全部', ids('all').length === 4);
  check('ラベルがメールディーラー語彙', STATUSES.open.label === '新着' && STATUSES.waiting_reply.label === '返信処理中' && STATUSES.done.label === '完了');
  check('対応中・保留も新着ビューに入る (自分のタスク)', INBOX_STATUSES.includes('in_progress') && INBOX_STATUSES.includes('pending'));

  check('既定ビューは受信トレイ', DEFAULT_VIEW === 'inbox' && listInquiries({ shop: String(shopV) }).view === 'inbox');
  check('不正なビュー指定は既定へフォールバック', listInquiries({ view: 'bogus', shop: String(shopV) }).view === 'inbox');
  const c = countByView();
  check('countByView が各ビューの件数を返す', typeof c.inbox === 'number' && typeof c.sent === 'number' && typeof c.done === 'number');
  // 「N日 返信なし」の基準はメッセージ本体の最終日時 (inquiries.last_message_at は手動の送信確定で
  // 更新されないため、そのままだと送信直後に「◯日返信なし」と誤表示される。Codexレビュー反映)
  {
    const vStale = mkV('v-stale', 'waiting_reply');   // 返信処理中 = 送信済みビューに出る
    addMsg(vStale, 1, '2026-07-01T10:00:00');
    addMsg(vStale, 0, '2026-07-22T10:00:00');   // こちらの返信は新しい
    db.prepare('UPDATE inquiries SET last_message_at = ? WHERE id = ?').run(T('2026-07-01T10:00:00'), vStale); // 古いまま (手動確定を再現)
    const row = listInquiries({ view: 'sent', shop: String(shopV) }).rows.find(r => r.id === vStale);
    check('last_message_at_actual はメッセージ本体の最終日時', row && row.last_message_at_actual === T('2026-07-22T10:00:00'));
  }
  check('VIEWS 定義が4種', Object.keys(VIEWS).length === 4);

  // 後片付け (以降のテストに影響させない)
  db.prepare('DELETE FROM inquiry_messages WHERE inquiry_id IN (SELECT id FROM inquiries WHERE shop_id = ?)').run(shopV);
  db.prepare('DELETE FROM inquiries WHERE shop_id = ?').run(shopV);
  db.prepare('DELETE FROM shops WHERE id = ?').run(shopV);
}

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

// 詳細画面の前後ナビ (一覧と同じ並び順で隣を返す。2026-08-10 スタッフ要望)
{
  const { getAdjacentInquiries } = await import('./queries.js');
  // all ビューの並び: inq2 (7/13) → inq1 (7/10) → inq3 (7/2)
  const mid = getAdjacentInquiries(inq1, { view: 'all' });
  check('前後ナビ: 中間行は両隣を返す', mid.prev?.id === inq2 && mid.next?.id === inq3);
  const first = getAdjacentInquiries(inq2, { view: 'all' });
  check('前後ナビ: 先頭行は prev なし', first.prev === null && first.next?.id === inq1);
  const last = getAdjacentInquiries(inq3, { view: 'all' });
  check('前後ナビ: 末尾行は next なし', last.prev?.id === inq1 && last.next === null);
  const inbox = getAdjacentInquiries(inq1, { view: 'inbox' });
  check('前後ナビ: ビュー絞り込みを反映 (完了は隣に出ない)', inbox.prev?.id === inq2 && inbox.next === null);
  // 上部タブ (group) の文脈: メールタブ中は email だけで隣を探す (楽天 inq2 は飛ばす)
  const grpMail = getAdjacentInquiries(inq1, { view: 'all', group: 'mail' });
  check('前後ナビ: group=mail はメールだけで隣を探す', grpMail.prev === null && grpMail.next?.id === inq3);
  const grpMall = getAdjacentInquiries(inq2, { view: 'all', group: 'mall' });
  check('前後ナビ: group=mall は1件のみ=両隣なし', grpMall.prev === null && grpMall.next === null);
  check('前後ナビ: 存在しないidは両方null',
    JSON.stringify(getAdjacentInquiries(99999)) === JSON.stringify({ prev: null, next: null })
    && JSON.stringify(getAdjacentInquiries(NaN)) === JSON.stringify({ prev: null, next: null }));

  // 同一時刻は一覧と同じく id 降順でタイブレーク (並び: inq1 → tieB → tieA → inq3)
  const tieT = T('2026-07-05T09:00:00+09:00');
  const mkTie = ext => insInq.run('email', shopEmail, ext, 'タイ', 't@example.com', 'タイ同時刻',
    'open', null, null, null, null, 0, 0, 0, tieT, tieT).lastInsertRowid;
  const tieA = mkTie('th-tie-a');
  const tieB = mkTie('th-tie-b');
  const tb = getAdjacentInquiries(tieB, { view: 'all' });
  const ta = getAdjacentInquiries(tieA, { view: 'all' });
  check('前後ナビ: 同一時刻は id 降順でタイブレーク',
    tb.prev?.id === inq1 && tb.next?.id === tieA && ta.prev?.id === tieB && ta.next?.id === inq3);

  // アーカイブ済みは隣として飛ばされる
  db.prepare('UPDATE inquiries SET is_archived = 1 WHERE id = ?').run(tieA);
  check('前後ナビ: アーカイブ済みは飛ばす', getAdjacentInquiries(tieB, { view: 'all' }).next?.id === inq3);

  // フォルダ文脈: 同じフォルダの中だけで隣を探す
  const navFolder = db.prepare("INSERT INTO inquiry_folders (name) VALUES ('ナビ用')").run().lastInsertRowid;
  db.prepare('UPDATE inquiries SET folder_id = ? WHERE id IN (?, ?)').run(navFolder, inq2, inq3);
  const inFolder = getAdjacentInquiries(inq3, { view: 'all', folder: String(navFolder) });
  check('前後ナビ: フォルダ内だけで隣を探す', inFolder.prev?.id === inq2 && inFolder.next === null);

  // 後片付け (以降のテストに影響させない)
  db.prepare('UPDATE inquiries SET folder_id = NULL WHERE folder_id = ?').run(navFolder);
  db.prepare('DELETE FROM inquiry_folders WHERE id = ?').run(navFolder);
  db.prepare("DELETE FROM inquiries WHERE external_inquiry_id IN ('th-tie-a', 'th-tie-b')").run();
}

// ─── 6. 操作ログ・メモ ───
console.log('6. 操作ログ・メモ');
logActivity(inq1, { userId: 'smoke-user', actionType: 'status_change', before: { status: 'open' }, after: { status: 'in_progress' } });
const log = db.prepare('SELECT * FROM inquiry_activity_logs WHERE inquiry_id = ? ORDER BY id DESC LIMIT 1').get(inq1);
check('logActivity 構造化記録', log && log.action_type === 'status_change'
  && JSON.parse(log.before_json).status === 'open' && JSON.parse(log.after_json).status === 'in_progress');
db.prepare('INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?,?,?)').run(inq1, 'smoke-user', 'テストメモ');
check('メモが詳細に載る', getInquiryDetail(inq1).notes.length === 1);

// ─── 7. テンプレート/Q&A (メールディーラーCSV取込) ───
console.log('7. テンプレート/Q&A取込');
check('parseCsv: 引用符内改行+""エスケープ',
  JSON.stringify(parseCsv('"a","b\nc","d""e"\n"2","3","4"')) === JSON.stringify([['a', 'b\nc', 'd"e'], ['2', '3', '4']]));
check('parseCsv: BOM除去', parseCsv('﻿"x"')[0][0] === 'x');
// 厳格化 (Codex R1 high: 不正CSVを黙って壊れた行にしない)
throws('parseCsv: 未クローズ引用符はエラー', () => parseCsv('"a","b\nc'), '閉じていません');
throws('parseCsv: セル途中の引用符はエラー', () => parseCsv('a"b,c'), 'セル途中');
throws('parseCsv: 閉じ引用符後の余分な文字はエラー', () => parseCsv('"a"x,"b"'), '余分な文字');
check('parseCsv: CR-only改行', JSON.stringify(parseCsv('"a","b"\r"c","d"')) === JSON.stringify([['a', 'b'], ['c', 'd']]));
check('parseCsv: CRLF改行+途中空行無視', JSON.stringify(parseCsv('"a"\r\n\r\n"b"')) === JSON.stringify([['a'], ['b']]));
check('parseCsv: 空セル明示行 ,, は残る (Codex R2)', JSON.stringify(parseCsv(',,')) === JSON.stringify([['', '', '']]));
check('parseCsv: 空セル明示行 "","" は残る', JSON.stringify(parseCsv('"",""')) === JSON.stringify([['', '']]));

const TPL_HDR = '"テンプレートID","テンプレート名","件名","本文（上）","本文（下）","To","From","Fromの表示名","Cc","Bcc","重要キーワード","キーワード","利用できる画面","備考","表示順序","テンプレートグループ","登録日","最終更新日"';
const tplCsv = [TPL_HDR,
  '"977","キャンセル完了","","本文です\n2行目","署名","","","","","","重要","キーワード","新規メール","備考メモ","1","注文キャンセル処理","2021/07/08 07:13:21","2025/03/12 14:15:53"',
  '"1009","名前だけ本文なし","","","","","","","","","","","","","2","グループB","",""',
].join('\n');
const tr1 = importTemplatesCsv(tplCsv);
check('テンプレ取込: 1新規+1スキップ', tr1.inserted === 1 && tr1.skipped === 1 && tr1.errors.length === 1);
const tr2 = importTemplatesCsv(tplCsv);
check('テンプレ再取込は更新 (冪等)', tr2.inserted === 0 && tr2.updated === 1);
const tpl977 = db.prepare("SELECT * FROM reply_templates WHERE external_id = '977'").get();
check('テンプレ列マッピング', tpl977.template_name === 'キャンセル完了' && tpl977.category === '注文キャンセル処理'
  && tpl977.template_body === '本文です\n2行目' && tpl977.body_bottom === '署名'
  && tpl977.keywords === '重要 / キーワード' && tpl977.sort_order === 1);
check('メールディーラー日時 JST→UTC正準', tpl977.source_updated_at === '2025-03-12T05:15:53Z');
throws('テンプレCSV: ヘッダ不正はエラー', () => importTemplatesCsv('"foo","bar"\n"1","2"'), '必須列');
// 列数不一致・長さ超過は行スキップ+エラー報告 (Codex R1 medium)
{
  const badRow = importTemplatesCsv([TPL_HDR, '"9001","列足りず","本文"'].join('\n'));
  check('テンプレ取込: 列数不一致はスキップ', badRow.skipped === 1 && /列数不一致/.test(badRow.errors[0]));
  const longName = 'あ'.repeat(201);
  const longRow = importTemplatesCsv([TPL_HDR,
    `"9002","${longName}","","本文","","","","","","","","","","","1","G","",""`].join('\n'));
  check('テンプレ取込: 長さ超過はスキップ', longRow.skipped === 1 && /長すぎます/.test(longRow.errors[0]));
}

// 手動追加 (external_id NULL) は取込で消えない + NULL複数許容
db.prepare("INSERT INTO reply_templates (template_name, template_body) VALUES ('手動A', 'x')").run();
db.prepare("INSERT INTO reply_templates (template_name, template_body) VALUES ('手動B', 'y')").run();
importTemplatesCsv(tplCsv);
check('手動追加分は再取込の影響なし (partial unique index)',
  db.prepare('SELECT COUNT(*) c FROM reply_templates WHERE external_id IS NULL').get().c === 2
  && db.prepare('SELECT COUNT(*) c FROM reply_templates').get().c === 3);

const lt = listTemplates({ q: 'キャンセル' });
check('listTemplates 検索', lt.rows.length === 1 && lt.categories.length === 1);

// 論理削除は再取込で復活する (is_active=1 に戻す仕様)
db.prepare("UPDATE reply_templates SET is_active = 0 WHERE external_id = '977'").run();
importTemplatesCsv(tplCsv);
check('論理削除は再取込で復活', db.prepare("SELECT is_active FROM reply_templates WHERE external_id = '977'").get().is_active === 1);

const QA_HDR = '"Q&A ID","カテゴリID","カテゴリ名","件名","質問内容","回答","備考","担当者","登録日","最終更新日","公開状況",';
const qaCsv = [QA_HDR,
  '"2","1","商品について","ひまし油の抽出方法","低温圧搾ですか?","高温圧搾です。","","管理者","2023/05/08 13:24:21","2023/05/08 13:24:37","公開",""',
  '"3","1","商品について","回答なし","質問だけ","","","","","","非公開",""',
].join('\n');
const qr1 = importQaCsv(qaCsv);
check('Q&A取込: 1新規+1スキップ (回答空)', qr1.inserted === 1 && qr1.skipped === 1);
const qr2 = importQaCsv(qaCsv);
check('Q&A再取込は更新 (冪等)', qr2.updated === 1);
const qa2 = db.prepare("SELECT * FROM qa_entries WHERE external_id = '2'").get();
check('Q&A列マッピング', qa2.title === 'ひまし油の抽出方法' && qa2.answer === '高温圧搾です。'
  && qa2.category === '商品について' && qa2.is_published === 1);
check('listQa 検索', listQa({ q: 'ひまし油' }).rows.length === 1);
throws('Q&A CSV: ヘッダ不正はエラー', () => importQaCsv('"x"\n"1"'), '必須列');

// ─── 8. デモデータ削除 (purge-demo) ───
console.log('8. デモデータ削除 (purge-demo)');
{
  const before = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
  // demoフィクスチャ: seed-demo が作るものと同じ構造 (店舗 + demo:チケット + 全子テーブル)
  const demoShop = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','デモ楽天','demo-rakuten-shop')").run().lastInsertRowid;
  const demoInq = insInq.run('rakuten', demoShop, 'demo:rk-1', 'デモ客', 'mask-demo', 'デモ問い合わせ',
    'open', null, null, null, null, 1, 0, 1, T('2026-07-14T09:00:00+09:00'), T('2026-07-14T09:00:00+09:00')).lastInsertRowid;
  const demoMsg = insMsg.run(demoInq, 'demo:m-1', 'customer', 'デモ客', 'デモ本文', 1, T('2026-07-14T09:00:00+09:00')).lastInsertRowid;
  db.prepare("INSERT INTO inquiry_attachments (inquiry_message_id, external_attachment_id, file_name) VALUES (?,?,?)").run(demoMsg, 'demo:att-1', 'd.jpg');
  db.prepare(`INSERT INTO outbox_replies (inquiry_id, channel_type, client_operation_id, body_text, created_by, base_conversation_rev)
    VALUES (?,?,?,?,?,?)`).run(demoInq, 'rakuten', 'op-demo-1', 'x', 'smoke', 0);
  const demoJob = db.prepare('INSERT INTO ai_jobs (inquiry_id, input_rev) VALUES (?, 0)').run(demoInq).lastInsertRowid;
  db.prepare('INSERT INTO ai_drafts (inquiry_id, ai_job_id, input_rev, draft_body) VALUES (?,?,0,?)').run(demoInq, demoJob, 'AI案');
  db.prepare('INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?,?,?)').run(demoInq, 'smoke', 'demoメモ');
  logActivity(demoInq, { actorType: 'system', actionType: 'seed', after: { source: 'smoke' } });
  db.prepare('INSERT INTO sync_state (shop_id) VALUES (?)').run(demoShop);
  db.prepare("INSERT INTO sync_errors (shop_id, inquiry_id, error_type) VALUES (?,?, 'fetch_failed')").run(demoShop, demoInq);

  // dry-run: 件数レポートのみで何も消えない
  const dry = purgeDemo({ apply: false });
  check('purge dry-run: 対象件数を検出', dry.deleted.inquiries === 1 && dry.deleted.inquiry_messages === 1
    && dry.deleted.inquiry_attachments === 1 && dry.deleted.outbox_replies === 1
    && dry.deleted.ai_jobs === 1 && dry.deleted.ai_drafts === 1 && dry.deleted.shops === 1);
  check('purge dry-run: 何も削除されない', db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before + 1
    && db.prepare('SELECT COUNT(*) c FROM shops WHERE id = ?').get(demoShop).c === 1);

  // apply: demoだけ消え、既存フィクスチャ・テンプレ/Q&Aは無傷
  const tplBefore = db.prepare('SELECT COUNT(*) c FROM reply_templates').get().c;
  const applied = purgeDemo({ apply: true });
  check('purge apply: demo全削除', applied.remainingDemo === 0
    && db.prepare('SELECT COUNT(*) c FROM shops WHERE id = ?').get(demoShop).c === 0
    && db.prepare('SELECT COUNT(*) c FROM sync_state WHERE shop_id = ?').get(demoShop).c === 0
    && db.prepare('SELECT COUNT(*) c FROM sync_errors WHERE shop_id = ?').get(demoShop).c === 0);
  check('purge apply: 非demoデータは無傷', db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before
    && db.prepare('SELECT COUNT(*) c FROM reply_templates').get().c === tplBefore
    && getInquiryDetail(inq1) !== null);

  // デモ店舗に実データが紐付いた場合は店舗を残す
  const mixedShop = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('yahoo','デモYahoo','demo-yahoo-seller')").run().lastInsertRowid;
  insInq.run('yahoo', mixedShop, 'demo:yh-1', 'デモ', 'y-demo', 'demoの件',
    'open', null, null, null, null, 1, 0, 0, T('2026-07-14T10:00:00+09:00'), null);
  insInq.run('yahoo', mixedShop, 'real-yh-1', '実顧客', 'y-real', '実データの件',
    'open', null, null, null, null, 1, 0, 0, T('2026-07-14T11:00:00+09:00'), null);
  const mixed = purgeDemo({ apply: true });
  check('purge: 実データ紐付き店舗は残す (警告のみ)', mixed.keptShops.length === 1
    && db.prepare('SELECT COUNT(*) c FROM shops WHERE id = ?').get(mixedShop).c === 1
    && db.prepare("SELECT COUNT(*) c FROM inquiries WHERE external_inquiry_id = 'real-yh-1'").get().c === 1
    && db.prepare("SELECT COUNT(*) c FROM inquiries WHERE external_inquiry_id = 'demo:yh-1'").get().c === 0);

  // GLOBの大文字小文字区別: 'DEMO:'で始まる実データは誤爆しない (Codexレビュー指摘)
  insInq.run('yahoo', mixedShop, 'DEMO:not-a-demo', '実顧客2', 'y-real2', '大文字DEMOで始まる実データ',
    'open', null, null, null, null, 1, 0, 0, T('2026-07-14T12:00:00+09:00'), null);
  const caseTest = purgeDemo({ apply: true });
  check('purge: 大文字DEMO:は削除対象外 (GLOB大小区別)', caseTest.deleted.inquiries === 0
    && db.prepare("SELECT COUNT(*) c FROM inquiries WHERE external_inquiry_id = 'DEMO:not-a-demo'").get().c === 1);
}

// ─── HTTP: 「この条件の全件を選択」一括API (2026-08-20 中原さん要望) ───
console.log('HTTP: 全件一括');
{
  const express = (await import('express')).default;
  const routerModule = await import('./router.js');
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;
  const jp = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) })
    .then(r => r.json().then(j => ({ status: r.status, j })));

  const shopH = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','全件一括店','ba@b-faith.biz')").run().lastInsertRowid;
  const mkH = (ext, status) => db.prepare(`INSERT INTO inquiries
      (channel_type, shop_id, external_inquiry_id, subject, internal_status, is_unread, received_at, conversation_rev)
    VALUES ('email', ?, ?, '全件一括対象', ?, 1, ?, 1)`).run(shopH, ext, status, T('2026-08-20T10:00:00+09:00')).lastInsertRowid;
  const h1 = mkH('ba1', 'open'), h2 = mkH('ba2', 'open');
  mkH('ba3', 'done');

  const html = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
  check('一覧に「この条件の全N件を選択」ボタンとFILTERが載る', html.includes('id="bulkAll"')
    && html.includes('この条件の全') && html.includes('bulk-by-filter'));

  // 上部の外部リンクバー (2026-08-25 中原さん要望)。🔗リンク管理で登録した分が自動で並ぶ
  // (登録・検証・削除の詳細は smoke-links.mjs)
  const { createQuickLink, deleteQuickLink } = await import('./links.js');
  const qk = createQuickLink({ name: '一覧バー確認', url: 'https://example.com/quick-bar', icon: '🧪' }, 'tester');
  const html2 = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
  check('登録したリンクが一覧上部に自動で出る',
    html2.includes('class="mall-links"') && html2.includes('https://example.com/quick-bar')
    && html2.includes('一覧バー確認') && html2.includes('target="_blank"'));
  deleteQuickLink(qk.id);
  const html3 = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
  check('削除したリンクは上部から消える', !html3.includes('https://example.com/quick-bar'));

  const dry = await jp('/api/inquiries/bulk-by-filter', { filter: { view: 'inbox', shop: String(shopH) }, ops: { status: 'done' }, dryRun: true });
  check('dryRun: 件数だけ返し状態は変えない', dry.status === 200 && dry.j.matched === 2
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(h1).internal_status === 'open');
  const ap = await jp('/api/inquiries/bulk-by-filter', { filter: { view: 'inbox', shop: String(shopH) }, ops: { status: 'done' } });
  check('適用: フィルタ一致の全件を完了に', ap.status === 200 && ap.j.updated === 2
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(h2).internal_status === 'done');
  const bad2 = await jp('/api/inquiries/bulk-by-filter', { filter: { view: 'inbox', shop: String(shopH) }, ops: {} });
  check('変更内容なしは400', bad2.status === 400);

  // クイック入口 + 本文プレビュー (2026-08-25 Codex議論の採用分)
  const { previewOf } = routerModule;
  check('previewOf: 引用行・URL・改行を落として1行に',
    previewOf('商品が届きません\nhttps://example.com/track\n> 前回のメール\n引用の続き') === '商品が届きません 引用の続き',
    JSON.stringify(previewOf('商品が届きません\nhttps://example.com/track\n> 前回のメール\n引用の続き')));
  check('previewOf: 引用ヘッダ以降を落とす',
    previewOf('返品したいです\n2026年8月20日(木) 18:20 雑貨イズム:\n> 元の本文') === '返品したいです');
  check('previewOf: 長文は…で切る', previewOf('あ'.repeat(200)).endsWith('…'));
  check('previewOf: 空・nullは空文字', previewOf(null) === '' && previewOf('') === '');

  const inqPv = mkH('ba-preview', 'open');
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
    VALUES (?,?,'customer','届いた商品が不足しています。至急ご対応ください',1,?)`).run(inqPv, 'pv-1', T('2026-08-21T10:00:00+09:00'));
  const htmlPv = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
  check('一覧に最新の顧客メッセージのプレビューが出る',
    htmlPv.includes('class="preview"') && htmlPv.includes('届いた商品が不足しています'));
  check('クイック入口5種が出る (新着/要確認/自分の対応中/未割当/滞留)',
    htmlPv.includes('class="quick-bar"') && htmlPv.includes('今日・昨日の新着') && htmlPv.includes('要確認')
    && htmlPv.includes('自分の対応中') && htmlPv.includes('未割当') && htmlPv.includes('14日以前の滞留'));
  const htmlQb = await (await fetch(`${base}/?view=inbox&attention=1`)).text();
  check('押した入口は選択状態 (on) になる', /class="on" href="[^"]*attention=1/.test(htmlQb));

  // 受信日時を一覧の左側 (状態の直後) に出す (2026-08-26 中原さん要望
  // 「一覧に受信したメールの日付と時間を表示してほしい」。右端だと横スクロールしないと見えなかった)
  {
    const inqDt = mkH('ba-datetime', 'open');
    const htmlDt = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
    check('一覧の見出しが「受信日時」', htmlDt.includes('>受信日時</th>'));
    check('受信日時セルに日付+時刻が出る (JST)',
      htmlDt.includes('data-label="受信日時">2026-08-20 10:00'), '2026-08-20 10:00 が見つからない');
    // 列順: 状態 → 受信日時 → ラベル (右端のままだと画面外で見えない、が今回の要望)
    const head = htmlDt.slice(htmlDt.indexOf('<thead>'), htmlDt.indexOf('</thead>'));
    check('列順は 状態 → 受信日時 → ラベル',
      head.indexOf('>状態<') < head.indexOf('>受信日時<') && head.indexOf('>受信日時<') < head.indexOf('>ラベル<'), head);
    check('注文/商品より前に出る (右端ではない)', head.indexOf('>受信日時<') < head.indexOf('注文 / 商品'));

    // 1通だけの行は「更新」を出さない (行が2行に膨らまないように)
    check('初回受信と最終メッセージが同じ行は「更新」を出さない',
      !/data-label="受信日時">2026-08-20 10:00<div class="sub">更新/.test(htmlDt));

    // 追い返信が来た行は「更新 <日時>」を併記する
    db.prepare('UPDATE inquiries SET last_message_at = ? WHERE id = ?').run(T('2026-08-25T09:30:00+09:00'), inqDt);
    const htmlDt2 = await (await fetch(`${base}/?view=inbox&shop=${shopH}`)).text();
    check('最終メッセージが違う行は「更新 日時」を併記', htmlDt2.includes('更新 2026-08-25 09:30'));

    // 返信処理中ビューでは従来どおり「返信待ち」を出す
    db.prepare("UPDATE inquiries SET internal_status = 'waiting_reply' WHERE id = ?").run(inqDt);
    const htmlSent = await (await fetch(`${base}/?view=sent&shop=${shopH}`)).text();
    check('返信処理中ビューは見出しが「受信日時 / 返信待ち」', htmlSent.includes('受信日時 / 返信待ち'));
    check('返信処理中ビューは日時と返信待ちを両方出す',
      htmlSent.includes('data-label="受信日時">2026-08-20 10:00') && /返信なし|返信待ち/.test(htmlSent));
    db.prepare("UPDATE inquiries SET internal_status = 'open', last_message_at = NULL WHERE id = ?").run(inqDt);
  }


  // 1クリック対応完了ボタン (2026-08-24 中原さん要望・メールディーラーの右上「対応完了」踏襲)
  const h4 = mkH('ba4', 'open'), h5 = mkH('ba5', 'done');
  const dOpen = await (await fetch(`${base}/inquiries/${h4}?view=inbox`)).text();
  check('詳細: 未完了は右上に「✅ 対応完了」1クリックボタン', dOpen.includes('id="quickDoneBtn"') && dOpen.includes('✅ 対応完了'));
  const dDone = await (await fetch(`${base}/inquiries/${h5}?view=inbox`)).text();
  check('詳細: 完了済みは押せない表示', !dDone.includes('id="quickDoneBtn"') && dDone.includes('✅ 完了済み'));

  // 返信欄 (2026-08-26 スタッフ要望×2): ①「送信」「送信して回答完了」の2ボタン ②テンプレートはカテゴリ見出し付き1本セレクト
  {
    process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
    const vm = await import('vm');
    const scriptOf = html => {
      // 最後の <script>…</script> = 詳細画面のクライアントJS。構文エラーがあると画面全体が死ぬので必ず検証
      const m = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
      return m.length ? m[m.length - 1][1] : '';
    };
    const dMail = await (await fetch(`${base}/inquiries/${h4}?view=inbox`)).text();
    check('メール: 「✉️ 送信」+「✅ 送信して完了」の2ボタン (チェックボックス廃止)',
      dMail.includes('id="replyBtn"') && dMail.includes('id="replyCompleteBtn"') && dMail.includes('✅ 送信して完了') && !dMail.includes('id="replyComplete"'));
    check('メール: モール側完了の案内は出ない', !dMail.includes('✅ 送信して回答完了') && !dMail.includes('ストアクリエイターPro') && dMail.includes('var MALL_COMPLETE = false'));
    check('テンプレートは1本のセレクト (カテゴリselect廃止・optgroupで見出し)',
      dMail.includes('id="tplSel"') && !dMail.includes('id="tplCat"') && dMail.includes("createElement('optgroup')"));
    let jsErr = null;
    try { new vm.Script(scriptOf(dMail)); } catch (e) { jsErr = e; }
    check('詳細画面のクライアントJSが構文OK (メール)', jsErr === null, String(jsErr));

    const shopYh = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('yahoo','Yahoo店','yh-ui')").run().lastInsertRowid;
    const iYh = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at, is_unread)
      VALUES ('yahoo', ?, 'yh-ui-1', 'Yahoo問い合わせ', ?, 1)`).run(shopYh, T('2026-08-26T10:00:00+09:00')).lastInsertRowid;
    const dYh = await (await fetch(`${base}/inquiries/${iYh}?view=inbox`)).text();
    check('Yahoo!: 「✅ 送信して回答完了」+ ストクリも完了になる案内', dYh.includes('✅ 送信して回答完了') && dYh.includes('ストアクリエイターPro') && dYh.includes('var MALL_COMPLETE = true'));
    let jsErr2 = null;
    try { new vm.Script(scriptOf(dYh)); } catch (e) { jsErr2 = e; }
    check('詳細画面のクライアントJSが構文OK (Yahoo!)', jsErr2 === null, String(jsErr2));
    delete process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
  }
  const qd = await jp(`/api/inquiries/${h4}/status`, { status: 'done' });
  check('1クリック完了のAPI: done遷移+completed_at刻印', qd.status === 200
    && db.prepare('SELECT internal_status, completed_at FROM inquiries WHERE id = ?').get(h4).internal_status === 'done'
    && !!db.prepare('SELECT completed_at FROM inquiries WHERE id = ?').get(h4).completed_at);

  // ─── 顧客情報の手入力 (2026-08-31 中原さん要望) ───
  // 注文番号等が付いて来ないメール問い合わせに、NE等で調べた結果を保存ボタン無しで残す。
  // モール同期で確定した項目 (楽天/Yahoo!の購入者問い合わせ) は変更できない (ロック)
  console.log('  -- 顧客情報の手入力 (自動保存 + 確定情報ロック)');
  {
    const { customerInfoState, manualFieldsAfterSync } = await import('./customer-info.js');
    const vm = await import('vm');
    const scriptOf = html => { const m = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)]; return m.length ? m[m.length - 1][1] : ''; };
    const rowOf = id => db.prepare('SELECT * FROM inquiries WHERE id = ?').get(id);

    const ciMail = mkH('ci-mail', 'open');
    db.prepare('UPDATE inquiries SET customer_identifier = ? WHERE id = ?').run('ci-customer@example.com', ciMail);
    const dCi = await (await fetch(`${base}/inquiries/${ciMail}?view=inbox`)).text();
    check('詳細: パネル名は「顧客情報」(「チケット情報」は廃止)', dCi.includes('<h3>顧客情報</h3>') && !dCi.includes('チケット情報'));
    check('メール (注文番号なし): 注文番号・商品名・商品コードが自動保存の入力欄になる',
      dCi.includes('id="ci_order_number"') && dCi.includes('id="ci_product_name"') && dCi.includes('id="ci_product_code"') && dCi.includes('class="sub ci-hint">✏️'));
    check('注文番号が無いうちは「NEで受注検索」導線もそのまま出る', dCi.includes('id="neMailSearch"'));
    check('自動保存のクライアントJS (change で /customer-info へ keepalive 送信)',
      dCi.includes("post('/customer-info', data, { keepalive: true })") && dCi.includes("addEventListener('change'"));
    let jsErrCi = null; try { new vm.Script(scriptOf(dCi)); } catch (e) { jsErrCi = e; }
    check('詳細画面のクライアントJSが構文OK (顧客情報の自動保存込み)', jsErrCi === null, String(jsErrCi));

    const r1 = await jp(`/api/inquiries/${ciMail}/customer-info`, { order_number: ' 373343-20260831-00001 ' });
    const row1 = rowOf(ciMail);
    check('API: 注文番号を保存 (前後空白は除去) + manual_fields に手入力として記録',
      r1.status === 200 && row1.order_number === '373343-20260831-00001' && row1.manual_fields === '["order_number"]', JSON.stringify(r1.j));
    check('API: 応答に保存後のNE受注リンク (メールでも手入力の番号でNEを開ける)',
      !!r1.j.links && String(r1.j.links.ne_order_url).includes('kensaku_denpyo_no=373343-20260831-00001') && r1.j.links.mall_order_url === null);
    check('API: 操作ログ customer_info_edit (before/after は変更項目のみ)',
      !!db.prepare("SELECT 1 FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'customer_info_edit' AND before_json = '{\"order_number\":null}' AND after_json = '{\"order_number\":\"373343-20260831-00001\"}'").get(ciMail));
    check('一覧検索: 手入力した注文番号でも見つかる', listInquiries({ q: '373343-20260831-00001' }).rows.some(r => r.id === ciMail));
    const dCi2 = await (await fetch(`${base}/inquiries/${ciMail}?view=inbox`)).text();
    check('保存後の詳細: 手入力の注文番号は引き続き入力欄 (ロックしない) + NE直リンク + 検索導線は消える',
      dCi2.includes('id="ci_order_number"') && dCi2.includes('value="373343-20260831-00001"')
      && dCi2.includes('kensaku_denpyo_no=373343-20260831-00001') && !dCi2.includes('id="neMailSearch"') && !dCi2.includes('<span class="ci-lock"'));
    check('保存後の詳細: 対応履歴に「顧客情報の編集」として日本語で出る', dCi2.includes('顧客情報の編集') && dCi2.includes('注文番号=373343-20260831-00001'));

    const r2 = await jp(`/api/inquiries/${ciMail}/customer-info`, { product_name: 'アロマストーン', product_code: 'AS-01' });
    const row2 = rowOf(ciMail);
    check('API: 商品名+商品コードを同時に保存', r2.status === 200 && row2.product_name === 'アロマストーン' && row2.product_code === 'AS-01'
      && JSON.parse(row2.manual_fields).length === 3 && r2.j.changed.length === 2);
    const r3 = await jp(`/api/inquiries/${ciMail}/customer-info`, { order_number: '' });
    const row3 = rowOf(ciMail);
    check('API: 空文字で NULL に戻り manual_fields からも外れる (商品は残る)',
      r3.status === 200 && row3.order_number === null && !JSON.parse(row3.manual_fields).includes('order_number') && row3.product_name === 'アロマストーン');
    const r4 = await jp(`/api/inquiries/${ciMail}/customer-info`, { order_number: 'x'.repeat(101) });
    check('API: 長すぎる値は 400', r4.status === 400 && rowOf(ciMail).order_number === null);
    const r5 = await jp(`/api/inquiries/${ciMail}/customer-info`, { foo: 'bar' });
    check('API: 既知の項目が無ければ 400', r5.status === 400);
    const r6 = await jp(`/api/inquiries/${ciMail}/customer-info`, { product_name: 'アロマストーン' });
    const logCount = db.prepare("SELECT COUNT(*) AS n FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'customer_info_edit'").get(ciMail).n;
    check('API: 同じ値は unchanged (ログを増やさない)', r6.status === 200 && r6.j.unchanged === true && logCount === 3, `logs=${logCount}`);
    const r7 = await jp(`/api/inquiries/${ciMail}/customer-info`, { product_code: 'AS-01\nrm -rf' });
    check('API: 改行は空白に畳まれて1行の値として保存', r7.status === 200 && rowOf(ciMail).product_code === 'AS-01 rm -rf');

    // 確定情報ロック: 楽天の購入者問い合わせ (同期が注文番号を入れた = manual_fields 無し) は変更不可
    const shopCiRk = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天CI店','rk-ci')").run().lastInsertRowid;
    const ciRk = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, order_number, product_name, received_at, is_unread)
      VALUES ('rakuten', ?, 'rk-ci-1', '注文の件', '123456-20260831-00009', '楽天商品', ?, 1)`).run(shopCiRk, T('2026-08-31T10:00:00+09:00')).lastInsertRowid;
    const dRk = await (await fetch(`${base}/inquiries/${ciRk}?view=inbox`)).text();
    check('楽天 (注文番号確定): 注文番号・商品名は🔒表示で入力欄なし。空の商品コードだけ入力欄',
      !dRk.includes('id="ci_order_number"') && !dRk.includes('id="ci_product_name"') && dRk.includes('id="ci_product_code"')
      && dRk.includes('123456-20260831-00009<span class="ci-lock"') && dRk.includes('楽天商品<span class="ci-lock"'));
    check('楽天 (注文番号確定): NE受注行とモールの注文リンクは従来どおり',
      dRk.includes('<dt>NE受注</dt>') && dRk.includes('kensaku_denpyo_no=123456-20260831-00009') && dRk.includes('orderNumber=123456-20260831-00009'));
    let jsErrRk = null; try { new vm.Script(scriptOf(dRk)); } catch (e) { jsErrRk = e; }
    check('詳細画面のクライアントJSが構文OK (ロック混在)', jsErrRk === null, String(jsErrRk));
    const l1 = await jp(`/api/inquiries/${ciRk}/customer-info`, { order_number: '999' });
    check('API: 確定した注文番号の変更は 409 で拒否 (値は不変)', l1.status === 409 && /確定情報/.test(l1.j.error) && rowOf(ciRk).order_number === '123456-20260831-00009');
    const l2 = await jp(`/api/inquiries/${ciRk}/customer-info`, { order_number: '' });
    check('API: 確定した注文番号は空にもできない', l2.status === 409 && rowOf(ciRk).order_number === '123456-20260831-00009');
    const l3 = await jp(`/api/inquiries/${ciRk}/customer-info`, { order_number: '123456-20260831-00009', product_code: 'RK-SKU-1' });
    const rowL3 = rowOf(ciRk);
    check('API: 確定項目に同じ値 + 空だった商品コードの手入力 = 商品コードだけ保存',
      l3.status === 200 && rowL3.product_code === 'RK-SKU-1' && rowL3.manual_fields === '["product_code"]' && rowL3.order_number === '123456-20260831-00009');
    check('API: 確定した注文番号の 409 ではログを残さない',
      db.prepare("SELECT COUNT(*) AS n FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'customer_info_edit'").get(ciRk).n === 1);
    const l4 = await jp(`/api/inquiries/${ciRk}/customer-info`, { order_number: '999', product_code: 'RK-SKU-2' });
    check('API: ロック項目を含む一括更新は丸ごと拒否 (部分適用しない)', l4.status === 409 && rowOf(ciRk).product_code === 'RK-SKU-1');
    db.prepare('UPDATE inquiries SET product_code = ?, manual_fields = NULL WHERE id = ?').run('RK-FIXED', ciRk);
    const dRk2 = await (await fetch(`${base}/inquiries/${ciRk}?view=inbox`)).text();
    check('全項目が確定: 入力欄は出さず「変更できません」の案内だけ', !dRk2.includes('class="ci-input"') && dRk2.includes('🔒 注文番号・商品はモールから取得した確定情報です'));

    // 判定ヘルパー (customer-info.js)
    const st = customerInfoState({ order_number: 'A', product_code: null, product_name: 'B', manual_fields: '["product_name"]' });
    check('customerInfoState: 値あり+手入力でない=ロック / 手入力=編集可 / 空=編集可',
      st.order_number.locked && !st.product_name.locked && st.product_name.manual && !st.product_code.locked && !st.product_code.manual);
    check('customerInfoState: 壊れた manual_fields は「手入力なし」扱い (値ありはロック側に倒す)',
      customerInfoState({ order_number: 'A', manual_fields: '{bad' }).order_number.locked);
    check('manualFieldsAfterSync: 同期が値を返した項目は手入力から外れ、返さない項目は残る',
      manualFieldsAfterSync('["order_number","product_code"]', { orderNumber: 'X', productCode: null }) === '["product_code"]'
      && manualFieldsAfterSync('["order_number"]', { orderNumber: 'X' }) === null
      && manualFieldsAfterSync(null, { orderNumber: 'X' }) === null);
  }

  await new Promise(r => srv.close(r));
}

// ─── 結果 ───
console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
// process.exit() は稼働中のasyncハンドルとぶつかりWindowsでlibuv abortし得るため exitCode で自然終了
process.exitCode = failed === 0 ? 0 : 1;
