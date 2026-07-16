// inquiry-hub outbox送信ワーカースモーク: 設計書§8.3の状態機械と二重送信防止を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke-outbox.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止 (smoke.mjs と同じガード)
import fs from 'fs';
import path from 'path';
import { initInquiryHubDB, getDB, toUtcIso } from './db.js';
import { createReplyJob, runOutboxPass, sweepZombies, resolveUnknown, cancelJob, listOutboxIssues, SendRejectedError } from './outbox.js';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-outbox-smoke)');
  process.exit(2);
}
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
fs.writeFileSync(markerPath, `created by apps/inquiry-hub/smoke-outbox.mjs at ${new Date().toISOString()}\n`);

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = (name, fn, msgPart) => {
  try { fn(); check(name, false, '例外なし'); }
  catch (e) { check(name, !msgPart || String(e.message).includes(msgPart), `期待(${msgPart}) 実際(${e.message})`); }
};

const T0 = Date.parse('2026-07-17T06:00:00Z');
const iso = min => toUtcIso(T0 + min * 60000);

const shopEmail = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール窓口','support@test.jp')").run().lastInsertRowid;
const shopRunner = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier, executor) VALUES ('yahoo','Yahoo店','yh-seller','runner')").run().lastInsertRowid;

const mkInquiry = (shopId, channel, extId, rev = 0) => db.prepare(`INSERT INTO inquiries
    (channel_type, shop_id, external_inquiry_id, customer_name, subject, conversation_rev, received_at)
  VALUES (?,?,?,?,?,?,?)`).run(channel, shopId, extId, '顧客', '件名', rev, iso(-60)).lastInsertRowid;

const okAdapter = () => {
  const state = { calls: 0 };
  return { state, sendReply: async () => { state.calls++; return { externalReplyId: `ext-reply-${state.calls}` }; } };
};

// ─── 1. ジョブ作成の冪等性 ───
console.log('1. ジョブ作成');
const inqA = mkInquiry(shopEmail, 'email', 'th-a');
{
  const r1 = createReplyJob({ inquiryId: inqA, channelType: 'email', bodyText: '返信します',
    createdBy: 'user-x', clientOperationId: 'op-a1', baseConversationRev: 0 });
  const r2 = createReplyJob({ inquiryId: inqA, channelType: 'email', bodyText: '返信します',
    createdBy: 'user-x', clientOperationId: 'op-a1', baseConversationRev: 0 });
  check('作成 + 同一operation_idは冪等 (二重登録しない)', r1.created && !r2.created && r1.id === r2.id
    && db.prepare('SELECT COUNT(*) c FROM outbox_replies').get().c === 1);
  // 未決着ジョブがある間は別operation_idでも作成不可 (1問い合わせ1送信の保証。Codexレビュー反映)
  const dup = createReplyJob({ inquiryId: inqA, channelType: 'email', bodyText: '別の返信',
    createdBy: 'user-y', clientOperationId: 'op-a2', baseConversationRev: 0 });
  check('未決着ジョブ中は新規作成不可', dup.conflict != null && dup.conflict.includes('未決着')
    && db.prepare('SELECT COUNT(*) c FROM outbox_replies').get().c === 1);
  // rev競合の拒否 (未決着ジョブのない問い合わせで確認)
  const inqRev = mkInquiry(shopEmail, 'email', 'th-rev', 5);
  const conflict = createReplyJob({ inquiryId: inqRev, channelType: 'email', bodyText: 'x',
    createdBy: 'user-x', clientOperationId: 'op-rev1', baseConversationRev: 99 });
  check('登録時のrev競合は拒否', conflict.conflict != null && conflict.conflict.includes('会話が更新')
    && conflict.id === null);
  throws('本文空は拒否', () => createReplyJob({ inquiryId: inqA, channelType: 'email', bodyText: ' ',
    createdBy: 'u', clientOperationId: 'op-a3', baseConversationRev: 0 }), '本文');
  throws('channel不一致は拒否', () => createReplyJob({ inquiryId: inqA, channelType: 'rakuten', bodyText: 'x',
    createdBy: 'u', clientOperationId: 'op-a4', baseConversationRev: 0 }), '一致しません');
}

// ─── 2. 送信成功パス ───
console.log('2. 送信成功');
{
  const adapter = okAdapter();
  const r = await runOutboxPass({ email: adapter }, { now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-a1'").get();
  check('sent + external_reply_id + lease解放', r.processed === 1 && r.results[0].outcome === 'sent'
    && job.status === 'sent' && job.external_reply_id === 'ext-reply-1' && job.lease_token === null && job.sent_at === iso(0));
  const msg = db.prepare('SELECT * FROM inquiry_messages WHERE outbox_id = ?').get(job.id);
  check('送信メッセージが会話に記録', msg && msg.is_incoming === 0 && msg.sender_type === 'shop'
    && msg.sent_by_user_id === 'user-x' && msg.external_message_id === 'ext-reply-1');
  const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqA);
  check('自送信で rev++ & open→waiting_reply', inq.conversation_rev === 1 && inq.internal_status === 'waiting_reply');
  const r2 = await runOutboxPass({ email: adapter }, { now: T0 + 60000 });
  check('2周目は何もしない (二重送信なし)', r2.processed === 0 && adapter.state.calls === 1);
  check('操作ログ (created/sent)', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id=? AND action_type IN ('reply_created','reply_sent')").get(inqA).c === 2);
}

// ─── 3. 明確な拒否 → failed ───
console.log('3. 明確な拒否');
const inqB = mkInquiry(shopEmail, 'email', 'th-b');
{
  createReplyJob({ inquiryId: inqB, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-b1', baseConversationRev: 0 });
  const r = await runOutboxPass({ email: { sendReply: async () => { throw new SendRejectedError('本文が長すぎます'); } } }, { now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-b1'").get();
  check('rejected → failed (未送信確定)', r.results[0].outcome === 'failed' && job.status === 'failed'
    && job.error_message.includes('長すぎます')
    && db.prepare('SELECT COUNT(*) c FROM inquiry_messages WHERE outbox_id = ?').get(job.id).c === 0);
  check('sync_errors に send_failed 記録', db.prepare(
    "SELECT COUNT(*) c FROM sync_errors WHERE inquiry_id=? AND error_type='send_failed'").get(inqB).c === 1);
}

// ─── 4. 結果不明 → unknown (自動再送しない) → 人手解決 ───
console.log('4. unknown と人手解決');
const inqC = mkInquiry(shopEmail, 'email', 'th-c');
{
  createReplyJob({ inquiryId: inqC, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-c1', baseConversationRev: 0 });
  const adapter = { calls: 0, sendReply: async () => { throw new Error('ETIMEDOUT'); } };
  await runOutboxPass({ email: adapter }, { now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-c1'").get();
  check('タイムアウト → unknown', job.status === 'unknown');
  const r2 = await runOutboxPass({ email: okAdapter() }, { now: T0 + 60000 });
  check('unknown は自動再送されない', r2.processed === 0
    && db.prepare("SELECT status FROM outbox_replies WHERE id = ?").get(job.id).status === 'unknown');

  throws('不正なresolutionは拒否', () => resolveUnknown(job.id, 'retry', 'boss'), '不正な resolution');
  const res = resolveUnknown(job.id, 'confirmed_not_sent', 'boss', { now: T0 + 120000 });
  check('confirmed_not_sent → failed (再送は新ジョブで可)', res.status === 'failed'
    && db.prepare('SELECT resolution, resolved_by FROM outbox_replies WHERE id = ?').get(job.id).resolution === 'confirmed_not_sent');
  throws('解決済みジョブの再解決は拒否', () => resolveUnknown(job.id, 'abandoned', 'boss'), 'unknown 状態ではありません');

  // confirmed_sent: 会話に記録される
  createReplyJob({ inquiryId: inqC, channelType: 'email', bodyText: '再送分', createdBy: 'u',
    clientOperationId: 'op-c2', baseConversationRev: 0 });
  await runOutboxPass({ email: { sendReply: async () => { throw new Error('socket hang up'); } } }, { now: T0 + 180000 });
  const job2 = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-c2'").get();
  const revBefore = db.prepare('SELECT conversation_rev FROM inquiries WHERE id=?').get(inqC).conversation_rev;
  const res2 = resolveUnknown(job2.id, 'confirmed_sent', 'boss', { now: T0 + 240000 });
  check('confirmed_sent → sent + 会話に記録 + rev++', res2.status === 'sent'
    && db.prepare('SELECT COUNT(*) c FROM inquiry_messages WHERE outbox_id = ?').get(job2.id).c === 1
    && db.prepare('SELECT conversation_rev FROM inquiries WHERE id=?').get(inqC).conversation_rev === revBefore + 1);

  // abandoned: unknown のまま resolution だけ確定
  createReplyJob({ inquiryId: inqC, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-c3', baseConversationRev: db.prepare('SELECT conversation_rev FROM inquiries WHERE id=?').get(inqC).conversation_rev });
  await runOutboxPass({ email: { sendReply: async () => { throw new Error('timeout'); } } }, { now: T0 + 300000 });
  const job3 = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-c3'").get();
  const res3 = resolveUnknown(job3.id, 'abandoned', 'boss');
  check('abandoned → unknownのまま確定 (再送ボタン対象外)', res3.status === 'unknown'
    && db.prepare('SELECT resolution FROM outbox_replies WHERE id=?').get(job3.id).resolution === 'abandoned');
  throws('abandoned は終端 (後から変更不可)', () => resolveUnknown(job3.id, 'confirmed_not_sent', 'boss'), 'unknown 状態ではありません');
}

// ─── 5. ゾンビ回収 (lease切れ sending → unknown) ───
console.log('5. ゾンビ回収');
const inqD = mkInquiry(shopEmail, 'email', 'th-d');
{
  createReplyJob({ inquiryId: inqD, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-d1', baseConversationRev: 0 });
  db.prepare("UPDATE outbox_replies SET status='sending', lease_token='dead-worker', lease_until=? WHERE client_operation_id='op-d1'")
    .run(iso(-1));
  const swept = sweepZombies({ now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-d1'").get();
  check('lease切れ sending → unknown (pendingに戻さない)', swept === 1 && job.status === 'unknown');
  const r = await runOutboxPass({ email: okAdapter() }, { now: T0 + 60000 });
  check('ゾンビは再送されない', r.processed === 0);
}

// ─── 6. 送信直前のrev競合 → needs_review ───
console.log('6. rev競合 → needs_review');
const inqE = mkInquiry(shopEmail, 'email', 'th-e');
{
  createReplyJob({ inquiryId: inqE, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-e1', baseConversationRev: 0 });
  db.prepare('UPDATE inquiries SET conversation_rev = 1 WHERE id = ?').run(inqE); // 顧客新着を模倣
  const adapter = okAdapter();
  const r = await runOutboxPass({ email: adapter }, { now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-e1'").get();
  check('rev競合 → needs_review + 送信しない', r.results[0].outcome === 'needs_review'
    && job.status === 'needs_review' && adapter.state.calls === 0);
  const r2 = await runOutboxPass({ email: adapter }, { now: T0 + 60000 });
  check('needs_review は二度と拾わない (永久ループなし)', r2.processed === 0 && adapter.state.calls === 0);

  // 再送信フロー: needs_review を取消 → 新ジョブとして作成できる
  const cancelled = cancelJob(job.id, 'user-x');
  check('needs_review の取消', cancelled.status === 'cancelled'
    && db.prepare('SELECT status FROM outbox_replies WHERE id=?').get(job.id).status === 'cancelled');
  throws('取消済みの再取消は拒否', () => cancelJob(job.id, 'user-x'), '取消可能な状態');
  const recreate = createReplyJob({ inquiryId: inqE, channelType: 'email', bodyText: '新しい内容で再送信',
    createdBy: 'user-x', clientOperationId: 'op-e2', baseConversationRev: 1 });
  check('取消後は新ジョブを作成できる', recreate.created === true);
  const r3 = await runOutboxPass({ email: adapter }, { now: T0 + 120000 });
  check('再作成ジョブが送信される', r3.results[0]?.outcome === 'sent' && adapter.state.calls === 1);
}

// ─── 6.5 同一問い合わせの sending 排他 (claimレベルの多層防御) ───
console.log('6.5 claim排他');
const inqG = mkInquiry(shopEmail, 'email', 'th-g');
{
  // 作成ガードを迂回して2つのジョブを直接投入 (並行ワーカーの競合状態を模倣)
  const ins = db.prepare(`INSERT INTO outbox_replies (inquiry_id, channel_type, client_operation_id, body_text, created_by, base_conversation_rev, status, lease_token, lease_until)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  ins.run(inqG, 'email', 'op-g1', 'x', 'u', 0, 'sending', 'worker-1', iso(60));
  ins.run(inqG, 'email', 'op-g2', 'y', 'u', 0, 'pending', null, null);
  const adapter = okAdapter();
  const r = await runOutboxPass({ email: adapter }, { now: T0 });
  check('同一問い合わせが sending 中は pending を拾わない', r.processed === 0 && adapter.state.calls === 0);
  db.prepare("UPDATE outbox_replies SET status='cancelled', lease_token=NULL WHERE client_operation_id IN ('op-g1','op-g2')").run();
}

// ─── 7. executor ゲート (Yahoo!ローカルランナー構成) ───
console.log('7. executor ゲート');
const inqY = mkInquiry(shopRunner, 'yahoo', 'yh-t1');
{
  createReplyJob({ inquiryId: inqY, channelType: 'yahoo', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-y1', baseConversationRev: 0 });
  const serverAdapter = okAdapter();
  const rServer = await runOutboxPass({ yahoo: serverAdapter }, { now: T0, executor: 'server' });
  check('server workerは executor=runner のジョブを拾わない', rServer.processed === 0 && serverAdapter.state.calls === 0);
  const runnerAdapter = okAdapter();
  const rRunner = await runOutboxPass({ yahoo: runnerAdapter }, { now: T0, executor: 'runner' });
  check('runner workerが送信する', rRunner.processed === 1 && rRunner.results[0].outcome === 'sent' && runnerAdapter.state.calls === 1);
}

// ─── 8. アダプター未設定 → needs_review / リース喪失 → メッセージ挿入しない ───
console.log('8. 異常系');
const inqF = mkInquiry(shopEmail, 'email', 'th-f');
{
  createReplyJob({ inquiryId: inqF, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-f1', baseConversationRev: 0 });
  const r = await runOutboxPass({}, { now: T0 });
  check('アダプター未設定 → needs_review', r.results[0].outcome === 'needs_review');

  cancelJob(db.prepare("SELECT id FROM outbox_replies WHERE client_operation_id='op-f1'").get().id, 'u');
  createReplyJob({ inquiryId: inqF, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-f2', baseConversationRev: 0 });
  const thief = { sendReply: async () => {
    db.prepare("UPDATE outbox_replies SET lease_token='thief' WHERE client_operation_id='op-f2'").run();
    return { externalReplyId: 'ext-f2' };
  } };
  const r2 = await runOutboxPass({ email: thief }, { now: T0 });
  const job = db.prepare("SELECT * FROM outbox_replies WHERE client_operation_id = 'op-f2'").get();
  check('リース喪失 → lease_lost (完了処理・メッセージ挿入をしない)', r2.results[0].outcome === 'lease_lost'
    && db.prepare('SELECT COUNT(*) c FROM inquiry_messages WHERE outbox_id = ?').get(job.id).c === 0);
}

// ─── 9. issues一覧 ───
console.log('9. issues一覧');
{
  // needs_review の実例を1件用意 (前段のものは取消済みのため)
  const inqH = mkInquiry(shopEmail, 'email', 'th-h');
  createReplyJob({ inquiryId: inqH, channelType: 'email', bodyText: 'x', createdBy: 'u',
    clientOperationId: 'op-h1', baseConversationRev: 0 });
  await runOutboxPass({}, { now: T0 });
  const issues = listOutboxIssues();
  const statuses = new Set(issues.map(i => i.status));
  check('unknown/needs_review/failed が一覧に載る', statuses.has('unknown') && statuses.has('needs_review') && statuses.has('failed')
    && issues.every(i => i.shop_name && i.subject !== undefined));
  check('resolution確定済み (abandoned等) は一覧に載らない', issues.every(i => i.resolution === null));
}

console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
