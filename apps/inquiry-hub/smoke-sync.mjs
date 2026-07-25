// inquiry-hub 同期エンジンスモーク: モックアダプターで設計書§8.1/§8.2の挙動を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke-sync.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止 (smoke.mjs と同じガード)
import fs from 'fs';
import path from 'path';
import { initInquiryHubDB, getDB, toUtcIso } from './db.js';
import { runSync, listSyncStatus, syntheticMessageId, OVERLAP_MS } from './sync/engine.js';
import { createMockAdapter } from './sync/adapters/mock.js';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-sync-smoke)');
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
fs.writeFileSync(markerPath, `created by apps/inquiry-hub/smoke-sync.mjs at ${new Date().toISOString()}\n`);

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

const shopId = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','rk-shop')").run().lastInsertRowid;

// テスト用時計 (決定的にするため固定エポックから進める)
const T0 = Date.parse('2026-07-17T03:00:00Z');
const iso = min => toUtcIso(T0 + min * 60000);

const item = (over = {}) => ({
  externalInquiryId: 'rk-001',
  customerName: '顧客A', customerIdentifier: 'mask-a', subject: '商品について',
  orderNumber: 'ORD-1', productCode: 'sku-1', productName: '商品1',
  externalStatus: 'incomplete', externalIsRead: false,
  receivedAt: iso(-120), updatedAt: iso(-120),
  messages: [
    { externalMessageId: 'rk-001-m1', senderType: 'customer', senderName: '顧客A',
      bodyText: '質問です', isIncoming: 1, receivedAt: iso(-120),
      attachments: [{ fileName: 'photo.jpg', contentType: 'image/jpeg', fileSize: 1000 }] },
  ],
  ...over,
});

// ─── 1. 初回同期 ───
console.log('1. 初回同期');
{
  const mock = createMockAdapter([item()]);
  const r = await runSync(shopId, mock, { now: T0 });
  check('初回同期 ok', r.ok && r.stats.newInquiries === 1 && r.stats.newMessages === 1);
  const st = db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
  check('committed_until が now に前進', st.committed_until === toUtcIso(T0));
  check('lease 解放済み', st.lease_until === null);
  const inq = db.prepare('SELECT * FROM inquiries WHERE external_inquiry_id = ?').get('rk-001');
  check('チケット作成 + 外部状態保存', inq.internal_status === 'open' && inq.external_status === 'incomplete'
    && inq.external_is_read === 0 && inq.conversation_rev === 1 && inq.is_unread === 1);
  check('添付メタデータ (synthetic ID)', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_attachments a JOIN inquiry_messages m ON m.id=a.inquiry_message_id WHERE m.inquiry_id=? AND a.external_attachment_id GLOB 'syn:*' AND a.fetch_status='pending'").get(inq.id).c === 1);
  check('初回 sinceIso は backfill 起点', mock.state.lastArgs.sinceIso === toUtcIso(T0 - 30 * 86400000));
}

// ─── 2. 再同期の冪等性 (オーバーラップ再取得しても増殖しない) ───
console.log('2. 再同期の冪等性');
{
  const mock = createMockAdapter([item()]);
  const r = await runSync(shopId, mock, { now: T0 + 15 * 60000 });
  check('再同期 ok・新規なし', r.ok && r.stats.newInquiries === 0 && r.stats.newMessages === 0);
  const inq = db.prepare('SELECT * FROM inquiries WHERE external_inquiry_id = ?').get('rk-001');
  check('rev 据え置き・メッセージ/添付増殖なし', inq.conversation_rev === 1
    && db.prepare('SELECT COUNT(*) c FROM inquiry_messages WHERE inquiry_id = ?').get(inq.id).c === 1
    && db.prepare('SELECT COUNT(*) c FROM inquiry_attachments a JOIN inquiry_messages m ON m.id=a.inquiry_message_id WHERE m.inquiry_id=?').get(inq.id).c === 1);
  check('sinceIso = committed_until − 60分', mock.state.lastArgs.sinceIso === toUtcIso(T0 - OVERLAP_MS));
}

// ─── 3. 店舗返信の取り込み (状態は変えない) → 顧客新着で done→open ───
console.log('3. 状態遷移');
{
  const inqRow = db.prepare('SELECT * FROM inquiries WHERE external_inquiry_id = ?').get('rk-001');
  // AI下書きを置いて stale 化を確認する
  const jobId = db.prepare('INSERT INTO ai_jobs (inquiry_id, input_rev) VALUES (?, 1)').run(inqRow.id).lastInsertRowid;
  db.prepare('INSERT INTO ai_drafts (inquiry_id, ai_job_id, input_rev, draft_body) VALUES (?,?,1,?)').run(inqRow.id, jobId, '返信案');
  db.prepare("UPDATE inquiries SET internal_status='done', is_unread=0, completed_at=? WHERE id=?").run(iso(-10), inqRow.id);

  // 店舗返信 (is_incoming=0) の新着: done のまま・未読にならない・rev は上がる
  const withShopReply = item({ updatedAt: iso(20), messages: [
    ...item().messages,
    { externalMessageId: 'rk-001-m2', senderType: 'shop', senderName: '店舗', bodyText: '回答です', isIncoming: 0, sentAt: iso(10), receivedAt: iso(10) },
  ]});
  await runSync(shopId, createMockAdapter([withShopReply]), { now: T0 + 30 * 60000 });
  let inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
  check('店舗返信: done のまま・未読化しない', inq.internal_status === 'done' && inq.is_unread === 0 && inq.conversation_rev === 2);
  check('店舗返信でも ai_draft は stale 化', db.prepare('SELECT is_stale FROM ai_drafts WHERE inquiry_id = ?').get(inqRow.id).is_stale === 1);

  // 顧客新着: done → open + 未読 + last_message_at 前進
  const withCustomerMsg = item({ updatedAt: iso(40), messages: [
    ...withShopReply.messages,
    { externalMessageId: 'rk-001-m3', senderType: 'customer', senderName: '顧客A', bodyText: '追加質問', isIncoming: 1, receivedAt: iso(35) },
  ]});
  const r = await runSync(shopId, createMockAdapter([withCustomerMsg]), { now: T0 + 45 * 60000 });
  inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
  check('顧客新着: done→open + 未読 + completed_atクリア', r.stats.reopened === 1
    && inq.internal_status === 'open' && inq.is_unread === 1 && inq.completed_at === null
    && inq.last_message_at === iso(35) && inq.conversation_rev === 3);
  check('再オープンが操作ログに残る', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id=? AND actor_type='system' AND action_type='status_change'").get(inqRow.id).c === 1);

  // メールディーラー準拠のステータス自動遷移 (2026-07-25)
  // ① 店舗から返信 (メールディーラー等アプリ外からの返信も同期で検知) → 新着系は「返信処理中」へ
  const withShopReply2 = item({ updatedAt: iso(50), messages: [
    ...withCustomerMsg.messages,
    { externalMessageId: 'rk-001-m4', senderType: 'shop', senderName: '店舗', bodyText: '追ってご案内します', isIncoming: 0, sentAt: iso(48) },
  ]});
  const r2 = await runSync(shopId, createMockAdapter([withShopReply2]), { now: T0 + 55 * 60000 });
  inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
  check('店舗返信で open→waiting_reply (返信処理中) へ自動遷移',
    r2.stats.movedToWaiting === 1 && inq.internal_status === 'waiting_reply');

  // ② 返信処理中に顧客から返事 → 新着へ戻る (完了からだけでなく返信処理中からも)
  const withCustomerAgain = item({ updatedAt: iso(60), messages: [
    ...withShopReply2.messages,
    { externalMessageId: 'rk-001-m5', senderType: 'customer', senderName: '顧客A', bodyText: 'ありがとうございます', isIncoming: 1, receivedAt: iso(58) },
  ]});
  const r3 = await runSync(shopId, createMockAdapter([withCustomerAgain]), { now: T0 + 65 * 60000 });
  inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
  check('返信処理中に顧客返信 → 新着へ戻る',
    r3.stats.reopened === 1 && inq.internal_status === 'open' && inq.is_unread === 1);
  // ③ 完了のまま店舗が追加案内しても完了を維持する (完了後の連絡で受信箱を汚さない)
  db.prepare("UPDATE inquiries SET internal_status = 'done' WHERE id = ?").run(inqRow.id);
  const withShopReply3 = item({ updatedAt: iso(70), messages: [
    ...withCustomerAgain.messages,
    { externalMessageId: 'rk-001-m6', senderType: 'shop', bodyText: '補足です', isIncoming: 0, sentAt: iso(68) },
  ]});
  await runSync(shopId, createMockAdapter([withShopReply3]), { now: T0 + 75 * 60000 });
  check('完了は店舗返信では変わらない',
    db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(inqRow.id).internal_status === 'done');

}

// ─── 4. 失敗時: committed_until 据え置き + エラー記録 → 復旧でリセット ───
console.log('4. 失敗と復旧');
{
  const stBefore = db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
  const failing = createMockAdapter([item()], { failAtCall: 1 });
  const r = await runSync(shopId, failing, { now: T0 + 60 * 60000 });
  const st = db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
  check('失敗: ok=false + committed_until 据え置き', !r.ok && st.committed_until === stBefore.committed_until);
  check('失敗: sync_errors 記録 + consecutive_failures=1', st.consecutive_failures === 1 && st.last_error?.includes('mock failure')
    && db.prepare("SELECT COUNT(*) c FROM sync_errors WHERE shop_id=? AND error_type='fetch_failed' AND resolved=0").get(shopId).c === 1);
  check('失敗: lease は解放される', st.lease_until === null);

  const ok = await runSync(shopId, createMockAdapter([]), { now: T0 + 75 * 60000 });
  const st2 = db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
  check('復旧: consecutive_failures リセット + committed 前進', ok.ok
    && st2.consecutive_failures === 0 && st2.last_error === null && st2.committed_until === toUtcIso(T0 + 75 * 60000));
}

// ─── 5. リース (多重起動防止) ───
console.log('5. リース');
{
  // リースを手で立てて他プロセス実行中を模倣
  db.prepare('UPDATE sync_state SET lease_until = ? WHERE shop_id = ?').run(toUtcIso(T0 + 999 * 60000), shopId);
  const r = await runSync(shopId, createMockAdapter([]), { now: T0 + 80 * 60000 });
  check('リース保持中は skipped', !r.ok && r.skipped === 'lease');
  // リース期限切れなら奪取できる
  const r2 = await runSync(shopId, createMockAdapter([]), { now: T0 + 1000 * 60000 });
  check('期限切れリースは奪取して実行', r2.ok);
}

// ─── 6. 修復同期 + cursor 伝搬 + inactive/不在shop ───
console.log('6. 修復同期ほか');
{
  const mock = createMockAdapter([], { nextCursor: 'cursor-123' });
  await runSync(shopId, mock, { now: T0 + 1010 * 60000, repair: true });
  check('repair: sinceIso が直近3日起点', mock.state.lastArgs.sinceIso === toUtcIso(T0 + 1010 * 60000 - 3 * 86400000));
  check('nextCursor が保存される', db.prepare('SELECT sync_cursor FROM sync_state WHERE shop_id=?').get(shopId).sync_cursor === 'cursor-123');
  const mock2 = createMockAdapter([]);
  await runSync(shopId, mock2, { now: T0 + 1020 * 60000 });
  check('cursor が次回 fetchNew に渡る', mock2.state.lastArgs.cursor === 'cursor-123');

  db.prepare('UPDATE shops SET is_active = 0 WHERE id = ?').run(shopId);
  const rInactive = await runSync(shopId, createMockAdapter([]), { now: T0 + 1030 * 60000 });
  check('inactive shop は skipped', !rInactive.ok && rInactive.skipped === 'inactive');
  db.prepare('UPDATE shops SET is_active = 1 WHERE id = ?').run(shopId);
  const rMissing = await runSync(99999, createMockAdapter([]), { now: T0 });
  check('存在しない shop はエラー', !rMissing.ok && /存在しません/.test(rMissing.error));
}

// ─── 7. Codexレビュー反映分 ───
console.log('7. rev加算・後着添付・observedUntil・リース奪取');
{
  // 複数新着メッセージ → rev はメッセージ件数ぶん加算
  const multi = item({ externalInquiryId: 'rk-multi', updatedAt: iso(1100), receivedAt: iso(1090), messages: [
    { externalMessageId: 'mm-1', senderType: 'customer', bodyText: '1', isIncoming: 1, receivedAt: iso(1090) },
    { externalMessageId: 'mm-2', senderType: 'customer', bodyText: '2', isIncoming: 1, receivedAt: iso(1095) },
    { externalMessageId: 'mm-3', senderType: 'shop', bodyText: '3', isIncoming: 0, sentAt: iso(1099) },
  ]});
  await runSync(shopId, createMockAdapter([multi]), { now: T0 + 1100 * 60000 });
  const mInq = db.prepare("SELECT * FROM inquiries WHERE external_inquiry_id = 'rk-multi'").get();
  check('複数新着: rev = メッセージ件数', mInq.conversation_rev === 3 && mInq.last_message_at === iso(1099));

  // 後着添付: 既存メッセージに添付が後から現れても取り込める
  const withLateAtt = { ...multi, updatedAt: iso(1110), messages: multi.messages.map(m =>
    m.externalMessageId === 'mm-1' ? { ...m, attachments: [{ externalAttachmentId: 'late-att', fileName: 'late.jpg' }] } : m) };
  const rLate = await runSync(shopId, createMockAdapter([withLateAtt]), { now: T0 + 1110 * 60000 });
  check('後着添付を既存メッセージに取り込む', rLate.stats.newMessages === 0
    && db.prepare("SELECT COUNT(*) c FROM inquiry_attachments WHERE external_attachment_id = 'late-att'").get().c === 1
    && db.prepare('SELECT conversation_rev FROM inquiries WHERE id = ?').get(mInq.id).conversation_rev === 3);

  // observedUntil < untilIso → committed は observedUntil までしか進まない。ただし既存より後退しない (単調増加)
  const obs = iso(1105);
  await runSync(shopId, createMockAdapter([], { observedUntil: obs }), { now: T0 + 1120 * 60000 });
  {
    const st = db.prepare('SELECT committed_until, observed_until FROM sync_state WHERE shop_id=?').get(shopId);
    check('committed_until は後退しない (単調増加) + observed は記録',
      st.committed_until === iso(1110) && st.observed_until === obs);
  }
  // 未コミット領域に対しては observedUntil が上限になる
  const obs2 = iso(1115);
  await runSync(shopId, createMockAdapter([], { observedUntil: obs2 }), { now: T0 + 1120 * 60000 });
  check('committed_until = observedUntil (完全列挙の上限)',
    db.prepare('SELECT committed_until FROM sync_state WHERE shop_id=?').get(shopId).committed_until === obs2);

  // リース奪取: fetchNew 中に別ジョブへリースが移ったらコミット破棄+奪取側のリースを消さない
  const stolenToken = 'stolen-token-xyz';
  const thief = createMockAdapter([item({ externalInquiryId: 'rk-lost', updatedAt: iso(1125), receivedAt: iso(1125),
      messages: [{ externalMessageId: 'lost-m1', senderType: 'customer', bodyText: 'x', isIncoming: 1, receivedAt: iso(1125) }] })], {
    onFetch: () => db.prepare('UPDATE sync_state SET lease_token = ?, lease_until = ? WHERE shop_id = ?')
      .run(stolenToken, toUtcIso(T0 + 2000 * 60000), shopId),
  });
  const before = db.prepare('SELECT committed_until FROM sync_state WHERE shop_id=?').get(shopId);
  const rLost = await runSync(shopId, thief, { now: T0 + 1130 * 60000 });
  const after = db.prepare('SELECT * FROM sync_state WHERE shop_id=?').get(shopId);
  check('リース喪失: コミット破棄 (leaseLost)', !rLost.ok && rLost.leaseLost === true
    && db.prepare("SELECT COUNT(*) c FROM inquiries WHERE external_inquiry_id = 'rk-lost'").get().c === 0
    && after.committed_until === before.committed_until);
  check('リース喪失: 奪取側のリースを消さない・状態を汚さない', after.lease_token === stolenToken
    && after.consecutive_failures === 0
    && db.prepare("SELECT COUNT(*) c FROM sync_errors WHERE shop_id=? AND error_type='lease_lost'").get(shopId).c === 1);
  db.prepare('UPDATE sync_state SET lease_token = NULL, lease_until = NULL WHERE shop_id = ?').run(shopId);
}

// ─── 8. synthetic ID の決定性 + 同期状態サマリ ───
console.log('8. synthetic ID・サマリ');
{
  const a = syntheticMessageId('rk-001', 1, iso(0), '本文');
  const b = syntheticMessageId('rk-001', 1, iso(0), '本文');
  const c = syntheticMessageId('rk-001', 0, iso(0), '本文');
  check('syntheticMessageId 決定的 + 区分で変わる', a === b && a !== c && a.startsWith('syn:'));

  const status = listSyncStatus();
  check('listSyncStatus', status.length === 1 && status[0].shop_id === shopId
    && typeof status[0].open_errors === 'number' && status[0].open_errors >= 1);
}

// ─── 9. 1回の同期で顧客・店舗の両方を取り込むケース (独立店舗で検証。Codex R1 high) ───
// 同期間隔中に往復した場合、件数で判定すると本来「返信処理中」なのに「新着」になってしまう。
// 時系列で最後のメッセージの向きを見ていることを確認する
console.log('9. 双方向を一括取込');
{
  const shopMix = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','混在店','rk-mix')").run().lastInsertRowid;
  const mixItem = (msgs, updatedAt) => ({
    externalInquiryId: 'mix-1', customerName: '顧客M', subject: '混在テスト',
    receivedAt: iso(0), updatedAt, messages: msgs,
  });
  const m1 = { externalMessageId: 'mix-m1', senderType: 'customer', bodyText: '質問です', isIncoming: 1, receivedAt: iso(0) };
  await runSync(shopMix, createMockAdapter([mixItem([m1], iso(0))]), { now: T0 + 5 * 60000 });
  const mixId = db.prepare("SELECT id FROM inquiries WHERE external_inquiry_id = 'mix-1'").get().id;
  check('初回は新着', db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(mixId).internal_status === 'open');

  // 顧客返信 → 店舗返信 を1回でまとめて取り込む (最後が店舗)。
  // 「この同期で新たに未読化しない」ことを見るため、いったん既読にしてから取り込む
  db.prepare('UPDATE inquiries SET is_unread = 0 WHERE id = ?').run(mixId);
  const r1 = await runSync(shopMix, createMockAdapter([mixItem([m1,
    { externalMessageId: 'mix-m2', senderType: 'customer', bodyText: '追加質問', isIncoming: 1, receivedAt: iso(10) },
    { externalMessageId: 'mix-m3', senderType: 'shop', bodyText: 'ご案内です', isIncoming: 0, sentAt: iso(20) },
  ], iso(25))]), { now: T0 + 30 * 60000 });
  const after1 = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(mixId);
  check('顧客→店舗の一括取込: 最後が店舗なので「返信処理中」(新着にしない)',
    after1.internal_status === 'waiting_reply' && r1.stats.movedToWaiting === 1 && r1.stats.reopened === 0);
  check('一括取込でも未読にしない (最後が店舗返信)', after1.is_unread === 0);

  // 店舗返信 → 顧客返信 を1回でまとめて取り込む (最後が顧客)
  const r2 = await runSync(shopMix, createMockAdapter([mixItem([m1,
    { externalMessageId: 'mix-m2', senderType: 'customer', bodyText: '追加質問', isIncoming: 1, receivedAt: iso(10) },
    { externalMessageId: 'mix-m3', senderType: 'shop', bodyText: 'ご案内です', isIncoming: 0, sentAt: iso(20) },
    { externalMessageId: 'mix-m4', senderType: 'shop', bodyText: '補足です', isIncoming: 0, sentAt: iso(30) },
    { externalMessageId: 'mix-m5', senderType: 'customer', bodyText: 'ありがとうございます', isIncoming: 1, receivedAt: iso(40) },
  ], iso(45))]), { now: T0 + 50 * 60000 });
  const after2 = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(mixId);
  check('店舗→顧客の一括取込: 最後が顧客なので「新着」+未読',
    after2.internal_status === 'open' && after2.is_unread === 1 && r2.stats.reopened === 1);
}

console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
