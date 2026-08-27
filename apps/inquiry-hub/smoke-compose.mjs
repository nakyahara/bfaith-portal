// inquiry-hub 新規メール作成スモーク (2026-08-27): 署名マスタ + 下書き/確定/掃除 + 送信経路 + 画面
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke-compose.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止 (smoke.mjs と同じガード)
import fs from 'fs';
import path from 'path';
import { initInquiryHubDB, getDB, toUtcIso } from './db.js';
import { createSignature, updateSignature, deleteSignature, listSignatures, getDefaultSignature,
  composeBodyWithSignature, SIGNATURE_BODY_MAX } from './signatures.js';
import { normalizeRecipient, normalizeSubject, normalizeCustomerName, validateBody, createComposeDraft,
  getComposeDraft, finalizeComposeDraft, pruneStaleComposeDrafts, attachExternalThread,
  adoptComposeInquiryByMessageIds, flagComposeThreadSplit, isComposeThread, listMailShops,
  resolveMailShop, COMPOSE_PREFIX, BODY_MAX } from './compose.js';
import { createReplyJob, runOutboxPass } from './outbox.js';
import { createGmailAdapter } from './sync/adapters/gmail.js';
import { listInquiries } from './queries.js';
import { setResolverForTest, clearMxCache } from './mx-check.js';

// 宛先ドメインの事前確認 (mx-check.js) は実DNSを引かせない。
// 'gmial.test' だけ「ドメインが存在しない」= 打ち間違いの再現、それ以外は受け取れる扱い
const nxError = () => Object.assign(new Error('stub ENOTFOUND'), { code: 'ENOTFOUND' });
function restoreStubResolver() {
  setResolverForTest({
    resolveMx: async (d) => {
      if (d.endsWith('gmial.test')) throw nxError();
      return [{ exchange: `mail.${d}`, priority: 10 }];
    },
    resolve4: async () => { throw nxError(); },
    resolve6: async () => { throw nxError(); },
  });
}
restoreStubResolver();
clearMxCache();

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-compose-smoke)');
  process.exit(2);
}
const dbPath = path.join(process.env.DATA_DIR, 'inquiry-hub.db');
const markerPath = path.join(process.env.DATA_DIR, '.inquiry-hub-smoke-db');
if (fs.existsSync(dbPath) && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の inquiry-hub.db があります (smoke 生成マーカーなし)。実 DB の可能性があるため中断します');
  process.exit(2);
}
for (const suffix of ['', '-wal', '-shm']) { try { fs.rmSync(dbPath + suffix); } catch { /* 無ければ良い */ } }

initInquiryHubDB();
const db = getDB();
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
fs.writeFileSync(markerPath, `created by apps/inquiry-hub/smoke-compose.mjs at ${new Date().toISOString()}\n`);

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = (name, fn, msgPart) => {
  try { fn(); check(name, false, '例外なし'); }
  catch (e) { check(name, !msgPart || String(e.message).includes(msgPart), `期待(${msgPart}) 実際(${e.message})`); }
};
const rejects = async (name, fn, msgPart) => {
  try { await fn(); check(name, false, '例外なし'); }
  catch (e) { check(name, !msgPart || String(e.message).includes(msgPart), `期待(${msgPart}) 実際(${e.message})`); }
};

// 改行を含む本文のサンプル (validateBody は整形せずそのまま返すことの確認用)
const BODY_SAMPLE = [' 本文', '', '2行目 '].join('\n');

const shopEmail = db.prepare(
  "INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール窓口','info@b-faith.biz')")
  .run().lastInsertRowid;

// ═══ 1. 署名マスタ ═══
console.log('1. 署名');
{
  const s1 = createSignature({ name: '雑貨イズム署名', body: '雑貨イズム（B-Faith株式会社）\nTEL 00-0000-0000', createdBy: 'tester' });
  check('最初の1件は自動的に既定になる', s1.isDefault === true && getDefaultSignature()?.id === s1.id);

  const s2 = createSignature({ name: 'メーカー・問屋への発注の署名', body: 'B-Faith株式会社 仕入担当', createdBy: 'tester' });
  check('2件目は既定にならない', s2.isDefault === false && getDefaultSignature()?.id === s1.id);
  check('一覧は既定→表示順の並び', listSignatures().map(s => s.id)[0] === s1.id && listSignatures().length === 2);

  throws('同名 (全角/半角・大小の違いも含む) は作れない',
    () => createSignature({ name: '雑貨イズム署名', body: 'x' }), '同じ名前');
  throws('名前が空は作れない', () => createSignature({ name: '  ', body: 'x' }), '名前が空');
  throws('本文が空は作れない', () => createSignature({ name: '空本文', body: '   ' }), '本文が空');
  throws('本文が長すぎると作れない',
    () => createSignature({ name: '長い', body: 'あ'.repeat(SIGNATURE_BODY_MAX + 1) }), '長すぎ');

  updateSignature(s2.id, { isDefault: true });
  check('既定は常に1件だけ (切替で前の既定が外れる)',
    getDefaultSignature().id === s2.id
    && db.prepare('SELECT COUNT(*) c FROM inquiry_signatures WHERE is_default = 1').get().c === 1);

  updateSignature(s2.id, { name: '発注用の署名', body: 'B-Faith株式会社 仕入担当\nTEL 00-0000-0000' });
  check('改名・本文変更が保存される',
    db.prepare('SELECT name, body FROM inquiry_signatures WHERE id = ?').get(s2.id).name === '発注用の署名');

  deleteSignature(s2.id);
  check('削除は論理削除 + 既定も外れる (他を勝手に既定へ昇格させない)',
    listSignatures().length === 1 && getDefaultSignature() === null
    && db.prepare('SELECT is_active FROM inquiry_signatures WHERE id = ?').get(s2.id).is_active === 0);
  check('削除した名前は作り直せる', createSignature({ name: '発注用の署名', body: '再作成' }).id > 0);

  // 本文への差し込み
  check('署名は本文の後ろに空行1つで続く',
    composeBodyWithSignature('お世話になっております。', '署名です') === 'お世話になっております。\n\n署名です');
  check('本文が空でも署名だけ入る (入力位置は上に空ける)',
    composeBodyWithSignature('', '署名です') === '\n\n署名です');
  check('署名なしなら本文そのまま', composeBodyWithSignature('本文だけ', '') === '本文だけ');
}

// ═══ 2. 宛先・件名の検証 ═══
console.log('2. 入力の検証');
{
  check('宛先はドメインだけ小文字化する (ローカル部は原文のまま)',
    normalizeRecipient('  Taro@Example.COM ') === 'Taro@example.com');
  throws('宛先が空はエラー', () => normalizeRecipient(''), '宛先メールアドレスを入力');
  throws('宛先の改行 (ヘッダインジェクション) は拒否', () => normalizeRecipient('a@b.jp\nBcc: x@y.jp'), '1件だけ');
  throws('宛先のカンマ (複数指定) は拒否', () => normalizeRecipient('a@b.jp,c@d.jp'), '1件だけ');
  throws('形式不正は拒否', () => normalizeRecipient('not-an-address'), '形式が正しくありません');
  throws('no-reply 宛は作成時点で断る (2026-08-26 事故の再発防止)',
    () => normalizeRecipient('no-reply@mercari-shops.com'), 'メルカリShops');
  throws('mailer-daemon 宛も断る', () => normalizeRecipient('mailer-daemon@googlemail.com'), '返信を受け付けない');
  throws('山括弧・引用符などヘッダを壊す文字は拒否', () => normalizeRecipient('<a@b.jp>'), '使えない文字');
  throws('ドメインにドットが無いものは拒否', () => normalizeRecipient('a@localhost'), '形式が正しくありません');
  throws('ローカル部の先頭ドットは拒否', () => normalizeRecipient('.abc@example.com'), '形式が正しくありません');
  throws('ローカル部の末尾ドットは拒否', () => normalizeRecipient('abc.@example.com'), '形式が正しくありません');
  throws('連続ドットは拒否', () => normalizeRecipient('a..b@example.com'), '形式が正しくありません');
  throws('ハイフンで始まるドメインは拒否', () => normalizeRecipient('a@-example.com'), '形式が正しくありません');
  throws('ハイフンで終わるドメインラベルは拒否', () => normalizeRecipient('a@example-.com'), '形式が正しくありません');
  throws('数字だけのTLDは拒否', () => normalizeRecipient('a@example.123'), '形式が正しくありません');
  throws('64文字を超えるローカル部は拒否',
    () => normalizeRecipient('a'.repeat(65) + '@example.com'), '形式が正しくありません');
  check('サブドメイン・記号入りの実在形は通る',
    normalizeRecipient('sales.dept+po@mail.example.co.jp') === 'sales.dept+po@mail.example.co.jp');
  throws('宛先の名前が長すぎるとエラー (黙って切らない)',
    () => normalizeCustomerName('あ'.repeat(101)), '長すぎ');
  throws('楽天マスクアドレス宛の新規メールは断る (返信から送ってもらう)',
    () => normalizeRecipient('abc@pc.fw.rakuten.ne.jp'), '楽天のマスクアドレス');
  check('本文はそのまま通る (整形しない)', validateBody(BODY_SAMPLE) === BODY_SAMPLE);
  throws('本文が空はエラー', () => validateBody('   '), '本文が空');
  throws('本文が長すぎるとエラー', () => validateBody('あ'.repeat(BODY_MAX + 1)), '長すぎ');

  check('件名は前後の空白と連続空白を詰める', normalizeSubject('  発注の  件  ') === '発注の 件');
  throws('件名が空はエラー', () => normalizeSubject('   '), '件名を入力');
  throws('件名が長すぎるとエラー', () => normalizeSubject('あ'.repeat(201)), '長すぎ');
  check('宛先の名前は任意 (空なら null)', normalizeCustomerName('  ') === null
    && normalizeCustomerName(' 山田 太郎  様 ') === '山田 太郎 様');

  check('メール店舗が引ける', listMailShops().length === 1 && resolveMailShop().id === shopEmail);
  throws('存在しない送信元はエラー', () => resolveMailShop(9999), '見つかりません');
}

// ═══ 3. 下書き (器) と確定 ═══
console.log('3. 下書きと確定');
let sentInquiryId = null;
{
  const d = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  const row = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(d.id);
  check('下書きは compose: の仮IDで作られる', isComposeThread(row.external_inquiry_id)
    && row.external_inquiry_id.startsWith(COMPOSE_PREFIX));
  check('下書きは一覧に出ない (is_archived=1)', row.is_archived === 1
    && listInquiries({ view: 'all' }).rows.every(r => r.id !== d.id));
  check('getComposeDraft は下書きだけ返す', getComposeDraft(d.id)?.id === d.id);

  const f = finalizeComposeDraft({ draftId: d.id, to: 'Kokyaku@Example.jp', subject: '  ご連絡  ',
    customerName: '顧客 太郎', actor: 'tester' });
  check('確定で宛先・件名が入り一覧に出る', f.inquiry.customer_identifier === 'Kokyaku@example.jp'
    && f.inquiry.subject === 'ご連絡' && f.inquiry.is_archived === 0 && f.created === false);
  check('確定は操作ログに残る',
    db.prepare("SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'compose_created'").get(d.id).c === 1);

  throws('確定済みの下書きをもう一度送ろうとすると案内が出る',
    () => finalizeComposeDraft({ draftId: d.id, to: 'a@b.jp', subject: 'x', actor: 'tester' }), '既に送信手続き済み');
  throws('存在しない下書きはエラー',
    () => finalizeComposeDraft({ draftId: 999999, to: 'a@b.jp', subject: 'x', actor: 'tester' }), '下書きが見つかりません');
  throws('宛先が不正なら確定しない',
    () => finalizeComposeDraft({ to: 'bad', subject: 'x', actor: 'tester' }), '形式が正しくありません');

  // 添付なしの経路: draftId 無しでも器ごと作る
  const f2 = finalizeComposeDraft({ to: 'x@example.jp', subject: '添付なし', actor: 'tester' });
  check('draftIdなしでも確定できる (器も一緒に作る)', f2.created === true && f2.inquiry.is_archived === 0);

  // 送信ジョブが付いたものは戻さない
  const job = createReplyJob({ inquiryId: f.inquiry.id, channelType: 'email', bodyText: '本文です',
    createdBy: 'tester', clientOperationId: 'op-compose-1', baseConversationRev: f.inquiry.conversation_rev });
  check('確定後は返信と同じ経路で送信ジョブが作れる', job.created === true && !job.conflict);
  sentInquiryId = f.inquiry.id;
}

// ═══ 4. 下書きの掃除 ═══
console.log('4. 下書きの掃除');
{
  const fresh = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  const old = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  const oldWithJob = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  const oldIso = toUtcIso(Date.now() - 30 * 3600e3);
  db.prepare('UPDATE inquiries SET created_at = ? WHERE id IN (?, ?)').run(oldIso, old.id, oldWithJob.id);
  // 添付を1つ持たせて、実体ごと消えることを確認する
  db.prepare(`INSERT INTO outbox_attachments (inquiry_id, file_name, content_type, file_size, body, uploaded_by)
    VALUES (?,?,?,?,?,?)`).run(old.id, 'a.pdf', 'application/pdf', 3, Buffer.from('abc'), 'tester');
  // 送信ジョブ付きの古い下書き (= 送信直前に落ちた等) は消さない
  db.prepare(`INSERT INTO outbox_replies (inquiry_id, channel_type, client_operation_id, body_text, created_by, base_conversation_rev)
    VALUES (?, 'email', 'op-old-draft', '本文', 'tester', 0)`).run(oldWithJob.id);

  // 「確定まで進んだのに送信ジョブが無い」= 異常終了の残骸も回収対象 (Codexレビュー High-1 の多層防御)
  const orphan = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  db.prepare("UPDATE inquiries SET is_archived = 0, customer_identifier = 'zangai@example.jp', subject = '残骸', created_at = ? WHERE id = ?")
    .run(oldIso, orphan.id);

  const n = pruneStaleComposeDrafts();
  check('24時間を過ぎた未送信の下書きは掃除される', n === 2
    && !db.prepare('SELECT id FROM inquiries WHERE id = ?').get(old.id));
  check('送信ジョブが無いまま表に出た残骸も回収する',
    !db.prepare('SELECT id FROM inquiries WHERE id = ?').get(orphan.id));
  check('掃除で添付の実体も消える',
    db.prepare('SELECT COUNT(*) c FROM outbox_attachments WHERE inquiry_id = ?').get(old.id).c === 0);
  check('作りたての下書きは残る', !!db.prepare('SELECT id FROM inquiries WHERE id = ?').get(fresh.id));
  check('送信ジョブが付いている下書きは消さない', !!db.prepare('SELECT id FROM inquiries WHERE id = ?').get(oldWithJob.id));
}

// ═══ 5. スレッドIDの差し替え ═══
console.log('5. スレッドIDの差し替え');
{
  const d = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  check('送信後に実スレッドIDへ差し替わる', attachExternalThread(db, d.id, 'thread-real-1').attached === true
    && db.prepare('SELECT external_inquiry_id FROM inquiries WHERE id = ?').get(d.id).external_inquiry_id === 'thread-real-1');
  check('差し替え済み (compose: でない) はもう触らない',
    attachExternalThread(db, d.id, 'thread-real-2').attached === false);

  const d2 = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  const conflict = attachExternalThread(db, d2.id, 'thread-real-1');
  check('同じスレッドIDのチケットが既にあるときは差し替えない (UNIQUE衝突で送信記録ごと失敗させない)',
    conflict.attached === false && conflict.conflictInquiryId === d.id
    && isComposeThread(db.prepare('SELECT external_inquiry_id FROM inquiries WHERE id = ?').get(d2.id).external_inquiry_id));
  check('衝突は⚠️要確認 + 操作ログで人が気付ける',
    db.prepare('SELECT needs_attention FROM inquiries WHERE id = ?').get(d2.id).needs_attention === 1
    && db.prepare("SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'compose_thread_conflict'").get(d2.id).c === 1);
  check('空のスレッドIDは無視', attachExternalThread(db, d2.id, '').attached === false);

  // 同期が先に走った場合の合流 (送信済みメッセージIDで compose チケットを見つけて昇格させる)
  const d3 = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, sent_at, received_at)
    VALUES (?, 'gmail-msg-77', 'shop', '送信済み本文', 0, ?, ?)`).run(d3.id, toUtcIso(Date.now()), toUtcIso(Date.now()));
  const adopted = adoptComposeInquiryByMessageIds(db, { channelType: 'email', shopId: shopEmail,
    externalInquiryId: 'thread-from-sync', messageIds: ['gmail-msg-77', 'gmail-msg-78'] });
  check('同期が先行しても送信済みメッセージIDで同じチケットに合流する',
    adopted?.id === d3.id && adopted.external_inquiry_id === 'thread-from-sync');
  check('関係ないメッセージIDでは合流しない',
    adoptComposeInquiryByMessageIds(db, { channelType: 'email', shopId: shopEmail,
      externalInquiryId: 'thread-x', messageIds: ['no-such-msg'] }) === null);

  // 既に実スレッドのチケットが作られてしまった後 = 会話が割れている。自動マージはせず印を付ける
  const d4 = createComposeDraft({ shopId: shopEmail, createdBy: 'tester' });
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, sent_at, received_at)
    VALUES (?, 'gmail-msg-88', 'shop', '送信済み本文', 0, ?, ?)`).run(d4.id, toUtcIso(Date.now()), toUtcIso(Date.now()));
  const realId = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at)
    VALUES ('email', ?, 'thread-split-1', '同期が作った方', ?)`).run(shopEmail, toUtcIso(Date.now())).lastInsertRowid;
  const split = flagComposeThreadSplit(db, { channelType: 'email', shopId: shopEmail,
    inquiryId: realId, messageIds: ['gmail-msg-88'] });
  check('分裂 (同じメールが2つの問い合わせ) を検知して両方に⚠️要確認を立てる',
    split === d4.id
    && db.prepare('SELECT needs_attention FROM inquiries WHERE id = ?').get(d4.id).needs_attention === 1
    && db.prepare('SELECT needs_attention FROM inquiries WHERE id = ?').get(realId).needs_attention === 1);
  const splitLogs = () => db.prepare("SELECT COUNT(*) c FROM inquiry_activity_logs WHERE action_type = 'compose_thread_split'").get().c;
  const logsAfterFirst = splitLogs();
  flagComposeThreadSplit(db, { channelType: 'email', shopId: shopEmail, inquiryId: realId, messageIds: ['gmail-msg-88'] });
  check('同期のたびにログを増やさない (印は1回だけ)', splitLogs() === logsAfterFirst);
  check('割れていなければ何もしない',
    flagComposeThreadSplit(db, { channelType: 'email', shopId: shopEmail, inquiryId: realId, messageIds: ['no-such'] }) === null);
}

// ═══ 6. 送信ワーカー: 新規メールの送信 ═══
console.log('6. 送信ワーカー');
{
  // ここまでのセクションで作った pending は対象外にする (このパスで拾うのは新規メール1件だけにする)
  db.prepare("UPDATE outbox_replies SET status = 'cancelled' WHERE status = 'pending'").run();
  const f = finalizeComposeDraft({ to: 'newmail@example.jp', subject: '新規のご連絡', actor: 'tester' });
  createReplyJob({ inquiryId: f.inquiry.id, channelType: 'email', bodyText: '新規メールの本文',
    createdBy: 'tester', clientOperationId: 'op-newmail-1', baseConversationRev: f.inquiry.conversation_rev });
  const adapter = { sendReply: async () => ({ externalReplyId: 'msg-1', externalThreadId: 'gmail-thread-1' }) };
  const r = await runOutboxPass({ email: adapter }, { executor: 'server' });
  check('送信ジョブが sent になる', r.results.length === 1 && r.results[0].outcome === 'sent');
  const after = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(f.inquiry.id);
  check('仮IDが実スレッドIDに差し替わる (以後の返信が同じチケットに載る)',
    after.external_inquiry_id === 'gmail-thread-1');
  check('送信内容が会話に記録される (状態は返信待ちへ)',
    db.prepare("SELECT COUNT(*) c FROM inquiry_messages WHERE inquiry_id = ? AND is_incoming = 0").get(f.inquiry.id).c === 1
    && after.internal_status === 'waiting_reply');

  // 同期が先に同じスレッドを取り込んでいた場合: 送信自体は成功。会話が割れている警告を残す
  db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at)
    VALUES ('email', ?, 'gmail-thread-2', '同期が先に作った方', ?)`).run(shopEmail, toUtcIso(Date.now()));
  const f2 = finalizeComposeDraft({ to: 'conflict@example.jp', subject: '衝突する新規メール', actor: 'tester' });
  createReplyJob({ inquiryId: f2.inquiry.id, channelType: 'email', bodyText: '本文',
    createdBy: 'tester', clientOperationId: 'op-newmail-2', baseConversationRev: f2.inquiry.conversation_rev });
  const r2 = await runOutboxPass({ email: { sendReply: async () => ({ externalReplyId: 'msg-2', externalThreadId: 'gmail-thread-2' }) } },
    { executor: 'server' });
  const job2 = db.prepare('SELECT * FROM outbox_replies WHERE client_operation_id = ?').get('op-newmail-2');
  check('スレッドID衝突でも送信は sent (顧客には届いている)',
    r2.results[0].outcome === 'sent' && job2.status === 'sent');
  check('衝突はジョブ履歴の警告 + ⚠️要確認で人に渡す (両方のチケットに印)',
    String(job2.error_message || '').includes('会話が2つに分かれています')
    && db.prepare('SELECT needs_attention FROM inquiries WHERE id = ?').get(f2.inquiry.id).needs_attention === 1
    && db.prepare("SELECT needs_attention FROM inquiries WHERE external_inquiry_id = 'gmail-thread-2'").get().needs_attention === 1);

  // Gmail が threadId を返さなかった場合 (差し替えようがないので仮IDのまま・送信は成功)
  const f3 = finalizeComposeDraft({ to: 'nothread@example.jp', subject: 'スレッドIDなし', actor: 'tester' });
  createReplyJob({ inquiryId: f3.inquiry.id, channelType: 'email', bodyText: '本文',
    createdBy: 'tester', clientOperationId: 'op-newmail-3', baseConversationRev: f3.inquiry.conversation_rev });
  await runOutboxPass({ email: { sendReply: async () => ({ externalReplyId: 'msg-3' }) } }, { executor: 'server' });
  const job3 = db.prepare('SELECT * FROM outbox_replies WHERE client_operation_id = ?').get('op-newmail-3');
  check('スレッドIDが返らなくても送信は成功扱い (顧客には届いている)', job3.status === 'sent'
    && isComposeThread(db.prepare('SELECT external_inquiry_id FROM inquiries WHERE id = ?').get(f3.inquiry.id).external_inquiry_id));
  check('スレッドID不明は黙って通さない (⚠️要確認 + ジョブ履歴の警告)',
    String(job3.error_message || '').includes('メールスレッドの紐付けに失敗')
    && db.prepare('SELECT needs_attention FROM inquiries WHERE id = ?').get(f3.inquiry.id).needs_attention === 1);
}

// ═══ 7. Gmailアダプター: 新規メールの組み立て ═══
console.log('7. Gmail送信の組み立て');
{
  const calls = [];
  const fetchImpl = async (url, opt) => {
    if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
    calls.push({ url: String(url), body: opt?.body ? JSON.parse(opt.body) : null });
    if (String(url).includes('messages/send')) {
      return { ok: true, status: 200, json: async () => ({ id: 'sent-1', threadId: 'gmail-thread-9' }) };
    }
    // From 実測検証の読み戻し
    return { ok: true, status: 200, json: async () => ({ payload: { headers: [{ name: 'From', value: 'info@b-faith.biz' }] } }) };
  };
  const gmail = createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r',
    sendMode: 'live', fetchImpl, sleepMs: 0 });
  const composeInq = { external_inquiry_id: `${COMPOSE_PREFIX}abc`, customer_identifier: 'to@example.jp', subject: 'ご案内' };
  const out = await gmail.sendReply({ inquiry: composeInq, bodyText: '本文' });
  const send = calls.find(c => c.url.includes('messages/send'));
  const mime = Buffer.from(String(send.body.raw).replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('新規メールは threadId を渡さない (Gmailが新しいスレッドを起こす)', !('threadId' in send.body));
  check('新規メールの件名に Re: を付けない', /^Subject: .*$/m.test(mime) && !/Subject:\s*Re:/i.test(mime));
  check('新規メールは In-Reply-To / References を付けない', !/In-Reply-To:/i.test(mime) && !/References:/i.test(mime));
  check('スレッドのメタ取得 (threads/…) を呼ばない', !calls.some(c => c.url.includes('threads/')));
  check('宛先は画面で入力したアドレス', /^To: to@example\.jp$/m.test(mime));

  // ローカル部の大小はそのまま・ドメインだけ小文字化して送る (2026-08-27 Codexレビュー3巡目)
  const callsCase = [];
  const gmailCase = createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r',
    sendMode: 'live', sleepMs: 0,
    fetchImpl: async (url, opt) => {
      if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
      callsCase.push({ url: String(url), body: opt?.body ? JSON.parse(opt.body) : null });
      if (String(url).includes('messages/send')) return { ok: true, status: 200, json: async () => ({ id: 'sent-case', threadId: 'th-case' }) };
      return { ok: true, status: 200, json: async () => ({ payload: { headers: [{ name: 'From', value: 'info@b-faith.biz' }] } }) };
    } });
  await gmailCase.sendReply({ inquiry: { external_inquiry_id: `${COMPOSE_PREFIX}case`,
    customer_identifier: 'Taro.Yamada@Example.JP', subject: '大文字の宛先' }, bodyText: '本文' });
  const mimeCase = Buffer.from(String(callsCase.find(c => c.url.includes('messages/send')).body.raw)
    .replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('宛先のローカル部は原文のまま・ドメインだけ小文字で送る',
    /^To: Taro\.Yamada@example\.jp$/m.test(mimeCase), (mimeCase.match(/^To: .*$/m) || [])[0]);
  check('実スレッドIDを返す (outboxが仮IDと差し替える)', out.externalThreadId === 'gmail-thread-9' && out.externalReplyId === 'sent-1');

  // 送信ワーカー側の確認 (画面を通らない経路・スレッドから宛先を復元した場合の保険)
  await rejects('送信直前にも宛先ドメインを確認し、存在しなければ未送信で止める',
    () => gmail.sendReply({ inquiry: { external_inquiry_id: `${COMPOSE_PREFIX}typo`,
      customer_identifier: 'taro@gmial.test', subject: '打ち間違い' }, bodyText: '本文' }),
    '打ち間違い');

  await rejects('宛先が空の新規メールは未送信で止める (スレッドから復元しない)',
    () => gmail.sendReply({ inquiry: { external_inquiry_id: `${COMPOSE_PREFIX}x`, customer_identifier: '', subject: 'x' }, bodyText: 'b' }),
    '新規メールの宛先が不正');
  const fetchNoThread = async (url, opt) => {
    if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
    if (String(url).includes('messages/send')) return { ok: true, status: 200, json: async () => ({ id: 'sent-nothread' }) };
    return { ok: true, status: 200, json: async () => ({ payload: { headers: [{ name: 'From', value: 'info@b-faith.biz' }] } }) };
  };
  const gmailNT = createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r',
    sendMode: 'live', fetchImpl: fetchNoThread, sleepMs: 0 });
  const outNT = await gmailNT.sendReply({ inquiry: composeInq, bodyText: '本文' });
  check('GmailレスポンスにthreadIdが無ければ externalThreadId を返さない',
    outNT.externalReplyId === 'sent-nothread' && outNT.externalThreadId === undefined);

  await rejects('件名が無い新規メールは未送信で止める',
    () => gmail.sendReply({ inquiry: { external_inquiry_id: `${COMPOSE_PREFIX}x`, customer_identifier: 'a@b.jp', subject: '' }, bodyText: 'b' }),
    '件名がありません');

  // 返信 (既存スレッド) の挙動が変わっていないこと
  const calls2 = [];
  const fetchImpl2 = async (url, opt) => {
    if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
    calls2.push({ url: String(url), body: opt?.body ? JSON.parse(opt.body) : null });
    if (String(url).includes('messages/send')) return { ok: true, status: 200, json: async () => ({ id: 'sent-2', threadId: 'th-existing' }) };
    if (String(url).includes('threads/')) {
      return { ok: true, status: 200, json: async () => ({ messages: [{ payload: { headers: [
        { name: 'Message-ID', value: '<prev@mail>' }, { name: 'From', value: 'kokyaku@example.jp' }] } }] }) };
    }
    return { ok: true, status: 200, json: async () => ({ payload: { headers: [{ name: 'From', value: 'info@b-faith.biz' }] } }) };
  };
  const gmail2 = createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r',
    sendMode: 'live', fetchImpl: fetchImpl2, sleepMs: 0 });
  const out2 = await gmail2.sendReply({ inquiry: { external_inquiry_id: 'th-existing', customer_identifier: 'kokyaku@example.jp', subject: '問い合わせ' }, bodyText: '返信' });
  const send2 = calls2.find(c => c.url.includes('messages/send'));
  const mime2 = Buffer.from(String(send2.body.raw).replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  const subj2 = (mime2.match(/^Subject: (.*)$/m) || [])[1] || '';
  const decoded2 = subj2.replace(/=\?UTF-8\?B\?([^?]*)\?=/g, (_, b) => Buffer.from(b, 'base64').toString('utf8'));
  check('返信は従来どおり件名に Re: が付く (新規メールとの違い)', decoded2.startsWith('Re: 問い合わせ'), decoded2);
  check('返信は既存スレッドに載せる', send2.body.threadId === 'th-existing' && /In-Reply-To: <prev@mail>/.test(mime2));
  check('返信では externalThreadId を返さない (差し替え不要)', out2.externalThreadId === undefined);
}

// ═══ 8. 画面 (1段目・2段目) ═══
console.log('8. 画面');
{
  process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
  const express = (await import('express')).default;
  const vm = await import('vm');
  const routerModule = await import('./router.js');
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;
  const jp = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) })
    .then(r => r.json().then(j => ({ status: r.status, j })));
  const scriptOf = html => {
    const m = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
    return m.length ? m[m.length - 1][1] : '';
  };

  const list = await (await fetch(`${base}/?view=inbox`)).text();
  check('サイドバー最上部に「✉️ メール作成」ボタンが出る',
    list.includes('class="nav-compose"') && list.includes('✉️ メール作成') && list.includes('href="/apps/inquiry-hub/compose"'));

  const step1 = await (await fetch(`${base}/compose`)).text();
  check('1段目: テンプレート・To/From設定・署名を選ぶ画面',
    step1.includes('テンプレート、To/From設定、署名を選択してください')
    && step1.includes('id="tplSel"') && step1.includes('id="shopSel"') && step1.includes('id="sigSel"'));
  check('1段目: To/From設定に送信元アドレスが出る', step1.includes('info@b-faith.biz'));
  check('1段目: 登録済みの署名が並ぶ', step1.includes('雑貨イズム署名'));
  check('1段目: 「次へ」で2段目へ進む', step1.includes('id="nextBtn"') && step1.includes('/apps/inquiry-hub/compose/new'));
  let e1 = null; try { new vm.Script(scriptOf(step1)); } catch (e) { e1 = e; }
  check('1段目のクライアントJSが構文OK', e1 === null, String(e1));

  // テンプレート + 署名を選んで2段目へ
  const tplId = db.prepare(`INSERT INTO reply_templates (category, template_name, template_body, subject)
    VALUES ('発注', '発注書送付', '発注書をお送りします。', '【発注】お世話になっております')`).run().lastInsertRowid;
  const sigId = listSignatures()[0].id;
  const step2 = await (await fetch(`${base}/compose/new?tpl=${tplId}&sig=${sigId}`)).text();
  check('2段目: 宛先・件名・本文の入力欄',
    step2.includes('id="cTo"') && step2.includes('id="cSubject"') && step2.includes('id="cBody"'));
  check('2段目: テンプレートの件名と本文が入った状態で開く',
    step2.includes('【発注】お世話になっております') && step2.includes('発注書をお送りします。'));
  check('2段目: 選んだ署名が本文末尾に展開される', step2.includes('雑貨イズム（B-Faith株式会社）'));
  check('2段目: 添付ボタンがある', step2.includes('id="attBtn"') && step2.includes('📎 ファイルを添付'));
  check('2段目: 「送信」と「送信して完了」の2ボタン', step2.includes('id="sendBtn"') && step2.includes('id="sendDoneBtn"'));
  let e2 = null; try { new vm.Script(scriptOf(step2)); } catch (e) { e2 = e; }
  check('2段目のクライアントJSが構文OK', e2 === null, String(e2));

  // 送信API
  const draft = await jp('/api/compose/draft', { shopId: shopEmail });
  check('下書きAPI: 器を1件作る', draft.status === 200 && draft.j.id > 0 && !!getComposeDraft(draft.j.id));

  const bad = await jp('/api/compose/send', { draftId: draft.j.id, to: 'no-reply@mercari-shops.com',
    subject: 'x', body: '本文', clientOperationId: 'op-ui-blocked' });
  check('送信API: no-reply 宛は400で止まる', bad.status === 400 && String(bad.j.error).includes('メルカリShops'));
  check('送信API: 失敗した下書きは一覧に出ないまま (器のまま残る)',
    db.prepare('SELECT is_archived FROM inquiries WHERE id = ?').get(draft.j.id).is_archived === 1);

  const noSubj = await jp('/api/compose/send', { draftId: draft.j.id, to: 'a@example.jp',
    subject: '  ', body: '本文', clientOperationId: 'op-ui-nosubj' });
  check('送信API: 件名なしは400', noSubj.status === 400 && String(noSubj.j.error).includes('件名'));
  check('送信API: 失敗しても宛先だけ入った問い合わせは増えない (トランザクションで巻き戻す)',
    db.prepare("SELECT COUNT(*) c FROM inquiries WHERE customer_identifier = 'a@example.jp' AND is_archived = 0").get().c === 0);

  const tooLong = await jp('/api/compose/send', { to: 'a@example.jp', subject: 'x',
    body: 'あ'.repeat(BODY_MAX + 1), clientOperationId: 'op-ui-toolong' });
  check('送信API: 本文が長すぎると400', tooLong.status === 400 && String(tooLong.j.error).includes('長すぎ'));

  const ok = await jp('/api/compose/send', { draftId: draft.j.id, to: 'torihikisaki@example.jp',
    subject: '発注のご連絡', customerName: '○○商事', body: '本文です', clientOperationId: 'op-ui-1' });
  check('送信API: 送信ジョブができて問い合わせIDが返る',
    ok.status === 200 && ok.j.inquiryId === draft.j.id && ok.j.outboxId > 0 && ok.j.duplicate === false);
  const made = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(draft.j.id);
  check('送信API: 一覧に出る状態になる (宛先・件名・宛名が入る)', made.is_archived === 0
    && made.customer_identifier === 'torihikisaki@example.jp' && made.subject === '発注のご連絡'
    && made.customer_name === '○○商事');
  check('送信API: 送信ジョブは pending (実送信はワーカー)',
    db.prepare("SELECT status, channel_type FROM outbox_replies WHERE id = ?").get(ok.j.outboxId).status === 'pending');

  const dup = await jp('/api/compose/send', { draftId: draft.j.id, to: 'torihikisaki@example.jp',
    subject: '発注のご連絡', body: '本文です', clientOperationId: 'op-ui-1' });
  check('送信API: 同じ操作IDの再POSTは既存ジョブを返す (冪等)',
    dup.status === 200 && dup.j.duplicate === true && dup.j.outboxId === ok.j.outboxId && dup.j.inquiryId === ok.j.inquiryId);
  check('送信API: 二重送信ガードで空スレッドを増やさない',
    db.prepare("SELECT COUNT(*) c FROM inquiries WHERE customer_identifier = 'torihikisaki@example.jp'").get().c === 1);

  // 同じ操作IDの同時POST (Codexレビュー 2巡目 High): 二重送信も空の問い合わせも作らない
  {
    const dr = await jp('/api/compose/draft', { shopId: shopEmail });
    const before = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
    const payload = { draftId: dr.j.id, to: 'douji@example.jp', subject: '同時POST', body: '本文',
      clientOperationId: 'op-race-1' };
    const [a1, a2] = await Promise.all([jp('/api/compose/send', payload), jp('/api/compose/send', payload)]);
    check('同時POST: 送信ジョブは1つだけ',
      db.prepare("SELECT COUNT(*) c FROM outbox_replies WHERE client_operation_id = 'op-race-1'").get().c === 1);
    check('同時POST: どちらの応答も同じ問い合わせ/ジョブを指す',
      a1.status === 200 && a2.status === 200 && a1.j.outboxId === a2.j.outboxId && a1.j.inquiryId === a2.j.inquiryId);
    check('同時POST: 問い合わせは増えない (器を使い回す)',
      db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before);

    const before2 = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
    const payload2 = { to: 'douji2@example.jp', subject: '同時POST (下書きなし)', body: '本文',
      clientOperationId: 'op-race-2' };
    const [b1, b2] = await Promise.all([jp('/api/compose/send', payload2), jp('/api/compose/send', payload2)]);
    check('同時POST (下書きなし): 問い合わせは1件しか増えない',
      db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before2 + 1
      && b1.status === 200 && b2.status === 200 && b1.j.outboxId === b2.j.outboxId);
  }

  // 宛先ドメインの事前確認 (2026-08-27): 打ち間違いは送る前に止める
  {
    const before = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
    const typo = await jp('/api/compose/send', { to: 'kokyaku@gmial.test', subject: '打ち間違い',
      body: '本文', clientOperationId: 'op-typo-1' });
    check('存在しないドメイン宛は送信前に400で止まる',
      typo.status === 400 && String(typo.j.error).includes('打ち間違い'));
    check('止めたときは問い合わせも送信ジョブも作らない',
      db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before
      && db.prepare("SELECT COUNT(*) c FROM outbox_replies WHERE client_operation_id = 'op-typo-1'").get().c === 0);
  }

  // 再POSTは宛先ドメインの判定より先に既存ジョブを返す (Codexレビュー: 送信済みなのに失敗表示にしない)
  {
    const dr = await jp('/api/compose/draft', { shopId: shopEmail });
    const first = await jp('/api/compose/send', { draftId: dr.j.id, to: 'saki@ok-domain.test',
      subject: 'DNS後戻り確認', body: '本文', clientOperationId: 'op-dns-retry' });
    check('1回目は普通に送信ジョブができる', first.status === 200 && first.j.duplicate === false);
    // 送信後にそのドメインが引けなくなった状況を作る (キャッシュを消してNXDOMAINに)
    clearMxCache();
    setResolverForTest({
      resolveMx: async () => { throw Object.assign(new Error('stub ENOTFOUND'), { code: 'ENOTFOUND' }); },
      resolve4: async () => { throw Object.assign(new Error('stub ENOTFOUND'), { code: 'ENOTFOUND' }); },
      resolve6: async () => { throw Object.assign(new Error('stub ENOTFOUND'), { code: 'ENOTFOUND' }); },
    });
    const again = await jp('/api/compose/send', { draftId: dr.j.id, to: 'saki@ok-domain.test',
      subject: 'DNS後戻り確認', body: '本文', clientOperationId: 'op-dns-retry' });
    check('同じ操作IDの再POSTはDNSが否定でも既存ジョブを返す (失敗表示にしない)',
      again.status === 200 && again.j.duplicate === true && again.j.outboxId === first.j.outboxId);
    restoreStubResolver();
    clearMxCache();
  }

  // 送信ジョブ作成に失敗したら問い合わせごと巻き戻る (Codexレビュー 1巡目 High-1)
  {
    const before = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
    const bad2 = await jp('/api/compose/send', { to: 'rollback@example.jp', subject: '巻き戻し確認',
      body: '本文', attachmentIds: [999999], clientOperationId: 'op-rollback-1' });
    check('添付が見つからない等でジョブが作れなければ問い合わせごと巻き戻る', bad2.status === 400
      && db.prepare('SELECT COUNT(*) c FROM inquiries').get().c === before
      && db.prepare("SELECT COUNT(*) c FROM inquiries WHERE customer_identifier = 'rollback@example.jp'").get().c === 0);
  }

  // 署名管理画面
  const sigPage = await (await fetch(`${base}/signatures`)).text();
  check('署名の管理画面が出る', sigPage.includes('id="createBtn"') && sigPage.includes('雑貨イズム署名'));
  let e3 = null; try { new vm.Script(scriptOf(sigPage)); } catch (e) { e3 = e; }
  check('署名画面のクライアントJSが構文OK', e3 === null, String(e3));
  const newSig = await jp('/api/signatures', { name: 'API作成の署名', body: '本文\n2行目' });
  check('署名API: 作成', newSig.status === 200 && listSignatures().some(s => s.name === 'API作成の署名'));
  const dupSig = await jp('/api/signatures', { name: 'API作成の署名', body: 'x' });
  check('署名API: 同名は400', dupSig.status === 400);
  const updSig = await jp(`/api/signatures/${newSig.j.id}`, { isDefault: true });
  check('署名API: 既定に切替', updSig.status === 200 && getDefaultSignature().id === newSig.j.id);
  const delSig = await jp(`/api/signatures/${newSig.j.id}/delete`, {});
  check('署名API: 削除', delSig.status === 200 && !listSignatures().some(s => s.id === newSig.j.id));

  // 機能フラグOFFのとき
  delete process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
  const off = await (await fetch(`${base}/compose`)).text();
  check('送信機能が無効なら作成画面は案内だけ (フォームを出さない)',
    off.includes('INQUIRY_HUB_REPLY_EDITOR_ENABLED') && !off.includes('id="nextBtn"'));
  const offApi = await jp('/api/compose/send', { to: 'a@example.jp', subject: 'x', body: 'y', clientOperationId: 'op-off' });
  check('送信機能が無効ならAPIも403', offApi.status === 403);

  srv.close();
}

console.log(`\n${failed === 0 ? 'OK' : 'NG'}: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed === 0 ? 0 : 1;
