// ⚙️運用管理画面 (router /admin + /api/admin/*) のHTTPスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-admin.mjs
// DATA_DIR 直下は使わず毎回ユニークな一時サブディレクトリを作る (既存ファイルは一切削除しない)
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。作業ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-admin-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-admin-'));
process.env.DATA_DIR = workDir;
// 他のsmokeと同じDATA_DIRを共有して連続実行しても誤検知しないよう、開始時点の状態を記録
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));
// 手動同期の no_transport 経路を確定させる (誤って実APIに出ないよう transport env を全て消す)
for (const k of ['RAKUTEN_SERVICE_SECRET', 'RAKUTEN_LICENSE_KEY', 'WAREHOUSE_URL', 'WAREHOUSE_SERVICE_TOKEN',
  'CF_ACCESS_CLIENT_ID', 'CF_ACCESS_CLIENT_SECRET', 'YAHOO_PROXY_URL', 'YAHOO_PROXY_SECRET']) delete process.env[k];

// db.js は import 時点で DATA_DIR を定数化するため、env 差し替え後に動的 import する
const { initInquiryHubDB, getDB, toUtcIso } = await import('./db.js');
const { createReplyJob } = await import('./outbox.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── fixture ───
const rkShop = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','373343')").run().lastInsertRowid;
db.prepare('INSERT INTO sync_state (shop_id, committed_until, last_error, consecutive_failures) VALUES (?,?,?,?)')
  .run(rkShop, toUtcIso(Date.now()), 'テストエラー', 3);
// 認証期限が30日以内に迫っている店舗 (期限警告の表示確認用。Codexレビュー反映)
const expShop = db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier, auth_expires_at)
  VALUES ('yahoo','Yahoo店','b-faith01', ?)`).run(toUtcIso(Date.now() + 10 * 86400000)).lastInsertRowid;
const inq = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, customer_name, received_at, conversation_rev)
  VALUES ('rakuten', ?, 'adm-1', '返金について', '顧客A', ?, 1)`).run(rkShop, toUtcIso(Date.now())).lastInsertRowid;
const errId = db.prepare("INSERT INTO sync_errors (shop_id, error_type, error_detail) VALUES (?, 'fetch_failed', '接続失敗')").run(rkShop).lastInsertRowid;
// 詳細画面の本文表示テスト用: 空行だらけの長文 (自動配信メール相当) と短い問い合わせ
const messyBody = ['自動配信のお知らせ', '', '', '', '', '96', '', '', '', '', '', '',
  ...Array.from({ length: 12 }, (_, i) => `本文${i + 1}行目です。`), '', '', '', '', '配信専用です'].join('\n');
db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, sender_name, message_body_text, is_incoming, received_at)
  VALUES (?,?,'customer','Amazon.co.jp',?,1,?)`).run(inq, 'am-long', messyBody, toUtcIso(Date.now()));
db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, sender_name, message_body_text, is_incoming, received_at)
  VALUES (?,?,'customer','顧客A',?,1,?)`).run(inq, 'am-short', '在庫はいつ入りますか?\n\nよろしくお願いします。', toUtcIso(Date.now()));

// outbox: unknown 1件 + needs_review 1件 (別問い合わせ。1問い合わせ1未決着制約のため)
const inq2 = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at, conversation_rev)
  VALUES ('rakuten', ?, 'adm-2', '在庫について', ?, 1)`).run(rkShop, toUtcIso(Date.now())).lastInsertRowid;
const jobU = createReplyJob({ inquiryId: inq, channelType: 'rakuten', bodyText: '返金します', createdBy: 'tester', clientOperationId: 'op-u', baseConversationRev: 1 });
db.prepare("UPDATE outbox_replies SET status = 'unknown' WHERE id = ?").run(jobU.id);
const jobR = createReplyJob({ inquiryId: inq2, channelType: 'rakuten', bodyText: '在庫あります', createdBy: 'tester', clientOperationId: 'op-r', baseConversationRev: 1 });
db.prepare("UPDATE outbox_replies SET status = 'needs_review' WHERE id = ?").run(jobR.id);
// pending のまま止められることの確認用 (3件目の問い合わせ)
const inq3 = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at, conversation_rev)
  VALUES ('rakuten', ?, 'adm-3', '配送について', ?, 1)`).run(rkShop, toUtcIso(Date.now())).lastInsertRowid;
const jobP = createReplyJob({ inquiryId: inq3, channelType: 'rakuten', bodyText: '明日発送します', createdBy: 'tester', clientOperationId: 'op-p', baseConversationRev: 1 });

// ─── ルーターをマウントして起動 ───
const app = express();
app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
const jpost = (p, data, headers = {}) => fetch(base + p, {
  method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(data || {}),
});

// ─── 1. 画面 ───
console.log('1. 画面表示');
{
  const r = await fetch(base + '/admin');
  const html = await r.text();
  check('GET /admin が200', r.status === 200);
  check('同期状態に店舗が出る', html.includes('楽天店'));
  check('連続失敗3回が赤表示情報付きで出る', html.includes('連続失敗 3回'));
  check('未解決同期エラーが出る', html.includes('接続失敗'));
  check('送信要対応に unknown と needs_review が出る', html.includes('❓結果不明') && html.includes('⏸️要確認'));
  check('pending (送信待ち) も要対応に出る (送信前に止められる)', html.includes('⏳送信待ち'));
  check('認証期限30日前警告が出る', html.includes('認証期限 残'));
  check('ナビに運用管理タブ', html.includes('運用管理'));
}

// ─── 1b. 詳細画面の本文表示 (空行圧縮+長文折りたたみ) ───
console.log('1b. 本文表示');
{
  const html = await (await fetch(base + `/inquiries/${inq}`)).text();
  check('連続空行が詰まる (<br>の3連以上が残らない)', !/(<br>\s*){3,}/.test(html));
  check('長文は「続きを表示」で畳まれる', html.includes('続きを表示'));
  check('短い問い合わせは畳まない (全文がそのまま出る)', html.includes('よろしくお願いします。')
    && html.split('続きを表示').length === 2);
}

// ─── 2. 手動同期API ───
console.log('2. 手動同期');
{
  const r404 = await jpost('/api/admin/sync/99999', {});
  check('存在しない店舗は404', r404.status === 404);
  const r503 = await jpost(`/api/admin/sync/${rkShop}`, {});
  check('transport env未設定は503 (実APIに出ない)', r503.status === 503);
  const r415 = await fetch(base + `/api/admin/sync/${rkShop}`, { method: 'POST' });
  check('JSON以外のPOSTは415 (CSRF二段防御)', r415.status === 415);
  const rBadOrigin = await jpost(`/api/admin/sync/${rkShop}`, {}, { Origin: 'https://evil.example.com' });
  check('別OriginのPOSTは403', rBadOrigin.status === 403);
}

// ─── 3. 送信要対応の解決 ───
console.log('3. 送信要対応');
{
  const rBad = await jpost(`/api/admin/outbox/${jobU.id}/resolve`, { resolution: 'nuke' });
  check('不正なresolutionは400', rBad.status === 400);
  const rNotUnknown = await jpost(`/api/admin/outbox/${jobR.id}/resolve`, { resolution: 'abandoned' });
  check('unknown以外のresolveは409', rNotUnknown.status === 409);

  const rOk = await jpost(`/api/admin/outbox/${jobU.id}/resolve`, { resolution: 'confirmed_not_sent' });
  check('unknownのresolveが成功', rOk.status === 200);
  const rowU = db.prepare('SELECT status, resolution, resolved_by FROM outbox_replies WHERE id = ?').get(jobU.id);
  check('confirmed_not_sent → failed + resolution記録', rowU.status === 'failed' && rowU.resolution === 'confirmed_not_sent' && rowU.resolved_by === 'portal');
  const rAgain = await jpost(`/api/admin/outbox/${jobU.id}/resolve`, { resolution: 'abandoned' });
  check('解決済みの再resolveは409 (終端状態)', rAgain.status === 409);

  const rCancel = await jpost(`/api/admin/outbox/${jobR.id}/cancel`, {});
  check('needs_reviewの取消が成功', rCancel.status === 200);
  check('cancelled になる', db.prepare('SELECT status FROM outbox_replies WHERE id = ?').get(jobR.id).status === 'cancelled');
  const rCancel2 = await jpost(`/api/admin/outbox/${jobR.id}/cancel`, {});
  check('取消済みの再取消は409', rCancel2.status === 409);

  const rCancelP = await jpost(`/api/admin/outbox/${jobP.id}/cancel`, {});
  check('pendingの取消が成功 (送信前に止める)', rCancelP.status === 200);
  check('pending → cancelled', db.prepare('SELECT status FROM outbox_replies WHERE id = ?').get(jobP.id).status === 'cancelled');
}

// ─── 3b. 認証期限の手動登録 ───
console.log('3b. 認証期限の手動登録');
{
  const rBad = await jpost(`/api/admin/shops/${rkShop}/auth-expiry`, { date: '2026/10/01' });
  check('不正な日付形式は400', rBad.status === 400);
  const rSet = await jpost(`/api/admin/shops/${rkShop}/auth-expiry`, { date: '2026-10-01' });
  check('期限の登録が成功', rSet.status === 200);
  check('JST当日終わり (UTC 15:00) で保存', db.prepare('SELECT auth_expires_at FROM shops WHERE id = ?').get(rkShop).auth_expires_at === '2026-10-01T15:00:00Z');
  const html = await (await fetch(base + '/admin')).text();
  check('画面に期限が表示される (楽天は編集フォームあり)', html.includes('期限 2026-10-02 00:00') && html.includes(`exp-${rkShop}`));
  check('Yahoo行は自動更新の注記 (編集フォームなし)', html.includes('再認可時に自動更新') && !html.includes(`exp-${expShop}`));
  const rClear = await jpost(`/api/admin/shops/${rkShop}/auth-expiry`, { date: '' });
  check('空指定で解除', rClear.status === 200 && db.prepare('SELECT auth_expires_at FROM shops WHERE id = ?').get(rkShop).auth_expires_at === null);
  const r404 = await jpost('/api/admin/shops/99999/auth-expiry', { date: '2026-10-01' });
  check('存在しない店舗は404', r404.status === 404);
}

// ─── 3c. 認証状態の自動更新 (getAuthStatus持ちアダプター) ───
console.log('3c. 認証状態の自動更新');
{
  const { refreshShopAuthStatus } = await import('./sync/cron.js');
  const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(expShop);
  const a = await refreshShopAuthStatus(shop, {
    async getAuthStatus() { return { ok: true, authExpiresAt: '2026-07-31T01:44:58.209Z' }; },
  });
  check('getAuthStatusの結果を返す', a?.ok === true);
  const row = db.prepare('SELECT auth_expires_at, authentication_status FROM shops WHERE id = ?').get(expShop);
  check('auth_expires_at が正準形式で自動更新', row.auth_expires_at === '2026-07-31T01:44:58Z');
  check('authentication_status=ok', row.authentication_status === 'ok');
  const aNg = await refreshShopAuthStatus(shop, {
    async getAuthStatus() { return { ok: false, authExpiresAt: null }; },
  });
  check('トークン無しは status=error (期限は保持)', aNg?.ok === false
    && db.prepare('SELECT authentication_status, auth_expires_at FROM shops WHERE id = ?').get(expShop).authentication_status === 'error'
    && db.prepare('SELECT auth_expires_at FROM shops WHERE id = ?').get(expShop).auth_expires_at === '2026-07-31T01:44:58Z');
  const aNone = await refreshShopAuthStatus(shop, {});
  check('getAuthStatus無しアダプターはno-op', aNone === null);
  const aThrow = await refreshShopAuthStatus(shop, {
    async getAuthStatus() { throw new Error('boom'); },
  });
  check('取得失敗はfail-soft (null)', aThrow === null);
}

// ─── 4. 同期エラーの解決 ───
console.log('4. 同期エラー解決');
{
  const r = await jpost(`/api/admin/sync-errors/${errId}/resolve`, {});
  check('解決が成功', r.status === 200);
  const row = db.prepare('SELECT resolved, resolved_at FROM sync_errors WHERE id = ?').get(errId);
  check('resolved=1 + resolved_at刻印', row.resolved === 1 && !!row.resolved_at);
  const r2 = await jpost(`/api/admin/sync-errors/${errId}/resolve`, {});
  check('解決済みの再解決は404', r2.status === 404);
  const html = await (await fetch(base + '/admin')).text();
  check('解決後は要対応リストから消える', !html.includes('❓結果不明') && !html.includes('接続失敗'));
}

// ─── 5. 返信エディタ (Dark Launch) ───
console.log('5. 返信エディタ');
{
  const { randomUUID } = await import('crypto');
  // フラグOFF (既定): UIも APIも無効
  delete process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
  const htmlOff = await (await fetch(base + `/inquiries/${inq}`)).text();
  check('フラグOFF: エディタ非表示 (プレースホルダのみ)', !htmlOff.includes('id="replyBody"') && htmlOff.includes('返信機能はまだ有効になっていません'));
  const rOff = await jpost(`/api/inquiries/${inq}/reply`, { body: 'x', clientOperationId: randomUUID(), baseConversationRev: 1 });
  check('フラグOFF: APIは403', rOff.status === 403);

  // フラグON
  process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
  const htmlOn = await (await fetch(base + `/inquiries/${inq}`)).text();
  check('フラグON: エディタ表示+ワーカー未稼働の警告', htmlOn.includes('id="replyBody"') && htmlOn.includes('送信ワーカーは停止中'));
  check('送信ジョブ履歴が表示される (failedになったジョブ)', htmlOn.includes('❌送信失敗'));

  const rEmpty = await jpost(`/api/inquiries/${inq}/reply`, { body: '  ', clientOperationId: randomUUID(), baseConversationRev: 1 });
  check('空本文は400', rEmpty.status === 400);
  const rBadOp = await jpost(`/api/inquiries/${inq}/reply`, { body: 'x', clientOperationId: 'not-a-uuid', baseConversationRev: 1 });
  check('不正な操作IDは400', rBadOp.status === 400);
  const rTooLong = await jpost(`/api/inquiries/${inq}/reply`, { body: 'あ'.repeat(10001), clientOperationId: randomUUID(), baseConversationRev: 1 });
  check('10000字超は400', rTooLong.status === 400);
  const rBadRevType = await jpost(`/api/inquiries/${inq}/reply`, { body: 'x', clientOperationId: randomUUID(), baseConversationRev: '1.5' });
  check('非整数のbaseConversationRevは400', rBadRevType.status === 400);
  // completeOnSend は boolean のみ ("false" を真と解釈して意図せず完了にしない。Codexレビュー反映)
  const rBadFlag = await jpost(`/api/inquiries/${inq}/reply`, { body: 'x', clientOperationId: randomUUID(), baseConversationRev: 1, completeOnSend: 'false' });
  check('completeOnSend が boolean 以外は400', rBadFlag.status === 400);
  const rBadRev = await jpost(`/api/inquiries/${inq}/reply`, { body: 'x', clientOperationId: randomUUID(), baseConversationRev: 99 });
  check('rev不一致 (新着競合) は409', rBadRev.status === 409);

  const opId = randomUUID();
  const rOk = await jpost(`/api/inquiries/${inq}/reply`, { body: 'ご返金いたします', clientOperationId: opId, baseConversationRev: 1 });
  const jOk = await rOk.json();
  check('返信ジョブ作成が成功', rOk.status === 200 && jOk.ok === true && !jOk.duplicate);
  const jobRow = db.prepare('SELECT status, body_text, created_by FROM outbox_replies WHERE id = ?').get(jOk.id);
  check('pending で作成される (ワーカー稼働まで送信されない)', jobRow.status === 'pending' && jobRow.body_text === 'ご返金いたします');
  const rDup = await jpost(`/api/inquiries/${inq}/reply`, { body: 'ご返金いたします', clientOperationId: opId, baseConversationRev: 1 });
  check('同一操作IDの再POSTは冪等 (duplicate)', rDup.status === 200 && (await rDup.json()).duplicate === true);
  const rSecond = await jpost(`/api/inquiries/${inq}/reply`, { body: '別の返信', clientOperationId: randomUUID(), baseConversationRev: 1 });
  check('未決着ジョブがある間は新規作成409 (1問い合わせ1送信)', rSecond.status === 409);
  const htmlActive = await (await fetch(base + `/inquiries/${inq}`)).text();
  check('未決着ジョブ中はフォームの代わりに案内表示', htmlActive.includes('未決着の送信ジョブ') && !htmlActive.includes('id="replyBody"'));
  check('運用管理の要対応に⏳送信待ちで出る', (await (await fetch(base + '/admin')).text()).includes('⏳送信待ち'));
  delete process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
}

check('DBは一時サブディレクトリのみに作成 (ベース直下に漏れない)',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
server.close();
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
