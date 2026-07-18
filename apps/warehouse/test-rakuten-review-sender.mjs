#!/usr/bin/env node
/**
 * test-rakuten-review-sender.mjs — PR-C4 スモークテスト (文面+送信エンジン、SMTP不使用=偽送信注入)
 * 実行: node apps/warehouse/test-rakuten-review-sender.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {
  buildFollowMail, buildCouponMail, buildCouponMailLowRating, TEMPLATE_BUILDERS, sampleContext,
  messageIdFor, SHOP_NAME,
} from './rakuten-review-mail-lib.js';
import {
  classifySendError, selectEligibleActions, claimActionGuarded, processReadyActions,
  recoverStaleClaims, couponUsableCheck, finalizeAttempt,
} from './rakuten-review-sender-lib.js';
import {
  ensureContactTables, loadContactKeys, encryptEmail, hmacEmail, hmacOrderKey, upsertContacts, addSuppression,
} from './rakuten-review-contacts-lib.js';
import { ensureCampaignTables } from './rakuten-review-campaign-lib.js';
import { ensureCouponRegistry, reserveMonth, markIssued } from './rakuten-coupon-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const NOW = '2026-07-18T05:00:00.000Z'; // 14:00 JST → 当月 2026-07

console.log('=== 1. メール文面 ===');
{
  const follow = buildFollowMail({ orderNumber: '373343-20260701-0000000001', shippingIso: '2026-07-08T12:00:00+09:00' });
  check('フォロー: 件名に店名', follow.subject.includes(SHOP_NAME));
  check('フォロー: 発送日と注文番号の差し込み', follow.text.includes('2026年7月8日') && follow.text.includes('373343-20260701-0000000001'));
  check('フォロー: 主目的=到着・状態確認の文', follow.text.includes('お手元に届きましたでしょうか') && follow.text.includes('破損・不具合'));
  check('フォロー: 配信停止案内あり', follow.text.includes('配信停止'));

  const coupon = buildCouponMail({ couponUrl: 'https://coupon.rakuten.co.jp/getCoupon?getkey=X--&rt=', couponEndIso: '2026-09-30T23:59:59+09:00' });
  check('クーポン: URL と期限の差し込み', coupon.text.includes('getkey=X--') && coupon.text.includes('2026年9月30日'));
  const low = buildCouponMailLowRating({ couponUrl: 'https://coupon.rakuten.co.jp/getCoupon?getkey=X--&rt=', couponEndIso: '2026-09-30T23:59:59+09:00' });
  check('低評価: 文面のみ別・クーポンURL同一 (特典差別なし)', low.subject.includes('貴重なご意見') && low.text.includes('getkey=X--'));

  // 楽天外リンク禁止 (規約): 本文中の URL が全て楽天ドメインであること
  for (const [name, mail] of [['follow', follow], ['coupon', coupon], ['coupon-low', low]]) {
    const urls = mail.text.match(/https?:\/\/[^\s)」]+/g) || [];
    check(`${name}: 本文リンクが楽天ドメインのみ (${urls.length}件)`,
      urls.length > 0 && urls.every((u) => /^https:\/\/[a-z.]*rakuten\.co\.jp\//.test(u)), urls.join(','));
    check(`${name}: undefined 混入なし`, !mail.text.includes('undefined') && !mail.subject.includes('undefined'));
  }
  check('sampleContext: 全テンプレがダミーで描画可能',
    Object.keys(TEMPLATE_BUILDERS).every((t) => TEMPLATE_BUILDERS[t](sampleContext(t)).text.length > 100));
  check('messageId: 決定的 (同一actionで同一)', messageIdFor(7, 'abcdef1234567890') === messageIdFor(7, 'abcdef1234567890')
    && messageIdFor(7, 'abcdef1234567890') !== messageIdFor(8, 'abcdef1234567890'));
  let missing = false;
  try { buildFollowMail({ orderNumber: 'X' }); } catch { missing = true; }
  check('必須コンテキスト欠落は fail-fast', missing);
}

console.log('=== 2. SMTPエラー分類 ===');
{
  check('responseCode 550 → rejected', classifySendError(Object.assign(new Error('x'), { responseCode: 550 })).kind === 'rejected');
  check('responseCode 421 → rejected (4xx=明確な拒否)', classifySendError(Object.assign(new Error('x'), { responseCode: 421 })).kind === 'rejected');
  check('コードなし (timeout等) → unknown', classifySendError(new Error('ETIMEDOUT')).kind === 'unknown');
}

// ─── フィクスチャ ───
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rrsender-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('foreign_keys = ON');
ensureRakutenReviewTables(db);
ensureContactTables(db);
ensureCampaignTables(db);
ensureCouponRegistry(db);
const keys = loadContactKeys({
  CONTACTS_ENC_KEY: crypto.randomBytes(32).toString('hex'),
  CONTACTS_HMAC_KEY: crypto.randomBytes(32).toString('hex'),
});

let seq = 0;
function makeOrder({ email, shippingIso = '2026-07-08T12:00:00+09:00', owner = 'self' } = {}) {
  const n = ++seq; // 注意: デフォルト引数内で ++seq すると email 指定時に番号が再利用される (実バグ経験済み)
  if (email === undefined) email = `user${n}@anshin.rakuten.co.jp`;
  const orderNumber = `373343-20260701-${String(n).padStart(10, '0')}`;
  upsertContacts(db, [{
    order_number: orderNumber,
    order_key_hmac: hmacOrderKey(orderNumber, keys),
    masked_email_enc: email ? encryptEmail(email, keys, orderNumber) : null,
    masked_email_hash: email ? hmacEmail(email, keys) : null,
    order_datetime: '2026-07-01T10:00:00+09:00',
    shipping_datetime: shippingIso,
    order_progress: 700,
    contact_delete_at: '2099-01-01T00:00:00.000Z',
  }]);
  if (owner) {
    db.prepare(`INSERT OR REPLACE INTO rakuten_order_campaign_ownership (order_number, owner, reason, decided_at) VALUES (?, ?, 'test', ?)`)
      .run(orderNumber, owner, NOW);
  }
  return { orderNumber, email };
}
// follow の schedule_stale ゲートを通すため scheduled は発送+10日正午JSTに一致させる
const SCHED_OK = '2026-07-18T03:00:00.000Z'; // 発送 7/8 正午 +10日
function makeAction({ orderNumber, type = 'follow', scheduled = SCHED_OK, expires = '2026-07-29T14:59:59.000Z' }) {
  const r = db.prepare(`
    INSERT INTO rakuten_campaign_actions (
      action_type, dedupe_key, order_number, status, template_version,
      scheduled_at, expires_at, ready_at, created_at, updated_at
    ) VALUES (?, ?, ?, 'ready', 'v1', ?, ?, ?, ?, ?)
  `).run(type, crypto.randomBytes(16).toString('hex'), orderNumber, scheduled, expires, NOW, NOW, NOW);
  return r.lastInsertRowid;
}
let revSeq = 0;
function addReview(orderNumber, { rating = 5, isDeleted = 0 } = {}) {
  db.prepare(`
    INSERT INTO fact_rakuten_reviews (
      review_url, review_type, item_id, rating, posted_at, date_jst, body, order_number,
      source_hash, first_seen_at, last_seen_at, is_deleted, imported_at, updated_at
    ) VALUES (?, 'item', 1, ?, '2026-07-15 10:00:00', '2026-07-15', '', ?, ?, ?, ?, ?, ?, ?)
  `).run(`https://review.rakuten.co.jp/item/1/373343_1/x${++revSeq}/`, rating, orderNumber, `h${revSeq}`, NOW, NOW, isDeleted, NOW, NOW);
}

console.log('=== 3. 送信ゲート (selectEligibleActions) ===');
{
  const ok = makeOrder({});                        // 全ゲート通過
  makeAction({ orderNumber: ok.orderNumber });
  const vendorOwned = makeOrder({ owner: 'vendor' });
  makeAction({ orderNumber: vendorOwned.orderNumber });
  const noOwnership = makeOrder({ owner: null });
  makeAction({ orderNumber: noOwnership.orderNumber });
  const reviewed = makeOrder({});
  addReview(reviewed.orderNumber, { isDeleted: 1 }); // 削除済みレビューでもフォロー抑止
  makeAction({ orderNumber: reviewed.orderNumber });
  const suppressed = makeOrder({ email: 'stop-c4@anshin.rakuten.co.jp' });
  addSuppression(db, 'stop-c4@anshin.rakuten.co.jp', 'test', keys);
  makeAction({ orderNumber: suppressed.orderNumber });
  const stale = makeOrder({});
  makeAction({ orderNumber: stale.orderNumber });
  db.prepare(`UPDATE rakuten_order_contacts SET shipping_datetime = '2026-07-10T12:00:00+09:00' WHERE order_number = ?`).run(stale.orderNumber); // 再発送=scheduled不一致
  const couponNoMonthly = makeOrder({});
  addReview(couponNoMonthly.orderNumber);
  makeAction({ orderNumber: couponNoMonthly.orderNumber, type: 'coupon' });

  const sel = selectEligibleActions(db, { nowIso: NOW, limit: 100 });
  const reasons = Object.fromEntries(sel.skipped.map((s) => [s.reason, (s.n || 0) + 1]));
  check('全ゲート通過は1件のみ', sel.eligible.length === 1 && sel.eligible[0].order_number === ok.orderNumber,
    JSON.stringify(sel.eligible.map((e) => e.order_number)));
  const skipReasons = sel.skipped.map((s) => s.reason);
  check('vendor所有と ownership 行なしは fail-closed で skip', skipReasons.filter((r) => r === 'not_self_ownership').length === 2);
  check('レビュー到来 (削除済み含む) の follow は skip', skipReasons.includes('review_exists'));
  check('suppression は skip', skipReasons.includes('suppressed'));
  check('再発送で scheduled 不一致は skip', skipReasons.includes('schedule_stale'));
  check('月次クーポン未発行の coupon は skip', skipReasons.includes('no_monthly_coupon'));
  check('monthlyCouponReady=false', sel.monthlyCouponReady === false);
  void reasons;
}

console.log('=== 4. 送信処理 (偽SMTP注入) ===');
{
  // 月次クーポンを発行済みに (2026-07)
  reserveMonth(db, { month: '2026-07', couponStart: '2026-07-01T00:00:00+09:00', couponEnd: '2026-09-30T23:59:59+09:00', nowIso: NOW });
  markIssued(db, { month: '2026-07', couponCode: 'C4TEST-CODE-1', pcGetUrl: 'https://coupon.rakuten.co.jp/getCoupon?getkey=C4--&rt=', nowIso: NOW });

  const sentMails = [];
  const okSend = async (m) => { sentMails.push(m); };

  // セクション3の couponNoMonthly action は月次クーポン発行済みになったため合流して送られる → 計2件が期待値
  const r1 = await processReadyActions(db, { keys, sendFn: okSend, nowIso: NOW, limit: 100 });
  check('成功: 2件送信 (フォロー1+月次クーポン発行で解禁されたクーポン1)', r1.sent === 2 && sentMails.length === 2, JSON.stringify(r1));
  check('宛先=復号済みマスクアドレス / From=雑貨イズム', sentMails.every((m) => m.to.endsWith('@anshin.rakuten.co.jp') && m.from.includes(SHOP_NAME)));
  const sentAction = db.prepare(`SELECT a.status, d.outcome, d.message_id FROM rakuten_campaign_actions a JOIN rakuten_campaign_delivery_attempts d ON d.action_id = a.id WHERE a.status = 'sent' LIMIT 1`).get();
  check('sent + attempt accepted + message_id 記録', sentAction?.outcome === 'accepted' && /@b-faith\.biz>$/.test(sentAction?.message_id));
  const r2 = await processReadyActions(db, { keys, sendFn: okSend, nowIso: NOW, limit: 100 });
  check('再実行で二重送信しない (at-most-once)', r2.sent === 0 && sentMails.length === 2);

  // クーポン (通常+低評価) — 月次クーポン発行済みなので送れる
  const cNormal = makeOrder({});
  addReview(cNormal.orderNumber, { rating: 5 });
  makeAction({ orderNumber: cNormal.orderNumber, type: 'coupon' });
  const cLow = makeOrder({});
  addReview(cLow.orderNumber, { rating: 1 });
  makeAction({ orderNumber: cLow.orderNumber, type: 'coupon' });
  const r3 = await processReadyActions(db, { keys, sendFn: okSend, nowIso: NOW, limit: 100 });
  check('クーポン2件送信', r3.sent === 2, JSON.stringify(r3));
  const subjects = sentMails.slice(2).map((m) => m.subject).sort();
  check('通常=お礼文面 / ★1=低評価文面 (クーポンURLは同一)', subjects.some((s) => s.includes('レビュー投稿ありがとう')) && subjects.some((s) => s.includes('貴重なご意見'))
    && sentMails.slice(2).every((m) => m.text.includes('getkey=C4--')));

  // 明確な拒否 → failed_safe
  const rej = makeOrder({});
  makeAction({ orderNumber: rej.orderNumber });
  const r4 = await processReadyActions(db, {
    keys, nowIso: NOW, limit: 100,
    sendFn: async () => { throw Object.assign(new Error('550 5.1.1 user unknown'), { responseCode: 550 }); },
  });
  check('SMTP 550 → failed_safe + attempt rejected', r4.failedSafe === 1
    && db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_actions WHERE status = 'failed_safe'`).get().n === 1
    && db.prepare(`SELECT smtp_code FROM rakuten_campaign_delivery_attempts ORDER BY id DESC LIMIT 1`).get().smtp_code === '550');

  // 結果不明 → ambiguous + 即中断 (後続 ready は手つかず)
  const amb1 = makeOrder({});
  const amb1Id = makeAction({ orderNumber: amb1.orderNumber });
  const amb2 = makeOrder({});
  const amb2Id = makeAction({ orderNumber: amb2.orderNumber });
  const r5 = await processReadyActions(db, {
    keys, nowIso: NOW, limit: 100,
    sendFn: async () => { throw new Error('ETIMEDOUT'); },
  });
  const statusOf = (id) => db.prepare(`SELECT status FROM rakuten_campaign_actions WHERE id = ?`).get(id).status;
  check('timeout → ambiguous 1件で即中断 (2件目は ready のまま手つかず)', r5.ambiguous === 1 && r5.sent === 0
    && statusOf(amb1Id) === 'ambiguous' && statusOf(amb2Id) === 'ready');
  check('ambiguous の attempt は残る (再送into UNIQUE 拒否の証跡)', db.prepare(`
    SELECT d.outcome FROM rakuten_campaign_delivery_attempts d JOIN rakuten_campaign_actions a ON a.id = d.action_id WHERE a.status = 'ambiguous'`).get().outcome === 'ambiguous');

  // claim の競り負け (attempt が既にある ready) → 絶対に送らない
  const raced = makeOrder({});
  const racedId = makeAction({ orderNumber: raced.orderNumber });
  db.prepare(`INSERT INTO rakuten_campaign_delivery_attempts (action_id, message_id, attempted_at, outcome) VALUES (?, '<pre-existing@x>', ?, 'ambiguous')`).run(racedId, NOW);
  const before = sentMails.length;
  const r6 = await processReadyActions(db, { keys, sendFn: okSend, nowIso: NOW, limit: 100 });
  // racedはclaim時にUNIQUE違反→claimLost。amb2の残りreadyは送信される
  check('attempt 既存の action は claim 不成立で送らない', r6.claimLost === 1 && !sentMails.slice(before).some((m) => m.text.includes(raced.orderNumber)));

  // PII: 結果オブジェクトに宛先・注文番号が含まれない
  const dump = JSON.stringify([r1, r3, r4, r5, r6]);
  check('結果サマリにメールアドレス・注文番号なし', !dump.includes('@anshin') && !dump.includes('373343-'));

  // note は固定分類のみ (SMTPエラー原文=宛先混入リスクを保存しない)
  check('failed_safe の note は固定値 smtp_rejected', db.prepare(`
    SELECT d.note FROM rakuten_campaign_delivery_attempts d JOIN rakuten_campaign_actions a ON a.id = d.action_id
     WHERE a.status = 'failed_safe'`).get().note === 'smtp_rejected');
  check('ambiguous の note は固定値 smtp_unknown', db.prepare(`
    SELECT d.note FROM rakuten_campaign_delivery_attempts d JOIN rakuten_campaign_actions a ON a.id = d.action_id
     WHERE a.status = 'ambiguous'`).get().note === 'smtp_unknown');
  check('成功時の smtp_code は null (実応答を偽装しない)', db.prepare(`
    SELECT d.smtp_code FROM rakuten_campaign_delivery_attempts d JOIN rakuten_campaign_actions a ON a.id = d.action_id
     WHERE a.status = 'sent' LIMIT 1`).get().smtp_code === null);
}

console.log('=== 5. R1対応: TOCTOU / claimed残留回収 / クーポンURL検証 ===');
{
  // TOCTOU: select 通過後に ownership が vendor に変わったら claim 内の再評価で弾く
  const toc = makeOrder({});
  const tocId = makeAction({ orderNumber: toc.orderNumber });
  const sel = selectEligibleActions(db, { nowIso: NOW, limit: 100 });
  check('前提: TOCTOU対象が eligible', sel.eligible.some((e) => e.id === tocId));
  db.prepare(`UPDATE rakuten_order_campaign_ownership SET owner = 'vendor' WHERE order_number = ?`).run(toc.orderNumber);
  const denied = claimActionGuarded(db, tocId, NOW);
  check('claim内再評価で拒否 (gateFailed=not_self_ownership)', denied.gateFailed === 'not_self_ownership');
  check('action は ready のまま・attempt 予約なし',
    db.prepare(`SELECT status FROM rakuten_campaign_actions WHERE id = ?`).get(tocId).status === 'ready'
    && db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_delivery_attempts WHERE action_id = ?`).get(tocId).n === 0);
  db.prepare(`UPDATE rakuten_order_campaign_ownership SET owner = 'self' WHERE order_number = ?`).run(toc.orderNumber);
  const granted = claimActionGuarded(db, tocId, NOW);
  check('条件復帰後は claim 成功 (messageId+最新スナップショット)', !!granted.messageId && granted.fresh.id === tocId);
  finalizeAttempt(db, { actionId: tocId, outcome: 'accepted', nowIso: NOW });

  // claimed 残留の回収: 送信を開始せず ambiguous に収束
  const fresh = makeOrder({});
  const freshId = makeAction({ orderNumber: fresh.orderNumber });
  const stale = makeOrder({});
  const staleId = makeAction({ orderNumber: stale.orderNumber });
  const c = claimActionGuarded(db, staleId, NOW); // claim したままクラッシュした体
  check('前提: claimed 作成', !!c.messageId);
  const sentMails2 = [];
  const rRec = await processReadyActions(db, { keys, sendFn: async (m) => { sentMails2.push(m); }, nowIso: NOW, limit: 100 });
  check('claimed 残留を ambiguous に回収し、送信は開始しない', rRec.staleRecovered === 1 && rRec.sent === 0 && sentMails2.length === 0
    && db.prepare(`SELECT status, status_reason FROM rakuten_campaign_actions WHERE id = ?`).get(staleId).status === 'ambiguous'
    && db.prepare(`SELECT note FROM rakuten_campaign_delivery_attempts WHERE action_id = ?`).get(staleId).note === 'stale_claim_recovered');
  check('回収後の再実行で通常送信が再開', (await processReadyActions(db, { keys, sendFn: async (m) => { sentMails2.push(m); }, nowIso: NOW, limit: 100 })).sent >= 1
    && db.prepare(`SELECT status FROM rakuten_campaign_actions WHERE id = ?`).get(freshId).status === 'sent');

  // クーポンURL検証 (楽天ドメイン https のみ)
  check('couponUsableCheck: 楽天以外・http は拒否',
    couponUsableCheck({ status: 'issued', pc_get_url: 'https://evil.example.com/x', coupon_end: '2026-09-30T23:59:59+09:00' }, NOW) === false
    && couponUsableCheck({ status: 'issued', pc_get_url: 'http://coupon.rakuten.co.jp/getCoupon?getkey=x', coupon_end: '2026-09-30T23:59:59+09:00' }, NOW) === false
    && couponUsableCheck({ status: 'issued', pc_get_url: 'https://coupon.rakuten.co.jp/getCoupon?getkey=x&rt=', coupon_end: '2026-09-30T23:59:59+09:00' }, NOW) === true);
  check('recoverStaleClaims: claimed なしなら 0', recoverStaleClaims(db, NOW) === 0);
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(tmp, { recursive: true, force: true });
process.exit(failed > 0 ? 1 : 0);
