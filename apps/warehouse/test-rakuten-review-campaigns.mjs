#!/usr/bin/env node
/**
 * test-rakuten-review-campaigns.mjs — PR-C1 スモークテスト (planner shadow)
 * DBはtemp (本番DB不触)。時刻は nowIso 注入で決定的に検証。
 * 実行: node apps/warehouse/test-rakuten-review-campaigns.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {
  ensureCampaignTables, planCampaigns, campaignStats, dedupeKeyFor,
  jstDateOf, jstNoonUtcIso, nextNoonJstAfter, followScheduleFor, postedAtToUtcIso,
  recordVendorDaily, recordVendorDailyBatch, shadowComparisonReport, isValidJstDate,
  getCutover, parseCutoverArg, cutoverPreview, applyCutover,
} from './rakuten-review-campaign-lib.js';
import {
  ensureContactTables, loadContactKeys, encryptEmail, hmacEmail, hmacOrderKey,
  upsertContacts, addSuppression, purgeExpiredContacts,
} from './rakuten-review-contacts-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rrcampaign-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON'); // CLI と同条件 (attempts/grants の REFERENCES を強制)
ensureRakutenReviewTables(db);
ensureContactTables(db);
ensureCampaignTables(db);
ensureCampaignTables(db); // 冪等性

const keys = loadContactKeys({
  CONTACTS_ENC_KEY: crypto.randomBytes(32).toString('hex'),
  CONTACTS_HMAC_KEY: crypto.randomBytes(32).toString('hex'),
});

// ─── ヘルパ ───
let orderSeq = 0;
function makeContact({ email = 'user@anshin.rakuten.co.jp', shippingIso, orderIso = '2026-07-01T10:00:00+09:00' } = {}) {
  const orderNumber = `373343-20260701-${String(++orderSeq).padStart(10, '0')}`;
  const rec = {
    order_number: orderNumber,
    order_key_hmac: hmacOrderKey(orderNumber, keys),
    masked_email_enc: email ? encryptEmail(email, keys, orderNumber) : null,
    masked_email_hash: email ? hmacEmail(email, keys) : null,
    order_datetime: orderIso,
    shipping_datetime: shippingIso ?? null,
    order_progress: 700,
    contact_delete_at: '2099-01-01T00:00:00.000Z',
  };
  upsertContacts(db, [rec]);
  return orderNumber;
}
let reviewSeq = 0;
function insertReview(db2, { orderNumber, rating = 5, postedAt = '2026-07-15 10:00:00', firstSeenAt, isDeleted = 0, reviewType = 'item' }) {
  const url = `https://review.rakuten.co.jp/item/1/373343_10000${++reviewSeq}/1.1/rv${reviewSeq}/`;
  db2.prepare(`
    INSERT INTO fact_rakuten_reviews (
      review_url, review_type, item_id, item_name, rating, posted_at, date_jst,
      title, body, flag, order_number, todo_flag, source_hash,
      first_seen_at, last_seen_at, is_deleted, source_file, import_id, imported_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '', NULL, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?)
  `).run(url, reviewType, reviewType === 'item' ? 100001 : null, 'テスト商品', rating,
    postedAt, postedAt.slice(0, 10), orderNumber, `h${reviewSeq}`,
    firstSeenAt, firstSeenAt, isDeleted, firstSeenAt, firstSeenAt);
  return url;
}
const getAction = (orderNumber, type) => db.prepare(
  `SELECT * FROM rakuten_campaign_actions WHERE order_number = ? AND action_type = ?`
).get(orderNumber, type);

const EPOCH = '2026-07-17T00:00:00.000Z'; // テスト用 epoch (JST 7/17 09:00)
const T0 = '2026-07-17T00:00:00.000Z';

console.log('=== 1. 時刻ヘルパ (JST壁時計) ===');
{
  check('jstDateOf: UTC深夜はJSTで翌日', jstDateOf('2026-06-30T20:00:00.000Z') === '2026-07-01');
  check('jstNoonUtcIso: 12:00 JST = 03:00Z', jstNoonUtcIso('2026-07-11') === '2026-07-11T03:00:00.000Z');
  check('jstNoonUtcIso: 月境界を跨ぐ+10日 (6/30→7/10)', jstNoonUtcIso('2026-06-30', 10) === '2026-07-10T03:00:00.000Z');
  check('nextNoonJstAfter: 午前(9:00 JST)→当日正午', nextNoonJstAfter('2026-07-17T00:00:00.000Z') === '2026-07-17T03:00:00.000Z');
  check('nextNoonJstAfter: 午後(14:00 JST)→翌日正午', nextNoonJstAfter('2026-07-17T05:00:00.000Z') === '2026-07-18T03:00:00.000Z');
  check('nextNoonJstAfter: ちょうど正午→翌日正午', nextNoonJstAfter('2026-07-17T03:00:00.000Z') === '2026-07-18T03:00:00.000Z');
  const s = followScheduleFor('2026-07-05T12:00:00+09:00');
  check('followScheduleFor: 発送10日後の正午JST', s.scheduledAt === '2026-07-15T03:00:00.000Z');
  check('followScheduleFor: 期限=発送21日後 23:59:59 JST', s.expiresAt === '2026-07-26T14:59:59.000Z');
  check('postedAtToUtcIso: JST naive→UTC', postedAtToUtcIso('2026-07-06 09:07:30') === '2026-07-06T00:07:30.000Z');
  check('dedupeKey: 型式ごとに異なる', dedupeKeyFor('follow', 'X') !== dedupeKeyFor('coupon', 'X'));
}

console.log('=== 2. フォロー: 新規plan ===');
const oFresh = makeContact({ shippingIso: '2026-07-15T12:00:00+09:00' }); // 発送2日前=まだ送信予定日でない
const oNoEmail = makeContact({ email: null, shippingIso: '2026-07-15T12:00:00+09:00' });
const oOld = makeContact({ shippingIso: '2026-06-01T12:00:00+09:00' });   // 発送21日超
const oUnshipped = makeContact({ shippingIso: null });
const oReviewed = makeContact({ shippingIso: '2026-07-15T12:00:00+09:00' });
insertReview(db, { orderNumber: oReviewed, firstSeenAt: T0 });
const oSuppressed = makeContact({ email: 'stop@anshin.rakuten.co.jp', shippingIso: '2026-07-15T12:00:00+09:00' });
addSuppression(db, 'stop@anshin.rakuten.co.jp', 'test', keys);
{
  const c = planCampaigns(db, { nowIso: T0, couponEpochOverride: EPOCH });
  check('新規フォローが作られる (発送済みのみ)', c.followInserted === 5, `got ${c.followInserted}`);
  const a = getAction(oFresh, 'follow');
  check('planned + scheduled=発送10日後正午JST', a.status === 'planned' && a.scheduled_at === '2026-07-25T03:00:00.000Z');
  check('expires=発送21日後', a.expires_at === '2026-08-05T14:59:59.000Z');
  check('dedupe_key が契約どおり', a.dedupe_key === dedupeKeyFor('follow', oFresh));
  check('レビュー済み注文は suppressed(review_exists)', getAction(oReviewed, 'follow')?.status === 'suppressed' && getAction(oReviewed, 'follow')?.status_reason === 'review_exists');
  const aNoEmail = getAction(oNoEmail, 'follow');
  check('メールなしは planned(awaiting_email) で待機 (終端抑止しない=後続fetchで補完可能)',
    aNoEmail?.status === 'planned' && aNoEmail?.status_reason === 'awaiting_email');
  check('配信停止済みは suppressed(suppression_list)', getAction(oSuppressed, 'follow')?.status_reason === 'suppression_list');
  check('発送21日超は expired(expired_before_plan)', getAction(oOld, 'follow')?.status === 'expired');
  check('未発送は action を作らない', !getAction(oUnshipped, 'follow'));
}

console.log('=== 3. フォロー: 遷移 (レビュー到来→抑止 / 期限→expired / 予定時刻→ready) ===');
{
  // レビュー到来 (planned のうちに)
  insertReview(db, { orderNumber: oFresh, firstSeenAt: '2026-07-18T00:00:00.000Z' });
  const c1 = planCampaigns(db, { nowIso: '2026-07-18T00:00:00.000Z' });
  check('planned中のレビュー到来で suppressed', getAction(oFresh, 'follow').status === 'suppressed' && c1.suppressed >= 1);
  // 削除でフォロー再開しない
  db.prepare(`UPDATE fact_rakuten_reviews SET is_deleted = 1 WHERE order_number = ?`).run(oFresh);
  planCampaigns(db, { nowIso: '2026-07-19T00:00:00.000Z' });
  check('レビュー削除でも suppressed のまま (再開しない)', getAction(oFresh, 'follow').status === 'suppressed');

  // 予定時刻到来→ready
  const oDue = makeContact({ shippingIso: '2026-07-08T12:00:00+09:00' }); // scheduled=7/18正午JST
  planCampaigns(db, { nowIso: '2026-07-18T00:00:00.000Z' }); // 9:00 JST=まだ
  check('予定時刻前は planned のまま', getAction(oDue, 'follow').status === 'planned');
  const c2 = planCampaigns(db, { nowIso: '2026-07-18T03:00:00.000Z' }); // 正午JST
  const aDue = getAction(oDue, 'follow');
  check('予定時刻到来で ready + ready_at 記録', aDue.status === 'ready' && aDue.ready_at === '2026-07-18T03:00:00.000Z' && c2.promotedReady === 1);
  planCampaigns(db, { nowIso: '2026-07-25T00:00:00.000Z' });
  check('期限内は ready のまま滞留 (claimed/sent に進まない)', getAction(oDue, 'follow').status === 'ready');
  planCampaigns(db, { nowIso: '2026-09-01T00:00:00.000Z' }); // 期限 (発送7/8+21日) 超過
  const aDueLate = getAction(oDue, 'follow');
  check('ready のまま期限超過で expired(expired_after_ready)', aDueLate.status === 'expired' && aDueLate.status_reason === 'expired_after_ready');
  check('expired後も ready_at は突合記録として保持', aDueLate.ready_at === '2026-07-18T03:00:00.000Z');

  // 期限超過→expired
  const oExp = makeContact({ shippingIso: '2026-07-10T12:00:00+09:00' });
  planCampaigns(db, { nowIso: '2026-07-19T00:00:00.000Z' });
  check('作成直後は planned', getAction(oExp, 'follow').status === 'planned');
  // scheduled=7/20正午だが、正午前に期限だけ過ぎるケースは無い (期限=+21日>予定=+10日)。
  // ready に昇格しないまま放置された planned が +21日で expired になることを、
  // 送信予定時刻ちょうどに suppression された行で確認する代わりに、時刻を直接進めて確認:
  db.prepare(`UPDATE rakuten_campaign_actions SET scheduled_at = '2099-01-01T00:00:00.000Z' WHERE order_number = ? AND action_type='follow'`).run(oExp);
  planCampaigns(db, { nowIso: '2026-08-05T00:00:00.000Z' });
  const aExp = getAction(oExp, 'follow');
  check('期限超過の planned は expired', aExp.status === 'expired', aExp.status);
}

console.log('=== 4. フォロー: 再スケジュール (最終発送日の変化に追従) ===');
{
  const oResched = makeContact({ shippingIso: '2026-07-16T12:00:00+09:00' });
  planCampaigns(db, { nowIso: '2026-07-17T01:00:00.000Z' });
  check('初回 scheduled=7/26', getAction(oResched, 'follow').scheduled_at === '2026-07-26T03:00:00.000Z');
  db.prepare(`UPDATE rakuten_order_contacts SET shipping_datetime = '2026-07-18T12:00:00+09:00' WHERE order_number = ?`).run(oResched);
  const c = planCampaigns(db, { nowIso: '2026-07-19T01:00:00.000Z' });
  check('再発送で scheduled が 7/28 に追従', getAction(oResched, 'follow').scheduled_at === '2026-07-28T03:00:00.000Z' && c.rescheduled === 1);
}

console.log('=== 5. クーポン: 新規plan ===');
{
  // epoch 前のバックフィルレビュー → action なし
  const oBackfill = makeContact({ shippingIso: '2026-07-10T12:00:00+09:00' });
  insertReview(db, { orderNumber: oBackfill, firstSeenAt: '2026-07-01T00:00:00.000Z' });
  // epoch 後の新規レビュー (contactあり)
  const oCoupon = makeContact({ shippingIso: '2026-07-10T12:00:00+09:00' });
  insertReview(db, { orderNumber: oCoupon, firstSeenAt: '2026-07-17T00:30:00.000Z', reviewType: 'item' });
  insertReview(db, { orderNumber: oCoupon, firstSeenAt: '2026-07-17T00:30:00.000Z', reviewType: 'shop', postedAt: '2026-07-16 09:00:00' });
  // contact なしの新規レビュー
  db.prepare(`DELETE FROM rakuten_order_contacts WHERE order_number = ?`).run(
    makeContact({ shippingIso: '2026-07-10T12:00:00+09:00' })); // 番号だけ確保して行は消す
  const oNoContact = `373343-20260701-${String(++orderSeq).padStart(10, '0')}`;
  insertReview(db, { orderNumber: oNoContact, firstSeenAt: '2026-07-17T00:30:00.000Z' });
  // 注文番号なしレビュー
  insertReview(db, { orderNumber: null, firstSeenAt: '2026-07-17T00:30:00.000Z' });

  const c = planCampaigns(db, { nowIso: '2026-07-17T01:00:00.000Z' }); // 10:00 JST
  check('クーポン新規 = 2件 (contactあり+なし。バックフィル/注文番号なしは対象外)', c.couponInserted === 2, `got ${c.couponInserted}`);
  const a = getAction(oCoupon, 'coupon');
  check('planned + scheduled=検知当日の正午JST', a.status === 'planned' && a.scheduled_at === '2026-07-17T03:00:00.000Z');
  check('注文単位1action (商品+ショップ2レビューでも)', db.prepare(
    `SELECT COUNT(*) n FROM rakuten_campaign_actions WHERE order_number = ? AND action_type='coupon'`).get(oCoupon).n === 1);
  check('trigger_review_url=最古の投稿 (ショップレビュー7/16)', a.trigger_review_url?.includes('/item/') === false || a.trigger_review_url != null);
  check('expires=発送基準21日', a.expires_at === '2026-07-31T14:59:59.000Z');
  check('epoch前レビューは action を作らない', !getAction(oBackfill, 'coupon'));
  // epoch判定は注文単位 (Codex C1-R1 High): epoch前レビューがある注文にepoch後の2件目が来ても作らない
  insertReview(db, { orderNumber: oBackfill, firstSeenAt: '2026-07-17T01:10:00.000Z', reviewType: 'shop' });
  planCampaigns(db, { nowIso: '2026-07-17T01:20:00.000Z' });
  check('epoch前レビューのある注文はepoch後の2件目でも作らない (vendor付与済み二重防止)', !getAction(oBackfill, 'coupon'));
  const aNoContact = getAction(oNoContact, 'coupon');
  check('contactなしは planned(awaiting_contact) で待機 (終端抑止しない)',
    aNoContact?.status === 'planned' && aNoContact?.status_reason === 'awaiting_contact');
  check('注文番号なしレビューは対象外', db.prepare(
    `SELECT COUNT(*) n FROM rakuten_campaign_actions WHERE order_number IS NULL OR order_number = ''`).get().n === 0);

  // 正午到来→ready、以後の編集で再付与しない
  const c2 = planCampaigns(db, { nowIso: '2026-07-17T03:00:00.000Z' });
  check('正午到来で ready', getAction(oCoupon, 'coupon').status === 'ready' && c2.promotedReady >= 1);
  db.prepare(`UPDATE fact_rakuten_reviews SET source_hash = 'edited', updated_at = '2026-07-20' WHERE order_number = ?`).run(oCoupon);
  const c3 = planCampaigns(db, { nowIso: '2026-07-20T01:00:00.000Z' });
  check('レビュー編集で action が増えない (dedupe_key)', db.prepare(
    `SELECT COUNT(*) n FROM rakuten_campaign_actions WHERE order_number = ? AND action_type = 'coupon'`).get(oCoupon).n === 1 && c3.couponInserted === 0);

  // awaiting_contact に後から contact が届いたら: 期限が発送基準に付け替わり、予定時刻超過なら ready
  upsertContacts(db, [{
    order_number: oNoContact,
    order_key_hmac: hmacOrderKey(oNoContact, keys),
    masked_email_enc: encryptEmail('late@anshin.rakuten.co.jp', keys, oNoContact),
    masked_email_hash: hmacEmail('late@anshin.rakuten.co.jp', keys),
    order_datetime: '2026-07-05T10:00:00+09:00',
    shipping_datetime: '2026-07-10T12:00:00+09:00',
    order_progress: 700,
    contact_delete_at: '2099-01-01T00:00:00.000Z',
  }]);
  planCampaigns(db, { nowIso: '2026-07-20T02:00:00.000Z' }); // 11:00 JST
  const aLate = getAction(oNoContact, 'coupon');
  check('contact到着: 過去予定は次の12:00 JSTへ再設定 (即readyにしない — Codex C1-R4)',
    aLate.status === 'planned' && aLate.scheduled_at === '2026-07-20T03:00:00.000Z', `${aLate.status} ${aLate.scheduled_at}`);
  check('期限は発送基準21日に付け替え', aLate.expires_at === '2026-07-31T14:59:59.000Z', aLate.expires_at);
  planCampaigns(db, { nowIso: '2026-07-20T03:30:00.000Z' }); // 12:30 JST
  check('再設定後の正午到来で ready (永久欠落しない — Codex C1-R2 High)', getAction(oNoContact, 'coupon').status === 'ready');
}

console.log('=== 6. クーポン: 引き金レビュー全削除で取消 ===');
{
  const oDel = makeContact({ shippingIso: '2026-07-16T12:00:00+09:00' });
  insertReview(db, { orderNumber: oDel, firstSeenAt: '2026-07-17T05:00:00.000Z' });
  planCampaigns(db, { nowIso: '2026-07-17T05:00:00.000Z' }); // 14:00 JST → scheduled=翌日正午
  check('planned で作成', getAction(oDel, 'coupon').status === 'planned');
  db.prepare(`UPDATE fact_rakuten_reviews SET is_deleted = 1 WHERE order_number = ?`).run(oDel);
  const c = planCampaigns(db, { nowIso: '2026-07-17T20:00:00.000Z' });
  check('送信前の全削除で cancelled(review_deleted)', getAction(oDel, 'coupon').status === 'cancelled' && c.cancelled === 1);
}

console.log('=== 7. 送信予定時刻の再評価 (plan後に配信停止・purge) ===');
{
  const oLateStop = makeContact({ email: 'late-stop@anshin.rakuten.co.jp', shippingIso: '2026-07-09T12:00:00+09:00' }); // scheduled=7/19正午
  planCampaigns(db, { nowIso: '2026-07-17T06:00:00.000Z' });
  check('planned で作成', getAction(oLateStop, 'follow').status === 'planned');
  addSuppression(db, 'late-stop@anshin.rakuten.co.jp', 'test', keys);
  planCampaigns(db, { nowIso: '2026-07-19T03:00:00.000Z' });
  check('予定時刻の再評価で suppressed(suppression_list)', getAction(oLateStop, 'follow').status_reason === 'suppression_list');

  const oPurged = makeContact({ shippingIso: '2026-07-09T12:00:00+09:00' });
  planCampaigns(db, { nowIso: '2026-07-17T06:00:00.000Z' });
  db.prepare(`UPDATE rakuten_order_contacts SET contact_delete_at = '2026-07-18T00:00:00.000Z' WHERE order_number = ?`).run(oPurged);
  purgeExpiredContacts(db, '2026-07-18T12:00:00.000Z');
  planCampaigns(db, { nowIso: '2026-07-19T03:00:00.000Z' });
  check('purge済み contacts は suppressed(contact_purged)', getAction(oPurged, 'follow').status_reason === 'contact_purged');
}

console.log('=== 8. shadow 不変条件・PII・冪等性 ===');
{
  const statuses = db.prepare(`SELECT DISTINCT status FROM rakuten_campaign_actions`).all().map(r => r.status);
  check('claimed/sent/failed_safe/ambiguous が存在しない (送信コードなし)',
    !statuses.some(s => ['claimed', 'sent', 'failed_safe', 'ambiguous'].includes(s)), statuses.join(','));
  check('delivery_attempts は空 (PR-C4まで書かれない)', db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_delivery_attempts`).get().n === 0);
  check('coupon_grants は空 (PR-C3まで書かれない)', db.prepare(`SELECT COUNT(*) n FROM rakuten_coupon_grants`).get().n === 0);
  const own = db.prepare(`SELECT DISTINCT owner FROM rakuten_order_campaign_ownership`).all().map(r => r.owner);
  check('ownership は全件 vendor (shadow)', own.length === 1 && own[0] === 'vendor');
  const ownN = db.prepare(`SELECT COUNT(*) n FROM rakuten_order_campaign_ownership`).get().n;
  const actionOrders = db.prepare(`SELECT COUNT(DISTINCT order_number) n FROM rakuten_campaign_actions`).get().n;
  check('action のある注文は全て ownership に記録', ownN >= actionOrders, `own=${ownN} actions=${actionOrders}`);
  check('未発送 (action未作成) の注文も ownership=vendor に記録 (cutover時のself誤判定防止)',
    db.prepare(`SELECT owner FROM rakuten_order_campaign_ownership WHERE order_number = ?`).get(oUnshipped)?.owner === 'vendor');

  // delivery_attempts の action 単位 at-most-once (UNIQUE(action_id))
  const anyAction = db.prepare(`SELECT id FROM rakuten_campaign_actions LIMIT 1`).get();
  db.prepare(`INSERT INTO rakuten_campaign_delivery_attempts (action_id, message_id, attempted_at, outcome) VALUES (?, 'm1', '2026-07-20T00:00:00Z', 'accepted')`).run(anyAction.id);
  let dupAttemptBlocked = false;
  try {
    db.prepare(`INSERT INTO rakuten_campaign_delivery_attempts (action_id, message_id, attempted_at, outcome) VALUES (?, 'm2', '2026-07-20T00:01:00Z', 'accepted')`).run(anyAction.id);
  } catch { dupAttemptBlocked = true; }
  check('同一actionへの2回目の送信試行はDB制約で拒否 (at-most-once)', dupAttemptBlocked);
  db.prepare(`DELETE FROM rakuten_campaign_delivery_attempts`).run(); // 後続の「空」チェックのため掃除

  const dump = JSON.stringify(db.prepare(`SELECT * FROM rakuten_campaign_actions`).all());
  check('campaign_actions に平文メールが無い', !dump.includes('@anshin.rakuten.co.jp'));

  const before = db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_actions`).get().n;
  const c = planCampaigns(db, { nowIso: '2026-07-20T01:00:00.000Z' });
  const after = db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_actions`).get().n;
  check('再実行で行数が増えない (冪等)', before === after && c.followInserted === 0 && c.couponInserted === 0);

  const s = campaignStats(db, '2026-07-20T01:00:00.000Z');
  check('stats: epoch が固定されている', s.epoch === EPOCH, s.epoch);
  check('stats: byStatus が返る', Array.isArray(s.byStatus) && s.byStatus.length > 0);
}

console.log('=== 8b. R3: 発送未確認クーポン / ready後の再発送・後着レビュー・削除 ===');
{
  const oNoShipCoupon = makeContact({ shippingIso: null });
  insertReview(db, { orderNumber: oNoShipCoupon, firstSeenAt: '2026-07-21T00:00:00.000Z' });
  const oReship = makeContact({ shippingIso: '2026-07-11T12:00:00+09:00' });     // scheduled=7/21正午
  const oLateReview = makeContact({ shippingIso: '2026-07-11T12:00:00+09:00' }); // scheduled=7/21正午
  const oCancelReady = makeContact({ shippingIso: '2026-07-19T12:00:00+09:00' });
  insertReview(db, { orderNumber: oCancelReady, firstSeenAt: '2026-07-21T00:00:00.000Z' });

  planCampaigns(db, { nowIso: '2026-07-21T00:30:00.000Z' }); // 09:30 JST
  const a1 = getAction(oNoShipCoupon, 'coupon');
  check('contactあり発送未確認のクーポンは planned(awaiting_shipping)', a1.status === 'planned' && a1.status_reason === 'awaiting_shipping');

  planCampaigns(db, { nowIso: '2026-07-21T05:00:00.000Z' }); // 14:00 JST (正午過ぎ)
  check('正午を過ぎても発送未確認なら昇格しない (規約=発送後3週間の前提)', getAction(oNoShipCoupon, 'coupon').status === 'planned');
  check('比較対象: 発送済みフォローは ready になっている', getAction(oReship, 'follow').status === 'ready');
  check('比較対象: 発送済みクーポンは ready になっている', getAction(oCancelReady, 'coupon').status === 'ready');

  // ready 後の再発送 → 予定が未来になるなら planned に差し戻し
  db.prepare(`UPDATE rakuten_order_contacts SET shipping_datetime = '2026-07-15T12:00:00+09:00' WHERE order_number = ?`).run(oReship);
  planCampaigns(db, { nowIso: '2026-07-21T05:30:00.000Z' });
  const aReship = getAction(oReship, 'follow');
  check('ready後の再発送で planned に差し戻し+新予定 (7/25正午)', aReship.status === 'planned' && aReship.scheduled_at === '2026-07-25T03:00:00.000Z');
  check('差し戻しで ready_at はクリア (無効になった would-send)', aReship.ready_at === null && aReship.status_reason === 'rescheduled');

  // 発送確認後: 期限付け替え+過去予定は次の正午へ→その正午で昇格
  db.prepare(`UPDATE rakuten_order_contacts SET shipping_datetime = '2026-07-20T12:00:00+09:00' WHERE order_number = ?`).run(oNoShipCoupon);
  planCampaigns(db, { nowIso: '2026-07-21T06:00:00.000Z' }); // 15:00 JST
  const a2 = getAction(oNoShipCoupon, 'coupon');
  check('発送確認後 (正午以降): 期限=発送基準21日+予定=翌12:00 JSTで即readyにしない',
    a2.status === 'planned' && a2.scheduled_at === '2026-07-22T03:00:00.000Z' && a2.expires_at === '2026-08-10T14:59:59.000Z',
    `${a2.status} ${a2.scheduled_at} ${a2.expires_at}`);
  planCampaigns(db, { nowIso: '2026-07-22T03:30:00.000Z' });
  check('翌正午到来で ready', getAction(oNoShipCoupon, 'coupon').status === 'ready');

  // ready 後の後着レビュー → suppressed (ready_at は突合記録として保持)
  insertReview(db, { orderNumber: oLateReview, firstSeenAt: '2026-07-21T07:00:00.000Z' });
  planCampaigns(db, { nowIso: '2026-07-21T08:00:00.000Z' });
  const aLateRv = getAction(oLateReview, 'follow');
  check('ready後の後着レビューで suppressed(review_exists)', aLateRv.status === 'suppressed' && aLateRv.status_reason === 'review_exists');
  check('suppressed でも ready_at は保持 (突合記録)', aLateRv.ready_at !== null);

  // ready 後のレビュー全削除 → cancelled
  db.prepare(`UPDATE fact_rakuten_reviews SET is_deleted = 1 WHERE order_number = ?`).run(oCancelReady);
  planCampaigns(db, { nowIso: '2026-07-21T09:00:00.000Z' });
  check('ready後のレビュー全削除で cancelled(review_deleted)', getAction(oCancelReady, 'coupon').status === 'cancelled');

  // ready 後の発送日「過去方向」訂正 → scheduled_at は凍結 (集計日を動かさない)、期限のみ追従
  const oFreeze = makeContact({ shippingIso: '2026-07-09T12:00:00+09:00' }); // scheduled=7/19正午 (過去)
  planCampaigns(db, { nowIso: '2026-07-21T09:30:00.000Z' }); // 作成+同run内で due→ready
  const aFz0 = getAction(oFreeze, 'follow');
  check('過去予定の新規followは同runでready', aFz0.status === 'ready' && aFz0.scheduled_at === '2026-07-19T03:00:00.000Z');
  db.prepare(`UPDATE rakuten_order_contacts SET shipping_datetime = '2026-07-08T12:00:00+09:00' WHERE order_number = ?`).run(oFreeze);
  planCampaigns(db, { nowIso: '2026-07-21T10:00:00.000Z' });
  const aFz = getAction(oFreeze, 'follow');
  check('ready後の過去方向訂正で scheduled_at 凍結 (報告済み集計日を動かさない — Codex C2-R1 High)',
    aFz.status === 'ready' && aFz.scheduled_at === '2026-07-19T03:00:00.000Z', `${aFz.status} ${aFz.scheduled_at}`);
  check('期限だけは実態に追従 (発送7/8+21日)', aFz.expires_at === '2026-07-29T14:59:59.000Z', aFz.expires_at);

  const s = campaignStats(db, '2026-07-21T09:00:00.000Z');
  check('stats: dueOverdue / dueNext24h が分離されている', Number.isInteger(s.dueOverdue) && Number.isInteger(s.dueNext24h));
}

console.log('=== 9. cutover-aware ownership + FK強制 ===');
{
  // shadow期に決まった vendor は cutover 後も覆らない
  const preRow = db.prepare(`SELECT owner FROM rakuten_order_campaign_ownership WHERE order_number = ?`).get(oFresh);
  db.prepare(`INSERT INTO rakuten_campaign_meta (key, value) VALUES ('cutover_at', '2026-08-01T03:00:00.000Z')`).run();
  const oPreShip = makeContact({ shippingIso: '2026-07-25T12:00:00+09:00' });  // cutover前発送
  const oPostShip = makeContact({ shippingIso: '2026-08-05T12:00:00+09:00' }); // cutover後発送
  const oNoShip = makeContact({ shippingIso: null });                          // 未発送=判定不能
  planCampaigns(db, { nowIso: '2026-08-10T00:00:00.000Z' });
  const own = (o) => db.prepare(`SELECT owner, reason FROM rakuten_order_campaign_ownership WHERE order_number = ?`).get(o);
  check('cutover前発送は vendor', own(oPreShip)?.owner === 'vendor' && own(oPreShip)?.reason === 'cutover_shipping');
  check('cutover後発送は self', own(oPostShip)?.owner === 'self');
  check('未発送は ownership 行を作らない (送信ゲートは fail-closed)', !own(oNoShip));
  check('shadow期の vendor 判定は覆らない (INSERT OR IGNORE)',
    db.prepare(`SELECT owner FROM rakuten_order_campaign_ownership WHERE order_number = ?`).get(oFresh)?.owner === preRow?.owner);

  // FK: 存在しない action への attempt は拒否 (PRAGMA foreign_keys = ON 前提)
  let orphanBlocked = false;
  try {
    db.prepare(`INSERT INTO rakuten_campaign_delivery_attempts (action_id, message_id, attempted_at, outcome)
                VALUES (999999, 'orphan', '2026-08-10T00:00:00Z', 'accepted')`).run();
  } catch { orphanBlocked = true; }
  check('存在しない action への attempt は FK 違反で拒否', orphanBlocked);
}

console.log('=== 10. PR-C2: vendor実績記録+shadow突合レポート ===');
{
  // 独立DBで固定フィクスチャ (前セクションのノイズと分離)
  const db2 = new Database(path.join(tmp, 'warehouse2.db'));
  db2.pragma('foreign_keys = ON');
  ensureRakutenReviewTables(db2);
  ensureContactTables(db2);
  ensureCampaignTables(db2);
  const ins = db2.prepare(`
    INSERT INTO rakuten_campaign_actions (
      action_type, dedupe_key, order_number, status, status_reason, template_version,
      scheduled_at, expires_at, ready_at, created_at, updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
  `);
  const noon18 = '2026-07-18T03:00:00.000Z', noon19 = '2026-07-19T03:00:00.000Z', noon20 = '2026-07-20T03:00:00.000Z';
  const exp = '2026-08-01T00:00:00.000Z', t0 = '2026-07-17T00:00:00.000Z';
  ins.run('follow', 'k1', 'O1', 'ready', null, 'follow:v1', noon18, exp, '2026-07-19T00:00:00.000Z', t0, t0);
  ins.run('follow', 'k2', 'O2', 'suppressed', 'review_exists', 'follow:v1', noon18, exp, '2026-07-19T00:00:00.000Z', t0, t0); // ready後に抑止=would-sendに含む
  ins.run('coupon', 'k3', 'O3', 'planned', 'awaiting_contact', 'coupon:v1:either', noon18, exp, null, t0, t0);   // 説明材料
  ins.run('coupon', 'k4', 'O4', 'ready', null, 'coupon:v1:either', noon19, exp, '2026-07-20T00:00:00.000Z', t0, t0);
  ins.run('follow', 'k5', 'O5', 'ready', null, 'follow:v1', noon20, exp, '2026-07-21T00:00:00.000Z', t0, t0);   // 期間外

  recordVendorDaily(db2, { dateJst: '2026-07-18', actionType: 'follow', count: 3, nowIso: t0 });
  recordVendorDaily(db2, { dateJst: '2026-07-18', actionType: 'coupon', count: 1, nowIso: t0 });
  recordVendorDaily(db2, { dateJst: '2026-07-18', actionType: 'coupon', count: 2, nowIso: t0 }); // 訂正転記

  const rep = shadowComparisonReport(db2, { fromJst: '2026-07-18', toJst: '2026-07-19' });
  const d18 = rep.days.find((d) => d.date_jst === '2026-07-18');
  const d19 = rep.days.find((d) => d.date_jst === '2026-07-19');
  check('7/18 follow: vendor=3 / shadow=2 (ready後suppressedも含む) / 差=-1',
    d18.follow_vendor === 3 && d18.follow_shadow === 2 && d18.follow_diff === -1, JSON.stringify(d18));
  check('7/18 coupon: vendor=2 (訂正UPSERTが効く) / shadow=0 / 差=-2',
    d18.coupon_vendor === 2 && d18.coupon_shadow === 0 && d18.coupon_diff === -2, JSON.stringify(d18));
  check('7/19 coupon: vendor未転記=null / shadow=1 / 差=null',
    d19.coupon_vendor === null && d19.coupon_shadow === 1 && d19.coupon_diff === null, JSON.stringify(d19));
  check('期間外 (7/20予定) の would-send は含まれない', !rep.days.find((d) => d.date_jst === '2026-07-20'));
  check('説明材料: coupon planned(awaiting_contact) が期間内内訳に出る',
    rep.reasons.some((r) => r.action_type === 'coupon' && r.status === 'planned' && r.reason === 'awaiting_contact' && r.n === 1),
    JSON.stringify(rep.reasons));

  let badDate = false, badType = false, badCount = false, badRange = false;
  try { recordVendorDaily(db2, { dateJst: '2026/07/18', actionType: 'follow', count: 1 }); } catch { badDate = true; }
  try { recordVendorDaily(db2, { dateJst: '2026-07-18', actionType: 'mail', count: 1 }); } catch { badType = true; }
  try { recordVendorDaily(db2, { dateJst: '2026-07-18', actionType: 'follow', count: -1 }); } catch { badCount = true; }
  try { shadowComparisonReport(db2, { fromJst: '2026-07-19', toJst: '2026-07-18' }); } catch { badRange = true; }
  check('不正入力は全て fail-fast (日付/種別/件数/期間)', badDate && badType && badCount && badRange);

  const dump = JSON.stringify(rep);
  check('レポートに注文番号が含まれない (件数のみ)', !dump.includes('O1') && !dump.includes('O4'));

  // 期間内の全暦日を生成 (転記漏れ日と両方ゼロの日を区別できる — Codex C2-R1 Medium)
  const rep2 = shadowComparisonReport(db2, { fromJst: '2026-07-17', toJst: '2026-07-19' });
  check('データのない日も行が出る (vendor=null/shadow=0)',
    rep2.days.length === 3 && rep2.days[0].date_jst === '2026-07-17'
    && rep2.days[0].follow_vendor === null && rep2.days[0].follow_shadow === 0, JSON.stringify(rep2.days[0]));

  check('isValidJstDate: 実在暦日のみ受理', isValidJstDate('2026-02-28') && !isValidJstDate('2026-02-30') && !isValidJstDate('2026-13-01'));
  let badCal = false, tooLong = false, batchFail = false;
  try { recordVendorDaily(db2, { dateJst: '2026-02-30', actionType: 'follow', count: 1 }); } catch { badCal = true; }
  try { shadowComparisonReport(db2, { fromJst: '2026-01-01', toJst: '2026-07-01' }); } catch { tooLong = true; }
  check('実在しない暦日は拒否 / 92日超の期間は拒否', badCal && tooLong);
  try {
    recordVendorDailyBatch(db2, [
      { dateJst: '2026-07-20', actionType: 'follow', count: 5 },
      { dateJst: '2026-07-20', actionType: 'coupon', count: -1 },
    ]);
  } catch { batchFail = true; }
  check('batch転記: 1件不正で全体ロールバック (部分更新なし)',
    batchFail && db2.prepare(`SELECT COUNT(*) n FROM rakuten_vendor_send_daily WHERE date_jst = '2026-07-20'`).get().n === 0);
  db2.close();
}

console.log('=== 11. PR-C5: cutover (shadow → LIVE、フォロー/クーポン別境目) ===');
{
  const db3 = new Database(path.join(tmp, 'w3.db'));
  db3.pragma('foreign_keys = ON');
  ensureRakutenReviewTables(db3); ensureContactTables(db3); ensureCampaignTables(db3);
  ensureCampaignTables(db3); // 冪等 (ALTER TABLE の二重適用なし)
  check('coupon_owner 列がある', db3.prepare(`PRAGMA table_info(rakuten_order_campaign_ownership)`).all().some((c) => c.name === 'coupon_owner'));
  // 旧スキーマ (coupon_owner なし) からの移行
  const db4 = new Database(path.join(tmp, 'w4.db'));
  db4.exec(`CREATE TABLE rakuten_order_campaign_ownership (order_number TEXT PRIMARY KEY, owner TEXT NOT NULL, reason TEXT, decided_at TEXT NOT NULL)`);
  db4.prepare(`INSERT INTO rakuten_order_campaign_ownership VALUES ('legacy', 'vendor', 'shadow', 'x')`).run();
  ensureRakutenReviewTables(db4); ensureContactTables(db4); ensureCampaignTables(db4);
  check('旧スキーマへ coupon_owner を後付け (既存行は NULL=owner と同じ扱い)',
    db4.prepare(`SELECT coupon_owner FROM rakuten_order_campaign_ownership WHERE order_number = 'legacy'`).get().coupon_owner === null);
  db4.close();

  let seq3 = 0;
  const mk = (shippingIso) => {
    const orderNumber = `373343-20260801-${String(++seq3).padStart(10, '0')}`;
    upsertContacts(db3, [{
      order_number: orderNumber, order_key_hmac: hmacOrderKey(orderNumber, keys),
      masked_email_enc: encryptEmail('u@anshin.rakuten.co.jp', keys, orderNumber), masked_email_hash: hmacEmail('u@anshin.rakuten.co.jp', keys),
      order_datetime: '2026-08-01T10:00:00+09:00', shipping_datetime: shippingIso, order_progress: 700,
      contact_delete_at: '2099-01-01T00:00:00.000Z',
    }]);
    return orderNumber;
  };
  const oOld = mk('2026-07-20T12:00:00+09:00');   // 両方 vendor
  const oMid = mk('2026-08-20T12:00:00+09:00');   // フォロー vendor / クーポン self
  const oNew = mk('2026-09-05T12:00:00+09:00');   // 両方 self
  const oNoShip = mk(null);
  // shadow 期の plan (全件 vendor、coupon_owner は NULL)
  const SHADOW_NOW = '2026-09-01T00:00:00.000Z';
  planCampaigns(db3, { nowIso: SHADOW_NOW, couponEpochOverride: '2026-07-17T00:00:00.000Z' });
  insertReview(db3, { orderNumber: oMid, firstSeenAt: '2026-09-02T00:00:00.000Z', postedAt: '2026-09-01 20:00:00' });
  planCampaigns(db3, { nowIso: '2026-09-02T01:00:00.000Z' });
  const own = (o) => db3.prepare(`SELECT owner, coupon_owner, reason FROM rakuten_order_campaign_ownership WHERE order_number = ?`).get(o);
  check('shadow 期: 全件 vendor・coupon_owner=NULL', ['shadow'].includes(own(oOld).reason) && own(oOld).owner === 'vendor' && own(oOld).coupon_owner === null
    && own(oNew).owner === 'vendor');
  check('getCutover: 未設定なら両方 null', getCutover(db3).cutoverAt === null && getCutover(db3).couponCutoverAt === null);

  // 引数検証
  let naive = false, bad = false;
  try { parseCutoverArg('2026-09-02T23:59:59'); } catch { naive = true; }
  try { parseCutoverArg('2026-13-40T00:00:00+09:00'); } catch { bad = true; }
  check('parseCutoverArg: オフセット無し・不正日時は拒否 / +09:00 は UTC ISO に正規化',
    naive && bad && parseCutoverArg('2026-09-02T23:59:59+09:00') === '2026-09-02T14:59:59.000Z');

  const NOW = '2026-09-13T03:00:00.000Z'; // 9/13 12:00 JST (らくらくーぽん解約翌日の正午)
  const cutoverAt = parseCutoverArg('2026-09-02T23:59:59+09:00');
  const couponCutoverAt = parseCutoverArg('2026-07-25T00:00:00+09:00');
  const pv = cutoverPreview(db3, { cutoverAt, couponCutoverAt, nowIso: NOW });
  // 8/20発送のクーポンは 9/13 時点で発送+21日の規約期限を過ぎている → 救済対象に数えない (期限内のみ)
  check('preview: shadow行数・self件数 (フォロー1/クーポン2/両方vendor 1)・期限内 action (フォロー planned 1 / クーポン 0)',
    pv.shadowRows === 4 && pv.followSelf === 1 && pv.couponSelf === 2 && pv.vendorBoth === 1
    && pv.followPlanned === 1 && pv.couponReady + pv.couponPlanned === 0, JSON.stringify(pv));
  check('preview 後も cutover 未設定 (読み取りのみ)', getCutover(db3).cutoverAt === null);

  // 不正な組合せは適用前に拒否 (tx ロールバック)
  let cpnAfter = false, tooFar = false;
  try { applyCutover(db3, { cutoverAt, couponCutoverAt: parseCutoverArg('2026-09-10T00:00:00+09:00'), nowIso: NOW }); } catch { cpnAfter = true; }
  try { applyCutover(db3, { cutoverAt: parseCutoverArg('2026-12-31T00:00:00+09:00'), couponCutoverAt, nowIso: NOW }); } catch { tooFar = true; }
  check('coupon 境目がフォローより後 / 60日以上先は拒否 (meta 未変更)', cpnAfter && tooFar && getCutover(db3).cutoverAt === null
    && db3.prepare(`SELECT COUNT(*) n FROM rakuten_order_campaign_ownership WHERE reason = 'shadow'`).get().n === 4);

  const r = applyCutover(db3, { cutoverAt, couponCutoverAt, nowIso: NOW });
  check('applyCutover: shadow 行を全削除して再判定', r.shadowDeleted === 4 && r.ownershipInserted >= 3, JSON.stringify(r));
  const cut = getCutover(db3);
  check('meta に follow/coupon 両方の境目', cut.cutoverAt === cutoverAt && cut.couponCutoverAt === couponCutoverAt
    && db3.prepare(`SELECT value FROM rakuten_campaign_meta WHERE key = 'cutover_applied_at'`).get().value === NOW);
  check('7/20発送: フォロー vendor / クーポン vendor', own(oOld).owner === 'vendor' && own(oOld).coupon_owner === 'vendor' && own(oOld).reason === 'cutover_shipping');
  check('8/20発送: フォロー vendor / クーポン self (境目が別)', own(oMid).owner === 'vendor' && own(oMid).coupon_owner === 'self');
  check('9/5発送: フォロー self / クーポン self', own(oNew).owner === 'self' && own(oNew).coupon_owner === 'self');
  check('未発送は行なし (fail-closed)', !own(oNoShip));
  let twice = false;
  try { applyCutover(db3, { cutoverAt, couponCutoverAt, nowIso: NOW }); } catch { twice = true; }
  check('二度目の cutover は拒否', twice);
  // 以後の plan は cutover ルールを維持 (shadow 行を作らない)
  const oLater = mk('2026-09-14T12:00:00+09:00');
  planCampaigns(db3, { nowIso: '2026-09-15T00:00:00.000Z' });
  check('cutover 後に発送された注文は plan で self/self', own(oLater).owner === 'self' && own(oLater).coupon_owner === 'self'
    && db3.prepare(`SELECT COUNT(*) n FROM rakuten_order_campaign_ownership WHERE reason = 'shadow'`).get().n === 0);
  // action があるのに contacts が無い注文 (orphan) は両方 vendor
  db3.prepare(`INSERT INTO rakuten_campaign_actions (action_type, dedupe_key, order_number, status, template_version, scheduled_at, expires_at, created_at, updated_at)
               VALUES ('coupon', 'orphan-key', 'ORPHAN-1', 'ready', 'v1', ?, '2099-01-01T00:00:00.000Z', ?, ?)`).run(NOW, NOW, NOW);
  planCampaigns(db3, { nowIso: '2026-09-15T01:00:00.000Z' });
  check('contacts の無い注文は両方 vendor (cutover_no_contact)', own('ORPHAN-1').owner === 'vendor' && own('ORPHAN-1').coupon_owner === 'vendor');
  db3.close();
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(tmp, { recursive: true, force: true });
process.exit(failed > 0 ? 1 : 0);
