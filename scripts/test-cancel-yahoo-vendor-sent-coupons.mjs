/**
 * test-cancel-yahoo-vendor-sent-coupons.mjs — vendor が送ったクーポンを二重に送らない仕組みの受入試験
 *
 * 本物のエンジン (Yahoo の planCampaigns / applyCutover / 送信ゲート / 送る直前の再判定) を一時 SQLite で動かし、
 * 9/12 13:00 の切り替えと同じ順番 (取り消し → cutover → 翌日の送信) を再現する。本番の DB には触らない。
 * 使い方: node scripts/test-cancel-yahoo-vendor-sent-coupons.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import { createCampaignEngine, tablesFor } from '../apps/warehouse/rakuten-review-campaign-lib.js';
import { ensureYahooReviewTables, importYahooReviewFile, HEADER_COLS } from '../apps/warehouse/yahoo-review-lib.js';
import {
  createSenderEngine, gateReason, vendorCouponCovered, firstReviewDateSql, VENDOR_COUPON_THROUGH_KEY,
} from '../apps/warehouse/rakuten-review-sender-lib.js';
import {
  findVendorSentCoupons, cancelVendorSentCoupons, validateReviewsThrough, currentStage, REASON,
} from '../apps/warehouse/cancel-yahoo-vendor-sent-coupons.js';

let passed = 0;
async function t(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}

const J = (jst) => new Date(`${jst}+09:00`).toISOString();   // JST の日時 → UTC ISO
const EPOCH = '2026-08-01T00:00:00.000Z';
const BOUNDARY = J('2026-09-02T23:59:59');
const THROUGH = '2026-09-11';   // vendor の最終送信 9/12 正午が受け持ったレビュー = 9/11 投稿分まで
const Y = createCampaignEngine('yahoo');
const S = createSenderEngine({
  mall: 'yahoo',
  monthlyCouponFor: () => ({
    status: 'issued', pc_get_url: 'https://shopping.yahoo.co.jp/coupon/test',
    coupon_start: '2026-09-01T00:00:00.000Z', coupon_end: '2026-09-30T14:59:59.000Z',
  }),
  resolveRecipient: async () => 'someone@example.com',
  buildMail: () => ({ subject: 's', text: 't' }),
  fromHeader: '"test" <info@b-faith.biz>',
  messageIdFor: (id, key) => `<t-${id}-${key}@b-faith.biz>`,
  couponUrlOk: (u) => String(u).startsWith('https://shopping.yahoo.co.jp/'),
});

function makeDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cancel-coupons-'));
  const file = path.join(dir, 'warehouse.db');
  const db = new Database(file);
  db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
    order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
  db.exec('CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)');
  db.exec('CREATE TABLE fact_yahoo_reviews (review_url TEXT PRIMARY KEY, order_number TEXT, rating INTEGER, posted_at TEXT, first_seen_at TEXT, is_deleted INTEGER DEFAULT 0)');
  Y.ensureCampaignTables(db);
  return { db, file };
}
const addOrder = (db, order, shipJst) => db.prepare(`INSERT INTO yahoo_order_contacts
  (order_number, shipping_datetime, masked_email_enc, masked_email_hash, fetched_at) VALUES (?, ?, 'enc', ?, ?)`)
  .run(order, shipJst ? `${shipJst}+09:00` : null, `h-${order}`, EPOCH);
// Yahoo のレビューは投稿「日」だけ ('YYYY-MM-DD 00:00:00')。first_seen_at = うちが取り込んだ時刻
const addReview = (db, order, postedDate, seenJst) => db.prepare(`INSERT INTO fact_yahoo_reviews
  (review_url, order_number, rating, posted_at, first_seen_at, is_deleted) VALUES (?, ?, 5, ?, ?, 0)`)
  .run(`yahoo:${order}`, order, `${postedDate} 00:00:00`, J(seenJst));
const plan = (db, jst) => Y.planCampaigns(db, { nowIso: J(jst), couponEpochOverride: EPOCH });
const couponOf = (db, order) => db.prepare(`SELECT * FROM yahoo_campaign_actions WHERE action_type = 'coupon' AND order_number = ?`).get(order);
const followOf = (db, order) => db.prepare(`SELECT * FROM yahoo_campaign_actions WHERE action_type = 'follow' AND order_number = ?`).get(order);
const meta = (db, key) => db.prepare('SELECT value FROM yahoo_campaign_meta WHERE key = ?').get(key)?.value ?? null;

console.log('9/12 13:00 の切り替えを本物のエンジンで再現');
{
  const { db, file } = makeDb();
  addOrder(db, 'A', '2026-09-03T10:00:00');   // うち側。9/10 投稿 → vendor が 9/11 正午に送信済み
  addOrder(db, 'B', '2026-09-04T10:00:00');   // うち側。9/11 投稿 → vendor が 9/12 正午に送信済み
  addOrder(db, 'C', '2026-09-03T11:00:00');   // うち側。9/11 投稿だが、うちの取込が切り替えのあと (9/12 14:00) に遅れた
  addOrder(db, 'D', '2026-09-05T10:00:00');   // うち側。9/12 朝の投稿 → vendor は送っていない (9/13 は止まっている)
  addOrder(db, 'E', null);                    // 発送日時があとから届く。9/10 投稿
  addOrder(db, 'V', '2026-09-01T10:00:00');   // vendor 側 (境目より前の発送)。9/10 投稿
  addOrder(db, 'N', '2026-09-03T12:00:00');   // うち側。レビューなし = フォローだけ
  addReview(db, 'A', '2026-09-10', '2026-09-11T01:00:00');
  addReview(db, 'E', '2026-09-10', '2026-09-11T01:00:00');
  addReview(db, 'V', '2026-09-10', '2026-09-11T01:00:00');
  plan(db, '2026-09-11T02:00:00');
  plan(db, '2026-09-11T12:30:00');
  addReview(db, 'B', '2026-09-11', '2026-09-12T01:00:00');
  plan(db, '2026-09-12T02:00:00');
  addReview(db, 'D', '2026-09-12', '2026-09-12T03:00:00');
  plan(db, '2026-09-12T03:00:00');
  plan(db, '2026-09-12T12:30:00');
  const now = J('2026-09-12T12:55:00');

  await t('前提: 切り替え前 (shadow) で A・B・D・V のクーポンは ready、E は発送待ちの planned', () => {
    assert.equal(currentStage(db), 'shadow');
    for (const o of ['A', 'B', 'D', 'V']) assert.equal(couponOf(db, o).status, 'ready', o);
    assert.equal(couponOf(db, 'E').status, 'planned');
    assert.equal(couponOf(db, 'D').scheduled_at, J('2026-09-12T12:00:00'), 'D はうちの予定が 9/12 正午 (旧判定なら取り消されていた)');
  });

  await t('試しは読み取り専用の接続で数えられる (= 何も書かない)。取り消しは読み取り専用では書けない', () => {
    const ro = new Database(file, { readonly: true, fileMustExist: true });
    const f = findVendorSentCoupons(ro, { reviewsThrough: THROUGH });
    assert.equal(f.total, 4, 'A・B・E・V (担当は見ない)');
    assert.deepEqual(f.byPostedDate, { '2026-09-10': 3, '2026-09-11': 1 });
    assert.equal(f.claimed, 0);
    assert.equal(f.currentThrough, null);
    assert.throws(() => cancelVendorSentCoupons(ro, { reviewsThrough: THROUGH, expect: 4, nowIso: now }), /readonly/i);
    ro.close();
  });

  await t('🚨 --expect が 1 件でも違えば、行も日付も書かずに止まる', () => {
    assert.throws(() => cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 3, nowIso: now }), (e) => e.code === 'EXPECT_MISMATCH');
    assert.equal(couponOf(db, 'A').status, 'ready');
    assert.equal(meta(db, VENDOR_COUPON_THROUGH_KEY), null);
  });

  await t('切り替えの前に流す: 4 行を cancelled にし、送信ゲートに日付を記録する (D は残す)', () => {
    const r = cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 4, nowIso: now });
    assert.equal(r.cancelled, 4);
    assert.equal(r.stage, 'shadow');
    for (const o of ['A', 'B', 'E', 'V']) assert.deepEqual([couponOf(db, o).status, couponOf(db, o).status_reason], ['cancelled', REASON], o);
    assert.equal(couponOf(db, 'D').status, 'ready', '🚨 9/12 投稿は vendor が送っていない → 取り消さない');
    assert.equal(meta(db, VENDOR_COUPON_THROUGH_KEY), THROUGH);
  });

  // E の発送日時が届き、13:00 に本物の cutover (境目 9/2)
  db.prepare(`UPDATE yahoo_order_contacts SET shipping_datetime = '2026-09-06T10:00:00+09:00' WHERE order_number = 'E'`).run();
  Y.applyCutover(db, { cutoverAt: BOUNDARY, couponCutoverAt: BOUNDARY, nowIso: J('2026-09-12T13:00:00') });
  // 切り替えのあとで C のレビュー (9/11 投稿) が遅れて取り込まれ、うちの予定は 9/13 正午になる
  addReview(db, 'C', '2026-09-11', '2026-09-12T14:00:00');
  plan(db, '2026-09-12T14:00:00');
  plan(db, '2026-09-13T12:10:00');
  const sendAt = J('2026-09-13T12:20:00');

  await t('cutover と再計画のあとも、取り消した行は cancelled のまま (作り直されない)', () => {
    assert.equal(currentStage(db), 'live');
    for (const o of ['A', 'B', 'E', 'V']) assert.equal(couponOf(db, o).status, 'cancelled', o);
    assert.equal(db.prepare(`SELECT COUNT(*) n FROM yahoo_campaign_actions WHERE action_type = 'coupon' AND order_number IN ('A','B','E','V')`).get().n, 4);
  });

  await t('🚨 あとから作られた C (9/11 投稿・予定 9/13 正午) は送信ゲートが止める', () => {
    const c = couponOf(db, 'C');
    assert.equal(c.status, 'ready');
    assert.equal(c.scheduled_at, J('2026-09-13T12:00:00'), '予定時刻では取り消し対象にならない形');
    const sel = S.selectEligibleActions(db, { nowIso: sendAt, limit: 100 });
    assert.ok(sel.skipped.some((k) => k.id === c.id && k.reason === 'vendor_already_sent'), JSON.stringify(sel.skipped));
    assert.ok(!sel.eligible.some((e) => e.id === c.id));
  });

  await t('9/13 12:20 の最初の送信: 送るのは D のクーポンと N のフォローだけ', () => {
    const sel = S.selectEligibleActions(db, { nowIso: sendAt, limit: 100 });
    const got = sel.eligible.map((e) => `${e.action_type}:${e.order_number}`).sort();
    assert.deepEqual(got, ['coupon:D', 'follow:N']);
  });

  await t('🚨 送る直前の再判定でも C は止まり、D は通る', () => {
    const rc = S.claimActionGuarded(db, couponOf(db, 'C').id, sendAt);
    assert.equal(rc.gateFailed, 'vendor_already_sent');
    assert.equal(couponOf(db, 'C').status, 'ready', '止めた行は claimed にならない');
    const rd = S.claimActionGuarded(db, couponOf(db, 'D').id, sendAt);
    assert.ok(rd.messageId && !rd.gateFailed, JSON.stringify(rd));
    assert.equal(couponOf(db, 'D').status, 'claimed');
  });

  await t('🚨 送り始めた (claimed) クーポンがあれば止まる / 別の日付は上書きしない', () => {
    assert.throws(() => cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 1, nowIso: sendAt }), (e) => e.code === 'SENDER_ACTIVE');
    assert.throws(() => cancelVendorSentCoupons(db, { reviewsThrough: '2026-09-10', expect: 1, nowIso: sendAt }), (e) => e.code === 'THROUGH_MISMATCH');
    assert.equal(couponOf(db, 'C').status, 'ready');
    assert.equal(meta(db, VENDOR_COUPON_THROUGH_KEY), THROUGH);
  });
  db.close();
}

console.log('\n取り込み直しで投稿日が上書きされても止める (本物のレビュー取込を通す・Codex R2)');
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cancel-coupons-import-'));
  const db = new Database(path.join(dir, 'warehouse.db'));
  db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
    order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
  db.exec('CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)');
  ensureYahooReviewTables(db);   // 本番と同じレビュー表 + 版の履歴 (fact_yahoo_review_revisions)
  Y.ensureCampaignTables(db);
  const ORDER = 'b-faith01-10290001';
  addOrder(db, ORDER, '2026-09-03T10:00:00');
  const zipOf = (date) => {
    const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
    const rows = [HEADER_COLS, [date, '5', '商品 item-a', 'item-a', ORDER, 'よい', 'よかった', '0', '0', '0']];
    const z = new AdmZip();
    z.addFile('20260801_20260913_ItemReview.csv', iconv.encode(rows.map((r) => r.map(esc).join(',')).join('\r\n') + '\r\n', 'Shift_JIS'));
    return z.toBuffer();
  };
  const importAt = (date, jst) => {
    const buf = zipOf(date);
    const r = importYahooReviewFile(db, { name: '20260801_20260913_ItemReview.zip', buffer: buf, sha256: crypto.createHash('sha256').update(buf).digest('hex'), nowIso: J(jst) });
    assert.equal(r.status, 'ok', JSON.stringify(r.results));
  };
  const coupon = () => couponOf(db, ORDER);

  plan(db, '2026-09-12T12:30:00');
  cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 0, nowIso: J('2026-09-12T12:55:00') });
  Y.applyCutover(db, { cutoverAt: BOUNDARY, couponCutoverAt: BOUNDARY, nowIso: J('2026-09-12T13:00:00') });
  importAt('20260911', '2026-09-12T14:00:00');   // 9/11 投稿が切り替えのあとに遅れて入る
  plan(db, '2026-09-12T14:00:00');
  plan(db, '2026-09-13T12:10:00');

  await t('前提: 遅れて入った 9/11 投稿のクーポンは ready になり、送る直前の再判定で止まる', () => {
    assert.equal(coupon().status, 'ready');
    assert.equal(S.claimActionGuarded(db, coupon().id, J('2026-09-13T12:20:00')).gateFailed, 'vendor_already_sent');
  });

  importAt('20260912', '2026-09-13T12:30:00');   // 同じ注文×商品のレビューが 9/12 投稿として取り込み直される
  plan(db, '2026-09-13T12:40:00');

  await t('🚨 今の行の投稿日が 9/12 に上書きされても、版の履歴の 9/11 を拾って止める', () => {
    assert.equal(db.prepare('SELECT posted_at FROM fact_yahoo_reviews WHERE order_number = ?').get(ORDER).posted_at, '2026-09-12 00:00:00', '上書きされたことの確認');
    assert.equal(db.prepare('SELECT COUNT(*) n FROM fact_yahoo_review_revisions').get().n, 2);
    const withoutHistory = db.prepare(`SELECT ${firstReviewDateSql(tablesFor('yahoo'), '?')} AS d`).get(ORDER).d;
    assert.equal(withoutHistory, '2026-09-12', '版の履歴を見なければ 9/12 = すり抜ける形になっていること (この試験が意味を持つ前提)');
    assert.equal(coupon().status, 'ready');
    const sel = S.selectEligibleActions(db, { nowIso: J('2026-09-13T12:50:00'), limit: 100 });
    assert.ok(sel.skipped.some((k) => k.id === coupon().id && k.reason === 'vendor_already_sent'), JSON.stringify(sel.skipped));
    assert.equal(S.claimActionGuarded(db, coupon().id, J('2026-09-13T12:50:00')).gateFailed, 'vendor_already_sent');
  });

  await t('取り消しスクリプトも同じ判定 (版の履歴あり) で数える', () => {
    const f = findVendorSentCoupons(db, { reviewsThrough: THROUGH });
    assert.equal(f.withRevisions, true);
    assert.deepEqual(f.ids, [coupon().id]);
    assert.deepEqual(f.byPostedDate, { '2026-09-11': 1 });
  });
  db.close();
}

console.log('\n衝突で今のレビュー行が消えても止める (本物のレビュー取込を通す・Codex R3)');
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cancel-coupons-conflict-'));
  const db = new Database(path.join(dir, 'warehouse.db'));
  db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
    order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
  db.exec('CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)');
  ensureYahooReviewTables(db);
  Y.ensureCampaignTables(db);
  const ORDER = 'b-faith01-10290002';
  addOrder(db, ORDER, '2026-09-03T10:00:00');
  const rowOf = (date, code, body) => [date, '5', `商品 ${code}`, code, ORDER, 'よい', body, '0', '0', '0'];
  const importRows = (rows, jst) => {
    const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
    const z = new AdmZip();
    z.addFile('20260801_20260913_ItemReview.csv', iconv.encode([HEADER_COLS, ...rows].map((r) => r.map(esc).join(',')).join('\r\n') + '\r\n', 'Shift_JIS'));
    const buf = z.toBuffer();
    const r = importYahooReviewFile(db, { name: '20260801_20260913_ItemReview.zip', buffer: buf, sha256: crypto.createHash('sha256').update(buf).digest('hex'), nowIso: J(jst) });
    assert.equal(r.status, 'ok', JSON.stringify(r.results));
  };
  const coupon = () => couponOf(db, ORDER);

  plan(db, '2026-09-12T12:30:00');
  cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 0, nowIso: J('2026-09-12T12:55:00') });
  Y.applyCutover(db, { cutoverAt: BOUNDARY, couponCutoverAt: BOUNDARY, nowIso: J('2026-09-12T13:00:00') });
  importRows([rowOf('20260911', 'item-a', 'よかった')], '2026-09-12T14:00:00');   // 商品 A の 9/11 投稿が遅れて入る
  plan(db, '2026-09-12T14:00:00');
  plan(db, '2026-09-13T12:10:00');
  // 同じ注文の商品 B に 9/12 投稿。商品 A は内容の違う 2 行 = 衝突として隔離され、fact 行が消える
  importRows([rowOf('20260912', 'item-b', 'また買う'), rowOf('20260911', 'item-a', 'よかった'), rowOf('20260911', 'item-a', '別の本文')], '2026-09-13T12:30:00');
  plan(db, '2026-09-13T12:40:00');

  await t('前提: 商品 A は衝突で fact から消え、今の行だけ・fact 経由の履歴だけなら最初の投稿日は商品 B の 9/12', () => {
    const facts = db.prepare('SELECT product_code, posted_at FROM fact_yahoo_reviews WHERE order_number = ?').all(ORDER);
    assert.deepEqual(facts, [{ product_code: 'item-b', posted_at: '2026-09-12 00:00:00' }]);
    assert.equal(db.prepare('SELECT COUNT(*) n FROM fact_yahoo_review_conflicts WHERE order_number = ?').get(ORDER).n, 1);
    const factOnly = { ...tablesFor('yahoo'), reviewConflicts: undefined };
    assert.equal(db.prepare(`SELECT ${firstReviewDateSql(factOnly, `'${ORDER}'`, { withRevisions: true })} AS d`).get().d, '2026-09-12',
      '衝突表を見なければすり抜ける形になっていること (この試験が意味を持つ前提)');
  });

  await t('🚨 衝突表から identity を引いて版の履歴の 9/11 を拾い、候補選定・送る直前の再判定・取り消しのすべてで止まる', () => {
    assert.equal(coupon().status, 'ready');
    const at = J('2026-09-13T12:50:00');
    const sel = S.selectEligibleActions(db, { nowIso: at, limit: 100 });
    assert.ok(sel.skipped.some((k) => k.id === coupon().id && k.reason === 'vendor_already_sent'), JSON.stringify(sel.skipped));
    assert.equal(S.claimActionGuarded(db, coupon().id, at).gateFailed, 'vendor_already_sent');
    assert.equal(coupon().status, 'ready');
    const f = findVendorSentCoupons(db, { reviewsThrough: THROUGH });
    assert.deepEqual(f.ids, [coupon().id]);
    assert.deepEqual(f.byPostedDate, { '2026-09-11': 1 });
  });
  db.close();
}

console.log('\n初めから衝突した行・衝突中に届いた古い日付も止める (本物のレビュー取込を通す・Codex R4)');
{
  const makeFull = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cancel-coupons-conflict2-'));
    const db = new Database(path.join(dir, 'warehouse.db'));
    db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
      order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
    db.exec('CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)');
    ensureYahooReviewTables(db);
    Y.ensureCampaignTables(db);
    return db;
  };
  const importRows = (db, rows, jst) => {
    const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
    const z = new AdmZip();
    z.addFile('20260801_20260913_ItemReview.csv', iconv.encode([HEADER_COLS, ...rows].map((r) => r.map(esc).join(',')).join('\r\n') + '\r\n', 'Shift_JIS'));
    const buf = z.toBuffer();
    const r = importYahooReviewFile(db, { name: '20260801_20260913_ItemReview.zip', buffer: buf, sha256: crypto.createHash('sha256').update(buf).digest('hex'), nowIso: J(jst) });
    assert.equal(r.status, 'ok', JSON.stringify(r.results));
  };
  const rowOf = (order, date, code, body) => [date, '5', `商品 ${code}`, code, order, 'よい', body, '0', '0', '0'];
  const cutover = (db) => {
    plan(db, '2026-09-12T12:30:00');
    cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 0, nowIso: J('2026-09-12T12:55:00') });
    Y.applyCutover(db, { cutoverAt: BOUNDARY, couponCutoverAt: BOUNDARY, nowIso: J('2026-09-12T13:00:00') });
  };
  const gateOf = (db, order, jst) => {
    const c = couponOf(db, order);
    const sel = S.selectEligibleActions(db, { nowIso: J(jst), limit: 100 });
    return { status: c?.status, skip: sel.skipped.find((k) => k.id === c?.id)?.reason ?? null, eligible: sel.eligible.some((e) => e.id === c?.id) };
  };

  await t('🚨 商品 A が初めから「9/11 投稿の違う 2 行」で衝突し、商品 B が 9/12 投稿 → 最初の投稿日は 9/11 で止まる', () => {
    const db = makeFull(); const ORDER = 'b-faith01-10290003';
    addOrder(db, ORDER, '2026-09-03T10:00:00');
    cutover(db);
    importRows(db, [rowOf(ORDER, '20260911', 'item-a', 'よかった'), rowOf(ORDER, '20260911', 'item-a', '別の本文'), rowOf(ORDER, '20260912', 'item-b', 'また買う')], '2026-09-12T14:00:00');
    plan(db, '2026-09-12T14:00:00'); plan(db, '2026-09-13T12:10:00');
    assert.equal(db.prepare('SELECT COUNT(*) n FROM fact_yahoo_review_conflicts WHERE order_number = ?').get(ORDER).n, 1, 'A は初めから衝突');
    const g = gateOf(db, ORDER, '2026-09-13T12:20:00');
    assert.deepEqual(g, { status: 'ready', skip: 'vendor_already_sent', eligible: false });
    assert.equal(S.claimActionGuarded(db, couponOf(db, ORDER).id, J('2026-09-13T12:20:00')).gateFailed, 'vendor_already_sent');
    assert.deepEqual(findVendorSentCoupons(db, { reviewsThrough: THROUGH }).byPostedDate, { '2026-09-11': 1 });
    db.close();
  });

  await t('🚨 衝突中の商品 A に、別のファイルで古い 9/11 投稿が届いたら、その日付で止まる (届く前は止めない)', () => {
    const db = makeFull(); const ORDER = 'b-faith01-10290004';
    addOrder(db, ORDER, '2026-09-03T10:00:00');
    cutover(db);
    importRows(db, [rowOf(ORDER, '20260912', 'item-a', 'よかった'), rowOf(ORDER, '20260912', 'item-a', '別の本文'), rowOf(ORDER, '20260912', 'item-b', 'また買う')], '2026-09-12T14:00:00');
    plan(db, '2026-09-12T14:00:00'); plan(db, '2026-09-13T12:10:00');
    assert.deepEqual(gateOf(db, ORDER, '2026-09-13T12:20:00'), { status: 'ready', skip: null, eligible: true }, '9/12 投稿だけなら vendor は送っていない → 送る');
    importRows(db, [rowOf(ORDER, '20260911', 'item-a', '古い本文')], '2026-09-13T12:15:00');   // 検証済みスナップショットでない = 衝突は隔離のまま
    assert.equal(db.prepare('SELECT COUNT(*) n FROM fact_yahoo_review_conflicts WHERE order_number = ?').get(ORDER).n, 1, '衝突は隔離のまま');
    assert.equal(gateOf(db, ORDER, '2026-09-13T12:20:00').skip, 'vendor_already_sent');
    db.close();
  });
}

console.log('\n巻き戻し・入力の検証');

await t('🚨 書き換えの途中で失敗したら、先に書いた行も日付も全部巻き戻す', () => {
  const { db } = makeDb();
  addOrder(db, 'P', '2026-09-03T10:00:00'); addOrder(db, 'Q', '2026-09-04T10:00:00');
  addReview(db, 'P', '2026-09-10', '2026-09-11T01:00:00'); addReview(db, 'Q', '2026-09-10', '2026-09-11T01:00:00');
  plan(db, '2026-09-11T02:00:00'); plan(db, '2026-09-11T12:30:00');
  const q = couponOf(db, 'Q').id;
  db.exec(`CREATE TRIGGER boom BEFORE UPDATE OF status ON yahoo_campaign_actions WHEN NEW.id = ${q} BEGIN SELECT RAISE(ABORT, 'boom'); END`);
  assert.throws(() => cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 2, nowIso: J('2026-09-12T12:55:00') }), /boom/);
  assert.equal(couponOf(db, 'P').status, 'ready', '先に書いた P も戻っている');
  assert.equal(meta(db, VENDOR_COUPON_THROUGH_KEY), null);
  db.close();
});

await t('送信ゲートの判定: 日付が記録されていないモール (楽天など) は止めない / 等号は vendor 側 / フォローは対象外', () => {
  assert.equal(vendorCouponCovered({ first_review_posted_at: '2026-09-10 00:00:00', vendor_coupon_reviews_through: null }), false);
  assert.equal(vendorCouponCovered({ first_review_posted_at: '2026-09-11 23:59:59', vendor_coupon_reviews_through: '2026-09-11' }), true);
  assert.equal(vendorCouponCovered({ first_review_posted_at: '2026-09-12 00:00:00', vendor_coupon_reviews_through: '2026-09-11' }), false);
  assert.equal(vendorCouponCovered({ first_review_posted_at: null, vendor_coupon_reviews_through: '2026-09-11' }), false);
  const base = {
    status: 'ready', scheduled_at: '2026-09-13T03:00:00.000Z', expires_at: '2026-09-30T00:00:00.000Z', owner: 'self', coupon_owner: 'self',
    masked_email_enc: 'enc', shipping_datetime: '2026-09-03T10:00:00+09:00', has_active_review: 1, has_review_any: 0,
    first_review_posted_at: '2026-09-10 00:00:00', vendor_coupon_reviews_through: '2026-09-11',
  };
  assert.equal(gateReason({ ...base, action_type: 'coupon' }, { nowIso: '2026-09-13T03:20:00.000Z', couponUsable: true }), 'vendor_already_sent');
  assert.equal(gateReason({ ...base, action_type: 'coupon', vendor_coupon_reviews_through: null }, { nowIso: '2026-09-13T03:20:00.000Z', couponUsable: true }), null);
});

await t('--reviews-through は実在する日付で、今日以前・14 日前以後だけ受け付ける', () => {
  const now = J('2026-09-12T12:55:00');
  assert.equal(validateReviewsThrough('2026-09-11', now), null);
  assert.equal(validateReviewsThrough('2026-09-12', now), null);
  assert.match(validateReviewsThrough('2026-09-13', now), /未来/);
  assert.match(validateReviewsThrough('2026-08-20', now), /古すぎる/);
  assert.match(validateReviewsThrough('2026-02-30', now), /実在/);
  assert.match(validateReviewsThrough('9/11', now), /実在/);
});

await t('--expect が整数でなければ受け付けない', () => {
  const { db } = makeDb();
  assert.throws(() => cancelVendorSentCoupons(db, { reviewsThrough: THROUGH, expect: 1.5, nowIso: J('2026-09-12T12:55:00') }), (e) => e.code === 'BAD_EXPECT');
  db.close();
});

console.log(`\n${passed} 件 PASS`);
