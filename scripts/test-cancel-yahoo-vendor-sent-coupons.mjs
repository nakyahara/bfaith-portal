/**
 * test-cancel-yahoo-vendor-sent-coupons.mjs — 切り替え直後のクーポン取り消しの受入試験
 *
 * 一時 SQLite に最小の表 (meta / actions / contacts / ownership) を作り、どの行が取り消され・どの行が残るかを見る。
 * 本番の DB には触らない。
 * 使い方: node scripts/test-cancel-yahoo-vendor-sent-coupons.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {
  findVendorSentCoupons, cancelVendorSentCoupons, currentCutover, REASON,
} from '../apps/warehouse/cancel-yahoo-vendor-sent-coupons.js';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }

const BOUNDARY = '2026-09-02T14:59:59.000Z';    // 2026-09-02T23:59:59+09:00
const VENDOR_LAST = '2026-09-12T03:45:00.000Z'; // 2026-09-12T12:45:00+09:00
const NOW = '2026-09-12T04:00:00.000Z';         // 2026-09-12T13:00:00+09:00

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cancel-coupons-'));
  const db = new Database(path.join(dir, 'warehouse.db'));
  db.exec(`
    CREATE TABLE yahoo_campaign_meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, shipping_datetime TEXT);
    CREATE TABLE yahoo_order_campaign_ownership (
      order_number TEXT PRIMARY KEY, owner TEXT NOT NULL, coupon_owner TEXT, reason TEXT, decided_at TEXT);
    CREATE TABLE yahoo_campaign_actions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action_type TEXT NOT NULL CHECK (action_type IN ('follow','coupon')),
      dedupe_key TEXT NOT NULL UNIQUE,
      order_number TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('planned','suppressed','ready','claimed','sent','failed_safe','ambiguous','cancelled','expired')),
      status_reason TEXT,
      scheduled_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );`);
  return { db, dir };
}
let seq = 0;
const addOrder = (db, order, shipIso) => db.prepare('INSERT INTO yahoo_order_contacts VALUES (?, ?)').run(order, shipIso);
const addAction = (db, o) => db.prepare(`INSERT INTO yahoo_campaign_actions
  (action_type, dedupe_key, order_number, status, scheduled_at, expires_at, updated_at) VALUES (?,?,?,?,?,?,?)`)
  .run(o.type || 'coupon', `k${++seq}`, o.order, o.status || 'ready', o.scheduled, o.expires || '2026-09-30T14:59:59.000Z', '2026-09-01T00:00:00.000Z').lastInsertRowid;
const own = (db, order, owner, couponOwner = owner) =>
  db.prepare(`INSERT OR REPLACE INTO yahoo_order_campaign_ownership VALUES (?, ?, ?, 'cutover_shipping', ?)`).run(order, owner, couponOwner, NOW);

/** applyCutoverT と同じ形にする: meta に境目 + 担当表 (発送が境目より後 = self)。STICKY は前から vendor に決まっていた注文 */
const goLive = (db, at = BOUNDARY) => {
  db.prepare(`INSERT INTO yahoo_campaign_meta VALUES ('cutover_at', ?)`).run(at);
  db.prepare(`INSERT INTO yahoo_campaign_meta VALUES ('coupon_cutover_at', ?)`).run(at);
  for (const c of db.prepare('SELECT * FROM yahoo_order_contacts WHERE shipping_datetime IS NOT NULL').all()) {
    const self = Date.parse(c.shipping_datetime) > Date.parse(at);
    own(db, c.order_number, self ? 'self' : 'vendor');
  }
  own(db, 'STICKY', 'vendor');   // 一度決めた担当は覆らない (INSERT OR IGNORE) → 発送が境目より後でも vendor
};

// 取り消す / 残す の見本
function seed(db) {
  addOrder(db, 'SELF-1', '2026-09-03T02:00:00.000Z');   // 境目より後に発送 = うち側
  addOrder(db, 'SELF-2', '2026-09-04T02:00:00.000Z');
  addOrder(db, 'VEND-1', '2026-09-01T02:00:00.000Z');   // 境目より前 = vendor 側
  addOrder(db, 'STICKY', '2026-09-05T02:00:00.000Z');   // 発送は境目より後だが、担当は vendor に決まっている
  addOrder(db, 'NOSHIP', null);
  return {
    // 取り消す: うち側・coupon・ready/planned・予定が vendor 最終送信以前・期限内
    cancelReady: addAction(db, { order: 'SELF-1', scheduled: '2026-09-08T03:00:00.000Z' }),
    cancelPlanned: addAction(db, { order: 'SELF-2', status: 'planned', scheduled: '2026-09-12T03:00:00.000Z' }),
    // 残す
    keepAfterVendor: addAction(db, { order: 'SELF-1', scheduled: '2026-09-13T03:00:00.000Z' }),   // vendor が止まったあとの予定
    keepVendorSide: addAction(db, { order: 'VEND-1', scheduled: '2026-09-08T03:00:00.000Z' }),   // vendor 側の注文
    keepSticky: addAction(db, { order: 'STICKY', scheduled: '2026-09-08T03:00:00.000Z' }),       // 担当表で vendor = うちは送らない
    keepFollow: addAction(db, { type: 'follow', order: 'SELF-1', scheduled: '2026-09-08T03:00:00.000Z' }),
    keepSent: addAction(db, { order: 'SELF-2', status: 'sent', scheduled: '2026-09-08T03:00:00.000Z' }),
    keepClaimed: addAction(db, { order: 'SELF-2', status: 'claimed', scheduled: '2026-09-08T03:00:00.000Z' }),
    keepExpired: addAction(db, { order: 'SELF-1', scheduled: '2026-09-07T03:00:00.000Z', expires: '2026-09-12T03:00:00.000Z' }),  // もう期限切れ
    keepNoShip: addAction(db, { order: 'NOSHIP', scheduled: '2026-09-08T03:00:00.000Z' }),
  };
}
const ACTION_COUNT = 10;
const statusOf = (db, id) => db.prepare('SELECT status, status_reason FROM yahoo_campaign_actions WHERE id = ?').get(id);
const find = (db) => findVendorSentCoupons(db, { boundaryIso: BOUNDARY, vendorLastSendIso: VENDOR_LAST, nowIso: NOW });
const cancel = (db, expect) => cancelVendorSentCoupons(db, { boundaryIso: BOUNDARY, vendorLastSendIso: VENDOR_LAST, expect, nowIso: NOW });

console.log('どの行を取り消すか');

t('切り替え後: 担当表でうち・未送信・vendor が送ったはずの予定・期限内 のクーポンだけを数える', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db);
  const f = find(db);
  assert.equal(f.basis, 'ownership');
  assert.deepEqual([...f.ids].sort(), [ids.cancelReady, ids.cancelPlanned].sort());
  assert.deepEqual(f.byDate, { '2026-09-08': 1, '2026-09-12': 1 }, '予定日は JST で数える');
  db.close();
});

t('🚨 担当表で vendor に決まっている注文は、発送が境目より後でも取り消さない (送信側と同じ判定)', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db);
  assert.ok(!find(db).ids.includes(ids.keepSticky));
  db.close();
});

t('切り替え前の試しは「発送が境目より後」で見込みを数える (担当表はまだ全部 vendor)', () => {
  const { db } = freshDb(); const ids = seed(db);
  const f = find(db);
  assert.equal(f.basis, 'shipping_preview');
  assert.deepEqual([...f.ids].sort(), [ids.cancelReady, ids.cancelPlanned, ids.keepSticky].sort(),
    '見込みは cutoverPreview と同じ決め方 (担当が覆らない注文までは分からない)');
  db.close();
});

t('試しは何も書き換えない', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db);
  find(db);
  assert.equal(statusOf(db, ids.cancelReady).status, 'ready');
  db.close();
});

console.log('\n取り消し');

t('live で --expect が合えば、該当の行だけ cancelled になる (消さない)', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db);
  const r = cancel(db, 2);
  assert.equal(r.cancelled, 2);
  assert.deepEqual(statusOf(db, ids.cancelReady), { status: 'cancelled', status_reason: REASON });
  assert.deepEqual(statusOf(db, ids.cancelPlanned), { status: 'cancelled', status_reason: REASON });
  for (const k of ['keepAfterVendor', 'keepVendorSide', 'keepSticky', 'keepFollow', 'keepSent', 'keepClaimed', 'keepExpired', 'keepNoShip']) {
    assert.notEqual(statusOf(db, ids[k]).status, 'cancelled', `${k} は残す`);
  }
  assert.equal(statusOf(db, ids.keepClaimed).status, 'claimed', '🚨 送り始めた行 (claimed) には触らない');
  assert.equal(db.prepare('SELECT COUNT(*) n FROM yahoo_campaign_actions').get().n, ACTION_COUNT, '行は消さない');
  db.close();
});

t('🚨 --expect と件数が 1 件でも違えば、何も書き換えずに止まる', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db);
  assert.throws(() => cancel(db, 3), (e) => e.code === 'EXPECT_MISMATCH');
  assert.equal(statusOf(db, ids.cancelReady).status, 'ready');
  assert.equal(statusOf(db, ids.cancelPlanned).status, 'planned');
  db.close();
});

t('🚨 切り替え前の見込みの件数を --expect に渡しても、live では担当表で数え直して合わなければ止まる', () => {
  const { db } = freshDb(); const ids = seed(db);
  const preview = find(db).total;   // 3 (STICKY を含む見込み)
  goLive(db);
  assert.throws(() => cancel(db, preview), (e) => e.code === 'EXPECT_MISMATCH');
  assert.equal(statusOf(db, ids.cancelReady).status, 'ready');
  db.close();
});

t('🚨 まだ切り替えていない (shadow) なら書き換えない', () => {
  const { db } = freshDb(); const ids = seed(db);
  assert.equal(currentCutover(db).stage, 'shadow');
  assert.throws(() => cancel(db, 3), (e) => e.code === 'NOT_LIVE');
  assert.equal(statusOf(db, ids.cancelReady).status, 'ready');
  db.close();
});

t('🚨 cutover の境目と違う境目を渡したら書き換えない (取り違えの歯止め)', () => {
  const { db } = freshDb(); const ids = seed(db); goLive(db, '2026-08-31T14:59:59.000Z');
  assert.throws(() => cancel(db, 2), (e) => e.code === 'BOUNDARY_MISMATCH');
  assert.equal(statusOf(db, ids.cancelReady).status, 'ready');
  db.close();
});

t('2 回目は 0 件 (もう取り消し済みなので何も起きない)', () => {
  const { db } = freshDb(); seed(db); goLive(db);
  cancel(db, 2);
  assert.equal(find(db).total, 0);
  assert.equal(cancel(db, 0).cancelled, 0);
  db.close();
});

t('--expect が整数でなければ受け付けない', () => {
  const { db } = freshDb(); seed(db); goLive(db);
  assert.throws(() => cancel(db, 1.5), (e) => e.code === 'BAD_EXPECT');
  db.close();
});

console.log(`\n${passed} 件 PASS`);
