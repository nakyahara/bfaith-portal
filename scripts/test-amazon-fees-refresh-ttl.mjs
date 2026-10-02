#!/usr/bin/env node
/**
 * test-amazon-fees-refresh-ttl.mjs — Amazon手数料のキャッシュを「期限で取り直す」決まりの試験と模擬 (2026-10-02)。
 *   🚨 起きていたこと: 取り直し (fetch-amazon-fees.js・毎朝 07 時台) と監視 (monitor-fee-coverage.js・08 時台) が同じ 168 時間を使っていた →
 *      7 日前の朝に取った SKU は、取り直しの時点では 167.9 時間 = 見送り・監視の時点では 168.x 時間 = 鮮度切れ。取り直しは実質 8 日ごと。
 *      同じ朝に取ったかたまり (10/1 = 1,567 件) が毎週 1 回監視に引っかかる (10/2 = 9/25 の 445 件で 94.24% = critical)。
 *   いまの決まり: SKU ごとの枠の日 (6 日周期・sha256(seller_sku)) に取り直す + 保険の境 132 時間。監視の 168 時間は変えない。
 *   ここで確かめること: 同じ SKU は毎回同じ枠 / 境の手前・後 / 168 時間の監視で鮮度切れにならない / 価格・ASIN・チャネルの条件と --force は変わらない /
 *   10/2 の本番の分布を入れて毎朝の取り直しを 4 週間回す模擬 (本物の filterByTTLAndDiff・メモリ上の SQLite) で、かたまりが散り・監視の鮮度切れが 0。
 *
 *   node scripts/test-amazon-fees-refresh-ttl.mjs          試験だけ
 *   node scripts/test-amazon-fees-refresh-ttl.mjs --table  模擬の表も出す (PR に載せた表。旧の 168 時間・依頼の例 (144 時間 − 0〜24 時間) と並べる)
 *
 * 🚨 試験に無いもの: SP-API への本物の要求・本番の warehouse.db・daily-sync.js の実行 (時間の上限はソースの数字だけ見る)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import {
  fetchAmazonFees, filterByTTLAndDiff, ttlRefreshReason, refreshSlotOf, jstDayIndex, makeBatches,
  REFRESH_CYCLE_DAYS, REFRESH_HARD_HOURS, REFRESH_SLOT_MIN_AGE_HOURS,
} from '../apps/warehouse/fetch-amazon-fees.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const H = 3600 * 1000;

// ─── 監視の 168 時間・daily-sync の時間の上限は、ソースの数字を読む (ここに写さない = 片方だけ変わったら試験が気づく) ───
const monitorSrc = fs.readFileSync(path.join(root, 'apps/warehouse/monitor-fee-coverage.js'), 'utf8');
const mm = monitorSrc.match(/^const TTL_HOURS = (\d+) \* (\d+);/m);
assert.ok(mm, 'monitor-fee-coverage.js の TTL_HOURS が読めない');
const MONITOR_STALE_HOURS = Number(mm[1]) * Number(mm[2]);
const dailySrc = fs.readFileSync(path.join(root, 'apps/warehouse/daily-sync.js'), 'utf8');
const dm = dailySrc.match(/'apps\/warehouse\/fetch-amazon-fees\.js --recent 30',\s*'Amazon手数料 \(--recent 30\)',\s*(\d+)/);
assert.ok(dm, 'daily-sync.js の Amazon手数料 の時間の上限が読めない');
const DAILY_SYNC_TIMEOUT_SEC = Number(dm[1]) / 1000;

// ─── 時刻の道具 ───
const jstMs = (ymd, hh = 0, mi = 0, ss = 0) => { const [y, m, d] = ymd.split('-').map(Number); return Date.UTC(y, m - 1, d, hh - 9, mi, ss); };
const utcStr = (ms) => new Date(ms).toISOString().replace('T', ' ').slice(0, 19);   // saveFees の now() と同じ形 (UTC・秒まで)
const floorSec = (ms) => Math.floor(ms / 1000) * 1000;
const addDays = (ymd, n) => { const d = new Date(ymd + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
/** 枠が「その日」になる SKU / ならない SKU を名前の候補から探す */
const findSku = (prefix, dayIdx, wantSlotDay) => {
  for (let i = 0; i < 1000; i++) { const s = `${prefix}${i}`; if ((refreshSlotOf(s) === dayIdx % REFRESH_CYCLE_DAYS) === wantSlotDay) return s; }
  throw new Error('候補が見つからない');
};

// ─── メモリ上の warehouse.db (fetch-amazon-fees.js が読む・書く表だけ。列は本番と同じ) ───
function openDb() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_sp_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_order_id TEXT, purchase_date TEXT, order_status TEXT, fulfillment_channel TEXT, asin TEXT, seller_sku TEXT, quantity INTEGER, item_price REAL);
    CREATE TABLE amazon_sku_fees (seller_sku TEXT PRIMARY KEY, asin TEXT, fulfillment_channel TEXT, referral_fee REAL, referral_fee_rate REAL, fba_fee REAL, variable_closing_fee REAL, per_item_fee REAL, total_fee REAL, price_used REAL, fetched_at TEXT);
    CREATE TABLE m_sku_master (seller_sku TEXT PRIMARY KEY, 商品名 TEXT)`);
  return db;
}
const putCache = (db, sku, fetchedMs, { asin = `B0${sku}`, channel = 'FBA', price = 1000 } = {}) => (db._put ||= db.prepare(`insert or replace into amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee, total_fee, price_used, fetched_at) values (?, ?, ?, 100, 400, ?, ?)`))
  .run(sku, asin, channel, price, fetchedMs == null ? null : utcStr(fetchedMs));
const item = (sku, { asin = `B0${sku}`, channel = 'FBA', price = 1000 } = {}) => ({ seller_sku: sku, asin, channel, last_price: price });

// ═══ 模擬: 10/2 の本番の分布を入れて、毎朝の取り直しと 1.5 時間後の監視を回す ═══
// 取った時刻の分布 (JST・07 時台。直近 30 日に売れた = --recent 30 の対象 = 監視の対象)。9/22〜9/24 の 86 件と、それより古い 1,366 件は直近 30 日に売れていないので入れない
const INITIAL = [['2026-09-25', 445], ['2026-09-26', 62], ['2026-09-27', 298], ['2026-09-28', 99], ['2026-09-29', 317], ['2026-09-30', 23], ['2026-10-01', 1567], ['2026-10-02', 270]];
const CLUMP = '2026-10-01';
const SEC_PER_BATCH = 2.5;   // 20 件 = 1 回の要求。2.1 秒の待ち (BATCH_SLEEP_MS) + 応答の時間 (見積り 0.4 秒)
const MONITOR_DELAY_MIN = 90;  // 監視は取り直しの 1.5 時間後
/** その朝の取り直しが始まる時刻 = 07:10〜07:49 JST (daily-sync は 07:00 起動・前のステップの長さで毎朝ずれる)。日付から決まる (毎回同じ) */
const startMinute = (ymd) => 10 + createHash('sha256').update('drift:' + ymd).digest().readUInt32BE(0) % 40;

const POLICIES = {
  /** いまの決まり = 本物の filterByTTLAndDiff (メモリ上の SQLite を読む) */
  new: (db, pop, fetched, nowMs) => filterByTTLAndDiff(db, pop.map((p) => ({ ...item(p.sku), _p: p })), nowMs).needRefresh.map((x) => x._p),
  /** 旧 = 経過 ≥ 168 時間 (監視と同じ) */
  old: (db, pop, fetched, nowMs) => pop.filter((p) => (nowMs - fetched.get(p.sku)) / H >= 168),
  /** 依頼の例 = 144 時間 − SKU ごとのずれ 0〜24 時間 */
  example: (db, pop, fetched, nowMs) => pop.filter((p) => (nowMs - fetched.get(p.sku)) / H >= 144 - (createHash('sha256').update('ex:' + p.sku).digest().readUInt32BE(0) % 1440) / 60),
};

function simulate(policy, { from = '2026-10-03', days = 28, missed = [] } = {}) {
  const pop = [];
  for (const [ymd, n] of INITIAL) for (let i = 0; i < n; i++) pop.push({ sku: `SIM-${ymd.slice(5).replace('-', '')}-${String(i).padStart(4, '0')}`, origin: ymd });
  const db = openDb();
  // 模擬を速くするための索引だけ (本番の表に無い。filterByTTLAndDiff の COLLATE NOCASE の引き当てが毎回 表全体を読まないように。判定は変わらない)
  db.exec('CREATE INDEX sim_asf_nocase ON amazon_sku_fees(seller_sku COLLATE NOCASE)');
  const fetched = new Map();
  for (const [ymd] of INITIAL) {
    makeBatches(pop.filter((p) => p.origin === ymd)).forEach((b, bi) => {
      const ts = floorSec(jstMs(ymd, 7, startMinute(ymd)) + (bi + 1) * SEC_PER_BATCH * 1000);
      for (const p of b) { fetched.set(p.sku, ts); putCache(db, p.sku, ts, { asin: `B0${p.sku}` }); }
    });
  }
  const upd = db.prepare('update amazon_sku_fees set fetched_at = ? where seller_sku = ?');
  const rows = [];
  for (let k = 0; k < days; k++) {
    const ymd = addDays(from, k);
    const start = jstMs(ymd, 7, startMinute(ymd));
    const ran = !missed.includes(ymd);
    const need = ran ? POLICIES[policy](db, pop, fetched, start) : [];
    // saveFees は batch ごとに保存した時刻を書く
    db.transaction(() => makeBatches(need).forEach((b, bi) => {
      const ts = floorSec(start + (bi + 1) * SEC_PER_BATCH * 1000);
      for (const p of b) { fetched.set(p.sku, ts); upd.run(utcStr(ts), p.sku); }
    }))();
    const mon = start + MONITOR_DELAY_MIN * 60 * 1000;
    let stale = 0, maxAge = 0;
    for (const p of pop) { const age = (mon - fetched.get(p.sku)) / H; if (age >= MONITOR_STALE_HOURS) stale++; if (age > maxAge) maxAge = age; }
    rows.push({ ymd, ran, refreshed: need.length, clump: need.filter((p) => p.origin === CLUMP).length, stale, maxAge, sec: Math.ceil(need.length / 20) * SEC_PER_BATCH });
  }
  return { rows, total: pop.length };
}

// ═══ 試験 ═══
console.log('Amazon手数料: 期限で取り直す決まり (境とずらし)');

await t('定数の根拠: 境 132 時間は「前の日 (5 日目) の朝には当たらず・6 日目の朝には当たる」を ±12 時間のずれまで守る / 枠の日の最小の経過 12 時間は同じ日の再試行 (最後 11:30 = 4.5 時間後) で二度取りせず・前の日の朝 (07:50 まで) に取ったものは当てる / 監視の 168 時間より 1 日以上手前', async () => {
  assert.equal(MONITOR_STALE_HOURS, 168, '監視の 168 時間は変えない');
  assert.equal(REFRESH_CYCLE_DAYS, 6);
  const DRIFT = 12;   // 毎朝の取り直しの時刻のずれの許容 (時間)
  assert.ok(REFRESH_HARD_HOURS - (REFRESH_CYCLE_DAYS - 1) * 24 >= DRIFT, '枠どおりに回っている SKU を前の日に境で取らない');
  assert.ok(REFRESH_CYCLE_DAYS * 24 - REFRESH_HARD_HOURS >= DRIFT, '6 日目の朝には境に当たる');
  assert.ok(MONITOR_STALE_HOURS - REFRESH_HARD_HOURS >= 24 + DRIFT / 2, '1 朝取り損ねても次の朝 (7 日目・監視の前) に境で取る');
  assert.ok(REFRESH_SLOT_MIN_AGE_HOURS > 4.5, '同じ日の再試行 (08:30 / 10:00 / 11:30) で、その朝に取った SKU を二度取りしない');
  assert.ok(REFRESH_SLOT_MIN_AGE_HOURS <= 24 - DRIFT, '前の日の朝に取った SKU は枠の日に取り直す (枠にそろう)');
});

await t('ずらしは決定的: 同じ SKU は何度呼んでも同じ枠・大文字小文字だけ違う SKU も同じ枠 (表は COLLATE NOCASE で結ぶ)・枠は 0〜5。3,081 件で各枠 0.8〜1.2 × 1/6 に散る', async () => {
  for (const s of ['pr_abc001', 'SIM-1001-0001', 'ｶﾀｶﾅ-SKU', '']) {
    const a = refreshSlotOf(s);
    for (let i = 0; i < 5; i++) assert.equal(refreshSlotOf(s), a);
    assert.ok(Number.isInteger(a) && a >= 0 && a < REFRESH_CYCLE_DAYS);
  }
  assert.equal(refreshSlotOf('PR_ABC001'), refreshSlotOf('pr_abc001'));
  const cnt = new Array(REFRESH_CYCLE_DAYS).fill(0);
  for (let i = 0; i < 3081; i++) cnt[refreshSlotOf(`pr_${String(i).padStart(6, '0')}`)]++;
  for (const c of cnt) assert.ok(c >= 0.8 * 3081 / 6 && c <= 1.2 * 3081 / 6, `枠の偏り ${cnt}`);
});

await t('JST の日付の通し番号: 07 時台の取得と 08:30〜11:30 の再試行と 23:59 は同じ日・翌 00:00 JST で次の日 (UTC の日付ではない)', async () => {
  const d = jstDayIndex(jstMs('2026-10-02', 7, 5));
  assert.equal(jstDayIndex(jstMs('2026-10-02', 0, 0)), d);
  assert.equal(jstDayIndex(jstMs('2026-10-02', 11, 30)), d);
  assert.equal(jstDayIndex(jstMs('2026-10-02', 23, 59, 59)), d);
  assert.equal(jstDayIndex(jstMs('2026-10-03', 0, 0)), d + 1);
  assert.equal(jstDayIndex(Date.UTC(2026, 9, 1, 23, 0)), d, 'UTC 10/1 23:00 = JST 10/2 08:00');
});

await t('境の手前/後 (ttlRefreshReason): 枠の日でない日は 131.99 時間 = 取らない・132 時間 = ttl_132.0h / 枠の日は 11.99 時間 = 取らない・12 時間 = slot_12.0h / 経過が読めない (NULL・NaN) は旧版と同じく期限では取らない', async () => {
  const now = jstMs('2026-10-05', 7, 20);
  const day = jstDayIndex(now);
  const off = findSku('off-', day, false), on = findSku('on-', day, true);
  assert.equal(ttlRefreshReason(off, 131.99, now), null);
  assert.equal(ttlRefreshReason(off, 132, now), 'ttl_132.0h');
  assert.equal(ttlRefreshReason(off, 167.9, now), 'ttl_167.9h', '旧版が見送っていた 167.9 時間は取る');
  assert.equal(ttlRefreshReason(off, 100, now), null);
  assert.equal(ttlRefreshReason(on, 11.99, now), null);
  assert.equal(ttlRefreshReason(on, 12, now), 'slot_12.0h');
  assert.equal(ttlRefreshReason(on, 100, now), 'slot_100.0h');
  assert.equal(ttlRefreshReason(on, 140, now), 'ttl_140.0h', '境を過ぎていれば理由は ttl');
  for (const bad of [null, undefined, NaN]) { assert.equal(ttlRefreshReason(off, bad, now), null); assert.equal(ttlRefreshReason(on, bad, now), null); }
  // 枠の日は同じ SKU で 6 日ごと
  const days = [];
  for (let k = 0; k < 18; k++) if (ttlRefreshReason(on, 50, now + k * 24 * H)) days.push(k);
  assert.deepEqual(days, [0, 6, 12]);
});

await t('filterByTTLAndDiff (メモリ上の SQLite): 期限は 132 時間の境と枠の日で取り直す / 価格 20% 超・ASIN・チャネルの変化・キャッシュ無しの条件と理由の文字は変わらない / SKU の大文字小文字が違っても同じ行を読む', async () => {
  const now = jstMs('2026-10-05', 7, 20);
  const day = jstDayIndex(now);
  const db = openDb();
  const off = (n) => findSku(`f${n}-`, day, false), on = (n) => findSku(`f${n}-`, day, true);
  const cases = [
    [off(1), now - 131.9 * H, {}, null],
    [off(2), now - 132.1 * H, {}, /^ttl_132\.1h$/],
    [off(3), now - 167.9 * H, {}, /^ttl_167\.9h$/],
    [on(4), now - 13 * H, {}, /^slot_13\.0h$/],
    [on(5), now - 11 * H, {}, null],
    [off(6), now - 2 * H, { price: 1250 }, /^price_diff_25pct$/],
    [off(7), now - 2 * H, { price: 1150 }, null],
    [off(8), now - 2 * H, { asin: 'B0OTHER' }, /^asin_changed$/],
    [off(9), now - 2 * H, { channel: 'FBM' }, /^channel_changed$/],
  ];
  for (const [sku, ms] of cases) putCache(db, sku, ms);
  const items = cases.map(([sku, , chg]) => item(sku, { asin: chg.asin || `B0${sku}`, channel: chg.channel || 'FBA', price: chg.price || 1000 }));
  items.push(item('never-cached'));
  const { needRefresh, skipped, skipReasons } = filterByTTLAndDiff(db, items, now);
  const reasonOf = Object.fromEntries(needRefresh.map((x) => [x.seller_sku, x.refresh_reason]));
  for (const [sku, , , want] of cases) {
    if (want) assert.match(reasonOf[sku] || '', want, sku);
    else assert.equal(reasonOf[sku], undefined, `${sku} は取らない`);
  }
  assert.equal(reasonOf['never-cached'], 'not_cached');
  assert.equal(skipped.length, 3); assert.equal(skipReasons.ttl_fresh, 3);
  // 大文字小文字だけ違う SKU (注文側が大文字) も同じキャッシュ行を読む = 同じ判定
  const upper = filterByTTLAndDiff(db, [item(off(2).toUpperCase(), { asin: `B0${off(2)}` })], now).needRefresh;
  assert.match(upper[0]?.refresh_reason || '', /^ttl_132\.1h$/);
});

await t('fetchAmazonFees を通しで (DB = メモリ・API = 差し替え): --recent は期限の来た SKU だけ取り直す・--force は今までどおり全部', async () => {
  const now = Date.now();
  const day = jstDayIndex(now);
  const today = new Date(now).toISOString().slice(0, 10);
  const db = openDb();
  const skus = { fresh: findSku('e1-', day, false), hard: findSku('e2-', day, false), slot: findSku('e3-', day, true) };
  for (const s of Object.values(skus)) db.prepare(`insert into raw_sp_orders (amazon_order_id, purchase_date, order_status, fulfillment_channel, asin, seller_sku, quantity, item_price) values (?, ?, 'Shipped', 'Amazon', ?, ?, 1, 1000)`).run(`o-${s}`, `${today}T00:00:00Z`, `B0${s}`, s);
  putCache(db, skus.fresh, now - 100 * H); putCache(db, skus.hard, now - 140 * H); putCache(db, skus.slot, now - 30 * H);
  const calls = [];
  const api = async (items) => { calls.push(...items.map((x) => `${x.seller_sku}:${x.refresh_reason}`)); return { results: items.map((it) => ({ seller_sku: it.seller_sku, asin: it.asin, channel: it.channel, referralFee: 100, referralFeeRate: 0.1, fbaFee: 300, variableClosingFee: 0, perItemFee: 0, totalFee: 400, price_used: it.last_price, refresh_reason: it.refresh_reason })), errors: [] }; };
  const r = await fetchAmazonFees('recent', 30, { db, fetchBatch: api, sleepFn: async () => {}, nowMs: now });
  assert.deepEqual([r.refreshed, r.skipped, r.failed], [2, 1, 0]);
  assert.deepEqual(calls.map((c) => c.replace(/\d+\.\d+h$/, 'Nh')).sort(), [`${skus.hard}:ttl_Nh`, `${skus.slot}:slot_Nh`].sort());
  calls.length = 0;
  const f = await fetchAmazonFees('recent', 30, { db, fetchBatch: api, sleepFn: async () => {}, nowMs: now, force: true });
  assert.equal(f.refreshed, 3);
  assert.deepEqual(calls.map((c) => c.split(':')[1]), ['force', 'force', 'force']);
});

const sim = { new: simulate('new'), old: simulate('old'), example: simulate('example') };

await t('🚨 模擬 (10/2 の分布 3,081 件・10/3〜10/30 の 28 朝・本物の filterByTTLAndDiff): 監視の時刻 (取り直しの 1.5 時間後) の鮮度切れは毎朝 0・最大の経過も 168 時間より 1 日近く手前', async () => {
  const { rows } = sim.new;
  for (const r of rows) assert.equal(r.stale, 0, `${r.ymd} 鮮度切れ ${r.stale}`);
  const maxAge = Math.max(...rows.map((r) => r.maxAge));
  assert.ok(maxAge < MONITOR_STALE_HOURS - 20, `最大の経過 ${maxAge.toFixed(1)} 時間`);
});

await t('🚨 模擬: 10/1 のかたまり (1,567 件) は 10/3〜10/8 の 6 朝ぜんぶに散る (どの朝も 4 割未満)・2 周目 (10/9〜) は毎朝 約 1/6・7 朝目からは全体も毎朝 約 1/6 (0.8〜1.2 倍) で平ら・どの朝も時間の上限 (daily-sync 600 秒) の半分以下', async () => {
  const { rows, total } = sim.new;
  const first = rows.slice(0, 6).map((r) => r.clump);
  // 10/7 だけ 2/6: 枠の日が 10/2 (= 切り替えの前) だった 1/6 が 10/7 に 132 時間の境に当たり、その朝の枠の 1/6 と重なる (切り替えのときの 1 回だけ。翌 10/8 にその 1/6 が自分の枠で取り直して枠にそろう)
  assert.ok(first.every((c) => c > 0 && c < 0.4 * 1567), JSON.stringify(first));
  for (const r of rows.slice(6)) assert.ok(r.clump <= 1.2 * 1567 / 6, `${r.ymd} かたまり ${r.clump} 件`);
  for (const r of rows.slice(6)) assert.ok(r.refreshed >= 0.8 * total / 6 && r.refreshed <= 1.2 * total / 6, `${r.ymd} ${r.refreshed} 件`);
  for (const r of rows) assert.ok(r.sec <= DAILY_SYNC_TIMEOUT_SEC / 2, `${r.ymd} ${r.sec} 秒`);
  // 全部 (3,081 件) を 1 朝で取り直す最悪のときでも上限に収まる
  assert.ok(Math.ceil(total / 20) * SEC_PER_BATCH < DAILY_SYNC_TIMEOUT_SEC);
});

await t('模擬の道具が鮮度切れを見つけられること (旧の 168 時間なら鮮度切れが出る) / 依頼の例 (144 時間 − 0〜24 時間) では 10/1 のかたまりが 1 朝に固まったまま (= 散らない) ことを記録しておく', async () => {
  assert.ok(sim.old.rows.some((r) => r.stale > 0), '旧の決まりで鮮度切れが出ない = 模擬が壊れている');
  const ex = sim.example.rows;
  assert.equal(ex.reduce((a, r) => a + r.stale, 0), 0);
  assert.ok(Math.max(...ex.slice(0, 7).map((r) => r.clump)) >= 0.9 * 1567, '例の決まりでもかたまりが散るなら、PR の説明を見直す');
});

await t('取り損ねた朝: 1 朝 (10/14) 取れなくても次の朝 (境 132 時間) で取り直し、監視の鮮度切れは 0 / 2 朝続けて取れなければ監視は鮮度切れを出す (本当の止まりは見逃さない)', async () => {
  const one = simulate('new', { missed: ['2026-10-14'] });
  for (const r of one.rows) assert.equal(r.stale, 0, `${r.ymd} 鮮度切れ ${r.stale}`);
  const two = simulate('new', { missed: ['2026-10-14', '2026-10-15'] });
  const r15 = two.rows.find((r) => r.ymd === '2026-10-15');
  assert.ok(r15.stale > 0, '2 朝止まったら監視が鮮度切れを出す');
  assert.equal(two.rows.find((r) => r.ymd === '2026-10-16').stale, 0, '戻った朝に取り直して消える');
});

if (process.argv.includes('--table')) {
  const N = 21;
  const fmtRow = (k) => {
    const a = sim.new.rows[k], o = sim.old.rows[k], e = sim.example.rows[k];
    return `| ${a.ymd.slice(5).replace('-', '/')} | ${a.refreshed} | ${a.clump} | ${Math.round(a.sec)} 秒 | ${a.maxAge.toFixed(1)} | **${a.stale}** | ${o.refreshed} | ${o.stale} | ${e.refreshed} | ${e.clump} | ${e.stale} |`;
  };
  console.log('\n| 朝 | 新: 取り直し | 新: うち 10/1 のかたまり | 新: かかる時間 | 新: 監視の時の最大経過 (時間) | 新: 監視の鮮度切れ | 旧 168h: 取り直し | 旧: 鮮度切れ | 例 144h−0〜24h: 取り直し | 例: うち 10/1 | 例: 鮮度切れ |');
  console.log('|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|');
  for (let k = 0; k < N; k++) console.log(fmtRow(k));
  const sum = (rows, f) => rows.reduce((a, r) => a + f(r), 0);
  for (const [name, s] of Object.entries(sim)) {
    const tail = s.rows.slice(7);
    console.log(`${name}: 28 朝の取り直し合計 ${sum(s.rows, (r) => r.refreshed)} 件 / 鮮度切れが出た朝 ${s.rows.filter((r) => r.stale > 0).length} / 8 朝目以降の 1 朝 最小 ${Math.min(...tail.map((r) => r.refreshed))}・最大 ${Math.max(...tail.map((r) => r.refreshed))} 件 / 最大の経過 ${Math.max(...s.rows.map((r) => r.maxAge)).toFixed(1)} 時間`);
  }
}

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng > 0 ? 1 : 0;
