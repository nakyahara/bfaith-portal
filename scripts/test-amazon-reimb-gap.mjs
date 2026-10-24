#!/usr/bin/env node
/**
 * test-amazon-reimb-gap.mjs — 補てん漏れのアラート (apps/warehouse/monitor-amazon-reimb-gap.js・2026-10-24) の試験。
 *   SP-API・GChat・本物の DB には繋がない (warehouse.db の代わりに :memory: の SQLite と見本の行。送り手は差し替え)
 *   ① 突き合わせ (紛失だけ・補てん済み・一部だけ補てん・見つかった分で相殺・補てんの取り消し・紛失より前の補てんは当てない)
 *   ② 日数 (待つ日数の前・期限の近く・期限切れ)・しきい値・単価 (補てんの単価 → 売価 → 数量だけ)・並び順・SKU で補てんを結ぶ
 *   ③ 表が空・取得が古い・在庫の台帳は 30 日より古い日を消さない
 *   ④ 週 1 回: 曜日・同じ週に 2 通送らない (同時に 2 つ走っても)・送れなかった週は翌朝もう一度・--dry-run は記録しない・文面
 * 使い方: node scripts/test-amazon-reimb-gap.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { recordFetchDay, addDays } from '../apps/warehouse/sp-api-report.js';
import * as LG from '../apps/warehouse/fetch-amazon-ledger.js';
import * as RB from '../apps/warehouse/fetch-amazon-reimbursements.js';
import { ensurePlanningHistoryTables } from '../apps/warehouse/fba-planning-history.js';
import {
  RULES, ALERT_WEEKDAY_JST, matchEvents, reimbKind, reimbUnits, evaluateReimbGap, buildGapMessage, runReimbGapAlert, claimGapWeek, ensureGapAlertLog,
} from '../apps/warehouse/monitor-amazon-reimb-gap.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } };
const quiet = () => {};

const TODAY = '2026-10-27';                                // 火曜
const TUE = Date.parse('2026-10-27T00:30:00Z');            // JST 10/27 (火) 09:30
const WED = TUE + 86400000, MON = TUE - 86400000, NEXT_TUE = TUE + 7 * 86400000;
const ago = (n) => addDays(TODAY, -n);

/** 表を作り、取得の記録 (台帳 = 2026-06-01〜昨日・補てん = 同じ) を入れた DB */
function openDb({ fetched = true, ledgerTo = ago(1), reimbTo = ago(1) } = {}) {
  const db = new Database(':memory:');
  LG.ensureLedgerTable(db);
  RB.ensureReimbursementsTable(db);
  if (fetched) {
    for (const [type, to] of [[LG.REPORT_TYPE, ledgerTo], [RB.REPORT_TYPE, reimbTo]]) {
      for (const d of ['2026-06-01', to]) recordFetchDay(db, { reportType: type, date: d, generation: 1, reportId: 'R', windowFrom: d, windowTo: d, rowCount: 0, fetchedAt: 'x' });
    }
  }
  return db;
}
let line = 0;
function led(db, { date, fnsku = 'X001', msku = 'sku-a', reason, qty, type = 'Adjustments', unrec = null, title = '見本の商品' }) {
  db.prepare(`INSERT INTO amazon_inventory_ledger (event_date, fnsku, msku, title, event_type, quantity, reason, unreconciled_quantity, line_no, report_id, generation, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'R', 1, 'x')`).run(date, fnsku, msku, title, type, qty, reason, unrec, ++line);
}
let rid = 0;
function reimb(db, { date, fnsku = 'X001', sku = 'sku-a', reason = 'Lost_Warehouse', qty = 1, cash = null, inv = null, perUnit = 2000, origType = null }) {
  db.prepare(`INSERT INTO amazon_fba_reimbursements (reimbursement_id, seller_sku, fnsku, occurrence, approval_date, approval_date_jst, reason, currency_unit, amount_per_unit, amount_total,
      quantity_reimbursed_cash, quantity_reimbursed_inventory, quantity_reimbursed_total, original_reimbursement_type, report_id, generation, first_seen_at, fetched_at)
    VALUES (?, ?, ?, 1, ?, ?, ?, 'JPY', ?, ?, ?, ?, ?, ?, 'R', 1, 'x', 'x')`)
    .run(`RB${++rid}`, sku, fnsku, `${date}T00:00:00+00:00`, date, reason, perUnit, qty === null ? null : perUnit * qty, cash, inv, qty, origType);
}
const ev = (db, rules = RULES) => evaluateReimbGap(db, { today: TODAY, rules });
const units = (r, fnsku = 'X001') => r.items.find((i) => i.fnsku === fnsku)?.units ?? 0;

console.log('① 突き合わせ');
await t('紛失だけ (M・数量 -2・35 日前) → 候補 2 個 (紛失)・期限 = 発生から 60 日・単価が分からなければ数量だけ', () => {
  const db = openDb();
  led(db, { date: ago(35), reason: 'M', qty: -2 });
  const r = ev(db);
  assert.equal(r.items.length, 1);
  const it = r.items[0];
  assert.deepEqual([it.fnsku, it.msku, it.units, it.lostUnits, it.damagedUnits, it.firstDate, it.deadline, it.daysLeft, it.urgent, it.value, it.priceSource],
    ['X001', 'sku-a', 2, 2, 0, ago(35), addDays(ago(35), 60), 25, false, null, null]);
});
await t('補てん済み (紛失 1 → 後で Lost_Warehouse 1) → 候補なし。在庫での補てん (quantity-reimbursed-inventory) も数える', () => {
  const db = openDb();
  led(db, { date: ago(40), reason: 'M', qty: -1 });
  reimb(db, { date: ago(20), qty: 1 });
  led(db, { date: ago(40), fnsku: 'X002', msku: 'sku-b', reason: 'M', qty: -1 });
  reimb(db, { date: ago(20), fnsku: 'X002', sku: 'sku-b', qty: null, cash: 0, inv: 1 });
  assert.equal(ev(db).items.length, 0);
});
await t('一部だけ補てん (破損 E -3 → Damaged_Warehouse 1) → 残り 2 個 (破損)・金額 = 個数 × 前の補てんの単価', () => {
  const db = openDb();
  led(db, { date: ago(30), reason: 'E', qty: -3 });
  led(db, { date: ago(30), reason: 'P', qty: 3 });   // 対になる「販売不可へ」の行は数えない
  reimb(db, { date: ago(10), reason: 'Damaged_Warehouse', qty: 1, perUnit: 1500 });
  const r = ev(db);
  assert.equal(r.items.length, 1);
  assert.deepEqual([r.items[0].units, r.items[0].damagedUnits, r.items[0].unitPrice, r.items[0].priceSource, r.items[0].value], [2, 2, 1500, 'reimb', 3000]);
});
await t('見つかった分で相殺 (紛失 2 → F +2 = なし / F +1 = 1 個)。F は破損を消さない', () => {
  const db = openDb();
  led(db, { date: ago(45), reason: 'M', qty: -2 });
  led(db, { date: ago(40), reason: 'F', qty: 2 });
  led(db, { date: ago(45), fnsku: 'X002', msku: 'sku-b', reason: 'M', qty: -2 });
  led(db, { date: ago(40), fnsku: 'X002', msku: 'sku-b', reason: 'F', qty: 1 });
  led(db, { date: ago(45), fnsku: 'X003', msku: 'sku-c', reason: 'E', qty: -1 });
  led(db, { date: ago(40), fnsku: 'X003', msku: 'sku-c', reason: 'F', qty: 1 });
  const r = ev(db);
  assert.deepEqual([units(r, 'X001'), units(r, 'X002'), units(r, 'X003')], [0, 1, 1]);
});
await t('紛失 → 補てん → 見つかった → 補てんの取り消し = 差し引きゼロ / 取り消しだけ (見つかっていない) = 紛失が戻る', () => {
  const db = openDb();
  led(db, { date: ago(50), reason: 'M', qty: -1 });
  reimb(db, { date: ago(40), qty: 1 });
  led(db, { date: ago(35), reason: 'F', qty: 1 });
  reimb(db, { date: ago(34), reason: 'Reimbursement_Reversal', qty: -1, origType: 'Lost_Warehouse' });
  led(db, { date: ago(50), fnsku: 'X002', msku: 'sku-b', reason: 'M', qty: -1 });
  reimb(db, { date: ago(40), fnsku: 'X002', sku: 'sku-b', qty: 1 });
  reimb(db, { date: ago(34), fnsku: 'X002', sku: 'sku-b', reason: 'Reimbursement_Reversal', qty: -1, origType: 'Lost_Warehouse' });
  const r = ev(db);
  assert.deepEqual([units(r, 'X001'), units(r, 'X002')], [0, 1]);
  assert.equal(r.items.find((i) => i.fnsku === 'X002').firstDate, ago(50));   // 戻った分は元の発生日 (期限も元の日から)
});
await t('🚨 補てんの取り消しは、取り消された補てん (original-reimbursement-id) が消した塊を戻す (最後の補てんの塊ではない・Codex R1 P2)', () => {
  const db = openDb();
  led(db, { date: ago(55), reason: 'M', qty: -1 });
  reimb(db, { date: ago(45), qty: 1 });                                   // この紛失の補てん
  const lostReimbId = `RB${rid}`;
  led(db, { date: ago(5), reason: 'E', qty: -1 });
  reimb(db, { date: ago(3), reason: 'Damaged_Warehouse', qty: 1 });
  db.prepare(`INSERT INTO amazon_fba_reimbursements (reimbursement_id, seller_sku, fnsku, occurrence, approval_date, approval_date_jst, reason, quantity_reimbursed_total, original_reimbursement_id, original_reimbursement_type, report_id, generation, first_seen_at, fetched_at)
    VALUES ('RV1', 'sku-a', 'X001', 1, ?, ?, 'Reimbursement_Reversal', -1, ?, 'Lost_Warehouse', 'R', 1, 'x', 'x')`).run(ago(2), ago(2), lostReimbId);
  const r = ev(db);
  assert.equal(r.items.length, 1);
  assert.deepEqual([r.items[0].lostUnits, r.items[0].damagedUnits, r.items[0].firstDate, r.items[0].daysLeft, r.items[0].urgent], [1, 0, ago(55), 5, true]);
  assert.equal(r.pending.items, 0);   // 5 日前の破損は補てん済みのまま (待っている に戻さない)
  // ID が無い取り消しでも、同じ種類の補てんの塊を先に戻す
  const m = matchEvents([
    { date: '2026-09-01', type: 'loss', kind: 'lost', qty: 1 }, { date: '2026-09-02', type: 'reimb', kind: 'lost', qty: 1, id: 'A' },
    { date: '2026-10-01', type: 'loss', kind: 'damaged', qty: 1 }, { date: '2026-10-02', type: 'reimb', kind: 'damaged', qty: 1, id: 'B' },
    { date: '2026-10-05', type: 'reimb', kind: 'lost', qty: -1 },
  ]);
  assert.deepEqual(m.open, [{ date: '2026-09-01', kind: 'lost', qty: 1 }]);
});
await t('🚨 紛失より前の補てん・見つかった (取り始める前の紛失の分) は、後の紛失に当てない = 候補に出る', () => {
  const db = openDb();
  reimb(db, { date: ago(60), qty: 1 });
  led(db, { date: ago(58), reason: 'F', qty: 1 });
  led(db, { date: ago(40), reason: 'M', qty: -1 });
  assert.equal(units(ev(db)), 1);
});
await t('補てんの種類: 倉庫の紛失・破損だけ数える (CustomerReturn などは数えない)・取り消しは元の種類で・個数は total → 現金 + 在庫', () => {
  assert.equal(reimbKind({ reason: 'lost_warehouse' }), 'lost');
  assert.equal(reimbKind({ reason: 'Damaged_Warehouse' }), 'damaged');
  assert.equal(reimbKind({ reason: 'CustomerReturn' }), null);
  assert.equal(reimbKind({ reason: 'Reimbursement_Reversal', original_reimbursement_type: 'Damaged_Warehouse' }), 'damaged');
  assert.equal(reimbKind({ reason: 'Reimbursement_Reversal', original_reimbursement_type: 'CustomerReturn' }), null);
  assert.equal(reimbKind({ reason: 'Reimbursement_Reversal', original_reimbursement_type: null }), 'any');
  assert.equal(reimbUnits({ quantity_reimbursed_total: 3, quantity_reimbursed_cash: 1, quantity_reimbursed_inventory: 1 }), 3);
  assert.equal(reimbUnits({ quantity_reimbursed_total: null, quantity_reimbursed_cash: 1, quantity_reimbursed_inventory: 2 }), 3);
  assert.equal(reimbUnits({ quantity_reimbursed_total: null, quantity_reimbursed_cash: null, quantity_reimbursed_inventory: null }), 0);
  const db = openDb();
  led(db, { date: ago(40), reason: 'M', qty: -1 });
  reimb(db, { date: ago(20), reason: 'CustomerReturn', qty: 1 });
  assert.equal(units(ev(db)), 1);
});
await t('補てんの FNSKU が空なら SKU (大文字小文字は問わない) で台帳の FNSKU に結ぶ', () => {
  const db = openDb();
  led(db, { date: ago(40), reason: 'M', qty: -1 });
  reimb(db, { date: ago(20), fnsku: '', sku: 'SKU-A', qty: 1 });
  assert.equal(ev(db).items.length, 0);
});
await t('matchEvents: 同じ種類を先に消す・同じ日は「紛失 → 見つかった → 補てん」の順', () => {
  const r = matchEvents([
    { date: '2026-09-01', type: 'loss', kind: 'damaged', qty: 1 },
    { date: '2026-09-02', type: 'loss', kind: 'lost', qty: 1 },
    { date: '2026-09-10', type: 'reimb', kind: 'lost', qty: 1 },
    { date: '2026-09-20', type: 'reimb', kind: 'lost', qty: 1 },   // 紛失が残っていなければ別の種類も
    { date: '2026-09-30', type: 'found', qty: 1 },
    { date: '2026-09-30', type: 'loss', kind: 'lost', qty: 1 },
  ]);
  assert.deepEqual([r.open, r.foundSurplus], [[], 0]);
});

console.log('② 日数・しきい値・単価・並び');
await t('待つ日数の前 (紛失 10 日前・破損 20 日前) は載せない (待っている に数える)・紛失 30 日 / 破損 25 日を過ぎたら載せる', () => {
  const db = openDb();
  led(db, { date: ago(10), reason: 'M', qty: -1 });
  led(db, { date: ago(20), fnsku: 'X002', msku: 'sku-b', reason: 'E', qty: -1 });
  led(db, { date: ago(RULES.WAIT_DAYS_LOST), fnsku: 'X003', msku: 'sku-c', reason: 'M', qty: -1 });
  led(db, { date: ago(RULES.WAIT_DAYS_DAMAGED), fnsku: 'X004', msku: 'sku-d', reason: 'E', qty: -1 });
  const r = ev(db);
  assert.deepEqual(r.items.map((i) => i.fnsku).sort(), ['X003', 'X004']);
  assert.deepEqual(r.pending, { items: 2, units: 2 });
});
await t('期限の近く (50 日前 = あと 10 日) は 🔴・ちょうど 60 日はまだ候補 (あと 0 日)・61 日は期限切れの可能性 (件数と金額だけ)', () => {
  const db = openDb();
  led(db, { date: ago(50), reason: 'M', qty: -1 });
  led(db, { date: ago(60), fnsku: 'X002', msku: 'sku-b', reason: 'M', qty: -1 });
  led(db, { date: ago(61), fnsku: 'X003', msku: 'sku-c', reason: 'M', qty: -2 });
  reimb(db, { date: ago(90), fnsku: 'X003', sku: 'sku-c', qty: 0, perUnit: 3000 });   // 単価の履歴だけ (数量 0)
  const r = ev(db);
  const a = r.items.find((i) => i.fnsku === 'X001'), b = r.items.find((i) => i.fnsku === 'X002');
  assert.deepEqual([a.daysLeft, a.urgent, b.daysLeft, b.urgent], [10, true, 0, true]);
  assert.equal(r.items.some((i) => i.fnsku === 'X003'), false);
  assert.deepEqual(r.expired, { items: 1, units: 2, valueYen: 6000 });
});
await t('単価: 補てんの単価 > 在庫計画の売価 > 分からない。金額の大きい順 (分からないものは後ろ)・金額が MIN_VALUE_YEN 未満は出さない', () => {
  const db = openDb();
  ensurePlanningHistoryTables(db);
  const plan = db.prepare(`INSERT INTO fba_planning_daily (snapshot_date, market, amazon_sku, your_price) VALUES (?, 'jp', ?, ?)`);
  plan.run('2026-10-01', 'sku-b', 9999); plan.run('2026-10-20', 'SKU-B', 4000);   // 新しい日の売価 (綴りの大小は問わない)
  plan.run('2026-10-20', 'sku-d', 500);                                          // 1 個 ¥500 < ¥1,000 = 出さない
  led(db, { date: ago(35), reason: 'M', qty: -1 });                                // 単価なし
  led(db, { date: ago(35), fnsku: 'X002', msku: 'sku-b', reason: 'M', qty: -1 }); // 売価 4,000
  led(db, { date: ago(35), fnsku: 'X003', msku: 'sku-c', reason: 'M', qty: -2 }); // 前の補てんの単価 2,500 × 2
  reimb(db, { date: ago(100), fnsku: 'X003', sku: 'sku-c', qty: 0, perUnit: 2500 });
  led(db, { date: ago(35), fnsku: 'X004', msku: 'sku-d', reason: 'M', qty: -1 });
  const r = ev(db);
  assert.deepEqual(r.items.map((i) => [i.fnsku, i.value, i.priceSource]), [['X003', 5000, 'reimb'], ['X002', 4000, 'price'], ['X001', null, null]]);
});
await t('Adjustments 以外の行 (Shipments など)・理由の小文字・範囲 (120 日) より前の紛失は見ない', () => {
  const db = openDb();
  led(db, { date: ago(35), reason: 'M', qty: -1, type: 'Shipments' });
  led(db, { date: ago(35), fnsku: 'X002', msku: 'sku-b', reason: 'm', qty: -1, type: 'adjustments' });
  led(db, { date: ago(130), fnsku: 'X003', msku: 'sku-c', reason: 'M', qty: -1 });
  const r = ev(db);
  assert.deepEqual(r.items.map((i) => i.fnsku), ['X002']);
  assert.deepEqual([r.expired.items, r.windowFrom], [0, ago(RULES.LOOKBACK_DAYS)]);
});
await t('Amazon の未照合は見せるだけ (既定)。TRUST_AMAZON_UNRECONCILED = true なら個数をそこまで削る (0 なら出さない)', () => {
  const db = openDb();
  led(db, { date: ago(35), reason: 'M', qty: -2, unrec: 0 });
  assert.deepEqual([units(ev(db)), ev(db).items[0].amazonUnreconciled], [2, 0]);
  assert.equal(ev(db, { ...RULES, TRUST_AMAZON_UNRECONCILED: true }).items.length, 0);
  // 削るときは塊ごと (内訳・期限・金額もそろえる。Codex R1 P2)
  const db2 = openDb();
  led(db2, { date: ago(50), reason: 'M', qty: -1, unrec: 0 });
  led(db2, { date: ago(35), reason: 'M', qty: -2, unrec: 1 });
  const trusted = ev(db2, { ...RULES, TRUST_AMAZON_UNRECONCILED: true });
  const it = trusted.items[0];
  assert.deepEqual([it.units, it.lostUnits, it.damagedUnits, it.amazonUnreconciled, it.firstDate], [1, 1, 0, 1, ago(50)]);
  assert.match(buildGapMessage(trusted), /計 1 個[\s\S]*紛失 1 個/);
});

console.log('③ 表が空・取得が古い・台帳の古い日');
await t('表が空 (取得の記録も無い) → no_data・取得の記録だけある → 候補なし (送らない)', async () => {
  const send = async () => { throw new Error('送らない'); };
  const empty = new Database(':memory:');
  const r0 = await runReimbGapAlert(empty, { nowMs: TUE, webhook: 'https://x', send, log: quiet });
  assert.equal(r0.status, 'no_data');
  assert.match(r0.lastLine, /^⚠️/);
  const db = openDb();
  const r1 = await runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send, log: quiet });
  assert.equal(r1.status, 'none');
  assert.match(r1.lastLine, /^✅/);
  assert.equal(buildGapMessage(ev(db)), null);
});
await t('台帳・補てんの取得が古い (最後が 4 日前) → stale (送らない・⚠️)', async () => {
  for (const o of [{ ledgerTo: ago(4) }, { reimbTo: ago(4) }]) {
    const db = openDb(o);
    led(db, { date: ago(35), reason: 'M', qty: -1 });
    const r = await runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send: async () => true, log: quiet });
    assert.equal(r.status, 'stale', JSON.stringify(o));
    assert.match(r.lastLine, /^⚠️.*古い/);
  }
});
await t('在庫の台帳は取り直した 30 日だけを置き換える = それより古い日の行は残る (検出に要る 60 日超の履歴が貯まる)', () => {
  const db = new Database(':memory:');
  const rep = (rows) => ({ headers: ['Date', 'FNSKU', 'MSKU', 'Event Type', 'Quantity', 'Reason'], rows: rows.map((r, i) => ({ __line: i + 2, Date: r[0], FNSKU: 'X001', MSKU: 'sku-a', 'Event Type': 'Adjustments', Quantity: String(r[1]), Reason: r[2] })) });
  LG.saveLedger(db, rep([['2026-08-01', -1, 'M'], ['2026-08-20', -1, 'E']]), { from: '2026-08-01', to: '2026-08-30', generation: 1, reportId: 'R1', now: () => 'x' });
  LG.saveLedger(db, rep([['2026-08-25', -2, 'M']]), { from: '2026-08-20', to: '2026-09-18', generation: 2, reportId: 'R2', now: () => 'x' });
  const rows = db.prepare('SELECT event_date, quantity, reason FROM amazon_inventory_ledger ORDER BY event_date').all();
  assert.deepEqual(rows.map((r) => [r.event_date, r.quantity, r.reason]), [['2026-08-01', -1, 'M'], ['2026-08-25', -2, 'M']]);   // 08-20 は取り直しで消えた (置き換え)・08-01 は残る
});

console.log('④ 週 1 回・1 通');
const seed = (db) => {
  led(db, { date: ago(50), reason: 'M', qty: -1, title: 'とても長い商品名がここに入ります 色 サイズ 入り数' });
  reimb(db, { date: ago(200), qty: 0, perUnit: 3200 });
  for (let i = 0; i < 18; i++) led(db, { date: ago(35), fnsku: `Y${String(i).padStart(3, '0')}`, msku: `sku-y${i}`, reason: i % 2 ? 'E' : 'M', qty: -1 });
};
await t('送る曜日 (火曜) だけ・送ったら同じ週は 2 通目を送らない (翌日も・--force は送る)・次の週はまた送る・--dry-run は曜日を問わず記録しない', async () => {
  assert.equal(ALERT_WEEKDAY_JST, 2);
  const db = openDb({ ledgerTo: ago(1), reimbTo: ago(1) });
  seed(db);
  let n = 0; const send = async () => { n++; return true; };
  assert.equal((await runReimbGapAlert(db, { nowMs: MON, webhook: 'https://x', send, log: quiet })).status, 'not_today');
  const dry = await runReimbGapAlert(db, { nowMs: MON, webhook: 'https://x', send, dryRun: true, log: quiet });
  assert.deepEqual([dry.status, n, db.prepare('SELECT COUNT(*) AS c FROM amazon_reimb_gap_alert_log').get().c], ['dry_run', 0, 0]);
  assert.equal((await runReimbGapAlert(db, { nowMs: TUE, webhook: '', send, log: quiet })).status, 'no_webhook');
  const r = await runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send, log: quiet });
  assert.deepEqual([r.status, n], ['sent', 1]);
  assert.match(r.lastLine, /^✅ 補てん漏れアラート: 送った \(候補 19 件 19 個・約 ¥3,200/);
  assert.equal((await runReimbGapAlert(db, { nowMs: TUE + 3600000, webhook: 'https://x', send, log: quiet })).status, 'already_sent');
  assert.equal((await runReimbGapAlert(db, { nowMs: WED, webhook: 'https://x', send, log: quiet })).status, 'not_today');
  assert.equal(n, 1);
  assert.equal((await runReimbGapAlert(db, { nowMs: WED, webhook: 'https://x', send, force: true, log: quiet })).status, 'sent');
  // 次の週 (取得の記録も進める)
  for (const type of [LG.REPORT_TYPE, RB.REPORT_TYPE]) recordFetchDay(db, { reportType: type, date: addDays(TODAY, 6), generation: 2, reportId: 'R2', windowFrom: 'x', windowTo: 'x', rowCount: 0, fetchedAt: 'x' });
  assert.equal((await runReimbGapAlert(db, { nowMs: NEXT_TUE, webhook: 'https://x', send, log: quiet })).status, 'sent');
});
await t('🚨 同時に 2 つ走っても 1 通 (送る前に週の権利を取る)・途中で落ちた印 (古い sending) は取り直せる', async () => {
  const db = openDb();
  seed(db);
  let n = 0;
  const slow = async () => { n++; await new Promise((r) => setTimeout(r, 20)); return true; };
  const [a, b] = await Promise.all([runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send: slow, log: quiet }), runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send: slow, log: quiet })]);
  assert.deepEqual([a.status, b.status].sort(), ['in_progress', 'sent']);
  assert.equal(n, 1);
  const db2 = openDb();
  ensureGapAlertLog(db2);
  assert.equal(claimGapWeek(db2, { week: '2026-10-26', asOf: TODAY, itemCount: 1, units: 1, valueYen: null, nowMs: TUE }).ok, true);
  assert.equal(claimGapWeek(db2, { week: '2026-10-26', asOf: TODAY, itemCount: 1, units: 1, valueYen: null, nowMs: TUE + 60000 }).ok, false);
  assert.equal(claimGapWeek(db2, { week: '2026-10-26', asOf: TODAY, itemCount: 1, units: 1, valueYen: null, nowMs: TUE + 11 * 60000 }).ok, true);
});
await t('送れなかった週 (false・例外) は記録を failed にして、同じ週の翌朝 (曜日でなくても) もう一度試す・最後の行は ⚠️', async () => {
  const db = openDb();
  seed(db);
  const r1 = await runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send: async () => false, log: quiet });
  assert.equal(r1.status, 'send_failed');
  assert.match(r1.lastLine, /^⚠️/);
  for (const type of [LG.REPORT_TYPE, RB.REPORT_TYPE]) recordFetchDay(db, { reportType: type, date: TODAY, generation: 2, reportId: 'R2', windowFrom: 'x', windowTo: 'x', rowCount: 0, fetchedAt: 'x' });
  assert.equal((await runReimbGapAlert(db, { nowMs: WED, webhook: 'https://x', send: async () => { throw new Error('ECONNRESET'); }, log: quiet })).status, 'send_failed');
  assert.equal((await runReimbGapAlert(db, { nowMs: WED + 3600000, webhook: 'https://x', send: async () => true, log: quiet })).status, 'sent');
  assert.equal(db.prepare(`SELECT status FROM amazon_reimb_gap_alert_log WHERE week_key = '2026-10-26'`).get().status, 'sent');
});
await t('文面: 合計・🔴 の件数・金額の大きい順・期限と残りの日数・上位 MAX_LIST 件 + ほか N 件・待っている / 期限切れの可能性・申請先', async () => {
  const db = openDb();
  seed(db);
  led(db, { date: ago(10), fnsku: 'Z001', msku: 'sku-z', reason: 'M', qty: -1 });   // 待っている
  led(db, { date: ago(70), fnsku: 'Z002', msku: 'sku-z2', reason: 'E', qty: -1 });  // 期限切れの可能性
  let sentText = null;
  const r = await runReimbGapAlert(db, { nowMs: TUE, webhook: 'https://x', send: async (_h, text) => { sentText = text; return true; }, log: quiet });
  assert.equal(r.status, 'sent');
  const m = sentText;
  assert.match(m, /^💴 \*Amazon 補てん漏れの候補 \(週 1 回\)\* 2026-10-27 時点\n/);
  assert.match(m, /まだ補てんされていない: 19 件 \/ 計 19 個 \/ 見込み 約 ¥3,200 \(金額が分かる 1 件\)/);
  assert.match(m, /🔴 申請の期限まで 14 日以内: 1 件/);
  const lines = m.split('\n').filter((l) => l.startsWith('• '));
  assert.equal(lines.length, RULES.MAX_LIST);
  assert.match(lines[0], /^• 🔴 sku-a \(X001\) とても長い商品名がここに入ります 色 サイズ 入 — 紛失 1 個 \(2026-09-07〜\) \/ ¥3,200 \(前の補てんの単価 ¥3,200\) \/ 期限 2026-11-06 \(あと 10 日\)$/);
  assert.match(lines[1], /^• sku-y\d+ \(Y\d{3}\) 見本の商品 — (紛失|破損) 1 個 \(2026-09-22〜\) \/ 期限 2026-11-21 \(あと 25 日\)$/);
  assert.match(m, /… ほか 4 件/);
  assert.match(m, /⏳ 待っている \(発生から紛失 30 日・破損 25 日の前\): 1 件 1 個/);
  assert.match(m, /⌛ 期限 \(発生から 60 日\) を過ぎた可能性: 1 件 1 個/);
  assert.match(m, /在庫の問題と補てん/);
  assert.ok(m.length <= 3900 + 20);
});

console.log('daily-sync・publish-gate への組み込み');
await t('「Amazon AI 分析用のレポート」の塊の中・補てんと台帳の取得の後・両方が成功した朝だけ・引数つき・止めない工程・m_products などを読まない', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const all = ds.slice(ds.indexOf('  // ── Amazon AI 分析用のレポート (設計 28 の外'), ds.indexOf('  // ── ここまで Amazon AI 分析用のレポート'));
  const iGap = all.indexOf("runScript('apps/warehouse/monitor-amazon-reimb-gap.js --daily'");
  assert.ok(iGap > all.indexOf("runScript('apps/warehouse/fetch-amazon-ledger.js --days 30'") && iGap > all.indexOf("runScript('apps/warehouse/fetch-amazon-reimbursements.js --days 60'"));
  assert.match(all, /if \(reimbResult\.success && ledgerResult\.success\) \{\n\s*const reimbGapResult = runScript\('apps\/warehouse\/monitor-amazon-reimb-gap\.js --daily', 'Amazon補てん漏れアラート', 120000\);\n\s*results\.push\(\{ name: 'Amazon補てん漏れアラート', \.\.\.reimbGapResult, warn: reimbGapResult\.success && isWarnSummary\(reimbGapResult\.summary\) \}\);/);
  assert.equal(ds.split('monitor-amazon-reimb-gap.js --daily').length - 1, 1);
  const G = await import('../apps/warehouse/publish-gate.js');
  assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, 'apps/warehouse/monitor-amazon-reimb-gap.js'));
  assert.equal(G.publishGateDecision('apps/warehouse/monitor-amazon-reimb-gap.js --daily', { broken: true }).skip, false);
  const src = fs.readFileSync(path.join(ROOT, 'apps/warehouse/monitor-amazon-reimb-gap.js'), 'utf8');
  assert.doesNotMatch(src, /\b(m_products(_history)?|m_set_components|exception_genka|v_product_master|v_sku_costed)\b/);
  assert.doesNotMatch(src.replace(/\/\/[^\n]*/g, ''), /process\.exit\(/);   // GChat の fetch の直後に exit しない (Windows の libuv)
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
