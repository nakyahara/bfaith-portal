/**
 * test-linegift-listing-diff.mjs — LINEギフト finance DQ の listing_diff_pct を「受注日の月どうし」で比べる (apps/warehouse/linegift-listing-diff.js)
 *
 * 2026-09-21: 受取日の月の fact と、受注日の月の listing を比べていたので、月末の受注が翌月の受取に流れるぶん構造的に 4〜5% ずれ、
 *   8 月が 5.06% で毎朝 ❌ → 8 月の LINEギフト finance の Render への同期が見送られ続けた。その形をそのまま再現する。
 *
 * 使い方: node scripts/test-linegift-listing-diff.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { linegiftListingDiff, listingDiffThreshold, listingDiffSeverity, FACT_WHITELIST_SQL } from '../apps/warehouse/linegift-listing-diff.js';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 6).join('\n      ')); } };

const DDL = `
  CREATE TABLE raw_linegift_orders (order_id TEXT PRIMARY KEY, status TEXT, sku_code TEXT, stock_count INTEGER, selling_price REAL, bought_date_jst TEXT, received_date_jst TEXT);
  CREATE TABLE f_sales_by_listing (日付 TEXT, モール TEXT, 売上金額 REAL);
  CREATE TABLE f_linegift_finance_sku_daily_v1 (date_jst TEXT, sku_code TEXT, gross_sales_jpy_incl REAL, cost_status TEXT, PRIMARY KEY (date_jst, sku_code));`;
function mkDb(file = ':memory:') {
  const db = new Database(file); db.exec(DDL);
  let n = 0;
  const order = (bought, received, price, { status = 'received', qty = 1, sku = 'sku-a' } = {}) => db.prepare(`INSERT INTO raw_linegift_orders VALUES (?, ?, ?, ?, ?, ?, ?)`).run(`o${++n}`, status, sku, qty, price, bought, received);
  const listing = (date, jpy) => db.prepare(`INSERT INTO f_sales_by_listing VALUES (?, 'linegift', ?)`).run(date, jpy);
  // fact = raw (received の whitelist) を受取日 × SKU に集約したもの (本物の build SQL と同じ式)
  const buildFact = () => { db.exec(`DELETE FROM f_linegift_finance_sku_daily_v1`); db.exec(`INSERT INTO f_linegift_finance_sku_daily_v1 SELECT received_date_jst, lower(trim(sku_code)), SUM(selling_price * stock_count), 'ok' FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} GROUP BY 1, 2`); };
  return { db, order, listing, buildFact };
}

console.log('比べ方');
t('🚨 月末の受注が翌月の受取に流れる月: 受取日の月で比べると 5% を超えるが、受注日の月どうしなら一致する (9/21 の 8 月の ❌ の形)', () => {
  const { db, order, listing, buildFact } = mkDb();
  // 8 月: 受注 100 万円。うち月末の 5.5 万円は 9/1・9/2 に受取。7 月末の受注 0.2 万円が 8/1 に受取
  order('2026-07-31', '2026-08-01', 2000);
  for (let d = 1; d <= 27; d++) order(`2026-08-${String(d).padStart(2, '0')}`, `2026-08-${String(d + 1).padStart(2, '0')}`, 35000);
  order('2026-08-30', '2026-09-01', 30000); order('2026-08-31', '2026-09-02', 25000);
  listing('2026-08-15', 27 * 35000 + 55000);   // NE = 受注日の月
  listing('2026-07-31', 2000);
  buildFact();
  const r = linegiftListingDiff(db, '2026-08');
  assert.deepEqual([r.listingAvail, r.listingJpy, r.boughtBasisJpy, r.receivedBasisFactJpy], [true, 1000000, 1000000, 27 * 35000 + 2000]);
  assert.equal(r.diffPct, 0);
  assert.ok(r.receivedBasisDiffPct > 5 && r.receivedBasisDiffPct < 6, `今までの比べ方の差 = ${r.receivedBasisDiffPct}`);
  db.close();
});
t('本当に欠けている月は、受注日の月どうしでも差が出る (取込の穴を見逃さない): 受注 100 万円のうち 8 万円ぶんの注文が raw に無い → 8% = error', () => {
  const { db, order, listing, buildFact } = mkDb();
  for (let d = 1; d <= 23; d++) order(`2026-08-${String(d).padStart(2, '0')}`, `2026-08-${String(d).padStart(2, '0')}`, 40000);
  listing('2026-08-15', 1000000); buildFact();
  const r = linegiftListingDiff(db, '2026-08');
  assert.equal(Math.round(r.diffPct * 1000) / 1000, 8);
  assert.equal(listingDiffSeverity(r.diffPct, { warn: 1, error: 5 }), 'error');
  db.close();
});
t('数えるのは fact と同じ行だけ (received・数量と価格が正・SKU あり)。受取がまだの受注は not_received に出す (取消は入れない)', () => {
  const { db, order, listing, buildFact } = mkDb();
  order('2026-09-01', '2026-09-02', 1000, { qty: 3 });
  order('2026-09-03', null, 5000, { status: 'gift_message_send' }); order('2026-09-04', null, 7000, { status: 'payment' }); order('2026-09-05', null, 9000, { status: 'cancel' });
  order('2026-09-06', '2026-09-06', 0); order('2026-09-06', '2026-09-06', 800, { qty: 0 }); order('2026-09-06', '2026-09-06', 800, { sku: ' ' }); order('2026-09-06', null, 800);
  listing('2026-09-10', 15000); buildFact();
  const r = linegiftListingDiff(db, '2026-09');
  assert.deepEqual([r.boughtBasisJpy, r.receivedBasisFactJpy, r.notReceivedJpy], [3000, 3000, 12000]);
  db.close();
});
t('listing が無い月は listingAvail = false・diffPct は null (判定は info)。月の形が違えば例外', () => {
  const { db } = mkDb();
  const r = linegiftListingDiff(db, '2026-08');
  assert.deepEqual([r.listingAvail, r.diffPct, listingDiffSeverity(r.diffPct, { warn: 1, error: 5 })], [false, null, 'info']);
  assert.throws(() => linegiftListingDiff(db, '2026-8'), /YYYY-MM/);
  db.close();
});
t('しきい値: 過去月は厳しいまま・当月と前月の月初 (recent_past) は当月用 (受取がまだの受注が fact に居ないぶん構造的に小さい)。重複期間は info', () => {
  const past = { warn: 1, error: 5 }, cur = { warn: 5, error: 15 };
  assert.deepEqual(['past', 'recent_past', 'current'].map((m) => listingDiffThreshold(m, past, cur)), [past, cur, cur]);
  assert.deepEqual([0.2, 1.5, 5.06, 16].map((v) => listingDiffSeverity(v, past)), ['info', 'warn', 'error', 'error']);
  assert.deepEqual([4.9, 5.4, 16].map((v) => listingDiffSeverity(v, cur)), ['info', 'warn', 'error']);
  assert.equal(listingDiffSeverity(40, past, { isDuplicatePeriod: true }), 'info');
});
t('🚨 fact に入る行の条件は build SQL (Step 1) と同じ: ソースとずれたら落ちる', () => {
  const sql = fs.readFileSync(path.join(root, 'sql', 'linegift', 'build_f_linegift_finance_sku_daily_v1.sql'), 'utf8').replace(/\r\n/g, '\n');
  const m = sql.match(/FROM raw_linegift_orders\nWHERE ([\s\S]*?);\nCREATE INDEX _silver_linegift_v1_idx/);
  assert.ok(m, 'build SQL の Step 1 の WHERE が見つからない');
  const norm = (x) => x.replace(/\s+/g, ' ').trim();
  const conds = norm(m[1]).split(' AND ').filter((c) => !/strftime\('%Y%m', received_date_jst\)/.test(c));
  assert.equal(conds.join(' AND '), norm(FACT_WHITELIST_SQL));
});

console.log('DQ のスクリプト (プロセスとして起動)');
t('8 月の形 (受取日の月なら 5% 超) でも listing_diff_pct は error にならない・details に両方の数字が残る', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lg-dq-'));
  try {
    const { db, order, listing, buildFact } = mkDb(path.join(dir, 'warehouse.db'));
    db.exec(`CREATE TABLE dq_run_results (run_id TEXT, check_name TEXT, severity TEXT, actual_value REAL, threshold_value REAL, details_json TEXT, checked_at TEXT, PRIMARY KEY (run_id, check_name))`);
    for (let d = 1; d <= 27; d++) order(`2026-01-${String(d).padStart(2, '0')}`, `2026-01-${String(d + 1).padStart(2, '0')}`, 35000);
    order('2026-01-30', '2026-02-01', 30000); order('2026-01-31', '2026-02-02', 25000);
    listing('2026-01-15', 1000000); buildFact(); db.close();
    const r = spawnSync(process.execPath, [path.join(root, 'apps', 'warehouse', 'run-linegift-finance-dq.js'), '--data-dir', dir, '--month', '2026-01', '--run-id', 'test-run'], { encoding: 'utf8' });
    const db2 = new Database(path.join(dir, 'warehouse.db'), { readonly: true });
    const row = db2.prepare(`SELECT severity, actual_value, threshold_value, details_json FROM dq_run_results WHERE run_id = 'test-run' AND check_name = 'listing_diff_pct'`).get();
    db2.close();
    assert.ok(row, `listing_diff_pct の結果が無い (exit ${r.status}): ${String(r.stderr).slice(0, 300)} ${String(r.stdout).slice(-300)}`);
    const d = JSON.parse(row.details_json);
    assert.deepEqual([row.severity, row.actual_value, row.threshold_value, d.basis, d.bought_basis_jpy, d.listing_jpy, d.received_basis_fact_jpy, Math.round(d.received_basis_diff_pct * 10) / 10], ['info', 0, 5, 'bought_month', 1000000, 1000000, 945000, 5.5]);
  } finally { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ } }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
