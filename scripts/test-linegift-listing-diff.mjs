/**
 * test-linegift-listing-diff.mjs — LINEギフト finance DQ: listing_diff_pct を「受注日の月どうし」で比べる + fact ↔ raw の一致 (apps/warehouse/linegift-listing-diff.js)
 *
 * 2026-09-21: 受取日の月の fact と、受注日の月の listing を比べていたので、月末の受注が翌月の受取に流れるぶん構造的に 4〜5% ずれ、
 *   8 月が 5.06% で毎朝 ❌ → 8 月の LINEギフト finance の Render への同期が見送られ続けた。その形をそのまま再現する。
 *   比べる相手を raw に変えたぶん、「fact が raw から欠けずに作られているか」は別の検査 (fact_raw_mismatch_keys) で見る (Codex #1398 R1)。
 *
 * fact の表は本物の DDL (sql/linegift/f_linegift_finance_sku_daily_v1.sql) で作る。DQ のスクリプトはプロセスとして起動し、**終了コードと全検査の記録** まで確かめる。
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
import { linegiftListingDiff, linegiftFactRawMismatch, listingDiffThreshold, listingDiffSeverity, FACT_WHITELIST_SQL } from '../apps/warehouse/linegift-listing-diff.js';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 6).join('\n      ')); } };

const FACT_DDL = fs.readFileSync(path.join(root, 'sql', 'linegift', 'f_linegift_finance_sku_daily_v1.sql'), 'utf8');
const DDL = `
  CREATE TABLE raw_linegift_orders (order_id TEXT PRIMARY KEY, status TEXT, sku_code TEXT, stock_count INTEGER, selling_price REAL, fee REAL, bought_date_jst TEXT, received_date_jst TEXT,
    bought_on_unix INTEGER, received_on_unix INTEGER, first_seen_at TEXT, last_seen_at TEXT, is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE f_sales_by_listing (日付 TEXT, モール TEXT, 売上金額 REAL);
  CREATE TABLE m_products (商品コード TEXT, 原価状態 TEXT, 原価 REAL);
  CREATE TABLE dq_run_results (run_id TEXT, check_name TEXT, severity TEXT, actual_value REAL, threshold_value REAL, details_json TEXT, checked_at TEXT, PRIMARY KEY (run_id, check_name));`;
const unix = (d) => (d ? Math.floor(Date.parse(`${d}T03:00:00Z`) / 1000) : null);
function mkDb(file = ':memory:') {
  const db = new Database(file); db.exec(DDL); db.exec(FACT_DDL);
  let n = 0;
  const order = (bought, received, price, { status = 'received', qty = 1, sku = 'sku-a' } = {}) => db.prepare(`INSERT INTO raw_linegift_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`)
    .run(`o${++n}`, status, sku, qty, price, Math.round(price * qty * 0.1295), bought, received, unix(bought), unix(received), `${bought}T03:00:00Z`, `${received || bought}T03:00:00Z`);
  const listing = (date, jpy) => db.prepare(`INSERT INTO f_sales_by_listing VALUES (?, 'linegift', ?)`).run(date, jpy);
  // fact = raw (received の whitelist) を (受取日 × 正規化 SKU) に集約したもの。売上の式は本物の build SQL と同じ (CAST・LOWER(TRIM())・ROUND(…, 2))。原価などは DQ のほかの検査が通る値
  const buildFact = () => {
    db.exec(`DELETE FROM f_linegift_finance_sku_daily_v1`);
    db.exec(`INSERT INTO f_linegift_finance_sku_daily_v1 (date_jst, sku_code, ne_code, resolution_method, units_ordered, units_net_sold, sales_principal_jpy_incl, gross_sales_jpy_incl, mall_fee_jpy_incl,
               shipping_quality, unit_cost_snapshot_incl, cogs_amount_jpy_incl, cost_status, is_cost_complete, order_count, line_count)
             SELECT received_date_jst, LOWER(TRIM(sku_code)), LOWER(TRIM(sku_code)), 'master_match', SUM(CAST(stock_count AS INTEGER)), SUM(CAST(stock_count AS INTEGER)),
                    ROUND(SUM(CAST(selling_price AS REAL) * CAST(stock_count AS INTEGER)), 2), ROUND(SUM(CAST(selling_price AS REAL) * CAST(stock_count AS INTEGER)), 2), COALESCE(SUM(fee), 0),
                    'no_shipping_in_api', 100, 100 * SUM(CAST(stock_count AS INTEGER)), 'complete', 1, COUNT(DISTINCT order_id), COUNT(*)
               FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} AND strftime('%Y%m', received_date_jst) IS NOT NULL GROUP BY 1, 2`);   // 月の条件 (strftime) が読めない受取日は、本物の build でも fact に入らない
  };
  return { db, order, listing, buildFact };
}
/** 月末の受注が翌月の受取に流れる月 (受注 100 万円・受取日の月なら 94.5 万円 = 5.5% のずれ) */
function monthEndFlow(h, ym) {
  for (let d = 1; d <= 27; d++) h.order(`${ym}-${String(d).padStart(2, '0')}`, `${ym}-${String(d + 1).padStart(2, '0')}`, 35000);
  const [y, m] = ym.split('-').map(Number); const next = `${m === 12 ? y + 1 : y}-${String(m === 12 ? 1 : m + 1).padStart(2, '0')}`;
  h.order(`${ym}-30`, `${next}-01`, 30000); h.order(`${ym}-31`, `${next}-02`, 25000);
  h.listing(`${ym}-15`, 1000000);
}

console.log('比べ方 (listing_diff_pct)');
t('🚨 月末の受注が翌月の受取に流れる月: 受取日の月で比べると 5% を超えるが、受注日の月どうしなら一致する (9/21 の 8 月の ❌ の形)', () => {
  const h = mkDb(); monthEndFlow(h, '2026-08'); h.order('2026-07-31', '2026-08-01', 2000); h.listing('2026-07-31', 2000); h.buildFact();
  const r = linegiftListingDiff(h.db, '2026-08');
  assert.deepEqual([r.listingAvail, r.listingJpy, r.boughtBasisJpy, r.receivedBasisFactJpy, r.receivedWithoutBoughtDate], [true, 1000000, 1000000, 27 * 35000 + 2000, 0]);
  assert.equal(r.diffPct, 0);
  assert.ok(r.receivedBasisDiffPct > 5 && r.receivedBasisDiffPct < 6, `今までの比べ方の差 = ${r.receivedBasisDiffPct}`);
  h.db.close();
});
t('本当に欠けている月は、受注日の月どうしでも差が出る (取込の穴を見逃さない): 受注 100 万円のうち 8 万円ぶんの注文が raw に無い → 8% = error', () => {
  const h = mkDb();
  for (let d = 1; d <= 23; d++) h.order(`2026-08-${String(d).padStart(2, '0')}`, `2026-08-${String(d).padStart(2, '0')}`, 40000);
  h.listing('2026-08-15', 1000000); h.buildFact();
  const r = linegiftListingDiff(h.db, '2026-08');
  assert.equal(Math.round(r.diffPct * 1000) / 1000, 8);
  assert.equal(listingDiffSeverity(r.diffPct, { warn: 1, error: 5 }), 'error');
  h.db.close();
});
t('数えるのは fact と同じ行だけ (received・数量と価格が正・SKU あり)。状態が received でも cancel でもない受注は not_received に出す。受注日の空の received 行は件数を出す', () => {
  const h = mkDb();
  h.order('2026-09-01', '2026-09-02', 1000, { qty: 3 });
  h.order('2026-09-03', null, 5000, { status: 'gift_message_send' }); h.order('2026-09-04', null, 7000, { status: 'payment' }); h.order('2026-09-05', null, 9000, { status: 'cancel' });
  h.order('2026-09-06', '2026-09-06', 0); h.order('2026-09-06', '2026-09-06', 800, { qty: 0 }); h.order('2026-09-06', '2026-09-06', 800, { sku: ' ' }); h.order('2026-09-06', null, 800);
  h.db.prepare(`INSERT INTO raw_linegift_orders (order_id, status, sku_code, stock_count, selling_price, bought_date_jst, received_date_jst) VALUES ('nob', 'received', 'sku-a', 1, 400, NULL, '2026-09-07')`).run();
  h.listing('2026-09-10', 15000); h.buildFact();
  const r = linegiftListingDiff(h.db, '2026-09');
  assert.deepEqual([r.boughtBasisJpy, r.receivedBasisFactJpy, r.notReceivedJpy, r.receivedWithoutBoughtDate], [3000, 3400, 12000, 1]);
  h.db.close();
});
t('listing が無い月は listingAvail = false・diffPct は null (判定は info)。月の形が違えば例外', () => {
  const { db } = mkDb();
  const r = linegiftListingDiff(db, '2026-08');
  assert.deepEqual([r.listingAvail, r.diffPct, listingDiffSeverity(r.diffPct, { warn: 1, error: 5 })], [false, null, 'info']);
  assert.throws(() => linegiftListingDiff(db, '2026-8'), /YYYY-MM/);
  assert.throws(() => linegiftFactRawMismatch(db, '202608'), /YYYY-MM/);
  db.close();
});
t('しきい値: 過去月は厳しいまま・当月と前月の月初 (recent_past) は当月用 (受取がまだの受注が fact に居ないぶん構造的に小さい)。重複期間は info', () => {
  const past = { warn: 1, error: 5 }, cur = { warn: 5, error: 15 };
  assert.deepEqual(['past', 'recent_past', 'current'].map((m) => listingDiffThreshold(m, past, cur)), [past, cur, cur]);
  assert.deepEqual([0.2, 1.5, 5.06, 16].map((v) => listingDiffSeverity(v, past)), ['info', 'warn', 'error', 'error']);
  assert.deepEqual([4.9, 5.4, 16].map((v) => listingDiffSeverity(v, cur)), ['info', 'warn', 'error']);
  assert.equal(listingDiffSeverity(40, past, { isDuplicatePeriod: true }), 'info');
});
t('🚨 fact に入る行の条件と売上の式は build SQL と同じ: ソースとずれたら落ちる', () => {
  const sql = fs.readFileSync(path.join(root, 'sql', 'linegift', 'build_f_linegift_finance_sku_daily_v1.sql'), 'utf8').replace(/\r\n/g, '\n');
  const m = sql.match(/FROM raw_linegift_orders\nWHERE ([\s\S]*?);\nCREATE INDEX _silver_linegift_v1_idx/);
  assert.ok(m, 'build SQL の Step 1 の WHERE が見つからない');
  const norm = (x) => x.replace(/\s+/g, ' ').trim();
  const conds = norm(m[1]).split(' AND ');
  assert.equal(conds.filter((c) => !/strftime\('%Y%m', received_date_jst\)/.test(c)).join(' AND '), norm(FACT_WHITELIST_SQL));
  // fact ↔ raw の検査 (linegiftFactRawMismatch) が写している式: 月の条件・SKU の正規化・数量と価格の CAST・行の式・丸め・集約の鍵
  for (const frag of [`CAST(strftime('%Y%m', received_date_jst) AS INTEGER) = :year_month_int`, 'LOWER(TRIM(sku_code)) AS sku_code', 'CAST(stock_count AS INTEGER) AS quantity', 'CAST(selling_price AS REAL) AS unit_price',
    'SUM(unit_price * quantity) AS sales_principal_jpy_incl', 'ROUND(c.sales_principal_jpy_incl, 2) AS gross_sales_jpy_incl', 'GROUP BY date_jst, sku_code']) assert.ok(norm(sql).includes(frag), `build SQL に無い: ${frag}`);
});

console.log('fact ↔ raw の一致 (fact_raw_mismatch_keys)');
t('🚨 raw と listing はそろっていて fact だけ欠けている (Codex #1398 R1 の再現: 旧 Check 2 なら 8% で error だった) → 受注日の月どうしの差は 0 のまま・fact ↔ raw が食い違いを数える', () => {
  const h = mkDb();
  for (let d = 1; d <= 25; d++) h.order(`2026-08-${String(d).padStart(2, '0')}`, `2026-08-${String(d).padStart(2, '0')}`, 40000);
  h.listing('2026-08-15', 1000000); h.buildFact();
  assert.deepEqual([linegiftFactRawMismatch(h.db, '2026-08').mismatched, linegiftFactRawMismatch(h.db, '2026-08').keys], [0, 25]);
  h.db.exec(`DELETE FROM f_linegift_finance_sku_daily_v1 WHERE date_jst IN ('2026-08-10', '2026-08-11')`);   // fact の build が途中で欠けた形
  assert.equal(linegiftListingDiff(h.db, '2026-08').diffPct, 0);
  const r = linegiftFactRawMismatch(h.db, '2026-08');
  assert.deepEqual([r.mismatched, r.missingInFact, r.extraInFact, r.amountDiffers, r.rawJpy, r.factJpy, r.examples.map((x) => [x.date, x.kind])], [2, 2, 0, 0, 1000000, 920000, [['2026-08-10', 'missing_in_fact'], ['2026-08-11', 'missing_in_fact']]]);
  h.db.close();
});
t('金額だけ古い (行数は同じ)・raw から消えた行が fact に残っている、も数える。SKU の大小文字と前後の空白・小数の価格・文字で入った数量は build と同じ式で寄せる = 食い違いにしない。読めない受取日は build と同じく数えない', () => {
  const h = mkDb();
  h.order('2026-08-01', '2026-08-02', 100.5, { qty: 3, sku: ' SKU-A ' }); h.order('2026-08-01', '2026-08-02', 200.25, { sku: 'sku-a' }); h.order('2026-08-03', '2026-08-04', 500, { sku: 'sku-b' });
  h.db.prepare(`UPDATE raw_linegift_orders SET stock_count = '3' WHERE order_id = 'o1'`).run();
  h.order('2026-08-05', '2026/08/06', 900, { sku: 'sku-c' });   // strftime が読めない形 → build は fact に入れない
  h.buildFact();
  assert.deepEqual([linegiftFactRawMismatch(h.db, '2026-08').mismatched, h.db.prepare(`SELECT gross_sales_jpy_incl AS p FROM f_linegift_finance_sku_daily_v1 WHERE date_jst = '2026-08-02'`).get().p], [0, 501.75]);
  h.db.exec(`UPDATE f_linegift_finance_sku_daily_v1 SET gross_sales_jpy_incl = 400 WHERE date_jst = '2026-08-04'`);   // 金額の更新漏れ
  h.db.exec(`DELETE FROM raw_linegift_orders WHERE order_id IN ('o1', 'o2')`);                                       // raw から消えた (fact に残った行)
  const r = linegiftFactRawMismatch(h.db, '2026-08');
  assert.deepEqual([r.mismatched, r.missingInFact, r.extraInFact, r.amountDiffers], [2, 0, 1, 1]);
  h.db.close();
});

console.log('DQ のスクリプト (プロセスとして起動。終了コードと全検査の記録まで)');
function runDq(dir, month) {
  const r = spawnSync(process.execPath, [path.join(root, 'apps', 'warehouse', 'run-linegift-finance-dq.js'), '--data-dir', dir, '--month', month, '--run-id', 'test-run'], { encoding: 'utf8' });
  const db = new Database(path.join(dir, 'warehouse.db'), { readonly: true });
  const rows = db.prepare(`SELECT check_name, severity, actual_value, threshold_value, details_json FROM dq_run_results WHERE run_id = 'test-run' ORDER BY check_name`).all();
  db.close();
  return { status: r.status, out: `${r.stdout}\n${r.stderr}`, rows, of: (name) => rows.find((x) => x.check_name === name) };
}
const CHECKS = ['fact_raw_mismatch_keys', 'fallback_to_item_code_rate_pct', 'fee_rate_drift_pct', 'horizon_frozen_observed_count', 'listing_diff_pct', 'missing_cost_rate_pct', 'monthless_received_rows',
  'provisional_state_age_days', 'received_missing_received_on_count', 'resolved_but_zero_cost_count', 'row_count_drift', 'shipping_missing_rate_pct', 'unmatched_sku_rate_pct', 'whitelist_coverage_pct'];
t('🚨 8 月の形 (受取日の月なら 5.5% のずれ) の過去月: 全部の検査を完走して exit 0 = 同期に進める。listing_diff_pct は info・details に両方の数字', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lg-dq-'));
  try {
    const h = mkDb(path.join(dir, 'warehouse.db')); monthEndFlow(h, '2026-01'); h.buildFact(); h.db.close();
    const r = runDq(dir, '2026-01');
    assert.equal(r.status, 0, `exit ${r.status}: ${r.out.slice(-600)}`);
    assert.deepEqual(r.rows.map((x) => x.check_name), CHECKS);
    assert.deepEqual(r.rows.filter((x) => x.severity === 'error').map((x) => x.check_name), []);
    const ld = r.of('listing_diff_pct'), d = JSON.parse(ld.details_json);
    assert.deepEqual([ld.severity, ld.actual_value, ld.threshold_value, d.basis, d.bought_basis_jpy, d.listing_jpy, d.received_basis_fact_jpy, Math.round(d.received_basis_diff_pct * 10) / 10, d.received_without_bought_date], ['info', 0, 5, 'bought_month', 1000000, 1000000, 945000, 5.5, 0]);
    assert.deepEqual([r.of('fact_raw_mismatch_keys').severity, r.of('fact_raw_mismatch_keys').actual_value], ['info', 0]);
  } finally { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ } }
});
t('🚨 raw と listing はそろっていて fact だけ欠けている月は exit 1 = 同期を止める (fact_raw_mismatch_keys が error。ほかの検査は全部通る = これが無いと同期ゲートを通っていた)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lg-dq-'));
  try {
    const h = mkDb(path.join(dir, 'warehouse.db'));
    for (let d = 1; d <= 25; d++) h.order(`2026-01-${String(d).padStart(2, '0')}`, `2026-01-${String(d).padStart(2, '0')}`, 40000);
    h.listing('2026-01-15', 1000000); h.buildFact();
    h.db.exec(`DELETE FROM f_linegift_finance_sku_daily_v1 WHERE date_jst IN ('2026-01-10', '2026-01-11')`); h.db.close();
    const r = runDq(dir, '2026-01');
    assert.equal(r.status, 1, `exit ${r.status}: ${r.out.slice(-600)}`);
    assert.deepEqual(r.rows.map((x) => x.check_name), CHECKS);
    assert.deepEqual(r.rows.filter((x) => x.severity === 'error').map((x) => x.check_name), ['fact_raw_mismatch_keys']);
    const fm = r.of('fact_raw_mismatch_keys'), d = JSON.parse(fm.details_json);
    assert.deepEqual([fm.actual_value, d.missing_in_fact, d.raw_jpy, d.fact_jpy, r.of('listing_diff_pct').severity], [2, 2, 1000000, 920000, 'info']);
  } finally { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ } }
});
t('listing が無い月でも fact ↔ raw の検査は省かない', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lg-dq-'));
  try {
    const h = mkDb(path.join(dir, 'warehouse.db'));
    for (let d = 1; d <= 5; d++) h.order(`2026-01-0${d}`, `2026-01-0${d}`, 40000);
    h.buildFact(); h.db.exec(`UPDATE f_linegift_finance_sku_daily_v1 SET gross_sales_jpy_incl = 1 WHERE date_jst = '2026-01-03'`); h.db.close();
    const r = runDq(dir, '2026-01');
    assert.deepEqual([r.status, r.of('listing_diff_pct').severity, JSON.parse(r.of('listing_diff_pct').details_json).skipped, r.of('fact_raw_mismatch_keys').severity, r.of('fact_raw_mismatch_keys').actual_value], [1, 'info', true, 'error', 1]);
  } finally { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ } }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
