#!/usr/bin/env node
/**
 * measure-amazon-finance.mjs — Amazon 財務 (F2b) を Render に送る前の容量の見積り (設計 12 §7 / D-W5)。warehouse.db は読むだけ・Render には触れない
 *
 * 送り手 (apps/company-db/push/amazon-finance.mjs) と同じ鍵の選び方・同じ集約で集合を作り、PGlite (0001〜の migration) に受け口と同じ手順で入れて測る:
 *   ① 初回の投入: 表 (core.order_finance_daily / order_finance_receipts・索引こみ) の大きさ・DB 全体の増え・1 行あたり
 *   ② 全部の注文をもう 1 回置き換える (変換の版を変えて delete → insert = 死んだ行の分の一時の増え。vacuum しない)
 *   ③ 日次の view (mart.v_finance_daily) の時間: 最後の 1 か月 / 全期間。月の手数料の view も
 * 出力の「財務の 1 行 (表 + 索引)」= CDB_FINANCE_ROW_BYTES・「受領の 1 注文」= CDB_FINANCE_ORDER_BYTES・置き換えの倍率 = CDB_FINANCE_REPLACE_FACTOR (送り手の容量の見張りの値)
 * 🚨 PGlite は本物の Postgres と大きさが少し違う (ページの詰め方は同じ・TOAST も同じ)。Render での 1 か月の試しでも測る (§7)
 *
 * 使い方 (miniPC): node scripts/company-db/measure-amazon-finance.mjs --from 2026-08-01 --to 2026-08-31 [--pglite-dir <空のフォルダ>]
 *                  node scripts/company-db/measure-amazon-finance.mjs --all   (全部の注文・疑似注文)
 * env: DATA_DIR
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './migrate.mjs';
import { makeIterate, makeBuild } from '../../apps/company-db/push/amazon-finance.mjs';
import { validateFinanceChunk, ingestOrderFinanceChunk } from '../../apps/company-db/ingest/order-finance.mjs';
import { AMAZON_FINANCE_TRANSFORM_VERSION } from '../../apps/company-db/push/amazon-finance-transform.mjs';
import { isDate } from '../../apps/company-db/push/pipeline.mjs';

const args = process.argv.slice(2);
const arg = (k) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; };
const all = args.includes('--all');
const from = arg('--from'), to = arg('--to');
if (!all && !(isDate(from) && isDate(to) && from <= to)) { console.error('--from YYYY-MM-DD --to YYYY-MM-DD か --all'); process.exit(2); }
const dataDir = (process.env.DATA_DIR || '').trim();
if (!dataDir) { console.error('DATA_DIR が無い'); process.exit(2); }
// 既定 = メモリ上 (ディスク上の PGlite は 1 か月で 1.5 時間以上かかった)。--pglite-dir <空のフォルダ> でディスク上
const pgDir = arg('--pglite-dir');
if (pgDir && fs.existsSync(pgDir) && fs.readdirSync(pgDir).length) { console.error(`--pglite-dir は空のフォルダ (${pgDir})`); process.exit(2); }

const mb = (b) => `${(Number(b) / 1048576).toFixed(1)} MB`;
const t0 = Date.now();
const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true });
const pg = pgDir ? new PGlite(pgDir) : new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: () => {} });
const db = pgliteAdapter(pg);
const size = async () => {
  const r = (await pg.query(`select pg_database_size(current_database()) as db, pg_total_relation_size('core.order_finance_daily') as daily, pg_total_relation_size('core.order_finance_receipts') as receipts,
    pg_relation_size('core.order_finance_daily') as heap, (select count(*) from core.order_finance_daily) as n_rows, (select count(*) from core.order_finance_receipts) as n_orders`)).rows[0];
  return Object.fromEntries(Object.entries(r).map(([k, v]) => [k, Number(v)]));
};

// ① 集合を作る (送り手と同じ = dry-run の iterate / build)
const run = { stats: { lines: 0, rawRows: 0, dedupRows: 0, maxLines: 0, maxLinesKey: null, maxBytes: 0, maxBytesKey: null, unmapped: { rows: 0, columns: {}, exampleIds: [] } }, noteChanged: () => {}, persistBeforeSend: () => {}, buildFailed: [],
  built: new Set(), receipts: { push: () => 0 } };   // makeBuild が使う (作れた注文・受領記録)。ここでは使わない (#1567 で足した入れ物が無いと全部の注文が「整形できない」になった)
const sel = all ? { mode: 'full', extraKeys: [] } : { mode: 'range', from, to, extraKeys: [] };
const stats = {};
const build = makeBuild(run);
const payloads = []; const errors = [];
warehouse.exec('begin');
try {
  for (const g of makeIterate(sel, run)(warehouse, stats, { fps: new Map(), pre: null, dryRun: true })) {
    try { payloads.push(build(g, { fps: new Map() }).payload); } catch (e) { errors.push(`${g.key}: ${e.message}`); }
  }
} finally { warehouse.exec('rollback'); }
const tBuild = Date.now();
console.log(`集合: 注文 ${stats.selectedOrders} + 疑似注文 ${stats.selectedPseudo} → 財務の行 ${run.stats.lines} (決済の行 ${run.stats.rawRows} → 重複除去 ${run.stats.dedupRows})・1 注文の最大 ${run.stats.maxLines} 行 (${run.stats.maxLinesKey})・最大の JSON ${Math.round(run.stats.maxBytes / 1024)} KB・整形できない ${errors.length}・鍵の分からない不正な行 ${stats.unkeyed.length}・拾われない金額の行 ${run.stats.unmapped.rows} ${JSON.stringify(run.stats.unmapped.columns)} (${((tBuild - t0) / 1000).toFixed(1)} 秒)`);
for (const e of errors.slice(0, 10)) console.log(`  整形できない: ${e}`);

// ② PGlite に入れる (受け口と同じ = validateFinanceChunk → ingestOrderFinanceChunk・200 注文ずつ)
const s0 = await size();
let seq = 0, runN = 0;   // run は chunk ごとに別 (番号は通し = 2 回目の投入と重ならない)
const load = async (tv) => {
  for (let i = 0; i < payloads.length; i += 200) {
    const rows = payloads.slice(i, i + 200).map((p) => ({ ...p, header: { ...p.header, transform_version: tv } }));
    const body = { run_id: `ship_${String(Date.now()).padStart(15, '0').slice(-15)}_${String(++runN).padStart(6, '0').slice(-6)}`, batch_seq: ++seq, chunk_index: 0, last: true, transform_version: tv, rows };
    const r = await ingestOrderFinanceChunk(db, { ...validateFinanceChunk(body), host: 'measure', log: () => {} });
    if (r.failed.length) throw new Error(`投入に失敗: ${JSON.stringify(r.failed[0])}`);
  }
};
await load(AMAZON_FINANCE_TRANSFORM_VERSION);
const s1 = await size();
const tLoad = Date.now();
await load(`${AMAZON_FINANCE_TRANSFORM_VERSION}_m`);   // 置き換え (版が違う = delete → insert)
const s2 = await size();
const tReplace = Date.now();
console.log(`① 初回: 行 ${s1.n_rows.toLocaleString()} / 注文 ${s1.n_orders.toLocaleString()}・表 ${mb(s1.daily)} (本体 ${mb(s1.heap)})・受領 ${mb(s1.receipts)}・DB の増え ${mb(s1.db - s0.db)} (${((tLoad - tBuild) / 1000).toFixed(1)} 秒)`);
console.log(`   → CDB_FINANCE_ROW_BYTES = 財務の 1 行 (表 + 索引) ${Math.round(s1.daily / Math.max(s1.n_rows, 1))} B / CDB_FINANCE_ORDER_BYTES = 受領の 1 注文 ${Math.round(s1.receipts / Math.max(s1.n_orders, 1))} B (受領の run の記録などを含めた DB の増え ÷ 行 = ${Math.round((s1.db - s0.db) / Math.max(s1.n_rows, 1))} B)`);
console.log(`② 置き換え (vacuum 前): 表 ${mb(s2.daily)}・DB ${mb(s2.db)}・初回からの増え ${mb(s2.db - s1.db)}・倍率 (置き換え後 ÷ 初回の DB の増え) ${((s2.db - s0.db) / Math.max(s1.db - s0.db, 1)).toFixed(2)} (${((tReplace - tLoad) / 1000).toFixed(1)} 秒)`);

// ③ view の時間
const range = (await pg.query(`select min(economic_date_jst)::text as a, max(economic_date_jst)::text as b from core.order_finance_daily where line_kind = 'sku'`)).rows[0];
const timed = async (label, sql, p) => { const a = Date.now(); const r = await pg.query(sql, p); console.log(`③ ${label}: ${r.rows.length.toLocaleString()} 行・${((Date.now() - a) / 1000).toFixed(2)} 秒`); };
if (range.b) {
  const lastMonth = range.b.slice(0, 7);
  await timed(`日次の財務 ${lastMonth}`, `select * from mart.v_finance_daily where economic_date_jst between $1::date and ($1::date + interval '1 month' - interval '1 day')::date`, [`${lastMonth}-01`]);
  await timed(`日次の財務 全期間 ${range.a}〜${range.b}`, `select * from mart.v_finance_daily`, []);
  await timed('月の手数料 全期間', `select * from mart.v_finance_account_fees_monthly`, []);
}
console.log(`PGlite = ${pgDir || 'メモリ'}${pgDir ? ' (測り終えたら消してよい)' : ''}。全体 ${((Date.now() - t0) / 1000).toFixed(1)} 秒`);
await pg.close();
warehouse.close();
