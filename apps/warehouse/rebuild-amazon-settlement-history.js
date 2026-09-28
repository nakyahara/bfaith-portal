#!/usr/bin/env node
/**
 * rebuild-amazon-settlement-history.js — 決済の重複除去を直した (出現順つき・2026-09-28) 後に、過去の集計を作り直す (1 回きり)
 *
 * 毎朝の daily-sync は「月の集計 = 未処理の月だけ」「日次の財務 = 当月 + 20 日までの前月だけ」を作り直す
 * → 直す前に作った過去の月は、数え落としたままの数字で残る。これを全部作り直して Render に送り直し、最後に照合する。
 *
 *   0. 前提の確認: 決済の行に 文書 / 行番号 の空が無い (出現順を数えられない行があれば止める)
 *   1. 月の集計 (fact_amazon_settlement_monthly_long / wide): rebuild-amazon-settlement-mart.js --all
 *   2. 日次の財務 (f_amazon_finance_sku_daily_v1): build-daily-fact.js --month を月ごと → sync-amazon-finance-daily.js --month で Render へ
 *      (原価の snapshot 列は UPSERT で温存 = 個数・金額・原価の合計・利益だけ直る)
 *   3. アカウント単位の手数料: rebuild-amazon-account-fees.js + sync-amazon-account-fees.js (毎朝も 14 か月分やるが、ここで今すぐ)
 *   4. 照合: 決済ごとに、出現順つきの重複除去の後の合計 = 振込額 (total-amount)。1 つでも合わなければ終了コード 1
 *
 * 使い方 (miniPC・daily-sync の時間を避ける):
 *   node -r dotenv/config apps/warehouse/rebuild-amazon-settlement-history.js --data-dir C:/Users/bfaith/bfaith-portal/data
 *   --check-only で 0 と 4 だけ (何も書かない) / --no-sync で Render に送らない / --from 2026-01 --to 2026-09 で日次の財務の月を絞る
 * 台帳: config/jobs-registry.mjs の settlement-history-rebuild (temporary_asset)
 */
import path from 'node:path';
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const DATA_DIR = (getArg('--data-dir') || process.env.DATA_DIR || '').trim().replace(/\\/g, '/');
const checkOnly = args.includes('--check-only');
const noSync = args.includes('--no-sync');
if (!DATA_DIR) { console.error('FATAL: --data-dir か DATA_DIR が要る'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db が無い: ${dbPath}`); process.exit(2); }
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');

const AMT = `(COALESCE(price_amount_micro,0) + COALESCE(item_related_fee_amount_micro,0) + COALESCE(promotion_amount_micro,0) + COALESCE(other_amount_micro,0) + COALESCE(shipment_fee_amount_micro,0) + COALESCE(order_fee_amount_micro,0) + COALESCE(misc_fee_amount_micro,0) + COALESCE(other_fee_amount_micro,0) + COALESCE(direct_payment_amount_micro,0))`;

function preflight(db) {
  const r = db.prepare(`SELECT COUNT(*) n, SUM(source_document_id IS NULL) null_doc, SUM(source_line_no IS NULL) null_line FROM raw_amazon_settlement_lines`).get();
  console.log(`[0] 決済の行 ${r.n} 行・文書の空 ${r.null_doc || 0}・行番号の空 ${r.null_line || 0}`);
  if (r.null_doc || r.null_line) { console.error('FATAL: 文書 / 行番号の空がある = 出現順を数えられない (同じ鍵の行を 1 行に潰すか数えすぎる)。先に原因を調べる'); process.exit(1); }
}

/** 決済ごとに 出現順つきの重複除去 (v_amazon_settlement_unified と同じ規則) の合計 = 振込額 か */
function check(db) {
  const heads = db.prepare(`SELECT source_settlement_id s, MAX(total_amount_micro) t, MIN(settlement_start_date) st FROM raw_amazon_settlement_headers GROUP BY 1 ORDER BY st`).all();
  const one = db.prepare(`
    WITH o AS (SELECT l.*, DENSE_RANK() OVER (PARTITION BY business_line_key, source_document_id ORDER BY source_line_no) AS occ FROM raw_amazon_settlement_lines l WHERE source_settlement_id = ?),
    d AS (SELECT ${AMT} a, ROW_NUMBER() OVER (PARTITION BY business_line_key, occ ORDER BY CASE source_layer WHEN 'sp_api_v1' THEN 1 WHEN 'sp_api_v2' THEN 1 WHEN 'manual_csv' THEN 2 ELSE 3 END, ingested_at DESC, source_document_id) rn FROM o)
    SELECT COUNT(*) n, SUM(a) a FROM d WHERE rn = 1`);
  let ok = 0; const bad = [];
  for (const h of heads) {
    const r = one.get(h.s);
    if (!r.n) { bad.push([String(h.st).slice(0, 10), '明細が無い']); continue; }
    if (r.a === h.t) ok++; else bad.push([String(h.st).slice(0, 10), `差 ${(r.a - h.t) / 1e6} 円`]);
  }
  console.log(`[4] 照合: 決済 ${heads.length} 件のうち振込額と一致 ${ok} 件`);
  for (const b of bad) console.log(`    ❌ ${b.join(' ')}`);
  return bad.length === 0;
}

function run(label, script, scriptArgs) {
  const t0 = Date.now();
  console.log(`\n=== ${label}: node ${script} ${scriptArgs.join(' ')}`);
  const out = execFileSync(process.execPath, [script, ...scriptArgs], { cwd: repoRoot, env: { ...process.env, DATA_DIR, CHUNK_SIZE: process.env.CHUNK_SIZE || '3000' }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 64 * 1024 * 1024 });
  const tail = out.trim().split('\n').slice(-3).join('\n    ');
  console.log(`    ${tail}\n    (${Math.round((Date.now() - t0) / 1000)} 秒)`);
}

const db = new Database(dbPath, { readonly: true, fileMustExist: true });
db.pragma('busy_timeout = 10000');
preflight(db);
const months = db.prepare(`SELECT DISTINCT year_month_int ym FROM raw_amazon_settlement_lines WHERE year_month_int IS NOT NULL ORDER BY 1`).all().map((r) => `${String(r.ym).slice(0, 4)}-${String(r.ym).slice(4, 6)}`);
db.close();
const from = getArg('--from') || months[0], to = getArg('--to') || months.at(-1);
const target = months.filter((m) => m >= from && m <= to);
console.log(`[0] 決済の行がある月: ${months.join(', ')} / 日次の財務を作り直す月: ${target.join(', ') || 'なし'}`);

if (!checkOnly) {
  run('[1] 月の集計', 'apps/warehouse/rebuild-amazon-settlement-mart.js', ['--all']);
  for (const m of target) {
    run(`[2] 日次の財務 ${m}`, 'scripts/amazon-finance/build-daily-fact.js', ['--data-dir', DATA_DIR, '--month', m]);
    if (!noSync) run(`[2] Render へ ${m}`, 'apps/warehouse/sync-amazon-finance-daily.js', ['--data-dir', DATA_DIR, '--month', m]);
  }
  // 手数料は「今日から N か月」で指定する = 決済の行がある最初の月まで届く月数
  const now = new Date(Date.now() + 9 * 3600 * 1000);
  const [fy, fm] = months[0].split('-').map(Number);
  const monthsBack = Math.min((now.getUTCFullYear() - fy) * 12 + (now.getUTCMonth() + 1 - fm) + 1, 60);
  run('[3] アカウント単位の手数料', 'apps/warehouse/rebuild-amazon-account-fees.js', ['--data-dir', DATA_DIR, '--months', String(monthsBack)]);
  if (!noSync) run('[3] Render へ', 'apps/warehouse/sync-amazon-account-fees.js', ['--data-dir', DATA_DIR, '--months', String(monthsBack)]);
}
const db2 = new Database(dbPath, { readonly: true, fileMustExist: true });
db2.pragma('busy_timeout = 10000');
const allOk = check(db2);
db2.close();
process.exit(allOk ? 0 : 1);
