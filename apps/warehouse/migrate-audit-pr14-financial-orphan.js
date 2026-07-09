/**
 * migrate-audit-pr14-financial-orphan.js — Amazon financial_* 孤児系統の廃止 (監査PR-14/F-8。一回きり・手動実行)
 *
 * 対象 (2026-07-09 本番で依存チェーンを実測追跡済み):
 *   孤児view 5本 (コード読者0件、DB残留のみ。validation は db.js から作成コード除去済み):
 *     v_settlement_v3_v4_validation      … V3→V4切替時の突合用 (役目終了)
 *     v_amazon_sku_profit_actual         … v3 (SKU別)
 *     v_amazon_profit_actual             … v3 (月次)
 *     fact_amazon_order_level_resolved   … financial_lines 直読み
 *     fact_amazon_account_level_summary  … financial_lines 直読み
 *   孤児table 3本:
 *     raw_amazon_financial_lines   1,645,727行 … 運用停止した旧実験系 (SSoT=settlement系、監査F-8)
 *     raw_amazon_financial_events    142,131行
 *     bridge_amazon_order_sale_month       0行 … CREATEのみで書き手/読み手なし (db.js作成コード除去済み)
 *   ※ 現役の v_amazon_sku_profit_actual_v4 / v_amazon_monthly_full_summary は settlement系のみに
 *     依存しており無関係 (単語境界precise checkで v3 非参照を確認済み)
 *
 * 安全策:
 *   ・view は DROP 前に全 DDL を dropped-views.sql として保存 (復元=そのまま実行)
 *   ・非空 table は iterate+gzip ストリーミングで全量ダンプ (行数検証付き。1.65M行でもOOMしない)
 *   ・DROP は view→table の順、検証付き
 *
 * 実行: cd C:\Users\bfaith\bfaith-portal && node apps/warehouse/migrate-audit-pr14-financial-orphan.js
 *       --dry-run で対象表示のみ
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { initDB, getDB } from './db.js';

const isDryRun = process.argv.includes('--dry-run');
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const stamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
const BACKUP_DIR = path.join(DATA_DIR, 'backup', `pr14-financial-orphan-${stamp}`);

const DROP_VIEWS = [
  'v_settlement_v3_v4_validation',
  'v_amazon_sku_profit_actual',
  'v_amazon_profit_actual',
  'fact_amazon_order_level_resolved',
  'fact_amazon_account_level_summary',
];
const DROP_TABLES = [
  'raw_amazon_financial_lines',
  'raw_amazon_financial_events',
  'bridge_amazon_order_sale_month',
];

await initDB();
const db = getDB();

// ─── 0) 事前ガード: 現役view(v4系)が削除対象を参照していないことを機械検証 ───
const targetsPattern = [...DROP_VIEWS, ...DROP_TABLES];
const refPrecise = (sql, name) => new RegExp(`${name}(?![A-Za-z0-9_])`).test(sql || '');
const survivors = db.prepare("SELECT name, sql FROM sqlite_master WHERE type='view'").all()
  .filter(v => !DROP_VIEWS.includes(v.name));
let guardNg = false;
for (const v of survivors) {
  for (const t of targetsPattern) {
    if (refPrecise(v.sql, t)) { console.error(`ABORT: 現役view ${v.name} が削除対象 ${t} を参照`); guardNg = true; }
  }
}
if (guardNg) process.exit(1);
console.log('[pr14] 事前ガード: 現役viewからの参照なし ✅');

const plan = { views: [], tables: [] };
for (const v of DROP_VIEWS) {
  const r = db.prepare("SELECT sql FROM sqlite_master WHERE type='view' AND name=?").get(v);
  if (r) plan.views.push({ name: v, sql: r.sql });
  else console.log(`  skip (view不存在): ${v}`);
}
for (const t of DROP_TABLES) {
  const r = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?").get(t);
  if (!r) { console.log(`  skip (table不存在): ${t}`); continue; }
  const n = db.prepare(`SELECT COUNT(*) c FROM "${t}"`).get().c;
  plan.tables.push({ name: t, sql: r.sql, n });
}
console.log(`[pr14] dry-run=${isDryRun}`);
for (const v of plan.views) console.log(`  DROP VIEW対象: ${v.name}`);
for (const t of plan.tables) console.log(`  DROP TABLE対象: ${t.name} (${t.n.toLocaleString()}行)`);

if (isDryRun) { console.log('[pr14] dry-run 終了 (変更なし)'); process.exit(0); }

fs.mkdirSync(BACKUP_DIR, { recursive: true });

// ─── 1) view DDL 保存 ───
const viewsSql = plan.views.map(v => v.sql + ';\n').join('\n');
fs.writeFileSync(path.join(BACKUP_DIR, 'dropped-views.sql'), viewsSql);
console.log(`[pr14] view DDL保存: dropped-views.sql (${plan.views.length}本)`);

// ─── 2) 非空 table をストリーミングdump (iterate+gzip、行数検証) ───
for (const t of plan.tables) {
  if (t.n === 0) continue;
  const out = path.join(BACKUP_DIR, `${t.name}.dump.gz`);
  const gz = zlib.createGzip({ level: 6 });
  const sink = fs.createWriteStream(out + '.tmp');
  gz.pipe(sink);
  gz.write(`-- DDL\n${t.sql};\n-- ROWS(jsonl)\n`);
  let dumped = 0;
  for (const row of db.prepare(`SELECT * FROM "${t.name}"`).iterate()) {
    if (!gz.write(JSON.stringify(row) + '\n')) await new Promise((r) => gz.once('drain', r));
    dumped++;
  }
  await new Promise((resolve, reject) => {
    sink.on('finish', resolve); sink.on('error', reject); gz.on('error', reject); gz.end();
  });
  if (dumped !== t.n) { fs.rmSync(out + '.tmp', { force: true }); throw new Error(`ABORT: ${t.name} ダンプ行数不一致 (${dumped} != ${t.n})`); }
  fs.renameSync(out + '.tmp', out);
  console.log(`[pr14] dump: ${t.name} (${dumped.toLocaleString()}行) → ${path.basename(out)}`);
}

// ─── 3) DROP (view→table の順、1トランザクション) ───
const tx = db.transaction(() => {
  for (const v of plan.views) db.exec(`DROP VIEW "${v.name}"`);
  for (const t of plan.tables) db.exec(`DROP TABLE "${t.name}"`);
});
tx();

// ─── 4) 検証 ───
let ok = true;
for (const x of [...plan.views.map(v => v.name), ...plan.tables.map(t => t.name)]) {
  if (db.prepare("SELECT 1 FROM sqlite_master WHERE name=?").get(x)) { console.error(`NG: ${x} が残存`); ok = false; }
}
// 現役v4系が壊れていないこと (SELECT 1件で実クエリ検証)。
// エラーメッセージが削除対象名を含む場合のみ「DROP起因の破損」として失敗。
// それ以外 (fact_ad_spend 等、削除と無関係な依存欠如=ローカル検証環境) は warn 扱い。
for (const v of ['v_amazon_sku_profit_actual_v4', 'v_amazon_monthly_full_summary']) {
  try {
    db.prepare(`SELECT * FROM ${v} LIMIT 1`).get();
    console.log(`[pr14] 現役view実クエリOK ✅: ${v}`);
  } catch (e) {
    if (targetsPattern.some(t => e.message.includes(t))) {
      console.error(`NG: 現役view ${v} がDROP起因で破損: ${e.message}`); ok = false;
    } else {
      console.warn(`[pr14] warn: ${v} 実クエリ不可 (削除と無関係な依存欠如: ${e.message})`);
    }
  }
}
try { console.log(`[pr14] wal_checkpoint: ${JSON.stringify(db.pragma('wal_checkpoint(TRUNCATE)'))}`); } catch {}
console.log(ok ? `[pr14] ✅ 完了: view${plan.views.length}本+table${plan.tables.length}本削除。バックアップ=${BACKUP_DIR}` : '[pr14] ❌ 検証NG');
process.exitCode = ok ? 0 : 1;
