import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-finance-promotion-tax.js — 日次の財務の値引きの消費税の分 (promotion_tax_jpy・2026-09-29) の試験
 *
 *   値引き (promotion_jpy) = Promotion の行の ABS の合計 (今まで通り・消費税の分 TaxDiscount も混ざる)
 *   promotion_tax_jpy = そのうち promotion_type = TaxDiscount の分。Amazon 分析の「税抜で引いた利益」で値引きから除く
 *   profit_amount (税込で引いた利益 = 今までの計算) は変えない
 *
 * 実行: node apps/warehouse/test-finance-promotion-tax.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'finance-promo-tax-test-'));
process.env.DATA_DIR = tmpDir;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const { initDB, getDB } = await import('./db.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`, YMI = Number(YM.replace('-', ''));
let n = 0;
const line = (o) => db.prepare(`INSERT INTO raw_amazon_settlement_lines (
    physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku, seller_sku_normalized, transaction_type,
    quantity_purchased, price_type, price_amount_micro, item_related_fee_type, item_related_fee_amount_micro, promotion_type, promotion_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, 'D1', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S1', ?, ?, ?, ?, 'O1', 'SKU-A', 'sku-a', 'Order', ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, `k-${n}`, n, `${YM}-05T01:00:00+00:00`, `${YM}-05 10:00:00`, `${YM}-05`, YMI,
    o.qty ?? null, o.pt ?? null, o.pa == null ? null : o.pa * 1e6, o.ft ?? null, o.fa == null ? null : o.fa * 1e6, o.prt ?? null, o.pra == null ? null : o.pra * 1e6);

line({ qty: 1 });
line({ pt: 'Principal', pa: 1000 }); line({ pt: 'Tax', pa: 100 });
line({ ft: 'Commission', fa: -110 }); line({ ft: 'FBAPerUnitFulfillmentFee', fa: -330 });
line({ prt: 'Principal', pra: -200 }); line({ prt: 'TaxDiscount', pra: -20 }); line({ prt: 'Shipping', pra: -50 });

execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
const r = db.prepare(`SELECT promotion_jpy pr, promotion_tax_jpy pt, profit_amount p, commission_jpy c, fba_fulfillment_jpy f FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-a'`).get();
ok(r && r.pr === 270 && r.pt === 20, `値引き 270 (本体 200 + 送料 50 + 税の分 20) のうち税の分 = 20 (${r && r.pr} / ${r && r.pt})`);
ok(r && r.p === 1000 - 110 - 330 - 270, `profit_amount (税込で引いた利益) は今まで通り = 1,000 − 110 − 330 − 270 = 290 (${r && r.p})`);
// Amazon 分析の税抜で引いた利益の式 = profit_amount + 課税の手数料 × 1/11 + 値引きの税の分
const ex = r.p + (r.c + r.f) / 11 + r.pt;
ok(Math.abs(ex - (1000 - 110 / 1.1 - 330 / 1.1 - 250)) < 1e-9, `税抜で引いた利益 = 1,000 − 100 − 300 − 250 = 350 (式: profit + 手数料 ÷ 11 + 税の分 = ${ex})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 値引きの税の分テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
