import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-amazon-v4-reconcile.js — 日次の財務と v4 の突き合わせ (amazon-finance-v4-reconcile.js・2026-09-29) の試験
 *
 *   決まりの違い (原価・ポイント・送料の税・返品の管理手数料・返金の範囲) を引くと、残りは 0 円になる
 *   (本番 1〜9 月も 0 円)。照合の関所 (run-amazon-finance-dq.js) は その残りで「月の合計の差」「説明できない残り」を判定する
 *   期待値は手で計算した値
 *
 * 実行: node apps/warehouse/test-amazon-v4-reconcile.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'amazon-v4-reconcile-test-'));
process.env.DATA_DIR = tmpDir;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const { initDB, getDB } = await import('./db.js');
const { reconcileMonthly, reconcileSkuTop } = await import('./amazon-finance-v4-reconcile.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`, YMI = Number(YM.replace('-', ''));
let n = 0;
const line = (o) => {
  const day = o.day ?? '05', sku = o.sku ?? 'SKU-B';
  db.prepare(`INSERT INTO raw_amazon_settlement_lines (
    physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku, seller_sku_normalized, transaction_type,
    quantity_purchased, price_type, price_amount_micro, item_related_fee_type, item_related_fee_amount_micro, promotion_type, promotion_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, 'D1', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, `k-${n}`, n, `${YM}-${day}T01:00:00+00:00`, `${YM}-${day} 10:00:00`, `${YM}-${day}`, YMI,
    o.order ?? 'O2', sku, sku.toLowerCase(), o.tt ?? 'Order',
    o.qty ?? null, o.pt ?? null, o.pa == null ? null : o.pa * 1e6, o.ft ?? null, o.fa == null ? null : o.fa * 1e6, o.prt ?? null, o.pra == null ? null : o.pra * 1e6);
};
// 2 個売って (送料の税 30・ポイント 20)、1 個は返品 (返品の管理手数料 −22・送料の返金 −300・返品の手数料 +50)、1 個はカードの支払い取り消し
line({ qty: 2 });
line({ pt: 'Principal', pa: 2000 }); line({ pt: 'Tax', pa: 200 }); line({ pt: 'Shipping', pa: 300 }); line({ pt: 'ShippingTax', pa: 30 });
line({ ft: 'Commission', fa: -220 }); line({ ft: 'FBAPerUnitFulfillmentFee', fa: -660 }); line({ ft: 'ShippingChargeback', fa: -330 }); line({ ft: 'PointsGranted', fa: -20 });
line({ prt: 'Shipping', pra: -300 }); line({ prt: 'TaxDiscount', pra: -30 });
const R = { tt: 'Refund', day: '10' };
line({ ...R, pt: 'Principal', pa: -1000 }); line({ ...R, pt: 'Tax', pa: -100 }); line({ ...R, pt: 'Shipping', pa: -300 }); line({ ...R, pt: 'RestockingFee', pa: 50 });
line({ ...R, ft: 'Commission', fa: 110 }); line({ ...R, ft: 'RefundCommission', fa: -22 }); line({ ...R, ft: 'ShippingChargeback', fa: 330 });
line({ ...R, prt: 'Shipping', pra: 300 }); line({ ...R, prt: 'TaxDiscount', pra: 30 });
const C = { tt: 'Chargeback Refund', day: '12' };
line({ ...C, pt: 'Principal', pa: -1000 }); line({ ...C, pt: 'Tax', pa: -100 });
line({ ...C, ft: 'Commission', fa: 110 }); line({ ...C, ft: 'RefundCommission', fa: -22 });
db.prepare(`INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, 原価, updated_at) VALUES ('sku-b', 'B', '単品', 'ok', 400, 't')`).run();
// v4 は SKU 別の広告費の表を参照する (本番は広告の取込が作る・ここでは空の表だけ)
db.exec(`CREATE TABLE IF NOT EXISTS fact_ad_spend (日付 TEXT, モール TEXT, ターゲット粒度 TEXT, ターゲット TEXT, 広告費 REAL, 広告経由売上 REAL)`);

const runNode = (args) => execFileSync(process.execPath, args, { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
runNode(['apps/warehouse/rebuild-amazon-settlement-mart.js', '--ym', String(YMI)]);
runNode(['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM]);

const r = reconcileMonthly(db, { month: YM })[0];
ok(!!r, `月の行がある (${JSON.stringify(r && { d: r.profit_d, v4: r.profit_v4 })})`);
// 手で計算した決まりの違い (日次 − v4 の向き)
ok(Math.round(r.points) === 20, `② ポイント = 20 (${r.points})`);
ok(Math.round(r.ship_tax) === 30, `③ 送料の税 (v4 だけ売上に入れる) = 30 (${r.ship_tax})`);
ok(Math.round(r.refund_commission) === -44, `④ 返品の管理手数料 (v4 に無い) = −22 × 2 = −44 (${r.refund_commission})`);
ok(Math.round(r.other_refund) === -1250, `⑤ 返金の範囲 (v4 に無い) = 送料の返金 −300 + 返品の手数料 +50 + 支払い取り消しの本体 −1,000 = −1,250 (${r.other_refund})`);
ok(Math.round(r.cogs_d) === 400 && Math.round(r.cogs_v4) === 800, `① 原価: 日次 = 400 × 返品を引いた 1 個 / v4 = 400 × 注文 2 個 (${r.cogs_d} / ${r.cogs_v4})`);
ok(Math.abs(r.resid) < 1e-6, `決まりの違いを引くと残り 0 (そのままの差 ${Math.round(r.raw_diff)} 円 / 残り ${r.resid})`);
ok(reconcileSkuTop(db, { month: YM }).length === 0, 'SKU × 月でも残り 0');

// 照合の関所: 月の合計の差・説明できない残りが 0 = info (GChat の通知先は外す)
const env = { ...process.env, DATA_DIR: tmpDir }; delete env.GCHAT_WEBHOOK_INSIGHT;
let out = '', code = 0;
try { out = execFileSync(process.execPath, ['apps/warehouse/run-amazon-finance-dq.js', '--month', YM, '--run-id', 'test-reconcile'], { cwd: repoRoot, env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { code = e.status; out = String(e.stdout || '') + String(e.stderr || ''); }
const res = Object.fromEntries(db.prepare(`SELECT check_name, severity, actual_value FROM dq_run_results WHERE run_id = 'test-reconcile'`).all().map((x) => [x.check_name, x]));
ok(res.monthly_total_diff_pct?.severity === 'info' && res.monthly_total_diff_pct.actual_value === 0, `関所: 月の合計の差 = 0% (info) (${JSON.stringify(res.monthly_total_diff_pct)})`);
ok(res.unbucketed_diff_jpy?.severity === 'info' && res.unbucketed_diff_jpy.actual_value === 0, `関所: 説明できない残り = 0 円 (info) (${JSON.stringify(res.unbucketed_diff_jpy)})`);
const adj = db.prepare(`SELECT bucket_amount a, details_json d FROM accounting_diff_buckets WHERE run_id = 'test-reconcile' AND bucket_code = 'adjustment_diff'`).get();
const det = adj && JSON.parse(adj.d);
ok(adj && Math.abs(adj.a - det.raw_diff) < 1e-6 && Math.round(det.points) === -20 && Math.round(det.other_refund) === -1250, `関所: 説明できる差 (決まりの違い) の内訳 = そのままの差 (${adj && Math.round(adj.a)} / ${det && Math.round(det.raw_diff)})`);

// 壊れたら残りが出る: v4 の元 (月の集計) を 1 円ずらす
db.prepare(`UPDATE fact_amazon_settlement_monthly_wide SET sales_principal_micro = sales_principal_micro + 1000000 WHERE year_month_int = ?`).run(YMI);
const r2 = reconcileMonthly(db, { month: YM })[0];
ok(Math.round(r2.resid) === -1, `v4 側が 1 円ずれると残り −1 円 (${r2.resid})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 日次の財務と v4 の突き合わせテスト ALL PASS ===');
process.exit(failed ? 1 : 0);
