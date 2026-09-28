import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-easy-ship-allocation.js — Easy Ship の配送料を SKU に割り振る (2026-09-28) の試験
 *
 *   日次の財務 (build_f_amazon_finance_sku_daily_v1.sql) が Amazon Easy Ship Charges (SKU なし・注文番号だけ) を
 *   同じ注文の売上の行の SKU に割り振って easy_ship_jpy に入れる (SKU ごとの利益を見るための列)。
 *   🚨 profit_amount からは引かない / 月の Easy Ship は全部アカウント単位の手数料 (easy_ship) で引く = 二重にも漏れにもならない (Codex #1520 R1)
 *   ① 1 SKU の注文 = 全部その SKU ② 複数 SKU = 本体売上の割合 ③ 本体 0 = 等分 ④ 売上の行が無い = 割り振らない
 *   ⑤ 金額の列は other-amount (古い月) と item-related-fee-amount (新しい月) の両方 ⑥ 1 円単位・端数は大きい順 (3 等分 34/33/33)
 *   ⑦ Easy Ship の返金 (正の額) は割り振り額を減らす ⑧ 料金の日に SKU の売上が無くても行ができる (照合の SKU の数には入れない)
 *   ⑨ 作り直しても同じ ⑩ あとから届いた売上の行で割り振られる
 *
 * 実行: node apps/warehouse/test-easy-ship-allocation.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'easyship-alloc-test-'));
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
// 決済の行 (V1 の形)。day = 日 / o = 注文番号 / sku = SKU (なしは null) / 金額は円
const line = ({ day = 5, o = null, sku = null, tx = 'Order', pt = null, pa = null, qty = null, fee = null, feeType = null, other = null }) => db.prepare(`INSERT INTO raw_amazon_settlement_lines (
    physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku, seller_sku_normalized, transaction_type,
    quantity_purchased, price_type, price_amount_micro, item_related_fee_type, item_related_fee_amount_micro, other_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, 'D1', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, `k-${n}`, n, `${YM}-${String(day).padStart(2, '0')}T01:00:00+00:00`, `${YM}-${String(day).padStart(2, '0')} 10:00:00`, `${YM}-${String(day).padStart(2, '0')}`, YMI,
    o, sku, sku && sku.toLowerCase(), tx, qty, pt, pa == null ? null : pa * 1e6, feeType, fee == null ? null : fee * 1e6, other == null ? null : other * 1e6);

// 売上 (5 日)
line({ o: 'O1', sku: 'SKU-A', qty: 1 }); line({ o: 'O1', sku: 'SKU-A', pt: 'Principal', pa: 1000 });
line({ o: 'O2', sku: 'SKU-A', qty: 1 }); line({ o: 'O2', sku: 'SKU-A', pt: 'Principal', pa: 600 });
line({ o: 'O2', sku: 'SKU-B', qty: 1 }); line({ o: 'O2', sku: 'SKU-B', pt: 'Principal', pa: 400 });
line({ o: 'O3', sku: 'SKU-A', pt: 'Principal', pa: 0 }); line({ o: 'O3', sku: 'SKU-B', pt: 'Principal', pa: 0 });
for (const s of ['SKU-X', 'SKU-Y', 'SKU-Z']) line({ day: 6, o: 'O5', sku: s, pt: 'Principal', pa: 500 });   // 3 等分用
// Easy Ship の料金 (7 日 = 売上の日と違う)
line({ day: 7, o: 'O1', tx: 'Amazon Easy Ship Charges', other: -500 });                                              // 古い月の形 (その他の金額の列)
line({ day: 7, o: 'O1', tx: 'Amazon Easy Ship Charges', other: 50 });                                                // Easy Ship の返金 (正の額)
line({ day: 7, o: 'O2', tx: 'Amazon Easy Ship Charges', fee: -1000, feeType: 'Amazon Easy Ship Charges' });           // 新しい月の形 (手数料の列)
line({ day: 7, o: 'O3', tx: 'Amazon Easy Ship Charges', fee: -300, feeType: 'Amazon Easy Ship Charges' });
line({ day: 7, o: 'O4', tx: 'Amazon Easy Ship Charges', fee: -200, feeType: 'Amazon Easy Ship Charges' });            // 売上の行が無い注文
line({ day: 7, o: 'O5', tx: 'Amazon Easy Ship Charges', fee: -100, feeType: 'Amazon Easy Ship Charges' });            // 3 SKU に 100 円

const run = (args) => execFileSync(process.execPath, args, { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
const build = () => run(['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM]);
build();
const fact = (sku, day) => db.prepare(`SELECT easy_ship_jpy e, profit_amount p, sales_principal_jpy s, source_layer_summary l FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = ? AND date_jst = ?`).get(sku, `${YM}-${String(day).padStart(2, '0')}`);
const a7 = fact('sku-a', 7), b7 = fact('sku-b', 7), a5 = fact('sku-a', 5);
ok(a7 && a7.e === 500 - 50 + 600 + 150, `🚨 SKU-A (7 日) = O1 全部 500 − 返金 50 + O2 の 6 割 600 + O3 の半分 150 = 1,200 (${a7 && a7.e})`);
ok(b7 && b7.e === 400 + 150, `SKU-B (7 日) = O2 の 4 割 400 + O3 の半分 150 = 550 (${b7 && b7.e})`);
const xyz = ['sku-x', 'sku-y', 'sku-z'].map((s) => fact(s, 7)?.e);
ok(JSON.stringify(xyz) === JSON.stringify([34, 33, 33]), `🚨 100 円を同じ売上の 3 SKU に = 1 円単位で 34 / 33 / 33・合計 100 (端数は大きい順・同じなら SKU 順) (${xyz.join(' / ')})`);
ok(a7 && a7.p === 0 && a7.s === 0 && a7.l === 'easy_ship_alloc', `🚨 料金の日に売上が無くても行ができる・profit_amount からは引かない (0)・印は easy_ship_alloc (${a7 && a7.p} / ${a7 && a7.l})`);
ok(a5 && a5.e === 0 && a5.p === 1600, `売上の日の行 (5 日) は今まで通り (Easy Ship 0・利益 1,600) (${a5 && a5.e} / ${a5 && a5.p})`);
const allocated = db.prepare(`SELECT SUM(easy_ship_jpy) s FROM f_amazon_finance_sku_daily_v1`).get().s;
ok(allocated === 1200 + 550 + 100, `割り振った合計 = 1,850 (O4 の 200 は売上の行が無いので割り振らない) (${allocated})`);

run(['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', '1']);
const acct = db.prepare(`SELECT amount_jpy a, row_count n FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ? AND fee_type = 'easy_ship'`).get(`${YM}-01`);
ok(acct && acct.a === -500 + 50 - 1000 - 300 - 200 - 100, `🚨 月の Easy Ship はアカウント単位の手数料で全部 (−2,050 = 割り振りに左右されない = 二重にも漏れにもならない) (${acct && acct.a})`);

// 作り直しても同じ・原価の snapshot は残る
db.prepare(`UPDATE f_amazon_finance_sku_daily_v1 SET unit_cost_snapshot = 100 WHERE seller_sku = 'sku-a' AND date_jst = ?`).run(`${YM}-05`);
build();
const a5b = fact('sku-a', 5), a7b = fact('sku-a', 7);
ok(a7b.e === 1200 && a7b.p === 0 && a5b.p === 1600 - 100 * 2, `作り直しても同じ (7 日 1,200・利益 0)・5 日は原価 snapshot 100 × 2 個を引く (${a7b.e} / ${a5b.p})`);

// O4 の売上の行があとから届いた → 割り振られる (手数料の月の合計は変わらない)
line({ o: 'O4', sku: 'SKU-C', pt: 'Principal', pa: 900 });
build();
run(['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', '1']);
const c7 = fact('sku-c', 7);
const acct2 = db.prepare(`SELECT amount_jpy a FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ? AND fee_type = 'easy_ship'`).get(`${YM}-01`);
ok(c7 && c7.e === 200 && acct2.a === -2050, `売上の行があとから届いた注文 = SKU に割り振られる・月の手数料は変わらない (${c7 && c7.e} / ${acct2.a})`);

// 照合 (DQ): 利益は足し戻さない (profit_amount に入っていない) / Easy Ship だけの行は SKU の数に入れない
const dq = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/run-amazon-finance-dq.js'), 'utf8');
const vr = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/validate-v4-reference.js'), 'utf8');
ok(!/easy_ship_jpy/.test(dq + vr) && (dq.match(/source_layer_summary <> 'easy_ship_alloc'/g) || []).length === 3 && /source_layer_summary <> 'easy_ship_alloc'/.test(vr), '照合: 利益は足し戻さない・Easy Ship だけの行 (売上の無い日) は SKU の数にも集合差 (両側) にも入れない');
const dqCount = db.prepare(`SELECT COUNT(DISTINCT seller_sku) c FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ? AND source_layer_summary <> 'easy_ship_alloc'`).get(YM).c;
ok(dqCount === 6, `照合の SKU の数 = 売上のある SKU だけ (A・B・C・X・Y・Z = 6) (${dqCount})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== Easy Ship の割り振りテスト ALL PASS ===');
process.exit(failed ? 1 : 0);
