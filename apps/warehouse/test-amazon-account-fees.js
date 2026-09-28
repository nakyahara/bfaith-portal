import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-amazon-account-fees.js — アカウント単位の手数料 (rebuild-amazon-account-fees.js) の試験
 *
 * 2026-09-28: Amazon が決済の取引の名前を変えていた (7 月から保管料 FBA Inventory Storage Fee・長期保管料 FBA Long Term Storage Fee、
 * 6 月から返送料 FBA Removal Order: Return Fee) のに古い名前しか拾わず、7〜9 月の保管料・長期保管料・返送料が 0 だった。
 *   ① 古い名前も新しい名前も同じ手数料の種類に入る
 *   ② 入れない取引 (Easy Ship・預かり金 など) は入らず、⚠️ にもならない
 *   ③ 分けられない SKU なしの取引 (知らない名前) が出たら最後の行が ⚠️ (daily-sync で「全部 OK」に数えない)・無ければ ✓
 *   ④ SKU の付いた行は入れない (SKU 単位の集計の側 = 二重にしない)
 *
 * 実行: node apps/warehouse/test-amazon-account-fees.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'account-fees-test-'));
process.env.DATA_DIR = tmpDir;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const { initDB, getDB } = await import('./db.js');
const { isWarnSummary } = await import('./amazon-fees-outcome.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
// 当月の日付 (手数料の集計は「今日から N か月」で絞る)
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`;
let n = 0;
const line = (tx, amount, { sku = null, fee = false } = {}) => db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, transaction_type, seller_sku, seller_sku_normalized, other_amount_micro, item_related_fee_type, item_related_fee_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, 'D1', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, `k-${n}`, n, `${YM}-05T00:00:00+00:00`, `${YM}-05 09:00:00`, `${YM}-05`, Number(YM.replace('-', '')), tx, sku, sku && sku.toLowerCase(),
    fee ? null : amount * 1e6, fee ? tx : null, fee ? amount * 1e6 : null);

// 古い名前・新しい名前・入れない取引・SKU の付いた行
line('Storage Fee', -100); line('FBA Inventory Storage Fee', -300000); line('Storage Fee - Correction', -50); line('Storage Fee - Reversal', 50);
line('StorageRenewalBilling', -10); line('FBA Long Term Storage Fee', -100000);
line('RemovalComplete', -5); line('FBA Removal Order: Return Fee', -60); line('FBA Removal Order: Disposal Fee', -40);
line('Subscription Fee', -4900); line('Inbound Defect Fee - Barcode cannot be scanned', -330);
line('Amazon Easy Ship Charges', -440, { fee: true }); line('Current Reserve Amount', -1000); line('Previous Reserve Amount Balance', 1000);
line('FBA Inventory Storage Fee', -999, { sku: 'SKU-A' });   // SKU の付いた行は入れない

const run = () => execFileSync(process.execPath, ['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', '1'], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
const lastLine = (out) => out.trim().split('\n').at(-1);
let out = run();
const got = Object.fromEntries(db.prepare(`SELECT fee_type f, amount_jpy a FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ?`).all(`${YM}-01`).map((r) => [r.f, Math.round(r.a)]));
ok(got.storage === -300100, `🚨 保管料 = 古い名前 -100 + 新しい名前 (FBA Inventory Storage Fee) -300,000 + 訂正 -50 + 取消 +50 = -300,100 (${got.storage})`);
ok(got.long_term_storage === -100010, `🚨 長期保管料 = StorageRenewalBilling + FBA Long Term Storage Fee = -100,010 (${got.long_term_storage})`);
ok(got.removal === -105, `🚨 返送・廃棄 = RemovalComplete + FBA Removal Order: Return Fee / Disposal Fee = -105 (${got.removal})`);
ok(got.subscription === -4900 && got.inbound_defect === -330, `月額登録料・納品不備はそのまま (${got.subscription} / ${got.inbound_defect})`);
ok(!('other_account_fee' in got), `入れない取引 (Easy Ship・預かり金) と SKU の付いた行は入らない (${JSON.stringify(got)})`);
ok(!isWarnSummary(lastLine(out)) && /分けられない SKU なしの取引 0/.test(lastLine(out)), `知らない名前が無ければ最後の行は ✓ (${lastLine(out)})`);

// 知らない名前の SKU なしの取引 → 最後の行が ⚠️ (金額はどこにも入らない = 人が分け方を足す)
line('FBA Brand New Fee', -777);
out = run();
ok(isWarnSummary(lastLine(out)) && /FBA Brand New Fee \(1 行・¥-777/.test(lastLine(out)), `🚨 知らない名前の SKU なしの取引が出たら最後の行が ⚠️ = daily-sync で「全部 OK」に数えない (${lastLine(out)})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== アカウント単位の手数料テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
