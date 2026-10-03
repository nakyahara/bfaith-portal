import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-amazon-account-fees.js — アカウント単位の手数料 (rebuild-amazon-account-fees.js) の試験
 *
 * 2026-09-28: Amazon が決済の取引の名前を変えていた (7 月から保管料 FBA Inventory Storage Fee・長期保管料 FBA Long Term Storage Fee、
 * 6 月から返送料 FBA Removal Order: Return Fee) のに古い名前しか拾わず、7〜9 月の保管料・長期保管料・返送料が 0 だった。
 *   ① 古い名前も新しい名前も同じ手数料の種類に入る
 *   ② 入れない取引 (預かり金 など) は入らず、⚠️ にもならない
 *   ⑥ Easy Ship の配送料 (2026-09-28 から easy_ship) = 金額が other-amount の月も item-related-fee-amount の月も数える
 *   ③ 分けられない SKU なしの取引 (知らない名前) が出たら最後の行が ⚠️ (daily-sync で「全部 OK」に数えない)・無ければ ✓
 *   ④ SKU の付いた行は入れない (SKU 単位の集計の側 = 二重にしない)。ただし納品不備は SKU の付いた行も入れる (2026-09-30 D-63・日次の財務から外した)
 *   ⑤ 前方一致で拾った未確認の名前は、金額を入れた上で ⚠️ / 同じ取引が 2 つの文書にあっても 1 回 / 低在庫手数料 / 前方一致の境目 (Codex #1515 R1)
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
const { backfillDocumentVersions } = await import('./amazon-settlement-versions.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
// 当月の日付 (手数料の集計は「今日から N か月」で絞る)
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`;
let n = 0;
// 🆕 2026-10-01 (D-66): 決済ごとに採る文書の版は 1 つ = 「同じ取引が 2 つの文書に」は別の決済 (S-DUP) の 2 つの文書にする (1 つの決済の文書 = その決済の全部の行)
const line = (tx, amount, { sku = null, fee = false, key = null, doc = 'D1', settlement = 'S1' } = {}) => db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, transaction_type, seller_sku, seller_sku_normalized, other_amount_micro, item_related_fee_type, item_related_fee_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, ?, 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, key || `k-${n}`, doc, n, settlement, `${YM}-05T00:00:00+00:00`, `${YM}-05 09:00:00`, `${YM}-05`, Number(YM.replace('-', '')), tx, sku, sku && sku.toLowerCase(),
    fee ? null : amount * 1e6, fee ? tx : null, fee ? amount * 1e6 : null);

// 古い名前・新しい名前・入れない取引・SKU の付いた行
line('Storage Fee', -100); line('FBA Inventory Storage Fee', -300000); line('Storage Fee - Correction', -50); line('Storage Fee - Reversal', 50);
line('StorageRenewalBilling', -10); line('FBA Long Term Storage Fee', -100000);
line('RemovalComplete', -5); line('FBA Removal Order: Return Fee', -60); line('FBA Removal Order: Return Fee', -60, { key: 'same-rm', doc: 'DA', settlement: 'S-DUP' }); line('FBA Removal Order: Return Fee', -60, { key: 'same-rm', doc: 'DB', settlement: 'S-DUP' });   // 同じ取引が 2 つの文書に = 1 回
line('FBA LowInventoryLevel Fee', -70);   // 低在庫手数料 (型で拾う・確かめ済みの型)
line('FBA Removal Orderly', -1);   // 前方一致の境目: 'FBA Removal Order' で始まる = removal に入るが未確認の名前として ⚠️ (最初の回で確かめ、消してから ✓ を確かめる)
line('Subscription Fee', -4900); line('Inbound Defect Fee - Barcode cannot be scanned', -330);
line('Amazon Easy Ship Charges', -440, { fee: true }); line('Amazon Easy Ship Charges', -100);   // 新しい月 = 手数料の列 / 古い月 = その他の金額の列
line('Current Reserve Amount', -1000); line('Previous Reserve Amount Balance', 1000);   // 入れない (預かり金の出し入れ)
line('Fee Adjustment', 120); line('Overpaid Fees Adjustment', 30);   // 🆕 2026-09-29 手数料の調整・払いすぎの返還 (戻り = 正) = その他に入れる
line('Goodwill Concession', 7);   // 入れない (今まで通り)
line('Fee Adjustment', 55, { sku: 'SKU-A' });   // SKU の付いた調整は日次の財務 (補てん) 側 = ここには入れない
line('FBA Inventory Storage Fee', -999, { sku: 'SKU-A' });   // SKU の付いた行は入れない
// 🆕 2026-09-30 (D-63): SKU の付いた納品不備は入れる (日次の財務の silver からは外す = 二重にしない)。
//   名前の大文字小文字は前方一致 (LIKE) と同じく問わない・同じ取引が 2 つの文書にあっても 1 回・空白だけの SKU は入れない (日次の財務にも入らない = 送り手が止める)
line('Inbound Defect Fee - Missing label', -200, { sku: 'SKU-A' }); line('inbound defect fee - x', -20, { sku: 'SKU-B' });
line('Inbound Defect Fee - Missing label', -100, { sku: 'SKU-C', key: 'same-idf', doc: 'DA', settlement: 'S-DUP' }); line('Inbound Defect Fee - Missing label', -100, { sku: 'SKU-C', key: 'same-idf', doc: 'DB', settlement: 'S-DUP' });
line('Inbound Defect Fee - Missing label', -9, { sku: '  ' });

// 直接入れた行には文書の版が無い = 過去の行と同じ backfill で版を付けてから build (build は版の無い行があれば止まる)
const run = () => { backfillDocumentVersions(db); return execFileSync(process.execPath, ['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', '1'], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' }); };
const lastLine = (out) => out.trim().split('\n').at(-1);
let out = run();
const got = Object.fromEntries(db.prepare(`SELECT fee_type f, amount_jpy a FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ?`).all(`${YM}-01`).map((r) => [r.f, Math.round(r.a)]));
ok(got.storage === -300100, `🚨 保管料 = 古い名前 -100 + 新しい名前 (FBA Inventory Storage Fee) -300,000 + 訂正 -50 + 取消 +50 = -300,100 (${got.storage})`);
ok(got.long_term_storage === -100010, `🚨 長期保管料 = StorageRenewalBilling + FBA Long Term Storage Fee = -100,010 (${got.long_term_storage})`);
ok(got.removal === -126, `🚨 返送・廃棄 = RemovalComplete -5 + FBA Removal Order: Return Fee -60 + 2 つの文書にある同じ取引 -60 (1 回) + 前方一致の境目 -1 = -126 (${got.removal})`);
ok(got.low_inventory === -70, `低在庫手数料 (型で拾う) = -70 (${got.low_inventory})`);
ok(got.subscription === -4900, `月額登録料はそのまま (${got.subscription})`);
const idf = db.prepare(`SELECT amount_jpy a, row_count n FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ? AND fee_type = 'inbound_defect'`).get(`${YM}-01`);
ok(Math.round(idf.a) === -650 && idf.n === 4, `🆕 納品不備 = SKU なし -330 + SKU 付き -200 + 小文字の名前 -20 + 2 つの文書の同じ取引 -100 (1 回) = -650・4 行 (空白だけの SKU -9 は入れない) (${idf.a} / ${idf.n})`);
ok(got.easy_ship === -540, `🚨 Easy Ship の配送料 = 手数料の列 -440 + その他の金額の列 -100 = -540 (${got.easy_ship})`);
ok(got.other_account_fee === 150, `🆕 手数料の調整 +120 + 払いすぎの返還 +30 = その他 +150 (SKU の付いた調整 +55・Goodwill +7 は入らない) (${got.other_account_fee})`);
ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE transaction_type LIKE '%Reserve%'`).get().n === 2 && Object.keys(got).sort().join() === 'easy_ship,inbound_defect,long_term_storage,low_inventory,other_account_fee,removal,storage,subscription', `入れない取引 (預かり金 2 行は入っている) と SKU の付いた行は入らない (${JSON.stringify(got)})`);
ok(isWarnSummary(lastLine(out)) && /未確認の名前 1 種類 \(集計に入っている\): FBA Removal Orderly → removal/.test(lastLine(out)) && !/分けられない/.test(lastLine(out)), `🚨 前方一致で拾った未確認の名前 = 金額は入れた上で ⚠️ (${lastLine(out)})`);
db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE transaction_type = 'FBA Removal Orderly'`).run();
out = run();
ok(!isWarnSummary(lastLine(out)) && /分けられない SKU なしの取引 0・未確認の名前 0/.test(lastLine(out)), `確かめた名前だけなら最後の行は ✓ (${lastLine(out)})`);
line('FBA Removal Order: Disposal Fee', -40);
out = run();
ok(isWarnSummary(lastLine(out)) && /FBA Removal Order: Disposal Fee → removal/.test(lastLine(out)) && Math.round(db.prepare(`SELECT amount_jpy a FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ? AND fee_type = 'removal'`).get(`${YM}-01`).a) === -165, `まだ見ていない返送の名前 (Disposal Fee) = 返送に入れて ⚠️ (${lastLine(out)})`);

// 知らない名前の SKU なしの取引 → 最後の行が ⚠️ (金額はどこにも入らない = 人が分け方を足す)
line('FBA Brand New Fee', -777);
out = run();
ok(isWarnSummary(lastLine(out)) && /分けられない SKU なしの取引 1 種類 \(集計に入っていない\): FBA Brand New Fee \(延べ 1 行・¥-777/.test(lastLine(out)), `🚨 知らない名前の SKU なしの取引が出たら最後の行が ⚠️ = daily-sync で「全部 OK」に数えない (${lastLine(out)})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== アカウント単位の手数料テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
