import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-finance-promotion-tax.js — 日次の財務の値引きの消費税の分 (promotion_tax_jpy) と 符号つきの正味 (2026-09-29) の試験
 *
 *   値引き (promotion_jpy) = Promotion の行の正味を反転 (値引きを正・戻りを負。消費税の分 TaxDiscount も混ざる)
 *   promotion_tax_jpy = そのうち promotion_type = TaxDiscount の分。Amazon 分析の「税抜で引いた利益」で値引きから除く
 *   手数料・値引き・返金 = 符号つきの正味を反転 (前は行ごとの ABS = 返品で戻る手数料も費用に数えていた。Codex #1522 R1 High)
 *   返金 = 本体 + 送料 + ギフト包装 − 返品の手数料 (RestockingFee)・カードの支払い取り消し (Chargeback Refund) も
 *   期待値は手で計算した値 (式を写さない)
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
const { backfillDocumentVersions } = await import('./amazon-settlement-versions.js');   // 直接入れた行に文書の版を付ける (build は版の無い行があれば止まる)

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`, YMI = Number(YM.replace('-', ''));
let n = 0;
const line = (o) => {
  const day = o.day ?? '05', sku = o.sku ?? 'SKU-A';
  db.prepare(`INSERT INTO raw_amazon_settlement_lines (
    physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id,
    posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku, seller_sku_normalized, transaction_type,
    quantity_purchased, price_type, price_amount_micro, item_related_fee_type, item_related_fee_amount_micro, promotion_type, promotion_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, ?, 'D1', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JPY', 'r', 'o', '2026-01-01 00:00:00')`)
  .run(`ph-${++n}`, `k-${n}`, n, `${YM}-${day}T01:00:00+00:00`, `${YM}-${day} 10:00:00`, `${YM}-${day}`, YMI,
    o.order ?? 'O1', sku, sku.toLowerCase(), o.tt ?? 'Order',
    o.qty ?? null, o.pt ?? null, o.pa == null ? null : o.pa * 1e6, o.ft ?? null, o.fa == null ? null : o.fa * 1e6, o.prt ?? null, o.pra == null ? null : o.pra * 1e6);
};

line({ qty: 1 });
line({ pt: 'Principal', pa: 1000 }); line({ pt: 'Tax', pa: 100 });
line({ ft: 'Commission', fa: -110 }); line({ ft: 'FBAPerUnitFulfillmentFee', fa: -330 });
line({ prt: 'Principal', pra: -200 }); line({ prt: 'TaxDiscount', pra: -20 }); line({ prt: 'Shipping', pra: -50 });

// SKU-B: 2 個売って (5 日)、1 個は返品 (10 日)、1 個はカードの支払い取り消し (12 日)
const B = { sku: 'SKU-B', order: 'O2' };
line({ ...B, qty: 2 });
line({ ...B, pt: 'Principal', pa: 2000 }); line({ ...B, pt: 'Tax', pa: 200 }); line({ ...B, pt: 'Shipping', pa: 300 });
line({ ...B, ft: 'Commission', fa: -220 }); line({ ...B, ft: 'FBAPerUnitFulfillmentFee', fa: -660 }); line({ ...B, ft: 'ShippingChargeback', fa: -300 });
line({ ...B, prt: 'Shipping', pra: -300 }); line({ ...B, prt: 'TaxDiscount', pra: -30 });
const R = { ...B, tt: 'Refund', day: '10' };
line({ ...R, pt: 'Principal', pa: -1000 }); line({ ...R, pt: 'Tax', pa: -100 }); line({ ...R, pt: 'Shipping', pa: -300 }); line({ ...R, pt: 'RestockingFee', pa: 50 });
line({ ...R, ft: 'Commission', fa: 110 }); line({ ...R, ft: 'RefundCommission', fa: -22 }); line({ ...R, ft: 'ShippingChargeback', fa: 300 });
line({ ...R, prt: 'Shipping', pra: 300 }); line({ ...R, prt: 'TaxDiscount', pra: 30 });
const C = { ...B, tt: 'Chargeback Refund', day: '12' };
line({ ...C, pt: 'Principal', pa: -1000 }); line({ ...C, pt: 'Tax', pa: -100 });
line({ ...C, ft: 'Commission', fa: 110 }); line({ ...C, ft: 'RefundCommission', fa: -22 });
// SKU-C: 出品者が付けたポイント (2026-09-29 から利益で引く・額面のまま)。5 日に 1,000 円で売って 30 ポイント、10 日に返品で 10 ポイント戻る
const P = { sku: 'SKU-C', order: 'O3' };
line({ ...P, qty: 1 }); line({ ...P, pt: 'Principal', pa: 1000 }); line({ ...P, ft: 'PointsGranted', fa: -30 });
line({ ...P, tt: 'Refund', day: '10', ft: 'PointsReturned', fa: 10 });
// SKU-B の原価 1 個 400 円 (m_products の直の商品コード = v_sku_costed の direct_master)。返品の 1 個は原価を戻す・支払い取り消しの 1 個は戻さない (Codex #1522 R2)
db.prepare(`INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, 原価, updated_at) VALUES ('sku-b', 'B', '単品', 'ok', 400, 't')`).run();

(backfillDocumentVersions(db), execFileSync)(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
const r = db.prepare(`SELECT promotion_jpy pr, promotion_tax_jpy pt, profit_amount p, commission_jpy c, fba_fulfillment_jpy f FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-a'`).get();
ok(r && r.pr === 270 && r.pt === 20, `値引き 270 (本体 200 + 送料 50 + 税の分 20) のうち税の分 = 20 (${r && r.pr} / ${r && r.pt})`);
ok(r && r.p === 1000 - 110 - 330 - 270, `profit_amount (税込で引いた利益) は今まで通り = 1,000 − 110 − 330 − 270 = 290 (${r && r.p})`);
// Amazon 分析の税抜で引いた利益の式 = profit_amount + 課税の手数料 × 1/11 + 値引きの税の分
const ex = r.p + (r.c + r.f) / 11 + r.pt;
ok(Math.abs(ex - (1000 - 110 / 1.1 - 330 / 1.1 - 250)) < 1e-9, `税抜で引いた利益 = 1,000 − 100 − 300 − 250 = 350 (式: profit + 手数料 ÷ 11 + 税の分 = ${ex})`);

const b = (day) => db.prepare(`SELECT * FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-b' AND date_jst = ?`).get(`${YM}-${day}`);
const b5 = b('05'), b10 = b('10'), b12 = b('12');
ok(b5 && b5.commission_jpy === 220 && b5.fba_fulfillment_jpy === 660 && b5.shipping_chargeback_jpy === 300 && b5.promotion_jpy === 330 && b5.promotion_tax_jpy === 30 && b5.cogs_amount === 800 && b5.profit_amount === -10,
  `B 売った日: 手数料 220・FBA 660・送料のチャージバック 300・値引き 330 (税 30)・原価 400×2・利益 2,300 − 1,510 − 800 = −10 (${b5 && [b5.commission_jpy, b5.fba_fulfillment_jpy, b5.shipping_chargeback_jpy, b5.promotion_jpy, b5.promotion_tax_jpy, b5.cogs_amount, b5.profit_amount]})`);
ok(b10 && b10.commission_jpy === -88 && b10.shipping_chargeback_jpy === -300 && b10.promotion_jpy === -330 && b10.promotion_tax_jpy === -30,
  `B 返品の日: 戻る手数料 110 − 返品の管理手数料 22 = −88 (前は ABS で +132)・チャージバック −300・値引き −330 (税 −30) (${b10 && [b10.commission_jpy, b10.shipping_chargeback_jpy, b10.promotion_jpy, b10.promotion_tax_jpy]})`);
ok(b10 && b10.refund_principal_jpy === 1250 && b10.units_refunded_customer === 1 && b10.cogs_amount === -400 && b10.profit_amount === -132,
  `B 返品の日: 返金 = 本体 1,000 + 送料 300 − 返品の手数料 50 = 1,250・返品数 1 (原価 −400 = 戻る)・利益 88 + 300 + 330 − 1,250 + 400 = −132 (${b10 && [b10.refund_principal_jpy, b10.units_refunded_customer, b10.cogs_amount, b10.profit_amount]})`);
ok(b12 && b12.refund_principal_jpy === 1000 && b12.units_refunded_customer === 0 && b12.commission_jpy === -88 && b12.cogs_amount === 0 && b12.profit_amount === -912,
  `B カードの支払い取り消しの日: 返金 1,000・返品数 0 (商品は戻らない = 原価を戻さない)・手数料 −88・利益 −912 (${b12 && [b12.refund_principal_jpy, b12.units_refunded_customer, b12.commission_jpy, b12.profit_amount]})`);
// 2 個ともお金は戻った・商品は 1 個だけ戻った = 残るのは 返品の手数料 50 − 返品の管理手数料 22×2 − FBA 660 − 戻らない 1 個の原価 400 = −1,054
const bSum = db.prepare(`SELECT SUM(profit_amount) p, SUM(commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + closing_fee_jpy + shipping_chargeback_jpy + giftwrap_chargeback_jpy) f, SUM(promotion_tax_jpy) t, SUM(units_net_sold) u FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-b'`).get();
ok(bSum.p === -1054 && bSum.u === 1, `B の合計: 利益 50 − 44 − 660 − 400 = −1,054・正味の販売数 1 (支払い取り消しの 1 個は戻らない) (${bSum.p} / ${bSum.u})`);
const bEx = bSum.p + bSum.f / 11 + bSum.t;
ok(Math.abs(bEx - (50 - 40 - 600 - 400)) < 1e-9, `B の税抜で引いた利益 = 50 − 40 − 600 − 400 = −990 (${bEx})`);

// 列を足す前に作った行 = NULL (0 にしない。作り直していない月を Render に送っても「未取得」と分かる。Codex #1522 R3)
{
  const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'finance-promo-tax-old-'));
  const Database = (await import('better-sqlite3')).default;
  const old = new Database(path.join(dir2, 'warehouse.db'));
  const ddl = fs.readFileSync(path.join(repoRoot, 'sql/amazon/f_amazon_finance_sku_daily_v1.sql'), 'utf8').replace(/^\s*--.*promotion_tax.*$/gm, '').replace(/^\s*--\s+NULL = まだ計算していない.*$/gm, '').replace(/^\s*promotion_tax_jpy REAL,\s*$/m, '');
  old.exec(ddl);
  ok(!old.prepare(`PRAGMA table_info(f_amazon_finance_sku_daily_v1)`).all().some((c) => c.name === 'promotion_tax_jpy'), '前提: 古い表に promotion_tax_jpy の列が無い');
  old.prepare(`INSERT INTO f_amazon_finance_sku_daily_v1 (date_jst, seller_sku, cost_status, source_layer_summary, source_row_count, built_at) VALUES ('2025-12-01', 'old', 'complete', '', 1, 't')`).run();
  old.close();
  // 列を足した後、決済の行の表が無いので集計は止まる (ここでは列を足すところだけ見る)
  let out = '';
  try { out = execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', dir2, '--month', '2025-11'], { cwd: repoRoot, env: { ...process.env, DATA_DIR: dir2 }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { out = String(e.stdout || '') + String(e.stderr || ''); }
  const c2 = new Database(path.join(dir2, 'warehouse.db'), { readonly: true });
  const col = c2.prepare(`PRAGMA table_info(f_amazon_finance_sku_daily_v1)`).all().find((c) => c.name === 'promotion_tax_jpy');
  const v = c2.prepare(`SELECT promotion_tax_jpy v FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'old'`).get();
  c2.close();
  ok(col && col.notnull === 0 && v && v.v === null, `古い表に列を足す = 作り直していない行は NULL (${JSON.stringify([col && col.notnull, v && v.v])} ${col ? '' : out.slice(-300)})`);
  fs.rmSync(dir2, { recursive: true, force: true });
}

const c5 = db.prepare(`SELECT points_jpy pt, other_fee_jpy o, profit_amount p FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c' AND date_jst = ?`).get(`${YM}-05`);
const c10 = db.prepare(`SELECT points_jpy pt, other_fee_jpy o, profit_amount p FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c' AND date_jst = ?`).get(`${YM}-10`);
ok(c5 && c5.pt === 30 && c5.o === 0 && c5.p === 970, `C ポイント: 付けた 30 = 費用 30・その他の手数料には入れない・利益 1,000 − 30 = 970 (${c5 && [c5.pt, c5.o, c5.p]})`);
ok(c10 && c10.pt === -10 && c10.p === 10, `C ポイント: 返品で戻る 10 = −10 (前は ABS で費用に数えていた)・利益 +10 (${c10 && [c10.pt, c10.p]})`);
// 税抜で引いた利益の式 (profit + 課税の手数料 × 1/11 + 値引きの税) にポイントは足し戻さない = 額面のまま引く
const cEx = db.prepare(`SELECT SUM(profit_amount + (commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + closing_fee_jpy + shipping_chargeback_jpy + giftwrap_chargeback_jpy) / 11 + COALESCE(promotion_tax_jpy, 0)) e FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c'`).get().e;
// 2 回目の集計 = 既存の行の上書き (ON CONFLICT の側の利益の式) でもポイントを引く・値が変わらない (Codex #1525 R1)
(backfillDocumentVersions(db), execFileSync)(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
const c5b = db.prepare(`SELECT points_jpy pt, profit_amount p FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c' AND date_jst = ?`).get(`${YM}-05`);
const b10b = db.prepare(`SELECT profit_amount p, promotion_tax_jpy t FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-b' AND date_jst = ?`).get(`${YM}-10`);
ok(c5b.pt === 30 && c5b.p === 970 && b10b.p === -132 && b10b.t === -30, `2 回目の集計 (上書き) でも同じ: C のポイント 30・利益 970 / B の返品の日の利益 −132・値引きの税 −30 (${[c5b.pt, c5b.p, b10b.p, b10b.t]})`);
ok(cEx === 1000 - 20,`C の税抜で引いた利益 = 1,000 − ポイント 20 (額面のまま) = 980 (${cEx})`);

// 材料の無くなった日 × SKU は作り直しで消える (UPSERT だけだと古い行が残る = Company DB との突き合わせが 0 にならない。2026-09-29 F2b-1)
{
  const snapBefore = db.prepare(`SELECT unit_cost_snapshot s, cost_snapshot_date_jst d FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-b' AND date_jst = ?`).get(`${YM}-05`);
  db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE seller_sku_normalized = 'sku-c' AND economic_date = ?`).run(`${YM}-10`);
  (backfillDocumentVersions(db), execFileSync)(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
  const c10gone = db.prepare(`SELECT COUNT(*) n FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c' AND date_jst = ?`).get(`${YM}-10`).n;
  const c5kept = db.prepare(`SELECT points_jpy pt FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-c' AND date_jst = ?`).get(`${YM}-05`);
  const snapAfter = db.prepare(`SELECT unit_cost_snapshot s, cost_snapshot_date_jst d FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-b' AND date_jst = ?`).get(`${YM}-05`);
  ok(c10gone === 0 && c5kept && c5kept.pt === 30 && JSON.stringify(snapAfter) === JSON.stringify(snapBefore),
    `材料の無くなった日 × SKU (C の 10 日) は作り直しで消える・ほかの行 (C の 5 日) と原価の記録 (B の 5 日) は残る (${c10gone} / ${c5kept && c5kept.pt} / ${JSON.stringify(snapAfter)})`);
  // 月の境: ほかの月の行は消さない (#1533 Codex R1)
  db.prepare(`INSERT INTO f_amazon_finance_sku_daily_v1 (date_jst, seller_sku, cost_status, source_layer_summary, source_row_count, built_at) VALUES ('2020-01-31', 'sku-other-month', 'complete', 'sp_api_v2', 1, 't')`).run();
  // 月の材料が全部消えた: 決済の行を全部消して作り直す → その月の行は全部消える (ほかの月は残る)
  db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE year_month_int = ?`).run(YMI);
  let out = '';
  try { out = (backfillDocumentVersions(db), execFileSync)(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { out = String(e.stdout || '') + String(e.stderr || ''); }
  const leftMonth = db.prepare(`SELECT COUNT(*) n FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?`).get(YM).n;
  const otherMonth = db.prepare(`SELECT COUNT(*) n FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-other-month'`).get().n;
  ok(leftMonth === 0 && otherMonth === 1, `月の材料が全部消えたら その月の行は全部消える・ほかの月の行は残る (${leftMonth} / ${otherMonth} ${leftMonth ? out.slice(-300) : ''})`);
}

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 値引きの税の分テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
