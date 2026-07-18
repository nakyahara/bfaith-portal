// margin-alert-job 機能スモーク: fixture 投入 → 集計/判定/整形/状態保存を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/profit-analysis/margin-alert-smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルへ書き込むため本番 DATA_DIR で実行禁止
import fs from 'node:fs';
import path from 'node:path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import {
  collectMarginRows, classifyMarginRows, formatMarginAlertMessage,
  loadAlertState, saveAlertState, resolveThresholdPct,
  mergeStateKeys, sanitizeText,
} from './margin-alert-job.js';
import { jstToday, addDays } from '../amazon-dashboard/queries.js';

// 本番 DB 誤実行ガード (rakuten-analytics smoke と同じサンドボックスマーカー方式)
const dataDir = process.env.DATA_DIR;
if (!dataDir) {
  console.error('FATAL: DATA_DIR が未指定です。smoke 専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/pa-smoke-data)');
  process.exit(2);
}
const marker = path.join(dataDir, '.pa-smoke-sandbox');
const dbFile = path.join(dataDir, 'warehouse-mirror.db');
if (fs.existsSync(dbFile) && !fs.existsSync(marker)) {
  console.error(`FATAL: ${dbFile} は smoke が作成した DB ではありません (マーカー ${marker} なし)。中断します`);
  process.exit(2);
}
if (!fs.existsSync(dbFile) && !fs.existsSync(marker) && fs.existsSync(dataDir)
    && fs.readdirSync(dataDir).length > 0) {
  console.error(`FATAL: ${dataDir} は空ではありません。smoke 専用の空ディレクトリを指定してください`);
  process.exit(2);
}
fs.mkdirSync(dataDir, { recursive: true });
fs.writeFileSync(marker, `profit-analysis margin-alert smoke sandbox (created ${new Date().toISOString()})\n`);

initMirrorDB();
const db = getMirrorDB();

let pass = 0, fail = 0;
function check(name, cond, detail = '') {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.error(`  ❌ ${name} ${detail}`); }
}

const today = jstToday();
const to = addDays(today, -1);
const from = addDays(today, -30);
const d1 = addDays(today, -5);
const d2 = addDays(today, -10);
const oldDate = addDays(today, -60); // ウィンドウ外 (混入しないことの検証用)

// ─── fixture 投入 ───
db.prepare(`DELETE FROM mirror_rakuten_finance_sku_daily`).run();
db.prepare(`DELETE FROM mirror_yahoo_finance_sku_daily`).run();
db.prepare(`DELETE FROM mirror_aupay_finance_sku_daily`).run();
db.prepare(`DELETE FROM mirror_qoo10_finance_sku_daily`).run();
db.prepare(`DELETE FROM mirror_amazon_finance_sku_daily`).run();
db.prepare(`DELETE FROM mirror_amazon_ads_sku_daily`).run();
db.prepare(`DELETE FROM mirror_amazon_ads_campaign_daily`).run();
db.prepare(`DELETE FROM dashboard_settings WHERE key = 'profit_margin_alert_state'`).run();

// 楽天: RK-LOW = 粗利率5% (割れ)、RK-OK = 30% (健全)、RK-NOCOST = 原価未登録 (対象外)、RK-OLD = ウィンドウ外
const insRakuten = db.prepare(`
  INSERT INTO mirror_rakuten_finance_sku_daily (
    date_jst, rakuten_code, ne_code, sku_resolution, product_name,
    units_ordered, units_net_sold,
    gross_sales_jpy_incl, net_sales_jpy_incl, variable_margin_jpy_incl,
    cogs_amount_jpy_incl, shipping_quality, cost_status, is_cost_complete,
    data_quality_score, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, 'resolved', ?, ?, ?, ?, ?, ?, ?, 'actual', ?, ?, 100, 'smoke', ?, datetime('now'))
`);
insRakuten.run(d1, 'rk-low', 'ne-1', '楽天低粗利テスト商品ロングネーム24文字超えチェック', 10, 10, 100000, 95000, 5000, 60000, 'complete', 1, 'h1');
insRakuten.run(d2, 'rk-ok', 'ne-2', '楽天健全商品', 5, 5, 50000, 48000, 15000, 20000, 'complete', 1, 'h2');
insRakuten.run(d1, 'rk-nocost', 'ne-3', '楽天原価未登録', 3, 3, 30000, 29000, 29000, 0, 'missing_cost', 0, 'h3');
insRakuten.run(oldDate, 'rk-old', 'ne-4', 'ウィンドウ外商品', 8, 8, 80000, 76000, 800, 50000, 'complete', 1, 'h4');

// Yahoo: YH-LOW = 8% (割れ)
db.prepare(`
  INSERT INTO mirror_yahoo_finance_sku_daily (
    date_jst, yahoo_sku_key, ne_code, resolution_method, product_name,
    units_ordered, units_net_sold, gross_sales_jpy_incl,
    variable_margin_partial_jpy_incl, shipping_quality, cost_status, is_cost_complete,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'yh-low', 'ne-5', 'sub_match', 'Yahoo低粗利商品', 4, 4, 40000, 3200, 'actual', 'complete', 1, 'smoke', 'h5', datetime('now'))
`).run(d1);

// au PAY: AU-EDGE = ちょうど10.0% (>= threshold なので割れではない、境界検証)
db.prepare(`
  INSERT INTO mirror_aupay_finance_sku_daily (
    date_jst, aupay_sku_key, ne_code, resolution_method, product_name,
    units_ordered, units_net_sold, gross_sales_jpy_incl,
    variable_margin_partial_jpy_incl, shipping_quality, cost_status, is_cost_complete,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'au-edge', 'ne-6', 'master_match', 'au境界値商品', 2, 2, 20000, 2000, 'actual', 'complete', 1, 'smoke', 'h6', datetime('now'))
`).run(d1);

// Qoo10: Q-LOSS = 赤字 (分母 = net_settlement)
db.prepare(`
  INSERT INTO mirror_qoo10_finance_sku_daily (
    date_jst, sku_code, ne_code, resolution_method, product_name,
    units_ordered, units_net_sold, customer_paid_jpy_incl, net_settlement_api_jpy_incl,
    variable_margin_jpy_incl, shipping_quality, cost_status, is_cost_complete,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'q-loss', 'ne-7', 'master_match', 'Qoo10赤字商品', 6, 6, 60000, 54000, -5400, 'actual_api', 'complete', 1, 'smoke', 'h7', datetime('now'))
`).run(d1);

// Amazon: AMZ-LOW = 広告前6%、広告費で赤字化 (広告込み併記の検証)
db.prepare(`
  INSERT INTO mirror_amazon_finance_sku_daily (
    date_jst, seller_sku, asin_norm, product_name, units_ordered, units_net_sold,
    sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy,
    cogs_amount, profit_amount, is_cost_complete, cost_status,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'amz-low', 'B000TEST01', 'Amazon低粗利商品', 20, 20, 200000, 0, 0, 150000, 12000, 1, 'complete', 'smoke', 'h8', datetime('now'))
`).run(d1);
db.prepare(`
  INSERT INTO mirror_amazon_ads_sku_daily (
    date_jst, mall, campaign_id, ad_type, target, target_granularity,
    clicks, impressions, ad_cost, ad_sales, ad_units,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'amazon', 'c1', 'SP', 'amz-low', 'sku', 100, 1000, 15000, 30000, 3, 'smoke', 'h9', datetime('now'))
`).run(d1);

// ─── Test 1: 集計 ───
console.log('Test 1: collectMarginRows');
const { rows, skipped } = collectMarginRows(db, from, to);
check('skipped なし', skipped.length === 0, JSON.stringify(skipped));
const byKey = new Map(rows.map(r => [`${r.mall}:${r.code}`, r]));
check('楽天 rk-low 粗利率 5.0%', byKey.get('rakuten:rk-low')?.marginPct === 5);
check('楽天 rk-ok 粗利率 30.0%', byKey.get('rakuten:rk-ok')?.marginPct === 30);
check('楽天 rk-nocost は costComplete=false', byKey.get('rakuten:rk-nocost')?.costComplete === false);
check('ウィンドウ外 rk-old は含まれない', !byKey.has('rakuten:rk-old'));
check('Yahoo yh-low 粗利率 8.0%', byKey.get('yahoo:yh-low')?.marginPct === 8);
check('au au-edge 粗利率 10.0%', byKey.get('aupay:au-edge')?.marginPct === 10);
check('Qoo10 q-loss 粗利率 -10.0%', byKey.get('qoo10:q-loss')?.marginPct === -10);
const amz = byKey.get('amazon:amz-low');
check('Amazon amz-low 広告前 6.0%', amz?.marginPct === 6, `got=${amz?.marginPct}`);
check('Amazon amz-low 広告込 -1.5%', amz?.adInclPct === -1.5, `got=${amz?.adInclPct}`);

// ─── Test 2: 判定 (初回 = prevKeys null) ───
console.log('Test 2: classifyMarginRows (初回)');
const threshold = resolveThresholdPct();
check('デフォルト閾値 10%', threshold === 10);
const r1 = classifyMarginRows(rows, threshold, null);
const flaggedKeys1 = new Set(r1.flagged.map(r => `${r.mall}:${r.code}`));
check('割れは4件 (rk-low/yh-low/q-loss/amz-low)', r1.flagged.length === 4, JSON.stringify([...flaggedKeys1]));
check('境界値 au-edge (10.0%) は割れ扱いしない', !flaggedKeys1.has('aupay:au-edge'));
check('原価未登録は excluded=1', r1.excludedCount === 1);
check('赤字は1件 (q-loss)', r1.lossCount === 1);
check('初回は全件 contItems 扱い', r1.newItems.length === 0 && r1.contItems.length === 4);
check('ワースト順 (先頭が q-loss)', r1.flagged[0].code === 'q-loss');

// ─── Test 3: 状態保存 → 新規/継続の分離 ───
console.log('Test 3: 状態保存と新規/継続');
check('初回 state は null', loadAlertState(db) === null);
saveAlertState(db, ['rakuten:rk-low', 'yahoo:yh-low'], today);
const state = loadAlertState(db);
check('state 保存/読込', state !== null && state.flagged_keys.length === 2);
const r2 = classifyMarginRows(rows, threshold, state.flagged_keys);
const newKeys = new Set(r2.newItems.map(r => `${r.mall}:${r.code}`));
check('新規 = q-loss + amz-low', r2.newItems.length === 2 && newKeys.has('qoo10:q-loss') && newKeys.has('amazon:amz-low'));
check('継続 = rk-low + yh-low', r2.contItems.length === 2);

// 前回ゼロ件 (flagged_keys=[]) でも初回扱いにならないこと
const r2b = classifyMarginRows(rows, threshold, []);
check('前回0件でも全件が新規扱い', r2b.newItems.length === 4 && r2b.contItems.length === 0);

// ─── Test 4: メッセージ整形 ───
console.log('Test 4: formatMarginAlertMessage');
const msg = formatMarginAlertMessage({
  todayJst: today, from, to, thresholdPct: threshold,
  result: r2, isFirstRun: false, skipped: [],
});
check('ヘッダー規約 (*粗利アラートサマリ*)', msg.startsWith('*粗利アラートサマリ*'));
check('新規セクションあり', msg.includes('🚨 新規割れ 2件'));
check('継続セクションあり', msg.includes('⚠️ 継続 2件'));
check('広告込み併記 (Amazon)', msg.includes('広告込 -1.5%'));
check('赤字件数表示', msg.includes('🔻 うち赤字 (粗利率<0%): 1件'));
check('判定対象外表示', msg.includes('判定対象外 (原価未登録など): 1件'));
check('モール別内訳あり', msg.includes('📊 モール別:'));
check('商品名が24文字で切られる', msg.includes('…'));
check('4000字未満', msg.length < 4000, `len=${msg.length}`);

// 初回実行メッセージ
const msgFirst = formatMarginAlertMessage({
  todayJst: today, from, to, thresholdPct: threshold,
  result: r1, isFirstRun: true, skipped: [],
});
check('初回は初回ラベル表示', msgFirst.includes('初回実行'));

// スキップモールの警告表示
const msgSkip = formatMarginAlertMessage({
  todayJst: today, from, to, thresholdPct: threshold,
  result: r2, isFirstRun: false, skipped: ['yahoo'],
});
check('スキップ警告表示', msgSkip.includes('集計スキップ') && msgSkip.includes('Yahoo'));

// ゼロ件メッセージ
const rEmpty = classifyMarginRows([], threshold, []);
const msgEmpty = formatMarginAlertMessage({
  todayJst: today, from, to, thresholdPct: threshold,
  result: rEmpty, isFirstRun: false, skipped: [],
});
check('ゼロ件は ✅ メッセージ', msgEmpty.includes('✅') && msgEmpty.includes('ありません'));

// ─── Test 4b: sanitize (Codex R1 Low — 改行/マークアップ入り商品名) ───
console.log('Test 4b: sanitizeText');
check('改行は空白化', sanitizeText('商品A\n偽装行') === '商品A 偽装行');
check('マークアップ除去', sanitizeText('*bold* _it_ `code`') === 'bold it code');
check('タブ/CR も空白化', sanitizeText('a\t\rb') === 'a b');

// ─── Test 4c: skipped モールの state 引き継ぎ (Codex R1 Medium #1) ───
console.log('Test 4c: mergeStateKeys');
const merged = mergeStateKeys(['rakuten:rk-low'], ['yahoo:yh-low', 'rakuten:rk-gone'], ['yahoo']);
check('skipped モールの前回キーを引き継ぐ', merged.includes('yahoo:yh-low') && merged.includes('rakuten:rk-low'));
check('成功モールの消えたキーは引き継がない', !merged.includes('rakuten:rk-gone'));
check('skipped なしなら現行キーのみ', mergeStateKeys(['a:1'], ['b:2'], []).join(',') === 'a:1');
check('前回 state なしでも安全', mergeStateKeys(['a:1'], undefined, ['yahoo']).join(',') === 'a:1');
check('現行と引き継ぎの重複は排除', mergeStateKeys(['yahoo:y1', 'a:1'], ['yahoo:y1'], ['yahoo']).join(',') === 'yahoo:y1,a:1');

// ─── Test 4d: Amazon ページング (Codex R1 High — limit 超過時の取りこぼし防止) ───
console.log('Test 4d: Amazon paging');
db.prepare(`
  INSERT INTO mirror_amazon_finance_sku_daily (
    date_jst, seller_sku, asin_norm, product_name, units_ordered, units_net_sold,
    sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy,
    cogs_amount, profit_amount, is_cost_complete, cost_status,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'amz-low2', 'B000TEST02', 'Amazon低粗利2', 5, 5, 50000, 0, 0, 40000, 2500, 1, 'complete', 'smoke', 'h10', datetime('now'))
`).run(d2);
const paged = collectMarginRows(db, from, to, { amazonPageSize: 1 });
const pagedAmz = paged.rows.filter(r => r.mall === 'amazon');
check('pageSize=1 でも全 Amazon SKU 取得', pagedAmz.length === 2, `got=${pagedAmz.length}`);

// ─── Test 5: 閾値 env 不正値フォールバック ───
console.log('Test 5: resolveThresholdPct');
process.env.MARGIN_ALERT_THRESHOLD_PCT = 'abc';
check('不正値はデフォルト10', resolveThresholdPct() === 10);
process.env.MARGIN_ALERT_THRESHOLD_PCT = '15';
check('正常値15は採用', resolveThresholdPct() === 15);
delete process.env.MARGIN_ALERT_THRESHOLD_PCT;

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail > 0 ? 1 : 0);
