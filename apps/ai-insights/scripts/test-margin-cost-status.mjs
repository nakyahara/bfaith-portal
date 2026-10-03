#!/usr/bin/env node
import { temporaryTestRoot } from '../../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
// ai-insights: 粗利のワースト SKU・原価の整備率が「原価がそろった行」を正しく数えるかの試験
// 実行: node apps/ai-insights/scripts/test-margin-cost-status.mjs
//
// 背景 (2026-10-03 Codex R-F4-1 High 6): facts.js が cost_status = 'ok' で絞っていたが、
// 統合 view (v_mall_finance_daily_unified) に入る 6 モールの finance 表の CHECK は
// complete / missing_cost / partial_cost / late_bound_after_close で 'ok' が無い
// → ワースト SKU が全モールで 0 件・cost_ok_row_share_pct が 0% だった。
//
// 試すこと:
//   1. 契約: 6 表すべての cost_status の CHECK に COST_COMPLETE_STATUS があり 'ok' が無い
//      (表の決まりが変わったら、この判定も一緒に直すよう落とす)
//   2. ワースト SKU: complete の赤字だけが 6 モールとも出る / missing・partial・late_bound は出ない
//   3. 整備率: complete の行の割合 (6 モールとも 2/5 = 40%)
//   4. 週次の GChat (AI 不調時の機械整形): 「赤字SKUあり」の論点が出る

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'ai-insights-margin-'));
process.env.DATA_DIR = scratch;

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '../../..');
const load = (rel) => import(pathToFileURL(path.join(repoRoot, rel)).href);

const { initMirrorDB, getMirrorDB } = await load('apps/warehouse-mirror/db.js');
initMirrorDB();
const { buildMarginFacts, COST_COMPLETE_STATUS } = await load('apps/ai-insights/facts.js');
const { buildFallbackBody, buildGChatMessage } = await load('scripts/ai-insights/weekly-prompt.js');

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${extra !== undefined ? ` — ${JSON.stringify(extra)?.slice(0, 400)}` : ''}`); }
}

const db = getMirrorDB();

// 統合 view に入る 6 表と、view が読む列 (sku / 売上 / 粗利)
const MALLS = [
  { mall: 'amazon', table: 'mirror_amazon_finance_sku_daily', sku: 'seller_sku',
    row: (sales, margin) => ({ sales_principal_jpy: sales, profit_amount: margin }) },
  { mall: 'rakuten', table: 'mirror_rakuten_finance_sku_daily', sku: 'rakuten_code',
    row: (sales, margin) => ({ gross_sales_jpy_incl: sales, variable_margin_jpy_incl: margin }) },
  { mall: 'yahoo', table: 'mirror_yahoo_finance_sku_daily', sku: 'yahoo_sku_key',
    row: (sales, margin) => ({ gross_sales_jpy_incl: sales, variable_margin_full_jpy_incl: margin, variable_margin_partial_jpy_incl: margin }) },
  { mall: 'aupay', table: 'mirror_aupay_finance_sku_daily', sku: 'aupay_sku_key',
    row: (sales, margin) => ({ gross_sales_jpy_incl: sales, variable_margin_full_jpy_incl: margin, variable_margin_partial_jpy_incl: margin }) },
  { mall: 'qoo10', table: 'mirror_qoo10_finance_sku_daily', sku: 'sku_code',
    row: (sales, margin) => ({ customer_paid_jpy_incl: sales, variable_margin_jpy_incl: margin, variable_margin_full_jpy_incl: margin }) },
  { mall: 'linegift', table: 'mirror_linegift_finance_sku_daily', sku: 'sku_code',
    row: (sales, margin) => ({ gross_sales_jpy_incl: sales, variable_margin_jpy_incl: margin }) },
];

/** CREATE TABLE の文から「列 IN ('a','b',...)」の許される値を読む */
function enumsOf(table) {
  const sql = db.prepare('SELECT sql FROM sqlite_master WHERE type = ? AND name = ?').get('table', table)?.sql || '';
  const out = {};
  for (const m of sql.matchAll(/(\w+)\s+IN\s*\(([^)]*)\)/g)) {
    if (!out[m[1]]) out[m[1]] = [...m[2].matchAll(/'([^']*)'/g)].map((x) => x[1]);
  }
  return out;
}

/** 表の NOT NULL・既定値なしの列を、型と CHECK の最初の値で埋めて 1 行入れる */
function insertRow(table, values) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  const enums = enumsOf(table);
  const row = {};
  for (const c of cols) {
    if (Object.hasOwn(values, c.name)) row[c.name] = values[c.name];
    else if (c.notnull && c.dflt_value == null) {
      row[c.name] = enums[c.name]?.[0] ?? (/INT|REAL|NUM/i.test(c.type) ? 0 : 'x');
    }
  }
  for (const k of Object.keys(values)) {
    if (!cols.some((c) => c.name === k)) throw new Error(`${table} に列 ${k} が無い`);
  }
  const names = Object.keys(row);
  db.prepare(`INSERT INTO ${table} (${names.join(',')}) VALUES (${names.map((n) => '@' + n).join(',')})`).run(row);
}

// ═══ 1. 契約 ═══
console.log('\n■ 契約: 6 表の cost_status の CHECK');
const EXPECTED_ENUM = ['complete', 'missing_cost', 'partial_cost', 'late_bound_after_close'];
ok(COST_COMPLETE_STATUS === 'complete', `COST_COMPLETE_STATUS = 'complete'`, COST_COMPLETE_STATUS);
for (const m of MALLS) {
  const e = enumsOf(m.table).cost_status;
  ok(Array.isArray(e) && e.includes(COST_COMPLETE_STATUS) && !e.includes('ok'),
    `${m.table}: CHECK に '${COST_COMPLETE_STATUS}' があり 'ok' は無い`, e);
  ok(JSON.stringify([...(e || [])].sort()) === JSON.stringify([...EXPECTED_ENUM].sort()),
    `${m.table}: CHECK の値は 4 つ (増減したらこの判定を見直す)`, e);
}
// 統合 view が全モールを出していること (ここが欠けると試験が空振りする)
const viewMalls = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'v_mall_finance_daily_unified'").get()?.sql || '';
for (const m of MALLS) ok(viewMalls.includes(m.table), `統合 view が ${m.table} を読む`);

// ═══ fixture ═══
// 対象週 2026-07-06(月)〜07-12(日)。各モールに 5 SKU:
//   neg-complete (赤字・原価そろう) / neg-missing / neg-partial / neg-late (赤字・原価そろわない) / pos-complete (黒字)
const PS = '2026-07-06';
const PE = '2026-07-13';
const STATUS_ROWS = [
  { key: 'neg-complete', status: 'complete', sales: 1000, margin: -300 },
  { key: 'neg-missing', status: 'missing_cost', sales: 1000, margin: -500 },
  { key: 'neg-partial', status: 'partial_cost', sales: 1000, margin: -700 },
  { key: 'neg-late', status: 'late_bound_after_close', sales: 1000, margin: -900 },
  { key: 'pos-complete', status: 'complete', sales: 2000, margin: 400 },
];
MALLS.forEach((m, i) => {
  for (const s of STATUS_ROWS) {
    // モールごとに金額をずらし、ワーストの並びを決定的にする
    const margin = s.margin < 0 ? s.margin - i * 10 : s.margin;
    insertRow(m.table, {
      date_jst: '2026-07-08',
      [m.sku]: `${m.mall}-${s.key}`,
      product_name: `${m.mall} ${s.key}`,
      units_net_sold: 1,
      cost_status: s.status,
      is_cost_complete: s.status === 'complete' ? 1 : 0,
      source_run_id: 'r', source_row_hash: `${m.mall}-${s.key}`, synced_at: '2026-07-13T00:00:00Z',
      ...m.row(s.sales, margin),
    });
  }
});

// ═══ 2. ワースト SKU ═══
console.log('\n■ ワースト SKU (原価がそろった赤字だけ)');
const facts = buildMarginFacts(db, PS, PE);
const worst = facts.negative_margin_skus;
ok(worst.length === MALLS.length, `ワーストは ${MALLS.length} 件 (各モールの neg-complete)`, worst);
for (const m of MALLS) {
  ok(worst.some((w) => w.mall === m.mall && w.sku === `${m.mall}-neg-complete`), `${m.mall}: 原価のそろった赤字 SKU が出る`, worst);
}
ok(worst.every((w) => w.sku.endsWith('neg-complete')), 'missing / partial / late_bound の赤字は出ない', worst.map((w) => w.sku));
ok(worst[0]?.sku === 'linegift-neg-complete' && worst[0]?.margin_yen === -350, '並び: いちばんの赤字が先頭 (linegift -350 円)', worst[0]);
ok(worst.length > 0 && worst.every((w) => w.units === 1 && w.sales_yen === 1000), '数量・売上の数字', worst);

// ═══ 3. 原価の整備率 ═══
console.log('\n■ 原価の整備率 (cost_ok_row_share_pct)');
for (const m of MALLS) {
  const r = facts.by_mall.find((b) => b.mall === m.mall);
  ok(r?.cost_ok_row_share_pct === 40, `${m.mall}: complete 2 行 / 5 行 = 40%`, r);
}
// 粗利の合計は status に関係なく全部の行 (絞るのはワーストと整備率だけ)
const amz = facts.by_mall.find((b) => b.mall === 'amazon');
ok(amz?.margin_yen === -300 - 500 - 700 - 900 + 400 && amz?.sales_yen === 6000, 'amazon の粗利・売上の合計は全部の行', amz);

// ═══ 4. 週次の GChat (AI 不調時の機械整形) ═══
console.log('\n■ 週次の GChat の文');
const input = {
  meta: { period_start: PS, period_label: '2026-07-06〜2026-07-12' },
  constraints: { prohibited_topic_areas: [] },
  coverage: [],
  facts: { margin: facts },
};
const body = buildFallbackBody(input);
const text = buildGChatMessage(body, input, 'WK-TEST');
console.log('----- GChat -----\n' + text + '\n-----------------');
ok(body.topics.some((t) => t.category === 'margin'), '粗利の論点が出る', body.topics);
ok(text.includes('赤字SKUあり: linegift neg-complete'), 'GChat に「赤字SKUあり: linegift neg-complete」', text);
ok(text.includes('linegift / 週間粗利 *-350円* (*1個*)'), 'GChat に週間粗利 -350 円・1 個', text);

console.log(`\n═══ 結果: ${pass} PASS / ${fail} FAIL ═══`);
try { db.close(); } catch { /* noop */ }
process.exitCode = fail > 0 ? 1 : 0;
