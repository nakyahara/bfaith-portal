/**
 * 画面 (EJS) が実データで描画できるかを確かめる。
 *   node apps/product-scout/test-render.mjs [concepts_YYYYMMDD.json]
 *
 * SQL のテストだけでは、テンプレートの参照漏れ (undefined.toLocaleString() 等) を
 * ログインして開くまで誰も気づけない。ここで先に落としておく。
 */
import ejs from 'ejs';
import Database from 'better-sqlite3';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createProductScoutTables } from './schema.js';
import {
  ingestSnapshot, ingestOwnFamilies, getLatestSnapshot, listCategories,
  listConcepts, countConcepts, countMatching, REASON_CODES, recordDecision,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const db = new Database(':memory:');
db.pragma('recursive_triggers = ON');
createProductScoutTables(db);

const file = process.argv[2];
const payload = file ? JSON.parse(fs.readFileSync(file, 'utf8')) : {
  generatedAt: '2026-08-28T02:00:00Z', algorithmVersion: 1,
  sourceProducts: 100, afterBaseFilter: 60, lastProgressAt: '2026-08-28T01:50:00Z', remainingTotal: 5,
  collection: [{ rootCategory: '1', name: 'DIY', state: 'collecting', asinTarget: 100, fetched: 95,
    complete: false, estimatedMissing: 400, fetchedAt: '2026-08-28T01:00:00Z', remaining: 5 }],
  concepts: [{ concept: 'A × シート裁断', categoryPath: 'x > y', form: 'シート裁断', hardGate: 'pass', rank: 1,
    gates: { amc: 'pass', size: 'pass', commodity: 'suspect', bigBrand: 'dominated' },
    productCount: 10, totalMonthlySold: 5000, brandCount: 8, top1Brand: 'B', top1SharePct: 20,
    medianPrice: 900, medianReferralFeePct: 15, smallSizeRatePct: null, medianReviewCount: 12,
    sourceComplete: false, sourceFetchedAt: '2026-08-28T01:00:00Z',
    examples: [{ asin: 'B0TEST', title: 't', brand: 'B', monthlySold: 100, price: 500, reviewCount: 3 }] }],
};
ingestSnapshot(payload, db);

const snap = getLatestSnapshot(db);
const categories = listCategories(snap.snapshot_id, db);
const sample = listConcepts({ snapshotId: snap.snapshot_id, gate: 'all', status: 'all', limit: 1 }, db)[0];
ingestOwnFamilies({ families: [
  { familyKey: '自社の既存商品', categoryPath: sample.category_path, form: sample.form, amcCapable: true,
    skuCount: 3, asinCount: 2, launchedOn: '2023-04-01', lastSoldOn: '2026-08-01', qty180: 1200,
    qtyAll: 9000, activeSkus: 3, discontinuedSkus: 0, medianPrice: 880, outcome: 'active' },
  { familyKey: '撤退した商品', categoryPath: sample.category_path, form: sample.form, amcCapable: true,
    skuCount: 2, asinCount: 1, launchedOn: '2020-03-05', lastSoldOn: '2021-06-01', qty180: 40,
    qtyAll: 120, activeSkus: 0, discontinuedSkus: 2, medianPrice: 700, outcome: 'withdrawn' },
] }, db);

// 判断済みの行も描けること (未判断だけ描いて満足しない)
recordDecision({ conceptId: sample.concept_id, decision: 'reject', reasonCode: 'commodity_price',
  comment: '中華が強い', recheckCondition: '原料が30%下がれば再検討', decidedBy: 'test@example.com' }, db);

const template = fs.readFileSync(path.join(__dirname, 'views/index.ejs'), 'utf8');
const counts = countConcepts(snap.snapshot_id, db);

// 実際に起きうる組み合わせを一通り描く。落ちるのはたいてい端のケース
const cases = [
  { label: '審査待ち (ゲート通過)', gate: 'pass', status: 'undecided' },
  { label: 'ゲート落ち', gate: 'fail', status: 'all' },
  { label: '要確認', gate: 'unknown', status: 'all' },
  { label: '判断済み (不採用)', gate: 'all', status: 'reject' },
  { label: '該当なし', gate: 'pass', status: 'adopt' },
];
let passed = 0;
for (const c of cases) {
  const total = countMatching({ snapshotId: snap.snapshot_id, gate: c.gate, status: c.status }, db);
  const concepts = listConcepts({ snapshotId: snap.snapshot_id, gate: c.gate, status: c.status, limit: 40 }, db);
  const html = ejs.render(template, {
    username: 'test@example.com', displayName: '中原',
    snapshot: snap, categories, counts, concepts, gate: c.gate, status: c.status,
    page: 1, pageSize: 40, total, totalPages: Math.max(1, Math.ceil(total / 40)),
    reasonCodes: REASON_CODES,
    signal: { level: 'green', title: '収集中', detail: '審査待ち n件' },
  }, { filename: path.join(__dirname, 'views/index.ejs') });
  assert.ok(html.includes('</html>'), `${c.label}: 最後まで描けていない`);
  assert.ok(!html.includes('undefined'), `${c.label}: undefined が画面に出ている`);
  assert.ok(!html.includes('NaN'), `${c.label}: NaN が画面に出ている`);
  console.log(`  ✓ ${c.label} (${concepts.length}件 / 全${total}件・${html.length}文字)`);
  passed++;
}

// 取り込み前 (まだ何も無い) でも描けること — 初回デプロイ直後の画面
const empty = ejs.render(template, {
  username: 'x', displayName: 'x', snapshot: null, categories: [], concepts: [],
  counts: { pass: 0, unknown: 0, fail: 0, undecidedPass: 0, decided: 0 },
  gate: 'pass', status: 'undecided', page: 1, pageSize: 40, total: 0, totalPages: 1,
  reasonCodes: REASON_CODES,
  signal: { level: 'gray', title: 'まだ取り込みがありません', detail: '...' },
}, { filename: path.join(__dirname, 'views/index.ejs') });
assert.ok(empty.includes('</html>'));
console.log('  ✓ 取り込み前 (空) でも描ける');
passed++;

// 自社の情報が実際に出ていること (JOINしただけで表示漏れ、を防ぐ)
const withOwn = ejs.render(template, {
  username: 'x', displayName: 'x', snapshot: snap, categories, counts,
  concepts: listConcepts({ snapshotId: snap.snapshot_id, gate: 'all', status: 'all', limit: 40 }, db),
  gate: 'all', status: 'all', page: 1, pageSize: 40, total: 1, totalPages: 1,
  reasonCodes: REASON_CODES, signal: { level: 'green', title: 'x', detail: 'y' },
}, { filename: path.join(__dirname, 'views/index.ejs') });
assert.ok(withOwn.includes('自社で販売中') || withOwn.includes('過去に出して撤退した'),
  '自社実績が画面に出ていない');
assert.ok(withOwn.includes('下限'), '不完全な分母が「下限」と表示されていない');
console.log('  ✓ 自社実績と「下限%」が画面に出る');
passed++;

console.log(`\n${passed} 件すべて通りました`);
