/**
 * 取り込み〜画面表示までの通し検証 (実データを使う)。
 *   node apps/product-scout/test-ingest.mjs <concepts_YYYYMMDD.json>
 *
 * 引数を省くと合成データで動かす。実ファイルを渡すと、本番と同じ規模 (約2,000テーマ) で
 * 「取り込めるか」「画面が引く SQL が正しい行を返すか」を確かめる。
 */
import Database from 'better-sqlite3';
import assert from 'node:assert';
import fs from 'node:fs';
import { createProductScoutTables } from './schema.js';
import {
  ingestSnapshot, getLatestSnapshot, listCategories, listConcepts, countConcepts,
  getConcept, recordDecision, countMatching,
} from './db.js';

const db = new Database(':memory:');
db.pragma('recursive_triggers = ON');
createProductScoutTables(db);

const file = process.argv[2];
const payload = file
  ? JSON.parse(fs.readFileSync(file, 'utf8'))
  : {
    generatedAt: '2026-08-28T02:00:00Z', algorithmVersion: 1,
    sourceProducts: 100, afterBaseFilter: 60, lastProgressAt: '2026-08-28T01:50:00Z', remainingTotal: 5,
    collection: [
      { rootCategory: '1', name: 'DIY', state: 'collecting', asinTarget: 100, fetched: 95, complete: true, estimatedMissing: 0, fetchedAt: '2026-08-28T01:00:00Z', remaining: 5 },
      { rootCategory: '2', name: 'ペット', state: 'not_started', asinTarget: null, fetched: 0, complete: null, estimatedMissing: null, fetchedAt: null, remaining: null },
    ],
    concepts: [
      { concept: 'A × シート裁断', categoryPath: 'x > y', form: 'シート裁断', hardGate: 'pass', rank: 1,
        gates: { amc: 'pass', size: 'pass', commodity: 'ok', bigBrand: 'open' },
        productCount: 10, totalMonthlySold: 5000, brandCount: 8, top1Brand: 'B', top1SharePct: 20,
        examples: [{ asin: 'B0TEST', title: 't', brand: 'B', monthlySold: 100, price: 500 }] },
      { concept: 'B × 食品', categoryPath: 'x > z', form: '食品・飼料', hardGate: 'fail', rank: 2,
        gateFailReason: 'AMCで作れない (食品・飼料)',
        gates: { amc: 'fail', size: 'unknown', commodity: 'ok', bigBrand: 'open' },
        productCount: 5, totalMonthlySold: 9000, brandCount: 3, examples: [] },
    ],
  };

let passed = 0;
const ok = (n) => { passed++; console.log('  ✓ ' + n); };

const t0 = Date.now();
const r = ingestSnapshot(payload, db);
console.log(`  取り込み ${r.concepts}テーマ / ${r.categories}カテゴリ (${Date.now() - t0}ms)`);
assert.strictEqual(r.concepts, payload.concepts.length);
ok('取り込みが通る');

// ⭐同じスナップショットを2回入れても増えない (冪等)。バッチは何度でも再実行されうる
const snap0 = getLatestSnapshot(db);
const before = countConcepts(snap0.snapshot_id, db);
ingestSnapshot(payload, db);
const after = countConcepts(snap0.snapshot_id, db);
assert.deepStrictEqual(after, before, '2回目の取り込みで件数が変わってはいけない');
ok('取り込みが冪等 (2回流しても増えない)');

const snap = getLatestSnapshot(db);
assert.ok(snap, 'スナップショットが引ける');
assert.strictEqual(snap.remaining_total, payload.remainingTotal);
assert.strictEqual(snap.last_progress_at, payload.lastProgressAt);
ok('最終前進時刻と残件が保存される');

const cats = listCategories(snap.snapshot_id, db);
assert.strictEqual(cats.length, payload.collection.length);
assert.ok(cats.some((c) => c.state === 'not_started'), '未投入カテゴリも行として残る');
ok('工程表に全カテゴリが並ぶ (未投入も含めて)');

const counts = countConcepts(snap.snapshot_id, db);
console.log(`  ゲート: 通過${counts.pass} / 要確認${counts.unknown} / 落ち${counts.fail}`);
assert.ok(counts.pass > 0, 'ゲート通過が1件以上');
ok('ゲート別の件数が数えられる');

const passList = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'undecided', limit: 5 }, db);
assert.ok(passList.length > 0);
assert.ok(passList.every((c) => c.hard_gate === 'pass'), 'ゲート通過だけが返る');
assert.ok(passList.every((c) => c.decision === null), '未判断だけが返る');
assert.ok(Array.isArray(passList[0].examples), '代表商品が配列で戻る');
ok('審査待ちの一覧が引ける');

// 順位どおりに並ぶ (人が見る順序が壊れていない)
const ranks = passList.map((c) => c.rank_in_snapshot);
assert.deepStrictEqual(ranks, [...ranks].sort((a, b) => a - b));
ok('順位どおりに並ぶ');

// 採否 → 一覧から消える
const target = passList[0];
recordDecision({ conceptId: target.concept_id, decision: 'reject', reasonCode: 'commodity_price',
  comment: '中華が強い', decidedBy: 'test@example.com' }, db);
const afterDecide = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'undecided', limit: 5 }, db);
assert.ok(!afterDecide.some((c) => c.concept_id === target.concept_id), '判断したものは審査待ちから外れる');
ok('採否を記録すると審査待ちから外れる');

const detail = getConcept(target.concept_id, db);
assert.strictEqual(detail.history.length, 1);
assert.ok(JSON.parse(detail.history[0].metrics_json).totalMonthlySold != null,
  '判断時点の指標が固定されていること');
ok('判断時点の指標がイベントに固定される');

// 不採用は理由コード必須 (画面のバグで理由なしが入らないように、DB層でも止める)
assert.throws(() => recordDecision({ conceptId: target.concept_id, decision: 'reject',
  decidedBy: 'x' }, db), /理由コード/);
ok('不採用は理由コードが必須');
assert.throws(() => recordDecision({ conceptId: target.concept_id, decision: 'reject',
  reasonCode: 'other', decidedBy: 'x' }, db), /コメント/);
ok('理由が「その他」ならコメント必須');

// ⭐再取り込みしても判断は残る (収集は毎日更新されるので、ここが消えると台帳が死ぬ)
ingestSnapshot(payload, db);
const stillDecided = getConcept(target.concept_id, db);
assert.strictEqual(stillDecided.history.length, 1, '再取り込みで判断履歴が消えてはいけない');
ok('再取り込みしても判断履歴は消えない');

// ⭐同一ミリ秒に判断が2件入っても、一覧が重複しないこと。
//   MAX(decided_at) で最新を選んでいた頃は、二重送信や別タブの同時操作で
//   2行とも「最新」になり、テーマが重複表示され件数まで水増しされた。
const dupTarget = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'undecided', limit: 1 }, db)[0];
const sameMs = '2026-09-20T00:00:00.000Z';
const rawInsert = db.prepare(
  'INSERT INTO scout_decisions (decision_id, concept_id, decision, decided_by, decided_at, metrics_json)'
  + " VALUES (?, ?, 'hold', 't', ?, '{}')"
);
rawInsert.run('dup1', dupTarget.concept_id, sameMs);
rawInsert.run('dup2', dupTarget.concept_id, sameMs);
const dupRows = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'all', limit: 500 }, db)
  .filter((c) => c.concept_id === dupTarget.concept_id);
assert.strictEqual(dupRows.length, 1, '同一時刻の判断が2件あっても一覧は1行');
ok('同一ミリ秒の判断が2件あっても最新が1件に決まる');

// ページ送り (約2,000テーマあるので上位だけ見て終わりにはできない)
const totalPass = countMatching({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'all' }, db);
assert.ok(totalPass > 0);
const p1 = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'all', limit: 2, offset: 0 }, db);
const p2 = listConcepts({ snapshotId: snap.snapshot_id, gate: 'pass', status: 'all', limit: 2, offset: 2 }, db);
assert.ok(p1.length > 0, '1ページ目が引ける');
assert.ok(!p1.some((a) => p2.some((b) => b.concept_id === a.concept_id)), 'ページ間で重複しない');
ok(`ページ送りできる (総件数 ${totalPass} / offset)`);

// ⭐同じ payload を送り直しても同じスナップショットになること。
//   ID に現在時刻を混ぜていた頃は、再送のたびに別スナップショットが積もった。
const idA = ingestSnapshot(payload, db).snapshotId;
const idB = ingestSnapshot(payload, db).snapshotId;
assert.strictEqual(idA, idB, '同じ中身なら同じ snapshot_id');
ok('再送しても同じスナップショットになる (冪等ID)');

// 判断者が取れないものは記録しない (台帳の値打ちは監査できることにある)
assert.throws(() => recordDecision({ conceptId: dupTarget.concept_id, decision: 'hold', decidedBy: null }, db),
  /判断者/);
ok('判断者が取れないときは記録しない');

// 採用・保留に不採用理由が付くと、後で理由を集計したときに水増しされる
assert.throws(() => recordDecision({ conceptId: dupTarget.concept_id, decision: 'adopt',
  reasonCode: 'no_edge', decidedBy: 't' }, db), /採用・保留/);
ok('採用・保留に不採用理由は付けられない');

// ⭐アプリを迂回して直接INSERTしても、不採用理由なしは DB が弾くこと
assert.throws(() => db.prepare(
  'INSERT INTO scout_decisions (decision_id, concept_id, decision, decided_by, decided_at)'
  + " VALUES ('raw1', ?, 'reject', 't', '2026-09-21T00:00:00Z')"
).run(dupTarget.concept_id), /CHECK/);
ok('DB制約でも不採用理由なしは弾かれる');

console.log(`\n${passed} 件すべて通りました`);
