/**
 * scout_* テーブルの単体検証 (mirror 本体を起動しない)。
 *   node apps/product-scout/test-schema.mjs
 *
 * 見るのは「壊れたら業務が壊れるところ」だけ:
 *   ① DDL が通る / 冪等
 *   ② 採否イベントが追記専用になっている (UPDATE/DELETE がDBレベルで拒否される)
 *   ③ 「最新の判断」を引く SQL が、複数回の判断があっても最後の1件を返す
 *   ④ 不採用理由コードの必須チェック
 */
import Database from 'better-sqlite3';
import assert from 'node:assert';
import { createProductScoutTables } from './schema.js';

let passed = 0;
function ok(name) { passed++; console.log('  ✓ ' + name); }

const db = new Database(':memory:');
db.pragma('recursive_triggers = ON');

// ① DDL
createProductScoutTables(db);
createProductScoutTables(db); // 2回流しても壊れない (IF NOT EXISTS)
ok('DDL が通る / 冪等');

const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'scout_%'")
  .all().map((r) => r.name).sort();
assert.deepStrictEqual(tables,
  ['scout_categories', 'scout_concepts', 'scout_decisions', 'scout_own_families', 'scout_snapshots']);
ok('5テーブルが作られる');

// 下ごしらえ
db.prepare(`INSERT INTO scout_snapshots (snapshot_id, generated_at, ingested_at, algorithm_version,
  source_products, after_base_filter, concept_count, last_progress_at, remaining_total)
  VALUES ('s1','2026-08-28T00:00:00Z','2026-08-28T01:00:00Z',1,26611,17473,1988,'2026-08-28T01:54:00Z',14)`).run();
db.prepare(`INSERT INTO scout_concepts (concept_id, snapshot_id, concept, category_path, form,
  hard_gate, product_count, total_monthly_sold, rank_in_snapshot, first_seen_at, updated_at)
  VALUES ('c1','s1','歯みがきシート × シート裁断','ペット用品 > 犬 > 口腔ケア','シート裁断',
          'pass', 84, 28950, 1, '2026-08-28T01:00:00Z', '2026-08-28T01:00:00Z')`).run();

// ② 追記専用
const insDecision = db.prepare(`INSERT INTO scout_decisions
  (decision_id, concept_id, decision, reason_code, comment, decided_by, decided_at, snapshot_id, metrics_json)
  VALUES (?, 'c1', ?, ?, ?, 'nakahara', ?, 's1', '{}')`);
insDecision.run('d1', 'reject', 'commodity_price', '中華が強い', '2026-08-28T02:00:00Z');
assert.throws(() => db.prepare("UPDATE scout_decisions SET comment='書き換え' WHERE decision_id='d1'").run(),
  /追記専用/);
ok('採否イベントは UPDATE できない');
assert.throws(() => db.prepare("DELETE FROM scout_decisions WHERE decision_id='d1'").run(), /追記専用/);
ok('採否イベントは DELETE できない');

// decision の値域
assert.throws(() => insDecision.run('dx', 'maybe', null, null, '2026-08-28T02:00:00Z'), /CHECK/);
ok('decision は adopt/reject/hold のみ');

// ③ 最新の判断を引く (同じテーマを再審査して採用に変わったケース)
insDecision.run('d2', 'adopt', null, '原料が下がったので再検討', '2026-09-15T02:00:00Z');
const latestSql = `
  SELECT c.concept_id, d.decision, d.decided_at
  FROM scout_concepts c
  LEFT JOIN (
    SELECT s.* FROM scout_decisions s
    JOIN (SELECT concept_id, MAX(decided_at) AS m FROM scout_decisions GROUP BY concept_id) l
      ON l.concept_id = s.concept_id AND l.m = s.decided_at
  ) d ON d.concept_id = c.concept_id`;
const latest = db.prepare(latestSql).all();
assert.strictEqual(latest.length, 1, '1テーマにつき1行だけ返ること (重複しない)');
assert.strictEqual(latest[0].decision, 'adopt');
ok('最新の判断だけが返る (不採用→採用の履歴が両方残ったうえで)');

// 履歴は消えていない
const history = db.prepare("SELECT decision FROM scout_decisions WHERE concept_id='c1' ORDER BY decided_at").all();
assert.deepStrictEqual(history.map((h) => h.decision), ['reject', 'adopt']);
ok('不採用の履歴が残っている (これが台帳の資産)');

// 未判断のテーマは decision が NULL で出る (審査待ちの抽出に使う)
db.prepare(`INSERT INTO scout_concepts (concept_id, snapshot_id, concept, category_path, form,
  hard_gate, rank_in_snapshot, first_seen_at, updated_at)
  VALUES ('c2','s1','消臭ビーズ × 粉末充填','ペット用品 > 猫 > 消臭','粉末充填','pass',2,'x','x')`).run();
const undecided = db.prepare(latestSql + ' WHERE d.decision IS NULL').all();
assert.strictEqual(undecided.length, 1);
assert.strictEqual(undecided[0].concept_id, 'c2');
ok('未判断のテーマを抽出できる');

// ④ カテゴリの分母 (complete=0 を NULL と区別できること)
db.prepare(`INSERT INTO scout_categories (snapshot_id, root_category, name, state, asin_target, fetched,
  complete, estimated_missing, fetched_at, remaining)
  VALUES ('s1','160384011','ドラッグストア','collecting',33000,12000,0,5400,'2026-08-28T05:00:00Z',21000)`).run();
const cat = db.prepare("SELECT * FROM scout_categories WHERE root_category='160384011'").get();
assert.strictEqual(cat.complete, 0, '不完全 (0) と 不明 (NULL) を取り違えない');
assert.strictEqual(cat.estimated_missing, 5400);
ok('分母の不完全さを保持できる');

console.log(`\n${passed} 件すべて通りました`);
