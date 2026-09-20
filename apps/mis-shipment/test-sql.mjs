/**
 * 誤出荷管理の SQL を、実 SQLite (一時ファイル) に通して確かめる。
 *
 *   node apps/mis-shipment/test-sql.mjs
 *
 * DDL は apps/warehouse-mirror/db.js の実物から抜き出して使う (テスト用に書き直さない)。
 * 書き直すと「テストは緑だが本番の DDL は違う」が起きる。
 *
 * 見ているもの:
 *   - 「要確認」(2026-09-20 の修正より前に登録された行) の判定と絞り込み
 *   - 訂正 → 訂正履歴 → 確認印 で「要確認」が消えること
 *   - テレコ (mix_up) の種別変更が CHECK 制約で拒否されること
 *   - 訂正履歴が append-only (UPDATE/DELETE が trigger で拒否される) こと
 *   - 一覧の LIKE 検索で % _ がワイルドカードにならないこと
 */
import Database from 'better-sqlite3';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// 既定はこのファイルが置かれているリポジトリ。
// node_modules が無い worktree の DDL を確かめたいときだけ MIS_TEST_REPO で差し替える。
const REPO = process.env.MIS_TEST_REPO ? path.resolve(process.env.MIS_TEST_REPO) : path.resolve(__dirname, '../..');

let failures = 0;
function check(label, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    console.log(`  ✅ ${label}: ${a}`);
  } else {
    failures++;
    console.log(`  ❌ ${label}: ${a}  (期待: ${e})`);
  }
}
function checkThrows(label, fn) {
  try {
    fn();
    failures++;
    console.log(`  ❌ ${label}: 拒否されずに通ってしまった`);
  } catch (e) {
    console.log(`  ✅ ${label}: ${String(e.message).split('\n')[0]}`);
  }
}

// ── DDL を実物から抜き出す ──────────────────────────────────
const mirrorSrc = fs.readFileSync(path.join(REPO, 'apps/warehouse-mirror/db.js'), 'utf8');
const stmts = [];
const re = /db\.exec\(\s*(`[\s\S]*?`|'[^']*')\s*\)/g;
let m;
while ((m = re.exec(mirrorSrc)) !== null) {
  const raw = m[1];
  if (!raw.includes('f_mis_shipment')) continue;
  if (raw.includes('${')) throw new Error('テンプレート展開が必要な DDL があります: ' + raw.slice(0, 60));
  stmts.push(raw.slice(1, -1));
}
if (stmts.length === 0) throw new Error('f_mis_shipment の DDL が見つかりませんでした');
console.log(`抜き出した DDL 文: ${stmts.length}`);

const dbPath = path.join(os.tmpdir(), `mis-shipment-test-${process.pid}.db`);
fs.rmSync(dbPath, { force: true });
const db = new Database(dbPath);
db.pragma('foreign_keys = ON');
db.pragma('recursive_triggers = ON');
for (const sql of stmts) db.exec(sql);

const tables = db.prepare(
  "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'f_mis%' ORDER BY name"
).all().map((r) => r.name);
check('作られた表', tables, ['f_mis_shipment_field_history', 'f_mis_shipment_status_history', 'f_mis_shipments']);

// ── テストデータ ────────────────────────────────────────────
const now = '2026-09-19T01:00:00.000Z';
function insert(id, clientId, createdAt, misType, stage, groupId) {
  db.prepare(`
    INSERT INTO f_mis_shipments
      (id, client_submission_id, payload_hash, version, occurred_on, reported_at,
       mall_order_id, order_id_unknown, mall, lookup_source, mis_type, qty_affected,
       loss_amount_jpy, process_stage, root_cause_stage, mix_up_group_id, status,
       reported_by, created_at, updated_at, updated_by)
    VALUES (?, ?, ?, 0, '2026-09-18', ?, '503-1', 0, 'amazon', 'mirror_auto', ?, 1,
            100, ?, 'unknown', ?, 'reported', 'a@b.c', ?, ?, 'a@b.c')
  `).run(id, clientId, 'a'.repeat(64), now, misType, stage, groupId, createdAt, createdAt);
}
insert(1, 'c1', '2026-09-18T00:00:00.000Z', 'wrong_item', 'picking', null);   // 不具合の時期
insert(2, 'c2', '2026-09-21T00:00:00.000Z', 'damage', 'inspection', null);    // 修正後
insert(3, 'c3', '2026-09-18T00:00:00.000Z', 'mix_up', 'picking', 'g-0001');   // テレコ

// db.js の NEEDS_FIELD_REVIEW_SQL と同じ条件
const FIELD_BUG_FIXED_AT = '2026-09-20T06:00:00.000Z';
const NEEDS = `
  created_at < ?
  AND NOT EXISTS (
    SELECT 1 FROM f_mis_shipment_field_history h
     WHERE h.mis_shipment_id = f_mis_shipments.id
       AND h.field_name = 'field_review'
  )`;

const flags = () => db.prepare(
  `SELECT id, (CASE WHEN ${NEEDS} THEN 1 ELSE 0 END) AS needs FROM f_mis_shipments
    WHERE deleted_at IS NULL ORDER BY id`
).all(FIELD_BUG_FIXED_AT).map((r) => [r.id, r.needs]);

console.log('\n[1] 要確認の判定 (修正前に登録した 1,3 が 1)');
check('flags', flags(), [[1, 1], [2, 0], [3, 1]]);

console.log('\n[2] 要確認だけに絞る');
check('ids', db.prepare(
  `SELECT id FROM f_mis_shipments WHERE deleted_at IS NULL AND ${NEEDS} ORDER BY id`
).all(FIELD_BUG_FIXED_AT).map((r) => r.id), [1, 3]);

console.log('\n[3] 残り件数');
check('count', db.prepare(
  `SELECT COUNT(*) AS n FROM f_mis_shipments WHERE deleted_at IS NULL AND ${NEEDS}`
).get(FIELD_BUG_FIXED_AT).n, 2);

console.log('\n[4] 訂正 → 履歴 → 確認印 で要確認が消える');
const FH = db.prepare(`
  INSERT INTO f_mis_shipment_field_history
    (mis_shipment_id, field_name, old_value, new_value, changed_by, changed_at)
  VALUES (?, ?, ?, ?, ?, ?)`);
db.transaction(() => {
  const r = db.prepare(`
    UPDATE f_mis_shipments SET process_stage = ?, updated_at = ?, updated_by = ?, version = version + 1
     WHERE id = ? AND version = ? AND deleted_at IS NULL`).run('packing', now, 'admin@b.c', 1, 0);
  check('UPDATE の件数', r.changes, 1);
  FH.run(1, 'process_stage', 'picking', 'packing', 'admin@b.c', now);
  FH.run(1, 'field_review', 'wrong_item / picking', 'wrong_item / packing', 'admin@b.c', now);
})();
check('訂正後の flags', flags(), [[1, 0], [2, 0], [3, 1]]);

console.log('\n[5] テレコの種別は変えられない (CHECK 制約)');
checkThrows('mix_up → wrong_item', () =>
  db.prepare('UPDATE f_mis_shipments SET mis_type = ? WHERE id = 3').run('wrong_item'));

console.log('\n[6] 訂正履歴は append-only');
checkThrows('UPDATE', () =>
  db.prepare('UPDATE f_mis_shipment_field_history SET new_value = ? WHERE id = 1').run('x'));
checkThrows('DELETE', () =>
  db.prepare('DELETE FROM f_mis_shipment_field_history WHERE id = 1').run());

console.log('\n[7] 知らない項目名は履歴に入れられない');
checkThrows('field_name = loss_amount_jpy', () =>
  FH.run(1, 'loss_amount_jpy', '1', '2', 'admin@b.c', now));

console.log('\n[8] 一覧の LIKE 検索で % _ がワイルドカードにならない');
db.prepare("UPDATE f_mis_shipments SET sku_snapshot = 'pr_100%off' WHERE id = 1").run();
db.prepare("UPDATE f_mis_shipments SET sku_snapshot = 'pr_200' WHERE id = 2").run();
const likeOf = (q) => '%' + String(q).replace(/[~%_]/g, (c) => '~' + c) + '%';
const search = (q) => db.prepare(
  "SELECT id FROM f_mis_shipments WHERE deleted_at IS NULL AND (sku_snapshot LIKE ? ESCAPE '~') ORDER BY id"
).all(likeOf(q)).map((r) => r.id);
check('"%" で探す', search('%'), [1]);
check('"pr_2" で探す', search('pr_2'), [2]);
check('"pr" で探す', search('pr'), [1, 2]);

db.close();
fs.rmSync(dbPath, { force: true });

console.log(failures === 0 ? '\n✅ すべて期待どおり' : `\n❌ ${failures} 件が期待と違う`);
process.exit(failures === 0 ? 0 : 1);
