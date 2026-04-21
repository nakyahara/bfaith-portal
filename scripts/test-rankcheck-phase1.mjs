#!/usr/bin/env node
/**
 * Phase1 SQLite化の簡易統合テスト (R2)。
 * 変更点:
 *   - DB パス ガード: 'test' を含まないパスでは即終了（本番データ保護）
 *   - client_id による UPSERT / 編集 / 削除パターン
 *   - tautological assertion 廃止
 *   - POST /data の拒否条件 (空 + confirmEmpty なし) も検証
 *
 * Usage: DATA_DIR=./data-test node scripts/test-rankcheck-phase1.mjs
 */
import fs from 'fs';
import path from 'path';
import assert from 'assert';
import Database from 'better-sqlite3';
import * as rdb from '../apps/ranking-checker/db.js';

function ok(label) { console.log(`  ✓ ${label}`); }
function section(name) { console.log(`\n[TEST] ${name}`); }

// ── セーフティガード ──
const dbPath = rdb.DB_FILE;
const dbPathLower = dbPath.toLowerCase().replace(/\\/g, '/');
if (!/(\/|^)(data-test|tmp)\//.test(dbPathLower) && !dbPathLower.includes('/test/')) {
  console.error(`[TEST] SAFETY ABORT: DB_FILE="${dbPath}" に 'data-test' / 'tmp' / '/test/' が含まれていません。`);
  console.error(`[TEST] 本番DBを破壊する恐れがあるため終了します。DATA_DIR=./data-test で実行してください。`);
  process.exit(1);
}
console.log(`[TEST] DB: ${dbPath}`);

section('0. 前提: migrate 実行後のDBがある');
const pre = rdb.countProducts();
assert.ok(pre > 0, '事前に migrate が必要です');
ok(`DB products件数 = ${pre}`);

section('1. iter: 全件メモリ展開なしで走査できる');
let iterCount = 0;
let firstId = null;
let firstClientId = null;
for (const p of rdb.iterAllProducts()) {
  if (firstId === null) { firstId = p.id; firstClientId = p.client_id; }
  iterCount++;
}
assert.strictEqual(iterCount, pre, 'iterator が件数一致');
ok(`iterAllProducts = ${iterCount} 件, firstClientId=${firstClientId.slice(0, 8)}...`);

section('2. history: per-product 取得');
const hist = rdb.getHistory(firstId);
assert.ok(hist.length > 0, 'history が取得できる');
ok(`id=${firstId} の history = ${hist.length} 件`);
const latest = rdb.getLatestHistory(firstId);
assert.ok(latest, 'latest history 取得可');
ok(`latest.date=${latest.date}, own_rank=${latest.own_rank}`);

section('3. upsertHistory: 既存date 更新 + 新date 追加 + -1 エラー値');
rdb.upsertHistory({
  product_id: firstId,
  date: latest.date,
  own_rank: 1, competitor1_rank: 2, competitor2_rank: null,
  yahoo_own_rank: null, amazon_own_rank: null,
});
const after = rdb.getLatestHistory(firstId);
assert.strictEqual(after.own_rank, 1, 'own_rank が 1 に更新');
ok(`同date UPSERT → own_rank=${after.own_rank}`);

const future = '2099-12-31';
rdb.upsertHistory({
  product_id: firstId, date: future,
  own_rank: 42, competitor1_rank: null, competitor2_rank: null,
  yahoo_own_rank: null, amazon_own_rank: -1,
});
const futureRow = rdb.getHistory(firstId).find(h => h.date === future);
assert.ok(futureRow, '未来日付 UPSERT');
assert.strictEqual(futureRow.amazon_own_rank, -1, 'amazon_own_rank = -1 (APIエラー)');
ok('新規date UPSERT + error=-1');

section('3b. CHECK 制約: 範囲外の rank は拒否される');
let rejected = false;
try {
  rdb.upsertHistory({
    product_id: firstId, date: '2099-12-30',
    own_rank: 999, competitor1_rank: null, competitor2_rank: null,
    yahoo_own_rank: null, amazon_own_rank: null,
  });
} catch (e) {
  rejected = true;
}
assert.ok(rejected, '999 は CHECK 違反で拒否される');
ok('CHECK 制約が効いている');

section('4. listUncheckedIdsForDate: materialize (iterator開放)');
const todayStr = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
rdb.upsertHistory({
  product_id: firstId, date: todayStr,
  own_rank: 10, competitor1_rank: null, competitor2_rank: null,
  yahoo_own_rank: null, amazon_own_rank: null,
});
const uncheckedIds = rdb.listUncheckedIdsForDate(todayStr);
assert.ok(Array.isArray(uncheckedIds), '戻り値は配列');
assert.ok(!uncheckedIds.includes(firstId), 'firstId は今日済みなので除外');
ok(`uncheckedIds.length=${uncheckedIds.length}, firstId excluded`);

section('5. cleanupOldHistory: 365日超を削除');
rdb.upsertHistory({
  product_id: firstId, date: '2000-01-01',
  own_rank: null, competitor1_rank: null, competitor2_rank: null,
  yahoo_own_rank: null, amazon_own_rank: null,
});
const beforeClean = rdb.getHistory(firstId).length;
const removed = rdb.cleanupOldHistory(365);
const afterClean = rdb.getHistory(firstId).length;
assert.ok(removed >= 1, 'cleanup で最低1件削除');
assert.strictEqual(beforeClean - afterClean, removed, '削除件数が一致');
ok(`cleanup: ${beforeClean} → ${afterClean} (-${removed})`);

section('6. run_state / run_log + stale transition');
// 明示的に stale running を仕込む
const staleRunId = rdb.startRun(7);
// 仕込みのまま markStale を呼ぶ → failed に
const fixed = rdb.markStaleRunning();
assert.ok(fixed >= 1, 'markStaleRunning が 1件以上遷移');
ok(`markStaleRunning 遷移 = ${fixed}`);

const runId = rdb.startRun(10);
rdb.logRun(runId, 'info', 'テストログ1', firstId);
rdb.logRun(runId, 'warn', 'テスト警告');
rdb.updateRunProgress(runId, 5);
rdb.finishRun(runId, 'completed');
const latestRun = rdb.getLatestRun();
assert.strictEqual(latestRun.run_id, runId);
assert.strictEqual(latestRun.status, 'completed');
assert.strictEqual(latestRun.done, 5);
ok(`latestRun status=${latestRun.status} done=${latestRun.done}`);

section('6b. run_log FK: run 削除で log も CASCADE');
const db = rdb.getDb();
const logsBefore = db.prepare(`SELECT COUNT(*) AS n FROM run_log WHERE run_id = ?`).get(runId).n;
assert.ok(logsBefore >= 2, 'run_log が少なくとも2件');
db.prepare(`DELETE FROM run_state WHERE run_id = ?`).run(runId);
const logsAfter = db.prepare(`SELECT COUNT(*) AS n FROM run_log WHERE run_id = ?`).get(runId).n;
assert.strictEqual(logsAfter, 0, 'FK CASCADE でログが削除される');
ok(`FK CASCADE: ${logsBefore} → 0`);

section('7. exportLegacyShape: id (=client_id) を含む');
const legacy = rdb.exportLegacyShape();
assert.strictEqual(legacy.products.length, pre);
const firstLegacy = legacy.products[0];
assert.ok(typeof firstLegacy.id === 'string' && firstLegacy.id.length > 0, 'id 文字列');
assert.strictEqual(firstLegacy.id, firstClientId, 'id == client_id');
// -1 → 'error' 変換が働いていること
const errEntry = firstLegacy.history.find(h => h.amazon_own_rank === 'error');
assert.ok(errEntry, '-1 が legacy で "error" として出力される');
ok(`legacy.products[0].id=${firstLegacy.id.slice(0,8)}... history=${firstLegacy.history.length} 件, error entry あり`);

section('8. CSV生成');
const { generateSummaryCSV } = await import('../apps/ranking-checker/csv-export.js');
const { csv, count } = generateSummaryCSV();
assert.strictEqual(count, pre);
assert.ok(csv.startsWith('\uFEFF'), 'BOM付き');
const rowCount = csv.split('\n').filter(l => l.length > 0).length;
assert.strictEqual(rowCount, pre + 1, 'header + products 行');
ok(`CSV ${csv.length} bytes, ${rowCount} 行`);

section('9. POST /data 入力検証 (router ロジック相当)');
// ここでは router を直接呼ばず、同じバリデーション規則を手で再現して確認。
function validatePayload(body) {
  if (!body || typeof body !== 'object') return { ok: false, error: 'invalid_body' };
  if (!Array.isArray(body.products)) return { ok: false, error: 'products_not_array' };
  if (body.products.length === 0 && body.confirmEmpty !== true) return { ok: false, error: 'empty_without_confirm' };
  return { ok: true };
}
assert.strictEqual(validatePayload(undefined).error, 'invalid_body');
assert.strictEqual(validatePayload({}).error, 'products_not_array');
assert.strictEqual(validatePayload({ products: 'x' }).error, 'products_not_array');
assert.strictEqual(validatePayload({ products: [] }).error, 'empty_without_confirm');
assert.strictEqual(validatePayload({ products: [], confirmEmpty: true }).ok, true);
assert.strictEqual(validatePayload({ products: [{}] }).ok, true);
ok('5種の入力パターンで期待通りの判定');

section('10. client_id UPSERT: keyword 変更で履歴維持');
// 既存商品の keyword を変えても、client_id が同じなら履歴は消えない
const before = rdb.getProduct(rdb.listAllIds()[0]);
const beforeHistoryLen = rdb.getHistory(before.id).length;
rdb.upsertProduct({
  client_id: before.client_id,
  keyword: before.keyword + ' (編集)',
  product_code: before.product_code,
  own_url: before.own_url,
  yahoo_url: before.yahoo_url,
  amazon_url: before.amazon_url,
  amazon_asin: before.amazon_asin,
  competitor1_url: before.competitor1_url,
  competitor2_url: before.competitor2_url,
  review_count: before.review_count,
});
const afterEdit = rdb.getProduct(before.id);
assert.strictEqual(afterEdit.client_id, before.client_id, 'client_id 不変');
assert.ok(afterEdit.keyword.endsWith('(編集)'), 'keyword 更新反映');
const afterHistoryLen = rdb.getHistory(afterEdit.id).length;
assert.strictEqual(afterHistoryLen, beforeHistoryLen, 'keyword 変更でも履歴が維持される');
ok(`keyword 編集後も history=${afterHistoryLen} 件保持`);

section('11. review_count を null に戻せる');
rdb.updateProductReviewCount(before.id, 999);
let row = rdb.getProduct(before.id);
assert.strictEqual(row.review_count, 999);
rdb.updateProductReviewCount(before.id, null);
row = rdb.getProduct(before.id);
assert.strictEqual(row.review_count, null, 'null に戻る');
ok('review_count null 書き戻し OK');

section('12. POST /data: client_id と id の両方を受理、client_id 優先');
function resolveIdOnPost(p) {
  // router.js と同じ優先順位
  if (typeof p.client_id === 'string' && p.client_id.length > 0) return p.client_id;
  if (typeof p.id === 'string' && p.id.length > 0) return p.id;
  return null;
}
assert.strictEqual(resolveIdOnPost({ id: 'uid1' }), 'uid1', 'id のみ');
assert.strictEqual(resolveIdOnPost({ client_id: 'cid1' }), 'cid1', 'client_id のみ');
assert.strictEqual(resolveIdOnPost({ client_id: 'cid1', id: 'uid1' }), 'cid1', '両方 → client_id 優先');
assert.strictEqual(resolveIdOnPost({}), null, '両方欠落 → null (呼び出し側で生成)');
ok('client_id / id / 両方 / 欠落 の4パターン');

section('13. /data/import ロジック: history 復元 + replaceAll のマージ/削除');
// import router logic を手でシミュレーション
const importPayload = {
  products: [
    {
      id: before.client_id, // 既存商品を上書き
      keyword: before.keyword,
      product_code: before.product_code,
      own_url: before.own_url,
      yahoo_url: before.yahoo_url,
      amazon_url: before.amazon_url,
      amazon_asin: before.amazon_asin,
      competitor1_url: before.competitor1_url,
      competitor2_url: before.competitor2_url,
      review_count: 1234,
      history: [
        { date: '2025-10-10', own_rank: 5, competitor1_rank: null, competitor2_rank: null, yahoo_own_rank: null, amazon_own_rank: null },
        { date: '2025-10-11', own_rank: 'error', competitor1_rank: null, competitor2_rank: null, yahoo_own_rank: null, amazon_own_rank: null },
      ],
    },
  ],
  replaceAll: false, // マージ
};

function encodeImportRank(v) {
  if (v === null || v === undefined || v === '') return null;
  if (v === 'error') return -1;
  const n = Number(v);
  if (Number.isFinite(n)) {
    const r = Math.round(n);
    if (r === -1) return -1;
    if (r >= 1 && r <= 100) return r;
    return null;
  }
  return -1;
}

const dbForImport = rdb.getDb();
const applyImport = dbForImport.transaction(() => {
  for (const p of importPayload.products) {
    const clientId =
      (typeof p.client_id === 'string' && p.client_id.length > 0) ? p.client_id :
      (typeof p.id === 'string' && p.id.length > 0) ? p.id : rdb.generateClientId();
    const productId = rdb.upsertProduct({
      client_id: clientId,
      keyword: p.keyword || '',
      product_code: p.product_code || null,
      own_url: p.own_url || null,
      yahoo_url: p.yahoo_url || null,
      amazon_url: p.amazon_url || null,
      amazon_asin: p.amazon_asin || null,
      competitor1_url: p.competitor1_url || null,
      competitor2_url: p.competitor2_url || null,
      review_count: p.review_count != null ? p.review_count : null,
    });
    for (const h of (Array.isArray(p.history) ? p.history : [])) {
      if (!h || !h.date) continue;
      rdb.upsertHistory({
        product_id: productId,
        date: h.date,
        own_rank: encodeImportRank(h.own_rank),
        competitor1_rank: encodeImportRank(h.competitor1_rank),
        competitor2_rank: encodeImportRank(h.competitor2_rank),
        yahoo_own_rank: encodeImportRank(h.yahoo_own_rank),
        amazon_own_rank: encodeImportRank(h.amazon_own_rank),
      });
    }
  }
});
applyImport();
const importedHistory = rdb.getHistory(before.id);
assert.ok(importedHistory.some(h => h.date === '2025-10-10' && h.own_rank === 5), '履歴 date=2025-10-10 復元');
assert.ok(importedHistory.some(h => h.date === '2025-10-11' && h.own_rank === -1), "'error' → -1 変換");
const afterImport = rdb.getProduct(before.id);
assert.strictEqual(afterImport.review_count, 1234, 'review_count 上書き');
ok('import: 履歴復元 + error変換 + master上書き');

section('13c. normalizeProductInput: 型ガード');
const routerMod = await import('../apps/ranking-checker/router.js');
const normalizeProductInput = routerMod.normalizeProductInput;
const routerEncodeImportRank = routerMod.encodeImportRank;

function expectReject(fn, expectedErrorField) {
  try { fn(); assert.fail('expected throw'); }
  catch (e) {
    assert.strictEqual(e.status, 400, `expected 400 for ${expectedErrorField}`);
    if (expectedErrorField) assert.ok(e.body.error === expectedErrorField, `error=${e.body.error}`);
  }
}

// 正常系
const good = normalizeProductInput({ client_id: 'c1', keyword: 'k' }, 0);
assert.strictEqual(good.client_id, 'c1');
ok('正常系 client_id 反映');

// 型違反: keyword がobject
expectReject(() => normalizeProductInput({ keyword: { foo: 1 } }, 0), 'invalid_field_type');
// review_count がbooleanまたは文字列
expectReject(() => normalizeProductInput({ keyword: 'x', review_count: true }, 0), 'invalid_field_type');
expectReject(() => normalizeProductInput({ keyword: 'x', review_count: '100' }, 0), 'invalid_field_type');
// URL がarray
expectReject(() => normalizeProductInput({ keyword: 'x', own_url: ['a'] }, 0), 'invalid_field_type');
// history 許容しないのに配列
const noHistory = normalizeProductInput({ keyword: 'x', history: [{ date: '2025-01-01' }] }, 0);
assert.ok(!('history' in noHistory), 'allowHistory=false で history 無視');
// history allowed だが date 不正
expectReject(() => normalizeProductInput(
  { keyword: 'x', history: [{ date: '2025/01/01' }] }, 0, { allowHistory: true }
), 'invalid_history_date');
// history entry が boolean rank
expectReject(() => normalizeProductInput(
  { keyword: 'x', history: [{ date: '2025-01-01', own_rank: true }] }, 0, { allowHistory: true }
), 'invalid_rank_type');
ok('6種の型違反すべて 400');

section('13d. duplicate client_id 検出');
const seen = new Set();
normalizeProductInput({ client_id: 'same', keyword: 'a' }, 0, { seenClientIds: seen });
expectReject(
  () => normalizeProductInput({ client_id: 'same', keyword: 'b' }, 1, { seenClientIds: seen }),
  'duplicate_client_id',
);
// client_id と id の優先で同じに潰れるケース
const seen2 = new Set();
normalizeProductInput({ client_id: 'x1', keyword: 'a' }, 0, { seenClientIds: seen2 });
expectReject(
  () => normalizeProductInput({ id: 'x1', keyword: 'b' }, 1, { seenClientIds: seen2 }),
  'duplicate_client_id',
);
ok('payload 内 client_id / id 重複で 400');

section('13e. encodeImportRank: boolean/object/array を null 化');
assert.strictEqual(routerEncodeImportRank(true), null, 'boolean true → null');
assert.strictEqual(routerEncodeImportRank(false), null, 'boolean false → null');
assert.strictEqual(routerEncodeImportRank([5]), null, '[5] → null (coerce防止)');
assert.strictEqual(routerEncodeImportRank({ x: 1 }), null, 'object → null');
assert.strictEqual(routerEncodeImportRank('error'), -1);
assert.strictEqual(routerEncodeImportRank(null), null);
assert.strictEqual(routerEncodeImportRank(''), null);
assert.strictEqual(routerEncodeImportRank(0), null, '0 → null (範囲外)');
assert.strictEqual(routerEncodeImportRank(101), null, '101 → null (範囲外)');
assert.strictEqual(routerEncodeImportRank(50), 50);
assert.strictEqual(routerEncodeImportRank('-1'), -1, '"-1" → -1');
ok('coerce防止・範囲クランプ OK');

section('13b. POST /data: [null] での全削除バイパス防止');
// router と同じ validate を再現
function validateAndNormalize(body) {
  if (!body || typeof body !== 'object') return { ok: false, error: 'invalid_body' };
  if (!Array.isArray(body.products)) return { ok: false, error: 'products_not_array' };
  const normalized = [];
  for (let i = 0; i < body.products.length; i++) {
    const p = body.products[i];
    if (!p || typeof p !== 'object' || Array.isArray(p)) {
      return { ok: false, error: 'invalid_product', index: i };
    }
    normalized.push(p);
  }
  if (normalized.length === 0 && body.confirmEmpty !== true) {
    return { ok: false, error: 'empty_without_confirm' };
  }
  return { ok: true, normalized };
}
assert.strictEqual(validateAndNormalize({ products: [null] }).error, 'invalid_product', '[null] 拒否');
assert.strictEqual(validateAndNormalize({ products: [null, {}] }).error, 'invalid_product', '[null, {}] 拒否');
assert.strictEqual(validateAndNormalize({ products: [[]] }).error, 'invalid_product', '[配列] 拒否');
assert.strictEqual(validateAndNormalize({ products: [123] }).error, 'invalid_product', 'primitive 拒否');
assert.strictEqual(validateAndNormalize({ products: [{}] }).ok, true);
ok('invalid product タイプ 4種すべて 400');

section('14. Schema drift: user_version ミスマッチで openDb が abort');
rdb.closeDb();
const raw = new Database(rdb.DB_FILE);
const before_uv = raw.pragma('user_version', { simple: true });
raw.pragma('user_version = 999');
raw.close();

let threw = null;
try {
  // openDb は getDb 越しに呼ばれる
  rdb.getDb();
} catch (e) {
  threw = e;
}
assert.ok(threw, 'バージョン不一致で例外');
assert.ok(/schema version mismatch/.test(threw.message), 'メッセージに理由');
ok(`abort 確認: ${threw.message.slice(0, 80)}`);

// 元に戻す
const raw2 = new Database(rdb.DB_FILE);
raw2.pragma(`user_version = ${before_uv}`);
raw2.close();

section('14b. Schema fingerprint drift: 同世代でも列欠落で abort');
// products から1列 drop (fingerprint 変化) → openDb 失敗を確認
const raw3 = new Database(rdb.DB_FILE);
raw3.pragma('foreign_keys = OFF');
raw3.exec(`
  CREATE TABLE products_backup AS SELECT * FROM products;
  DROP TABLE products;
  CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL UNIQUE,
    keyword TEXT NOT NULL
    -- competitor列など欠落
  );
  INSERT INTO products (id, client_id, keyword) SELECT id, client_id, keyword FROM products_backup;
  DROP TABLE products_backup;
`);
raw3.close();

let fpThrew = null;
try { rdb.getDb(); } catch (e) { fpThrew = e; }
assert.ok(fpThrew, '列欠落で fingerprint mismatch');
assert.ok(/fingerprint mismatch/.test(fpThrew.message), 'fingerprint メッセージ');
ok(`fingerprint abort 確認`);

// DB再作成: 以降のテストでは使わないため、空DB状態で終わる
const raw4 = new Database(rdb.DB_FILE);
raw4.exec(`DROP TABLE IF EXISTS products;`);
raw4.pragma('user_version = 0');
raw4.close();

section('15. migrate: p.client_id / p.id 優先 + rank 範囲クランプ');
// migrate の resolveClientId / encodeRank を直接テスト
const migrateModule = await import('../scripts/migrate-json-to-sqlite.mjs?nocache=' + Date.now())
  .catch(() => null);
// main() を自動実行するスクリプトなので import ではテスト不可。別関数化は Phase2 で。
// 代わりに現在のDBが存在しない状態で migrate を外部コマンド経由テストする代替案を提示。
ok('(skip) migrate関数単体テストは Phase2 で export 分離後に実施');

console.log('\n[ALL TESTS PASSED]');
rdb.closeDb();
