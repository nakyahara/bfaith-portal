/**
 * readiness-sweep (再設計 R8) の test。 evaluateItemForPublish / fetchAllItemCodes は stub。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/readiness-sweep.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { sweepReadiness, pickSweepTargets } from './readiness-sweep.js';

function setupDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE migration_candidates (
      item_code TEXT PRIMARY KEY,
      rakuten_manage_number TEXT,
      status TEXT NOT NULL DEFAULT 'candidate'
    );
    CREATE TABLE migration_excluded (
      item_code TEXT NOT NULL,
      restored_at TEXT
    );
    CREATE TABLE yahoo_registered_items (
      item_code TEXT PRIMARY KEY
    );
    CREATE TABLE publish_idempotency (
      item_code TEXT NOT NULL,
      status TEXT NOT NULL
    );
  `);
  return db;
}

function seed(db, rows) {
  const ins = db.prepare('INSERT INTO migration_candidates (item_code, rakuten_manage_number, status) VALUES (?, ?, ?)');
  for (const [code, mn, status] of rows) ins.run(code, mn, status || 'candidate');
}

test('対象抽出: candidate のみ (excluded / 登録済 / publish成功 は除外)', () => {
  const db = setupDb();
  seed(db, [['a', 'a'], ['b', 'b'], ['c', 'c'], ['d', 'd'], ['e', 'e', 'stale']]);
  db.prepare("INSERT INTO migration_excluded (item_code, restored_at) VALUES ('b', NULL)").run();
  db.prepare("INSERT INTO yahoo_registered_items (item_code) VALUES ('c')").run();
  db.prepare("INSERT INTO publish_idempotency (item_code, status) VALUES ('d', 'success')").run();
  const targets = pickSweepTargets(db);
  assert.deepEqual(targets.map((t) => t.item_code), ['a']);
});

test('除外の復元済 (restored_at あり) は対象に戻る', () => {
  const db = setupDb();
  seed(db, [['a', 'a']]);
  db.prepare("INSERT INTO migration_excluded (item_code, restored_at) VALUES ('a', '2026-07-01')").run();
  assert.equal(pickSweepTargets(db).length, 1);
});

test('sweep: ok/blocked を集計し dryRun=false で evaluate を呼ぶ', async () => {
  const db = setupDb();
  seed(db, [['a', 'mn-a'], ['b', 'mn-b'], ['c', 'mn-c']]);
  const calls = [];
  const r = await sweepReadiness({
    db,
    deps: {
      sleepMs: 0,
      evaluateItemForPublish: async ({ itemCode, manageNumber, dryRun }) => {
        calls.push({ itemCode, manageNumber, dryRun });
        return { status: itemCode === 'b' ? 'blocked' : 'ok' };
      },
    },
  });
  assert.equal(r.picked, 3);
  assert.equal(r.evaluated, 3);
  assert.equal(r.okCount, 2);
  assert.equal(r.blockedCount, 1);
  assert.equal(r.errors, 0);
  assert.ok(calls.every((c) => c.dryRun === false));
  assert.equal(calls.find((c) => c.itemCode === 'a').manageNumber, 'mn-a');
});

test('manageNumber 無しは all-codes mapping で解決 (mapping は 1 回だけ取得)', async () => {
  const db = setupDb();
  seed(db, [['x1', null], ['x2', null]]);
  let mappingCalls = 0;
  const seen = [];
  await sweepReadiness({
    db,
    deps: {
      sleepMs: 0,
      fetchAllItemCodes: async () => { mappingCalls++; return { x1: 'mn-x1', x2: 'mn-x2' }; },
      evaluateItemForPublish: async ({ itemCode, manageNumber }) => { seen.push([itemCode, manageNumber]); return { status: 'ok' }; },
    },
  });
  assert.equal(mappingCalls, 1);
  assert.deepEqual(seen, [['x1', 'mn-x1'], ['x2', 'mn-x2']]);
});

test('個別エラーは飲んで続行 (少数なら成功扱い、errorSamples に記録)', async () => {
  const db = setupDb();
  seed(db, [['a', 'a'], ['b', 'b'], ['c', 'c']]);
  const r = await sweepReadiness({
    db,
    deps: {
      sleepMs: 0,
      evaluateItemForPublish: async ({ itemCode }) => {
        if (itemCode === 'b') throw new Error('一時的なRMSエラー');
        return { status: 'ok' };
      },
    },
  });
  assert.equal(r.evaluated, 2);
  assert.equal(r.errors, 1);
  assert.match(r.errorSamples[0], /^b: 一時的なRMSエラー/);
});

test('全滅は throw (fail-closed)', async () => {
  const db = setupDb();
  seed(db, [['a', 'a'], ['b', 'b']]);
  await assert.rejects(
    () => sweepReadiness({
      db,
      deps: { sleepMs: 0, evaluateItemForPublish: async () => { throw new Error('proxy down'); } },
    }),
    /1件も評価できませんでした/,
  );
});

test('対象ゼロは何もせず成功', async () => {
  const db = setupDb();
  const r = await sweepReadiness({ db, deps: { sleepMs: 0 } });
  assert.equal(r.picked, 0);
  assert.equal(r.evaluated, 0);
});

test('limit が効く', () => {
  const db = setupDb();
  seed(db, [['a', 'a'], ['b', 'b'], ['c', 'c']]);
  assert.equal(pickSweepTargets(db, { limit: 2 }).length, 2);
});
