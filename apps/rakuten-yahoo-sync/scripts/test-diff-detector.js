/**
 * E-7-b smoke runner: migration 006 + lib/diff-detector の動作確認 (実 楽天/Yahoo API は叩かない、 mock のみ)。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-diff-detector.js
 *
 * 検証項目:
 *   - migration 006 schema (migration_candidates テーブル / index / CHECK 制約)
 *   - detectMigrationCandidates:
 *       (1) baseline 未確立 → 412 fail-closed (assertYahooBaselineComplete 経由)
 *       (2) 通常 run: 新規候補 upsert + sync_runs success
 *       (3) overlap=0 → fail-closed (allowZeroOverlap=false default)
 *       (4) overlap=0 + allowZeroOverlap=true → 通る
 *       (5) overlap_rate 急減 (前回比 50% 以下) → fail-closed
 *       (6) env RYS_MIN_OVERLAP_RATE 下限 → fail-closed
 *       (7) candidate が Yahoo に出た → resolved + resolved_at
 *       (8) candidate が楽天から消えた → missing_rakuten_count++ + 閾値で stale flip
 *       (9) stale だった row が再度候補化 → candidate に戻す + missing=0
 *       (10) resolved row は upsert で resolved 維持
 *       (11) dry-run: writes なし、 stats のみ
 *       (12) lease lost detection (R2 High-1 と同等)
 *       (13) rakutenItems が Map / object / 非対応型 の正規化
 *       (14) sync_runs.status='running' UNIQUE INDEX (同名同時 1 件)
 */

import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { syncYahooBaseline } from '../lib/yahoo-store-sync.js';
import {
  detectMigrationCandidates,
  DEFAULT_STALE_MISSING_THRESHOLD,
  normalizeRakutenItems,
} from '../lib/diff-detector.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrations');

let failed = 0;
function ok(name) { console.log(`OK   ${name}`); }
function fail(name, msg) { failed += 1; console.error(`FAIL ${name}: ${msg}`); }
function expect(name, cond, msg) { if (cond) ok(name); else fail(name, msg); }

function freshDb() {
  const db = new Database(':memory:');
  const files = fs.readdirSync(MIGRATIONS_DIR).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) db.exec(fs.readFileSync(path.join(MIGRATIONS_DIR, f), 'utf8'));
  return db;
}

// Yahoo baseline を確立しておく helper (mock 5 件/query)
async function establishBaseline(db, perQuery = 5) {
  const mockApi = async ({ query, offset, results }) => {
    const items = [];
    const start = offset + 1;
    const end = Math.min(offset + results, perQuery);
    for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
    return { ok: true, items, totalResultsAvailable: perQuery, totalResultsReturned: items.length, firstResultPosition: start };
  };
  return syncYahooBaseline({ db, deps: { callApi: mockApi }, triggeredBy: 'cron' });
}

// 楽天 itemNumber → manageNumber Map を生成
function makeRakutenMap(itemCodes) {
  const m = new Map();
  for (const code of itemCodes) m.set(code, 'N' + code);
  return m;
}

// ──── テスト本体 ────

async function testMigration() {
  const db = freshDb();
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map((r) => r.name);
  expect('migration: migration_candidates created', tables.includes('migration_candidates'), `tables=${tables.join(',')}`);

  // status CHECK
  try {
    db.prepare("INSERT INTO migration_candidates (item_code, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status) VALUES ('x', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'bogus')").run();
    fail('migration: status CHECK', 'bogus accepted');
  } catch (e) {
    expect('migration: status CHECK rejects bogus', /CHECK/.test(e.message), e.message);
  }

  // missing_rakuten_count negative
  try {
    db.prepare("INSERT INTO migration_candidates (item_code, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, missing_rakuten_count, status) VALUES ('y', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', -1, 'candidate')").run();
    fail('migration: missing_rakuten_count >=0', '-1 accepted');
  } catch (e) {
    expect('migration: missing_rakuten_count CHECK', /CHECK/.test(e.message), e.message);
  }
}

async function testNormalize() {
  expect('normalize: Map passthrough', normalizeRakutenItems(new Map([['a', 'N1']])) instanceof Map, 'expected Map');
  expect('normalize: object → Map', normalizeRakutenItems({ a: 'N1', b: 'N2' }).get('b') === 'N2', 'failed');
  try { normalizeRakutenItems('foo'); fail('normalize: string rejected', 'accepted'); }
  catch (e) { expect('normalize: string rejected', /must be Map/.test(e.message), e.message); }
  try { normalizeRakutenItems(null); fail('normalize: null rejected', 'accepted'); }
  catch (e) { expect('normalize: null rejected', /must be Map/.test(e.message), e.message); }
}

async function testDetector() {
  // (1) baseline 未確立 → fail-closed
  {
    const db = freshDb();
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1']) });
      fail('det (1): baseline not established should fail', 'no throw');
    } catch (e) {
      expect('det (1): baseline guard 412', e.statusCode === 412, `statusCode=${e.statusCode}`);
    }
  }

  // (2) 通常 run: 楽天 6 件、 Yahoo 36*5=180 件 (a-1..a-5 等)、 楽天 a-1 は Yahoo に居る (overlap=1)、 残 5 件は新規候補
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    const rakuten = makeRakutenMap(['a-1', 'new-1', 'new-2', 'new-3', 'new-4', 'new-5']);
    const r = await detectMigrationCandidates({ db, rakutenItems: rakuten, triggeredBy: 'cron' });
    expect('det (2): rakutenTotal=6', r.rakutenTotal === 6, `got ${r.rakutenTotal}`);
    expect('det (2): yahooTotal=180', r.yahooTotal === 180, `got ${r.yahooTotal}`);
    expect('det (2): overlap=1', r.overlap === 1, `got ${r.overlap}`);
    expect('det (2): candidatesDetected=5', r.candidatesDetected === 5, `got ${r.candidatesDetected}`);
    expect('det (2): newlyDetected=5', r.newlyDetected === 5, `got ${r.newlyDetected}`);
    const rows = db.prepare('SELECT COUNT(*) AS c FROM migration_candidates').get().c;
    expect('det (2): migration_candidates 5 rows', rows === 5, `got ${rows}`);
    const sync = db.prepare("SELECT status, rakuten_total, yahoo_total, overlap, overlap_rate, candidates_new FROM sync_runs WHERE sync_name='rakuten_diff' ORDER BY id DESC LIMIT 1").get();
    expect('det (2): sync_runs success + stats recorded', sync && sync.status === 'success' && sync.candidates_new === 5, JSON.stringify(sync));
  }

  // (3) overlap=0 → fail-closed (allowZeroOverlap=false)
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 楽天は Yahoo に 1 件もマッチしないコード
    const rakuten = makeRakutenMap(['ne-1', 'ne-2']);
    try {
      await detectMigrationCandidates({ db, rakutenItems: rakuten });
      fail('det (3): overlap=0 should throw', 'no throw');
    } catch (e) {
      expect('det (3): overlap=0 fail-closed', /overlap=0/.test(e.message), e.message);
    }
    // sync_runs は failed として記録
    const sync = db.prepare("SELECT status FROM sync_runs WHERE sync_name='rakuten_diff' ORDER BY id DESC LIMIT 1").get();
    expect('det (3): sync_runs marked failed', sync && sync.status === 'failed', JSON.stringify(sync));
  }

  // (4) overlap=0 + allowZeroOverlap=true → 通る
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    const rakuten = makeRakutenMap(['fresh-1']);
    const r = await detectMigrationCandidates({ db, rakutenItems: rakuten, allowZeroOverlap: true });
    expect('det (4): allowZeroOverlap=true bypass', r.overlap === 0 && r.newlyDetected === 1, JSON.stringify(r));
  }

  // (5) overlap_rate 急減 (前回成功比 50% 以下) → fail-closed
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 1 回目: overlap 高い (a-1..a-5 全部 yahoo に居る)
    const rakuten1 = makeRakutenMap(['a-1', 'a-2', 'a-3', 'a-4', 'a-5', 'new-1', 'new-2']);
    await detectMigrationCandidates({ db, rakutenItems: rakuten1 });
    // 2 回目: overlap が急減 (a-1 だけ overlap、 残 6 件は new) → overlap_rate 1/7=0.143、 前回 5/7=0.714 → drop ratio = 0.2 < 0.5 → fail
    const rakuten2 = makeRakutenMap(['a-1', 'big-1', 'big-2', 'big-3', 'big-4', 'big-5', 'big-6']);
    try {
      await detectMigrationCandidates({ db, rakutenItems: rakuten2 });
      fail('det (5): overlap rate drop should fail', 'no throw');
    } catch (e) {
      expect('det (5): overlap_rate drop fail-closed', /dropped/.test(e.message), e.message);
    }
  }

  // (6) env RYS_MIN_OVERLAP_RATE = 0.9 → 通常 overlap=1/6=0.17 で fail
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    process.env.RYS_MIN_OVERLAP_RATE = '0.9';
    try {
      const rakuten = makeRakutenMap(['a-1', 'new-1', 'new-2', 'new-3', 'new-4', 'new-5']);
      await detectMigrationCandidates({ db, rakutenItems: rakuten });
      fail('det (6): env min should fail', 'no throw');
    } catch (e) {
      expect('det (6): env RYS_MIN_OVERLAP_RATE fail-closed', /env RYS_MIN_OVERLAP_RATE/.test(e.message), e.message);
    }
    delete process.env.RYS_MIN_OVERLAP_RATE;
  }

  // (7) candidate が Yahoo に出た → resolved
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 1 回目: new-1 を candidate に登録
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'new-1']) });
    const before = db.prepare("SELECT status FROM migration_candidates WHERE item_code='new-1'").get();
    expect('det (7a): pre - new-1 is candidate', before.status === 'candidate', JSON.stringify(before));
    // 手動で yahoo_registered_items に new-1 を追加 (= Yahoo に出た)
    db.prepare("INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source) VALUES ('new-1', 'new-1', 0, ?, 'yahoo_existing')").run(new Date().toISOString());
    // 2 回目: 再 detect、 new-1 は yahoo 側にあるので resolved に flip
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'new-1']) });
    const after = db.prepare("SELECT status, resolved_at FROM migration_candidates WHERE item_code='new-1'").get();
    expect('det (7b): post - new-1 is resolved', after.status === 'resolved' && after.resolved_at !== null, JSON.stringify(after));
  }

  // (8) candidate が楽天から消えた → missing_rakuten_count++、 閾値で stale
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 初回: new-1 を candidate に
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'new-1']) });
    // 2 回目: new-1 楽天から消失、 missing_rakuten_count=1、 まだ stale じゃない (threshold=2)
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1']), allowZeroOverlap: false });
    const mid = db.prepare("SELECT status, missing_rakuten_count FROM migration_candidates WHERE item_code='new-1'").get();
    expect('det (8a): missing count=1, still candidate', mid.status === 'candidate' && mid.missing_rakuten_count === 1, JSON.stringify(mid));
    // 3 回目: new-1 まだ楽天から消失、 missing=2 → stale flip
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1']) });
    const after = db.prepare("SELECT status, missing_rakuten_count, stale_at FROM migration_candidates WHERE item_code='new-1'").get();
    expect('det (8b): missing count=2 → stale', after.status === 'stale' && after.stale_at !== null, JSON.stringify(after));
  }

  // (9) stale だった row が再度候補化 → candidate に戻る + missing=0
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 初回 + 楽天消失 x2 で stale
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'come-back']) });
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1']), staleMissingThreshold: 1 });
    const stale = db.prepare("SELECT status FROM migration_candidates WHERE item_code='come-back'").get();
    expect('det (9a): pre - come-back is stale', stale.status === 'stale', JSON.stringify(stale));
    // 楽天に再登場
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'come-back']) });
    const revived = db.prepare("SELECT status, missing_rakuten_count, stale_at FROM migration_candidates WHERE item_code='come-back'").get();
    expect('det (9b): post - come-back is candidate, missing reset', revived.status === 'candidate' && revived.missing_rakuten_count === 0 && revived.stale_at === null, JSON.stringify(revived));
  }

  // (10) resolved row は upsert で resolved 維持 (楽天に再出現してても)
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'res-1']) });
    db.prepare("INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source) VALUES ('res-1', 'res-1', 0, ?, 'yahoo_existing')").run(new Date().toISOString());
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'res-1']) });
    const after1 = db.prepare("SELECT status FROM migration_candidates WHERE item_code='res-1'").get();
    expect('det (10a): res-1 is resolved', after1.status === 'resolved', JSON.stringify(after1));
    // Yahoo から消したと仮定して再 run、 楽天には残る (= 楽天有 + Yahoo無 の候補に再該当)、
    // 仕様: resolved は維持される (revoke しすぎないため)
    db.prepare("DELETE FROM yahoo_registered_items WHERE item_code='res-1'").run();
    // base 取り直し (yahoo_registered_items 変更後)
    await establishBaseline(db, 5);
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'res-1']) });
    const after2 = db.prepare("SELECT status FROM migration_candidates WHERE item_code='res-1'").get();
    expect('det (10b): resolved is preserved even if rakuten-only again', after2.status === 'resolved', JSON.stringify(after2));
  }

  // (11) dry-run: writes なし、 stats のみ
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    const rakuten = makeRakutenMap(['a-1', 'dry-1', 'dry-2']);
    const r = await detectMigrationCandidates({ db, rakutenItems: rakuten, dryRun: true });
    expect('det (11a): dry-run stats include candidatesDetected', r.candidatesDetected === 2 && r.dryRun === true, JSON.stringify(r));
    const candidates = db.prepare('SELECT COUNT(*) AS c FROM migration_candidates').get().c;
    expect('det (11b): dry-run does NOT write migration_candidates', candidates === 0, `candidates=${candidates}`);
    const sync = db.prepare("SELECT status FROM sync_runs WHERE sync_name='rakuten_diff' ORDER BY id DESC LIMIT 1").get();
    expect('det (11c): dry-run finishes sync_run as success', sync && sync.status === 'success', JSON.stringify(sync));
  }

  // (12) lease lost detection
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 同名 running を pre-insert (lease 有効) → 新 run は UNIQUE INDEX で拒否
    const futureIso = new Date(Date.now() + 30 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at) VALUES ('rakuten_diff', ?, 'running', 'cron', ?)").run(new Date().toISOString(), futureIso);
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1']) });
      fail('det (12): UNIQUE running guard should block', 'no throw');
    } catch (e) {
      expect('det (12): valid lease blocks new diff run', /UNIQUE/.test(e.message), e.message);
    }
  }

  // (13) sync_runs UNIQUE INDEX: 同名同時 1 件 (12 で確認済、 追加で別名は OK)
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // yahoo_baseline running を入れた状態でも rakuten_diff は通る
    const futureIso = new Date(Date.now() + 30 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at) VALUES ('yahoo_baseline', ?, 'running', 'cron', ?)").run(new Date().toISOString(), futureIso);
    const r = await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'new-x']) });
    expect('det (13): yahoo_baseline running does NOT block rakuten_diff', r.newlyDetected === 1, JSON.stringify(r));
  }

  // (14) Codex R1 High-1: baseline 確立後に incomplete yahoo_baseline success が走った状態で diff は fail-closed
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 直後に 「stale_rows > 0 になる incomplete run」 をシミュレート (1 query だけ items 減らす)
    const incompleteMock = async ({ query, offset, results }) => {
      const total = (query === 'a') ? 4 : 5;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, total);
      for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
      return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
    };
    await syncYahooBaseline({ db, deps: { callApi: incompleteMock }, triggeredBy: 'cron' });
    // baseline_state は前回のまま (incomplete だから establish しない)、
    // ただし最新 success の run_id は state.baseline_run_id と異なる → fail-closed
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'fresh-1']) });
      fail('det (14): post-incomplete baseline guard should fail', 'no throw');
    } catch (e) {
      expect('det (14): baseline_state vs latest success mismatch → 412', e.statusCode === 412 && /baseline_state/.test(e.message), `statusCode=${e.statusCode} msg=${e.message}`);
    }
  }

  // (15) Codex R1 High-2: dry-run の success は overlap 急減判定を汚染しない
  // 注: makeRakutenMap で渡したコードのうち yahoo set ('a-1'..'a-5' 等 36*5=180 件) と一致しない物は 候補。
  //      'mig-N' は m query が 'm-1'..'m-5' しか返さないので yahoo にいない (= 候補)。
  // 設計: 通常 run (overlap_rate=0.5) → dry-run (overlap_rate=1.0) → 通常 run (overlap_rate=0.4)
  //   汚染なし: 3 番目は通常 run (0.5) を参照、 ratio = 0.4/0.5 = 0.8 OK pass
  //   汚染あり: 3 番目が dry-run (1.0) を参照、 ratio = 0.4/1.0 = 0.4 < 0.5 FAIL
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 1. 通常 run: rakuten 6 件、 overlap 3 (a-1/a-2/a-3 yahoo に居る) → rate 0.5
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'a-2', 'a-3', 'mig-1', 'mig-2', 'mig-3']) });
    // 2. dry-run: rakuten 5 件、 overlap 5 → rate 1.0 (汚染源として最大値)
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'a-2', 'a-3', 'a-4', 'a-5']), dryRun: true });
    // 3. 通常 run: rakuten 5 件、 overlap 2 (a-1/a-2 のみ) → rate 0.4
    //    通常 1 (0.5) を参照: 0.4/0.5 = 0.8 OK
    //    汚染して dry-run (1.0) を参照すると: 0.4/1.0 = 0.4 FAIL
    const r = await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'a-2', 'mig-4', 'mig-5', 'mig-6']) });
    expect('det (15): dry-run does NOT pollute overlap drop check', r.newlyDetected === 3 && Math.abs(r.overlapRate - 0.4) < 0.01, JSON.stringify(r));
    // sync_runs に dry_run 列が記録されてる
    const dryRunRows = db.prepare("SELECT COUNT(*) AS c FROM sync_runs WHERE dry_run = 1").get().c;
    expect('det (15b): dry_run flag recorded', dryRunRows >= 1, `count=${dryRunRows}`);
  }

  // (16) Codex R1 Medium-1: RYS_MIN_OVERLAP_RATE strict parse
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    process.env.RYS_MIN_OVERLAP_RATE = '0.5abc';
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'x-1']) });
      fail('det (16a): invalid env value should fail', 'no throw');
    } catch (e) {
      expect('det (16a): RYS_MIN_OVERLAP_RATE invalid string fails', /invalid value/.test(e.message), e.message);
    }
    process.env.RYS_MIN_OVERLAP_RATE = '1.5';
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'x-1']) });
      fail('det (16b): out of range value should fail', 'no throw');
    } catch (e) {
      expect('det (16b): RYS_MIN_OVERLAP_RATE >1 fails', /invalid value/.test(e.message), e.message);
    }
    process.env.RYS_MIN_OVERLAP_RATE = '-0.1';
    try {
      await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'x-1']) });
      fail('det (16c): negative value should fail', 'no throw');
    } catch (e) {
      expect('det (16c): RYS_MIN_OVERLAP_RATE <0 fails', /invalid value/.test(e.message), e.message);
    }
    delete process.env.RYS_MIN_OVERLAP_RATE;
  }

  // (18) Codex R2 High: baseline guard と Yahoo set 読み込みの間に別の syncYahooBaseline が走った race
  //   diff の最中に incomplete baseline success が走ったら、 SELECT 直前の再 guard で 412 fail-closed する。
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 楽天 fetch を 「遅延」 させるシミュレーション: rakutenItems を遅延 Promise で渡す
    // → その間に別の incomplete baseline run を走らせる → assertYahooBaselineFixed が 2 回目で fail
    let delayResolver;
    const delayedRakuten = new Promise((resolve) => { delayResolver = resolve; });
    const detectionPromise = (async () => {
      const rakuten = await delayedRakuten;
      // detectMigrationCandidates が rakutenItems を Map で受け取れるようにする
      return detectMigrationCandidates({ db, rakutenItems: rakuten });
    })();
    // 別 process が incomplete sync を走らせる
    const incompleteMock = async ({ query, offset, results }) => {
      const total = (query === 'a') ? 4 : 5;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, total);
      for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
      return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
    };
    await syncYahooBaseline({ db, deps: { callApi: incompleteMock }, triggeredBy: 'cron' });
    // 楽天 fetch を解放
    delayResolver(makeRakutenMap(['a-1', 'fresh-1']));
    try {
      await detectionPromise;
      fail('det (18): mid-flight incomplete baseline should fail diff', 'no throw');
    } catch (e) {
      expect('det (18): mid-flight baseline race → 412 fail-closed', e.statusCode === 412, `statusCode=${e.statusCode} msg=${e.message?.slice(0,100)}`);
    }
  }

  // (17) Codex R1 Medium-2: resolved row は upsert で一切触らない (status だけでなく他列も)
  {
    const db = freshDb();
    await establishBaseline(db, 5);
    // 初回: keep-resolved を candidate に登録
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'keep-resolved']) });
    // Yahoo に出した状態をシミュレート → resolved
    db.prepare("INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source) VALUES ('keep-resolved', 'keep-resolved', 0, ?, 'yahoo_existing')").run(new Date().toISOString());
    await detectMigrationCandidates({ db, rakutenItems: makeRakutenMap(['a-1', 'keep-resolved']) });
    const after1 = db.prepare("SELECT status, resolved_at, last_detected_at, rakuten_manage_number FROM migration_candidates WHERE item_code='keep-resolved'").get();
    expect('det (17a): resolved + resolved_at + manage_number recorded', after1.status === 'resolved' && after1.resolved_at !== null && after1.rakuten_manage_number === 'Nkeep-resolved', JSON.stringify(after1));
    const lastDetectedSnapshot = after1.last_detected_at;
    // Yahoo から消した + 再 baseline 確立 + manage_number 違う rakuten で再 diff
    db.prepare("DELETE FROM yahoo_registered_items WHERE item_code='keep-resolved'").run();
    await establishBaseline(db, 5);
    await new Promise((resolve) => setTimeout(resolve, 30));
    // 楽天は keep-resolved の manage_number を変える、 a-1, a-2, a-3 で overlap 確保 (50%)
    const rakuten2 = new Map([['a-1', 'Na-1'], ['a-2', 'Na-2'], ['a-3', 'Na-3'], ['keep-resolved', 'N-different-manage-no']]);
    await detectMigrationCandidates({ db, rakutenItems: rakuten2, allowZeroOverlap: false });
    const after2 = db.prepare("SELECT status, resolved_at, last_detected_at, rakuten_manage_number FROM migration_candidates WHERE item_code='keep-resolved'").get();
    expect('det (17b): resolved row status preserved', after2.status === 'resolved', JSON.stringify(after2));
    expect('det (17b): resolved row manage_number not overwritten', after2.rakuten_manage_number === 'Nkeep-resolved', `expected=Nkeep-resolved, got=${after2.rakuten_manage_number}`);
    expect('det (17b): resolved row last_detected_at not overwritten', after2.last_detected_at === lastDetectedSnapshot, `expected=${lastDetectedSnapshot}, got=${after2.last_detected_at}`);
  }
}

// ──── runner ────
(async () => {
  console.log('=== Phase E-7-b smoke runner ===');
  console.log('--- migration ---');
  await testMigration();
  console.log('--- normalize ---');
  await testNormalize();
  console.log('--- detectMigrationCandidates ---');
  await testDetector();
  console.log('================================');
  if (failed > 0) {
    console.error(`FAILED: ${failed} check(s) did not pass`);
    process.exit(1);
  } else {
    console.log('ALL CHECKS PASSED');
  }
})().catch((e) => {
  console.error('unexpected runner error:', e);
  process.exit(2);
});
