/**
 * inbound-baseline.js — 毎朝の FBA レポート取得の前後に「納品プラン・出荷便の状態」を記録して照合する (FBA 補充 B1。2026-09-26)
 *
 * miniPC の常駐サーバ (fba-service.js の /snapshot-reports ジョブ) の中で動く。
 *   S0 = レポートを頼む直前 / S1 = レポートを保存した直後 に inbound-snapshot.js でスナップショットを取り、
 *   ① S0→S1 で変わったプラン・便 (レポート作成中に動いた) ② S1 とレポートの照合 (準備中・輸送中+受領中) を
 *   その日の「基準」として保存する。
 * 🚨 B1 は記録するだけ。失敗してもレポートの保存・日次処理は止めない (結果の 1 行に注記するだけ)。
 *    B2 で 9:40 の自動決定が、この基準と Render が計算に使うレポートの世代 (source_fetched_at) の一致を確かめて使う
 *
 * 置き場所 = DATA_DIR/fba-inbound-state.db (better-sqlite3。fba.db (sql.js) には書かない = 書き手を増やさない)
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import {
  takeInboundSnapshot, summarizeSnapshot, checkAgainstReport, diffSnapshots, nextTracked, nextPlanCache,
} from '../fba-replenishment/inbound-snapshot.js';

const KEEP_DAYS = 30;

/** fba.db の source_fetched_at ('YYYY-MM-DD HH:MM:SS' = UTC、または ISO) を ms に。読めなければ NaN */
export function sourceAtMs(v) {
  if (!v) return NaN;
  const s = String(v);
  return Date.parse(/[TZ]|[+-]\d\d:?\d\d$/.test(s) ? s : `${s.replace(' ', 'T')}Z`);
}

let db = null;
/** 呼ばれた時点の DATA_DIR で開く (試験は DATA_DIR を一時フォルダに向ける) */
export function openInboundStateDb() {
  const dir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const file = path.join(dir, 'fba-inbound-state.db');
  if (db && db.name === file) return db;
  if (db) { try { db.close(); } catch { /* 閉じ済み */ } }
  fs.mkdirSync(dir, { recursive: true });
  db = new Database(file);
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS inbound_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      business_date TEXT NOT NULL,
      phase TEXT NOT NULL,                 -- S0 / S1 / adhoc
      started_at TEXT NOT NULL,
      finished_at TEXT,
      ms INTEGER, calls INTEGER,
      complete INTEGER NOT NULL,
      errors TEXT,                         -- JSON
      payload TEXT NOT NULL,               -- JSON (スナップショットそのもの)
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS ix_inbound_snapshots_day ON inbound_snapshots (business_date, phase, id);
    CREATE TABLE IF NOT EXISTS inbound_baselines (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      business_date TEXT NOT NULL,
      s0_id INTEGER, s1_id INTEGER,
      report_fetched_at TEXT,              -- レポートを取り終えた時刻 (runFbaReportSnapshot の fetchedAt)
      restock_source_at TEXT,              -- fba.db restock_latest.source_fetched_at (Render が計算に使う世代。B2 で一致を確かめる)
      planning_source_at TEXT,
      usable INTEGER NOT NULL,             -- S0・S1 とも complete
      result TEXT NOT NULL,                -- JSON (照合・変化・一部出荷・結べない v0 便)
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS ix_inbound_baselines_day ON inbound_baselines (business_date, id);
    CREATE TABLE IF NOT EXISTS inbound_state_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
  `);
  return db;
}

function readCache(d) {
  const get = (k) => { const r = d.prepare('SELECT value FROM inbound_state_kv WHERE key = ?').get(k); return r ? JSON.parse(r.value) : null; };
  return { plans: get('plan_cache') || {}, tracked: get('tracked_plans') || [] };
}

function writeSnapshot(d, { businessDate, phase, snap, cache }) {
  const put = d.prepare(`INSERT INTO inbound_state_kv (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`);
  return d.transaction(() => {
    const id = d.prepare(`INSERT INTO inbound_snapshots (business_date, phase, started_at, finished_at, ms, calls, complete, errors, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(businessDate, phase, snap.startedAt, snap.finishedAt, snap.ms, snap.calls,
      snap.complete ? 1 : 0, JSON.stringify(snap.errors.concat(snap.unknownStates)), JSON.stringify(snap)).lastInsertRowid;
    const now = new Date().toISOString();
    put.run('plan_cache', JSON.stringify(nextPlanCache(snap, cache.plans)), now);
    put.run('tracked_plans', JSON.stringify(nextTracked(snap, cache.tracked)), now);
    d.prepare(`DELETE FROM inbound_snapshots WHERE created_at < ?`).run(new Date(Date.now() - KEEP_DAYS * 86400e3).toISOString());
    return Number(id);
  })();
}

/**
 * レポート取得の前後に呼ぶ入口 (runFbaReportSnapshot の inboundCapture)。**投げない** (注記の文字列を返す)。
 * @param {'S0'|'S1'} phase
 * @param {object} ctx
 * @param {string} ctx.businessDate
 * @param {(path: string, label: string) => Promise<object>} ctx.call
 * @param {object[]} [ctx.restockRows]   S1: 保存した RESTOCK の行 (正規化済み)
 * @param {string} [ctx.fetchedAt]       S1: レポートを取り終えた時刻
 * @param {object} [ctx.freshness]       S1: fba.db の getInputFreshness() (保存した表の世代)
 * @param {{ restock: boolean, planning: boolean }} [ctx.saved]  S1: 今回のレポートを restock_latest / planning_latest に保存できたか
 * @param {object} [ctx.snapshotOpts]    試験用 (sleep・paceMs・clock・budgetMs)
 */
export async function captureInboundPhase(phase, ctx) {
  try {
    const d = openInboundStateDb();
    const cache = readCache(d);
    const snap = await takeInboundSnapshot({ call: ctx.call, cache, ...(ctx.snapshotOpts || {}) });
    const id = writeSnapshot(d, { businessDate: ctx.businessDate, phase, snap, cache });
    const head = `${phase} ${snap.complete ? 'ok' : `不完全 (${snap.errors.concat(snap.unknownStates).slice(0, 2).join(' / ')})`} ${Math.round(snap.ms / 1000)}秒 ${snap.calls}回`;
    if (phase !== 'S1') return { id, note: head };

    // 基準 = 同じ日の直前の S0 と、この S1
    const s0row = d.prepare(`SELECT id, payload FROM inbound_snapshots WHERE business_date = ? AND phase = 'S0' AND id < ? ORDER BY id DESC LIMIT 1`)
      .get(ctx.businessDate, id);
    const s0 = s0row ? JSON.parse(s0row.payload) : null;
    const summary = summarizeSnapshot(snap);
    const check = checkAgainstReport(summary, ctx.restockRows || []);
    const diff = s0 ? diffSnapshots(s0, snap) : null;
    // 🚨 今回のレポートを保存できて、表の世代 (source_fetched_at) が S0 のあとだと確かめられたときだけ使える
    //    (保存に失敗すると前回の世代が残り、B2 の世代一致が古いレポートで通ってしまう。Codex PR #1463 R1 Medium 5)
    const genOk = !!(s0 && ctx.saved?.restock && ctx.saved?.planning
      && sourceAtMs(ctx.freshness?.restock_source_at) >= Date.parse(s0.startedAt)
      && sourceAtMs(ctx.freshness?.planning_source_at) >= Date.parse(s0.startedAt));
    const usable = !!(s0 && s0.complete && snap.complete && ctx.restockRows?.length && genOk);
    const result = {
      s0: s0 ? { complete: s0.complete, ms: s0.ms, calls: s0.calls, errors: s0.errors.length, deferred: s0.deferred } : null,
      s1: { complete: snap.complete, ms: snap.ms, calls: snap.calls, errors: snap.errors.length, deferred: snap.deferred },
      report_saved: ctx.saved || null, generation_ok: genOk,
      changed_during_report: diff ? { count: diff.changes.length, skus: diff.skus, changes: diff.changes.slice(0, 50), received_only: diff.receivedOnly.length, unknown: diff.unknown.length } : null,
      working_mismatch: { count: check.workingMismatch.length, top: check.workingMismatch.slice(0, 50) },
      open_mismatch: { count: check.openMismatch.length, top: check.openMismatch.slice(0, 50) },
      skus_checked: check.skusChecked,
      mixed_plans: summary.mixedPlans,
      unlinked_v0: summary.unlinkedV0,
      v0_only_tracked: summary.v0OnlyTracked.length,
      plans: snap.plans.filter((p) => !p.reused).length, v0_shipments: snap.v0.length,
    };
    d.prepare(`INSERT INTO inbound_baselines (business_date, s0_id, s1_id, report_fetched_at, restock_source_at, planning_source_at, usable, result)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(ctx.businessDate, s0row?.id ?? null, id, ctx.fetchedAt || null,
      ctx.freshness?.restock_source_at ?? null, ctx.freshness?.planning_source_at ?? null, usable ? 1 : 0, JSON.stringify(result));
    const note = `${head} / 準備中の照合: ${usable ? '' : '⚠️使えない '}レポート作成中に動いた ${diff ? diff.changes.length : '?'}・準備中の不一致 ${check.workingMismatch.length}・輸送中の不一致 ${check.openMismatch.length}${summary.mixedPlans.length ? `・一部出荷 ${summary.mixedPlans.length}` : ''}${summary.unlinkedV0.length ? `・結べない v0 便 ${summary.unlinkedV0.length}` : ''}`;
    return { id, note, result, usable };
  } catch (e) {
    return { id: null, note: `${phase} 失敗: ${String(e.message).slice(0, 120)}` };
  }
}
