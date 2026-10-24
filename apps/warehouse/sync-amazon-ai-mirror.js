#!/usr/bin/env node
/**
 * sync-amazon-ai-mirror.js — Amazon AI の分析 (ai-insights の facts-amazon.js) が読む表を Render の mirror へ (2026-10-24)
 *
 * warehouse.db の 13 表 → mirror_<表名> (/api/sync/<entity>/chunk・date_range clear・staging は受け手)。
 * 表・列・日数の決まりは lib/amazon-ai-mirror.js (受け手と共有)。sync-amazon-price-snapshot.js と同じ型 (chunk POST + sync_runs の記録)。
 *
 *   - 毎朝 (--daily): 表ごとに直近 sendDays 日を送り直す (取得が直近を取り直す幅)。前の回に送れた最後の日 (sync_runs の applied) が
 *     それより前なら、その日から送り直す (止まっていた日を埋める)。いちばん新しい run が失敗していれば (--full の途中など) その run の最初の日から。一度も送っていない表は残す日数 (keepDays) ぶん全部
 *   - 1 回の run は runDays 日まで (初回の全部送りを小分けにする = Render の stage を小さく)。run ごとに sync_runs に 1 行
 *   - 表が無い・行が 1 つも無く一度も送っていない (まだ取り始めていない) = 送らない (⏭️・失敗にしない・mirror はそのまま)。一度送った表が空になったら送る (mirror の行を消す)
 *   - 期間の中に行が無い日 = その日を mirror でも空にする (元が正)
 *   - 送るのは facts-amazon.js が読む列と主キーだけ。返品・補てんは元の主キー (注文番号など) のハッシュ row_key だけ送る
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ai-mirror.js --daily
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ai-mirror.js --daily --dry-run      # 送らずに 表ごとの期間・行数・大きさ
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ai-mirror.js --full                 # 残す日数ぶん全部送り直す (mirror を作り直したとき)
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ai-mirror.js --daily --entity amazon_catalog_rank_daily
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY (送るとき必須) / CHUNK_SIZE (既定 3000・上限 5000)
 * 終わり方: 全部の表が送れた or 送らない (⏭️) = exit 0 / どれかの表が失敗 = exit 1 (他の表は送る)。最後の行 = daily-sync の要約
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import {
  AMAZON_AI_MIRROR_ENTITIES, AMAZON_AI_CONTRACT_VERSION, seedAmazonAiMirrorContracts,
  addDaysYmd, datesBetween, isKeptDate,
} from '../../lib/amazon-ai-mirror.js';

export const DEFAULT_CHUNK_SIZE = 3000;
export const MAX_CHUNK_SIZE = 5000;
const POST_TIMEOUT_MS = 120000;

export const jstToday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600 * 1000).toISOString().slice(0, 10);

/**
 * 表の状態: missing = 表が無い / empty = 表に行が 1 つも無く、一度も送っていない (まだ取り始めていない) / ok
 *   🚨 一度送った表は、元が空になっても (ERROR が全部直った・JP の行が無い) 送る = mirror の行を消す (古い ERROR を残さない。Codex R2 P2)。
 *   「空」は where (ERROR だけ・JP だけ) で絞らずに表全体で見る (絞った行が 0 = 正しく 0 件のこともある)
 */
export function sourceState(db, spec) {
  const t = db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(spec.source);
  if (!t) return 'missing';
  if (db.prepare(`SELECT 1 FROM ${spec.source} LIMIT 1`).get()) return 'ok';
  return lastAppliedTo(db, spec.entity) ? 'ok' : 'empty';
}

/** 前の回に送れた最後の日 (この表の applied の run の scope_to の最大)。無ければ null */
export function lastAppliedTo(db, entity) {
  return db.prepare(`SELECT MAX(scope_to) AS d FROM sync_runs WHERE entity = ? AND status = 'applied'`).get(entity)?.d || null;
}
/**
 * いちばん新しい run が失敗・途中 (started のまま) なら、その run の最初の日 (そこから後は送れていない = run は古い順に送り、失敗したら止める)。
 * --full の途中で止まったときも、次の回はそこから今日まで送り直す (前に送れた最後の日だけ見ると、直近しか送らずに古い日が欠けたままになる。Codex R2 P2)
 */
export function resumeFrom(db, entity) {
  const r = db.prepare(`SELECT status, scope_from FROM sync_runs WHERE entity = ? ORDER BY started_at DESC, rowid DESC LIMIT 1`).get(entity);
  return r && r.status !== 'applied' ? r.scope_from : null;
}

/**
 * 送る期間 [from, to]。to = 今日 (JST)。
 *   full / 一度も送っていない = 残す日数ぶん全部。それ以外 = 直近 sendDays 日、前の回の最後の日がそれより前ならそこから・
 *   前の回が途中で失敗していればその run の最初の日から (どれも残す日数まで)
 */
export function planScope(spec, { today, lastTo = null, resume = null, full = false }) {
  const to = today;
  const keepFrom = addDaysYmd(to, -(spec.keepDays - 1));
  if (full || !lastTo) return { from: keepFrom, to, mode: full ? 'full' : 'first' };
  const base = lastTo < to ? addDaysYmd(lastTo, 1) : to;   // 前の回の次の日 (今日までに送っていれば今日)
  let from = addDaysYmd(base, -(spec.sendDays - 1));
  if (resume && resume < from) from = resume;
  return { from: from < keepFrom ? keepFrom : from, to, mode: from < addDaysYmd(to, -(spec.sendDays - 1)) ? 'catch_up' : 'daily' };
}

/** [from, to] を runDays 日ずつに分ける (古い順) */
export function splitRuns(from, to, runDays) {
  const out = [];
  for (let f = from; f <= to; f = addDaysYmd(f, runDays)) {
    const t = addDaysYmd(f, runDays - 1);
    out.push({ from: f, to: t < to ? t : to });
  }
  return out;
}

const rowKeyOf = (spec, r) => crypto.createHash('sha256').update(JSON.stringify(spec.rowKey.map((c) => r[c]))).digest('hex').slice(0, 16);

/**
 * 1 回の run の行 (送る列だけ)。残さない日 (月末でない古い写しの日) の行は送らない (受け手が消す決まりと同じ = isKeptDate)。
 * keepTo = 残す日を決める基準の日 (= 全体の最後の日 = 今日)
 */
export function selectRunRows(db, spec, { from, to, keepTo }) {
  const sendCols = spec.cols.map(([c]) => c);
  const selectCols = [...new Set([...sendCols.filter((c) => c !== 'row_key'), ...(spec.rowKey || [])])];
  const w = spec.where ? `AND ${spec.where}` : '';
  const rows = db.prepare(`SELECT ${selectCols.join(', ')} FROM ${spec.source}
    WHERE ${spec.dateCol} >= ? AND ${spec.dateCol} <= ? ${w}
    ORDER BY ${spec.dateCol}, ${(spec.rowKey || spec.pk).join(', ')}`).all(from, to);
  const out = [];
  for (const r of rows) {
    if (!isKeptDate(spec, r[spec.dateCol], keepTo)) continue;
    const o = {};
    for (const c of sendCols) o[c] = c === 'row_key' ? rowKeyOf(spec, r) : r[c];
    out.push(o);
  }
  return out;
}

/** 行の監査の列 (source_run_id / source_row_hash / synced_at) を付けて chunk に分ける。0 行でも 1 つ (clear だけ) */
export function buildChunks(spec, rows, { runId, syncedAt, chunkSize }) {
  const sendCols = spec.cols.map(([c]) => c);
  const enriched = rows.map((r) => ({
    ...r,
    source_run_id: runId,
    source_row_hash: crypto.createHash('sha256').update(JSON.stringify(sendCols.map((c) => r[c]))).digest('hex').slice(0, 16),
    synced_at: syncedAt,
  }));
  const chunks = [];
  for (let i = 0; i < enriched.length; i += chunkSize) chunks.push(enriched.slice(i, i + chunkSize));
  if (chunks.length === 0) chunks.push([]);
  return chunks;
}

/** 受け手に送る chunk の本文 (sync-amazon-price-snapshot.js と同じ形) */
export function chunkBody(spec, chunks, i, { runId, from, to }) {
  const payload = { rows: chunks[i] };
  return {
    sync_run_id: runId, contract_version: AMAZON_AI_CONTRACT_VERSION,
    scope_from: from, scope_to: to,
    chunk_index: i, chunk_count: chunks.length,
    is_first: i === 0, is_last: i === chunks.length - 1,
    row_count: chunks[i].length,
    payload_checksum: crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex'),
    meta: i === 0 ? { [spec.clearMetaKey]: datesBetween(from, to) } : {},
    payload,
  };
}

/** 本物の送り口 (fetch)。{ ok, status, json, text } を返す (投げない) */
export function makeHttpPost({ renderUrl, syncKey, fetchImpl = fetch, timeoutMs = POST_TIMEOUT_MS }) {
  return async (entity, body) => {
    const headers = { 'Content-Type': 'application/json' };
    if (syncKey) headers['x-sync-key'] = syncKey;
    try {
      const res = await fetchImpl(`${renderUrl}/api/sync/${entity}/chunk`, {
        method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await res.text();
      let json = null;
      try { json = JSON.parse(text); } catch { /* 本文が JSON でない */ }
      return { ok: res.ok, status: res.status, json, text };
    } catch (e) {
      return { ok: false, status: 0, json: null, text: String(e?.message || e) };
    }
  };
}

const newRunId = (entity) => `${entity}-v${AMAZON_AI_CONTRACT_VERSION}-${new Date().toISOString().replace(/[:.]/g, '').replace('Z', '')}-${crypto.randomBytes(3).toString('hex')}`;

/** 1 回の run を送る (sync_runs に started → applied / failed)。投げない */
async function sendRun(db, spec, run, { keepTo, dryRun, post, chunkSize, log }) {
  const rows = selectRunRows(db, spec, { from: run.from, to: run.to, keepTo });
  const runId = newRunId(spec.entity);
  const chunks = buildChunks(spec, rows, { runId, syncedAt: new Date().toISOString(), chunkSize });
  const bodies = chunks.map((_, i) => chunkBody(spec, chunks, i, { runId, from: run.from, to: run.to }));
  const bytes = bodies.reduce((s, b) => s + Buffer.byteLength(JSON.stringify(b)), 0);
  if (dryRun) return { ok: true, rows: rows.length, chunks: chunks.length, bytes };
  db.prepare(`INSERT INTO sync_runs (run_id, entity, contract_version, source_host, scope_from, scope_to, chunk_count_expected, status, started_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'started', ?)`).run(runId, spec.entity, AMAZON_AI_CONTRACT_VERSION, os.hostname(), run.from, run.to, chunks.length, new Date().toISOString());
  let sentRows = 0, applied = 0, error = null;
  for (let i = 0; i < chunks.length; i++) {
    const r = await post(spec.entity, bodies[i]);
    if (!r.ok) { error = `HTTP ${r.status}: ${String(r.text || '').slice(0, 300)}`; break; }
    sentRows += chunks[i].length;
    applied++;
  }
  const at = new Date().toISOString();
  if (error) {
    db.prepare(`UPDATE sync_runs SET status = 'failed', error_message = ?, completed_at = ?, chunk_count_received = ?, row_count_received = ? WHERE run_id = ?`)
      .run(error, at, applied, sentRows, runId);
    log(`  ✗ ${spec.entity} ${run.from}〜${run.to}: ${error}`);
    return { ok: false, rows: sentRows, chunks: applied, bytes, error };
  }
  db.prepare(`UPDATE sync_runs SET status = 'applied', chunk_count_received = ?, row_count_received = ?, completed_at = ?, applied_at = ? WHERE run_id = ?`)
    .run(applied, sentRows, at, at, runId);
  log(`  ✓ ${spec.entity} ${run.from}〜${run.to}: ${sentRows.toLocaleString()} 行 / ${applied} chunk (run_id=${runId})`);
  return { ok: true, rows: sentRows, chunks: applied, bytes };
}

/**
 * 1 つの表を送る。投げない (失敗は { status: 'failed', error })。
 * 🚨 run は古い順。途中の run が失敗したら後の run は送らない (前の回の最後の日 = applied の scope_to が穴をまたがない = 次の朝にそこから送り直す)
 */
export async function syncEntity(db, spec, { today, full = false, dryRun = false, post, chunkSize = DEFAULT_CHUNK_SIZE, log = console.log }) {
  try {
    const state = sourceState(db, spec);
    if (state !== 'ok') {
      const reason = state === 'missing' ? '表が無い' : '行が無い (まだ取り始めていない)';
      log(`  ⏭️ ${spec.entity}: ${reason} = 送らない (mirror はそのまま)`);
      return { entity: spec.entity, status: 'skipped', reason: state };
    }
    const contract = dryRun ? { contract_version: AMAZON_AI_CONTRACT_VERSION }   // dry-run は読むだけ (契約は送る回に入れる)
      : db.prepare(`SELECT contract_version FROM sync_contracts WHERE entity = ?`).get(spec.entity);
    if (!contract || contract.contract_version !== AMAZON_AI_CONTRACT_VERSION) {
      return { entity: spec.entity, status: 'failed', error: `契約 (sync_contracts) が無い・版が違う (code=${AMAZON_AI_CONTRACT_VERSION}, db=${contract?.contract_version ?? 'なし'})` };
    }
    const scope = planScope(spec, { today, lastTo: lastAppliedTo(db, spec.entity), resume: resumeFrom(db, spec.entity), full });
    const runs = splitRuns(scope.from, scope.to, spec.runDays);
    log(`  ${spec.entity}: ${scope.from}〜${scope.to} (${scope.mode}・run ${runs.length})`);
    let rows = 0, chunks = 0, bytes = 0;
    for (const run of runs) {
      const r = await sendRun(db, spec, run, { keepTo: scope.to, dryRun, post, chunkSize, log });
      rows += r.rows; chunks += r.chunks; bytes += r.bytes;
      if (!r.ok) return { entity: spec.entity, status: 'failed', error: r.error, rows, chunks, bytes, scope };
    }
    return { entity: spec.entity, status: dryRun ? 'dry_run' : 'sent', rows, chunks, bytes, scope, runs: runs.length };
  } catch (e) {
    return { entity: spec.entity, status: 'failed', error: String(e?.message || e) };
  }
}

/** 全部の表 (entities で絞れる)。表ごとに独立 (1 つの失敗で他を止めない) */
export async function syncAmazonAiMirror(db, { today = jstToday(), full = false, dryRun = false, entities = null, post, chunkSize = DEFAULT_CHUNK_SIZE, log = console.log } = {}) {
  if (!dryRun) seedAmazonAiMirrorContracts(db);   // 契約は code が正 (initDB の前でも送れる・冪等)
  const specs = entities ? AMAZON_AI_MIRROR_ENTITIES.filter((s) => entities.includes(s.entity)) : AMAZON_AI_MIRROR_ENTITIES;
  const outcomes = [];
  for (const spec of specs) outcomes.push(await syncEntity(db, spec, { today, full, dryRun, post, chunkSize, log }));
  return outcomes;
}

const mb = (b) => `${(b / 1048576).toFixed(1)}MB`;
/** daily-sync の要約の 1 行 (最後の行)。失敗 = ❌ / 送れた (⏭️ があっても) = ✅ */
export function summaryLine(outcomes, { elapsedSec = null, dryRun = false } = {}) {
  const sent = outcomes.filter((o) => o.status === 'sent' || o.status === 'dry_run');
  const skipped = outcomes.filter((o) => o.status === 'skipped');
  const failed = outcomes.filter((o) => o.status === 'failed');
  const rows = sent.reduce((s, o) => s + (o.rows || 0), 0);
  const bytes = sent.reduce((s, o) => s + (o.bytes || 0), 0);
  const parts = [`${dryRun ? '送る見込み' : '送った'} ${sent.length}/${outcomes.length} 表 (${rows.toLocaleString()} 行・${mb(bytes)}${elapsedSec != null ? `・${elapsedSec} 秒` : ''})`];
  if (skipped.length) parts.push(`⏭️ まだ無い ${skipped.length} (${skipped.map((o) => o.entity).join(', ')})`);
  if (failed.length) parts.push(`失敗 ${failed.length} (${failed.map((o) => `${o.entity}: ${String(o.error).slice(0, 120)}`).join(' / ')})`);
  return `${failed.length ? '❌' : '✅'} Amazon AI の mirror: ${parts.join(' / ')}`;
}

// ── 単体実行 ──
const isMain = !!process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (flag) => { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
  const full = args.includes('--full');
  const dryRun = args.includes('--dry-run');
  const entity = getArg('--entity');
  const renderUrl = process.env.RENDER_MIRROR_URL;
  const syncKey = process.env.MIRROR_SYNC_KEY;
  const chunkSize = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE);
  if (!args.includes('--daily') && !full) { console.error('FATAL: --daily か --full が要る'); process.exit(2); }
  if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
  const dbPath = path.join(DATA_DIR, 'warehouse.db');
  if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
  if (!dryRun && (!renderUrl || !syncKey)) { console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2); }
  if (entity && !AMAZON_AI_MIRROR_ENTITIES.some((s) => s.entity === entity)) {
    console.error(`FATAL: unknown --entity '${entity}' (valid: ${AMAZON_AI_MIRROR_ENTITIES.map((s) => s.entity).join(', ')})`); process.exit(2);
  }
  const db = new Database(dbPath, dryRun ? { readonly: true } : {});
  db.pragma('busy_timeout = 60000');
  const started = Date.now();
  console.log(`=== sync Amazon AI の mirror (v${AMAZON_AI_CONTRACT_VERSION}) / 今日 ${jstToday()} / ${full ? 'full' : 'daily'}${dryRun ? ' / dry-run' : ''} ===`);
  const outcomes = await syncAmazonAiMirror(db, {
    full, dryRun, entities: entity ? [entity] : null, chunkSize,
    post: dryRun ? null : makeHttpPost({ renderUrl, syncKey }),
  });
  db.close();
  for (const o of outcomes) {
    if (o.status === 'failed') console.error(`  ❌ ${o.entity}: ${o.error}`);
    else if (o.status !== 'skipped') console.log(`  ${o.entity}: ${o.rows.toLocaleString()} 行・${o.chunks} chunk・${mb(o.bytes)}`);
  }
  console.log(summaryLine(outcomes, { elapsedSec: Math.round((Date.now() - started) / 1000), dryRun }));
  // 通信直後の process.exit() は Windows node で libuv assertion を踏む → exitCode + 自然終了 (#614 と同根)
  process.exitCode = outcomes.some((o) => o.status === 'failed') ? 1 : 0;
}
