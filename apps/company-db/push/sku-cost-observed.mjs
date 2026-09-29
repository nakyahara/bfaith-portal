#!/usr/bin/env node
/**
 * push/sku-cost-observed.mjs — miniPC の原価の履歴 (warehouse.db m_products_history) から「観測の原価」(SKU × 期間) を作って Company DB (Render Postgres) へ送る
 *   (D7b-2。設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.3・§3.4・D-57。受け口 = POST /apps/company-db/sync/sku-cost-observed・受け皿 = 0046)。
 *
 *   node apps/company-db/push/sku-cost-observed.mjs --send        ← daily-sync (「m_products 履歴記録」の直後)。全部を作り直して、Render と違えば 1 要求で入れ替える
 *   node apps/company-db/push/sku-cost-observed.mjs --dry-run     ← 作って数えるだけ (Render は読むだけ = POST しない・台帳を開かない)
 *   [--data-dir <dir>] (既定 = env DATA_DIR → リポジトリの data/)
 *
 * 期間の作り方 (設計 §3.4。🚨 core.sku_costs とは別の表 = 夜間ロードの原価の行を触らない):
 *   - 値 = 夜間ロードと同じ: 状態 (原価状態) は COMPLETE / OVERRIDDEN だけ (load/sources.mjs の mapCost)・原価は Math.round・数でない / 負は原価不明 (load/engine.mjs の costForLoad)
 *   - changed_at (履歴の記録が書いた UTC の時刻) の **JST の日の翌日から** 有効 (日次の処理が朝に気づく = その日の注文は前の原価)
 *   - 🚨 例外 = **最初の BASELINE_RESET (5/5 の写し)** はその日から observed。それより前 (2026-01-01 〜 写しの日の前日) は同じ値を estimated で推定
 *   - 最初の写しに無く後で初めて出た商品コードは、初めて出た日より前を推定しない (原価不明)
 *   - 同じ有効日に複数の変化 = 最後の値 (changed_at → history_id の順で最後)。BASELINE_RESET / INSERT / UPDATE = 値の観測・DELETE = 原価不明の始まり (再 INSERT はその翌日から)
 *   - PARTIAL / MISSING / 読めない原価 = 原価不明の期間 (行を作らない)。原価と状態が変わらない履歴の行は区切りにしない (続く同じ値はまとめる)
 *   - 最初の写しより前の履歴の行 (旧 trigger の名残があれば) は使わない (数えて要約に出す)
 * 商品コード → SKU (夜間ロードと同じ正規化 normSku = core.norm_code と衝突の隔離):
 *   - 履歴の商品コードを正規化して **2 つ以上が同じになる = 曖昧 = どれも送らない** (ambiguous_code_count)
 *   - Render の core.skus に無い (GET …/sku-cost-observed/sku-codes で読む)・形が不正 = 結びつかない = 送らない (unresolved_code_count)
 * SKU ごとの境目 (§3.3): **送り手は全部送り、読む側 (mart.v_sku_cost_observed_effective) で core.sku_costs の最初の valid_from より前に切る**
 *   (sku_costs が後から始まる SKU・同じ日の夜間ロードとの前後でも、読むときに正しく切れる。送る中身が SQLite だけで決まる = 再送が同じ中身になる)
 * 世代 (設計 §3.4 = coverage と同じ):
 *   - 回の始めに Render の今の世代を読み、台帳 (DATA_DIR/company-db-push.db の sku_cost_observed:) の連番を少なくともそこまで進める
 *   - Render の今の manifest (checksum・行の数・結びつかない数・曖昧な数) と同じなら送らない (変わりなし)
 *   - 違えば新しい世代を **HTTP の前に台帳に書いてから** (送る manifest も pending として) 送る。応答が失われた (前の回の pending が Render より新しく中身も同じ) なら **同じ世代・同じ中身で再送** (= same)
 *   - 🚨 --dry-run は台帳を開かない・世代を採らない・POST しない (Render は GET で読むだけ)
 * 送信の失敗 (HTTP・409・stale)・別の送り手の見送りは ❌ / ⏸️ で exit 1 = daily-sync の工程が失敗 (成功の合図を出さない)・retry に載る
 * Render に 0046 がまだ無い (status が 409 not_migrated) = ⏭️ で exit 0 (マージから migrate (中原さんの指示の後) までの朝を ❌ にしない・世代も採らない)
 */
import 'dotenv/config';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { normSku } from '../../../lib/sku-norm.js';
import { mapCost } from '../load/sources.mjs';
import { costForLoad } from '../load/engine.mjs';
import { postJson, fetchAllKeys, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { syncBase } from './ne-shipments.mjs';
import { openLedger, LockLostError } from './ledger.mjs';
import { observedChecksum, observedRowOf, byCodeFrom, sameManifest, SOURCE, MAX_ROWS } from '../ingest/sku-cost-observed.mjs';
import { isValidCode } from '../ingest/stock-daily.mjs';

export const KIND = 'sku_cost_observed';          // 台帳の種類 (meta の鍵の前に付く)
export const ESTIMATE_FROM = '2026-01-01';        // 推定の始まり = policy の period_from (amazon / jp。0043)
export const MAX_BODY_BYTES = 11 * 1024 * 1024;   // 受け口の parser は 12MB
export const META_PENDING = 'pending';            // 送る前に書く { generation, checksum, row_count, unresolved_code_count, ambiguous_code_count }
const OPS = ['BASELINE_RESET', 'INSERT', 'UPDATE', 'DELETE'];
const CHANGED_AT_RE = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})$/;
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);

/** 履歴の changed_at (record-m-products-history.js が書く UTC の 'YYYY-MM-DD HH:MM:SS') → 'YYYY-MM-DDTHH:MM:SSZ'。読めなければ例外 (推測しない) */
export function changedAtIso(v) {
  const m = typeof v === 'string' ? CHANGED_AT_RE.exec(v) : null;
  const iso = m ? `${m[1]}T${m[2]}Z` : null;
  if (!iso || Number.isNaN(Date.parse(iso)) || new Date(Date.parse(iso)).toISOString().replace(/\.\d{3}Z$/, 'Z') !== iso) throw new Error(`changed_at が読めない (UTC の YYYY-MM-DD HH:MM:SS でない): ${JSON.stringify(v).slice(0, 40)}`);
  return iso;
}
/** UTC の時刻 → JST の日 */
export const jstDayOf = (iso) => new Date(Date.parse(iso) + 9 * 3600 * 1000).toISOString().slice(0, 10);

/** 履歴の 1 行の値 = { cost_jpy, cost_status } / 原価不明 = null (DELETE・PARTIAL / MISSING・数でない・負) */
export function historyValueOf(row) {
  if (row.operation === 'DELETE') return null;
  const c = costForLoad(mapCost(row));
  return c ? { cost_jpy: c.cost_jpy, cost_status: c.cost_status } : null;
}
const sameValue = (a, b) => (a === null || b === null ? a === b : a.cost_jpy === b.cost_jpy && a.cost_status === b.cost_status);

/**
 * 履歴の行 → 商品コードごとの期間 (純粋な関数・試験はここを直に)。
 * rows = [{ history_id, 商品コード, 原価, 原価ソース, 原価状態, changed_at, operation }]
 * 戻り値 = { periods: [行 (送る形)], codes: [履歴に出る商品コード (最初の写し以後)], baselineAt, baselineDay, ignoredBeforeBaseline, estimatedRows }
 */
export function buildObservedPeriods(rows, { estimateFrom = ESTIMATE_FROM } = {}) {
  const evs = rows.map((r) => {
    const hid = Number(r.history_id);
    if (!Number.isSafeInteger(hid) || hid <= 0) throw new Error(`history_id が読めない: ${JSON.stringify(r.history_id)}`);
    if (typeof r['商品コード'] !== 'string' || r['商品コード'] === '') throw new Error(`history_id ${hid}: 商品コードが空`);
    if (!OPS.includes(r.operation)) throw new Error(`history_id ${hid}: 知らない operation ${JSON.stringify(r.operation).slice(0, 30)}`);
    let at;
    try { at = changedAtIso(r.changed_at); } catch (e) { throw new Error(`history_id ${hid}: ${e.message}`); }
    return { hid, code: r['商品コード'], at, operation: r.operation, row: r };
  });
  const baselineAt = evs.filter((e) => e.operation === 'BASELINE_RESET').reduce((m, e) => (m === null || e.at < m ? e.at : m), null);
  if (baselineAt === null) throw new Error('m_products_history に BASELINE_RESET (最初の写し) が無い = 観測の始まりが決まらない');
  const baselineDay = jstDayOf(baselineAt);
  const byCode = new Map();
  let ignoredBeforeBaseline = 0;
  for (const e of evs) {
    if (e.at < baselineAt) { ignoredBeforeBaseline++; continue; }
    if (!byCode.has(e.code)) byCode.set(e.code, []);
    byCode.get(e.code).push(e);
  }
  const periods = [];
  let estimatedRows = 0;
  for (const [code, list] of byCode) {
    list.sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : a.hid - b.hid));
    const byEff = new Map();   // 有効日 → その日の最後の変化
    let baselineEv = null;
    for (const e of list) {
      const firstBaseline = e.operation === 'BASELINE_RESET' && e.at === baselineAt;
      const eff = firstBaseline ? baselineDay : addDays(jstDayOf(e.at), 1);
      if (firstBaseline) baselineEv = e;
      byEff.set(eff, e);
    }
    const mk = (seg, to, method, from = seg.from) => ({
      product_code: code, cost_jpy: seg.value.cost_jpy, cost_status: seg.value.cost_status, valid_from: from, valid_to: to,
      backfill_method: method, source_history_id: seg.ev.hid, first_observed_at: seg.ev.at,
    });
    let seg = null;
    for (const eff of [...byEff.keys()].sort()) {
      const ev = byEff.get(eff), value = historyValueOf(ev.row);
      if (seg && sameValue(seg.value, value)) continue;   // 原価と状態が変わらない = 区切りにしない
      if (seg && seg.value) periods.push(mk(seg, addDays(eff, -1), 'observed_daily_diff'));
      seg = { value, from: eff, ev };
    }
    if (seg && seg.value) periods.push(mk(seg, null, 'observed_daily_diff'));
    // 最初の写しの前 (2026-01-01 〜 写しの日の前日) を写しの値で推定。写しに無い・写しの値が原価不明のコードは推定しない
    if (baselineEv && estimateFrom < baselineDay) {
      const v = historyValueOf(byEff.get(baselineDay).row);
      if (v) { periods.push(mk({ value: v, ev: byEff.get(baselineDay) }, addDays(baselineDay, -1), 'estimated_before_first_snapshot', estimateFrom)); estimatedRows++; }
    }
  }
  periods.sort(byCodeFrom);
  return { periods, codes: [...byCode.keys()], baselineAt, baselineDay, ignoredBeforeBaseline, estimatedRows };
}

/**
 * 商品コードの結びつけ (夜間ロードと同じ正規化と衝突の隔離) → 送る行と manifest。
 * renderNorms = Render の core.skus の code_norm の集合。戻り値 = { rows, manifest, ambiguousCodes, unresolvedCodes, skus }
 */
export function planPayload(built, renderNorms) {
  const byNorm = new Map();
  for (const c of built.codes) { const k = normSku(c); if (!byNorm.has(k)) byNorm.set(k, []); byNorm.get(k).push(c); }
  const ambiguous = new Set(), unresolved = new Set();
  for (const c of built.codes) {
    const k = normSku(c);
    if (k !== '' && byNorm.get(k).length > 1) ambiguous.add(c);
    else if (k === '' || !isValidCode(c) || !renderNorms.has(k)) unresolved.add(c);
  }
  const rows = built.periods.filter((p) => !ambiguous.has(p.product_code) && !unresolved.has(p.product_code)).map((p) => observedRowOf(p));
  const manifest = { checksum: observedChecksum(rows), row_count: rows.length, unresolved_code_count: unresolved.size, ambiguous_code_count: ambiguous.size };
  const sort = (s) => [...s].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return { rows, manifest, ambiguousCodes: sort(ambiguous), unresolvedCodes: sort(unresolved), skus: new Set(rows.map((r) => r.product_code)).size };
}

/** 履歴を読む (1 文 = 1 つの読み取りの時点)。表が無ければ例外 */
export function readHistory(warehouse) {
  if (!warehouse.prepare(`select 1 as x from sqlite_master where type = 'table' and name = 'm_products_history'`).get()) throw new Error('m_products_history が無い (apps/warehouse/record-m-products-history.js がまだ一度も走っていない)');
  return warehouse.prepare('select history_id, 商品コード, 原価, 原価ソース, 原価状態, changed_at, operation from m_products_history order by 商品コード, changed_at, history_id').all();
}

const isHex64 = (v) => typeof v === 'string' && /^[0-9a-f]{64}$/.test(v);
const isCnt = (v) => Number.isInteger(v) && v >= 0;
/** Render の status の応答の形を確かめる。戻り値 = load (null = まだ 1 度も受けていない) */
function remoteLoadOf(j) {
  if (!j || typeof j !== 'object' || !Object.hasOwn(j, 'load')) throw new Error('Render の状態の応答に load が無い');
  const l = j.load;
  if (l === null) return null;
  if (!l || !Number.isSafeInteger(l.generation) || l.generation <= 0 || !isHex64(l.checksum) || !isCnt(l.row_count) || !isCnt(l.unresolved_code_count) || !isCnt(l.ambiguous_code_count)) {
    throw new Error(`Render の状態の応答の形が分からない: ${JSON.stringify(l).slice(0, 160)}`);
  }
  return l;
}
function parsePending(raw) {
  if (!raw) return null;
  try { const p = JSON.parse(raw); return p && Number.isSafeInteger(p.generation) && p.generation > 0 && isHex64(p.checksum) ? p : null; } catch { return null; }
}
const newPushRunId = (d) => `push_sco_${d.toISOString().replace(/[-:.TZ]/g, '').slice(0, 17)}_${crypto.randomBytes(3).toString('hex')}`;

/**
 * 送る (本体)。戻り値 = { ok, dryRun, status: 'applied'|'same'|'unchanged'|'stale'|'dry-run'|'not_migrated'|null, generation, reusedGeneration, rows, skus, manifest, built: {…}, lockedBy, lastLine }
 *   送信の失敗 (HTTP・409・応答の食い違い) は例外 (呼ぶ側 = CLI が ❌ で exit 1)。ledger を渡さなければ dataDir の台帳を開く (dry-run では開かない)
 * 差し替え (試験): fetchImpl / sleep / now / pid / isAlive / ledger
 */
export async function pushSkuCostObserved({ warehouse, dataDir = null, ledger = null, fetchImpl = fetch, base, syncKey, dryRun = false, log = console.log, sleep, now = () => new Date(),
  owner = `pid:${process.pid}`, pid = process.pid, isAlive, estimateFrom = ESTIMATE_FROM, maxBodyBytes = MAX_BODY_BYTES } = {}) {
  if (!base) throw new Error('Render の宛先が無い (RENDER_MIRROR_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const startedAt = now();
  const out = { ok: false, dryRun, status: null, generation: null, reusedGeneration: false, rows: 0, skus: 0, manifest: null, built: null, remote: null, lockedBy: null, lastLine: '' };
  // 🚨 dry-run は台帳を開かない (開くだけで表を作る = 書く)
  let L = null, ownLedger = false;
  if (!dryRun) {
    if (ledger) L = ledger; else { if (!dataDir) throw new Error('台帳の場所 (DATA_DIR) が無い'); L = openLedger(dataDir, { kind: KIND }); ownLedger = true; }
    const lock = L.acquireLock({ owner, pid, now: startedAt, ...(isAlive ? { isAlive } : {}) });
    if (!lock.ok) {
      out.lockedBy = lock.held;
      out.lastLine = `⏸️ Company DB 観測の原価: 別の送り手が走っているので見送り (${lock.held.owner} pid ${lock.held.pid} 心拍 ${lock.held.heartbeat_at})`;
      if (ownLedger) L.close();
      return out;
    }
  }
  const mustOwn = () => { if (L && !L.renewLock(owner, now())) throw new LockLostError(); };
  const runId = newPushRunId(startedAt);
  let err = null;
  try {
    if (L) L.recordRun({ run_id: runId, mode: 'full', started_at: startedAt.toISOString() });
    // ① Render の今の世代 (台帳を少なくともそこまで進める = 台帳を失くした・Render を復元した)
    const sres = await fetchImpl(`${base}/sku-cost-observed/status`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (sres.status === 409) {
      const j = await sres.json().catch(() => null);
      if (j && j.error === 'not_migrated') { out.status = 'not_migrated'; out.ok = true; return out; }   // 0046 の適用前 = 送らない (⏭️・exit 0)。世代も採らない
      throw new Error(`Render の観測の原価の状態が取れない: HTTP 409 ${JSON.stringify(j).slice(0, 160)}`);
    }
    if (!sres.ok) throw new Error(`Render の観測の原価の状態が取れない: HTTP ${sres.status}${sres.status === 404 ? ' (Render がまだ新しい版になっていない?)' : ''} ${(await sres.text()).replace(/\s+/g, ' ').slice(0, 200)}`);
    const remote = remoteLoadOf(await sres.json());
    out.remote = remote;
    mustOwn();
    if (L && remote) L.ensureBatchSeqAtLeast(remote.generation, now());
    // ② Render の SKU の一覧 (商品コードを結べるか)
    const norms = new Set(await fetchAllKeys(fetchImpl, { base, syncKey, path: '/sku-cost-observed/sku-codes', keysOf: (j) => j.keys }));
    mustOwn();
    // ③ 履歴 → 期間 → 送る行
    const built = buildObservedPeriods(readHistory(warehouse), { estimateFrom });
    const plan = planPayload(built, norms);
    Object.assign(out, { rows: plan.rows.length, skus: plan.skus, manifest: plan.manifest,
      built: { baselineAt: built.baselineAt, baselineDay: built.baselineDay, codes: built.codes.length, periods: built.periods.length, estimatedRows: built.estimatedRows, ignoredBeforeBaseline: built.ignoredBeforeBaseline,
        ambiguousExamples: plan.ambiguousCodes.slice(0, 5), unresolvedExamples: plan.unresolvedCodes.slice(0, 5) } });
    if (plan.rows.length > MAX_ROWS) throw new Error(`行が多すぎる (${plan.rows.length} > 受け口の上限 ${MAX_ROWS})`);
    const payloadBase = { source: SOURCE, ...plan.manifest, rows: plan.rows };
    const bytes = Buffer.byteLength(JSON.stringify({ ...payloadBase, generation: Number.MAX_SAFE_INTEGER }));
    if (bytes > maxBodyBytes) throw new Error(`送る中身が大きすぎる (${bytes} バイト > ${maxBodyBytes})`);
    // ④ Render と同じ中身なら送らない
    if (remote && sameManifest(remote, plan.manifest)) { out.status = 'unchanged'; out.generation = remote.generation; out.ok = true; return out; }
    if (dryRun) { out.status = 'dry-run'; out.ok = true; return out; }
    // ⑤ 世代: 前の回の pending が Render より新しく中身も同じ = 応答が失われた → 同じ世代で再送 / ほかは新しい世代 (HTTP の前に台帳へ)
    const pending = parsePending(L.getMeta(META_PENDING));
    let gen;
    if (pending && pending.generation > (remote ? remote.generation : 0) && pending.generation <= L.currentBatchSeq() && sameManifest(pending, plan.manifest)) { gen = pending.generation; out.reusedGeneration = true; }
    else {
      gen = L.nextBatchSeq(now(), owner);
      L.setMeta({ [META_PENDING]: JSON.stringify({ generation: gen, ...plan.manifest }) }, { owner, at: now() });
    }
    out.generation = gen;
    // ⑥ 送る (5xx・通信の失敗は同じ body で再送 = same になる。4xx は例外)
    const res = await postJson(fetchImpl, { base, syncKey, path: '/sku-cost-observed', log, sleep, body: { ...payloadBase, generation: gen }, beforeAttempt: mustOwn });
    if (!res || !['applied', 'same', 'stale'].includes(res.status)) throw new Error(`受け口の応答が分からない: ${JSON.stringify(res).slice(0, 160)}`);
    if (res.checksum !== plan.manifest.checksum || res.generation !== gen) throw new Error(`受け口の応答 (世代 ${res.generation} / 指紋 ${String(res.checksum).slice(0, 16)}…) が送った内容と違う`);
    out.status = res.status;
    if (res.status === 'stale') throw new Error(`Render の方が新しい世代 (${res.remote_generation} > ${gen}) = 書かれなかった (別の送り手・台帳の食い違い?)`);
    if (res.status === 'applied' && res.rows !== plan.rows.length) throw new Error(`受け口が入れた行数 ${res.rows} が、送った行数 ${plan.rows.length} と違う`);
    out.ok = true;
    return out;
  } catch (e) {
    err = e; out.ok = false; out.error = e.message;
    throw e;
  } finally {
    const b = out.built;
    const tail = b ? ` / SKU ${out.skus} (推定の行 ${b.estimatedRows}) / 結びつかない商品コード ${out.manifest.unresolved_code_count} / 曖昧 ${out.manifest.ambiguous_code_count}${b.ignoredBeforeBaseline ? ` / 最初の写しより前の履歴 ${b.ignoredBeforeBaseline} 行は使わない` : ''}` : '';
    if (!out.lockedBy) {
      out.lastLine = err ? `❌ Company DB 観測の原価: ${String(err.message).replace(/\s+/g, ' ').slice(0, 300)}${out.generation ? ` (世代 ${out.generation})` : ''}`
        : out.status === 'not_migrated' ? '⏭️ Company DB 観測の原価: Render に migration 0046 がまだ無い = 送らない (中原さんの指示で migrate した後に送る)'
        : out.status === 'dry-run' ? `dry-run: 送る予定 行 ${out.rows}${tail} / Render の今の世代 ${out.remote ? out.remote.generation : 'なし'} [dry-run = 送っていない]`
        : out.status === 'unchanged' ? `✅ Company DB 観測の原価: 変わりなし (Render の世代 ${out.generation} と同じ中身・送らない) 行 ${out.rows}${tail}`
        : `✅ Company DB 観測の原価: ${out.status === 'same' ? '送り直し (same)' : '入れ替えた'} 世代 ${out.generation}${out.reusedGeneration ? ' (応答が失われた前の回と同じ世代)' : ''} 行 ${out.rows}${tail}`;
    }
    if (L) {
      try {
        L.recordRun({ run_id: runId, mode: 'full', started_at: startedAt.toISOString(), finished_at: now().toISOString(), batch_seq: out.generation, scanned: out.built ? out.built.periods : null, in_scope: out.built ? out.built.codes : null,
          changed: out.rows, sent: out.status === 'applied' || out.status === 'same' ? out.rows : 0, applied: out.status === 'applied' ? 1 : 0, same: out.status === 'same' ? 1 : 0, stale: out.status === 'stale' ? 1 : 0,
          failed: err ? 1 : 0, transform_errors: 0, ok: out.ok ? 1 : 0, note: err ? String(err.message).slice(0, 300) : out.status });
      } catch { /* 記録は補助 */ }
      L.releaseLock(owner);
      if (ownLedger) L.close();
    }
    if (err) err.result = out;   // CLI が要約の 1 行と数を出せるように
  }
}

export function parseArgs(argv) {
  const out = { send: false, dryRun: false, dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--send') out.send = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--data-dir') { const v = argv[++i]; if (v === undefined || v.startsWith('--')) throw new Error('--data-dir に値が無い'); out.dataDir = v; }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.send === out.dryRun) throw new Error('--send か --dry-run のどちらか 1 つを付ける');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = a.dataDir || process.env.DATA_DIR || path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..', 'data');
    const file = path.join(dataDir, 'warehouse.db');
    if (!fs.existsSync(file)) throw new Error(`warehouse.db が無い: ${file} (--data-dir か DATA_DIR)`);
    const db = new Database(file, { readonly: true, fileMustExist: true });
    let r;
    try { r = await pushSkuCostObserved({ warehouse: db, dataDir, base: syncBase(), syncKey: process.env.MIRROR_SYNC_KEY || '', dryRun: a.dryRun }); }
    catch (e) { r = e.result || { ok: false, lastLine: `❌ Company DB 観測の原価: ${String(e.message).replace(/\s+/g, ' ').slice(0, 300)}` }; }
    finally { db.close(); }
    if (r.built) console.log(`[company-db sku-cost-observed] 最初の写し ${r.built.baselineAt} (JST ${r.built.baselineDay}) / 履歴の商品コード ${r.built.codes} / 期間 ${r.built.periods} / 曖昧の例 ${r.built.ambiguousExamples.join(', ') || '-'} / 結びつかない例 ${r.built.unresolvedExamples.join(', ') || '-'}`);
    console.log(String(r.lastLine).replace(/\s+/g, ' '));   // 最後の 1 行を複数行にしない (daily-sync が朝の通知に載せる)
    code = r.ok && !r.lockedBy ? 0 : 1;   // 見送り (別の送り手) も失敗 = retry に載る (ほかの送り手と同じ)
  } catch (e) {
    console.log(`❌ Company DB 観測の原価: ${String(e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
  }
  // 🚨 fetch の直後に process.exit() しない (Windows の Node で終了コード 127。stock-daily.mjs と同じ)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
