/**
 * pipeline.mjs — miniPC から Company DB (Render) へ「行の集合」を送る共通の流れ。伝票 (ne-shipments.mjs) と注文 (mall-orders.mjs) が使う。D5a / D5b
 *
 * 流れ (Codex R1〜R4 で固めた約束。詳細は db/company/README.md「出荷を毎日送る」):
 *   lock (pid + 心拍) → Render の状態 (受領記録の照合・追跡対象の取り戻し・世代の補正・件数の見張り)
 *   → ① raw を 1 つの読み取り取引で流し読みし、指紋 (整形の版 + header.content_hash + 明細) が台帳と違う行だけを outbox に書く (raw の snapshot はここで閉じる)
 *   → ② outbox から chunk (行数・明細数・バイト数の上限) を取って POST → applied / same を送付済みに書き、送った行を outbox から消し、受領記録を残す (持ち主の確認と同じ取引)
 *   → lock を外す。失敗・stale・整形できない行が 1 つでもあれば ok = false
 *
 * 種類ごとに渡すもの:
 *   kind / label / paths { post, status, receipt, keys } / countOf(statusJson) → { count, maxBatchSeq } / keysOf(keysJson) → string[]
 *   iterate(warehouse, stats) = raw を鍵順に流し読みする generator (呼ぶ側の読み取り取引の中で動く。stats は自由に使ってよい)
 *   inScope(group, fps) / build(group, ctx) → { key, payload, n_lines, no_synced_at? } (throw = 整形できない) / transformVersion
 */
import crypto from 'node:crypto';
import { canonicalJson } from './ne-shipments-transform.mjs';
import { LockLostError } from './ledger.mjs';
import { payloadChecksum } from '../ingest/chunk.mjs';

export const DEFAULT_CHUNK = 200;
export const MAX_CHUNK = 1000;
export const MIN_SPLIT = 25;                         // 期限超過で割るときの下限
export const MAX_BODY_BYTES = 8 * 1024 * 1024;       // 1 chunk の JSON (受け口の parser は 12MB)
export const MAX_LINES_PER_ROW = 500;
export const MAX_LINES_PER_CHUNK = 5000;
export const HTTP_TIMEOUT_MS = 120000;
export const RETRIES = 6;                            // 5xx / 通信エラーの再送 (5・10・20・40・80 秒 = 合計 155 秒。master へのマージで Render が再デプロイされる 1〜3 分の 502 をまたぐ)
export const backoffMs = (attempt) => 5000 * 2 ** (attempt - 1);
const HEARTBEAT_EVERY = 5000;                        // 走査中の心拍 (行数)
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function newRunId(now = new Date()) {
  return `ship_${now.toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}
/** 行の指紋 = 整形の版 + ヘッダの content_hash + 明細 (台帳と比べるためだけ。版が変われば全部送り直す) */
export function fingerprintOf(transformVersion, payload) {
  return crypto.createHash('sha256').update(transformVersion + '|' + payload.header.content_hash + '|' + canonicalJson(payload.lines)).digest('hex').slice(0, 32);
}

async function getJson(fetchImpl, url, syncKey, what) {
  const res = await fetchImpl(url, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`${what}が取れない: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  return res.json();
}
/** 前回受領確認した chunk が Render に同じ内容で残っているか */
export async function fetchReceiptFound(fetchImpl, { base, syncKey, path, receipt }) {
  const j = await getJson(fetchImpl, `${base}${path}?run_id=${encodeURIComponent(receipt.run_id)}&chunk_index=${receipt.chunk_index}`, syncKey, 'Render の受領記録');
  if (!j || typeof j.found !== 'boolean') throw new Error('Render の受領記録の応答に found が無い');
  return j.found && j.payload_checksum === receipt.payload_checksum;
}
/** Render にある鍵を全部 (keyset で数回に分けて) */
export async function fetchAllKeys(fetchImpl, { base, syncKey, path, keysOf, limit = 20000 }) {
  const out = []; let after = '';
  const sep = path.includes('?') ? '&' : '?';
  for (let i = 0; i < 1000; i++) {
    const j = await getJson(fetchImpl, `${base}${path}${sep}after=${encodeURIComponent(after)}&limit=${limit}`, syncKey, 'Render の鍵');
    const keys = keysOf(j);
    if (!Array.isArray(keys)) throw new Error('Render の鍵の応答に一覧が無い');
    out.push(...keys);
    if (!j.next) return out;
    after = j.next;
  }
  throw new Error('Render の鍵が多すぎる (1,000 ページ超)');
}

export async function postJson(fetchImpl, { base, syncKey, path, body, log, sleep = defaultSleep, beforeAttempt = () => {} }) {
  let lastErr = null;
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    beforeAttempt();   // 再送の直前にも持ち主を確かめる (奪われていたら送らない)
    try {
      const res = await fetchImpl(`${base}${path}`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': syncKey }, body: JSON.stringify(body), signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
      });
      const text = await res.text();
      let json = null; try { json = JSON.parse(text); } catch { /* 本文が JSON でない */ }
      if (res.ok) {
        if (!json || typeof json !== 'object') throw new Error('応答が JSON でない');
        return json;
      }
      const msg = `HTTP ${res.status} ${json && json.error ? json.error : text.slice(0, 200)}`;
      if (res.status === 503 && json && json.code === 'CHUNK_DEADLINE') return { deadline: true, message: msg };   // 割って送り直す (再送はしない)
      if (res.status >= 400 && res.status < 500 && res.status !== 429) throw Object.assign(new Error(msg), { fatal: true });   // 直しても再送では通らない
      lastErr = new Error(msg);
    } catch (e) {
      if (e.fatal || e.code === 'LOCK_LOST') throw e;
      lastErr = e;
    }
    if (attempt < RETRIES) { log(`  送信に失敗 (${String(lastErr.message).replace(/\s+/g, ' ').slice(0, 120)})。${backoffMs(attempt) / 1000} 秒後に再送 (${attempt}/${RETRIES})`); await sleep(backoffMs(attempt)); }
  }
  throw lastErr;
}

/**
 * 送る (本体)。戻り値 = { ok, mode, dryRun, runId, batchSeq, scanned, inScope, unchanged, changed, sent, applied, same, stale, failed[], staleKeys[], chunks, transformErrors[], noSyncedAt, lockedBy, remote, ledgerReset, ledgerRebuilt, carriedOver, stats }
 * 差し替え (試験): fetchImpl / now / sleep / pid / isAlive
 */
export async function runPush({
  kind, label, warehouse, ledger, fetchImpl = fetch, base, syncKey, paths, countOf, keysOf, iterate, inScope, build, transformVersion, mode = 'incremental', scopeLabel = '',
  chunkSize = DEFAULT_CHUNK, dryRun = false, force = false, log = console.log, now = () => new Date(), sleep = defaultSleep, owner = `pid:${process.pid}`, pid = process.pid, isAlive = undefined,
  minSplit = MIN_SPLIT, maxBodyBytes = MAX_BODY_BYTES, stats = {},
}) {
  if (chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk は 1〜${MAX_CHUNK}`);
  if (!dryRun) {
    if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL を確かめる。https で同じホストのときだけ)');
    if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  }
  const startedAt = now();
  const r = { ok: false, kind, mode, dryRun, force, runId: null, batchSeq: null, scanned: 0, inScope: 0, unchanged: 0, changed: 0, sent: 0, applied: 0, same: 0, stale: 0, failed: [], staleKeys: [], chunks: 0,
    transformErrors: [], noSyncedAt: 0, lockedBy: null, example: null, remote: null, ledgerReset: null, ledgerRebuilt: 0, carriedOver: 0, stats };
  if (!dryRun) {
    const lock = ledger.acquireLock({ owner, pid, now: startedAt, ...(isAlive ? { isAlive } : {}) });
    if (!lock.ok) { r.lockedBy = lock.held; log(`[company-db push ${label}] 別の送り手が走っている (${lock.held.owner} pid ${lock.held.pid} 心拍 ${lock.held.heartbeat_at}) ので見送った`); return r; }
  }
  const mustOwn = () => { if (!ledger.renewLock(owner, now())) throw new LockLostError(); };
  let fps;
  try {
    if (!dryRun) {
      r.runId = newRunId(startedAt); ledger.recordRun({ run_id: r.runId, mode, started_at: startedAt.toISOString() });
      const statusJson = await getJson(fetchImpl, `${base}${paths.status}`, syncKey, 'Render の状態');
      mustOwn();   // HTTP を待った後は必ず持ち主を確かめてから台帳に書く (Codex R3 #4 / R4 #1)
      r.remote = countOf(statusJson);
      if (!Number.isInteger(r.remote.count)) throw new Error('Render の状態の応答に件数が無い');
      if (r.remote.maxBatchSeq != null) ledger.ensureBatchSeqAtLeast(r.remote.maxBatchSeq, startedAt);   // 世代は Render の最大以上 (台帳を失くしても 'stale' で全部弾かれない)
      const receipt = ledger.getLastReceipt();
      if (receipt) {
        const found = await fetchReceiptFound(fetchImpl, { base, syncKey, path: paths.receipt, receipt });
        mustOwn();
        if (!found) {
          const n = ledger.resetFingerprints(owner);
          r.ledgerReset = `receipt_missing:${receipt.run_id}/${receipt.chunk_index}`;
          log(`[company-db push ${label}] 前回の受領記録 (${receipt.run_id} chunk ${receipt.chunk_index}) が Render に無い → Render が復元・作り直されたとみなし、台帳の指紋 ${n} 件を空にして全部送り直す`);
        }
      }
      if (!ledger.isInitialized() && ledger.countTracked() === 0 && r.remote.count > 0) {
        const keys = await fetchAllKeys(fetchImpl, { base, syncKey, path: paths.keys, keysOf });
        mustOwn();
        r.ledgerRebuilt = ledger.trackKeys(keys, startedAt, owner);
        log(`[company-db push ${label}] 台帳が空なので Render の投入済み ${keys.length} 件を追跡対象に取り戻した (指紋は空 = 全部送り直す)`);
      }
      const co = ledger.carryOverOutbox(owner, startedAt);
      if (co.leftover) { r.carriedOver = co.carried; log(`[company-db push ${label}] 前回送らずに残った ${co.leftover} 件を追跡対象に引き継いだ`); }
      const confirmed = ledger.countConfirmed();
      if (r.remote.count < confirmed) throw new Error(`Render の${label} ${r.remote.count} 件 < 台帳の送付確認済み ${confirmed} 件 = 説明のつかない食い違い。Render と台帳を確かめてから --reset-ledger で指紋を空にして送り直す`);
      ledger.markInitialized(startedAt);
      mustOwn();
    }
    fps = ledger.loadFingerprints();
    // ── ① raw を 1 つの読み取り取引で流し読み (snapshot はここで閉じる。HTTP の間は持たない。Codex R2 #6) ──
    let buf = [];
    const flushBuf = () => { if (buf.length) { ledger.pushOutbox(r.runId, buf); buf = []; } };
    const ctx = { startedAt, fps };
    warehouse.exec('begin');
    try {
      for (const group of iterate(warehouse, stats)) {
        r.scanned++;
        if (!dryRun && r.scanned % HEARTBEAT_EVERY === 0) mustOwn();   // 走査中も心拍 (Codex R3 #4)
        if (!inScope(group, fps)) continue;
        r.inScope++;
        let item;
        try {
          item = build(group, ctx);
          if (item.payload.lines.length > MAX_LINES_PER_ROW) throw new Error(`明細が ${item.payload.lines.length} 行 (上限 ${MAX_LINES_PER_ROW})`);
        } catch (e) { r.transformErrors.push({ key: group.key, error: e.message }); continue; }
        if (item.no_synced_at) r.noSyncedAt++;
        const fp = fingerprintOf(transformVersion, item.payload);
        if (!force && fps.get(item.key) === fp) { r.unchanged++; continue; }
        r.changed++;
        if (!r.example) r.example = item.payload;
        if (dryRun) continue;
        const payload = JSON.stringify(item.payload);
        buf.push({ key: item.key, fp, payload, n_lines: item.payload.lines.length, n_bytes: Buffer.byteLength(payload) });
        if (buf.length >= 1000) flushBuf();
      }
      flushBuf();
    } finally {
      try { warehouse.exec('rollback'); } catch { /* 読むだけの取引 */ }
    }
    log(`[company-db push ${label}] ${scopeLabel}: 読んだ ${r.scanned} / 範囲 ${r.inScope} / 変化なし ${r.unchanged} / 変わった ${r.changed} (整形できない ${r.transformErrors.length} / 更新時刻無し ${r.noSyncedAt})${dryRun ? ' [dry-run]' : ''}`);
    for (const t of r.transformErrors.slice(0, 20)) log(`  整形できない: ${t.key}: ${t.error}`);
    if (dryRun) { if (r.example) log(`  例: ${JSON.stringify(r.example).slice(0, 600)}`); r.ok = r.transformErrors.length === 0; return r; }

    // ── ② outbox から chunk を取って送る ──
    const total = ledger.countOutbox(r.runId);
    let chunkIndex = 0;
    const sendRows = async (rows, last) => {
      if (r.batchSeq == null) r.batchSeq = ledger.nextBatchSeq(now(), owner);   // 世代は最初の chunk の直前に取る (送る物が無い run では進めない。持ち主の確認と同じ取引)
      const items = rows.map((p) => p.item);
      const body = { run_id: r.runId, batch_seq: r.batchSeq, chunk_index: chunkIndex++, last, transform_version: transformVersion, rows: items };
      const res = await postJson(fetchImpl, { base, syncKey, path: paths.post, log, sleep, body, beforeAttempt: mustOwn });
      if (res.deadline) {
        chunkIndex--;                                                                // 使わなかった番号を戻す
        if (rows.length <= minSplit) throw new Error(`${res.message} (${rows.length} 件でも期限超過)`);
        const half = Math.ceil(rows.length / 2);
        log(`  期限超過 (${rows.length} 件) → ${half} + ${rows.length - half} に割って送り直す`);
        await sendRows(rows.slice(0, half), false);
        await sendRows(rows.slice(half), last);
        return;
      }
      for (const k of ['applied', 'same', 'stale']) { if (!Number.isInteger(res[k])) throw new Error(`応答に ${k} が無い`); r[k] += res[k]; }
      const staleKeys = res.stale_keys ?? res.stale_slips;
      if (!Array.isArray(res.failed) || !Array.isArray(staleKeys)) throw new Error('応答に failed / stale_keys が無い');
      if (res.applied + res.same + res.stale + res.failed.length !== rows.length) throw new Error(`chunk ${body.chunk_index}: 送った ${rows.length} と応答の合計 ${res.applied + res.same + res.stale + res.failed.length} が合わない`);
      const failedKeys = res.failed.map((f) => f.key ?? f.ne_slip_no);
      r.failed.push(...res.failed.map((f, i) => ({ ...f, key: failedKeys[i] }))); r.staleKeys.push(...staleKeys);
      const skip = new Set([...failedKeys, ...staleKeys]);
      // 受領記録の指紋は受け口と同じ計算 (validateChunk が正規化した rows = key を含む) → 送る行に key を足して計算
      const normalized = rows.map((p) => ({ key: p.key, ...p.item }));
      ledger.ackOutbox(rows, rows.filter((p) => !skip.has(p.key)).map((p) => ({ key: p.key, fp: p.fp })), r.batchSeq,
        { owner, at: now(), receipt: { run_id: r.runId, chunk_index: body.chunk_index, payload_checksum: payloadChecksum(normalized) } });
      r.sent += rows.length; r.chunks++;
      if (r.chunks % 10 === 0 || last) log(`  chunk ${body.chunk_index}${last ? ' (last)' : ''}: 累計 sent ${r.sent} applied ${r.applied} same ${r.same} stale ${r.stale} failed ${r.failed.length}`);
    };
    let after = 0;
    while (true) {
      const taken = ledger.takeOutbox(r.runId, after, { maxRows: chunkSize, maxLines: MAX_LINES_PER_CHUNK, maxBytes: maxBodyBytes });
      if (!taken.length) break;
      const rows = taken.map((x) => ({ seq: x.seq, key: x.key, fp: x.fp, item: JSON.parse(x.payload) }));
      after = taken[taken.length - 1].seq;
      await sendRows(rows, after >= total.max_seq);
    }
    for (const f of r.failed.slice(0, 20)) log(`  失敗: ${f.key}: ${f.error}`);
    if (r.staleKeys.length) log(`  stale (Render のほうが新しい世代): ${r.staleKeys.length} 件 = 世代がずれている (次回また送る。続くなら台帳と Render を確かめる)`);
    r.ok = r.failed.length === 0 && r.transformErrors.length === 0 && r.stale === 0;
    if (total.n >= 50000) { try { ledger.vacuum(); } catch { /* 詰めるだけ */ } }
    return r;
  } catch (e) {
    r.error = e.message;
    throw e;
  } finally {
    if (!dryRun) {
      if (r.runId) ledger.recordRun({ run_id: r.runId, mode, started_at: startedAt.toISOString(), finished_at: now().toISOString(), batch_seq: r.batchSeq, scanned: r.scanned, in_scope: r.inScope, changed: r.changed,
        sent: r.sent, applied: r.applied, same: r.same, stale: r.stale, failed: r.failed.length, transform_errors: r.transformErrors.length, ok: r.ok ? 1 : 0, note: r.error ? String(r.error).slice(0, 300) : null });
      ledger.releaseLock(owner);
    }
  }
}

/** 1 行の要約 (daily-sync は最後の行を朝の通知に載せる) */
export function summarizePush(r, label, unit = '件') {
  if (r.lockedBy) return `⏸️ Company DB ${label} push: 別の送り手が走っているので見送り (${r.lockedBy.owner} pid ${r.lockedBy.pid} 心拍 ${r.lockedBy.heartbeat_at})`;
  if (r.dryRun) return `dry-run: 読んだ ${r.scanned} / 範囲 ${r.inScope} / 変わった ${r.changed} (整形できない ${r.transformErrors.length})`;
  const head = r.ok ? '✅' : '❌';
  return `${head} Company DB ${label} push: 変わった ${r.changed} ${unit}を送った (applied ${r.applied} / same ${r.same} / stale ${r.stale}${r.stale ? ' ⚠️世代ずれ' : ''} / failed ${r.failed.length} / 整形できない ${r.transformErrors.length})`
    + ` 世代 ${r.batchSeq ?? '-'} chunk ${r.chunks} / 範囲 ${r.inScope} ${unit}のうち変化なし ${r.unchanged}`
    + (r.ledgerReset ? ` / ⚠️Render が復元されていたので台帳の指紋を空にして送り直した (${r.ledgerReset})` : '')
    + (r.ledgerRebuilt ? ` / ⚠️台帳が空だったので Render から ${r.ledgerRebuilt} ${unit}を取り戻した` : '')
    + (r.carriedOver ? ` / 前回の残り ${r.carriedOver} ${unit}を引き継ぎ` : '');
}

/** from〜to を days 日以内の窓に分ける (Render の受け口は 400 日まで) */
export function splitWindows(from, to, days = 366) {
  const out = [];
  let a = from;
  while (a <= to) {
    const end = new Date(Date.parse(`${a}T00:00:00Z`) + (days - 1) * 86400000).toISOString().slice(0, 10);
    const b = end < to ? end : to;
    out.push([a, b]);
    a = new Date(Date.parse(`${b}T00:00:00Z`) + 86400000).toISOString().slice(0, 10);
  }
  return out;
}
export const isDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
export function jstDate(offsetDays = 0) { return new Date(Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000).toISOString().slice(0, 10); }
