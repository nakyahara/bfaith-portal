#!/usr/bin/env node
/**
 * ne-shipments.mjs — miniPC の NE 伝票 (warehouse.db の raw_ne_order_base + raw_ne_orders) を Company DB (Render Postgres) に送る。D5a (08 §4.7 / §9 D5)
 *
 * 何をするか:
 *   ① 台帳 (DATA_DIR/company-db-push.db = ledger.mjs) の「送付済みの指紋」を読み、raw を伝票番号順に**全部**流し読みして (ヘッダ + 明細を伝票ごとにまとめる。1 つの読み取り取引)、
 *      整形 (ne-shipments-transform.mjs) → 指紋を比べ、**指紋が変わった伝票だけ**を台帳の outbox に書く (時刻には頼らない。PR #1336 Codex R1 #1〜#3)。
 *      範囲 = 受注日か出荷確定日が 2025-01-01 以降 (D-28) **または 追跡中 (投入済み)** (出荷確定日を消されても追跡する)。raw の snapshot はここで閉じる (HTTP の間は持たない。R2 #6)
 *   ② 世代 (台帳の batch_seq。最初の chunk の直前に取引の中で +1。Render の最大世代以上に補正してから) を付け、outbox から chunk (伝票 200 / 明細 5,000 / 8MB) を取って
 *      Render の POST /apps/company-db/sync/shipments へ送る (x-sync-key。Render 側は 1 chunk = 1 取引で core.apply_shipment_batch() を呼び、失敗した伝票と stale の伝票を返す。
 *      期限超過 (503 CHUNK_DEADLINE) なら半分に割って送り直す)
 *   ③ 'applied' / 'same' が返った伝票の指紋を台帳に書き、送った行を outbox から消し、受領記録 (run_id / chunk_index / 内容の指紋) を残す (1 取引)。failed / stale は書かない = 次回また送る。
 *      失敗・stale・整形できない伝票が 1 つでもあれば exit 1 (朝の通知に ❌)
 *   ④ 送り手の排他 = 台帳の lock (持ち主・pid・心拍。pid が生きていて心拍が 15 分以内のときだけ拒む = daily-sync に 30 分で殺された送り手の lock は残らない)。
 *      走査の途中・送る前・台帳に書く取引の中で持ち主を確かめ、奪われていたら止める
 *   ⑤ 台帳と Render の食い違い (R3): run の最初に Render の状態を取り (a) 前回の受領記録が Render に無ければ「復元・作り直し」とみなして指紋を空にし全部送り直す
 *      (b) 台帳が空で Render に伝票があれば (台帳を失くした) Render から投入済みの伝票番号を取り戻して追跡対象にする (c) 前回送らずに残った outbox の伝票番号も追跡対象に引き継ぐ
 *      (d) Render の伝票数 < 送付確認済み なら止める (説明のつかない食い違い)
 *   --reconcile: 旧 f_shipments_daily (miniPC) と mart.v_shipments_daily (Render) を 日 × 店舗 × 配送方法 で突き合わせる (08 §9 D5 の「f_shipments_daily との突合」)
 *
 * 使い方 (miniPC。daily-sync の 1 ステップ = NE 取得 → 出荷サマリ再構築 の後):
 *   node apps/company-db/push/ne-shipments.mjs --incremental                       → 範囲の伝票のうち指紋が変わったもの (初回 = 2025-01-01 以降の全部)
 *   node apps/company-db/push/ne-shipments.mjs --from 2025-01-01 --to 2025-01-31   → 受注日の範囲だけ (初回のバックフィルを月ごとに。台帳に書くので後の --incremental は残りだけ送る)
 *   node apps/company-db/push/ne-shipments.mjs --incremental --dry-run            → 送らずに件数と例だけ
 *   node apps/company-db/push/ne-shipments.mjs --incremental --force              → 指紋が同じでも送る (Render 側を疑うとき。'same' が返るだけ)
 *   node apps/company-db/push/ne-shipments.mjs --reconcile --days 90              → 突合 (差があれば exit 1)。--from/--to で任意の期間 (366 日ごとに分けて問い合わせる)。--all で 2025-01-01 から今日まで
 *   node apps/company-db/push/ne-shipments.mjs --reset-ledger                     → 台帳の指紋を空にする (Render を復元・作り直したとき。次の --incremental で全部送り直す。自動でも見つける)
 *
 * env: DATA_DIR (warehouse.db と台帳の場所。--data-dir でも可) / RENDER_MIRROR_URL (送り先の origin をここから取る。RENDER_PORTAL_URL があれば同じホストのときだけ優先) /
 *      MIRROR_SYNC_KEY (x-sync-key) / CDB_PUSH_CHUNK (1 chunk の伝票数。既定 200、上限 1000)
 *
 * 🚨 秘密は表示しない。🚨 daily-sync の runScript は引数が無いと '7' を足すので、必ず --incremental などの引数を付けて呼ぶ。
 * 🚨 warehouse.db は読むだけ (表を足さない・書かない)。台帳は別ファイル (作り直せる写し = バックアップの対象にしない)
 */
import 'dotenv/config';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { buildShipment, canonicalJson, TRANSFORM_VERSION } from './ne-shipments-transform.mjs';
import { openLedger, LockLostError } from './ledger.mjs';
import { MAX_LINES_PER_SLIP, MAX_LINES_PER_CHUNK, payloadChecksum } from '../ingest/shipments.mjs';
import { baseOrigin } from '../../../scripts/company-db/remote-load.mjs';

export const DEFAULT_FLOOR = '2025-01-01';          // D-28: 2025-01-01 以降の注文 (+ その期間に出荷した古い注文)
export const DEFAULT_CHUNK = 200;
export const MAX_CHUNK = 1000;
export const MIN_SPLIT = 25;                         // 期限超過で割るときの下限
export const MAX_BODY_BYTES = 8 * 1024 * 1024;       // 1 chunk の JSON (受け口の parser は 12MB)
const HTTP_TIMEOUT_MS = 120000;
const RETRIES = 3;
const RECONCILE_WINDOW_DAYS = 366;
const HEARTBEAT_EVERY = 5000;                        // 走査中の心拍 (伝票数)

export function newRunId(now = new Date()) {
  return `ship_${now.toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}
const isDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 伝票の指紋 = 整形の版 + ヘッダの content_hash + 明細 (台帳と比べるためだけ。Render の lines_checksum とは別物でよい。版が変われば全部送り直す) */
export function fingerprint(item) {
  return crypto.createHash('sha256').update(TRANSFORM_VERSION + '|' + item.header.content_hash + '|' + canonicalJson(item.lines)).digest('hex').slice(0, 32);
}

/** Render の Company DB 同期 API の base ('https://host/apps/company-db/sync')。取れなければ '' */
export function syncBase(env = process.env) {
  const o = baseOrigin(env);
  return o ? `${o}/apps/company-db/sync` : '';
}

/**
 * raw を伝票番号順に流し読みして、伝票ごとに { base, lines } を返す generator (同じ接続の 2 つの statement を merge。呼ぶ側が読み取り取引で包む)。
 * ヘッダの無い明細は onLinesWithoutBase(伝票番号) に渡す
 */
export function* iterateSlips(warehouse, { onLinesWithoutBase = () => {} } = {}) {
  const bases = warehouse.prepare('select * from raw_ne_order_base order by 伝票番号').iterate();
  const lines = warehouse.prepare('select * from raw_ne_orders order by 伝票番号, 明細行番号').iterate();
  let cur = lines.next();
  try {
    for (const b of bases) {
      const slip = String(b.伝票番号);
      while (!cur.done && String(cur.value.伝票番号) < slip) { onLinesWithoutBase(String(cur.value.伝票番号)); cur = lines.next(); }
      const ls = [];
      while (!cur.done && String(cur.value.伝票番号) === slip) { ls.push(cur.value); cur = lines.next(); }
      yield { base: b, lines: ls };
    }
    while (!cur.done) { onLinesWithoutBase(String(cur.value.伝票番号)); cur = lines.next(); }
  } finally {
    if (typeof bases.return === 'function') bases.return();
    if (typeof lines.return === 'function') lines.return();
  }
}

async function getJson(fetchImpl, url, syncKey, what) {
  const res = await fetchImpl(url, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`${what}が取れない: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  return res.json();
}
/** Render の状態 (GET /shipments/status) → { max_batch_seq, shipments }。取れなければ例外 (状態が分からないまま送らない) */
export async function fetchStatus(fetchImpl, { base, syncKey }) {
  const j = await getJson(fetchImpl, `${base}/shipments/status`, syncKey, 'Render の状態');
  if (!j || !j.counts || !Number.isInteger(j.counts.shipments)) throw new Error('Render の状態の応答に counts.shipments が無い');
  return { max_batch_seq: j.counts.max_batch_seq == null ? null : Number(j.counts.max_batch_seq), shipments: j.counts.shipments };
}
/** 前回受領確認した chunk が Render に同じ内容で残っているか */
export async function fetchReceiptFound(fetchImpl, { base, syncKey, receipt }) {
  const j = await getJson(fetchImpl, `${base}/shipments/receipt?run_id=${encodeURIComponent(receipt.run_id)}&chunk_index=${receipt.chunk_index}`, syncKey, 'Render の受領記録');
  if (!j || typeof j.found !== 'boolean') throw new Error('Render の受領記録の応答に found が無い');
  return j.found && j.payload_checksum === receipt.payload_checksum;
}
/** Render にある伝票番号を全部 (keyset で数回に分けて) */
export async function fetchAllSlips(fetchImpl, { base, syncKey, limit = 20000 }) {
  const out = []; let after = '';
  for (let i = 0; i < 1000; i++) {
    const j = await getJson(fetchImpl, `${base}/shipments/slips?after=${encodeURIComponent(after)}&limit=${limit}`, syncKey, 'Render の伝票番号');
    if (!j || !Array.isArray(j.slips)) throw new Error('Render の伝票番号の応答に slips が無い');
    out.push(...j.slips);
    if (!j.next) return out;
    after = j.next;
  }
  throw new Error('Render の伝票番号が多すぎる (1,000 ページ超)');
}

async function postChunk(fetchImpl, { base, syncKey, body, log, sleep = defaultSleep, beforeAttempt = () => {} }) {
  let lastErr = null;
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    beforeAttempt();   // 再送の直前にも持ち主を確かめる (奪われていたら送らない)
    try {
      const res = await fetchImpl(`${base}/shipments`, {
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
    if (attempt < RETRIES) { log(`  送信に失敗 (${lastErr.message})。${attempt * 5} 秒後に再送 (${attempt}/${RETRIES})`); await sleep(attempt * 5000); }
  }
  throw lastErr;
}

/**
 * 送る (本体)。戻り値 = { ok, mode, dryRun, runId, batchSeq, scanned, inScope, unchanged, changed, sent, applied, same, stale, failed[], staleSlips[], chunks, transformErrors[], linesWithoutBase, noSyncedAt, lockedBy, remote, ledgerReset, ledgerRebuilt, carriedOver }
 * 差し替え (試験): fetchImpl / now / sleep / pid / isAlive。warehouse = better-sqlite3 (読むだけ)、ledger = openLedger()
 *
 * 流れ: lock → Render の状態 (受領記録の照合・追跡対象の取り戻し・世代の補正・伝票数の見張り) → ① raw を 1 つの読み取り取引で流し読みして、変わった伝票を台帳の outbox に書く (raw の snapshot はここで閉じる)
 *       → ② outbox から chunk (伝票数・明細数・バイト数の上限) を取って送る → applied / same を送付済みに書き、送った行を outbox から消し、受領記録を残す (持ち主の確認と同じ取引) → lock を外す
 */
export async function pushShipments({ warehouse, ledger, fetchImpl = fetch, base, syncKey, chunkSize = DEFAULT_CHUNK, dryRun = false, force = false, floor = DEFAULT_FLOOR, from = null, to = null,
  log = console.log, now = () => new Date(), sleep = defaultSleep, owner = `pid:${process.pid}`, pid = process.pid, isAlive = undefined, minSplit = MIN_SPLIT, maxBodyBytes = MAX_BODY_BYTES }) {
  const mode = from && to ? 'range' : 'incremental';
  if (chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk は 1〜${MAX_CHUNK}`);
  if (!dryRun) {
    if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL を確かめる。https で同じホストのときだけ)');
    if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  }
  const startedAt = now();
  const r = { ok: false, mode, dryRun, force, runId: null, batchSeq: null, scanned: 0, inScope: 0, unchanged: 0, changed: 0, sent: 0, applied: 0, same: 0, stale: 0, failed: [], staleSlips: [], chunks: 0,
    transformErrors: [], linesWithoutBase: 0, noSyncedAt: 0, lockedBy: null, example: null, remote: null, ledgerReset: null, ledgerRebuilt: 0, carriedOver: 0 };
  if (!dryRun) {
    const lock = ledger.acquireLock({ owner, pid, now: startedAt, ...(isAlive ? { isAlive } : {}) });
    if (!lock.ok) { r.lockedBy = lock.held; log(`[company-db push] 別の送り手が走っている (${lock.held.owner} pid ${lock.held.pid} 心拍 ${lock.held.heartbeat_at}) ので見送った`); return r; }
  }
  const mustOwn = () => { if (!ledger.renewLock(owner, now())) throw new LockLostError(); };
  const noBase = new Set();
  const floorTs = `${floor} 00:00:00`;
  let fps;
  try {
    if (!dryRun) {
      r.runId = newRunId(startedAt); ledger.recordRun({ run_id: r.runId, mode, started_at: startedAt.toISOString() });
      // ── Render の状態と台帳の食い違い (Codex R2 #2 / R3 #1〜#3) ──
      r.remote = await fetchStatus(fetchImpl, { base, syncKey });
      mustOwn();   // HTTP を待つ間に奪われていたら、台帳に何も書かずに止める (Codex R3 #4)
      if (r.remote.max_batch_seq != null) ledger.ensureBatchSeqAtLeast(r.remote.max_batch_seq, startedAt);   // 世代は Render の最大以上 (台帳を失くしても 'stale' で全部弾かれない)
      const receipt = ledger.getLastReceipt();
      if (receipt && !(await fetchReceiptFound(fetchImpl, { base, syncKey, receipt }))) {
        // 前回受領確認した chunk が Render に無い = Render が過去に復元された・作り直された → 指紋を空にして全部送り直す (伝票番号 = 追跡対象は残す)
        const n = ledger.resetFingerprints();
        r.ledgerReset = `receipt_missing:${receipt.run_id}/${receipt.chunk_index}`;
        log(`[company-db push] 前回の受領記録 (${receipt.run_id} chunk ${receipt.chunk_index}) が Render に無い → Render が復元・作り直されたとみなし、台帳の指紋 ${n} 件を空にして全部送り直す`);
      }
      if (!ledger.isInitialized() && ledger.countTracked() === 0 && r.remote.shipments > 0) {
        // 新しい台帳なのに Render に伝票がある = 台帳を失くした → Render から投入済みの伝票番号を取り戻して追跡対象に (範囲の条件から外れた伝票も追える)
        const slips = await fetchAllSlips(fetchImpl, { base, syncKey });
        r.ledgerRebuilt = ledger.trackSlips(slips, startedAt);
        log(`[company-db push] 台帳が空なので Render の投入済み ${slips.length} 伝票を追跡対象に取り戻した (指紋は空 = 全部送り直す)`);
      }
      const leftover = ledger.outboxSlips();
      if (leftover.length) { r.carriedOver = ledger.trackSlips(leftover, startedAt); ledger.clearOutbox(); log(`[company-db push] 前回送らずに残った ${leftover.length} 伝票を追跡対象に引き継いだ`); }
      const confirmed = ledger.countConfirmed();
      if (r.remote.shipments < confirmed) throw new Error(`Render の伝票 ${r.remote.shipments} 件 < 台帳の送付確認済み ${confirmed} 件 = 説明のつかない食い違い。Render と台帳を確かめてから --reset-ledger で指紋を空にして送り直す`);
      ledger.markInitialized(startedAt);
      mustOwn();
    }
    fps = ledger.loadFingerprints();
    const inRange = (b) => (mode === 'range' ? (String(b.受注日 || '') >= `${from} 00:00:00` && String(b.受注日 || '') < `${to} 99`)
      : (String(b.受注日 || '') >= floorTs || String(b.出荷確定日 || '') >= floorTs || fps.has(String(b.伝票番号))));
    // ── ① raw を 1 つの読み取り取引で流し読み (snapshot はここで閉じる。HTTP の間は持たない。Codex R2 #6) ──
    let buf = [];
    const flushBuf = () => { if (buf.length) { ledger.pushOutbox(r.runId, buf); buf = []; } };
    warehouse.exec('begin');
    try {
      for (const { base: b, lines } of iterateSlips(warehouse, { onLinesWithoutBase: (s) => noBase.add(s) })) {
        r.scanned++;
        if (!dryRun && r.scanned % HEARTBEAT_EVERY === 0) mustOwn();   // 走査中も心拍 (Codex R3 #4)
        if (!inRange(b)) continue;
        r.inScope++;
        let item;
        try {
          item = buildShipment(b, lines, { fallbackSourceUpdatedAt: startedAt.toISOString() });
          if (item.lines.length > MAX_LINES_PER_SLIP) throw new Error(`明細が ${item.lines.length} 行 (上限 ${MAX_LINES_PER_SLIP})`);
        } catch (e) { r.transformErrors.push({ ne_slip_no: String(b.伝票番号), error: e.message }); continue; }
        if (item.no_synced_at) r.noSyncedAt++;
        const fp = fingerprint(item);
        if (!force && fps.get(item.ne_slip_no) === fp) { r.unchanged++; continue; }
        r.changed++;
        if (!r.example) r.example = { ne_slip_no: item.ne_slip_no, header: item.header, lines: item.lines };
        if (dryRun) continue;
        const payload = JSON.stringify({ ne_slip_no: item.ne_slip_no, header: item.header, lines: item.lines });
        buf.push({ ne_slip_no: item.ne_slip_no, fp, payload, n_lines: item.lines.length, n_bytes: Buffer.byteLength(payload) });
        if (buf.length >= 1000) flushBuf();
      }
      flushBuf();
    } finally {
      try { warehouse.exec('rollback'); } catch { /* 読むだけの取引 */ }
    }
    r.linesWithoutBase = noBase.size;
    log(`[company-db push] ${mode === 'range' ? `受注日 ${from}〜${to}` : `範囲 ${floor} 以降 + 追跡中`}: 読んだ ${r.scanned} / 範囲 ${r.inScope} / 変化なし ${r.unchanged} / 変わった ${r.changed}`
      + ` (整形できない ${r.transformErrors.length} / ヘッダ無しの明細 ${r.linesWithoutBase} 伝票 / synced_at 無し ${r.noSyncedAt})${dryRun ? ' [dry-run]' : ''}`);
    for (const t of r.transformErrors.slice(0, 20)) log(`  整形できない: ${t.ne_slip_no}: ${t.error}`);
    if (dryRun) { if (r.example) log(`  例: ${JSON.stringify(r.example).slice(0, 600)}`); r.ok = r.transformErrors.length === 0; return r; }

    // ── ② outbox から chunk を取って送る ──
    const total = ledger.countOutbox(r.runId);
    let chunkIndex = 0;
    const sendRows = async (rows, last) => {
      if (r.batchSeq == null) r.batchSeq = ledger.nextBatchSeq(now(), owner);   // 世代は最初の chunk の直前に取る (送る物が無い run では進めない。持ち主の確認と同じ取引)
      const items = rows.map((p) => p.item);
      const body = { run_id: r.runId, batch_seq: r.batchSeq, chunk_index: chunkIndex++, last, transform_version: TRANSFORM_VERSION, rows: items };
      const res = await postChunk(fetchImpl, { base, syncKey, log, sleep, body, beforeAttempt: mustOwn });
      if (res.deadline) {
        chunkIndex--;                                                                // 使わなかった番号を戻す
        if (rows.length <= minSplit) throw new Error(`${res.message} (${rows.length} 伝票でも期限超過)`);
        const half = Math.ceil(rows.length / 2);
        log(`  期限超過 (${rows.length} 伝票) → ${half} + ${rows.length - half} に割って送り直す`);
        await sendRows(rows.slice(0, half), false);
        await sendRows(rows.slice(half), last);
        return;
      }
      for (const k of ['applied', 'same', 'stale']) { if (!Number.isInteger(res[k])) throw new Error(`応答に ${k} が無い`); r[k] += res[k]; }
      if (!Array.isArray(res.failed) || !Array.isArray(res.stale_slips)) throw new Error('応答に failed / stale_slips が無い');
      if (res.applied + res.same + res.stale + res.failed.length !== rows.length) throw new Error(`chunk ${body.chunk_index}: 送った ${rows.length} と応答の合計 ${res.applied + res.same + res.stale + res.failed.length} が合わない`);
      r.failed.push(...res.failed); r.staleSlips.push(...res.stale_slips);
      const skip = new Set([...res.failed.map((f) => f.ne_slip_no), ...res.stale_slips]);
      ledger.ackOutbox(rows, rows.filter((p) => !skip.has(p.item.ne_slip_no)).map((p) => ({ ne_slip_no: p.item.ne_slip_no, fp: p.fp })), r.batchSeq,
        { owner, at: now(), receipt: { run_id: r.runId, chunk_index: body.chunk_index, payload_checksum: payloadChecksum(items) } });
      r.sent += rows.length; r.chunks++;
      if (r.chunks % 10 === 0 || last) log(`  chunk ${body.chunk_index}${last ? ' (last)' : ''}: 累計 sent ${r.sent} applied ${r.applied} same ${r.same} stale ${r.stale} failed ${r.failed.length}`);
    };
    let after = 0;
    while (true) {
      const taken = ledger.takeOutbox(r.runId, after, { maxRows: chunkSize, maxLines: MAX_LINES_PER_CHUNK, maxBytes: maxBodyBytes });
      if (!taken.length) break;
      const rows = taken.map((x) => ({ seq: x.seq, fp: x.fp, item: JSON.parse(x.payload) }));
      after = taken[taken.length - 1].seq;
      await sendRows(rows, after >= total.max_seq);
    }
    for (const f of r.failed.slice(0, 20)) log(`  失敗: ${f.ne_slip_no}: ${f.error}`);
    if (r.staleSlips.length) log(`  stale (Render のほうが新しい世代): ${r.staleSlips.length} 伝票 = 世代がずれている (次回また送る。続くなら台帳と Render を確かめる)`);
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
export function summarizeResult(r) {
  if (r.lockedBy) return `⏸️ Company DB 出荷 push: 別の送り手が走っているので見送り (${r.lockedBy.owner} pid ${r.lockedBy.pid} 心拍 ${r.lockedBy.heartbeat_at})`;
  if (r.dryRun) return `dry-run: 読んだ ${r.scanned} / 範囲 ${r.inScope} / 変わった ${r.changed} (整形できない ${r.transformErrors.length})`;
  const head = r.ok ? '✅' : '❌';
  return `${head} Company DB 出荷 push: 変わった ${r.changed} 伝票を送った (applied ${r.applied} / same ${r.same} / stale ${r.stale}${r.stale ? ' ⚠️世代ずれ' : ''} / failed ${r.failed.length} / 整形できない ${r.transformErrors.length})`
    + ` 世代 ${r.batchSeq ?? '-'} chunk ${r.chunks} / 範囲 ${r.inScope} 伝票のうち変化なし ${r.unchanged}`
    + (r.ledgerReset ? ` / ⚠️Render が復元されていたので台帳の指紋を空にして送り直した (${r.ledgerReset})` : '')
    + (r.ledgerRebuilt ? ` / ⚠️台帳が空だったので Render から ${r.ledgerRebuilt} 伝票を取り戻した` : '')
    + (r.carriedOver ? ` / 前回の残り ${r.carriedOver} 伝票を引き継ぎ` : '')
    + (r.linesWithoutBase ? ` / ヘッダ無しの明細 ${r.linesWithoutBase} 伝票` : '');
}

/**
 * 突合: miniPC の f_shipments_daily (旧表) と Render の mart.v_shipments_daily を 日 × 店舗 × 配送方法 で比べる (純粋な比較部分は diffDaily)
 */
export function diffDaily(localRows, remoteRows) {
  const key = (r) => `${r.ship_date}|${r.shop_code}|${r.delivery_id}`;
  const L = new Map(localRows.map((r) => [key(r), r])), R = new Map(remoteRows.map((r) => [key(r), r]));
  const mismatched = [], onlyLocal = [], onlyRemote = [];
  let matched = 0;
  for (const [k, l] of L) {
    const rr = R.get(k);
    if (!rr) { onlyLocal.push(l); continue; }
    const diffs = [];
    for (const f of ['slips', 'cancelled_slips']) if (Number(l[f]) !== Number(rr[f])) diffs.push(`${f} ${l[f]}≠${rr[f]}`);
    if (String(l.delivery_name ?? '') !== String(rr.delivery_name ?? '')) diffs.push(`delivery_name ${l.delivery_name}≠${rr.delivery_name}`);
    if (diffs.length) mismatched.push({ key: k, diffs }); else matched++;
  }
  for (const [k, rr] of R) if (!L.has(k)) onlyRemote.push(rr);
  return { compared: L.size + onlyRemote.length, matched, mismatched, onlyLocal, onlyRemote };
}

/** from〜to を RECONCILE_WINDOW_DAYS 日以内の窓に分ける (Render の受け口は 400 日まで) */
export function splitWindows(from, to, days = RECONCILE_WINDOW_DAYS) {
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

export async function reconcileShipmentsDaily({ warehouse, fetchImpl = fetch, base, syncKey, from, to, log = console.log }) {
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const total = { ok: true, windows: [], matched: 0, mismatched: [], onlyLocal: [], onlyRemote: [], localSlips: 0, remoteSlips: 0 };
  for (const [a, b] of splitWindows(from, to)) {
    const local = warehouse.prepare('select ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips from f_shipments_daily where ship_date >= ? and ship_date <= ?').all(a, b);
    const res = await fetchImpl(`${base}/shipments/daily?from=${a}&to=${b}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`Render の日次が取れない (${a}〜${b}): HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const remote = (await res.json()).rows;
    if (!Array.isArray(remote)) throw new Error('Render の応答に rows が無い');
    const d = diffDaily(local, remote);
    const localSlips = local.reduce((s, x) => s + Number(x.slips), 0), remoteSlips = remote.reduce((s, x) => s + Number(x.slips), 0);
    const ok = d.mismatched.length === 0 && d.onlyLocal.length === 0 && d.onlyRemote.length === 0;
    log(`[company-db reconcile] ${a}〜${b}: 行 miniPC ${local.length} / Render ${remote.length}、伝票 miniPC ${localSlips} / Render ${remoteSlips}、一致 ${d.matched} / 不一致 ${d.mismatched.length} / miniPC だけ ${d.onlyLocal.length} / Render だけ ${d.onlyRemote.length} ${ok ? '✅' : '❌'}`);
    for (const m of d.mismatched.slice(0, 30)) log(`  不一致 ${m.key}: ${m.diffs.join(', ')}`);
    for (const x of d.onlyLocal.slice(0, 10)) log(`  miniPC だけ ${x.ship_date}|${x.shop_code}|${x.delivery_id}: slips ${x.slips}`);
    for (const x of d.onlyRemote.slice(0, 10)) log(`  Render だけ ${x.ship_date}|${x.shop_code}|${x.delivery_id}: slips ${x.slips}`);
    total.windows.push({ from: a, to: b, ok, ...d, localSlips, remoteSlips });
    total.ok = total.ok && ok; total.matched += d.matched; total.mismatched.push(...d.mismatched); total.onlyLocal.push(...d.onlyLocal); total.onlyRemote.push(...d.onlyRemote);
    total.localSlips += localSlips; total.remoteSlips += remoteSlips;
  }
  log(`${total.ok ? '✅' : '❌'} 突合 ${from}〜${to}: ${total.ok ? '全部一致' : `差 ${total.mismatched.length + total.onlyLocal.length + total.onlyRemote.length} 行`} (伝票 miniPC ${total.localSlips} / Render ${total.remoteSlips}、${total.windows.length} 窓)`);
  return total;
}

function parseArgs(argv) {
  const out = { incremental: false, dryRun: false, force: false, reconcile: false, all: false, resetLedger: false, from: null, to: null, days: null, dataDir: null, chunk: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--incremental') out.incremental = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--force') out.force = true;
    else if (a === '--reconcile') out.reconcile = true;
    else if (a === '--all') out.all = true;
    else if (a === '--reset-ledger') out.resetLedger = true;
    else if (a === '--from') out.from = argv[++i];
    else if (a === '--to') out.to = argv[++i];
    else if (a === '--days') out.days = argv[++i];
    else if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--chunk') out.chunk = argv[++i];
    else throw new Error(`知らない引数: ${a}`);
  }
  return out;
}

function jstDate(offsetDays = 0) { return new Date(Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000).toISOString().slice(0, 10); }

async function main() {
  const a = parseArgs(process.argv.slice(2));
  const dataDir = (process.env.DATA_DIR || a.dataDir || '').trim();
  if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
  const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${a.chunk ?? process.env.CDB_PUSH_CHUNK} (1〜${MAX_CHUNK})`);
  if ((a.from && !a.to) || (!a.from && a.to)) throw new Error('--from と --to は組で');
  if (a.from && (!isDate(a.from) || !isDate(a.to) || a.from > a.to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });   // 読むだけ
  const ledger = openLedger(dataDir);
  try {
    const base = syncBase(), syncKey = process.env.MIRROR_SYNC_KEY || '';
    if (a.resetLedger) {
      const n = ledger.resetFingerprints();
      console.log(`台帳の指紋を空にした: ${n} 伝票 (伝票番号は残す)。次の --incremental で全部送り直す ('same' が返るだけ)`);
      return;
    }
    if (a.reconcile) {
      let from = a.from, to = a.to;
      if (a.all) { from = DEFAULT_FLOOR; to = jstDate(0); }
      else if (!from) { const days = a.days != null ? Number(a.days) : 90; if (!Number.isInteger(days) || days < 1 || days > 730) throw new Error('--days は 1〜730'); from = jstDate(-(days - 1)); to = jstDate(0); }
      const rr = await reconcileShipmentsDaily({ warehouse, base, syncKey, from, to });
      process.exitCode = rr.ok ? 0 : 1;
      return;
    }
    if (!a.incremental && !a.from) throw new Error('--incremental か --from/--to を指定する (daily-sync は --incremental)');
    const r = await pushShipments({ warehouse, ledger, base, syncKey, chunkSize, dryRun: a.dryRun, force: a.force, from: a.from, to: a.to });
    console.log(summarizeResult(r));
    const success = r.lockedBy ? false : (r.dryRun ? r.transformErrors.length === 0 : r.ok);
    process.exitCode = success ? 0 : 1;
  } finally { ledger.close(); warehouse.close(); }
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
if (isMain) main().catch((e) => { console.error(`❌ Company DB 出荷 push: ${e.message}`); process.exit(1); });
