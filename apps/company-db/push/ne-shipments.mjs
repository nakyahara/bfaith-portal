#!/usr/bin/env node
/**
 * ne-shipments.mjs — miniPC の NE 伝票 (warehouse.db の raw_ne_order_base + raw_ne_orders) を Company DB (Render Postgres) に送る。D5a (08 §4.7 / §9 D5)
 *
 * 何をするか:
 *   ① 前回送った位置 (sync_meta.cdb_shipments_cursor = raw の synced_at) 以降に変わった伝票を選ぶ (ヘッダか明細のどちらかが変わった伝票。伝票単位で完全な明細集合を送る)
 *   ② ne-shipments-transform.mjs で header / lines に整える (内部 ID は送らない。時刻は JST → +09:00)
 *   ③ 世代 (sync_meta.cdb_shipments_batch_seq を +1) を付け、chunk (既定 200 伝票) ごとに Render の POST /apps/company-db/sync/shipments へ送る
 *      (x-sync-key。Render 側は 1 chunk = 1 取引で core.apply_shipment_batch() を呼び、失敗した伝票だけを返す)
 *   ④ 全 chunk が通り、失敗した伝票が 0 ならカーソルを進める。失敗があればカーソルは進めず exit 1 (翌日また同じ伝票から送る + 朝の通知に ❌)
 *   --reconcile: 旧 f_shipments_daily (miniPC) と mart.v_shipments_daily (Render) を 日 × 店舗 × 配送方法 で突き合わせる (08 §9 D5 の「f_shipments_daily との突合」)
 *
 * 使い方 (miniPC。daily-sync の 1 ステップ = NE 取得 → 出荷サマリ再構築 の後):
 *   node apps/company-db/push/ne-shipments.mjs --incremental                 → カーソル以降 (初回はカーソル無し = 2025-01-01 以降の全部 = D-28)
 *   node apps/company-db/push/ne-shipments.mjs --from 2025-01-01 --to 2025-01-31   → 受注日の範囲を送る (初回のバックフィルを月ごとに。カーソルは動かさない)
 *   node apps/company-db/push/ne-shipments.mjs --incremental --dry-run      → 送らずに件数と例だけ
 *   node apps/company-db/push/ne-shipments.mjs --reconcile --days 90        → 突合 (差があれば exit 1)
 *
 * env: DATA_DIR (warehouse.db の場所。--data-dir でも可) / RENDER_MIRROR_URL (送り先の origin をここから取る。RENDER_PORTAL_URL があれば同じホストのときだけ優先) /
 *      MIRROR_SYNC_KEY (x-sync-key) / CDB_PUSH_CHUNK (1 chunk の伝票数。既定 200、上限 1000)
 *
 * 🚨 秘密は表示しない。🚨 daily-sync の runScript は引数が無いと '7' を足すので、必ず --incremental などの引数を付けて呼ぶ
 */
import 'dotenv/config';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { buildShipment, TRANSFORM_VERSION } from './ne-shipments-transform.mjs';
import { baseOrigin } from '../../../scripts/company-db/remote-load.mjs';

export const CURSOR_KEY = 'cdb_shipments_cursor';
export const SEQ_KEY = 'cdb_shipments_batch_seq';
export const DEFAULT_FLOOR = '2025-01-01';          // D-28: 2025-01-01 以降の注文 (+ その期間に出荷した古い注文)
export const DEFAULT_CHUNK = 200;
export const MAX_CHUNK = 1000;
const HTTP_TIMEOUT_MS = 120000;
const RETRIES = 3;

export function newRunId(now = new Date()) {
  return `ship_${now.toISOString().replace(/[-:.TZ]/g, '').slice(0, 15)}_${crypto.randomBytes(3).toString('hex')}`;
}
const utcNaive = (d) => d.toISOString().replace('T', ' ').slice(0, 19);   // raw の synced_at と同じ形 (UTC)
const isDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;

export function readMeta(db, key) {
  const r = db.prepare('select value from sync_meta where key = ?').get(key);
  return r && r.value != null && r.value !== '' ? String(r.value) : null;
}
export function writeMeta(db, key, value) {
  db.prepare('insert or replace into sync_meta (key, value, updated_at) values (?, ?, ?)').run(key, value, utcNaive(new Date()));
}

/**
 * 送る伝票を選ぶ (純粋に読むだけ)。
 *   incremental: synced_at >= cursor (ヘッダか明細) の伝票のうち、受注日 >= floor か 出荷確定日 >= floor のもの。cursor が無ければ floor 以降の全部
 *   range: 受注日が from〜to (両端を含む) の伝票 (カーソルは見ない = バックフィル)
 * @returns {{ slips: {base, lines}[], maxSyncedAt: string|null, linesWithoutBase: number }}
 */
export function selectSlips(db, { cursor = null, floor = DEFAULT_FLOOR, from = null, to = null } = {}) {
  let bases;
  if (from && to) {
    bases = db.prepare(`select * from raw_ne_order_base where 受注日 >= ? and 受注日 < ? order by 伝票番号`).all(`${from} 00:00:00`, `${to} 99`);
  } else if (cursor) {
    bases = db.prepare(`
      with changed as (
        select 伝票番号 from raw_ne_order_base where synced_at >= @cursor
        union
        select 伝票番号 from raw_ne_orders where synced_at >= @cursor
      )
      select b.* from raw_ne_order_base b join changed c on c.伝票番号 = b.伝票番号
       where b.受注日 >= @floor or b.出荷確定日 >= @floor
       order by b.伝票番号`).all({ cursor, floor: `${floor} 00:00:00` });
  } else {
    bases = db.prepare(`select * from raw_ne_order_base where 受注日 >= ? or 出荷確定日 >= ? order by 伝票番号`).all(`${floor} 00:00:00`, `${floor} 00:00:00`);
  }
  const lineStmt = db.prepare('select * from raw_ne_orders where 伝票番号 = ? order by 明細行番号');
  let maxSyncedAt = null;
  const slips = bases.map((base) => {
    const lines = lineStmt.all(base.伝票番号);
    for (const s of [base.synced_at, ...lines.map((l) => l.synced_at)]) if (s && (!maxSyncedAt || s > maxSyncedAt)) maxSyncedAt = s;
    return { base, lines };
  });
  // ヘッダ (受注ベース) がまだ無い伝票の明細は送れない (header の材料が無い)。件数だけ出す = 取れていないことを黙らせない
  const linesWithoutBase = cursor
    ? db.prepare(`select count(distinct o.伝票番号) as n from raw_ne_orders o where o.synced_at >= ? and not exists (select 1 from raw_ne_order_base b where b.伝票番号 = o.伝票番号)`).get(cursor).n
    : db.prepare(`select count(distinct o.伝票番号) as n from raw_ne_orders o where (o.受注日 >= ? or o.出荷確定日 >= ?) and not exists (select 1 from raw_ne_order_base b where b.伝票番号 = o.伝票番号)`).get(`${floor} 00:00:00`, `${floor} 00:00:00`).n;
  return { slips, maxSyncedAt, linesWithoutBase: Number(linesWithoutBase) };
}

/** Render の Company DB 同期 API の base ('https://host/apps/company-db/sync')。取れなければ '' */
export function syncBase(env = process.env) {
  const o = baseOrigin(env);
  return o ? `${o}/apps/company-db/sync` : '';
}

const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function postChunk(fetchImpl, { base, syncKey, body, log, sleep = defaultSleep }) {
  let lastErr = null;
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
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
      if (res.status >= 400 && res.status < 500 && res.status !== 429) throw Object.assign(new Error(msg), { fatal: true });   // 直しても再送では通らない
      lastErr = new Error(msg);
    } catch (e) {
      if (e.fatal) throw e;
      lastErr = e;
    }
    if (attempt < RETRIES) { log(`  送信に失敗 (${lastErr.message})。${attempt * 5} 秒後に再送 (${attempt}/${RETRIES})`); await sleep(attempt * 5000); }
  }
  throw lastErr;
}

/**
 * 送る (本体)。戻り値 = { ok, mode, selected, transformErrors, sent, applied, same, stale, failed, chunks, batchSeq, runId, cursorBefore, cursorAfter, linesWithoutBase, noSyncedAt }
 * 差し替え (試験): fetchImpl / now
 */
export async function pushShipments({ db, fetchImpl = fetch, base, syncKey, chunkSize = DEFAULT_CHUNK, dryRun = false, floor = DEFAULT_FLOOR, from = null, to = null, log = console.log, now = () => new Date(), sleep = defaultSleep }) {
  const mode = from && to ? 'range' : 'incremental';
  if (chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk は 1〜${MAX_CHUNK}`);
  const cursorBefore = mode === 'incremental' ? readMeta(db, CURSOR_KEY) : null;
  const startedAt = now();
  const { slips, maxSyncedAt, linesWithoutBase } = selectSlips(db, { cursor: cursorBefore, floor, from, to });
  const items = [];
  const transformErrors = [];
  let noSyncedAt = 0;
  for (const { base: b, lines } of slips) {
    try {
      const it = buildShipment(b, lines, { fallbackSourceUpdatedAt: startedAt.toISOString() });
      if (it.no_synced_at) noSyncedAt++;
      items.push(it);
    } catch (e) { transformErrors.push({ ne_slip_no: String(b.伝票番号), error: e.message }); }
  }
  log(`[company-db push] ${mode === 'range' ? `受注日 ${from}〜${to}` : `カーソル ${cursorBefore || '(無し = ' + floor + ' 以降の全部)'}`}: 伝票 ${slips.length} 件 (整形できない ${transformErrors.length} / ヘッダ無しの明細 ${linesWithoutBase} 伝票 / synced_at 無し ${noSyncedAt})`);
  if (transformErrors.length) for (const t of transformErrors.slice(0, 20)) log(`  整形できない: ${t.ne_slip_no}: ${t.error}`);
  const result = { ok: false, mode, selected: slips.length, transformErrors, sent: 0, applied: 0, same: 0, stale: 0, failed: [], chunks: 0, batchSeq: null, runId: null, cursorBefore, cursorAfter: cursorBefore, linesWithoutBase, noSyncedAt, dryRun };
  if (dryRun) {
    if (items.length) log(`  例: ${JSON.stringify({ ne_slip_no: items[0].ne_slip_no, header: items[0].header, lines: items[0].lines }).slice(0, 600)}`);
    result.ok = transformErrors.length === 0;
    return result;
  }
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL を確かめる。https で同じホストのときだけ)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  if (items.length === 0) {
    // 送る物が無い = 変化なし。カーソルは進めない (次回また同じ位置から。synced_at >= cursor なので重複は 'same' で吸収される)
    result.ok = transformErrors.length === 0;
    log(`[company-db push] 送る伝票なし${transformErrors.length ? ' (整形できない伝票があるので exit 1)' : ''}`);
    return result;
  }
  const batchSeq = (Number(readMeta(db, SEQ_KEY)) || 0) + 1;
  writeMeta(db, SEQ_KEY, String(batchSeq));   // 送る前に進める (途中で落ちても次の run はさらに新しい世代 = 古い世代が後から勝てない)
  const runId = newRunId(startedAt);
  result.batchSeq = batchSeq; result.runId = runId;
  const chunkCount = Math.ceil(items.length / chunkSize);
  for (let i = 0; i < chunkCount; i++) {
    const rows = items.slice(i * chunkSize, (i + 1) * chunkSize).map((it) => ({ ne_slip_no: it.ne_slip_no, header: it.header, lines: it.lines }));
    const r = await postChunk(fetchImpl, { base, syncKey, log, sleep, body: { run_id: runId, batch_seq: batchSeq, chunk_index: i, chunk_count: chunkCount, transform_version: TRANSFORM_VERSION, rows } });
    for (const k of ['applied', 'same', 'stale']) { if (!Number.isInteger(r[k])) throw new Error(`応答に ${k} が無い`); result[k] += r[k]; }
    if (!Array.isArray(r.failed)) throw new Error('応答に failed が無い');
    result.failed.push(...r.failed);
    result.sent += rows.length; result.chunks++;
    if (r.applied + r.same + r.stale + r.failed.length !== rows.length) throw new Error(`chunk ${i + 1}: 送った ${rows.length} と応答の合計 ${r.applied + r.same + r.stale + r.failed.length} が合わない`);
    if (chunkCount > 1 && (i % 10 === 9 || i === chunkCount - 1)) log(`  chunk ${i + 1}/${chunkCount}: applied ${result.applied} same ${result.same} stale ${result.stale} failed ${result.failed.length}`);
  }
  if (result.failed.length) for (const f of result.failed.slice(0, 20)) log(`  失敗: ${f.ne_slip_no}: ${f.error}`);
  result.ok = result.failed.length === 0 && transformErrors.length === 0;
  if (mode === 'incremental' && result.ok && maxSyncedAt) {
    writeMeta(db, CURSOR_KEY, maxSyncedAt);
    result.cursorAfter = maxSyncedAt;
  }
  return result;
}

/** 1 行の要約 (daily-sync は最後の行を朝の通知に載せる) */
export function summarizeResult(r) {
  if (r.dryRun) return `dry-run: 伝票 ${r.selected} 件 (整形できない ${r.transformErrors.length})`;
  const head = r.ok ? '✅' : '❌';
  return `${head} Company DB 出荷 push: 伝票 ${r.sent}/${r.selected} 件 (applied ${r.applied} / same ${r.same} / stale ${r.stale} / failed ${r.failed.length} / 整形できない ${r.transformErrors.length})`
    + (r.batchSeq ? ` 世代 ${r.batchSeq} chunk ${r.chunks}` : '')
    + (r.mode === 'incremental' ? ` カーソル ${r.cursorBefore || '-'} → ${r.cursorAfter || '-'}` : ` 受注日の範囲 (カーソルは動かさない)`)
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
    const r = R.get(k);
    if (!r) { onlyLocal.push(l); continue; }
    const diffs = [];
    for (const f of ['slips', 'cancelled_slips']) if (Number(l[f]) !== Number(r[f])) diffs.push(`${f} ${l[f]}≠${r[f]}`);
    if (String(l.delivery_name ?? '') !== String(r.delivery_name ?? '')) diffs.push(`delivery_name ${l.delivery_name}≠${r.delivery_name}`);
    if (diffs.length) mismatched.push({ key: k, diffs }); else matched++;
  }
  for (const [k, r] of R) if (!L.has(k)) onlyRemote.push(r);
  return { compared: L.size + onlyRemote.length, matched, mismatched, onlyLocal, onlyRemote };
}

export async function reconcileShipmentsDaily({ db, fetchImpl = fetch, base, syncKey, from, to, log = console.log }) {
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const local = db.prepare('select ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips from f_shipments_daily where ship_date >= ? and ship_date <= ?').all(from, to);
  const res = await fetchImpl(`${base}/shipments/daily?from=${from}&to=${to}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`Render の日次が取れない: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  const remote = (await res.json()).rows;
  if (!Array.isArray(remote)) throw new Error('Render の応答に rows が無い');
  const d = diffDaily(local, remote);
  const localSlips = local.reduce((a, r) => a + Number(r.slips), 0), remoteSlips = remote.reduce((a, r) => a + Number(r.slips), 0);
  log(`[company-db reconcile] ${from}〜${to}: 行 miniPC ${local.length} / Render ${remote.length}、伝票 miniPC ${localSlips} / Render ${remoteSlips}、一致 ${d.matched} / 不一致 ${d.mismatched.length} / miniPC だけ ${d.onlyLocal.length} / Render だけ ${d.onlyRemote.length}`);
  for (const m of d.mismatched.slice(0, 30)) log(`  不一致 ${m.key}: ${m.diffs.join(', ')}`);
  for (const r of d.onlyLocal.slice(0, 10)) log(`  miniPC だけ ${r.ship_date}|${r.shop_code}|${r.delivery_id}: slips ${r.slips}`);
  for (const r of d.onlyRemote.slice(0, 10)) log(`  Render だけ ${r.ship_date}|${r.shop_code}|${r.delivery_id}: slips ${r.slips}`);
  const ok = d.mismatched.length === 0 && d.onlyLocal.length === 0 && d.onlyRemote.length === 0;
  log(`${ok ? '✅' : '❌'} 突合 ${from}〜${to}: ${ok ? '全部一致' : `差 ${d.mismatched.length + d.onlyLocal.length + d.onlyRemote.length} 行`} (伝票 ${localSlips} / ${remoteSlips})`);
  return { ok, ...d, localSlips, remoteSlips };
}

function parseArgs(argv) {
  const out = { incremental: false, dryRun: false, reconcile: false, from: null, to: null, days: null, dataDir: null, chunk: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--incremental') out.incremental = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--reconcile') out.reconcile = true;
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
  const db = new Database(path.join(dataDir, 'warehouse.db'), { timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });
  try {
    const base = syncBase(), syncKey = process.env.MIRROR_SYNC_KEY || '';
    if (a.reconcile) {
      let from = a.from, to = a.to;
      if (!from) { const days = a.days != null ? Number(a.days) : 90; if (!Number.isInteger(days) || days < 1 || days > 730) throw new Error('--days は 1〜730'); from = jstDate(-(days - 1)); to = jstDate(0); }
      const r = await reconcileShipmentsDaily({ db, base, syncKey, from, to });
      process.exitCode = r.ok ? 0 : 1;
      return;
    }
    if (!a.incremental && !a.from) throw new Error('--incremental か --from/--to を指定する (daily-sync は --incremental)');
    const r = await pushShipments({ db, base, syncKey, chunkSize, dryRun: a.dryRun, from: a.from, to: a.to });
    console.log(summarizeResult(r));
    process.exitCode = r.ok ? 0 : 1;
  } finally { db.close(); }
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main().catch((e) => { console.error(`❌ Company DB 出荷 push: ${e.message}`); process.exit(1); });
