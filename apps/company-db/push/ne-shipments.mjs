#!/usr/bin/env node
/**
 * ne-shipments.mjs — miniPC の NE 伝票 (warehouse.db の raw_ne_order_base + raw_ne_orders) を Company DB (Render Postgres) に送る。D5a (08 §4.7 / §9 D5)
 *
 * 流れは共通部 (pipeline.mjs):
 *   ① 台帳 (DATA_DIR/company-db-push.db = ledger.mjs、種類 'shipment') の「送付済みの指紋」を読み、raw を伝票番号順に**全部**流し読みして (ヘッダ + 明細を伝票ごとにまとめる。1 つの読み取り取引)、
 *      整形 (ne-shipments-transform.mjs) → 指紋を比べ、**指紋が変わった伝票だけ**を台帳の outbox に書く (時刻には頼らない。PR #1336 Codex R1 #1〜#3)。
 *      範囲 = 受注日か出荷確定日が 2025-01-01 以降 (D-28) **または 追跡中 (投入済み)**。raw の snapshot はここで閉じる (HTTP の間は持たない。R2 #6)
 *   ② 世代 (台帳の batch_seq。最初の chunk の直前に取引の中で +1。Render の最大世代以上に補正してから) を付け、outbox から chunk (伝票 200 / 明細 5,000 / 8MB) を取って
 *      Render の POST /apps/company-db/sync/shipments へ送る (x-sync-key。Render 側は 1 chunk = 1 取引で core.apply_shipment_batch() を呼ぶ。期限超過 (503 CHUNK_DEADLINE) なら半分に割って送り直す)
 *   ③ 'applied' / 'same' が返った伝票の指紋を台帳に書き、送った行を outbox から消し、受領記録を残す (1 取引)。失敗・stale・整形できない伝票が 1 つでもあれば exit 1
 *   ④ 送り手の排他 = 台帳の lock (持ち主・pid・心拍)。台帳と Render の食い違い (受領記録の照合・追跡対象の取り戻し・件数の見張り) も共通部
 *   --reconcile: 旧 f_shipments_daily (miniPC) と mart.v_shipments_daily (Render) を 日 × 店舗 × 配送方法 で突き合わせる
 *
 * 使い方 (miniPC。daily-sync の 1 ステップ = NE 取得 → 出荷サマリ再構築 の後):
 *   node apps/company-db/push/ne-shipments.mjs --incremental                       → 範囲の伝票のうち指紋が変わったもの (初回 = 2025-01-01 以降の全部)
 *   node apps/company-db/push/ne-shipments.mjs --from 2025-01-01 --to 2025-01-31   → 受注日の範囲だけ (初回のバックフィルを 2 か月ずつ)
 *   node apps/company-db/push/ne-shipments.mjs --incremental --dry-run            → 送らずに件数と例だけ
 *   node apps/company-db/push/ne-shipments.mjs --incremental --force              → 指紋が同じでも送る ('same' が返るだけ)
 *   node apps/company-db/push/ne-shipments.mjs --reconcile --days 90              → 突合 (差があれば exit 1)。--from/--to / --all
 *   node apps/company-db/push/ne-shipments.mjs --reset-ledger                     → 台帳の指紋を空にする (Render を復元・作り直したとき。自動でも見つける)
 *
 * env: DATA_DIR / RENDER_MIRROR_URL (RENDER_PORTAL_URL があれば同じホストのときだけ優先) / MIRROR_SYNC_KEY / CDB_PUSH_CHUNK (既定 200、上限 1000)
 * 🚨 秘密は表示しない。🚨 daily-sync の runScript は引数が無いと '7' を足すので、必ず --incremental などの引数を付けて呼ぶ。warehouse.db は読むだけ。台帳は別ファイル (作り直せる写し)
 */
import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { buildShipment, TRANSFORM_VERSION } from './ne-shipments-transform.mjs';
import { openLedger } from './ledger.mjs';
import { runPush, summarizePush, fingerprintOf, newRunId, splitWindows, isDate, jstDate, DEFAULT_CHUNK, MAX_CHUNK, MIN_SPLIT, MAX_BODY_BYTES, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { baseOrigin } from '../../../scripts/company-db/remote-load.mjs';

export { newRunId, splitWindows, DEFAULT_CHUNK, MAX_CHUNK, MIN_SPLIT, MAX_BODY_BYTES };
export const DEFAULT_FLOOR = '2025-01-01';          // D-28: 2025-01-01 以降の注文 (+ その期間に出荷した古い注文)

/** 伝票の指紋 (共通部の式。整形の版 + ヘッダの content_hash + 明細) */
export const fingerprint = (item) => fingerprintOf(TRANSFORM_VERSION, item);

/** Render の Company DB 同期 API の base ('https://host/apps/company-db/sync')。取れなければ '' */
export function syncBase(env = process.env) {
  const o = baseOrigin(env);
  return o ? `${o}/apps/company-db/sync` : '';
}

/**
 * raw を伝票番号順に流し読みして、伝票ごとに { key, base, lines } を返す generator (同じ接続の 2 つの statement を merge。呼ぶ側が読み取り取引で包む)。
 * ヘッダの無い明細は onLinesWithoutBase(伝票番号, 明細の行) に渡す
 */
export function* iterateSlips(warehouse, { onLinesWithoutBase = () => {} } = {}) {
  const bases = warehouse.prepare('select * from raw_ne_order_base order by 伝票番号').iterate();
  const lines = warehouse.prepare('select * from raw_ne_orders order by 伝票番号, 明細行番号').iterate();
  let cur = lines.next();
  try {
    for (const b of bases) {
      const slip = String(b.伝票番号);
      while (!cur.done && String(cur.value.伝票番号) < slip) { onLinesWithoutBase(String(cur.value.伝票番号), cur.value); cur = lines.next(); }
      const ls = [];
      while (!cur.done && String(cur.value.伝票番号) === slip) { ls.push(cur.value); cur = lines.next(); }
      yield { key: slip, base: b, lines: ls };
    }
    while (!cur.done) { onLinesWithoutBase(String(cur.value.伝票番号), cur.value); cur = lines.next(); }
  } finally {
    if (typeof bases.return === 'function') bases.return();
    if (typeof lines.return === 'function') lines.return();
  }
}

/** Render の状態 (GET /shipments/status) → { max_batch_seq, shipments }。取れなければ例外 */
export async function fetchStatus(fetchImpl, { base, syncKey }) {
  const res = await fetchImpl(`${base}/shipments/status`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`Render の状態が取れない: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  const j = await res.json();
  if (!j || !j.counts || !Number.isInteger(j.counts.shipments)) throw new Error('Render の状態の応答に counts.shipments が無い');
  return { max_batch_seq: j.counts.max_batch_seq == null ? null : Number(j.counts.max_batch_seq), shipments: j.counts.shipments };
}

/**
 * 送る (本体 = pipeline.runPush に伝票の読み方・整形・範囲を渡す)。戻り値は共通部の形 + 伝票の呼び名 (staleSlips / failed[].ne_slip_no / transformErrors[].ne_slip_no / linesWithoutBase)
 */
export async function pushShipments({ warehouse, ledger, base, syncKey, floor = DEFAULT_FLOOR, from = null, to = null, ...rest }) {
  const mode = from && to ? 'range' : 'incremental';
  const floorTs = `${floor} 00:00:00`;
  const inRange = (b) => (mode === 'range' ? (String(b.受注日 || '') >= `${from} 00:00:00` && String(b.受注日 || '') < `${to} 99`) : (String(b.受注日 || '') >= floorTs || String(b.出荷確定日 || '') >= floorTs));
  const stats = { noBase: new Set() };
  const r = await runPush({
    kind: 'shipment', label: '伝票', warehouse, ledger, base, syncKey, mode, stats,
    scopeLabel: mode === 'range' ? `受注日 ${from}〜${to}` : `範囲 ${floor} 以降 + 追跡中`,
    paths: { post: '/shipments', status: '/shipments/status', receipt: '/shipments/receipt', keys: '/shipments/slips' },
    countOf: (j) => ({ count: j && j.counts ? j.counts.shipments : undefined, maxBatchSeq: j && j.counts && j.counts.max_batch_seq != null ? Number(j.counts.max_batch_seq) : null }),
    keysOf: (j) => (j ? j.slips : null),
    // ヘッダ (受注ベース) の無い明細は範囲の中だけ数える (raw_ne_orders は受注ベースより古くから溜まっていて、2025 年より前の 120 万伝票に受注ベースが無いのは正常 = 9/14 実測)
    iterate: (wh, st) => iterateSlips(wh, { onLinesWithoutBase: (s, l) => { if (inRange(l)) st.noBase.add(s); } }),
    inScope: (g, fps) => inRange(g.base) || (mode !== 'range' && fps.has(g.key)),   // 追跡中の伝票を範囲に足すのは incremental だけ (--from/--to は期間で区切る = D5a のまま。Codex D5b-1 R2 #6)
    build: (g, ctx) => { const it = buildShipment(g.base, g.lines, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString() }); return { key: it.ne_slip_no, payload: { ne_slip_no: it.ne_slip_no, header: it.header, lines: it.lines }, n_lines: it.lines.length, no_synced_at: it.no_synced_at }; },
    transformVersion: TRANSFORM_VERSION,
    ...rest,
  });
  r.linesWithoutBase = stats.noBase.size;
  r.staleSlips = r.staleKeys;
  r.transformErrors = r.transformErrors.map((t) => ({ ...t, ne_slip_no: t.key }));
  r.failed = r.failed.map((f) => ({ ...f, ne_slip_no: f.ne_slip_no ?? f.key }));
  return r;
}

/** 1 行の要約 (daily-sync は最後の行を朝の通知に載せる) */
export function summarizeResult(r) {
  return summarizePush(r, '出荷', '伝票') + (r.linesWithoutBase ? ` / ヘッダ無しの明細 ${r.linesWithoutBase} 伝票` : '');
}

/** 突合: miniPC の f_shipments_daily (旧表) と Render の mart.v_shipments_daily を 日 × 店舗 × 配送方法 で比べる (純粋な比較部分は diffDaily) */
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

async function main() {
  const a = parseArgs(process.argv.slice(2));
  const dataDir = (process.env.DATA_DIR || a.dataDir || '').trim();
  if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
  const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${a.chunk ?? process.env.CDB_PUSH_CHUNK} (1〜${MAX_CHUNK})`);
  if ((a.from && !a.to) || (!a.from && a.to)) throw new Error('--from と --to は組で');
  if (a.from && (!isDate(a.from) || !isDate(a.to) || a.from > a.to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });   // 読むだけ
  const ledger = openLedger(dataDir, { kind: 'shipment' });
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
// 落ちたときの最後の行は 1 行にする (Render のデプロイ中の 502 は本文が HTML = そのまま出すと何十行にもなる。2026-09-21 に本番のバックフィルで出た)。
// 🚨 fetch の直後に process.exit() しない: Windows の Node は libuv の assertion で異常終了して終了コードが 127 になる (#1386)。exitCode を置いて自然に終わらせ、保険に 10 秒後 (unref = ループを延ばさない)
if (isMain) main().catch((e) => {
  console.error(`❌ Company DB 出荷 push: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
  process.exitCode = 1;
  setTimeout(() => process.exit(1), 10000).unref();
});
