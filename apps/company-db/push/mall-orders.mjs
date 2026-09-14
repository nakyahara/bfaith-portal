#!/usr/bin/env node
/**
 * mall-orders.mjs — miniPC のモールの注文 (warehouse.db の raw_*_orders) を Company DB (Render Postgres) に送る。D5b (08 §4.1 / §4.7 / §9 D5)。まず楽天 (D5b-1)
 *
 * 流れは伝票 (ne-shipments.mjs) と同じ共通部 (pipeline.mjs): 台帳 (種類 'order:<mall>') の指紋で差分を決め、outbox から chunk で送り、失敗・stale は次回また送る。
 * 送った後、注文が入ったので伝票との結び直し (POST /shipments/relink = core.relink_shipments_bulk) を回す。
 *
 * 使い方 (miniPC。daily-sync の 1 ステップ = 楽天 RMS API の取込の後):
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental                        → 範囲 (注文日 2025-01-01 以降 = D-28、または追跡中) のうち指紋が変わった注文
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --from 2025-01-01 --to 2025-02-28    → 注文日の範囲だけ (初回のバックフィルを 2 か月ずつ)
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental --dry-run             → 送らずに件数と例だけ
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --reconcile --days 90               → 日ごとの注文数・明細数・商品代の合計を raw と Render で突き合わせる (差があれば exit 1)
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --reset-ledger                      → 台帳の指紋を空にする
 *   node apps/company-db/push/mall-orders.mjs --relink                                            → 伝票との結び直しだけ
 *
 * env: DATA_DIR / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CDB_PUSH_CHUNK (伝票と同じ)
 * 🚨 秘密は表示しない。🚨 daily-sync の runScript は引数が無いと '7' を足すので、必ず --mall などの引数を付けて呼ぶ。warehouse.db は読むだけ
 */
import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { openLedger } from './ledger.mjs';
import { runPush, summarizePush, splitWindows, isDate, jstDate, DEFAULT_CHUNK, MAX_CHUNK, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { buildRakutenOrder, RAKUTEN_TRANSFORM_VERSION, RAKUTEN_SENTINEL } from './mall-orders-transform.mjs';
import { syncBase } from './ne-shipments.mjs';

export const DEFAULT_FLOOR = '2025-01-01';          // D-28
export const MALL_SPECS = {
  rakuten: {
    label: '楽天の注文', scope: 'main', transformVersion: RAKUTEN_TRANSFORM_VERSION,
    /** raw を注文番号順に流し読みして、注文ごとに { key, no, rows } を返す (同じ接続の 1 statement = 1 スナップショット) */
    iterate: function* (warehouse) {
      const it = warehouse.prepare('select * from raw_rakuten_orders order by order_number, item_detail_id').iterate();
      let cur = null;
      for (const row of it) {
        const no = String(row.order_number ?? '');
        if (cur && cur.no === no) { cur.rows.push(row); continue; }
        if (cur) yield cur;
        cur = { key: `rakuten|main|${no}`, no, rows: [row], order_date: String(row.order_date ?? '') };
      }
      if (cur) yield cur;
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    /**
     * floor 以降に出荷確定した楽天の伝票 (raw_ne_order_base の店舗 1 = core.ne_shops。NE 受注番号 = 楽天の注文番号) が参照する、注文日が floor より前の注文番号 (D-28)。
     * 表が無ければ null (呼ぶ側が警告)。呼ぶ側の読み取り取引の中で呼ぶ
     */
    referencedByShipments: (warehouse, floor) => {
      if (!warehouse.prepare(`select name from sqlite_master where type = 'table' and name = 'raw_ne_order_base'`).get()) return null;
      const ts = `${floor} 00:00:00`;
      return new Set(warehouse.prepare(`select 受注番号 as no from raw_ne_order_base where 店舗コード = '1' and 受注番号 is not null and 受注日 < ? and 出荷確定日 >= ?`).all(ts, ts).map((r) => String(r.no)));
    },
    build: (group, ctx, stats) => buildRakutenOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString(), stats }),
    /** 突合の材料 (miniPC 側): 注文日ごとの 注文数 / 明細数 / 商品代 (goods_price) の合計 / 取消の注文数。raw と同じ式を Render (GET /orders/daily) が持つ */
    dailySql: `with o as (
        select order_number, substr(min(order_date), 1, 10) as d, max(order_status) as st, max(goods_price) as gp, count(*) as n_lines
          from raw_rakuten_orders group by order_number)
      select d as order_date, count(*) as orders, sum(n_lines) as lines,
             sum(case when gp is null or gp = ${RAKUTEN_SENTINEL} or gp < 0 then 0 else round(gp) end) as items_amount_jpy,
             sum(case when st in (800, 900) then 1 else 0 end) as cancelled
        from o where d >= ? and d <= ? group by d`,
  },
};

/** 突合の純粋な比較 */
export function diffDailyOrders(localRows, remoteRows) {
  const L = new Map(localRows.map((r) => [r.order_date, r])), R = new Map(remoteRows.map((r) => [r.order_date, r]));
  const mismatched = [], onlyLocal = [], onlyRemote = [];
  let matched = 0;
  for (const [k, l] of L) {
    const rr = R.get(k);
    if (!rr) { onlyLocal.push(l); continue; }
    const diffs = [];
    for (const f of ['orders', 'lines', 'items_amount_jpy', 'cancelled']) if (Number(l[f]) !== Number(rr[f])) diffs.push(`${f} ${l[f]}≠${rr[f]}`);
    if (diffs.length) mismatched.push({ key: k, diffs }); else matched++;
  }
  for (const [k, rr] of R) if (!L.has(k)) onlyRemote.push(rr);
  return { compared: L.size + onlyRemote.length, matched, mismatched, onlyLocal, onlyRemote };
}

export async function reconcileOrdersDaily({ mall, warehouse, fetchImpl = fetch, base, syncKey, from, to, log = console.log }) {
  const spec = MALL_SPECS[mall]; if (!spec) throw new Error(`知らないモール: ${mall}`);
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const total = { ok: true, windows: [], matched: 0, mismatched: [], onlyLocal: [], onlyRemote: [], localOrders: 0, remoteOrders: 0 };
  for (const [a, b] of splitWindows(from, to)) {
    const local = warehouse.prepare(spec.dailySql).all(a, b);
    const res = await fetchImpl(`${base}/orders/daily?mall=${mall}&scope=${spec.scope}&from=${a}&to=${b}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`Render の日次が取れない (${a}〜${b}): HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const remote = (await res.json()).rows;
    if (!Array.isArray(remote)) throw new Error('Render の応答に rows が無い');
    const d = diffDailyOrders(local, remote);
    const lo = local.reduce((s, x) => s + Number(x.orders), 0), ro = remote.reduce((s, x) => s + Number(x.orders), 0);
    const ok = d.mismatched.length === 0 && d.onlyLocal.length === 0 && d.onlyRemote.length === 0;
    log(`[company-db reconcile ${spec.label}] ${a}〜${b}: 日 miniPC ${local.length} / Render ${remote.length}、注文 miniPC ${lo} / Render ${ro}、一致 ${d.matched} / 不一致 ${d.mismatched.length} / miniPC だけ ${d.onlyLocal.length} / Render だけ ${d.onlyRemote.length} ${ok ? '✅' : '❌'}`);
    for (const m of d.mismatched.slice(0, 30)) log(`  不一致 ${m.key}: ${m.diffs.join(', ')}`);
    for (const x of d.onlyLocal.slice(0, 10)) log(`  miniPC だけ ${x.order_date}: 注文 ${x.orders}`);
    for (const x of d.onlyRemote.slice(0, 10)) log(`  Render だけ ${x.order_date}: 注文 ${x.orders}`);
    total.windows.push({ from: a, to: b, ok, ...d, localOrders: lo, remoteOrders: ro });
    total.ok = total.ok && ok; total.matched += d.matched; total.mismatched.push(...d.mismatched); total.onlyLocal.push(...d.onlyLocal); total.onlyRemote.push(...d.onlyRemote);
    total.localOrders += lo; total.remoteOrders += ro;
  }
  log(`${total.ok ? '✅' : '❌'} 突合 ${spec.label} ${from}〜${to}: ${total.ok ? '全部一致' : `差 ${total.mismatched.length + total.onlyLocal.length + total.onlyRemote.length} 日`} (注文 miniPC ${total.localOrders} / Render ${total.remoteOrders}、${total.windows.length} 窓)`);
  return total;
}

/**
 * 伝票 → 注文の結び直し (Render の core.relink_shipments_bulk を shipment_id の順に回す)。
 * 戻り値 = { linked, examined, calls, complete, next }。maxCalls で打ち切ったら complete = false と続きの位置 next (Codex D5b-1 R1 #6)
 */
export async function relinkShipments({ fetchImpl = fetch, base, syncKey, limit = 20000, maxCalls = 200, after = 0, budgetMs = Infinity, now = () => Date.now(), onProgress = () => {}, log = console.log, beforeCall = () => {} }) {
  const started = now();
  let linked = 0, examined = 0, calls = 0, complete = false, reason = null;
  for (;;) {
    if (calls >= maxCalls) { reason = 'max_calls'; break; }
    if (now() - started >= budgetMs) { reason = 'budget'; break; }   // 時間予算 (daily-sync の 30 分より手前で区切り、続きは次の run。Codex D5b-1 R3 #1)
    beforeCall();   // lock の中で回すときは HTTP のたびに持ち主を確かめ心拍を打つ (奪われていたら LockLostError)
    const res = await fetchImpl(`${base}/shipments/relink`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': syncKey }, body: JSON.stringify({ after, limit }), signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`結び直しが失敗: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const j = await res.json(); calls++;
    linked += Number(j.linked || 0); examined += Number(j.examined || 0);
    if (!j.examined || j.last_id == null) { complete = true; break; }
    after = Number(j.last_id);
    onProgress(after);   // 成功のたびに続きの位置を残す (途中で落ちても・殺されても、済んだ所からやり直す。Codex R3 #1)
  }
  log(`[company-db relink] 伝票 ${examined} 件を見て ${linked} 件を注文に結んだ (${calls} 回${complete ? '' : `。${reason === 'budget' ? '時間切れ' : '回数の上限'}で打ち切り = 続きは shipment_id > ${after}`})`);
  return { linked, examined, calls, complete, next: complete ? 0 : after, reason };
}

export const DEFAULT_RELINK_BUDGET_MS = 10 * 60 * 1000;   // 結び直しの時間予算 (daily-sync のステップは 30 分。push 自体の後に回すので 10 分で区切る。env CDB_RELINK_BUDGET_MS)

export const RELINK_PENDING_KEY = 'relink_pending';   // '1' = 結び直しが要る (注文を送った・前回が失敗か打ち切り)。完了で '0' (Codex D5b-1 R1 #5)
export const RELINK_NEXT_KEY = 'relink_next';         // 打ち切ったときの続きの位置 (shipment_id)。注文を送る前と完了で 0
/** 注文を送る run が最初の chunk の直前 (世代を取る取引) に書く印 = HTTP より先に「結び直しが要る」を永続化 (応答を失って run が落ちても消えない。Codex R2 #1 / #2) */
export const RELINK_META_ON_SEND = { [RELINK_PENDING_KEY]: '1', [RELINK_NEXT_KEY]: '0' };

/**
 * push の後の結び直し (**送り手の lock の中** = runPush の afterSend から呼ぶ。Codex R2 #3: 別の run が印を消せない)。
 *   台帳の relink_pending が '1' なら回す (印は注文を送る前に付く)。失敗 → 印は残る (run は ❌ = retry の対象) / 打ち切り → 続きの位置を残す / 完了 → 印を消す (持ち主の確認と同じ取引)
 * 戻り値 = { ran, pending, result, error }
 */
export async function relinkAfterPush({ ledger, owner = null, fetchImpl = fetch, base, syncKey, limit = 20000, maxCalls = 200, budgetMs = DEFAULT_RELINK_BUDGET_MS, log = console.log, now = () => new Date(), mustOwn = () => {} }) {
  if (ledger.getMeta(RELINK_PENDING_KEY) !== '1') return { ran: false, pending: false, result: null, error: null };
  const after = Number(ledger.getMeta(RELINK_NEXT_KEY)) || 0;
  const saveNext = (next) => ledger.setMeta({ [RELINK_NEXT_KEY]: String(next) }, { owner, at: now() });   // 持ち主の確認と同じ取引
  try {
    const r = await relinkShipments({ fetchImpl, base, syncKey, limit, maxCalls, after, budgetMs, now: () => now().getTime(), onProgress: saveNext, log, beforeCall: mustOwn });
    if (r.complete) ledger.setMeta({ [RELINK_PENDING_KEY]: '0', [RELINK_NEXT_KEY]: '0' }, { owner, at: now() });
    else { saveNext(r.next); log(`[company-db relink] ${r.reason === 'budget' ? `時間予算 ${Math.round(budgetMs / 1000)} 秒` : `${maxCalls} 回`}で打ち切り。次の run で shipment_id > ${r.next} から続ける`); }
    return { ran: true, pending: !r.complete, result: r, error: null };
  } catch (e) {
    if (e && e.code === 'LOCK_LOST') throw e;
    log(`[company-db relink] 失敗: ${e.message} → 次の run でやり直す (台帳の印はそのまま)`);
    return { ran: true, pending: true, result: null, error: e.message };
  }
}

/** 1 モールを送る (pipeline.runPush の種類ごとの設定) */
export async function pushOrders({ mall, warehouse, ledger, base, syncKey, floor = DEFAULT_FLOOR, from = null, to = null, relink = true, relinkLimit = 20000, relinkMaxCalls = 200, relinkBudgetMs = Number(process.env.CDB_RELINK_BUDGET_MS) || DEFAULT_RELINK_BUDGET_MS, ...rest }) {
  const spec = MALL_SPECS[mall]; if (!spec) throw new Error(`知らないモール: ${mall}`);
  const mode = from && to ? 'range' : 'incremental';
  const logf = rest.log || console.log;
  const stats = { sentinel: 0, negative: 0, referenced: null, referencedCount: null };
  // 範囲 (incremental) = 注文日が floor 以降 / 追跡中 / **floor 以降に出荷確定した伝票が参照する注文** (D-28 = 出荷から辿れる古い注文も入れる。Codex D5b-1 R3 #2)。--from/--to は注文日の期間だけ
  const inRange = (g, fps) => (mode === 'range' ? (g.order_date >= from && g.order_date < `${to}T99`) : (g.order_date >= floor || fps.has(g.key) || (stats.referenced != null && stats.referenced.has(g.no))));
  const iterate = function* (wh, st) {
    if (mode !== 'range' && spec.referencedByShipments) {
      st.referenced = spec.referencedByShipments(wh, floor);   // raw の読み取り取引の中 (同じ snapshot)
      st.referencedCount = st.referenced ? st.referenced.size : null;
      if (st.referenced == null) logf(`[company-db push ${spec.label}] ⚠️ raw_ne_order_base が無いので「範囲内の出荷が参照する古い注文」を範囲に入れられない`);
    }
    yield* spec.iterate(wh, st);
  };
  return runPush({
    kind: `order:${mall}`, label: spec.label, warehouse, ledger, base, syncKey, mode, stats,
    scopeLabel: mode === 'range' ? `注文日 ${from}〜${to}` : `注文日 ${floor} 以降 + 追跡中 + ${floor} 以降の出荷が参照する注文`,
    paths: { post: '/orders', status: `/orders/status?mall=${mall}&scope=${spec.scope}`, receipt: '/orders/receipt', keys: `/orders/keys?mall=${mall}&scope=${spec.scope}` },
    countOf: (j) => ({ count: j && j.counts ? j.counts.orders : undefined, maxBatchSeq: j && j.counts && j.counts.max_batch_seq != null ? Number(j.counts.max_batch_seq) : null }),
    keysOf: (j) => (j && Array.isArray(j.keys) ? j.keys.map((no) => `${mall}|${spec.scope}|${no}`) : null),
    iterate, inScope: inRange, build: (g, ctx) => spec.build(g, ctx, stats), transformVersion: spec.transformVersion,
    // 伝票との結び直し: 「要る」の印は最初の chunk の直前 (世代を取る取引) に書き、送り終えた後に lock の中で回す (Codex R2 #1〜#3)。続きの位置は HTTP 成功のたびに・時間予算で区切る (R3 #1)
    metaOnFirstChunk: relink ? RELINK_META_ON_SEND : null,
    afterSend: relink ? (ctx) => relinkAfterPush({ ledger, owner: ctx.owner, fetchImpl: ctx.fetchImpl, base, syncKey, limit: relinkLimit, maxCalls: relinkMaxCalls, budgetMs: relinkBudgetMs, log: ctx.log, now: ctx.now, mustOwn: ctx.mustOwn }) : null,
    ...rest,
  });
}

function parseArgs(argv) {
  const out = { mall: null, incremental: false, dryRun: false, force: false, reconcile: false, relink: false, all: false, resetLedger: false, from: null, to: null, days: null, dataDir: null, chunk: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--mall') out.mall = argv[++i];
    else if (a === '--incremental') out.incremental = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--force') out.force = true;
    else if (a === '--reconcile') out.reconcile = true;
    else if (a === '--relink') out.relink = true;
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
  const base = syncBase(), syncKey = process.env.MIRROR_SYNC_KEY || '';
  if (a.relink && !a.mall) {
    const r = await relinkShipments({ base, syncKey });
    console.log(`${r.complete ? '✅' : '⚠️'} 結び直し: ${r.linked} 件 (見た伝票 ${r.examined}、${r.calls} 回)${r.complete ? '' : ` 打ち切り = 続きは shipment_id > ${r.next} (もう一度流す)`}`);
    process.exitCode = r.complete ? 0 : 1;
    return;
  }
  if (!a.mall || !MALL_SPECS[a.mall]) throw new Error(`--mall を指定する (${Object.keys(MALL_SPECS).join(' / ')})`);
  const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${a.chunk ?? process.env.CDB_PUSH_CHUNK} (1〜${MAX_CHUNK})`);
  if ((a.from && !a.to) || (!a.from && a.to)) throw new Error('--from と --to は組で');
  if (a.from && (!isDate(a.from) || !isDate(a.to) || a.from > a.to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });   // 読むだけ
  const ledger = openLedger(dataDir, { kind: `order:${a.mall}` });
  try {
    if (a.resetLedger) { const n = ledger.resetFingerprints(); console.log(`台帳 (${a.mall}) の指紋を空にした: ${n} 件 (鍵は残す)。次の --incremental で全部送り直す ('same' が返るだけ)`); return; }
    if (a.reconcile) {
      let from = a.from, to = a.to;
      if (a.all) { from = DEFAULT_FLOOR; to = jstDate(0); }
      else if (!from) { const days = a.days != null ? Number(a.days) : 90; if (!Number.isInteger(days) || days < 1 || days > 730) throw new Error('--days は 1〜730'); from = jstDate(-(days - 1)); to = jstDate(0); }
      const rr = await reconcileOrdersDaily({ mall: a.mall, warehouse, base, syncKey, from, to });
      process.exitCode = rr.ok ? 0 : 1;
      return;
    }
    if (!a.incremental && !a.from) throw new Error('--incremental か --from/--to を指定する (daily-sync は --incremental)');
    const r = await pushOrders({ mall: a.mall, warehouse, ledger, base, syncKey, chunkSize, dryRun: a.dryRun, force: a.force, from: a.from, to: a.to });
    if (r.stats && (r.stats.sentinel || r.stats.negative)) console.log(`  金額を null にした: 番兵 (-9999) ${r.stats.sentinel} 個 / 負 ${r.stats.negative} 個`);
    const rl = r.afterSend || { ran: false, pending: false, result: null, error: null };   // 結び直しは runPush の中 (lock の中) で済んでいる
    const relinkNote = !rl.ran ? '' : rl.error ? ` / ❌ 伝票の結び直しに失敗 (${rl.error.slice(0, 120)}。次の run でやり直す)` : ` / 伝票の結び直し ${rl.result.linked} 件${rl.pending ? ' (打ち切り。次の run で続きから)' : ''}`;
    console.log(summarizePush(r, MALL_SPECS[a.mall].label) + relinkNote);
    const success = r.lockedBy ? false : (r.dryRun ? r.transformErrors.length === 0 : r.ok);
    process.exitCode = success ? 0 : 1;
  } finally { ledger.close(); warehouse.close(); }
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
if (isMain) main().catch((e) => { console.error(`❌ Company DB 注文 push: ${e.message}`); process.exit(1); });
