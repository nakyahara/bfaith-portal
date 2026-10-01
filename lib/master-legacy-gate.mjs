/**
 * master-legacy-gate.mjs — 商品・仕入先マスタの古い入口の門 (Company DB構想 14 §5・§10 契約 v3 H1・§9 v2 M2 / PR #1565 Codex R1・中間レビュー)
 *
 * 切替の段階 (ops.master_cutover_state。lib/master-cutover.mjs) が legacy_open のときだけ、古い入口
 * (config/master-legacy-entries.mjs の一覧) で書ける。frozen / company_owner / new_open は書けない
 * = 持ち主表 (config/master-ownership.mjs) がまだ 'load' でも閉じる (契約 v3 H1)。
 * 🚨 段階が読めない (接続先が無い・つながらない・表が無い・壊れた値) = 書けない (fail-closed)。
 *    書き込み (API・CLI) は**毎回**読む。前に読めた値は使わない。途切れには接続の使い回し (プール 1 本) と短い読み直し (1 回・200ms) で備える
 *    🚨 読めないときは「閉じた」ではなく「分からない」= 要求ごと 503 (一部だけ書かない・別の税率に切り替える、はしない = 切替前の瞬断で黙って違う値を作らない)
 *
 * 段階の読み方 (⑤-1 のロールをそのまま使う):
 *   門のロール (⑤-1: グループ master_gate・ログインは場所ごと) = Render は COMPANY_DB_MASTER_GATE_RENDER_URL (master_gate_render)・
 *   miniPC は COMPANY_DB_MASTER_GATE_MINIPC_URL (master_gate_minipc)。記録の関数は「ログインの役 = master_gate_ + 場所」でないと拒む
 *   (Render の資格で minipc を名乗れない) → 無ければ COMPANY_DB_URL (Render)。🚨 見張りの watcher (接続 3 本まで) には落ちない (見張り・照合と食い合わない)
 *   プロセスごとに接続 1 本のプールを 10 分つないだままにする (miniPC → Render の PostgreSQL はつなぎ直すと TLS と認証で 0.5〜0.8 秒かかる)。
 *   書き込みはそれでも毎回、段階を読み直す (つないだままなのは接続だけ)
 *   画面 (帯を出すかどうか) だけは 30 秒前の結果を使う。30 秒を過ぎたら、同時に来た画面は 1 回の読みを分け合い、1 秒待つ (読み直さない)。
 *   1 秒で返らない = その画面だけ 5 分前までの結果を使う (無ければ読めない扱い)。遅れて返った結果は使い回しに入れる (打ち切りを「読めない」として 30 秒残さない)
 *
 * drain: 門を通った書き込みを、終わるまでプロセスの中で数える (件数・いちばん古い開始)。門の記録と読み戻しに出す。
 *   終わり = ハンドラが応答を返したとき (res.end) と legacyHandler で包んだ promise が終わったとき。相手が切れた (close) では減らさない。
 *   外の API を待ってから書くハンドラは書く直前に legacyWriteFence。定期実行は runLegacyJob の中 (Codex #1565 R2 High 1・Medium 2)
 *   段階を読んでいる間に相手が切れた要求は書かない (数えもしない)。時間のかかる取込 (CSV) は書く直前にもう一度読む (legacyRecheck)。
 *   CLI は書いている間、段階の鍵 (hashtext('ops.master_cutover')) を共有で持つ = 段階を変える関数 (排他) は CLI が書き終わるまで待つ (runWithLegacyCliLock)
 * 門の記録 (ack): 起動のときと、要求が来たついでに 5 分おきに、⑤-1 の ops.record_legacy_gate_ack (lib/master-cutover.mjs の recordLegacyGateAck。
 *   場所ごとの門のログイン) で書く。中身 = host・プロセスの名札・build の番号・古い入口の一覧 (manifest)・持ち主表のハッシュ・見た段階・書きかけ。
 *   書く前に確かめる (場所・build の番号・段階を読める・書く接続先・関数がある)。返事 (ack_id・manifest_hash・acked_at) を確かめてから「書けた」にする。
 *   止めるとき (SIGTERM / SIGINT) は、書き直しをやめ・新しい書き込みを 503 にし・切符を止め・書いている途中の記録と書きかけ (0 になるまで) を待ってから
 *   「止めた」(stopped: true・理由) を 1 回 (長くても timeoutMs)。止めている途中は await の後・切符を出す直前・書く直前の確かめで毎回見る (Codex #1565 R3 Medium)。
 *   記録も段階を読むプールの 1 本で書く = プロセスあたり接続 1 本 (Codex #1565 R2 Medium 3)。止まったまま書けなかったプロセスは
 *   scripts/company-db/master-legacy-instance.mjs で人が「止めた」を書く (⑤-1 の段階の関数は、今までに記録を書いたプロセスで、15 分以内の記録も「止めた」も無いものがあれば進めない = 何日前でも)
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readCutoverPhase, legacyWritable, CUTOVER_PHASES, PHASE_LABELS, ownershipHash, recordLegacyGateAck, manifestHashOf } from './master-cutover.mjs';
import { LEGACY_ENTRIES, LEGACY_EXEMPT, MASTER_EDIT_URL, entriesForApp, entryById } from '../config/master-legacy-entries.mjs';
import { isRender } from './is-render.js';

export { MASTER_EDIT_URL };
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const SCREEN_FRESH_MS = 30 * 1000;
export const SCREEN_READ_TIMEOUT_MS = 1000;
export const ACK_REFRESH_MS = 5 * 60 * 1000;
export const CLI_EXIT_CODE = 3;
const CONNECT_TIMEOUT_MS = 3000;
const STATEMENT_TIMEOUT_MS = 5000;
const POOL_MAX = 1;
const POOL_IDLE_MS = 10 * 60 * 1000;          // 10 分使わなければ閉じる (要求が来れば 5 分おきの門の記録の確かめでも使う = ふだんはつないだまま)
const SCREEN_UNREADABLE_FRESH_MS = 5 * 1000;  // 画面: 「読めない」は 5 秒だけ使い回す (直ったらすぐ帯を外す)
const SCREEN_STALE_MAX_MS = 5 * 60 * 1000;    // 画面: 1 秒で返らないとき、この古さまでの結果を使う (見せ方だけ・書き込みは毎回読む)
const READ_ATTEMPTS = 2;
const RETRY_DELAY_MS = 200;
const SLOW_READ_MS = 1000;
const CUTOVER_LOCK_KEY = `hashtext('ops.master_cutover')`;

export const FROZEN_MESSAGE = 'マスタは新しい画面で直します。ここでは書けません (切替のため閉じました)';
export const UNREADABLE_MESSAGE = '切替の段階を読めないので、マスタの書き込みを止めています。少し待ってもう一度。続くときは管理者へ';
export const SHUTTING_DOWN_MESSAGE = 'このサーバーは止めている途中なので、マスタの書き込みを止めています。少し待ってもう一度';
export const UNREADABLE_DRY_RUN_WARNING = '切替の段階を読めません。試し (書かない) だけ見せています。本当に取り込むのは段階を読めるようになってから';
/** 書かない試し (dry run) か。入口の dry_run = 'body_true' (本文の dry_run が true) / 'body_not_false' (本文の dry_run が false でなければ) */
function isDryRun(entry, req) {
  const v = req.body && typeof req.body === 'object' ? req.body.dry_run : undefined;
  if (entry.dry_run === 'body_true') return v === true;
  if (entry.dry_run === 'body_not_false') return v !== false;
  return false;
}

/** ⑤-1 の門の記録の関数 (0051。10 個目までの引数 = 止めた・止めた理由)。無い = 0051 の前 = 書かない */
export const ACK_FUNCTION_SIGNATURE = 'ops.record_legacy_gate_ack(text,text,text,jsonb,text,text,integer,timestamptz,boolean,text)';
/** 止めた理由の長さ (0051 の ck_mlga_stopped = 1〜200 字) */
const STOPPED_REASON_MAX = 200;
/**
 * このプロセスが門の記録を書く場所 (server.js の起動・止めるとき・要求のついでと、読み戻しの API で同じ判定を使う)。
 * Render = 'render' / miniPC の WarehouseServer (PORTAL_VARIANT=warehouse) = 'minipc' / それ以外 (手元の PC など) = null = 書かない
 */
export function legacyAckHost(env = process.env) {
  if (isRender(env)) return 'render';
  return String(env.PORTAL_VARIANT || 'render').trim().toLowerCase() === 'warehouse' ? 'minipc' : null;
}
/** 場所ごとの門のログイン (⑤-1: master_gate_render / master_gate_minipc)。env の名前 */
export const GATE_URL_ENV = Object.freeze({ render: 'COMPANY_DB_MASTER_GATE_RENDER_URL', minipc: 'COMPANY_DB_MASTER_GATE_MINIPC_URL' });
/** その場所の門の接続先 (記録を書く。無ければ null = 書かない) */
export function gateUrlFor(host, env = process.env) {
  const k = GATE_URL_ENV[host];
  return k ? String(env[k] || '').trim() || null : null;
}
/** 段階を読む接続先 (門のログイン → 照会用のロール → 表の持ち主) */
export function phaseUrlFrom(env = process.env) {
  // 🚨 見張りの watcher (COMPANY_DB_WATCH_URL・接続 3 本まで) には落ちない (中間レビュー 2 回目 Low)
  return gateUrlFor('render', env) || gateUrlFor('minipc', env) || String(env.COMPANY_DB_URL || '').trim() || null;
}

// ─── 数える (遅さ・読めない・断った回数。読み戻しに出す) ───
const stats = { reads_ok: 0, reads_failed: 0, retries: 0, refused_410: 0, refused_503: 0, client_gone: 0, client_gone_while_writing: 0, writes_aborted: 0, screen_timeouts: 0, dry_run_passed: 0, last_latency_ms: null, max_latency_ms: 0, last_error: null, last_error_at: null };
export function legacyGateStats() { return { ...stats }; }

// ─── プール (プロセスごと・接続先ごとに 1 つ・接続 1 本) ───
let pool = null;
let poolUrl = null;
let pgModules = null;   // pg と接続の設定の関数 (読み込みは 1 回だけ)
async function poolFor(url) {
  if (pool && poolUrl === url) return pool;
  // 🚨 読み込みを待つのは作る前だけ。待った後にもう一度見る = 同時に来た最初の読みで 2 つ以上のプールを作らない
  //    (作ってしまうと捨てたプールの接続が残り、門のログインの接続の上限を食う。Codex #1565 R2 Medium 3 の試験で見つけた)
  if (!pgModules) {
    const [{ default: pg }, { pgClientOptions }] = await Promise.all([import('pg'), import('../scripts/company-db/migrate.mjs')]);
    pgModules = { pg, pgClientOptions };
  }
  if (pool && poolUrl === url) return pool;
  if (pool) { const old = pool; pool = null; old.end().catch(() => {}); }
  const { pg, pgClientOptions } = pgModules;
  const base = pgClientOptions(url);
  pool = new pg.Pool({
    ...base, connectionString: base.connectionString, ssl: base.ssl,
    max: POOL_MAX, idleTimeoutMillis: POOL_IDLE_MS, connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
    statement_timeout: STATEMENT_TIMEOUT_MS, query_timeout: STATEMENT_TIMEOUT_MS + 1000,
    application_name: 'master-legacy-gate', allowExitOnIdle: true,   // CLI が読み終わったら終われる (サーバーは HTTP が動いているので終わらない)
    keepAlive: true, keepAliveInitialDelayMillis: 30 * 1000,            // つないだままの接続を途中の機器に切られにくくする
  });
  pool.on('error', (e) => console.warn('[master-legacy-gate] 待機中の接続が切れた (次の読みでつなぎ直す):', e && e.message));
  poolUrl = url;
  return pool;
}
/** プールを閉じる (試験・CLI の終わり) */
export async function closeLegacyGatePool() {
  if (!pool) return;
  const p = pool; pool = null; poolUrl = null;
  try { await p.end(); } catch { /* 閉じるのに失敗しても結果は変えない */ }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** 本番の読み方: プールで段階を読む。attempts = 読む回数 (書き込みは 2 = 1 回読み直す / 画面は 1)。例外は投げない */
async function defaultReadPhase({ env = process.env, attempts = READ_ATTEMPTS } = {}) {
  const url = phaseUrlFrom(env);
  if (!url) return { readable: false, phase: null, error: `Company DB の接続先が無い (${GATE_URL_ENV.render} / ${GATE_URL_ENV.minipc} / COMPANY_DB_URL)` };
  let last = null;
  for (let i = 0; i < attempts; i++) {
    if (i > 0) { stats.retries++; await sleep(RETRY_DELAY_MS); }
    const t0 = Date.now();
    try {
      const p = await poolFor(url);
      last = await readCutoverPhase({ query: (text, params) => p.query(text, params) });
    } catch (e) {
      last = { readable: false, phase: null, error: String((e && e.message) || e).slice(0, 300) };
    }
    const ms = Date.now() - t0;
    stats.last_latency_ms = ms;
    stats.max_latency_ms = Math.max(stats.max_latency_ms, ms);
    if (ms >= SLOW_READ_MS) console.warn(`[master-legacy-gate] 段階の読みが遅い: ${ms}ms`);
    if (last && last.readable) return last;
  }
  return last;
}

// ─── 状態 (プロセスの中だけ) ───
let reader = defaultReadPhase;
let clock = () => Date.now();
let lastScreen = null;       // { state, at } 画面用の使い回し (読めた = 30 秒・読めない = 5 秒。書き込みは使わない)
let screenInflight = null;   // 画面用の読みの途中 (同時の画面で分け合う。1 秒で諦めた画面があっても、読み終わるまで残す)
let screenGen = 0;           // 読み方を差し替えた・使い回しを捨てた回数 (前の読みの遅れた結果を入れない)

/** 試験用: 段階の読み方を差し替える (本番では呼ばない)。差し替えると前の結果は捨てる。fn({ attempts }) */
export function __setLegacyPhaseReader(fn) { reader = fn || defaultReadPhase; lastScreen = null; screenInflight = null; screenGen++; }
export function __setLegacyClock(fn) { clock = fn || (() => Date.now()); }
export function __resetLegacyGate() { lastScreen = null; screenInflight = null; screenGen++; }

function toState(s, now) {
  if (s && s.readable === true && CUTOVER_PHASES.includes(s.phase)) {
    stats.reads_ok++;
    return { writable: legacyWritable(s), readable: true, phase: s.phase, source: 'db', error: null };
  }
  stats.reads_failed++;
  const err = (s && s.error) || '段階を読めない';
  stats.last_error = String(err).slice(0, 300); stats.last_error_at = new Date(now).toISOString();
  console.warn(`[master-legacy-gate] 段階を読めない (${stats.last_error}・累計 ${stats.reads_failed} 回)`);
  return { writable: false, readable: false, phase: null, source: 'unreadable', error: err };
}

/**
 * 画面用: 使い回しが切れたら、同時の画面は 1 回の読みを分け合い、1 秒待つ (読み直さない)。見せ方だけに使う。
 * 🚨 1 秒で返らない (中間レビュー 2 回目 M-A: つなぎ直しが遅い) = この画面だけ 5 分前までの結果を使う (無ければ読めない扱い)。
 *    打ち切りは使い回しに入れない。読みは続けて、遅れて返った結果を使い回しに入れる (= 次の画面は正しい結果)
 */
function isFresh(entry, now) {
  return !!entry && now - entry.at < (entry.state.readable ? SCREEN_FRESH_MS : SCREEN_UNREADABLE_FRESH_MS);
}
async function readForScreen(now) {
  if (!screenInflight) {
    const startedAt = now;
    const gen = screenGen;
    const p = Promise.resolve()
      .then(() => reader({ attempts: 1 }))
      .catch((e) => ({ readable: false, phase: null, error: String((e && e.message) || e) }))
      .then((s) => {
        const state = toState(s, clock());
        // 書き込みの読み (もっと新しい) が先に使い回しを更新していれば上書きしない
        if (gen === screenGen && (!lastScreen || lastScreen.at <= startedAt)) lastScreen = { state, at: clock() };
        return state;
      })
      .finally(() => { if (screenInflight === p) screenInflight = null; });
    screenInflight = p;
  }
  let timer;
  const timeout = new Promise((r) => { timer = setTimeout(() => r(null), SCREEN_READ_TIMEOUT_MS); });
  try {
    const state = await Promise.race([screenInflight, timeout]);
    if (state) return state;
  } finally { clearTimeout(timer); }
  stats.screen_timeouts++;
  if (lastScreen && lastScreen.state.readable && now - lastScreen.at < SCREEN_STALE_MAX_MS) {
    return { ...lastScreen.state, source: 'screen_stale' };
  }
  return { writable: false, readable: false, phase: null, source: 'screen_timeout', error: `画面の読みが ${SCREEN_READ_TIMEOUT_MS}ms で返らない` };
}

/**
 * 今、古い入口で書いてよいか。
 * purpose: 'write' (毎回読む・前の値を使わない・1 回読み直す) / 'screen' (30 秒使い回す・1 秒で打ち切る。帯を出すかだけに使う)
 * 戻り値 { writable, readable, phase, source: 'db'|'unreadable'|'screen_cache', error }
 */
export async function checkLegacyGate({ purpose = 'write' } = {}) {
  const now = clock();
  if (purpose === 'screen') {
    if (isFresh(lastScreen, now)) return { ...lastScreen.state, source: 'screen_cache' };
    return readForScreen(now);
  }
  let s;
  try { s = await reader({ attempts: READ_ATTEMPTS }); } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
  const state = toState(s, now);
  lastScreen = { state, at: now };
  return state;
}

/** 断るときの HTTP の答え [status, body] (段階が読めない = 503 / 閉じた = 410) */
export function refusal(state, entry) {
  if (state && state.error === 'shutting_down') {
    return [503, { error: 'master_shutting_down', message: SHUTTING_DOWN_MESSAGE, url: MASTER_EDIT_URL, entry: entry?.id ?? null }];
  }
  if (!state.readable) {
    return [503, { error: 'master_phase_unreadable', message: UNREADABLE_MESSAGE, url: MASTER_EDIT_URL, entry: entry?.id ?? null }];
  }
  return [410, { error: 'master_frozen', message: FROZEN_MESSAGE, url: MASTER_EDIT_URL, entry: entry?.id ?? null, phase: state.phase }];
}
function refuse(res, state, entry) {
  const [status, body] = refusal(state, entry);
  if (status === 503) stats.refused_503++; else stats.refused_410++;
  console.warn(`[master-legacy-gate] ${status} ${entry?.id ?? '?'} (410 累計 ${stats.refused_410}・503 累計 ${stats.refused_503})`);
  return res.status(status).json(body);
}

// ─── drain: 門を通った書き込みを数える ───
const inflight = new Map();
let inflightSeq = 0;
/** 書き込みを始めた (門を通った) = 終わりの関数を返す (何度呼んでもよい) */
export function beginLegacyWrite(entryId) {
  const id = ++inflightSeq;
  inflight.set(id, { entry: entryId, started_at: clock() });
  let done = false;
  return () => {
    if (done) return;
    done = true;
    inflight.delete(id);
    if (inflight.size === 0) { const ws = drainWaiters.splice(0); for (const w of ws) w(); }
  };
}
const drainWaiters = [];
/** 書きかけが 0 になったら解ける (止めるときに「止めた」を書く前に待つ) */
function whenInflightEmpty() {
  if (inflight.size === 0) return Promise.resolve();
  return new Promise((r) => drainWaiters.push(r));
}
/** いま書いている途中の件数・いちばん古い開始・入口ごとの件数 */
export function legacyInflight() {
  const rows = [...inflight.values()];
  const oldest = rows.reduce((m, r) => (m == null || r.started_at < m ? r.started_at : m), null);
  const byEntry = {};
  for (const r of rows) byEntry[r.entry] = (byEntry[r.entry] || 0) + 1;
  return { count: rows.length, oldest_started_at: oldest == null ? null : new Date(oldest).toISOString(), by_entry: byEntry };
}
/** 相手が切れた (段階を読んでいる間に接続が閉じた) か。🚨 req.destroyed は本文を読み終えると true になるので見ない */
const clientGone = (req, res) => !!(res.destroyed || res.writableEnded || (req.socket && req.socket.destroyed));
/**
 * 書き込みを通すとき: 相手がもう居なければ書かない (数えない)。居れば「書き込みの切符」を渡し、ハンドラが終わるまで数える。
 * 🚨 終わり = ハンドラが応答を返した (res.end) とき + legacyHandler で包んだハンドラの promise が終わったとき (Codex #1565 R2 High 1)。
 *    相手が切れた ('close') では減らさない: async のハンドラは相手が切れても動き続けて書く (例: Notion を待ってから SQLite)。
 *    その間に「書きかけ 0」の門の記録を書くと、最後の同期の後に書き込みが着く。相手が切れたら切符を止める (abort) = 書く直前の
 *    legacyWriteFence が止める。応答を返さないまま終わらないハンドラは数えが残る = 段階を進められない (安全側。読み戻しの oldest_started_at で見える)
 * 切符 = res.locals.masterLegacyTicket { entry, signal (AbortSignal), hold() }
 */
const liveTickets = new Set();   // 門を通って、まだ終わっていない書き込みの切符 (HTTP・定期実行。止めるときに全部止める)
/** 切符を出す (数える + 止められる)。🚨 出す直前に「止めている途中」を見る = 呼び手は await の後にここを通る (Codex #1565 R3 Medium) */
function issueTicket(entryId) {
  if (shuttingDown) return null;
  const endCount = beginLegacyWrite(entryId);
  const ac = new AbortController();
  let released = false;
  const t = {
    entry: entryId, signal: ac.signal,
    abort: (why) => { if (!ac.signal.aborted) ac.abort(why); },
    release: () => { if (released) return; released = true; liveTickets.delete(t); endCount(); },
  };
  liveTickets.add(t);
  return t;
}
const SHUTTING_DOWN_STATE = Object.freeze({ writable: false, readable: false, phase: null, error: 'shutting_down' });
function admitWrite(req, res, entryId) {
  if (clientGone(req, res)) {
    stats.client_gone++;
    console.warn(`[master-legacy-gate] 段階を読んでいる間に相手が切れた = 書かない (${entryId})`);
    return false;
  }
  const ticket = issueTicket(entryId);
  if (!ticket) { refuse(res, SHUTTING_DOWN_STATE, { id: entryId }); return false; }   // 段階を読んでいる間に止め始めた = 通さない
  let ended = false;
  let holds = 0;
  const maybeRelease = () => { if (ended && holds === 0) ticket.release(); };
  /** ハンドラの仕事を数える (legacyHandler が使う)。返した関数を呼ぶまで、応答を返しても数えたまま */
  ticket.hold = () => { holds++; let done = false; return () => { if (!done) { done = true; holds--; maybeRelease(); } }; };
  res.locals.masterLegacyTicket = ticket;
  const origEnd = res.end;
  res.end = function legacyTicketEnd(...args) {
    res.end = origEnd;
    ended = true;
    maybeRelease();
    return origEnd.apply(this, args);
  };
  res.on('close', () => {
    if (res.writableEnded) return;
    stats.client_gone_while_writing++;
    ticket.abort('client_gone');   // 数えは残す (ハンドラが終わるまで)。書く直前の legacyWriteFence が止める
  });
  return true;
}

/**
 * async のハンドラを包む: 門を通った書き込みを、ハンドラの promise が終わるまで数える (try/finally)。
 * 誤りは next へ渡す (Express 4 は async のハンドラの誤りを拾わない = 応答が返らず数えが残るのを防ぐ)。門を通っていない要求はそのまま
 */
export function legacyHandler(fn) {
  return function legacyHandlerWrapped(req, res, next) {
    const t = res.locals && res.locals.masterLegacyTicket;
    const release = t ? t.hold() : () => {};
    let p;
    try { p = Promise.resolve(fn(req, res, next)); } catch (e) { release(); return next(e); }
    return p.catch((e) => next(e)).finally(release);
  };
}

/**
 * 定期実行 (job) の書き込みを門で包む (Codex #1565 R2 Medium 2): 段階を毎回読み、legacy_open のときだけ fn を流し、
 * 終わるまで書きかけに数える (try/finally = 門の記録の書きかけと読み戻しの inflight.by_entry に出る)。閉じている・読めない・止めている途中 = 流さない。
 * 同じプロセスの門の記録が書きかけを運ぶので、段階の鍵 (CLI の共有の鍵) は持たない (⑤-1 は frozen の後の記録で書きかけ 0 を求める)。
 * 戻り値 { ran, state, result }
 */
export async function runLegacyJob(entryId, fn) {
  const entry = entryById(entryId);
  if (!entry || entry.kind !== 'job') throw new Error(`master-legacy-gate: job の入口が一覧に無い: ${entryId}`);
  if (shuttingDown) return { ran: false, state: SHUTTING_DOWN_STATE };
  const state = await checkLegacyGate({ purpose: 'write' });
  // 🚨 await の後にもう一度 (段階を読んでいる間に止め始めた = 流さない。Codex #1565 R3 Medium)
  if (shuttingDown) return { ran: false, state: SHUTTING_DOWN_STATE };
  if (!state.writable) return { ran: false, state };
  const ticket = issueTicket(entryId);
  if (!ticket) return { ran: false, state: SHUTTING_DOWN_STATE };
  // fn({ signal, fence }): 待ってから書く定期実行は、書く直前に fence() (止めている途中・段階が変わった = 投げる)。
  // 止めるときは切符を止め、定期実行が終わる (書きかけ 0) まで「止めた」を書かない
  try { return { ran: true, state, result: await fn({ signal: ticket.signal, fence: () => fenceTicket(ticket) }) }; } finally { ticket.release(); }
}

/** 書く直前の確かめで止めたときの誤り (code = 'master_legacy_aborted'・status / body = 返す答え) */
function abortedError(reason, state = null, entryId = null) {
  stats.writes_aborted++;
  const [status, body] = reason === 'client_gone'
    ? [499, { error: 'client_gone', message: '相手が切れたので書きませんでした', url: MASTER_EDIT_URL, entry: entryId }]
    : refusal(reason === 'shutting_down' ? SHUTTING_DOWN_STATE : (state || { readable: false }), { id: entryId });
  return Object.assign(new Error(`[master-legacy-gate] 書く直前に止めた (${reason}): ${body.message}`), { code: 'master_legacy_aborted', reason, status, body });
}
/**
 * 書く直前の確かめ (外の API を待ってから書くハンドラが、書く直前に呼ぶ)。相手が切れた・段階が変わった (読めない) = 投げる (書かない)。
 * 門を通っていない (一覧に無い・書かない試し) = 何もしない。戻り値なし。投げた誤りは code = 'master_legacy_aborted' (status / body つき)
 */
export async function legacyWriteFence(res) {
  const t = res && res.locals && res.locals.masterLegacyTicket;
  if (!t) return;
  await fenceTicket(t);
}
/** 切符の書く直前の確かめ。止めている途中・止められた切符・段階が閉じた / 読めない = 投げる (await の前と後の両方で見る) */
async function fenceTicket(t) {
  if (shuttingDown) t.abort('shutting_down');   // 「止めた」を書く (書いた) プロセスは、もう書かない
  if (t.signal.aborted) throw abortedError(String(t.signal.reason || 'aborted'), null, t.entry);
  const s = await checkLegacyGate({ purpose: 'write' });
  if (shuttingDown) t.abort('shutting_down');   // 段階を読んでいる間に止め始めた
  if (t.signal.aborted) throw abortedError(String(t.signal.reason || 'aborted'), null, t.entry);
  if (!s.writable) { t.abort(s.readable ? 'phase_closed' : 'phase_unreadable'); throw abortedError(s.readable ? 'phase_closed' : 'phase_unreadable', s, t.entry); }
}
/** legacyWriteFence が止めた誤りなら、その答えを返す (返したら true)。ハンドラの catch で使う */
export function respondIfLegacyAborted(res, e) {
  if (!e || e.code !== 'master_legacy_aborted') return false;
  console.warn(e.message);
  if (!res.headersSent && !res.writableEnded) res.status(e.status).json({ ok: false, ...e.body, error: e.body.error, message: e.body.message });
  else if (!res.writableEnded) res.end();
  return true;
}

/** '/api/shipping/:sku' → 正規表現 (Express 4 の既定と同じく大文字小文字を区別しない・末尾の / は任意・:param は名前つきの組) */
export function pathPattern(p) {
  const esc = String(p).split('/').map((seg) => (seg.startsWith(':') ? `(?<${seg.slice(1)}>[^/]+)` : seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))).join('/');
  return new RegExp(`^${esc === '/' ? '' : esc}/?$`, 'i');
}
const methodMatches = (entryMethod, reqMethod) => entryMethod === reqMethod || (entryMethod === 'GET' && reqMethod === 'HEAD');
const safeDecode = (s) => { try { return decodeURIComponent(s); } catch { return s; } };
/** 入口の when (道の一部の値で絞る: purchase-orders の /api/masters/:kind は kind = suppliers だけ) に合うか */
function whenMatches(entry, groups) {
  if (!entry.when) return true;
  const v = safeDecode(String(groups?.[entry.when.param] ?? '')).toLowerCase();
  return entry.when.in.includes(v);
}

/**
 * Express の門 (router の最初に置く)。config/master-legacy-entries.mjs の app の入口だけを見る。
 *   route        → 閉じていれば 410 / 読めない 503 (本文を読む前 = multer の取込より前に置ける)。通したら終わるまで数える (drain)
 *   route_field  → その欄を送ったときだけ同じ (本文を読んだ後に置く)
 *   route_part   → 段階が読めて legacy_open = そのまま / 読めて閉じている = 断らずにマスタの部分だけ書かない
 *                  (res.locals.masterLegacyWrite.writable = false) / 🚨 読めない = 要求ごと 503 (一部を黙って落とさない)
 *   screen       → res.locals.masterLegacy = 画面用の状態 (帯を出すか)。断らない
 * 一覧に無い要求は段階を読まずにそのまま通す
 */
export function masterLegacyGate(app) {
  const entries = entriesForApp(app).map((e) => ({ e, re: pathPattern(e.path) }));
  if (!entries.length) throw new Error(`master-legacy-gate: app ${app} の入口が一覧に無い`);
  return async function masterLegacyGateMw(req, res, next) {
    const hits = [];
    for (const x of entries) {
      if (!methodMatches(x.e.method, req.method)) continue;
      const m = x.re.exec(req.path);
      if (m && whenMatches(x.e, m.groups)) hits.push(x);
    }
    if (!hits.length) return next();
    try {
      const screen = hits.find((x) => x.e.kind === 'screen');
      const part = hits.find((x) => x.e.kind === 'route_part');
      const route = hits.find((x) => x.e.kind === 'route')
        || hits.find((x) => x.e.kind === 'route_field' && req.body && typeof req.body === 'object' && req.body[x.e.field] !== undefined);
      if ((route || part) && shuttingDown) {
        // 止めている途中 (「止めた」を書く・書いた) = 新しい書き込みを通さない (Codex #1565 R2 Medium 3)
        return refuse(res, { writable: false, readable: false, phase: null, error: 'shutting_down' }, (route || part).e);
      }
      // 段階を読む (await) のは先に全部済ませる。ここから next() までは await しない = 止めている途中の確かめ → 切符 → next を 1 続きで
      // (段階を読んでいる間に止め始めた要求は、切符を出さずに 503。Codex #1565 R3 Medium)
      const writeState = route || part ? await checkLegacyGate({ purpose: 'write' }) : null;
      const screenState = screen ? await checkLegacyGate({ purpose: 'screen' }) : null;
      if ((route || part) && shuttingDown) return clientGone(req, res) ? undefined : refuse(res, SHUTTING_DOWN_STATE, (route || part).e);
      if (route) {
        const state = writeState;
        if (!state.writable && !state.readable && isDryRun(route.e, req)) {
          // 書かない試し (dry run) = 段階を読めないときは注意つきで通す (中間レビュー 2 回目 Low)。閉じた後 (読めて frozen 以降) は 410 のまま
          stats.dry_run_passed++;
          res.set('X-Master-Legacy-Warning', 'phase_unreadable');
          res.locals.masterLegacyWarning = UNREADABLE_DRY_RUN_WARNING;
          console.warn(`[master-legacy-gate] 段階を読めないが、書かない試しなので通す (${route.e.id})`);
        } else {
          if (!state.writable) return clientGone(req, res) ? undefined : refuse(res, state, route.e);
          if (!admitWrite(req, res, route.e.id)) return undefined;
        }
      } else if (part) {
        const state = writeState;
        if (!state.readable) return clientGone(req, res) ? undefined : refuse(res, state, part.e);
        res.locals.masterLegacyWrite = state;
        if (!admitWrite(req, res, part.e.id)) return undefined;
      }
      if (screen) res.locals.masterLegacy = screenInfo(screenState);
      return next();
    } catch (e) {
      // 門そのものが壊れた = 書かせない (fail-closed)。画面は見せる
      console.error('[master-legacy-gate] 門の誤り:', e && e.message);
      if (hits.some((x) => x.e.kind !== 'screen')) return refuse(res, { writable: false, readable: false }, hits[0].e);
      res.locals.masterLegacy = screenInfo({ writable: false, readable: false, phase: null });
      return next();
    }
  };
}

/**
 * 書く直前にもう一度読む (時間のかかる取込 = CSV の受け取りの後・書く前)。route の multer の後に置く。
 * 閉じていれば受け取ったファイルを消して 410 / 503
 */
export function legacyRecheck(entryId) {
  const entry = entryById(entryId);
  if (!entry) throw new Error(`master-legacy-gate: 入口が一覧に無い: ${entryId}`);
  return async function legacyRecheckMw(req, res, next) {
    if (entry.when && !whenMatches(entry, req.params)) return next();   // 例: /api/masters/:kind/csv の kind が suppliers でない = マスタではない
    const t = res.locals && res.locals.masterLegacyTicket;
    let state;
    try { state = await checkLegacyGate({ purpose: 'write' }); } catch { state = { writable: false, readable: false }; }
    // await の後に全部見る (受け取っている間・読んでいる間に止め始めた・切符を止められた・相手が切れた・閉じた。Codex #1565 R3 Medium)。
    // ここから next() までは await しない
    if (shuttingDown && t) t.abort('shutting_down');
    const why = shuttingDown ? 'shutting_down'
      : t && t.signal.aborted ? String(t.signal.reason || 'aborted')
        : clientGone(req, res) ? 'client_gone'
          : !state.writable ? (state.readable ? 'phase_closed' : 'phase_unreadable') : null;
    if (!why) return next();
    for (const f of [req.file, ...(Array.isArray(req.files) ? req.files : [])]) {
      if (f && f.path) { try { fs.unlinkSync(f.path); } catch { /* 消せなくても書き込みはしない */ } }
    }
    if (why === 'client_gone') stats.client_gone++;
    // 答えを返す = 切符も返る (相手が切れていても返す。返さないと書きかけの数が残る)
    respondIfLegacyAborted(res, abortedError(why, state, entry.id));
    return undefined;
  };
}

/** 画面に渡す形 (frozen = 帯を出して書く部品を隠す) */
export function screenInfo(state) {
  const frozen = !state.writable;
  return {
    frozen, readable: !!state.readable, phase: state.phase ?? null,
    phase_label: state.phase ? PHASE_LABELS[state.phase] : null,
    url: MASTER_EDIT_URL,
    reason: !frozen ? null : (state.readable ? '切替のため、マスタはここでは直せません (見るだけ)' : '切替の段階を読めないので、ここでは書けません (見るだけ)'),
  };
}

const escHtml = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

/**
 * 帯の HTML (画面の body の最初に入れる)。frozen でなければ空文字 = 今までどおり。
 * hideSelectors = 隠す書く部品 (CSS の選択子。!important で JS の style.display にも勝つ)
 */
export function legacyBannerHtml(info, { hideSelectors = [] } = {}) {
  if (!info || !info.frozen) return '';
  const css = hideSelectors.length ? `<style>${hideSelectors.join(',')}{display:none !important}</style>` : '';
  return `${css}<div class="master-legacy-banner" role="status" style="background:#fff3cd;color:#664d03;border:1px solid #ffe69c;border-radius:6px;padding:10px 16px;margin:8px 16px;font-size:14px;line-height:1.6">`
    + `<a href="${escHtml(info.url)}" target="_blank" rel="noopener" style="color:#664d03;font-weight:bold">マスタは新しい画面で直します ↗</a>`
    + `<div style="font-size:12px">${escHtml(info.reason)}${info.phase_label ? ` (段階: ${escHtml(info.phase_label)})` : ''}</div></div>`;
}

// ─── CLI ───
async function loadDotenvIfNeeded(env) {
  if (reader === defaultReadPhase && !phaseUrlFrom(env) && env === process.env) {
    try { (await import('dotenv')).config({ quiet: true }); } catch { /* dotenv が無くても下で「接続先が無い」として止める */ }
  }
}
function cliRefuse(entry, s, log) {
  const why = s && s.readable === true
    ? `段階が ${s.phase} (${PHASE_LABELS[s.phase] || '?'}) = 古い入口は閉じています`
    : `切替の段階を読めない (${(s && s.error) || '?'}) = 止めます (fail-closed)`;
  log(`❌ ${entry.file}${entry.mode && entry.mode !== '*' ? ` ${entry.mode}` : ''}: マスタは新しい画面で直します → ${MASTER_EDIT_URL}\n   ${why}。何も書いていません (終了コード ${CLI_EXIT_CODE})`);
  process.exitCode = CLI_EXIT_CODE;
  return false;
}
function cliEntryOrThrow(entryId) {
  const entry = entryById(entryId);
  if (!entry || entry.kind !== 'cli') throw new Error(`master-legacy-gate: CLI の入口が一覧に無い: ${entryId}`);
  return entry;
}

/**
 * CLI の門 (最初の確かめ)。mode が分かったらすぐ (引数・ファイルの検査より前・データに触る前) に呼ぶ。
 * 閉じていれば理由を出して process.exitCode = 3 にし false を返す
 * (process.exit は呼ばない = 呼んだ側が何もしないで終わる。pg の直後の process.exit は Windows で 127 になる #1386)
 * 毎回読む (前の値は使わない)。接続先が env に無ければリポジトリ直下の .env を読む (daily-sync と同じ dotenv)
 */
export async function legacyCliGate(entryId, { env = process.env, log = console.error, read = null } = {}) {
  const entry = cliEntryOrThrow(entryId);
  await loadDotenvIfNeeded(env);
  let s;
  try {
    s = read ? await read() : (reader !== defaultReadPhase ? await reader({ attempts: READ_ATTEMPTS }) : await defaultReadPhase({ env }));
  } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
  // CLI はこの後プールを使わない (書く直前は runWithLegacyCliLock の専用の接続) = ここで閉じる。
  // 開いたままだと、後の process.exit で Windows の終了コード 127 になることがある (中間レビュー 2 回目 Low)
  if (!read && reader === defaultReadPhase) await closeLegacyGatePool();
  if (s && s.readable === true && legacyWritable(s)) return true;
  return cliRefuse(entry, s, log);
}

/**
 * CLI が書くところ。段階の鍵 (hashtext('ops.master_cutover')) を**共有**で持ったまま、同じ接続で段階を読み直し、legacy_open のときだけ fn を流す。
 * 段階を変える関数 (ops.set_master_cutover_phase = 排他の鍵) は、CLI が書き終わるまで待つ = CLI の書き込みが段階の変わり目をまたがない。
 * 鍵を持っている間は pg_locks に見える (別のプロセス = 読み戻しに出ない CLI が「書いている」と分かる)。
 * 戻り値 { ran, result }。閉じている・読めない = ran: false・process.exitCode = 3 (fn は流さない)
 */
export async function runWithLegacyCliLock(entryId, fn, { env = process.env, log = console.error } = {}) {
  const entry = cliEntryOrThrow(entryId);
  await loadDotenvIfNeeded(env);
  if (reader !== defaultReadPhase) {   // 試験で読み方を差し替えたとき (本番は差し替えない) = 鍵は取らずに読み直すだけ
    let s;
    try { s = await reader({ attempts: READ_ATTEMPTS }); } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
    if (!(s && s.readable === true && legacyWritable(s))) return { ran: false, refused: cliRefuse(entry, s, log) };
    return { ran: true, result: await fn() };
  }
  const url = phaseUrlFrom(env);
  if (!url) return { ran: false, refused: cliRefuse(entry, { readable: false, error: 'Company DB の接続先が無い' }, log) };
  let client = null;
  let locked = false;
  try {
    const { openPgClient, pgAdapter } = await import('../scripts/company-db/migrate.mjs');
    client = await openPgClient(url, { application_name: `master-legacy-cli:${path.basename(entry.file)}`, connectionTimeoutMillis: CONNECT_TIMEOUT_MS, statement_timeout: 60 * 1000 });
    await client.query(`select pg_advisory_lock_shared(${CUTOVER_LOCK_KEY})`);   // 段階を変えている最中なら、終わるまで待つ (長くても 60 秒で打ち切り = 読めない扱い)
    locked = true;
    const s = await readCutoverPhase(pgAdapter(client));
    if (!(s.readable === true && legacyWritable(s))) return { ran: false, refused: cliRefuse(entry, s, log) };
    return { ran: true, result: await fn() };
  } catch (e) {
    if (!locked) return { ran: false, refused: cliRefuse(entry, { readable: false, error: String((e && e.message) || e) }, log) };
    throw e;   // 書いている途中の誤りはそのまま (呼び手の今までの扱い)
  } finally {
    if (client) {
      if (locked) { try { await client.query(`select pg_advisory_unlock_shared(${CUTOVER_LOCK_KEY})`); } catch { /* 切れれば鍵も外れる */ } }
      try { await client.end(); } catch { /* 結果は変えない */ }
    }
  }
}

// ─── 門の記録 (ack) ───
const BOOT_ID = crypto.randomBytes(4).toString('hex');
/** プロセスごとの名札 (Render の RENDER_INSTANCE_ID か PC 名・pid・起動の乱数。⑤-1 の形 = 英数字と _.:- で 100 字まで) */
export function instanceId(env = process.env) {
  return `${env.RENDER_INSTANCE_ID || os.hostname()}:${process.pid}:${BOOT_ID}`.replace(/[^A-Za-z0-9_.:-]/g, '_').slice(-100);
}
/**
 * 古い入口の一覧 (manifest) = ⑤-1 の形 { entries: [{ id, kind: 'code' | 'manual', ... }] }。
 *   code = コードで閉じる入口 (LEGACY_ENTRIES)・manual = 機械では閉じられない入口 (NE の画面・GAS = 切替の証拠 manual_entries_stopped と同じ集合)。
 *   閉じない口 (写し・閉じ済み) は exempt に載せるだけ (入口ではない)。ハッシュは DB が計算する (ops.legacy_manifest_hash)
 */
export function legacyManifest() {
  const pick = (e) => Object.fromEntries(['type', 'host', 'app', 'file', 'method', 'path', 'when', 'field', 'part', 'mode', 'recheck', 'when_frozen', 'guard', 'dry_run'].filter((k) => (k === 'type' ? e.kind : e[k]) !== undefined).map((k) => [k, k === 'type' ? e.kind : e[k]]));
  return {
    schema: 'master-legacy-manifest/2',
    entries: [
      ...LEGACY_ENTRIES.map((e) => ({ id: e.id, kind: 'code', ...pick(e) })),
      ...LEGACY_EXEMPT.filter((e) => e.kind === 'manual').map((e) => ({ id: e.id, kind: 'manual', host: e.host })),
    ],
    exempt: LEGACY_EXEMPT.filter((e) => e.kind !== 'manual').map((e) => ({ id: e.id, ...pick(e) })),
  };
}
/** git の HEAD を .git のファイルから読む (git が無い環境用) */
function headFromGitFiles(repoDir) {
  let gitDir = path.join(repoDir, '.git');
  if (fs.statSync(gitDir).isFile()) {   // worktree: "gitdir: <path>"
    const m = fs.readFileSync(gitDir, 'utf8').match(/^gitdir:\s*(.+)\s*$/m);
    if (!m) return null;
    gitDir = path.resolve(repoDir, m[1].trim());
  }
  const head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8').trim();
  if (/^[0-9a-f]{40}$/.test(head)) return head;
  const ref = (head.match(/^ref:\s*(.+)$/) || [])[1];
  if (!ref) return null;
  const common = fs.existsSync(path.join(gitDir, 'commondir')) ? path.resolve(gitDir, fs.readFileSync(path.join(gitDir, 'commondir'), 'utf8').trim()) : gitDir;
  for (const dir of [gitDir, common]) {
    const f = path.join(dir, ref);
    if (fs.existsSync(f)) { const v = fs.readFileSync(f, 'utf8').trim(); if (/^[0-9a-f]{40}$/.test(v)) return v; }
  }
  const packed = path.join(common, 'packed-refs');
  if (fs.existsSync(packed)) {
    for (const line of fs.readFileSync(packed, 'utf8').split(/\r?\n/)) {
      const [sha, name] = line.split(' ');
      if (name === ref && /^[0-9a-f]{40}$/.test(sha)) return sha;
    }
  }
  return null;
}
let buildIdCache;
/** 動いている build の番号 (Render = RENDER_GIT_COMMIT・miniPC = リポジトリの git HEAD)。分からなければ null (= 記録を書かない) */
export function resolveBuildId({ env = process.env, repoDir = REPO_ROOT, fresh = false } = {}) {
  if (!fresh && buildIdCache !== undefined && env === process.env && repoDir === REPO_ROOT) return buildIdCache;
  let id = null;
  const rc = String(env.RENDER_GIT_COMMIT || '').trim();
  if (/^[0-9a-f]{7,64}$/i.test(rc)) id = rc.toLowerCase();
  if (!id) {
    try {
      const out = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repoDir, encoding: 'utf8', timeout: 5000, windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'] }).trim();
      if (/^[0-9a-f]{40}$/.test(out)) id = out;
    } catch { /* git が無い → .git を読む */ }
  }
  if (!id) { try { id = headFromGitFiles(repoDir); } catch { id = null; } }
  if (env === process.env && repoDir === REPO_ROOT) buildIdCache = id;
  return id;
}

let ackState = { state: 'not_yet', at: null, detail: null, ack_id: null, manifest_hash: null };
let ackRunning = null;
let shuttingDown = false;   // 止めている途中 (SIGTERM / SIGINT の後) = 記録の書き直しをしない・新しい書き込みを通さない
let stoppedOnce = null;     // 「止めた」は 1 回だけ
let lastAckAttemptAt = 0;
let lastAckLogKey = null;
export function legacyAckState() { return { ...ackState }; }

/** 一覧と持ち主表の指紋 (切替の手順の「全部の環境で読み戻す」で、環境ごとに同じかを比べる。manifest_hash は最後に書けた記録の DB の値) */
export async function legacyGateFingerprint() {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  return {
    owner_hash: ownershipHash(MASTER_OWNERSHIP), manifest_hash: ackState.manifest_hash, manifest_entries: legacyManifest().entries.length,
    build_id: resolveBuildId(), instance_id: instanceId(),
  };
}

/**
 * 「この環境・このプロセスに古い入口の門を載せた」の記録 (ack) を Company DB に残す (⑤-1 の ops.record_legacy_gate_ack = recordLegacyGateAck)。
 * 書く前に確かめる: host・build の番号・段階を読める (= env・表・select の権限)・書く接続先 (master_gate)・関数がある。どれか欠ける = 書かない (理由を残す)。
 * 書いた後: 返事の ack_id・manifest_hash (DB が同じ一覧から計算した値と同じ)・acked_at を確かめてから 'acked'。
 * 見た段階が書く間に変わった (stale_phase) = 段階を読み直して 1 回だけ書き直す。例外は投げない (起動・要求を止めない)。
 * stopped = true: 「止めた」の記録 (理由が要る・書きかけ 0)。instance = 人が別のプロセスを「止めた」にするときの名札 (既定 = このプロセス)
 * 戻り値 { state: 'acked'|'stopped'|'precheck_failed'|'no_function'|'bad_reply'|'error', detail, ack_id, manifest_hash }
 */
export async function ackLegacyGates({ host, env = process.env, connect = null, repoDir = REPO_ROOT, stopped = false, stoppedReason = null, instance = null } = {}) {
  const done = (state, detail, ack_id = null, manifest_hash = null) => {
    // 別のプロセスの名札で書いた (人の --stop・次の起動で前のプロセスを「止めた」にする) = このプロセスの門の記録の状態は変えない
    if (instance) {
      if (state !== 'stopped') console.error(`[master-legacy-gate] ❌ ${instance} の「止めた」を書けない (${state}): ${detail}`);
      return { state, at: new Date(clock()).toISOString(), detail, ack_id, manifest_hash };
    }
    ackState = { state, at: new Date(clock()).toISOString(), detail, ack_id, manifest_hash: manifest_hash ?? (state === 'acked' ? null : ackState.manifest_hash) };
    // 同じ失敗を 5 分ごとに並べない (状態か理由が変わったときだけ出す)。関数がまだ無い = 1 回だけ注意
    const key = `${state}\0${detail}`;
    if (state !== 'acked' && state !== 'stopped' && key !== lastAckLogKey) {
      (state === 'no_function' ? console.warn : console.error)(`[master-legacy-gate] ${state === 'no_function' ? '⚠️' : '❌'} 門の記録を書けない (${state}): ${detail}`);
    }
    lastAckLogKey = state === 'acked' || state === 'stopped' ? null : key;
    return { ...ackState };
  };
  if (host !== 'render' && host !== 'minipc') return done('precheck_failed', `知らない場所: ${host}`);
  const buildId = resolveBuildId({ env, repoDir });
  if (!buildId) return done('precheck_failed', 'build の番号が分からない (RENDER_GIT_COMMIT も git の HEAD も読めない)');
  let s = await checkLegacyGate({ purpose: 'write' });
  if (!s.readable) return done('precheck_failed', `段階を読めない (${s.error}) = env・表・select の権限を確かめる`);
  const url = gateUrlFor(host, env);
  if (!url && !connect) return done('precheck_failed', `記録を書く接続先が無い (${GATE_URL_ENV[host]} = ⑤-1 の門のログイン master_gate_${host})`);
  if (stopped && !(typeof stoppedReason === 'string' && stoppedReason.trim())) return done('precheck_failed', '「止めた」には理由が要る');
  let conn = null;
  try {
    if (connect) conn = await connect();
    else if (reader === defaultReadPhase && url === phaseUrlFrom(process.env)) {
      // 門のログインで段階も読んでいる = 同じプール (1 本) で書く = プロセスあたり接続 1 本 (配り直しで古い + 新しいが重なっても 2 本。Codex #1565 R2 Medium 3)
      const p = await poolFor(url);
      conn = { db: { query: (text, params) => p.query(text, params) }, close: async () => {} };
    } else {
      const { openPgClient, pgAdapter } = await import('../scripts/company-db/migrate.mjs');
      const client = await openPgClient(url, { application_name: 'master-legacy-gate-ack', connectionTimeoutMillis: CONNECT_TIMEOUT_MS, statement_timeout: STATEMENT_TIMEOUT_MS, query_timeout: STATEMENT_TIMEOUT_MS + 1000 });
      conn = { db: pgAdapter(client), close: () => client.end() };
    }
    const has = (await conn.db.query('select to_regprocedure($1) is not null as ok', [ACK_FUNCTION_SIGNATURE])).rows[0]?.ok;
    if (!has) return done('no_function', `${ACK_FUNCTION_SIGNATURE} が無い (0051 の前)`);
    const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
    const manifest = legacyManifest();
    const expectHash = await manifestHashOf(conn.db, manifest);
    let r = null;
    for (let i = 0; i < 2; i++) {
      const infl = legacyInflight();
      try {
        r = await recordLegacyGateAck(conn.db, {
          host, instanceId: instance || instanceId(env), buildId, manifest, ownership: MASTER_OWNERSHIP, phaseSeen: s.phase,
          // 「止めた」= このプロセスはもう書かない (書きかけは数えない)。人が別のプロセスを「止めた」にするときも 0
          inflightCount: stopped ? 0 : infl.count, oldestInflightAt: stopped ? null : infl.oldest_started_at,
          ...(stopped ? { stopped: true, stoppedReason: stoppedReason.trim().slice(0, STOPPED_REASON_MAX) } : {}),
        });
        break;
      } catch (e) {
        if (i === 0 && /stale_phase/.test(String(e && e.message))) {   // 書く間に段階が変わった = 読み直して 1 回だけ
          s = await checkLegacyGate({ purpose: 'write' });
          if (!s.readable) return done('precheck_failed', `段階を読めない (${s.error})`);
          continue;
        }
        throw e;
      }
    }
    // 返事の stopped も確かめる (「止めた」を頼んだのに普通の記録になった = 段階を進める門が黙っているプロセスとして止める。その逆も)
    const ok = r && r.ack_id != null && /^\d+$/.test(String(r.ack_id)) && r.manifest_hash === expectHash && typeof r.manifest_hash === 'string' && /^[0-9a-f]{64}$/.test(r.manifest_hash) && r.acked_at
      && r.stopped === (stopped === true);
    if (!ok) return done('bad_reply', `記録の返事が違う: ${JSON.stringify(r).slice(0, 200)}`);
    return done(stopped ? 'stopped' : 'acked', `${host}${instance ? ` ${instance}` : ''} build=${buildId.slice(0, 12)} manifest=${r.manifest_hash.slice(0, 12)} phase=${s.phase} ${stopped ? `止めた (${stoppedReason})` : `書きかけ=${legacyInflight().count}`}`, String(r.ack_id), r.manifest_hash);
  } catch (e) {
    const msg = String((e && e.message) || e).slice(0, 300);
    // ログインと場所が違う (⑤-1 の関数が 42501 gate_host_mismatch で拒む) = env の取り違え。直し方を添える
    if (/gate_host_mismatch/.test(msg)) return done('error', `${msg} (${GATE_URL_ENV[host]} は master_gate_${host} のログインにする)`);
    return done('error', msg);
  } finally {
    if (conn) { try { await conn.close(); } catch { /* 結果は変えない */ } }
  }
}

/** 5 分より前の記録なら、裏で書き直す (待たない・同時に 2 本走らせない)。起動と、要求が来たついで (legacyAckHeartbeat) に呼ぶ */
export function maybeRefreshLegacyAck({ host, env = process.env, force = false, connect = null } = {}) {
  if (!host || shuttingDown) return null;   // 止めている途中は書き直さない (「止めた」の後に普通の記録が着かない)
  const now = clock();
  if (ackRunning) return ackRunning;
  if (!force && now - lastAckAttemptAt < ACK_REFRESH_MS) return null;
  lastAckAttemptAt = now;
  ackRunning = ackLegacyGates({ host, env, connect }).catch((e) => ({ state: 'error', detail: String(e && e.message) })).finally(() => { ackRunning = null; });
  return ackRunning;
}
/**
 * 止めるとき (server.js の SIGTERM / SIGINT) に「止めた」を書く。長くても timeoutMs で諦める (止まるのを待たせない)。
 * 書けなかった = このプロセスは「15 分以内の記録も止めた も無い」になる → 人が scripts/company-db/master-legacy-instance.mjs で「止めた」を書く
 */
export function ackLegacyGatesStopped({ host, reason, env = process.env, timeoutMs = 5000, connect = null } = {}) {
  // 順番 (Codex #1565 R2 Medium 3): (1) 止めている途中の印 = 記録の書き直しをやめ、新しい書き込みを 503・生きている切符を止める
  //   (2) 書いている途中の普通の記録を待つ (3) 「止めた」を 1 回だけ書く (4) プールを閉じる。全部で長くても timeoutMs。
  //   途中の普通の記録が時間内に終わらない = 「止めた」は書かない (「止めた」の後に普通の記録が着くと、最後の記録が stopped = false に戻る)
  shuttingDown = true;
  for (const t of liveTickets) t.abort('shutting_down');
  if (stoppedOnce) return stoppedOnce;
  stoppedOnce = (async () => {
    if (!host) return { state: 'precheck_failed', detail: 'host が無い' };
    const deadline = Date.now() + timeoutMs;
    const TIMEOUT = Symbol('timeout');
    const within = async (p) => {
      let timer;
      const t = new Promise((r) => { timer = setTimeout(() => r(TIMEOUT), Math.max(0, deadline - Date.now())); });
      try { return await Promise.race([p, t]); } finally { clearTimeout(timer); }
    };
    if (ackRunning && (await within(ackRunning)) === TIMEOUT) {
      return { state: 'error', detail: `書いている途中の門の記録が ${timeoutMs}ms で終わらない = 「止めた」は書かない (順番を守る。次の起動か --stop で書く)` };
    }
    // 書きかけ (HTTP の切符・定期実行) が 0 になるまで待つ。切符は止めたので、書く直前の確かめ (fence・recheck) で止まって早く終わる。
    // 終わらない = 「止めた」(書きかけ 0) を書かない (本当は書きかけがあるのに 0 と書かない。Codex #1565 R3 Medium)
    if ((await within(whenInflightEmpty())) === TIMEOUT) {
      return { state: 'error', detail: `書きかけ ${legacyInflight().count} 件が ${timeoutMs}ms で終わらない = 「止めた」は書かない (次の起動か --stop で書く)` };
    }
    const r = await within(ackLegacyGates({ host, env, connect, stopped: true, stoppedReason: reason }));
    if (r === TIMEOUT) return { state: 'error', detail: `${timeoutMs}ms で「止めた」を書けなかった` };
    return r;
  })().finally(() => closeLegacyGatePool());
  return stoppedOnce;
}
/** 止めている途中か (読み戻し・試験) */
export function legacyShuttingDown() { return shuttingDown; }

/** プロセスが生きているか (pid)。使い回された pid も「生きている」になる = 安全側 (書かない) */
function pidAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return !!(e && e.code === 'EPERM'); }
}
/** miniPC の WarehouseServer の前の起動の名札を残すファイル (DATA_DIR の中) */
export function instanceStateFile(env = process.env) {
  return path.join(String(env.DATA_DIR || '').trim() || path.join(REPO_ROOT, 'data'), 'master-legacy-instance.json');
}
/**
 * 次の起動で、前の起動 (同じ PC・同じ場所) のプロセスが「止めた」を書かずに消えていたら「止めた」を書く (中間レビュー 2 回目 Low)。
 * なぜ: miniPC の WarehouseServer は WinSW のサービス。止めるときに Ctrl+C (Node では SIGINT) を送るはずだが、届いたか・2 秒で書けたかは
 *   ここからは分からない。「止めた」が無いと、ずっと「黙っているプロセス」として段階を進める関数 (⑤-1) が止まる (年齢では外れない)。
 * 🚨 安全: 前のプロセスの pid がまだある = 書かない (生きているのに「止めた」にすると、段階を進める門が古い版を見落とす)。
 *    pid が別のプロセスに使い回されていても「ある」= 書かない (安全側。人が --list で見て --stop)。PC 名が違う・場所が違う = 書かない。
 * Render は使わない (ファイルが起動ごとに消える・Render は止めるとき SIGTERM を送り 30 秒待つ)。
 * 戻り値 { state: 'stopped'|'skipped'|(ackLegacyGates の state), detail }。今の名札は必ず残す (書けなくても次の起動が確かめる)
 */
export async function markPreviousInstanceStopped({ host, env = process.env, stateFile = instanceStateFile(env), isAlive = pidAlive, connect = null } = {}) {
  let prev = null;
  try { prev = JSON.parse(fs.readFileSync(stateFile, 'utf8')); } catch { prev = null; }
  const me = { host, instance_id: instanceId(env), pid: process.pid, hostname: os.hostname(), started_at: new Date(clock()).toISOString() };
  let result = { state: 'skipped', detail: '前の起動の名札が無い' };
  if (prev && typeof prev === 'object' && prev.instance_id && prev.instance_id !== me.instance_id) {
    if (prev.host !== host || prev.hostname !== me.hostname) result = { state: 'skipped', detail: `前の名札は別の場所 / PC (${prev.host} / ${prev.hostname}) = 書かない` };
    else if (!Number.isInteger(prev.pid) || prev.pid === process.pid || isAlive(prev.pid)) result = { state: 'skipped', detail: `前のプロセス (pid ${prev.pid}) がまだある = 書かない (--list で見て --stop)` };
    else {
      result = await ackLegacyGates({ host, env, connect, stopped: true, instance: prev.instance_id,
        stoppedReason: `次の起動で前のプロセスが居ないのを確かめた (pid ${prev.pid}・${String(prev.started_at || '?').slice(0, 25)} 起動)` });
    }
  }
  try { fs.mkdirSync(path.dirname(stateFile), { recursive: true }); fs.writeFileSync(stateFile, JSON.stringify(me)); } catch (e) { console.warn('[master-legacy-gate] 名札を残せない:', e && e.message); }
  return result;
}

/** 試験用 */
export function __resetLegacyAck() { ackState = { state: 'not_yet', at: null, detail: null, ack_id: null, manifest_hash: null }; ackRunning = null; lastAckAttemptAt = 0; lastAckLogKey = null; shuttingDown = false; stoppedOnce = null; }

/** server.js の app.use に置く: 要求が来たついでに門の記録を 5 分おきに書き直す (新しい定期実行は作らない。死活の確かめの要求でも回る) */
export function legacyAckHeartbeat(host) {
  return function legacyAckHeartbeatMw(req, res, next) {
    try { maybeRefreshLegacyAck({ host }); } catch { /* 要求は止めない */ }
    next();
  };
}

/** 読み戻し用 (この環境・このプロセスが見ている段階・書きかけ・門の記録。段階を毎回読み、門の記録も書き直す) */
export async function legacyGateStatus({ host = null } = {}) {
  const state = await checkLegacyGate({ purpose: 'write' });
  const ack = host ? await (maybeRefreshLegacyAck({ host, force: true }) || Promise.resolve(legacyAckState())) : legacyAckState();
  return {
    host, pid: process.pid, checked_at: new Date(clock()).toISOString(),
    ...state, ...(await legacyGateFingerprint()),
    inflight: legacyInflight(), stats: legacyGateStats(), ack,
  };
}
