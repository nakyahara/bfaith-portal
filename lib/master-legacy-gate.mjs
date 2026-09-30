/**
 * master-legacy-gate.mjs — 商品・仕入先マスタの古い入口の門 (Company DB構想 14 §5・§10 契約 v3 H1・§9 v2 M2 / PR #1565 Codex R1)
 *
 * 切替の段階 (ops.master_cutover_state。lib/master-cutover.mjs) が legacy_open のときだけ、古い入口
 * (config/master-legacy-entries.mjs の一覧) で書ける。frozen / company_owner / new_open は書けない
 * = 持ち主表 (config/master-ownership.mjs) がまだ 'load' でも閉じる (契約 v3 H1)。
 * 🚨 段階が読めない (接続先が無い・つながらない・表が無い・壊れた値) = 書けない (fail-closed)。
 *    書き込み (API・CLI) は**毎回**読む。前に読めた値は使わない (R1 H1)。途切れには接続の使い回し (プール) と短い再試行 (1 回・200ms) で備える
 *
 * 段階の読み方 (どちらも新しい秘密は足さない。今ある接続をそのまま使う):
 *   - miniPC (WarehouseServer・CLI): COMPANY_DB_WATCH_URL (見張りと同じ照会用のロール watcher = select だけ。接続は 3 本まで)。無ければ COMPANY_DB_URL
 *   - Render: COMPANY_DB_URL。COMPANY_DB_WATCH_URL があればそちらを先に使う
 *   プロセスごとに接続 2 本までのプール (使い回す・接続 3 秒・文 5 秒で打ち切り)。段階そのものは毎回問い合わせる
 *   画面 (帯を出すかどうか) だけは 30 秒前の結果を使ってよい (見せ方だけ。書けるかは API が毎回決める)
 *
 * drain (R1 H3): 門を通った書き込みを、終わるまでプロセスの中で数える (件数・いちばん古い開始)。読み戻し (GET /apps/warehouse/api/master-legacy-gate)
 *   に出す = frozen の後、全部の場所で 0 件になってから最後の同期へ進む (db/company/README.md)。時間のかかる取込 (CSV) は書く直前にもう一度読む (legacyRecheck)
 * 門の記録 (ack・R1 H2): 起動のときと、要求が来たついでに 5 分おきに、⑤-1 の書き手の関数 (ACK_WRITER) で「この build・この一覧 (manifest_hash)・
 *   この持ち主表の門を持つプロセスが動いている・書きかけ何件」を書く。書く前に確かめる (段階を読める = env・表・select の権限・build の番号)。
 *   記録の表・段階を進める関数の確かめ (新しさ・全部のプロセス) は ⑤-1 (0050) の持ち物
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readCutoverPhase, legacyWritable, CUTOVER_PHASES, PHASE_LABELS, ownershipHash } from './master-cutover.mjs';
import { LEGACY_ENTRIES, LEGACY_EXEMPT, MASTER_EDIT_URL, entriesForApp, entryById } from '../config/master-legacy-entries.mjs';

export { MASTER_EDIT_URL };
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const SCREEN_FRESH_MS = 30 * 1000;
export const ACK_REFRESH_MS = 5 * 60 * 1000;
export const CLI_EXIT_CODE = 3;
const CONNECT_TIMEOUT_MS = 3000;
const STATEMENT_TIMEOUT_MS = 5000;
const POOL_MAX = 2;
const READ_ATTEMPTS = 2;
const RETRY_DELAY_MS = 200;
const SLOW_READ_MS = 1000;

export const FROZEN_MESSAGE = 'マスタは新しい画面で直します。ここでは書けません (切替のため閉じました)';
export const UNREADABLE_MESSAGE = '切替の段階を読めないので、マスタの書き込みを止めています。少し待ってもう一度。続くときは管理者へ';

/** 接続先 (照会用のロールを先に) */
export function phaseUrlFrom(env = process.env) {
  return String(env.COMPANY_DB_WATCH_URL || '').trim() || String(env.COMPANY_DB_URL || '').trim() || null;
}
/** 門の記録を書く接続先 (miniPC = 記録用のロール watch_writer・Render = 表の持ち主) */
export function ackUrlFrom(env = process.env) {
  return String(env.COMPANY_DB_WATCH_WRITER_URL || '').trim() || String(env.COMPANY_DB_URL || '').trim() || null;
}

// ─── 数える (M6: 遅さ・読めない・断った回数。読み戻しに出す) ───
const stats = { reads_ok: 0, reads_failed: 0, retries: 0, refused_410: 0, refused_503: 0, last_latency_ms: null, max_latency_ms: 0, last_error: null, last_error_at: null };
export function legacyGateStats() { return { ...stats }; }

// ─── プール (プロセスごと・接続先ごとに 1 つ) ───
let pool = null;
let poolUrl = null;
async function poolFor(url) {
  if (pool && poolUrl === url) return pool;
  if (pool) { const old = pool; pool = null; old.end().catch(() => {}); }
  const { default: pg } = await import('pg');
  const { pgClientOptions } = await import('../scripts/company-db/migrate.mjs');
  const base = pgClientOptions(url);
  pool = new pg.Pool({
    ...base, connectionString: base.connectionString, ssl: base.ssl,
    max: POOL_MAX, idleTimeoutMillis: 60 * 1000, connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
    statement_timeout: STATEMENT_TIMEOUT_MS, query_timeout: STATEMENT_TIMEOUT_MS + 1000,
    application_name: 'master-legacy-gate', allowExitOnIdle: true,   // CLI が読み終わったら終われる (サーバーは HTTP が動いているので終わらない)
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
/** 本番の読み方: プールで段階を読む。読めなければ 1 回だけ 200ms 後に読み直す。例外は投げない */
async function defaultReadPhase({ env = process.env } = {}) {
  const url = phaseUrlFrom(env);
  if (!url) return { readable: false, phase: null, error: 'Company DB の接続先が無い (COMPANY_DB_WATCH_URL / COMPANY_DB_URL)' };
  let last = null;
  for (let i = 0; i < READ_ATTEMPTS; i++) {
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
let lastScreen = null;    // { state, at } 画面用の 30 秒の使い回し (書き込みは使わない)

/** 試験用: 段階の読み方を差し替える (本番では呼ばない)。差し替えると前の結果は捨てる */
export function __setLegacyPhaseReader(fn) { reader = fn || defaultReadPhase; lastScreen = null; }
export function __setLegacyClock(fn) { clock = fn || (() => Date.now()); }
export function __resetLegacyGate() { lastScreen = null; }

/**
 * 今、古い入口で書いてよいか。
 * purpose: 'write' (毎回読む・前の値を使わない) / 'screen' (30 秒使い回す。帯を出すかだけに使う)
 * 戻り値 { writable, readable, phase, source: 'db'|'unreadable'|'screen_cache', error }
 */
export async function checkLegacyGate({ purpose = 'write' } = {}) {
  const now = clock();
  if (purpose === 'screen' && lastScreen && now - lastScreen.at < SCREEN_FRESH_MS) return { ...lastScreen.state, source: 'screen_cache' };
  let s;
  try { s = await reader(); } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
  let state;
  if (s && s.readable === true && CUTOVER_PHASES.includes(s.phase)) {
    stats.reads_ok++;
    state = { writable: legacyWritable(s), readable: true, phase: s.phase, source: 'db', error: null };
  } else {
    stats.reads_failed++;
    const err = (s && s.error) || '段階を読めない';
    stats.last_error = String(err).slice(0, 300); stats.last_error_at = new Date(now).toISOString();
    console.warn(`[master-legacy-gate] 段階を読めない = 閉じる (${stats.last_error}・累計 ${stats.reads_failed} 回)`);
    state = { writable: false, readable: false, phase: null, source: 'unreadable', error: err };
  }
  lastScreen = { state, at: now };
  return state;
}

/** 断るときの HTTP の答え [status, body] (段階が読めない = 503 / 閉じた = 410) */
export function refusal(state, entry) {
  if (!state.readable && !state.writable) {
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
  return () => { if (!done) { done = true; inflight.delete(id); } };
}
/** いま書いている途中の件数・いちばん古い開始・入口ごとの件数 */
export function legacyInflight() {
  const rows = [...inflight.values()];
  const oldest = rows.reduce((m, r) => (m == null || r.started_at < m ? r.started_at : m), null);
  const byEntry = {};
  for (const r of rows) byEntry[r.entry] = (byEntry[r.entry] || 0) + 1;
  return { count: rows.length, oldest_started_at: oldest == null ? null : new Date(oldest).toISOString(), by_entry: byEntry };
}
function trackUntilDone(res, entryId) {
  const end = beginLegacyWrite(entryId);
  res.on('finish', end);
  res.on('close', end);
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
 *   route        → 閉じていれば 410 / 503 (本文を読む前 = multer の取込より前に置ける)。通したら終わるまで数える (drain)
 *   route_field  → その欄を送ったときだけ同じ (本文を読んだ後に置く)
 *   route_part   → 断らない。res.locals.masterLegacyWrite = 毎回読んだ状態 (handler が writable のときだけマスタの部分を書く)
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
      if (route) {
        const state = await checkLegacyGate({ purpose: 'write' });
        if (!state.writable) return refuse(res, state, route.e);
        trackUntilDone(res, route.e.id);
      } else if (part) {
        const state = await checkLegacyGate({ purpose: 'write' });
        res.locals.masterLegacyWrite = state;
        if (state.writable) trackUntilDone(res, part.e.id);
      }
      if (screen) res.locals.masterLegacy = screenInfo(await checkLegacyGate({ purpose: 'screen' }));
      return next();
    } catch (e) {
      // 門そのものが壊れた = 書かせない (fail-closed)。画面は見せる
      console.error('[master-legacy-gate] 門の誤り:', e && e.message);
      if (hits.some((x) => x.e.kind === 'route' || x.e.kind === 'route_field')) return refuse(res, { writable: false, readable: false }, hits[0].e);
      res.locals.masterLegacyWrite = { writable: false, readable: false, phase: null, source: 'unreadable', error: String(e && e.message) };
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
    let state;
    try { state = await checkLegacyGate({ purpose: 'write' }); } catch { state = { writable: false, readable: false }; }
    if (state.writable) return next();
    for (const f of [req.file, ...(Array.isArray(req.files) ? req.files : [])]) {
      if (f && f.path) { try { fs.unlinkSync(f.path); } catch { /* 消せなくても書き込みはしない */ } }
    }
    return refuse(res, state, entry);
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

/**
 * CLI の門。mode が分かったらすぐ (引数・ファイルの検査より前・データに触る前) に呼ぶ。書く直前にももう一度呼ぶ。
 * 閉じていれば理由を出して process.exitCode = 3 にし false を返す
 * (process.exit は呼ばない = 呼んだ側が何もしないで終わる。pg の直後の process.exit は Windows で 127 になる #1386)
 * 毎回読む (前の値は使わない)。接続先が env に無ければリポジトリ直下の .env を読む (daily-sync と同じ dotenv)
 */
export async function legacyCliGate(entryId, { env = process.env, log = console.error, read = null } = {}) {
  const entry = entryById(entryId);
  if (!entry || entry.kind !== 'cli') throw new Error(`master-legacy-gate: CLI の入口が一覧に無い: ${entryId}`);
  const overridden = reader !== defaultReadPhase;   // 試験で読み方を差し替えたとき (本番は差し替えない)
  if (!read && !overridden && !phaseUrlFrom(env) && env === process.env) {
    try { (await import('dotenv')).config({ quiet: true }); } catch { /* dotenv が無くても下で「接続先が無い」として止める */ }
  }
  let s;
  try {
    s = read ? await read() : (overridden ? await reader() : await defaultReadPhase({ env }));
  } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
  if (s && s.readable === true && legacyWritable(s)) return true;
  const why = s && s.readable === true
    ? `段階が ${s.phase} (${PHASE_LABELS[s.phase] || '?'}) = 古い入口は閉じています`
    : `切替の段階を読めない (${(s && s.error) || '?'}) = 止めます (fail-closed)`;
  log(`❌ ${entry.file}${entry.mode && entry.mode !== '*' ? ` ${entry.mode}` : ''}: マスタは新しい画面で直します → ${MASTER_EDIT_URL}\n   ${why}。何も書いていません (終了コード ${CLI_EXIT_CODE})`);
  process.exitCode = CLI_EXIT_CODE;
  return false;
}

// ─── 門の記録 (ack) ───
const BOOT_ID = crypto.randomBytes(4).toString('hex');
/** プロセスごとの名札 (Render の RENDER_INSTANCE_ID か PC 名・pid・起動の乱数) */
export function instanceId(env = process.env) {
  return `${env.RENDER_INSTANCE_ID || os.hostname()}:${process.pid}:${BOOT_ID}`.replace(/[^A-Za-z0-9._:@-]/g, '_').slice(0, 120);
}
/** 形を決めた JSON (キーを並べる = どの PC でも同じ文字列) */
function canonicalJson(v) {
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v).filter((k) => v[k] !== undefined).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(v[k])}`).join(',')}}`;
  return JSON.stringify(v ?? null);
}
/**
 * 門の中身 (manifest) = 一覧 (config/master-legacy-entries.mjs) を機械で読める形にしたもの。閉じる入口・閉じない口・手の入口 (機械では閉じられない) の id。
 * manifest_hash = その形を決めた JSON の sha256。門の記録に入れ、段階を進める DB の関数が「全部のプロセスが同じ門か」を比べる (⑤-1 の契約)
 */
export function legacyManifest() {
  const pick = (e) => Object.fromEntries(['id', 'kind', 'host', 'app', 'file', 'mount', 'method', 'path', 'when', 'field', 'part', 'mode', 'recheck', 'when_frozen', 'writes', 'owner_cols', 'guard'].filter((k) => e[k] !== undefined).map((k) => [k, e[k]]));
  return {
    schema: 'master-legacy-manifest/1',
    entries: LEGACY_ENTRIES.map(pick),
    exempt: LEGACY_EXEMPT.map(pick),
    manual_entry_ids: LEGACY_EXEMPT.filter((e) => e.kind === 'manual').map((e) => e.id),
  };
}
export function manifestHash(manifest = legacyManifest()) {
  return crypto.createHash('sha256').update(canonicalJson(manifest)).digest('hex');
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

/** 一覧と持ち主表の指紋 (切替の手順の「全部の環境で読み戻す」で、環境ごとに同じ版かを比べる) */
export async function legacyGateFingerprint() {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  return { owner_hash: ownershipHash(MASTER_OWNERSHIP), manifest_hash: manifestHash(), build_id: resolveBuildId(), instance_id: instanceId() };
}

/**
 * ⑤-1 の門の記録の書き手への口 (adapter)。🚨 関数の名前・引数の形は ⑤-1 の直し (0050) で決まる = 決まったらここ (ACK_WRITER と writeLegacyAck) だけ直す。
 * 契約 (⑤-1): ops.master_legacy_gate_acks の行 = host・instance_id・build_id (null にしない)・manifest_hash・owner_hash・phase_seen・
 *   inflight_count・oldest_inflight_at (acked_at は DB が入れる)。ops.master_legacy_manifests = manifest_hash → 中身 (手の入口の id を含む)。
 *   書くのは security definer の関数だけ。呼び手が門の版を名乗ることはない (証拠 = build_id + manifest_hash)
 * TODO(⑤-1 の直しの後): 本当の名前と形に合わせる。今は一つの jsonb を渡す形で書き、関数が無ければ何もしない ('no_function')
 */
export const ACK_WRITER = Object.freeze({ regprocedure: 'ops.record_master_legacy_gate_ack(jsonb)', call: 'select ops.record_master_legacy_gate_ack($1::jsonb) as r' });
async function writeLegacyAck(db, payload) {
  const has = (await db.query(`select to_regprocedure($1) is not null as ok`, [ACK_WRITER.regprocedure])).rows[0]?.ok;
  if (!has) return { written: false, detail: `${ACK_WRITER.regprocedure} が無い (⑤-1 の直しの前)` };
  const r = (await db.query(ACK_WRITER.call, [JSON.stringify(payload)])).rows[0]?.r ?? null;
  return { written: true, result: r };
}
/** 書く中身 (試験でも使う) */
export async function legacyAckPayload({ host, env = process.env, phaseSeen }) {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  const manifest = legacyManifest();
  const infl = legacyInflight();
  return {
    host, instance_id: instanceId(env), build_id: resolveBuildId({ env }),
    manifest_hash: manifestHash(manifest), manifest,
    owner_hash: ownershipHash(MASTER_OWNERSHIP), phase_seen: phaseSeen ?? null,
    inflight_count: infl.count, oldest_inflight_at: infl.oldest_started_at,
  };
}

let ackState = { state: 'not_yet', at: null, detail: null, ack_id: null };
let ackRunning = null;
let lastAckAttemptAt = 0;
export function legacyAckState() { return { ...ackState }; }

/**
 * 「この環境・このプロセスに古い入口の門を載せた」の記録 (ack) を Company DB に残す (⑤-1 の書き手の関数 = ACK_WRITER)。
 * 書く前に確かめる (M6): host・build の番号・段階を読める (= env・表・select の権限)・書く接続先・関数がある。どれか欠ける = 書かない (理由を残す)。
 * 例外は投げない (起動・要求を止めない)。戻り値 { state: 'acked'|'precheck_failed'|'no_function'|'error', detail, ack_id }
 */
export async function ackLegacyGates({ host, env = process.env, connect = null, repoDir = REPO_ROOT } = {}) {
  const done = (state, detail, ack_id = null) => {
    ackState = { state, at: new Date(clock()).toISOString(), detail, ack_id };
    if (state !== 'acked') console.error(`[master-legacy-gate] ❌ 門の記録を書けない (${state}): ${detail}`);
    return { ...ackState };
  };
  if (host !== 'render' && host !== 'minipc') return done('precheck_failed', `知らない場所: ${host}`);
  const buildId = resolveBuildId({ env, repoDir });
  if (!buildId) return done('precheck_failed', 'build の番号が分からない (RENDER_GIT_COMMIT も git の HEAD も読めない)');
  const s = await checkLegacyGate({ purpose: 'write' });
  if (!s.readable) return done('precheck_failed', `段階を読めない (${s.error}) = env・表・select の権限を確かめる`);
  const url = ackUrlFrom(env);
  if (!url && !connect) return done('precheck_failed', '記録を書く接続先が無い (COMPANY_DB_WATCH_WRITER_URL / COMPANY_DB_URL)');
  let conn = null;
  try {
    if (connect) conn = await connect();
    else {
      const { openPgClient, pgAdapter } = await import('../scripts/company-db/migrate.mjs');
      const client = await openPgClient(url, { application_name: 'master-legacy-gate-ack', connectionTimeoutMillis: CONNECT_TIMEOUT_MS, statement_timeout: STATEMENT_TIMEOUT_MS, query_timeout: STATEMENT_TIMEOUT_MS + 1000 });
      conn = { db: pgAdapter(client), close: () => client.end() };
    }
    const payload = { ...(await legacyAckPayload({ host, env, phaseSeen: s.phase })), build_id: buildId };
    const w = await writeLegacyAck(conn.db, payload);
    if (!w.written) return done('no_function', w.detail);
    return done('acked', `${host} build=${buildId.slice(0, 12)} manifest=${payload.manifest_hash.slice(0, 12)} phase=${s.phase} 書きかけ=${payload.inflight_count}`, w.result);
  } catch (e) {
    return done('error', String((e && e.message) || e).slice(0, 300));
  } finally {
    if (conn) { try { await conn.close(); } catch { /* 結果は変えない */ } }
  }
}

/** 5 分より前の記録なら、裏で書き直す (待たない・同時に 2 本走らせない)。起動と、要求が来たついで (legacyAckHeartbeat) に呼ぶ */
export function maybeRefreshLegacyAck({ host, env = process.env, force = false } = {}) {
  if (!host) return null;
  const now = clock();
  if (ackRunning) return ackRunning;
  if (!force && now - lastAckAttemptAt < ACK_REFRESH_MS) return null;
  lastAckAttemptAt = now;
  ackRunning = ackLegacyGates({ host, env }).catch((e) => ({ state: 'error', detail: String(e && e.message) })).finally(() => { ackRunning = null; });
  return ackRunning;
}
/** 試験用 */
export function __resetLegacyAck() { ackState = { state: 'not_yet', at: null, detail: null, ack_id: null }; ackRunning = null; lastAckAttemptAt = 0; }

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
