/**
 * master-legacy-gate.mjs — 商品・仕入先マスタの古い入口の門 (Company DB構想 14 §5・§10 契約 v3 H1・§9 v2 M2)
 *
 * 切替の段階 (ops.master_cutover_state。lib/master-cutover.mjs) が legacy_open のときだけ、古い入口
 * (config/master-legacy-entries.mjs の一覧) で書ける。frozen / company_owner / new_open は書けない
 * = 持ち主表 (config/master-ownership.mjs) がまだ 'load' でも閉じる (契約 v3 H1)。
 * 🚨 段階が読めない (接続先が無い・つながらない・表が無い・壊れた値) = 書けない (fail-closed)
 *
 * 段階の読み方 (どちらも新しい秘密は足さない。今ある接続をそのまま使う):
 *   - miniPC (WarehouseServer・CLI): COMPANY_DB_WATCH_URL (見張りと同じ照会用のロール watcher = select だけ。
 *     daily-sync の見張り・マスタ照合・lz-daily と同じ)。無ければ COMPANY_DB_URL
 *   - Render: COMPANY_DB_URL (company-db・master-edit と同じ)。COMPANY_DB_WATCH_URL があればそちらを先に使う
 *   1 回ごとに短い接続を開いて閉じる (接続 5 秒・文 5 秒で打ち切り = 画面を長く止めない)
 *
 * 前に読めた値の使い回し (一時的な途切れで日々の作業を止めないため。契約で許された ≤10 分の中の 5 分):
 *   - 書き込み (API・CLI) は毎回読む。読めたらその値で決める (前の値は見ない)
 *   - 読めなかったときだけ、「最後に読めた段階が legacy_open で、それが 5 分以内」なら通す (source = 'last_good')
 *     = 段階が変わったのを一度でも読んだら (frozen 以降を読んだら) 前の legacy_open は使わない
 *   🚨 引き換え: frozen に進めた直後の 5 分以内に Company DB が途切れると、その環境はまだ legacy_open を覚えていて通しうる。
 *      切替の手順で「全部の環境の読み戻し」(GET /apps/warehouse/api/master-legacy-gate) をすると、
 *      その環境は frozen を読んだことになり、以後は途切れても通さない (= 読み戻しが済めば穴は無い)。CLI は毎回の新しいプロセス = 使い回さない
 *   - 画面 (帯を出すかどうか) は 30 秒だけ前の結果 (読めた・読めないの両方) を使う (1 画面ごとに接続しない)。
 *     画面は見せ方だけ。書けるかは API が毎回決める
 */
import crypto from 'node:crypto';
import { readCutoverPhase, legacyWritable, CUTOVER_PHASES, PHASE_LABELS } from './master-cutover.mjs';
import { LEGACY_ENTRIES, MASTER_EDIT_URL, LEGACY_GATES_VERSION, entriesForApp, entryById } from '../config/master-legacy-entries.mjs';

export { MASTER_EDIT_URL, LEGACY_GATES_VERSION };
export const FALLBACK_MAX_MS = 10 * 60 * 1000;          // 契約の上限 (これより長くはしない)
export const DEFAULT_FALLBACK_MS = 5 * 60 * 1000;
export const SCREEN_FRESH_MS = 30 * 1000;
export const CLI_EXIT_CODE = 3;
const CONNECT_TIMEOUT_MS = 5000;
const STATEMENT_TIMEOUT_MS = 5000;

export const FROZEN_MESSAGE = 'マスタは新しい画面で直します。ここでは書けません (切替のため閉じました)';
export const UNREADABLE_MESSAGE = '切替の段階を読めないので、マスタの書き込みを止めています。少し待ってもう一度。続くときは管理者へ';

/** 接続先 (照会用のロールを先に) */
export function phaseUrlFrom(env = process.env) {
  return String(env.COMPANY_DB_WATCH_URL || '').trim() || String(env.COMPANY_DB_URL || '').trim() || null;
}

/** 本番の読み方: 短い接続を開いて段階を読んで閉じる。例外は投げない */
async function defaultReadPhase({ env = process.env } = {}) {
  const url = phaseUrlFrom(env);
  if (!url) return { readable: false, phase: null, error: 'Company DB の接続先が無い (COMPANY_DB_WATCH_URL / COMPANY_DB_URL)' };
  let client = null;
  try {
    const { openPgClient, pgAdapter } = await import('../scripts/company-db/migrate.mjs');
    client = await openPgClient(url, {
      application_name: 'master-legacy-gate',
      connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
      statement_timeout: STATEMENT_TIMEOUT_MS,
      query_timeout: STATEMENT_TIMEOUT_MS + 1000,
    });
    return await readCutoverPhase(pgAdapter(client));
  } catch (e) {
    return { readable: false, phase: null, error: String((e && e.message) || e).slice(0, 300) };
  } finally {
    if (client) { try { await client.end(); } catch { /* 閉じるのに失敗しても結果は変えない */ } }
  }
}

// ─── 状態 (プロセスの中だけ) ───
let reader = defaultReadPhase;
let clock = () => Date.now();
let lastGood = null;      // { phase, at } 最後に読めた段階
let lastScreen = null;    // { state, at } 画面用の 30 秒の使い回し
let inflight = null;      // 今読んでいる最中の Promise (同時の要求で分け合う)

/** 試験用: 段階の読み方を差し替える (本番では呼ばない)。差し替えると前の結果は捨てる */
export function __setLegacyPhaseReader(fn) { reader = fn || defaultReadPhase; lastGood = null; lastScreen = null; inflight = null; }
export function __setLegacyClock(fn) { clock = fn || (() => Date.now()); }
export function __resetLegacyGate() { lastGood = null; lastScreen = null; inflight = null; }

/**
 * 今、古い入口で書いてよいか。
 * purpose: 'write' (毎回読む) / 'screen' (30 秒使い回す)
 * 戻り値 { writable, readable, phase, source: 'db'|'last_good'|'unreadable'|'screen_cache', error, last_good_at }
 */
export async function checkLegacyGate({ purpose = 'write', fallbackMs = DEFAULT_FALLBACK_MS } = {}) {
  const now = clock();
  if (purpose === 'screen' && lastScreen && now - lastScreen.at < SCREEN_FRESH_MS) return { ...lastScreen.state, source: 'screen_cache' };
  let s;
  try {
    // 同時に来た要求は 1 回の読みを分け合う (miniPC の照会用ロール watcher は接続が 3 本まで = 画面の人数分つながない)
    if (!inflight) inflight = Promise.resolve().then(() => reader()).finally(() => { inflight = null; });
    s = await inflight;
  } catch (e) { s = { readable: false, phase: null, error: String((e && e.message) || e) }; }
  let state;
  if (s && s.readable === true && CUTOVER_PHASES.includes(s.phase)) {
    lastGood = { phase: s.phase, at: now };
    state = { writable: legacyWritable(s), readable: true, phase: s.phase, source: 'db', error: null, last_good_at: null };
  } else {
    const fb = Math.min(Math.max(0, Number(fallbackMs) || 0), FALLBACK_MAX_MS);
    const err = (s && s.error) || '段階を読めない';
    if (lastGood && lastGood.phase === 'legacy_open' && now - lastGood.at <= fb) {
      state = { writable: true, readable: false, phase: 'legacy_open', source: 'last_good', error: err, last_good_at: new Date(lastGood.at).toISOString() };
      console.warn(`[master-legacy-gate] 段階を読めない (${err}) → ${Math.round((now - lastGood.at) / 1000)} 秒前に読めた legacy_open で通す`);
    } else {
      state = { writable: false, readable: false, phase: null, source: 'unreadable', error: err, last_good_at: lastGood ? new Date(lastGood.at).toISOString() : null };
    }
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

/** '/api/shipping/:sku' → 正規表現 (Express 4 の既定と同じく大文字小文字を区別しない・末尾の / は任意) */
export function pathPattern(p) {
  const esc = String(p).split('/').map((seg) => (seg.startsWith(':') ? '[^/]+' : seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))).join('/');
  return new RegExp(`^${esc === '/' ? '' : esc}/?$`, 'i');
}
const methodMatches = (entryMethod, reqMethod) => entryMethod === reqMethod || (entryMethod === 'GET' && reqMethod === 'HEAD');

/**
 * Express の門 (router の最初に置く)。config/master-legacy-entries.mjs の app の入口だけを見る。
 *   route        → 閉じていれば 410 / 503 (本文を読む前 = multer の取込より前に置く)
 *   route_field  → その欄を送ったときだけ同じ (本文を読んだ後に置く)
 *   screen       → res.locals.masterLegacy = 画面用の状態 (帯を出すか)。断らない
 * 一覧に無い要求は段階を読まずにそのまま通す
 */
export function masterLegacyGate(app) {
  const entries = entriesForApp(app).map((e) => ({ e, re: pathPattern(e.path) }));
  if (!entries.length) throw new Error(`master-legacy-gate: app ${app} の入口が一覧に無い`);
  return async function masterLegacyGateMw(req, res, next) {
    const hits = entries.filter((x) => methodMatches(x.e.method, req.method) && x.re.test(req.path));
    if (!hits.length) return next();
    try {
      const screen = hits.find((x) => x.e.kind === 'screen');
      const route = hits.find((x) => x.e.kind === 'route')
        || hits.find((x) => x.e.kind === 'route_field' && req.body && typeof req.body === 'object' && req.body[x.e.field] !== undefined);
      if (route) {
        const state = await checkLegacyGate({ purpose: 'write' });
        if (!state.writable) {
          const [status, body] = refusal(state, route.e);
          return res.status(status).json(body);
        }
      }
      if (screen) res.locals.masterLegacy = screenInfo(await checkLegacyGate({ purpose: 'screen' }));
      return next();
    } catch (e) {
      // 門そのものが壊れた = 書かせない (fail-closed)。画面は見せる
      console.error('[master-legacy-gate] 門の誤り:', e && e.message);
      if (hits.some((x) => x.e.kind !== 'screen')) return res.status(503).json({ error: 'master_phase_unreadable', message: UNREADABLE_MESSAGE, url: MASTER_EDIT_URL });
      res.locals.masterLegacy = screenInfo({ writable: false, readable: false, phase: null, error: String(e && e.message) });
      return next();
    }
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
 * CLI の門。書く mode の最初 (データに触る前) に呼ぶ。閉じていれば理由を出して process.exitCode = 3 にし false を返す
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

/** 一覧と持ち主表の指紋 (切替の手順の「全部の環境で読み戻す」で、環境ごとに同じ版かを比べる) */
export async function legacyGateFingerprint() {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  const h = (v) => crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex').slice(0, 16);
  const sortedOwnership = Object.fromEntries(Object.entries(MASTER_OWNERSHIP).sort(([a], [b]) => a.localeCompare(b)));
  return { owner_hash: h(sortedOwnership), entries_hash: h(LEGACY_ENTRIES.map((e) => e.id)), build_id: process.env.RENDER_GIT_COMMIT || null };
}

/** 読み戻し用 (この環境・このプロセスが見ている段階。毎回読む) */
export async function legacyGateStatus({ host = null } = {}) {
  const state = await checkLegacyGate({ purpose: 'write' });
  return { host, pid: process.pid, checked_at: new Date(clock()).toISOString(), legacy_gates_version: LEGACY_GATES_VERSION, ...state, ...(await legacyGateFingerprint()) };
}

/**
 * 「この環境に古い入口の門を載せた」の印 (ack) を Company DB に残す (⑤-1 Codex R1: 段階を legacy_open から進めるには
 * 必要な全部の環境の ack が要る = 門を配る前に frozen へ進めない)。server.js の起動のときに 1 回だけ呼ぶ。
 * 書く先 = ⑤-1 の ops.master_legacy_gate_acks (host・build_id・owner_hash・legacy_gates_version・phase_seen・acked_at)。
 * 🚨 書くのは DB の関数 ops.ack_master_legacy_gate(host text, build_id text, owner_hash text, legacy_gates_version integer, phase_seen text)
 *    だけ (表に直接書かない)。その関数がまだ無い (⑤-1 の直しの前) = 何もしない ('no_function')。
 *    TODO(⑤-1 の直しの後に rebase): 関数を migration で足す (security definer・search_path = pg_catalog, pg_temp・
 *    public の execute を外し、miniPC の記録用のロール watch_writer と表の持ち主にだけ execute)。
 *    TODO: miniPC の CLI (手の取込) の門は WarehouseServer の起動の ack で代える (同じチェックアウトのコード)。daily-sync の毎朝の
 *    更新が要るなら daily-sync の既存の 1 段として足す (新しい定期実行は作らない)
 * 接続: COMPANY_DB_WATCH_WRITER_URL (miniPC の記録用) → COMPANY_DB_URL (Render)。例外は投げない (起動を止めない)
 * 戻り値 { state: 'acked'|'no_function'|'not_configured'|'error', detail }
 */
export async function ackLegacyGates({ host, env = process.env, connect = null } = {}) {
  if (!host) return { state: 'not_configured', detail: 'host が無い' };
  const url = String(env.COMPANY_DB_WATCH_WRITER_URL || '').trim() || String(env.COMPANY_DB_URL || '').trim();
  if (!url && !connect) return { state: 'not_configured', detail: 'COMPANY_DB_WATCH_WRITER_URL / COMPANY_DB_URL が無い' };
  let conn = null;
  try {
    if (connect) conn = await connect();
    else {
      const { openPgClient, pgAdapter } = await import('../scripts/company-db/migrate.mjs');
      const client = await openPgClient(url, { application_name: 'master-legacy-gate-ack', connectionTimeoutMillis: CONNECT_TIMEOUT_MS, statement_timeout: STATEMENT_TIMEOUT_MS, query_timeout: STATEMENT_TIMEOUT_MS + 1000 });
      conn = { db: pgAdapter(client), close: () => client.end() };
    }
    const has = (await conn.db.query(`select to_regprocedure('ops.ack_master_legacy_gate(text,text,text,integer,text)') is not null as ok`)).rows[0]?.ok;
    if (!has) return { state: 'no_function', detail: 'ops.ack_master_legacy_gate がまだ無い (⑤-1 の直しの後)' };
    const phase = await readCutoverPhase(conn.db);
    const fp = await legacyGateFingerprint();
    await conn.db.query('select ops.ack_master_legacy_gate($1, $2, $3, $4, $5)', [host, fp.build_id, fp.owner_hash, LEGACY_GATES_VERSION, phase.readable ? phase.phase : null]);
    return { state: 'acked', detail: `${host} v${LEGACY_GATES_VERSION} owner=${fp.owner_hash} phase=${phase.phase ?? '?'}` };
  } catch (e) {
    return { state: 'error', detail: String((e && e.message) || e).slice(0, 300) };
  } finally {
    if (conn) { try { await conn.close(); } catch { /* 結果は変えない */ } }
  }
}
