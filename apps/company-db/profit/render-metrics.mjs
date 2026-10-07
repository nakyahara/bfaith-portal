/**
 * render-metrics.mjs — Render の API から Company DB (Render Postgres) の Memory・Memory の上限・Disk Usage・Disk Capacity を読む部品
 *   (D-60 v3.4 の PR 2・設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「資源の関門」)
 *
 * 🚨 使う所はまだ無い (単体の部品)。利益の受け口 (/amazon-profit/daily・/totals) は 503 のまま。開けるのは §3.10 の PR 6 だけ
 *
 * 契約 (§3.10「資源の関門」・Codex R-D60-v3-3 M4 / v3-4 M5・H4・#1600 R1):
 *   - Postgres の resource の ID を設定で固定 (CDB_RENDER_PG_RESOURCE_ID・`dpg-` で始まる)。応答の系列の resource の label がこの ID と同じでなければ「不可」
 *   - 使う label を固定 (RESOURCE_LABEL_FIELD)。label が無い・2 つある・値が違う → 「不可」
 *   - 単位は bytes だけ (BYTE_UNITS にある倍率 1 の綴りだけ = 推測で倍率を掛けない。知らない単位は「不可」)
 *   - 🆕 値は **JSON の本文の文字のままで整数 (小数点・指数なし) かつ safe integer** のときだけ受け取る (#1600 R1 M3)。
 *     JSON.parse の binary64 に直すと 1073741824.00000001 が 1073741824 になり、切り上げ / 切り捨ての安全の向きが逆になりうる
 *     → 文字を見て、小数・指数・2^53 以上は全部 METRICS_NOT_INTEGER (= 503)。丸めはしない
 *     🆕 #1600 R2: 本文は自前の厳密な JSON の読み方 (parseJsonIntegersOnly・依存なし) で読み、数の字句をそのまま見る
 *     (JSON.parse の reviver の context.source は本番の node:20-slim で渡らない = 使わない)
 *   - 各 endpoint は系列がちょうど 1 つ。その系列の最新の点を採る
 *   - Memory と Memory の上限・Disk Usage と Capacity は同じ resource で、点の時刻の差が 2 分以内
 *   - 空の配列・空の系列・重複の系列・同じ時刻の点が 2 つ・未来の時刻・負の値・数でない値・別の resource → 全部「不可」
 *   - 🆕 時刻は RFC 3339 の暦の要素を全部確かめる (2026-02-30・2025-02-29・25 時・時差 +24:00 などは「不可」= Date.parse に直させない・#1600 R1 M1)。
 *     小数秒は 9 桁まで受け取り、ナノ秒の整数で比べる (丸めない)
 *   - 取得から 2 分を超えた値は「不可」(取った時に確かめる + 使う直前に checkFreshness でもう一度)
 *   - HTTP の timeout は短い (既定 全体 5 秒)。200 でない応答は **全部**「不可」(400 を含む)
 *   - 🆕 本文は逐次読み、実のバイトが 1MB を超えた時点で読むのをやめる (Content-Length が無い・少なく書いた・chunked でも・#1600 R1 M2)
 *   - 「不可」は例外を投げずに { ok: false, reason } で返す (呼び手は 503 PROFIT_METRICS_UNAVAILABLE にする)。reason は下の REASONS の固定のコードだけ
 *   - 🆕 理由の優先 (#1600 R1 Low): ① 設定 (METRICS_CONFIG・要求を送らない) ② 全体の timeout が起きたら METRICS_TIMEOUT (ほかの endpoint の理由より先)
 *     ③ それ以外は METRIC_ENDPOINTS の順 (memory → memoryLimit → diskUsage → diskCapacity) で最初の endpoint の理由 ④ 4 つとも読めた後の組の検査
 *     (PAIR_SKEW → ZERO_LIMIT → INCONSISTENT)
 *
 * 🚨 RENDER_API_KEY (workspace の全部に触れる強い秘密) の扱い:
 *   - 値をログ・例外の文・戻り値・fixture に絶対に残さない。この部品は console を使わない・例外を外に出さない (中の例外は METRICS_INTERNAL に変える)
 *   - 鍵は RenderApiKey に包む (JSON.stringify・String()・util.inspect は [redacted])。鍵を出すのは Authorization の header を作る 1 か所だけ
 *   - Render の応答の本文はそのまま返さない・記録しない (401 の本文に鍵が映っても外に出ない)
 *   - redirect は追わない (redirect: 'error' = 別の host に Authorization を送らない)
 *   - 送り先は公式の https://api.render.com/v1 だけ (試験の時だけ 127.0.0.1 の http を許す)
 *
 * 🚨 Memory の値の使いどころ (R-v3-4 H4): Render の metrics は resolutionSeconds ≥ 30 の bucket で、bucket の値が区間の最大か平均か sample か
 *   公式の説明に無い = **最大の証明にならない** (11.7 秒の計算の短い山を取り逃がしうる)。→ 本番の要求の直前の fail-closed の関門にだけ使う。
 *   校正の最大 (memory の山) の証明には使わない。戻り値の peakProof は常に false
 *
 * 公式の API の形 (2026-10-03 に https://api-docs.render.com/reference/get-memory ほかで確かめた):
 *   GET https://api.render.com/v1/metrics/{memory|memory-limit|disk-usage|disk-capacity}
 *     ?resource=<ID>&startTime=<date-time>&endTime=<date-time>&resolutionSeconds=<≥30>   (aggregationMethod は付けない = 系列をまとめさせない)
 *   Authorization: Bearer <RENDER_API_KEY>
 *   200 = [{ labels: [{ field, value }], values: [{ timestamp, value }], unit }]
 *   🚨 公式の OpenAPI の共有の例は labels[].field = "service"・unit = "GB" (Postgres の保証ではなく、field と unit の一覧も無い)。
 *     この部品は「resource の label・bytes の整数」だけを正常とし、それ以外は 503 に閉じる。本物の応答で確かめるまで 503 のまま (PR 6 の前に直す)
 */
import { inspect } from 'node:util';

export const RENDER_API_BASE = 'https://api.render.com/v1';
/** 読む 4 つ (この順が理由の優先の順) */
export const METRIC_ENDPOINTS = Object.freeze({
  memory: '/metrics/memory',
  memoryLimit: '/metrics/memory-limit',
  diskUsage: '/metrics/disk-usage',
  diskCapacity: '/metrics/disk-capacity',
});
/**
 * 系列の resource を表す label の field の名前。🚨 公式の説明に label の field の名前の一覧が無い = 未確認の仮の値 (query の引数の名前に合わせた)。
 * 違っていれば全部 METRICS_LABEL_MISSING (= 503) になるだけ (開く向きには間違えない)。PR 6 の前に、本物の応答 1 回で確かめて直す
 */
export const RESOURCE_LABEL_FIELD = 'resource';
/** bytes と読んでよい単位 (倍率 1 だけ)。🚨 Render の応答の unit の綴りは公式の説明に無い (例は "GB") = 知らない綴りは推測で掛け算せず「不可」 */
export const BYTE_UNITS = Object.freeze(['bytes', 'byte', 'B', 'By']);
export const MAX_AGE_MS = 2 * 60 * 1000;          // 取得 (と使う時) から 2 分を超えた点は「不可」
export const MAX_PAIR_SKEW_MS = 2 * 60 * 1000;    // 使用と上限の点の時刻の差
export const DEFAULT_TIMEOUT_MS = 5000;           // 4 つの要求の全体
export const RESOLUTION_SECONDS = 30;             // 公式の最小
export const WINDOW_MS = 10 * 60 * 1000;          // 読む区間 (今から 10 分前まで)
export const MAX_BODY_BYTES = 1024 * 1024;        // 1 つの応答の本文の実のバイトの上限

/** 「不可」の理由のコード (これ以外は返さない)。呼び手は全部 503 PROFIT_METRICS_UNAVAILABLE */
export const REASONS = Object.freeze([
  'METRICS_CONFIG',            // 鍵・resource の ID・送り先・timeout の設定が無い / 形が違う (要求を送らない)
  'METRICS_AUTH',              // 401・403
  'METRICS_RATE_LIMITED',      // 429
  'METRICS_UPSTREAM',          // 5xx
  'METRICS_HTTP',              // ほかの 200 でない応答 (400・404・1xx/2xx/3xx のうち fetch が応答として返すもの = 201・204・300・304 など)
  'METRICS_TIMEOUT',           // 全体の timeout (ほかの理由より先)
  'METRICS_NETWORK',           // 網の失敗・redirect の 3xx (301・302・303・307・308 = redirect: 'error' で fetch が例外にする)
  'METRICS_SHAPE',             // JSON でない・UTF-8 でない・配列でない・系列や点の形が違う・時刻が読めない / 実在しない・本文が 1MB を超える
  'METRICS_NOT_INTEGER',       // 値が本文の文字のままで整数でない (小数・指数) か safe integer を超える
  'METRICS_EMPTY',             // 空の配列・点の無い系列
  'METRICS_DUPLICATE_SERIES',  // 系列が 2 つ以上
  'METRICS_DUPLICATE_POINT',   // 同じ時刻の点が 2 つ
  'METRICS_LABEL_MISSING',     // resource の label が無い・2 つある
  'METRICS_WRONG_RESOURCE',    // 別の resource
  'METRICS_UNIT',              // bytes でない単位
  'METRICS_NEGATIVE',          // 負の値
  'METRICS_FUTURE',            // 未来の時刻
  'METRICS_STALE',             // 最新の点が 2 分より古い
  'METRICS_PAIR_SKEW',         // 使用と上限の点の時刻の差が 2 分を超える
  'METRICS_ZERO_LIMIT',        // 上限・容量が 0
  'METRICS_INCONSISTENT',      // 使用 > 上限・容量
  'METRICS_INTERNAL',          // この部品の中の思わぬ例外 (中身は出さない)
]);

const REDACTED = '[redacted]';
const keyStore = new WeakMap();
/** RENDER_API_KEY の包み。値は外から見えない (JSON・文字列・inspect は [redacted]) */
export class RenderApiKey {
  constructor(value) {
    if (typeof value !== 'string' || !/^[\x21-\x7e]{8,512}$/.test(value)) throw new TypeError('RENDER_API_KEY の形が違う');   // 値は文に入れない
    keyStore.set(this, value);
    Object.freeze(this);
  }
  toJSON() { return REDACTED; }
  toString() { return REDACTED; }
  [inspect.custom]() { return `RenderApiKey(${REDACTED})`; }
}
const revealKey = (k) => keyStore.get(k);

const RESOURCE_ID_RE = /^dpg-[a-z0-9][a-z0-9-]{2,78}$/;
const LOOPBACK_BASE_RE = /^http:\/\/127\.0\.0\.1:\d{1,5}$/;
const fail = (reason) => Object.freeze({ ok: false, reason });

/**
 * 環境変数から設定を読む。鍵は RenderApiKey に包む (戻り値を記録しても鍵は出ない)
 * @returns {{ ok: true, config: { apiKey: RenderApiKey, resourceId: string } } | { ok: false, reason: 'METRICS_CONFIG' }}
 */
export function readRenderMetricsConfig(env = process.env) {
  try {
    const resourceId = env.CDB_RENDER_PG_RESOURCE_ID;
    if (typeof resourceId !== 'string' || !RESOURCE_ID_RE.test(resourceId)) return fail('METRICS_CONFIG');
    let apiKey;
    try { apiKey = new RenderApiKey(env.RENDER_API_KEY); } catch { return fail('METRICS_CONFIG'); }
    return Object.freeze({ ok: true, config: Object.freeze({ apiKey, resourceId }) });
  } catch { return fail('METRICS_CONFIG'); }
}

// ─── RFC 3339 の時刻 (暦の要素を全部確かめる) → epoch のナノ秒 (BigInt)。読めない / 実在しないなら null ───
const RFC3339_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:(Z)|([+-])(\d{2}):(\d{2}))$/;
const daysInMonth = (y, m) => [31, (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0 ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1];
export function parseRfc3339Nanos(s) {
  if (typeof s !== 'string') return null;
  const m = RFC3339_RE.exec(s);
  if (!m) return null;
  const [y, mo, d, h, mi, se] = m.slice(1, 7).map(Number);
  if (y < 1970 || mo < 1 || mo > 12 || d < 1 || d > daysInMonth(y, mo) || h > 23 || mi > 59 || se > 59) return null;   // 閏秒 (60) も受け取らない
  let offMin = 0;
  if (!m[8]) {
    const oh = Number(m[10]), om = Number(m[11]);
    if (oh > 23 || om > 59) return null;
    offMin = (m[9] === '-' ? -1 : 1) * (oh * 60 + om);
  }
  const sec = Date.UTC(y, mo - 1, d, h, mi, se) / 1000 - offMin * 60;   // 要素を確かめた後なので Date.UTC は直さない
  if (!Number.isSafeInteger(sec)) return null;
  return BigInt(sec) * 1_000_000_000n + BigInt((m[7] || '').padEnd(9, '0') || '0');
}
const NS_PER_MS = 1_000_000n;
const msFloor = (ns) => Number(ns / NS_PER_MS);   // ns ≥ 0 (1970 年より前は受け取らない)

class Reject extends Error { constructor(reason) { super(reason); this.reason = reason; } }

/** 値が整数の文字でない・safe integer を超える数の印 (下の JSON の読み方が置き換える) */
const NOT_INTEGER = Object.freeze({ notInteger: true });
const INT_SOURCE_RE = /^-?(0|[1-9]\d*)$/;
const MAX_JSON_DEPTH = 32;
/**
 * JSON を読む (RFC 8259 の厳密な文法・依存なし・Node 20 で動く・#1600 R2 M1)。数は **本文の字句のまま** 見て、
 * 整数の字句 (小数点・指数なし) かつ safe integer のときだけ数にする (ほかは NOT_INTEGER = binary64 に丸めた後で判定しない)。
 *   🚨 JSON.parse の reviver の第 3 引数 (context.source・TC39 の source text access) は本番の Docker (node:20-slim) では渡らない = 使わない
 *   - 文字列は 1 つずつ字句を切り出して JSON.parse に渡す (escape の解釈は標準のまま)
 *   - 同じ key が 2 つ・container ([ と {) の入れ子が 32 を超える (32 段は受け取り 33 段は拒む)・末尾の余り・文法の違反は例外 (= METRICS_SHAPE)
 *   - object は prototype の無い object に入れる (key が "__proto__" でも prototype を変えない)
 */
export function parseJsonIntegersOnly(text) {
  if (typeof text !== 'string') throw new Reject('METRICS_SHAPE');
  let i = 0;
  const STRING_RE = /"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"/y;
  const NUMBER_RE = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  const ws = () => { while (i < text.length && (text[i] === ' ' || text[i] === '\t' || text[i] === '\n' || text[i] === '\r')) i++; };
  const bad = () => { throw new Reject('METRICS_SHAPE'); };
  const lex = (re) => { re.lastIndex = i; const m = re.exec(text); if (!m) bad(); i = re.lastIndex; return m[0]; };
  // depth = この値を囲む container ([ と {) の数。container を開くとき、入れ子の数 (depth + 1) が 32 を超えたら拒む (#1600 R3 Low)
  const value = (depth) => {
    ws();
    const c = text[i];
    if (c === '{') {
      if (depth + 1 > MAX_JSON_DEPTH) bad();
      i++; const obj = Object.create(null); ws();
      if (text[i] === '}') { i++; return obj; }
      for (;;) {
        ws(); if (text[i] !== '"') bad();
        const key = JSON.parse(lex(STRING_RE));
        if (Object.prototype.hasOwnProperty.call(obj, key)) bad();   // 同じ key が 2 つ = どちらを採るか決めない
        ws(); if (text[i] !== ':') bad(); i++;
        obj[key] = value(depth + 1);
        ws();
        if (text[i] === ',') { i++; continue; }
        if (text[i] === '}') { i++; return obj; }
        bad();
      }
    }
    if (c === '[') {
      if (depth + 1 > MAX_JSON_DEPTH) bad();
      i++; const arr = []; ws();
      if (text[i] === ']') { i++; return arr; }
      for (;;) {
        arr.push(value(depth + 1)); ws();
        if (text[i] === ',') { i++; continue; }
        if (text[i] === ']') { i++; return arr; }
        bad();
      }
    }
    if (c === '"') return JSON.parse(lex(STRING_RE));
    if (c === '-' || (c >= '0' && c <= '9')) {
      const src = lex(NUMBER_RE);
      if (!INT_SOURCE_RE.test(src)) return NOT_INTEGER;
      const n = Number(src);
      return Number.isSafeInteger(n) ? n : NOT_INTEGER;
    }
    for (const [word, v] of [['true', true], ['false', false], ['null', null]]) {
      if (text.startsWith(word, i)) { i += word.length; return v; }
    }
    return bad();
  };
  const out = value(0);
  ws();
  if (i !== text.length) bad();
  return out;
}

/** 1 つの endpoint の応答の本文 (parse 済み) を確かめ、最新の点を返す。nowNs = 応答を受けた時刻 (ナノ秒) */
function pickLatestPoint(body, resourceId, nowNs) {
  if (!Array.isArray(body)) throw new Reject('METRICS_SHAPE');
  if (body.length === 0) throw new Reject('METRICS_EMPTY');
  for (const s of body) {
    if (!s || typeof s !== 'object' || Array.isArray(s) || !Array.isArray(s.labels) || !Array.isArray(s.values) || typeof s.unit !== 'string') throw new Reject('METRICS_SHAPE');
    for (const l of s.labels) if (!l || typeof l !== 'object' || typeof l.field !== 'string' || typeof l.value !== 'string') throw new Reject('METRICS_SHAPE');
  }
  // resource の label は系列ごとに先に確かめる (別の resource の系列が混ざっていれば、系列の数より先に WRONG_RESOURCE)
  for (const s of body) {
    const rl = s.labels.filter((l) => l.field === RESOURCE_LABEL_FIELD);
    if (rl.length !== 1) throw new Reject('METRICS_LABEL_MISSING');
    if (rl[0].value !== resourceId) throw new Reject('METRICS_WRONG_RESOURCE');
  }
  if (body.length !== 1) throw new Reject('METRICS_DUPLICATE_SERIES');
  const s = body[0];
  if (!BYTE_UNITS.includes(s.unit)) throw new Reject('METRICS_UNIT');
  if (s.values.length === 0) throw new Reject('METRICS_EMPTY');
  const seen = new Set();
  let latest = null;
  for (const p of s.values) {
    if (!p || typeof p !== 'object' || Array.isArray(p) || p === NOT_INTEGER) throw new Reject('METRICS_SHAPE');
    const at = parseRfc3339Nanos(p.timestamp);
    if (at == null) throw new Reject('METRICS_SHAPE');
    if (p.value === NOT_INTEGER) throw new Reject('METRICS_NOT_INTEGER');
    if (typeof p.value !== 'number' || !Number.isSafeInteger(p.value)) throw new Reject('METRICS_SHAPE');
    if (p.value < 0) throw new Reject('METRICS_NEGATIVE');
    if (at > nowNs) throw new Reject('METRICS_FUTURE');
    if (seen.has(at)) throw new Reject('METRICS_DUPLICATE_POINT');
    seen.add(at);
    if (!latest || at > latest.at) latest = { at, value: p.value };
  }
  if (nowNs - latest.at > BigInt(MAX_AGE_MS) * NS_PER_MS) throw new Reject('METRICS_STALE');
  return latest;
}

/** 本文を逐次読み、実のバイトが上限を超えたら読むのをやめて捨てる (全部を先にメモリに載せない) */
async function readBodyTextCapped(res) {
  const len = Number(res.headers && typeof res.headers.get === 'function' ? res.headers.get('content-length') : NaN);
  if (Number.isFinite(len) && len > MAX_BODY_BYTES) { await discardBody(res); throw new Reject('METRICS_SHAPE'); }
  if (!res.body || typeof res.body.getReader !== 'function') throw new Reject('METRICS_SHAPE');
  const reader = res.body.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (!(value instanceof Uint8Array)) { try { await reader.cancel(); } catch { /* */ } throw new Reject('METRICS_SHAPE'); }
    total += value.byteLength;
    if (total > MAX_BODY_BYTES) { try { await reader.cancel(); } catch { /* 捨てるだけ */ } throw new Reject('METRICS_SHAPE'); }
    chunks.push(value);
  }
  const buf = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { buf.set(c, off); off += c.byteLength; }
  return new TextDecoder('utf-8', { fatal: true }).decode(buf);   // UTF-8 でなければ例外 → METRICS_SHAPE
}

const discardBody = async (res) => { try { if (res.body && typeof res.body.cancel === 'function') await res.body.cancel(); } catch { /* 捨てるだけ */ } };

const issued = new WeakSet();

/**
 * Render の API から 4 つの値を読む。例外は投げない
 * @param {{ apiKey: RenderApiKey, resourceId: string, fetchImpl?: typeof fetch, now?: () => number, timeoutMs?: number, baseUrl?: string }} opts
 * @returns {Promise<{ ok: true, snapshot: object } | { ok: false, reason: string }>}
 *   snapshot = { resourceId, fetchedAt, resolutionSeconds, peakProof: false, memoryUsedBytes, memoryLimitBytes, diskUsedBytes, diskCapacityBytes,
 *                points: { memory, memoryLimit, diskUsage, diskCapacity } (各点の時刻 ISO・ミリ秒は切り捨て = 古い向き), oldestPointAt }
 */
export async function fetchPostgresMetrics(opts = {}) {
  try {
    const { apiKey, resourceId, fetchImpl = globalThis.fetch, now = Date.now, timeoutMs = DEFAULT_TIMEOUT_MS, baseUrl = RENDER_API_BASE } = opts;
    if (!(apiKey instanceof RenderApiKey) || typeof resourceId !== 'string' || !RESOURCE_ID_RE.test(resourceId)) return fail('METRICS_CONFIG');
    if (typeof fetchImpl !== 'function' || typeof now !== 'function') return fail('METRICS_CONFIG');
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 10000) return fail('METRICS_CONFIG');
    if (baseUrl !== RENDER_API_BASE && !LOOPBACK_BASE_RE.test(baseUrl)) return fail('METRICS_CONFIG');

    const startMs = now();
    if (!Number.isSafeInteger(startMs) || startMs < 0) return fail('METRICS_CONFIG');
    const ac = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ac.abort(); }, timeoutMs);
    const qs = new URLSearchParams({
      resource: resourceId,
      startTime: new Date(startMs - WINDOW_MS).toISOString(),
      endTime: new Date(startMs).toISOString(),
      resolutionSeconds: String(RESOLUTION_SECONDS),
    }).toString();

    const one = async (path) => {
      let res;
      try {
        res = await fetchImpl(`${baseUrl}${path}?${qs}`, {
          method: 'GET',
          headers: { Authorization: `Bearer ${revealKey(apiKey)}`, Accept: 'application/json' },
          redirect: 'error',
          signal: ac.signal,
        });
      } catch {
        return fail(timedOut ? 'METRICS_TIMEOUT' : 'METRICS_NETWORK');   // 例外の中身 (URL・header を含みうる) は捨てる
      }
      try {
        const st = res.status;
        if (st !== 200) {   // 200 でない応答は全部「不可」(400 を含む・本文は読まずに捨てる)
          await discardBody(res);
          if (st === 401 || st === 403) return fail('METRICS_AUTH');
          if (st === 429) return fail('METRICS_RATE_LIMITED');
          if (st >= 500 && st <= 599) return fail('METRICS_UPSTREAM');
          return fail('METRICS_HTTP');
        }
        let body;
        try { body = parseJsonIntegersOnly(await readBodyTextCapped(res)); } catch (e) {
          if (timedOut) return fail('METRICS_TIMEOUT');
          return fail(e instanceof Reject ? e.reason : 'METRICS_SHAPE');
        }
        const nowMs = now();
        if (!Number.isSafeInteger(nowMs) || nowMs < 0) return fail('METRICS_INTERNAL');
        return Object.freeze({ ok: true, point: pickLatestPoint(body, resourceId, BigInt(nowMs) * NS_PER_MS) });
      } catch (e) {
        if (e instanceof Reject) return fail(e.reason);
        return fail(timedOut ? 'METRICS_TIMEOUT' : 'METRICS_INTERNAL');
      }
    };

    let results;
    try {
      results = await Promise.all(Object.values(METRIC_ENDPOINTS).map(one));
    } finally {
      clearTimeout(timer);
      ac.abort();   // 残りの要求があれば止める
    }
    if (timedOut) return fail('METRICS_TIMEOUT');   // 優先 ②: timeout はほかの endpoint の理由より先
    const names = Object.keys(METRIC_ENDPOINTS);
    for (const r of results) if (!r.ok) return fail(r.reason);   // 優先 ③: METRIC_ENDPOINTS の順で最初の理由
    const p = Object.fromEntries(names.map((n, i) => [n, results[i].point]));

    // 優先 ④: 使用と上限の点の時刻の差 (同じ resource は上で確かめた)。今は「どの点も 2 分以内 (STALE)・未来でない (FUTURE)」から差は 2 分以内に
    // 収まる = この検査は MAX_AGE_MS を変えたときの守り (2 つの規則を別々に持つ。試験は fixture では作れない)
    const skew = BigInt(MAX_PAIR_SKEW_MS) * NS_PER_MS;
    const absDiff = (a, b) => (a > b ? a - b : b - a);
    if (absDiff(p.memory.at, p.memoryLimit.at) > skew || absDiff(p.diskUsage.at, p.diskCapacity.at) > skew) return fail('METRICS_PAIR_SKEW');
    // 値は整数の文字の safe integer だけ (丸めない)
    const memoryUsedBytes = p.memory.value, memoryLimitBytes = p.memoryLimit.value;
    const diskUsedBytes = p.diskUsage.value, diskCapacityBytes = p.diskCapacity.value;
    if (memoryLimitBytes <= 0 || diskCapacityBytes <= 0) return fail('METRICS_ZERO_LIMIT');
    if (memoryUsedBytes > memoryLimitBytes || diskUsedBytes > diskCapacityBytes) return fail('METRICS_INCONSISTENT');

    const oldest = names.reduce((a, n) => (p[n].at < a ? p[n].at : a), p[names[0]].at);
    const snapshot = Object.freeze({
      resourceId,
      fetchedAt: new Date(startMs).toISOString(),
      resolutionSeconds: RESOLUTION_SECONDS,
      peakProof: false,   // 30 秒の bucket の値は区間の最大の証明にならない (要求の直前の関門にだけ使う)
      memoryUsedBytes, memoryLimitBytes, diskUsedBytes, diskCapacityBytes,
      points: Object.freeze(Object.fromEntries(names.map((n) => [n, new Date(msFloor(p[n].at)).toISOString()]))),
      oldestPointAt: new Date(msFloor(oldest)).toISOString(),   // ミリ秒に切り捨て = 古い向き (鮮度の再検査は厳しくなる側)
    });
    issued.add(snapshot);
    return Object.freeze({ ok: true, snapshot });
  } catch (e) {
    return fail(e instanceof Reject ? e.reason : 'METRICS_INTERNAL');
  }
}

/** 200 でない応答の理由 (本文は読まずに捨てる) */
async function statusReason(res) {
  await discardBody(res);
  const st = res.status;
  if (st === 401 || st === 403) return 'METRICS_AUTH';
  if (st === 429) return 'METRICS_RATE_LIMITED';
  if (st >= 500 && st <= 599) return 'METRICS_UPSTREAM';
  return 'METRICS_HTTP';
}

/**
 * 🆕 (PR #1606・設計 13 v3.13 ④ = 容量の resource と接続先の照合) Postgres の resource の名札を読む = GET /v1/postgres/{ID} の id と databaseName。
 * 🚨 password を返す connection-info (GET /v1/postgres/{ID}/connection-info) は読まない。応答の本文はそのまま返さない (id と databaseName だけ)。
 * 例外は投げない。戻り = { ok: true, id, databaseName } | { ok: false, reason } (reason は REASONS のどれか)。
 *   id が設定の ID と違う = METRICS_WRONG_RESOURCE / databaseName が無い・文字でない = METRICS_SHAPE
 * @param {{ apiKey: RenderApiKey, resourceId: string, fetchImpl?: typeof fetch, timeoutMs?: number, baseUrl?: string }} opts
 */
export async function fetchPostgresIdentity(opts = {}) {
  try {
    const { apiKey, resourceId, fetchImpl = globalThis.fetch, timeoutMs = DEFAULT_TIMEOUT_MS, baseUrl = RENDER_API_BASE } = opts;
    if (!(apiKey instanceof RenderApiKey) || typeof resourceId !== 'string' || !RESOURCE_ID_RE.test(resourceId)) return fail('METRICS_CONFIG');
    if (typeof fetchImpl !== 'function') return fail('METRICS_CONFIG');
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 10000) return fail('METRICS_CONFIG');
    if (baseUrl !== RENDER_API_BASE && !LOOPBACK_BASE_RE.test(baseUrl)) return fail('METRICS_CONFIG');
    const ac = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ac.abort(); }, timeoutMs);
    try {
      let res;
      try {
        res = await fetchImpl(`${baseUrl}/postgres/${encodeURIComponent(resourceId)}`, {
          method: 'GET',
          headers: { Authorization: `Bearer ${revealKey(apiKey)}`, Accept: 'application/json' },
          redirect: 'error',
          signal: ac.signal,
        });
      } catch {
        return fail(timedOut ? 'METRICS_TIMEOUT' : 'METRICS_NETWORK');
      }
      if (res.status !== 200) return fail(await statusReason(res));
      let body;
      try { body = parseJsonIntegersOnly(await readBodyTextCapped(res)); } catch (e) {
        if (timedOut) return fail('METRICS_TIMEOUT');
        return fail(e instanceof Reject ? e.reason : 'METRICS_SHAPE');
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) return fail('METRICS_SHAPE');
      if (body.id !== resourceId) return fail('METRICS_WRONG_RESOURCE');
      if (typeof body.databaseName !== 'string' || !body.databaseName) return fail('METRICS_SHAPE');
      return Object.freeze({ ok: true, id: body.id, databaseName: body.databaseName });
    } finally {
      clearTimeout(timer);
      ac.abort();
    }
  } catch (e) {
    return fail(e instanceof Reject ? e.reason : 'METRICS_INTERNAL');
  }
}

/**
 * 使う直前の鮮度の再検査 (§3.10 の取引の 4. = lock の直後)。この部品が作った snapshot だけを認める (手で作った物・古い物は STALE)
 * @returns {{ ok: true } | { ok: false, reason: 'METRICS_STALE' }}
 */
export function checkFreshness(snapshot, nowMs = Date.now()) {
  try {
    if (!snapshot || typeof snapshot !== 'object' || !issued.has(snapshot) || !Number.isFinite(nowMs)) return fail('METRICS_STALE');
    const oldest = Date.parse(snapshot.oldestPointAt), fetched = Date.parse(snapshot.fetchedAt);
    if (!Number.isFinite(oldest) || !Number.isFinite(fetched)) return fail('METRICS_STALE');
    if (nowMs < fetched) return fail('METRICS_STALE');   // 時計が戻った = 鮮度を言えない
    if (nowMs - oldest > MAX_AGE_MS) return fail('METRICS_STALE');
    return Object.freeze({ ok: true });
  } catch { return fail('METRICS_STALE'); }
}
