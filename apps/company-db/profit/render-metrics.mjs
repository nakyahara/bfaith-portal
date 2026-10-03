/**
 * render-metrics.mjs — Render の API から Company DB (Render Postgres) の Memory・Memory の上限・Disk Usage・Disk Capacity を読む部品
 *   (D-60 v3.4 の PR 2・設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「資源の関門」)
 *
 * 🚨 使う所はまだ無い (単体の部品)。利益の受け口 (/amazon-profit/daily・/totals) は 503 のまま。開けるのは §3.10 の PR 6 だけ
 *
 * 契約 (§3.10「資源の関門」・Codex R-D60-v3-3 M4 / v3-4 M5・H4):
 *   - Postgres の resource の ID を設定で固定 (CDB_RENDER_PG_RESOURCE_ID・`dpg-` で始まる)。応答の系列の resource の label がこの ID と同じでなければ「不可」
 *   - 使う label を固定 (RESOURCE_LABEL_FIELD)。label が無い・2 つある・値が違う → 「不可」
 *   - 単位は bytes に直す (BYTE_UNITS にある単位だけ = 推測で倍率を掛けない。知らない単位は「不可」)。
 *     使用 (memory・disk usage) は切り上げ・上限 (memory limit・disk capacity) は切り捨て (安全側は方向で違う)
 *   - 各 endpoint は系列がちょうど 1 つ。その系列の最新の点を採る
 *   - Memory と Memory の上限・Disk Usage と Capacity は同じ resource で、点の時刻の差が 2 分以内
 *   - 空の配列・空の系列・重複の系列・同じ時刻の点が 2 つ・未来の時刻・負の値・数でない値・別の resource → 全部「不可」
 *   - 取得から 2 分を超えた値は「不可」(取った時に確かめる + 使う直前に checkFreshness でもう一度)
 *   - HTTP の timeout は短い (既定 全体 5 秒)。401・403・429・5xx・ほかの非 200・網の失敗・timeout・redirect → 全部「不可」
 *   - 「不可」は例外を投げずに { ok: false, reason } で返す (呼び手は 503 PROFIT_METRICS_UNAVAILABLE にする)。reason は下の REASONS の固定のコードだけ
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
 */
import { inspect } from 'node:util';

export const RENDER_API_BASE = 'https://api.render.com/v1';
/** 読む 4 つ (この順で結果を確かめる = 理由のコードが決まった順になる) */
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
/** bytes と読んでよい単位 (倍率 1 だけ)。🚨 Render の応答の unit の綴りは公式の説明に無い = 知らない綴り (MB など) は推測で掛け算せず「不可」 */
export const BYTE_UNITS = Object.freeze(['bytes', 'byte', 'B', 'By']);
export const MAX_AGE_MS = 2 * 60 * 1000;          // 取得 (と使う時) から 2 分を超えた点は「不可」
export const MAX_PAIR_SKEW_MS = 2 * 60 * 1000;    // 使用と上限の点の時刻の差
export const DEFAULT_TIMEOUT_MS = 5000;           // 4 つの要求の全体
export const RESOLUTION_SECONDS = 30;             // 公式の最小
export const WINDOW_MS = 10 * 60 * 1000;          // 読む区間 (今から 10 分前まで)
export const MAX_BODY_BYTES = 1024 * 1024;

/** 「不可」の理由のコード (これ以外は返さない)。呼び手は全部 503 PROFIT_METRICS_UNAVAILABLE */
export const REASONS = Object.freeze([
  'METRICS_CONFIG',            // 鍵・resource の ID・送り先・timeout の設定が無い / 形が違う
  'METRICS_AUTH',              // 401・403
  'METRICS_RATE_LIMITED',      // 429
  'METRICS_UPSTREAM',          // 5xx
  'METRICS_HTTP',              // ほかの非 200 (3xx を含む)
  'METRICS_TIMEOUT',           // 全体の timeout
  'METRICS_NETWORK',           // 網の失敗・redirect
  'METRICS_SHAPE',             // JSON でない・配列でない・系列や点の形が違う・時刻や値が読めない・大きすぎる
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

const ISO_RE = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/;
/** 厳しい ISO 8601 (時差つき) → ms。読めなければ null */
const parseTs = (s) => {
  if (typeof s !== 'string') return null;
  const m = ISO_RE.exec(s); if (!m) return null;
  const ms = Date.parse(`${m[1]}.${(m[2] || '0').padEnd(3, '0').slice(0, 3)}${m[3]}`);
  return Number.isFinite(ms) ? ms : null;
};

class Reject extends Error { constructor(reason) { super(reason); this.reason = reason; } }

/** 1 つの endpoint の応答の本文 (parse 済み) を確かめ、最新の点を返す。nowMs = 応答を受けた時刻 */
function pickLatestPoint(body, resourceId, nowMs) {
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
    if (!p || typeof p !== 'object' || Array.isArray(p)) throw new Reject('METRICS_SHAPE');
    const at = parseTs(p.timestamp);
    if (at == null || typeof p.value !== 'number' || !Number.isFinite(p.value)) throw new Reject('METRICS_SHAPE');
    if (p.value < 0) throw new Reject('METRICS_NEGATIVE');
    if (at > nowMs) throw new Reject('METRICS_FUTURE');
    if (seen.has(at)) throw new Reject('METRICS_DUPLICATE_POINT');
    seen.add(at);
    if (!latest || at > latest.at) latest = { at, value: p.value };
  }
  if (nowMs - latest.at > MAX_AGE_MS) throw new Reject('METRICS_STALE');
  return latest;
}

const toBytes = (value, direction) => {
  const b = direction === 'up' ? Math.ceil(value) : Math.floor(value);
  if (!Number.isSafeInteger(b)) throw new Reject('METRICS_SHAPE');
  return b;
};

async function readBodyText(res) {
  const len = Number(res.headers && res.headers.get && res.headers.get('content-length'));
  if (Number.isFinite(len) && len > MAX_BODY_BYTES) throw new Reject('METRICS_SHAPE');
  const text = await res.text();
  if (text.length > MAX_BODY_BYTES) throw new Reject('METRICS_SHAPE');
  return text;
}

const discardBody = async (res) => { try { if (res.body && typeof res.body.cancel === 'function') await res.body.cancel(); } catch { /* 捨てるだけ */ } };

const issued = new WeakSet();

/**
 * Render の API から 4 つの値を読む。例外は投げない
 * @param {{ apiKey: RenderApiKey, resourceId: string, fetchImpl?: typeof fetch, now?: () => number, timeoutMs?: number, baseUrl?: string }} opts
 * @returns {Promise<{ ok: true, snapshot: object } | { ok: false, reason: string }>}
 *   snapshot = { resourceId, fetchedAt, resolutionSeconds, peakProof: false, memoryUsedBytes, memoryLimitBytes, diskUsedBytes, diskCapacityBytes,
 *                points: { memory, memoryLimit, diskUsage, diskCapacity } (各点の時刻 ISO), oldestPointAt }
 */
export async function fetchPostgresMetrics(opts = {}) {
  try {
    const { apiKey, resourceId, fetchImpl = globalThis.fetch, now = Date.now, timeoutMs = DEFAULT_TIMEOUT_MS, baseUrl = RENDER_API_BASE } = opts;
    if (!(apiKey instanceof RenderApiKey) || typeof resourceId !== 'string' || !RESOURCE_ID_RE.test(resourceId)) return fail('METRICS_CONFIG');
    if (typeof fetchImpl !== 'function' || typeof now !== 'function') return fail('METRICS_CONFIG');
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 10000) return fail('METRICS_CONFIG');
    if (baseUrl !== RENDER_API_BASE && !LOOPBACK_BASE_RE.test(baseUrl)) return fail('METRICS_CONFIG');

    const startMs = now();
    if (!Number.isFinite(startMs)) return fail('METRICS_CONFIG');
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
        if (st !== 200) {
          await discardBody(res);
          if (st === 401 || st === 403) return fail('METRICS_AUTH');
          if (st === 429) return fail('METRICS_RATE_LIMITED');
          if (st >= 500 && st <= 599) return fail('METRICS_UPSTREAM');
          return fail('METRICS_HTTP');
        }
        let body;
        try { body = JSON.parse(await readBodyText(res)); } catch (e) {
          if (timedOut) return fail('METRICS_TIMEOUT');
          return fail(e instanceof Reject ? e.reason : 'METRICS_SHAPE');
        }
        const nowMs = now();
        if (!Number.isFinite(nowMs)) return fail('METRICS_INTERNAL');
        return Object.freeze({ ok: true, point: pickLatestPoint(body, resourceId, nowMs) });
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
    if (timedOut) return fail('METRICS_TIMEOUT');
    const names = Object.keys(METRIC_ENDPOINTS);
    for (const r of results) if (!r.ok) return fail(r.reason);   // METRIC_ENDPOINTS の順で最初の理由
    const p = Object.fromEntries(names.map((n, i) => [n, results[i].point]));

    // 使用と上限の点の時刻の差 (同じ resource は上で確かめた)。今は「どの点も 2 分以内 (STALE)・未来でない (FUTURE)」から差は 2 分以内に収まる = この検査は
    // MAX_AGE_MS を変えたときの守り (2 つの規則を別々に持つ。試験は fixture では作れない)
    if (Math.abs(p.memory.at - p.memoryLimit.at) > MAX_PAIR_SKEW_MS) return fail('METRICS_PAIR_SKEW');
    if (Math.abs(p.diskUsage.at - p.diskCapacity.at) > MAX_PAIR_SKEW_MS) return fail('METRICS_PAIR_SKEW');
    const memoryUsedBytes = toBytes(p.memory.value, 'up');
    const memoryLimitBytes = toBytes(p.memoryLimit.value, 'down');
    const diskUsedBytes = toBytes(p.diskUsage.value, 'up');
    const diskCapacityBytes = toBytes(p.diskCapacity.value, 'down');
    if (memoryLimitBytes <= 0 || diskCapacityBytes <= 0) return fail('METRICS_ZERO_LIMIT');
    if (memoryUsedBytes > memoryLimitBytes || diskUsedBytes > diskCapacityBytes) return fail('METRICS_INCONSISTENT');

    const oldest = Math.min(...names.map((n) => p[n].at));
    const snapshot = Object.freeze({
      resourceId,
      fetchedAt: new Date(startMs).toISOString(),
      resolutionSeconds: RESOLUTION_SECONDS,
      peakProof: false,   // 30 秒の bucket の値は区間の最大の証明にならない (要求の直前の関門にだけ使う)
      memoryUsedBytes, memoryLimitBytes, diskUsedBytes, diskCapacityBytes,
      points: Object.freeze(Object.fromEntries(names.map((n) => [n, new Date(p[n].at).toISOString()]))),
      oldestPointAt: new Date(oldest).toISOString(),
    });
    issued.add(snapshot);
    return Object.freeze({ ok: true, snapshot });
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
