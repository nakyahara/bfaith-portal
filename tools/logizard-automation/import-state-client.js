/**
 * import-state-client.js — ポータルの「ロジザードの取込の状態」の口を呼ぶ (マスタ正本切替 ③c-1b-1・契約 v3 H1・H5)
 *
 * 使う = miniPC の自動の ③ (③c-1b-2)・少数件の試験・毎晩の成果物の送り (③c-1b-3b-3)・人の CLI (import-state-cli.js)。
 * env: LZ_LOCK_TOKEN (Bearer)・LZ_IMPORT_STATE_URL (既定 https://bfaith-portal.onrender.com)
 * 口に届かない・断られた = ImportStateClientError (呼び手は「始めない」側に倒す)。
 *
 * 手元の初期化の印 (H5): ポータルの init_id と同じものを各 PC のファイルに持ち、起動のたびに照合する。
 *   食い違い・片方が無い = 止める (記録の消失・設定違い)。
 */
import fs from 'fs';
import path from 'path';

export const DEFAULT_URL = 'https://bfaith-portal.onrender.com';
export const BASE_PATH = '/apps/logizard-import-state/api';

export class ImportStateClientError extends Error {
  constructor(code, message, status = null) { super(message); this.code = code; this.status = status; }
}
/** limit は 1〜max の整数 (ほか = 送らずに断る) */
function checkLimit(limit, max) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > max) throw new ImportStateClientError('bad_request', `limit は 1〜${max} の整数`);
  return limit;
}

/**
 * @param {object} [opts]
 * @param {string} [opts.url]
 * @param {string} [opts.token]
 * @param {typeof fetch} [opts.fetchImpl]
 * @param {number} [opts.timeoutMs]
 */
export function createImportStateClient({ url = process.env.LZ_IMPORT_STATE_URL || DEFAULT_URL, token = process.env.LZ_LOCK_TOKEN, fetchImpl = fetch, timeoutMs = 15000 } = {}) {
  let origin;
  try { origin = new URL(String(url).trim()).origin; } catch { throw new ImportStateClientError('bad_url', `LZ_IMPORT_STATE_URL が URL でない: ${url}`); }
  if (!/^https:/.test(origin) && !/^http:\/\/(127\.0\.0\.1|localhost)(:\d+)?$/.test(origin)) throw new ImportStateClientError('bad_url', 'https だけ (Bearer を載せるため。試験の localhost は除く)');
  if (!token) throw new ImportStateClientError('no_token', 'LZ_LOCK_TOKEN が無い');
  /** raw = 本文をバイト列のまま送る (成果物の CSV) */
  async function call(method, p, body, { raw = false } = {}) {
    let res;
    try {
      res = await fetchImpl(`${origin}${BASE_PATH}${p}`, {
        method, headers: { Authorization: `Bearer ${token}`, ...(body ? { 'Content-Type': raw ? 'application/octet-stream' : 'application/json' } : {}) },
        body: body ? (raw ? body : JSON.stringify(body)) : undefined, signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (e) { throw new ImportStateClientError('unreachable', `ポータルの口に届かない: ${String(e && e.message).slice(0, 160)}`); }
    let j = null;
    try { j = await res.json(); } catch { /* 下で */ }
    if (!res.ok || !j || j.ok === false) throw new ImportStateClientError((j && j.error) || `http_${res.status}`, (j && j.message) || `HTTP ${res.status}`, res.status);
    return j;
  }
  return {
    status: (events = 20) => call('GET', `/status?events=${events}`),
    init: (b) => call('POST', '/init', b),
    recover: (b) => call('POST', '/recover', b),
    acquire: (b) => call('POST', '/lock/acquire', b),
    extend: (b) => call('POST', '/lock/extend', b),
    release: (b) => call('POST', '/lock/release', b),
    transition: (b) => call('POST', '/transition', b),
    markUnknown: (b) => call('POST', '/mark-unknown', b),
    resolve: (b) => call('POST', '/resolve', b),
    halt: (b) => call('POST', '/halt', b),
    resume: (b) => call('POST', '/resume', b),
    notified: (b) => call('POST', '/notified', b),
    // ③c-1b-3b-2b: 毎晩の成果物 (K3-1)・知らせの outbox (K3-4)
    putArtifact: ({ csvBuf, sourceRunId, targetAsOf, verdict, sha256, rows, by }) => {
      if (!Buffer.isBuffer(csvBuf)) throw new ImportStateClientError('bad_request', 'csvBuf (Buffer) が要る');
      const qs = new URLSearchParams({ source_run_id: String(sourceRunId), target_as_of: String(targetAsOf), verdict: String(verdict), sha256: String(sha256), rows: String(rows), by: String(by) });
      return call('POST', `/artifacts?${qs}`, csvBuf, { raw: true });
    },
    getArtifact: (sourceRunId) => call('GET', `/artifacts/${encodeURIComponent(String(sourceRunId))}`),
    listArtifacts: (limit = 14) => call('GET', `/artifacts?limit=${checkLimit(limit, 60)}`),
    outbox: (limit = 20) => call('GET', `/outbox?limit=${checkLimit(limit, 100)}`),
    outboxSent: (b) => call('POST', '/outbox/sent', b),
    // 毎晩の本番を始められるか (副作用なし・③c-1b-2b-2 N4)
    nightlyReadiness: ({ source_run_id, csv_sha256, rows, target_as_of }) => call('GET', `/nightly-readiness?${new URLSearchParams({ source_run_id: String(source_run_id), csv_sha256: String(csv_sha256), rows: String(rows), target_as_of: String(target_as_of) })}`),
  };
}

/** 手元の初期化の印を読む (無い = null・壊れた = throw) */
export function readLocalInit(file) {
  let txt;
  try { txt = fs.readFileSync(file, 'utf8'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; }
  const j = JSON.parse(txt);
  if (!j || typeof j.init_id !== 'string' || !/^lzi_/.test(j.init_id)) throw new ImportStateClientError('local_init_broken', `手元の初期化の印の形が違う: ${file}`);
  return j;
}

/** 手元の初期化の印を書く (replace = 既にあっても書き換える。人の判断のときだけ) */
export function writeLocalInit(file, { initId, by, note = null, replace = false, now = new Date() }) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const body = JSON.stringify({ init_id: initId, by, note, at: now.toISOString() }, null, 1);
  if (replace) {
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, body, { flag: 'wx' });
    fs.renameSync(tmp, file);
  } else fs.writeFileSync(file, body, { flag: 'wx' });   // あれば断る (EEXIST)
}

/**
 * ポータルと手元の初期化の印を照合する (起動のたび。H5)
 * @returns {{ ok: boolean, reason: string|null, status: object|null }}
 */
export async function checkInit(client, localFile) {
  const status = await client.status(5);
  let local;
  try { local = readLocalInit(localFile); } catch (e) { return { ok: false, reason: `手元の初期化の印が読めない (${e.message})`, status }; }
  if (!status.initialized && !local) return { ok: false, reason: 'まだ初期化していない (import-state-cli.js init)', status };
  if (!status.initialized) return { ok: false, reason: 'ポータルの状態が無い (ポータル側の消失?) = 人が履歴を確かめて recover', status };
  if (!local) return { ok: false, reason: `この PC の初期化の印が無い (${localFile}) = 消失か設定違い。人が確かめて adopt / recover`, status };
  if (local.init_id !== status.init_id) return { ok: false, reason: `初期化の識別子が違う (ポータル ${status.init_id}・この PC ${local.init_id})`, status };
  return { ok: true, reason: null, status };
}
