/**
 * NE の商品 / セット商品の取得の件数と「取得中」の印 (広げる道 PR-9。設計 = 新商品の入口を広げる道 v14 §3.6.3・§3.7・§5.2・§8-4b・R-g)
 *
 * ■ 取得の件数 (sync_meta の ne_api_<kind>_fetch_counts。1 つの JSON)
 *   取得 (ne-api.js の fetchProducts / fetchSetProducts) が、完了の印 (ne_api_<kind>_complete_at) を書くのと **同じ SQLite の書き込みの取引** で残す。
 *   照合 ② はこれを、生の P・S を読むのと **同じ読み取りの取引** で読む (readNeFetchCounts)。
 *
 *   契約の鍵 (設計 v13 §3.6.3 の表。照合の封の products_fetch / set_fetch = fetchCountsForSeal の形。どれも 0 以上の整数):
 *     fetched_rows            API から受け取った行の数 (全部のページの合計)
 *     write_attempts          raw 表へ書くことを試みた行の数 (INSERT OR REPLACE の回数 = 重なった行も含む。商品 = 通し番号の増え rev1 − rev0 と同じ)
 *     stored_rows             その取得の synced_at で raw 表に残った distinct の行の数 (= complete_count。取引の中で DB で数える)
 *     dropped_no_code         コードが空で落とした行 (商品 = 商品コード / セット = セット商品コード (親))
 *     dropped_missing_fields  要る欄が欠けて落とした行 (商品 = 0 / セット = 親はあるが構成品の商品コード (子) が空)
 *   式 (これ 1 つ): fetched_rows = write_attempts + dropped_no_code + dropped_missing_fields
 *   重なり = write_attempts − stored_rows (照合の DB が計算する。重なりの鍵は持たない = 二重に数えない)
 *
 *   契約の外の補助 (照合の封には渡さない): dropped_missing_detail (列ごとの内訳・式に使わない)・
 *     page_limit / pages / page_rows / last_page_rows (API を呼んだ回数と各回の行数。最後のページが短い = そこで止めた。R-g の調べ用)・
 *     version・kind・complete_at・complete_rev (どの完了の印の回の記録か)・
 *     fetch_fingerprint (取得の版 = computeFetchFingerprint。設計 v14 §3.7 の component = fetch。照合が封の expected_fetch_fp と比べる)・
 *     notes (行は落とさないが既定値で書いた行の数。セット = quantity_defaulted_rows)・
 *     started_at / finished_at (取得の始め / 完了。ISO の UTC・ミリ秒。設計 R20: 完了の印の時刻 complete_at は raw の集合の世代 = 始めの時刻のまま変えない。
 *     完了の時刻は finished_at = 完了の印を書く取引の中の今。キューの取得は products の finished_at の後に始める)
 *   write_attempts は通し番号 (revision) からは導かない: 書いた行を直接数える (セットは入れ替えの DELETE でも通し番号が増える = 前の回の行数が混ざる。R14)
 *
 *   書く前の確かめ (ne-api.js。崩れたら完了の印を書かない = fail-closed):
 *     式・stored_rows ≤ write_attempts・ページ (Σpage_rows = fetched_rows・途中のページは満杯・最後のページは短い) に加えて、
 *     **取得のコードが数えた重なり (同じキーが 2 度目以降に来た行) = write_attempts − stored_rows** (DB で数えた行と、コードの数えが合う)。
 *     重なりは記録しないので、この最後の確かめは書く時だけ (checkNeFetchCounts の expectDuplicates)。
 *
 * ■ 取得中の印 (sync_meta の ne_api_<kind>_in_progress。設計 v13 §5.2・R12 M4)
 *   取得の始め (最初の API の呼び出しの前) に独立した取引で commit し、完了の印と同じ取引で消す (自分の run_id の印だけ)。
 *   途中で失敗した・件数が合わず完了の印を付けなかった回は **残る** (次に最後まで取れた回が上書きして消す)。
 *   🚨 同じ種類の取得は 1 本ずつ (Codex PR #1642 R1 Medium): 印の取得がまだ生きていれば新しい取得は印を書かずに throw (NE_FETCH_BUSY)。
 *   死んだ印 (この process で終わった回・PID が居ない / node でない / 印より後に始まったプロセス = PID の使い回し・形が違う・始めた時刻が未来・
 *   NE_FETCH_STALE_MS より古い) だけ回収する (judgeNeFetchMark。Codex #1642 R2 Medium)。
 *   = 「A 開始 → B 開始 → B 完了 → A がまだ通信中」で印が消える、が起きない
 *   セットの取得は全部のページをメモリに集めてから書く = API と通信している間は SQLite の書き込みの鍵を持たない → 開く前のゲート (PR-7) は
 *   BEGIN IMMEDIATE ではこの間を見つけられないので、この印で拒む。
 *
 * 🚨「API の総件数との一致」は証明しない (今の取得は総件数を受け取っていない。設計 R11 H4)。最後のページの小さな欠けは R-g (照合の only_in_cdb の増えで見る)。
 * 🚨 このファイルは db.js (取込の書き込みの接続) を読み込まない = 照合 (読み取り専用の接続) からも使える
 */

import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isAliveNodeSince, FUTURE_SKEW_MS } from './retry-lock.js';

export const NE_FETCH_COUNTS_VERSION = 'fc1';
export const NE_FETCH_KINDS = Object.freeze(['products', 'setproducts']);
export const NE_FETCH_COUNTS_KEY = Object.freeze({ products: 'ne_api_products_fetch_counts', setproducts: 'ne_api_setproducts_fetch_counts' });
export const NE_FETCH_IN_PROGRESS_KEY = Object.freeze({ products: 'ne_api_products_in_progress', setproducts: 'ne_api_setproducts_in_progress' });

/**
 * 取得の版 (設計 v14 §3.7 の component = fetch・R14)。対象のファイル (リポジトリの根からのパス・この固定の順) =
 *   取得 (ne-api.js)・取得が使う守りの部品 (このファイル)・通し番号 / 完了の印 / 書き方の保存がある db.js・取得中の印の生きているかの判定 (retry-lock.js)・アップロードキューの取得 (ne-upload-queue.js。PR-9b)。
 * 版 = 各ファイルについて「パス + NUL + 中身 (改行を LF にそろえる = CRLF → LF。Windows の checkout でも同じ値) + NUL」をこの順につなげた UTF-8 の sha256 (64 桁の 16 進)。
 * 取得は API の入力を読む前に 1 回だけ計算し、完了まで同じ値を持ち回る (途中でファイルが変わっても、記録する版は始めに計算した値)。
 * 🚨 一覧と計算はここ 1 か所 (PR-1 の版の登録の CLI はこれを import して使う)
 */
export const NE_FETCH_FINGERPRINT_FILES = Object.freeze(['apps/warehouse/ne-api.js', 'apps/warehouse/ne-fetch-counts.js', 'apps/warehouse/db.js', 'apps/warehouse/retry-lock.js', 'apps/warehouse/ne-upload-queue.js']);
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
/** 取得の版を計算する。root = リポジトリの根 (既定 = このファイルから見た根)。読めないファイルがあれば throw */
export function computeFetchFingerprint(root = REPO_ROOT) {
  const h = crypto.createHash('sha256');
  for (const f of NE_FETCH_FINGERPRINT_FILES) h.update(`${f}\u0000${fs.readFileSync(path.join(root, f), 'utf8').replace(/\r\n/g, '\n')}\u0000`, 'utf8');
  return h.digest('hex');
}
const FP_RE = /^[0-9a-f]{64}$/;
/** 照合の封に渡す鍵 (設計 v13 §3.6.3・§8-4b)。この順・この 5 つだけ */
export const NE_FETCH_SEAL_KEYS = Object.freeze(['fetched_rows', 'write_attempts', 'stored_rows', 'dropped_no_code', 'dropped_missing_fields']);
/**
 * 落とした行の分類 (設計 R13 Medium。endpoint ごとの必要欄と、排他の分類の順を固定する。1 行は必ず 1 つの分類 = 二重に数えない):
 *   商品 (api_v1_master_goods)    必要欄 = goods_id。          goods_id が空 (欄が無い・null・空文字) → dropped_no_code。ほかの欄の欠けでは落とさない (今までどおり既定値で書く)
 *   セット (api_v1_master_setgoods) 必要欄 = set_goods_id → set_goods_detail_goods_id の順に見る。
 *     1. set_goods_id が空 → dropped_no_code (子も空の行・ほかの欄がどれだけ欠けていても、ここで決まる)
 *     2. 親はあるが set_goods_detail_goods_id が空 → dropped_missing_fields (内訳 set_goods_detail_goods_id)
 *     ほかの欄 (名前・売価・数量・代表・作成日) の欠けでは落とさない
 * dropped_missing_fields の列ごとの内訳の鍵 = 上の 2 の欄の全部。商品 = なし / セット = 構成品の商品コード
 */
export const NE_FETCH_MISSING_FIELDS = Object.freeze({
  products: Object.freeze([]),
  setproducts: Object.freeze(['set_goods_detail_goods_id']),
});
/**
 * 行は落とさないが、今の取り込みが既定値に置き換えて書いた行の数 (別の integrity 情報・式には使わない。R14)。notes の鍵:
 *   セット quantity_defaulted_rows = 構成品の数 (set_goods_detail_quantity) を整数として読めない・0 で、1 として書いた行 (書いた行の中で数える)
 */
export const NE_FETCH_NOTE_KEYS = Object.freeze({
  products: Object.freeze([]),
  setproducts: Object.freeze(['quantity_defaulted_rows']),
});
const COUNT_FIELDS = [...NE_FETCH_SEAL_KEYS, 'page_limit', 'pages', 'last_page_rows', 'complete_rev'];

const isCount = (v) => Number.isSafeInteger(v) && v >= 0;

/**
 * 取得の件数の記録を確かめる。返り値 = 崩れた点の一覧 (空 = 合格)。書く前 (ne-api.js) と読む時 (readNeFetchCounts) の両方で使う
 * @param {'products'|'setproducts'} kind
 * @param {object} rec
 * @param {{ expectDuplicates?: number }} [opts]  書く時だけ: 取得のコードが数えた重なり (= write_attempts − stored_rows であるべき)
 */
export function checkNeFetchCounts(kind, rec, opts = {}) {
  const fields = NE_FETCH_MISSING_FIELDS[kind];
  if (!fields) return [`unknown_kind:${kind}`];
  if (!rec || typeof rec !== 'object' || Array.isArray(rec)) return ['not_object'];
  const p = [];
  if (rec.version !== NE_FETCH_COUNTS_VERSION) p.push(`version:${rec.version}`);
  if (rec.kind !== kind) p.push(`kind:${rec.kind}`);
  if (typeof rec.fetch_fingerprint !== 'string' || !FP_RE.test(rec.fetch_fingerprint)) p.push('fetch_fingerprint');
  if (typeof rec.complete_at !== 'string' || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(rec.complete_at)) p.push('complete_at');
  // 取得の始め (= 完了の印の時刻と同じ時刻・秒まで同じ) と完了 (完了の印を書いた取引の中の今)。ISO の UTC・ミリ秒。完了 ≧ 始め (設計 R20)
  const isoOk = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(v) && Number.isFinite(Date.parse(v)) && new Date(Date.parse(v)).toISOString() === v;
  if (!isoOk(rec.started_at)) p.push('started_at');
  else if (typeof rec.complete_at === 'string' && rec.started_at.replace('T', ' ').slice(0, 19) !== rec.complete_at) p.push('started_at_ne_complete_at');
  if (!isoOk(rec.finished_at)) p.push('finished_at');
  else if (isoOk(rec.started_at) && Date.parse(rec.finished_at) < Date.parse(rec.started_at)) p.push('finished_before_started');
  for (const f of COUNT_FIELDS) if (!isCount(rec[f])) p.push(`not_count:${f}`);
  const d = rec.dropped_missing_detail;
  if (!d || typeof d !== 'object' || Array.isArray(d)) p.push('dropped_missing_detail');
  else {
    const keys = Object.keys(d).sort();
    if (keys.join(',') !== [...fields].sort().join(',')) p.push(`detail_keys:${keys.join(',')}`);   // 欄の欠け・余分 = 知らない形
    for (const f of fields) if (!isCount(d[f])) p.push(`not_count:detail.${f}`);
  }
  const n = rec.notes, noteKeys = NE_FETCH_NOTE_KEYS[kind];
  if (!n || typeof n !== 'object' || Array.isArray(n)) p.push('notes');
  else {
    const keys = Object.keys(n).sort();
    if (keys.join(',') !== [...noteKeys].sort().join(',')) p.push(`note_keys:${keys.join(',')}`);
    for (const k of noteKeys) if (!isCount(n[k])) p.push(`not_count:notes.${k}`);
  }
  if (!Array.isArray(rec.page_rows) || !rec.page_rows.every(isCount)) p.push('page_rows');
  if (p.length) return p;   // 形が崩れていれば関係は見ない
  if (kind === 'setproducts' && n.quantity_defaulted_rows > rec.write_attempts) p.push('quantity_defaulted_gt_attempts');
  if (rec.fetched_rows !== rec.write_attempts + rec.dropped_no_code + rec.dropped_missing_fields) p.push('fetched_ne_attempts_plus_dropped');
  if (rec.stored_rows > rec.write_attempts) p.push('stored_gt_attempts');
  if (fields.reduce((s, f) => s + d[f], 0) !== rec.dropped_missing_fields) p.push('detail_sum_ne_missing_fields');
  if (opts.expectDuplicates !== undefined && rec.write_attempts - rec.stored_rows !== opts.expectDuplicates) p.push('stored_ne_attempts_minus_duplicates');
  if (rec.page_rows.reduce((s, n) => s + n, 0) !== rec.fetched_rows) p.push('page_rows_sum_ne_fetched');
  if (rec.pages !== rec.page_rows.length) p.push('pages_ne_page_rows');
  if (rec.pages < 1) p.push('no_page');   // API を 1 回も呼ばずに完了はしない
  else {
    if (rec.last_page_rows !== rec.page_rows[rec.pages - 1]) p.push('last_page_rows');
    if (rec.page_limit < 1) p.push('page_limit');
    else {
      if (rec.page_rows.slice(0, -1).some((n) => n !== rec.page_limit)) p.push('page_not_full_before_last');   // 取得は短いページで止まる = 途中のページは満杯のはず
      if (rec.last_page_rows >= rec.page_limit) p.push('last_page_not_short');                               // 満杯のページの後は必ず次を呼ぶ
    }
  }
  return p;
}

/** 照合の封に渡す形 (設計 v13 §8-4b の products_fetch / set_fetch)。契約の 5 つの鍵だけ */
export function fetchCountsForSeal(rec) {
  return Object.fromEntries(NE_FETCH_SEAL_KEYS.map((k) => [k, rec[k]]));
}

/**
 * sync_meta の値 (key → value の object) から取得の件数を読む。取得中の印が無く、完了の印 (complete_at・complete_rev) と同じ回の記録のときだけ ok。
 * @param {Record<string, string|null|undefined>} meta  sync_meta の key → value
 * @returns {{ ok: boolean, fetch_fingerprint: string|null, fetch_fingerprint_mismatch?: true, products: object, setproducts: object }}
 *   fetch_fingerprint = 両方の取得の版 (同じときだけ。照合が封へ渡す)。両方 ok で版が違う = 朝の 2 つの取得が別のコード → ok = false・fetch_fingerprint_mismatch
 *   各 kind = { ok, reason?, problems?, in_progress?, counts?, seal? }
 *   reason: fetch_in_progress (取得中の印がある = 取得が走っている / 途中で終わった。in_progress = 印の中身・読めなければ null) /
 *           no_mark (完了の印が無い) / no_record (記録が無い = PR-9 の前の取得・CSV の取込の後) / unreadable (JSON が壊れた) /
 *           invalid (checkNeFetchCounts が崩れを見つけた) / not_this_fetch (印の時刻・通し番号と記録が違う回)
 */
export function evalNeFetchCounts(meta) {
  const out = { ok: true, fetch_fingerprint: null };
  for (const kind of NE_FETCH_KINDS) {
    const r = evalOne(kind, meta || {});
    out[kind] = r;
    if (!r.ok) out.ok = false;
  }
  if (out.ok) {
    const fps = NE_FETCH_KINDS.map((k) => out[k].counts.fetch_fingerprint);
    if (fps.every((f) => f === fps[0])) out.fetch_fingerprint = fps[0];
    else { out.ok = false; out.fetch_fingerprint_mismatch = true; }
  }
  return out;
}
function evalOne(kind, meta) {
  // 取得中の印がある = 取得が走っている / 途中で終わった (完了の印より先に見る。読めない値も「取得中」とみなす)
  const ip = meta[NE_FETCH_IN_PROGRESS_KEY[kind]];
  if (ip != null) { let v; try { v = JSON.parse(ip); } catch { v = null; } return { ok: false, reason: 'fetch_in_progress', in_progress: v }; }
  const at = meta[`ne_api_${kind}_complete_at`] ?? null, rev = meta[`ne_api_${kind}_complete_rev`] ?? null;
  if (at == null || rev == null) return { ok: false, reason: 'no_mark' };
  const raw = meta[NE_FETCH_COUNTS_KEY[kind]];
  if (raw == null) return { ok: false, reason: 'no_record' };
  let rec;
  try { rec = JSON.parse(raw); } catch { return { ok: false, reason: 'unreadable' }; }
  const problems = checkNeFetchCounts(kind, rec);
  if (problems.length) return { ok: false, reason: 'invalid', problems };
  if (rec.complete_at !== at || String(rec.complete_rev) !== String(rev)) return { ok: false, reason: 'not_this_fetch' };
  return { ok: true, counts: rec, seal: fetchCountsForSeal(rec) };
}

/**
 * 照合 ② 用: **呼び手が開いた読み取りの取引の中で** (compare-ne.mjs の readNeSide の BEGIN 〜 COMMIT。生の P・S を読むのと同じ取引) 取得の件数と取得中の印を読む。
 * この関数は取引を開かない・閉じない (呼び手の取引の snapshot で読む)
 * @param {import('better-sqlite3').Database} db
 */
export function readNeFetchCounts(db) {
  const keys = NE_FETCH_KINDS.flatMap((k) => [`ne_api_${k}_complete_at`, `ne_api_${k}_complete_rev`, NE_FETCH_COUNTS_KEY[k], NE_FETCH_IN_PROGRESS_KEY[k]]);
  const meta = Object.fromEntries(db.prepare(`SELECT key, value FROM sync_meta WHERE key IN (${keys.map(() => '?').join(', ')})`).all(...keys).map((r) => [r.key, r.value]));
  return evalNeFetchCounts(meta);
}


const nowText = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
/** 印を回収してよい年齢 (これより古い印は、PID が生きて見えても回収する = Windows の PID の使い回し対策。取得は数分で終わる) */
export const NE_FETCH_STALE_MS = 6 * 60 * 60 * 1000;
/** この process の中で今走っている取得の run_id (取得の関数の finally で外す) */
const RUNNING = new Set();
/** 印の形 (beginNeFetch が書く形と同じでなければ壊れた印 = 回収する) */
function markShapeOk(v, kind) {
  return !!v && typeof v === 'object' && !Array.isArray(v)
    && v.version === NE_FETCH_COUNTS_VERSION && (kind === undefined || v.kind === kind)
    && typeof v.run_id === 'string' && /^[0-9a-f-]{36}$/.test(v.run_id)
    && typeof v.started_at === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(v.started_at)
    && Number.isSafeInteger(v.started_ms) && v.started_ms > 0
    && Number.isSafeInteger(v.pid) && v.pid > 0
    && typeof v.host === 'string' && v.host.length > 0;
}
/**
 * 前の印がまだ走っている取得のものか (Codex #1642 R1 Medium: 生きている取得の印は上書きしない・死んだ / 期限切れの印だけ回収する /
 * R2 Medium: PID の使い回し・関係の無いプロセスを「取得中」と見ない)。
 *   読めない・形が違う (欄の欠け・型・知らない版・別の種類) = 回収 (このコードが書いた印ではない) /
 *   始めた時刻が未来 (FUTURE_SKEW_MS より先) = 回収 / NE_FETCH_STALE_MS より古い = 回収 /
 *   同じ host・同じ process = この process で今走っている run_id のときだけ生きている (失敗して finally を抜けた回は死んでいる) /
 *   同じ host・別の process = その PID が **生きている node** で、**そのプロセスが印を書く前 (+ 30 秒) から動いている** ときだけ生きている
 *     (retry-lock.js の isAliveNodeSince と同じ判定 = 後から始まったプロセス = PID の使い回し = 回収。Windows は tasklist と Get-Process の開始時刻) /
 *   別の host = 期限まで生きているとみなす (確かめられない = 並走しない側)
 * @param {string} raw  sync_meta の値
 * @param {number} [nowMs]
 * @param {{ kind?: string, isAlive?: (pid: number, startedAtIso: string) => boolean }} [o]  isAlive は試験で差し替える
 * @returns {{ alive: boolean, prev: object|null }}
 */
export function judgeNeFetchMark(raw, nowMs = Date.now(), { kind, isAlive = (pid, at) => isAliveNodeSince(pid, at, { now: new Date(nowMs) }) } = {}) {
  let v = null;
  try { v = JSON.parse(raw); } catch { return { alive: false, prev: null }; }
  if (!markShapeOk(v, kind)) return { alive: false, prev: v };
  if (v.started_ms > nowMs + FUTURE_SKEW_MS) return { alive: false, prev: v };
  if (nowMs - v.started_ms >= NE_FETCH_STALE_MS) return { alive: false, prev: v };
  if (v.host !== os.hostname()) return { alive: true, prev: v };
  if (v.pid === process.pid) return { alive: RUNNING.has(v.run_id), prev: v };
  return { alive: !!isAlive(v.pid, new Date(v.started_ms).toISOString()), prev: v };
}
/**
 * 取得中の印を書く (取得の始め・最初の API の呼び出しの前。呼び手の取引の中で = 呼び手が commit する)。返り値 = 書いた値 (消す時に渡す)。
 * 同じ種類の取得の印が既にあり、その取得が生きていれば **書かずに throw** (code = NE_FETCH_BUSY。同じ種類の取得は 1 本ずつ)。
 * 死んだ / 期限切れの印は上書きして回収する。
 * 中身 = { version, kind, run_id (この回の一意の ID), started_at (この回の時刻 = 完了の印に書く時刻), started_ms, pid, host }
 * @param {import('better-sqlite3').Database} db  取込の書き込みの接続
 */
export function beginNeFetch(db, kind, startedAt) {
  const key = NE_FETCH_IN_PROGRESS_KEY[kind];
  if (!key) throw new Error(`beginNeFetch: 知らない種類 ${kind}`);
  const cur = db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(key);
  if (cur) {
    const j = judgeNeFetchMark(cur.value, Date.now(), { kind });
    if (j.alive) {
      const e = new Error(`[NE] 同じ種類の取得 (${kind}) がまだ走っている (pid ${j.prev.pid}・${j.prev.host}・開始 ${j.prev.started_at}) → この取得はしない`);
      e.code = 'NE_FETCH_BUSY';
      throw e;
    }
  }
  const value = JSON.stringify({ version: NE_FETCH_COUNTS_VERSION, kind, run_id: crypto.randomUUID(), started_at: startedAt, started_ms: Date.now(), pid: process.pid, host: os.hostname() });
  db.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(key, value, nowText());
  RUNNING.add(JSON.parse(value).run_id);
  return value;
}
/** 自分が書いた取得中の印だけを消す (完了の印と同じ取引の中で)。返り値 = 消した行の数 (0 = 自分の印ではない = 残す) */
export function endNeFetch(db, kind, value) {
  const key = NE_FETCH_IN_PROGRESS_KEY[kind];
  if (!key) throw new Error(`endNeFetch: 知らない種類 ${kind}`);
  return db.prepare('DELETE FROM sync_meta WHERE key = ? AND value = ?').run(key, value).changes;
}
/**
 * 取得の関数を抜けるとき (成功・失敗・印を付けなかった回のどれでも) に呼ぶ。DB の印には触らない (失敗した回の印は残る = ゲートが拒む)。
 * この process の「今走っている」から外す = 次の取得がその印を死んだ印として回収できる
 */
export function releaseNeFetch(value) {
  try { RUNNING.delete(JSON.parse(value).run_id); } catch { /* 読めない値 = 何もしない */ }
}
