/**
 * NE の商品 / セット商品の取得の件数と「取得中」の印 (広げる道 PR-9。設計 = 新商品の入口を広げる道 v13 §3.6.3・§5.2・§8-4b・R-g)
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
 *     version・kind・complete_at・complete_rev (どの完了の印の回の記録か)
 *
 *   書く前の確かめ (ne-api.js。崩れたら完了の印を書かない = fail-closed):
 *     式・stored_rows ≤ write_attempts・ページ (Σpage_rows = fetched_rows・途中のページは満杯・最後のページは短い) に加えて、
 *     **取得のコードが数えた重なり (同じキーが 2 度目以降に来た行) = write_attempts − stored_rows** (DB で数えた行と、コードの数えが合う)。
 *     重なりは記録しないので、この最後の確かめは書く時だけ (checkNeFetchCounts の expectDuplicates)。
 *
 * ■ 取得中の印 (sync_meta の ne_api_<kind>_in_progress。設計 v13 §5.2・R12 M4)
 *   取得の始め (最初の API の呼び出しの前) に独立した取引で commit し、完了の印と同じ取引で消す (自分の run_id の印だけ)。
 *   途中で失敗した・件数が合わず完了の印を付けなかった回は **残る** (次に最後まで取れた回が上書きして消す)。
 *   セットの取得は全部のページをメモリに集めてから書く = API と通信している間は SQLite の書き込みの鍵を持たない → 開く前のゲート (PR-7) は
 *   BEGIN IMMEDIATE ではこの間を見つけられないので、この印で拒む。
 *
 * 🚨「API の総件数との一致」は証明しない (今の取得は総件数を受け取っていない。設計 R11 H4)。最後のページの小さな欠けは R-g (照合の only_in_cdb の増えで見る)。
 * 🚨 このファイルは db.js (取込の書き込みの接続) を読み込まない = 照合 (読み取り専用の接続) からも使える
 */

import crypto from 'node:crypto';

export const NE_FETCH_COUNTS_VERSION = 'fc1';
export const NE_FETCH_KINDS = Object.freeze(['products', 'setproducts']);
export const NE_FETCH_COUNTS_KEY = Object.freeze({ products: 'ne_api_products_fetch_counts', setproducts: 'ne_api_setproducts_fetch_counts' });
export const NE_FETCH_IN_PROGRESS_KEY = Object.freeze({ products: 'ne_api_products_in_progress', setproducts: 'ne_api_setproducts_in_progress' });
/** 照合の封に渡す鍵 (設計 v13 §3.6.3・§8-4b)。この順・この 5 つだけ */
export const NE_FETCH_SEAL_KEYS = Object.freeze(['fetched_rows', 'write_attempts', 'stored_rows', 'dropped_no_code', 'dropped_missing_fields']);
/** dropped_missing_fields の列ごとの内訳の鍵 (今のコードが欠けで落とす欄の全部)。商品 = なし / セット = 構成品の商品コード */
export const NE_FETCH_MISSING_FIELDS = Object.freeze({
  products: Object.freeze([]),
  setproducts: Object.freeze(['set_goods_detail_goods_id']),
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
  if (typeof rec.complete_at !== 'string' || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(rec.complete_at)) p.push('complete_at');
  for (const f of COUNT_FIELDS) if (!isCount(rec[f])) p.push(`not_count:${f}`);
  const d = rec.dropped_missing_detail;
  if (!d || typeof d !== 'object' || Array.isArray(d)) p.push('dropped_missing_detail');
  else {
    const keys = Object.keys(d).sort();
    if (keys.join(',') !== [...fields].sort().join(',')) p.push(`detail_keys:${keys.join(',')}`);   // 欄の欠け・余分 = 知らない形
    for (const f of fields) if (!isCount(d[f])) p.push(`not_count:detail.${f}`);
  }
  if (!Array.isArray(rec.page_rows) || !rec.page_rows.every(isCount)) p.push('page_rows');
  if (p.length) return p;   // 形が崩れていれば関係は見ない
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
 * @returns {{ ok: boolean, products: object, setproducts: object }}  各 kind = { ok, reason?, problems?, in_progress?, counts?, seal? }
 *   reason: fetch_in_progress (取得中の印がある = 取得が走っている / 途中で終わった。in_progress = 印の中身・読めなければ null) /
 *           no_mark (完了の印が無い) / no_record (記録が無い = PR-9 の前の取得・CSV の取込の後) / unreadable (JSON が壊れた) /
 *           invalid (checkNeFetchCounts が崩れを見つけた) / not_this_fetch (印の時刻・通し番号と記録が違う回)
 */
export function evalNeFetchCounts(meta) {
  const out = { ok: true };
  for (const kind of NE_FETCH_KINDS) {
    const r = evalOne(kind, meta || {});
    out[kind] = r;
    if (!r.ok) out.ok = false;
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
/**
 * 取得中の印を書く (取得の始め。呼び手の取引の中で = 呼び手が commit する)。前の回が残した印は上書きする。返り値 = 書いた値 (消す時に渡す)
 * 中身 = { version, kind, run_id (この取得の回の一意の ID), started_at (この回の時刻 = 完了の印に書く時刻), pid }
 * @param {import('better-sqlite3').Database} db  取込の書き込みの接続
 */
export function beginNeFetch(db, kind, startedAt) {
  const key = NE_FETCH_IN_PROGRESS_KEY[kind];
  if (!key) throw new Error(`beginNeFetch: 知らない種類 ${kind}`);
  const value = JSON.stringify({ version: NE_FETCH_COUNTS_VERSION, kind, run_id: crypto.randomUUID(), started_at: startedAt, pid: process.pid });
  db.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(key, value, nowText());
  return value;
}
/** 自分が書いた取得中の印だけを消す (完了の印と同じ取引の中で)。返り値 = 消した行の数 (0 = 後から別の取得が印を書いた = その印は残す) */
export function endNeFetch(db, kind, value) {
  const key = NE_FETCH_IN_PROGRESS_KEY[kind];
  if (!key) throw new Error(`endNeFetch: 知らない種類 ${kind}`);
  return db.prepare('DELETE FROM sync_meta WHERE key = ? AND value = ?').run(key, value).changes;
}
