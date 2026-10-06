/**
 * NE のアップロードキューの取得 (広げる道 PR-9b。設計 v20 §3.9 の 4・R17 M1・R18 H2/H3・R19 M2/Low)
 *
 * NE の API `/api_v1_system_que/count` と `/api_v1_system_que/search` (人がアップロードしたファイル名・状態・アップロードした時刻) を、
 * miniPC の今の NE の取得 (ne-api.js の sync) の中で **読むだけ** で取る (同じトークン・書き込みの API は呼ばない)。
 * 翌朝「配った新規登録の CSV を実際に NE に取り込んだ時刻」を照合 ② と DB (PR-1) が見るための材料。
 *
 * ■ いつ: 1 日 1 回・朝の照合 ② の前の sync の中で、**商品の取得 (products) が完了した後** (R19 M2・R20)。
 *   回の記録に、取り始めた時点の商品の完了の印 (products_complete_at・rev) と商品の取得の完了の時刻 (products_finished_at = 件数の記録の finished_at) を残す
 *   = DB が「キューの始め (started_at = 期間の終わり) ≥ products_finished_at」を確かめられる
 * ■ 検索の条件
 *   - 期間の始め = DB の読むだけの関数 `ops.ne_reg_queue_window_start()` (PR-1・watcher・キューの判定の印が無い配った商品の一番古い issued_at − 5 分・最大 30 日)。
 *     関数が null = そういう商品が無い → 取得を始めた時刻 − 1 日。**DB を読めない (env が無い・つながらない・関数が無い) = キューを読まない** (その回は完全でない = waiting)
 *   - 期間の終わり = 取得を始めた時刻 (固定 = 読んでいる間に増えた行で数が動かない)
 *   - 形 = JST の 'YYYY-MM-DD HH:MM:SS' (NE の時計は JST)。`que_creation_date-gte` / `-lte`・`que_method_name-eq` = SYOHIN_KIHON_CSV (ほかの機能の行を読まない)
 * ■ 順と完全 (state = complete) の条件 (R18 H3)
 *   - `/count` (前) → `/search` で全部のページ (1 回目) → 全部のページ (2 回目) → `/count` (後)
 *   - 🚨 `/search` の応答の count は「その応答の行の数」で総件数ではない (公式の例: offset=1・limit=2 で count=2) = 総件数としては使わない
 *   - 完全 = 前と後の総件数が同じ (読める) / 1 回目も 2 回目も: 読んだ行の数 = 総件数・que_id の重なりが無い・que_id の無い行が無い・
 *     どのページも応答の count = その行の数・途中のページは満杯・最後のページは短い / 1 回目と 2 回目で que_id の集合と各行のハッシュが同じ / 取得の版がある
 *   - 行のハッシュ (R19 Low) = RFC 8785 (JCS) の canonical JSON の sha256。API が返した項目の名前をそのままキーに・値は生の文字 (数も文字)・null は null・
 *     空の文字は ""・欠けた項目はキーを書かない (queueRowCanonical)
 *   - ページは 1 回の読みにつき NE_QUEUE_MAX_PAGES (10) まで。総件数 (前) がそれを超える = 読まない (完全でない・too_many_pages = 古い未決の商品を人が片付ける)
 *   - API の失敗 = failed (error)。始めた印 (running) は最初の API の前に commit = 途中で process が止まった回は running のまま = 完全でない
 *   - 照合 / PR-1 は complete の回だけを「完全」とみなす (それ以外 = waiting)
 * ■ 行は API の値を生の文字で 2 回とも残す (pass 1 / 2・欄ごとの列 = String(値)・null / 欠落 = NULL・raw_json = API が返した 1 行・row_hash)。古い回は 14 回だけ残す
 * ■ API の呼び出しの数 = 2 (count) + 2 × ページの数。ふつう 1 ページ = 1 日 4 回 (月 約 120 回)・最大 10 ページ = 1 日 22 回 (月 約 660 回)。今の約 240 回 / 月と合わせて最大 約 900 回 (上限 1,000 回)
 *
 * 🚨 このファイルは ne-api.js を読み込まない (callNE は引数で受け取る = 輪にしない)。db.js も読み込まない (照合の読み取り専用の接続からも使える)
 * 🚨 取得の版 (ne-fetch-counts.js の NE_FETCH_FINGERPRINT_FILES) に入っている
 */
import crypto from 'node:crypto';
import canonicalize from 'canonicalize';

export const NE_QUEUE_VERSION = 'q1';
export const NE_QUEUE_PAGE_LIMIT = 1000;
export const NE_QUEUE_MAX_PAGES = 10;
export const NE_QUEUE_KEEP_RUNS = 14;
export const NE_QUEUE_NONE_PENDING_MS = 86400000;   // 判定を待つ商品が無いときの期間 = 1 日
export const NE_QUEUE_TZ = 'Asia/Tokyo';
export const NE_QUEUE_METHOD = 'SYOHIN_KIHON_CSV';
/** 取る欄 (API の説明のフィールド名。https://developer.next-engine.com/api/api_v1_system_que/search/ 2026-10-06 に読んだ) */
export const NE_QUEUE_FIELDS = Object.freeze(['que_id', 'que_method_name', 'que_upload_name', 'que_client_file_name', 'que_file_name', 'que_status_id',
  'que_message', 'que_deleted_flag', 'que_creation_date', 'que_last_modified_date']);
export const NE_QUEUE_STATES = Object.freeze(['running', 'complete', 'incomplete', 'failed']);
/** 期間の始め (PR-1 の DB の関数・watcher が実行・読むだけ) */
export const WINDOW_START_SQL = 'select ops.ne_reg_queue_window_start() as window_start';

/** Date → JST の 'YYYY-MM-DD HH:MM:SS' (NE の時計の形) */
export function jstText(d) {
  return new Date(d.getTime() + 9 * 3600000).toISOString().replace('T', ' ').slice(0, 19);
}
const rawText = (v) => (v === undefined || v === null ? null : typeof v === 'object' ? JSON.stringify(v) : String(v));
const strictCount = (v) => (typeof v === 'number' ? (Number.isSafeInteger(v) && v >= 0 ? v : null) : typeof v === 'string' && /^\d{1,15}$/.test(v) ? Number(v) : null);
/**
 * 行の canonical JSON (RFC 8785 / JCS)。API が返した項目の名前をそのままキーに・値は生の文字 (数・真偽も文字)・null は null・欠けた項目は書かない。
 * object / 配列の値 (想定外) はそのまま JCS で書く。行が object でなければ JCS でその値
 */
export function queueRowCanonical(it) {
  if (!it || typeof it !== 'object' || Array.isArray(it)) return canonicalize(it ?? null);
  const o = {};
  for (const [k, v] of Object.entries(it)) {
    if (v === undefined) continue;
    o[k] = v === null || typeof v === 'object' ? v : String(v);
  }
  return canonicalize(o);
}
export const queueRowHash = (it) => crypto.createHash('sha256').update(queueRowCanonical(it), 'utf8').digest('hex');

/**
 * 期間の始めを DB から読む (watcher・読むだけ)。throw しない
 * @param {{ query?: (sql: string) => Promise<{ rows: any[] }>, url?: string }} o  query を渡せばそれを使う (試験) / url (既定 = env COMPANY_DB_WATCH_URL) で pg につなぐ
 * @returns {Promise<{ source: 'db'|'none_pending'|'unavailable', start: Date|null, reason?: string }>}
 */
export async function readQueueWindowStart({ query, url = (process.env.COMPANY_DB_WATCH_URL || '').trim() } = {}) {
  let close = null;
  try {
    if (!query) {
      if (!url) return { source: 'unavailable', start: null, reason: 'no_COMPANY_DB_WATCH_URL' };
      const { default: pg } = await import('pg');
      const { pgClientOptions } = await import('../../scripts/company-db/migrate.mjs');
      const base = pgClientOptions(url);
      const client = new pg.Client({ ...base, connectionString: base.connectionString, ssl: base.ssl, connectionTimeoutMillis: 15000 });
      await client.connect();
      close = () => client.end();
      await client.query("set statement_timeout = '10s'");
      await client.query('set default_transaction_read_only = on');
      query = (sql) => client.query(sql);
    }
    const rows = (await query(WINDOW_START_SQL)).rows;
    if (!Array.isArray(rows) || rows.length !== 1 || !('window_start' in rows[0])) return { source: 'unavailable', start: null, reason: 'unexpected_result' };
    const v = rows[0].window_start;
    if (v === null) return { source: 'none_pending', start: null };
    const start = v instanceof Date ? v : new Date(v);
    if (!Number.isFinite(start.getTime())) return { source: 'unavailable', start: null, reason: 'start_unreadable' };
    return { source: 'db', start };
  } catch (e) {
    return { source: 'unavailable', start: null, reason: String(e && e.message || e).slice(0, 200) };
  } finally {
    if (close) { try { await close(); } catch { /* */ } }
  }
}

/**
 * 2 回の読みと前後の総件数から完全かを決める (fetchUploadQueue の中と試験で使う)。返り値 = 崩れた点の一覧 (空 = 完全)
 * @param {{ countBefore: any, countAfter: any, passes: Array<Array<{ items: any, respCount: any }>>, pageLimit: number, fetchFingerprint: string|null }} x
 */
export function judgeQueueRun({ countBefore, countAfter, passes, pageLimit, fetchFingerprint }) {
  const p = [];
  const cb = strictCount(countBefore), ca = strictCount(countAfter);
  if (cb === null) p.push('count_before_unreadable');
  if (ca === null) p.push('count_after_unreadable');
  if (cb !== null && ca !== null && cb !== ca) p.push('count_changed');
  if (passes.length !== 2) p.push('not_two_passes');
  const seen = passes.map((pages, n) => {
    const tag = `pass${n + 1}`;
    if (!pages.length) p.push(`${tag}_no_page`);
    let rows = 0, dup = false;
    const hashes = new Map();
    pages.forEach((pg, i) => {
      if (!Array.isArray(pg.items)) { p.push(`${tag}_page_${i}_not_array`); return; }
      rows += pg.items.length;
      if (strictCount(pg.respCount) !== pg.items.length) p.push(`${tag}_page_${i}_count_ne_rows`);
      if (pg.items.length > pageLimit) p.push(`${tag}_page_${i}_over_limit`);
      if (i < pages.length - 1 && pg.items.length !== pageLimit) p.push(`${tag}_page_${i}_not_full`);
      if (i === pages.length - 1 && pg.items.length >= pageLimit) p.push(`${tag}_last_page_not_short`);
      for (const it of pg.items) {
        const id = it && typeof it === 'object' ? rawText(it.que_id) : null;
        if (id === null || id === '') { p.push(`${tag}_row_without_que_id`); continue; }
        if (hashes.has(id)) dup = true;
        hashes.set(id, queueRowHash(it));
      }
    });
    if (cb !== null && rows !== cb) p.push(`${tag}_rows_ne_count`);
    if (dup) p.push(`${tag}_que_id_not_distinct`);
    return hashes;
  });
  if (seen.length === 2) {
    const [a, b] = seen;
    const ids = new Set([...a.keys(), ...b.keys()]);
    if ([...ids].some((id) => !a.has(id) || !b.has(id))) p.push('que_ids_differ');
    else if ([...ids].some((id) => a.get(id) !== b.get(id))) p.push('row_hash_differs');
  }
  if (typeof fetchFingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(fetchFingerprint)) p.push('no_fetch_fingerprint');
  return [...new Set(p)];
}

/**
 * アップロードキューを取る。**throw しない** (失敗は failed の回として残し、結果を返す = 商品・受注の取得を止めない)
 * @param {{ db: import('better-sqlite3').Database, callNE: Function, fetchFingerprint: string|null,
 *   windowStart: { source: string, start: Date|null, reason?: string }, products?: { complete_at: string|null, complete_rev: string|null, finished_at: string|null },
 *   now?: () => Date, pageLimit?: number, maxPages?: number }} o
 * @returns {Promise<{ run_id: string, state: string, problems: string[], rows: number, error?: string }>}
 */
export async function fetchUploadQueue({ db, callNE, fetchFingerprint, windowStart, products = { complete_at: null, complete_rev: null, finished_at: null },
  now = () => new Date(), pageLimit = NE_QUEUE_PAGE_LIMIT, maxPages = NE_QUEUE_MAX_PAGES }) {
  const started = now();
  const runId = crypto.randomUUID();
  const ws = windowStart || { source: 'unavailable', start: null, reason: 'not_given' };
  const startOk = ws.source === 'db' && ws.start instanceof Date && Number.isFinite(ws.start.getTime());
  const fromDate = startOk ? ws.start : ws.source === 'none_pending' ? new Date(started.getTime() - NE_QUEUE_NONE_PENDING_MS) : null;
  const windowTo = jstText(started), windowFrom = fromDate ? jstText(fromDate) : null;
  const insertRun = (state, extra = {}) => db.prepare(`INSERT INTO ne_upload_queue_runs (run_id, version, started_at, finished_at, window_from, window_to, window_tz, method, page_limit, max_pages,
      fetch_fingerprint, state, problems, window_source, window_reason, products_complete_at, products_complete_rev, products_finished_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(runId, NE_QUEUE_VERSION, started.toISOString(), extra.finished_at ?? null, windowFrom, windowTo, NE_QUEUE_TZ, NE_QUEUE_METHOD,
    pageLimit, maxPages, fetchFingerprint ?? null, state, extra.problems ? JSON.stringify(extra.problems) : null, String(ws.source), ws.reason ?? null,
    products?.complete_at ?? null, products?.complete_rev == null ? null : String(products.complete_rev), products?.finished_at ?? null);
  // 期間の始めを読めない = キューを読まない (完全でない)
  if (!windowFrom) {
    db.transaction(() => { insertRun('incomplete', { finished_at: now().toISOString(), problems: ['window_start_unavailable'] }); pruneQueueRuns(db, runId); })();
    return { run_id: runId, state: 'incomplete', problems: ['window_start_unavailable'], rows: 0 };
  }
  // 始めた印 (running) を先に commit する = 途中で process が止まった回も「完全でない」と分かる
  insertRun('running');
  const cond = { 'que_creation_date-gte': windowFrom, 'que_creation_date-lte': windowTo, 'que_method_name-eq': NE_QUEUE_METHOD };
  try {
    const countBefore = (await callNE('/api_v1_system_que/count', cond)).count;
    const cb = strictCount(countBefore);
    let passes = [], countAfter = null, problems;
    if (cb !== null && cb > maxPages * pageLimit) {
      problems = ['too_many_pages'];   // 読まない (月の呼び出しの上限)。古い未決の商品を人が片付ける
    } else {
      for (let n = 0; n < 2; n++) {
        const pages = [];
        let offset = 0;
        while (true) {
          const data = await callNE('/api_v1_system_que/search', { fields: NE_QUEUE_FIELDS.join(','), limit: String(pageLimit), offset: String(offset), ...cond });
          pages.push({ items: data.data, respCount: data.count });
          if (!Array.isArray(data.data) || data.data.length < pageLimit) break;
          if (pages.length >= maxPages) { pages.overflow = true; break; }
          offset += pageLimit;
        }
        passes.push(pages);
      }
      countAfter = (await callNE('/api_v1_system_que/count', cond)).count;
      problems = judgeQueueRun({ countBefore, countAfter, passes, pageLimit, fetchFingerprint });
      if (passes.some((ps) => ps.overflow)) problems.push('too_many_pages');
    }
    const state = problems.length ? 'incomplete' : 'complete';
    const rowsOf = (pages) => pages.flatMap((pg) => (Array.isArray(pg.items) ? pg.items : []));
    const last = passes.length === 2 ? rowsOf(passes[1]) : [];
    db.transaction(() => {
      const ins = db.prepare(`INSERT INTO raw_ne_upload_queue (run_id, pass, row_no, ${NE_QUEUE_FIELDS.join(', ')}, raw_json, row_hash)
        VALUES (?, ?, ?, ${NE_QUEUE_FIELDS.map(() => '?').join(', ')}, ?, ?)`);
      passes.forEach((pages, n) => rowsOf(pages).forEach((it, i) => {
        const o = it && typeof it === 'object' ? it : {};
        ins.run(runId, n + 1, i, ...NE_QUEUE_FIELDS.map((f) => rawText(o[f])), JSON.stringify(it ?? null), queueRowHash(it));
      }));
      db.prepare(`UPDATE ne_upload_queue_runs SET finished_at = ?, page_rows = ?, rows_read = ?, distinct_que_ids = ?,
        api_count_before = ?, api_count_after = ?, state = ?, problems = ? WHERE run_id = ? AND state = 'running'`)
        .run(now().toISOString(), JSON.stringify(passes.map((pages) => pages.map((pg) => (Array.isArray(pg.items) ? pg.items.length : null)))),
          last.length, new Set(last.map((it) => rawText(it?.que_id)).filter((v) => v !== null && v !== '')).size,
          rawText(countBefore), rawText(countAfter), state, JSON.stringify(problems), runId);
      pruneQueueRuns(db, runId);
    })();
    return { run_id: runId, state, problems, rows: last.length };
  } catch (e) {
    const msg = String(e && e.message || e).slice(0, 500);
    try {
      db.prepare("UPDATE ne_upload_queue_runs SET finished_at = ?, state = 'failed', error = ? WHERE run_id = ? AND state = 'running'").run(now().toISOString(), msg, runId);
    } catch { /* 記録できなくても running のまま = 完全でない */ }
    return { run_id: runId, state: 'failed', problems: [], rows: 0, error: msg };
  }
}

/** 新しい NE_QUEUE_KEEP_RUNS 回 (と今回) だけ残す */
function pruneQueueRuns(db, keepRunId) {
  const keep = [...new Set([keepRunId, ...db.prepare('SELECT run_id FROM ne_upload_queue_runs ORDER BY started_at DESC, run_id DESC LIMIT ?').all(NE_QUEUE_KEEP_RUNS).map((r) => r.run_id)])];
  const ph = keep.map(() => '?').join(', ');
  db.prepare(`DELETE FROM raw_ne_upload_queue WHERE run_id NOT IN (${ph})`).run(...keep);
  db.prepare(`DELETE FROM ne_upload_queue_runs WHERE run_id NOT IN (${ph})`).run(...keep);
}

/**
 * 照合 ② ((Y)) 用: **呼び手が開いた読み取りの取引の中で** 最新の回 (始めた時刻が一番新しい・状態は問わない) とその行 (2 回目の読み) を読む。
 * complete = その回が完全か (state = complete・版・行の数が記録と同じ)。完全でない回では、照合 / PR-1 は「キューに無い」を決めない (waiting)。
 * 期間が判定の対象を覆うか・期間の終わり (started_at) ≥ products_complete_at か は PR-1 の DB が run の値で確かめる
 * @param {import('better-sqlite3').Database} db
 * @returns {{ run: object|null, rows: object[], complete: boolean, reason?: string }}
 */
export function readLatestUploadQueueRun(db) {
  const has = (t) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(t);
  if (!has('ne_upload_queue_runs') || !has('raw_ne_upload_queue')) return { run: null, rows: [], complete: false, reason: 'no_table' };
  const run = db.prepare('SELECT * FROM ne_upload_queue_runs ORDER BY started_at DESC, run_id DESC LIMIT 1').get() ?? null;
  if (!run) return { run: null, rows: [], complete: false, reason: 'no_run' };
  const rows = db.prepare('SELECT * FROM raw_ne_upload_queue WHERE run_id = ? AND pass = 2 ORDER BY row_no').all(run.run_id);
  if (run.state !== 'complete') return { run, rows, complete: false, reason: run.state };
  if (rows.length !== run.rows_read || run.version !== NE_QUEUE_VERSION) return { run, rows, complete: false, reason: 'record_mismatch' };
  return { run, rows, complete: true };
}
