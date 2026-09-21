#!/usr/bin/env node
/**
 * push/stock-daily.mjs — miniPC の「その日の在庫 (SKU 単位)」を Company DB (Render Postgres) へ送る (Company DB構想 08 §3.3 の ③ NE = D2b-1 / ④ FBA = D2b-2)。
 *
 *   node apps/company-db/push/stock-daily.mjs --source ne --days 14            ← daily-sync (NE 在庫スナップショットの直後)。直近 14 日で「まだ Render に無い日」だけ送る
 *   node apps/company-db/push/stock-daily.mjs --source fba_jp --days 14        ← daily-sync (FBA 在庫スナップショットの直後)。fba_us も同じ
 *   node apps/company-db/push/stock-daily.mjs --source ne --all                ← 初回: 元データの最初の日から今日まで (1 日 1 要求)
 *   node apps/company-db/push/stock-daily.mjs --source fba_jp --from 2026-09-01 --to 2026-09-20 [--dry-run]
 *
 * 決め:
 *   - 🚨 **台帳を持たない**: どの日を送り済みかは Render に聞く (GET …/stock-daily/status)。進み具合を miniPC の側に持つと、Render を復元したとき・台帳を失くしたときに食い違う
 *   - mirror (Render 同期) は経由しない (D5a と同じ判断: 受け口が冪等を担う。Render 同期の荷物を増やさない = 9/15 の 413 の再発を避ける)
 *   - 1 日 = 1 要求 = Render 側で 1 取引 (building → 行 → complete / partial)。先に確定した日は書き換えない: 確定済みの日は送らず、内容の指紋だけ比べて、違えば ⚠️ に出す
 *   - 元データに行の無い日: 過去の日 → missing と申告 (取れなかった日を「在庫 0」と読ませない) / 今日 → 失敗 (朝のスナップショットが先に要る。fba_us だけは失敗にしない = US の取得は失敗しても朝のステップは成功)
 *   - 行の検証は受け口と同じ規則・同じ関数。1 行でも外れたら、その日は送らず ❌ (部分的な日を作らない)
 *   - 🚨 送る内容 (日付の形の検査・取得時刻・在庫行) は 1 つの読み取り取引で確定してから送る (readWindow)
 * NE  = warehouse.db の ne_stock_daily_snapshot (business_date, 商品コード, 在庫数, captured_at)
 * FBA = fba.db の daily_snapshots / daily_snapshots_us (snapshot_date, amazon_sku, 7 区分)。
 *   - 🚨 fba.db は sql.js (ファイル全体を書き戻す)。常駐サーバが保存している最中のファイルを読まないよう、読むあいだ db.js と同じ lock を取る (apps/fba-replenishment/file-lock.js)
 *   - 🚨 **daily_snapshots をそのまま送らない** (Codex #1388 R1): RESTOCK と PLANNING を混ぜた表で、RESTOCK に無い SKU の FC 移管中・処理中・出荷待ち が 0 で入り (「取れなかった」と「0」が区別できない)、
 *     同じ日の取り直しで値が変わり、行がいつの取得か残らない。→ 朝のスナップショットが取得した行そのものから作る **送る版** (fba.db の cdb_stock_export / cdb_stock_export_days) を読む:
 *     RESTOCK に無い SKU の 3 区分は null・RESTOCK が丸ごと無い日は partial・版の値と取得時刻は同じ回のもの
 *   - 版の無い過去の日 (この仕組みの前・30 日より前): **推定しない**。daily_snapshots の available と入庫の 3 つだけを読み、3 区分は null・partial・取得時刻はその日の朝の定刻 (07:30 JST) + captured_at_nominal
 * 在庫数だけ (個人情報なし)。env: DATA_DIR / RENDER_MIRROR_URL (RENDER_PORTAL_URL) / MIRROR_SYNC_KEY / WAREHOUSE_BUSINESS_DATE (daily-sync が JST で確定)
 * 最後の 1 行が daily-sync の朝の通知に載る
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { postJson, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { syncBase } from './ne-shipments.mjs';
import { checksumOf, isRealDate, jstDate, strictInstant, isValidCode, fbaRowOf, MAX_ROWS, FBA_COLS, RESTOCK_COLS } from '../ingest/stock-daily.mjs';
import { withSqliteFileLock, lockDbFileOf } from '../../fba-replenishment/file-lock.js';

export const DEFAULT_DAYS = 14;
export const MAX_RANGE_DAYS = 800;   // 受け口の status の上限と同じ
export const MAX_BODY_BYTES = 3.5 * 1024 * 1024;   // 受け口の parser は 4MB。超える日は送る前に失敗にする (dry-run でも分かるように)
const REMOTE_STATUSES = ['complete', 'partial', 'missing'];   // building は 1 取引の中だけ = 外から見えたら異常
export const WINDOW_DAYS = 31;   // 1 回の読み取り取引で確定する日数
export const FBA_NOMINAL_TIME = 'T07:30:00+09:00';   // 取得時刻の記録が無い過去の日に入れる、朝のスナップショットの定刻
export const FBA_LOCK_WAIT_MS = 15000;   // 常駐サーバの保存 (100MB で 1 秒弱) を待つ上限。送り手は専用のプロセスなので、待っても誰も止めない

const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
export function datesBetween(from, to) { const out = []; for (let d = from; d <= to; d = addDays(d, 1)) out.push(d); return out; }

/** NE のその日の行を受け口と同じ規則で検証して返す。外れた行があれば errors に入る */
export function rowsOfDay(rawRows) {
  const rows = [], errors = [], seen = new Set();
  for (const r of rawRows) {
    const code = r.code, qty = r.qty;
    if (!isValidCode(code)) { errors.push(`商品コードが不正: ${JSON.stringify(String(code)).slice(0, 60)}`); continue; }
    if (seen.has(code)) { errors.push(`商品コードが重複: ${code.slice(0, 60)}`); continue; }
    seen.add(code);
    if (!Number.isInteger(qty) || qty < 0 || qty > 2147483647) { errors.push(`在庫数が 0 以上の整数でない: ${code.slice(0, 40)} = ${String(qty).slice(0, 20)}`); continue; }
    rows.push({ code, qty });
  }
  return { rows, errors };
}

/** 取得時刻の一覧 → { capturedAt | null, capturedError | null }。行ごとに検証して UTC にそろえ、1 日に 1 つだけ (max() は不正な時刻の行を隠す。Codex #1383 R2 #1) */
function oneInstant(raws) {
  const bad = raws.filter((c) => strictInstant(c) === null);
  if (bad.length) return { capturedAt: null, capturedError: `captured_at が読めない行がある (実在する日時で Z か ±HH:MM つき・未来は不可): ${JSON.stringify(bad[0]).slice(0, 44)}` };
  const instants = [...new Set(raws.map((c) => strictInstant(c)))];
  if (instants.length !== 1) return { capturedAt: null, capturedError: `取得時刻が ${instants.length} つある (1 日 = 1 回の取得のはず。取り直しの途中かもしれない): ${instants.slice(0, 2).join(' / ')}` };
  return { capturedAt: instants[0], capturedError: null };
}

const hasExportTables = (db) => !!db.prepare(`select 1 as x from sqlite_master where type = 'table' and name = 'cdb_stock_export_days'`).get();   // 常駐サーバが古い版のままだと、まだ無い
const PLAIN_FBA_COLS = FBA_COLS.filter((c) => !RESTOCK_COLS.includes(c));   // available と入庫の 3 つ = RESTOCK でも PLANNING でも取れる

/** FBA のその日を読む (table = daily_snapshots | daily_snapshots_us、market = jp | us)。戻り値は readWindow の 1 日ぶん */
function readFbaDay(db, d, { table, market }) {
  const day = hasExportTables(db) ? db.prepare(`select captured_at, restock_rows from cdb_stock_export_days where snapshot_date = ? and market = ?`).get(d, market) : null;
  // 版がある日: 版の行をそのまま (3 区分の null = RESTOCK に載っていない SKU)。RESTOCK が丸ごと無い版は partial。
  // 版の無い過去の日: 推定しない = available と入庫だけを読み、3 区分は null で partial
  const partial = day ? !(Number(day.restock_rows) > 0) : true;
  const raw = day
    ? db.prepare(`select amazon_sku as code, ${FBA_COLS.join(', ')} from cdb_stock_export where snapshot_date = ? and market = ? order by amazon_sku`).all(d, market)
    : db.prepare(`select amazon_sku as code, ${PLAIN_FBA_COLS.join(', ')} from ${table} where snapshot_date = ? order by amazon_sku`).all(d);
  const rows = [], errors = [], seen = new Set();
  for (const r of raw) {
    try {
      const row = fbaRowOf(r, partial);   // 受け口と同じ検証 (値を null に置き換えてから検証しない = 不正な元の値を隠さない。Codex #1388 R1 #4)
      if (seen.has(row.code)) throw new Error('出品 SKU が重複');
      seen.add(row.code);
      const { qty, ...send } = row;   // qty は受け口が同じ式で出す (送らない)
      rows.push(send);
    } catch (e) { errors.push(`${JSON.stringify(String(r.code)).slice(0, 50)}: ${e.message}`); }
  }
  if (day && !partial && rows.length > 0 && !rows.some((x) => x.fba_fc_transfer != null)) errors.push('RESTOCK が取れた版なのに、3 区分の入った行が 1 つも無い');
  const cap = oneInstant([day ? day.captured_at : `${d}${FBA_NOMINAL_TIME}`]);
  return { rows, errors, ...cap, partial, nominal: !day, basis: day ? 'export' : 'history' };
}

/** FBA の日付の一覧: daily_snapshots の日付 ∪ 送る版の日付 */
const fbaDates = (db, table, market, lo = null, hi = null) => {
  const range = lo === null ? '' : ' and snapshot_date between ? and ?';
  const args = lo === null ? [] : [lo, hi];
  const a = db.prepare(`select distinct snapshot_date as d from ${table} where 1 = 1${range}`).all(...args).map((r) => r.d);
  const b = hasExportTables(db) ? db.prepare(`select snapshot_date as d from cdb_stock_export_days where market = ?${range}`).all(market, ...args).map((r) => r.d) : [];
  return [...new Set([...a, ...b])].sort();
};

/** source → 元データの読み方 */
export const SOURCES = {
  ne: {
    label: 'NE', scope: 'main', dbFile: 'warehouse.db', lock: false, todayRequired: true,
    // 🚨 範囲で絞る **前** に、元データの日付の形を全部確かめる (文字列の between は '2026-09-19T00:00:00' のような形の違う日付を黙って範囲の外に落とす
    //    → その日を missing と申告したり、形の合う行だけで complete にしてしまう。Codex #1383 R1 #1)
    allDates: (db) => db.prepare(`select distinct business_date as d from ne_stock_daily_snapshot`).all().map((r) => r.d),
    datesIn: (db, lo, hi) => db.prepare(`select distinct business_date as d from ne_stock_daily_snapshot where business_date between ? and ? order by 1`).all(lo, hi).map((r) => r.d),
    readDay: (db, d) => ({
      ...rowsOfDay(db.prepare(`select 商品コード as code, 在庫数 as qty from ne_stock_daily_snapshot where business_date = ? order by 商品コード`).all(d)),
      ...oneInstant(db.prepare(`select distinct captured_at as c from ne_stock_daily_snapshot where business_date = ?`).all(d).map((x) => x.c)),
      partial: false, nominal: false, basis: 'recorded',
    }),
  },
  fba_jp: {
    label: 'FBA', scope: 'jp', dbFile: 'fba.db', lock: true, todayRequired: true,
    allDates: (db) => fbaDates(db, 'daily_snapshots', 'jp'),
    datesIn: (db, lo, hi) => fbaDates(db, 'daily_snapshots', 'jp', lo, hi),
    readDay: (db, d) => readFbaDay(db, d, { table: 'daily_snapshots', market: 'jp' }),
  },
  fba_us: {
    label: 'FBA US', scope: 'us', dbFile: 'fba.db', lock: true, todayRequired: false,   // US の取得は失敗しても朝のスナップショットは成功 = 今日の行が無いだけでは失敗にしない
    allDates: (db) => fbaDates(db, 'daily_snapshots_us', 'us'),
    datesIn: (db, lo, hi) => fbaDates(db, 'daily_snapshots_us', 'us', lo, hi),
    readDay: (db, d) => readFbaDay(db, d, { table: 'daily_snapshots_us', market: 'us' }),
  },
};
const specOf = (source) => (typeof source === 'string' && Object.hasOwn(SOURCES, source) ? SOURCES[source] : null);

/** 元データの日付の形を全部確かめる (範囲で絞る前に)。読めない日付が 1 種類でもあれば例外 */
export function assertDatesReadable(db, spec) {
  const badDates = spec.allDates(db).filter((d) => !isRealDate(d));
  if (badDates.length) throw new Error(`元データに読めない日付が ${badDates.length} 種類ある (例: ${JSON.stringify(badDates[0]).slice(0, 40)})。範囲の判定が信用できないので、どの日も送らない`);
}

/**
 * 期間ぶんの「送る内容」を **1 つの読み取り取引** で確定する (Codex #1383 R2 #2): 日付の形の検査・その日の取得時刻・在庫行 を同じ瞬間の元データから取る。
 * 別々に読むと、Render の応答を待つ間にスナップショットが取り直されたとき「08:01 の在庫数を 07:01 の取得時刻で送る」ことができてしまう
 * (内容の指紋は取得時刻を含まない = 確定した後は直らない)。HTTP を待つ間は取引も lock も持たない = ここで読んだ内容をそのまま送る。
 * 戻り値 = Map(日付 → { rows, errors, capturedAt | null, capturedError | null, partial, nominal, basis })
 * guard = 読むあいだを包むもの (FBA は fba.db の file lock)
 */
export function readWindow(db, spec, lo, hi, guard = (fn) => fn()) {
  const read = () => {
    assertDatesReadable(db, spec);   // 送る内容と同じ取引の中でも確かめる
    const days = new Map();
    for (const d of spec.datesIn(db, lo, hi)) days.set(d, spec.readDay(db, d));
    return days;
  };
  // better-sqlite3: transaction() = BEGIN 〜 COMMIT (読むだけでも同じ断面になる)。差し替えの db に transaction が無ければそのまま読む (試験用の簡易なもの)
  return guard(() => (typeof db.transaction === 'function' ? db.transaction(read)() : read()));
}

export function parseArgs(argv) {
  const out = { source: null, days: null, from: null, to: null, all: false, dryRun: false, dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => { const v = argv[++i]; if (v === undefined || v.startsWith('--')) throw new Error(`${a} に値が無い`); return v; };
    if (a === '--source') out.source = val();
    else if (a === '--days') out.days = val();
    else if (a === '--from') out.from = val();
    else if (a === '--to') out.to = val();
    else if (a === '--all') out.all = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--data-dir') out.dataDir = val();
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!specOf(out.source)) throw new Error(`--source は ${Object.keys(SOURCES).join(' / ')}`);
  const modes = [out.days !== null, out.from !== null || out.to !== null, out.all].filter(Boolean).length;
  if (modes > 1) throw new Error('--days / --from --to / --all はどれか 1 つ');
  if (out.days !== null) { if (!/^[1-9][0-9]{0,2}$/.test(out.days)) throw new Error(`--days は 1〜999 の整数: ${out.days}`); out.days = Number(out.days); }
  if ((out.from !== null) !== (out.to !== null)) throw new Error('--from と --to は両方付ける');
  if (out.from !== null && (!isRealDate(out.from) || !isRealDate(out.to) || out.from > out.to)) throw new Error('--from / --to は実在する YYYY-MM-DD で from <= to');
  return out;
}

/**
 * 本体。戻り値 = { ok, source, from, to, sent: [...], same, done, mismatched: [...], missingDeclared: [...], missingKept, failed: [...], partialDays, upgraded, nominalDays, todayAbsent, dryRun, lastLine }
 * 差し替え (試験): warehouse (better-sqlite3 互換の元データの DB。NE = warehouse.db / FBA = fba.db) / fetchImpl / today / guard
 */
export async function pushStockDaily({ source, warehouse, fetchImpl = fetch, base, syncKey, today, days = null, from = null, to = null, all = false, dryRun = false, log = console.log, sleep, guard }) {
  const spec = specOf(source); if (!spec) throw new Error(`知らない source: ${source}`);
  if (!base) throw new Error('Render の宛先が無い (RENDER_MIRROR_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  if (!isRealDate(today)) throw new Error(`today が不正: ${today}`);
  const g = guard || ((fn) => fn());
  let lo, hi = today;
  if (all) {
    const ds = g(() => spec.allDates(warehouse));
    if (ds.length === 0) throw new Error(`元データが空 (${spec.label})`);
    const bad = ds.filter((d) => !isRealDate(d));
    if (bad.length) throw new Error(`元データに読めない日付が ${bad.length} 種類ある (例: ${JSON.stringify(bad[0]).slice(0, 40)})。範囲の判定が信用できないので、どの日も送らない`);
    lo = ds.reduce((a, b) => (a < b ? a : b));
  } else if (from !== null) { lo = from; hi = to; if (hi > today) throw new Error(`--to が未来 (今日 = ${today}): ${hi}`); }
  else lo = addDays(today, -((days ?? DEFAULT_DAYS) - 1));
  if (datesBetween(lo, hi).length > MAX_RANGE_DAYS) throw new Error(`範囲が長すぎる (${MAX_RANGE_DAYS} 日まで): ${lo}〜${hi}`);

  g(() => assertDatesReadable(warehouse, spec));   // Render に聞く前に、元データの日付の形だけ先に確かめる (読めない日付があれば要求もしない。在庫行までは読まない)
  const sres = await fetchImpl(`${base}/stock-daily/status?source=${encodeURIComponent(source)}&scope=${encodeURIComponent(spec.scope)}&from=${lo}&to=${hi}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!sres.ok) throw new Error(`Render の状態が取れない: HTTP ${sres.status}${sres.status === 404 ? ' (Render がまだ新しい版になっていない?)' : ''} ${(await sres.text()).replace(/\s+/g, ' ').slice(0, 160)}`);
  const sj = await sres.json();
  if (!sj || !Array.isArray(sj.days)) throw new Error('Render の状態の応答に days が無い');
  // 応答は 1 件ずつ確かめる (形の分からない行を「確定済み」と読まない。Codex #1383 R1 #3)
  const remote = new Map();
  for (const x of sj.days) {
    if (!x || !isRealDate(x.snapshot_date) || x.snapshot_date < lo || x.snapshot_date > hi) throw new Error(`Render の状態の応答に、読めない・範囲の外の日付がある: ${JSON.stringify(x).slice(0, 120)}`);
    if (remote.has(x.snapshot_date)) throw new Error(`Render の状態の応答に同じ日が 2 つある: ${x.snapshot_date}`);
    if (!REMOTE_STATUSES.includes(x.status)) throw new Error(`Render の状態の応答に分からない状態がある: ${x.snapshot_date} = ${String(x.status).slice(0, 30)}`);
    if ((x.status === 'complete' || x.status === 'partial') && (typeof x.checksum !== 'string' || !/^[0-9a-f]{64}$/.test(x.checksum))) throw new Error(`Render の状態の応答で、${x.status} の日に内容の指紋が無い: ${x.snapshot_date}`);
    remote.set(x.snapshot_date, x);
  }

  const out = { ok: true, source, from: lo, to: hi, sent: [], same: 0, done: 0, mismatched: [], missingDeclared: [], missingKept: 0, failed: [], dryRun, unresolved: 0, rowsSent: 0, partialDays: 0, upgraded: 0, nominalDays: 0, todayAbsent: false };
  let local = new Map(), windowEnd = null;
  for (const d of datesBetween(lo, hi)) {
    // 31 日ずつ、送る内容を 1 つの読み取り取引で確定してから送る (HTTP を待つ間に元データが変わっても、読んだ内容のまま送る)
    if (windowEnd === null || d > windowEnd) { windowEnd = addDays(d, WINDOW_DAYS - 1) > hi ? hi : addDays(d, WINDOW_DAYS - 1); local = readWindow(warehouse, spec, d, windowEnd, g); }
    const l = local.get(d), r = remote.get(d);
    const settled = !!r && r.status !== 'missing';
    try {
      if (!l) {
        if (settled) { out.done++; continue; }                       // 元データには無いが Render には確定済み (元の履歴を消した後など) → 触らない
        if (d >= today) {
          if (!spec.todayRequired) { out.todayAbsent = true; continue; }   // 今日の行がまだ無い (US)。申告も失敗もしない = 明日以降に決まる
          throw new Error(`今日 (${d}) の元データが無い (朝の在庫スナップショットが先に要る)`);
        }
        if (r) { out.missingKept++; continue; }                      // もう missing と申告済み
        if (!dryRun) {
          const mres = await postJson(fetchImpl, { base, syncKey, path: '/stock-daily', log, sleep, body: { source, scope: spec.scope, snapshot_date: d, missing: true } });
          if (!mres || (mres.status !== 'missing' && mres.status !== 'missing_same')) throw new Error(`missing の申告への応答が分からない: ${JSON.stringify(mres).slice(0, 160)}`);
        }
        out.missingDeclared.push(d);
        continue;
      }
      const { rows, errors } = l;
      if (errors.length) throw new Error(`検証に通らない行が ${errors.length} 件 (例: ${errors[0]}) → この日は送らない`);
      if (rows.length === 0) throw new Error('行が 0 件');
      if (rows.length > MAX_ROWS) throw new Error(`行が多すぎる (${rows.length} > 受け口の上限 ${MAX_ROWS})`);
      const upgrade = settled && r.status === 'partial' && !l.partial;   // Render には一部だけの日・手元は全部取れている (後から RESTOCK が取れた) → 送って上げる
      if (settled && !upgrade) {
        out.done++;
        // 先に確定した日は書き換えない。違いだけ知らせる (指紋の有無は上で確かめた)。Render が complete で手元が partial、も指紋が違う = 「違う」
        if (r.checksum !== checksumOf(source, rows, l.partial)) out.mismatched.push(d);
        continue;
      }
      if (l.capturedError) throw new Error(l.capturedError);   // 受け口と同じ検証を、その日の行の全部に (タイムゾーンの無い日時・実在しない日時・未来・複数の時刻 は送らない)
      const payload = { source, scope: spec.scope, snapshot_date: d, captured_at: l.capturedAt, rows };
      if (l.partial) payload.partial = true;
      if (l.nominal) payload.captured_at_nominal = true;
      const bytes = Buffer.byteLength(JSON.stringify(payload));
      if (bytes > MAX_BODY_BYTES) throw new Error(`1 日ぶんが大きすぎる (${bytes} バイト > ${MAX_BODY_BYTES})`);
      if (dryRun) { out.sent.push({ date: d, rows: rows.length, status: 'dry-run', partial: l.partial }); out.rowsSent += rows.length; if (l.partial) out.partialDays++; if (l.nominal) out.nominalDays++; if (upgrade) out.upgraded++; continue; }
      const res = await postJson(fetchImpl, { base, syncKey, path: '/stock-daily', log, sleep, body: payload });
      if (!res || (res.status !== 'applied' && res.status !== 'same')) throw new Error(`受け口の応答が分からない: ${JSON.stringify(res).slice(0, 160)}`);
      if (res.status === 'same') { out.same++; continue; }
      if (res.rows !== rows.length) throw new Error(`受け口が入れた行数 ${res.rows} が、送った行数 ${rows.length} と違う`);
      // day_status は新しい受け口だけが返す (Render が古い版のあいだに走った回を、保存できているのに失敗にしない)
      if (res.day_status !== undefined && res.day_status !== (l.partial ? 'partial' : 'complete')) throw new Error(`受け口が付けた日の状態 ${res.day_status} が、送った内容 (${l.partial ? 'partial' : 'complete'}) と違う`);
      if (l.partial) out.partialDays++;
      if (l.nominal) out.nominalDays++;
      if (upgrade) out.upgraded++;
      out.sent.push({ date: d, rows: res.rows, status: res.status, unresolved: res.unresolved, partial: l.partial });
      out.rowsSent += res.rows; out.unresolved += Number(res.unresolved || 0);
      log(`[company-db stock-daily ${source}] ${d}: ${res.rows} 行${l.partial ? ' (partial = RESTOCK の 3 区分は不明)' : ''}${upgrade ? ' (partial から上げた)' : ''} (SKU が分からない ${res.unresolved})`);
    } catch (e) {
      out.ok = false;
      out.failed.push({ date: d, error: String(e.message).replace(/\s+/g, ' ').slice(0, 200) });
      log(`[company-db stock-daily ${source}] ${d}: ❌ ${e.message}`);
    }
  }
  const parts = [`${dryRun ? '送る予定' : '送った'} ${out.sent.length} 日 (${out.rowsSent} 行${dryRun ? '' : ` / SKU が分からない ${out.unresolved}`})`, `確定済み ${out.done + out.same} 日`];
  if (out.partialDays) parts.push(`うち一部だけ取れた日 (partial) ${out.partialDays}`);
  if (out.upgraded) parts.push(`partial から上げた ${out.upgraded} 日`);
  if (out.nominalDays) parts.push(`取得時刻の記録が無く定刻を入れた ${out.nominalDays} 日`);
  if (out.missingDeclared.length) parts.push(`取れていない日を申告 ${out.missingDeclared.length} 日 (${out.missingDeclared.slice(0, 3).join(', ')}${out.missingDeclared.length > 3 ? ' ほか' : ''})`);
  if (out.todayAbsent) parts.push('今日の行はまだ無い (失敗にしない)');
  if (out.mismatched.length) parts.push(`⚠️ 確定済みと内容が違う日 ${out.mismatched.length} (${out.mismatched.slice(0, 3).join(', ')}。書き換えない)`);
  if (out.failed.length) parts.push(`失敗 ${out.failed.length} 日 (${out.failed.slice(0, 2).map((f) => `${f.date}: ${f.error}`).join(' / ')})`);
  out.lastLine = `${out.failed.length ? '❌' : out.mismatched.length ? '⚠️' : '✅'} Company DB 在庫日次 (${spec.label}) ${lo}〜${hi}: ${parts.join(' / ')}${dryRun ? ' [dry-run = 送っていない]' : ''}`;
  return out;
}

/**
 * 元データの DB を開く。FBA (lock = true) は「読むあいだだけ」fba.db の file lock を取り、接続もそのあいだだけ開く
 * (常駐サーバが保存している最中のファイルを読まない・lock の外で古い断面を持ち続けない)。戻り値 = { handle, guard, close }
 */
export function openSource(spec, file, { lockWaitMs = FBA_LOCK_WAIT_MS } = {}) {
  if (!spec.lock) {
    const db = new Database(file, { readonly: true, fileMustExist: true });
    return { handle: db, guard: undefined, close: () => db.close() };
  }
  let cur = null;
  const guard = (fn) => withSqliteFileLock(lockDbFileOf(file), lockWaitMs, () => {
    cur = new Database(file, { readonly: true, fileMustExist: true });
    try { return fn(); } finally { cur.close(); cur = null; }
  });
  const need = () => { if (!cur) throw new Error('fba.db は lock の中でだけ読む'); return cur; };
  const handle = { prepare: (sql) => need().prepare(sql), transaction: (fn) => need().transaction(fn) };
  return { handle, guard, close: () => {} };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const a = parseArgs(process.argv.slice(2));
    const spec = specOf(a.source);
    const dataDir = a.dataDir || process.env.DATA_DIR || path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..', 'data');
    const file = path.join(dataDir, spec.dbFile);
    if (!fs.existsSync(file)) throw new Error(`${spec.dbFile} が無い: ${file} (--data-dir か DATA_DIR)`);
    const today = process.env.WAREHOUSE_BUSINESS_DATE || jstDate(new Date());
    const src = openSource(spec, file);
    let r;
    try { r = await pushStockDaily({ source: a.source, warehouse: src.handle, base: syncBase(), syncKey: process.env.MIRROR_SYNC_KEY || '', today, days: a.days, from: a.from, to: a.to, all: a.all, dryRun: a.dryRun, guard: src.guard }); }
    finally { src.close(); }
    console.log(String(r.lastLine).replace(/\s+/g, ' '));   // 最後の 1 行を複数行にしない
    code = r.ok ? 0 : 1;
  } catch (e) {
    console.log(`❌ Company DB 在庫日次: ${String(e.message).replace(/\s+/g, ' ').slice(0, 400)}`);   // 最後の 1 行 (daily-sync が要約に使う) を複数行にしない (404 の HTML など)
  }
  // 🚨 fetch の直後に process.exit() しない: Windows の Node では libuv の assertion (`!(handle->flags & UV_HANDLE_CLOSING)`) で異常終了し、終了コードが 127 になる
  //    (2026-09-20 に本番の dry-run と手元で再現)。ほかの送り手 (mall-orders / ne-shipments) と同じく exitCode を置いて自然に終わらせる。
  //    何かがイベントループを持ち続けたときの保険に、10 秒後に終わらせる (unref = このタイマー自体はループを延ばさない)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
