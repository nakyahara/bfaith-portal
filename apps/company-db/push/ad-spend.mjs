#!/usr/bin/env node
/**
 * push/ad-spend.mjs — miniPC の広告費の日次 (warehouse.db fact_ad_spend) を Company DB (Render Postgres) へ送る (Company DB構想 11 の ②)。
 *
 *   node apps/company-db/push/ad-spend.mjs --mall amazon --days 35       ← daily-sync (「Amazon Ads (SKU)」が成功した朝)。昨日から 35 日
 *   node apps/company-db/push/ad-spend.mjs --mall amazon --all           ← 初回: 取得の記録 (ads_fetch_days) の最初の日から昨日まで
 *   node apps/company-db/push/ad-spend.mjs --mall amazon --from 2026-09-01 --to 2026-09-20 [--dry-run]
 *   node apps/company-db/push/ad-spend.mjs --mall amazon --legacy [--from … --to …] [--dry-run]   ← 1 回だけ: 取得の記録より前の日を「古い取込の行」の印で (下)
 *
 * 決め (設計 11 §5 = Codex 設計レビュー D1):
 *   - 🚨 **送るのは取得の記録 (ads_fetch_days) がある日だけ** (D1 #2・#4): 記録 = 取込 (fetch-amazon-ads.js) がレポートを最後まで取り、検査を通して、その日を丸ごと置き換えた印。
 *     記録の無い日 (取込の作り直しより前の日・取れなかった日) は送らない = 「0 円の日」を作らない。記録があって行が 0 の日は、0 行として送る (空の集合で置き換え)
 *   - 行と記録は **同じ読み取り取引で** 読む (取込が途中まで書いた日を読まない)。記録の行数・費用の合計と行が合わない日は送らず ❌
 *   - 世代 (generation = 取込がレポートを頼んだ時刻 ms) と report_id を送る。古い世代は受け口が stale で拒む (送信の時刻で世代を作らない。D1 #3)
 *   - 🚨 **台帳を持たない** (在庫日次と同じ): どの日を送り済みかは Render に聞く (GET …/ad-spend/status)。Render と同じ世代・レポート・指紋の日は送らない
 *   - 金額は REAL を **小数 2 桁の十進の文字列** にして送る。2 桁より細かい値 (取込は 2 桁に丸めて書く = 本来ありえない) は ❌ (黙って丸めない。D1 #6)
 *   - 送った後に毎回 relink (マスタが後から増えた SKU の行を出品に結び直す。同じ指紋の日は送り直さないので、送るだけでは結び直らない。D1 #7)
 *   - 🚨 広告のプロファイルは 1 つだけの前提 (取込と同じ)。記録に別のプロファイルがあれば何も送らない
 *
 * --legacy = 「古い取込の行」(中原さん 2026-09-27): Amazon は約 95 日より前を取り直させてくれない → 作り直す前の取込 (UPSERT だけ) が書いた行しか無い日を、印を付けて入れる。
 *   - 範囲 = 元データの最初の日 〜 取得の記録の最初の日の前日だけ (記録のある日には送らない)。印 = 世代 1 + report_id legacy:upsert-v1 (本物の取得が来れば必ず負ける)
 *   - 🚨 対象 (出品者 SKU) が大文字の行は送らない: 2026-05-03〜04 の取込が小文字にする前の形で書いた行が残っていて、3/1〜5/3 は全部が小文字の行と二重になっている
 *     (9/27 実測: 76,344 行・3〜5 月で 約 119 万円。小文字の行だけの合計がキャンペーンの合計と月ごとに一致)
 *   - 🚨 日ごとに、SKU 別の合計がキャンペーンの合計 (fact_ad_spend_campaign) と 1 円以内で合う日だけ送る。合わない日・行が無い日・キャンペーンの合計が無い日は送らず ⚠️ (推測で埋めない)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { postJson, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { syncBase } from './ne-shipments.mjs';
import { adSpendChecksum, adRowOf, centsToMoney, MAX_ROWS, LEGACY_GENERATION, LEGACY_REPORT_ID } from '../ingest/ad-spend.mjs';
import { isRealDate, jstDate } from '../ingest/stock-daily.mjs';
import { writeEvidence } from './evidence.mjs';

export const DEFAULT_DAYS = 35;   // 取込の 30 日 + 余裕
export const MAX_RANGE_DAYS = 800;   // 受け口の status の上限と同じ
export const MAX_BODY_BYTES = 3.5 * 1024 * 1024;   // 受け口の parser は 4MB
export const WINDOW_DAYS = 31;   // 1 回の読み取り取引で確定する日数
export const LEGACY_TOLERANCE_CENTS = 100;   // 古い取込の行: SKU 別の合計とキャンペーンの合計の差の許容 (1 円。別々のレポートの丸め)
export const REPORT_TYPE = 'spAdvertisedProduct';   // fetch-amazon-ads.js の REPORT_TYPE (warehouse の CommonJS を読み込まないよう写す。試験で同じか確かめる)
/** モール → 元データの読み方。楽天 RPP・au PAY は取込が動いてから */
export const MALLS = {
  amazon: { label: 'Amazon SP', scope: 'jp', adType: 'SP', reportType: REPORT_TYPE, factMall: 'amazon' },
};
const specOf = (mall) => (typeof mall === 'string' && Object.hasOwn(MALLS, mall) ? MALLS[mall] : null);

const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
export function datesBetween(from, to) { const out = []; for (let d = from; d <= to; d = addDays(d, 1)) out.push(d); return out; }

/** REAL の金額 → 銭 (整数)。負・読めない・小数 2 桁より細かい値は例外 */
export function realToCents(x, label) {
  if (typeof x !== 'number' || !Number.isFinite(x) || x < 0) throw new Error(`${label} が 0 以上の数でない: ${JSON.stringify(x)}`);
  const c = Math.round(x * 100);
  if (Math.abs(x * 100 - c) > 1e-4) throw new Error(`${label} が小数 2 桁より細かい (黙って丸めない): ${x}`);
  if (!Number.isSafeInteger(c)) throw new Error(`${label} が大きすぎる: ${x}`);
  return c;
}
const moneyOf = (x, label) => centsToMoney(BigInt(realToCents(x, label)));

/** 取得の記録のプロファイルが 1 つだけか確かめる (取込と同じ前提) */
export function assertSingleProfile(db, spec) {
  const ps = db.prepare('select distinct profile_id as p from ads_fetch_days where report_type = ?').all(spec.reportType).map((r) => r.p);
  if (ps.length > 1) throw new Error(`取得の記録に広告プロファイルが ${ps.length} つある (${ps.slice(0, 3).join(', ')}) = 1 つだけの前提が崩れている。どの日も送らない`);
}

const FACT_COLS = 'キャンペーンID as c, ターゲット粒度 as g, ターゲット as t, クリック数 as k, インプレッション as i, 広告費 as cost, 広告経由売上 as s, 広告経由数量 as u';
/** fact_ad_spend の行 → 受け口と同じ検証を通した行 + 費用の合計 (銭) + 通らなかった行の理由 */
function convertRows(raw) {
  const rows = [], errors = [];
  let cents = 0;
  for (const r of raw) {
    try {
      const row = adRowOf({
        campaign_id: r.c == null ? r.c : String(r.c), target_granularity: r.g, target_code: r.t, clicks: r.k, impressions: r.i,
        cost: moneyOf(r.cost, 'cost'), sales_1d: r.s == null ? null : moneyOf(r.s, 'sales_1d'), units_1d: r.u == null ? null : r.u,
      });   // 受け口と同じ検証
      cents += realToCents(r.cost, 'cost');
      rows.push(row);
    } catch (e) { errors.push(`${JSON.stringify(String(r.c)).slice(0, 30)} / ${JSON.stringify(String(r.t)).slice(0, 40)}: ${e.message}`); }
  }
  return { rows, errors, cents };
}

/** その日の記録 + 行 → { gen, reportId, rows, checksum, errors } (記録と行が合わなければ errors) */
function readDay(db, spec, d, rec) {
  const { rows, errors, cents } = convertRows(db.prepare(`select ${FACT_COLS} from fact_ad_spend where 日付 = ? and モール = ? and 広告タイプ = ? order by キャンペーンID, ターゲット粒度, ターゲット`).all(d, spec.factMall, spec.adType));
  if (!errors.length) {
    if (rows.length !== Number(rec.row_count)) errors.push(`行の数 ${rows.length} が取得の記録 (${rec.row_count}) と合わない (取込の後で行が書き換わった?)`);
    else {
      let recCents;
      try { recCents = realToCents(Number(rec.cost_total), '記録の費用の合計'); } catch (e) { errors.push(e.message); }
      if (recCents !== undefined && recCents !== cents) errors.push(`費用の合計 ${centsToMoney(BigInt(cents))} が取得の記録 (${rec.cost_total}) と合わない`);
    }
  }
  const gen = Number(rec.generation);
  if (!Number.isSafeInteger(gen) || gen <= 0) errors.push(`取得の記録の世代が読めない: ${rec.generation}`);
  return { gen, reportId: String(rec.report_id), fetchedAt: rec.fetched_at, rows, errors, checksum: errors.length ? null : adSpendChecksum(d, rows) };
}

/** [lo, hi] の記録と行を 1 つの読み取り取引で確定する。戻り値 = Map(日 → readDay の結果) */
export function readWindow(db, spec, lo, hi) {
  const read = () => {
    assertSingleProfile(db, spec);
    const recs = db.prepare(`select date_jst, generation, report_id, row_count, cost_total, fetched_at from ads_fetch_days where report_type = ? and date_jst between ? and ? order by date_jst`).all(spec.reportType, lo, hi);
    const days = new Map();
    for (const rec of recs) {
      if (!isRealDate(rec.date_jst)) throw new Error(`取得の記録に読めない日付がある: ${JSON.stringify(rec.date_jst).slice(0, 30)}`);
      days.set(rec.date_jst, readDay(db, spec, rec.date_jst, rec));
    }
    return days;
  };
  return typeof db.transaction === 'function' ? db.transaction(read)() : read();
}

/**
 * 古い取込の行 (--legacy) の [lo, hi] を 1 つの読み取り取引で確定する。戻り値 = Map(日 → { gen, reportId, rows, errors, checksum, skip, droppedUpper })
 *   skip = 送らない理由 (行が無い・キャンペーンの合計が無い・合わない)。記録のある日は入れない (呼ぶ側が範囲で外す + ここでも見る)
 */
export function readLegacyWindow(db, spec, lo, hi) {
  const read = () => {
    const days = new Map();
    const hasRec = db.prepare('select 1 as x from ads_fetch_days where report_type = ? and date_jst = ?');
    const camp = db.prepare('select count(*) as n, sum(広告費) as s from fact_ad_spend_campaign where 日付 = ? and モール = ? and 広告タイプ = ?');
    const upper = db.prepare('select count(*) as n from fact_ad_spend where 日付 = ? and モール = ? and 広告タイプ = ? and ターゲット <> lower(ターゲット)');
    const get = db.prepare(`select ${FACT_COLS} from fact_ad_spend where 日付 = ? and モール = ? and 広告タイプ = ? and ターゲット = lower(ターゲット) order by キャンペーンID, ターゲット粒度, ターゲット`);
    for (const d of datesBetween(lo, hi)) {
      if (hasRec.get(spec.reportType, d)) throw new Error(`${d} には取得の記録がある = 古い取込の行としては送らない (範囲の指定が違う)`);
      const droppedUpper = Number(upper.get(d, spec.factMall, spec.adType).n);
      const { rows, errors, cents } = convertRows(get.all(d, spec.factMall, spec.adType));
      const base = { gen: LEGACY_GENERATION, reportId: LEGACY_REPORT_ID, fetchedAt: null, rows, errors, checksum: null, skip: null, droppedUpper };
      if (!errors.length && rows.length === 0) { days.set(d, { ...base, skip: '行が無い (取れていない日。0 円の日を作らない)' }); continue; }
      const c = camp.get(d, spec.factMall, spec.adType);
      if (!errors.length && !(Number(c.n) > 0)) { days.set(d, { ...base, skip: 'キャンペーンの合計が無い (検算できない)' }); continue; }
      if (!errors.length) {
        const campCents = Math.round(Number(c.s) * 100);
        if (!Number.isSafeInteger(campCents) || Math.abs(campCents - cents) > LEGACY_TOLERANCE_CENTS) { days.set(d, { ...base, skip: `SKU 別の合計 ${centsToMoney(BigInt(cents))} がキャンペーンの合計 ${c.s} と 1 円より違う` }); continue; }
      }
      days.set(d, { ...base, checksum: errors.length ? null : adSpendChecksum(d, rows) });
    }
    return days;
  };
  return typeof db.transaction === 'function' ? db.transaction(read)() : read();
}

export function parseArgs(argv) {
  const out = { mall: null, days: null, from: null, to: null, all: false, legacy: false, dryRun: false, dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => { const v = argv[++i]; if (v === undefined || v.startsWith('--')) throw new Error(`${a} に値が無い`); return v; };
    if (a === '--mall') out.mall = val();
    else if (a === '--days') out.days = val();
    else if (a === '--from') out.from = val();
    else if (a === '--to') out.to = val();
    else if (a === '--all') out.all = true;
    else if (a === '--legacy') out.legacy = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--data-dir') out.dataDir = val();
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!specOf(out.mall)) throw new Error(`--mall は ${Object.keys(MALLS).join(' / ')}`);
  const modes = [out.days !== null, out.from !== null || out.to !== null, out.all].filter(Boolean).length;
  if (modes > 1) throw new Error('--days / --from --to / --all はどれか 1 つ');
  if (out.legacy && (out.days !== null || out.all)) throw new Error('--legacy は --from --to か範囲なし (元データの最初の日 〜 取得の記録の最初の日の前日) で');
  if (out.days !== null) { if (!/^[1-9][0-9]{0,2}$/.test(out.days)) throw new Error(`--days は 1〜999 の整数: ${out.days}`); out.days = Number(out.days); }
  if ((out.from !== null) !== (out.to !== null)) throw new Error('--from と --to は両方付ける');
  if (out.from !== null && (!isRealDate(out.from) || !isRealDate(out.to) || out.from > out.to)) throw new Error('--from / --to は実在する YYYY-MM-DD で from <= to');
  return out;
}

const REMOTE_OK = ['applied', 'same', 'refreshed', 'stale'];

/**
 * 送る (本体)。戻り値 = { ok, mall, from, to, sent[], same, done, refreshed, stale[], remoteNewer[], noRecord[], failed[], rowsSent, unresolvedSku, relink, yesterday, lastLine }
 * 差し替え (試験): fetchImpl / sleep
 */
export async function pushAdSpend({ mall, warehouse, fetchImpl = fetch, base, syncKey, today, days = null, from = null, to = null, all = false, legacy = false, dryRun = false, log = console.log, sleep }) {
  const spec = specOf(mall); if (!spec) throw new Error(`知らない mall: ${mall}`);
  if (!base) throw new Error('Render の宛先が無い (RENDER_MIRROR_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  if (!isRealDate(today)) throw new Error(`today が不正: ${today}`);
  const yesterday = addDays(today, -1);
  if (!warehouse.prepare(`select 1 as x from sqlite_master where type = 'table' and name = 'ads_fetch_days'`).get()) {
    throw new Error('取得の記録の表 (ads_fetch_days) が無い = 作り直した取込 (fetch-amazon-ads.js・#1483) がまだ一度も走っていない');
  }
  let lo, hi = yesterday;
  if (legacy) {
    const first = warehouse.prepare('select min(date_jst) as d from ads_fetch_days where report_type = ?').get(spec.reportType);
    if (!first || !isRealDate(first.d)) throw new Error('取得の記録 (ads_fetch_days) が無い = 古い取込の行の範囲が決まらない。先に作り直した取込で直近の日を取る (fetch-amazon-ads.js --days 90)');
    const lastLegacy = addDays(first.d, -1);
    if (from !== null) { lo = from; hi = to; if (hi > lastLegacy) throw new Error(`--to は取得の記録の最初の日の前日 (${lastLegacy}) まで: ${hi}`); }
    else {
      const f0 = warehouse.prepare('select min(日付) as d from fact_ad_spend where モール = ? and 広告タイプ = ?').get(spec.factMall, spec.adType);
      if (!f0 || !isRealDate(f0.d)) throw new Error('元データ (fact_ad_spend) が空・読めない日付');
      lo = f0.d; hi = lastLegacy;
    }
  } else if (all) {
    const first = warehouse.prepare('select min(date_jst) as d from ads_fetch_days where report_type = ?').get(spec.reportType);
    if (!first || !first.d) throw new Error(`取得の記録 (ads_fetch_days) が空 (${spec.label})。取込の作り直し (#1483) の後の取込が一度も走っていない`);
    if (!isRealDate(first.d)) throw new Error(`取得の記録に読めない日付がある: ${JSON.stringify(first.d).slice(0, 30)}`);
    lo = first.d;
  } else if (from !== null) { lo = from; hi = to; if (hi > yesterday) throw new Error(`--to は昨日 (${yesterday}) まで: ${hi}`); }
  else lo = addDays(yesterday, -((days ?? DEFAULT_DAYS) - 1));
  if (lo > hi) throw new Error(`範囲が空: ${lo}〜${hi}`);
  if (datesBetween(lo, hi).length > MAX_RANGE_DAYS) throw new Error(`範囲が長すぎる (${MAX_RANGE_DAYS} 日まで): ${lo}〜${hi}`);
  assertSingleProfile(warehouse, spec);   // Render に聞く前に (表が無ければ上で止まっている)

  const q = `mall=${encodeURIComponent(mall)}&scope=${encodeURIComponent(spec.scope)}&ad_type=${encodeURIComponent(spec.adType)}&from=${lo}&to=${hi}`;
  const sres = await fetchImpl(`${base}/ad-spend/status?${q}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!sres.ok) throw new Error(`Render の状態が取れない: HTTP ${sres.status}${sres.status === 404 ? ' (Render がまだ新しい版になっていない?)' : ''} ${(await sres.text()).replace(/\s+/g, ' ').slice(0, 160)}`);
  const sj = await sres.json();
  if (!sj || !Array.isArray(sj.days)) throw new Error('Render の状態の応答に days が無い');
  const remote = new Map();
  for (const x of sj.days) {
    if (!x || !isRealDate(x.date_jst) || x.date_jst < lo || x.date_jst > hi) throw new Error(`Render の状態の応答に、読めない・範囲の外の日付がある: ${JSON.stringify(x).slice(0, 120)}`);
    if (remote.has(x.date_jst)) throw new Error(`Render の状態の応答に同じ日が 2 つある: ${x.date_jst}`);
    if (!Number.isSafeInteger(x.generation) || typeof x.checksum !== 'string' || !/^[0-9a-f]{64}$/.test(x.checksum) || typeof x.report_id !== 'string') throw new Error(`Render の状態の応答の形が分からない: ${JSON.stringify(x).slice(0, 120)}`);
    remote.set(x.date_jst, x);
  }

  const out = { ok: true, mall, legacy, from: lo, to: hi, sent: [], same: 0, done: 0, refreshed: 0, stale: [], remoteNewer: [], noRecord: [], legacySkipped: [], droppedUpper: 0, failed: [], dryRun, rowsSent: 0, unresolvedSku: 0, relink: null,
    yesterday: { date: yesterday, local: false, generation: null, fetchedAt: null, onRender: false } };
  let local = new Map(), windowEnd = null;
  for (const d of datesBetween(lo, hi)) {
    // 31 日ずつ、送る内容を 1 つの読み取り取引で確定してから送る (HTTP を待つ間に取込が書き換えても、読んだ内容のまま送る)
    if (windowEnd === null || d > windowEnd) { windowEnd = addDays(d, WINDOW_DAYS - 1) > hi ? hi : addDays(d, WINDOW_DAYS - 1); local = (legacy ? readLegacyWindow : readWindow)(warehouse, spec, d, windowEnd); }
    const l = local.get(d), r = remote.get(d);
    try {
      if (legacy) {
        if (r && r.generation > LEGACY_GENERATION) { out.done++; continue; }   // Render に本物の取得がある日 = 古い行で戻さない (受け口も stale で拒む)
        out.droppedUpper += l.droppedUpper;
        if (l.skip) { out.legacySkipped.push({ date: d, reason: l.skip }); continue; }
      }
      if (!l) {
        if (r) out.done++;   // 手元に記録は無いが Render にはある (元の履歴を消した後など) → 触らない
        else out.noRecord.push(d);   // 取れていない日 = 送らない (0 円の日を作らない)
        continue;
      }
      if (d === yesterday) Object.assign(out.yesterday, { local: true, generation: l.gen, fetchedAt: l.fetchedAt || null });
      if (l.errors.length) throw new Error(`${l.errors.length} 件の食い違い (例: ${l.errors[0]}) → この日は送らない`);
      if (l.rows.length > MAX_ROWS) throw new Error(`行が多すぎる (${l.rows.length} > 受け口の上限 ${MAX_ROWS})`);
      if (r && r.generation === l.gen && r.report_id === l.reportId && r.checksum === l.checksum) {
        out.done++;
        if (d === yesterday) out.yesterday.onRender = true;
        continue;
      }
      if (r && r.generation > l.gen) { out.remoteNewer.push(d); continue; }   // Render の方が新しい取得 (miniPC の warehouse.db を戻した?) → 送らない・知らせる
      const payload = { mall, scope: spec.scope, ad_type: spec.adType, date_jst: d, generation: l.gen, report_id: l.reportId, checksum: l.checksum, rows: l.rows };
      const bytes = Buffer.byteLength(JSON.stringify(payload));
      if (bytes > MAX_BODY_BYTES) throw new Error(`1 日ぶんが大きすぎる (${bytes} バイト > ${MAX_BODY_BYTES})`);
      if (dryRun) { out.sent.push({ date: d, rows: l.rows.length, status: 'dry-run' }); out.rowsSent += l.rows.length; continue; }
      const res = await postJson(fetchImpl, { base, syncKey, path: '/ad-spend/day', log, sleep, body: payload });
      if (!res || !REMOTE_OK.includes(res.status)) throw new Error(`受け口の応答が分からない: ${JSON.stringify(res).slice(0, 160)}`);
      if (res.checksum !== l.checksum) throw new Error(`受け口の指紋 ${String(res.checksum).slice(0, 16)}… が送った内容と違う`);
      if (res.status === 'stale') { out.stale.push(d); log(`[company-db ad-spend ${mall}] ${d}: ⚠️ Render の方が新しい取得 (世代 ${res.remote_generation} > ${l.gen}) = 書かなかった`); continue; }
      if (d === yesterday) out.yesterday.onRender = true;
      if (res.status === 'same') { out.same++; continue; }
      if (res.status === 'refreshed') { out.refreshed++; continue; }
      if (res.rows !== l.rows.length) throw new Error(`受け口が入れた行数 ${res.rows} が、送った行数 ${l.rows.length} と違う`);
      out.sent.push({ date: d, rows: res.rows, status: res.status, unresolvedSku: res.unresolved_sku });
      out.rowsSent += res.rows; out.unresolvedSku += Number(res.unresolved_sku || 0);
    } catch (e) {
      out.ok = false;
      out.failed.push({ date: d, error: String(e.message).replace(/\s+/g, ' ').slice(0, 200) });
      log(`[company-db ad-spend ${mall}] ${d}: ❌ ${e.message}`);
    }
  }
  if (!dryRun) {
    // マスタが後から増えた SKU を結び直す (送らなかった日の行も)。失敗しても日の送信は済んでいる = ⚠️ に留める
    try { out.relink = await postJson(fetchImpl, { base, syncKey, path: '/ad-spend/relink', log, sleep, body: {} }); }
    catch (e) { out.relink = { error: String(e.message).slice(0, 200) }; }
  }
  const parts = [`${dryRun ? '送る予定' : '送った'} ${out.sent.length} 日 (${out.rowsSent} 行${dryRun ? '' : ` / SKU なのに出品が分からない ${out.unresolvedSku}`})`, `送り済み ${out.done + out.same} 日`];
  if (out.refreshed) parts.push(`中身が同じで世代だけ進めた ${out.refreshed} 日`);
  if (out.relink && !out.relink.error) parts.push(`結び直し ${out.relink.relinked} 行 (出品が分からない SKU の行 残り ${out.relink.unresolved_sku})`);
  if (legacy) parts.unshift('古い取込の行 (印 = 世代 1 + legacy:upsert-v1)');
  if (out.droppedUpper) parts.push(`大文字の重複行を外した ${out.droppedUpper} 行`);
  if (out.legacySkipped.length) parts.push(`⚠️ 送らなかった日 ${out.legacySkipped.length} (${out.legacySkipped.slice(0, 2).map((x) => `${x.date}: ${x.reason}`).join(' / ')})`);
  if (out.noRecord.length) parts.push(`⚠️ 取得の記録が無い日 ${out.noRecord.length} (${out.noRecord.slice(0, 3).join(', ')}${out.noRecord.length > 3 ? ' ほか' : ''}。送らない)`);
  if (out.stale.length || out.remoteNewer.length) parts.push(`⚠️ Render の方が新しい取得の日 ${out.stale.length + out.remoteNewer.length} (${[...out.stale, ...out.remoteNewer].slice(0, 3).join(', ')}。書き換えない)`);
  if (out.relink && out.relink.error) parts.push(`⚠️ 結び直しに失敗 (${out.relink.error.slice(0, 80)})`);
  if (out.failed.length) parts.push(`失敗 ${out.failed.length} 日 (${out.failed.slice(0, 2).map((f) => `${f.date}: ${f.error}`).join(' / ')})`);
  const warn = out.noRecord.length || out.legacySkipped.length || out.stale.length || out.remoteNewer.length || (out.relink && out.relink.error);
  out.lastLine = `${out.failed.length ? '❌' : warn ? '⚠️' : '✅'} Company DB 広告費 (${spec.label}) ${lo}〜${hi}: ${parts.join(' / ')}${dryRun ? ' [dry-run = 送っていない]' : ''}`;
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = a.dataDir || process.env.DATA_DIR || path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..', 'data');
    const file = path.join(dataDir, 'warehouse.db');
    if (!fs.existsSync(file)) throw new Error(`warehouse.db が無い: ${file} (--data-dir か DATA_DIR)`);
    const today = process.env.WAREHOUSE_BUSINESS_DATE || jstDate(new Date());
    const db = new Database(file, { readonly: true, fileMustExist: true });
    let r;
    try { r = await pushAdSpend({ mall: a.mall, warehouse: db, base: syncBase(), syncKey: process.env.MIRROR_SYNC_KEY || '', today, days: a.days, from: a.from, to: a.to, all: a.all, legacy: a.legacy, dryRun: a.dryRun }); }
    finally { db.close(); }
    console.log(String(r.lastLine).replace(/\s+/g, ' '));   // 最後の 1 行を複数行にしない
    // 朝の見張り (③) に渡す証跡。昨日の日が手元にあるか・Render に届いたか・どの取得の世代か
    if (!a.dryRun && !a.legacy) writeEvidence(dataDir, `ad-spend-${a.mall}`, { kind: 'ad_spend', mall: a.mall, ok: !!r.ok, today, from: r.from, to: r.to, sent_days: r.sent.length, rows_sent: r.rowsSent, done: r.done, same: r.same,
      refreshed: r.refreshed, stale: r.stale.length + r.remoteNewer.length, no_record: r.noRecord.length, no_record_days: r.noRecord.slice(-5), failed: r.failed.length, failed_days: r.failed.slice(0, 5).map((f) => f.date),
      unresolved_sku: r.relink && !r.relink.error ? r.relink.unresolved_sku : null, relink_error: r.relink && r.relink.error ? r.relink.error : null, yesterday: r.yesterday });
    code = r.ok ? 0 : 1;
  } catch (e) {
    console.log(`❌ Company DB 広告費: ${String(e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    try { const a2 = parseArgs(process.argv.slice(2)); if (!a2.dryRun) writeEvidence(a2.dataDir || process.env.DATA_DIR || '', `ad-spend-${a2.mall}`, { kind: 'ad_spend', mall: a2.mall, ok: false, error: String(e && e.message).slice(0, 300) }); } catch { /* 証跡は補助 */ }
  }
  // 🚨 fetch の直後に process.exit() しない (Windows の Node で終了コード 127。stock-daily.mjs と同じ)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
