/**
 * ingest/ad-spend.mjs — miniPC から届いた「その日の広告費 (日 × キャンペーン × 対象)」を core.ad_spend_daily に入れる受け口の本体
 *   (Company DB構想 11 の ②。受け皿 = migration 0035。送り手 = apps/company-db/push/ad-spend.mjs)。
 *
 * 1 日 = 1 要求 = 1 取引: その日の行を丸ごと消して入れ直す (取り直しで行が増えも減りもする)。日の状態 core.ad_spend_days も同じ取引で書く。
 *
 * 決め (設計 11 §5 = Codex 設計レビュー D1):
 *   - 🚨 **取得の世代で古い要求を拒む** (D1 #3): generation = miniPC でレポートを頼んだ時刻 (ms。取込 fetch-amazon-ads.js が ads_fetch_days に記録したもの。送信の時刻ではない)
 *       Render の世代より古い → stale (書かない) / 同じ世代 + 同じレポート + 同じ指紋 → same / 同じ世代でどれかが違う → CONFLICT (409 = どちらが新しいか分からない)
 *       新しい世代で指紋が同じ → refreshed (行は触らず世代だけ進める = 見張りが「今朝の取得が届いた」と読める) / 新しい世代で指紋が違う → applied (置き換え)
 *   - 🚨 **0 行の日も送る** (D1 #4): 最後まで取れて 0 行 = 空の集合で置き換える。取れていない日は送り手が送らない (受け口には届かない)
 *   - 金額は **十進の文字列** (小数 2 桁まで。'123.4' も '123.40' も同じ値)。数 (number) では受けない = 浮動小数の誤差を指紋に入れない。2 桁より細かい値は拒む (黙って丸めない。D1 #6)
 *   - 広告経由の売上・数量は 1 日の帰属 (sales1d / unitsSoldClicks1d)。分からない = null (0 と別。指紋でも区別する)
 *   - 内容の指紋 = adSpendChecksum (送り手も同じ関数)。受け口は届いた行から計算し直し、送り手の値と合わなければ 400 (途中で壊れた送信を入れない)
 *   - 出品の結び: 粒度 sku の行だけ core.resolve_listing_id (D1 #7)。後からマスタが増えたら relinkAdSpend (送り手が毎回呼ぶ)
 *   - 同時実行は advisory lock (モール + scope + 広告タイプ + 日) で直列化。取れなければ LOCKED (503 = 送り手がやり直す)
 */
import crypto from 'node:crypto';
import { isRealDate, jstDate, isValidCode, err } from './stock-daily.mjs';

export const COMPANY_ID = 1;
export const ENTITY = 'ad_spend_daily';
export const MAX_ROWS = 20000;   // Amazon SP = 1 日 約 2,000 行 (2026-09 時点)
/** モール → { sourceSystem, scopes, adTypes, listingMall (出品を探すモール) }。楽天 RPP・au PAY は取込が動いてから足す */
export const AD_SOURCES = {
  amazon: { sourceSystem: 'amazon', scopes: ['jp'], adTypes: ['SP'], listingMall: 'amazon' },
};
export const GRANULARITIES = ['sku', 'asin', 'none'];
export const CHECKSUM_VERSION = 'ad-v1';
const bad = (m) => err('BAD_REQUEST', m);
const INT32_MAX = 2147483647;
const isCount = (v) => Number.isInteger(v) && v >= 0 && v <= INT32_MAX;
const MONEY_RE = /^(0|[1-9][0-9]{0,11})(\.[0-9]{1,2})?$/;   // numeric(14,2) に入る 0 以上の値 (12 桁 + 小数 2 桁)
// 🚨 制御文字はソースにエスケープで書かず、文字コードから作る (stock-daily.mjs と同じ理由)
const SEP = String.fromCharCode(0), LF = String.fromCharCode(10);

/** 金額の文字列 → 小数 2 桁にそろえた文字列 ('12' → '12.00'、'12.5' → '12.50')。形が外れていれば null */
export function canonMoney(v) {
  if (typeof v !== 'string' || !MONEY_RE.test(v)) return null;
  const [i, f = ''] = v.split('.');
  return `${i}.${f.padEnd(2, '0')}`;
}
/** 小数 2 桁の文字列 → 銭 (整数の BigInt)。合計を浮動小数で足さない */
export const moneyCents = (s) => BigInt(s.replace('.', ''));
export const centsToMoney = (c) => { const s = c.toString().padStart(3, '0'); return `${s.slice(0, -2)}.${s.slice(-2)}`; };

/** 行の鍵 (日の中で一意): キャンペーン + 粒度 + 対象 */
const keyOf = (r) => `${r.campaign_id}${SEP}${r.target_granularity}${SEP}${r.target_code}`;
/** 🚨 並びは文字列の UTF-16 の順 (< で比べる。localeCompare は環境で変わる) */
const byKey = (a, b) => { const x = keyOf(a), y = keyOf(b); return x < y ? -1 : x > y ? 1 : 0; };

/**
 * 内容の指紋 (送り手も同じ関数)。版 + 日 + 行を鍵の順に: 金額は小数 2 桁の文字列・null は空 (0 と区別)。
 * 🚨 本番に保存済みの指紋と同じ式のまま変えない (変えるときは CHECKSUM_VERSION を上げ、全部の日が「違う」になるのを受け入れる)
 */
export function adSpendChecksum(dateJst, rows) {
  const h = crypto.createHash('sha256');
  h.update(`${CHECKSUM_VERSION}${SEP}${dateJst}${LF}`);
  for (const r of [...rows].sort(byKey)) {
    h.update([r.campaign_id, r.target_granularity, r.target_code, r.clicks, r.impressions, r.cost, r.sales_1d ?? '', r.units_1d ?? ''].join(SEP) + LF);
  }
  return h.digest('hex');
}

/** 1 行を検証して正規化する (送り手も同じ関数を使う)。外れていれば理由の文字列を投げる */
export function adRowOf(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error('object でない');
  const campaignId = r.campaign_id;
  if (typeof campaignId !== 'string' || !isValidCode(campaignId) || campaignId.length > 100) throw new Error(`campaign_id が不正: ${JSON.stringify(String(campaignId)).slice(0, 40)}`);
  const g = r.target_granularity;
  if (typeof g !== 'string' || !GRANULARITIES.includes(g)) throw new Error(`target_granularity は ${GRANULARITIES.join(' / ')}: ${String(g).slice(0, 20)}`);
  const code = r.target_code;
  if (g === 'none') { if (code !== '') throw new Error('粒度 none の target_code は空'); }
  else if (!isValidCode(code)) throw new Error(`target_code が不正 (空・前後の空白・制御文字・200 文字超): ${JSON.stringify(String(code)).slice(0, 40)}`);
  if (!isCount(r.clicks)) throw new Error(`clicks は 0 以上の整数: ${String(r.clicks).slice(0, 20)}`);
  if (!isCount(r.impressions)) throw new Error(`impressions は 0 以上の整数: ${String(r.impressions).slice(0, 20)}`);
  const cost = canonMoney(r.cost);
  if (cost === null) throw new Error(`cost は 0 以上・小数 2 桁までの十進の文字列: ${JSON.stringify(r.cost).slice(0, 30)}`);
  let sales = null;
  if (r.sales_1d !== null) { sales = canonMoney(r.sales_1d); if (sales === null) throw new Error(`sales_1d は null か、0 以上・小数 2 桁までの十進の文字列: ${JSON.stringify(r.sales_1d).slice(0, 30)}`); }
  if (r.units_1d !== null && !isCount(r.units_1d)) throw new Error(`units_1d は null か 0 以上の整数: ${String(r.units_1d).slice(0, 20)}`);
  return { campaign_id: campaignId, target_granularity: g, target_code: code, clicks: r.clicks, impressions: r.impressions, cost, sales_1d: sales, units_1d: r.units_1d };
}

/** 要求の検証。戻り値 = { mall, scope, adType, dateJst, generation, reportId, rows, checksum, costTotal } */
export function validateAdSpendBody(body, { todayJst = jstDate(new Date()) } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  const mall = body.mall;
  if (typeof mall !== 'string' || !Object.hasOwn(AD_SOURCES, mall)) throw bad(`mall は ${Object.keys(AD_SOURCES).join(' / ')}: ${String(mall).slice(0, 30)}`);
  const spec = AD_SOURCES[mall];
  if (typeof body.scope !== 'string' || !spec.scopes.includes(body.scope)) throw bad(`scope は ${spec.scopes.join(' / ')}: ${String(body.scope).slice(0, 30)}`);
  if (typeof body.ad_type !== 'string' || !spec.adTypes.includes(body.ad_type)) throw bad(`ad_type は ${spec.adTypes.join(' / ')}: ${String(body.ad_type).slice(0, 30)}`);
  const dateJst = body.date_jst;
  if (!isRealDate(dateJst)) throw bad(`date_jst は実在する YYYY-MM-DD: ${String(dateJst).slice(0, 30)}`);
  if (dateJst > todayJst) throw bad(`date_jst が未来 (JST の今日 = ${todayJst}): ${dateJst}`);
  const generation = body.generation;
  if (!Number.isSafeInteger(generation) || generation <= 0) throw bad(`generation は正の整数 (取込がレポートを頼んだ時刻 ms): ${String(generation).slice(0, 30)}`);
  const reportId = body.report_id;
  if (typeof reportId !== 'string' || !isValidCode(reportId)) throw bad('report_id が無い・不正');
  if (!Array.isArray(body.rows)) throw bad('rows は配列 (0 行の日は空の配列 = 最後まで取れて 0 行)');
  if (body.rows.length > MAX_ROWS) throw bad(`rows が多すぎる (${body.rows.length} > ${MAX_ROWS})`);
  const rows = [], seen = new Set();
  let cents = 0n;
  for (let i = 0; i < body.rows.length; i++) {
    let row;
    try { row = adRowOf(body.rows[i]); } catch (e) { throw bad(`rows[${i}]: ${e.message}`); }
    const k = keyOf(row);
    if (seen.has(k)) throw bad(`rows[${i}] が重複 (同じキャンペーン・粒度・対象): ${row.campaign_id} / ${row.target_granularity} / ${row.target_code.slice(0, 40)}`);
    seen.add(k);
    cents += moneyCents(row.cost);
    rows.push(row);
  }
  const checksum = adSpendChecksum(dateJst, rows);
  if (body.checksum !== checksum) throw bad(`checksum が届いた行から計算した値と合わない (送り手 ${String(body.checksum).slice(0, 16)}… / 受け口 ${checksum.slice(0, 16)}…)`);
  return { mall, scope: body.scope, adType: body.ad_type, dateJst, generation, reportId, rows, checksum, costTotal: centsToMoney(cents) };
}

export function newAdRunId(mall) {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  const stamp = `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}${p(d.getUTCMilliseconds(), 3)}`;
  return `ads_${mall}_${stamp}_${crypto.randomBytes(3).toString('hex')}`;
}

const affected = (r) => (r && (r.rowCount ?? r.affectedRows ?? (Array.isArray(r.rows) ? r.rows.length : 0))) || 0;

/**
 * 1 日ぶんを入れる。db = { query, exec } (router の pgAdapter / 試験の PGlite)。
 * @returns {{ status: 'applied'|'same'|'refreshed'|'stale', mall, scope, ad_type, date_jst, rows, resolved, unresolved_sku, run_id, checksum, generation, remote_generation? }}
 *   例外: BAD_REQUEST (400) / CONFLICT (409 = 同じ世代で違う内容) / LOCKED (503)
 */
export async function ingestAdSpendDay(db, body, { host = 'render', companyId = COMPANY_ID, todayJst, afterWrite = null, log = () => {} } = {}) {
  const v = validateAdSpendBody(body, todayJst ? { todayJst } : {});
  const spec = AD_SOURCES[v.mall];
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const base = { mall: v.mall, scope: v.scope, ad_type: v.adType, date_jst: v.dateJst, checksum: v.checksum, generation: v.generation };
  const dayKey = [v.dateJst, companyId, v.mall, v.scope, v.adType];
  const dayWhere = `date_jst = $1::date and company_id = $2::smallint and mall = $3 and scope_key = $4 and ad_type = $5`;
  await db.exec('begin');
  try {
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`company-db:ad-spend:${v.mall}:${v.scope}:${v.adType}:${v.dateJst}`])).got;
    if (!got) throw err('LOCKED', `${v.mall}/${v.scope}/${v.adType} ${v.dateJst} の別の取込が走っている`);
    const day = await one(`select source_generation, source_report_id, checksum, row_count, ingest_run_id from core.ad_spend_days where ${dayWhere} for update`, dayKey);
    if (day) {
      const rg = Number(day.source_generation);
      if (rg > v.generation) {
        await db.exec('rollback');
        log(`${v.dateJst}: stale (Render の世代 ${rg} > 届いた世代 ${v.generation})`);
        return { status: 'stale', ...base, rows: v.rows.length, resolved: null, unresolved_sku: null, run_id: day.ingest_run_id, remote_generation: rg };
      }
      if (rg === v.generation) {
        await db.exec('rollback');
        if (day.source_report_id === v.reportId && day.checksum === v.checksum) {
          return { status: 'same', ...base, rows: Number(day.row_count), resolved: null, unresolved_sku: null, run_id: day.ingest_run_id };
        }
        throw err('CONFLICT', `${v.dateJst}: 同じ世代 (${v.generation}) で${day.source_report_id !== v.reportId ? `別のレポート (${day.source_report_id} / ${v.reportId})` : '内容が違う'} = どちらが新しいか分からないので書かない`);
      }
      if (day.checksum === v.checksum) {
        // 新しい取得で中身が同じ: 行は触らず世代だけ進める (見張りが「今朝の取得が届いた」と読める。行の ingest_run_id は入れた回のまま)
        await db.query(`update core.ad_spend_days set source_generation = $6, source_report_id = $7, updated_at = now() where ${dayWhere}`, [...dayKey, v.generation, v.reportId]);
        if (afterWrite) await afterWrite();
        await db.exec('commit');
        return { status: 'refreshed', ...base, rows: Number(day.row_count), resolved: null, unresolved_sku: null, run_id: day.ingest_run_id };
      }
    }
    const runId = newAdRunId(v.mall);
    await db.query(
      `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, status, complete, rows_seen, source_tz, checksum, format_version)
       values ($1, $2, $3, $4, $5, now(), 'running', false, $6, 'Asia/Tokyo', $7, $8)`, [runId, spec.sourceSystem, ENTITY, `${v.scope}:${v.adType}`, host, v.rows.length, v.checksum, CHECKSUM_VERSION]);
    // 日の状態を先に (行の外部キーの親)。行は on delete cascade ではなく、ここで明示して消す (日の行を残したまま入れ直す)
    await db.query(`delete from core.ad_spend_daily where ${dayWhere}`, dayKey);
    await db.query(
      `insert into core.ad_spend_days (company_id, mall, scope_key, ad_type, date_jst, source_generation, source_report_id, checksum, row_count, cost_total, ingest_run_id, updated_at)
       values ($2::smallint, $3, $4, $5, $1::date, $6, $7, $8, $9, $10::numeric, $11, now())
       on conflict (company_id, mall, scope_key, ad_type, date_jst) do update set source_generation = excluded.source_generation, source_report_id = excluded.source_report_id,
         checksum = excluded.checksum, row_count = excluded.row_count, cost_total = excluded.cost_total, ingest_run_id = excluded.ingest_run_id, updated_at = now()`,
      [...dayKey, v.generation, v.reportId, v.checksum, v.rows.length, v.costTotal, runId]);
    const col = (c) => v.rows.map((r) => r[c]);
    const inserted = v.rows.length === 0 ? 0 : affected(await db.query(
      `insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ad_sales_1d, units_1d, ingest_run_id)
       select $2::smallint, $3, $4, $5, $1::date, t.c, t.g, t.code,
              case when t.g = 'sku' then core.resolve_listing_id($2::smallint, $7, t.code) end,
              t.k, t.i, t.cost::numeric, t.s::numeric, t.u, $6
         from unnest($8::text[], $9::text[], $10::text[], $11::int[], $12::int[], $13::text[], $14::text[], $15::int[]) as t(c, g, code, k, i, cost, s, u)`,
      [...dayKey, runId, spec.listingMall, col('campaign_id'), col('target_granularity'), col('target_code'), col('clicks'), col('impressions'), col('cost'), col('sales_1d'), col('units_1d')]));
    if (inserted !== v.rows.length) throw err('ROWS_MISMATCH', `入れた行数 ${inserted} が、届いた行数 ${v.rows.length} と違う`);
    const cnt = await one(`select count(*) filter (where listing_id is not null)::int as resolved, count(*) filter (where listing_id is null and target_granularity = 'sku')::int as unresolved_sku
                             from core.ad_spend_daily where ${dayWhere}`, dayKey);
    await db.query(`update ops.ingest_runs set status = 'success', complete = true, finished_at = now(), rows_inserted = $2, rows_skipped = 0 where ingest_run_id = $1`, [runId, inserted]);
    if (afterWrite) await afterWrite();   // 試験用: 全部書いた後・commit の前
    await db.exec('commit');
    log(`${v.dateJst}: applied (行 ${inserted} / 費用 ${v.costTotal} / 出品が分かった ${cnt.resolved} / SKU なのに分からない ${cnt.unresolved_sku}${day ? ` / 世代 ${day.source_generation} → ${v.generation}` : ''})`);
    return { status: 'applied', ...base, rows: inserted, resolved: Number(cnt.resolved), unresolved_sku: Number(cnt.unresolved_sku), run_id: runId };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/** 期間の日の状態 (送り手が「送る日」を決める・見張りが読む)。戻り値 = [{ date_jst, generation, report_id, checksum, row_count, cost_total }] */
export async function adSpendStatus(db, { mall, scope, adType, from, to, companyId = COMPANY_ID }) {
  if (typeof mall !== 'string' || !Object.hasOwn(AD_SOURCES, mall)) throw bad('mall が不正');
  const spec = AD_SOURCES[mall];
  if (!spec.scopes.includes(scope)) throw bad('scope が不正');
  if (!spec.adTypes.includes(adType)) throw bad('ad_type が不正');
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw bad('from / to は YYYY-MM-DD で from <= to');
  if ((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 > 800) throw bad('範囲は 800 日まで');
  const r = await db.query(
    `select date_jst::text as date_jst, source_generation::text as generation, source_report_id as report_id, checksum, row_count, cost_total::text as cost_total
       from core.ad_spend_days where company_id = $1::smallint and mall = $2 and scope_key = $3 and ad_type = $4 and date_jst between $5::date and $6::date order by date_jst`,
    [companyId, mall, scope, adType, from, to]);
  return r.rows.map((x) => ({ date_jst: x.date_jst, generation: Number(x.generation), report_id: x.report_id, checksum: x.checksum, row_count: Number(x.row_count), cost_total: x.cost_total }));
}

/** マスタが後から増えたとき、listing_id が null の sku の行を解き直す。戻り値 = { relinked, unresolved_sku } */
export async function relinkAdSpend(db, { companyId = COMPANY_ID } = {}) {
  const relinked = Number((await db.query(`select core.relink_ad_spend_listings($1::smallint) as n`, [companyId])).rows[0].n);
  const left = Number((await db.query(`select count(*)::int as n from core.ad_spend_daily where company_id = $1::smallint and listing_id is null and target_granularity = 'sku'`, [companyId])).rows[0].n);
  return { relinked, unresolved_sku: left };
}
