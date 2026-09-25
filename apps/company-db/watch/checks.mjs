/**
 * checks.mjs — 見張りの項目の評価 (config/watch-checks.mjs の定義を読んで SQL を流す)。設計 = 09 §2・§4
 *
 * 各評価は「評価キー (check × scope)」ごとに 1 つの結果を返す:
 *   { checkId, scopeKey, subjectType, periodFrom, periodTo, verdict, severity, observed, threshold, reason, inputGeneration, sampleSize, items, itemTotal }
 *   verdict = pass / breach / blocked / execution_error。severity = 定義の重さ (verdict とは別の軸)
 * 🚨 「行がある = そろっている」と読まない: 前提 (完了の印) が無ければ blocked。pass にしない
 * SQL は固定・逐次。db = { query(text, params) } (pg の client でも PGlite でも同じ)
 * W13 だけは Company DB ではなく miniPC のファイル (照合の全件 JSON) を読む (env DATA_DIR)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readEvidence } from '../push/evidence.mjs';

export const scopeKeyOf = (a, b) => `${a}/${b}`;
export const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
const jstDateOf = (iso) => new Date(Date.parse(iso) + 9 * 3600000).toISOString().slice(0, 10);
const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
const oneOf = async (db, sql, params) => (await db.query(sql, params)).rows[0] || null;

/** 評価キーの予定 (評価の前に確定する = 「12 項目中 12」ではなく「キー N のうち N」) */
export function plannedKeys(config) {
  const keys = [];
  // 定義の順番の約束: depends の相手は先に評価される (後ろにあると「未評価」で blocked のまま = 定義の誤り)
  const seen = new Set();
  for (const c of config.CHECKS) {
    for (const dep of c.depends || []) { const id = dep.endsWith(':*') ? dep.slice(0, -2) : dep; if (!seen.has(id)) throw new Error(`定義の順番が違う: ${c.id} の前提 ${id} が先に無い (config/watch-checks.mjs の CHECKS)`); }
    seen.add(c.id);
  }
  for (const c of config.CHECKS) {
    if (c.id === 'W1' || c.id === 'W2') for (const s of config.STOCK_SCOPES) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(s.source, s.scope) });
    else if (c.id === 'W3' || c.id === 'W5' || c.id === 'W4') keys.push({ checkId: c.id, scopeKey: scopeKeyOf(config.STOCK_DIFF.source, config.STOCK_DIFF.scope) });
    else if (c.id === 'W7' || c.id === 'W9' || c.id === 'W8' || c.id === 'W11') for (const m of config.ORDER_MALLS) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(m.mall, m.scope) });
    else if (c.id === 'W6') keys.push({ checkId: c.id, scopeKey: scopeKeyOf('all', config.W6_SCOPE.scope) });
    else if (c.id === 'W12') keys.push({ checkId: c.id, scopeKey: 'db/company' });
    else if (c.id === 'W13') keys.push({ checkId: c.id, scopeKey: config.W13_SCOPE });
    else if (c.id === 'W10') { for (const k of config.W10_KINDS) keys.push({ checkId: c.id, scopeKey: w10KindKey(k) }); keys.push({ checkId: c.id, scopeKey: W10_OTHER }); }
  }
  return keys;
}

const base = (check, scopeKey, extra) => ({ checkId: check.id, checkVersion: check.version, scopeKey, subjectType: 'scope', severity: check.severity, observed: null, threshold: null, reason: null, inputGeneration: null, sampleSize: null, items: [], itemTotal: null, periodFrom: null, periodTo: null, ...extra });

/** partial を許す例外が今日も有効か (until を過ぎたら効かない = 期限つき) */
export const partialAllowed = (scopeDef, asOf) => !!(scopeDef.allowPartial && (!scopeDef.allowPartial.until || asOf <= scopeDef.allowPartial.until));

// ── W1 在庫の取込の完了
export async function evalW1(ctx, check) {
  const { db, config, asOf, now } = ctx;
  const out = [];
  for (const s of config.STOCK_SCOPES) {
    const day = addDays(asOf, s.dayOffset);
    const scopeKey = scopeKeyOf(s.source, s.scope);
    const row = await oneOf(db, `select status, ingest_run_id, rows, built_at::text as built_at, completed_at::text as completed_at from snapshots.stock_capture_days
      where snapshot_date = $1::date and source = $2 and scope_key = $3 and company_id = $4::smallint`, [day, s.source, s.scope, config.COMPANY_ID]);
    const stale = await oneOf(db, `select count(*)::int as n, min(snapshot_date)::text as oldest from snapshots.stock_capture_days
      where source = $1 and scope_key = $2 and company_id = $3::smallint and status = 'building' and built_at < $4::timestamptz - ($5::int * interval '1 minute')`,
      [s.source, s.scope, config.COMPANY_ID, now.toISOString(), config.BUILDING_STALE_MINUTES]);
    const allow = partialAllowed(s, asOf);
    const observed = { day, status: row ? row.status : 'absent', rows: row ? row.rows : null, stale_building: stale.n, stale_building_oldest: stale.oldest, partial_allowed: allow, allow_reason: s.allowPartial ? s.allowPartial.reason : null, allow_until: s.allowPartial ? s.allowPartial.until : null };
    const r = base(check, scopeKey, { periodFrom: day, periodTo: day, observed, inputGeneration: row ? { capture_run_id: row.ingest_run_id } : null });
    if (!row) { r.verdict = 'breach'; r.reason = `対象日 ${day} の取得記録が無い (送り手が走っていない・失敗した)`; }
    else if (row.status === 'complete') { r.verdict = 'pass'; }
    else if (row.status === 'partial' && allow) { r.verdict = 'pass'; r.reason = `partial (例外: ${s.allowPartial.reason}。見直し ${s.allowPartial.until})`; }
    else if (row.status === 'building') { r.verdict = 'breach'; r.reason = `対象日 ${day} が building のまま (作りかけが残っている)`; }
    else { r.verdict = 'breach'; r.reason = `対象日 ${day} が ${row.status}${row.status === 'partial' && s.allowPartial ? ' (例外の期限切れ)' : ''}`; }
    if (stale.n > 0) { r.verdict = 'breach'; r.reason = `${r.reason ? r.reason + ' / ' : ''}building の滞留 ${stale.n} 日 (最古 ${stale.oldest})`; }
    out.push(r);
  }
  return out;
}

// ── W2 在庫の欠測の履歴 (窓の中の missing / partial / 行なし。案件は日ごと)
export async function evalW2(ctx, check) {
  const { db, config, asOf } = ctx;
  const out = [];
  for (const s of config.STOCK_SCOPES) {
    const to = addDays(asOf, s.dayOffset - 1);
    // 監視の開始日 (since) より前の日は評価しない = 在庫日次を作る前・バックフィルで埋まらない履歴を「欠測」と数えない (窓の頭を since で切る)
    const from = [addDays(to, -(config.W2_WINDOW_DAYS - 1)), s.since || ''].reduce((a, b) => (a > b ? a : b));
    const scopeKey = scopeKeyOf(s.source, s.scope);
    const allow = partialAllowed(s, asOf);
    const bad = [];
    let days = 0;
    if (from <= to) {
      const rows = await rowsOf(db, `select snapshot_date::text as d, status from snapshots.stock_capture_days
        where source = $1 and scope_key = $2 and company_id = $3::smallint and snapshot_date between $4::date and $5::date`, [s.source, s.scope, config.COMPANY_ID, from, to]);
      const byDay = new Map(rows.map((x) => [x.d, x.status]));
      for (let d = from; d <= to; d = addDays(d, 1)) {
        days++;
        const st = byDay.get(d) || 'absent';
        if (st === 'complete' || (st === 'partial' && allow)) continue;
        bad.push({ subjectType: 'day', subjectKey: d, payload: { status: st } });
      }
    }
    const r = base(check, scopeKey, { periodFrom: from <= to ? from : null, periodTo: from <= to ? to : null, observed: { window_days: config.W2_WINDOW_DAYS, days_evaluated: days, since: s.since || null, bad_days: bad.map((b) => `${b.subjectKey}:${b.payload.status}`), partial_allowed: allow }, threshold: { bad_days: 0 }, sampleSize: days, items: bad, itemTotal: bad.length });
    r.verdict = bad.length ? 'breach' : 'pass';
    if (bad.length) r.reason = `欠測 ${bad.length} 日 (${bad.slice(0, 3).map((b) => `${b.subjectKey.slice(5)} ${b.payload.status}`).join(', ')}${bad.length > 3 ? ' ほか' : ''})`;
    out.push(r);
  }
  return out;
}

// ── W3 在庫の差の完了
export async function evalW3(ctx, check) {
  const { db, config, asOf } = ctx;
  const d = config.STOCK_DIFF, scopeKey = scopeKeyOf(d.source, d.scope), day = addDays(asOf, -1);
  const r = base(check, scopeKey, { periodFrom: day, periodTo: day, threshold: { status: 'done' } });
  const migrated = (await oneOf(db, `select to_regclass('snapshots.stock_diff_days') is not null as ok`, [])).ok;
  if (!migrated) { r.verdict = 'blocked'; r.reason = '0022 (snapshots.stock_diff_days) が未適用'; return [r]; }
  const row = await oneOf(db, `select status, skip_reason, events, unresolved_changed, created_at::text as created_at from snapshots.stock_diff_days
    where to_date = $1::date and source = $2 and scope_key = $3 and calc_version = $4 and company_id = $5::smallint`, [day, d.source, d.scope, d.calcVersion, config.COMPANY_ID]);
  r.observed = { day, calc_version: d.calcVersion, status: row ? row.status : 'absent', skip_reason: row ? row.skip_reason : null, events: row ? row.events : null, unresolved_changed: row ? row.unresolved_changed : null };
  if (!row) { r.verdict = 'breach'; r.reason = `昨日 (${day}) の差の印が無い (毎時 :35 の回が作っていない)`; }
  else if (row.status === 'done') { r.verdict = 'pass'; }
  else { r.verdict = 'pass'; r.reason = `skipped (${row.skip_reason}) = 前日の欠測。W2 で見る`; }
  return [r];
}

/**
 * 今朝の差分送信 (push) の証跡の判定 = W7 の本体。W10 も「取り直しが確かめられた送信か」「W7 が扱った run か」に使う。
 * 戻り値 = { verdict: pass / breach / blocked, reason, observed, inputGeneration, contract }
 */
export async function judgePush(db, ev, { scope, syncRunId = null, unbound = false }) {
  const o = { verdict: null, reason: null, observed: null, inputGeneration: null };
  const done = (verdict, reason) => Object.assign(o, { verdict, reason });
  if (!ev) return done('blocked', `今朝の実行${syncRunId ? ` (${syncRunId})` : ''} の push の証跡が無い (push が走っていない = 上流の取込が失敗した・見送った、か証跡を書けなかった)`);
  // 🚨 同じ実行の証跡だけを採用する: 同じ日の手動実行 (ID なし) や別の回の証跡で pass にしない (Codex R1 High)。dry-run で ID が無いときだけ、今日の証跡を「結びつけずに」読む
  if (!unbound && !syncRunId) return done('blocked', '実行 ID (DAILY_SYNC_RUN_ID) が無い = どの回の証跡か結びつけられない');
  if (!unbound && (ev.sync_run_id || null) !== syncRunId) return done('blocked', `別の実行の証跡 (証跡 ${ev.sync_run_id || 'ID なし = 手動'} / 今朝 ${syncRunId})`);
  if (ev.mode && ev.mode !== 'incremental') return done('blocked', `証跡の mode が ${ev.mode} (範囲を流した回 = 全対象を走査していない)`);
  o.observed = { sync_run_id: ev.sync_run_id || null, bound: !unbound, run_id: ev.run_id ?? null, batch_seq: ev.batch_seq ?? null, scanned: ev.scanned ?? null, in_scope: ev.in_scope ?? null, changed: ev.changed ?? null, applied: ev.applied ?? null, same: ev.same ?? null, stale: ev.stale ?? null, failed: ev.failed ?? null, transform_errors: ev.transform_errors ?? null, skipped: ev.skipped || null, error: ev.error || null, started_at: ev.started_at || null };
  o.inputGeneration = { evidence_written_at: ev.written_at || null, run_id: ev.run_id ?? null, batch_seq: ev.batch_seq ?? null };
  if (ev.error && !ev.run_id && ev.ok === false) return done('breach', `push が落ちた: ${String(ev.error).slice(0, 120)}`);
  if (ev.skipped) return done('blocked', `送り手が見送った (${ev.skipped})`);
  if (ev.scope && ev.scope !== scope) return done('breach', `scope が違う (証跡 ${ev.scope} / 期待 ${scope})`);
  if (ev.locked) return done('blocked', '別の送り手が走っていて見送った');
  const bad = [];
  if (ev.push_ok === false) bad.push('push_ok=false');
  if ((ev.failed ?? 0) > 0) bad.push(`failed ${ev.failed}`);
  if ((ev.stale ?? 0) > 0) bad.push(`stale ${ev.stale}`);
  if ((ev.transform_errors ?? 0) > 0) bad.push(`整形できない ${ev.transform_errors}`);
  if (bad.length) return done('breach', `push に失敗がある (${bad.join(' / ')})`);
  if (!Number.isInteger(ev.changed)) return done('blocked', '証跡の形が違う (changed が無い)');
  if (ev.changed === 0) {
    // 🚨 変更ゼロの朝は chunk を送らない = Render に run が無い。走査が完了して変わった注文が 0 だった、を証跡で認める (設計 09 §2.1 の契約)
    o.observed.contract = 'zero_change'; return done('pass', '変わった注文 0 (走査は完了)');
  }
  if (!ev.run_id) return done('blocked', '送ったのに run_id が無い');
  const run = await oneOf(db, `select status, complete, rows_seen, rows_inserted, finished_at::text as finished_at from ops.ingest_runs where ingest_run_id = $1`, [ev.run_id]);
  if (!run) return done('breach', `Render に run ${ev.run_id} が無い`);
  o.observed.run = run;
  if (run.status !== 'success' || run.complete !== true) return done('breach', `Render の run が ${run.status}${run.complete ? '' : ' (未完)'} (🚨 complete は partial でも立つ = status も見る)`);
  return done('pass', null);
}

// ── W7 注文の取込の完了 (証跡 + 送信があれば ops.ingest_runs)
export async function evalW7(ctx, check) {
  const { db, config, evidence, syncRunId = null, unbound = false } = ctx;
  const out = [];
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const r = base(check, scopeKey, { threshold: { push_ok: true, failed: 0, stale: 0, transform_errors: 0, mode: 'incremental', sync_run_id: syncRunId } });
    const j = await judgePush(db, evidence[`orders-${m.mall}`] || null, { scope: m.scope, syncRunId, unbound });
    r.verdict = j.verdict; r.reason = j.reason; r.observed = j.observed; r.inputGeneration = j.inputGeneration;
    out.push(r);
  }
  return out;
}

// ── W9 売上日次の公開 (前提 = W7 の同じ mall)
export async function evalW9(ctx, check) {
  const { db, config, evidence, asOf } = ctx;
  const out = [];
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const ev = evidence[`orders-${m.mall}`] || null;
    const from = addDays(asOf, -config.W9_LOOKBACK_DAYS), to = addDays(asOf, -1);
    const r = base(check, scopeKey, { periodFrom: from, periodTo: to, threshold: { session_open: false, gaps: 0 } });
    if (ev && ev.sales && ev.sales.skipped === 'not_migrated') { r.verdict = 'blocked'; r.reason = '0021 (売上日次) が未適用'; out.push(r); continue; }
    const st = await oneOf(db, `select session_id, session_started_at::text as session_started_at, watermark::text as watermark from mart.sales_daily_state where company_id = $1::smallint and mall = $2 and scope_key = $3`, [config.COMPANY_ID, m.mall, m.scope]);
    if (!st) { r.verdict = 'blocked'; r.reason = '売上日次の状態が無い (まだ一度も作っていない)'; out.push(r); continue; }
    // 注文のある日は公開されているか (注文ゼロの日は公開行が無いのが正 = 0 件として扱う)
    const days = await rowsOf(db, `with d as (select generate_series($4::date, $5::date, interval '1 day')::date as day)
      select d.day::text as day,
             exists (select 1 from core.orders o where o.company_id = $1::smallint and o.mall = $2 and o.scope_key = $3 and o.order_date_jst = d.day) as has_orders,
             exists (select 1 from mart.sales_daily_published p where p.company_id = $1::smallint and p.mall = $2 and p.scope_key = $3 and p.date_jst = d.day) as published
        from d order by d.day`, [config.COMPANY_ID, m.mall, m.scope, from, to]);
    const gaps = days.filter((d) => d.has_orders && !d.published).map((d) => d.day);
    r.observed = { session_id: st.session_id, session_started_at: st.session_started_at, watermark: st.watermark, days: days.map((d) => `${d.day.slice(5)}:${d.has_orders ? (d.published ? 'pub' : 'GAP') : 'zero'}`), gaps, changed: ev ? ev.changed ?? null : null, sales: ev ? ev.sales : null };
    r.inputGeneration = { watermark: st.watermark };
    const bad = [];
    if (ev && ev.sales && ev.sales.ok === false) bad.push(`作り直しに失敗 (${String(ev.sales.error || '').slice(0, 80)})`);
    if (st.session_id) bad.push(`回 (session) が開いたまま (${String(st.session_started_at).slice(0, 16)} から)`);
    if (ev && Number.isInteger(ev.changed) && ev.changed > 0 && ev.started_at) {
      const wm = st.watermark ? Date.parse(st.watermark) : NaN;
      const need = Date.parse(ev.started_at) - config.W9_WATERMARK_SLACK_MINUTES * 60000;
      if (!(wm >= need)) bad.push(`変わった注文 ${ev.changed} 件を送ったのに watermark が進んでいない (${st.watermark || 'null'})`);
    }
    if (gaps.length) bad.push(`注文があるのに公開されていない日 ${gaps.length} (${gaps.map((g) => g.slice(5)).join(', ')})`);
    r.verdict = bad.length ? 'breach' : 'pass';
    if (bad.length) r.reason = bad.join(' / ');
    out.push(r);
  }
  return out;
}

// ── W5 解決できない在庫の差 (昨日の差で SKU が分からなかった商品コード。前提 = W3)
const DIFF_J = `
  with a as (select source_code, sku_id, qty from snapshots.sku_stock_daily where snapshot_date = $1::date and source = $3 and scope_key = $4 and company_id = $5::smallint),
       b as (select source_code, sku_id, qty from snapshots.sku_stock_daily where snapshot_date = $2::date and source = $3 and scope_key = $4 and company_id = $5::smallint),
       j as (select a.sku_id as sa, b.sku_id as sb, coalesce(a.qty, 0)::bigint as qa, coalesce(b.qty, 0)::bigint as qb from a full join b using (source_code))`;   // stock-diff.mjs と同じ結び方 (商品コードの粒度)
export async function evalW5(ctx, check) {
  const { db, config, asOf } = ctx;
  const d = config.STOCK_DIFF, scopeKey = scopeKeyOf(d.source, d.scope), day = addDays(asOf, -1);
  const r = base(check, scopeKey, { periodFrom: day, periodTo: day, threshold: { unresolved_max: config.W5_MAX_UNRESOLVED, share_max: config.W5_MAX_UNRESOLVED_SHARE, escalate_days: config.W5_ESCALATE_DAYS } });
  if (!(await oneOf(db, `select to_regclass('snapshots.stock_diff_days') is not null as ok`, [])).ok) { r.verdict = 'blocked'; r.reason = '0022 (snapshots.stock_diff_days) が未適用'; return [r]; }
  // 昨日から数えて done の日を最大 escalate_days 日 (skipped が挟まれば連続は切れる = その日で止める)
  const rows = await rowsOf(db, `select to_date::text as to_date, from_date::text as from_date, status, skip_reason, events, unresolved_changed from snapshots.stock_diff_days
    where source = $1 and scope_key = $2 and calc_version = $3 and company_id = $4::smallint and to_date <= $5::date order by to_date desc limit $6`, [d.source, d.scope, d.calcVersion, config.COMPANY_ID, day, config.W5_ESCALATE_DAYS]);
  const yesterday = rows[0] && rows[0].to_date === day ? rows[0] : null;
  r.observed = { day, calc_version: d.calcVersion, status: yesterday ? yesterday.status : 'absent' };
  if (!yesterday) { r.verdict = 'blocked'; r.reason = `昨日 (${day}) の差の印が無い (W3 が見る)`; return [r]; }
  if (yesterday.status !== 'done') { r.verdict = 'pass'; r.reason = `skipped (${yesterday.skip_reason}) = 差を作っていない日 (W2 で見る)`; return [r]; }
  const days = [];
  for (const x of rows) {
    if (x.status !== 'done') break;   // 連続は done の日だけ
    if (days.length && addDays(days[days.length - 1].to_date, -1) !== x.to_date) break;   // 日付が飛んでいれば切る
    const q = await oneOf(db, `${DIFF_J} select count(*) filter (where sa is null and sb is null and qa <> qb)::int as unresolved_codes, count(*) filter (where qa <> qb)::int as changed_codes,
        coalesce(sum(abs(qb - qa)) filter (where sa is null and sb is null), 0)::bigint as unresolved_qty, coalesce(sum(abs(qb - qa)), 0)::bigint as changed_qty from j`, [x.from_date, x.to_date, d.source, d.scope, config.COMPANY_ID]);
    const share = Number(q.changed_qty) > 0 ? Number(q.unresolved_qty) / Number(q.changed_qty) : 0;
    const over = Number(x.unresolved_changed) > config.W5_MAX_UNRESOLVED || share > config.W5_MAX_UNRESOLVED_SHARE;
    const mismatch = Number(x.unresolved_changed) !== Number(q.unresolved_codes);   // 印 (差を作ったとき) と今の日次が食い違う = 日次が後から変わった・印が古い
    if (mismatch && days.length) break;   // 過去の日の食い違いは連続を切るだけ (昨日の食い違いは下で blocked)
    days.push({ to_date: x.to_date, from_date: x.from_date, unresolved_changed: Number(x.unresolved_changed), unresolved_codes_now: Number(q.unresolved_codes), changed_codes: Number(q.changed_codes), unresolved_qty: Number(q.unresolved_qty), changed_qty: Number(q.changed_qty), share: Math.round(share * 10000) / 10000, over, mismatch });
  }
  const y = days[0];
  if (y.mismatch) {
    // 🚨 印の件数と今の日次から数えた件数が違う = どちらかが古い。混ぜて pass にしない (差を作り直す = README「締めをやり直す」)
    r.observed = { ...r.observed, ...y }; r.inputGeneration = { from: y.from_date, to: y.to_date };
    r.verdict = 'blocked'; r.reason = `差の印 (${y.unresolved_changed} 件) と今の日次 (${y.unresolved_codes_now} 件) が食い違う = 印が古いか日次が変わった。締めをやり直す (README「在庫を毎時写す」)`;
    return [r];
  }
  let streak = 0; for (const x of days) { if (!x.over) break; streak++; }
  r.observed = { ...r.observed, ...y, streak, days: days.map((x) => `${x.to_date.slice(5)}:${x.unresolved_changed}/${x.changed_codes} ${(x.share * 100).toFixed(1)}%${x.over ? ' over' : ''}`) };
  r.inputGeneration = { from: y.from_date, to: y.to_date };
  r.sampleSize = y.changed_codes;
  if (!y.over) { r.verdict = 'pass'; return [r]; }
  r.verdict = 'breach';
  r.severity = streak >= config.W5_ESCALATE_DAYS ? 'warn' : check.severity;   // 続けば重さを上げる (info → warn)
  r.reason = `SKU が分からない商品コード ${y.unresolved_changed} 件 (上限 ${config.W5_MAX_UNRESOLVED}) / 数量の ${(y.share * 100).toFixed(1)}% (上限 ${config.W5_MAX_UNRESOLVED_SHARE * 100}%)${streak >= config.W5_ESCALATE_DAYS ? ` — ${streak} 日連続` : ''}`;
  return [r];
}

// ── W6 売れ筋 SKU の欠品 (前提 = W1 の全部 + W7 の全部 + 窓の中の売上日次が公開済み)。案件は SKU ごと
// 販売 = 公開済みの売上日次 (v_sales_daily) の正味数量 (取消を引く)。SKU が直接分かる明細 + セット (sku_id null・listing あり) は listing_components で構成 SKU × 数量に展開
/** W6 の販売: 元の販売行 (sid) → 出品の構成 → NE のセット構成 を末端 (単品) まで展開する。展開しきれない経路のある元の販売行は bad (元の数量で 1 回だけ「展開できない販売」に数える) */
export const W6_MAX_SET_DEPTH = 5;
const W6_SOLD = `
  with recursive s as (select row_number() over (order by date_jst, listing_id, sku_id) as sid, listing_id, sku_id, (units_ordered - units_cancelled)::bigint as units, date_jst from mart.v_sales_daily
              where company_id = $1::smallint and date_jst between $2::date and $3::date and units_ordered - units_cancelled > 0),
       u0 as (select sid, sku_id, units, date_jst from s where sku_id is not null
             union all select s.sid, c.sku_id, s.units * c.qty, s.date_jst from s join core.listing_components c on c.company_id = $1::smallint and c.listing_id = s.listing_id where s.sku_id is null),
       -- 🚨 NE のセット商品の SKU (sku_kind = 'set') は在庫を持たない (在庫は構成品側) → core.sku_components で末端 (単品) まで構成品 × 数量に展開する (入れ子のセットも。循環は path で止める。Codex #1433 R1)
       x as (select u0.sid, u0.sku_id, u0.units, u0.date_jst, 0 as depth, array[u0.sku_id] as path from u0
             union all select x.sid, sc.child_sku_id, x.units * sc.qty, x.date_jst, x.depth + 1, x.path || sc.child_sku_id
               from x join core.skus k on k.company_id = $1::smallint and k.sku_id = x.sku_id and k.sku_kind = 'set'
                      join core.sku_components sc on sc.company_id = $1::smallint and sc.parent_sku_id = x.sku_id
              where x.depth < ${W6_MAX_SET_DEPTH} and not (sc.child_sku_id = any(x.path))),
       node as (select x.*, k.sku_kind, exists (select 1 from core.sku_components sc where sc.company_id = $1::smallint and sc.parent_sku_id = x.sku_id) as has_comp,
                       exists (select 1 from core.sku_components sc where sc.company_id = $1::smallint and sc.parent_sku_id = x.sku_id and sc.child_sku_id = any(x.path)) as has_cycle
                  from x join core.skus k on k.company_id = $1::smallint and k.sku_id = x.sku_id),
       u as (select sku_id, units, date_jst from node where sku_kind <> 'set'),
       -- 展開しきれない元の販売行 = 出品に当たらない・出品の構成が無い / 構成の無いセット / 深すぎる・循環するセット
       bad as (select sid from s where sku_id is null and (listing_id is null or not exists (select 1 from core.listing_components c where c.company_id = $1::smallint and c.listing_id = s.listing_id))
               union select sid from node where sku_kind = 'set' and (not has_comp or has_cycle or depth >= ${W6_MAX_SET_DEPTH})),
       sold as (select sku_id, sum(units)::bigint as units, count(distinct date_jst)::int as days, max(date_jst)::text as last_day from u group by sku_id)`;   // 日付を持ったまま合算 = 販売日数は直接 + セットの和集合
export async function evalW6(ctx, check) {
  const { db, config, asOf, openIssues = [] } = ctx;
  const scopeKey = scopeKeyOf('all', config.W6_SCOPE.scope), to = addDays(asOf, -1), from = addDays(to, -(config.W6_SALES_DAYS - 1));
  const r = base(check, scopeKey, { periodFrom: from, periodTo: to, threshold: { stock: 0, sales_days: config.W6_SALES_DAYS, unexpanded_share_max: config.W6_MAX_UNEXPANDED_SHARE }, severity: asOf < config.W6_INFO_UNTIL ? 'info' : check.severity });
  // 在庫の view は「complete な scope が 1 つも無ければ null (不明)」= null を 0 と読まない
  const st = await oneOf(db, `select max(warehouse_as_of)::text as w_as_of, max(fba_jp_as_of)::text as f_as_of, count(*)::int as skus,
      count(*) filter (where warehouse_as_of is null)::int as w_null, count(*) filter (where fba_jp_as_of is null)::int as f_null from mart.v_sku_stock where company_id = $1::smallint`, [config.COMPANY_ID]);
  r.observed = { sales_from: from, sales_to: to, warehouse_as_of: st.w_as_of, fba_jp_as_of: st.f_as_of, skus: st.skus };
  if (!st.skus) { r.verdict = 'blocked'; r.reason = 'SKU が 1 つも無い (core.skus)'; return [r]; }
  if (st.w_null || st.f_null) { r.verdict = 'blocked'; r.reason = `在庫が不明 (complete な日が無い: ${st.w_null ? '倉庫 ' : ''}${st.f_null ? 'FBA JP' : ''})`; return [r]; }
  // 🚨 販売履歴の完全性: 窓の中に「注文があるのに未公開の日」や開いた session があれば、未公開の売上を「売れていない」と読まない = blocked
  const malls = config.ORDER_MALLS.map((m) => m.mall), scopes = config.ORDER_MALLS.map((m) => m.scope);
  const gaps = await rowsOf(db, `with d as (select generate_series($2::date, $3::date, interval '1 day')::date as day), m as (select unnest($4::text[]) as mall, unnest($5::text[]) as scope)
    select m.mall || '/' || m.scope || ' ' || d.day::text as k from m, d
     where exists (select 1 from core.orders o where o.company_id = $1::smallint and o.mall = m.mall and o.scope_key = m.scope and o.order_date_jst = d.day)
       and not exists (select 1 from mart.sales_daily_published p where p.company_id = $1::smallint and p.mall = m.mall and p.scope_key = m.scope and p.date_jst = d.day)
     order by 1`, [config.COMPANY_ID, from, to, malls, scopes]);
  const openSessions = await rowsOf(db, `select mall || '/' || scope_key as k from mart.sales_daily_state where company_id = $1::smallint and session_id is not null and mall = any($2::text[]) order by 1`, [config.COMPANY_ID, malls]);
  r.observed.unpublished_days = gaps.length; r.observed.open_sessions = openSessions.map((x) => x.k);
  if (gaps.length || openSessions.length) {
    r.verdict = 'blocked';
    r.reason = `売上日次が窓の全部で公開されていない (${gaps.length ? `注文があるのに未公開の日 ${gaps.length}: ${gaps.slice(0, 3).map((x) => x.k).join(', ')}${gaps.length > 3 ? ' ほか' : ''}` : ''}${gaps.length && openSessions.length ? ' / ' : ''}${openSessions.length ? `開いた session: ${openSessions.map((x) => x.k).join(', ')}` : ''}) = 未公開の売上を「売れていない」と読まない`;
    return [r];
  }
  // SKU に展開できない正味の販売 (listing にも当たらない・構成が無い) の割合。多ければ「販売履歴が不完全」= blocked。少なければ観測に残す
  const un = await oneOf(db, `${W6_SOLD} select (select coalesce(sum(units), 0) from s) as total_units, (select count(*) from sold) as sold_skus,
      (select coalesce(sum(s.units), 0) from s where s.sid in (select sid from bad)) as unexpanded_units, (select count(*) from (select distinct sid from bad) z) as unexpanded_rows,
      (select count(distinct sku_id) from node where sku_kind = 'set' and not has_comp) as sets_without_components`, [config.COMPANY_ID, from, to]);
  const unexpandedShare = Number(un.total_units) > 0 ? Number(un.unexpanded_units) / Number(un.total_units) : 0;
  r.observed.sold_skus = Number(un.sold_skus); r.observed.total_units = Number(un.total_units); r.observed.unexpanded_units = Number(un.unexpanded_units); r.observed.unexpanded_rows = Number(un.unexpanded_rows); r.observed.sets_without_components = Number(un.sets_without_components); r.observed.unexpanded_share = Math.round(unexpandedShare * 10000) / 10000;
  r.sampleSize = Number(un.sold_skus);
  if (unexpandedShare > config.W6_MAX_UNEXPANDED_SHARE) { r.verdict = 'blocked'; r.reason = `SKU に展開できない販売が正味数量の ${(unexpandedShare * 100).toFixed(1)}% (上限 ${config.W6_MAX_UNEXPANDED_SHARE * 100}%) = 販売履歴が不完全 (出品と SKU の紐付け = product-hub)`; return [r]; }
  const rows = await rowsOf(db, `${W6_SOLD}
    select s.sku_id, k.code, k.name, k.handling, s.units, s.days, s.last_day, v.warehouse_qty, v.warehouse_allocated_qty, v.fba_jp_available, v.fba_jp_inbound
      from sold s join mart.v_sku_stock v on v.company_id = $1::smallint and v.sku_id = s.sku_id join core.skus k on k.company_id = $1::smallint and k.sku_id = s.sku_id
     where coalesce(v.warehouse_qty, 0) + coalesce(v.fba_jp_available, 0) = 0 and k.handling <> 'discontinued' and k.sku_kind <> 'set'   -- セットの中のセット (構成品がセット) も在庫を持たない = 判定しない
     order by s.units desc, s.sku_id`, [config.COMPANY_ID, from, to]);
  r.items = rows.map((x) => ({ subjectType: 'sku', subjectKey: String(x.sku_id), payload: { code: x.code, name: String(x.name || '').slice(0, 60), units: Number(x.units), days_sold: x.days, last_sold: x.last_day, warehouse_qty: x.warehouse_qty, warehouse_allocated_qty: x.warehouse_allocated_qty, fba_jp_available: x.fba_jp_available, fba_jp_inbound: x.fba_jp_inbound, weight: Number(x.units) } }));
  r.itemTotal = rows.length;
  r.observed.out_of_stock = rows.length; r.observed.top = rows.slice(0, 3).map((x) => `${x.code} (${x.units})`);
  r.inputGeneration = { warehouse_as_of: st.w_as_of, fba_jp_as_of: st.f_as_of };
  // 🚨 open の案件 (SKU) が今回の明細に無いとき、「在庫が入った (回復)」と「監視対象外 (廃番・窓から外れた = 在庫は 0 のまま)」を分ける (engine は outOfScope を回復にしない)
  const inItems = new Set(r.items.map((i) => i.subjectKey));
  const mine = openIssues.filter((i) => i.check_id === check.id && i.scope_key === scopeKey && i.subject_type === 'sku' && !inItems.has(i.subject_key)).map((i) => i.subject_key);
  if (mine.length) {
    const ex = await rowsOf(db, `${W6_SOLD} select k.sku_id::text as sku_id, k.handling, k.sku_kind, coalesce(v.warehouse_qty, 0) + coalesce(v.fba_jp_available, 0) as stock, exists (select 1 from sold where sold.sku_id = k.sku_id) as sold_now
        from core.skus k join mart.v_sku_stock v on v.company_id = k.company_id and v.sku_id = k.sku_id where k.company_id = $1::smallint and k.sku_id = any($4::bigint[])`, [config.COMPANY_ID, from, to, mine]);
    r.outOfScope = {};
    for (const x of ex) {
      if (x.sku_kind === 'set') r.outOfScope[x.sku_id] = 'set_sku';   // セットは在庫を持たない = 判定の対象ではない (回復ではない)
      else if (Number(x.stock) === 0) r.outOfScope[x.sku_id] = x.handling === 'discontinued' ? 'discontinued' : 'no_sales_in_window';
    }
    for (const id of mine) if (!ex.some((x) => x.sku_id === id)) r.outOfScope[id] = 'sku_missing';
    r.observed.out_of_scope = r.outOfScope;
  }
  r.verdict = rows.length ? 'breach' : 'pass';
  if (rows.length) r.reason = `売れ筋 ${un.sold_skus} SKU のうち在庫 0 が ${rows.length} (${r.observed.top.join(', ')}${rows.length > 3 ? ' ほか' : ''})`;
  return [r];
}

// ── W8 注文の日次の異常 (モール × 昨日。前提 = W7・W9 の同じモール)
const median = (xs) => { if (!xs.length) return null; const a = [...xs].sort((p, q) => p - q); const m = a.length >> 1; return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2; };
const mad = (xs, med) => (xs.length ? median(xs.map((x) => Math.abs(x - med))) : null);
const r4 = (x) => (x == null ? null : Math.round(x * 10000) / 10000);
/** W8 が読む日 = 昨日 + 同じ曜日の過去 N 週 (指紋も同じ日を読む) */
export function w8Days(config, asOf) {
  const day = addDays(asOf, -1);
  return { day, baseline: Array.from({ length: config.W8_BASELINE_WEEKS }, (_, i) => addDays(day, -7 * (i + 1))) };
}
export async function evalW8(ctx, check) {
  const { db, config, asOf } = ctx;
  const out = [];
  const { day, baseline } = w8Days(config, asOf);
  const allDays = [day, ...baseline];
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const r = base(check, scopeKey, { periodFrom: day, periodTo: day, severity: asOf < config.W8_INFO_UNTIL ? 'info' : check.severity,
      threshold: { mad_k: config.W8_MAD_K, min_abs_orders: config.W8_MIN_ABS_ORDERS, min_abs_sales_jpy: config.W8_MIN_ABS_SALES_JPY, min_rate_delta: config.W8_MIN_RATE_DELTA, min_samples: config.W8_MIN_SAMPLES, small_mall_orders_per_day: config.W8_SMALL_MALL_ORDERS_PER_DAY, small_max_cancel_rate: config.W8_SMALL_MAX_CANCEL_RATE, small_max_unknown_rate: config.W8_SMALL_MAX_UNKNOWN_RATE } });
    const first = await oneOf(db, `select order_date_jst::text as d from core.orders where company_id = $1::smallint and mall = $2 and scope_key = $3 order by order_date_jst limit 1`, [config.COMPANY_ID, m.mall, m.scope]);
    const counts = await rowsOf(db, `select order_date_jst::text as d, count(*)::int as n, count(*) filter (where is_cancelled)::int as c from core.orders
      where company_id = $1::smallint and mall = $2 and scope_key = $3 and order_date_jst = any($4::text[]::date[]) group by 1`, [config.COMPANY_ID, m.mall, m.scope, allDays]);
    const sales = await rowsOf(db, `select date_jst::text as d, sum(sales_jpy)::bigint as s, sum(lines)::int as l, sum(lines_amount_unknown)::int as u from mart.v_sales_daily
      where company_id = $1::smallint and mall = $2 and scope_key = $3 and date_jst = any($4::text[]::date[]) group by 1`, [config.COMPANY_ID, m.mall, m.scope, allDays]);
    const pubRows = await rowsOf(db, `select date_jst::text as d from mart.sales_daily_published where company_id = $1::smallint and mall = $2 and scope_key = $3 and date_jst = any($4::text[]::date[])`, [config.COMPANY_ID, m.mall, m.scope, allDays]);
    // 取込の完了の証跡 = 翌朝 (as_of = D+1) の見張りで同じ scope の W7 が pass (同じ日に見張りが 2 回あれば最後の回の判定)。翌々朝の pass は D の証跡ではない (D+1 に失敗した D が直った証明にならない)
    //   🚨 ops.ingest_runs の success・complete は証跡にしない (Codex R3 High): 「届いた chunk の処理が済んだ」であって走査の完了ではない (整形に失敗した注文を飛ばして残りを送っても success。--from/--to の手動 push も同じ形)
    const evidenceDays = baseline.map((d) => addDays(d, 1));
    const w7Rows = await rowsOf(db, `select z.d from (select distinct on (r.as_of_date) r.as_of_date::text as d, x.verdict from ops.watch_results x join ops.watch_runs r on r.watch_run_id = x.watch_run_id
      where x.company_id = $1::smallint and x.check_id = 'W7' and x.scope_key = $2 and r.as_of_date = any($3::text[]::date[]) order by r.as_of_date, r.started_at desc, x.watch_result_id desc) z where z.verdict = 'pass'`, [config.COMPANY_ID, scopeKey, evidenceDays]);
    const evidence = new Set(w7Rows.map((x) => x.d));
    const verified = (d) => (m.ordersSince && m.reconciledThrough && d >= m.ordersSince && d <= m.reconciledThrough) || evidence.has(addDays(d, 1));
    const cnt = new Map(counts.map((x) => [x.d, x])), sal = new Map(sales.map((x) => [x.d, x])), pubSet = new Set(pubRows.map((x) => x.d));
    const at = (d) => { const c = cnt.get(d) || { n: 0, c: 0 }; const s = sal.get(d) || { s: 0, l: 0, u: 0 }; return { d, orders: Number(c.n), cancelled: Number(c.c), sales: Number(s.s), lines: Number(s.l), unknown: Number(s.u), cancel_rate: Number(c.n) ? Number(c.c) / Number(c.n) : 0, unknown_rate: Number(s.l) ? Number(s.u) / Number(s.l) : 0 }; };
    const y = at(day);
    // 🚨 平常の標本の完全性 (Codex #1412 R1/R2/R3): 「行が無い = 0」「少ない件数」を黙って平常に混ぜない
    //   ① 取込の完了が確かめられない日 (突合済みの範囲の外で、翌朝の W7 pass も無い) は除外 (unverified)。ゼロの日も少ない日も同じ扱い
    //   ② 注文があるのに売上日次が未公開の日は除外 (unpublished。売上 0 として平常を下に引かない)
    const samples = [], excluded = [];
    const holidays = new Set(config.NON_BUSINESS_DAYS || []);
    for (const d of baseline) {
      const x = at(d);
      if (holidays.has(d)) { excluded.push({ d, reason: 'non_business_day' }); continue; }   // 祝日・年末年始は平日と比べない (9/22 のシルバーウィーク)
      if (!verified(d)) { excluded.push({ d, reason: 'unverified' }); continue; }
      if (x.orders > 0 && !pubSet.has(d)) { excluded.push({ d, reason: 'unpublished' }); continue; }
      samples.push(x);
    }
    r.observed = { day, first_order_day: first ? first.d : null, orders_since: m.ordersSince || null, reconciled_through: m.reconciledThrough || null, evidence_days: [...evidence].sort(), published: pubSet.has(day), yesterday: y, samples: samples.length, excluded: excluded.map((e) => `${e.d.slice(5)}:${e.reason}`), baseline: samples.map((s) => `${s.d.slice(5)}:${s.orders}/${s.sales}/${r4(s.cancel_rate)}/${r4(s.unknown_rate)}`) };
    r.sampleSize = samples.length;
    r.inputGeneration = { day, baseline_days: samples.map((s) => s.d) };
    if (config.NON_BUSINESS_DAYS_UNTIL && day > config.NON_BUSINESS_DAYS_UNTIL) { r.verdict = 'blocked'; r.reason = `祝日の一覧 (NON_BUSINESS_DAYS) が ${config.NON_BUSINESS_DAYS_UNTIL} までしか無い = 翌年の分を足して NON_BUSINESS_DAYS_UNTIL を延ばす (config/watch-checks.mjs)`; out.push(r); continue; }
    if (holidays.has(day)) { r.verdict = 'blocked'; r.reason = `昨日 (${day}) は祝日・年末年始 = 平日と比べない`; out.push(r); continue; }
    if (samples.length < config.W8_MIN_SAMPLES) { r.verdict = 'blocked'; r.reason = `有効標本 ${samples.length} < ${config.W8_MIN_SAMPLES} (除外 ${excluded.length}: ${excluded.slice(0, 3).map((e) => `${e.d.slice(5)} ${e.reason}`).join(', ')}${excluded.length > 3 ? ' ほか' : ''}。最初の注文 ${first ? first.d : 'なし'}。平常が決まらない)`; out.push(r); continue; }
    if (y.orders > 0 && !pubSet.has(day)) { r.verdict = 'blocked'; r.reason = `昨日 (${day}) に注文があるのに売上日次が未公開 (W9 が見る)`; out.push(r); continue; }
    const st = (key) => { const xs = samples.map((s) => s[key]); const med = median(xs); return { med, mad: mad(xs, med) }; };
    const so = st('orders'), ss = st('sales'), sc = st('cancel_rate'), su = st('unknown_rate');
    const small = so.med < config.W8_SMALL_MALL_ORDERS_PER_DAY;
    r.observed.stats = { small, orders: { median: so.med, mad: so.mad }, sales: { median: ss.med, mad: ss.mad }, cancel_rate: { median: r4(sc.med), mad: r4(sc.mad) }, unknown_rate: { median: r4(su.med), mad: r4(su.mad) } };
    const bad = [];
    if (y.orders === 0 && so.med > 0) bad.push(`昨日の注文 0 件 (平常の中央値 ${so.med})`);
    if (!small) {
      const dOrders = y.orders - so.med, dSales = y.sales - ss.med;
      if (y.orders > 0 && Math.abs(dOrders) > config.W8_MAD_K * so.mad && Math.abs(dOrders) >= config.W8_MIN_ABS_ORDERS) bad.push(`件数 ${y.orders} (平常 ${so.med} ± ${r4(config.W8_MAD_K * so.mad)})`);
      if (Math.abs(dSales) > config.W8_MAD_K * ss.mad && Math.abs(dSales) >= config.W8_MIN_ABS_SALES_JPY) bad.push(`売上 ${y.sales.toLocaleString()} 円 (平常 ${ss.med.toLocaleString()} ± ${Math.round(config.W8_MAD_K * ss.mad).toLocaleString()})`);
      if (y.cancel_rate > sc.med + Math.max(config.W8_MAD_K * sc.mad, config.W8_MIN_RATE_DELTA)) bad.push(`取消率 ${r4(y.cancel_rate * 100)}% (平常 ${r4(sc.med * 100)}%)`);
      if (y.unknown_rate > su.med + Math.max(config.W8_MAD_K * su.mad, config.W8_MIN_RATE_DELTA)) bad.push(`金額不明の明細 ${r4(y.unknown_rate * 100)}% (平常 ${r4(su.med * 100)}%)`);
    } else {
      if (y.cancel_rate > config.W8_SMALL_MAX_CANCEL_RATE) bad.push(`取消率 ${r4(y.cancel_rate * 100)}% (上限 ${config.W8_SMALL_MAX_CANCEL_RATE * 100}%)`);
      if (y.unknown_rate > config.W8_SMALL_MAX_UNKNOWN_RATE) bad.push(`金額不明の明細 ${r4(y.unknown_rate * 100)}% (上限 ${config.W8_SMALL_MAX_UNKNOWN_RATE * 100}%)`);
    }
    r.verdict = bad.length ? 'breach' : 'pass';
    if (bad.length) r.reason = `${day}: ${bad.join(' / ')}${small ? ' (小規模 = 統計の判定なし)' : ''}`;
    else if (small) r.reason = `小規模モール (平常 ${so.med} 件/日) = 統計の判定なし`;
    out.push(r);
  }
  return out;
}

// ── W10 回復していない取込の異常 (ops.ingest_runs。前提なし)。案件は取込の種類ごとに 1 つ・未回復の run は明細
export const w10KindKey = (k) => `${k.source}.${k.entity}/${k.scope}`;
export const W10_OTHER = 'other/*';
const w10Match = (list, run) => list.find((k) => k.source === run.source_system && k.entity === run.entity && k.scope === run.scope_key) || null;
/** 悪い run (failed / partial / 止まった running)。since = started_at の JST の日。started_ms = 送信の証跡と比べる時刻 */
const W10_BAD_RUNS = `select ingest_run_id, source_system, entity, scope_key, status, complete, started_at::text as started_at, (extract(epoch from started_at) * 1000)::bigint as started_ms,
         (started_at at time zone 'Asia/Tokyo')::date::text as started_jst, finished_at::text as finished_at, rows_seen, checksum, left(coalesce(error, ''), 200) as error
    from ops.ingest_runs
   where started_at >= ($1::date::timestamp at time zone 'Asia/Tokyo')
     and (status in ('failed', 'partial') or (status = 'running' and started_at < $2::timestamptz - ($3::int * interval '1 minute')))
   order by started_at, ingest_run_id`;
/** chunk の失敗した行 (応答の failed = 全件。run の failed_ranges は 200 件で切れる) と、その行の今の世代。注文 = (モール, scope, 注文番号) / 出荷 = 伝票番号。鍵の無い要素も 1 行 (key null) で返す */
const W10_FAILED_KEYS = `
  with r as (select ingest_run_id, source_system, entity, scope_key, case when checksum ~ '^[0-9]+$' then checksum::bigint end as batch from ops.ingest_runs where ingest_run_id = any($1::text[])),
       k as (select c.ingest_run_id, nullif(f.value ->> 'key', '') as key, coalesce(f.value ->> 'mall_order_no', f.value ->> 'ne_slip_no') as no
               from ops.ingest_chunks c cross join lateral jsonb_array_elements(case when jsonb_typeof(c.result -> 'failed') = 'array' then c.result -> 'failed' else '[]'::jsonb end) f
              where c.ingest_run_id = any($1::text[]))
  select k.ingest_run_id, k.key, r.batch,
         case r.entity when 'orders' then o.received_batch_seq when 'shipments' then s.received_batch_seq end as seq
    from k join r using (ingest_run_id)
    left join core.orders o on r.entity = 'orders' and k.key is not null and o.company_id = $2::smallint and o.mall = r.source_system and o.scope_key = r.scope_key
                             and o.mall_order_no = coalesce(k.no, substr(k.key, length(r.source_system) + length(r.scope_key) + 3))
    left join core.shipments s on r.entity = 'shipments' and k.key is not null and s.company_id = $2::smallint and s.ne_slip_no = coalesce(k.no, k.key)`;
/** chunk で送る取込の、今朝の差分送信の証跡の名前と scope (送り手が書く。W7 と同じ形) */
const w10EvidenceOf = (kind) => (kind.entity === 'orders' ? { name: `orders-${kind.source}`, scope: kind.scope, w7: true } : kind.entity === 'shipments' ? { name: 'shipments', scope: kind.scope, w7: false } : null);

/** 今朝の差分送信の証跡が「daily-sync の送り手が実際に使った世代」を持つか (その世代で当たった行は正規の送り手が送った = 信頼できる世代)。W7 の判定の pass / breach は問わない (失敗があっても当たった行は本物) */
const w10Trusted = (p) => !!(p && p.ev && p.j.observed && !(p.ev.scope && p.ev.scope !== p.scope) && !p.ev.locked && !p.ev.skipped && Number.isInteger(p.ev.batch_seq) && Number.isInteger(p.ev.changed) && p.ev.changed > 0);
/** 過去の日の証跡 (miniPC に 14 日残る) の世代が信頼できるか = daily-sync の回 (実行 ID あり。手で流した回は .manual で別の名前) の差分送信で、世代を使って送った。今朝の実行 ID とは結び付けない (その日の回の証跡) */
const w10TrustedPast = (ev, scope) => !!(ev && !ev.error && ev.sync_run_id && (!ev.mode || ev.mode === 'incremental') && !(ev.scope && ev.scope !== scope) && !ev.locked && !ev.skipped && Number.isInteger(ev.batch_seq) && Number.isInteger(ev.changed) && ev.changed > 0);
/**
 * 🚨 回復の決め方 (Codex #1417 R1 / R2: 後から来た run が閉じた・行の世代が進んだ・今朝の送信が pass だった だけでは取り直しの証明にならない):
 *   keys (注文・出荷):
 *     partial = 失敗した行 (ops.ingest_chunks の result.failed = 全件) の **1 行ずつ**、今の行の世代 (received_batch_seq) が run の世代より新しく、かつ「信頼できる世代」
 *       (daily-sync の差分送信の証跡が持つ batch_seq = 今朝の証跡 + W10 が記録してきた履歴) であること = 正規の送り手がその行を送り直して当たった。
 *       別の送り手 (別の台帳・手動の再投入) が古い内容で世代だけ進めた、その行を走査しない送信が pass した、では回復にしない。鍵の読めない失敗が 1 つでもあれば回復にしない
 *     running のまま止まった run・failed = 送れなかった行は Render から見えない (outbox は鍵だけを追跡に移す・raw から消えた注文は走査されない) = **自動では回復にしない**。
 *       突合 (--reconcile) で raw と一致を確かめたら W10_ACCEPTED_RUNS に書く (run ごと、または種類 × この時刻より前)
 *     一度証明できた回復は W10 の記録 (observed.proven) に残し、翌朝の送信の成否で未回復に戻さない (Codex R2 #3)
 *   generation (ロジザード) = 同じか新しい世代の success・complete (状態の写し)
 *   capture (在庫の日次) = その run を指す取得記録の日が、今 complete / 例外つきの partial / 上書きされた (partial → complete に上がると行は新しい run を指す)。
 *     W1 / W2 の窓の中の日は W1 / W2 が見る (W10 は数えない)・監視の開始日 (STOCK_SCOPES の since) より前の日は数えない (申告済みの履歴)
 */
export async function evalW10(ctx, check) {
  const { db, config, asOf, now, evidence = {}, evidenceHistory = {}, syncRunId = null, unbound = false } = ctx;
  const severity = asOf < config.W10_INFO_UNTIL ? 'info' : check.severity;
  // 今朝の差分送信の判定 (W7 と同じ)。取り直しの証明と「W7 が扱った run」の両方に使う
  const pushes = new Map();
  for (const k of config.W10_KINDS) {
    const e = w10EvidenceOf(k); if (!e) continue;
    const ev = evidence[e.name] || null;
    const j = await judgePush(db, ev, { scope: e.scope, syncRunId, unbound });
    const p = { ...e, ev, j };
    p.trusted = w10Trusted(p);
    pushes.set(w10KindKey(k), p);
  }
  // 今朝の push の run のうち W7 が実際に判定した (pass / breach) ものだけ W7 に任せる。W7 が blocked (別の実行・範囲・見送り) なら W10 が数える (Codex R1 #3)
  const todays = new Set();
  for (const p of pushes.values()) if (p.w7 && p.ev && typeof p.ev.run_id === 'string' && (p.j.verdict === 'pass' || p.j.verdict === 'breach')) todays.add(p.ev.run_id);
  // 受け入れ = run ごと ({ runId }) か、種類 × この時刻より前 ({ kind: 'rakuten.orders/main', startedBefore: ISO }。突合で確かめた時刻)
  const acceptedIds = new Set((config.W10_ACCEPTED_RUNS || []).filter((a) => a.runId).map((a) => a.runId));
  const acceptedBefore = (config.W10_ACCEPTED_RUNS || []).filter((a) => a.kind && a.startedBefore).map((a) => ({ kind: a.kind, ms: Date.parse(a.startedBefore) }));
  const isAccepted = (run) => acceptedIds.has(run.ingest_run_id) || acceptedBefore.some((a) => a.kind === `${run.source_system}.${run.entity}/${run.scope_key}` && Number(run.started_ms) < a.ms);
  const bad = await rowsOf(db, W10_BAD_RUNS, [config.W10_SINCE, now.toISOString(), config.W10_STUCK_MINUTES]);
  const groups = new Map([...config.W10_KINDS.map((k) => [w10KindKey(k), { kind: k, runs: [] }]), [W10_OTHER, { kind: null, runs: [] }]]);
  const skipped = { todays: [], accepted: [], delegated: 0 };
  for (const run of bad) {
    if (todays.has(run.ingest_run_id)) { skipped.todays.push(run.ingest_run_id); continue; }
    if (isAccepted(run)) { skipped.accepted.push(run.ingest_run_id); continue; }
    const kind = w10Match(config.W10_KINDS, run);
    if (kind) { groups.get(w10KindKey(kind)).runs.push(run); continue; }
    if (w10Match(config.W10_DELEGATED, run)) { skipped.delegated++; continue; }
    groups.get(W10_OTHER).runs.push(run);
  }
  const out = [];
  for (const [scopeKey, g] of groups) {
    const r = base(check, scopeKey, { severity, threshold: { unrecovered: 0, since: config.W10_SINCE, stuck_minutes: config.W10_STUCK_MINUTES } });
    const items = [];
    const notCounted = [];   // 回復ではなく「W10 が数えない」(W1 / W2 の窓の中・監視の開始日より前)
    const push = pushes.get(scopeKey) || null;
    const proven = [];
    const provenBefore = new Set(), trusted = new Set();
    if (g.kind && g.kind.recovery === 'keys') {   // 悪い run が 0 件でも前の記録を読む (引き継ぐ一覧を空で上書きしない。Codex R3 Low)
      const ids = g.runs.map((x) => x.ingest_run_id);
      // これまでに証明した回復 (最後に記録した W10 の結果 = 記録した順。as_of や開始時刻の順ではない) と、信頼できる世代 (今朝 + 記録してきた履歴)
      const prev = await oneOf(db, `select x.observed -> 'proven' as p, x.observed -> 'trusted_batches' as t from ops.watch_results x
          where x.company_id = $1::smallint and x.check_id = 'W10' and x.scope_key = $2 and jsonb_typeof(x.observed -> 'proven') = 'array' order by x.watch_result_id desc limit 1`, [config.COMPANY_ID, scopeKey]);
      for (const id of prev && Array.isArray(prev.p) ? prev.p : []) provenBefore.add(String(id));
      for (const b of prev && Array.isArray(prev.t) ? prev.t : []) if (Number.isSafeInteger(Number(b))) trusted.add(Number(b));
      if (push && push.trusted) trusted.add(Number(push.ev.batch_seq));
      for (const day of Object.keys(evidenceHistory || {})) { const ev = (evidenceHistory[day] || {})[push ? push.name : '']; if (push && w10TrustedPast(ev, push.scope)) trusted.add(Number(ev.batch_seq)); }
      const keys = await rowsOf(db, W10_FAILED_KEYS, [ids, config.COMPANY_ID]);
      const byRun = new Map();
      for (const k of keys) {
        const e = byRun.get(k.ingest_run_id) || { elements: 0, keyless: 0, failed: new Set(), remaining: new Set() };
        e.elements++;
        if (k.key == null) { e.keyless++; byRun.set(k.ingest_run_id, e); continue; }
        e.failed.add(k.key);
        // 送り直しが当たった = 今の行がこの run より新しい世代 かつ 信頼できる世代 (正規の送り手の差分送信)。行が無い・世代が同じか古い・出どころの分からない世代 = 残っている
        if (k.seq == null || k.batch == null || Number(k.seq) <= Number(k.batch) || !trusted.has(Number(k.seq))) e.remaining.add(k.key);
        byRun.set(k.ingest_run_id, e);
      }
      const failedRows = new Map((await rowsOf(db, `select ingest_run_id, coalesce(sum(rows_failed), 0)::int as n from ops.ingest_chunks where ingest_run_id = any($1::text[]) group by 1`, [ids])).map((x) => [x.ingest_run_id, x.n]));
      for (const run of g.runs) {
        if (provenBefore.has(run.ingest_run_id)) { proven.push(run.ingest_run_id); continue; }   // 前に証明した = 回復のまま
        const e = byRun.get(run.ingest_run_id) || { elements: 0, keyless: 0, failed: new Set(), remaining: new Set() };
        const why = [];
        const nFailed = failedRows.get(run.ingest_run_id) || 0;
        if (!/^[0-9]+$/.test(String(run.checksum || ''))) why.push('世代 (batch_seq) が読めない');
        // 行の失敗の数と、読めた鍵の数が合わない = 応答の形が違う → 読めた鍵だけで回復にしない (Codex R1 Low)
        if (e.keyless > 0 || e.elements < nFailed) why.push(`失敗 ${nFailed} 行のうち鍵が読めない ${Math.max(nFailed - e.failed.size, e.keyless)}`);
        if (e.remaining.size) why.push(`正規の差分送信で送り直されていない行 ${e.remaining.size} / ${e.failed.size}`);
        if (run.status !== 'partial') why.push(`${run.status === 'running' ? '止まった run' : '失敗した run'} = 送れなかった行は Render から見えない (自動では回復にしない。突合 --reconcile で確かめて W10_ACCEPTED_RUNS に)`);
        if (why.length) items.push({ run, why, remaining: [...e.remaining], failed: e.failed.size });
        else proven.push(run.ingest_run_id);
      }
    } else if (g.kind && g.kind.recovery === 'generation' && g.runs.length) {
      const ids = g.runs.map((x) => x.ingest_run_id);
      const rec = new Map((await rowsOf(db, `select b.ingest_run_id, exists (select 1 from ops.ingest_runs l where l.source_system = b.source_system and l.entity = b.entity and l.scope_key = b.scope_key and l.status = 'success' and l.complete
            and ((b.checksum is not null and l.checksum >= b.checksum) or (b.checksum is null and l.started_at > b.started_at))) as ok
          from ops.ingest_runs b where b.ingest_run_id = any($1::text[])`, [ids])).map((x) => [x.ingest_run_id, x.ok]));
      for (const run of g.runs) if (!rec.get(run.ingest_run_id)) items.push({ run, why: [`${run.status === 'running' ? '止まった' : run.status} 後に同じか新しい世代の success が無い`], remaining: [], failed: 0 });
    } else if (g.kind && g.kind.recovery === 'capture' && g.runs.length) {
      const s = config.STOCK_SCOPES.find((x) => x.source === g.kind.stockSource);
      const ids = g.runs.map((x) => x.ingest_run_id);
      const days = new Map((await rowsOf(db, `select ingest_run_id, snapshot_date::text as d, status from snapshots.stock_capture_days where company_id = $1::smallint and source = $2 and scope_key = $3 and ingest_run_id = any($4::text[])`,
        [config.COMPANY_ID, g.kind.stockSource, s ? s.scope : g.kind.scope, ids])).map((x) => [x.ingest_run_id, x]));
      const w2From = s ? addDays(asOf, s.dayOffset - config.W2_WINDOW_DAYS) : null;   // W2 の窓の頭 (asOf + dayOffset − 7)。これより後 (W1 の今日まで) は W1 / W2 が見る
      for (const run of g.runs) {
        const day = days.get(run.ingest_run_id) || null;
        if (!s) { items.push({ run, why: ['STOCK_SCOPES に対応する在庫の scope が無い'], remaining: [], failed: 0 }); continue; }
        if (run.status !== 'partial') { items.push({ run, why: [`1 取引の取込なのに ${run.status} が残っている (想定外)`], remaining: [], failed: 0 }); continue; }
        if (!day) continue;   // どの日も指していない = その日は新しい run で上書きされた (partial → complete に上がった) = 回復
        if (s.since && day.d < s.since) { notCounted.push(`${run.ingest_run_id}:before_since`); continue; }
        if (day.d >= w2From) { notCounted.push(`${run.ingest_run_id}:w1_w2`); continue; }
        if (day.status === 'complete' || (day.status === 'partial' && partialAllowed(s, asOf))) continue;
        items.push({ run, why: [`${day.d} が ${day.status} のまま (W2 の窓から外れた。${s.allowPartial ? (partialAllowed(s, asOf) ? '' : '例外の期限切れ') : '例外なし'})`], remaining: [], failed: 0 });
      }
    } else if (!g.kind) {
      // 定義に無い種類 = 回復の決め方が分からない。run 自体が悪いままなら異常 (W10_KINDS か W10_DELEGATED に足す)
      for (const run of g.runs) items.push({ run, why: [`定義に無い種類 (${run.source_system}/${run.entity}/${run.scope_key}) = config/watch-checks.mjs の W10_KINDS か W10_DELEGATED に足す`], remaining: [], failed: 0 });
    }
    r.items = items.map((x) => ({ subjectType: 'ingest_run', subjectKey: x.run.ingest_run_id, payload: { kind: `${x.run.source_system}/${x.run.entity}/${x.run.scope_key}`, status: x.run.status, started_at: x.run.started_at, finished_at: x.run.finished_at, batch: x.run.checksum, why: x.why, failed_keys: x.failed, remaining_keys: x.remaining.length, remaining_sample: x.remaining.slice(0, 3), error: x.run.error || null, weight: Math.max(x.remaining.length, 1) } }));
    r.itemTotal = items.length;
    r.sampleSize = g.runs.length;
    const newest = [...items].reverse();   // 通知では新しい run から (新しい失敗が古い失敗に埋もれない。Codex R1 Low)
    r.observed = { since: config.W10_SINCE, bad_runs: g.runs.length, recovered: g.runs.length - items.length - notCounted.length, unrecovered: items.length, not_counted: notCounted,
      oldest: items.length ? items[0].run.started_at : null, newest: newest.slice(0, 3).map((x) => `${x.run.ingest_run_id}:${x.run.status}`) };
    if (push) r.observed.todays_push = { name: push.name, verdict: push.j.verdict, reason: push.j.reason, started_at: push.ev ? push.ev.started_at || null : null, run_id: push.ev ? push.ev.run_id ?? null : null, batch_seq: push.ev && Number.isInteger(push.ev.batch_seq) ? push.ev.batch_seq : null, trusted: push.trusted };
    if (g.kind && g.kind.recovery === 'keys') {
      // 次の朝に引き継ぐ: 証明できた回復 (前の一覧 + 今回。監視の範囲 W10_SINCE の外に出たものも捨てない = 範囲を戻しても証明が残る) と、
      // 信頼できる世代 (まだ証明できていない run の世代より新しいものだけ = 当たりうる世代だけ残す。見張りの記録の保持期限に左右されない)
      const unproven = g.runs.filter((x) => !proven.includes(x.ingest_run_id)).map((x) => Number(x.checksum)).filter(Number.isSafeInteger);
      const floor = unproven.length ? Math.min(...unproven) : Infinity;
      r.observed.proven = [...new Set([...provenBefore, ...proven])].sort();
      r.observed.trusted_batches = [...trusted].filter((b) => b > floor).sort((p, q) => p - q);
    }
    if (scopeKey === W10_OTHER) Object.assign(r.observed, { skipped_todays_push: skipped.todays, skipped_accepted: skipped.accepted, skipped_delegated: skipped.delegated });
    r.inputGeneration = { unrecovered: items.map((x) => x.run.ingest_run_id) };
    if (items.length) { r.periodFrom = items[0].run.started_jst; r.periodTo = items[items.length - 1].run.started_jst; }
    r.verdict = items.length ? 'breach' : 'pass';
    if (items.length) r.reason = `回復していない run ${items.length} (新しい順: ${newest.slice(0, 2).map((x) => `${x.run.ingest_run_id} ${x.run.status}: ${x.why.join(' / ')}`).join(' ; ')}${items.length > 2 ? ' ほか' : ''})`;
    out.push(r);
  }
  return out;
}

// ── W11 注文と出荷の未リンク・発送遅れ (モールごと。前提 = W7 の同じモール + 今朝の出荷の push + 結び直しが済んでいる)
/** W11 が読む注文 (自社発送・注文日 from〜to・取消でない) のうち候補だけ: A / A' = モールで出荷済み & 有効な伝票なし / B・B2 = 未発送アラートの無いモールで未発送の状態 ($6 = その状態の一覧。無ければ空) */
const W11_ROWS = `
  with o as (
    select o.order_id, o.mall_order_no, o.order_date_jst, o.status, (o.source_updated_at at time zone 'Asia/Tokyo')::date as su
      from core.orders o
     where o.company_id = $1::smallint and o.mall = $2 and o.scope_key = $3 and o.shop_code is not null
       and o.order_date_jst between $4::date and $5::date and not o.is_cancelled and o.status <> 'cancelled'),
  j as (
    select o.*, x.n_slips, x.n_active, x.last_ship
      from o cross join lateral (select count(*)::int as n_slips, count(*) filter (where not s.is_cancelled)::int as n_active,
                                        max(s.ship_date_jst) filter (where not s.is_cancelled) as last_ship
                                   from core.shipments s where s.order_id = o.order_id) x)
  select j.order_id::text as order_id, j.mall_order_no, j.order_date_jst::text as d, j.status, j.su::text as su, j.n_slips, j.n_active, j.last_ship::text as last_ship,
         case when j.n_slips = 0 and j.status in ('shipped', 'delivered', 'returned') then exists (
           select 1 from core.ne_shops n join core.shipments s on s.company_id = n.company_id and s.shop_code = n.shop_code and s.order_id is null
                                        and s.ne_order_no = substr(j.mall_order_no, length(n.order_no_prefix) + 1)
            where n.company_id = $1::smallint and n.mall = $2 and n.scope_key = $3 and left(j.mall_order_no, length(n.order_no_prefix)) = n.order_no_prefix) else false end as slip_not_linked
    from j
   where (j.n_active = 0 and j.status in ('shipped', 'delivered', 'returned')) or j.status = any($6::text[])
   order by j.order_date_jst, j.order_id`;
/** W11 が読む日の範囲 (評価と指紋で同じ) */
export const w11Range = (config, asOf) => ({ from: addDays(asOf, -config.W11_WINDOW_DAYS), to: addDays(asOf, -config.W11_LAG_DAYS) });
const w11Unshipped = (config, mall) => (config.W11_UNSHIPPED_MALLS || []).find((u) => u.mall === mall) || null;
const daysBetween = (a, b) => Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86400000);
export async function evalW11(ctx, check) {
  const { db, config, asOf, evidence = {}, syncRunId = null, unbound = false } = ctx;
  const out = [];
  const { from, to } = w11Range(config, asOf);
  // 今朝の出荷の push (NE の伝票) が確かめられないと「伝票なし」「NE で未出荷」を言えない = blocked
  const ship = await judgePush(db, evidence.shipments || null, { scope: 'main', syncRunId, unbound });
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const u = w11Unshipped(config, m.mall);
    const maxCancelledOnly = (config.W11_CANCELLED_ONLY_MAX || {})[m.mall] ?? 0;
    const r = base(check, scopeKey, { periodFrom: from, periodTo: to, severity: asOf < config.W11_INFO_UNTIL ? 'info' : check.severity,
      threshold: { a: 0, a_cancelled_only_max: maxCancelledOnly, b: 0, b2: 0, lag_days: config.W11_LAG_DAYS, b_max_days: config.W11_B_MAX_DAYS, window_days: config.W11_WINDOW_DAYS, b2_grace_days: config.W11_B2_GRACE_DAYS, unshipped_statuses: u ? u.notShipped : [], b2_enabled: !!(u && u.b2) } });
    if (ship.verdict !== 'pass') { r.verdict = 'blocked'; r.reason = `今朝の出荷の push が確かめられない (${ship.verdict}${ship.reason ? `: ${String(ship.reason).slice(0, 100)}` : ''}) = NE の伝票がそろっているか分からない`; out.push(r); continue; }
    const ev = evidence[`orders-${m.mall}`] || null;
    if (ev && ev.relink && (ev.relink.ok === false || ev.relink.pending)) { r.verdict = 'blocked'; r.reason = `伝票との結び直しが${ev.relink.ok === false ? '失敗した' : '途中 (次の push で続き)'} = 結び付いていない注文を数えられない`; out.push(r); continue; }
    const rows = await rowsOf(db, W11_ROWS, [config.COMPANY_ID, m.mall, m.scope, from, to, u ? u.notShipped : []]);
    const items = [];
    for (const x of rows) {
      let kind = null;
      const shippedAtMall = ['shipped', 'delivered', 'returned'].includes(x.status);
      if (shippedAtMall && Number(x.n_slips) === 0) kind = x.slip_not_linked ? 'A_slip_not_linked' : 'A_no_slip';
      else if (shippedAtMall && Number(x.n_active) === 0) kind = 'A_cancelled_only';   // 結び付いた伝票がキャンセルだけ (多くは同梱)
      else if (u && u.notShipped.includes(x.status)) {
        // 内容が最後に変わった取込の日から数える (注文日より後なら。住所入力・支払いが後から済んだ注文は数え直す)。🚨 状態以外の訂正でも数え直す近似 = 注文日から W11_B_MAX_DAYS 日で必ず出す (安全網)
        const since = x.su && x.su > x.d ? x.su : x.d;
        if (!x.last_ship) { if (daysBetween(since, asOf) >= config.W11_LAG_DAYS || daysBetween(x.d, asOf) >= config.W11_B_MAX_DAYS) kind = 'B_unshipped'; }
        else if (u.b2 && daysBetween(x.last_ship, asOf) >= config.W11_B2_GRACE_DAYS) kind = 'B2_mall_not_notified';
      }
      if (kind) items.push({ subjectType: 'order', subjectKey: x.order_id, payload: { kind, mall_order_no: x.mall_order_no, order_date: x.d, status: x.status, content_changed: x.su, ne_slips: Number(x.n_slips), ne_active: Number(x.n_active), ne_shipped: x.last_ship, age_days: daysBetween(x.d, asOf), weight: daysBetween(x.d, asOf) } });
    }
    const count = (k) => items.filter((i) => i.payload.kind.startsWith(k)).length;
    const a = count('A_no_slip') + count('A_slip_not_linked'), ac = count('A_cancelled_only'), b = count('B_'), b2 = count('B2_');
    r.items = items; r.itemTotal = items.length; r.sampleSize = rows.length;
    r.observed = { from, to, a, a_slip_not_linked: count('A_slip_not_linked'), a_cancelled_only: ac, a_cancelled_only_max: maxCancelledOnly, b, b2, unshipped_mall: !!u,
      oldest: items.length ? items[0].payload.order_date : null };
    r.inputGeneration = { from, to, shipments_run_id: evidence.shipments ? evidence.shipments.run_id ?? null : null };
    const parts = [];
    if (a) parts.push(`モールで出荷済みなのに NE の伝票なし ${a}${r.observed.a_slip_not_linked ? ` (うち番号の合う伝票が結べていない ${r.observed.a_slip_not_linked})` : ''}`);
    if (ac > maxCancelledOnly) parts.push(`結び付いた伝票がキャンセルだけ ${ac} (上限 ${maxCancelledOnly} = 同梱の目安を超えた。同梱でない取消が混ざっている疑い)`);
    if (b) parts.push(`モールでも NE でも未発送のまま (内容が ${config.W11_LAG_DAYS} 日変わっていない / 注文から ${config.W11_B_MAX_DAYS} 日) ${b}`);
    if (b2) parts.push(`NE で出荷して ${config.W11_B2_GRACE_DAYS} 日たつのにモールが未発送 ${b2}`);
    r.verdict = parts.length ? 'breach' : 'pass';
    if (parts.length) r.reason = `${parts.join(' / ')} (注文日 ${from}〜${to})`;
    else if (ac) r.reason = `結び付いた伝票がキャンセルだけ ${ac} (上限 ${maxCancelledOnly} 以内 = 同梱の目安。明細に残す)`;
    out.push(r);
  }
  return out;
}

// ── W4 在庫の純減の異常 (ロジザードの在庫の差の昨日の区間。前提 = W3)
/** W4 が読む日 = 昨日 + 同じ曜日の過去 N 週 (指紋も同じ日を読む) */
export function w4Days(config, asOf) {
  const day = addDays(asOf, -1);
  return { day, baseline: Array.from({ length: config.W4_BASELINE_WEEKS }, (_, i) => addDays(day, -7 * (i + 1))) };
}
/** 日ごとの 差の印 と、その区間のイベントの和 (source_ref = '<scope>:<前日>..<日>'・calc_version のもの) */
const W4_ROWS = `
  with d as (select unnest($5::text[])::date as day)
  select d.day::text as d, m.status, m.events,
         e.n, e.out_qty, e.in_qty, e.net
    from d
    left join snapshots.stock_diff_days m on m.to_date = d.day and m.source = $1 and m.scope_key = $2 and m.calc_version = $3 and m.company_id = $4::smallint
    left join lateral (select count(*)::int as n, coalesce(sum(-x.qty_delta) filter (where x.qty_delta < 0), 0)::bigint as out_qty,
                              coalesce(sum(x.qty_delta) filter (where x.qty_delta > 0), 0)::bigint as in_qty, coalesce(sum(x.qty_delta), 0)::bigint as net
                         from events.inventory_events x
                        where x.source_system = 'logizard_diff' and x.source_ref = $2 || ':' || (d.day - 1)::text || '..' || d.day::text and x.payload ->> 'calc' = $3 and x.company_id = $4::smallint) e on true
   order by d.day`;
export async function evalW4(ctx, check) {
  const { db, config, asOf } = ctx;
  const sd = config.STOCK_DIFF, scopeKey = scopeKeyOf(sd.source, sd.scope);
  const { day, baseline } = w4Days(config, asOf);
  const holidays = new Set(config.NON_BUSINESS_DAYS || []);
  const r = base(check, scopeKey, { periodFrom: day, periodTo: day, severity: asOf < config.W4_INFO_UNTIL ? 'info' : check.severity,
    threshold: { mad_k: config.W4_MAD_K, min_abs_qty: config.W4_MIN_ABS_QTY, min_samples: config.W4_MIN_SAMPLES, baseline_weeks: config.W4_BASELINE_WEEKS } });
  if (!(await oneOf(db, `select to_regclass('snapshots.stock_diff_days') is not null as ok`, [])).ok) { r.verdict = 'blocked'; r.reason = '0022 (snapshots.stock_diff_days) が未適用'; return [r]; }
  const rows = await rowsOf(db, W4_ROWS, [sd.source, sd.scope, sd.calcVersion, config.COMPANY_ID, [day, ...baseline]]);
  const byDay = new Map(rows.map((x) => [x.d, x]));
  // 使える日 = 差を作った (done) 日で、印の件数 (変わった SKU の数) と中身のイベントの数が合う日
  const judge = (d) => {
    const x = byDay.get(d);
    if (holidays.has(d)) return { ok: false, reason: 'non_business_day' };
    if (!x || !x.status) return { ok: false, reason: 'no_diff' };
    if (x.status !== 'done') return { ok: false, reason: `diff_${x.status}` };
    if (Number(x.events) !== Number(x.n)) return { ok: false, reason: `mismatch_${x.events}/${x.n}` };
    return { ok: true, d, out: Number(x.out_qty), in: Number(x.in_qty), net: Number(x.net), skus: Number(x.n) };
  };
  // 祝日の一覧の期限切れ = 足し忘れ (祝日を平日と比べる偽の異常になる) → 判定しない
  if (config.NON_BUSINESS_DAYS_UNTIL && day > config.NON_BUSINESS_DAYS_UNTIL) { r.verdict = 'blocked'; r.reason = `祝日の一覧 (NON_BUSINESS_DAYS) が ${config.NON_BUSINESS_DAYS_UNTIL} までしか無い = 翌年の分を足して NON_BUSINESS_DAYS_UNTIL を延ばす (config/watch-checks.mjs)`; return [r]; }
  const y = judge(day);
  const samples = [], excluded = [];
  for (const b of baseline) { const s = judge(b); if (s.ok) samples.push(s); else excluded.push({ d: b, reason: s.reason }); }
  r.observed = { day, yesterday: y.ok ? { out: y.out, in: y.in, net: y.net, skus: y.skus } : { reason: y.reason }, samples: samples.length,
    excluded: excluded.map((e) => `${e.d.slice(5)}:${e.reason}`), baseline: samples.map((s) => `${s.d.slice(5)}:${s.out}/${s.in}/${s.net}`) };
  r.sampleSize = samples.length;
  r.inputGeneration = { day, baseline_days: samples.map((s) => s.d) };
  if (!y.ok) {
    r.verdict = 'blocked';
    r.reason = y.reason === 'non_business_day' ? `昨日 (${day}) は祝日・年末年始 = 平日と比べない`
      : y.reason === 'no_diff' ? `昨日 (${day}) の差の印が無い (W3 が見る)`
      : y.reason.startsWith('diff_') ? `昨日 (${day}) は差を作っていない (${y.reason.slice(5)} = W2 が見る)`
      : `昨日 (${day}) の差の印とイベントの数が合わない (${y.reason.slice(9)} = 締めをやり直す。README「在庫を毎時写す」)`;
    return [r];
  }
  if (samples.length < config.W4_MIN_SAMPLES) {
    r.verdict = 'blocked';
    r.reason = `有効標本 ${samples.length} < ${config.W4_MIN_SAMPLES} (同じ曜日の過去 ${config.W4_BASELINE_WEEKS} 週。在庫の差は 9/20 から = 平常の標本がたまるまで判定しない。除外 ${excluded.length}: ${excluded.slice(0, 3).map((e) => `${e.d.slice(5)} ${e.reason}`).join(', ')}${excluded.length > 3 ? ' ほか' : ''})`;
    return [r];
  }
  const st = (key) => { const xs = samples.map((s) => s[key]); const med = median(xs); return { med, mad: mad(xs, med) }; };
  const so = st('out'), sn = st('net'), si = st('in');
  r.observed.stats = { out: { median: so.med, mad: so.mad }, net: { median: sn.med, mad: sn.mad }, in: { median: si.med, mad: si.mad } };
  const bad = [];
  const dOut = y.out - so.med;
  if (Math.abs(dOut) > config.W4_MAD_K * so.mad && Math.abs(dOut) >= config.W4_MIN_ABS_QTY) bad.push(`減った数 ${y.out.toLocaleString()} 個 (平常 ${so.med.toLocaleString()} ± ${Math.round(config.W4_MAD_K * so.mad).toLocaleString()}。${dOut > 0 ? '多すぎ = 大量の減少' : '少なすぎ = 出荷が在庫に反映されていない疑い'})`);
  const dNet = sn.med - y.net;
  if (dNet > config.W4_MAD_K * sn.mad && dNet >= config.W4_MIN_ABS_QTY) bad.push(`差し引き ${y.net.toLocaleString()} 個 (平常 ${sn.med.toLocaleString()} ± ${Math.round(config.W4_MAD_K * sn.mad).toLocaleString()} = 大きく減った)`);
  r.verdict = bad.length ? 'breach' : 'pass';
  if (bad.length) r.reason = `${day}: ${bad.join(' / ')} (理由は分からない = 出荷・入荷・棚卸し・FBA 納品と突き合わせる)`;
  return [r];
}

// ── W12 DB の容量 (前提なし)。今の大きさ + 毎晩の締めが残した大きさの記録 (ops.job_runs)
/** W12 が読む大きさの記録 (JST の日ごとに最後の 1 件)。summary は text の JSON (maintainInventory が書く)。🚨 ::jsonb に通さず正規表現で数字だけ取る = 壊れた summary で評価ごと落ちない (Codex #1423 R1) */
const W12_ROWS = `
  select distinct on (d) d::text as d, bytes::text as bytes from (
    select (started_at at time zone 'Asia/Tokyo')::date as d, (substring(summary from '"db_bytes":([0-9]{1,15})[,}]'))::bigint as bytes, started_at, job_run_id
      from ops.job_runs
     where job_id = $1 and status = 'ok' and summary like '%"step":"maintain"%' and started_at >= ($2::date::timestamp at time zone 'Asia/Tokyo')) x
   where bytes is not null
   order by d, started_at desc, job_run_id desc`;
export async function evalW12(ctx, check) {
  const { db, config, asOf } = ctx;
  const from = addDays(asOf, -config.W12_HISTORY_DAYS);
  const r = base(check, 'db/company', { periodFrom: from, periodTo: asOf, severity: asOf < config.W12_INFO_UNTIL ? 'info' : check.severity,
    threshold: { disk_mb: Math.round(config.W12_DISK_BYTES / 1048576), warn_mb: Math.round(config.W12_WARN_BYTES / 1048576), min_remaining_days: config.W12_MIN_REMAINING_DAYS, min_deltas: config.W12_MIN_DELTAS } });
  // 🚨 今の大きさは MVCC ではない (読むたびに変わる) = 指紋に入れない (入れると毎回「世代が変わった」になる)
  const current = Number((await oneOf(db, `select pg_database_size(current_database())::text as b`, [])).b);
  const hist = await rowsOf(db, W12_ROWS, [config.W12_JOB_ID, from]);
  const deltas = [];
  for (let i = 1; i < hist.length; i++) {
    const gap = Math.round((Date.parse(`${hist[i].d}T00:00:00Z`) - Date.parse(`${hist[i - 1].d}T00:00:00Z`)) / 86400000);
    if (gap > 0) deltas.push({ d: hist[i].d, perDay: (Number(hist[i].bytes) - Number(hist[i - 1].bytes)) / gap });
  }
  const mb = (b) => Math.round(b / 1048576);
  r.observed = { current_mb: mb(current), disk_mb: mb(config.W12_DISK_BYTES), warn_mb: mb(config.W12_WARN_BYTES), history: hist.map((h) => `${h.d.slice(5)}:${mb(Number(h.bytes))}`), deltas_mb: deltas.map((x) => `${x.d.slice(5)}:${mb(x.perDay)}`) };
  r.sampleSize = deltas.length;
  r.inputGeneration = { history_days: hist.map((h) => h.d) };
  const bad = [];
  if (current >= config.W12_WARN_BYTES) bad.push(`今の大きさ ${mb(current).toLocaleString()} MB が ${mb(config.W12_WARN_BYTES).toLocaleString()} MB を超えた`);
  const latest = hist.length ? hist[hist.length - 1].d : null;
  r.observed.latest_record = latest;
  const stale = !latest || Math.round((Date.parse(`${asOf}T00:00:00Z`) - Date.parse(`${latest}T00:00:00Z`)) / 86400000) > config.W12_MAX_STALE_DAYS;
  if (deltas.length < config.W12_MIN_DELTAS || stale) {
    // 残り日数は推計しない (記録が足りない・途絶えた)。7 GB の判定は続ける
    if (bad.length) { r.verdict = 'breach'; r.reason = bad.join(' / '); return [r]; }
    r.verdict = 'blocked';
    r.reason = stale ? `大きさの記録が途絶えている (最新 ${latest || 'なし'}・${config.W12_MAX_STALE_DAYS} 日より古い = 毎晩の締め (maintainInventory) が動いていない。古い増え方で「余裕あり」と言わない)`
      : `大きさの記録が足りない (日ごとの増え分 ${deltas.length} < ${config.W12_MIN_DELTAS}。毎晩の締めの記録 = ops.job_runs)`;
    return [r];
  }
  const growth = median(deltas.map((x) => x.perDay));
  const remaining = growth > 0 ? (config.W12_DISK_BYTES - current) / growth : null;
  r.observed.growth_mb_per_day = Math.round(growth / 1048576 * 10) / 10;
  r.observed.max_delta_mb = mb(Math.max(...deltas.map((x) => x.perDay)));
  r.observed.remaining_days = remaining == null ? null : Math.floor(remaining);
  if (remaining != null && remaining < config.W12_MIN_REMAINING_DAYS) bad.push(`容量 ${mb(config.W12_DISK_BYTES).toLocaleString()} MB まで あと ${Math.floor(remaining)} 日 (1 日 ${r.observed.growth_mb_per_day} MB = 日ごとの増え分の中央値)`);
  r.verdict = bad.length ? 'breach' : 'pass';
  if (bad.length) r.reason = `${bad.join(' / ')} (pg_database_size = Render のディスク全体ではない)`;
  return [r];
}

// ── W13 マスタの照合 ①ロードの検証 (照合の証跡と全件 JSON を確かめて、全案件を渡す。Company DB構想 10 §6.1.1 B3・Codex ③a-2 B-R0 #4 #5)
const sha256Of = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
/** 明細の payload を小さく (保存は 200 行・64 KiB に間引かれる。全案件の照合は engine が items 全部で行う) */
const w13Payload = (i) => {
  const cut = (a) => (Array.isArray(a) ? a.slice(0, 5) : a);
  return { type: i.type, code: i.code, diffs: cut(i.diffs), expected: i.expected, actual: cut(i.actual), missing: cut(i.missing), qty: cut(i.qty), extra: cut(i.extra), pruned: i.pruned,
    change_candidates: Array.isArray(i.change_candidates) ? i.change_candidates.slice(0, 3).map((e) => ({ entity: e.entity_type, op: e.operation, attr: e.attribute, actor: `${e.actor_type}:${e.actor_id ?? ''}`, at: e.recorded_at })) : undefined };
};
/**
 * W13 の証跡を**ファイルから**読む (DATA_DIR があれば。無いときだけ起動時に渡された evidence)。
 * 評価と世代の指紋 (generationOf) が同じ読み方をする = 評価の途中で証跡が差し替わっても、再評価で新しい方を使う (Codex #1456 R1 High-2)
 */
export function readW13Evidence(config, asOf, evidence) {
  const dataDir = (process.env.DATA_DIR || '').trim();
  if (dataDir) {
    try { return readEvidence(dataDir, asOf)[config.W13_EVIDENCE] ?? null; } catch (e) { return { error: String(e && e.message).slice(0, 120) }; }
  }
  return evidence ? evidence[config.W13_EVIDENCE] ?? null : null;
}
export async function evalW13(ctx, check) {
  const { config, asOf, evidence, syncRunId, openIssues = [] } = ctx;
  const scopeKey = config.W13_SCOPE;
  const r = base(check, scopeKey, { periodFrom: asOf, periodTo: asOf });
  const hold = (reason) => { r.verdict = 'blocked'; r.reason = reason; return [r]; };
  const ev = readW13Evidence(config, asOf, evidence);
  r.inputGeneration = ev ? { evidence_state: ev.state ?? null, compare_run_id: ev.compare_run_id ?? null, sha256: ev.sha256 ?? null } : null;
  if (!ev) return hold('照合の証跡が無い (daily-sync の「マスタ照合」が走っていない)');
  if (ev.error) return hold(`照合の証跡が読めない (${ev.error})`);
  if (syncRunId && ev.sync_run_id !== syncRunId) return hold(`照合の証跡が今朝の実行のものでない (${ev.sync_run_id ?? 'なし'} / ${syncRunId})`);
  if (ev.state !== 'complete') return hold(`照合が終わっていない (${ev.state}${ev.error ? `: ${ev.error}` : ''})`);
  if (ev.as_of !== asOf) return hold(`照合の日が違う (${ev.as_of} / ${asOf})`);
  const dataDir = (process.env.DATA_DIR || '').trim();
  if (!dataDir) return hold('DATA_DIR が無い (照合の全件 JSON を読めない)');
  let buf;
  try { buf = fs.readFileSync(path.join(dataDir, String(ev.json_path || ''))); } catch (e) { return hold(`照合の全件 JSON が読めない (${String(e && e.message).slice(0, 120)})`); }
  if (sha256Of(buf) !== ev.sha256) return hold('照合の全件 JSON のハッシュが証跡と違う');
  let res;
  try { res = JSON.parse(buf.toString('utf8')); } catch { return hold('照合の全件 JSON が JSON でない'); }
  const items = Array.isArray(res.items) ? res.items : [];
  if (res.format !== config.W13_FORMAT || res.compare_run_id !== ev.compare_run_id || res.as_of !== asOf || res.verdict !== ev.verdict
    || (ev.counts && ev.counts.items != null && ev.counts.items !== items.length) || (res.load?.ingest_run_id ?? null) !== (ev.load?.ingest_run_id ?? null)) {
    return hold('照合の全件 JSON と証跡が食い違う (ID・日付・判定・件数・ロード)');
  }
  r.inputGeneration = { compare_run_id: res.compare_run_id, sha256: ev.sha256, load: res.load?.ingest_run_id ?? null };
  r.observed = { compare_run_id: res.compare_run_id, load: res.load?.ingest_run_id ?? null, load_started_at: res.load?.started_at ?? null, verdict: res.verdict,
    blocked_reason: res.blocked_reason, counts: res.counts, observed_at: res.finished_at ?? null };
  if (res.verdict === 'blocked') return hold(`照合が判定できない (${res.blocked_reason})`);
  // 全案件 (保存の段で間引く)。同じ subject key は 1 つ
  const seen = new Set();
  for (const i of items) {
    if (!i || typeof i.subject_key !== 'string' || seen.has(i.subject_key)) continue;
    seen.add(i.subject_key);
    r.items.push({ subjectType: 'sku_problem', subjectKey: i.subject_key, payload: w13Payload(i) });
  }
  r.itemTotal = r.items.length;
  r.sampleSize = res.counts?.compared?.value ?? null;
  // 🚨 open の案件が今回の明細に無いとき: 今回その種類 × SKU を比べた = 回復 / 比べていない (ロードの判断で対象外・種類ごと比べていない) = 回復にしない (outOfScope)
  const compared = Object.fromEntries(Object.entries(res.compared || {}).map(([k, v]) => [k, new Set(Array.isArray(v) ? v : [])]));
  const out = {};
  for (const i of openIssues) {
    if (i.check_id !== check.id || i.scope_key !== scopeKey || seen.has(i.subject_key)) continue;
    const at = String(i.subject_key).indexOf(':');
    const type = at > 0 ? i.subject_key.slice(0, at) : '', norm = at > 0 ? i.subject_key.slice(at + 1) : '';
    if (compared[type] && compared[type].has(norm)) continue;   // 比べて差が無い = 回復
    out[i.subject_key] = (res.exclusions && res.exclusions[i.subject_key]) || 'not_compared';
  }
  if (Object.keys(out).length) { r.outOfScope = out; r.observed.out_of_scope = out; }
  r.verdict = r.items.length ? 'breach' : 'pass';
  if (r.items.length) {
    const t = res.counts?.by_type || {};
    r.reason = `ロードの後にあるべき値と違う ${r.items.length} 件 (無い ${t.missing ?? 0} / 値 ${t.value ?? 0} / 原価 ${t.cost ?? 0} / 代表の仕入先 ${t.primary_supplier ?? 0} / 構成 ${t.components ?? 0})`;
  }
  return [r];
}

export const EVALUATORS = { W1: evalW1, W2: evalW2, W3: evalW3, W7: evalW7, W9: evalW9, W5: evalW5, W6: evalW6, W8: evalW8, W10: evalW10, W11: evalW11, W4: evalW4, W12: evalW12, W13: evalW13 };

/**
 * 世代の指紋: 各評価が「実際に読む値」を、評価と同じ範囲でまとめた文字列。snapshot の中と、閉じた後で比べる (違えば再評価。09 §2.1)
 * 🚨 集計値 (件数・最大時刻) ではなく鍵ごとの値にする (Codex R2 #1: 既存の running な run に途中 chunk が届くと、注文は増えるのに run の数も時刻も変わらない)。
 *    項目を足したら、その項目が読む値をここにも足す (evalW* と対で保つ)
 */
export async function generationOf(db, config, asOf, { evidence = {} } = {}) {
  const from = addDays(asOf, -10);   // W1 の対象日 (asOf + dayOffset) と W2 の窓 ([asOf + dayOffset − 7, asOf + dayOffset − 1]) と W3 の昨日 を全部含む
  const w9From = addDays(asOf, -Math.max(config.W9_LOOKBACK_DAYS, config.W6_SALES_DAYS || 0)), w9To = addDays(asOf, -1);   // W9 の窓と W6 の販売の窓の広い方 (公開行の指紋)
  // 🚨 取引の中で呼ぶ (savepoint で各部分を守る = 表が壊れていても読めた部分で指紋を作る。壊れた部分は評価も execution_error になる)
  const part = async (sql, params) => {
    await db.exec('savepoint gen');
    try { const r = await oneOf(db, sql, params); await db.exec('release savepoint gen'); return r; }
    catch (e) { try { await db.exec('rollback to savepoint gen'); } catch { /* */ } return { error: String(e && e.message).slice(0, 120) }; }
  };
  // W1 / W2: 取得記録を鍵ごとに (status・run・completed_at)。building の滞留は全期間を見るので、building の行は期間を限らず全部
  const cap = await part(`select coalesce(string_agg(source || '/' || scope_key || '/' || snapshot_date || '=' || status || ':' || coalesce(ingest_run_id, '') || ':' || coalesce(completed_at::text, ''), ',' order by source, scope_key, snapshot_date), '') as s
    from snapshots.stock_capture_days where company_id = $1::smallint and snapshot_date >= $2::date`, [config.COMPANY_ID, from]);
  const building = await part(`select coalesce(string_agg(source || '/' || scope_key || '/' || snapshot_date || '@' || built_at::text, ',' order by source, scope_key, snapshot_date), '') as s
    from snapshots.stock_capture_days where company_id = $1::smallint and status = 'building'`, [config.COMPANY_ID]);
  // W3: 差の印 (status・skip_reason・件数)
  const diff = await part(`select case when to_regclass('snapshots.stock_diff_days') is null then '-' else (select coalesce(string_agg(source || '/' || scope_key || '/' || calc_version || '/' || to_date || '=' || status || ':' || coalesce(skip_reason, '') || ':' || events || ':' || unresolved_changed || ':' || created_at::text, ',' order by source, scope_key, calc_version, to_date), '')
    from snapshots.stock_diff_days where company_id = $1::smallint and to_date >= $2::date) end as s`, [config.COMPANY_ID, from]);
  // W7: 証跡が指す run そのもの (status・complete・rows_seen = chunk が届けば変わる)
  const runIds = Object.values(evidence || {}).map((e) => e && e.run_id).filter((x) => typeof x === 'string' && x);
  const runs = await part(`select coalesce(string_agg(ingest_run_id || '=' || status || ':' || coalesce(complete::text, '') || ':' || coalesce(finished_at::text, '') || ':' || coalesce(rows_seen::text, ''), ',' order by ingest_run_id), '') as s
    from ops.ingest_runs where ingest_run_id = any($1::text[])`, [runIds]);
  // W9: state (session・watermark)・窓の公開行・窓の日ごとの注文の有無 (evalW9 と同じ問い合わせ = 途中 chunk で注文が増えた日も表れる。core.orders の全件は数えない)
  const sales = await part(`select coalesce(string_agg(mall || '/' || scope_key || '=' || coalesce(watermark::text, '') || ':' || coalesce(session_id, ''), ',' order by mall, scope_key), '') as s from mart.sales_daily_state where company_id = $1::smallint`, [config.COMPANY_ID]);
  const pub = await part(`select coalesce(string_agg(mall || '/' || scope_key || '/' || date_jst || '=' || run_id, ',' order by mall, scope_key, date_jst), '') as s
    from mart.sales_daily_published where company_id = $1::smallint and date_jst between $2::date and $3::date`, [config.COMPANY_ID, w9From, w9To]);
  const orders = await part(`select coalesce(string_agg(m.mall || '/' || m.scope || '/' || d.day::date || '=' || (exists (select 1 from core.orders o where o.company_id = $1::smallint and o.mall = m.mall and o.scope_key = m.scope and o.order_date_jst = d.day::date))::int, ',' order by m.mall, m.scope, d.day), '') as s
    from unnest($2::text[], $3::text[]) as m(mall, scope), generate_series($4::date, $5::date, interval '1 day') as d(day)`, [config.COMPANY_ID, config.ORDER_MALLS.map((m) => m.mall), config.ORDER_MALLS.map((m) => m.scope), w9From, w9To]);
  // W5: 日次の元 (直近の差の日の D / D−1) と W6: 在庫の view の元 (source ごとの最新の complete の日の日次) = 行ごとの hash の和 (順序に依らず・鍵ごとの値が変われば変わる)
  const latest = await part(`select coalesce(string_agg(source || '/' || scope_key || '=' || d, ',' order by source, scope_key), '') as s, coalesce(array_agg(d), '{}'::text[]) as days
    from (select source, scope_key, max(snapshot_date)::text as d from snapshots.stock_capture_days where company_id = $1::smallint and status = 'complete' group by 1, 2) x`, [config.COMPANY_ID]);
  const w5From = addDays(asOf, -((config.W5_ESCALATE_DAYS || 3) + 1));
  const daily = await part(`select coalesce(string_agg(source || '/' || scope_key || '/' || snapshot_date || '=' || n || ':' || q || ':' || h, ',' order by source, scope_key, snapshot_date), '') as s
    from (select source, scope_key, snapshot_date, count(*) as n, sum(qty) as q, sum(hashtext(source_code || ':' || coalesce(sku_id::text, '') || ':' || qty || ':' || coalesce(fba_available::text, '')))::bigint as h
            from snapshots.sku_stock_daily where company_id = $1::smallint and (snapshot_date >= $2::date or snapshot_date = any($3::text[]::date[])) group by 1, 2, 3) x`, [config.COMPANY_ID, w5From, Array.isArray(latest.days) ? latest.days.map(String) : []]);
  // W6: SKU の属性 (廃番) と出品の構成 (セットの展開)
  const skus = await part(`select count(*)::int as n, coalesce(sum(hashtext(sku_id::text || ':' || handling || ':' || sku_kind)), 0)::bigint as h from core.skus where company_id = $1::smallint`, [config.COMPANY_ID]);
  const setComps = await part(`select count(*)::int as n, coalesce(sum(hashtext(parent_sku_id::text || ':' || child_sku_id::text || ':' || qty)), 0)::bigint as h from core.sku_components where company_id = $1::smallint`, [config.COMPANY_ID]);
  const comps = await part(`select count(*)::int as n, coalesce(sum(hashtext(listing_id::text || ':' || sku_id::text || ':' || qty)), 0)::bigint as h from core.listing_components where company_id = $1::smallint`, [config.COMPANY_ID]);
  // W8: 昨日 + 同じ曜日の過去 N 週の 注文の件数・取消 (core.orders) と 売上日次の合計 (v_sales_daily)・昨日の公開・モールの最初の注文日
  const w8 = w8Days(config, asOf); const w8All = [w8.day, ...w8.baseline];
  const w8Orders = await part(`select coalesce(string_agg(mall || '/' || scope_key || '/' || order_date_jst || '=' || n || ':' || c, ',' order by mall, scope_key, order_date_jst), '') as s
    from (select mall, scope_key, order_date_jst, count(*) as n, count(*) filter (where is_cancelled) as c from core.orders where company_id = $1::smallint and mall = any($2::text[]) and order_date_jst = any($3::text[]::date[]) group by 1, 2, 3) x`,
    [config.COMPANY_ID, config.ORDER_MALLS.map((m) => m.mall), w8All]);
  const w8Sales = await part(`select coalesce(string_agg(mall || '/' || scope_key || '/' || date_jst || '=' || s || ':' || l || ':' || u, ',' order by mall, scope_key, date_jst), '') as s
    from (select mall, scope_key, date_jst, sum(sales_jpy) as s, sum(lines) as l, sum(lines_amount_unknown) as u from mart.v_sales_daily where company_id = $1::smallint and mall = any($2::text[]) and date_jst = any($3::text[]::date[]) group by 1, 2, 3) x`,
    [config.COMPANY_ID, config.ORDER_MALLS.map((m) => m.mall), w8All]);
  // 最初の注文日はモールごとに索引の先頭 1 件 (全履歴の group by min() にしない = 129 万件を読まない。Codex #1412 R1)
  const w8First = await part(`select coalesce(string_agg(m.mall || '/' || m.scope || '=' || coalesce(f.d, ''), ',' order by m.mall, m.scope), '') as s
    from unnest($2::text[], $3::text[]) as m(mall, scope)
    left join lateral (select o.order_date_jst::text as d from core.orders o where o.company_id = $1::smallint and o.mall = m.mall and o.scope_key = m.scope order by o.order_date_jst limit 1) f on true`,
    [config.COMPANY_ID, config.ORDER_MALLS.map((m) => m.mall), config.ORDER_MALLS.map((m) => m.scope)]);
  const w8Pub = await part(`select coalesce(string_agg(mall || '/' || scope_key || '/' || date_jst || '=' || run_id, ',' order by mall, scope_key, date_jst), '') as s
    from mart.sales_daily_published where company_id = $1::smallint and mall = any($2::text[]) and date_jst = any($3::text[]::date[])`, [config.COMPANY_ID, config.ORDER_MALLS.map((m) => m.mall), w8All]);
  // W8 の取込の完了の証跡 (翌朝 = D+1 の W7 の判定。対象のモールの scope だけ・回ごと = 同じ日の再実行も指紋に入る)
  const w8Ev = w8.baseline.map((d) => addDays(d, 1));
  const w8W7 = await part(`select coalesce(string_agg(x.scope_key || '/' || r.as_of_date || '=' || x.verdict || ':' || x.watch_run_id, ',' order by x.scope_key, r.as_of_date, r.started_at, x.watch_result_id), '') as s from ops.watch_results x join ops.watch_runs r on r.watch_run_id = x.watch_run_id
    where x.company_id = $1::smallint and x.check_id = 'W7' and x.scope_key = any($2::text[]) and r.as_of_date = any($3::text[]::date[])`, [config.COMPANY_ID, config.ORDER_MALLS.map((m) => scopeKeyOf(m.mall, m.scope)), w8Ev]);
  // W10: run の全部 (悪い run と、回復に使う後続の run = 種類・開始・状態・complete・世代・行数・エラー) / chunk (止まった run に届く途中 chunk・失敗の数) / 失敗した行の今の世代 (取り直されたら変わる) /
  //   在庫の日次の取得記録の全期間 (在庫の日次の run が指す日の状態。W1 / W2 の指紋は直近 10 日だけ)
  //   行ごとの hash の和 (順序に依らず・鍵ごとの値が変われば変わる)。W10 は since より前の run も回復の根拠に読む = 期間を限らない
  const w10Runs = await part(`select count(*)::int as n, coalesce(sum(hashtext(ingest_run_id || '=' || source_system || '/' || entity || '/' || scope_key || ':' || started_at::text || ':' || status || ':' || coalesce(complete::text, '') || ':' || coalesce(finished_at::text, '') || ':' || coalesce(rows_seen::text, '') || ':' || coalesce(checksum, '') || ':' || coalesce(error, ''))), 0)::bigint as h from ops.ingest_runs`, []);
  const w10Cap = await part(`select count(*)::int as n, coalesce(sum(hashtext(source || '/' || scope_key || '/' || snapshot_date || '=' || status || ':' || coalesce(ingest_run_id, ''))), 0)::bigint as h from snapshots.stock_capture_days where company_id = $1::smallint`, [config.COMPANY_ID]);
  const w10Chunks = await part(`select count(*)::int as n, coalesce(sum(hashtext(ingest_run_id || ':' || chunk_index || ':' || rows_failed)), 0)::bigint as h from ops.ingest_chunks`, []);
  const w10Ids = await part(`select coalesce(array_agg(distinct ingest_run_id), '{}'::text[]) as ids from ops.ingest_chunks where rows_failed > 0`, []);
  const w10Keys = await part(`select count(*)::int as n, coalesce(sum(hashtext(x.ingest_run_id || ':' || coalesce(x.key, '') || ':' || coalesce(x.seq::text, ''))), 0)::bigint as h from (${W10_FAILED_KEYS}) x`, [Array.isArray(w10Ids.ids) ? w10Ids.ids : [], config.COMPANY_ID]);
  const w10Hist = await part(`select count(*)::int as n, coalesce(sum(hashtext(x.scope_key || ':' || x.watch_result_id || ':' || coalesce((x.observed -> 'proven')::text, '') || ':' || coalesce(x.observed -> 'todays_push' ->> 'batch_seq', '') || ':' || coalesce(x.observed -> 'todays_push' ->> 'trusted', ''))), 0)::bigint as h
    from ops.watch_results x where x.company_id = $1::smallint and x.check_id = 'W10'`, [config.COMPANY_ID]);
  // W11: 評価と同じ問い合わせの結果 (モールごとの候補の注文 = 状態・伝票の数・出荷済みの伝票・結べていない伝票) の hash の和
  const w11 = [];
  if (config.W11_WINDOW_DAYS) {
    const { from: f11, to: t11 } = w11Range(config, asOf);
    for (const m of config.ORDER_MALLS) {
      const u = w11Unshipped(config, m.mall);
      w11.push(await part(`select count(*)::int as n, coalesce(sum(hashtext(x.order_id || ':' || x.mall_order_no || ':' || x.d || ':' || x.status || ':' || coalesce(x.su, '') || ':' || x.n_slips || ':' || x.n_active || ':' || coalesce(x.last_ship, '') || ':' || x.slip_not_linked)), 0)::bigint as h from (${W11_ROWS}) x`,
        [config.COMPANY_ID, m.mall, m.scope, f11, t11, u ? u.notShipped : []]));
    }
  }
  // W4: 昨日 + 同じ曜日の過去 N 週の 差の印 と イベントの和 (評価と同じ問い合わせ)。W12: 大きさの記録 (今の大きさは入れない = MVCC ではない)
  let w4 = null, w12 = null;
  if (config.W4_BASELINE_WEEKS) {
    const d4 = w4Days(config, asOf); const sd = config.STOCK_DIFF;
    w4 = await part(`select case when to_regclass('snapshots.stock_diff_days') is null then '-' else (select coalesce(string_agg(x.d || '=' || coalesce(x.status, '') || ':' || coalesce(x.events::text, '') || ':' || x.n || ':' || x.out_qty || ':' || x.in_qty || ':' || x.net, ',' order by x.d), '') from (${W4_ROWS}) x) end as s`,
      [sd.source, sd.scope, sd.calcVersion, config.COMPANY_ID, [d4.day, ...d4.baseline]]);
  }
  // W13: 照合の証跡を**ファイルから読み直す** (evalW13 と同じ readW13Evidence。evidence のオブジェクトは評価の前後で同じ = 差し替えが見えない。Codex ③a-2 B-R0 #7)
  let w13 = null;
  if (config.W13_EVIDENCE) {
    const e = readW13Evidence(config, asOf, evidence);
    w13 = e ? `${e.state ?? ''}:${e.compare_run_id ?? ''}:${e.sha256 ?? ''}:${e.sync_run_id ?? ''}:${e.error ?? ''}` : '-';
  }
  if (config.W12_HISTORY_DAYS) w12 = await part(`select coalesce(string_agg(x.d || '=' || x.bytes, ',' order by x.d), '') as s from (${W12_ROWS}) x`, [config.W12_JOB_ID, addDays(asOf, -config.W12_HISTORY_DAYS)]);
  return JSON.stringify([cap, building, diff, runs, sales, pub, orders, latest.s, daily, skus, comps, setComps, w8Orders, w8Sales, w8First, w8Pub, w8W7, w10Runs, w10Chunks, w10Keys, w10Cap, w10Hist, w11, w4, w12, w13]);
}
