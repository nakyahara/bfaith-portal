/**
 * checks.mjs — 見張りの項目の評価 (config/watch-checks.mjs の定義を読んで SQL を流す)。設計 = 09 §2・§4
 *
 * 各評価は「評価キー (check × scope)」ごとに 1 つの結果を返す:
 *   { checkId, scopeKey, subjectType, periodFrom, periodTo, verdict, severity, observed, threshold, reason, inputGeneration, sampleSize, items, itemTotal }
 *   verdict = pass / breach / blocked / execution_error。severity = 定義の重さ (verdict とは別の軸)
 * 🚨 「行がある = そろっている」と読まない: 前提 (完了の印) が無ければ blocked。pass にしない
 * SQL は固定・逐次。db = { query(text, params) } (pg の client でも PGlite でも同じ)
 */

export const scopeKeyOf = (a, b) => `${a}/${b}`;
export const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
const jstDateOf = (iso) => new Date(Date.parse(iso) + 9 * 3600000).toISOString().slice(0, 10);
const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
const oneOf = async (db, sql, params) => (await db.query(sql, params)).rows[0] || null;

/** 評価キーの予定 (評価の前に確定する = 「12 項目中 12」ではなく「キー N のうち N」) */
export function plannedKeys(config) {
  const keys = [];
  for (const c of config.CHECKS) {
    if (c.id === 'W1' || c.id === 'W2') for (const s of config.STOCK_SCOPES) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(s.source, s.scope) });
    else if (c.id === 'W3') keys.push({ checkId: c.id, scopeKey: scopeKeyOf(config.STOCK_DIFF.source, config.STOCK_DIFF.scope) });
    else if (c.id === 'W7' || c.id === 'W9') for (const m of config.ORDER_MALLS) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(m.mall, m.scope) });
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
    const to = addDays(asOf, s.dayOffset - 1), from = addDays(to, -(config.W2_WINDOW_DAYS - 1));
    const scopeKey = scopeKeyOf(s.source, s.scope);
    const rows = await rowsOf(db, `select snapshot_date::text as d, status from snapshots.stock_capture_days
      where source = $1 and scope_key = $2 and company_id = $3::smallint and snapshot_date between $4::date and $5::date`, [s.source, s.scope, config.COMPANY_ID, from, to]);
    const byDay = new Map(rows.map((x) => [x.d, x.status]));
    const allow = partialAllowed(s, asOf);
    const bad = [];
    for (let d = from; d <= to; d = addDays(d, 1)) {
      const st = byDay.get(d) || 'absent';
      if (st === 'complete' || (st === 'partial' && allow)) continue;
      bad.push({ subjectType: 'day', subjectKey: d, payload: { status: st } });
    }
    const r = base(check, scopeKey, { periodFrom: from, periodTo: to, observed: { window_days: config.W2_WINDOW_DAYS, bad_days: bad.map((b) => `${b.subjectKey}:${b.payload.status}`), partial_allowed: allow }, threshold: { bad_days: 0 }, sampleSize: config.W2_WINDOW_DAYS, items: bad, itemTotal: bad.length });
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

// ── W7 注文の取込の完了 (証跡 + 送信があれば ops.ingest_runs)
export async function evalW7(ctx, check) {
  const { db, config, evidence } = ctx;
  const out = [];
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const ev = evidence[`orders-${m.mall}`] || null;
    const r = base(check, scopeKey, { threshold: { push_ok: true, failed: 0, stale: 0, transform_errors: 0 } });
    if (!ev) { r.verdict = 'blocked'; r.reason = '今朝の push の証跡が無い (push が走っていない・証跡を書けなかった)'; out.push(r); continue; }
    r.observed = { run_id: ev.run_id ?? null, batch_seq: ev.batch_seq ?? null, scanned: ev.scanned ?? null, in_scope: ev.in_scope ?? null, changed: ev.changed ?? null, applied: ev.applied ?? null, same: ev.same ?? null, stale: ev.stale ?? null, failed: ev.failed ?? null, transform_errors: ev.transform_errors ?? null, skipped: ev.skipped || null, error: ev.error || null, started_at: ev.started_at || null };
    r.inputGeneration = { evidence_written_at: ev.written_at || null, run_id: ev.run_id ?? null, batch_seq: ev.batch_seq ?? null };
    if (ev.error && !ev.run_id && ev.ok === false) { r.verdict = 'breach'; r.reason = `push が落ちた: ${String(ev.error).slice(0, 120)}`; out.push(r); continue; }
    if (ev.skipped) { r.verdict = 'blocked'; r.reason = `送り手が見送った (${ev.skipped})`; out.push(r); continue; }
    if (ev.scope && ev.scope !== m.scope) { r.verdict = 'breach'; r.reason = `scope が違う (証跡 ${ev.scope} / 期待 ${m.scope})`; out.push(r); continue; }
    if (ev.locked) { r.verdict = 'blocked'; r.reason = '別の送り手が走っていて見送った'; out.push(r); continue; }
    const bad = [];
    if (ev.push_ok === false) bad.push('push_ok=false');
    if ((ev.failed ?? 0) > 0) bad.push(`failed ${ev.failed}`);
    if ((ev.stale ?? 0) > 0) bad.push(`stale ${ev.stale}`);
    if ((ev.transform_errors ?? 0) > 0) bad.push(`整形できない ${ev.transform_errors}`);
    if (bad.length) { r.verdict = 'breach'; r.reason = `push に失敗がある (${bad.join(' / ')})`; out.push(r); continue; }
    if (!Number.isInteger(ev.changed)) { r.verdict = 'blocked'; r.reason = '証跡の形が違う (changed が無い)'; out.push(r); continue; }
    if (ev.changed === 0) {
      // 🚨 変更ゼロの朝は chunk を送らない = Render に run が無い。走査が完了して変わった注文が 0 だった、を証跡で認める (設計 09 §2.1 の契約)
      r.verdict = 'pass'; r.reason = '変わった注文 0 (走査は完了)'; r.observed.contract = 'zero_change'; out.push(r); continue;
    }
    if (!ev.run_id) { r.verdict = 'blocked'; r.reason = '送ったのに run_id が無い'; out.push(r); continue; }
    const run = await oneOf(db, `select status, complete, rows_seen, rows_inserted, finished_at::text as finished_at from ops.ingest_runs where ingest_run_id = $1`, [ev.run_id]);
    if (!run) { r.verdict = 'breach'; r.reason = `Render に run ${ev.run_id} が無い`; }
    else if (run.status !== 'success' || run.complete !== true) { r.verdict = 'breach'; r.reason = `Render の run が ${run.status}${run.complete ? '' : ' (未完)'} (🚨 complete は partial でも立つ = status も見る)`; r.observed.run = run; }
    else { r.verdict = 'pass'; r.observed.run = run; }
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

export const EVALUATORS = { W1: evalW1, W2: evalW2, W3: evalW3, W7: evalW7, W9: evalW9 };
