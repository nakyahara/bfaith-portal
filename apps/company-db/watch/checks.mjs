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
  // 定義の順番の約束: depends の相手は先に評価される (後ろにあると「未評価」で blocked のまま = 定義の誤り)
  const seen = new Set();
  for (const c of config.CHECKS) {
    for (const dep of c.depends || []) { const id = dep.endsWith(':*') ? dep.slice(0, -2) : dep; if (!seen.has(id)) throw new Error(`定義の順番が違う: ${c.id} の前提 ${id} が先に無い (config/watch-checks.mjs の CHECKS)`); }
    seen.add(c.id);
  }
  for (const c of config.CHECKS) {
    if (c.id === 'W1' || c.id === 'W2') for (const s of config.STOCK_SCOPES) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(s.source, s.scope) });
    else if (c.id === 'W3' || c.id === 'W5') keys.push({ checkId: c.id, scopeKey: scopeKeyOf(config.STOCK_DIFF.source, config.STOCK_DIFF.scope) });
    else if (c.id === 'W7' || c.id === 'W9') for (const m of config.ORDER_MALLS) keys.push({ checkId: c.id, scopeKey: scopeKeyOf(m.mall, m.scope) });
    else if (c.id === 'W6') keys.push({ checkId: c.id, scopeKey: scopeKeyOf('all', config.W6_SCOPE.scope) });
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

// ── W7 注文の取込の完了 (証跡 + 送信があれば ops.ingest_runs)
export async function evalW7(ctx, check) {
  const { db, config, evidence, syncRunId = null, unbound = false } = ctx;
  const out = [];
  for (const m of config.ORDER_MALLS) {
    const scopeKey = scopeKeyOf(m.mall, m.scope);
    const ev = evidence[`orders-${m.mall}`] || null;
    const r = base(check, scopeKey, { threshold: { push_ok: true, failed: 0, stale: 0, transform_errors: 0, mode: 'incremental', sync_run_id: syncRunId } });
    if (!ev) { r.verdict = 'blocked'; r.reason = `今朝の実行${syncRunId ? ` (${syncRunId})` : ''} の push の証跡が無い (push が走っていない = 上流の取込が失敗した・見送った、か証跡を書けなかった)`; out.push(r); continue; }
    // 🚨 同じ実行の証跡だけを採用する: 同じ日の手動実行 (ID なし) や別の回の証跡で pass にしない (Codex R1 High)。dry-run で ID が無いときだけ、今日の証跡を「結びつけずに」読む
    if (!unbound && !syncRunId) { r.verdict = 'blocked'; r.reason = '実行 ID (DAILY_SYNC_RUN_ID) が無い = どの回の証跡か結びつけられない'; out.push(r); continue; }
    if (!unbound && (ev.sync_run_id || null) !== syncRunId) { r.verdict = 'blocked'; r.reason = `別の実行の証跡 (証跡 ${ev.sync_run_id || 'ID なし = 手動'} / 今朝 ${syncRunId})`; out.push(r); continue; }
    if (ev.mode && ev.mode !== 'incremental') { r.verdict = 'blocked'; r.reason = `証跡の mode が ${ev.mode} (範囲を流した回 = 全対象を走査していない)`; out.push(r); continue; }
    r.observed = { sync_run_id: ev.sync_run_id || null, bound: !unbound, run_id: ev.run_id ?? null, batch_seq: ev.batch_seq ?? null, scanned: ev.scanned ?? null, in_scope: ev.in_scope ?? null, changed: ev.changed ?? null, applied: ev.applied ?? null, same: ev.same ?? null, stale: ev.stale ?? null, failed: ev.failed ?? null, transform_errors: ev.transform_errors ?? null, skipped: ev.skipped || null, error: ev.error || null, started_at: ev.started_at || null };
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
const W6_SOLD = `
  with s as (select listing_id, sku_id, (units_ordered - units_cancelled)::bigint as units, date_jst from mart.v_sales_daily
              where company_id = $1::smallint and date_jst between $2::date and $3::date and units_ordered - units_cancelled > 0),
       direct as (select sku_id, sum(units) as units, count(distinct date_jst) as days, max(date_jst) as last_day from s where sku_id is not null group by sku_id),
       viaset as (select c.sku_id, sum(s.units * c.qty) as units, count(distinct s.date_jst) as days, max(s.date_jst) as last_day
                    from s join core.listing_components c on c.company_id = $1::smallint and c.listing_id = s.listing_id where s.sku_id is null group by c.sku_id),
       sold as (select sku_id, sum(units)::bigint as units, max(days)::int as days, max(last_day)::text as last_day from (select * from direct union all select * from viaset) u group by sku_id)`;
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
      (select coalesce(sum(units), 0) from s where sku_id is null and (listing_id is null or not exists (select 1 from core.listing_components c where c.company_id = $1::smallint and c.listing_id = s.listing_id))) as unexpanded_units,
      (select count(*) from s where sku_id is null and (listing_id is null or not exists (select 1 from core.listing_components c where c.company_id = $1::smallint and c.listing_id = s.listing_id))) as unexpanded_rows`, [config.COMPANY_ID, from, to]);
  const unexpandedShare = Number(un.total_units) > 0 ? Number(un.unexpanded_units) / Number(un.total_units) : 0;
  r.observed.sold_skus = Number(un.sold_skus); r.observed.total_units = Number(un.total_units); r.observed.unexpanded_units = Number(un.unexpanded_units); r.observed.unexpanded_rows = Number(un.unexpanded_rows); r.observed.unexpanded_share = Math.round(unexpandedShare * 10000) / 10000;
  r.sampleSize = Number(un.sold_skus);
  if (unexpandedShare > config.W6_MAX_UNEXPANDED_SHARE) { r.verdict = 'blocked'; r.reason = `SKU に展開できない販売が正味数量の ${(unexpandedShare * 100).toFixed(1)}% (上限 ${config.W6_MAX_UNEXPANDED_SHARE * 100}%) = 販売履歴が不完全 (出品と SKU の紐付け = product-hub)`; return [r]; }
  const rows = await rowsOf(db, `${W6_SOLD}
    select s.sku_id, k.code, k.name, k.handling, s.units, s.days, s.last_day, v.warehouse_qty, v.warehouse_allocated_qty, v.fba_jp_available, v.fba_jp_inbound
      from sold s join mart.v_sku_stock v on v.company_id = $1::smallint and v.sku_id = s.sku_id join core.skus k on k.company_id = $1::smallint and k.sku_id = s.sku_id
     where coalesce(v.warehouse_qty, 0) + coalesce(v.fba_jp_available, 0) = 0 and k.handling <> 'discontinued'
     order by s.units desc, s.sku_id`, [config.COMPANY_ID, from, to]);
  r.items = rows.map((x) => ({ subjectType: 'sku', subjectKey: String(x.sku_id), payload: { code: x.code, name: String(x.name || '').slice(0, 60), units: Number(x.units), days_sold: x.days, last_sold: x.last_day, warehouse_qty: x.warehouse_qty, warehouse_allocated_qty: x.warehouse_allocated_qty, fba_jp_available: x.fba_jp_available, fba_jp_inbound: x.fba_jp_inbound, weight: Number(x.units) } }));
  r.itemTotal = rows.length;
  r.observed.out_of_stock = rows.length; r.observed.top = rows.slice(0, 3).map((x) => `${x.code} (${x.units})`);
  r.inputGeneration = { warehouse_as_of: st.w_as_of, fba_jp_as_of: st.f_as_of };
  // 🚨 open の案件 (SKU) が今回の明細に無いとき、「在庫が入った (回復)」と「監視対象外 (廃番・窓から外れた = 在庫は 0 のまま)」を分ける (engine は outOfScope を回復にしない)
  const inItems = new Set(r.items.map((i) => i.subjectKey));
  const mine = openIssues.filter((i) => i.check_id === check.id && i.scope_key === scopeKey && i.subject_type === 'sku' && !inItems.has(i.subject_key)).map((i) => i.subject_key);
  if (mine.length) {
    const ex = await rowsOf(db, `${W6_SOLD} select k.sku_id::text as sku_id, k.handling, coalesce(v.warehouse_qty, 0) + coalesce(v.fba_jp_available, 0) as stock, exists (select 1 from sold where sold.sku_id = k.sku_id) as sold_now
        from core.skus k join mart.v_sku_stock v on v.company_id = k.company_id and v.sku_id = k.sku_id where k.company_id = $1::smallint and k.sku_id = any($4::bigint[])`, [config.COMPANY_ID, from, to, mine]);
    r.outOfScope = {};
    for (const x of ex) if (Number(x.stock) === 0) r.outOfScope[x.sku_id] = x.handling === 'discontinued' ? 'discontinued' : 'no_sales_in_window';
    for (const id of mine) if (!ex.some((x) => x.sku_id === id)) r.outOfScope[id] = 'sku_missing';
    r.observed.out_of_scope = r.outOfScope;
  }
  r.verdict = rows.length ? 'breach' : 'pass';
  if (rows.length) r.reason = `売れ筋 ${un.sold_skus} SKU のうち在庫 0 が ${rows.length} (${r.observed.top.join(', ')}${rows.length > 3 ? ' ほか' : ''})`;
  return [r];
}

export const EVALUATORS = { W1: evalW1, W2: evalW2, W3: evalW3, W7: evalW7, W9: evalW9, W5: evalW5, W6: evalW6 };

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
  const skus = await part(`select count(*)::int as n, coalesce(sum(hashtext(sku_id::text || ':' || handling)), 0)::bigint as h from core.skus where company_id = $1::smallint`, [config.COMPANY_ID]);
  const comps = await part(`select count(*)::int as n, coalesce(sum(hashtext(listing_id::text || ':' || sku_id::text || ':' || qty)), 0)::bigint as h from core.listing_components where company_id = $1::smallint`, [config.COMPANY_ID]);
  return JSON.stringify([cap, building, diff, runs, sales, pub, orders, latest.s, daily, skus, comps]);
}
