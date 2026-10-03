/**
 * reresolve-batch.mjs — 注文明細の解き直しの夜の回し方 (D-60 PR 1b-0r の部品・設計 13 §3.10「reresolve の batch の契約」v3.12)
 *
 * 🚨 夜間ロード (engine.mjs 8b) からはまだ呼ばない。engine は旧い 3 引数 core.reresolve_order_lines(smallint, text, date) のまま (切り替えは PR 1b-0e・
 *    本番の件数を読むだけの SQL で確かめた後)。この部品は 0057 の関数を使う「夜の手順」を 1 か所に固め、試験 (scripts/test-company-db-reresolve-batch-pg.mjs) で守る。
 *
 * 夜の手順 (会社 × モールごと):
 *   ⓪ reserveNight  = 本体の取引の **前に別の短い取引で** 今夜の窓 [今日 − 35, 明日) を ops.reresolve_backlog_windows に予約 (cursor 0・on conflict do nothing) し、
 *                      retry の cursor の行 ops.reresolve_retry_cursor を作る (無ければ 0)。本体が巻き戻っても予約は残る = 次の晩に最初から流し直す
 *   ① runNightBody (呼び手の本体の取引の中) の retry = cursor の行を for update で読み、周回の外 (high-water が null) なら retry の表の今の max(order_id) を
 *      high-water に保存して新しい周回 (cursor 0・cycle_started_at = now())。表が空なら周回を始めず関数を呼ばない (batch に数えない)。
 *      その周回は order_id ≦ high-water だけ = p_before_order_id = high-water + 1 (🚨 high-water が bigint の最大値なら null = R-D60-v3-13 L1)。
 *      呼ぶ前に周回の残り (cursor < id ≦ high-water) が空かを exists で確かめる (batch に数えない)。retry の予算 R まで。
 *      has_more = false か残りが空 = 周回の完了 (cycles_completed + 1・cursor 0・high-water null)・その晩の retry はそこで終わり (同じ晩に新しい周回を始めない)
 *   ② 同じ取引で 持ち越しの窓を古い順に (今夜の予約を含む・保存した cursor から)・1 晩の上限 N の残りまで。流し終えた窓の行は消す・上限で止まった窓は cursor を残す
 *   retry の予算 R は 1〜N − 1 (窓に最低 1 batch を残す)。違えば何も流さずに例外
 *
 * 報告 (readRetryState + estimateCycleNights + retryWarnings): retry の表の残り Q 注文・cursor・high-water・cycle_started_at・1 周の見込みの晩の数
 *   (最悪の保証 ⌈Q ÷ R⌉ / 式 ⌈B ÷ R⌉ / 実測 ⌈Q ÷ (R × e)⌉)・⚠️ 4 つ (失敗にはしない)。🚨 ⌈Q ÷ R⌉ と B は「読んだ時点の retry の表」についての見込み
 *   (周回の途中で high-water 以下・cursor より後ろに行が入ると、その周回の対象は増える = R-D60-v3-13 M1。保証は「1 batch は少なくとも 1 注文進む」まで)
 *
 * order_id は全部 BigInt で持つ (pg は int8 を文字で返す・Number にすると 2^53 を超えて精度を失う = R-D60-v3-13 L1)。
 * db = { query(text, params) → { rows } } (pg の Client / migrate.mjs の pgAdapter)。
 */

/** 既定の値 (設計 13 の目安。値は engine の定数 = 新しい env は作らない・PR 1b-0e で本番の件数から決める) */
export const RERESOLVE_NIGHT_DEFAULTS = Object.freeze({
  nightCap: 20,        // 1 晩の batch の数の上限 N (retry と窓をまたいで数える)
  retryBudget: 5,      // そのうち retry の予算 R (1〜N − 1)
  maxOrders: 2000,     // 1 batch の注文の上限 (≦ 5,000)
  maxLines: 10000,     // 1 batch の未解決の明細の上限 (≦ 20,000)
  pastDays: 35,        // 夜の窓 = [JST の今日 − 35, JST の明日) = 今日を含む 36 日 (今の 8b と同じ)
});
export const LIMITS = Object.freeze({ maxOrders: 5000, maxLines: 20000, windowDays: 62 });
export const PG_BIGINT_MAX = 9223372036854775807n;
export const WARN_DAYS = 7;
export const WARN_ATTEMPTS = 7;

const toBig = (v) => (v === null || v === undefined ? null : BigInt(String(v)));
const isInt = (v) => Number.isInteger(v);

/** 周回の high-water から retry の関数の p_before_order_id (排他の上端) を作る。最大値なら null (+ 1 は溢れる) */
export function beforeOrderIdFor(through) {
  const t = toBig(through);
  if (t === null || t <= 0n || t > PG_BIGINT_MAX) throw Object.assign(new Error(`high-water が不正: ${through}`), { code: 'RERESOLVE_BAD_HIGH_WATER' });
  return t === PG_BIGINT_MAX ? null : t + 1n;
}

/** 夜の窓 = [JST の今日 − pastDays, JST の明日)。'YYYY-MM-DD' の組 (engine の reresolveSince と同じく JST に寄せてから切る) */
export function nightWindow(now = new Date(), pastDays = RERESOLVE_NIGHT_DEFAULTS.pastDays) {
  const jstMidnight = Date.parse(new Date(now.getTime() + 9 * 3600000).toISOString().slice(0, 10) + 'T00:00:00Z');
  const day = (n) => new Date(jstMidnight + n * 86400000).toISOString().slice(0, 10);
  return { since: day(-pastDays), until: day(1) };
}

/** 予算と上限の確かめ (何も流す前)。違えば例外 */
export function checkNightOptions(o) {
  const bad = (m) => { throw Object.assign(new Error(m), { code: 'RERESOLVE_BAD_OPTIONS' }); };
  if (!isInt(o.company) || typeof o.mall !== 'string' || !o.mall) bad('会社 (整数) とモールが要る');
  if (!isInt(o.nightCap) || o.nightCap < 2) bad(`1 晩の上限 N は 2 以上 (${o.nightCap})`);
  if (!isInt(o.retryBudget) || o.retryBudget < 1 || o.retryBudget > o.nightCap - 1) bad(`retry の予算 R は 1〜${o.nightCap - 1} (窓に最低 1 batch を残す・${o.retryBudget})`);
  if (!isInt(o.maxOrders) || o.maxOrders < 1 || o.maxOrders > LIMITS.maxOrders) bad(`maxOrders は 1〜${LIMITS.maxOrders}`);
  if (!isInt(o.maxLines) || o.maxLines < 1 || o.maxLines > LIMITS.maxLines) bad(`maxLines は 1〜${LIMITS.maxLines}`);
}

/** ⓪ 予約 (自分で begin / commit する短い取引 = 呼び手は取引の外で呼ぶ) */
export async function reserveNight(db, { company, mall, since, until }) {
  await db.query('begin');
  try {
    await db.query(`insert into ops.reresolve_backlog_windows (company_id, mall, p_since, p_until) values ($1::smallint, $2, $3::date, $4::date) on conflict do nothing`, [company, mall, since, until]);
    await db.query(`insert into ops.reresolve_retry_cursor (company_id, mall) values ($1::smallint, $2) on conflict do nothing`, [company, mall]);
    await db.query('commit');
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    throw e;
  }
}

const sumInto = (rep, r) => {
  rep.candidates += Number(r.candidates); rep.resolved += Number(r.resolved); rep.ordersTouched += Number(r.orders_touched); rep.ordersSkippedLocked += Number(r.orders_skipped_locked);
};

/**
 * ①② 本体 (呼び手の取引の中で呼ぶ・commit / rollback は呼び手)。戻り = 報告の材料
 *   opts = { company, mall, nightCap, retryBudget, maxOrders, maxLines }
 */
export async function runNightBody(db, opts) {
  const o = { ...RERESOLVE_NIGHT_DEFAULTS, ...opts };
  checkNightOptions(o);
  const { company, mall, nightCap, retryBudget, maxOrders, maxLines } = o;
  const rep = {
    batches: 0, retryBatches: 0, windowBatches: 0, probes: 0, retryExamined: 0, retryProcessed: 0,
    candidates: 0, resolved: 0, ordersTouched: 0, ordersSkippedLocked: 0,
    cycleStarted: false, cycleCompleted: false, savedCursor: 0n, savedThrough: null,
    windowsDone: 0, windowsCarried: 0, skippedInRetry: [], skippedInWindows: [], retryBatchLog: [],
  };
  // ① retry
  const cur = (await db.query(`select after_order_id, cycle_through_order_id from ops.reresolve_retry_cursor where company_id = $1::smallint and mall = $2 for update`, [company, mall])).rows[0];
  if (!cur) throw Object.assign(new Error('retry の cursor の行が無い = 先に reserveNight を流す'), { code: 'RERESOLVE_NOT_RESERVED' });
  let after = toBig(cur.after_order_id);
  let through = toBig(cur.cycle_through_order_id);
  while (rep.retryBatches < retryBudget) {
    if (through === null) {
      const hw = toBig((await db.query(`select max(order_id) as m from ops.reresolve_retry_orders where company_id = $1::smallint and mall = $2`, [company, mall])).rows[0].m);
      if (hw === null) break;   // 表が空 = 周回を始めない・関数を呼ばない (batch に数えない)
      through = hw; after = 0n; rep.cycleStarted = true;
    }
    rep.probes++;
    const ex = (await db.query(`select exists (select 1 from ops.reresolve_retry_orders where company_id = $1::smallint and mall = $2 and order_id > $3::bigint and order_id <= $4::bigint) as e`,
      [company, mall, after.toString(), through.toString()])).rows[0].e;
    if (!ex) { rep.cycleCompleted = true; break; }   // 周回の残りが空 = 呼ばずに周回の完了 (batch に数えない)
    const before = beforeOrderIdFor(through);
    const r = (await db.query(`select * from core.reresolve_order_lines_retry($1::smallint, $2, $3::bigint, $4::bigint, $5::integer, $6::integer)`,
      [company, mall, after.toString(), before === null ? null : before.toString(), maxOrders, maxLines])).rows[0];
    rep.batches++; rep.retryBatches++; sumInto(rep, r);
    rep.retryExamined += Number(r.orders_examined); rep.retryProcessed += Number(r.orders_examined) - Number(r.orders_skipped_locked);
    rep.skippedInRetry.push(...(r.skipped_order_ids || []).map(toBig));
    rep.retryBatchLog.push({ examined: Number(r.orders_examined), hasMore: r.has_more, stop: r.stop_reason });
    after = toBig(r.next_after_order_id);
    if (!r.has_more) { rep.cycleCompleted = true; break; }   // high-water に着いた = 周回の完了
  }
  if (rep.cycleCompleted) { after = 0n; through = null; }
  await db.query(`update ops.reresolve_retry_cursor set after_order_id = $3::bigint, cycle_through_order_id = $4::bigint, updated_at = now(),
      cycles_completed = cycles_completed + $5::integer, cycle_started_at = case when $6::boolean then now() else cycle_started_at end
    where company_id = $1::smallint and mall = $2`, [company, mall, after.toString(), through === null ? null : through.toString(), rep.cycleCompleted ? 1 : 0, rep.cycleStarted]);
  rep.savedCursor = after; rep.savedThrough = through;

  // ② 持ち越しの窓 (古い順・今夜の予約を含む)
  const wins = (await db.query(`select p_since::text as s, p_until::text as u, after_order_id from ops.reresolve_backlog_windows
     where company_id = $1::smallint and mall = $2 order by p_since, p_until for update`, [company, mall])).rows;
  for (const w of wins) {
    let wcur = toBig(w.after_order_id); let done = false;
    while (rep.batches < nightCap) {
      const r = (await db.query(`select * from core.reresolve_order_lines($1::smallint, $2, $3::date, $4::date, $5::bigint, $6::integer, $7::integer)`,
        [company, mall, w.s, w.u, wcur.toString(), maxOrders, maxLines])).rows[0];
      rep.batches++; rep.windowBatches++; sumInto(rep, r);
      rep.skippedInWindows.push(...(r.skipped_order_ids || []).map(toBig));
      wcur = toBig(r.next_after_order_id);
      if (!r.has_more) { done = true; break; }
    }
    if (done) {
      await db.query(`delete from ops.reresolve_backlog_windows where company_id = $1::smallint and mall = $2 and p_since = $3::date and p_until = $4::date`, [company, mall, w.s, w.u]);
      rep.windowsDone++;
    } else {
      await db.query(`update ops.reresolve_backlog_windows set after_order_id = $5::bigint, saved_at = now() where company_id = $1::smallint and mall = $2 and p_since = $3::date and p_until = $4::date`,
        [company, mall, w.s, w.u, wcur.toString()]);
      rep.windowsCarried++;
      break;   // 1 晩の上限 = 後ろの窓は次の晩
    }
  }
  rep.windowsLeft = wins.length - rep.windowsDone;
  return rep;
}

/**
 * ⓪ → ① ② を 1 回 (予約は自分の短い取引・本体は自分の取引)。1b-0e の engine は ⓪ と ①② を自分の取引の中に置く (この関数は試験と人の手順のため)
 *   opts.now / opts.window ({ since, until }) / opts.failBody (試験 = 本体の取引を巻き戻す)
 */
export async function runReresolveNight(db, opts) {
  const o = { ...RERESOLVE_NIGHT_DEFAULTS, ...opts };
  checkNightOptions(o);
  const win = o.window || nightWindow(o.now || new Date(), o.pastDays);
  await reserveNight(db, { company: o.company, mall: o.mall, since: win.since, until: win.until });
  await db.query('begin');
  try {
    const rep = await runNightBody(db, o);
    if (o.failBody) throw Object.assign(new Error('本体の取引の失敗 (試験)'), { code: 'RERESOLVE_TEST_FAIL' });
    await db.query('commit');
    return { ...rep, committed: true, window: win };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    if (e.code === 'RERESOLVE_TEST_FAIL') return { committed: false, error: e.message, window: win };
    throw e;
  }
}

/**
 * retry の表の状態 (報告の材料・読むだけ)。自分で read only の取引を開く (statement_timeout 5 秒) = 呼び手は取引の外で呼ぶ
 *   戻り = { Q, L, Lmax, att7, old7, cursor: { after, through, startedAt, cyclesCompleted } | null, cycleOld }
 */
export async function readRetryState(db, { company, mall, statementTimeout = '5s' }) {
  await db.query('begin read only');
  try {
    await db.query(`set local statement_timeout = '${String(statementTimeout).replace(/[^0-9a-z ]/gi, '')}'`);
    const a = (await db.query(`with q as (
        select t.order_id, t.attempts, t.first_skipped_at,
               (select count(*)::integer from core.order_lines l
                 where l.order_id = t.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null) as n
          from ops.reresolve_retry_orders t where t.company_id = $1::smallint and t.mall = $2)
      select count(*)::integer as q, coalesce(sum(n), 0)::bigint as l, coalesce(max(n), 0)::integer as lmax,
             count(*) filter (where attempts >= $3)::integer as att7,
             count(*) filter (where first_skipped_at < now() - make_interval(days => $4))::integer as old7
        from q`, [company, mall, WARN_ATTEMPTS, WARN_DAYS])).rows[0];
    const c = (await db.query(`select after_order_id, cycle_through_order_id, cycle_started_at, cycles_completed,
        (cycle_through_order_id is not null and cycle_started_at < now() - make_interval(days => $3)) as cycle_old
      from ops.reresolve_retry_cursor where company_id = $1::smallint and mall = $2`, [company, mall, WARN_DAYS])).rows[0];
    await db.query('commit');
    return {
      Q: Number(a.q), L: Number(a.l), Lmax: Number(a.lmax), att7: Number(a.att7), old7: Number(a.old7),
      cursor: c ? { after: toBig(c.after_order_id), through: toBig(c.cycle_through_order_id), startedAt: c.cycle_started_at, cyclesCompleted: Number(c.cycles_completed) } : null,
      cycleOld: !!(c && c.cycle_old),
    };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
}

/**
 * 1 周の見込みの晩の数 (純粋な計算)
 *   最悪の保証 worst = ⌈Q ÷ R⌉ (1 batch は少なくとも 1 注文)
 *   式 formula = ⌈B ÷ R⌉・B = min(Q, ⌊Q ÷ maxOrders⌋ + ⌊L ÷ d⌋ + 1)・d = maxLines − Lmax + 1 (d ≦ 0 なら B = Q・Q = 0 なら B = 0)
 *   実測 measured = ⌈Q ÷ (R × e)⌉・e = 直近の retry の batch のうち has_more で止まった batch の orders_examined の最小 (無ければ null)
 *   見込み est = max(formula, measured)
 */
export function estimateCycleNights({ Q, L, Lmax, R, maxOrders, maxLines, recentBatches = [] }) {
  if (!(isInt(R) && R >= 1)) throw new Error(`R は 1 以上 (${R})`);
  const d = maxLines - Lmax + 1;
  const B = Q === 0 ? 0 : (d <= 0 ? Q : Math.min(Q, Math.floor(Q / maxOrders) + Math.floor(L / d) + 1));
  const worst = Math.ceil(Q / R);
  const formula = Math.ceil(B / R);
  const full = recentBatches.filter((b) => b.hasMore).map((b) => b.examined).filter((x) => x > 0);
  const e = full.length ? Math.min(...full) : null;
  const measured = e ? Math.ceil(Q / (R * e)) : null;
  return { Q, L, Lmax, B, d, worst, formula, e, measured, est: Math.max(formula, measured ?? 0) };
}

/** ⚠️ の 4 つ (どれか 1 つで出す・失敗にはしない) */
export function retryWarnings(state, estimate) {
  const w = [];
  if (estimate.est > WARN_DAYS) w.push({ kind: 'cycle_nights', text: `1 周の見込み ${estimate.est} 晩 > ${WARN_DAYS} 晩 (R か N を見直す)` });
  if (state.att7 > 0) w.push({ kind: 'attempts', text: `attempts ≧ ${WARN_ATTEMPTS} の注文 ${state.att7} (別の書き手が長く持っている印)` });
  if (state.old7 > 0) w.push({ kind: 'first_skipped', text: `${WARN_DAYS} 日より前に skip された注文 ${state.old7}` });
  if (state.cycleOld) w.push({ kind: 'cycle_started', text: `周回の始まりが ${WARN_DAYS} 日より前 (周回の途中)` });
  return w;
}

/** 報告の 1 行 (失敗にしない) */
export function retryReportLine(mall, state, estimate, warnings) {
  const c = state.cursor;
  const pos = c ? `cursor ${c.after}${c.through === null ? ' (周回の外)' : ` / high-water ${c.through} / 周回の始まり ${c.startedAt ? new Date(c.startedAt).toISOString() : '-'}`}` : 'cursor なし';
  return `${mall}: retry の表の残り ${state.Q} 注文 (未解決の明細 ${state.L}) / ${pos} / 1 周の見込み ${estimate.est} 晩 (式 ${estimate.formula}・実測 ${estimate.measured ?? '-'}・最悪 ${estimate.worst})`
    + (warnings.length ? ` / ⚠️ ${warnings.map((x) => x.text).join('・')}` : '');
}
