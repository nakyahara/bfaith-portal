/**
 * stock-diff.mjs — 在庫の「増えた / 減った」を、完走した日どうしの差から作る (Company DB構想 08 §3.2 の 3 段目 / §3.3 ②。D2c)
 *
 *   snapshots.sku_stock_daily (source = 'logizard'。complete の日) の 前日 → 当日 の差 (SKU 単位) を
 *   events.inventory_events に confidence = 'inferred' で追記する。理由 (入荷・出荷・棚卸し…) は分からない = reason_code は null。
 *   入荷検品・ピッキング・梱包からの exact のイベント (二重書き) とは別もの。exact がそろうまでの「何かが動いた」の記録。
 *
 * 約束:
 *   - **新しい定期実行は作らない**。毎時ジョブ (inventory-hourly.mjs) が、日の締めの後に呼ぶ
 *   - 作り終えた日は snapshots.stock_diff_days (0022) に印を残す。**イベントの追記と同じ取引**。差が 0 件の日 (倉庫が動かない日) も done で残る
 *     = 「イベントがある = 済んだ」と読まない (件数を完了の代わりにしない)。止まっていた日は、次に動いた回が古い順に追いつく
 *   - 間に取れなかった日がある区間は作らない (08 §3.2): 前日が missing / partial → skipped (prev_not_complete)。最初の日 → skipped (first_day)
 *   - complete の日は変わらない (0011 の trigger) ので、同じ 2 日からは何度作っても同じ差。idempotency_key = 'lzdiff:v1:<scope>:<sku_id>:<前日>:<当日>' + on conflict do nothing
 *   - 🚨 ただし保守の手順で締めをやり直すと、日次は変わりうる。inferred のイベントは日次から作り直せる派生データ = やり直すときは、その区間のイベントも保守の手順で消す (README)。
 *     消し忘れても黙って done にしない: 追記の後に「その区間のイベントの集合 = いまの日次から作った差」を照合し、合わなければ例外 (印は付かない。Codex #1396 R1)
 *   - 0022 が未適用のあいだは何もしない ({ status: 'not_migrated' })。毎時ジョブは落とさない
 *
 * 差の式 (版 = CALC_VERSION。変えるときは版を上げる = 印の主キーとイベントの鍵の両方に入っている):
 *   - 商品コード (source_code) ごとに 前日と当日を突き合わせ、行が無い側は 0 (08 §3.2「行が無い sku = その scope に在庫が無い」)
 *   - SKU は 当日の sku_id、無ければ前日の sku_id (間に SKU が登録されたコードを「+全量」と読まない = コードで突き合わせてから SKU に寄せる)
 *   - 前日と当日で別の SKU に当たるコード (マスタの付け替え) は、前の SKU の −全量 と 新しい SKU の +全量 に分ける
 *   - 両日とも SKU が分からないコードは、イベントにできない (events.inventory_events.sku_id は not null) → 変わった数だけ数えて印に残す (unresolved_changed)
 *   - occurred_at = 当日の世代 (その日の最後に完走した取得の時刻)。本当に動いたのは 前日の世代〜当日の世代 の間のどこか (payload に両方の世代を残す)
 *   - location_id は入れない (SKU 単位の差。ロケ単位の移動は棚移動で +/− が打ち消し合うだけの雑音になる)
 */
import { SOURCE, SCOPE, COMPANY_ID } from './logizard.mjs';

// 🚨 版を上げるときは CALC_VERSION を変えるだけでは足りない: 同じ区間 (source_ref) に旧版のイベントが残る → 下の照合は版で絞っていないので合わなくなる (値が同じなら二重に残る)。
//    旧版のイベントを消して置き換えるか、版別の照合と読み取りにするかを、版を上げる PR で一緒に決める (Codex #1396 R2 Low)
export const CALC_VERSION = 'lzdiff:v1';
export const SOURCE_SYSTEM = 'logizard_diff';
/** イベントの source_ref = 区間の名前。保守でその区間のイベントを消すとき・作った後の照合で引く (0022 の索引) */
export const sourceRefOf = (scope, from, to) => `${scope}:${from}..${to}`;
const LOCK_KEY = 'company-db-stock-diff';
const rollbackQuiet = async (db) => { try { await db.exec('rollback'); } catch { /* 取引の外なら何もしない */ } };
const affected = (r) => r.rowCount ?? r.affectedRows ?? 0;

/** 'YYYY-MM-DD' の前日 (Date の TZ を経由しない) */
export function prevDay(d) {
  const [y, m, day] = d.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, day - 1)).toISOString().slice(0, 10);
}

/**
 * 1 日ぶん (当日 = day) の差を作る (1 取引)。
 * 戻り値 = { day, status: 'done'|'skipped'|'waiting'|'locked'|'exists', from, events, unresolvedChanged, skipReason }
 *   waiting = 前日の締めがまだ (capture の行が無い・building)。印は付けない = 次の回にもう一度見る
 */
export async function inferStockDiffDay(db, day, { companyId = COMPANY_ID, source = SOURCE, scope = SCOPE, log = () => {} } = {}) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error(`day は 'YYYY-MM-DD': ${day}`);
  if (source !== SOURCE) throw new Error(`いま差を作れるのは ${SOURCE} だけ (世代 = ops.ingest_runs.checksum が取得時刻、という前提がほかの source では成り立たない): ${source}`);
  const one = async (sql, p) => (await db.query(sql, p)).rows[0];
  const from = prevDay(day);
  await db.exec('begin');
  try {
    // 同時に 2 本走っても、同じ日を二重に作らない (印の主キーでも止まるが、待たずに見送る)
    const got = (await one(`select pg_try_advisory_xact_lock(hashtext($1)) as got`, [`${LOCK_KEY}:${source}:${scope}`])).got;
    if (!got) { await rollbackQuiet(db); return { day, status: 'locked' }; }
    if (await one(`select 1 as x from snapshots.stock_diff_days where to_date = $1::date and source = $2 and scope_key = $3 and calc_version = $4`, [day, source, scope, CALC_VERSION])) {
      await rollbackQuiet(db); return { day, status: 'exists' };
    }
    const cur = await one(
      `select d.status, d.company_id, r.checksum as generation from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
        where d.snapshot_date = $1::date and d.source = $2 and d.scope_key = $3 for share of d`, [day, source, scope]);
    if (!cur || cur.status !== 'complete') throw new Error(`${day} は complete ではない (${cur ? cur.status : '行が無い'})`);
    if (Number(cur.company_id) !== Number(companyId)) throw new Error(`${day} の取得記録は別の会社 (${cur.company_id})`);
    const prev = await one(
      `select d.status, r.checksum as generation from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
        where d.snapshot_date = $1::date and d.source = $2 and d.scope_key = $3 for share of d`, [from, source, scope]);
    const mark = (status, fromDate, skipReason, events, unresolved) => db.query(
      `insert into snapshots.stock_diff_days (to_date, source, scope_key, calc_version, company_id, from_date, status, skip_reason, events, unresolved_changed)
       values ($1::date, $2, $3, $4, $5::smallint, $6::date, $7, $8, $9, $10)`, [day, source, scope, CALC_VERSION, companyId, fromDate, status, skipReason, events, unresolved]);
    let skipReason = null;
    if (!prev) {
      // 前日の行が無い: それより前に 1 日も無ければ「最初の日」。あるなら、前日の締めがまだ (保守で消して作り直している途中など) = 待つ
      const older = await one(`select 1 as x from snapshots.stock_capture_days where source = $1 and scope_key = $2 and snapshot_date < $3::date limit 1`, [source, scope, day]);
      if (older) { await rollbackQuiet(db); log(`${day}: 前日 (${from}) の締めがまだ → 待つ`); return { day, status: 'waiting', from }; }
      skipReason = 'first_day';
    } else if (prev.status === 'building') {
      await rollbackQuiet(db); log(`${day}: 前日 (${from}) が作りかけ → 待つ`); return { day, status: 'waiting', from };
    } else if (prev.status !== 'complete') skipReason = 'prev_not_complete';
    if (skipReason) {
      await mark('skipped', null, skipReason, 0, 0);
      await db.exec('commit');
      log(`${day}: skipped (${skipReason})`);
      return { day, status: 'skipped', from: null, events: 0, unresolvedChanged: 0, skipReason };
    }
    const J = `
      with a as (select source_code, sku_id, qty from snapshots.sku_stock_daily where snapshot_date = $1::text::date and source = $3::text and scope_key = $4::text and company_id = $5::smallint),
           b as (select source_code, sku_id, qty from snapshots.sku_stock_daily where snapshot_date = $2::text::date and source = $3::text and scope_key = $4::text and company_id = $5::smallint),
           j as (select a.sku_id as sa, b.sku_id as sb, coalesce(a.qty, 0)::bigint as qa, coalesce(b.qty, 0)::bigint as qb from a full join b using (source_code))`;
    const base = [from, day, source, scope, companyId];
    const unresolved = Number((await one(`${J} select count(*)::int as n from j where sa is null and sb is null and qa <> qb`, base)).n);
    const D = `${J},
           parts as (
             select coalesce(sb, sa) as sku_id, qa, qb from j where sa is null or sb is null or sa = sb
             union all select sa, qa, 0::bigint from j where sa is not null and sb is not null and sa <> sb
             union all select sb, 0::bigint, qb from j where sa is not null and sb is not null and sa <> sb),
           d as (select sku_id, sum(qa) as qa, sum(qb) as qb from parts where sku_id is not null group by sku_id)`;
    // 印に残す件数 = 数量が変わった SKU の数 (= あるべきイベントの数)。追記できた行数にはしない (印だけ消して作り直した回は、イベントが既にあって 0 行になる)
    const events = Number((await one(`${D} select count(*)::int as n from d where qb <> qa`, base)).n);
    const inserted = affected(await db.query(
      `${D}
       insert into events.inventory_events (company_id, occurred_at, actor_type, source_system, source_ref, idempotency_key, payload, sku_id, qty_delta, qty_after, confidence)
       select $5::smallint, $6::text::timestamptz, 'system', $7::text, $4::text || ':' || $1::text || '..' || $2::text, $8::text || ':' || $4::text || ':' || d.sku_id || ':' || $1::text || ':' || $2::text,
              jsonb_build_object('calc', $8::text, 'source', $3::text, 'scope', $4::text, 'from', $1::text, 'to', $2::text, 'qty_before', d.qa, 'from_generation', $9::text, 'to_generation', $6::text),
              d.sku_id, (d.qb - d.qa)::integer, d.qb::integer, 'inferred'
         from d where d.qb <> d.qa
       on conflict (idempotency_key) do nothing`, [...base, cur.generation, SOURCE_SYSTEM, CALC_VERSION, prev.generation]));
    if (inserted > events) throw new Error(`追記した行数 ${inserted} が、変わった SKU の数 ${events} より多い`);
    // 🚨 その区間のイベントの集合が、いまの日次から作った差と同じであること (SKU・差・差の後の数量・両日の世代)。
    //    締めをやり直して日次が変わったのに古いイベントが残っていると、同じ鍵は on conflict で読み飛ばされ・差が 0 になった SKU の古いイベントも残る → 印だけ done にしない
    const bad = await one(
      `${D},
           want as (select sku_id, (qb - qa)::bigint as delta, qb::bigint as after from d where qb <> qa),
           have as (select sku_id, qty_delta::bigint as delta, qty_after::bigint as after, payload->>'from_generation' as fg, payload->>'to_generation' as tg
                      from events.inventory_events where source_system = $6::text and source_ref = $7::text)
       select count(*)::int as n, min(coalesce(w.sku_id, h.sku_id)) as sku_id
         from want w full join have h using (sku_id)
        where w.sku_id is null or h.sku_id is null or w.delta <> h.delta or w.after is distinct from h.after or h.fg is distinct from $8::text or h.tg is distinct from $9::text`,
      [...base, SOURCE_SYSTEM, sourceRefOf(scope, from, day), prev.generation, cur.generation]);
    if (Number(bad.n) > 0) {
      throw Object.assign(new Error(`${from}..${day} のイベントが既にあり、いまの日次から作った差と ${bad.n} SKU で合わない (例 sku_id ${bad.sku_id})。締めをやり直したなら、README「締めをやり直す」の手順でこの区間の inferred のイベントを消してから`), { code: 'STOCK_DIFF_MISMATCH' });
    }
    await mark('done', from, null, events, unresolved);
    await db.exec('commit');
    log(`${day}: done (${from} との差 = イベント ${events} 件${unresolved ? ` / SKU が分からず作れなかった ${unresolved} コード` : ''})`);
    return { day, status: 'done', from, events, unresolvedChanged: unresolved, skipReason: null };
  } catch (e) {
    await rollbackQuiet(db);
    throw Object.assign(new Error(`${day} の在庫の差を作れない: ${e.message}`), { code: e.code || 'STOCK_DIFF_FAILED', day });
  }
}

/**
 * まだ差を作っていない complete の日を、古い順に全部作る。
 * 戻り値 = { status: 'ok'|'not_migrated', days: [inferStockDiffDay の戻り値...], backlog, locked }
 *   backlog = maxDays で打ち切った・待ちの日がある・別の回が走っていた = まだ残っている
 */
export async function inferStockDiffs(db, { source = SOURCE, scope = SCOPE, maxDays = 60, log = () => {}, ...rest } = {}) {
  const migrated = (await db.query(`select to_regclass('snapshots.stock_diff_days') is not null as ok`)).rows[0].ok;
  if (!migrated) return { status: 'not_migrated', days: [], backlog: false, locked: false };
  const pending = (await db.query(
    `select d.snapshot_date::text as day from snapshots.stock_capture_days d
      where d.source = $1 and d.scope_key = $2 and d.status = 'complete'
        and not exists (select 1 from snapshots.stock_diff_days x where x.to_date = d.snapshot_date and x.source = d.source and x.scope_key = d.scope_key and x.calc_version = $3)
      order by d.snapshot_date limit $4::integer`, [source, scope, CALC_VERSION, maxDays + 1])).rows.map((r) => r.day);
  const days = []; let backlog = pending.length > maxDays, locked = false;
  for (const day of pending.slice(0, maxDays)) {
    const r = await inferStockDiffDay(db, day, { source, scope, log, ...rest });
    if (r.status === 'locked') { locked = true; backlog = true; break; }
    if (r.status === 'waiting') { backlog = true; continue; }   // この日は待つ。後ろの日は前日がそろっていれば作れる
    if (r.status !== 'exists') days.push(r);
  }
  return { status: 'ok', days, backlog, locked };
}
