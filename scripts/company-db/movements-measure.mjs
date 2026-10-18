/**
 * movements-measure.mjs — 商品ごとの入出庫履歴 (CompanyDB構想 26) の PR-0: 本番を **読むだけ** で測る (書かない)
 *
 * 測ること:
 *   1. 材料の表ごとの行数と、いちばん古い日・新しい日
 *      (core.shipments / shipment_lines / snapshots.stock_capture_days / sku_stock_daily / warehouse_stock_daily /
 *       events.inventory_events / snapshots.stock_diff_days / core.purchase_orders)
 *   2. 出荷の店舗ごとの件数 (FBA納品 = 15 が入っているか) と、出荷確定の時刻 (JST の時) の分かれ方
 *   3. ロジザードの毎日の在庫の取得時刻 (JST の時) の分かれ方
 *   4. 動いている単品 約 30 個 × 直近 30 日で、「在庫の日次の増減 ≠ 出荷」の日がどれだけあるか (26 §4.1・Q1)
 *      - 基準は 2 つ: ロジザードの日次 (logizard) と NE の在庫の日次 (ne)
 *      - 出荷の切り方は 2 つ: 取得時刻で切る (time = 設計どおり) / 出荷確定日で切る (date)
 *      - ずれの大きさの分かれ方・「今日 +N / 明日 −N」の組 (時刻のずれで起きる組) の数
 *      🚨 仕入 (入荷) は発注アプリ (Render の SQLite) にしか無い = miniPC からは読めない。
 *         なので、ここでいう「合わない分」には仕入も入っている (増える側のずれの多くは仕入のはず)
 *   5. 画面で使う形の問い合わせの速さ (いちばん売れている単品で explain analyze)
 *   6. (DATA_DIR があれば) miniPC の warehouse.db の NE 伝票と、Company DB の出荷の数・新しさの突き合わせ
 *
 * 安全:
 *   - Company DB は照会用のロール (COMPANY_DB_WATCH_URL = watcher・select だけ) で、begin read only の取引の中で読む。最後は rollback
 *   - statement_timeout を取引の中だけ 60 秒にする (重い問い合わせで本番を止めない)
 *   - warehouse.db は読み取り専用 (readonly・fileMustExist) で開く
 *   - 出すのは件数・数量・商品コードだけ (注文番号・宛先・送り状番号などは出さない)
 *
 * 使い方 (miniPC):
 *   set DOTENV_CONFIG_PATH=C:\Users\bfaith\bfaith-portal\.env&& set DATA_DIR=C:\Users\bfaith\bfaith-portal\data&& node -r dotenv/config scripts/company-db/movements-measure.mjs
 *   オプション: --days 30 (見る日数・7〜90) --sample 30 (見る単品の数・5〜100) --json-only (日本語のまとめを出さない)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient } from './migrate.mjs';

const COMPANY_ID = 1;
const DAY_MS = 86400000;
const jstHour = (d) => new Date(new Date(d).getTime() + 9 * 3600000).getUTCHours();
const jstDate = (d) => new Date(new Date(d).getTime() + 9 * 3600000).toISOString().slice(0, 10);
const addDays = (ymd, n) => new Date(Date.parse(`${ymd}T00:00:00Z`) + n * DAY_MS).toISOString().slice(0, 10);
const num = (v) => (v == null ? null : Number(v));

/** ずれの大きさの段 */
export function sizeBucket(n) {
  const a = Math.abs(n);
  if (a === 0) return '0';
  if (a === 1) return '1';
  if (a <= 5) return '2-5';
  if (a <= 20) return '6-20';
  if (a <= 100) return '21-100';
  return '101+';
}

/**
 * 1 つの SKU・1 つの基準の、区間ごとの「合わない分」を作る (純粋な関数・試験で使う)
 * @param days   complete の日 [{ day, gen (Date|string|null) }] 日付の順
 * @param stock  Map(day → 数) complete の日に行が無い = 0
 * @param ships  [{ at (Date|string), date ('YYYY-MM-DD'), qty }]
 * @returns [{ from, to, gapDays, delta, shippedTime, shippedDate, unTime, unDate }]
 *   un = (当日の在庫 − 前の在庫) + 区間の出荷。0 = 出荷だけで説明できる。正 = 増えた (仕入・返品・棚卸しなど)・負 = 減った (他勘定出庫など)
 */
export function intervalsOf(days, stock, ships) {
  const out = [];
  for (let i = 1; i < days.length; i++) {
    const p = days[i - 1], d = days[i];
    const sp = stock.get(p.day) ?? 0, sd = stock.get(d.day) ?? 0;
    const gp = p.gen ? new Date(p.gen).getTime() : null, gd = d.gen ? new Date(d.gen).getTime() : null;
    let st = null, sdt = 0;
    if (gp != null && gd != null) {
      st = 0;
      for (const s of ships) { const t = new Date(s.at).getTime(); if (t > gp && t <= gd) st += s.qty; }
    }
    for (const s of ships) if (s.date > p.day && s.date <= d.day) sdt += s.qty;
    const delta = sd - sp;
    out.push({ from: p.day, to: d.day, gapDays: Math.round((Date.parse(d.day) - Date.parse(p.day)) / DAY_MS), delta, shippedTime: st, shippedDate: sdt,
      unTime: st == null ? null : delta + st, unDate: delta + sdt });
  }
  return out;
}

/** 区間の「合わない分」の列から、数・大きさ・組 (今日 +N / 明日 −N) を数える */
export function statsOf(values) {
  const v = values.filter((x) => x != null);
  const buckets = { '1': 0, '2-5': 0, '6-20': 0, '21-100': 0, '101+': 0 };
  let nonzero = 0, pos = 0, neg = 0, absSum = 0, posSum = 0, negSum = 0;
  for (const x of v) {
    if (x === 0) continue;
    nonzero++; absSum += Math.abs(x); buckets[sizeBucket(x)]++;
    if (x > 0) { pos++; posSum += x; } else { neg++; negSum += x; }
  }
  // 組 = 続く 2 区間がちょうど打ち消し合う (u_i + u_{i+1} = 0)。前から順に、重ねずに数える
  let pairs = 0, pairQty = 0, oppositeNext = 0;
  for (let i = 0; i + 1 < values.length; i++) {
    const a = values[i], b = values[i + 1];
    if (a == null || b == null || a === 0 || b === 0) continue;
    if (Math.sign(a) !== Math.sign(b)) oppositeNext++;
  }
  for (let i = 0; i + 1 < values.length; i++) {
    const a = values[i], b = values[i + 1];
    if (a != null && b != null && a !== 0 && a + b === 0) { pairs++; pairQty += Math.abs(a); i++; }
  }
  return { intervals: v.length, nonzero, pos, neg, posSum, negSum, absSum, buckets, pairs, pairQty, oppositeNext, nonzeroAfterPairs: nonzero - 2 * pairs };
}

const addStats = (acc, s) => {
  if (!acc) return JSON.parse(JSON.stringify(s));
  for (const k of Object.keys(s)) {
    if (k === 'buckets') for (const b of Object.keys(s.buckets)) acc.buckets[b] += s.buckets[b];
    else acc[k] += s[k];
  }
  return acc;
};

/**
 * 測る本体 (c = { query }。取引は呼ぶ側)
 * @returns 結果の JSON
 */
export async function measure(c, { days = 30, sample = 30, now = new Date(), log = () => {} } = {}) {
  const q = async (label, sql, params = []) => {
    const t0 = Date.now();
    const rows = (await c.query(sql, params)).rows;
    log(`${label}: ${rows.length} 行・${Date.now() - t0} ms`);
    return rows;
  };
  const one = async (label, sql, params) => (await q(label, sql, params))[0];
  const today = jstDate(now);
  const from = addDays(today, -days);
  const out = { as_of: now.toISOString(), today_jst: today, window: { from, to: today, days }, sources: {}, notes: [] };

  // ── 1. 表ごとの行数と日付の幅
  out.sources.shipments = await one('出荷 (伝票)',
    `select count(*)::int as rows, count(*) filter (where is_cancelled)::int as cancelled, count(*) filter (where shipped_at is not null)::int as shipped,
            min(ship_date_jst)::text as min_ship_date, max(ship_date_jst)::text as max_ship_date,
            min(order_date_jst)::text as min_order_date, max(order_date_jst)::text as max_order_date, max(shipped_at) as max_shipped_at
       from core.shipments where company_id = $1`, [COMPANY_ID]);
  out.sources.shipment_lines = await one('出荷 (明細)',
    `select count(*)::int as rows, count(*) filter (where sku_id is null)::int as unresolved, count(*) filter (where removed_at is not null)::int as removed,
            count(*) filter (where is_cancelled)::int as cancelled
       from core.shipment_lines where company_id = $1`, [COMPANY_ID]);
  out.sources.stock_capture_days = await q('在庫の取得の日',
    `select source, scope_key, count(*)::int as days, count(*) filter (where status = 'complete')::int as complete,
            count(*) filter (where status = 'partial')::int as partial, count(*) filter (where status = 'missing')::int as missing,
            min(snapshot_date)::text as min_date, max(snapshot_date)::text as max_date,
            min(snapshot_date) filter (where status = 'complete')::text as min_complete, max(snapshot_date) filter (where status = 'complete')::text as max_complete,
            count(*) filter (where captured_at is null and status <> 'missing')::int as no_captured_at
       from snapshots.stock_capture_days where company_id = $1 group by 1, 2 order by 1, 2`, [COMPANY_ID]);
  out.sources.sku_stock_daily = await q('在庫の日次 (SKU)',
    `select source, count(*)::int as rows, count(*) filter (where sku_id is null)::int as no_sku, min(snapshot_date)::text as min_date, max(snapshot_date)::text as max_date
       from snapshots.sku_stock_daily where company_id = $1 group by 1 order by 1`, [COMPANY_ID]);
  out.sources.warehouse_stock_daily = await one('在庫の日次 (ロケ)',
    `select count(*)::int as rows, min(snapshot_date)::text as min_date, max(snapshot_date)::text as max_date from snapshots.warehouse_stock_daily where company_id = $1`, [COMPANY_ID]);
  out.sources.inventory_events = await q('在庫のイベント',
    `select source_system, confidence, count(*)::int as rows, min(occurred_at) as min_at, max(occurred_at) as max_at
       from events.inventory_events where company_id = $1 group by 1, 2 order by 1, 2`, [COMPANY_ID]);
  out.sources.stock_diff_days = (await one('差の印の表', `select to_regclass('snapshots.stock_diff_days') is not null as ok`)).ok
    ? await q('差の印', `select status, coalesce(skip_reason, '') as skip_reason, count(*)::int as days, min(to_date)::text as min_date, max(to_date)::text as max_date,
                               sum(events)::int as events, sum(unresolved_changed)::int as unresolved from snapshots.stock_diff_days group by 1, 2 order by 1, 2`)
    : 'not_migrated';
  out.sources.purchase_orders_in_company_db = await one('発注 (Company DB の受け皿)',
    `select (select count(*)::int from core.purchase_orders) as orders, (select count(*)::int from core.purchase_order_lines) as lines`);

  // ── 2. 出荷の店舗と時刻 (直近の期間)
  out.shipments_by_shop = await q('店舗ごとの出荷',
    `select s.shop_code, coalesce(n.shop_name, '?') as shop_name, count(*)::int as slips, count(*) filter (where s.is_cancelled)::int as cancelled
       from core.shipments s left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code
      where s.company_id = $1 and s.ship_date_jst >= $2::date group by 1, 2 order by count(*) desc`, [COMPANY_ID, from]);
  const shipHours = await q('出荷確定の時刻',
    `select extract(hour from s.shipped_at at time zone 'Asia/Tokyo')::int as h, count(*)::int as slips
       from core.shipments s where s.company_id = $1 and s.ship_date_jst >= $2::date and not s.is_cancelled group by 1 order by 1`, [COMPANY_ID, from]);
  out.shipped_at_hour_jst = Object.fromEntries(shipHours.map((r) => [r.h, r.slips]));
  out.set_lines_in_shipments = await one('出荷の明細のうちセットの SKU',
    `select count(*)::int as lines, count(*) filter (where k.sku_kind = 'set')::int as set_lines, count(*) filter (where k.sku_kind = 'exception')::int as exception_lines
       from core.shipments s join core.shipment_lines l on l.shipment_id = s.shipment_id join core.skus k on k.sku_id = l.sku_id
      where s.company_id = $1 and s.ship_date_jst >= $2::date and l.removed_at is null`, [COMPANY_ID, from]);

  // ── 3. 在庫の取得の日と時刻 (logizard / ne)
  const capRows = await q('取得の日 (期間)',
    `select d.source, d.scope_key, d.snapshot_date::text as day, d.status,
            coalesce(d.captured_at, case when d.source = 'logizard' and r.checksum ~ '^\\d{4}-\\d{2}-\\d{2}T' then r.checksum::timestamptz end) as gen
       from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
      where d.company_id = $1 and d.source in ('logizard', 'ne') and d.snapshot_date >= $2::date - 1
      order by d.source, d.scope_key, d.snapshot_date`, [COMPANY_ID, from]);
  const scopeOf = {};
  for (const src of ['logizard', 'ne']) {
    const cnt = {};
    for (const r of capRows) if (r.source === src && r.status === 'complete') cnt[r.scope_key] = (cnt[r.scope_key] || 0) + 1;
    scopeOf[src] = Object.keys(cnt).sort((a, b) => cnt[b] - cnt[a])[0] || null;
  }
  const daysOf = {};
  out.capture = {};
  for (const src of ['logizard', 'ne']) {
    const rows = capRows.filter((r) => r.source === src && r.scope_key === scopeOf[src]);
    daysOf[src] = rows.filter((r) => r.status === 'complete').map((r) => ({ day: r.day, gen: r.gen }));
    const hours = {};
    for (const r of daysOf[src]) if (r.gen) { const h = jstHour(r.gen); hours[h] = (hours[h] || 0) + 1; }
    out.capture[src] = { scope: scopeOf[src], days: rows.length, complete: daysOf[src].length, not_complete: rows.filter((r) => r.status !== 'complete').map((r) => `${r.day}:${r.status}`),
      gen_hour_jst: hours, gen_date_mismatch: daysOf[src].filter((r) => r.gen && jstDate(r.gen) !== r.day).length };
  }

  // ── 4. 単品の見本: 期間の出荷数の多い順に上位 + 中ほどから間引いて
  const ranked = await q('出荷の多い単品',
    `select l.sku_id::text as sku_id, k.code, sum(l.qty)::int as qty, count(*)::int as lines
       from core.shipments s join core.shipment_lines l on l.shipment_id = s.shipment_id join core.skus k on k.sku_id = l.sku_id
      where s.company_id = $1 and s.ship_date_jst >= $2::date and not s.is_cancelled and s.shipped_at is not null
        and l.removed_at is null and not l.is_cancelled and k.sku_kind = 'single'
      group by 1, 2 order by sum(l.qty) desc, 1 limit 400`, [COMPANY_ID, from]);
  const topN = Math.min(ranked.length, Math.ceil(sample * 2 / 3));
  const picks = ranked.slice(0, topN);
  const rest = ranked.slice(topN);
  const want = sample - picks.length;
  if (want > 0 && rest.length) { const step = Math.max(1, Math.floor(rest.length / want)); for (let i = 0; i < rest.length && picks.length < sample; i += step) picks.push(rest[i]); }
  out.sample = { skus: picks.length, ranked_singles: ranked.length, top: topN };
  const ids = picks.map((p) => p.sku_id);
  if (!ids.length) { out.notes.push('期間に出荷のある単品が無い'); return out; }

  const stockRows = await q('見本の在庫の日次',
    `select source, scope_key, snapshot_date::text as day, sku_id::text as sku_id, sum(qty)::int as qty, count(*)::int as codes
       from snapshots.sku_stock_daily where sku_id = any($1::bigint[]) and source in ('logizard', 'ne') and snapshot_date >= $2::date - 1
      group by 1, 2, 3, 4`, [ids, from]);
  const shipRows = await q('見本の出荷',
    `select l.sku_id::text as sku_id, s.shipped_at as at, s.ship_date_jst::text as date, l.qty
       from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
      where l.sku_id = any($1::bigint[]) and l.removed_at is null and not l.is_cancelled and not s.is_cancelled
        and s.shipped_at >= ($2::date - 3)::timestamp at time zone 'Asia/Tokyo'`, [ids, from]);
  out.sample.multi_code_days = stockRows.filter((r) => r.codes > 1).length;

  const total = {}; const perSku = [];
  for (const p of picks) {
    const ships = shipRows.filter((s) => s.sku_id === p.sku_id).map((s) => ({ at: s.at, date: s.date, qty: Number(s.qty) }));
    const row = { code: p.code, shipped: p.qty };
    for (const src of ['logizard', 'ne']) {
      const st = new Map(stockRows.filter((r) => r.source === src && r.scope_key === scopeOf[src] && r.sku_id === p.sku_id).map((r) => [r.day, Number(r.qty)]));
      const iv = intervalsOf(daysOf[src], st, ships);
      for (const how of ['time', 'date']) {
        const vals = iv.map((x) => (how === 'time' ? x.unTime : x.unDate));
        const s = statsOf(vals);
        total[`${src}_${how}`] = addStats(total[`${src}_${how}`], s);
        if (how === 'time') row[src] = { nonzero: s.nonzero, pairs: s.pairs, pos: s.posSum, neg: s.negSum, gaps: iv.filter((x) => x.gapDays > 1).length };
      }
    }
    perSku.push(row);
  }
  out.unexplained = total;
  out.per_sku = perSku;

  // ── 5. 画面の問い合わせの速さ (いちばん売れている単品・13 か月)
  const explain = async (label, sql, params) => {
    const r = await q(label, `explain (analyze, buffers, format json) ${sql}`, params);
    const plan = r[0]['QUERY PLAN'][0];
    const idx = new Set();
    const walk = (n) => { if (n['Index Name']) idx.add(n['Index Name']); for (const k of n.Plans || []) walk(k); };
    walk(plan.Plan);
    return { ms: Math.round(plan['Execution Time']), rows: plan.Plan['Actual Rows'], indexes: [...idx] };
  };
  const top = picks[0].sku_id;
  out.speed = {
    sku: picks[0].code,
    shipments_13m: await explain('速さ: 出荷 13 か月',
      `select s.shipped_at, s.ne_slip_no, s.shop_code, l.qty from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
        where l.sku_id = $1::bigint and l.removed_at is null and not l.is_cancelled and not s.is_cancelled and s.shipped_at >= now() - interval '13 months'
        order by s.shipped_at desc limit 200`, [top]),
    shipments_monthly_13m: await explain('速さ: 出荷の月別 13 か月',
      `select date_trunc('month', s.order_date_jst) as m, sum(l.qty) from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
        where l.sku_id = $1::bigint and l.removed_at is null and not l.is_cancelled and not s.is_cancelled and s.order_date_jst >= current_date - interval '13 months' group by 1`, [top]),
    stock_daily_13m: await explain('速さ: 在庫の日次 13 か月',
      `select snapshot_date, sum(qty) from snapshots.sku_stock_daily where sku_id = $1::bigint and source = 'logizard' and snapshot_date >= current_date - interval '13 months' group by 1`, [top]),
    all_time_lines: Number((await one('その単品の明細の数', `select count(*)::int as n from core.shipment_lines where sku_id = $1::bigint`, [top])).n),
  };
  out.notes.push('仕入 (入荷) は発注アプリ (Render の SQLite) にしか無く、miniPC からは読めない = 「合わない分」の増える側には仕入が入っている');
  return out;
}

/** miniPC の warehouse.db (NE 伝票の元) と Company DB の出荷の突き合わせ (読むだけ)。無ければ null */
export async function measureSqlite(dataDir, { from }) {
  const file = dataDir && path.join(dataDir, 'warehouse.db');
  if (!file || !fs.existsSync(file)) return { skipped: `warehouse.db が無い (${dataDir || 'DATA_DIR 未設定'})` };
  const { default: Database } = await import('better-sqlite3');
  const db = new Database(file, { readonly: true, fileMustExist: true, timeout: 5000 });
  try {
    const g = (sql, ...p) => db.prepare(sql).get(...p);
    return {
      ne_order_base: g(`select count(*) as rows, min(出荷確定日) as min_ship, max(出荷確定日) as max_ship from raw_ne_order_base where 出荷確定日 is not null and 出荷確定日 <> ''`),
      // 出荷確定日は 'YYYY-MM-DD HH:MM:SS' (JST)。Company DB の shipments_by_shop の合計と比べる (送り漏れ・遅れの目安)
      ne_shipped_in_window: g(`select count(*) as slips, sum(キャンセル区分 = 'キャンセル') as cancelled from raw_ne_order_base where 出荷確定日 >= ?`, from),
      ne_stock_snapshot: g(`select count(distinct business_date) as days, min(business_date) as min_date, max(business_date) as max_date from ne_stock_daily_snapshot`),
    };
  } catch (e) {
    return { error: e.message };
  } finally {
    db.close();
  }
}

/** 日本語の短いまとめ */
export function summaryJa(r) {
  const L = [];
  const s = r.sources || {};
  if (s.shipments) L.push(`出荷: 伝票 ${s.shipments.rows} 件 (出荷確定日 ${s.shipments.min_ship_date}〜${s.shipments.max_ship_date})・明細 ${s.shipment_lines?.rows} 行 (SKU 不明 ${s.shipment_lines?.unresolved})`);
  for (const d of s.stock_capture_days || []) L.push(`在庫の日次 ${d.source}/${d.scope_key}: complete ${d.complete} 日 (${d.min_complete}〜${d.max_complete})・partial ${d.partial}・missing ${d.missing}`);
  for (const e of s.inventory_events || []) L.push(`在庫のイベント ${e.source_system}/${e.confidence}: ${e.rows} 行`);
  if (s.purchase_orders_in_company_db) L.push(`Company DB の発注の受け皿: ${s.purchase_orders_in_company_db.orders} 件`);
  const fba = (r.shipments_by_shop || []).find((x) => x.shop_code === '15');
  L.push(`FBA納品 (店舗 15) の伝票 (期間): ${fba ? fba.slips : 0} 件`);
  if (r.set_lines_in_shipments) L.push(`出荷の明細のうちセットの SKU: ${r.set_lines_in_shipments.set_lines} / ${r.set_lines_in_shipments.lines} 行`);
  for (const src of ['logizard', 'ne']) {
    const cap = r.capture?.[src];
    if (cap) L.push(`${src} の取得時刻 (JST の時 → 日数): ${JSON.stringify(cap.gen_hour_jst)}・取れなかった日 ${cap.not_complete.length}`);
  }
  if (r.sample) L.push(`見本: 単品 ${r.sample.skus} 個 × ${r.window.days} 日`);
  for (const [k, t] of Object.entries(r.unexplained || {})) {
    if (!t) continue;
    const pct = t.intervals ? Math.round((t.nonzero / t.intervals) * 100) : 0;
    L.push(`${k}: 区間 ${t.intervals} のうち合わない ${t.nonzero} (${pct}%)・増 ${t.pos} 回 (+${t.posSum})・減 ${t.neg} 回 (${t.negSum})・大きさ ${JSON.stringify(t.buckets)}・打ち消し合う組 ${t.pairs} (${t.pairQty} 個)・組を除くと ${t.nonzeroAfterPairs}`);
  }
  if (r.speed) L.push(`速さ (${r.speed.sku}・明細 ${r.speed.all_time_lines} 行): 出荷 ${r.speed.shipments_13m.ms} ms・月別 ${r.speed.shipments_monthly_13m.ms} ms・在庫の日次 ${r.speed.stock_daily_13m.ms} ms`);
  for (const n of r.notes || []) L.push(`※ ${n}`);
  return L.join('\n');
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
if (isMain) {
  const args = process.argv.slice(2);
  const opt = (name, def) => { const i = args.indexOf(name); return i >= 0 && args[i + 1] ? Number(args[i + 1]) : def; };
  const days = opt('--days', 30), sample = opt('--sample', 30);
  if (!Number.isInteger(days) || days < 7 || days > 90 || !Number.isInteger(sample) || sample < 5 || sample > 100) throw new Error('--days は 7〜90・--sample は 5〜100');
  const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
  if (!url) throw new Error('COMPANY_DB_WATCH_URL が無い (照会用のロールで読む)');
  const quiet = args.includes('--json-only');
  const log = quiet ? () => {} : (m) => console.error(`  ${m}`);
  const c = await openPgClient(url);
  let r;
  try {
    await c.query('begin read only');
    await c.query(`set local statement_timeout = '60s'`);
    await c.query(`set local lock_timeout = '5s'`);
    r = await measure(c, { days, sample, log });
    await c.query('rollback');
  } finally {
    await c.end();
  }
  r.sqlite = await measureSqlite(process.env.DATA_DIR, { from: r.window.from });
  console.log(JSON.stringify(r));
  if (!quiet) console.log(`\n${summaryJa(r)}`);
}
