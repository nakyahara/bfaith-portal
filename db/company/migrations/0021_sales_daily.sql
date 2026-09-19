-- 0021: 売上の日次 mart.sales_daily (Company DB構想 08 §4.5 / §9 D7 の最初の 1 本 = D7a。注文別の利益 v_order_profit は財務 (F2b) と広告がそろってから)
--
--   粒度 = (注文日 JST, モール, scope, shop_code, listing_id, sku_id)。材料 = core.orders + 現行の明細 (core.order_lines.removed_at is null)。取消も数える。
--     shop_code を粒度に入れる理由 = Amazon は 自社発送 '4' / FBA null をここでしか見分けられない (0013 に fulfillment の列は無い)。
--   D-31: 売上 = (商品代 − 取消した商品代) + 送料 − 店負担の値引 (税込) / 顧客が払った額 = 売上 − モール負担の値引 − ポイント (別列)。
--     送料・値引・ポイントは注文のヘッダにしか無い → 注文の中の明細へ按分する。重み = 取消を引いた商品代の比 (合計が 0 なら数量の比、それも 0 なら等分)、端数は最大剰余法
--     (= 明細に配った額の合計が、必ずヘッダの額と 1 円も違わない)。取り消された注文は商品代と取消だけ数え、送料・値引・ポイントは配らない (売上 0)。
--     金額が null の明細 (Amazon の取消・金額の分からない行など) は 0 として足し、行数を lines_amount_unknown に数える (0 円の売上と区別できるように)。
--
--   🚨 P-5 = 集計は run_id publish (DELETE → INSERT で上書きしない): 行は run ごとに追記し、日付ごとの「公開の指し先」(sales_daily_published) を **同じ取引で** 差し替える。
--     読む側は mart.v_sales_daily (指し先の run の行だけ)。作りかけ・失敗の run は取引ごと消えるので見えない。指されなくなった古い行は猶予のあと purge で消す。
--   🚨 どの日を作り直すかは DB が自分で見つける (送り手から「変わった日付」を受け取らない = 途中で落ちた run の分が失われない):
--     前回そろって終わった回の開始時刻 (watermark) より後に core.orders.updated_at が動いた注文の注文日 (+ まだ公開の無い日)。
--     updated_at は取込の取引の開始時刻 = 集計を始めた後で commit される取込がある → watermark から 15 分さかのぼって拾う (受け口の chunk の期限は 80 秒)。
--     1 回の呼び出しで作る日数に上限があり、残りは同じ session で呼び直す (session の中で作った日は作り直さない)。全部終わったときだけ watermark を session の開始時刻に進める。
--     注文日そのものが変わった注文の「前の日」は拾えない (実データでは起きていない)。--all (p_reset) で全部の日を作り直せる。
--   同時実行 = (会社, モール, scope) ごとの advisory lock で直列にする (§7.7 READ COMMITTED)。集計は 1 つの INSERT … SELECT = 1 つの snapshot。
-- 🚨 0001〜0020 の表・関数は変えない (core.orders に索引を 1 つ足すだけ)。

create index if not exists ix_orders_mall_updated on core.orders (company_id, mall, scope_key, updated_at);

create table mart.sales_daily_runs (
  run_id      text primary key,
  company_id  smallint not null references core.companies,
  mall        text not null,
  scope_key   text not null,
  session_at  timestamptz not null,                     -- この run が属する回 (session) の開始時刻。同じ session の中で作った日は作り直さない
  started_at  timestamptz not null,
  finished_at timestamptz not null,
  n_dates     integer not null check (n_dates >= 0),
  n_rows      integer not null check (n_rows >= 0),
  n_orders    integer not null check (n_orders >= 0),
  built_by    text,
  unique (company_id, run_id)
);
create index ix_sales_daily_runs_scope on mart.sales_daily_runs (company_id, mall, scope_key, started_at desc);

create table mart.sales_daily (
  run_id                     text not null references mart.sales_daily_runs,
  company_id                 smallint not null references core.companies,
  date_jst                   date not null,             -- 注文日 (JST)
  mall                       text not null,
  scope_key                  text not null,
  shop_code                  text,
  listing_id                 bigint,
  sku_id                     bigint,
  orders                     integer not null check (orders >= 0),            -- この粒度に明細を持つ注文の数 (取消も数える)
  orders_cancelled           integer not null check (orders_cancelled >= 0),  -- そのうち取り消された注文
  lines                      integer not null check (lines >= 0),
  units_ordered              integer not null check (units_ordered >= 0),
  units_cancelled            integer not null check (units_cancelled >= 0),   -- 取り消された注文は全数、明細の取消は cancelled_qty
  items_amount_jpy           bigint not null check (items_amount_jpy >= 0),   -- 商品代 (税込。取消も含む)
  cancelled_items_amount_jpy bigint not null check (cancelled_items_amount_jpy >= 0),
  shipping_alloc_jpy         bigint not null default 0 check (shipping_alloc_jpy >= 0),
  shop_coupon_alloc_jpy      bigint not null default 0 check (shop_coupon_alloc_jpy >= 0),
  mall_coupon_alloc_jpy      bigint not null default 0 check (mall_coupon_alloc_jpy >= 0),
  points_alloc_jpy           bigint not null default 0 check (points_alloc_jpy >= 0),
  sales_jpy                  bigint not null,           -- (商品代 − 取消) + 送料 − 店負担の値引。値引が大きければ負もあり得る
  customer_paid_jpy          bigint not null,           -- 売上 − モール負担の値引 − ポイント (計算値。モールの言う「払った額」は core.orders.total_amount_jpy)
  lines_amount_unknown       integer not null default 0 check (lines_amount_unknown >= 0),  -- 金額が null の明細 (0 として足した)
  lines_unresolved           integer not null default 0 check (lines_unresolved >= 0),      -- 出品にも SKU にも当たらなかった明細
  built_at                   timestamptz not null default now(),
  grain_key                  text not null generated always as (coalesce(shop_code, '-') || '|' || coalesce(listing_id::text, '-') || '|' || coalesce(sku_id::text, '-')) stored,
  primary key (run_id, company_id, date_jst, mall, scope_key, grain_key),
  constraint ck_sales_daily_cancelled check (cancelled_items_amount_jpy <= items_amount_jpy and units_cancelled <= units_ordered and orders_cancelled <= orders),
  constraint ck_sales_daily_sales check (sales_jpy = items_amount_jpy - cancelled_items_amount_jpy + shipping_alloc_jpy - shop_coupon_alloc_jpy),
  constraint ck_sales_daily_paid check (customer_paid_jpy = sales_jpy - mall_coupon_alloc_jpy - points_alloc_jpy),
  foreign key (company_id, run_id) references mart.sales_daily_runs (company_id, run_id),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_sales_daily_date on mart.sales_daily (company_id, mall, scope_key, date_jst);
create index ix_sales_daily_sku on mart.sales_daily (company_id, sku_id, date_jst) where sku_id is not null;

-- 公開の指し先: (会社, モール, scope, 日付) → いま読ませる run。注文が 1 件も無い日も指す (= その日は 0 行が正しい)
create table mart.sales_daily_published (
  company_id   smallint not null references core.companies,
  mall         text not null,
  scope_key    text not null,
  date_jst     date not null,
  run_id       text not null references mart.sales_daily_runs,
  published_at timestamptz not null default now(),
  primary key (company_id, mall, scope_key, date_jst),
  foreign key (company_id, run_id) references mart.sales_daily_runs (company_id, run_id)
);
create index ix_sales_daily_published_run on mart.sales_daily_published (run_id);

-- 作り直しの進み具合: watermark = 「全部の日を作り終えた最後の回」の開始時刻。null = まだ 1 回も全部は終わっていない (全部の日が対象)
create table mart.sales_daily_state (
  company_id smallint not null references core.companies,
  mall       text not null,
  scope_key  text not null,
  watermark  timestamptz,
  updated_at timestamptz not null default now(),
  primary key (company_id, mall, scope_key)
);

create or replace view mart.v_sales_daily as
  select s.* from mart.sales_daily s
    join mart.sales_daily_published p on p.company_id = s.company_id and p.mall = s.mall and p.scope_key = s.scope_key and p.date_jst = s.date_jst and p.run_id = s.run_id;

-- 公開中の集計と、材料 (core.orders / order_lines) を今そのまま足した値の食い違い。0 行が正常。公開の後に注文が動いた日は食い違う = refresh がまだ、の印でもある。
-- 按分の検算を兼ねる: 送料・値引・ポイントは「取り消されていない・現行の明細が 1 行以上ある注文」のヘッダの合計と 1 円も違わないはず
create or replace view mart.v_sales_daily_check as
  with src as (
    select o.company_id, o.mall, o.scope_key, o.order_date_jst as date_jst,
           count(*) filter (where x.n_lines > 0)::integer as orders, coalesce(sum(x.n_lines), 0)::integer as lines, coalesce(sum(x.amt), 0)::bigint as items_amount_jpy,
           coalesce(sum(case when not o.is_cancelled and x.n_lines > 0 then coalesce(o.shipping_fee_jpy, 0) else 0 end), 0)::bigint as shipping_jpy,
           coalesce(sum(case when not o.is_cancelled and x.n_lines > 0 then coalesce(o.shop_coupon_jpy, 0) else 0 end), 0)::bigint as shop_coupon_jpy,
           coalesce(sum(case when not o.is_cancelled and x.n_lines > 0 then coalesce(o.mall_coupon_jpy, 0) else 0 end), 0)::bigint as mall_coupon_jpy,
           coalesce(sum(case when not o.is_cancelled and x.n_lines > 0 then coalesce(o.points_used_jpy, 0) else 0 end), 0)::bigint as points_jpy
      from core.orders o
      cross join lateral (select count(*) as n_lines, coalesce(sum(coalesce(l.line_amount_jpy, 0)), 0) as amt from core.order_lines l where l.order_id = o.order_id and l.removed_at is null) x
     group by 1, 2, 3, 4
  ), pub as (
    select company_id, mall, scope_key, date_jst, sum(lines)::integer as lines, sum(items_amount_jpy)::bigint as items_amount_jpy, sum(shipping_alloc_jpy)::bigint as shipping_jpy,
           sum(shop_coupon_alloc_jpy)::bigint as shop_coupon_jpy, sum(mall_coupon_alloc_jpy)::bigint as mall_coupon_jpy, sum(points_alloc_jpy)::bigint as points_jpy
      from mart.v_sales_daily group by 1, 2, 3, 4
  )
  select coalesce(s.company_id, p.company_id) as company_id, coalesce(s.mall, p.mall) as mall, coalesce(s.scope_key, p.scope_key) as scope_key, coalesce(s.date_jst, p.date_jst) as date_jst,
         (d.run_id is not null) as is_published,
         coalesce(s.lines, 0) as src_lines, coalesce(p.lines, 0) as pub_lines, coalesce(s.items_amount_jpy, 0) as src_items_amount_jpy, coalesce(p.items_amount_jpy, 0) as pub_items_amount_jpy,
         coalesce(s.shipping_jpy, 0) as src_shipping_jpy, coalesce(p.shipping_jpy, 0) as pub_shipping_jpy, coalesce(s.shop_coupon_jpy, 0) as src_shop_coupon_jpy, coalesce(p.shop_coupon_jpy, 0) as pub_shop_coupon_jpy,
         coalesce(s.mall_coupon_jpy, 0) as src_mall_coupon_jpy, coalesce(p.mall_coupon_jpy, 0) as pub_mall_coupon_jpy, coalesce(s.points_jpy, 0) as src_points_jpy, coalesce(p.points_jpy, 0) as pub_points_jpy
    from src s full join pub p using (company_id, mall, scope_key, date_jst)
    left join mart.sales_daily_published d on d.company_id = coalesce(s.company_id, p.company_id) and d.mall = coalesce(s.mall, p.mall) and d.scope_key = coalesce(s.scope_key, p.scope_key) and d.date_jst = coalesce(s.date_jst, p.date_jst)
   where (coalesce(s.lines, 0), coalesce(s.items_amount_jpy, 0), coalesce(s.shipping_jpy, 0), coalesce(s.shop_coupon_jpy, 0), coalesce(s.mall_coupon_jpy, 0), coalesce(s.points_jpy, 0))
      is distinct from (coalesce(p.lines, 0), coalesce(p.items_amount_jpy, 0), coalesce(p.shipping_jpy, 0), coalesce(p.shop_coupon_jpy, 0), coalesce(p.mall_coupon_jpy, 0), coalesce(p.points_jpy, 0));

-- 渡された日付の集合を 1 つの run として作り、公開の指し先を同じ取引で差し替える。呼ぶ側 (refresh_sales_daily) が lock を持つ
create or replace function mart.build_sales_daily_dates(p_company_id smallint, p_mall text, p_scope_key text, p_dates date[], p_session timestamptz, p_built_by text)
returns table (run_id text, n_rows integer, n_orders integer) language plpgsql as $$
declare
  v_run text := 'sd_' || to_char(clock_timestamp() at time zone 'UTC', 'YYYYMMDDHH24MISSMS') || '_' || substr(md5(random()::text || clock_timestamp()::text), 1, 6);
  v_started timestamptz := clock_timestamp();
  v_rows integer; v_orders integer;
begin
  if p_dates is null or cardinality(p_dates) = 0 then raise exception 'p_dates must not be empty'; end if;
  insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_at, started_at, finished_at, n_dates, n_rows, n_orders, built_by)
    values (v_run, p_company_id, p_mall, p_scope_key, p_session, v_started, v_started, cardinality(p_dates), 0, 0, p_built_by);
  with l as (
    select o.order_id, o.order_date_jst, o.shop_code, o.is_cancelled, ol.order_line_id, ol.listing_id, ol.sku_id, ol.qty, (ol.line_amount_jpy is null) as amt_unknown,
           case when o.is_cancelled then ol.qty else ol.cancelled_qty end as cqty,
           coalesce(ol.line_amount_jpy, 0) as amt,
           case when o.is_cancelled then coalesce(ol.line_amount_jpy, 0)
                when ol.cancelled_qty > 0 and ol.qty > 0 then least(coalesce(ol.line_amount_jpy, 0), round(coalesce(ol.line_amount_jpy, 0)::numeric * ol.cancelled_qty / ol.qty)::bigint)
                else 0 end as camt,
           o.shipping_fee_jpy, o.shop_coupon_jpy, o.mall_coupon_jpy, o.points_used_jpy
      from core.orders o join core.order_lines ol on ol.order_id = o.order_id and ol.removed_at is null
     where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = any (p_dates)
  ), w as (
    select l.*, sum(amt - camt) over (partition by order_id) as net_sum, sum(qty - cqty) over (partition by order_id) as qty_sum, count(*) over (partition by order_id) as n_lines from l
  ), ww as (   -- 按分の重み: 取消を引いた商品代 → 数量 → 等分
    select w.*, (case when net_sum > 0 then amt - camt when qty_sum > 0 then qty - cqty else 1 end)::bigint as wt,
                (case when net_sum > 0 then net_sum when qty_sum > 0 then qty_sum else n_lines end)::bigint as wsum
      from w
  ), k as (
    select ww.order_id, ww.order_line_id, ww.wt, ww.wsum, x.kind, x.h
      from ww cross join lateral (values ('ship', ww.shipping_fee_jpy), ('shop', ww.shop_coupon_jpy), ('mall', ww.mall_coupon_jpy), ('pt', ww.points_used_jpy)) x (kind, h)
     where not ww.is_cancelled and x.h is not null and x.h > 0
  ), a as (   -- 最大剰余法: まず切り捨てで配り、余りを端数の大きい明細から 1 円ずつ
    select k.*, floor((k.h::numeric * k.wt) / k.wsum)::bigint as base, ((k.h::numeric * k.wt) % k.wsum) as frac
      from k
  ), r as (
    select a.order_line_id, a.kind, a.base, a.h - sum(a.base) over (partition by a.order_id, a.kind) as rem,
           row_number() over (partition by a.order_id, a.kind order by a.frac desc, a.order_line_id) as rn
      from a
  ), alloc as (
    select order_line_id,
           sum(case when kind = 'ship' then base + case when rn <= rem then 1 else 0 end else 0 end)::bigint as ship,
           sum(case when kind = 'shop' then base + case when rn <= rem then 1 else 0 end else 0 end)::bigint as shop,
           sum(case when kind = 'mall' then base + case when rn <= rem then 1 else 0 end else 0 end)::bigint as mall,
           sum(case when kind = 'pt'   then base + case when rn <= rem then 1 else 0 end else 0 end)::bigint as pt
      from r group by order_line_id
  ), g as (
    select ww.order_date_jst, ww.shop_code, ww.listing_id, ww.sku_id,
           count(distinct ww.order_id)::integer as orders, count(distinct ww.order_id) filter (where ww.is_cancelled)::integer as orders_cancelled, count(*)::integer as lines,
           sum(ww.qty)::integer as units_ordered, sum(ww.cqty)::integer as units_cancelled, sum(ww.amt)::bigint as items_amount, sum(ww.camt)::bigint as cancelled_amount,
           coalesce(sum(al.ship), 0)::bigint as ship, coalesce(sum(al.shop), 0)::bigint as shop, coalesce(sum(al.mall), 0)::bigint as mall, coalesce(sum(al.pt), 0)::bigint as pt,
           count(*) filter (where ww.amt_unknown)::integer as lines_amount_unknown, count(*) filter (where ww.listing_id is null and ww.sku_id is null)::integer as lines_unresolved
      from ww left join alloc al on al.order_line_id = ww.order_line_id
     group by 1, 2, 3, 4
  )
  insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, shop_code, listing_id, sku_id, orders, orders_cancelled, lines, units_ordered, units_cancelled,
      items_amount_jpy, cancelled_items_amount_jpy, shipping_alloc_jpy, shop_coupon_alloc_jpy, mall_coupon_alloc_jpy, points_alloc_jpy, sales_jpy, customer_paid_jpy, lines_amount_unknown, lines_unresolved)
    select v_run, p_company_id, g.order_date_jst, p_mall, p_scope_key, g.shop_code, g.listing_id, g.sku_id, g.orders, g.orders_cancelled, g.lines, g.units_ordered, g.units_cancelled,
           g.items_amount, g.cancelled_amount, g.ship, g.shop, g.mall, g.pt,
           g.items_amount - g.cancelled_amount + g.ship - g.shop, g.items_amount - g.cancelled_amount + g.ship - g.shop - g.mall - g.pt, g.lines_amount_unknown, g.lines_unresolved
      from g;
  get diagnostics v_rows = row_count;
  select count(*) into v_orders from core.orders o
   where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = any (p_dates);
  -- 公開の指し先を差し替える (注文が 1 件も無い日も指す)
  insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id, published_at)
    select p_company_id, p_mall, p_scope_key, d, v_run, clock_timestamp() from unnest(p_dates) d group by d
  on conflict (company_id, mall, scope_key, date_jst) do update set run_id = excluded.run_id, published_at = excluded.published_at;
  update mart.sales_daily_runs r set finished_at = clock_timestamp(), n_rows = v_rows, n_orders = v_orders where r.run_id = v_run;
  return query select v_run, v_rows, v_orders;
end
$$;

-- 作り直しが要る日を見つけて、古い順に p_limit 日ぶん作る。戻り値の remaining > 0 なら、同じ session (戻り値の session_at をそのまま渡す) で呼び直す。
--   p_session = null なら新しい回を始める。p_reset = true なら watermark を忘れて全部の日を対象にする (最初の 1 回だけ渡す)
create or replace function mart.refresh_sales_daily(p_company_id smallint, p_mall text, p_scope_key text, p_session timestamptz default null, p_limit integer default 31, p_reset boolean default false, p_built_by text default null)
returns table (session_at timestamptz, run_id text, dates_built integer, remaining integer, n_rows integer, n_orders integer) language plpgsql as $$
declare
  v_session timestamptz; v_wm timestamptz; v_all date[]; v_take date[]; v_run text; v_rows integer := 0; v_orders integer := 0;
begin
  if p_limit is null or p_limit < 1 or p_limit > 400 then raise exception 'p_limit must be 1..400'; end if;
  perform pg_advisory_xact_lock(hashtext('sales_daily:' || p_company_id || ':' || p_mall || ':' || p_scope_key));
  v_session := coalesce(p_session, clock_timestamp());   -- lock を取った後の時刻 (前の回が終わってから始まる)
  insert into mart.sales_daily_state (company_id, mall, scope_key, watermark) values (p_company_id, p_mall, p_scope_key, null) on conflict do nothing;
  if p_reset then update mart.sales_daily_state s set watermark = null, updated_at = clock_timestamp() where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key; end if;
  select s.watermark into v_wm from mart.sales_daily_state s where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
  select coalesce(array_agg(d order by d), '{}') into v_all from (
    select distinct o.order_date_jst as d from core.orders o
     where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key
       and (v_wm is null or o.updated_at > v_wm - interval '15 minutes'
            or not exists (select 1 from mart.sales_daily_published p0 where p0.company_id = o.company_id and p0.mall = o.mall and p0.scope_key = o.scope_key and p0.date_jst = o.order_date_jst))
       and not exists (select 1 from mart.sales_daily_published p join mart.sales_daily_runs r on r.run_id = p.run_id
                        where p.company_id = o.company_id and p.mall = o.mall and p.scope_key = o.scope_key and p.date_jst = o.order_date_jst and r.session_at >= v_session)
  ) x;
  v_take := v_all[1:p_limit];
  if cardinality(v_take) > 0 then
    select b.run_id, b.n_rows, b.n_orders into v_run, v_rows, v_orders from mart.build_sales_daily_dates(p_company_id, p_mall, p_scope_key, v_take, v_session, p_built_by) b;
  end if;
  if cardinality(v_all) <= p_limit then
    update mart.sales_daily_state s set watermark = v_session, updated_at = clock_timestamp() where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
  end if;
  return query select v_session, v_run, cardinality(v_take), greatest(cardinality(v_all) - p_limit, 0), v_rows, v_orders;
end
$$;

-- 指されなくなった古い行を消す (猶予 = p_keep_days。公開中の行は消さない)。戻り値 = 消した行数
create or replace function mart.purge_sales_daily(p_company_id smallint, p_keep_days integer default 3) returns integer language plpgsql as $$
declare v_n integer;
begin
  if p_keep_days is null or p_keep_days < 1 then raise exception 'p_keep_days must be >= 1'; end if;
  delete from mart.sales_daily s
   where s.company_id = p_company_id and s.built_at < now() - make_interval(days => p_keep_days)
     and not exists (select 1 from mart.sales_daily_published p where p.company_id = s.company_id and p.mall = s.mall and p.scope_key = s.scope_key and p.date_jst = s.date_jst and p.run_id = s.run_id);
  get diagnostics v_n = row_count;
  return v_n;
end
$$;
