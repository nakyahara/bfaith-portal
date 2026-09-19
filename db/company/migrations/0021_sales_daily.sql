-- 0021: 売上の日次 mart.sales_daily (Company DB構想 08 §4.5 / §9 D7 の最初の 1 本 = D7a。注文別の利益 v_order_profit は財務 (F2b) と広告がそろってから)
--
--   粒度 = (注文日 JST, モール, scope, shop_code, listing_id, sku_id)。材料 = core.orders + 現行の明細 (core.order_lines.removed_at is null)。取消も数える。
--     shop_code を粒度に入れる理由 = Amazon は 自社発送 '4' / FBA null をここでしか見分けられない (0013 に fulfillment の列は無い)。
--   D-31: 売上 = (商品代 − 取消した商品代) + 送料 − 店負担の値引 (税込) / 顧客が払った額 = 売上 − モール負担の値引 − ポイント (別列)。
--     送料・値引・ポイントは注文のヘッダにしか無い → 注文の中の明細へ按分する。重み = 取消を引いた商品代の比 (合計が 0 なら数量の比、それも 0 なら等分)、端数は最大剰余法
--     (= 明細に配った額の合計が、必ずヘッダの額と 1 円も違わない。商は div() = 整数の商。numeric の割り算は有限桁に丸まり floor / round の前に境界を越えることがある = Codex R1 #4 / R2 #2。
--      明細の一部取消の取消額 = 商品代 × 取消数量 ÷ 数量 の四捨五入 も div(2ac + q, 2q) で厳密に)。
--     取り消された注文は商品代と取消だけ数え、送料・値引・ポイントは配らない (売上 0)。現行の明細が 0 行の注文は、集計にも検算にも入らない (配る先が無い)。
--     金額が null の明細 (Amazon の取消・金額の分からない行など) は 0 として足し、行数を lines_amount_unknown に数える (0 円の売上と区別できるように)。
--     中間の計算は numeric、保存のときに bigint へ (あふれたら明示の例外 = P-1。黙って回り込まない)。
--
--   🚨 P-5 = 集計は run_id publish (DELETE → INSERT で上書きしない): 行は run ごとに追記し、日付ごとの「公開の指し先」(sales_daily_published) を **同じ取引で** 差し替える。
--     読む側は mart.v_sales_daily (指し先の run の行だけ)。作りかけ・失敗の run は取引ごと消えるので見えない。指されなくなった古い行は猶予のあと purge で消す。
--   🚨 どの日を作り直すかは DB が自分で見つける (送り手から「変わった日付」も「回の目印」も受け取らない):
--     ・回 (session) は DB が発行して sales_daily_state に持ち、**回を開いた時点の対象日を sales_daily_session_dates に固定する**。1 回の呼び出しで作る日数に上限があり、残りは呼び直す。
--       **途中で止まっても、次の呼び出しは同じ回の続きから** (一覧の「まだ作っていない日」を古い順に)。呼び出しのたびに対象日を取り直すと、新しい日が入り続ける間は回が閉じない (Codex R2 #1)。外から時刻や識別子を渡す口は無い (未来の時刻を渡されてその日が永久に作り直されなくなる、を作らない = Codex R1 #1 / #2)
--     ・対象の日 = watermark (前回そろって終わった回の開始時刻) より後に core.orders.updated_at が動いた注文の注文日。watermark が null (初回・reset) のときは、注文のある日と公開中の日の全部。
--       updated_at は取込の取引の開始時刻 = 集計を始めた後で commit される取込がある → watermark から 15 分さかのぼって拾う (受け口の chunk の期限は 80 秒)。
--       回が全部終わったときだけ watermark をその回の開始時刻に進めて回を閉じる。回の途中で動いた注文・途中で増えた日は、次の回がさかのぼりで拾う
--       (前から開いていた回の続きを終えた呼び出しは resumed = true を返す → 送り手はもう 1 回ぶん回して追いつく)。
--     ・注文日そのものが変わった注文の「前の日」は、ふだんの回では拾えない (実データでは起きていない)。reset (--all) は公開中の日も全部作り直すので、注文が居なくなった日は 0 行で公開し直される (R1 #3)。
--   同時実行 = (会社, モール, scope) ごとの advisory lock で直列にする (§7.7 READ COMMITTED)。集計は 1 つの INSERT … SELECT = 1 つの snapshot。
-- 🚨 0001〜0020 の表・関数は変えない (core.orders に索引を 2 つ足すだけ)。

create index if not exists ix_orders_mall_updated on core.orders (company_id, mall, scope_key, updated_at);      -- 作り直す日の抽出 (更新時刻の範囲)
create index if not exists ix_orders_mall_date on core.orders (company_id, mall, scope_key, order_date_jst);     -- 日付の集合での集計・検算

create table mart.sales_daily_runs (
  run_id      text primary key,
  company_id  smallint not null references core.companies,
  mall        text not null,
  scope_key   text not null,
  session_id  text not null,                            -- この run が属する回。同じ回の中で作った日は作り直さない
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
  units_ordered              bigint not null check (units_ordered >= 0),
  units_cancelled            bigint not null check (units_cancelled >= 0),    -- 取り消された注文は全数、明細の取消は cancelled_qty
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
  -- null と実際の値がぶつからない形にする ('-' を null の代わりにすると shop_code = '-' と衝突する = R1 #7)。後ろの 2 つは数字か 'n' だけなので、shop_code に '|' が入っていても右から一意に読める
  grain_key                  text not null generated always as (case when shop_code is null then 'n' else 's:' || shop_code end || '|' || coalesce(listing_id::text, 'n') || '|' || coalesce(sku_id::text, 'n')) stored,
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

-- 作り直しの進み具合。watermark = 「全部の日を作り終えた最後の回」の開始時刻 (null = まだ 1 回も全部は終わっていない・reset した = 全部の日が対象)。
-- session_id / session_started_at = いま開いている回 (null = 開いていない)。途中で止まった回は開いたまま残り、次の呼び出しが続きをやる
create table mart.sales_daily_state (
  company_id         smallint not null references core.companies,
  mall               text not null,
  scope_key          text not null,
  watermark          timestamptz,
  session_id         text,
  session_started_at timestamptz,
  updated_at         timestamptz not null default now(),
  primary key (company_id, mall, scope_key),
  constraint ck_sales_daily_state_session check ((session_id is null) = (session_started_at is null))
);

-- 開いている回の対象日 (回を開いた時点で固定する)。呼び出しのたびに対象日を取り直すと、新しい日が入り続ける間は回が閉じず、その回で先に作った日の後からの変更も反映されない (Codex R2 #1)。
-- 途中で増えた対象日は次の回が拾う (watermark = この回の開始時刻 から 15 分さかのぼるので漏れない)。回が閉じたら行は消す
create table mart.sales_daily_session_dates (
  company_id smallint not null references core.companies,
  mall       text not null,
  scope_key  text not null,
  session_id text not null,
  date_jst   date not null,
  run_id     text references mart.sales_daily_runs,     -- null = まだ作っていない
  primary key (company_id, mall, scope_key, session_id, date_jst)
);

create or replace view mart.v_sales_daily as
  select s.* from mart.sales_daily s
    join mart.sales_daily_published p on p.company_id = s.company_id and p.mall = s.mall and p.scope_key = s.scope_key and p.date_jst = s.date_jst and p.run_id = s.run_id;

-- 注文 1 件の、按分に依らない数字 (検算の材料。集計の本体 build_sales_daily_dates とは別に式を持つ = 按分の誤りを見つけられる)。
-- 期間で先に絞る (view にすると日付の条件が集計の後ろに残り、モールの全期間を走査する = R1 #6)
create or replace function mart.sales_daily_check(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (date_jst date, is_published boolean, src_lines bigint, pub_lines bigint, src_units bigint, pub_units bigint, src_units_cancelled bigint, pub_units_cancelled bigint,
  src_items_amount_jpy numeric, pub_items_amount_jpy numeric, src_cancelled_amount_jpy numeric, pub_cancelled_amount_jpy numeric, src_sales_jpy numeric, pub_sales_jpy numeric,
  src_customer_paid_jpy numeric, pub_customer_paid_jpy numeric, src_lines_amount_unknown bigint, pub_lines_amount_unknown bigint)
language sql stable as $$
  with ord as (   -- 現行の明細が 1 行以上ある注文だけ (0 行の注文は集計にも入らない)
    select o.order_date_jst as d, o.is_cancelled, x.n_lines, x.units, x.cunits, x.amt, x.camt, x.unknown,
           case when o.is_cancelled then 0::numeric else coalesce(o.shipping_fee_jpy, 0)::numeric - coalesce(o.shop_coupon_jpy, 0)::numeric end as ship_minus_shop,
           case when o.is_cancelled then 0::numeric else coalesce(o.mall_coupon_jpy, 0)::numeric + coalesce(o.points_used_jpy, 0)::numeric end as mall_plus_points
      from core.orders o
      cross join lateral (
        select count(*) as n_lines, coalesce(sum(l.qty), 0)::bigint as units,
               coalesce(sum(case when o.is_cancelled then l.qty else l.cancelled_qty end), 0)::bigint as cunits,
               coalesce(sum(coalesce(l.line_amount_jpy, 0)), 0)::numeric as amt,
               coalesce(sum(case when o.is_cancelled then coalesce(l.line_amount_jpy, 0)
                                 when l.cancelled_qty > 0 and l.qty > 0 then least(coalesce(l.line_amount_jpy, 0)::numeric, div(2 * coalesce(l.line_amount_jpy, 0)::numeric * l.cancelled_qty + l.qty, 2 * l.qty::numeric))
                                 else 0 end), 0)::numeric as camt,
               count(*) filter (where l.line_amount_jpy is null) as unknown
          from core.order_lines l where l.order_id = o.order_id and l.removed_at is null) x
     where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst between p_from and p_to and x.n_lines > 0
  ), src as (
    select d, sum(n_lines)::bigint as lines, sum(units)::bigint as units, sum(cunits)::bigint as cunits, sum(amt) as amt, sum(camt) as camt,
           sum(amt - camt + ship_minus_shop) as sales, sum(amt - camt + ship_minus_shop - mall_plus_points) as paid, sum(unknown)::bigint as unknown
      from ord group by d
  ), pub as (
    select s.date_jst as d, sum(s.lines)::bigint as lines, sum(s.units_ordered)::bigint as units, sum(s.units_cancelled)::bigint as cunits, sum(s.items_amount_jpy) as amt, sum(s.cancelled_items_amount_jpy) as camt,
           sum(s.sales_jpy) as sales, sum(s.customer_paid_jpy) as paid, sum(s.lines_amount_unknown)::bigint as unknown
      from mart.v_sales_daily s
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key and s.date_jst between p_from and p_to
     group by s.date_jst
  )
  select coalesce(s.d, p.d), (pp.run_id is not null), coalesce(s.lines, 0), coalesce(p.lines, 0), coalesce(s.units, 0), coalesce(p.units, 0), coalesce(s.cunits, 0), coalesce(p.cunits, 0),
         coalesce(s.amt, 0), coalesce(p.amt, 0), coalesce(s.camt, 0), coalesce(p.camt, 0), coalesce(s.sales, 0), coalesce(p.sales, 0), coalesce(s.paid, 0), coalesce(p.paid, 0),
         coalesce(s.unknown, 0), coalesce(p.unknown, 0)
    from src s full join pub p on p.d = s.d
    left join mart.sales_daily_published pp on pp.company_id = p_company_id and pp.mall = p_mall and pp.scope_key = p_scope_key and pp.date_jst = coalesce(s.d, p.d)
   where (coalesce(s.lines, 0), coalesce(s.units, 0), coalesce(s.cunits, 0), coalesce(s.amt, 0), coalesce(s.camt, 0), coalesce(s.sales, 0), coalesce(s.paid, 0), coalesce(s.unknown, 0))
      is distinct from (coalesce(p.lines, 0), coalesce(p.units, 0), coalesce(p.cunits, 0), coalesce(p.amt, 0), coalesce(p.camt, 0), coalesce(p.sales, 0), coalesce(p.paid, 0), coalesce(p.unknown, 0))
   order by 1;
$$;

-- 渡された日付の集合を 1 つの run として作り、公開の指し先を同じ取引で差し替える。呼ぶ側 (refresh_sales_daily) が lock を持つ
create or replace function mart.build_sales_daily_dates(p_company_id smallint, p_mall text, p_scope_key text, p_dates date[], p_session_id text, p_built_by text)
returns table (run_id text, n_rows integer, n_orders integer) language plpgsql as $$
declare
  v_run text := 'sd_' || to_char(clock_timestamp() at time zone 'UTC', 'YYYYMMDDHH24MISSMS') || '_' || substr(md5(random()::text || clock_timestamp()::text), 1, 8);
  v_started timestamptz := clock_timestamp();
  v_rows integer; v_orders integer;
begin
  if p_dates is null or cardinality(p_dates) = 0 then raise exception 'p_dates must not be empty'; end if;
  if p_session_id is null or p_session_id = '' then raise exception 'p_session_id is required'; end if;
  insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders, built_by)
    values (v_run, p_company_id, p_mall, p_scope_key, p_session_id, v_started, v_started, cardinality(p_dates), 0, 0, p_built_by);
  with l as (
    select o.order_id, o.order_date_jst, o.shop_code, o.is_cancelled, ol.order_line_id, ol.listing_id, ol.sku_id, ol.qty::numeric as qty, (ol.line_amount_jpy is null) as amt_unknown,
           (case when o.is_cancelled then ol.qty else ol.cancelled_qty end)::numeric as cqty,
           coalesce(ol.line_amount_jpy, 0)::numeric as amt,
           (case when o.is_cancelled then coalesce(ol.line_amount_jpy, 0)
                 when ol.cancelled_qty > 0 and ol.qty > 0 then least(coalesce(ol.line_amount_jpy, 0)::numeric, div(2 * coalesce(ol.line_amount_jpy, 0)::numeric * ol.cancelled_qty + ol.qty, 2 * ol.qty::numeric))
                 else 0 end)::numeric as camt,
           o.shipping_fee_jpy, o.shop_coupon_jpy, o.mall_coupon_jpy, o.points_used_jpy
      from core.orders o join core.order_lines ol on ol.order_id = o.order_id and ol.removed_at is null
     where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = any (p_dates)
  ), w as (
    select l.*, sum(amt - camt) over (partition by order_id) as net_sum, sum(qty - cqty) over (partition by order_id) as qty_sum, count(*) over (partition by order_id)::numeric as n_lines from l
  ), ww as (   -- 按分の重み: 取消を引いた商品代 → 数量 → 等分 (numeric のまま。bigint に戻すと合計があふれる)
    select w.*, (case when net_sum > 0 then amt - camt when qty_sum > 0 then qty - cqty else 1 end) as wt,
                (case when net_sum > 0 then net_sum when qty_sum > 0 then qty_sum else n_lines end) as wsum
      from w
  ), k as (
    select ww.order_id, ww.order_line_id, ww.wt, ww.wsum, x.kind, x.h::numeric as h
      from ww cross join lateral (values ('ship', ww.shipping_fee_jpy), ('shop', ww.shop_coupon_jpy), ('mall', ww.mall_coupon_jpy), ('pt', ww.points_used_jpy)) x (kind, h)
     where not ww.is_cancelled and x.h is not null and x.h > 0
  ), a as (   -- 最大剰余法: まず整数の商 (div) で配り、余りを端数 (h × wt を wsum で割った余り) の大きい明細から 1 円ずつ
    select k.*, div(k.h * k.wt, k.wsum) as base, mod(k.h * k.wt, k.wsum) as frac from k
  ), r as (
    select a.order_line_id, a.kind, a.base, a.h - sum(a.base) over (partition by a.order_id, a.kind) as rem,
           row_number() over (partition by a.order_id, a.kind order by a.frac desc, a.order_line_id) as rn
      from a
  ), alloc as (
    select order_line_id,
           sum(case when kind = 'ship' then base + case when rn <= rem then 1 else 0 end else 0 end) as ship,
           sum(case when kind = 'shop' then base + case when rn <= rem then 1 else 0 end else 0 end) as shop,
           sum(case when kind = 'mall' then base + case when rn <= rem then 1 else 0 end else 0 end) as mall,
           sum(case when kind = 'pt'   then base + case when rn <= rem then 1 else 0 end else 0 end) as pt
      from r group by order_line_id
  ), g as (
    select ww.order_date_jst, ww.shop_code, ww.listing_id, ww.sku_id,
           count(distinct ww.order_id)::integer as orders, count(distinct ww.order_id) filter (where ww.is_cancelled)::integer as orders_cancelled, count(*)::integer as lines,
           sum(ww.qty) as units_ordered, sum(ww.cqty) as units_cancelled, sum(ww.amt) as items_amount, sum(ww.camt) as cancelled_amount,
           coalesce(sum(al.ship), 0) as ship, coalesce(sum(al.shop), 0) as shop, coalesce(sum(al.mall), 0) as mall, coalesce(sum(al.pt), 0) as pt,
           count(*) filter (where ww.amt_unknown)::integer as lines_amount_unknown, count(*) filter (where ww.listing_id is null and ww.sku_id is null)::integer as lines_unresolved
      from ww left join alloc al on al.order_line_id = ww.order_line_id
     group by 1, 2, 3, 4
  )
  insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, shop_code, listing_id, sku_id, orders, orders_cancelled, lines, units_ordered, units_cancelled,
      items_amount_jpy, cancelled_items_amount_jpy, shipping_alloc_jpy, shop_coupon_alloc_jpy, mall_coupon_alloc_jpy, points_alloc_jpy, sales_jpy, customer_paid_jpy, lines_amount_unknown, lines_unresolved)
    select v_run, p_company_id, g.order_date_jst, p_mall, p_scope_key, g.shop_code, g.listing_id, g.sku_id, g.orders, g.orders_cancelled, g.lines, g.units_ordered::bigint, g.units_cancelled::bigint,
           g.items_amount::bigint, g.cancelled_amount::bigint, g.ship::bigint, g.shop::bigint, g.mall::bigint, g.pt::bigint,
           (g.items_amount - g.cancelled_amount + g.ship - g.shop)::bigint, (g.items_amount - g.cancelled_amount + g.ship - g.shop - g.mall - g.pt)::bigint, g.lines_amount_unknown, g.lines_unresolved
      from g;   -- numeric → bigint は範囲を外れると例外 (P-1。黙って回り込まない)
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

-- 作り直しが要る日を古い順に p_limit 日ぶん作る。remaining > 0 なら呼び直す (引数は同じでよい。回と、その回の対象日は DB が覚えている)。
--   回を開くとき (開いている回が無いとき) に対象日を決めて sales_daily_session_dates に固定する。以後の呼び出しはその一覧の「まだ作っていない日」を古い順に取るだけ。
--   全部作ったら watermark = 回の開始時刻 にして回を閉じる。resumed = この呼び出しより前から開いていた回の続きだった
--     (= 回の開始からその呼び出しまでに動いた注文は、次の回でないと拾えない → 送り手は resumed の回を終えたら、もう 1 回ぶん回す)
--   p_reset = true: 開いている回を捨てて watermark を忘れ、注文のある日と公開中の日を全部作り直す回を始める (呼び直しでは false を渡す。true を渡すたびに最初からになる)
create or replace function mart.refresh_sales_daily(p_company_id smallint, p_mall text, p_scope_key text, p_limit integer default 31, p_reset boolean default false, p_built_by text default null)
returns table (session_id text, session_started_at timestamptz, resumed boolean, run_id text, dates_built integer, remaining integer, n_rows integer, n_orders integer) language plpgsql as $$
declare
  st mart.sales_daily_state%rowtype; v_resumed boolean := true; v_take date[]; v_left integer; v_run text; v_rows integer := 0; v_orders integer := 0;
begin
  if p_limit is null or p_limit < 1 or p_limit > 400 then raise exception 'p_limit must be 1..400'; end if;
  perform pg_advisory_xact_lock(hashtext('sales_daily:' || p_company_id || ':' || p_mall || ':' || p_scope_key));
  insert into mart.sales_daily_state (company_id, mall, scope_key) values (p_company_id, p_mall, p_scope_key) on conflict do nothing;
  if coalesce(p_reset, false) then
    delete from mart.sales_daily_session_dates d where d.company_id = p_company_id and d.mall = p_mall and d.scope_key = p_scope_key;
    update mart.sales_daily_state s set watermark = null, session_id = null, session_started_at = null, updated_at = clock_timestamp()
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
  end if;
  select * into st from mart.sales_daily_state s where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
  if st.session_id is null then
    -- 回を開く (lock を取った後の時刻 = 前の呼び出しが終わってから) + 対象日を固定する
    v_resumed := false;
    st.session_id := 'ss_' || to_char(clock_timestamp() at time zone 'UTC', 'YYYYMMDDHH24MISSMS') || '_' || substr(md5(random()::text || clock_timestamp()::text), 1, 8);
    st.session_started_at := clock_timestamp();
    update mart.sales_daily_state s set session_id = st.session_id, session_started_at = st.session_started_at, updated_at = clock_timestamp()
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
    if st.watermark is null then
      -- 初回・reset: 注文のある日 + 公開中の日 (注文が別の日へ移って居なくなった日を 0 行で公開し直す)
      insert into mart.sales_daily_session_dates (company_id, mall, scope_key, session_id, date_jst)
        select p_company_id, p_mall, p_scope_key, st.session_id, x.d from (
          select o.order_date_jst as d from core.orders o where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key
          union
          select p.date_jst from mart.sales_daily_published p where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key) x;
    else
      -- ふだん: watermark (−15 分) より後に動いた注文の注文日だけ (更新時刻の索引の範囲走査)。新しい日の注文も updated_at が新しいのでここで拾える
      insert into mart.sales_daily_session_dates (company_id, mall, scope_key, session_id, date_jst)
        select distinct p_company_id, p_mall, p_scope_key, st.session_id, o.order_date_jst from core.orders o
         where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.updated_at > st.watermark - interval '15 minutes';
    end if;
  end if;
  select coalesce(array_agg(x.date_jst order by x.date_jst), '{}') into v_take from (
    select d.date_jst from mart.sales_daily_session_dates d
     where d.company_id = p_company_id and d.mall = p_mall and d.scope_key = p_scope_key and d.session_id = st.session_id and d.run_id is null
     order by d.date_jst limit p_limit) x;
  if cardinality(v_take) > 0 then
    select b.run_id, b.n_rows, b.n_orders into v_run, v_rows, v_orders from mart.build_sales_daily_dates(p_company_id, p_mall, p_scope_key, v_take, st.session_id, p_built_by) b;
    update mart.sales_daily_session_dates d set run_id = v_run
     where d.company_id = p_company_id and d.mall = p_mall and d.scope_key = p_scope_key and d.session_id = st.session_id and d.date_jst = any (v_take);
  end if;
  select count(*) into v_left from mart.sales_daily_session_dates d
   where d.company_id = p_company_id and d.mall = p_mall and d.scope_key = p_scope_key and d.session_id = st.session_id and d.run_id is null;
  if v_left = 0 then   -- この回は全部終わった → watermark を回の開始時刻へ進めて回を閉じる (対象日の一覧は消す)
    delete from mart.sales_daily_session_dates d where d.company_id = p_company_id and d.mall = p_mall and d.scope_key = p_scope_key;
    update mart.sales_daily_state s set watermark = st.session_started_at, session_id = null, session_started_at = null, updated_at = clock_timestamp()
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key;
  end if;
  return query select st.session_id, st.session_started_at, v_resumed, v_run, cardinality(v_take), v_left, v_rows, v_orders;
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
