-- 0042: SKU ごとの動き (商品 360 の「売れ方・広告・在庫」。08 §4.5 の v_product_360 の列追加を、期間を引数に取る関数で)
--   mart.sku_activity(会社, from, to)      = SKU ごとの 正味の販売数量 (セットは構成品に展開・モール別)・売上 (1 SKU だけの品物)・Amazon の広告費・今の在庫と何日もつか
--   mart.sku_activity_gaps(会社, from, to) = SKU に割り振れなかった分 (展開できない販売・複数 SKU のセットの売上と広告費・出品の分からない広告費)。🚨 先にこちらで穴の大きさを見る
--   静的な属性 (名前・原価・JAN・出品の一覧) は mart.v_product_360 のまま。こちらは期間の動き
--
-- 決め:
--   ・関数にする (view にしない) = 期間で先に絞る (売上日次は 60 万行超)
--   ・数量: 売上日次 (mart.v_sales_daily) の正味数量 (注文 − 取消) を、見張り W6 と同じ規則で末端の SKU まで展開する (mart.sales_expanded_to_skus)
--       SKU の分かる明細はその SKU / 出品だけの明細は出品の構成 (core.listing_components) × 数量 / NE のセット商品 (sku_kind = 'set') は core.sku_components で構成品まで (入れ子 5 段・循環は止める)
--   ・🚨 売上と広告費は「その品物が 1 つの SKU だけでできている」ときだけ SKU に付ける (1 SKU × N 個のまとめ売りも付ける)。
--       複数の SKU のセットの売上・広告費は割り振らない (按分の決まりが無い = 推測で割らない) → sku_activity_gaps に出す
--   ・在庫 = mart.v_sku_stock の 倉庫 (ロジザード) + FBA JP の販売可能 (W6 と同じ)。何日もつか = 在庫 ÷ (期間の正味数量 ÷ 期間の日数)。売れていなければ null
--   ・返すのは期間に売れたか広告のあった SKU だけ (在庫だけの SKU は v_sku_stock で見る)

-- 売上日次の行 (sid) を末端の SKU まで展開する。展開できない行は sku_id null の 1 行 (units = 元の数量)。single = その行が 1 つの SKU だけでできている
create or replace function mart.sales_expanded_to_skus(p_company_id smallint, p_from date, p_to date)
returns table (sid bigint, mall text, src_units bigint, src_sales bigint, sku_id bigint, units bigint, single boolean)
language sql stable as $$
  with recursive
  s as (
    select row_number() over (order by v.date_jst, v.mall, v.scope_key, v.listing_id, v.sku_id, v.shop_code) as sid,
           v.mall, v.listing_id, v.sku_id, (v.units_ordered - v.units_cancelled)::bigint as units, v.sales_jpy::bigint as sales
      from mart.v_sales_daily v
     where v.company_id = p_company_id and v.date_jst between p_from and p_to
  ),
  u0 as (
    select s.sid, s.sku_id, s.units from s where s.sku_id is not null
    union all
    select s.sid, c.sku_id, s.units * c.qty from s join core.listing_components c on c.company_id = p_company_id and c.listing_id = s.listing_id where s.sku_id is null
  ),
  x as (
    select u0.sid, u0.sku_id, u0.units, 0 as depth, array[u0.sku_id] as path from u0
    union all
    select x.sid, sc.child_sku_id, x.units * sc.qty, x.depth + 1, x.path || sc.child_sku_id
      from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id and k.sku_kind = 'set'
             join core.sku_components sc on sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id
     where x.depth < 5 and not (sc.child_sku_id = any(x.path))
  ),
  term as (select x.sid, x.sku_id, sum(x.units)::bigint as units from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id where k.sku_kind <> 'set' group by x.sid, x.sku_id),
  n as (select term.sid, count(*) as n from term group by term.sid)
  select s.sid, s.mall, s.units, s.sales, t.sku_id, coalesce(t.units, s.units), coalesce(n.n, 0) = 1
    from s left join term t on t.sid = s.sid left join n on n.sid = s.sid
$$;
comment on function mart.sales_expanded_to_skus(smallint, date, date) is '売上日次の行を末端の SKU まで展開 (0042。見張り W6 と同じ規則)。展開できない行は sku_id null。single = 1 つの SKU だけの品物';

-- 出品を末端の SKU まで展開 (広告費を付けるため)。single = その出品が 1 つの SKU だけでできている
create or replace function mart.listings_to_skus(p_company_id smallint, p_listing_ids bigint[])
returns table (listing_id bigint, sku_id bigint, single boolean)
language sql stable as $$
  with recursive
  x as (
    select c.listing_id, c.sku_id, 0 as depth, array[c.sku_id] as path from core.listing_components c where c.company_id = p_company_id and c.listing_id = any(p_listing_ids)
    union all
    select x.listing_id, sc.child_sku_id, x.depth + 1, x.path || sc.child_sku_id
      from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id and k.sku_kind = 'set'
             join core.sku_components sc on sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id
     where x.depth < 5 and not (sc.child_sku_id = any(x.path))
  ),
  term as (select distinct x.listing_id, x.sku_id from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id where k.sku_kind <> 'set')
  select t.listing_id, t.sku_id, count(*) over (partition by t.listing_id) = 1 from term t
$$;

create or replace function mart.sku_activity(p_company_id smallint, p_from date, p_to date)
returns table (sku_id bigint, sku_code text, sku_name text, product_id bigint, product_name text, handling text,
  units_net bigint, units_via_sets bigint, units_by_mall jsonb,
  sales_jpy bigint, sales_by_mall jsonb,
  amazon_ad_cost numeric, amazon_ad_sales_1d numeric, amazon_sales_jpy bigint,
  stock_qty bigint, fba_jp_inbound bigint, stock_as_of date, daily_units numeric, cover_days numeric)
language sql stable as $$
  with e as (select * from mart.sales_expanded_to_skus(p_company_id, p_from, p_to) where sku_id is not null),
  um as (select e.sku_id, e.mall, sum(e.units)::bigint as u, coalesce(sum(e.units) filter (where not e.single), 0)::bigint as via from e group by e.sku_id, e.mall),
  ua as (select um.sku_id, sum(um.u)::bigint as units_net, sum(um.via)::bigint as via, jsonb_object_agg(um.mall, um.u order by um.mall) as by_mall from um group by um.sku_id),
  sm as (select e.sku_id, e.mall, sum(e.src_sales)::bigint as s from e where e.single group by e.sku_id, e.mall),
  sa as (select sm.sku_id, sum(sm.s)::bigint as sales, jsonb_object_agg(sm.mall, sm.s order by sm.mall) as by_mall, coalesce(sum(sm.s) filter (where sm.mall = 'amazon'), 0)::bigint as amazon from sm group by sm.sku_id),
  ad as (select a.listing_id, sum(a.ad_cost) as cost, sum(a.ad_sales_1d) as s1
           from core.ad_spend_daily a where a.company_id = p_company_id and a.mall = 'amazon' and a.date_jst between p_from and p_to and a.listing_id is not null group by a.listing_id),
  lsk as (select * from mart.listings_to_skus(p_company_id, (select coalesce(array_agg(ad.listing_id), '{}') from ad))),
  aa as (select l.sku_id, sum(ad.cost) as cost, sum(ad.s1) as s1 from ad join lsk l on l.listing_id = ad.listing_id and l.single group by l.sku_id),
  ids as (select ua.sku_id from ua union select aa.sku_id from aa)
  select k.sku_id, k.code, k.name, p.product_id, p.name, k.handling,
         coalesce(ua.units_net, 0), coalesce(ua.via, 0), coalesce(ua.by_mall, '{}'::jsonb),
         coalesce(sa.sales, 0), coalesce(sa.by_mall, '{}'::jsonb),
         coalesce(aa.cost, 0), aa.s1, coalesce(sa.amazon, 0),
         (coalesce(st.warehouse_qty, 0) + coalesce(st.fba_jp_available, 0))::bigint, st.fba_jp_inbound::bigint,
         least(st.warehouse_as_of, st.fba_jp_as_of),
         round(coalesce(ua.units_net, 0)::numeric / (p_to - p_from + 1), 2),
         case when coalesce(ua.units_net, 0) > 0 then round((coalesce(st.warehouse_qty, 0) + coalesce(st.fba_jp_available, 0))::numeric / (ua.units_net::numeric / (p_to - p_from + 1)), 1) end
    from ids join core.skus k on k.company_id = p_company_id and k.sku_id = ids.sku_id
    left join core.products p on p.company_id = k.company_id and p.product_id = k.product_id
    left join ua on ua.sku_id = ids.sku_id left join sa on sa.sku_id = ids.sku_id left join aa on aa.sku_id = ids.sku_id
    left join mart.v_sku_stock st on st.company_id = k.company_id and st.sku_id = k.sku_id
$$;
comment on function mart.sku_activity(smallint, date, date) is 'SKU ごとの動き (0042): 正味の販売数量 (セットは構成品に展開)・売上 (1 SKU だけの品物)・Amazon の広告費・在庫と何日もつか。先に mart.sku_activity_gaps で割り振れなかった分を見る';

create or replace function mart.sku_activity_gaps(p_company_id smallint, p_from date, p_to date)
returns table (units_total bigint, units_unexpanded bigint, sales_total bigint, sales_attributed bigint, sales_on_sets bigint, sales_unexpanded bigint,
  ad_total numeric, ad_attributed numeric, ad_on_sets numeric, ad_unlinked numeric)
language sql stable as $$
  with e as (select * from mart.sales_expanded_to_skus(p_company_id, p_from, p_to)),
  rowlvl as (select e.sid, max(e.src_units) as src_units, max(e.src_sales) as src_sales, bool_or(e.sku_id is null) as unexpanded, bool_or(e.single) as single from e group by e.sid),
  ad as (select a.listing_id, sum(a.ad_cost) as cost from core.ad_spend_daily a where a.company_id = p_company_id and a.mall = 'amazon' and a.date_jst between p_from and p_to group by a.listing_id),
  lsk as (select distinct l.listing_id, l.single from mart.listings_to_skus(p_company_id, (select coalesce(array_agg(ad.listing_id) filter (where ad.listing_id is not null), '{}') from ad)) l)
  select coalesce(sum(r.src_units), 0)::bigint,
         coalesce(sum(r.src_units) filter (where r.unexpanded), 0)::bigint,
         coalesce(sum(r.src_sales), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where r.single), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where not r.single and not r.unexpanded), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where r.unexpanded), 0)::bigint,
         (select coalesce(sum(ad.cost), 0) from ad),
         (select coalesce(sum(ad.cost), 0) from ad join lsk on lsk.listing_id = ad.listing_id and lsk.single),
         (select coalesce(sum(ad.cost), 0) from ad join lsk on lsk.listing_id = ad.listing_id and not lsk.single),
         (select coalesce(sum(ad.cost), 0) from ad where ad.listing_id is null or not exists (select 1 from lsk where lsk.listing_id = ad.listing_id))
    from rowlvl r
$$;
comment on function mart.sku_activity_gaps(smallint, date, date) is 'SKU に割り振れなかった分 (0042): 展開できない販売・複数 SKU のセットの売上・広告費 (セット / 出品が分からない)';
