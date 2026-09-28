-- 0042: SKU ごとの動き (商品 360 の「売れ方・広告・在庫」。08 §4.5 の v_product_360 の列追加を、期間を引数に取る関数で)
--   mart.sku_activity(会社, from, to)      = SKU ごとの 正味の販売数量 (セットは構成品に展開・モール別)・売上 (1 SKU だけの品物)・Amazon の広告費・今の在庫と何日もつか
--   mart.sku_activity_gaps(会社, from, to) = SKU に割り振れなかった分 (展開できない販売・複数 SKU のセットの売上と広告費・出品の分からない広告費)。🚨 先にこちらで穴の大きさを見る
--   静的な属性 (名前・原価・JAN・出品の一覧) は mart.v_product_360 のまま。こちらは期間の動き
--
-- 決め:
--   ・関数にする (view にしない) = 期間で先に絞る (売上日次は 60 万行超)
--   ・数量: 売上日次 (mart.v_sales_daily) の正味数量 (注文 − 取消) を、見張り W6 と同じ規則で末端の SKU まで展開する (mart.sales_expanded_to_skus)
--       SKU の分かる明細はその SKU / 出品だけの明細は出品の構成 (core.listing_components) × 数量 / NE のセット商品 (sku_kind = 'set') は core.sku_components で構成品まで (入れ子 5 段・循環は止める)
--   ・🚨 展開しきれない経路が 1 本でもある行 (出品に当たらない・出品の構成が無い / 構成の無いセット / 深すぎる・循環するセット = W6 の bad) は complete = false。
--       届いた末端の数量は数える (W6 と同じ) が、売上・広告費は付けない (一部だけ見えた構成で 1 SKU と決めつけない。Codex #1506 R1)
--   ・🚨 売上と広告費は「その品物が 1 つの SKU だけでできている (展開しきった上で)」ときだけ SKU に付ける (1 SKU × N 個のまとめ売りも付ける)。
--       複数の SKU のセットの売上・広告費は割り振らない (按分の決まりが無い = 推測で割らない) → sku_activity_gaps に出す
--   ・在庫 = mart.v_sku_stock の 倉庫 (ロジザード) + FBA JP の販売可能 (W6 と同じ)。🚨 どちらかが不明 (complete な日が無い = null) なら合計も何日もつかも null (不明を 0 と読まない。Codex #1506 R1)
--       何日もつか = 在庫 ÷ (期間の正味数量 ÷ 期間の日数)。売れていなければ null
--   ・広告経由の売上が分からない行 (ad_sales_1d null) が 1 行でもあれば amazon_ad_sales_1d は null (一部だけの和を出さない) + その行数を返す
--   ・返すのは期間に売れたか広告のあった SKU だけ (在庫だけの SKU は v_sku_stock で見る)

-- 売上日次の行 (sid) を末端の SKU まで展開する。末端に 1 つも届かない行は sku_id null の 1 行 (units = 元の数量)
--   complete = 展開しきった (W6 の bad でない) / single = complete かつ末端の SKU が 1 つ / via_set = NE のセット SKU を通って届いた数量
create or replace function mart.sales_expanded_to_skus(p_company_id smallint, p_from date, p_to date)
returns table (sid bigint, mall text, src_units bigint, src_sales bigint, sku_id bigint, units bigint, complete boolean, single boolean, via_set boolean)
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
  node as (
    select x.sid, x.sku_id, x.units, x.depth, k.sku_kind,
           exists (select 1 from core.sku_components sc where sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id) as has_comp,
           exists (select 1 from core.sku_components sc where sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id and sc.child_sku_id = any(x.path)) as has_cycle
      from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id
  ),
  -- 展開しきれない行 (W6 の bad と同じ): 出品に当たらない・出品の構成が無い / 構成の無いセット / 深すぎる・循環するセット
  bad as (
    select s.sid from s where s.sku_id is null and (s.listing_id is null or not exists (select 1 from core.listing_components c where c.company_id = p_company_id and c.listing_id = s.listing_id))
    union
    select node.sid from node where node.sku_kind = 'set' and (not node.has_comp or node.has_cycle or node.depth >= 5)
  ),
  term as (select node.sid, node.sku_id, sum(node.units)::bigint as units, bool_or(node.depth > 0) as via_set from node where node.sku_kind <> 'set' group by node.sid, node.sku_id),
  n as (select term.sid, count(*) as n from term group by term.sid)
  select s.sid, s.mall, s.units, s.sales, t.sku_id, coalesce(t.units, s.units),
         b.sid is null, b.sid is null and coalesce(n.n, 0) = 1, coalesce(t.via_set, false)
    from s left join term t on t.sid = s.sid left join n on n.sid = s.sid left join bad b on b.sid = s.sid
$$;
comment on function mart.sales_expanded_to_skus(smallint, date, date) is '売上日次の行を末端の SKU まで展開 (0042。見張り W6 と同じ規則)。末端に届かない行は sku_id null。complete = 展開しきった / single = complete かつ 1 つの SKU だけの品物 / via_set = NE のセットを通った';

-- 出品を末端の SKU まで展開 (広告費を付けるため)。complete = 展開しきった (構成が無い出品・構成の無いセット・深すぎる / 循環するセットがあれば false) / single = complete かつ末端の SKU が 1 つ
--   末端に 1 つも届かない出品は返さない (= 呼び手は「出品が分からない」側に数える)
create or replace function mart.listings_to_skus(p_company_id smallint, p_listing_ids bigint[])
returns table (listing_id bigint, sku_id bigint, complete boolean, single boolean)
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
  node as (
    select x.listing_id, x.sku_id, x.depth, k.sku_kind,
           exists (select 1 from core.sku_components sc where sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id) as has_comp,
           exists (select 1 from core.sku_components sc where sc.company_id = p_company_id and sc.parent_sku_id = x.sku_id and sc.child_sku_id = any(x.path)) as has_cycle
      from x join core.skus k on k.company_id = p_company_id and k.sku_id = x.sku_id
  ),
  bad as (select distinct node.listing_id from node where node.sku_kind = 'set' and (not node.has_comp or node.has_cycle or node.depth >= 5)),
  term as (select distinct node.listing_id, node.sku_id from node where node.sku_kind <> 'set')
  select t.listing_id, t.sku_id, b.listing_id is null, b.listing_id is null and count(*) over (partition by t.listing_id) = 1
    from term t left join bad b on b.listing_id = t.listing_id
$$;

create or replace function mart.sku_activity(p_company_id smallint, p_from date, p_to date)
returns table (sku_id bigint, sku_code text, sku_name text, product_id bigint, product_name text, handling text,
  units_net bigint, units_via_sets bigint, units_by_mall jsonb,
  sales_jpy bigint, sales_by_mall jsonb,
  amazon_ad_cost numeric, amazon_ad_sales_1d numeric, amazon_ad_unknown_rows bigint, amazon_sales_jpy bigint,
  warehouse_qty bigint, fba_jp_available bigint, stock_qty bigint, fba_jp_inbound bigint, stock_as_of date, daily_units numeric, cover_days numeric)
language sql stable as $$
  with e as (select * from mart.sales_expanded_to_skus(p_company_id, p_from, p_to) where sku_id is not null),
  -- セット経由の数量 = 複数 SKU の品物 (出品のセット) か NE のセット SKU を通った数量 (1 つの構成品だけの NE のセットも含む。まとめ売り 1 SKU × N 個は含めない)
  um as (select e.sku_id, e.mall, sum(e.units)::bigint as u, coalesce(sum(e.units) filter (where not e.single or e.via_set), 0)::bigint as via from e group by e.sku_id, e.mall),
  ua as (select um.sku_id, sum(um.u)::bigint as units_net, sum(um.via)::bigint as via, jsonb_object_agg(um.mall, um.u order by um.mall) as by_mall from um group by um.sku_id),
  sm as (select e.sku_id, e.mall, sum(e.src_sales)::bigint as s from e where e.single group by e.sku_id, e.mall),
  sa as (select sm.sku_id, sum(sm.s)::bigint as sales, jsonb_object_agg(sm.mall, sm.s order by sm.mall) as by_mall, coalesce(sum(sm.s) filter (where sm.mall = 'amazon'), 0)::bigint as amazon from sm group by sm.sku_id),
  ad as (select a.listing_id, sum(a.ad_cost) as cost, sum(a.ad_sales_1d) as s1, count(*) filter (where a.ad_sales_1d is null) as unk
           from core.ad_spend_daily a where a.company_id = p_company_id and a.mall = 'amazon' and a.date_jst between p_from and p_to and a.listing_id is not null group by a.listing_id),
  lsk as (select * from mart.listings_to_skus(p_company_id, (select coalesce(array_agg(ad.listing_id), '{}') from ad))),
  aa as (select l.sku_id, sum(ad.cost) as cost, case when sum(ad.unk) = 0 then sum(ad.s1) end as s1, sum(ad.unk)::bigint as unk
           from ad join lsk l on l.listing_id = ad.listing_id and l.single group by l.sku_id),
  ids as (select ua.sku_id from ua union select aa.sku_id from aa)
  select k.sku_id, k.code, k.name, p.product_id, p.name, k.handling,
         coalesce(ua.units_net, 0), coalesce(ua.via, 0), coalesce(ua.by_mall, '{}'::jsonb),
         coalesce(sa.sales, 0), coalesce(sa.by_mall, '{}'::jsonb),
         coalesce(aa.cost, 0), aa.s1, coalesce(aa.unk, 0), coalesce(sa.amazon, 0),
         st.warehouse_qty::bigint, st.fba_jp_available::bigint,
         (st.warehouse_qty + st.fba_jp_available)::bigint, st.fba_jp_inbound::bigint,
         case when st.warehouse_qty is not null and st.fba_jp_available is not null then least(st.warehouse_as_of, st.fba_jp_as_of) end,
         round(coalesce(ua.units_net, 0)::numeric / (p_to - p_from + 1), 2),
         case when coalesce(ua.units_net, 0) > 0 then round((st.warehouse_qty + st.fba_jp_available)::numeric / (ua.units_net::numeric / (p_to - p_from + 1)), 1) end
    from ids join core.skus k on k.company_id = p_company_id and k.sku_id = ids.sku_id
    left join core.products p on p.company_id = k.company_id and p.product_id = k.product_id
    left join ua on ua.sku_id = ids.sku_id left join sa on sa.sku_id = ids.sku_id left join aa on aa.sku_id = ids.sku_id
    left join mart.v_sku_stock st on st.company_id = k.company_id and st.sku_id = k.sku_id
$$;
comment on function mart.sku_activity(smallint, date, date) is 'SKU ごとの動き (0042): 正味の販売数量 (セットは構成品に展開)・売上 (展開しきった 1 SKU だけの品物)・Amazon の広告費・在庫 (不明は null) と何日もつか。先に mart.sku_activity_gaps で割り振れなかった分を見る';

create or replace function mart.sku_activity_gaps(p_company_id smallint, p_from date, p_to date)
returns table (units_total bigint, units_unexpanded bigint, sales_total bigint, sales_attributed bigint, sales_on_sets bigint, sales_unexpanded bigint,
  ad_total numeric, ad_attributed numeric, ad_on_sets numeric, ad_unlinked numeric)
language sql stable as $$
  with e as (select * from mart.sales_expanded_to_skus(p_company_id, p_from, p_to)),
  rowlvl as (select e.sid, max(e.src_units) as src_units, max(e.src_sales) as src_sales, not bool_and(e.complete) as unexpanded, bool_or(e.single) as single from e group by e.sid),
  ad as (select a.listing_id, sum(a.ad_cost) as cost from core.ad_spend_daily a where a.company_id = p_company_id and a.mall = 'amazon' and a.date_jst between p_from and p_to group by a.listing_id),
  lsk as (select distinct l.listing_id, l.complete, l.single from mart.listings_to_skus(p_company_id, (select coalesce(array_agg(ad.listing_id) filter (where ad.listing_id is not null), '{}') from ad)) l)
  select coalesce(sum(r.src_units), 0)::bigint,
         coalesce(sum(r.src_units) filter (where r.unexpanded), 0)::bigint,
         coalesce(sum(r.src_sales), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where r.single), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where not r.single and not r.unexpanded), 0)::bigint,
         coalesce(sum(r.src_sales) filter (where r.unexpanded), 0)::bigint,
         (select coalesce(sum(ad.cost), 0) from ad),
         (select coalesce(sum(ad.cost), 0) from ad join lsk on lsk.listing_id = ad.listing_id and lsk.single),
         (select coalesce(sum(ad.cost), 0) from ad join lsk on lsk.listing_id = ad.listing_id and lsk.complete and not lsk.single),
         (select coalesce(sum(ad.cost), 0) from ad where ad.listing_id is null or not exists (select 1 from lsk where lsk.listing_id = ad.listing_id and lsk.complete))
    from rowlvl r
$$;
comment on function mart.sku_activity_gaps(smallint, date, date) is 'SKU に割り振れなかった分 (0042): 展開しきれない販売 (一部だけ届いた行も)・複数 SKU のセットの売上・広告費 (セット / 出品が分からない・展開しきれない)';
