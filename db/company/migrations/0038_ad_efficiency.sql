-- 0038: 出品ごとの広告の効き目 (Company DB構想 11 の次 = 08 §4.5 の v_order_profit / v_product_360 の前に、広告費 × 売上日次だけで作れる部分)
--   mart.ad_efficiency(会社, モール, scope, from, to, 日ごとか) = 出品ごとの 広告費・広告経由の売上 (1 日の帰属)・売上 (売上日次)・TACoS・ACoS・広告経由の割合
--   mart.ad_efficiency_coverage(会社, モール, scope, from, to) = その期間の材料がそろっているか (広告費の日・古い取込の行の日・売上日次の未公開の日・公開の後に注文が動いた日 = 作り直し待ち (watermark より後。控えめ = 作り直し済みでも出ることがある)・開いた回)
--
-- 決め:
--   ・関数にする (view にしない): 売上日次は 60 万行超。view だと日付の条件が集計の後ろに残り全期間を走査する (0021 の sales_daily_check と同じ理由)
--   ・結ぶ鍵は出品 (listing_id)。広告の SKU の行 (0035 = core.resolve_listing_id) と注文の明細 (0013/0016) は同じ関数で出品に当たる = 同じ出品に集まる
--   ・出品に当たらない行は捨てない: 広告は 'ad:<粒度>:<対象>' (asin・none・出品の分からない sku)、売上は 'sales:unresolved' の行にまとめる (unresolved_key)
--   ・売上 = 売上日次の sales_jpy (注文日 JST・取消を引く・送料を含み店負担の値引を引く = D-31)。自社発送と FBA (shop_code) はまとめる
--   ・比率は分母が 0 か分からないとき null (0 で割らない・0 と読ませない)。🚨 **一部だけ分かっている和で比率を作らない** (#1492 Codex R1):
--       広告経由の売上が分からない行 (ad_unknown_rows > 0) があれば ACoS・広告経由の割合は null / 売上に金額の分からない明細 (sales_amount_unknown_lines > 0) があれば TACoS・広告経由の割合は null
--   ・order_grains = 売上日次の粒度 (出品 × SKU × 出荷元) ごとの注文数の **延べ** (同じ注文が SKU・出荷元で分かれると重複する。注文数そのものではない)
--   ・🚨 広告経由の売上 (sales1d) は Amazon の帰属 = 広告をクリックした 1 日以内の購入。広告した SKU 以外の購入 (同じ出品者の別の商品) も入りうる = 出品の売上の内訳ではない
--     → ad_sales_share (広告経由の割合) が 1 を超えることがある (その出品の売上より、その広告から生まれた売上が多い)
--   ・材料の欠け (広告費の日が無い・売上日次が未公開) は結果の行に混ぜない = ad_efficiency_coverage で先に確かめる

create or replace function mart.ad_efficiency(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date, p_by_day boolean default false)
returns table (date_jst date, listing_id bigint, listing_code text, title text, unresolved_key text,
  ad_cost numeric, clicks bigint, impressions bigint, ad_sales_1d numeric, ad_units_1d bigint, ad_unknown_rows integer,
  sales_jpy bigint, units_net bigint, order_grains bigint, sales_amount_unknown_lines bigint, sales_unresolved_lines bigint,
  tacos numeric, acos_1d numeric, ad_sales_share numeric)
language sql stable as $$
  with ad as (
    select case when p_by_day then a.date_jst end as d, a.listing_id as lid,
           case when a.listing_id is null then 'ad:' || a.target_granularity || case when a.target_code <> '' then ':' || a.target_code else '' end end as uk,
           sum(a.ad_cost) as cost, sum(a.clicks)::bigint as clicks, sum(a.impressions)::bigint as imp,
           sum(a.ad_sales_1d) as s1, sum(a.units_1d)::bigint as u1, count(*) filter (where a.ad_sales_1d is null)::int as unk
      from core.ad_spend_daily a
     where a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.date_jst between p_from and p_to
     group by 1, 2, 3
  ), sa as (
    select case when p_by_day then s.date_jst end as d, s.listing_id as lid,
           case when s.listing_id is null then 'sales:unresolved' end as uk,
           sum(s.sales_jpy)::bigint as sales, sum(s.units_ordered - s.units_cancelled)::bigint as units, sum(s.orders)::bigint as grains,
           sum(s.lines_amount_unknown)::bigint as amt_unknown, sum(s.lines_unresolved)::bigint as unres
      from mart.v_sales_daily s
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key and s.date_jst between p_from and p_to
     group by 1, 2, 3
  ),
  -- full join は等号の鍵だけ受ける (is not distinct from は不可) → null の出ない鍵を作って結ぶ
  adk as (select ad.*, coalesce(ad.d, date '1900-01-01') as kd, coalesce(ad.lid, -1) as kl, coalesce(ad.uk, '') as ku from ad),
  sak as (select sa.*, coalesce(sa.d, date '1900-01-01') as kd, coalesce(sa.lid, -1) as kl, coalesce(sa.uk, '') as ku from sa)
  select coalesce(adk.d, sak.d) as date_jst, coalesce(adk.lid, sak.lid) as listing_id, l.listing_code, l.title, coalesce(adk.uk, sak.uk) as unresolved_key,
         coalesce(adk.cost, 0) as ad_cost, coalesce(adk.clicks, 0) as clicks, coalesce(adk.imp, 0) as impressions, adk.s1 as ad_sales_1d, adk.u1 as ad_units_1d, coalesce(adk.unk, 0) as ad_unknown_rows,
         coalesce(sak.sales, 0) as sales_jpy, coalesce(sak.units, 0) as units_net, coalesce(sak.grains, 0) as order_grains,
         coalesce(sak.amt_unknown, 0) as sales_amount_unknown_lines, coalesce(sak.unres, 0) as sales_unresolved_lines,
         case when sak.sales > 0 and coalesce(sak.amt_unknown, 0) = 0 then round(coalesce(adk.cost, 0) / sak.sales, 4) end as tacos,
         case when adk.s1 > 0 and adk.unk = 0 then round(adk.cost / adk.s1, 4) end as acos_1d,
         case when sak.sales > 0 and coalesce(sak.amt_unknown, 0) = 0 and adk.s1 is not null and adk.unk = 0 then round(adk.s1 / sak.sales, 4) end as ad_sales_share
    from adk full join sak on adk.kd = sak.kd and adk.kl = sak.kl and adk.ku = sak.ku
    left join core.listings l on l.company_id = p_company_id and l.listing_id = coalesce(adk.lid, sak.lid)
$$;
comment on function mart.ad_efficiency(smallint, text, text, date, date, boolean) is '出品ごとの広告の効き目 (0038)。先に mart.ad_efficiency_coverage で材料がそろっているか確かめる';

create or replace function mart.ad_efficiency_coverage(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (days integer, ad_days integer, ad_legacy_days integer, ad_missing_days date[], order_days integer, sales_unpublished_days date[], sales_pending_days date[], sales_session_open boolean)
language sql stable as $$
  with d as (select g::date as day from generate_series(p_from, p_to, interval '1 day') g),
  x as (
    select d.day,
           exists (select 1 from core.ad_spend_days a where a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.date_jst = d.day) as has_ad,
           exists (select 1 from core.ad_spend_days a where a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.date_jst = d.day and a.source_report_id like 'legacy:%') as legacy,
           exists (select 1 from core.orders o where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = d.day) as has_orders,
           exists (select 1 from mart.sales_daily_published p where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key and p.date_jst = d.day) as published,
           -- 作り直し待ち = 前回そろって終わった回 (watermark) の後に注文が動いた日 (公開済みでも古い)。watermark が無ければ注文のある日は全部 (#1492 Codex R1)
           --   🚨 作り直しの本体 (0021 refresh_sales_daily) と同じく watermark − 15 分 まで遡る: updated_at は取込の取引の開始時刻 = 集計の前に始まり後で commit した取込は watermark より前の時刻を持つ (#1492 Codex R2)
           exists (select 1 from core.orders o where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = d.day
                     and o.updated_at > coalesce((select s.watermark - interval '15 minutes' from mart.sales_daily_state s where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key), '-infinity'::timestamptz)) as pending
      from d
  )
  select count(*)::int, count(*) filter (where has_ad)::int, count(*) filter (where legacy)::int,
         coalesce(array_agg(day order by day) filter (where not has_ad), '{}'),
         count(*) filter (where has_orders)::int,
         coalesce(array_agg(day order by day) filter (where has_orders and not published), '{}'),
         coalesce(array_agg(day order by day) filter (where published and pending), '{}'),
         exists (select 1 from mart.sales_daily_state s where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key and s.session_id is not null)
    from x
$$;
comment on function mart.ad_efficiency_coverage(smallint, text, text, date, date) is '広告の効き目の材料がそろっているか (0038): 広告費の日・古い取込の行の日・売上日次の未公開の日・公開後に注文が動いた日 (作り直し待ち)・開いた回';
