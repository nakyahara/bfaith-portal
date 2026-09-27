-- 0039: 出品ごとの広告の効き目 (0038) の直し (2026-09-27・本番のデータで分かったこと)
--   ① 売上に効く金額不明の明細だけ数える: 売上日次の lines_amount_unknown は Amazon の取消の明細 (数量 0・金額 null) も数える = 売上に効かないのに、
--      0038 はそれで TACoS・広告経由の割合を null にしていた (本番の直近 30 日で広告のある 1,600 出品のうち 719 出品・広告費の 79%)。
--      金額 null の明細 3,553 行は全部 数量 0 の取消の明細 → 売上日次の式 (0021 build_sales_daily_dates の camt) で売上に効く明細だけ数える (core から):
--      取り消された注文の明細 = 効かない / 数量 > 0 で全部取り消された明細 = 効かない / それ以外 (一部取消・取り消されていない注文の数量 0 の明細 = 0021 は商品代を足す) = 効く (#1493 Codex R1)
--   ② coverage の「作り直し待ち」(watermark − 15 分の後に注文が動いた日) は毎朝の push の直後の回でも出続ける (本番で直近 30 日のうち 22 日) = 見分けにならない
--      → sales_stale_days = ① 公開の値と、材料を今そのまま足した値が **実際に食い違う日** (mart.sales_daily_check。日の合計。遅れて commit した取込も出る)
--        ∪ ② 公開した回 (sales_daily_runs.started_at) より **後に** 注文が動いた日 (日の合計が変わらない出品の付け替えも出る。#1493 Codex R1 High)。
--        ② は遡らない = 回の前の push の更新は出さない (0038 の誤報を出さない)。① と ② の両方をすり抜けるのは「回の前に始まり後で commit した取込で、日の合計が変わらない変更」だけ
--   列が変わる関数 (coverage) は drop してから作る。ad_efficiency は列が同じ = create or replace

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
           sum(s.lines_unresolved)::bigint as unres
      from mart.v_sales_daily s
     where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key and s.date_jst between p_from and p_to
     group by 1, 2, 3
  ),
  -- 売上に効く金額不明の明細 (0039。売上日次の lines_amount_unknown は取消の明細 (数量 0) も数える = 売上に効かない)。条件は 0021 の camt と同じ
  unk as (
    select case when p_by_day then o.order_date_jst end as d, l.listing_id as lid, case when l.listing_id is null then 'sales:unresolved' end as uk, count(*)::bigint as n
      from core.orders o join core.order_lines l on l.company_id = o.company_id and l.order_id = o.order_id and l.removed_at is null
     where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst between p_from and p_to
       and l.line_amount_jpy is null and not o.is_cancelled and not (l.qty > 0 and l.cancelled_qty >= l.qty)
     group by 1, 2, 3
  ),
  -- full join は等号の鍵だけ受ける (is not distinct from は不可) → null の出ない鍵を作って結ぶ
  adk as (select ad.*, coalesce(ad.d, date '1900-01-01') as kd, coalesce(ad.lid, -1) as kl, coalesce(ad.uk, '') as ku from ad),
  unkk as (select coalesce(unk.d, date '1900-01-01') as kd, coalesce(unk.lid, -1) as kl, coalesce(unk.uk, '') as ku, unk.n from unk),
  -- 金額不明の数は 1 回だけ集計して鍵で結ぶ (売上の行ごとに読み直さない。#1493 Codex R1)
  sak as (select sa.*, k.kd, k.kl, k.ku, coalesce(unkk.n, 0) as amt_unknown
            from sa cross join lateral (select coalesce(sa.d, date '1900-01-01') as kd, coalesce(sa.lid, -1) as kl, coalesce(sa.uk, '') as ku) k
            left join unkk on unkk.kd = k.kd and unkk.kl = k.kl and unkk.ku = k.ku)
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
comment on function mart.ad_efficiency(smallint, text, text, date, date, boolean) is '出品ごとの広告の効き目 (0038・0039)。sales_amount_unknown_lines = 売上に効く金額不明の明細 (取消の明細は数えない)。先に mart.ad_efficiency_coverage で材料がそろっているか確かめる';

drop function mart.ad_efficiency_coverage(smallint, text, text, date, date);
create function mart.ad_efficiency_coverage(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (days integer, ad_days integer, ad_legacy_days integer, ad_missing_days date[], order_days integer, sales_unpublished_days date[], sales_stale_days date[], sales_session_open boolean)
language sql stable as $$
  with d as (select g::date as day from generate_series(p_from, p_to, interval '1 day') g),
  x as (
    select d.day,
           exists (select 1 from core.ad_spend_days a where a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.date_jst = d.day) as has_ad,
           exists (select 1 from core.ad_spend_days a where a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.date_jst = d.day and a.source_report_id like 'legacy:%') as legacy,
           exists (select 1 from core.orders o where o.company_id = p_company_id and o.mall = p_mall and o.scope_key = p_scope_key and o.order_date_jst = d.day) as has_orders,
           exists (select 1 from mart.sales_daily_published p where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key and p.date_jst = d.day) as published,
           -- ② 公開した回が始まった後に注文が動いた日 (遡らない)
           exists (select 1 from mart.sales_daily_published p join mart.sales_daily_runs r on r.run_id = p.run_id
                    join core.orders o on o.company_id = p.company_id and o.mall = p.mall and o.scope_key = p.scope_key and o.order_date_jst = p.date_jst and o.updated_at > r.started_at
                   where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key and p.date_jst = d.day) as touched
      from d
  ),
  -- ① 公開済みで、公開の値と材料が食い違う日 (0021 の検算。明細数・数量・取消・商品代・取消額・売上・払った額・金額不明の明細数のどれか)
  stale as (select c.date_jst from mart.sales_daily_check(p_company_id, p_mall, p_scope_key, p_from, p_to) c where c.is_published)
  select count(*)::int, count(*) filter (where has_ad)::int, count(*) filter (where legacy)::int,
         coalesce(array_agg(day order by day) filter (where not has_ad), '{}'),
         count(*) filter (where has_orders)::int,
         coalesce(array_agg(day order by day) filter (where has_orders and not published), '{}'),
         coalesce((select array_agg(z.day order by z.day) from (select s.date_jst as day from stale s union select x2.day from x x2 where x2.touched) z), '{}'),
         exists (select 1 from mart.sales_daily_state s where s.company_id = p_company_id and s.mall = p_mall and s.scope_key = p_scope_key and s.session_id is not null)
    from x
$$;
comment on function mart.ad_efficiency_coverage(smallint, text, text, date, date) is '広告の効き目の材料がそろっているか (0038・0039): 広告費の日・古い取込の行の日・売上日次の未公開の日・公開の値が古い日 (sales_daily_check の食い違い ∪ 公開した回の後に注文が動いた日)・開いた回';
