-- 0045 日次の財務を期間で引く関数の計画を直す (F2b・2026-09-30)
--
-- 0044 の mart.finance_daily_range は期間の月だけを読むようになったが、本番 (52 万行) で 2 月 1 か月に 33 秒・8/16〜9/29 は 120 秒で打ち切り。
--   実行計画 (2026-09-30 本番で EXPLAIN ANALYZE): 期間の決め (policy) との結びつけの範囲型 (daterange @>) の見込みが外れて「採用する行 = 1 行」(実際 5.5 万行) と見て、
--   日 × SKU (1.8 万行) と月の単価 (970 行) を 1 組ずつ突き合わせる (nested loop = 1,786 万回の比較) を選んでいた。作業用のメモリを増やしても変わらない。
-- 直し (式は同じ = 単価・返品数・利益の計算は 0043 / 0044 のまま):
--   ① 範囲型の結びつけを同じ意味の大小の比較に (period_from 以上・period_to が無ければ上限なし・period_to 未満)
--   ② 計上日の月を列に持ち、月の単価との結びつけを等号に (式 date_trunc の突き合わせをやめる)
--   ③ 関数の中だけ nested loop を使わない (SET enable_nestloop = off。見込みが外れても遅い計画に戻らない保険。関数の外には効かない)
--   本番で読むだけで試した: ① ② だけで 2 月 1.0 秒・8/16〜9/29 1.1 秒 / ③ だけで 0.4 秒・0.8 秒
--   試験 = scripts/test-company-db-finance.mjs の「関数と view が同じ期間で全列一致」(0044 と同じ)
create or replace function mart.finance_daily_range(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (company_id smallint, mall text, scope_key text, economic_date_jst date, seller_sku text,
  units_ordered integer, units_refunded_customer integer, units_marketplace_guarantee integer, units_a_to_z_refund integer, units_net_sold integer,
  sales_principal_jpy bigint, sales_shipping_jpy bigint, sales_giftwrap_jpy bigint, sales_tax_jpy bigint,
  commission_jpy bigint, fba_fulfillment_jpy bigint, fba_storage_jpy bigint, closing_fee_jpy bigint,
  shipping_chargeback_jpy bigint, giftwrap_chargeback_jpy bigint,
  promotion_jpy bigint, promotion_tax_jpy bigint, points_jpy bigint,
  warehouse_damage_jpy bigint, warehouse_lost_jpy bigint, safe_t_jpy bigint,
  refund_principal_jpy bigint, reversal_reimbursement_jpy bigint,
  misc_fee_jpy bigint, other_fee_jpy bigint, other_amount_jpy bigint,
  profit_before_cogs_jpy bigint, source_lines integer, order_rows integer)
language sql stable
set enable_nestloop = off
as $$
with adopted as (
  select f.*, date_trunc('month', f.economic_date_jst)::date as month from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
 where f.line_kind = 'sku'
   and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
   -- 単価 (計上日の月 × SKU) は月の全部の行で決まる = 期間の最初の月の 1 日から最後の月の末日までを読む
   and f.economic_date_jst >= date_trunc('month', p_from)::date
   and f.economic_date_jst < (date_trunc('month', p_to) + interval '1 month')::date
),
daily as (
  select company_id, mall, scope_key, economic_date_jst, seller_sku, month,
         sum(units_ordered) as units_ordered,
         sum(sales_principal_jpy) as sales_principal_jpy, sum(sales_shipping_jpy) as sales_shipping_jpy, sum(sales_giftwrap_jpy) as sales_giftwrap_jpy, sum(sales_tax_jpy) as sales_tax_jpy,
         -sum(commission_jpy) as commission_jpy, -sum(fba_fulfillment_jpy) as fba_fulfillment_jpy, -sum(fba_storage_jpy) as fba_storage_jpy,
         -sum(shipping_chargeback_jpy) as shipping_chargeback_jpy, -sum(giftwrap_chargeback_jpy) as giftwrap_chargeback_jpy,
         -sum(promotion_jpy) as promotion_jpy, -sum(promotion_tax_jpy) as promotion_tax_jpy, -sum(points_jpy) as points_jpy,
         sum(warehouse_damage_jpy) as warehouse_damage_jpy, sum(warehouse_lost_jpy) as warehouse_lost_jpy, sum(safe_t_jpy) as safe_t_jpy,
         -sum(refund_principal_jpy) as refund_principal_jpy, -sum(refund_principal_customer_jpy) as refund_customer_jpy, -sum(refund_principal_atoz_jpy) as refund_atoz_jpy,
         sum(reversal_reimbursement_jpy) as reversal_reimbursement_jpy,
         sum(misc_fee_jpy) as misc_fee_jpy, sum(other_fee_jpy) as other_fee_jpy, sum(other_amount_jpy) as other_amount_jpy,
         sum(source_lines) as source_lines, count(*) as order_rows
    from adopted
   group by company_id, mall, scope_key, economic_date_jst, seller_sku, month
),
unit_price_month as (
  select company_id, mall, scope_key, seller_sku, month,
         case when sum(units_ordered) = 0 then null
              else core.sqlite_round((sum(sales_principal_jpy) * 1000000)::float8 / sum(units_ordered)::float8) end as unit_price_micro
    from adopted
   group by company_id, mall, scope_key, seller_sku, month
),
est as (
  select d.*,
         coalesce(core.sqlite_round((d.refund_customer_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8), 0)::integer as units_refunded_customer,
         coalesce(core.sqlite_round((d.refund_atoz_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8), 0)::integer as units_a_to_z_refund
    from daily d
    left join unit_price_month u
      on u.company_id = d.company_id and u.mall = d.mall and u.scope_key = d.scope_key and u.seller_sku = d.seller_sku and u.month = d.month
)
select company_id, mall, scope_key, economic_date_jst, seller_sku,
       units_ordered::integer as units_ordered, units_refunded_customer, 0::integer as units_marketplace_guarantee, units_a_to_z_refund,
       (units_ordered - units_refunded_customer - units_a_to_z_refund)::integer as units_net_sold,
       sales_principal_jpy::bigint as sales_principal_jpy, sales_shipping_jpy::bigint as sales_shipping_jpy, sales_giftwrap_jpy::bigint as sales_giftwrap_jpy, sales_tax_jpy::bigint as sales_tax_jpy,
       commission_jpy::bigint as commission_jpy, fba_fulfillment_jpy::bigint as fba_fulfillment_jpy, fba_storage_jpy::bigint as fba_storage_jpy, 0::bigint as closing_fee_jpy,
       shipping_chargeback_jpy::bigint as shipping_chargeback_jpy, giftwrap_chargeback_jpy::bigint as giftwrap_chargeback_jpy,
       promotion_jpy::bigint as promotion_jpy, promotion_tax_jpy::bigint as promotion_tax_jpy, points_jpy::bigint as points_jpy,
       warehouse_damage_jpy::bigint as warehouse_damage_jpy, warehouse_lost_jpy::bigint as warehouse_lost_jpy, safe_t_jpy::bigint as safe_t_jpy,
       refund_principal_jpy::bigint as refund_principal_jpy, reversal_reimbursement_jpy::bigint as reversal_reimbursement_jpy,
       misc_fee_jpy::bigint as misc_fee_jpy, other_fee_jpy::bigint as other_fee_jpy, other_amount_jpy::bigint as other_amount_jpy,
       (sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy - commission_jpy - fba_fulfillment_jpy - fba_storage_jpy
         - shipping_chargeback_jpy - giftwrap_chargeback_jpy - promotion_jpy - points_jpy - refund_principal_jpy
         + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy)::bigint as profit_before_cogs_jpy,
       source_lines::integer as source_lines, order_rows::integer as order_rows
  from est
 where economic_date_jst between p_from and p_to
$$;
