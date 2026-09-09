-- 0007 mart: AI と画面が読む層 (06 §6.3)。v1 = 表だけで組める範囲 (販売速度・在庫・広告は Phase 2/3 で列を足す)
--
--   mart.v_listing_360     = 販路商品 1 行 (状態・価格・順位・評価 + 代表 SKU + 構成の配列)
--   mart.v_product_360     = SKU 1 行 (商品属性 + 原価 + 有効な梱包実測/推定 + 外部 ID + モール別の現在値 (配列) + 欠落フラグ)
--   mart.v_product_dq      = 属性の欠落 (1 行 1 欠落)。PW-05 の材料
--   mart.v_cross_mall_diff = 同じ SKU のモール間の値ズレ (単品出品どうしの価格・ソース別の最新 JAN の不一致)。PW-03 / PW-07 の材料
--   🚨 「10 の表から毎回組み立てる」をやめるための層。AI ロールはここと core の SELECT だけ (Phase 9)
--   🚨 ASIN は listing → catalog_items 経由だけ (product への直接 ASIN は external_ids の CHECK で拒む)

create or replace view mart.v_listing_360 as
select
  l.listing_id, l.company_id, l.mall, l.shop_code, l.listing_code, l.listing_norm, l.title as listing_title, l.status as listing_status,
  l.catalog_item_id, ci.asin, ci.marketplace_id, ci.package_scope as asin_package_scope,
  ls.status as current_status, ls.status_raw, ls.price_jpy, ls.sale_price_jpy, ls.points_rate, ls.stock_qty, ls.stock_unlimited,
  ls.fulfillment, ls.shipping_method, ls.delivery_free, ls.tax_rate as listing_tax_rate, ls.category_path, ls.issues_json,
  ls.price_observed_at, ls.status_observed_at, ls.stock_observed_at, ls.observed_at as state_observed_at,
  sd.snapshot_date as last_snapshot_date, sd.buybox_price_jpy, sd.buybox_is_ours, sd.offers_count, sd.lowest_price_jpy, sd.rating, sd.review_count,
  comp.component_count,
  comp.representative_sku_id, comp.representative_qty, s.code as representative_sku_code, s.name as representative_sku_name, s.sku_kind as representative_sku_kind, s.product_id as representative_product_id,
  comp.components_json,
  l.last_seen_at, l.mall_updated_at
from core.listings l
left join core.listing_states ls on ls.listing_id = l.listing_id
left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
left join lateral (
  select d.* from snapshots.listing_daily d where d.listing_id = l.listing_id order by d.snapshot_date desc limit 1
) sd on true
left join lateral (
  select count(*)::int as component_count,
         (array_agg(c.sku_id order by c.sort_order, c.sku_id))[1] as representative_sku_id,
         (array_agg(c.qty order by c.sort_order, c.sku_id))[1] as representative_qty,
         jsonb_agg(jsonb_build_object('sku_id', c.sku_id, 'qty', c.qty, 'resolution', c.resolution) order by c.sort_order, c.sku_id) as components_json
  from core.listing_components c where c.listing_id = l.listing_id
) comp on true
left join core.skus s on s.sku_id = comp.representative_sku_id;

create or replace view mart.v_product_360 as
with active_cost as (
  select sku_id, cost_jpy, cost_source, cost_status from core.sku_costs where valid_to is null
),
pkg as (
  select product_id, length_mm, width_mm, height_mm, weight_g, source_system as physical_source, is_measured
  from core.product_physicals where scope = 'package' and is_effective
),
ids as (
  select entity_type, entity_id,
         max(external_value) filter (where id_kind = 'jan') as jan,
         max(external_value) filter (where id_kind = 'fnsku') as fnsku
  from core.external_ids where valid_to is null
  group by entity_type, entity_id
),
malls as (
  -- 🚨 同じ店舗に複数出品 (FBA/FBM 併売など) があるので、店舗をキーにした object ではなく listing ごとの配列にする
  select lc.sku_id,
         count(*)::int as listing_count,
         count(*) filter (where coalesce(ls.status, l.status) = 'active')::int as active_listing_count,
         jsonb_agg(jsonb_build_object('listing_id', l.listing_id, 'mall', l.mall, 'shop_code', l.shop_code, 'listing_code', l.listing_code,
                                      'qty', lc.qty, 'status', coalesce(ls.status, l.status), 'price_jpy', ls.price_jpy,
                                      'sale_price_jpy', ls.sale_price_jpy, 'stock_qty', ls.stock_qty, 'fulfillment', ls.fulfillment,
                                      'asin', ci.asin, 'observed_at', ls.observed_at)
                   order by l.mall, l.shop_code, l.listing_id) as listings_json,
         -- 単品出品 (構成 1 行・qty=1) の価格だけ。A×1 + B×1 の組合せ価格を A の価格にしない
         min(ls.price_jpy) filter (where lc.qty = 1 and sc.n = 1) as min_price_jpy,
         max(ls.price_jpy) filter (where lc.qty = 1 and sc.n = 1) as max_price_jpy,
         max(ci.asin) as asin_via_listing
  from core.listing_components lc
  join core.listings l on l.listing_id = lc.listing_id
  left join core.listing_states ls on ls.listing_id = l.listing_id
  left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
  join lateral (select count(*)::int as n from core.listing_components x where x.listing_id = lc.listing_id) sc on true
  where l.status <> 'deleted'
  group by lc.sku_id
)
select
  s.sku_id, s.company_id, s.code as sku_code, s.code_norm, s.name as sku_name, s.sku_kind, s.handling, s.tax_rate, s.tax_class,
  p.product_id, p.name as product_name, p.brand, p.manufacturer, p.own_brand, p.sales_class, p.rakuten_genre_id, p.product_kind,
  p.unit_count, p.unit_count_uom, p.net_content, p.net_content_uom, p.release_date, p.discontinued_on, p.status as product_status,
  coalesce(ip.jan, isk.jan) as jan,
  m.asin_via_listing as asin,                           -- listing → catalog_items 経由だけ
  isk.fnsku,
  c.cost_jpy, c.cost_source, c.cost_status,
  k.length_mm as package_length_mm, k.width_mm as package_width_mm, k.height_mm as package_height_mm, k.weight_g as package_weight_g,
  k.physical_source, k.is_measured as package_is_measured,
  coalesce(m.listing_count, 0) as listing_count, coalesce(m.active_listing_count, 0) as active_listing_count,
  m.listings_json, m.min_price_jpy, m.max_price_jpy,
  array_remove(array[
    case when coalesce(ip.jan, isk.jan) is null then 'jan' end,
    case when p.brand is null then 'brand' end,
    case when k.weight_g is null then 'package_weight' end,
    case when k.length_mm is null or k.width_mm is null or k.height_mm is null then 'package_dims' end,
    case when c.cost_jpy is null then 'cost' end,
    case when s.tax_rate is null then 'tax_rate' end,
    case when p.rakuten_genre_id is null then 'genre' end,
    case when p.unit_count is null then 'unit_count' end,
    case when s.handling = 'active' and coalesce(m.active_listing_count, 0) = 0 then 'no_active_listing' end
  ], null) as dq_flags,
  greatest(s.updated_at, p.updated_at) as updated_at
from core.skus s
left join core.products p on p.product_id = s.product_id
left join ids ip on ip.entity_type = 'product' and ip.entity_id = p.product_id
left join ids isk on isk.entity_type = 'sku' and isk.entity_id = s.sku_id
left join active_cost c on c.sku_id = s.sku_id
left join pkg k on k.product_id = p.product_id
left join malls m on m.sku_id = s.sku_id;

create or replace view mart.v_product_dq as
select v.sku_id, v.sku_code, v.product_id, v.handling, f.flag as missing
from mart.v_product_360 v
cross join lateral unnest(v.dq_flags) as f(flag);

create or replace view mart.v_cross_mall_diff as
with prices as (
  -- 単品出品 = 構成が 1 行だけで qty=1 の listing だけを比べる (A×1 + B×1 の組合せ価格を A の価格にしない)
  select lc.sku_id, l.mall, l.listing_id, ls.price_jpy
  from core.listing_components lc
  join core.listings l on l.listing_id = lc.listing_id
  join core.listing_states ls on ls.listing_id = l.listing_id
  where lc.qty = 1 and ls.status = 'active' and ls.price_jpy is not null
    and (select count(*) from core.listing_components x where x.listing_id = lc.listing_id) = 1
),
price_diff as (
  select sku_id, 'price' as diff_kind,
         jsonb_object_agg(mall || ':' || listing_id, price_jpy order by mall, listing_id) as values_by_source,
         max(price_jpy) - min(price_jpy) as spread_jpy,
         round((max(price_jpy) - min(price_jpy))::numeric / nullif(min(price_jpy), 0), 3) as spread_ratio
  from prices group by sku_id having count(distinct price_jpy) > 1
),
jan_latest as (
  -- ソースごとの最新の観測だけを比べる (過去の訂正を「今の不一致」にしない)
  select distinct on (o.entity_id, o.source_system) o.entity_id as product_id, o.source_system, o.value_text
  from core.product_attribute_observations o
  where o.entity_type = 'product' and o.attribute = 'jan' and o.packaging_scope = 'item' and o.value_text is not null
  order by o.entity_id, o.source_system, o.observed_at desc, o.observation_id desc
),
jan_diff as (
  select s.sku_id, 'jan' as diff_kind,
         jsonb_object_agg(j.source_system, j.value_text order by j.source_system) as values_by_source,
         null::bigint as spread_jpy, null::numeric as spread_ratio
  from jan_latest j join core.skus s on s.product_id = j.product_id
  group by s.sku_id having count(distinct j.value_text) > 1
)
select * from price_diff
union all
select * from jan_diff;
