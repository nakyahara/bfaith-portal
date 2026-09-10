-- 0009 mart: v_product_360 の ASIN を「単品出品のもの」に限る
--
-- 何が困っていたか:
--   ASIN は listing → catalog_items 経由でしか持たない (product に直接 ASIN は付けない)。
--   その引き当てが「その SKU を含む出品なら何でも」だったので、**セット商品の ASIN が、中に入っている
--   単品 SKU の ASIN として出ていた**。価格は 0007 の時点で単品出品だけに絞ってあったのに、ASIN は
--   絞っていなかった (取りこぼし)。
--
--   例: 「A + B の 2 点セット」という Amazon 出品 (ASIN = X) があると、A の 360 行にも B の 360 行にも
--       asin = X が出る。A 単品の ASIN は別にあるのに、セットのほうが後勝ちで出ることがあった。
--
-- どう直すか:
--   価格と同じ条件 (構成が 1 行だけ・qty = 1) で絞る。単品出品が無い SKU の asin は null になる
--   (「分からない」を「分かっている」ように見せない)。
--
-- 🚨 列の顔ぶれは変えない (create or replace view で差し替えるだけ)。v_product_dq / v_cross_mall_diff は
--    この view を参照しているので、列を足したり順番を変えたりすると差し替えられない。

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
         -- 単品出品 (構成 1 行・qty=1) の ASIN だけ。セット出品の ASIN を、その構成 SKU の ASIN にしない (0009)
         max(ci.asin) filter (where lc.qty = 1 and sc.n = 1) as asin_via_listing
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
  m.asin_via_listing as asin,                           -- listing → catalog_items 経由だけ (0009 で単品出品に限定)
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
