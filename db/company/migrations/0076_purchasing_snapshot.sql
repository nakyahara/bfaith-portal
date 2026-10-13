-- 0076: 発注の設定の読むだけの写し (schema purchasing) (2026-10-13。AI_reference CompanyDB構想/21 v4 の C2・中原さんの決定 1 = a / 5 = b)
-- 🚨 前提 = 0001 (core.make_append_only)・0007 / 0009 (mart.v_product_360)・0075 (発注の設定の知らせ = 夜間の段の先の段)
--
-- なぜ: 発注の設定 (発注ロット・発注条件グループ・原料グループ・ケース・容量) の正本は発注アプリ (Render の SQLite の po_*・D-44・決定 1 = a)。
--   Company DB からは見えない = AI や他の業務が読めない。→ Company DB に「読むだけの写し」を置く (正本にはしない・持ち主の表 master-ownership には入れない)。
-- なにを:
--   1. purchasing.snapshot_runs = 写しの世代 (1 回の読み = 1 行・追記だけ)。read_at = 発注アプリの SQLite を読んだ時刻 = 写しの値は「この時の発注アプリの値」。
--      状態 applied (入れ替えた) / unchanged (前と同じ = 行は触らない) / rejected (完全に読めなかった = 入れ替えない・前の世代のまま)。件数・hash・問題・変化の数・
--      override (減りの受け入れを使った回 = env の値 token・表・件数。同じ token は 2 回目から効かない)
--   2. 写しの表 (鍵は発注アプリと同じ・行ごとに最後に値を変えた世代 run_id):
--      purchasing.sku_order_settings      = po_product_attrs (商品ごと: 発注ロット・発注条件グループ / 原料グループの紐づけ・容量/個・ケース)。
--                                           sku_id = 写した時に Company DB のコードで結んだ SKU (無ければ null)・primary_supplier_code = 写した時の代表の仕入先 (core)
--      purchasing.order_condition_groups  = po_order_conditions (発注条件グループの定義・仕入先つき)
--      purchasing.material_groups         = po_material_groups (原料グループの定義)
--   3. purchasing.v_snapshot_status = 今の写し (最後の applied / unchanged) の読んだ時刻・何時間前か・36 時間を超えたか・最後に断った回
--   4. mart.v_product_360 の最後に列を足す (発注ロット・グループ・ケース・容量と、写しの読んだ時刻・何時間前か)。
--      決定 5 = b (中原さん 10/13): 古い写し (36 時間超) も AI はそのまま使う = 行を外さない・時刻 (po_settings_read_at / po_settings_age_hours) を見せる
-- 書く人: 夜間ロードの後ろの段 (apps/company-db/load/po-settings-snapshot.mjs・表の持ち主のロール = COMPANY_DB_URL) だけ。本体のロードとは別の取引。
--   画面のロール・見張り・AI は読むだけ (書く権限は渡さない)。列の制限は要らない (秘密の値・ファイルのバイト列は無い)
-- 🚨 この migration は今のデータを何も変えない (新しい表は空 = 最初の夜間の段で入る)。バックアップ (apps/company-db/backup/dump.mjs) は取らない = 写しは発注アプリから作り直せる

create schema purchasing;
comment on schema purchasing is '発注アプリ (Render の SQLite の po_*) の発注の設定の読むだけの写し (0076・CompanyDB構想 21 C2)。正本は発注アプリ (D-44)。書くのは夜間ロードの後ろの段 (表の持ち主のロール) だけ';
revoke all on schema purchasing from public;

-- 1. 写しの世代
create table purchasing.snapshot_runs (
  run_id                 text primary key check (run_id ~ '^pos_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  company_id             smallint not null default 1 references core.companies,
  source                 text not null check (source in ('render-sqlite')),
  read_at                timestamptz not null,
  source_max_updated_at  text check (source_max_updated_at is null or length(source_max_updated_at) <= 40),
  status                 text not null check (status in ('applied', 'unchanged', 'rejected')),
  counts                 jsonb not null check (jsonb_typeof(counts) = 'object'),
  hashes                 jsonb not null check (jsonb_typeof(hashes) = 'object'),
  problems               jsonb not null default '[]'::jsonb check (jsonb_typeof(problems) = 'array'),
  changes                jsonb check (changes is null or jsonb_typeof(changes) = 'object'),
  override               jsonb check (override is null or (jsonb_typeof(override) = 'object' and override ? 'token')),   -- 減りの受け入れ (1 回だけ・Codex #1708 R1 M2)
  host                   text check (host is null or length(host) <= 100),
  started_at             timestamptz not null default now(),
  finished_at            timestamptz not null default clock_timestamp(),
  constraint ck_psr_rejected check ((status = 'rejected') = (jsonb_array_length(problems) > 0))
);
create index ix_snapshot_runs_read on purchasing.snapshot_runs (read_at desc) where status in ('applied', 'unchanged');
select core.make_append_only('purchasing', 'snapshot_runs');
comment on table purchasing.snapshot_runs is '発注の設定の写しの世代 (0076)。read_at = 発注アプリの SQLite を読んだ時刻。rejected = 完全に読めなかった (前の世代のまま)。追記だけ';

-- 2. 写しの表
create table purchasing.sku_order_settings (
  product_key            text primary key check (length(product_key) between 1 and 255 and product_key = lower(btrim(product_key))),
  product_code           text not null check (length(product_code) between 1 and 255),
  sku_id                 bigint,
  order_lot              numeric check (order_lot is null or order_lot > 0),
  condition_id           text,
  material_group_id      text,
  capacity_per_unit      numeric check (capacity_per_unit is null or capacity_per_unit > 0),
  case_group             text,
  case_lot               numeric check (case_lot is null or case_lot > 0),
  primary_supplier_code  text,
  created_via            text,
  source_created_at      text,
  source_updated_at      text,
  run_id                 text not null references purchasing.snapshot_runs
);
create index ix_sku_order_settings_sku on purchasing.sku_order_settings (sku_id);
comment on table purchasing.sku_order_settings is '発注アプリ po_product_attrs の読むだけの写し (0076)。正本は発注アプリ。sku_id / primary_supplier_code = 写した時の Company DB の値。run_id = この行の値を最後に変えた世代';
create table purchasing.order_condition_groups (
  condition_id           text primary key check (length(condition_id) between 1 and 255),
  supplier_code          text,
  maker_name             text,
  display_name           text not null,
  condition_type         text not null,
  condition_value        numeric not null,
  unit                   text,
  source_created_at      text,
  source_updated_at      text,
  run_id                 text not null references purchasing.snapshot_runs
);
comment on table purchasing.order_condition_groups is '発注アプリ po_order_conditions (発注条件グループ) の読むだけの写し (0076)';
create table purchasing.material_groups (
  group_id               text primary key check (length(group_id) between 1 and 255),
  name                   text not null,
  min_order_qty          numeric,
  unit                   text,
  source_created_at      text,
  source_updated_at      text,
  run_id                 text not null references purchasing.snapshot_runs
);
comment on table purchasing.material_groups is '発注アプリ po_material_groups (原料グループ) の読むだけの写し (0076)';

-- 3. 今の写しの様子 (1 行・まだ 1 回も入っていない = 行なし)
create view purchasing.v_snapshot_status as
  select r.run_id, r.read_at, r.source_max_updated_at, r.counts, r.status,
         round((extract(epoch from (now() - r.read_at)) / 3600)::numeric, 1) as age_hours,
         (now() - r.read_at) > interval '36 hours' as stale,
         (select max(x.finished_at) from purchasing.snapshot_runs x where x.status = 'rejected' and x.finished_at > r.finished_at) as rejected_since_at
    from purchasing.snapshot_runs r
   where r.status in ('applied', 'unchanged')
   order by r.read_at desc, r.finished_at desc
   limit 1;
comment on view purchasing.v_snapshot_status is '今の発注の設定の写し (0076)。read_at = いつの発注アプリの値か・age_hours・stale (36 時間超 = 決定 5 = b: 使ってよいが時刻を見せる)・rejected_since_at = その後に完全に読めなかった回';

-- 4. 商品 360 に発注の設定 (最後に列を足すだけ = 0009 の列はそのまま・同じ順)
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
),
po as (
  -- 🆕 0076: 発注の設定の写し (発注アプリが正本・写した時刻つき)。SKU に 1 行だけ (全角など正規化で同じ SKU に当たる 2 行が来ても行を増やさない = 鍵の小さい方)
  select distinct on (os.sku_id) os.sku_id, os.order_lot, os.condition_id, os.material_group_id, os.case_group, os.case_lot, os.capacity_per_unit
  from purchasing.sku_order_settings os where os.sku_id is not null
  order by os.sku_id, os.product_key
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
  greatest(s.updated_at, p.updated_at) as updated_at,
  -- 🆕 0076: 発注の設定 (発注アプリの写し)。po_settings_read_at = いつの発注アプリの値か (写しが無い = null)。古くても外さない (決定 5 = b)
  po.order_lot as po_order_lot, po.condition_id as po_condition_id, po.material_group_id as po_material_group_id,
  po.case_group as po_case_group, po.case_lot as po_case_lot, po.capacity_per_unit as po_capacity_per_unit,
  st.read_at as po_settings_read_at, st.age_hours as po_settings_age_hours
from core.skus s
left join core.products p on p.product_id = s.product_id
left join ids ip on ip.entity_type = 'product' and ip.entity_id = p.product_id
left join ids isk on isk.entity_type = 'sku' and isk.entity_id = s.sku_id
left join active_cost c on c.sku_id = s.sku_id
left join pkg k on k.product_id = p.product_id
left join malls m on m.sku_id = s.sku_id
left join po on po.sku_id = s.sku_id
left join purchasing.v_snapshot_status st on true;

-- 見張り (AI) は読むだけ (create-watch-roles.mjs の WATCH_SCHEMAS にも purchasing = 流し直しても同じ)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant usage on schema purchasing to watcher';
    execute 'grant select on all tables in schema purchasing to watcher';
  end if;
end $$;
