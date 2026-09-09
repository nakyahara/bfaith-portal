-- 0003 core: 商品の属性 (観測と解決)・Amazon カタログ・販路商品の現在値と文言 (06 §5.3〜§5.4、§11)
--
-- 考え方 (06 §5.3): 属性は出どころによって別の値を言う (JAN が NE とロジザードと Amazon で違う、重量が API と実測で違う)。
--   観測 (product_attribute_observations) は全部 append-only (trigger で強制) で残し、
--   解決 (core の列) は attribute_resolution_rules の優先順位で 1 つ選ぶ。採用した観測と規則版は attribute_resolutions に残す。
--   🚨 同じ対象・同じ包装範囲 (packaging_scope) の値だけを比べる (3 個パックの寸法を単品に当てない)。
--   🚨 観測の一意性は「内容」ではなく「観測の回」(observation_key) で取る。A→B→A の 3 回目も、同じ値の再観測も残る。
--      content_hash は「前回と同じか」を見るための列で、unique ではない (06 §11-1 と同じ考え)

-- 規則の版 (解決結果が参照する。存在しない版を書けない)
create table core.rule_versions (
  rule_version text primary key,
  note         text,
  created_at   timestamptz not null default now()
);

-- 物理属性 (item / package / case)。実測 (measured) は API 値より優先。
-- 各軸の根拠 (どの観測を採用したか) は attribute_resolutions (attribute = 'package_weight_g' など) に属性ごとに残す
create table core.product_physicals (
  product_physical_id bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  product_id     bigint not null references core.products,
  scope          text not null check (scope in ('item','package','case')),
  length_mm      integer check (length_mm > 0),
  width_mm       integer check (width_mm > 0),
  height_mm      integer check (height_mm > 0),
  weight_g       integer check (weight_g > 0),
  units_per_case integer check (units_per_case > 0),    -- ② ケース入数 (scope='case')
  case_content_sku_id bigint references core.skus,      -- ケースの中身 (単品 SKU)
  source_system  text not null,                         -- 'measured','amazon_catalog','yahoo','ne','product_hub','supplier','imported'
  source_ref     text,
  is_measured    boolean not null default false,
  observed_at    timestamptz not null,
  is_effective   boolean not null default false,        -- 優先規則で選ばれた 1 行
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now()
);
create unique index ux_product_physicals_effective on core.product_physicals (product_id, scope) where is_effective;
create index ix_product_physicals_product on core.product_physicals (product_id, scope);
create trigger trg_product_physicals_touch before update on core.product_physicals for each row execute function core.touch_updated_at();

-- 表示義務・法令属性 (化粧品・食品)
create table core.product_compliance (
  product_id      bigint primary key references core.products,
  company_id      smallint not null references core.companies,
  ingredients     text,
  precautions     text,
  distributor     text,
  manufacturer_jp text,
  allergens       text,
  shelf_life_days integer check (shelf_life_days > 0),
  source_system   text not null,
  source_ref      text,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now()
);
create trigger trg_product_compliance_touch before update on core.product_compliance for each row execute function core.touch_updated_at();

-- 属性の観測 (誰が・いつ・どの包装範囲について・何と言ったか。append-only)
create table core.product_attribute_observations (
  observation_id  bigint generated always as identity primary key,
  observation_key text not null unique,                 -- 観測の回の冪等キー (例: '<ingest_run_id>:<entity>:<attribute>:<scope>' / 手入力は 'manual:<worker>:<time>')
  entity_type     text not null check (entity_type in ('product','sku','listing','catalog_item')),
  entity_id       bigint not null,
  attribute       text not null,                        -- 'jan','brand','manufacturer','unit_count','net_content','package_weight_g','package_length_mm',...
  packaging_scope text not null default 'item' check (packaging_scope in ('item','package','case','listing')),
  value_text      text,
  value_num       numeric,
  value_unit      text,
  raw_text        text,                                 -- 原文 (単位換算前)
  source_system   text not null,                        -- 'ne','amazon_catalog','amazon_listing','rakuten','yahoo','aupay','qoo10','logizard','product_hub','notion_import','fba_sheet_import','manual','measured'
  source_ref      text,
  raw_observation_id bigint,                            -- raw.<src>_observations.observation_id
  observed_at     timestamptz not null,
  content_hash    text not null,                        -- entity+attribute+scope+value+unit+source の安定ハッシュ (前回と同じかを見る。unique ではない)
  created_at      timestamptz not null default now(),
  constraint ck_pao_has_value check (value_text is not null or value_num is not null),
  -- 解決結果からの複合 FK 用 (観測 id と対象・属性・包装範囲の組を一意に)
  unique (observation_id, entity_type, entity_id, attribute, packaging_scope)
);
create index ix_pao_entity on core.product_attribute_observations (entity_type, entity_id, attribute, packaging_scope, observed_at desc);
create index ix_pao_latest_per_source on core.product_attribute_observations (entity_type, entity_id, attribute, packaging_scope, source_system, observed_at desc);
select core.make_append_only('core', 'product_attribute_observations');

-- 解決規則 (属性 × 包装範囲 × source の優先順位。小さいほど優先。データで持つ = 人も AI も読める)
create table core.attribute_resolution_rules (
  attribute       text not null,
  packaging_scope text not null default 'item',
  source_system   text not null,
  priority        smallint not null check (priority > 0),
  rule_version    text not null references core.rule_versions,
  primary key (attribute, packaging_scope, source_system, rule_version),
  unique (attribute, packaging_scope, rule_version, priority)
);

-- 解決結果 (core の列に入れた値が、どの観測・どの規則版から来たか)。
-- 複合 FK で「観測の対象・属性・包装範囲」と一致することを保証。規則版は、その属性・包装範囲に規則が存在する版だけ (trigger)
create table core.attribute_resolutions (
  entity_type     text not null,
  entity_id       bigint not null,
  attribute       text not null,
  packaging_scope text not null,
  resolved_observation_id bigint not null,
  rule_version    text not null references core.rule_versions,
  resolved_at     timestamptz not null default now(),
  primary key (entity_type, entity_id, attribute, packaging_scope),
  foreign key (resolved_observation_id, entity_type, entity_id, attribute, packaging_scope)
    references core.product_attribute_observations (observation_id, entity_type, entity_id, attribute, packaging_scope)
);
create or replace function core.check_resolution_rule_exists() returns trigger language plpgsql as $$
begin
  if not exists (
    select 1 from core.attribute_resolution_rules r
    where r.attribute = new.attribute and r.packaging_scope = new.packaging_scope and r.rule_version = new.rule_version
  ) then
    raise exception '規則版 % に 属性 % (包装範囲 %) の規則が無い', new.rule_version, new.attribute, new.packaging_scope using errcode = 'foreign_key_violation';
  end if;
  return new;
end
$$;
create trigger trg_attribute_resolutions_rule before insert or update on core.attribute_resolutions
  for each row execute function core.check_resolution_rule_exists();

-- Amazon カタログ (ASIN)。product の外部 ID にしない (06 §11-3)。自社 product/listing の存在を必須にしない (D-21)
create table core.catalog_items (
  catalog_item_id bigint generated always as identity primary key,
  marketplace_id  text not null,
  asin            text not null,
  package_scope   text not null default 'unknown' check (package_scope in ('single','multipack','set','unknown')),
  pack_count      integer check (pack_count > 0),       -- multipack のとき n
  parent_asin     text,
  variation_theme text,
  brand           text,
  manufacturer    text,
  item_name       text,
  browse_node_id  text,
  first_seen_at   timestamptz not null default now(),
  last_seen_at    timestamptz,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
  unique (marketplace_id, asin),
  constraint ck_catalog_items_pack_count check (package_scope <> 'multipack' or pack_count is not null)
);
create trigger trg_catalog_items_touch before update on core.catalog_items for each row execute function core.touch_updated_at();
alter table core.listings add constraint fk_listings_catalog_item foreign key (catalog_item_id) references core.catalog_items;
create index ix_listings_catalog_item on core.listings (catalog_item_id) where catalog_item_id is not null;

create table core.catalog_item_products (                -- ASIN → product の解決 (構成数量つき。未解決を許容)
  catalog_item_id bigint not null references core.catalog_items,
  product_id      bigint not null references core.products,
  qty             integer not null check (qty > 0),
  resolution      text not null check (resolution in ('exact','normalized','concat','map','manual','inferred','imported')),
  resolved_by_type text not null check (resolved_by_type in ('human','ai','system')),
  resolved_by_id  text,
  evidence        jsonb,
  created_at      timestamptz not null default now(),
  primary key (catalog_item_id, product_id)
);

-- 販路商品の「いまの状態」(最新スナップショットの投影。項目群ごとの鮮度を持つ)
create table core.listing_states (
  listing_id      bigint primary key references core.listings,
  status          text not null check (status in ('active','hidden','out_of_stock','suppressed','deleted','unknown')),
  status_raw      text,
  price_jpy       bigint check (price_jpy >= 0),
  sale_price_jpy  bigint check (sale_price_jpy >= 0),
  sale_from       timestamptz,
  sale_to         timestamptz,
  points_rate     numeric(5,2),
  stock_qty       integer,
  stock_unlimited boolean,
  fulfillment     text check (fulfillment in ('fba','fbm','own')),
  shipping_method text,
  delivery_free   boolean,
  tax_rate        numeric(4,2),
  lead_time_days  integer,
  category_path   text,
  issues_json     jsonb,
  price_observed_at  timestamptz,
  status_observed_at timestamptz,
  stock_observed_at  timestamptz,
  observed_at     timestamptz not null,
  snapshot_run_id text not null,
  updated_at      timestamptz not null default now()
);
create trigger trg_listing_states_touch before update on core.listing_states for each row execute function core.touch_updated_at();

-- 文言 (モール別タイトル・キャッチ・説明)。変化したときだけ新行 = 履歴。A→B→A も残る (unique は観測時刻)。
-- is_current の付け替えは「古い行を false に → 新しい行を true で insert」(update は is_current だけ許す)
create table core.listing_texts (
  listing_text_id bigint generated always as identity primary key,
  listing_id     bigint not null references core.listings,
  field          text not null check (field in ('title','catch','description','description_sp','description_html','bullet','meta_desc','search_keywords','abstract')),
  body           text not null,
  content_hash   text not null,
  observed_at    timestamptz not null,
  source_system  text not null,
  is_current     boolean not null default true,
  created_at     timestamptz not null default now(),
  unique (listing_id, field, observed_at, source_system)
);
create unique index ux_listing_texts_current on core.listing_texts (listing_id, field) where is_current;
create index ix_listing_texts_history on core.listing_texts (listing_id, field, observed_at desc);

-- 画像 (制作の正本は docs.documents = Drive/Canva。モールに載っている URL は Reference)
create table core.listing_images (
  listing_image_id bigint generated always as identity primary key,
  listing_id     bigint not null references core.listings,
  role           text not null,                        -- 'MAIN','PT01'..'PT08','SWCH' / 'main','sub1'..
  url            text not null,
  width_px       integer,
  height_px      integer,
  content_hash   text,
  observed_at    timestamptz not null,
  is_current     boolean not null default true,
  created_at     timestamptz not null default now()
);
create index ix_listing_images_current on core.listing_images (listing_id) where is_current;
