-- 0002 core: 会社・人・商品・SKU・販路商品・外部ID・仕入先・倉庫 (03 §2〜§3 + 06 §5.2〜§5.4)
--
-- 粒度 (03 §2.1 / 06 §5.2):
--   product = カタログ上の商品 (JAN 単位が目安。色・サイズ違いは別 product、親子は parent_product_id)
--   sku     = 自社が在庫・出荷する単位 = NE 商品コード粒度。単品は product と 1:1、セットは sku_components
--   listing = 販路商品 (モール × 出品コード)。1 listing = N sku × qty (listing_components)
-- 外部 ID は列を増やさず core.external_ids 1 表 (多対多・履歴つき・解決根拠つき)。
-- 🚨 ASIN は product の外部 ID にしない (06 §11-3): core.catalog_items (0003) に置き listing と 1:N。

create table core.companies (
  company_id   smallint primary key,
  name         text not null,
  kind         text not null check (kind in ('parent','subsidiary')),
  created_at   timestamptz not null default now()
);

create table core.workers (                              -- 人 (社員・作業者・いろは利用者)。company_id で分離 (D-10)
  worker_id      bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  staff_no       text,                                  -- staff.db の管理番号 (B-Faith)
  worker_code    text,                                  -- 'w01' 等の短縮表記
  display_name   text not null,
  login_email    text,
  worker_type    text not null check (worker_type in ('employee','part_time','contractor','iroha_user','iroha_staff','system')),
  active         boolean not null default true,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, staff_no),
  unique (login_email)
);
create trigger trg_workers_touch before update on core.workers for each row execute function core.touch_updated_at();

create table core.products (                             -- カタログ上の商品
  product_id       bigint generated always as identity primary key,
  company_id       smallint not null references core.companies,
  parent_product_id bigint references core.products,    -- バリエーション親
  display_code     text,                                -- 表示用 (代表 NE 商品コードなど)。主キーではない
  name             text not null,                       -- 社内標準の商品名 (モール別タイトルは listing_texts)
  brand            text,
  manufacturer     text,
  model_number     text,
  own_brand        boolean not null default false,
  sales_class      smallint check (sales_class between 1 and 4),   -- 1自社 2取扱限定 3仕入 4輸出
  rakuten_genre_id bigint,                              -- 分類 SSoT = 楽天ジャンル
  product_kind     text not null default 'general' check (product_kind in ('general','cosmetic','food','pet','medical_device','hazmat')),
  unit_count       integer check (unit_count > 0),      -- ①商品そのものの入数 (1 個買ったら何個入っているか。D-20)
  unit_count_uom   text,                                -- '個','本','枚','包'
  net_content      numeric(10,2) check (net_content >= 0),   -- 内容量の数値
  net_content_uom  text,                                -- 'ml','g','L'
  country_of_origin text,
  release_date     date,
  discontinued_on  date,
  expiry_managed   boolean not null default false,
  status           text not null default 'active' check (status in ('draft','active','discontinued')),
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now()
);
create index ix_products_company_status on core.products (company_id, status);
create index ix_products_parent on core.products (parent_product_id) where parent_product_id is not null;
create trigger trg_products_touch before update on core.products for each row execute function core.touch_updated_at();

create table core.skus (                                 -- 在庫・出荷単位 (NE 商品コード粒度)
  sku_id         bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  product_id     bigint references core.products,       -- 単品は NOT NULL、セット・例外は NULL 可
  sku_kind       text not null check (sku_kind in ('single','set','exception')),
  code           text not null,                         -- NE 商品コード原文
  code_norm      text not null,                         -- core.norm_code(code)。比較はこちら
  name           text not null,
  tax_rate       numeric(4,2) check (tax_rate in (0.08, 0.10)),   -- null = 未解決
  tax_class      text check (tax_class in ('STANDARD_10','REDUCED_8','MIXED','UNKNOWN')),
  handling       text not null default 'active' check (handling in ('active','discontinued','unknown')),
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, code_norm),
  constraint ck_skus_single_has_product check (sku_kind <> 'single' or product_id is not null)
);
create index ix_skus_product on core.skus (product_id);
create trigger trg_skus_touch before update on core.skus for each row execute function core.touch_updated_at();

create table core.sku_components (                       -- セット構成
  parent_sku_id  bigint not null references core.skus,
  child_sku_id   bigint not null references core.skus,
  qty            integer not null check (qty > 0),
  sort_order     smallint not null default 0,
  source         text not null check (source in ('ne','manual','giftset','imported')),
  created_at     timestamptz not null default now(),
  primary key (parent_sku_id, child_sku_id),
  constraint ck_sku_components_not_self check (parent_sku_id <> child_sku_id)
);

create table core.sku_costs (                            -- 原価 (有効期間付き。履歴 = 行の追加。D-4)
  sku_cost_id    bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  sku_id         bigint not null references core.skus,
  cost_jpy       bigint not null check (cost_jpy >= 0),
  cost_source    text not null check (cost_source in ('ne','manual','set_calc','override_zero','imported')),
  cost_status    text not null check (cost_status in ('COMPLETE','OVERRIDDEN','PARTIAL','MISSING')),
  valid_from     date not null,
  valid_to       date,
  reason         text,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  constraint ck_sku_costs_period check (valid_to is null or valid_to >= valid_from)
);
create unique index ux_sku_costs_active on core.sku_costs (sku_id) where valid_to is null;

create table core.suppliers (
  supplier_id    bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  code           text not null, code_norm text not null,
  name           text not null,
  order_method   text,
  lead_time_days integer check (lead_time_days >= 0),
  active         boolean not null default true,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, code_norm)
);
create trigger trg_suppliers_touch before update on core.suppliers for each row execute function core.touch_updated_at();

create table core.supplier_skus (                        -- 先方品番・発注条件 (入数 5 区分の ③④。D-20)
  supplier_id    bigint not null references core.suppliers,
  sku_id         bigint not null references core.skus,
  vendor_code    text,
  order_unit     text,                                   -- 先方の発注単位 ('case','inner','each')
  stock_units_per_order_unit integer check (stock_units_per_order_unit > 0),   -- ③ 発注単位 → 在庫単位の換算
  min_order_qty  integer check (min_order_qty > 0),      -- ④ 最低発注数量 (order_unit で数える)
  order_multiple integer check (order_multiple > 0),     -- ④ 発注の増分 (order_unit で数える)
  unit_cost_jpy  bigint check (unit_cost_jpy >= 0),
  lead_time_days integer check (lead_time_days >= 0),
  active         boolean not null default true,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
  primary key (supplier_id, sku_id)
);
create trigger trg_supplier_skus_touch before update on core.supplier_skus for each row execute function core.touch_updated_at();

create table core.listings (                             -- 販路商品
  listing_id     bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  mall           text not null check (mall in ('amazon','amazon_us','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  shop_code      text not null default '',               -- Yahoo store_id / Amazon sellerId@marketplace 等。無いモールは ''
  listing_code   text not null,                         -- モール側の出品コード原文 (seller_sku / manageNumber / item_code ...)
  listing_norm   text not null,                         -- core.norm_code(listing_code)
  title          text,
  status         text not null default 'active' check (status in ('active','inactive','deleted','unknown')),
  listing_url    text,
  mall_item_id   text,                                  -- モール内部 ID (楽天 itemId / Qoo10 item_no)
  parent_listing_id bigint references core.listings,   -- バリエーション親
  variation_theme text,
  catalog_item_id bigint,                               -- Amazon の ASIN (0003 で FK を張る)。1 catalog_item : N listings
  first_seen_at  timestamptz,
  last_seen_at   timestamptz,                           -- complete=true の取得で存在を確認した時刻だけ更新
  mall_updated_at timestamptz,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (mall, shop_code, listing_norm)
);
create index ix_listings_company_mall on core.listings (company_id, mall, status);
create trigger trg_listings_touch before update on core.listings for each row execute function core.touch_updated_at();

create table core.listing_components (                   -- listing = N sku × qty (m_sku_components / f_*_sku_map の後継)
  listing_id     bigint not null references core.listings,
  sku_id         bigint not null references core.skus,
  qty            integer not null check (qty > 0),
  sort_order     smallint not null default 0,
  resolution     text not null check (resolution in ('exact','normalized','concat','map','manual','inferred','imported')),
  resolved_by_type text not null check (resolved_by_type in ('human','ai','system')),
  resolved_by_id text,
  evidence       jsonb,
  created_at     timestamptz not null default now(),
  primary key (listing_id, sku_id)
);
create index ix_listing_components_sku on core.listing_components (sku_id);

-- 外部 ID (1 表。付け替えは valid_to を埋めてから新行。同一 system/kind の値は有効期間内で 1 エンティティだけ)
create table core.external_ids (
  external_id_row   bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  entity_type       text not null check (entity_type in ('product','sku','listing','catalog_item','supplier','worker','location','document')),
  entity_id         bigint not null,
  system            text not null,      -- 'ne','amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari','logizard','jan','asin','fnsku','notion','drive','pricetar'
  id_kind           text not null,      -- 'product_code','seller_sku','asin','fnsku','jan','manage_number','item_code','item_id','page_id','file_id',...
  external_value    text not null,      -- 原文
  external_norm     text not null,      -- 正規化 (core.norm_code)
  resolution        text not null check (resolution in ('exact','normalized','concat','map','manual','inferred','imported')),
  resolved_by_type  text not null check (resolved_by_type in ('human','ai','system')),
  resolved_by_id    text,
  evidence          jsonb,
  valid_from        timestamptz not null default now(),
  valid_to          timestamptz,
  created_at        timestamptz not null default now(),
  constraint ck_external_ids_period check (valid_to is null or valid_to >= valid_from)
);
create unique index ux_external_ids_active on core.external_ids (system, id_kind, external_norm) where valid_to is null;
create index ix_external_ids_entity on core.external_ids (entity_type, entity_id);

create table core.warehouses (
  warehouse_id smallint primary key,
  company_id   smallint not null references core.companies,
  code         text not null unique,
  name         text not null,
  kind         text not null check (kind in ('own','fba','3pl','virtual')),
  created_at   timestamptz not null default now()
);

create table core.locations (                            -- 倉庫ロケ (ロジザード体系 + 仮想)。いろは棟は building='iroha' (D-8)
  location_id    bigint generated always as identity primary key,
  warehouse_id   smallint not null references core.warehouses,
  code           text not null,                        -- 'P3FA-001-002-03'
  block          text, floor smallint, col smallint, bay smallint, level smallint,
  building       text check (building in ('main','iroha')),
  is_pick_face   boolean not null default false,
  is_virtual     boolean not null default false,
  active         boolean not null default true,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
  unique (warehouse_id, code)
);
create trigger trg_locations_touch before update on core.locations for each row execute function core.touch_updated_at();
