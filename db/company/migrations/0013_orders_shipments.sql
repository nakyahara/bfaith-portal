-- 0013 受注・出荷 (Phase 6 前半。08 §4.1〜4.3 / §4.7。D4)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』(草案 v4 = Codex 3 巡 + PR のレビュー)。
--   受注の raw は Company DB に持ち込まない (年 236 万注文。§4.1)。miniPC が warehouse.db の追記ログから core の形に整えて §4.7 の契約で push する (取込ジョブ = D5)。
--   注文 = モールの注文 1 件 (core.orders / order_lines)、出荷 = NE の伝票 1 件 (core.shipments / shipment_lines)。状態の履歴は持たない (D-36 = a。出荷済み・取消の時刻を列で)。
--   同梱 (複数の注文 → 1 伝票) は表せない前提 (D-30 の回答待ち。実測 2025-01〜 で 1 注文 → 複数伝票は 1 件だけ)。
--
--   core.order_status_map   = 状態の正規化はデータで (NE = 1 受注メール取込済 / 2 起票済 / 20 納品書印刷待ち / 40 納品書印刷済 / 50 出荷確定済)。未登録は unknown で入れて DQ に出す
--   core.ne_shops           = NE の店舗コード → モール・scope・注文番号の接頭辞 (Yahoo は 'b-faith01-' + NE 受注番号)。伝票と注文を結ぶ根拠。実データ (warehouse.db の shops) から
--   core.mall_order_policy  = モール × scope の「注文を入れてよいか」。Yahoo は約款 第 10 条の確認まで false (D-32 = b)。apply_order_batch が見る
--   core.orders / order_lines / shipments / shipment_lines = 上のとおり。取得世代 = received_batch_seq (最後に受け取った世代。内容が同じでも進める) / source_updated_at (内容の更新時刻) /
--                             content_hash (ヘッダの指紋) + lines_checksum (明細集合の指紋) / transform_version (§4.7)。金額は JPY だけ (bigint)
--   core.apply_order_batch() / core.apply_shipment_batch() = §4.7 の契約 (ヘッダを for update → 古い世代は 'stale' → 内容が同じなら世代だけ進めて 'same' → 同じ世代で内容違いは例外 →
--                             ヘッダ更新 + 明細の丸ごと置換で 'applied'。1 取引)。内部 ID (listing_id / sku_id) は Render が解決する (会社 × モールで 1 件に当たるときだけ。当たらなければ unresolved_code)
--   core.link_shipment_order() / core.relink_shipments() = 伝票の NE 受注番号 → 注文 (ne_shops の接頭辞つき)。注文が未着なら null のまま → 夜間に結び直す
--   events.inventory_events.shipment_line_id = exact の在庫イベントを出荷明細に結ぶ (会社一致の複合 FK)
--   mart.v_shipments_daily  = 既存 f_shipments_daily と同じ式 (slips = 出荷確定日のある伝票の数 (取消を含む)、cancelled_slips = 内数、delivery_name = 出荷確定日が一番新しい伝票の名称)
-- 🚨 0004 の raw 13 ソース・0011 の在庫・0012 の財務には触らない。既存 events.inventory_events に列を 1 つ足す (null 可) だけ。

-- ─── 状態の正規化 (データで持つ) ───
create table core.order_status_map (
  source_system  text not null,                        -- 'ne' / 'amazon' / 'rakuten' / ...
  source_value   text not null,
  status         text not null check (status in ('new','confirmed','ready','shipped','delivered','cancelled','on_hold','returned','unknown')),
  note           text,
  created_at     timestamptz not null default now(),
  primary key (source_system, source_value)
);
insert into core.order_status_map (source_system, source_value, status, note) values
  ('ne', '1',  'new',       '受注メール取込済'),
  ('ne', '2',  'confirmed', '起票済'),
  ('ne', '20', 'ready',     '納品書印刷待ち'),
  ('ne', '40', 'ready',     '納品書印刷済'),
  ('ne', '50', 'shipped',   '出荷確定済');
create or replace function core.map_order_status(p_source_system text, p_source_value text) returns text language sql stable as $$
  select coalesce((select m.status from core.order_status_map m where m.source_system = p_source_system and m.source_value = p_source_value), 'unknown');
$$;

-- ─── NE の店舗コード → モール (伝票と注文を結ぶ根拠。warehouse.db の shops を 2026-09-14 に写した) ───
create table core.ne_shops (
  company_id      smallint not null references core.companies,
  shop_code       text not null,
  shop_name       text not null,
  platform        text not null,                       -- NE 側の呼び名 (rakuten / amazon_fbm / _ignore ...)
  mall            text check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari','other')),   -- null = 注文と結ばない (FBA 納品・社内)
  scope_key       text,
  order_no_prefix text not null default '',            -- モールの注文番号 = 接頭辞 || NE 受注番号 (Yahoo は 'b-faith01-')
  active          boolean not null default true,
  note            text,
  primary key (company_id, shop_code),
  constraint ck_ne_shops_mall_scope check ((mall is null) = (scope_key is null))
);
insert into core.ne_shops (company_id, shop_code, shop_name, platform, mall, scope_key, order_no_prefix, note) values
  (1, '1',  '雑貨イズム楽天市場店',         'rakuten',       'rakuten',  'main', '',            null),
  (1, '2',  '雑貨イズムYahoo!店',           'yahoo',         'yahoo',    'main', 'b-faith01-',  'NE 受注番号は 8 桁だけ。モール側は b-faith01-99999999'),
  (1, '3',  'ヤフオク店',                   'yahoo_auction', 'other',    'main', '',            null),
  (1, '4',  '雑貨イズムAmazon店',           'amazon_fbm',    'amazon',   'jp',   '',            '自社発送分だけ (FBA は NE を通らない)'),
  (1, '5',  '雑貨イズムauPay!店',           'aupay',         'aupay',    'main', '',            null),
  (1, '6',  '雑貨イズムQoo10店',            'qoo10',         'qoo10',    'main', '',            '注文の鍵は source_order_key (一致 62%。残りは要調査)'),
  (1, '7',  'ライジングAmazon',             '_ignore',       null,       null,   '',            '対象外'),
  (1, '8',  '雑貨イズムメルカリshops',      'mercari',       'mercari',  'main', '',            null),
  (1, '9',  'ラクマ',                       'rakuma',        'other',    'main', '',            null),
  (1, '10', '卸',                           'wholesale',     'other',    'main', '',            null),
  (1, '11', 'LINEギフト',                   'linegift',      'linegift', 'main', '',            null),
  (1, '12', '雑貨イズムMy Smart Store店',   'mysmartstore',  'other',    'main', '',            null),
  (1, '13', '雑貨イズムdショッピング',      'dshopping',     'other',    'main', '',            null),
  (1, '14', 'LINE ギフト',                  'linegift',      'linegift', 'main', '',            null),
  (1, '15', 'FBA納品',                      '_ignore',       null,       null,   '',            '対象外 (FBA 納品の伝票)');

-- ─── 注文を入れてよいか (モール × scope)。Yahoo は約款 第 10 条の確認まで false (D-32 = b) ───
create table core.mall_order_policy (
  company_id      smallint not null references core.companies,
  mall            text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari','other')),
  scope_key       text not null,
  orders_enabled  boolean not null default false,
  note            text,
  updated_at      timestamptz not null default now(),
  primary key (company_id, mall, scope_key)
);
create trigger trg_mall_order_policy_touch before update on core.mall_order_policy for each row execute function core.touch_updated_at();
insert into core.mall_order_policy (company_id, mall, scope_key, orders_enabled, note) values
  (1, 'amazon',   'jp',   true,  null),
  (1, 'rakuten',  'main', true,  null),
  (1, 'yahoo',    'main', false, 'D-32: 出店 API 約款 第 10 条の確認まで入れない (集計値も確認対象)'),
  (1, 'aupay',    'main', true,  null),
  (1, 'qoo10',    'main', true,  null),
  (1, 'linegift', 'main', true,  null),
  (1, 'mercari',  'main', true,  null),
  (1, 'other',    'main', false, 'ヤフオク・ラクマ・卸・MSS・d ショッピングは当面入れない');

-- 世代だけ進める更新 (received_batch_seq / last_ingest_run_id) では updated_at を動かさない (§4.7 ②「他の列と updated_at は変えない」)
create or replace function core.touch_updated_at_unless_seq_only() returns trigger language plpgsql as $$
begin
  if (to_jsonb(new) - 'received_batch_seq' - 'last_ingest_run_id' - 'updated_at') is distinct from (to_jsonb(old) - 'received_batch_seq' - 'last_ingest_run_id' - 'updated_at') then
    new.updated_at := now();
  else
    new.updated_at := old.updated_at;
  end if;
  return new;
end
$$;

-- ─── 注文 = モールの注文 1 件 ───
create table core.orders (
  order_id           bigint generated always as identity primary key,
  company_id         smallint not null references core.companies,
  mall               text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari','other')),
  scope_key          text not null,
  mall_order_no      text not null,
  source_system      text not null check (source_system in ('mall_api','ne')),
  shop_code          text,
  ordered_at         timestamptz not null,
  order_date_jst     date not null,
  status             text not null check (status in ('new','confirmed','ready','shipped','delivered','cancelled','on_hold','returned','unknown')),
  status_source      text,
  is_cancelled       boolean not null default false,
  cancelled_at       timestamptz,
  shipped_at_source  timestamptz,
  total_amount_jpy   bigint check (total_amount_jpy >= 0),   -- 顧客が払った額
  items_amount_jpy   bigint check (items_amount_jpy >= 0),   -- 商品代 (税込)
  shipping_fee_jpy   bigint check (shipping_fee_jpy >= 0),
  shop_coupon_jpy    bigint check (shop_coupon_jpy >= 0),    -- 店負担の値引
  mall_coupon_jpy    bigint check (mall_coupon_jpy >= 0),    -- モール負担の値引
  points_used_jpy    bigint check (points_used_jpy >= 0),
  currency           text not null default 'JPY' check (currency = 'JPY'),
  amount_source      text not null default 'unknown' check (amount_source in ('mall_api','ne','unknown')),
  received_batch_seq bigint not null,
  source_updated_at  timestamptz not null,
  transform_version  text not null,
  content_hash       text not null,                    -- ヘッダの指紋
  lines_checksum     text not null default '',         -- 明細集合の指紋 (内容が同じかの判定)
  first_ingest_run_id text references ops.ingest_runs,
  last_ingest_run_id  text references ops.ingest_runs,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, mall, scope_key, mall_order_no),
  unique (company_id, order_id),
  constraint ck_orders_cancelled check (not is_cancelled or status = 'cancelled')
);
create index ix_orders_date on core.orders (company_id, order_date_jst desc);
create index ix_orders_open on core.orders (company_id, status) where status not in ('shipped','delivered','cancelled');
create trigger trg_orders_touch before update on core.orders for each row execute function core.touch_updated_at_unless_seq_only();

create table core.order_lines (
  order_line_id    bigint generated always as identity primary key,
  company_id       smallint not null references core.companies,
  order_id         bigint not null references core.orders on delete cascade,
  line_key         text not null,
  listing_id       bigint,
  sku_id           bigint,
  unresolved_code  text,                               -- listing / sku のどちらにも当たらなかった元のコード
  qty              integer not null check (qty >= 0),
  cancelled_qty    integer not null default 0 check (cancelled_qty >= 0 and cancelled_qty <= qty),
  unit_price_jpy   bigint check (unit_price_jpy >= 0),
  line_amount_jpy  bigint check (line_amount_jpy >= 0),
  tax_rate         numeric(4,2) check (tax_rate in (0.08, 0.10)),
  amount_source    text not null default 'unknown' check (amount_source in ('mall_api','ne','unknown')),
  source_line_ref  text,
  received_batch_seq bigint not null,                   -- 注文と同じ世代の明細集合だけが残る
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (order_id, line_key),
  constraint ck_order_lines_resolved check (sku_id is not null or listing_id is not null or unresolved_code is not null),
  foreign key (company_id, order_id) references core.orders (company_id, order_id),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_order_lines_sku on core.order_lines (sku_id) where sku_id is not null;
create index ix_order_lines_listing on core.order_lines (listing_id) where listing_id is not null;
create index ix_order_lines_unresolved on core.order_lines (company_id, unresolved_code) where unresolved_code is not null;
create trigger trg_order_lines_touch before update on core.order_lines for each row execute function core.touch_updated_at_unless_seq_only();

-- ─── 出荷 = NE の伝票 1 件 ───
create table core.shipments (
  shipment_id          bigint generated always as identity primary key,
  company_id           smallint not null references core.companies,
  ne_slip_no           text not null,
  order_id             bigint,                          -- NE 受注番号 → orders。未着なら null (夜間に結び直す)
  ne_order_no          text,
  shop_code            text,                           -- NE 店舗コード (ne_shops)
  status               text not null check (status in ('new','confirmed','ready','shipped','delivered','cancelled','on_hold','returned','unknown')),
  is_cancelled         boolean not null default false,
  ne_status_code       text,
  cancelled_at         timestamptz,
  shipped_at           timestamptz,
  ship_date_jst        date,                           -- 出荷確定日 (JST)
  order_date_jst       date,                           -- NE の受注日
  carrier              text,
  delivery_method_code text,
  delivery_method_name text,
  tracking_no          text,
  tracking_source      text,
  batch_code           text,
  synced_to_ne_at      timestamptz,
  received_batch_seq   bigint not null,
  source_updated_at    timestamptz not null,
  transform_version    text not null,
  content_hash         text not null,
  lines_checksum       text not null default '',
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, ne_slip_no),
  unique (company_id, shipment_id),
  foreign key (company_id, order_id) references core.orders (company_id, order_id),
  foreign key (company_id, shop_code) references core.ne_shops (company_id, shop_code),
  constraint ck_shipments_cancelled check (not is_cancelled or status = 'cancelled'),
  constraint ck_shipments_ship_date check ((shipped_at is null) = (ship_date_jst is null))
);
create index ix_shipments_order on core.shipments (order_id) where order_id is not null;
create index ix_shipments_date on core.shipments (company_id, ship_date_jst desc);
create index ix_shipments_unlinked on core.shipments (company_id, shop_code, ne_order_no) where order_id is null and ne_order_no is not null;
create trigger trg_shipments_touch before update on core.shipments for each row execute function core.touch_updated_at_unless_seq_only();

create table core.shipment_lines (
  shipment_line_id bigint generated always as identity primary key,
  company_id       smallint not null references core.companies,
  shipment_id      bigint not null references core.shipments on delete cascade,
  line_no          text not null,
  sku_id           bigint,
  unresolved_code  text,
  qty              integer not null check (qty >= 0),
  allocated_qty    integer check (allocated_qty >= 0),
  is_cancelled     boolean not null default false,
  received_batch_seq bigint not null,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (shipment_id, line_no),
  unique (company_id, shipment_line_id),
  constraint ck_shipment_lines_resolved check (sku_id is not null or unresolved_code is not null),
  foreign key (company_id, shipment_id) references core.shipments (company_id, shipment_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_shipment_lines_sku on core.shipment_lines (sku_id) where sku_id is not null;
create trigger trg_shipment_lines_touch before update on core.shipment_lines for each row execute function core.touch_updated_at_unless_seq_only();

-- exact の在庫イベント (ピッキング・梱包) を出荷明細に結ぶ (会社一致)
alter table events.inventory_events add column shipment_line_id bigint;
alter table events.inventory_events add constraint fk_inventory_events_shipment_line foreign key (company_id, shipment_line_id) references core.shipment_lines (company_id, shipment_line_id);
create index ix_inventory_events_shipment_line on events.inventory_events (shipment_line_id) where shipment_line_id is not null;

-- ─── 内部 ID の解決 (会社 × モール (× scope) で 1 件に当たるときだけ) ───
create or replace function core.resolve_listing_id(p_company_id smallint, p_mall text, p_code text) returns bigint language sql stable as $$
  select min(l.listing_id) from core.listings l
   where l.company_id = p_company_id and l.mall = p_mall and p_code is not null and l.listing_norm = core.norm_code(p_code)
  having count(*) = 1;
$$;
create or replace function core.resolve_sku_id(p_company_id smallint, p_code text) returns bigint language sql stable as $$
  select s.sku_id from core.skus s where s.company_id = p_company_id and p_code is not null and s.code_norm = core.norm_code(p_code);
$$;

-- ─── §4.7 の契約: 注文 (ヘッダ + 明細集合) を 1 世代ぶん適用。戻り値 = 'applied' / 'same' / 'stale' ───
--   p_header の鍵 = source_system (mall_api | ne), shop_code, ordered_at, order_date_jst (無ければ ordered_at の JST 日付), status (正規化済み) か status_source (source_system で map),
--                   is_cancelled, cancelled_at, shipped_at_source, total_amount_jpy, items_amount_jpy, shipping_fee_jpy, shop_coupon_jpy, mall_coupon_jpy, points_used_jpy, amount_source,
--                   source_updated_at, transform_version, content_hash, lines_checksum, ingest_run_id
--   p_lines の要素 = line_key, listing_code, sku_code, qty, cancelled_qty, unit_price_jpy, line_amount_jpy, tax_rate, amount_source, source_line_ref
--   🚨 mall_order_policy で orders_enabled でなければ例外 (Yahoo = D-32)。currency が JPY 以外なら例外
create or replace function core.apply_order_batch(
  p_company_id smallint, p_mall text, p_scope_key text, p_mall_order_no text, p_batch_seq bigint, p_header jsonb, p_lines jsonb
) returns text language plpgsql as $$
declare
  rec core.orders%rowtype;
  v_enabled boolean;
  v_status text;
  v_hash text := p_header ->> 'content_hash';
  v_lines_ck text := coalesce(p_header ->> 'lines_checksum', '');
  v_ordered_at timestamptz := (p_header ->> 'ordered_at')::timestamptz;
  n_rows integer; n_ins integer;
begin
  if p_header is null or jsonb_typeof(p_header) <> 'object' then raise exception 'p_header must be a json object'; end if;
  if p_lines is null or jsonb_typeof(p_lines) <> 'array' then raise exception 'p_lines must be a json array'; end if;
  if p_batch_seq is null or p_batch_seq <= 0 then raise exception 'p_batch_seq must be positive'; end if;
  if v_hash is null or v_hash = '' then raise exception 'p_header.content_hash is required'; end if;
  if v_ordered_at is null then raise exception 'p_header.ordered_at is required'; end if;
  if (p_header ? 'currency') and p_header ->> 'currency' <> 'JPY' then raise exception 'non-JPY currency is not accepted'; end if;
  select orders_enabled into v_enabled from core.mall_order_policy where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key;
  if v_enabled is null then raise exception 'mall_order_policy has no row for % / % / % (decide before ingesting)', p_company_id, p_mall, p_scope_key; end if;
  if not v_enabled then raise exception 'orders are not enabled for % / % / % (D-32 etc.)', p_company_id, p_mall, p_scope_key; end if;
  n_rows := jsonb_array_length(p_lines);
  v_status := coalesce(p_header ->> 'status', core.map_order_status(p_header ->> 'source_system', p_header ->> 'status_source'));
  if coalesce((p_header ->> 'is_cancelled')::boolean, false) then v_status := 'cancelled'; end if;

  select * into rec from core.orders
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no for update;
  if found then
    if p_batch_seq < rec.received_batch_seq then return 'stale'; end if;
    if rec.content_hash = v_hash and rec.lines_checksum = v_lines_ck then
      if p_batch_seq > rec.received_batch_seq then
        update core.orders set received_batch_seq = p_batch_seq, last_ingest_run_id = coalesce(p_header ->> 'ingest_run_id', last_ingest_run_id) where order_id = rec.order_id;
        update core.order_lines set received_batch_seq = p_batch_seq where order_id = rec.order_id;
      end if;
      return 'same';
    end if;
    if p_batch_seq = rec.received_batch_seq then
      raise exception 'batch % for order % was already applied with different content', p_batch_seq, p_mall_order_no;
    end if;
    update core.orders set
      source_system = coalesce(p_header ->> 'source_system', source_system), shop_code = coalesce(p_header ->> 'shop_code', shop_code),
      ordered_at = v_ordered_at, order_date_jst = coalesce((p_header ->> 'order_date_jst')::date, (v_ordered_at at time zone 'Asia/Tokyo')::date),
      status = v_status, status_source = p_header ->> 'status_source',
      is_cancelled = coalesce((p_header ->> 'is_cancelled')::boolean, false), cancelled_at = (p_header ->> 'cancelled_at')::timestamptz, shipped_at_source = (p_header ->> 'shipped_at_source')::timestamptz,
      total_amount_jpy = (p_header ->> 'total_amount_jpy')::bigint, items_amount_jpy = (p_header ->> 'items_amount_jpy')::bigint, shipping_fee_jpy = (p_header ->> 'shipping_fee_jpy')::bigint,
      shop_coupon_jpy = (p_header ->> 'shop_coupon_jpy')::bigint, mall_coupon_jpy = (p_header ->> 'mall_coupon_jpy')::bigint, points_used_jpy = (p_header ->> 'points_used_jpy')::bigint,
      amount_source = coalesce(p_header ->> 'amount_source', 'unknown'),
      received_batch_seq = p_batch_seq, source_updated_at = (p_header ->> 'source_updated_at')::timestamptz, transform_version = p_header ->> 'transform_version',
      content_hash = v_hash, lines_checksum = v_lines_ck, last_ingest_run_id = coalesce(p_header ->> 'ingest_run_id', last_ingest_run_id)
    where order_id = rec.order_id;
    delete from core.order_lines where order_id = rec.order_id;
  else
    insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, shop_code, ordered_at, order_date_jst, status, status_source, is_cancelled, cancelled_at, shipped_at_source,
      total_amount_jpy, items_amount_jpy, shipping_fee_jpy, shop_coupon_jpy, mall_coupon_jpy, points_used_jpy, amount_source,
      received_batch_seq, source_updated_at, transform_version, content_hash, lines_checksum, first_ingest_run_id, last_ingest_run_id)
    values (p_company_id, p_mall, p_scope_key, p_mall_order_no, p_header ->> 'source_system', p_header ->> 'shop_code', v_ordered_at,
      coalesce((p_header ->> 'order_date_jst')::date, (v_ordered_at at time zone 'Asia/Tokyo')::date), v_status, p_header ->> 'status_source',
      coalesce((p_header ->> 'is_cancelled')::boolean, false), (p_header ->> 'cancelled_at')::timestamptz, (p_header ->> 'shipped_at_source')::timestamptz,
      (p_header ->> 'total_amount_jpy')::bigint, (p_header ->> 'items_amount_jpy')::bigint, (p_header ->> 'shipping_fee_jpy')::bigint,
      (p_header ->> 'shop_coupon_jpy')::bigint, (p_header ->> 'mall_coupon_jpy')::bigint, (p_header ->> 'points_used_jpy')::bigint, coalesce(p_header ->> 'amount_source', 'unknown'),
      p_batch_seq, (p_header ->> 'source_updated_at')::timestamptz, p_header ->> 'transform_version', v_hash, v_lines_ck, p_header ->> 'ingest_run_id', p_header ->> 'ingest_run_id')
    returning * into rec;
  end if;
  -- 明細集合 (内部 ID は解決できたものだけ。どれにも当たらなければ unresolved_code に元のコード)
  insert into core.order_lines (company_id, order_id, line_key, listing_id, sku_id, unresolved_code, qty, cancelled_qty, unit_price_jpy, line_amount_jpy, tax_rate, amount_source, source_line_ref, received_batch_seq)
  select p_company_id, rec.order_id, r ->> 'line_key', x.listing_id, x.sku_id,
         case when x.listing_id is null and x.sku_id is null then coalesce(nullif(r ->> 'sku_code', ''), nullif(r ->> 'listing_code', ''), '?') end,
         coalesce((r ->> 'qty')::integer, 0), coalesce((r ->> 'cancelled_qty')::integer, 0), (r ->> 'unit_price_jpy')::bigint, (r ->> 'line_amount_jpy')::bigint,
         (r ->> 'tax_rate')::numeric, coalesce(r ->> 'amount_source', 'unknown'), r ->> 'source_line_ref', p_batch_seq
    from jsonb_array_elements(p_lines) r
    cross join lateral (select core.resolve_listing_id(p_company_id, p_mall, r ->> 'listing_code') as listing_id, core.resolve_sku_id(p_company_id, r ->> 'sku_code') as sku_id) x;
  get diagnostics n_ins = row_count;
  if n_ins <> n_rows then raise exception 'inserted % lines but % were given', n_ins, n_rows; end if;
  return 'applied';
end
$$;

-- 伝票 → 注文 (ne_shops の接頭辞つき)。見つかれば order_id を入れて true
create or replace function core.link_shipment_order(p_shipment_id bigint) returns boolean language plpgsql as $$
declare v_order_id bigint;
begin
  select o.order_id into v_order_id
    from core.shipments s
    join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code and n.mall is not null
    join core.orders o on o.company_id = s.company_id and o.mall = n.mall and o.scope_key = n.scope_key and o.mall_order_no = n.order_no_prefix || s.ne_order_no
   where s.shipment_id = p_shipment_id and s.ne_order_no is not null;
  if v_order_id is null then return false; end if;
  update core.shipments set order_id = v_order_id where shipment_id = p_shipment_id and order_id is distinct from v_order_id;
  return true;
end
$$;
-- 未着だった注文が届いた後の結び直し (夜間)。戻り値 = 結べた伝票の数
create or replace function core.relink_shipments(p_company_id smallint) returns integer language plpgsql as $$
declare n integer := 0; r record;
begin
  for r in select shipment_id from core.shipments where company_id = p_company_id and order_id is null and ne_order_no is not null and shop_code is not null loop
    if core.link_shipment_order(r.shipment_id) then n := n + 1; end if;
  end loop;
  return n;
end
$$;

-- ─── §4.7 の契約: 伝票 (ヘッダ + 明細集合) を 1 世代ぶん適用。戻り値 = 'applied' / 'same' / 'stale' ───
--   p_header の鍵 = ne_order_no, shop_code, ne_status_code (→ status は map 'ne'), is_cancelled, cancelled_at, shipped_at (→ ship_date_jst は JST 日付), order_date_jst,
--                   carrier, delivery_method_code, delivery_method_name, tracking_no, tracking_source, batch_code, synced_to_ne_at, source_updated_at, transform_version, content_hash, lines_checksum
--   p_lines の要素 = line_no, sku_code, qty, allocated_qty, is_cancelled
create or replace function core.apply_shipment_batch(
  p_company_id smallint, p_ne_slip_no text, p_batch_seq bigint, p_header jsonb, p_lines jsonb
) returns text language plpgsql as $$
declare
  rec core.shipments%rowtype;
  v_status text;
  v_hash text := p_header ->> 'content_hash';
  v_lines_ck text := coalesce(p_header ->> 'lines_checksum', '');
  v_shipped_at timestamptz := (p_header ->> 'shipped_at')::timestamptz;
  n_rows integer; n_ins integer;
begin
  if p_header is null or jsonb_typeof(p_header) <> 'object' then raise exception 'p_header must be a json object'; end if;
  if p_lines is null or jsonb_typeof(p_lines) <> 'array' then raise exception 'p_lines must be a json array'; end if;
  if p_batch_seq is null or p_batch_seq <= 0 then raise exception 'p_batch_seq must be positive'; end if;
  if v_hash is null or v_hash = '' then raise exception 'p_header.content_hash is required'; end if;
  n_rows := jsonb_array_length(p_lines);
  v_status := core.map_order_status('ne', p_header ->> 'ne_status_code');
  if coalesce((p_header ->> 'is_cancelled')::boolean, false) then v_status := 'cancelled'; end if;

  select * into rec from core.shipments where company_id = p_company_id and ne_slip_no = p_ne_slip_no for update;
  if found then
    if p_batch_seq < rec.received_batch_seq then return 'stale'; end if;
    if rec.content_hash = v_hash and rec.lines_checksum = v_lines_ck then
      if p_batch_seq > rec.received_batch_seq then
        update core.shipments set received_batch_seq = p_batch_seq where shipment_id = rec.shipment_id;
        update core.shipment_lines set received_batch_seq = p_batch_seq where shipment_id = rec.shipment_id;
      end if;
      perform core.link_shipment_order(rec.shipment_id);
      return 'same';
    end if;
    if p_batch_seq = rec.received_batch_seq then
      raise exception 'batch % for slip % was already applied with different content', p_batch_seq, p_ne_slip_no;
    end if;
    update core.shipments set
      ne_order_no = p_header ->> 'ne_order_no', shop_code = p_header ->> 'shop_code', status = v_status, ne_status_code = p_header ->> 'ne_status_code',
      is_cancelled = coalesce((p_header ->> 'is_cancelled')::boolean, false), cancelled_at = (p_header ->> 'cancelled_at')::timestamptz,
      shipped_at = v_shipped_at, ship_date_jst = (v_shipped_at at time zone 'Asia/Tokyo')::date, order_date_jst = (p_header ->> 'order_date_jst')::date,
      carrier = p_header ->> 'carrier', delivery_method_code = p_header ->> 'delivery_method_code', delivery_method_name = p_header ->> 'delivery_method_name',
      tracking_no = p_header ->> 'tracking_no', tracking_source = p_header ->> 'tracking_source', batch_code = p_header ->> 'batch_code', synced_to_ne_at = (p_header ->> 'synced_to_ne_at')::timestamptz,
      received_batch_seq = p_batch_seq, source_updated_at = (p_header ->> 'source_updated_at')::timestamptz, transform_version = p_header ->> 'transform_version',
      content_hash = v_hash, lines_checksum = v_lines_ck
    where shipment_id = rec.shipment_id;
    delete from core.shipment_lines where shipment_id = rec.shipment_id;
  else
    insert into core.shipments (company_id, ne_slip_no, ne_order_no, shop_code, status, ne_status_code, is_cancelled, cancelled_at, shipped_at, ship_date_jst, order_date_jst,
      carrier, delivery_method_code, delivery_method_name, tracking_no, tracking_source, batch_code, synced_to_ne_at,
      received_batch_seq, source_updated_at, transform_version, content_hash, lines_checksum)
    values (p_company_id, p_ne_slip_no, p_header ->> 'ne_order_no', p_header ->> 'shop_code', v_status, p_header ->> 'ne_status_code',
      coalesce((p_header ->> 'is_cancelled')::boolean, false), (p_header ->> 'cancelled_at')::timestamptz, v_shipped_at, (v_shipped_at at time zone 'Asia/Tokyo')::date, (p_header ->> 'order_date_jst')::date,
      p_header ->> 'carrier', p_header ->> 'delivery_method_code', p_header ->> 'delivery_method_name', p_header ->> 'tracking_no', p_header ->> 'tracking_source', p_header ->> 'batch_code', (p_header ->> 'synced_to_ne_at')::timestamptz,
      p_batch_seq, (p_header ->> 'source_updated_at')::timestamptz, p_header ->> 'transform_version', v_hash, v_lines_ck)
    returning * into rec;
  end if;
  insert into core.shipment_lines (company_id, shipment_id, line_no, sku_id, unresolved_code, qty, allocated_qty, is_cancelled, received_batch_seq)
  select p_company_id, rec.shipment_id, r ->> 'line_no', x.sku_id, case when x.sku_id is null then coalesce(nullif(r ->> 'sku_code', ''), '?') end,
         coalesce((r ->> 'qty')::integer, 0), (r ->> 'allocated_qty')::integer, coalesce((r ->> 'is_cancelled')::boolean, false), p_batch_seq
    from jsonb_array_elements(p_lines) r
    cross join lateral (select core.resolve_sku_id(p_company_id, r ->> 'sku_code') as sku_id) x;
  get diagnostics n_ins = row_count;
  if n_ins <> n_rows then raise exception 'inserted % lines but % were given', n_ins, n_rows; end if;
  perform core.link_shipment_order(rec.shipment_id);
  return 'applied';
end
$$;

-- ─── 出荷の日次 (既存 f_shipments_daily と同じ式。apps/warehouse/rebuild-shipments-daily.js) ───
--   slips = 出荷確定日のある伝票の数 (取消を含む) / cancelled_slips = その内数 / delivery_id = 配送方法ID ('' if null) /
--   delivery_name = その配送方法ID で出荷確定日が一番新しい伝票の名称 (同着は伝票番号の大きい方。名前が無ければ '(未設定)')
create or replace view mart.v_shipments_daily as
with names as (
  select distinct on (s.company_id, coalesce(s.delivery_method_code, ''))
         s.company_id, coalesce(s.delivery_method_code, '') as delivery_id, coalesce(nullif(s.delivery_method_name, ''), '(未設定)') as delivery_name
    from core.shipments s
   where s.ship_date_jst is not null
   order by s.company_id, coalesce(s.delivery_method_code, ''), s.shipped_at desc, s.ne_slip_no desc
)
select s.company_id, s.ship_date_jst as ship_date, coalesce(s.shop_code, '') as shop_code, coalesce(s.delivery_method_code, '') as delivery_id,
       coalesce(max(n.delivery_name), '(未設定)') as delivery_name,
       count(*)::integer as slips,
       (count(*) filter (where s.is_cancelled))::integer as cancelled_slips
  from core.shipments s
  left join names n on n.company_id = s.company_id and n.delivery_id = coalesce(s.delivery_method_code, '')
 where s.ship_date_jst is not null
 group by s.company_id, s.ship_date_jst, coalesce(s.shop_code, ''), coalesce(s.delivery_method_code, '');

-- 結ばれていない伝票 (注文が未着 / 店舗が対象外 / 番号の形が違う) = DQ の材料
create or replace view mart.v_shipments_unlinked as
select s.company_id, s.ne_slip_no, s.shop_code, n.mall, s.ne_order_no, s.ship_date_jst, s.status,
       case when s.shop_code is null then 'no_shop' when n.mall is null then 'shop_not_linked' when s.ne_order_no is null then 'no_order_no' else 'order_missing' end as reason
  from core.shipments s
  left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code
 where s.order_id is null;
