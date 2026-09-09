-- 0005 snapshots (日次・append-only・月パーティション) と events (変化・append-only) (06 §5.4、03 §4)
--
-- snapshots: 「昨日と今日を比べる」材料。13 か月日次 + 以後は週次集約 (D-19)。日次原本は gz で外部保存。
-- events:    誰が・いつ・何を変えたか。取消は逆イベント (reverses_event_id)。idempotency_key unique (P-6)。
--            価格差分だけから「人が変えた」と断定しない: 差分由来は actor_type='external', actor_id は null。

-- ── snapshots ──
create table snapshots.listing_daily (
  snapshot_date    date not null,                       -- JST 業務日
  listing_id       bigint not null references core.listings,
  status           text not null,
  price_jpy        bigint check (price_jpy >= 0),
  sale_price_jpy   bigint check (sale_price_jpy >= 0),
  points_rate      numeric(5,2),
  stock_qty        integer,
  fulfillment      text,
  buybox_price_jpy bigint check (buybox_price_jpy >= 0),
  buybox_is_ours   boolean,
  offers_count     integer,
  lowest_price_jpy bigint check (lowest_price_jpy >= 0),
  rating           numeric(3,2),
  review_count     integer,
  complete         boolean not null,                    -- その日の取得が complete=true だったか
  source_run_id    text not null,
  observed_at      timestamptz not null,
  primary key (snapshot_date, listing_id)
) partition by range (snapshot_date);
create table snapshots.listing_daily_default partition of snapshots.listing_daily default;

create table snapshots.catalog_asin_daily (               -- ASIN 単位 (自社 product/listing の存在を必須にしない。D-21)
  snapshot_date    date not null,
  marketplace_id   text not null,
  asin             text not null,
  is_ours          boolean not null,                    -- 観測日時点で自社出品と関連あり
  buybox_price_jpy bigint check (buybox_price_jpy >= 0),
  offers_count     integer,
  rating           numeric(3,2),
  review_count     integer,
  source_run_id    text not null,
  observed_at      timestamptz not null,
  primary key (snapshot_date, marketplace_id, asin)
) partition by range (snapshot_date);
create table snapshots.catalog_asin_daily_default partition of snapshots.catalog_asin_daily default;

create table snapshots.catalog_asin_rank_daily (          -- 順位は種別・カテゴリ別 (違うカテゴリの順位を前日比較しない。D-18)
  snapshot_date    date not null,
  marketplace_id   text not null,
  asin             text not null,
  rank_type        text not null check (rank_type in ('classification','display_group')),
  category_id      text not null,
  category_title   text,
  rank             integer not null check (rank > 0),
  observed_at      timestamptz not null,
  primary key (snapshot_date, marketplace_id, asin, rank_type, category_id)
) partition by range (snapshot_date);
create table snapshots.catalog_asin_rank_daily_default partition of snapshots.catalog_asin_rank_daily default;

create table snapshots.listing_weekly (                   -- 13 か月より古い日次の集約 (週 1 日の抜き取りではない。D-19)
  week_start          date not null,                    -- 月曜
  listing_id          bigint not null references core.listings,
  status_at_week_end  text,
  price_at_week_end_jpy bigint check (price_at_week_end_jpy >= 0),
  price_min_jpy       bigint check (price_min_jpy >= 0),
  price_max_jpy       bigint check (price_max_jpy >= 0),
  hidden_days         integer not null default 0,
  stockout_days       integer not null default 0,
  observed_days       integer not null default 0,
  built_at            timestamptz not null default now(),
  primary key (week_start, listing_id)
);

-- 月パーティションを作る (取込側が対象月を渡す。無ければ default パーティションに入る = 落ちない)
create or replace function snapshots.ensure_month_partitions(p_from date, p_to date) returns integer language plpgsql as $$
declare
  t text;
  m date := date_trunc('month', p_from)::date;
  created integer := 0;
  part text;
begin
  while m <= p_to loop
    foreach t in array array['listing_daily', 'catalog_asin_daily', 'catalog_asin_rank_daily'] loop
      part := format('%s_%s', t, to_char(m, 'YYYYMM'));
      if to_regclass(format('snapshots.%I', part)) is null then
        execute format('create table snapshots.%I partition of snapshots.%I for values from (%L) to (%L)',
                       part, t, m, (m + interval '1 month')::date);
        created := created + 1;
      end if;
    end loop;
    m := (m + interval '1 month')::date;
  end loop;
  return created;
end
$$;

-- ── events (共通列を各表に同じ順で置く。03 §4) ──
create table events.price_change_events (
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),
  actor_id          text,
  source_system     text not null,                      -- 'price_update','amazon_pricing','snapshot_diff','ai_action',...
  source_ref        text,
  reason_code       text,
  reason_text       text,
  idempotency_key   text not null unique,
  reverses_event_id bigint references events.price_change_events,
  payload           jsonb,
  listing_id        bigint not null references core.listings,
  price_kind        text not null default 'standard' check (price_kind in ('standard','sale','points_rate','member')),
  old_price_jpy     bigint check (old_price_jpy >= 0),
  new_price_jpy     bigint not null check (new_price_jpy >= 0),
  decision_id       bigint
);
create index ix_price_change_events_listing on events.price_change_events (listing_id, occurred_at desc);

create table events.listing_change_events (               -- 価格以外の変化 (状態・在庫・文言・画像・分類)
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),
  actor_id          text,
  source_system     text not null,
  source_ref        text,
  reason_code       text,
  reason_text       text,
  idempotency_key   text not null unique,
  reverses_event_id bigint references events.listing_change_events,
  payload           jsonb,
  listing_id        bigint not null references core.listings,
  attribute         text not null,                      -- 'status','stock','title','image_main','category','points','sale_period'
  old_value         text,
  new_value         text,
  detected_by       text not null check (detected_by in ('snapshot_diff','tool','api_write','manual_entry')),
  decision_id       bigint
);
create index ix_listing_change_events_listing on events.listing_change_events (listing_id, occurred_at desc);

create table events.sku_attribute_events (                -- 税率・売上分類・取扱区分・原価などの属性変更の履歴
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),
  actor_id          text,
  source_system     text not null,
  source_ref        text,
  reason_code       text,
  reason_text       text,
  idempotency_key   text not null unique,
  reverses_event_id bigint references events.sku_attribute_events,
  payload           jsonb,
  sku_id            bigint not null references core.skus,
  attribute         text not null,
  old_value         text,
  new_value         text
);
create index ix_sku_attribute_events_sku on events.sku_attribute_events (sku_id, occurred_at desc);

create table events.inventory_events (                    -- 入出庫・調整 (Phase 3 で二重書きの受け皿。表だけ先に)
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),
  actor_id          text,
  source_system     text not null,                      -- 'picking','packing','inbound_check','logizard_diff',...
  source_ref        text,
  reason_code       text,                               -- 'receipt','shipment','fba_allocation','stocktake_adj','damage','found','return'
  reason_text       text,
  idempotency_key   text not null unique,
  reverses_event_id bigint references events.inventory_events,
  payload           jsonb,
  sku_id            bigint not null references core.skus,
  location_id       bigint references core.locations,
  qty_delta         integer not null,
  qty_after         integer,
  confidence        text not null default 'exact' check (confidence in ('exact','inferred'))
);
create index ix_inventory_events_sku_time on events.inventory_events (sku_id, occurred_at desc);

create table events.work_events (                         -- 倉庫作業 (Phase 3。表だけ先に)
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),
  actor_id          text,
  source_system     text not null,
  source_ref        text,
  reason_code       text,
  reason_text       text,
  idempotency_key   text not null unique,
  reverses_event_id bigint references events.work_events,
  payload           jsonb,
  worker_id         bigint references core.workers,
  task_type         text not null,                      -- 'pick','pack','inbound_check','stocking','fba_pick','giftset'
  sku_id            bigint references core.skus,
  location_id       bigint references core.locations,
  qty               integer,
  started_at        timestamptz,
  finished_at       timestamptz,
  device_id         text,
  outcome           text check (outcome in ('done','shortage','mistake','void'))
);
create index ix_work_events_worker_time on events.work_events (worker_id, occurred_at desc);
