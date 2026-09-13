-- 0011 在庫 (Phase 3。08 §3)
--
--   raw.logizard_inventory_*      = ロジザードの毎時写し (変わった行だけの観測。observed_at = 取得時刻)
--   snapshots.stock_capture_days  = 会社 × source × scope × 日 の「取れたか」(building → complete / partial / missing)。日次表は building の間だけ書ける
--   snapshots.warehouse_stock_daily (sku × ロケ × 日。90 日) / sku_stock_daily (sku × 日 × source。13 か月) / sku_stock_weekly (永久)
--   mart.v_warehouse_stock_current / mart.v_sku_stock = いまの在庫 (完走した取得だけを読む)
-- 取込の契約と保持期間は 08 §3.3 / §7.4。取込ジョブは次の PR (D2)。

select raw.ensure_source('logizard_inventory',
  array['商品ID','商品名','バーコード','ブロック略称','ロケ','品質区分名','有効期限','入荷日','在庫数','引当数','ロケ業務区分','商品予備項目004','最終入荷日','最終出荷日','ブロック引当順']);

-- 取得の日ごとの状態 = **会社 × source × scope × 日** (#3)。日次表は building の間だけ書け・変えられ、complete に上げた日だけ view が読む
create table snapshots.stock_capture_days (
  snapshot_date  date not null,
  source         text not null check (source in ('logizard','ne','fba_jp','fba_us')),
  scope_key      text not null,                          -- 取得元のアカウント (倉庫 / NE 店舗 / SP-API のアカウント)
  company_id     smallint not null references core.companies,
  status         text not null check (status in ('building','complete','partial','missing')),
  ingest_run_id  text references ops.ingest_runs,
  captured_at    timestamptz,
  rows           integer,
  built_at       timestamptz not null default now(),
  completed_at   timestamptz,
  primary key (snapshot_date, source, scope_key),
  unique (snapshot_date, source, scope_key, company_id, ingest_run_id),   -- 日次表の複合 FK の参照先 (会社を含む = 他社の capture に行を付けられない)
  constraint ck_stock_capture_days_run check ((status = 'missing') = (ingest_run_id is null)),
  constraint ck_stock_capture_days_completed check ((status = 'complete') = (completed_at is not null))
);
create index ix_stock_capture_days_latest on snapshots.stock_capture_days (company_id, source, scope_key, snapshot_date desc) where status = 'complete';

-- 日次表の INSERT / UPDATE / DELETE は、その (日, source, scope, run) が building のときだけ (#2)。
-- capture 行を for share でロックする = 完了処理 (update … set status='complete') と直列化。
-- 保守 (保持期間の削除・パーティション移送) は set local snapshots.maintenance = 'on' で通す (専用経路。取込では使わない)
create or replace function snapshots.check_capture_day_building() returns trigger language plpgsql as $$
declare
  rec record := coalesce(new, old);
  st text;
begin
  if current_setting('snapshots.maintenance', true) = 'on' then return coalesce(new, old); end if;
  select status into st from snapshots.stock_capture_days
   where snapshot_date = rec.snapshot_date and source = rec.source and scope_key = rec.scope_key and ingest_run_id = rec.ingest_run_id
   for share;
  if st is distinct from 'building' then
    raise exception 'stock_capture_days(%, %, %, %) is % (rows can only be changed while building)', rec.snapshot_date, rec.source, rec.scope_key, rec.ingest_run_id, coalesce(st, 'absent');
  end if;
  return coalesce(new, old);
end
$$;

create table snapshots.warehouse_stock_daily (
  snapshot_date    date not null,
  source           text not null default 'logizard' check (source = 'logizard'),
  scope_key        text not null,
  line_key         text not null,
  company_id       smallint not null references core.companies,
  sku_id           bigint,
  logizard_code    text not null,
  location_id      bigint,
  location_code    text not null,
  block_code       text,
  quality          text,
  expiry_date      date,
  received_date    date,
  qty              integer not null check (qty >= 0),
  allocated_qty    integer not null default 0 check (allocated_qty >= 0),
  captured_at      timestamptz not null,
  ingest_run_id    text not null references ops.ingest_runs,
  primary key (snapshot_date, scope_key, line_key),
  foreign key (snapshot_date, source, scope_key, company_id, ingest_run_id) references snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, ingest_run_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (company_id, location_id) references core.locations (company_id, location_id)
) partition by range (snapshot_date);
create table snapshots.warehouse_stock_daily_default partition of snapshots.warehouse_stock_daily default;
create index ix_warehouse_stock_daily_sku on snapshots.warehouse_stock_daily (sku_id, snapshot_date desc);
create trigger trg_warehouse_stock_daily_building before insert or update or delete on snapshots.warehouse_stock_daily for each row execute function snapshots.check_capture_day_building();

create table snapshots.sku_stock_daily (
  snapshot_date        date not null,
  source               text not null check (source in ('logizard','ne','fba_jp','fba_us')),
  scope_key            text not null,
  source_code          text not null,
  company_id           smallint not null references core.companies,
  sku_id               bigint,
  qty                  integer not null check (qty >= 0),
  allocated_qty        integer check (allocated_qty >= 0),
  fba_available        integer check (fba_available >= 0),
  fba_fc_transfer      integer check (fba_fc_transfer >= 0),
  fba_fc_processing    integer check (fba_fc_processing >= 0),
  fba_customer_order   integer check (fba_customer_order >= 0),
  fba_inbound_working  integer check (fba_inbound_working >= 0),
  fba_inbound_shipped  integer check (fba_inbound_shipped >= 0),
  fba_inbound_received integer check (fba_inbound_received >= 0),
  captured_at          timestamptz not null,
  ingest_run_id        text not null references ops.ingest_runs,
  primary key (snapshot_date, source, scope_key, source_code),
  constraint ck_sku_stock_fba_cols check (source like 'fba%' or (fba_available is null and fba_inbound_working is null)),
  foreign key (snapshot_date, source, scope_key, company_id, ingest_run_id) references snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, ingest_run_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
) partition by range (snapshot_date);
create table snapshots.sku_stock_daily_default partition of snapshots.sku_stock_daily default;
create index ix_sku_stock_daily_sku on snapshots.sku_stock_daily (sku_id, snapshot_date desc);
create trigger trg_sku_stock_daily_building before insert or update or delete on snapshots.sku_stock_daily for each row execute function snapshots.check_capture_day_building();

create table snapshots.sku_stock_weekly (
  week_start      date not null,
  source          text not null check (source in ('logizard','ne','fba_jp','fba_us')),
  company_id      smallint not null references core.companies,
  sku_id          bigint not null,
  qty_at_week_end integer,
  qty_min         integer, qty_max integer,
  stockout_days   integer not null default 0,
  observed_days   integer not null default 0,
  built_at        timestamptz not null default now(),
  primary key (week_start, source, sku_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);

-- いまの倉庫在庫: 完走した取得の観測だけを (scope_key, business_key) ごとに observed_at (= 取得時刻) の順で読む
create or replace view mart.v_warehouse_stock_current as
with runs as (
  select r.ingest_run_id from ops.ingest_runs r
   where r.source_system = 'logizard' and r.entity = 'inventory' and r.status = 'success' and r.complete
),
state as (
  select distinct on (o.scope_key, o.business_key) o.scope_key, o.business_key, o.fetch_status, o.content_hash, o.observed_at, o.ingest_run_id
    from raw.logizard_inventory_observations o join runs r on r.ingest_run_id = o.ingest_run_id
   order by o.scope_key, o.business_key, o.observed_at desc, o.observation_id desc
)
select s.scope_key, s.business_key as line_key,
       c.payload ->> '商品ID' as logizard_code,
       (c.payload ->> 'ブロック略称') || '-' || (c.payload ->> 'ロケ') as location_code,
       c.payload ->> 'ブロック略称' as block_code,
       c.payload ->> '品質区分名' as quality,
       (c.payload ->> '在庫数')::integer as qty,
       (c.payload ->> '引当数')::integer as allocated_qty,
       s.observed_at as captured_at,
       s.ingest_run_id
  from state s join raw.logizard_inventory_contents c on c.content_hash = s.content_hash
 where s.fetch_status = 'ok';

-- 1 SKU 1 行 (#3)。**会社 × source × scope ごと**に最新の complete の日で切り、その日の行を SKU に合算。
-- その会社にその source の complete な scope が 1 つも無ければ null (不明)。あれば、行の無い SKU は 0
create or replace view mart.v_sku_stock as
with latest_day as (
  select company_id, source, scope_key, max(snapshot_date) as snapshot_date
    from snapshots.stock_capture_days where status = 'complete'
   group by company_id, source, scope_key
),
agg as (
  select d.company_id, d.sku_id, d.source,
         min(l.snapshot_date) as as_of,                   -- scope が複数なら一番古い完走日 (それ以降は全 scope 揃っていない)
         sum(d.qty) as qty, sum(d.allocated_qty) as allocated_qty, sum(d.fba_available) as fba_available,
         sum(coalesce(d.fba_inbound_working, 0) + coalesce(d.fba_inbound_shipped, 0) + coalesce(d.fba_inbound_received, 0)) as fba_inbound
    from latest_day l
    join snapshots.sku_stock_daily d on d.company_id = l.company_id and d.source = l.source and d.scope_key = l.scope_key and d.snapshot_date = l.snapshot_date
   where d.sku_id is not null
   group by d.company_id, d.sku_id, d.source
),
has as (
  select company_id, source, min(snapshot_date) as as_of from latest_day group by company_id, source
)
select k.company_id, k.sku_id,
       hw.as_of as warehouse_as_of,
       case when hw.company_id is not null then coalesce(aw.qty, 0) end as warehouse_qty,
       case when hw.company_id is not null then coalesce(aw.allocated_qty, 0) end as warehouse_allocated_qty,
       hn.as_of as ne_as_of,
       case when hn.company_id is not null then coalesce(an.qty, 0) end as ne_qty,
       hf.as_of as fba_jp_as_of,
       case when hf.company_id is not null then coalesce(af.fba_available, 0) end as fba_jp_available,
       case when hf.company_id is not null then coalesce(af.fba_inbound, 0) end as fba_jp_inbound
  from core.skus k
  left join has hw on hw.company_id = k.company_id and hw.source = 'logizard'
  left join has hn on hn.company_id = k.company_id and hn.source = 'ne'
  left join has hf on hf.company_id = k.company_id and hf.source = 'fba_jp'
  left join agg aw on aw.company_id = k.company_id and aw.sku_id = k.sku_id and aw.source = 'logizard'
  left join agg an on an.company_id = k.company_id and an.sku_id = k.sku_id and an.source = 'ne'
  left join agg af on af.company_id = k.company_id and af.sku_id = k.sku_id and af.source = 'fba_jp';

