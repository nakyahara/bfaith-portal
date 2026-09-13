-- 0010 土台 (Phase 3〜7 の共通部品。08 §7)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』(Codex 3 巡・原文は同 _raw/08_Codexレビュー_20260913.md)
--   raw.payload_allowed / raw.ensure_source(src, scalar_keys, array_keys) = 「入れてよい列の一覧」を CHECK で強制する raw の 2 表型 (0004 の型に許可リストを足したもの)
--   raw.purge_superseded_observations = 保持期間の整理 (鍵ごとの最新の有効な状態は消さない)
--   snapshots.ensure_month_partitions_for = 表名の配列を受ける月パーティション (既存の ensure_month_partitions はこれを呼ぶ。振る舞いは同じ)
--   core.ensure_location = ロジザードのロケーションを自動で足す (code = ブロック略称-ロケ)
--   external_ids / ingest_runs の CHECK 拡張、locations / workers の (company_id, id) unique、既存 events の親子の会社一致 (複合 FK)
-- 🚨 0004 の 13 ソースは触らない。

create or replace function raw.payload_allowed(p jsonb, p_scalar_keys text[], p_array_keys text[]) returns boolean language sql immutable as $$
  select jsonb_typeof(p) = 'object'
     and coalesce((
       select bool_and(
         case
           when e.key = any(p_scalar_keys) then jsonb_typeof(e.value) in ('string','number','boolean','null')
           when e.key = any(p_array_keys) then jsonb_typeof(e.value) = 'array'
             and coalesce((
               select bool_and(jsonb_typeof(x) = 'object'
                 and coalesce((select bool_and(i.key = any(p_scalar_keys) and jsonb_typeof(i.value) in ('string','number','boolean','null')) from jsonb_each(x) i), true))
                 from jsonb_array_elements(e.value) x), true)
           else false
         end)
         from jsonb_each(p) e), true);
$$;

-- #5 観測の scope_key は run の scope_key と一致させる (複合 FK の参照先)
create unique index ux_ingest_runs_id_scope on ops.ingest_runs (ingest_run_id, scope_key);

create or replace function raw.ensure_source(p_src text, p_scalar_keys text[] default null, p_array_keys text[] default '{}') returns void language plpgsql as $$
begin
  if p_src !~ '^[a-z][a-z0-9_]*$' then raise exception 'raw.ensure_source: bad source name %', p_src; end if;
  if to_regclass(format('raw.%I_contents', p_src)) is null then
    execute format($f$
      create table raw.%1$I_contents (
        content_hash   text primary key,
        payload        jsonb not null,
        payload_bytes  integer,
        first_seen_at  timestamptz not null default now()
      )$f$, p_src);
    perform core.make_append_only('raw', p_src || '_contents');
  end if;
  if p_scalar_keys is not null then
    execute format('alter table raw.%1$I_contents drop constraint if exists ck_%1$s_payload', p_src);
    execute format('alter table raw.%1$I_contents add constraint ck_%1$s_payload check (raw.payload_allowed(payload, %2$L::text[], %3$L::text[]))', p_src, p_scalar_keys, p_array_keys);
  end if;
  if to_regclass(format('raw.%I_observations', p_src)) is null then
    execute format($f$
      create table raw.%1$I_observations (
        observation_id bigint generated always as identity primary key,
        ingest_run_id  text not null references ops.ingest_runs,
        scope_key      text not null,
        business_key   text not null,
        content_hash   text references raw.%1$I_contents,
        fetch_status   text not null check (fetch_status in ('ok','not_found','error','skipped')),
        observed_at    timestamptz not null,
        created_at     timestamptz not null default now(),
        unique (ingest_run_id, scope_key, business_key),
        constraint ck_%1$s_obs_content check ((fetch_status = 'ok') = (content_hash is not null)),
        foreign key (ingest_run_id, scope_key) references ops.ingest_runs (ingest_run_id, scope_key)
      )$f$, p_src);
    execute format('create index ix_%1$s_obs_key_time on raw.%1$I_observations (scope_key, business_key, observed_at desc, observation_id desc)', p_src);
    execute format('create index ix_%1$s_obs_content on raw.%1$I_observations (content_hash)', p_src);   -- #10 contents の削除時の FK 確認
    perform core.make_append_only('raw', p_src || '_observations');
  end if;
end
$$;

-- 7.4 保持期間の整理 (#1 #10)。
--   消すのは「keep_days より古く、同じ (scope_key, business_key) に **完走した取得の状態観測 (ok / not_found) がもっと新しくある**」観測だけ。
--   → 鍵ごとの最新の有効な状態 (= 現在庫 view が読む行) は残る。失敗した run の観測や error/skipped は置き換えの根拠にしない。
--   ロックは取込と同じ順 (contents → observations)。append-only trigger (core.reject_mutation) だけを外し、元の tgenabled に戻す。
--   保守トランザクションは短く (この関数 1 回 = 1 トランザクション)。戻り値 = 消した観測数
create or replace function raw.purge_superseded_observations(p_src text, p_keep_days integer) returns integer language plpgsql as $$
declare
  n integer;
  t record;
  rels text[] := '{}';
  names text[] := '{}';
  states text[] := '{}';
  i integer;
begin
  if p_src !~ '^[a-z][a-z0-9_]*$' then raise exception 'bad source name %', p_src; end if;
  execute format('lock table raw.%I_contents, raw.%I_observations in share row exclusive mode', p_src, p_src);
  for t in
    select c.relname, tg.tgname, tg.tgenabled
      from pg_trigger tg join pg_class c on c.oid = tg.tgrelid join pg_namespace ns on ns.oid = c.relnamespace
     where ns.nspname = 'raw' and c.relname in (p_src || '_contents', p_src || '_observations')
       and tg.tgfoid = 'core.reject_mutation'::regproc and not tg.tgisinternal
  loop
    rels := rels || t.relname::text; names := names || t.tgname::text; states := states || t.tgenabled::text;
    execute format('alter table only raw.%I disable trigger %I', t.relname, t.tgname);
  end loop;
  execute format($f$
    delete from raw.%1$I_observations o
     where o.observed_at < now() - make_interval(days => $1)
       and exists (select 1 from raw.%1$I_observations n join ops.ingest_runs r on r.ingest_run_id = n.ingest_run_id
                    where n.scope_key = o.scope_key and n.business_key = o.business_key
                      and n.fetch_status in ('ok','not_found') and r.status = 'success' and r.complete
                      and (n.observed_at, n.observation_id) > (o.observed_at, o.observation_id))$f$, p_src) using p_keep_days;
  get diagnostics n = row_count;
  execute format('delete from raw.%1$I_contents c where not exists (select 1 from raw.%1$I_observations o where o.content_hash = c.content_hash)', p_src);
  for i in 1 .. coalesce(array_length(names, 1), 0) loop
    execute format('alter table only raw.%I enable %s trigger %I', rels[i],
                   case states[i] when 'A' then 'always' when 'R' then 'replica' else '' end, names[i]);
  end loop;
  return n;
end
$$;

create or replace function snapshots.ensure_month_partitions_for(p_tables text[], p_from date, p_to date) returns integer language plpgsql as $$
declare
  t text;
  m date := date_trunc('month', p_from)::date;
  m_next date;
  created integer := 0;
  part text;
  col text;
begin
  while m <= p_to loop
    m_next := (m + interval '1 month')::date;
    foreach t in array p_tables loop
      part := format('%s_%s', t, to_char(m, 'YYYYMM'));
      if to_regclass(format('snapshots.%I', part)) is null then
        select a.attname into col
          from pg_partitioned_table p join pg_attribute a on a.attrelid = p.partrelid and a.attnum = p.partattrs[0]
         where p.partrelid = format('snapshots.%I', t)::regclass;
        execute format('create table snapshots.%I (like snapshots.%I including all)', part, t);
        execute format('with moved as (delete from snapshots.%I where %I >= %L and %I < %L returning *) insert into snapshots.%I select * from moved',
                       t || '_default', col, m, col, m_next, part);
        execute format('alter table snapshots.%I attach partition snapshots.%I for values from (%L) to (%L)', t, part, m, m_next);
        created := created + 1;
      end if;
    end loop;
    m := m_next;
  end loop;
  return created;
end
$$;
create or replace function snapshots.ensure_month_partitions(p_from date, p_to date) returns integer language sql as $$
  select snapshots.ensure_month_partitions_for(array['listing_daily', 'catalog_asin_daily', 'catalog_asin_rank_daily'], p_from, p_to);
$$;

-- #4 会社一致の複合 FK の参照先
create unique index ux_locations_company_id on core.locations (company_id, location_id);
create unique index ux_workers_company_id on core.workers (company_id, worker_id);

-- 3.2 core.locations の自動追加。code = ブロック略称-ロケ。既存行の会社が違えば例外 (#4)
create or replace function core.ensure_location(p_company_id smallint, p_warehouse_id smallint, p_block text, p_loke text) returns bigint language plpgsql as $$
declare
  v_id bigint;
  v_company smallint;
  v_code text := coalesce(nullif(p_block, ''), '-') || '-' || coalesce(nullif(p_loke, ''), '-');
begin
  select location_id, company_id into v_id, v_company from core.locations where warehouse_id = p_warehouse_id and code = v_code;
  if v_id is not null then
    if v_company <> p_company_id then raise exception 'location % belongs to company %, not %', v_code, v_company, p_company_id; end if;
    return v_id;
  end if;
  insert into core.locations (company_id, warehouse_id, code, block, building, created_by_type, created_by_id)
  values (p_company_id, p_warehouse_id, v_code, p_block, case when p_block ~ '^R' then 'iroha' else 'main' end, 'system', 'logizard_inventory')
  on conflict (warehouse_id, code) do nothing
  returning location_id into v_id;
  if v_id is null then
    select location_id, company_id into v_id, v_company from core.locations where warehouse_id = p_warehouse_id and code = v_code;
    if v_company <> p_company_id then raise exception 'location % belongs to company %, not %', v_code, v_company, p_company_id; end if;
  end if;
  return v_id;
end
$$;

alter table core.external_ids drop constraint external_ids_entity_type_check;
alter table core.external_ids add constraint external_ids_entity_type_check
  check (entity_type in ('product','sku','listing','catalog_item','supplier','worker','location','document','order','shipment','purchase_order'));

alter table ops.ingest_runs drop constraint ingest_runs_status_check;
alter table ops.ingest_runs add constraint ingest_runs_status_check check (status in ('running','success','failed','partial','skipped'));

-- #4 既存の events にも親子の会社一致 (表は空)
alter table events.inventory_events add constraint fk_inventory_events_company_sku foreign key (company_id, sku_id) references core.skus (company_id, sku_id);
alter table events.inventory_events add constraint fk_inventory_events_company_location foreign key (company_id, location_id) references core.locations (company_id, location_id);
alter table events.work_events add constraint fk_work_events_company_worker foreign key (company_id, worker_id) references core.workers (company_id, worker_id);
alter table events.work_events add constraint fk_work_events_company_sku foreign key (company_id, sku_id) references core.skus (company_id, sku_id);
alter table events.work_events add constraint fk_work_events_company_location foreign key (company_id, location_id) references core.locations (company_id, location_id);

