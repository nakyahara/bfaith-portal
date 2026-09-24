-- 0026: マスタの変更の記録と版番号 (2026-09-24。Company DB構想 10 §5.2 / PR ②c-1。Codex 設計レビュー ②c を反映)
--
-- なぜ: 商品・仕入先マスタの正本を Company DB に移す (10。D-39)。ポータルの編集画面 (PR ⑤) の前に、
--   「誰が・いつ・何を・何から何へ変えたか」と「編集中に他の人・夜間ロードが変えていないか (楽観ロック)」を DB の中で持つ。
-- なにを:
--   1. events.master_change_events (append-only): 対象の表の行が変わるたびに、トリガーが同じ取引の中で書く (本体の更新が巻き戻れば記録も消える)
--      - UPDATE = 変わった列ごとに 1 行 (attribute / old_value / new_value)。INSERT / DELETE = 行全体を 1 行 (attribute = null)
--      - 「行が無い」は SQL の null、「値が空」は json の null で区別する (INSERT の old_value = null / UPDATE で空にした new_value = 'null'::jsonb)
--      - change_id = 1 回の行の変更を束ねる (同じ UPDATE で変わった列どうし)。entity_key = 主キー (複合キーも) を DB 側で組み立てる。entity_id は主キーが 1 列の表だけ
--      - 管理用の列 (updated_at・version・created_*・resolved_by_*・resolved_at・生成列 *_norm・first/last_seen_at) は比べない。それ以外の列は全部 = 列を足しても記録し忘れない
--      - 誰が: 取引ごとの set_config('core.actor_type' / 'core.actor_id' / 'core.source_system' / 'core.run_id' / 'core.request_id' / 'core.reason', …, true)。
--        is_local = true なので取引を出れば消える (接続を使い回しても次の取引に漏れない)。無ければ actor_type = 'system'・source_system = 'sql'。db_user = current_user を必ず残す
--      - 冪等キーは持たない: 1 行 = DB で実際に起きた 1 回の変更。API の再送の制御 (同じ保存を 2 回しない) はポータルの側で request_id を使って行う (PR ⑤)
--   2. version (products / skus / suppliers / supplier_skus / listings): 比べる列が実際に変わったときだけ +1 (値が同じ UPDATE・入力の version は信じない)。
--      夜間ロードの変更でも上がる = 編集中に夜間ロードが同じ行を変えたら、人の保存は 409 になる
--   3. 子の表の変更で親の version も上げる: セット構成 (sku_components) と原価 (sku_costs) → SKU、出品の構成 (listing_components) → 出品。
--      構成を一括で保存する画面で「他の人が構成品を足した」を見落とさないため (子の行ごとの version では新規追加を検知できない。Codex ②c High)
--   4. events.sku_attribute_events (0005。書き手なし) は使わない印だけ付ける (drop しない)
-- 保持: 当面は全件を DB に残す (値が同じ行は夜間ロードが UPDATE しないので、ふだんの晩は変わった分だけ増える)。
--   🚨 見直す条件 = 1,000 万行 または 2 GB を超えたら (その前に退避先・期間・復元方法を決める。append-only なので消すときは保守経路 = trigger を disable)

create table events.master_change_events (
  event_id      bigint generated always as identity primary key,
  company_id    smallint not null references core.companies,
  change_id     uuid not null,
  operation     text not null check (operation in ('INSERT','UPDATE','DELETE')),
  entity_type   text not null check (entity_type in ('product','sku','supplier','supplier_sku','sku_component','sku_cost','listing','listing_component')),
  entity_id     bigint,
  entity_key    jsonb not null,
  attribute     text,
  old_value     jsonb,
  new_value     jsonb,
  actor_type    text not null check (actor_type in ('human','ai','system','external')),
  actor_id      text,
  source_system text not null,
  run_id        text,
  request_id    text,
  reason_text   text,
  db_user       text not null default current_user,
  recorded_at   timestamptz not null default now(),
  constraint ck_master_change_shape check (
    (operation = 'UPDATE' and attribute is not null)
    or (operation = 'INSERT' and attribute is null and old_value is null and new_value is not null)
    or (operation = 'DELETE' and attribute is null and new_value is null and old_value is not null))
);
create index ix_master_change_events_entity on events.master_change_events (entity_type, entity_id, recorded_at desc);
create index ix_master_change_events_key on events.master_change_events using btree (entity_type, (entity_key::text), recorded_at desc);
create index ix_master_change_events_run on events.master_change_events (run_id) where run_id is not null;
create index ix_master_change_events_change on events.master_change_events (change_id);
select core.make_append_only('events', 'master_change_events');

comment on table events.sku_attribute_events is '非推奨 (2026-09-24 / 0026): 書き手なし。マスタの変更の記録は events.master_change_events を使う';

-- 比べない列 (管理用・生成列・観測の時刻)
create or replace function core.master_audit_ignored_columns() returns text[] language sql immutable as $$
  select array['updated_at','version','created_at','created_by_type','created_by_id','resolved_by_type','resolved_by_id','resolved_at',
               'code_norm','listing_norm','external_norm','first_seen_at','last_seen_at']
$$;

-- 1. 変更の記録 (AFTER 行トリガー)。tg_argv[0] = entity_type、tg_argv[1] = 主キーの列 (カンマ区切り)
create or replace function core.audit_master_change() returns trigger language plpgsql as $$
declare
  v_entity   text := tg_argv[0];
  v_keys     text[] := string_to_array(tg_argv[1], ',');
  v_ignored  text[] := core.master_audit_ignored_columns();
  v_old      jsonb;
  v_new      jsonb;
  v_row      jsonb;
  v_key      jsonb := '{}'::jsonb;
  v_id       bigint;
  v_change   uuid := gen_random_uuid();
  v_actor_t  text := coalesce(nullif(current_setting('core.actor_type', true), ''), 'system');
  v_actor_id text := nullif(current_setting('core.actor_id', true), '');
  v_source   text := coalesce(nullif(current_setting('core.source_system', true), ''), 'sql');
  v_run      text := nullif(current_setting('core.run_id', true), '');
  v_req      text := nullif(current_setting('core.request_id', true), '');
  v_reason   text := nullif(current_setting('core.reason', true), '');
  v_cols     text[];
  c          text;
  k          text;
begin
  if tg_op in ('UPDATE','DELETE') then v_old := to_jsonb(old); end if;
  if tg_op in ('INSERT','UPDATE') then v_new := to_jsonb(new); end if;
  v_row := coalesce(v_new, v_old);
  foreach k in array v_keys loop v_key := v_key || jsonb_build_object(k, v_row -> k); end loop;
  if array_length(v_keys, 1) = 1 then v_id := (v_row ->> v_keys[1])::bigint; end if;
  select array_agg(x order by x) into v_cols from jsonb_object_keys(v_row) as t(x) where not (x = any(v_ignored));

  if tg_op = 'UPDATE' then
    foreach c in array v_cols loop
      if (v_old -> c) is distinct from (v_new -> c) then
        insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, attribute, old_value, new_value,
                                                 actor_type, actor_id, source_system, run_id, request_id, reason_text)
        values ((v_row ->> 'company_id')::smallint, v_change, 'UPDATE', v_entity, v_id, v_key, c, v_old -> c, v_new -> c,
                v_actor_t, v_actor_id, v_source, v_run, v_req, v_reason);
      end if;
    end loop;
    return null;
  end if;

  insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, attribute, old_value, new_value,
                                           actor_type, actor_id, source_system, run_id, request_id, reason_text)
  values ((v_row ->> 'company_id')::smallint, v_change, tg_op, v_entity, v_id, v_key, null,
          case when tg_op = 'DELETE' then (select jsonb_object_agg(x, v_old -> x) from unnest(v_cols) as u(x)) end,
          case when tg_op = 'INSERT' then (select jsonb_object_agg(x, v_new -> x) from unnest(v_cols) as u(x)) end,
          v_actor_t, v_actor_id, v_source, v_run, v_req, v_reason);
  return null;
end $$;

-- 2. version (BEFORE UPDATE)。比べる列が変わったとき、または子の表が親を上げるとき (core.version_bump = 'on') だけ +1。入力の version は信じない
create or replace function core.bump_master_version() returns trigger language plpgsql as $$
declare
  v_ignored text[] := core.master_audit_ignored_columns();
  v_old     jsonb := to_jsonb(old);
  v_new     jsonb := to_jsonb(new);
  v_changed boolean;
begin
  select exists (select 1 from jsonb_object_keys(v_new) as t(x) where not (x = any(v_ignored)) and (v_old -> x) is distinct from (v_new -> x)) into v_changed;
  if v_changed or coalesce(current_setting('core.version_bump', true), '') = 'on' then
    new.version := old.version + 1;
  else
    new.version := old.version;
  end if;
  return new;
end $$;

-- 3. 子の表の変更で親の version を上げる (AFTER 行トリガー)。tg_argv[0] = 親の表、[1] = 親の主キー列、[2] = 子の側の列
create or replace function core.bump_parent_version() returns trigger language plpgsql as $$
declare
  v_parent text := tg_argv[0];
  v_pk     text := tg_argv[1];
  v_fk     text := tg_argv[2];
  v_ids    bigint[];
begin
  if tg_op in ('INSERT','UPDATE') then v_ids := array_append(v_ids, (to_jsonb(new) ->> v_fk)::bigint); end if;
  if tg_op in ('UPDATE','DELETE') then v_ids := array_append(v_ids, (to_jsonb(old) ->> v_fk)::bigint); end if;
  perform set_config('core.version_bump', 'on', true);
  execute format('update %s set version = version where %I = any($1)', v_parent, v_pk) using v_ids;
  perform set_config('core.version_bump', '', true);   -- この後の同じ取引の UPDATE に漏らさない
  return null;
end $$;

alter table core.products      add column version integer not null default 1 check (version > 0);
alter table core.skus          add column version integer not null default 1 check (version > 0);
alter table core.suppliers     add column version integer not null default 1 check (version > 0);
alter table core.supplier_skus add column version integer not null default 1 check (version > 0);
alter table core.listings      add column version integer not null default 1 check (version > 0);

create trigger trg_products_version      before update on core.products      for each row execute function core.bump_master_version();
create trigger trg_skus_version          before update on core.skus          for each row execute function core.bump_master_version();
create trigger trg_suppliers_version     before update on core.suppliers     for each row execute function core.bump_master_version();
create trigger trg_supplier_skus_version before update on core.supplier_skus for each row execute function core.bump_master_version();
create trigger trg_listings_version      before update on core.listings      for each row execute function core.bump_master_version();

create trigger trg_products_audit          after insert or update or delete on core.products          for each row execute function core.audit_master_change('product', 'product_id');
create trigger trg_skus_audit              after insert or update or delete on core.skus              for each row execute function core.audit_master_change('sku', 'sku_id');
create trigger trg_suppliers_audit         after insert or update or delete on core.suppliers         for each row execute function core.audit_master_change('supplier', 'supplier_id');
create trigger trg_supplier_skus_audit     after insert or update or delete on core.supplier_skus     for each row execute function core.audit_master_change('supplier_sku', 'supplier_id,sku_id');
create trigger trg_sku_components_audit    after insert or update or delete on core.sku_components    for each row execute function core.audit_master_change('sku_component', 'parent_sku_id,child_sku_id');
create trigger trg_sku_costs_audit         after insert or update or delete on core.sku_costs         for each row execute function core.audit_master_change('sku_cost', 'sku_cost_id');
create trigger trg_listings_audit          after insert or update or delete on core.listings          for each row execute function core.audit_master_change('listing', 'listing_id');
create trigger trg_listing_components_audit after insert or update or delete on core.listing_components for each row execute function core.audit_master_change('listing_component', 'listing_id,sku_id');

create trigger trg_sku_components_bump_parent    after insert or update or delete on core.sku_components    for each row execute function core.bump_parent_version('core.skus', 'sku_id', 'parent_sku_id');
create trigger trg_sku_costs_bump_parent         after insert or update or delete on core.sku_costs         for each row execute function core.bump_parent_version('core.skus', 'sku_id', 'sku_id');
create trigger trg_listing_components_bump_parent after insert or update or delete on core.listing_components for each row execute function core.bump_parent_version('core.listings', 'listing_id', 'listing_id');
