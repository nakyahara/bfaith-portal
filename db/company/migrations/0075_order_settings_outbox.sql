-- 0075: 新商品の登録の「発注の設定の知らせ」(outbox) (2026-10-13。AI_reference CompanyDB構想/21 v4 の C1・中原さんの決定 1 = a / 2 = b)
-- 🚨 前提 = 0052 (ops.master_edit_requests の sku_create・ops.js_stable_sha256)・0067 (ops.variation_batches・ops.variation_sub_request_id)・0071 (登録をやめた = cancelled)
--
-- なぜ: 新商品の登録は ① Company DB に登録 → ② 発注アプリ (Render の SQLite の po_*) に発注の設定を書く の 2 段 (apps/master-edit/router.mjs)。
--   Postgres と SQLite は 1 つの取引にできない = ② だけ失敗すると「商品は登録できたのに発注の設定だけ消える」(画面に「商品の画面で入れて」と出すだけ)。
--   → ① と同じ Postgres の取引に「この発注の設定を発注アプリへ届ける」知らせを積み、届くまで何度でも届け直す (product-hub のカードの知らせ 0052 と同じ作り)。
--   正本は発注アプリのまま (D-44・決定 1 = a)。Company DB は「届けた / まだ」を持つだけ (発注の設定の値の正本にはしない)。
-- なにを:
--   1. ops.order_settings_outbox = 知らせ (SKU に 1 つ)。鍵 = registration_request_id (登録の ID) と delivery_request_id (届けの ID = 発注アプリの
--      po_order_settings_requests の request_id と同じ = 単品は登録の ID・まとまりの子は ops.variation_sub_request_id(登録の ID, 'order:' || 正規化した SKU)
--      = 今の router.mjs の作り)。中身 (schema・payload・hash・作った人・時刻) は変えない・消さない。
--      状態: pending (まだ) → done (発注アプリに書けた / 前に書いてあった) / failed (失敗 = 自動で 5 回まで・夜間の段はその後も毎晩) /
--            conflict (発注アプリにほかの画面がもうこの商品の行を作っていた = 上書きしない・人が確かめる) / dismissed (人が理由をつけて閉じた)
--   2. ops.enqueue_order_settings(登録の ID, 人, 中身の配列) = 知らせを積む (security definer)。🚨 登録と同じ取引の中だけ:
--        単品 = ops.master_edit_requests の sku_create・done・同じ人・**この取引で書いた行** (xmin) の SKU だけ (1 件)
--        まとまり = ops.variation_batches の closed・同じ人・**この取引で開いた** (txid) まとめての登録の子だけ (子の sku_create もこの取引)
--      = 画面のロールが後から別の取引で知らせを作れない (知らせの表の insert は渡さない)。hash は DB が作る (ops.js_stable_sha256 = 画面・届け手の stable と同じ形)
--   3. ops.claim_order_settings_events = 届ける知らせを借りる (期限つきの借り・for update skip locked)。mode = auto (登録の直後・画面を開いたとき) /
--      sweep (夜間ロードの後ろの段 = 失敗の回数の上限なし) / manual (人の「もう一度」= conflict も)
--   4. ops.finish_order_settings_event = 結果を書く (借りた人だけ・済んだ知らせは変えない)
--   5. ops.dismiss_order_settings_event = 人が閉じる (理由が要る・failed / conflict だけ・商品の画面で発注の設定を入れ直した後など)
--   6. ops.v_order_settings_outbox_open = 届いていない知らせの状態ごとの数・いちばん古い時刻・24 時間を超えた数 (画面の上の件数・見張り)
-- 🚨 この migration は今のデータを何も変えない (新しい表は空)。登録の関数 (ops.register_new_sku・ops.variation_batch_close) は置き換えない
--    (知らせは呼び手 = lib/master-register.mjs・lib/master-variation.mjs が同じ取引の中で 2. を呼ぶ。取引の外で呼ぶと 2. が断る)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す (0034 の約束)。
--    画面のロール (master_edit) への権限は scripts/company-db/create-master-edit-roles.mjs の流し直しで (表の select と 2〜5 の実行)

-- 1. 知らせ
create table ops.order_settings_outbox (
  event_id                 uuid primary key default gen_random_uuid(),
  company_id               smallint not null default 1 references core.companies,
  sku_id                   bigint not null,
  code                     text not null check (length(code) between 1 and 255),
  registration_kind        text not null check (registration_kind in ('single', 'variation')),
  registration_request_id  uuid not null,
  delivery_request_id      uuid not null unique,
  schema_version           text not null check (schema_version ~ '^po-settings-v[0-9]+$'),
  payload                  jsonb not null check (jsonb_typeof(payload) = 'object'),
  payload_hash             text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  created_by               text not null check (length(created_by) between 1 and 320),
  created_at               timestamptz not null default now(),
  status                   text not null default 'pending' check (status in ('pending', 'failed', 'conflict', 'done', 'dismissed')),
  attempts                 integer not null default 0 check (attempts >= 0),
  last_error               text check (last_error is null or length(last_error) <= 2000),
  lease_owner              text,
  leased_until             timestamptz,
  result                   jsonb check (result is null or jsonb_typeof(result) = 'object'),
  closed_at                timestamptz,
  closed_by                text check (closed_by is null or length(closed_by) between 1 and 320),
  close_reason             text check (close_reason is null or length(close_reason) between 1 and 500),
  updated_at               timestamptz not null default now(),
  unique (sku_id),
  constraint ck_oso_closed check ((status in ('done', 'dismissed')) = (closed_at is not null)),
  constraint ck_oso_dismissed check ((status = 'dismissed') = (close_reason is not null and closed_by is not null)),
  constraint ck_oso_lease check ((lease_owner is null) = (leased_until is null)),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_order_settings_outbox_open on ops.order_settings_outbox (created_at) where status in ('pending', 'failed', 'conflict');
create index ix_order_settings_outbox_reg on ops.order_settings_outbox (registration_request_id);
comment on table ops.order_settings_outbox is '新商品の登録の発注の設定を発注アプリ (SQLite の po_*) へ届ける知らせ (0075・CompanyDB構想 21 C1)。登録と同じ取引で書く。中身は変えない・消さない。正本は発注アプリ';

create function ops.guard_order_settings_outbox() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'order_settings_outbox は消さない' using errcode = 'P0001'; end if;
  if (new.event_id, new.company_id, new.sku_id, new.code, new.registration_kind, new.registration_request_id, new.delivery_request_id, new.schema_version,
      new.payload, new.payload_hash, new.created_by, new.created_at)
     is distinct from (old.event_id, old.company_id, old.sku_id, old.code, old.registration_kind, old.registration_request_id, old.delivery_request_id, old.schema_version,
      old.payload, old.payload_hash, old.created_by, old.created_at) then
    raise exception 'order_settings_outbox の知らせの中身 (ID・SKU・中身・hash・作った人) は変えない' using errcode = 'P0001';
  end if;
  if old.status in ('done', 'dismissed') then raise exception '閉じた知らせ (done / dismissed) は変えない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_order_settings_outbox_guard before update or delete on ops.order_settings_outbox
  for each row execute function ops.guard_order_settings_outbox();
create trigger trg_order_settings_outbox_no_truncate before truncate on ops.order_settings_outbox
  for each statement execute function core.reject_mutation();

-- 中身の形 (表を読まない)。問題が無ければ null・あれば最初の 1 つの名前。🚨 lib/order-settings-outbox.mjs の orderPayloadShapeProblem と同じ決まり
--   { schema: 'po-settings-v1', code, supplier_code: 文字 | null, patch: { 列: 文字 | null }, new_condition: {...} | null, new_material: {...} | null }
--   値は全部 文字か null (数も文字 = hash の stable が整数しか扱わないため・発注アプリの部品が数に直して確かめる)
create function ops.order_settings_payload_problem(p jsonb) returns text language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  k text;
  v jsonb;
begin
  if p is null or pg_catalog.jsonb_typeof(p) <> 'object' then return 'payload_not_object'; end if;
  if (select pg_catalog.array_agg(x order by x collate "C") from pg_catalog.jsonb_object_keys(p) x) is distinct from array['code', 'new_condition', 'new_material', 'patch', 'schema', 'supplier_code'] then
    return 'payload_keys';
  end if;
  if p ->> 'schema' is distinct from 'po-settings-v1' then return 'schema'; end if;
  if pg_catalog.jsonb_typeof(p -> 'code') <> 'string' or pg_catalog.length(p ->> 'code') not between 1 and 255 then return 'code'; end if;
  if pg_catalog.jsonb_typeof(p -> 'supplier_code') not in ('string', 'null') then return 'supplier_code'; end if;
  if pg_catalog.jsonb_typeof(p -> 'patch') <> 'object' then return 'patch'; end if;
  for k, v in select e.key, e.value from pg_catalog.jsonb_each(p -> 'patch') e loop
    if k not in ('order_lot', 'condition_id', 'material_group_id', 'capacity_per_unit', 'case_group', 'case_lot') then return 'patch_key'; end if;
    if pg_catalog.jsonb_typeof(v) not in ('string', 'null') then return 'patch_value'; end if;
  end loop;
  if pg_catalog.jsonb_typeof(p -> 'new_condition') = 'object' then
    if (select pg_catalog.array_agg(x order by x collate "C") from pg_catalog.jsonb_object_keys(p -> 'new_condition') x)
       is distinct from array['condition_id', 'condition_type', 'condition_value', 'display_name', 'maker_name', 'unit'] then return 'new_condition_keys'; end if;
    if exists (select 1 from pg_catalog.jsonb_each(p -> 'new_condition') e where pg_catalog.jsonb_typeof(e.value) not in ('string', 'null')) then return 'new_condition_value'; end if;
  elsif pg_catalog.jsonb_typeof(p -> 'new_condition') <> 'null' then return 'new_condition';
  end if;
  if pg_catalog.jsonb_typeof(p -> 'new_material') = 'object' then
    if (select pg_catalog.array_agg(x order by x collate "C") from pg_catalog.jsonb_object_keys(p -> 'new_material') x)
       is distinct from array['group_id', 'min_order_qty', 'name', 'unit'] then return 'new_material_keys'; end if;
    if exists (select 1 from pg_catalog.jsonb_each(p -> 'new_material') e where pg_catalog.jsonb_typeof(e.value) not in ('string', 'null')) then return 'new_material_value'; end if;
  elsif pg_catalog.jsonb_typeof(p -> 'new_material') <> 'null' then return 'new_material';
  end if;
  return null;
end $$;
revoke all on function ops.order_settings_payload_problem(jsonb) from public;

-- 2. 知らせを積む (登録と同じ取引の中だけ)。p_items = [{ code, payload }] (payload.code = code)。戻り値 = [{ event_id, code, sku_id, delivery_request_id }]
create function ops.enqueue_order_settings(p_registration_request_id uuid, p_actor_id text, p_items jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_req    record;
  v_batch  ops.variation_batches;
  v_kind   text;
  v_item   jsonb;
  v_code   text;
  v_norm   text;
  v_prob   text;
  v_sku    record;
  v_del    uuid;
  v_id     uuid;
  v_out    jsonb := '[]'::jsonb;
  v_seen   text[] := '{}';
begin
  if p_registration_request_id is null then raise exception 'invalid_input: 登録の ID (request_id) が要る' using errcode = '22023'; end if;
  if p_actor_id is null or pg_catalog.length(p_actor_id) not between 1 and 320 then raise exception 'invalid_input: 人 (actor) が要る' using errcode = '22023'; end if;
  if p_items is null or pg_catalog.jsonb_typeof(p_items) <> 'array' or pg_catalog.jsonb_array_length(p_items) not between 1 and 120 then
    raise exception 'invalid_input: 知らせは 1〜120 件の配列' using errcode = '22023';
  end if;
  -- 登録 = この取引で書いた単品の登録 (sku_create・done) か、この取引で開いて閉じたまとめての登録
  select m.request_id, m.operation, m.status, m.actor_id, m.sku_id, m.xmin = pg_catalog.pg_current_xact_id()::xid as fresh
    into v_req from ops.master_edit_requests m where m.request_id = p_registration_request_id;
  if found and v_req.operation = 'sku_create' then
    if v_req.status <> 'done' or not v_req.fresh or v_req.actor_id is distinct from p_actor_id then
      raise exception 'not_in_registration: 発注の設定の知らせは、同じ人の登録 (sku_create) と同じ取引の中だけで積む' using errcode = '42501';
    end if;
    v_kind := 'single';
    if pg_catalog.jsonb_array_length(p_items) <> 1 then raise exception 'invalid_input: 単品の登録の知らせは 1 件' using errcode = '22023'; end if;
  else
    select * into v_batch from ops.variation_batches b where b.request_id = p_registration_request_id;
    if not found or v_batch.status <> 'closed' or v_batch.txid <> pg_catalog.txid_current() or v_batch.actor_id is distinct from p_actor_id then
      raise exception 'not_in_registration: 発注の設定の知らせは、同じ人の登録 (単品 / まとめての登録) と同じ取引の中だけで積む' using errcode = '42501';
    end if;
    v_kind := 'variation';
  end if;
  for v_item in select x from pg_catalog.jsonb_array_elements(p_items) x loop
    if pg_catalog.jsonb_typeof(v_item) <> 'object' or pg_catalog.jsonb_typeof(v_item -> 'code') <> 'string' then
      raise exception 'invalid_input: 知らせの 1 件 = { code, payload }' using errcode = '22023';
    end if;
    v_code := v_item ->> 'code';
    v_norm := core.norm_code(v_code);
    if v_norm = any (v_seen) then raise exception 'invalid_input: 同じ商品 % の知らせが 2 つ', v_code using errcode = '22023'; end if;
    v_seen := v_seen || v_norm;
    v_prob := ops.order_settings_payload_problem(v_item -> 'payload');
    if v_prob is not null then raise exception 'invalid_input: 発注の設定の知らせの形が違う (%・%)', v_prob, v_code using errcode = '22023'; end if;
    if (v_item -> 'payload' ->> 'code') is distinct from v_code then raise exception 'invalid_input: 知らせの中身のコードが % と違う', v_code using errcode = '22023'; end if;
    select s.sku_id, s.code, s.sku_kind into v_sku from core.skus s where s.company_id = 1 and s.code_norm = v_norm;
    if not found or v_sku.sku_kind <> 'single' or v_sku.code is distinct from v_code then
      raise exception 'not_in_registration: % はこの登録の単品でない', v_code using errcode = '42501';
    end if;
    if v_kind = 'single' then
      if v_sku.sku_id is distinct from v_req.sku_id then raise exception 'not_in_registration: % はこの登録の商品でない', v_code using errcode = '42501'; end if;
      v_del := p_registration_request_id;
    else
      if not exists (select 1 from pg_catalog.unnest(v_batch.child_codes) c where core.norm_code(c) = v_norm) or not exists (
           select 1 from ops.master_edit_requests m where m.request_id = ops.variation_sub_request_id(p_registration_request_id, 'child:' || v_norm)
              and m.operation = 'sku_create' and m.status = 'done' and m.sku_id = v_sku.sku_id and m.xmin = pg_catalog.pg_current_xact_id()::xid) then
        raise exception 'not_in_registration: % はこのまとめての登録でこの取引に登録した子でない', v_code using errcode = '42501';
      end if;
      v_del := ops.variation_sub_request_id(p_registration_request_id, 'order:' || v_norm);
    end if;
    insert into ops.order_settings_outbox (company_id, sku_id, code, registration_kind, registration_request_id, delivery_request_id, schema_version, payload, payload_hash, created_by)
      values (1, v_sku.sku_id, v_code, v_kind, p_registration_request_id, v_del, v_item -> 'payload' ->> 'schema', v_item -> 'payload',
              ops.js_stable_sha256(v_item -> 'payload'), p_actor_id)
    returning event_id into v_id;
    v_out := v_out || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('event_id', v_id::text, 'code', v_code, 'sku_id', v_sku.sku_id::text, 'delivery_request_id', v_del::text));
  end loop;
  return v_out;
end $$;
revoke all on function ops.enqueue_order_settings(uuid, text, jsonb) from public;

-- 3. 届ける知らせを借りる。p_mode = auto (まだ・失敗で回数の上限の前) / sweep (まだ・失敗 = 回数を問わない = 夜間ロードの後ろの段) / manual (人の「もう一度」= 衝突も)。
--    登録の ID・知らせの ID で絞れる。借りた行を返す (試した回数を 1 増やす)。登録の状態 (やめた = cancelled なら届け手が書かずに閉じる) も返す。
--    p_exclude = この回にもう借りた知らせ (夜間の段が何周しても同じ失敗の行に塞がれない・Codex #1703 R1 M1)。
--    順 = まだ (pending) が先 → 試した回数の少ない順 → 古い順 (いつも失敗する古い行が新しい知らせの前を塞がない)
create function ops.claim_order_settings_events(p_owner text, p_mode text, p_registration_request_id uuid default null, p_event_id uuid default null,
                                                p_limit integer default 20, p_lease_seconds integer default 120, p_max_auto integer default 5,
                                                p_exclude uuid[] default null)
  returns table (event_id uuid, sku_id bigint, code text, registration_kind text, registration_request_id uuid, delivery_request_id uuid, schema_version text,
                 payload jsonb, payload_hash text, created_by text, attempts integer, status text, reg_state text)
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_owner is null or p_owner !~ '^[A-Za-z0-9_.:-]{1,100}$' then raise exception 'invalid_input: 借りる人 (owner) の形が違う' using errcode = '22023'; end if;
  if p_mode is null or p_mode not in ('auto', 'sweep', 'manual') then raise exception 'invalid_input: mode は auto / sweep / manual' using errcode = '22023'; end if;
  return query
  with c as (
    select o.event_id from ops.order_settings_outbox o
     where (case p_mode when 'auto' then o.status = 'pending' or (o.status = 'failed' and o.attempts < greatest(1, p_max_auto))
                        when 'sweep' then o.status in ('pending', 'failed')
                        else o.status in ('pending', 'failed', 'conflict') end)
       and (o.leased_until is null or o.leased_until < pg_catalog.now())
       and (p_registration_request_id is null or o.registration_request_id = p_registration_request_id)
       and (p_event_id is null or o.event_id = p_event_id)
       and (p_exclude is null or not (o.event_id = any (p_exclude)))
     order by (o.status <> 'pending'), o.attempts, o.created_at, o.event_id limit greatest(1, least(200, coalesce(p_limit, 20))) for update skip locked),
  u as (
    update ops.order_settings_outbox o set lease_owner = p_owner,
           leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => greatest(10, least(3600, coalesce(p_lease_seconds, 120)))),
           attempts = o.attempts + 1, updated_at = pg_catalog.now()
      from c where o.event_id = c.event_id
    returning o.event_id, o.sku_id, o.code, o.registration_kind, o.registration_request_id, o.delivery_request_id, o.schema_version, o.payload, o.payload_hash,
              o.created_by, o.attempts, o.status, o.created_at)
  select u.event_id, u.sku_id, u.code, u.registration_kind, u.registration_request_id, u.delivery_request_id, u.schema_version, u.payload, u.payload_hash,
         u.created_by, u.attempts, u.status, r.state
    from u left join ops.master_registrations r on r.sku_id = u.sku_id
   order by u.created_at, u.event_id;
end $$;
revoke all on function ops.claim_order_settings_events(text, text, uuid, uuid, integer, integer, integer, uuid[]) from public;

-- 4. 借りた知らせの結果を書く (借りた人だけ・閉じた知らせは変えない)。p_status = done / failed / conflict。書けたら true (借りが切れてほかが取った = false)
create function ops.finish_order_settings_event(p_event_id uuid, p_owner text, p_status text, p_result jsonb, p_error text) returns boolean
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n integer;
begin
  if p_status is null or p_status not in ('done', 'failed', 'conflict') then raise exception 'invalid_input: status は done / failed / conflict' using errcode = '22023'; end if;
  if p_result is not null and pg_catalog.jsonb_typeof(p_result) <> 'object' then raise exception 'invalid_input: result は object' using errcode = '22023'; end if;
  update ops.order_settings_outbox set status = p_status, result = p_result, last_error = case when p_status = 'done' then null else pg_catalog.left(p_error, 2000) end,
         closed_at = case when p_status = 'done' then pg_catalog.now() end, lease_owner = null, leased_until = null, updated_at = pg_catalog.now()
   where event_id = p_event_id and lease_owner = p_owner and status not in ('done', 'dismissed');
  get diagnostics v_n = row_count;
  return v_n = 1;
end $$;
revoke all on function ops.finish_order_settings_event(uuid, text, text, jsonb, text) from public;

-- 5. 人が閉じる (failed / conflict だけ・借りの最中は閉じない・理由 1〜500 字)。閉じたら true。もう閉じていた・借りの最中 = false
create function ops.dismiss_order_settings_event(p_event_id uuid, p_actor_id text, p_reason text) returns boolean
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n integer;
  v_reason text := pg_catalog.btrim(coalesce(p_reason, ''));
begin
  if p_actor_id is null or pg_catalog.length(p_actor_id) not between 1 and 320 then raise exception 'invalid_input: 人 (actor) が要る' using errcode = '22023'; end if;
  if pg_catalog.length(v_reason) not between 1 and 500 or v_reason ~ '[[:cntrl:]]' then raise exception 'invalid_input: 閉じる理由 (1〜500 字・改行なし) が要る' using errcode = '22023'; end if;
  update ops.order_settings_outbox set status = 'dismissed', closed_at = pg_catalog.now(), closed_by = p_actor_id, close_reason = v_reason,
         lease_owner = null, leased_until = null, updated_at = pg_catalog.now()
   where event_id = p_event_id and status in ('failed', 'conflict') and (leased_until is null or leased_until < pg_catalog.now());
  get diagnostics v_n = row_count;
  return v_n = 1;
end $$;
revoke all on function ops.dismiss_order_settings_event(uuid, text, text) from public;

-- 6. 届いていない知らせ (画面の上の件数・見張り・夜間の段の 24 時間の知らせ)
create view ops.v_order_settings_outbox_open as
  select o.status, count(*)::int as n, min(o.created_at) as oldest_at, max(o.attempts) as max_attempts,
         count(*) filter (where o.created_at < now() - interval '24 hours')::int as overdue
    from ops.order_settings_outbox o where o.status in ('pending', 'failed', 'conflict') group by o.status;
comment on view ops.v_order_settings_outbox_open is '発注の設定がまだ発注アプリに届いていない知らせ (0075)。状態ごとの数・いちばん古い時刻・24 時間を超えた数';

-- 見張りは読むだけ。画面のロールの権限は scripts/company-db/create-master-edit-roles.mjs (migration の後に流し直す)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.order_settings_outbox, ops.v_order_settings_outbox_open to watcher';
  end if;
end $$;
