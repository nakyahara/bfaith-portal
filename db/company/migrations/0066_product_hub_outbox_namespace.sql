-- 0066: product-hub の知らせ (ops.product_hub_outbox) に名前空間 (entity_kind / entity_id / revision) を足す (2026-10-08。Company DB構想 20「代表の正本を自社DBへ」v7 §⑤・§⑩ の PR-3)
-- 🚨 前提 = 0052 (知らせの表と借りる / 結果を書く関数)・0064 (PR-1 大文字のコード)・0065 (PR-2 CSV の版)。番号はマージの順で振り直す (中身は 0064 / 0065 に依らない)
--
-- なぜ:
--   色違い・サイズ違いのまとまり (group product) の出品カードは、まとまりで 1 枚 (中原さんの答え 12 = a)。今の知らせの表は sku_id が必須・(sku_id, kind) で一意 =
--   まとまりの知らせを入れる所が無い。sku_id の欄に product_id を入れると、別の SKU のカードに結んでしまう (設計 R2 High 4)。
--   → 知らせに名前空間を足す: entity_kind (sku / variation_group)・entity_id (sku_id / group_product_id)・revision。一意 = (entity_kind, entity_id, kind, revision)。
-- なにを:
--   1. 列を足す:
--        group_product_id = まとまりの知らせだけ (core.products の番号・会社一致の外部キー)。sku_id = SKU の知らせだけ (null を許す = まとまりの知らせ)。どちらか一方だけ
--        entity_kind = 生成の列 (group_product_id があれば variation_group・無ければ sku)
--        entity_id   = 生成の列 (sku_id か group_product_id)
--        revision    = 既定 1。SKU の知らせは 1 だけ (カードは SKU ごとに 1 枚・今の (sku_id, kind) の一意と同じ意味)
--      🚨 前からの行は entity_kind = sku・entity_id = sku_id・revision = 1 になる (生成の列と既定値 = UPDATE をしない = 0052 の guard (done は変えない) に当たらない)。
--         同じ migration の終わりに数えて確かめる (違えば止める)
--   2. 種類 (kind) に group_snapshot、版 (schema_version) に ph-group-v1。SKU の知らせ = card_create / ph-card-vN だけ・まとまりの知らせ = group_snapshot / ph-group-vN だけ
--   3. 一意を (sku_id, kind) から (entity_kind, entity_id, kind, revision) に替える
--   4. 前からの insert と借りる関数は互換のまま (PR-4 = product-hub のまとまりのカード・PR-5 = まとまりの DB を配るまで今の単品のカードを止めない):
--        ・登録の関数 (ops.register_new_sku = 0061 の版) の insert (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by) は
--          そのまま通る (entity_* は生成・revision は既定 1)。登録の関数は変えない
--        ・ops.claim_card_events (product-hub の今の取り込み・lib/product-hub-outbox.mjs の runCardOutbox / linkCardToExisting) は SKU の知らせだけを借りる
--          (0052 の本文に「and o.entity_kind = 'sku'」を足しただけ・試験で本文の差を機械で確かめる) = 今の product-hub に、まだ知らない
--          まとまりの知らせ (ph-group-v1) を渡さない
--        ・ops.finish_card_event は変えない (event_id で結果を書く = どちらの知らせにも使える)
--   5. 名前空間つきで借りる ops.claim_outbox_events (entity_kind を必ず渡す・戻り値に entity_kind / entity_id / revision / kind)。PR-4 の product-hub が使う。
--      状態の決まり (auto / manual / link・期限つきの借り・回数) は claim_card_events と同じ
--   6. まとまりの知らせ = 毎回「今のまとまりの全部」の完全なスナップショット (設計 R3 High 3・§⑤)。差分の知らせは作らない。形 = ph-group-v1 (下の 7.)。
--      DB の決まり (知らせの表の insert の trigger = 書き方によらず全部の insert に効く):
--        ・同じまとまりの revision は増えるだけ: もっと大きい / 同じ revision の知らせがもうある = 断る (group_revision_not_newer)。
--          まとまりごとの鍵 (core.variation_group:<group_product_id>・設計 §⑤ の鍵の順のまとまりの鍵と同じ) で順番待ち = 同時の 2 つの取引でも同じ revision は 1 回だけ
--          (一意の (entity_kind, entity_id, kind, revision) が最後の守り)。🚨 前の revision と続いているか (+1 か) は見ない = revision を上げる数の持ち主は PR-5 の表
--        ・payload_hash = DB が作る ops.js_stable_sha256(payload) と同じ・作った人 = payload の created_by
--        ・形 (ops.group_snapshot_shape_problem) と、今の Company DB の値と同じか (ops.group_snapshot_problem): まとまりの商品・コード・名前・単品の代表か札か・
--          有効な子 (親がこのまとまりの単品で登録の状態が cancelled でない) の全部・廃止した子 (cancelled) の全部・子のコード・名前・売価・有効な JAN
--      product-hub の側の決まり (PR-4): 今の revision より大きいときだけ全部を置き換える・小さい / 同じは何もしない (= 古い revision は無視・同じ revision は 1 回だけ効く)
--   7. 知らせを書く部品 ops.enqueue_group_snapshot (security definer・だれにも渡さない = PR-5 のまとまりの関数の中だけで呼ぶ)。hash は DB が作る。
--      🚨 画面のロール (master_edit) が呼び手の取引でまとまりの知らせを書くのは、今は断る (ops.guard_product_hub_outbox_session に分かれ道を足した)。
--         PR-5 がまとまりの約束の操作を足すときに、この guard を置き換えて開く
--   8. 0052 の中身の守り (ops.guard_product_hub_outbox) に group_product_id と revision を足す (変えない列)
-- ph-group-v1 の形 (lib/product-hub-outbox.mjs の groupSnapshotShapeProblem と同じ決まり・試験で同じ見本を両方に通す):
--   { schema: 'ph-group-v1', revision: 1 以上の整数, created_by: 人,
--     group: { product_id: '数字', sku_id: '数字' (単品の代表) | null (札), code, name, kind: 'tag' | 'single' },
--     axes: [{ axis: 1 | 2, name }] (0〜2 つ・1 つなら 1・2 つなら 1 と 2 = 横 / 縦・並びは axis の順),
--     options: [{ axis, code: '-WH' (^-[A-Za-z0-9]{1,10}$), name, sort: 0 以上の整数 }] (有効な選択肢・軸ごとに code は小文字で一意・name は前後の空白を除いて NFKC でそろえた形で一意),
--     children: [{ sku_id: '数字', code, name, price: 0 以上の整数 | null, choices: {} | { '1': 横の code } | { '1': 横, '2': 縦 }, jans: ['8 か 13 桁'] }] (有効な子・1,000 まで。
--       choices は軸の全部か空 (軸を入れる前からの子)。choices がある子のコード (小文字) = まとまりのコード + 横 + 縦 (小文字)・(横, 縦) の組は子どうしで一意),
--     cancelled_children: [{ sku_id, code }] (廃止した子・有効な子と重ならない),
--     common: { shipping: { code, method, cost_jpy } | null, amazon_url, asin, official_url (文字 | null), reference_urls: [文字], yahoo: { price, price_sagawa, delivery_label, category_id, path } の一部 | null } }
-- 🚨 この migration は商品の値を何も変えない。知らせの行の中身も変えない (列を足すだけ)。今の単品の登録とカードの取り込みは前と同じに動く
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す (0034 の約束)
-- 🚨 ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しが要る: master_edit に ops.claim_outbox_events の実行を付ける (流さなくても今の動きは変わらない)

-- 1〜3. 列・制約
alter table ops.product_hub_outbox alter column sku_id drop not null;
alter table ops.product_hub_outbox add column group_product_id bigint;
alter table ops.product_hub_outbox add column revision integer not null default 1;
alter table ops.product_hub_outbox add column entity_kind text generated always as (case when group_product_id is not null then 'variation_group' else 'sku' end) stored;
alter table ops.product_hub_outbox add column entity_id bigint generated always as (coalesce(sku_id, group_product_id)) stored;
alter table ops.product_hub_outbox add constraint fk_pho_group foreign key (company_id, group_product_id) references core.products (company_id, product_id);
alter table ops.product_hub_outbox add constraint ck_pho_entity check ((sku_id is null) <> (group_product_id is null));
alter table ops.product_hub_outbox add constraint ck_pho_revision check (revision >= 1 and (sku_id is null or revision = 1));
alter table ops.product_hub_outbox drop constraint product_hub_outbox_kind_check;
alter table ops.product_hub_outbox drop constraint product_hub_outbox_schema_version_check;
alter table ops.product_hub_outbox add constraint ck_pho_kind check (
  (sku_id is not null and kind = 'card_create' and schema_version ~ '^ph-card-v[0-9]+$')
  or (group_product_id is not null and kind = 'group_snapshot' and schema_version ~ '^ph-group-v[0-9]+$'));
alter table ops.product_hub_outbox drop constraint product_hub_outbox_sku_id_kind_key;
alter table ops.product_hub_outbox add constraint ux_pho_entity unique (entity_kind, entity_id, kind, revision);
create index ix_product_hub_outbox_group on ops.product_hub_outbox (group_product_id, revision) where group_product_id is not null;
comment on column ops.product_hub_outbox.entity_kind is '知らせの相手の種類 (0066): sku = SKU のカード (sku_id) / variation_group = まとまりのカード (group_product_id)。生成の列';
comment on column ops.product_hub_outbox.entity_id is '知らせの相手の番号 (0066): sku_id か group_product_id。生成の列';
comment on column ops.product_hub_outbox.revision is 'まとまりの revision (0066)。SKU の知らせは 1 だけ。まとまりは増えるだけ (同じ / 小さい revision は insert で断る)';
comment on column ops.product_hub_outbox.group_product_id is 'まとまりの商品 (0066・札か単品の代表)。まとまりの知らせだけ';
comment on table ops.product_hub_outbox is 'product-hub のカードの知らせ (0052・0066 で名前空間)。登録と同じ取引で書く。中身は変えない・消さない。SKU の知らせは cdb_sku_id で冪等・まとまりの知らせは revision が大きいときだけ置き換え';

-- 前からの行の確かめ (生成の列と既定値で埋まった = UPDATE していない)
do $$
declare v_bad integer;
begin
  select pg_catalog.count(*) into v_bad from ops.product_hub_outbox o
   where o.entity_kind is distinct from 'sku' or o.entity_id is distinct from o.sku_id or o.revision is distinct from 1 or o.group_product_id is not null;
  if v_bad <> 0 then raise exception '0066: 前からの知らせ % 件が entity_kind = sku・entity_id = sku_id・revision = 1 になっていない', v_bad; end if;
end $$;

-- 8. 中身の守り (0052 の本文に group_product_id と revision を足しただけ)
create or replace function ops.guard_product_hub_outbox() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'product_hub_outbox は消さない' using errcode = 'P0001'; end if;
  if (new.event_id, new.company_id, new.sku_id, new.kind, new.schema_version, new.payload, new.payload_hash, new.request_id, new.created_by, new.created_at, new.group_product_id, new.revision)
     is distinct from (old.event_id, old.company_id, old.sku_id, old.kind, old.schema_version, old.payload, old.payload_hash, old.request_id, old.created_by, old.created_at, old.group_product_id, old.revision) then
    raise exception 'product_hub_outbox の知らせの中身 (event_id・版・payload・hash) は変えない' using errcode = 'P0001';
  end if;
  if old.status = 'done' then raise exception '済んだ知らせ (done) は変えない' using errcode = 'P0001'; end if;
  return new;
end $$;

-- 7. 画面のロールの insert の保険 (0052 の本文に、まとまりの知らせの分かれ道を足しただけ)。
--   まとまりの知らせは、今は画面のロールの取引では書けない (まとまりの約束の操作は PR-5 で足す = そのときこの関数を置き換える)
create or replace function ops.guard_product_hub_outbox_session() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' then return new; end if;
  if new.group_product_id is not null then
    raise exception 'group_snapshot_session_required: まとまりの知らせは、まとまりの約束の関数 (PR-5) の中だけで書く' using errcode = '42501';
  end if;
  v_sess := ops.current_master_write_session();
  if v_sess.session_id is null or v_sess.operation is distinct from 'sku_create' then
    raise exception 'master_write_session_required: カードの知らせは、登録の約束 (ops.register_new_sku) の中だけで書く' using errcode = '42501';
  end if;
  if new.sku_id is distinct from v_sess.sku_id or new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then
    raise exception 'master_write_session_mismatch: カードの知らせの SKU・request_id・作った人が登録の約束と違う' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_product_hub_outbox_session() from public;

-- 4. 今の借りる関数 = SKU の知らせだけ (0052 の本文に「and o.entity_kind = 'sku'」を足しただけ)
create or replace function ops.claim_card_events(p_owner text, p_mode text, p_event_id uuid default null, p_sku_id bigint default null, p_limit integer default 20,
                                      p_lease_seconds integer default 120, p_max_auto integer default 5)
  returns table (event_id uuid, sku_id bigint, schema_version text, payload jsonb, payload_hash text, attempts integer, status text, result jsonb)
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_owner is null or p_owner !~ '^[A-Za-z0-9_.:-]{1,100}$' then raise exception 'invalid_input: 借りる人 (owner) の形が違う' using errcode = '22023'; end if;
  if p_mode is null or p_mode not in ('auto', 'manual', 'link') then raise exception 'invalid_input: mode は auto / manual / link' using errcode = '22023'; end if;
  return query
  with c as (
    select o.event_id from ops.product_hub_outbox o
     where (case p_mode when 'auto' then o.status = 'pending' or (o.status = 'failed' and o.attempts < greatest(1, p_max_auto))
                        when 'manual' then o.status in ('pending', 'failed', 'conflict')
                        else o.status = 'conflict' end)
       and o.entity_kind = 'sku'
       and (o.leased_until is null or o.leased_until < pg_catalog.now())
       and (p_event_id is null or o.event_id = p_event_id) and (p_sku_id is null or o.sku_id = p_sku_id)
     order by o.created_at, o.event_id limit greatest(1, least(200, coalesce(p_limit, 20))) for update skip locked)
  update ops.product_hub_outbox o set lease_owner = p_owner, leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => greatest(10, least(3600, coalesce(p_lease_seconds, 120)))),
         attempts = o.attempts + 1, updated_at = pg_catalog.now()
    from c where o.event_id = c.event_id
  returning o.event_id, o.sku_id, o.schema_version, o.payload, o.payload_hash, o.attempts, o.status, o.result;
end $$;
revoke all on function ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer) from public;

-- 5. 名前空間つきで借りる (entity_kind は必ず渡す = sku / variation_group を混ぜて借りない)。状態の決まりは claim_card_events と同じ
create function ops.claim_outbox_events(p_owner text, p_mode text, p_entity_kind text, p_event_id uuid default null, p_entity_id bigint default null, p_limit integer default 20,
                                        p_lease_seconds integer default 120, p_max_auto integer default 5)
  returns table (event_id uuid, entity_kind text, entity_id bigint, revision integer, kind text, sku_id bigint, group_product_id bigint,
                 schema_version text, payload jsonb, payload_hash text, attempts integer, status text, result jsonb)
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_owner is null or p_owner !~ '^[A-Za-z0-9_.:-]{1,100}$' then raise exception 'invalid_input: 借りる人 (owner) の形が違う' using errcode = '22023'; end if;
  if p_mode is null or p_mode not in ('auto', 'manual', 'link') then raise exception 'invalid_input: mode は auto / manual / link' using errcode = '22023'; end if;
  if p_entity_kind is null or p_entity_kind not in ('sku', 'variation_group') then raise exception 'invalid_input: entity_kind は sku / variation_group' using errcode = '22023'; end if;
  return query
  with c as (
    select o.event_id from ops.product_hub_outbox o
     where (case p_mode when 'auto' then o.status = 'pending' or (o.status = 'failed' and o.attempts < greatest(1, p_max_auto))
                        when 'manual' then o.status in ('pending', 'failed', 'conflict')
                        else o.status = 'conflict' end)
       and o.entity_kind = p_entity_kind
       and (o.leased_until is null or o.leased_until < pg_catalog.now())
       and (p_event_id is null or o.event_id = p_event_id) and (p_entity_id is null or o.entity_id = p_entity_id)
     order by o.created_at, o.event_id limit greatest(1, least(200, coalesce(p_limit, 20))) for update skip locked)
  update ops.product_hub_outbox o set lease_owner = p_owner, leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => greatest(10, least(3600, coalesce(p_lease_seconds, 120)))),
         attempts = o.attempts + 1, updated_at = pg_catalog.now()
    from c where o.event_id = c.event_id
  returning o.event_id, o.entity_kind, o.entity_id, o.revision, o.kind, o.sku_id, o.group_product_id, o.schema_version, o.payload, o.payload_hash, o.attempts, o.status, o.result;
end $$;
revoke all on function ops.claim_outbox_events(text, text, text, uuid, bigint, integer, integer, integer) from public;

-- 6. ph-group-v1 の形 (表を読まない)。問題が無ければ null・あれば最初の 1 つ (lib/product-hub-outbox.mjs の groupSnapshotShapeProblem と同じ決まり)
create function ops.group_snapshot_shape_problem(p jsonb) returns text
  language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v_axes    integer[];
  v_e       jsonb;
  v_k       text;
  v_n       integer;
  v_gcode   text;
  v_code    text;
  v_c1      text;
  v_c2      text;
  v_ids     text[] := '{}';
  v_codes   text[] := '{}';
  v_combos  text[] := '{}';
  v_onames  text[] := '{}';
  v_ocodes  text[] := '{}';
  v_oexact  text[] := '{}';
begin
  if pg_catalog.jsonb_typeof(p) is distinct from 'object' then return 'payload_not_object'; end if;
  if exists (select 1 from pg_catalog.jsonb_object_keys(p) k where k not in ('schema', 'revision', 'created_by', 'group', 'axes', 'options', 'children', 'cancelled_children', 'common'))
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(p)) <> 9 then return 'payload_keys'; end if;
  if (p ->> 'schema') is distinct from 'ph-group-v1' or pg_catalog.jsonb_typeof(p -> 'schema') <> 'string' then return 'schema'; end if;
  if pg_catalog.jsonb_typeof(p -> 'revision') <> 'number' or (p ->> 'revision') !~ '^[1-9][0-9]{0,9}$' or (p ->> 'revision')::numeric > 2147483647 then return 'revision'; end if;
  if pg_catalog.jsonb_typeof(p -> 'created_by') <> 'string' or pg_catalog.length(p ->> 'created_by') not between 1 and 320 or (p ->> 'created_by') ~ '[[:cntrl:]]' then return 'created_by'; end if;

  -- group
  v_e := p -> 'group';
  if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
     or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('product_id', 'sku_id', 'code', 'name', 'kind'))
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 5 then return 'group_keys'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'product_id') <> 'string' or (v_e ->> 'product_id') !~ '^[1-9][0-9]{0,17}$' then return 'group_product_id'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'kind') <> 'string' or (v_e ->> 'kind') not in ('tag', 'single') then return 'group_kind'; end if;
  if (v_e ->> 'kind') = 'tag' and pg_catalog.jsonb_typeof(v_e -> 'sku_id') <> 'null' then return 'group_sku_id'; end if;
  if (v_e ->> 'kind') = 'single' and (pg_catalog.jsonb_typeof(v_e -> 'sku_id') <> 'string' or (v_e ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$') then return 'group_sku_id'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'code') <> 'string' or pg_catalog.length(v_e ->> 'code') not between 1 and 255 or (v_e ->> 'code') ~ '[[:cntrl:]]'
     or (v_e ->> 'code') <> pg_catalog.btrim(v_e ->> 'code') then return 'group_code'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'name') <> 'string' or pg_catalog.length(v_e ->> 'name') not between 1 and 255 or (v_e ->> 'name') ~ '[[:cntrl:]]' then return 'group_name'; end if;
  v_gcode := pg_catalog.lower(v_e ->> 'code');

  -- axes (0〜2・1 つなら 1・2 つなら 1, 2 の順)
  if pg_catalog.jsonb_typeof(p -> 'axes') is distinct from 'array' or pg_catalog.jsonb_array_length(p -> 'axes') > 2 then return 'axes'; end if;
  v_axes := '{}';
  for v_e in select x from pg_catalog.jsonb_array_elements(p -> 'axes') x loop
    if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
       or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('axis', 'name'))
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 2 then return 'axis_keys'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'axis') <> 'number' or (v_e ->> 'axis') not in ('1', '2') then return 'axis_no'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'name') <> 'string' or pg_catalog.length(pg_catalog.btrim(v_e ->> 'name')) not between 1 and 100 or (v_e ->> 'name') ~ '[[:cntrl:]]' then return 'axis_name'; end if;
    v_axes := v_axes || (v_e ->> 'axis')::integer;
  end loop;
  if v_axes is distinct from (array[1, 2])[1:pg_catalog.cardinality(v_axes)] then return 'axes_order'; end if;

  -- options (有効な選択肢)
  if pg_catalog.jsonb_typeof(p -> 'options') is distinct from 'array' then return 'options'; end if;
  for v_e in select x from pg_catalog.jsonb_array_elements(p -> 'options') x loop
    if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
       or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('axis', 'code', 'name', 'sort'))
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 4 then return 'option_keys'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'axis') <> 'number' or (v_e ->> 'axis') not in ('1', '2') or not ((v_e ->> 'axis')::integer = any (v_axes)) then return 'option_axis'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'code') <> 'string' or (v_e ->> 'code') !~ '^-[A-Za-z0-9]{1,10}$' then return 'option_code'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'name') <> 'string' or pg_catalog.length(pg_catalog.btrim(v_e ->> 'name')) not between 1 and 100 or (v_e ->> 'name') ~ '[[:cntrl:]]' then return 'option_name'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'sort') <> 'number' or (v_e ->> 'sort') !~ '^(0|[1-9][0-9]{0,8})$' then return 'option_sort'; end if;
    v_k := (v_e ->> 'axis') || ':' || pg_catalog.lower(v_e ->> 'code');
    if v_k = any (v_ocodes) then return 'option_code_dup'; end if;
    v_ocodes := v_ocodes || v_k;
    v_oexact := v_oexact || ((v_e ->> 'axis') || ':' || (v_e ->> 'code'));
    v_k := (v_e ->> 'axis') || ':' || pg_catalog.btrim(pg_catalog.normalize(v_e ->> 'name', 'NFKC'));
    if v_k = any (v_onames) then return 'option_name_dup'; end if;
    v_onames := v_onames || v_k;
  end loop;

  -- children (有効な子)
  if pg_catalog.jsonb_typeof(p -> 'children') is distinct from 'array' or pg_catalog.jsonb_array_length(p -> 'children') > 1000 then return 'children'; end if;
  for v_e in select x from pg_catalog.jsonb_array_elements(p -> 'children') x loop
    if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
       or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('sku_id', 'code', 'name', 'price', 'choices', 'jans'))
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 6 then return 'child_keys'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'sku_id') <> 'string' or (v_e ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$' then return 'child_sku_id'; end if;
    if (v_e ->> 'sku_id') = any (v_ids) then return 'child_dup'; end if;
    v_ids := v_ids || (v_e ->> 'sku_id');
    if pg_catalog.jsonb_typeof(v_e -> 'code') <> 'string' or pg_catalog.length(v_e ->> 'code') not between 1 and 255 or (v_e ->> 'code') ~ '[[:cntrl:]]'
       or (v_e ->> 'code') <> pg_catalog.btrim(v_e ->> 'code') then return 'child_code'; end if;
    v_code := pg_catalog.lower(v_e ->> 'code');
    if v_code = any (v_codes) then return 'child_dup'; end if;
    v_codes := v_codes || v_code;
    if pg_catalog.jsonb_typeof(v_e -> 'name') <> 'string' or pg_catalog.length(v_e ->> 'name') not between 1 and 255 or (v_e ->> 'name') ~ '[[:cntrl:]]' then return 'child_name'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'price') not in ('null', 'number') or (pg_catalog.jsonb_typeof(v_e -> 'price') = 'number' and (v_e ->> 'price') !~ '^(0|[1-9][0-9]{0,11})$') then return 'child_price'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'jans') is distinct from 'array' or pg_catalog.jsonb_array_length(v_e -> 'jans') > 5
       or exists (select 1 from pg_catalog.jsonb_array_elements(v_e -> 'jans') j where pg_catalog.jsonb_typeof(j) <> 'string' or (j #>> '{}') !~ '^([0-9]{8}|[0-9]{13})$')
       or (select pg_catalog.count(distinct j) from pg_catalog.jsonb_array_elements_text(v_e -> 'jans') j) <> pg_catalog.jsonb_array_length(v_e -> 'jans') then return 'child_jans'; end if;
    -- choices = 空 (軸を入れる前からの子) か、軸の全部
    if pg_catalog.jsonb_typeof(v_e -> 'choices') is distinct from 'object' then return 'child_choices'; end if;
    select pg_catalog.count(*) into v_n from pg_catalog.jsonb_object_keys(v_e -> 'choices');
    if v_n > 0 then
      if exists (select 1 from pg_catalog.jsonb_object_keys(v_e -> 'choices') k where k not in ('1', '2'))
         or v_n <> pg_catalog.cardinality(v_axes)
         or exists (select 1 from pg_catalog.unnest(v_axes) a where pg_catalog.jsonb_typeof((v_e -> 'choices') -> (a::text)) is distinct from 'string') then return 'child_choices'; end if;
      v_c1 := v_e -> 'choices' ->> '1';
      v_c2 := v_e -> 'choices' ->> '2';
      -- 選択肢は有効な選択肢の code を打ったとおりに (大文字小文字も同じ)
      if not (('1:' || v_c1) = any (v_oexact)) or (v_c2 is not null and not (('2:' || v_c2) = any (v_oexact))) then return 'child_choice_unknown'; end if;
      if v_code is distinct from v_gcode || pg_catalog.lower(v_c1) || pg_catalog.lower(coalesce(v_c2, '')) then return 'child_code_not_group_plus_choices'; end if;
      v_k := pg_catalog.lower(v_c1) || '|' || pg_catalog.lower(coalesce(v_c2, ''));
      if v_k = any (v_combos) then return 'child_choice_dup'; end if;
      v_combos := v_combos || v_k;
    end if;
  end loop;

  -- cancelled_children (廃止した子)
  if pg_catalog.jsonb_typeof(p -> 'cancelled_children') is distinct from 'array' or pg_catalog.jsonb_array_length(p -> 'cancelled_children') > 1000 then return 'cancelled_children'; end if;
  for v_e in select x from pg_catalog.jsonb_array_elements(p -> 'cancelled_children') x loop
    if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
       or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('sku_id', 'code'))
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 2 then return 'cancelled_keys'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'sku_id') <> 'string' or (v_e ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$' then return 'cancelled_sku_id'; end if;
    if pg_catalog.jsonb_typeof(v_e -> 'code') <> 'string' or pg_catalog.length(v_e ->> 'code') not between 1 and 255 or (v_e ->> 'code') ~ '[[:cntrl:]]'
       or (v_e ->> 'code') <> pg_catalog.btrim(v_e ->> 'code') then return 'cancelled_code'; end if;
    if (v_e ->> 'sku_id') = any (v_ids) or pg_catalog.lower(v_e ->> 'code') = any (v_codes) then return 'cancelled_dup'; end if;
    v_ids := v_ids || (v_e ->> 'sku_id');
    v_codes := v_codes || pg_catalog.lower(v_e ->> 'code');
  end loop;

  -- common (共通の欄・カードの知らせ ph-card-v1 と同じ欄の形)
  v_e := p -> 'common';
  if pg_catalog.jsonb_typeof(v_e) is distinct from 'object'
     or exists (select 1 from pg_catalog.jsonb_object_keys(v_e) k where k not in ('shipping', 'amazon_url', 'asin', 'official_url', 'reference_urls', 'yahoo'))
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e)) <> 6 then return 'common_keys'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'shipping') not in ('null', 'object')
     or (pg_catalog.jsonb_typeof(v_e -> 'shipping') = 'object' and (
          exists (select 1 from pg_catalog.jsonb_object_keys(v_e -> 'shipping') k where k not in ('code', 'method', 'cost_jpy'))
          or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_e -> 'shipping')) <> 3)) then return 'common_shipping'; end if;
  if exists (select 1 from pg_catalog.unnest(array['amazon_url', 'asin', 'official_url']) f where pg_catalog.jsonb_typeof(v_e -> f) not in ('null', 'string')) then return 'common_urls'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'reference_urls') <> 'array'
     or exists (select 1 from pg_catalog.jsonb_array_elements(v_e -> 'reference_urls') u where pg_catalog.jsonb_typeof(u) <> 'string') then return 'common_reference_urls'; end if;
  if pg_catalog.jsonb_typeof(v_e -> 'yahoo') not in ('null', 'object')
     or exists (select 1 from pg_catalog.jsonb_object_keys(case when pg_catalog.jsonb_typeof(v_e -> 'yahoo') = 'object' then v_e -> 'yahoo' else '{}'::jsonb end) k
                 where k not in ('price', 'price_sagawa', 'delivery_label', 'category_id', 'path')) then return 'common_yahoo'; end if;
  return null;
end $$;
revoke all on function ops.group_snapshot_shape_problem(jsonb) from public;

-- 6. 形 + 今の Company DB の値と同じか (まとまり・子の全部・廃止した子の全部)。問題が無ければ null
create function ops.group_snapshot_problem(p_company_id smallint, p_group_product_id bigint, p_revision integer, p jsonb) returns text
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_prob   text;
  v_prod   core.products;
  v_rep    core.skus;
  v_g      jsonb;
  v_active text[];
  v_cancel text[];
  v_bad    text;
begin
  v_prob := ops.group_snapshot_shape_problem(p);
  if v_prob is not null then return v_prob; end if;
  if (p ->> 'revision')::integer is distinct from p_revision then return 'revision_mismatch'; end if;
  v_g := p -> 'group';
  if (v_g ->> 'product_id')::bigint is distinct from p_group_product_id then return 'group_mismatch'; end if;
  select * into v_prod from core.products where product_id = p_group_product_id and company_id = p_company_id;
  if v_prod.product_id is null then return 'group_not_found'; end if;
  if v_prod.parent_product_id is not null then return 'group_has_parent'; end if;
  if (v_g ->> 'name') is distinct from v_prod.name then return 'group_name_differs'; end if;
  select * into v_rep from core.skus where product_id = p_group_product_id and company_id = p_company_id and sku_kind = 'single';
  if v_rep.sku_id is not null then
    if (v_g ->> 'kind') <> 'single' or (v_g ->> 'sku_id')::bigint is distinct from v_rep.sku_id or (v_g ->> 'code') is distinct from v_rep.code then return 'group_rep_differs'; end if;
  else
    if (v_g ->> 'kind') <> 'tag' or exists (select 1 from core.skus where product_id = p_group_product_id) or (v_g ->> 'code') is distinct from v_prod.display_code then return 'group_tag_differs'; end if;
  end if;
  -- 子の全部 (親がこのまとまりの単品の SKU)。登録の状態が cancelled = 廃止した子・それ以外 (状態の行が無いのも) = 有効な子
  select coalesce(pg_catalog.array_agg(s.sku_id::text order by s.sku_id) filter (where r.state is distinct from 'cancelled'), '{}'),
         coalesce(pg_catalog.array_agg(s.sku_id::text order by s.sku_id) filter (where r.state = 'cancelled'), '{}')
    into v_active, v_cancel
    from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single'
    left join ops.master_registrations r on r.sku_id = s.sku_id
   where c.parent_product_id = p_group_product_id and c.company_id = p_company_id;
  if v_active is distinct from (select coalesce(pg_catalog.array_agg(x ->> 'sku_id' order by (x ->> 'sku_id')::bigint), '{}') from pg_catalog.jsonb_array_elements(p -> 'children') x) then
    return 'children_differ';
  end if;
  if v_cancel is distinct from (select coalesce(pg_catalog.array_agg(x ->> 'sku_id' order by (x ->> 'sku_id')::bigint), '{}') from pg_catalog.jsonb_array_elements(p -> 'cancelled_children') x) then
    return 'cancelled_children_differ';
  end if;
  -- 子ごとの値 (コード・名前・売価・有効な JAN) = 今の Company DB の値
  select x ->> 'code' into v_bad
    from pg_catalog.jsonb_array_elements(p -> 'children') x join core.skus s on s.sku_id = (x ->> 'sku_id')::bigint
   where (x ->> 'code') is distinct from s.code or (x ->> 'name') is distinct from s.name
      or (case when pg_catalog.jsonb_typeof(x -> 'price') = 'null' then null else (x ->> 'price')::bigint end) is distinct from s.standard_price_jpy
      or (select coalesce(pg_catalog.array_agg(j order by j), '{}') from pg_catalog.jsonb_array_elements_text(x -> 'jans') j)
         is distinct from (select coalesce(pg_catalog.array_agg(e.external_value order by e.external_value), '{}') from core.external_ids e
                            where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null)
   limit 1;
  if v_bad is not null then return 'child_differs: ' || v_bad; end if;
  select x ->> 'code' into v_bad
    from pg_catalog.jsonb_array_elements(p -> 'cancelled_children') x join core.skus s on s.sku_id = (x ->> 'sku_id')::bigint
   where (x ->> 'code') is distinct from s.code limit 1;
  if v_bad is not null then return 'cancelled_child_differs: ' || v_bad; end if;
  return null;
end $$;
revoke all on function ops.group_snapshot_problem(smallint, bigint, integer, jsonb) from public;

-- 6. まとまりの知らせの insert の決まり (書き方によらず全部の insert に効く)
create function ops.guard_product_hub_outbox_group() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prob text;
begin
  -- まとまりごとの鍵 (設計 §⑤ の鍵の順の「まとまりごとの鍵」と同じ) = 同じまとまりの知らせは順番に入る
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.variation_group:' || new.group_product_id::text, 0));
  if exists (select 1 from ops.product_hub_outbox o where o.group_product_id = new.group_product_id and o.kind = new.kind and o.revision >= new.revision) then
    raise exception 'group_revision_not_newer: まとまり % の知らせに revision % 以上がもうある (revision は増えるだけ・同じ revision は 1 回だけ)', new.group_product_id, new.revision
      using errcode = 'P0001';
  end if;
  if new.payload_hash is distinct from ops.js_stable_sha256(new.payload) then
    raise exception 'invalid_value: まとまりの知らせの payload_hash が DB の作る hash と違う' using errcode = '22023';
  end if;
  if new.created_by is distinct from (new.payload ->> 'created_by') then
    raise exception 'invalid_value: まとまりの知らせの作った人が payload の created_by と違う' using errcode = '22023';
  end if;
  v_prob := ops.group_snapshot_problem(new.company_id, new.group_product_id, new.revision, new.payload);
  if v_prob is not null then
    raise exception 'invalid_value: まとまりの知らせ (ph-group-v1) が形か今の Company DB の値と違う: %', v_prob using errcode = '22023';
  end if;
  return new;
end $$;
revoke all on function ops.guard_product_hub_outbox_group() from public;
create trigger trg_product_hub_outbox_group before insert on ops.product_hub_outbox
  for each row when (new.group_product_id is not null) execute function ops.guard_product_hub_outbox_group();

-- 7. まとまりの知らせを書く部品 (PR-5 のまとまりの関数の中だけで呼ぶ・だれにも渡さない)。hash は DB が作る。決まりは上の trigger
create function ops.enqueue_group_snapshot(p_group_product_id bigint, p_revision integer, p_payload jsonb, p_request_id uuid, p_actor_id text) returns uuid
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_event uuid;
begin
  if p_group_product_id is null or p_revision is null or p_payload is null or p_request_id is null or p_actor_id is null then
    raise exception 'invalid_input: まとまり・revision・payload・request_id・人は必須' using errcode = '22023';
  end if;
  insert into ops.product_hub_outbox (company_id, sku_id, group_product_id, kind, schema_version, payload, payload_hash, revision, request_id, created_by)
    values (1, null, p_group_product_id, 'group_snapshot', 'ph-group-v1', p_payload, ops.js_stable_sha256(p_payload), p_revision, p_request_id, p_actor_id)
    returning event_id into v_event;
  return v_event;
end $$;
revoke all on function ops.enqueue_group_snapshot(bigint, integer, jsonb, uuid, text) from public;
