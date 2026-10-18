-- 0088 (0084 = メーカー #1739・0085 = 入出庫の履歴 #1741・0086 = 販売数のグラフ #1742・0087 = 大きさ・重さ #1744 の次。migrate は欠番を断る):
--   ブランドの名簿と商品ごとのブランド・商品の段階 (区分 = 定番 / 季節 / 試し・終売の予定日)・初めて売った日 (発売日) を読む関数 (中原さん 2026-10-17)
--   中原さん「メーカーとは別にブランド (自社ブランド・他社ブランド) を入れたい」「商品の段階: 発売日 (= 初めて売った日)・定番 / 季節 / 試し・終売の予定日」
--   決まり: Company DB だけに持つ (NE・ロジザード・product-hub には送らない)・どれも必須でない (新商品の登録の「値か入力なし」に数えない)・単品だけ (セットは構成品から)
--
-- なにを:
--   1. core.brands = ブランドの名簿 (名前・同じと見る形 name_norm = メーカーと同じ決まり core.maker_name_norm・会社の中で一意・自社ブランドの印 own_brand)。
--      🚨 core.products.brand / own_brand (0002・mart 0007 / 0009 / 0077 に出る) は使わない:
--        brand = 夜間ロードの属性の解決 (apps/company-db/load/engine.mjs の 10. = qoo10 / product_hub の観測) が書く列 = ポータルで入れた値を翌朝に上書きする / 消す道がある。
--        own_brand = 商品の行の印 (ブランドの印ではない・夜間ロードは書かないが持ち主の表 (config/master-ownership.mjs) にも無く、書くには core.products の update の権限を広げる)。
--        メーカー (0084) と同じく、名簿の番号で別の表に持つ = 夜間ロードは触らない。mart にポータルのブランドを出すのは後の PR
--   2. core.product_brands = 商品 (単品の商品の行) ごとの今のブランド (1 行 = 1 商品・無い = ブランドなし)
--   3. core.product_lifecycle = 商品ごとの区分 (staple 定番 / seasonal 季節 / trial 試し) と終売の予定日 (どちらも無い = 行を消す)
--   4. 記録 (追記だけ): ops.product_brand_events・ops.brand_requests (名簿に足す要求の控え)・ops.brand_flag_events (自社ブランドの印を変えた)・ops.product_lifecycle_events
--   5. 書く関数 (security definer・新商品の登録 / まとまりの登録 / 商品の画面の保存の取引の中で): ops.create_brand・ops.set_brand_own・ops.set_product_brand・ops.set_product_lifecycle
--   6. 読む関数: ops.sku_first_sold(sku_ids) = 初めて売った日 (NE の出荷 + モールの注文から・下の 7.)・ops.sales_records_from() = 記録の始まりの日
--      (画面のロールに注文・出荷の表は読ませない = 0054 / 0074 と同じ)
-- 🚨 既にある表・関数は変えない (表と関数を足すだけ・今のデータは変えない)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す。
--    画面のロール (master_edit) の権限 (表の select・関数の実行) は scripts/company-db/create-master-edit-roles.mjs を流し直して付ける (流すまで画面は欄を出さない)

-- ═══════════ 1. 名簿・商品ごとのブランド・段階・記録 ═══════════
-- 名前の形 (掃除・同じと見る形・問題) はメーカーの 0084 の関数をそのまま使う (JS = lib/master-maker.mjs と同じ決まり)
create table core.brands (
  brand_id           bigint generated always as identity primary key,
  company_id         smallint not null default 1 references core.companies,
  name               text not null,
  name_norm          text not null,
  own_brand          boolean not null default false,
  active             boolean not null default true,
  created_by_type    text not null default 'human',
  created_by_id      text not null,
  created_request_id uuid not null unique,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  constraint ck_brands_name check (name = core.maker_name_clean(name) and ops.maker_name_problem(name) is null),
  constraint ck_brands_norm check (name_norm = core.maker_name_norm(name)),
  unique (company_id, name_norm),
  unique (company_id, brand_id)
);
comment on table core.brands is '🆕 0088: ブランドの名簿 (own_brand = 自社ブランド)。Company DB だけに持つ。同じと見る名前 (name_norm) は会社の中で 1 つ。書くのは ops.create_brand / ops.set_brand_own だけ';

create table core.product_brands (
  product_id bigint primary key references core.products,
  company_id smallint not null default 1 references core.companies,
  brand_id   bigint not null,
  set_by     text not null,
  set_via    text not null check (set_via in ('register', 'variation', 'sku_page')),
  request_id uuid not null,
  set_at     timestamptz not null default now(),
  foreign key (company_id, brand_id) references core.brands (company_id, brand_id)
);
create index ix_product_brands_brand on core.product_brands (brand_id);
comment on table core.product_brands is '🆕 0088: 商品 (単品の商品の行) ごとの今のブランド (行が無い = なし)。夜間ロードは触らない (core.products.brand とは別)。書くのは ops.set_product_brand だけ';

create table core.product_lifecycle (
  product_id     bigint primary key references core.products,
  company_id     smallint not null default 1 references core.companies,
  stage          text check (stage in ('staple', 'seasonal', 'trial')),
  end_planned_on date check (end_planned_on between date '2000-01-01' and date '2099-12-31'),
  set_by         text not null,
  set_via        text not null check (set_via in ('register', 'variation', 'sku_page')),
  request_id     uuid not null,
  set_at         timestamptz not null default now(),
  constraint ck_product_lifecycle_some check (stage is not null or end_planned_on is not null)
);
create index ix_product_lifecycle_end on core.product_lifecycle (end_planned_on) where end_planned_on is not null;
comment on table core.product_lifecycle is '🆕 0088: 商品ごとの区分 (staple = 定番 / seasonal = 季節 / trial = 試し) と終売の予定日。どちらも無い = 行が無い。書くのは ops.set_product_lifecycle だけ';

create table ops.product_brand_events (
  event_id      bigint generated always as identity primary key,
  company_id    smallint not null default 1 references core.companies,
  product_id    bigint not null references core.products,
  sku_id        bigint not null references core.skus,
  from_brand_id bigint references core.brands,
  to_brand_id   bigint references core.brands,
  via           text not null check (via in ('register', 'variation', 'sku_page')),
  actor_id      text not null,
  request_id    uuid not null,
  reason        text,
  db_user       text not null,
  created_at    timestamptz not null default now(),
  constraint ck_pbe_changed check (from_brand_id is distinct from to_brand_id),
  unique (request_id, product_id)
);
create index ix_product_brand_events_product on ops.product_brand_events (product_id, event_id desc);
comment on table ops.product_brand_events is '🆕 0088: 商品のブランドを付けた・変えた・外した記録 (追記だけ・書くのは ops.set_product_brand だけ)';

create table ops.brand_requests (
  request_id uuid        primary key,
  actor_id   text        not null,
  name_norm  text        not null,
  own_brand  boolean     not null,
  brand_id   bigint      not null references core.brands,
  existed    boolean     not null,
  created_at timestamptz not null default now()
);
comment on table ops.brand_requests is '🆕 0088: ops.create_brand の要求の控え (足した回・既にあった名前を返した回の両方)。同じ request_id = この答え・違う名前 / 人 / 印 = request_id_reused';

create table ops.brand_flag_events (
  event_id   bigint generated always as identity primary key,
  brand_id   bigint not null references core.brands,
  from_own   boolean not null,
  to_own     boolean not null,
  actor_id   text not null,
  request_id uuid not null unique,
  changed    boolean not null,
  reason     text,
  db_user    text not null,
  created_at timestamptz not null default now(),
  constraint ck_bfe_changed check (changed = (from_own <> to_own))
);
comment on table ops.brand_flag_events is '🆕 0088: ブランドの「自社ブランド」の印の要求の記録 (追記だけ・書くのは ops.set_brand_own だけ)。変えなかった (もうその印だった) 要求も changed = false で残す = 押し直しはいつもこの答え (#brand Codex R1 M)';

create table ops.product_lifecycle_events (
  event_id   bigint generated always as identity primary key,
  company_id smallint not null default 1 references core.companies,
  product_id bigint not null references core.products,
  sku_id     bigint not null references core.skus,
  from_stage text,
  to_stage   text,
  from_end   date,
  to_end     date,
  via        text not null check (via in ('register', 'variation', 'sku_page')),
  actor_id   text not null,
  request_id uuid not null,
  reason     text,
  db_user    text not null,
  created_at timestamptz not null default now(),
  constraint ck_ple_changed check (from_stage is distinct from to_stage or from_end is distinct from to_end),
  unique (request_id, product_id)
);
create index ix_product_lifecycle_events_product on ops.product_lifecycle_events (product_id, event_id desc);
comment on table ops.product_lifecycle_events is '🆕 0088: 商品の区分・終売の予定日を付けた・変えた・外した記録 (追記だけ・書くのは ops.set_product_lifecycle だけ)';
revoke all on core.brands, core.product_brands, core.product_lifecycle, ops.product_brand_events, ops.brand_requests, ops.brand_flag_events, ops.product_lifecycle_events from public;

-- 守り: 書くのは関数の中だけ (印 ops.brand_life_protocol)・記録は追記だけ・TRUNCATE できない
create function ops.guard_brand_life_tables() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_table_name in ('product_brand_events', 'brand_requests', 'brand_flag_events', 'product_lifecycle_events') and tg_op <> 'INSERT' then
    raise exception '%: 記録は変えない・消さない (追記だけ)', tg_table_name using errcode = '42501';
  end if;
  if coalesce(pg_catalog.current_setting('ops.brand_life_protocol', true), '') is distinct from '1' then
    raise exception '%: ブランド・商品の段階の表は ops.create_brand / ops.set_brand_own / ops.set_product_brand / ops.set_product_lifecycle の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_brand_life_tables() from public;
do $$
declare t text;
begin
  foreach t in array array['core.brands', 'core.product_brands', 'core.product_lifecycle', 'ops.product_brand_events', 'ops.brand_requests', 'ops.brand_flag_events', 'ops.product_lifecycle_events'] loop
    execute format('create trigger trg_brand_life_guard before insert or update or delete on %s for each row execute function ops.guard_brand_life_tables()', t);
    execute format('create trigger trg_brand_life_no_truncate before truncate on %s for each statement execute function core.reject_mutation()', t);
  end loop;
end $$;

-- ═══════════ 2. 名簿に足す・自社ブランドの印 ═══════════
/**
 * p_name = 人が打った名前 (掃除してから確かめる)・p_own = 自社ブランドとして足すか。戻り値 = { ok, brand_id, name, own_brand, existed, replayed? }
 *   existed = 同じと見る名前のブランドが既にあった (足さずにそれを返す = 重ねない・印も名簿のまま)。押し直し (同じ request_id・同じと見る名前・同じ人・同じ印) = 控えの答え
 * 断る: invalid_input (形・人) / before_cutover (切替の段階が new_open でない) / request_id_reused (同じ request_id で違う名前・人・印)
 */
create function ops.create_brand(p_request_id uuid, p_actor_id text, p_name text, p_own boolean) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_name text := core.maker_name_clean(p_name);
  v_norm text;
  v_prob text := ops.maker_name_problem(p_name);
  v_own  boolean := coalesce(p_own, false);
  v_b    core.brands;
  v_q    ops.brand_requests;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if v_prob is not null then
    raise exception 'invalid_input: ブランドの名前 (%)', case v_prob when 'empty' then '空' when 'too_long' then '100 字まで' else '改行・制御文字は入れない' end using errcode = '22023';
  end if;
  if (select s.phase from ops.master_cutover_state s where s.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  v_norm := core.maker_name_norm(v_name);
  -- 鍵: 同じ request_id → 同じと見る名前 (2 人が同時に同じブランドを足しても 1 つ)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.brand_request:' || p_request_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.brand:' || v_norm, 0));
  select * into v_q from ops.brand_requests r where r.request_id = p_request_id;
  if v_q.request_id is not null then
    if v_q.name_norm is distinct from v_norm or v_q.actor_id is distinct from p_actor_id or v_q.own_brand is distinct from v_own then
      raise exception 'request_id_reused: request_id % はほかの名前・人で使われている', p_request_id using errcode = '23505';
    end if;
    select * into v_b from core.brands b where b.brand_id = v_q.brand_id;
    return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', v_b.own_brand, 'existed', v_q.existed, 'replayed', true);
  end if;
  perform pg_catalog.set_config('ops.brand_life_protocol', '1', true);
  select * into v_b from core.brands b where b.company_id = 1 and b.name_norm = v_norm;
  if v_b.brand_id is not null then
    insert into ops.brand_requests (request_id, actor_id, name_norm, own_brand, brand_id, existed) values (p_request_id, p_actor_id, v_norm, v_own, v_b.brand_id, true);
    perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
    return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', v_b.own_brand, 'existed', true, 'active', v_b.active);
  end if;
  insert into core.brands (company_id, name, name_norm, own_brand, created_by_id, created_request_id) values (1, v_name, v_norm, v_own, p_actor_id, p_request_id) returning * into v_b;
  insert into ops.brand_requests (request_id, actor_id, name_norm, own_brand, brand_id, existed) values (p_request_id, p_actor_id, v_norm, v_own, v_b.brand_id, false);
  perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', v_b.own_brand, 'existed', false);
end $$;
revoke all on function ops.create_brand(uuid, text, text, boolean) from public;

/**
 * 自社ブランドの印を変える (名簿の行・そのブランドの全部の商品に効く)。p_seen_own = 画面が見ていた印 (違う = version_conflict)。
 * 戻り値 = { ok, brand_id, name, own_brand, changed, replayed? }。同じ request_id の押し直し = 前の答え (違う中身 = request_id_reused)
 */
create function ops.set_brand_own(p_request_id uuid, p_actor_id text, p_brand_id bigint, p_own boolean, p_seen_own boolean, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_b core.brands;
  v_e ops.brand_flag_events;
begin
  if p_request_id is null or p_brand_id is null or p_own is null or p_seen_own is null then raise exception 'invalid_input: request_id・ブランド・印が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if (select s.phase from ops.master_cutover_state s where s.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.brand_request:' || p_request_id::text, 0));
  select * into v_e from ops.brand_flag_events e where e.request_id = p_request_id;
  if v_e.event_id is not null then
    if v_e.brand_id is distinct from p_brand_id or v_e.to_own is distinct from p_own or v_e.actor_id is distinct from p_actor_id then
      raise exception 'request_id_reused: request_id % はほかの中身で使われている', p_request_id using errcode = '23505';
    end if;
    select * into v_b from core.brands b where b.brand_id = p_brand_id;
    return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', v_e.to_own, 'changed', v_e.changed, 'replayed', true);
  end if;
  select * into v_b from core.brands b where b.brand_id = p_brand_id and b.company_id = 1 for update;
  if v_b.brand_id is null or not v_b.active then raise exception 'brand_unknown: ブランド % は名簿に無いか、使わないブランド', p_brand_id using errcode = '22023'; end if;
  -- 🆕 #brand Codex R1 M: もうその印 = 変えないが、要求は残す (応答が届かずに押し直したとき、その間にほかの人が戻した印を黙って上書きしない)
  if v_b.own_brand = p_own then
    perform pg_catalog.set_config('ops.brand_life_protocol', '1', true);
    insert into ops.brand_flag_events (brand_id, from_own, to_own, actor_id, request_id, changed, reason, db_user) values (p_brand_id, v_b.own_brand, p_own, p_actor_id, p_request_id, false, p_reason, v_db_user);
    perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
    return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', v_b.own_brand, 'changed', false);
  end if;
  if v_b.own_brand is distinct from p_seen_own then
    raise exception 'version_conflict: 画面を開いた後にこのブランドの自社ブランドの印が変わった' using errcode = '40001';
  end if;
  perform pg_catalog.set_config('ops.brand_life_protocol', '1', true);
  update core.brands set own_brand = p_own, updated_at = pg_catalog.now() where brand_id = p_brand_id;
  insert into ops.brand_flag_events (brand_id, from_own, to_own, actor_id, request_id, changed, reason, db_user) values (p_brand_id, v_b.own_brand, p_own, p_actor_id, p_request_id, true, p_reason, v_db_user);
  perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'brand_id', v_b.brand_id::text, 'name', v_b.name, 'own_brand', p_own, 'changed', true);
end $$;
revoke all on function ops.set_brand_own(uuid, text, bigint, boolean, boolean, text) from public;

-- ═══════════ 3. 商品に付ける (ブランド・段階の共通の確かめ) ═══════════
/**
 * 書いてよい単品の商品の行を返す (持ち主だけの部品・security definer の関数の中から呼ぶ)。メーカーの ops.set_product_maker と同じ決まり:
 *   単品・会社 1・やめていない (cancelled でない)・門を通った保存・登録の取引の中 (sku_page = この取引の今の約束 sku_edit・この SKU /
 *   register・variation = この取引でこの SKU を登録した約束 sku_create の行 = この取引の中で作られた行)。鍵 = SKU の鍵 → 商品の行
 */
create function ops.brand_life_target(p_sku_id bigint, p_via text) returns bigint
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  s      record;
  v_sess ops.master_write_sessions;
begin
  if (select c.phase from ops.master_cutover_state c where c.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  select k.sku_id, k.company_id, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' or s.product_id is null then
    raise exception 'not_single: ブランド・商品の段階は単品の商品だけ (SKU %・セットは構成品から)', p_sku_id using errcode = '22023';
  end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  if p_via = 'sku_page' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'sku_edit' or v_sess.sku_id is distinct from p_sku_id then
      raise exception 'master_write_session_required: 商品の画面のブランド・商品の段階は、同じ取引でこの SKU の保存 (ops.begin_master_write) を始めた後だけ' using errcode = '42501';
    end if;
  elsif not exists (select 1 from ops.master_write_sessions w where w.txid = pg_catalog.txid_current() and w.created_at >= pg_catalog.now() and w.operation = 'sku_create' and w.sku_id = p_sku_id) then
    raise exception 'master_write_session_required: 新商品のブランド・商品の段階は、その SKU を登録した同じ取引の中だけ (ops.register_new_sku の後)' using errcode = '42501';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform 1 from core.products p where p.product_id = s.product_id for update;
  return s.product_id;
end $$;
revoke all on function ops.brand_life_target(bigint, text) from public;

/**
 * 商品のブランドを付ける / 変える / 外す。p_brand_id = 付けるブランド (null = 外す)・p_seen_brand_id = 画面が見ていた今のブランド (新商品 = いつも null)。
 * 戻り値 = { ok, sku_id, product_id, brand_id, brand_name, own_brand, from_brand_id, from_brand_name, changed }
 * 断る: invalid_input / before_cutover / not_single / cancelled / brand_unknown / master_write_session_required / version_conflict
 */
create function ops.set_product_brand(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_brand_id bigint, p_seen_brand_id bigint, p_via text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_pid  bigint;
  v_b    core.brands;
  v_cur  core.product_brands;
  v_from text;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'variation', 'sku_page') then raise exception 'invalid_input: どこから は register / variation / sku_page' using errcode = '22023'; end if;
  if p_via in ('register', 'variation') and p_seen_brand_id is not null then raise exception 'invalid_input: 新商品の登録に「見ていたブランド」は無い' using errcode = '22023'; end if;
  v_pid := ops.brand_life_target(p_sku_id, p_via);
  if p_brand_id is not null then
    select * into v_b from core.brands b where b.brand_id = p_brand_id for share;
    if v_b.brand_id is null or v_b.company_id <> 1 or not v_b.active then
      raise exception 'brand_unknown: ブランド % は名簿に無いか、使わないブランド', p_brand_id using errcode = '22023';
    end if;
  end if;
  select * into v_cur from core.product_brands x where x.product_id = v_pid;
  if v_cur.brand_id is distinct from p_seen_brand_id then
    raise exception 'version_conflict: 画面を開いた後にこの商品のブランドが変わった (今 %・開いたとき %)', coalesce(v_cur.brand_id::text, 'なし'), coalesce(p_seen_brand_id::text, 'なし') using errcode = '40001';
  end if;
  v_from := (select b.name from core.brands b where b.brand_id = v_cur.brand_id);
  if v_cur.brand_id is not distinct from p_brand_id then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', v_pid::text, 'brand_id', p_brand_id::text, 'brand_name', v_b.name, 'own_brand', v_b.own_brand,
      'from_brand_id', v_cur.brand_id::text, 'from_brand_name', v_from, 'changed', false);
  end if;
  perform pg_catalog.set_config('ops.brand_life_protocol', '1', true);
  if p_brand_id is null then
    delete from core.product_brands x where x.product_id = v_pid;
  else
    insert into core.product_brands (product_id, company_id, brand_id, set_by, set_via, request_id) values (v_pid, 1, p_brand_id, p_actor_id, p_via, p_request_id)
      on conflict (product_id) do update set brand_id = excluded.brand_id, set_by = excluded.set_by, set_via = excluded.set_via, request_id = excluded.request_id, set_at = pg_catalog.now();
  end if;
  insert into ops.product_brand_events (company_id, product_id, sku_id, from_brand_id, to_brand_id, via, actor_id, request_id, reason, db_user)
    values (1, v_pid, p_sku_id, v_cur.brand_id, p_brand_id, p_via, p_actor_id, p_request_id, p_reason, v_db_user);
  perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', v_pid::text, 'brand_id', p_brand_id::text, 'brand_name', v_b.name, 'own_brand', v_b.own_brand,
    'from_brand_id', v_cur.brand_id::text, 'from_brand_name', v_from, 'changed', true);
end $$;
revoke all on function ops.set_product_brand(uuid, text, bigint, bigint, bigint, text, text) from public;

/**
 * 商品の区分・終売の予定日を決める (両方の「保存した後の値」を渡す = 片方だけ変えるときも今のもう片方を渡す)。
 *   p_stage = staple / seasonal / trial / null (なし)・p_end = 終売の予定日 / null (なし)・p_seen_* = 画面が見ていた今の値 (新商品 = どちらも null)。
 *   両方 null = 行を消す (区分も予定日も無い)。戻り値 = { ok, sku_id, product_id, stage, end_planned_on, from_stage, from_end, changed }
 * 断る: invalid_input / before_cutover / not_single / cancelled / master_write_session_required / version_conflict (今の値が見ていた値と違う)
 */
create function ops.set_product_lifecycle(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_stage text, p_end date, p_seen_stage text, p_seen_end date, p_via text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_pid bigint;
  v_cur core.product_lifecycle;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'variation', 'sku_page') then raise exception 'invalid_input: どこから は register / variation / sku_page' using errcode = '22023'; end if;
  if p_via in ('register', 'variation') and (p_seen_stage is not null or p_seen_end is not null) then raise exception 'invalid_input: 新商品の登録に「見ていた値」は無い' using errcode = '22023'; end if;
  if p_stage is not null and p_stage not in ('staple', 'seasonal', 'trial') then raise exception 'invalid_input: 区分は staple / seasonal / trial' using errcode = '22023'; end if;
  if p_end is not null and (p_end < date '2000-01-01' or p_end > date '2099-12-31') then raise exception 'invalid_input: 終売の予定日は 2000-01-01〜2099-12-31' using errcode = '22023'; end if;
  v_pid := ops.brand_life_target(p_sku_id, p_via);
  select * into v_cur from core.product_lifecycle x where x.product_id = v_pid;
  if v_cur.stage is distinct from p_seen_stage or v_cur.end_planned_on is distinct from p_seen_end then
    raise exception 'version_conflict: 画面を開いた後にこの商品の区分・終売の予定日が変わった' using errcode = '40001';
  end if;
  if v_cur.stage is not distinct from p_stage and v_cur.end_planned_on is not distinct from p_end then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', v_pid::text, 'stage', p_stage, 'end_planned_on', p_end::text,
      'from_stage', v_cur.stage, 'from_end', v_cur.end_planned_on::text, 'changed', false);
  end if;
  perform pg_catalog.set_config('ops.brand_life_protocol', '1', true);
  if p_stage is null and p_end is null then
    delete from core.product_lifecycle x where x.product_id = v_pid;
  else
    insert into core.product_lifecycle (product_id, company_id, stage, end_planned_on, set_by, set_via, request_id) values (v_pid, 1, p_stage, p_end, p_actor_id, p_via, p_request_id)
      on conflict (product_id) do update set stage = excluded.stage, end_planned_on = excluded.end_planned_on, set_by = excluded.set_by, set_via = excluded.set_via,
        request_id = excluded.request_id, set_at = pg_catalog.now();
  end if;
  insert into ops.product_lifecycle_events (company_id, product_id, sku_id, from_stage, to_stage, from_end, to_end, via, actor_id, request_id, reason, db_user)
    values (1, v_pid, p_sku_id, v_cur.stage, p_stage, v_cur.end_planned_on, p_end, p_via, p_actor_id, p_request_id, p_reason, v_db_user);
  perform pg_catalog.set_config('ops.brand_life_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', v_pid::text, 'stage', p_stage, 'end_planned_on', p_end::text,
    'from_stage', v_cur.stage, 'from_end', v_cur.end_planned_on::text, 'changed', true);
end $$;
revoke all on function ops.set_product_lifecycle(uuid, text, bigint, text, date, text, date, text, text) from public;

-- ═══════════ 4. 初めて売った日 (発売日) ═══════════
/**
 * SKU ごとの初めて売った日 = 次の 2 つのいちばん早い日 (人は入れない・Company DB の記録から毎回出す):
 *   ① NE の出荷 (core.shipments × shipment_lines): 出荷した (shipped_at あり)・取り消していない伝票と明細・数 > 0。日 = 受注日 (無ければ出荷の日)。
 *      NE はセットを構成品に分けて持つ = 単品のコードの行。対象外の店 (core.ne_shops の platform = '_ignore' = FBA 納品・ライジング Amazon) は数えない (売ったのではない)。卸の店は数える
 *   ② モールの注文 (core.orders × order_lines・FBA のお客の注文はここだけ): 取り消していない注文・数 > 取消。日 = 注文日。
 *      明細の SKU がこの SKU か、この SKU を構成品に持つセット (core.sku_components の 1 段) なら数える。出品の構成 (core.listing_components) へ進むのは
 *      SKU の分からない明細だけ (mart.sales_expanded_to_skus と同じ・#brand Codex R1 M: SKU の分かる明細を出品の今の構成で読み替えない)
 *   戻り値 = (sku_id, first_date, via = 'shipment' / 'order', detail = 店の名前 / モール)。売れていない SKU は行が無い。p_sku_ids は 5,000 個まで
 *   🚨 記録の始まり (出荷 = 2025-01 から・ops.sales_records_from) より前に売っていた商品は、記録の始まりの近くの日になる (画面が「これより前かも」と言う)
 */
create function ops.sku_first_sold(p_sku_ids bigint[])
  returns table (sku_id bigint, first_date date, via text, detail text)
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_sku_ids is null then raise exception 'invalid_input: sku_ids が要る' using errcode = '22023'; end if;
  if pg_catalog.cardinality(p_sku_ids) > 5000 then raise exception 'invalid_input: sku_ids は 5,000 個まで' using errcode = '22023'; end if;
  return query
  with ids as (select distinct x as id from pg_catalog.unnest(p_sku_ids) as t(x) where x is not null),
  tg as (   -- want = 知りたい SKU・hit = 明細に出る SKU (自分か、自分を構成品に持つセット)
    select i.id as want, i.id as hit from ids i
    union
    select c.child_sku_id, c.parent_sku_id from core.sku_components c where c.child_sku_id in (select i.id from ids i)
  ),
  sh as (
    select distinct on (sl.sku_id) sl.sku_id as want, coalesce(s.order_date_jst, s.ship_date_jst) as d, coalesce(n.shop_name, s.shop_code, '') as detail
      from core.shipment_lines sl
      join core.shipments s on s.shipment_id = sl.shipment_id
      left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code
     where sl.sku_id in (select i.id from ids i) and sl.removed_at is null and not sl.is_cancelled and sl.qty > 0
       and s.company_id = 1 and not s.is_cancelled and s.shipped_at is not null and coalesce(n.platform, '') <> '_ignore'
     order by sl.sku_id, coalesce(s.order_date_jst, s.ship_date_jst), s.shipment_id
  ),
  od as (
    select tg.want, o.order_date_jst as d, o.mall, o.order_id
      from tg join core.order_lines ol on ol.sku_id = tg.hit and ol.removed_at is null and ol.qty > ol.cancelled_qty
      join core.orders o on o.order_id = ol.order_id and o.company_id = 1 and not o.is_cancelled
    union all
    select tg.want, o.order_date_jst, o.mall, o.order_id
      from tg join core.listing_components lc on lc.sku_id = tg.hit
      join core.order_lines ol on ol.listing_id = lc.listing_id and ol.sku_id is null and ol.removed_at is null and ol.qty > ol.cancelled_qty
      join core.orders o on o.order_id = ol.order_id and o.company_id = 1 and not o.is_cancelled
  ),
  od1 as (select distinct on (od.want) od.want, od.d, od.mall as detail from od order by od.want, od.d, od.order_id),
  allx as (
    select sh.want, sh.d, 'shipment'::text as via, sh.detail from sh where sh.d is not null
    union all
    select od1.want, od1.d, 'order'::text, od1.detail from od1 where od1.d is not null
  )
  select distinct on (a.want) a.want, a.d, a.via, a.detail from allx a order by a.want, a.d, a.via;   -- 同じ日 = 注文 (order) を先に
end $$;
revoke all on function ops.sku_first_sold(bigint[]) from public;

/** 記録の始まりの日 (画面の「記録: 出荷 YYYY-MM-DD〜・モールの注文 YYYY-MM-DD〜」) = (src = 'shipment' / 'order', first_date)。記録が無い = その行は null の日 */
create function ops.sales_records_from() returns table (src text, first_date date)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select 'shipment'::text, (select pg_catalog.min(s.ship_date_jst) from core.shipments s where s.company_id = 1)
  union all
  select 'order'::text, (select pg_catalog.min(o.order_date_jst) from core.orders o where o.company_id = 1)
$$;
revoke all on function ops.sales_records_from() from public;
