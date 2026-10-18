-- 0084 (仮の番号・マージの時に付け替える。0083 = #1734。先に 0084 を使う PR があれば次の空き番号へ): メーカー (製造元) の名簿と、商品ごとのメーカー (中原さん 2026-10-17)
--   中原さん「マスタの入力で、仕入先とは別にメーカーの情報を入れたい。問屋から仕入れるとメーカー名と仕入先名が違う」
--   決まり: Company DB だけに持つ (NE・ロジザード・product-hub には送らない)・入れ方 = 名簿から選ぶ (探せる) + その場で新しいメーカーを足す・
--     「仕入先と同じ」= 代表の仕入先の名前のメーカー (名簿に無ければ足す)。単品だけ (セットのメーカーは構成品から)
--
-- なにを:
--   1. core.makers = メーカーの名簿 (名前・同じと見る形 name_norm = NFKC → 小文字 → ひらがなをカタカナ → 空白を除く・会社の中で一意)。
--      🚨 core.products.manufacturer (0002) は使わない: 夜間ロードの属性の解決 (apps/company-db/load/engine.mjs の 10. = product_hub / amazon_catalog の観測) が書く列 =
--      ポータルで入れた値を翌朝に上書きする / 消す道がある。持ち主の表 (config/master-ownership.mjs) にも無い。名簿の番号で別の表に持つ = 夜間ロードは触らない
--   2. core.product_makers = 商品 (単品の商品の行) ごとの今のメーカー (1 行 = 1 商品・無い = メーカーなし)
--   3. ops.product_maker_events = 付けた・変えた・外したの記録 (追記だけ・request_id と商品で一意)
--   4. ops.create_maker(request_id, 人, 名前) = 名簿に足す (同じと見る名前が既にある = それを返す = 重ねない)・押し直し = 前の答え
--   5. ops.set_product_maker(request_id, 人, sku_id, メーカー, 画面が見ていたメーカー, どこから, 理由) = 商品のメーカーを付ける / 変える / 外す
--      (新商品の登録・まとまりの登録 = 登録と同じ取引 / 商品の画面 = 保存と同じ取引)。単品・会社 1・やめていない・名簿にある有効なメーカー・
--      今のメーカーが画面の見ていたものと違う = version_conflict
-- 🚨 既にある表・関数は変えない (表と関数を足すだけ)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す。
--    画面のロール (master_edit) の権限 (表の select・2 つの関数の実行) は scripts/company-db/create-master-edit-roles.mjs を流し直して付ける (流すまで画面は欄を出さない)

-- ═══════════ 0. 名前の形 (JS = lib/master-maker.mjs と同じ決まり) ═══════════
/** 名前の掃除: 空白 (半角・全角・ノーブレーク) の続きを 1 つの半角の空白に・前後の空白を除く (表に残す名前) */
create function core.maker_name_clean(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.btrim(pg_catalog.regexp_replace(p, '[ 　 ]+', ' ', 'g'))
$$;
revoke all on function core.maker_name_clean(text) from public;
/**
 * 同じと見る形: NFKC (半角カナ → 全角・全角英数 → 半角) → 小文字 → ひらがな → カタカナ → 空白を除く
 *   (apps/master-edit/search-fold.mjs の foldSearch と同じ + 空白を除く。「ハチミツ本舗」「はちみつ 本舗」「ﾊﾁﾐﾂ本舗」は同じ)
 */
create function core.maker_name_norm(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.replace(pg_catalog.translate(pg_catalog.lower(normalize(p, NFKC)),
    'ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゕゖゝゞ',
    'ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶヽヾ'), ' ', '')
$$;
revoke all on function core.maker_name_norm(text) from public;
/** 名前の問題 (無い = null): empty (空) / too_long (100 字より長い) / control (改行・制御文字) */
create function ops.maker_name_problem(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when p is null or core.maker_name_clean(p) = '' or core.maker_name_norm(core.maker_name_clean(p)) = '' then 'empty'
              when p ~ '[[:cntrl:]]' or p ~ '[  ]' then 'control'
              when pg_catalog.char_length(core.maker_name_clean(p)) > 100 then 'too_long' end
$$;
revoke all on function ops.maker_name_problem(text) from public;

-- ═══════════ 1. 名簿・商品ごとのメーカー・記録 ═══════════
create table core.makers (
  maker_id           bigint generated always as identity primary key,
  company_id         smallint not null default 1 references core.companies,
  name               text not null,
  name_norm          text not null,
  active             boolean not null default true,
  created_by_type    text not null default 'human',
  created_by_id      text not null,
  created_request_id uuid not null unique,
  created_at         timestamptz not null default now(),
  constraint ck_makers_name check (name = core.maker_name_clean(name) and ops.maker_name_problem(name) is null),
  constraint ck_makers_norm check (name_norm = core.maker_name_norm(name)),
  unique (company_id, name_norm),
  unique (company_id, maker_id)
);
comment on table core.makers is '🆕 0084: メーカー (製造元) の名簿。Company DB だけに持つ (NE・ロジザード・product-hub には送らない)。同じと見る名前 (name_norm) は会社の中で 1 つ。書くのは ops.create_maker だけ';

create table core.product_makers (
  product_id bigint primary key references core.products,
  company_id smallint not null default 1 references core.companies,
  maker_id   bigint not null,
  set_by     text not null,
  set_via    text not null check (set_via in ('register', 'variation', 'sku_page')),
  request_id uuid not null,
  set_at     timestamptz not null default now(),
  foreign key (company_id, maker_id) references core.makers (company_id, maker_id)   -- メーカーも同じ会社
);
create index ix_product_makers_maker on core.product_makers (maker_id);
comment on table core.product_makers is '🆕 0084: 商品 (単品の商品の行) ごとの今のメーカー (行が無い = メーカーなし)。夜間ロードは触らない (core.products.manufacturer とは別)。書くのは ops.set_product_maker だけ';

create table ops.product_maker_events (
  event_id      bigint generated always as identity primary key,
  company_id    smallint not null default 1 references core.companies,
  product_id    bigint not null references core.products,
  sku_id        bigint not null references core.skus,
  from_maker_id bigint references core.makers,
  to_maker_id   bigint references core.makers,
  via           text not null check (via in ('register', 'variation', 'sku_page')),
  actor_id      text not null,
  request_id    uuid not null,
  reason        text,
  db_user       text not null,
  created_at    timestamptz not null default now(),
  constraint ck_pme_changed check (from_maker_id is distinct from to_maker_id),
  unique (request_id, product_id)
);
create index ix_product_maker_events_product on ops.product_maker_events (product_id, event_id desc);
comment on table ops.product_maker_events is '🆕 0084: 商品のメーカーを付けた・変えた・外した記録 (追記だけ・書くのは ops.set_product_maker だけ)';
revoke all on core.makers, core.product_makers, ops.product_maker_events from public;

-- 守り: 書くのは 2 つの関数の中だけ (印 ops.maker_protocol)・記録は追記だけ・TRUNCATE できない
create function ops.guard_maker_tables() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_table_name in ('product_maker_events', 'maker_requests') and tg_op <> 'INSERT' then
    raise exception 'product_maker_events: 記録は変えない・消さない (追記だけ)' using errcode = '42501';
  end if;
  if coalesce(pg_catalog.current_setting('ops.maker_protocol', true), '') is distinct from '1' then
    raise exception '%: メーカーの表は ops.create_maker / ops.set_product_maker の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_maker_tables() from public;
create trigger trg_makers_guard before insert or update or delete on core.makers for each row execute function ops.guard_maker_tables();
create trigger trg_product_makers_guard before insert or update or delete on core.product_makers for each row execute function ops.guard_maker_tables();
create trigger trg_product_maker_events_guard before insert or update or delete on ops.product_maker_events for each row execute function ops.guard_maker_tables();
create trigger trg_makers_no_truncate before truncate on core.makers for each statement execute function core.reject_mutation();
create trigger trg_product_makers_no_truncate before truncate on core.product_makers for each statement execute function core.reject_mutation();
create trigger trg_product_maker_events_no_truncate before truncate on ops.product_maker_events for each statement execute function core.reject_mutation();
-- 名簿に足す要求の控え (#maker Codex R1 Low: 既にあった名前を返した回も控える = 同じ request_id で違う名前を送っても新しく作らない)。追記だけ・関数の中だけ
create table ops.maker_requests (
  request_id uuid        primary key,
  actor_id   text        not null,
  name_norm  text        not null,
  maker_id   bigint      not null references core.makers,
  existed    boolean     not null,
  created_at timestamptz not null default now()
);
revoke all on ops.maker_requests from public;
comment on table ops.maker_requests is '🆕 0084: ops.create_maker の要求の控え (足した回・既にあった名前を返した回の両方)。同じ request_id = この答え・違う名前 / 人 = request_id_reused';
create trigger trg_maker_requests_guard before insert or update or delete on ops.maker_requests for each row execute function ops.guard_maker_tables();
create trigger trg_maker_requests_no_truncate before truncate on ops.maker_requests for each statement execute function core.reject_mutation();

-- ═══════════ 2. 名簿に足す ═══════════
/**
 * p_name = 人が打った名前 (掃除してから確かめる)。戻り値 = { ok, maker_id, name, existed, replayed? }
 *   existed = 同じと見る名前のメーカーが既にあった (足さずにそれを返す = 重ねない)。押し直し (同じ request_id・同じと見る名前・同じ人) = 控え (ops.maker_requests) の答え (replayed)
 * 断る: invalid_input (形・人) / before_cutover (切替の段階が new_open でない) / request_id_reused (同じ request_id で違う名前・人)
 */
create function ops.create_maker(p_request_id uuid, p_actor_id text, p_name text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_name text := core.maker_name_clean(p_name);
  v_norm text;
  v_prob text := ops.maker_name_problem(p_name);
  v_m    core.makers;
  v_q    ops.maker_requests;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if v_prob is not null then
    raise exception 'invalid_input: メーカーの名前 (%)', case v_prob when 'empty' then '空' when 'too_long' then '100 字まで' else '改行・制御文字は入れない' end using errcode = '22023';
  end if;
  if (select s.phase from ops.master_cutover_state s where s.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  v_norm := core.maker_name_norm(v_name);
  -- 鍵: 同じ request_id (同じ番号の要求は 1 つずつ) → 同じと見る名前 (2 人が同時に同じメーカーを足しても 1 つ)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.maker_request:' || p_request_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.maker:' || v_norm, 0));
  -- 押し直し = 控えの答え (足した回も・既にあった名前を返した回も)。違う名前・人 = 断る
  select * into v_q from ops.maker_requests r where r.request_id = p_request_id;
  if v_q.request_id is not null then
    if v_q.name_norm is distinct from v_norm or v_q.actor_id is distinct from p_actor_id then
      raise exception 'request_id_reused: request_id % はほかの名前・人で使われている', p_request_id using errcode = '23505';
    end if;
    select * into v_m from core.makers m where m.maker_id = v_q.maker_id;
    return pg_catalog.jsonb_build_object('ok', true, 'maker_id', v_m.maker_id::text, 'name', v_m.name, 'existed', v_q.existed, 'replayed', true);
  end if;
  perform pg_catalog.set_config('ops.maker_protocol', '1', true);
  select * into v_m from core.makers m where m.company_id = 1 and m.name_norm = v_norm;
  if v_m.maker_id is not null then
    insert into ops.maker_requests (request_id, actor_id, name_norm, maker_id, existed) values (p_request_id, p_actor_id, v_norm, v_m.maker_id, true);
    perform pg_catalog.set_config('ops.maker_protocol', '', true);
    return pg_catalog.jsonb_build_object('ok', true, 'maker_id', v_m.maker_id::text, 'name', v_m.name, 'existed', true, 'active', v_m.active);
  end if;
  insert into core.makers (company_id, name, name_norm, created_by_id, created_request_id) values (1, v_name, v_norm, p_actor_id, p_request_id) returning * into v_m;
  insert into ops.maker_requests (request_id, actor_id, name_norm, maker_id, existed) values (p_request_id, p_actor_id, v_norm, v_m.maker_id, false);
  perform pg_catalog.set_config('ops.maker_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'maker_id', v_m.maker_id::text, 'name', v_m.name, 'existed', false);
end $$;
revoke all on function ops.create_maker(uuid, text, text) from public;

-- ═══════════ 3. 商品のメーカー ═══════════
/**
 * p_maker_id = 付けるメーカー (null = 外す)・p_seen_maker_id = 画面が見ていた今のメーカー (無い = null。新商品の登録 = いつも null)・
 * p_via = register / variation (登録の取引の中) / sku_page (商品の画面の保存の取引の中)・p_reason = 記録に残す理由 (無くてよい)
 * 戻り値 = { ok, sku_id, product_id, maker_id, maker_name, from_maker_id, from_maker_name, changed }
 * 断る: invalid_input / before_cutover / not_single (セット・例外・商品の行の無い SKU) / cancelled (登録をやめた) / maker_unknown (名簿に無い・使わない) /
 *       master_write_session_required (門を通った保存・登録の取引の外) / version_conflict (今のメーカーが画面の見ていたものと違う)
 */
create function ops.set_product_maker(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_maker_id bigint, p_seen_maker_id bigint, p_via text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_mk      core.makers;
  v_cur     core.product_makers;
  v_from    text;
  v_sess    ops.master_write_sessions;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'variation', 'sku_page') then raise exception 'invalid_input: どこから は register / variation / sku_page' using errcode = '22023'; end if;
  if p_via in ('register', 'variation') and p_seen_maker_id is not null then raise exception 'invalid_input: 新商品の登録に「見ていたメーカー」は無い' using errcode = '22023'; end if;
  if (select c.phase from ops.master_cutover_state c where c.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  select k.sku_id, k.company_id, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' or s.product_id is null then
    raise exception 'not_single: メーカーは単品の商品だけ (SKU %・セットのメーカーは構成品から)', p_sku_id using errcode = '22023';
  end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  -- 守り: 門を通った保存・登録の取引の中だけ (直に呼んでも、段階・持ち主表・MASTER_EDIT_OPEN・新規開始の許可を見た約束が無ければ書かない)。
  --   sku_page = この取引の今の約束 (ops.begin_master_write = sku_edit・この SKU) / register・variation = この取引でこの SKU を登録した約束 (ops.register_new_sku = sku_create) の行
  if p_via = 'sku_page' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'sku_edit' or v_sess.sku_id is distinct from p_sku_id then
      raise exception 'master_write_session_required: 商品の画面のメーカーは、同じ取引でこの SKU の保存 (ops.begin_master_write) を始めた後だけ' using errcode = '42501';
    end if;
  -- 🆕 #maker Codex R2 M: 取引の番号だけでなく、約束の行がこの取引の中で作られたこと (created_at = 入れた時の clock_timestamp ≧ この取引の始まり now()) も見る
  --   (バックアップから戻した DB で古い約束の行の取引の番号が今と重なっても、過去の登録を今の登録と見ない)
  elsif not exists (select 1 from ops.master_write_sessions w where w.txid = pg_catalog.txid_current() and w.created_at >= pg_catalog.now() and w.operation = 'sku_create' and w.sku_id = p_sku_id) then
    raise exception 'master_write_session_required: 新商品のメーカーは、その SKU を登録した同じ取引の中だけ (ops.register_new_sku の後)' using errcode = '42501';
  end if;
  -- 鍵: SKU の鍵 → 商品の行 (商品の画面の保存・登録の取引はもう持っている = 同じ取引で取り直しても待たない)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform 1 from core.products p where p.product_id = s.product_id for update;
  if p_maker_id is not null then
    select * into v_mk from core.makers m where m.maker_id = p_maker_id for share;
    if v_mk.maker_id is null or v_mk.company_id <> 1 or not v_mk.active then
      raise exception 'maker_unknown: メーカー % は名簿に無いか、使わないメーカー', p_maker_id using errcode = '22023';
    end if;
  end if;
  select * into v_cur from core.product_makers x where x.product_id = s.product_id;
  if v_cur.maker_id is distinct from p_seen_maker_id then
    raise exception 'version_conflict: 画面を開いた後にこの商品のメーカーが変わった (今 %・開いたとき %)', coalesce(v_cur.maker_id::text, 'なし'), coalesce(p_seen_maker_id::text, 'なし') using errcode = '40001';
  end if;
  v_from := (select m.name from core.makers m where m.maker_id = v_cur.maker_id);
  if v_cur.maker_id is not distinct from p_maker_id then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'maker_id', p_maker_id::text, 'maker_name', v_mk.name,
      'from_maker_id', v_cur.maker_id::text, 'from_maker_name', v_from, 'changed', false);
  end if;
  perform pg_catalog.set_config('ops.maker_protocol', '1', true);
  if p_maker_id is null then
    delete from core.product_makers x where x.product_id = s.product_id;
  else
    insert into core.product_makers (product_id, company_id, maker_id, set_by, set_via, request_id) values (s.product_id, 1, p_maker_id, p_actor_id, p_via, p_request_id)
      on conflict (product_id) do update set maker_id = excluded.maker_id, set_by = excluded.set_by, set_via = excluded.set_via, request_id = excluded.request_id, set_at = pg_catalog.now();
  end if;
  insert into ops.product_maker_events (company_id, product_id, sku_id, from_maker_id, to_maker_id, via, actor_id, request_id, reason, db_user)
    values (1, s.product_id, p_sku_id, v_cur.maker_id, p_maker_id, p_via, p_actor_id, p_request_id, p_reason, v_db_user);
  perform pg_catalog.set_config('ops.maker_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'maker_id', p_maker_id::text, 'maker_name', v_mk.name,
    'from_maker_id', v_cur.maker_id::text, 'from_maker_name', v_from, 'changed', true);
end $$;
revoke all on function ops.set_product_maker(uuid, text, bigint, bigint, bigint, text, text) from public;
