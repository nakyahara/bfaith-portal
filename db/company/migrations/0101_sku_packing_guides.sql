-- 0101 (仮番号・0100 の次。マージの時に次の空き番号に付け替える。migrate は欠番を断る): 商品 (単品) の梱包指示 = 入れ方の写真 (3 枚まで・順番と一言) と注意点の文
--   (中原さん 2026-10-23「梱包が複雑になる商品が増えてきたので、どう袋や箱に入れたかの写真を 3 枚ぐらいと、梱包時の注意点を入れたい。
--    商品マスター (master-edit) からも、梱包システムからカメラでそのまま登録したい」)
--
-- 決めたこと (10/23 中原さん・Codex と Claude のすり合わせの後):
--   - 写真は「入れ方の例」。使う資材は梱包の資材カード (梱包アプリの SQLite) が正 = ここに資材の項目は持たない
--   - 現場で撮った写真も責任者の確認なしですぐ使う (記録は残す = 戻せる)
--   - 写真は Company DB に入れる (bytea・アプリが長辺 1280px の JPEG に作り直してから渡す・1 枚 1.5 MB まで)。
--     🚨 バックアップ (apps/company-db/backup/dump.mjs) は bytea の列のある表を小分けに読む・入れる (この PR で直した)
--   - 単品だけ (NE を通った時点でセットは構成品に分かれる = 梱包にセットの行は来ない)。セット・例外の SKU には持たせない
--   - 梱包アプリ (miniPC の SQLite) は読むだけの写し。正はここ
--
-- なにを:
--   1. core.sku_packing_guides = SKU ごとの注意点の文 (1 行 = 1 SKU・行が無い = まだ何も無い)
--   2. core.sku_packing_photos = 写真 (中身は変えない。外した写真も行は残す = 前の版を見られる。位置 1〜3 = 今の写真・null = 外した)
--   3. ops.sku_packing_guide_events = 写真を足した・指示を保存したの記録 (追記だけ・request_id で一意 = 押し直しは前の答え)。SKU の最後の記録の番号 = 画面が見ていた版
--   4. ops.add_sku_packing_photo(...) = 写真を 1 枚足す (空いている次の位置へ・3 枚あれば断る。版は見ない = 足すのはほかの人の変更とぶつからない)
--   5. ops.set_sku_packing_guide(...) = 注意点の文と写真の並び・一言を保存する (並びに無い今の写真は外す。画面が見ていた版と違えば version_conflict)
-- 守り: 0087 (梱包の大きさ・重さ) と同じ = 表は関数の中でだけ書く (印 ops.packing_guide_protocol)・記録は追記だけ・TRUNCATE できない・写真の中身は変えない。
--   切替の門 (new_open・ops.begin_master_write) は見ない = NE に送らない社内だけの値 (発注の設定と同じく、いつでも)。書ける人はアプリが決める
--   (商品の画面 = 名簿 MASTER_EDITORS の人 / 梱包 = 梱包アプリの作業者。人の欄は 0053 の ops.reg_actor_problem の形 = メールでなくてよい)
-- 🚨 既にある表・関数は変えない (足すだけ)。security definer = search_path = pg_catalog, pg_temp・public の実行権を外す。
--    画面のロール (master_edit) の権限は scripts/company-db/create-master-edit-roles.mjs を流し直して付ける (流すまで画面は欄を出さない)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regclass('core.product_packages') is null or pg_catalog.to_regprocedure('ops.reg_actor_problem(text, text)') is null then
    raise exception '0101 の前提: 0087 (梱包の大きさ・重さ)・0053 (人の形の確かめ) が無い';
  end if;
  if pg_catalog.to_regclass('core.sku_packing_guides') is not null then raise exception '0101: core.sku_packing_guides がもうある'; end if;
end $$;

-- ═══════════ 1. 表 ═══════════
create table core.sku_packing_guides (
  sku_id      bigint primary key references core.skus,
  company_id  smallint not null default 1 references core.companies,
  note        text not null default '' check (pg_catalog.length(note) <= 2000 and note !~ '[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]'),
  updated_by  text not null,
  updated_via text not null check (updated_via in ('sku_page', 'packing')),
  updated_at  timestamptz not null default now(),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
comment on table core.sku_packing_guides is '🆕 0101: 商品 (単品の SKU) の梱包時の注意点の文 (行が無い = まだ何も無い・空の文 = 注意点なし)。書くのは ops.set_sku_packing_guide / ops.add_sku_packing_photo だけ。写真は core.sku_packing_photos';

create table core.sku_packing_photos (
  photo_id   bigint generated always as identity primary key,
  sku_id     bigint not null references core.skus,
  company_id smallint not null default 1 references core.companies,
  position   smallint check (position between 1 and 3),   -- 今の写真の並び (1 = 最初)・null = 外した
  caption    text not null default '' check (pg_catalog.length(caption) <= 100 and caption !~ '[[:cntrl:]]'),
  image      bytea not null check (pg_catalog.octet_length(image) between 100 and 1500000),
  sha256     text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  width      integer not null check (width between 1 and 4000),
  height     integer not null check (height between 1 and 4000),
  added_by   text not null,
  added_via  text not null check (added_via in ('sku_page', 'packing')),
  added_at   timestamptz not null default now(),
  removed_by text,
  removed_at timestamptz,
  constraint ck_sku_packing_photos_removed check ((position is null) = (removed_at is not null) and (removed_at is null) = (removed_by is null)),
  -- 並べ替えは 1 つの取引の中で位置を入れ替える = 終わりで確かめる (null = 外した写真は何枚でもよい)
  constraint ux_sku_packing_photos_position unique (sku_id, position) deferrable initially deferred,
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_sku_packing_photos_sku on core.sku_packing_photos (sku_id) where position is not null;
comment on table core.sku_packing_photos is '🆕 0101: 梱包指示の写真 (JPEG・長辺 1280px まで・アプリが作り直したもの)。中身 (image・sha256・大きさ) は変えない。外した写真も行は残す (position = null)';

create table ops.sku_packing_guide_events (
  event_id   bigint generated always as identity primary key,
  company_id smallint not null default 1 references core.companies,
  sku_id     bigint not null references core.skus,
  action     text not null check (action in ('add_photo', 'set_guide')),
  from_value jsonb,   -- 前の指示 { note, photos: [{ photo_id, position, caption, sha256 }] } (無かった = null)
  to_value   jsonb not null,   -- 後の指示 (同じ形)
  via        text not null check (via in ('sku_page', 'packing')),
  actor_id   text not null,
  request_id uuid not null unique,
  db_user    text not null,
  created_at timestamptz not null default now()
);
create index ix_sku_packing_guide_events_sku on ops.sku_packing_guide_events (sku_id, event_id desc);
comment on table ops.sku_packing_guide_events is '🆕 0101: 梱包指示の写真を足した・保存した記録 (追記だけ)。SKU の最後の記録の番号 = 画面が見ていた版。request_id で一意 = 押し直しは前の答え';
revoke all on core.sku_packing_guides, core.sku_packing_photos, ops.sku_packing_guide_events from public;

-- 守り: 書くのは関数の中だけ (印 ops.packing_guide_protocol)・記録は追記だけ・写真の中身は変えない・消さない・TRUNCATE できない
create function ops.guard_packing_guide_tables() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_table_name = 'sku_packing_guide_events' and tg_op <> 'INSERT' then
    raise exception 'sku_packing_guide_events: 記録は変えない・消さない (追記だけ)' using errcode = '42501';
  end if;
  if tg_table_name = 'sku_packing_photos' and tg_op = 'DELETE' then
    raise exception 'sku_packing_photos: 写真の行は消さない (外す = position を null に)' using errcode = '42501';
  end if;
  -- 写真の表の列は写真の表のときだけ読む (plpgsql は and を先で打ち切らない = ほかの表の行で new.photo_id を読むと落ちる)
  if tg_table_name = 'sku_packing_photos' and tg_op = 'UPDATE' then
    if (new.photo_id, new.sku_id, new.company_id, new.image, new.sha256, new.width, new.height, new.added_by, new.added_via, new.added_at)
       is distinct from (old.photo_id, old.sku_id, old.company_id, old.image, old.sha256, old.width, old.height, old.added_by, old.added_via, old.added_at) then
      raise exception 'sku_packing_photos: 写真の中身は変えない (変えられるのは並び・一言・外したか)' using errcode = '42501';
    end if;
    if old.position is null and new.position is not null then
      raise exception 'sku_packing_photos: 外した写真は戻さない (撮り直すか、もう一度足す)' using errcode = '42501';
    end if;
  end if;
  if coalesce(pg_catalog.current_setting('ops.packing_guide_protocol', true), '') is distinct from '1' then
    raise exception '%: 梱包指示の表は ops.add_sku_packing_photo / ops.set_sku_packing_guide の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_packing_guide_tables() from public;
create trigger trg_sku_packing_guides_guard before insert or update or delete on core.sku_packing_guides for each row execute function ops.guard_packing_guide_tables();
create trigger trg_sku_packing_photos_guard before insert or update or delete on core.sku_packing_photos for each row execute function ops.guard_packing_guide_tables();
create trigger trg_sku_packing_guide_events_guard before insert or update or delete on ops.sku_packing_guide_events for each row execute function ops.guard_packing_guide_tables();
create trigger trg_sku_packing_guides_no_truncate before truncate on core.sku_packing_guides for each statement execute function core.reject_mutation();
create trigger trg_sku_packing_photos_no_truncate before truncate on core.sku_packing_photos for each statement execute function core.reject_mutation();
create trigger trg_sku_packing_guide_events_no_truncate before truncate on ops.sku_packing_guide_events for each statement execute function core.reject_mutation();

-- ═══════════ 2. 部品 (持ち主だけ) ═══════════
/** 今の指示の形 { note, photos: [{ photo_id, position, caption, sha256 }] } (行も写真も無い = null)。記録と戻り値に使う */
create function ops.packing_guide_state(p_sku_id bigint) returns jsonb language sql stable set search_path = pg_catalog, pg_temp as $$
  select case when g.sku_id is null and ph.list is null then null
              else pg_catalog.jsonb_build_object('note', coalesce(g.note, ''), 'photos', coalesce(ph.list, '[]'::jsonb)) end
    from (select 1) one
    left join core.sku_packing_guides g on g.sku_id = p_sku_id
    left join lateral (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('photo_id', p.photo_id::text, 'position', p.position, 'caption', p.caption, 'sha256', p.sha256) order by p.position) as list
                         from core.sku_packing_photos p where p.sku_id = p_sku_id and p.position is not null) ph on true
$$;
revoke all on function ops.packing_guide_state(bigint) from public;

/**
 * 共通の確かめ (形・単品・会社 1・登録をやめていない)。戻り値 = 実際の db_user の名前。
 * 断る: invalid_input / not_single / cancelled
 */
create function ops.packing_guide_check(p_request_id uuid, p_actor_id text, p_via text, p_sku_id bigint) returns text
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  s record;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null then raise exception 'invalid_input: 人の形が違う' using errcode = '22023'; end if;
  if p_via is null or p_via not in ('sku_page', 'packing') then raise exception 'invalid_input: どこから は sku_page / packing' using errcode = '22023'; end if;
  select k.sku_id, k.company_id, k.sku_kind, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' then
    raise exception 'not_single: 梱包指示は単品の商品だけ (SKU %・セットは梱包の時に構成品に分かれている)', p_sku_id using errcode = '22023';
  end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  return case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
end $$;
revoke all on function ops.packing_guide_check(uuid, text, text, bigint) from public;

-- ═══════════ 3. 写真を足す ═══════════
/**
 * p_image = JPEG (アプリが作り直したもの・100 B〜1.5 MB)・p_width / p_height = その大きさ (px・1〜4000)・p_caption = 一言 (100 字まで・空でよい)
 * 版は見ない (足すのはほかの人の変更とぶつからない)。空いている次の位置へ。押し直し (同じ request_id) = 前の答え
 * 戻り値 = { ok, sku_id, photo_id, position, sha256, event_id, replay }
 * 断る: invalid_input / not_single / cancelled / photos_full (もう 3 枚ある) / request_reused (同じ request_id をほかの用に使った)
 */
create function ops.add_sku_packing_photo(p_request_id uuid, p_actor_id text, p_via text, p_sku_id bigint, p_image bytea, p_width integer, p_height integer, p_caption text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text;
  v_prev    ops.sku_packing_guide_events;
  v_from    jsonb;
  v_to      jsonb;
  v_pos     smallint;
  v_photo   bigint;
  v_sha     text;
  v_event   bigint;
  v_caption text := pg_catalog.btrim(coalesce(p_caption, ''));
begin
  v_db_user := ops.packing_guide_check(p_request_id, p_actor_id, p_via, p_sku_id);
  if p_image is null or pg_catalog.octet_length(p_image) not between 100 and 1500000 then raise exception 'invalid_input: 写真は 100 B〜1.5 MB' using errcode = '22023'; end if;
  if pg_catalog.substring(p_image, 1, 3) <> '\xffd8ff'::bytea then raise exception 'invalid_input: 写真は JPEG で渡す (アプリが作り直す)' using errcode = '22023'; end if;
  if p_width is null or p_height is null or p_width not between 1 and 4000 or p_height not between 1 and 4000 then raise exception 'invalid_input: 写真の大きさの形が違う' using errcode = '22023'; end if;
  if pg_catalog.length(v_caption) > 100 or v_caption ~ '[[:cntrl:]]' then raise exception 'invalid_input: 一言は 100 字まで (改行なし)' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  -- 押し直し: 同じ request_id の記録があれば、その答えを返す (別の SKU・別の用 = 断る)
  select * into v_prev from ops.sku_packing_guide_events e where e.request_id = p_request_id;
  if v_prev.event_id is not null then
    if v_prev.sku_id <> p_sku_id or v_prev.action <> 'add_photo' then raise exception 'request_reused: この request_id はほかの操作に使った' using errcode = '22023'; end if;
    select p.photo_id, p.position, p.sha256 into v_photo, v_pos, v_sha from core.sku_packing_photos p
     where p.photo_id = (v_prev.to_value ->> 'added_photo_id')::bigint;
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'photo_id', v_photo::text, 'position', v_pos, 'sha256', v_sha, 'event_id', v_prev.event_id::text, 'replay', true);
  end if;
  -- 足す位置 = 今の並びのいちばん後ろ (保存はいつも 1 から詰める = 間は空かない)
  v_pos := coalesce((select max(p.position) from core.sku_packing_photos p where p.sku_id = p_sku_id and p.position is not null), 0) + 1;
  if v_pos > 3 then raise exception 'photos_full: 写真はもう 3 枚ある (どれかを外してから足す)' using errcode = 'P0001'; end if;
  v_from := ops.packing_guide_state(p_sku_id);
  v_sha := pg_catalog.encode(pg_catalog.sha256(p_image), 'hex');
  perform pg_catalog.set_config('ops.packing_guide_protocol', '1', true);
  insert into core.sku_packing_photos (sku_id, company_id, position, caption, image, sha256, width, height, added_by, added_via)
    values (p_sku_id, 1, v_pos, v_caption, p_image, v_sha, p_width, p_height, p_actor_id, p_via) returning photo_id into v_photo;
  insert into core.sku_packing_guides (sku_id, company_id, note, updated_by, updated_via)
    values (p_sku_id, 1, '', p_actor_id, p_via)
    on conflict (sku_id) do update set updated_by = excluded.updated_by, updated_via = excluded.updated_via, updated_at = pg_catalog.now();
  v_to := ops.packing_guide_state(p_sku_id) || pg_catalog.jsonb_build_object('added_photo_id', v_photo::text, 'added_sha256', v_sha);
  insert into ops.sku_packing_guide_events (company_id, sku_id, action, from_value, to_value, via, actor_id, request_id, db_user)
    values (1, p_sku_id, 'add_photo', v_from, v_to, p_via, p_actor_id, p_request_id, v_db_user) returning event_id into v_event;
  perform pg_catalog.set_config('ops.packing_guide_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'photo_id', v_photo::text, 'position', v_pos, 'sha256', v_sha, 'event_id', v_event::text, 'replay', false);
end $$;
revoke all on function ops.add_sku_packing_photo(uuid, text, text, bigint, bytea, integer, integer, text) from public;

-- ═══════════ 4. 指示を保存する ═══════════
/**
 * p_seen_event_id = 画面が見ていた最後の記録の番号 (無い = null)・p_note = 注意点の文 (2000 字まで・空 = なし)・
 * p_photos = 残す今の写真の並び [{ "photo_id": "<番号>", "caption": "<一言>" }, ...] (3 つまで・並び = 位置 1, 2, 3・無い今の写真は外す)
 * 戻り値 = { ok, sku_id, changed, event_id, replay, guide }
 * 断る: invalid_input / not_single / cancelled / request_reused / unknown_photo (この SKU の今の写真でない) / version_conflict (画面を開いた後に変わった)
 */
create function ops.set_sku_packing_guide(p_request_id uuid, p_actor_id text, p_via text, p_sku_id bigint, p_seen_event_id bigint, p_note text, p_photos jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text;
  v_prev    ops.sku_packing_guide_events;
  v_last    bigint;
  v_from    jsonb;
  v_to      jsonb;
  v_event   bigint;
  v_note    text := coalesce(p_note, '');
  v_ids     bigint[];
  v_caps    text[];
  v_item    jsonb;
  v_cap     text;
  i         integer;
begin
  v_db_user := ops.packing_guide_check(p_request_id, p_actor_id, p_via, p_sku_id);
  -- 改行は残す (\n・\r・タブだけ許す)
  v_note := pg_catalog.regexp_replace(v_note, '\r\n?', E'\n', 'g');
  if pg_catalog.length(v_note) > 2000 or v_note ~ '[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]' then raise exception 'invalid_input: 注意点は 2000 字まで' using errcode = '22023'; end if;
  if pg_catalog.btrim(v_note, E' \t\n') = '' then v_note := ''; end if;
  if p_photos is null or pg_catalog.jsonb_typeof(p_photos) <> 'array' or pg_catalog.jsonb_array_length(p_photos) > 3 then
    raise exception 'invalid_input: 写真の並びは 3 つまでの配列' using errcode = '22023';
  end if;
  v_ids := '{}'; v_caps := '{}';
  for v_item in select x from pg_catalog.jsonb_array_elements(p_photos) as t(x) loop
    if pg_catalog.jsonb_typeof(v_item) <> 'object' or coalesce(v_item ->> 'photo_id', '') !~ '^[1-9][0-9]{0,17}$' then
      raise exception 'invalid_input: 写真の並びの形が違う' using errcode = '22023';
    end if;
    v_cap := pg_catalog.btrim(coalesce(v_item ->> 'caption', ''));
    if pg_catalog.length(v_cap) > 100 or v_cap ~ '[[:cntrl:]]' then raise exception 'invalid_input: 一言は 100 字まで (改行なし)' using errcode = '22023'; end if;
    if (v_item ->> 'photo_id')::bigint = any (v_ids) then raise exception 'invalid_input: 同じ写真が 2 回ある' using errcode = '22023'; end if;
    v_ids := v_ids || (v_item ->> 'photo_id')::bigint;
    v_caps := v_caps || v_cap;
  end loop;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  select * into v_prev from ops.sku_packing_guide_events e where e.request_id = p_request_id;
  if v_prev.event_id is not null then
    if v_prev.sku_id <> p_sku_id or v_prev.action <> 'set_guide' then raise exception 'request_reused: この request_id はほかの操作に使った' using errcode = '22023'; end if;
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'changed', true, 'event_id', v_prev.event_id::text, 'replay', true, 'guide', v_prev.to_value);
  end if;
  v_last := (select max(e.event_id) from ops.sku_packing_guide_events e where e.sku_id = p_sku_id);
  if v_last is distinct from p_seen_event_id then
    raise exception 'version_conflict: 画面を開いた後にこの商品の梱包指示が変わった (今の記録 %・開いたとき %)', coalesce(v_last::text, 'なし'), coalesce(p_seen_event_id::text, 'なし') using errcode = '40001';
  end if;
  if exists (select 1 from pg_catalog.unnest(v_ids) as u(id)
              where not exists (select 1 from core.sku_packing_photos p where p.photo_id = u.id and p.sku_id = p_sku_id and p.position is not null)) then
    raise exception 'unknown_photo: この商品の今の写真でないものが並びにある' using errcode = '22023';
  end if;
  v_from := ops.packing_guide_state(p_sku_id);
  perform pg_catalog.set_config('ops.packing_guide_protocol', '1', true);
  -- 並びに無い今の写真を外す → 残す写真の位置と一言 (位置の一意は取引の終わりで確かめる)
  update core.sku_packing_photos p set position = null, removed_at = pg_catalog.now(), removed_by = p_actor_id
   where p.sku_id = p_sku_id and p.position is not null and not (p.photo_id = any (v_ids));
  for i in 1 .. coalesce(pg_catalog.array_length(v_ids, 1), 0) loop
    update core.sku_packing_photos p set position = i, caption = v_caps[i]
     where p.photo_id = v_ids[i] and (p.position, p.caption) is distinct from (i::smallint, v_caps[i]);
  end loop;
  -- 注意点の行 (まだ行が無くて文も空 = 作らない = 何も無いまま)
  if v_note <> '' or exists (select 1 from core.sku_packing_guides g where g.sku_id = p_sku_id) then
    insert into core.sku_packing_guides (sku_id, company_id, note, updated_by, updated_via)
      values (p_sku_id, 1, v_note, p_actor_id, p_via)
      on conflict (sku_id) do update set note = excluded.note, updated_by = excluded.updated_by, updated_via = excluded.updated_via, updated_at = pg_catalog.now()
      where core.sku_packing_guides.note is distinct from excluded.note;
  end if;
  v_to := ops.packing_guide_state(p_sku_id);
  if v_from is not distinct from v_to then
    perform pg_catalog.set_config('ops.packing_guide_protocol', '', true);
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'changed', false, 'event_id', v_last::text, 'replay', false, 'guide', v_to);
  end if;
  -- 写真の並び・一言だけ変えた = 注意点の行の「最後に変えた人」も新しくする
  update core.sku_packing_guides g set updated_by = p_actor_id, updated_via = p_via, updated_at = pg_catalog.now() where g.sku_id = p_sku_id;
  insert into ops.sku_packing_guide_events (company_id, sku_id, action, from_value, to_value, via, actor_id, request_id, db_user)
    values (1, p_sku_id, 'set_guide', v_from, v_to, p_via, p_actor_id, p_request_id, v_db_user) returning event_id into v_event;
  perform pg_catalog.set_config('ops.packing_guide_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'changed', true, 'event_id', v_event::text, 'replay', false, 'guide', v_to);
end $$;
revoke all on function ops.set_sku_packing_guide(uuid, text, text, bigint, bigint, text, jsonb) from public;
