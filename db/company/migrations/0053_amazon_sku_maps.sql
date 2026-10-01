-- 0053: Amazon SKU の対応 (seller SKU ↔ NE コード) を Company DB で直せるようにする (2026-10-02。Company DB構想 16「Amazon SKU の対応の編集」§2・§3・§7 v2 H5 / M7 / M10・
--       §8 契約 v3 (Codex 設計 R1 の High 4 = 墓標の物理削除を拒む・関連の列の全部の書き込みで不変条件を見る)。PR ⑦-1)
-- 🚨 前提 = 0051_master_edit.sql (⑤-1)・0052_master_registrations.sql (⑤-2a)。番号は今 0053 (④a #1564・⑤-2b #1571 も 0053 = 後でマージする方の番号を上げる)
--
-- なぜ: 今の正本は miniPC の m_sku_master + m_sku_components (seller SKU ごとの名前と構成)。切替 (⑥) の後は Company DB を正にして、
--   人がマスタ入力画面 (apps/master-edit の「Amazon SKU」) で直す。古い表への写し (⑦-2) は、この表から決まった並べ方 (lib/sku-map-canonical.js・sku-map-canon-v1) を作る。
--   構成の正本は core.listing_components のまま (Company DB の読み手 0007・0042・0049 を変えない)。
-- なにを:
--   1. core.amazon_sku_maps = 1 行 = 1 つの対応 (出品 1 つに 1 行)。state = active / deleted (墓標)。origin = legacy (切替の日の移行) / portal (画面)。
--      registered_at = 写しの親の created_at・changed_at = 親の updated_at (timestamptz(3) = ミリ秒。文字 → 時刻 → 文字で元に戻る)。
--      変更の記録・version・出品の version は 0026 の 3 つのトリガー (entity_type = 'amazon_sku_map')
--   2. 墓標は消さない (契約 v3 High 4): BEFORE DELETE / TRUNCATE の trigger がいつも拒む。画面のロールに DELETE は渡さない
--      (表の持ち主は復元 (apps/company-db/backup/dump.mjs = ユーザーの trigger を止めて消して入れ直す) のために DELETE の権限を持ったまま = 普通の DELETE は trigger が拒む)。
--      削除 = active → deleted (理由・人・時刻・構成を全部消す を 1 つの取引で)。同じ SKU の再登録は deleted → active (登録日は新しくなる)
--      🚨 残る危うさ (Codex #1586 R1 High = ⑥ の go / no-go「夜間ロードのロールを分ける」): 夜間ロード・push・migration・復元・ロールの設定は全部
--         同じログイン (COMPANY_DB_URL = DB・schema・表の持ち主・CREATEROLE) で動く。持ち主は trigger を止められ (ALTER TABLE … DISABLE TRIGGER)・
--         schema の持ち主として表を DROP でき・CREATEROLE で作ったロールの一員に自分でなれる = この表の持ち主だけ別のロールにしても守りにならない
--         (試した: PostgreSQL 18 で、持ち主を NOLOGIN のロールにしても、作った人が自分に SET を付け直して戻れる・schema の持ち主は DROP できる)。
--         本当の直し = 夜間ロードと push を、持ち主でなく CREATEROLE の無い別のログインにする (今の全部の書き手に効く = ⑥)。
--         それまでの手当て (下の 9.): 消えた対応 = 変更の記録 (追記だけ) に対応の行の記録があるのに今の行が無い出品 (ops.amazon_map_lost_listings) を
--           ・夜間ロードは「対応がある」と同じに扱う (墓標を消されても、自動の構成を作り直さない) + 報告に出す
--           ・切替の段階を company_owner / new_open に進める前提にする (1 件でもあれば進めない)
--         両方の表の trigger を止めて消す (2 つの間違い) までは、墓標が消えても自動の対応は戻らない
--   3. 不変条件 (契約 v3 H5・High 4) = commit のときに見る (deferred の constraint trigger)。core.amazon_sku_maps・core.listing_components・
--      core.listings (mall・shop_code・listing_code) の全部の INSERT / UPDATE / DELETE から、その出品を確かめる:
--        対応の出品が Amazon (日本) = mall 'amazon'・shop_code 'main@A1VC38T7YXB528' / listing_norm = core.norm_code(seller_sku) /
--        active = 構成が 1 行以上・sort_order が 0..N-1 (隙間・重なりなし) / deleted = 構成が 0 行
--      対応の無い出品は何も見ない (= 今の夜間ロードの動きは変わらない・今は 1 行も無い)
--   4. 構成の書き手 (H5): 段階が company_owner / new_open の間、対応のある出品の構成と対応の行は、core.source_system が
--      'portal_amazon_map' (この画面の関数) か 'amazon_map_migration' (切替の日の移行) のときだけ書ける (行の列ではなく取引の設定を見る)。
--      legacy_open / frozen は今までどおり (夜間ロードは対応のある出品をもともと触らない = engine.mjs)
--   5. 構成の行の「変えた時刻」core.listing_components.updated_at (null = created_at と同じ)。写しの構成の created_at / updated_at (M7):
--        そのまま = 変えない / 数量か並びを変えた = updated_at だけ今 / 消してまた足した・新しい = 両方とも今。対応の行の changed_at は何か変えた保存のときだけ今
--   6. 画面の保存 = 専用の security definer の関数 2 つ (0052 の登録の関数と同じ形):
--        ops.save_amazon_sku_map (登録・直す・墓標から戻す) / ops.delete_amazon_sku_map (墓標にする)
--      関数の中で: 段階 new_open・持ち主表のハッシュ・持ち主 listing_components.amazon = company・request_id が未使用・形 (seller SKU は写しの受け手と同じ決まり)・
--        出品ごとの鍵 → 出品と対応の行の鍵 → 版 (ops.amazon_map_versions = 画面が読んだ版) → 構成品 (ある SKU・単品かセット・登録の状態が NE 確認済み以降) を DB で確かめ、
--        書き込みの約束 (0051 の ops.master_write_sessions・operation = amazon_map_save / amazon_map_delete・listing_id) → 出品 (無ければ作る) → 対応 → 構成 → 保存の記録 done を 1 か所で。
--        約束と保存の記録の payload_hash = DB が作った「関数が書いた値」のハッシュ (ops.js_stable_sha256)・結果も DB が作る (画面の要求のハッシュは結果の request_payload_hash)
--      画面のロール master_edit には表の書き込みを渡さない (関数の実行だけ)。関数の中の書き込みも呼び手は master_edit = 下の守り (ops.guard_amazon_map_write) と 0051 の約束で見る
--   7. 0051 の約束の表・保存の記録・関数を、必要なところだけ広げる:
--        ops.master_write_sessions: operation に amazon_map_save / amazon_map_delete・sku_id を null 可・listing_id (遅らせられる外部キー fk_mws_listing)・
--          source_system = 操作ごと (portal_master_edit / portal_amazon_map)・相手 = SKU か出品のどちらか 1 つ
--        ops.master_edit_requests: operation に同じ 2 つ・listing_id
--        ops.check_master_write_session_done: SKU と出品を「null も同じ」で比べる (sku_edit・sku_create の答えは変わらない)
--        ops.master_write_allowed: 前の行は全部そのまま + 2 つの操作の行
--   8. 「未登録」の一覧の材料 ops.amazon_map_unmapped_recent (直近の Amazon の注文で、出品に構成が無い・出品が無い seller SKU。M11) と
--      売上の公開のそろい ops.amazon_map_sales_coverage (そろっていない日は画面が「未判定」と出す)
--   9. 消えた対応 ops.amazon_map_lost_listings (変更の記録にあるのに行が無い = trigger を止めて消された)。夜間ロードは対応があると同じに扱う・切替の前提 (0053_amazon_map)
-- 🚨 この migration は今のデータを何も変えない (新しい表は空・足した列は null・制約は新しい行と新しい操作にだけ効く)。
--    切替の前の片付け (16 §5 の 4) は移行の影運転 (scripts/company-db/amazon-map-migrate.mjs) が数える
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す (0034 の約束)

-- 0. Amazon (日本) の出品の店舗キー (apps/company-db/load/sources.mjs の SHOP_CODES.amazon と同じ)
create function core.amazon_jp_shop_code() returns text language sql immutable as $$ select 'main@A1VC38T7YXB528'::text $$;

-- 写しの受け手 (lib/sku-map-canonical.js の skuMapKeyProblem) と同じ鍵の決まり。問題があれば理由、無ければ null。
--   空・256 文字以上・制御文字 (U+0000〜U+001F・U+007F)・前後の空白 (SKU_MAP_EDGE_SPACE_CHARS = JS の trim が削る文字)・ASCII の大文字
create function core.amazon_map_key_problem(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case
    when p is null or length(p) = 0 then '空'
    when length(p) > 255 then '255 文字を超える'
    when p ~ '[\u0001-\u001f\u007f]' then '制御文字を含む'
    when p ~ '^[\u0009-\u000d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]'
      or p ~ '[\u0009-\u000d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]$' then '前後に空白 (全角の空白・NBSP・BOM も)'
    when p ~ '[A-Z]' then '大文字を含む (小文字にそろえる)'
  end
$$;
-- 名前が空 (空白だけ = 全角の空白だけも) か
create function core.amazon_map_name_blank(p text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select p is null or p ~ '^[\u0009-\u000d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]*$'
$$;

-- 1. 対応の表
create table core.amazon_sku_maps (
  listing_id      bigint primary key,
  company_id      smallint not null default 1 references core.companies,
  seller_sku      text not null unique,      -- 写しの seller_sku (小文字・前後の空白なし = m_sku_master の CHECK と同じ + ASCII の大文字なし)
  name            text not null,             -- 社内の商品名 (m_sku_master.商品名)
  state           text not null check (state in ('active', 'deleted')),
  origin          text not null check (origin in ('legacy', 'portal')),
  registered_at   timestamptz(3) not null,   -- 写しの親の created_at (再登録で新しくなる)
  registered_by   text,
  changed_at      timestamptz(3) not null,   -- 写しの親の updated_at (何か変えた保存のときだけ進む)
  changed_by      text,
  deleted_at      timestamptz(3),
  deleted_by      text,
  deleted_reason  text,
  version         bigint not null default nextval('core.master_version_seq') check (version > 0),
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),
  -- 鍵と名前は写しの受け手 (lib/sku-map-canonical.js の skuMapKeyProblem・SKU_MAP_EDGE_SPACE_CHARS) と同じ決まり = 前後の TAB・NBSP・全角の空白も断る (Codex #1586 R1 M1)。
  --   どの書き手 (画面の関数・切替の日の移行・持ち主の手の SQL) にも効く。新しい表 = 今の行は無い
  constraint ck_asm_seller_sku check (core.amazon_map_key_problem(seller_sku) is null),
  constraint ck_asm_name check (not core.amazon_map_name_blank(name)),
  constraint ck_asm_deleted check ((state = 'deleted') = (deleted_at is not null) and (state = 'deleted') = (deleted_by is not null) and (state = 'deleted') = (deleted_reason is not null)),
  constraint ck_asm_reason check (deleted_reason is null or length(btrim(deleted_reason)) between 1 and 200),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id)
);
comment on table core.amazon_sku_maps is 'Amazon SKU の対応 (0053・⑦-1)。1 行 = 1 つの出品の対応。構成は core.listing_components。墓標 (deleted) は消さない。書くのは ops.save_amazon_sku_map / ops.delete_amazon_sku_map と切替の日の移行だけ';
create trigger trg_amazon_sku_maps_touch before update on core.amazon_sku_maps for each row execute function core.touch_updated_at();
create trigger trg_amazon_sku_maps_version before insert or update on core.amazon_sku_maps for each row execute function core.bump_master_version();
create trigger trg_amazon_sku_maps_audit after insert or update or delete on core.amazon_sku_maps for each row execute function core.audit_master_change('amazon_sku_map', 'listing_id');
create trigger trg_amazon_sku_maps_bump_parent after insert or update or delete on core.amazon_sku_maps for each row execute function core.bump_parent_version('core.listings', 'listing_id', 'listing_id');

-- 変更の記録の種類に amazon_sku_map。🚨 not valid = 前の CHECK (この値を含まない狭い集合) が今の行を守っていた = 大きな記録の表を読み直さない (新しい行には効く)
do $$
declare v text;
begin
  select c.conname into strict v from pg_catalog.pg_constraint c
   where c.conrelid = 'events.master_change_events'::regclass and c.contype = 'c' and pg_catalog.pg_get_constraintdef(c.oid) like '%entity_type%';
  execute format('alter table events.master_change_events drop constraint %I', v);
end $$;
alter table events.master_change_events add constraint ck_mce_entity_type
  check (entity_type in ('product','sku','supplier','supplier_sku','sku_component','sku_cost','listing','listing_component','amazon_sku_map')) not valid;

-- 2. 墓標を消さない (契約 v3 High 4)。DELETE も TRUNCATE もいつも拒む (復元はユーザーの trigger を止めてから消す = 影響しない)
create function core.reject_amazon_map_delete() returns trigger language plpgsql as $$
begin
  raise exception 'amazon_map_no_delete: Amazon SKU の対応 (墓標も) は消さない。やめるときは ops.delete_amazon_sku_map で墓標 (deleted) にする' using errcode = '42501';
end $$;
create trigger trg_amazon_sku_maps_no_delete before delete on core.amazon_sku_maps for each row execute function core.reject_amazon_map_delete();
create trigger trg_amazon_sku_maps_no_truncate before truncate on core.amazon_sku_maps for each statement execute function core.reject_amazon_map_delete();
revoke delete, truncate on core.amazon_sku_maps from public;

-- 5. 構成の行の「変えた時刻」(写しの構成の updated_at。null = created_at と同じ)。夜間ロードは書かない (対応のある出品の構成は触らない)
alter table core.listing_components add column updated_at timestamptz;
comment on column core.listing_components.updated_at is 'Amazon SKU の対応の構成の行を数量・並びで変えた時刻 (0053)。null = created_at と同じ。写し (⑦-2) の構成の updated_at';

-- 3. 不変条件 (commit のときに出品ごとに見る)。問題があれば理由、無ければ null (対応の無い出品は null)
create function core.amazon_map_problem(p_listing_id bigint) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  m       core.amazon_sku_maps%rowtype;
  v_mall  text;
  v_shop  text;
  v_norm  text;
  v_n     integer;
  v_min   integer;
  v_max   integer;
  v_dist  integer;
begin
  if p_listing_id is null then return null; end if;
  select * into m from core.amazon_sku_maps where listing_id = p_listing_id;
  if not found then return null; end if;
  select l.mall, l.shop_code, l.listing_norm into v_mall, v_shop, v_norm from core.listings l where l.listing_id = p_listing_id;
  if not found then return '出品が無い'; end if;
  if v_mall <> 'amazon' or v_shop <> core.amazon_jp_shop_code() then return format('出品が Amazon (日本) でない (%s / %s)', v_mall, v_shop); end if;
  if v_norm is distinct from core.norm_code(m.seller_sku) then return format('出品のコード (%s) と seller SKU (%s) が違う', v_norm, m.seller_sku); end if;
  select count(*)::int, min(c.sort_order)::int, max(c.sort_order)::int, count(distinct c.sort_order)::int into v_n, v_min, v_max, v_dist
    from core.listing_components c where c.listing_id = p_listing_id;
  if m.state = 'active' then
    if v_n = 0 then return '有効な対応に構成が無い'; end if;
    if v_min <> 0 or v_max <> v_n - 1 or v_dist <> v_n then return format('構成の並び (sort_order) が 0..%s でない', v_n - 1); end if;
  elsif v_n > 0 then
    return format('墓標 (deleted) に構成が %s 行残っている', v_n);
  end if;
  return null;
end $$;
revoke all on function core.amazon_map_problem(bigint) from public;

create function core.check_amazon_map_invariant() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_old jsonb := case when tg_op in ('UPDATE', 'DELETE') then to_jsonb(old) end;
  v_new jsonb := case when tg_op in ('INSERT', 'UPDATE') then to_jsonb(new) end;
  x     bigint;
  v_bad text;
begin
  for x in select distinct u.id from unnest(array[(v_old ->> 'listing_id')::bigint, (v_new ->> 'listing_id')::bigint]) as u(id) where u.id is not null loop
    v_bad := core.amazon_map_problem(x);
    if v_bad is not null then
      raise exception 'amazon_map_invariant: 出品 % の Amazon SKU の対応: %', x, v_bad using errcode = '23514';
    end if;
  end loop;
  return null;
end $$;
revoke all on function core.check_amazon_map_invariant() from public;
create constraint trigger trg_amazon_sku_maps_invariant after insert or update on core.amazon_sku_maps
  deferrable initially deferred for each row execute function core.check_amazon_map_invariant();
create constraint trigger trg_listing_components_amazon_map after insert or update or delete on core.listing_components
  deferrable initially deferred for each row execute function core.check_amazon_map_invariant();
create constraint trigger trg_listings_amazon_map after update of mall, shop_code, listing_code or delete on core.listings
  deferrable initially deferred for each row execute function core.check_amazon_map_invariant();

-- 4. 構成の書き手 (H5): company_owner / new_open の間、対応のある出品の構成・対応の行は、取引の設定 core.source_system が
--    portal_amazon_map (画面の関数) / amazon_map_migration (切替の日の移行) のときだけ書ける。段階が読めない = 書けない (fail-closed)
create function core.guard_amazon_map_writer() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_ids   bigint[] := array_remove(array[case when tg_op in ('UPDATE', 'DELETE') then (to_jsonb(old) ->> 'listing_id')::bigint end,
                                         case when tg_op in ('INSERT', 'UPDATE') then (to_jsonb(new) ->> 'listing_id')::bigint end], null);
  v_phase text;
  v_src   text := coalesce(pg_catalog.current_setting('core.source_system', true), '');
begin
  -- 構成の行は、対応のある出品のときだけ見る (対応の行そのものはいつも)
  if tg_table_name <> 'amazon_sku_maps' and not exists (select 1 from core.amazon_sku_maps m where m.listing_id = any(v_ids)) then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  if (v_phase is null or v_phase in ('company_owner', 'new_open')) and v_src not in ('portal_amazon_map', 'amazon_map_migration') then
    raise exception 'amazon_map_writer: 切替の段階が % の間、Amazon SKU の対応のある出品の構成 (%) は画面の関数か切替の日の移行だけが書く (core.source_system = %)',
      coalesce(v_phase, '読めない'), tg_table_name, nullif(v_src, '') using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function core.guard_amazon_map_writer() from public;
create trigger trg_listing_components_amazon_map_writer before insert or update or delete on core.listing_components
  for each row execute function core.guard_amazon_map_writer();
create trigger trg_amazon_sku_maps_writer before insert or update on core.amazon_sku_maps
  for each row execute function core.guard_amazon_map_writer();

-- 7. 0051 の約束の表・保存の記録を広げる
-- 7a. 保存の記録 (追記だけ): 操作に 2 つ・出品
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create', 'amazon_map_save', 'amazon_map_delete'));
alter table ops.master_edit_requests add column listing_id bigint references core.listings (listing_id);
create index ix_master_edit_requests_listing on ops.master_edit_requests (listing_id, started_at desc) where listing_id is not null;

-- 7b. 約束の表: 操作・相手 (SKU か出品)・出どころ
do $$
declare v text;
begin
  select c.conname into strict v from pg_catalog.pg_constraint c
   where c.conrelid = 'ops.master_write_sessions'::regclass and c.contype = 'c' and pg_catalog.pg_get_constraintdef(c.oid) like '%source_system%';
  execute format('alter table ops.master_write_sessions drop constraint %I', v);
end $$;
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create', 'amazon_map_save', 'amazon_map_delete'));
alter table ops.master_write_sessions alter column sku_id drop not null;
alter table ops.master_write_sessions add column listing_id bigint;
-- 出品の外部キーは遅らせられる形 (ふだんはすぐ確かめる)。保存の関数だけが、まだ無い出品を作るときに commit のときの確かめにする (0052 の fk_mws_sku と同じ)
alter table ops.master_write_sessions add constraint fk_mws_listing foreign key (listing_id) references core.listings (listing_id) deferrable initially immediate;
alter table ops.master_write_sessions add constraint ck_mws_target check (case when operation in ('amazon_map_save', 'amazon_map_delete')
  then listing_id is not null and sku_id is null else sku_id is not null and listing_id is null end);
alter table ops.master_write_sessions add constraint ck_mws_source check (source_system = case when operation in ('amazon_map_save', 'amazon_map_delete') then 'portal_amazon_map' else 'portal_master_edit' end);

-- 7c. commit のときの確かめ (0051): SKU と出品を「null も同じ」で比べる。sku_edit・sku_create は出品が null どうし = 答えは前と同じ
create or replace function ops.check_master_write_session_done() returns trigger
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
begin
  if (select count(*) from ops.master_edit_requests r
       where r.request_id = new.request_id and r.status = 'done' and r.actor_id = new.actor_id
         and r.sku_id is not distinct from new.sku_id and r.listing_id is not distinct from new.listing_id
         and r.operation = new.operation and r.payload_hash = new.payload_hash
         and r.xmin::text = (new.txid % 4294967296)::text) <> 1 then   -- 同じ取引で書いた done (前からある記録は数えない)
    raise exception 'master_write_session_unfinished: 約束した取引 (request_id %) は、同じ取引で約束どおりの保存の記録 done を書かないと commit できない', new.request_id using errcode = '42501';
  end if;
  return null;
end $$;

-- 7d. 操作ごとの書いてよい (表・書き方)。sku_edit・sku_create の行は 0052 と同じ。
--     amazon_map_save = 出品 (無ければ作る・0026 の version の付け替え)・対応・構成 / amazon_map_delete = 対応を墓標に・構成を消す・出品の version
create or replace function ops.master_write_allowed(p_operation text, p_table text, p_op text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from (values
      ('sku_edit', 'core.skus', 'UPDATE'), ('sku_edit', 'core.products', 'UPDATE'),
      ('sku_edit', 'core.supplier_skus', 'INSERT'), ('sku_edit', 'core.supplier_skus', 'UPDATE'),
      ('sku_edit', 'core.sku_costs', 'INSERT'), ('sku_edit', 'core.sku_costs', 'UPDATE'), ('sku_edit', 'core.sku_costs', 'DELETE'),
      ('sku_edit', 'ops.sku_component_requests', 'INSERT'), ('sku_edit', 'ops.sku_component_requests', 'UPDATE'),
      ('sku_edit', 'ops.sku_component_breaches', 'UPDATE'),
      ('sku_create', 'core.products', 'INSERT'), ('sku_create', 'core.products', 'UPDATE'),
      ('sku_create', 'core.skus', 'INSERT'), ('sku_create', 'core.skus', 'UPDATE'),
      ('sku_create', 'core.supplier_skus', 'INSERT'), ('sku_create', 'core.sku_costs', 'INSERT'),
      ('sku_create', 'ops.sku_component_requests', 'INSERT'),
      ('amazon_map_save', 'core.listings', 'INSERT'), ('amazon_map_save', 'core.listings', 'UPDATE'),
      ('amazon_map_save', 'core.amazon_sku_maps', 'INSERT'), ('amazon_map_save', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_save', 'core.listing_components', 'INSERT'), ('amazon_map_save', 'core.listing_components', 'UPDATE'), ('amazon_map_save', 'core.listing_components', 'DELETE'),
      ('amazon_map_delete', 'core.listings', 'UPDATE'),
      ('amazon_map_delete', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_delete', 'core.listing_components', 'DELETE')) as m(op, tbl, act)
    where m.op = p_operation and m.tbl = p_table and m.act = p_op)
$$;

-- 7e. 画面のロール master_edit の書き込みの守り (出品・構成・対応)。0051 の ops.guard_master_edit_write と同じ考え:
--     呼び手 (SET ROLE の役 か ログインした役) が master_edit のときだけ見る。今の取引の約束が無い・Amazon の操作でない・段階が new_open でない・
--     操作で書けない (表・書き方)・約束の出品でない行・持ち主 listing_components.amazon が company でない = 42501
-- 🚨 security definer (画面のロールに ops.master_write_sessions を読ませない)
create function ops.guard_amazon_map_write() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_sess    ops.master_write_sessions;
  v_old     bigint;
  v_new     bigint;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  v_sess := ops.current_master_write_session();
  if v_sess.session_id is null then
    raise exception 'master_write_session_required: 画面のロールの書き込み (%) は、保存の関数 (ops.save_amazon_sku_map / ops.delete_amazon_sku_map) の中だけ', v_tbl using errcode = '42501';
  end if;
  if v_sess.operation not in ('amazon_map_save', 'amazon_map_delete') then
    raise exception 'master_write_operation: 約束の操作 % では % に書けない', v_sess.operation, v_tbl using errcode = '42501';
  end if;
  if (select s.phase from ops.master_cutover_state s where s.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない (%)', v_tbl using errcode = '42501';
  end if;
  if not ops.master_write_allowed(v_sess.operation, v_tbl, tg_op) then
    raise exception 'master_write_operation: 約束の操作 % では % に % できない', v_sess.operation, v_tbl, tg_op using errcode = '42501';
  end if;
  if tg_op in ('UPDATE', 'DELETE') then v_old := (to_jsonb(old) ->> 'listing_id')::bigint; end if;
  if tg_op in ('INSERT', 'UPDATE') then v_new := (to_jsonb(new) ->> 'listing_id')::bigint; end if;
  if (tg_op <> 'INSERT' and v_old is distinct from v_sess.listing_id) or (tg_op <> 'DELETE' and v_new is distinct from v_sess.listing_id) then
    raise exception 'master_write_target: 約束の相手 (出品 %) の行でない (%)', v_sess.listing_id, v_tbl using errcode = '42501';
  end if;
  if (v_sess.ownership ->> 'listing_components.amazon') is distinct from 'company' then
    raise exception 'owner_not_company: listing_components.amazon の持ち主が company でない (%)', v_tbl using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_amazon_map_write() from public;
create trigger trg_amazon_map_master_edit_guard before insert or update or delete on core.amazon_sku_maps for each row execute function ops.guard_amazon_map_write();
create trigger trg_amazon_map_master_edit_guard before insert or update or delete on core.listing_components for each row execute function ops.guard_amazon_map_write();
create trigger trg_amazon_map_master_edit_guard before insert or update or delete on core.listings for each row execute function ops.guard_amazon_map_write();

-- 保存の記録 done の出品 = 約束の出品 (0051 の guard は SKU・操作・中身のハッシュを見る。出品はここで)
create function ops.guard_master_edit_request_listing() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' or new.status <> 'done' then return new; end if;
  v_sess := ops.current_master_write_session();
  if new.listing_id is distinct from v_sess.listing_id then
    raise exception 'master_write_session_mismatch: 保存の記録 (done) の出品 % が約束の出品 % と違う', new.listing_id, v_sess.listing_id using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_edit_request_listing() from public;
create trigger trg_master_edit_requests_listing before insert on ops.master_edit_requests for each row execute function ops.guard_master_edit_request_listing();

-- 6. 画面の保存
-- 版 (画面が読んだ出品と対応の行の版)。出品の version は構成・対応の変更でも上がる (0026 の親の version)。lib/amazon-map-write.mjs の amazonMapVersionsOf と同じ形
create function ops.amazon_map_versions(p_listing_id bigint) returns jsonb language sql stable set search_path = pg_catalog, pg_temp as $$
  select jsonb_build_object(
    'listing', (select l.listing_id::text || ':' || l.version::text from core.listings l where l.listing_id = p_listing_id),
    'map', (select m.listing_id::text || ':' || m.version::text || ':' || m.state from core.amazon_sku_maps m where m.listing_id = p_listing_id))
$$;

-- 保存・削除の始めの共通 (鍵・形・段階・持ち主表・request_id・出品と対応の行・版)。2 つの関数の中だけで呼ぶ (public の実行権なし)
--   戻り値 { listing_id (無ければ null), phase, owner_hash, versions, map: { seller_sku, name, state } | null }
create function ops.amazon_map_begin(p_operation text, p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_sku   text := p_entry ->> 'seller_sku';
  v_phase text;
  v_owner text;
  v_hash  text;
  v_bad   text;
  v_lid   bigint;
  v_map   core.amazon_sku_maps%rowtype;
  v_ver   jsonb;
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.master_cutover'));   -- 段階を変える取引と並ぶ (画面は先に取っている = 同じ鍵)
  perform pg_catalog.pg_advisory_xact_lock_shared(core.master_write_lock_key());                -- 夜間ロード・切替の日の移行と並ぶ (画面は先に取っている = 同じ鍵)
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if p_actor_id is null or length(btrim(p_actor_id)) = 0 or length(p_actor_id) > 320 or p_actor_id ~ '[[:cntrl:]]' then
    raise exception 'invalid_input: 保存する人 (actor_id) の形が違う' using errcode = '22023';
  end if;
  if p_reason is not null and (length(p_reason) > 200 or p_reason ~ '[[:cntrl:]]') then raise exception 'invalid_input: 理由は 200 字まで・制御文字なし' using errcode = '22023'; end if;
  if p_ownership is null or jsonb_typeof(p_ownership) <> 'object'
     or exists (select 1 from jsonb_each(p_ownership) e where jsonb_typeof(e.value) <> 'string' or (e.value #>> '{}') not in ('load', 'company')) then
    raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023';
  end if;
  if coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: 要求のハッシュ (64 桁の 16 進) が要る' using errcode = '22023'; end if;
  if p_entry is null or jsonb_typeof(p_entry) <> 'object' or jsonb_typeof(p_entry -> 'seller_sku') is distinct from 'string' then
    raise exception 'invalid_input: 保存の中身 (seller_sku) が要る' using errcode = '22023';
  end if;
  if jsonb_typeof(p_entry -> 'versions') is distinct from 'object'
     or exists (select 1 from jsonb_object_keys(p_entry -> 'versions') k where k not in ('listing', 'map'))
     or coalesce(jsonb_typeof(p_entry -> 'versions' -> 'listing'), 'missing') not in ('string', 'null')
     or coalesce(jsonb_typeof(p_entry -> 'versions' -> 'map'), 'missing') not in ('string', 'null') then
    raise exception 'invalid_input: 版 (versions = { listing, map }) が要る' using errcode = '22023';
  end if;
  if p_entry ? 'result' then raise exception 'invalid_input: 保存の結果 (result) は DB が作る (渡さない)' using errcode = '22023'; end if;
  -- 段階・持ち主表 (ops.begin_master_write と同じ) + この画面の列の持ち主
  select s.phase, s.owner_hash into v_phase, v_owner from ops.master_cutover_state s where s.id = 1;
  v_hash := ops.ownership_hash(p_ownership);
  if v_phase is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が % (new_open でない)', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
  if v_owner is distinct from v_hash then raise exception 'before_cutover: 持ち主表が切替のときの記録と違う' using errcode = 'P0001'; end if;
  if (p_ownership ->> 'listing_components.amazon') is distinct from 'company' then
    raise exception 'before_cutover: Amazon SKU の対応 (listing_components.amazon) の持ち主がまだ company でない' using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'invalid_input: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '22023';
  end if;
  -- seller SKU (写しの受け手と同じ決まり。前からある対応の削除は、今の行の値なので形は見ない)
  v_bad := core.amazon_map_key_problem(v_sku);
  if v_bad is not null and p_operation = 'amazon_map_save' then raise exception 'invalid_value: seller SKU が使えない形です (%・%)', v_bad, v_sku using errcode = '22023'; end if;
  -- 出品ごとの鍵 (正規化したコード。出品がまだ無いときも 2 つの保存が並ぶ) → 出品の行 → 対応の行
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.amazon_map:' || coalesce(core.norm_code(v_sku), ''), 0));
  select l.listing_id into v_lid from core.listings l
   where l.company_id = 1 and l.mall = 'amazon' and l.shop_code = core.amazon_jp_shop_code() and l.listing_norm = core.norm_code(v_sku)
   for update;
  if v_lid is not null then
    select * into v_map from core.amazon_sku_maps m where m.listing_id = v_lid for update;
    if v_map.listing_id is not null and v_map.seller_sku is distinct from v_sku then
      raise exception 'norm_collision: この出品には別の seller SKU (%) の対応がある (正規化すると同じ)', v_map.seller_sku using errcode = 'P0001';
    end if;
  end if;
  -- 版 (画面が読んだ後に出品・構成・対応が変わった = 409)
  v_ver := ops.amazon_map_versions(v_lid);
  if (p_entry -> 'versions') is distinct from v_ver then
    raise exception 'version_conflict: 画面を開いた後にこの Amazon SKU (出品・構成・対応) が変わった (DB の版と違う)' using errcode = 'P0001';
  end if;
  return jsonb_build_object('listing_id', v_lid, 'phase', v_phase, 'owner_hash', v_hash, 'versions', v_ver,
    'map', case when v_map.listing_id is null then null else jsonb_build_object('seller_sku', v_map.seller_sku, 'name', v_map.name, 'state', v_map.state) end);
end $$;
revoke all on function ops.amazon_map_begin(text, uuid, text, text, jsonb, text, jsonb) from public;

-- 構成品の形を確かめて、DB の値で作り直す: [{ sku_id, code, qty }] (1〜20 行・並び = 配列の順) →
--   [{ sku_id, code (SKU の今のコード), qty, sort_order (0 から) }]。SKU ごとの鍵 (画面と同じ鍵・sku_id の順) → 行を for share。
--   ある SKU・コードが同じ・単品かセット (例外の SKU は不可)・登録の状態が NE 確認済み以降 (ne_confirmed / distributable / available) だけ
create function ops.amazon_map_components(p_rows jsonb) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_n    integer;
  v_ids  bigint[];
  v_bad  text;
  v_rows jsonb;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then raise exception 'invalid_value: 構成品 (components) は配列' using errcode = '22023'; end if;
  v_n := jsonb_array_length(p_rows);
  if v_n < 1 or v_n > 20 then raise exception 'invalid_value: 構成品は 1〜20 行 (% 行)', v_n using errcode = '22023'; end if;
  if exists (select 1 from jsonb_array_elements(p_rows) e
              where jsonb_typeof(e) <> 'object'
                 or exists (select 1 from jsonb_object_keys(e) k where k not in ('sku_id', 'code', 'qty'))
                 or jsonb_typeof(e -> 'sku_id') is distinct from 'number' or (e ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$'
                 or jsonb_typeof(e -> 'code') is distinct from 'string'
                 or jsonb_typeof(e -> 'qty') is distinct from 'number' or (e ->> 'qty') !~ '^[1-9][0-9]{0,2}$') then
    raise exception 'invalid_value: 構成品の行は { sku_id, code, qty (1〜999) } だけ' using errcode = '22023';
  end if;
  select array_agg(distinct (e ->> 'sku_id')::bigint order by (e ->> 'sku_id')::bigint) into v_ids from jsonb_array_elements(p_rows) e;
  if cardinality(v_ids) <> v_n then raise exception 'invalid_value: 構成品が重なっている (同じ品は 1 行にして数量で)' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || x::text, 0)) from unnest(v_ids) as t(x) order by x;
  perform 1 from core.skus where sku_id = any(v_ids) order by sku_id for share;
  with r as (
    select e.ord, (e.v ->> 'sku_id')::bigint as sku_id, e.v ->> 'code' as code, (e.v ->> 'qty')::integer as qty
      from jsonb_array_elements(p_rows) with ordinality as e(v, ord)),
  f as (select r.*, k.code as k_code, k.sku_kind, reg.state from r left join core.skus k on k.sku_id = r.sku_id left join ops.master_registrations reg on reg.sku_id = r.sku_id)
  select (select string_agg(format('%s (%s)', f.code, case when f.k_code is null then '無い SKU' when f.k_code <> f.code then 'コードが違う'
                                                          when f.sku_kind not in ('single', 'set') then '例外の SKU' else coalesce('状態 ' || f.state, '登録の状態が無い') end), '・' order by f.ord)
            from f where f.k_code is null or f.k_code <> f.code or f.sku_kind not in ('single', 'set') or f.state is null or f.state not in ('ne_confirmed', 'distributable', 'available')),
         (select jsonb_agg(jsonb_build_object('sku_id', f.sku_id, 'code', f.k_code, 'qty', f.qty, 'sort_order', f.ord - 1) order by f.ord) from f)
    into v_bad, v_rows;
  if v_bad is not null then raise exception 'component_unusable: 構成品に使えない商品がある: %', v_bad using errcode = 'P0001'; end if;
  return v_rows;
end $$;
revoke all on function ops.amazon_map_components(jsonb) from public;

-- 登録・直す・墓標から戻す (同じ取引で 1 回。画面 = lib/amazon-map-write.mjs が鍵 (request_id → 段階 → マスタの書き込み) と門の後に呼ぶ)
--   p_entry = { seller_sku, name, components: [{ sku_id, code, qty }], versions: { listing, map }, started_at }
--   変わる項目が無い = 何も書かず保存の記録 done (no_change) だけ。出品が無ければ作る (タイトル = 名前。前からある出品のタイトル・状態は夜間ロードのまま)
--   構成: 消えた品の行は消す・数量か並びが変わった行は直す (manual・updated_at = 今)・新しい品の行は足す (manual・created_at = updated_at = 今)・同じ行は触らない
--   🚨 例外の受け止め (begin ... exception) を使わない (サブトランザクションの中の done は 0051 の commit の確かめに数えられない)
create function ops.save_amazon_sku_map(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user  text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tx       bigint := pg_catalog.txid_current();
  v_now      timestamptz := pg_catalog.date_trunc('milliseconds', pg_catalog.now());
  v_reason   text := nullif(p_reason, '');
  v_sku      text := p_entry ->> 'seller_sku';
  v_name     text := p_entry ->> 'name';
  v_b        jsonb;
  v_lid      bigint;
  v_new_lst  boolean := false;
  v_map      jsonb;
  v_rows     jsonb;
  v_cur      jsonb;
  v_created  boolean;
  v_revive   boolean;
  v_name_chg boolean;
  v_comp_chg boolean;
  v_changed  boolean;
  v_written  jsonb;
  v_whash    text;
  v_sess     uuid := pg_catalog.gen_random_uuid();
  v_src_prev text := pg_catalog.current_setting('core.source_system', true);
  v_result   jsonb;
begin
  v_b := ops.amazon_map_begin('amazon_map_save', p_request_id, p_actor_id, v_reason, p_ownership, p_payload_hash, p_entry);
  if exists (select 1 from jsonb_object_keys(p_entry) k where k not in ('seller_sku', 'name', 'components', 'versions', 'started_at')) then
    raise exception 'invalid_input: 保存の中身の知らない欄' using errcode = '22023';
  end if;
  if jsonb_typeof(p_entry -> 'name') is distinct from 'string' or v_name is distinct from btrim(v_name) or length(v_name) not between 1 and 255
     or v_name ~ '[[:cntrl:]]' or core.amazon_map_name_blank(v_name) then
    raise exception 'invalid_value: 名前は 1〜255 字 (前後の空白・制御文字なし・空白だけは不可)' using errcode = '22023';
  end if;
  v_rows := ops.amazon_map_components(p_entry -> 'components');
  v_lid := (v_b ->> 'listing_id')::bigint;
  v_map := v_b -> 'map';
  if v_lid is null then
    v_lid := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.listings', 'listing_id')::regclass);
    v_new_lst := true;
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('sku_id', c.sku_id, 'qty', c.qty, 'sort_order', c.sort_order) order by c.sort_order, c.sku_id), '[]'::jsonb) into v_cur
    from core.listing_components c where c.listing_id = v_lid;
  v_created := jsonb_typeof(v_map) is distinct from 'object';
  v_revive := not v_created and (v_map ->> 'state') = 'deleted';
  v_name_chg := v_created or (v_map ->> 'name') is distinct from v_name;
  v_comp_chg := v_cur is distinct from (select coalesce(jsonb_agg(jsonb_build_object('sku_id', (r ->> 'sku_id')::bigint, 'qty', (r ->> 'qty')::integer, 'sort_order', (r ->> 'sort_order')::integer)
                                                          order by (r ->> 'sort_order')::integer, (r ->> 'sku_id')::bigint), '[]'::jsonb) from jsonb_array_elements(v_rows) r);
  v_changed := v_created or v_revive or v_name_chg or v_comp_chg;
  -- 関数が書く値のハッシュ (約束と保存の記録の payload_hash = DB が作る。画面の要求のハッシュも入れて結ぶ)
  v_written := jsonb_build_object('op', 'amazon_map_save', 'request', p_payload_hash, 'seller_sku', v_sku, 'name', v_name,
    'components', (select jsonb_agg(jsonb_build_array((r ->> 'sku_id')::bigint, (r ->> 'qty')::integer, (r ->> 'sort_order')::integer) order by (r ->> 'sort_order')::integer) from jsonb_array_elements(v_rows) r),
    'reason', v_reason, 'versions', v_b -> 'versions');
  v_whash := ops.js_stable_sha256(v_written);
  -- 書き込みの約束 (0051 の約束の表・operation = amazon_map_save)。出品がまだ無いときは、出品の外部キーをこの取引だけ commit のときに確かめる
  if v_new_lst then set constraints ops.fk_mws_listing deferred; end if;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_sess, v_tx, p_request_id, 'amazon_map_save', null, v_lid, '{}'::bigint[], '{}'::bigint[], ops.js_stable_sha256(v_b -> 'versions'), v_whash, v_b -> 'versions',
            p_actor_id, v_reason, 'portal_amazon_map', v_db_user, v_b ->> 'phase', v_b ->> 'owner_hash', p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_sess::text, true);
  perform pg_catalog.set_config('core.source_system', 'portal_amazon_map', true);
  if v_changed then
    if v_new_lst then
      insert into core.listings (listing_id, company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
        overriding system value values (v_lid, 1, 'amazon', core.amazon_jp_shop_code(), v_sku, v_name, 'active', 'human', p_actor_id);
    end if;
    if v_created then
      insert into core.amazon_sku_maps (listing_id, company_id, seller_sku, name, state, origin, registered_at, registered_by, changed_at, changed_by)
        values (v_lid, 1, v_sku, v_name, 'active', 'portal', v_now, p_actor_id, v_now, p_actor_id);
    elsif v_revive then
      update core.amazon_sku_maps set state = 'active', origin = 'portal', name = v_name, registered_at = v_now, registered_by = p_actor_id, changed_at = v_now, changed_by = p_actor_id,
             deleted_at = null, deleted_by = null, deleted_reason = null
       where listing_id = v_lid;
    else
      update core.amazon_sku_maps set name = v_name, changed_at = v_now, changed_by = p_actor_id where listing_id = v_lid;
    end if;
    delete from core.listing_components c
     where c.listing_id = v_lid and not exists (select 1 from jsonb_array_elements(v_rows) r where (r ->> 'sku_id')::bigint = c.sku_id);
    update core.listing_components c set qty = w.qty, sort_order = w.so, resolution = 'manual', resolved_by_type = 'human', resolved_by_id = p_actor_id,
           evidence = jsonb_build_object('source', 'portal_amazon_map', 'request_id', p_request_id::text), updated_at = v_now
      from (select (r ->> 'sku_id')::bigint as sku_id, (r ->> 'qty')::integer as qty, (r ->> 'sort_order')::smallint as so from jsonb_array_elements(v_rows) r) w
     where c.listing_id = v_lid and c.sku_id = w.sku_id and (c.qty, c.sort_order) is distinct from (w.qty, w.so);
    insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type, resolved_by_id, evidence, created_at, updated_at)
      select 1, v_lid, (r ->> 'sku_id')::bigint, (r ->> 'qty')::integer, (r ->> 'sort_order')::smallint, 'manual', 'human', p_actor_id,
             jsonb_build_object('source', 'portal_amazon_map', 'request_id', p_request_id::text), v_now, v_now
        from jsonb_array_elements(v_rows) r
       where not exists (select 1 from core.listing_components c where c.listing_id = v_lid and c.sku_id = (r ->> 'sku_id')::bigint);
  end if;
  -- 保存の記録 done (約束どおり = 0051 の commit の確かめ)。結果は DB の値から作る
  v_result := jsonb_build_object(
    'ok', true, 'no_change', not v_changed, 'seller_sku', v_sku, 'listing_id', v_lid::text, 'state', 'active',
    'created', v_created, 'revived', v_revive, 'listing_created', v_new_lst and v_changed,
    'name', case when v_name_chg and not v_created then jsonb_build_object('from', v_map ->> 'name', 'to', v_name) end,
    'components', (select jsonb_agg(jsonb_build_object('code', r ->> 'code', 'qty', (r ->> 'qty')::integer, 'sort_order', (r ->> 'sort_order')::integer) order by (r ->> 'sort_order')::integer)
                     from jsonb_array_elements(v_rows) r),
    'components_changed', v_comp_chg,
    'notes', to_jsonb(array_remove(array[
        case when v_changed then '古い表 (miniPC の SKU マスタ・Render)・FBA 補充に届くのは、次の写し (毎朝 07:00) の後です' end,
        case when v_changed and (v_created or v_revive) then 'この SKU の前からの注文が商品に結びつくのは、次の夜の取り込み (結び直し) の後です' end], null)),
    'request_id', p_request_id::text, 'request_payload_hash', p_payload_hash);
  insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, listing_id, actor_id, payload_hash, status, result, started_at)
    values (p_request_id, 1, 'amazon_map_save', left(v_sku, 60), null, v_lid, p_actor_id, v_whash, 'done', v_result,
            least(coalesce(ops.amazon_map_ts(p_entry ->> 'started_at'), pg_catalog.now()), pg_catalog.clock_timestamp()));
  -- 約束を閉じる = 関数の外では (同じ取引でも) 画面のロールは書けない。出どころの設定も前に戻す
  perform pg_catalog.set_config('ops.master_write_session', '', true);
  perform pg_catalog.set_config('core.source_system', coalesce(v_src_prev, ''), true);
  return v_result;
end $$;
revoke all on function ops.save_amazon_sku_map(uuid, text, text, jsonb, text, jsonb) from public;

-- 墓標にする (1 件ずつ・理由が要る)。p_entry = { seller_sku, versions, started_at }。対応が無い・もう墓標 = 断る。構成は全部消す (変更の記録に残る)
create function ops.delete_amazon_sku_map(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user  text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tx       bigint := pg_catalog.txid_current();
  v_now      timestamptz := pg_catalog.date_trunc('milliseconds', pg_catalog.now());
  v_reason   text := p_reason;
  v_sku      text := p_entry ->> 'seller_sku';
  v_b        jsonb;
  v_lid      bigint;
  v_removed  jsonb;
  v_written  jsonb;
  v_whash    text;
  v_sess     uuid := pg_catalog.gen_random_uuid();
  v_src_prev text := pg_catalog.current_setting('core.source_system', true);
  v_result   jsonb;
begin
  if v_reason is null or length(btrim(v_reason)) = 0 then raise exception 'invalid_input: 墓標にする理由が要る' using errcode = '22023'; end if;
  v_b := ops.amazon_map_begin('amazon_map_delete', p_request_id, p_actor_id, v_reason, p_ownership, p_payload_hash, p_entry);
  if exists (select 1 from jsonb_object_keys(p_entry) k where k not in ('seller_sku', 'versions', 'started_at')) then
    raise exception 'invalid_input: 削除の中身の知らない欄' using errcode = '22023';
  end if;
  v_lid := (v_b ->> 'listing_id')::bigint;
  if v_lid is null or jsonb_typeof(v_b -> 'map') is distinct from 'object' then raise exception 'not_found: seller SKU % の対応が無い', v_sku using errcode = 'P0001'; end if;
  if (v_b -> 'map' ->> 'state') = 'deleted' then raise exception 'already_deleted: seller SKU % はもう墓標 (削除済み)', v_sku using errcode = 'P0001'; end if;
  select coalesce(jsonb_agg(jsonb_build_object('code', k.code, 'qty', c.qty, 'sort_order', c.sort_order) order by c.sort_order, c.sku_id), '[]'::jsonb) into v_removed
    from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = v_lid;
  v_written := jsonb_build_object('op', 'amazon_map_delete', 'request', p_payload_hash, 'seller_sku', v_sku, 'reason', btrim(v_reason), 'versions', v_b -> 'versions');
  v_whash := ops.js_stable_sha256(v_written);
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_sess, v_tx, p_request_id, 'amazon_map_delete', null, v_lid, '{}'::bigint[], '{}'::bigint[], ops.js_stable_sha256(v_b -> 'versions'), v_whash, v_b -> 'versions',
            p_actor_id, btrim(v_reason), 'portal_amazon_map', v_db_user, v_b ->> 'phase', v_b ->> 'owner_hash', p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_sess::text, true);
  perform pg_catalog.set_config('core.source_system', 'portal_amazon_map', true);
  delete from core.listing_components where listing_id = v_lid;
  update core.amazon_sku_maps set state = 'deleted', deleted_at = v_now, deleted_by = p_actor_id, deleted_reason = btrim(v_reason), changed_at = v_now, changed_by = p_actor_id
   where listing_id = v_lid;
  v_result := jsonb_build_object('ok', true, 'seller_sku', v_sku, 'listing_id', v_lid::text, 'state', 'deleted', 'removed', v_removed,
    'notes', to_jsonb(array['古い表 (miniPC の SKU マスタ・Render)・FBA 補充から消えるのは、次の写し (毎朝 07:00) の後です',
                            '墓標は残ります (夜間の取り込みはこの出品の構成を作り直しません)。同じ seller SKU はもう一度登録できます']),
    'request_id', p_request_id::text, 'request_payload_hash', p_payload_hash);
  insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, listing_id, actor_id, payload_hash, status, result, started_at)
    values (p_request_id, 1, 'amazon_map_delete', left(v_sku, 60), null, v_lid, p_actor_id, v_whash, 'done', v_result,
            least(coalesce(ops.amazon_map_ts(p_entry ->> 'started_at'), pg_catalog.now()), pg_catalog.clock_timestamp()));
  perform pg_catalog.set_config('ops.master_write_session', '', true);
  perform pg_catalog.set_config('core.source_system', coalesce(v_src_prev, ''), true);
  return v_result;
end $$;
revoke all on function ops.delete_amazon_sku_map(uuid, text, text, jsonb, text, jsonb) from public;

-- 画面が渡す受けた時刻 (読めなければ null = DB の now())。例外の受け止めを保存の関数の中に置かない (サブトランザクション) ための別の関数
create function ops.amazon_map_ts(p text) returns timestamptz language sql stable set search_path = pg_catalog, pg_temp as $$
  select case when p ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?([+-]\d{2}(:?\d{2})?|Z)?$' then p::timestamptz end
$$;

-- 8. 「未登録」の一覧の材料 (M11): 直近 p_days 日 (東京の日付・p_today を含む) の Amazon (日本) の注文の明細 (取り消した注文・全部取り消した明細は除く) で、SKU に当たらず、
--    出品が無い (コードだけ) か出品に構成が無いもの。数 = 取り消しを引いた数。seller SKU ごとの数・注文の数・最後の日・対応の状態 (墓標 = deleted)。
--    FBA / FBM は Render の mirror_amazon_sku_fees (SQLite) で画面が分ける。security definer = 画面のロールに注文の表を読ませない
create function ops.amazon_map_unmapped_recent(p_today date, p_days integer default 7)
  returns table (code text, listing_id bigint, map_state text, units bigint, orders bigint, last_date date)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce(l.listing_code, ol.unresolved_code) as code, l.listing_id, m.state,
         sum(ol.qty - ol.cancelled_qty)::bigint, count(distinct o.order_id)::bigint, max(o.order_date_jst)
    from core.orders o
    join core.order_lines ol on ol.order_id = o.order_id and ol.removed_at is null
    left join core.listings l on l.listing_id = ol.listing_id
    left join core.amazon_sku_maps m on m.listing_id = l.listing_id
   where o.company_id = 1 and o.mall = 'amazon' and o.order_date_jst between p_today - (greatest(1, least(p_days, 60)) - 1) and p_today
     and not o.is_cancelled and ol.qty > ol.cancelled_qty   -- 取り消した注文・全部取り消した明細は「売れた」に数えない (Codex #1586 R1 Low)
     and ol.sku_id is null
     and (ol.listing_id is null or not exists (select 1 from core.listing_components c where c.listing_id = ol.listing_id))
     and coalesce(l.mall, 'amazon') = 'amazon'
   group by 1, 2, 3
$$;
revoke all on function ops.amazon_map_unmapped_recent(date, integer) from public;

-- 売上の公開のそろい: 直近 p_days 日のうち、Amazon の売上の日次 (mart.sales_daily_published) が公開されていない日。空 = そろっている
create function ops.amazon_map_sales_coverage(p_today date, p_days integer default 7) returns date[]
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce(array_agg(d::date order by d), '{}')
    from generate_series(p_today - (greatest(1, least(p_days, 60)) - 1), p_today, interval '1 day') as g(d)
   where not exists (select 1 from mart.sales_daily_published p where p.company_id = 1 and p.mall = 'amazon' and p.date_jst = g.d::date)
$$;
revoke all on function ops.amazon_map_sales_coverage(date, integer) from public;

-- 9. 消えた対応 (Codex #1586 R1 High の手当て・上の 2. の 🚨): 変更の記録 (events.master_change_events・追記だけ) に対応の行 (amazon_sku_map) の記録が
--    あるのに、今の core.amazon_sku_maps に行が無い出品 = trigger を止めて消された (墓標も)。普通の道では起きない (DELETE は trigger が拒む・復元は記録も一緒に戻す)。
--    夜間ロード (engine.mjs) はこの出品も「対応がある」と同じに扱う (自動の構成を作り直さない)。最後の記録の seller SKU と時刻を返す
create function ops.amazon_map_lost_listings() returns table (listing_id bigint, seller_sku text, last_recorded_at timestamptz)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select e.entity_id, (array_agg(coalesce(e.new_value ->> 'seller_sku', e.old_value ->> 'seller_sku') order by e.event_id desc)
                         filter (where coalesce(e.new_value ->> 'seller_sku', e.old_value ->> 'seller_sku') is not null))[1],
         max(e.recorded_at)
    from events.master_change_events e
   where e.entity_type = 'amazon_sku_map' and e.entity_id is not null
     and not exists (select 1 from core.amazon_sku_maps m where m.listing_id = e.entity_id)
   group by e.entity_id
$$;
revoke all on function ops.amazon_map_lost_listings() from public;

-- 切替の前提 (0051 の差し込み口に 1 行足す): company_owner / new_open に進むのは、消えた対応が 0 件のときだけ
create function ops.amazon_map_prereq(p_from text, p_to text) returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_n integer;
begin
  if p_to in ('company_owner', 'new_open') then
    select count(*)::int into v_n from ops.amazon_map_lost_listings();
    if v_n > 0 then
      return array[format('amazon_map_lost: 変更の記録にあるのに行が無い Amazon SKU の対応が %s 件ある (trigger を止めて消された。ops.amazon_map_lost_listings() を見て戻す)', v_n)];
    end if;
  end if;
  return '{}'::text[];
end $$;
revoke all on function ops.amazon_map_prereq(text, text) from public;
insert into ops.master_cutover_prereq_checks (name, fn) values ('0053_amazon_map', 'ops.amazon_map_prereq(text, text)');

-- 見張りは読むだけ。画面のロールの権限は scripts/company-db/create-master-edit-roles.mjs (migration の後に流し直す)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.amazon_sku_maps to watcher';
  end if;
end $$;
