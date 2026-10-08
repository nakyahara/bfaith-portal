-- 0061: 新商品の登録で「色違い・サイズ違いの代表」を選べるようにする (2026-10-08 中原さんの決定 a)。🚨 番号は仮 (並行の PR #1659 なども 0061 = マージの順で振り直す)
--
-- なぜ: 新商品の登録 (/apps/master-edit/new?kind=single) に代表 (どの商品の色違い・サイズ違いか) を選ぶ欄が無く、NE 登録の CSV の代表の列は
--   いつも empty だった = バリエーションを登録できない。
-- 決まり (中原さん 10/8・a): 代表の持ち主は NE のまま (config/master-ownership.mjs の products.parent = load)。
--   新商品の登録で代表を選ぶ → NE 登録の CSV の daihyo_syohin_code に入れて NE に送る → 夜間ロードが NE の代表商品コードから Company DB の親を付ける
--   → 翌朝の照合 ② が expected.parent と NE の代表を比べる (違えば partial)。🚨 Company DB の core.products の親 (parent_product_id) にはポータルから書かない
-- NE の代表商品コード = 色違い・サイズ違いのまとまりの「名札」(実データ 2,133 のうち 2,128 は商品として実在しないコード)。実在する単品のコードのこともある (その商品が親)。
--   Company DB では名札 = SKU を持たない product (display_code = 名札のコード・夜間ロードが作る D-24)・単品 = その商品の product
-- 何をするか:
--   1. ops.registration_parents = 下書きの単品ごとに「NE に登録する代表」(1 行・コードは Company DB の名札 / 単品の書き方)。書くのは ops.set_registration_parent だけ
--   2. ops.set_registration_parent = 代表を決める / 外す (約束 reg_parent_set・下書きの間だけ・配った後の CSV があれば 409・作っただけの CSV は使わないにする)
--   3. ops.ne_reg_canonical (0053) = 登録の代表があれば、それを CSV の代表の列 (NE の元の書き方) と expected.parent に。NE の元のコードに無い = 止まる理由
--      (lib/master-reg-csv.mjs の regMaterialOf も同じ決まり)。🚨 ほかは 0053 の本文と一字も変えない (scripts/test-master-reg-parent.mjs の [M] が機械で確かめる)
--   4. 約束の操作に reg_parent_set を足す: master_write_sessions / master_edit_requests の CHECK・ops.open_reg_write (0053)・ops.master_write_allowed (0054)。
--      ほかの行・操作は 0053 / 0054 と同じ ([M] が確かめる)
-- 照合 ② (ops.ne_reg_compare / record_ne_registration_check) は変えない: 前から expected.parent と NE の代表 (norm・自分自身と空 = null) を比べている
-- 権限: 関数・表は public から外す。master_edit への実行・読み取りは scripts/company-db/create-master-edit-roles.mjs (流し直す)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path の最後に pg_temp (0034 の約束)

-- ═══════════ 1. 登録の代表の表 ═══════════
create table ops.registration_parents (
  sku_id      bigint primary key references core.skus (sku_id) on delete cascade,
  parent_code text not null check (parent_code ~ '^[A-Za-z0-9_-]{1,30}$'),
  parent_norm text not null,
  request_id  uuid not null,
  set_by      text not null check (length(btrim(set_by)) > 0),
  set_at      timestamptz not null default now(),
  constraint ck_rp_norm check (parent_norm = lower(parent_code))
);
comment on table ops.registration_parents is '新商品 (下書きの単品) の NE に登録する代表 = 色違い・サイズ違いの名札か単品のコード (0061)。NE 登録の CSV の代表の列と照合 ② の expected.parent に入る。core.products の親とは別 (持ち主は NE = 夜間ロード)。書くのは ops.set_registration_parent だけ';

-- 画面のロール (master_edit) が書く = 約束 reg_parent_set の中・約束の SKU の行だけ (表の権限は渡さない = 関数の中の書き方の保険)
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない
create function ops.guard_registration_parents() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  v_sess := ops.current_master_write_session();
  if v_sess.session_id is null or v_sess.operation is distinct from 'reg_parent_set' then
    raise exception 'master_write_session_required: 登録の代表 (ops.registration_parents) は ops.set_registration_parent の中だけで書く' using errcode = '42501';
  end if;
  if (tg_op <> 'INSERT' and old.sku_id is distinct from v_sess.sku_id) or (tg_op <> 'DELETE' and new.sku_id is distinct from v_sess.sku_id) then
    raise exception 'master_write_target: 約束の SKU (%) の行でない', v_sess.sku_id using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_registration_parents() from public;
create trigger trg_registration_parents_guard before insert or update or delete on ops.registration_parents for each row execute function ops.guard_registration_parents();
create trigger trg_registration_parents_no_truncate before truncate on ops.registration_parents for each statement execute function core.reject_mutation();
revoke all on ops.registration_parents from public;

-- ═══════════ 4. 約束の操作に reg_parent_set (0054 の一覧 + 1 つ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete', 'reg_parent_set'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete', 'reg_parent_set'));

-- ops.open_reg_write (0053) = 操作の一覧に reg_parent_set を足すだけ
create or replace function ops.open_reg_write(p_operation text, p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb,
                                   p_sku_id bigint, p_derived bigint[], p_products bigint[], p_payload_hash text, p_targets jsonb) returns uuid
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_phase   text;
begin
  if p_operation is null or p_operation not in ('reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit',
                                                'supplier_create', 'supplier_declare', 'supplier_deactivate', 'reg_parent_set') then
    raise exception 'invalid_input: 知らない操作 %', p_operation using errcode = '22023';
  end if;
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: payload_hash (64 桁) が要る' using errcode = '22023'; end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'request_id_reused: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '23505';
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, p_operation, p_sku_id, coalesce(p_derived, '{}'), coalesce(p_products, '{}'), pg_catalog.repeat('0', 64),
            p_payload_hash, coalesce(p_targets, '{}'::jsonb), p_actor_id, nullif(p_reason, ''), 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(p_ownership), p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  return v_id;
end $$;
revoke all on function ops.open_reg_write(text, uuid, text, text, jsonb, bigint, bigint[], bigint[], text, jsonb) from public;

-- ops.master_write_allowed (0054) = 0054 の行は全部そのまま + reg_parent_set の行
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
      -- 0053 (⑤-2b)
      ('sku_edit', 'ops.ne_reg_exports', 'UPDATE'), ('sku_edit', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_build', 'ops.ne_reg_exports', 'INSERT'), ('reg_csv_build', 'ops.ne_reg_export_items', 'INSERT'), ('reg_csv_build', 'ops.ne_reg_export_rows', 'INSERT'),
      ('reg_csv_issue', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_issue', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_declare', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_declare', 'ops.ne_reg_export_items', 'UPDATE'), ('reg_csv_declare', 'ops.ne_reg_attempts', 'INSERT'),
      ('reg_csv_supersede', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_supersede', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_verified', 'ops.ne_csv_verified', 'INSERT'),
      ('jan_edit', 'core.external_ids', 'INSERT'), ('jan_edit', 'core.external_ids', 'UPDATE'), ('jan_edit', 'core.skus', 'UPDATE'), ('jan_edit', 'core.products', 'UPDATE'),
      ('jan_edit', 'ops.ne_reg_exports', 'UPDATE'), ('jan_edit', 'ops.ne_reg_export_items', 'UPDATE'),
      ('supplier_create', 'core.suppliers', 'INSERT'),
      ('supplier_deactivate', 'core.suppliers', 'UPDATE'), ('supplier_deactivate', 'core.supplier_skus', 'INSERT'), ('supplier_deactivate', 'core.supplier_skus', 'UPDATE'),
      ('supplier_deactivate', 'ops.ne_reg_exports', 'UPDATE'), ('supplier_deactivate', 'ops.ne_reg_export_items', 'UPDATE'),
      -- 0054 (⑦-1)
      ('amazon_map_save', 'core.listings', 'INSERT'), ('amazon_map_save', 'core.listings', 'UPDATE'),
      ('amazon_map_save', 'core.amazon_sku_maps', 'INSERT'), ('amazon_map_save', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_save', 'core.listing_components', 'INSERT'), ('amazon_map_save', 'core.listing_components', 'UPDATE'), ('amazon_map_save', 'core.listing_components', 'DELETE'),
      ('amazon_map_delete', 'core.listings', 'UPDATE'),
      ('amazon_map_delete', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_delete', 'core.listing_components', 'DELETE'),
      -- 0061 (新商品の代表 = 色違い・サイズ違い): 登録の代表の表と「作っただけの CSV を使わないにする」だけ
      ('reg_parent_set', 'ops.registration_parents', 'INSERT'), ('reg_parent_set', 'ops.registration_parents', 'UPDATE'), ('reg_parent_set', 'ops.registration_parents', 'DELETE'),
      ('reg_parent_set', 'ops.ne_reg_exports', 'UPDATE'), ('reg_parent_set', 'ops.ne_reg_export_items', 'UPDATE')) as m(op, tbl, act)
    where m.op = p_operation and m.tbl = p_table and m.act = p_op)
$$;


-- ═══════════ 2. 代表を決める / 外す ═══════════
/**
 * 新商品 (下書きの単品) の「色違い・サイズ違いの代表」を決める / 外す。NE 登録の CSV の代表の列 (daihyo_syohin_code) と照合 ② の expected.parent に入る。
 *   🚨 core.products の親には書かない (持ち主は NE = 夜間ロード。NE に取り込んだ後、夜間ロードが NE の代表商品コードから親を付ける)
 * 呼ぶところ: 新商品の登録 (lib/master-register.mjs = 登録の関数の後・同じ取引) と、下書きの間の商品の画面 (lib/master-reg-parent.mjs)
 * p_seen = 画面が見ていた今の代表 (無し = null。大文字小文字は問わない)・p_parent_code = 決める代表 (null / 空 = 外す)
 * 決まり:
 *   単品で商品がある・登録の状態が下書き (draft)・今の代表が画面が見ていたものと同じ (違えば version_conflict)・
 *   代表 = Company DB の「ほかの単品」(SKU のコード) か「名札」(SKU を持たない商品の display_code・1 つだけ)。セット・例外の SKU・自分は不可。
 *   ほかのまとまりに入っている単品は不可 (parent_nested = そのまとまりの名札を選ぶ。2 段のまとまりを作らない)。
 *   入れるコードは Company DB の書き方 (名札 = display_code / 単品 = SKU のコード) にそろえる。形 = NE のコード (英数字・- _ の 30 字まで)
 *   NE にあるかはここでは見ない (NE 登録の CSV を作るとき ops.ne_reg_canonical が NE の元のコードで確かめて止める)
 *   その商品の NE 登録の CSV を配った後 (issued / import_declared / partial) = 直せない (reg_csv_issued)・作っただけ = そのファイルを使わないにする
 * 変わらない = 何も書かない (no_change)。鍵: 段階の共有 → マスタの書き込みの共有 → SKU → CSV → 約束 (reg_parent_set) → 書く → 保存の記録 done
 */
create function ops.set_registration_parent(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint, p_seen text, p_parent_code text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_sku    record;
  v_tgt    record;
  v_reg    text;
  v_cur    text;
  v_want   text := nullif(pg_catalog.btrim(coalesce(p_parent_code, '')), '');
  v_seen   text := nullif(pg_catalog.btrim(coalesce(p_seen, '')), '');
  v_kind   text;
  v_name   text;
  v_n      integer;
  v_sup    jsonb;
  v_result jsonb;
begin
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if v_want is not null and v_want !~ '^[A-Za-z0-9_-]{1,30}$' then
    raise exception 'invalid_value: 代表のコード % は NE のコードの形 (英数字・- と _ の 30 字まで) でない', v_want using errcode = '22023';
  end if;
  perform ops.reg_write_gate(p_ownership, '{}');   -- 段階 new_open・持ち主表 (代表の持ち主は NE のまま = company は求めない。core.products の親は書かない)
  select k.sku_id, k.code, k.code_norm, k.sku_kind, k.product_id into v_sku from core.skus k where k.sku_id = p_sku_id;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if v_sku.sku_kind is distinct from 'single' or v_sku.product_id is null then
    raise exception 'invalid_input: 代表 (色違い・サイズ違い) を決められるのは商品のある単品だけ' using errcode = '22023';
  end if;
  perform ops.ne_reg_lock_skus(array[p_sku_id]);   -- SKU の鍵 → CSV の鍵 (保存・CSV を作る / 配る / 申告と並ぶ)
  select r.state into v_reg from ops.master_registrations r where r.sku_id = p_sku_id;
  if v_reg is distinct from 'draft' then
    raise exception 'not_draft: 代表を決められるのは下書きの間だけ (今 %)。NE に送った後の代表は NE で直す', coalesce(v_reg, '登録の状態なし') using errcode = 'P0001';
  end if;
  select x.parent_code into v_cur from ops.registration_parents x where x.sku_id = p_sku_id;
  if pg_catalog.lower(v_cur) is distinct from pg_catalog.lower(v_seen) then
    raise exception 'version_conflict: 画面を開いた後にこの商品の代表が変わった (今 %)', coalesce(v_cur, 'なし') using errcode = 'P0001';
  end if;
  -- 代表を Company DB で引く (単品のコード → 名札の display_code の順)・書き方をそろえる
  if v_want is not null then
    if pg_catalog.lower(v_want) = v_sku.code_norm then raise exception 'invalid_value: 自分自身は代表にできない' using errcode = '22023'; end if;
    select k.code, k.sku_kind, k.product_id, p.name, p.parent_product_id, pp.display_code as nest into v_tgt
      from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
     where k.company_id = 1 and k.code_norm = pg_catalog.lower(v_want);
    if found then
      if v_tgt.sku_kind is distinct from 'single' or v_tgt.product_id is null then
        raise exception 'parent_not_single: % は % なので代表にできない (代表は単品か名札)', v_tgt.code, case v_tgt.sku_kind when 'set' then 'セット' else '例外の SKU' end using errcode = 'P0001';
      end if;
      if v_tgt.product_id = v_sku.product_id then raise exception 'invalid_value: 自分自身は代表にできない' using errcode = '22023'; end if;
      if v_tgt.parent_product_id is not null then
        raise exception 'parent_nested: % は名札 % のまとまりに入っている (代表には % を選ぶ)', v_tgt.code, coalesce(v_tgt.nest, '?'), coalesce(v_tgt.nest, 'その名札') using errcode = 'P0001';
      end if;
      v_want := v_tgt.code; v_kind := 'single'; v_name := v_tgt.name;
    else
      select pg_catalog.count(*)::integer, pg_catalog.min(p.display_code), pg_catalog.min(p.name) into v_n, v_want, v_name from core.products p
       where p.company_id = 1 and core.norm_code(p.display_code) = pg_catalog.lower(pg_catalog.btrim(p_parent_code))
         and not exists (select 1 from core.skus k where k.product_id = p.product_id);
      if v_n = 0 then raise exception 'parent_not_found: 代表のコード % は Company DB に無い (名札か単品のコード)', pg_catalog.btrim(p_parent_code) using errcode = 'P0001'; end if;
      if v_n > 1 then raise exception 'parent_ambiguous: 代表のコード % の名札が % つある (どれか決められない)', pg_catalog.btrim(p_parent_code), v_n using errcode = 'P0001'; end if;
      v_kind := 'tag';
    end if;
    if v_want !~ '^[A-Za-z0-9_-]{1,30}$' then
      raise exception 'invalid_value: 代表のコード % は NE のコードの形 (英数字・- と _ の 30 字まで) でない', v_want using errcode = '22023';
    end if;
  end if;
  if v_cur is not distinct from v_want then
    return pg_catalog.jsonb_build_object('ok', true, 'code', v_sku.code, 'no_change', true, 'variation_parent', v_cur);
  end if;
  if exists (select 1 from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state in ('issued', 'import_declared', 'partial')) then
    raise exception 'reg_csv_issued: この商品の NE 登録の CSV を配った後なので代表は直せない (先にそのファイルを使わないにする)' using errcode = 'P0001';
  end if;
  perform ops.open_reg_write('reg_parent_set', p_request_id, p_actor_id, p_reason, p_ownership, p_sku_id, '{}'::bigint[], array[v_sku.product_id],
    ops.reg_hash(pg_catalog.jsonb_build_object('op', 'reg_parent_set', 'sku_id', p_sku_id, 'from', v_cur, 'to', v_want, 'reason', p_reason)),
    pg_catalog.jsonb_build_object('sku_id', p_sku_id::text));
  v_sup := ops.ne_reg_supersede_built(array[p_sku_id], p_actor_id, '代表 (色違い・サイズ違い)');
  if v_want is null then
    delete from ops.registration_parents where sku_id = p_sku_id;
  else
    insert into ops.registration_parents (sku_id, parent_code, parent_norm, request_id, set_by) values (p_sku_id, v_want, pg_catalog.lower(v_want), p_request_id, p_actor_id)
      on conflict (sku_id) do update set parent_code = excluded.parent_code, parent_norm = excluded.parent_norm, request_id = excluded.request_id,
                                         set_by = excluded.set_by, set_at = pg_catalog.now();
  end if;
  v_result := pg_catalog.jsonb_build_object('ok', true, 'code', v_sku.code, 'kind', 'single', 'request_id', p_request_id::text,
    'changed', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('field', 'variation_parent', 'label', '代表 (色違い・サイズ違い)', 'from', v_cur, 'to', v_want, 'ne', 'reg_csv')),
    'variation_parent', case when v_want is null then null else pg_catalog.jsonb_build_object('code', v_want, 'kind', v_kind, 'name', v_name) end,
    'superseded', v_sup -> 'superseded');
  perform ops.close_reg_write(v_result, v_sku.code);
  return v_result;
end $$;
revoke all on function ops.set_registration_parent(uuid, text, text, jsonb, bigint, text, text) from public;

-- ═══════════ 3. NE 登録の CSV の材料 (0053 の ops.ne_reg_canonical + 登録の代表) ═══════════
/**
 * 1 つの SKU の新規登録の CSV の行と NE で確かめる値を、Company DB の今の値 (core.* / ops.*) から作る (0053 と同じ。🆕 0061 = 登録の代表)。
 * lib/master-reg-csv.mjs の regMaterialOf と同じ決まり。戻り値 { cells, expected, blockers: [理由] }。呼び手は SKU (と構成品) の鍵の後に呼ぶ
 */
create or replace function ops.ne_reg_canonical(p_sku_id bigint, p_day date) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  v_blk    text[] := '{}';
  v_price  bigint;
  v_tax    numeric;
  v_taxc   text;
  v_hand   text;
  v_cost   record;
  v_costv  bigint;
  v_nsup   integer;
  v_sup    record;
  v_supc   text;
  v_par    text;
  v_parc   text := 'empty';
  v_pr     record;
  v_jans   text[];
  v_cells  jsonb := '[]'::jsonb;
  v_exp    jsonb;
  v_req    jsonb;
  r        record;
  v_kids   jsonb := '[]'::jsonb;
  v_n      integer := 0;
  v_ntax   integer := 0;
  v_badtax boolean := false;
begin
  select k.sku_id, k.code, k.code_norm, k.sku_kind, k.name, k.tax_rate, k.handling, k.standard_price_jpy, k.product_id, p.parent_product_id, pp.display_code as parent_code,
         rp.parent_code as reg_parent_code
    into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
    left join ops.registration_parents rp on rp.sku_id = k.sku_id
   where k.sku_id = p_sku_id;
  if not found then return pg_catalog.jsonb_build_object('blockers', pg_catalog.jsonb_build_array('商品が無い')); end if;
  if not ops.ne_reg_name_ok(s.name) then v_blk := v_blk || '名前を CSV に書けない'::text; end if;
  if s.standard_price_jpy is null or s.standard_price_jpy <> pg_catalog.trunc(s.standard_price_jpy::numeric) or s.standard_price_jpy not between 1 and 999999999 then
    v_blk := v_blk || '標準売価が無い (1〜999,999,999 円)'::text;
  else
    v_price := s.standard_price_jpy::bigint;
  end if;
  if s.sku_kind = 'single' then
    v_tax := case when s.tax_rate = 0.1 then 0.1 when s.tax_rate = 0.08 then 0.08 end;
    v_taxc := case when v_tax = 0.1 then '10' when v_tax = 0.08 then '8' end;
    if v_taxc is null then v_blk := v_blk || '税率が無い (8% か 10%)'::text; end if;
    v_hand := case s.handling when 'active' then '0' when 'discontinued' then '1' end;
    if v_hand is null then v_blk := v_blk || '取扱区分が 取扱中 / 中止 でない'::text; end if;
    -- p_day の原価 (lib/master-write.mjs の costAsOfJoin と同じ選び方)
    select y.cost_jpy, y.cost_status into v_cost from core.sku_costs y
     where y.sku_id = p_sku_id and y.valid_from <= p_day and (y.valid_to is null or y.valid_to >= p_day)
     order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1;
    if v_cost.cost_status is null or v_cost.cost_status not in ('COMPLETE', 'OVERRIDDEN') or v_cost.cost_jpy is null
       or v_cost.cost_jpy <> pg_catalog.trunc(v_cost.cost_jpy) or v_cost.cost_jpy < 1 then
      v_blk := v_blk || '原価が無い (今日の原価・1 円以上)'::text;
    else
      v_costv := v_cost.cost_jpy::bigint;
    end if;
    select pg_catalog.count(*) into v_nsup from core.supplier_skus x where x.sku_id = p_sku_id and x.is_primary;
    if v_nsup <> 1 then
      v_blk := v_blk || '代表の仕入先が決まっていない'::text;
    else
      select su.supplier_id, su.code, rg.state as reg_state into v_sup
        from core.supplier_skus x join core.suppliers su on su.supplier_id = x.supplier_id
        left join ops.supplier_registrations rg on rg.supplier_id = su.supplier_id
       where x.sku_id = p_sku_id and x.is_primary;
      v_supc := core.canonical_supplier_code(v_sup.code);
      if v_supc is null or v_supc !~ '^[0-9]{4}$' then v_blk := v_blk || '代表の仕入先のコードが NE の形 (4 桁の数字) でない'::text; v_supc := null; end if;
      if v_sup.reg_state is not null and v_sup.reg_state <> 'ne_confirmed' then v_blk := v_blk || '代表の仕入先の「NE に登録した」の申告がまだ'::text; end if;
    end if;
    -- 代表 (親): なし = empty / あり = 名札か商品の NE の元の書き方 (名札を先に)
    --   🆕 0061: 新商品の登録で選んだ代表 (ops.registration_parents) があればそれ (Company DB の親より先)。NE の元のコードに無い = 止める
    if s.reg_parent_code is not null then
      v_par := core.norm_code(s.reg_parent_code);
      select c.state, c.ne_code into v_pr from ops.master_ne_codes c where c.code_norm = v_par and c.kind in ('rep', 'product')
       order by (c.kind = 'rep') desc limit 1;
      if v_pr.state is distinct from 'ok' then v_blk := v_blk || ('選んだ代表 ' || s.reg_parent_code || ' が NE に無い (NE の代表・商品のコードに無いか、書き方が 1 つに決まらない)');
      else v_parc := v_pr.ne_code; end if;
    elsif s.parent_product_id is not null then
      v_par := core.norm_code(coalesce(s.parent_code, ''));
      select c.state, c.ne_code into v_pr from ops.master_ne_codes c where c.code_norm = v_par and c.kind in ('rep', 'product')
       order by (c.kind = 'rep') desc limit 1;
      if coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;
      else v_parc := v_pr.ne_code; end if;
    end if;
    select coalesce(pg_catalog.array_agg(e.external_value order by e.external_id_row), '{}') into v_jans from core.external_ids e
     where s.product_id is not null and e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null;
    if pg_catalog.cardinality(v_jans) > 1 then v_blk := v_blk || 'JAN が 2 つ以上ある (NE には 1 つ)'::text; end if;
    if exists (select 1 from pg_catalog.unnest(v_jans) j where not ops.jan_check_ok(j)) then v_blk := v_blk || 'JAN のチェック数字が合わない'::text; end if;
    v_cells := pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_supc, ''), coalesce(v_costv::text, ''), coalesce(v_price::text, ''),
      coalesce(v_taxc, ''), coalesce(v_hand, ''), v_parc, coalesce(v_jans[1], 'empty')));
    v_exp := pg_catalog.jsonb_build_object('kind', 'single', 'values', pg_catalog.jsonb_build_object('name', s.name, 'supplier', v_supc, 'cost', v_costv, 'price', v_price,
      'tax_rate', v_tax, 'handling', s.handling, 'parent', v_par));
  elsif s.sku_kind = 'set' then
    -- 構成 = 開いている構成の依頼 (新しいセットは core.sku_components が空) か、今の構成 (並び = 依頼の sort / 今の sort_order・コード)
    select q.rows into v_req from ops.sku_component_requests q where q.set_sku_id = p_sku_id and q.status = 'open';
    for r in
      select k.sku_id, k.code, k.code_norm, k.tax_rate, x.qty, rg.state as reg_state, nc.state as nc_state, nc.ne_code
        from (select (e ->> 'sku_id')::bigint as sku_id, (e ->> 'qty')::numeric as qty, (e ->> 'sort')::numeric as srt, null::text as cn
                from pg_catalog.jsonb_array_elements(coalesce(v_req, '[]'::jsonb)) e where v_req is not null
              union all
              select c.child_sku_id, c.qty::numeric, c.sort_order::numeric, null::text from core.sku_components c where v_req is null and c.parent_sku_id = p_sku_id) x
        left join core.skus k on k.sku_id = x.sku_id
        left join ops.master_registrations rg on rg.sku_id = x.sku_id
        left join ops.master_ne_codes nc on nc.kind = 'product' and nc.code_norm = k.code_norm
       order by x.srt, k.code_norm
    loop
      v_n := v_n + 1;
      if r.code is null then v_blk := v_blk || '構成品の SKU が無い'::text; continue; end if;
      if r.tax_rate is null or r.tax_rate not in (0.1, 0.08) then v_badtax := true; elsif r.tax_rate = 0.08 then v_ntax := v_ntax + 1; end if;
      if r.reg_state is null or r.reg_state not in ('ne_confirmed', 'distributable', 'available') then v_blk := v_blk || ('構成品 ' || r.code || ' が NE 確認済みでない'); end if;
      if r.nc_state is distinct from 'ok' then v_blk := v_blk || ('構成品 ' || r.code || ' の NE の書き方が確かめられない'); end if;
      if r.qty is null or r.qty <> pg_catalog.trunc(r.qty) or r.qty not between 1 and 999 then v_blk := v_blk || ('構成品 ' || r.code || ' の数量'); end if;
      v_cells := v_cells || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_price::text, ''), '%TAX%', coalesce(r.ne_code, ''),
        coalesce(r.qty::bigint::text, '')));
      v_kids := v_kids || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code_norm', r.code_norm, 'qty', r.qty::bigint));
    end loop;
    if v_n = 0 then v_blk := v_blk || '構成品が無い'::text; end if;
    -- セットの税率 = 構成品の税率が全部分かるときだけ・混ざれば低い方 (lib/master-set-rules.js の deriveSetTaxCdb)
    v_taxc := case when v_badtax or v_n = 0 then null when v_ntax > 0 then '8' else '10' end;
    if v_taxc is null then v_blk := v_blk || 'セットの税率が決まらない (構成品の税率)'::text; end if;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_set(c, '{3}', pg_catalog.to_jsonb(coalesce(v_taxc, ''))) order by o), '[]'::jsonb) into v_cells
      from pg_catalog.jsonb_array_elements(v_cells) with ordinality as t(c, o);
    v_exp := pg_catalog.jsonb_build_object('kind', 'set', 'values', pg_catalog.jsonb_build_object('name', s.name, 'price', v_price, 'children', v_kids));
  else
    v_blk := v_blk || '例外の SKU は NE に登録しない'::text;
  end if;
  return pg_catalog.jsonb_build_object('cells', v_cells, 'expected', v_exp, 'blockers', pg_catalog.to_jsonb(v_blk));
end $$;
revoke all on function ops.ne_reg_canonical(bigint, date) from public;
