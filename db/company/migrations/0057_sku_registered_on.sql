-- 0057: 商品の登録日 (core.skus.registered_on。2026-10-05 中原さんの要望「商品登録日を持たせてほしい。既存の商品は NE のデータを取得して」)
--
-- 何を持つか: SKU (NE の商品コードの粒度) ごとに「いつ登録した商品か」= JST の日付 1 つと、その出どころ 1 つ。
--   registered_on        date  = 登録日 (JST の暦日。時刻は持たない = 一覧で並べる・絞るのは日付で足りる。NE の作成日の時刻は NE の画面で見る)
--   registered_on_source text  = 出どころ
--     'ne'         = NE の商品マスタの作成日 (goods_creation_date → raw_ne_products.作成日 → 商品管理リストの公開 snapshot の 登録日。夜間ロードが入れる)
--     'portal'     = ポータルの新商品の登録 (apps/master-edit/new・ops.register_new_sku) で作った日 (下で作り直す登録の関数が明示して入れる)
--     'first_seen' = 夜間ロードが NE で初めて見て SKU を作った日 (NE の作成日が取れないセット・例外の SKU だけ。NE のセットの API の作成日は今は取っていない)
-- 決まり:
--   ・一度入った値は変えない (下の trigger)。夜間ロードは registered_on が空の行だけを埋める (NE の値が後から変わっても上書きしない)。
--     ポータルで登録した商品は、あとで NE に登録しても (NE の作成日 = CSV を取り込んだ日) ポータルの日のまま
--   ・既にある行は空のまま。ただし 0057 の前にポータルで登録した SKU (ops.master_registrations の origin = 'new_entry' = ⑤-2a の登録の関数が作った) だけは、
--     この migration の中で先に portal・登録の状態の行を作った日 (created_at の JST の日付 = 登録の関数の v_today と同じ取引の時刻) で埋める (下の「0057 の前の
--     ポータルの登録」)。埋めないと、単品は NE に登録した後の夜間ロードで NE の作成日 (= CSV を取り込んだ日) の ne で確定し、セットは NE の作成日を取らないので
--     ずっと空になる (#1617 Codex R2 Medium)。この埋めも変更の記録と version が 1 回付く (source_system = migration_0057)
--     それ以外の既にある行は、適用の後の最初の夜間ロードが、商品管理リストの公開 snapshot の 登録日 から単品を埋める
--     (2026-10-05 の本番: 単品 5,059 件は全部 NE の作成日あり・セット 2,229 件と例外 89 件は無い = 空のまま = 画面は「—」)
--   ・🚨 列の既定値は持たない (= 空)。書き手が明示する: 登録の関数 = 今日 + portal / 夜間ロード = NE の作成日・初めて見た日・空。
--     既定値に「今日 + portal」を置くと、0057 の後に残った古い夜間ロードのプロセスや戻したコード (2 つの列を書かない INSERT) が、
--     NE から来た新商品を portal・その日で確定させ、trigger で直せなくなる (#1617 Codex R1 Medium)。列を書かない INSERT = 空 = 次の夜間ロードが NE の作成日で埋める
--   ・持ち主表 (config/master-ownership.mjs) のキーにはしない = 'load' / 'company' のどちらでも同じ「空なら 1 回だけ入れる」(切替の epoch の対象の外)
--   ・画面のロール master_edit はこの列を UPDATE できない (create-master-edit-roles.mjs の列の権限に入れない)。読むのは表の SELECT で読める
--   ・変更の記録 (0026 の events.master_change_events) と version は、ほかの列と同じに付く (最初の夜間ロードで単品 ~5,000 件 × 2 列の UPDATE の記録が 1 回だけ増える)。
--     🚨 version が 1 回進む = その晩に開いていた単品の画面と、その単品を構成品に含むセット・構成の依頼の画面の保存は 1 度 409 (開き直し)
-- 直すとき (間違った値を直す保守): 持ち主が trg_skus_registered_on_fixed を disable → 直す → enable (同じ取引で)。アプリからは直さない

alter table core.skus
  add column registered_on date,
  add column registered_on_source text;

alter table core.skus
  add constraint ck_skus_registered_on_source check (registered_on_source in ('ne', 'portal', 'first_seen')),
  add constraint ck_skus_registered_on_pair check ((registered_on is null) = (registered_on_source is null)),
  add constraint ck_skus_registered_on_range check (registered_on is null or registered_on >= date '2000-01-01');

comment on column core.skus.registered_on is '登録日 (JST の日付)。一度入ったら変えない。出どころは registered_on_source (ne = NE の作成日 / portal = ポータルで登録した日 / first_seen = 夜間ロードが初めて見た日)。空 = 分からない (0057 の前からあるセット・例外・NE の作成日がまだ取れていない単品)。既定値なし = 書き手が明示する';
comment on column core.skus.registered_on_source is '登録日の出どころ: ne / portal / first_seen (registered_on と同時に空 / 値あり)';

-- 一度入った登録日は変えない (空 → 値 だけ通す)。夜間ロードの「空の行だけ」の決まりを DB でも守る
create function core.guard_sku_registered_on() returns trigger language plpgsql set search_path = pg_catalog, core, pg_temp as $$
begin
  if old.registered_on is not null
     and (new.registered_on is distinct from old.registered_on or new.registered_on_source is distinct from old.registered_on_source) then
    raise exception 'registered_on_fixed: SKU % の登録日 (% · %) は一度入ったら変えない', old.code, old.registered_on, old.registered_on_source
      using errcode = '23514';
  end if;
  return new;
end $$;
create trigger trg_skus_registered_on_fixed before update of registered_on, registered_on_source on core.skus
  for each row execute function core.guard_sku_registered_on();

-- 0057 の前のポータルの登録 (#1617 Codex R2 Medium): ⑤-2a (0052) の登録の関数で作った SKU = 登録の状態の行の origin = 'new_entry'。
--   登録日 = その行を作った時刻 (created_at = 登録の関数の取引の now()) の JST の日付 = 0057 の後の登録の関数が書く v_today と同じ決め方。
--   状態 (draft〜available・cancelled) は問わない = ポータルで作った SKU であることは変わらない。空の行だけ (trigger は 空 → 値 を通す)。
--   変更の記録 (0026) と version が付く = 本番で数えた件数は README の 0057 の節。設定は同じ取引の中だけ (is_local)・終わったら消す
select pg_catalog.set_config('core.actor_type', 'system', true), pg_catalog.set_config('core.source_system', 'migration_0057', true),
       pg_catalog.set_config('core.reason', '0057: 0057 の前にポータルで登録した SKU の登録日 (ops.master_registrations の origin = new_entry の created_at の JST の日付)', true);
update core.skus s
   set registered_on = (r.created_at at time zone 'Asia/Tokyo')::date,
       registered_on_source = 'portal'
  from ops.master_registrations r
 where r.sku_id = s.sku_id
   and r.company_id = s.company_id
   and r.origin = 'new_entry'
   and s.registered_on is null;
select pg_catalog.set_config('core.actor_type', '', true), pg_catalog.set_config('core.source_system', '', true), pg_catalog.set_config('core.reason', '', true);

-- 新商品の登録の関数 (0052 の ops.register_new_sku) を作り直す: SKU の INSERT に登録日 (この取引の JST の今日 = v_today) と portal を明示して足すだけ。
--   🚨 ほかは 0052 の定義と一字も変えない (0053〜0056 は作り直していない = 0052 が最新の定義)。署名・security definer・search_path・持ち主は同じ。
--   create or replace = 権限 (0052 の revoke all from public・create-master-edit-roles.mjs の master_edit への grant execute) はそのまま残る
--   試験: scripts/test-sku-registered-on.mjs が「0052 の本文との差 = この INSERT だけ」を機械で確かめる
create or replace function ops.register_new_sku(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user  text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tx       bigint := pg_catalog.txid_current();
  v_today    date := (pg_catalog.now() at time zone 'Asia/Tokyo')::date;
  v_kind     text := p_entry ->> 'kind';
  v_code     text := p_entry ->> 'code';
  v_prod     jsonb := p_entry -> 'product';
  v_s        jsonb := p_entry -> 'sku';
  v_cost     jsonb := p_entry -> 'cost';
  v_req      jsonb := p_entry -> 'component_request';
  v_card     jsonb := p_entry -> 'card';
  v_reason   text := nullif(p_reason, '');
  v_name     text;
  v_price    bigint;
  v_tax      numeric;
  v_tclass   text;
  v_handling text;
  v_override smallint;
  v_own      text;
  v_sales    smallint;
  v_ship_c   text;
  v_ship_m   text;
  v_ship_y   bigint;
  v_reorder  numeric;
  v_supplier bigint;
  v_cjpy     bigint;
  v_csrc     text;
  v_cstat    text;
  v_creason  text;
  v_der      jsonb;
  v_rows     jsonb;
  v_rhash    text;
  v_payload  jsonb;
  v_phash    text;
  v_written  jsonb;
  v_whash    text;
  v_phase    text;
  v_owner    text;
  v_hash     text;
  v_bad      text;
  v_product  bigint;
  v_sku      bigint;
  v_sess     uuid := pg_catalog.gen_random_uuid();
  v_event    uuid;
  v_result   jsonb;
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.master_cutover'));   -- 段階を変える取引と並ぶ (画面は先に取っている = 同じ鍵)
  perform pg_catalog.pg_advisory_xact_lock_shared(core.master_write_lock_key());                -- 夜間ロードと並ぶ (画面は先に取っている = 同じ鍵)
  -- 形 (呼び手・持ち主表・要求のハッシュ・中身の外形)
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if p_actor_id is null or length(btrim(p_actor_id)) = 0 or length(p_actor_id) > 320 or p_actor_id ~ '[[:cntrl:]]' then
    raise exception 'invalid_input: 登録する人 (actor_id) の形が違う' using errcode = '22023';
  end if;
  if v_reason is not null and (length(v_reason) > 200 or v_reason ~ '[[:cntrl:]]') then raise exception 'invalid_input: 理由は 200 字まで・制御文字なし' using errcode = '22023'; end if;
  if p_ownership is null or jsonb_typeof(p_ownership) <> 'object'
     or exists (select 1 from jsonb_each(p_ownership) e where jsonb_typeof(e.value) <> 'string' or (e.value #>> '{}') not in ('load', 'company')) then
    raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023';
  end if;
  if coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: 要求のハッシュ (64 桁の 16 進) が要る' using errcode = '22023'; end if;
  if p_entry is null or jsonb_typeof(p_entry) <> 'object' or v_kind is null or v_kind not in ('single', 'set') or jsonb_typeof(v_s) is distinct from 'object' then
    raise exception 'invalid_input: 登録の中身 (kind = single / set・sku) が要る' using errcode = '22023';
  end if;
  if (v_kind = 'single') is distinct from (jsonb_typeof(v_prod) = 'object') then
    raise exception 'invalid_input: 単品は商品 (product) が要り、セットは商品を作らない' using errcode = '22023';
  end if;
  if (v_kind = 'set') is distinct from (jsonb_typeof(v_req) = 'object') then
    raise exception 'invalid_input: セットは構成の依頼 (component_request) が要り、単品は構成を持たない' using errcode = '22023';
  end if;
  -- 結果は DB が作る (呼び手の結果は受け取らない = 保存の記録の結果を偽れない・#1566 Codex R4 Medium)
  if p_entry ? 'result' then raise exception 'invalid_input: 登録の結果 (result) は DB が作る (渡さない)' using errcode = '22023'; end if;
  if coalesce(p_entry -> 'supplier_id', 'null'::jsonb) <> 'null'::jsonb
     and (jsonb_typeof(p_entry -> 'supplier_id') not in ('number', 'string') or (p_entry ->> 'supplier_id') !~ '^[1-9][0-9]{0,17}$') then
    raise exception 'invalid_input: 代表の仕入先は番号' using errcode = '22023';
  end if;
  v_supplier := (p_entry ->> 'supplier_id')::bigint;
  if v_kind = 'set' and v_supplier is not null then raise exception 'invalid_input: セットに代表の仕入先は付けない' using errcode = '22023'; end if;
  -- 段階・持ち主表 (ops.begin_master_write と同じ)・backfill
  select phase, owner_hash into v_phase, v_owner from ops.master_cutover_state where id = 1;
  v_hash := ops.ownership_hash(p_ownership);
  if v_phase is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が % (new_open でない)', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
  if v_owner is distinct from v_hash then raise exception 'before_cutover: 持ち主表が切替のときの記録と違う' using errcode = 'P0001'; end if;
  if (select count(*) from ops.master_registration_backfill) <> 1 then
    raise exception 'backfill_missing: 既存の SKU の登録の状態 (backfill) が済んでいないので、新商品は登録しない' using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'invalid_input: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '22023';
  end if;
  -- コード (新しいコードの鍵 = 画面と同じ鍵 → 決まり)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.new_code:' || coalesce(core.norm_code(v_code), ''), 0));
  v_bad := ops.new_sku_code_problem(v_code);
  if v_bad is not null then raise exception '%: 商品コード % は新しい商品に使えない', v_bad, v_code using errcode = 'P0001'; end if;

  -- 値 (共通): 名前・売価・送料・推奨保有月数
  v_name := v_s ->> 'name';
  if jsonb_typeof(v_s -> 'name') is distinct from 'string' or v_name is distinct from btrim(v_name) or length(v_name) not between 1 and 255
     or v_name ~ '[[:cntrl:]]' or lower(v_name) = 'empty' then
    raise exception 'invalid_value: 名前は 1〜255 字 (前後の空白・制御文字なし・empty でない)' using errcode = '22023';
  end if;
  if jsonb_typeof(v_s -> 'standard_price_jpy') is distinct from 'number' or (v_s ->> 'standard_price_jpy') !~ '^[1-9][0-9]{0,8}$' then
    raise exception 'invalid_value: 売価は 1〜999,999,999 の整数' using errcode = '22023';
  end if;
  v_price := (v_s ->> 'standard_price_jpy')::bigint;
  v_ship_c := v_s ->> 'shipping_code';
  v_ship_m := v_s ->> 'shipping_method';
  if jsonb_typeof(v_s -> 'shipping_code') is distinct from 'string' or length(btrim(v_ship_c)) not between 1 and 20 or v_ship_c ~ '[[:cntrl:]]'
     or jsonb_typeof(v_s -> 'shipping_method') is distinct from 'string' or length(btrim(v_ship_m)) not between 1 and 200 or v_ship_m ~ '[[:cntrl:]]' then
    raise exception 'invalid_value: 送料コードと発送方法 (名前) が要る' using errcode = '22023';
  end if;
  if (v_s -> 'shipping_cost_jpy') is not null and jsonb_typeof(v_s -> 'shipping_cost_jpy') <> 'null'
     and (jsonb_typeof(v_s -> 'shipping_cost_jpy') <> 'number' or (v_s ->> 'shipping_cost_jpy') !~ '^[0-9]{1,9}$') then
    raise exception 'invalid_value: 送料は 0 以上の整数か無し' using errcode = '22023';
  end if;
  v_ship_y := case when jsonb_typeof(v_s -> 'shipping_cost_jpy') = 'number' then (v_s ->> 'shipping_cost_jpy')::bigint end;
  if (v_s -> 'reorder_months') is not null and jsonb_typeof(v_s -> 'reorder_months') <> 'null'
     and (jsonb_typeof(v_s -> 'reorder_months') <> 'number' or (v_s ->> 'reorder_months') !~ '^[0-9]{1,2}(\.[0-9])?$' or (v_s ->> 'reorder_months')::numeric > 60) then
    raise exception 'invalid_value: 推奨保有月数は 0〜60 (小数は 1 桁まで) か無し' using errcode = '22023';
  end if;
  v_reorder := case when jsonb_typeof(v_s -> 'reorder_months') = 'number' then (v_s ->> 'reorder_months')::numeric end;
  -- 原価の形 (無い / { jpy, source, status, valid_from, reason })
  if v_cost is not null and jsonb_typeof(v_cost) <> 'null' then
    if jsonb_typeof(v_cost) <> 'object' or jsonb_typeof(v_cost -> 'jpy') is distinct from 'number' or (v_cost ->> 'jpy') !~ '^[0-9]{1,9}$'
       or (v_cost ->> 'valid_from') is distinct from v_today::text
       or jsonb_typeof(v_cost -> 'reason') is distinct from 'string' or length(btrim(v_cost ->> 'reason')) not between 1 and 200 or (v_cost ->> 'reason') ~ '[[:cntrl:]]' then
      raise exception 'invalid_value: 原価は 0〜999,999,999 の整数・今日 (東京 %) から・理由つき', v_today using errcode = '22023';
    end if;
    v_cjpy := (v_cost ->> 'jpy')::bigint; v_csrc := v_cost ->> 'source'; v_cstat := v_cost ->> 'status'; v_creason := v_cost ->> 'reason';
  end if;

  if v_kind = 'single' then
    -- 単品: 税率と税区分が合う・取扱 active・セットだけの列は持たない・商品の名前 = SKU の名前・原価は manual / COMPLETE だけ
    if jsonb_typeof(v_s -> 'tax_rate') is distinct from 'number' or (v_s ->> 'tax_rate')::numeric not in (0.08, 0.10) then
      raise exception 'invalid_value: 単品の税率は 8%% か 10%%' using errcode = '22023';
    end if;
    v_tax := (v_s ->> 'tax_rate')::numeric;
    v_tclass := case when v_tax = 0.08 then 'REDUCED_8' else 'STANDARD_10' end;
    if (v_s ->> 'tax_class') is distinct from v_tclass then raise exception 'invalid_value: 税率 % と税区分 % が合わない', v_tax, v_s ->> 'tax_class' using errcode = '22023'; end if;
    if (v_s ->> 'handling') is distinct from 'active' then raise exception 'invalid_value: 新しい単品の取扱は active' using errcode = '22023'; end if;
    v_handling := 'active';
    if coalesce(v_s -> 'set_sales_class_override', 'null'::jsonb) <> 'null'::jsonb or coalesce(v_s -> 'handling_own', 'null'::jsonb) <> 'null'::jsonb then
      raise exception 'invalid_value: 単品にセットだけの列 (売上分類の上書き・セット自身の取扱) は付けない' using errcode = '22023';
    end if;
    if (v_prod ->> 'name') is distinct from v_name then raise exception 'invalid_value: 商品の名前は SKU の名前と同じ' using errcode = '22023'; end if;
    if coalesce(v_prod -> 'sales_class', 'null'::jsonb) <> 'null'::jsonb
       and (jsonb_typeof(v_prod -> 'sales_class') <> 'number' or (v_prod ->> 'sales_class') !~ '^[1-4]$') then
      raise exception 'invalid_value: 売上分類は 1〜4 か無し' using errcode = '22023';
    end if;
    v_sales := (v_prod ->> 'sales_class')::smallint;
    if jsonb_typeof(v_prod -> 'expiry_managed') is distinct from 'boolean'
       or coalesce(jsonb_typeof(v_prod -> 'inbound_date_managed'), 'null') not in ('boolean', 'null') then
      raise exception 'invalid_value: 有効期限の管理・入荷日の管理は あり / なし' using errcode = '22023';
    end if;
    if v_cjpy is not null and (v_csrc is distinct from 'manual' or v_cstat is distinct from 'COMPLETE') then
      raise exception 'invalid_value: 単品の原価は人が入れた原価 (manual / COMPLETE) だけ' using errcode = '22023';
    end if;
  else
    -- セット: 構成品を DB で確かめて導く値を DB で決め、入ってきた値と同じときだけ
    v_own := v_s ->> 'handling_own';
    if v_own is null or v_own not in ('active', 'discontinued') then raise exception 'invalid_value: セット自身の取扱は active / discontinued' using errcode = '22023'; end if;
    v_der := ops.new_set_derivation(v_req -> 'rows', v_own, v_today);
    v_rows := v_der -> 'rows';
    v_rhash := v_der ->> 'rows_hash';
    if (v_der ->> 'tax_rate') is null then raise exception 'set_underivable: 構成品の税率が未入力なので、セットの税率が決まらない' using errcode = 'P0001'; end if;
    v_tax := (v_der ->> 'tax_rate')::numeric; v_tclass := v_der ->> 'tax_class'; v_handling := v_der ->> 'handling';
    if jsonb_typeof(v_s -> 'tax_rate') is distinct from 'number' or (v_s ->> 'tax_rate')::numeric <> v_tax or (v_s ->> 'tax_class') is distinct from v_tclass
       or (v_s ->> 'handling') is distinct from v_handling then
      raise exception 'derived_mismatch: セットの税率・税区分・取扱が構成品から導いた値 (% / % / %) と違う', v_tax, v_tclass, v_handling using errcode = 'P0001';
    end if;
    if coalesce(v_s -> 'set_sales_class_override', 'null'::jsonb) <> 'null'::jsonb then
      if jsonb_typeof(v_s -> 'set_sales_class_override') <> 'number' or (v_s ->> 'set_sales_class_override') !~ '^[1-4]$' then
        raise exception 'invalid_value: 売上分類の上書きは 1〜4' using errcode = '22023';
      end if;
      v_override := (v_s ->> 'set_sales_class_override')::smallint;
      if (v_der ->> 'sales_from_components') is not null then
        raise exception 'derived_mismatch: 構成品から売上分類 (%) を導けるので、上書きはできない', v_der ->> 'sales_from_components' using errcode = 'P0001';
      end if;
    elsif (v_der ->> 'sales_from_components') is null then
      raise exception 'set_underivable: 構成品から売上分類を導けない (上書きが要る)' using errcode = 'P0001';
    end if;
    if v_cjpy is null then raise exception 'set_underivable: セットの原価が要る (構成品の合計か例外原価)' using errcode = 'P0001'; end if;
    if v_csrc = 'set_calc' then
      if v_cstat is distinct from 'COMPLETE' or (v_der ->> 'cost_status') is distinct from 'COMPLETE' or v_cjpy <> (v_der ->> 'cost_jpy')::bigint then
        raise exception 'derived_mismatch: セットの原価が構成品から導いた値 (% / %) と違う', v_der ->> 'cost_status', v_der ->> 'cost_jpy' using errcode = 'P0001';
      end if;
    elsif v_csrc is distinct from 'manual' or v_cstat is distinct from 'OVERRIDDEN' then
      raise exception 'invalid_value: セットの原価は構成品の合計 (set_calc / COMPLETE) か例外原価 (manual / OVERRIDDEN) だけ' using errcode = '22023';
    end if;
  end if;

  -- カードの知らせ: 写しの欄が書く値と同じ・SKU の番号は入れない・hash は DB が作る
  if v_card is not null and jsonb_typeof(v_card) <> 'null' then
    v_payload := v_card -> 'payload';
    if jsonb_typeof(v_card) <> 'object' or (v_card ->> 'schema_version') is distinct from 'ph-card-v1' or jsonb_typeof(v_payload) is distinct from 'object'
       or v_payload ? 'cdb_sku_id' or (v_payload ->> 'schema') is distinct from (v_card ->> 'schema_version')
       or (v_payload ->> 'code') is distinct from v_code or (v_payload ->> 'kind') is distinct from v_kind or (v_payload ->> 'name') is distinct from v_name
       or (v_payload -> 'price') is distinct from to_jsonb(v_price)
       or (v_payload -> 'shipping') is distinct from jsonb_build_object('code', v_ship_c, 'method', v_ship_m, 'cost_jpy', v_ship_y)
       or (v_payload -> 'components') is distinct from coalesce((select jsonb_agg(jsonb_build_object('code', e ->> 'code', 'qty', (e -> 'qty')) order by (e ->> 'sort')::integer)
                                                                  from jsonb_array_elements(v_rows) e), '[]'::jsonb)
       or (v_payload ->> 'created_by') is distinct from p_actor_id
       -- キーは決まった名前だけ (入れ子も)・URL などは文字か null・参考 URL は文字の配列 (#1566 Codex R4 Low)
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof(v_payload) = 'object' then v_payload else '{}'::jsonb end) k where k not in ('schema', 'code', 'kind', 'name', 'price', 'shipping', 'amazon_url', 'asin', 'official_url',
                                                                              'reference_urls', 'set_decision', 'yahoo', 'components', 'created_by'))
       or coalesce(jsonb_typeof(v_payload -> 'set_decision'), 'null') not in ('null', 'object')
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof((v_payload -> 'set_decision')) = 'object' then (v_payload -> 'set_decision') else '{}'::jsonb end) k where k not in ('decision', 'reason_code', 'reason_text'))
       or coalesce(jsonb_typeof(v_payload -> 'yahoo'), 'null') not in ('null', 'object')
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof((v_payload -> 'yahoo')) = 'object' then (v_payload -> 'yahoo') else '{}'::jsonb end) k where k not in ('price', 'price_sagawa', 'delivery_label', 'category_id', 'path'))
       or coalesce(jsonb_typeof(v_payload -> 'reference_urls'), 'null') <> 'array'
       or exists (select 1 from jsonb_array_elements(case when jsonb_typeof(v_payload -> 'reference_urls') = 'array' then v_payload -> 'reference_urls' else '[]'::jsonb end) u
                   where jsonb_typeof(u) <> 'string')
       or exists (select 1 from unnest(array['amazon_url', 'asin', 'official_url']) f where coalesce(jsonb_typeof(v_payload -> f), 'null') not in ('null', 'string')) then
      raise exception 'invalid_value: カードの知らせの写しの欄 (版・コード・種類・名前・売価・送料・構成品・作った人) が書く値と違うか、知らない欄・形の違う欄がある' using errcode = '22023';
    end if;
    v_phash := ops.js_stable_sha256(v_payload);
  end if;

  -- 関数が書く値のハッシュ (約束と保存の記録の payload_hash = DB が作る。アプリの要求のハッシュも入れて結ぶ)
  v_written := jsonb_build_object('request', p_payload_hash, 'kind', v_kind, 'code', v_code, 'name', v_name, 'tax_rate', v_tax, 'tax_class', v_tclass, 'handling', v_handling,
    'standard_price_jpy', v_price, 'shipping', jsonb_build_array(v_ship_c, v_ship_m, v_ship_y), 'reorder_months', v_reorder, 'set_sales_class_override', v_override,
    'handling_own', v_own, 'product', case when v_kind = 'single' then jsonb_build_object('sales_class', v_sales, 'expiry_managed', (v_prod -> 'expiry_managed'),
      'inbound_date_managed', coalesce(v_prod -> 'inbound_date_managed', 'null'::jsonb)) end,
    'supplier_id', v_supplier, 'cost', case when v_cjpy is not null then jsonb_build_array(v_cjpy, v_csrc, v_cstat, v_today, v_creason) end,
    'rows', v_rows, 'rows_hash', v_rhash, 'card', v_phash, 'reason', v_reason);
  v_whash := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_written::text, 'UTF8')), 'hex');

  -- 番号を振る (商品 → SKU)
  if v_kind = 'single' then v_product := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.products', 'product_id')::regclass); end if;
  v_sku := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.skus', 'sku_id')::regclass);
  -- 登録の約束 (0051 の約束の表・operation = sku_create)。行を入れる前に書く = 0051 の guard・変更の記録が この約束で見る。編集の印は無い (0 の 64 桁)・版は無い ({})
  --   約束の SKU の外部キーは、この取引だけ commit のときに確かめる (SKU の行はこの後に入れる)
  set constraints ops.fk_mws_sku deferred;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_sess, v_tx, p_request_id, 'sku_create', v_sku, '{}'::bigint[], case when v_product is null then '{}'::bigint[] else array[v_product] end,
            pg_catalog.repeat('0', 64), v_whash, '{}'::jsonb, p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, v_hash, p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_sess::text, true);
  -- 行を入れる: 商品 (単品) → SKU → 状態 draft → 仕入先 → 原価 → 構成の依頼 → カードの知らせ (どれも上で確かめた / DB で作った値だけ)
  if v_kind = 'single' then
    insert into core.products (product_id, company_id, display_code, name, sales_class, status, expiry_managed, inbound_date_managed, created_by_type, created_by_id)
      overriding system value
      values (v_product, 1, v_code, v_name, v_sales, 'active', (v_prod ->> 'expiry_managed')::boolean, (v_prod ->> 'inbound_date_managed')::boolean, 'human', p_actor_id);
  end if;
  -- 0057: 登録日 = この取引の JST の今日 (v_today = 原価の valid_from と同じ日)・出どころ portal を明示する (列の既定値には頼らない = 既定は空)
  insert into core.skus (sku_id, company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling, standard_price_jpy,
                         shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own, created_by_type, created_by_id,
                         registered_on, registered_on_source)
    overriding system value
    values (v_sku, 1, v_product, v_kind, v_code, v_name, v_tax, v_tclass, v_handling, v_price, v_ship_c, v_ship_m, v_ship_y, v_reorder, v_override, v_own, 'human', p_actor_id,
            v_today, 'portal');
  perform ops.create_sku_registration(v_sku, p_actor_id, p_request_id::text, v_reason);
  if v_supplier is not null then
    insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values (1, v_supplier, v_sku, true, 'human', p_actor_id);
  end if;
  if v_cjpy is not null then
    insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id)
      values (1, v_sku, v_cjpy, v_csrc, v_cstat, v_today, v_creason, 'human', p_actor_id);
  end if;
  if v_kind = 'set' then
    insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id)
      values (1, v_sku, v_rows, v_rhash, '[]'::jsonb,
              case when jsonb_typeof(v_req -> 'reason') = 'string' and length(v_req ->> 'reason') between 1 and 200 and (v_req ->> 'reason') !~ '[[:cntrl:]]' then v_req ->> 'reason' end,
              p_actor_id, p_request_id);
  end if;
  if v_phash is not null then
    insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
      values (1, v_sku, 'card_create', 'ph-card-v1', v_payload, v_phash, p_request_id, p_actor_id)
      returning event_id into v_event;
  end if;
  -- 保存の記録 done (約束どおり = 0051 の commit の確かめ)。結果は全部 DB の値から作る (確かめた / 導き直した値・振った番号・知らせ・要求のハッシュ)。
  --   気をつけること・このあと の文は lib/master-register.mjs の前の文と同じ (画面はこの結果をそのまま出す・同じ request_id の答えも同じ)
  v_result := jsonb_build_object(
    'ok', true, 'code', v_code, 'kind', v_kind, 'sku_id', v_sku::text, 'state', 'draft', 'request_id', p_request_id::text,
    'tax', jsonb_build_object('rate', v_tax, 'class', v_tclass), 'handling', v_handling,
    'cost', case when v_cjpy is not null then jsonb_build_object('jpy', v_cjpy, 'source', v_csrc) end,
    'shipping', jsonb_build_object('code', v_ship_c, 'method', v_ship_m, 'cost_jpy', v_ship_y),
    'components', coalesce((select jsonb_agg(jsonb_build_object('code', e ->> 'code', 'qty', e -> 'qty') order by (e ->> 'sort')::integer)
                              from jsonb_array_elements(v_rows) e), '[]'::jsonb),
    'warnings', to_jsonb(array_remove(array[
        case when v_tclass = 'MIXED' then '構成品の税率が 8% と 10% で混ざっています (セットの税率は低い方の 8%・MIXED)' end,
        case when (v_der ->> 'stopped_codes') is not null then format('中止の構成品 (%s) があるので、セットも中止になります', v_der ->> 'stopped_codes') end,
        case when v_name ~ '_(白ビ袋|梱機プ|長3封|白プチ|ネコ段|K-44|K-50|K-60|厚紙封|パフ箱|その他)$'
             then '名前の末尾に資材の印があります。資材は梱包アプリで登録します (新しい名前には付けない・D-47)' end], null)),
    'ne_steps', to_jsonb(array_remove(array[
        '下書きで登録しました。NE・ロジザードへの登録 (新規登録の CSV) はまだです (次の段階で「NE 登録へ進む」を足します)',
        case when v_kind = 'set' then 'セットの構成は「構成の依頼」として持っています。NE に登録して NE の構成が同じと確かめたら、今の構成になります' end], null)),
    'card', case when v_event is null then null else jsonb_build_object('event_id', v_event::text, 'status', 'pending') end,
    'request_payload_hash', p_payload_hash);
  insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
    values (p_request_id, 1, 'sku_create', v_code, v_sku, p_actor_id, v_whash, 'done', v_result,
            least(coalesce((p_entry ->> 'started_at')::timestamptz, pg_catalog.now()), pg_catalog.clock_timestamp()));
  -- 約束を閉じる = この関数の外では (同じ取引でも) 画面のロールは登録の約束で書けない。約束の行と done は commit の確かめに残る
  perform pg_catalog.set_config('ops.master_write_session', '', true);
  return v_result;
end $$;
