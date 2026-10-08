-- 0061: 新商品の登録 = 下書きの保存で要る欄を足す (2026-10-08 中原さん「下書き保存時にわかる内容だから必須にする」)
--   1. 単品の売上分類が要る (1〜4。画面の「あとで」を無くした)。セットは今までどおり: 構成品から導く・導けないときだけ上書きが要る (set_underivable)
--   2. 推奨保有月数が要る (単品もセットも・0〜60。0 は入れてよい)
--   3. ロジザードの有効期限の管理は 0052 から「あり / なし (boolean)」が要る = 関数は変えない (画面とサーバーが「選ばないと なし」をやめた)。
--      入荷日の管理は今までどおり無くてよい (null = 不明)
-- 何をするか: ops.register_new_sku (0060 の版) を create or replace で置き換える。🚨 ほかは 0060 の本文と一字も変えない
--   (試験: scripts/test-master-register.mjs の [G-0061] が「0060 の本文との差 = 上の 2 か所 (4 行) だけ」を機械で確かめる)。
--   署名・security definer・search_path・持ち主は同じ。create or replace = 権限 (0052 の revoke all from public・
--   create-master-edit-roles.mjs の master_edit への grant execute) はそのまま残る
-- 既にある行は変えない: 0061 の前に下書きで登録した単品に売上分類・推奨保有月数が無くても、そのまま (商品の画面で入れる)
-- NE 登録の CSV (ops.ne_reg_build) は変えない: 売上分類・推奨保有月数・ロジザードの管理は NE 登録の CSV の列に無い

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
  perform ops._new_entry_lease_shared_locks();   -- 🆕 0058 §3.10: 段階の鍵より前に許可の共有の鍵 (直接呼んでもアプリと同じ順。request の鍵は呼び手 = アプリが先に取る)
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
  -- 🆕 0060 (中原さん 2026-10-08): 単品は代表の仕入先が要る (NE の商品マスタで必須の項目)。セットは今までどおり付けない
  if v_kind = 'single' and v_supplier is null then raise exception 'invalid_value: 単品は代表の仕入先が要る (NE で必須の項目)' using errcode = '22023'; end if;
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
  -- 🆕 0058 (§3.7・§3.8): 新商品の開放の許可 (種類ごと・共有の鍵 = 取り消し・照合 ② の始めに閉じる・結果の記録と並ぶ)。無い = new_entry_closed (アプリは 409)
  if v_kind = 'single' then perform ops._require_new_entry_lease('single'); end if;   -- セットは sku_components を広げるまで画面の門 (NEW_ENTRY_KEYS) のまま
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
  -- 🆕 0060 (中原さん 2026-10-08): 発送方法は無くてよい (届いてサイズを見てから商品の画面で入れる)。無い = 送料コード・発送方法 (名前)・送料の 3 つとも null。
  --   ある = 今までどおり送料コードと発送方法 (名前) の両方が要る (片方だけ・送料だけは断る)
  if coalesce(jsonb_typeof(v_s -> 'shipping_code'), 'null') = 'null' and coalesce(jsonb_typeof(v_s -> 'shipping_method'), 'null') = 'null' then
    if coalesce(jsonb_typeof(v_s -> 'shipping_cost_jpy'), 'null') <> 'null' then
      raise exception 'invalid_value: 送料コードが無いのに送料がある' using errcode = '22023';
    end if;
  elsif jsonb_typeof(v_s -> 'shipping_code') is distinct from 'string' or length(btrim(v_ship_c)) not between 1 and 20 or v_ship_c ~ '[[:cntrl:]]'
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
  -- 🆕 0061 (中原さん 2026-10-08): 推奨保有月数は要る (単品もセットも・0 は入れてよい)
  if v_reorder is null then raise exception 'invalid_value: 推奨保有月数が要る (0〜60)' using errcode = '22023'; end if;
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
    -- 🆕 0061 (中原さん 2026-10-08): 単品は売上分類が要る (「あとで」は無い)。セットは今までどおり構成品から導く / 導けないときだけ上書き
    if v_sales is null then raise exception 'invalid_value: 単品は売上分類が要る (1〜4)' using errcode = '22023'; end if;
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
        case when v_ship_c is null then '発送方法 (送料コード) はまだです。届いてサイズを見てから、商品の画面で入れてください (利益の見込み・product-hub の楽天の配送方法に使います)' end,
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
