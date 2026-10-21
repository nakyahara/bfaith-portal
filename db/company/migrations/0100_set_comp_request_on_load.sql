-- 0100 (仮番号・0099 の次。マージの時に次の空き番号に付け替える。migrate は欠番を断る): 今あるセットの中身 (構成) を、構成の持ち主 (sku_components) が NE (load) のまま
--   「依頼」の形で変えられるようにする (2026-10-21 中原さん「近道を作って」・AI_reference CompanyDB構想/24 §経緯 10/21・PR-0b)
--
-- 流れ: 商品の画面で構成を変える = 構成の依頼 (ops.sku_component_requests・core.sku_components は書かない) →
--   足す・数を変える = 「NEへ送る」の「セットの中身の CSV」/ 外す・入れ替える = NE の画面で (CSV ではできない) →
--   翌朝の NE の取得 → その夜の夜間ロードが core を NE の中身にし、依頼を閉じる (0099 の closeRequestOnLoad)
-- なにを:
--   1. ops.set_comp_request_on_load() = 印 (アプリが to_regprocedure で「0100 の後の DB か」を見る = 前の DB では今までどおり load の間は構成を直せない)
--   2. ops.guard_master_edit_write (0099 の版を写して 🆕 0100 の continue の行だけ変えた): 依頼と食い違いの表は、持ち主 sku_components が load でも通す
--      (表・操作の許可 (ops.master_write_allowed)・約束の相手の行・構成品は単品だけ (業務の約束) ・ほかの持ち主のキーは今までどおり)
-- 変えない: core.sku_components (夜間ロードだけ)・持ち主表・照合 ②・NE 登録の CSV・夜間ロード
-- 🚨 この migration は値を何も変えない (関数の置き換えと印だけ)。create or replace = 持ち主・実行権はそのまま (ロールの流し直しは要らない)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops.set_entry_on_load()') is null or pg_catalog.to_regprocedure('ops.guard_master_edit_write()') is null then
    raise exception '0100 の前提: 0099 (セットの新商品を構成の持ち主 load のまま登録) が無い';
  end if;
  if pg_catalog.to_regprocedure('ops.set_comp_request_on_load()') is not null then raise exception '0100: ops.set_comp_request_on_load がもうある'; end if;
end $$;

-- ═══════════ 1. 印 ═══════════
create function ops.set_comp_request_on_load() returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$ select true $$;
comment on function ops.set_comp_request_on_load() is '0100: 今あるセットの構成の依頼を構成の持ち主 load のまま作れる DB の印 (依頼と食い違いの表は持ち主 sku_components が load でも書ける)';
revoke all on function ops.set_comp_request_on_load() from public;

-- ═══════════ 2. 書き込みの守り (0099 の版 + 🆕 0100 の continue の行) ═══════════
create or replace function ops.guard_master_edit_write() returns trigger
  language plpgsql security definer set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_old     jsonb;
  v_new     jsonb;
  v_sess    ops.master_write_sessions;
  v_sku     bigint;
  v_role    text;   -- 'edited' (直す SKU) / 'derived' (含むセット) / null (相手でない)
  v_bad     text;
  v_loop    boolean;
  v_depth   integer;
  k         text;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op in ('UPDATE', 'DELETE') then v_old := to_jsonb(old); end if;
  if tg_op in ('INSERT', 'UPDATE') then v_new := to_jsonb(new); end if;
  v_sess := ops.current_master_write_session();
  if v_tbl = 'ops.master_edit_requests' then
    if tg_op = 'INSERT' and (v_new ->> 'status') = 'failed' then return new; end if;
    if v_sess.session_id is null then
      raise exception 'master_write_session_required: 保存の記録 (done) は、同じ取引で ops.begin_master_write をした後だけ' using errcode = '42501';
    end if;
    if tg_op <> 'INSERT' or (v_new ->> 'request_id')::uuid is distinct from v_sess.request_id or (v_new ->> 'actor_id') is distinct from v_sess.actor_id
       or (v_new ->> 'sku_id')::bigint is distinct from v_sess.sku_id or (v_new ->> 'operation') is distinct from v_sess.operation
       or (v_new ->> 'payload_hash') is distinct from v_sess.payload_hash then
      raise exception 'master_write_session_mismatch: 保存の記録 (done) が約束 (request_id・人・SKU・操作・保存の中身のハッシュ) と違う' using errcode = '42501';
    end if;
    return new;
  end if;
  if v_sess.session_id is null then
    raise exception 'master_write_session_required: 画面のロールの書き込み (%) は、同じ取引で ops.begin_master_write を呼んだ後だけ', v_tbl using errcode = '42501';
  end if;
  if (select phase from ops.master_cutover_state where id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない (%)', v_tbl using errcode = '42501';
  end if;
  if not ops.master_write_allowed(v_sess.operation, v_tbl, tg_op) then
    raise exception 'master_write_operation: 約束の操作 % では % に % できない', v_sess.operation, v_tbl, tg_op using errcode = '42501';
  end if;
  -- 約束の相手の行か (変える前と後の両方)。SKU と原価は 直す SKU / 含むセット を分ける
  if v_tbl in ('core.skus', 'core.sku_costs') then
    if v_old is not null and v_new is not null and (v_old ->> 'sku_id') is distinct from (v_new ->> 'sku_id') then v_role := null;
    else
      v_sku := coalesce(v_new ->> 'sku_id', v_old ->> 'sku_id')::bigint;
      v_role := case when v_sku = v_sess.sku_id then 'edited' when v_sku = any(v_sess.derived_sku_ids) then 'derived' end;
    end if;
  elsif v_tbl = 'core.products' then
    v_role := case when (v_old is null or (v_old ->> 'product_id')::bigint = any(v_sess.target_product_ids))
                    and (v_new is null or (v_new ->> 'product_id')::bigint = any(v_sess.target_product_ids)) then 'edited' end;
  elsif v_tbl = 'core.supplier_skus' then
    v_role := case when (v_old is null or (v_old ->> 'sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'sku_id')::bigint = v_sess.sku_id) then 'edited' end;
  elsif v_tbl in ('ops.sku_component_requests', 'ops.sku_component_breaches') then
    v_role := case when (v_old is null or (v_old ->> 'set_sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'set_sku_id')::bigint = v_sess.sku_id) then 'edited' end;
  end if;
  if v_role is null then
    raise exception 'master_write_target: 約束の相手 (SKU %) の行でない (%)', v_sess.sku_id, v_tbl using errcode = '42501';
  end if;
  -- 含むセット = 導く値の計算し直しだけ
  if v_role = 'derived' then
    if v_tbl = 'core.skus' then
      select string_agg(x, ', ' order by x) into v_bad from jsonb_object_keys(v_new) as t(x)
       where not (x = any(core.master_audit_ignored_columns())) and not (x = any(ops.master_write_derived_columns())) and (v_old -> x) is distinct from (v_new -> x);
      if v_bad is not null then
        raise exception 'master_write_derived_only: 含むセット (SKU %) は導く値 (税率・税区分・取扱区分) だけ直せる (変えた列: %)', v_sku, v_bad using errcode = '42501';
      end if;
    elsif (tg_op = 'INSERT' and (v_new ->> 'cost_source') is distinct from 'set_calc')
       or (tg_op in ('UPDATE', 'DELETE') and (v_old ->> 'cost_source') in ('manual', 'override_zero')) then
      raise exception 'master_write_derived_only: 含むセット (SKU %) の原価は計算の行 (set_calc) だけ (人が決めた原価は触らない)', v_sku using errcode = '42501';
    end if;
  end if;
  -- 業務の約束 (アプリの確かめを DB でも・#1563 R5 M1)
  if v_tbl = 'core.supplier_skus' and coalesce((v_new ->> 'is_primary')::boolean, false)
     and not exists (select 1 from core.suppliers s where s.supplier_id = (v_new ->> 'supplier_id')::bigint and s.active) then
    raise exception 'master_write_invariant: 代表の仕入先 % は取引停止か無い', v_new ->> 'supplier_id' using errcode = '42501';
  end if;
  if v_tbl = 'ops.sku_component_requests' and tg_op = 'INSERT' and ops.component_request_rows_ok(v_new -> 'rows', 1, 20)   -- 形の誤りは表の CHECK が拒む
     and exists (select 1 from jsonb_array_elements(v_new -> 'rows') e
                  where (e ->> 'sku_id')::bigint = v_sess.sku_id
                     or not exists (select 1 from core.skus k where k.sku_id = (e ->> 'sku_id')::bigint and k.sku_kind = 'single')) then
    raise exception 'master_write_invariant: 構成の依頼の構成品は、ある単品だけ (セット自身・セット・例外の SKU・無い SKU は入れない)' using errcode = '42501';
  end if;
  if v_tbl = 'core.products' and tg_op = 'UPDATE' and (v_new ->> 'parent_product_id') is not null
     and (v_old ->> 'parent_product_id') is distinct from (v_new ->> 'parent_product_id') then
    with recursive up(id, d) as (
      select (v_new ->> 'parent_product_id')::bigint, 1
      union all
      select p.parent_product_id, up.d + 1 from core.products p join up on p.product_id = up.id where p.parent_product_id is not null and up.d < 200)
    select coalesce(bool_or(id = (v_new ->> 'product_id')::bigint), false), max(d) into v_loop, v_depth from up;
    if v_loop or v_depth >= 200 then
      raise exception 'master_write_invariant: 代表 (親) にすると親子が輪になる (商品 %)', v_new ->> 'product_id' using errcode = '42501';
    end if;
  end if;
  foreach k in array ops.master_edit_owner_keys(v_tbl, tg_op, v_old, v_new) loop
    -- 🆕 0100: 構成の依頼と食い違いの表 (ops.sku_component_requests・ops.sku_component_breaches) は、構成の持ち主 (sku_components) が load でも通す
    --   (0099 の新商品の登録に加えて、商品の画面の保存 (sku_edit) の依頼を作る・取り下げる・置き換える・食い違いを閉じる)。どれも core.sku_components は書かない
    --   (core の構成は今までどおり夜間ロードが NE から書く・依頼は夜間ロードの後に NE の観測と同じなら閉じる = lib/master-write.mjs の closeRequestOnLoad)
    continue when k = 'sku_components' and v_tbl in ('ops.sku_component_requests', 'ops.sku_component_breaches');
    if (v_sess.ownership ->> k) is distinct from 'company' then
      raise exception 'owner_not_company: % の持ち主が company でない (%)', k, v_tbl using errcode = '42501';
    end if;
  end loop;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
