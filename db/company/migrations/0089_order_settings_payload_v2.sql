-- 0089 (0088 = ブランド・商品の段階の次。migrate は欠番を断る): 発注の設定の知らせ v2 = 先方品番・入数つき (AI_reference CompanyDB構想/27 §2.4 の ⑤-2)
--
-- なぜ: 新商品の登録で入れた先方品番・入数 (先方 1 単位 = 弊社何個) を、発注の設定の知らせ (0075・ops.order_settings_outbox) で
--   発注アプリ (Render の SQLite の po_vendor_code_map) に届けたい。0075 の形の確かめ ops.order_settings_payload_problem は
--   'po-settings-v1' と鍵の並びを決め打ちしている = v2 の知らせは積めない (enqueue が invalid_input で断る)。
-- なにを:
--   1. ops.order_settings_payload_problem を v1 / v2 の両方を受けるように置き換える (create or replace = 持ち主・権限はそのまま)。
--      v1 = 今までと同じ決まり・同じ順。v2 = v1 の 6 つの鍵 + vendor { vendor_code: 文字 1〜100 字, qty_per_unit: 文字 | null }
--   2. 印の関数 ops.order_settings_payload_versions() = 受ける版の一覧。アプリはこの関数が「あるか」(to_regprocedure・権限なしで分かる) で
--      v2 を積んでよいかを決める (lib/order-settings-outbox.mjs の orderPayloadV2Ready)。1 と同じ取引で作る = 印がある = 確かめも v2 を受ける
-- 🚨 表・ほかの関数は変えない。enqueue / claim / finish は版を見ない (表の check は 'po-settings-v[0-9]+' = v2 を通す・0075)。
--    🚨 lib/order-settings-outbox.mjs の orderPayloadShapeProblem と同じ決まり・同じ順 (試験 scripts/test-order-settings-outbox.mjs で固定)
-- 順: コード (v1 / v2 の両方を届けられる・v2 は印があるときだけ積む) を先に出す → この migration。前の版のコードに戻した間の v2 の知らせは
--   「失敗 (schema)」で残る (消えない) = 進めた後の夜間の段・人の「もう一度」で届く。

create or replace function ops.order_settings_payload_problem(p jsonb) returns text language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  k text;
  v jsonb;
  v_v2 boolean;
begin
  if p is null or pg_catalog.jsonb_typeof(p) <> 'object' then return 'payload_not_object'; end if;
  v_v2 := (p ->> 'schema') is not distinct from 'po-settings-v2';
  if (select pg_catalog.array_agg(x order by x collate "C") from pg_catalog.jsonb_object_keys(p) x) is distinct from
     (case when v_v2 then array['code', 'new_condition', 'new_material', 'patch', 'schema', 'supplier_code', 'vendor']
           else array['code', 'new_condition', 'new_material', 'patch', 'schema', 'supplier_code'] end) then
    return 'payload_keys';
  end if;
  if (p ->> 'schema') is distinct from 'po-settings-v1' and not v_v2 then return 'schema'; end if;
  if v_v2 then
    if pg_catalog.jsonb_typeof(p -> 'vendor') <> 'object' then return 'vendor'; end if;
    if (select pg_catalog.array_agg(x order by x collate "C") from pg_catalog.jsonb_object_keys(p -> 'vendor') x) is distinct from array['qty_per_unit', 'vendor_code'] then
      return 'vendor_keys';
    end if;
    if pg_catalog.jsonb_typeof(p -> 'vendor' -> 'vendor_code') <> 'string' or pg_catalog.length(p -> 'vendor' ->> 'vendor_code') not between 1 and 100 then return 'vendor_code'; end if;
    if pg_catalog.jsonb_typeof(p -> 'vendor' -> 'qty_per_unit') not in ('string', 'null') then return 'vendor_value'; end if;
  end if;
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

-- 印: 受ける版の一覧 (アプリは「あるか」だけを見る・実行の権限は持ち主だけ)
create function ops.order_settings_payload_versions() returns text[] language sql immutable set search_path = pg_catalog, pg_temp as $$
  select array['po-settings-v1', 'po-settings-v2']::text[]
$$;
revoke all on function ops.order_settings_payload_versions() from public;
