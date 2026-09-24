-- 0025: 仕入先コードの形を揃え、先頭の 0 の食い違いで二重になった仕入先を 1 行にまとめる (2026-09-24。Company DB構想 10 §9 D)
--
-- なぜ: 発注アプリ (purchase-orders) は仕入先コードの先頭の 0 を外して持ち ('0001' → '1')、NE の商品と共有マスタは 4 桁 ('0001')。
--   初期ロードはそのまま入れたので、同じ仕入先が 2 行になった (本番 2026-09-24: core.suppliers 83 行 = 実際は 43 社、40 社が二重。
--   '0001' 形 = 名前がコードのまま・NE の商品との紐付けを持つ / '1' 形 = 名前・発注方法・先方品番を持つ)。
-- なにを:
--   1. core.canonical_supplier_code(): 前後の空白 (core.norm_code と同じ ECMAScript の空白集合 = JS の trim と同じ) を外し、
--      数字だけのコードは 4 桁の 0 埋め (NE の形)。4 桁より長い数字は先頭の 0 を外すだけ。数字以外はそのまま
--      (apps/company-db/load/sources.mjs の canonicalSupplierCode と同じ規則。夜間ロードも今後はこの形で入れる)
--   2. core.merge_duplicate_suppliers(): 揃えた形が同じ行のまとまりごとに、残す行 = 既に正しい形の行 (無ければ supplier_id の小さい行) を決めて寄せる。
--      何度呼んでもよい (二重が無ければ何もしない)。🚨 古い夜間ロードが二重を作り直したときは、人がこれを呼べば直る (0025 は流し直せないため)
--        - 仕入先: 名前 (残す行の名前がコードのままなら、寄せる行の本当の名前)・発注方法・リードタイム (残す行が空なら)・有効
--        - 仕入先ごとの商品 (supplier_skus): まとめた後の (残す行, 商品) ごとに、**残す行 → 寄せる行 (supplier_id 順) の優先順で列ごとに空でない最初の値**
--          を採って 1 行にする (3 行以上のまとまりで後ろの行にしか無い値も失わない。Codex #1441 R1 High)
--        - 発注 (purchase_orders)・外部 ID (entity_type = 'supplier')・文書の紐付け (docs.document_links。寄せる行どうしの重なりも 1 行に。R1 Medium)
--        - 寄せた行を消し、残った行のコードを揃えた形に書き換え、まだ二重が残っていれば raise (fail-close)
--   3. 0025 の中で 1 回呼ぶ
-- 🚨 名前が同じだけの行は合体させない (Codex 10 R1)。合体させるのはコードの数字が一致する行だけ
-- 🚨 夜間ロードが古いコード (揃える前の sources.mjs) のままだと '1' 形を作り直すので、このファイルは **新しいコードが Render に出た後** に流す。
--    夜間ロード (02:00 JST) の最中には流さない (ロードとの排他は無い)

create or replace function core.canonical_supplier_code(p text) returns text language sql immutable strict as $$
  with t as (
    select regexp_replace(p, '^[\u0009\u000a\u000b\u000c\u000d    -     　﻿]+|[\u0009\u000a\u000b\u000c\u000d    -     　﻿]+$', '', 'g') as c
  ), u as (
    select c, regexp_replace(c, '^0+(?=[0-9])', '') as x from t
  )
  select case when c ~ '^[0-9]+$' then (case when length(x) < 4 then lpad(x, 4, '0') else x end) else c end from u
$$;

create or replace function core.merge_duplicate_suppliers()
returns table (merged_suppliers integer, supplier_skus_after integer, renamed_codes integer) language plpgsql as $$
declare
  v_merged integer;
  v_skus integer;
  v_renamed integer;
begin
  drop table if exists _sup_merge;
  create temp table _sup_merge on commit drop as
  with c as (
    select supplier_id, company_id, code, core.canonical_supplier_code(code) as canon from core.suppliers
  ), keeper as (
    select distinct on (company_id, canon) company_id, canon, supplier_id as keep_id
    from c
    order by company_id, canon, (code = canon) desc, supplier_id
  )
  select c.supplier_id as drop_id, k.keep_id, c.company_id
  from c join keeper k on k.company_id = c.company_id and k.canon = c.canon
  where c.supplier_id <> k.keep_id;
  select count(*) into v_merged from _sup_merge;

  if v_merged > 0 then
    -- 仕入先: 名前・発注方法・リードタイム・有効を寄せる行から補う
    with agg as (
      select m.keep_id,
             (array_agg(s.name order by s.supplier_id) filter (where s.name is distinct from s.code))[1] as real_name,
             (array_agg(s.order_method order by s.supplier_id) filter (where s.order_method is not null))[1] as om,
             (array_agg(s.lead_time_days order by s.supplier_id) filter (where s.lead_time_days is not null))[1] as lt,
             bool_or(s.active) as any_active
      from _sup_merge m join core.suppliers s on s.supplier_id = m.drop_id
      group by m.keep_id
    )
    update core.suppliers k set
      name = case when k.name = k.code and a.real_name is not null then a.real_name else k.name end,
      order_method = coalesce(k.order_method, a.om),
      lead_time_days = coalesce(k.lead_time_days, a.lt),
      active = k.active or a.any_active
    from agg a
    where k.supplier_id = a.keep_id;

    -- 仕入先ごとの商品: (残す行, 商品) ごとに 残す行 → 寄せる行 (supplier_id 順) の優先で列ごとに空でない最初の値
    drop table if exists _ss_merged;
    create temp table _ss_merged on commit drop as
    with grp as (
      select coalesce(m.keep_id, x.supplier_id) as keep_id, (m.drop_id is not null) as is_drop, x.*
      from core.supplier_skus x
      left join _sup_merge m on m.drop_id = x.supplier_id
      where x.supplier_id in (select keep_id from _sup_merge union select drop_id from _sup_merge)
    )
    select keep_id, sku_id, min(company_id) as company_id,
           (array_agg(vendor_code order by is_drop, supplier_id) filter (where vendor_code is not null))[1] as vendor_code,
           (array_agg(order_unit order by is_drop, supplier_id) filter (where order_unit is not null))[1] as order_unit,
           (array_agg(stock_units_per_order_unit order by is_drop, supplier_id) filter (where stock_units_per_order_unit is not null))[1] as stock_units_per_order_unit,
           (array_agg(min_order_qty order by is_drop, supplier_id) filter (where min_order_qty is not null))[1] as min_order_qty,
           (array_agg(order_multiple order by is_drop, supplier_id) filter (where order_multiple is not null))[1] as order_multiple,
           (array_agg(unit_cost_jpy order by is_drop, supplier_id) filter (where unit_cost_jpy is not null))[1] as unit_cost_jpy,
           (array_agg(lead_time_days order by is_drop, supplier_id) filter (where lead_time_days is not null))[1] as lead_time_days,
           bool_or(active) as active,
           min(created_at) as created_at,
           (array_agg(created_by_type order by is_drop, supplier_id))[1] as created_by_type,
           (array_agg(created_by_id order by is_drop, supplier_id))[1] as created_by_id
    from grp
    group by keep_id, sku_id;

    delete from core.supplier_skus x using _sup_merge m where x.supplier_id = m.drop_id;
    update core.supplier_skus k set
      vendor_code = g.vendor_code, order_unit = g.order_unit, stock_units_per_order_unit = g.stock_units_per_order_unit,
      min_order_qty = g.min_order_qty, order_multiple = g.order_multiple, unit_cost_jpy = g.unit_cost_jpy,
      lead_time_days = g.lead_time_days, active = g.active
    from _ss_merged g
    where k.supplier_id = g.keep_id and k.sku_id = g.sku_id
      and (k.vendor_code, k.order_unit, k.stock_units_per_order_unit, k.min_order_qty, k.order_multiple, k.unit_cost_jpy, k.lead_time_days, k.active)
          is distinct from (g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple, g.unit_cost_jpy, g.lead_time_days, g.active);
    insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple,
                                    unit_cost_jpy, lead_time_days, active, created_at, created_by_type, created_by_id)
    select g.company_id, g.keep_id, g.sku_id, g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple,
           g.unit_cost_jpy, g.lead_time_days, g.active, g.created_at, g.created_by_type, g.created_by_id
    from _ss_merged g
    where not exists (select 1 from core.supplier_skus k where k.supplier_id = g.keep_id and k.sku_id = g.sku_id);

    -- 発注・外部 ID
    update core.purchase_orders p set supplier_id = m.keep_id from _sup_merge m where p.supplier_id = m.drop_id;
    update core.external_ids e set entity_id = m.keep_id from _sup_merge m where e.entity_type = 'supplier' and e.entity_id = m.drop_id;

    -- 文書の紐付け: まとめた後の (文書, 残す行) で 1 行 (寄せる行どうしの重なりも)。役割・作成時刻は残す行 → 寄せる行の順で最初の値
    drop table if exists _dl_merged;
    create temp table _dl_merged on commit drop as
    with grp as (
      select l.document_id, coalesce(m.keep_id, l.entity_id) as keep_id, (m.drop_id is not null) as is_drop, l.entity_id, l.link_role, l.created_at
      from docs.document_links l
      left join _sup_merge m on m.drop_id = l.entity_id
      where l.entity_type = 'supplier' and l.entity_id in (select keep_id from _sup_merge union select drop_id from _sup_merge)
    )
    select document_id, keep_id,
           (array_agg(link_role order by is_drop, entity_id) filter (where link_role is not null))[1] as link_role,
           min(created_at) as created_at
    from grp group by document_id, keep_id;
    delete from docs.document_links l using _sup_merge m where l.entity_type = 'supplier' and l.entity_id = m.drop_id;
    update docs.document_links l set link_role = g.link_role
    from _dl_merged g
    where l.entity_type = 'supplier' and l.document_id = g.document_id and l.entity_id = g.keep_id and l.link_role is distinct from g.link_role;
    insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at)
    select g.document_id, 'supplier', g.keep_id, g.link_role, g.created_at from _dl_merged g
    where not exists (select 1 from docs.document_links l where l.document_id = g.document_id and l.entity_type = 'supplier' and l.entity_id = g.keep_id);

    -- 寄せた行を消す
    delete from core.suppliers s using _sup_merge m where s.supplier_id = m.drop_id;
  end if;

  -- 残った行のコードを揃えた形に
  update core.suppliers set code = core.canonical_supplier_code(code) where code <> core.canonical_supplier_code(code);
  get diagnostics v_renamed = row_count;

  if exists (select 1 from core.suppliers group by company_id, core.canonical_supplier_code(code) having count(*) > 1) then
    raise exception 'merge_duplicate_suppliers: 仕入先コードを揃えても二重が残っている';
  end if;
  select count(*) into v_skus from core.supplier_skus;
  return query select v_merged, v_skus, v_renamed;
end $$;

select * from core.merge_duplicate_suppliers();
