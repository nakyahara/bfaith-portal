-- 0025: 仕入先コードの形を揃え、先頭の 0 の食い違いで二重になった仕入先を 1 行にまとめる (2026-09-24。Company DB構想 10 §9 D)
--
-- なぜ: 発注アプリ (purchase-orders) は仕入先コードの先頭の 0 を外して持ち ('0001' → '1')、NE の商品と共有マスタは 4 桁 ('0001')。
--   初期ロードはそのまま入れたので、同じ仕入先が 2 行になった (本番 2026-09-24: core.suppliers 83 行 = 実際は 43 社、40 社が二重。
--   '0001' 形 = 名前がコードのまま・NE の商品との紐付けを持つ / '1' 形 = 名前・発注方法・先方品番を持つ)。
-- なにを:
--   1. core.canonical_supplier_code(): 数字だけのコードは 4 桁の 0 埋め (NE の形)。4 桁より長い数字は先頭の 0 を外すだけ。数字以外はそのまま
--      (apps/company-db/load/sources.mjs の canonicalSupplierCode と同じ規則。夜間ロードも今後はこの形で入れる)
--   2. 揃えた形が同じ行のまとまりごとに、残す行 = 既に正しい形の行 (無ければ supplier_id の小さい行) を決め、ほかの行から寄せる:
--      名前 (残す行の名前がコードのままなら、寄せる行の本当の名前)・発注方法・リードタイム (残す行が空なら)・有効
--      仕入先ごとの商品 (supplier_skus): 同じ商品が残す行にあれば空欄だけ埋めて寄せる行の方を消す。無ければ付け替える
--      発注 (purchase_orders)・外部 ID (entity_type = 'supplier')・文書の紐付け (docs.document_links) も付け替える
--   3. 寄せた行を消す → 残った行のコードを揃えた形に書き換える
--   4. まだ二重が残っていれば巻き戻す (fail-close)
-- 🚨 名前が同じだけの行は合体させない (Codex 10 R1)。合体させるのはコードの数字が一致する行だけ
-- 🚨 夜間ロードが古いコード (揃える前の sources.mjs) のままだと '1' 形を作り直すので、このファイルは **新しいコードが Render に出た後** に流す

create or replace function core.canonical_supplier_code(p text) returns text language sql immutable strict as $$
  select case
    when btrim(p) ~ '^[0-9]+$' then
      case when length(regexp_replace(btrim(p), '^0+(?=[0-9])', '')) < 4
           then lpad(regexp_replace(btrim(p), '^0+(?=[0-9])', ''), 4, '0')
           else regexp_replace(btrim(p), '^0+(?=[0-9])', '') end
    else btrim(p)
  end
$$;

-- 寄せる行 → 残す行
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

-- 2a. 残す行の名前・発注方法・リードタイム・有効を、寄せる行から補う
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

-- 2b. 仕入先ごとの商品: 残す行に同じ商品があれば空欄だけ埋める
update core.supplier_skus k set
  vendor_code = coalesce(k.vendor_code, d.vendor_code),
  order_unit = coalesce(k.order_unit, d.order_unit),
  stock_units_per_order_unit = coalesce(k.stock_units_per_order_unit, d.stock_units_per_order_unit),
  min_order_qty = coalesce(k.min_order_qty, d.min_order_qty),
  order_multiple = coalesce(k.order_multiple, d.order_multiple),
  unit_cost_jpy = coalesce(k.unit_cost_jpy, d.unit_cost_jpy),
  lead_time_days = coalesce(k.lead_time_days, d.lead_time_days),
  active = k.active or d.active
from core.supplier_skus d join _sup_merge m on m.drop_id = d.supplier_id
where k.supplier_id = m.keep_id and k.sku_id = d.sku_id;

-- 2c. 残す行と重なる行・寄せる行どうしで重なる行 (supplier_id の大きい方) を消す
delete from core.supplier_skus d using _sup_merge m
where d.supplier_id = m.drop_id
  and exists (select 1 from core.supplier_skus k where k.supplier_id = m.keep_id and k.sku_id = d.sku_id);
delete from core.supplier_skus d using _sup_merge m
where d.supplier_id = m.drop_id
  and exists (select 1 from core.supplier_skus d2 join _sup_merge m2 on m2.drop_id = d2.supplier_id
              where m2.keep_id = m.keep_id and d2.sku_id = d.sku_id and d2.supplier_id < d.supplier_id);

-- 2d. 残りは付け替える
update core.supplier_skus d set supplier_id = m.keep_id from _sup_merge m where d.supplier_id = m.drop_id;

-- 2e. 発注・外部 ID・文書の紐付け
update core.purchase_orders p set supplier_id = m.keep_id from _sup_merge m where p.supplier_id = m.drop_id;
update core.external_ids e set entity_id = m.keep_id from _sup_merge m where e.entity_type = 'supplier' and e.entity_id = m.drop_id;
delete from docs.document_links l using _sup_merge m
where l.entity_type = 'supplier' and l.entity_id = m.drop_id
  and exists (select 1 from docs.document_links l2 where l2.document_id = l.document_id and l2.entity_type = 'supplier' and l2.entity_id = m.keep_id);
update docs.document_links l set entity_id = m.keep_id from _sup_merge m where l.entity_type = 'supplier' and l.entity_id = m.drop_id;

-- 3. 寄せた行を消し、残った行のコードを揃えた形に
delete from core.suppliers s using _sup_merge m where s.supplier_id = m.drop_id;
update core.suppliers set code = core.canonical_supplier_code(code) where code <> core.canonical_supplier_code(code);

-- 4. まだ二重が残っていれば巻き戻す
do $$
begin
  if exists (select 1 from core.suppliers group by company_id, core.canonical_supplier_code(code) having count(*) > 1) then
    raise exception '0025: 仕入先コードを揃えても二重が残っている';
  end if;
end $$;
