-- 0027: 商品・仕入先マスタに足りない列を足す (2026-09-24。Company DB構想 10 §3 / PR ②c-2。Codex ②c 設計レビューを反映)
--
-- なぜ: 正本を Company DB に移す (10。D-39) と、今 NE・/register (miniPC の上書き表)・発注アプリに散っている値を Company DB が持つ必要がある。
--   切替日までは夜間ロード (apps/company-db/load) が SQLite の写しから埋め続ける (持ち主は 'load')。
-- なにを:
--   core.skus: standard_price_jpy (標準売価 = NE 登録 CSV の baika_tnk) / shipping_code・shipping_method・shipping_cost_jpy (自社の計算用の送料。
--              /register の送料を重ねた後の値。実際にかかった配送費とは別) / reorder_months (推奨保有月数。商品管理リストの公開 snapshot から)
--   core.products: inbound_date_managed (ロジザード新商品の「入荷日管理」の初期値。以後の正はロジザード。夜間ロードは入れない = 既存の商品は null = 不明)
--   core.suppliers: email_to・email_cc・contact_name・fax_number・relay_to・order_memo (発注アプリの仕入先の連絡先。切替日までは発注アプリの値に合わせる)
--   core.supplier_skus: is_primary (代表の仕入先 = NE の商品の仕入先コード。NE / ロジザードへはこれだけ出す)。SKU ごとに最大 1 つ (部分 unique)
--     🚨 保証するのは「最大 1 つ」で「必ず 1 つ」ではない (NE の仕入先コードが空の商品は代表なし = 保留)
--   core.merge_duplicate_suppliers() (0025) を新しい列に追従させる (寄せる行にだけある連絡先・代表の印を失わない。Codex ②c High)
-- 🚨 金額は円の bigint・0 以上・null = 未取得 (0 円と区別する。既定値なし)。コード類 (送料コード・FAX) は先頭の 0 を持てる text
-- 🚨 列の変更は 0026 のトリガーが自動で記録し version を上げる (管理用の列以外は全部比べる)。ここで足した列も対象

alter table core.skus
  add column standard_price_jpy bigint check (standard_price_jpy >= 0),
  add column shipping_code      text,
  add column shipping_method    text,
  add column shipping_cost_jpy  bigint check (shipping_cost_jpy >= 0),
  add column reorder_months     numeric(4,1) check (reorder_months >= 0 and reorder_months <= 60);
comment on column core.skus.standard_price_jpy is '標準売価 (円・税込・NE の売価)。null = 未取得';
comment on column core.skus.shipping_cost_jpy is '自社の計算用の送料 (円)。/register の送料コードから引いた値。実際の配送費とは別。null = 未登録';
comment on column core.skus.reorder_months is '推奨保有月数 (0〜60・小数 1 桁)。null = 未登録 (0 と区別する)';

alter table core.products add column inbound_date_managed boolean;
comment on column core.products.inbound_date_managed is 'ロジザード新商品の入荷日管理の初期値 (登録時に人が選ぶ)。以後の正はロジザード。null = 不明';

alter table core.suppliers
  add column email_to     text,
  add column email_cc     text,
  add column contact_name text,
  add column fax_number   text,
  add column relay_to     text,
  add column order_memo   text;

alter table core.supplier_skus add column is_primary boolean not null default false;
create unique index ux_supplier_skus_primary on core.supplier_skus (company_id, sku_id) where is_primary;

-- 0025 の仕入先をまとめる関数を新しい列に追従させる (中身は 0025 と同じ。足したのは 連絡先 6 列・is_primary の寄せ方だけ)
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
    -- 仕入先: 名前・発注方法・リードタイム・有効・連絡先を寄せる行から補う (残す行が空のときだけ)
    with agg as (
      select m.keep_id,
             (array_agg(s.name order by s.supplier_id) filter (where s.name is distinct from s.code))[1] as real_name,
             (array_agg(s.order_method order by s.supplier_id) filter (where s.order_method is not null))[1] as om,
             (array_agg(s.lead_time_days order by s.supplier_id) filter (where s.lead_time_days is not null))[1] as lt,
             (array_agg(s.email_to order by s.supplier_id) filter (where s.email_to is not null))[1] as email_to,
             (array_agg(s.email_cc order by s.supplier_id) filter (where s.email_cc is not null))[1] as email_cc,
             (array_agg(s.contact_name order by s.supplier_id) filter (where s.contact_name is not null))[1] as contact_name,
             (array_agg(s.fax_number order by s.supplier_id) filter (where s.fax_number is not null))[1] as fax_number,
             (array_agg(s.relay_to order by s.supplier_id) filter (where s.relay_to is not null))[1] as relay_to,
             (array_agg(s.order_memo order by s.supplier_id) filter (where s.order_memo is not null))[1] as order_memo,
             bool_or(s.active) as any_active
      from _sup_merge m join core.suppliers s on s.supplier_id = m.drop_id
      group by m.keep_id
    )
    update core.suppliers k set
      name = case when k.name = k.code and a.real_name is not null then a.real_name else k.name end,
      order_method = coalesce(k.order_method, a.om),
      lead_time_days = coalesce(k.lead_time_days, a.lt),
      email_to = coalesce(k.email_to, a.email_to),
      email_cc = coalesce(k.email_cc, a.email_cc),
      contact_name = coalesce(k.contact_name, a.contact_name),
      fax_number = coalesce(k.fax_number, a.fax_number),
      relay_to = coalesce(k.relay_to, a.relay_to),
      order_memo = coalesce(k.order_memo, a.order_memo),
      active = k.active or a.any_active
    from agg a
    where k.supplier_id = a.keep_id;

    -- 仕入先ごとの商品: (残す行, 商品) ごとに 残す行 → 寄せる行 (supplier_id 順) の優先で列ごとに空でない最初の値。代表の印はどれかが代表なら代表
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
           bool_or(is_primary) as is_primary,
           min(created_at) as created_at,
           (array_agg(created_by_type order by is_drop, supplier_id))[1] as created_by_type,
           (array_agg(created_by_id order by is_drop, supplier_id))[1] as created_by_id
    from grp
    group by keep_id, sku_id;

    delete from core.supplier_skus x using _sup_merge m where x.supplier_id = m.drop_id;
    update core.supplier_skus k set
      vendor_code = g.vendor_code, order_unit = g.order_unit, stock_units_per_order_unit = g.stock_units_per_order_unit,
      min_order_qty = g.min_order_qty, order_multiple = g.order_multiple, unit_cost_jpy = g.unit_cost_jpy,
      lead_time_days = g.lead_time_days, active = g.active, is_primary = g.is_primary
    from _ss_merged g
    where k.supplier_id = g.keep_id and k.sku_id = g.sku_id
      and (k.vendor_code, k.order_unit, k.stock_units_per_order_unit, k.min_order_qty, k.order_multiple, k.unit_cost_jpy, k.lead_time_days, k.active, k.is_primary)
          is distinct from (g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple, g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary);
    insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple,
                                    unit_cost_jpy, lead_time_days, active, is_primary, created_at, created_by_type, created_by_id)
    select g.company_id, g.keep_id, g.sku_id, g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple,
           g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary, g.created_at, g.created_by_type, g.created_by_id
    from _ss_merged g
    where not exists (select 1 from core.supplier_skus k where k.supplier_id = g.keep_id and k.sku_id = g.sku_id);

    -- 発注・外部 ID
    update core.purchase_orders p set supplier_id = m.keep_id from _sup_merge m where p.supplier_id = m.drop_id;
    update core.external_ids e set entity_id = m.keep_id from _sup_merge m where e.entity_type = 'supplier' and e.entity_id = m.drop_id;

    -- 文書の紐付け
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

    delete from core.suppliers s using _sup_merge m where s.supplier_id = m.drop_id;
  end if;

  update core.suppliers set code = core.canonical_supplier_code(code) where code <> core.canonical_supplier_code(code);
  get diagnostics v_renamed = row_count;

  if exists (select 1 from core.suppliers group by company_id, core.canonical_supplier_code(code) having count(*) > 1) then
    raise exception 'merge_duplicate_suppliers: 仕入先コードを揃えても二重が残っている';
  end if;
  select count(*) into v_skus from core.supplier_skus;
  return query select v_merged, v_skus, v_renamed;
end $$;
