-- 0057: 一時の表 (TEMP) を使う関数のうち 2 つを、一時の表を使わない形に書き直す (D-60 v3.6 の PR 1b-0 の一部・2026-10-04)
--   設計 = AI_reference CompanyDB構想/13 §3.10「PR 1b-0」と「明示の一時の表 (TEMP) の hard bound」(Codex R-D60-v3-6 H1)。Codex R-D60-v3-7 の判定 =
--   「relink_shipments_bulk と merge_duplicate_suppliers の TEMP なし化と比較の試験は着手してよい。reresolve_order_lines は止める」
--
-- なぜ: `temp_file_limit` は明示の一時の表を縛らない。TEMP を持つ LOGIN の役割は関数の外から任意の大きさの一時の表を作れる = PR 1b で TEMP の権限を
--   superuser でない全部の LOGIN の役割から外す。その前に、正当な呼び手が使う関数から一時の表を無くしておく (外した後も今までどおり動くように)。
--
-- なにを (署名・戻り値・結果は変えない。CREATE OR REPLACE = 持ち主・EXECUTE の権限の表はそのまま):
--   1. core.relink_shipments_bulk(smallint, bigint, integer) (0017 の置き換え)
--      0017 の一時の表 _relink_cand2 → 件数の上限つきの配列の変数 4 つ (shipment_id・mall・scope_key・照合用の注文番号)。上限は今までどおり p_limit ≦ 100,000 (関数の中で強制)。
--      文の分け方は 0017 と同じ = ① 候補を shipment_id の順に p_limit 件 for update で取る (skip locked にしない) ② 別の文で orders と **列どうしの等結合**で結ぶ
--      (0017 の教訓 = 式での結合は件数で実行計画が反転する。unnest の列は列なので一意索引 (company_id, mall, scope_key, mall_order_no) がそのまま使える)。
--      戻り値 = linked (結んだ数)・examined (候補の数)・last_id (候補の最後の shipment_id・候補 0 件なら null) = 0017 と同じ
--   2. core.merge_duplicate_suppliers() (0027 の置き換え)
--      0027 の一時の表 3 つ → ① _sup_merge = 配列の変数 2 つ (寄せる行・残す行) ② _ss_merged・_dl_merged = jsonb の変数 (jsonb_to_recordset で型つきの行に戻す)。
--      文の順番は 0027 と同じ (仕入先を補う → 仕入先ごとの商品をまとめた値を取る → 寄せる行の商品を消す → 残す行を直す → 足す → 発注・外部 ID → 文書の紐付け → 寄せた仕入先を消す
--      → コードを揃える → 二重が残れば raise)。消してから足す順を変えない = 代表の印 (ux_supplier_skus_primary = SKU ごとに代表は最大 1 つ) と trigger の前提をそのまま守る。
--      🆕 上限 = 仕入先 (core.suppliers の全部の行) ≦ 5,000 を最初に確かめる (超えたら 54000 で止まる = 何も変えない)。本番は約 40〜80 行
--   どちらも search_path を pg_catalog, pg_temp に固定し、表と関数は schema で修飾する (SECURITY INVOKER のまま)
-- 変えないこと:
--   🚨 core.reresolve_order_lines (0024・一時の表 4 つ) には触らない (Codex R-D60-v3-7 = 上限の単位・cursor・期間の端・戻り値を直してから)
--   🚨 TEMP の権限は外さない (外すのは PR 1b)。EXECUTE の権限も変えない (merge_duplicate_suppliers を PUBLIC・watcher・runtime から外すのは PR 1b = 設計の「重い入口の分け」の表)
--   表・索引・trigger・データは変えない (関数の差し替えだけ)。2 回流しても同じ
-- 試験 = scripts/test-company-db-no-temp-pg.mjs (使い捨ての本物の PostgreSQL で、0017 / 0027 の版と 1 行も違わないこと・TEMP の権限の無い役割で呼べること)

create or replace function core.relink_shipments_bulk(p_company_id smallint, p_after bigint default 0, p_limit integer default 20000)
returns table (linked integer, examined integer, last_id bigint)
language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_ids    bigint[];
  v_malls  text[];
  v_scopes text[];
  v_nos    text[];
  v_linked integer := 0;
  v_examined integer := 0;
  v_last bigint := null;
begin
  if p_limit is null or p_limit <= 0 or p_limit > 100000 then raise exception 'p_limit must be 1..100000'; end if;
  -- 候補 = 未結合の伝票を shipment_id の順に p_limit 件 (for update。skip locked にしない = 飛ばした伝票を「完了」にしない)。
  -- 店舗 (ne_shops) が無い / mall が null (対象外の店) の伝票も候補に数える (examined) が、照合用の鍵が null なので結ばれない (0016 / 0017 と同じ)
  select array_agg(c.shipment_id order by c.shipment_id), array_agg(c.mall order by c.shipment_id),
         array_agg(c.scope_key order by c.shipment_id), array_agg(c.mall_order_no order by c.shipment_id)
    into v_ids, v_malls, v_scopes, v_nos
    from (select s.shipment_id, n.mall, n.scope_key, n.order_no_prefix || s.ne_order_no as mall_order_no
            from core.shipments s
            left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code and n.mall is not null
           where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null and s.shipment_id > coalesce(p_after, 0)
           order by s.shipment_id limit p_limit
           for update of s) c;
  v_examined := coalesce(pg_catalog.cardinality(v_ids), 0);
  if v_examined > 0 then
    v_last := v_ids[v_examined];   -- shipment_id の順に並べた最後 = 0017 の max(shipment_id)
    update core.shipments s set order_id = o.order_id
      from unnest(v_ids, v_malls, v_scopes, v_nos) as c(shipment_id, mall, scope_key, mall_order_no)
      join core.orders o on o.company_id = p_company_id and o.mall = c.mall and o.scope_key = c.scope_key and o.mall_order_no = c.mall_order_no
     where s.shipment_id = c.shipment_id and c.mall is not null;
    get diagnostics v_linked = row_count;
  end if;
  linked := v_linked; examined := v_examined; last_id := v_last;
  return next;
end
$$;

create or replace function core.merge_duplicate_suppliers()
returns table (merged_suppliers integer, supplier_skus_after integer, renamed_codes integer)
language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  c_max_suppliers constant integer := 5000;
  v_suppliers bigint;
  v_drop bigint[];      -- 寄せる行 (消す supplier_id)
  v_keep bigint[];      -- v_drop と同じ位置の残す行
  v_both bigint[];
  v_ss jsonb;           -- 0027 の _ss_merged
  v_dl jsonb;           -- 0027 の _dl_merged
  v_merged integer;
  v_skus integer;
  v_renamed integer;
begin
  select count(*) into v_suppliers from core.suppliers;
  if v_suppliers > c_max_suppliers then
    raise exception 'merge_duplicate_suppliers: 仕入先が % 行 (上限 % 行) = 一度にまとめない (人が確かめる)', v_suppliers, c_max_suppliers using errcode = '54000';
  end if;

  with c as (
    select supplier_id, company_id, code, core.canonical_supplier_code(code) as canon from core.suppliers
  ), keeper as (
    select distinct on (company_id, canon) company_id, canon, supplier_id as keep_id
    from c
    order by company_id, canon, (code = canon) desc, supplier_id
  )
  select array_agg(c.supplier_id order by c.supplier_id), array_agg(k.keep_id order by c.supplier_id)
    into v_drop, v_keep
  from c join keeper k on k.company_id = c.company_id and k.canon = c.canon
  where c.supplier_id <> k.keep_id;
  v_merged := coalesce(pg_catalog.cardinality(v_drop), 0);

  if v_merged > 0 then
    v_both := v_keep || v_drop;
    -- 仕入先: 名前・発注方法・リードタイム・有効・連絡先を寄せる行から補う (残す行が空のときだけ)
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), agg as (
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
      from m join core.suppliers s on s.supplier_id = m.drop_id
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
    --   消す前に値を取っておく (0027 の _ss_merged と同じ中身を jsonb の変数に。消してから足す順は 0027 と同じ)
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), grp as (
      select coalesce(m.keep_id, x.supplier_id) as keep_id, (m.drop_id is not null) as is_drop, x.*
      from core.supplier_skus x
      left join m on m.drop_id = x.supplier_id
      where x.supplier_id = any(v_both)
    ), g as (
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
      group by keep_id, sku_id
    )
    select coalesce(jsonb_agg(to_jsonb(g) order by g.keep_id, g.sku_id), '[]'::jsonb) into v_ss from g;

    delete from core.supplier_skus x using unnest(v_drop) as m(drop_id) where x.supplier_id = m.drop_id;
    with g as (
      select * from jsonb_to_recordset(v_ss) as r(keep_id bigint, sku_id bigint, company_id smallint, vendor_code text, order_unit text, stock_units_per_order_unit integer,
                                                    min_order_qty integer, order_multiple integer, unit_cost_jpy bigint, lead_time_days integer, active boolean, is_primary boolean,
                                                    created_at timestamptz, created_by_type text, created_by_id text)
    )
    update core.supplier_skus k set
      vendor_code = g.vendor_code, order_unit = g.order_unit, stock_units_per_order_unit = g.stock_units_per_order_unit,
      min_order_qty = g.min_order_qty, order_multiple = g.order_multiple, unit_cost_jpy = g.unit_cost_jpy,
      lead_time_days = g.lead_time_days, active = g.active, is_primary = g.is_primary
    from g
    where k.supplier_id = g.keep_id and k.sku_id = g.sku_id
      and (k.vendor_code, k.order_unit, k.stock_units_per_order_unit, k.min_order_qty, k.order_multiple, k.unit_cost_jpy, k.lead_time_days, k.active, k.is_primary)
          is distinct from (g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple, g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary);
    with g as (
      select * from jsonb_to_recordset(v_ss) as r(keep_id bigint, sku_id bigint, company_id smallint, vendor_code text, order_unit text, stock_units_per_order_unit integer,
                                                    min_order_qty integer, order_multiple integer, unit_cost_jpy bigint, lead_time_days integer, active boolean, is_primary boolean,
                                                    created_at timestamptz, created_by_type text, created_by_id text)
    )
    insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple,
                                    unit_cost_jpy, lead_time_days, active, is_primary, created_at, created_by_type, created_by_id)
    select g.company_id, g.keep_id, g.sku_id, g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple,
           g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary, g.created_at, g.created_by_type, g.created_by_id
    from g
    where not exists (select 1 from core.supplier_skus k where k.supplier_id = g.keep_id and k.sku_id = g.sku_id);

    -- 発注・外部 ID
    update core.purchase_orders p set supplier_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where p.supplier_id = m.drop_id;
    update core.external_ids e set entity_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where e.entity_type = 'supplier' and e.entity_id = m.drop_id;

    -- 文書の紐付け (0027 の _dl_merged と同じ中身を jsonb の変数に)
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), grp as (
      select l.document_id, coalesce(m.keep_id, l.entity_id) as keep_id, (m.drop_id is not null) as is_drop, l.entity_id, l.link_role, l.created_at
      from docs.document_links l
      left join m on m.drop_id = l.entity_id
      where l.entity_type = 'supplier' and l.entity_id = any(v_both)
    ), g as (
      select document_id, keep_id,
             (array_agg(link_role order by is_drop, entity_id) filter (where link_role is not null))[1] as link_role,
             min(created_at) as created_at
      from grp group by document_id, keep_id
    )
    select coalesce(jsonb_agg(to_jsonb(g) order by g.document_id, g.keep_id), '[]'::jsonb) into v_dl from g;
    delete from docs.document_links l using unnest(v_drop) as m(drop_id) where l.entity_type = 'supplier' and l.entity_id = m.drop_id;
    with g as (
      select * from jsonb_to_recordset(v_dl) as r(document_id bigint, keep_id bigint, link_role text, created_at timestamptz)
    )
    update docs.document_links l set link_role = g.link_role
    from g
    where l.entity_type = 'supplier' and l.document_id = g.document_id and l.entity_id = g.keep_id and l.link_role is distinct from g.link_role;
    with g as (
      select * from jsonb_to_recordset(v_dl) as r(document_id bigint, keep_id bigint, link_role text, created_at timestamptz)
    )
    insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at)
    select g.document_id, 'supplier', g.keep_id, g.link_role, g.created_at from g
    where not exists (select 1 from docs.document_links l where l.document_id = g.document_id and l.entity_type = 'supplier' and l.entity_id = g.keep_id);

    delete from core.suppliers s using unnest(v_drop) as m(drop_id) where s.supplier_id = m.drop_id;
  end if;

  update core.suppliers set code = core.canonical_supplier_code(code) where code <> core.canonical_supplier_code(code);
  get diagnostics v_renamed = row_count;

  if exists (select 1 from core.suppliers group by company_id, core.canonical_supplier_code(code) having count(*) > 1) then
    raise exception 'merge_duplicate_suppliers: 仕入先コードを揃えても二重が残っている';
  end if;
  select count(*) into v_skus from core.supplier_skus;
  return query select v_merged, v_skus, v_renamed;
end $$;
