-- 0057: 一時の表 (TEMP) を使う関数のうち 2 つを、一時の表を使わない形に書き直す (D-60 v3.6 の PR 1b-0 の一部・2026-10-04)
--   設計 = AI_reference CompanyDB構想/13 §3.10「PR 1b-0」と「明示の一時の表 (TEMP) の hard bound」(Codex R-D60-v3-6 H1)。Codex R-D60-v3-7 の判定 =
--   「relink_shipments_bulk と merge_duplicate_suppliers の TEMP なし化と比較の試験は着手してよい。reresolve_order_lines は止める」
--
-- なぜ: `temp_file_limit` は明示の一時の表を縛らない。TEMP を持つ LOGIN の役割は関数の外から任意の大きさの一時の表を作れる = PR 1b で TEMP の権限を
--   superuser でない全部の LOGIN の役割から外す。その前に、正当な呼び手が使う関数から一時の表を無くしておく (外した後も今までどおり動くように)。
--
-- 形 (Codex PR #1605 R1 High = 「一時の表を配列・jsonb の変数に移すと backend の heap に全部載る・バイト数の上限が無い」への対応):
--   🚨 中間の結果を **変数に溜めない**。一時の表だった所は **1 つの文の中の CTE** (WITH) にする。文の中の中間の結果 (CTE の tuplestore・並べ替え・hash) は
--      work_mem (hash は × hash_mem_multiplier) を超えたら一時のファイルに逃げる = backend の heap は件数・文字の長さに依らず work_mem の桁で頭打ち
--      (変数の配列・jsonb は heap に全部載り、どこにも逃げない)。ディスクに逃げた分は temp_file_limit の対象 (本番は今 -1 = 無制限。0017 / 0027 の一時の表もディスクは無制限だった)。
--   変数に残すのは relink の戻り値 3 つと、merge の「寄せる行 → 残す行」の対応 (bigint の配列 2 つ) だけ。対応は **仕入先 ≦ 5,000 行を同じ文の同じ snapshot で数えてから** 作る
--      = 要素は 5,000 未満 (8 バイト × 2 × 5,000 = 約 80 KB が上限)。超えたら配列を作らずに 54000 で止まる (何も変えない)。
--   🚨 枠を明示する = どちらの関数にも work_mem = 4MB・hash_mem_multiplier = 2 を付ける (関数の中だけ・出るときに戻る。呼び手の work_mem に依らない)。
--      文の中で同時に work_mem を使う所 (CTE の tuplestore・並べ替え・hash (× 2)) は数個 = backend の heap の増え分は 件数・文字の長さ・呼び手の設定に依らず数十 MB
--      (測った: 4MB で 旧 +17〜75 MB / 新 +21〜29 MB。付けないと呼び手が 32MB のとき新 +130 MB (旧 +75 MB) = work_mem に比例した。README の 0057 の節の表)
--   変える順は 0017 / 0027 と同じにする: 1 つの文の中の「データを変える CTE」は、後の CTE が前の CTE の結果を **最初に 1 回だけ評価する条件** (One-Time Filter =
--      `(select count(*) from 前) >= 0`) で待つ = 前の CTE を全部終えてから次を始める (消す → 直す → 足す。relink は全部の候補を lock してから結ぶ)。
--
-- なにを (署名・戻り値・結果は変えない。CREATE OR REPLACE = 持ち主・EXECUTE の権限の表はそのまま):
--   1. core.relink_shipments_bulk(smallint, bigint, integer) (0017 の置き換え)
--      0017 の一時の表 _relink_cand2 → 1 つの文: 候補の CTE (materialized・shipment_id の順に p_limit 件 for update。skip locked にしない) →
--      全部の候補を lock し終えてから (One-Time Filter) orders と **列どうしの等結合**で結ぶ UPDATE の CTE → 候補の数・最後の番号・結んだ数を数える。
--      (0017 の教訓 = 式での結合は件数で実行計画が反転する → 結合の鍵は CTE の列)。
--      🚨 関数に enable_nestloop = off・enable_mergejoin = off を付ける (= 結合は hash だけ。設定は関数の中だけ・出るときに戻る):
--         0017 は一時の表を analyze して候補の本当の件数と列の分布を planner に渡していた。CTE は渡せない (CTE の列には統計が無い = 選択度は既定値) →
--         (a) 統計の無い / 古い orders では入れ子のループで 0021 の索引 (company_id, mall, scope_key, 日付 / 更新時刻) を引いて mall_order_no を Filter で見る
--         (b) work_mem が大きい (32MB) と、その索引の並びを使う merge join を (mall, scope_key) だけの鍵で選び mall_order_no を Join Filter で見る
--         = どちらも 候補 × 同じモールの全部の注文 = 件数の 2 乗 (使い捨ての PG で測った: (a) 注文 300k・統計なし 5,000 件 = 1 回 36 秒・100,000 件は 60 秒で打ち切り /
--         (b) 100,000 件・注文番号 1,000 文字・統計あり・work_mem 32MB = 9 分を超えても終わらない)。hash join は等号の条件を全部 hash の鍵に使う = 件数に比例
--         (hash の表は work_mem × hash_mem_multiplier を超えたら batch に分けてディスクへ)
--      上限は今までどおり p_limit ≦ 100,000 (関数の中で強制)。戻り値 = linked・examined・last_id (候補 0 件なら null) = 0017 と同じ
--      🚨 0017 との違い (同時に動くときだけ): 0017 は「候補を取る文」と「結ぶ文」が別の snapshot。この形は 1 つの snapshot。
--         候補の lock を待った相手が **同じ取引で注文を足した / 注文の鍵を変えた** ときだけ、0017 は待った後の注文を見て結び、この形は文の始めの注文で結ぶ
--         (結べなかった伝票は order_id が null のまま残り、次の走査 (注文を送った後の先頭からの走査 = push/mall-orders.mjs の relink_rescan) で結ばれる)。
--         試験 = scripts/test-company-db-no-temp-pg.mjs の同時の試験 (2 接続を止めて比べる)
--   2. core.merge_duplicate_suppliers() (0027 の置き換え)
--      0027 の一時の表 3 つ → ① _sup_merge = 上限つきの配列の変数 2 つ (寄せる行・残す行。上の 5,000) ② _ss_merged・_dl_merged = 1 つの文の中の CTE:
--      「寄せる行を消す (RETURNING で消した行の値を返す)」→ 残す行 (文の始めの snapshot) と消した行から列ごとの値をまとめる → 残す行を直す → 足す。
--      文の順番は 0027 と同じ (仕入先を補う → 仕入先ごとの商品 (消す → 直す → 足す) → 発注・外部 ID → 文書の紐付け (消す → 直す → 足す) → 寄せた仕入先を消す
--      → コードを揃える → 二重が残れば raise)。消してから足す順を変えない = 代表の印 (ux_supplier_skus_primary = SKU ごとに代表は最大 1 つ) と trigger の前提をそのまま守る。
--      監査 (AFTER の行 trigger) は 0027 では文ごとに、この形では文の終わりにまとめて発火する (並ぶ順 = 消す → 直す → 足す は同じ)。BEFORE の trigger は行ごとに同じ順
--      🚨 0027 との違い (同時に動くときだけ): 0027 は「まとめた値を取る文」と「消す文」が別の snapshot。この形は消した行そのもの (lock を待った後の最新の版) の値でまとめる
--         = 寄せる行を別の取引が直して commit した直後でも、直した値を残す行に移す (0027 は直す前の値で上書きすることがあった)
--      🆕 上限 = 仕入先 (core.suppliers の全部の行) ≦ 5,000 (超えたら 54000 で止まる = 何も変えない)。本番は約 40〜80 行
--      relink と同じ理由で enable_nestloop = off・enable_mergejoin = off (CTE の列に統計が無い = (残す行, 商品) の一部の鍵だけの結合を選ばせない)
--   どちらも search_path を pg_catalog, pg_temp に固定し、表と関数は schema で修飾する (SECURITY INVOKER のまま)
-- 変えないこと:
--   🚨 core.reresolve_order_lines (0024・一時の表 4 つ) には触らない (Codex R-D60-v3-7 = 上限の単位・cursor・期間の端・戻り値を直してから)
--   🚨 TEMP の権限は外さない (外すのは PR 1b)。EXECUTE の権限も変えない (merge_duplicate_suppliers を PUBLIC・watcher・runtime から外すのは PR 1b = 設計の「重い入口の分け」の表)
--   表・索引・trigger・データは変えない (関数の差し替えだけ)。2 回流しても同じ
-- 試験 = scripts/test-company-db-no-temp-pg.mjs (使い捨ての本物の PostgreSQL で、0017 / 0027 の版と 1 行も違わないこと・2 接続の同時の試験・TEMP の権限の無い役割で呼べること)
--   メモリ = scripts/company-db/measure-no-temp-mem.mjs (backend のピークのメモリを 0017 / 0027・R1 の配列の版・この版で測る。README の 0057 の節に表)

create or replace function core.relink_shipments_bulk(p_company_id smallint, p_after bigint default 0, p_limit integer default 20000)
returns table (linked integer, examined integer, last_id bigint)
language plpgsql set search_path = pg_catalog, pg_temp set enable_nestloop = off set enable_mergejoin = off set work_mem = '4MB' set hash_mem_multiplier = 2 as $$
declare
  v_linked integer := 0;
  v_examined integer := 0;
  v_last bigint := null;
begin
  if p_limit is null or p_limit <= 0 or p_limit > 100000 then raise exception 'p_limit must be 1..100000'; end if;
  -- 候補 = 未結合の伝票を shipment_id の順に p_limit 件 (for update。skip locked にしない = 飛ばした伝票を「完了」にしない)。
  -- 店舗 (ne_shops) が無い / mall が null (対象外の店) の伝票も候補に数える (examined) が、照合用の鍵が null なので結ばれない (0016 / 0017 と同じ)
  with c as materialized (
    select s.shipment_id, n.mall, n.scope_key, n.order_no_prefix || s.ne_order_no as mall_order_no
      from core.shipments s
      left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code and n.mall is not null
     where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null and s.shipment_id > coalesce(p_after, 0)
     order by s.shipment_id limit p_limit
     for update of s
  ), u as (
    update core.shipments s set order_id = o.order_id
      from c
      join core.orders o on o.company_id = p_company_id and o.mall = c.mall and o.scope_key = c.scope_key and o.mall_order_no = c.mall_order_no
     where s.shipment_id = c.shipment_id and c.mall is not null
       and (select count(*) from c) >= 0   -- One-Time Filter = 全部の候補を lock し終えてから結ぶ (0017 の 2 つの文の順)
    returning 1
  )
  select (select count(*) from c)::integer, (select max(c.shipment_id) from c), (select count(*) from u)::integer
    into v_examined, v_last, v_linked;
  linked := v_linked; examined := v_examined; last_id := v_last;
  return next;
end
$$;

create or replace function core.merge_duplicate_suppliers()
returns table (merged_suppliers integer, supplier_skus_after integer, renamed_codes integer)
language plpgsql set search_path = pg_catalog, pg_temp set enable_nestloop = off set enable_mergejoin = off set work_mem = '4MB' set hash_mem_multiplier = 2 as $$
declare
  c_max_suppliers constant integer := 5000;
  v_suppliers bigint;
  v_drop bigint[];      -- 寄せる行 (消す supplier_id)。要素は 5,000 未満 (下の文で同じ snapshot の仕入先を数えてから作る)
  v_keep bigint[];      -- v_drop と同じ位置の残す行
  v_merged integer;
  v_skus integer;
  v_renamed integer;
  v_n bigint;
begin
  -- 寄せる行 → 残す行。仕入先の数と同じ snapshot で数え、上限を超えたら配列を作らない (One-Time Filter で d が空) = 競合中に足されても 5,000 を超えない
  with c as materialized (
    select supplier_id, company_id, code, core.canonical_supplier_code(code) as canon from core.suppliers
  ), keeper as (
    select distinct on (company_id, canon) company_id, canon, supplier_id as keep_id
    from c
    order by company_id, canon, (code = canon) desc, supplier_id
  ), d as (
    select c.supplier_id as drop_id, k.keep_id
    from c join keeper k on k.company_id = c.company_id and k.canon = c.canon
    where c.supplier_id <> k.keep_id
      and (select count(*) from c) <= c_max_suppliers
  )
  select (select count(*) from c), array_agg(d.drop_id order by d.drop_id), array_agg(d.keep_id order by d.drop_id)
    into v_suppliers, v_drop, v_keep
  from d;
  if v_suppliers > c_max_suppliers then
    raise exception 'merge_duplicate_suppliers: 仕入先が % 行 (上限 % 行) = 一度にまとめない (人が確かめる)', v_suppliers, c_max_suppliers using errcode = '54000';
  end if;
  v_merged := coalesce(pg_catalog.cardinality(v_drop), 0);

  if v_merged > 0 then
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
    --   1 つの文で 消す (del) → 直す (upd) → 足す (ins)。まとめる値 (g = 0027 の _ss_merged) は 残す行 (文の始めの snapshot) と del が返した消した行から作る
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), del as (
      delete from core.supplier_skus x using m where x.supplier_id = m.drop_id
      returning m.keep_id, x.supplier_id, x.sku_id, x.company_id, x.vendor_code, x.order_unit, x.stock_units_per_order_unit, x.min_order_qty, x.order_multiple,
                x.unit_cost_jpy, x.lead_time_days, x.active, x.is_primary, x.created_at, x.created_by_type, x.created_by_id
    ), grp as (
      select x.supplier_id as keep_id, false as is_drop, x.supplier_id, x.sku_id, x.company_id, x.vendor_code, x.order_unit, x.stock_units_per_order_unit, x.min_order_qty,
             x.order_multiple, x.unit_cost_jpy, x.lead_time_days, x.active, x.is_primary, x.created_at, x.created_by_type, x.created_by_id
      from core.supplier_skus x
      where x.supplier_id = any(v_keep)
      union all
      select d.keep_id, true, d.supplier_id, d.sku_id, d.company_id, d.vendor_code, d.order_unit, d.stock_units_per_order_unit, d.min_order_qty,
             d.order_multiple, d.unit_cost_jpy, d.lead_time_days, d.active, d.is_primary, d.created_at, d.created_by_type, d.created_by_id
      from del d
    ), g as materialized (
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
    ), upd as (
      update core.supplier_skus k set
        vendor_code = g.vendor_code, order_unit = g.order_unit, stock_units_per_order_unit = g.stock_units_per_order_unit,
        min_order_qty = g.min_order_qty, order_multiple = g.order_multiple, unit_cost_jpy = g.unit_cost_jpy,
        lead_time_days = g.lead_time_days, active = g.active, is_primary = g.is_primary
      from g
      where k.supplier_id = g.keep_id and k.sku_id = g.sku_id
        and (k.vendor_code, k.order_unit, k.stock_units_per_order_unit, k.min_order_qty, k.order_multiple, k.unit_cost_jpy, k.lead_time_days, k.active, k.is_primary)
            is distinct from (g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple, g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary)
        and (select count(*) from del) >= 0   -- One-Time Filter = 消し終えてから直す (代表の印の一意索引の前提・0027 の順)
      returning 1
    ), ins as (
      insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple,
                                      unit_cost_jpy, lead_time_days, active, is_primary, created_at, created_by_type, created_by_id)
      select g.company_id, g.keep_id, g.sku_id, g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple,
             g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary, g.created_at, g.created_by_type, g.created_by_id
      from g
      where not exists (select 1 from core.supplier_skus k where k.supplier_id = g.keep_id and k.sku_id = g.sku_id)
        and (select count(*) from upd) >= 0   -- One-Time Filter = 直し終えてから足す (0027 の順)
      returning 1
    )
    select count(*) into v_n from ins;

    -- 発注・外部 ID
    update core.purchase_orders p set supplier_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where p.supplier_id = m.drop_id;
    update core.external_ids e set entity_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where e.entity_type = 'supplier' and e.entity_id = m.drop_id;

    -- 文書の紐付け: 1 つの文で 消す → 直す → 足す (まとめる値 = 0027 の _dl_merged)
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), del as (
      delete from docs.document_links l using m where l.entity_type = 'supplier' and l.entity_id = m.drop_id
      returning l.document_id, m.keep_id, l.entity_id, l.link_role, l.created_at
    ), grp as (
      select l.document_id, l.entity_id as keep_id, false as is_drop, l.entity_id, l.link_role, l.created_at
      from docs.document_links l
      where l.entity_type = 'supplier' and l.entity_id = any(v_keep)
      union all
      select d.document_id, d.keep_id, true, d.entity_id, d.link_role, d.created_at from del d
    ), g as materialized (
      select document_id, keep_id,
             (array_agg(link_role order by is_drop, entity_id) filter (where link_role is not null))[1] as link_role,
             min(created_at) as created_at
      from grp group by document_id, keep_id
    ), upd as (
      update docs.document_links l set link_role = g.link_role
      from g
      where l.entity_type = 'supplier' and l.document_id = g.document_id and l.entity_id = g.keep_id and l.link_role is distinct from g.link_role
        and (select count(*) from del) >= 0   -- One-Time Filter = 消し終えてから直す
      returning 1
    ), ins as (
      insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at)
      select g.document_id, 'supplier', g.keep_id, g.link_role, g.created_at from g
      where not exists (select 1 from docs.document_links l where l.document_id = g.document_id and l.entity_type = 'supplier' and l.entity_id = g.keep_id)
        and (select count(*) from upd) >= 0   -- One-Time Filter = 直し終えてから足す
      returning 1
    )
    select count(*) into v_n from ins;

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
