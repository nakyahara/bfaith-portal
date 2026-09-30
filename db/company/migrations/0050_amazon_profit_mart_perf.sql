-- 0050 Amazon の利益の mart を速くする (D7b-3 の続き。2026-09-30)
--
-- 0049 (#1559) を本適用した後に本番で読むだけで測った時間 (statement_timeout 120s):
--   mart.amazon_profit_daily_range 9 月 1 か月 = 11.7 秒 (50,237 行) / 93 日 = 46.2 秒 (162,457 行)
--   mart.amazon_profit_day_totals_range 1〜9 月 = 120 秒で打ち切り / 9 月 1 か月 = 15.4 秒
--   部品 (9 月): finance_daily_sku_range 3.1 秒 / _amazon_easy_ship_alloc 0.9 秒 / _amazon_profit_ad_children 0.7 秒 = 重いのは _amazon_profit_rows の本体
-- 直すもの (結果は 0049 と完全に同じ = scripts/test-company-db-amazon-profit.mjs と合成のデータの scripts/bench-company-db-amazon-profit.mjs で突き合わせる):
--   ① mart._amazon_profit_rows (create or replace・引数と戻りは 0049 のまま) = 行ごとの計算を先にまとめてから結ぶ形に
--   ② mart.amazon_profit_day_totals_range の契約を最大 93 日 (両端を含む) に縮める (400 日は 120 秒で打ち切り = 守り)。読む口 /totals も 93 日。
--      長い期間 (年の合計など) は分けて呼んでつなげても正式な合計にならない = 夜に月ごとの集計表を作る別の設計 (後で。今は利用者がいない)
--   ③ mart.amazon_profit_daily_range (公開の行の関数・引数と戻りは 0049 のまま) = 結果の順をここだけで決める (行の本体は幅の広い行を並べ替えない)
-- 🚨 0049 のファイルは直さない (本番に入った)。表・型・ほかの関数は変えない
-- ─── 行の本体 (日 × 出品・未解決は日 × 正規化 seller SKU) ───
--   材料 (日の決済の状態・広告の日の状態・広告の子・Easy Ship の割り振り) は呼び手が 1 回だけ計算して配列で渡す (日の合計が同じ材料を使い回す・#1559 Codex R1 Medium 1)
create or replace function mart._amazon_profit_rows(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date,
  p_days mart.amazon_profit_finance_day[], p_ad_days mart.amazon_profit_ad_day[], p_adc mart.amazon_profit_ad_child[], p_es mart.amazon_easy_ship_alloc_row[])
returns table (
  company_id smallint, mall text, scope_key text, economic_date_jst date,
  listing_id bigint, seller_sku_norm text, listing_resolution text, listing_code text,
  received_listing_ids bigint[], received_listing_unresolved_count integer, ad_received_listing_ids bigint[], ad_received_unresolved_rows integer,
  units_ordered integer, units_refunded_customer integer, units_marketplace_guarantee integer, units_a_to_z_refund integer, units_net_sold integer,
  units_refunded_customer_unrounded numeric, units_a_to_z_refund_unrounded numeric,
  sales_principal_jpy bigint, sales_shipping_jpy bigint, sales_giftwrap_jpy bigint, sales_tax_jpy bigint,
  commission_jpy bigint, fba_fulfillment_jpy bigint, fba_storage_jpy bigint, closing_fee_jpy bigint,
  shipping_chargeback_jpy bigint, giftwrap_chargeback_jpy bigint, promotion_jpy bigint, promotion_tax_jpy bigint, points_jpy bigint,
  warehouse_damage_jpy bigint, warehouse_lost_jpy bigint, safe_t_jpy bigint, refund_principal_jpy bigint, reversal_reimbursement_jpy bigint,
  misc_fee_jpy bigint, other_fee_jpy bigint, other_amount_jpy bigint,
  profit_before_cogs_jpy bigint, taxable_sku_fee_cost_jpy bigint, net_jpy bigint, unmapped_jpy bigint,
  unclassified_component_count integer, unclassified_mapped_jpy bigint, unclassified_abs_jpy bigint, unmapped_component_count integer, finance_legacy_rows integer,
  source_lines integer, order_rows integer,
  day_finance_status text, refund_units_status text, refund_incomplete_child_count integer, refund_unestimated_jpy bigint,
  component_unit_cost_jpy bigint, cogs_jpy bigint, cost_basis text, composition_basis text,
  missing_cost_sku_ids bigint[], cost_sku_cost_ids bigint[], cost_observed_ids bigint[],
  ad_status text, ad_cost numeric, ad_rows integer, easy_ship_alloc_jpy bigint,
  contribution_before_ad_incl_jpy bigint, contribution_before_ad_excl numeric,
  contribution_after_ad_incl numeric, contribution_after_ad_excl numeric,
  contribution_before_ad_assuming_incomplete_zero_incl_jpy bigint, contribution_before_ad_assuming_incomplete_zero_excl numeric,
  contribution_after_ad_assuming_incomplete_zero_incl numeric, contribution_after_ad_assuming_incomplete_zero_excl numeric,
  profit_incomplete_reasons text[], assumed_zero_reasons text[],
  master_basis text, master_notes text[], composition_hash text, cost_input_hash text,
  calculation_version text, observed_generation bigint, composition_audit_since timestamptz, composition_audit_through timestamptz,
  finance_coverage_generation bigint, finance_source_revision bigint, calculated_at timestamptz)
language sql stable
-- 0050: この関数の中だけ
--   ・work_mem 32MB = 幅の広い行 (1 行 約 100 列) の hash の結び・集計がディスクに溢れにくい (合成のデータ 3 万行で 3〜15MB の溢れが 4 か所)。
--     🚨 work_mem は sort / hash の節ごと (hash はさらに hash_mem_multiplier 倍) = 1 回の呼び出しで数倍になりうる。Render の Postgres (1GB) に対して
--        64MB は 2 本重なると危ない (#1562 Codex R1 Medium 1) → 32MB (PGlite で 64MB とほぼ同じ速さ) + 読む口は同時に 1 本だけ (advisory lock・router.mjs)
--   ・enable_nestloop off = 材料の配列 (引数) の行の数の見込みが外れて、CTE を何万回も読み直す nested loop を選ばない保険 (0045 と同じ)
--   ・plan_cache_mode = force_generic_plan = PostgreSQL 18 から sql の関数の文も plan cache を使い、最初の数回は引数の値 (材料の配列) を入れた custom plan になる。
--     合成のデータ (PGlite = PostgreSQL 18.3) で custom plan は約 9.5 秒・generic plan は約 2.8 秒 (同じ結果) = いつも generic にそろえる
--     (PostgreSQL 17 以前は sql の関数の文を引数の値を知らずに計画する = もともと generic と同じ形・この設定は害が無い)
set work_mem = '32MB'
set enable_nestloop = off
set plan_cache_mode = force_generic_plan
as $$
with
days as materialized (select u.* from unnest(p_days) u),
ad_days as materialized (select u.* from unnest(p_ad_days) u),
child as materialized (select * from mart.finance_daily_sku_range(p_company_id, p_mall, p_scope_key, p_from, p_to)),
-- 旧い形の版の SKU の行 (分けられない部品の 4 列を持たない = 数で確かめられない = fail-closed で finance_unclassified)
legacy as materialized (
  select f.economic_date_jst as day, core.norm_code(f.seller_sku) as sku_norm, count(*)::int as n
    from core.order_finance_daily f
    join core.finance_source_policy p
      on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
     and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
   where f.line_kind = 'sku' and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
     and f.economic_date_jst between p_from and p_to
     -- 0050: 今の送り手の版 (amazon_finance_v2) は正規表現を通さずに外す (同じ判定)。AND の評価の順は保証されない = CASE で順を決める (#1562 Codex R1 Low 1)
     and case when f.transform_version = 'amazon_finance_v2' then false else not core.finance_version_has_class(f.transform_version) end
   group by 1, 2
),
es as materialized (
  select e.economic_date_jst as day, e.seller_sku_norm as sku_norm, sum(e.easy_ship_cost_jpy)::bigint as cost
    from unnest(p_es) e
   where e.allocated
   group by 1, 2
),
adc as materialized (select u.* from unnest(p_adc) u),
ad_l as (select adc.date_jst as day, adc.listing_id as lid, sum(adc.ad_cost) as cost, sum(adc.ad_rows)::int as n,
                sum(adc.received_unresolved_rows)::int as rcv_unres from adc where adc.listing_id is not null group by 1, 2),
-- 広告の受け取り時の出品の集合 (日 × 今の出品ごと・ID の昇順)
ad_rcv as (
  select x.day, x.lid, array_agg(distinct x.rid order by x.rid) as ids
    from (select adc.date_jst as day, adc.listing_id as lid, unnest(adc.received_listing_ids) as rid from adc where adc.listing_id is not null) x
   group by 1, 2
),
ad_u as (select adc.date_jst as day, sum(adc.ad_rows)::int as n from adc where adc.listing_id is null group by 1),
-- 今のマスタで正規化 seller SKU → 出品 (0043 の受け口と同じ = listing_norm の直接の一致・会社 × モールで 1 件のときだけ。0 件 / 2 件以上 = 未解決)
norms as (select child.seller_sku_norm as sku_norm from child union select es.sku_norm from es),
res as materialized (
  select norms.sku_norm, case when count(l.listing_id) = 1 then min(l.listing_id) end as lid
    from norms
    left join core.listings l on l.company_id = p_company_id and l.mall = p_mall and l.listing_norm = norms.sku_norm
   group by norms.sku_norm
),
-- 財務の子に行の鍵 (rk = 出品の ID か 'u:' + 正規化 SKU)。直接の一致 = 1 つの出品の正規化 SKU は 1 つ = 鍵ごとに子は高々 1 つ
fk as (
  select c.*, res.lid, coalesce(res.lid::text, 'u:' || c.seller_sku_norm) as rk, coalesce(lg.n, 0) as legacy_n
    from child c
    join res on res.sku_norm = c.seller_sku_norm
    left join legacy lg on lg.day = c.economic_date_jst and lg.sku_norm = c.seller_sku_norm
),
ek as (
  select es.day, res.lid, coalesce(res.lid::text, 'u:' || es.sku_norm) as rk, case when res.lid is null then es.sku_norm end as unorm, sum(es.cost)::bigint as cost
    from es join res on res.sku_norm = es.sku_norm
   group by 1, 2, 3, 4
),
keys as (
  select fk.economic_date_jst as day, fk.lid, fk.rk, case when fk.lid is null then fk.seller_sku_norm end as unorm from fk
  union select ek.day, ek.lid, ek.rk, ek.unorm from ek
  union select ad_l.day, ad_l.lid, ad_l.lid::text, null::text from ad_l
),
rl as (select distinct keys.day, keys.lid from keys where keys.lid is not null),
-- 今の構成 (D-64)
comp as materialized (
  select lc.listing_id as lid, lc.sku_id, lc.qty
    from core.listing_components lc
   where lc.company_id = p_company_id and lc.listing_id in (select rl.lid from rl)
),
comp_l as (
  select comp.lid, count(*)::int as n_comp,
         encode(sha256(convert_to('{"components":[' || string_agg('{"qty":' || comp.qty || ',"sku_id":"' || comp.sku_id || '"}', ',' order by comp.sku_id)
                || '],"listing_id":"' || comp.lid || '"}', 'UTF8')), 'hex') as composition_hash
    from comp group by comp.lid
),
-- 原価 = SKU × 日に 1 行 (§3.3)。sku_costs が先・観測の原価は sku_costs の最初の日より前だけ (v_sku_cost_observed_effective)
need as materialized (select distinct rl.day, comp.sku_id from rl join comp on comp.lid = rl.lid),
cand as (
  select need.day, need.sku_id, 1 as pri, c.sku_cost_id as row_id, c.valid_from, c.created_at, c.cost_jpy, c.cost_status, 'sku_costs'::text as src, 'sku_costs'::text as basis
    from need join core.sku_costs c
      on c.company_id = p_company_id and c.sku_id = need.sku_id and c.valid_from <= need.day and (c.valid_to is null or need.day <= c.valid_to)
  union all
  select need.day, need.sku_id, 2, o.sku_cost_observed_id, o.valid_from, null::timestamptz, o.cost_jpy, o.cost_status, 'observed'::text, o.cost_basis
    from need join mart.v_sku_cost_observed_effective o
      on o.company_id = p_company_id and o.sku_id = need.sku_id and o.valid_from <= need.day and (o.valid_to is null or need.day <= o.valid_to)
),
pick as materialized (
  select distinct on (cand.sku_id, cand.day) cand.*
    from cand
   order by cand.sku_id, cand.day, cand.pri, cand.valid_from desc, cand.created_at desc nulls last, cand.row_id desc
),
cc as (
  select rl.day, rl.lid, comp.sku_id, comp.qty, pick.src, pick.row_id, pick.cost_jpy, pick.cost_status,
         coalesce(pick.cost_status in ('COMPLETE', 'OVERRIDDEN'), false) as known,
         case when not coalesce(pick.cost_status in ('COMPLETE', 'OVERRIDDEN'), false) then 4
              when pick.basis = 'sku_costs' then 1 when pick.basis = 'observed' then 2 else 3 end as basis_rank
    from rl join comp on comp.lid = rl.lid
    left join pick on pick.sku_id = comp.sku_id and pick.day = rl.day
),
-- 日 × 出品の原価のまとめ。0050: cost_input_hash (JSON の組み立て + SHA-256) は行ごとでなく「採った原価の行の組」ごとに 1 回
--   (採った行 (source・行の ID) の組 = 原価・状態・SKU も決まる = hash の中身が決まる。hash に出品の ID は入らない = 別の出品・別の日で同じ組なら同じ hash)
ucp as (
  select cc.day, cc.lid, bool_and(cc.known) as all_known,
         sum(cc.qty::bigint * cc.cost_jpy) filter (where cc.known) as known_sum,
         max(cc.basis_rank) as basis_rank,
         coalesce(array_agg(cc.sku_id order by cc.sku_id) filter (where not cc.known), '{}') as missing_ids,
         coalesce(array_agg(cc.row_id order by cc.row_id) filter (where cc.known and cc.src = 'sku_costs'), '{}') as sc_ids,
         coalesce(array_agg(cc.row_id order by cc.row_id) filter (where cc.known and cc.src = 'observed'), '{}') as ob_ids,
         case when bool_and(cc.known) then string_agg(cc.src || ':' || cc.row_id, ',' order by cc.sku_id, cc.src collate "C") end as pick_key
    from cc group by cc.day, cc.lid
),
pick_hash as (   -- 組ごとに代表の (日, 出品) 1 つから 0049 と同じ JSON を作る
  select h.pick_key,
         encode(sha256(convert_to('[' || string_agg('{"cost_jpy":' || cc.cost_jpy || ',"cost_status":' || to_json(cc.cost_status)::text
           || ',"row_id":"' || cc.row_id || '","sku_id":"' || cc.sku_id || '","source":"' || cc.src || '"}', ',' order by cc.sku_id, cc.src collate "C") || ']', 'UTF8')), 'hex') as cost_input_hash
    from (select distinct on (ucp.pick_key) ucp.pick_key, ucp.day, ucp.lid from ucp where ucp.pick_key is not null order by ucp.pick_key, ucp.day, ucp.lid) h
    join cc on cc.day = h.day and cc.lid = h.lid
   group by h.pick_key
),
uc as (
  select ucp.day, ucp.lid, ucp.all_known, ucp.known_sum, ucp.basis_rank, ucp.missing_ids, ucp.sc_ids, ucp.ob_ids, ph.cost_input_hash
    from ucp left join pick_hash ph on ph.pick_key = ucp.pick_key
),
-- 監査の記録 (出品の構成の全部の変更・出品の識別 (INSERT・DELETE・mall・shop_code・listing_code))。出品ごとの最大の時刻
aud as (
  select x.lid, max(x.at) as through
    from (
      select e.entity_id as lid, e.recorded_at as at from events.master_change_events e
       where e.company_id = p_company_id and e.entity_type = 'listing'
         and (e.operation in ('INSERT', 'DELETE') or e.attribute in ('mall', 'shop_code', 'listing_code'))
      union all
      select (e.entity_key ->> 'listing_id')::bigint, e.recorded_at from events.master_change_events e     -- INSERT = new・UPDATE = new・DELETE = old の listing_id
       where e.company_id = p_company_id and e.entity_type = 'listing_component'
      union all
      select (e.old_value #>> '{}')::bigint, e.recorded_at from events.master_change_events e              -- UPDATE で listing_id を付け替えた = 旧い出品も
       where e.company_id = p_company_id and e.entity_type = 'listing_component' and e.operation = 'UPDATE' and e.attribute = 'listing_id'
    ) x
   where x.lid in (select rl.lid from rl)
   group by x.lid
),
-- 0050: 日の単位の値を先に 1 日 1 行で (0049 は行ごとに時刻の変換・月末の計算・監査の始まりとの比べを繰り返していた)
dx as (
  select d.economic_date_jst as day, d.day_finance_status, d.coverage_generation, d.source_revision,
         -- 月末まで決済がそろっていない月 = 子が monthly でも partial (§3.2)
         (d.complete_to is null or (date_trunc('month', d.economic_date_jst) + interval '1 month - 1 day')::date > d.complete_to) as month_open,
         ((d.economic_date_jst - 1)::timestamp at time zone 'Asia/Tokyo') as audit_from,   -- その日の前日の JST 00:00
         ((d.economic_date_jst - 1)::timestamp at time zone 'Asia/Tokyo') < mart.amazon_profit_composition_audit_since() as pre_audit,
         ad.ad_status, coalesce(ad_u.n, 0) as ad_unres_n
    from days d
    join ad_days ad on ad.date_jst = d.economic_date_jst
    left join ad_u on ad_u.day = d.economic_date_jst
),
-- 0050: 出品の単位の値を先に 1 出品 1 行で (出品のコード・構成の数と hash・監査の記録の最大の時刻)
lx as (
  select x.lid, l.listing_code, comp_l.n_comp, comp_l.composition_hash, aud.through
    from (select distinct rl.lid from rl) x
    left join core.listings l on l.listing_id = x.lid
    left join comp_l on comp_l.lid = x.lid
    left join aud on aud.lid = x.lid
),
-- 0050: 日 × 出品の単位の値を先に 1 つに (原価・広告費・広告の受け取り時の出品)
dl as (
  select rl.day, rl.lid, uc.all_known, uc.known_sum, uc.basis_rank, uc.missing_ids, uc.sc_ids, uc.ob_ids, uc.cost_input_hash,
         ad_l.cost as ad_linked, coalesce(ad_l.n, 0) as ad_n, coalesce(ad_l.rcv_unres, 0) as ad_received_unres, coalesce(ad_rcv.ids, '{}'::bigint[]) as ad_received_ids
    from rl
    left join uc on uc.day = rl.day and uc.lid = rl.lid
    left join ad_l on ad_l.day = rl.day and ad_l.lid = rl.lid
    left join ad_rcv on ad_rcv.day = rl.day and ad_rcv.lid = rl.lid
),
r0 as (
  select k.day, k.lid, k.unorm, lx.listing_code,
         f.received_listing_ids, f.received_listing_unresolved_count,
         coalesce(f.units_ordered, 0) as units_ordered, coalesce(f.units_refunded_customer, 0) as units_refunded_customer,
         coalesce(f.units_a_to_z_refund, 0) as units_a_to_z_refund, coalesce(f.units_net_sold, 0) as units_net_sold,
         coalesce(f.units_marketplace_guarantee, 0) as units_marketplace_guarantee,
         -- 丸める前の返品数 (子の値。unit_price_missing の子は null のまま・子の無い行は 0)
         case when f.economic_date_jst is null then 0::numeric else f.units_refunded_customer_unrounded end as units_refunded_customer_unrounded,
         case when f.economic_date_jst is null then 0::numeric else f.units_a_to_z_refund_unrounded end as units_a_to_z_refund_unrounded,
         coalesce(dl.ad_received_ids, '{}'::bigint[]) as ad_received_ids, coalesce(dl.ad_received_unres, 0) as ad_received_unres,
         coalesce(f.sales_principal_jpy, 0) as sales_principal_jpy, coalesce(f.sales_shipping_jpy, 0) as sales_shipping_jpy,
         coalesce(f.sales_giftwrap_jpy, 0) as sales_giftwrap_jpy, coalesce(f.sales_tax_jpy, 0) as sales_tax_jpy,
         coalesce(f.commission_jpy, 0) as commission_jpy, coalesce(f.fba_fulfillment_jpy, 0) as fba_fulfillment_jpy,
         coalesce(f.fba_storage_jpy, 0) as fba_storage_jpy, coalesce(f.closing_fee_jpy, 0) as closing_fee_jpy,
         coalesce(f.shipping_chargeback_jpy, 0) as shipping_chargeback_jpy, coalesce(f.giftwrap_chargeback_jpy, 0) as giftwrap_chargeback_jpy,
         coalesce(f.promotion_jpy, 0) as promotion_jpy, coalesce(f.promotion_tax_jpy, 0) as promotion_tax_jpy, coalesce(f.points_jpy, 0) as points_jpy,
         coalesce(f.warehouse_damage_jpy, 0) as warehouse_damage_jpy, coalesce(f.warehouse_lost_jpy, 0) as warehouse_lost_jpy,
         coalesce(f.safe_t_jpy, 0) as safe_t_jpy, coalesce(f.refund_principal_jpy, 0) as refund_principal_jpy,
         coalesce(f.reversal_reimbursement_jpy, 0) as reversal_reimbursement_jpy,
         coalesce(f.misc_fee_jpy, 0) as misc_fee_jpy, coalesce(f.other_fee_jpy, 0) as other_fee_jpy, coalesce(f.other_amount_jpy, 0) as other_amount_jpy,
         coalesce(f.profit_before_cogs_jpy, 0) as profit_before_cogs_jpy, coalesce(f.net_jpy, 0) as net_jpy, coalesce(f.unmapped_jpy, 0) as unmapped_jpy,
         coalesce(f.unclassified_component_count, 0) as unclassified_component_count, coalesce(f.unclassified_mapped_jpy, 0) as unclassified_mapped_jpy,
         coalesce(f.unclassified_abs_jpy, 0) as unclassified_abs_jpy, coalesce(f.unmapped_component_count, 0) as unmapped_component_count,
         coalesce(f.legacy_n, 0) as legacy_n, coalesce(f.source_lines, 0) as source_lines, coalesce(f.order_rows, 0) as order_rows,
         -- 返品の状態の強さ (1 no_refund < 2 monthly < 3 partial < 4 unit_price_missing)。🆕 monthly でも月末まで決済がそろっていない月は partial (単価がまだ動く・§3.2)
         case when f.refund_units_status is null or f.refund_units_status = 'no_refund' then 1
              when f.refund_units_status = 'unit_price_missing' then 4
              when f.refund_units_status = 'estimated_partial_month_unit_price' then 3
              when d.month_open then 3
              else 2 end as refund_rank,
         coalesce(f.refund_unestimated_jpy, 0) as refund_unestimated_jpy,
         d.day_finance_status, d.coverage_generation, d.source_revision, d.ad_status, dl.ad_linked, coalesce(dl.ad_n, 0) as ad_n, d.ad_unres_n,
         coalesce(ek.cost, 0) as es_cost,
         lx.n_comp, lx.composition_hash,
         dl.all_known, dl.known_sum, dl.basis_rank, dl.missing_ids, dl.sc_ids, dl.ob_ids, dl.cost_input_hash,
         lx.through, d.audit_from, d.pre_audit
    from keys k
    join dx d on d.day = k.day
    left join fk f on f.economic_date_jst = k.day and f.rk = k.rk
    left join ek on ek.day = k.day and ek.rk = k.rk
    left join dl on dl.day = k.day and dl.lid = k.lid
    left join lx on lx.lid = k.lid
),
r1 as (
  select r0.*,
         case when r0.lid is not null and r0.n_comp is not null and r0.all_known then r0.known_sum end as unit_cost,
         (r0.commission_jpy + r0.fba_fulfillment_jpy + r0.fba_storage_jpy + r0.closing_fee_jpy + r0.shipping_chargeback_jpy + r0.giftwrap_chargeback_jpy) as taxable,
         case when r0.ad_status in ('not_collected', 'missing') then null else coalesce(r0.ad_linked, 0) end as ad_cost_v,
         r0.day_finance_status <> 'complete' as g_fin,
         (r0.unclassified_component_count + r0.unmapped_component_count + r0.legacy_n) > 0 as g_uncl,
         r0.refund_rank = 4 as g_ref_unknown,
         r0.refund_rank = 3 as g_ref_partial,
         r0.lid is null as g_unres,
         r0.lid is not null and r0.n_comp is null as g_comp,
         r0.lid is not null and r0.n_comp is not null and not r0.all_known as g_cost,
         r0.ad_status = 'not_collected' as g_ad_nc,
         r0.ad_status = 'missing' as g_ad_miss,
         r0.ad_status = 'legacy_incomplete' as g_ad_legacy,
         r0.ad_unres_n > 0 as g_ad_unres,
         r0.lid is not null and r0.pre_audit as n_pre,
         r0.lid is not null and r0.through is not null and r0.through >= r0.audit_from as n_after,
         -- 受け取り時の出品 (財務と広告の保存済みの listing_id) を **集合で** 比べる (#1559 Codex R1 Medium 2):
         --   出品の行 = 受け取りの記録があり、「今の出品 1 つだけ・受け取り時の未解決 0」と一致しない / 未解決の行 = 受け取り時に出品が決まっていた
         --   0050: 0049 は行ごとに「財務 ∪ 広告」の集合を副問い合わせ (unnest → array_agg(distinct)) で作っていた (1 行に 2 回・本番 5 万行) → 配列を作らない同じ判定に:
         --   集合が {今の出品} と一致しない ⇔ 集合が空 か 今の出品と違う要素がある (any = 配列の要素のどれか。空の配列は false)。
         --   「受け取りの記録があって集合が空」= 未解決の数 > 0 = 右の 1 つめで拾う (だから「集合が空」を別に書かない)
         case when r0.lid is not null
              then (r0.rcv_any or r0.rcv_unres > 0)
                   and (r0.rcv_unres > 0 or coalesce(r0.lid <> any(r0.rcv_fin), false) or coalesce(r0.lid <> any(r0.ad_received_ids), false))
              else r0.rcv_any end as n_changed
    from (select r.*,
                 coalesce(r.received_listing_ids, '{}'::bigint[]) as rcv_fin,
                 cardinality(coalesce(r.received_listing_ids, '{}'::bigint[])) > 0 or cardinality(r.ad_received_ids) > 0 as rcv_any,
                 coalesce(r.received_listing_unresolved_count, 0) + r.ad_received_unres as rcv_unres
            from r0 r) r0
),
r2 as (
  select r1.*,
         not (r1.g_fin or r1.g_uncl or r1.g_ref_unknown or r1.g_ref_partial or r1.g_unres or r1.g_comp or r1.g_cost) as ok_before,
         case when r1.unit_cost is not null then r1.units_net_sold::bigint * r1.unit_cost end as cogs,
         -- 0 と仮定: 不明の原価・構成なし・出品未解決 = 原価 0 (分かっている構成の原価だけ)
         r1.profit_before_cogs_jpy - r1.units_net_sold::bigint * coalesce(r1.known_sum, 0) as before_zero
    from r1
),
r3 as (
  select r2.*,
         r2.ok_before and not (r2.g_ad_nc or r2.g_ad_miss or r2.g_ad_legacy or r2.g_ad_unres) as ok_after,
         case when r2.ok_before then r2.profit_before_cogs_jpy - r2.cogs end as before_incl
    from r2
)
select p_company_id, p_mall, p_scope_key, r3.day,
       r3.lid, r3.unorm, case when r3.lid is null then 'unresolved' else 'resolved' end, r3.listing_code,
       coalesce(r3.received_listing_ids, '{}'::bigint[]), coalesce(r3.received_listing_unresolved_count, 0), r3.ad_received_ids, r3.ad_received_unres,
       r3.units_ordered, r3.units_refunded_customer, r3.units_marketplace_guarantee, r3.units_a_to_z_refund, r3.units_net_sold,
       r3.units_refunded_customer_unrounded, r3.units_a_to_z_refund_unrounded,
       r3.sales_principal_jpy, r3.sales_shipping_jpy, r3.sales_giftwrap_jpy, r3.sales_tax_jpy,
       r3.commission_jpy, r3.fba_fulfillment_jpy, r3.fba_storage_jpy, r3.closing_fee_jpy,
       r3.shipping_chargeback_jpy, r3.giftwrap_chargeback_jpy, r3.promotion_jpy, r3.promotion_tax_jpy, r3.points_jpy,
       r3.warehouse_damage_jpy, r3.warehouse_lost_jpy, r3.safe_t_jpy, r3.refund_principal_jpy, r3.reversal_reimbursement_jpy,
       r3.misc_fee_jpy, r3.other_fee_jpy, r3.other_amount_jpy,
       r3.profit_before_cogs_jpy, r3.taxable::bigint, r3.net_jpy, r3.unmapped_jpy,
       r3.unclassified_component_count, r3.unclassified_mapped_jpy, r3.unclassified_abs_jpy, r3.unmapped_component_count, r3.legacy_n,
       r3.source_lines, r3.order_rows,
       r3.day_finance_status,
       case r3.refund_rank when 1 then 'no_refund' when 2 then 'estimated_monthly_unit_price' when 3 then 'estimated_partial_month_unit_price' else 'unit_price_missing' end,
       case when r3.refund_rank >= 3 then 1 else 0 end,
       r3.refund_unestimated_jpy,
       r3.unit_cost, r3.cogs,
       case when r3.g_unres or r3.g_comp then 'missing'
            else case r3.basis_rank when 1 then 'sku_costs' when 2 then 'observed' when 3 then 'estimated' else 'missing' end end,
       case when r3.g_unres then 'listing_unresolved' when r3.g_comp then 'missing'
            when r3.n_pre then 'pre_audit_unverifiable' when r3.n_after then 'current_after_recorded_change'
            else 'current_no_recorded_change' end,
       coalesce(r3.missing_ids, '{}'::bigint[]), coalesce(r3.sc_ids, '{}'::bigint[]), coalesce(r3.ob_ids, '{}'::bigint[]),
       r3.ad_status, round(r3.ad_cost_v, 2), r3.ad_n, r3.es_cost,   -- 金額の numeric は小数 2 桁 (広告の行が無い日の 0 も '0.00')
       r3.before_incl,
       case when r3.ok_before then round(r3.before_incl + r3.taxable::numeric / 11 + r3.promotion_tax_jpy, 2) end,
       case when r3.ok_after then round(r3.before_incl - r3.ad_cost_v * 1.1, 2) end,
       case when r3.ok_after then round(r3.before_incl + r3.taxable::numeric / 11 + r3.promotion_tax_jpy - r3.ad_cost_v, 2) end,
       r3.before_zero,
       round(r3.before_zero + r3.taxable::numeric / 11 + r3.promotion_tax_jpy, 2),
       round(r3.before_zero - coalesce(r3.ad_cost_v, 0) * 1.1, 2),
       round(r3.before_zero + r3.taxable::numeric / 11 + r3.promotion_tax_jpy - coalesce(r3.ad_cost_v, 0), 2),
       array_remove(array[
         case when r3.g_fin then 'finance_incomplete' end, case when r3.g_uncl then 'finance_unclassified' end,
         case when r3.g_ref_unknown then 'refund_units_unknown' end, case when r3.g_ref_partial then 'refund_units_partial_month' end,
         case when r3.g_unres then 'listing_unresolved' end, case when r3.g_comp then 'composition_missing' end, case when r3.g_cost then 'cost_missing' end,
         case when r3.g_ad_nc then 'ad_not_collected' end, case when r3.g_ad_miss then 'ad_missing' end,
         case when r3.g_ad_legacy then 'ad_legacy_unverified' end, case when r3.g_ad_unres then 'ad_unresolved' end]::text[], null),
       array_remove(array[
         case when r3.g_fin then 'finance_incomplete' end, case when r3.g_uncl then 'finance_unclassified' end,
         case when r3.g_ref_unknown then 'refund_units_unknown' end,
         case when r3.g_unres then 'listing_unresolved' end, case when r3.g_comp then 'composition_missing' end, case when r3.g_cost then 'cost_missing' end,
         case when r3.g_ad_nc then 'ad_not_collected' end, case when r3.g_ad_miss then 'ad_missing' end,
         case when r3.g_ad_legacy then 'ad_legacy_unverified' end, case when r3.g_ad_unres then 'ad_unresolved' end]::text[], null),
       'current',
       array_remove(array[
         case when r3.n_pre then 'pre_audit_unverifiable' end, case when r3.n_after then 'current_after_recorded_change' end,
         case when r3.n_changed then 'listing_changed_since_received' end]::text[], null),
       case when not (r3.g_unres or r3.g_comp) then r3.composition_hash end,
       r3.cost_input_hash,
       'amazon_profit_v1',
       (select max(g.generation) from core.sku_cost_observed_loads g where g.company_id = p_company_id and g.source = 'warehouse_sqlite'),
       mart.amazon_profit_composition_audit_since(),
       r3.through,
       r3.coverage_generation, r3.source_revision,   -- core.finance_coverage_state (D7b-1b まで null)
       statement_timestamp()
  from r3
  -- 0050: 並べ替えない (約 100 列の幅の広い行の並べ替え = 重い・日の合計には要らない)。順は公開の mart.amazon_profit_daily_range だけが決める (#1562 Codex R1 Medium 2)
$$;

-- ─── 引数の確かめ (sql の関数の where から呼ぶ形。0049 の mart.amazon_profit_assert_args と同じ確かめ・違えば例外) ───
create or replace function mart.amazon_profit_args_ok(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns boolean language plpgsql stable as $$
begin
  perform mart.amazon_profit_assert_args(p_company_id, p_mall, p_scope_key, p_from, p_to);
  return true;
end
$$;

-- ─── 公開の行の関数 (0050: 順はここだけで決める = 行の本体は並べ替えない) ───
create or replace function mart.amazon_profit_daily_range(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (
  company_id smallint, mall text, scope_key text, economic_date_jst date,
  listing_id bigint, seller_sku_norm text, listing_resolution text, listing_code text,
  received_listing_ids bigint[], received_listing_unresolved_count integer, ad_received_listing_ids bigint[], ad_received_unresolved_rows integer,
  units_ordered integer, units_refunded_customer integer, units_marketplace_guarantee integer, units_a_to_z_refund integer, units_net_sold integer,
  units_refunded_customer_unrounded numeric, units_a_to_z_refund_unrounded numeric,
  sales_principal_jpy bigint, sales_shipping_jpy bigint, sales_giftwrap_jpy bigint, sales_tax_jpy bigint,
  commission_jpy bigint, fba_fulfillment_jpy bigint, fba_storage_jpy bigint, closing_fee_jpy bigint,
  shipping_chargeback_jpy bigint, giftwrap_chargeback_jpy bigint, promotion_jpy bigint, promotion_tax_jpy bigint, points_jpy bigint,
  warehouse_damage_jpy bigint, warehouse_lost_jpy bigint, safe_t_jpy bigint, refund_principal_jpy bigint, reversal_reimbursement_jpy bigint,
  misc_fee_jpy bigint, other_fee_jpy bigint, other_amount_jpy bigint,
  profit_before_cogs_jpy bigint, taxable_sku_fee_cost_jpy bigint, net_jpy bigint, unmapped_jpy bigint,
  unclassified_component_count integer, unclassified_mapped_jpy bigint, unclassified_abs_jpy bigint, unmapped_component_count integer, finance_legacy_rows integer,
  source_lines integer, order_rows integer,
  day_finance_status text, refund_units_status text, refund_incomplete_child_count integer, refund_unestimated_jpy bigint,
  component_unit_cost_jpy bigint, cogs_jpy bigint, cost_basis text, composition_basis text,
  missing_cost_sku_ids bigint[], cost_sku_cost_ids bigint[], cost_observed_ids bigint[],
  ad_status text, ad_cost numeric, ad_rows integer, easy_ship_alloc_jpy bigint,
  contribution_before_ad_incl_jpy bigint, contribution_before_ad_excl numeric,
  contribution_after_ad_incl numeric, contribution_after_ad_excl numeric,
  contribution_before_ad_assuming_incomplete_zero_incl_jpy bigint, contribution_before_ad_assuming_incomplete_zero_excl numeric,
  contribution_after_ad_assuming_incomplete_zero_incl numeric, contribution_after_ad_assuming_incomplete_zero_excl numeric,
  profit_incomplete_reasons text[], assumed_zero_reasons text[],
  master_basis text, master_notes text[], composition_hash text, cost_input_hash text,
  calculation_version text, observed_generation bigint, composition_audit_since timestamptz, composition_audit_through timestamptz,
  finance_coverage_generation bigint, finance_source_revision bigint, calculated_at timestamptz)
-- 0050: plpgsql → sql (引数・戻りは 0049 のまま)。plpgsql の return query は結果 (約 100 列 × 行) を一度ためる = work_mem を超えるとディスクに溢れる
--   (合成のデータ 3 万行で 6.3 秒 → sql の関数で 3.4 秒)。引数の確かめは where の関数 (行を見ない = 最初に 1 回だけ評価される one-time filter)
--   work_mem 32MB = 最後の並べ替え (幅の広い行) も溢れにくく (本体と同じ値・読む口は同時に 1 本だけ)
language sql stable
set work_mem = '32MB'
as $$
  select r.* from mart._amazon_profit_rows(p_company_id, p_mall, p_scope_key, p_from, p_to,
    array(select d from mart._amazon_profit_finance_days(p_company_id, p_mall, p_scope_key, p_from, p_to) d),
    array(select a from mart._amazon_profit_ad_days(p_company_id, p_mall, p_scope_key, p_from, p_to) a),
    array(select c from mart._amazon_profit_ad_children(p_company_id, p_mall, p_scope_key, p_from, p_to) c),
    array(select e from mart._amazon_easy_ship_alloc(p_company_id, p_mall, p_scope_key, p_from, p_to) e)) r
   where mart.amazon_profit_args_ok(p_company_id, p_mall, p_scope_key, p_from, p_to)
   -- 公開の結果の順 (0049 の行の本体の最後の並べ替えと同じ)。ここだけで決める (読む口の /daily はこの順をそのまま返す・#1562 Codex R1 Medium 2)
   order by r.economic_date_jst, r.listing_id is null, r.listing_id, r.seller_sku_norm collate "C"
$$;

create or replace function mart.amazon_profit_day_totals_range(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (
  row_kind text, period_from date, period_to date, economic_date_jst date, month_start date, day_count integer,
  day_finance_status text, complete_days integer, resolved_rows integer, unresolved_rows integer,
  units_ordered bigint, units_net_sold bigint, sales_principal_jpy bigint, sales_tax_jpy bigint, profit_before_cogs_jpy bigint, cogs_jpy bigint,
  easy_ship_alloc_jpy bigint, easy_ship_unallocated_jpy bigint, easy_ship_unallocated_count integer,
  ad_status text, ad_cost_total numeric, ad_cost_allocated numeric, ad_cost_unresolved numeric, ad_unresolved_rows integer,
  account_fee_cost_jpy bigint, account_fee_cost_excl numeric,
  account_fee_storage_cost_jpy bigint, account_fee_long_term_storage_cost_jpy bigint, account_fee_removal_cost_jpy bigint, account_fee_inbound_defect_cost_jpy bigint,
  account_fee_low_inventory_cost_jpy bigint, account_fee_subscription_cost_jpy bigint, account_fee_easy_ship_cost_jpy bigint, account_fee_other_cost_jpy bigint,
  net_jpy bigint, not_account_fee_mapped_jpy bigint, unknown_line_mapped_jpy bigint, unclassified_mapped_jpy bigint, unmapped_jpy bigint,
  unknown_line_rows integer, unclassified_component_count integer, unmapped_component_count integer,
  sku_unclassified_component_count integer, sku_unmapped_component_count integer, finance_legacy_rows integer,
  contribution_before_ad_incl_jpy bigint, contribution_before_ad_excl numeric, contribution_after_ad_incl numeric, contribution_after_ad_excl numeric,
  profit_after_account_fees_incl numeric, profit_after_account_fees_excl numeric,
  contribution_after_ad_assuming_incomplete_zero_incl numeric, contribution_after_ad_assuming_incomplete_zero_excl numeric,
  profit_after_account_fees_assuming_incomplete_zero_incl numeric, profit_after_account_fees_assuming_incomplete_zero_excl numeric,
  before_ad_incomplete_days date[], before_ad_incomplete_day_count integer,
  after_ad_incomplete_days date[], after_ad_incomplete_day_count integer,
  after_account_fees_incomplete_days date[], after_account_fees_incomplete_day_count integer,
  profit_incomplete_reasons text[], master_basis text, master_note_counts jsonb,
  calculation_version text, finance_coverage_generation bigint, finance_source_revision bigint, calculated_at timestamptz)
language plpgsql stable as $$
begin
  -- 0050: 日の合計は最大 93 日 (両端を含む = to − from <= 92)。本番で 1〜9 月が 120 秒で打ち切り (行の本体を期間の全部の日で作る) = 守り。
  --   400 日の確かめ (assert_args) より先 = 401 日以上でも 93 日の文が出る (#1562 Codex R1 Low 2。null は下の assert_args が拒む)。
  --   🚨 長い期間 (年の合計など) は分けて呼んでつなげても正式な合計にならない (正式かどうかは期間の全部の日で決まる) = 今は利用者がいないので 93 日のまま。
  --      長い期間の合計は、夜に月ごとの集計表を作る別の設計 (後で・#1562 Codex R1 Medium 3)
  if p_to - p_from > 92 then
    raise exception 'invalid_input: 日の合計は 93 日まで (両端を含む。% 〜 % = % 日)。長い期間は月ごとに呼ぶ', p_from, p_to, p_to - p_from + 1 using errcode = '22023';
  end if;
  perform mart.amazon_profit_assert_args(p_company_id, p_mall, p_scope_key, p_from, p_to);
  return query select * from mart._amazon_profit_totals(p_company_id, p_mall, p_scope_key, p_from, p_to);
end
$$;
comment on function mart.amazon_profit_day_totals_range(smallint, text, text, date, date) is 'Amazon の利益の mart (0049・D7b-3・0050 で最大 93 日): 日 / 暦の月 / 期間の中の月の小計 / 期間の合計 (row_kind)。寄与・広告の後・月の手数料を引いた後を列の組ごとの条件で正式にし、不完全な日を返す';

-- ─── 権限 (0049 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function mart.amazon_profit_args_ok(smallint, text, text, date, date) to watcher';
  end if;
end $$;
