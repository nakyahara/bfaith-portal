-- 0047 Amazon の利益の mart の下ごしらえ (D7b-1a。2026-09-30)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 (§3.2・§3.5b・§3.6・§3.7・§4 の D7b-1)。
--   D7b-1 のうち coverage (決済のそろい・coordinator・inventory・lease・文書の版) を除いた部分。coverage は D7b-1b。
--
-- ① core.order_finance_daily に「分けられない決済の部品」の数と額の 4 列 (既存の行は 0。送り手の変換の版を上げて全部送り直す)
--    unclassified_component_count = 0 でない「分けられない」生の部品の数 (全部の line_kind)
--    unclassified_mapped_jpy      = その部品の符号つきの合計 (決済の符号のまま)
--    unclassified_abs_jpy         = その部品ごとの絶対値の合計 (まとめた後の金額の絶対値ではない = +100 と −100 は 200)
--    unmapped_component_count     = unmapped_jpy に入った 0 でない生の部品の数
--    🚨 集約の後では復元できない (+100 と −100 が打ち消して 0 になる) → 送り手の変換が生の部品を分類するときに数える
--       (apps/company-db/push/amazon-finance-transform.mjs)。正式な利益を止めるかどうかは **金額でなく数** で決める (§3.7・R5 H3・R22 H1)
--    分類の優先順位 (§3.7・生の部品を 1 回だけ消費する):
--       ① 月の手数料の line_kind の行の手数料の材料 (other_amount + item_related_fee = account_fee_amount_jpy) → ② not_account_fee の行
--       → ③ unknown の行 (unknown_line_mapped) → ④ どれにも消費されなかった部品 = 「分けられない」→ ⑤ unmapped_jpy
--       SKU の行で分けられない = misc_fee / other_fee / other_amount の列に入った部品 (D-63 で分けるまで)。
--       月の手数料の行で分けられない = 手数料の材料以外の部品 (price・promotion・misc_fee・other_fee)
-- ② core.apply_order_finance_batch を 4 列も入れるように (引数・戻り値・規則は 0043 のまま)
-- ③ mart.finance_daily_sku_range(会社, モール, scope, from, to) = 日 × 正規化 seller SKU (core.norm_code) の子の粒度 (§3.2・R13 H1)
--    D7b-3 の利益の関数が計算のときに今のマスタで出品に結び直してまとめる材料。今の mart.finance_daily_range (0045) は画面・突き合わせのため残し、戻りの型は変えない (R20 M4)
--    金額の式は 0045 と同じ。違いは次だけ:
--      ・粒度 = 日 × core.norm_code(seller_sku) (0045 は受け取った seller_sku そのまま)
--      ・closing_fee_jpy = −Σ closing_fee (0045 は固定の 0) で profit_before_cogs_jpy から引く (§3.5b。今の決済には 0 = 金額は変わらない)
--      ・received_listing_ids (受け取りのときの listing_id の集合・ID の昇順・診断) / received_listing_unresolved_count (受け取りのとき未解決だった行の数)
--      ・net_jpy / unmapped_jpy / unclassified_* / unmapped_component_count (決済の符号のまま = 0045 の費用を正にした列とは符号が逆)
--      ・refund_units_status / 丸める前の返品数 / 推定できない返品の額
--    返品数の推定は 0045 と同じ「計上日の月 × 受け取った seller SKU の単価」で子ごとに計算してから正規化 SKU にまとめる (合計は 0045 と同じ)
--    契約: from <= to・最大 400 日 (両端を含む日数 = to − from <= 399)・違えば例外 (errcode 22023)。期間の月だけを読む・nested loop を使わない (0045 と同じ)

-- ─── ① 4 列 (既存の行は 0) ───
alter table core.order_finance_daily
  add column unclassified_component_count integer not null default 0,
  add column unclassified_mapped_jpy      bigint  not null default 0,
  add column unclassified_abs_jpy         bigint  not null default 0,
  add column unmapped_component_count     integer not null default 0;
-- 数と額の形 (既存の行 = 全部 0 で満たす)。「0 でない部品があれば数 > 0」は送り手と受け口の形の確かめ (order-finance-checksum.mjs) が見る
--   (unmapped_jpy <> 0 なら数 > 0 は既存の行 (数 = 0) が満たさないので表の CHECK にしない = 送り直すまでの行の 'same' の更新で落とさない)
alter table core.order_finance_daily
  add constraint ck_order_finance_daily_unclassified check (
    unclassified_component_count >= 0 and unclassified_abs_jpy >= 0
    and abs(unclassified_mapped_jpy) <= unclassified_abs_jpy
    and (unclassified_component_count = 0) = (unclassified_abs_jpy = 0)),
  add constraint ck_order_finance_daily_unmapped_count check (unmapped_component_count >= 0);

-- ─── ② apply = 0043 と同じ + 4 列 ───
create or replace function core.apply_order_finance_batch(
  p_company_id smallint, p_mall text, p_scope_key text, p_mall_order_no text,
  p_batch_seq bigint, p_set_checksum text, p_transform_version text, p_rows jsonb
) returns text language plpgsql as $$
declare
  rec core.order_finance_receipts%rowtype;
  n_rows integer;
  n_ins integer;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then raise exception 'p_rows must be a json array'; end if;
  if p_batch_seq is null or p_batch_seq <= 0 then raise exception 'p_batch_seq must be positive'; end if;
  if p_set_checksum is null or p_set_checksum !~ '^[0-9a-f]{64}$' then raise exception 'p_set_checksum must be a sha256 hex'; end if;
  if p_transform_version is null or p_transform_version = '' then raise exception 'p_transform_version is required'; end if;
  n_rows := jsonb_array_length(p_rows);
  if exists (select 1 from jsonb_array_elements(p_rows) r where coalesce(r ->> 'mall_order_no', p_mall_order_no) <> p_mall_order_no) then
    raise exception 'p_rows contains a different mall_order_no (expected %)', p_mall_order_no;
  end if;
  if exists (select 1 from jsonb_array_elements(p_rows) r where r ? 'currency' and r ->> 'currency' <> 'JPY') then
    raise exception 'p_rows contains a non-JPY currency (only JPY is accepted)';
  end if;
  insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines, transform_version)
  values (p_company_id, p_mall, p_scope_key, p_mall_order_no, 0, '', 0, p_transform_version)
  on conflict (company_id, mall, scope_key, mall_order_no) do nothing;
  select * into rec from core.order_finance_receipts
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no for update;
  if p_batch_seq < rec.received_batch_seq then return 'stale'; end if;
  if rec.received_batch_seq > 0 and rec.set_checksum = p_set_checksum and rec.transform_version is not distinct from p_transform_version then
    if p_batch_seq > rec.received_batch_seq then
      update core.order_finance_receipts set received_batch_seq = p_batch_seq
       where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
      update core.order_finance_daily set received_batch_seq = p_batch_seq
       where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
    end if;
    return 'same';
  end if;
  if p_batch_seq = rec.received_batch_seq then
    raise exception 'batch % for order % was already applied with a different content or version (% / % <> % / %)',
      p_batch_seq, p_mall_order_no, rec.set_checksum, rec.transform_version, p_set_checksum, p_transform_version;
  end if;
  delete from core.order_finance_daily
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source, listing_id,
    units_ordered,
    sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy, commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
    shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy, points_jpy, warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy, refund_principal_jpy,
    reversal_reimbursement_jpy, misc_fee_jpy, other_fee_jpy, other_amount_jpy, unmapped_jpy, net_jpy,
    promotion_tax_jpy, refund_principal_customer_jpy, refund_principal_atoz_jpy, account_fee_amount_jpy,
    unclassified_component_count, unclassified_mapped_jpy, unclassified_abs_jpy, unmapped_component_count,
    source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
  select p_company_id, p_mall, p_scope_key, p_mall_order_no, (r ->> 'economic_date_jst')::date, r ->> 'seller_sku', r ->> 'line_kind', r ->> 'source',
         case when r ->> 'seller_sku' = '-' then null else
           (select min(l.listing_id) from core.listings l
             where l.company_id = p_company_id and l.mall = p_mall and l.listing_norm = core.norm_code(r ->> 'seller_sku')
             having count(*) = 1) end,   -- 会社 × モール で 1 件だけ当たるときに限る
         coalesce((r ->> 'units_ordered')::integer, 0),
         coalesce((r ->> 'sales_principal_jpy')::bigint, 0), coalesce((r ->> 'sales_shipping_jpy')::bigint, 0), coalesce((r ->> 'sales_giftwrap_jpy')::bigint, 0), coalesce((r ->> 'sales_tax_jpy')::bigint, 0),
         coalesce((r ->> 'commission_jpy')::bigint, 0), coalesce((r ->> 'fba_fulfillment_jpy')::bigint, 0), coalesce((r ->> 'fba_storage_jpy')::bigint, 0), coalesce((r ->> 'closing_fee_jpy')::bigint, 0),
         coalesce((r ->> 'shipping_chargeback_jpy')::bigint, 0), coalesce((r ->> 'giftwrap_chargeback_jpy')::bigint, 0), coalesce((r ->> 'promotion_jpy')::bigint, 0), coalesce((r ->> 'points_jpy')::bigint, 0),
         coalesce((r ->> 'warehouse_damage_jpy')::bigint, 0), coalesce((r ->> 'warehouse_lost_jpy')::bigint, 0), coalesce((r ->> 'safe_t_jpy')::bigint, 0), coalesce((r ->> 'refund_principal_jpy')::bigint, 0),
         coalesce((r ->> 'reversal_reimbursement_jpy')::bigint, 0), coalesce((r ->> 'misc_fee_jpy')::bigint, 0), coalesce((r ->> 'other_fee_jpy')::bigint, 0), coalesce((r ->> 'other_amount_jpy')::bigint, 0),
         coalesce((r ->> 'unmapped_jpy')::bigint, 0),
         coalesce((r ->> 'sales_principal_jpy')::bigint, 0) + coalesce((r ->> 'sales_shipping_jpy')::bigint, 0) + coalesce((r ->> 'sales_giftwrap_jpy')::bigint, 0) + coalesce((r ->> 'sales_tax_jpy')::bigint, 0)
           + coalesce((r ->> 'commission_jpy')::bigint, 0) + coalesce((r ->> 'fba_fulfillment_jpy')::bigint, 0) + coalesce((r ->> 'fba_storage_jpy')::bigint, 0) + coalesce((r ->> 'closing_fee_jpy')::bigint, 0)
           + coalesce((r ->> 'shipping_chargeback_jpy')::bigint, 0) + coalesce((r ->> 'giftwrap_chargeback_jpy')::bigint, 0) + coalesce((r ->> 'promotion_jpy')::bigint, 0) + coalesce((r ->> 'points_jpy')::bigint, 0)
           + coalesce((r ->> 'warehouse_damage_jpy')::bigint, 0) + coalesce((r ->> 'warehouse_lost_jpy')::bigint, 0) + coalesce((r ->> 'safe_t_jpy')::bigint, 0) + coalesce((r ->> 'refund_principal_jpy')::bigint, 0)
           + coalesce((r ->> 'reversal_reimbursement_jpy')::bigint, 0) + coalesce((r ->> 'misc_fee_jpy')::bigint, 0) + coalesce((r ->> 'other_fee_jpy')::bigint, 0) + coalesce((r ->> 'other_amount_jpy')::bigint, 0)
           + coalesce((r ->> 'unmapped_jpy')::bigint, 0),
         coalesce((r ->> 'promotion_tax_jpy')::bigint, 0), coalesce((r ->> 'refund_principal_customer_jpy')::bigint, 0), coalesce((r ->> 'refund_principal_atoz_jpy')::bigint, 0),
         coalesce((r ->> 'account_fee_amount_jpy')::bigint, 0),
         -- 0047: 分けられない部品の数と額 (古い送り手の payload には無い = 0)
         coalesce((r ->> 'unclassified_component_count')::integer, 0), coalesce((r ->> 'unclassified_mapped_jpy')::bigint, 0),
         coalesce((r ->> 'unclassified_abs_jpy')::bigint, 0), coalesce((r ->> 'unmapped_component_count')::integer, 0),
         (r ->> 'source_lines')::integer, p_batch_seq, (r ->> 'source_updated_at')::timestamptz, p_transform_version, r ->> 'content_hash'
    from jsonb_array_elements(p_rows) r;
  get diagnostics n_ins = row_count;
  if n_ins <> n_rows then raise exception 'inserted % rows but % were given', n_ins, n_rows; end if;
  update core.order_finance_receipts
     set received_batch_seq = p_batch_seq, set_checksum = p_set_checksum, lines = n_rows, transform_version = p_transform_version, received_at = now()
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
  return 'applied';
end
$$;

-- ─── ③ 日 × 正規化 seller SKU の子の粒度 ───
-- refund_units_status (子の状態。§3.2・R17 M5。出品の行への縮約は D7b-3 の利益の関数):
--   no_refund                          = 返品数の推定に使う返金 (本体の customer / A-to-z) が無い
--   unit_price_missing                 = 返金があるのに、その月 × 受け取った seller SKU の単価が無い (Order の数量 0・本体 0) = 返品数を推定できない
--                                        (返品数は 0045 と同じく 0 個・推定できない額 = refund_unestimated_jpy。D7b-3 で原価・正式な利益 null)
--   estimated_partial_month_unit_price = 単価はあるが、その月がまだ終わっていない (単価が動く = D7b-3 でその月の返品のある行の正式な利益は null)
--   estimated_monthly_unit_price       = 月の単価で割った推定 (実測ではない)
--   1 つの子 (日 × 正規化 SKU) に受け取った seller SKU が 2 つ以上あるときは弱い方 (unit_price_missing > partial > monthly > no_refund)
--   🚨 partial の判定 = 設計では「その月の最後の日が関数を呼んだ日より後、または月末まで決済がそろっていない」。
--      決済のそろい (core.finance_coverage) はまだ無い (D7b-1b) → 当面は **その計上日の月の最後の日が今日 (JST・statement_timestamp) 以降** を partial とする。
--      D7b-1b で coverage の complete_to (月末まで決済がそろったか) に置き換える
-- units_*_unrounded = 丸める前の返品数 (返金 ÷ 単価・小数 6 桁)。unit_price_missing の子は null
-- refund_unestimated_jpy = unit_price_missing の返金の額 (本体の customer + A-to-z・費用を正 = 0045 の refund の列と同じ向き)
create or replace function mart.finance_daily_sku_range(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (company_id smallint, mall text, scope_key text, economic_date_jst date, seller_sku_norm text,
  received_listing_ids bigint[], received_listing_unresolved_count integer,
  units_ordered integer, units_refunded_customer integer, units_marketplace_guarantee integer, units_a_to_z_refund integer, units_net_sold integer,
  sales_principal_jpy bigint, sales_shipping_jpy bigint, sales_giftwrap_jpy bigint, sales_tax_jpy bigint,
  commission_jpy bigint, fba_fulfillment_jpy bigint, fba_storage_jpy bigint, closing_fee_jpy bigint,
  shipping_chargeback_jpy bigint, giftwrap_chargeback_jpy bigint,
  promotion_jpy bigint, promotion_tax_jpy bigint, points_jpy bigint,
  warehouse_damage_jpy bigint, warehouse_lost_jpy bigint, safe_t_jpy bigint,
  refund_principal_jpy bigint, reversal_reimbursement_jpy bigint,
  misc_fee_jpy bigint, other_fee_jpy bigint, other_amount_jpy bigint,
  profit_before_cogs_jpy bigint, source_lines integer, order_rows integer,
  net_jpy bigint, unmapped_jpy bigint,
  unclassified_component_count integer, unclassified_mapped_jpy bigint, unclassified_abs_jpy bigint, unmapped_component_count integer,
  refund_units_status text, units_refunded_customer_unrounded numeric, units_a_to_z_refund_unrounded numeric, refund_unestimated_jpy bigint)
language plpgsql stable
set enable_nestloop = off
as $$
#variable_conflict use_column
declare
  v_today date := (statement_timestamp() at time zone 'Asia/Tokyo')::date;   -- partial の判定 (D7b-1b で coverage に置き換える)
begin
  if p_company_id is null or p_mall is null or p_scope_key is null or p_from is null or p_to is null then
    raise exception 'invalid_input: 会社・モール・scope・from・to は必須' using errcode = '22023';
  end if;
  if p_from > p_to then
    raise exception 'invalid_input: from (%) が to (%) より後', p_from, p_to using errcode = '22023';
  end if;
  if p_to - p_from > 399 then
    raise exception 'invalid_input: 期間は 400 日まで (両端を含む。% 〜 % = % 日)', p_from, p_to, p_to - p_from + 1 using errcode = '22023';
  end if;
  return query
  with adopted as (
    select f.*, date_trunc('month', f.economic_date_jst)::date as month, core.norm_code(f.seller_sku) as sku_norm
      from core.order_finance_daily f
      join core.finance_source_policy p
        on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
       and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
     where f.line_kind = 'sku'
       and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
       -- 単価 (計上日の月 × SKU) は月の全部の行で決まる = 期間の最初の月の 1 日から最後の月の末日までを読む (0045 と同じ)
       and f.economic_date_jst >= date_trunc('month', p_from)::date
       and f.economic_date_jst < (date_trunc('month', p_to) + interval '1 month')::date
  ),
  -- 金額 (日 × 正規化 SKU・期間の日だけ)。費用の列の向きは 0045 と同じ (−Σ)。closing_fee は 0045 と違い実際の値
  child as (
    select economic_date_jst, sku_norm,
           sum(units_ordered) as units_ordered,
           sum(sales_principal_jpy) as sales_principal_jpy, sum(sales_shipping_jpy) as sales_shipping_jpy, sum(sales_giftwrap_jpy) as sales_giftwrap_jpy, sum(sales_tax_jpy) as sales_tax_jpy,
           -sum(commission_jpy) as commission_jpy, -sum(fba_fulfillment_jpy) as fba_fulfillment_jpy, -sum(fba_storage_jpy) as fba_storage_jpy, -sum(closing_fee_jpy) as closing_fee_jpy,
           -sum(shipping_chargeback_jpy) as shipping_chargeback_jpy, -sum(giftwrap_chargeback_jpy) as giftwrap_chargeback_jpy,
           -sum(promotion_jpy) as promotion_jpy, -sum(promotion_tax_jpy) as promotion_tax_jpy, -sum(points_jpy) as points_jpy,
           sum(warehouse_damage_jpy) as warehouse_damage_jpy, sum(warehouse_lost_jpy) as warehouse_lost_jpy, sum(safe_t_jpy) as safe_t_jpy,
           -sum(refund_principal_jpy) as refund_principal_jpy, sum(reversal_reimbursement_jpy) as reversal_reimbursement_jpy,
           sum(misc_fee_jpy) as misc_fee_jpy, sum(other_fee_jpy) as other_fee_jpy, sum(other_amount_jpy) as other_amount_jpy,
           sum(net_jpy) as net_jpy, sum(unmapped_jpy) as unmapped_jpy,
           sum(unclassified_component_count) as unclassified_component_count, sum(unclassified_mapped_jpy) as unclassified_mapped_jpy,
           sum(unclassified_abs_jpy) as unclassified_abs_jpy, sum(unmapped_component_count) as unmapped_component_count,
           coalesce(array_agg(distinct listing_id order by listing_id) filter (where listing_id is not null), '{}'::bigint[]) as received_listing_ids,
           count(*) filter (where listing_id is null) as received_listing_unresolved_count,
           sum(source_lines) as source_lines, count(*) as order_rows
      from adopted
     where economic_date_jst between p_from and p_to
     group by economic_date_jst, sku_norm
  ),
  -- 返品数の推定 = 0045 と同じ「計上日の月 × 受け取った seller SKU の単価」(子ごとに計算してから正規化 SKU にまとめる)
  raw_daily as (
    select economic_date_jst, seller_sku, sku_norm, month,
           -sum(refund_principal_customer_jpy) as refund_customer_jpy, -sum(refund_principal_atoz_jpy) as refund_atoz_jpy
      from adopted
     where economic_date_jst between p_from and p_to
     group by economic_date_jst, seller_sku, sku_norm, month
  ),
  unit_price_month as (
    select seller_sku, month,
           case when sum(units_ordered) = 0 then null
                else core.sqlite_round((sum(sales_principal_jpy) * 1000000)::float8 / sum(units_ordered)::float8) end as unit_price_micro
      from adopted
     group by seller_sku, month
  ),
  raw_est as (
    select d.economic_date_jst, d.sku_norm,
           coalesce(core.sqlite_round((d.refund_customer_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8), 0) as units_refunded_customer,
           coalesce(core.sqlite_round((d.refund_atoz_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8), 0) as units_a_to_z_refund,
           case when d.refund_customer_jpy = 0 then 0::numeric when coalesce(u.unit_price_micro, 0) = 0 then null
                else round(d.refund_customer_jpy * 1000000::numeric / u.unit_price_micro, 6) end as customer_unrounded,
           case when d.refund_atoz_jpy = 0 then 0::numeric when coalesce(u.unit_price_micro, 0) = 0 then null
                else round(d.refund_atoz_jpy * 1000000::numeric / u.unit_price_micro, 6) end as atoz_unrounded,
           -- 状態の強さ: 1 no_refund < 2 monthly < 3 partial < 4 unit_price_missing (子にまとめるときは大きい方)
           case when d.refund_customer_jpy = 0 and d.refund_atoz_jpy = 0 then 1
                when coalesce(u.unit_price_micro, 0) = 0 then 4
                when (d.month + interval '1 month' - interval '1 day')::date >= v_today then 3
                else 2 end as status_rank,
           case when (d.refund_customer_jpy <> 0 or d.refund_atoz_jpy <> 0) and coalesce(u.unit_price_micro, 0) = 0
                then d.refund_customer_jpy + d.refund_atoz_jpy else 0 end as unestimated_jpy
      from raw_daily d
      left join unit_price_month u on u.seller_sku = d.seller_sku and u.month = d.month
  ),
  child_refund as (
    select economic_date_jst, sku_norm,
           sum(units_refunded_customer) as units_refunded_customer, sum(units_a_to_z_refund) as units_a_to_z_refund,
           max(status_rank) as status_rank,
           case when max(status_rank) = 4 then null else sum(customer_unrounded) end as customer_unrounded,
           case when max(status_rank) = 4 then null else sum(atoz_unrounded) end as atoz_unrounded,
           sum(unestimated_jpy) as unestimated_jpy
      from raw_est
     group by economic_date_jst, sku_norm
  )
  select p_company_id, p_mall, p_scope_key, c.economic_date_jst, c.sku_norm,
         c.received_listing_ids, c.received_listing_unresolved_count::integer,
         c.units_ordered::integer, r.units_refunded_customer::integer, 0::integer, r.units_a_to_z_refund::integer,
         (c.units_ordered - r.units_refunded_customer - r.units_a_to_z_refund)::integer,
         c.sales_principal_jpy::bigint, c.sales_shipping_jpy::bigint, c.sales_giftwrap_jpy::bigint, c.sales_tax_jpy::bigint,
         c.commission_jpy::bigint, c.fba_fulfillment_jpy::bigint, c.fba_storage_jpy::bigint, c.closing_fee_jpy::bigint,
         c.shipping_chargeback_jpy::bigint, c.giftwrap_chargeback_jpy::bigint,
         c.promotion_jpy::bigint, c.promotion_tax_jpy::bigint, c.points_jpy::bigint,
         c.warehouse_damage_jpy::bigint, c.warehouse_lost_jpy::bigint, c.safe_t_jpy::bigint,
         c.refund_principal_jpy::bigint, c.reversal_reimbursement_jpy::bigint,
         c.misc_fee_jpy::bigint, c.other_fee_jpy::bigint, c.other_amount_jpy::bigint,
         -- §3.5b = 0045 の式 + closing_fee
         (c.sales_principal_jpy + c.sales_shipping_jpy + c.sales_giftwrap_jpy - c.commission_jpy - c.fba_fulfillment_jpy - c.fba_storage_jpy - c.closing_fee_jpy
           - c.shipping_chargeback_jpy - c.giftwrap_chargeback_jpy - c.promotion_jpy - c.points_jpy - c.refund_principal_jpy
           + c.warehouse_damage_jpy + c.warehouse_lost_jpy + c.safe_t_jpy + c.reversal_reimbursement_jpy)::bigint,
         c.source_lines::integer, c.order_rows::integer,
         c.net_jpy::bigint, c.unmapped_jpy::bigint,
         c.unclassified_component_count::integer, c.unclassified_mapped_jpy::bigint, c.unclassified_abs_jpy::bigint, c.unmapped_component_count::integer,
         (case r.status_rank when 1 then 'no_refund' when 2 then 'estimated_monthly_unit_price'
                             when 3 then 'estimated_partial_month_unit_price' else 'unit_price_missing' end)::text,
         r.customer_unrounded::numeric, r.atoz_unrounded::numeric, r.unestimated_jpy::bigint
    from child c
    join child_refund r on r.economic_date_jst = c.economic_date_jst and r.sku_norm = c.sku_norm;
end
$$;

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function mart.finance_daily_sku_range(smallint, text, text, date, date) to watcher';
  end if;
end $$;
