-- 0043 Amazon 財務の受け口を作り直す (F2b-1。2026-09-29)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』v5.1 (Codex 設計レビュー 5 巡・R4「着手してよい」)。
--   0012 の旧互換 (legacy_*・v_finance_daily_legacy (明細ごとの ABS・単価は trunc)・finance_daily の「≥ 0」の CHECK) は、
--   2026-09-29 の SQLite の日次の財務の決まりの変更 (#1522 / #1525 = 符号つきの正味を反転・返金の範囲・ポイント・値引きの税) で古くなった (D-49 = 捨てる)。
--   0012 の表は本番で 0 行 (送り手が無かった) = 作り直して消えるものは無い。念のため最初に 4 つとも空であることを確かめ、空でなければ止める。
--
--   core.order_finance_daily   = 注文 × 計上日 × SKU × 行の種類 (line_kind) × 取得元 = 1 行。金額は整数円・決済の符号のまま (売上 +・手数料 / 返金 / 値引き −・補てんは符号のまま)
--                                注文番号の無い行は 日ごとの疑似注文 '-:YYYY-MM-DD' (D-50)。SKU はあれば持つ (補てんなど)・無ければ '-'
--                                net_jpy = 20 金額列 + unmapped_jpy (= 決済の行の金額の全部)。内訳 3 列と account_fee_amount_jpy は net に入れない
--   core.apply_order_finance_batch() = 注文 (疑似注文) の集合を丸ごと置き換える。checksum は受け口 (Render の Node) が内容から計算し直した値を渡す (送り手の申告は ingest で突き合わせ済み)
--   mart.v_finance_daily       = SQLite の f_amazon_finance_sku_daily_v1 と同じ列名・同じ式 (原価・利益の列は持たない = profit_before_cogs_jpy)
--   mart.v_finance_account_fees_monthly = SQLite の f_amazon_account_fees_monthly_v1 と同じ (月 × 手数料の種類・符号は負 = 費用)
--   mart.v_order_finance_summary / mart.v_order_finance_uncovered (policy が無い日 + policy の source と行の source が違う日)
--   mart.finance_daily         = run_id publish の表 (この PR では作り直すだけ。公開は D7b)
--   policy                     = amazon / jp に amazon_settlement_unified [2026-01-01, 無期限) (D-51)
-- 🚨 0004 の raw・0011 の在庫・0013 の注文には触らない。

-- ─── 安全の手順: 4 つの表を排他で押さえ、空でなければ止める (同じ取引で最後まで) ───
lock table core.finance_source_policy, core.order_finance_receipts, core.order_finance_daily, mart.finance_daily in access exclusive mode;
do $$
declare n_policy bigint; n_receipts bigint; n_daily bigint; n_mart bigint;
begin
  select count(*) into n_policy from core.finance_source_policy;
  select count(*) into n_receipts from core.order_finance_receipts;
  select count(*) into n_daily from core.order_finance_daily;
  select count(*) into n_mart from mart.finance_daily;
  if n_policy + n_receipts + n_daily + n_mart > 0 then
    raise exception '0043: 0012 の表が空ではない (policy %, receipts %, order_finance_daily %, finance_daily %) = 誰かが送った。作り直しの前提が崩れているので止める',
      n_policy, n_receipts, n_daily, n_mart;
  end if;
end $$;

-- ─── 0012 の古いものを名前で順に消す (cascade は使わない) ───
drop view mart.v_order_finance_summary;
drop view mart.v_order_finance_uncovered;
drop view mart.v_finance_daily_legacy;
drop function core.assert_legacy_complete(smallint, text, text, date, date);
drop function core.apply_order_finance_batch(smallint, text, text, text, bigint, text, text, jsonb);
drop table core.order_finance_daily;
drop table mart.finance_daily;

-- ─── 取得元: amazon_settlement_unified (miniPC の出現順つき重複除去の後) を足す (D-51) ───
alter table core.finance_source_policy drop constraint finance_source_policy_source_check;
alter table core.finance_source_policy add constraint ck_finance_source_policy_source
  check (source in ('amazon_settlement_flat_v1', 'amazon_settlement_flat_v2', 'amazon_finances_api', 'amazon_settlement_unified', 'mall_finance_daily_v1'));

-- ─── 注文 × 計上日 × SKU × 行の種類 × 取得元 ───
create table core.order_finance_daily (
  company_id                 smallint not null references core.companies,
  mall                       text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key                  text not null,
  mall_order_no              text not null,             -- 本物の注文番号 / 注文番号の無い行は '-:YYYY-MM-DD' (計上日)
  economic_date_jst          date not null,
  seller_sku                 text not null check (seller_sku <> ''),   -- '-' = SKU が無い
  line_kind                  text not null check (line_kind in ('sku', 'storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory',
                                                                  'subscription', 'easy_ship', 'other_account_fee', 'not_account_fee', 'unknown')),
  source                     text not null check (source in ('amazon_settlement_flat_v1', 'amazon_settlement_flat_v2', 'amazon_finances_api', 'amazon_settlement_unified', 'mall_finance_daily_v1')),
  sku_id                     bigint,
  listing_id                 bigint,
  currency                   text not null default 'JPY' check (currency = 'JPY'),
  units_ordered              integer not null default 0,
  -- 金額 20 列 (整数円・決済の符号のまま・net に入る)
  sales_principal_jpy        bigint not null default 0,
  sales_shipping_jpy         bigint not null default 0,
  sales_giftwrap_jpy         bigint not null default 0,
  sales_tax_jpy              bigint not null default 0,
  commission_jpy             bigint not null default 0,  -- Commission + RefundCommission
  fba_fulfillment_jpy        bigint not null default 0,
  fba_storage_jpy            bigint not null default 0,
  closing_fee_jpy            bigint not null default 0,
  shipping_chargeback_jpy    bigint not null default 0,
  giftwrap_chargeback_jpy    bigint not null default 0,
  promotion_jpy              bigint not null default 0,
  points_jpy                 bigint not null default 0,  -- PointsGranted / PointsReturned (#1525)
  warehouse_damage_jpy       bigint not null default 0,
  warehouse_lost_jpy         bigint not null default 0,
  safe_t_jpy                 bigint not null default 0,
  refund_principal_jpy       bigint not null default 0,  -- 返金のすべて (本体 + 送料・ギフト包装の返金 + 返品の手数料 + カードの支払い取り消しの本体)
  reversal_reimbursement_jpy bigint not null default 0,
  misc_fee_jpy               bigint not null default 0,
  other_fee_jpy              bigint not null default 0,
  other_amount_jpy           bigint not null default 0,
  unmapped_jpy               bigint not null default 0,  -- 20 列のどれにも入らない金額 (shipment_fee / order_fee / direct_payment ほか)。日次の view には入れない
  net_jpy                    bigint not null default 0,  -- 20 列 + unmapped (CHECK)
  -- 内訳 (net に入れない・親の列の一部)
  promotion_tax_jpy          bigint not null default 0,  -- promotion のうち TaxDiscount
  refund_principal_customer_jpy bigint not null default 0, -- refund のうち Refund / Refund_Retrocharge / Order_Retrocharge の本体 (返品数の推定)
  refund_principal_atoz_jpy  bigint not null default 0,  -- refund のうち A-to-z の本体 (返品数の推定)
  account_fee_amount_jpy     bigint not null default 0,  -- SKU の無い行の other_amount + item_related_fee (月の手数料の build と同じ足し方)
  source_lines               integer not null check (source_lines > 0),
  received_batch_seq         bigint not null,
  source_updated_at          timestamptz not null,
  transform_version          text not null,
  content_hash               text not null,
  built_at                   timestamptz not null default now(),
  primary key (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source),
  constraint ck_order_finance_daily_net check (net_jpy = sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy + sales_tax_jpy
    + commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + closing_fee_jpy + shipping_chargeback_jpy + giftwrap_chargeback_jpy + promotion_jpy + points_jpy
    + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + refund_principal_jpy + reversal_reimbursement_jpy
    + misc_fee_jpy + other_fee_jpy + other_amount_jpy + unmapped_jpy),
  -- SKU のある行 = 'sku' / SKU の無い行 = 手数料の種類 (どちらか一方)
  constraint ck_order_finance_daily_kind check ((seller_sku = '-') = (line_kind <> 'sku')),
  constraint ck_order_finance_daily_account_fee check (line_kind <> 'sku' or account_fee_amount_jpy = 0),
  -- 疑似注文の番号 = '-:' + 計上日
  constraint ck_order_finance_daily_pseudo check (mall_order_no not like '-%' or mall_order_no = '-:' || economic_date_jst::text),
  foreign key (company_id, mall, scope_key, mall_order_no) references core.order_finance_receipts (company_id, mall, scope_key, mall_order_no),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id)
);
create index ix_order_finance_daily_date on core.order_finance_daily (company_id, mall, scope_key, economic_date_jst);
create index ix_order_finance_daily_sku on core.order_finance_daily (sku_id, economic_date_jst) where sku_id is not null;

-- ─── 注文 (疑似注文) の集合を丸ごと置き換える (1 取引の中で呼ぶ)。戻り値 = 'applied' / 'same' / 'stale' ───
--   p_set_checksum = 受け口が内容から計算し直した集合の checksum (apps/company-db/finance/order-finance-checksum.mjs。送り手の申告とは ingest で突き合わせ済み)
--   ① 世代が既存より小さい = 'stale' (何も変えない) ② checksum と変換の版がどちらも同じ = 'same' (世代だけ進める)
--   ③ 同じ世代なのに内容か版が違う = 例外 ④ それ以外 = 削除 → 挿入 → 受領状態の更新 (空の集合 = 注文の行が全部消えた・受領状態は残る)
--   金額が整数でない ('1.5'::bigint は例外)・未知の line_kind・疑似注文の番号と計上日が違う = 例外 (表の CHECK)
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

-- ─── 採用する行 (policy が指す source・その日) ───
-- 日次の財務 (SQLite の f_amazon_finance_sku_daily_v1 と同じ列名・同じ式。line_kind = 'sku' の行・日 × SKU)
--   費用の列 = −Σ(符号のまま) / 売上・補てん・misc / other = Σ / closing_fee・units_marketplace_guarantee = 0 (build の固定値)
--   単価 (計上日の月 × SKU) = ROUND(Σ Order の本体 (micro) ÷ Σ Order の数量) (build の unit_price_month。SQLite の ROUND は double の割り算の後に 0.5 を 0 から遠い方へ = ここも double で割ってから numeric の round)
--   返品数 = ROUND(−Σ 本体の返金 (micro) ÷ 単価)・単価が無い / 0 なら 0
--   profit_before_cogs_jpy = build の profit_amount から原価を除いた式 (sales_tax・misc・other・promotion_tax は入れない・points は引く)
create view mart.v_finance_daily as
with adopted as (
  select f.* from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst
 where f.line_kind = 'sku'
),
daily as (
  select company_id, mall, scope_key, economic_date_jst, seller_sku,
         sum(units_ordered) as units_ordered,
         sum(sales_principal_jpy) as sales_principal_jpy, sum(sales_shipping_jpy) as sales_shipping_jpy, sum(sales_giftwrap_jpy) as sales_giftwrap_jpy, sum(sales_tax_jpy) as sales_tax_jpy,
         -sum(commission_jpy) as commission_jpy, -sum(fba_fulfillment_jpy) as fba_fulfillment_jpy, -sum(fba_storage_jpy) as fba_storage_jpy,
         -sum(shipping_chargeback_jpy) as shipping_chargeback_jpy, -sum(giftwrap_chargeback_jpy) as giftwrap_chargeback_jpy,
         -sum(promotion_jpy) as promotion_jpy, -sum(promotion_tax_jpy) as promotion_tax_jpy, -sum(points_jpy) as points_jpy,
         sum(warehouse_damage_jpy) as warehouse_damage_jpy, sum(warehouse_lost_jpy) as warehouse_lost_jpy, sum(safe_t_jpy) as safe_t_jpy,
         -sum(refund_principal_jpy) as refund_principal_jpy, -sum(refund_principal_customer_jpy) as refund_customer_jpy, -sum(refund_principal_atoz_jpy) as refund_atoz_jpy,
         sum(reversal_reimbursement_jpy) as reversal_reimbursement_jpy,
         sum(misc_fee_jpy) as misc_fee_jpy, sum(other_fee_jpy) as other_fee_jpy, sum(other_amount_jpy) as other_amount_jpy,
         sum(source_lines) as source_lines, count(*) as order_rows
    from adopted
   group by company_id, mall, scope_key, economic_date_jst, seller_sku
),
unit_price_month as (
  select company_id, mall, scope_key, seller_sku, date_trunc('month', economic_date_jst)::date as month,
         case when sum(units_ordered) = 0 then null
              else round(((sum(sales_principal_jpy) * 1000000)::float8 / sum(units_ordered)::float8)::numeric) end as unit_price_micro
    from adopted
   group by company_id, mall, scope_key, seller_sku, date_trunc('month', economic_date_jst)::date
),
est as (
  select d.*,
         coalesce(round(((d.refund_customer_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8)::numeric), 0)::integer as units_refunded_customer,
         coalesce(round(((d.refund_atoz_jpy * 1000000)::float8 / nullif(u.unit_price_micro, 0)::float8)::numeric), 0)::integer as units_a_to_z_refund
    from daily d
    left join unit_price_month u
      on u.company_id = d.company_id and u.mall = d.mall and u.scope_key = d.scope_key and u.seller_sku = d.seller_sku and u.month = date_trunc('month', d.economic_date_jst)::date
)
select company_id, mall, scope_key, economic_date_jst, seller_sku,
       units_ordered::integer as units_ordered, units_refunded_customer, 0::integer as units_marketplace_guarantee, units_a_to_z_refund,
       (units_ordered - units_refunded_customer - units_a_to_z_refund)::integer as units_net_sold,
       sales_principal_jpy::bigint as sales_principal_jpy, sales_shipping_jpy::bigint as sales_shipping_jpy, sales_giftwrap_jpy::bigint as sales_giftwrap_jpy, sales_tax_jpy::bigint as sales_tax_jpy,
       commission_jpy::bigint as commission_jpy, fba_fulfillment_jpy::bigint as fba_fulfillment_jpy, fba_storage_jpy::bigint as fba_storage_jpy, 0::bigint as closing_fee_jpy,
       shipping_chargeback_jpy::bigint as shipping_chargeback_jpy, giftwrap_chargeback_jpy::bigint as giftwrap_chargeback_jpy,
       promotion_jpy::bigint as promotion_jpy, promotion_tax_jpy::bigint as promotion_tax_jpy, points_jpy::bigint as points_jpy,
       warehouse_damage_jpy::bigint as warehouse_damage_jpy, warehouse_lost_jpy::bigint as warehouse_lost_jpy, safe_t_jpy::bigint as safe_t_jpy,
       refund_principal_jpy::bigint as refund_principal_jpy, reversal_reimbursement_jpy::bigint as reversal_reimbursement_jpy,
       misc_fee_jpy::bigint as misc_fee_jpy, other_fee_jpy::bigint as other_fee_jpy, other_amount_jpy::bigint as other_amount_jpy,
       (sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy - commission_jpy - fba_fulfillment_jpy - fba_storage_jpy
         - shipping_chargeback_jpy - giftwrap_chargeback_jpy - promotion_jpy - points_jpy - refund_principal_jpy
         + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy)::bigint as profit_before_cogs_jpy,
       source_lines::integer as source_lines, order_rows::integer as order_rows
  from est;

-- 月のアカウント単位の手数料 (SQLite の f_amazon_account_fees_monthly_v1 と同じ: 月 × 種類・Σ(other_amount + item_related_fee)・負 = 費用)
--   注文番号の有無は問わない (本物の注文の Easy Ship の料金も入る)・手数料に入れない種類 (not_account_fee / unknown) は入れない
create view mart.v_finance_account_fees_monthly as
select f.company_id, f.mall, f.scope_key, date_trunc('month', f.economic_date_jst)::date as month_start_jst, f.line_kind as fee_type,
       sum(f.account_fee_amount_jpy)::bigint as amount_jpy, sum(f.source_lines)::integer as row_count
  from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst
 where f.line_kind in ('storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee')
 group by f.company_id, f.mall, f.scope_key, date_trunc('month', f.economic_date_jst)::date, f.line_kind;

-- 注文の累計 (本物の注文だけ = 疑似注文 '-:' は入れない)。policy が指す source の行だけ
create view mart.v_order_finance_summary as
select f.company_id, f.mall, f.scope_key, f.mall_order_no,
       min(f.economic_date_jst) as first_economic_date_jst, max(f.economic_date_jst) as last_economic_date_jst,
       count(*)::integer as lines, sum(f.units_ordered)::integer as units_ordered,
       sum(f.sales_principal_jpy)::bigint as sales_principal_jpy, sum(f.sales_shipping_jpy)::bigint as sales_shipping_jpy, sum(f.sales_giftwrap_jpy)::bigint as sales_giftwrap_jpy, sum(f.sales_tax_jpy)::bigint as sales_tax_jpy,
       sum(f.commission_jpy)::bigint as commission_jpy, sum(f.fba_fulfillment_jpy)::bigint as fba_fulfillment_jpy, sum(f.fba_storage_jpy)::bigint as fba_storage_jpy, sum(f.closing_fee_jpy)::bigint as closing_fee_jpy,
       sum(f.shipping_chargeback_jpy)::bigint as shipping_chargeback_jpy, sum(f.giftwrap_chargeback_jpy)::bigint as giftwrap_chargeback_jpy,
       sum(f.promotion_jpy)::bigint as promotion_jpy, sum(f.points_jpy)::bigint as points_jpy,
       sum(f.warehouse_damage_jpy)::bigint as warehouse_damage_jpy, sum(f.warehouse_lost_jpy)::bigint as warehouse_lost_jpy, sum(f.safe_t_jpy)::bigint as safe_t_jpy,
       sum(f.refund_principal_jpy)::bigint as refund_principal_jpy, sum(f.reversal_reimbursement_jpy)::bigint as reversal_reimbursement_jpy,
       sum(f.misc_fee_jpy)::bigint as misc_fee_jpy, sum(f.other_fee_jpy)::bigint as other_fee_jpy, sum(f.other_amount_jpy)::bigint as other_amount_jpy,
       sum(f.unmapped_jpy)::bigint as unmapped_jpy, sum(f.net_jpy)::bigint as net_jpy
  from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst
 where f.mall_order_no not like '-%'
 group by f.company_id, f.mall, f.scope_key, f.mall_order_no;

-- 採用されない行 (0 件が正常): policy がその日を覆っていない行 (reason = no_policy) と、覆っているが source が違う行 (reason = source_mismatch)
create view mart.v_order_finance_uncovered as
select f.company_id, f.mall, f.scope_key, f.mall_order_no, f.economic_date_jst, f.seller_sku, f.line_kind, f.source, f.net_jpy, f.received_batch_seq,
       case when not exists (
              select 1 from core.finance_source_policy p
               where p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key
                 and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst) then 'no_policy'
            else 'source_mismatch' end as reason
  from core.order_finance_daily f
 where not exists (
   select 1 from core.finance_source_policy p
    where p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
      and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst);

-- ─── 日次の公開の表 (run_id publish・D7b で使う。この PR では作るだけ)。v_finance_daily と同じ列の決まり (費用は正・戻りは負 = 負もありうる) ───
create table mart.finance_daily (
  run_id                     text not null,
  company_id                 smallint not null references core.companies,
  economic_date_jst          date not null,
  mall                       text not null,
  scope_key                  text not null,
  listing_id                 bigint,
  sku_id                     bigint,
  seller_sku                 text check (seller_sku is null or seller_sku not in ('', '-')),
  currency                   text not null default 'JPY' check (currency = 'JPY'),
  units_ordered              integer not null default 0,
  units_refunded_customer    integer not null default 0,
  units_marketplace_guarantee integer not null default 0,
  units_a_to_z_refund        integer not null default 0,
  units_net_sold             integer not null default 0,
  sales_principal_jpy        bigint not null default 0,
  sales_shipping_jpy         bigint not null default 0,
  sales_giftwrap_jpy         bigint not null default 0,
  sales_tax_jpy              bigint not null default 0,
  commission_jpy             bigint not null default 0,
  fba_fulfillment_jpy        bigint not null default 0,
  fba_storage_jpy            bigint not null default 0,
  closing_fee_jpy            bigint not null default 0,
  shipping_chargeback_jpy    bigint not null default 0,
  giftwrap_chargeback_jpy    bigint not null default 0,
  promotion_jpy              bigint not null default 0,
  promotion_tax_jpy          bigint not null default 0,
  points_jpy                 bigint not null default 0,
  warehouse_damage_jpy       bigint not null default 0,
  warehouse_lost_jpy         bigint not null default 0,
  safe_t_jpy                 bigint not null default 0,
  refund_principal_jpy       bigint not null default 0,
  reversal_reimbursement_jpy bigint not null default 0,
  misc_fee_jpy               bigint not null default 0,
  other_fee_jpy              bigint not null default 0,
  other_amount_jpy           bigint not null default 0,
  profit_before_cogs_jpy     bigint not null default 0,
  source                     text not null,
  source_lines               integer not null default 0,
  built_at                   timestamptz not null default now(),
  grain_key                  text not null generated always as (coalesce(listing_id::text, '-') || '|' || coalesce(sku_id::text, '-') || '|' || coalesce(seller_sku, '-')) stored,
  primary key (run_id, company_id, economic_date_jst, mall, scope_key, grain_key),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_finance_daily_date on mart.finance_daily (company_id, mall, economic_date_jst);

-- ─── policy: amazon / jp は amazon_settlement_unified [2026-01-01, 無期限) (D-51。決済の行は 2026-01 から) ───
insert into core.finance_source_policy (company_id, mall, scope_key, period_from, period_to, source, note)
select c.company_id, 'amazon', 'jp', date '2026-01-01', null, 'amazon_settlement_unified', '0043: miniPC の出現順つき重複除去の後 (V1 / V2 の混ざりは重複除去で 1 つ)'
  from core.companies c where c.company_id = 1;   -- B-Faith (0008。注文の受け口も company_id = 1)

-- ─── 権限 (0041 と同じ形: ロールがあれば付ける。watcher は既定の権限でも読めるが明示する) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.order_finance_daily, mart.finance_daily, mart.v_finance_daily, mart.v_finance_account_fees_monthly, mart.v_order_finance_summary, mart.v_order_finance_uncovered to watcher';
  end if;
end $$;
