-- 0012 Amazon 財務 (Phase 6・期限 2026-11-11。08 §4.4 / F2 の受け皿)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』§4.4 / §4.7 (草案 v4 = Codex 3 巡 + PR #1322 のレビュー)。
--   明細 (決済レポートの行) は Company DB に置かない (D-37 = a)。miniPC が明細に「採用する取得元 (policy)」と訂正を当ててから、
--   **注文 × 計上日 × SKU × 取得元** の集約を作って push する (§4.7 の契約。miniPC 側の取込 = F2b は次の PR)。
--
--   core.finance_source_policy      = 会社 × モール × scope × 期間 [from, to) → 採用する取得元。重複は trigger で禁止 (advisory lock。§7.7)
--   core.finance_policy_gaps()      = 期間の中で policy が無い区間。core.assert_finance_policy_covered() = 無ければ例外 (公開集計はこれを通す = 「未設定の期間は集計を失敗させる」)
--   core.order_finance_receipts     = 注文単位の受領状態 (最後に受け取った世代・集合の checksum)。明細集合が空になっても世代が残る (古い世代を拒む根拠)
--   core.order_finance_daily        = 注文 × 計上日 × SKU × 取得元 = 1 行。**符号は決済レポートのまま** (売上 +、手数料・返金・販促 −、補填は符号そのまま)。net = 19 列の合計 (CHECK)
--   core.apply_order_finance_batch()= §4.7 の契約で注文の明細集合を丸ごと置き換える (受領行を for update → 古い世代は拒む → 内容が同じなら世代だけ進める → 置換。1 取引)
--   mart.v_order_finance_summary    = 注文の累計 (全内訳)。policy が指す source の行だけを足す (旧と V2 の両方が入っていても二重にならない)
--   mart.v_order_finance_uncovered  = policy がどの source も指していない計上日の行 (黙って落ちる行を見える所に出す)
--   mart.finance_daily              = 日次集計 (run_id publish)。**旧表 f_amazon_finance_sku_daily_v1 と同じ列名・同じ符号規約** (手数料・返金・販促は絶対値、補填は符号そのまま)
--                                     → 移行期 (F3) はこれと f_* を列ごとに突き合わせる (D-35 = 差 0 円)。原価・利益の列は持たない (D7 の v_order_profit)
--
-- 符号の対応 (order_finance_daily → finance_daily。公開の step で変換する):
--   sales_* : そのまま / commission・fba_fulfillment・fba_storage・closing_fee・shipping_chargeback・giftwrap_chargeback・promotion・refund_principal : 符号を反転 (絶対値) /
--   warehouse_damage・warehouse_lost・safe_t・reversal_reimbursement : そのまま / misc_fee・other_fee・other_amount : そのまま (旧表は「保持のみ・利益式に入れない」)
-- 🚨 0004 の raw 13 ソースにも 0011 の在庫にも触らない。

-- ─── policy ───
create table core.finance_source_policy (
  policy_id    bigint generated always as identity primary key,
  company_id   smallint not null references core.companies,
  mall         text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key    text not null,
  period_from  date not null,
  period_to    date,
  source       text not null check (source in ('amazon_settlement_flat_v1','amazon_settlement_flat_v2','amazon_finances_api','mall_finance_daily_v1')),
  note         text,
  created_at   timestamptz not null default now(),
  unique (company_id, mall, scope_key, period_from, source),
  constraint ck_finance_source_policy_period check (period_to is null or period_to > period_from)
);
-- 期間の重複禁止 (#9: READ COMMITTED 前提。advisory lock で直列化し、ロック取得後の文は最新の commit を見る)
create or replace function core.check_finance_policy_overlap() returns trigger language plpgsql as $$
begin
  perform pg_advisory_xact_lock(hashtext('core.finance_source_policy'));
  if exists (
    select 1 from core.finance_source_policy p
     where p.company_id = new.company_id and p.mall = new.mall and p.scope_key = new.scope_key and p.policy_id <> coalesce(new.policy_id, -1)
       and daterange(p.period_from, p.period_to, '[)') && daterange(new.period_from, new.period_to, '[)')
  ) then
    raise exception 'finance_source_policy overlaps an existing period for % / % / %', new.company_id, new.mall, new.scope_key;
  end if;
  return new;
end
$$;
create trigger trg_finance_source_policy_overlap before insert or update on core.finance_source_policy for each row execute function core.check_finance_policy_overlap();

-- [p_from, p_to) の中で policy が無い区間 (無ければ 0 行)
create or replace function core.finance_policy_gaps(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (gap_from date, gap_to date) language plpgsql stable as $$
declare
  cur date := p_from;
  p record;
begin
  if p_to is null or p_from is null or p_to <= p_from then return; end if;
  for p in
    select period_from, period_to from core.finance_source_policy
     where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key
       and period_from < p_to and (period_to is null or period_to > p_from)
     order by period_from
  loop
    if p.period_from > cur then gap_from := cur; gap_to := least(p.period_from, p_to); return next; end if;
    if p.period_to is null then cur := p_to; exit; end if;
    if p.period_to > cur then cur := p.period_to; end if;
    exit when cur >= p_to;
  end loop;
  if cur < p_to then gap_from := cur; gap_to := p_to; return next; end if;
  return;
end
$$;
-- 公開集計 (finance_daily の build) はこれを通してから書く = 「未設定の期間は集計を失敗させる」
create or replace function core.assert_finance_policy_covered(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date) returns void language plpgsql stable as $$
declare g record;
begin
  select * into g from core.finance_policy_gaps(p_company_id, p_mall, p_scope_key, p_from, p_to) limit 1;
  if found then
    raise exception 'finance_source_policy has a gap [%, %) for % / % / % (set the policy before aggregating)', g.gap_from, g.gap_to, p_company_id, p_mall, p_scope_key;
  end if;
end
$$;

-- ─── 注文単位の受領状態 (§4.7: 世代の判定の根拠。明細集合が空になっても残る) ───
create table core.order_finance_receipts (
  company_id          smallint not null references core.companies,
  mall                text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key           text not null,
  mall_order_no       text not null,
  received_batch_seq  bigint not null,                  -- 最後に受け取った世代 (単調増加。内容が同じでも進める)
  set_checksum        text not null,                    -- 明細集合の checksum (内容が同じかの判定)
  lines               integer not null,                 -- いまの明細の行数 (0 = 集合が空)
  transform_version   text,
  received_at         timestamptz not null default now(),
  updated_at          timestamptz not null default now(),
  primary key (company_id, mall, scope_key, mall_order_no)
);
create trigger trg_order_finance_receipts_touch before update on core.order_finance_receipts for each row execute function core.touch_updated_at();

-- ─── 注文 × 計上日 × SKU × 取得元 の集約 ───
-- 注文番号を持たない費用 (保管料・月額など) は mall_order_no = '-'、seller_sku = '-' の行。再構築で置き換える (append-only にしない)
create table core.order_finance_daily (
  company_id                 smallint not null references core.companies,
  mall                       text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key                  text not null,
  mall_order_no              text not null,             -- '-' = 注文に紐付かない費用
  economic_date_jst          date not null,
  seller_sku                 text not null,             -- '-' = SKU に紐付かない
  source                     text not null check (source in ('amazon_settlement_flat_v1','amazon_settlement_flat_v2','amazon_finances_api','mall_finance_daily_v1')),
  sku_id                     bigint,
  listing_id                 bigint,
  currency                   text not null default 'JPY' check (currency = 'JPY'),
  -- 数量 (旧表と同じ 5 列)
  units_ordered              integer not null default 0,
  units_refunded_customer    integer not null default 0,
  units_marketplace_guarantee integer not null default 0,
  units_a_to_z_refund        integer not null default 0,
  units_net_sold             integer not null default 0,
  -- 金額 19 列 (符号は決済レポートのまま)
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
  warehouse_damage_jpy       bigint not null default 0,
  warehouse_lost_jpy         bigint not null default 0,
  safe_t_jpy                 bigint not null default 0,
  refund_principal_jpy       bigint not null default 0,
  reversal_reimbursement_jpy bigint not null default 0,
  misc_fee_jpy               bigint not null default 0,
  other_fee_jpy              bigint not null default 0,
  other_amount_jpy           bigint not null default 0,
  net_jpy                    bigint not null default 0, -- 19 列の合計 (CHECK)
  source_lines               integer not null check (source_lines > 0),
  received_batch_seq         bigint not null,           -- 受領状態表と同じ世代 (行にも残す)
  source_updated_at          timestamptz not null,      -- 元の最終計上時刻
  transform_version          text not null,
  content_hash               text not null,
  built_at                   timestamptz not null default now(),
  primary key (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source),
  constraint ck_order_finance_daily_net check (net_jpy = sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy + sales_tax_jpy
    + commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + closing_fee_jpy + shipping_chargeback_jpy + giftwrap_chargeback_jpy + promotion_jpy
    + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + refund_principal_jpy + reversal_reimbursement_jpy
    + misc_fee_jpy + other_fee_jpy + other_amount_jpy),
  foreign key (company_id, mall, scope_key, mall_order_no) references core.order_finance_receipts (company_id, mall, scope_key, mall_order_no),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id)
);
create index ix_order_finance_daily_date on core.order_finance_daily (company_id, mall, economic_date_jst);
create index ix_order_finance_daily_sku on core.order_finance_daily (sku_id, economic_date_jst) where sku_id is not null;

-- §4.7 の契約で「注文の明細集合」を丸ごと置き換える (1 取引の中で呼ぶ)。戻り値 = 'applied' / 'same' / 'stale'
--   p_rows = jsonb の配列。要素の鍵 = economic_date_jst, seller_sku, source, source_lines, source_updated_at, content_hash, 数量・金額の列名 (無い列は 0)。
--   ① received_batch_seq が既存より小さい世代は拒む ('stale'、何も変えない) ② 集合の checksum が同じなら世代だけ進める ('same')
--   ③ 同じ世代なのに内容が違えば例外 (契約違反) ④ それ以外は削除 → 挿入 → 受領状態の更新。空の集合 = 注文の明細が全部消えた (受領状態は残る)
--   listing_id は seller_sku から解決 (会社 × モール で一意に当たるときだけ)。sku_id は F2b で (構成の解決が要る)
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
  n_rows := jsonb_array_length(p_rows);
  if exists (select 1 from jsonb_array_elements(p_rows) r where coalesce(r ->> 'mall_order_no', p_mall_order_no) <> p_mall_order_no) then
    raise exception 'p_rows contains a different mall_order_no (expected %)', p_mall_order_no;
  end if;
  -- 受領状態の行を作って for update (同じ注文の同時更新を直列化)
  insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines, transform_version)
  values (p_company_id, p_mall, p_scope_key, p_mall_order_no, 0, '', 0, p_transform_version)
  on conflict (company_id, mall, scope_key, mall_order_no) do nothing;
  select * into rec from core.order_finance_receipts
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no for update;
  if p_batch_seq < rec.received_batch_seq then return 'stale'; end if;
  if rec.received_batch_seq > 0 and rec.set_checksum = p_set_checksum then
    -- 内容が同じ = 世代だけ進める (他の列と updated_at は変えない)
    if p_batch_seq > rec.received_batch_seq then
      update core.order_finance_receipts set received_batch_seq = p_batch_seq
       where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
      update core.order_finance_daily set received_batch_seq = p_batch_seq
       where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
    end if;
    return 'same';
  end if;
  if p_batch_seq = rec.received_batch_seq then
    raise exception 'batch % for order % was already applied with a different checksum (% <> %)', p_batch_seq, p_mall_order_no, rec.set_checksum, p_set_checksum;
  end if;
  delete from core.order_finance_daily
   where company_id = p_company_id and mall = p_mall and scope_key = p_scope_key and mall_order_no = p_mall_order_no;
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, listing_id,
    units_ordered, units_refunded_customer, units_marketplace_guarantee, units_a_to_z_refund, units_net_sold,
    sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy, commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
    shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy, warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy, refund_principal_jpy,
    reversal_reimbursement_jpy, misc_fee_jpy, other_fee_jpy, other_amount_jpy, net_jpy,
    source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
  select p_company_id, p_mall, p_scope_key, p_mall_order_no, (r ->> 'economic_date_jst')::date, coalesce(nullif(trim(r ->> 'seller_sku'), ''), '-'), r ->> 'source',
         (select min(l.listing_id) from core.listings l
           where l.company_id = p_company_id and l.mall = p_mall and l.listing_norm = core.norm_code(r ->> 'seller_sku')
           having count(*) = 1),   -- 会社 × モール で 1 件だけ当たるときに限る (店舗が増えて曖昧なら null)
         coalesce((r ->> 'units_ordered')::integer, 0), coalesce((r ->> 'units_refunded_customer')::integer, 0), coalesce((r ->> 'units_marketplace_guarantee')::integer, 0),
         coalesce((r ->> 'units_a_to_z_refund')::integer, 0), coalesce((r ->> 'units_net_sold')::integer, 0),
         coalesce((r ->> 'sales_principal_jpy')::bigint, 0), coalesce((r ->> 'sales_shipping_jpy')::bigint, 0), coalesce((r ->> 'sales_giftwrap_jpy')::bigint, 0), coalesce((r ->> 'sales_tax_jpy')::bigint, 0),
         coalesce((r ->> 'commission_jpy')::bigint, 0), coalesce((r ->> 'fba_fulfillment_jpy')::bigint, 0), coalesce((r ->> 'fba_storage_jpy')::bigint, 0), coalesce((r ->> 'closing_fee_jpy')::bigint, 0),
         coalesce((r ->> 'shipping_chargeback_jpy')::bigint, 0), coalesce((r ->> 'giftwrap_chargeback_jpy')::bigint, 0), coalesce((r ->> 'promotion_jpy')::bigint, 0),
         coalesce((r ->> 'warehouse_damage_jpy')::bigint, 0), coalesce((r ->> 'warehouse_lost_jpy')::bigint, 0), coalesce((r ->> 'safe_t_jpy')::bigint, 0), coalesce((r ->> 'refund_principal_jpy')::bigint, 0),
         coalesce((r ->> 'reversal_reimbursement_jpy')::bigint, 0), coalesce((r ->> 'misc_fee_jpy')::bigint, 0), coalesce((r ->> 'other_fee_jpy')::bigint, 0), coalesce((r ->> 'other_amount_jpy')::bigint, 0),
         coalesce((r ->> 'net_jpy')::bigint,
           coalesce((r ->> 'sales_principal_jpy')::bigint, 0) + coalesce((r ->> 'sales_shipping_jpy')::bigint, 0) + coalesce((r ->> 'sales_giftwrap_jpy')::bigint, 0) + coalesce((r ->> 'sales_tax_jpy')::bigint, 0)
           + coalesce((r ->> 'commission_jpy')::bigint, 0) + coalesce((r ->> 'fba_fulfillment_jpy')::bigint, 0) + coalesce((r ->> 'fba_storage_jpy')::bigint, 0) + coalesce((r ->> 'closing_fee_jpy')::bigint, 0)
           + coalesce((r ->> 'shipping_chargeback_jpy')::bigint, 0) + coalesce((r ->> 'giftwrap_chargeback_jpy')::bigint, 0) + coalesce((r ->> 'promotion_jpy')::bigint, 0)
           + coalesce((r ->> 'warehouse_damage_jpy')::bigint, 0) + coalesce((r ->> 'warehouse_lost_jpy')::bigint, 0) + coalesce((r ->> 'safe_t_jpy')::bigint, 0) + coalesce((r ->> 'refund_principal_jpy')::bigint, 0)
           + coalesce((r ->> 'reversal_reimbursement_jpy')::bigint, 0) + coalesce((r ->> 'misc_fee_jpy')::bigint, 0) + coalesce((r ->> 'other_fee_jpy')::bigint, 0) + coalesce((r ->> 'other_amount_jpy')::bigint, 0)),
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

-- ─── 注文の累計 (view)。policy が指す source の行だけ。sum(bigint) は numeric になるので bigint に戻す (03 §10) ───
create or replace view mart.v_order_finance_summary as
select f.company_id, f.mall, f.scope_key, f.mall_order_no,
       min(f.economic_date_jst) as first_economic_date_jst, max(f.economic_date_jst) as last_economic_date_jst,
       count(*)::integer as lines,
       sum(f.units_ordered)::integer as units_ordered, sum(f.units_refunded_customer)::integer as units_refunded_customer,
       sum(f.units_marketplace_guarantee)::integer as units_marketplace_guarantee, sum(f.units_a_to_z_refund)::integer as units_a_to_z_refund, sum(f.units_net_sold)::integer as units_net_sold,
       sum(f.sales_principal_jpy)::bigint as sales_principal_jpy, sum(f.sales_shipping_jpy)::bigint as sales_shipping_jpy, sum(f.sales_giftwrap_jpy)::bigint as sales_giftwrap_jpy, sum(f.sales_tax_jpy)::bigint as sales_tax_jpy,
       sum(f.commission_jpy)::bigint as commission_jpy, sum(f.fba_fulfillment_jpy)::bigint as fba_fulfillment_jpy, sum(f.fba_storage_jpy)::bigint as fba_storage_jpy, sum(f.closing_fee_jpy)::bigint as closing_fee_jpy,
       sum(f.shipping_chargeback_jpy)::bigint as shipping_chargeback_jpy, sum(f.giftwrap_chargeback_jpy)::bigint as giftwrap_chargeback_jpy, sum(f.promotion_jpy)::bigint as promotion_jpy,
       sum(f.warehouse_damage_jpy)::bigint as warehouse_damage_jpy, sum(f.warehouse_lost_jpy)::bigint as warehouse_lost_jpy, sum(f.safe_t_jpy)::bigint as safe_t_jpy,
       sum(f.refund_principal_jpy)::bigint as refund_principal_jpy, sum(f.reversal_reimbursement_jpy)::bigint as reversal_reimbursement_jpy,
       sum(f.misc_fee_jpy)::bigint as misc_fee_jpy, sum(f.other_fee_jpy)::bigint as other_fee_jpy, sum(f.other_amount_jpy)::bigint as other_amount_jpy,
       sum(f.net_jpy)::bigint as net_jpy
  from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst
 where f.mall_order_no <> '-'
 group by f.company_id, f.mall, f.scope_key, f.mall_order_no;

-- policy がどの source も指していない計上日の行 (= 累計にも日次にも入らない行。0 件が正常)
create or replace view mart.v_order_finance_uncovered as
select f.company_id, f.mall, f.scope_key, f.mall_order_no, f.economic_date_jst, f.seller_sku, f.source, f.net_jpy, f.received_batch_seq
  from core.order_finance_daily f
 where not exists (
   select 1 from core.finance_source_policy p
    where p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key
      and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst);

-- ─── 日次集計 (run_id publish。旧表 f_amazon_finance_sku_daily_v1 と同じ列名・同じ符号規約) ───
-- listing / sku / seller_sku のどれが null でも主キーが組める (grain_key)。seller_sku の「無し」は null だけ ('-' や '' は入れない = grain_key の衝突を防ぐ)
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
  commission_jpy             bigint not null default 0,  -- 絶対値 (旧表の規約)
  fba_fulfillment_jpy        bigint not null default 0,  -- 絶対値
  fba_storage_jpy            bigint not null default 0,  -- 絶対値
  closing_fee_jpy            bigint not null default 0,  -- 絶対値
  shipping_chargeback_jpy    bigint not null default 0,  -- 絶対値
  giftwrap_chargeback_jpy    bigint not null default 0,  -- 絶対値
  promotion_jpy              bigint not null default 0,  -- 絶対値
  warehouse_damage_jpy       bigint not null default 0,  -- 符号そのまま
  warehouse_lost_jpy         bigint not null default 0,  -- 符号そのまま
  safe_t_jpy                 bigint not null default 0,  -- 符号そのまま
  refund_principal_jpy       bigint not null default 0,  -- 絶対値
  reversal_reimbursement_jpy bigint not null default 0,  -- 符号そのまま
  misc_fee_jpy               bigint not null default 0,
  other_fee_jpy              bigint not null default 0,
  other_amount_jpy           bigint not null default 0,
  source                     text not null,
  source_row_count           integer not null default 0,
  built_at                   timestamptz not null default now(),
  grain_key                  text not null generated always as (coalesce(listing_id::text, '-') || '|' || coalesce(sku_id::text, '-') || '|' || coalesce(seller_sku, '-')) stored,
  primary key (run_id, company_id, economic_date_jst, mall, scope_key, grain_key),
  constraint ck_finance_daily_abs check (commission_jpy >= 0 and fba_fulfillment_jpy >= 0 and fba_storage_jpy >= 0 and closing_fee_jpy >= 0
    and shipping_chargeback_jpy >= 0 and giftwrap_chargeback_jpy >= 0 and promotion_jpy >= 0 and refund_principal_jpy >= 0),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_finance_daily_date on mart.finance_daily (company_id, mall, economic_date_jst);
