-- 0012 Amazon 財務 (Phase 6・期限 2026-11-11。08 §4.4 / F2 の受け皿)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』§4.4 (草案 v4 = Codex 3 巡)。
--   明細 (決済レポートの行) は Company DB に置かない (D-37 = a)。miniPC が明細に「採用する取得元 (policy)」と訂正を当ててから、
--   **注文 × 計上日 × SKU × 取得元** の集約を作って push する (§4.7 の契約。取込ジョブは F2 の miniPC 側 = 次の PR)。
--   core.finance_source_policy   = 会社 × モール × scope × 期間 → 採用する取得元 (旧レポート v1 / 後継 V2 / Finances API)。期間 [from, to) の重複を trigger で禁止 (advisory lock。§7.7)
--   core.order_finance_daily     = 注文 × 計上日 × SKU × 取得元 = 1 行 (JPY だけ。net は各列の合計 = CHECK で検算)
--   mart.v_order_finance_summary = 注文の累計。policy が指す source の行だけを足す (旧と V2 の両方が入っていても二重にならない)
--   mart.finance_daily           = 日次集計 (run_id publish)。f_amazon_finance_sku_daily_v1 と同じ列名 → 移行期 (F3) はこれと f_* を突き合わせる (D-35 = 差 0 円)
-- 🚨 0004 の raw 13 ソースにも 0011 の在庫にも触らない。

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

-- 注文 × 計上日 × SKU × 取得元 の集約 (#6)。miniPC が明細に policy と訂正を当ててから作り、mirror に push (§4.7)。
-- 注文番号を持たない費用 (保管料・月額など) は mall_order_no = '-'、seller_sku = '-' の行。再構築で置き換える (append-only にしない)
create table core.order_finance_daily (
  company_id          smallint not null references core.companies,
  mall                text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key           text not null,
  mall_order_no       text not null,                    -- '-' = 注文に紐付かない費用
  economic_date_jst   date not null,
  seller_sku          text not null,                    -- '-' = SKU に紐付かない
  source              text not null,                    -- finance_source_policy.source の値
  sku_id              bigint,
  listing_id          bigint,
  currency            text not null default 'JPY' check (currency = 'JPY'),
  qty_net             integer not null default 0,
  principal_jpy       bigint not null default 0,
  tax_jpy             bigint not null default 0,
  shipping_jpy        bigint not null default 0,
  giftwrap_jpy        bigint not null default 0,
  promotion_jpy       bigint not null default 0,
  commission_jpy      bigint not null default 0,
  fulfillment_fee_jpy bigint not null default 0,
  storage_fee_jpy     bigint not null default 0,
  other_fee_jpy       bigint not null default 0,
  refund_jpy          bigint not null default 0,
  other_jpy           bigint not null default 0,
  net_jpy             bigint not null default 0,        -- 上の合計 (取込側で検算 + ここでも CHECK)
  source_lines        integer not null,
  received_batch_seq  bigint not null,                  -- 最後に受け取った世代 (単調増加。内容が同じでも進める #7)
  source_updated_at   timestamptz not null,             -- 元の最終計上時刻
  transform_version   text not null,
  content_hash        text not null,
  built_at            timestamptz not null default now(),
  primary key (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source),
  constraint ck_order_finance_daily_source check (source in ('amazon_settlement_flat_v1','amazon_settlement_flat_v2','amazon_finances_api','mall_finance_daily_v1')),
  constraint ck_order_finance_daily_net check (net_jpy = principal_jpy + tax_jpy + shipping_jpy + giftwrap_jpy + promotion_jpy + commission_jpy + fulfillment_fee_jpy + storage_fee_jpy + other_fee_jpy + refund_jpy + other_jpy),
  constraint ck_order_finance_daily_lines check (source_lines > 0),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id)
);
create index ix_order_finance_daily_date on core.order_finance_daily (company_id, mall, economic_date_jst);
create index ix_order_finance_daily_sku on core.order_finance_daily (sku_id, economic_date_jst) where sku_id is not null;

-- 注文の累計 (view)。policy が指す source の行だけ。sum(bigint) は numeric になるので bigint に戻す (03 §10: 円の列は bigint)
create or replace view mart.v_order_finance_summary as
select f.company_id, f.mall, f.scope_key, f.mall_order_no,
       min(f.economic_date_jst) as first_economic_date_jst, max(f.economic_date_jst) as last_economic_date_jst,
       sum(f.qty_net)::bigint as qty_net, sum(f.principal_jpy)::bigint as principal_jpy, sum(f.tax_jpy)::bigint as tax_jpy, sum(f.shipping_jpy)::bigint as shipping_jpy,
       sum(f.promotion_jpy)::bigint as promotion_jpy, sum(f.commission_jpy)::bigint as commission_jpy, sum(f.fulfillment_fee_jpy)::bigint as fulfillment_fee_jpy,
       sum(f.other_fee_jpy + f.storage_fee_jpy)::bigint as other_fee_jpy, sum(f.refund_jpy)::bigint as refund_jpy, sum(f.other_jpy)::bigint as other_jpy, sum(f.net_jpy)::bigint as net_jpy
  from core.order_finance_daily f
  join core.finance_source_policy p
    on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
   and daterange(p.period_from, p.period_to, '[)') @> f.economic_date_jst
 where f.mall_order_no <> '-'
 group by f.company_id, f.mall, f.scope_key, f.mall_order_no;

-- 日次集計 (run_id publish。f_amazon_finance_sku_daily_v1 と同じ列名)。listing / sku / seller_sku のどれが null でも主キーが組める (grain_key)
create table mart.finance_daily (
  run_id               text not null,
  company_id           smallint not null references core.companies,
  economic_date_jst    date not null,
  mall                 text not null,
  scope_key            text not null,
  listing_id           bigint,
  sku_id               bigint,
  seller_sku           text,
  currency             text not null default 'JPY' check (currency = 'JPY'),
  units_net_sold       integer not null default 0,
  sales_principal_jpy  bigint not null default 0,
  sales_shipping_jpy   bigint not null default 0,
  sales_tax_jpy        bigint not null default 0,
  commission_jpy       bigint not null default 0,
  fba_fulfillment_jpy  bigint not null default 0,
  fba_storage_jpy      bigint not null default 0,
  promotion_jpy        bigint not null default 0,
  refund_principal_jpy bigint not null default 0,
  other_fee_jpy        bigint not null default 0,
  other_amount_jpy     bigint not null default 0,
  source               text not null,
  built_at             timestamptz not null default now(),
  grain_key            text not null generated always as (coalesce(listing_id::text, '-') || '|' || coalesce(sku_id::text, '-') || '|' || coalesce(seller_sku, '-')) stored,
  primary key (run_id, company_id, economic_date_jst, mall, scope_key, grain_key),
  foreign key (company_id, listing_id) references core.listings (company_id, listing_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_finance_daily_date on mart.finance_daily (company_id, mall, economic_date_jst);
