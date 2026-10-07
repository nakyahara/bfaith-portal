-- 0035: 広告費の日次 (Company DB構想 11 = 08 §4.5 の mart.ad_spend_daily を具体化。D7b の一部)
--   まず Amazon の SP (スポンサープロダクト) だけ。元 = miniPC の warehouse.db fact_ad_spend (日 × キャンペーン × 対象) と ads_fetch_days (取得の完全性の記録)。
--   楽天 RPP (月 × 商品・7 月で止まっている)・au PAY (費用 0) は、取込が動き・中身が出てから同じ受け皿に足す (mall / ad_type を足すだけ)。
--
-- 決め (設計 11 + Codex 設計レビュー D1。原文 = AI_reference CompanyDB構想/_raw/11_Codex設計レビュー_広告費_20260927.md):
--   ・1 日 = 1 まとまりで置き換える (日の中の行は取り直しで増えも減りもする)。置き換えの単位 = (会社, モール, scope, 広告タイプ, 日)
--     = SP だけの要求はほかの広告タイプの行を消さない
--   ・日の状態 core.ad_spend_days に「どの取得の世代か」(source_generation = miniPC でレポートを頼んだ時刻 ms・source_report_id) と内容の指紋を持つ。
--     受け口は 古い世代 → stale (書かない) / 同じ世代・同じレポート・同じ指紋 → same / 同じ世代で違うもの → 409 (どちらが新しいか分からない) /
--     新しい世代で指紋が同じ → refreshed (行は触らず世代だけ進める) / 新しい世代で指紋が違う → 置き換え
--   ・行が 0 の日も「0 行で取れた」として日の状態を持つ (取れていない日 = 状態の行が無い)
--   ・金額は numeric(14,2) (Amazon の費用は円未満の端数がある = 2026-09-27 に 45.7 万行中 9.2 万行。送り手も受け口も 2 桁より細かい値は拒む = 黙って丸めない)。日の合計は numeric(16,2)
--     🚨 列名に _jpy を付けない (03 §10 = 「*_jpy は円の整数 bigint」の約束。端数のある円は発注の unit_cost と同じく別の名前で numeric)
--   ・広告経由の売上は 1 日の帰属 (sales1d) = 列名で明示。売上・数量が分からない行は null (0 にしない。mart は分からない行の数を出す)
--   ・出品との結び: 粒度 sku の行だけ core.resolve_listing_id (会社 × モールで 1 件に当たるとき)。asin・none は null。
--     マスタが後から増えたときのために core.relink_ad_spend_listings (listing_id が null の sku の行を解き直す)
--   ・指紋は受け口 (Render の Node) が検証済みの行から計算する (送り手と同じ関数)。DB の関数では作らない
-- 🚨 個人情報なし (キャンペーン ID・SKU・数字だけ)。

create table core.ad_spend_days (
  company_id        smallint not null references core.companies,
  mall              text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari','other')),
  scope_key         text not null,
  ad_type           text not null check (ad_type in ('SP')),
  date_jst          date not null,
  source_generation bigint not null check (source_generation > 0),
  source_report_id  text not null check (source_report_id <> ''),
  checksum          text not null check (checksum ~ '^[0-9a-f]{64}$'),
  row_count         integer not null check (row_count >= 0),
  cost_total        numeric(16,2) not null check (cost_total >= 0),
  ingest_run_id     text not null,
  updated_at        timestamptz not null default now(),
  primary key (company_id, mall, scope_key, ad_type, date_jst)
);
comment on table core.ad_spend_days is '広告費の日の状態 (0035)。行がある = その日を最後まで取れた取得で置き換えた (0 行の日も)。source_generation = miniPC でレポートを頼んだ時刻 (ms)';

create table core.ad_spend_daily (
  company_id         smallint not null,
  mall               text not null,
  scope_key          text not null,
  ad_type            text not null,
  date_jst           date not null,
  campaign_id        text not null check (campaign_id <> '' and length(campaign_id) <= 100),
  target_granularity text not null check (target_granularity in ('sku','asin','none')),
  target_code        text not null check ((target_granularity = 'none') = (target_code = '') and length(target_code) <= 200),
  listing_id         bigint references core.listings,
  clicks             integer not null check (clicks >= 0),
  impressions        integer not null check (impressions >= 0),
  ad_cost            numeric(14,2) not null check (ad_cost >= 0),
  ad_sales_1d        numeric(14,2) check (ad_sales_1d >= 0),
  units_1d           integer check (units_1d >= 0),
  ingest_run_id      text not null,
  primary key (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code),
  foreign key (company_id, mall, scope_key, ad_type, date_jst) references core.ad_spend_days on delete cascade,
  constraint ck_ad_spend_listing_sku check (listing_id is null or target_granularity = 'sku')
);
create index ix_ad_spend_daily_listing on core.ad_spend_daily (company_id, listing_id, date_jst) where listing_id is not null;
create index ix_ad_spend_daily_unlinked on core.ad_spend_daily (company_id, mall) where listing_id is null and target_granularity = 'sku';
comment on table core.ad_spend_daily is '広告費の日次の行 (0035)。日 × キャンペーン × 対象 (SKU / ASIN / none)。ad_sales_1d = 広告経由の売上 (1 日の帰属)';

-- マスタが後から増えたとき: listing_id が null の sku の行を解き直す。戻り値 = 結んだ行数
create or replace function core.relink_ad_spend_listings(p_company_id smallint) returns integer language plpgsql as $$
declare n integer;
begin
  update core.ad_spend_daily a set listing_id = x.lid
    from (select company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code,
                 core.resolve_listing_id(company_id, mall, target_code) as lid
            from core.ad_spend_daily
           where company_id = p_company_id and listing_id is null and target_granularity = 'sku') x
   where x.lid is not null and a.company_id = x.company_id and a.mall = x.mall and a.scope_key = x.scope_key and a.ad_type = x.ad_type
     and a.date_jst = x.date_jst and a.campaign_id = x.campaign_id and a.target_granularity = x.target_granularity and a.target_code = x.target_code;
  get diagnostics n = row_count;
  return n;
end $$;

-- 日 × モール × scope × 広告タイプ × 出品 (結べなかった行は 粒度 + コード) の合計。キャンペーンはまとめる。
-- 🚨 売上・数量の分からない行は sum から外れる = 数を出す (0 として隠さない)
create or replace view mart.v_ad_spend_daily as
  select company_id, mall, scope_key, ad_type, date_jst, listing_id,
         case when listing_id is null then target_granularity end as unresolved_granularity,
         case when listing_id is null then target_code end as unresolved_code,
         sum(ad_cost) as ad_cost, sum(clicks)::bigint as clicks, sum(impressions)::bigint as impressions,
         sum(ad_sales_1d) as ad_sales_1d, count(*) filter (where ad_sales_1d is null)::int as sales_unknown_rows,
         sum(units_1d)::bigint as units_1d, count(*) filter (where units_1d is null)::int as units_unknown_rows,
         count(*)::int as rows
    from core.ad_spend_daily
   group by company_id, mall, scope_key, ad_type, date_jst, listing_id,
            case when listing_id is null then target_granularity end, case when listing_id is null then target_code end;
