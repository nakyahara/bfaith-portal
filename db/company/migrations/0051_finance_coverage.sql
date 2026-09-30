-- 0051 決済のそろい core.finance_coverage (D7b-1b-2 = Render 側。2026-09-30)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1 (D-65・D-66)。
--   「Amazon 側で決済がそろった」と「その全部の行が Company DB に入った」の **両方** を満たす最後の日 = complete_to を、会社 × モール × scope × source ごとに 1 行で持つ。
--   値を作るのは miniPC の coordinator (D7b-1b-3・後の PR = 一覧・初期の印・文書の版・lease・source_revision の trigger)。この PR は受け皿と受け口だけ。
--
-- 作るもの:
--   ① core.finance_coverage = 今の世代の状態 (1 key = 1 行)。列は §3.1 の一覧 + 無効にした印 (invalidated_at / invalidated_reason)
--   ② core.finance_coverage_state(会社, モール, scope, source) を **差し替える** (0049 の「全部 null」の差し込み口。名前・引数・戻りの形は同じ = 呼び手の mart (0049) は変えない)
--      complete_to = state = complete **かつ complete の時に検査した policy の指紋 = 今の policy の指紋** のときだけ (updating・行なし・policy が変わった = null) /
--      generation = 今の世代 (updating でも返す・行なしは null) / source_revision = complete_to と同じ条件
--   ③ core.finance_policy_fingerprint(会社, モール, scope) = policy の全期間 (source・period_from・period_to) の正規の JSON の SHA-256 (#1561 Codex R2 High)
--      🚨 policy を後から変えた (起点を広げる・狭める・source を変える) ら前の complete は流用しない = 次の回の updating → complete (新しい指紋) でやり直す
--   ④ mart.finance_daily_sku_range (0047) を create or replace: 返品の状態の partial の判定を「今日」基準から **coverage の complete_to** 基準に (§3.2・#1561 Codex R2 Medium)
--      = 計上日の月の最後の日 > その行の source の complete_to (または null) なら estimated_partial_month_unit_price。0049 の mart の再判定と同じ規則 (矛盾しない)
--
-- 状態の移り方 (§3.1。apps/company-db/ingest/finance-coverage.mjs が 1 取引・財務の chunk と同じ advisory lock の中で行う):
--   ・古い世代 → stale (何もしない)
--   ・新しい世代は updating からだけ (新しい世代の直接の complete = 409)。新しい世代の updating は前の complete を無効にする (manifest の列を空にする)
--   ・同じ世代・同じ token の updating の再送 = same / 違う token = 409 / 同じ世代の complete → updating = 409
--   ・同じ世代・同じ token の updating → complete は 1 回だけ (受領記録から receipt digest を計算し、manifest と合わなければ 409)
--   ・同じ世代の complete の再送 = request_hash が同じなら same・違えば 409
--   ・🚨 財務の chunk が受領記録を変えたら、その会社 × モール × scope の complete の行を全部 updating に落とす (invalidated_at = 印。同じ世代では complete に戻れない = 次の世代から)。
--     token の無い chunk (今の送り手) = 全部の source / token 付きの chunk = ほかの source (か別の世代) の complete (受領記録と receipt digest は source で分かれていない。#1561 Codex R1 High 1)
--   ・complete の manifest は evidence_chain_from ≤ policy の起点 (UTC の瞬間) でなければ受けない (#1561 Codex R1 High 2。鎖の窓の連続は coordinator = D7b-1b-3)
--
-- 🚨 manifest の値の多く (一覧・初期の印・採った文書・期待の report の集合) は Render では確かめられない (miniPC の SQLite にしか無い) = 送り手の申告を保存するだけ。
--    Render が確かめるのは receipt digest (受領記録の今の集合) と、日付・時刻・数・digest の形と、complete_to = settlements_through の JST の日の前日。
-- 🚨 既存の表は変えない。関数は core.finance_coverage_state と mart.finance_daily_sku_range (同じ引数・同じ戻りの形) だけ差し替える。個人情報なし。

create table core.finance_coverage (
  company_id               smallint not null references core.companies,
  mall                     text not null check (mall in ('amazon','rakuten','yahoo','aupay','qoo10','linegift','mercari')),
  scope_key                text not null,
  source                   text not null check (source in ('amazon_settlement_flat_v1', 'amazon_settlement_flat_v2', 'amazon_finances_api', 'amazon_settlement_unified', 'mall_finance_daily_v1')),
  state                    text not null check (state in ('updating', 'complete')),
  generation               bigint not null check (generation > 0),                        -- coverage 専用の連番 (送り手の台帳・財務の batch_seq とは別)
  run_token                text not null check (run_token ~ '^[0-9A-Za-z._:-]{16,100}$'),  -- その回の token (監査。digest には入れない)
  updating_at              timestamptz not null,                                           -- この世代の updating を受けた時刻 (サーバー)
  -- ─── complete の manifest (送り手の申告。complete のときは全部必須 = 下の CHECK) ───
  complete_to              date,                                                           -- settlements_through の JST の日の前日
  settlements_through      timestamptz,                                                    -- 起点から途切れずにつながる最後の end (実時刻)
  source_revision          bigint check (source_revision >= 0),                           -- SQLite の決済の生の表の版 (読み取りの時点)
  headers_count            integer check (headers_count >= 1),
  headers_checksum         text check (headers_checksum ~ '^[0-9a-f]{64}$'),
  receipt_count            integer check (receipt_count >= 0),
  receipt_lines            bigint check (receipt_lines >= 0),
  receipt_digest           text check (receipt_digest ~ '^[0-9a-f]{64}$'),
  inventory_snapshot_id    text check (inventory_snapshot_id ~ '^[0-9A-Za-z._:-]{1,80}$'),
  inventory_count          integer check (inventory_count >= 0),
  inventory_digest         text check (inventory_digest ~ '^[0-9a-f]{64}$'),
  inventory_completed_at   timestamptz,
  initial_marker_id        text check (initial_marker_id ~ '^[0-9A-Za-z._:-]{1,80}$'),
  initial_marker_digest    text check (initial_marker_digest ~ '^[0-9a-f]{64}$'),
  selected_documents_count integer check (selected_documents_count >= 1),
  selected_documents_digest text check (selected_documents_digest ~ '^[0-9a-f]{64}$'),
  evidence_chain_from      timestamptz,
  evidence_chain_through   timestamptz,
  expected_report_count    integer check (expected_report_count >= 0),
  expected_report_digest   text check (expected_report_digest ~ '^[0-9a-f]{64}$'),
  inventory_runs_digest    text check (inventory_runs_digest ~ '^[0-9a-f]{64}$'),
  policy_fingerprint       text check (policy_fingerprint ~ '^[0-9a-f]{64}$'),            -- 検査した policy の全期間の指紋 (core.finance_policy_fingerprint)。今の policy と違えば complete_to は null と読む
  request_hash             text check (request_hash ~ '^[0-9a-f]{64}$'),                  -- complete の要求の正規の hash (サーバーの作る列は入れない)
  completed_at             timestamptz,
  -- ─── 無効にした印 (complete の後に token の無い書き込みが受領記録を変えた) ───
  invalidated_at           timestamptz,
  invalidated_reason       text check (invalidated_reason in ('untokened_finance_write', 'other_coverage_finance_write')),   -- token の無い chunk / ほかの coverage の token の chunk
  updated_at               timestamptz not null default now(),
  primary key (company_id, mall, scope_key, source),
  -- complete = manifest が全部そろう (1 つでも欠けた complete を作らない)・無効の印は無い
  constraint ck_finance_coverage_complete check (state <> 'complete' or (
        complete_to is not null and settlements_through is not null and source_revision is not null
    and headers_count is not null and headers_checksum is not null
    and receipt_count is not null and receipt_lines is not null and receipt_digest is not null
    and inventory_snapshot_id is not null and inventory_count is not null and inventory_digest is not null and inventory_completed_at is not null
    and initial_marker_id is not null and initial_marker_digest is not null
    and selected_documents_count is not null and selected_documents_digest is not null
    and evidence_chain_from is not null and evidence_chain_through is not null
    and expected_report_count is not null and expected_report_digest is not null and inventory_runs_digest is not null and policy_fingerprint is not null
    and request_hash is not null and completed_at is not null
    and invalidated_at is null and invalidated_reason is null)),
  -- complete_to = settlements_through の JST の日の前日 (§3.1: end の日は途中)
  constraint ck_finance_coverage_complete_to check (complete_to is null or settlements_through is null
    or complete_to = (settlements_through at time zone 'Asia/Tokyo')::date - 1),
  constraint ck_finance_coverage_receipts check (receipt_count is null or receipt_lines is null or (receipt_lines >= receipt_count and (receipt_count = 0) = (receipt_lines = 0))),
  constraint ck_finance_coverage_evidence check (evidence_chain_from is null or evidence_chain_through is null or evidence_chain_from <= evidence_chain_through),
  constraint ck_finance_coverage_invalidated check ((invalidated_at is null) = (invalidated_reason is null))
);
comment on table core.finance_coverage is '決済のそろい (0051・D7b-1b-2。13 §3.1)。会社 × モール × scope × source = 1 行 = 今の世代の状態。complete のときだけ complete_to が正式 (core.finance_coverage_state)。書くのは受け口 (ingest/finance-coverage.mjs) だけ = 財務の chunk と同じ advisory lock の中';
comment on column core.finance_coverage.invalidated_at is 'complete の後に財務の chunk (token の無い今の送り手・ほかの coverage の token の chunk) が受領記録を変えて updating に落とした時刻。この世代では complete に戻れない (次の世代の updating で消える)';
create trigger trg_finance_coverage_touch before update on core.finance_coverage for each row execute function core.touch_updated_at();

-- ─── policy の全期間の指紋 (JS の finance/coverage-manifest.mjs policyFingerprint と同じ値・試験で固定) ───
--   正規の JSON {"format":"fpf-v1","policies":[{"period_from":"YYYY-MM-DD","period_to":"YYYY-MM-DD"|null,"source":"…"}, …]} (period_from → source の順。値は ASCII) の SHA-256 (16 進)
create or replace function core.finance_policy_fingerprint(p_company_id smallint, p_mall text, p_scope_key text) returns text language sql stable as $$
  select encode(sha256(convert_to('{"format":"fpf-v1","policies":['
           || coalesce(string_agg('{"period_from":' || to_json(p.period_from::text)::text
                                  || ',"period_to":' || coalesce(to_json(p.period_to::text)::text, 'null')
                                  || ',"source":' || to_json(p.source)::text || '}', ',' order by p.period_from, p.source collate "C"), '')
           || ']}', 'UTF8')), 'hex')
    from core.finance_source_policy p
   where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key
$$;
comment on function core.finance_policy_fingerprint(smallint, text, text) is '会社 × モール × scope の finance_source_policy の全期間の指紋 (0051)。coverage の complete はこの値と一緒に保存し、今の値と違えば core.finance_coverage_state は complete_to を返さない';

-- ─── core.finance_coverage_state を差し替える (0049 の差し込み口。名前・引数・戻りの形は同じ・呼び手は変えない) ───
--   戻り = いつも 1 行。complete_to / source_revision = state = complete **かつ policy の指紋が今と同じ** のときだけ
--   (updating・行なし・policy が complete の後に変わった = null = 正式な利益は null。#1561 Codex R2 High) / generation = 今の世代 (行なしは null)
create or replace function core.finance_coverage_state(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
returns table (complete_to date, generation bigint, source_revision bigint) language plpgsql stable as $$
declare
  v_fp text := core.finance_policy_fingerprint(p_company_id, p_mall, p_scope_key);
begin
  return query
    select case when c.state = 'complete' and c.policy_fingerprint = v_fp then c.complete_to end, c.generation,
           case when c.state = 'complete' and c.policy_fingerprint = v_fp then c.source_revision end
      from core.finance_coverage c
     where c.company_id = p_company_id and c.mall = p_mall and c.scope_key = p_scope_key and c.source = p_source;
  if not found then
    return query select null::date, null::bigint, null::bigint;
  end if;
end
$$;
comment on function core.finance_coverage_state(smallint, text, text, text) is '決済のそろい (0049 の差し込み口を 0051 で差し替え)。1 行 = complete_to (complete かつ policy の指紋が今と同じときだけ)・generation (今の世代)・source_revision (complete_to と同じ条件)。core.finance_coverage が無い key は全部 null。Amazon の利益の mart (0049) と mart.finance_daily_sku_range の返品の状態がこれを読む';

-- ─── mart.finance_daily_sku_range (0047) を差し替える: 返品の状態の partial = coverage 基準 (§3.2・#1561 Codex R2 Medium) ───
--   0047 との違いは 2 か所だけ: ① 当面の「今日 (JST) 基準」をやめる ② 行の source ごとに core.finance_coverage_state の complete_to を読み、
--   計上日の月の最後の日 > complete_to (または complete_to が null・0 行 / 2 行以上) なら estimated_partial_month_unit_price (単価は月の全部の日の平均 = 月末まで決済がそろうまで動く)。
--   0049 の mart は子が monthly でも同じ条件で partial に上げている = 同じ規則 (0049 は変えない)。引数・戻りの形・金額の式は 0047 のまま
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
  -- 🆕 決済のそろい (source ごとに 1 回。ちょうど 1 行のときだけ使う = 0049 と同じ fail-closed)
  cov as materialized (
    select s.source,
           (select case when count(*) = 1 then min(x.complete_to) end from core.finance_coverage_state(p_company_id, p_mall, p_scope_key, s.source) x) as complete_to
      from (select distinct a.source from adopted a) s
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
  -- 返品数の推定 = 0045 と同じ「計上日の月 × 受け取った seller SKU の単価」(子ごとに計算してから正規化 SKU にまとめる)。
  --   source も持つ (policy は期間が重ならない = 同じ日の行の source は 1 つ = まとめ方は 0047 と同じ)
  raw_daily as (
    select economic_date_jst, seller_sku, sku_norm, month, source,
           -sum(refund_principal_customer_jpy) as refund_customer_jpy, -sum(refund_principal_atoz_jpy) as refund_atoz_jpy
      from adopted
     where economic_date_jst between p_from and p_to
     group by economic_date_jst, seller_sku, sku_norm, month, source
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
           --   🆕 partial = 計上日の月の最後の日 > その source の complete_to (または null) = 月末まで決済がそろっていない (0049 の再判定と同じ条件)
           case when d.refund_customer_jpy = 0 and d.refund_atoz_jpy = 0 then 1
                when coalesce(u.unit_price_micro, 0) = 0 then 4
                when cv.complete_to is null or (d.month + interval '1 month' - interval '1 day')::date > cv.complete_to then 3
                else 2 end as status_rank,
           case when (d.refund_customer_jpy <> 0 or d.refund_atoz_jpy <> 0) and coalesce(u.unit_price_micro, 0) = 0
                then d.refund_customer_jpy + d.refund_atoz_jpy else 0 end as unestimated_jpy
      from raw_daily d
      left join unit_price_month u on u.seller_sku = d.seller_sku and u.month = d.month
      left join cov cv on cv.source = d.source
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

-- ─── 権限 (0049 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.finance_coverage to watcher';
    execute 'grant execute on function core.finance_coverage_state(smallint, text, text, text), core.finance_policy_fingerprint(smallint, text, text), mart.finance_daily_sku_range(smallint, text, text, date, date) to watcher';
  end if;
end $$;
