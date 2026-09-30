-- 0050 決済のそろい core.finance_coverage (D7b-1b-2 = Render 側。2026-09-30)
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
--   ④ core.finance_month_settled(会社, モール, scope, 月) = その月の **全部の日** で policy がちょうど 1 つ・その日の source の coverage の complete_to ≥ その日 (#1561 Codex R3 High 1)
--   ⑤ mart.finance_daily_sku_range (0047) を create or replace: 返品の状態の partial の判定を「今日」基準から ④ に (§3.2・#1561 Codex R2 Medium・R3 High 1)
--   ⑥ mart._amazon_profit_rows (0049・本番に入っている) を create or replace: 返品の状態の再判定も ④ に (返品の日の source の complete_to だけを見ていた = 月の途中の source の切り替えで誤る)
--   ⑦ core.finance_policy_snapshot(会社, モール, scope, source) = policy の起点・source の有無・指紋を **1 つの文 (同じスナップショット)** で返す (complete の受け口が使う・#1561 Codex R3 High 2)
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
-- 🚨 既存の表は変えない。既存の関数は core.finance_coverage_state・mart.finance_daily_sku_range・mart._amazon_profit_rows (同じ引数・同じ戻りの形) だけ差し替える。個人情報なし。

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
comment on table core.finance_coverage is '決済のそろい (0050・D7b-1b-2。13 §3.1)。会社 × モール × scope × source = 1 行 = 今の世代の状態。complete のときだけ complete_to が正式 (core.finance_coverage_state)。書くのは受け口 (ingest/finance-coverage.mjs) だけ = 財務の chunk と同じ advisory lock の中';
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
comment on function core.finance_policy_fingerprint(smallint, text, text) is '会社 × モール × scope の finance_source_policy の全期間の指紋 (0050)。coverage の complete はこの値と一緒に保存し、今の値と違えば core.finance_coverage_state は complete_to を返さない';

-- ─── policy のスナップショット (complete の受け口が使う・#1561 Codex R3 High 2) ───
--   その source の policy の数・起点 (最初の period_from)・会社 × モール × scope の全期間の指紋を **1 つの文** で返す = 同じスナップショット
--   (STABLE の関数は呼んだ文のスナップショットで読む = 指紋の関数も同じ時点)。前は起点と指紋を READ COMMITTED の別々の文で読んでいた =
--   間に policy が変わると、古い狭い起点で検査して新しい広い指紋を保存できた。受け口はこの前に policy の trigger (0012) と同じ advisory lock も取る
create or replace function core.finance_policy_snapshot(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
returns table (source_policy_count integer, origin_from date, fingerprint text) language sql stable as $$
  select (select count(*)::int from core.finance_source_policy p where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key and p.source = p_source),
         (select min(p.period_from) from core.finance_source_policy p where p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key and p.source = p_source),
         core.finance_policy_fingerprint(p_company_id, p_mall, p_scope_key)
$$;
comment on function core.finance_policy_snapshot(smallint, text, text, text) is 'coverage の complete の受け口が読む policy の 1 つの時点 (0050): その source の policy の数・起点・全期間の指紋 (1 つの文 = 同じスナップショット)';

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
comment on function core.finance_coverage_state(smallint, text, text, text) is '決済のそろい (0049 の差し込み口を 0050 で差し替え)。1 行 = complete_to (complete かつ policy の指紋が今と同じときだけ)・generation (今の世代)・source_revision (complete_to と同じ条件)。core.finance_coverage が無い key は全部 null。Amazon の利益の mart (0049) と mart.finance_daily_sku_range の返品の状態がこれを読む';

-- ─── 月の決済がそろったか (返品の単価 = 月 × SKU の全部の日の平均が確定したか。#1561 Codex R2 Medium・R3 High 1) ───
--   true = その月の **全部の日** で、policy がちょうど 1 つ・その日の source の coverage (core.finance_coverage_state) の complete_to ≥ その日。
--   1 日でも満たさなければ false (policy が 0 / 2 つ以上の日・coverage が無い / 0 行 / 2 行以上・complete_to がその日より前)。
--   🚨 月の途中で policy の source が A → B に切り替わると、単価は両方の source の行から作られる = 返品の日の source (B) だけ見ると A が未完了でも確定と誤る (R3 High 1)
create or replace function core.finance_month_settled(p_company_id smallint, p_mall text, p_scope_key text, p_month date) returns boolean language sql stable as $$
  with d as (
    select g::date as day
      from generate_series(date_trunc('month', p_month)::timestamp, (date_trunc('month', p_month) + interval '1 month - 1 day')::timestamp, interval '1 day') g
  ),
  pol as (
    select d.day, count(p.policy_id)::int as n, min(p.source) as source
      from d left join core.finance_source_policy p
        on p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key
       and d.day >= p.period_from and (p.period_to is null or d.day < p.period_to)
     group by d.day
  ),
  cov as (
    select s.source, (select case when count(*) = 1 then min(x.complete_to) end from core.finance_coverage_state(p_company_id, p_mall, p_scope_key, s.source) x) as complete_to
      from (select distinct pol.source from pol where pol.n = 1) s
  )
  -- bool_and は null を飛ばす = 条件の null は false に直してから (fail-closed)
  select coalesce(bool_and(coalesce(pol.n = 1 and cov.complete_to is not null and pol.day <= cov.complete_to, false)), false)
    from pol left join cov on cov.source = pol.source
$$;
comment on function core.finance_month_settled(smallint, text, text, date) is '月の決済がそろったか (0050)。その月の全部の日で policy がちょうど 1 つ・その日の source の coverage の complete_to ≥ その日のときだけ true。返品の単価 (月 × SKU) が確定したかの判定 = mart.finance_daily_sku_range と Amazon の利益の mart の estimated_partial_month_unit_price';

-- ─── mart.finance_daily_sku_range (0047) を差し替える: 返品の状態の partial = coverage 基準 (§3.2・#1561 Codex R2 Medium・R3 High 1) ───
--   0047 との違いは 2 か所だけ: ① 当面の「今日 (JST) 基準」をやめる ② 計上日の月が core.finance_month_settled (月の全部の日がそろった) でなければ
--   estimated_partial_month_unit_price (単価は月の全部の日の平均 = 月末まで決済がそろうまで動く)。
--   0049 の mart の再判定も同じ関数に直した (下の mart._amazon_profit_rows)。引数・戻りの形・金額の式は 0047 のまま
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
  -- 🆕 月の決済がそろったか (月ごとに 1 回・その月の全部の日 = core.finance_month_settled)
  mset as materialized (
    select m.month, core.finance_month_settled(p_company_id, p_mall, p_scope_key, m.month) as settled
      from (select distinct raw_daily.month from raw_daily) m
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
           --   🆕 partial = 計上日の月の全部の日の決済がそろっていない (core.finance_month_settled・0049 の再判定と同じ関数)
           case when d.refund_customer_jpy = 0 and d.refund_atoz_jpy = 0 then 1
                when coalesce(u.unit_price_micro, 0) = 0 then 4
                when not coalesce(ms.settled, false) then 3
                else 2 end as status_rank,
           case when (d.refund_customer_jpy <> 0 or d.refund_atoz_jpy <> 0) and coalesce(u.unit_price_micro, 0) = 0
                then d.refund_customer_jpy + d.refund_atoz_jpy else 0 end as unestimated_jpy
      from raw_daily d
      left join unit_price_month u on u.seller_sku = d.seller_sku and u.month = d.month
      left join mset ms on ms.month = d.month
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

-- ─── 0049 の mart._amazon_profit_rows を差し替える (本番に入っている関数。#1561 Codex R3 High 1) ───
--   0049 との違いは返品の状態の再判定の 1 か所だけ: 子 (上の mart.finance_daily_sku_range) が monthly でも、その月が core.finance_month_settled でなければ partial
--   (0049 は「返品の日の source の complete_to ≥ 月末」だった = 月の途中で source が切り替わると、もう片方の source が未完了でも確定と読んだ)。
--   ほかの本文・引数・戻りの形は 0049 のまま (0049 のファイルから写した)。呼び手 (mart.amazon_profit_daily_range / _amazon_profit_totals) は変えない
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
language sql stable as $$
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
     and f.economic_date_jst between p_from and p_to and not core.finance_version_has_class(f.transform_version)
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
-- 🆕 0050: 月ごとに 1 回「その月の全部の日で policy がちょうど 1 つ・その日の source の coverage がその日まで complete」(#1561 Codex R3 High 1)
mset as materialized (
  select m.month, core.finance_month_settled(p_company_id, p_mall, p_scope_key, m.month) as settled
    from (select distinct date_trunc('month', keys.day)::date as month from keys) m
),
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
uc as (
  select cc.day, cc.lid, bool_and(cc.known) as all_known,
         sum(cc.qty::bigint * cc.cost_jpy) filter (where cc.known) as known_sum,
         max(cc.basis_rank) as basis_rank,
         coalesce(array_agg(cc.sku_id order by cc.sku_id) filter (where not cc.known), '{}') as missing_ids,
         coalesce(array_agg(cc.row_id order by cc.row_id) filter (where cc.known and cc.src = 'sku_costs'), '{}') as sc_ids,
         coalesce(array_agg(cc.row_id order by cc.row_id) filter (where cc.known and cc.src = 'observed'), '{}') as ob_ids,
         case when bool_and(cc.known) then
           encode(sha256(convert_to('[' || string_agg('{"cost_jpy":' || cc.cost_jpy || ',"cost_status":' || to_json(cc.cost_status)::text
             || ',"row_id":"' || cc.row_id || '","sku_id":"' || cc.sku_id || '","source":"' || cc.src || '"}', ',' order by cc.sku_id, cc.src collate "C") || ']', 'UTF8')), 'hex')
         end as cost_input_hash
    from cc group by cc.day, cc.lid
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
r0 as (
  select k.day, k.lid, k.unorm, l.listing_code,
         f.received_listing_ids, f.received_listing_unresolved_count,
         coalesce(f.units_ordered, 0) as units_ordered, coalesce(f.units_refunded_customer, 0) as units_refunded_customer,
         coalesce(f.units_a_to_z_refund, 0) as units_a_to_z_refund, coalesce(f.units_net_sold, 0) as units_net_sold,
         coalesce(f.units_marketplace_guarantee, 0) as units_marketplace_guarantee,
         -- 丸める前の返品数 (子の値。unit_price_missing の子は null のまま・子の無い行は 0)
         case when f.economic_date_jst is null then 0::numeric else f.units_refunded_customer_unrounded end as units_refunded_customer_unrounded,
         case when f.economic_date_jst is null then 0::numeric else f.units_a_to_z_refund_unrounded end as units_a_to_z_refund_unrounded,
         coalesce(ad_rcv.ids, '{}'::bigint[]) as ad_received_ids, coalesce(ad_l.rcv_unres, 0) as ad_received_unres,
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
              when not coalesce(ms.settled, false) then 3   -- 🆕 0050: 返品の日の source だけでなく、その月の全部の日 (単価は月の全部の日の平均)
              else 2 end as refund_rank,
         coalesce(f.refund_unestimated_jpy, 0) as refund_unestimated_jpy,
         d.day_finance_status, d.coverage_generation, d.source_revision, ad.ad_status, ad_l.cost as ad_linked, coalesce(ad_l.n, 0) as ad_n, coalesce(ad_u.n, 0) as ad_unres_n,
         coalesce(ek.cost, 0) as es_cost,
         comp_l.n_comp, comp_l.composition_hash,
         uc.all_known, uc.known_sum, uc.basis_rank, uc.missing_ids, uc.sc_ids, uc.ob_ids, uc.cost_input_hash,
         aud.through,
         ((k.day - 1)::timestamp at time zone 'Asia/Tokyo') as audit_from   -- その日の前日の JST 00:00
    from keys k
    join days d on d.economic_date_jst = k.day
    join ad_days ad on ad.date_jst = k.day
    left join mset ms on ms.month = date_trunc('month', k.day)::date
    left join fk f on f.economic_date_jst = k.day and f.rk = k.rk
    left join ek on ek.day = k.day and ek.rk = k.rk
    left join ad_l on ad_l.day = k.day and ad_l.lid = k.lid
    left join ad_rcv on ad_rcv.day = k.day and ad_rcv.lid = k.lid
    left join ad_u on ad_u.day = k.day
    left join comp_l on comp_l.lid = k.lid
    left join uc on uc.day = k.day and uc.lid = k.lid
    left join aud on aud.lid = k.lid
    left join core.listings l on l.listing_id = k.lid
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
         r0.lid is not null and r0.audit_from < mart.amazon_profit_composition_audit_since() as n_pre,
         r0.lid is not null and r0.through is not null and r0.through >= r0.audit_from as n_after,
         -- 受け取り時の出品 (財務と広告の保存済みの listing_id) を **集合で** 比べる (#1559 Codex R1 Medium 2):
         --   出品の行 = 受け取りの記録があり、「今の出品 1 つだけ・受け取り時の未解決 0」と一致しない / 未解決の行 = 受け取り時に出品が決まっていた
         case when r0.lid is not null
              then (cardinality(r0.rcv_ids) > 0 or r0.rcv_unres > 0) and (r0.rcv_ids <> array[r0.lid] or r0.rcv_unres > 0)
              else cardinality(r0.rcv_ids) > 0 end as n_changed
    from (select r.*,
                 (select coalesce(array_agg(distinct x order by x), '{}'::bigint[])
                    from unnest(coalesce(r.received_listing_ids, '{}'::bigint[]) || r.ad_received_ids) x) as rcv_ids,
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
 order by r3.day, r3.lid is null, r3.lid, r3.unorm collate "C"
$$;

-- ─── 権限 (0049 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.finance_coverage to watcher';
    execute 'grant execute on function core.finance_coverage_state(smallint, text, text, text), core.finance_policy_fingerprint(smallint, text, text), core.finance_policy_snapshot(smallint, text, text, text),'
         || ' core.finance_month_settled(smallint, text, text, date), mart.finance_daily_sku_range(smallint, text, text, date, date) to watcher';
  end if;
end $$;
