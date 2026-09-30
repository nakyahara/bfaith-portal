-- 0049 Amazon の利益の mart (D7b-3。2026-09-30)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 (D-55〜D-66・§3.3・§3.5・§3.5b・§3.6・§3.7・§4 の D7b-3)。
--   日 × Amazon の出品 (listing) の「寄与の利益」(月の手数料を引く前) と、日 / 月 / 期間の合計 (月の手数料を引いた後も) を **関数で都度** 計算する (D-60)。
--   🚨 分からないもの (決済のそろい・原価・広告費・返品数) を 0 として利益を確定しない = 正式な列は null + 理由のコード。0 と仮定した値は別の名前 (…_assuming_incomplete_zero_…)。
--
-- 作るもの:
--   ① core.finance_coverage_complete_to(会社, モール, scope, source) = 決済がそろった最後の日。🚨 **今は常に null を返す** (決済のそろい = core.finance_coverage は D7b-1b・後の PR)。
--      → 今は全部の日が day_finance_status = missing / provisional = **正式な利益は全部 null** (0 と仮定の値は出る)。
--      D7b-1b がこの関数を **差し替える** (同じ名前・同じ引数・戻り値 date。coverage が complete のときだけ complete_to を返す)。呼び手 (この mart) は変えない
--   ② mart.amazon_profit_composition_audit_since() = この mart を有効にした時刻 (0049 の適用の時刻を埋め込む・D-62 の composition_audit_since。0026 の適用時刻ではない)
--   ③ mart.amazon_account_fee_tax_rate(line_kind) = 月の手数料の税の表 (Amazon の決済の手数料は全部税込 = 8 種類とも 10%・#1517 と同じ。§3.6)
--   ④ mart.amazon_profit_daily_range(会社, モール, scope, from, to)      = 日 × 出品の行 (§3.5・§3.5b・§3.7)
--   ⑤ mart.amazon_profit_day_totals_range(会社, モール, scope, from, to) = 日 / 暦の月 / 期間の中の月の小計 / 期間の合計 (§3.6)
--   内部の部品 (mart._amazon_…): 日の決済の状態・広告の日の状態・広告の子の結び直し・Easy Ship の割り振り・行と合計の本体 (SQL の関数 = 出力の列の名前と表の列の名前がぶつからない)
--
-- 決め (設計のとおり):
--   ・D-64 = **構成と出品の結びつけは計算のときの今のマスタ** (master_basis = 'current')。財務は正規化 seller SKU の子 (mart.finance_daily_sku_range・0047) を
--     今の core.listings の listing_norm の **直接の一致** (0043 の受け口と同じ規則 = 会社 × モールで 1 件のときだけ) で出品に結び直す。
--     広告は target_granularity = 'sku' の行だけ core.resolve_listing_id (今の取込と同じ = external_ids の別名も含む)・asin / none は常に未解決 (R14 H2)。
--     保存済みの listing_id は診断 (received_listing_ids) だけ。受け取ったときと今の結び直しが違えば master_notes に listing_changed_since_received
--   ・行の鍵 = listing_id / seller_sku_norm / listing_resolution (resolved の行は seller_sku_norm が null・unresolved の行は listing_id が null)。
--     出品に結びつかない広告の行は行を作らない (日の合計の「配れない広告費」の内訳だけ)
--   ・原価 = 構成 (core.listing_components の今の行) × SKU ごとに **その日を覆う 1 行** (§3.3): core.sku_costs と観測の原価 (mart.v_sku_cost_observed_effective) の中から
--     valid_from DESC, created_at DESC, 行の ID DESC の最初 (同じ日に 2 回変わった取込の行を二重に数えない)。採る状態 = COMPLETE / OVERRIDDEN だけ (override_zero の 0 円は正しい 0)。
--     PARTIAL / MISSING / 行が無い = 原価不明 (cost_missing・missing_cost_sku_ids)。cost_basis = 構成の中の最も弱いもの (missing > estimated > observed > sku_costs)
--   ・金額の式 = §3.5b (固定):
--       cogs_jpy                  = units_net_sold × Σ(qty × その日の原価)   (原価が 1 つでも不明なら null・units_net_sold = 0 でも null)
--       contribution_before_ad_incl_jpy = profit_before_cogs_jpy (0047 の式 = closing_fee も引く) − cogs_jpy
--       contribution_before_ad_excl     = 上 + taxable_sku_fee_cost_jpy / 11 + promotion_tax_jpy   (課税の 6 手数料 = commission・fba_fulfillment・fba_storage・closing_fee・chargeback 2 つ)
--       contribution_after_ad_incl      = contribution_before_ad_incl − ad_cost × 1.1   (✅ 広告費は税抜 = D-61・9/30 決定)
--       contribution_after_ad_excl      = contribution_before_ad_excl − ad_cost
--     税抜・広告・0 と仮定・合計の値は numeric を **途中で丸めず**、返すときだけ小数 2 桁 (列の名前に _jpy を付けない)
--   ・正式な値の条件 (ゲート) と理由のコード = §3.7 の固定の順:
--       finance_incomplete (日が complete でない) → finance_unclassified (分けられない部品の数 > 0 = unclassified_component_count + unmapped_component_count。
--       🆕 + 旧い形の版の行 (4 列を持たない = 数を確かめられない) も数える = finance_legacy_rows) → refund_units_unknown (unit_price_missing) →
--       refund_units_partial_month (estimated_partial_month_unit_price。🆕 子が monthly でも、月末まで決済がそろっていない (月末 > complete_to か complete_to が null) なら partial) →
--       listing_unresolved → composition_missing → cost_missing → ad_not_collected → ad_missing → ad_legacy_unverified → ad_unresolved
--       寄与 (before ad) = 前の 7 つのどれかで null / 広告の後 (after ad) = 11 のどれかで null。
--       🚨 ad_unresolved = その日 × 広告の種類に出品に結びつかない広告の行が 1 つでもある = **出品の行の** 広告の後だけ null (日 / 月 / 期間の合計は ad_spend_days の全額を引くので止めない・R24 M4)
--   ・0 と仮定した値 (…_assuming_incomplete_zero_…) と assumed_zero_reasons = 上の理由のうち refund_units_partial_month を除くもの
--       (原価不明 = 0 / 構成なし・出品未解決 = 原価 0 / 返品数不明 = 0 個 / 広告 not_collected・missing = 0・legacy = 記録済みの額・ad_unresolved = その出品に結びついた額だけ /
--        未確定の日 = 今ある行だけ / 分けられない部品 = 金額を足さない)。partial は推定の返品数を使う (0 と置いていない) = 理由に入れない
--   ・master_notes (情報の印・正式な値を止めない・固定の順): pre_audit_unverifiable (その日の前日の JST 00:00 が composition_audit_since より前) /
--     current_after_recorded_change (その日の前日の JST 00:00 以降に、その出品の構成 (listing_components の全部の変更) か出品の識別 (listings の INSERT・DELETE・mall・shop_code・listing_code) の監査の記録がある) /
--     listing_changed_since_received。composition_basis = listing_unresolved → missing → pre_audit_unverifiable → current_after_recorded_change → current_no_recorded_change (前の 2 つだけがゲート)
--   ・hash = 正規の JSON の SHA-256 (apps/company-db/canonical-hash.mjs と同じ規則: 鍵の順は固定・ID (bigint) は 10 進の文字列・円と個数は整数)。
--       composition_hash = {"components":[{"qty":n,"sku_id":"…"}] (sku_id の順),"listing_id":"…"} (listing_unresolved / missing のときは null)
--       cost_input_hash  = [{"cost_jpy":n,"cost_status":"…","row_id":"…","sku_id":"…","source":"sku_costs"|"observed"}] (sku_id → source の順。cost_missing・unresolved・missing のときは null)
--     鍵は固定の ASCII の名前だけ = 文字列の順とバイトの順が同じ。JS の canonicalSha256 と一致することを試験で固定 (scripts/test-company-db-amazon-profit.mjs)
--   ・Easy Ship (D-59) = 注文 × 計上日で料金と返金を正味 (−Σ account_fee_amount_jpy = 費用を正) にしてから、同じ注文の SKU の本体売上の割合で 1 円単位に割り振る
--     (合計が 0 以下なら売上の行のある SKU で等分・端数は小数部の大きい順 → 同じなら正規化 SKU のバイトの順・正味が負なら絶対値で配って符号を戻す・正味 0 は配らない)。
--     🚨 **期間に依らない** (R15 M1): 期間の中に料金がある注文の本体売上は **期間の外の日も含む全部の日** (日ごとに policy が採った source の行だけ) から取る。
--     「売上の行」= その注文の SKU の行で units_ordered・sales_principal・sales_shipping・sales_giftwrap のどれかが 0 でない (= 決済の Order の行から来た値)。
--     行の easy_ship_alloc_jpy は内訳 (寄与から引かない)・日の合計は月の手数料 (easy_ship) で引く = 二重に引かない。売上の行が無くて配らない額は日の合計の easy_ship_unallocated_jpy と件数
--   ・日の合計 (§3.6): row_kind = day / calendar_month / range_month_subtotal / range_total (重ならない)。日の行は取引の無い日も 1 行。
--     列の組ごとの正式な値の条件: before ad = その日が complete かつ全部の行の寄与が正式 / after ad = + 広告の日の状態が complete (か verified_legacy) = 広告費は ad_spend_days.cost_total の全額 /
--     after account fees = + 全部の line_kind の分けられない部品の数・unmapped の部品の数・unknown の行・旧い形の行が 0。期間の合計は 1 日でも正式でない日があれば null・列の組ごとに不完全な日
--     月の手数料 = account_fee_cost_jpy = −Σ account_fee_amount_jpy (費用を正)・税抜 = 種類ごとに ÷ (1 + 税率)
--     分けられない金額 (3 区分・重ならない): unknown_line_mapped_jpy (unknown の行の net − unmapped) / unclassified_mapped_jpy (SKU の行の misc_fee + other_fee + other_amount と
--     月の手数料の行の net − account_fee_amount − unmapped = 4 列の符号つきの額と同じ・旧い形の行でも同じ式) / unmapped_jpy (全部の行)。
--     保存則 = net_jpy = profit_before_cogs_jpy + sales_tax_jpy − account_fee_cost_jpy + unknown_line_mapped_jpy + unclassified_mapped_jpy + unmapped_jpy + not_account_fee_mapped_jpy
--   ・契約: from <= to・最大 400 日 (両端を含む = to − from <= 399)・今は amazon / jp だけ (受け取り時の出品の決め方が shop_code を見ない = Amazon のアカウントが 1 つの間だけ正しい・§3.5)。
--     違えば例外 (22023 invalid_input)。行の関数はその期間に財務・広告・Easy Ship のどの行も無ければ 0 行・日の合計の関数は日の行を必ず返す
--   ・calculated_at = statement_timestamp() (1 回の呼び出しの全部の行・合計で同じ値)。finance_coverage_generation / finance_source_revision は D7b-1b まで null
-- 🚨 表は作らない (関数だけ)・既存の表・関数には触らない (0043〜0048 の関数はそのまま呼ぶ)。

-- ─── ① 決済のそろい (D7b-1b が差し替える) ───
create or replace function core.finance_coverage_complete_to(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
returns date language plpgsql stable as $$
begin
  -- 🚨 今は常に null = 決済がそろったと言える日が無い = 全部の日の正式な利益は null (0 と仮定の値は出る)。
  --    D7b-1b (core.finance_coverage・coordinator・決済のレポートの一覧) がこの関数を差し替え、その source の coverage が complete のときだけ complete_to を返す
  return null;
end
$$;
comment on function core.finance_coverage_complete_to(smallint, text, text, text) is '決済がそろった最後の日 (0049)。今は常に null (D7b-1b の core.finance_coverage が差し替える)。Amazon の利益の mart (0049) の日の状態 (day_finance_status) がこれを読む';

-- ─── ② composition_audit_since = この mart を有効にした時刻 (0049 の適用の時刻を埋め込む) ───
do $do$ begin
  execute format($f$create or replace function mart.amazon_profit_composition_audit_since() returns timestamptz language sql immutable as $b$ select %L::timestamptz $b$ $f$, now());
end $do$;
comment on function mart.amazon_profit_composition_audit_since() is 'Amazon の利益の mart の composition_audit_since (0049 の適用の時刻)。この時刻より前の日 (前日の JST 00:00 が前) の行は master_notes に pre_audit_unverifiable';

-- ─── ③ 月の手数料の税の表 (Amazon の決済の手数料は全部税込・#1517 と同じ)。表の外の種類は null ───
create or replace function mart.amazon_account_fee_tax_rate(p_line_kind text) returns numeric language sql immutable as $$
  select case when p_line_kind in ('storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee')
              then 0.10::numeric end
$$;

-- ─── 引数の確かめ (公開の 2 つの関数の入口) ───
create or replace function mart.amazon_profit_assert_args(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns void language plpgsql stable as $$
begin
  if p_company_id is null or p_mall is null or p_scope_key is null or p_from is null or p_to is null then
    raise exception 'invalid_input: 会社・モール・scope・from・to は必須' using errcode = '22023';
  end if;
  if p_mall <> 'amazon' or p_scope_key <> 'jp' then
    raise exception 'invalid_input: 今は amazon / jp だけ (% / %。受け取り時の出品の決め方が shop_code を見ない = Amazon のアカウントが 1 つの間だけ正しい)', p_mall, p_scope_key using errcode = '22023';
  end if;
  if p_from > p_to then
    raise exception 'invalid_input: from (%) が to (%) より後', p_from, p_to using errcode = '22023';
  end if;
  if p_to - p_from > 399 then
    raise exception 'invalid_input: 期間は 400 日まで (両端を含む。% 〜 % = % 日)', p_from, p_to, p_to - p_from + 1 using errcode = '22023';
  end if;
end
$$;

-- ─── 日の決済の状態 (§3.1 の最後: 日に当てる coverage = その日を覆う policy の source の coverage。policy が 0 件 / 2 件以上の日は missing) ───
--   day_finance_status: 日 <= complete_to → complete (財務の行が無い日は確定の 0) / それ以外で財務の行 (どの line_kind でも) あり → provisional / 無し → missing
create or replace function mart._amazon_profit_finance_days(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (economic_date_jst date, policy_count integer, policy_source text, complete_to date, has_finance_rows boolean, day_finance_status text)
language sql stable as $$
  with d as (select g::date as day from generate_series(p_from::timestamp, p_to::timestamp, interval '1 day') g),
  pol as (
    select d.day, count(p.policy_id)::int as n, min(p.source) as source
      from d left join core.finance_source_policy p
        on p.company_id = p_company_id and p.mall = p_mall and p.scope_key = p_scope_key
       and d.day >= p.period_from and (p.period_to is null or d.day < p.period_to)
     group by d.day
  ),
  cov as materialized (
    select s.source, core.finance_coverage_complete_to(p_company_id, p_mall, p_scope_key, s.source) as complete_to
      from (select distinct pol.source from pol where pol.n = 1) s
  ),
  fin as (
    select f.economic_date_jst as day
      from core.order_finance_daily f
      join core.finance_source_policy p
        on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
       and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
     where f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key and f.economic_date_jst between p_from and p_to
     group by f.economic_date_jst
  )
  select pol.day, pol.n, case when pol.n = 1 then pol.source end, cov.complete_to, fin.day is not null,
         case when pol.n <> 1 then 'missing'
              when cov.complete_to is not null and pol.day <= cov.complete_to then 'complete'
              when fin.day is not null then 'provisional'
              else 'missing' end
    from pol
    left join cov on pol.n = 1 and cov.source = pol.source
    left join fin on fin.day = pol.day
$$;

-- ─── 広告の日の状態 (§3.5: 親 = 日 × 広告の種類 = core.ad_spend_days から先に決める) ───
--   日 < 2026-02-05 → not_collected / 親が無い → missing (費用 null) / 親の source_report_id が legacy: → legacy_incomplete / それ以外 → complete。
--   広告の種類が 2 つ以上になったら弱い方 (not_collected > missing > legacy_incomplete > verified_legacy (将来) > complete)。必須の種類 = 今は SP だけ
create or replace function mart._amazon_profit_ad_days(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (date_jst date, ad_status text, ad_cost_total numeric)
language sql stable as $$
  with d as (select g::date as day from generate_series(p_from::timestamp, p_to::timestamp, interval '1 day') g),
  req as (select t.ad_type from (values ('SP')) as t(ad_type)),   -- 必須の広告の種類 (種類が増えたらここに足す)
  per as (
    select d.day, a.cost_total,
           case when d.day < date '2026-02-05' then 5            -- 広告費の日次は 2026-02-05 から (0035)
                when a.date_jst is null then 4
                when a.source_report_id like 'legacy:%' then 3   -- 古い取込の日 (0039)。verified_legacy (2) は検証の表を作ってから
                else 1 end as rnk
      from d cross join req
      left join core.ad_spend_days a
        on a.company_id = p_company_id and a.mall = p_mall and a.scope_key = p_scope_key and a.ad_type = req.ad_type and a.date_jst = d.day
  )
  select per.day,
         case max(per.rnk) when 5 then 'not_collected' when 4 then 'missing' when 3 then 'legacy_incomplete' when 2 then 'verified_legacy' else 'complete' end,
         case when max(per.rnk) >= 4 then null else sum(per.cost_total) end
    from per group by per.day
$$;

-- ─── 広告の子を今のマスタで出品に結び直す (§3.5・R14 H2)。sku の行だけ core.resolve_listing_id・asin / none は常に未解決 (listing_id null) ───
--   保存済みの core.ad_spend_daily.listing_id は使わない (受け取り・relink のときのマスタ = D-64 では今のマスタで決め直す)
create or replace function mart._amazon_profit_ad_children(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (date_jst date, ad_type text, listing_id bigint, unresolved_granularity text, unresolved_code text, ad_cost numeric, ad_rows integer)
language sql stable as $$
  with a as materialized (
    select x.date_jst, x.ad_type, x.target_granularity, x.target_code, x.ad_cost
      from core.ad_spend_daily x
     where x.company_id = p_company_id and x.mall = p_mall and x.scope_key = p_scope_key and x.date_jst between p_from and p_to
  ),
  res as materialized (   -- コードごとに 1 回だけ解く
    select c.target_code, core.resolve_listing_id(p_company_id, p_mall, c.target_code) as lid
      from (select distinct a.target_code from a where a.target_granularity = 'sku') c
  )
  select a.date_jst, a.ad_type, res.lid,
         case when res.lid is null then a.target_granularity end, case when res.lid is null then a.target_code end,
         sum(a.ad_cost), count(*)::int
    from a left join res on a.target_granularity = 'sku' and res.target_code = a.target_code
   group by 1, 2, 3, 4, 5
$$;

-- ─── Easy Ship の割り振り (D-59・R15 M1。SQLite の build (D7b-0 = #1548) と同じ規則) ───
--   料金 = 期間の中の easy_ship の行を 注文 × 計上日 で正味 (Σ account_fee_amount_jpy。負 = 費用)。正味 0 は配らない
--   割り振り = 同じ注文の SKU (正規化) の本体売上 (全部の日) の割合 / 合計が 0 以下なら等分。1 円単位・端数は小数部の大きい順 → 正規化 SKU のバイトの順
--   easy_ship_cost_jpy = 費用を正 (正味が負 = 料金 → 正の額・正味が正 = 返金が多い → 負の額)。allocated = false = 売上の行が無くて配らない額 (seller_sku_norm は null)
create or replace function mart._amazon_easy_ship_alloc(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (economic_date_jst date, mall_order_no text, seller_sku_norm text, easy_ship_cost_jpy bigint, allocated boolean)
language sql stable as $$
  with charge as materialized (
    select f.mall_order_no, f.economic_date_jst as day, sum(f.account_fee_amount_jpy) as amt
      from core.order_finance_daily f
      join core.finance_source_policy p
        on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
       and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
     where f.line_kind = 'easy_ship' and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
       and f.economic_date_jst between p_from and p_to
     group by f.mall_order_no, f.economic_date_jst
    having sum(f.account_fee_amount_jpy) <> 0
  ),
  -- 注文の本体売上 (🚨 期間の外の日も含む全部の日。日ごとに policy が採った source の行だけ = source が 2 つになっても二重にしない)
  w as materialized (
    select f.mall_order_no, core.norm_code(f.seller_sku) as sku_norm, sum(f.sales_principal_jpy) as principal
      from core.order_finance_daily f
      join core.finance_source_policy p
        on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
       and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
     where f.line_kind = 'sku' and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
       and f.mall_order_no in (select charge.mall_order_no from charge where charge.mall_order_no not like '-%')
     group by f.mall_order_no, core.norm_code(f.seller_sku)
    having bool_or(f.units_ordered <> 0 or f.sales_principal_jpy <> 0 or f.sales_shipping_jpy <> 0 or f.sales_giftwrap_jpy <> 0)   -- 売上の行 (Order の行から来た値)
  ),
  wt as (
    select w.mall_order_no, w.sku_norm, w.principal,
           sum(w.principal) over (partition by w.mall_order_no) as total, count(*) over (partition by w.mall_order_no) as n
      from w
  ),
  share as (
    select c.day, c.mall_order_no, c.amt, wt.sku_norm,
           case when wt.total > 0 then abs(c.amt)::numeric * wt.principal / wt.total else abs(c.amt)::numeric / wt.n end as exact
      from charge c join wt on wt.mall_order_no = c.mall_order_no
  ),
  base as (
    select s.day, s.mall_order_no, s.amt, s.sku_norm, trunc(s.exact) as fl,
           abs(s.amt) - sum(trunc(s.exact)) over (partition by s.day, s.mall_order_no) as remainder,
           row_number() over (partition by s.day, s.mall_order_no order by s.exact - trunc(s.exact) desc, s.sku_norm collate "C") as rk
      from share s
  )
  select base.day, base.mall_order_no, base.sku_norm,
         ((case when base.amt < 0 then 1 else -1 end) * (base.fl + case when base.rk <= base.remainder then 1 else 0 end))::bigint, true
    from base
  union all
  select c.day, c.mall_order_no, null::text, (-c.amt)::bigint, false
    from charge c where not exists (select 1 from wt where wt.mall_order_no = c.mall_order_no)
$$;

-- ─── 行の本体 (日 × 出品・未解決は日 × 正規化 seller SKU) ───
create or replace function mart._amazon_profit_rows(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (
  company_id smallint, mall text, scope_key text, economic_date_jst date,
  listing_id bigint, seller_sku_norm text, listing_resolution text, listing_code text,
  received_listing_ids bigint[], received_listing_unresolved_count integer,
  units_ordered integer, units_refunded_customer integer, units_a_to_z_refund integer, units_net_sold integer,
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
days as materialized (select * from mart._amazon_profit_finance_days(p_company_id, p_mall, p_scope_key, p_from, p_to)),
ad_days as materialized (select * from mart._amazon_profit_ad_days(p_company_id, p_mall, p_scope_key, p_from, p_to)),
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
    from mart._amazon_easy_ship_alloc(p_company_id, p_mall, p_scope_key, p_from, p_to) e
   where e.allocated
   group by 1, 2
),
adc as materialized (select * from mart._amazon_profit_ad_children(p_company_id, p_mall, p_scope_key, p_from, p_to)),
ad_l as (select adc.date_jst as day, adc.listing_id as lid, sum(adc.ad_cost) as cost, sum(adc.ad_rows)::int as n from adc where adc.listing_id is not null group by 1, 2),
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
              when d.complete_to is null or (date_trunc('month', k.day) + interval '1 month - 1 day')::date > d.complete_to then 3
              else 2 end as refund_rank,
         coalesce(f.refund_unestimated_jpy, 0) as refund_unestimated_jpy,
         d.day_finance_status, ad.ad_status, ad_l.cost as ad_linked, coalesce(ad_l.n, 0) as ad_n, coalesce(ad_u.n, 0) as ad_unres_n,
         coalesce(ek.cost, 0) as es_cost,
         comp_l.n_comp, comp_l.composition_hash,
         uc.all_known, uc.known_sum, uc.basis_rank, uc.missing_ids, uc.sc_ids, uc.ob_ids, uc.cost_input_hash,
         aud.through,
         ((k.day - 1)::timestamp at time zone 'Asia/Tokyo') as audit_from   -- その日の前日の JST 00:00
    from keys k
    join days d on d.economic_date_jst = k.day
    join ad_days ad on ad.date_jst = k.day
    left join fk f on f.economic_date_jst = k.day and f.rk = k.rk
    left join ek on ek.day = k.day and ek.rk = k.rk
    left join ad_l on ad_l.day = k.day and ad_l.lid = k.lid
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
         case when r0.lid is not null
              then coalesce(r0.lid <> any(r0.received_listing_ids), false) or coalesce(r0.received_listing_unresolved_count, 0) > 0
              else coalesce(cardinality(r0.received_listing_ids), 0) > 0 end as n_changed
    from r0
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
       coalesce(r3.received_listing_ids, '{}'::bigint[]), coalesce(r3.received_listing_unresolved_count, 0),
       r3.units_ordered, r3.units_refunded_customer, r3.units_a_to_z_refund, r3.units_net_sold,
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
       r3.ad_status, r3.ad_cost_v, r3.ad_n, r3.es_cost,
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
       null::bigint, null::bigint,   -- D7b-1b (core.finance_coverage の世代と source_revision) まで null
       statement_timestamp()
  from r3
 order by r3.day, r3.lid is null, r3.lid, r3.unorm collate "C"
$$;

-- ─── 日 / 月 / 期間の合計の本体 ───
create or replace function mart._amazon_profit_totals(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
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
language sql stable as $$
with
days as materialized (select * from mart._amazon_profit_finance_days(p_company_id, p_mall, p_scope_key, p_from, p_to)),
ad_days as materialized (select * from mart._amazon_profit_ad_days(p_company_id, p_mall, p_scope_key, p_from, p_to)),
rws as materialized (select * from mart._amazon_profit_rows(p_company_id, p_mall, p_scope_key, p_from, p_to)),
adc as materialized (select * from mart._amazon_profit_ad_children(p_company_id, p_mall, p_scope_key, p_from, p_to)),
esu as (
  select e.economic_date_jst as day, sum(e.easy_ship_cost_jpy) as cost, count(*)::int as n
    from mart._amazon_easy_ship_alloc(p_company_id, p_mall, p_scope_key, p_from, p_to) e
   where not e.allocated group by 1
),
-- SKU の無い行 (月の手数料・損益の外・unknown)。行き先は §3.7 の表
ns as (
  select f.economic_date_jst as day,
         sum(-f.account_fee_amount_jpy) filter (where mart.amazon_account_fee_tax_rate(f.line_kind) is not null) as fee,
         sum((-f.account_fee_amount_jpy)::numeric / (1 + mart.amazon_account_fee_tax_rate(f.line_kind))) filter (where mart.amazon_account_fee_tax_rate(f.line_kind) is not null) as fee_excl,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'storage') as fee_storage,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'long_term_storage') as fee_lts,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'removal') as fee_removal,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'inbound_defect') as fee_inbound,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'low_inventory') as fee_low,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'subscription') as fee_sub,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'easy_ship') as fee_es,
         sum(-f.account_fee_amount_jpy) filter (where f.line_kind = 'other_account_fee') as fee_other,
         sum(f.net_jpy - f.account_fee_amount_jpy - f.unmapped_jpy) filter (where mart.amazon_account_fee_tax_rate(f.line_kind) is not null) as fee_uncl_mapped,
         sum(f.net_jpy - f.unmapped_jpy) filter (where f.line_kind = 'not_account_fee') as naf,
         sum(f.net_jpy - f.unmapped_jpy) filter (where f.line_kind = 'unknown') as unk,
         count(*) filter (where f.line_kind = 'unknown') as unk_rows,
         sum(f.net_jpy) as net, sum(f.unmapped_jpy) as unmapped,
         sum(f.unclassified_component_count) as uc, sum(f.unmapped_component_count) as umc,
         count(*) filter (where not core.finance_version_has_class(f.transform_version)) as legacy
    from core.order_finance_daily f
    join core.finance_source_policy p
      on p.company_id = f.company_id and p.mall = f.mall and p.scope_key = f.scope_key and p.source = f.source
     and f.economic_date_jst >= p.period_from and (p.period_to is null or f.economic_date_jst < p.period_to)
   where f.line_kind <> 'sku' and f.company_id = p_company_id and f.mall = p_mall and f.scope_key = p_scope_key
     and f.economic_date_jst between p_from and p_to
   group by 1
),
rd as (
  select r.economic_date_jst as day,
         count(*) filter (where r.listing_resolution = 'resolved') as n_res, count(*) filter (where r.listing_resolution = 'unresolved') as n_unres,
         sum(r.units_ordered) as units_ordered, sum(r.units_net_sold) as units_net_sold, sum(r.sales_principal_jpy) as sales_principal,
         sum(r.sales_tax_jpy) as sales_tax, sum(r.profit_before_cogs_jpy) as pbc,
         bool_and(r.cogs_jpy is not null) as cogs_all, sum(r.cogs_jpy) as cogs,
         sum(r.easy_ship_alloc_jpy) as es_alloc,
         sum(r.net_jpy) as net, sum(r.unmapped_jpy) as unmapped, sum(r.misc_fee_jpy + r.other_fee_jpy + r.other_amount_jpy) as uncl_mapped,
         sum(r.unclassified_component_count) as uc, sum(r.unmapped_component_count) as umc, sum(r.finance_legacy_rows) as legacy,
         bool_and(r.contribution_before_ad_incl_jpy is not null) as rows_before_ok,
         sum(r.contribution_before_ad_incl_jpy) as before_incl,
         sum(r.taxable_sku_fee_cost_jpy) as taxable, sum(r.promotion_tax_jpy) as promo_tax,
         sum(r.contribution_before_ad_assuming_incomplete_zero_incl_jpy) as before_zero,
         bool_or('refund_units_unknown' = any(r.profit_incomplete_reasons)) as f_ref_unknown,
         bool_or('refund_units_partial_month' = any(r.profit_incomplete_reasons)) as f_ref_partial,
         bool_or('listing_unresolved' = any(r.profit_incomplete_reasons)) as f_unres,
         bool_or('composition_missing' = any(r.profit_incomplete_reasons)) as f_comp,
         bool_or('cost_missing' = any(r.profit_incomplete_reasons)) as f_cost,
         count(*) filter (where 'pre_audit_unverifiable' = any(r.master_notes)) as n_pre,
         count(*) filter (where 'current_after_recorded_change' = any(r.master_notes)) as n_after,
         count(*) filter (where 'listing_changed_since_received' = any(r.master_notes)) as n_changed
    from rws r group by 1
),
adu as (
  select adc.date_jst as day,
         sum(adc.ad_cost) filter (where adc.listing_id is not null) as alloc,
         sum(adc.ad_cost) filter (where adc.listing_id is null) as unres,
         sum(adc.ad_rows) filter (where adc.listing_id is null) as unres_rows
    from adc group by 1
),
dd as (
  select d.economic_date_jst as day, d.day_finance_status, a.ad_status, a.ad_cost_total,
         case a.ad_status when 'not_collected' then 5 when 'missing' then 4 when 'legacy_incomplete' then 3 when 'verified_legacy' then 2 else 1 end as ad_rank,
         coalesce(rd.n_res, 0) as n_res, coalesce(rd.n_unres, 0) as n_unres,
         coalesce(rd.units_ordered, 0) as units_ordered, coalesce(rd.units_net_sold, 0) as units_net_sold, coalesce(rd.sales_principal, 0) as sales_principal,
         coalesce(rd.sales_tax, 0) as sales_tax, coalesce(rd.pbc, 0) as pbc,
         coalesce(rd.cogs_all, true) as cogs_all, coalesce(rd.cogs, 0) as cogs,
         coalesce(rd.es_alloc, 0) as es_alloc, coalesce(esu.cost, 0) as es_unalloc, coalesce(esu.n, 0) as es_unalloc_n,
         coalesce(adu.alloc, 0) as ad_alloc, coalesce(adu.unres, 0) as ad_unres, coalesce(adu.unres_rows, 0) as ad_unres_rows,
         coalesce(ns.fee, 0) as fee, coalesce(ns.fee_excl, 0) as fee_excl,
         coalesce(ns.fee_storage, 0) as fee_storage, coalesce(ns.fee_lts, 0) as fee_lts, coalesce(ns.fee_removal, 0) as fee_removal,
         coalesce(ns.fee_inbound, 0) as fee_inbound, coalesce(ns.fee_low, 0) as fee_low, coalesce(ns.fee_sub, 0) as fee_sub,
         coalesce(ns.fee_es, 0) as fee_es, coalesce(ns.fee_other, 0) as fee_other,
         coalesce(rd.net, 0) + coalesce(ns.net, 0) as net,
         coalesce(ns.naf, 0) as naf, coalesce(ns.unk, 0) as unk, coalesce(ns.unk_rows, 0) as unk_rows,
         coalesce(rd.uncl_mapped, 0) + coalesce(ns.fee_uncl_mapped, 0) as uncl_mapped,
         coalesce(rd.unmapped, 0) + coalesce(ns.unmapped, 0) as unmapped,
         coalesce(rd.uc, 0) + coalesce(ns.uc, 0) as uc, coalesce(rd.umc, 0) + coalesce(ns.umc, 0) as umc,
         coalesce(rd.uc, 0) as sku_uc, coalesce(rd.umc, 0) as sku_umc,
         coalesce(rd.legacy, 0) + coalesce(ns.legacy, 0) as legacy,
         coalesce(rd.uc, 0) + coalesce(rd.umc, 0) + coalesce(rd.legacy, 0) as sku_block,
         coalesce(ns.uc, 0) + coalesce(ns.umc, 0) + coalesce(ns.legacy, 0) + coalesce(ns.unk_rows, 0) as fee_block,
         -- before ad = その日が complete かつ全部の行の寄与が正式 (行が無い日 = 確定の 0)
         d.day_finance_status = 'complete' and coalesce(rd.rows_before_ok, true) as ok_before,
         coalesce(rd.before_incl, 0) as before_incl_sum,
         coalesce(rd.taxable, 0) as taxable, coalesce(rd.promo_tax, 0) as promo_tax, coalesce(rd.before_zero, 0) as before_zero,
         coalesce(rd.f_ref_unknown, false) as f_ref_unknown, coalesce(rd.f_ref_partial, false) as f_ref_partial,
         coalesce(rd.f_unres, false) as f_unres, coalesce(rd.f_comp, false) as f_comp, coalesce(rd.f_cost, false) as f_cost,
         coalesce(rd.n_pre, 0) as n_pre, coalesce(rd.n_after, 0) as n_after, coalesce(rd.n_changed, 0) as n_changed
    from days d
    join ad_days a on a.date_jst = d.economic_date_jst
    left join rd on rd.day = d.economic_date_jst
    left join ns on ns.day = d.economic_date_jst
    left join adu on adu.day = d.economic_date_jst
    left join esu on esu.day = d.economic_date_jst
),
dv as (   -- 日の値 (丸めない)
  select dd.*,
         dd.ok_before and dd.ad_status in ('complete', 'verified_legacy') as ok_after,
         dd.ok_before and dd.ad_status in ('complete', 'verified_legacy') and dd.fee_block = 0 as ok_fees,
         dd.before_incl_sum + dd.taxable::numeric / 11 + dd.promo_tax as before_excl_raw,
         dd.before_zero + dd.taxable::numeric / 11 + dd.promo_tax as before_zero_excl_raw,
         coalesce(dd.ad_cost_total, 0) as ad_zero
    from dd
),
grp as (
  select 'day'::text as kind, dv.day as pf, dv.day as pt, dv.day as eday, null::date as ms, 1 as ord from dv
  union all
  select case when mm.m >= p_from and mm.me <= p_to then 'calendar_month' else 'range_month_subtotal' end,
         greatest(mm.m, p_from), least(mm.me, p_to), null::date, mm.m, 2
    from (select m0.m, (m0.m + interval '1 month - 1 day')::date as me
            from (select distinct date_trunc('month', dv.day)::date as m from dv) m0) mm
  union all
  select 'range_total', p_from, p_to, null::date, null::date, 3
)
select g.kind, g.pf, g.pt, g.eday, g.ms, count(*)::int,
       case when g.kind = 'day' then min(dv.day_finance_status) end,
       (count(*) filter (where dv.day_finance_status = 'complete'))::int,
       sum(dv.n_res)::int, sum(dv.n_unres)::int,
       sum(dv.units_ordered)::bigint, sum(dv.units_net_sold)::bigint, sum(dv.sales_principal)::bigint, sum(dv.sales_tax)::bigint, sum(dv.pbc)::bigint,
       case when bool_and(dv.cogs_all) then sum(dv.cogs)::bigint end,
       sum(dv.es_alloc)::bigint, sum(dv.es_unalloc)::bigint, sum(dv.es_unalloc_n)::int,
       case max(dv.ad_rank) when 5 then 'not_collected' when 4 then 'missing' when 3 then 'legacy_incomplete' when 2 then 'verified_legacy' else 'complete' end,
       case when max(dv.ad_rank) >= 4 then null else round(sum(dv.ad_cost_total), 2) end,
       round(sum(dv.ad_alloc), 2), round(sum(dv.ad_unres), 2), sum(dv.ad_unres_rows)::int,
       sum(dv.fee)::bigint, round(sum(dv.fee_excl), 2),
       sum(dv.fee_storage)::bigint, sum(dv.fee_lts)::bigint, sum(dv.fee_removal)::bigint, sum(dv.fee_inbound)::bigint,
       sum(dv.fee_low)::bigint, sum(dv.fee_sub)::bigint, sum(dv.fee_es)::bigint, sum(dv.fee_other)::bigint,
       sum(dv.net)::bigint, sum(dv.naf)::bigint, sum(dv.unk)::bigint, sum(dv.uncl_mapped)::bigint, sum(dv.unmapped)::bigint,
       sum(dv.unk_rows)::int, sum(dv.uc)::int, sum(dv.umc)::int, sum(dv.sku_uc)::int, sum(dv.sku_umc)::int, sum(dv.legacy)::int,
       case when bool_and(dv.ok_before) then sum(dv.before_incl_sum)::bigint end,
       case when bool_and(dv.ok_before) then round(sum(dv.before_excl_raw), 2) end,
       case when bool_and(dv.ok_after) then round(sum(dv.before_incl_sum - dv.ad_cost_total * 1.1), 2) end,
       case when bool_and(dv.ok_after) then round(sum(dv.before_excl_raw - dv.ad_cost_total), 2) end,
       case when bool_and(dv.ok_fees) then round(sum(dv.before_incl_sum - dv.ad_cost_total * 1.1 - dv.fee), 2) end,
       case when bool_and(dv.ok_fees) then round(sum(dv.before_excl_raw - dv.ad_cost_total - dv.fee_excl), 2) end,
       round(sum(dv.before_zero - dv.ad_zero * 1.1), 2),
       round(sum(dv.before_zero_excl_raw - dv.ad_zero), 2),
       round(sum(dv.before_zero - dv.ad_zero * 1.1 - dv.fee), 2),
       round(sum(dv.before_zero_excl_raw - dv.ad_zero - dv.fee_excl), 2),
       coalesce(array_agg(dv.day order by dv.day) filter (where not dv.ok_before), '{}'::date[]), (count(*) filter (where not dv.ok_before))::int,
       coalesce(array_agg(dv.day order by dv.day) filter (where not dv.ok_after), '{}'::date[]), (count(*) filter (where not dv.ok_after))::int,
       coalesce(array_agg(dv.day order by dv.day) filter (where not dv.ok_fees), '{}'::date[]), (count(*) filter (where not dv.ok_fees))::int,
       -- 理由 (§3.7 の固定の順)。ad_unresolved は合計を止めない = 入れない (未解決の額と行の数は ad_cost_unresolved / ad_unresolved_rows)
       array_remove(array[
         case when bool_or(dv.day_finance_status <> 'complete') then 'finance_incomplete' end,
         case when bool_or(dv.sku_block + dv.fee_block > 0) then 'finance_unclassified' end,
         case when bool_or(dv.f_ref_unknown) then 'refund_units_unknown' end, case when bool_or(dv.f_ref_partial) then 'refund_units_partial_month' end,
         case when bool_or(dv.f_unres) then 'listing_unresolved' end, case when bool_or(dv.f_comp) then 'composition_missing' end,
         case when bool_or(dv.f_cost) then 'cost_missing' end,
         case when bool_or(dv.ad_status = 'not_collected') then 'ad_not_collected' end, case when bool_or(dv.ad_status = 'missing') then 'ad_missing' end,
         case when bool_or(dv.ad_status = 'legacy_incomplete') then 'ad_legacy_unverified' end]::text[], null),
       'current',
       jsonb_build_object('pre_audit_unverifiable', sum(dv.n_pre)::int, 'current_after_recorded_change', sum(dv.n_after)::int,
                          'listing_changed_since_received', sum(dv.n_changed)::int),
       'amazon_profit_v1', null::bigint, null::bigint, statement_timestamp()
  from grp g join dv on dv.day between g.pf and g.pt
 group by g.kind, g.pf, g.pt, g.eday, g.ms, g.ord
 order by g.ord, g.pf
$$;

-- ─── 公開の 2 つ (引数を確かめてから本体を呼ぶ) ───
create or replace function mart.amazon_profit_daily_range(p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date)
returns table (
  company_id smallint, mall text, scope_key text, economic_date_jst date,
  listing_id bigint, seller_sku_norm text, listing_resolution text, listing_code text,
  received_listing_ids bigint[], received_listing_unresolved_count integer,
  units_ordered integer, units_refunded_customer integer, units_a_to_z_refund integer, units_net_sold integer,
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
language plpgsql stable as $$
begin
  perform mart.amazon_profit_assert_args(p_company_id, p_mall, p_scope_key, p_from, p_to);
  return query select * from mart._amazon_profit_rows(p_company_id, p_mall, p_scope_key, p_from, p_to);
end
$$;
comment on function mart.amazon_profit_daily_range(smallint, text, text, date, date) is 'Amazon の利益の mart (0049・D7b-3): 日 × 出品の寄与の利益 (月の手数料を引く前)。構成と出品の結びつけは計算のときの今のマスタ (master_basis = current)。正式な値は分からないものがあれば null + profit_incomplete_reasons。0 と仮定の値は …_assuming_incomplete_zero_…';

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
  perform mart.amazon_profit_assert_args(p_company_id, p_mall, p_scope_key, p_from, p_to);
  return query select * from mart._amazon_profit_totals(p_company_id, p_mall, p_scope_key, p_from, p_to);
end
$$;
comment on function mart.amazon_profit_day_totals_range(smallint, text, text, date, date) is 'Amazon の利益の mart (0049・D7b-3): 日 / 暦の月 / 期間の中の月の小計 / 期間の合計 (row_kind)。寄与・広告の後・月の手数料を引いた後を列の組ごとの条件で正式にし、不完全な日を返す';

-- ─── 権限 (0047 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function mart.amazon_profit_daily_range(smallint, text, text, date, date), mart.amazon_profit_day_totals_range(smallint, text, text, date, date),'
         || ' core.finance_coverage_complete_to(smallint, text, text, text), mart.amazon_profit_composition_audit_since(), mart.amazon_account_fee_tax_rate(text) to watcher';
  end if;
end $$;
