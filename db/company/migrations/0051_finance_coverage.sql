-- 0051 決済のそろい core.finance_coverage (D7b-1b-2 = Render 側。2026-09-30)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1 (D-65・D-66)。
--   「Amazon 側で決済がそろった」と「その全部の行が Company DB に入った」の **両方** を満たす最後の日 = complete_to を、会社 × モール × scope × source ごとに 1 行で持つ。
--   値を作るのは miniPC の coordinator (D7b-1b-3・後の PR = 一覧・初期の印・文書の版・lease・source_revision の trigger)。この PR は受け皿と受け口だけ。
--
-- 作るもの:
--   ① core.finance_coverage = 今の世代の状態 (1 key = 1 行)。列は §3.1 の一覧 + 無効にした印 (invalidated_at / invalidated_reason)
--   ② core.finance_coverage_state(会社, モール, scope, source) を **差し替える** (0049 の「全部 null」の差し込み口。名前・引数・戻りの形は同じ = 呼び手の mart (0049) は変えない)
--      complete_to = state = complete のときだけ (updating・行なしは null) / generation = 今の世代 (updating でも返す・行なしは null) / source_revision = complete のときだけ
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
-- 🚨 既存の表・関数は変えない (core.finance_coverage_state だけ差し替える)。個人情報なし。

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
    and expected_report_count is not null and expected_report_digest is not null and inventory_runs_digest is not null
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

-- ─── core.finance_coverage_state を差し替える (0049 の差し込み口。名前・引数・戻りの形は同じ・呼び手は変えない) ───
--   戻り = いつも 1 行。complete_to / source_revision = state = complete のときだけ (updating・行なしは null = 正式な利益は null) / generation = 今の世代 (行なしは null)
create or replace function core.finance_coverage_state(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
returns table (complete_to date, generation bigint, source_revision bigint) language plpgsql stable as $$
begin
  return query
    select case when c.state = 'complete' then c.complete_to end, c.generation, case when c.state = 'complete' then c.source_revision end
      from core.finance_coverage c
     where c.company_id = p_company_id and c.mall = p_mall and c.scope_key = p_scope_key and c.source = p_source;
  if not found then
    return query select null::date, null::bigint, null::bigint;
  end if;
end
$$;
comment on function core.finance_coverage_state(smallint, text, text, text) is '決済のそろい (0049 の差し込み口を 0051 で差し替え)。1 行 = complete_to (complete のときだけ)・generation (今の世代)・source_revision (complete のときだけ)。core.finance_coverage が無い key は全部 null。Amazon の利益の mart (0049) の日の状態 (day_finance_status) と finance_coverage_generation / finance_source_revision がこれを読む';

-- ─── 権限 (0049 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.finance_coverage to watcher';
    execute 'grant execute on function core.finance_coverage_state(smallint, text, text, text) to watcher';
  end if;
end $$;
