-- 0023: 見張り (Company DB を毎朝読んで「おかしいところ」に気づく仕組み) の結果の表。設計 = AI_reference CompanyDB構想/09 §7
--
--   定義 (何を・どう見るか) は DB に持たない = config/watch-checks.mjs が正本 (既存の ai.watch_rules (0006) は使わない = 廃止候補)。
--   ここに残すのは **結果** だけ:
--     watch_runs          1 実行 (毎朝の daily-sync の 1 ステップ)。予定した評価キーの数と完了数、daily-sync から渡された証跡 (push の結果) をそのまま
--     watch_results       1 run × 1 check × 1 scope × 対象期間。判定は 4 値 (pass / breach / blocked / execution_error) で、重さ (info / warn / error) とは別の軸
--     watch_issues        案件 = 未解決の異常 (check × scope × 型つきの対象)。検知の状態 (open / recovered / out_of_window) と、人の扱い (未確認 / 確認済み / 対応中 / 保留) は別の列
--     watch_result_items  明細 (SKU や日付)。全件で判定した後の抜粋 (影響度順・上限つき)。案件の管理には使わない
--   🚨 「行がある = そろっている」と読まない: 前提 (完了の印) が無ければ blocked = pass にしない。「異常なし」の誤報は誤検知より危ない
--   書くのは記録用のロール (watch_writer: insert + 限定 update)。読むのは照会用のロール (watcher: select だけ)。scripts/company-db/create-watch-roles.mjs
-- 🚨 0001〜0022 の表・関数は変えない。

create table ops.watch_runs (
  watch_run_id    text primary key,                        -- 'watch_<ISO ms>_<乱数>'
  company_id      smallint not null references core.companies,
  as_of_date      date not null,                          -- 評価の基準日 (JST の今日)。対象日は結果ごと
  started_at      timestamptz not null,
  finished_at     timestamptz,
  host            text,
  checks_version  text not null,                          -- config/watch-checks.mjs の版
  planned_keys    integer not null check (planned_keys >= 0),   -- scope に展開した評価キーの数 (「項目 12 のうち」ではなく「キー 31 のうち」)
  completed_keys  integer not null default 0 check (completed_keys >= 0),
  evidence        jsonb,                                  -- daily-sync (miniPC) から渡された証跡 = 各 push の結果 (run_id・件数・失敗)。変更ゼロの朝は ingest_runs が無いので、ここが唯一の記録
  summary         jsonb,                                  -- {pass, breach, blocked, execution_error, new_issues, continued, recovered}
  last_line       text,                                   -- 朝の通知に出した 1 行
  created_at      timestamptz not null default now()
);
create index ix_watch_runs_company_date on ops.watch_runs (company_id, as_of_date desc);

create table ops.watch_results (
  watch_result_id  bigint generated always as identity primary key,
  watch_run_id     text not null references ops.watch_runs,
  company_id       smallint not null references core.companies,
  check_id         text not null,                         -- 'W1' …
  check_version    text not null,
  scope_key        text not null,                         -- 'logizard/main' / 'amazon/jp' …
  subject_type     text not null default 'scope',         -- 'scope' / 'day' / 'sku' / 'mall' …
  period_from      date,
  period_to        date,
  evaluated_at     timestamptz not null default now(),
  verdict          text not null check (verdict in ('pass','breach','blocked','execution_error')),
  severity         text not null check (severity in ('info','warn','error')),
  observed         jsonb,                                 -- 観測値の内訳 (何が breach だったか)
  threshold        jsonb,
  sample_size      integer,
  input_generation jsonb,                                 -- 使った世代 (capture の run / 公開の run / 証跡の run_id)
  reason           text,                                  -- blocked / execution_error の理由・breach の要約
  duration_ms      integer,
  item_total       integer,                               -- 明細: 全件の該当数
  item_saved       integer,                               --       保存した数 (抜粋)
  item_selection   text                                   --       選び方
);
create index ix_watch_results_run on ops.watch_results (watch_run_id);
create index ix_watch_results_check on ops.watch_results (company_id, check_id, scope_key, evaluated_at desc);

create table ops.watch_issues (
  watch_issue_id   bigint generated always as identity primary key,
  company_id       smallint not null references core.companies,
  check_id         text not null,
  scope_key        text not null,
  subject_type     text not null default 'scope',
  subject_key      text not null default '',              -- 対象の識別 ('' = scope 全体 / 日付 / SKU の内部 ID)
  state            text not null check (state in ('open','recovered','out_of_window')),
  severity         text not null check (severity in ('info','warn','error')),
  first_seen_at    timestamptz not null,
  last_seen_at     timestamptz not null,
  days_seen        integer not null default 1 check (days_seen >= 1),
  recovered_at     timestamptz,
  transitions      integer not null default 1 check (transitions >= 1),   -- 状態遷移の番号 (通知の重複排除 = 案件 ID + この番号)
  first_result_id  bigint references ops.watch_results,
  last_result_id   bigint references ops.watch_results,
  summary          text,
  -- 人の扱い (検知の状態とは別。「確認済み」を「回復」とみなさない)
  handling         text not null default 'unreviewed' check (handling in ('unreviewed','acknowledged','in_progress','snoozed')),
  handling_until   timestamptz,                           -- snoozed の期限 (過ぎたら再提示)
  handling_note    text,
  handled_by       text,
  updated_at       timestamptz not null default now(),
  constraint ck_watch_issues_recovered check ((state = 'recovered') = (recovered_at is not null))
);
-- open の案件は 会社 × check × scope × 対象 で 1 つだけ (並行して 2 本走っても二重に作れない。#1403 Codex R1 #4。実行の直列化は advisory lock = 二重目)
create unique index ux_watch_issues_open on ops.watch_issues (company_id, check_id, scope_key, subject_type, subject_key) where state = 'open';
create index ix_watch_issues_state on ops.watch_issues (company_id, state, last_seen_at desc);
create trigger trg_watch_issues_touch before update on ops.watch_issues for each row execute function core.touch_updated_at();

create table ops.watch_result_items (
  watch_result_id  bigint not null references ops.watch_results,
  rank             integer not null check (rank >= 1),
  subject_type     text not null,
  subject_key      text not null,
  payload          jsonb not null,
  primary key (watch_result_id, rank)
);

comment on table ops.watch_runs is '見張りの実行 (毎朝 1 回)。証跡 (push の結果) と評価キーの予定数・完了数';
comment on table ops.watch_results is '見張りの結果。verdict は pass / breach / blocked / execution_error (重さとは別の軸)';
comment on table ops.watch_issues is '案件 = 未解決の異常。検知の状態と人の扱いは別の列';
comment on table ops.watch_result_items is '結果の明細の抜粋 (上限つき)。案件の管理には使わない';
