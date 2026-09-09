-- 0006 ai (判断・所見・Action Queue・評価・見張り規則) / docs (文書台帳) / ops (ジョブ実行履歴) (03 §5〜§6、06 §6)
--
-- 既存 5 形式の AI 表 (ai_daily_findings / ai_jobs+ai_drafts / draft_ai_outputs / category_ai / radash_actions) を
-- 1 つの型に集約する受け皿。検知は決定的 SQL (ai.watch_rules)、AI は解釈のみ (ai-insights の原則)。
-- 安全機構 (方針 7) は表ではなく実行サービスの責務: validation.passed=true 以外は実行しない、unknown は人へ。

create table ai.decisions (                               -- AI が何を判断したか (提案・所見・分類・下書き すべて)
  decision_id      bigint generated always as identity primary key,
  company_id       smallint not null references core.companies,
  domain           text not null,                       -- 'product_watch','fba_replenishment','price','ads','inquiry_reply','product_copy','category','daily_finding'
  decision_kind    text not null check (decision_kind in ('proposal','finding','classification','draft','forecast')),
  subject_type     text,                                -- 'product','sku','listing','catalog_item','order','inquiry'
  subject_id       bigint,
  summary          text not null,
  rationale        text,
  confidence       numeric(4,3) check (confidence between 0 and 1),
  severity         text check (severity in ('info','warn','critical')),
  proposed_action  jsonb,                               -- {action_type, parameters, requires_approval}
  inputs_ref       jsonb not null,                      -- 参照したデータ (表名・run_id・期間・スナップショット日)
  model            text,                                -- 'claude-fable-5-1' / 'rule:PW-06' / 'sql:v1'
  prompt_version   text,
  rule_version     text,
  generated_by     text not null check (generated_by in ('llm','rule','sql','hybrid')),
  autonomy_level   smallint not null default 0 check (autonomy_level between 0 and 3),
  status           text not null default 'new' check (status in ('new','reviewable','stale','approved','rejected','executed','superseded','dismissed','resolved')),
  dedupe_key       text,                                -- 同じ所見の再掲を抑える鍵 (rule × subject × 値)
  expires_at       timestamptz,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);
create index ix_ai_decisions_domain_status on ai.decisions (domain, status, created_at desc);
create index ix_ai_decisions_subject on ai.decisions (subject_type, subject_id);
create index ix_ai_decisions_dedupe on ai.decisions (dedupe_key) where dedupe_key is not null and status in ('new','reviewable');
create trigger trg_ai_decisions_touch before update on ai.decisions for each row execute function core.touch_updated_at();

create table ai.decision_reviews (                        -- 人の承認・却下・修正 (append-only)
  review_id      bigint generated always as identity primary key,
  decision_id    bigint not null references ai.decisions,
  reviewer_id    bigint references core.workers,
  verdict        text not null check (verdict in ('approved','approved_modified','rejected','deferred','acknowledged')),
  modified_action jsonb,
  comment        text,
  reviewed_at    timestamptz not null default now()
);
create index ix_ai_decision_reviews_decision on ai.decision_reviews (decision_id);

create table ai.autonomy_policies (                       -- domain × action_type ごとの委任レベル (AI が自分で上げられない)
  domain         text not null,
  action_type    text not null,
  autonomy_level smallint not null check (autonomy_level between 0 and 3),
  updated_by_id  bigint references core.workers,
  updated_at     timestamptz not null default now(),
  primary key (domain, action_type)
);

create table ai.actions (                                 -- Action Queue (実行要求)。Phase 9 まで記録のみ
  action_id      bigint generated always as identity primary key,
  company_id     smallint not null references core.companies,
  decision_id    bigint references ai.decisions,
  action_type    text not null,
  target_system  text not null,                         -- 'rakuten','yahoo','ne','amazon','gmail','notion','company_db'
  parameters     jsonb not null,
  validation     jsonb,
  status         text not null default 'queued' check (status in ('queued','validated','rejected','claimed','executing','succeeded','failed','unknown','cancelled')),
  idempotency_key text not null unique,
  claimed_by     text,
  claimed_at     timestamptz,
  lease_until    timestamptz,
  requested_by_type text not null check (requested_by_type in ('human','ai','system')),
  requested_by_id text,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now()
);
create index ix_ai_actions_status on ai.actions (status, created_at);
create trigger trg_ai_actions_touch before update on ai.actions for each row execute function core.touch_updated_at();

create table ai.action_results (                          -- 実行結果 (append-only。1 action に複数試行可)
  result_id      bigint generated always as identity primary key,
  action_id      bigint not null references ai.actions,
  attempt_no     smallint not null check (attempt_no > 0),
  outcome        text not null check (outcome in ('succeeded','failed','unknown')),
  external_ref   text,
  response       jsonb,
  executed_at    timestamptz not null default now(),
  unique (action_id, attempt_no)
);

create table ai.decision_outcomes (                       -- 後から見た良し悪し (「先月の判断のうち間違っていたもの」)
  outcome_id     bigint generated always as identity primary key,
  decision_id    bigint not null references ai.decisions,
  evaluated_at   timestamptz not null default now(),
  evaluator_type text not null check (evaluator_type in ('human','rule','llm')),
  score          numeric(4,3) check (score between -1 and 1),
  verdict        text check (verdict in ('good','neutral','bad','unknown')),
  evidence       jsonb
);
create index ix_ai_decision_outcomes_decision on ai.decision_outcomes (decision_id);

create table ai.watch_rules (                             -- 見張り規則 (決定的 SQL)。所見は ai.decisions(generated_by='sql')
  rule_id        text primary key,                       -- 'PW-01'
  domain         text not null,
  title          text not null,
  definition_sql text not null,                          -- 返す列: subject_type, subject_id, severity, summary, evidence jsonb, dedupe_key
  severity_default text not null check (severity_default in ('info','warn','critical')),
  needs          text[] not null,                        -- 必要データ: '{listing_states,sku_costs}'
  enabled        boolean not null default true,
  owner_worker_id bigint references core.workers,
  rule_version   text not null default 'v1',
  created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create trigger trg_ai_watch_rules_touch before update on ai.watch_rules for each row execute function core.touch_updated_at();

create table ai.catalog_watchlist (                       -- 監視対象 ASIN (自社出品の有無と独立。D-21)
  marketplace_id text not null,
  asin           text not null,
  purpose        text not null check (purpose in ('own','competitor','scout')),
  owner_worker_id bigint references core.workers,
  note           text,
  active         boolean not null default true,
  added_at       timestamptz not null default now(),
  primary key (marketplace_id, asin)
);

-- ── docs ──
create table docs.documents (
  document_id   bigint generated always as identity primary key,
  company_id    smallint not null references core.companies,
  document_type text not null,                          -- 'invoice','receipt','contract','manual','product_image_folder','canva_design','email_attachment','pdf_slip'
  storage       text not null check (storage in ('drive','notion','mf','local','url')),
  external_ref  text not null,                          -- Drive fileId / URL
  title         text,
  mime_type     text,
  content_hash  text,
  ai_summary    text,
  search_text   text,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now()
);
create unique index ux_documents_storage_ref on docs.documents (storage, external_ref);
create trigger trg_documents_touch before update on docs.documents for each row execute function core.touch_updated_at();

create table docs.document_links (                        -- 文書 ↔ エンティティ (多対多)
  document_id bigint not null references docs.documents,
  entity_type text not null,
  entity_id   bigint not null,
  link_role   text,                                      -- 'main_image','evidence','attachment','production_doc'
  created_at  timestamptz not null default now(),
  primary key (document_id, entity_type, entity_id)
);
create index ix_document_links_entity on docs.document_links (entity_type, entity_id);

-- ── ops ──
create table ops.job_runs (                               -- jobs-registry の実行履歴 (append-only)
  job_run_id  bigint generated always as identity primary key,
  job_id      text not null,
  host        text not null,
  started_at  timestamptz not null,
  finished_at timestamptz,
  status      text not null check (status in ('ok','partial','fail','running')),
  exit_code   integer,
  summary     text,
  created_at  timestamptz not null default now()
);
create index ix_job_runs_job_time on ops.job_runs (job_id, started_at desc);
