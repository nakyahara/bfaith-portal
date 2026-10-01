-- 0053 持ち主の設定の epoch (マスタ正本切替 ④a。Codex #1564 R1 H1。2026-10-01)
-- 積み方: master の 0050 → ⑤-1 の 0051 (切替の段階・前提の差し込み口・画面の保存の門) → ⑤-2a の 0052 → この 0053 (0051 / 0052 だけに寄る。マージの順で番号を付け替える)
--
-- なぜ: config/master-ownership.mjs を 'company' に書き換えた夜間ロードの時点で、実際の持ち主が切り替わってしまう (初回の写し・作り直しが失敗しても)。
--   → 持ち主の設定を 3 つに分ける:
--     configured = config/master-ownership.mjs (コードに書いた「こうしたい」)。これだけでは何も変わらない
--     prepared   = 人がコマンドで「次にこれにする」と記録したもの (scripts/company-db/master-ownership-epoch.mjs prepare)。
--                  切替の日だけ、明示して頼んだロード (remote-load.mjs load --apply --use-prepared) がこの持ち主で動く
--     active     = 今使っている持ち主。毎晩の夜間ロード・miniPC の写し・作り直しはこれ。prepared の世代で miniPC の作り直しと入れた後の確かめが
--                  通ったときだけ、人がコマンドで prepared → active にする (master-ownership-epoch.mjs activate。確かめの証拠を残す)
--   行が無い = active は全部 'load' (今までと同じ)。
-- 🚨 古い書き込み口 (/register など) を閉じるのは ⑤-3 の切替の手順 (持ち主を変える前に閉じる)。ここでは門を作らない。
-- 🚨 書くのは持ち主のコマンドだけ (DB を作ったユーザー)。watcher は読むだけ (ops の既定の権限)。

create table ops.master_ownership_state (
  id                 smallint primary key default 1 check (id = 1),
  active_hash        text not null check (active_hash ~ '^[0-9a-f]{64}$'),
  active_map         jsonb not null check (jsonb_typeof(active_map) = 'object'),
  activated_at       timestamptz not null default now(),
  activated_by       text not null,
  activated_evidence jsonb,              -- 有効にしたときの確かめ (miniPC の作り直しの ID・世代・入れた後の確かめ)
  prepared_hash      text check (prepared_hash ~ '^[0-9a-f]{64}$'),
  prepared_map       jsonb check (prepared_map is null or jsonb_typeof(prepared_map) = 'object'),
  prepared_at        timestamptz,
  prepared_by        text,
  updated_at         timestamptz not null default now(),
  constraint ck_master_ownership_prepared check ((prepared_hash is null) = (prepared_map is null) and (prepared_hash is null) = (prepared_at is null)),
  constraint ck_master_ownership_prepared_differs check (prepared_hash is null or prepared_hash <> active_hash)
);
comment on table ops.master_ownership_state is '持ち主の設定の epoch (④a)。active = 夜間ロード・写し・作り直しが使う / prepared = 切替の日に明示のロードだけが使う。行が無い = 全部 load';

-- 変更の記録 (足すだけ)
create table ops.master_ownership_events (
  event_id       bigint generated always as identity primary key,
  action         text not null check (action in ('init', 'prepare', 'cancel_prepare', 'activate')),
  ownership_hash text not null check (ownership_hash ~ '^[0-9a-f]{64}$'),
  ownership      jsonb not null,
  actor          text not null,
  evidence       jsonb,
  recorded_at    timestamptz not null default now()
);
create or replace function ops.master_ownership_events_append_only() returns trigger language plpgsql as $$
begin
  raise exception 'ops.master_ownership_events は足すだけ (append-only)';
end $$;
create trigger trg_master_ownership_events_append_only before update or delete on ops.master_ownership_events
  for each row execute function ops.master_ownership_events_append_only();

-- watcher (miniPC の写し = publish/fetch.mjs) が読む。書くのは DB を作ったユーザー (master-ownership-epoch.mjs) だけ
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then execute 'grant select on ops.master_ownership_state, ops.master_ownership_events to watcher'; end if;
end $$;
