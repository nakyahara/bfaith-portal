-- 0055 持ち主の設定の epoch (マスタ正本切替 ④a。Codex #1564 R1 H1・R2 High 3。2026-10-01)
-- 積み方: master の 0050 → ⑤-1 の 0051 (切替の段階・前提の差し込み口・画面の保存の門) → ⑤-2a の 0052 → ⑤-2b の 0053 → ⑦-1 の 0054 → この 0055 (0051〜0054 に寄る。マージの順で番号を付け替える)
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
-- 🚨 持ち主の正を 1 つにする (Codex #1564 R2 High 3): ⑤-1 の切替の段階の持ち主表 (ops.master_cutover_state.owner_hash) と画面の保存の持ち主表は、
--   この表の active (epoch) と同じでなければならない (下の 4 つ。⑤-1 の関数は上書きしない = 差し込み口の表に 1 行足す・⑤-1 の表に trigger を足す。
--   例外は 1 つだけ: 持ち主表のハッシュの式 ops.ownership_hash を「load の列は数えない」に作り直す = 下の「1 つの式に」。#1564 Codex R3 Medium)
-- 🚨 夜間ロードと epoch を変えるコマンドは epoch の鍵 (ops.master_ownership_lock_key()) で並ぶ (#1564 Codex R3 High 1)
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

-- ─── 持ち主の epoch の鍵 (#1564 Codex R3 High 1) ───
-- 夜間ロード (apps/company-db/load/engine.mjs) は取引の冒頭に共有で取ってから epoch を読む (書き終わるまで持つ)。prepare / activate / cancel
-- (apps/company-db/load/ownership-state.mjs) は排他で取る = ロードの途中で epoch が変わらない (古い active を読んだロードが新しい active の後に commit しない)。
-- 数は 0036 の core.parent_lock_key() = 4705310036・0051 の core.master_write_lock_key() = 4705310051 と同じ作り (重ならない固定の数。
-- 2^31 より大きい = hashtext() の鍵 (ops.master_cutover など) とも重ならない)。
-- 🚨 鍵の順 (全部の書き手で同じ): 持ち主の epoch (0055) → 切替の段階 (0051 の hashtext('ops.master_cutover')) → マスタの書き込み (0051) → 親子 (0036) → 行
create function ops.master_ownership_lock_key() returns bigint language sql immutable as $$ select 4705310055::bigint $$;

-- ─── 夜間ロードの commit の順 (#1564 Codex R4 Medium 2・High) ───
-- 本適用の夜間ロード 1 回 = 1 行 (dry-run は巻き戻す = 行が無い)。commit_seq = DB が振る番号 = commit の順:
--   夜間ロードは取引の最後 (commit の直前) に、epoch の鍵 (共有) とマスタの書き込みの鍵 (0051・排他) を持ったまま 1 行足す
--   = 足してから commit までほかのロードは入れない = 番号の順 = commit の順 (送り手の時計 started_at / finished_at・場所 (host) では決めない)。
--   写し (publish/fetch.mjs) は「最後に commit したロード」= 番号の一番大きい行を使う (毎晩の cron か、--use-prepared の明示のロードかを問わない)。
--   activate は鍵 (排他) の後に、証拠の世代が読んだロードの番号 = 一番大きい番号かを見る (その後のロードがあれば断る)。
--   照合 ① (master-compare/compare-load.mjs) は今までどおり毎晩の cron の回 (host = render-nightly) を見る (別の目的)
create table ops.master_load_commits (
  commit_seq     bigint generated always as identity primary key,
  ingest_run_id  text not null unique references ops.ingest_runs (ingest_run_id),
  epoch          text not null check (epoch in ('active', 'prepared', 'default', 'explicit')),
  ownership_hash text not null check (ownership_hash ~ '^[0-9a-f]{64}$'),
  host           text,
  committed_at   timestamptz not null default clock_timestamp()
);
comment on table ops.master_load_commits is '夜間ロードの commit の順 (④a)。commit_seq = DB が振る番号 = commit の順 (送り手の時計で並べない)。写し・activate が使う';
create or replace function ops.master_load_commits_append_only() returns trigger language plpgsql as $$
begin
  raise exception 'ops.master_load_commits は足すだけ (append-only)';
end $$;
create trigger trg_master_load_commits_append_only before update or delete on ops.master_load_commits
  for each row execute function ops.master_load_commits_append_only();
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then execute 'grant select on ops.master_load_commits to watcher'; end if;
end $$;

-- ─── 持ち主表のハッシュを 1 つの式に (#1564 Codex R3 Medium) ───
-- ⑤-1 (0051) の ops.ownership_hash を作り直す (同じ名前・同じ形・同じ使い方。中身の式だけ):
--   持ち主が 'load' の列は数えない = [キー, 値] を load でない列だけキーの順に並べた JSON の sha256 (lib/master-cutover.mjs の ownershipHash と同じ)。
--   なぜ: 記録に無い列は load (④a の epoch はそう読む) なのに、0051 の式は持ち主表の全部の列でハッシュを作っていた = 切替の後に OWNED_COLUMNS に列を
--   足すと (足した列は load)、画面の持ち主表のハッシュが段階の記録 (owner_hash) と違い、保存・登録が before_cutover で止まる。
--   この式なら列を足しても (load のまま) ハッシュは変わらない。使うところ = ⑤-1 の ops.begin_master_write・⑤-2a の ops.register_new_sku・
--   ⑤-2b (0053) の ops.reg_write_gate / ops.open_reg_write・⑦-1 (0054) の ops.amazon_map_begin (どれも段階の記録と画面の持ち主表を比べる = 同じ式で比べる)・下の 1.〜3.
-- 🚨 前の式で記録したハッシュが残っていると比べられない = 段階が company_owner / new_open (owner_hash を記録した後) の DB では作り直さない (止める)。
--   本番は legacy_open (owner_hash は記録していない)・門の記録 (ops.legacy_gate_acks) は動いているプロセスが 15 分ごとに今のコード (同じ式) で書き直す
do $$ begin
  if exists (select 1 from ops.master_cutover_state where phase in ('company_owner', 'new_open')) then
    raise exception '0055: 切替の段階が company_owner / new_open = 段階に記録した持ち主表のハッシュが前の式 = ops.ownership_hash の式を変えられない';
  end if;
end $$;
create or replace function ops.ownership_hash(p jsonb) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select encode(sha256(convert_to('[' || coalesce(string_agg('[' || to_json(t.k)::text || ',' || to_json(t.v)::text || ']', ',' order by t.k collate "C"), '') || ']', 'UTF8')), 'hex')
    from jsonb_each_text(p) as t(k, v)
   where t.v is distinct from 'load'
$$;

-- ─── 持ち主の正を 1 つにする (Codex #1564 R2 High 3) ───
-- active の持ち主表 (記録したまま。記録に無い列 = 'load' = apps/company-db/load/ownership-state.mjs の checkedMap と同じ読み方)。行が無い = 全部 load = {}
create function ops.master_ownership_active_map() returns jsonb
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select coalesce((select s.active_map from ops.master_ownership_state s where s.id = 1), '{}'::jsonb)
$$;
revoke all on function ops.master_ownership_active_map() from public;

-- 持ち主表 p が active と同じか = 1 つの式のハッシュが同じ (load の列は数えない = どちらかに無い列は load と同じ。#1564 Codex R3 Medium)。
--   ⑤-1 の画面の保存・⑤-2a の登録 (ops.master_write_sessions の ownership) と比べる
create function ops.master_ownership_matches_active(p jsonb) returns boolean
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select p is not null and jsonb_typeof(p) = 'object' and ops.ownership_hash(p) = ops.ownership_hash(ops.master_ownership_active_map())
$$;
revoke all on function ops.master_ownership_matches_active(jsonb) from public;

-- 1. 前提 (⑤-1 の差し込み口 ops.master_cutover_prereq_checks に 1 行足す = ops.set_master_cutover_phase が呼ぶ。集める関数は上書きしない):
--    frozen → company_owner (と、その先 = new_open) は、epoch が active になっている (master-ownership-epoch.mjs activate 済み) ときだけ:
--    行がある・prepared が残っていない・active に持ち主が C の列がある・記録のハッシュが中身と同じ。
--    🚨 証拠の owner_hash は前提の関数に渡らない (形が (p_from, p_to)) = owner_hash と active が同じことは 2. の trigger で見る
create function ops.master_ownership_epoch_prereq(p_from text, p_to text) returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[] := '{}';
  r record;
begin
  if p_to in ('company_owner', 'new_open') then
    select active_hash, active_map, prepared_hash into r from ops.master_ownership_state where id = 1;
    if not found then
      v_problems := v_problems || 'epoch_missing: 持ち主の epoch の記録が無い (master-ownership-epoch.mjs prepare → 写し → 作り直し → 確かめ → activate が先)'::text;
    else
      if r.prepared_hash is not null then v_problems := v_problems || 'epoch_prepared_pending: prepared が残っている (activate か cancel が先)'::text; end if;
      if not exists (select 1 from jsonb_each_text(r.active_map) e where e.value = 'company') then
        v_problems := v_problems || 'epoch_all_load: active に持ち主が C の列が無い (activate が先)'::text;
      end if;
      if ops.ownership_hash(r.active_map) is distinct from r.active_hash then v_problems := v_problems || 'epoch_broken: active の記録のハッシュが中身と違う'::text; end if;
    end if;
  end if;
  return v_problems;
end $$;
revoke all on function ops.master_ownership_epoch_prereq(text, text) from public;
insert into ops.master_cutover_prereq_checks (name, fn) values ('0055_ownership_epoch', 'ops.master_ownership_epoch_prereq(text, text)');

-- 2. 段階の行の持ち主表 = active (⑤-1 の ops.master_cutover_state に trigger を足す。⑤-1 の関数は上書きしない):
--    company_owner・new_open に入る・その owner_hash を変える = owner_hash が active の記録のハッシュと同じときだけ (証拠の owner_hash = epoch)。
--    前提の関数 (1.) と同じ答えの保険も見る (set_master_cutover_phase が後の migration で作り直されて差し込み口を呼び忘れても通さない)
create function ops.guard_master_cutover_epoch() returns trigger
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[];
  v_active text;
begin
  if new.phase in ('company_owner', 'new_open') and (new.phase is distinct from old.phase or new.owner_hash is distinct from old.owner_hash) then
    v_problems := ops.master_ownership_epoch_prereq(old.phase, new.phase);
    select active_hash into v_active from ops.master_ownership_state where id = 1;
    if new.owner_hash is distinct from v_active then
      v_problems := v_problems || format('owner_hash_not_active: 段階の持ち主表 (%s) が持ち主の epoch (active %s) と違う', left(coalesce(new.owner_hash, '-'), 12), left(coalesce(v_active, 'なし'), 12));
    end if;
    if coalesce(array_length(v_problems, 1), 0) > 0 then
      raise exception 'cutover_epoch: 段階を % にしない: %', new.phase, array_to_string(v_problems, ' / ') using errcode = 'P0001';
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_cutover_epoch() from public;
-- 名前 = ⑤-1 の守り (trg_master_cutover_state_guard)・⑤-2a の前提 (trg_master_cutover_state_prereq) の後に動く (BEFORE の trigger は名前の順 = 直接の UPDATE は今までどおり ⑤-1 の守りで断る)
create trigger trg_master_cutover_state_prereq_0055 before update on ops.master_cutover_state
  for each row execute function ops.guard_master_cutover_epoch();

-- 3. 画面の保存の門 = active (⑤-1 の ops.begin_master_write が書く ops.master_write_sessions に trigger を足す。begin_master_write は上書きしない):
--    保存を始めるときの持ち主表 (画面が動かしている config) が active と同じ (記録に無い列 = load) ときだけ = 段階の記録 (⑤-1 が見る) と epoch の両方に合う
create function ops.guard_master_write_epoch() returns trigger
  language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if not ops.master_ownership_matches_active(new.ownership) then
    raise exception 'before_cutover: 持ち主表が持ち主の epoch (active) と違う (master-ownership-epoch.mjs status)' using errcode = 'P0001';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_write_epoch() from public;
create trigger trg_master_write_sessions_epoch before insert on ops.master_write_sessions
  for each row execute function ops.guard_master_write_epoch();
