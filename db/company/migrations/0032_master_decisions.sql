-- 0032: マスタ照合 ② の判断の台帳 (2026-09-26。Company DB構想 10 §6.1.1「D1 判断の台帳の契約 v3」。Codex D-R0・D-R1)
--
-- なぜ: 照合 ② (Company DB ↔ NE) の判断の一覧 (税率の補い・例外原価・セット名の空欄 など) は、毎朝の全件 JSON (miniPC) にしか無い。
--   人が「差を残す」「NE を直す」を決めても残す場所が無い = 毎朝同じ差が「判断待ち」で出続ける。切替の go/no-go の集計もできない。
-- なにを:
--   ops.master_decision_candidates   = 判断の候補 (承認の指紋が主キー)。指紋の元 (print)・選べる解決・意味の版は不変。観測 (最初 / 最後に見た回) は関数だけが進める
--   ops.master_decision_observations = (指紋・照合の回) ごとの観測。入れ直し (replay) で同じ回を二重に数えない・最後に見た日時を巻き戻さない
--   ops.master_decision_events       = 出来事 (追記だけ): approved (解決と、NE / CDB を直すなら目標) / rejected / revoked / action_done (どの approved の完了か)
-- 書く人:
--   照合 (miniPC・ロール watch_writer) = ops.record_decision_candidates(jsonb) と ops.record_decision_done(bigint, text, jsonb) の**実行だけ** (表へ直接は書けない = 承認つきの行を作れない)
--   ポータル (Render・本来のロール) = 出来事の approved / rejected / revoked (D2' の API。認証済みのメールを actor に)
-- 🚨 action_done は「その approved の出来事がまだその指紋の最新の判断」で「まだ完了していない」ときだけ書ける (関数の中で候補の行を for update で確かめる)
-- 🚨 関数は security definer + search_path 固定。public の実行権は外す (scripts/company-db/create-watch-roles.mjs が流し直しても watch_writer の実行権は残す)

create table ops.master_decision_candidates (
  fingerprint     text primary key check (fingerprint ~ '^[0-9a-f]{64}$'),
  company_id      smallint not null default 1,
  subject_key     text not null,
  code_norm       text not null,
  col             text not null,
  child           text,
  cls             text not null,
  reason_kind     text not null,
  semantic        text not null,
  print           jsonb not null check (jsonb_typeof(print) = 'object'),
  resolutions     jsonb not null check (jsonb_typeof(resolutions) = 'array'),
  proposal        jsonb,
  first_seen_run  text not null,
  first_seen_at   timestamptz not null,
  last_seen_run   text not null,
  last_seen_at    timestamptz not null,
  seen_count      integer not null default 1 check (seen_count >= 1),
  recorded_at     timestamptz not null default now()
);
create index ix_mdc_subject on ops.master_decision_candidates (subject_key);
create index ix_mdc_last_seen on ops.master_decision_candidates (last_seen_at);

create table ops.master_decision_observations (
  fingerprint    text not null references ops.master_decision_candidates (fingerprint),
  compare_run_id text not null check (compare_run_id ~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  observed_at    timestamptz not null,
  primary key (fingerprint, compare_run_id)
);

create table ops.master_decision_events (
  event_id          bigserial primary key,
  fingerprint       text not null references ops.master_decision_candidates (fingerprint),
  kind              text not null check (kind in ('approved', 'rejected', 'revoked', 'action_done')),
  resolution        text check (resolution in ('accept_difference', 'fix_ne', 'fix_cdb', 'fix_input', 'spec')),
  target            jsonb,
  approved_event_id bigint references ops.master_decision_events (event_id),
  actor_type        text not null check (actor_type in ('user', 'system')),
  actor             text not null check (length(actor) > 0),
  shown_fingerprint text,
  observed          jsonb,
  note              text,
  created_at        timestamptz not null default now(),
  constraint ck_mde_resolution check ((kind = 'approved') = (resolution is not null)),
  constraint ck_mde_fix_target check (kind <> 'approved' or resolution not in ('fix_ne', 'fix_cdb') or (target is not null and jsonb_typeof(target) = 'object' and target ? 'value')),
  constraint ck_mde_done check ((kind = 'action_done') = (approved_event_id is not null)),
  constraint ck_mde_done_system check (kind <> 'action_done' or actor_type = 'system'),
  constraint ck_mde_user_judgment check (kind = 'action_done' or actor_type = 'user')
);
create unique index ux_mde_done_once on ops.master_decision_events (approved_event_id) where kind = 'action_done';
create index ix_mde_fingerprint on ops.master_decision_events (fingerprint, event_id);

-- 出来事は追記だけ (直さない・消さない = 判断の監査を永久に残す)
create function ops.master_decision_events_append_only() returns trigger language plpgsql as $$
begin
  raise exception 'ops.master_decision_events は追記だけ (%) ', tg_op using errcode = 'P0001';
end $$;
create trigger trg_mde_append_only before update or delete on ops.master_decision_events
  for each row execute function ops.master_decision_events_append_only();

-- 承認の解決は、その候補の選べる解決の中から (画面・API の誤りで「比べられない列を差を残す」などの承認を作らない。照合はそれを前提に案件を閉じる)
create function ops.master_decision_events_resolution_allowed() returns trigger language plpgsql as $$
begin
  if new.kind = 'approved' and new.resolution is not null and not exists (
       select 1 from ops.master_decision_candidates c where c.fingerprint = new.fingerprint and c.resolutions ? new.resolution) then
    raise exception '候補の選べる解決に無い: % (%)', new.resolution, new.fingerprint using errcode = '23514';
  end if;
  return new;
end $$;
create trigger trg_mde_resolution_allowed before insert on ops.master_decision_events
  for each row execute function ops.master_decision_events_resolution_allowed();

-- 候補の不変の部分を守る。最初に見た日時は前へだけ・最後に見た日時は後ろへだけ動く。消さない
create function ops.master_decision_candidates_guard() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ops.master_decision_candidates は消さない' using errcode = 'P0001'; end if;
  if (new.fingerprint, new.company_id, new.subject_key, new.code_norm, new.col, new.child, new.cls, new.reason_kind, new.semantic, new.print, new.resolutions, new.proposal)
     is distinct from (old.fingerprint, old.company_id, old.subject_key, old.code_norm, old.col, old.child, old.cls, old.reason_kind, old.semantic, old.print, old.resolutions, old.proposal) then
    raise exception '判断の候補の不変の列は変えない (%)', old.fingerprint using errcode = 'P0001';
  end if;
  if new.last_seen_at < old.last_seen_at or new.first_seen_at > old.first_seen_at then
    raise exception '観測の日時を巻き戻さない (%)', old.fingerprint using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_mdc_guard before update or delete on ops.master_decision_candidates
  for each row execute function ops.master_decision_candidates_guard();

-- 照合が候補と観測を入れる (冪等)。p = { compare_run_id, observed_at, decisions: [{ fingerprint, subject_key, code_norm, col, child, cls, reason_kind, semantic, print, resolutions, proposal }] }
create function ops.record_decision_candidates(p jsonb) returns integer
  language plpgsql security definer set search_path = pg_catalog, ops as $$
declare
  v_run text := p ->> 'compare_run_id';
  v_at  timestamptz := (p ->> 'observed_at')::timestamptz;
  d jsonb; v_fp text; n integer := 0;
begin
  if v_run is null or v_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if v_at is null then raise exception 'observed_at が無い' using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'decisions') <> 'array' then raise exception 'decisions が配列でない' using errcode = '22023'; end if;
  for d in select value from jsonb_array_elements(p -> 'decisions') loop
    v_fp := d ->> 'fingerprint';
    if v_fp is null or v_fp !~ '^[0-9a-f]{64}$' then raise exception '指紋の形が違う: %', v_fp using errcode = '22023'; end if;
    if jsonb_typeof(d -> 'print') <> 'object' or jsonb_typeof(d -> 'resolutions') <> 'array' then raise exception '候補の形が違う: %', v_fp using errcode = '22023'; end if;
    if exists (select 1 from jsonb_array_elements_text(d -> 'resolutions') r where r not in ('accept_difference', 'fix_ne', 'fix_cdb', 'fix_input', 'spec')) then
      raise exception '知らない解決: %', v_fp using errcode = '22023';
    end if;
    insert into ops.master_decision_candidates (fingerprint, subject_key, code_norm, col, child, cls, reason_kind, semantic, print, resolutions, proposal,
        first_seen_run, first_seen_at, last_seen_run, last_seen_at, seen_count)
      values (v_fp, d ->> 'subject_key', d ->> 'code_norm', d ->> 'col', d ->> 'child', d ->> 'cls', d ->> 'reason_kind', d ->> 'semantic', d -> 'print', d -> 'resolutions', d -> 'proposal',
        v_run, v_at, v_run, v_at, 1)
      on conflict (fingerprint) do nothing;
    -- 同じ指紋を直列にしてから観測を足して数える (並行した照合と replay で見た回数を少なく数えない。Codex #1475 R2)
    perform 1 from ops.master_decision_candidates where fingerprint = v_fp for update;
    insert into ops.master_decision_observations (fingerprint, compare_run_id, observed_at) values (v_fp, v_run, v_at) on conflict do nothing;
    if found then
      update ops.master_decision_candidates c set
          seen_count = (select count(*) from ops.master_decision_observations o where o.fingerprint = v_fp),
          last_seen_run = case when v_at > c.last_seen_at then v_run else c.last_seen_run end,
          last_seen_at = greatest(c.last_seen_at, v_at),
          first_seen_run = case when v_at < c.first_seen_at then v_run else c.first_seen_run end,
          first_seen_at = least(c.first_seen_at, v_at)
        where c.fingerprint = v_fp;
      n := n + 1;
    end if;
  end loop;
  return n;
end $$;

-- 照合が「直す」承認の完了を書く。その approved がまだその指紋の最新の判断で、まだ完了していないときだけ (true = 書いた)
create function ops.record_decision_done(p_approved_event_id bigint, p_compare_run_id text, p_observed jsonb) returns boolean
  language plpgsql security definer set search_path = pg_catalog, ops as $$
declare
  ev ops.master_decision_events%rowtype; v_latest bigint;
begin
  if p_compare_run_id is null or p_compare_run_id !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'compare_run_id の形が違う: %', p_compare_run_id using errcode = '22023'; end if;
  select * into ev from ops.master_decision_events where event_id = p_approved_event_id and kind = 'approved';
  if not found or ev.resolution not in ('fix_ne', 'fix_cdb') then return false; end if;
  -- 観測は承認の目標そのものと照らす (側・単位・値)。食い違い = 呼び手の誤り = 拒む (Codex #1475 R1 Medium)
  if p_observed is null or jsonb_typeof(p_observed) <> 'object' then raise exception '観測が無い' using errcode = '22023'; end if;
  if (p_observed ->> 'side') is distinct from (case ev.resolution when 'fix_ne' then 'ne' else 'cdb' end) then raise exception '観測の側が承認と違う' using errcode = '22023'; end if;
  if (p_observed -> 'subject_key') is distinct from (ev.target -> 'subject_key') or (p_observed -> 'col') is distinct from (ev.target -> 'col')
     or coalesce(p_observed -> 'child', 'null'::jsonb) is distinct from coalesce(ev.target -> 'child', 'null'::jsonb) then
    raise exception '観測の単位が目標と違う' using errcode = '22023';
  end if;
  if (p_observed -> 'value') is distinct from (ev.target -> 'value') then raise exception '観測の値が目標値と違う' using errcode = '22023'; end if;
  -- 同じ指紋の判断を直列にする (ポータルの API も同じ行を for update で取る)
  perform 1 from ops.master_decision_candidates where fingerprint = ev.fingerprint for update;
  select max(event_id) into v_latest from ops.master_decision_events where fingerprint = ev.fingerprint and kind in ('approved', 'rejected', 'revoked');
  if v_latest is distinct from ev.event_id then return false; end if;
  if exists (select 1 from ops.master_decision_events where approved_event_id = ev.event_id and kind = 'action_done') then return false; end if;
  insert into ops.master_decision_events (fingerprint, kind, approved_event_id, actor_type, actor, observed)
    values (ev.fingerprint, 'action_done', ev.event_id, 'system', p_compare_run_id, p_observed);
  return true;
end $$;

revoke all on function ops.record_decision_candidates(jsonb) from public;
revoke all on function ops.record_decision_done(bigint, text, jsonb) from public;
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.record_decision_candidates(jsonb) to watch_writer';
    execute 'grant execute on function ops.record_decision_done(bigint, text, jsonb) to watch_writer';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_decision_candidates, ops.master_decision_observations, ops.master_decision_events to watcher';
  end if;
end $$;
