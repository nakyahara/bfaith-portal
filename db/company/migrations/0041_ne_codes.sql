-- 0041: NE のコードの元の書き方 (2026-09-27。Company DB構想 10 §6.1.1「③b-1b NE の元のコードの契約 v3」。Codex ③b-1b-R0・R1)
--
-- なぜ: NE の商品コードは大文字・小文字を区別する (1,150 / 5,008 件に大文字)。私たちの NE の取得はコードを小文字にして保存し、
--   ③b-1 (0040) の CSV は小文字のコードを書く = 大文字のコードの商品で、NE の一括登録が別の商品の新規登録になるおそれ。
-- なにを:
--   ops.master_ne_codes     = norm → NE の元の書き方 (kind = product (商品・セット・セットの子のコード) / rep (代表の名札))。
--                             state = ok (書き方が 1 つ) / collided (2 つ以上) / invalid (使えない文字・壊れた記録)。ok のときだけ ne_code がある
--   ops.master_ne_code_mark = どの照合の回の中身か (1 行)。CSV は「印の回 = 最新の照合の回 = 候補の最後に見た回」のときだけ使う
--   ops.record_ne_codes     = 照合 (miniPC・watch_writer) が 1 回 = 1 つの取引で全部を入れ替えて印を進める。
--     固定の鍵で並べる (初回も)・observed_at は照合の回の表の値・(observed_at, compare_run_id) が今の印より新しいときだけ・
--     同じ回の再送は中身のハッシュ (行の順に依らない) が同じなら何もしない・違えば拒む・入力の (code_norm, kind) の重複は拒む
--   ops.ne_csv_export_rows.ne_code の CHECK を大文字も通すように広げる (CSV に元の書き方を書く)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path の最後に pg_temp (0034 の約束)。public の実行権を外す。watch_writer は実行だけ・watcher は読むだけ

create table ops.master_ne_codes (
  code_norm  text not null check (length(code_norm) > 0),
  kind       text not null check (kind in ('product', 'rep')),
  state      text not null check (state in ('ok', 'collided', 'invalid')),
  ne_code    text,
  spellings  jsonb not null check (jsonb_typeof(spellings) = 'array'),
  primary key (code_norm, kind),
  constraint ck_mnc_state check ((state = 'ok') = (ne_code is not null)),
  constraint ck_mnc_code check (ne_code is null or (ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and lower(ne_code) = code_norm))
);

create table ops.master_ne_code_mark (
  id             smallint primary key default 1 check (id = 1),
  compare_run_id text not null references ops.master_compare_runs (compare_run_id),
  observed_at    timestamptz not null,
  content_hash   text not null check (content_hash ~ '^[0-9a-f]{32}$'),
  counts         jsonb not null,
  recorded_at    timestamptz not null default now()
);

create function ops.record_ne_codes(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_run  text := p ->> 'compare_run_id';
  v_at   timestamptz;
  v_hash text;
  v_n    integer;
  v_dup  integer;
  v_bad  integer;
  m      ops.master_ne_code_mark%rowtype;
  v_counts jsonb;
begin
  -- 書き手を並べる (印が無い初回も。検査の前に。Codex ③b-1b-R1 H2)
  perform pg_advisory_xact_lock(hashtext('ops.ne_codes'));
  if v_run is null or v_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'entries') is distinct from 'array' then raise exception 'invalid_input: entries が配列でない' using errcode = '22023'; end if;
  -- 時刻は照合の回の表の値 (入力の時刻は使わない。Codex ③b-1b-R1 M3)
  select observed_at into v_at from ops.master_compare_runs where compare_run_id = v_run;
  if not found then raise exception 'unknown_run: 照合の回の記録が無い: %', v_run using errcode = '23503'; end if;
  select count(*), count(distinct (x.code_norm, x.kind)),
         count(*) filter (where x.code_norm is null or x.kind is null or x.state is null or x.spellings is null or jsonb_typeof(x.spellings) <> 'array')
    into v_n, v_dup, v_bad
    from jsonb_to_recordset(p -> 'entries') as x(code_norm text, kind text, state text, ne_code text, spellings jsonb);
  if v_bad > 0 then raise exception 'invalid_input: 項目が足りない行が % 行', v_bad using errcode = '22023'; end if;
  if v_dup <> v_n then raise exception 'invalid_input: (code_norm, kind) の重複が % 行', v_n - v_dup using errcode = '22023'; end if;
  -- 中身のハッシュ (行の順に依らない)
  select md5(coalesce(string_agg(x.code_norm || '|' || x.kind || '|' || x.state || '|' || coalesce(x.ne_code, '') || '|' || x.spellings::text, E'\n' order by x.code_norm, x.kind), ''))
    into v_hash
    from jsonb_to_recordset(p -> 'entries') as x(code_norm text, kind text, state text, ne_code text, spellings jsonb);
  select * into m from ops.master_ne_code_mark where id = 1;
  if found then
    if m.compare_run_id = v_run then
      if m.content_hash = v_hash then return jsonb_build_object('state', 'unchanged', 'rows', v_n); end if;
      raise exception 'run_conflict: 同じ回 % の中身が違う', v_run using errcode = '23505';
    end if;
    if (v_at, v_run) <= (m.observed_at, m.compare_run_id) then
      raise exception 'stale_run: % は今の印の回 % より新しくない', v_run, m.compare_run_id using errcode = '22023';
    end if;
  end if;
  delete from ops.master_ne_codes;
  insert into ops.master_ne_codes (code_norm, kind, state, ne_code, spellings)
    select x.code_norm, x.kind, x.state, x.ne_code, x.spellings
      from jsonb_to_recordset(p -> 'entries') as x(code_norm text, kind text, state text, ne_code text, spellings jsonb);
  select jsonb_object_agg(k, n) into v_counts from (select kind || ':' || state as k, count(*) as n from ops.master_ne_codes group by 1) c;
  insert into ops.master_ne_code_mark (id, compare_run_id, observed_at, content_hash, counts, recorded_at)
    values (1, v_run, v_at, v_hash, coalesce(v_counts, '{}'::jsonb), now())
    on conflict (id) do update set compare_run_id = excluded.compare_run_id, observed_at = excluded.observed_at, content_hash = excluded.content_hash,
      counts = excluded.counts, recorded_at = excluded.recorded_at;
  return jsonb_build_object('state', 'written', 'rows', v_n, 'counts', coalesce(v_counts, '{}'::jsonb));
end $$;

revoke all on function ops.record_ne_codes(jsonb) from public;

-- CSV に元の書き方を書く (大文字も)。0040 の列の CHECK (小文字だけ) を広げる
alter table ops.ne_csv_export_rows drop constraint ne_csv_export_rows_ne_code_check;
alter table ops.ne_csv_export_rows add constraint ck_ne_csv_row_ne_code check (ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and lower(ne_code) = code_norm);

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.record_ne_codes(jsonb) to watch_writer';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_ne_codes, ops.master_ne_code_mark to watcher';
  end if;
end $$;
