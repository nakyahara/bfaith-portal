-- 0034: 照合の回の記録 + 0032・0033 の関数の直し (2026-09-27。Codex #1481 R1 High・R2 High・#1479 マージ後 Low 2)
--
-- なぜ:
--   ① 判断の画面 (D2') は「今朝の照合に出ている差か」を最新の照合の回で決める。0032 の観測は候補ごとなので、候補が 0 件になった回は残らない
--      = 差が全部消えた朝でも、前の回の候補が「今出ている」に見えて承認できてしまう (Codex #1481 R1 High)
--   ② 0033 の record_ne_baseline は同じ単位の重複を hash の照合に任せていた = 同じ値を先に送ると hash が変わらず、次の別の値が通る (Codex #1479 マージ後 Low 2)
-- なにを:
--   ops.master_compare_runs = 判断の台帳に書けた照合の回 (候補 0 件の回も)。照合が record_decision_candidates を呼ぶと同じ文の中で記録する
--     🚨 blocked・台帳に書けなかった回は入らない = 画面の「今朝の照合」は最後に判定して書けた回のまま (その回の日時を画面に出す)
--   ops.master_ne_baseline.touched_txid = その単位を最後に触った取引。record_ne_baseline は同じ取引で同じ単位を 2 度目に触ったら拒む
--     🚨 一時の表は使わない (security definer の関数が呼び手の作った同じ名前の一時の表・trigger を使うと、持ち主の権限で動かされる。Codex #1481 R2 High)
--   record_decision_candidates = 回を記録する行だけ足す (ほかは 0032 と同じ) / record_ne_baseline = 触った取引の確かめだけ足す (ほかは 0033 と同じ)
--   (create or replace = 持ち主・実行権 (watch_writer) はそのまま)

create table ops.master_compare_runs (
  compare_run_id text primary key check (compare_run_id ~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  observed_at    timestamptz not null,
  candidates     integer not null check (candidates >= 0),
  recorded_at    timestamptz not null default now()
);
create index ix_mcr_observed on ops.master_compare_runs (observed_at desc, compare_run_id desc);
-- これまでの回 (候補がある回だけ = 0 件の回は残っていない) を観測から入れる
insert into ops.master_compare_runs (compare_run_id, observed_at, candidates)
  select compare_run_id, max(observed_at), count(*)::int from ops.master_decision_observations group by compare_run_id
  on conflict (compare_run_id) do nothing;
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then execute 'grant select on ops.master_compare_runs to watcher'; end if;
end $$;

alter table ops.master_ne_baseline add column touched_txid bigint;   -- その単位を最後に触った取引 (同じ取引の 2 度目を拒む)

create or replace function ops.record_decision_candidates(p jsonb) returns integer
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_run text := p ->> 'compare_run_id';
  v_at  timestamptz := (p ->> 'observed_at')::timestamptz;
  d jsonb; v_fp text; n integer := 0;
begin
  if v_run is null or v_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if v_at is null then raise exception 'observed_at が無い' using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'decisions') <> 'array' then raise exception 'decisions が配列でない' using errcode = '22023'; end if;
  -- 照合の回を記録する (候補が 0 件の回も = 「今朝の照合に出ている差か」の基準。候補と同じ文 = 同じ取引。入れ直しで二重にしない。Codex #1481 R1 High)
  insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values (v_run, v_at, jsonb_array_length(p -> 'decisions'))
    on conflict (compare_run_id) do nothing;
  -- 指紋の順に処理する = 候補の行のロックの順をどの呼び手でも同じにする (照合と replay が逆順で取り合ってデッドロックしない。Codex #1475 R3)
  for d in select value from jsonb_array_elements(p -> 'decisions') order by value ->> 'fingerprint' loop
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

create or replace function ops.record_ne_baseline(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  c_version constant integer := 1;    -- この migration が受け付ける正規化の版 (版を上げる migration でここも上げる = 古い照合の書き戻しを拒む)
  c_run_re  constant text := '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$';
  c_ne_at   constant text := '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$';
  v_run text := p ->> 'compare_run_id';
  v_exp text := p ->> 'expected_mark';
  v_nv integer;
  g jsonb := p -> 'generation';
  v_pat timestamptz; v_prev bigint; v_sat timestamptz; v_srev bigint; v_cat timestamptz;
  m ops.master_ne_baseline_mark%rowtype;
  cur ops.master_ne_baseline%rowtype;
  u jsonb; v jsonb; v_code text; v_col text; v_hash text; v_ph text; v_pv integer; v_cver bigint;
  n_ins integer := 0; n_upd integer := 0;
begin
  -- 入力の検証
  if v_run is null or v_run !~ c_run_re then raise exception 'invalid_input: compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if v_exp is not null and v_exp !~ c_run_re then raise exception 'invalid_input: expected_mark の形が違う: %', v_exp using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'norm_version') is distinct from 'number' then raise exception 'invalid_input: norm_version が無い' using errcode = '22023'; end if;
  v_nv := (p ->> 'norm_version')::integer;
  if v_nv <> c_version then raise exception 'norm_version_rejected: この関数が受け付ける版は % (送られた版 %)', c_version, v_nv using errcode = '22023'; end if;
  if jsonb_typeof(g) is distinct from 'object'
     or coalesce(g ->> 'products_at', '') !~ c_ne_at or coalesce(g ->> 'sets_at', '') !~ c_ne_at
     or coalesce(g ->> 'products_rev', '') !~ '^[0-9]{1,18}$' or coalesce(g ->> 'sets_rev', '') !~ '^[0-9]{1,18}$'
     or coalesce(g ->> 'cdb_read_at', '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$' then   -- UTC の ISO だけ ('infinity'・'now'・時間帯なしを拒む)
    raise exception 'invalid_input: generation の形が違う' using errcode = '22023';
  end if;
  begin
    v_pat := ((g ->> 'products_at') || '+00')::timestamptz; v_sat := ((g ->> 'sets_at') || '+00')::timestamptz;
    v_prev := (g ->> 'products_rev')::bigint; v_srev := (g ->> 'sets_rev')::bigint;
    v_cat := (g ->> 'cdb_read_at')::timestamptz;
    if not (isfinite(v_pat) and isfinite(v_sat) and isfinite(v_cat)) then raise exception 'not finite'; end if;
  exception when others then
    raise exception 'invalid_input: generation の時刻・番号が読めない' using errcode = '22023';
  end;
  if jsonb_typeof(p -> 'units') is distinct from 'array' then raise exception 'invalid_input: units が配列でない' using errcode = '22023'; end if;

  -- 直列 (札の行が無い初回も。D2-R1 M1) → 札を照らす
  perform pg_advisory_xact_lock(hashtext('ops.master_ne_baseline'));
  select * into m from ops.master_ne_baseline_mark where id = 1 for update;
  if found then
    if m.compare_run_id = v_run then
      -- 続き (同じ取引で分けて送った後半) だけ。別の取引で同じ ID は再利用 = 拒む (D2-R1 M2)
      if m.accepted_txid <> txid_current() then raise exception 'run_reused: 同じ compare_run_id を別の取引で使った: %', v_run using errcode = 'P0001'; end if;
      if m.prev_run is distinct from v_exp or m.norm_version <> v_nv or m.ne_products_at <> v_pat or m.ne_products_rev <> v_prev
         or m.ne_sets_at <> v_sat or m.ne_sets_rev <> v_srev or m.cdb_read_at <> v_cat then
        raise exception 'continuation_mismatch: 続きの札・世代・版が最初の送りと違う: %', v_run using errcode = 'P0001';
      end if;
    else
      if m.compare_run_id is distinct from v_exp then
        raise exception 'mark_moved: 読んだ札 % の後に % が受け付けられた', coalesce(v_exp, '(なし)'), m.compare_run_id using errcode = 'P0001';
      end if;
      if v_prev < m.ne_products_rev or v_srev < m.ne_sets_rev or v_pat < m.ne_products_at or v_sat < m.ne_sets_at or v_cat < m.cdb_read_at then
        raise exception 'stale_run: 今回の世代が札 % より古い成分がある', m.compare_run_id using errcode = 'P0001';
      end if;
    end if;
  else
    if v_exp is not null then raise exception 'mark_moved: 読んだ札 % が今は無い', v_exp using errcode = 'P0001'; end if;
    if exists (select 1 from ops.master_ne_baseline) then raise exception 'baseline_without_mark: 札が無いのに基準がある (復旧の途中?)' using errcode = 'P0001'; end if;
  end if;

  -- 単位 (code_norm, col の順 = 行ロックの順をそろえる)
  for u in select value from jsonb_array_elements(p -> 'units') order by value ->> 'code_norm', value ->> 'col' loop
    v_code := u ->> 'code_norm'; v_col := u ->> 'col'; v := u -> 'value';
    if jsonb_typeof(u) is distinct from 'object' or coalesce(v_code, '') = '' or v is null
       or v_col is null or v_col not in ('exists', 'kind', 'name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier', 'components') then
      raise exception 'invalid_input: 単位の形が違う (code_norm・col・value)' using errcode = '22023';   -- col が無いと下の OR の連鎖が NULL で素通りする (レビューの Low)
    end if;
    if not (
      (v_col = 'exists' and jsonb_typeof(v) = 'boolean')
      or (v_col = 'kind' and jsonb_typeof(v) = 'string' and v #>> '{}' in ('single', 'set'))
      or (v_col = 'name' and jsonb_typeof(v) in ('string', 'null'))
      or (v_col = 'tax_rate' and jsonb_typeof(v) in ('number', 'null'))
      or (v_col = 'handling' and jsonb_typeof(v) = 'string')
      or (v_col in ('standard_price_jpy', 'cost') and (jsonb_typeof(v) = 'null' or (jsonb_typeof(v) = 'number' and (v #>> '{}')::numeric > 0)))
      or (v_col = 'primary_supplier' and jsonb_typeof(v) = 'array' and not exists (select 1 from jsonb_array_elements(v) x where jsonb_typeof(x) <> 'string'))
      or (v_col = 'components' and jsonb_typeof(v) = 'array' and jsonb_array_length(v) > 0 and not exists (
            select 1 from jsonb_array_elements(v) x where jsonb_typeof(x) <> 'array' or jsonb_array_length(x) <> 2
              or jsonb_typeof(x -> 0) <> 'string' or jsonb_typeof(x -> 1) <> 'number' or (x ->> 1)::numeric <= 0))
    ) then
      raise exception 'invalid_input: 単位 %/% の値の型が違う', v_code, v_col using errcode = '22023';
    end if;
    v_ph := u ->> 'prev_hash';
    v_pv := case when jsonb_typeof(u -> 'prev_version') = 'number' then (u ->> 'prev_version')::integer else null end;
    v_cver := case when jsonb_typeof(u -> 'cdb_version') = 'number' then (u ->> 'cdb_version')::bigint else null end;
    v_hash := encode(sha256(convert_to(v::text, 'UTF8')), 'hex');
    select * into cur from ops.master_ne_baseline where company_id = 1 and code_norm = v_code and col = v_col for update;
    if found then
      -- 同じ取引で 2 度目 (1 回の送りの中・分けた送りの間。同じ値を先に送った後の別の値も) = 拒む (Codex #1479 マージ後 Low 2)
      if cur.touched_txid = txid_current() then raise exception 'unit_conflict: %/% を同じ回で 2 度送った', v_code, v_col using errcode = 'P0001'; end if;
      -- 読んだ時の値と今が違う = 札と単位の整合が破れた (分けた送りの重複・契約の外の更新)。全部巻き戻す (D2-R1 M3)
      if cur.value_hash is distinct from v_ph or cur.norm_version is distinct from v_pv then
        raise exception 'unit_conflict: %/% の基準が読んだ時と違う', v_code, v_col using errcode = 'P0001';
      end if;
      if cur.value_hash = v_hash and cur.norm_version = v_nv then   -- 同じ値・同じ版 = 値は書かない (触った印だけ。送り手は変わった単位だけ送るので普段は来ない)
        update ops.master_ne_baseline set touched_txid = txid_current() where company_id = 1 and code_norm = v_code and col = v_col;
        continue;
      end if;
      update ops.master_ne_baseline set value = v, value_hash = v_hash, norm_version = v_nv, since_run = v_run, since_at = v_cat,
          ne_products_at = v_pat, ne_products_rev = v_prev, ne_sets_at = v_sat, ne_sets_rev = v_srev, cdb_read_at = v_cat, cdb_version = v_cver, updated_at = now(), touched_txid = txid_current()
        where company_id = 1 and code_norm = v_code and col = v_col;
      n_upd := n_upd + 1;
    else
      if v_ph is not null then raise exception 'unit_conflict: %/% の基準が読んだ時にはあったのに今は無い', v_code, v_col using errcode = 'P0001'; end if;
      insert into ops.master_ne_baseline (code_norm, col, value, value_hash, norm_version, since_run, since_at, ne_products_at, ne_products_rev, ne_sets_at, ne_sets_rev, cdb_read_at, cdb_version, touched_txid)
        values (v_code, v_col, v, v_hash, v_nv, v_run, v_cat, v_pat, v_prev, v_sat, v_srev, v_cat, v_cver, txid_current());   -- 同じ単位の 2 回目は同じ取引の中で先の行が見える = 上の unit_conflict で全部巻き戻る
      n_ins := n_ins + 1;
    end if;
  end loop;

  insert into ops.master_ne_baseline_mark (id, compare_run_id, prev_run, accepted_txid, norm_version, ne_products_at, ne_products_rev, ne_sets_at, ne_sets_rev, cdb_read_at, accepted_at)
    values (1, v_run, v_exp, txid_current(), v_nv, v_pat, v_prev, v_sat, v_srev, v_cat, now())
    on conflict (id) do update set compare_run_id = excluded.compare_run_id, prev_run = excluded.prev_run, accepted_txid = excluded.accepted_txid, norm_version = excluded.norm_version,
      ne_products_at = excluded.ne_products_at, ne_products_rev = excluded.ne_products_rev, ne_sets_at = excluded.ne_sets_at, ne_sets_rev = excluded.ne_sets_rev,
      cdb_read_at = excluded.cdb_read_at, accepted_at = excluded.accepted_at;
  return jsonb_build_object('inserted', n_ins, 'updated', n_upd);
end $$;
