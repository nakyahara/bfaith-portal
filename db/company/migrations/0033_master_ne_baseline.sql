-- 0033: マスタ照合 ② の「最後に一致した値」(2026-09-26。Company DB構想 10 §6.1.1「D2 最後に一致した値の契約 v2」+ D2-R1 の実装条件。Codex D2-R0・R1)
--
-- なぜ: 切替 (持ち主を company にする日) の後、毎朝の差が「Company DB 側が変わった (NE への反映待ち)」か「NE 側で変わった (逆流の疑い)」か
--   「両方 (競合)」かを見分けるには、NE と Company DB が最後に一致した値が要る。切替の前から貯める (切替の前は照合の分類を変えない = 影運転)
-- なにを:
--   ops.master_ne_baseline      = 単位 (SKU × 列・構成は親ごとの集合) ごとの、D2 の意味で一致した値。**値が変わった・新しい・正規化の版が違う単位だけ**書き換える
--   ops.master_ne_baseline_mark = 最後に受け付けた照合の回 (世代の最高到達点 = 札)。書く回は、読んだ札が今も同じで世代が 1 成分も後退していないときだけ受け付ける
-- 書く人: 照合 (miniPC・ロール watch_writer) = ops.record_ne_baseline(jsonb) の**実行だけ** (表へ直接は書けない)
-- 🚨 札の検証 (送らなかった単位の並行変更も検出する) / 取引単位の advisory lock (札の行が無い初回も直列) / 続き = 同じ回 ID・同じ取引・同じ前の札・世代・版だけ
-- 🚨 拒む = 例外 (呼び手の取引ごと巻き戻る)。メッセージの頭 = mark_moved / stale_run / unit_conflict / run_reused / continuation_mismatch / baseline_without_mark / norm_version_rejected / invalid_input
-- 復旧 (世代が戻って stale_run が続く): db/company/README.md の手順 (照合を止める → 基準と札を**両方**捨てる → 集め直す)。札だけを下げない

create table ops.master_ne_baseline (
  company_id      smallint not null default 1 check (company_id = 1),
  code_norm       text not null check (length(code_norm) > 0),
  col             text not null check (col in ('exists', 'kind', 'name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier', 'components')),
  value           jsonb not null,                 -- D2 の正規化をした値 (値なし = JSON の null)
  value_hash      text not null check (value_hash ~ '^[0-9a-f]{64}$'),   -- 関数が計算する (呼び手の hash は信じない)
  norm_version    integer not null check (norm_version > 0),
  since_run       text not null check (since_run ~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),   -- その値で一致を初めて見た回 (最後に再確認した回ではない)
  since_at        timestamptz not null,           -- その回の CDB の読みの時刻
  ne_products_at  timestamptz not null,
  ne_products_rev bigint not null check (ne_products_rev >= 0),
  ne_sets_at      timestamptz not null,
  ne_sets_rev     bigint not null check (ne_sets_rev >= 0),
  cdb_read_at     timestamptz not null,
  cdb_version     bigint,                          -- 観測した SKU 行の version (補助の証跡だけ。同じ version でも値は比べる。仕入先・有無は null)
  updated_at      timestamptz not null default now(),
  primary key (company_id, code_norm, col)
);

create table ops.master_ne_baseline_mark (
  id              smallint primary key check (id = 1),
  compare_run_id  text not null check (compare_run_id ~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  prev_run        text,                            -- この回が読んだ札 (続きの確かめに使う)
  accepted_txid   bigint not null,                 -- 受け付けた取引 (続き = 同じ取引だけ)
  norm_version    integer not null check (norm_version > 0),
  ne_products_at  timestamptz not null,
  ne_products_rev bigint not null check (ne_products_rev >= 0),
  ne_sets_at      timestamptz not null,
  ne_sets_rev     bigint not null check (ne_sets_rev >= 0),
  cdb_read_at     timestamptz not null,
  accepted_at     timestamptz not null default now()
);

-- 照合が 1 回分の基準を書く (1 回 = 1 つの取引。5,000 単位ずつ同じ取引で呼んでよい。変更ゼロでも呼ぶ = 札を照らして進める)
-- p = { compare_run_id, expected_mark (読んだ札の compare_run_id | null), norm_version,
--       generation: { products_at, products_rev, sets_at, sets_rev, cdb_read_at },
--       units: [{ code_norm, col, value, cdb_version, prev_hash (読んだ value_hash | null), prev_version (| null) }] }
-- 戻り値 = { inserted, updated }
create function ops.record_ne_baseline(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops as $$
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
     or jsonb_typeof(g -> 'cdb_read_at') is distinct from 'string' then
    raise exception 'invalid_input: generation の形が違う' using errcode = '22023';
  end if;
  begin
    v_pat := ((g ->> 'products_at') || '+00')::timestamptz; v_sat := ((g ->> 'sets_at') || '+00')::timestamptz;
    v_prev := (g ->> 'products_rev')::bigint; v_srev := (g ->> 'sets_rev')::bigint;
    v_cat := (g ->> 'cdb_read_at')::timestamptz;
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
    if jsonb_typeof(u) is distinct from 'object' or coalesce(v_code, '') = '' or v is null then raise exception 'invalid_input: 単位の形が違う' using errcode = '22023'; end if;
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
      -- 読んだ時の値と今が違う = 札と単位の整合が破れた (分けた送りの重複・契約の外の更新)。全部巻き戻す (D2-R1 M3)
      if cur.value_hash is distinct from v_ph or cur.norm_version is distinct from v_pv then
        raise exception 'unit_conflict: %/% の基準が読んだ時と違う', v_code, v_col using errcode = 'P0001';
      end if;
      if cur.value_hash = v_hash and cur.norm_version = v_nv then continue; end if;   -- 同じ値・同じ版 = 書かない
      update ops.master_ne_baseline set value = v, value_hash = v_hash, norm_version = v_nv, since_run = v_run, since_at = v_cat,
          ne_products_at = v_pat, ne_products_rev = v_prev, ne_sets_at = v_sat, ne_sets_rev = v_srev, cdb_read_at = v_cat, cdb_version = v_cver, updated_at = now()
        where company_id = 1 and code_norm = v_code and col = v_col;
      n_upd := n_upd + 1;
    else
      if v_ph is not null then raise exception 'unit_conflict: %/% の基準が読んだ時にはあったのに今は無い', v_code, v_col using errcode = 'P0001'; end if;
      insert into ops.master_ne_baseline (code_norm, col, value, value_hash, norm_version, since_run, since_at, ne_products_at, ne_products_rev, ne_sets_at, ne_sets_rev, cdb_read_at, cdb_version)
        values (v_code, v_col, v, v_hash, v_nv, v_run, v_cat, v_pat, v_prev, v_sat, v_srev, v_cat, v_cver);   -- 同じ単位の 2 回目 = 主キーの違反 = 全部巻き戻る
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

revoke all on function ops.record_ne_baseline(jsonb) from public;
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.record_ne_baseline(jsonb) to watch_writer';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_ne_baseline, ops.master_ne_baseline_mark to watcher';
  end if;
end $$;
