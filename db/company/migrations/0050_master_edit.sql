-- 0050: マスタ入力画面の土台 (2026-09-30。Company DB構想 14「マスタ入力画面」§3・§6 ⑤-1 / 10 §2・§8。Codex 設計レビュー ⑤-R0・R1 の契約・PR #1563 R1・R2)
--
-- なぜ: 既にある商品・セットの値を Company DB で人が直す画面 (apps/master-edit・Render だけ) を作る。書く先は Company DB だけ。
--   変更の記録と version は 0026 のトリガーが書く。ここで足すのは「切替の段階と、その門」「セットに置く列」「保存 1 回の記録」「セットの構成の依頼・NE の構成の観測・食い違い」
--   「原価の期間の守り」「変更の記録を画面のロールが偽れないようにする直し (0026 の関数)」。
-- なにを:
--   1. 切替の段階 ops.master_cutover_state (1 行): legacy_open (今 = 古い入口が正) → frozen (古い入口を止めた) → company_owner (持ち主が Company DB) → new_open (新しい画面で保存できる)。
--      一方向・1 段ずつ。進めるのは ops.set_master_cutover_phase だけ (security definer・public の実行権なし = 運用のロール master_ops)。記録と証拠は ops.master_cutover_events (追記だけ)。
--      🚨 門 (PR #1563 R2 H2 の契約。⑤-3 は記録を書くだけ・中身の検査はここ):
--        ・動いている場所 (ops.master_cutover_required_hosts() = render・minipc) のプロセスごと (instance_id) が、起動と一定の間隔で ops.record_legacy_gate_ack を呼ぶ
--          (build_id・古い入口の一覧 (manifest) ・持ち主表のハッシュ・見た段階・書きかけの数)。時刻はサーバーの時計。記録は追記だけ。manifest は ops.master_legacy_manifests (ハッシュごとに 1 行)
--        ・段階を進めるとき、新しい記録 (ops.master_cutover_ack_fresh_minutes() 分以内) をプロセスごとの最後の 1 件で見る:
--          全部の場所に 1 件以上・どれも build_id が証拠の expected_builds[場所] の中・manifest_hash と owner_hash が証拠と同じ・見た段階が今の段階。1 つでも外れたら拒む
--          legacy_open → frozen     : + drain (書きかけを流し終えた) + 手の入口 (manifest の kind = manual) を止めた一覧が manifest の手の入口と完全に同じ集合
--          frozen → company_owner   : + 記録が frozen に入った後・書きかけ 0 (新しい持ち主表のハッシュ = 証拠の owner_hash を段階に残す)
--          company_owner → new_open : + 記録が company_owner に入った後・書きかけ 0・owner_hash が company_owner のときと同じ
--        ・差し込み口 ops.master_cutover_prereq_problems(from, to) (既定 = 問題なし)。後の migration (⑤-2a の 0051・⑥ の準備) が create or replace で「まだの項目」を返す = 拒む
--        ⑤-1 では誰も門の記録を書かない (書くのは ⑤-3) = ⑤-3 が配られるまで legacy_open から進めない
--      🚨 読めない = 閉じている (fail-closed。lib/master-cutover.mjs)。新しい画面の保存 = 段階 new_open **かつ** 持ち主表のハッシュが段階の記録と同じ **かつ** 列が company
--         **かつ** env MASTER_EDIT_OPEN = 1。保存の取引は hashtext('ops.master_cutover') の共有の鍵を持つ = 段階を変える取引 (排他の鍵) と並ぶ
--   2. core.skus.set_sales_class_override (1〜4・null) / handling_own ('active' / 'discontinued'・null) = セットに置く人が決めた値。
--      🚨 sku_kind = 'set' だけ、の CHECK は付けない: 夜間ロードが NE の種類替えを写したときに 1 行の CHECK で夜間ロード全体を止めない
--   3. ops.master_edit_requests = 保存 1 回 = 1 行。done は保存と同じ取引・failed は巻き戻った後に同じ request_id の鍵を取って (既に結果があれば書かない)。追記だけ
--   4. セットの構成: core.sku_components = 最後に確かめた構成 (画面は書かない)。ops.sku_component_requests = 依頼 (セットごとに開いているのは 1 つ)。
--      ops.ne_set_observation_runs / ops.ne_set_observations = NE のセットの構成の観測 (追記だけ)。書くのは ops.record_ne_set_observations (security definer・観測のロール master_observer だけ)。
--        完全な回 (complete) = 知らない・セットでない・重なる・並びの分からない・知らない構成品の行が 1 つも無い・求めた数 = 取った数 = 残した数・原本のハッシュと取得の世代がある。
--        観測の時刻は未来 (5 分より先) にしない・36 時間より前にしない
--      ops.sku_component_breaches = 食い違い (NE でやること): mismatch / stale / unrequested_diff / underivable (NE は依頼どおりだが、依頼の構成では導く値が決まらない)
--      依頼を上げるのは lib/master-write.mjs の promoteComponentRequest(観測の番号) だけ (夜間ロード = 持ち主のロール)
--   5. core.sku_costs: 期間の重なりの守り (この画面と昇格の書き込み・持ち主でないロールの書き込み) と、持ち主でないロールは過去の行を消さない・閉じた行を変えない
--      🚨 既知の未達 (PR #1563 R1 M7・R2 M7): 表全体を半開区間 [from, to) にそろえて排他制約 (exclusion) を付けるのは ⑥ 切替の go/no-go の項目 (このPRではしない)。そろえる読み手・書き手 =
--         夜間ロード apps/company-db/load/engine.mjs (§5 の付け替え = greatest(valid_from, 今日 − 1) で閉じる = 同じ日の 2 回で 1 日重なる)・照合 master-compare/compare-load.mjs / compare-ne.mjs・
--         apps/master-decisions/lz-cdb.mjs・apps/company-db/router.mjs・push/sku-cost-observed.mjs・0007 / 0009 の mart (valid_to is null)・0046 v_sku_cost_observed_effective・
--         0049 mart.amazon_profit_* (その日を覆う 1 行)。いまは全部が両端を含む前提で読み・書きしている
--   6. 0026 の変更の記録の関数 (core.audit_master_change・core.bump_parent_version) を security definer にする (PR #1563 R2 M4)。
--      画面のロールに events.master_change_events の insert を渡さない = 記録を偽れない。db_user は「SET ROLE の役 か ログインした役」(持ち主の権限で動いても呼び手を残す)
-- 🚨 この migration は商品の値を何も変えない (列は全部 null で足す・切替の段階は legacy_open から・0026 の関数は同じ記録を書く)

-- 1. 切替の段階と門
create table ops.master_cutover_state (
  id          smallint primary key default 1 check (id = 1),
  phase       text not null check (phase in ('legacy_open', 'frozen', 'company_owner', 'new_open')),
  owner_hash  text check (owner_hash is null or owner_hash ~ '^[0-9a-f]{64}$'),   -- company_owner に進めたときの持ち主表のハッシュ (lib/master-cutover.mjs の ownershipHash)
  changed_at  timestamptz not null default now(),
  changed_by  text not null check (length(changed_by) > 0),
  note        text check (note is null or length(note) <= 500),
  constraint ck_mcs_owner_hash check ((phase in ('company_owner', 'new_open')) = (owner_hash is not null))
);
insert into ops.master_cutover_state (id, phase, changed_by, note) values (1, 'legacy_open', 'migration_0050', '最初 = 古い入口 (NE・/register) が正');
comment on table ops.master_cutover_state is '商品マスタの切替の段階 (0050)。legacy_open → frozen → company_owner → new_open の一方向。変えるのは ops.set_master_cutover_phase だけ';

create table ops.master_cutover_events (
  event_id    bigint generated always as identity primary key,
  from_phase  text not null,
  to_phase    text not null,
  actor       text not null check (length(actor) > 0),
  evidence    jsonb not null check (jsonb_typeof(evidence) = 'object'),
  acks        jsonb not null check (jsonb_typeof(acks) = 'array'),   -- 門を確かめたときに使った記録 (ack_id・host・instance_id・build_id)
  note        text check (note is null or length(note) <= 500),
  changed_at  timestamptz not null default now()
);
select core.make_append_only('ops', 'master_cutover_events');

-- 古い入口の一覧 (⑤-3 の機械で読める一覧)。{ entries: [{ id, kind: 'code' | 'manual', ... }] }。kind = manual = コードでは閉じられない入口 (人が止める)
create table ops.master_legacy_manifests (
  manifest_hash text primary key check (manifest_hash ~ '^[0-9a-f]{64}$'),
  entries       jsonb not null check (jsonb_typeof(entries) = 'object'),
  recorded_at   timestamptz not null default now()
);
select core.make_append_only('ops', 'master_legacy_manifests');

-- プロセスごとの「古い入口の門を持つ版で動いている」記録 (⑤-3 が起動と一定の間隔で書く。⑤-1 では誰も書かない)
create table ops.master_legacy_gate_acks (
  ack_id             bigint generated always as identity primary key,
  host               text not null check (host in ('render', 'minipc')),
  instance_id        text not null check (instance_id ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  build_id           text not null check (length(build_id) between 1 and 100),
  manifest_hash      text not null references ops.master_legacy_manifests (manifest_hash),
  owner_hash         text not null check (owner_hash ~ '^[0-9a-f]{64}$'),
  phase_seen         text not null check (phase_seen in ('legacy_open', 'frozen', 'company_owner', 'new_open')),
  inflight_count     integer not null check (inflight_count >= 0),   -- 古い入口の書きかけ (受けたが終わっていない書き込み) の数
  oldest_inflight_at timestamptz,
  acked_at           timestamptz not null default clock_timestamp(),
  constraint ck_mlga_inflight check ((inflight_count = 0) = (oldest_inflight_at is null))
);
create index ix_master_legacy_gate_acks_host on ops.master_legacy_gate_acks (host, instance_id, acked_at desc, ack_id desc);
select core.make_append_only('ops', 'master_legacy_gate_acks');
comment on table ops.master_legacy_gate_acks is 'プロセスごとの門の記録 (0050。書くのは ⑤-3 = ops.record_legacy_gate_ack だけ)。切替の段階を進める門が読む';

-- 門の設定 (⑤-3・⑥ で変えるときは create or replace)
create function ops.master_cutover_required_hosts() returns text[] language sql immutable as $$ select array['minipc', 'render']::text[] $$;
create function ops.master_cutover_ack_fresh_minutes() returns integer language sql immutable as $$ select 15 $$;

-- 段階を進める前にそろっているべきもの (差し込み口)。既定 = 問題なし。後の migration が create or replace で「まだの項目」を返す (空でなければ段階を進めない)
create function ops.master_cutover_prereq_problems(p_from text, p_to text) returns text[]
  language plpgsql stable security definer set search_path = pg_catalog, ops, pg_temp as $$
begin
  return array[]::text[];
end $$;
revoke all on function ops.master_cutover_prereq_problems(text, text) from public;

create function ops.guard_master_cutover_state() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'master_cutover_state は消さない' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.cutover_protocol', true), '') is distinct from '1' then
    raise exception '切替の段階は ops.set_master_cutover_phase でだけ変える' using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_master_cutover_state_guard before update or delete on ops.master_cutover_state
  for each row execute function ops.guard_master_cutover_state();

-- 証拠の時刻が読めるか (読めなければ false)
create function ops.cutover_is_ts(p text) returns boolean language plpgsql stable set search_path = pg_catalog, pg_temp as $$
begin
  if p is null or length(p) = 0 then return false; end if;
  perform p::timestamptz;
  return true;
exception when others then
  return false;
end $$;

-- 古い入口の一覧の形 (entries = 1 つ以上・id は一意・kind = code / manual)。正しければハッシュ、違えば例外
create function ops.legacy_manifest_hash(p_manifest jsonb) returns text language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
begin
  if p_manifest is null or jsonb_typeof(p_manifest) <> 'object' or jsonb_typeof(p_manifest -> 'entries') is distinct from 'array' or jsonb_array_length(p_manifest -> 'entries') = 0 then
    raise exception 'invalid_manifest: { entries: [{ id, kind }] } (1 つ以上) が要る' using errcode = '22023';
  end if;
  if exists (select 1 from jsonb_array_elements(p_manifest -> 'entries') x
              where jsonb_typeof(x) <> 'object' or coalesce(x ->> 'id', '') !~ '^[A-Za-z0-9_.:/-]{1,120}$' or coalesce(x ->> 'kind', '') not in ('code', 'manual')) then
    raise exception 'invalid_manifest: 各行に id (英数字と _.:/-) と kind (code / manual) が要る' using errcode = '22023';
  end if;
  if (select count(*) <> count(distinct x ->> 'id') from jsonb_array_elements(p_manifest -> 'entries') x) then
    raise exception 'invalid_manifest: id が重なっている' using errcode = '22023';
  end if;
  return encode(sha256(convert_to(p_manifest::text, 'UTF8')), 'hex');
end $$;

-- 門の記録を 1 件書く (⑤-3 の古い入口の門が呼ぶ)。時刻はサーバーの時計・見た段階は今の段階と同じでないと拒む・manifest はハッシュごとに 1 回だけ残す
-- 🚨 security definer: 呼ぶロール (master_gate) に表の書き込みの権限を渡さない。一時の表を使わない・search_path の最後に pg_temp
create function ops.record_legacy_gate_ack(p_host text, p_instance_id text, p_build_id text, p_manifest jsonb, p_owner_hash text, p_phase_seen text,
                                           p_inflight_count integer, p_oldest_inflight_at timestamptz) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_hash  text := ops.legacy_manifest_hash(p_manifest);
  v_phase text;
  v_id    bigint;
  v_at    timestamptz;
begin
  select phase into v_phase from ops.master_cutover_state where id = 1;
  if p_phase_seen is distinct from v_phase then
    raise exception 'stale_phase: 見た段階 % が今の段階 % と違う (段階を読み直してから書く)', p_phase_seen, v_phase using errcode = 'P0001';
  end if;
  insert into ops.master_legacy_manifests (manifest_hash, entries) values (v_hash, p_manifest) on conflict (manifest_hash) do nothing;
  insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at)
    values (p_host, p_instance_id, p_build_id, v_hash, p_owner_hash, p_phase_seen, p_inflight_count, p_oldest_inflight_at)
    returning ack_id, acked_at into v_id, v_at;
  return jsonb_build_object('ack_id', v_id, 'manifest_hash', v_hash, 'acked_at', v_at);
end $$;
revoke all on function ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz) from public;

-- 1 段だけ進める (飛ばさない・戻さない・証拠と全部の場所の新しい記録が要る)。鍵 (排他) → 行 (for update) → 差し込み口 → 証拠 → 門 → 状態 → 記録
-- 証拠 = { expected_builds: { render: [...], minipc: [...] }, manifest_hash, owner_hash, (→ frozen だけ) drain: { done, checked_by, checked_at }, manual_entries_stopped: [{ id, by, at }] }
-- 🚨 security definer: 呼ぶ人に表の書き込みの権限を渡さない (運用のロール master_ops に実行だけ)。一時の表を使わない・search_path の最後に pg_temp (0034 の約束)
create function ops.set_master_cutover_phase(p_to text, p_actor text, p_evidence jsonb, p_note text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_order   text[] := array['legacy_open', 'frozen', 'company_owner', 'new_open'];
  v_from    text;
  v_since   timestamptz;
  v_hash    text;
  v_hosts   text[] := ops.master_cutover_required_hosts();
  v_fresh   interval := make_interval(mins => ops.master_cutover_ack_fresh_minutes());
  v_now     timestamptz := clock_timestamp();
  v_manifest text;
  v_owner   text;
  v_entries jsonb;
  v_builds  jsonb;
  v_acks    jsonb := '[]'::jsonb;
  v_problems text[] := '{}';
  v_host    text;
  v_n       integer;
  v_want    text[];
  v_got     text[];
  a         record;
  e         jsonb;
begin
  perform pg_advisory_xact_lock(hashtext('ops.master_cutover'));
  if p_actor is null or length(p_actor) = 0 then raise exception 'invalid_input: 誰が変えたか (actor) が要る' using errcode = '22023'; end if;
  if not (p_to = any(v_order)) then raise exception 'invalid_input: 知らない段階 %', p_to using errcode = '22023'; end if;
  if p_evidence is null or jsonb_typeof(p_evidence) <> 'object' then raise exception 'evidence_required: 証拠 (jsonb の object) が要る' using errcode = '22023'; end if;
  select phase, changed_at, owner_hash into v_from, v_since, v_hash from ops.master_cutover_state where id = 1 for update;
  if not found then raise exception 'no_state: 切替の段階の行が無い' using errcode = 'P0001'; end if;
  if array_position(v_order, p_to) <> array_position(v_order, v_from) + 1 then
    raise exception 'one_way: 段階は % から % に進めない (1 段ずつ・戻さない)', v_from, p_to using errcode = 'P0001';
  end if;
  -- 差し込み口 (後の migration の「まだの項目」)
  v_problems := coalesce(ops.master_cutover_prereq_problems(v_from, p_to), '{}');
  if array_length(v_problems, 1) > 0 then
    raise exception 'prereq_failed: %', array_to_string(v_problems, ' / ') using errcode = 'P0001';
  end if;

  -- 証拠の共通の形
  v_manifest := p_evidence ->> 'manifest_hash';
  select entries into v_entries from ops.master_legacy_manifests where manifest_hash = v_manifest;
  if not found then raise exception 'evidence_invalid: manifest_hash が記録された古い入口の一覧に無い' using errcode = '22023'; end if;
  v_owner := p_evidence ->> 'owner_hash';
  if v_owner is null or v_owner !~ '^[0-9a-f]{64}$' then raise exception 'evidence_invalid: owner_hash (64 桁の 16 進) が要る' using errcode = '22023'; end if;
  if p_to = 'new_open' and v_owner is distinct from v_hash then raise exception 'evidence_invalid: owner_hash が company_owner のときと違う' using errcode = '22023'; end if;
  v_builds := p_evidence -> 'expected_builds';
  if v_builds is null or jsonb_typeof(v_builds) <> 'object' then raise exception 'evidence_invalid: expected_builds (場所ごとの build_id の一覧) が要る' using errcode = '22023'; end if;
  foreach v_host in array v_hosts loop
    if jsonb_typeof(v_builds -> v_host) is distinct from 'array' or jsonb_array_length(v_builds -> v_host) = 0
       or exists (select 1 from jsonb_array_elements(v_builds -> v_host) x where jsonb_typeof(x) <> 'string') then
      raise exception 'evidence_invalid: expected_builds.% (build_id の文字の配列・1 つ以上) が要る', v_host using errcode = '22023';
    end if;
  end loop;

  if p_to = 'frozen' then
    -- drain = 書きかけ (古い入口の途中の処理・送り待ち) を流し終えた
    e := p_evidence -> 'drain';
    if e is null or jsonb_typeof(e) <> 'object' or (e ->> 'done') is distinct from 'true' or coalesce(length(e ->> 'checked_by'), 0) = 0 or not ops.cutover_is_ts(e ->> 'checked_at') then
      raise exception 'evidence_invalid: drain = { done: true, checked_by, checked_at } が要る' using errcode = '22023';
    end if;
    -- 手の入口を止めた一覧 = manifest の手の入口 (kind = manual) と完全に同じ集合
    e := p_evidence -> 'manual_entries_stopped';
    if e is null or jsonb_typeof(e) <> 'array' then raise exception 'evidence_invalid: manual_entries_stopped = [{ id, by, at }, ...] が要る' using errcode = '22023'; end if;
    if exists (select 1 from jsonb_array_elements(e) x
                where jsonb_typeof(x) <> 'object' or coalesce(length(x ->> 'id'), 0) = 0 or coalesce(length(x ->> 'by'), 0) = 0 or not ops.cutover_is_ts(x ->> 'at')) then
      raise exception 'evidence_invalid: manual_entries_stopped の各行に id・by・at が要る' using errcode = '22023';
    end if;
    select coalesce(array_agg(x ->> 'id' order by x ->> 'id'), '{}'), count(*) into v_got, v_n from jsonb_array_elements(e) x;
    if v_n <> (select count(distinct x ->> 'id') from jsonb_array_elements(e) x) then raise exception 'evidence_invalid: manual_entries_stopped の id が重なっている' using errcode = '22023'; end if;
    select coalesce(array_agg(x ->> 'id' order by x ->> 'id'), '{}') into v_want from jsonb_array_elements(v_entries -> 'entries') x where x ->> 'kind' = 'manual';
    if v_got is distinct from v_want then
      raise exception 'evidence_invalid: 止めた手の入口 (%) が古い入口の一覧の手の入口 (%) と同じでない', array_to_string(v_got, ', '), array_to_string(v_want, ', ') using errcode = '22023';
    end if;
  end if;

  -- 門: 場所ごと・プロセスごとの最後の新しい記録
  v_problems := '{}';
  foreach v_host in array v_hosts loop
    v_n := 0;
    for a in select distinct on (k.instance_id) k.* from ops.master_legacy_gate_acks k
               where k.host = v_host and k.acked_at >= v_now - v_fresh
               order by k.instance_id, k.acked_at desc, k.ack_id desc loop
      v_n := v_n + 1;
      if not ((v_builds -> v_host) ? a.build_id) then v_problems := array_append(v_problems, format('%s/%s: 予定に無い build %s が動いている', v_host, a.instance_id, a.build_id)); end if;
      if a.manifest_hash is distinct from v_manifest then v_problems := array_append(v_problems, format('%s/%s: 古い入口の一覧が違う', v_host, a.instance_id)); end if;
      if a.owner_hash is distinct from v_owner then v_problems := array_append(v_problems, format('%s/%s: 持ち主表のハッシュが違う', v_host, a.instance_id)); end if;
      if a.phase_seen is distinct from v_from then v_problems := array_append(v_problems, format('%s/%s: 見た段階が %s (今は %s)', v_host, a.instance_id, a.phase_seen, v_from)); end if;
      if p_to <> 'frozen' and a.acked_at <= v_since then v_problems := array_append(v_problems, format('%s/%s: 記録が今の段階に入る前', v_host, a.instance_id)); end if;
      if p_to <> 'frozen' and a.inflight_count <> 0 then v_problems := array_append(v_problems, format('%s/%s: 書きかけが %s 件ある', v_host, a.instance_id, a.inflight_count)); end if;
      v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', a.ack_id, 'host', a.host, 'instance_id', a.instance_id, 'build_id', a.build_id, 'acked_at', a.acked_at));
    end loop;
    if v_n = 0 then v_problems := array_append(v_problems, format('%s: %s 分以内の記録が無い', v_host, ops.master_cutover_ack_fresh_minutes())); end if;
  end loop;
  if array_length(v_problems, 1) > 0 then
    raise exception 'acks_invalid: %', array_to_string(v_problems, ' / ') using errcode = 'P0001';
  end if;

  perform pg_catalog.set_config('ops.cutover_protocol', '1', true);
  update ops.master_cutover_state set phase = p_to, changed_at = now(), changed_by = p_actor, note = p_note,
         owner_hash = case when p_to = 'frozen' then null else v_owner end
   where id = 1;
  perform pg_catalog.set_config('ops.cutover_protocol', '', true);
  insert into ops.master_cutover_events (from_phase, to_phase, actor, evidence, acks, note) values (v_from, p_to, p_actor, p_evidence, v_acks, p_note);
  return jsonb_build_object('from', v_from, 'to', p_to, 'acks', v_acks);
end $$;
revoke all on function ops.set_master_cutover_phase(text, text, jsonb, text) from public;

-- 2. セットに置く列
alter table core.skus
  add column set_sales_class_override smallint check (set_sales_class_override between 1 and 4),
  add column handling_own text check (handling_own in ('active', 'discontinued'));
comment on column core.skus.set_sales_class_override is 'セットの売上分類を人が決めた値 (1〜4)。null = 構成品から導く (lib/master-set-rules.js)。0050';
comment on column core.skus.handling_own is 'セット自身の取扱区分 (人が決めた値)。skus.handling はセット自身 + 構成品から導いた値。null = まだ人が決めていない。0050';

-- 3. 保存 1 回の記録 (追記だけ)
create table ops.master_edit_requests (
  request_id   uuid primary key,
  company_id   smallint not null default 1 references core.companies,
  operation    text not null check (operation in ('sku_edit')),
  target_code  text not null check (length(target_code) between 1 and 60),
  sku_id       bigint references core.skus (sku_id),
  actor_id     text not null check (length(actor_id) between 1 and 320),
  payload_hash text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  status       text not null check (status in ('done', 'failed')),
  result       jsonb check (result is null or jsonb_typeof(result) = 'object'),
  error        jsonb check (error is null or jsonb_typeof(error) = 'object'),
  started_at   timestamptz not null,
  finished_at  timestamptz not null default clock_timestamp(),
  constraint ck_mer_done check ((status = 'done') = (result is not null)),
  constraint ck_mer_failed check ((status = 'failed') = (error is not null)),
  constraint ck_mer_time check (finished_at >= started_at)
);
create index ix_master_edit_requests_sku on ops.master_edit_requests (sku_id, started_at desc);
select core.make_append_only('ops', 'master_edit_requests');
comment on table ops.master_edit_requests is 'マスタ入力画面の保存 1 回 = 1 行 (0050)。done は保存と同じ取引・failed は巻き戻った後。追記だけ';

-- 4. セットの構成: NE の観測 → 依頼 → 食い違い
create table ops.ne_set_observation_runs (
  run_id            text primary key check (run_id ~ '^[A-Za-z0-9_.:-]{1,80}$'),
  observed_at       timestamptz not null,   -- NE から取った時刻
  complete          boolean not null,       -- NE のセットを全部取れた回か
  requested_count   integer check (requested_count >= 0),   -- NE に求めたセットの数
  fetched_count     integer check (fetched_count >= 0),     -- NE から返ってきたセットの数
  saved_count       integer not null check (saved_count >= 0),   -- ここに残したセットの数
  skipped_count     integer not null check (skipped_count >= 0), -- 知らない・セットでない・重なるので残さなかった数
  raw_hash          text check (raw_hash is null or raw_hash ~ '^[0-9a-f]{64}$'),   -- NE の原本 (取得の結果そのもの) のハッシュ
  source_generation text check (source_generation is null or length(source_generation) between 1 and 100),   -- 取得の世代 (NE の取得の回・完了の印)
  content_hash      text not null check (content_hash ~ '^[0-9a-f]{32}$'),
  recorded_at       timestamptz not null default now(),
  constraint ck_nsor_complete check (not complete or (requested_count is not null and requested_count = fetched_count and fetched_count = saved_count
                                                     and skipped_count = 0 and raw_hash is not null and source_generation is not null))
);
select core.make_append_only('ops', 'ne_set_observation_runs');

create table ops.ne_set_observations (
  observation_id bigint generated always as identity primary key,
  run_id         text not null references ops.ne_set_observation_runs (run_id),
  set_sku_id     bigint not null references core.skus (sku_id),
  rows           jsonb not null check (jsonb_typeof(rows) = 'array' and jsonb_array_length(rows) <= 100),   -- [{sku_id (知らないコード = null), code, qty, sort}] 並び = sort の順
  unique (run_id, set_sku_id)
);
create index ix_ne_set_observations_set on ops.ne_set_observations (set_sku_id, observation_id desc);
select core.make_append_only('ops', 'ne_set_observations');
comment on table ops.ne_set_observations is 'NE のセットの構成の観測 (0050)。書くのは ops.record_ne_set_observations (観測のロール master_observer だけ)。追記だけ';

create table ops.sku_component_requests (
  component_request_id bigint generated always as identity primary key,
  company_id      smallint not null default 1 references core.companies,
  set_sku_id      bigint not null references core.skus (sku_id),
  rows            jsonb not null check (jsonb_typeof(rows) = 'array' and jsonb_array_length(rows) between 1 and 20),   -- [{sku_id, code, qty, sort}] sort = 1〜n
  rows_hash       text not null check (rows_hash ~ '^[0-9a-f]{64}$'),
  base_rows       jsonb not null check (jsonb_typeof(base_rows) = 'array'),   -- 依頼したときの core.sku_components [{sku_id, code, qty, sort}]
  reason          text check (reason is null or length(reason) <= 200),
  requested_by    text not null check (length(requested_by) > 0),
  edit_request_id uuid not null,   -- ops.master_edit_requests の request_id (同じ取引の最後に書くので FK は付けない)
  status          text not null default 'open' check (status in ('open', 'applied', 'cancelled')),
  created_at      timestamptz not null default now(),
  closed_at       timestamptz,
  closed_by       text,
  close_reason    text check (close_reason in ('matched', 'withdrawn', 'superseded')),
  applied_observation_id bigint references ops.ne_set_observations (observation_id),
  constraint ck_scr_open check ((status = 'open') = (closed_at is null) and (status = 'open') = (close_reason is null) and (status = 'open') = (closed_by is null)),
  constraint ck_scr_applied check ((status = 'applied') = (close_reason = 'matched') and (status = 'applied') = (applied_observation_id is not null))
);
create unique index ux_sku_component_requests_open on ops.sku_component_requests (set_sku_id) where status = 'open';
create index ix_sku_component_requests_set on ops.sku_component_requests (set_sku_id, created_at desc);
comment on table ops.sku_component_requests is 'セットの構成を変えたい依頼 (0050)。画面は core.sku_components を書かない。NE の観測が同じになったら lib/master-write.mjs の promoteComponentRequest が上げて閉じる';

create function ops.guard_sku_component_requests() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'sku_component_requests は消さない (取り下げは cancelled)' using errcode = 'P0001'; end if;
  if (new.component_request_id, new.company_id, new.set_sku_id, new.rows, new.rows_hash, new.base_rows, new.reason, new.requested_by, new.edit_request_id, new.created_at)
     is distinct from (old.component_request_id, old.company_id, old.set_sku_id, old.rows, old.rows_hash, old.base_rows, old.reason, old.requested_by, old.edit_request_id, old.created_at) then
    raise exception 'sku_component_requests の依頼の中身は書き換えない (閉じて新しく依頼する)' using errcode = 'P0001';
  end if;
  if old.status <> 'open' then raise exception '閉じた依頼 (%) は変えない', old.status using errcode = 'P0001'; end if;
  if new.status = 'applied' and not exists (select 1 from ops.ne_set_observations o where o.observation_id = new.applied_observation_id and o.set_sku_id = new.set_sku_id) then
    raise exception '依頼を上げる観測が同じセットのものでない' using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_sku_component_requests_guard before update or delete on ops.sku_component_requests
  for each row execute function ops.guard_sku_component_requests();

create table ops.sku_component_breaches (
  breach_id            bigint generated always as identity primary key,
  set_sku_id           bigint not null references core.skus (sku_id),
  kind                 text not null check (kind in ('mismatch', 'stale', 'unrequested_diff', 'underivable')),
  observation_id       bigint not null references ops.ne_set_observations (observation_id),
  component_request_id bigint references ops.sku_component_requests (component_request_id),
  details              jsonb not null check (jsonb_typeof(details) = 'object'),   -- { observed: [...], expected: [...], blockers?: [...] }
  details_hash         text not null check (details_hash ~ '^[0-9a-f]{64}$'),
  status               text not null default 'open' check (status in ('open', 'closed')),
  created_at           timestamptz not null default now(),
  closed_at            timestamptz,
  closed_by            text,
  close_reason         text check (close_reason in ('resolved', 'superseded', 'dismissed')),
  constraint ck_scb_request check ((kind = 'unrequested_diff') = (component_request_id is null)),
  constraint ck_scb_open check ((status = 'open') = (closed_at is null) and (status = 'open') = (close_reason is null) and (status = 'open') = (closed_by is null))
);
create unique index ux_sku_component_breaches_open on ops.sku_component_breaches (set_sku_id, kind) where status = 'open';
comment on table ops.sku_component_breaches is 'セットの構成の食い違い = NE でやること (0050)。mismatch / stale / unrequested_diff / underivable。閉じ方 = resolved / superseded / dismissed';

create function ops.guard_sku_component_breaches() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'sku_component_breaches は消さない (閉じる)' using errcode = 'P0001'; end if;
  if (new.breach_id, new.set_sku_id, new.kind, new.observation_id, new.component_request_id, new.details, new.details_hash, new.created_at)
     is distinct from (old.breach_id, old.set_sku_id, old.kind, old.observation_id, old.component_request_id, old.details, old.details_hash, old.created_at) then
    raise exception 'sku_component_breaches の中身は書き換えない (閉じて新しく残す)' using errcode = 'P0001';
  end if;
  if old.status <> 'open' then raise exception '閉じた食い違いは変えない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_sku_component_breaches_guard before update or delete on ops.sku_component_breaches
  for each row execute function ops.guard_sku_component_breaches();

-- NE のセットの構成の観測を 1 回分書く (観測のロール master_observer だけ・⑤-2 で夜間ロードから呼ぶ)。
-- p = { run_id, observed_at, complete, requested, fetched, raw_hash, source_generation, sets: [{ set_code, rows: [{ code, qty, sort }] }] }
-- 完全な回 (complete = true) は、知らないセット・セットでない SKU・重なるセット・行の形の誤り・重なる構成品・重なる並び・知らない構成品を 1 つも許さない (拒む)。
--   完全でない回は、残せないセットを飛ばして数える (上げる根拠には使わない)。同じ run_id の再送 = 中身が同じなら何もしない・違えば拒む
-- 🚨 security definer (呼ぶロールに表の書き込みの権限を渡さない)。一時の表を使わない・search_path の最後に pg_temp
create function ops.record_ne_set_observations(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_run      text := p ->> 'run_id';
  v_complete boolean;
  v_at       timestamptz;
  v_hash     text;
  v_prev     text;
  v_saved    integer := 0;
  v_skip     integer := 0;
  v_seen     bigint[] := '{}';
  s          jsonb;
  v_set      bigint;
  v_rows     jsonb;
  v_bad      text;
begin
  if v_run is null or v_run !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if not ops.cutover_is_ts(p ->> 'observed_at') then raise exception 'invalid_input: observed_at が読めない' using errcode = '22023'; end if;
  v_at := (p ->> 'observed_at')::timestamptz;
  if v_at > clock_timestamp() + interval '5 minutes' then raise exception 'invalid_input: observed_at が未来' using errcode = '22023'; end if;
  if v_at < clock_timestamp() - interval '36 hours' then raise exception 'invalid_input: observed_at が古すぎる (36 時間より前)' using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'complete') is distinct from 'boolean' then raise exception 'invalid_input: complete (true / false) が要る' using errcode = '22023'; end if;
  v_complete := (p ->> 'complete')::boolean;
  if jsonb_typeof(p -> 'sets') is distinct from 'array' then raise exception 'invalid_input: sets が配列でない' using errcode = '22023'; end if;
  if v_complete then
    if jsonb_typeof(p -> 'requested') is distinct from 'number' or jsonb_typeof(p -> 'fetched') is distinct from 'number'
       or (p ->> 'requested')::integer <> jsonb_array_length(p -> 'sets') or (p ->> 'fetched')::integer <> jsonb_array_length(p -> 'sets') then
      raise exception 'invalid_input: 完全な回は requested = fetched = sets の数 が要る' using errcode = '22023';
    end if;
    if coalesce(p ->> 'raw_hash', '') !~ '^[0-9a-f]{64}$' or coalesce(length(p ->> 'source_generation'), 0) = 0 then
      raise exception 'invalid_input: 完全な回は raw_hash (原本のハッシュ) と source_generation (取得の世代) が要る' using errcode = '22023';
    end if;
  end if;
  v_hash := md5(p::text);
  perform pg_advisory_xact_lock(hashtext('ops.ne_set_observations:' || v_run));
  select content_hash into v_prev from ops.ne_set_observation_runs where run_id = v_run;
  if found then
    if v_prev = v_hash then return jsonb_build_object('state', 'unchanged', 'run_id', v_run); end if;
    raise exception 'run_conflict: 同じ回 % の中身が違う', v_run using errcode = '23505';
  end if;
  -- 先に回の行 (数は後で入れられない = 追記だけ) を作るため、セットを先に確かめて数える
  for s in select * from jsonb_array_elements(p -> 'sets') loop
    v_bad := null;
    select k.sku_id into v_set from core.skus k where k.code_norm = core.norm_code(s ->> 'set_code') and k.sku_kind = 'set';
    if not found then v_bad := format('知らないセット・セットでない %s', s ->> 'set_code');
    elsif v_set = any(v_seen) then v_bad := format('同じセットが 2 回 %s', s ->> 'set_code');
    elsif jsonb_typeof(s -> 'rows') is distinct from 'array' then v_bad := format('行が配列でない %s', s ->> 'set_code');
    elsif exists (select 1 from jsonb_array_elements(s -> 'rows') r
                   where coalesce(length(r ->> 'code'), 0) = 0 or jsonb_typeof(r -> 'qty') is distinct from 'number' or jsonb_typeof(r -> 'sort') is distinct from 'number') then
      v_bad := format('行の形が違う %s', s ->> 'set_code');
    elsif v_complete and ((select count(*) <> count(distinct core.norm_code(r ->> 'code')) or count(*) <> count(distinct (r ->> 'sort')::numeric) from jsonb_array_elements(s -> 'rows') r)
                          or exists (select 1 from jsonb_array_elements(s -> 'rows') r where not exists (select 1 from core.skus k where k.code_norm = core.norm_code(r ->> 'code')))) then
      v_bad := format('構成品・並びが重なる / 知らない構成品 %s', s ->> 'set_code');
    end if;
    if v_bad is not null then
      if v_complete then raise exception 'invalid_input: 完全な回に残せないセットがある: %', v_bad using errcode = '22023'; end if;
      v_skip := v_skip + 1;
      continue;
    end if;
    v_seen := array_append(v_seen, v_set);
  end loop;
  insert into ops.ne_set_observation_runs (run_id, observed_at, complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation, content_hash)
    values (v_run, v_at, v_complete, (p ->> 'requested')::integer, (p ->> 'fetched')::integer, coalesce(array_length(v_seen, 1), 0), v_skip,
            nullif(p ->> 'raw_hash', ''), nullif(p ->> 'source_generation', ''), v_hash);
  v_seen := '{}';
  for s in select * from jsonb_array_elements(p -> 'sets') loop
    select k.sku_id into v_set from core.skus k where k.code_norm = core.norm_code(s ->> 'set_code') and k.sku_kind = 'set';
    if not found or v_set = any(v_seen) or jsonb_typeof(s -> 'rows') is distinct from 'array'
       or exists (select 1 from jsonb_array_elements(s -> 'rows') r
                   where coalesce(length(r ->> 'code'), 0) = 0 or jsonb_typeof(r -> 'qty') is distinct from 'number' or jsonb_typeof(r -> 'sort') is distinct from 'number') then
      continue;
    end if;
    v_seen := array_append(v_seen, v_set);
    select coalesce(jsonb_agg(jsonb_build_object('sku_id', k.sku_id, 'code', r ->> 'code', 'qty', (r ->> 'qty')::integer, 'sort', (r ->> 'sort')::integer)
                              order by (r ->> 'sort')::integer, r ->> 'code'), '[]'::jsonb)
      into v_rows
      from jsonb_array_elements(s -> 'rows') r
      left join core.skus k on k.code_norm = core.norm_code(r ->> 'code');
    insert into ops.ne_set_observations (run_id, set_sku_id, rows) values (v_run, v_set, v_rows);
    v_saved := v_saved + 1;
  end loop;
  return jsonb_build_object('state', 'written', 'run_id', v_run, 'sets', v_saved, 'skipped', v_skip);
end $$;
revoke all on function ops.record_ne_set_observations(jsonb) from public;

-- 5. 原価の守り (両端を含む [valid_from, valid_to]。valid_to = null = ずっと)。上の 🚨 M7 = ⑥ の前提
--   ・重なり: この画面と昇格の書き込み (source_system) と、表の持ち主でないロール (source_system を偽っても) の書き込みだけ見る
--   ・持ち主でないロールは、今日 (東京) より前に始まった行を消さない・閉じた行 (valid_to がある) を変えない・昨日より前で閉じない (過去の粗利を変えない)
create function core.guard_sku_cost_overlap() returns trigger language plpgsql as $$
declare
  v_owner boolean := pg_catalog.pg_has_role(current_user, (select c.relowner from pg_catalog.pg_class c where c.oid = tg_relid), 'USAGE');
begin
  if v_owner and coalesce(pg_catalog.current_setting('core.source_system', true), '') not in ('portal_master_edit', 'ne_observation') then return null; end if;
  if exists (select 1 from core.sku_costs o
              where o.sku_id = new.sku_id and o.sku_cost_id <> new.sku_cost_id
                and o.valid_from <= coalesce(new.valid_to, 'infinity'::date) and new.valid_from <= coalesce(o.valid_to, 'infinity'::date)) then
    raise exception 'sku_cost_overlap: SKU % の原価の期間が重なる (% 〜 %)', new.sku_id, new.valid_from, coalesce(new.valid_to::text, '') using errcode = '23P01';
  end if;
  return null;
end $$;
create trigger trg_sku_costs_no_overlap after insert or update of sku_id, valid_from, valid_to on core.sku_costs
  for each row execute function core.guard_sku_cost_overlap();

create function core.guard_sku_cost_history() returns trigger language plpgsql as $$
declare
  v_today date := (now() at time zone 'Asia/Tokyo')::date;
begin
  if pg_catalog.pg_has_role(current_user, (select c.relowner from pg_catalog.pg_class c where c.oid = tg_relid), 'USAGE') then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  if tg_op = 'DELETE' then
    if old.valid_from < v_today then raise exception 'sku_cost_history: 今日より前に始まった原価の行は消さない (過去の粗利を変えない)' using errcode = '42501'; end if;
    return old;
  end if;
  if old.valid_to is not null then raise exception 'sku_cost_history: 閉じた原価の行は変えない' using errcode = '42501'; end if;
  if new.valid_to is not null and new.valid_to < v_today - 1 then raise exception 'sku_cost_history: 昨日より前で閉じない (過去の粗利を変えない)' using errcode = '42501'; end if;
  return new;
end $$;
create trigger trg_sku_costs_history before update or delete on core.sku_costs
  for each row execute function core.guard_sku_cost_history();

-- 6. 0026 の変更の記録の関数を security definer に (画面のロールに記録の表の insert を渡さない = 偽れない。PR #1563 R2 M4)。
--    中身は 0026 と同じ (記録する列・比べ方)。違うのは db_user = 「SET ROLE の役 か ログインした役」を明示すること (持ち主の権限で動いても呼び手を残す)
create or replace function core.audit_master_change() returns trigger
  language plpgsql security definer set search_path = pg_catalog, core, events, pg_temp as $$
declare
  v_entity   text := tg_argv[0];
  v_keys     text[] := string_to_array(tg_argv[1], ',');
  v_ignored  text[] := core.master_audit_ignored_columns();
  v_old      jsonb;
  v_new      jsonb;
  v_row      jsonb;
  v_key      jsonb := '{}'::jsonb;
  v_id       bigint;
  v_change   uuid := gen_random_uuid();
  v_actor_t  text := coalesce(nullif(current_setting('core.actor_type', true), ''), 'system');
  v_actor_id text := nullif(current_setting('core.actor_id', true), '');
  v_source   text := coalesce(nullif(current_setting('core.source_system', true), ''), 'sql');
  v_run      text := nullif(current_setting('core.run_id', true), '');
  v_req      text := nullif(current_setting('core.request_id', true), '');
  v_reason   text := nullif(current_setting('core.reason', true), '');
  v_db_user  text := case when coalesce(current_setting('role', true), 'none') <> 'none' then current_setting('role', true) else session_user::text end;
  v_cols     text[];
  c          text;
  k          text;
begin
  if tg_op in ('UPDATE','DELETE') then v_old := to_jsonb(old); end if;
  if tg_op in ('INSERT','UPDATE') then v_new := to_jsonb(new); end if;
  v_row := coalesce(v_new, v_old);
  foreach k in array v_keys loop v_key := v_key || jsonb_build_object(k, v_row -> k); end loop;
  if array_length(v_keys, 1) = 1 then v_id := (v_row ->> v_keys[1])::bigint; end if;
  select array_agg(x order by x) into v_cols from jsonb_object_keys(v_row) as t(x) where not (x = any(v_ignored));

  if tg_op = 'UPDATE' then
    foreach c in array v_cols loop
      if (v_old -> c) is distinct from (v_new -> c) then
        insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, attribute, old_value, new_value,
                                                 actor_type, actor_id, source_system, run_id, request_id, reason_text, db_user)
        values ((v_row ->> 'company_id')::smallint, v_change, 'UPDATE', v_entity, v_id, v_key, c, v_old -> c, v_new -> c,
                v_actor_t, v_actor_id, v_source, v_run, v_req, v_reason, v_db_user);
      end if;
    end loop;
    return null;
  end if;

  insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, attribute, old_value, new_value,
                                           actor_type, actor_id, source_system, run_id, request_id, reason_text, db_user)
  values ((v_row ->> 'company_id')::smallint, v_change, tg_op, v_entity, v_id, v_key, null,
          case when tg_op = 'DELETE' then (select jsonb_object_agg(x, v_old -> x) from unnest(v_cols) as u(x)) end,
          case when tg_op = 'INSERT' then (select jsonb_object_agg(x, v_new -> x) from unnest(v_cols) as u(x)) end,
          v_actor_t, v_actor_id, v_source, v_run, v_req, v_reason, v_db_user);
  return null;
end $$;
revoke all on function core.audit_master_change() from public;

-- 子の表の変更で親の version を上げる (0026 と同じ中身)。security definer = 画面のロールに skus.version の update を渡さない
create or replace function core.bump_parent_version() returns trigger
  language plpgsql security definer set search_path = pg_catalog, core, pg_temp as $$
declare
  v_parent text := tg_argv[0];
  v_pk     text := tg_argv[1];
  v_fk     text := tg_argv[2];
  v_ids    bigint[];
  v_ignored text[] := core.master_audit_ignored_columns();
  v_changed boolean;
begin
  if tg_op = 'UPDATE' then
    select exists (select 1 from jsonb_object_keys(to_jsonb(new)) as t(x)
                   where not (x = any(v_ignored)) and (to_jsonb(old) -> x) is distinct from (to_jsonb(new) -> x)) into v_changed;
    if not v_changed then return null; end if;
  end if;
  if tg_op in ('INSERT','UPDATE') then v_ids := array_append(v_ids, (to_jsonb(new) ->> v_fk)::bigint); end if;
  if tg_op in ('UPDATE','DELETE') then v_ids := array_append(v_ids, (to_jsonb(old) ->> v_fk)::bigint); end if;
  perform set_config('core.version_bump', 'on', true);
  execute format('update %s set version = version where %I = any($1)', v_parent, v_pk) using v_ids;
  perform set_config('core.version_bump', '', true);   -- この後の同じ取引の UPDATE に漏らさない
  return null;
end $$;
revoke all on function core.bump_parent_version() from public;

-- 仕入先の行に共有の鍵を掛ける (保存が「仕入先を止める」処理と並ぶ・PR #1563 R2 M5)。鍵は呼んだ取引の終わりまで残る
-- security definer = 画面のロールに core.suppliers の update を渡さない (select ... for share は update の権限が要るため)。実行は master_edit だけ (create-master-edit-roles.mjs)
create or replace function core.lock_suppliers_for_share(p_ids bigint[]) returns void
language plpgsql security definer set search_path = pg_catalog, core, pg_temp as $$
begin
  perform 1 from core.suppliers where supplier_id = any(p_ids) order by supplier_id for share;
end $$;
revoke all on function core.lock_suppliers_for_share(bigint[]) from public;

-- 読むだけの見張り (watcher)。書くロール (master_edit・master_ops・master_observer・master_gate) の権限は scripts/company-db/create-master-edit-roles.mjs が付ける
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_cutover_state, ops.master_cutover_events, ops.master_legacy_manifests, ops.master_legacy_gate_acks, ops.master_edit_requests,
      ops.sku_component_requests, ops.ne_set_observation_runs, ops.ne_set_observations, ops.sku_component_breaches to watcher';
  end if;
end $$;
