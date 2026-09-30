-- 0050: マスタ入力画面の土台 (2026-09-30。Company DB構想 14「マスタ入力画面」§3・§6 ⑤-1 / 10 §2・§8。Codex 設計レビュー ⑤-R0・R1 の契約・PR #1563 R1)
--
-- なぜ: 既にある商品・セットの値を Company DB で人が直す画面 (apps/master-edit・Render だけ) を作る。書く先は Company DB だけ。
--   変更の記録と version は 0026 のトリガーが書く。ここで足すのは「切替の段階と、その門」「セットに置く列」「保存 1 回の記録」「セットの構成の依頼・NE の構成の観測・食い違い」
--   「原価の期間の重なりの守り」。
-- なにを:
--   1. ops.master_cutover_state (1 行) = 切替の段階。legacy_open (今 = 古い入口が正) → frozen (古い入口を止めた) → company_owner (持ち主が Company DB) → new_open (新しい画面で保存できる)。
--      一方向・1 段ずつ。進めるのは ops.set_master_cutover_phase だけ (security definer・public の実行権なし = 運用のロールだけ)。変えた記録と証拠は ops.master_cutover_events (追記だけ)。
--      🚨 門 (PR #1563 R1 H1): 進めるには、動いている全部の場所 (ops.master_cutover_required_hosts() = render・minipc) が
--         「古い入口の門を持つ版」で起動したと名乗った記録 (ops.master_legacy_gate_acks の legacy_gates_version ≧ ops.master_legacy_gates_required_version()) と、証拠 (jsonb) が要る:
--           legacy_open → frozen        : 全部の場所の門の版 + drain (書きかけを流し終えた: done = true・確かめた人・時刻) + 手の入口を止めた一覧 (入口・止めた人・時刻・1 つ以上)
--           frozen → company_owner      : 持ち主表のハッシュ (owner_hash) + 全部の場所が frozen の後にそのハッシュで起動した記録
--           company_owner → new_open    : 同じハッシュ + 全部の場所が company_owner の後にそのハッシュで起動した記録
--         ⑤-1 では誰も ops.master_legacy_gate_acks を書かない (書くのは ⑤-3 の古い入口の門) = ⑤-3 が配られるまで legacy_open から進めない。
--      🚨 読めない = 閉じている (fail-closed。lib/master-cutover.mjs)。新しい画面の保存 = 段階 new_open **かつ** 持ち主表の列が company **かつ** 持ち主表のハッシュが段階の記録と同じ
--         **かつ** env MASTER_EDIT_OPEN = 1。保存の取引は hashtext('ops.master_cutover') の共有の鍵を持つ = 段階を変える取引 (排他の鍵) と並ぶ
--   2. core.skus.set_sales_class_override (1〜4・null) = セットの売上分類を人が決めた値 (導けないときだけ画面が許す)。セットは products の行を持たない (0002:70) ので SKU に置く
--      core.skus.handling_own ('active' / 'discontinued'・null) = セット自身の取扱区分。skus.handling は「セット自身 + 構成品」から導いた値。null = まだ人が決めていない
--      🚨 sku_kind = 'set' だけ、の CHECK は付けない: 夜間ロードが NE の種類替えを写したときに 1 行の CHECK で夜間ロード全体を止めない
--   3. ops.master_edit_requests = 保存 1 回 = 1 行 (request_id = 画面が作る UUID)。🚨 保存と同じ取引で done (結果つき) を書く = 「処理中」のまま残る行は無い。
--      巻き戻った保存は取引の後に failed (誤りつき) を残す。追記だけ。同じ request_id + 同じ中身 = 残した結果・誤りを返す / 中身が違う = 409 (request_id ごとの鍵を最初に取る)
--   4. セットの構成 (PR #1563 R1 H2): core.sku_components = 最後に確かめた構成。画面は書かない。
--      ops.sku_component_requests = 構成を変えたい依頼 (セットごとに開いているのは 1 つ・新しい依頼は同じ取引で前の依頼を閉じる)
--      ops.ne_set_observation_runs / ops.ne_set_observations = NE のセットの構成の観測 (取得の回ごと・完全に取れた印・観測の時刻・行)。追記だけ。
--        書くのは ops.record_ne_set_observations (public の実行権なし = 夜間ロードのロールだけ。⑤-2 で夜間ロードから呼ぶ)
--      ops.sku_component_breaches = 食い違い (NE でやること): mismatch (依頼と NE が違う) / stale (依頼から 7 日たっても NE が違う) / unrequested_diff (依頼が無いのに NE が今の構成と違う)
--      依頼を上げるのは lib/master-write.mjs の promoteComponentRequest(観測の番号) だけ: DB から観測を読み直し、完全な回・依頼より後・構成品 / 数量 / 並び / 行の数まで同じときだけ
--        core.sku_components を依頼の構成にし、導く値を計算し直し、依頼を applied で閉じる (同じ取引)。違えば食い違いを残す
--   5. core.sku_costs の期間の重なりの守り (この画面と構成の依頼の昇格 = source_system が portal_master_edit / ne_observation の書き込みだけ)。
--      期間は今までどおり両端を含む [valid_from, valid_to]。
--      🚨 既知の未達 (PR #1563 R1 M7): 表全体を半開区間 [from, to) にそろえて排他制約 (exclusion) を付けるのは ⑥ 切替の前提条件 (このPRではしない)。そろえる読み手・書き手 =
--         夜間ロード apps/company-db/load/engine.mjs (§5 の付け替え = greatest(valid_from, 今日 − 1) で閉じる = 同じ日の 2 回で 1 日重なる)・照合 master-compare/compare-load.mjs / compare-ne.mjs・
--         apps/master-decisions/lz-cdb.mjs・apps/company-db/router.mjs・push/sku-cost-observed.mjs・0007 / 0009 の mart (valid_to is null)・0046 v_sku_cost_observed_effective・
--         0049 mart.amazon_profit_* (その日を覆う 1 行)。いまは全部が両端を含む前提で読み・書きしている
-- 🚨 この migration は商品の値を何も変えない (列は全部 null で足す・切替の段階は legacy_open から)

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
  acks        jsonb not null check (jsonb_typeof(acks) = 'array'),   -- 門を確かめたときに使った場所ごとの記録 (ack_id・host・build_id・owner_hash・版)
  note        text check (note is null or length(note) <= 500),
  changed_at  timestamptz not null default now()
);
select core.make_append_only('ops', 'master_cutover_events');

-- 動いている場所ごとの「古い入口の門を持つ版で起動した」記録 (⑤-3 の門が起動のたびに書く。⑤-1 では誰も書かない)
create table ops.master_legacy_gate_acks (
  ack_id               bigint generated always as identity primary key,
  host                 text not null check (host ~ '^[a-z0-9_-]{1,40}$'),
  build_id             text not null check (length(build_id) between 1 and 100),
  owner_hash           text not null check (owner_hash ~ '^[0-9a-f]{64}$'),
  legacy_gates_version integer not null check (legacy_gates_version >= 0),
  phase_seen           text not null check (phase_seen in ('legacy_open', 'frozen', 'company_owner', 'new_open')),
  acked_at             timestamptz not null default now()
);
create index ix_master_legacy_gate_acks_host on ops.master_legacy_gate_acks (host, acked_at desc, ack_id desc);
select core.make_append_only('ops', 'master_legacy_gate_acks');
comment on table ops.master_legacy_gate_acks is '動いている場所 (render・minipc) が古い入口の門を持つ版で起動したと名乗った記録 (0050。書くのは ⑤-3)。切替の段階を進める門が読む';

-- 門の設定 (⑤-3 で「古い入口の門の版」を上げるときは create or replace で直す)
create function ops.master_cutover_required_hosts() returns text[] language sql immutable as $$ select array['minipc', 'render']::text[] $$;
create function ops.master_legacy_gates_required_version() returns integer language sql immutable as $$ select 1 $$;

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

-- 1 段だけ進める (飛ばさない・戻さない・証拠と全部の場所の記録が要る)。鍵 (排他) → 行 (for update) → 門 → 状態 → 記録
-- 🚨 security definer: 呼ぶ人に表の書き込みの権限を渡さない (運用のロールに実行だけ)。一時の表を使わない・search_path の最後に pg_temp (0034 の約束)
create function ops.set_master_cutover_phase(p_to text, p_actor text, p_evidence jsonb, p_note text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_order text[] := array['legacy_open', 'frozen', 'company_owner', 'new_open'];
  v_from  text;
  v_since timestamptz;
  v_hash  text;
  v_new_hash text;
  v_req   integer := ops.master_legacy_gates_required_version();
  v_hosts text[] := ops.master_cutover_required_hosts();
  v_acks  jsonb := '[]'::jsonb;
  v_missing text[] := '{}';
  v_host  text;
  a       record;
  e       jsonb;
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

  if p_to = 'frozen' then
    -- drain = 書きかけ (古い入口の途中の処理・送り待ち) を流し終えた
    e := p_evidence -> 'drain';
    if e is null or jsonb_typeof(e) <> 'object' or (e ->> 'done') is distinct from 'true' or coalesce(length(e ->> 'checked_by'), 0) = 0 or not ops.cutover_is_ts(e ->> 'checked_at') then
      raise exception 'evidence_invalid: drain = { done: true, checked_by, checked_at } が要る' using errcode = '22023';
    end if;
    -- 手の入口 (機械では止められない入口) を止めた一覧
    e := p_evidence -> 'manual_entries_stopped';
    if e is null or jsonb_typeof(e) <> 'array' or jsonb_array_length(e) = 0 then
      raise exception 'evidence_invalid: manual_entries_stopped = [{ entry, stopped_by, stopped_at }, ...] (1 つ以上) が要る' using errcode = '22023';
    end if;
    if exists (select 1 from jsonb_array_elements(e) x
                where jsonb_typeof(x) <> 'object' or coalesce(length(x ->> 'entry'), 0) = 0 or coalesce(length(x ->> 'stopped_by'), 0) = 0 or not ops.cutover_is_ts(x ->> 'stopped_at')) then
      raise exception 'evidence_invalid: manual_entries_stopped の各行に entry・stopped_by・stopped_at が要る' using errcode = '22023';
    end if;
    -- 全部の場所の最後の記録が門の版以上
    foreach v_host in array v_hosts loop
      select ack_id, host, build_id, owner_hash, legacy_gates_version, phase_seen, acked_at into a
        from ops.master_legacy_gate_acks where host = v_host order by acked_at desc, ack_id desc limit 1;
      if not found or a.legacy_gates_version < v_req then v_missing := array_append(v_missing, v_host);
      else v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', a.ack_id, 'host', a.host, 'build_id', a.build_id, 'owner_hash', a.owner_hash, 'version', a.legacy_gates_version)); end if;
    end loop;
  else
    -- 持ち主表のハッシュ: company_owner に進めるとき決める・new_open は同じもの
    v_new_hash := p_evidence ->> 'owner_hash';
    if v_new_hash is null or v_new_hash !~ '^[0-9a-f]{64}$' then raise exception 'evidence_invalid: owner_hash (64 桁の 16 進) が要る' using errcode = '22023'; end if;
    if p_to = 'new_open' and v_new_hash is distinct from v_hash then
      raise exception 'evidence_invalid: owner_hash が company_owner のときと違う' using errcode = '22023';
    end if;
    -- 全部の場所が、今の段階に入った後に、その段階を見て、そのハッシュ・門の版で起動した
    foreach v_host in array v_hosts loop
      select ack_id, host, build_id, owner_hash, legacy_gates_version, phase_seen, acked_at into a
        from ops.master_legacy_gate_acks
       where host = v_host and acked_at > v_since and phase_seen = v_from and owner_hash = v_new_hash and legacy_gates_version >= v_req
       order by acked_at desc, ack_id desc limit 1;
      if not found then v_missing := array_append(v_missing, v_host);
      else v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', a.ack_id, 'host', a.host, 'build_id', a.build_id, 'owner_hash', a.owner_hash, 'version', a.legacy_gates_version)); end if;
    end loop;
  end if;
  if array_length(v_missing, 1) > 0 then
    raise exception 'acks_missing: 門の記録が足りない場所: % (必要な版 %・今の段階 % の後に起動した記録)', array_to_string(v_missing, ', '), v_req, v_from using errcode = 'P0001';
  end if;

  perform pg_catalog.set_config('ops.cutover_protocol', '1', true);
  update ops.master_cutover_state set phase = p_to, changed_at = now(), changed_by = p_actor, note = p_note,
         owner_hash = case when p_to = 'frozen' then null else v_new_hash end
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
  run_id       text primary key check (run_id ~ '^[A-Za-z0-9_.:-]{1,80}$'),
  observed_at  timestamptz not null,   -- NE から取った時刻
  complete     boolean not null,       -- NE のセットを全部取れた回か (途中で止まった・ページが欠けた = false)
  set_count    integer not null check (set_count >= 0),
  content_hash text not null check (content_hash ~ '^[0-9a-f]{32}$'),
  recorded_at  timestamptz not null default now()
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
comment on table ops.ne_set_observations is 'NE のセットの構成の観測 (0050)。書くのは ops.record_ne_set_observations (夜間ロードのロールだけ)。追記だけ';

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
  kind                 text not null check (kind in ('mismatch', 'stale', 'unrequested_diff')),
  observation_id       bigint not null references ops.ne_set_observations (observation_id),
  component_request_id bigint references ops.sku_component_requests (component_request_id),
  details              jsonb not null check (jsonb_typeof(details) = 'object'),   -- { observed: [...], expected: [...] }
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
comment on table ops.sku_component_breaches is 'セットの構成の食い違い = NE でやること (0050)。mismatch / stale / unrequested_diff。閉じ方 = resolved (上げた・NE が今の構成に戻った) / superseded / dismissed';

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

-- NE のセットの構成の観測を 1 回分書く (夜間ロードのロールだけ・⑤-2 で呼ぶ)。
-- p = { run_id, observed_at, complete, sets: [{ set_code, rows: [{ code, qty, sort }] }] }。同じ run_id の再送 = 中身が同じなら何もしない・違えば拒む
-- 知らないセットのコード・セットでない SKU は飛ばして数える。知らない構成品のコードは sku_id = null のまま残す (上げるときは必ず食い違い)
create function ops.record_ne_set_observations(p jsonb) returns jsonb
  language plpgsql set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_run   text := p ->> 'run_id';
  v_at    timestamptz;
  v_hash  text;
  v_prev  text;
  v_sets  integer := 0;
  v_skip  integer := 0;
  s       jsonb;
  v_set   bigint;
  v_rows  jsonb;
begin
  if v_run is null or v_run !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if not ops.cutover_is_ts(p ->> 'observed_at') then raise exception 'invalid_input: observed_at が読めない' using errcode = '22023'; end if;
  v_at := (p ->> 'observed_at')::timestamptz;
  if jsonb_typeof(p -> 'complete') is distinct from 'boolean' then raise exception 'invalid_input: complete (true / false) が要る' using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'sets') is distinct from 'array' then raise exception 'invalid_input: sets が配列でない' using errcode = '22023'; end if;
  v_hash := md5(jsonb_build_array(v_at, p -> 'complete', p -> 'sets')::text);
  perform pg_advisory_xact_lock(hashtext('ops.ne_set_observations:' || v_run));
  select content_hash into v_prev from ops.ne_set_observation_runs where run_id = v_run;
  if found then
    if v_prev = v_hash then return jsonb_build_object('state', 'unchanged', 'run_id', v_run); end if;
    raise exception 'run_conflict: 同じ回 % の中身が違う', v_run using errcode = '23505';
  end if;
  insert into ops.ne_set_observation_runs (run_id, observed_at, complete, set_count, content_hash)
    values (v_run, v_at, (p ->> 'complete')::boolean, jsonb_array_length(p -> 'sets'), v_hash);
  for s in select * from jsonb_array_elements(p -> 'sets') loop
    select k.sku_id into v_set from core.skus k where k.code_norm = core.norm_code(s ->> 'set_code') and k.sku_kind = 'set';
    if not found or jsonb_typeof(s -> 'rows') is distinct from 'array' then v_skip := v_skip + 1; continue; end if;
    if exists (select 1 from jsonb_array_elements(s -> 'rows') r
                where coalesce(length(r ->> 'code'), 0) = 0 or jsonb_typeof(r -> 'qty') is distinct from 'number' or jsonb_typeof(r -> 'sort') is distinct from 'number') then
      raise exception 'invalid_input: セット % の行に code・qty・sort が要る', s ->> 'set_code' using errcode = '22023';
    end if;
    select coalesce(jsonb_agg(jsonb_build_object('sku_id', k.sku_id, 'code', r ->> 'code', 'qty', (r ->> 'qty')::integer, 'sort', (r ->> 'sort')::integer)
                              order by (r ->> 'sort')::integer, r ->> 'code'), '[]'::jsonb)
      into v_rows
      from jsonb_array_elements(s -> 'rows') r
      left join core.skus k on k.code_norm = core.norm_code(r ->> 'code');
    insert into ops.ne_set_observations (run_id, set_sku_id, rows) values (v_run, v_set, v_rows);
    v_sets := v_sets + 1;
  end loop;
  return jsonb_build_object('state', 'written', 'run_id', v_run, 'sets', v_sets, 'skipped', v_skip);
end $$;
revoke all on function ops.record_ne_set_observations(jsonb) from public;

-- 5. 原価の期間の重なり (両端を含む [valid_from, valid_to]。valid_to = null = ずっと)。この画面と昇格の書き込みだけ見る (上の 🚨 M7 = ⑥ の前提)
create function core.guard_sku_cost_overlap() returns trigger language plpgsql as $$
begin
  if coalesce(pg_catalog.current_setting('core.source_system', true), '') not in ('portal_master_edit', 'ne_observation') then return null; end if;
  if exists (select 1 from core.sku_costs o
              where o.sku_id = new.sku_id and o.sku_cost_id <> new.sku_cost_id
                and o.valid_from <= coalesce(new.valid_to, 'infinity'::date) and new.valid_from <= coalesce(o.valid_to, 'infinity'::date)) then
    raise exception 'sku_cost_overlap: SKU % の原価の期間が重なる (% 〜 %)', new.sku_id, new.valid_from, coalesce(new.valid_to::text, '') using errcode = '23P01';
  end if;
  return null;
end $$;
create trigger trg_sku_costs_no_overlap after insert or update of sku_id, valid_from, valid_to on core.sku_costs
  for each row execute function core.guard_sku_cost_overlap();

-- 読むだけの見張り (watcher)。書くロール (master_edit・master_ops) の権限は scripts/company-db/create-master-edit-roles.mjs が付ける
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_cutover_state, ops.master_cutover_events, ops.master_legacy_gate_acks, ops.master_edit_requests, ops.sku_component_requests,
      ops.ne_set_observation_runs, ops.ne_set_observations, ops.sku_component_breaches to watcher';
  end if;
end $$;
