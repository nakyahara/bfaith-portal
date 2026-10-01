-- 0051: 新商品の登録と登録の状態・product-hub のカードの outbox (2026-10-01。Company DB構想 14「マスタ入力画面」§9 v2 H4・§10 契約 v3 H3 / Medium 1・§11 / 10 §2。
--       PR #1566 Codex R1 = High 3 / Medium 4 の直し・仮レビュー (Codex の代わり) の直し)
-- 🚨 前提 = ⑤-1 (feat/sor5-input・PR #1563) の 0050_master_edit.sql だけ (と 0049 までの本流)。ほかの未マージのブランチの 0050 / 0051
--    (#1561 finance-coverage の 0051・#1562 perf の 0050) には寄らない = マージの順番で番号を付け直すときは、このファイルの中身はそのまま動かせる
--
-- なぜ: 新商品を Company DB で作る (画面 D = apps/master-edit/new)。作ったばかりの商品は NE にもロジザードにも無い =
--   「業務で使ってよい商品か」を SKU ごとの状態で持ち、行が無い商品は使わない (fail-closed)。
--   同じ保存で product-hub (SQLite) の出品カードも作りたいが、Postgres と SQLite は 1 つの取引にできない = outbox を使う。
-- なにを:
--   1. ops.master_registrations = SKU ごとの登録の状態 (1 SKU = 1 行)。
--        draft (下書き) → ne_pending (NE 登録待ち) → ne_confirmed (NE 確認済み) → distributable (配る対象) → available (業務で使える)
--        + quarantined (切替の後に夜間ロードが NE で見つけた知らない商品 = 自動では使えるにしない) / cancelled (やめた)
--      🚨 行が無い = 使えない。
--      🚨 権限の境界 (R1 H2) = 表の権限: 画面や運用のロール (master_edit / master_ops・scripts/company-db/create-master-edit-roles.mjs) には
--         この表と履歴の INSERT / UPDATE / DELETE を渡さない。書くのは下の security definer の関数 (search_path 固定・public の実行権なし) だけ。
--         ops.registration_protocol (GUC) は関数の中の印でしかない (持ち主のロールの手の DML を止める保険。権限の境界ではない)
--      🚨 根拠は本物の記録で (R1 H3): ne_pending (新規登録の CSV の品目)・ne_confirmed (SKU ごとの照合の結果)・distributable / available (配る世代と
--         場所ごとの受け取り) の根拠の表は ⑤-2b / ④ で作る。それまでは、この関数はそこへ進めない (not_ready)。
--         ⑤-2a で行ける状態 = draft (新商品の登録)・quarantined (夜間ロード)・cancelled (人の理由)・available (切替の日の backfill だけ)
--   2. ops.backfill_sku_registrations = 切替の手順の 1 歩 (自動では流さない)。切替の段階が frozen / company_owner の間に 1 回だけ、
--      その時点の「状態の行が無い SKU」を、人が先に見た件数とハッシュ (ops.registration_backfill_plan) と同じときだけ available にする。
--      ハッシュ = (company_id, sku_id, code_norm, sku_kind) の行の sha256 (R1 M4 = 消して同じコードで作り直した SKU も違うハッシュ)。
--      済んだ印 = ops.master_registration_backfill (1 行・追記だけ)。
--   3. core.skus に行を足す取引は、次のとき同じ取引で状態の行も作らないと commit できない (ops.check_sku_registered = 遅らせた制約の trigger):
--        - backfill の後はだれでも
--        - backfill の前も、表の持ち主 (夜間ロード・migration) とスーパーユーザー以外 (= 画面のロール)
--      夜間ロードが NE から作った SKU は ops.quarantine_unregistered_skus が同じ取引で quarantined にする (engine.mjs が呼ぶ。backfill の前は何もしない)
--   4. 切替の門 (R1 H1): new_open に進むのは、backfill がちょうど 1 回済み **かつ** 状態の行の無い SKU が 0 件のときだけ
--      (前提の関数 ops.master_registrations_prereq を 0050 の差し込み口の表 ops.master_cutover_prereq_checks に 1 行足す = set_master_cutover_phase が呼ぶ。
--       集める関数 ops.master_cutover_prereq_problems は上書きしない (#1563 R3)。段階の行の trigger もこの関数を呼ぶ = 下の理由)
--   5. ops.v_sku_distributable (distributable・available = 写しに載せてよい) / ops.v_sku_available (available だけ = 業務で使ってよい)。
--      🚨 今の読み手 (core.skus を直接読む所) はまだ切り替えない (⑤-3 / ⑥)
--   6. ops.product_hub_outbox = product-hub のカードを作る知らせ。登録と同じ取引で書く。event_id・schema_version・payload・payload_hash は変えない。
--      消費 (apps/product-hub/services/cdb-card-intake.js) は cdb_sku_id の一意で冪等。状態 = pending → done / failed (再試行) / conflict (同じコードのカードが既にある
--      = 人が「既存のカードをこの商品に結ぶ」で done にする)。
--      🚨 状態・結果・借りを書くのは security definer の関数 (ops.claim_card_events / ops.finish_card_event = 借りた人だけが結果を書ける) だけ。画面のロールは知らせの insert と読みだけ
--      (仮レビュー L7)。画面のロールの insert は 0050 と同じ守り (trg_master_edit_guard = 同じ取引で ops.begin_master_write の後・段階 new_open) +
--      request_id・作った人が ops.begin_master_write の行と同じ (偽れない)
--   7. ops.master_edit_requests.operation に 'sku_create' (新商品の登録の保存 1 回) を足す
--   8. 0050 の画面のロールの書き込みの約束 (#1563 R3 M2) との関係:
--      ・新商品の登録 (lib/master-register.mjs) は、書く前に同じ取引で ops.begin_master_write を呼ぶ。商品・SKU・仕入先・原価・構成の依頼・保存の記録の
--        INSERT は 0050 の trg_master_edit_guard が見る (SKU の INSERT = 値のある列の持ち主 = skus.sku_kind なども company)
--      ・ops.create_sku_registration (security definer) も、呼び手が master_edit なら同じ取引の ops.begin_master_write の行が要る。
--        誰が・request_id・理由はその行から取る (引数と違えば 42501 = 偽れない)。SKU を作った取引の中だけ (not_new_sku) は今までどおり
--      ・ops.claim_card_events / ops.finish_card_event は行を要らない: 商品の値 (core.*) を書かず、知らせの届け先の状態だけ。
--        取り込みは保存とは別の時 (product-hub のボードを開いたとき) に動く = 「保存の取引」が無い。借り (lease) の持ち主だけが結果を書ける
--      ・ops.transition_sku_registration / backfill は運用のロール (master_ops) だけ = 画面のロールの約束の外
-- 🚨 この migration は商品の値を何も変えない (状態の行は 1 行も作らない = backfill は切替の日に人が流す)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す (0034 の約束)

-- 1. 登録の状態
create table ops.master_registrations (
  sku_id            bigint primary key,
  company_id        smallint not null default 1 references core.companies,
  state             text not null check (state in ('draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined', 'cancelled')),
  origin            text not null check (origin in ('new_entry', 'backfill', 'ne_discovered')),
  distribution_generation text check (distribution_generation is null or length(distribution_generation) between 1 and 200),
  created_at        timestamptz not null default now(),
  created_by        text not null check (length(created_by) > 0),
  state_changed_at  timestamptz not null default now(),
  state_changed_by  text not null check (length(state_changed_by) > 0),
  constraint ck_mr_generation check (state <> 'distributable' or distribution_generation is not null),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_master_registrations_state on ops.master_registrations (state);
comment on table ops.master_registrations is 'SKU ごとの登録の状態 (0051)。行が無い = 使えない。書くのは security definer の関数だけ (画面・運用のロールに DML を渡さない)';

create table ops.master_registration_events (
  event_id    bigint generated always as identity primary key,
  company_id  smallint not null default 1 references core.companies,
  sku_id      bigint not null,
  from_state  text,   -- null = 行を作った
  to_state    text not null,
  actor_type  text not null check (actor_type in ('human', 'system')),
  actor_id    text not null check (length(actor_id) > 0),
  reason      text check (reason is null or length(reason) <= 500),
  evidence    jsonb not null default '{}'::jsonb check (jsonb_typeof(evidence) = 'object'),
  request_id  text,
  recorded_at timestamptz not null default now(),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_master_registration_events_sku on ops.master_registration_events (sku_id, event_id desc);
select core.make_append_only('ops', 'master_registration_events');

create table ops.master_registration_backfill (
  id            smallint primary key default 1 check (id = 1),
  sku_count     integer not null check (sku_count >= 0),
  snapshot_hash text not null check (snapshot_hash ~ '^[0-9a-f]{64}$'),
  phase         text not null,
  actor         text not null check (length(actor) > 0),
  note          text check (note is null or length(note) <= 500),
  done_at       timestamptz not null default now()
);
select core.make_append_only('ops', 'master_registration_backfill');
comment on table ops.master_registration_backfill is '既存の SKU を available にした印 (0051・切替の手順で 1 回だけ)。この後は状態の行の無い SKU を commit できない・new_open の前提';

-- 関数の中だけ印を立てる (持ち主のロールの手の DML を止める保険。権限の境界は表の権限 = 上の 🚨)。DELETE・TRUNCATE はいつでも拒む
create function ops.guard_master_registrations() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception '登録の状態は消さない (やめるときは cancelled)' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.registration_protocol', true), '') is distinct from '1' then
    raise exception '登録の状態は ops の関数 (create_sku_registration / transition_sku_registration ほか) でだけ書く' using errcode = 'P0001';
  end if;
  if tg_op = 'UPDATE' and (new.sku_id, new.company_id, new.origin, new.created_at, new.created_by)
                          is distinct from (old.sku_id, old.company_id, old.origin, old.created_at, old.created_by) then
    raise exception '登録の状態の行の SKU・出どころ・作った記録は変えない' using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_master_registrations_guard before insert or update or delete on ops.master_registrations
  for each row execute function ops.guard_master_registrations();
create trigger trg_master_registrations_no_truncate before truncate on ops.master_registrations
  for each statement execute function core.reject_mutation();

-- 進む向きの地図 (一方向。戻さない)。⑤-2a で開いているのは → cancelled だけ (ほかは根拠の表ができてから = transition_sku_registration の not_ready)
create function ops.registration_transition_allowed(p_from text, p_to text) returns boolean language sql immutable as $$
  select (p_from || '>' || p_to) = any (array[
    'draft>ne_pending', 'draft>cancelled',
    'ne_pending>ne_confirmed', 'ne_pending>cancelled',
    'ne_confirmed>distributable',
    'distributable>available',
    'quarantined>ne_confirmed', 'quarantined>cancelled'])
$$;

-- 新商品の登録で行を作る (draft)。SKU を作ったのと同じ取引の中でだけ (前からある SKU を下書きに戻さない)
create function ops.create_sku_registration(p_sku_id bigint, p_actor text, p_request_id text default null, p_reason text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_company smallint;
  v_event   bigint;
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions%rowtype;
  v_actor   text := p_actor;
  v_req     text := p_request_id;
  v_reason  text := p_reason;
begin
  if p_actor is null or length(p_actor) = 0 then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  -- 画面のロール = 同じ取引で ops.begin_master_write をした後だけ (0050 の約束と同じ)。誰が・request_id・理由はその行から (引数と違えば 42501)
  if v_db_user = 'master_edit' then
    select * into v_sess from ops.master_write_sessions where txid = pg_catalog.txid_current();
    if not found then
      raise exception 'master_write_session_required: 下書きの状態は、同じ取引で ops.begin_master_write をした後だけ作る' using errcode = '42501';
    end if;
    if p_actor is distinct from v_sess.actor_id or (p_request_id is not null and p_request_id is distinct from v_sess.request_id::text) then
      raise exception 'master_write_session_mismatch: 誰が・request_id が ops.begin_master_write のときと違う' using errcode = '42501';
    end if;
    v_actor := v_sess.actor_id;
    v_req := v_sess.request_id::text;
    v_reason := coalesce(v_sess.reason, p_reason);
  end if;
  select company_id into v_company from core.skus where sku_id = p_sku_id;
  if not found then raise exception 'no_sku: SKU % が無い', p_sku_id using errcode = 'P0001'; end if;
  if exists (select 1 from ops.master_registrations where sku_id = p_sku_id) then
    raise exception 'already_registered: SKU % にはもう状態の行がある', p_sku_id using errcode = '23505';
  end if;
  -- この取引で作った SKU か = SKU の行そのもので見る (仮レビュー M-A: 変更の記録の表は見ない = 記録を偽っても通らない)。
  --   行の xmin がこの取引 (= この取引で入れたか直した) **かつ** created_at がこの取引の時刻 (直しただけの前からの行は created_at が古い。画面のロールは created_at を書けない)
  if not exists (select 1 from core.skus s
                  where s.sku_id = p_sku_id and s.xmin = pg_catalog.pg_current_xact_id()::xid and s.created_at = pg_catalog.now()) then
    raise exception 'not_new_sku: 下書きの状態は SKU を作った取引の中でだけ作る (SKU %)', p_sku_id using errcode = 'P0001';
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  insert into ops.master_registrations (sku_id, company_id, state, origin, created_by, state_changed_by)
    values (p_sku_id, v_company, 'draft', 'new_entry', v_actor, v_actor);
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (v_company, p_sku_id, null, 'draft', 'human', v_actor, v_reason, '{}'::jsonb, v_req) returning event_id into v_event;
  return jsonb_build_object('sku_id', p_sku_id, 'state', 'draft', 'event_id', v_event);
end $$;
revoke all on function ops.create_sku_registration(bigint, text, text, text) from public;

-- 状態を 1 つ進める (一方向・根拠は本物の記録で)。SKU ごとの鍵 (lib/master-write.mjs と同じ鍵) → 行 → 検査 → 行 → 記録
--   ⑤-2a で開いているのは → cancelled (人が理由を書いてだけ) のだけ。ほかは根拠の表ができてから (R1 H3):
--     ne_pending    ← 新規登録の CSV の品目 (この SKU・issued / import_declared) を関数が読んで鍵を取る (⑤-2b)
--     ne_confirmed  ← SKU ごとの照合の結果 (完全な NE の取得の回に結ぶ) (⑤-2b)。quarantined からは人の理由も
--     distributable ← 配る世代の記録 (④)
--     available     ← その世代の場所ごとの受け取りの記録が全部そろった (④)
create function ops.transition_sku_registration(p_sku_id bigint, p_to text, p_actor_type text, p_actor_id text,
                                                p_reason text default null, p_evidence jsonb default '{}'::jsonb, p_request_id text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r        ops.master_registrations%rowtype;
  v_ev     jsonb := coalesce(p_evidence, '{}'::jsonb);
  v_event  bigint;
begin
  if p_actor_type is null or p_actor_type not in ('human', 'system') then raise exception 'invalid_input: actor_type は human か system' using errcode = '22023'; end if;
  if p_actor_id is null or length(p_actor_id) = 0 then raise exception 'invalid_input: 誰が (actor_id) が要る' using errcode = '22023'; end if;
  if jsonb_typeof(v_ev) <> 'object' then raise exception 'invalid_input: 根拠 (evidence) は object' using errcode = '22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('core.sku:' || p_sku_id::text, 0));
  select * into r from ops.master_registrations where sku_id = p_sku_id for update;
  if not found then raise exception 'no_registration: SKU % に状態の行が無い (使えない商品)', p_sku_id using errcode = 'P0001'; end if;
  if not ops.registration_transition_allowed(r.state, p_to) then
    raise exception 'one_way: 状態は % から % に進めない', r.state, p_to using errcode = 'P0001';
  end if;
  if p_to in ('ne_pending', 'ne_confirmed', 'distributable', 'available') then
    raise exception 'not_ready: % への根拠 (%) の表がまだ無い = 進めない (⑤-2b / ④ で作る)', p_to,
      case p_to when 'ne_pending' then '新規登録の CSV の品目' when 'ne_confirmed' then 'SKU ごとの照合の結果' when 'distributable' then '配る世代' else '場所ごとの受け取り' end
      using errcode = 'P0001';
  end if;
  -- cancelled = 人が理由を書いてだけ
  if p_actor_type <> 'human' or coalesce(length(trim(p_reason)), 0) = 0 then raise exception 'no_evidence: やめるのは人が理由を書いてだけ' using errcode = '22023'; end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
revoke all on function ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text) from public;

-- 2. 切替の日の backfill (人が流す 1 回だけ)。先に plan で件数とハッシュを見て、同じ値を渡す。
--    ハッシュ = 状態の行の無い SKU の (company_id, sku_id, code_norm, sku_kind) を sku_id の順に並べた行の sha256 (R1 M4)
create function ops.registration_backfill_plan() returns table (sku_count integer, snapshot_hash text)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select count(*)::int,
         encode(sha256(convert_to(coalesce(string_agg(s.company_id::text || '|' || s.sku_id::text || '|' || s.code_norm || '|' || s.sku_kind, E'\n' order by s.sku_id), ''), 'UTF8')), 'hex')
    from core.skus s where not exists (select 1 from ops.master_registrations r where r.sku_id = s.sku_id)
$$;
revoke all on function ops.registration_backfill_plan() from public;

create function ops.backfill_sku_registrations(p_expected_count integer, p_expected_hash text, p_actor text, p_note text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_phase text;
  v_count integer;
  v_hash  text;
begin
  if p_actor is null or length(p_actor) = 0 then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  perform pg_advisory_xact_lock(hashtext('ops.master_registrations_backfill'));
  select phase into v_phase from ops.master_cutover_state where id = 1 for share;
  if v_phase is null or v_phase not in ('frozen', 'company_owner') then
    raise exception 'wrong_phase: backfill は切替の段階が frozen か company_owner の間だけ (今 = %)', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
  if exists (select 1 from ops.master_registration_backfill) then raise exception 'already_done: backfill はもう済んでいる' using errcode = 'P0001'; end if;
  lock table core.skus in share mode;   -- 数えてから入れるまで SKU を足させない・消させない (固定のスナップショット)
  select p.sku_count, p.snapshot_hash into v_count, v_hash from ops.registration_backfill_plan() p;
  if v_count is distinct from p_expected_count or v_hash is distinct from p_expected_hash then
    raise exception 'snapshot_mismatch: 今は % 件 (%)・渡されたのは % 件 (%)。もう一度 plan を見てから', v_count, v_hash, p_expected_count, p_expected_hash using errcode = 'P0001';
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  with ins as (
    insert into ops.master_registrations (sku_id, company_id, state, origin, created_by, state_changed_by)
      select s.sku_id, s.company_id, 'available', 'backfill', p_actor, p_actor from core.skus s
       where not exists (select 1 from ops.master_registrations r where r.sku_id = s.sku_id)
      returning sku_id, company_id)
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence)
    select i.company_id, i.sku_id, null, 'available', 'human', p_actor, '切替の日の backfill (既存の商品)', jsonb_build_object('snapshot_hash', v_hash, 'sku_count', v_count)
      from ins i;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_backfill (id, sku_count, snapshot_hash, phase, actor, note) values (1, v_count, v_hash, v_phase, p_actor, p_note);
  return jsonb_build_object('sku_count', v_count, 'snapshot_hash', v_hash, 'phase', v_phase);
end $$;
revoke all on function ops.backfill_sku_registrations(integer, text, text, text) from public;

-- backfill の後に状態の行の無い SKU (= 夜間ロードが NE から作った) を quarantined に。backfill の前は何もしない (0 を返す)。呼ぶのは夜間ロード (表の持ち主) だけ
create function ops.quarantine_unregistered_skus(p_run_id text) returns integer
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n integer;
begin
  if not exists (select 1 from ops.master_registration_backfill) then return 0; end if;
  perform pg_advisory_xact_lock(hashtext('ops.master_registrations_backfill'));
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  with ins as (
    insert into ops.master_registrations (sku_id, company_id, state, origin, created_by, state_changed_by)
      select s.sku_id, s.company_id, 'quarantined', 'ne_discovered', 'company_db_load', 'company_db_load' from core.skus s
       where not exists (select 1 from ops.master_registrations r where r.sku_id = s.sku_id)
      returning sku_id, company_id)
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence)
    select i.company_id, i.sku_id, null, 'quarantined', 'system', 'company_db_load', '切替の後に NE で見つけた知らない商品', jsonb_build_object('run_id', p_run_id)
      from ins i;
  get diagnostics v_n = row_count;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  return v_n;
end $$;
revoke all on function ops.quarantine_unregistered_skus(text) from public;

-- 3. SKU を足した取引の commit のときの確かめ。trigger の関数は呼び手のロールで動く (current_user = 足したロール) →
--    確かめる本体は security definer (呼び手に表の読みの権限を渡さない)。本体の実行権が無いロールは足せない (fail-closed)
create function ops.sku_registration_problem(p_sku_id bigint, p_role text) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_owner text;
  v_super boolean;
begin
  if not exists (select 1 from core.skus where sku_id = p_sku_id) then return null; end if;   -- 同じ取引で消えた行
  if exists (select 1 from ops.master_registrations where sku_id = p_sku_id) then return null; end if;
  if exists (select 1 from ops.master_registration_backfill) then return 'backfill の後'; end if;
  select pg_get_userbyid(c.relowner) into v_owner from pg_class c where c.oid = 'core.skus'::regclass;
  select r.rolsuper into v_super from pg_roles r where r.rolname = p_role;
  if p_role = v_owner or coalesce(v_super, false) then return null; end if;   -- backfill の前の夜間ロード・migration
  return format('表の持ち主でないロール %s', p_role);
end $$;
revoke all on function ops.sku_registration_problem(bigint, text) from public;

create function ops.check_sku_registered() returns trigger language plpgsql as $$
declare
  v_why text := ops.sku_registration_problem(new.sku_id, current_user::text);
begin
  if v_why is not null then
    raise exception 'unregistered_sku: SKU % (%) に登録の状態が無い (%。新商品の登録の画面か、夜間ロードの ops.quarantine_unregistered_skus を通す)', new.sku_id, new.code, v_why
      using errcode = '23514';
  end if;
  return null;
end $$;
create constraint trigger trg_skus_registered after insert on core.skus deferrable initially deferred
  for each row execute function ops.check_sku_registered();

-- 4. 切替の門: new_open に進む前提 (R1 H1)。問題の一覧 (空 = 進んでよい)。0050 の差し込み口の表 ops.master_cutover_prereq_checks に 1 行足す
--    = ops.set_master_cutover_phase が集める関数 (ops.master_cutover_prereq_problems) から呼んで断る (prereq_failed: 0051_registrations: ...)。
--    集める関数は上書きしない (#1563 R3 = 後の migration の前提を消さない)。security definer にしない (集める関数が持ち主の権限で呼ぶ)
create function ops.master_registrations_prereq(p_from text, p_to text) returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[] := '{}';
  v_backfills integer;
  v_unregistered integer;
begin
  if p_to = 'new_open' then
    select count(*)::int into v_backfills from ops.master_registration_backfill;
    if v_backfills <> 1 then
      v_problems := v_problems || format('backfill_missing: 既存の SKU の登録の状態 (ops.backfill_sku_registrations) が済んでいない (%s 回)', v_backfills);
    end if;
    select count(*)::int into v_unregistered from core.skus s where not exists (select 1 from ops.master_registrations r where r.sku_id = s.sku_id);
    if v_unregistered > 0 then
      v_problems := v_problems || format('unregistered_skus: 登録の状態の行が無い SKU が %s 件ある', v_unregistered);
    end if;
  end if;
  return v_problems;
end $$;
revoke all on function ops.master_registrations_prereq(text, text) from public;
insert into ops.master_cutover_prereq_checks (name, fn) values ('0051_registrations', 'ops.master_registrations_prereq(text, text)');

-- 保険の trigger (段階の行が変わるときにも同じ前提の関数を見る)。#1563 R3 の差し込み口の表の後も残す理由:
--   ① ops.set_master_cutover_phase は後の migration (⑤-3・⑥) でも create or replace される = 集める関数を呼び忘れた版・表の行を消した版が入っても new_open に進めない
--   ② 持ち主のロールが印 (ops.cutover_protocol) を立てて段階の行を直接変えても、前提は外せない
--   前提の中身は 1 か所 (ops.master_registrations_prereq) = 差し込み口と trigger で答えは同じ (二重に断るだけで、通るものは変わらない)。
--   (復元は trigger を止めて入れる = 影響しない)
create function ops.guard_master_cutover_prereq() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[];
begin
  if new.phase is distinct from old.phase then
    v_problems := ops.master_registrations_prereq(old.phase, new.phase);
    if coalesce(array_length(v_problems, 1), 0) > 0 then
      raise exception 'cutover_prereq: 段階を % から % に進めない: %', old.phase, new.phase, array_to_string(v_problems, ' / ') using errcode = 'P0001';
    end if;
  end if;
  return new;
end $$;
create trigger trg_master_cutover_state_prereq before update on ops.master_cutover_state
  for each row execute function ops.guard_master_cutover_prereq();

-- 5. 読む口 (今の読み手はまだ切り替えない = ⑤-3 / ⑥)
create view ops.v_sku_distributable as
  select s.sku_id, s.company_id, s.code, s.code_norm, s.sku_kind, r.state, r.distribution_generation
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state in ('distributable', 'available');
comment on view ops.v_sku_distributable is '古い表への写し (④) に載せてよい SKU (0051)。状態の行が無い SKU は入らない';
create view ops.v_sku_available as
  select s.sku_id, s.company_id, s.code, s.code_norm, s.sku_kind, r.state
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state = 'available';
comment on view ops.v_sku_available is '業務で使ってよい SKU (0051)。状態の行が無い SKU は入らない (fail-closed)';

-- 6. product-hub のカードの outbox
create table ops.product_hub_outbox (
  event_id       uuid primary key default gen_random_uuid(),
  company_id     smallint not null default 1 references core.companies,
  sku_id         bigint not null,
  kind           text not null check (kind in ('card_create')),
  schema_version text not null check (schema_version ~ '^ph-card-v[0-9]+$'),
  payload        jsonb not null check (jsonb_typeof(payload) = 'object'),
  payload_hash   text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  request_id     uuid not null,
  created_by     text not null check (length(created_by) > 0),
  created_at     timestamptz not null default now(),
  status         text not null default 'pending' check (status in ('pending', 'failed', 'done', 'conflict')),
  attempts       integer not null default 0 check (attempts >= 0),
  last_error     text check (last_error is null or length(last_error) <= 2000),
  lease_owner    text,
  leased_until   timestamptz,
  result         jsonb check (result is null or jsonb_typeof(result) = 'object'),
  done_at        timestamptz,
  updated_at     timestamptz not null default now(),
  unique (sku_id, kind),
  constraint ck_pho_done check ((status = 'done') = (done_at is not null)),
  constraint ck_pho_lease check ((lease_owner is null) = (leased_until is null)),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_product_hub_outbox_open on ops.product_hub_outbox (created_at) where status <> 'done';
comment on table ops.product_hub_outbox is 'product-hub のカードを作る知らせ (0051)。登録と同じ取引で書く。中身は変えない・消さない。消費は cdb_sku_id の一意で冪等';

create function ops.guard_product_hub_outbox() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'product_hub_outbox は消さない' using errcode = 'P0001'; end if;
  if (new.event_id, new.company_id, new.sku_id, new.kind, new.schema_version, new.payload, new.payload_hash, new.request_id, new.created_by, new.created_at)
     is distinct from (old.event_id, old.company_id, old.sku_id, old.kind, old.schema_version, old.payload, old.payload_hash, old.request_id, old.created_by, old.created_at) then
    raise exception 'product_hub_outbox の知らせの中身 (event_id・版・payload・hash) は変えない' using errcode = 'P0001';
  end if;
  if old.status = 'done' then raise exception '済んだ知らせ (done) は変えない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_product_hub_outbox_guard before update or delete on ops.product_hub_outbox
  for each row execute function ops.guard_product_hub_outbox();
create trigger trg_product_hub_outbox_no_truncate before truncate on ops.product_hub_outbox
  for each statement execute function core.reject_mutation();
-- 画面のロールの insert = 0050 と同じ守り (同じ取引で ops.begin_master_write の後・段階 new_open。#1563 R3 M2) + request_id・作った人が その行と同じ
create trigger trg_master_edit_guard before insert on ops.product_hub_outbox for each row execute function ops.guard_master_edit_write();
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない。名前の順 (trg_master_edit_guard の後) に動く
create function ops.guard_product_hub_outbox_session() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions%rowtype;
begin
  if v_db_user is distinct from 'master_edit' then return new; end if;
  select * into v_sess from ops.master_write_sessions where txid = pg_catalog.txid_current();
  if not found then
    raise exception 'master_write_session_required: カードの知らせは、同じ取引で ops.begin_master_write をした後だけ' using errcode = '42501';
  end if;
  if new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then
    raise exception 'master_write_session_mismatch: カードの知らせの request_id・作った人が ops.begin_master_write のときと違う' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_product_hub_outbox_session() from public;
create trigger trg_product_hub_outbox_session before insert on ops.product_hub_outbox
  for each row execute function ops.guard_product_hub_outbox_session();

-- 取り込む知らせを借りる (lease)。p_mode = auto (まだ・失敗で回数の上限の前) / manual (人が押した = 失敗・衝突も) / link (衝突だけ = 既存のカードに結ぶ)。
-- 借りた行を返す (for update skip locked = 同時に 2 つは借りない・借りの期限の前はほかが取らない)。試した回数を 1 増やす
create function ops.claim_card_events(p_owner text, p_mode text, p_event_id uuid default null, p_sku_id bigint default null, p_limit integer default 20,
                                      p_lease_seconds integer default 120, p_max_auto integer default 5)
  returns table (event_id uuid, sku_id bigint, schema_version text, payload jsonb, payload_hash text, attempts integer, status text, result jsonb)
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_owner is null or p_owner !~ '^[A-Za-z0-9_.:-]{1,100}$' then raise exception 'invalid_input: 借りる人 (owner) の形が違う' using errcode = '22023'; end if;
  if p_mode is null or p_mode not in ('auto', 'manual', 'link') then raise exception 'invalid_input: mode は auto / manual / link' using errcode = '22023'; end if;
  return query
  with c as (
    select o.event_id from ops.product_hub_outbox o
     where (case p_mode when 'auto' then o.status = 'pending' or (o.status = 'failed' and o.attempts < greatest(1, p_max_auto))
                        when 'manual' then o.status in ('pending', 'failed', 'conflict')
                        else o.status = 'conflict' end)
       and (o.leased_until is null or o.leased_until < pg_catalog.now())
       and (p_event_id is null or o.event_id = p_event_id) and (p_sku_id is null or o.sku_id = p_sku_id)
     order by o.created_at, o.event_id limit greatest(1, least(200, coalesce(p_limit, 20))) for update skip locked)
  update ops.product_hub_outbox o set lease_owner = p_owner, leased_until = pg_catalog.now() + pg_catalog.make_interval(secs => greatest(10, least(3600, coalesce(p_lease_seconds, 120)))),
         attempts = o.attempts + 1, updated_at = pg_catalog.now()
    from c where o.event_id = c.event_id
  returning o.event_id, o.sku_id, o.schema_version, o.payload, o.payload_hash, o.attempts, o.status, o.result;
end $$;
revoke all on function ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer) from public;

-- 借りた知らせの結果を書く (借りた人だけ・済んだ知らせは変えない)。p_status = done / failed / conflict。書けたら true (借りが切れてほかが取った = false)
create function ops.finish_card_event(p_event_id uuid, p_owner text, p_status text, p_result jsonb, p_error text) returns boolean
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n integer;
begin
  if p_status is null or p_status not in ('done', 'failed', 'conflict') then raise exception 'invalid_input: status は done / failed / conflict' using errcode = '22023'; end if;
  if p_result is not null and jsonb_typeof(p_result) <> 'object' then raise exception 'invalid_input: result は object' using errcode = '22023'; end if;
  update ops.product_hub_outbox set status = p_status, result = p_result, last_error = left(p_error, 2000),
         done_at = case when p_status = 'done' then pg_catalog.now() end, lease_owner = null, leased_until = null, updated_at = pg_catalog.now()
   where event_id = p_event_id and lease_owner = p_owner and status <> 'done';
  get diagnostics v_n = row_count;
  return v_n = 1;
end $$;
revoke all on function ops.finish_card_event(uuid, text, text, jsonb, text) from public;

-- 見張り・毎朝のまとめ用 (仮レビュー L6): カード作成待ち (まだ・失敗・衝突) の数と、いちばん古い知らせの時刻
create view ops.v_product_hub_outbox_open as
  select o.status, count(*)::int as n, min(o.created_at) as oldest_at, max(o.attempts) as max_attempts
    from ops.product_hub_outbox o where o.status <> 'done' group by o.status;
comment on view ops.v_product_hub_outbox_open is 'product-hub のカード作成待ち (0051)。done 以外の知らせの状態ごとの数 (見張り・毎朝のまとめ)';

-- 7. 保存の記録に新商品の登録
alter table ops.master_edit_requests drop constraint master_edit_requests_operation_check;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create'));

-- 見張りは読むだけ。画面・運用のロールの権限は scripts/company-db/create-master-edit-roles.mjs (⑤-1 のロールを作る手の操作。migration の後に流し直す)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_registrations, ops.master_registration_events, ops.master_registration_backfill, ops.product_hub_outbox, ops.v_sku_distributable, ops.v_sku_available, ops.v_product_hub_outbox_open to watcher';
  end if;
end $$;
