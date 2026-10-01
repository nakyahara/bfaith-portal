-- 0052: 新商品の登録と登録の状態・product-hub のカードの outbox (2026-10-01。Company DB構想 14「マスタ入力画面」§9 v2 H4・§10 契約 v3 H3 / Medium 1・§11 / 10 §2。
--       PR #1566 Codex R1・R2・仮レビューの直し。⑤-1 (#1563 = 0051_master_edit.sql・マージ済み) の最後の形に合わせた)
-- 🚨 前提 = 0051_master_edit.sql (⑤-1)。0050 は finance_coverage (関係なし)
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
--      (前提の関数 ops.master_registrations_prereq を 0051 の差し込み口の表 ops.master_cutover_prereq_checks に 1 行足す = 0052_registrations。
--       set_master_cutover_phase が集める関数から呼ぶ。集める関数は上書きしない。段階の行の trigger もこの関数を呼ぶ = 下の理由)
--   5. ops.v_sku_distributable (distributable・available = 写しに載せてよい) / ops.v_sku_available (available だけ = 業務で使ってよい)。
--      🚨 今の読み手 (core.skus を直接読む所) はまだ切り替えない (⑤-3 / ⑥)
--   6. ops.product_hub_outbox = product-hub のカードを作る知らせ。登録と同じ取引で書く。event_id・schema_version・payload・payload_hash は変えない。
--      SKU は知らせの行の sku_id (DB が振った番号) で結ぶ (payload には入れない = 番号を振る前に payload と hash を作れる)。
--      消費 (apps/product-hub/services/cdb-card-intake.js) は cdb_sku_id の一意で冪等。状態 = pending → done / failed (再試行) / conflict (同じコードのカードが既にある
--      = 人が「既存のカードをこの商品に結ぶ」で done にする)。
--      🚨 知らせを書くのは登録の関数 (下の 8.) だけ・状態・結果・借りを書くのは ops.claim_card_events / ops.finish_card_event (借りた人だけ) だけ。
--         画面のロールは知らせを読むだけ (insert も渡さない)
--   7. ops.master_edit_requests.operation に 'sku_create' (新商品の登録の保存 1 回) を足す
--   8. 新商品の登録 = 登録だけの security definer の関数 ops.register_new_sku (0051 の 8. の「⑤-2a へ」の形):
--      番号を振る (商品・SKU) → 登録の約束 → 商品・SKU・状態 draft・仕入先・原価・構成の依頼・カードの知らせ → 保存の記録 done を、関数の中の 1 か所で。
--      画面のロール master_edit には core.products / core.skus / 知らせの INSERT を渡さない (関数の実行だけ)。
--      🚨 0051 の守り (guard・変更の記録・done の確かめ) はそのまま全部効かせる: 関数の中の書き込みも呼び手は master_edit (security definer でも
--         SET ROLE の役 / ログインした役は変わらない) = 0051 の約束の表に「登録の約束」(operation = sku_create) を、この関数だけが書く:
--         ・約束の相手 = 関数が先に振った SKU の番号・商品の番号 (行を入れる前に約束を書く = guard が「約束の相手の行」と見る)
--         ・ops.begin_master_write は sku_edit のままにする = 画面のロールは登録の約束を作れない (この関数を通るしかない)
--         ・関数の終わりに約束の設定 (ops.master_write_session) を消す = 関数の外では、同じ取引でも画面のロールは登録の約束で書けない
--         ・done は関数が約束どおり (request_id・人・SKU・操作 sku_create・保存の中身のハッシュ) に書く = 0051 の deferred の確かめを満たす
--           (関数の中に例外の受け止め = サブトランザクションを作らない: done の xmin が取引の番号と違ってしまう)
--      このために 0051 の物を 3 つだけ変える (ほかは触らない):
--         ・ops.master_write_sessions.operation の CHECK に 'sku_create'
--         ・ops.master_write_sessions.sku_id の外部キーを遅らせられる形に (名前 fk_mws_sku・ふだんは今までどおりすぐ確かめる。登録の関数だけが
--           set constraints で commit のときの確かめにする = 約束を先に書き、SKU の行はその後に同じ取引で入れる。復元などほかの書き手は変わらない)
--         ・ops.master_write_allowed に sku_create の行 (商品・SKU・仕入先・原価・構成の依頼の INSERT と、0026 の version の付け替えの UPDATE)。sku_edit の行は 0051 と同じ
--      ・ops.create_sku_registration (security definer) は、呼び手が master_edit なら同じ SKU の登録の約束が要る (誰が・request_id・理由はその行から)。画面のロールには渡さない
--      ・ops.claim_card_events / ops.finish_card_event は約束を要らない: 商品の値 (core.*) を書かず、知らせの届け先の状態だけ。
--        取り込みは保存とは別の時 (product-hub のボードを開いたとき) に動く。借り (lease) の持ち主だけが結果を書ける
--      ・新しいコードの決まり (形・Company DB / 名札 / NE の元のコード / 消したコード) は ops.new_sku_code_problem 1 か所 (画面の確かめと登録の関数が同じものを呼ぶ)
--      ・🚨 画面のロールが関数を直接呼んでも、書く値を DB が確かめる / 作り直す (#1566 Codex R3 Medium 1。アプリの計算を信じない):
--        単品 = 税率と税区分が合う・名前・売価・送料コードと発送方法・原価は人が入れた原価 (manual / COMPLETE) を DB の今日 (東京) から・セットだけの列は持たない /
--        セット = 構成品を DB で確かめて鍵を取り、税率・税区分・取扱・売上分類・原価を DB で導き直して (ops.new_set_derivation) 同じときだけ /
--        構成の rows・rows_hash とカードの知らせの hash は DB が作る (ops.js_stable_sha256 = 画面・取り込みの stable と同じ形)・カードの写しの欄は書く値と同じ /
--        約束と保存の記録の payload_hash = DB が作った「関数が書いた値」のハッシュ (画面の要求のハッシュは記録の結果 request_payload_hash = 同じ request_id の確かめ)
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
comment on table ops.master_registrations is 'SKU ごとの登録の状態 (0052)。行が無い = 使えない。書くのは security definer の関数だけ (画面・運用のロールに DML を渡さない)';

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
comment on table ops.master_registration_backfill is '既存の SKU を available にした印 (0052・切替の手順で 1 回だけ)。この後は状態の行の無い SKU を commit できない・new_open の前提';

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
  v_sess    ops.master_write_sessions;
  v_actor   text := p_actor;
  v_req     text := p_request_id;
  v_reason  text := p_reason;
begin
  if p_actor is null or length(p_actor) = 0 then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  -- 画面のロール = 同じ SKU の登録の約束 (0051 の約束の表・operation = sku_create = ops.register_new_sku だけが書く) の中だけ。
  --   誰が・request_id・理由はその行から (引数と違えば 42501)
  if v_db_user = 'master_edit' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'sku_create' or v_sess.sku_id is distinct from p_sku_id then
      raise exception 'master_write_session_required: 下書きの状態は、その SKU の登録の約束 (ops.register_new_sku) の中だけで作る' using errcode = '42501';
    end if;
    if p_actor is distinct from v_sess.actor_id or (p_request_id is not null and p_request_id is distinct from v_sess.request_id::text) then
      raise exception 'master_write_session_mismatch: 誰が・request_id が登録の約束と違う' using errcode = '42501';
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

-- 4. 切替の門: new_open に進む前提 (R1 H1)。問題の一覧 (空 = 進んでよい)。0051 の差し込み口の表 ops.master_cutover_prereq_checks に 1 行足す
--    = ops.set_master_cutover_phase が集める関数 (ops.master_cutover_prereq_problems) から呼んで断る (prereq_failed: 0052_registrations: ...)。
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
insert into ops.master_cutover_prereq_checks (name, fn) values ('0052_registrations', 'ops.master_registrations_prereq(text, text)');

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
comment on view ops.v_sku_distributable is '古い表への写し (④) に載せてよい SKU (0052)。状態の行が無い SKU は入らない';
create view ops.v_sku_available as
  select s.sku_id, s.company_id, s.code, s.code_norm, s.sku_kind, r.state
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state = 'available';
comment on view ops.v_sku_available is '業務で使ってよい SKU (0052)。状態の行が無い SKU は入らない (fail-closed)';

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
comment on table ops.product_hub_outbox is 'product-hub のカードを作る知らせ (0052)。登録と同じ取引で書く。中身は変えない・消さない。消費は cdb_sku_id の一意で冪等';

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
-- 呼び手が master_edit の知らせの insert = 登録の約束 (sku_create) の中で、その SKU・request_id・人のものだけ (ops.register_new_sku の中だけ)。
--   画面のロールには知らせの insert を渡さない (表の権限が境界・これは関数の中の書き方の保険)。0051 の guard (trg_master_edit_guard) は付けない
--   (0051 の guard は知っている表の「約束の相手」しか分からない = 知らせの表は分からない。ここで同じことを見る)
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない
create function ops.guard_product_hub_outbox_session() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' then return new; end if;
  v_sess := ops.current_master_write_session();
  if v_sess.session_id is null or v_sess.operation is distinct from 'sku_create' then
    raise exception 'master_write_session_required: カードの知らせは、登録の約束 (ops.register_new_sku) の中だけで書く' using errcode = '42501';
  end if;
  if new.sku_id is distinct from v_sess.sku_id or new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then
    raise exception 'master_write_session_mismatch: カードの知らせの SKU・request_id・作った人が登録の約束と違う' using errcode = '42501';
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
comment on view ops.v_product_hub_outbox_open is 'product-hub のカード作成待ち (0052)。done 以外の知らせの状態ごとの数 (見張り・毎朝のまとめ)';

-- 7. 保存の記録に新商品の登録
alter table ops.master_edit_requests drop constraint master_edit_requests_operation_check;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create'));

-- 8. 新商品の登録 (上の 8.)
-- 8a. 0051 の約束の表に「登録の約束」(sku_create)。書くのは ops.register_new_sku だけ (ops.begin_master_write は sku_edit のまま)
do $$
declare v text;
begin
  select c.conname into strict v from pg_catalog.pg_constraint c
   where c.conrelid = 'ops.master_write_sessions'::regclass and c.contype = 'c' and pg_catalog.pg_get_constraintdef(c.oid) like '%operation%';
  execute format('alter table ops.master_write_sessions drop constraint %I', v);
  -- 約束の SKU の外部キーを遅らせられる形に (ふだんはすぐ確かめる = initially immediate)。登録の関数だけが set constraints ops.fk_mws_sku deferred にする
  --   (登録の約束は、SKU の行より先に、関数が振った番号で書く)。initially deferred にしない = 復元 (行を入れた後に trigger を戻す) が「待っている確かめ」で止まらない
  select c.conname into strict v from pg_catalog.pg_constraint c
   where c.conrelid = 'ops.master_write_sessions'::regclass and c.contype = 'f' and c.confrelid = 'core.skus'::regclass;
  execute format('alter table ops.master_write_sessions rename constraint %I to fk_mws_sku', v);
  alter table ops.master_write_sessions alter constraint fk_mws_sku deferrable initially immediate;
end $$;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create'));

-- 8b. 操作ごとの書いてよい (表・書き方)。sku_edit の行は 0051 と同じ。sku_create = 新しい商品・SKU と、その仕入先・原価・構成の依頼の INSERT と、
--     0026 の version の付け替え (仕入先・原価を入れると SKU の・SKU を入れると商品の version を上げる UPDATE)。相手の行は 0051 の guard が約束で見る (新しい SKU・商品だけ)
create or replace function ops.master_write_allowed(p_operation text, p_table text, p_op text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from (values
      ('sku_edit', 'core.skus', 'UPDATE'), ('sku_edit', 'core.products', 'UPDATE'),
      ('sku_edit', 'core.supplier_skus', 'INSERT'), ('sku_edit', 'core.supplier_skus', 'UPDATE'),
      ('sku_edit', 'core.sku_costs', 'INSERT'), ('sku_edit', 'core.sku_costs', 'UPDATE'), ('sku_edit', 'core.sku_costs', 'DELETE'),
      ('sku_edit', 'ops.sku_component_requests', 'INSERT'), ('sku_edit', 'ops.sku_component_requests', 'UPDATE'),
      ('sku_edit', 'ops.sku_component_breaches', 'UPDATE'),
      ('sku_create', 'core.products', 'INSERT'), ('sku_create', 'core.products', 'UPDATE'),
      ('sku_create', 'core.skus', 'INSERT'), ('sku_create', 'core.skus', 'UPDATE'),
      ('sku_create', 'core.supplier_skus', 'INSERT'), ('sku_create', 'core.sku_costs', 'INSERT'),
      ('sku_create', 'ops.sku_component_requests', 'INSERT')) as m(op, tbl, act)
    where m.op = p_operation and m.tbl = p_table and m.act = p_op)
$$;

-- 8c. 新しいコードの決まり (画面の確かめ lib/master-register.mjs の checkNewCodeInDb と登録の関数が同じものを呼ぶ)。問題が無ければ null
--   code_shape = 小文字の英字・数字・- と _ の 1〜30 字・set- で始まらない (lib/master-write.mjs の validateNewSkuCode と同じ)
--   code_taken = Company DB にある / code_is_rep = 代表 (名札)・商品のコード / code_in_ne = NE の元のコード (0041) / code_used_before = 前に使って消した SKU のコード
create function ops.new_sku_code_problem(p_code text) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_norm text;
begin
  if p_code is null or p_code !~ '^[a-z0-9_-]{1,30}$' or p_code ~ '^set-' then return 'code_shape'; end if;
  v_norm := core.norm_code(p_code);
  if exists (select 1 from core.skus where company_id = 1 and code_norm = v_norm) then return 'code_taken'; end if;
  if exists (select 1 from core.products where company_id = 1 and core.norm_code(display_code) = v_norm) then return 'code_is_rep'; end if;
  if exists (select 1 from ops.master_ne_codes where code_norm = v_norm) then return 'code_in_ne'; end if;
  if exists (select 1 from events.master_change_events
              where entity_type = 'sku' and operation = 'DELETE' and core.norm_code(old_value ->> 'code') = v_norm) then return 'code_used_before'; end if;
  return null;
end $$;
revoke all on function ops.new_sku_code_problem(text) from public;

-- 8d. 画面 (JS) の stable() と同じ形の JSON の文字 (キーは文字の順・空白なし・数は整数だけ)。カードの知らせの hash・構成の rows_hash を DB でも作る
--     (lib/master-write.mjs の stable = 取り込み (apps/product-hub) が sha256(stable(payload)) で確かめる)。整数でない数・2^53 を超える数は拒む (JS と形が合わない)
--     🚨 キーは ASCII (0x20〜0x7e) だけ: JS はキーを UTF-16 の単位の順・ここは C (UTF-8 のバイト) の順で並べる = ASCII の外 (U+E000 と 😀 など) で並びが違う。
--        ASCII の外のキーは拒む (知らせの payload のキーは決まった ASCII の名前だけ = 登録の関数がキーの一覧でも確かめる・#1566 Codex R4 Low)
create function ops.js_stable(p jsonb) returns text language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v_t   text := pg_catalog.jsonb_typeof(p);
  v_out text;
  v_n   numeric;
begin
  if p is null or v_t = 'null' then return 'null'; end if;
  if v_t = 'object' then
    if exists (select 1 from pg_catalog.jsonb_object_keys(p) k where k !~ '^[ -~]*$') then
      raise exception 'invalid_input: JSON のキーは ASCII だけ (JS と並びが合わない)' using errcode = '22023';
    end if;
    select '{' || coalesce(string_agg(pg_catalog.to_json(e.key)::text || ':' || ops.js_stable(e.value), ',' order by e.key collate "C"), '') || '}' into v_out
      from pg_catalog.jsonb_each(p) e;
    return v_out;
  elsif v_t = 'array' then
    select '[' || coalesce(string_agg(ops.js_stable(e.value), ',' order by e.ord), '') || ']' into v_out
      from pg_catalog.jsonb_array_elements(p) with ordinality as e(value, ord);
    return v_out;
  elsif v_t = 'string' then
    return pg_catalog.to_json(p #>> '{}')::text;
  elsif v_t = 'boolean' then
    return p::text;
  end if;
  v_n := (p::text)::numeric;
  if v_n <> pg_catalog.trunc(v_n) or abs(v_n) > 9007199254740991 then
    raise exception 'invalid_input: 知らせ・構成の数は整数だけ (%)', p::text using errcode = '22023';
  end if;
  return pg_catalog.trunc(v_n)::text;
end $$;
revoke all on function ops.js_stable(jsonb) from public;
create function ops.js_stable_sha256(p jsonb) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(ops.js_stable(p), 'UTF8')), 'hex')
$$;
revoke all on function ops.js_stable_sha256(jsonb) from public;

-- 8e. 新しいセットの構成品を DB で確かめて、導く値を DB で決める (lib/master-set-rules.js の deriveSetCdb と同じ決め方を SQL で。登録の関数だけが呼ぶ)
--   p_rows = [{ sku_id, code, qty, sort }] (並び = sort = 1〜N の配列の順)・p_today = 原価を見る日 (東京の今日)
--   確かめる: 1〜20 行・各行は sku_id (整数)・code (文字)・qty (1〜999 の整数)・sort だけ・sku_id は重ならない・ある単品・code が SKU のコードと同じ・
--             登録をやめた (cancelled)・要確認 (quarantined)・状態の行が無い商品でない。SKU ごとの鍵 (画面と同じ鍵) を sku_id の順に取り、行を for share で読む
--   決める: 税率 (1 つでも未入力 = 決まらない・混ざれば低い方 + MIXED)・売上分類 (構成品の MIN・輸出 4 と 1〜3 の混在 / 未入力 = 決まらない)・
--           取扱 (セット自身か構成品が 1 つでも中止 = 中止)・原価 (全部の構成品にその日の原価 (COMPLETE / OVERRIDDEN・> 0) があれば 数量 × 原価 の合計 = COMPLETE)
--   戻り値 { rows (DB の値で作り直した構成), rows_hash (ops.js_stable_sha256), tax_rate, tax_class, sales_from_components, handling, cost_status, cost_jpy,
--            stopped_codes (中止の構成品のコード・'・' でつなぐ = 気をつけること) }
create function ops.new_set_derivation(p_rows jsonb, p_handling_own text, p_today date) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_n       integer;
  v_ids     bigint[];
  v_bad     text;
  v_rows    jsonb;
  v_rates   numeric[];
  v_classes integer[];
  v_tax     numeric;
  v_tclass  text;
  v_sales   integer;
  v_stop    boolean;
  v_stopped text;
  v_all     boolean;
  v_any     boolean;
  v_sum     numeric;
begin
  if p_rows is null or pg_catalog.jsonb_typeof(p_rows) <> 'array' then raise exception 'invalid_input: 構成品 (rows) は配列' using errcode = '22023'; end if;
  v_n := pg_catalog.jsonb_array_length(p_rows);
  if v_n < 1 or v_n > 20 then raise exception 'invalid_input: 構成品は 1〜20 行 (% 行)', v_n using errcode = '22023'; end if;
  if exists (select 1 from pg_catalog.jsonb_array_elements(p_rows) with ordinality as e(v, ord)
              where pg_catalog.jsonb_typeof(e.v) <> 'object'
                 or exists (select 1 from pg_catalog.jsonb_object_keys(e.v) k where k not in ('sku_id', 'code', 'qty', 'sort'))
                 or pg_catalog.jsonb_typeof(e.v -> 'sku_id') is distinct from 'number' or (e.v ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$'
                 or pg_catalog.jsonb_typeof(e.v -> 'code') is distinct from 'string'
                 or pg_catalog.jsonb_typeof(e.v -> 'qty') is distinct from 'number' or (e.v ->> 'qty') !~ '^[1-9][0-9]{0,2}$'
                 or pg_catalog.jsonb_typeof(e.v -> 'sort') is distinct from 'number' or (e.v ->> 'sort') is distinct from e.ord::text) then
    raise exception 'invalid_input: 構成品の行は { sku_id, code, qty (1〜999), sort (1 からの並び) } だけ' using errcode = '22023';
  end if;
  select array_agg(distinct (e ->> 'sku_id')::bigint order by (e ->> 'sku_id')::bigint) into v_ids from pg_catalog.jsonb_array_elements(p_rows) e;
  if pg_catalog.cardinality(v_ids) <> v_n then raise exception 'invalid_input: 構成品が重なっている' using errcode = '22023'; end if;
  -- SKU ごとの鍵 (画面の lib/master-register.mjs と同じ鍵・sku_id の順) → 行を for share
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || x::text, 0)) from unnest(v_ids) as t(x) order by x;
  perform 1 from core.skus where sku_id = any(v_ids) order by sku_id for share;
  -- 構成品の値 (その日の原価 = 0049 と同じ選び方: 始まりが遅い → 作った時刻が遅い → 番号が大きい)
  with r as (
    select e.ord, (e.v ->> 'sku_id')::bigint as sku_id, e.v ->> 'code' as code, (e.v ->> 'qty')::integer as qty
      from pg_catalog.jsonb_array_elements(p_rows) with ordinality as e(v, ord)),
  f as (
    select r.*, k.code as k_code, k.sku_kind, k.tax_rate, k.handling, p.sales_class, reg.state,
           (select case when y.cost_status in ('COMPLETE', 'OVERRIDDEN') then y.cost_jpy end
              from core.sku_costs y where y.sku_id = r.sku_id and y.valid_from <= p_today and (y.valid_to is null or y.valid_to >= p_today)
             order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1) as cost_jpy
      from r left join core.skus k on k.sku_id = r.sku_id left join core.products p on p.product_id = k.product_id
      left join ops.master_registrations reg on reg.sku_id = r.sku_id)
  select (select string_agg(format('%s (%s)', f.code, case when f.k_code is null then '無い SKU' when f.k_code <> f.code then 'コードが違う'
                                                          when f.sku_kind <> 'single' then '単品でない' else coalesce('状態 ' || f.state, '状態の行が無い') end), '・' order by f.ord)
            from f where f.k_code is null or f.k_code <> f.code or f.sku_kind <> 'single' or f.state is null or f.state in ('cancelled', 'quarantined')),
         (select jsonb_agg(jsonb_build_object('sku_id', f.sku_id, 'code', f.k_code, 'qty', f.qty, 'sort', f.ord) order by f.ord) from f),
         (select array_agg(f.tax_rate) from f),
         (select array_agg(f.sales_class::integer) from f),
         (select bool_or(f.handling = 'discontinued') from f),
         (select string_agg(f.k_code, '・' order by f.ord) from f where f.handling = 'discontinued'),
         (select bool_and(coalesce(f.cost_jpy, 0) > 0) from f),
         (select bool_or(coalesce(f.cost_jpy, 0) > 0) from f),
         (select sum(f.cost_jpy * f.qty) from f)
    into v_bad, v_rows, v_rates, v_classes, v_stop, v_stopped, v_all, v_any, v_sum;
  if v_bad is not null then raise exception 'component_unusable: 構成品に使えない商品がある: %', v_bad using errcode = 'P0001'; end if;
  -- 税率
  if exists (select 1 from unnest(v_rates) x where x is null) then
    v_tax := null; v_tclass := 'UNKNOWN';
  elsif (select count(distinct x) from unnest(v_rates) x) = 1 then
    v_tax := v_rates[1]; v_tclass := case when v_tax = 0.08 then 'REDUCED_8' when v_tax = 0.10 then 'STANDARD_10' else 'UNKNOWN' end;
    if v_tclass = 'UNKNOWN' then v_tax := null; end if;
  else
    select min(x) into v_tax from unnest(v_rates) x; v_tclass := 'MIXED';
  end if;
  -- 売上分類 (構成品から)
  if exists (select 1 from unnest(v_classes) x where x is null or x not between 1 and 4)
     or (4 = any(v_classes) and exists (select 1 from unnest(v_classes) x where x <> 4)) then
    v_sales := null;
  else
    select min(x) into v_sales from unnest(v_classes) x;
  end if;
  return jsonb_build_object('rows', v_rows, 'rows_hash', ops.js_stable_sha256(v_rows), 'tax_rate', v_tax, 'tax_class', v_tclass, 'sales_from_components', v_sales,
    'handling', case when p_handling_own = 'discontinued' or coalesce(v_stop, false) then 'discontinued' else 'active' end,
    'cost_status', case when v_all then 'COMPLETE' when v_any then 'PARTIAL' else 'MISSING' end,
    'cost_jpy', case when v_all then pg_catalog.round(pg_catalog.round(v_sum, 2), 0) end, 'stopped_codes', v_stopped);
end $$;
revoke all on function ops.new_set_derivation(jsonb, text, date) from public;

-- 8f. 新商品を登録する (同じ取引で 1 回。画面 = lib/master-register.mjs が鍵 (request_id → 段階 → マスタの書き込み → 新しいコード → 構成品) と門・値の確かめの後に呼ぶ)
--   p_entry = { kind: single | set, code, started_at,
--               product: { name, sales_class, expiry_managed, inbound_date_managed } (単品だけ),
--               sku: { name, tax_rate, tax_class, handling, standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own },
--               supplier_id (単品・代表・無くてよい), cost: { jpy, source, status, valid_from, reason } (単品は無くてよい・セットは要る),
--               component_request: { rows: [{ sku_id, code, qty, sort }], rows_hash, reason } (セットだけ),
--               card: { schema_version, payload, payload_hash } (カードを作らないなら null) }   🚨 result は渡さない (結果は DB が作る・渡せば断る)
--   🚨 画面のロールが直接呼んでも、書く値を DB が確かめる / 作り直す (#1566 Codex R3 Medium 1。アプリの計算を信じない):
--      ・段階 new_open・持ち主表のハッシュ (ops.begin_master_write と同じ)・backfill がちょうど 1 回・request_id が未使用・コードの決まり (ops.new_sku_code_problem)
--      ・共通: 名前 (1〜255 字・制御文字なし・empty でない)・売価 1〜999,999,999 の整数・送料コードと発送方法 (名前) がある・送料 0 以上 (無くてよい)・推奨保有月数 0〜60
--      ・単品: 税率 8% / 10% と税区分が合う (REDUCED_8 / STANDARD_10)・取扱 active・商品の名前 = SKU の名前・売上分類 1〜4 か無し・
--        セットだけの列 (売上分類の上書き・セット自身の取扱・構成) は持たない・原価は無いか manual / COMPLETE・0 以上の整数・今日 (東京) から・理由つき
--      ・セット: 構成品を DB で確かめて鍵を取り、税率・税区分・取扱・売上分類・原価を DB で導いて (ops.new_set_derivation) 入ってきた値と同じときだけ。
--        売上分類の上書きは構成品から導けないときだけ・導けず上書きも無い = 断る。原価 = 導いた合計 (set_calc / COMPLETE) か、例外原価 (manual / OVERRIDDEN・理由つき) だけ。
--        仕入先・商品の行は持たない
--      ・構成の rows・rows_hash = DB で作り直した値を書く。カードの知らせ = payload の写しの欄 (版・コード・種類・名前・売価・送料・構成品・作った人) が書く値と同じで、
--        SKU の番号を payload に入れない・payload_hash は DB が作る (ops.js_stable_sha256 = 取り込みの確かめと同じ形)
--      ・約束と保存の記録の payload_hash = DB が作った「関数が書いた値」のハッシュ (アプリの要求のハッシュ p_payload_hash は記録の結果 request_payload_hash に残す = 同じ request_id の確かめ)
--   列の持ち主・業務の約束 (代表の仕入先は取引中) は 0051 の guard が書くときに見る。
--   🚨 例外の受け止め (begin ... exception) を使わない: サブトランザクションの中で書いた done は xmin が取引の番号と違い、0051 の commit の確かめに数えられない
create function ops.register_new_sku(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user  text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tx       bigint := pg_catalog.txid_current();
  v_today    date := (pg_catalog.now() at time zone 'Asia/Tokyo')::date;
  v_kind     text := p_entry ->> 'kind';
  v_code     text := p_entry ->> 'code';
  v_prod     jsonb := p_entry -> 'product';
  v_s        jsonb := p_entry -> 'sku';
  v_cost     jsonb := p_entry -> 'cost';
  v_req      jsonb := p_entry -> 'component_request';
  v_card     jsonb := p_entry -> 'card';
  v_reason   text := nullif(p_reason, '');
  v_name     text;
  v_price    bigint;
  v_tax      numeric;
  v_tclass   text;
  v_handling text;
  v_override smallint;
  v_own      text;
  v_sales    smallint;
  v_ship_c   text;
  v_ship_m   text;
  v_ship_y   bigint;
  v_reorder  numeric;
  v_supplier bigint;
  v_cjpy     bigint;
  v_csrc     text;
  v_cstat    text;
  v_creason  text;
  v_der      jsonb;
  v_rows     jsonb;
  v_rhash    text;
  v_payload  jsonb;
  v_phash    text;
  v_written  jsonb;
  v_whash    text;
  v_phase    text;
  v_owner    text;
  v_hash     text;
  v_bad      text;
  v_product  bigint;
  v_sku      bigint;
  v_sess     uuid := pg_catalog.gen_random_uuid();
  v_event    uuid;
  v_result   jsonb;
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.master_cutover'));   -- 段階を変える取引と並ぶ (画面は先に取っている = 同じ鍵)
  perform pg_catalog.pg_advisory_xact_lock_shared(core.master_write_lock_key());                -- 夜間ロードと並ぶ (画面は先に取っている = 同じ鍵)
  -- 形 (呼び手・持ち主表・要求のハッシュ・中身の外形)
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if p_actor_id is null or length(btrim(p_actor_id)) = 0 or length(p_actor_id) > 320 or p_actor_id ~ '[[:cntrl:]]' then
    raise exception 'invalid_input: 登録する人 (actor_id) の形が違う' using errcode = '22023';
  end if;
  if v_reason is not null and (length(v_reason) > 200 or v_reason ~ '[[:cntrl:]]') then raise exception 'invalid_input: 理由は 200 字まで・制御文字なし' using errcode = '22023'; end if;
  if p_ownership is null or jsonb_typeof(p_ownership) <> 'object'
     or exists (select 1 from jsonb_each(p_ownership) e where jsonb_typeof(e.value) <> 'string' or (e.value #>> '{}') not in ('load', 'company')) then
    raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023';
  end if;
  if coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: 要求のハッシュ (64 桁の 16 進) が要る' using errcode = '22023'; end if;
  if p_entry is null or jsonb_typeof(p_entry) <> 'object' or v_kind is null or v_kind not in ('single', 'set') or jsonb_typeof(v_s) is distinct from 'object' then
    raise exception 'invalid_input: 登録の中身 (kind = single / set・sku) が要る' using errcode = '22023';
  end if;
  if (v_kind = 'single') is distinct from (jsonb_typeof(v_prod) = 'object') then
    raise exception 'invalid_input: 単品は商品 (product) が要り、セットは商品を作らない' using errcode = '22023';
  end if;
  if (v_kind = 'set') is distinct from (jsonb_typeof(v_req) = 'object') then
    raise exception 'invalid_input: セットは構成の依頼 (component_request) が要り、単品は構成を持たない' using errcode = '22023';
  end if;
  -- 結果は DB が作る (呼び手の結果は受け取らない = 保存の記録の結果を偽れない・#1566 Codex R4 Medium)
  if p_entry ? 'result' then raise exception 'invalid_input: 登録の結果 (result) は DB が作る (渡さない)' using errcode = '22023'; end if;
  if coalesce(p_entry -> 'supplier_id', 'null'::jsonb) <> 'null'::jsonb
     and (jsonb_typeof(p_entry -> 'supplier_id') not in ('number', 'string') or (p_entry ->> 'supplier_id') !~ '^[1-9][0-9]{0,17}$') then
    raise exception 'invalid_input: 代表の仕入先は番号' using errcode = '22023';
  end if;
  v_supplier := (p_entry ->> 'supplier_id')::bigint;
  if v_kind = 'set' and v_supplier is not null then raise exception 'invalid_input: セットに代表の仕入先は付けない' using errcode = '22023'; end if;
  -- 段階・持ち主表 (ops.begin_master_write と同じ)・backfill
  select phase, owner_hash into v_phase, v_owner from ops.master_cutover_state where id = 1;
  v_hash := ops.ownership_hash(p_ownership);
  if v_phase is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が % (new_open でない)', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
  if v_owner is distinct from v_hash then raise exception 'before_cutover: 持ち主表が切替のときの記録と違う' using errcode = 'P0001'; end if;
  if (select count(*) from ops.master_registration_backfill) <> 1 then
    raise exception 'backfill_missing: 既存の SKU の登録の状態 (backfill) が済んでいないので、新商品は登録しない' using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'invalid_input: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '22023';
  end if;
  -- コード (新しいコードの鍵 = 画面と同じ鍵 → 決まり)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.new_code:' || coalesce(core.norm_code(v_code), ''), 0));
  v_bad := ops.new_sku_code_problem(v_code);
  if v_bad is not null then raise exception '%: 商品コード % は新しい商品に使えない', v_bad, v_code using errcode = 'P0001'; end if;

  -- 値 (共通): 名前・売価・送料・推奨保有月数
  v_name := v_s ->> 'name';
  if jsonb_typeof(v_s -> 'name') is distinct from 'string' or v_name is distinct from btrim(v_name) or length(v_name) not between 1 and 255
     or v_name ~ '[[:cntrl:]]' or lower(v_name) = 'empty' then
    raise exception 'invalid_value: 名前は 1〜255 字 (前後の空白・制御文字なし・empty でない)' using errcode = '22023';
  end if;
  if jsonb_typeof(v_s -> 'standard_price_jpy') is distinct from 'number' or (v_s ->> 'standard_price_jpy') !~ '^[1-9][0-9]{0,8}$' then
    raise exception 'invalid_value: 売価は 1〜999,999,999 の整数' using errcode = '22023';
  end if;
  v_price := (v_s ->> 'standard_price_jpy')::bigint;
  v_ship_c := v_s ->> 'shipping_code';
  v_ship_m := v_s ->> 'shipping_method';
  if jsonb_typeof(v_s -> 'shipping_code') is distinct from 'string' or length(btrim(v_ship_c)) not between 1 and 20 or v_ship_c ~ '[[:cntrl:]]'
     or jsonb_typeof(v_s -> 'shipping_method') is distinct from 'string' or length(btrim(v_ship_m)) not between 1 and 200 or v_ship_m ~ '[[:cntrl:]]' then
    raise exception 'invalid_value: 送料コードと発送方法 (名前) が要る' using errcode = '22023';
  end if;
  if (v_s -> 'shipping_cost_jpy') is not null and jsonb_typeof(v_s -> 'shipping_cost_jpy') <> 'null'
     and (jsonb_typeof(v_s -> 'shipping_cost_jpy') <> 'number' or (v_s ->> 'shipping_cost_jpy') !~ '^[0-9]{1,9}$') then
    raise exception 'invalid_value: 送料は 0 以上の整数か無し' using errcode = '22023';
  end if;
  v_ship_y := case when jsonb_typeof(v_s -> 'shipping_cost_jpy') = 'number' then (v_s ->> 'shipping_cost_jpy')::bigint end;
  if (v_s -> 'reorder_months') is not null and jsonb_typeof(v_s -> 'reorder_months') <> 'null'
     and (jsonb_typeof(v_s -> 'reorder_months') <> 'number' or (v_s ->> 'reorder_months') !~ '^[0-9]{1,2}(\.[0-9])?$' or (v_s ->> 'reorder_months')::numeric > 60) then
    raise exception 'invalid_value: 推奨保有月数は 0〜60 (小数は 1 桁まで) か無し' using errcode = '22023';
  end if;
  v_reorder := case when jsonb_typeof(v_s -> 'reorder_months') = 'number' then (v_s ->> 'reorder_months')::numeric end;
  -- 原価の形 (無い / { jpy, source, status, valid_from, reason })
  if v_cost is not null and jsonb_typeof(v_cost) <> 'null' then
    if jsonb_typeof(v_cost) <> 'object' or jsonb_typeof(v_cost -> 'jpy') is distinct from 'number' or (v_cost ->> 'jpy') !~ '^[0-9]{1,9}$'
       or (v_cost ->> 'valid_from') is distinct from v_today::text
       or jsonb_typeof(v_cost -> 'reason') is distinct from 'string' or length(btrim(v_cost ->> 'reason')) not between 1 and 200 or (v_cost ->> 'reason') ~ '[[:cntrl:]]' then
      raise exception 'invalid_value: 原価は 0〜999,999,999 の整数・今日 (東京 %) から・理由つき', v_today using errcode = '22023';
    end if;
    v_cjpy := (v_cost ->> 'jpy')::bigint; v_csrc := v_cost ->> 'source'; v_cstat := v_cost ->> 'status'; v_creason := v_cost ->> 'reason';
  end if;

  if v_kind = 'single' then
    -- 単品: 税率と税区分が合う・取扱 active・セットだけの列は持たない・商品の名前 = SKU の名前・原価は manual / COMPLETE だけ
    if jsonb_typeof(v_s -> 'tax_rate') is distinct from 'number' or (v_s ->> 'tax_rate')::numeric not in (0.08, 0.10) then
      raise exception 'invalid_value: 単品の税率は 8%% か 10%%' using errcode = '22023';
    end if;
    v_tax := (v_s ->> 'tax_rate')::numeric;
    v_tclass := case when v_tax = 0.08 then 'REDUCED_8' else 'STANDARD_10' end;
    if (v_s ->> 'tax_class') is distinct from v_tclass then raise exception 'invalid_value: 税率 % と税区分 % が合わない', v_tax, v_s ->> 'tax_class' using errcode = '22023'; end if;
    if (v_s ->> 'handling') is distinct from 'active' then raise exception 'invalid_value: 新しい単品の取扱は active' using errcode = '22023'; end if;
    v_handling := 'active';
    if coalesce(v_s -> 'set_sales_class_override', 'null'::jsonb) <> 'null'::jsonb or coalesce(v_s -> 'handling_own', 'null'::jsonb) <> 'null'::jsonb then
      raise exception 'invalid_value: 単品にセットだけの列 (売上分類の上書き・セット自身の取扱) は付けない' using errcode = '22023';
    end if;
    if (v_prod ->> 'name') is distinct from v_name then raise exception 'invalid_value: 商品の名前は SKU の名前と同じ' using errcode = '22023'; end if;
    if coalesce(v_prod -> 'sales_class', 'null'::jsonb) <> 'null'::jsonb
       and (jsonb_typeof(v_prod -> 'sales_class') <> 'number' or (v_prod ->> 'sales_class') !~ '^[1-4]$') then
      raise exception 'invalid_value: 売上分類は 1〜4 か無し' using errcode = '22023';
    end if;
    v_sales := (v_prod ->> 'sales_class')::smallint;
    if jsonb_typeof(v_prod -> 'expiry_managed') is distinct from 'boolean'
       or coalesce(jsonb_typeof(v_prod -> 'inbound_date_managed'), 'null') not in ('boolean', 'null') then
      raise exception 'invalid_value: 有効期限の管理・入荷日の管理は あり / なし' using errcode = '22023';
    end if;
    if v_cjpy is not null and (v_csrc is distinct from 'manual' or v_cstat is distinct from 'COMPLETE') then
      raise exception 'invalid_value: 単品の原価は人が入れた原価 (manual / COMPLETE) だけ' using errcode = '22023';
    end if;
  else
    -- セット: 構成品を DB で確かめて導く値を DB で決め、入ってきた値と同じときだけ
    v_own := v_s ->> 'handling_own';
    if v_own is null or v_own not in ('active', 'discontinued') then raise exception 'invalid_value: セット自身の取扱は active / discontinued' using errcode = '22023'; end if;
    v_der := ops.new_set_derivation(v_req -> 'rows', v_own, v_today);
    v_rows := v_der -> 'rows';
    v_rhash := v_der ->> 'rows_hash';
    if (v_der ->> 'tax_rate') is null then raise exception 'set_underivable: 構成品の税率が未入力なので、セットの税率が決まらない' using errcode = 'P0001'; end if;
    v_tax := (v_der ->> 'tax_rate')::numeric; v_tclass := v_der ->> 'tax_class'; v_handling := v_der ->> 'handling';
    if jsonb_typeof(v_s -> 'tax_rate') is distinct from 'number' or (v_s ->> 'tax_rate')::numeric <> v_tax or (v_s ->> 'tax_class') is distinct from v_tclass
       or (v_s ->> 'handling') is distinct from v_handling then
      raise exception 'derived_mismatch: セットの税率・税区分・取扱が構成品から導いた値 (% / % / %) と違う', v_tax, v_tclass, v_handling using errcode = 'P0001';
    end if;
    if coalesce(v_s -> 'set_sales_class_override', 'null'::jsonb) <> 'null'::jsonb then
      if jsonb_typeof(v_s -> 'set_sales_class_override') <> 'number' or (v_s ->> 'set_sales_class_override') !~ '^[1-4]$' then
        raise exception 'invalid_value: 売上分類の上書きは 1〜4' using errcode = '22023';
      end if;
      v_override := (v_s ->> 'set_sales_class_override')::smallint;
      if (v_der ->> 'sales_from_components') is not null then
        raise exception 'derived_mismatch: 構成品から売上分類 (%) を導けるので、上書きはできない', v_der ->> 'sales_from_components' using errcode = 'P0001';
      end if;
    elsif (v_der ->> 'sales_from_components') is null then
      raise exception 'set_underivable: 構成品から売上分類を導けない (上書きが要る)' using errcode = 'P0001';
    end if;
    if v_cjpy is null then raise exception 'set_underivable: セットの原価が要る (構成品の合計か例外原価)' using errcode = 'P0001'; end if;
    if v_csrc = 'set_calc' then
      if v_cstat is distinct from 'COMPLETE' or (v_der ->> 'cost_status') is distinct from 'COMPLETE' or v_cjpy <> (v_der ->> 'cost_jpy')::bigint then
        raise exception 'derived_mismatch: セットの原価が構成品から導いた値 (% / %) と違う', v_der ->> 'cost_status', v_der ->> 'cost_jpy' using errcode = 'P0001';
      end if;
    elsif v_csrc is distinct from 'manual' or v_cstat is distinct from 'OVERRIDDEN' then
      raise exception 'invalid_value: セットの原価は構成品の合計 (set_calc / COMPLETE) か例外原価 (manual / OVERRIDDEN) だけ' using errcode = '22023';
    end if;
  end if;

  -- カードの知らせ: 写しの欄が書く値と同じ・SKU の番号は入れない・hash は DB が作る
  if v_card is not null and jsonb_typeof(v_card) <> 'null' then
    v_payload := v_card -> 'payload';
    if jsonb_typeof(v_card) <> 'object' or (v_card ->> 'schema_version') is distinct from 'ph-card-v1' or jsonb_typeof(v_payload) is distinct from 'object'
       or v_payload ? 'cdb_sku_id' or (v_payload ->> 'schema') is distinct from (v_card ->> 'schema_version')
       or (v_payload ->> 'code') is distinct from v_code or (v_payload ->> 'kind') is distinct from v_kind or (v_payload ->> 'name') is distinct from v_name
       or (v_payload -> 'price') is distinct from to_jsonb(v_price)
       or (v_payload -> 'shipping') is distinct from jsonb_build_object('code', v_ship_c, 'method', v_ship_m, 'cost_jpy', v_ship_y)
       or (v_payload -> 'components') is distinct from coalesce((select jsonb_agg(jsonb_build_object('code', e ->> 'code', 'qty', (e -> 'qty')) order by (e ->> 'sort')::integer)
                                                                  from jsonb_array_elements(v_rows) e), '[]'::jsonb)
       or (v_payload ->> 'created_by') is distinct from p_actor_id
       -- キーは決まった名前だけ (入れ子も)・URL などは文字か null・参考 URL は文字の配列 (#1566 Codex R4 Low)
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof(v_payload) = 'object' then v_payload else '{}'::jsonb end) k where k not in ('schema', 'code', 'kind', 'name', 'price', 'shipping', 'amazon_url', 'asin', 'official_url',
                                                                              'reference_urls', 'set_decision', 'yahoo', 'components', 'created_by'))
       or coalesce(jsonb_typeof(v_payload -> 'set_decision'), 'null') not in ('null', 'object')
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof((v_payload -> 'set_decision')) = 'object' then (v_payload -> 'set_decision') else '{}'::jsonb end) k where k not in ('decision', 'reason_code', 'reason_text'))
       or coalesce(jsonb_typeof(v_payload -> 'yahoo'), 'null') not in ('null', 'object')
       or exists (select 1 from jsonb_object_keys(case when jsonb_typeof((v_payload -> 'yahoo')) = 'object' then (v_payload -> 'yahoo') else '{}'::jsonb end) k where k not in ('price', 'price_sagawa', 'delivery_label', 'category_id', 'path'))
       or coalesce(jsonb_typeof(v_payload -> 'reference_urls'), 'null') <> 'array'
       or exists (select 1 from jsonb_array_elements(case when jsonb_typeof(v_payload -> 'reference_urls') = 'array' then v_payload -> 'reference_urls' else '[]'::jsonb end) u
                   where jsonb_typeof(u) <> 'string')
       or exists (select 1 from unnest(array['amazon_url', 'asin', 'official_url']) f where coalesce(jsonb_typeof(v_payload -> f), 'null') not in ('null', 'string')) then
      raise exception 'invalid_value: カードの知らせの写しの欄 (版・コード・種類・名前・売価・送料・構成品・作った人) が書く値と違うか、知らない欄・形の違う欄がある' using errcode = '22023';
    end if;
    v_phash := ops.js_stable_sha256(v_payload);
  end if;

  -- 関数が書く値のハッシュ (約束と保存の記録の payload_hash = DB が作る。アプリの要求のハッシュも入れて結ぶ)
  v_written := jsonb_build_object('request', p_payload_hash, 'kind', v_kind, 'code', v_code, 'name', v_name, 'tax_rate', v_tax, 'tax_class', v_tclass, 'handling', v_handling,
    'standard_price_jpy', v_price, 'shipping', jsonb_build_array(v_ship_c, v_ship_m, v_ship_y), 'reorder_months', v_reorder, 'set_sales_class_override', v_override,
    'handling_own', v_own, 'product', case when v_kind = 'single' then jsonb_build_object('sales_class', v_sales, 'expiry_managed', (v_prod -> 'expiry_managed'),
      'inbound_date_managed', coalesce(v_prod -> 'inbound_date_managed', 'null'::jsonb)) end,
    'supplier_id', v_supplier, 'cost', case when v_cjpy is not null then jsonb_build_array(v_cjpy, v_csrc, v_cstat, v_today, v_creason) end,
    'rows', v_rows, 'rows_hash', v_rhash, 'card', v_phash, 'reason', v_reason);
  v_whash := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_written::text, 'UTF8')), 'hex');

  -- 番号を振る (商品 → SKU)
  if v_kind = 'single' then v_product := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.products', 'product_id')::regclass); end if;
  v_sku := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.skus', 'sku_id')::regclass);
  -- 登録の約束 (0051 の約束の表・operation = sku_create)。行を入れる前に書く = 0051 の guard・変更の記録が この約束で見る。編集の印は無い (0 の 64 桁)・版は無い ({})
  --   約束の SKU の外部キーは、この取引だけ commit のときに確かめる (SKU の行はこの後に入れる)
  set constraints ops.fk_mws_sku deferred;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_sess, v_tx, p_request_id, 'sku_create', v_sku, '{}'::bigint[], case when v_product is null then '{}'::bigint[] else array[v_product] end,
            pg_catalog.repeat('0', 64), v_whash, '{}'::jsonb, p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, v_hash, p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_sess::text, true);
  -- 行を入れる: 商品 (単品) → SKU → 状態 draft → 仕入先 → 原価 → 構成の依頼 → カードの知らせ (どれも上で確かめた / DB で作った値だけ)
  if v_kind = 'single' then
    insert into core.products (product_id, company_id, display_code, name, sales_class, status, expiry_managed, inbound_date_managed, created_by_type, created_by_id)
      overriding system value
      values (v_product, 1, v_code, v_name, v_sales, 'active', (v_prod ->> 'expiry_managed')::boolean, (v_prod ->> 'inbound_date_managed')::boolean, 'human', p_actor_id);
  end if;
  insert into core.skus (sku_id, company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling, standard_price_jpy,
                         shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own, created_by_type, created_by_id)
    overriding system value
    values (v_sku, 1, v_product, v_kind, v_code, v_name, v_tax, v_tclass, v_handling, v_price, v_ship_c, v_ship_m, v_ship_y, v_reorder, v_override, v_own, 'human', p_actor_id);
  perform ops.create_sku_registration(v_sku, p_actor_id, p_request_id::text, v_reason);
  if v_supplier is not null then
    insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values (1, v_supplier, v_sku, true, 'human', p_actor_id);
  end if;
  if v_cjpy is not null then
    insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id)
      values (1, v_sku, v_cjpy, v_csrc, v_cstat, v_today, v_creason, 'human', p_actor_id);
  end if;
  if v_kind = 'set' then
    insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id)
      values (1, v_sku, v_rows, v_rhash, '[]'::jsonb,
              case when jsonb_typeof(v_req -> 'reason') = 'string' and length(v_req ->> 'reason') between 1 and 200 and (v_req ->> 'reason') !~ '[[:cntrl:]]' then v_req ->> 'reason' end,
              p_actor_id, p_request_id);
  end if;
  if v_phash is not null then
    insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
      values (1, v_sku, 'card_create', 'ph-card-v1', v_payload, v_phash, p_request_id, p_actor_id)
      returning event_id into v_event;
  end if;
  -- 保存の記録 done (約束どおり = 0051 の commit の確かめ)。結果は全部 DB の値から作る (確かめた / 導き直した値・振った番号・知らせ・要求のハッシュ)。
  --   気をつけること・このあと の文は lib/master-register.mjs の前の文と同じ (画面はこの結果をそのまま出す・同じ request_id の答えも同じ)
  v_result := jsonb_build_object(
    'ok', true, 'code', v_code, 'kind', v_kind, 'sku_id', v_sku::text, 'state', 'draft', 'request_id', p_request_id::text,
    'tax', jsonb_build_object('rate', v_tax, 'class', v_tclass), 'handling', v_handling,
    'cost', case when v_cjpy is not null then jsonb_build_object('jpy', v_cjpy, 'source', v_csrc) end,
    'shipping', jsonb_build_object('code', v_ship_c, 'method', v_ship_m, 'cost_jpy', v_ship_y),
    'components', coalesce((select jsonb_agg(jsonb_build_object('code', e ->> 'code', 'qty', e -> 'qty') order by (e ->> 'sort')::integer)
                              from jsonb_array_elements(v_rows) e), '[]'::jsonb),
    'warnings', to_jsonb(array_remove(array[
        case when v_tclass = 'MIXED' then '構成品の税率が 8% と 10% で混ざっています (セットの税率は低い方の 8%・MIXED)' end,
        case when (v_der ->> 'stopped_codes') is not null then format('中止の構成品 (%s) があるので、セットも中止になります', v_der ->> 'stopped_codes') end,
        case when v_name ~ '_(白ビ袋|梱機プ|長3封|白プチ|ネコ段|K-44|K-50|K-60|厚紙封|パフ箱|その他)$'
             then '名前の末尾に資材の印があります。資材は梱包アプリで登録します (新しい名前には付けない・D-47)' end], null)),
    'ne_steps', to_jsonb(array_remove(array[
        '下書きで登録しました。NE・ロジザードへの登録 (新規登録の CSV) はまだです (次の段階で「NE 登録へ進む」を足します)',
        case when v_kind = 'set' then 'セットの構成は「構成の依頼」として持っています。NE に登録して NE の構成が同じと確かめたら、今の構成になります' end], null)),
    'card', case when v_event is null then null else jsonb_build_object('event_id', v_event::text, 'status', 'pending') end,
    'request_payload_hash', p_payload_hash);
  insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
    values (p_request_id, 1, 'sku_create', v_code, v_sku, p_actor_id, v_whash, 'done', v_result,
            least(coalesce((p_entry ->> 'started_at')::timestamptz, pg_catalog.now()), pg_catalog.clock_timestamp()));
  -- 約束を閉じる = この関数の外では (同じ取引でも) 画面のロールは登録の約束で書けない。約束の行と done は commit の確かめに残る
  perform pg_catalog.set_config('ops.master_write_session', '', true);
  return v_result;
end $$;
revoke all on function ops.register_new_sku(uuid, text, text, jsonb, text, jsonb) from public;

-- 見張りは読むだけ。画面・運用のロールの権限は scripts/company-db/create-master-edit-roles.mjs (⑤-1 のロールを作る手の操作。migration の後に流し直す)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_registrations, ops.master_registration_events, ops.master_registration_backfill, ops.product_hub_outbox, ops.v_sku_distributable, ops.v_sku_available, ops.v_product_hub_outbox_open to watcher';
  end if;
end $$;
