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
--        ・場所はログインで決まる (#1563 仮レビュー Low 3): 書けるのはログインのロール master_gate_render / master_gate_minipc (まとめのロール master_gate のメンバー) だけ。
--          session_user が 'master_gate_' || host でなければ拒む (gate_host_mismatch・42501)。記録に session_role を残す
--        ・止まったプロセス: 止めるとき (正しく終わる・人が CLI で「止めた」と書く) は stopped = true + 理由 (stopped_reason) の記録を書く
--        ・段階を進めるとき、今までに 1 回でも記録のあるプロセス全部 (記録の表 = 知っている実体の台帳。年齢では外さない・#1563 R3 High 1) の最後の 1 件を見る:
--          止まった記録 = 外す / ops.master_cutover_ack_fresh_minutes() (15) 分より前 = 黙っている = 拒む (止めたなら stopped の記録を書く。25 時間前でも同じ) /
--          新しい記録 = 全部の場所に 1 件以上・どれも build_id が証拠の expected_builds[場所] の中・manifest_hash と owner_hash が証拠と同じ・見た段階が今の段階・書きかけ 0。
--          1 つでも外れたら拒む。🚨 記録の表は消さない (消すと台帳から実体が消える)
--          legacy_open → frozen     : + drain (書きかけを流し終えた) + 手の入口 (manifest の kind = manual) を止めた一覧が manifest の手の入口と完全に同じ集合。
--                                     drain.checked_at と手の入口の at は今の段階に入った後・サーバーの今以前
--          frozen → company_owner   : + 記録が frozen に入った後 (新しい持ち主表のハッシュ = 証拠の owner_hash を段階に残す)
--          company_owner → new_open : + 記録が company_owner に入った後・owner_hash が company_owner のときと同じ
--          🚨 一度も記録を書かない実体 (門を持たない古い build) は DB からは見えない = 切替の手順で、各場所で動いている実体の一覧と記録を人が照らす
--        ・前提の差し込み口 (#1563 R3): 表 ops.master_cutover_prereq_checks に (名前・関数) を足す。ops.master_cutover_prereq_problems(from, to) が足された関数を全部呼び、
--          問題をつなげて返す (空でなければ進めない)。後の migration (⑤-2a・④a・⑥ の準備) は関数を create or replace せず、表に 1 行足す (表は追記だけ = 前の項目を消さない・#1563 R4 Low 3)
--        ⑤-1 では誰も門の記録を書かない (書くのは ⑤-3) = ⑤-3 が配られるまで legacy_open から進めない
--      🚨 読めない = 閉じている (fail-closed。lib/master-cutover.mjs)。新しい画面の保存 = 段階 new_open **かつ** 持ち主表のハッシュが段階の記録と同じ **かつ** 列が company
--         **かつ** env MASTER_EDIT_OPEN = 1。保存の取引は hashtext('ops.master_cutover') の共有の鍵を持つ = 段階を変える取引 (排他の鍵) と並ぶ
--      🚨 DB でも守る (#1563 R3 M2): 画面のロール master_edit の書き込みは、同じ取引で ops.begin_master_write (security definer) を呼んだ後だけ (下の 8.)
--   2. core.skus.set_sales_class_override (1〜4・null) / handling_own ('active' / 'discontinued'・null) = セットに置く人が決めた値。
--      🚨 sku_kind = 'set' だけ、の CHECK は付けない: 夜間ロードが NE の種類替えを写したときに 1 行の CHECK で夜間ロード全体を止めない
--   3. ops.master_edit_requests = 保存 1 回 = 1 行。done は保存と同じ取引・failed は巻き戻った後に同じ request_id の鍵を取って (既に結果があれば書かない)。追記だけ
--   4. セットの構成: core.sku_components = 最後に確かめた構成 (画面は書かない)。ops.sku_component_requests = 依頼 (セットごとに開いているのは 1 つ)。
--      ops.ne_set_observation_runs / ops.ne_set_observations = NE のセットの構成の観測 (追記だけ)。書くのは ops.record_ne_set_observations (security definer・観測のロール master_observer だけ)。
--        完全な回 (complete) = 知らない・セットでない・重なる・並びの分からない・知らない構成品の行が 1 つも無い・求めた数 = 取った数 = 残した数・原本のハッシュと取得の世代がある。
--        観測の時刻は未来 (5 分より先) にしない・36 時間より前にしない。
--        数は厳密な整数 (#1563 R3 M3): requested / fetched = 0〜1,000,000・qty = 1〜99,999・sort = セットごとに 1〜N (送り手 = 夜間ロードは NE の並びに 1 から番号を振る)。
--        0.6・1.0・-1 などは形の誤り (完全な回 = 拒む・完全でない回 = そのセットを飛ばす)
--        書くときに、観測するセットの SKU ごとの鍵を sku_id の順に取る (昇格と同じ鍵 = 古い観測の昇格と新しい観測の書き込みが並ぶ・#1563 R3 M4)
--      ops.sku_component_breaches = 食い違い (NE でやること): mismatch / stale / unrequested_diff / underivable (NE は依頼どおりだが、依頼の構成では導く値が決まらない)
--      依頼を上げるのは lib/master-write.mjs の promoteComponentRequest(観測の番号) だけ (夜間ロード = 持ち主のロール)
--   5. core.sku_costs: 期間の重なりの守り (この画面と昇格の書き込み・持ち主でないロールの書き込み) と、持ち主でないロールは過去の行を消さない・閉じた行を変えない
--      ・期間を縮めるだけの UPDATE (同じ SKU・新しい期間が前の期間の中) は見ない (新しい重なりを作れない)。夜間ロードが同じ日に 2 回付け替えた [d, d] と [d, null] が
--        既にあっても、今の行を「昨日で閉じる」は通す (#1563 仮レビュー M1)。今日の行を入れ直すときに前からの重なりに当たったら 23P01 = 画面・昇格は 409 cost_overlap (500 にしない)
--      🚨 前からある重なりの行は ⑥ の前に掃除する (下の M7 の go/no-go の項目に入れる)
--      🚨 既知の未達 (PR #1563 R1 M7・R2 M7): 表全体を半開区間 [from, to) にそろえて排他制約 (exclusion) を付けるのは ⑥ 切替の go/no-go の項目 (このPRではしない)。そろえる読み手・書き手 =
--         夜間ロード apps/company-db/load/engine.mjs (§5 の付け替え = greatest(valid_from, 今日 − 1) で閉じる = 同じ日の 2 回で 1 日重なる)・照合 master-compare/compare-load.mjs / compare-ne.mjs・
--         apps/master-decisions/lz-cdb.mjs・apps/company-db/router.mjs・push/sku-cost-observed.mjs・0007 / 0009 の mart (valid_to is null)・0046 v_sku_cost_observed_effective・
--         0049 mart.amazon_profit_* (その日を覆う 1 行)。いまは全部が両端を含む前提で読み・書きしている
--   6. 0026 の変更の記録の関数 (core.audit_master_change・core.bump_parent_version) を security definer にする (PR #1563 R2 M4)。
--      画面のロールに events.master_change_events の insert を渡さない = 記録を偽れない。db_user は「SET ROLE の役 か ログインした役」(持ち主の権限で動いても呼び手を残す)
--   7. マスタの書き込みの鍵 core.master_write_lock_key() = 4705310050 (0036 の親子の鍵 4705310036 と同じ作り。#1563 仮レビュー M3):
--      夜間ロード (apps/company-db/load/engine.mjs) は取引の冒頭 (親子の鍵より前) に排他で取る。この画面の保存・構成の依頼の昇格は段階の共有の鍵の後に共有で取る
--      (短く待つ = lib/master-write.mjs の MASTER_WRITE_WAIT。待ちきれなければ 409「夜間の取り込み中」)。夜間ロードの長い取引と保存が行の鍵で待ち合う (デッドロック) のを防ぐ
--   8. 画面のロール master_edit の書き込みの約束 (#1563 R3 M2。列の権限だけでは、段階・持ち主・記録の誰が を飛ばして直接書けた):
--      ・ops.begin_master_write(request_id, actor_id, reason, 持ち主表, 操作, SKU, 編集の印, 保存の中身のハッシュ, 版) = security definer。
--        段階が new_open・持ち主表のハッシュが段階の記録と同じ・操作 (今は sku_edit) が分かるときだけ、書いてよい行 (直す SKU + 含むセット・その商品) を DB が決めて鍵を取り、
--        版 (ops.master_edit_versions = 編集の印が見ている行の version) が呼び手の言う版と同じか確かめ (#1563 R4 M2 = 編集の印を DB でも確かめる)、
--        今の取引の番号 (txid_current()) の約束の行を ops.master_write_sessions に書く (画面のロールはこの表を書けない)
--      ・画面のロールが書ける表 (core.skus・products・supplier_skus・sku_costs・ops.sku_component_requests・sku_component_breaches・master_edit_requests の done) の
--        BEFORE の trigger: 呼び手が master_edit なら、約束が無い・段階が new_open でない・約束の操作で書けない・約束の相手でない行・触った列の持ち主が company でない = 42501。
--        保存の記録の done は約束と同じ request_id・人・SKU・操作・保存の中身のハッシュだけ
--      🚨 DB は人を確かめられない: actor_id はアプリ (ログイン・名簿 MASTER_EDITORS) が言う値。DB が守るのは「約束どおりの相手・操作・版・持ち主・段階」と db_user の記録まで
--      ・変更の記録 (core.audit_master_change) は、呼び手が master_edit なら誰が・request_id・理由をその行から取る (core.actor_* の設定は使わない = 偽れない)
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
  session_role       text not null,                                   -- 書いたログインのロール (session_user)。場所ごとに決まる (#1563 仮レビュー Low 3)
  stopped            boolean not null default false,                  -- このプロセスは止まった (正しく終わった・人が CLI で止めたと書いた)。段階を進める門はこのプロセスを外す
  stopped_reason     text,
  acked_at           timestamptz not null default clock_timestamp(),
  constraint ck_mlga_inflight check ((inflight_count = 0) = (oldest_inflight_at is null)),
  constraint ck_mlga_session check (session_role = 'master_gate_' || host),
  constraint ck_mlga_stopped check (stopped = (stopped_reason is not null) and (stopped_reason is null or length(stopped_reason) between 1 and 200)),
  constraint ck_mlga_stopped_drained check (not stopped or (inflight_count = 0 and oldest_inflight_at is null))   -- 止まった = 書きかけ 0 (#1563 R4 High 1)
);
create index ix_master_legacy_gate_acks_host on ops.master_legacy_gate_acks (host, instance_id, acked_at desc, ack_id desc);
select core.make_append_only('ops', 'master_legacy_gate_acks');
comment on table ops.master_legacy_gate_acks is 'プロセスごとの門の記録 (0050。書くのは ⑤-3 = ops.record_legacy_gate_ack だけ・ログイン master_gate_<場所>)。切替の段階を進める門が読む';

-- 門の設定 (⑤-3・⑥ で変えるときは create or replace)
create function ops.master_cutover_required_hosts() returns text[] language sql immutable as $$ select array['minipc', 'render']::text[] $$;
create function ops.master_cutover_ack_fresh_minutes() returns integer language sql immutable as $$ select 15 $$;

-- 段階を進める前にそろっているべきもの (前提の差し込み口・#1563 R3)。後の migration は表に 1 行足す (関数を create or replace しない = 前の項目を消さない):
--   insert into ops.master_cutover_prereq_checks (name, fn) values ('0051_backfill', 'ops.my_check(text, text)');
--   関数 = ops の中・この表と同じ持ち主・(p_from text, p_to text) returns text[] (問題の文の配列・無ければ空)。それ以外は足せない (trigger)。書けるのは表の持ち主だけ
create table ops.master_cutover_prereq_checks (
  name     text primary key check (name ~ '^[a-z0-9_.:-]{1,80}$'),
  fn       regprocedure not null,
  added_at timestamptz not null default now()
);
comment on table ops.master_cutover_prereq_checks is '切替の段階を進める前提の関数の一覧 (0050・#1563 R3)。ops.master_cutover_prereq_problems が全部呼ぶ。後の migration は 1 行足す (追記だけ・関数の中身は create or replace)';
select core.make_append_only('ops', 'master_cutover_prereq_checks');   -- 足すだけ (変える・消す・truncate は拒む。前の項目を消さない・#1563 R4 Low 3)

-- 関数が前提の関数として使ってよい形か (ops の中・表の持ち主の関数・(text, text) returns text[]・ふつうの関数・自分自身でない)。だめなら理由、よければ null
create function ops.master_cutover_prereq_fn_problem(p_fn regprocedure) returns text
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select case
    when p.oid is null then '関数が無い'
    when n.nspname <> 'ops' then format('関数が ops の中に無い (%s)', n.nspname)
    when p.proowner <> (select c.relowner from pg_catalog.pg_class c where c.oid = 'ops.master_cutover_prereq_checks'::regclass) then '関数の持ち主が表の持ち主でない'
    when p.prokind <> 'f' or p.pronargs <> 2 or p.proargtypes::text <> '25 25' or p.prorettype <> 'text[]'::regtype then '関数の形が (p_from text, p_to text) returns text[] でない'
    when p.proname in ('master_cutover_prereq_problems', 'set_master_cutover_phase') then '集める関数・段階の関数そのものは足せない'
  end
  from (select p_fn::oid as oid) x
  left join pg_catalog.pg_proc p on p.oid = x.oid
  left join pg_catalog.pg_namespace n on n.oid = p.pronamespace
$$;

create function ops.guard_master_cutover_prereq_checks() returns trigger language plpgsql set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_bad text;
begin
  if tg_op = 'DELETE' then return old; end if;
  v_bad := ops.master_cutover_prereq_fn_problem(new.fn);
  if v_bad is not null then raise exception 'prereq_check_invalid: % (%)', v_bad, new.fn using errcode = '42501'; end if;
  return new;
end $$;
create trigger trg_master_cutover_prereq_checks_guard before insert or update on ops.master_cutover_prereq_checks
  for each row execute function ops.guard_master_cutover_prereq_checks();

-- 足された前提の関数を名前の順に全部呼び、問題を「名前: 問題」でつなげて返す (空 = 進めてよい)。呼ぶ直前にも形を確かめる (後で持ち主・場所が変わった = 問題として返す = 進めない)
create function ops.master_cutover_prereq_problems(p_from text, p_to text) returns text[]
  language plpgsql stable security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  r      record;
  v_out  text[] := '{}';
  v_bad  text;
  v_name text;
  v_res  text[];
begin
  for r in select c.name, c.fn from ops.master_cutover_prereq_checks c order by c.name loop
    v_bad := ops.master_cutover_prereq_fn_problem(r.fn);
    if v_bad is not null then v_out := array_append(v_out, format('%s: 前提の関数が使えない (%s)', r.name, v_bad)); continue; end if;
    select format('%I.%I', n.nspname, p.proname) into v_name from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid = p.pronamespace where p.oid = r.fn::oid;
    execute format('select %s($1::text, $2::text)', v_name) into v_res using p_from, p_to;
    v_out := v_out || coalesce((select array_agg(format('%s: %s', r.name, x) order by o) from unnest(v_res) with ordinality as u(x, o) where x is not null), '{}');
  end loop;
  return v_out;
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
-- 証拠の時刻が読めて、p_after より後・p_upto 以前か (読めなければ false。順に確かめる = 読めない文字を時刻にしない)
create function ops.cutover_ts_between(p text, p_after timestamptz, p_upto timestamptz) returns boolean language plpgsql stable set search_path = pg_catalog, ops, pg_temp as $$
declare
  v timestamptz;
begin
  if not ops.cutover_is_ts(p) then return false; end if;
  v := p::timestamptz;
  return v > p_after and v <= p_upto;
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
-- 場所はログインで決まる: session_user = 'master_gate_' || p_host でなければ拒む (gate_host_mismatch・42501。Render のログインで minipc を名乗れない)
-- p_stopped = true = このプロセスは止まった (p_stopped_reason が要る)。段階を進める門はこのプロセスを外す (黙っているプロセスとして止めない)
-- 🚨 security definer: 呼ぶロール (master_gate_<場所>) に表の書き込みの権限を渡さない。一時の表を使わない・search_path の最後に pg_temp
create function ops.record_legacy_gate_ack(p_host text, p_instance_id text, p_build_id text, p_manifest jsonb, p_owner_hash text, p_phase_seen text,
                                           p_inflight_count integer, p_oldest_inflight_at timestamptz,
                                           p_stopped boolean default false, p_stopped_reason text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_hash    text := ops.legacy_manifest_hash(p_manifest);
  v_phase   text;
  v_id      bigint;
  v_at      timestamptz;
  v_stopped boolean := coalesce(p_stopped, false);
begin
  if p_host is null or not (p_host = any(ops.master_cutover_required_hosts())) then
    raise exception 'invalid_input: 知らない場所 % (render / minipc)', p_host using errcode = '22023';
  end if;
  if session_user::text is distinct from 'master_gate_' || p_host then
    raise exception 'gate_host_mismatch: ログイン % では場所 % の記録を書けない (master_gate_% でログインする)', session_user, p_host, p_host using errcode = '42501';
  end if;
  if v_stopped and (coalesce(p_inflight_count, 0) <> 0 or p_oldest_inflight_at is not null) then
    raise exception 'invalid_input: 止まった記録 (stopped) は書きかけ 0 のときだけ (書きかけ %)。書きかけを流し終えてから止める', p_inflight_count using errcode = '22023';
  end if;
  if v_stopped and (p_stopped_reason is null or length(btrim(p_stopped_reason)) = 0) then
    raise exception 'invalid_input: 止まった記録 (stopped) には理由 (stopped_reason) が要る' using errcode = '22023';
  end if;
  if not v_stopped and p_stopped_reason is not null then
    raise exception 'invalid_input: 止まった記録でないのに理由 (stopped_reason) がある' using errcode = '22023';
  end if;
  select phase into v_phase from ops.master_cutover_state where id = 1;
  if p_phase_seen is distinct from v_phase then
    raise exception 'stale_phase: 見た段階 % が今の段階 % と違う (段階を読み直してから書く)', p_phase_seen, v_phase using errcode = 'P0001';
  end if;
  insert into ops.master_legacy_manifests (manifest_hash, entries) values (v_hash, p_manifest) on conflict (manifest_hash) do nothing;
  insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at, session_role, stopped, stopped_reason)
    values (p_host, p_instance_id, p_build_id, v_hash, p_owner_hash, p_phase_seen, p_inflight_count, p_oldest_inflight_at, session_user::text, v_stopped,
            case when v_stopped then btrim(p_stopped_reason) end)
    returning ack_id, acked_at into v_id, v_at;
  return jsonb_build_object('ack_id', v_id, 'manifest_hash', v_hash, 'acked_at', v_at, 'stopped', v_stopped);
end $$;
revoke all on function ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz, boolean, text) from public;

-- 1 段だけ進める (飛ばさない・戻さない・証拠と全部の場所の新しい記録が要る・黙っているプロセスがあれば進めない)。鍵 (排他) → 行 (for update) → 差し込み口 → 証拠 → 門 → 状態 → 記録
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
    -- 証拠の時刻は今の段階に入った後・サーバーの今以前 (#1563 R3 High 1。前の切替の試み・先の日付の書き込みを使い回さない)
    if not ops.cutover_ts_between(e ->> 'checked_at', v_since, v_now) then
      raise exception 'evidence_invalid: drain.checked_at (%) は今の段階に入った後 (%)・今 (%) 以前', e ->> 'checked_at', v_since, v_now using errcode = '22023';
    end if;
    -- 手の入口を止めた一覧 = manifest の手の入口 (kind = manual) と完全に同じ集合
    e := p_evidence -> 'manual_entries_stopped';
    if e is null or jsonb_typeof(e) <> 'array' then raise exception 'evidence_invalid: manual_entries_stopped = [{ id, by, at }, ...] が要る' using errcode = '22023'; end if;
    if exists (select 1 from jsonb_array_elements(e) x
                where jsonb_typeof(x) <> 'object' or coalesce(length(x ->> 'id'), 0) = 0 or coalesce(length(x ->> 'by'), 0) = 0 or not ops.cutover_is_ts(x ->> 'at')) then
      raise exception 'evidence_invalid: manual_entries_stopped の各行に id・by・at が要る' using errcode = '22023';
    end if;
    if exists (select 1 from jsonb_array_elements(e) x where not ops.cutover_ts_between(x ->> 'at', v_since, v_now)) then
      raise exception 'evidence_invalid: manual_entries_stopped の at は今の段階に入った後 (%)・今 (%) 以前', v_since, v_now using errcode = '22023';
    end if;
    select coalesce(array_agg(x ->> 'id' order by x ->> 'id'), '{}'), count(*) into v_got, v_n from jsonb_array_elements(e) x;
    if v_n <> (select count(distinct x ->> 'id') from jsonb_array_elements(e) x) then raise exception 'evidence_invalid: manual_entries_stopped の id が重なっている' using errcode = '22023'; end if;
    select coalesce(array_agg(x ->> 'id' order by x ->> 'id'), '{}') into v_want from jsonb_array_elements(v_entries -> 'entries') x where x ->> 'kind' = 'manual';
    if v_got is distinct from v_want then
      raise exception 'evidence_invalid: 止めた手の入口 (%) が古い入口の一覧の手の入口 (%) と同じでない', array_to_string(v_got, ', '), array_to_string(v_want, ', ') using errcode = '22023';
    end if;
  end if;

  -- 門: 場所ごと・今までに記録のある全部のプロセスの最後の記録 (年齢で外さない = 25 時間前に黙ったプロセスも止まった記録が要る。#1563 R3 High 1)。
  --   止まった記録 = 外す / 新しい記録 (fresh 分以内) = 下の検査 / それより前 = 黙っている = 拒む (#1563 仮レビュー Low 3)
  v_problems := '{}';
  foreach v_host in array v_hosts loop
    v_n := 0;
    for a in select distinct on (k.instance_id) k.* from ops.master_legacy_gate_acks k
               where k.host = v_host
               order by k.instance_id, k.acked_at desc, k.ack_id desc loop
      -- 書きかけは止まった・黙っているプロセスを外す前に見る (止まった記録に書きかけがあっても通さない・#1563 R4 High 1)
      if a.inflight_count <> 0 or a.oldest_inflight_at is not null then
        v_problems := array_append(v_problems, format('%s/%s: 書きかけが %s 件ある', v_host, a.instance_id, a.inflight_count));
      end if;
      if a.stopped then
        v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', a.ack_id, 'host', a.host, 'instance_id', a.instance_id, 'build_id', a.build_id, 'acked_at', a.acked_at, 'stopped', true));
        continue;
      end if;
      if a.acked_at < v_now - v_fresh then
        v_problems := array_append(v_problems, format('%s/%s: 黙っている (最後の記録 %s が %s 分より前。止めたプロセスなら stopped の記録を書く)',
                                                      v_host, a.instance_id, a.acked_at, ops.master_cutover_ack_fresh_minutes()));
        continue;
      end if;
      v_n := v_n + 1;
      if not ((v_builds -> v_host) ? a.build_id) then v_problems := array_append(v_problems, format('%s/%s: 予定に無い build %s が動いている', v_host, a.instance_id, a.build_id)); end if;
      if a.manifest_hash is distinct from v_manifest then v_problems := array_append(v_problems, format('%s/%s: 古い入口の一覧が違う', v_host, a.instance_id)); end if;
      if a.owner_hash is distinct from v_owner then v_problems := array_append(v_problems, format('%s/%s: 持ち主表のハッシュが違う', v_host, a.instance_id)); end if;
      if a.phase_seen is distinct from v_from then v_problems := array_append(v_problems, format('%s/%s: 見た段階が %s (今は %s)', v_host, a.instance_id, a.phase_seen, v_from)); end if;
      if p_to <> 'frozen' and a.acked_at <= v_since then v_problems := array_append(v_problems, format('%s/%s: 記録が今の段階に入る前', v_host, a.instance_id)); end if;
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

-- 数が厳密な整数か (jsonb の number・小数点や指数の形でない・lo〜hi)。0.6・1.0・-1 = false (#1563 R3 M3)
create function ops.ne_obs_int_ok(p jsonb, lo integer, hi integer) returns boolean language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
begin
  if p is null or jsonb_typeof(p) <> 'number' or (p #>> '{}') !~ '^[0-9]{1,9}$' then return false; end if;
  return (p #>> '{}')::integer between lo and hi;
end $$;

-- 1 つのセットの行の形 (どの回でも): 配列・100 行まで・各行 = { code (空でない), qty (1〜99,999 の整数), sort (整数) }・sort はセットの中で 1〜N (送り手が NE の並びに 1 から振る)。
-- だめなら理由、よければ null。順に確かめる (整数と分かってから数として比べる)
create function ops.ne_set_rows_problem(p_rows jsonb) returns text language plpgsql immutable set search_path = pg_catalog, ops, pg_temp as $$
declare
  v_n integer;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then return '行が配列でない'; end if;
  v_n := jsonb_array_length(p_rows);
  if v_n > 100 then return '行が 100 より多い'; end if;
  if exists (select 1 from jsonb_array_elements(p_rows) r
              where jsonb_typeof(r) <> 'object' or coalesce(length(r ->> 'code'), 0) = 0
                 or not ops.ne_obs_int_ok(r -> 'qty', 1, 99999) or not ops.ne_obs_int_ok(r -> 'sort', 1, 100)) then
    return '行の形が違う (code・qty = 1〜99,999 の整数・sort = 1 以上の整数)';
  end if;
  if (select count(distinct (r ->> 'sort')::integer) <> v_n or max((r ->> 'sort')::integer) <> v_n from jsonb_array_elements(p_rows) r) then
    return '並び (sort) が 1〜行の数になっていない';
  end if;
  return null;
end $$;

-- NE のセットの構成の観測を 1 回分書く (観測のロール master_observer だけ・⑤-2 で夜間ロードから呼ぶ)。
-- p = { run_id, observed_at, complete, requested, fetched, raw_hash, source_generation, sets: [{ set_code, rows: [{ code, qty, sort }] }] }
-- 完全な回 (complete = true) は、知らないセット・セットでない SKU・重なるセット・行の形の誤り・重なる構成品・知らない構成品を 1 つも許さない (拒む)。
--   完全でない回は、残せないセットを飛ばして数える (上げる根拠には使わない)。同じ run_id の再送 = 中身が同じなら何もしない・違えば拒む
--   requested / fetched = 0〜1,000,000 の整数 (完全な回は要る・完全でない回もあれば同じ形)。qty・sort は ops.ne_set_rows_problem (#1563 R3 M3)
-- 鍵: 回の鍵 → 残すセットの SKU ごとの鍵 (sku_id の順・lib/master-write.mjs の SKU_LOCK_SQL と同じ数) → 書く (#1563 R3 M4。昇格は段階 → マスタの書き込み → SKU → CSV の順で、
--   ここは SKU の鍵だけを取る = 逆の順にならない。古い観測の昇格が終わるまで新しい観測は書けない / 新しい観測を書いた後の昇格はそれを見る)
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
  v_ok       jsonb := '[]'::jsonb;   -- 残すセット [{ set_id, rows }]
  s          jsonb;
  v_set      bigint;
  v_rows     jsonb;
  v_bad      text;
  x          bigint;
begin
  if v_run is null or v_run !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if not ops.cutover_is_ts(p ->> 'observed_at') then raise exception 'invalid_input: observed_at が読めない' using errcode = '22023'; end if;
  v_at := (p ->> 'observed_at')::timestamptz;
  if v_at > clock_timestamp() + interval '5 minutes' then raise exception 'invalid_input: observed_at が未来' using errcode = '22023'; end if;
  if v_at < clock_timestamp() - interval '36 hours' then raise exception 'invalid_input: observed_at が古すぎる (36 時間より前)' using errcode = '22023'; end if;
  if jsonb_typeof(p -> 'complete') is distinct from 'boolean' then raise exception 'invalid_input: complete (true / false) が要る' using errcode = '22023'; end if;
  v_complete := (p ->> 'complete')::boolean;
  if jsonb_typeof(p -> 'sets') is distinct from 'array' then raise exception 'invalid_input: sets が配列でない' using errcode = '22023'; end if;
  if (p -> 'requested' is not null and jsonb_typeof(p -> 'requested') <> 'null' and not ops.ne_obs_int_ok(p -> 'requested', 0, 1000000))
     or (p -> 'fetched' is not null and jsonb_typeof(p -> 'fetched') <> 'null' and not ops.ne_obs_int_ok(p -> 'fetched', 0, 1000000)) then
    raise exception 'invalid_input: requested・fetched は 0〜1,000,000 の整数' using errcode = '22023';
  end if;
  if v_complete then
    if not ops.ne_obs_int_ok(p -> 'requested', 0, 1000000) or not ops.ne_obs_int_ok(p -> 'fetched', 0, 1000000)
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
  -- 先に回の行 (数は後で入れられない = 追記だけ) を作るため、セットを先に確かめて数える (残すセットは v_ok に)
  for s in select * from jsonb_array_elements(p -> 'sets') loop
    v_bad := null;
    select k.sku_id into v_set from core.skus k where k.code_norm = core.norm_code(s ->> 'set_code') and k.sku_kind = 'set';
    if not found then v_bad := format('知らないセット・セットでない %s', s ->> 'set_code');
    elsif v_set = any(v_seen) then v_bad := format('同じセットが 2 回 %s', s ->> 'set_code');
    else
      v_bad := ops.ne_set_rows_problem(s -> 'rows');
      if v_bad is not null then v_bad := format('%s %s', v_bad, s ->> 'set_code');
      elsif v_complete and ((select count(*) <> count(distinct core.norm_code(r ->> 'code')) from jsonb_array_elements(s -> 'rows') r)
                            or exists (select 1 from jsonb_array_elements(s -> 'rows') r where not exists (select 1 from core.skus k where k.code_norm = core.norm_code(r ->> 'code')))) then
        v_bad := format('構成品・並びが重なる / 知らない構成品 %s', s ->> 'set_code');
      end if;
    end if;
    if v_bad is not null then
      if v_complete then raise exception 'invalid_input: 完全な回に残せないセットがある: %', v_bad using errcode = '22023'; end if;
      v_skip := v_skip + 1;
      continue;
    end if;
    v_seen := array_append(v_seen, v_set);
    v_ok := v_ok || jsonb_build_array(jsonb_build_object('set_id', v_set, 'rows', s -> 'rows'));
  end loop;
  -- 残すセットの SKU ごとの鍵 (sku_id の小さい順。昇格と同じ鍵 = 古い観測の昇格と並ぶ・#1563 R3 M4)
  for x in select u.id from unnest(v_seen) as u(id) order by u.id loop
    perform pg_advisory_xact_lock(hashtextextended('core.sku:' || x::text, 0));
  end loop;
  insert into ops.ne_set_observation_runs (run_id, observed_at, complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation, content_hash)
    values (v_run, v_at, v_complete, (p ->> 'requested')::integer, (p ->> 'fetched')::integer, coalesce(array_length(v_seen, 1), 0), v_skip,
            nullif(p ->> 'raw_hash', ''), nullif(p ->> 'source_generation', ''), v_hash);
  for s in select * from jsonb_array_elements(v_ok) loop
    select coalesce(jsonb_agg(jsonb_build_object('sku_id', k.sku_id, 'code', r ->> 'code', 'qty', (r ->> 'qty')::integer, 'sort', (r ->> 'sort')::integer)
                              order by (r ->> 'sort')::integer, r ->> 'code'), '[]'::jsonb)
      into v_rows
      from jsonb_array_elements(s -> 'rows') r
      left join core.skus k on k.code_norm = core.norm_code(r ->> 'code');
    insert into ops.ne_set_observations (run_id, set_sku_id, rows) values (v_run, (s ->> 'set_id')::bigint, v_rows);
    v_saved := v_saved + 1;
  end loop;
  return jsonb_build_object('state', 'written', 'run_id', v_run, 'sets', v_saved, 'skipped', v_skip);
end $$;
revoke all on function ops.record_ne_set_observations(jsonb) from public;

-- 5. 原価の守り (両端を含む [valid_from, valid_to]。valid_to = null = ずっと)。上の 🚨 M7 = ⑥ の前提
--   ・重なり: この画面と昇格の書き込み (source_system) と、表の持ち主でないロール (source_system を偽っても) の書き込みだけ見る
--   ・期間を縮めるだけの UPDATE (同じ SKU・新しい [from, to] が前の [from, to] の中) は見ない = 新しい重なりは作れない。
--     夜間ロードが同じ日に 2 回付け替えた [d, d] と [d, null] (前からの重なり) があっても、今の行を昨日で閉じるのは通す (#1563 仮レビュー M1)
--   ・持ち主でないロールは、今日 (東京) より前に始まった行を消さない・閉じた行 (valid_to がある) を変えない・昨日より前で閉じない (過去の粗利を変えない)
create function core.guard_sku_cost_overlap() returns trigger language plpgsql as $$
declare
  v_owner boolean := pg_catalog.pg_has_role(current_user, (select c.relowner from pg_catalog.pg_class c where c.oid = tg_relid), 'USAGE');
begin
  if v_owner and coalesce(pg_catalog.current_setting('core.source_system', true), '') not in ('portal_master_edit', 'ne_observation') then return null; end if;
  if tg_op = 'UPDATE' and new.sku_id = old.sku_id and new.valid_from >= old.valid_from
     and coalesce(new.valid_to, 'infinity'::date) <= coalesce(old.valid_to, 'infinity'::date) then
    return null;
  end if;
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
  -- 画面のロールの書き込みは、誰が・request_id・理由を ops.begin_master_write の行から取る (core.actor_* の設定は使わない = 偽れない。#1563 R3 M2)
  if v_db_user = 'master_edit' then
    select s.request_id::text, s.actor_id, s.reason, s.source_system into v_req, v_actor_id, v_reason, v_source
      from ops.master_write_sessions s where s.txid = pg_catalog.txid_current();
    if not found then raise exception 'master_write_session_required: 画面のロールの書き込みは ops.begin_master_write の後だけ' using errcode = '42501'; end if;
    v_actor_t := 'human';
    v_run := null;
  end if;
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

-- 7. マスタの書き込みの鍵 (上の 7.)。夜間ロードは排他・この画面の保存と構成の依頼の昇格は共有。数は 0036 の core.parent_lock_key() と重ならない固定の数
create function core.master_write_lock_key() returns bigint language sql immutable as $$ select 4705310050::bigint $$;

-- 8. 画面のロール master_edit の書き込みの約束 (上の 8.・#1563 R3 M2・R4 M2)
--    1 回の保存 = 1 つの「書き込みの約束」(ops.master_write_sessions の 1 行・取引ごと)。約束は 直す SKU・操作・画面が読んだ編集の印・保存の中身のハッシュ・
--    DB で確かめた版 に結びつく。画面のロールはその約束の SKU (と DB が決めた関わる行) にだけ・その操作の書き方でだけ書ける
--    🚨 DB は人を確かめられない: actor_id は画面 (アプリ) が言う値。DB が残すのは db_user (= master_edit) と、約束の中身。人の確かめはアプリのログイン (名簿 MASTER_EDITORS)
create table ops.master_write_sessions (
  txid               bigint primary key,   -- txid_current() = 書き込みを始めた取引
  request_id         uuid not null,
  operation          text not null check (operation in ('sku_edit')),   -- ⑤-2a などが操作を足すときは、この CHECK・begin・guard の操作の表を一緒に変える
  sku_id             bigint not null references core.skus (sku_id),   -- 直す SKU (保存の相手)
  target_sku_ids     bigint[] not null,    -- 書いてよい SKU = 直す SKU + (単品なら) それを含むセット (DB が決める)
  target_product_ids bigint[] not null,    -- 書いてよい商品 = 直す SKU の商品 (DB が決める)
  edit_token         text not null check (edit_token ~ '^[0-9a-f]{64}$'),     -- 画面が読んだ編集の印 (記録)
  payload_hash       text not null check (payload_hash ~ '^[0-9a-f]{64}$'),   -- 保存の中身のハッシュ = 保存の記録 (done) と同じでないと書けない
  versions           jsonb not null check (jsonb_typeof(versions) = 'object'),   -- DB で確かめた版 (ops.master_edit_versions)
  actor_id           text not null check (length(actor_id) between 1 and 320),  -- 🚨 アプリが言う人 (DB では確かめられない)
  reason             text check (reason is null or length(reason) <= 200),
  source_system      text not null check (source_system = 'portal_master_edit'),
  db_user            text not null,
  phase              text not null,
  owner_hash         text not null check (owner_hash ~ '^[0-9a-f]{64}$'),
  ownership          jsonb not null check (jsonb_typeof(ownership) = 'object'),
  created_at         timestamptz not null default clock_timestamp()
);
select core.make_append_only('ops', 'master_write_sessions');
comment on table ops.master_write_sessions is '画面のロールの書き込みの約束 (0050・#1563 R3 M2・R4 M2)。書くのは ops.begin_master_write だけ。guard と変更の記録がこの行を見る。actor_id はアプリが言う値';

-- 持ち主表のハッシュ (lib/master-cutover.mjs の ownershipHash と同じ = キーの順に [キー, 値] の配列を JSON にした sha256)
create function ops.ownership_hash(p jsonb) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select encode(sha256(convert_to('[' || coalesce(string_agg('[' || to_json(t.k)::text || ',' || to_json(t.v)::text || ']', ',' order by t.k collate "C"), '') || ']', 'UTF8')), 'hex')
    from jsonb_each_text(p) as t(k, v)
$$;

-- 1 つの SKU の版 (編集の印が見ている行の版の集まり)。lib/master-write.mjs の editVersionsOf(readCurrent の結果) と同じ形・同じ並び (文字の順):
--   sku = 'id:version' / product = 'id:version' か null / suppliers = ['supplier_id:supplier_skus.version:suppliers.version'] /
--   parent_sets (単品など) = ['set_id:version'] / components (セット) = ['child_id:version:product_version'] / request (セット) = 開いている依頼の番号 か null /
--   request_components = ['child_id:version:product_version']
--   原価・構成の行の変更は SKU の version を上げる (0026) = sku に入る。JAN は保存で書かないので入れない
create function ops.master_edit_versions(p_sku_id bigint) returns jsonb language sql stable set search_path = pg_catalog, pg_temp as $$
  select jsonb_build_object(
    'sku', s.sku_id::text || ':' || s.version::text,
    'product', (select p.product_id::text || ':' || p.version::text from core.products p where p.product_id = s.product_id),
    'suppliers', coalesce((select jsonb_agg(t.x order by t.x collate "C") from (
        select ss.supplier_id::text || ':' || ss.version::text || ':' || sp.version::text as x
          from core.supplier_skus ss join core.suppliers sp on sp.supplier_id = ss.supplier_id where ss.sku_id = s.sku_id) t), '[]'::jsonb),
    'parent_sets', case when s.sku_kind = 'set' then '[]'::jsonb else coalesce((select jsonb_agg(t.x order by t.x collate "C") from (
        select c.parent_sku_id::text || ':' || ps.version::text as x
          from core.sku_components c join core.skus ps on ps.sku_id = c.parent_sku_id where c.child_sku_id = s.sku_id) t), '[]'::jsonb) end,
    'components', case when s.sku_kind <> 'set' then '[]'::jsonb else coalesce((select jsonb_agg(t.x order by t.x collate "C") from (
        select c.child_sku_id::text || ':' || k.version::text || ':' || coalesce(kp.version::text, '') as x
          from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id left join core.products kp on kp.product_id = k.product_id
         where c.parent_sku_id = s.sku_id) t), '[]'::jsonb) end,
    'request', case when s.sku_kind = 'set' then (select r.component_request_id::text from ops.sku_component_requests r where r.set_sku_id = s.sku_id and r.status = 'open') end,
    'request_components', case when s.sku_kind <> 'set' then '[]'::jsonb else coalesce((select jsonb_agg(t.x order by t.x collate "C") from (
        select (e ->> 'sku_id') || ':' || coalesce(k.version::text, '') || ':' || coalesce(kp.version::text, '') as x
          from ops.sku_component_requests r cross join lateral jsonb_array_elements(r.rows) e
          left join core.skus k on k.sku_id = (e ->> 'sku_id')::bigint left join core.products kp on kp.product_id = k.product_id
         where r.set_sku_id = s.sku_id and r.status = 'open') t), '[]'::jsonb) end)
  from core.skus s where s.sku_id = p_sku_id
$$;

-- 画面の保存を始める (同じ取引で、画面のロールが書く前に 1 回)。保存の流れでは、行の鍵を取り編集の印を確かめた後に呼ぶ (lib/master-write.mjs)。
--   段階が new_open・持ち主表 (呼び手が動かしている表) のハッシュが段階の記録と同じ・操作が分かる・SKU がある、を確かめ、
--   書いてよい行 (直す SKU + 含むセット・その商品) を DB が決めて鍵を取り (商品 → SKU の順)、その上で版 (ops.master_edit_versions) が呼び手の言う版と同じか確かめる
--   (編集の印を DB でも確かめる = 画面が読んだ後に変わっていれば 409)。それから今の取引の約束の行を書く
-- 🚨 security definer (画面のロールに ops.master_write_sessions の書き込みを渡さない)。一時の表を使わない・search_path の最後に pg_temp
create function ops.begin_master_write(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb,
                                       p_operation text, p_sku_id bigint, p_edit_token text, p_payload_hash text, p_versions jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_db_user  text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_phase    text;
  v_owner    text;
  v_hash     text;
  v_tx       bigint := pg_catalog.txid_current();
  v_kind     text;
  v_product  bigint;
  v_skus     bigint[];
  v_products bigint[];
  v_now      jsonb;
begin
  perform pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'));      -- 段階を変える取引と並ぶ (画面は先に取っている = 同じ鍵)
  perform pg_advisory_xact_lock_shared(core.master_write_lock_key());         -- 夜間ロードと並ぶ (画面は先に取っている = 同じ鍵)
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if p_actor_id is null or length(btrim(p_actor_id)) = 0 or length(p_actor_id) > 320 or p_actor_id ~ '[[:cntrl:]]' then
    raise exception 'invalid_input: 保存する人 (actor_id) の形が違う' using errcode = '22023';
  end if;
  if p_reason is not null and length(p_reason) > 200 then raise exception 'invalid_input: 理由は 200 字まで' using errcode = '22023'; end if;
  if p_ownership is null or jsonb_typeof(p_ownership) <> 'object'
     or exists (select 1 from jsonb_each(p_ownership) e where jsonb_typeof(e.value) <> 'string' or (e.value #>> '{}') not in ('load', 'company')) then
    raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023';
  end if;
  select phase, owner_hash into v_phase, v_owner from ops.master_cutover_state where id = 1;
  v_hash := ops.ownership_hash(p_ownership);
  if v_phase is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が % (new_open でない)', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
  if v_owner is distinct from v_hash then raise exception 'before_cutover: 持ち主表が切替のときの記録と違う' using errcode = 'P0001'; end if;
  if p_operation is null or p_operation not in ('sku_edit') then raise exception 'invalid_input: 知らない操作 %', p_operation using errcode = '22023'; end if;
  if coalesce(p_edit_token, '') !~ '^[0-9a-f]{64}$' or coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: 編集の印・保存の中身のハッシュ (64 桁の 16 進) が要る' using errcode = '22023';
  end if;
  if p_versions is null or jsonb_typeof(p_versions) <> 'object' then raise exception 'invalid_input: 版 (versions) が要る' using errcode = '22023'; end if;
  if exists (select 1 from ops.master_write_sessions where txid = v_tx) then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select k.sku_kind, k.product_id into v_kind, v_product from core.skus k where k.sku_id = p_sku_id;
  if not found then raise exception 'invalid_input: SKU % が無い', p_sku_id using errcode = '22023'; end if;
  -- 書いてよい行 = 直す SKU + (単品など) それを含むセット・直す SKU の商品 (DB が決める。呼び手の言う相手は使わない)
  select array_agg(x order by x) into v_skus from (
    select p_sku_id as x
    union select c.parent_sku_id from core.sku_components c where c.child_sku_id = p_sku_id and v_kind <> 'set') t;
  v_products := case when v_product is null then '{}'::bigint[] else array[v_product] end;
  -- 行の鍵 (商品 → SKU の順 = 保存の流れと同じ。保存は先に取っている = 同じ鍵)。鍵の後に版を確かめる = 確かめた後に変わらない
  perform 1 from core.products where product_id = any(v_products) order by product_id for update;
  perform 1 from core.skus where sku_id = any(v_skus) order by sku_id for update;
  v_now := ops.master_edit_versions(p_sku_id);
  if p_versions is distinct from v_now then
    raise exception 'version_conflict: 画面を開いた後にこの商品 (または構成品・仕入先・含むセット・構成の依頼) が変わった (DB の版と違う)' using errcode = 'P0001';
  end if;
  insert into ops.master_write_sessions (txid, request_id, operation, sku_id, target_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_tx, p_request_id, p_operation, p_sku_id, v_skus, v_products, p_edit_token, p_payload_hash, v_now,
            p_actor_id, nullif(p_reason, ''), 'portal_master_edit', v_db_user, v_phase, v_hash, p_ownership);
  return jsonb_build_object('txid', v_tx, 'phase', v_phase, 'target_sku_ids', to_jsonb(v_skus), 'target_product_ids', to_jsonb(v_products));
end $$;
revoke all on function ops.begin_master_write(uuid, text, text, jsonb, text, bigint, text, text, jsonb) from public;

-- 画面のロールが触った列 → 持ち主表のキー (lib/master-write.mjs の SINGLE_FIELDS / SET_FIELDS と同じ。⑤-2a の新しい行の INSERT も = 値のある列)
create function ops.master_edit_owner_keys(p_table text, p_op text, p_old jsonb, p_new jsonb) returns text[]
  language sql immutable set search_path = pg_catalog, pg_temp as $$
  select coalesce(array_agg(distinct m.k order by m.k), '{}')
    from (values
      ('core.skus', 'name', 'skus.name'), ('core.skus', 'sku_kind', 'skus.sku_kind'), ('core.skus', 'tax_rate', 'skus.tax_rate'), ('core.skus', 'tax_class', 'skus.tax_class'),
      ('core.skus', 'handling', 'skus.handling'), ('core.skus', 'handling_own', 'skus.handling'), ('core.skus', 'standard_price_jpy', 'skus.standard_price'),
      ('core.skus', 'shipping_code', 'skus.shipping'), ('core.skus', 'shipping_method', 'skus.shipping'), ('core.skus', 'shipping_cost_jpy', 'skus.shipping'),
      ('core.skus', 'reorder_months', 'skus.reorder_months'), ('core.skus', 'set_sales_class_override', 'products.sales_class'),
      ('core.products', 'name', 'products.name'), ('core.products', 'status', 'products.status'), ('core.products', 'sales_class', 'products.sales_class'),
      ('core.products', 'parent_product_id', 'products.parent'), ('core.products', 'parent_set_by', 'products.parent'),
      ('core.supplier_skus', '*', 'supplier_skus.is_primary'), ('core.sku_costs', '*', 'sku_costs'),
      ('ops.sku_component_requests', '*', 'sku_components'), ('ops.sku_component_breaches', '*', 'sku_components')) as m(tbl, col, k)
   where m.tbl = p_table
     and (m.col = '*'
          or (p_op = 'UPDATE' and (p_old -> m.col) is distinct from (p_new -> m.col))
          or (p_op = 'INSERT' and coalesce(p_new -> m.col, 'null'::jsonb) <> 'null'::jsonb)
          or (p_op = 'DELETE' and coalesce(p_old -> m.col, 'null'::jsonb) <> 'null'::jsonb))
$$;

-- 操作ごとに、画面のロールが書いてよい (表・書き方)。sku_edit = 既にある SKU を直す (SKU・商品は直すだけ・原価は入れ替え・構成は依頼)
create function ops.master_write_allowed(p_operation text, p_table text, p_op text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from (values
      ('sku_edit', 'core.skus', 'UPDATE'), ('sku_edit', 'core.products', 'UPDATE'),
      ('sku_edit', 'core.supplier_skus', 'INSERT'), ('sku_edit', 'core.supplier_skus', 'UPDATE'),
      ('sku_edit', 'core.sku_costs', 'INSERT'), ('sku_edit', 'core.sku_costs', 'UPDATE'), ('sku_edit', 'core.sku_costs', 'DELETE'),
      ('sku_edit', 'ops.sku_component_requests', 'INSERT'), ('sku_edit', 'ops.sku_component_requests', 'UPDATE'),
      ('sku_edit', 'ops.sku_component_breaches', 'UPDATE')) as m(op, tbl, act)
    where m.op = p_operation and m.tbl = p_table and m.act = p_op)
$$;

-- 画面のロールが書く表の BEFORE の守り。呼び手 (SET ROLE の役 か ログインした役) が master_edit のときだけ見る (夜間ロード・昇格・ほかの書き手は今までどおり)
--   ・同じ取引に約束 (ops.begin_master_write の行) が無い = 42501 (保存の記録の failed だけは無くてよい = 切替前の 409 も残す)
--   ・保存の記録の done = 約束と同じ request_id・人・SKU・操作・保存の中身のハッシュだけ
--   ・段階が new_open でない・約束の操作で書けない (表・書き方)・約束の相手でない行 (SKU・商品・SKU の仕入先・原価・セットの依頼と食い違い)・
--     触った列の持ち主が company でない (始めたときの持ち主表で) = 42501
-- 🚨 security definer (画面のロールに ops.master_write_sessions を読ませない)。security definer の関数 (0026 の version の付け替え) の中の書き込みも呼び手は master_edit = 同じ約束で見る
create function ops.guard_master_edit_write() returns trigger
  language plpgsql security definer set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_old     jsonb;
  v_new     jsonb;
  v_sess    ops.master_write_sessions%rowtype;
  v_has     boolean;
  v_ok      boolean;
  k         text;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op in ('UPDATE', 'DELETE') then v_old := to_jsonb(old); end if;
  if tg_op in ('INSERT', 'UPDATE') then v_new := to_jsonb(new); end if;
  select * into v_sess from ops.master_write_sessions where txid = pg_catalog.txid_current();
  v_has := found;
  if v_tbl = 'ops.master_edit_requests' then
    if tg_op = 'INSERT' and (v_new ->> 'status') = 'failed' then return new; end if;
    if not v_has then
      raise exception 'master_write_session_required: 保存の記録 (done) は、同じ取引で ops.begin_master_write をした後だけ' using errcode = '42501';
    end if;
    if tg_op <> 'INSERT' or (v_new ->> 'request_id')::uuid is distinct from v_sess.request_id or (v_new ->> 'actor_id') is distinct from v_sess.actor_id
       or (v_new ->> 'sku_id')::bigint is distinct from v_sess.sku_id or (v_new ->> 'operation') is distinct from v_sess.operation
       or (v_new ->> 'payload_hash') is distinct from v_sess.payload_hash then
      raise exception 'master_write_session_mismatch: 保存の記録 (done) が約束 (request_id・人・SKU・操作・保存の中身のハッシュ) と違う' using errcode = '42501';
    end if;
    return new;
  end if;
  if not v_has then
    raise exception 'master_write_session_required: 画面のロールの書き込み (%) は、同じ取引で ops.begin_master_write を呼んだ後だけ', v_tbl using errcode = '42501';
  end if;
  if (select phase from ops.master_cutover_state where id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない (%)', v_tbl using errcode = '42501';
  end if;
  if not ops.master_write_allowed(v_sess.operation, v_tbl, tg_op) then
    raise exception 'master_write_operation: 約束の操作 % では % に % できない', v_sess.operation, v_tbl, tg_op using errcode = '42501';
  end if;
  -- 約束の相手の行か (変える前と後の両方)
  v_ok := case v_tbl
    when 'core.skus' then (v_old is null or (v_old ->> 'sku_id')::bigint = any(v_sess.target_sku_ids)) and (v_new is null or (v_new ->> 'sku_id')::bigint = any(v_sess.target_sku_ids))
    when 'core.sku_costs' then (v_old is null or (v_old ->> 'sku_id')::bigint = any(v_sess.target_sku_ids)) and (v_new is null or (v_new ->> 'sku_id')::bigint = any(v_sess.target_sku_ids))
    when 'core.products' then (v_old is null or (v_old ->> 'product_id')::bigint = any(v_sess.target_product_ids)) and (v_new is null or (v_new ->> 'product_id')::bigint = any(v_sess.target_product_ids))
    when 'core.supplier_skus' then (v_old is null or (v_old ->> 'sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'sku_id')::bigint = v_sess.sku_id)
    when 'ops.sku_component_requests' then (v_old is null or (v_old ->> 'set_sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'set_sku_id')::bigint = v_sess.sku_id)
    when 'ops.sku_component_breaches' then (v_old is null or (v_old ->> 'set_sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'set_sku_id')::bigint = v_sess.sku_id)
    else false end;
  if not v_ok then
    raise exception 'master_write_target: 約束の相手 (SKU %) の行でない (%)', v_sess.sku_id, v_tbl using errcode = '42501';
  end if;
  foreach k in array ops.master_edit_owner_keys(v_tbl, tg_op, v_old, v_new) loop
    if (v_sess.ownership ->> k) is distinct from 'company' then
      raise exception 'owner_not_company: % の持ち主が company でない (%)', k, v_tbl using errcode = '42501';
    end if;
  end loop;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_master_edit_write() from public;
create trigger trg_master_edit_guard before insert or update or delete on core.skus for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert or update or delete on core.products for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert or update or delete on core.supplier_skus for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert or update or delete on core.sku_costs for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert or update or delete on ops.sku_component_requests for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert or update or delete on ops.sku_component_breaches for each row execute function ops.guard_master_edit_write();
create trigger trg_master_edit_guard before insert on ops.master_edit_requests for each row execute function ops.guard_master_edit_write();

-- 読むだけの見張り (watcher)。書くロール (master_edit・master_ops・master_observer・master_gate) の権限は scripts/company-db/create-master-edit-roles.mjs が付ける
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_cutover_state, ops.master_cutover_events, ops.master_legacy_manifests, ops.master_legacy_gate_acks, ops.master_edit_requests,
      ops.sku_component_requests, ops.ne_set_observation_runs, ops.ne_set_observations, ops.sku_component_breaches, ops.master_write_sessions, ops.master_cutover_prereq_checks to watcher';
  end if;
end $$;
