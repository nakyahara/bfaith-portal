-- 0058 広げる道 PR-1 = new_open のまま持ち主に company のキーを足す DB の部品 (2026-10-06。設計 = 広げる道 v10 §3 + 最小の計画 newentry_min_plan.md = 今の正)
-- 🚨 番号は仮置き (マージの順で振り直す)。積み方: 0055 (持ち主の epoch) → 0057 → この 0058
--
-- なぜ: 段階 new_open のまま (13 キーは company)、持ち主に skus.sku_kind を足したい (中原さんの決定 10/6)。今の activate は frozen のときだけ
--   (apps/company-db/load/ownership-state.mjs)・段階の owner_hash を new_open のまま変える道が無い。
--   sku_kind は forward-only (戻す道を作らない) なので、広げた後は DB が区分の書き換え・削除と、区分に依る行の最終形を守る。
-- なにを (sku_kind の持ち主が load の間 = 今は、G18・G19 は何もしない = 夜間ロード・13 キーの保存・原価の予約・migration・復元は今までどおり):
--   1. 試み (G8): ops.master_widen_attempts (widen_prepare_id・base_commit_seq (bigint)・loader_fingerprint・manifest_hash・足すキー)・出来事 (追記だけ)・手の入口の停止 (DB の時刻)
--      ops.prepare_master_widen / ops.cancel_master_widen / ops.record_widen_manual_stop / ops.widen_master_ownership (DB の持ち主だけ)
--   2. 判定の本体 ops._widen_judge (何も書かない・どのロールにも EXECUTE を与えない) と watcher 用の読むだけの ops.widen_check_readonly (G26・R9 Low 1)
--   3. G5: 段階 company_owner / new_open の間、持ち主の epoch の行は widen の関数でだけ変わる (直接の UPDATE・今の prepare / activate / cancel は拒む)・足すだけ
--   4. G7: 門の記録の 2 版 (active / prepared の見たハッシュ・capable)。ops.record_legacy_gate_ack_v2 が DB の今の値と照らして書く (呼び手 = PR-2)
--   5. G6': ops.master_ownership_active_map() を SECURITY DEFINER に (master_edit に EXECUTE だけ・R1 H4)
--   6. G18: sku_kind の持ち主が company の間、全ロールで core.skus の区分の変更と DELETE (と TRUNCATE) を拒む。保守の印 (G24) だけ通す
--   7. G19: 同じ間、core.skus と core.sku_components の行の最終形を deferred の constraint trigger で守る (保守の印でも外さない)
--   8. G24: 保守の印 ops.begin_master_maintenance (UUID + 取引の番号 + セッションのユーザー・取引の中の GUC・0051 の画面の約束と同じ形)
--   9. G25: 復元の最後 (commit の前) の数え直し ops.assert_sku_kind_shape_after_restore() (apps/company-db/backup/dump.mjs が呼ぶ)
--  10. 照合 ② の新商品のゲートの結果 ops.new_entry_gate_results (watch_writer が ops.record_new_entry_gate で 1 行・kind_gate の 5 つ + DB が数えた最終形)
--  11. 開放の許可 (lease): 照合 ② の開始で ops.close_new_entry_for_compare (watch_writer) が閉じる → ops.grant_new_entry_lease(種類, 今回の回)
--      (ログイン new_entry_gate・一番新しい結果の行 = 今回の回・全部 0・今日・widen の後・停止の床より新しい → 翌日 10:00 まで)・
--      ops.revoke_new_entry_lease (停止の床)・ops.acquire_new_entry_locks (アプリの鍵の入口)。新商品を作る 3 つの関数の中で強制
--  12. NE で一度でも見たコードの履歴 (seed + 毎日の照合)・配る直前の重なりの確かめ (NE の登録の CSV は upsert = 社内の DB の重なりの確かめで止める)
--  13. 配ったファイルは ops.ne_reg_file だけで渡す (配ってから 2 時間 + 配った時の許可が有効)。file_bytes の列を読めるロールを無くす
--  14. 鍵の順 (§3.10): request → 許可 (共有) → 切替の段階 → マスタの書き込み → SKU → CSV → NE のコード。許可を出す / 取り消す / 結果の記録 = 許可の排他を最初に
-- 🚨 全部の関数: search_path = pg_catalog, pg_temp・完全修飾・REVOKE EXECUTE FROM PUBLIC・要るロールだけ grant。一時の表を使わない
-- 🚨 作らないもの (中原さんの決定 10/6 = 上書きの事故を自動で見つける仕組み一式をやめる): G23・照合の回の封・版の台帳・取得の件数の基準・事故の表・
--    アップロードキュー・NE の全件の書き出しの集約。G27 の開く前のゲートの段 (daily-sync) は PR-7

-- ─── 0. 小さい部品 ───
-- 広げてよいキー (正本 17 §13 の下書き: PR-8 (narrow + ロールの分離) の前に広げてよいのは skus.sku_kind だけ)
create function ops.master_widen_allowed_keys() returns text[] language sql immutable set search_path = pg_catalog, pg_temp as $$ select array['skus.sku_kind']::text[] $$;
revoke all on function ops.master_widen_allowed_keys() from public;

-- 呼んだ人 (session_user) が DB の持ち主のロール (持ち主の epoch の表の持ち主) か、そのメンバーか (owner の mode = cdb_owner に SET できるログイン)
create function ops.session_is_db_owner() returns boolean language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.pg_has_role(session_user, c.relowner, 'MEMBER')
    from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'ops' and c.relname = 'master_ownership_state'
$$;
revoke all on function ops.session_is_db_owner() from public;

-- この接続が今の取引で取引の鍵 (advisory・bigint) を持っているか (p_exclusive = 排他)。widen が「自分で鍵を取った」を確かめる (R9 Low 1)
create function ops.holds_xact_advisory(p_key bigint, p_exclusive boolean) returns boolean language sql stable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from pg_catalog.pg_locks l
                  where l.locktype = 'advisory' and l.pid = pg_catalog.pg_backend_pid() and l.granted and l.objsubid = 1
                    and l.classid = ((p_key >> 32) & 4294967295)::text::oid and l.objid = (p_key & 4294967295)::text::oid
                    and l.mode = case when p_exclusive then 'ExclusiveLock' else 'ShareLock' end)
$$;
revoke all on function ops.holds_xact_advisory(bigint, boolean) from public;

-- 文字が RFC 3339 の明示の offset つきの時刻か (R8 Low・R9 M2)。読めて有限なら timestamptz、だめなら null (例外にしない)
create function ops.rfc3339_ts(p text) returns timestamptz language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v timestamptz;
begin
  if p is null or p !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})$' then return null; end if;
  begin
    v := p::timestamptz;
  exception when others then
    return null;
  end;
  if not pg_catalog.isfinite(v) then return null; end if;
  return v;
end $$;
revoke all on function ops.rfc3339_ts(text) from public;

-- ─── 1. 持ち主の epoch の出来事に widen を足す (G4) ───
alter table ops.master_ownership_events drop constraint master_ownership_events_action_check;
alter table ops.master_ownership_events add constraint master_ownership_events_action_check check (action in ('init', 'prepare', 'cancel_prepare', 'activate', 'widen'));

-- ─── 2. G6': active の持ち主表を読む関数を DEFINER に (master_edit は表を読めない = EXECUTE だけ渡す・R1 H4) ───
create or replace function ops.master_ownership_active_map() returns jsonb
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce((select s.active_map from ops.master_ownership_state s where s.id = 1), '{}'::jsonb)
$$;
revoke all on function ops.master_ownership_active_map() from public;

-- sku_kind の持ち主が company か (active。行が無い = 全部 load)。G18・G19・G25 が使う
create function ops.sku_kind_locked() returns boolean language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce((select s.active_map ->> 'skus.sku_kind' from ops.master_ownership_state s where s.id = 1), 'load') = 'company'
$$;
revoke all on function ops.sku_kind_locked() from public;

-- ─── 3. G7: 門の記録の 2 版 ───
-- 2 版 = 書いたときに DB の active / prepared のハッシュを見た (関数が今の値と照らす) + このプロセスの build が company にできるキー (capable)
alter table ops.master_legacy_gate_acks
  add column ack_version        smallint not null default 1 check (ack_version in (1, 2)),
  add column active_hash_seen   text check (active_hash_seen is null or active_hash_seen ~ '^[0-9a-f]{64}$'),
  add column prepared_hash_seen text check (prepared_hash_seen is null or prepared_hash_seen ~ '^[0-9a-f]{64}$'),
  add column capable            text[],
  add constraint ck_mlga_version check ((ack_version = 1 and active_hash_seen is null and prepared_hash_seen is null and capable is null)
                                     or (ack_version = 2 and active_hash_seen is not null and capable is not null));

create function ops.record_legacy_gate_ack_v2(p_host text, p_instance_id text, p_build_id text, p_manifest jsonb, p_owner_hash text, p_phase_seen text,
                                              p_inflight_count integer, p_oldest_inflight_at timestamptz,
                                              p_active_hash_seen text, p_prepared_hash_seen text, p_capable text[],
                                              p_stopped boolean default false, p_stopped_reason text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_hash     text := ops.legacy_manifest_hash(p_manifest);
  v_phase    text;
  v_active   text;
  v_prepared text;
  v_id       bigint;
  v_at       timestamptz;
  v_stopped  boolean := coalesce(p_stopped, false);
begin
  if p_host is null or not (p_host = any(ops.master_cutover_required_hosts())) then
    raise exception 'invalid_input: 知らない場所 % (render / minipc)', p_host using errcode = '22023';
  end if;
  if session_user::text is distinct from 'master_gate_' || p_host then
    raise exception 'gate_host_mismatch: ログイン % では場所 % の記録を書けない (master_gate_% でログインする)', session_user, p_host, p_host using errcode = '42501';
  end if;
  if v_stopped and (coalesce(p_inflight_count, 0) <> 0 or p_oldest_inflight_at is not null) then
    raise exception 'invalid_input: 止まった記録 (stopped) は書きかけ 0 のときだけ (書きかけ %)', p_inflight_count using errcode = '22023';
  end if;
  if v_stopped and (p_stopped_reason is null or length(btrim(p_stopped_reason)) = 0) then
    raise exception 'invalid_input: 止まった記録 (stopped) には理由 (stopped_reason) が要る' using errcode = '22023';
  end if;
  if not v_stopped and p_stopped_reason is not null then
    raise exception 'invalid_input: 止まった記録でないのに理由 (stopped_reason) がある' using errcode = '22023';
  end if;
  if coalesce(p_active_hash_seen, '') !~ '^[0-9a-f]{64}$' or (p_prepared_hash_seen is not null and p_prepared_hash_seen !~ '^[0-9a-f]{64}$') then
    raise exception 'invalid_input: 見た active / prepared のハッシュ (64 桁の 16 進・prepared は無ければ null) が要る' using errcode = '22023';
  end if;
  if p_capable is null or cardinality(p_capable) > 200 or array_position(p_capable, null) is not null
     or exists (select 1 from unnest(p_capable) k where k !~ '^[a-z][a-z0-9_]{0,40}(\.[a-z][a-z0-9_]{0,40})?$')
     or (select count(*) <> count(distinct k) from unnest(p_capable) k) then
    raise exception 'invalid_input: capable (company にできるキーの配列・重ならない・200 まで) の形が違う' using errcode = '22023';
  end if;
  -- epoch の鍵 (共有) = prepare / widen (排他) と並ぶ = 見たハッシュと DB の今の値を同じ時点で照らす
  perform pg_catalog.pg_advisory_xact_lock_shared(ops.master_ownership_lock_key());
  select phase into v_phase from ops.master_cutover_state where id = 1;
  if p_phase_seen is distinct from v_phase then
    raise exception 'stale_phase: 見た段階 % が今の段階 % と違う (段階を読み直してから書く)', p_phase_seen, v_phase using errcode = 'P0001';
  end if;
  select s.active_hash, s.prepared_hash into v_active, v_prepared from ops.master_ownership_state s where s.id = 1;
  if not found then v_active := ops.ownership_hash('{}'::jsonb); v_prepared := null; end if;   -- 行が無い = 全部 load
  if p_active_hash_seen is distinct from v_active or p_prepared_hash_seen is distinct from v_prepared then
    raise exception 'stale_ownership: 見た持ち主 (active %・prepared %) が DB の今 (active %・prepared %) と違う (持ち主を読み直してから書く)',
      left(p_active_hash_seen, 12), coalesce(left(p_prepared_hash_seen, 12), 'なし'), left(v_active, 12), coalesce(left(v_prepared, 12), 'なし') using errcode = 'P0001';
  end if;
  insert into ops.master_legacy_manifests (manifest_hash, entries) values (v_hash, p_manifest) on conflict (manifest_hash) do nothing;
  insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at, session_role, stopped, stopped_reason,
                                           ack_version, active_hash_seen, prepared_hash_seen, capable)
    values (p_host, p_instance_id, p_build_id, v_hash, p_owner_hash, p_phase_seen, p_inflight_count, p_oldest_inflight_at, session_user::text, v_stopped,
            case when v_stopped then btrim(p_stopped_reason) end, 2, p_active_hash_seen, p_prepared_hash_seen,
            (select coalesce(array_agg(k order by k), '{}') from unnest(p_capable) k))
    returning ack_id, acked_at into v_id, v_at;
  return jsonb_build_object('ack_id', v_id, 'manifest_hash', v_hash, 'acked_at', v_at, 'stopped', v_stopped, 'ack_version', 2);
end $$;
revoke all on function ops.record_legacy_gate_ack_v2(text, text, text, jsonb, text, text, integer, timestamptz, text, text, text[], boolean, text) from public;

-- ─── 4. 試み (G8)・出来事・手の入口の停止 ───
create table ops.master_widen_attempts (
  widen_prepare_id   uuid primary key,
  company_id         smallint not null check (company_id = 1),
  added_keys         text[] not null check (cardinality(added_keys) >= 1),
  active_hash        text not null check (active_hash ~ '^[0-9a-f]{64}$'),     -- prepare のときの active (widen まで変わらないこと)
  prepared_hash      text not null check (prepared_hash ~ '^[0-9a-f]{64}$'),
  prepared_map       jsonb not null check (jsonb_typeof(prepared_map) = 'object'),
  prepared_at        timestamptz not null,                                     -- = ops.master_ownership_state.prepared_at (DB の時計・epoch の鍵の後)
  base_commit_seq    bigint not null check (base_commit_seq >= 0),             -- prepare のときの coalesce(max(commit_seq), 0) (epoch の排他の鍵の後・同じ取引)
  loader_fingerprint text not null check (loader_fingerprint ~ '^[0-9a-f]{64}$'), -- 夜間ロードの規則の指紋 (engine.mjs の LOAD_RULE_FINGERPRINT・同じ commit)
  manifest_hash      text not null references ops.master_legacy_manifests (manifest_hash),   -- 書き手の ack が見ているべき古い入口の一覧・止める手の入口の元
  prepared_by        text not null check (length(prepared_by) between 1 and 200),
  state              text not null default 'prepared' check (state in ('prepared', 'cancelled', 'widened')),
  closed_at          timestamptz,
  closed_by          text,
  constraint ck_mwa_closed check ((state = 'prepared') = (closed_at is null) and (state = 'prepared') = (closed_by is null))
);
create unique index ux_master_widen_attempts_open on ops.master_widen_attempts ((true)) where state = 'prepared';
comment on table ops.master_widen_attempts is '広げる道の試み (0058・G8)。1 回 = widen_prepare_id。base_commit_seq より後の commit がちょうど 2 つ (回収 → prepared) で widen。書くのは 0058 の関数だけ';

-- 書き換えは関数の中だけ (設定 ops.widen_protocol)・prepared → cancelled / widened の 1 回だけ・中身は変えない・消さない
create function ops.guard_master_widen_attempts() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op = 'DELETE' then raise exception 'master_widen_attempts は消さない' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.widen_protocol', true), '') is distinct from '1' then
    raise exception 'master_widen_attempts は 0058 の関数 (prepare / cancel / widen) でだけ書く' using errcode = '42501';
  end if;
  if tg_op = 'UPDATE' then
    if old.state <> 'prepared' or new.state not in ('cancelled', 'widened') then
      raise exception 'master_widen_attempts の状態は prepared → cancelled / widened の 1 回だけ (% → %)', old.state, new.state using errcode = 'P0001';
    end if;
    if (new.widen_prepare_id, new.company_id, new.added_keys, new.active_hash, new.prepared_hash, new.prepared_map, new.prepared_at, new.base_commit_seq,
        new.loader_fingerprint, new.manifest_hash, new.prepared_by)
       is distinct from (old.widen_prepare_id, old.company_id, old.added_keys, old.active_hash, old.prepared_hash, old.prepared_map, old.prepared_at, old.base_commit_seq,
        old.loader_fingerprint, old.manifest_hash, old.prepared_by) then
      raise exception 'master_widen_attempts の中身は書き換えない (閉じて新しい試みを作る)' using errcode = 'P0001';
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_widen_attempts() from public;
create trigger trg_master_widen_attempts_guard before insert or update or delete on ops.master_widen_attempts
  for each row execute function ops.guard_master_widen_attempts();
create trigger trg_master_widen_attempts_truncate before truncate on ops.master_widen_attempts for each statement execute function core.reject_mutation();

-- 試みの出来事 (追記だけ)。widen の判定の結果 (数・2 つの commit_seq・run_id・ack) はここに残る (60 日で消える ops.load_decisions に頼らない)
create table ops.master_widen_events (
  event_id         bigint generated always as identity primary key,
  widen_prepare_id uuid not null references ops.master_widen_attempts (widen_prepare_id),
  action           text not null check (action in ('prepare', 'manual_stop', 'cancel', 'widen')),
  actor            text not null check (length(actor) between 1 and 200),
  detail           jsonb not null check (jsonb_typeof(detail) = 'object'),
  recorded_at      timestamptz not null default clock_timestamp()
);
create index ix_master_widen_events_attempt on ops.master_widen_events (widen_prepare_id, event_id);
select core.make_append_only('ops', 'master_widen_events');

-- 手の入口 (NE の画面など・manifest の kind = manual) を止めた記録 (DB の時刻・試みごと・追記だけ)。止めるのは足すキーと owner_cols が重なる入口
create table ops.master_widen_manual_stops (
  widen_prepare_id uuid not null references ops.master_widen_attempts (widen_prepare_id),
  entry_id         text not null check (entry_id ~ '^[A-Za-z0-9_.:/-]{1,120}$'),
  stopped_by       text not null check (length(btrim(stopped_by)) between 1 and 200),
  note             text check (note is null or length(note) <= 500),
  stopped_at       timestamptz not null default clock_timestamp(),
  primary key (widen_prepare_id, entry_id)
);
select core.make_append_only('ops', 'master_widen_manual_stops');

-- 足すキーに関係する手の入口 (manifest の kind = manual で owner_cols が足すキーと重なる)。🚨 owner_cols が無い・配列でない手の入口 = 重なるとみる (fail-closed)
create function ops.widen_required_manual_entries(p_manifest_hash text, p_added text[]) returns text[]
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select coalesce(array_agg(distinct x ->> 'id' order by x ->> 'id'), '{}')
    from ops.master_legacy_manifests m cross join lateral jsonb_array_elements(m.entries -> 'entries') x
   where m.manifest_hash = p_manifest_hash and x ->> 'kind' = 'manual'
     and (jsonb_typeof(x -> 'owner_cols') is distinct from 'array'
          or exists (select 1 from jsonb_array_elements_text(x -> 'owner_cols') c where c = any(p_added)))
$$;
revoke all on function ops.widen_required_manual_entries(text, text[]) from public;

-- ─── 5. G5: 段階 company_owner / new_open の間、持ち主の epoch は 0058 の関数でだけ変わる・prepared は「active + 足すだけ」 ───
create function ops.guard_master_ownership_widen() returns trigger language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_phase text;
  v_row   ops.master_ownership_state;
begin
  select c.phase into v_phase from ops.master_cutover_state c where c.id = 1;
  if v_phase is null or v_phase not in ('company_owner', 'new_open') then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  if tg_op = 'DELETE' then
    if coalesce(pg_catalog.current_setting('ops.widen_protocol', true), '') is distinct from '1' then
      raise exception 'widen_protocol_required: 段階 % では持ち主の epoch の行を消さない', v_phase using errcode = '42501';
    end if;
    return old;
  end if;
  if coalesce(pg_catalog.current_setting('ops.widen_protocol', true), '') is distinct from '1'
     and (tg_op = 'INSERT' or (new.active_hash, new.active_map, new.prepared_hash, new.prepared_map, new.prepared_at)
                                is distinct from (old.active_hash, old.active_map, old.prepared_hash, old.prepared_map, old.prepared_at)) then
    raise exception 'widen_protocol_required: 段階 % では持ち主の epoch は widen の関数 (ops.prepare_master_widen / cancel_master_widen / widen_master_ownership) でだけ変える', v_phase using errcode = '42501';
  end if;
  v_row := new;
  if v_row.prepared_map is not null and exists (select 1 from jsonb_each_text(v_row.active_map) a where a.value = 'company' and (v_row.prepared_map ->> a.key) is distinct from 'company') then
    raise exception 'widen_not_additive: 段階 % では prepared は active に company のキーを足すだけ (company を load に戻さない)', v_phase using errcode = 'P0001';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_ownership_widen() from public;
create trigger trg_master_ownership_state_widen before insert or update or delete on ops.master_ownership_state
  for each row execute function ops.guard_master_ownership_widen();

-- ─── 6. G24: 保守の印 (G18 を通す唯一の道。G19 は通さない) ───
create table ops.master_maintenance_marks (
  mark_id      uuid primary key,
  txid         bigint not null,
  session_role text not null,
  reason       text not null check (length(btrim(reason)) between 1 and 500),
  created_at   timestamptz not null default clock_timestamp()
);
select core.make_append_only('ops', 'master_maintenance_marks');
comment on table ops.master_maintenance_marks is '保守の印 (0058・G24)。書くのは ops.begin_master_maintenance だけ (DB の持ち主)。どのロールにも select を渡さない';

-- 直接の INSERT を拒む (DB の持ち主でも。印は関数の中だけ = 関数が同じ取引で設定 ops.master_maintenance_insert に作る UUID を立てる)
create function ops.guard_master_maintenance_marks() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if new.txid is distinct from pg_catalog.txid_current() or new.session_role is distinct from session_user::text
     or coalesce(pg_catalog.current_setting('ops.master_maintenance_insert', true), '') is distinct from new.mark_id::text then
    raise exception 'maintenance_mark_forged: 保守の印は ops.begin_master_maintenance でだけ作る' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_maintenance_marks() from public;
create trigger trg_master_maintenance_marks_guard before insert on ops.master_maintenance_marks for each row execute function ops.guard_master_maintenance_marks();

-- 印を作る (DB の持ち主だけ・理由が要る)。UUID を表 (取引の番号・セッションのユーザーつき) と取引の中だけの設定 ops.master_maintenance に置く
create function ops.begin_master_maintenance(p_reason text) returns uuid
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_id uuid := gen_random_uuid();
begin
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'maintenance_owner_only: 保守の印は DB の持ち主だけが作れる (session_user %)', session_user using errcode = '42501';
  end if;
  if p_reason is null or length(btrim(p_reason)) = 0 or length(p_reason) > 500 then
    raise exception 'invalid_input: 保守の理由 (1〜500 字) が要る' using errcode = '22023';
  end if;
  perform pg_catalog.set_config('ops.master_maintenance_insert', v_id::text, true);
  insert into ops.master_maintenance_marks (mark_id, txid, session_role, reason) values (v_id, pg_catalog.txid_current(), session_user::text, btrim(p_reason));
  perform pg_catalog.set_config('ops.master_maintenance_insert', '', true);
  perform pg_catalog.set_config('ops.master_maintenance', v_id::text, true);   -- 取引の中だけ
  return v_id;
end $$;
revoke all on function ops.begin_master_maintenance(text) from public;

-- 今の取引に有効な印があるか = 設定の UUID・取引の番号・その行を書いた取引 (xmin)・セッションのユーザーが全部一致 (復元した古い印・取引の番号だけの一致・GUC だけ・直接の INSERT では通らない)
create function ops.master_maintenance_active() returns boolean
  language plpgsql volatile security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_id text := nullif(pg_catalog.current_setting('ops.master_maintenance', true), '');
begin
  if v_id is null or v_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then return false; end if;
  return exists (select 1 from ops.master_maintenance_marks m
                  where m.mark_id = v_id::uuid and m.txid = pg_catalog.txid_current() and m.session_role = session_user::text
                    and m.xmin::text = (pg_catalog.txid_current() % 4294967296)::text);
end $$;
revoke all on function ops.master_maintenance_active() from public;

-- ─── 7. G18: sku_kind の持ち主が company の間、全ロールで区分の変更と DELETE を拒む (保守の印だけ通す) ───
create function core.guard_sku_kind_locked() returns trigger language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if not ops.sku_kind_locked() then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op = 'UPDATE' and old.sku_kind is not distinct from new.sku_kind then return new; end if;
  if tg_op = 'TRUNCATE' then
    if ops.master_maintenance_active() then return null; end if;
    raise exception 'sku_kind_locked: 区分 (skus.sku_kind) の持ち主が company の間は core.skus を truncate しない' using errcode = '42501';
  end if;
  if ops.master_maintenance_active() then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op = 'DELETE' then
    raise exception 'sku_kind_locked: 区分 (skus.sku_kind) の持ち主が company の間は SKU を消さない (SKU % %。やめるのは状態で)', old.sku_id, old.code using errcode = '42501';
  end if;
  raise exception 'sku_kind_locked: 区分 (skus.sku_kind) の持ち主が company の間は区分を変えない (SKU % %: % → %)', old.sku_id, old.code, old.sku_kind, new.sku_kind using errcode = '42501';
end $$;
revoke all on function core.guard_sku_kind_locked() from public;
create trigger trg_skus_kind_locked before update or delete on core.skus for each row execute function core.guard_sku_kind_locked();
create trigger trg_skus_kind_locked_truncate before truncate on core.skus for each statement execute function core.guard_sku_kind_locked();

-- ─── 8. G19: 行の最終形 (deferred = commit のときの形)。保守の印でも外さない ───
-- 最終形の数 (会社 = null なら全部)。widen の判定 (会社 1)・復元の数え直し (全部)・§9 の確かめが同じ式を使う
create function ops.sku_kind_shape_counts(p_company_id integer default null) returns jsonb
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select jsonb_build_object(
    'single_product_mismatch', (select count(*) from core.skus s where (p_company_id is null or s.company_id = p_company_id) and (s.sku_kind = 'single') <> (s.product_id is not null)),
    'non_set_parent_components', (select count(*) from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id
                                   where (p_company_id is null or c.company_id = p_company_id) and p.sku_kind <> 'set'))
$$;
revoke all on function ops.sku_kind_shape_counts(integer) from public;

create function core.check_sku_kind_shape() returns trigger language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_kind text;
  v_pid  bigint;
  v_id   bigint;
begin
  if not ops.sku_kind_locked() then return null; end if;
  if tg_table_name = 'skus' then
    v_id := new.sku_id;
    select s.sku_kind, s.product_id into v_kind, v_pid from core.skus s where s.sku_id = v_id;
    if not found then return null; end if;
    if (v_kind = 'single') <> (v_pid is not null) then
      raise exception 'sku_kind_shape: SKU % の最終形が違う (区分 % なのに product_id %)', v_id, v_kind, coalesce(v_pid::text, 'なし') using errcode = '23514';
    end if;
    if v_kind <> 'set' and exists (select 1 from core.sku_components c where c.parent_sku_id = v_id) then
      raise exception 'sku_kind_shape: セットでない SKU % (%) に構成の行がある', v_id, v_kind using errcode = '23514';
    end if;
  else
    -- commit のときの形 = 構成の行が今もあれば、その親の今の区分 (行を後で消した = 見ない)
    select s.sku_kind into v_kind from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id
     where c.parent_sku_id = new.parent_sku_id and c.child_sku_id = new.child_sku_id;
    if found and v_kind <> 'set' then
      raise exception 'sku_kind_shape: 構成の親 (SKU %) がセットでない (%)', new.parent_sku_id, v_kind using errcode = '23514';
    end if;
  end if;
  return null;
end $$;
revoke all on function core.check_sku_kind_shape() from public;
create constraint trigger trg_skus_kind_shape after insert or update of sku_kind, product_id on core.skus
  deferrable initially deferred for each row execute function core.check_sku_kind_shape();
create constraint trigger trg_sku_components_kind_shape after insert or update of parent_sku_id on core.sku_components
  deferrable initially deferred for each row execute function core.check_sku_kind_shape();

-- ─── 9. G25: 復元の最後 (trigger を戻した後・commit の前) の数え直し。sku_kind の持ち主が company で最終形が 0 でなければ拒む = 復元全体が rollback ───
create function ops.assert_sku_kind_shape_after_restore() returns jsonb language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v jsonb := ops.sku_kind_shape_counts(null);
  v_locked boolean := ops.sku_kind_locked();
begin
  if v_locked and ((v ->> 'single_product_mismatch')::bigint <> 0 or (v ->> 'non_set_parent_components')::bigint <> 0) then
    raise exception 'restore_kind_shape: 区分の持ち主が company なのに、戻した行の最終形が違う (%) = 復元しない (ダンプを選び直す)', v::text using errcode = '23514';
  end if;
  return v || jsonb_build_object('locked', v_locked);
end $$;
revoke all on function ops.assert_sku_kind_shape_after_restore() from public;

-- ─── 10. 判定の本体 (G26)。何も書かない・鍵を取らない・どのロールにも EXECUTE を与えない (DEFINER の 2 つから呼ぶ) ───
-- 戻り値 = { ok, problems: [...], counts: {...}, loads: { recovery, prepared } (commit_seq は文字 = JS の Number にしない), acks: [...] }
create function ops._widen_judge(p_widen_prepare_id uuid, p_company_id integer) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[] := '{}';
  v_counts   jsonb := '{}'::jsonb;
  v_acks     jsonb := '[]'::jsonb;
  v_loads    jsonb := '{}'::jsonb;
  a          ops.master_widen_attempts;
  st         ops.master_ownership_state;
  v_phase    text;
  v_added    text[];
  v_hosts    text[] := ops.master_cutover_required_hosts();
  v_fresh    interval := make_interval(mins => ops.master_cutover_ack_fresh_minutes());
  v_now      timestamptz := clock_timestamp();
  v_host     text;
  v_n        integer;
  k          record;
  v_want     text[];
  v_got      text[];
  v_stop_at  timestamptz;
  c1         ops.master_load_commits;
  c2         ops.master_load_commits;
  v_ncommits bigint;
  m          record;
  v_ts       timestamptz;
  v_skus     jsonb;
  v_kind     jsonb;
  v_unv      jsonb;
  v_shape    jsonb;
  v_affected bigint;
begin
  if p_company_id is distinct from 1 then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('unsupported_company: 会社 ' || coalesce(p_company_id::text, 'null') || ' (今は会社 1 だけ)'), 'counts', '{}'::jsonb);
  end if;
  -- 1. 試み・段階・epoch・足すだけ
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id;
  if not found then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('attempt_missing: 試み ' || coalesce(p_widen_prepare_id::text, 'null') || ' が無い'), 'counts', '{}'::jsonb);
  end if;
  if a.state <> 'prepared' then v_problems := v_problems || format('attempt_not_prepared: 試みの状態が %s', a.state); end if;
  select c.phase into v_phase from ops.master_cutover_state c where c.id = 1;
  if v_phase is distinct from 'new_open' then v_problems := v_problems || format('phase_not_new_open: 段階が %s', coalesce(v_phase, '読めない')); end if;
  select * into st from ops.master_ownership_state s where s.id = 1;
  if not found then
    v_problems := v_problems || 'epoch_missing: 持ち主の epoch の行が無い'::text;
  else
    if st.prepared_hash is distinct from a.prepared_hash or st.prepared_at is distinct from a.prepared_at then
      v_problems := v_problems || 'prepared_changed: prepared が試みのものでない (cancel / 別の prepare)'::text;
    end if;
    if st.active_hash is distinct from a.active_hash then v_problems := v_problems || 'active_changed: prepare の後に active が変わった'::text; end if;
    if ops.ownership_hash(st.active_map) is distinct from st.active_hash then v_problems := v_problems || 'epoch_broken: active のハッシュが中身と違う'::text; end if;
    if st.prepared_map is not null and ops.ownership_hash(st.prepared_map) is distinct from st.prepared_hash then v_problems := v_problems || 'epoch_broken: prepared のハッシュが中身と違う'::text; end if;
  end if;
  if ops.ownership_hash(a.prepared_map) is distinct from a.prepared_hash then v_problems := v_problems || 'attempt_broken: 試みの持ち主表のハッシュが中身と違う'::text; end if;
  select coalesce(array_agg(p.key order by p.key), '{}') into v_added from jsonb_each_text(a.prepared_map) p
   where p.value = 'company' and (st.active_map ->> p.key) is distinct from 'company';
  if v_added is distinct from (select array_agg(x order by x) from unnest(a.added_keys) x) then
    v_problems := v_problems || format('not_additive: 足すキーが試みと違う (今 %s・試み %s)', array_to_string(v_added, ','), array_to_string(a.added_keys, ','));
  end if;
  if exists (select 1 from jsonb_each_text(st.active_map) x where x.value = 'company' and (a.prepared_map ->> x.key) is distinct from 'company') then
    v_problems := v_problems || 'not_additive: prepared が active の company のキーを load に戻す'::text;
  end if;
  if not (a.added_keys <@ ops.master_widen_allowed_keys()) then
    v_problems := v_problems || format('key_not_allowed: 広げてよいのは %s だけ (PR-8 の前)', array_to_string(ops.master_widen_allowed_keys(), ','));
  end if;

  -- 2. 書き手の ack (全部のプロセス・2 版・prepare の後・書きかけ 0・capable ⊇ 足すキー・試みの manifest・active / prepared を見た)
  foreach v_host in array v_hosts loop
    v_n := 0;
    for k in select distinct on (g.instance_id) g.* from ops.master_legacy_gate_acks g where g.host = v_host order by g.instance_id, g.acked_at desc, g.ack_id desc loop
      if k.inflight_count <> 0 or k.oldest_inflight_at is not null then
        v_problems := v_problems || format('ack: %s/%s: 書きかけが %s 件ある', v_host, k.instance_id, k.inflight_count);
      end if;
      if k.stopped then continue; end if;
      if k.acked_at < v_now - v_fresh then
        v_problems := v_problems || format('ack: %s/%s: 黙っている (最後の記録 %s。止めたプロセスなら stopped の記録を書く)', v_host, k.instance_id, k.acked_at);
        continue;
      end if;
      v_n := v_n + 1;
      if k.ack_version <> 2 then v_problems := v_problems || format('ack: %s/%s: 記録が 1 版 (active / prepared を見た証拠が無い = 新しい build の門が要る)', v_host, k.instance_id);
      else
        if k.acked_at <= a.prepared_at then v_problems := v_problems || format('ack: %s/%s: 記録が prepare の前', v_host, k.instance_id); end if;
        if k.active_hash_seen is distinct from a.active_hash then v_problems := v_problems || format('ack: %s/%s: 見た active が違う', v_host, k.instance_id); end if;
        if k.prepared_hash_seen is distinct from a.prepared_hash then v_problems := v_problems || format('ack: %s/%s: 見た prepared が試みのものでない', v_host, k.instance_id); end if;
        if not (a.added_keys <@ k.capable) then v_problems := v_problems || format('ack: %s/%s: build が足すキー (%s) を company にできない', v_host, k.instance_id, array_to_string(a.added_keys, ',')); end if;
      end if;
      if k.manifest_hash is distinct from a.manifest_hash then v_problems := v_problems || format('ack: %s/%s: 古い入口の一覧が試みのものと違う', v_host, k.instance_id); end if;
      if k.phase_seen is distinct from 'new_open' then v_problems := v_problems || format('ack: %s/%s: 見た段階が %s', v_host, k.instance_id, k.phase_seen); end if;
      v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', k.ack_id::text, 'host', k.host, 'instance_id', k.instance_id, 'build_id', k.build_id, 'acked_at', k.acked_at));
    end loop;
    if v_n = 0 then v_problems := v_problems || format('ack: %s: %s 分以内の記録が無い', v_host, ops.master_cutover_ack_fresh_minutes()); end if;
  end loop;

  -- 3. 手の入口の停止 = 足すキーに関係する manual の入口と完全一致 (DB の時刻・prepare の後)
  v_want := ops.widen_required_manual_entries(a.manifest_hash, a.added_keys);
  select coalesce(array_agg(s.entry_id order by s.entry_id), '{}'), max(s.stopped_at) into v_got, v_stop_at
    from ops.master_widen_manual_stops s where s.widen_prepare_id = a.widen_prepare_id;
  if v_got is distinct from v_want then
    v_problems := v_problems || format('manual_stops: 止めた手の入口 (%s) が要る入口 (%s) と同じでない', array_to_string(v_got, ','), array_to_string(v_want, ','));
  end if;
  if exists (select 1 from ops.master_widen_manual_stops s where s.widen_prepare_id = a.widen_prepare_id and s.stopped_at <= a.prepared_at) then
    v_problems := v_problems || 'manual_stops: 停止の記録が prepare の前'::text;
  end if;
  v_stop_at := greatest(coalesce(v_stop_at, a.prepared_at), a.prepared_at);

  -- 4. 試みの中のロード = base より後の commit がちょうど 2 つ (回収 = active → prepared = 試みの prepared・prepared が全体の最新)
  select count(*) into v_ncommits from ops.master_load_commits x where x.commit_seq > a.base_commit_seq;
  v_counts := v_counts || jsonb_build_object('commits_after_base', v_ncommits, 'base_commit_seq', a.base_commit_seq::text);
  if v_ncommits <> 2 then
    v_problems := v_problems || format('loads: 試みの中の commit が %s 個 (ちょうど 2 つ = 回収のロード → prepared のロード。夜間ロードが挟まった / prepared を 2 回流した = 試みを作り直す)', v_ncommits);
  else
    select * into c1 from ops.master_load_commits x where x.commit_seq > a.base_commit_seq order by x.commit_seq asc limit 1;
    select * into c2 from ops.master_load_commits x where x.commit_seq > a.base_commit_seq order by x.commit_seq desc limit 1;
    v_loads := jsonb_build_object('recovery', jsonb_build_object('commit_seq', c1.commit_seq::text, 'run_id', c1.ingest_run_id, 'epoch', c1.epoch),
                                  'prepared', jsonb_build_object('commit_seq', c2.commit_seq::text, 'run_id', c2.ingest_run_id, 'epoch', c2.epoch));
    if c1.epoch <> 'active' or c1.ownership_hash is distinct from a.active_hash then
      v_problems := v_problems || format('loads: 1 つ目 (%s) が回収のロード (epoch active・持ち主 = 今の active) でない (%s)', c1.ingest_run_id, c1.epoch);
    end if;
    if c2.epoch <> 'prepared' or c2.ownership_hash is distinct from a.prepared_hash then
      v_problems := v_problems || format('loads: 2 つ目 (%s) が試みの prepared のロードでない (%s)', c2.ingest_run_id, c2.epoch);
    end if;

    -- 5. ①⑦② 材料 (4 行・matched・ロードの中で世代が同じ・2 つのロードで世代とハッシュが同じ・規則の指紋・取得の時刻)
    select count(*) into v_n from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.entity in ('products', 'set_components');
    if v_n <> 4 then v_problems := v_problems || format('material: 2 つのロード × products・set_components の 4 行がそろっていない (%s 行)', v_n); end if;
    if exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.status <> 'matched') then
      v_problems := v_problems || 'material: matched でない材料がある (世代と中身が合わない / 世代が無い)'::text;
    end if;
    if (select count(distinct lm.generation_id) from ops.load_materials lm where lm.ingest_run_id = c1.ingest_run_id) <> 1
       or (select count(distinct lm.generation_id) from ops.load_materials lm where lm.ingest_run_id = c2.ingest_run_id) <> 1
       or exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.generation_id is null) then
      v_problems := v_problems || 'material: 同じロードの中で products と set_components の世代が違う'::text;
    end if;
    if exists (select 1 from ops.load_materials x join ops.load_materials y on y.entity = x.entity
                where x.ingest_run_id = c1.ingest_run_id and y.ingest_run_id = c2.ingest_run_id
                  and (x.generation_id is distinct from y.generation_id or x.content_hash is distinct from y.content_hash)) then
      v_problems := v_problems || 'material: 2 つのロードで材料の世代・ハッシュが違う'::text;
    end if;
    if exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.rule_fingerprint is distinct from a.loader_fingerprint) then
      v_problems := v_problems || 'material: ロードの規則の指紋が 2 つで違う / 試みの版 (loader_fingerprint) と違う'::text;
    end if;
    for m in select lm.entity, lm.ingest_run_id, lm.source_complete_at from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) order by lm.ingest_run_id, lm.entity loop
      v_ts := ops.rfc3339_ts(m.source_complete_at);
      if v_ts is null then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が RFC 3339 (明示の offset つき) でない', m.entity, coalesce(m.source_complete_at, 'null'));
      elsif v_ts <= v_stop_at then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が手の入口の停止 (%s) の前', m.entity, m.source_complete_at, v_stop_at);
      elsif v_ts > v_now + interval '5 minutes' then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が DB の今 + 5 分より先', m.entity, m.source_complete_at);
      end if;
    end loop;

    -- 6. ③④ prepared のロードの判断の記録 (section skus の payload.sku_kind・format = sku-kind-v1)
    select ld.payload into v_skus from ops.load_decisions ld where ld.ingest_run_id = c2.ingest_run_id and ld.section = 'skus';
    v_kind := case when jsonb_typeof(v_skus) = 'object' then v_skus -> 'sku_kind' end;
    if v_kind is null or jsonb_typeof(v_kind) <> 'object' then
      v_problems := v_problems || 'decisions: prepared のロードの判断の記録に sku_kind が無い'::text;
    else
      if (v_kind ->> 'format') is distinct from 'sku-kind-v1' or jsonb_typeof(v_kind -> 'format') is distinct from 'string' then
        v_problems := v_problems || format('decisions: sku_kind の形の版が %s (sku-kind-v1 だけ)', coalesce(v_kind ->> 'format', 'なし'));
      end if;
      if jsonb_typeof(v_kind -> 'held') is distinct from 'array' then
        v_problems := v_problems || 'decisions: sku_kind.held が配列でない'::text;
      elsif exists (select 1 from jsonb_array_elements(v_kind -> 'held') h where jsonb_typeof(h) <> 'string') then
        v_problems := v_problems || 'decisions: sku_kind.held が文字の配列でない'::text;
      elsif jsonb_array_length(v_kind -> 'held') <> 0 then
        v_problems := v_problems || format('decisions: sku_kind.held が %s 件 (区分の食い違いが残っている = 0 だけ)', jsonb_array_length(v_kind -> 'held'));
      end if;
      v_counts := v_counts || jsonb_build_object('held', case when jsonb_typeof(v_kind -> 'held') = 'array' then jsonb_array_length(v_kind -> 'held') end);
      v_unv := v_kind -> 'unverifiable';
      if jsonb_typeof(v_unv) is distinct from 'array' then
        v_problems := v_problems || 'decisions: sku_kind.unverifiable が配列でない'::text;
      elsif exists (select 1 from jsonb_array_elements(v_unv) u
                     where case
                       when jsonb_typeof(u) <> 'object' then true
                       when exists (select 1 from jsonb_object_keys(u) kk where kk not in ('reason', 'raw_code', 'code_norm')) then true
                       when jsonb_typeof(u -> 'reason') is distinct from 'string' or jsonb_typeof(u -> 'raw_code') is distinct from 'string' then true
                       when (u ->> 'reason') not in ('empty_code', 'unknown_kind', 'norm_collision') then true
                       when (u ->> 'reason') = 'empty_code' then not (jsonb_typeof(u -> 'code_norm') = 'null' and core.norm_code(u ->> 'raw_code') = '')
                       when jsonb_typeof(u -> 'code_norm') is distinct from 'string' then true
                       else (u ->> 'code_norm') = '' or core.norm_code(u ->> 'raw_code') is distinct from (u ->> 'code_norm') end) then
        v_problems := v_problems || 'decisions: sku_kind.unverifiable の行の形が違う ({reason, raw_code, code_norm}・core.norm_code(raw_code) = code_norm)'::text;
      else
        select count(distinct s.sku_id) into v_affected from core.skus s
         where s.company_id = p_company_id and s.code_norm in (select u ->> 'code_norm' from jsonb_array_elements(v_unv) u where (u ->> 'reason') <> 'empty_code');
        v_counts := v_counts || jsonb_build_object('unverifiable', jsonb_array_length(v_unv), 'affected_existing_cdb', v_affected);
        if v_affected <> 0 then
          v_problems := v_problems || format('decisions: 区分を確かめられない材料の行が C の SKU %s 件に当たる (NE で直してから試みを作り直す)', v_affected);
        end if;
      end if;
    end if;
  end if;

  -- 7. ⑤⑥ 最終形
  v_shape := ops.sku_kind_shape_counts(p_company_id);
  v_counts := v_counts || v_shape;
  if (v_shape ->> 'single_product_mismatch')::bigint <> 0 then v_problems := v_problems || format('shape: 区分と product_id の不整合が %s 件', v_shape ->> 'single_product_mismatch'); end if;
  if (v_shape ->> 'non_set_parent_components')::bigint <> 0 then v_problems := v_problems || format('shape: セットでない親の構成が %s 件', v_shape ->> 'non_set_parent_components'); end if;

  return jsonb_build_object('ok', coalesce(array_length(v_problems, 1), 0) = 0, 'problems', to_jsonb(v_problems), 'counts', v_counts, 'loads', v_loads, 'acks', v_acks,
    'widen_prepare_id', a.widen_prepare_id, 'added_keys', to_jsonb(a.added_keys), 'stop_at', v_stop_at, 'checked_at', v_now);
end $$;
revoke all on function ops._widen_judge(uuid, integer) from public;

-- watcher 用の読むだけ (§9 の 5b・§5 の手順 5b)。本体を呼ぶだけ・書かない・鍵を取らない
create function ops.widen_check_readonly(p_widen_prepare_id uuid, p_company_id integer) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_company_id is distinct from 1 then
    raise exception 'unsupported_company: 会社 % (今は会社 1 だけ。持ち主の epoch は全社で 1 つ)', p_company_id using errcode = '22023';
  end if;
  return ops._widen_judge(p_widen_prepare_id, p_company_id);
end $$;
revoke all on function ops.widen_check_readonly(uuid, integer) from public;

-- ─── 11. prepare --widen / 手の入口の停止 / cancel / widen (DB の持ち主だけ) ───
-- prepare: epoch の排他の鍵 → 段階の共有の鍵 → 足すだけを確かめ → 鍵の後に base_commit_seq を同じ取引で bigint で取る (走っていたロードは commit を待ってから base に入る)
create function ops.prepare_master_widen(p_company_id integer, p_prepared_map jsonb, p_loader_fingerprint text, p_manifest jsonb, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_phase   text;
  st        ops.master_ownership_state;
  v_hash    text;
  v_added   text[];
  v_mhash   text;
  v_base    bigint;
  v_at      timestamptz;
  v_id      uuid := gen_random_uuid();
  v_manual  text[];
begin
  if p_company_id is distinct from 1 then
    raise exception 'unsupported_company: 会社 % (今は会社 1 だけ。持ち主の epoch は全社で 1 つ)', p_company_id using errcode = '22023';
  end if;
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'widen_owner_only: 広げる道の操作は DB の持ち主だけ (session_user %)', session_user using errcode = '42501';
  end if;
  if p_actor is null or length(btrim(p_actor)) = 0 or length(p_actor) > 200 then raise exception 'invalid_input: actor (1〜200 字) が要る' using errcode = '22023'; end if;
  if coalesce(p_loader_fingerprint, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: loader_fingerprint (夜間ロードの規則の指紋・64 桁の 16 進) が要る' using errcode = '22023'; end if;
  if p_prepared_map is null or jsonb_typeof(p_prepared_map) <> 'object' then raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023'; end if;
  if exists (select 1 from jsonb_each(p_prepared_map) e
              where case when jsonb_typeof(e.value) <> 'string' then true
                         else (e.value #>> '{}') not in ('load', 'company') or e.key !~ '^[a-z][a-z0-9_]{0,40}(\.[a-z][a-z0-9_]{0,40})?$' end) then
    raise exception 'invalid_input: 持ち主表 ({ キー: load / company }) が要る' using errcode = '22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.master_ownership_lock_key());              -- epoch の排他 (夜間ロードの共有と並ぶ = 走っていたロードの commit を待つ)
  perform pg_catalog.pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'));          -- 段階の共有
  select c.phase into v_phase from ops.master_cutover_state c where c.id = 1;
  if v_phase is distinct from 'new_open' then raise exception 'widen_phase: 段階が % (広げる道は new_open のときだけ)', coalesce(v_phase, '読めない') using errcode = 'P0001'; end if;
  select * into st from ops.master_ownership_state s where s.id = 1 for update;
  if not found then raise exception 'epoch_missing: 持ち主の epoch の行が無い' using errcode = 'P0001'; end if;
  if st.prepared_hash is not null then raise exception 'prepared_exists: prepared が残っている (先に cancel)' using errcode = 'P0001'; end if;
  if exists (select 1 from jsonb_each_text(st.active_map) x where x.value = 'company' and (p_prepared_map ->> x.key) is distinct from 'company') then
    raise exception 'widen_not_additive: active の company のキーを load に戻せない (足すだけ)' using errcode = 'P0001';
  end if;
  select coalesce(array_agg(p.key order by p.key), '{}') into v_added from jsonb_each_text(p_prepared_map) p where p.value = 'company' and (st.active_map ->> p.key) is distinct from 'company';
  if cardinality(v_added) = 0 then raise exception 'widen_nothing: 足すキーが無い' using errcode = 'P0001'; end if;
  if not (v_added <@ ops.master_widen_allowed_keys()) then
    raise exception 'widen_key_not_allowed: 広げてよいのは % だけ (PR-8 の前・足そうとした %)', array_to_string(ops.master_widen_allowed_keys(), ','), array_to_string(v_added, ',') using errcode = 'P0001';
  end if;
  v_hash := ops.ownership_hash(p_prepared_map);
  v_mhash := ops.legacy_manifest_hash(p_manifest);
  if not exists (select 1 from ops.master_legacy_manifests lm where lm.manifest_hash = v_mhash) then
    raise exception 'manifest_unknown: この古い入口の一覧 (%) を門の記録で見たプロセスが無い (新しい build を配ってから)', left(v_mhash, 12) using errcode = 'P0001';
  end if;
  v_manual := ops.widen_required_manual_entries(v_mhash, v_added);
  -- 鍵の後に、同じ取引で base を取る (bigint のまま。行が無い = 0)
  select coalesce(max(x.commit_seq), 0) into v_base from ops.master_load_commits x;
  v_at := clock_timestamp();
  perform pg_catalog.set_config('ops.widen_protocol', '1', true);
  update ops.master_ownership_state set prepared_hash = v_hash, prepared_map = p_prepared_map, prepared_at = v_at, prepared_by = btrim(p_actor), updated_at = now() where id = 1;
  insert into ops.master_widen_attempts (widen_prepare_id, company_id, added_keys, active_hash, prepared_hash, prepared_map, prepared_at, base_commit_seq, loader_fingerprint,
                                         manifest_hash, prepared_by)
    values (v_id, p_company_id, v_added, st.active_hash, v_hash, p_prepared_map, v_at, v_base, p_loader_fingerprint, v_mhash, btrim(p_actor));
  perform pg_catalog.set_config('ops.widen_protocol', '', true);
  insert into ops.master_ownership_events (action, ownership_hash, ownership, actor, evidence)
    values ('prepare', v_hash, p_prepared_map, btrim(p_actor), jsonb_build_object('widen_prepare_id', v_id, 'base_commit_seq', v_base::text, 'added_keys', to_jsonb(v_added)));
  insert into ops.master_widen_events (widen_prepare_id, action, actor, detail)
    values (v_id, 'prepare', btrim(p_actor), jsonb_build_object('base_commit_seq', v_base::text, 'added_keys', to_jsonb(v_added), 'manifest_hash', v_mhash,
            'required_manual_entries', to_jsonb(v_manual), 'loader_fingerprint', p_loader_fingerprint));
  return jsonb_build_object('widen_prepare_id', v_id, 'prepared_hash', v_hash, 'prepared_at', v_at, 'base_commit_seq', v_base::text, 'added_keys', to_jsonb(v_added),
    'manifest_hash', v_mhash, 'required_manual_entries', to_jsonb(v_manual));
end $$;
revoke all on function ops.prepare_master_widen(integer, jsonb, text, jsonb, text) from public;

-- 手の入口を止めた記録 (DB の時刻)。止めてよいのは試みの「要る入口」だけ・試みが prepared のときだけ
create function ops.record_widen_manual_stop(p_widen_prepare_id uuid, p_entry_id text, p_stopped_by text, p_note text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  a    ops.master_widen_attempts;
  v_at timestamptz;
begin
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'widen_owner_only: 広げる道の操作は DB の持ち主だけ (session_user %)', session_user using errcode = '42501';
  end if;
  if p_stopped_by is null or length(btrim(p_stopped_by)) = 0 or length(p_stopped_by) > 200 then raise exception 'invalid_input: 止めた人 (1〜200 字) が要る' using errcode = '22023'; end if;
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id for update;
  if not found then raise exception 'attempt_missing: 試み % が無い', p_widen_prepare_id using errcode = 'P0001'; end if;
  if a.state <> 'prepared' then raise exception 'attempt_not_prepared: 試みの状態が %', a.state using errcode = 'P0001'; end if;
  if not (p_entry_id = any(ops.widen_required_manual_entries(a.manifest_hash, a.added_keys))) then
    raise exception 'entry_not_required: % は足すキー (%) に関係する手の入口でない', p_entry_id, array_to_string(a.added_keys, ',') using errcode = '22023';
  end if;
  insert into ops.master_widen_manual_stops (widen_prepare_id, entry_id, stopped_by, note) values (p_widen_prepare_id, p_entry_id, btrim(p_stopped_by), p_note)
    returning stopped_at into v_at;
  insert into ops.master_widen_events (widen_prepare_id, action, actor, detail)
    values (p_widen_prepare_id, 'manual_stop', btrim(p_stopped_by), jsonb_build_object('entry_id', p_entry_id, 'stopped_at', v_at, 'note', p_note));
  return jsonb_build_object('widen_prepare_id', p_widen_prepare_id, 'entry_id', p_entry_id, 'stopped_at', v_at);
end $$;
revoke all on function ops.record_widen_manual_stop(uuid, text, text, text) from public;

-- cancel: prepared を消す・試みを cancelled に (active は変えない)。次の prepare は新しい試み・新しい base
create function ops.cancel_master_widen(p_widen_prepare_id uuid, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  a  ops.master_widen_attempts;
  st ops.master_ownership_state;
begin
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'widen_owner_only: 広げる道の操作は DB の持ち主だけ (session_user %)', session_user using errcode = '42501';
  end if;
  if p_actor is null or length(btrim(p_actor)) = 0 or length(p_actor) > 200 then raise exception 'invalid_input: actor (1〜200 字) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.master_ownership_lock_key());
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id for update;
  if not found then raise exception 'attempt_missing: 試み % が無い', p_widen_prepare_id using errcode = 'P0001'; end if;
  if a.state <> 'prepared' then raise exception 'attempt_not_prepared: 試みの状態が %', a.state using errcode = 'P0001'; end if;
  select * into st from ops.master_ownership_state s where s.id = 1 for update;
  perform pg_catalog.set_config('ops.widen_protocol', '1', true);
  if found and st.prepared_hash is not distinct from a.prepared_hash and st.prepared_at is not distinct from a.prepared_at then
    update ops.master_ownership_state set prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now() where id = 1;
    insert into ops.master_ownership_events (action, ownership_hash, ownership, actor, evidence)
      values ('cancel_prepare', a.prepared_hash, a.prepared_map, btrim(p_actor), jsonb_build_object('widen_prepare_id', a.widen_prepare_id));
  end if;
  update ops.master_widen_attempts set state = 'cancelled', closed_at = clock_timestamp(), closed_by = btrim(p_actor) where widen_prepare_id = a.widen_prepare_id;
  perform pg_catalog.set_config('ops.widen_protocol', '', true);
  insert into ops.master_widen_events (widen_prepare_id, action, actor, detail) values (a.widen_prepare_id, 'cancel', btrim(p_actor), '{}'::jsonb);
  return jsonb_build_object('widen_prepare_id', a.widen_prepare_id, 'cancelled', true);
end $$;
revoke all on function ops.cancel_master_widen(uuid, text) from public;

-- widen (apply): DB の持ち主だけ。鍵 = epoch の排他 → 段階の排他 → マスタの書き込みの排他 (自分で取ったことを確かめる) → 判定の本体 → 写しの証拠 →
--   active ← prepared (prepared を消す) → 試みを widened → 出来事 → 段階の owner_hash → 段階の出来事 (同じ取引・0055 の守りがこの順を求める = G3)
create function ops.widen_master_ownership(p_widen_prepare_id uuid, p_company_id integer, p_actor text, p_evidence jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v       jsonb;
  a       ops.master_widen_attempts;
  v_seq   text;
  v_ev    jsonb;
begin
  if p_company_id is distinct from 1 then
    raise exception 'unsupported_company: 会社 % (今は会社 1 だけ。持ち主の epoch は全社で 1 つ)', p_company_id using errcode = '22023';
  end if;
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'widen_owner_only: 広げる道の操作は DB の持ち主だけ (session_user %)', session_user using errcode = '42501';
  end if;
  if p_actor is null or length(btrim(p_actor)) = 0 or length(p_actor) > 200 then raise exception 'invalid_input: actor (1〜200 字) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.master_ownership_lock_key());
  perform pg_catalog.pg_advisory_xact_lock(hashtext('ops.master_cutover'));
  perform pg_catalog.pg_advisory_xact_lock(core.master_write_lock_key());
  if not (ops.holds_xact_advisory(ops.master_ownership_lock_key(), true) and ops.holds_xact_advisory(hashtext('ops.master_cutover')::bigint, true)
          and ops.holds_xact_advisory(core.master_write_lock_key(), true)) then
    raise exception 'widen_locks: epoch・段階・マスタの書き込みの排他の鍵を持っていない' using errcode = '55000';
  end if;
  perform 1 from ops.master_ownership_state s where s.id = 1 for update;
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id for update;
  v := ops._widen_judge(p_widen_prepare_id, p_company_id);
  if not (v ->> 'ok')::boolean then
    raise exception 'widen_rejected: %', (select string_agg(x, ' / ') from jsonb_array_elements_text(v -> 'problems') x) using errcode = 'P0001', detail = v::text;
  end if;
  -- 6. 写しの証拠 (activate と同じ・CLI が miniPC の作り直しと確かめから集める)。読んだロード = 試みの prepared のロード
  v_seq := v #>> '{loads,prepared,commit_seq}';
  if p_evidence is null or jsonb_typeof(p_evidence) <> 'object' then raise exception 'evidence_invalid: 写しの証拠 (object) が要る' using errcode = '22023'; end if;
  if jsonb_typeof(p_evidence -> 'load_commit_seq') not in ('string', 'number') or (p_evidence ->> 'load_commit_seq') is distinct from v_seq then
    raise exception 'evidence_invalid: 写しの世代が読んだロード (%) が試みの prepared のロード (%) でない', p_evidence ->> 'load_commit_seq', v_seq using errcode = '22023';
  end if;
  if coalesce(length(p_evidence ->> 'build_id'), 0) = 0 or coalesce(length(p_evidence ->> 'generation_id'), 0) = 0 then
    raise exception 'evidence_invalid: 作り直し (build_id) と写しの世代 (generation_id) が要る' using errcode = '22023';
  end if;
  v_ev := jsonb_build_object('widen_prepare_id', a.widen_prepare_id, 'evidence', p_evidence, 'loads', v -> 'loads', 'counts', v -> 'counts');
  perform pg_catalog.set_config('ops.widen_protocol', '1', true);
  update ops.master_ownership_state set active_hash = prepared_hash, active_map = prepared_map, activated_at = now(), activated_by = btrim(p_actor), activated_evidence = v_ev,
         prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now() where id = 1;
  update ops.master_widen_attempts set state = 'widened', closed_at = clock_timestamp(), closed_by = btrim(p_actor) where widen_prepare_id = a.widen_prepare_id;
  perform pg_catalog.set_config('ops.widen_protocol', '', true);
  insert into ops.master_ownership_events (action, ownership_hash, ownership, actor, evidence) values ('widen', a.prepared_hash, a.prepared_map, btrim(p_actor), v_ev);
  insert into ops.master_widen_events (widen_prepare_id, action, actor, detail) values (a.widen_prepare_id, 'widen', btrim(p_actor), v || jsonb_build_object('evidence', p_evidence));
  -- 段階は new_open のまま、持ち主表のハッシュだけ付け替える (0051 の守り = 設定 ops.cutover_protocol・0055 の守り = owner_hash = active・prepared が無い)
  perform pg_catalog.set_config('ops.cutover_protocol', '1', true);
  update ops.master_cutover_state set owner_hash = a.prepared_hash, changed_at = now(), changed_by = btrim(p_actor),
         note = left('widen ' || array_to_string(a.added_keys, ',') || ' (' || a.widen_prepare_id::text || ')', 500)
   where id = 1;
  perform pg_catalog.set_config('ops.cutover_protocol', '', true);
  insert into ops.master_cutover_events (from_phase, to_phase, actor, evidence, acks, note)
    values ('new_open', 'new_open', btrim(p_actor), v_ev || jsonb_build_object('action', 'widen', 'owner_hash', a.prepared_hash), coalesce(v -> 'acks', '[]'::jsonb),
            left('widen ' || array_to_string(a.added_keys, ','), 500));
  return jsonb_build_object('widened', true, 'widen_prepare_id', a.widen_prepare_id, 'active_hash', a.prepared_hash, 'added_keys', to_jsonb(a.added_keys), 'loads', v -> 'loads', 'counts', v -> 'counts');
end $$;
revoke all on function ops.widen_master_ownership(uuid, integer, text, jsonb) from public;

-- ─── 12. 照合 ② の新商品のゲートの結果 (最小の計画 §3)。照合 ② の最後に watch_writer が 1 行・追記だけ・compare_run_id は一意 ───
--   kind_gate の 5 つの数は照合 ② のプログラムの数 (信じて使う = 残る危なさ 5)。最終形の 2 つの数は DB が自分で数えて同じ行に書く (呼び手の数を信じない)
create table ops.new_entry_gate_results (
  result_id                 bigint generated always as identity primary key,
  compare_run_id            text not null unique check (compare_run_id ~ '^[A-Za-z0-9_.:-]{1,80}$'),
  products_complete_at      timestamptz not null,
  setproducts_complete_at   timestamptz not null,
  kind_gate                 jsonb not null check (jsonb_typeof(kind_gate) = 'object'),
  single_product_mismatch   bigint not null check (single_product_mismatch >= 0),
  non_set_parent_components bigint not null check (non_set_parent_components >= 0),
  recorded_by               text not null,
  created_at                timestamptz not null default clock_timestamp()
);
comment on table ops.new_entry_gate_results is '照合 ② の新商品のゲートの結果 (0058・最小の計画 §3)。書くのは ops.record_new_entry_gate (watch_writer) だけ。許可は一番新しい行で出し、一番新しい行の許可だけが有効';

create function ops.guard_new_entry_gate_results() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.gate_result_protocol', true), '') is distinct from '1' then
    raise exception 'new_entry_gate_results は ops.record_new_entry_gate でだけ書く' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_new_entry_gate_results() from public;
create trigger trg_new_entry_gate_results_guard before insert on ops.new_entry_gate_results for each row execute function ops.guard_new_entry_gate_results();
select core.make_append_only('ops', 'new_entry_gate_results');

-- 0 以上の bigint の整数 (数か数字の文字)。違えば null
create function ops.jsonb_nonneg_bigint(p jsonb) returns bigint language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
begin
  if p is null then return null; end if;
  if pg_catalog.jsonb_typeof(p) = 'number' then
    if (p #>> '{}') !~ '^[0-9]{1,19}$' then return null; end if;
  elsif pg_catalog.jsonb_typeof(p) = 'string' then
    if (p #>> '{}') !~ '^[0-9]{1,19}$' then return null; end if;
  else return null; end if;
  begin return (p #>> '{}')::bigint; exception when others then return null; end;
end $$;
revoke all on function ops.jsonb_nonneg_bigint(jsonb) from public;

-- 種類ごとの許可の鍵 (保存 = 共有 / grant・revoke・ゲートの結果の記録 = 排他・設計 §3.10 の 2)
create function ops.new_entry_lease_lock_key(p_kind text) returns bigint language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.hashtext('ops.new_entry_lease:' || p_kind)::bigint
$$;
revoke all on function ops.new_entry_lease_lock_key(text) from public;
-- 許可の種類 (今は single だけ = set は sku_components を広げるまで閉じたまま)。排他の鍵はこの順に取る
create function ops.new_entry_lease_kinds() returns text[] language sql immutable as $$ select array['single', 'set']::text[] $$;
revoke all on function ops.new_entry_lease_kinds() from public;

-- 照合 ② の結果を 1 行残す (watch_writer)。kind_gate = { raw_mismatch, raw_unverifiable_affected_existing_cdb, norm_collision, unknown_kind, integrity_untrusted }
--   (この 5 つだけ・0 以上の整数)。取得の完了の時刻 (RFC 3339・明示の offset) は記録の時刻 (clock_timestamp) と同じ JST の日で、それより前 (古い取得の数を拒む)。
--   最初に許可の排他の鍵を種類の順に取る = 保存の取引の完了を待ってから書く (書いた瞬間に前の許可は無効 = _new_entry_lease_ok が一番新しい行を見る)。
--   同じ compare_run_id の 2 回目は拒む (unique)。戻り値 = { result_id (文字), shape }
create function ops.record_new_entry_gate(p_compare_run_id text, p_products_complete_at text, p_setproducts_complete_at text, p_kind_gate jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  k       text;
  v_pat   timestamptz;
  v_sat   timestamptz;
  v_now   timestamptz;
  v_day   date;
  v_shape jsonb;
  v_id    bigint;
begin
  foreach k in array ops.new_entry_lease_kinds() loop perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(k)); end loop;
  if coalesce(p_compare_run_id, '') !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: 照合の回 (英数字と _.:- の 1〜80 字) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_kind_gate) is distinct from 'object'
     or (select pg_catalog.array_agg(x order by x) from pg_catalog.jsonb_object_keys(p_kind_gate) x)
        is distinct from array['integrity_untrusted', 'norm_collision', 'raw_mismatch', 'raw_unverifiable_affected_existing_cdb', 'unknown_kind']
     or exists (select 1 from pg_catalog.jsonb_each(p_kind_gate) e where ops.jsonb_nonneg_bigint(e.value) is null) then
    raise exception 'invalid_input: kind_gate は 5 つの鍵 (raw_mismatch・raw_unverifiable_affected_existing_cdb・norm_collision・unknown_kind・integrity_untrusted) だけ・0 以上の整数' using errcode = '22023';
  end if;
  v_pat := ops.rfc3339_ts(p_products_complete_at);
  v_sat := ops.rfc3339_ts(p_setproducts_complete_at);
  if v_pat is null or v_sat is null then raise exception 'invalid_input: 取得の完了の時刻 (RFC 3339・明示の offset) が要る' using errcode = '22023'; end if;
  v_now := pg_catalog.clock_timestamp();
  v_day := (v_now at time zone 'Asia/Tokyo')::date;
  if v_pat > v_now or v_sat > v_now then raise exception 'invalid_input: 取得の完了の時刻が今より後' using errcode = '22023'; end if;
  if (v_pat at time zone 'Asia/Tokyo')::date <> v_day or (v_sat at time zone 'Asia/Tokyo')::date <> v_day then
    raise exception 'stale_fetch: 取得の完了の日が今日 (JST %) でない = 古い取得の数は残さない', v_day using errcode = 'P0001';
  end if;
  v_shape := ops.sku_kind_shape_counts(1);
  perform pg_catalog.set_config('ops.gate_result_protocol', '1', true);
  insert into ops.new_entry_gate_results (compare_run_id, products_complete_at, setproducts_complete_at, kind_gate, single_product_mismatch, non_set_parent_components, recorded_by, created_at)
    values (p_compare_run_id, v_pat, v_sat,
            (select pg_catalog.jsonb_object_agg(e.key, ops.jsonb_nonneg_bigint(e.value)) from pg_catalog.jsonb_each(p_kind_gate) e),
            (v_shape ->> 'single_product_mismatch')::bigint, (v_shape ->> 'non_set_parent_components')::bigint, session_user::text, v_now)
    returning result_id into v_id;
  perform pg_catalog.set_config('ops.gate_result_protocol', '', true);
  return pg_catalog.jsonb_build_object('result_id', v_id::text, 'compare_run_id', p_compare_run_id, 'shape', v_shape);
end $$;
revoke all on function ops.record_new_entry_gate(text, text, text, jsonb) from public;

-- ─── 12c. 開放の許可 (lease・最小の計画 §3)。新商品を作る DB の関数自身が確かめる (14 で register_new_sku・ne_reg_build・ne_reg_issue に組み込む) ───
create table ops.master_new_entry_leases (
  lease_id       bigint generated always as identity primary key,
  kind           text not null check (kind in ('single')),
  result_id      bigint not null references ops.new_entry_gate_results (result_id),   -- 許可を出した照合 ② の結果の行
  compare_run_id text not null,                                                        -- その行の照合の回 (配る時の「NE のコードの回 = 許可の回」に使う)
  granted_by     text not null,
  granted_at     timestamptz not null default clock_timestamp(),
  expires_at     timestamptz not null,
  revoked_at     timestamptz,
  revoke_reason  text check (revoke_reason is null or length(revoke_reason) between 1 and 500),
  revoked_by     text,
  constraint ck_mnel_revoked check ((revoked_at is null) = (revoke_reason is null) and (revoked_at is null) = (revoked_by is null)),
  constraint ck_mnel_expires check (expires_at > granted_at)
);
create index ix_master_new_entry_leases_kind on ops.master_new_entry_leases (kind, lease_id desc);
comment on table ops.master_new_entry_leases is '新商品の入口の開放の許可 (0058・最小の計画 §3)。出すのは ops.grant_new_entry_lease だけ (ログイン new_entry_gate)・期限 = 翌日 10:00 (JST)。新商品を作る DB の関数が ops._require_new_entry_lease で確かめる';

create function ops.guard_master_new_entry_leases() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op = 'DELETE' then raise exception 'master_new_entry_leases は消さない' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.lease_protocol', true), '') is distinct from '1' then
    raise exception 'master_new_entry_leases は ops.grant_new_entry_lease / revoke_new_entry_lease でだけ書く' using errcode = '42501';
  end if;
  if tg_op = 'UPDATE' then
    if old.revoked_at is not null or new.revoked_at is null then raise exception '許可の取り消しは 1 回だけ' using errcode = 'P0001'; end if;
    if (new.lease_id, new.kind, new.result_id, new.compare_run_id, new.granted_by, new.granted_at, new.expires_at)
       is distinct from (old.lease_id, old.kind, old.result_id, old.compare_run_id, old.granted_by, old.granted_at, old.expires_at) then
      raise exception '許可の中身は書き換えない (取り消して新しく出す)' using errcode = 'P0001';
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_new_entry_leases() from public;
create trigger trg_master_new_entry_leases_guard before insert or update or delete on ops.master_new_entry_leases for each row execute function ops.guard_master_new_entry_leases();
create trigger trg_master_new_entry_leases_truncate before truncate on ops.master_new_entry_leases for each statement execute function core.reject_mutation();

-- 許可の期限 = 東京の今日の翌日 10:00 (DB と session の TimeZone に左右されない)
create function ops.new_entry_lease_expiry(p_now timestamptz) returns timestamptz language sql immutable set search_path = pg_catalog, pg_temp as $$
  select (((p_now at time zone 'Asia/Tokyo')::date + 1) + time '10:00') at time zone 'Asia/Tokyo'
$$;
revoke all on function ops.new_entry_lease_expiry(timestamptz) from public;

-- 停止の床 (追記だけ): 取り消しのたびに、許可の有無によらず、その時点の一番新しい結果の行の番号を足す。許可はこれより新しい結果の行でしか出ない
create table ops.master_new_entry_stop_floors (
  floor_id        bigint generated always as identity primary key,
  kind            text not null check (kind in ('single', 'set')),
  floor_result_id bigint not null check (floor_result_id >= 0),
  closed_by_compare_run_id text check (closed_by_compare_run_id is null or closed_by_compare_run_id ~ '^[A-Za-z0-9_.:-]{1,80}$'),   -- 照合 ② の開始が閉じた = その回
  reason          text not null check (length(btrim(reason)) between 1 and 500),
  recorded_by     text not null,
  recorded_at     timestamptz not null default clock_timestamp()
);
select core.make_append_only('ops', 'master_new_entry_stop_floors');

-- 結果の行で許可を出せるか = 問題の配列 (空 = 出せる)。許可を出すとき (grant) が呼ぶ:
--   一番新しい結果の行・kind_gate の 5 つが全部 0・その行の最終形の 2 つと今の最終形が 0・その行が今日 (JST)・区分の持ち主が company・その行が widen の後・停止の床より新しい
create function ops._new_entry_gate_problems(p_kind text, p_result_id bigint) returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  r          ops.new_entry_gate_results;
  v_out      text[] := '{}';
  v_max      bigint;
  v_floor    bigint;
  v_widen_at timestamptz;
  v_shape    jsonb;
  v_bad      text[];
begin
  if p_kind is distinct from 'single' then return array['kind_not_supported: 新商品の種類 ' || coalesce(p_kind, 'null') || ' の許可はまだ無い (single だけ)']; end if;
  select max(x.result_id) into v_max from ops.new_entry_gate_results x;
  if v_max is null then return array['no_gate_result: 照合 ② のゲートの結果がまだ無い']; end if;
  if p_result_id is distinct from v_max then v_out := v_out || format('not_latest: 結果の行 %s は一番新しい行 (%s) でない', p_result_id, v_max); end if;
  select * into r from ops.new_entry_gate_results x where x.result_id = p_result_id;
  if not found then return v_out || 'result_missing'::text; end if;
  select pg_catalog.array_agg(e.key order by e.key) into v_bad from pg_catalog.jsonb_each(r.kind_gate) e where (e.value #>> '{}')::bigint <> 0;
  if v_bad is not null then v_out := v_out || format('kind_gate: 区分のゲートの数が 0 でない (%s)', pg_catalog.array_to_string(v_bad, '・')); end if;
  if r.single_product_mismatch <> 0 or r.non_set_parent_components <> 0 then
    v_out := v_out || format('shape_at_compare: 照合のときの最終形が 0 でない (単品と product_id %s・セットでない親の構成 %s)', r.single_product_mismatch, r.non_set_parent_components);
  end if;
  v_shape := ops.sku_kind_shape_counts(1);
  if (v_shape ->> 'single_product_mismatch')::bigint <> 0 or (v_shape ->> 'non_set_parent_components')::bigint <> 0 then v_out := v_out || format('shape: %s', v_shape::text); end if;
  if (r.created_at at time zone 'Asia/Tokyo')::date <> (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date then
    v_out := v_out || 'not_today: 結果の行が今日 (JST) のものでない'::text;
  end if;
  if not ops.sku_kind_locked() then v_out := v_out || 'sku_kind_not_company: 区分の持ち主が company でない (widen の前)'::text; end if;
  select max(a.closed_at) into v_widen_at from ops.master_widen_attempts a where a.state = 'widened' and 'skus.sku_kind' = any(a.added_keys);
  if v_widen_at is null then v_out := v_out || 'not_widened: skus.sku_kind を広げた記録が無い'::text;
  elsif r.created_at <= v_widen_at then v_out := v_out || 'before_widen: 結果の行が widen の前 (widen の後の照合 ② だけ)'::text;
  end if;
  select max(f.floor_result_id) into v_floor from ops.master_new_entry_stop_floors f where f.kind = p_kind;
  if v_floor is not null and p_result_id <= v_floor then v_out := v_out || format('stop_floor: 結果の行 %s は止めた時点の行 (%s) より新しくない', p_result_id, v_floor); end if;
  return v_out;
end $$;
revoke all on function ops._new_entry_gate_problems(text, bigint) from public;

-- 今有効な許可があるか (private・EXECUTE なし): その種類の最新の許可が取り消されていない・期限の前・許可の行 = 一番新しい結果の行
--   (新しい照合 ② が結果を書いた瞬間に前の許可は無効)・停止の床より新しい・区分の持ち主が company
create function ops._new_entry_lease_ok(p_kind text) returns boolean
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  l       ops.master_new_entry_leases;
  v_floor bigint;
begin
  select * into l from ops.master_new_entry_leases x where x.kind = p_kind order by x.lease_id desc limit 1;
  if not found or l.revoked_at is not null or pg_catalog.clock_timestamp() >= l.expires_at then return false; end if;
  if l.result_id is distinct from (select max(x.result_id) from ops.new_entry_gate_results x) then return false; end if;
  select max(f.floor_result_id) into v_floor from ops.master_new_entry_stop_floors f where f.kind = p_kind;
  if v_floor is not null and l.result_id <= v_floor then return false; end if;
  return ops.sku_kind_locked();
end $$;
revoke all on function ops._new_entry_lease_ok(text) from public;

-- 許可の共有の鍵 (§3.10 の 2)。新商品を作る 3 つの関数が request の鍵の後・段階の鍵の前に取る (private)。今の新商品の許可は single だけ = single の鍵
create function ops._new_entry_lease_shared_locks() returns void
  language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key('single'));
end $$;
revoke all on function ops._new_entry_lease_shared_locks() from public;

-- 新商品を作る DB の関数の中で呼ぶ (private)。種類ごとの共有の鍵 (grant・revoke・ゲートの結果の記録 = 排他と並ぶ) → 有効でなければ new_entry_closed
create function ops._require_new_entry_lease(p_kind text) returns void
  language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key(coalesce(p_kind, '')));
  if not ops._new_entry_lease_ok(p_kind) then
    raise exception 'new_entry_closed: 新商品 (%) の入口は閉じている (今朝の照合のゲートの許可が無い・期限切れ・取り消し・新しい照合の結果)', coalesce(p_kind, 'null') using errcode = 'P0001';
  end if;
end $$;
revoke all on function ops._require_new_entry_lease(text) from public;

-- 画面の表示用の読むだけ (master_edit・watcher)。保存の強制は上の DB の関数がする
create function ops.new_entry_lease_valid(p_kind text) returns boolean
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select ops._new_entry_lease_ok(p_kind)
$$;
revoke all on function ops.new_entry_lease_valid(text) from public;

-- アプリ (画面のロール master_edit) が新商品を作る取引で request の鍵の直後 (段階の鍵の前) に呼ぶ (PR-2 #1640 Codex R3: 鍵の式を JS に写さない = 順がずれない)。
--   §3.10 の順で種類の許可の共有の鍵を取り (取引の終わりまで持つ)、その時点で許可が有効か (ops._new_entry_lease_ok と同じ判定) を返す。
--   保存の強制は DB の関数 (register_new_sku・ne_reg_build・ne_reg_issue) がする = これの戻り値は画面の早い 409 のため
create function ops.acquire_new_entry_locks(p_kind text) returns boolean
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_kind is null or not (p_kind = any (ops.new_entry_lease_kinds())) then
    raise exception 'invalid_input: 知らない新商品の種類 %', coalesce(p_kind, 'null') using errcode = '22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key(p_kind));
  return ops._new_entry_lease_ok(p_kind);
end $$;
revoke all on function ops.acquire_new_entry_locks(text) from public;

-- 許可を出す (daily-sync の照合 ② の次の段 = ログイン new_entry_gate)。排他の鍵 → 一番新しい結果の行で条件を全部この中で確かめる → 1 行。
--   p_compare_run_id = 今回の照合 ② の回 (段がその回の report から渡す)。一番新しい結果の行の回と完全に一致するときだけ出す
--   (同じ日の再実行が結果を書く前に落ちた = 一番新しい行は前の回 = 出さない。再試行も同じ組)。
--   どれか外れたら拒む (呼び手は revoke してから非 0 で終わる)。戻り値 = { lease_id, result_id, expires_at }
create function ops.grant_new_entry_lease(p_kind text, p_compare_run_id text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r          ops.new_entry_gate_results;
  v_problems text[];
  v_now      timestamptz;
  v_id       bigint;
  v_expires  timestamptz;
begin
  if p_kind is distinct from 'single' then raise exception 'invalid_input: 知らない新商品の種類 % (今は single だけ)', p_kind using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(p_kind));
  select * into r from ops.new_entry_gate_results x order by x.result_id desc limit 1;
  v_problems := ops._new_entry_gate_problems(p_kind, r.result_id);
  if r.compare_run_id is distinct from p_compare_run_id then
    v_problems := v_problems || format('compare_run_mismatch: 一番新しい結果の行の回 (%s) が今回の照合の回 (%s) でない', coalesce(r.compare_run_id, 'なし'), coalesce(p_compare_run_id, 'null'));
  end if;
  if coalesce(array_length(v_problems, 1), 0) > 0 then
    raise exception 'lease_denied: %', array_to_string(v_problems, ' / ') using errcode = 'P0001';
  end if;
  v_now := pg_catalog.clock_timestamp();
  v_expires := ops.new_entry_lease_expiry(v_now);
  perform pg_catalog.set_config('ops.lease_protocol', '1', true);
  insert into ops.master_new_entry_leases (kind, result_id, compare_run_id, granted_by, granted_at, expires_at)
    values (p_kind, r.result_id, r.compare_run_id, session_user::text, v_now, v_expires)
    returning lease_id into v_id;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  return jsonb_build_object('lease_id', v_id::text, 'kind', p_kind, 'result_id', r.result_id::text, 'compare_run_id', r.compare_run_id, 'expires_at', v_expires);
end $$;
revoke all on function ops.grant_new_entry_lease(text, text) from public;

-- 照合 ② の始めに入口を閉じる (watch_writer・最小の計画 §3 の 0・Codex の High 1)。照合 ② は何かを読む前にこれを呼んで commit する。
--   今の許可を全部の種類で取り消し、停止の床をその時の一番新しい結果の行の番号まで進める (どの回が閉じたかを残す) = 照合の途中・照合が失敗した日は閉じたまま・
--   同じ日のもっと前の成功の行は床より古いので使えない。排他の鍵 (種類の順) = 保存の取引の完了を待ってから。同じ回の再実行でもう一度呼んでよい (床を足すだけ)。
--   戻り値 = { revoked (取り消した数), floor_result_id (文字), compare_run_id }
create function ops.close_new_entry_for_compare(p_compare_run_id text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  k       text;
  v_n     integer;
  v_floor bigint;
begin
  if coalesce(p_compare_run_id, '') !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: 照合の回 (英数字と _.:- の 1〜80 字) が要る' using errcode = '22023'; end if;
  foreach k in array ops.new_entry_lease_kinds() loop perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(k)); end loop;
  perform pg_catalog.set_config('ops.lease_protocol', '1', true);
  update ops.master_new_entry_leases set revoked_at = clock_timestamp(), revoke_reason = left('照合 ② の始めで閉じた (' || p_compare_run_id || ')', 500), revoked_by = session_user::text
   where revoked_at is null;
  get diagnostics v_n = row_count;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  select coalesce(max(x.result_id), 0) into v_floor from ops.new_entry_gate_results x;
  foreach k in array ops.new_entry_lease_kinds() loop
    insert into ops.master_new_entry_stop_floors (kind, floor_result_id, closed_by_compare_run_id, reason, recorded_by)
      values (k, v_floor, p_compare_run_id, '照合 ② の始め', session_user::text);
  end loop;
  return jsonb_build_object('revoked', v_n, 'floor_result_id', v_floor::text, 'compare_run_id', p_compare_run_id);
end $$;
revoke all on function ops.close_new_entry_for_compare(text) from public;

-- 許可を取り消す (ゲートの失敗 = new_entry_gate・人が止めたい = DB の持ち主)。排他の鍵 = 保存の取引の完了を待ってから・その後の保存は閉じる。
--   許可の有無によらず停止の床を足す。戻り値 = { revoked (取り消した数), floor_result_id (文字) }
create function ops.revoke_new_entry_lease(p_kind text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n     integer;
  v_floor bigint;
begin
  if p_kind is distinct from 'single' then raise exception 'invalid_input: 知らない新商品の種類 %', p_kind using errcode = '22023'; end if;
  if p_reason is null or length(btrim(p_reason)) = 0 then raise exception 'invalid_input: 取り消しの理由が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(p_kind));
  perform pg_catalog.set_config('ops.lease_protocol', '1', true);
  update ops.master_new_entry_leases set revoked_at = clock_timestamp(), revoke_reason = left(btrim(p_reason), 500), revoked_by = session_user::text
   where kind = p_kind and revoked_at is null;
  get diagnostics v_n = row_count;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  select coalesce(max(x.result_id), 0) into v_floor from ops.new_entry_gate_results x;
  insert into ops.master_new_entry_stop_floors (kind, floor_result_id, reason, recorded_by) values (p_kind, v_floor, left(btrim(p_reason), 500), session_user::text);
  return jsonb_build_object('revoked', v_n, 'floor_result_id', v_floor::text);
end $$;
revoke all on function ops.revoke_new_entry_lease(text, text) from public;

-- ─── 12f. 配ったファイルは短い間だけ渡す (v16 §3.9 の 3・R15 H1・R16 H1) ───
-- 配った時の許可と照合の回を export に残す (ne_reg_issue の初回が書く・動かさない)
alter table ops.ne_reg_exports add column lease_id bigint references ops.master_new_entry_leases (lease_id);
alter table ops.ne_reg_exports add column lease_compare_run_id text;

-- 0053 の守りに「配った時の許可は動かさない・配る時だけ書く」を足す (ほかは 0053 と同じ)
create or replace function ops.guard_ne_reg_exports() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_reg_exports は消さない' using errcode = 'P0001'; end if;
  if (new.export_id, new.company_id, new.kind, new.schema_version, new.header, new.encoding, new.trial, new.item_count, new.row_count, new.aggregate_token,
      new.payload_hash, new.sha256, new.file_bytes, new.request_id, new.ne_codes_run, new.cost_day, new.created_by, new.created_at)
     is distinct from
     (old.export_id, old.company_id, old.kind, old.schema_version, old.header, old.encoding, old.trial, old.item_count, old.row_count, old.aggregate_token,
      old.payload_hash, old.sha256, old.file_bytes, old.request_id, old.ne_codes_run, old.cost_day, old.created_by, old.created_at) then
    raise exception 'ne_reg_exports の中身は書き換えない (状態の列だけ)' using errcode = 'P0001';
  end if;
  if old.state = 'closed' and new is distinct from old then raise exception '閉じたファイルは変えない' using errcode = 'P0001'; end if;
  if new.state <> old.state and not ((old.state || '>' || new.state) = any (array['built>issued', 'issued>declared', 'built>closed', 'issued>closed', 'declared>closed'])) then
    raise exception 'ファイルの状態は % から % に進めない', old.state, new.state using errcode = 'P0001';
  end if;
  if old.issued_at is not null and (new.issued_at, new.issued_by) is distinct from (old.issued_at, old.issued_by) then raise exception '最初に配った記録は動かさない' using errcode = 'P0001'; end if;
  if old.declared_at is not null and (new.declared_at, new.declared_by) is distinct from (old.declared_at, old.declared_by) then raise exception '最初の申告の記録は動かさない' using errcode = 'P0001'; end if;
  -- 🆕 0058: 配った時の許可・照合の回は built → issued の 1 回だけ書く (後から付け替えない)
  if (new.lease_id, new.lease_compare_run_id) is distinct from (old.lease_id, old.lease_compare_run_id)
     and not (old.state = 'built' and new.state = 'issued' and old.lease_id is null and old.lease_compare_run_id is null) then
    raise exception '配った時の許可 (lease_id・lease_compare_run_id) は配る時だけ書く (動かさない)' using errcode = 'P0001';
  end if;
  return new;
end $$;

-- ファイル名 (lib/master-reg-csv.mjs の regFileName と同じ式: ne_register_<kind>_<作った JST の YYYYMMDD_HHMMSS>_<export_id>[_trial].csv)
create function ops.ne_reg_file_name(e ops.ne_reg_exports) returns text language sql stable set search_path = pg_catalog, pg_temp as $$
  select 'ne_register_' || e.kind || '_' || pg_catalog.to_char(e.created_at at time zone 'Asia/Tokyo', 'YYYYMMDD_HH24MISS') || '_' || e.export_id::text
         || case when e.trial then '_trial' else '' end || '.csv'
$$;
revoke all on function ops.ne_reg_file_name(ops.ne_reg_exports) from public;

-- 配ったファイルの byte 列 (master_edit だけ)。渡すのは: 配った (issued / declared) + 配ってから 2 時間以内 + 配った時の許可がまだ有効
--   (取り消されていない・期限の前・その種類の最新の許可・許可の行 = 一番新しい照合 ② の結果の行・停止の床 = ops._new_entry_lease_ok)。
--   どれか外れたら reg_file_expired: <理由> (アプリは 409)。人が「使わない」にして作り直す (build と issue で重なりをもう一度確かめる)
--   鍵 (R19 M・§3.10 の 2): 最初に許可の共有の鍵を取り、許可の確かめから byte 列を返すまで (取引の終わりまで) 持つ
--   = 確かめの直後に取り消し・照合 ② の結果の記録 (排他) が割り込んで、無効になった許可のファイルを返すことが無い
create function ops.ne_reg_file(p_export_id bigint) returns table (file_name text, sha256 text, file_bytes bytea)
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e      ops.ne_reg_exports;
  v_last bigint;
begin
  perform ops._new_entry_lease_shared_locks();
  select * into e from ops.ne_reg_exports x where x.export_id = p_export_id;
  if not found then raise exception 'not_found: ファイル % が無い', p_export_id using errcode = 'P0001'; end if;
  if e.state not in ('issued', 'declared') then
    raise exception 'reg_file_expired: not_issued (ファイル % は %)', p_export_id, e.state using errcode = 'P0001';
  end if;
  if e.issued_at is null or pg_catalog.clock_timestamp() >= e.issued_at + interval '2 hours' then
    raise exception 'reg_file_expired: issued_over_2h (配ってから 2 時間を過ぎた。使わないにして作り直す)' using errcode = 'P0001';
  end if;
  if e.lease_id is null then
    raise exception 'reg_file_expired: no_lease (配った時の許可の記録が無い)' using errcode = 'P0001';
  end if;
  select pg_catalog.max(l.lease_id) into v_last from ops.master_new_entry_leases l where l.kind = 'single';
  if v_last is distinct from e.lease_id or not ops._new_entry_lease_ok('single') then
    raise exception 'reg_file_expired: lease_invalid (配った時の許可がもう有効でない = 取り消し・期限切れ・新しい照合の結果)' using errcode = 'P0001';
  end if;
  if pg_catalog.encode(pg_catalog.sha256(e.file_bytes), 'hex') is distinct from e.sha256 then
    raise exception 'sha256_mismatch: ファイル % の sha256 が記録と違う', p_export_id using errcode = 'P0001';
  end if;
  return query select ops.ne_reg_file_name(e), e.sha256, e.file_bytes;
end $$;
revoke all on function ops.ne_reg_file(bigint) from public;

-- file_bytes を直接読めるロールを無くす (R16 H1): 表の SELECT を持つ owner でない全部のロール (と PUBLIC) から表の SELECT を外し、file_bytes 以外の列だけを列ごとに渡す。
--   列ごとの file_bytes の SELECT も外す。DB の持ち主だけが呼ぶ (0058 の最後と、2 つのロールの script が「全部の表に GRANT」の後に毎回呼ぶ = 流し直しても戻らない)
create function ops.restrict_ne_reg_file_bytes() returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_cols text;
  r      record;
  v_done text[] := '{}';
begin
  select pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' order by a.attnum) into v_cols
    from pg_catalog.pg_attribute a where a.attrelid = 'ops.ne_reg_exports'::regclass and a.attnum > 0 and not a.attisdropped and a.attname <> 'file_bytes';
  for r in select distinct x.grantee from pg_catalog.pg_class c cross join lateral pg_catalog.aclexplode(c.relacl) x
            where c.oid = 'ops.ne_reg_exports'::regclass and x.privilege_type = 'SELECT' and x.grantee <> c.relowner loop
    if r.grantee = 0 then
      execute 'revoke select on ops.ne_reg_exports from public';
      v_done := v_done || 'PUBLIC'::text;
    else
      execute pg_catalog.format('revoke select on ops.ne_reg_exports from %I', r.grantee::regrole::text);
      execute pg_catalog.format('grant select (%s) on ops.ne_reg_exports to %I', v_cols, r.grantee::regrole::text);
      v_done := v_done || r.grantee::regrole::text;
    end if;
  end loop;
  for r in select distinct x.grantee from pg_catalog.pg_attribute a cross join lateral pg_catalog.aclexplode(a.attacl) x
            where a.attrelid = 'ops.ne_reg_exports'::regclass and a.attname = 'file_bytes' and x.privilege_type = 'SELECT'
              and x.grantee <> (select c.relowner from pg_catalog.pg_class c where c.oid = 'ops.ne_reg_exports'::regclass) loop
    if r.grantee = 0 then execute 'revoke select (file_bytes) on ops.ne_reg_exports from public';
    else execute pg_catalog.format('revoke select (file_bytes) on ops.ne_reg_exports from %I', r.grantee::regrole::text); end if;
  end loop;
  return pg_catalog.jsonb_build_object('restricted', pg_catalog.to_jsonb(v_done));
end $$;
revoke all on function ops.restrict_ne_reg_file_bytes() from public;

-- ─── 12d. NE で一度でも見たコードの履歴 (v13 §3.9・R12 H2)。追記だけ・照合が NE のコードを入れ替えるのと同じ取引で足す (14a) ───
create table ops.master_ne_code_history (
  code_norm                 text not null check (length(code_norm) > 0),
  kind                      text not null check (kind in ('product', 'rep')),
  source                    text not null check (source in ('seed', 'record_ne_codes')),   -- 0058 の時の seed / 毎日の照合 (ops.record_ne_codes)
  first_seen_compare_run_id text,
  first_seen_at             timestamptz not null default clock_timestamp(),
  primary key (code_norm, kind)
);
select core.make_append_only('ops', 'master_ne_code_history');
comment on table ops.master_ne_code_history is 'NE で一度でも見たコード (0058・v13 §3.9)。新しいコードの確かめ (ops.new_sku_code_problem) と NE 登録の CSV の build は、今のコードに加えてここも「NE にもうある」と見る';

-- 0058 を当てる時の seed = 今の NE のコード (0041 の最後の照合の回) を履歴に入れる。あとは毎日の照合が足す (NE の全件の書き出しの集約はしない = 最小の計画 §2。
--   取り込みの直前に人が NE の画面で検索する = 履歴は「見えたことのあるコードを早めに止める」保険)
insert into ops.master_ne_code_history (code_norm, kind, source, first_seen_compare_run_id)
  select c.code_norm, c.kind, 'seed', (select m.compare_run_id from ops.master_ne_code_mark m where m.id = 1) from ops.master_ne_codes c
  on conflict (code_norm, kind) do nothing;

-- ─── 14. NE で一度でも見たコードの履歴 (v13 §3.9・R12 H2) と、新商品を作る DB の関数の中の許可の強制 (v12 §3.7・§3.8) ───
--   0041 / 0052 / 0053 / 0057 の関数を create or replace (中身は元のまま・足したのは 🆕 の行だけ。持ち主・実行権はそのまま)

-- 14a. 照合が NE のコードを入れ替えるとき、初めて見たコードを履歴にも足す (0041)
create or replace function ops.record_ne_codes(p jsonb) returns jsonb
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
  -- 🆕 0058 (v13 §3.9): 初めて見たコードを履歴に足す (同じ取引・消さない = 次の朝の取得で消えても「NE にもうある」として残る)
  insert into ops.master_ne_code_history (code_norm, kind, source, first_seen_compare_run_id) select c.code_norm, c.kind, 'record_ne_codes', v_run from ops.master_ne_codes c
    on conflict (code_norm, kind) do nothing;
  select jsonb_object_agg(k, n) into v_counts from (select kind || ':' || state as k, count(*) as n from ops.master_ne_codes group by 1) c;
  insert into ops.master_ne_code_mark (id, compare_run_id, observed_at, content_hash, counts, recorded_at)
    values (1, v_run, v_at, v_hash, coalesce(v_counts, '{}'::jsonb), now())
    on conflict (id) do update set compare_run_id = excluded.compare_run_id, observed_at = excluded.observed_at, content_hash = excluded.content_hash,
      counts = excluded.counts, recorded_at = excluded.recorded_at;
  return jsonb_build_object('state', 'written', 'rows', v_n, 'counts', coalesce(v_counts, '{}'::jsonb));
end $$;

-- 14b. 新しいコードの決まり: NE の今のコードに加えて、前に NE で見たコードも「NE にもうある」(0052)
create or replace function ops.new_sku_code_problem(p_code text) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_norm text;
begin
  if p_code is null or p_code !~ '^[a-z0-9_-]{1,30}$' or p_code ~ '^set-' then return 'code_shape'; end if;
  v_norm := core.norm_code(p_code);
  if exists (select 1 from core.skus where company_id = 1 and code_norm = v_norm) then return 'code_taken'; end if;
  if exists (select 1 from core.products where company_id = 1 and core.norm_code(display_code) = v_norm) then return 'code_is_rep'; end if;
  if exists (select 1 from ops.master_ne_codes where code_norm = v_norm) then return 'code_in_ne'; end if;
  if exists (select 1 from ops.master_ne_code_history h where h.code_norm = v_norm) then return 'code_in_ne'; end if;   -- 🆕 0058: 前に NE で見たコード (今朝の取得で欠けても)
  if exists (select 1 from events.master_change_events
              where entity_type = 'sku' and operation = 'DELETE' and core.norm_code(old_value ->> 'code') = v_norm) then return 'code_used_before'; end if;
  return null;
end $$;

-- 14c. 新商品の登録 (0057 の版) の中で許可を確かめる
create or replace function ops.register_new_sku(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_payload_hash text, p_entry jsonb) returns jsonb
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
  perform ops._new_entry_lease_shared_locks();   -- 🆕 0058 §3.10: 段階の鍵より前に許可の共有の鍵 (直接呼んでもアプリと同じ順。request の鍵は呼び手 = アプリが先に取る)
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
  -- 🆕 0058 (§3.7・§3.8): 新商品の開放の許可 (種類ごと・共有の鍵 = 取り消し・照合 ② の始めに閉じる・結果の記録と並ぶ)。無い = new_entry_closed (アプリは 409)
  if v_kind = 'single' then perform ops._require_new_entry_lease('single'); end if;   -- セットは sku_components を広げるまで画面の門 (NEW_ENTRY_KEYS) のまま
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
  -- 0057: 登録日 = この取引の JST の今日 (v_today = 原価の valid_from と同じ日)・出どころ portal を明示する (列の既定値には頼らない = 既定は空)
  insert into core.skus (sku_id, company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling, standard_price_jpy,
                         shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own, created_by_type, created_by_id,
                         registered_on, registered_on_source)
    overriding system value
    values (v_sku, 1, v_product, v_kind, v_code, v_name, v_tax, v_tclass, v_handling, v_price, v_ship_c, v_ship_m, v_ship_y, v_reorder, v_override, v_own, 'human', p_actor_id,
            v_today, 'portal');
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

-- 14d. NE 登録の CSV を作る (0053): 新しい export の前だけ許可・NE の履歴も見る
create or replace function ops.ne_reg_build(p jsonb, p_bytes bytea) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_rid      uuid;
  v_actor    text := pg_catalog.btrim(coalesce(p ->> 'actor', ''));
  v_kind     text := p ->> 'kind';
  v_schema   text := p ->> 'schema_version';
  v_header   text := p ->> 'header';
  v_day      date;
  v_cols     text[];
  v_prev     record;
  v_ids      bigint[];
  v_want     bigint[];
  it         jsonb;
  rw         jsonb;
  v_sku      record;
  v_reg      text;
  v_mark     text;
  v_latest   text;
  v_nrows    integer := 0;
  v_nitems   integer := 0;
  v_lines    text[] := '{}';
  v_text     text;
  v_bytes    bytea;
  v_sha      text;
  v_verified boolean;
  v_trial    boolean;
  v_export   bigint;
  v_item     bigint;
  v_row_no   integer := 0;
  v_from     integer;
  v_canon    jsonb;
  v_canons   jsonb := '[]'::jsonb;   -- [{ sku_id, code_norm, expected, cells, snapshot_hash, item_token }]
  v_snap     text;
  v_token    text;
  v_payload  text;
  v_agg      text;
  v_result   jsonb;
  c          jsonb;
begin
  if v_actor = '' or pg_catalog.length(v_actor) > 320 then raise exception 'invalid_input: 作る人 (actor) が要る' using errcode = '22023'; end if;
  begin v_rid := (p ->> 'request_id')::uuid; exception when others then raise exception 'invalid_input: request_id の形が違う' using errcode = '22023'; end;
  if v_rid is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(v_actor, p ->> 'reason') is not null then raise exception 'invalid_input: 作る人・理由の形が違う' using errcode = '22023'; end if;
  if not ((v_kind = 'products' and v_schema ~ '^ne-reg-single-v[0-9]+$' and v_header like 'syohin_code,%')
       or (v_kind = 'sets' and v_schema ~ '^ne-reg-set-v[0-9]+$' and v_header like 'set_syohin_code,%')) then
    raise exception 'invalid_input: 種類・形の版・見出しが合わない (% / % / %)', v_kind, v_schema, v_header using errcode = '22023';
  end if;
  v_cols := pg_catalog.string_to_array(v_header, ',');
  if v_cols && array['zaiko_su', 'yoyaku_zaiko_su', 'nyusyukko_riyu', 'visible_flg'] then
    raise exception 'invalid_input: 在庫の列は出さない (%)', v_header using errcode = '22023';
  end if;
  -- 形の版ごとの見出し (版を上げたらここも ops.ne_reg_canonical も足す)
  if not ((v_schema = 'ne-reg-single-v1' and v_header = 'syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code')
       or (v_schema = 'ne-reg-set-v1' and v_header = 'set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo')) then
    raise exception 'invalid_input: 形の版 % の見出しが違う (%)', v_schema, v_header using errcode = '22023';
  end if;
  if pg_catalog.jsonb_typeof(p -> 'items') is distinct from 'array' or pg_catalog.jsonb_array_length(p -> 'items') not between 1 and 1000 then
    raise exception 'invalid_input: items は 1〜1000 件' using errcode = '22023';
  end if;
  -- 印・ハッシュは関数が計算する (呼び手の値は受けない)
  if p ? 'aggregate_token' or p ? 'payload_hash'
     or exists (select 1 from pg_catalog.jsonb_array_elements(p -> 'items') i where pg_catalog.jsonb_typeof(i) <> 'object' or i ? 'item_token' or i ? 'snapshot_hash') then
    raise exception 'caller_hash: 印とハッシュ (aggregate_token・payload_hash・item_token・snapshot_hash) は関数が計算する = 送らない' using errcode = '22023';
  end if;
  begin v_day := (p ->> 'cost_day')::date; exception when others then v_day := null; end;
  if v_day is null or v_day < (pg_catalog.now() at time zone 'Asia/Tokyo')::date - 1 then
    raise exception 'invalid_input: 原価を見る日 (cost_day) が無い / 東京の今日より前' using errcode = '22023';
  end if;
  -- 🆕 0058 §3.10 (PR-2 Codex R4): 鍵の順 = request の鍵 → 許可の共有の鍵 → 段階の共有の鍵 → マスタの書き込みの共有の鍵 → SKU → CSV (アプリと同じ・直接呼んでも同じ)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.ne_reg_request:' || v_rid::text, 0));
  perform ops._new_entry_lease_shared_locks();
  perform ops.reg_write_gate(p -> 'ownership');   -- 段階の共有の鍵 → マスタの書き込みの共有の鍵 → 段階・持ち主表
  -- 同じ request_id = 同じファイル (種類・商品・作る人が違えば拒む)
  select e.export_id, e.kind, e.created_by, e.state, e.sha256, e.trial into v_prev from ops.ne_reg_exports e where e.request_id = v_rid;
  if found then
    select pg_catalog.array_agg(distinct (i ->> 'sku_id')::bigint order by (i ->> 'sku_id')::bigint) into v_want from pg_catalog.jsonb_array_elements(p -> 'items') i;
    if v_prev.kind is distinct from v_kind or v_prev.created_by is distinct from v_actor
       or v_want is distinct from (select pg_catalog.array_agg(x.sku_id order by x.sku_id) from ops.ne_reg_export_items x where x.export_id = v_prev.export_id) then
      raise exception 'request_id_reused: 同じ番号 (request_id) で違う中身' using errcode = '23505';
    end if;
    return pg_catalog.jsonb_build_object('export_id', v_prev.export_id, 'state', v_prev.state, 'sha256', v_prev.sha256, 'trial', v_prev.trial, 'replayed', true);
  end if;
  -- 🆕 0058 (v13 §3.8): 同じ request_id・同じ中身の replay (上) は許可が要らない。新しい export を作る前だけ許可を確かめる
  if v_kind = 'products' then perform ops._require_new_entry_lease('single'); end if;   -- 単品の CSV だけ (セットは画面の門のまま)
  -- 鍵: 商品と構成品 (今の構成 + 開いている構成の依頼) の SKU (sku_id の順) → CSV の鍵 → NE の元のコード (共有・照合の書き手と並ぶ)
  select pg_catalog.array_agg(distinct x) into v_ids from (
    select (i ->> 'sku_id')::bigint as x from pg_catalog.jsonb_array_elements(p -> 'items') i
    union all
    select c2.child_sku_id from pg_catalog.jsonb_array_elements(p -> 'items') i join core.sku_components c2 on c2.parent_sku_id = (i ->> 'sku_id')::bigint where v_kind = 'sets'
    union all
    select (e ->> 'sku_id')::bigint from pg_catalog.jsonb_array_elements(p -> 'items') i
      join ops.sku_component_requests q on q.set_sku_id = (i ->> 'sku_id')::bigint and q.status = 'open'
      cross join lateral pg_catalog.jsonb_array_elements(q.rows) e where v_kind = 'sets') t;
  perform ops.ne_reg_lock_skus(v_ids);
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  -- NE の元のコードは最新の照合の回のもの (呼び手の見た回と同じ)
  select m.compare_run_id into v_mark from ops.master_ne_code_mark m where m.id = 1;
  select r.compare_run_id into v_latest from ops.master_compare_runs r order by r.observed_at desc, r.compare_run_id desc limit 1;
  if v_mark is null or v_mark is distinct from v_latest or v_mark is distinct from (p ->> 'ne_codes_run') then
    raise exception 'ne_codes_stale: NE の元のコードが最新の照合の回のものでない (印 % / 最新 % / 画面 %)', v_mark, v_latest, p ->> 'ne_codes_run' using errcode = 'P0001';
  end if;
  v_lines := array[v_header];
  for it in select * from pg_catalog.jsonb_array_elements(p -> 'items') loop
    v_nitems := v_nitems + 1;
    select s.sku_id, s.code, s.code_norm, s.sku_kind, s.version, s.product_id, (select pr.version from core.products pr where pr.product_id = s.product_id) as product_version
      into v_sku from core.skus s where s.sku_id = (it ->> 'sku_id')::bigint for share;
    if not found then raise exception 'not_ready: SKU % が無い', it ->> 'sku_id' using errcode = 'P0001'; end if;
    if v_sku.sku_kind is distinct from (case v_kind when 'products' then 'single' else 'set' end) then
      raise exception 'not_ready: % は % でない', v_sku.code, v_kind using errcode = 'P0001';
    end if;
    if v_sku.code !~ '^[a-z0-9_-]{1,30}$' then raise exception 'not_ready: % は新しいコードの形でない', v_sku.code using errcode = 'P0001'; end if;
    select r.state into v_reg from ops.master_registrations r where r.sku_id = v_sku.sku_id for share;
    if v_reg is null or v_reg not in ('draft', 'ne_pending') then
      raise exception 'not_ready: % の登録の状態が % (下書き・NE 登録待ちだけ)', v_sku.code, coalesce(v_reg, 'なし') using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.ne_reg_export_items x where x.sku_id = v_sku.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial')) then
      raise exception 'not_ready: % にはまだ終わっていないファイルがある', v_sku.code using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.master_ne_codes c2 where c2.kind = 'product' and c2.code_norm = v_sku.code_norm)
       or exists (select 1 from ops.master_ne_code_history h where h.kind = 'product' and h.code_norm = v_sku.code_norm) then   -- 🆕 0058: 前に NE で見たコード
      raise exception 'already_in_ne: コード % は NE にもうある = 新規登録しない (同じ登録か確かめる / 別のコード)', v_sku.code using errcode = 'P0001';
    end if;
    -- 🚨 行と確かめる値 = 鍵の後に関数が今の値から作ったものと完全に同じ (High 1)
    v_canon := ops.ne_reg_canonical(v_sku.sku_id, v_day);
    if pg_catalog.jsonb_array_length(v_canon -> 'blockers') > 0 then
      raise exception 'not_ready: % を CSV にできない (%)', v_sku.code, (select pg_catalog.string_agg(b, '・') from pg_catalog.jsonb_array_elements_text(v_canon -> 'blockers') b)
        using errcode = 'P0001';
    end if;
    if pg_catalog.jsonb_typeof(it -> 'rows') is distinct from 'array' or (it -> 'rows') is distinct from (v_canon -> 'cells') then
      raise exception 'not_canonical: % の行が Company DB の今の値から作った行と違う', v_sku.code using errcode = '22023';
    end if;
    if (it -> 'expected') is distinct from (v_canon -> 'expected') then
      raise exception 'not_canonical: % の確かめる値が Company DB の今の値と違う', v_sku.code using errcode = '22023';
    end if;
    if exists (select 1 from pg_catalog.jsonb_array_elements(v_canon -> 'cells') r2 where pg_catalog.jsonb_array_length(r2) <> pg_catalog.array_length(v_cols, 1)) then
      raise exception 'invalid_input: % の行のセルの数が見出しと違う', v_sku.code using errcode = '22023';
    end if;
    for rw in select * from pg_catalog.jsonb_array_elements(v_canon -> 'cells') loop
      v_nrows := v_nrows + 1;
      if exists (select 1 from pg_catalog.jsonb_array_elements_text(rw) c3 where ops.ne_reg_csv_cell(c3) is null) then
        raise exception 'invalid_input: % の行に書けない文字 (制御文字) がある', v_sku.code using errcode = '22023';
      end if;
      select pg_catalog.string_agg(ops.ne_reg_csv_cell(c4.value #>> '{}'), ',' order by c4.ordinality) into v_text
        from pg_catalog.jsonb_array_elements(rw) with ordinality c4;
      v_lines := v_lines || v_text;
    end loop;
    -- 照合で確かめる値 = 配る行のセル (二重の確かめ)
    if ops.ne_reg_expected_problem(v_kind, v_canon -> 'expected', v_canon -> 'cells') is not null then
      raise exception 'expected_mismatch: % の確かめる値 (%) が CSV の行と違う', v_sku.code, ops.ne_reg_expected_problem(v_kind, v_canon -> 'expected', v_canon -> 'cells') using errcode = '22023';
    end if;
    -- 商品ごとの印 (関数が計算する): snapshot = 確かめる値と行 / item_token = 版 + 登録の状態 + snapshot
    v_snap := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('expected', v_canon -> 'expected', 'cells', v_canon -> 'cells')::text, 'UTF8')), 'hex');
    v_token := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('v', 'nrt-1', 'sku', pg_catalog.jsonb_build_array(v_sku.sku_id, v_sku.version),
      'product', pg_catalog.jsonb_build_array(v_sku.product_id, v_sku.product_version), 'registration', v_reg, 'snapshot', v_snap)::text, 'UTF8')), 'hex');
    v_canons := v_canons || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('sku_id', v_sku.sku_id, 'code_norm', v_sku.code_norm, 'code', v_sku.code, 'sku_kind', v_sku.sku_kind,
      'expected', v_canon -> 'expected', 'cells', v_canon -> 'cells', 'snapshot_hash', v_snap, 'item_token', v_token));
  end loop;
  if v_nrows > 1000 then raise exception 'too_many: 1 つのファイルに 1,000 行まで (% 行)', v_nrows using errcode = '22023'; end if;
  -- byte 列 = 関数が組み直した CSV と同じ (UTF-8・BOM なし・CRLF・最後の行にも CRLF)
  v_bytes := pg_catalog.convert_to(pg_catalog.array_to_string(v_lines, E'\r\n') || E'\r\n', 'UTF8');
  if p_bytes is distinct from v_bytes then raise exception 'bytes_mismatch: 配る byte 列が行から組み直した CSV と違う' using errcode = '22023'; end if;
  v_sha := pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex');
  -- ファイルの印 (関数が計算する): payload = 形の版・見出し・商品ごとの確かめる値と行 / aggregate = 商品ごとの item_token
  v_payload := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('schema', v_schema, 'header', v_header,
    'items', (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', x -> 'code_norm', 'expected', x -> 'expected', 'cells', x -> 'cells') order by o)
                from pg_catalog.jsonb_array_elements(v_canons) with ordinality t(x, o)))::text, 'UTF8')), 'hex');
  v_agg := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('schema', v_schema,
    'tokens', (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(x -> 'code_norm', x -> 'item_token') order by o)
                 from pg_catalog.jsonb_array_elements(v_canons) with ordinality t(x, o)))::text, 'UTF8')), 'hex');
  -- 実機の確かめの門 (種類 × 形の版 × 見出し・最後の記録が ok)。確かめていない = 試し用 (5 行まで)
  select coalesce((select v.result = 'ok' from ops.ne_csv_verified v
                    where v.kind = v_kind and v.col = 'new_registration' and v.encoding = 'utf8' and v.header = v_header and v.converter_version = v_schema
                    order by v.verified_id desc limit 1), false) into v_verified;
  v_trial := not v_verified;
  if v_trial and v_nrows > 5 then
    raise exception 'trial_limit: 形 % は実機でまだ確かめていないので、試し用 = 5 行まで (% 行)', v_schema, v_nrows using errcode = 'P0001';
  end if;
  -- 約束 (reg_csv_build・相手 = これから作るファイルの番号と商品・構成品の SKU = 関数が決めた)。番号を先に振る = 約束に書ける (⑤-2a の登録と同じ)
  v_export := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('ops.ne_reg_exports', 'export_id')::regclass);
  perform ops.open_reg_write('reg_csv_build', v_rid, v_actor, p ->> 'reason', p -> 'ownership', null, null, null, v_payload,
    pg_catalog.jsonb_build_object('export_id', v_export::text, 'sku_ids', pg_catalog.to_jsonb(v_ids), 'kind', v_kind));
  insert into ops.ne_reg_exports (export_id, kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes,
                                  request_id, ne_codes_run, cost_day, created_by)
    overriding system value
    values (v_export, v_kind, v_schema, v_header, 'utf8', v_trial, v_nitems, v_nrows, v_agg, v_payload, v_sha, v_bytes, v_rid, v_mark, v_day, v_actor);
  for c in select * from pg_catalog.jsonb_array_elements(v_canons) loop
    v_from := v_row_no + 1;
    insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
      values (v_export, (c ->> 'sku_id')::bigint, c ->> 'code_norm', c ->> 'code', c ->> 'sku_kind', c ->> 'item_token', c -> 'expected', c ->> 'snapshot_hash',
              v_from, v_from + pg_catalog.jsonb_array_length(c -> 'cells') - 1, v_actor)
      returning item_id into v_item;
    for rw in select * from pg_catalog.jsonb_array_elements(c -> 'cells') loop
      v_row_no := v_row_no + 1;
      insert into ops.ne_reg_export_rows (export_id, row_no, item_id, cells) values (v_export, v_row_no, v_item, rw);
    end loop;
  end loop;
  v_result := pg_catalog.jsonb_build_object('export_id', v_export::text, 'state', 'built', 'sha256', v_sha, 'trial', v_trial, 'rows', v_nrows, 'items', v_nitems, 'replayed', false,
    'payload_hash', v_payload, 'aggregate_token', v_agg);
  perform ops.close_reg_write(v_result, 'reg-csv #' || v_export);
  return v_result;
end $$;

-- 14e. 配る (0053): 初回の built → issued だけ許可
create or replace function ops.ne_reg_issue(p_request_id uuid, p_actor_id text, p_ownership jsonb, p_export_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e         ops.ne_reg_exports%rowtype;
  v_gone    text[];
  v_result  jsonb;
  v_lease   bigint;
  v_run     text;
  v_mark    text;
  v_now     timestamptz;
begin
  perform ops._new_entry_lease_shared_locks();   -- 🆕 0058 §3.10: 段階の鍵より前に許可の共有の鍵 (直接呼んでもアプリと同じ順。request の鍵は呼び手 = アプリが先に取る)
  if ops.reg_actor_problem(p_actor_id, null) is not null then raise exception 'invalid_input: 配る人 (actor) の形が違う' using errcode = '22023'; end if;
  perform ops.reg_write_gate(p_ownership);
  e := ops.ne_reg_lock_export(p_export_id);
  if e.state = 'closed' then raise exception 'closed: ファイル % は閉じている (%)', p_export_id, e.close_reason using errcode = 'P0001'; end if;
  if e.state <> 'built' then return pg_catalog.jsonb_build_object('export_id', p_export_id::text, 'state', e.state, 'already', true); end if;
  select pg_catalog.array_agg(i.ne_code order by i.item_id) into v_gone
    from ops.ne_reg_export_items i left join ops.master_registrations r on r.sku_id = i.sku_id
   where i.export_id = p_export_id and (i.state <> 'built' or r.state is null or r.state not in ('draft', 'ne_pending'));
  perform ops.open_reg_write('reg_csv_issue', p_request_id, p_actor_id, null, p_ownership, null, null, null,
    ops.reg_hash(pg_catalog.jsonb_build_object('op', 'reg_csv_issue', 'export_id', p_export_id, 'sha256', e.sha256)),
    pg_catalog.jsonb_build_object('export_id', p_export_id::text, 'sku_ids', ops.ne_reg_export_skus(p_export_id)));
  if v_gone is not null then
    update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left('配る前に使えなくなった商品がある (' || pg_catalog.array_to_string(v_gone, '・') || ')', 500),
           superseded_correction = 'まだ配っていない (NE には何もしていない)。作り直す', state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id
     where export_id = p_export_id and state = 'built';
    update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor_id, close_reason = 'superseded' where export_id = p_export_id;
    v_result := pg_catalog.jsonb_build_object('export_id', p_export_id::text, 'state', 'closed', 'refused', true, 'reason', 'item_superseded', 'codes', pg_catalog.to_jsonb(v_gone));
  else
    -- 🆕 0058 (v13 §3.8): 初回の built → issued (初めて配る) だけ許可を確かめる (もう配った・閉じる道は要らない)
    if e.kind = 'products' then perform ops._require_new_entry_lease('single'); end if;   -- 単品の CSV だけ
    -- 🆕 0058 (v17 §3.9 R16 M5): 配った時の許可と、その許可を出した朝の照合の回を export に残す (ops.ne_reg_file が「配った時の許可がまだ有効」を見る)。
    --   セットの CSV は許可が要らない = 有効な許可があるときだけ残す (無ければダウンロードできない = 閉じる側・設計への質問)
    select l.lease_id, l.compare_run_id into v_lease, v_run from ops.master_new_entry_leases l where l.kind = 'single' order by l.lease_id desc limit 1;
    if v_lease is not null and not ops._new_entry_lease_ok('single') then v_lease := null; v_run := null; end if;
    -- 🆕 0058 (v15 §3.9): 配る直前に、その時の NE のコード (最新の照合の回) と履歴の両方に無いことをもう一度確かめる (build の後の新しい取得で見えたコードを配らない)
    perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
    -- 🆕 0058 (R16 M5): 確かめる NE のコードは許可を出した朝の照合の回のもの (ops.master_ne_code_mark.compare_run_id = 許可の compare_run_id)。違えば ne_codes_stale
    if e.kind = 'products' then
      select m.compare_run_id into v_mark from ops.master_ne_code_mark m where m.id = 1;
      if v_mark is distinct from v_run then
        raise exception 'ne_codes_stale: NE のコードの回 (%) が許可を出した照合の回 (%) と違う = 配らない (新しい照合とゲートの後に)', coalesce(v_mark, 'なし'), coalesce(v_run, 'なし') using errcode = 'P0001';
      end if;
    end if;
    select pg_catalog.array_agg(k.code order by k.code) into v_gone from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id
     where i.export_id = p_export_id and i.state = 'built'
       and (exists (select 1 from ops.master_ne_codes c2 where c2.kind = 'product' and c2.code_norm = k.code_norm)
            or exists (select 1 from ops.master_ne_code_history h where h.kind = 'product' and h.code_norm = k.code_norm));
    if v_gone is not null then
      raise exception 'already_in_ne: コード % は NE にもうある (build の後に見えた) = 配らない (使わないにして作り直す)', pg_catalog.array_to_string(v_gone, '・') using errcode = 'P0001';
    end if;
    -- 確かめから issued_at までを同じ取引で。issued_at = 実際の時刻 (取引の始めでない = 2 時間の窓と翌朝の取り込みの時刻の比べに使う)
    v_now := pg_catalog.clock_timestamp();
    update ops.ne_reg_export_items set state = 'issued', state_changed_at = v_now, state_changed_by = p_actor_id where export_id = p_export_id and state = 'built';
    update ops.ne_reg_exports set state = 'issued', issued_at = v_now, issued_by = p_actor_id, lease_id = v_lease, lease_compare_run_id = v_run where export_id = p_export_id;
    v_result := pg_catalog.jsonb_build_object('export_id', p_export_id::text, 'state', 'issued', 'already', false);
  end if;
  perform ops.close_reg_write(v_result, 'reg-csv #' || p_export_id);
  return v_result;
end $$;

-- ─── 15. 権限 (ロールがある DB だけ。無い DB は create-master-edit-roles.mjs / create-watch-roles.mjs が後で付ける) ───
--   watcher        = 0058 の表を読む (保守の印の表は除く)・読むだけの判定 ops.widen_check_readonly・画面の表示用の ops.new_entry_lease_valid
--   watch_writer   = 照合 ② の開始で許可を閉じる ops.close_new_entry_for_compare と結果の記録 ops.record_new_entry_gate だけ (許可は出せない = 照合のコード 1 本で開放まで完結しない)
--   new_entry_gate = 開放の許可を出す / 取り消す だけ (NOINHERIT のログイン)
--   master_edit    = active_map・ops.new_entry_lease_valid (表示用)・ops.acquire_new_entry_locks (鍵の入口)・配ったファイルの ops.ne_reg_file (file_bytes の列は読めない)
--   master_gate    = 門の記録の 2 版
--   DB の持ち主だけ (だれにも渡さない) = prepare / cancel / 手の入口の停止 / widen / 保守の印 / 判定の本体 ops._widen_judge / private の _ の関数
revoke all on ops.master_maintenance_marks from public;
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_widen_attempts, ops.master_widen_events, ops.master_widen_manual_stops, ops.new_entry_gate_results,
      ops.master_new_entry_leases, ops.master_new_entry_stop_floors, ops.master_ne_code_history to watcher';
    execute 'revoke all on ops.master_maintenance_marks from watcher';
    execute 'grant execute on function ops.widen_check_readonly(uuid, integer) to watcher';
    execute 'grant execute on function ops.new_entry_lease_valid(text) to watcher';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'revoke all on ops.master_maintenance_marks from watch_writer';
    execute 'grant execute on function ops.record_new_entry_gate(text, text, text, jsonb) to watch_writer';
    execute 'grant execute on function ops.close_new_entry_for_compare(text) to watch_writer';
  end if;
  if exists (select 1 from pg_roles where rolname = 'new_entry_gate') then
    execute 'grant usage on schema ops to new_entry_gate';
    execute 'grant execute on function ops.grant_new_entry_lease(text, text) to new_entry_gate';
    execute 'grant execute on function ops.revoke_new_entry_lease(text, text) to new_entry_gate';
  end if;
  if exists (select 1 from pg_roles where rolname = 'master_edit') then
    execute 'grant execute on function ops.master_ownership_active_map() to master_edit';
    execute 'grant execute on function ops.new_entry_lease_valid(text) to master_edit';
    execute 'grant execute on function ops.ne_reg_file(bigint) to master_edit';
    execute 'grant execute on function ops.acquire_new_entry_locks(text) to master_edit';
  end if;
  if exists (select 1 from pg_roles where rolname = 'master_gate') then
    execute 'grant execute on function ops.record_legacy_gate_ack_v2(text, text, text, jsonb, text, text, integer, timestamptz, text, text, text[], boolean, text) to master_gate';
  end if;
end $$;

-- file_bytes を直接読めるロールを無くす (R16 H1)。2 つのロールの script も「全部の表に GRANT」の後に毎回これを呼ぶ (流し直しても戻らない)
select ops.restrict_ne_reg_file_bytes();
