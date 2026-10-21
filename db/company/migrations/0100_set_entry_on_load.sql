-- 0100 (仮番号・マージの時に次の空き番号に付け替える。migrate は欠番を断る): セットの新商品を、構成の持ち主 (sku_components) が NE (load) のままポータルで登録できるようにする
--   (2026-10-21 中原さん「セット商品つくれないんだけど。作れるようにして」・AI_reference CompanyDB構想/24 の「依頼 → NE で確かめる」の道を、持ち主の切替の前に使う)
--
-- なぜ切替なしで開けてよいか:
--   ・新商品の登録 (セット) が書くのは、セットの SKU・原価・登録の状態と「構成の依頼」(ops.sku_component_requests) だけ。core.sku_components は書かない (0052 から同じ)
--   ・core.sku_components は今までどおり夜間ロードが NE から書く (持ち主 load)。NE に取り込んだ後の夜に NE の中身が core に入る
--     = 出荷・売上の古い表 (NE から作る) と core の構成は今と同じく NE の中身のまま。ポータルの依頼は「NE に入れてほしい中身」(NE 登録の CSV の元) だけ
--   ・持ち主を変えないので、切替・戻す道は要らない (止めるのは今の非常の止め MASTER_NEW_ENTRY_STOP=1 か、毎朝の許可の取り消し)
-- なにを (前の最後の版を写して 🆕 0100 の行だけ変えた・鍵の順・security definer・search_path・実行権は同じ):
--   1. ops.set_entry_on_load() = 印 (アプリ・試験が to_regprocedure で「0100 の後の DB か」を見る)
--   2. 毎朝の許可: 単品の許可を出すとき (ops.grant_new_entry_lease('single', 回)) にセットの許可も同じ結果の行・同じ期限で出す / 取り消す (revoke 'single') ときはセットも取り消す。
--      毎朝の段 (new-entry-gate.mjs) は変えない (今までどおり single を呼ぶ)。'set' を直接出す / 取り消す呼び出しは今までどおり断る
--   3. 当てた時点で単品の許可が有効なら、同じ結果の行・同じ期限でセットの許可を 1 つ出す (翌朝を待たずに開く)
--   4. ops.register_new_sku (0061 の版): セットも許可を確かめる (0058〜0099 は画面の門だけ)
--   5. ops.guard_master_edit_write (0051 の版): 新商品の登録の約束 (sku_create) の中でセットの構成の依頼を INSERT するときだけ、持ち主 sku_components が load でも通す
--      (ほかの書き込み = 既にあるセットの構成の依頼・食い違いは今までどおり company のときだけ)
--   6. NE 登録の CSV のセットの版 ne-reg-set-v2 (試し用の 5 行の門なし・見出しと中身は v1 と同じ = AI_reference CompanyDB構想/24 の Q2 = a)。v1 は作らない・配らない版にする
--      (本番でセットの CSV はまだ 1 つも作っていない = 前に作った v1 のファイルは無い)
-- 変えない: core.sku_components の書き方 (夜間ロード)・持ち主表・照合 ②・新商品の許可の条件 (照合 ② の結果)・単品とまとまりの登録・CSV の作り方 (ops.ne_reg_build / ne_reg_issue)
-- 🚨 この migration は商品・構成・CSV の値を何も変えない (関数の置き換えと、許可の行を 1 つ足すだけ)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops.register_new_sku(uuid, text, text, jsonb, text, jsonb)') is null
     or pg_catalog.to_regprocedure('ops.guard_master_edit_write()') is null
     or pg_catalog.to_regprocedure('ops.grant_new_entry_lease(text, text)') is null or pg_catalog.to_regprocedure('ops.revoke_new_entry_lease(text, text)') is null
     or pg_catalog.to_regprocedure('ops.ne_reg_schema_rule(text)') is null or pg_catalog.to_regprocedure('ops.new_entry_lease_expiry(timestamptz)') is null
     or pg_catalog.to_regprocedure('ops.draft_in_ne_check(bigint)') is null then
    raise exception '0100 の前提: 新商品の登録 (0061)・書き込みの守り (0051)・許可 (0058・0082)・CSV の版 (0067)・0098 が無い';
  end if;
  if pg_catalog.to_regprocedure('ops.set_entry_on_load()') is not null then raise exception '0100: ops.set_entry_on_load がもうある'; end if;
end $$;

-- ═══════════ 1. 印 ═══════════
create function ops.set_entry_on_load() returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$ select true $$;
comment on function ops.set_entry_on_load() is '0100: セットの新商品を構成の持ち主 load のまま登録できる DB の印 (セットの許可は単品の許可と一緒に出る・CSV は ne-reg-set-v2)';
revoke all on function ops.set_entry_on_load() from public;

-- ═══════════ 2. 毎朝の許可: 単品と一緒にセットも ═══════════
-- 2a. 許可を出す (0082 の版 + 🆕 0100 の行)。鍵の順 = single → set (ops.new_entry_lease_kinds の順 = 保存の取引の共有の鍵・照合 ② の結果の記録と同じ順)
create or replace function ops.grant_new_entry_lease(p_kind text, p_compare_run_id text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r          ops.new_entry_gate_results;
  v_problems text[];
  v_now      timestamptz;
  v_id       bigint;
  v_expires  timestamptz;
  v_replaced integer;
  v_set_id   bigint;   -- 🆕 0100
begin
  if p_kind is distinct from 'single' then raise exception 'invalid_input: 知らない新商品の種類 % (single を出すとセットも一緒に出る)', p_kind using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(p_kind));
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key('set'));   -- 🆕 0100: セットの許可も同じ取引で書く (single の後 = 鍵の順は同じ)
  -- 🆕 0082 (#1722 Codex R1 High 2): 今回の回の結果の行が無い (今朝の記録の前に落ちた) = 古い行の問題 (not_today など) を混ぜずに current_result_missing だけ
  --   (毎朝の段はこの理由では閉じない = 今の許可はそのまま)
  if not exists (select 1 from ops.new_entry_gate_results x where x.compare_run_id = p_compare_run_id) then
    raise exception 'lease_denied: current_result_missing: 今回の照合の回 (%) の結果の行が無い (記録の前に落ちた) = 新しい許可は出さない', coalesce(p_compare_run_id, 'null') using errcode = 'P0001';
  end if;
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
  -- 🆕 0082: 前の許可は置き換わる (効くのは一番新しい許可だけ = 0058 から)。読みやすさのために取り消しの印を付ける
  update ops.master_new_entry_leases set revoked_at = v_now, revoke_reason = pg_catalog.left('置き換え: 次の許可 (lease ' || v_id::text || ') が出た', 500), revoked_by = session_user::text
   where kind = p_kind and revoked_at is null and lease_id <> v_id;
  get diagnostics v_replaced = row_count;
  -- 🆕 0100: セットの許可も同じ結果の行・同じ回・同じ期限で出す (セットの新商品は構成を依頼で持つだけ = 単品と同じ確かめ (照合 ② の区分のゲート) で開く)。前のセットの許可は置き換え
  insert into ops.master_new_entry_leases (kind, result_id, compare_run_id, granted_by, granted_at, expires_at)
    values ('set', r.result_id, r.compare_run_id, session_user::text, v_now, v_expires)
    returning lease_id into v_set_id;
  update ops.master_new_entry_leases set revoked_at = v_now, revoke_reason = pg_catalog.left('置き換え: 次の許可 (lease ' || v_set_id::text || ') が出た', 500), revoked_by = session_user::text
   where kind = 'set' and revoked_at is null and lease_id <> v_set_id;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  return jsonb_build_object('lease_id', v_id::text, 'kind', p_kind, 'result_id', r.result_id::text, 'compare_run_id', r.compare_run_id, 'expires_at', v_expires, 'replaced', v_replaced,
    'set_lease_id', v_set_id::text);
end $$;

-- 2b. 取り消す (0058 の版 + 🆕 0100 の行): single を取り消すとセットも取り消す (停止の床もセットに足す = 次の許可までセットも閉じる)
create or replace function ops.revoke_new_entry_lease(p_kind text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_n     integer;
  v_floor bigint;
  v_sn    integer;   -- 🆕 0100
begin
  if p_kind is distinct from 'single' then raise exception 'invalid_input: 知らない新商品の種類 % (single を取り消すとセットも一緒に取り消す)', p_kind using errcode = '22023'; end if;
  if p_reason is null or length(btrim(p_reason)) = 0 then raise exception 'invalid_input: 取り消しの理由が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(p_kind));
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key('set'));   -- 🆕 0100
  perform pg_catalog.set_config('ops.lease_protocol', '1', true);
  update ops.master_new_entry_leases set revoked_at = clock_timestamp(), revoke_reason = left(btrim(p_reason), 500), revoked_by = session_user::text
   where kind = p_kind and revoked_at is null;
  get diagnostics v_n = row_count;
  -- 🆕 0100: セットの許可も取り消す
  update ops.master_new_entry_leases set revoked_at = clock_timestamp(), revoke_reason = left(btrim(p_reason), 500), revoked_by = session_user::text
   where kind = 'set' and revoked_at is null;
  get diagnostics v_sn = row_count;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  select coalesce(max(x.result_id), 0) into v_floor from ops.new_entry_gate_results x;
  insert into ops.master_new_entry_stop_floors (kind, floor_result_id, reason, recorded_by) values (p_kind, v_floor, left(btrim(p_reason), 500), session_user::text);
  insert into ops.master_new_entry_stop_floors (kind, floor_result_id, reason, recorded_by) values ('set', v_floor, left(btrim(p_reason), 500), session_user::text);   -- 🆕 0100
  return jsonb_build_object('revoked', v_n, 'floor_result_id', v_floor::text, 'set_revoked', v_sn);
end $$;

-- ═══════════ 3. 当てた時点の単品の許可が有効なら、同じ結果の行・同じ期限でセットの許可を 1 つ出す ═══════════
--   (もう一度流しても、セットの許可が同じ結果の行で有効なら何もしない。試験は @@SET_LEASE_BEGIN@@〜@@SET_LEASE_END@@ の文をそのまま流す)
-- @@SET_LEASE_BEGIN@@
do $$
declare
  l     ops.master_new_entry_leases;
  v_new bigint;
begin
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key('single'));
  perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key('set'));
  select * into l from ops.master_new_entry_leases x where x.kind = 'single' order by x.lease_id desc limit 1;
  if not found or not ops._new_entry_lease_ok('single') then
    raise notice '0100: 単品の許可が今は無い = セットの許可は次の朝の許可と一緒に出る';
    return;
  end if;
  if ops._new_entry_lease_ok('set') and exists (select 1 from ops.master_new_entry_leases x
       where x.lease_id = (select max(y.lease_id) from ops.master_new_entry_leases y where y.kind = 'set') and x.result_id = l.result_id) then
    raise notice '0100: セットの許可はもう有効 (単品の許可 % と同じ結果の行)', l.lease_id;
    return;
  end if;
  perform pg_catalog.set_config('ops.lease_protocol', '1', true);
  insert into ops.master_new_entry_leases (kind, result_id, compare_run_id, granted_by, granted_at, expires_at)
    values ('set', l.result_id, l.compare_run_id, pg_catalog.left('0100 で単品の許可 (lease ' || l.lease_id::text || ') に合わせて出した', 500), l.granted_at, l.expires_at)
    returning lease_id into v_new;
  update ops.master_new_entry_leases set revoked_at = pg_catalog.clock_timestamp(), revoke_reason = pg_catalog.left('置き換え: 0100 のセットの許可 (lease ' || v_new::text || ')', 500), revoked_by = session_user::text
   where kind = 'set' and revoked_at is null and lease_id <> v_new;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  raise notice '0100: セットの許可 % を出した (単品の許可 % と同じ結果の行・期限 %)', v_new, l.lease_id, l.expires_at;
end $$;
-- @@SET_LEASE_END@@

-- ═══════════ 4. 新商品の登録 (0061 の版 + 🆕 0100 の 1 行 = セットも許可を確かめる) ═══════════
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
  -- 🆕 0060 (中原さん 2026-10-08): 単品は代表の仕入先が要る (NE の商品マスタで必須の項目)。セットは今までどおり付けない
  if v_kind = 'single' and v_supplier is null then raise exception 'invalid_value: 単品は代表の仕入先が要る (NE で必須の項目)' using errcode = '22023'; end if;
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
  perform ops._require_new_entry_lease(v_kind);   -- 🆕 0100: セットもセットの許可 (毎朝の単品の許可と一緒に出る)。0058〜0099 は単品だけ
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
  -- 🆕 0060 (中原さん 2026-10-08): 発送方法は無くてよい (届いてサイズを見てから商品の画面で入れる)。無い = 送料コード・発送方法 (名前)・送料の 3 つとも null。
  --   ある = 今までどおり送料コードと発送方法 (名前) の両方が要る (片方だけ・送料だけは断る)
  if coalesce(jsonb_typeof(v_s -> 'shipping_code'), 'null') = 'null' and coalesce(jsonb_typeof(v_s -> 'shipping_method'), 'null') = 'null' then
    if coalesce(jsonb_typeof(v_s -> 'shipping_cost_jpy'), 'null') <> 'null' then
      raise exception 'invalid_value: 送料コードが無いのに送料がある' using errcode = '22023';
    end if;
  elsif jsonb_typeof(v_s -> 'shipping_code') is distinct from 'string' or length(btrim(v_ship_c)) not between 1 and 20 or v_ship_c ~ '[[:cntrl:]]'
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
  -- 🆕 0061 (中原さん 2026-10-08): 推奨保有月数は要る (単品もセットも・0 は入れてよい)
  if v_reorder is null then raise exception 'invalid_value: 推奨保有月数が要る (0〜60)' using errcode = '22023'; end if;
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
    -- 🆕 0061 (中原さん 2026-10-08): 単品は売上分類が要る (「あとで」は無い)。セットは今までどおり構成品から導く / 導けないときだけ上書き
    if v_sales is null then raise exception 'invalid_value: 単品は売上分類が要る (1〜4)' using errcode = '22023'; end if;
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
        case when v_ship_c is null then '発送方法 (送料コード) はまだです。届いてサイズを見てから、商品の画面で入れてください (利益の見込み・product-hub の楽天の配送方法に使います)' end,
        case when v_tclass = 'MIXED' then '構成品の税率が 8% と 10% で混ざっています (セットの税率は低い方の 8%・MIXED)' end,
        case when (v_der ->> 'stopped_codes') is not null then format('中止の構成品 (%s) があるので、セットも中止になります', v_der ->> 'stopped_codes') end,
        case when v_name ~ '_(白ビ袋|梱機プ|長3封|白プチ|ネコ段|K-44|K-50|K-60|厚紙封|パフ箱|その他)$'
             then '名前の末尾に資材の印があります。資材は梱包アプリで登録します (新しい名前には付けない・D-47)' end], null)),
    'ne_steps', to_jsonb(array_remove(array[
        case when v_kind = 'set' then '下書きで登録しました。次は「NE 登録の CSV」で CSV を作って NE に取り込み、「NE に取り込んだ」を押します (🆕 0100)'
             else '下書きで登録しました。NE・ロジザードへの登録 (新規登録の CSV) はまだです (次の段階で「NE 登録へ進む」を足します)' end,
        case when v_kind = 'set' then 'セットの構成は「構成の依頼」として持っています。NE に取り込んで翌朝 NE の構成が同じと確かめたら、その夜に今の構成になります (🆕 0100)' end], null)),
    'card', case when v_event is null then null else jsonb_build_object('event_id', v_event::text, 'status', 'pending') end,
    'request_payload_hash', p_payload_hash);
  insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
    values (p_request_id, 1, 'sku_create', v_code, v_sku, p_actor_id, v_whash, 'done', v_result,
            least(coalesce((p_entry ->> 'started_at')::timestamptz, pg_catalog.now()), pg_catalog.clock_timestamp()));
  -- 約束を閉じる = この関数の外では (同じ取引でも) 画面のロールは登録の約束で書けない。約束の行と done は commit の確かめに残る
  perform pg_catalog.set_config('ops.master_write_session', '', true);
  return v_result;
end $$;

-- ═══════════ 5. 書き込みの守り (0051 の版 + 🆕 0100 の continue の 1 行) ═══════════
create or replace function ops.guard_master_edit_write() returns trigger
  language plpgsql security definer set search_path = pg_catalog, ops, core, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_old     jsonb;
  v_new     jsonb;
  v_sess    ops.master_write_sessions;
  v_sku     bigint;
  v_role    text;   -- 'edited' (直す SKU) / 'derived' (含むセット) / null (相手でない)
  v_bad     text;
  v_loop    boolean;
  v_depth   integer;
  k         text;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op in ('UPDATE', 'DELETE') then v_old := to_jsonb(old); end if;
  if tg_op in ('INSERT', 'UPDATE') then v_new := to_jsonb(new); end if;
  v_sess := ops.current_master_write_session();
  if v_tbl = 'ops.master_edit_requests' then
    if tg_op = 'INSERT' and (v_new ->> 'status') = 'failed' then return new; end if;
    if v_sess.session_id is null then
      raise exception 'master_write_session_required: 保存の記録 (done) は、同じ取引で ops.begin_master_write をした後だけ' using errcode = '42501';
    end if;
    if tg_op <> 'INSERT' or (v_new ->> 'request_id')::uuid is distinct from v_sess.request_id or (v_new ->> 'actor_id') is distinct from v_sess.actor_id
       or (v_new ->> 'sku_id')::bigint is distinct from v_sess.sku_id or (v_new ->> 'operation') is distinct from v_sess.operation
       or (v_new ->> 'payload_hash') is distinct from v_sess.payload_hash then
      raise exception 'master_write_session_mismatch: 保存の記録 (done) が約束 (request_id・人・SKU・操作・保存の中身のハッシュ) と違う' using errcode = '42501';
    end if;
    return new;
  end if;
  if v_sess.session_id is null then
    raise exception 'master_write_session_required: 画面のロールの書き込み (%) は、同じ取引で ops.begin_master_write を呼んだ後だけ', v_tbl using errcode = '42501';
  end if;
  if (select phase from ops.master_cutover_state where id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない (%)', v_tbl using errcode = '42501';
  end if;
  if not ops.master_write_allowed(v_sess.operation, v_tbl, tg_op) then
    raise exception 'master_write_operation: 約束の操作 % では % に % できない', v_sess.operation, v_tbl, tg_op using errcode = '42501';
  end if;
  -- 約束の相手の行か (変える前と後の両方)。SKU と原価は 直す SKU / 含むセット を分ける
  if v_tbl in ('core.skus', 'core.sku_costs') then
    if v_old is not null and v_new is not null and (v_old ->> 'sku_id') is distinct from (v_new ->> 'sku_id') then v_role := null;
    else
      v_sku := coalesce(v_new ->> 'sku_id', v_old ->> 'sku_id')::bigint;
      v_role := case when v_sku = v_sess.sku_id then 'edited' when v_sku = any(v_sess.derived_sku_ids) then 'derived' end;
    end if;
  elsif v_tbl = 'core.products' then
    v_role := case when (v_old is null or (v_old ->> 'product_id')::bigint = any(v_sess.target_product_ids))
                    and (v_new is null or (v_new ->> 'product_id')::bigint = any(v_sess.target_product_ids)) then 'edited' end;
  elsif v_tbl = 'core.supplier_skus' then
    v_role := case when (v_old is null or (v_old ->> 'sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'sku_id')::bigint = v_sess.sku_id) then 'edited' end;
  elsif v_tbl in ('ops.sku_component_requests', 'ops.sku_component_breaches') then
    v_role := case when (v_old is null or (v_old ->> 'set_sku_id')::bigint = v_sess.sku_id) and (v_new is null or (v_new ->> 'set_sku_id')::bigint = v_sess.sku_id) then 'edited' end;
  end if;
  if v_role is null then
    raise exception 'master_write_target: 約束の相手 (SKU %) の行でない (%)', v_sess.sku_id, v_tbl using errcode = '42501';
  end if;
  -- 含むセット = 導く値の計算し直しだけ
  if v_role = 'derived' then
    if v_tbl = 'core.skus' then
      select string_agg(x, ', ' order by x) into v_bad from jsonb_object_keys(v_new) as t(x)
       where not (x = any(core.master_audit_ignored_columns())) and not (x = any(ops.master_write_derived_columns())) and (v_old -> x) is distinct from (v_new -> x);
      if v_bad is not null then
        raise exception 'master_write_derived_only: 含むセット (SKU %) は導く値 (税率・税区分・取扱区分) だけ直せる (変えた列: %)', v_sku, v_bad using errcode = '42501';
      end if;
    elsif (tg_op = 'INSERT' and (v_new ->> 'cost_source') is distinct from 'set_calc')
       or (tg_op in ('UPDATE', 'DELETE') and (v_old ->> 'cost_source') in ('manual', 'override_zero')) then
      raise exception 'master_write_derived_only: 含むセット (SKU %) の原価は計算の行 (set_calc) だけ (人が決めた原価は触らない)', v_sku using errcode = '42501';
    end if;
  end if;
  -- 業務の約束 (アプリの確かめを DB でも・#1563 R5 M1)
  if v_tbl = 'core.supplier_skus' and coalesce((v_new ->> 'is_primary')::boolean, false)
     and not exists (select 1 from core.suppliers s where s.supplier_id = (v_new ->> 'supplier_id')::bigint and s.active) then
    raise exception 'master_write_invariant: 代表の仕入先 % は取引停止か無い', v_new ->> 'supplier_id' using errcode = '42501';
  end if;
  if v_tbl = 'ops.sku_component_requests' and tg_op = 'INSERT' and ops.component_request_rows_ok(v_new -> 'rows', 1, 20)   -- 形の誤りは表の CHECK が拒む
     and exists (select 1 from jsonb_array_elements(v_new -> 'rows') e
                  where (e ->> 'sku_id')::bigint = v_sess.sku_id
                     or not exists (select 1 from core.skus k where k.sku_id = (e ->> 'sku_id')::bigint and k.sku_kind = 'single')) then
    raise exception 'master_write_invariant: 構成の依頼の構成品は、ある単品だけ (セット自身・セット・例外の SKU・無い SKU は入れない)' using errcode = '42501';
  end if;
  if v_tbl = 'core.products' and tg_op = 'UPDATE' and (v_new ->> 'parent_product_id') is not null
     and (v_old ->> 'parent_product_id') is distinct from (v_new ->> 'parent_product_id') then
    with recursive up(id, d) as (
      select (v_new ->> 'parent_product_id')::bigint, 1
      union all
      select p.parent_product_id, up.d + 1 from core.products p join up on p.product_id = up.id where p.parent_product_id is not null and up.d < 200)
    select coalesce(bool_or(id = (v_new ->> 'product_id')::bigint), false), max(d) into v_loop, v_depth from up;
    if v_loop or v_depth >= 200 then
      raise exception 'master_write_invariant: 代表 (親) にすると親子が輪になる (商品 %)', v_new ->> 'product_id' using errcode = '42501';
    end if;
  end if;
  foreach k in array ops.master_edit_owner_keys(v_tbl, tg_op, v_old, v_new) loop
    -- 🆕 0100: 新商品の登録 (約束 sku_create) が新しいセットの構成の依頼を 1 つ入れるのは、構成の持ち主 (sku_components) が load でも通す
    --   (依頼は「NE に入れてほしい中身」だけ = core.sku_components は書かない。core の構成は今までどおり夜間ロードが NE から書く)
    continue when k = 'sku_components' and v_tbl = 'ops.sku_component_requests' and tg_op = 'INSERT' and v_sess.operation = 'sku_create';
    if (v_sess.ownership ->> k) is distinct from 'company' then
      raise exception 'owner_not_company: % の持ち主が company でない (%)', k, v_tbl using errcode = '42501';
    end if;
  end loop;
  return case when tg_op = 'DELETE' then old else new end;
end $$;

-- ═══════════ 6. NE 登録の CSV のセットの版 ne-reg-set-v2 (0067 の版 + 🆕 0100 の 2 行) ═══════════
--   v2 = 試し用の門なし (最初から 1 ファイル 1,000 行まで)・見出しと中身 (ops.ne_reg_canonical) は v1 と同じ。v1 = 作らない・配らない (前の版の実機の確かめは引き継がない = 0065 と同じ考え)
create or replace function ops.ne_reg_schema_rule(p_schema text) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case p_schema
    when 'ne-reg-single-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": false, "why": "0065 で引退 (JAN を送る版)。単品は ne-reg-single-v2", "trial_gate": true, "jan": "value", "parent": "ne_code"}'::jsonb
    when 'ne-reg-single-v2' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "ne_code"}'::jsonb
    when 'ne-reg-variation-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "group"}'::jsonb
    when 'ne-reg-set-v1' then '{"kind": "sets", "sku_kind": "set", "header": "set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo",
      "buildable": false, "why": "0100 で引退 (試し用の 5 行の門のある版)。セットは ne-reg-set-v2", "trial_gate": true, "jan": null, "parent": null}'::jsonb
    when 'ne-reg-set-v2' then '{"kind": "sets", "sku_kind": "set", "header": "set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo",
      "buildable": true, "why": null, "trial_gate": false, "jan": null, "parent": null}'::jsonb
  end
$$;
