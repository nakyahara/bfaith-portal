-- 0082 新商品の許可の切れ目を 0 にする (2026-10-14・中原さん「登録できない時間とか作るな」)
--
-- なぜ: 0058 の許可 (ops.master_new_entry_leases) は毎朝 07:00 (JST) に期限が切れ、照合 ② の始め (ops.close_new_entry_for_compare) でも閉じていた。
--   新しい許可は照合 ② の後の段 (new-entry-gate.mjs・~09:00) が出す = 毎朝 07:00〜09:00 ごろ新商品 (単品・まとまり) の登録・NE 登録の CSV が止まっていた。
-- なにを (予定で閉じる時間を 0 にする。問題が出たときに閉じるのは今までどおり):
--   1. 期限 = 出したときから 72 時間 (ops.new_entry_lease_expiry)。毎朝の確かめが 3 日続けて流れないときだけ切れる「見張りの安全弁」(予定の切れ目にしない)
--   2. 置き換え: 次の許可を出したら前の許可は置き換わる (一番新しい許可だけが効く = 今までの作り)。前の行には取り消しの印 (理由 = 置き換え) を付ける
--   3. 許可の後に照合 ② の結果の行が増えても、その行が全部問題なし (kind_gate の 5 つ・最終形の 2 つが 0) なら許可はそのまま
--      (今までは新しい結果の行が書かれた瞬間に前の許可が無効 = 結果 → 次の段の grant の間も閉じていた)。
--      問題のある結果の行 (区分の差など) が書かれた瞬間に許可は無効 (今までどおり・次の段の grant も拒む → revoke = 停止の床)
--   4. 照合 ② の始めに閉じない: ops.close_new_entry_for_compare は何もしない (古いアプリの互換で残す)・結果の記録は「閉じた床」を求めない
--      (close_floor_id は null)。照合は repeatable read の 1 つの snapshot で読む = 照合の途中に作られた新商品はこの回の数に入らず、
--      次の朝の照合が確かめる (許可を出した後に作られた新商品と同じ扱い)。結果の行の最終形の 2 つの数は記録のとき DB が今の表で数える (途中の商品も入る)
--   5. 配る (ops.ne_reg_issue) の NE のコードの回 = 許可の回、または許可の回より新しい照合の回 (照合の途中で NE のコードの印だけ先に進んだ朝も配れる)
-- 🚨 変えないもの: 取り消し (ops.revoke_new_entry_lease = 停止の床)・復元の停止・grant の条件 (今回の回・全部 0・今日・widen の後・停止の床より新しい)・
--    配ったファイルは配った時の許可が一番新しく有効な間だけ (ops.ne_reg_file)。新しい関数は作らない (create or replace だけ = 実行権はそのまま = ロールの流し直しは要らない)
-- 🚨 全部の関数: search_path = pg_catalog, pg_temp・完全修飾・一時の表を使わない

-- ─── 1. 期限 = 出したときから 72 時間 ───
create or replace function ops.new_entry_lease_expiry(p_now timestamptz) returns timestamptz language sql immutable set search_path = pg_catalog, pg_temp as $$
  select p_now + interval '72 hours'
$$;
comment on function ops.new_entry_lease_expiry(timestamptz) is '新商品の許可の期限 (0082) = 出したときから 72 時間。毎朝の確かめが 3 日続けて流れないときだけ切れる見張りの安全弁 (予定の切れ目ではない)。毎朝の次の許可が前の許可を置き換える';
comment on table ops.master_new_entry_leases is '新商品の入口の開放の許可 (0058・0082)。出すのは ops.grant_new_entry_lease だけ (ログイン new_entry_gate)。一番新しい許可だけが効く (次の許可が前を置き換える)・期限 = 出したときから 72 時間 (安全弁)。許可の後の照合 ② の結果に問題があれば無効。新商品を作る DB の関数が ops._require_new_entry_lease で確かめる';

-- ─── 2. 結果の行は「照合の始めに閉じた床」を持たなくてよい (0082 の後の行 = null) ───
alter table ops.new_entry_gate_results alter column close_floor_id drop not null;
comment on column ops.new_entry_gate_results.close_floor_id is '0058〜0081: 同じ回の照合 ② の始めに閉じた停止の床 (single)。0082 から照合の始めに閉じない = null';

-- ─── 3. 結果の記録 (watch_writer)。照合の始めに閉じたことを求めない ───
--   最初に許可の排他の鍵を種類の順に取る = 保存の取引の完了を待ってから書く (問題のある結果を書いた瞬間に許可は無効 = _new_entry_lease_ok)。
--   同じ compare_run_id の 2 回目は拒む (unique)。戻り値 = { result_id (文字), compare_run_id, shape }
create or replace function ops.record_new_entry_gate(p_compare_run_id text, p_products_complete_at text, p_setproducts_complete_at text, p_kind_gate jsonb) returns jsonb
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
  -- 🆕 0082: 照合の始めに閉じたこと (0058 の not_closed) は求めない。照合は 1 つの snapshot で読む = 途中に作られた新商品は次の朝の照合が確かめる
  v_shape := ops.sku_kind_shape_counts(1);
  perform pg_catalog.set_config('ops.gate_result_protocol', '1', true);
  insert into ops.new_entry_gate_results (compare_run_id, products_complete_at, setproducts_complete_at, kind_gate, single_product_mismatch, non_set_parent_components, close_floor_id, recorded_by, created_at)
    values (p_compare_run_id, v_pat, v_sat,
            (select pg_catalog.jsonb_object_agg(e.key, ops.jsonb_nonneg_bigint(e.value)) from pg_catalog.jsonb_each(p_kind_gate) e),
            (v_shape ->> 'single_product_mismatch')::bigint, (v_shape ->> 'non_set_parent_components')::bigint, null, session_user::text, v_now)
    returning result_id into v_id;
  perform pg_catalog.set_config('ops.gate_result_protocol', '', true);
  return pg_catalog.jsonb_build_object('result_id', v_id::text, 'compare_run_id', p_compare_run_id, 'shape', v_shape);
end $$;

-- ─── 4. 結果の行で許可を出せるか (grant が呼ぶ)。0058 の not_closed (照合の始めに閉じた床との結び付き) を外す。ほかは同じ ───
create or replace function ops._new_entry_gate_problems(p_kind text, p_result_id bigint) returns text[]
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

-- ─── 5. 今有効な許可があるか (private) ───
--   その種類の一番新しい許可が: 取り消されていない・期限 (72 時間) の前・許可の後の結果の行が全部問題なし
--   (kind_gate の 5 つ・最終形の 2 つが 0 = 問題のある結果を書いた瞬間に無効)・停止の床より新しい・区分の持ち主が company
create or replace function ops._new_entry_lease_ok(p_kind text) returns boolean
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  l       ops.master_new_entry_leases;
  v_floor bigint;
begin
  select * into l from ops.master_new_entry_leases x where x.kind = p_kind order by x.lease_id desc limit 1;
  if not found or l.revoked_at is not null or pg_catalog.clock_timestamp() >= l.expires_at then return false; end if;
  -- 🆕 0082: 許可の後の結果の行は問題なしなら許可はそのまま (次の段の grant までの間も閉じない)。問題のある行 = すぐ無効
  if exists (select 1 from ops.new_entry_gate_results x
              where x.result_id > l.result_id
                and (x.single_product_mismatch <> 0 or x.non_set_parent_components <> 0
                     or exists (select 1 from pg_catalog.jsonb_each(x.kind_gate) e where (e.value #>> '{}')::bigint <> 0))) then
    return false;
  end if;
  select max(f.floor_result_id) into v_floor from ops.master_new_entry_stop_floors f where f.kind = p_kind;
  if v_floor is not null and l.result_id <= v_floor then return false; end if;
  return ops.sku_kind_locked();
end $$;

-- 新商品を作る DB の関数の中で呼ぶ (private)。言葉だけ直す (切れる理由に「定時」は無い)
create or replace function ops._require_new_entry_lease(p_kind text) returns void
  language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key(coalesce(p_kind, '')));
  if not ops._new_entry_lease_ok(p_kind) then
    raise exception 'new_entry_closed: 新商品 (%) の入口は閉じている (確かめの許可が無い・確かめで問題が出た・取り消し・確かめが 3 日流れていない)', coalesce(p_kind, 'null') using errcode = 'P0001';
  end if;
end $$;

-- ─── 6. 許可を出す (ログイン new_entry_gate)。条件は 0058 と同じ。期限 = 72 時間・前の許可は置き換え (取り消しの印) ───
create or replace function ops.grant_new_entry_lease(p_kind text, p_compare_run_id text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r          ops.new_entry_gate_results;
  v_problems text[];
  v_now      timestamptz;
  v_id       bigint;
  v_expires  timestamptz;
  v_replaced integer;
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
  -- 🆕 0082: 前の許可は置き換わる (効くのは一番新しい許可だけ = 0058 から)。読みやすさのために取り消しの印を付ける
  update ops.master_new_entry_leases set revoked_at = v_now, revoke_reason = pg_catalog.left('置き換え: 次の許可 (lease ' || v_id::text || ') が出た', 500), revoked_by = session_user::text
   where kind = p_kind and revoked_at is null and lease_id <> v_id;
  get diagnostics v_replaced = row_count;
  perform pg_catalog.set_config('ops.lease_protocol', '', true);
  return jsonb_build_object('lease_id', v_id::text, 'kind', p_kind, 'result_id', r.result_id::text, 'compare_run_id', r.compare_run_id, 'expires_at', v_expires, 'replaced', v_replaced);
end $$;

-- ─── 7. 照合 ② の始めに閉じない。関数は古いアプリ (0082 の前の run.mjs) の互換で残す = 何もしない (入力の形だけ確かめる) ───
create or replace function ops.close_new_entry_for_compare(p_compare_run_id text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(p_compare_run_id, '') !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: 照合の回 (英数字と _.:- の 1〜80 字) が要る' using errcode = '22023'; end if;
  return jsonb_build_object('revoked', 0, 'floor_result_id', null, 'compare_run_id', p_compare_run_id, 'noop', true);
end $$;
comment on function ops.close_new_entry_for_compare(text) is '0082 から何もしない (照合 ② の始めに許可を閉じない = 登録できない時間を作らない)。0082 の前のアプリの互換で残す';

-- ─── 8. 配る (0065 の版 + 🆕 0082 の NE のコードの回の確かめ) ───
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
  -- 🆕 0065 (#1664 Codex R1 Medium): 初めて配る (built → issued) のは、今作れる版のファイルだけ (0065 の前に作って配っていない v1 = JAN を送る版を後から配らない)
  if (ops.ne_reg_schema_rule(e.schema_version) -> 'buildable') is distinct from 'true'::jsonb then
    raise exception 'schema_not_buildable: ファイル % の形の版 % はもう配らない (%)。使わないにして作り直す', p_export_id, e.schema_version,
      coalesce(ops.ne_reg_schema_rule(e.schema_version) ->> 'why', '知らない版') using errcode = 'P0001';
  end if;
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
    perform ops._require_new_entry_lease(case when e.kind = 'products' then 'single' else 'set' end);   -- セットはセットの許可 (#1644 Codex R1 Medium 2)
    -- 🆕 0058 (v17 §3.9 R16 M5): 配った時の許可と、その許可を出した朝の照合の回を export に残す (ops.ne_reg_file が「配った時の許可がまだ有効」を見る)。
    --   セットの CSV はセット専用の許可が要る (上の _require_new_entry_lease)。本番の grant は single だけ = セットは閉じたまま (#1644 Codex R1 Medium 2 / R2 Low)
    select l.lease_id, l.compare_run_id into v_lease, v_run from ops.master_new_entry_leases l
     where l.kind = case when e.kind = 'products' then 'single' else 'set' end order by l.lease_id desc limit 1;
    -- 🆕 0058 (v15 §3.9): 配る直前に、その時の NE のコード (最新の照合の回) と履歴の両方に無いことをもう一度確かめる (build の後の新しい取得で見えたコードを配らない)
    perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
    -- 🆕 0058 (R16 M5): 確かめる NE のコードは許可を出した朝の照合の回のもの (ops.master_ne_code_mark.compare_run_id = 許可の compare_run_id)。違えば ne_codes_stale
    -- 🆕 0082: 許可の回より新しい照合の回 (ops.master_compare_runs の順) の印も通す = 照合の途中で印だけ先に進んだ朝 (次の許可の前) も配れる
    --   (新しい回のコード + 消さない履歴 ops.master_ne_code_history = 許可の回より広く確かめる)。許可の回より古い印・回の記録が無い印は今までどおり ne_codes_stale
    if e.kind = 'products' then
      select m.compare_run_id into v_mark from ops.master_ne_code_mark m where m.id = 1;
      if v_mark is distinct from v_run and not exists (
           select 1 from ops.master_compare_runs a, ops.master_compare_runs b
            where a.compare_run_id = v_mark and b.compare_run_id = v_run and (a.observed_at, a.compare_run_id) > (b.observed_at, b.compare_run_id)) then
        raise exception 'ne_codes_stale: NE のコードの回 (%) が許可を出した照合の回 (%) と違う = 配らない (新しい照合とゲートの後に)', coalesce(v_mark, 'なし'), coalesce(v_run, 'なし') using errcode = 'P0001';
      end if;
    end if;
    select pg_catalog.array_agg(k.code order by k.code) into v_gone from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id
     where i.export_id = p_export_id and i.state = 'built'
       and ops.ne_code_seen(k.code_norm);   -- 商品のコードも代表のコードも (登録の時と同じ範囲・#1640 R5)
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
