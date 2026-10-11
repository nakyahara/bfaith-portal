-- 0070: 業務で使える商品 (available) を「NE から消えた」でやめる (cancelled) (2026-10-11。中原さんの決定 10/11 Q1 = a)
-- 🚨 前提 = 0068 (代表の数えと門・ops.record_parent_gate)・0067 (約束の操作の CHECK の今の版)・0063 (ops.transition_sku_registration の今の版)。
--    0069 (#1679・まとめての登録の要求のハッシュ) とは別の表・関数だけを触る (番号はマージの順で振り直す・中身は番号に依らない)
--
-- なぜ: 0068 の drift-list (代表のずれ) の 23 件のうち 18 件 = NE ではもう消えた商品 (多くは別のコードで作り直し) なのに、Company DB では登録の状態が
--   available のまま (parent_incomparable・not_in_ne)。登録の状態は available から先に進めない (0052 の行き先の決まり・cancelled に行けるのは
--   draft / ne_pending / quarantined だけ) = 0068 の判定表で数えない側 (cancelled) にできない = 代表の数えが 0 にならない。
-- なにを (中原さんの決定 = DB が自分で「完全な NE の取得に N 回続けて無い」を確かめてから、人が理由を付けて動かす):
--   1. ops.master_ne_absence = 完全で信用できる NE の取得 (照合 ② の ops.record_parent_gate が記録した回) に「無い」Company DB の SKU のコードと、続けて無い回数。
--      ops.record_parent_gate (0068 の版から写して、記録の後に 1 行足しただけ) が同じ取引で書く (ops._record_ne_absence)。
--      🚨 証拠に 0068 の記録の回を使う理由: その回は「完全な取得」(complete = true・許可の一覧 trust の 4 つが全部 ok = 取得の件数・行の落ちなし・
--         区分のゲート・代表の台帳) と DB が確かめた回だけ・取得の完了は記録と同じ JST の日だけ・同じ回は 1 回 = 「無い」と言ってよい回。
--         観測の rows は NE の取得の全部の単品・セットのコード・untrusted (正規化の衝突・取込の整合で保持) は「無いと言えない」= 無いに数えない。
--         0041 の ops.master_ne_codes は毎回入れ替える最新の 1 回分だけ (回ごとの記録が無い)・record_ne_codes は取得の信用を見ない
--         = 「続けて N 回」の証拠にはしない。今そこにあれば断る (下の 2. の in_ne_codes) = 安全側の守りだけに使う。
--         0058 の ops.master_ne_code_history は「一度でも見た」= 消えた商品も載っている = 使えない
--      続けて = 記録の回 (result_id) の続き。記録しなかった回 (確かめられない取得) は「無い」の証拠にも「ある」の証拠にもしない (数えない・途切れさせない)。
--      同じ NE の取得 (ne_generation_id) を照合し直した回は数を増やさない (= 数えるのは別々の完全な取得)
--   2. ops.sku_gone_from_ne_check(SKU) = やめてよいかを DB が自分の今の値で確かめる (読むだけ)。全部を満たすときだけ ok:
--        登録の状態 available・単品 (セット・例外は外す)・一番新しい完全な取得の記録が ops.ne_gone_fresh_hours() 時間の中・その回にも無い・
--        続けて無い回数 >= ops.ne_gone_required_runs() (= 3)・今の NE のコードの台帳 (0041) にも無い・生きた NE 登録の CSV の品目 (built / issued /
--        import_declared / partial) が無い・ほかのセットの構成品 (core.sku_components) でない・開いている構成の依頼に入っていない・
--        販路の構成 (core.listing_components・削除した出品を除く = Amazon の対応ほか) に使われていない・その商品がほかの商品の親 (代表) でない
--      ops.sku_gone_from_ne_list(コード | null) = CLI の dry-run 用 (null = 候補 = available で、一番新しい完全な取得に無い SKU)
--   3. 登録の状態の行き先に available → cancelled を 1 つ足す (ops.registration_transition_allowed = 0052 の版から写して 1 行足しただけ)。
--      ops.transition_sku_registration (0063 の版から写して足しただけ): available → cancelled は「NE から消えた」の約束の中だけ
--      (約束の操作 sku_gone_cancel・同じ SKU・人・request_id)・2. の確かめが ok のときだけ・根拠 (evidence) は DB が作る (呼び手の根拠は受けない)。
--      = 運用のロール (master_ops) が transition_sku_registration を直に呼んでも available はやめられない (約束は下の関数だけが書く)
--   4. ops.cancel_sku_gone_from_ne(request_id, 人, 理由, SKU) = 動かす関数 1 つ (security definer)。理由が要る・同じ request_id の押し直しは前の答え。
--      鍵: 新商品の許可 (共有・種類の順 = 照合 ② の記録の排他と並ぶ) → 段階 → マスタの書き込み (共有・夜間ロードと並ぶ) → SKU → NE のコード (共有) → SKU の行。
--      段階 new_open・持ち主表 (DB の active) が段階の記録と同じ (0053 の ops.reg_write_gate) → 2. の確かめ → 約束 (0051 の表・操作 sku_gone_cancel) →
--      登録の状態 (3.) → 保存の記録 done (0053 の ops.close_reg_write)
--   5. 約束の操作 (ops.master_write_sessions・ops.master_edit_requests の CHECK) に sku_gone_cancel (0067 の版から写して 1 つ足しただけ)
-- 🚨 0068 の数え方 (判定表) は変えない = cancelled は今までどおり数えない側
-- 🚨 この migration は商品・登録の状態の値を何も変えない (1. の表は次の照合 ② の記録から貯まる = 当てた後の 3 回の照合までは、どの SKU もやめられない)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit (画面) と master_ops (CLI) に 4. と 2. の実行を足す
-- 🚨 夜間ロード (apps/company-db/load/engine.mjs) は登録の状態を「行の無い SKU を quarantined にする」(ops.quarantine_unregistered_skus) だけ = cancelled を available に戻さない

-- ═══════════ 1. 完全な取得に無いコードと続けて無い回数 ═══════════
/** 続けて何回の完全な取得に無ければやめてよいか (中原さんの案 = 3 回) */
create function ops.ne_gone_required_runs() returns integer language sql immutable set search_path = pg_catalog, pg_temp as $$ select 3 $$;
revoke all on function ops.ne_gone_required_runs() from public;
/** 一番新しい完全な取得の記録の古さの上限 (時間)。毎朝の照合 ② が 1 回止まった日の夕方まで = それより古ければ「今も無い」と言わない */
create function ops.ne_gone_fresh_hours() returns integer language sql immutable set search_path = pg_catalog, pg_temp as $$ select 36 $$;
revoke all on function ops.ne_gone_fresh_hours() from public;

create table ops.master_ne_absence (
  company_id             smallint not null default 1 references core.companies,
  code_norm              text not null check (pg_catalog.length(code_norm) between 1 and 200),
  first_absent_result_id bigint not null references ops.master_parent_gate_results (result_id),   -- 続けて無いの最初の回
  last_absent_result_id  bigint not null references ops.master_parent_gate_results (result_id),   -- 続けて無いの最後の回 (= 一番新しい記録の回のときだけ「今も無い」)
  streak                 integer not null check (streak >= 1),                                     -- 続けて無い回数 (記録の回の続き)
  updated_at             timestamptz not null default pg_catalog.clock_timestamp(),
  primary key (company_id, code_norm),
  constraint ck_mna_order check (last_absent_result_id >= first_absent_result_id)
);
comment on table ops.master_ne_absence is '完全で信用できる NE の取得 (照合 ② の ops.record_parent_gate の記録の回) に無い Company DB の SKU のコードと続けて無い回数 (0070)。書くのは ops.record_parent_gate (の中の ops._record_ne_absence) だけ';

create function ops.guard_master_ne_absence() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.ne_absence_protocol', true), '') is distinct from '1' then
    raise exception 'master_ne_absence は ops.record_parent_gate (照合 ② の記録) の中でだけ書く' using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_master_ne_absence() from public;
create trigger trg_master_ne_absence_guard before insert or update or delete on ops.master_ne_absence
  for each row execute function ops.guard_master_ne_absence();
create trigger trg_master_ne_absence_no_truncate before truncate on ops.master_ne_absence
  for each statement execute function core.reject_mutation();

/**
 * 記録した回 (p_result_id・完全で信用できる取得) の観測 p_ne から、無いコードを数える (ops.record_parent_gate の中だけ・記録の行の後・同じ取引)。
 *   無い = Company DB の SKU (会社 1) のコードで、観測の rows (単品・セット) にも untrusted (無いと言えない) にも無い
 *   前の記録の回 (result_id の 1 つ前) にも無かった = 続けて (+1。前の回と同じ NE の取得 (ne_generation_id) の照合し直し = +0)・そうでなければ 1 から。
 *   この回にある (か SKU が無くなった) コードの行は消す (途切れた)
 * 戻り値 = この回に無いコードの数
 */
create function ops._record_ne_absence(p_result_id bigint, p_ne jsonb) returns integer
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_prev     bigint;
  v_step     integer;
  v_n        integer;
begin
  select pg_catalog.max(r.result_id) into v_prev from ops.master_parent_gate_results r where r.result_id < p_result_id;
  -- 同じ NE の取得 (世代 = ne_generation_id) を照合し直した回は「続けて」を増やさない (数えるのは別々の完全な取得の回数・続きは途切れさせない)
  select case when exists (select 1 from ops.master_parent_gate_results a join ops.master_parent_gate_results b on b.ne_generation_id = a.ne_generation_id
                            where a.result_id = p_result_id and b.result_id = v_prev) then 0 else 1 end into v_step;
  perform pg_catalog.set_config('ops.ne_absence_protocol', '1', true);
  with obs as materialized (
    select (x ->> 0) as code_norm from pg_catalog.jsonb_array_elements(p_ne -> 'rows') x
    union
    select u.v from pg_catalog.jsonb_array_elements_text(coalesce(p_ne -> 'untrusted', '[]'::jsonb)) u(v)),
  gone as (
    select distinct k.code_norm from core.skus k
     where k.company_id = 1 and not exists (select 1 from obs o where o.code_norm = k.code_norm))
  insert into ops.master_ne_absence as t (company_id, code_norm, first_absent_result_id, last_absent_result_id, streak)
    select 1, g.code_norm, p_result_id, p_result_id, 1 from gone g
  on conflict (company_id, code_norm) do update set
    streak = case when t.last_absent_result_id = v_prev then t.streak + v_step else 1 end,
    first_absent_result_id = case when t.last_absent_result_id = v_prev then t.first_absent_result_id else excluded.first_absent_result_id end,
    last_absent_result_id = excluded.last_absent_result_id,
    updated_at = pg_catalog.clock_timestamp();
  get diagnostics v_n = row_count;
  delete from ops.master_ne_absence a where a.company_id = 1 and a.last_absent_result_id <> p_result_id;
  perform pg_catalog.set_config('ops.ne_absence_protocol', '', true);
  return v_n;
end $$;
revoke all on function ops._record_ne_absence(bigint, jsonb) from public;

-- 照合 ② の記録 (0068 の版から写して、記録の行の後に 1 行足しただけ = 🆕 0070 の行)
/**
 * 照合 ② の最後に watch_writer が 1 回 (結果の JSON を書いた後 = 封をした回)。DB が自分で数えて (ops.parent_raw_gate) 1 行を残す。
 *   p_fetch = { generation_id, raw_hash, products_complete_at, setproducts_complete_at } (NE の完全な取得・RFC 3339 の明示の offset)。
 *     取得の完了は記録の時刻と同じ JST の日で、それより前 (古い取得の数を残さない = 0058 の ops.record_new_entry_gate と同じ)
 *   p_material_generation_id = 照合が読んだ材料の世代 (widen の判定がロードの材料の世代と照らす)・p_evidence_sha256 = 照合の結果の JSON の sha256
 *   鍵 = 新商品の許可の排他の鍵 (種類の順) = CSV を作る / 配る取引 (共有) の完了を待ってから書く (書いた瞬間から門はこの行を読む)
 *   同じ照合の回の 2 回目は拒む (run_reused)。戻り値 = { result_id (文字), counts, counted, excluded, samples, owner, gate: ops.parent_gate_state() }
 *   🆕 0070: 記録した回の観測から、完全な取得に無いコードと続けて無い回数 (ops.master_ne_absence) を同じ取引で書く (ops._record_ne_absence)
 */
create or replace function ops.record_parent_gate(p_compare_run_id text, p_fetch jsonb, p_material_generation_id text, p_evidence_sha256 text, p_ne jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  k       text;
  v_pat   timestamptz;
  v_sat   timestamptz;
  v_now   timestamptz;
  v_day   date;
  v       jsonb;
  v_owner text;
  v_id    bigint;
begin
  foreach k in array ops.new_entry_lease_kinds() loop perform pg_catalog.pg_advisory_xact_lock(ops.new_entry_lease_lock_key(k)); end loop;
  if coalesce(p_compare_run_id, '') !~ '^[A-Za-z0-9_.:-]{1,80}$' then raise exception 'invalid_input: 照合の回 (英数字と _.:- の 1〜80 字) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_fetch) is distinct from 'object' or coalesce(p_fetch ->> 'generation_id', '') !~ '^[A-Za-z0-9_.:-]{1,120}$'
     or coalesce(p_fetch ->> 'raw_hash', '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: NE の取得 (generation_id・raw_hash・products_complete_at・setproducts_complete_at) が要る' using errcode = '22023';
  end if;
  v_pat := ops.rfc3339_ts(p_fetch ->> 'products_complete_at');
  v_sat := ops.rfc3339_ts(p_fetch ->> 'setproducts_complete_at');
  if v_pat is null or v_sat is null then raise exception 'invalid_input: 取得の完了の時刻 (RFC 3339・明示の offset) が要る' using errcode = '22023'; end if;
  if coalesce(p_material_generation_id, '') !~ '^[A-Za-z0-9_.:-]{1,200}$' then raise exception 'invalid_input: 材料の世代が要る' using errcode = '22023'; end if;
  if coalesce(p_evidence_sha256, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: 照合の結果の JSON の sha256 (64 桁の 16 進) が要る = 封をした回だけ' using errcode = '22023'; end if;
  v_now := pg_catalog.clock_timestamp();
  v_day := (v_now at time zone 'Asia/Tokyo')::date;
  if v_pat > v_now or v_sat > v_now then raise exception 'invalid_input: 取得の完了の時刻が今より後' using errcode = '22023'; end if;
  if (v_pat at time zone 'Asia/Tokyo')::date <> v_day or (v_sat at time zone 'Asia/Tokyo')::date <> v_day then
    raise exception 'stale_fetch: 取得の完了の日が今日 (JST %) でない = 古い取得の数は残さない', v_day using errcode = 'P0001';
  end if;
  if exists (select 1 from ops.master_parent_gate_results r where r.compare_run_id = p_compare_run_id) then
    raise exception 'run_reused: 照合の回 % の代表の数えはもう残した', p_compare_run_id using errcode = 'P0001';
  end if;
  v := ops.parent_raw_gate(1, p_ne, false);   -- 観測の形はここで確かめる (rep_spellings を含む)
  -- 🆕 #1676 Codex R2 High: 書き方の台帳 (代表) を読めない回は記録しない = 0 件の記録で門・widen を開けない (一番新しい記録は前の回のまま)
  if (p_ne #>> '{rep_spellings,state}') is distinct from 'ok' then
    raise exception 'spellings_unavailable: NE のコードの元の書き方 (代表) を読めない回 (%) の代表の数えは残さない (書き方の衝突を見落とす)', p_ne #>> '{rep_spellings,reason}' using errcode = 'P0001';
  end if;
  -- 🆕 #1676 Codex R3 High 2・R4: 照合の許可の一覧 (取得の件数・取込の整合・区分のゲート・台帳) の全部が明示的に ok の回だけ記録する。
  --   complete = true・trust の鍵 = ちょうど 4 つ・全部 'ok'。どれか 1 つでも違う・知らない鍵 = 断る (既定は閉じる = 一番新しい記録は前の回のまま)
  if (p_ne -> 'complete') is distinct from 'true'::jsonb or pg_catalog.jsonb_typeof(p_ne -> 'trust') is distinct from 'object'
     or (select pg_catalog.array_agg(tk.key order by tk.key) from pg_catalog.jsonb_object_keys(p_ne -> 'trust') as tk(key))
        is distinct from array['fetch_counts', 'integrity', 'kind_gate_integrity', 'rep_spellings']
     or exists (select 1 from pg_catalog.jsonb_each(p_ne -> 'trust') e where e.value is distinct from '"ok"'::jsonb) then
    raise exception 'ne_untrusted: NE の取得を確かめられない回 (%) の代表の数えは残さない (どのコードか特定できない)',
      coalesce(left(p_ne ->> 'incomplete_reason', 100), left((select pg_catalog.string_agg(e.key || '=' || (e.value #>> '{}'), ',' order by e.key)
        from pg_catalog.jsonb_each(case when pg_catalog.jsonb_typeof(p_ne -> 'trust') = 'object' then p_ne -> 'trust' else '{}'::jsonb end) e where e.value is distinct from '"ok"'::jsonb), 100), 'trust_missing')
      using errcode = 'P0001';
  end if;
  v_owner := case when ops.parent_gate_enforced() then 'company' else 'load' end;
  perform pg_catalog.set_config('ops.parent_gate_protocol', '1', true);
  insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
                                              evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by, created_at)
    values (1, p_compare_run_id, p_fetch ->> 'generation_id', p_fetch ->> 'raw_hash', v_pat, v_sat, p_material_generation_id, p_evidence_sha256,
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_ne::text, 'UTF8')), 'hex'), v -> 'counts', (v ->> 'counted')::bigint, v -> 'excluded', v -> 'samples',
            v_owner, session_user::text, v_now)
    returning result_id into v_id;
  perform ops._record_ne_absence(v_id, p_ne);   -- 🆕 0070: 完全な取得に無いコードと続けて無い回数 (同じ取引・この回の観測)
  perform pg_catalog.set_config('ops.parent_gate_protocol', '', true);
  return pg_catalog.jsonb_build_object('result_id', v_id::text, 'compare_run_id', p_compare_run_id, 'counts', v -> 'counts', 'counted', v -> 'counted', 'excluded', v -> 'excluded',
    'samples', v -> 'samples', 'obs', v -> 'obs', 'owner', v_owner, 'gate', ops.parent_gate_state());
end $$;
revoke all on function ops.record_parent_gate(text, jsonb, text, text, jsonb) from public;

-- ═══════════ 2. やめてよいかの確かめ (読むだけ) ═══════════
/**
 * SKU を「NE から消えた」でやめてよいか。何も書かない。戻り値 = {
 *   sku_id (文字), code, ok, problems: [{ reason, text }] (空 = ok), reg_state, required_runs, streak (続けて無い回数・無い = 0), fresh_hours,
 *   latest: { result_id, compare_run_id, created_at } (一番新しい完全な取得の記録), absent_since: { result_id, compare_run_id, products_complete_at } (続けて無いの最初の回),
 *   evidence (ok のときだけ = 登録の状態の記録の根拠) }
 * reason: no_sku / not_company / not_available / not_single / no_trusted_fetch / fetch_old / in_ne_latest / absent_short / in_ne_codes /
 *         live_file / set_component / component_request / listing_component / parent_of_others
 */
create function ops.sku_gone_from_ne_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  a        ops.master_ne_absence;
  g        ops.master_parent_gate_results;
  f        ops.master_parent_gate_results;
  m        ops.master_ne_code_mark;
  v_req    integer := ops.ne_gone_required_runs();
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_p      jsonb := '[]'::jsonb;
  v_streak integer := 0;
  v_txt    text;
  v_ok     boolean;
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if not found then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ok', false,
      'problems', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_sku', 'text', 'Company DB にその SKU が無い')));
  end if;
  if s.company_id <> 1 then v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_company', 'text', '会社 1 の SKU でない')); end if;
  if s.reg is distinct from 'available' then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_available',
      'text', pg_catalog.format('登録の状態が %s (やめられるのは「業務で使える」(available) の商品だけ)', coalesce(s.reg, '行が無い'))));
  end if;
  if s.sku_kind is distinct from 'single' then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_single', 'text', pg_catalog.format('%s はこの操作でやめない (単品だけ)', case s.sku_kind when 'set' then 'セット' else '例外の SKU' end)));
  end if;
  -- NE の完全な取得 (照合 ② の記録の回) に続けて無いか
  select * into g from ops.master_parent_gate_results x order by x.result_id desc limit 1;
  if g.result_id is null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_trusted_fetch', 'text', '完全で信用できる NE の取得の記録 (照合 ② の代表の数え) がまだ無い'));
  else
    if g.created_at < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'fetch_old',
        'text', pg_catalog.format('一番新しい完全な NE の取得の記録 (照合の回 %s) が %s 時間より古い (今朝の照合 ② を待つ)', g.compare_run_id, v_hours)));
    end if;
    select * into a from ops.master_ne_absence x where x.company_id = 1 and x.code_norm = s.code_norm;
    if a.code_norm is null or a.last_absent_result_id <> g.result_id then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_latest',
        'text', pg_catalog.format('一番新しい完全な NE の取得 (照合の回 %s) にある (か、無いと言えない) = NE から消えていない', g.compare_run_id)));
    else
      v_streak := a.streak;
      select * into f from ops.master_parent_gate_results x where x.result_id = a.first_absent_result_id;
      if a.streak < v_req then
        v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'absent_short',
          'text', pg_catalog.format('完全な NE の取得に続けて無いのが %s 回 (%s 回で、やめられる)', a.streak, v_req)));
      end if;
    end if;
  end if;
  -- 今の NE のコードの台帳 (0041・最新の照合の回の入れ替え) にあれば断る (安全側の守りだけ)
  select * into m from ops.master_ne_code_mark x where x.id = 1;
  if exists (select 1 from ops.master_ne_codes c where c.code_norm = s.code_norm) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_codes',
      'text', pg_catalog.format('今の NE のコードの台帳 (照合の回 %s) にある', coalesce(m.compare_run_id, '不明'))));
  end if;
  -- 使われていないか
  select pg_catalog.string_agg('#' || i.export_id::text || ' ' || i.state, '・' order by i.item_id) into v_txt
    from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'live_file', 'text', pg_catalog.format('終わっていない NE 登録の CSV の品目がある (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where c.child_sku_id = s.sku_id order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'set_component', 'text', pg_catalog.format('ほかのセットの構成品になっている (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select distinct p.code from ops.sku_component_requests q join core.skus p on p.sku_id = q.set_sku_id
           where q.status = 'open' and exists (select 1 from pg_catalog.jsonb_array_elements(q.rows) e where (e ->> 'sku_id') = s.sku_id::text) order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'component_request', 'text', pg_catalog.format('開いている構成の依頼に入っている (セット %s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status <> 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component', 'text', pg_catalog.format('販路の出品の構成に使われている (%s)', v_txt)));
  end if;
  if s.product_id is not null and exists (select 1 from core.products c where c.parent_product_id = s.product_id and c.product_id <> s.product_id) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'parent_of_others', 'text', 'この商品がほかの商品の代表 (親) になっている'));
  end if;
  v_ok := pg_catalog.jsonb_array_length(v_p) = 0;
  return pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ok', v_ok, 'problems', v_p, 'reg_state', s.reg, 'sku_kind', s.sku_kind,
    'required_runs', v_req, 'fresh_hours', v_hours, 'streak', v_streak,
    'latest', case when g.result_id is null then null else pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id, 'created_at', g.created_at) end,
    'absent_since', case when f.result_id is null then null else pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id, 'products_complete_at', f.products_complete_at) end,
    'evidence', case when v_ok then pg_catalog.jsonb_build_object('rule', 'gone_from_ne_v1', 'required_runs', v_req, 'streak', v_streak,
        'first_absent', pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id, 'products_complete_at', f.products_complete_at),
        'last_absent', pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id, 'products_complete_at', g.products_complete_at,
          'ne_generation_id', g.ne_generation_id),
        'ne_codes_run', m.compare_run_id) end);
end $$;
revoke all on function ops.sku_gone_from_ne_check(bigint) from public;

/**
 * CLI の dry-run 用 (読むだけ)。p_codes = 確かめるコード (500 件まで・code_norm で探す) / null = 候補 (登録の状態 available で、
 * 一番新しい完全な取得の記録の回に無い SKU)。戻り値 = [ops.sku_gone_from_ne_check の答え (無いコード = { code, ok: false, problems: [no_sku] })] (コードの順)
 */
create function ops.sku_gone_from_ne_list(p_codes text[]) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_out jsonb;
begin
  if p_codes is not null and (pg_catalog.cardinality(p_codes) > 500 or exists (select 1 from pg_catalog.unnest(p_codes) c where c is null or pg_catalog.length(c) not between 1 and 200)) then
    raise exception 'invalid_input: コードは 1〜200 字で 500 件まで' using errcode = '22023';
  end if;
  if p_codes is null then
    select coalesce(pg_catalog.jsonb_agg(ops.sku_gone_from_ne_check(k.sku_id) order by k.code_norm), '[]'::jsonb) into v_out
      from core.skus k join ops.master_registrations r on r.sku_id = k.sku_id
      join ops.master_ne_absence a on a.company_id = 1 and a.code_norm = k.code_norm
     where k.company_id = 1 and r.state = 'available'
       and a.last_absent_result_id = (select pg_catalog.max(x.result_id) from ops.master_parent_gate_results x);
  else
    select coalesce(pg_catalog.jsonb_agg(case when k.sku_id is null
             then pg_catalog.jsonb_build_object('sku_id', null, 'code', c.code, 'ok', false,
                    'problems', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_sku', 'text', 'Company DB にそのコードの SKU が無い')))
             else ops.sku_gone_from_ne_check(k.sku_id) end order by core.norm_code(c.code), c.code), '[]'::jsonb) into v_out
      from (select distinct on (core.norm_code(u.v)) u.v as code from pg_catalog.unnest(p_codes) u(v) order by core.norm_code(u.v), u.v) c
      left join core.skus k on k.company_id = 1 and k.code_norm = core.norm_code(c.code);
  end if;
  return v_out;
end $$;
revoke all on function ops.sku_gone_from_ne_list(text[]) from public;

-- ═══════════ 3. 登録の状態の行き先 (0052 の版から写して 1 行足しただけ) ═══════════
-- 進む向きの地図 (一方向。戻さない)。⑤-2a で開いているのは → cancelled だけ (ほかは根拠の表ができてから = transition_sku_registration の not_ready)
create or replace function ops.registration_transition_allowed(p_from text, p_to text) returns boolean language sql immutable as $$
  select (p_from || '>' || p_to) = any (array[
    'draft>ne_pending', 'draft>cancelled',
    'ne_pending>ne_confirmed', 'ne_pending>cancelled',
    'ne_confirmed>distributable',
    'distributable>available',
    'quarantined>ne_confirmed', 'quarantined>cancelled'])
    or (p_from || '>' || p_to) = 'available>cancelled'   -- 🆕 0070: NE から消えた (ops.transition_sku_registration が「NE から消えた」の約束と DB の確かめを求める)
$$;

-- 状態を 1 つ進める (0063 の版から写して、available → cancelled の守りを足しただけ = 🆕 0070 の行)
/**
 * ne_pending・ne_confirmed の根拠は、関数が 0053 の記録から自分で読んで鍵を取る (呼び手の渡す根拠の JSON は信じない = 渡したら拒む caller_evidence)。
 * distributable / available は ④ まで not_ready のまま。
 *   ne_pending   ← (人) この SKU の新規登録の CSV の品目が import_declared (🆕 0063: か、申告を結んだ partial)・その試み (結果 ok / partial・sha256 = ファイルの記録) がある
 *                ← (system・0063) 下書きから: この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・
 *                  申告なし (attempt_id が null)・回の記録がある = 申告をしなかった品目を照合が確かめた (照合の確かめの中だけが通る道)
 *   ne_confirmed ← ne_pending から: この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・回の記録がある (system)
 *                  quarantined から: NE で見つけた商品の照合の結果の表はまだ無い = not_ready
 *   🆕 0070 cancelled ← available から: 「NE から消えた」の約束 (ops.cancel_sku_gone_from_ne・操作 sku_gone_cancel・同じ SKU・人・request_id) の中だけ・
 *                  ops.sku_gone_from_ne_check が ok・根拠は DB が作る (呼び手の根拠は受けない)
 */
create or replace function ops.transition_sku_registration(p_sku_id bigint, p_to text, p_actor_type text, p_actor_id text,
                                                           p_reason text default null, p_evidence jsonb default '{}'::jsonb, p_request_id text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r        ops.master_registrations%rowtype;
  v_ev     jsonb := coalesce(p_evidence, '{}'::jsonb);
  v_rec    record;
  v_event  bigint;
  v_sess   ops.master_write_sessions;   -- 🆕 0070
  v_chk    jsonb;                       -- 🆕 0070
begin
  if p_actor_type is null or p_actor_type not in ('human', 'system') then raise exception 'invalid_input: actor_type は human か system' using errcode = '22023'; end if;
  if p_actor_id is null or pg_catalog.length(p_actor_id) = 0 then raise exception 'invalid_input: 誰が (actor_id) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(v_ev) <> 'object' then raise exception 'invalid_input: 根拠 (evidence) は object' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  select * into r from ops.master_registrations x where x.sku_id = p_sku_id for update;
  if not found then raise exception 'no_registration: SKU % に状態の行が無い (使えない商品)', p_sku_id using errcode = 'P0001'; end if;
  if not ops.registration_transition_allowed(r.state, p_to) then
    raise exception 'one_way: 状態は % から % に進めない', r.state, p_to using errcode = 'P0001';
  end if;
  if p_to in ('distributable', 'available') then
    raise exception 'not_ready: % への根拠 (%) の表がまだ無い = 進めない (④ で作る)', p_to, case p_to when 'distributable' then '配る世代' else '場所ごとの受け取り' end
      using errcode = 'P0001';
  end if;
  if p_to in ('ne_pending', 'ne_confirmed') then
    -- NE で見つけた商品 (要確認) を NE 確認済みにする根拠 (照合の結果の表) はまだ無い = 開いていない (呼び手の根拠の有無より先に)
    if p_to = 'ne_confirmed' and r.state = 'quarantined' then
      raise exception 'not_ready: NE で見つけた商品を NE 確認済みにする根拠 (照合の結果の表) はまだ無い' using errcode = 'P0001';
    end if;
    if v_ev <> '{}'::jsonb then
      raise exception 'caller_evidence: % の根拠は関数が記録 (新規登録の CSV・照合の確かめ) から読む。呼び手の根拠は受けない', p_to using errcode = '22023';
    end if;
    if p_to = 'ne_pending' and p_actor_type = 'human' then
      select i.item_id, i.export_id, a.attempt_id, a.sha256, a.result, a.declared_by, a.declared_at into v_rec
        from ops.ne_reg_export_items i
        join ops.ne_reg_exports e on e.export_id = i.export_id
        join ops.ne_reg_attempts a on a.attempt_id = i.attempt_id and a.export_id = i.export_id
       where i.sku_id = p_sku_id and i.state in ('import_declared', 'partial') and e.state = 'declared' and a.sha256 = e.sha256 and a.result in ('ok', 'partial')   -- 🆕 0063: 申告を結んだ partial も
       order by i.item_id desc limit 1
       for share of i, e;
      if not found then
        raise exception 'no_evidence: SKU % に、取り込んだと申告した新規登録の CSV の品目が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('export_id', v_rec.export_id, 'item_id', v_rec.item_id, 'attempt_id', v_rec.attempt_id, 'sha256', v_rec.sha256,
                                            'result', v_rec.result, 'declared_by', v_rec.declared_by, 'declared_at', v_rec.declared_at);
    elsif p_to = 'ne_pending' then
      -- 🆕 0063: 申告をしなかった品目を、配った後の NE の完全な取得で照合が確かめた (system・ne_compare だけ)
      if p_actor_id <> 'ne_compare' then raise exception 'no_evidence: 申告なしの NE 登録待ちは翌朝の照合 (ne_compare) の確かめで' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at, e.issued_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
        join ops.ne_reg_exports e on e.export_id = i.export_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id and i.attempt_id is null
         and e.issued_at is not null and c.fetched_at > e.issued_at
         and ops.ne_reg_auto_block(i.item_id) = ''   -- 自動で確かめてよい品目だけ (単品・JAN を送っていない・配った時の許可の印がある)
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、配った後の NE の完全な取得で全部の列が合った確かめ (verified・申告なし) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'issued_at', v_rec.issued_at, 'matched', true, 'declared', false);
    else
      if p_actor_type <> 'system' then raise exception 'no_evidence: NE 確認済みは翌朝の照合 (system) で' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、NE の完全な取得で全部の列が合った確かめ (verified) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'matched', true);
    end if;
  else
    -- cancelled = 人が理由を書いてだけ
    if p_actor_type <> 'human' or coalesce(pg_catalog.length(pg_catalog.btrim(p_reason)), 0) = 0 then raise exception 'no_evidence: やめるのは人が理由を書いてだけ' using errcode = '22023'; end if;
    -- 🆕 0070 ここから: 業務で使える (available) → やめた = 「NE から消えた」の約束の中だけ・DB の確かめが ok・根拠は DB が作る
    if r.state = 'available' then
      v_sess := ops.current_master_write_session();
      if v_sess.session_id is null or v_sess.operation is distinct from 'sku_gone_cancel' or v_sess.sku_id is distinct from p_sku_id
         or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id then
        raise exception 'gone_session_required: 業務で使える商品をやめるのは「NE から消えた」の操作 (ops.cancel_sku_gone_from_ne) の中だけ' using errcode = '42501';
      end if;
      if v_ev <> '{}'::jsonb then
        raise exception 'caller_evidence: NE から消えた根拠は DB が完全な NE の取得の記録から作る。呼び手の根拠は受けない' using errcode = '22023';
      end if;
      v_chk := ops.sku_gone_from_ne_check(p_sku_id);
      if (v_chk ->> 'ok') is distinct from 'true' then
        raise exception 'not_gone_from_ne: %', (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x) using errcode = 'P0001';
      end if;
      v_ev := v_chk -> 'evidence';
    end if;
    -- 🆕 0070 ここまで
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
revoke all on function ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text) from public;

-- ═══════════ 5. 約束の操作 (0067 の CHECK の版から写して sku_gone_cancel を 1 つ足しただけ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel'));

-- ═══════════ 4. 動かす関数 ═══════════
/**
 * 業務で使える商品 (available) を「NE から消えた」でやめる (cancelled)。人が理由 (200 字まで・改行なし) を書いてだけ・1 件ずつ・1 つの取引。
 *   同じ request_id の押し直し = 前の答え (replayed)。違う操作・人・SKU・理由で同じ request_id = request_id_reused
 *   鍵: 新商品の許可 (共有・種類の順) → 段階 (共有) → マスタの書き込み (共有) → SKU → NE のコード (共有) → SKU の行 (for update = 構成・出品の構成の外部キーの insert と並ぶ)
 *   段階 new_open・持ち主表 (DB の active) が段階の記録と同じ (ops.reg_write_gate)。確かめ (ops.sku_gone_from_ne_check) が ok でなければ not_gone_from_ne (理由を全部)
 *   約束 (0051 の ops.master_write_sessions・操作 sku_gone_cancel・この関数だけが書く) → 登録の状態 (ops.transition_sku_registration が約束と確かめをもう一度見る) →
 *   保存の記録 done (ops.close_reg_write)。戻り値 = { ok, request_id, code, sku_id, from, state, event_id, evidence }
 * 呼べるロール = master_edit (画面・名簿 MASTER_DECISION_APPROVERS の人) と master_ops (CLI scripts/company-db/master-gone-from-ne.mjs)。
 *   🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)。DB が残すのは db_user と約束の中身
 */
create function ops.cancel_sku_gone_from_ne(p_request_id uuid, p_actor_id text, p_reason text, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  v_code    text;
  v_chk     jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_tr      jsonb;
  v_result  jsonb;
  k         text;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: やめるのは人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  foreach k in array ops.new_entry_lease_kinds() loop perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key(k)); end loop;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'sku_gone_cancel', 'sku_id', p_sku_id::text, 'reason', v_reason));
  select x.operation, x.actor_id, x.status, x.result, x.payload_hash, x.sku_id into v_prev from ops.master_edit_requests x where x.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'sku_gone_cancel' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done'
       or v_prev.payload_hash is distinct from v_hash or v_prev.sku_id is distinct from p_sku_id then
      raise exception 'request_id_reused: request_id % はほかの操作・人・商品・理由で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  select x.code into v_code from core.skus x where x.sku_id = p_sku_id and x.company_id = 1 for update;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  v_chk := ops.sku_gone_from_ne_check(p_sku_id);
  if (v_chk ->> 'ok') is distinct from 'true' then
    raise exception 'not_gone_from_ne: % はやめられない (%)', v_code, (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x)
      using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'sku_gone_cancel', p_sku_id, '{}', '{}', pg_catalog.repeat('0', 64),
            v_hash, pg_catalog.jsonb_build_object('sku_id', p_sku_id::text), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  v_tr := ops.transition_sku_registration(p_sku_id, 'cancelled', 'human', p_actor_id, v_reason, '{}'::jsonb, p_request_id::text);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', v_code, 'sku_id', p_sku_id::text, 'from', 'available', 'state', 'cancelled',
    'event_id', v_tr ->> 'event_id', 'evidence', v_chk -> 'evidence');
  perform ops.close_reg_write(v_result, v_code);
  return v_result;
end $$;
revoke all on function ops.cancel_sku_gone_from_ne(uuid, text, text, bigint) from public;

-- ─── 権限 (0068 と同じ形)。master_edit・master_ops には scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表は読ませない・関数の実行だけ) ───
--   watcher = 表 ops.master_ne_absence を読むだけ (ops の全部の表の select = create-watch-roles.mjs の既定の権限で付く)・確かめ (読むだけ) の実行
--   だれにも渡さない = ops._record_ne_absence・ops.guard_master_ne_absence (ops.record_parent_gate の中から持ち主の権限で呼ぶ)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_ne_absence to watcher';
    execute 'grant execute on function ops.sku_gone_from_ne_check(bigint) to watcher';
    execute 'grant execute on function ops.sku_gone_from_ne_list(text[]) to watcher';
  end if;
end $$;
