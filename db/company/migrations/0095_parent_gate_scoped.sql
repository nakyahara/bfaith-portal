-- 0095: 代表 (親) の門を「ずれた色違いのまとまり」だけに絞る (2026-10-19 中原さんの決定 = 作る。AI_reference CompanyDB構想/20 v7 §⑥ の門の直し)
-- 🚨 前提 = 0068 (代表の数えと門)・0070 (ops.record_parent_gate の今の版 = NE から消えた日数の記録の 1 行)。0068 の表・数え (ops.parent_raw_gate) は変えない
--
-- なぜ: 0068 の門は、毎朝の照合 ② の代表のずれが 1 件でもあると、新しい NE 登録の CSV (作る・配る) を全部閉じる。
--   10/19 に NE で直接足した色違いの子 6 件 (diffuserrefill200-2-ra/-st・reeddiffuser-ra/-st・wae10-ra/-st = Company DB では要確認・親なし・
--   NE の代表 = diffuserrefill200-2 / reeddiffuser / wae10) のずれで、関係の無い新商品の CSV まで全部止まった
--   (6 件それぞれで「NE の代表を採用する」を押すまで)。中原さんの決定: ずれたまとまりに入る商品の CSV だけを止める。
-- 門がある理由 (0068・設計 v7 §⑤): 配った後の代表は NE と社内で同じ、が前提。ずれたまとまりに CSV が代表を書くと、まとまりがさらに崩れる・違う代表を NE に書く。
--   = 止めるのは「その CSV が、ずれたまとまりに触る」とき。触らない商品 (代表が別のまとまり・代表なし) の CSV は、ずれたまとまりの NE の値を何も書かない。
-- なにを:
--   1. ops.master_parent_gate_scopes (追記だけ・照合の回ごとに 1 行) = 照合 ② の記録の回に、ずれた単品をまとまりの鍵 (コードの norm) に分けたもの。
--      書くのは ops.record_parent_gate (0070 の版から写して 1 行足しただけ = 🆕 0095 の行) の中の ops._record_parent_gate_scope だけ (同じ取引・同じ観測)
--      鍵 = ずれた単品のコード・その単品の名札 (display_code)・NE の代表・社内の代表 (記録の時)。分けられるずれ = NE と違う・社内に親が無い・当たる親が 2 つ以上・
--      完全な取得に無い (not_in_ne)。分けられないずれ (比べられない = 取得で保持・NE ではセット・代表が読めない・行が落ちた / 2 段 / 循環) = 全部止める (今までどおり)
--   2. 門 (品目の表の trigger ops.guard_ne_reg_parent_gate を作り直す・見る所は今までどおり = 品目の insert (作る) と built → issued (配る) だけ・
--      作り直し (前の品目が failed) は通す)。決めるのは DB (CSV を作る / 配る取引の中・今までと同じ鍵の下):
--      全部止める = 記録が無い・今朝の許可の回と違う・今日でない・記録の数えが読めない・まとまりに分けていない記録 (0095 の前)・分けた数が数えと合わない・分けられないずれがある
--      その商品だけ止める = この商品の鍵 (CSV に書く代表・社内の代表・名札・コード・セットなら構成品の代表・名札・コード) が、まだ直っていないずれの鍵に当たる
--      直った = 記録の後に社内を NE に合わせた (「NE の代表を採用する」など): 社内の代表の norm = 記録した NE の代表・2 段でない・NE の代表に当たる商品が 1 つ
--        (翌朝の数えと同じ決まり = NE と違う・社内に親が無いだけ。NE を直したときは翌朝の照合で消える)
--   3. ops.parent_gate_state() に scope (全部止めるか・止めているまとまり) を足す (朝の要約・drift-list)
--   4. ops.parent_gate_csv_check(SKU の配列) = 画面 (master_edit) が CSV の候補ごとに止まるか・なぜかを読む (読むだけ)。止めるのは 2. の trigger (画面を信じない)
-- 🚨 security definer の関数は search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表なし・public の実行権なし
-- 🚨 商品・登録・ファイルの値は何も変えない。0095 の前の記録 (まとまりに分けていない) でずれがある朝は今までどおり全部止める = 当てた後の最初の照合から絞る
-- 鍵: 門は読むだけ (鍵を足さない)。記録は 0068 と同じ = 新商品の許可の排他の鍵 (作る / 配る取引は共有で持つ = 記録と門の判断は重ならない)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops.record_parent_gate(text, jsonb, text, text, jsonb)') is null or pg_catalog.to_regprocedure('ops._record_ne_absence(bigint, jsonb)') is null
     or pg_catalog.to_regprocedure('ops.parent_raw_gate(integer, jsonb, boolean)') is null then
    raise exception '0095 の前提: 代表の数えと門 (0068)・NE から消えた日数 (0070) が無い';
  end if;
  if pg_catalog.to_regclass('ops.master_parent_gate_scopes') is not null then raise exception '0095: ops.master_parent_gate_scopes がもうある'; end if;
end $$;

-- ═══════════ 1. 表 (追記だけ・照合の回ごとに 1 行・書くのは ops._record_parent_gate_scope だけ) ═══════════
create table ops.master_parent_gate_scopes (
  result_id    bigint primary key references ops.master_parent_gate_results (result_id),
  scope        jsonb not null check (jsonb_typeof(scope) = 'object' and scope ->> 'format' = 'parent-scope-v1'
                                     and jsonb_typeof(scope -> 'items') = 'array' and jsonb_typeof(scope #> '{global,count}') = 'number'),
  item_count   integer generated always as (jsonb_array_length(scope -> 'items')) stored,      -- 分けたずれの数
  global_count bigint generated always as ((scope #>> '{global,count}')::bigint) stored,      -- 分けられないずれの数 (> 0 = 全部止める)
  created_at   timestamptz not null default clock_timestamp()
);
comment on table ops.master_parent_gate_scopes is '照合 ② の代表のずれを色違いのまとまり (鍵) に分けたもの (0095)。書くのは ops.record_parent_gate の中の ops._record_parent_gate_scope だけ。門 (ops.guard_ne_reg_parent_gate) が一番新しい記録の行を読む';
-- 鍵の表 (分けたずれ 1 つ × その鍵 1 つで 1 行)。門は品目の行ごとに、この商品の鍵で索引を引いて当たるずれだけを読む (ずれの数に比例しない)
create table ops.master_parent_gate_scope_keys (
  result_id  bigint not null references ops.master_parent_gate_scopes (result_id),
  key        text not null check (length(key) between 1 and 200),
  code_norm  text not null check (length(code_norm) between 1 and 200),
  klass      text not null,   -- ずれの数え (直ったかを jsonb を開かずに見る)
  rep_norm   text,            -- NE の代表の norm (なし = null)
  primary key (result_id, key, code_norm)
);
comment on table ops.master_parent_gate_scope_keys is '照合 ② の代表のずれ (分けたもの) の鍵 (0095)。書くのは ops._record_parent_gate_scope だけ。門が (記録の回・鍵) で引く';
-- 名札 (display_code) の norm の索引 (#Codex R3: 門が「今の社内で、この商品の鍵と同じ名札の商品の下にいるずれ」を引く・直ったかの「NE の代表に当たる商品の数」を数える)。
--   core.norm_code は immutable。名札の norm で探す今の関数 (新しいコードの確かめ code_is_rep ほか) も速くなる (結果は変わらない)
create index ix_products_display_norm on core.products (core.norm_code(display_code)) where display_code is not null;

create function ops.guard_master_parent_gate_scopes() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.parent_gate_protocol', true), '') is distinct from '1' then
    raise exception '% は ops.record_parent_gate でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_parent_gate_scopes() from public;
create trigger trg_master_parent_gate_scopes_guard before insert on ops.master_parent_gate_scopes for each row execute function ops.guard_master_parent_gate_scopes();
create trigger trg_master_parent_gate_scope_keys_guard before insert on ops.master_parent_gate_scope_keys for each row execute function ops.guard_master_parent_gate_scopes();
select core.make_append_only('ops', 'master_parent_gate_scopes');
select core.make_append_only('ops', 'master_parent_gate_scope_keys');

-- ═══════════ 2. 記録の時: ずれをまとまりの鍵に分ける (何も書かない) ═══════════
/**
 * p_ne = 照合 ② の観測 (ops.parent_raw_gate と同じ)。数えと分け方は ops.parent_raw_gate の一覧 (items) そのもの (分け方を 2 か所に持たない)。
 * 戻り値 { format: 'parent-scope-v1',
 *   items: [{ code, code_norm, class, rep_norm (NE の代表の norm・なし = null), group (まとめの鍵), fix ('adopt' = 要確認で親なし = 「NE の代表を採用する」で直る / 'fix'),
 *             keys: [鍵の norm], parents: [止めるまとまりの norm (NE の代表・社内の代表・NE の代表のコードを持つ商品の名札)], names: { 鍵の norm: 書き方 } }],   -- 分けられるずれ
 *   global: { count, codes: [20 件まで] } }                          -- 分けられないずれ (全部止める)
 * 分けられるずれが 5,000 件を超える = 全部を分けられないに倒す (表を大きくしない)
 */
create function ops._parent_gate_scope_of(p_ne jsonb) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v     jsonb;
  v_out jsonb;
  v_n   bigint;
begin
  v := ops.parent_raw_gate(1, p_ne, true);
  with it as (
    select x ->> 'code' as code, core.norm_code(x ->> 'code') as code_norm, x ->> 'class' as klass, x ->> 'reason' as reason,
           x ->> 'ne_parent' as ne_raw, x ->> 'cdb_parent' as cdb_parent, x ->> 'reg_state' as reg
      from pg_catalog.jsonb_array_elements(v -> 'items') x),
  obs as (select distinct on (o ->> 0) o ->> 0 as code_norm, o ->> 3 as rep_norm
            from pg_catalog.jsonb_array_elements(p_ne -> 'rows') o where (o ->> 1) = 'single' order by o ->> 0, o ->> 3),
  sc as (
    select it.*, ob.rep_norm,
           (it.klass in ('parent_mismatch', 'parent_missing', 'parent_ambiguous') or (it.klass = 'parent_incomparable' and it.reason = 'not_in_ne')) as scoped,
           p.display_code as own_disp, rp.display_code as rdisp
      from it left join obs ob on ob.code_norm = it.code_norm
      left join core.skus k on k.company_id = 1 and k.code_norm = it.code_norm
      left join core.products p on p.product_id = k.product_id
      left join core.skus rk on rk.company_id = 1 and rk.code_norm = ob.rep_norm
      left join core.products rp on rp.product_id = rk.product_id),
  kv as (   -- 鍵と書き方 (NE の代表・社内の代表・NE の代表のコードを持つ商品の名札・自分の名札・コード)
    select s.code_norm, z.k, z.name, z.pri
      from sc s, lateral (values (s.code_norm, s.code, 5), (nullif(core.norm_code(coalesce(s.own_disp, '')), ''), s.own_disp, 4),
                                 (s.rep_norm, coalesce(s.ne_raw, s.rep_norm), 1), (nullif(core.norm_code(coalesce(s.cdb_parent, '')), ''), s.cdb_parent, 2),
                                 (nullif(core.norm_code(coalesce(s.rdisp, '')), ''), s.rdisp, 3)) as z(k, name, pri)
     where s.scoped and z.k is not null and z.k <> ''),
  per as (
    select s.code, s.code_norm, s.klass, s.rep_norm, s.reg,
           (select pg_catalog.jsonb_agg(distinct v.k) from (values (s.rep_norm), (nullif(core.norm_code(coalesce(s.cdb_parent, '')), '')), (nullif(core.norm_code(coalesce(s.rdisp, '')), ''))) as v(k)
             where v.k is not null) as parents,
           (select x.k from kv x where x.code_norm = s.code_norm order by x.pri limit 1) as grp,
           (select pg_catalog.jsonb_agg(d.k order by d.k) from (select distinct x.k from kv x where x.code_norm = s.code_norm) d) as keys,
           (select pg_catalog.jsonb_object_agg(d.k, d.name) from (select distinct on (x.k) x.k, x.name from kv x where x.code_norm = s.code_norm order by x.k, x.pri) d) as names
      from sc s where s.scoped)
  select pg_catalog.jsonb_build_object('format', 'parent-scope-v1',
    'items', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', p.code, 'code_norm', p.code_norm, 'class', p.klass, 'rep_norm', p.rep_norm, 'group', p.grp,
        'fix', case when p.klass = 'parent_missing' and p.reg = 'quarantined' then 'adopt' else 'fix' end, 'keys', p.keys, 'parents', coalesce(p.parents, '[]'::jsonb), 'names', p.names) order by p.code_norm) from per p), '[]'::jsonb),
    'global', pg_catalog.jsonb_build_object('count', (select pg_catalog.count(*) from sc s where not s.scoped),
      'codes', coalesce((select pg_catalog.jsonb_agg(g.code order by g.code_norm) from (select s.code, s.code_norm from sc s where not s.scoped order by s.code_norm limit 20) g), '[]'::jsonb)))
    into v_out;
  v_n := pg_catalog.jsonb_array_length(v_out -> 'items');
  if v_n > 5000 then
    v_out := pg_catalog.jsonb_build_object('format', 'parent-scope-v1', 'items', '[]'::jsonb, 'too_many', v_n,
      'global', pg_catalog.jsonb_build_object('count', v_n + (v_out #>> '{global,count}')::bigint,
        'codes', (select coalesce(pg_catalog.jsonb_agg(q.e -> 'code'), '[]'::jsonb) from (select x.e from pg_catalog.jsonb_array_elements(v_out -> 'items') as x(e) limit 20) q)));
  end if;
  return v_out;
end $$;
revoke all on function ops._parent_gate_scope_of(jsonb) from public;

-- 記録の回 (p_result_id) の分け方を 1 行残す (ops.record_parent_gate の中だけ = 印 ops.parent_gate_protocol が立っている間)
create function ops._record_parent_gate_scope(p_result_id bigint, p_ne jsonb) returns void
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  s jsonb := ops._parent_gate_scope_of(p_ne);
begin
  insert into ops.master_parent_gate_scopes (result_id, scope) values (p_result_id, s);
  insert into ops.master_parent_gate_scope_keys (result_id, key, code_norm, klass, rep_norm)
    select distinct p_result_id, k.v, e ->> 'code_norm', e ->> 'class', e ->> 'rep_norm'
      from pg_catalog.jsonb_array_elements(s -> 'items') e, pg_catalog.jsonb_array_elements_text(e -> 'keys') as k(v)
     where k.v <> '' and coalesce(e ->> 'code_norm', '') <> '';
end $$;
revoke all on function ops._record_parent_gate_scope(bigint, jsonb) from public;

-- 照合 ② の記録 (0070 の版から写して、記録の行の後に 1 行足しただけ = 🆕 0095 の行)
/**
 * 照合 ② の最後に watch_writer が 1 回 (結果の JSON を書いた後 = 封をした回)。DB が自分で数えて (ops.parent_raw_gate) 1 行を残す。
 *   p_fetch = { generation_id, raw_hash, products_complete_at, setproducts_complete_at } (NE の完全な取得・RFC 3339 の明示の offset)。
 *     取得の完了は記録の時刻と同じ JST の日で、それより前 (古い取得の数を残さない = 0058 の ops.record_new_entry_gate と同じ)
 *   p_material_generation_id = 照合が読んだ材料の世代 (widen の判定がロードの材料の世代と照らす)・p_evidence_sha256 = 照合の結果の JSON の sha256
 *   鍵 = 新商品の許可の排他の鍵 (種類の順) = CSV を作る / 配る取引 (共有) の完了を待ってから書く (書いた瞬間から門はこの行を読む)
 *   同じ照合の回の 2 回目は拒む (run_reused)。戻り値 = { result_id (文字), counts, counted, excluded, samples, owner, gate: ops.parent_gate_state() }
 *   🆕 0070: 記録した回の観測から、完全な取得に無いコードと続けて無い日数 (ops.master_ne_absence) を同じ取引で書く (ops._record_ne_absence)
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
  perform ops._record_ne_absence(v_id, p_ne);   -- 🆕 0070: 完全な取得に無いコードと続けて無い日数 (同じ取引・この回の観測)
  perform ops._record_parent_gate_scope(v_id, p_ne);   -- 🆕 0095: ずれを色違いのまとまり (鍵) に分けて残す (同じ取引・この回の観測 = 門がまとまりだけを止める)
  perform pg_catalog.set_config('ops.parent_gate_protocol', '', true);
  return pg_catalog.jsonb_build_object('result_id', v_id::text, 'compare_run_id', p_compare_run_id, 'counts', v -> 'counts', 'counted', v -> 'counted', 'excluded', v -> 'excluded',
    'samples', v -> 'samples', 'obs', v -> 'obs', 'owner', v_owner, 'gate', ops.parent_gate_state());
end $$;
revoke all on function ops.record_parent_gate(text, jsonb, text, text, jsonb) from public;

-- ═══════════ 3. 門の判断 (読むだけ) ═══════════
/**
 * 全部止める理由 (空 = まとまりだけ止める・止めない)。持ち主によらず数える (止めるかは ops.parent_gate_enforced)。
 *   0068 の ops._parent_gate_problems と同じ 3 つ (記録が無い・今朝の許可の回と違う・今日でない) +
 *   ずれがある記録で: 数えが読めない・まとまりに分けていない (0095 の前の記録)・分けた数が数えと合わない・分けられないずれがある
 */
create function ops._parent_gate_global_problems() returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  r       ops.master_parent_gate_results;
  v_run   text;
  v_out   text[] := '{}';
  v_bad   boolean;
  v_total numeric;
  v_keys  bigint;
  v_has   boolean;
  v_n     bigint;
  v_gn    bigint;
begin
  select * into r from ops.master_parent_gate_results x order by x.result_id desc limit 1;
  if not found then return array['no_parent_gate_result: 照合 ② の代表 (親) の数えの記録がまだ無い']; end if;
  select g.compare_run_id into v_run from ops.new_entry_gate_results g order by g.result_id desc limit 1;
  if v_run is distinct from r.compare_run_id then
    v_out := v_out || format('parent_gate_other_run: 代表の数えの回 (%s) が新商品の許可の照合の回 (%s) と違う (今朝の照合で代表を数えられなかった)', r.compare_run_id, coalesce(v_run, 'なし'));
  end if;
  if (r.created_at at time zone 'Asia/Tokyo')::date <> (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date then
    v_out := v_out || format('parent_gate_not_today: 代表の数えの記録 (%s) が今日 (JST) のものでない', r.compare_run_id);
  end if;
  select pg_catalog.bool_or(ops.jsonb_nonneg_bigint(e.value) is null), pg_catalog.sum(ops.jsonb_nonneg_bigint(e.value)), pg_catalog.count(*)
    into v_bad, v_total, v_keys from pg_catalog.jsonb_each(r.counts) e;
  if coalesce(v_bad, true) or v_keys <> 6 then
    return v_out || format('parent_raw_unreadable: 代表の数えの形が違う (照合の回 %s)', r.compare_run_id);
  end if;
  if v_total > 0 then
    select true, sc.item_count, sc.global_count into v_has, v_n, v_gn from ops.master_parent_gate_scopes sc where sc.result_id = r.result_id;
    if v_has is null then
      v_out := v_out || format('parent_raw_unscoped: 代表のずれ %s 件をまとまりに分けていない記録 (0095 の前・照合の回 %s)', v_total, r.compare_run_id);
    elsif v_n is null or v_gn is null or v_gn < 0 or v_n + v_gn <> v_total then
      v_out := v_out || format('parent_raw_scope_mismatch: まとまりに分けた数 (%s + %s) が代表のずれの数 (%s) と合わない (照合の回 %s)', coalesce(v_n::text, '?'), coalesce(v_gn::text, '?'), v_total, r.compare_run_id);
    elsif v_gn > 0 then
      v_out := v_out || format('parent_raw_global: どのまとまりか分からない代表のずれ %s 件 (%s・照合の回 %s)', v_gn,
        (select pg_catalog.string_agg(c, '・') from (select pg_catalog.jsonb_array_elements_text(sc.scope #> '{global,codes}') as c from ops.master_parent_gate_scopes sc
                                                     where sc.result_id = r.result_id limit 5) q), r.compare_run_id);
    end if;
  end if;
  return v_out;
end $$;
revoke all on function ops._parent_gate_global_problems() from public;

/**
 * 記録したずれ 1 件が、記録の後に社内を直して NE (記録した観測) と同じになったか。翌朝の数え (ops.parent_raw_gate) と同じ決まり:
 *   NE と違う・社内に親が無いだけ (ほかの数えは翌朝の照合まで直ったと見ない)・NE の代表なし = 社内も親なし /
 *   NE の代表 R = 社内の親の名札の norm が R・2 段でない (親が親を持たない・自分が子を持たない)・R に当たる Company DB の商品がちょうど 1 つ (札の display_code・単品のコード)
 */
create function ops._parent_gate_resolved(p_code_norm text, p_class text, p_rep_norm text) returns boolean
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select case when p_class is distinct from 'parent_missing' and p_class is distinct from 'parent_mismatch' then false else coalesce((
    select case
        when p.parent_product_id is null then p_rep_norm is null
        when p_rep_norm is null or p.parent_product_id = p.product_id or pp.parent_product_id is not null then false
        when exists (select 1 from core.products c where c.parent_product_id = p.product_id and c.product_id <> p.product_id) then false
        when coalesce(core.norm_code(pp.display_code), '') is distinct from p_rep_norm then false
        else (select pg_catalog.count(distinct z.pid) from (
                select q.product_id as pid from core.products q where q.company_id = 1 and q.display_code is not null and core.norm_code(q.display_code) = p_rep_norm
                union all
                select s2.product_id from core.skus s2 where s2.company_id = 1 and s2.code_norm = p_rep_norm and s2.product_id is not null) z) = 1 end
      from core.skus k join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
     where k.company_id = 1 and k.code_norm = p_code_norm), false) end
$$;
revoke all on function ops._parent_gate_resolved(text, text, text) from public;

/**
 * 一番新しい記録のずれ (分けたもの) に、今の社内の鍵を足した一覧 (直ったかはまだ見ない = 当たったものだけ後で見る・重い数えを全部にしない)。
 *   ずれの鍵 = 記録の鍵 (コード・名札・NE の代表・社内の代表) + 今の社内 (そのずれた単品の名札・代表の名札 = 記録の後に親を付けた・変えたときも当たる)
 *     + NE の代表のコードを持つ商品の名札 (#Codex R1 High: 代表の商品の名札 (display_code) と SKU のコードが違う = NE の代表 (SKU のコード) と、
 *       そのまとまりの子の社内の代表 (名札) が違う鍵になる = 名札も鍵にする)
 *   gkeys = 止めるまとまり (NE の代表・社内の代表 (記録の時と今)・NE の代表のコードを持つ商品の名札)。無い = group (朝の要約・画面の「止めているまとまり」)
 * p_codes = 読むずれ (code_norm の配列・null = 全部)。門は当たったずれだけを渡す (ずれの数に比例しない)
 * 戻り値 [{ code, code_norm, class, rep_norm, fix, group, keys: [norm], gkeys: [norm], names: { norm: 書き方 } }] (無い = [])
 */
create function ops._parent_gate_items(p_codes text[]) returns jsonb
  language sql stable set search_path = pg_catalog, pg_temp as $$
  with s as (
    select sc.scope from ops.master_parent_gate_scopes sc
     where sc.result_id = (select x.result_id from ops.master_parent_gate_results x order by x.result_id desc limit 1)),
  it as (
    select e as item, o.ord from s, pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(s.scope -> 'items') = 'array' then s.scope -> 'items' else '[]'::jsonb end)
      with ordinality as o(e, ord)
     where p_codes is null or (o.e ->> 'code_norm') = any (p_codes)),
  cur as (
    select it.item, it.ord, p.display_code as own_disp, pp.display_code as pdisp, rp.display_code as rdisp
      from it left join core.skus k on k.company_id = 1 and k.code_norm = (it.item ->> 'code_norm')
      left join core.products p on p.product_id = k.product_id
      left join core.products pp on pp.product_id = p.parent_product_id
      left join core.skus rk on rk.company_id = 1 and rk.code_norm = (it.item ->> 'rep_norm')
      left join core.products rp on rp.product_id = rk.product_id),
  ex as (
    select c.*, case when pg_catalog.jsonb_typeof(c.item -> 'parents') = 'array' then c.item -> 'parents'
                     else pg_catalog.jsonb_build_array(c.item -> 'rep_norm') end as rparents
      from cur c)
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', c.item ->> 'code', 'code_norm', c.item ->> 'code_norm', 'class', c.item ->> 'class',
      'rep_norm', c.item ->> 'rep_norm', 'fix', c.item ->> 'fix', 'group', coalesce(c.item ->> 'group', c.item ->> 'code_norm'),
      'keys', (select coalesce(pg_catalog.jsonb_agg(distinct z.k), '[]'::jsonb) from (
                 select pg_catalog.jsonb_array_elements_text(case when pg_catalog.jsonb_typeof(c.item -> 'keys') = 'array' then c.item -> 'keys' else '[]'::jsonb end) as k
                 union all select core.norm_code(c.own_disp) union all select core.norm_code(c.pdisp) union all select core.norm_code(c.rdisp)) z where z.k is not null and z.k <> ''),
      'gkeys', (select coalesce(pg_catalog.jsonb_agg(distinct z.k), pg_catalog.jsonb_build_array(coalesce(c.item ->> 'group', c.item ->> 'code_norm'))) from (
                 select x.v as k from pg_catalog.jsonb_array_elements_text(coalesce(c.rparents, '[]'::jsonb)) as x(v)
                 union all select core.norm_code(c.pdisp) union all select core.norm_code(c.rdisp)) z where z.k is not null and z.k <> ''),
      'names', (select coalesce(pg_catalog.jsonb_object_agg(z.k, z.name), '{}'::jsonb) from (
                 select distinct on (y.k) y.k, y.name from (
                   select nn.key as k, nn.value as name, 1 as pri from pg_catalog.jsonb_each_text(case when pg_catalog.jsonb_typeof(c.item -> 'names') = 'object' then c.item -> 'names' else '{}'::jsonb end) nn
                   union all select core.norm_code(c.own_disp), c.own_disp, 2 union all select core.norm_code(c.pdisp), c.pdisp, 2
                   union all select core.norm_code(c.rdisp), c.rdisp, 3) y
                  where y.k is not null and y.k <> '' and y.name is not null order by y.k, y.pri) z)) order by c.ord), '[]'::jsonb)
    from ex c
$$;
revoke all on function ops._parent_gate_items(text[]) from public;

/**
 * 1 つの SKU の CSV の鍵 (先に見る順・重なりは先の順を残す): CSV に書く代表 (品目の期待の値 expected.values.parent)・社内の代表の名札・自分の名札・自分のコード・
 *   セットなら構成品 (期待の値の children・今の構成・開いている構成の依頼) の代表の名札・名札・コード
 */
create function ops._parent_gate_sku_keys(p_sku_id bigint, p_expected jsonb) returns text[]
  language sql stable set search_path = pg_catalog, pg_temp as $$
  with kids as (
    select k2.sku_id from core.skus k2
     where k2.company_id = 1 and pg_catalog.jsonb_typeof(p_expected #> '{values,children}') = 'array'
       and k2.code_norm in (select c ->> 'code_norm' from pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(p_expected #> '{values,children}') = 'array'
                                                                                          then p_expected #> '{values,children}' else '[]'::jsonb end) c)
    union select c.child_sku_id from core.sku_components c where c.parent_sku_id = p_sku_id
    union select case when (x ->> 'sku_id') ~ '^[0-9]{1,18}$' then (x ->> 'sku_id')::bigint end from ops.sku_component_requests q, pg_catalog.jsonb_array_elements(q.rows) x
     where q.set_sku_id = p_sku_id and q.status = 'open' and pg_catalog.jsonb_typeof(q.rows) = 'array'),
  raw (k, pri) as (
    select core.norm_code(p_expected #>> '{values,parent}'), 1 where pg_catalog.jsonb_typeof(p_expected #> '{values,parent}') = 'string'
    union all select core.norm_code(pp.display_code), 2 from core.skus k join core.products p on p.product_id = k.product_id join core.products pp on pp.product_id = p.parent_product_id where k.sku_id = p_sku_id
    union all select core.norm_code(p.display_code), 3 from core.skus k join core.products p on p.product_id = k.product_id where k.sku_id = p_sku_id
    union all select k.code_norm, 4 from core.skus k where k.sku_id = p_sku_id
    union all select core.norm_code(pp.display_code), 5 from kids d join core.skus k on k.sku_id = d.sku_id join core.products p on p.product_id = k.product_id join core.products pp on pp.product_id = p.parent_product_id
    union all select core.norm_code(p.display_code), 6 from kids d join core.skus k on k.sku_id = d.sku_id join core.products p on p.product_id = k.product_id
    union all select k.code_norm, 7 from kids d join core.skus k on k.sku_id = d.sku_id)
  select coalesce(pg_catalog.array_agg(d.k order by d.pri, d.k), '{}'::text[])
    from (select distinct on (r.k) r.k, r.pri from raw r where r.k is not null and r.k <> '' order by r.k, r.pri) d
$$;
revoke all on function ops._parent_gate_sku_keys(bigint, jsonb) from public;

/**
 * この商品の鍵 (p_keys) が、まだ直っていないずれ (p_items = ops._parent_gate_items()) に当たるか。当たらない = null。
 *   直ったか (ops._parent_gate_resolved) は当たったずれだけ見る。p_items の resolved (真偽) があればそれを使う (画面の一覧で何度も数えない)
 * 戻り値 { kind: 'group', label (まとまりの書き方 = この商品の鍵の先の順で当たった最初の鍵 (ずれた単品のコードでないもの)。無い = 当たったずれのまとめの鍵),
 *          codes: [直すずれた単品 3 件まで],
 *          fix ('adopt' = 当たった全部が「NE の代表を採用する」で直る / 'fix'), text (画面の 1 行) }
 */
create function ops._parent_gate_hit(p_keys text[], p_items jsonb) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  e       jsonb;
  v_ek    text[];
  v_hit   text[] := '{}';
  v_codes text[] := '{}';
  v_cnorm text[] := '{}';
  v_grp   text;
  v_names jsonb := '{}'::jsonb;
  v_adopt boolean := true;
  v_label text;
  v_k     text;
  v_pre   text;
  v_post  text;
begin
  if coalesce(pg_catalog.cardinality(p_keys), 0) = 0 or pg_catalog.jsonb_typeof(p_items) is distinct from 'array' then return null; end if;
  for e in select x from pg_catalog.jsonb_array_elements(p_items) as t(x) loop
    v_ek := array(select pg_catalog.jsonb_array_elements_text(case when pg_catalog.jsonb_typeof(e -> 'keys') = 'array' then e -> 'keys' else '[]'::jsonb end));
    if not coalesce(v_ek && p_keys, false) then continue; end if;
    if coalesce((e ->> 'resolved')::boolean, ops._parent_gate_resolved(e ->> 'code_norm', e ->> 'class', e ->> 'rep_norm')) then continue; end if;
    v_hit := v_hit || v_ek;
    v_codes := v_codes || (e ->> 'code');
    v_cnorm := v_cnorm || (e ->> 'code_norm');
    v_grp := coalesce(v_grp, e ->> 'group');
    if pg_catalog.jsonb_typeof(e -> 'names') = 'object' then v_names := (e -> 'names') || v_names; end if;
    if (e ->> 'fix') is distinct from 'adopt' then v_adopt := false; end if;
  end loop;
  if pg_catalog.cardinality(v_codes) = 0 then return null; end if;
  foreach v_k in array p_keys loop
    if v_k = any (v_hit) and not (v_k = any (v_cnorm)) then v_label := v_names ->> v_k; v_label := coalesce(v_label, v_k); exit; end if;
  end loop;
  v_label := coalesce(v_label, v_names ->> v_grp, v_grp, v_codes[1]);
  v_codes := v_codes[1:3];
  v_pre := format('色違いのまとまり %s が NE とずれているので、この商品は CSV にできません。', v_label);
  v_post := case when v_adopt then ' などの画面で『NE の代表を採用する』を押すと直ります' else ' などの代表を NE と同じにすると直ります' end;
  -- 画面は pre・codes (商品の画面へのリンク)・post を並べる (文の中を探さない = #Codex R1 Low)。text = 3 つをつないだもの
  return pg_catalog.jsonb_build_object('kind', 'group', 'label', v_label, 'codes', pg_catalog.to_jsonb(v_codes), 'fix', case when v_adopt then 'adopt' else 'fix' end,
    'pre', v_pre, 'post', v_post, 'text', v_pre || pg_catalog.array_to_string(v_codes, '・') || v_post);
end $$;
revoke all on function ops._parent_gate_hit(text[], jsonb) from public;

/**
 * 1 つの SKU の CSV が触る商品 (今の社内): 自分の商品・社内の代表・セットの構成品の商品とその代表。
 *   この下 (parent_product_id) にいるずれは、記録の後に親を付けた・変えたずれでも当たる (ops._parent_gate_codes)
 */
create function ops._parent_gate_sku_pids(p_sku_id bigint, p_expected jsonb) returns bigint[]
  language sql stable set search_path = pg_catalog, pg_temp as $$
  with kids as (
    select k2.sku_id from core.skus k2
     where k2.company_id = 1 and pg_catalog.jsonb_typeof(p_expected #> '{values,children}') = 'array'
       and k2.code_norm in (select c ->> 'code_norm' from pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(p_expected #> '{values,children}') = 'array'
                                                                                          then p_expected #> '{values,children}' else '[]'::jsonb end) c)
    union select c.child_sku_id from core.sku_components c where c.parent_sku_id = p_sku_id
    union select case when (x ->> 'sku_id') ~ '^[0-9]{1,18}$' then (x ->> 'sku_id')::bigint end from ops.sku_component_requests q, pg_catalog.jsonb_array_elements(q.rows) x
     where q.set_sku_id = p_sku_id and q.status = 'open' and pg_catalog.jsonb_typeof(q.rows) = 'array'),
  me as (select p_sku_id as sku_id union select d.sku_id from kids d where d.sku_id is not null)
  select coalesce(pg_catalog.array_agg(distinct z.pid), '{}'::bigint[]) from (
    select p.product_id as pid from me m join core.skus k on k.sku_id = m.sku_id join core.products p on p.product_id = k.product_id
    union all
    select p.parent_product_id from me m join core.skus k on k.sku_id = m.sku_id join core.products p on p.product_id = k.product_id) z
   where z.pid is not null
$$;
revoke all on function ops._parent_gate_sku_pids(bigint, jsonb) from public;

/**
 * 一番新しい記録の分けたずれのうち、この商品に当たりうるもの (code_norm の配列): 鍵の表をこの商品の鍵で引く (索引) +
 *   今の社内でこの商品の代表・自分・構成品の商品の下 (parent_product_id の索引) にいるずれ・この商品の鍵と同じ名札の商品の下 / そのものにいるずれ
 *   (記録の後に親を付けた・変えた・名札が同じ別の商品 = 名札の norm の索引)
 */
create function ops._parent_gate_codes(p_keys text[], p_pids bigint[]) returns text[]
  language sql stable set search_path = pg_catalog, pg_temp as $$
  with r as (select x.result_id from ops.master_parent_gate_results x order by x.result_id desc limit 1)
  select coalesce(pg_catalog.array_agg(distinct z.code_norm), '{}'::text[]) from (
    select sk.code_norm from r join ops.master_parent_gate_scope_keys sk on sk.result_id = r.result_id where sk.key = any (p_keys)
    union all
    select k.code_norm from r, core.products c join core.skus k on k.product_id = c.product_id
     where c.parent_product_id = any (p_pids) and k.company_id = 1
       and exists (select 1 from ops.master_parent_gate_scope_keys sk where sk.result_id = r.result_id and sk.key = k.code_norm and sk.code_norm = k.code_norm)
    union all   -- 今の社内で、この商品の鍵と同じ名札 (norm) の商品の下にいるずれ (別の商品 ID で同じ名札 = #Codex R3 High・名札の norm の索引)
    select k.code_norm from r, core.products pp join core.products c on c.parent_product_id = pp.product_id join core.skus k on k.product_id = c.product_id
     where pp.display_code is not null and core.norm_code(pp.display_code) = any (p_keys) and k.company_id = 1
       and exists (select 1 from ops.master_parent_gate_scope_keys sk where sk.result_id = r.result_id and sk.key = k.code_norm and sk.code_norm = k.code_norm)
    union all   -- 今の社内で、この商品の鍵と同じ名札の商品そのもののずれ (記録の後に名札を変えた)
    select k.code_norm from r, core.products c join core.skus k on k.product_id = c.product_id
     where c.display_code is not null and core.norm_code(c.display_code) = any (p_keys) and k.company_id = 1
       and exists (select 1 from ops.master_parent_gate_scope_keys sk where sk.result_id = r.result_id and sk.key = k.code_norm and sk.code_norm = k.code_norm)) z
$$;
revoke all on function ops._parent_gate_codes(text[], bigint[]) from public;

-- 1 つの SKU の CSV が、まだ直っていないずれたまとまりに触るか (全部止める理由が無いときだけ呼ぶ)。触らない = null。
--   当たりうるずれだけを読む (行ごとの手間はずれの数に比例しない = #Codex R2 Medium)
create function ops._parent_gate_match(p_sku_id bigint, p_expected jsonb) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_keys  text[] := ops._parent_gate_sku_keys(p_sku_id, p_expected);
  v_codes text[];
  v_open  text[];
begin
  v_codes := ops._parent_gate_codes(v_keys, ops._parent_gate_sku_pids(p_sku_id, p_expected));
  if coalesce(pg_catalog.cardinality(v_codes), 0) = 0 then return null; end if;
  -- まだ直っていないずれだけ (鍵の表の数えと NE の代表で見る = jsonb を開かない・#Codex R3 Medium: 直ったまとまりの行ごとに全部を組み直さない)
  select pg_catalog.array_agg(sk.code_norm) into v_open
    from ops.master_parent_gate_scope_keys sk
   where sk.result_id = (select x.result_id from ops.master_parent_gate_results x order by x.result_id desc limit 1)
     and sk.key = sk.code_norm and sk.code_norm = any (v_codes)
     and not ops._parent_gate_resolved(sk.code_norm, sk.klass, sk.rep_norm);
  if coalesce(pg_catalog.cardinality(v_open), 0) = 0 then return null; end if;
  return ops._parent_gate_hit(v_keys, (select pg_catalog.jsonb_agg(e || '{"resolved": false}'::jsonb) from pg_catalog.jsonb_array_elements(ops._parent_gate_items(v_open)) e));
end $$;
revoke all on function ops._parent_gate_match(bigint, jsonb) from public;

/**
 * 全部止めるときの答え (p_problems = ops._parent_gate_global_problems() の空でない答え)。
 *   why = unknown_drift (理由が全部「ずれ」= どのまとまりか分からない) / unchecked (今朝の照合で数えを残せていない ほか)
 * 戻り値 { kind: 'all', why, codes: [直すずれた単品 3 件まで], problems, pre, post, text (画面の 1 行 = pre + codes + post) }
 */
create function ops._parent_gate_all_block(p_problems text[]) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  r       ops.master_parent_gate_results;
  v_drift boolean;
  v_codes text[] := '{}';
  s       jsonb;
  v_pre   text;
  v_post  text;
begin
  v_drift := not exists (select 1 from pg_catalog.unnest(p_problems) x where x !~ '^parent_raw');
  if v_drift then
    select * into r from ops.master_parent_gate_results x order by x.result_id desc limit 1;
    select sc.scope into s from ops.master_parent_gate_scopes sc where sc.result_id = r.result_id;
    if s is not null and pg_catalog.jsonb_typeof(s #> '{global,codes}') = 'array' then
      v_codes := array(select pg_catalog.jsonb_array_elements_text(s #> '{global,codes}') limit 3);
    else
      v_codes := array(select c from (select e.key, pg_catalog.jsonb_array_elements_text(case when pg_catalog.jsonb_typeof(e.value) = 'array' then e.value else '[]'::jsonb end) as c
                                        from pg_catalog.jsonb_each(case when pg_catalog.jsonb_typeof(r.samples) = 'object' then r.samples else '{}'::jsonb end) e) q order by q.key, q.c limit 3);
    end if;
  end if;
  if not v_drift then
    v_pre := '今朝の照合で色違いのまとまりを確かめられなかったので、いまは新しい商品の CSV を作れません。照合が済めば作れます'; v_post := ''; v_codes := '{}';
  elsif pg_catalog.cardinality(v_codes) = 0 then
    v_pre := 'NE とずれた商品の中に、どの色違いのまとまりか分からないものがあるので、いまは新しい商品の CSV を作れません。担当者に知らせてください'; v_post := '';
  else
    v_pre := 'NE とずれた商品の中に、どの色違いのまとまりか分からないものがあるので、いまは新しい商品の CSV を作れません。'; v_post := ' などを直すと作れます';
  end if;
  return pg_catalog.jsonb_build_object('kind', 'all', 'why', case when v_drift then 'unknown_drift' else 'unchecked' end, 'codes', pg_catalog.to_jsonb(v_codes), 'problems', pg_catalog.to_jsonb(p_problems),
    'pre', v_pre, 'post', v_post, 'text', v_pre || pg_catalog.array_to_string(v_codes, '・') || v_post);
end $$;
revoke all on function ops._parent_gate_all_block(text[]) from public;

-- 1 つの品目の判断 (trigger)。止めない = null / 全部止める = kind all / この商品だけ止める = kind group
create function ops._parent_gate_block(p_sku_id bigint, p_expected jsonb) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_g text[] := ops._parent_gate_global_problems();
begin
  if coalesce(pg_catalog.cardinality(v_g), 0) > 0 then return ops._parent_gate_all_block(v_g); end if;
  return ops._parent_gate_match(p_sku_id, p_expected);
end $$;
revoke all on function ops._parent_gate_block(bigint, jsonb) from public;

-- ─── 新しい NE 登録の CSV の門 (0068 の trigger の関数を作り直す。見る所 (作る = insert built・配る = built → issued)・持ち主・作り直しは 0068 と同じ) ───
create or replace function ops.guard_ne_reg_parent_gate() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prev text;
  v_b    jsonb;
begin
  if tg_op = 'INSERT' then
    if new.state is distinct from 'built' then return new; end if;
  elsif not (old.state = 'built' and new.state = 'issued') then
    return new;
  end if;
  if not ops.parent_gate_enforced() then return new; end if;   -- 持ち主が load の間は閉じない (数えて知らせるだけ)
  select i.state into v_prev from ops.ne_reg_export_items i
   where i.sku_id = new.sku_id and i.item_id < new.item_id and i.state <> 'superseded' order by i.item_id desc limit 1;
  if v_prev = 'failed' then return new; end if;   -- 作り直し (取り込めなかった商品だけ)
  -- 🆕 0095: 全部止める (今朝の数えが無い・どのまとまりか分からないずれ) か、この商品の CSV がずれたまとまりに触るときだけ止める
  v_b := ops._parent_gate_block(new.sku_id, new.expected);
  if v_b is null then return new; end if;
  if (v_b ->> 'kind') = 'all' then
    raise exception 'parent_gate_closed: %。配ったファイルの申告・照合・取り込めなかった商品だけの作り直し・廃止はできます (担当者向け: 代表 (親) の数えの門が閉じている = %・ずれの一覧 = drift-list.mjs)',
      v_b ->> 'text', (select pg_catalog.string_agg(x, ' / ') from pg_catalog.jsonb_array_elements_text(v_b -> 'problems') x) using errcode = 'P0001';
  end if;
  raise exception 'parent_gate_group: %', v_b ->> 'text' using errcode = 'P0001';
end $$;
revoke all on function ops.guard_ne_reg_parent_gate() from public;

-- ─── 門の状態 (0068 の版に scope を足した = 🆕 0095 の行) ───
-- 門の状態 (読むだけ・watcher / 照合の要約 / drift-list)。enforced = products.parent の持ち主が company (閉じる) / open = enforced でない か 理由が無い
create or replace function ops.parent_gate_state() returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  r       ops.master_parent_gate_results;
  v_p     text[] := ops._parent_gate_problems();
  v_enf   boolean := ops.parent_gate_enforced();
  v_g     text[] := ops._parent_gate_global_problems();   -- 🆕 0095
begin
  select * into r from ops.master_parent_gate_results x order by x.result_id desc limit 1;
  return pg_catalog.jsonb_build_object('enforced', v_enf, 'open', not v_enf or coalesce(pg_catalog.array_length(v_p, 1), 0) = 0, 'problems', pg_catalog.to_jsonb(v_p),
    'result_id', r.result_id::text, 'compare_run_id', r.compare_run_id, 'counts', r.counts, 'created_at', r.created_at,
    'scope', ops._parent_gate_scope_state(v_g));   -- 🆕 0095
end $$;
revoke all on function ops.parent_gate_state() from public;

/**
 * 門の絞り方 (parent_gate_state の scope)。{ all: 全部止めるか, problems: 全部止める理由, groups: [{ group, label, codes: [5 件まで], count, fix }] (まだ直っていない・20 まで。
 *   まとまり = ずれ 1 つが止めるまとまりの全部 = NE の代表・社内の代表 (記録の時と今)・NE の代表のコードを持つ商品の名札 = NE と違うずれは 2 つのまとまりに出る),
 *   group_count, open_count (まだ直っていないずれ), resolved (記録の後に直った) }。all のときは groups を出さない (全部止めている)
 */
create function ops._parent_gate_scope_state(p_global text[]) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_out jsonb;
begin
  if coalesce(pg_catalog.cardinality(p_global), 0) > 0 then
    return pg_catalog.jsonb_build_object('all', true, 'problems', pg_catalog.to_jsonb(p_global), 'groups', '[]'::jsonb, 'group_count', 0);
  end if;
  with it as (
    select e, e ->> 'code' as code, e ->> 'code_norm' as code_norm, e ->> 'fix' as fix, ops._parent_gate_resolved(e ->> 'code_norm', e ->> 'class', e ->> 'rep_norm') as resolved
      from pg_catalog.jsonb_array_elements(ops._parent_gate_items(null)) e),
  gk as (
    select i.*, k.v as grp from it i, pg_catalog.jsonb_array_elements_text(i.e -> 'gkeys') as k(v) where not i.resolved),
  g as (
    select x.grp, coalesce(pg_catalog.min(x.e -> 'names' ->> x.grp), x.grp) as label, pg_catalog.count(distinct x.code_norm) as n, pg_catalog.bool_and(x.fix = 'adopt') as adopt,
           (pg_catalog.array_agg(distinct x.code order by x.code))[1:5] as codes
      from gk x group by x.grp)
  select pg_catalog.jsonb_build_object('all', false, 'problems', '[]'::jsonb,
    'groups', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('group', q.grp, 'label', q.label, 'codes', pg_catalog.to_jsonb(q.codes), 'count', q.n,
        'fix', case when q.adopt then 'adopt' else 'fix' end) order by q.grp) from (select * from g order by g.grp limit 20) q), '[]'::jsonb),
    'group_count', (select pg_catalog.count(*) from g), 'open_count', (select pg_catalog.count(*) from it where not it.resolved),
    'resolved', (select pg_catalog.count(*) from it where it.resolved))
    into v_out;
  return v_out;
end $$;
revoke all on function ops._parent_gate_scope_state(text[]) from public;

-- ═══════════ 4. 画面の読み取り (master_edit・読むだけ) ═══════════
/**
 * CSV の候補の SKU (5,000 件まで) が門で止まるか。止めるのは trigger (ここは表示だけ = 同じ部品で読む)。
 * 戻り値 { enforced, all: null | { kind: 'all', why, codes, pre, post, text } (全部止める), rebuild: [全部止める朝でも通す作り直しの sku_id (文字)],
 *          items: [{ sku_id (文字), kind: 'group', label, codes, fix, pre, post, text }] (止まる商品だけ) }
 *   持ち主が load = 止めない (enforced false)。作り直し (前の品目が failed) の商品は止めない (trigger と同じ)
 */
create function ops.parent_gate_csv_check(p_sku_ids bigint[]) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_g   text[];
  v_id  bigint;
  v_m   jsonb;
  v_out jsonb := '[]'::jsonb;
  v_all jsonb;
  v_items jsonb;
  v_kmap  jsonb;
  v_cmap  jsonb;
  v_kall  text[];
begin
  if p_sku_ids is null or coalesce(pg_catalog.cardinality(p_sku_ids), 0) > 5000 then raise exception 'invalid_input: SKU の配列 (5,000 件まで)' using errcode = '22023'; end if;
  if not ops.parent_gate_enforced() then return pg_catalog.jsonb_build_object('enforced', false, 'all', null, 'items', '[]'::jsonb); end if;
  v_g := ops._parent_gate_global_problems();
  if coalesce(pg_catalog.cardinality(v_g), 0) > 0 then
    v_all := ops._parent_gate_all_block(v_g);
    return pg_catalog.jsonb_build_object('enforced', true, 'all', v_all - 'problems', 'items', '[]'::jsonb,
      'rebuild', coalesce((select pg_catalog.jsonb_agg(x.id::text order by x.id) from (select distinct u as id from pg_catalog.unnest(p_sku_ids) u where u is not null) x
                            where (select i.state from ops.ne_reg_export_items i where i.sku_id = x.id and i.state <> 'superseded' order by i.item_id desc limit 1) = 'failed'), '[]'::jsonb));
  end if;
  -- 候補ごとに当たりうるずれを引き (索引)、まとめて 1 回だけ読む・直ったかもずれごとに 1 回だけ数える (候補ごとに数えない)
  select coalesce(pg_catalog.jsonb_object_agg(c.id::text, pg_catalog.to_jsonb(c.keys)), '{}'::jsonb),
         coalesce(pg_catalog.jsonb_object_agg(c.id::text, pg_catalog.to_jsonb(c.codes)), '{}'::jsonb)
    into v_kmap, v_cmap
    from (select x.id, ops._parent_gate_sku_keys(x.id, null) as keys,
                 ops._parent_gate_codes(ops._parent_gate_sku_keys(x.id, null), ops._parent_gate_sku_pids(x.id, null)) as codes
            from (select distinct u as id from pg_catalog.unnest(p_sku_ids) u where u is not null) x) c;
  v_kall := array(select distinct t.k from pg_catalog.jsonb_each(v_cmap) m, pg_catalog.jsonb_array_elements_text(m.value) as t(k));
  if pg_catalog.cardinality(v_kall) = 0 then return pg_catalog.jsonb_build_object('enforced', true, 'all', null, 'items', '[]'::jsonb); end if;
  select coalesce(pg_catalog.jsonb_agg(o.e || pg_catalog.jsonb_build_object('resolved', ops._parent_gate_resolved(o.e ->> 'code_norm', o.e ->> 'class', o.e ->> 'rep_norm')) order by o.n), '[]'::jsonb)
    into v_items from pg_catalog.jsonb_array_elements(ops._parent_gate_items(v_kall)) with ordinality as o(e, n);
  for v_id in select m.key::bigint from pg_catalog.jsonb_each(v_cmap) m where pg_catalog.jsonb_array_length(m.value) > 0 order by m.key::bigint loop
    if (select i.state from ops.ne_reg_export_items i where i.sku_id = v_id and i.state <> 'superseded' order by i.item_id desc limit 1) = 'failed' then continue; end if;
    v_m := ops._parent_gate_hit(array(select pg_catalog.jsonb_array_elements_text(v_kmap -> v_id::text)),
      (select coalesce(pg_catalog.jsonb_agg(e), '[]'::jsonb) from pg_catalog.jsonb_array_elements(v_items) e where (e ->> 'code_norm') in (select pg_catalog.jsonb_array_elements_text(v_cmap -> v_id::text))));
    if v_m is not null then v_out := v_out || pg_catalog.jsonb_build_array(v_m || pg_catalog.jsonb_build_object('sku_id', v_id::text)); end if;
  end loop;
  return pg_catalog.jsonb_build_object('enforced', true, 'all', null, 'items', v_out);
end $$;
revoke all on function ops.parent_gate_csv_check(bigint[]) from public;

-- ═══════════ 5. 権限 (migration の中で渡す = ロールの流し直しを待たない。後から作るロールは create-master-edit-roles.mjs が同じ grant を付ける) ═══════════
--   master_edit (画面) = ops.parent_gate_csv_check の実行だけ。表 ops.master_parent_gate_scopes はだれにも渡さない (watcher は「ops の全部の表」の select で読める = 照合の要約の読み直し)。
--   だれにも渡さない = 分け方・判断の部品・trigger の関数 (security definer の中から持ち主の権限で呼ぶ)。
--   ops.parent_gate_state / ops.record_parent_gate の権限は create or replace で今のまま (watcher / watch_writer)
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'master_edit') then
    execute 'grant usage on schema ops to master_edit';
    execute 'grant execute on function ops.parent_gate_csv_check(bigint[]) to master_edit';
  end if;
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_parent_gate_scopes, ops.master_parent_gate_scope_keys to watcher';
  end if;
end $$;
