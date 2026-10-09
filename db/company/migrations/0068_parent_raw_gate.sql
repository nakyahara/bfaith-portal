-- 0068: 代表 (親) の数えと門 (2026-10-09。AI_reference CompanyDB構想/20_代表の正本を自社DBへ_設計 v7 §②・§⑥・§⑩ の PR-6)
-- 🚨 前提 = 0058 (広げる道・新商品の許可)・0059 (Amazon の widen・_widen_judge の今の版)・0065 (NE 登録の CSV の版・3 者一致)・0066。
--    0067 (まとまりの DB・PR-5) とは別の関数・表だけを触る (ops.ne_reg_build / ne_reg_issue / ne_reg_canonical / record_ne_registration_check は作り直さない =
--    門は品目の表の trigger で効かせる)。番号はマージの順で振り直す (中身は番号に依らない)
--
-- なぜ (中原さんの決定 10/8 夜・設計 v7 §②・§⑥):
--   代表 (products.parent) の持ち主を夜間ロード (load) から Company DB (company) に広げる前に、今の代表のずれを数えて、直して 0 にしてから広げる
--   (差を残したまま広げる承認の表はやめた)。数えは承認 (accept_difference) で減らない「生の数え」。広げた後は、毎朝の照合 ② の数えが 0 でなければ
--   新しい一般の NE 登録の CSV (作る / 配る) を閉じる (配った後の代表は NE と社内で同じ、が古い表 (NE の値を読むアプリ) の前提 = §⑤)。
-- なにを:
--   1. ops.master_widen_allowed_keys() に products.parent (0059 の版から写して足しただけ)
--   2. 親専用の生の数え ops.parent_raw_gate(会社, NE の観測, 一覧を出すか) = 6 つの数え (単品だけ・登録の状態 × 品目の状態の判定表 1 つ = §⑥)。
--      NE の観測 (完全な取得の単品の代表) は照合 ② のプログラムが渡す (DB は NE を読めない)。数えは DB が自分の今の値で数える (呼び手の数は受けない)。
--      何も書かない = 読むだけの CLI drift-list (watcher) も同じ関数を呼ぶ
--   3. 構造の数え ops.parent_structure_counts(会社) = 全部の商品の 2 段・循環 (widen の判定がその場で数える)
--   4. 照合 ② の数えの記録 ops.master_parent_gate_results (追記だけ) と書く関数 ops.record_parent_gate (watch_writer)
--   5. 門 ops.parent_gate_state() (読むだけ・watcher) と、新しい一般の NE 登録の CSV を閉じる trigger (ops.ne_reg_export_items の作る = insert built・
--      配る = built → issued)。🚨 products.parent の持ち主 (DB の active) が company のときだけ閉じる。load の間 (今) は数えて知らせるだけ (今の単品の登録を止めない)。
--      通すもの (§⑥ の「門が閉じていても通す」): 配ったファイルの再取得・申告・照合・使わない (品目の insert / built → issued 以外は見ない)・
--      作り直し (その SKU の前の品目 (使わないにしたものを除く) が failed = 取り込めなかった商品だけ)・廃止・quarantined の代表の採用 (品目を書かない)
--   6. ops._widen_judge (0059 の版から写して、products.parent を足す試みのときだけの判定を足しただけ):
--      生の数え = 0 (6 つ)・その記録は封をした照合の回 (結果の JSON の sha256 つき)・prepared のロードの後・同じ材料の世代・NE の取得が手の入口の停止の後・
--      全部の商品の 2 段・循環 = 0 (その場で数える)
--   7. 判断の画面の「差を残す」(accept_difference) を代表 (親) の候補に使わせない (承認の出来事の trigger)
-- 🚨 security definer の関数は search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない・public の実行権なし (0058 / 0059 / 0065 と同じ)
-- 🚨 この migration は商品・登録・ファイルの値を何も変えない (変わるのは: 照合 ② が数えを記録する・広げてよいキー・代表を company にした後の CSV の門)

-- ─── 1. 広げてよいキー (0059 の版から products.parent を足しただけ) ───
create or replace function ops.master_widen_allowed_keys() returns text[] language sql immutable set search_path = pg_catalog, pg_temp as $$
  select array['listing_components.amazon', 'products.parent', 'skus.sku_kind']::text[]
$$;
revoke all on function ops.master_widen_allowed_keys() from public;

-- ─── 2. 構造の部品: 親を辿ると循環する (か 64 段を超える) 商品 (会社の全部の商品・親のある商品から辿る)。何も書かない・だれにも渡さない ───
create function ops._parent_loop_products(p_company_id integer) returns setof bigint
  language sql stable set search_path = pg_catalog, pg_temp as $$
  with recursive walk (start_id, cur_id, path, depth, cyc) as (
    select p.product_id, p.parent_product_id, array[p.product_id], 1, p.parent_product_id = p.product_id
      from core.products p where p.company_id = p_company_id and p.parent_product_id is not null
    union all
    select w.start_id, x.parent_product_id, w.path || w.cur_id, w.depth + 1, x.parent_product_id = any (w.path || w.cur_id)
      from walk w join core.products x on x.product_id = w.cur_id
     where not w.cyc and w.depth < 64 and x.parent_product_id is not null
  )
  select distinct w.start_id from walk w where w.cyc or w.depth >= 64
$$;
revoke all on function ops._parent_loop_products(integer) from public;

-- 構造の数え (会社の全部の商品・登録の状態によらない = DB は「親か子のどちらか一方」を全部の商品で持つ)。widen の判定がその場で数える
--   two_level = 親を持つ商品で、親も親を持つ・自分も子を持つ / loop = 親を辿ると循環する
create function ops.parent_structure_counts(p_company_id integer) returns jsonb
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_object(
    'two_level', (select pg_catalog.count(*) from core.products p join core.products q on q.product_id = p.parent_product_id
                   where p.company_id = p_company_id and p.parent_product_id <> p.product_id
                     and (q.parent_product_id is not null
                          or exists (select 1 from core.products c where c.parent_product_id = p.product_id and c.product_id <> p.product_id))),
    'loop', (select pg_catalog.count(*) from ops._parent_loop_products(p_company_id)))
$$;
revoke all on function ops.parent_structure_counts(integer) from public;

-- ─── 3. 親専用の生の数え (設計 v7 §② の 6 つ・§⑥ の判定表)。何も書かない ───
/**
 * p_ne = 照合 ② が NE の完全な取得から作った観測 (apps/company-db/master-compare/parent-gate.mjs の parentObservations):
 *   { format: 'parent-obs-v1', complete: boolean (行が落ちていない取得), untrusted: [code_norm] (正規化の衝突・取込の整合で保持した商品),
 *     rows: [[code_norm, kind ('single' | 'set'), rep_state ('ok' | 'unknown'), rep_norm (親なし = null), rep_raw (代表の原文)]] (セットは後ろの 3 つが null) }
 * 数える対象 (判定表・上の行から): 単品 (sku_kind = single) だけ (セット・例外は外す = excluded)。
 *   品目の最新が partial = 数える / 登録 cancelled = 外す / quarantined = 数える /
 *   draft・ne_pending: 品目が verified = 数える・issued / import_declared = 外す (登録の確かめ = 3 者一致で見る)・failed = 外す (復旧中)・
 *                      品目なし / built / superseded = 外す (NE にまだ無くて当然 = 自己デッドロックを防ぐ) /
 *   それ以外 (ne_confirmed・distributable・available・backfill・状態の行が無い前からの商品) = 数える
 * 1 つの単品は 1 つの数えだけに入る (上から):
 *   parent_loop          = 親を辿ると循環する
 *   parent_two_level     = 親を持ち、親も親を持つ / 自分も子を持つ (2 段)
 *   parent_incomparable  = NE の代表が分からない (対象の商品なのに取得に無い・取得で保持した商品・NE ではセット・代表の元の値が不明)
 *   parent_ambiguous     = NE の代表が Company DB の 2 つ以上の商品に当たる (札の display_code・単品のコード) / NE の代表の書き方が 2 つ以上 (正規化で衝突)
 *   parent_missing       = NE に代表があるのに Company DB に親が無い (保持・帰属 null の行も数える)
 *   parent_mismatch      = NE の代表 (自分自身・空 = なし) と Company DB の親 (親の display_code の norm) が違う
 * 戻り値 { format, counts: {6 つ}, counted (数える対象の数), excluded: {set, exception, draft, issued, failed, cancelled}, obs: {...}, samples: {数え: [コード 10 件まで]},
 *          items (p_detail のときだけ = drift-list の一覧: [{ code, class, reason, ne_parent, cdb_parent, reg_state, item_state }]) }
 */
create function ops.parent_raw_gate(p_company_id integer, p_ne jsonb, p_detail boolean default false) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_out jsonb;
begin
  if p_company_id is distinct from 1 then raise exception 'invalid_input: 会社 % (今は会社 1 だけ)', coalesce(p_company_id::text, 'null') using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_ne) is distinct from 'object' or (p_ne ->> 'format') is distinct from 'parent-obs-v1'
     or pg_catalog.jsonb_typeof(p_ne -> 'rows') is distinct from 'array' or pg_catalog.jsonb_typeof(p_ne -> 'untrusted') is distinct from 'array'
     or pg_catalog.jsonb_typeof(p_ne -> 'complete') is distinct from 'boolean' then
    raise exception 'invalid_input: NE の観測は { format: parent-obs-v1, complete, untrusted: [...], rows: [...] }' using errcode = '22023';
  end if;
  if pg_catalog.jsonb_array_length(p_ne -> 'rows') > 200000 then raise exception 'invalid_input: NE の観測の行が多すぎる (200,000 行まで)' using errcode = '22023'; end if;
  if exists (select 1 from pg_catalog.jsonb_array_elements(p_ne -> 'rows') x
              where case   -- 順に確かめる (配列でない値に jsonb_array_length を呼ばない)
                      when pg_catalog.jsonb_typeof(x) <> 'array' then true
                      when pg_catalog.jsonb_array_length(x) <> 5 then true
                      when pg_catalog.jsonb_typeof(x -> 0) <> 'string' or pg_catalog.length(x ->> 0) not between 1 and 200 then true
                      when pg_catalog.jsonb_typeof(x -> 1) <> 'string' or (x ->> 1) not in ('single', 'set') then true
                      when (x ->> 1) = 'set' then pg_catalog.jsonb_typeof(x -> 2) <> 'null' or pg_catalog.jsonb_typeof(x -> 3) <> 'null' or pg_catalog.jsonb_typeof(x -> 4) <> 'null'
                      when pg_catalog.jsonb_typeof(x -> 2) <> 'string' or (x ->> 2) not in ('ok', 'unknown') then true
                      when pg_catalog.jsonb_typeof(x -> 3) not in ('string', 'null') or pg_catalog.jsonb_typeof(x -> 4) not in ('string', 'null') then true
                      when (x ->> 2) = 'unknown' then pg_catalog.jsonb_typeof(x -> 3) <> 'null'
                      when pg_catalog.jsonb_typeof(x -> 3) = 'string' then pg_catalog.length(x ->> 3) not between 1 and 200
                        or pg_catalog.jsonb_typeof(x -> 4) <> 'string' or pg_catalog.length(x ->> 4) not between 1 and 200
                      else false end) then
    raise exception 'invalid_input: NE の観測の行の形が違う ([code_norm, single|set, ok|unknown, rep_norm|null, rep_raw|null])' using errcode = '22023';
  end if;
  if exists (select 1 from pg_catalog.jsonb_array_elements(p_ne -> 'untrusted') u where pg_catalog.jsonb_typeof(u) <> 'string' or pg_catalog.length(u #>> '{}') not between 1 and 200) then
    raise exception 'invalid_input: untrusted は code_norm の文字の配列' using errcode = '22023';
  end if;

  with obs0 as materialized (
    select x ->> 0 as code_norm, x ->> 1 as kind, x ->> 2 as rep_state, x ->> 3 as rep_norm, x ->> 4 as rep_raw
      from pg_catalog.jsonb_array_elements(p_ne -> 'rows') x),
  dup as (select o.code_norm from obs0 o group by o.code_norm having pg_catalog.count(*) > 1),
  obs as (select distinct on (o.code_norm) o.* from obs0 o order by o.code_norm, o.kind),
  untr as (select u.v as code_norm from pg_catalog.jsonb_array_elements_text(p_ne -> 'untrusted') u(v) union select d.code_norm from dup d),
  rep_coll as (select o.rep_norm from obs0 o where o.kind = 'single' and o.rep_norm is not null group by o.rep_norm having pg_catalog.count(distinct o.rep_raw) > 1),
  cand as (
    select z.norm, pg_catalog.count(distinct z.product_id) as n from (
      select core.norm_code(p.display_code) as norm, p.product_id from core.products p where p.company_id = p_company_id and p.display_code is not null
      union all
      select k.code_norm, k.product_id from core.skus k where k.company_id = p_company_id and k.product_id is not null) z
     where z.norm is not null and z.norm <> '' group by z.norm),
  loops as (select l.product_id from ops._parent_loop_products(p_company_id) as l(product_id)),
  kids as (select distinct c.parent_product_id as product_id from core.products c
            where c.company_id = p_company_id and c.parent_product_id is not null and c.parent_product_id <> c.product_id),
  li as (select distinct on (i.sku_id) i.sku_id, i.state from ops.ne_reg_export_items i order by i.sku_id, i.item_id desc),
  base as (
    select k.sku_id, k.code, k.code_norm, k.sku_kind, k.product_id, p.parent_product_id as pid, pp.display_code as pdisp, pp.parent_product_id as gpid,
           (kd.product_id is not null) as has_kids, (lp.product_id is not null) as in_loop, r.state as reg, li.state as item
      from core.skus k
      left join core.products p on p.product_id = k.product_id
      left join core.products pp on pp.product_id = p.parent_product_id
      left join kids kd on kd.product_id = k.product_id
      left join loops lp on lp.product_id = k.product_id
      left join ops.master_registrations r on r.sku_id = k.sku_id
      left join li on li.sku_id = k.sku_id
     where k.company_id = p_company_id),
  scoped as (
    select b.*, case
        when b.sku_kind = 'set' then 'set'
        when b.sku_kind is distinct from 'single' then 'exception'
        when b.item = 'partial' then 'count'
        when b.reg = 'cancelled' then 'cancelled'
        when b.reg = 'quarantined' then 'count'
        when b.reg in ('draft', 'ne_pending') and b.item = 'verified' then 'count'
        when b.reg in ('draft', 'ne_pending') and b.item in ('issued', 'import_declared') then 'issued'
        when b.reg in ('draft', 'ne_pending') and b.item = 'failed' then 'failed'
        when b.reg in ('draft', 'ne_pending') then 'draft'
        else 'count' end as scope
      from base b),
  cls as (
    select s.*, o.kind as ne_kind, o.rep_state, o.rep_norm, o.rep_raw,
      case
        when s.in_loop then 'parent_loop'
        when s.pid is not null and (s.gpid is not null or s.has_kids) then 'parent_two_level'
        when o.code_norm is null or u.code_norm is not null or o.kind <> 'single' or o.rep_state <> 'ok' then 'parent_incomparable'
        when o.rep_norm is not null and (coalesce(c.n, 0) > 1 or rc.rep_norm is not null) then 'parent_ambiguous'
        when o.rep_norm is not null and s.pid is null then 'parent_missing'
        when s.pid is null then null
        when o.rep_norm is null then 'parent_mismatch'
        when coalesce(core.norm_code(s.pdisp), '') is distinct from o.rep_norm then 'parent_mismatch'
        else null end as klass,
      case
        when s.in_loop or (s.pid is not null and (s.gpid is not null or s.has_kids)) then null
        when o.code_norm is null then case when (p_ne -> 'complete') = 'true'::jsonb then 'not_in_ne' else 'not_in_ne_rows_dropped' end
        when u.code_norm is not null then 'ne_untrusted'
        when o.kind <> 'single' then 'ne_is_set'
        when o.rep_state <> 'ok' then 'ne_rep_unknown'
        when o.rep_norm is not null and coalesce(c.n, 0) > 1 then 'cdb_candidates_' || c.n::text
        when o.rep_norm is not null and rc.rep_norm is not null then 'ne_rep_spellings'
        when s.pid is not null and coalesce(core.norm_code(s.pdisp), '') = '' then 'cdb_parent_no_code'
        else null end as reason
      from scoped s
      left join obs o on o.code_norm = s.code_norm
      left join untr u on u.code_norm = s.code_norm
      left join cand c on c.norm = o.rep_norm
      left join rep_coll rc on rc.rep_norm = o.rep_norm),
  samples as (
    select x.klass, pg_catalog.jsonb_agg(x.code order by x.code_norm) as codes
      from (select c.klass, c.code, c.code_norm, pg_catalog.row_number() over (partition by c.klass order by c.code_norm) as rn
              from cls c where c.scope = 'count' and c.klass is not null) x
     where x.rn <= 10 group by x.klass)
  select pg_catalog.jsonb_build_object(
    'format', 'parent-raw-v1',
    'counts', pg_catalog.jsonb_build_object(
      'parent_mismatch', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_mismatch'),
      'parent_incomparable', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_incomparable'),
      'parent_ambiguous', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_ambiguous'),
      'parent_missing', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_missing'),
      'parent_two_level', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_two_level'),
      'parent_loop', pg_catalog.count(*) filter (where t.scope = 'count' and t.klass = 'parent_loop')),
    'counted', pg_catalog.count(*) filter (where t.scope = 'count'),
    'excluded', pg_catalog.jsonb_build_object('set', pg_catalog.count(*) filter (where t.scope = 'set'), 'exception', pg_catalog.count(*) filter (where t.scope = 'exception'),
      'draft', pg_catalog.count(*) filter (where t.scope = 'draft'), 'issued', pg_catalog.count(*) filter (where t.scope = 'issued'),
      'failed', pg_catalog.count(*) filter (where t.scope = 'failed'), 'cancelled', pg_catalog.count(*) filter (where t.scope = 'cancelled')),
    'obs', pg_catalog.jsonb_build_object('rows', (select pg_catalog.count(*) from obs0), 'singles', (select pg_catalog.count(*) from obs0 o where o.kind = 'single'),
      'sets', (select pg_catalog.count(*) from obs0 o where o.kind = 'set'), 'untrusted', (select pg_catalog.count(*) from untr), 'complete', p_ne -> 'complete'),
    'samples', coalesce((select pg_catalog.jsonb_object_agg(sm.klass, sm.codes) from samples sm), '{}'::jsonb),
    'items', case when p_detail then coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', c.code, 'class', c.klass, 'reason', c.reason,
        'ne_parent', c.rep_raw, 'cdb_parent', c.pdisp, 'reg_state', c.reg, 'item_state', c.item) order by c.klass, c.code_norm)
        from cls c where c.scope = 'count' and c.klass is not null), '[]'::jsonb) end)
    into v_out
    from cls t;
  return v_out;
end $$;
revoke all on function ops.parent_raw_gate(integer, jsonb, boolean) from public;

-- ─── 4. 照合 ② の数えの記録 (追記だけ・照合の回ごとに 1 行・書くのは ops.record_parent_gate だけ) ───
create table ops.master_parent_gate_results (
  result_id               bigint generated always as identity primary key,
  company_id              smallint not null default 1 references core.companies,
  compare_run_id          text not null unique check (compare_run_id ~ '^[A-Za-z0-9_.:-]{1,80}$'),
  ne_generation_id        text not null check (ne_generation_id ~ '^[A-Za-z0-9_.:-]{1,120}$'),   -- NE の完全な取得の世代 (compare-ne の neFetchIdentity)
  ne_raw_hash             text not null check (ne_raw_hash ~ '^[0-9a-f]{64}$'),
  products_complete_at    timestamptz not null,   -- NE の取得の完了 (単品)
  setproducts_complete_at timestamptz not null,   -- NE の取得の完了 (セット)
  material_generation_id  text not null check (material_generation_id ~ '^[A-Za-z0-9_.:-]{1,200}$'),   -- 照合が読んだ材料の世代 (= ロードの ops.load_materials.generation_id)
  evidence_sha256         text not null check (evidence_sha256 ~ '^[0-9a-f]{64}$'),   -- 照合の結果の JSON (不変) の sha256 = 封をした回
  obs_hash                text not null check (obs_hash ~ '^[0-9a-f]{64}$'),         -- 渡された NE の観測の sha256 (jsonb の文字)
  counts                  jsonb not null check (jsonb_typeof(counts) = 'object'),
  counted                 bigint not null check (counted >= 0),
  excluded                jsonb not null check (jsonb_typeof(excluded) = 'object'),
  samples                 jsonb not null check (jsonb_typeof(samples) = 'object'),
  owner_at_record         text not null check (owner_at_record in ('load', 'company')),   -- 記録した時の products.parent の持ち主 (DB の active)
  recorded_by             text not null,
  created_at              timestamptz not null default clock_timestamp()
);
comment on table ops.master_parent_gate_results is '照合 ② の代表 (親) の生の数え (0068・設計 20 v7 §⑥)。書くのは ops.record_parent_gate (watch_writer) だけ。門 (ops.parent_gate_state) と widen の判定は一番新しい行を読む';

create function ops.guard_master_parent_gate_results() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.parent_gate_protocol', true), '') is distinct from '1' then
    raise exception 'master_parent_gate_results は ops.record_parent_gate でだけ書く' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_parent_gate_results() from public;
create trigger trg_master_parent_gate_results_guard before insert on ops.master_parent_gate_results for each row execute function ops.guard_master_parent_gate_results();
select core.make_append_only('ops', 'master_parent_gate_results');

-- products.parent の持ち主 (DB の active。行が無い = 全部 load)。company のときだけ門を閉じる
create function ops.parent_gate_enforced() returns boolean language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce((select s.active_map ->> 'products.parent' from ops.master_ownership_state s where s.id = 1), 'load') = 'company'
$$;
revoke all on function ops.parent_gate_enforced() from public;

/**
 * 照合 ② の最後に watch_writer が 1 回 (結果の JSON を書いた後 = 封をした回)。DB が自分で数えて (ops.parent_raw_gate) 1 行を残す。
 *   p_fetch = { generation_id, raw_hash, products_complete_at, setproducts_complete_at } (NE の完全な取得・RFC 3339 の明示の offset)。
 *     取得の完了は記録の時刻と同じ JST の日で、それより前 (古い取得の数を残さない = 0058 の ops.record_new_entry_gate と同じ)
 *   p_material_generation_id = 照合が読んだ材料の世代 (widen の判定がロードの材料の世代と照らす)・p_evidence_sha256 = 照合の結果の JSON の sha256
 *   鍵 = 新商品の許可の排他の鍵 (種類の順) = CSV を作る / 配る取引 (共有) の完了を待ってから書く (書いた瞬間から門はこの行を読む)
 *   同じ照合の回の 2 回目は拒む (run_reused)。戻り値 = { result_id (文字), counts, counted, excluded, samples, owner, gate: ops.parent_gate_state() }
 */
create function ops.record_parent_gate(p_compare_run_id text, p_fetch jsonb, p_material_generation_id text, p_evidence_sha256 text, p_ne jsonb) returns jsonb
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
  v := ops.parent_raw_gate(1, p_ne, false);
  v_owner := case when ops.parent_gate_enforced() then 'company' else 'load' end;
  perform pg_catalog.set_config('ops.parent_gate_protocol', '1', true);
  insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
                                              evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by, created_at)
    values (1, p_compare_run_id, p_fetch ->> 'generation_id', p_fetch ->> 'raw_hash', v_pat, v_sat, p_material_generation_id, p_evidence_sha256,
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_ne::text, 'UTF8')), 'hex'), v -> 'counts', (v ->> 'counted')::bigint, v -> 'excluded', v -> 'samples',
            v_owner, session_user::text, v_now)
    returning result_id into v_id;
  perform pg_catalog.set_config('ops.parent_gate_protocol', '', true);
  return pg_catalog.jsonb_build_object('result_id', v_id::text, 'compare_run_id', p_compare_run_id, 'counts', v -> 'counts', 'counted', v -> 'counted', 'excluded', v -> 'excluded',
    'samples', v -> 'samples', 'obs', v -> 'obs', 'owner', v_owner, 'gate', ops.parent_gate_state());
end $$;
revoke all on function ops.record_parent_gate(text, jsonb, text, text, jsonb) from public;

-- ─── 5. 門 (読むだけ) ───
-- 門が閉じている理由 (空 = 開いている)。持ち主によらず数える (load の間は ops.parent_gate_state が enforced = false で「知らせだけ」と返す):
--   一番新しい記録が無い / 一番新しい記録の照合の回が、今の新商品の許可を出す一番新しいゲートの結果の回 (0058) と違う (今朝の照合で代表を数えられなかった) /
--   今日 (JST) の記録でない / 6 つの数えのどれかが 0 でない
create function ops._parent_gate_problems() returns text[]
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  r      ops.master_parent_gate_results;
  v_run  text;
  v_out  text[] := '{}';
  v_bad  text;
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
  select pg_catalog.string_agg(e.key || ' ' || (e.value #>> '{}'), '・' order by e.key) into v_bad
    from pg_catalog.jsonb_each(r.counts) e where ops.jsonb_nonneg_bigint(e.value) is distinct from 0;
  if v_bad is not null then v_out := v_out || format('parent_raw: 代表のずれが 0 でない (%s・照合の回 %s)', v_bad, r.compare_run_id); end if;
  return v_out;
end $$;
revoke all on function ops._parent_gate_problems() from public;

-- 門の状態 (読むだけ・watcher / 照合の要約 / drift-list)。enforced = products.parent の持ち主が company (閉じる) / open = enforced でない か 理由が無い
create function ops.parent_gate_state() returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  r       ops.master_parent_gate_results;
  v_p     text[] := ops._parent_gate_problems();
  v_enf   boolean := ops.parent_gate_enforced();
begin
  select * into r from ops.master_parent_gate_results x order by x.result_id desc limit 1;
  return pg_catalog.jsonb_build_object('enforced', v_enf, 'open', not v_enf or coalesce(pg_catalog.array_length(v_p, 1), 0) = 0, 'problems', pg_catalog.to_jsonb(v_p),
    'result_id', r.result_id::text, 'compare_run_id', r.compare_run_id, 'counts', r.counts, 'created_at', r.created_at);
end $$;
revoke all on function ops.parent_gate_state() from public;

-- ─── 5b. 新しい一般の NE 登録の CSV を閉じる (品目の表の trigger = どの書き手も当たる・ops.ne_reg_build / ne_reg_issue は作り直さない) ───
--   見るのは 品目の insert (作る = built) と built → issued (初めて配る) だけ。それ以外 (申告・照合の確かめ・使わない・failed) は見ない = 直す道は門で止めない。
--   作り直し = その SKU の前の品目 (使わないにした superseded を除く) の最後が failed (取り込めなかった商品だけの新しいファイル) = 通す
create function ops.guard_ne_reg_parent_gate() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prev text;
  v_p    text[];
begin
  if tg_op = 'INSERT' then
    if new.state is distinct from 'built' then return new; end if;
  elsif not (old.state = 'built' and new.state = 'issued') then
    return new;
  end if;
  if not ops.parent_gate_enforced() then return new; end if;   -- 持ち主が load の間 (今) は閉じない (数えて知らせるだけ)
  select i.state into v_prev from ops.ne_reg_export_items i
   where i.sku_id = new.sku_id and i.item_id < new.item_id and i.state <> 'superseded' order by i.item_id desc limit 1;
  if v_prev = 'failed' then return new; end if;   -- 作り直し (取り込めなかった商品だけ)
  v_p := ops._parent_gate_problems();
  if coalesce(pg_catalog.array_length(v_p, 1), 0) > 0 then
    raise exception 'parent_gate_closed: 代表 (親) の数えの門が閉じているので、新しい NE 登録の CSV は作らない・配らない (%)。配ったファイルの申告・照合・取り込めなかった商品だけの作り直し・廃止はできる。ずれの一覧 = drift-list.mjs',
      pg_catalog.array_to_string(v_p, ' / ') using errcode = 'P0001';
  end if;
  return new;
end $$;
revoke all on function ops.guard_ne_reg_parent_gate() from public;
create trigger trg_ne_reg_items_parent_gate before insert or update of state on ops.ne_reg_export_items
  for each row execute function ops.guard_ne_reg_parent_gate();

-- ─── 7. 判断の画面の「差を残す」を代表 (親) に使わせない (設計 v7 §② / §⑥・承認で生の数えは減らない) ───
--   前からの候補 (resolutions に accept_difference が入ったまま) にも効く = 承認の出来事の insert で断る
create function ops.master_decision_events_parent_no_accept() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if new.kind = 'approved' and new.resolution = 'accept_difference'
     and exists (select 1 from ops.master_decision_candidates c where c.fingerprint = new.fingerprint and c.col = 'parent') then
    raise exception 'parent_no_accept: 代表 (親) の差は「差を残す」で閉じない (NE か社内を直して 0 にする・設計 20 v7 §②) (%)', new.fingerprint using errcode = '23514';
  end if;
  return new;
end $$;
revoke all on function ops.master_decision_events_parent_no_accept() from public;
create trigger trg_mde_parent_no_accept before insert on ops.master_decision_events
  for each row execute function ops.master_decision_events_parent_no_accept();

-- ─── 6. 判定の本体 (0059 の版から写して、products.parent を足す試みのときだけの判定 (9) を足しただけ) ───
create or replace function ops._widen_judge(p_widen_prepare_id uuid, p_company_id integer) returns jsonb
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
  v_kind_added boolean;   -- 0059: skus.sku_kind を足す試み = 区分の判断の記録と最終形を見る
  v_amz_added  boolean;   -- 0059: listing_components.amazon を足す試み = 消えた対応 0・active の対応 1 件以上を見る
  v_amz      jsonb;
  v_common   jsonb;
  v_parent_added boolean;   -- 🆕 0068: products.parent を足す試み = 照合 ② の代表の生の数え 0・構造の数え 0 を見る
  pgr        ops.master_parent_gate_results;
  v_struct   jsonb;
  v_mgen     text;
  v_bad      text;
begin
  if p_company_id is distinct from 1 then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('unsupported_company: 会社 ' || coalesce(p_company_id::text, 'null') || ' (今は会社 1 だけ)'), 'counts', '{}'::jsonb);
  end if;
  -- 1〜3. 試み・段階・epoch・足すだけ・ack・手の入口の停止 = 共通の部品 (移行の窓と同じ。#1648 Codex R1 Medium 1)
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id;
  if not found then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('attempt_missing: 試み ' || coalesce(p_widen_prepare_id::text, 'null') || ' が無い'), 'counts', '{}'::jsonb);
  end if;
  v_kind_added := 'skus.sku_kind' = any(a.added_keys);
  v_amz_added := 'listing_components.amazon' = any(a.added_keys);
  v_parent_added := 'products.parent' = any(a.added_keys);
  v_common := ops._widen_attempt_common(p_widen_prepare_id, v_now);
  select coalesce(array_agg(x.v order by x.i), '{}') into v_problems from jsonb_array_elements_text(v_common -> 'problems') with ordinality as x(v, i);
  v_acks := v_common -> 'acks';
  v_stop_at := (v_common ->> 'stop_at')::timestamptz;

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

    -- 6. ③④ prepared のロードの判断の記録 (section skus の payload.sku_kind・format = sku-kind-v1)。🆕 0059: skus.sku_kind を足す試みのときだけ
    --    (sku_kind がもう company の後に Amazon だけを足す日は、夜間ロードが区分の食い違いを held に記録し続ける = 見ない)
    if v_kind_added then
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
  end if;

  -- 7. ⑤⑥ 最終形。🆕 0059: skus.sku_kind を足す試みのときだけ (足した後は G19 が commit のときに守る)
  if v_kind_added then
    v_shape := ops.sku_kind_shape_counts(p_company_id);
    v_counts := v_counts || v_shape;
    if (v_shape ->> 'single_product_mismatch')::bigint <> 0 then v_problems := v_problems || format('shape: 区分と product_id の不整合が %s 件', v_shape ->> 'single_product_mismatch'); end if;
    if (v_shape ->> 'non_set_parent_components')::bigint <> 0 then v_problems := v_problems || format('shape: セットでない親の構成が %s 件', v_shape ->> 'non_set_parent_components'); end if;
  end if;

  -- 8. 🆕 0059: Amazon SKU の対応 (listing_components.amazon を足す試みのときだけ) = 消えた対応 0 件・active の対応が 1 件以上 (移行が済んだ)。
  --    古い表と Company DB のハッシュの一致は CLI が鍵の後に読んで照らし、widen が写しの証拠 (amazon_map) の数をこの数と照らす
  if v_amz_added then
    v_amz := ops.widen_amazon_map_counts(p_company_id);
    v_counts := v_counts || v_amz;
    if (v_amz ->> 'amazon_map_lost')::bigint <> 0 then
      v_problems := v_problems || format('amazon_map: 消えた対応が %s 件 (変更の記録にあるのに行が無い = trigger を止めて消された。ops.amazon_map_lost_listings() を見て戻す)', v_amz ->> 'amazon_map_lost');
    end if;
    if (v_amz ->> 'amazon_map_active')::bigint < 1 then
      v_problems := v_problems || 'amazon_map: active の対応が 0 件 (移行 amazon-map-migrate.mjs --apply がまだ = 古い表を移してから)'::text;
    end if;
  end if;

  -- 9. 🆕 0068: 代表 (products.parent を足す試みのときだけ・設計 20 v7 §②)。差を残す承認では通らない (生の数え)
  --    a. 一番新しい照合 ② の代表の数えの記録 (封をした回 = 結果の JSON の sha256 つき・ops.record_parent_gate だけが書く) の 6 つの数え = 0
  --    b. その記録は試みの prepared のロードの commit の後 (ロードの後の Company DB を数えた)・照合が読んだ材料の世代 = prepared のロードの材料の世代
  --       (同じ世代 = 同じ NE の取得から作った材料)・NE の取得の完了が手の入口の停止の後 (止めた後に NE の画面で代表を直していない取得)
  --    c. 全部の商品の 2 段・循環 = 0 (ここで数える = 照合の後に変わっていないか)
  if v_parent_added then
    select * into pgr from ops.master_parent_gate_results x order by x.result_id desc limit 1;
    if not found then
      v_problems := v_problems || 'parent_gate: 照合 ② の代表 (親) の数えの記録が無い (prepared のロードの後に照合 ② を流す)'::text;
    else
      v_counts := v_counts || jsonb_build_object('parent_gate_result_id', pgr.result_id::text, 'parent_gate_compare_run', pgr.compare_run_id, 'parent_raw', pgr.counts,
        'parent_counted', pgr.counted, 'parent_excluded', pgr.excluded);
      select string_agg(e.key || ' ' || (e.value #>> '{}'), '・' order by e.key) into v_bad from jsonb_each(pgr.counts) e where ops.jsonb_nonneg_bigint(e.value) is distinct from 0;
      if v_bad is not null or (select count(*) from jsonb_object_keys(pgr.counts)) <> 6 then
        v_problems := v_problems || format('parent_raw: 代表のずれが 0 でない (%s・照合の回 %s。drift-list.mjs で一覧を見て直してから)', coalesce(v_bad, '数えの形が違う'), pgr.compare_run_id);
      end if;
      if c2.commit_seq is null then
        v_problems := v_problems || 'parent_gate: prepared のロードが決まらない (試みの中の commit がちょうど 2 つでない)'::text;
      else
        if pgr.created_at <= c2.committed_at then
          v_problems := v_problems || format('parent_gate: 代表の数えの記録 (%s) が prepared のロード (%s) の前', pgr.compare_run_id, c2.ingest_run_id);
        end if;
        select lm.generation_id into v_mgen from ops.load_materials lm where lm.ingest_run_id = c2.ingest_run_id and lm.entity = 'products';
        if pgr.material_generation_id is distinct from v_mgen then
          v_problems := v_problems || format('parent_gate: 照合が読んだ材料の世代 (%s) が prepared のロードの材料の世代 (%s) と違う', pgr.material_generation_id, coalesce(v_mgen, 'なし'));
        end if;
      end if;
      if pgr.products_complete_at <= v_stop_at or pgr.setproducts_complete_at <= v_stop_at then
        v_problems := v_problems || format('parent_gate: 照合の NE の取得 (%s) が手の入口の停止 (%s) の前', least(pgr.products_complete_at, pgr.setproducts_complete_at), v_stop_at);
      end if;
    end if;
    v_struct := ops.parent_structure_counts(p_company_id);
    v_counts := v_counts || jsonb_build_object('parent_structure', v_struct);
    if (v_struct ->> 'two_level')::bigint <> 0 or (v_struct ->> 'loop')::bigint <> 0 then
      v_problems := v_problems || format('parent_structure: 2 段の商品 %s・循環の商品 %s (全部の商品で 0 だけ)', v_struct ->> 'two_level', v_struct ->> 'loop');
    end if;
  end if;

  return jsonb_build_object('ok', coalesce(array_length(v_problems, 1), 0) = 0, 'problems', to_jsonb(v_problems), 'counts', v_counts, 'loads', v_loads, 'acks', v_acks,
    'widen_prepare_id', a.widen_prepare_id, 'added_keys', to_jsonb(a.added_keys), 'stop_at', v_stop_at, 'checked_at', v_now);
end $$;
revoke all on function ops._widen_judge(uuid, integer) from public;

-- ─── 権限 (0058 / 0059 と同じ形)。create or replace は今の権限を残す = 判定の本体・広げてよいキーはだれにも渡さない (watcher の読むだけの判定 = 0058 の grant のまま) ───
--   watcher       = 生の数え (drift-list・照合の要約の読み直し)・門の状態 (読むだけ)
--   watch_writer  = 照合 ② の数えの記録
--   だれにも渡さない = 構造の部品・門の理由・持ち主の判定・trigger の関数 (security definer の中から持ち主の権限で呼ぶ)
--   🚨 ロールを後から作る DB は scripts/company-db/create-watch-roles.mjs が同じ grant を付ける
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function ops.parent_raw_gate(integer, jsonb, boolean) to watcher';
    execute 'grant execute on function ops.parent_gate_state() to watcher';
    execute 'revoke all on function ops._widen_judge(uuid, integer) from watcher';
    execute 'revoke all on function ops.record_parent_gate(text, jsonb, text, text, jsonb) from watcher';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant execute on function ops.record_parent_gate(text, jsonb, text, text, jsonb) to watch_writer';
  end if;
end $$;
