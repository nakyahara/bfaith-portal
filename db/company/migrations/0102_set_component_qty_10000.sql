-- 0102 (仮番号・0101 の次。マージの時に次の空き番号に付け替える。migrate は欠番を断る): セットの構成品の数の上限を 999 → 10,000 にする
--   (2026-10-26 中原さん「10000 個まで上限上げて」・インスタント歯ブラシの 100 個セット (tbbk-100) から。NE にはもう構成品 700 個のセット (toothbrushbk-700) がある)
--
-- なにを (どちらも関数の置き換えだけ・値は何も変えない):
--   1. ops.new_set_derivation (0052 の版を写して、数量の形の確かめ (1〜999) と一言だけを 1〜10,000 に変えた)
--      = 新しいセットの登録 (ops.register_new_sku・0099 の版) が構成品を確かめて導く値を決める関数
--   2. ops.ne_reg_canonical (0067 の版を写して、セットの構成品の数量の止め (1〜999) だけを 1〜10,000 に変えた)
--      = NE 登録の CSV (セット) の行と確かめる値。lib/master-reg-csv.mjs の regMaterialOf と同じ決まり (アプリも同じ PR で 10,000 にした)
-- 変えない: 構成の依頼の形 (ops.component_request_rows_ok = 1〜99,999・0051)・夜間ロードの NE の構成の読み取り (1〜99,999)・
--   core.sku_components (qty > 0)・Amazon の対応 (0054 / 0073 の 1〜999 = 別の話・Amazon の 1 つの出品に入れる NE の商品の数)
-- 🚨 create or replace = 持ち主・実行権はそのまま (ロールの流し直しは要らない)。アプリ (lib/master-write.mjs の MAX_QTY・lib/master-reg-csv.mjs) が先に 10,000 になっても、
--    この migration の前の DB は 1,000 個以上のセットを登録の関数・CSV の止めで断る (何も書かない) = 先にアプリでも先に DB でも壊れない

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops.new_set_derivation(jsonb, text, date)') is null or pg_catalog.to_regprocedure('ops.ne_reg_canonical(bigint, date)') is null
     or pg_catalog.to_regprocedure('ops.set_entry_on_load()') is null then
    raise exception '0102 の前提: 0052 (登録の関数)・0067 (NE 登録の CSV の版)・0099 (セットの新商品の登録) が無い';
  end if;
  if pg_catalog.strpos(pg_catalog.pg_get_functiondef('ops.new_set_derivation(jsonb, text, date)'::regprocedure), '[0-9]{0,2}') = 0
     or pg_catalog.strpos(pg_catalog.pg_get_functiondef('ops.ne_reg_canonical(bigint, date)'::regprocedure), 'between 1 and 999 then') = 0 then
    raise exception '0102 の前提: ops.new_set_derivation / ops.ne_reg_canonical が 0052 / 0067 の版でない (ほかの migration が置き換えた = 写し直してから流す)';
  end if;
end $$;

-- ═══════════ 1. 新しいセットの構成品の確かめ (0052 の版 + 🆕 0102 = 数量 1〜10,000) ═══════════
-- 8e. 新しいセットの構成品を DB で確かめて、導く値を DB で決める (lib/master-set-rules.js の deriveSetCdb と同じ決め方を SQL で。登録の関数だけが呼ぶ)
--   確かめる: 1〜20 行・各行は sku_id (整数)・code (文字)・qty (🆕 0102: 1〜10,000 の整数)・sort だけ・(ほかは 0052 と同じ)
create or replace function ops.new_set_derivation(p_rows jsonb, p_handling_own text, p_today date) returns jsonb
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
                 or pg_catalog.jsonb_typeof(e.v -> 'qty') is distinct from 'number' or (e.v ->> 'qty') !~ '^([1-9][0-9]{0,3}|10000)$'
                 or pg_catalog.jsonb_typeof(e.v -> 'sort') is distinct from 'number' or (e.v ->> 'sort') is distinct from e.ord::text) then
    raise exception 'invalid_input: 構成品の行は { sku_id, code, qty (1〜10,000), sort (1 からの並び) } だけ' using errcode = '22023';
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

-- ═══════════ 2. NE 登録の CSV の行と確かめる値 (0067 の版 + 🆕 0102 = セットの構成品の数量 1〜10,000) ═══════════
create or replace function ops.ne_reg_canonical(p_sku_id bigint, p_day date) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  v_blk    text[] := '{}';
  v_price  bigint;
  v_tax    numeric;
  v_taxc   text;
  v_hand   text;
  v_cost   record;
  v_costv  bigint;
  v_nsup   integer;
  v_sup    record;
  v_supc   text;
  v_par    text;
  v_parc   text := 'empty';
  v_pr     record;
  v_cells  jsonb := '[]'::jsonb;
  v_exp    jsonb;
  v_req    jsonb;
  r        record;
  v_kids   jsonb := '[]'::jsonb;
  v_n      integer := 0;
  v_ntax   integer := 0;
  v_badtax boolean := false;
  v_portal text;
begin
  select k.sku_id, k.code, k.code_norm, k.sku_kind, k.name, k.tax_rate, k.handling, k.standard_price_jpy, k.product_id, p.parent_product_id, pp.display_code as parent_code
    into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
   where k.sku_id = p_sku_id;
  if not found then return pg_catalog.jsonb_build_object('blockers', pg_catalog.jsonb_build_array('商品が無い')); end if;
  if not ops.ne_reg_name_ok(s.name) then v_blk := v_blk || '名前を CSV に書けない'::text; end if;
  if s.standard_price_jpy is null or s.standard_price_jpy <> pg_catalog.trunc(s.standard_price_jpy::numeric) or s.standard_price_jpy not between 1 and 999999999 then
    v_blk := v_blk || '標準売価が無い (1〜999,999,999 円)'::text;
  else
    v_price := s.standard_price_jpy::bigint;
  end if;
  if s.sku_kind = 'single' then
    v_tax := case when s.tax_rate = 0.1 then 0.1 when s.tax_rate = 0.08 then 0.08 end;
    v_taxc := case when v_tax = 0.1 then '10' when v_tax = 0.08 then '8' end;
    if v_taxc is null then v_blk := v_blk || '税率が無い (8% か 10%)'::text; end if;
    v_hand := case s.handling when 'active' then '0' when 'discontinued' then '1' end;
    if v_hand is null then v_blk := v_blk || '取扱区分が 取扱中 / 中止 でない'::text; end if;
    -- p_day の原価 (lib/master-write.mjs の costAsOfJoin と同じ選び方)
    select y.cost_jpy, y.cost_status into v_cost from core.sku_costs y
     where y.sku_id = p_sku_id and y.valid_from <= p_day and (y.valid_to is null or y.valid_to >= p_day)
     order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1;
    if v_cost.cost_status is null or v_cost.cost_status not in ('COMPLETE', 'OVERRIDDEN') or v_cost.cost_jpy is null
       or v_cost.cost_jpy <> pg_catalog.trunc(v_cost.cost_jpy) or v_cost.cost_jpy < 1 then
      v_blk := v_blk || '原価が無い (今日の原価・1 円以上)'::text;
    else
      v_costv := v_cost.cost_jpy::bigint;
    end if;
    select pg_catalog.count(*) into v_nsup from core.supplier_skus x where x.sku_id = p_sku_id and x.is_primary;
    if v_nsup <> 1 then
      v_blk := v_blk || '代表の仕入先が決まっていない'::text;
    else
      select su.supplier_id, su.code, rg.state as reg_state into v_sup
        from core.supplier_skus x join core.suppliers su on su.supplier_id = x.supplier_id
        left join ops.supplier_registrations rg on rg.supplier_id = su.supplier_id
       where x.sku_id = p_sku_id and x.is_primary;
      v_supc := core.canonical_supplier_code(v_sup.code);
      if v_supc is null or v_supc !~ '^[0-9]{4}$' then v_blk := v_blk || '代表の仕入先のコードが NE の形 (4 桁の数字) でない'::text; v_supc := null; end if;
      if v_sup.reg_state is not null and v_sup.reg_state <> 'ne_confirmed' then v_blk := v_blk || '代表の仕入先の「NE に登録した」の申告がまだ'::text; end if;
    end if;
    -- 代表 (親): なし = empty / あり = 名札か商品の NE の元の書き方 (名札を先に)
    if s.parent_product_id is not null then
      v_par := core.norm_code(coalesce(s.parent_code, ''));
      select c.state, c.ne_code into v_pr from ops.master_ne_codes c where c.code_norm = v_par and c.kind in ('rep', 'product')
       order by (c.kind = 'rep') desc limit 1;
      -- 🆕 0067: NE に無い (0041 に行が無い) まとまりは、ポータルで作った札 (予約 source = portal・同じまとまり) なら予約のコード (打ったとおり)
      select g.code into v_portal from ops.variation_group_codes g
       where g.company_id = 1 and g.code_norm = v_par and g.group_product_id = s.parent_product_id and g.source = 'portal';
      if coalesce(v_par, '') <> '' and v_pr.state is null and v_portal is not null then v_parc := v_portal;
      elsif coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;
      else v_parc := v_pr.ne_code; end if;
    end if;
    -- 🆕 0065 (ne-reg-single-v2・中原さん 10/8 夜): JAN は NE に送らない (Company DB の JAN は残す) = JAN の列は常に empty・JAN の数とチェック数字で止めない
    v_cells := pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_supc, ''), coalesce(v_costv::text, ''), coalesce(v_price::text, ''),
      coalesce(v_taxc, ''), coalesce(v_hand, ''), v_parc, 'empty'));
    v_exp := pg_catalog.jsonb_build_object('kind', 'single', 'values', pg_catalog.jsonb_build_object('name', s.name, 'supplier', v_supc, 'cost', v_costv, 'price', v_price,
      'tax_rate', v_tax, 'handling', s.handling, 'parent', v_par));
  elsif s.sku_kind = 'set' then
    -- 構成 = 開いている構成の依頼 (新しいセットは core.sku_components が空) か、今の構成 (並び = 依頼の sort / 今の sort_order・コード)
    select q.rows into v_req from ops.sku_component_requests q where q.set_sku_id = p_sku_id and q.status = 'open';
    for r in
      select k.sku_id, k.code, k.code_norm, k.tax_rate, x.qty, rg.state as reg_state, nc.state as nc_state, nc.ne_code
        from (select (e ->> 'sku_id')::bigint as sku_id, (e ->> 'qty')::numeric as qty, (e ->> 'sort')::numeric as srt, null::text as cn
                from pg_catalog.jsonb_array_elements(coalesce(v_req, '[]'::jsonb)) e where v_req is not null
              union all
              select c.child_sku_id, c.qty::numeric, c.sort_order::numeric, null::text from core.sku_components c where v_req is null and c.parent_sku_id = p_sku_id) x
        left join core.skus k on k.sku_id = x.sku_id
        left join ops.master_registrations rg on rg.sku_id = x.sku_id
        left join ops.master_ne_codes nc on nc.kind = 'product' and nc.code_norm = k.code_norm
       order by x.srt, k.code_norm
    loop
      v_n := v_n + 1;
      if r.code is null then v_blk := v_blk || '構成品の SKU が無い'::text; continue; end if;
      if r.tax_rate is null or r.tax_rate not in (0.1, 0.08) then v_badtax := true; elsif r.tax_rate = 0.08 then v_ntax := v_ntax + 1; end if;
      if r.reg_state is null or r.reg_state not in ('ne_confirmed', 'distributable', 'available') then v_blk := v_blk || ('構成品 ' || r.code || ' が NE 確認済みでない'); end if;
      if r.nc_state is distinct from 'ok' then v_blk := v_blk || ('構成品 ' || r.code || ' の NE の書き方が確かめられない'); end if;
      if r.qty is null or r.qty <> pg_catalog.trunc(r.qty) or r.qty not between 1 and 10000 then v_blk := v_blk || ('構成品 ' || r.code || ' の数量'); end if;
      v_cells := v_cells || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_price::text, ''), '%TAX%', coalesce(r.ne_code, ''),
        coalesce(r.qty::bigint::text, '')));
      v_kids := v_kids || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code_norm', r.code_norm, 'qty', r.qty::bigint));
    end loop;
    if v_n = 0 then v_blk := v_blk || '構成品が無い'::text; end if;
    -- セットの税率 = 構成品の税率が全部分かるときだけ・混ざれば低い方 (lib/master-set-rules.js の deriveSetTaxCdb)
    v_taxc := case when v_badtax or v_n = 0 then null when v_ntax > 0 then '8' else '10' end;
    if v_taxc is null then v_blk := v_blk || 'セットの税率が決まらない (構成品の税率)'::text; end if;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_set(c, '{3}', pg_catalog.to_jsonb(coalesce(v_taxc, ''))) order by o), '[]'::jsonb) into v_cells
      from pg_catalog.jsonb_array_elements(v_cells) with ordinality as t(c, o);
    v_exp := pg_catalog.jsonb_build_object('kind', 'set', 'values', pg_catalog.jsonb_build_object('name', s.name, 'price', v_price, 'children', v_kids));
  else
    v_blk := v_blk || '例外の SKU は NE に登録しない'::text;
  end if;
  return pg_catalog.jsonb_build_object('cells', v_cells, 'expected', v_exp, 'blockers', pg_catalog.to_jsonb(v_blk));
end $$;

-- ═══════════ 3. 確かめ ═══════════
do $$ begin
  if pg_catalog.strpos(pg_catalog.pg_get_functiondef('ops.new_set_derivation(jsonb, text, date)'::regprocedure), '{0,3}|10000)') = 0
     or pg_catalog.strpos(pg_catalog.pg_get_functiondef('ops.ne_reg_canonical(bigint, date)'::regprocedure), 'between 1 and 10000 then') = 0 then
    raise exception '0102: 置き換えが効いていない';
  end if;
end $$;
