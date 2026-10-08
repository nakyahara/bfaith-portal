-- 0065: 新商品の NE 登録の CSV の版を上げる (単品 = ne-reg-single-v2・まとまり = ne-reg-variation-v1 の決まり) + 翌朝の確かめを 3 者一致に + 一度でも配った商品の代表 (親) を変えない守り
--       (2026-10-08。AI_reference CompanyDB構想/20_代表の正本を自社DBへ_設計 v7 §⑤・§⑩ の PR-2)
-- 🚨 前提 = 0053 (新規登録の CSV)・0058 (配る道の許可)・0063 (申告なしの確かめ)・0064 (大文字のコード・PR-1)。番号はマージの順で振り直す (中身は番号に依らない)
--
-- なぜ:
--   ・中原さん 10/8 夜「NE には JAN をほとんど入れていない。むしろ商品名に JAN を入れている」= NE 登録の CSV の JAN の列は empty にする (JAN は Company DB に残す)。
--     NE の完全な取得に JAN は無い (比べられない) = JAN を送ると 0063 の ops.ne_reg_auto_block が jan_not_compared を返し、翌朝の自動の確かめにならない。empty なら自動で確かめられる
--   ・CSV の形を変えたら版を上げる (前の版の実機の確かめは引き継がない = Codex 設計 R1 H1・R2 M2)。単品 = ne-reg-single-v2・まとまり = ne-reg-variation-v1 (どちらも JAN = empty)
--   ・中原さんの答え「ためしは要らない」= この 2 つの版は「試し用 5 行まで」の門を外す (1 ファイル 1,000 行まで)。最初のファイルの品目が翌朝 3 者一致で
--     確かめられたら、実機の確かめ (schema_verified = ops.ne_csv_verified) に記録だけ残す (門にはしない)
--   ・代表 (親) は登録の後に変えない (中原さんの確定 10/8 夜)。翌朝の確かめは「保存した期待値 = NE の観測 = 今の Company DB の値」の 3 者一致のときだけ verified (Codex 設計 R1 H3)
-- なにを:
--   1. ops.ne_reg_exports の形の版の CHECK に variation を足す
--   2. ops.ne_reg_schema_rule = 形の版の決まりの 1 か所 (種類・見出し・作れるか・試し用の門・JAN・代表の書き方)。lib/master-reg-csv.mjs の REG_SCHEMA_RULES と同じ (試験で照らす)
--        ne-reg-single-v1    = 作らない (0065 で引退・前に配ったファイルの確かめ・申告・使わないは今までどおり)
--        ne-reg-single-v2    = 単品の新しい版。JAN = empty・門なし
--        ne-reg-variation-v1 = まとまり (色違い・サイズ違い) の版。JAN = empty・代表商品コード = まとまりのコード (打ったとおり)・門なし。
--                              🚨 まだ作らない (まとまりの表と関数 = PR-5 で開く)。版の名前と決まりだけここで固定する
--        ne-reg-set-v1       = セット。今までどおり (試し用 5 行までの門あり)
--   3. ops.ne_reg_canonical (0053 の置き換え): 単品の JAN の列を常に empty に (JAN の確かめで止めない)
--   4. ops.ne_reg_build (0058 の置き換え): 形の版の確かめを ops.ne_reg_schema_rule に・作れない版は schema_not_buildable・門の無い版は試し用にしない
--   5. ops.ne_reg_cdb_compare = 期待値と今の Company DB の値を比べる (単品の代表 (親)。まとまりの PR で効く・関数の形はここで)
--   6. ops.record_ne_registration_check (0063 の置き換え): verified = NE の観測が期待値と合う かつ 今の Company DB も期待値と合う (3 者一致)。
--        NE は合うが Company DB が違う = partial (起きないはずの事故 = 答えの cdb_drift で知らせる)・門の無い版の最初の確かめ = schema_verified の記録だけ
--   7. 「一度でも配った」の守り: ops.sku_ever_issued (ファイルの issued_at = 一度付いたら動かない・品目は消さない = 追記だけの証跡) と
--        core.guard_parent_after_issue (core.products の代表 (親) を、その商品の SKU の CSV を一度でも配った後は変えない・外さない)。
--        products.parent の持ち主 (DB の active) が company のときだけ見る (load の間は夜間ロードが NE の代表を写す = 止めない)
-- 一部だけ NE に入ったとき (設計 v7 §⑤ R3 High 1・0063 の動きのまま = 関数は変えない):
--   申告の無い issued の品目が NE に無いとき、確かめは failed にしない (待ちのまま・3 日を過ぎたら not_imported の ℹ️)。
--   = 「N 件失敗」のときは、元のファイルの sha256 で partial を申告する (必須)。申告の後の完全な取得で、入った品目 = verified / 入らなかった品目 = failed (not_in_ne)。
--   failed の品目は生きていない = その商品だけの新しいファイルを作れる。申告しないと待ちのまま (作り直せない)。つかいかた (manual.ejs) に書いた
-- 🚨 security definer の関数は search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない・public の実行権なし (0053 / 0058 / 0063 と同じ)。
--    create or replace は今の権限を残す。新しい部品はだれにも渡さない (security definer の関数の中から持ち主の権限で呼ぶ)
-- 🚨 この migration は商品・仕入先・登録の状態・ファイルの値を何も変えない (変わるのは次に作るファイル・次の照合の確かめ・次の代表の書き込みから)

-- ─── 1. 形の版の CHECK (variation を足す) ───
alter table ops.ne_reg_exports drop constraint ne_reg_exports_schema_version_check;
alter table ops.ne_reg_exports add constraint ne_reg_exports_schema_version_check check (schema_version ~ '^ne-reg-(single|set|variation)-v[0-9]+$');

-- ─── 2. 形の版の決まり (1 か所)。知らない版 = null ───
--   kind = ops.ne_reg_exports.kind / sku_kind = 行の SKU の種類 / header = 見出し (版を上げたら ops.ne_reg_canonical も見直す) /
--   buildable = 新しいファイルを作れるか (why = 作れない理由) / trial_gate = 実機で確かめるまで試し用 (5 行まで) /
--   jan = JAN の列 (value = Company DB の有効な JAN / empty = 常に empty) / parent = 代表商品コードの列 (ne_code = 親の NE の元の書き方 / group = まとまりのコード)
create function ops.ne_reg_schema_rule(p_schema text) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case p_schema
    when 'ne-reg-single-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": false, "why": "0065 で引退 (JAN を送る版)。単品は ne-reg-single-v2", "trial_gate": true, "jan": "value", "parent": "ne_code"}'::jsonb
    when 'ne-reg-single-v2' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "ne_code"}'::jsonb
    when 'ne-reg-variation-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": false, "why": "まとまりの表と関数 (PR-5) の後に開く", "trial_gate": false, "jan": "empty", "parent": "group"}'::jsonb
    when 'ne-reg-set-v1' then '{"kind": "sets", "sku_kind": "set", "header": "set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo",
      "buildable": true, "why": null, "trial_gate": true, "jan": null, "parent": null}'::jsonb
  end
$$;
revoke all on function ops.ne_reg_schema_rule(text) from public;

-- ─── 3. 1 つの SKU の CSV の行と確かめる値 (0053 の置き換え・引数は同じ) ───
/**
 * 0053 と同じ決まり (lib/master-reg-csv.mjs の regMaterialOf と同じ) で、変えたのは単品の JAN だけ:
 *   🆕 0065 (ne-reg-single-v2・中原さん 10/8 夜): JAN の列は常に empty (Company DB の JAN は残す・NE には送らない)・JAN の数とチェック数字で止めない
 */
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
      if coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;
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
      if r.qty is null or r.qty <> pg_catalog.trunc(r.qty) or r.qty not between 1 and 999 then v_blk := v_blk || ('構成品 ' || r.code || ' の数量'); end if;
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

revoke all on function ops.ne_reg_canonical(bigint, date) from public;

-- ─── 4. NE 登録の CSV を作る (0058 の置き換え・引数は同じ) ───
/**
 * 0058 と同じ (鍵の順・許可・NE のコード・行と確かめる値の照らし直し・印とハッシュ・約束) で、変えたのは形の版の確かめと試し用だけ:
 *   🆕 0065: 種類・見出し・作れる版かは ops.ne_reg_schema_rule (作れない版 = schema_not_buildable)・門の無い版 (single-v2・variation-v1) は試し用にしない
 */
create or replace function ops.ne_reg_build(p jsonb, p_bytes bytea) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_rid      uuid;
  v_actor    text := pg_catalog.btrim(coalesce(p ->> 'actor', ''));
  v_kind     text := p ->> 'kind';
  v_schema   text := p ->> 'schema_version';
  v_header   text := p ->> 'header';
  v_rule     jsonb;
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
  -- 🆕 0065: 形の版の決まりは ops.ne_reg_schema_rule の 1 か所 (種類・見出し・作れる版か・試し用の門)
  v_rule := ops.ne_reg_schema_rule(v_schema);
  if v_rule is null or (v_rule ->> 'kind') is distinct from v_kind or (v_rule ->> 'header') is distinct from v_header then
    raise exception 'invalid_input: 種類・形の版・見出しが合わない (% / % / %)', v_kind, v_schema, v_header using errcode = '22023';
  end if;
  if (v_rule -> 'buildable') is distinct from 'true'::jsonb then
    raise exception 'schema_not_buildable: 形の版 % では新しいファイルを作らない (%)', v_schema, v_rule ->> 'why' using errcode = 'P0001';
  end if;
  v_cols := pg_catalog.string_to_array(v_header, ',');
  if v_cols && array['zaiko_su', 'yoyaku_zaiko_su', 'nyusyukko_riyu', 'visible_flg'] then
    raise exception 'invalid_input: 在庫の列は出さない (%)', v_header using errcode = '22023';
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
  -- 単品の CSV = 単品の許可・セットの CSV = セットの許可 (今は出す道が無い = DB の境界でも閉じたまま・#1644 Codex R1 Medium 2)
  perform ops._require_new_entry_lease(case when v_kind = 'products' then 'single' else 'set' end);
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
    if ops.ne_code_seen(v_sku.code_norm) then   -- 🆕 0058: 今の NE のコードと前に NE で見たコード・商品のコードも代表のコードも (登録の時と同じ範囲・#1640 R5)
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
  -- 実機の確かめの門 (種類 × 形の版 × 見出し・最後の記録が ok)。確かめていない = 試し用 (5 行まで)。🆕 0065: 門の無い版 (single-v2・variation-v1) は試し用にしない
  select coalesce((select v.result = 'ok' from ops.ne_csv_verified v
                    where v.kind = v_kind and v.col = 'new_registration' and v.encoding = 'utf8' and v.header = v_header and v.converter_version = v_schema
                    order by v.verified_id desc limit 1), false) into v_verified;
  v_trial := (v_rule -> 'trial_gate') = 'true'::jsonb and not v_verified;
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


-- ─── 5. 期待値と今の Company DB の値を比べる (3 者一致の 3 つ目) ───
--   単品 = 代表 (親) = 親の display_code の norm (親なし = null。ops.ne_reg_canonical の期待値と同じ作り方) / セット = 比べない (ok)。
--   まとまり (PR-5) の子の代表も同じ列 (期待値の parent = まとまりのコードの norm)。戻り値 { ok, cols: { parent: { ok, expected, cdb } } }
create function ops.ne_reg_cdb_compare(p_expected jsonb, p_sku_id bigint) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_found boolean := false;
  v_cur   jsonb;
  v_exp   jsonb;
  v_ok    boolean;
begin
  if (p_expected ->> 'kind') is distinct from 'single' then
    return pg_catalog.jsonb_build_object('ok', true, 'cols', '{}'::jsonb, 'not_compared', '["set"]'::jsonb);
  end if;
  select true, case when p.parent_product_id is null then 'null'::jsonb else pg_catalog.to_jsonb(core.norm_code(coalesce(pp.display_code, ''))) end
    into v_found, v_cur
    from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
   where k.sku_id = p_sku_id;
  v_exp := coalesce(p_expected -> 'values' -> 'parent', 'null'::jsonb);
  v_ok := coalesce(v_found, false) and v_cur = v_exp;
  return pg_catalog.jsonb_build_object('ok', v_ok,
    'cols', pg_catalog.jsonb_build_object('parent', pg_catalog.jsonb_build_object('ok', v_ok, 'expected', v_exp, 'cdb', coalesce(v_cur, 'null'::jsonb))));
end $$;
revoke all on function ops.ne_reg_cdb_compare(jsonb, bigint) from public;

-- ─── 6. 翌朝の照合の確かめ (0063 の置き換え・引数は同じ) ───
/**
 * 0063 と同じ (受け取り・観測・申告あり / なしの道・自動にしない品目・知らせ・鍵の順) で、変えたのは:
 *   🆕 0065 3 者一致: 比べる = NE の観測が期待値と全部の列で合う (ops.ne_reg_compare) かつ 今の Company DB が期待値と合う (ops.ne_reg_cdb_compare) = verified。
 *     NE は合うが Company DB が違う = partial + 答えの cdb_drift (起きないはずの事故 = 知らせる)。記録の detail に cdb
 *   🆕 0065 schema_verified: 門の無い版 (ops.ne_reg_schema_rule の trial_gate = false) の品目を初めて verified にしたとき、その版の実機の確かめの記録が
 *     まだ 1 つも無ければ ok を 1 行残す (verified_by = ne_compare・記録だけ = 作る門には使わない)
 * 戻り値 { compare_run_id, counts, not_imported, not_imported_days, needs_declaration, cdb_drift: [{ code, export_id, cols }] }
 */
create or replace function ops.record_ne_registration_check(p_run text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  h          ops.ne_reg_compare_runs%rowtype;
  rc         ops.ne_reg_compare_receipts%rowtype;
  it         record;
  v_obs      jsonb;
  v_fetched  timestamptz;
  v_cmp      jsonb;
  v_out      text;
  v_basis    text;
  v_late     boolean;
  v_days     constant integer := 3;   -- 配ってから何日たっても NE に無ければ「取り込まれていないらしい」と知らせるか (要約だけ・失敗にしない)
  v_counts   jsonb := '{}'::jsonb;
  v_missing  jsonb := '[]'::jsonb;
  v_block    text;
  v_needs    jsonb := '[]'::jsonb;
  v_reg      text;
  v_cdb      jsonb;
  v_drift    jsonb := '[]'::jsonb;
begin
  if p_run is null or p_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', p_run using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_reg_check'));
  select * into rc from ops.ne_reg_compare_receipts r where r.compare_run_id = p_run;
  if not found then raise exception 'not_sealed: 照合の回 % は最後まで終わった受け取りが無い = 確かめない', p_run using errcode = 'P0001'; end if;
  select * into h from ops.ne_reg_compare_runs r where r.compare_run_id = p_run;
  if h.observation_hash is distinct from rc.observation_hash or ops.ne_reg_observation_hash(p_run) is distinct from rc.observation_hash then
    raise exception 'receipt_mismatch: 照合の回 % の観測が受け取りと違う', p_run using errcode = 'P0001';
  end if;
  -- 鍵: SKU ごと (sku_id の順・保存と同じ鍵) → CSV の鍵
  perform ops.ne_reg_lock_skus((select pg_catalog.array_agg(t.sku_id) from ops.v_ne_reg_targets t
                                 where t.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)));
  for it in select i.*, e.declared_at as export_declared_at, e.issued_at as export_issued_at, e.kind as export_kind, e.schema_version as export_schema, e.header as export_header
              from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id
             where i.state in ('issued', 'import_declared', 'partial')
               and i.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)
             order by i.sku_id for update of i loop
    if exists (select 1 from ops.ne_reg_checks c where c.compare_run_id = p_run and c.item_id = it.item_id) then continue; end if;
    select o.observation into v_obs from ops.ne_reg_compare_observations o where o.compare_run_id = p_run and o.code_norm = it.code_norm;
    v_fetched := case when it.sku_kind = 'set' then h.sets_at else h.products_at end;
    v_cmp := null;
    v_cdb := null;
    v_late := false;
    v_block := null;
    -- 申告あり = 申告の時刻から (0053 のまま)・申告なし = 配った時刻から (0063)
    v_basis := case when it.attempt_id is not null then 'declared' else 'issued' end;
    if v_basis = 'declared' then
      if it.export_declared_at is null or v_fetched <= it.export_declared_at then
        v_out := 'waiting';
      elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
        v_out := 'waiting';
      elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
        v_out := case when h.absence_trusted then 'failed' else 'waiting' end;
      else
        v_cmp := ops.ne_reg_compare(it.expected, v_obs);
        v_cdb := ops.ne_reg_cdb_compare(it.expected, it.sku_id);   -- 🆕 0065: 3 者一致 (期待値 = NE の観測 = 今の Company DB)
        v_out := case when (v_cmp -> 'ok') = 'true'::jsonb and (v_cdb -> 'ok') = 'true'::jsonb then 'verified' else 'partial' end;
      end if;
    else
      if it.export_issued_at is null or v_fetched <= it.export_issued_at then
        v_out := 'waiting';   -- 配る前の取得 = 比べない
      elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
        v_out := 'waiting';
      elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
        v_out := 'waiting';   -- 申告が無い = 「取り込めなかった」と決めない
        v_late := h.absence_trusted and v_fetched > it.export_issued_at + pg_catalog.make_interval(days => v_days);
      else
        v_cmp := ops.ne_reg_compare(it.expected, v_obs);
        v_cdb := ops.ne_reg_cdb_compare(it.expected, it.sku_id);   -- 🆕 0065: 3 者一致 (期待値 = NE の観測 = 今の Company DB)
        v_block := nullif(ops.ne_reg_auto_block(it.item_id), '');
        if v_block is not null then
          v_out := 'in_ne_undeclared';   -- 比べられない列を送った / 配った時の印が無い = 自動にしない (申告すると 0053 のまま確かめる)
        else
          v_out := case when (v_cmp -> 'ok') = 'true'::jsonb and (v_cdb -> 'ok') = 'true'::jsonb then 'verified' else 'partial' end;
        end if;
      end if;
    end if;
    insert into ops.ne_reg_checks (compare_run_id, item_id, sku_id, fetched_at, outcome, detail)
      values (p_run, it.item_id, it.sku_id, v_fetched, v_out,
              pg_catalog.jsonb_build_object('state_before', it.state, 'basis', v_basis, 'compare', v_cmp, 'present', v_obs -> 'present', 'trusted', v_obs -> 'trusted',
                'not_imported', v_late, 'auto_block', v_block, 'cdb', v_cdb, 'fetch_generation', h.fetch_generation, 'raw_hash', h.raw_hash, 'evidence_sha256', rc.evidence_sha256));
    if v_out = 'verified' then
      update ops.ne_reg_export_items set state = 'verified', verified_run = p_run, verified_at = pg_catalog.now(), state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare'
       where item_id = it.item_id;
      -- 🆕 0065: 門の無い版の最初の確かめ = 実機の確かめ (schema_verified) に記録だけ (その版の記録がまだ 1 つも無いとき)
      if (ops.ne_reg_schema_rule(it.export_schema) -> 'trial_gate') = 'false'::jsonb
         and not exists (select 1 from ops.ne_csv_verified v where v.kind = it.export_kind and v.col = 'new_registration' and v.encoding = 'utf8'
                           and v.header = it.export_header and v.converter_version = it.export_schema) then
        insert into ops.ne_csv_verified (export_id, kind, col, encoding, header, converter_version, result, note, verified_by, reg_export_id)
          values (null, it.export_kind, 'new_registration', 'utf8', it.export_header, it.export_schema, 'ok', '最初の品目が翌朝の照合で 3 者一致 (0065・記録だけ)', 'ne_compare', it.export_id);
      end if;
      select r.state into v_reg from ops.master_registrations r where r.sku_id = it.sku_id;
      -- 申告なし = 下書きのまま → 照合の確かめで NE 登録待ちを通って NE 確認済みに (根拠は関数がこの確かめの記録から読む)
      if v_reg = 'draft' and v_basis = 'issued' then
        perform ops.transition_sku_registration(it.sku_id, 'ne_pending', 'system', 'ne_compare', null, '{}'::jsonb, null);
        v_reg := 'ne_pending';
      end if;
      if v_reg = 'ne_pending' then
        perform ops.transition_sku_registration(it.sku_id, 'ne_confirmed', 'system', 'ne_compare', null, '{}'::jsonb, null);
      end if;
    elsif v_out = 'partial' and it.state in ('issued', 'import_declared') then
      update ops.ne_reg_export_items set state = 'partial', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    elsif v_out = 'failed' then
      update ops.ne_reg_export_items set state = 'failed', failed_reason = 'not_in_ne', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    end if;
    if v_out = 'in_ne_undeclared' then
      v_needs := v_needs || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', it.ne_code, 'export_id', it.export_id::text, 'reason', v_block));
    end if;
    if v_out = 'partial' and (v_cmp -> 'ok') = 'true'::jsonb and (v_cdb -> 'ok') is distinct from 'true'::jsonb then
      v_drift := v_drift || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', it.ne_code, 'export_id', it.export_id::text, 'cols', v_cdb -> 'cols'));   -- 🆕 0065
    end if;
    if v_late then
      v_missing := v_missing || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', it.ne_code, 'export_id', it.export_id::text, 'issued_at', it.export_issued_at,
        'days', pg_catalog.floor(pg_catalog.date_part('epoch', v_fetched - it.export_issued_at) / 86400)::integer));
    end if;
    v_counts := v_counts || pg_catalog.jsonb_build_object(v_out, coalesce((v_counts ->> v_out)::integer, 0) + 1);
  end loop;
  -- 全部の商品が終わったファイルは閉じる
  update ops.ne_reg_exports e set state = 'closed', closed_at = pg_catalog.now(), closed_by = 'ne_compare', close_reason = 'finished'
   where e.state in ('issued', 'declared')
     and not exists (select 1 from ops.ne_reg_export_items i where i.export_id = e.export_id and i.state in ('built', 'issued', 'import_declared', 'partial'));
  return pg_catalog.jsonb_build_object('compare_run_id', p_run, 'counts', v_counts, 'not_imported', v_missing, 'not_imported_days', v_days, 'needs_declaration', v_needs, 'cdb_drift', v_drift);
end $$;

revoke all on function ops.record_ne_registration_check(text) from public;

-- ─── 7. 一度でも配った商品の代表 (親) を変えない ───
-- 「一度でも配った」= その SKU を含む NE 登録の CSV のファイルに配った時刻 (ops.ne_reg_exports.issued_at) がある。issued_at は一度付いたら動かない (0053 の trigger)・
--   品目の行は消さない = 品目が後で使わない (superseded)・取り込めなかった (failed) になっても残る (設計 v7 §⑤ R1 High 3)
create function ops.sku_ever_issued(p_sku_id bigint) returns boolean language sql stable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id
                  where i.sku_id = p_sku_id and e.issued_at is not null)
$$;
revoke all on function ops.sku_ever_issued(bigint) from public;

-- core.products の代表 (親) を、その商品の SKU の NE 登録の CSV を一度でも配った後は変えない・外さない (画面以外の書き手も = どのロールも)。
--   products.parent の持ち主 (DB の active) が company のときだけ見る: load の間は夜間ロードが NE の代表を写す道 (止めない・NE に入った後は NE の値 = 配った値)。
--   代表を付けるのは新商品の下書きの間だけ (設計 v7 §④・§⑤)。NE で直接作られた商品の代表の 1 回だけの採用 (PR-5) は一度も配っていない商品 = ここに当たらない
-- 🚨 security definer = 呼び手 (画面のロール) に持ち主表・CSV の表の読みを渡さない
create function core.guard_parent_after_issue() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_codes text;
begin
  if new.parent_product_id is not distinct from old.parent_product_id then return new; end if;
  if coalesce((select s.active_map ->> 'products.parent' from ops.master_ownership_state s where s.id = 1), 'load') is distinct from 'company' then return new; end if;
  select pg_catalog.string_agg(k.code, '・' order by k.code_norm) into v_codes from core.skus k where k.product_id = new.product_id and ops.sku_ever_issued(k.sku_id);
  if v_codes is not null then
    raise exception 'parent_frozen: 商品 % (%) は NE 登録の CSV を一度配った = 代表 (親) は変えない・外さない', new.product_id, v_codes using errcode = 'P0001';
  end if;
  return new;
end $$;
revoke all on function core.guard_parent_after_issue() from public;
create trigger trg_products_parent_after_issue before update of parent_product_id on core.products
  for each row execute function core.guard_parent_after_issue();

comment on table ops.ne_reg_exports is '新商品の NE 登録の CSV のファイル 1 つ = 1 行 (0053)。中身 (byte 列・ハッシュ・印) は変えない・消さない。書くのは ops.ne_reg_* の関数だけ。形の版の決まりは ops.ne_reg_schema_rule (0065)';

-- ─── 権限 (0063 と同じ形)。create or replace は今の権限を残す = 照合の確かめは watch_writer だけ (流し直し)・新しい部品はだれにも渡さない ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant execute on function ops.record_ne_registration_check(text) to watch_writer';
  end if;
end $$;
