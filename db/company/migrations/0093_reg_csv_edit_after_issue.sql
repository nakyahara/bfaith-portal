-- 0093 (0092 = 販売数のグラフの索引の次。migrate は欠番を断る): 新商品の NE 登録の CSV (0053) を配った後も、NE に送る欄 (名前・取扱区分・標準売価・原価・税率・
--   代表の仕入先・JAN / セットの名前・売価) を直せるようにする (セットの構成の依頼だけは確かめが終わるまで今までどおり) (2026-10-19 中原さん「名前修正できるようにしてよ … 毎回修正できないとかが多すぎて使えない」)
--
-- なぜ外してよいか: 翌朝の照合の確かめ (ops.record_ne_registration_check・0065 の版) は、NE の観測を品目の行の期待値 (ops.ne_reg_export_items.expected =
--   作ったときに固めた値・trigger で変えられない) と比べる (ops.ne_reg_compare)。今の Company DB と比べるのは代表 (親) だけ (ops.ne_reg_cdb_compare) で、
--   代表は 0065 の「一度でも配ったら変えない」(core.guard_parent_after_issue) が別に守る = ここでは触らない。
--   = 配った後に今の値を直しても確かめは壊れない (NE が配った CSV の値なら verified)。確かめの後の NE と Company DB の差は、毎朝の照合 ② の値の差
--   (持ち主が Company DB の列 = NE を Company DB の値に) として出て、今までどおり「マスタの判断 → NE に取り込む CSV」(0040) で NE に送る
-- なにを (どれも前の最後の版を写して 🆕 0093 の行だけ変えた・ほかの確かめ・鍵・権限・security definer・search_path は同じ):
--   1. ops.reg_csv_edit_after_issue() = 印 (アプリが「0093 の後の DB か」を to_regprocedure で見る。0093 の前の DB = アプリは今までどおり 409 reg_csv_issued)
--   2. ops.ne_reg_supersede_built (0053 の版): 配った後の品目があっても、作っただけ (built) のファイルは使わないにする (前 = 配った後の品目が 1 つでもあれば何もしないで知らせる)。
--      戻り値の形は同じ { issued, superseded } = issued は知らせるだけ (保存は断らない)
--   3. ops.edit_sku_jan (0080 の版): 配った後の reg_csv_issued の断りを外す
--   4. ops.deactivate_supplier (0053 の版): 付け替える商品の配った後の reg_csv_issued の断りを外す
--   5. ops.guard_reg_csv_live (0053 の版・画面のロールの直接の書き込みの守り): 断るのは作っただけ (built) のファイルの商品だけ (理由の名前 = reg_csv_built)。
--      ただしセットの構成の依頼 (ops.sku_component_requests) は今までどおり配った後も確かめが終わるまで断る (reg_csv_issued)
--      (確かめは配った構成と NE を比べる = 先に NE の構成を直すと partial のまま止まる・#0093 Codex R1 Medium。アプリは lib/master-write.mjs の REG_LOCK_AFTER_ISSUE)
-- 変えない: NE に取り込む CSV (0040) の csv_issued の断り (確かめに期待値の写しが無い = 設計 25 PR-2 で扱う)・代表 (親) の 0065 の断り・
--   ロジザードの最初の値 (0062 = 配った後は直せない)・品目の行と期待値 (変えない・消さない)・作る関数の「まだ終わっていないファイルがある」(同じ商品を 2 つのファイルに入れない)
-- 🚨 この migration は値を何も変えない (関数を作り直すだけ)。create or replace = 持ち主・master_edit の実行権・public から外した実行権はそのまま
--   (create-master-edit-roles.mjs の流し直しは要らない)。revoke は念のためもう一度

-- ═══════════ 1. 印 ═══════════
create or replace function ops.reg_csv_edit_after_issue() returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$ select true $$;
comment on function ops.reg_csv_edit_after_issue() is '0093: 新商品の NE 登録の CSV を配った後も NE に送る欄を直せる DB の印 (アプリが to_regprocedure で見る)';
revoke all on function ops.reg_csv_edit_after_issue() from public;

-- ═══════════ 2. 作っただけのファイルを使わないにする (0053 の版から) ═══════════
/**
 * (関数の中だけ) この SKU たちの作っただけ (built) の新規登録の CSV = そのファイルを閉じる (中の商品は全部 superseded = 作り直す)。
 * 🆕 0093: 配った後 (issued / import_declared / partial) の品目があっても止めない (知らせるだけ = issued に入れる)。戻り値 { issued: [...], superseded: [export_id] }
 */
create or replace function ops.ne_reg_supersede_built(p_sku_ids bigint[], p_actor text, p_what text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_issued  jsonb;
  v_exports bigint[];
begin
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('export_id', i.export_id::text, 'sku_id', i.sku_id::text, 'code', s.code, 'state', i.state) order by i.item_id)
    into v_issued
    from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id
   where i.sku_id = any (coalesce(p_sku_ids, '{}')) and i.state in ('issued', 'import_declared', 'partial');
  select pg_catalog.array_agg(distinct i.export_id) into v_exports from ops.ne_reg_export_items i where i.sku_id = any (coalesce(p_sku_ids, '{}')) and i.state = 'built';
  if v_exports is null then return pg_catalog.jsonb_build_object('issued', coalesce(v_issued, '[]'::jsonb), 'superseded', '[]'::jsonb); end if;
  update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left('配る前に ' || coalesce(p_what, '') || 'を直した', 500),
         superseded_correction = 'まだ配っていない (NE には何もしていない)。作り直す', state_changed_at = pg_catalog.now(), state_changed_by = p_actor
   where export_id = any (v_exports) and state = 'built';
  update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor, close_reason = 'superseded' where export_id = any (v_exports) and state = 'built';
  return pg_catalog.jsonb_build_object('issued', coalesce(v_issued, '[]'::jsonb), 'superseded', (select pg_catalog.jsonb_agg(x::text order by x) from pg_catalog.unnest(v_exports) x));
end $$;
revoke all on function ops.ne_reg_supersede_built(bigint[], text, text) from public;

-- ═══════════ 3. JAN を足す・外す (0080 の版から) ═══════════
create or replace function ops.edit_sku_jan(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint, p_seen jsonb, p_jans jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_sku     record;
  v_skus    bigint[];
  v_cur     text[];
  v_seen    text[];
  v_want    text[];
  v_add     text[];
  v_rem     text[];
  v_holder  text;
  v_lz      text;
  v_sup     jsonb;
  v_result  jsonb;
  x         text;
begin
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_jans) is distinct from 'array' or pg_catalog.jsonb_typeof(p_seen) is distinct from 'array'
     or exists (select 1 from pg_catalog.jsonb_array_elements(p_jans) j where pg_catalog.jsonb_typeof(j) <> 'string' or not ops.jan_check_ok(j #>> '{}'))
     or exists (select 1 from pg_catalog.jsonb_array_elements(p_seen) j where pg_catalog.jsonb_typeof(j) <> 'string') then
    raise exception 'invalid_input: JAN は 8 桁か 13 桁の数字でチェック数字が合うものの配列 (画面が見ていた JAN も配列)' using errcode = '22023';
  end if;
  select coalesce(pg_catalog.array_agg(distinct t order by t), '{}') into v_want from pg_catalog.jsonb_array_elements_text(p_jans) t;
  if pg_catalog.cardinality(v_want) > 5 then raise exception 'invalid_input: JAN は 5 つまで' using errcode = '22023'; end if;
  select coalesce(pg_catalog.array_agg(distinct t order by t), '{}') into v_seen from pg_catalog.jsonb_array_elements_text(p_seen) t;
  perform ops.reg_write_gate(p_ownership, array['external_ids.jan']);
  select k.sku_id, k.code, k.sku_kind, k.product_id into v_sku from core.skus k where k.sku_id = p_sku_id;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if v_sku.sku_kind is distinct from 'single' or v_sku.product_id is null then raise exception 'invalid_input: JAN は商品のある単品だけ' using errcode = '22023'; end if;
  -- 鍵: 同じ商品の SKU (sku_id の順) → CSV → 商品・SKU の行 (JAN は商品の値 = 同じ商品の SKU の CSV に入る)
  select pg_catalog.array_agg(k.sku_id order by k.sku_id) into v_skus from core.skus k where k.product_id = v_sku.product_id;
  perform ops.ne_reg_lock_skus(v_skus);
  -- 🆕 0080 (#1719 Codex R8 High): 足す (かもしれない) JAN の値の鍵を並べ替えた順で取る = ops.set_sku_logizard_barcode と同じ鍵 (ops.lzbc:<値>)。
  --   鍵の順は書く関数と同じ「SKU の鍵 → 値の鍵」= 同時でも、どちらかが待ってから相手の commit を見る
  foreach x in array v_want loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lzbc:' || x, 0));
  end loop;
  perform 1 from core.products p where p.product_id = v_sku.product_id for update;
  perform 1 from core.skus k where k.sku_id = any (v_skus) order by k.sku_id for update;
  select coalesce(pg_catalog.array_agg(e.external_value order by e.external_value), '{}') into v_cur from core.external_ids e
   where e.entity_type = 'product' and e.entity_id = v_sku.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null;
  if v_cur is distinct from v_seen then
    raise exception 'version_conflict: 画面を開いた後にこの商品の JAN が変わった (今 %)', pg_catalog.array_to_string(v_cur, '・') using errcode = 'P0001';
  end if;
  select coalesce(pg_catalog.array_agg(t order by t), '{}') into v_add from pg_catalog.unnest(v_want) t where not (t = any (v_cur));
  select coalesce(pg_catalog.array_agg(t order by t), '{}') into v_rem from pg_catalog.unnest(v_cur) t where not (t = any (v_want));
  if pg_catalog.cardinality(v_add) = 0 and pg_catalog.cardinality(v_rem) = 0 then
    return pg_catalog.jsonb_build_object('ok', true, 'code', v_sku.code, 'no_change', true, 'jan', pg_catalog.to_jsonb(v_cur));
  end if;
  foreach x in array v_add loop
    select coalesce((select s.code from core.skus s where e.entity_type = 'product' and s.product_id = e.entity_id order by s.code_norm limit 1), e.entity_type || ' ' || e.entity_id)
      into v_holder from core.external_ids e
     where e.system = 'jan' and e.id_kind = 'jan' and e.external_norm = core.norm_code(x) and e.valid_to is null
       and not (e.entity_type = 'product' and e.entity_id = v_sku.product_id) limit 1;
    if v_holder is not null then raise exception 'jan_taken: JAN % はほかの商品 (%) の有効な JAN', x, v_holder using errcode = 'P0001'; end if;
    -- 🆕 0080: ほかの商品の今のロジザードのバーコードと同じ JAN = 断る (同じ商品の SKU は自分の値)
    --   (一度でも x を持った SKU の一番新しい行が x のもの = 今の値。前の値は効かない)
    select k.code into v_lz from (select distinct on (r.sku_id) r.sku_id, r.value from ops.sku_logizard_barcode_rules r
                                    where r.company_id = 1 and r.sku_id in (select r0.sku_id from ops.sku_logizard_barcode_rules r0 where r0.value = x)
                                    order by r.sku_id, r.rule_row desc) c
      join core.skus k on k.sku_id = c.sku_id left join ops.master_registrations g on g.sku_id = c.sku_id
     where c.value = x and k.product_id is distinct from v_sku.product_id and g.state is distinct from 'cancelled'
     order by k.code_norm limit 1;
    if v_lz is not null then raise exception 'jan_taken: JAN % はほかの商品 (%) のロジザードのバーコード (ロジザードに送る値) と同じ', x, v_lz using errcode = 'P0001'; end if;
  end loop;
  -- 🆕 0093: NE 登録の CSV を配った後 (issued / import_declared / partial) でも JAN を直せる (0080 の reg_csv_issued の断りを外した・ほかは 0080 のまま)
  perform ops.open_reg_write('jan_edit', p_request_id, p_actor_id, p_reason, p_ownership, p_sku_id,
    (select coalesce(pg_catalog.array_agg(s order by s), '{}') from pg_catalog.unnest(v_skus) s where s <> p_sku_id), array[v_sku.product_id],
    ops.reg_hash(pg_catalog.jsonb_build_object('op', 'jan_edit', 'sku_id', p_sku_id, 'from', v_cur, 'to', v_want, 'reason', p_reason)),
    pg_catalog.jsonb_build_object('sku_ids', pg_catalog.to_jsonb(v_skus), 'product_id', v_sku.product_id::text));
  v_sup := ops.ne_reg_supersede_built(v_skus, p_actor_id, 'JAN');
  update core.external_ids set valid_to = pg_catalog.now()
   where entity_type = 'product' and entity_id = v_sku.product_id and system = 'jan' and id_kind = 'jan' and valid_to is null and external_value = any (v_rem);
  insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id, evidence)
    select 1, 'product', v_sku.product_id, 'jan', 'jan', t, 'manual', 'human', p_actor_id,
           pg_catalog.jsonb_build_object('request_id', p_request_id::text, 'reason', p_reason, 'source', 'portal_master_edit')
      from pg_catalog.unnest(v_add) t order by t;
  v_result := pg_catalog.jsonb_build_object('ok', true, 'code', v_sku.code, 'kind', 'single', 'request_id', p_request_id::text,
    'changed', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('field', 'jan', 'label', 'JAN', 'from', pg_catalog.to_jsonb(v_cur), 'to', pg_catalog.to_jsonb(v_want), 'ne', 'manual')),
    'superseded', v_sup -> 'superseded');
  perform ops.close_reg_write(v_result, v_sku.code);
  return v_result;
end $$;
revoke all on function ops.edit_sku_jan(uuid, text, text, jsonb, bigint, jsonb, jsonb) from public;

-- ═══════════ 4. 仕入先の取引停止 (0053 の版から) ═══════════
create or replace function ops.deactivate_supplier(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_code text, p_reassign_to text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_reassign boolean := coalesce(pg_catalog.btrim(p_reassign_to), '') <> '';
  v_id       bigint;
  v_code     text;
  v_active   boolean;
  v_planned  bigint[];
  v_used     bigint[];
  v_codes    text[];
  v_to       record;
  v_to_id    bigint;
  v_bad      text;
  v_sup      jsonb := pg_catalog.jsonb_build_object('superseded', '[]'::jsonb);
  v_result   jsonb;
begin
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or coalesce(pg_catalog.btrim(p_reason), '') = '' then
    raise exception 'invalid_input: 人・理由 (200 字まで) が要る' using errcode = '22023';
  end if;
  perform ops.reg_write_gate(p_ownership, array['suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days']
    || case when v_reassign then array['supplier_skus.is_primary'] else '{}'::text[] end);
  select s.supplier_id into v_id from core.suppliers s where s.company_id = 1 and s.code_norm = core.norm_code(coalesce(p_code, ''));
  if not found then raise exception 'not_found: 仕入先 % は Company DB に無い', p_code using errcode = 'P0002'; end if;
  select coalesce(pg_catalog.array_agg(x.sku_id order by x.sku_id), '{}') into v_planned from core.supplier_skus x where x.supplier_id = v_id and x.is_primary;
  if v_reassign then
    perform ops.ne_reg_lock_skus(v_planned);   -- SKU の鍵 (sku_id の順) → CSV の鍵
  end if;
  select s.code, s.active into v_code, v_active from core.suppliers s where s.supplier_id = v_id for update;
  if not v_active then return pg_catalog.jsonb_build_object('ok', true, 'code', v_code, 'active', false, 'already', true); end if;
  select coalesce(pg_catalog.array_agg(x.sku_id order by x.sku_id), '{}'), coalesce(pg_catalog.array_agg(k.code order by k.code_norm), '{}') into v_used, v_codes
    from core.supplier_skus x join core.skus k on k.sku_id = x.sku_id where x.supplier_id = v_id and x.is_primary;
  perform 1 from core.supplier_skus x where x.supplier_id = v_id and x.is_primary order by x.sku_id for update;
  if pg_catalog.cardinality(v_used) > 0 then
    if not v_reassign then
      raise exception 'supplier_in_use: 仕入先 % は % 件の商品の代表の仕入先 (%)', v_code, pg_catalog.cardinality(v_used), pg_catalog.array_to_string(v_codes[1:10], '・') using errcode = 'P0001';
    end if;
    if exists (select 1 from pg_catalog.unnest(v_used) u where not (u = any (v_planned))) then
      raise exception 'retry: 付け替える商品がちょうど増えた (もう一度押す)' using errcode = 'P0001';
    end if;
    select s.supplier_id, s.code, s.active, r.state as reg_state into v_to from core.suppliers s left join ops.supplier_registrations r on r.supplier_id = s.supplier_id
     where s.company_id = 1 and s.code_norm = core.norm_code(p_reassign_to) for update of s;
    v_to_id := v_to.supplier_id;
    if v_to_id is null then raise exception 'invalid_input: 付け替える先の仕入先 % が無い', p_reassign_to using errcode = '22023'; end if;
    if v_to.supplier_id = v_id then raise exception 'invalid_input: 付け替える先が同じ仕入先' using errcode = '22023'; end if;
    if not v_to.active then raise exception 'invalid_input: 付け替える先の仕入先 % は取引停止', v_to.code using errcode = '22023'; end if;
    if v_to.reg_state is not null and v_to.reg_state <> 'ne_confirmed' then
      raise exception 'supplier_not_confirmed: 付け替える先の仕入先 % は「NE に登録した」の申告がまだ', v_to.code using errcode = 'P0001';
    end if;
    select pg_catalog.string_agg(distinct k.code, '・') into v_bad
      from core.skus k join ops.ne_csv_export_rows r on r.code_norm = k.code_norm join ops.ne_csv_exports e on e.export_id = r.export_id
     where k.sku_id = any (v_used) and r.col = 'primary_supplier' and (e.state in ('made', 'checked') or (e.state = 'declared' and r.reserved));
    if v_bad is not null then raise exception 'csv_issued: % の代表の仕入先が入った NE に取り込む CSV が出ている', v_bad using errcode = 'P0001'; end if;
    -- 🆕 0093: 付け替える商品の NE 登録の CSV を配った後でも付け替える (0053 の reg_csv_issued の断りを外した・作っただけのファイルは下で使わないにする)
  end if;
  perform ops.open_reg_write('supplier_deactivate', p_request_id, p_actor_id, p_reason, p_ownership, null, null, null,
    ops.reg_hash(pg_catalog.jsonb_build_object('op', 'supplier_deactivate', 'supplier_id', v_id, 'reassign_to', v_to_id, 'sku_ids', v_used, 'reason', p_reason)),
    pg_catalog.jsonb_build_object('supplier_id', v_id::text, 'reassign_to', v_to_id::text, 'sku_ids', pg_catalog.to_jsonb(v_used)));
  if pg_catalog.cardinality(v_used) > 0 then
    v_sup := ops.ne_reg_supersede_built(v_used, p_actor_id, '代表の仕入先');
    update core.supplier_skus set is_primary = false where supplier_id = v_id and sku_id = any (v_used);
    insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id)
      select 1, v_to_id, u, true, 'human', p_actor_id from pg_catalog.unnest(v_used) u order by u
      on conflict (supplier_id, sku_id) do update set is_primary = true;
  end if;
  update core.suppliers set active = false where supplier_id = v_id;
  v_result := pg_catalog.jsonb_build_object('ok', true, 'code', v_code, 'active', false, 'reassigned', pg_catalog.to_jsonb(v_codes), 'reg_csv_superseded', v_sup -> 'superseded');
  perform ops.close_reg_write(v_result, v_code);
  return v_result;
end $$;
revoke all on function ops.deactivate_supplier(uuid, text, text, jsonb, text, text) from public;

-- ═══════════ 5. 画面のロールの直接の書き込みの守り (0053 の版から) ═══════════
-- 🆕 0093: 断るのは作っただけ (built) の NE 登録の CSV の商品だけ (保存の流れは先に ops.ne_reg_guard_on_save で使わないにする = 呼び忘れの守り)。配った後は直せる
create or replace function ops.guard_reg_csv_live() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_old     jsonb;
  v_new     jsonb;
  v_row     jsonb;
  v_skus    bigint[] := '{}';
  v_kind    text;
  v_cols    text[];
  v_codes   text;
  v_states  text[] := array['built'];   -- 🆕 0093: 断る品目の状態 (セットの構成の依頼だけ配った後も)
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  if tg_op in ('UPDATE', 'DELETE') then v_old := pg_catalog.to_jsonb(old); end if;
  if tg_op in ('INSERT', 'UPDATE') then v_new := pg_catalog.to_jsonb(new); end if;
  v_row := coalesce(v_new, v_old);
  if v_tbl = 'core.skus' then
    if tg_op = 'UPDATE' then
      v_kind := v_new ->> 'sku_kind';
      v_cols := case when v_kind = 'set' then array['name', 'standard_price_jpy', 'tax_rate', 'tax_class'] else array['name', 'handling', 'standard_price_jpy', 'tax_rate', 'tax_class'] end;
      if exists (select 1 from pg_catalog.unnest(v_cols) c where (v_old -> c) is distinct from (v_new -> c)) then
        v_skus := array[(v_new ->> 'sku_id')::bigint];
        if v_kind = 'single' and ((v_old -> 'tax_rate') is distinct from (v_new -> 'tax_rate') or (v_old -> 'tax_class') is distinct from (v_new -> 'tax_class')) then
          v_skus := v_skus || coalesce((select pg_catalog.array_agg(c.parent_sku_id) from core.sku_components c where c.child_sku_id = (v_new ->> 'sku_id')::bigint), '{}')
                           || coalesce((select pg_catalog.array_agg(q.set_sku_id) from ops.sku_component_requests q, pg_catalog.jsonb_array_elements(q.rows) e
                                         where q.status = 'open' and (e ->> 'sku_id')::bigint = (v_new ->> 'sku_id')::bigint), '{}');
        end if;
      end if;
    end if;
  elsif v_tbl = 'core.products' then
    if tg_op = 'UPDATE' and (v_old -> 'parent_product_id') is distinct from (v_new -> 'parent_product_id') then
      select coalesce(pg_catalog.array_agg(k.sku_id), '{}') into v_skus from core.skus k where k.product_id = (v_new ->> 'product_id')::bigint;
    end if;
  elsif v_tbl = 'core.supplier_skus' then
    if coalesce((v_old ->> 'is_primary')::boolean, false) or coalesce((v_new ->> 'is_primary')::boolean, false) then
      if tg_op <> 'UPDATE' or (v_old -> 'is_primary') is distinct from (v_new -> 'is_primary') or (v_old -> 'supplier_id') is distinct from (v_new -> 'supplier_id') then
        v_skus := array[(v_row ->> 'sku_id')::bigint];
      end if;
    end if;
  elsif v_tbl = 'core.sku_costs' then
    if exists (select 1 from core.skus k where k.sku_id = (v_row ->> 'sku_id')::bigint and k.sku_kind = 'single') then v_skus := array[(v_row ->> 'sku_id')::bigint]; end if;
  elsif v_tbl = 'ops.sku_component_requests' then
    v_skus := array[(v_row ->> 'set_sku_id')::bigint];
    v_states := array['built', 'issued', 'import_declared', 'partial'];   -- 🆕 0093: 構成は確かめが終わるまで (0053 のまま)
  end if;
  if pg_catalog.cardinality(v_skus) > 0 then
    select pg_catalog.string_agg(distinct s.code, '・') into v_codes
      from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id
     where i.sku_id = any (v_skus) and i.state = any (v_states);   -- 🆕 0093: 作っただけ (built) だけ。配った後 (issued / import_declared / partial) は断らない (構成の依頼を除く)
    if v_codes is not null and v_tbl = 'ops.sku_component_requests' then
      raise exception 'reg_csv_issued: 新商品の NE 登録の CSV が出ているセット (%) の構成の依頼は、確かめが終わるまで直せない (%)', v_codes, v_tbl using errcode = '42501';
    end if;
    if v_codes is not null then
      raise exception 'reg_csv_built: 作っただけの NE 登録の CSV に入っている商品 (%) の NE に送る欄は、先にそのファイルを使わないにしてから直す (保存の流れは ops.ne_reg_guard_on_save が先に使わないにする・%)', v_codes, v_tbl using errcode = '42501';
    end if;
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_reg_csv_live() from public;
