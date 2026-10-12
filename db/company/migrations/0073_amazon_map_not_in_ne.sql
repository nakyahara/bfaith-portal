-- 0073: Amazon SKU の対応の構成品に、まだ NE に無い新商品 (下書き draft・NE 登録待ち ne_pending) と、NE で直接作った商品 (要確認 quarantined) も使えるようにする
--       (2026-10-12・中原さん「ここで NE にないと登録できないっていう制限は不要。だって登録が一日遅れになってしまうからね」・要確認も使う = B)
-- 前提 = 0054 (ops.amazon_map_components)・0067 (ops.cancel_variation_child)。0072 (#1690・NE から消す CSV) はこの 2 つの関数を書き換えていない (試験 [15] で確かめる)。
--   0072 の参照の守り (ops.guard_sku_ref_not_cancelled = やめた・NE から消す依頼の途中の SKU を出品の構成に新しく入れない) はそのまま効く
--
-- なぜ: 新商品は 登録 (下書き) → NE 登録の CSV を配る (NE 登録待ち) → 次の朝の NE の取得で NE 確認済み、の順。Amazon の対応は NE 確認済みからしか作れなかった
--   = Amazon に出す SKU の対応が 1 日遅れる。要確認 = 夜間ロードが NE で見つけた、ポータルで作っていない商品 (NE にある)。人が代表の採用の画面で確かめる前でも、
--   Amazon の構成には使ってよい (中原さん)。どの読み手も構成品が NE にあるかを確かめていない (止まらない・下は調べた結果):
--   ・⑦-2 の写し (古い SKU マスタ m_sku_components・Render の写し) = 形 (validateSkuMap) だけを見る。NE のコードはそのまま写る
--   ・FBA 補充 = 倉庫 (ロジザード) に行が無い構成品は在庫 0 で保留 (warehouse_unknown・SKU ごと)。NE 受注 CSV は NE の商品の写しに無いコードの SKU を出さない
--   ・売上の日次 = core.skus の行で構成に展開する (登録の状態で絞らない)。f_sales (miniPC) は商品管理リストに無いコードの「直接販売」になる (毎回作り直す = NE に入れば直る)
--   ・夜間ロード = 対応のある出品の構成は作らない・直さない・NE と照らさない
-- なにを:
--   1. ops.amazon_map_components (0054 の版を写して 🆕 0073 の所だけ変えた): 使える登録の状態 = draft・ne_pending・ne_confirmed・distributable・available・quarantined。
--      断るのは今のまま = 無い SKU・コードが違う・例外の SKU・登録の状態が無い (切替の前)・やめた (cancelled)。画面の候補・保存の前の確かめ
--      (lib/amazon-map-write.mjs の componentUnusable) も同じ決まり
--   2. ops.cancel_variation_child (0067 の版を写して 🆕 0073 の所だけ足した): 販路の出品の構成に使われている子は廃止しない (listing_component)。
--      0067 は出品の構成を見なかった = 下書き・NE 登録待ちの子が Amazon の構成に入れるようになると、子を廃止して有効な対応に「やめた」構成品が残る穴になる。
--      0071 の下書きを消す (ops.sku_draft_cancel_check)・0072 の NE から消す (ops.ne_del_check) は前から出品の構成 (削除済みの出品も) に使われていれば断る = 同じ決まりにそろえた
--   3. ops.transition_sku_registration (0071 の版を写して 🆕 0073 の所だけ足した): 要確認 → やめた (人の理由) は、販路の出品の構成に使われていれば断る (listing_component)。
--      要確認をやめる道 (運用のロールが直に呼ぶ) は今まで出品の構成を見なかった = 要確認が Amazon の構成に入れるようになると、有効な対応に「やめた」構成品が残る穴
-- 変えない: 0070 の「NE から消えた」(available だけが対象・出品の構成に使われていれば断る)・0070 / 0072 の参照の守り

-- ═══════════ 1. Amazon SKU の対応の構成品 = まだ NE に無い新商品も ═══════════
-- 構成品の形を確かめて、DB の値で作り直す (0054 の説明のまま)。使える登録の状態 = 下書き・NE 登録待ち・NE 確認済み以降・要確認 (🆕 0073)
create or replace function ops.amazon_map_components(p_rows jsonb) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_n    integer;
  v_ids  bigint[];
  v_bad  text;
  v_rows jsonb;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then raise exception 'invalid_value: 構成品 (components) は配列' using errcode = '22023'; end if;
  v_n := jsonb_array_length(p_rows);
  if v_n < 1 or v_n > 20 then raise exception 'invalid_value: 構成品は 1〜20 行 (% 行)', v_n using errcode = '22023'; end if;
  if exists (select 1 from jsonb_array_elements(p_rows) e
              where jsonb_typeof(e) <> 'object'
                 or exists (select 1 from jsonb_object_keys(e) k where k not in ('sku_id', 'code', 'qty'))
                 or jsonb_typeof(e -> 'sku_id') is distinct from 'number' or (e ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$'
                 or jsonb_typeof(e -> 'code') is distinct from 'string'
                 or jsonb_typeof(e -> 'qty') is distinct from 'number' or (e ->> 'qty') !~ '^[1-9][0-9]{0,2}$') then
    raise exception 'invalid_value: 構成品の行は { sku_id, code, qty (1〜999) } だけ' using errcode = '22023';
  end if;
  select array_agg(distinct (e ->> 'sku_id')::bigint order by (e ->> 'sku_id')::bigint) into v_ids from jsonb_array_elements(p_rows) e;
  if cardinality(v_ids) <> v_n then raise exception 'invalid_value: 構成品が重なっている (同じ品は 1 行にして数量で)' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || x::text, 0)) from unnest(v_ids) as t(x) order by x;
  perform 1 from core.skus where sku_id = any(v_ids) order by sku_id for share;
  with r as (
    select e.ord, (e.v ->> 'sku_id')::bigint as sku_id, e.v ->> 'code' as code, (e.v ->> 'qty')::integer as qty
      from jsonb_array_elements(p_rows) with ordinality as e(v, ord)),
  f as (select r.*, k.code as k_code, k.sku_kind, reg.state from r left join core.skus k on k.sku_id = r.sku_id left join ops.master_registrations reg on reg.sku_id = r.sku_id)
  select (select string_agg(format('%s (%s)', f.code, case when f.k_code is null then '無い SKU' when f.k_code <> f.code then 'コードが違う'
                                                          when f.sku_kind not in ('single', 'set') then '例外の SKU' else coalesce('状態 ' || f.state, '登録の状態が無い') end), '・' order by f.ord)
  -- 🆕 0073: 使える登録の状態に 下書き (draft)・NE 登録待ち (ne_pending)・要確認 (quarantined) を足した (下の行の not in の 3 つ。ほかは 0054 の版のまま)
            from f where f.k_code is null or f.k_code <> f.code or f.sku_kind not in ('single', 'set') or f.state is null or f.state not in ('draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined')),
         (select jsonb_agg(jsonb_build_object('sku_id', f.sku_id, 'code', f.k_code, 'qty', f.qty, 'sort_order', f.ord - 1) order by f.ord) from f)
    into v_bad, v_rows;
  if v_bad is not null then raise exception 'component_unusable: 構成品に使えない商品がある: %', v_bad using errcode = 'P0001'; end if;
  return v_rows;
end $$;

-- ═══════════ 2. 子の廃止 = 出品の構成に使われていれば断る ═══════════
create or replace function ops.cancel_variation_child(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_s      record;
  v_prev   jsonb;
  v_hash   text;
  v_bump   jsonb;
  v_result jsonb;
  v_txt    text;   -- 🆕 0073
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or coalesce(pg_catalog.length(pg_catalog.btrim(p_reason)), 0) = 0 then
    raise exception 'invalid_input: 廃止は人が理由 (200 字まで) を書いてだけ' using errcode = '22023';
  end if;
  perform ops._variation_gate(p_ownership, array['products.parent']);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'variation_child_cancel', 'sku_id', p_sku_id, 'reason', p_reason));
  v_prev := ops._variation_replay(p_request_id, 'variation_child_cancel', p_actor_id, v_hash, p_sku_id, null);
  if v_prev is not null then return v_prev; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  -- 🆕 #1677 Codex R1 High: NE のコードの共有の鍵 (SKU の後・まとまりのコード / まとまりの前) = 「NE に一度も現れていない」を鍵の後に読み、commit まで照合に入れ替えさせない
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  select s.sku_id, s.code, s.code_norm, s.sku_kind, s.product_id, p.parent_product_id, r.state as reg into v_s
    from core.skus s left join core.products p on p.product_id = s.product_id left join ops.master_registrations r on r.sku_id = s.sku_id
   where s.sku_id = p_sku_id and s.company_id = 1;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if v_s.sku_kind <> 'single' or v_s.parent_product_id is null then raise exception 'not_variation_child: % はまとまりの子でない', v_s.code using errcode = 'P0001'; end if;
  perform ops._variation_lock_existing_group(v_s.parent_product_id);   -- まとまりのコード → まとまり (SKU の後)
  if v_s.reg is null or v_s.reg not in ('draft', 'ne_pending') then
    raise exception 'child_not_cancellable: % の登録の状態が % (廃止できるのは下書き・NE 登録待ちの子)', v_s.code, coalesce(v_s.reg, 'なし') using errcode = 'P0001';
  end if;
  if exists (select 1 from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state in ('built', 'issued', 'import_declared', 'partial')) then
    raise exception 'live_file: % には終わっていない NE 登録の CSV がある (配っていない = 使わないにする・配った = 申告して確かめを待つ)', v_s.code using errcode = 'P0001';
  end if;
  if ops.ne_code_seen(v_s.code_norm) then
    raise exception 'seen_in_ne: % は NE に一度でも現れた = 廃止しない (NE にある商品を隠さない)', v_s.code using errcode = 'P0001';
  end if;
  -- 🆕 0073 ここから: 販路の出品の構成 (Amazon SKU の対応など。削除済みの出品も) に使われている子は廃止しない = 0071 の下書きを消す (listing_component・
  --   listing_component_deleted) と同じ決まり。0073 からまだ NE に無い商品 (下書き・NE 登録待ち) も Amazon の構成に使える = 廃止すると有効な対応に「やめた」構成品が残る
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select distinct l.mall || ':' || l.listing_code || case when l.status = 'deleted' then ' (削除済みの出品)' else '' end as w
            from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = p_sku_id order by 1 limit 6) x;
  if v_txt is not null then
    raise exception 'listing_component: % は販路の出品の構成に使われている (%) = 先にその出品の構成から外す', v_s.code, v_txt using errcode = 'P0001';
  end if;
  -- 🆕 0073 ここまで
  perform ops._open_variation_write('variation_child_cancel', p_request_id, p_actor_id, p_reason, p_ownership, p_sku_id, '{}'::bigint[], v_hash,
    pg_catalog.jsonb_build_object('group_product_id', v_s.parent_product_id::text));
  perform ops._variation_group_resolve(v_s.parent_product_id, p_actor_id, 'load', false);
  perform ops.transition_sku_registration(p_sku_id, 'cancelled', 'human', p_actor_id, p_reason, '{}'::jsonb, p_request_id::text);
  v_bump := ops._variation_bump(v_s.parent_product_id, p_request_id, p_actor_id);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', v_s.code, 'sku_id', p_sku_id::text, 'state', 'cancelled',
    'group_product_id', v_s.parent_product_id::text, 'revision', v_bump -> 'revision', 'event_id', v_bump -> 'event_id');
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  perform ops.close_reg_write(v_result, v_s.code);
  return v_result;
end $$;

-- ═══════════ 3. 要確認をやめる = 出品の構成に使われていれば断る ═══════════
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
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;   -- 🆕 0071
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
    -- 🆕 0071 ここから: 下書き・NE 登録待ち → やめた = 下書きを消す (ops.cancel_sku_draft・約束 sku_draft_cancel)・まとまりの子の廃止 (ops.cancel_variation_child・約束 variation_child_cancel)
    --   の中だけ (同じ SKU・人・request_id)。運用のロール (master_ops) が直に呼んでも断る = 消せる条件 (一度配った・NE に現れた・使われている) と片付けを迂回しない。
    --   表の持ち主のロール (とそのメンバー = migration・手の復旧) は約束を求めない (表を直接書けるロール = 守りの境界はログインのロール)
    if r.state in ('draft', 'ne_pending')
       and not pg_catalog.pg_has_role(v_db_user, (select c.relowner from pg_catalog.pg_class c where c.oid = 'ops.master_registrations'::pg_catalog.regclass), 'MEMBER') then
      v_sess := ops.current_master_write_session();
      if v_sess.session_id is null or v_sess.operation not in ('sku_draft_cancel', 'variation_child_cancel') or v_sess.sku_id is distinct from p_sku_id
         or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id then
        raise exception 'draft_cancel_session_required: 下書き・NE 登録待ちをやめるのは「下書きを消す」(ops.cancel_sku_draft)・「子の廃止」(ops.cancel_variation_child) の中だけ' using errcode = '42501';
      end if;
    end if;
    -- 🆕 0071 ここまで
    -- 🆕 0073 ここから: 要確認 (NE で直接作った商品) → やめた = 販路の出品の構成 (Amazon SKU の対応など・削除済みの出品も) に使われていれば断る。
    --   0073 から要確認も Amazon の構成に使える = やめると有効な対応に「やめた」構成品が残る (0070 の NE から消えた・0071 の下書きを消す・0072 の NE から消す・
    --   0073 の子の廃止と同じ決まり)。先にその出品の構成から外す
    if r.state = 'quarantined' and exists (select 1 from core.listing_components lc where lc.sku_id = p_sku_id) then
      raise exception 'listing_component: 要確認の商品 % は販路の出品の構成に使われている (%) = 先にその出品の構成から外す',
        (select k.code from core.skus k where k.sku_id = p_sku_id),
        (select pg_catalog.string_agg(x.w, '・' order by x.w) from (select distinct l.mall || ':' || l.listing_code as w from core.listing_components lc
            join core.listings l on l.listing_id = lc.listing_id where lc.sku_id = p_sku_id order by 1 limit 6) x)
        using errcode = 'P0001';
    end if;
    -- 🆕 0073 ここまで
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
