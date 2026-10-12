-- 0072 (仮の番号): Amazon SKU の対応の構成品に、まだ NE に無い新商品 (下書き draft・NE 登録待ち ne_pending) も使えるようにする (2026-10-12・中原さん
--       「ここで NE にないと登録できないっていう制限は不要。だって登録が一日遅れになってしまうからね」)
-- 🚨 番号は仮 = 今の master の次 (欠番は migrate が断るので 0074 にしておけない)。#1690 (0072)・#1691 (0073) が先にマージされたら 0074 に付け替える (中身の 🆕 0074 の印はそのまま)。前提 = 0054 (ops.amazon_map_components)・0067 (ops.cancel_variation_child)。0072 / 0073 の物は使わない
--
-- なぜ: 新商品は 登録 (下書き) → NE 登録の CSV を配る (NE 登録待ち) → 次の朝の NE の取得で NE 確認済み、の順。Amazon の対応は NE 確認済みからしか作れなかった
--   = Amazon に出す SKU の対応が 1 日遅れる。どの読み手も構成品が NE にあるかを確かめていない (止まらない・下は調べた結果):
--   ・⑦-2 の写し (古い SKU マスタ m_sku_components・Render の写し) = 形 (validateSkuMap) だけを見る。NE のコードはそのまま写る
--   ・FBA 補充 = 倉庫 (ロジザード) に行が無い構成品は在庫 0 で保留 (warehouse_unknown・SKU ごと)。全体は止めない
--   ・売上の日次 = core.skus の行で構成に展開する (登録の状態で絞らない)。f_sales (miniPC) は商品管理リストに無いコードの「直接販売」になる (毎回作り直す = NE に入れば直る)
--   ・夜間ロード = 対応のある出品の構成は作らない・直さない・NE と照らさない
-- なにを:
--   1. ops.amazon_map_components (0054 の版を写して 🆕 0074 の所だけ変えた): 使える登録の状態 = draft・ne_pending・ne_confirmed・distributable・available。
--      断るのは今のまま = 無い SKU・コードが違う・例外の SKU・登録の状態が無い (切替の前)・要確認 (quarantined = 夜間ロードが NE で見つけた知らない商品 = 人が確かめる前)・
--      やめた (cancelled)。画面の候補・保存の前の確かめ (lib/amazon-map-write.mjs の componentUnusable) も同じ決まり
--   2. ops.cancel_variation_child (0067 の版を写して 🆕 0074 の所だけ足した): 販路の出品の構成に使われている子は廃止しない (listing_component)。
--      0067 は出品の構成を見なかった = 下書き・NE 登録待ちの子が Amazon の構成に入れるようになると、子を廃止して有効な対応に「やめた」構成品が残る穴になる。
--      0071 の下書きを消す (ops.sku_draft_cancel_check) は前から出品の構成 (削除済みの出品も) に使われていれば断る = 同じ決まりにそろえた
-- 変えない: 0070 の「NE から消えた」(available だけが対象・出品の構成に使われていれば断る)・0070 の参照の守り (やめた SKU を新しく構成に入れない)

-- ═══════════ 1. Amazon SKU の対応の構成品 = まだ NE に無い新商品も ═══════════
-- 構成品の形を確かめて、DB の値で作り直す (0054 の説明のまま)。使える登録の状態 = 下書き・NE 登録待ち・NE 確認済み以降 (🆕 0074)
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
  -- 🆕 0074: 使える登録の状態に 下書き (draft)・NE 登録待ち (ne_pending) を足した (下の行の not in の 2 つ。ほかは 0054 の版のまま)
            from f where f.k_code is null or f.k_code <> f.code or f.sku_kind not in ('single', 'set') or f.state is null or f.state not in ('draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available')),
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
  v_txt    text;   -- 🆕 0074
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
  -- 🆕 0074 ここから: 販路の出品の構成 (Amazon SKU の対応など。削除済みの出品も) に使われている子は廃止しない = 0071 の下書きを消す (listing_component・
  --   listing_component_deleted) と同じ決まり。0074 からまだ NE に無い商品 (下書き・NE 登録待ち) も Amazon の構成に使える = 廃止すると有効な対応に「やめた」構成品が残る
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select distinct l.mall || ':' || l.listing_code || case when l.status = 'deleted' then ' (削除済みの出品)' else '' end as w
            from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = p_sku_id order by 1 limit 6) x;
  if v_txt is not null then
    raise exception 'listing_component: % は販路の出品の構成に使われている (%) = 先にその出品の構成から外す', v_s.code, v_txt using errcode = 'P0001';
  end if;
  -- 🆕 0074 ここまで
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
