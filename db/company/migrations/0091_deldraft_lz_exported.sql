-- 0091 (0090 = 表示の項目 #1748 の次。migrate は欠番を断る): ロジザード登録の CSV に出した新商品の下書きは「下書きを消す」で消さない
--   (2026-10-18・中原さんの決定「ロジザード登録の CSV に出した (issued) ものは消せないようにする」)
-- 前提 = 0071 (ops.sku_draft_cancel_check・ops.cancel_sku_draft)・0073 (ops.cancel_variation_child の今の版)・0081 (ops.lz_new_export_items)。
--   0074〜0085 はこの 2 つの関数を書き換えていない (試験 scripts/test-master-draft-cancel.mjs [M4] で確かめる)
--
-- なぜ: 0071 は「ロジザードの商品マスタへ出す CSV は NE にある商品だけ = NE に配る前の下書きは出さない」を前提にしていた。
--   0081 (#1721) から、ポータルのロジザード登録の CSV (lib/lz-new-export.mjs) は新商品の登録から来た単品 (origin = new_entry・やめていない) を
--   NE に配る前の下書きのうちから出す = 下書きを消すと、ロジザードにだけ商品が残る (Company DB では「やめた」・ロジザードには登録の CSV で入った)
-- なにを:
--   1. ops.sku_draft_cancel_check (0071 の版を写して 🆕 0091 の行だけ足した): ロジザード登録の CSV に出した (品目が issued) = 消さない (理由 lz_issued・ファイルの番号)。
--      取り消した (voided) ファイルの品目は数えない = 取り消せば (ロジザードから消した後) また消せる。ops.cancel_sku_draft はこの確かめを鍵の後に呼ぶ = 書き換えない
--   2. ops.cancel_variation_child (0073 の版を写して 🆕 0091 の行だけ足した): まとまりの子の廃止も同じ決まり (lz_issued で断る)
-- 鍵 (競り): CSV を出す関数 (ops.lz_new_export_issue) = 要求の鍵 → SKU の鍵 (core.sku:<id>・sku_id の順) → 値の鍵 → 鍵の後に「やめていない」を読む。
--   消す (ops.cancel_sku_draft) = 段階・マスタの書き込み (共有) → SKU の鍵 (ops.ne_reg_lock_skus) → … → 鍵の後にこの確かめ。
--   子の廃止 = SKU の鍵 → … → 鍵の後に lz_issued を読む。どちらも同じ SKU の鍵を行の鍵より先に取る・CSV を出す関数はほかの鍵 (段階・NE の CSV・NE のコード・まとまり) を取らない
--   = 待ちの輪ができない。後から来た方は先の commit を待ち、その後の文で先の結果を読む (read committed) = 片方だけが通る
-- 変えない: ops.cancel_sku_draft (確かめを呼ぶだけ)・CSV を出す関数・取り消す関数・0070 の「NE から消えた」・0072 の「NE から消す」(NE にある商品 = 下書きではない)
-- 🚨 この migration は値を何も変えない (関数を作り直すだけ)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す

-- ═══════════ 1. 消してよいかの確かめ (0071 の版から写して 🆕 0091 の行だけ足した) ═══════════
create or replace function ops.sku_draft_cancel_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  o        record;
  v_p      jsonb := '[]'::jsonb;
  v_txt    text;
  v_csv    jsonb;
  v_card   jsonb;
  v_jans   jsonb;
  v_req    bigint;
  v_alone  boolean;
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.product_id, p.parent_product_id, r.state as reg into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join ops.master_registrations r on r.sku_id = k.sku_id
   where k.sku_id = p_sku_id;
  if not found then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ok', false,
      'problems', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_sku', 'text', 'Company DB にその SKU が無い')));
  end if;
  if s.company_id <> 1 then v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_company', 'text', '会社 1 の SKU でない')); end if;
  if s.reg is distinct from 'draft' then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_draft',
      'text', pg_catalog.format('登録の状態が %s (消せるのは新商品の登録の下書きだけ)', coalesce(s.reg, '行が無い'))));
  end if;
  if ops.sku_ever_issued(s.sku_id) then
    select pg_catalog.string_agg(distinct '#' || e.export_id::text, '・') into v_txt
      from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id where i.sku_id = s.sku_id and e.issued_at is not null;
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'ever_issued',
      'text', pg_catalog.format('NE 登録の CSV (ファイル %s) を一度配った = NE に入ったかもしれない (消さない)', v_txt)));
  end if;
  -- 🆕 0091 ここから: ロジザード登録の CSV (0081 の ops.lz_new_export_items) に出した (issued) 下書きは消さない (中原さんの決定 2026-10-18)。
  --   消すとロジザードにだけ商品が残る = 先にロジザードから消す。取り消した (voided) ファイルだけなら消せる。出した品目は SKU ごとに 1 つ (ux_lz_new_items_issued_sku)。
  --   CSV を出す関数 (ops.lz_new_export_issue) は SKU の鍵の下で「やめていない」を確かめる・消す関数はこの確かめを SKU の鍵の後に呼ぶ = 同時でも片方だけが通る
  select pg_catalog.string_agg('#' || i.export_id::text, '・' order by i.export_id) into v_txt
    from ops.lz_new_export_items i where i.sku_id = s.sku_id and i.state = 'issued';
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'lz_issued',
      'text', pg_catalog.format('ロジザード登録の CSV に出しています (ファイル %s)。ロジザードから消すのが先です (ロジザードから消した後、ロジザード登録の CSV の画面でそのファイルを取り消すと消せます)', v_txt)));
  end if;
  -- 🆕 0091 ここまで
  if ops.ne_code_seen(s.code_norm) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'seen_in_ne', 'text', 'NE に一度でも現れたコード (NE にある商品を隠さない)'));
  end if;
  if s.parent_product_id is not null and s.parent_product_id <> s.product_id then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'variation_child',
      'text', '色違い・サイズ違いのまとまりの子 (「このまとまりから外す (廃止)」で消す = まとまりのカードも直る)'));
  end if;
  if s.product_id is not null and exists (select 1 from core.products c where c.parent_product_id = s.product_id and c.product_id <> s.product_id) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'parent_of_others', 'text', 'この商品がほかの商品の代表 (親) になっている'));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where c.child_sku_id = s.sku_id order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'set_component', 'text', pg_catalog.format('ほかのセットの構成品になっている (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select distinct p.code from ops.sku_component_requests q join core.skus p on p.sku_id = q.set_sku_id
           where q.status = 'open' and q.set_sku_id <> s.sku_id
             and exists (select 1 from pg_catalog.jsonb_array_elements(q.rows) e where (e ->> 'sku_id') = s.sku_id::text) order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'component_request', 'text', pg_catalog.format('ほかのセットの開いている構成の依頼に入っている (セット %s = 先にそのセットを直すか消す)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status <> 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component', 'text', pg_catalog.format('販路の出品の構成に使われている (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status = 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component_deleted',
      'text', pg_catalog.format('削除済みの出品 %s の構成に入っている (出品が戻ると消した商品が使われる = 先にその出品の構成から外す)', v_txt)));
  end if;
  -- カードの知らせ (SKU の知らせ = card_create)。取り込み中 (借りの期限の中) = 今は消さない (取り込みが SQLite にカードを作っている途中かもしれない)
  select x.event_id, x.status, x.result, x.leased_until into o from ops.product_hub_outbox x where x.sku_id = s.sku_id and x.kind = 'card_create';
  if o.event_id is not null then
    v_card := pg_catalog.jsonb_build_object('event_id', o.event_id::text, 'status', o.status, 'outcome', o.result ->> 'outcome', 'draft_id', o.result -> 'draft_id');
    if o.status <> 'done' and o.leased_until is not null and o.leased_until > pg_catalog.now() then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'card_busy',
        'text', 'product-hub のカードをちょうど作っているところ (少し待ってからもう一度)'));
    end if;
  end if;
  -- 片付ける物の見込み
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('export_id', f.export_id::text,
           'codes', coalesce((select pg_catalog.jsonb_agg(k.code order by k.code_norm) from ops.ne_reg_export_items j join core.skus k on k.sku_id = j.sku_id
                               where j.export_id = f.export_id and j.sku_id <> s.sku_id and j.state = 'built'), '[]'::jsonb)) order by f.export_id), '[]'::jsonb)
    into v_csv
    from (select distinct i.export_id from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state = 'built') f;
  v_alone := s.product_id is not null and not exists (select 1 from core.skus k where k.product_id = s.product_id and k.sku_id <> s.sku_id);
  select coalesce(pg_catalog.jsonb_agg(e.external_value order by e.external_value), '[]'::jsonb) into v_jans
    from core.external_ids e
   where v_alone and e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null;
  select q.component_request_id into v_req from ops.sku_component_requests q where q.set_sku_id = s.sku_id and q.status = 'open';
  return pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ok', pg_catalog.jsonb_array_length(v_p) = 0, 'problems', v_p,
    'reg_state', s.reg, 'sku_kind', s.sku_kind,
    'cleanup', pg_catalog.jsonb_build_object('reg_csv', v_csv, 'card', v_card, 'jans', v_jans, 'component_request', case when v_req is null then null else v_req::text end));
end $$;
revoke all on function ops.sku_draft_cancel_check(bigint) from public;

-- ═══════════ 2. まとまりの子の廃止 (0073 の版から写して 🆕 0091 の行だけ足した) ═══════════
create or replace function ops.cancel_variation_child(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_s      record;
  v_prev   jsonb;
  v_hash   text;
  v_bump   jsonb;
  v_result jsonb;
  v_txt    text;   -- 🆕 0073
  v_lz     bigint;   -- 🆕 0091
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
  -- 🆕 0091 ここから: ロジザード登録の CSV (0081) に出した (issued) 子は廃止しない = 下書きを消す (ops.sku_draft_cancel_check の lz_issued) と同じ決まり。
  --   取り消した (voided) ファイルだけなら廃止できる。SKU の鍵 (上) の後に読む = CSV を出す関数 (SKU の鍵の下で「やめていない」を確かめる) と同時でも片方だけが通る
  select i.export_id into v_lz from ops.lz_new_export_items i where i.sku_id = p_sku_id and i.state = 'issued' order by i.export_id limit 1;
  if v_lz is not null then
    raise exception 'lz_issued: % はロジザード登録の CSV に出しています (ファイル #%)。ロジザードから消すのが先です (ロジザードから消した後、ロジザード登録の CSV の画面でそのファイルを取り消すと廃止できます)',
      v_s.code, v_lz using errcode = 'P0001';
  end if;
  -- 🆕 0091 ここまで
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
revoke all on function ops.cancel_variation_child(uuid, text, text, jsonb, bigint) from public;
