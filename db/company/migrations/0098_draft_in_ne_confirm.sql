-- 0098: 「NE にあるので登録済みにする」= 新商品の下書き (NE 登録待ち) で、NE にもう同じ商品がある (NE で先に登録した・CSV の値を NE で直した) ものを NE 確認済みにする DB の道
--   (2026-10-20 中原さんの承認。本番 10/20 = chinaseima・guargum50 (CSV にしていない)・calculationwb・bikecover-BK / -SV・nanairos-yo (CSV の品目が一部違う partial のまま) が下書きで止まっている)
-- 🚨 前提 = 0079 (ops.transition_sku_registration・確かめ待ちの view・約束の操作の CHECK の今の版)・0072 (ops.master_write_allowed の今の版)・0070 (地図の今の版)・
--    0094 (ops.ne_upd_units の今の版)・0095 (代表の門の部品 ops._parent_gate_resolved)・0053 / 0063 / 0065 (NE 登録の CSV・照合 ② の観測)
--
-- なぜ: 下書き → NE 確認済みの道は「NE 登録の CSV の品目が翌朝の照合で全部の列が合った (verified)」だけ。
--   ・CSV にしていない下書きで、NE に同じコードの商品がもうある = CSV にできない (already_in_ne)・照合の確かめも無い = いつまでも下書き
--   ・CSV を配った後に Company DB の値を直した (0093 で許した) = 品目の期待値 (配った値) が NE と合わない = いつまでも partial = 下書き
--   下書きの間は「NE へ送る」(0094) も新商品の途中 (new_product) として差を出さない = NE とちがう名前も直せない
-- 決まり (中原さんの承認):
--   NE の今朝の完全な取得にそのコードがある (大文字・小文字も同じ)・区分 (単品 / セット) が同じ・生きている NE 登録の CSV の品目は、解決する一部違う (partial) の 1 つだけ
--   (それは消さずに「使わない」(superseded・理由 = 人が決めた resolved_by_human) にして記録を残す)・代表 (色違いのまとまり) は NE と同じ
--   (代表の門の「直った」と同じ決まり = ops._parent_gate_resolved。違えば断る = 先に代表をそろえる)・セットの中身は NE と同じ (NE へ送るでは直せない)。
--   ロジザードのコード (0078 / 0080 / 0081) には触らない。登録済みにした後は ふつうの NE 確認済みの商品と同じ = Company DB と NE の違い (名前ほか) は「NE へ送る」に出る
-- なにを:
--   1. 照合 ② の確かめ待ち (ops.v_ne_reg_targets) に「下書き・NE 登録待ちで、NE のコードの台帳にもうある (生きている CSV の品目が無い)」を足す
--      (0079 の版を写して枝を 1 つ足しただけ) = 翌朝の照合 ② から、その商品の NE の観測 (区分・代表・セットの中身) が残る。
--      🚨 CSV にしていない下書き (chinaseima・guargum50) は、この migration の後の最初の照合 ② の後から登録済みにできる (今はまだ観測が無い)
--   2. 地図 (ops.registration_transition_allowed・0070 の版) に draft > ne_confirmed を 1 行 (下の 5. が約束と確かめを求める)
--   3. 約束の操作 (ops.master_write_sessions・ops.master_edit_requests の CHECK・0079 の版) に draft_ne_confirm・
--      書いてよい表 (ops.master_write_allowed・0072 の版) に (draft_ne_confirm, ops.ne_reg_export_items, UPDATE) = 一部違う品目を「使わない」にするだけ
--   4. ops.draft_in_ne_check(SKU) = 登録済みにしてよいかの表 (読むだけ・security definer・鍵を取らない)。
--      { sku_id, code, sku_kind, reg_state, in_ne, ok, problems: [{ reason, text }], item (解決する一部違う品目), diffs (NE と違う欄 = 「NE へ送る」に出る), evidence, check_hash }
--   5. ops.transition_sku_registration (0079 の版を写して 🆕 0098 のかたまりだけ足した): 下書き・NE 登録待ち → NE 確認済み = 約束 draft_ne_confirm の中・同じ SKU・人・
--      request_id・人 (human) のときだけ。根拠は呼び手から受けず DB が 4. の表から作る (ok・check_hash が約束と同じ)。下書きから直に NE 確認済みは、約束の外では断る
--   6. ops.confirm_draft_in_ne(request_id, 人, SKU, check_hash) = 登録済みにする (security definer・1 件 = 1 取引・同じ request_id の押し直し = 前の答え)。
--      鍵 (今の順): 段階 (共有) → マスタの書き込み (共有) → request_id → SKU → 親子 (共有) → CSV → NE のコード (共有) → 登録の状態の行 (for update) →
--      生きている品目の行 (for update) → 構成品の登録の状態の行 (for share)。鍵の後に 4. をもう一度: ok でない = not_confirmable・check_hash が違う = stale。
--      約束 (操作 draft_ne_confirm・この関数だけが書く) → 登録の状態 (5.・履歴の根拠に品目と resolved_by_human) → 一部違う品目を superseded → 保存の記録 done
--   7. ops.ne_upd_units (0094 の版を写して 🆕 0098 の行だけ足した): 6. で登録済みにした新商品は、登録 (ポータルの INSERT) で人が入れた値も「人が入れた今の値」=
--      NE と違えば「NE へ送る」に出す (0094 は「その欄を最後に UPDATE したのが人」だけ = 登録したままの名前 (calculationwb) が出なかった)。ほかの商品は今までどおり
-- 変えない: 照合の確かめ (record_ne_registration_check)・要確認の確認 (0079)・NE 登録の CSV を作る / 配る / 申告 / 使わない・代表の門 (0095)・ロジザードの表と関数・夜間ロード
-- 🚨 この migration は商品・登録の状態・CSV の値を何も変えない (確かめ待ちの view が広がるだけ = 次の朝の照合 ② から観測が残る)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--    master_edit (画面) = 4. 6. の実行・watcher = 4. の実行 (下の do で付ける。create-master-edit-roles.mjs・create-watch-roles.mjs の流し直しでも付く)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops.quarantine_confirm_check(bigint)') is null or pg_catalog.to_regprocedure('ops.ne_upd_units(bigint[], text)') is null
     or pg_catalog.to_regprocedure('ops._parent_gate_resolved(text, text, text)') is null or pg_catalog.to_regprocedure('ops.ne_reg_canonical(bigint, date)') is null then
    raise exception '0098 の前提: 要確認の確認 (0079)・NE へ送る (0094)・代表の門 (0095)・NE 登録の CSV の期待値 (0065) が無い';
  end if;
  if pg_catalog.to_regprocedure('ops.draft_in_ne_check(bigint)') is not null then raise exception '0098: ops.draft_in_ne_check がもうある'; end if;
end $$;

-- ═══════════ 1. 照合 ② の確かめ待ちに「NE にもうある下書き」(0079 の版から写して枝を 1 つ足した) ═══════════
create or replace view ops.v_ne_reg_targets as
  select i.item_id, i.export_id, i.sku_id, i.code_norm, i.sku_kind, i.state
    from ops.ne_reg_export_items i
   where i.state in ('issued', 'import_declared', 'partial')
  union all
  -- 🆕 0079: NE で直接作られた商品 (quarantined) を全部 (単品もセットも・親の有無・products.parent の持ち主を問わず) = 翌朝の照合が NE の観測を残す
  --   (確認済みにする根拠 = ops.quarantine_confirm_check・代表の採用の根拠 = 0067 の部分集合も含む)。コードが 80 字を超える SKU は入れない (観測の表の CHECK)
  select null::bigint, null::bigint, s.sku_id, s.code_norm, s.sku_kind, 'quarantined'::text
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state = 'quarantined' and s.sku_kind in ('single', 'set') and pg_catalog.length(s.code_norm) between 1 and 80
     -- #1715 Codex R1 M2: 上の枝 (生きている NE 登録の CSV の品目) にある SKU は重ねない (同じ SKU を 2 行にしない)
     and not exists (select 1 from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('issued', 'import_declared', 'partial'))
  -- 🆕 0098 ここから: 下書き・NE 登録待ちの新商品で、NE のコードの台帳 (0041・商品のコード) にもうあるもの (NE で先に登録した・CSV の値を NE で直した) を全部
  --   (単品もセットも・親の有無を問わず) = 翌朝の照合 ② が NE の観測を残す (「NE にあるので登録済みにする」の根拠 = ops.draft_in_ne_check)。
  --   上の 2 つの枝と重ねない (生きている NE 登録の CSV の品目がある SKU は上の枝)。コードが 80 字を超える SKU は入れない (観測の表の CHECK)
  union all
  select null::bigint, null::bigint, s.sku_id, s.code_norm, s.sku_kind, 'draft_in_ne'::text
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state in ('draft', 'ne_pending') and s.sku_kind in ('single', 'set') and pg_catalog.length(s.code_norm) between 1 and 80
     and exists (select 1 from ops.master_ne_codes c where c.code_norm = s.code_norm and c.kind = 'product')
     and not exists (select 1 from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('issued', 'import_declared', 'partial'));
  -- 🆕 0098 ここまで
comment on view ops.v_ne_reg_targets is '翌朝の照合 ② が NE の完全な取得の値を送る商品 (0053・0063 から配っただけの品目も・0079 から要確認 (quarantined) の単品とセットの全部・0098 から NE のコードの台帳にもうある下書き・NE 登録待ち)';

-- ═══════════ 2. 登録の状態の行き先 (0070 の版から写して 1 行足しただけ) ═══════════
-- 進む向きの地図 (一方向。戻さない)
create or replace function ops.registration_transition_allowed(p_from text, p_to text) returns boolean language sql immutable as $$
  select (p_from || '>' || p_to) = any (array[
    'draft>ne_pending', 'draft>cancelled',
    'ne_pending>ne_confirmed', 'ne_pending>cancelled',
    'ne_confirmed>distributable',
    'distributable>available',
    'quarantined>ne_confirmed', 'quarantined>cancelled'])
    or (p_from || '>' || p_to) = 'available>cancelled'   -- 🆕 0070: NE から消えた (ops.transition_sku_registration が「NE から消えた」の約束と DB の確かめを求める)
    or (p_from || '>' || p_to) = 'draft>ne_confirmed'   -- 🆕 0098: NE にあるので登録済みにする (ops.transition_sku_registration が「登録済みにする」の約束と DB の確かめを求める)
$$;

-- ═══════════ 3. 約束の操作 (0079 の CHECK の版から写して draft_ne_confirm を 1 つ足しただけ)・書いてよい表 (0072 の版から写して 1 行足しただけ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build',
  'quarantine_confirm',
  'draft_ne_confirm'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build',
  'quarantine_confirm',
  'draft_ne_confirm'));

create or replace function ops.master_write_allowed(p_operation text, p_table text, p_op text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from (values
      ('sku_edit', 'core.skus', 'UPDATE'), ('sku_edit', 'core.products', 'UPDATE'),
      ('sku_edit', 'core.supplier_skus', 'INSERT'), ('sku_edit', 'core.supplier_skus', 'UPDATE'),
      ('sku_edit', 'core.sku_costs', 'INSERT'), ('sku_edit', 'core.sku_costs', 'UPDATE'), ('sku_edit', 'core.sku_costs', 'DELETE'),
      ('sku_edit', 'ops.sku_component_requests', 'INSERT'), ('sku_edit', 'ops.sku_component_requests', 'UPDATE'),
      ('sku_edit', 'ops.sku_component_breaches', 'UPDATE'),
      ('sku_create', 'core.products', 'INSERT'), ('sku_create', 'core.products', 'UPDATE'),
      ('sku_create', 'core.skus', 'INSERT'), ('sku_create', 'core.skus', 'UPDATE'),
      ('sku_create', 'core.supplier_skus', 'INSERT'), ('sku_create', 'core.sku_costs', 'INSERT'),
      ('sku_create', 'ops.sku_component_requests', 'INSERT'),
      -- 0053 (⑤-2b)
      ('sku_edit', 'ops.ne_reg_exports', 'UPDATE'), ('sku_edit', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_build', 'ops.ne_reg_exports', 'INSERT'), ('reg_csv_build', 'ops.ne_reg_export_items', 'INSERT'), ('reg_csv_build', 'ops.ne_reg_export_rows', 'INSERT'),
      ('reg_csv_issue', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_issue', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_declare', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_declare', 'ops.ne_reg_export_items', 'UPDATE'), ('reg_csv_declare', 'ops.ne_reg_attempts', 'INSERT'),
      ('reg_csv_supersede', 'ops.ne_reg_exports', 'UPDATE'), ('reg_csv_supersede', 'ops.ne_reg_export_items', 'UPDATE'),
      ('reg_csv_verified', 'ops.ne_csv_verified', 'INSERT'),
      ('jan_edit', 'core.external_ids', 'INSERT'), ('jan_edit', 'core.external_ids', 'UPDATE'), ('jan_edit', 'core.skus', 'UPDATE'), ('jan_edit', 'core.products', 'UPDATE'),
      ('jan_edit', 'ops.ne_reg_exports', 'UPDATE'), ('jan_edit', 'ops.ne_reg_export_items', 'UPDATE'),
      ('supplier_create', 'core.suppliers', 'INSERT'),
      ('supplier_deactivate', 'core.suppliers', 'UPDATE'), ('supplier_deactivate', 'core.supplier_skus', 'INSERT'), ('supplier_deactivate', 'core.supplier_skus', 'UPDATE'),
      ('supplier_deactivate', 'ops.ne_reg_exports', 'UPDATE'), ('supplier_deactivate', 'ops.ne_reg_export_items', 'UPDATE'),
      -- 0054 (⑦-1)
      ('amazon_map_save', 'core.listings', 'INSERT'), ('amazon_map_save', 'core.listings', 'UPDATE'),
      ('amazon_map_save', 'core.amazon_sku_maps', 'INSERT'), ('amazon_map_save', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_save', 'core.listing_components', 'INSERT'), ('amazon_map_save', 'core.listing_components', 'UPDATE'), ('amazon_map_save', 'core.listing_components', 'DELETE'),
      ('amazon_map_delete', 'core.listings', 'UPDATE'),
      ('amazon_map_delete', 'core.amazon_sku_maps', 'UPDATE'),
      ('amazon_map_delete', 'core.listing_components', 'DELETE'),
      -- 🆕 0067 (まとまり): 札を作る = 商品の INSERT・子に親を付ける = 商品の UPDATE・名前を直す = 札の名前・軸・選択肢の UPDATE・採用 = 札の INSERT と子の UPDATE。
      --   まとまりの表 (予約・revision) はどのまとまりの操作でも足す (最初に触るときの予約)
      ('variation_batch_open', 'core.products', 'INSERT'), ('variation_batch_open', 'ops.variation_group_codes', 'INSERT'),
      ('variation_batch_open', 'ops.variation_group_revisions', 'INSERT'), ('variation_batch_open', 'ops.variation_batches', 'INSERT'),
      ('variation_batch_close', 'core.products', 'UPDATE'), ('variation_batch_close', 'core.variation_axes', 'INSERT'), ('variation_batch_close', 'core.variation_options', 'INSERT'),
      ('variation_batch_close', 'core.sku_variation_choices', 'INSERT'), ('variation_batch_close', 'ops.variation_group_revisions', 'UPDATE'),
      ('variation_batch_close', 'ops.variation_batches', 'UPDATE'),
      ('variation_child_cancel', 'ops.variation_group_codes', 'INSERT'), ('variation_child_cancel', 'ops.variation_group_revisions', 'INSERT'),
      ('variation_child_cancel', 'ops.variation_group_revisions', 'UPDATE'),
      ('variation_label_edit', 'core.products', 'UPDATE'), ('variation_label_edit', 'core.variation_axes', 'UPDATE'), ('variation_label_edit', 'core.variation_options', 'UPDATE'),
      ('variation_label_edit', 'ops.variation_group_codes', 'INSERT'), ('variation_label_edit', 'ops.variation_group_revisions', 'INSERT'),
      ('variation_label_edit', 'ops.variation_group_revisions', 'UPDATE'),
      ('parent_adopt_ne', 'core.products', 'INSERT'), ('parent_adopt_ne', 'core.products', 'UPDATE'), ('parent_adopt_ne', 'ops.variation_group_codes', 'INSERT'),
      ('parent_adopt_ne', 'ops.variation_group_revisions', 'INSERT'), ('parent_adopt_ne', 'ops.variation_group_revisions', 'UPDATE'),
      ('parent_adopt_ne', 'ops.variation_parent_adoptions', 'INSERT')) as m(op, tbl, act)
    where m.op = p_operation and m.tbl = p_table and m.act = p_op)
    -- 🆕 0071 ここから: 下書きを消す = 作っただけの NE 登録の CSV を使わないにする・セットの下書きの構成の依頼を取り下げる・JAN を外す (と version の付け替え)
    or (p_operation, p_table, p_op) in (('sku_draft_cancel', 'ops.ne_reg_exports', 'UPDATE'), ('sku_draft_cancel', 'ops.ne_reg_export_items', 'UPDATE'),
      ('sku_draft_cancel', 'ops.sku_component_requests', 'UPDATE'),
      ('sku_draft_cancel', 'core.external_ids', 'UPDATE'), ('sku_draft_cancel', 'core.skus', 'UPDATE'), ('sku_draft_cancel', 'core.products', 'UPDATE'))
    -- 🆕 0071 ここまで
    -- 🆕 0072 ここから: NE から消えたセットをやめる (ops.cancel_sku_gone_from_ne の中で構成を外す) = 親のセットの version の付け替え (0026 の core.bump_parent_version)
    or (p_operation, p_table, p_op) = ('sku_gone_cancel', 'core.skus', 'UPDATE')
    -- 🆕 0072 ここまで
    -- 🆕 0098 ここから: NE にあるので登録済みにする = 一部違う (partial) の NE 登録の CSV の品目を「使わない」(superseded・人が決めた resolved_by_human) にする
    or (p_operation, p_table, p_op) = ('draft_ne_confirm', 'ops.ne_reg_export_items', 'UPDATE')
    -- 🆕 0098 ここまで
$$;

-- ═══════════ 4. 登録済みにしてよいかの表 (読むだけ) ═══════════
/** 表の 1 行 ({ reason, text }) */
create function ops._draft_in_ne_problem(p_reason text, p_text text) returns jsonb
  language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', p_reason, 'text', p_text))
$$;
revoke all on function ops._draft_in_ne_problem(text, text) from public;

/**
 * 「NE にあるので登録済みにする」をしてよいか。何も書かない・鍵を取らない (watcher の読むだけの取引でも動く)。
 * 戻り値 = { sku_id (文字), code, sku_kind, reg_state, in_ne (NE のコードの台帳に商品のコードとしてある = 画面が箱を出すか), ok,
 *            problems: [{ reason, text }], item ({ item_id, export_id, state: 'partial' } = 一緒に「使わない」にする品目 / null), diffs (NE と違う欄 = 「NE へ送る」に出る),
 *            evidence (ok のときだけ = 登録の状態の履歴の根拠), check_hash }
 * reason: no_sku / not_draft / kind / not_in_ne / ne_code (書き方が違う・2 つ以上) / ne_run (照合の回が無い・古い) / ne_observed (観測が無い・NE に無い・信用できない・区分が違う) /
 *         live_file (終わっていない CSV の品目) / ne_delete (NE から消す依頼の途中) / parent (代表が NE と違う・読めない) /
 *         set_components / set_request / set_children (セットの構成品・構成の依頼・中身が NE と違う)
 * 照合の回の新しさ = 要確認の確かめ (0079 の ops.quarantine_confirm_check) と同じ: 最新の封をした照合 ② の回 = NE のコードの台帳の回・取得が ops.ne_gone_fresh_hours() 時間の中
 */
create function ops.draft_in_ne_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  v_p      jsonb := '[]'::jsonb;
  v_today  date := (pg_catalog.now() at time zone 'Asia/Tokyo')::date;
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_nc     record;
  v_in_ne  boolean := false;
  v_run    text;
  v_mark   text;
  h        ops.ne_reg_compare_runs;
  v_rc     ops.ne_reg_compare_receipts;
  v_fresh  boolean := false;
  v_obs    jsonb;
  v_obsok  boolean := false;
  v_nlive  integer;
  v_npart  integer;
  v_txt    text;
  v_item   jsonb;
  v_ncol   jsonb;
  v_nrep   text;
  v_repok  boolean;
  v_n      integer;
  v_bad    text;
  v_kids   jsonb;
  v_diff   text[];
  v_exp    jsonb;
  v_cmp    jsonb;
  v_diffs  jsonb := '[]'::jsonb;
  v_ok     boolean;
  v_obsh   text;
  v_ev     jsonb;
  v_hash   text;
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.version as sku_version, k.product_id, p.version as product_version, p.parent_product_id,
         pp.display_code as parent_code, r.state as reg, r.origin
    into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
    left join ops.master_registrations r on r.sku_id = k.sku_id
   where k.sku_id = p_sku_id;
  if not found or s.company_id <> 1 then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'sku_kind', null, 'reg_state', null, 'in_ne', false, 'ok', false,
      'problems', ops._draft_in_ne_problem('no_sku', 'Company DB にその商品がありません'), 'item', null, 'diffs', '[]'::jsonb, 'evidence', null, 'check_hash', null);
  end if;
  -- ─── 登録の状態・区分 ───
  if s.reg is null or s.reg not in ('draft', 'ne_pending') or s.origin is distinct from 'new_entry' then
    v_p := v_p || ops._draft_in_ne_problem('not_draft', pg_catalog.format('登録の状態が %s です (登録済みにできるのは、ポータルで登録した下書き・NE 登録待ちの商品だけ)',
      coalesce(case s.reg when 'draft' then '下書き' when 'ne_pending' then 'NE 登録待ち' when 'ne_confirmed' then 'NE 確認済み' when 'quarantined' then '要確認'
                          when 'cancelled' then 'やめた' when 'distributable' then '配れる' when 'available' then '業務で使える' else s.reg end, 'なし')));
  end if;
  if s.sku_kind is null or s.sku_kind not in ('single', 'set') then
    v_p := v_p || ops._draft_in_ne_problem('kind', '例外の SKU は登録済みにしません');
  end if;
  -- ─── NE のコードの台帳 (0041 = 最新の照合の回・商品のコード)。書き方 (大文字・小文字) は NE 登録の CSV の確かめ (0064) と同じ = 打ったとおりと完全に同じ ───
  select c.state, c.ne_code into v_nc from ops.master_ne_codes c where c.code_norm = s.code_norm and c.kind = 'product';
  v_in_ne := found;
  if not v_in_ne then
    v_p := v_p || ops._draft_in_ne_problem('not_in_ne', 'NE にこのコードの商品がありません (今朝の NE の取得)');
  elsif v_nc.state is distinct from 'ok' then
    v_p := v_p || ops._draft_in_ne_problem('ne_code', 'NE に大文字・小文字だけ違う同じコードが 2 つ以上あるか、コードを読めません (どの商品か決まらない)');
  elsif v_nc.ne_code is distinct from s.code then
    v_p := v_p || ops._draft_in_ne_problem('ne_code', pg_catalog.format('NE のコード %s と、この商品のコード %s は大文字・小文字が違います (NE では別の商品)', v_nc.ne_code, s.code));
  end if;
  -- ─── 照合 ② の回と観測 (0079 の要確認の確かめと同じ新しさ) ───
  select m.compare_run_id into v_run from ops.ne_reg_compare_receipts rc join ops.master_compare_runs m on m.compare_run_id = rc.compare_run_id
   order by m.observed_at desc, m.compare_run_id desc limit 1;
  select x.compare_run_id into v_mark from ops.master_ne_code_mark x where x.id = 1;
  select * into h from ops.ne_reg_compare_runs x where x.compare_run_id = v_run;
  select * into v_rc from ops.ne_reg_compare_receipts x where x.compare_run_id = v_run;
  v_fresh := coalesce(v_run is not null and v_run = v_mark and h.compare_run_id is not null
             and least(h.products_at, h.sets_at) >= pg_catalog.now() - pg_catalog.make_interval(hours => v_hours), false);
  if v_run is not null then
    select x.observation into v_obs from ops.ne_reg_compare_observations x where x.compare_run_id = v_run and x.code_norm = s.code_norm;
  end if;
  v_obsok := coalesce(v_fresh and v_obs is not null and (v_obs -> 'present') = 'true'::jsonb and (v_obs -> 'trusted') = 'true'::jsonb
                      and (v_obs ->> 'kind') is not distinct from s.sku_kind, false);
  if not v_in_ne then
    null;   -- NE に無い = 上の 1 行だけ (観測の理由を重ねない)
  elsif not v_fresh then
    v_p := v_p || ops._draft_in_ne_problem('ne_run', case when v_run is null then 'まだ照合 (毎朝の NE との照らし合わせ) の記録がありません。明日の朝の照合の後に押せます'
      else '今朝の照合がまだ終わっていないか、NE の取得が古いです。今朝の照合が終わった後に押せます' end);
  elsif v_obs is null then
    v_p := v_p || ops._draft_in_ne_problem('ne_observed', '今朝の照合では、この商品の NE の中身 (代表・セットの中身) をまだ見ていません。明日の朝の照合の後に押せます');
  elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
    v_p := v_p || ops._draft_in_ne_problem('ne_observed', '今朝の NE の取得に、このコードの商品がありません');
  elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
    v_p := v_p || ops._draft_in_ne_problem('ne_observed', '今朝の NE の取得で、このコードの商品を確かめられませんでした。明日の朝の照合の後に押せます');
  elsif (v_obs ->> 'kind') is distinct from s.sku_kind then
    v_p := v_p || ops._draft_in_ne_problem('ne_observed', pg_catalog.format('NE では%s、Company DB では%sです (別の商品)',
      case v_obs ->> 'kind' when 'set' then 'セット' when 'single' then '単品' else '区分が不明' end, case s.sku_kind when 'set' then 'セット' else '単品' end));
  end if;
  -- ─── 生きている NE 登録の CSV の品目: 無い / 一部違う (partial) が 1 つだけ (それを「使わない」にする) ───
  select pg_catalog.count(*), pg_catalog.count(*) filter (where i.state = 'partial'),
         pg_catalog.string_agg('#' || i.export_id::text || ' ' || case i.state when 'built' then '作っただけ' when 'issued' then '配った (翌朝の確かめ待ち)'
                                 when 'import_declared' then '取り込んだと申告 (翌朝の確かめ待ち)' when 'partial' then 'NE とちがう所がある' else i.state end, '・' order by i.item_id)
    into v_nlive, v_npart, v_txt
    from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if v_nlive = 1 and v_npart = 1 then
    select pg_catalog.jsonb_build_object('item_id', i.item_id::text, 'export_id', i.export_id::text, 'state', i.state) into v_item
      from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state = 'partial';
  elsif v_nlive > 0 then
    v_p := v_p || ops._draft_in_ne_problem('live_file', pg_catalog.format('NE 登録の CSV がまだ終わっていません (ファイル %s)。翌朝の確かめを待つか、「これまでの記録」でそのファイルを「使わない」にしてから', v_txt));
  end if;
  if exists (select 1 from ops.ne_del_items d where d.sku_id = p_sku_id and d.state = 'live') then
    v_p := v_p || ops._draft_in_ne_problem('ne_delete', 'NE から消す依頼の途中です');
  end if;
  -- ─── 代表 (色違いのまとまり) = NE と同じ (単品・観測があるときだけ見られる)。決まりは代表の門の「直った」(0095 の ops._parent_gate_resolved) と同じ:
  --     NE の代表なし = 社内も親なし / NE の代表 R = 社内の親の名札が R・2 段でない・R に当たる商品がちょうど 1 つ ───
  if s.sku_kind = 'single' and v_obsok then
    v_ncol := v_obs -> 'cols' -> 'parent';
    v_repok := true;
    if (v_ncol ->> 'st') = 'ok' and pg_catalog.jsonb_typeof(v_ncol -> 'v') = 'string' and coalesce(v_ncol ->> 'v', '') <> '' and core.norm_code(v_ncol ->> 'v') <> s.code_norm then
      v_nrep := core.norm_code(v_ncol ->> 'v');
    elsif (v_ncol ->> 'st') in ('ok', 'no_value') then
      v_nrep := null;
    else
      v_repok := false;
    end if;
    if not v_repok then
      v_p := v_p || ops._draft_in_ne_problem('parent', 'NE の代表商品コードを読めません。明日の朝の照合の後に押せます');
    elsif not ops._parent_gate_resolved(s.code_norm, 'parent_mismatch', v_nrep) then
      v_p := v_p || ops._draft_in_ne_problem('parent', case
        when v_nrep is null then pg_catalog.format('NE では代表がありませんが、Company DB ではまとまり %s の子です。代表をそろえてから押してください', coalesce(s.parent_code, '?'))
        when s.parent_product_id is null then pg_catalog.format('NE ではまとまり %s の子ですが、Company DB では代表がありません。代表をそろえてから押してください', v_ncol ->> 'v')
        when coalesce(core.norm_code(s.parent_code), '') is distinct from v_nrep then
          pg_catalog.format('代表が NE (%s) と Company DB (%s) で違います。代表をそろえてから押してください', v_ncol ->> 'v', coalesce(s.parent_code, '?'))
        else pg_catalog.format('まとまり %s の形 (親の親・同じコードの商品が 2 つ ほか) が NE と合いません。代表をそろえてから押してください', v_ncol ->> 'v') end);
    end if;
  end if;
  -- ─── セット: 構成品 (1 つ以上・全部 NE 確認済み以降)・開いている構成の依頼が無い・中身 (子と数量) が NE と同じ (NE へ送るでは直せない) ───
  if s.sku_kind = 'set' then
    select pg_catalog.count(*), pg_catalog.string_agg(case when rg.state is null or rg.state not in ('ne_confirmed', 'distributable', 'available') then k2.code end, '・' order by k2.code)
      into v_n, v_bad
      from core.sku_components c2 join core.skus k2 on k2.sku_id = c2.child_sku_id left join ops.master_registrations rg on rg.sku_id = c2.child_sku_id
     where c2.parent_sku_id = p_sku_id;
    if exists (select 1 from ops.sku_component_requests q where q.set_sku_id = p_sku_id and q.status = 'open') then
      v_p := v_p || ops._draft_in_ne_problem('set_request', 'セットの中身の依頼がまだ開いています (中身が決まってから押せます)');
    elsif v_n = 0 then
      v_p := v_p || ops._draft_in_ne_problem('set_components', 'セットの中身 (構成品) がありません');
    elsif v_bad is not null then
      v_p := v_p || ops._draft_in_ne_problem('set_components', pg_catalog.format('構成品 %s がまだ登録済みではありません (先に構成品を)', v_bad));
    elsif v_obsok then
      select pg_catalog.jsonb_object_agg(k2.code_norm, c2.qty) into v_kids from core.sku_components c2 join core.skus k2 on k2.sku_id = c2.child_sku_id where c2.parent_sku_id = p_sku_id;
      select pg_catalog.array_agg(coalesce(a.cn, b.cn) order by coalesce(a.cn, b.cn)) into v_diff
        from (select t.key as cn, t.value as qty from pg_catalog.jsonb_each(coalesce(v_kids, '{}'::jsonb)) t) a
        full join (select ch ->> 'code_norm' as cn, ch from pg_catalog.jsonb_array_elements(coalesce(v_obs -> 'children', '[]'::jsonb)) ch) b on b.cn = a.cn
       where b.cn is null or a.cn is null or (b.ch ->> 'st') is distinct from 'ok' or (b.ch -> 'v') is distinct from a.qty;
      if v_diff is not null then
        v_p := v_p || ops._draft_in_ne_problem('set_children', pg_catalog.format('セットの中身が NE と違います (%s)。「NE へ送る」では直せないので、NE か Company DB で中身をそろえてから押してください',
          pg_catalog.array_to_string(v_diff, '・')));
      end if;
    end if;
  end if;
  -- ─── NE と違う欄 (登録済みにした後に「NE へ送る」に出る・知らせだけ) = 今の Company DB の値 (NE 登録の CSV と同じ作り方) と今朝の観測 ───
  if v_obsok then
    v_exp := ops.ne_reg_canonical(p_sku_id, v_today) -> 'expected';
    if v_exp is not null and (v_exp ->> 'kind') = s.sku_kind then
      v_cmp := ops.ne_reg_compare(v_exp, v_obs);
      select coalesce(pg_catalog.jsonb_agg(x.key order by x.key), '[]'::jsonb) into v_diffs
        from pg_catalog.jsonb_each(coalesce(v_cmp -> 'cols', '{}'::jsonb)) x
       where x.key not in ('parent', 'children') and (x.value -> 'ok') is distinct from 'true'::jsonb;
    end if;
  end if;

  v_ok := pg_catalog.jsonb_array_length(v_p) = 0;
  v_obsh := case when v_obs is null then null else pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_obs::text, 'UTF8')), 'hex') end;
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('v', 'draft-in-ne-v1', 'sku_id', p_sku_id::text, 'day', v_today::text, 'sku_version', s.sku_version::text,
    'product_version', s.product_version::text, 'reg', s.reg, 'run', v_run, 'mark', v_mark, 'observation_hash', v_obsh, 'ne_code', v_nc.ne_code, 'item', v_item,
    'problems', v_p, 'diffs', v_diffs));
  if v_ok then
    v_ev := pg_catalog.jsonb_build_object('rule', 'draft_in_ne_v1', 'check_hash', v_hash, 'compare_run_id', v_run, 'ne_codes_run', v_mark, 'ne_code', v_nc.ne_code,
      'receipt_observation_hash', v_rc.observation_hash, 'evidence_sha256', v_rc.evidence_sha256, 'fetch_generation', h.fetch_generation,
      'products_at', h.products_at, 'sets_at', h.sets_at, 'observation_hash', v_obsh, 'from_state', s.reg,
      'item', case when v_item is null then null else v_item || '{"outcome": "resolved_by_human"}'::jsonb end,
      'parent', case when s.sku_kind = 'single' then pg_catalog.jsonb_build_object('cdb', s.parent_code, 'ne', v_nrep) end,
      'diffs', v_diffs, 'day', v_today::text);
  end if;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', s.code, 'sku_kind', s.sku_kind, 'reg_state', s.reg, 'in_ne', v_in_ne, 'ok', v_ok,
    'problems', v_p, 'item', v_item, 'diffs', v_diffs, 'evidence', v_ev, 'check_hash', v_hash);
end $$;
revoke all on function ops.draft_in_ne_check(bigint) from public;

-- ═══════════ 5. 状態を 1 つ進める (0079 の版を写して、🆕 0098 のかたまりだけ足した) ═══════════
/**
 * 0079 の版と同じ (ne_pending・ne_confirmed (ne_pending から・quarantined から)・cancelled の道・0070 / 0071 / 0073 の守り)。足したのは 🆕 0098 のかたまり:
 *   draft・ne_pending → ne_confirmed = 約束 draft_ne_confirm (ops.confirm_draft_in_ne だけが書く) の中・同じ SKU・人・request_id・人 (human) のときだけ。
 *   呼び手の根拠は受けない。ops.draft_in_ne_check をもう一度呼び、ok で check_hash が約束の check_hash と同じときだけ。根拠 = その表の evidence (DB が作る)。
 *   draft → ne_confirmed は約束の外では断る (地図に 0098 で足した行の守り)
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
  -- 🆕 0079 ここから: 要確認 (NE で直接作られた商品) → NE 確認済み = 「確認済みにする」の約束 (ops.confirm_quarantined_sku・操作 quarantine_confirm) の中だけ・
  --   同じ SKU・人・request_id・人 (human)。呼び手の根拠は受けない。確かめ (ops.quarantine_confirm_check) をもう一度: ok で、check_hash が約束の check_hash と同じときだけ。
  --   根拠は DB が確かめの表から作る。運用のロール・持ち主のロールが直に呼んでも断る (0070 の「NE から消えた」と同じ型)。下の 0073 の not_ready の行には届かない
  if p_to = 'ne_confirmed' and r.state = 'quarantined' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'quarantine_confirm' or v_sess.sku_id is distinct from p_sku_id
       or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id or p_actor_type is distinct from 'human' then
      raise exception 'confirm_session_required: 要確認を NE 確認済みにするのは「確認済みにする」の操作 (ops.confirm_quarantined_sku) の中だけ' using errcode = '42501';
    end if;
    if v_ev <> '{}'::jsonb then
      raise exception 'caller_evidence: 確認済みの根拠は DB が確かめの表から作る。呼び手の根拠は受けない' using errcode = '22023';
    end if;
    v_chk := ops.quarantine_confirm_check(p_sku_id);
    if (v_chk ->> 'ok') is distinct from 'true' then
      raise exception 'not_confirmable: %', (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'items') x
                                              where (x -> 'ok') is distinct from 'true'::jsonb) using errcode = 'P0001';
    end if;
    if (v_chk ->> 'check_hash') is distinct from (v_sess.versions ->> 'check_hash') then
      raise exception 'stale: 確かめの表が約束の時と違う (読み直して押し直す)' using errcode = 'P0001';
    end if;
    v_ev := v_chk -> 'evidence';
    perform pg_catalog.set_config('ops.registration_protocol', '1', true);
    update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
    perform pg_catalog.set_config('ops.registration_protocol', '', true);
    insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
      values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
  end if;
  -- 🆕 0079 ここまで
  -- 🆕 0098 ここから: 下書き・NE 登録待ち → NE 確認済み = 「NE にあるので登録済みにする」の約束 (ops.confirm_draft_in_ne・操作 draft_ne_confirm) の中・同じ SKU・人・request_id・
  --   人 (human) のときだけ。呼び手の根拠は受けない。確かめ (ops.draft_in_ne_check) をもう一度: ok で、check_hash が約束の check_hash と同じときだけ。根拠は DB が確かめの表から作る。
  --   下書きから直に NE 確認済み (地図の draft > ne_confirmed) はこの道だけ = ほかの呼び手 (運用のロール・持ち主・照合) は断る。
  --   NE 登録待ちから (約束が無い) = 今までどおり下の照合の確かめ (system・verified) の道
  if p_to = 'ne_confirmed' and r.state in ('draft', 'ne_pending') then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is not null and v_sess.operation = 'draft_ne_confirm' then
      if v_sess.sku_id is distinct from p_sku_id or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id
         or p_actor_type is distinct from 'human' then
        raise exception 'confirm_session_required: NE にある下書きを登録済みにするのは「NE にあるので登録済みにする」の操作 (ops.confirm_draft_in_ne) の中だけ (同じ商品・人・番号)' using errcode = '42501';
      end if;
      if v_ev <> '{}'::jsonb then
        raise exception 'caller_evidence: 登録済みにする根拠は DB が確かめの表から作る。呼び手の根拠は受けない' using errcode = '22023';
      end if;
      v_chk := ops.draft_in_ne_check(p_sku_id);
      if (v_chk ->> 'ok') is distinct from 'true' then
        raise exception 'not_confirmable: %', (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x) using errcode = 'P0001';
      end if;
      if (v_chk ->> 'check_hash') is distinct from (v_sess.versions ->> 'check_hash') then
        raise exception 'stale: 確かめの表が約束の時と違う (読み直して押し直す)' using errcode = 'P0001';
      end if;
      v_ev := v_chk -> 'evidence';
      perform pg_catalog.set_config('ops.registration_protocol', '1', true);
      update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
      perform pg_catalog.set_config('ops.registration_protocol', '', true);
      insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
        values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
      return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
    elsif r.state = 'draft' then
      raise exception 'confirm_session_required: 下書きを NE 確認済みにするのは「NE にあるので登録済みにする」の操作 (ops.confirm_draft_in_ne) の中だけ' using errcode = '42501';
    end if;
  end if;
  -- 🆕 0098 ここまで
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

-- ═══════════ 6. 登録済みにする関数 ═══════════
/**
 * 下書き・NE 登録待ちで NE にもうある商品を NE 確認済みにする (1 件ずつ・1 つの取引)。
 *   p_check_hash = 画面が見た確かめの表のハッシュ (ops.draft_in_ne_check の check_hash)。押した時の表と違う = stale (読み直して押し直す)
 *   同じ request_id の押し直し = 前の答え (replayed)。違う操作・人・SKU・表で同じ request_id = request_id_reused
 *   鍵: 段階 (共有) → マスタの書き込み (共有・夜間ロードと並ぶ) → request_id → SKU (保存・CSV と同じ鍵) → 親子 (共有・代表を変える保存と並ぶ) → CSV (照合の確かめ・作る・配る・使わないと並ぶ) →
 *       NE のコード (共有・照合の入れ替えと並ぶ) → 登録の状態の行 (for update) → 生きている品目の行 (for update) → 構成品の登録の状態の行 (for share = 構成品を同時にやめない)
 *   段階 new_open・持ち主表 = DB の active が段階の記録と同じ (0053 の ops.reg_write_gate)。確かめが ok でない = not_confirmable (理由を全部)・ハッシュが違う = stale
 *   約束 (ops.master_write_sessions・操作 draft_ne_confirm) → 登録の状態 (ops.transition_sku_registration が約束と確かめをもう一度見る) →
 *   一部違う品目 = superseded (理由 = resolved_by_human・NE で何をするか) → 保存の記録 done (ops.close_reg_write)。
 *   戻り値 = { ok, request_id, code, sku_id, from, state, event_id, check_hash, resolved_item, diffs }
 * 呼べるロール = master_edit (画面・名簿 MASTER_DECISION_APPROVERS の人 = NE 登録の CSV と同じ)。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)
 * 商品の値 (core.*)・ロジザードの表は書かない
 */
create function ops.confirm_draft_in_ne(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_check_hash text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  s         record;
  v_chk     jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_tr      jsonb;
  v_item    jsonb;
  v_n       integer;
  v_result  jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null then raise exception 'invalid_input: 人 (actor) の形が違う' using errcode = '22023'; end if;
  if coalesce(p_check_hash, '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: 確かめの表のハッシュ (check_hash・64 桁の 16 進) が要る (画面で読んでから押す)' using errcode = '22023';
  end if;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);   -- 段階 (共有) → マスタの書き込み (共有) → 段階 new_open・持ち主表 = 段階の記録
  -- 同じ request_id の押し直しを直列に (保存 lib/master-write.mjs の REQUEST_LOCK_SQL と同じ鍵) → その後に前の結果を読む = 同時の押し直しも前の答え
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.master_edit_request:' || p_request_id::text, 0));
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'draft_ne_confirm', 'sku_id', p_sku_id::text, 'check_hash', p_check_hash));
  select x.operation, x.actor_id, x.status, x.result, x.payload_hash, x.sku_id into v_prev from ops.master_edit_requests x where x.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'draft_ne_confirm' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done'
       or v_prev.payload_hash is distinct from v_hash or v_prev.sku_id is distinct from p_sku_id then
      raise exception 'request_id_reused: request_id % はほかの操作・人・商品・表で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock_shared(core.parent_lock_key());
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_csv'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  select k.code, k.code_norm into s from core.skus k where k.sku_id = p_sku_id and k.company_id = 1;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  perform 1 from ops.master_registrations x where x.sku_id = p_sku_id for update;
  perform 1 from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state in ('built', 'issued', 'import_declared', 'partial') order by i.item_id for update;
  perform 1 from ops.master_registrations x where x.sku_id in (select c.child_sku_id from core.sku_components c where c.parent_sku_id = p_sku_id) order by x.sku_id for share;
  v_chk := ops.draft_in_ne_check(p_sku_id);
  if (v_chk ->> 'ok') is distinct from 'true' then
    raise exception 'not_confirmable: % は登録済みにできません (%)', s.code, (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x)
      using errcode = 'P0001';
  end if;
  if (v_chk ->> 'check_hash') is distinct from p_check_hash then
    raise exception 'stale: % の様子が画面を開いた時と変わりました (値か照合が変わった = 画面を読み直してもう一度)', s.code using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  v_item := v_chk -> 'item';
  select x.phase into v_phase from ops.master_cutover_state x where x.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'draft_ne_confirm', p_sku_id, '{}', '{}', pg_catalog.repeat('0', 64),
            v_hash, pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'check_hash', p_check_hash,
              'item_id', case when pg_catalog.jsonb_typeof(v_item) = 'object' then v_item ->> 'item_id' end),
            p_actor_id, null, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  v_tr := ops.transition_sku_registration(p_sku_id, 'ne_confirmed', 'human', p_actor_id, 'NE にあるので登録済みにした', '{}'::jsonb, p_request_id::text);
  -- 一部違う (partial) の品目 = 消さずに「使わない」(人が決めた = resolved_by_human)。ファイルは翌朝の照合の確かめが「終わった」(finished) で閉じる (ほかの品目が終われば)
  if pg_catalog.jsonb_typeof(v_item) = 'object' then
    update ops.ne_reg_export_items set state = 'superseded',
           superseded_reason = pg_catalog.left('resolved_by_human: NE にあるので登録済みにした (' || p_actor_id || ')', 500),
           superseded_correction = 'NE の今の商品をそのまま使う。NE とちがう所は「NEへ送る」で NE に送る',
           state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id
     where item_id = (v_item ->> 'item_id')::bigint and sku_id = p_sku_id and state = 'partial';
    get diagnostics v_n = row_count;
    if v_n <> 1 then raise exception 'stale: % の NE 登録の CSV の品目が変わった (画面を読み直してもう一度)', s.code using errcode = 'P0001'; end if;
  end if;
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', s.code, 'sku_id', p_sku_id::text, 'from', v_chk ->> 'reg_state',
    'state', 'ne_confirmed', 'event_id', v_tr ->> 'event_id', 'check_hash', p_check_hash,
    'resolved_item', case when pg_catalog.jsonb_typeof(v_item) = 'object' then v_item || '{"outcome": "resolved_by_human"}'::jsonb end, 'diffs', v_chk -> 'diffs');
  perform ops.close_reg_write(v_result, s.code);
  return v_result;
end $$;
revoke all on function ops.confirm_draft_in_ne(uuid, text, bigint, text) from public;

-- ═══════════ 7. NE へ送るの差 (0094 の版を写して、🆕 0098 の行だけ足した) ═══════════
/**
 * 0094 と同じ。足したのは 🆕 0098 の行: 「NE にあるので登録済みにする」で NE 確認済みにした新商品 (登録の状態の履歴の根拠 rule = draft_in_ne_v1) は、
 *   名前・取扱・税率・売価を登録 (ポータルの INSERT) のまま直していなくても、ポータルで人が入れた値 = NE と違えば送る (原価・代表の仕入先は 0094 から INSERT も見ている)。
 *   ほかの商品 (照合で確かめた新商品・夜間ロードの商品) は今までどおり「その欄を最後に UPDATE したのが人」だけ
 */
create or replace function ops.ne_upd_units(p_sku_ids bigint[], p_entry text) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  m        ops.master_ne_values_mark;
  v_code_run text;
  v_today  date := core.jst_date(pg_catalog.clock_timestamp());
  v_own    jsonb := ops.master_ownership_active_map();
  v_phase  text;
  v_hash   text;
  v_lz_at  timestamptz;
  v_lz_fresh boolean;
  v_case_ok boolean;
  v_p_today boolean;
  v_s_today boolean;
  v_open   boolean;
  v_problems jsonb := '[]'::jsonb;
  v_blank  jsonb;
  v_units  jsonb;
begin
  select * into m from ops.master_ne_values_mark k where k.id = 1;
  select k.compare_run_id into v_code_run from ops.master_ne_code_mark k where k.id = 1;
  select s.phase, s.owner_hash into v_phase, v_hash from ops.master_cutover_state s where s.id = 1;
  select k.exported_at into v_lz_at from ops.master_lz_codes_mark k where k.id = 1;
  v_lz_fresh := v_lz_at is not null and v_lz_at > pg_catalog.clock_timestamp() - interval '36 hours';
  -- 大文字・小文字の直し (NE の書き方 → ロジザードの書き方) は、NE の実物で「今ある商品のコードを書き換える (新しい商品を作らない)」を確かめた後だけ (設計 25 §5 の 3)。
  --   確かめる前 = NE の今の書き方で送る (書き方が 2 つ = 決まらない = 送らない)・書き方だけの行も作らない
  if p_entry is null or p_entry not in ('bulk', 'api') then raise exception 'invalid_input: 入口は bulk / api' using errcode = '22023'; end if;
  v_case_ok := coalesce((select v.result = 'ok' from ops.ne_upd_verified v where v.kind = 'products' and v.col = 'code_case' and v.entry = p_entry order by v.verified_id desc limit 1), false);
  v_open := v_phase = 'new_open' and v_hash is not distinct from ops.ownership_hash(v_own);
  if not v_open then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_open', 'text', 'Company DB の保存がまだ開いていません (切替の段階)'));
  end if;
  if m.id is null then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_ne_values', 'text', '今朝の NE の値がまだ Company DB にありません (毎朝の照合のあとに入ります)'));
  elsif m.code_mark_run is distinct from m.compare_run_id or v_code_run is distinct from m.compare_run_id then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'ne_codes_other_run', 'text', 'NE の値と NE のコードの書き方が同じ朝のものではありません (次の照合を待つ)'));
  end if;
  v_p_today := m.id is not null and core.jst_date(m.products_started_at) = v_today;
  v_s_today := m.id is not null and core.jst_date(m.sets_started_at) = v_today;
  if m.id is not null and not v_p_today and not v_s_today then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'ne_not_today', 'text', '今朝の NE の取得がまだです (比べる NE の値が今日のものではない)'));
  end if;
  -- 空欄にしてよい欄 (入口・種類・欄ごとに最後の試しの記録が ok)
  select pg_catalog.jsonb_build_object(
           'bulk', pg_catalog.jsonb_build_object('products', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.entry = 'bulk' and x.kind = 'products' and x.result = 'ok'), '[]'::jsonb),
                                                 'sets', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.entry = 'bulk' and x.kind = 'sets' and x.result = 'ok'), '[]'::jsonb)),
           'api', pg_catalog.jsonb_build_object('products', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.entry = 'api' and x.kind = 'products' and x.result = 'ok'), '[]'::jsonb),
                                                'sets', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.entry = 'api' and x.kind = 'sets' and x.result = 'ok'), '[]'::jsonb)))
    into v_blank
    from (select distinct on (v.entry, v.kind, v.col) v.entry, v.kind, v.col, v.result from ops.ne_upd_verified v where v.col <> 'code_case' order by v.entry, v.kind, v.col, v.verified_id desc) x;

  with cols as (select * from ops.ne_upd_cols()),
  sk as (
    select k.sku_id, k.code, k.code_norm, k.sku_kind, case k.sku_kind when 'single' then 'products' else 'sets' end as kind,
           k.name, k.handling, k.tax_rate, k.standard_price_jpy
      from core.skus k
     where k.company_id = 1 and k.sku_kind in ('single', 'set') and (p_sku_ids is null or k.sku_id = any (p_sku_ids))
  ),
  cur as (
    select s.sku_id, s.code, s.code_norm, s.sku_kind, s.kind, s.name as sku_name, c.col, c.ord, c.keys, c.attr,
           case c.col
             when 'name' then ops.ne_upd_text(s.name)
             when 'handling' then coalesce(ops.ne_upd_text(s.handling), 'null'::jsonb)
             when 'tax_rate' then coalesce(pg_catalog.to_jsonb(s.tax_rate::numeric), 'null'::jsonb)
             when 'standard_price_jpy' then ops.ne_upd_yen(s.standard_price_jpy::numeric)
             when 'cost' then ops.ne_upd_yen(co.cost_jpy::numeric)
             when 'primary_supplier' then coalesce(ps.codes, '[]'::jsonb)
           end as cdb0,
           case c.attr when 'name' then pg_catalog.to_jsonb(s.name) when 'handling' then pg_catalog.to_jsonb(s.handling)
                       when 'tax_rate' then pg_catalog.to_jsonb(s.tax_rate) when 'standard_price_jpy' then pg_catalog.to_jsonb(s.standard_price_jpy) end as raw,
           co.sku_cost_id, ps.supplier_id as primary_supplier_id, ps.n as primary_n
      from sk s join cols c on c.kind = s.kind
      left join core.sku_costs co on c.col = 'cost' and co.sku_id = s.sku_id and co.valid_to is null
      left join lateral (select pg_catalog.jsonb_agg(sup.code_norm order by sup.code_norm) as codes, pg_catalog.min(x.supplier_id) as supplier_id, pg_catalog.count(*) as n
                           from core.supplier_skus x join core.suppliers sup on sup.supplier_id = x.supplier_id
                          where c.col = 'primary_supplier' and x.sku_id = s.sku_id and x.is_primary) ps on true
     where not exists (select 1 from pg_catalog.unnest(c.keys) k where (v_own ->> k) is distinct from 'company')
  ),
  -- 取扱の空は 'unknown' (baseline.mjs の cdbValue)
  cur2 as (select u.*, case when u.col = 'handling' and u.cdb0 = 'null'::jsonb then '"unknown"'::jsonb else u.cdb0 end as cdb from cur u),
  d as (
    select u.*, v.state as ne_state, v.value as ne, v.ne_kind,
           -- 今朝のその種類の取得の後に最後に出した値 (無ければ null)
           (select lr.expect from ops.ne_upd_rows lr join ops.ne_upd_files f on f.file_id = lr.file_id
             where lr.code_norm = u.code_norm and lr.col = u.col and lr.role = 'change' and ops.ne_upd_file_out(f.state, f.claimed_at, f.downloaded_at)
               and coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) >= case u.kind when 'products' then m.products_started_at else m.sets_started_at end
             order by coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) desc, lr.row_id desc limit 1) as last_out
      from cur2 u join ops.master_ne_values v on v.code_norm = u.code_norm and v.col = u.col
     where v.ne_kind = case u.sku_kind when 'single' then 'single' else 'set' end
  ),
  -- 単品のコードの書き方 (設計 25 §2.4)。セットは NE の書き方だけ
  sp as (
    select s.sku_id, s.code_norm, s.kind, nc.state as nc_state, nc.ne_code as x, nc.spellings as nc_spellings, lz.state as lz_state, lz.lz_code as l,
           case
             when nc.state is null or nc.state = 'invalid' then null
             when s.kind = 'sets' then case when nc.state = 'ok' then nc.ne_code end
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'one' then lz.lz_code
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'many' then null
             when nc.state = 'ok' then nc.ne_code
             when nc.state = 'collided' and v_case_ok and v_lz_fresh and lz.state = 'one' then lz.lz_code
             else null
           end as code_to,
           case
             when nc.state is null or nc.state = 'invalid' then 'ne_code_unknown'
             when s.kind = 'sets' and nc.state <> 'ok' then 'ne_collided'
             when s.kind = 'sets' then null
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'many' then 'lz_many'
             when nc.state = 'ok' then null
             when nc.state = 'collided' and not v_case_ok then 'case_unverified'
             when nc.state = 'collided' and not v_lz_fresh then 'lz_old'
             when nc.state = 'collided' and coalesce(lz.state, '') <> 'one' then 'ne_collided'
             else null
           end as code_why
      from sk s
      left join ops.master_ne_codes nc on nc.code_norm = s.code_norm and nc.kind = 'product'
      left join ops.master_lz_codes lz on lz.code_norm = s.code_norm
  ),
  -- SKU の止め (新商品の途中・NE から消す依頼の途中)
  blk as (
    select s.sku_id,
           case when r.state in ('draft', 'ne_pending', 'cancelled')
                  or exists (select 1 from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial')) then 'new_product'
                when exists (select 1 from ops.ne_del_items i join ops.ne_del_exports e on e.export_id = i.export_id
                              where i.sku_id = s.sku_id and i.action = 'delete'
                                and (i.state = 'live' or (i.state = 'withdrawn' and e.first_downloaded_at is not null and i.state_changed_at > pg_catalog.clock_timestamp() - interval '30 days'))) then 'deleting' end as why
      from sk s left join ops.master_registrations r on r.sku_id = s.sku_id
  ),
  -- 値の差がある単位 + その欄の今の値を最後に入れた変更の記録
  ch as (
    select d.*, e.event_id as ev_id, e.source_system as ev_source, e.actor_id as ev_actor, e.recorded_at as ev_at, e.new_value as ev_new, e.operation as ev_op
      from d
      left join lateral (
        select x.* from (
          -- 🆕 0098 ここから: 「NE にあるので登録済みにする」で NE 確認済みにした新商品 (登録の状態の履歴の根拠 rule = draft_in_ne_v1) は、登録 (ポータルの INSERT・
          --   source_system portal_master_edit) で人が入れた値も「人が入れた今の値」= その後に直していなくても NE と違えば送る (下の human の 🆕 0098 の行)
          (select e4.* from events.master_change_events e4
            where d.attr is not null and e4.entity_type = 'sku' and e4.entity_id = d.sku_id and e4.operation = 'INSERT'
              and exists (select 1 from ops.master_registration_events re where re.sku_id = d.sku_id and re.to_state = 'ne_confirmed' and (re.evidence ->> 'rule') = 'draft_in_ne_v1')
            order by e4.recorded_at desc, e4.event_id desc limit 1)
          union all
          -- 🆕 0098 ここまで
          (select e1.* from events.master_change_events e1
            where d.attr is not null and e1.entity_type = 'sku' and e1.entity_id = d.sku_id and e1.attribute = d.attr
            order by e1.recorded_at desc, e1.event_id desc limit 1)
          union all
          (select e2.* from events.master_change_events e2
            where d.col = 'cost' and d.sku_cost_id is not null and e2.entity_type = 'sku_cost' and e2.entity_id = d.sku_cost_id
              and (e2.operation = 'INSERT' or (e2.operation = 'UPDATE' and e2.attribute = 'cost_jpy'))
            order by e2.recorded_at desc, e2.event_id desc limit 1)
          union all
          (select e3.* from events.master_change_events e3
            where d.col = 'primary_supplier' and d.primary_n = 1 and e3.entity_type = 'supplier_sku'
              and (e3.entity_key::text) = pg_catalog.jsonb_build_object('supplier_id', d.primary_supplier_id, 'sku_id', d.sku_id)::text
              and ((e3.operation = 'INSERT' and (e3.new_value ->> 'is_primary') = 'true') or (e3.operation = 'UPDATE' and e3.attribute = 'is_primary'))
            order by e3.recorded_at desc, e3.event_id desc limit 1)) x
        order by x.recorded_at desc, x.event_id desc limit 1) e on true
     where d.ne_state = 'ok' and (d.ne is distinct from d.cdb or (d.last_out is not null and d.last_out is distinct from d.cdb))   -- 出した古い値を正す (Codex PR-2 R3 H2)
  ),
  -- 人がポータルで入れた今の値だけ (夜間ロード・NE の観測の値 = わざと残した差 は出さない)
  human as (
    select c.* from ch c
     where c.ev_source = 'portal_master_edit' and (c.attr is null or c.ev_new is not distinct from c.raw)
        or (c.ev_source = 'portal_master_edit' and c.ev_op = 'INSERT' and c.attr is not null and (c.ev_new -> c.attr) is not distinct from c.raw)   -- 🆕 0098: 登録の INSERT の値 (上の e4 = 登録済みにした新商品だけ)
  ),
  -- その変更の後のマスタの判断 (単位ごとの最新)
  dec as (
    select h.sku_id, h.col, x.kind as dec_kind, x.resolution, x.target, x.done
      from human h
      join lateral (
        select e.kind, e.resolution, e.target, e.created_at,
               exists (select 1 from ops.master_decision_events d2 where d2.kind = 'action_done' and d2.approved_event_id = e.event_id) as done
          from ops.master_decision_events e join ops.master_decision_candidates c on c.fingerprint = e.fingerprint
         where c.code_norm = h.code_norm and c.col = h.col and c.child is null and e.kind in ('approved', 'rejected', 'revoked')
         order by e.event_id desc limit 1) x on x.created_at > h.ev_at
  ),
  chg as (
    select h.sku_id, h.code, h.code_norm, h.kind, h.col, h.ord, 'change'::text as role, h.sku_name, h.ne, h.cdb, h.ev_actor, h.ev_at,
           ops.ne_upd_cell(h.col, h.cdb, h.code_norm) as cellr, sp.code_to, sp.code_why, sp.x, sp.l,
           coalesce(b.why, case when h.col = 'handling' and exists (select 1 from ops.ne_del_items i where i.sku_id = h.sku_id and i.action = 'handling' and i.created_at > h.ev_at)
                                then 'handling_csv' end) as blk_why,
           case when dc.dec_kind = 'rejected' or (dc.dec_kind = 'approved' and dc.resolution <> 'fix_ne') then 'decided'
                when dc.dec_kind = 'approved' and dc.resolution = 'fix_ne' and not dc.done and (dc.target -> 'value') is distinct from h.cdb then 'decision_other_value' end as dec_why
      from human h join sp on sp.sku_id = h.sku_id join blk b on b.sku_id = h.sku_id
      left join dec dc on dc.sku_id = h.sku_id and dc.col = h.col
  ),
  -- 送った記録 (今朝のその種類の取得の後に作った = 送っている途中 / 前に NE が受け付けた回の数)
  chg2 as (
    select c.*, (c.cellr ->> 'cell') as cell,
           coalesce((select lr.expect = c.cdb and lr.ne_code = c.code_to
              from ops.ne_upd_rows lr join ops.ne_upd_files f on f.file_id = lr.file_id join ops.ne_upd_sends s on s.send_id = lr.send_id
             where lr.code_norm = c.code_norm and lr.col = c.col and ops.ne_upd_file_out(f.state, f.claimed_at, f.downloaded_at)
               and coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) >= case c.kind when 'products' then m.products_started_at else m.sets_started_at end
             order by coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) desc, lr.row_id desc limit 1), false) as in_flight,
           (select pg_catalog.count(distinct r.send_id) from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id
             where r.code_norm = c.code_norm and r.col = c.col and r.expect = c.cdb and r.ne_code = c.code_to and f.state in ('queued', 'ok')
               and f.uploaded_at < case c.kind when 'products' then m.products_started_at else m.sets_started_at end
               and f.uploaded_at > pg_catalog.clock_timestamp() - interval '4 days') as sent_ok
      from chg c
  ),
  chg3 as (
    select c.*,
           case when not v_open then 'not_open'
                when (c.kind = 'products' and not v_p_today) or (c.kind = 'sets' and not v_s_today) or m.code_mark_run is distinct from m.compare_run_id
                     or v_code_run is distinct from m.compare_run_id then 'ne_not_today'
                when c.dec_why is not null then c.dec_why
                when c.blk_why is not null then c.blk_why
                when c.code_why is not null or c.code_to is null then coalesce(c.code_why, 'ne_code_unknown')
                when (c.cellr ->> 'ok') <> 'true' then c.cellr ->> 'why' end as why
      from chg2 c
  ),
  -- コードの書き方だけを直す行 (単品・値の差で送る行が無い SKU)。欄 = 原価 → 売価 → 名前 の順で、今朝の NE と Company DB が同じで CSV に書ける最初の欄
  carrier_sku as (
    select sp.* from sp where sp.kind = 'products' and sp.code_to is not null and sp.l is not null and sp.code_to is distinct from sp.x
       and not exists (select 1 from chg3 c where c.sku_id = sp.sku_id and c.why is null)
  ),
  car as (
    select distinct on (cs.sku_id) cs.sku_id, d.code, d.code_norm, d.kind, d.col, d.ord, 'spelling'::text as role, d.sku_name, d.ne, d.cdb, null::text as ev_actor, null::timestamptz as ev_at,
           ops.ne_upd_cell(d.col, d.cdb, d.code_norm) as cellr, cs.code_to, cs.code_why, cs.x, cs.l, b.why as blk_why, null::text as dec_why
      from carrier_sku cs join d on d.sku_id = cs.sku_id join blk b on b.sku_id = cs.sku_id
     where d.ne_state = 'ok' and d.ne = d.cdb and d.col in ('cost', 'standard_price_jpy', 'name') and (ops.ne_upd_cell(d.col, d.cdb, d.code_norm) ->> 'ok') = 'true'
     order by cs.sku_id, case d.col when 'cost' then 1 when 'standard_price_jpy' then 2 else 3 end
  ),
  car2 as (
    select c.*, (c.cellr ->> 'cell') as cell,
           exists (select 1 from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id join ops.ne_upd_sends s on s.send_id = r.send_id
                    where r.code_norm = c.code_norm and r.ne_code = c.code_to and coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) >= m.products_started_at
                      and ops.ne_upd_file_out(f.state, f.claimed_at, f.downloaded_at)) as in_flight,
           (select pg_catalog.count(distinct r.send_id) from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id
             where r.code_norm = c.code_norm and r.ne_code = c.code_to and f.state in ('queued', 'ok') and f.uploaded_at < m.products_started_at
               and f.uploaded_at > pg_catalog.clock_timestamp() - interval '4 days') as sent_ok,
           case when not v_open then 'not_open'
                when not v_p_today or m.code_mark_run is distinct from m.compare_run_id or v_code_run is distinct from m.compare_run_id then 'ne_not_today'
                when c.blk_why is not null then c.blk_why end as why
      from car c
  ),
  allu as (
    select c.sku_id, c.code, c.code_norm, c.kind, c.col, c.ord, c.role, c.sku_name, c.ne, c.cdb, c.cell, c.code_to, c.x, c.l, c.ev_actor, c.ev_at, c.in_flight, c.sent_ok, c.why from chg3 c
    union all
    select c.sku_id, c.code, c.code_norm, c.kind, c.col, c.ord, c.role, c.sku_name, c.ne, c.cdb, c.cell, c.code_to, c.x, c.l, c.ev_actor, c.ev_at, c.in_flight, c.sent_ok, c.why from car2 c
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'sku_id', a.sku_id::text, 'code', a.code, 'code_norm', a.code_norm, 'kind', a.kind, 'col', a.col, 'ord', a.ord, 'role', a.role, 'name', a.sku_name,
           'ne_value', a.ne, 'cdb_value', a.cdb, 'cell', case when a.why is null then a.cell end, 'ne_code', case when a.why is null then a.code_to end,
           'ne_spelling', a.x, 'lz_spelling', a.l, 'spelling_fix', a.code_to is not null and a.code_to is distinct from a.x,
           'status', case when a.why is not null then 'blocked' when a.in_flight then 'sending' when a.sent_ok >= 2 then 'stuck' else 'send' end,
           'why', a.why, 'changed_by', a.ev_actor, 'changed_at', a.ev_at, 'sent_count', a.sent_ok)
         order by a.code_norm, a.kind, a.ord), '[]'::jsonb)
    into v_units
    from allu a;

  return pg_catalog.jsonb_build_object('ok', v_open and m.id is not null and m.code_mark_run is not distinct from m.compare_run_id and v_code_run is not distinct from m.compare_run_id
                                              and (v_p_today or v_s_today),
    'problems', v_problems,
    'ne', case when m.id is null then null else pg_catalog.jsonb_build_object('run', m.compare_run_id, 'products_started_at', m.products_started_at, 'sets_started_at', m.sets_started_at,
            'products_today', v_p_today, 'sets_today', v_s_today, 'norm_version', m.norm_version) end,
    'lz', pg_catalog.jsonb_build_object('exported_at', v_lz_at, 'fresh', v_lz_fresh),
    'blank_ok', v_blank, 'units', v_units);
end $$;

-- ─── 権限 (0079 と同じ形)。master_edit には scripts/company-db/create-master-edit-roles.mjs の流し直しでも付く (表は増やさない・関数の実行だけ) ───
--   master_edit = 確かめ・登録済みにする / watcher = 確かめ (読むだけ)。だれにも渡さない = ops._draft_in_ne_problem (確かめの中の部品)。
--   create or replace の関数 (地図・書いてよい表・状態を進める・NE へ送るの差) と view は今の権限のまま
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'master_edit') then
    execute 'grant usage on schema ops to master_edit';
    execute 'grant execute on function ops.draft_in_ne_check(bigint) to master_edit';
    execute 'grant execute on function ops.confirm_draft_in_ne(uuid, text, bigint, text) to master_edit';
  end if;
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'watcher') then
    execute 'grant execute on function ops.draft_in_ne_check(bigint) to watcher';
  end if;
end $$;
