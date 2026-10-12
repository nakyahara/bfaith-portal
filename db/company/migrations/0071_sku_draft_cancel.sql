-- 0071: 新商品の登録の下書きで、まだ NE に配っていない商品を消す (= 登録をやめた cancelled にする) (2026-10-11。中原さんの頼み 10/11「自社商品 DB で商品を削除できるようにしたい」の場面 a)
-- 🚨 前提 = 0067 (約束の操作の CHECK・ops.master_write_allowed の今の版)・0065 (ops.sku_ever_issued)・0058 (ops.ne_code_seen)・0053 (core.guard_master_edit_jan の今の版)。
--    0070 (#1683・available → cancelled「NE から消えた」) の後の番号。0070 の関数・trigger は使わない・置き換えない (0070 が無い DB でも動く)。
--    約束の操作の CHECK だけは 0070 の版 (0067 の全部 + sku_gone_cancel) に sku_draft_cancel を足した (0070 が無い DB でも CHECK が広いだけ)
--
-- なぜ: 新商品の登録の画面で作った下書き (入れ間違い・発売中止) を消す道が無い。NE 登録の CSV を一度も配っていない下書きは NE に無い。
--   ロジザードの商品マスタへ出す CSV は NE の取得の商品だけを回す (apps/master-decisions/lz-cdb.mjs の classifyForLz・scripts/company-db/lz-daily.mjs) = NE に配る前の下書きは出さない
--   (手でロジザードに登録する道は人の決まり)。product-hub の出品カードは画面が先に読んで、除外していないカードがあれば消さない (Postgres の外)
-- 「消す」の決め方 = 行を物理的に消さない。登録の状態を cancelled (やめた) にして、画面の一覧から隠す:
--   ・登録の状態の行・履歴 (ops.master_registration_events)・保存の記録・約束・変更の記録 (events.master_change_events)・カードの知らせ・NE 登録の CSV の品目は
--     どれも「消さない」の守り (DELETE の trigger・追記だけ・外部キー) がある = SKU の行を消すにはこの守りを全部外すことになる (監査が消える)
--   ・0067 の子の廃止 (ops.cancel_variation_child)・0070 の NE から消えた と同じ cancelled = 読み手 (新しいセットの構成品・NE 登録の CSV・まとまりのスナップショット・
--     0070 の参照の守り・画面の保存) は前から cancelled を「使わない」と扱う
--   ・🚨 消したコードはもう使えない (SKU の行が残る = ops.new_sku_code_problem の code_taken。0067 / 0070 と同じ「やめたコードは使い回さない」)。
--     値の入れ間違いなら消さずに直す (下書きは商品の画面で直せる)。種類 (単品 / セット) の間違い = 別のコードで登録し直す
-- なにを:
--   1. ops.sku_draft_cancel_check(SKU) = 消してよいかを DB が今の値で確かめる (読むだけ)。全部を満たすときだけ ok:
--        会社 1・登録の状態が下書き (draft)・NE 登録の CSV を一度も配っていない (ops.sku_ever_issued)・NE に一度も現れていない (ops.ne_code_seen = 今の NE と前に見たコード)・
--        まとまりの子でない (= 0067 の「子の廃止」の道)・ほかの商品の代表 (親) でない・ほかのセットの構成品でない・ほかのセットの開いている構成の依頼に入っていない・
--        販路の出品の構成に無い (削除済みの出品も = 0070 と同じ)・カードの知らせを取り込み中でない (借りの期限の中)
--      🚨 NE 登録待ち (ne_pending) は下書きから CSV を配って申告した後にしか来ない (0063 の行き先の決まり) = いつも「一度配った」= 消さない
--      片付ける物の見込み (cleanup) も返す: 作っただけの NE 登録の CSV (ファイルと、一緒に作り直しになるほかの商品)・カードの知らせ・外す JAN・閉じる構成の依頼
--   2. ops.cancel_sku_draft(request_id, 人, 理由, SKU) = 消す (security definer・1 件ずつ・1 つの取引・理由が要る・押し直し = 前の答え)。同じ取引で:
--        a. 作っただけ (built) の NE 登録の CSV = そのファイルを使わない (品目 superseded・ファイル closed) にする (ほかの商品も入っていれば、その商品は作り直し = 0053 の
--           ops.ne_reg_supersede_built と同じ扱い・理由の文だけ違う)
--        b. セットの下書き自身の開いている構成の依頼 = 取り下げ (cancelled / withdrawn)。残すと夜間の昇格・0070 の「構成の依頼に入っている」に当たる
--        c. 商品の有効な JAN = 外す (valid_to = 今)。残すと、正しいコードで登録し直すときに同じ JAN が jan_taken で断られる (その商品の SKU がこの下書きだけのとき)
--        d. product-hub のカードの知らせ (まだ取り込んでいない = pending / failed / conflict) = 済み (done) にして結果に「作らない (下書きを消した)」= もうカードを作らない。
--           もう取り込んだ (done) = 知らせはそのまま。カード (Render の SQLite) は画面が Postgres の取引の前に読み、除外していないカードが 1 枚でもあれば消さない
--           (409 card_exists = 人が先に product-hub で除外する・Codex #1688 R1 High 2・3。Postgres と SQLite は 1 つの取引にできない)
--        e. 登録の状態 draft → cancelled (0052 の行き先の地図に前からある・ops.transition_sku_registration (下の 5. = この約束の中だけ通す) = 人の理由・根拠は DB が作った片付けの中身)
--        f. 保存の記録 done (0053 の ops.close_reg_write)
--      残す (消さない) 物: SKU・商品の行・原価・仕入先 (記録として。読み手は登録の状態で使わないと分かる)・発注の設定 (Render の SQLite = 発注アプリは NE にある商品だけを計算する)
--      鍵: 段階 (共有) → マスタの書き込み (共有) → SKU → CSV → NE のコード (共有) → 登録の状態の行 (排他 = 0070 の参照の守りの共有と並ぶ) → SKU・商品の行 → カードの知らせの行
--   3. 約束の操作 sku_draft_cancel (ops.master_write_sessions・ops.master_edit_requests の CHECK) と、その操作で画面のロールが書いてよい表 (ops.master_write_allowed = 0067 の版 + 6 行):
--        NE 登録の CSV の 2 つの表の UPDATE (作っただけのファイルを使わないにする = 0053 の守りがそれ以外を断る)・構成の依頼の UPDATE (取り下げ)・
--        JAN の UPDATE (外す)・SKU と商品の UPDATE (JAN を外したときの version の付け替え = 0053 の core.bump_jan_owner_version)
--   4. core.guard_master_edit_jan (0053 の版から写して 🆕 0071 の行だけ足した): 画面のロールの JAN の書き込みに、約束 sku_draft_cancel の「外す」(UPDATE) だけを足す
--   5. ops.transition_sku_registration (0070 (#1683) の版から写して 🆕 0071 の行だけ足した・Codex #1688 R1 High 1): 下書き・NE 登録待ち → やめた は、
--      約束 sku_draft_cancel (この関数)・variation_child_cancel (0067 の子の廃止) の中だけ (同じ SKU・人・request_id)。運用のロール (master_ops) が直に呼んでも断る
--      (表の持ち主のロールとそのメンバーは求めない)
-- 🚨 この migration は商品・登録の状態の値を何も変えない (関数と守りを足すだけ)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit に 1. と 2. の実行を足す (流さなくても今の動きは変わらない)

-- ═══════════ 3. 約束の操作 (0070 の CHECK の版 = 0067 の全部 + sku_gone_cancel から写して sku_draft_cancel を 1 つ足しただけ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel'));

-- 操作ごとに、画面のロールが書いてよい (表・書き方)。0067 の版から写して 🆕 0071 の行 (ここから〜ここまで) だけ足した
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
$$;

-- ═══════════ 4. 画面のロールの JAN の書き込み (0053 の版から写して 🆕 0071 の行だけ足した) ═══════════
-- 画面のロール (master_edit) の JAN の行の書き込み (#1571 R1 High 3・⑤-2a の知らせの守りと同じ作り): JAN の約束 (jan_edit = ops.edit_sku_jan の中) だけ・
--   約束の商品の JAN の行だけ・段階 new_open・始めたときの持ち主表で external_ids.jan が company・足す行は人が決めた (manual) 約束の人の行だけ。
--   0051 の guard (trg_master_edit_guard) は core.external_ids の相手を知らない = 付けない (ここで見る)。表の権限は画面のロールに渡さない
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない
create or replace function core.guard_master_edit_jan() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  v_sess := ops.current_master_write_session();
  if v_sess.session_id is null or v_sess.operation is distinct from 'jan_edit' then
    -- 🆕 0071 ここから: 下書きを消す (ops.cancel_sku_draft の中) = 約束の商品の JAN を外す (UPDATE) だけ。ほかは今までどおり断る
    if not (v_sess.session_id is not null and v_sess.operation = 'sku_draft_cancel' and tg_op = 'UPDATE') then
    -- 🆕 0071 ここまで
    raise exception 'master_write_session_required: 画面のロールの JAN の書き込みは JAN の約束 (ops.edit_sku_jan) の中だけ' using errcode = '42501';
    end if;   -- 🆕 0071
  end if;
  if not ops.master_write_allowed(v_sess.operation, 'core.external_ids', tg_op) then
    raise exception 'master_write_operation: 約束の操作 % では core.external_ids に % できない', v_sess.operation, tg_op using errcode = '42501';
  end if;
  if (select s.phase from ops.master_cutover_state s where s.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない (core.external_ids)' using errcode = '42501';
  end if;
  if (tg_op <> 'INSERT' and not (old.entity_type = 'product' and old.system = 'jan' and old.entity_id = any (v_sess.target_product_ids)))
     or (tg_op <> 'DELETE' and not (new.entity_type = 'product' and new.system = 'jan' and new.entity_id = any (v_sess.target_product_ids))) then
    raise exception 'master_write_target: JAN の約束の商品の JAN の行でない' using errcode = '42501';
  end if;
  if (v_sess.ownership ->> 'external_ids.jan') is distinct from 'company' then
    raise exception 'owner_not_company: external_ids.jan の持ち主が company でない (core.external_ids)' using errcode = '42501';
  end if;
  if tg_op = 'INSERT' and (new.resolution is distinct from 'manual' or new.resolved_by_type is distinct from 'human' or new.resolved_by_id is distinct from v_sess.actor_id) then
    raise exception 'master_write_session_mismatch: 足す JAN の行は人が決めた (manual) 約束の人の行だけ' using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function core.guard_master_edit_jan() from public;

-- ═══════════ 1. 消してよいかの確かめ (読むだけ) ═══════════
/**
 * 下書きを消してよいか。何も書かない。戻り値 = {
 *   sku_id (文字), code, ok, problems: [{ reason, text }] (空 = ok), reg_state, sku_kind,
 *   cleanup: { reg_csv: [{ export_id, codes (ほかの商品のコード = 一緒に作り直しになる) }], card: { event_id, status, outcome, draft_id } | null,
 *              jans: [JAN] (外す), component_request: 番号 | null (取り下げる) } }
 * reason: no_sku / not_company / not_draft / ever_issued / seen_in_ne / variation_child / parent_of_others / set_component / component_request /
 *         listing_component / listing_component_deleted / card_busy
 */
create function ops.sku_draft_cancel_check(p_sku_id bigint) returns jsonb
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

-- ═══════════ 2. 消す ═══════════
/**
 * 新商品の登録の下書き (NE に一度も配っていない) を消す = 登録をやめた (cancelled)。人が理由 (200 字まで・改行なし) を書いてだけ・1 件ずつ・1 つの取引。
 *   同じ request_id の押し直し = 前の答え (replayed)。違う操作・人・SKU・理由で同じ request_id = request_id_reused
 *   鍵: 段階 (共有) → マスタの書き込み (共有) → SKU → CSV (ops.ne_reg_lock_skus = NE 登録の CSV を作る・配る・使わないと並ぶ) → NE のコード (共有) →
 *       登録の状態の行 (排他 = 0070 の参照の守りの共有と並ぶ) → SKU・商品の行 (排他 = 構成・出品の構成・親の外部キーと並ぶ) → カードの知らせの行 (排他 = 借りる関数の skip locked)
 *   段階 new_open・持ち主表 = DB の active が段階の記録と同じ (0053 の ops.reg_write_gate)。確かめ (ops.sku_draft_cancel_check) が ok でなければ not_cancellable (理由を全部)
 *   約束 (0051 の ops.master_write_sessions・操作 sku_draft_cancel・この関数だけが書く) → 片付け (上の 2. の a〜d) → 登録の状態 (e) → 保存の記録 done (f)。
 *   戻り値 = { ok, request_id, code, sku_id, from, state, event_id, cleanup: { reg_csv, card, jans, component_request } }
 * 呼べるロール = master_edit (画面・名簿 MASTER_EDITORS の人)。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)。DB が残すのは db_user と約束の中身
 */
create function ops.cancel_sku_draft(p_request_id uuid, p_actor_id text, p_reason text, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  v_s       record;
  v_chk     jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_exports bigint[];
  v_csv     jsonb;
  v_req     bigint;
  v_jans    jsonb := '[]'::jsonb;
  v_card    jsonb;
  v_ev      jsonb;
  v_tr      jsonb;
  v_result  jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: 消すのは人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);   -- 段階 (共有) → マスタの書き込み (共有) → 段階 new_open・持ち主表 = 段階の記録
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'sku_draft_cancel', 'sku_id', p_sku_id::text, 'reason', v_reason));
  perform ops.ne_reg_lock_skus(array[p_sku_id]);   -- SKU → CSV (同じ SKU の 2 つ目の押し直しはここで前の取引の commit を待つ = 下で前の答えを見る)
  select x.operation, x.actor_id, x.status, x.result, x.payload_hash, x.sku_id into v_prev from ops.master_edit_requests x where x.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'sku_draft_cancel' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done'
       or v_prev.payload_hash is distinct from v_hash or v_prev.sku_id is distinct from p_sku_id then
      raise exception 'request_id_reused: request_id % はほかの操作・人・商品・理由で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));   -- 「NE に一度も現れていない」を commit まで照合に入れ替えさせない (0067 の子の廃止と同じ)
  perform 1 from ops.master_registrations x where x.sku_id = p_sku_id for update;
  select k.code, k.product_id into v_s from core.skus k where k.sku_id = p_sku_id and k.company_id = 1 for update;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if v_s.product_id is not null then perform 1 from core.products p where p.product_id = v_s.product_id for update; end if;
  perform 1 from ops.product_hub_outbox x where x.sku_id = p_sku_id and x.kind = 'card_create' for update;
  v_chk := ops.sku_draft_cancel_check(p_sku_id);
  if (v_chk ->> 'ok') is distinct from 'true' then
    raise exception 'not_cancellable: % は消せない (%)', v_s.code, (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x)
      using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'sku_draft_cancel', p_sku_id, '{}', case when v_s.product_id is null then '{}'::bigint[] else array[v_s.product_id] end,
            pg_catalog.repeat('0', 64), v_hash, pg_catalog.jsonb_build_object('sku_id', p_sku_id::text), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase,
            ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  -- a. 作っただけ (built) の NE 登録の CSV = そのファイルを使わないにする (中のほかの商品も = 作り直し。0053 の ops.ne_reg_supersede_built と同じ形・理由の文だけ違う)
  v_csv := v_chk #> '{cleanup,reg_csv}';
  select pg_catalog.array_agg(distinct i.export_id) into v_exports from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state = 'built';
  if v_exports is not null then
    update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left('配る前に下書き ' || v_s.code || ' を消した (' || v_reason || ')', 500),
           superseded_correction = 'まだ配っていない (NE には何もしていない)。ほかの商品は作り直す', state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id
     where export_id = any (v_exports) and state = 'built';
    update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor_id, close_reason = 'superseded' where export_id = any (v_exports) and state = 'built';
  end if;
  -- b. セットの下書き自身の開いている構成の依頼 = 取り下げ
  update ops.sku_component_requests set status = 'cancelled', closed_at = pg_catalog.now(), closed_by = p_actor_id, close_reason = 'withdrawn'
   where set_sku_id = p_sku_id and status = 'open'
  returning component_request_id into v_req;
  -- c. 商品の有効な JAN を外す (その商品の SKU がこの下書きだけのとき = 確かめの見込みと同じ)
  if pg_catalog.jsonb_array_length(v_chk #> '{cleanup,jans}') > 0 then
    with u as (
      update core.external_ids set valid_to = pg_catalog.now()
       where entity_type = 'product' and entity_id = v_s.product_id and system = 'jan' and id_kind = 'jan' and valid_to is null
      returning external_value)
    select coalesce(pg_catalog.jsonb_agg(u.external_value order by u.external_value), '[]'::jsonb) into v_jans from u;
  end if;
  -- d. カードの知らせ (まだ取り込んでいない) = 済み・結果「作らない (下書きを消した)」= もう取り込まない (借りる関数は done を取らない)
  v_card := v_chk #> '{cleanup,card}';
  if v_card is not null and jsonb_typeof(v_card) = 'object' and (v_card ->> 'status') <> 'done' then
    update ops.product_hub_outbox set status = 'done', done_at = pg_catalog.now(), lease_owner = null, leased_until = null, last_error = null, updated_at = pg_catalog.now(),
           result = pg_catalog.jsonb_build_object('outcome', 'cancelled', 'reason', v_reason, 'by', p_actor_id, 'request_id', p_request_id::text, 'status_before', v_card ->> 'status',
                                                  'draft_id', null)
     where sku_id = p_sku_id and kind = 'card_create' and status <> 'done';
    v_card := v_card || pg_catalog.jsonb_build_object('status', 'done', 'outcome', 'cancelled', 'status_before', v_card ->> 'status');
  end if;
  -- e. 登録の状態 draft → cancelled (根拠 = DB が作った片付けの中身)
  v_ev := pg_catalog.jsonb_build_object('rule', 'draft_cancel_v1', 'never_issued', true, 'seen_in_ne', false,
    'reg_csv_closed', coalesce(v_csv, '[]'::jsonb), 'component_request', case when v_req is null then null else v_req::text end, 'jans_removed', v_jans, 'card', v_card);
  v_tr := ops.transition_sku_registration(p_sku_id, 'cancelled', 'human', p_actor_id, v_reason, v_ev, p_request_id::text);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', v_s.code, 'sku_id', p_sku_id::text, 'from', 'draft', 'state', 'cancelled',
    'event_id', v_tr ->> 'event_id',
    'cleanup', pg_catalog.jsonb_build_object('reg_csv', coalesce(v_csv, '[]'::jsonb), 'card', v_card, 'jans', v_jans, 'component_request', case when v_req is null then null else v_req::text end));
  -- f. 保存の記録 done (約束を閉じる = この関数の外では、同じ取引でも画面のロールはこの約束で書けない)
  perform ops.close_reg_write(v_result, v_s.code);
  return v_result;
end $$;
revoke all on function ops.cancel_sku_draft(uuid, text, text, bigint) from public;

-- ═══════════ 5. 登録の状態を 1 つ進める (0070 (#1683) の版から写して 🆕 0071 の行だけ足した・Codex #1688 R1 High 1) ═══════════
-- 下書き・NE 登録待ち → やめた は、下書きを消す・子の廃止の約束の中だけ (運用のロール master_ops が直に呼んで条件と片付けを迂回しない)。
--   要確認 (quarantined) → やめた は今までどおり (人の理由)・業務で使える → やめた は 0070 の決まりのまま
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
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
revoke all on function ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text) from public;

-- ─── 権限 (0070 と同じ形)。master_edit には scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表は読ませない・関数の実行だけ) ───
--   watcher = 確かめ (読むだけ) の実行
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function ops.sku_draft_cancel_check(bigint) to watcher';
  end if;
end $$;
