-- 0072: NE にもう登録した商品を「NE から消す」CSV を作る → 人が NE に取り込む → NE から消えたのを確かめて Company DB でもやめる (2026-10-12。
--       中原さんの頼み 10/11「自社商品 DB で商品を削除できるようにしたい」の場面 c・決定 10/12 Q1 a / Q2 a / Q3 a / Q4 b)
-- 🚨 前提 = 0071 (約束の操作の CHECK・ops.master_write_allowed の今の版)・0070 (ops.sku_gone_from_ne_check・ops.cancel_sku_gone_from_ne・
--    ops.guard_sku_ref_not_cancelled・ops.master_ne_absence)・0053 (ops.reg_write_gate・ops.close_reg_write・ops.ne_reg_lock_skus)・0041 (ops.master_ne_codes)・
--    0011 (snapshots.sku_stock_daily)
--
-- なぜ: NE の商品のマスタの削除は CSV だけ (公開のマニュアル ID:8641「商品管理データの削除 (マスタ削除)」・API の口は無い)。ポータルで「消す」と決めた商品の
--   削除の CSV を作り、人が NE に取り込み、NE から消えたのを毎朝の照合 (0070 の「完全な NE の取得に JST の取得の日ごとに 1・3 日続けて無い」) が確かめてから
--   Company DB でもやめる (cancelled)。NE のマスタへの書き込みは全部「CSV を作る → 人が NE に取り込む → 翌朝の照合で確かめる」(0040・0053 と同じ)
-- NE の削除の CSV の形 (マニュアル ID:8641 のひな形 syohindel.csv = 実物のバイト列 83 52 81 5B 83 68 0D 0A):
--   見出し「コード」の 1 列だけ・Shift_JIS・CRLF。単品 = [設定]→[マスタ削除]→「商品マスタ」/ セット = 「セット商品マスタ」で取り込む (別のファイル)。
--   削除のフラグの列は無い = 書いたコードが全部消える。NE が断る = セットに入っている (先にセットを消す)・有効な受注 / 発注の明細・出荷前の受注に引き当たっている在庫。
--   🚨 マニュアルの断る一覧に「在庫がある」は無い = 在庫があっても消える恐れ → 在庫が残っている商品は入れない (決定 Q3 a)。
--   消すと一緒に消える (警告なし) = 単品のページ情報・同じ代表の子を全部消すと代表のページ情報も・商品コード紐付
-- コードは NE の元の書き方 (0041 の ops.master_ne_codes・大文字も) = 英数字と _- だけ (表の CHECK) = Shift_JIS でも ASCII と同じバイト。見出しだけ Shift_JIS の「コード」
-- なにを:
--   1. ops.ne_del_exports (ファイル = 作った人・時刻・JST の日・理由・見出し・バイト列・sha256・初めてのダウンロード) /
--      ops.ne_del_items (ファイルの商品ごと = 単品 / セット・NE の元のコード・確かめの答え (在庫の値を含む)・状態)。表を書くのは下の関数だけ (取引の中の印・持ち主の手も止める)・消さない
--      品目の状態 (消す): live (依頼の途中) → withdrawn (人が取り下げた) / handled (代わりに取扱中止の CSV を作った) / gone (NE から消えた = Company DB でやめた)
--      品目 (取扱中止): recorded (記録だけ)。SKU ごとに live の品目は 1 つ (部分の一意の索引)
--   2. ops.ne_del_check(SKU, 同じ依頼の SKU, アプリが読んだ在庫) = 消す CSV に入れてよいかを DB が今の値で確かめる (読むだけ)。全部を満たすときだけ ok:
--        会社 1・登録の状態 available・単品かセット・依頼の途中でない・NE のコードの台帳 (0041) に書き方が 1 つ (印が 36 時間の中)・一番新しい完全な NE の取得 (0070) に無い でない
--        (= もう消えた商品は「NE から消えた (やめる)」へ)・生きた NE 登録の CSV の品目が無い・(単品) それを中身に持つ生きたセットが無い (同じ依頼か依頼の途中のセットは除く =
--        セットのファイルを先に取り込む)・ほかのセットの開いている構成の依頼に無い・(セット) 自身の開いている構成の依頼が無い・出品の構成に無い (削除済みの出品も)・ほかの商品の親でない・
--        (単品) 在庫 0 (決定 Q3 a): Company DB の在庫の日次 (NE = 前日か今日の complete の日に行があって 0・ロジザード = 2 日の中の complete の日に在庫 0・引当 0) と
--        アプリが読んだ値 (NE の在庫・引当・発注残 = 商品管理リストの公開の回 / FBA の在庫 / ロジザードの今の在庫と引当 (毎時の写し・別々に) / 発注アプリの注文残) が全部 0。
--        どれかを読めない = 断る (DB はアプリの値を確かめられない = 記録に残す)
--   3. ops.ne_del_build(request_id, 人, 理由, SKU の配列, アプリが読んだ在庫) = 確かめ直して (鍵の後) ファイルを作る (セット → 単品の 2 つまで)・記録・約束 ne_del_build・保存の記録
--   4. ops.ne_del_download(人, ファイル) = バイト列を渡す (作った JST の日だけ・中の消す品目が全部まだ依頼の途中のときだけ)・初めてのダウンロードを残す
--   5. ops.ne_del_withdraw(request_id, 人, 理由, 品目) = 依頼を取り下げる (live → withdrawn・約束 ne_del_withdraw)
--   6. 断られたとき (決定 Q2 a): ops.ne_del_refused_check(品目) = 依頼のファイルを初めてダウンロードした JST の日から 3 日以上たって、その後の一番新しい完全な NE の取得にまだある (単品)。
--      ops.ne_handling_build(request_id, 人, 理由, 品目の配列) = 代わりに「取扱中止」の CSV (apps/master-decisions/ne-csv.mjs の products:handling = 見出し syohin_code,toriatukai_kbn・
--      値 1・UTF-8・CRLF・9/27 実機で確かめ済みの形) を作る・消す品目は handled (依頼は終わり = 参照の守りを外す)
--   7. ops.ne_del_overview(SKU | null) = 画面の一覧 (依頼の途中の全部と 30 日の中に終わった品目・ファイル (バイト列なし)・状態 = まだダウンロードしていない / 待ち /
--      NE に無い N 日 / NE から消えた (やめられる) / 断られたかも)
--   8. 0070 の「NE から消えた → やめる」にセットの道を足す (決定 Q4 b): ops.sku_gone_from_ne_check (0070 の版から写して 🆕 0072 の行だけ足した) = セットも ok にできる・
--      セット自身の開いている構成の依頼は断る。ops.cancel_sku_gone_from_ne (0070 の版から写して 🆕 0072 の行だけ) = セットは構成 (core.sku_components の親がこのセット) を
--      同じ取引で外す (中身の単品が「構成品」でなくなる = 単品の依頼・やめるに進める)・NE から消す依頼の品目を gone に
--   9. 依頼の途中の SKU を新しい参照 (構成の子・構成の依頼・出品の構成 (Amazon の対応も)・代表) に入れない: ops.guard_sku_ref_not_cancelled (0070 の版から写して 🆕 0072 の行だけ) +
--      ops.sku_ref_ne_deleting。やめたセット (親) に構成を作らない (夜間ロードが NE の取得の古い行で戻さない = miniPC の raw_ne_set_products は消えたセットの行を消さない)。
--      構成の依頼のセット自身も見る。夜間ロード (engine.mjs) はこれらを先に飛ばす (止めない)
--  10. 約束の操作 (CHECK・0071 の版 + 3 つ) と ops.master_write_allowed (0071 の版から写して 🆕 0072 の行 = 構成を外したときのセットの version の付け替え)
-- 🚨 この migration は商品・登録の状態の値を何も変えない (表と関数と守りを足すだけ)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit に 2〜7 の実行を足す (流さなくても今の動きは変わらない = 画面は「まだ使えない」)

-- ═══════════ 10. 約束の操作 (0071 の CHECK の版 = 0070 の全部 + sku_draft_cancel から写して 3 つ足しただけ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build'));

-- 操作ごとに、画面のロールが書いてよい (表・書き方)。0071 の版から写して 🆕 0072 の行 (ここから〜ここまで) だけ足した
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
$$;

-- ═══════════ 1. 表 ═══════════
create table ops.ne_del_exports (
  export_id           bigint generated always as identity primary key,
  company_id          smallint not null default 1 references core.companies,
  kind                text not null check (kind in ('products', 'sets', 'handling')),
  schema_version      text not null,
  encoding            text not null,
  header              text not null,
  file_bytes          bytea not null,
  sha256              text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  row_count           integer not null check (row_count between 1 and 1000),
  request_id          uuid not null,
  reason              text not null check (pg_catalog.length(reason) between 1 and 200),
  built_by            text not null check (pg_catalog.length(built_by) between 1 and 320),
  built_at            timestamptz not null default pg_catalog.clock_timestamp(),
  built_day           date not null,   -- JST の作った日 (ダウンロードはこの日だけ)
  db_user             text not null,
  first_downloaded_at timestamptz,
  first_downloaded_by text,
  download_count      integer not null default 0 check (download_count >= 0),
  constraint ck_nde_shape check ((kind, schema_version, encoding, header) in (('products', 'ne-del-products-v1', 'sjis', 'コード'), ('sets', 'ne-del-sets-v1', 'sjis', 'コード'),
                                                                             ('handling', 'ne-handling-v1', 'utf8', 'syohin_code,toriatukai_kbn'))),
  constraint ck_nde_sha check (sha256 = pg_catalog.encode(pg_catalog.sha256(file_bytes), 'hex')),
  constraint ck_nde_download check ((first_downloaded_at is null) = (first_downloaded_by is null) and (first_downloaded_at is null) = (download_count = 0))
);
comment on table ops.ne_del_exports is 'NE から消す CSV (単品 / セット) と、断られたときの取扱中止の CSV (0072)。書くのは ops.ne_del_build / ops.ne_handling_build / ops.ne_del_download だけ';

create table ops.ne_del_items (
  item_id          bigint generated always as identity primary key,
  export_id        bigint not null references ops.ne_del_exports (export_id),
  company_id       smallint not null default 1 references core.companies,
  sku_id           bigint not null,
  code_norm        text not null,
  ne_code          text not null check (ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(ne_code) = code_norm),
  sku_kind         text not null check (sku_kind in ('single', 'set')),
  action           text not null check (action in ('delete', 'handling')),
  state            text not null,
  replaces_item_id bigint references ops.ne_del_items (item_id),
  evidence         jsonb not null check (pg_catalog.jsonb_typeof(evidence) = 'object'),
  created_at       timestamptz not null default pg_catalog.clock_timestamp(),
  state_changed_at timestamptz not null default pg_catalog.clock_timestamp(),
  state_changed_by text not null check (pg_catalog.length(state_changed_by) between 1 and 320),
  state_reason     text check (state_reason is null or pg_catalog.length(state_reason) <= 200),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  unique (export_id, sku_id),
  constraint ck_ndi_state check ((action = 'delete' and state in ('live', 'withdrawn', 'handled', 'gone') and replaces_item_id is null)
                              or (action = 'handling' and state = 'recorded' and sku_kind = 'single' and replaces_item_id is not null))
);
create unique index ux_ne_del_items_live on ops.ne_del_items (sku_id) where state = 'live';
create index ix_ne_del_items_sku on ops.ne_del_items (sku_id, item_id);
create index ix_ne_del_items_export on ops.ne_del_items (export_id);
comment on table ops.ne_del_items is 'NE から消す CSV の商品ごとの品目と状態 (0072)。live = 依頼の途中 (新しい参照に入れない)。書くのは 0072 の関数と ops.cancel_sku_gone_from_ne だけ';

/** 品目の状態の行き先 (一方向。戻さない) */
create function ops.ne_del_item_transition_allowed(p_from text, p_to text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select (p_from || '>' || p_to) = any (array['live>withdrawn', 'live>handled', 'live>gone'])
$$;
revoke all on function ops.ne_del_item_transition_allowed(text, text) from public;

-- 表の守り: 書くのは 0072 の関数 (と ops.cancel_sku_gone_from_ne) だけ (取引の中の印 ops.ne_del_protocol = '1' = 持ち主の手の DML も止める)・消さない・
--   ファイル = 中身は変えない (ダウンロードの記録の列だけ)・品目 = 状態を一方向に進める列だけ
create function ops.guard_ne_del_write() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_del_write: % の行は消さない', tg_table_name using errcode = '42501'; end if;
  if coalesce(pg_catalog.current_setting('ops.ne_del_protocol', true), '') is distinct from '1' then
    raise exception 'ne_del_write: % は NE から消す CSV の関数 (ops.ne_del_build ほか) の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  if tg_op = 'UPDATE' then
    if tg_table_name = 'ne_del_exports' then
      if (pg_catalog.to_jsonb(new) - 'first_downloaded_at' - 'first_downloaded_by' - 'download_count') is distinct from (pg_catalog.to_jsonb(old) - 'first_downloaded_at' - 'first_downloaded_by' - 'download_count')
         or (old.first_downloaded_at is not null and (new.first_downloaded_at, new.first_downloaded_by) is distinct from (old.first_downloaded_at, old.first_downloaded_by))
         or new.download_count < old.download_count then
        raise exception 'ne_del_write: ファイルの中身は変えない (ダウンロードの記録だけ)' using errcode = '42501';
      end if;
    else
      if (pg_catalog.to_jsonb(new) - 'state' - 'state_changed_at' - 'state_changed_by' - 'state_reason') is distinct from (pg_catalog.to_jsonb(old) - 'state' - 'state_changed_at' - 'state_changed_by' - 'state_reason')
         or not ops.ne_del_item_transition_allowed(old.state, new.state) then
        raise exception 'ne_del_write: 品目は状態を一方向に進めるだけ (% → %)', old.state, new.state using errcode = '42501';
      end if;
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_ne_del_write() from public;
create trigger trg_ne_del_exports_guard before insert or update or delete on ops.ne_del_exports for each row execute function ops.guard_ne_del_write();
create trigger trg_ne_del_exports_no_truncate before truncate on ops.ne_del_exports for each statement execute function core.reject_mutation();
create trigger trg_ne_del_items_guard before insert or update or delete on ops.ne_del_items for each row execute function ops.guard_ne_del_write();
create trigger trg_ne_del_items_no_truncate before truncate on ops.ne_del_items for each statement execute function core.reject_mutation();

-- ═══════════ 2. 消す CSV に入れてよいかの確かめ (読むだけ) ═══════════
/** 確かめの理由の 1 行 (配列 1 つ) */
create function ops._ne_del_problem(p_reason text, p_text text) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', p_reason, 'text', p_text))
$$;
revoke all on function ops._ne_del_problem(text, text) from public;
/** アプリが読む在庫の値の鍵 (lib/master-ne-delete.mjs の APP_STOCK_KEYS と同じ・試験で照らす) と言葉 */
create function ops.ne_del_app_stock_keys() returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select '{"ne_own": "NE の在庫", "ne_allocated": "NE の引当", "ne_on_order": "NE の発注残", "fba": "FBA の在庫", "logizard": "ロジザードの在庫 (今)", "logizard_allocated": "ロジザードの引当 (今)", "po_backorder": "発注アプリの注文残"}'::jsonb
$$;
revoke all on function ops.ne_del_app_stock_keys() from public;

/**
 * NE から消す CSV に入れてよいか。何も書かない。p_batch = 同じ依頼の SKU (セットと中身の単品を一緒に消す) / p_app_stock = アプリが読んだ在庫の値 (単品だけ見る・下の形)。戻り値 = {
 *   sku_id (文字), code, ne_code (NE の元の書き方 | null), sku_kind, reg_state, ok, problems: [{ reason, text }] (空 = ok), warnings: [{ reason, text }],
 *   stock: { db: { ne_day, ne_rows, ne_qty, logizard_day, logizard_qty, logizard_allocated }, app: p_app_stock } (単品だけ), ne_codes_run }
 * p_app_stock = { ne_own, ne_allocated, ne_on_order, fba, logizard, logizard_allocated, po_backorder (どれも 0 以上の整数・1 つでも欠ける / 整数でない = 読めない), sources: {...} } か { unreadable: '読めない理由' } (null = 読めない)
 * reason: no_sku / not_company / not_available / kind_exception / live_request / ne_codes_old / not_in_ne / gone_from_ne / ne_code_bad / live_file / set_component /
 *         component_request / own_component_request / listing_component / listing_component_deleted / parent_of_others /
 *         stock_ne_unknown / stock_ne / stock_logizard_unknown / stock_logizard / stock_app_unknown / stock_app
 * warnings: set_first (同じ依頼 / 依頼の途中のセットの中身 = セットのファイルを先に取り込む) / page_info (同じ代表の子を全部消す = 代表のページ情報も消える)
 */
create function ops.ne_del_check(p_sku_id bigint, p_batch bigint[], p_app_stock jsonb) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  m        ops.master_ne_code_mark;
  c        ops.master_ne_codes;
  am       ops.master_ne_absence_mark;
  v_batch  bigint[] := coalesce(p_batch, '{}'::bigint[]);
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_today  date := (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date;
  v_p      jsonb := '[]'::jsonb;
  v_w      jsonb := '[]'::jsonb;
  v_txt    text;
  v_db     jsonb := null;
  v_ne_day date;
  v_ne_n   integer;
  v_ne_q   bigint;
  v_lz_day date;
  v_lz_q   bigint;
  v_lz_a   bigint;
  v_keys   jsonb := ops.ne_del_app_stock_keys();
  v_miss   text;
  v_nz     text;
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.product_id, p.parent_product_id, r.state as reg into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join ops.master_registrations r on r.sku_id = k.sku_id
   where k.sku_id = p_sku_id;
  if not found then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ne_code', null, 'ok', false, 'problems', ops._ne_del_problem('no_sku', 'Company DB にその SKU が無い'), 'warnings', '[]'::jsonb);
  end if;
  if s.company_id <> 1 then v_p := v_p || ops._ne_del_problem('not_company', '会社 1 の SKU でない'); end if;
  if s.reg is distinct from 'available' then
    v_p := v_p || ops._ne_del_problem('not_available', pg_catalog.format('登録の状態が %s (NE から消すのは「業務で使える」(available) の商品だけ。下書きは「この下書きを消す」)', coalesce(s.reg, '行が無い')));
  end if;
  if s.sku_kind is distinct from 'single' and s.sku_kind is distinct from 'set' then
    v_p := v_p || ops._ne_del_problem('kind_exception', '例外の SKU は NE から消す CSV に入れない (単品とセットだけ)');
  end if;
  select pg_catalog.string_agg('#' || i.export_id::text, '・' order by i.item_id) into v_txt from ops.ne_del_items i where i.sku_id = s.sku_id and i.state = 'live';
  if v_txt is not null then v_p := v_p || ops._ne_del_problem('live_request', pg_catalog.format('もう NE から消す依頼の途中 (ファイル %s)', v_txt)); end if;
  -- NE の元の書き方 (0041)・今も NE にあるか (0041 の台帳と 0070 の一番新しい完全な取得)
  select * into m from ops.master_ne_code_mark x where x.id = 1;
  select * into c from ops.master_ne_codes x where x.code_norm = s.code_norm and x.kind = 'product';
  if m.id is null or m.observed_at < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
    v_p := v_p || ops._ne_del_problem('ne_codes_old', pg_catalog.format('NE のコードの台帳 (照合の回 %s) が %s 時間より古い (今朝の照合を待つ)', coalesce(m.compare_run_id, 'なし'), v_hours));
  end if;
  select * into am from ops.master_ne_absence_mark x where x.id = 1;
  if am.id is not null and exists (select 1 from ops.master_ne_absence a where a.company_id = 1 and a.code_norm = s.code_norm and a.last_absent_result_id = am.result_id) then
    v_p := v_p || ops._ne_del_problem('gone_from_ne', '一番新しい完全な NE の取得にもう無い = 消す CSV は要らない (商品の画面の「NE から消えた (やめる)」へ)');
  elsif c.code_norm is null then
    v_p := v_p || ops._ne_del_problem('not_in_ne', pg_catalog.format('NE のコードの台帳 (照合の回 %s) に無い = NE に無い (消す CSV は要らない)', coalesce(m.compare_run_id, 'なし')));
  elsif c.state is distinct from 'ok' then
    v_p := v_p || ops._ne_del_problem('ne_code_bad', pg_catalog.format('NE のコードの書き方が 1 つに決まらない (%s) = NE の画面で確かめる', c.state));
  end if;
  -- 使われていないか (0070 の確かめと同じ・単品はセットの中身の決まりだけ違う)
  select pg_catalog.string_agg('#' || i.export_id::text || ' ' || i.state, '・' order by i.item_id) into v_txt
    from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if v_txt is not null then v_p := v_p || ops._ne_del_problem('live_file', pg_catalog.format('終わっていない NE 登録の CSV の品目がある (%s)', v_txt)); end if;
  -- (単品) それを中身に持つ生きたセット (やめていない) が無い。同じ依頼のセット・依頼の途中のセットは除く (= セットのファイルを先に NE に取り込む)
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components cc join core.skus p on p.sku_id = cc.parent_sku_id left join ops.master_registrations r on r.sku_id = cc.parent_sku_id
           where cc.child_sku_id = s.sku_id and r.state is distinct from 'cancelled' and not (cc.parent_sku_id = any (v_batch))
             and not exists (select 1 from ops.ne_del_items i where i.sku_id = cc.parent_sku_id and i.state = 'live')
           order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || ops._ne_del_problem('set_component', pg_catalog.format('それを中身に持つセット (%s) がある = 先にセットを消す (同じ依頼に入れるか、セットの依頼を先に)', v_txt));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components cc join core.skus p on p.sku_id = cc.parent_sku_id left join ops.master_registrations r on r.sku_id = cc.parent_sku_id
           where cc.child_sku_id = s.sku_id and r.state is distinct from 'cancelled'
             and ((cc.parent_sku_id = any (v_batch)) or exists (select 1 from ops.ne_del_items i where i.sku_id = cc.parent_sku_id and i.state = 'live'))
           order by p.code limit 6) x;
  if v_txt is not null then
    v_w := v_w || ops._ne_del_problem('set_first', pg_catalog.format('セット %s の中身 = NE にはセットのファイルを先に取り込む (セットが残っていると NE が断る)', v_txt));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select distinct p.code from ops.sku_component_requests q join core.skus p on p.sku_id = q.set_sku_id
           where q.status = 'open' and q.set_sku_id <> s.sku_id and exists (select 1 from pg_catalog.jsonb_array_elements(q.rows) e where (e ->> 'sku_id') = s.sku_id::text) order by p.code limit 6) x;
  if v_txt is not null then v_p := v_p || ops._ne_del_problem('component_request', pg_catalog.format('ほかのセットの開いている構成の依頼に入っている (セット %s)', v_txt)); end if;
  if s.sku_kind = 'set' and exists (select 1 from ops.sku_component_requests q where q.set_sku_id = s.sku_id and q.status = 'open') then
    v_p := v_p || ops._ne_del_problem('own_component_request', 'このセットの開いている構成の依頼がある (先に構成の依頼を閉じる)');
  end if;
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status <> 'deleted' order by 1 limit 6) x;
  if v_txt is not null then v_p := v_p || ops._ne_del_problem('listing_component', pg_catalog.format('販路の出品の構成に使われている (%s) = 先にモール・Amazon の対応を外す', v_txt)); end if;
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status = 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || ops._ne_del_problem('listing_component_deleted', pg_catalog.format('削除済みの出品 %s の構成に入っている (出品が戻ると消した商品が使われる = 先にその出品の構成から外す)', v_txt));
  end if;
  if s.product_id is not null and exists (select 1 from core.products cp where cp.parent_product_id = s.product_id and cp.product_id <> s.product_id) then
    v_p := v_p || ops._ne_del_problem('parent_of_others', 'この商品がほかの商品の代表 (親) になっている');
  end if;
  -- (単品) 同じ代表の子を全部消す = NE の代表のページ情報も消える (警告は出ない・止めはしない)
  if s.sku_kind = 'single' and s.parent_product_id is not null and s.parent_product_id <> s.product_id
     and not exists (select 1 from core.products cp join core.skus k on k.product_id = cp.product_id left join ops.master_registrations r on r.sku_id = k.sku_id
                      where cp.parent_product_id = s.parent_product_id and k.sku_id <> s.sku_id and r.state is distinct from 'cancelled'
                        and not (k.sku_id = any (v_batch)) and not exists (select 1 from ops.ne_del_items i where i.sku_id = k.sku_id and i.state = 'live')) then
    v_w := v_w || ops._ne_del_problem('page_info', '同じ代表 (親) の子を全部消す = NE の代表のページ情報も一緒に消える (NE は警告を出さない)');
  end if;
  -- (単品) 在庫 0 (決定 Q3 a)。① Company DB の在庫の日次 (DB が自分で読む) ② アプリが読んだ値 (DB は確かめられない = 記録に残す)。どれかを読めない = 断る
  if s.sku_kind = 'single' then
    select d.snapshot_date into v_ne_day from snapshots.stock_capture_days d
     where d.company_id = 1 and d.source = 'ne' and d.status = 'complete' order by d.snapshot_date desc limit 1;
    if v_ne_day is not null then
      select pg_catalog.count(*)::integer, coalesce(pg_catalog.sum(x.qty), 0) into v_ne_n, v_ne_q
        from snapshots.sku_stock_daily x where x.sku_id = s.sku_id and x.snapshot_date = v_ne_day and x.source = 'ne' and x.company_id = 1;
    end if;
    if v_ne_day is null or v_ne_day < v_today - 1 then
      v_p := v_p || ops._ne_del_problem('stock_ne_unknown', pg_catalog.format('Company DB の NE の在庫の日次が新しくない (一番新しい日 %s・今日か前日が要る)', coalesce(v_ne_day::text, 'なし')));
    elsif v_ne_n = 0 then
      v_p := v_p || ops._ne_del_problem('stock_ne_unknown', pg_catalog.format('Company DB の NE の在庫の日次 (%s) にこの商品の行が無い = 在庫を確かめられない', v_ne_day));
    elsif v_ne_q > 0 then
      v_p := v_p || ops._ne_del_problem('stock_ne', pg_catalog.format('NE の在庫が %s 個ある (%s の在庫の日次) = 在庫 0 になるまで待つ', v_ne_q, v_ne_day));
    end if;
    select d.snapshot_date into v_lz_day from snapshots.stock_capture_days d
     where d.company_id = 1 and d.source = 'logizard' and d.status = 'complete' order by d.snapshot_date desc limit 1;
    if v_lz_day is not null then
      select coalesce(pg_catalog.sum(x.qty), 0), coalesce(pg_catalog.sum(x.allocated_qty), 0) into v_lz_q, v_lz_a
        from snapshots.sku_stock_daily x where x.sku_id = s.sku_id and x.snapshot_date = v_lz_day and x.source = 'logizard' and x.company_id = 1;
    end if;
    if v_lz_day is null or v_lz_day < v_today - 2 then
      v_p := v_p || ops._ne_del_problem('stock_logizard_unknown', pg_catalog.format('Company DB のロジザードの在庫の日次が新しくない (一番新しい日 %s・2 日の中が要る)', coalesce(v_lz_day::text, 'なし')));
    elsif v_lz_q > 0 or v_lz_a > 0 then
      v_p := v_p || ops._ne_del_problem('stock_logizard', pg_catalog.format('ロジザードの在庫が %s 個 (引当 %s) ある (%s の在庫の日次) = 在庫 0 になるまで待つ', v_lz_q, v_lz_a, v_lz_day));
    end if;
    v_db := pg_catalog.jsonb_build_object('ne_day', v_ne_day, 'ne_rows', v_ne_n, 'ne_qty', v_ne_q, 'logizard_day', v_lz_day, 'logizard_qty', v_lz_q, 'logizard_allocated', v_lz_a);
    if p_app_stock is null or pg_catalog.jsonb_typeof(p_app_stock) is distinct from 'object' then
      v_p := v_p || ops._ne_del_problem('stock_app_unknown', 'アプリが在庫の値を読めていない (在庫を確かめられない)');
    elsif p_app_stock ? 'unreadable' then
      v_p := v_p || ops._ne_del_problem('stock_app_unknown', pg_catalog.format('在庫を確かめられない (%s)', pg_catalog.left(coalesce(p_app_stock ->> 'unreadable', '読めない'), 200)));
    else
      select pg_catalog.string_agg(e.value #>> '{}', '・' order by e.key) filter (where pg_catalog.jsonb_typeof(p_app_stock -> e.key) is distinct from 'number' or (p_app_stock ->> e.key) !~ '^[0-9]{1,10}$'),
             pg_catalog.string_agg((e.value #>> '{}') || ' ' || (p_app_stock ->> e.key), '・' order by e.key)
               filter (where pg_catalog.jsonb_typeof(p_app_stock -> e.key) = 'number' and (p_app_stock ->> e.key) ~ '^[0-9]{1,10}$' and (p_app_stock ->> e.key)::bigint > 0)
        into v_miss, v_nz from pg_catalog.jsonb_each(v_keys) e;
      if v_miss is not null then v_p := v_p || ops._ne_del_problem('stock_app_unknown', pg_catalog.format('在庫の値が足りない・形が違う (%s)', v_miss)); end if;
      if v_nz is not null then v_p := v_p || ops._ne_del_problem('stock_app', pg_catalog.format('在庫・引当・注文残がある (%s) = 0 になるまで待つ', v_nz)); end if;
    end if;
  end if;
  return pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ne_code', case when c.state = 'ok' then c.ne_code end, 'sku_kind', s.sku_kind, 'reg_state', s.reg,
    'ok', pg_catalog.jsonb_array_length(v_p) = 0, 'problems', v_p, 'warnings', v_w,
    'stock', case when s.sku_kind = 'single' then pg_catalog.jsonb_build_object('db', v_db, 'app', p_app_stock) end, 'ne_codes_run', m.compare_run_id);
end $$;
revoke all on function ops.ne_del_check(bigint, bigint[], jsonb) from public;

/** 何件かをまとめて確かめる (読むだけ・画面の「確かめる」)。同じ依頼 = p_sku_ids 全部。p_app_stock = { "<sku_id>": アプリの在庫 }。戻り値 = 確かめの答えの配列 (sku_id の順) */
create function ops.ne_del_check_many(p_sku_ids bigint[], p_app_stock jsonb) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_ids bigint[];
begin
  if p_sku_ids is null or pg_catalog.cardinality(p_sku_ids) not between 1 and 500 or pg_catalog.array_position(p_sku_ids, null) is not null then
    raise exception 'invalid_input: SKU は 1〜500 件' using errcode = '22023';
  end if;
  v_ids := array(select distinct x from pg_catalog.unnest(p_sku_ids) x order by 1);
  return (select coalesce(pg_catalog.jsonb_agg(ops.ne_del_check(x, v_ids, case when pg_catalog.jsonb_typeof(p_app_stock) = 'object' then p_app_stock -> x::text end) order by x), '[]'::jsonb)
            from pg_catalog.unnest(v_ids) x);
end $$;
revoke all on function ops.ne_del_check_many(bigint[], jsonb) from public;

-- ═══════════ 3. 消す CSV を作る ═══════════
/** 消す CSV のバイト列 (見出し「コード」= Shift_JIS 83 52 81 5B 83 68・CRLF・コードは英数字と _- だけ = ASCII と同じ・最後の行も CRLF) */
create function ops.ne_del_csv_bytes(p_codes text[]) returns bytea language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.decode('8352815b83680d0a', 'hex') || pg_catalog.convert_to(coalesce((select pg_catalog.string_agg(x || pg_catalog.chr(13) || pg_catalog.chr(10), '' order by o)
                                                                                          from pg_catalog.unnest(p_codes) with ordinality as t(x, o)), ''), 'UTF8')
$$;
revoke all on function ops.ne_del_csv_bytes(text[]) from public;
/** 取扱中止の CSV のバイト列 (apps/master-decisions/ne-csv.mjs の buildCsv(products:handling) と同じ = 見出し syohin_code,toriatukai_kbn・値 1 = 取扱中止・UTF-8・BOM なし・CRLF) */
create function ops.ne_handling_csv_bytes(p_codes text[]) returns bytea language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.convert_to('syohin_code,toriatukai_kbn' || pg_catalog.chr(13) || pg_catalog.chr(10)
    || coalesce((select pg_catalog.string_agg(x || ',1' || pg_catalog.chr(13) || pg_catalog.chr(10), '' order by o) from pg_catalog.unnest(p_codes) with ordinality as t(x, o)), ''), 'UTF8')
$$;
revoke all on function ops.ne_handling_csv_bytes(text[]) from public;

/**
 * NE から消す CSV を作る (1 つの取引・人が理由 (200 字まで・改行なし) を書いてだけ・1〜500 件)。同じ request_id の押し直し = 前の答え (replayed)。
 *   違う人・中身 (SKU・理由・在庫の値) で同じ request_id = request_id_reused
 *   鍵: 段階 (共有) → マスタの書き込み (共有・夜間ロードと並ぶ) → SKU (sku_id の順) → CSV (ops.ne_reg_lock_skus = NE 登録の CSV と並ぶ) → NE のコード (共有) →
 *       登録の状態の行 (排他・sku_id の順 = 参照を作る trigger の共有と並ぶ) → SKU の行 (排他)
 *   段階 new_open・持ち主表 = 段階の記録 (ops.reg_write_gate)。確かめ (ops.ne_del_check・同じ依頼 = 全部の SKU) が 1 件でも ok でなければ not_deletable (理由を全部)
 *   約束 (操作 ne_del_build・この関数だけが書く) → ファイル (セット → 単品の順・コードの順の行) → 品目 (live・根拠 = 確かめの答え) → 保存の記録 done。
 *   戻り値 = { ok, request_id, exports: [{ export_id, kind, row_count, sha256, codes }], warnings: [{ code, reason, text }] }
 * 呼べるロール = master_edit (画面・名簿 MASTER_DECISION_APPROVERS の人)。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)
 */
create function ops.ne_del_build(p_request_id uuid, p_actor_id text, p_reason text, p_sku_ids bigint[], p_app_stock jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_ids     bigint[];
  v_stock   jsonb := case when pg_catalog.jsonb_typeof(p_app_stock) = 'object' then p_app_stock else '{}'::jsonb end;
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  v_chk     jsonb;
  v_checks  jsonb := '[]'::jsonb;
  v_bad     text;
  v_warn    jsonb := '[]'::jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_today   date := (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date;
  v_kind    text;
  v_codes   text[];
  v_exp     bigint;
  v_bytes   bytea;
  v_exports jsonb := '[]'::jsonb;
  v_result  jsonb;
  x         bigint;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: NE から消すのは人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  if p_sku_ids is null or pg_catalog.cardinality(p_sku_ids) not between 1 and 500 or pg_catalog.array_position(p_sku_ids, null) is not null then
    raise exception 'invalid_input: SKU は 1〜500 件' using errcode = '22023';
  end if;
  v_ids := array(select distinct t.x from pg_catalog.unnest(p_sku_ids) t(x) order by 1);
  if pg_catalog.cardinality(v_ids) <> pg_catalog.cardinality(p_sku_ids) then raise exception 'invalid_input: 同じ SKU が 2 回ある' using errcode = '22023'; end if;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'ne_del_build', 'sku_ids', pg_catalog.to_jsonb(v_ids::text[]), 'reason', v_reason, 'app_stock', v_stock));
  perform ops.ne_reg_lock_skus(v_ids);   -- SKU (sku_id の順) → CSV (同じ request_id の 2 つ目の押し直しはここで前の取引の commit を待つ = 下で前の答えを見る)
  select r.operation, r.actor_id, r.status, r.result, r.payload_hash into v_prev from ops.master_edit_requests r where r.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'ne_del_build' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done' or v_prev.payload_hash is distinct from v_hash then
      raise exception 'request_id_reused: request_id % はほかの操作・人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));   -- NE のコードの台帳を commit まで照合に入れ替えさせない
  perform 1 from ops.master_registrations r where r.sku_id = any (v_ids) order by r.sku_id for update;
  perform 1 from core.skus k where k.sku_id = any (v_ids) order by k.sku_id for update;
  foreach x in array v_ids loop
    v_chk := ops.ne_del_check(x, v_ids, coalesce(v_stock -> x::text, '{"unreadable": "アプリが在庫の値を渡していない"}'::jsonb));
    v_checks := v_checks || pg_catalog.jsonb_build_array(v_chk);
    if (v_chk ->> 'ok') is distinct from 'true' then
      v_bad := coalesce(v_bad || ' / ', '') || coalesce(v_chk ->> 'code', x::text) || ' (' || (select pg_catalog.string_agg(p ->> 'text', '・') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') p) || ')';
    end if;
    v_warn := v_warn || coalesce((select pg_catalog.jsonb_agg(w || pg_catalog.jsonb_build_object('code', v_chk ->> 'code')) from pg_catalog.jsonb_array_elements(v_chk -> 'warnings') w), '[]'::jsonb);
  end loop;
  if v_bad is not null then raise exception 'not_deletable: NE から消す CSV に入れられない: %', pg_catalog.left(v_bad, 1500) using errcode = 'P0001'; end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select st.phase into v_phase from ops.master_cutover_state st where st.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'ne_del_build', null, '{}', '{}', pg_catalog.repeat('0', 64), v_hash,
            pg_catalog.jsonb_build_object('sku_ids', pg_catalog.to_jsonb(v_ids::text[])), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  perform pg_catalog.set_config('ops.ne_del_protocol', '1', true);
  -- セット → 単品の順 (NE にはセットのファイルを先に取り込む)。行 = NE の元のコードの順
  foreach v_kind in array array['sets', 'products'] loop
    select pg_catalog.array_agg(c ->> 'ne_code' order by c ->> 'ne_code') into v_codes from pg_catalog.jsonb_array_elements(v_checks) c
     where (c ->> 'sku_kind') = case v_kind when 'sets' then 'set' else 'single' end;
    continue when v_codes is null;
    v_bytes := ops.ne_del_csv_bytes(v_codes);
    insert into ops.ne_del_exports (company_id, kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
      values (1, v_kind, case v_kind when 'sets' then 'ne-del-sets-v1' else 'ne-del-products-v1' end, 'sjis', 'コード', v_bytes,
              pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), pg_catalog.cardinality(v_codes), p_request_id, v_reason, p_actor_id, v_today, v_db_user)
      returning export_id into v_exp;
    insert into ops.ne_del_items (export_id, company_id, sku_id, code_norm, ne_code, sku_kind, action, state, evidence, state_changed_by)
      select v_exp, 1, (c ->> 'sku_id')::bigint, pg_catalog.lower(c ->> 'ne_code'), c ->> 'ne_code', c ->> 'sku_kind', 'delete', 'live', c, p_actor_id
        from pg_catalog.jsonb_array_elements(v_checks) c where (c ->> 'sku_kind') = case v_kind when 'sets' then 'set' else 'single' end;
    v_exports := v_exports || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('export_id', v_exp::text, 'kind', v_kind, 'row_count', pg_catalog.cardinality(v_codes),
      'sha256', pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), 'codes', pg_catalog.to_jsonb(v_codes)));
  end loop;
  perform pg_catalog.set_config('ops.ne_del_protocol', '', true);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'exports', v_exports, 'warnings', v_warn, 'built_day', v_today);
  perform ops.close_reg_write(v_result, 'ne_del:' || (v_checks -> 0 ->> 'code'));
  return v_result;
end $$;
revoke all on function ops.ne_del_build(uuid, text, text, bigint[], jsonb) from public;

-- ═══════════ 4. ダウンロード ═══════════
/**
 * ファイルのバイト列を渡す (画面のダウンロード)。消すファイル (products / sets) は作った JST の日だけ (古いファイル = 在庫・依頼が変わったかもしれない = 作り直す)。
 *   取扱中止のファイル (handling) は後の日も同じバイト列 (sha256) のまま渡す (作った時に元の品目を handled にした = 当日に受け取れなくても戻らない・#1690 Codex R1 Medium 3)・
 * 消すファイルは中の品目が全部まだ依頼の途中 (live) のときだけ (取り下げた・やめた商品を NE で消さない)。初めてのダウンロード (人・時刻) と回数を残す。
 * 戻り値 = { export_id, kind, file_name, sha256, bytes_b64, row_count, first_downloaded_at }
 */
create function ops.ne_del_download(p_actor_id text, p_export_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e       ops.ne_del_exports;
  v_today date := (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date;
  v_txt   text;
begin
  if ops.reg_actor_problem(p_actor_id, null) is not null then raise exception 'invalid_input: 人 (ログインのメール) が要る' using errcode = '22023'; end if;
  select * into e from ops.ne_del_exports x where x.export_id = p_export_id for update;
  if e.export_id is null then raise exception 'not_found: ファイル % が無い', p_export_id using errcode = 'P0002'; end if;
  if e.kind in ('products', 'sets') and e.built_day <> v_today then
    raise exception 'file_old: ファイル #% は % に作った = 作った日だけダウンロードできる (在庫・依頼が変わったかもしれない。消す商品は「取り下げ」てから作り直す)', e.export_id, e.built_day using errcode = 'P0001';
  end if;
  if e.kind in ('products', 'sets') then
    perform 1 from ops.ne_del_items i where i.export_id = e.export_id order by i.item_id for share;
    select pg_catalog.string_agg(i.ne_code || ' ' || i.state, '・' order by i.ne_code) into v_txt from ops.ne_del_items i where i.export_id = e.export_id and i.state <> 'live';
    if v_txt is not null then
      raise exception 'file_changed: ファイル #% の中に、もう依頼の途中でない商品がある (%) = このファイルは使わない (作り直す)', e.export_id, v_txt using errcode = 'P0001';
    end if;
  end if;
  perform pg_catalog.set_config('ops.ne_del_protocol', '1', true);
  update ops.ne_del_exports set first_downloaded_at = coalesce(first_downloaded_at, pg_catalog.clock_timestamp()), first_downloaded_by = coalesce(first_downloaded_by, p_actor_id),
         download_count = download_count + 1
   where export_id = e.export_id returning * into e;
  perform pg_catalog.set_config('ops.ne_del_protocol', '', true);
  return pg_catalog.jsonb_build_object('export_id', e.export_id::text, 'kind', e.kind, 'sha256', e.sha256, 'row_count', e.row_count, 'bytes_b64', pg_catalog.encode(e.file_bytes, 'base64'),
    'file_name', pg_catalog.format('%s-%s-%s.csv', case e.kind when 'products' then 'ne-delete-products' when 'sets' then 'ne-delete-sets' else 'ne-handling-stop' end,
                                   pg_catalog.to_char(e.built_day, 'YYYYMMDD'), e.export_id),
    'first_downloaded_at', e.first_downloaded_at, 'download_count', e.download_count);
end $$;
revoke all on function ops.ne_del_download(text, bigint) from public;

-- ═══════════ 5. 取り下げ ═══════════
/**
 * 依頼の途中の品目 (live) を取り下げる (withdrawn・1 件・理由が要る)。NE で消すのをやめた・NE が断った商品を残す・間違えた ときに。
 *   🚨 取り下げても、ダウンロードしたファイルを NE に取り込めば NE からは消える (Company DB は NE から消えたのを照合で知る = 「NE から消えた (やめる)」)
 *   鍵: 段階 (共有) → マスタの書き込み (共有) → SKU → CSV → 品目の行 (排他)。約束 ne_del_withdraw → 品目 → 保存の記録。押し直し = 前の答え
 */
create function ops.ne_del_withdraw(p_request_id uuid, p_actor_id text, p_reason text, p_item_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  i         ops.ne_del_items;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_result  jsonb;
begin
  if p_request_id is null or p_item_id is null then raise exception 'invalid_input: request_id と品目が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: 取り下げは人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'ne_del_withdraw', 'item_id', p_item_id::text, 'reason', v_reason));
  select * into i from ops.ne_del_items x where x.item_id = p_item_id;
  if i.item_id is null then raise exception 'not_found: 品目 % が無い', p_item_id using errcode = 'P0002'; end if;
  perform ops.ne_reg_lock_skus(array[i.sku_id]);
  select r.operation, r.actor_id, r.status, r.result, r.payload_hash into v_prev from ops.master_edit_requests r where r.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'ne_del_withdraw' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done' or v_prev.payload_hash is distinct from v_hash then
      raise exception 'request_id_reused: request_id % はほかの操作・人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  select * into i from ops.ne_del_items x where x.item_id = p_item_id for update;
  if i.action <> 'delete' or i.state <> 'live' then
    raise exception 'not_live: 品目 % (%) は依頼の途中でない (%)', p_item_id, i.ne_code, i.state using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select st.phase into v_phase from ops.master_cutover_state st where st.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'ne_del_withdraw', i.sku_id, '{}', '{}', pg_catalog.repeat('0', 64), v_hash,
            pg_catalog.jsonb_build_object('item_id', p_item_id::text), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  perform pg_catalog.set_config('ops.ne_del_protocol', '1', true);
  update ops.ne_del_items set state = 'withdrawn', state_changed_at = pg_catalog.clock_timestamp(), state_changed_by = p_actor_id, state_reason = v_reason where item_id = p_item_id;
  perform pg_catalog.set_config('ops.ne_del_protocol', '', true);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'item_id', p_item_id::text, 'export_id', i.export_id::text, 'code', i.ne_code, 'state', 'withdrawn');
  perform ops.close_reg_write(v_result, i.ne_code);
  return v_result;
end $$;
revoke all on function ops.ne_del_withdraw(uuid, text, text, bigint) from public;

-- ═══════════ 6. 断られたとき = 代わりに取扱中止の CSV (決定 Q2 a) ═══════════
/**
 * 品目が「NE が消すのを断った (かもしれない)」か (読むだけ)。全部を満たすときだけ ok:
 *   消す品目で依頼の途中 (live)・単品 (取扱区分の CSV の列の表は単品だけ = セットは NE の画面で)・登録の状態 available・
 *   ファイルを初めてダウンロードした JST の日から ops.ne_gone_required_runs() (= 3) 日以上たった・数えた一番新しい完全な NE の取得 (0070) が
 *   ダウンロードより後で、**3 日の待機の期限 (ダウンロードした日 + 3 日の JST 0 時) 以後の取得**で (#1690 Codex R1 Medium 2)・36 時間の中・その取得にまだある・NE のコードの台帳 (0041) に書き方が 1 つ (印が 36 時間の中)
 * 戻り値 = { item_id, code, ne_code, ok, problems: [{ reason, text }], downloaded_day, latest_fetch }
 */
create function ops.ne_del_refused_check(p_item_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  i       ops.ne_del_items;
  e       ops.ne_del_exports;
  am      ops.master_ne_absence_mark;
  g       ops.master_parent_gate_results;
  m       ops.master_ne_code_mark;
  c       ops.master_ne_codes;
  v_reg   text;
  v_p     jsonb := '[]'::jsonb;
  v_hours integer := ops.ne_gone_fresh_hours();
  v_days  integer := ops.ne_gone_required_runs();
  v_today date := (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date;
  v_dday  date;
begin
  select * into i from ops.ne_del_items x where x.item_id = p_item_id;
  if i.item_id is null then
    return pg_catalog.jsonb_build_object('item_id', p_item_id::text, 'ok', false, 'problems', ops._ne_del_problem('no_item', '品目が無い'));
  end if;
  select * into e from ops.ne_del_exports x where x.export_id = i.export_id;
  select r.state into v_reg from ops.master_registrations r where r.sku_id = i.sku_id;
  if i.action <> 'delete' or i.state <> 'live' then v_p := v_p || ops._ne_del_problem('not_live', pg_catalog.format('依頼の途中でない (%s)', i.state)); end if;
  if i.sku_kind <> 'single' then v_p := v_p || ops._ne_del_problem('not_single', 'セットに取扱中止の CSV は無い (NE の画面で直すか、セットの依頼を取り下げる)'); end if;
  if v_reg is distinct from 'available' then v_p := v_p || ops._ne_del_problem('not_available', pg_catalog.format('登録の状態が %s', coalesce(v_reg, '行が無い'))); end if;
  v_dday := (e.first_downloaded_at at time zone 'Asia/Tokyo')::date;
  if e.first_downloaded_at is null then
    v_p := v_p || ops._ne_del_problem('not_downloaded', 'まだ消す CSV をダウンロードしていない');
  elsif v_dday > v_today - v_days then
    v_p := v_p || ops._ne_del_problem('too_soon', pg_catalog.format('ダウンロードした日 (%s) から %s 日たっていない', v_dday, v_days));
  end if;
  select * into am from ops.master_ne_absence_mark x where x.id = 1;
  select * into g from ops.master_parent_gate_results x where x.result_id = am.result_id;
  if g.result_id is null then
    v_p := v_p || ops._ne_del_problem('no_trusted_fetch', '完全で信用できる NE の取得の記録がまだ無い');
  else
    if least(g.products_complete_at, g.setproducts_complete_at) < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
      v_p := v_p || ops._ne_del_problem('fetch_old', pg_catalog.format('一番新しい完全な NE の取得が %s 時間より古い (今朝の照合を待つ)', v_hours));
    end if;
    if e.first_downloaded_at is not null and least(g.products_complete_at, g.setproducts_complete_at) <= e.first_downloaded_at then
      v_p := v_p || ops._ne_del_problem('fetch_before_download', 'ダウンロードの後の完全な NE の取得がまだ無い');
    end if;
    -- 3 日の待機の期限 (ダウンロードした日 + 3 日の JST 0 時) より前の取得で「3 日たっても NE にある」と言わない (日をまたいだだけで、期限の後の取得がまだ = 待つ)
    if e.first_downloaded_at is not null and (least(g.products_complete_at, g.setproducts_complete_at) at time zone 'Asia/Tokyo')::date < v_dday + v_days then
      v_p := v_p || ops._ne_del_problem('fetch_too_soon', pg_catalog.format('3 日の待機の後 (%s 以後) の完全な NE の取得がまだ (一番新しい取得 %s)', v_dday + v_days,
        pg_catalog.to_char(least(g.products_complete_at, g.setproducts_complete_at) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI')));
    end if;
    if exists (select 1 from ops.master_ne_absence a where a.company_id = 1 and a.code_norm = i.code_norm and a.last_absent_result_id = g.result_id) then
      v_p := v_p || ops._ne_del_problem('gone_from_ne', '一番新しい完全な NE の取得に無い = NE から消えた (「NE から消えた (やめる)」へ)');
    end if;
  end if;
  select * into m from ops.master_ne_code_mark x where x.id = 1;
  select * into c from ops.master_ne_codes x where x.code_norm = i.code_norm and x.kind = 'product';
  if m.id is null or m.observed_at < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
    v_p := v_p || ops._ne_del_problem('ne_codes_old', 'NE のコードの台帳が古い (今朝の照合を待つ)');
  elsif c.state is distinct from 'ok' or c.ne_code is distinct from i.ne_code then
    v_p := v_p || ops._ne_del_problem('ne_code_bad', 'NE のコードの台帳に、消す CSV と同じ書き方で 1 つだけ無い');
  end if;
  return pg_catalog.jsonb_build_object('item_id', i.item_id::text, 'export_id', i.export_id::text, 'code', i.ne_code, 'ne_code', i.ne_code, 'sku_id', i.sku_id::text,
    'ok', pg_catalog.jsonb_array_length(v_p) = 0, 'problems', v_p, 'downloaded_day', v_dday,
    'latest_fetch', case when g.result_id is null then null else pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id,
      'complete_at', least(g.products_complete_at, g.setproducts_complete_at)) end);
end $$;
revoke all on function ops.ne_del_refused_check(bigint) from public;

/**
 * 断られた品目の代わりに「取扱中止」の CSV を作る (1 つの取引・理由が要る・1〜500 件)。同じ request_id の押し直し = 前の答え。
 *   鍵: 段階 (共有) → マスタの書き込み (共有) → SKU (sku_id の順) → CSV → NE のコード (共有) → 品目の行 (排他・item_id の順)
 *   全部の品目が ops.ne_del_refused_check で ok のときだけ。ファイル (取扱中止・UTF-8) と品目 (取扱中止 = recorded・元の品目) を書き、元の消す品目は handled (依頼は終わり)
 *   戻り値 = { ok, request_id, export: { export_id, kind, row_count, sha256, codes }, items }
 */
create function ops.ne_handling_build(p_request_id uuid, p_actor_id text, p_reason text, p_item_ids bigint[]) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_ids     bigint[];
  v_skus    bigint[];
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  v_chk     jsonb;
  v_checks  jsonb := '[]'::jsonb;
  v_bad     text;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_today   date := (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date;
  v_codes   text[];
  v_bytes   bytea;
  v_exp     bigint;
  v_result  jsonb;
  x         bigint;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: 取扱中止の CSV は人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  if p_item_ids is null or pg_catalog.cardinality(p_item_ids) not between 1 and 500 or pg_catalog.array_position(p_item_ids, null) is not null then
    raise exception 'invalid_input: 品目は 1〜500 件' using errcode = '22023';
  end if;
  v_ids := array(select distinct t.x from pg_catalog.unnest(p_item_ids) t(x) order by 1);
  if pg_catalog.cardinality(v_ids) <> pg_catalog.cardinality(p_item_ids) then raise exception 'invalid_input: 同じ品目が 2 回ある' using errcode = '22023'; end if;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'ne_handling_build', 'item_ids', pg_catalog.to_jsonb(v_ids::text[]), 'reason', v_reason));
  v_skus := array(select distinct i.sku_id from ops.ne_del_items i where i.item_id = any (v_ids) order by 1);
  perform ops.ne_reg_lock_skus(v_skus);
  select r.operation, r.actor_id, r.status, r.result, r.payload_hash into v_prev from ops.master_edit_requests r where r.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'ne_handling_build' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done' or v_prev.payload_hash is distinct from v_hash then
      raise exception 'request_id_reused: request_id % はほかの操作・人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  perform 1 from ops.ne_del_items i where i.item_id = any (v_ids) order by i.item_id for update;
  foreach x in array v_ids loop
    v_chk := ops.ne_del_refused_check(x);
    v_checks := v_checks || pg_catalog.jsonb_build_array(v_chk);
    if (v_chk ->> 'ok') is distinct from 'true' then
      v_bad := coalesce(v_bad || ' / ', '') || coalesce(v_chk ->> 'code', '#' || x::text) || ' (' || (select pg_catalog.string_agg(p ->> 'text', '・') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') p) || ')';
    end if;
  end loop;
  if v_bad is not null then raise exception 'not_refused: 取扱中止の CSV にできない: %', pg_catalog.left(v_bad, 1500) using errcode = 'P0001'; end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select st.phase into v_phase from ops.master_cutover_state st where st.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'ne_handling_build', null, '{}', '{}', pg_catalog.repeat('0', 64), v_hash,
            pg_catalog.jsonb_build_object('item_ids', pg_catalog.to_jsonb(v_ids::text[])), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  perform pg_catalog.set_config('ops.ne_del_protocol', '1', true);
  select pg_catalog.array_agg(c ->> 'ne_code' order by c ->> 'ne_code') into v_codes from pg_catalog.jsonb_array_elements(v_checks) c;
  v_bytes := ops.ne_handling_csv_bytes(v_codes);
  insert into ops.ne_del_exports (company_id, kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
    values (1, 'handling', 'ne-handling-v1', 'utf8', 'syohin_code,toriatukai_kbn', v_bytes, pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), pg_catalog.cardinality(v_codes),
            p_request_id, v_reason, p_actor_id, v_today, v_db_user)
    returning export_id into v_exp;
  insert into ops.ne_del_items (export_id, company_id, sku_id, code_norm, ne_code, sku_kind, action, state, replaces_item_id, evidence, state_changed_by)
    select v_exp, 1, i.sku_id, i.code_norm, i.ne_code, 'single', 'handling', 'recorded', i.item_id, c, p_actor_id
      from pg_catalog.jsonb_array_elements(v_checks) c join ops.ne_del_items i on i.item_id = (c ->> 'item_id')::bigint;
  update ops.ne_del_items set state = 'handled', state_changed_at = pg_catalog.clock_timestamp(), state_changed_by = p_actor_id,
         state_reason = pg_catalog.left('取扱中止の CSV #' || v_exp::text || ' (' || v_reason || ')', 200)
   where item_id = any (v_ids);
  perform pg_catalog.set_config('ops.ne_del_protocol', '', true);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text,
    'export', pg_catalog.jsonb_build_object('export_id', v_exp::text, 'kind', 'handling', 'row_count', pg_catalog.cardinality(v_codes), 'sha256', pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'),
                                            'codes', pg_catalog.to_jsonb(v_codes)),
    'items', pg_catalog.to_jsonb(v_ids::text[]), 'built_day', v_today);
  perform ops.close_reg_write(v_result, 'ne_handling:' || v_codes[1]);
  return v_result;
end $$;
revoke all on function ops.ne_handling_build(uuid, text, text, bigint[]) from public;

-- ═══════════ 7. 画面の一覧 (読むだけ) ═══════════
/**
 * 依頼の途中の品目の今の様子 (読むだけ)。status:
 *   not_downloaded (まだダウンロードしていない) / gone_ready (NE から消えた = 「やめる」を押せる) / gone_counting (一番新しい完全な取得に無い・日数が足りない か ほかの止め) /
 *   refused (ダウンロードから 3 日以上たっても NE にある = 取扱中止の CSV を作れる) / waiting (NE に取り込むのを待つ・照合を待つ)。live でない = その状態のまま
 */
create function ops.ne_del_item_status(p_item_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  i      ops.ne_del_items;
  e      ops.ne_del_exports;
  am     ops.master_ne_absence_mark;
  a      ops.master_ne_absence;
  v_gone jsonb;
  v_ref  jsonb;
  v_st   text;
begin
  select * into i from ops.ne_del_items x where x.item_id = p_item_id;
  if i.item_id is null then return null; end if;
  select * into e from ops.ne_del_exports x where x.export_id = i.export_id;
  if i.state <> 'live' then return pg_catalog.jsonb_build_object('status', i.state); end if;
  select * into am from ops.master_ne_absence_mark x where x.id = 1;
  select * into a from ops.master_ne_absence x where x.company_id = 1 and x.code_norm = i.code_norm and x.last_absent_result_id = am.result_id;
  v_gone := ops.sku_gone_from_ne_check(i.sku_id);
  v_ref := ops.ne_del_refused_check(i.item_id);
  v_st := case when e.first_downloaded_at is null then 'not_downloaded'
               when (v_gone ->> 'ok') = 'true' then 'gone_ready'
               when a.code_norm is not null then 'gone_counting'
               when (v_ref ->> 'ok') = 'true' then 'refused'
               else 'waiting' end;
  return pg_catalog.jsonb_build_object('status', v_st, 'streak', coalesce(a.streak, 0), 'required', ops.ne_gone_required_runs(),
    'gone_problems', case when (v_gone ->> 'ok') = 'true' then '[]'::jsonb else v_gone -> 'problems' end,
    'refused_problems', v_ref -> 'problems', 'downloaded_day', v_ref -> 'downloaded_day');
end $$;
revoke all on function ops.ne_del_item_status(bigint) from public;

/**
 * 画面の一覧 (読むだけ・バイト列は出さない)。p_sku_id = その商品の品目だけ (商品の画面) / null = 全部。
 * 戻り値 = { today, items: [{ item_id, export_id, sku_id, code, ne_code, sku_kind, action, state, state_changed_at, state_changed_by, state_reason, replaces_item_id, created_at,
 *            status: ops.ne_del_item_status }], exports: [{ export_id, kind, row_count, sha256, built_by, built_at, built_day, reason, first_downloaded_at, first_downloaded_by, download_count }] }
 *   品目 = 依頼の途中の全部 + 30 日の中に作った・変わった品目 (新しい順・500 件まで)。ファイル = その品目のファイル
 */
create function ops.ne_del_overview(p_sku_id bigint) returns jsonb
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  with it as (
    select i.* from ops.ne_del_items i
     where (p_sku_id is null or i.sku_id = p_sku_id)
       and (i.state = 'live' or i.state_changed_at > pg_catalog.clock_timestamp() - interval '30 days' or i.created_at > pg_catalog.clock_timestamp() - interval '30 days')
     order by i.item_id desc limit 500)
  select pg_catalog.jsonb_build_object('today', (pg_catalog.clock_timestamp() at time zone 'Asia/Tokyo')::date,
    'items', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('item_id', it.item_id::text, 'export_id', it.export_id::text, 'sku_id', it.sku_id::text, 'code', k.code,
        'ne_code', it.ne_code, 'sku_kind', it.sku_kind, 'action', it.action, 'state', it.state, 'state_changed_at', it.state_changed_at, 'state_changed_by', it.state_changed_by,
        'state_reason', it.state_reason, 'replaces_item_id', it.replaces_item_id::text, 'created_at', it.created_at, 'reg_state', r.state,
        'status', ops.ne_del_item_status(it.item_id)) order by it.item_id desc)
      from it join core.skus k on k.sku_id = it.sku_id left join ops.master_registrations r on r.sku_id = it.sku_id), '[]'::jsonb),
    'exports', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('export_id', e.export_id::text, 'kind', e.kind, 'row_count', e.row_count, 'sha256', e.sha256,
        'built_by', e.built_by, 'built_at', e.built_at, 'built_day', e.built_day, 'reason', e.reason, 'first_downloaded_at', e.first_downloaded_at,
        'first_downloaded_by', e.first_downloaded_by, 'download_count', e.download_count) order by e.export_id desc)
      from ops.ne_del_exports e where e.export_id in (select it.export_id from it)), '[]'::jsonb))
$$;
revoke all on function ops.ne_del_overview(bigint) from public;

-- ═══════════ 8. 「NE から消えた → やめる」にセットの道 (0070 の版から写して 🆕 0072 の行だけ足した・決定 Q4 b) ═══════════
/**
 * SKU を「NE から消えた」でやめてよいか (0070 の契約のまま)。🆕 0072: セット (sku_kind = set) も ok にできる (例外の SKU は今までどおり not_single)・
 *   セット自身の開いている構成の依頼は断る (own_component_request = やめるときに構成を外す・依頼が残ると夜間の昇格で戻る)
 */
create or replace function ops.sku_gone_from_ne_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  a        ops.master_ne_absence;
  g        ops.master_parent_gate_results;
  f        ops.master_parent_gate_results;
  m        ops.master_ne_code_mark;
  am       ops.master_ne_absence_mark;
  v_req    integer := ops.ne_gone_required_runs();
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_p      jsonb := '[]'::jsonb;
  v_streak integer := 0;
  v_txt    text;
  v_ok     boolean;
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if not found then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ok', false,
      'problems', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_sku', 'text', 'Company DB にその SKU が無い')));
  end if;
  if s.company_id <> 1 then v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_company', 'text', '会社 1 の SKU でない')); end if;
  if s.reg is distinct from 'available' then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_available',
      'text', pg_catalog.format('登録の状態が %s (やめられるのは「業務で使える」(available) の商品だけ)', coalesce(s.reg, '行が無い'))));
  end if;
  if s.sku_kind is distinct from 'single' then
    if s.sku_kind is distinct from 'set' then   -- 🆕 0072: セットも「NE から消えた」でやめられる (決定 10/12 Q4 b・やめるときに構成を外す)
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_single', 'text', pg_catalog.format('%s はこの操作でやめない (単品だけ)', case s.sku_kind when 'set' then 'セット' else '例外の SKU' end)));
    end if;   -- 🆕 0072
  end if;
  -- NE の完全な取得 (照合 ② の記録の回) に続けて無いか。🆕 #1683 Codex R1 High 1: 「一番新しい」= 数えた一番新しい取得 (取得の時刻の順・ops.master_ne_absence_mark)・
  --   新しさ = 実際の取得の完了の時刻 (単品とセットの古い方)。記録の時刻 (created_at)・記録の順 (result_id) では決めない
  select * into am from ops.master_ne_absence_mark x where x.id = 1;
  select * into g from ops.master_parent_gate_results x where x.result_id = am.result_id;
  if g.result_id is null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_trusted_fetch', 'text', '完全で信用できる NE の取得の記録 (照合 ② の代表の数え) がまだ無い'));
  else
    if least(g.products_complete_at, g.setproducts_complete_at) < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'fetch_old',
        'text', pg_catalog.format('一番新しい完全な NE の取得 (照合の回 %s・取得の完了 %s) が %s 時間より古い (今朝の照合 ② を待つ)', g.compare_run_id,
          pg_catalog.to_char(least(g.products_complete_at, g.setproducts_complete_at) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI'), v_hours)));
    end if;
    select * into a from ops.master_ne_absence x where x.company_id = 1 and x.code_norm = s.code_norm;
    if a.code_norm is null or a.last_absent_result_id <> g.result_id then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_latest',
        'text', pg_catalog.format('一番新しい完全な NE の取得 (照合の回 %s) にある (か、無いと言えない) = NE から消えていない', g.compare_run_id)));
    else
      v_streak := a.streak;
      select * into f from ops.master_parent_gate_results x where x.result_id = a.first_absent_result_id;
      if a.streak < v_req then
        v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'absent_short',
          'text', pg_catalog.format('完全な NE の取得に続けて無いのが %s 日 (%s 日で、やめられる・1 日に何回取得しても 1 日)', a.streak, v_req)));
      end if;
    end if;
  end if;
  -- 今の NE のコードの台帳 (0041・最新の照合の回の入れ替え) にあれば断る (安全側の守りだけ)
  select * into m from ops.master_ne_code_mark x where x.id = 1;
  if exists (select 1 from ops.master_ne_codes c where c.code_norm = s.code_norm) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_codes',
      'text', pg_catalog.format('今の NE のコードの台帳 (照合の回 %s) にある', coalesce(m.compare_run_id, '不明'))));
  end if;
  -- 使われていないか
  select pg_catalog.string_agg('#' || i.export_id::text || ' ' || i.state, '・' order by i.item_id) into v_txt
    from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'live_file', 'text', pg_catalog.format('終わっていない NE 登録の CSV の品目がある (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where c.child_sku_id = s.sku_id order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'set_component', 'text', pg_catalog.format('ほかのセットの構成品になっている (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select distinct p.code from ops.sku_component_requests q join core.skus p on p.sku_id = q.set_sku_id
           where q.status = 'open' and exists (select 1 from pg_catalog.jsonb_array_elements(q.rows) e where (e ->> 'sku_id') = s.sku_id::text) order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'component_request', 'text', pg_catalog.format('開いている構成の依頼に入っている (セット %s)', v_txt)));
  end if;
  -- 🆕 0072 ここから: セット自身の開いている構成の依頼 = 先に閉じる (やめるときに構成を外す・依頼が残ると夜間の昇格で構成が戻る)
  if s.sku_kind = 'set' and exists (select 1 from ops.sku_component_requests q where q.set_sku_id = s.sku_id and q.status = 'open') then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'own_component_request', 'text', 'このセットの開いている構成の依頼がある (先に構成の依頼を閉じる)'));
  end if;
  -- 🆕 0072 ここまで
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status <> 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component', 'text', pg_catalog.format('販路の出品の構成に使われている (%s)', v_txt)));
  end if;
  -- 🆕 #1683 Codex R3 High: 削除済み (status = deleted) の出品に残る構成も断る = 後で出品が戻る (夜間ロードが active に戻す) と、やめた商品が使われてしまう。
  --   どの出品か分かるように出す (先に構成を外す = Amazon の対応の墓標・モールの対応を直す)
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status = 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component_deleted',
      'text', pg_catalog.format('削除済みの出品 %s の構成に入っている (出品が戻るとやめた商品が使われる = 先にその出品の構成から外す)', v_txt)));
  end if;
  if s.product_id is not null and exists (select 1 from core.products c where c.parent_product_id = s.product_id and c.product_id <> s.product_id) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'parent_of_others', 'text', 'この商品がほかの商品の代表 (親) になっている'));
  end if;
  v_ok := pg_catalog.jsonb_array_length(v_p) = 0;
  return pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ok', v_ok, 'problems', v_p, 'reg_state', s.reg, 'sku_kind', s.sku_kind,
    'required_runs', v_req, 'fresh_hours', v_hours, 'streak', v_streak,
    'latest', case when g.result_id is null then null else pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id,
        'products_complete_at', g.products_complete_at, 'setproducts_complete_at', g.setproducts_complete_at, 'fetch_day', am.fetch_day) end,
    'absent_since', case when f.result_id is null then null else pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id,
        'products_complete_at', f.products_complete_at, 'day', a.first_absent_day) end,
    'evidence', case when v_ok then pg_catalog.jsonb_build_object('rule', 'gone_from_ne_v2', 'required_days', v_req, 'streak_days', v_streak,
        'first_absent', pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id, 'products_complete_at', f.products_complete_at, 'day', a.first_absent_day),
        'last_absent', pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id, 'products_complete_at', g.products_complete_at,
          'setproducts_complete_at', g.setproducts_complete_at, 'day', a.last_absent_day, 'ne_generation_id', g.ne_generation_id),
        'ne_codes_run', m.compare_run_id) end);
end $$;
revoke all on function ops.sku_gone_from_ne_check(bigint) from public;

/**
 * 業務で使える商品を「NE から消えた」でやめる (0070 の契約のまま)。🆕 0072: セットは同じ取引で構成 (core.sku_components の親がこのセット) を外す
 *   (0026 の変更の記録に残る・中身の単品が「構成品」でなくなる)・NE から消す依頼の品目 (live) を gone に。戻り値に components_removed・ne_del_items を足す
 */
create or replace function ops.cancel_sku_gone_from_ne(p_request_id uuid, p_actor_id text, p_reason text, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_own     jsonb;
  v_hash    text;
  v_prev    record;
  v_code    text;
  v_chk     jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_tr      jsonb;
  v_result  jsonb;
  k         text;
  v_comps   jsonb := '[]'::jsonb;   -- 🆕 0072: 外したセットの構成
  v_items   jsonb := '[]'::jsonb;   -- 🆕 0072: 「消えた」にした NE から消す依頼の品目
  v_cfg     jsonb;                  -- 🆕 0072: 変更の記録の設定 (外した後に元に戻す)
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 then
    raise exception 'invalid_input: やめるのは人が理由 (200 字まで・改行なし) を書いてだけ' using errcode = '22023';
  end if;
  foreach k in array ops.new_entry_lease_kinds() loop perform pg_catalog.pg_advisory_xact_lock_shared(ops.new_entry_lease_lock_key(k)); end loop;
  v_own := ops.master_ownership_active_map();
  perform ops.reg_write_gate(v_own);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'sku_gone_cancel', 'sku_id', p_sku_id::text, 'reason', v_reason));
  select x.operation, x.actor_id, x.status, x.result, x.payload_hash, x.sku_id into v_prev from ops.master_edit_requests x where x.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'sku_gone_cancel' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done'
       or v_prev.payload_hash is distinct from v_hash or v_prev.sku_id is distinct from p_sku_id then
      raise exception 'request_id_reused: request_id % はほかの操作・人・商品・理由で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  -- 🆕 #1683 Codex R1 High 2: 登録の状態の行を先に (SKU の行より前に) 排他で取る = 参照を作る道 (下の 6. の trigger) はこの行を共有で取ってから cancelled を断る
  --   = 確かめの後に構成・依頼・出品の構成・代表が増えない (先に参照を作った取引が commit するまで、ここで待つ → 確かめが見る)
  perform 1 from ops.master_registrations x where x.sku_id = p_sku_id for update;
  select x.code into v_code from core.skus x where x.sku_id = p_sku_id and x.company_id = 1 for update;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  v_chk := ops.sku_gone_from_ne_check(p_sku_id);
  if (v_chk ->> 'ok') is distinct from 'true' then
    raise exception 'not_gone_from_ne: % はやめられない (%)', v_code, (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x)
      using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'sku_gone_cancel', p_sku_id, '{}', '{}', pg_catalog.repeat('0', 64),
            v_hash, pg_catalog.jsonb_build_object('sku_id', p_sku_id::text), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(v_own), v_own);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  v_tr := ops.transition_sku_registration(p_sku_id, 'cancelled', 'human', p_actor_id, v_reason, '{}'::jsonb, p_request_id::text);
  -- 🆕 0072 ここから: セット = 構成 (親がこのセット) を外す (中身の単品が「構成品」でなくなる・0026 の変更の記録に残る)・NE から消す依頼の品目 (live) = 消えた (gone)
  if (v_chk ->> 'sku_kind') = 'set' then
    -- 変更の記録 (0026) の 誰が・request_id・理由: 画面のロールは約束の行から (audit が読む)・CLI (master_ops) などはこの設定から。外した後に元に戻す (呼び手の取引に漏らさない)
    select pg_catalog.jsonb_object_agg(x.k, coalesce(pg_catalog.current_setting(x.k, true), '')) into v_cfg
      from pg_catalog.unnest(array['core.actor_type', 'core.actor_id', 'core.source_system', 'core.request_id', 'core.reason', 'core.run_id']) as x(k);
    perform pg_catalog.set_config('core.actor_type', 'human', true), pg_catalog.set_config('core.actor_id', p_actor_id, true), pg_catalog.set_config('core.source_system', 'sku_gone_cancel', true),
            pg_catalog.set_config('core.request_id', p_request_id::text, true), pg_catalog.set_config('core.reason', v_reason, true), pg_catalog.set_config('core.run_id', '', true);
    with d as (delete from core.sku_components cc where cc.parent_sku_id = p_sku_id returning cc.child_sku_id, cc.qty, cc.source)
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', ck.code, 'qty', d.qty, 'source', d.source) order by ck.code_norm), '[]'::jsonb) into v_comps
      from d join core.skus ck on ck.sku_id = d.child_sku_id;
    perform pg_catalog.set_config(e.key, e.value #>> '{}', true) from pg_catalog.jsonb_each(v_cfg) e;
  end if;
  perform pg_catalog.set_config('ops.ne_del_protocol', '1', true);
  with u as (
    update ops.ne_del_items set state = 'gone', state_changed_at = pg_catalog.clock_timestamp(), state_changed_by = p_actor_id, state_reason = pg_catalog.left(v_reason, 200)
     where sku_id = p_sku_id and action = 'delete' and state = 'live' returning item_id)
  select coalesce(pg_catalog.jsonb_agg(u.item_id::text order by u.item_id), '[]'::jsonb) into v_items from u;
  perform pg_catalog.set_config('ops.ne_del_protocol', '', true);
  -- 🆕 0072 ここまで
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', v_code, 'sku_id', p_sku_id::text, 'from', 'available', 'state', 'cancelled',
    'event_id', v_tr ->> 'event_id', 'evidence', v_chk -> 'evidence');
  v_result := v_result || pg_catalog.jsonb_build_object('components_removed', v_comps, 'ne_del_items', v_items);   -- 🆕 0072
  perform ops.close_reg_write(v_result, v_code);
  return v_result;
end $$;
revoke all on function ops.cancel_sku_gone_from_ne(uuid, text, text, bigint) from public;

-- ═══════════ 9. 依頼の途中の SKU・やめたセットへの参照を新しく作らない (0070 の版から写して 🆕 0072 の行だけ足した) ═══════════
/**
 * 依頼の途中 (NE から消す CSV の品目が live) の SKU のコード (無ければ null)。参照する SKU の登録の状態の行を共有で取ってから読む
 * (ops.ne_del_build は同じ行を排他で取ってから確かめて品目を書く = どちらが先でも「依頼の後に参照が増える」が起きない)
 */
create function ops.sku_ref_ne_deleting(p_sku_ids bigint[]) returns text
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_bad text;
begin
  if p_sku_ids is null or pg_catalog.cardinality(p_sku_ids) = 0 then return null; end if;
  perform 1 from ops.master_registrations r where r.sku_id = any (p_sku_ids) order by r.sku_id for share;
  select pg_catalog.string_agg(k.code, '・' order by k.code) into v_bad
    from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where i.sku_id = any (p_sku_ids) and i.state = 'live';
  return v_bad;
end $$;
revoke all on function ops.sku_ref_ne_deleting(bigint[]) from public;

/**
 * やめた SKU への参照を新しく作らない (0070 の契約のまま)。🆕 0072: NE から消す依頼の途中の SKU も (sku_ne_deleting)・構成の親 (セット) がやめた なら構成を作らない・
 *   構成の依頼のセット自身も見る
 */
create or replace function ops.guard_sku_ref_not_cancelled() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_ids bigint[];
  v_bad text;
begin
  if tg_table_name = 'sku_components' then
    -- 🆕 0072 ここから: 構成の親 (セット) がやめた (cancelled) = 構成を作らない (やめたセットの構成は外した = 夜間ロードの古い NE の行で戻さない)
    if (tg_op = 'INSERT' or new.parent_sku_id is distinct from old.parent_sku_id) then
      v_bad := ops.sku_ref_cancelled(array[new.parent_sku_id]);
      if v_bad is not null then
        raise exception 'sku_cancelled: やめた (cancelled) セット % に構成は作れない (%)', v_bad, tg_table_schema || '.' || tg_table_name using errcode = '23514';
      end if;
    end if;
    -- 🆕 0072 ここまで
    if tg_op = 'UPDATE' and new.child_sku_id is not distinct from old.child_sku_id then return new; end if;
    v_ids := array[new.child_sku_id];
  elsif tg_table_name = 'listing_components' then
    if tg_op = 'UPDATE' and new.sku_id is not distinct from old.sku_id then return new; end if;
    v_ids := array[new.sku_id];
  elsif tg_table_name = 'sku_component_requests' then
    select pg_catalog.array_agg(distinct (e ->> 'sku_id')::bigint) into v_ids
      from pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(new.rows) = 'array' then new.rows else '[]'::jsonb end) e
     where (e ->> 'sku_id') ~ '^[0-9]{1,18}$';
    v_ids := coalesce(v_ids, '{}'::bigint[]) || new.set_sku_id;   -- 🆕 0072: 依頼のセット自身も (やめた・NE から消す依頼の途中のセットに新しい構成の依頼を作らない)
  elsif tg_table_name = 'products' then
    if new.parent_product_id is null or new.parent_product_id = new.product_id
       or (tg_op = 'UPDATE' and new.parent_product_id is not distinct from old.parent_product_id) then return new; end if;
    select pg_catalog.array_agg(k.sku_id) into v_ids from core.skus k where k.product_id = new.parent_product_id;
  else
    raise exception 'guard_sku_ref_not_cancelled: 知らない表 %', tg_table_name;
  end if;
  v_bad := ops.sku_ref_cancelled(v_ids);
  -- 🆕 0072 ここから: NE から消す依頼の途中 (ops.ne_del_items が live) の SKU も新しい参照に入れない (依頼を取り下げるか、やめてから)
  if v_bad is null then
    v_bad := ops.sku_ref_ne_deleting(v_ids);
    if v_bad is not null then
      raise exception 'sku_ne_deleting: NE から消す依頼の途中の商品 % は、構成・出品の構成・構成の依頼・代表 (親) に使えない (%)', v_bad, tg_table_schema || '.' || tg_table_name
        using errcode = '23514';
    end if;
  end if;
  -- 🆕 0072 ここまで
  if v_bad is not null then
    raise exception 'sku_cancelled: やめた (cancelled) 商品 % は、構成・出品の構成・構成の依頼・代表 (親) に使えない (%)', v_bad, tg_table_schema || '.' || tg_table_name
      using errcode = '23514';
  end if;
  return new;
end $$;
revoke all on function ops.guard_sku_ref_not_cancelled() from public;
-- 構成の親 (セット) を変える更新も見る (やめたセットへの構成を作らない) = trigger の列を足して作り直す
drop trigger trg_sku_components_not_cancelled on core.sku_components;
create trigger trg_sku_components_not_cancelled before insert or update of child_sku_id, parent_sku_id on core.sku_components
  for each row execute function ops.guard_sku_ref_not_cancelled();

-- ─── バイト列を直接読めるロールを無くす (#1690 Codex R1 Medium 1・0058 の ops.restrict_ne_reg_file_bytes と同じ考え) ───
-- 表の SELECT を持つ owner でない全部のロール (と PUBLIC) から表の SELECT を外し、file_bytes 以外の列だけを列ごとに渡す。列ごとの file_bytes の SELECT も外す。
--   DB の持ち主だけが呼ぶ (この migration の最後と、2 つのロールの script が「全部の表に GRANT」の後に毎回呼ぶ = 流し直しても戻らない)。バイト列は ops.ne_del_download だけ
create function ops.restrict_ne_del_file_bytes() returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_cols text;
  r      record;
  v_done text[] := '{}';
begin
  select pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' order by a.attnum) into v_cols
    from pg_catalog.pg_attribute a where a.attrelid = 'ops.ne_del_exports'::regclass and a.attnum > 0 and not a.attisdropped and a.attname <> 'file_bytes';
  for r in select distinct x.grantee from pg_catalog.pg_class c cross join lateral pg_catalog.aclexplode(c.relacl) x
            where c.oid = 'ops.ne_del_exports'::regclass and x.privilege_type = 'SELECT' and x.grantee <> c.relowner loop
    if r.grantee = 0 then
      execute 'revoke select on ops.ne_del_exports from public';
      v_done := v_done || 'PUBLIC'::text;
    else
      execute pg_catalog.format('revoke select on ops.ne_del_exports from %I', r.grantee::regrole::text);
      execute pg_catalog.format('grant select (%s) on ops.ne_del_exports to %I', v_cols, r.grantee::regrole::text);
      v_done := v_done || r.grantee::regrole::text;
    end if;
  end loop;
  for r in select distinct x.grantee from pg_catalog.pg_attribute a cross join lateral pg_catalog.aclexplode(a.attacl) x
            where a.attrelid = 'ops.ne_del_exports'::regclass and a.attname = 'file_bytes' and x.privilege_type = 'SELECT'
              and x.grantee <> (select c.relowner from pg_catalog.pg_class c where c.oid = 'ops.ne_del_exports'::regclass) loop
    if r.grantee = 0 then execute 'revoke select (file_bytes) on ops.ne_del_exports from public';
    else execute pg_catalog.format('revoke select (file_bytes) on ops.ne_del_exports from %I', r.grantee::regrole::text); end if;
  end loop;
  return pg_catalog.jsonb_build_object('restricted', pg_catalog.to_jsonb(v_done));
end $$;
revoke all on function ops.restrict_ne_del_file_bytes() from public;

-- ─── 権限 (0070 / 0071 と同じ形)。master_edit には scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表は読ませない・関数の実行だけ) ───
--   watcher = 表を読むだけ (ops の全部の表の select = create-watch-roles.mjs の既定の権限・ops.ne_del_exports の file_bytes は除く)・確かめと一覧 (読むだけ) の実行。ファイルのバイト列は ops.ne_del_download だけ (master_edit)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function ops.ne_del_check(bigint, bigint[], jsonb) to watcher';
    execute 'grant execute on function ops.ne_del_check_many(bigint[], jsonb) to watcher';
    execute 'grant execute on function ops.ne_del_refused_check(bigint) to watcher';
    execute 'grant execute on function ops.ne_del_item_status(bigint) to watcher';
    execute 'grant execute on function ops.ne_del_overview(bigint) to watcher';
  end if;
end $$;
-- 既定の権限 (ops の新しい表の select = watcher) で付いたバイト列の SELECT を外す
select ops.restrict_ne_del_file_bytes();
