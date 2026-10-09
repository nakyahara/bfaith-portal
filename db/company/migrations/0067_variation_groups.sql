-- 0067: 色違い・サイズ違いのまとまり (group product) の DB (2026-10-09。AI_reference CompanyDB構想/20「代表の正本を自社DBへ」v7 §③・§⑤・§⑩ の PR-5)
-- 🚨 前提 = 0064 (大文字のコード)・0065 (CSV の版・3 者一致・一度でも配った商品の代表を変えない)・0066 (知らせの名前空間・ops.enqueue_group_snapshot)。どれも本番に適用済み
--
-- なぜ: 代表 (products.parent) を Company DB の持ち物にする (中原さん 10/8 夜 = 決定 b)。新商品の登録を NE の「選択肢つき商品の登録」と同じ形にする:
--   代表コード (まとまりのコード = 楽天の商品管理番号を人が打つ) + 横軸 / 縦軸の名前 + 選択肢 (名前・番号 ^-[A-Za-z0-9]{1,10}$) → 子 = 代表 + 横の番号 + 縦の番号。
--   代表は登録の時に 1 回だけ・NE に配った後は変えない。軸・選択肢名・まとまりの名前は後から直せる (社内だけ)。
-- 🚨 今は何も変わらない: まとまりの関数は全部、DB の持ち主の active が products.parent = company のときだけ動く (今は load = 断る = parent_not_company)。
--    親なしのふつうの登録 (ops.register_new_sku)・夜間ロード・照合は今までどおり (親か子のどちらか一方の守りも company のときだけ見る)
-- なにを:
--   1. 予約の表 ops.variation_group_codes (会社 + norm で一意・SKU の登録と同じ鍵 core.new_code:<norm>)。今ある札 (SKU の無い product) と単品の代表 (子を持つ単品) を
--      事前検査の後に入れる (重なりが 1 つでもあれば migration を止める = ops.reserve_existing_variation_groups)。その後に夜間ロードが作った札は、まとまりの関数が
--      最初に触るときに予約する (load)・widen の前に同じ関数を流し直せる (DB の持ち主だけ)
--   2. 軸 core.variation_axes (まとまり × 軸 1 = 横 / 2 = 縦)・選択肢 core.variation_options (選択肢番号は軸ごとに小文字で一意・選択肢名は軸ごとに
--      前後の空白を除いて NFKC でそろえた形で一意)・子の選択肢 core.sku_variation_choices ((まとまり, 横, 縦) で一意)。変更の記録 = 0026 の trigger
--   3. revision = まとまりごとの整数 (ops.variation_group_revisions)。まとまり・子・軸・選択肢の有効な状態か名前が変わる取引で「ちょうど 1」上がる
--      (同じ取引で 2 回目は断る = revision_twice・失敗した取引は巻き戻る)。上がった取引で、その revision の完全なスナップショット (ph-group-v1) を知らせに書く
--   4. 親か子のどちらか一方 (core.check_parent_one_level・遅らせた constraint trigger): 親を持つ商品は子を持てない・子を持つ商品は親を持てない。
--      products.parent の active が company のときだけ見る (load の間の夜間ロードを止めない)
--   5. 関数 (security definer・search_path = pg_catalog, pg_temp・一時の表を使わない・約束 (0051 の ops.master_write_sessions) の操作を足した):
--        ops.variation_batch_open   = まとまりを作る (札 + 予約) か 今あるまとまりを選ぶ・軸と選択肢と子のコードを全部確かめる・子のコードの鍵 (約束 variation_batch_open)
--        (アプリが同じ取引で子ごとに ops.register_new_sku (子の request_id = ops.variation_sub_request_id(まとめての request_id, 'child:' || norm))・JAN は ops.edit_sku_jan)
--        ops.variation_batch_close  = 子に親を付ける (下書き・この取引で作った・一度も配っていない子だけ)・軸・選択肢・子の選択肢・revision・スナップショット (約束 variation_batch_close)
--        ops.cancel_variation_child = 子の廃止 (下書き / NE 登録待ちで、生きているファイルが無く、NE に一度も現れていない子だけ・約束 variation_child_cancel)
--        ops.edit_variation_labels  = まとまりの名前 (札だけ)・軸の名前・選択肢名を直す (社内だけ・番号は直せない・約束 variation_label_edit)
--        ops.adopt_ne_parent_for_quarantined = NE で直接作られた商品 (quarantined) の代表を 1 回だけ採用 (最新の封のある照合の回の NE の観測から・約束 parent_adopt_ne)
--      開いて閉じなかったまとめての登録は commit で断る (ops.check_variation_batch_closed)
--   6. NE 登録の CSV のまとまりの版 ne-reg-variation-v1 を開く (作れる版): ops.ne_reg_schema_rule (0065 の置き換え)・
--      ops.ne_reg_canonical (0065 の置き換え = 代表の列だけ: NE に無いまとまりでも、ポータルで作った札 (予約 source = portal) なら予約のコード (打ったとおり))・
--      ops.ne_reg_build (0065 の置き換え = まとまりの版は全部の品目が同じまとまりの子・NE 登録待ちの子を全部入れる (まとまりで 1 ファイル)・
--      単品の版にポータルで作ったまとまりの子は入れない)。lib/master-reg-csv.mjs の regMaterialOf も同じ決まり (試験で照らす)
--   7. 知らせの保険 ops.guard_product_hub_outbox_session (0066 の置き換え): まとまりの知らせは、まとまりの約束の中で・約束のまとまり・request_id・人だけ
--   8. 照合 ② の確かめ待ちの商品 ops.v_ne_reg_targets に、products.parent が company のとき「親の無い quarantined の単品 (まだ採用していない)」を足す
--      = 翌朝の照合が NE の完全な取得の観測を残す → 代表の採用の根拠 (miniPC の照合のコードは変えない = 観測の作りはコードを問わない)
-- 🚨 この migration が書くのは予約の表だけ (今ある札・単品の代表)。商品・SKU・親子・登録の状態・ファイルの値は変えない
-- 🚨 ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しが要る: master_edit に 5 つの関数と確かめの 4 つの実行・まとまりの表の読み取り
--    (流さなくても今の動きは変わらない = 関数は products.parent が company のときだけ動く)

-- ═══════════ 1. 約束の操作 (0054 の CHECK と ops.master_write_allowed に足す・前の行は全部そのまま) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne'));

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
$$;

-- ═══════════ 2. 小さい部品 ═══════════
-- products.parent の持ち主が company か (DB の active。行が無い = 全部 load)。まとまりの関数・親か子のどちらか一方の守り・照合の確かめ待ちの quarantined が見る
create function ops.variation_parent_company() returns boolean language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce((select s.active_map ->> 'products.parent' from ops.master_ownership_state s where s.id = 1), 'load') = 'company'
$$;
revoke all on function ops.variation_parent_company() from public;

-- 1 回のまとめての登録の子の数の上限 (設計 v7 §④・R2 Medium 7: 本物の PG で時間の上限つきで測るまで小さく = 20)
create function ops.variation_max_children() returns integer language sql immutable set search_path = pg_catalog, pg_temp as $$ select 20 $$;
revoke all on function ops.variation_max_children() from public;

-- まとめての request_id から子・閉じるの request_id を決める (lib/master-write.mjs の janRequestId と同じ作り方: sha256(request_id || ':' || 印) の 16 進の先頭 32 字を uuid の形に)。
--   印 = 'child:' || 子のコードの norm / 'close'
create function ops.variation_sub_request_id(p_request_id uuid, p_tag text) returns uuid language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_request_id::text || ':' || p_tag, 'UTF8')), 'hex'), 1, 32)::uuid
$$;
revoke all on function ops.variation_sub_request_id(uuid, text) from public;

-- 軸の名前・選択肢名・まとまりの名前の形 (問題が無ければ null)。前後の空白なし・制御文字なし・1〜p_max 字
create function ops.variation_label_problem(p text, p_max integer) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when p is null or pg_catalog.length(p) not between 1 and p_max or p is distinct from pg_catalog.btrim(p) or p ~ '[[:cntrl:]]'
                   or pg_catalog.length(pg_catalog.btrim(pg_catalog.normalize(p, 'NFKC'))) = 0 then 'label' end
$$;
revoke all on function ops.variation_label_problem(text, integer) from public;

-- まとまりの鍵 (0066 の知らせの trigger と同じ鍵 = 同じまとまりの書き手は順番に)・コードの鍵 (SKU の登録と同じ鍵 core.new_code:<norm>)
create function ops.variation_lock_group(p_gid bigint) returns void language sql set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.variation_group:' || p_gid::text, 0))
$$;
revoke all on function ops.variation_lock_group(bigint) from public;
create function ops.variation_lock_code(p_norm text) returns void language sql set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.new_code:' || coalesce(p_norm, ''), 0))
$$;
revoke all on function ops.variation_lock_code(text) from public;

-- ═══════════ 3. 表 ═══════════
-- 3a. まとまりのコードの予約 (会社 + norm で一意)。source = portal (ポータルで作った札) / load (前からある札・単品の代表 = NE の代表から) / ne_adopt (quarantined の採用で作った札)。
--     状態の列は持たない (v5)。追記だけ
create table ops.variation_group_codes (
  company_id        smallint not null default 1 references core.companies,
  code_norm         text not null check (pg_catalog.length(code_norm) between 1 and 255),
  code              text not null check (pg_catalog.length(code) between 1 and 255 and code !~ '[[:cntrl:]]'),
  group_product_id  bigint not null,
  source            text not null check (source in ('portal', 'load', 'ne_adopt')),
  reserved_by       text not null check (pg_catalog.length(reserved_by) between 1 and 320),
  reserved_at       timestamptz not null default pg_catalog.now(),
  primary key (company_id, code_norm),
  constraint ux_vgc_group unique (group_product_id),
  constraint ck_vgc_norm check (code_norm = core.norm_code(code)),
  constraint ck_vgc_portal check (source <> 'portal' or (code ~ '^[A-Za-z0-9_-]{1,30}$' and code !~* '^set-')),
  foreign key (company_id, group_product_id) references core.products (company_id, product_id)
);
select core.make_append_only('ops', 'variation_group_codes');
comment on table ops.variation_group_codes is 'まとまりのコードの予約 (0067)。会社 + norm で一意・SKU の登録と同じ鍵 core.new_code:<norm>。書くのはまとまりの関数だけ。追記だけ';

-- 3b. 軸 (まとまり × 軸 1 = 横 / 2 = 縦)。まとまり 1 つに 0〜2 行 (縦は横があるときだけ = 関数が決める)
create table core.variation_axes (
  company_id        smallint not null default 1 references core.companies,
  group_product_id  bigint not null,
  axis              smallint not null check (axis in (1, 2)),
  name              text not null check (ops.variation_label_problem(name, 100) is null),
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  primary key (group_product_id, axis),
  foreign key (company_id, group_product_id) references core.products (company_id, product_id)
);
comment on table core.variation_axes is 'まとまりの軸 (0067)。1 = 横・2 = 縦。名前は後から直せる (社内だけ)。書くのはまとまりの関数だけ';

-- 3c. 選択肢 (まとまり × 軸 × 選択肢)。番号は軸ごとに小文字で一意・名前は軸ごとに NFKC でそろえた形で一意 (product-hub の selector value の一意と同じ)
create table core.variation_options (
  option_id         bigint generated always as identity primary key,
  company_id        smallint not null default 1 references core.companies,
  group_product_id  bigint not null,
  axis              smallint not null check (axis in (1, 2)),
  code              text not null check (code ~ '^-[A-Za-z0-9]{1,10}$'),
  code_norm         text not null generated always as (pg_catalog.lower(code)) stored,
  name              text not null check (ops.variation_label_problem(name, 100) is null),
  name_key          text not null generated always as (pg_catalog.btrim(pg_catalog.normalize(name, 'NFKC'))) stored,
  sort              integer not null check (sort >= 0),
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  foreign key (group_product_id, axis) references core.variation_axes (group_product_id, axis),
  constraint ux_vo_code unique (group_product_id, axis, code_norm),
  constraint ux_vo_name unique (group_product_id, axis, name_key),
  constraint ux_vo_key unique (option_id, group_product_id, axis)
);
comment on table core.variation_options is 'まとまりの選択肢 (0067)。番号 (-WH) は子のコードの一部 = 直せない・名前は直せる (社内だけ)。書くのはまとまりの関数だけ';

-- 3d. 子の選択肢 (子の SKU ごとに 1 行)。横は要る・縦は 2 軸のときだけ。(まとまり, 横, 縦) で一意
create table core.sku_variation_choices (
  sku_id            bigint primary key,
  company_id        smallint not null default 1 references core.companies,
  group_product_id  bigint not null,
  option1_id        bigint not null,
  axis1             smallint not null default 1 check (axis1 = 1),
  option2_id        bigint,
  axis2             smallint not null default 2 check (axis2 = 2),
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (option1_id, group_product_id, axis1) references core.variation_options (option_id, group_product_id, axis),
  foreign key (option2_id, group_product_id, axis2) references core.variation_options (option_id, group_product_id, axis)
);
create unique index ux_sku_variation_choices_combo on core.sku_variation_choices (group_product_id, option1_id, coalesce(option2_id, 0));
comment on table core.sku_variation_choices is '子の SKU の選択肢 (0067)。変えない (子の廃止は登録の状態で)。書くのはまとめての登録の関数だけ';

-- 3e. まとまりの revision と共通の欄 (知らせの common)。revision 0 = まだ知らせを出していない
create table ops.variation_group_revisions (
  group_product_id  bigint primary key,
  company_id        smallint not null default 1 references core.companies,
  revision          integer not null default 0 check (revision >= 0),
  revision_txid     bigint,
  common            jsonb not null default '{"shipping": null, "amazon_url": null, "asin": null, "official_url": null, "reference_urls": [], "yahoo": null}'::jsonb
                    check (pg_catalog.jsonb_typeof(common) = 'object'),
  updated_at        timestamptz not null default pg_catalog.now(),
  updated_by        text,
  foreign key (company_id, group_product_id) references core.products (company_id, product_id)
);
comment on table ops.variation_group_revisions is 'まとまりの revision (0067)。変わる取引でちょうど 1 上がる (同じ取引で 2 回目は断る)。上がった取引でスナップショットを知らせに書く';

-- 3f. まとめての登録 (1 回 = 1 行)。開いた取引の中で閉じないと commit できない (下の 4.)
create table ops.variation_batches (
  request_id        uuid primary key,
  company_id        smallint not null default 1 references core.companies,
  txid              bigint not null,
  group_product_id  bigint not null,
  group_created     boolean not null,
  spec              jsonb not null check (pg_catalog.jsonb_typeof(spec) = 'object'),
  spec_hash         text not null check (spec_hash ~ '^[0-9a-f]{64}$'),
  child_codes       text[] not null,
  actor_id          text not null check (pg_catalog.length(actor_id) between 1 and 320),
  status            text not null default 'open' check (status in ('open', 'closed')),
  result            jsonb check (result is null or pg_catalog.jsonb_typeof(result) = 'object'),
  created_at        timestamptz not null default pg_catalog.clock_timestamp(),
  closed_at         timestamptz,
  constraint ck_vb_closed check ((status = 'closed') = (closed_at is not null and result is not null)),
  foreign key (company_id, group_product_id) references core.products (company_id, product_id)
);
comment on table ops.variation_batches is 'まとめての登録 (0067)。ops.variation_batch_open が開き、同じ取引で ops.variation_batch_close が閉じる (閉じないと commit できない)';

-- 3g. quarantined の代表の採用 (1 SKU に 1 回だけ = 由来の記録)。追記だけ
create table ops.variation_parent_adoptions (
  sku_id            bigint primary key,
  company_id        smallint not null default 1 references core.companies,
  product_id        bigint not null,
  group_product_id  bigint not null,
  compare_run_id    text not null,
  ne_rep_code       text not null,
  group_created     boolean not null,
  adopted_by        text not null check (pg_catalog.length(adopted_by) between 1 and 320),
  request_id        uuid not null,
  adopted_at        timestamptz not null default pg_catalog.now(),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  foreign key (company_id, product_id) references core.products (company_id, product_id),
  foreign key (company_id, group_product_id) references core.products (company_id, product_id)
);
select core.make_append_only('ops', 'variation_parent_adoptions');
comment on table ops.variation_parent_adoptions is 'NE で直接作られた商品 (quarantined) の代表を NE から 1 回だけ採用した記録 (0067)。親の帰属 (parent_set_by) は manual・由来はここ';

-- 変更の記録 (0026 と同じ関数 = 0051 から security definer: 画面のロールは約束の行から誰が・request_id・理由)
alter table events.master_change_events drop constraint master_change_events_entity_type_check;
alter table events.master_change_events add constraint master_change_events_entity_type_check
  check (entity_type in ('product', 'sku', 'supplier', 'supplier_sku', 'sku_component', 'sku_cost', 'listing', 'listing_component', 'external_id', 'amazon_sku_map',
                         'variation_axis', 'variation_option', 'sku_variation_choice')) not valid;
create trigger trg_variation_axes_audit after insert or update or delete on core.variation_axes for each row execute function core.audit_master_change('variation_axis', 'group_product_id,axis');
create trigger trg_variation_options_audit after insert or update or delete on core.variation_options for each row execute function core.audit_master_change('variation_option', 'option_id');
create trigger trg_sku_variation_choices_audit after insert or update or delete on core.sku_variation_choices for each row execute function core.audit_master_change('sku_variation_choice', 'sku_id');

-- まとまりの表の守り: 書くのはまとまりの関数だけ (関数が取引の中だけ ops.variation_protocol = '1' を立てる・持ち主のロールの手の DML も止める)・消さない・
--   画面のロール (master_edit) は約束の操作で書いてよい表と書き方だけ・変えてよい列だけ (軸 / 選択肢 = 名前・revision = 1 つ上げる・共通の欄・まとめての登録 = 閉じる)
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない
create function ops.guard_variation_write() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_tbl     text := tg_table_schema || '.' || tg_table_name;
  v_sess    ops.master_write_sessions;
  v_old     jsonb;
  v_new     jsonb;
begin
  if tg_op = 'DELETE' then raise exception 'variation_write: % の行は消さない', v_tbl using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.variation_protocol', true), '') is distinct from '1' then
    raise exception 'variation_write: % はまとまりの関数 (ops.variation_batch_open ほか) の中だけで書く', v_tbl using errcode = 'P0001';
  end if;
  if v_db_user = 'master_edit' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or not ops.master_write_allowed(v_sess.operation, v_tbl, tg_op) then
      raise exception 'master_write_operation: 約束の操作 % では % に % できない', coalesce(v_sess.operation, '(約束なし)'), v_tbl, tg_op using errcode = '42501';
    end if;
  end if;
  if tg_op = 'UPDATE' then
    -- 🚨 表ごとに列が違う = 行は jsonb で読む (old / new の列名を直接書かない)
    v_old := pg_catalog.to_jsonb(old);
    v_new := pg_catalog.to_jsonb(new);
    if v_tbl = 'core.variation_axes' then
      if (v_new - 'name' - 'updated_at') is distinct from (v_old - 'name' - 'updated_at') then
        raise exception 'variation_write: 軸は名前だけ直せる' using errcode = 'P0001';
      end if;
    elsif v_tbl = 'core.variation_options' then
      -- 生成の列 (code_norm・name_key) は BEFORE の trigger ではまだ計算されていない = 比べない (code は比べる = 番号は変わらない)
      if (v_new - 'name' - 'name_key' - 'code_norm' - 'updated_at') is distinct from (v_old - 'name' - 'name_key' - 'code_norm' - 'updated_at') then
        raise exception 'variation_write: 選択肢は名前だけ直せる (番号は子のコードの一部)' using errcode = 'P0001';
      end if;
    elsif v_tbl = 'ops.variation_group_revisions' then
      if (v_new - 'revision' - 'revision_txid' - 'common' - 'updated_at' - 'updated_by') is distinct from (v_old - 'revision' - 'revision_txid' - 'common' - 'updated_at' - 'updated_by')
         or (v_new ->> 'revision')::integer not in ((v_old ->> 'revision')::integer, (v_old ->> 'revision')::integer + 1)
         or ((v_new ->> 'revision')::integer = (v_old ->> 'revision')::integer + 1 and (v_new ->> 'revision_txid')::bigint is distinct from pg_catalog.txid_current())
         or ((v_new ->> 'revision')::integer = (v_old ->> 'revision')::integer and (v_new -> 'revision_txid') is distinct from (v_old -> 'revision_txid')) then
        raise exception 'variation_write: revision は 1 つずつ上げるだけ (上げた取引の番号つき)・ほかは共通の欄だけ' using errcode = 'P0001';
      end if;
    elsif v_tbl = 'ops.variation_batches' then
      if (v_new - 'status' - 'result' - 'closed_at') is distinct from (v_old - 'status' - 'result' - 'closed_at')
         or (v_old ->> 'status') is distinct from 'open' or (v_new ->> 'status') is distinct from 'closed' then
        raise exception 'variation_write: まとめての登録は開いたものを閉じるだけ' using errcode = 'P0001';
      end if;
    else
      raise exception 'variation_write: % の行は変えない', v_tbl using errcode = 'P0001';
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_variation_write() from public;
create trigger trg_variation_write before insert or update or delete on ops.variation_group_codes for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on core.variation_axes for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on core.variation_options for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on core.sku_variation_choices for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on ops.variation_group_revisions for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on ops.variation_batches for each row execute function ops.guard_variation_write();
create trigger trg_variation_write before insert or update or delete on ops.variation_parent_adoptions for each row execute function ops.guard_variation_write();
create trigger trg_variation_no_truncate before truncate on core.variation_axes for each statement execute function core.reject_mutation();
create trigger trg_variation_no_truncate before truncate on core.variation_options for each statement execute function core.reject_mutation();
create trigger trg_variation_no_truncate before truncate on core.sku_variation_choices for each statement execute function core.reject_mutation();
create trigger trg_variation_no_truncate before truncate on ops.variation_group_revisions for each statement execute function core.reject_mutation();
create trigger trg_variation_no_truncate before truncate on ops.variation_batches for each statement execute function core.reject_mutation();

-- ═══════════ 4. 守り ═══════════
-- 4a. 開いたまとめての登録は、同じ取引で閉じないと commit できない (子だけ・札だけ残る道を作らない = 設計 §⑤ M6)
create function ops.check_variation_batch_closed() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if (select b.status from ops.variation_batches b where b.request_id = new.request_id) is distinct from 'closed' then
    raise exception 'variation_batch_unfinished: まとめての登録 (request_id %) は、同じ取引で ops.variation_batch_close を呼ばないと commit できない', new.request_id using errcode = '42501';
  end if;
  return null;
end $$;
revoke all on function ops.check_variation_batch_closed() from public;
create constraint trigger trg_variation_batches_closed after insert on ops.variation_batches
  deferrable initially deferred for each row execute function ops.check_variation_batch_closed();

-- 4b. 親か子のどちらか一方 (設計 §③ R1 High 5): 親を持つ商品は子を持てない・子を持つ商品は親を持てない。commit のときの最後の形で見る (遅らせた constraint trigger)。
--     products.parent の active が company のときだけ (load の間は夜間ロードが NE の代表を写す = 止めない・2 段の数は PR-6 の数え (parent_two_level))
create function core.check_parent_one_level() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_par bigint;
begin
  if not ops.variation_parent_company() then return null; end if;
  select p.parent_product_id into v_par from core.products p where p.product_id = new.product_id;
  if v_par is null then return null; end if;
  if exists (select 1 from core.products p where p.product_id = v_par and p.parent_product_id is not null) then
    raise exception 'parent_two_level: 商品 % の親 % にも親がある (親か子のどちらか一方)', new.product_id, v_par using errcode = '23514';
  end if;
  if exists (select 1 from core.products c where c.parent_product_id = new.product_id) then
    raise exception 'parent_two_level: 商品 % は子を持つ = 親を持てない (親か子のどちらか一方)', new.product_id using errcode = '23514';
  end if;
  return null;
end $$;
revoke all on function core.check_parent_one_level() from public;
create constraint trigger trg_products_parent_one_level after insert or update of parent_product_id on core.products
  deferrable initially deferred for each row when (new.parent_product_id is not null) execute function core.check_parent_one_level();

-- ═══════════ 5. 今あるまとまりの予約 (事前検査 → 入れる) ═══════════
-- 今あるまとまり = 札 (SKU の無い product・表示のコードがある) と 単品の代表 (子を持つ単品の product = SKU のコード)
create function ops.variation_existing_groups() returns table (group_product_id bigint, code text, code_norm text, kind text)
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select p.product_id, p.display_code, core.norm_code(p.display_code), 'tag'::text
    from core.products p
   where p.company_id = 1 and coalesce(pg_catalog.btrim(p.display_code), '') <> ''
     and not exists (select 1 from core.skus k where k.product_id = p.product_id)
  union all
  select p.product_id, k.code, k.code_norm, 'single'::text
    from core.products p join core.skus k on k.product_id = p.product_id and k.sku_kind = 'single'
   where p.company_id = 1 and exists (select 1 from core.products c where c.parent_product_id = p.product_id)
$$;
revoke all on function ops.variation_existing_groups() from public;

-- 重なりの数え (読むだけ・事前検査): 同じ norm のまとまりが 2 つ以上 (dup) / 札のコードがほかの SKU のコード (tag_is_sku) / 予約と違うまとまり (reserved_other) /
--   表示のコードの無い札 (tag_without_code = 予約しない・数だけ)。戻り値 { candidates, dup, tag_is_sku, reserved_other, tag_without_code, examples }
create function ops.variation_reservation_check() returns jsonb
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  with g as (select * from ops.variation_existing_groups()),
  dup as (select g.code_norm, pg_catalog.count(*) as n, pg_catalog.array_agg(g.group_product_id order by g.group_product_id) as ids from g group by g.code_norm having pg_catalog.count(*) > 1),
  tag_sku as (select g.code_norm, g.group_product_id from g join core.skus k on k.company_id = 1 and k.code_norm = g.code_norm and k.product_id is distinct from g.group_product_id where g.kind = 'tag'),
  res_other as (select g.code_norm, g.group_product_id from g join ops.variation_group_codes r on r.company_id = 1 and r.code_norm = g.code_norm and r.group_product_id <> g.group_product_id),
  res_moved as (select r.code_norm, r.group_product_id from ops.variation_group_codes r join g on g.group_product_id = r.group_product_id and g.code_norm <> r.code_norm)
  select pg_catalog.jsonb_build_object(
    'candidates', (select pg_catalog.count(*) from g),
    'reserved', (select pg_catalog.count(*) from ops.variation_group_codes),
    'dup', (select pg_catalog.count(*) from dup),
    'tag_is_sku', (select pg_catalog.count(*) from tag_sku),
    'reserved_other', (select pg_catalog.count(*) from res_other) + (select pg_catalog.count(*) from res_moved),
    'tag_without_code', (select pg_catalog.count(*) from core.products p where p.company_id = 1 and coalesce(pg_catalog.btrim(p.display_code), '') = ''
                           and not exists (select 1 from core.skus k where k.product_id = p.product_id)),
    'examples', (select coalesce(pg_catalog.jsonb_agg(x), '[]'::jsonb) from (
        (select pg_catalog.jsonb_build_object('kind', 'dup', 'code_norm', d.code_norm, 'product_ids', pg_catalog.to_jsonb(d.ids)) as x from dup d order by d.code_norm limit 10)
        union all (select pg_catalog.jsonb_build_object('kind', 'tag_is_sku', 'code_norm', t.code_norm, 'product_id', t.group_product_id) from tag_sku t order by t.code_norm limit 10)
        union all (select pg_catalog.jsonb_build_object('kind', 'reserved_other', 'code_norm', o.code_norm, 'product_id', o.group_product_id) from res_other o order by o.code_norm limit 10)) e))
$$;
revoke all on function ops.variation_reservation_check() from public;

-- 今あるまとまりを予約に入れる (DB の持ち主だけ・何回流しても同じ・widen の前の流し直しにも)。重なりが 1 つでもあれば何も入れずに止める
create function ops.reserve_existing_variation_groups(p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_chk jsonb;
  v_n   integer;
begin
  if p_actor is null or pg_catalog.length(pg_catalog.btrim(p_actor)) = 0 then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(core.parent_lock_key());   -- 親子の書き手 (夜間ロード・まとまりの関数) と並ぶ
  v_chk := ops.variation_reservation_check();
  if (v_chk ->> 'dup')::integer + (v_chk ->> 'tag_is_sku')::integer + (v_chk ->> 'reserved_other')::integer > 0 then
    raise exception 'reservation_collision: 今あるまとまりのコードが重なる (同じ norm のまとまり % / 札のコードが SKU のコード % / 予約と違う %)。直してから流す: %',
      v_chk ->> 'dup', v_chk ->> 'tag_is_sku', v_chk ->> 'reserved_other', v_chk -> 'examples' using errcode = 'P0001';
  end if;
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  insert into ops.variation_group_codes (company_id, code_norm, code, group_product_id, source, reserved_by)
    select 1, g.code_norm, g.code, g.group_product_id, 'load', p_actor from ops.variation_existing_groups() g
     where not exists (select 1 from ops.variation_group_codes r where r.group_product_id = g.group_product_id)
     order by g.code_norm;
  get diagnostics v_n = row_count;
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  return v_chk || pg_catalog.jsonb_build_object('inserted', v_n);
end $$;
revoke all on function ops.reserve_existing_variation_groups(text) from public;

-- 事前検査 (この migration の中) → 入れる。重なりがあれば migration ごと止まる (何も変えない)
select ops.reserve_existing_variation_groups('migration_0067');

-- ═══════════ 6. まとまりのコード・まとまりを読む・スナップショット ═══════════
/**
 * 新しいまとまりのコードの決まり (画面の確かめと ops.variation_batch_open が同じものを呼ぶ)。問題が無ければ null:
 *   code_shape = ^[A-Za-z0-9_-]{1,30}$・set- で始めない / group_exists = 予約か商品のコード (札・単品) にある = 「今あるまとまりを選ぶ」/
 *   code_taken = SKU のコード / code_in_ne = NE の今のコードか前に見たコード (商品も代表も) / code_used_before = 消した SKU のコード
 */
create function ops.variation_group_code_problem(p_code text) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_norm text;
begin
  if p_code is null or p_code !~ '^[A-Za-z0-9_-]{1,30}$' or p_code ~* '^set-' then return 'code_shape'; end if;
  v_norm := core.norm_code(p_code);
  if exists (select 1 from ops.variation_group_codes r where r.company_id = 1 and r.code_norm = v_norm) then return 'group_exists'; end if;
  if exists (select 1 from core.skus k where k.company_id = 1 and k.code_norm = v_norm) then return 'code_taken'; end if;
  if exists (select 1 from core.products p where p.company_id = 1 and core.norm_code(p.display_code) = v_norm) then return 'group_exists'; end if;
  if ops.ne_code_seen(v_norm) then return 'code_in_ne'; end if;
  if exists (select 1 from events.master_change_events e
              where e.entity_type = 'sku' and e.operation = 'DELETE' and core.norm_code(e.old_value ->> 'code') = v_norm) then return 'code_used_before'; end if;
  return null;
end $$;
revoke all on function ops.variation_group_code_problem(text) from public;

/**
 * まとまりを読んで確かめ、予約と revision の行が無ければ足す (まとまりの関数の中だけ・まとまりの鍵の後・約束の後)。戻り値 { product_id, code, kind (tag / single), sku_id }
 *   まとまり = 親の無い商品で、札 (SKU なし・表示のコードあり) か 単品の代表 (単品の SKU・子がある。p_allow_lone_single = 子の無い単品も (採用))
 *   予約が無い = 今ここで予約する (source = p_source)。同じ norm の別のまとまり・同じ表示のコードの別の札・札のコードが SKU = group_ambiguous
 */
create function ops._variation_group_resolve(p_gid bigint, p_actor text, p_source text, p_allow_lone_single boolean) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_p      record;
  v_nsku   integer;
  v_skuid  bigint;
  v_skind  text;
  v_code   text;
  v_kind   text;
  v_norm   text;
  v_res    ops.variation_group_codes;
begin
  select p.product_id, p.display_code, p.parent_product_id into v_p from core.products p where p.product_id = p_gid and p.company_id = 1;
  if not found then raise exception 'not_found: まとまり (商品 %) が無い', p_gid using errcode = 'P0002'; end if;
  if v_p.parent_product_id is not null then raise exception 'group_has_parent: 商品 % は親を持つ = まとまりにできない (親か子のどちらか一方)', p_gid using errcode = 'P0001'; end if;
  select pg_catalog.count(*) into v_nsku from core.skus k where k.product_id = p_gid;
  if v_nsku = 0 then
    v_kind := 'tag';
    v_code := v_p.display_code;
    if coalesce(pg_catalog.btrim(v_code), '') = '' then raise exception 'not_a_group: 札 (商品 %) にコードが無い', p_gid using errcode = 'P0001'; end if;
  else
    select k.sku_id, k.code, k.sku_kind into v_skuid, v_code, v_skind from core.skus k where k.product_id = p_gid order by k.sku_id limit 1;
    if v_nsku <> 1 or v_skind <> 'single' then raise exception 'not_a_group: 商品 % は単品の代表にできない (SKU % 件・%)', p_gid, v_nsku, v_skind using errcode = 'P0001'; end if;
    if not coalesce(p_allow_lone_single, false) and not exists (select 1 from core.products c where c.parent_product_id = p_gid) then
      raise exception 'not_a_group: 単品 % は子が無い = まとまりでない (新しいまとまりは「新しいまとまりを作る」)', v_code using errcode = 'P0001';
    end if;
    v_kind := 'single';
  end if;
  v_norm := core.norm_code(v_code);
  select * into v_res from ops.variation_group_codes r where r.group_product_id = p_gid;
  if found then
    if v_res.code_norm is distinct from v_norm then
      raise exception 'group_ambiguous: まとまり % の予約のコード % と今のコード % が違う', p_gid, v_res.code, v_code using errcode = 'P0001';
    end if;
  else
    if exists (select 1 from ops.variation_group_codes r where r.company_id = 1 and r.code_norm = v_norm)
       or exists (select 1 from core.products o where o.company_id = 1 and o.product_id <> p_gid and core.norm_code(o.display_code) = v_norm
                    and not exists (select 1 from core.skus k where k.product_id = o.product_id))
       or (v_kind = 'tag' and exists (select 1 from core.skus k where k.company_id = 1 and k.code_norm = v_norm)) then
      raise exception 'group_ambiguous: まとまりのコード % がほかのまとまり・SKU と重なる (決められない)', v_code using errcode = 'P0001';
    end if;
    perform pg_catalog.set_config('ops.variation_protocol', '1', true);
    insert into ops.variation_group_codes (company_id, code_norm, code, group_product_id, source, reserved_by) values (1, v_norm, v_code, p_gid, p_source, p_actor);
  end if;
  if not exists (select 1 from ops.variation_group_revisions r where r.group_product_id = p_gid) then
    perform pg_catalog.set_config('ops.variation_protocol', '1', true);
    insert into ops.variation_group_revisions (group_product_id, company_id, updated_by) values (p_gid, 1, p_actor);
  end if;
  return pg_catalog.jsonb_build_object('product_id', p_gid, 'code', v_code, 'kind', v_kind, 'sku_id', v_skuid);
end $$;
revoke all on function ops._variation_group_resolve(bigint, text, text, boolean) from public;

/**
 * 今あるまとまりの鍵 (まとまりのコードの鍵 → まとまりの鍵 = 設計 §⑤ の鍵の順)。コード = 札の表示のコード / 単品の代表の SKU のコード。
 * 鍵の後に読み直して、コードが変わっていれば retry。無い = not_found。戻り値 = まとまりのコード
 */
create function ops._variation_lock_existing_group(p_gid bigint) returns text
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_code  text;
  v_again text;
begin
  select coalesce((select k.code from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single' order by k.sku_id limit 1), p.display_code) into v_code
    from core.products p where p.product_id = p_gid and p.company_id = 1;
  if v_code is null then raise exception 'not_found: まとまり (商品 %) が無い', p_gid using errcode = 'P0002'; end if;
  perform ops.variation_lock_code(core.norm_code(v_code));
  perform ops.variation_lock_group(p_gid);
  select coalesce((select k.code from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single' order by k.sku_id limit 1), p.display_code) into v_again
    from core.products p where p.product_id = p_gid and p.company_id = 1;
  if v_again is distinct from v_code then raise exception 'retry: まとまり % のコードがちょうど変わった。もう一度', p_gid using errcode = 'P0001'; end if;
  return v_code;
end $$;
revoke all on function ops._variation_lock_existing_group(bigint) from public;

/**
 * 今のまとまりの完全なスナップショット (ph-group-v1・0066 の ops.group_snapshot_problem と同じ形)。読むだけ。
 *   有効な子 = 親がこのまとまりの単品で登録の状態が cancelled でない (状態の行が無い前からの子も)・廃止した子 = cancelled。
 *   子の選択肢 = 選択肢の行があれば軸の全部・無い (軸を入れる前からの子) = {}。JAN = 商品の有効な JAN (並びは文字の順)。共通の欄 = revision の表
 */
create function ops.variation_group_snapshot(p_gid bigint, p_revision integer, p_actor text) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_p     record;
  v_rep   record;
  v_group jsonb;
begin
  select p.product_id, p.display_code, p.name into v_p from core.products p where p.product_id = p_gid and p.company_id = 1;
  select k.sku_id, k.code into v_rep from core.skus k where k.product_id = p_gid and k.sku_kind = 'single' order by k.sku_id limit 1;
  v_group := pg_catalog.jsonb_build_object('product_id', p_gid::text, 'sku_id', case when v_rep.sku_id is null then null else v_rep.sku_id::text end,
    'code', coalesce(v_rep.code, v_p.display_code), 'name', v_p.name, 'kind', case when v_rep.sku_id is null then 'tag' else 'single' end);
  return pg_catalog.jsonb_build_object(
    'schema', 'ph-group-v1', 'revision', p_revision, 'created_by', p_actor, 'group', v_group,
    'axes', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('axis', a.axis, 'name', a.name) order by a.axis)
                        from core.variation_axes a where a.group_product_id = p_gid), '[]'::jsonb),
    'options', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('axis', o.axis, 'code', o.code, 'name', o.name, 'sort', o.sort) order by o.axis, o.sort, o.option_id)
                           from core.variation_options o where o.group_product_id = p_gid), '[]'::jsonb),
    'children', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'name', s.name,
                              'price', case when s.standard_price_jpy is null then null else pg_catalog.to_jsonb(s.standard_price_jpy) end,
                              'choices', case when ch.sku_id is null then '{}'::jsonb
                                              else pg_catalog.jsonb_build_object('1', o1.code) || case when o2.option_id is null then '{}'::jsonb else pg_catalog.jsonb_build_object('2', o2.code) end end,
                              'jans', coalesce((select pg_catalog.jsonb_agg(e.external_value order by e.external_value) from core.external_ids e
                                                 where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null), '[]'::jsonb))
                              order by s.sku_id)
                            from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single'
                            left join ops.master_registrations r on r.sku_id = s.sku_id
                            left join core.sku_variation_choices ch on ch.sku_id = s.sku_id and ch.group_product_id = p_gid
                            left join core.variation_options o1 on o1.option_id = ch.option1_id
                            left join core.variation_options o2 on o2.option_id = ch.option2_id
                           where c.parent_product_id = p_gid and c.company_id = 1 and r.state is distinct from 'cancelled'), '[]'::jsonb),
    'cancelled_children', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code) order by s.sku_id)
                                      from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single'
                                      join ops.master_registrations r on r.sku_id = s.sku_id
                                     where c.parent_product_id = p_gid and c.company_id = 1 and r.state = 'cancelled'), '[]'::jsonb),
    'common', coalesce((select r.common from ops.variation_group_revisions r where r.group_product_id = p_gid),
                       '{"shipping": null, "amazon_url": null, "asin": null, "official_url": null, "reference_urls": [], "yahoo": null}'::jsonb));
end $$;
revoke all on function ops.variation_group_snapshot(bigint, integer, text) from public;

/**
 * revision をちょうど 1 上げて、その revision の完全なスナップショットを知らせに書く (まとまりの関数の中だけ・約束の後・書いた後)。
 * 同じ取引で 2 回目 = revision_twice (変わる取引でちょうど 1)。知らせの request_id・人 = 今の約束 (0066 の知らせの保険と 0067 の置き換えが見る)
 */
create function ops._variation_bump(p_gid bigint, p_request_id uuid, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_r     ops.variation_group_revisions;
  v_rev   integer;
  v_snap  jsonb;
  v_prob  text;
  v_event uuid;
begin
  perform ops.variation_lock_group(p_gid);
  select * into v_r from ops.variation_group_revisions r where r.group_product_id = p_gid for update;
  if not found then raise exception 'not_found: まとまり % の revision の行が無い', p_gid using errcode = 'P0002'; end if;
  if v_r.revision_txid is not distinct from pg_catalog.txid_current() then
    raise exception 'revision_twice: まとまり % はこの取引でもう変えた (変わる取引で revision はちょうど 1 つ上がる = 1 つの取引で 1 回)', p_gid using errcode = 'P0001';
  end if;
  v_rev := v_r.revision + 1;
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  update ops.variation_group_revisions set revision = v_rev, revision_txid = pg_catalog.txid_current(), updated_at = pg_catalog.now(), updated_by = p_actor
   where group_product_id = p_gid;
  v_snap := ops.variation_group_snapshot(p_gid, v_rev, p_actor);
  v_prob := ops.group_snapshot_problem(1::smallint, p_gid, v_rev, v_snap);
  if v_prob is not null then raise exception 'invalid_value: まとまり % のスナップショットが ph-group-v1 の形に合わない (%)', p_gid, v_prob using errcode = '22023'; end if;
  v_event := ops.enqueue_group_snapshot(p_gid, v_rev, v_snap, p_request_id, p_actor);
  return pg_catalog.jsonb_build_object('revision', v_rev, 'event_id', v_event::text);
end $$;
revoke all on function ops._variation_bump(bigint, uuid, text) from public;

/**
 * まとまりの関数の入口の門: 段階の共有の鍵 → マスタの書き込みの共有の鍵 → 段階・持ち主表 (0053 の ops.reg_write_gate) →
 * products.parent の DB の active が company (でなければ parent_not_company = 今は全部ここで断る) → 書く列の持ち主のキーが company
 */
create function ops._variation_gate(p_ownership jsonb, p_keys text[]) returns void
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  perform ops.reg_write_gate(p_ownership);
  if not ops.variation_parent_company() then
    raise exception 'parent_not_company: 代表 (products.parent) の持ち主がまだ Company DB でない (DB の active が load) = まとまりの操作はできない' using errcode = 'P0001';
  end if;
  perform ops.reg_write_gate(p_ownership, p_keys);
end $$;
revoke all on function ops._variation_gate(jsonb, text[]) from public;

/** まとまりの約束を書く (0053 の ops.open_reg_write と同じ形・操作はまとまりの 5 つ)。p_targets = DB が決めた相手 ({ group_product_id } = 知らせの保険が見る) */
create function ops._open_variation_write(p_operation text, p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb,
                                          p_sku_id bigint, p_products bigint[], p_payload_hash text, p_targets jsonb) returns uuid
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_phase   text;
begin
  if p_operation is null or p_operation not in ('variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne') then
    raise exception 'invalid_input: 知らない操作 %', p_operation using errcode = '22023';
  end if;
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if coalesce(p_payload_hash, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: payload_hash (64 桁) が要る' using errcode = '22023'; end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'request_id_reused: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '23505';
  end if;
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, p_operation, p_sku_id, '{}', coalesce(p_products, '{}'), pg_catalog.repeat('0', 64),
            p_payload_hash, coalesce(p_targets, '{}'::jsonb), p_actor_id, nullif(p_reason, ''), 'portal_master_edit', v_db_user, v_phase, ops.ownership_hash(p_ownership), p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  return v_id;
end $$;
revoke all on function ops._open_variation_write(text, uuid, text, text, jsonb, bigint, bigint[], text, jsonb) from public;

/**
 * 同じ request_id の前の答え (無ければ null)。操作・人・done・要求のハッシュ (保存の記録の payload_hash = 呼び手の要求から DB が作った値)・相手 (SKU / まとまり) が
 * 全部同じとき = 前の結果に replayed。どれか違う = request_id_reused (同じ番号で違う相手・中身を黙って前の答えにしない・#1677 Codex R1 Medium 2)
 */
create function ops._variation_replay(p_request_id uuid, p_operation text, p_actor_id text, p_payload_hash text, p_sku_id bigint, p_group_product_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  r record;
begin
  select m.operation, m.actor_id, m.status, m.result, m.payload_hash, m.sku_id into r from ops.master_edit_requests m where m.request_id = p_request_id;
  if not found then return null; end if;
  if r.operation is distinct from p_operation or r.actor_id is distinct from p_actor_id or r.status is distinct from 'done' then
    raise exception 'request_id_reused: request_id % はほかの操作・人で使われている', p_request_id using errcode = '23505';
  end if;
  if r.payload_hash is distinct from p_payload_hash or r.sku_id is distinct from p_sku_id
     or (p_group_product_id is not null and (r.result ->> 'group_product_id') is distinct from p_group_product_id::text) then
    raise exception 'request_id_reused: request_id % は違う相手・中身で使われている (同じ操作の押し直しは同じ相手・同じ中身だけ)', p_request_id using errcode = '23505';
  end if;
  return r.result || '{"replayed": true}'::jsonb;
end $$;
revoke all on function ops._variation_replay(uuid, text, text, text, bigint, bigint) from public;

-- ═══════════ 7. まとめての登録 (開く → 子ごとに ops.register_new_sku・JAN は ops.edit_sku_jan → 閉じる。1 つの取引) ═══════════
/**
 * まとめての登録を開く。p_spec = {
 *   group: { code, name } (新しいまとまり = 札を作る・コード = 楽天の商品管理番号を人が打つ) | { product_id } (今あるまとまり = 札か単品の代表),
 *   axes: [{ axis: 1, name }, { axis: 2, name }] (新しいまとまり・軸の無い今あるまとまりは要る (1〜2 つ)。軸のある今あるまとまりは無しか今と同じ = 軸の数・名前は変えない),
 *   options: [{ axis, code: '-WH', name }] (足す選択肢だけ・並びはこの順),
 *   children: [{ code: 'hakama-WH-90', choices: { '1': '-WH', '2': '-90' } }] (1〜20・コード = まとまりのコード + 横 + 縦 (打ったとおり)) }
 * 鍵: request → 許可 (共有) → 段階 → マスタの書き込み → 親子 (排他) → まとまりのコード → まとまり → 子のコード (norm の順)。
 * 確かめ: products.parent の active が company・新しいまとまりのコードの決まり・選択肢番号の形と軸ごとの一意 (今の選択肢とも)・選択肢名の一意・
 *   子のコード = まとまり + 選択肢・子のコードの一意 (この登録の中・今の Company DB・NE・消したコード = ops.new_sku_code_problem)・(横, 縦) の一意 (今の子とも)。
 * 書く: 新しいまとまり = 札 (SKU の無い商品) と予約 (portal)・revision の行 / 今あるまとまり = 予約 (無ければ load)・revision の行。まとめての登録の行 (open)。
 * 同じ request_id = 閉じた前の答え (replayed)。戻り値 = 子ごとの request_id (ops.variation_sub_request_id(request_id, 'child:' || norm))・閉じる request_id
 */
create function ops.variation_batch_open(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_spec jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prev     ops.variation_batches;
  v_g        jsonb := p_spec -> 'group';
  v_new      boolean;
  v_gid      bigint;
  v_gcode    text;
  v_gname    text;
  v_resolved jsonb;
  v_axes_cur jsonb;
  v_axes     jsonb;
  v_axes_new boolean;
  v_naxes    integer;
  v_opts     jsonb;    -- 今の選択肢 + 足す選択肢 [{ axis, code, name }]
  v_add      jsonb := '[]'::jsonb;
  v_kids     jsonb := '[]'::jsonb;
  v_norms    text[] := '{}';
  v_combos   text[] := '{}';
  v_codes    text[] := '{}';
  e          jsonb;
  v_k        text;
  v_c1       text;
  v_c2       text;
  v_bad      text;
  v_hash     text;
  v_norm     text;
  v_result   jsonb;
  v_n        integer;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_spec) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(p_spec) k where k not in ('group', 'axes', 'options', 'children')) then
    raise exception 'invalid_input: まとめての登録の中身 (group・axes・options・children) の形が違う' using errcode = '22023';
  end if;
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'variation_batch_open', 'actor', p_actor_id, 'spec', p_spec));
  -- 鍵: request → 許可 (共有) → 段階 → マスタの書き込み (reg_write_gate) → 親子
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.variation_request:' || p_request_id::text, 0));
  perform ops._new_entry_lease_shared_locks();
  perform ops._variation_gate(p_ownership, array['products.parent', 'products.name']);
  select * into v_prev from ops.variation_batches b where b.request_id = p_request_id;
  if found then
    if v_prev.actor_id is distinct from p_actor_id or v_prev.spec_hash is distinct from v_hash then
      raise exception 'request_id_reused: 同じ番号 (request_id) で違う中身' using errcode = '23505';
    end if;
    if v_prev.status = 'closed' then return v_prev.result || '{"replayed": true}'::jsonb; end if;
    raise exception 'variation_batch_unfinished: まとめての登録 % はこの取引でもう開いている', p_request_id using errcode = '55000';
  end if;
  if exists (select 1 from ops.master_edit_requests r where r.request_id = p_request_id) then
    raise exception 'request_id_reused: request_id % はもう使われている (保存の記録がある)', p_request_id using errcode = '23505';
  end if;
  perform ops._require_new_entry_lease('single');   -- 子は新しい単品 = 単品の開放の許可 (ops.register_new_sku も子ごとに見る)
  perform pg_catalog.pg_advisory_xact_lock(core.parent_lock_key());
  -- 🆕 #1677 Codex R1 High: CSV の鍵 (JAN の ops.edit_sku_jan も同じ取引で取る = 先に) → NE のコードの共有の鍵 (照合の ops.record_ne_codes の排他と並ぶ)。
  --   鍵の順 = 0065 の共通の順 (親子 → CSV → NE のコード) の後に まとまりのコード → まとまり。コードの確かめ (まとまり・子 = NE の今と前に見たコード) はこの鍵の後で読む・commit まで持つ
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_csv'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));

  -- まとまり (コードの鍵 → まとまりの鍵)
  if pg_catalog.jsonb_typeof(v_g) is distinct from 'object' then raise exception 'invalid_input: group が要る' using errcode = '22023'; end if;
  v_new := v_g ? 'code';
  if v_new then
    if exists (select 1 from pg_catalog.jsonb_object_keys(v_g) k where k not in ('code', 'name')) or pg_catalog.jsonb_typeof(v_g -> 'code') is distinct from 'string'
       or pg_catalog.jsonb_typeof(v_g -> 'name') is distinct from 'string' then
      raise exception 'invalid_input: 新しいまとまりは { code, name }' using errcode = '22023';
    end if;
    v_gcode := v_g ->> 'code';
    v_gname := v_g ->> 'name';
    if ops.variation_label_problem(v_gname, 255) is not null or pg_catalog.lower(v_gname) = 'empty' then
      raise exception 'invalid_value: まとまりの名前は 1〜255 字 (前後の空白・制御文字なし)' using errcode = '22023';
    end if;
    perform ops.variation_lock_code(core.norm_code(v_gcode));
    v_bad := ops.variation_group_code_problem(v_gcode);
    if v_bad is not null then raise exception '%: まとまりのコード % は新しいまとまりに使えない', v_bad, v_gcode using errcode = 'P0001'; end if;
    v_gid := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.products', 'product_id')::regclass);
    perform ops.variation_lock_group(v_gid);
    v_axes_cur := '[]'::jsonb;
    v_opts := '[]'::jsonb;
  else
    if exists (select 1 from pg_catalog.jsonb_object_keys(v_g) k where k <> 'product_id') or pg_catalog.jsonb_typeof(v_g -> 'product_id') not in ('string', 'number')
       or (v_g ->> 'product_id') !~ '^[1-9][0-9]{0,17}$' then
      raise exception 'invalid_input: 今あるまとまりは { product_id }' using errcode = '22023';
    end if;
    v_gid := (v_g ->> 'product_id')::bigint;
    v_gcode := ops._variation_lock_existing_group(v_gid);   -- まとまりのコード → まとまり
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('axis', a.axis, 'name', a.name) order by a.axis), '[]'::jsonb) into v_axes_cur
      from core.variation_axes a where a.group_product_id = v_gid;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('axis', o.axis, 'code', o.code, 'name', o.name) order by o.axis, o.sort, o.option_id), '[]'::jsonb) into v_opts
      from core.variation_options o where o.group_product_id = v_gid;
    -- 今の子の組 (選択肢の番号の小文字)
    select coalesce(pg_catalog.array_agg(o1.code_norm || '|' || coalesce(o2.code_norm, '')), '{}') into v_combos
      from core.sku_variation_choices ch join core.variation_options o1 on o1.option_id = ch.option1_id left join core.variation_options o2 on o2.option_id = ch.option2_id
     where ch.group_product_id = v_gid;
  end if;

  -- 軸
  v_axes_new := v_axes_cur = '[]'::jsonb;
  if v_axes_new then
    v_axes := p_spec -> 'axes';
    if pg_catalog.jsonb_typeof(v_axes) is distinct from 'array' or pg_catalog.jsonb_array_length(v_axes) not between 1 and 2 then
      raise exception 'invalid_input: 軸 (axes) は 1〜2 つ (横・要るときだけ縦)' using errcode = '22023';
    end if;
    v_n := 0;
    for e in select x from pg_catalog.jsonb_array_elements(v_axes) x loop
      v_n := v_n + 1;
      if pg_catalog.jsonb_typeof(e) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(e) k where k not in ('axis', 'name'))
         or (e ->> 'axis') is distinct from v_n::text or pg_catalog.jsonb_typeof(e -> 'axis') is distinct from 'number'
         or pg_catalog.jsonb_typeof(e -> 'name') is distinct from 'string' or ops.variation_label_problem(e ->> 'name', 100) is not null then
        raise exception 'invalid_input: 軸は [{ axis: 1, name }, { axis: 2, name }] の順 (名前は 1〜100 字・前後の空白なし)' using errcode = '22023';
      end if;
    end loop;
  else
    if p_spec ? 'axes' and (p_spec -> 'axes') is distinct from v_axes_cur then
      raise exception 'axes_fixed: 今あるまとまりの軸は変えない (軸の数は同じ・名前は「名前を直す」で)' using errcode = 'P0001';
    end if;
    v_axes := v_axes_cur;
  end if;
  v_naxes := pg_catalog.jsonb_array_length(v_axes);

  -- 足す選択肢
  if coalesce(pg_catalog.jsonb_typeof(p_spec -> 'options'), 'array') <> 'array' then raise exception 'invalid_input: options は配列' using errcode = '22023'; end if;
  for e in select x from pg_catalog.jsonb_array_elements(coalesce(p_spec -> 'options', '[]'::jsonb)) x loop
    if pg_catalog.jsonb_typeof(e) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(e) k where k not in ('axis', 'code', 'name'))
       or pg_catalog.jsonb_typeof(e -> 'axis') is distinct from 'number' or (e ->> 'axis') not in ('1', '2')
       or (case when (e ->> 'axis') in ('1', '2') then (e ->> 'axis')::integer else 99 end) > v_naxes   -- 1.5 などを integer にしない
       or pg_catalog.jsonb_typeof(e -> 'code') is distinct from 'string' or pg_catalog.jsonb_typeof(e -> 'name') is distinct from 'string' then
      raise exception 'invalid_input: 選択肢は { axis (1 / 2・軸のあるもの), code, name }' using errcode = '22023';
    end if;
    if (e ->> 'code') !~ '^-[A-Za-z0-9]{1,10}$' then
      raise exception 'option_code_shape: 選択肢番号 % は「-」+ 英数字 1〜10 字 (例 -WH・-90)', e ->> 'code' using errcode = '22023';
    end if;
    if ops.variation_label_problem(e ->> 'name', 100) is not null then
      raise exception 'invalid_value: 選択肢名 % は 1〜100 字 (前後の空白・制御文字なし)', e ->> 'name' using errcode = '22023';
    end if;
    if exists (select 1 from pg_catalog.jsonb_array_elements(v_opts) o where (o ->> 'axis') = (e ->> 'axis') and pg_catalog.lower(o ->> 'code') = pg_catalog.lower(e ->> 'code')) then
      raise exception 'option_exists: 軸 % の選択肢番号 % はもうある', e ->> 'axis', e ->> 'code' using errcode = 'P0001';
    end if;
    if exists (select 1 from pg_catalog.jsonb_array_elements(v_opts) o
                where (o ->> 'axis') = (e ->> 'axis') and pg_catalog.btrim(pg_catalog.normalize(o ->> 'name', 'NFKC')) = pg_catalog.btrim(pg_catalog.normalize(e ->> 'name', 'NFKC'))) then
      raise exception 'option_name_exists: 軸 % の選択肢名 % はもうある', e ->> 'axis', e ->> 'name' using errcode = 'P0001';
    end if;
    v_opts := v_opts || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('axis', (e ->> 'axis')::integer, 'code', e ->> 'code', 'name', e ->> 'name'));
    v_add := v_add || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('axis', (e ->> 'axis')::integer, 'code', e ->> 'code', 'name', e ->> 'name'));
  end loop;

  -- 子 (コード = まとまり + 選択肢・一意・組の一意)
  if pg_catalog.jsonb_typeof(p_spec -> 'children') is distinct from 'array' or pg_catalog.jsonb_array_length(p_spec -> 'children') not between 1 and ops.variation_max_children() then
    raise exception 'too_many: 子は 1〜% 件 (1 回のまとめての登録)', ops.variation_max_children() using errcode = '22023';
  end if;
  for e in select x from pg_catalog.jsonb_array_elements(p_spec -> 'children') x loop
    if pg_catalog.jsonb_typeof(e) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(e) k where k not in ('code', 'choices'))
       or pg_catalog.jsonb_typeof(e -> 'code') is distinct from 'string' or pg_catalog.jsonb_typeof(e -> 'choices') is distinct from 'object' then
      raise exception 'invalid_input: 子は { code, choices }' using errcode = '22023';
    end if;
    if (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(e -> 'choices')) <> v_naxes
       or exists (select 1 from pg_catalog.jsonb_object_keys(e -> 'choices') k where k not in ('1', '2') or (case when k in ('1', '2') then k::integer else 99 end) > v_naxes)
       or exists (select 1 from pg_catalog.jsonb_each(e -> 'choices') c where pg_catalog.jsonb_typeof(c.value) <> 'string') then
      raise exception 'invalid_input: 子 % の選択肢は軸の全部 (%) を 1 つずつ', e ->> 'code', v_naxes using errcode = '22023';
    end if;
    v_c1 := e -> 'choices' ->> '1';
    v_c2 := e -> 'choices' ->> '2';
    if not exists (select 1 from pg_catalog.jsonb_array_elements(v_opts) o where (o ->> 'axis') = '1' and (o ->> 'code') = v_c1)
       or (v_c2 is not null and not exists (select 1 from pg_catalog.jsonb_array_elements(v_opts) o where (o ->> 'axis') = '2' and (o ->> 'code') = v_c2)) then
      raise exception 'choice_unknown: 子 % の選択肢 % / % がまとまりの選択肢 (打ったとおり) に無い', e ->> 'code', v_c1, coalesce(v_c2, '-') using errcode = 'P0001';
    end if;
    if (e ->> 'code') is distinct from v_gcode || v_c1 || coalesce(v_c2, '') then
      raise exception 'child_code_not_group_plus_choices: 子のコード % は まとまりのコード + 選択肢番号 (%) でない', e ->> 'code', v_gcode || v_c1 || coalesce(v_c2, '') using errcode = 'P0001';
    end if;
    v_norm := core.norm_code(e ->> 'code');
    if v_norm = any (v_norms) then raise exception 'child_dup: 子のコード % が 2 回 (大文字小文字を問わず)', e ->> 'code' using errcode = 'P0001'; end if;
    v_k := pg_catalog.lower(v_c1) || '|' || coalesce(pg_catalog.lower(v_c2), '');
    if v_k = any (v_combos) then raise exception 'choice_exists: 選択肢の組 % / % の子はもうある', v_c1, coalesce(v_c2, '-') using errcode = 'P0001'; end if;
    v_norms := v_norms || v_norm;
    v_combos := v_combos || v_k;
    v_codes := v_codes || (e ->> 'code');
    v_kids := v_kids || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', e ->> 'code', 'choices', e -> 'choices'));
  end loop;
  -- 子のコードの鍵 (norm の順・SKU の登録と同じ鍵) → 新しいコードの決まり (0064 の ops.new_sku_code_problem = 形・Company DB・名札・NE・消したコード)
  for v_norm in select x from pg_catalog.unnest(v_norms) x order by 1 loop perform ops.variation_lock_code(v_norm); end loop;
  for v_k in select x from pg_catalog.unnest(v_codes) x loop
    v_bad := ops.new_sku_code_problem(v_k);
    if v_bad is not null then raise exception '%: 子のコード % は新しい商品に使えない', v_bad, v_k using errcode = 'P0001'; end if;
  end loop;

  -- 約束 → 書く
  perform ops._open_variation_write('variation_batch_open', p_request_id, p_actor_id, p_reason, p_ownership, null,
    case when v_new then array[v_gid] else '{}'::bigint[] end, v_hash, pg_catalog.jsonb_build_object('group_product_id', v_gid::text));
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  if v_new then
    insert into core.products (product_id, company_id, display_code, name, status, created_by_type, created_by_id)
      overriding system value values (v_gid, 1, v_gcode, v_gname, 'active', 'human', p_actor_id);
    insert into ops.variation_group_codes (company_id, code_norm, code, group_product_id, source, reserved_by) values (1, core.norm_code(v_gcode), v_gcode, v_gid, 'portal', p_actor_id);
    insert into ops.variation_group_revisions (group_product_id, company_id, updated_by) values (v_gid, 1, p_actor_id);
  else
    v_resolved := ops._variation_group_resolve(v_gid, p_actor_id, 'load', false);
    if (v_resolved ->> 'code') is distinct from v_gcode then raise exception 'retry: まとまり % のコードがちょうど変わった', v_gid using errcode = 'P0001'; end if;
  end if;
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'group_product_id', v_gid::text, 'group_code', v_gcode, 'group_created', v_new,
    'axes', v_axes,
    'children', (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', x ->> 'code',
                   'request_id', ops.variation_sub_request_id(p_request_id, 'child:' || core.norm_code(x ->> 'code'))::text) order by o) from pg_catalog.jsonb_array_elements(v_kids) with ordinality t(x, o)),
    'close_request_id', ops.variation_sub_request_id(p_request_id, 'close')::text, 'max_children', ops.variation_max_children());
  insert into ops.variation_batches (request_id, company_id, txid, group_product_id, group_created, spec, spec_hash, child_codes, actor_id)
    values (p_request_id, 1, pg_catalog.txid_current(), v_gid, v_new,
            pg_catalog.jsonb_build_object('group_code', v_gcode, 'axes', v_axes, 'axes_new', v_axes_new, 'options', v_add, 'children', v_kids),
            v_hash, v_codes, p_actor_id);
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  perform ops.close_reg_write(v_result, v_gcode);
  return v_result;
end $$;
revoke all on function ops.variation_batch_open(uuid, text, text, jsonb, jsonb) from public;

/**
 * まとめての登録を閉じる (同じ取引・開いた人だけ)。子 = 開いたときのコードの全部が、この取引で ops.register_new_sku で登録した下書きの単品
 *   (子の request_id = ops.variation_sub_request_id(まとめての request_id, 'child:' || norm)・カードの知らせなし・親なし・一度も配っていない)。
 * 書く: 軸 (新しいときだけ)・足す選択肢 (並び = 今の最後の次から)・子の選択肢・子の親 (帰属 manual)・共通の欄 (p_common があれば)・revision + 1・スナップショットの知らせ・
 *   まとめての登録の行を閉じる。約束 = variation_batch_close (request_id = ops.variation_sub_request_id(まとめての request_id, 'close'))
 */
create function ops.variation_batch_close(p_request_id uuid, p_actor_id text, p_ownership jsonb, p_common jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_b       ops.variation_batches;
  v_close   uuid;
  v_code    text;
  v_s       record;
  v_skus    bigint[] := '{}';
  v_prods   bigint[] := '{}';
  v_kids    jsonb := '[]'::jsonb;
  e         jsonb;
  v_sort    integer;
  v_bump    jsonb;
  v_result  jsonb;
  v_hash    text;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  select * into v_b from ops.variation_batches b where b.request_id = p_request_id for update;
  if not found or v_b.status <> 'open' or v_b.txid <> pg_catalog.txid_current() then
    raise exception 'no_open_batch: この取引で開いたまとめての登録 (request_id %) が無い', p_request_id using errcode = 'P0001';
  end if;
  if v_b.actor_id is distinct from p_actor_id then raise exception 'master_write_session_mismatch: まとめての登録を開いた人と違う' using errcode = '42501'; end if;
  if p_common is not null and pg_catalog.jsonb_typeof(p_common) <> 'object' then raise exception 'invalid_input: 共通の欄 (common) は object' using errcode = '22023'; end if;
  perform ops._variation_gate(p_ownership, array['products.parent', 'products.name']);
  perform pg_catalog.pg_advisory_xact_lock(core.parent_lock_key());
  perform ops.variation_lock_group(v_b.group_product_id);
  -- 子 (開いたときのコードの順)
  foreach v_code in array v_b.child_codes loop
    select s.sku_id, s.code, s.code_norm, s.sku_kind, s.product_id, s.xmin = pg_catalog.pg_current_xact_id()::xid and s.created_at = pg_catalog.now() as fresh,
           r.state as reg, p.parent_product_id
      into v_s
      from core.skus s left join ops.master_registrations r on r.sku_id = s.sku_id left join core.products p on p.product_id = s.product_id
     where s.company_id = 1 and s.code_norm = core.norm_code(v_code);
    if not found then raise exception 'child_not_registered: 子 % がまだ登録されていない (先に ops.register_new_sku)', v_code using errcode = 'P0001'; end if;
    if v_s.code is distinct from v_code or v_s.sku_kind <> 'single' or v_s.product_id is null then
      raise exception 'child_mismatch: 子 % が開いたときのコード・単品と違う (%)', v_code, v_s.code using errcode = 'P0001';
    end if;
    if not v_s.fresh or v_s.reg is distinct from 'draft' then
      raise exception 'child_not_new: 子 % はこの取引で登録した下書きでない', v_code using errcode = 'P0001';
    end if;
    if not exists (select 1 from ops.master_edit_requests m where m.request_id = ops.variation_sub_request_id(p_request_id, 'child:' || v_s.code_norm)
                     and m.operation = 'sku_create' and m.status = 'done' and m.sku_id = v_s.sku_id) then
      raise exception 'child_request_mismatch: 子 % の登録の request_id がまとめての登録から決めた番号でない', v_code using errcode = 'P0001';
    end if;
    if v_s.parent_product_id is not null then raise exception 'child_has_parent: 子 % にはもう親がある', v_code using errcode = 'P0001'; end if;
    if exists (select 1 from ops.product_hub_outbox o where o.sku_id = v_s.sku_id) then
      raise exception 'child_card_event: 子 % にカードの知らせがある (子ごとのカードは作らない = まとまりで 1 枚。登録の card は無し)', v_code using errcode = 'P0001';
    end if;
    if ops.sku_ever_issued(v_s.sku_id) then raise exception 'parent_frozen: 子 % は NE 登録の CSV を配った', v_code using errcode = 'P0001'; end if;
    v_skus := v_skus || v_s.sku_id;
    v_prods := v_prods || v_s.product_id;
  end loop;
  v_close := ops.variation_sub_request_id(p_request_id, 'close');
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'variation_batch_close', 'batch', p_request_id::text, 'skus', pg_catalog.to_jsonb(v_skus), 'common', p_common));
  perform ops._open_variation_write('variation_batch_close', v_close, p_actor_id, null, p_ownership, null, v_prods, v_hash,
    pg_catalog.jsonb_build_object('group_product_id', v_b.group_product_id::text, 'batch', p_request_id::text));
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  -- 軸 (新しいときだけ)・足す選択肢 (並び = 軸ごとに今の最後の次から)
  if (v_b.spec -> 'axes_new') = 'true'::jsonb then
    insert into core.variation_axes (company_id, group_product_id, axis, name, created_by_type, created_by_id)
      select 1, v_b.group_product_id, (a ->> 'axis')::smallint, a ->> 'name', 'human', p_actor_id from pg_catalog.jsonb_array_elements(v_b.spec -> 'axes') a order by (a ->> 'axis')::integer;
  end if;
  for e in select x from pg_catalog.jsonb_array_elements(v_b.spec -> 'options') x loop
    select coalesce(pg_catalog.max(o.sort) + 1, 0) into v_sort from core.variation_options o where o.group_product_id = v_b.group_product_id and o.axis = (e ->> 'axis')::smallint;
    insert into core.variation_options (company_id, group_product_id, axis, code, name, sort, created_by_type, created_by_id)
      values (1, v_b.group_product_id, (e ->> 'axis')::smallint, e ->> 'code', e ->> 'name', v_sort, 'human', p_actor_id);
  end loop;
  -- 子の選択肢 (開いたときの組)
  insert into core.sku_variation_choices (sku_id, company_id, group_product_id, option1_id, option2_id, created_by_type, created_by_id)
    select s.sku_id, 1, v_b.group_product_id, o1.option_id, o2.option_id, 'human', p_actor_id
      from pg_catalog.jsonb_array_elements(v_b.spec -> 'children') c
      join core.skus s on s.company_id = 1 and s.code_norm = core.norm_code(c ->> 'code')
      join core.variation_options o1 on o1.group_product_id = v_b.group_product_id and o1.axis = 1 and o1.code = (c -> 'choices' ->> '1')
      left join core.variation_options o2 on o2.group_product_id = v_b.group_product_id and o2.axis = 2 and o2.code = (c -> 'choices' ->> '2');
  if (select pg_catalog.count(*) from core.sku_variation_choices ch where ch.sku_id = any (v_skus)) <> pg_catalog.cardinality(v_skus)
     or exists (select 1 from core.sku_variation_choices ch where ch.sku_id = any (v_skus)
                 and (ch.option2_id is not null) is distinct from (pg_catalog.jsonb_array_length(v_b.spec -> 'axes') = 2)) then
    raise exception 'choice_unknown: 子の選択肢を書けなかった (開いたときの選択肢と違う)' using errcode = 'P0001';
  end if;
  -- 子の親 (親子の鍵と印 = 0036・帰属 manual。一度も配っていない子だけ = 0065 の守りも同じ)
  perform pg_catalog.set_config('core.parent_protocol', '1', true);
  update core.products set parent_product_id = v_b.group_product_id, parent_set_by = 'manual' where product_id = any (v_prods);
  perform pg_catalog.set_config('core.parent_protocol', '', true);
  if p_common is not null then
    update ops.variation_group_revisions set common = p_common, updated_at = pg_catalog.now(), updated_by = p_actor_id where group_product_id = v_b.group_product_id;
  end if;
  v_bump := ops._variation_bump(v_b.group_product_id, v_close, p_actor_id);
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', s.code, 'sku_id', s.sku_id::text) order by s.code_norm) into v_kids
    from core.skus s where s.sku_id = any (v_skus);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'group_product_id', v_b.group_product_id::text, 'group_code', v_b.spec ->> 'group_code',
    'group_created', v_b.group_created, 'children', v_kids, 'revision', v_bump -> 'revision', 'event_id', v_bump -> 'event_id');
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  update ops.variation_batches set status = 'closed', result = v_result, closed_at = pg_catalog.clock_timestamp() where request_id = p_request_id;
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  perform ops.close_reg_write(v_result, v_b.spec ->> 'group_code');
  return v_result;
end $$;
revoke all on function ops.variation_batch_close(uuid, text, jsonb, jsonb) from public;

-- ═══════════ 8. 子の廃止・名前を直す・quarantined の代表の採用 ═══════════
/**
 * 子の廃止 (設計 §⑤ variation_child_cancel): まとまりの子の単品で、登録の状態が 下書き / NE 登録待ち・生きているファイル (built / issued / import_declared / partial) が無い・
 *   NE に一度も現れていない (0058 の ops.ne_code_seen = 今の NE と前に見たコード) ときだけ。登録の状態を cancelled (人の理由) にし、revision + 1・スナップショット。
 *   廃止したコードは使い回さない (SKU は残る = ops.new_sku_code_problem の code_taken)。まとまりやほかの子を自動で作り直さない
 * 鍵: 段階 → マスタの書き込み → SKU → まとまり
 */
create function ops.cancel_variation_child(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_s      record;
  v_prev   jsonb;
  v_hash   text;
  v_bump   jsonb;
  v_result jsonb;
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

/**
 * まとまりの名前 (札だけ・単品の代表の名前は単品の保存で)・軸の名前・選択肢名を直す (社内だけ・NE に送らない・番号とコードは直せない)。
 *   p_seen_revision = 画面が見た revision (違えば version_conflict)。p_changes = { name?, axes?: [{ axis, name }], options?: [{ axis, code (打ったとおり), name }] }。
 *   変わる所が無い = 何も書かない (no_change・revision はそのまま)。選択肢名は直した後も軸ごとに一意。revision + 1・スナップショット (product-hub は「社内の選択肢名が変わった」を出す)
 */
create function ops.edit_variation_labels(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_group_product_id bigint, p_seen_revision integer, p_changes jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prev    jsonb;
  v_code    text;
  v_kind    text;
  v_rev     integer;
  v_pname   text;
  v_name    text;
  v_axes    jsonb := '[]'::jsonb;   -- 変わる軸
  v_opts    jsonb := '[]'::jsonb;   -- 変わる選択肢 [{ option_id, name }]
  e         jsonb;
  v_cur     record;
  v_hash    text;
  v_bump    jsonb;
  v_result  jsonb;
begin
  if p_request_id is null or p_group_product_id is null then raise exception 'invalid_input: request_id とまとまりが要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p_changes) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(p_changes) k where k not in ('name', 'axes', 'options'))
     or coalesce(pg_catalog.jsonb_typeof(p_changes -> 'axes'), 'array') <> 'array' or coalesce(pg_catalog.jsonb_typeof(p_changes -> 'options'), 'array') <> 'array'
     or (p_changes ? 'name' and pg_catalog.jsonb_typeof(p_changes -> 'name') <> 'string') then
    raise exception 'invalid_input: 直す中身は { name, axes: [{ axis, name }], options: [{ axis, code, name }] }' using errcode = '22023';
  end if;
  perform ops._variation_gate(p_ownership, array['products.parent', 'products.name']);
  -- 要求のハッシュ = 呼び手の要求そのもの (まとまり・見た revision・直す中身・理由) = 押し直しは同じ値 (#1677 Codex R1 Medium 2)
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'variation_label_edit', 'group_product_id', p_group_product_id, 'seen_revision', p_seen_revision, 'changes', p_changes, 'reason', p_reason));
  v_prev := ops._variation_replay(p_request_id, 'variation_label_edit', p_actor_id, v_hash, null, p_group_product_id);
  if v_prev is not null then return v_prev; end if;
  perform ops._variation_lock_existing_group(p_group_product_id);   -- まとまりのコード → まとまり
  select coalesce((select k.code from core.skus k where k.product_id = p.product_id and k.sku_kind = 'single' order by k.sku_id limit 1), p.display_code),
         case when exists (select 1 from core.skus k where k.product_id = p.product_id) then 'single' else 'tag' end, p.name
    into v_code, v_kind, v_pname from core.products p where p.product_id = p_group_product_id and p.company_id = 1;
  if v_code is null then raise exception 'not_found: まとまり (商品 %) が無い', p_group_product_id using errcode = 'P0002'; end if;
  v_rev := coalesce((select r.revision from ops.variation_group_revisions r where r.group_product_id = p_group_product_id), 0);
  if p_seen_revision is distinct from v_rev then
    raise exception 'version_conflict: 画面を開いた後にまとまり % が変わった (revision % → %)', v_code, p_seen_revision, v_rev using errcode = 'P0001';
  end if;
  -- まとまりの名前 (札だけ)
  if p_changes ? 'name' and (p_changes ->> 'name') is distinct from v_pname then
    if v_kind <> 'tag' then raise exception 'group_name_is_single: 単品の代表のまとまりの名前は単品の名前 (商品の画面で直す)' using errcode = 'P0001'; end if;
    v_name := p_changes ->> 'name';
    if ops.variation_label_problem(v_name, 255) is not null or pg_catalog.lower(v_name) = 'empty' then
      raise exception 'invalid_value: まとまりの名前は 1〜255 字 (前後の空白・制御文字なし)' using errcode = '22023';
    end if;
  end if;
  -- 軸の名前
  for e in select x from pg_catalog.jsonb_array_elements(coalesce(p_changes -> 'axes', '[]'::jsonb)) x loop
    if pg_catalog.jsonb_typeof(e) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(e) k where k not in ('axis', 'name'))
       or pg_catalog.jsonb_typeof(e -> 'axis') is distinct from 'number' or (e ->> 'axis') not in ('1', '2') or pg_catalog.jsonb_typeof(e -> 'name') is distinct from 'string' then
      raise exception 'invalid_input: 軸は { axis, name }' using errcode = '22023';
    end if;
    select a.axis, a.name into v_cur from core.variation_axes a where a.group_product_id = p_group_product_id and a.axis = (e ->> 'axis')::smallint;
    if not found then raise exception 'not_found: まとまり % に軸 % が無い', v_code, e ->> 'axis' using errcode = 'P0002'; end if;
    if ops.variation_label_problem(e ->> 'name', 100) is not null then raise exception 'invalid_value: 軸の名前は 1〜100 字 (前後の空白・制御文字なし)' using errcode = '22023'; end if;
    if (e ->> 'name') is distinct from v_cur.name then v_axes := v_axes || pg_catalog.jsonb_build_array(e); end if;
  end loop;
  -- 選択肢名 (番号は打ったとおりで選ぶ)
  for e in select x from pg_catalog.jsonb_array_elements(coalesce(p_changes -> 'options', '[]'::jsonb)) x loop
    if pg_catalog.jsonb_typeof(e) is distinct from 'object' or exists (select 1 from pg_catalog.jsonb_object_keys(e) k where k not in ('axis', 'code', 'name'))
       or pg_catalog.jsonb_typeof(e -> 'axis') is distinct from 'number' or (e ->> 'axis') not in ('1', '2')
       or pg_catalog.jsonb_typeof(e -> 'code') is distinct from 'string' or pg_catalog.jsonb_typeof(e -> 'name') is distinct from 'string' then
      raise exception 'invalid_input: 選択肢は { axis, code, name }' using errcode = '22023';
    end if;
    select o.option_id, o.name into v_cur from core.variation_options o where o.group_product_id = p_group_product_id and o.axis = (e ->> 'axis')::smallint and o.code = (e ->> 'code');
    if not found then raise exception 'not_found: まとまり % の軸 % に選択肢 % が無い (番号は打ったとおり)', v_code, e ->> 'axis', e ->> 'code' using errcode = 'P0002'; end if;
    if ops.variation_label_problem(e ->> 'name', 100) is not null then raise exception 'invalid_value: 選択肢名は 1〜100 字 (前後の空白・制御文字なし)' using errcode = '22023'; end if;
    if (e ->> 'name') is distinct from v_cur.name then
      v_opts := v_opts || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('option_id', v_cur.option_id, 'axis', (e ->> 'axis')::integer, 'name', e ->> 'name'));
    end if;
  end loop;
  -- 直した後も選択肢名は軸ごとに一意 (直さない選択肢とも・直す選択肢どうしも)
  if exists (select 1 from (
               select o.axis, pg_catalog.btrim(pg_catalog.normalize(coalesce((select c ->> 'name' from pg_catalog.jsonb_array_elements(v_opts) c where (c ->> 'option_id')::bigint = o.option_id), o.name), 'NFKC')) as k
                 from core.variation_options o where o.group_product_id = p_group_product_id) t
             group by t.axis, t.k having pg_catalog.count(*) > 1) then
    raise exception 'option_name_exists: 直した後の選択肢名が同じ軸のほかの選択肢と同じになる' using errcode = 'P0001';
  end if;
  if v_name is null and v_axes = '[]'::jsonb and v_opts = '[]'::jsonb then
    return pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'group_product_id', p_group_product_id::text, 'no_change', true, 'revision', v_rev);
  end if;
  perform ops._open_variation_write('variation_label_edit', p_request_id, p_actor_id, p_reason, p_ownership, null,
    case when v_name is not null then array[p_group_product_id] else '{}'::bigint[] end,
    v_hash,
    pg_catalog.jsonb_build_object('group_product_id', p_group_product_id::text));
  perform ops._variation_group_resolve(p_group_product_id, p_actor_id, 'load', false);
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  if v_name is not null then update core.products set name = v_name where product_id = p_group_product_id; end if;
  update core.variation_axes a set name = c ->> 'name', updated_at = pg_catalog.now()
    from pg_catalog.jsonb_array_elements(v_axes) c where a.group_product_id = p_group_product_id and a.axis = (c ->> 'axis')::smallint;
  update core.variation_options o set name = c ->> 'name', updated_at = pg_catalog.now()
    from pg_catalog.jsonb_array_elements(v_opts) c where o.option_id = (c ->> 'option_id')::bigint;
  v_bump := ops._variation_bump(p_group_product_id, p_request_id, p_actor_id);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'group_product_id', p_group_product_id::text, 'group_code', v_code,
    'changed', pg_catalog.jsonb_build_object('name', v_name, 'axes', v_axes, 'options', v_opts), 'revision', v_bump -> 'revision', 'event_id', v_bump -> 'event_id');
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  perform ops.close_reg_write(v_result, v_code);
  return v_result;
end $$;
revoke all on function ops.edit_variation_labels(uuid, text, text, jsonb, bigint, integer, jsonb) from public;

/**
 * NE で直接作られた商品 (quarantined) の代表を、NE から 1 回だけ採用する (設計 §⑤ R3 High 4・中原さんの答え a・約束 parent_adopt_ne)。DB が自分で確かめる:
 *   1. 登録の状態が quarantined・Company DB の親が無い・前に採用していない (ops.variation_parent_adoptions)・子を持たない (2 段にしない)
 *   2. 最新の封のある照合の回 (受け取りのある回の最新) = NE の元のコードの印の回 (0041) で、その商品の観測が present・trusted・単品・代表の列が読めて値がある。
 *      代表の元の書き方 = その回の 0041 (rep → product の順・ok だけ)
 *   3. 代表のコードが予約にある (今あるまとまり) / 予約に無い = 今ある札・単品 (子の無い単品も = NE ではそれが代表) を 1 つだけ見つけて予約 /
 *      見つからない = 同じ取引で札を作って予約 (ne_adopt)。2 つ以上に当たる = group_ambiguous
 *   書く: 親 (帰属 manual・由来は採用の記録)・revision + 1・スナップショット。その後は凍結 (親を変える関数は無い)
 * 鍵: 段階 → マスタの書き込み → SKU → 親子 (排他) → まとまりのコード → まとまり。🚨 PR-6 の親の門が閉じていても通す (直す道)
 */
create function ops.adopt_ne_parent_for_quarantined(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_prev     jsonb;
  v_s        record;
  v_run      text;
  v_obs      jsonb;
  v_par      jsonb;
  v_rnorm    text;
  v_rcode    text;
  v_gid      bigint;
  v_created  boolean := false;
  v_n        integer;
  v_gname    text;
  v_hash     text;
  v_bump     jsonb;
  v_result   jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null then raise exception 'invalid_input: 人・理由の形が違う' using errcode = '22023'; end if;
  perform ops._variation_gate(p_ownership, array['products.parent', 'products.name']);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'parent_adopt_ne', 'sku_id', p_sku_id, 'reason', p_reason));
  v_prev := ops._variation_replay(p_request_id, 'parent_adopt_ne', p_actor_id, v_hash, p_sku_id, null);
  if v_prev is not null then return v_prev; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  -- 🆕 #1677 Codex R1 High: SKU → 親子 (排他) → NE のコード (共有) の後に、最新の封のある回・NE の元のコードの印・観測・元の書き方を読む (照合が入れ替える途中の世代を混ぜない・commit まで持つ)
  perform pg_catalog.pg_advisory_xact_lock(core.parent_lock_key());
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  select s.sku_id, s.code, s.code_norm, s.sku_kind, s.name, s.product_id, p.parent_product_id, r.state as reg into v_s
    from core.skus s left join core.products p on p.product_id = s.product_id left join ops.master_registrations r on r.sku_id = s.sku_id
   where s.sku_id = p_sku_id and s.company_id = 1;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if v_s.sku_kind <> 'single' or v_s.product_id is null then raise exception 'invalid_input: % は単品でない', v_s.code using errcode = '22023'; end if;
  if v_s.reg is distinct from 'quarantined' then raise exception 'not_quarantined: % の登録の状態が % (採用は NE で直接作られた商品 = quarantined だけ)', v_s.code, coalesce(v_s.reg, 'なし') using errcode = 'P0001'; end if;
  if exists (select 1 from ops.variation_parent_adoptions a where a.sku_id = p_sku_id) then raise exception 'already_adopted: % の代表はもう採用した (1 回だけ)', v_s.code using errcode = 'P0001'; end if;
  if v_s.parent_product_id is not null then raise exception 'already_has_parent: % にはもう親がある', v_s.code using errcode = 'P0001'; end if;
  if exists (select 1 from core.products c where c.parent_product_id = v_s.product_id) then
    raise exception 'parent_two_level: % は子を持つ = 親を持てない (親か子のどちらか一方)', v_s.code using errcode = 'P0001';
  end if;
  -- 最新の封のある照合の回 = NE の元のコードの印の回
  select m.compare_run_id into v_run from ops.ne_reg_compare_receipts rc join ops.master_compare_runs m on m.compare_run_id = rc.compare_run_id
   order by m.observed_at desc, m.compare_run_id desc limit 1;
  if v_run is null or v_run is distinct from (select k.compare_run_id from ops.master_ne_code_mark k where k.id = 1) then
    raise exception 'ne_not_fresh: 最新の封のある照合の回 (%) と NE の元のコードの回が違う (翌朝の照合の後に)', coalesce(v_run, 'なし') using errcode = 'P0001';
  end if;
  select o.observation into v_obs from ops.ne_reg_compare_observations o where o.compare_run_id = v_run and o.code_norm = v_s.code_norm;
  if v_obs is null then raise exception 'ne_not_observed: 照合の回 % に % の NE の観測が無い (翌朝の照合の後に)', v_run, v_s.code using errcode = 'P0001'; end if;
  if (v_obs -> 'present') is distinct from 'true'::jsonb or (v_obs -> 'trusted') is distinct from 'true'::jsonb or (v_obs ->> 'kind') is distinct from 'single' then
    raise exception 'ne_not_trusted: 照合の回 % の % の観測が NE にある・信用できる単品でない', v_run, v_s.code using errcode = 'P0001';
  end if;
  v_par := v_obs -> 'cols' -> 'parent';
  if (v_par ->> 'st') is distinct from 'ok' then raise exception 'ne_parent_unreadable: NE の % の代表が読めない (%)', v_s.code, coalesce(v_par ->> 'st', 'なし') using errcode = 'P0001'; end if;
  if pg_catalog.jsonb_typeof(v_par -> 'v') is distinct from 'string' or coalesce(v_par ->> 'v', '') = '' then
    raise exception 'ne_no_parent: NE の % に代表が無い (採用するものが無い)', v_s.code using errcode = 'P0001';
  end if;
  v_rnorm := core.norm_code(v_par ->> 'v');
  if v_rnorm = v_s.code_norm then raise exception 'ne_no_parent: NE の % の代表は自分自身 (代表なしと同じ)', v_s.code using errcode = 'P0001'; end if;
  select c.ne_code into v_rcode from ops.master_ne_codes c where c.code_norm = v_rnorm and c.kind in ('rep', 'product') and c.state = 'ok'
   order by (c.kind = 'rep') desc limit 1;
  if v_rcode is null then raise exception 'ne_parent_spelling: NE の代表 % の元の書き方が確かめられない (0041)', v_rnorm using errcode = 'P0001'; end if;
  perform ops.variation_lock_code(v_rnorm);
  -- まとまり: 予約 → 今ある札・単品 → 無ければ作る
  select r.group_product_id into v_gid from ops.variation_group_codes r where r.company_id = 1 and r.code_norm = v_rnorm;
  if v_gid is null then
    select pg_catalog.count(*), pg_catalog.min(x.pid) into v_n, v_gid from (
      select p.product_id as pid from core.products p where p.company_id = 1 and core.norm_code(p.display_code) = v_rnorm
         and not exists (select 1 from core.skus k where k.product_id = p.product_id)
      union
      select k.product_id from core.skus k where k.company_id = 1 and k.code_norm = v_rnorm) x;
    if v_n > 1 then raise exception 'group_ambiguous: NE の代表 % が Company DB のまとまり 2 つ以上に当たる', v_rcode using errcode = 'P0001'; end if;
    if v_n = 0 then
      v_gid := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('core.products', 'product_id')::regclass);
      v_created := true;
    end if;
  end if;
  if v_gid is null then raise exception 'group_ambiguous: NE の代表 % のまとまりが決まらない', v_rcode using errcode = 'P0001'; end if;
  if v_gid = v_s.product_id then raise exception 'ne_no_parent: NE の % の代表は自分自身', v_s.code using errcode = 'P0001'; end if;
  perform ops.variation_lock_group(v_gid);
  perform ops._open_variation_write('parent_adopt_ne', p_request_id, p_actor_id, p_reason, p_ownership, p_sku_id,
    case when v_created then array[v_s.product_id, v_gid] else array[v_s.product_id] end,
    v_hash,
    pg_catalog.jsonb_build_object('group_product_id', v_gid::text));
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  if v_created then
    v_gname := pg_catalog.left(coalesce(nullif(pg_catalog.btrim(pg_catalog.split_part(v_s.name, '【', 1)), ''), v_rcode), 255);
    insert into core.products (product_id, company_id, display_code, name, status, created_by_type, created_by_id)
      overriding system value values (v_gid, 1, v_rcode, v_gname, 'active', 'human', p_actor_id);
    insert into ops.variation_group_codes (company_id, code_norm, code, group_product_id, source, reserved_by) values (1, v_rnorm, v_rcode, v_gid, 'ne_adopt', p_actor_id);
    insert into ops.variation_group_revisions (group_product_id, company_id, updated_by) values (v_gid, 1, p_actor_id);
  else
    perform ops._variation_group_resolve(v_gid, p_actor_id, 'load', true);
  end if;
  perform pg_catalog.set_config('core.parent_protocol', '1', true);
  update core.products set parent_product_id = v_gid, parent_set_by = 'manual' where product_id = v_s.product_id;
  perform pg_catalog.set_config('core.parent_protocol', '', true);
  perform pg_catalog.set_config('ops.variation_protocol', '1', true);
  insert into ops.variation_parent_adoptions (sku_id, company_id, product_id, group_product_id, compare_run_id, ne_rep_code, group_created, adopted_by, request_id)
    values (p_sku_id, 1, v_s.product_id, v_gid, v_run, v_rcode, v_created, p_actor_id, p_request_id);
  v_bump := ops._variation_bump(v_gid, p_request_id, p_actor_id);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', v_s.code, 'sku_id', p_sku_id::text, 'group_product_id', v_gid::text,
    'group_code', v_rcode, 'group_created', v_created, 'compare_run_id', v_run, 'revision', v_bump -> 'revision', 'event_id', v_bump -> 'event_id');
  perform pg_catalog.set_config('ops.variation_protocol', '', true);
  perform ops.close_reg_write(v_result, v_s.code);
  return v_result;
end $$;
revoke all on function ops.adopt_ne_parent_for_quarantined(uuid, text, text, jsonb, bigint) from public;

-- ═══════════ 9. 知らせの保険 (0066 の置き換え): まとまりの知らせ = まとまりの約束の中で・約束のまとまり・request_id・人だけ ═══════════
create or replace function ops.guard_product_hub_outbox_session() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions;
begin
  if v_db_user is distinct from 'master_edit' then return new; end if;
  v_sess := ops.current_master_write_session();
  if new.group_product_id is not null then
    -- 🆕 0067: まとまりの約束 (まとめての登録を閉じる・子の廃止・名前を直す・代表の採用) の中で、約束のまとまり・request_id・作った人だけ
    if v_sess.session_id is null or v_sess.operation not in ('variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne')
       or (v_sess.versions ->> 'group_product_id') is distinct from new.group_product_id::text
       or new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then
      raise exception 'group_snapshot_session_required: まとまりの知らせは、まとまりの約束の関数の中で、約束のまとまり・request_id・人だけ' using errcode = '42501';
    end if;
    return new;
  end if;
  if v_sess.session_id is null or v_sess.operation is distinct from 'sku_create' then
    raise exception 'master_write_session_required: カードの知らせは、登録の約束 (ops.register_new_sku) の中だけで書く' using errcode = '42501';
  end if;
  if new.sku_id is distinct from v_sess.sku_id or new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then
    raise exception 'master_write_session_mismatch: カードの知らせの SKU・request_id・作った人が登録の約束と違う' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_product_hub_outbox_session() from public;

-- ═══════════ 10. NE 登録の CSV のまとまりの版 (ne-reg-variation-v1) を開く ═══════════
-- 10a. 形の版の決まり (0065 の置き換え): ne-reg-variation-v1 = 作れる版 (ほかは 0065 と同じ)
create or replace function ops.ne_reg_schema_rule(p_schema text) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case p_schema
    when 'ne-reg-single-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": false, "why": "0065 で引退 (JAN を送る版)。単品は ne-reg-single-v2", "trial_gate": true, "jan": "value", "parent": "ne_code"}'::jsonb
    when 'ne-reg-single-v2' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "ne_code"}'::jsonb
    when 'ne-reg-variation-v1' then '{"kind": "products", "sku_kind": "single", "header": "syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code",
      "buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "group"}'::jsonb
    when 'ne-reg-set-v1' then '{"kind": "sets", "sku_kind": "set", "header": "set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo",
      "buildable": true, "why": null, "trial_gate": true, "jan": null, "parent": null}'::jsonb
  end
$$;

-- 10b. 1 つの SKU の CSV の行と確かめる値 (0065 の置き換え・引数は同じ)
/**
 * 0065 と同じ決まり (lib/master-reg-csv.mjs の regMaterialOf と同じ) で、変えたのは代表 (親) の列だけ:
 *   🆕 0067: 親の NE の元の書き方 (0041) が無い = ポータルで作った札 (予約 source = portal・同じまとまり) なら予約のコード (打ったとおり) を書く。
 *     NE にあるまとまり = 今どおり NE の書き方・collided / invalid = 止める
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

-- 10c. NE 登録の CSV を作る (0065 の置き換え・引数は同じ)
/**
 * 0065 と同じ (鍵の順・許可・形の版の決まり・NE のコード・大文字のコードの確かめ・行と確かめる値の照らし直し・印とハッシュ・約束) で、変えたのは代表の列の版の決まりだけ:
 *   🆕 0067: まとまりの版 (ops.ne_reg_schema_rule の parent = group = ne-reg-variation-v1) = 全部の品目が同じまとまりの子 (variation_mixed / 子でない = not_ready)・
 *     そのまとまりの NE 登録待ちの子 (下書き / NE 登録待ちで生きているファイルの無い単品) を全部入れる (variation_incomplete = まとまりで 1 ファイル)。
 *     単品の版 (parent = ne_code) = ポータルで作ったまとまり (予約 source = portal) の子は入れない (variation_file_required)
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
  v_vpar     bigint;   -- 🆕 0067: 品目の親
  v_group    bigint;   -- 🆕 0067: まとまりの版のまとまり
  v_left     text;     -- 🆕 0067: まとまりの NE 登録待ちの子でファイルに入っていないもの
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
  select e.export_id, e.kind, e.schema_version, e.header, e.created_by, e.state, e.sha256, e.trial into v_prev from ops.ne_reg_exports e where e.request_id = v_rid;
  if found then
    select pg_catalog.array_agg(distinct (i ->> 'sku_id')::bigint order by (i ->> 'sku_id')::bigint) into v_want from pg_catalog.jsonb_array_elements(p -> 'items') i;
    -- 🆕 0067 (#1677 Codex R1 Medium 1): 形の版と見出しも同じときだけ前のファイル (single-v2 と variation-v1 はどちらも products)
    if v_prev.kind is distinct from v_kind or v_prev.schema_version is distinct from v_schema or v_prev.header is distinct from v_header or v_prev.created_by is distinct from v_actor
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
    -- 🆕 0064: 大文字も (CSV には打ったとおりの書き方を書く・品目の ne_code の CHECK も同じ形)
    if v_sku.code !~ '^[A-Za-z0-9_-]{1,30}$' then raise exception 'not_ready: % は新しいコードの形でない', v_sku.code using errcode = 'P0001'; end if;
    -- 🆕 0067: 代表の列の版の決まり (まとまりの版 = 全部が同じまとまりの子 / 単品の版 = ポータルで作ったまとまりの子は入れない)
    select pp.parent_product_id into v_vpar from core.products pp where pp.product_id = v_sku.product_id;
    if (v_rule ->> 'parent') = 'group' then
      if v_vpar is null then raise exception 'not_ready: % はまとまりの子でない (形の版 % はまとまりの子だけ)', v_sku.code, v_schema using errcode = 'P0001'; end if;
      if v_group is null then v_group := v_vpar;
      elsif v_group <> v_vpar then raise exception 'variation_mixed: 1 つのファイルは 1 つのまとまりの子だけ (%)', v_sku.code using errcode = 'P0001'; end if;
    elsif v_vpar is not null and exists (select 1 from ops.variation_group_codes g where g.group_product_id = v_vpar and g.source = 'portal') then
      raise exception 'variation_file_required: % はポータルで作ったまとまりの子 = ne-reg-variation-v1 のファイルで作る', v_sku.code using errcode = 'P0001';
    end if;
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
  -- 🆕 0067: まとまりの版 = そのまとまりの NE 登録待ちの子 (下書き / NE 登録待ち・生きているファイルなし) を全部入れる (まとまりで 1 ファイル)
  if v_group is not null then
    select pg_catalog.string_agg(k.code, '・' order by k.code_norm) into v_left
      from core.products cp join core.skus k on k.product_id = cp.product_id and k.sku_kind = 'single' join ops.master_registrations mr on mr.sku_id = k.sku_id
     where cp.parent_product_id = v_group and mr.state in ('draft', 'ne_pending')
       and not exists (select 1 from ops.ne_reg_export_items x where x.sku_id = k.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial'))
       and not (k.sku_id = any (v_ids));
    if v_left is not null then
      raise exception 'variation_incomplete: まとまりの NE 登録待ちの子 (%) もこのファイルに入れる (まとまりで 1 ファイル)', v_left using errcode = 'P0001';
    end if;
  end if;
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

-- ═══════════ 11. 照合 ② の確かめ待ちに、採用を待つ quarantined を足す (products.parent が company のときだけ) ═══════════
create or replace view ops.v_ne_reg_targets as
  select i.item_id, i.export_id, i.sku_id, i.code_norm, i.sku_kind, i.state
    from ops.ne_reg_export_items i
   where i.state in ('issued', 'import_declared', 'partial')
  union all
  -- 🆕 0067: NE で直接作られた商品 (quarantined) で、親が無く、まだ代表を採用していない単品 = 翌朝の照合が NE の観測を残す (代表の採用の根拠)
  select null::bigint, null::bigint, s.sku_id, s.code_norm, s.sku_kind, 'quarantined'::text
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id join core.products p on p.product_id = s.product_id
   where r.state = 'quarantined' and s.sku_kind = 'single' and p.parent_product_id is null
     and not exists (select 1 from ops.variation_parent_adoptions a where a.sku_id = s.sku_id)
     -- 🚨 関数 (ops.variation_parent_company) を view に書かない = 読み手 (watcher) に実行権が要る。表は view の持ち主の権限で読む
     and coalesce((select m.active_map ->> 'products.parent' from ops.master_ownership_state m where m.id = 1), 'load') = 'company';
comment on view ops.v_ne_reg_targets is '翌朝の照合 ② が NE の完全な取得の値を送る商品 (0053・0063 から配っただけの品目も・0067 から products.parent が company のとき代表を採用する前の quarantined の単品も)';

-- ═══════════ 12. 権限 ═══════════
-- create or replace は今の権限を残す (ne_reg_build = 画面のロール・ne_reg_canonical / schema_rule = だれにも)。新しい関数は public から外した (上)。
-- 画面のロールの実行 (5 つの関数と確かめの 4 つ)・まとまりの表の読み取りは scripts/company-db/create-master-edit-roles.mjs (流し直し)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.variation_group_codes, core.variation_axes, core.variation_options, core.sku_variation_choices, ops.variation_group_revisions,
      ops.variation_batches, ops.variation_parent_adoptions to watcher';
  end if;
end $$;
