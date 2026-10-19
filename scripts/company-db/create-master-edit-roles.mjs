#!/usr/bin/env node
/**
 * create-master-edit-roles.mjs — マスタの入力の仕組みのロールを作る / 権限をそろえる (Company DB構想 14 ⑤-1・PR #1563 R1 M8・R2 H2 / M3 / M4・仮レビュー Low 2 / Low 3 / Low 4)
 *   create-watch-roles.mjs と同じ作り = 1 取引・作った後に属性を確かめて違えば取り消す。流し直すと、前に付けた権限を外してから付け直す
 *
 * master_edit     (Render の apps/master-edit が env COMPANY_DB_MASTER_EDIT_URL で使う。無ければ画面は見るだけ):
 *   読む: 画面が読む表だけ (MASTER_EDIT_SELECT) / 書く: 保存の経路で書く表の列だけ (MASTER_EDIT_WRITE。insert も列を絞る)
 *   🚨 書けるのは同じ取引で ops.begin_master_write (実行だけ渡す) を呼んだ後・その約束の相手 (SKU・含むセット・商品)・操作の書き方だけ (DB の trigger。段階・持ち主表・版・誰が を DB で守る・#1563 R3 M2・R4 M2)
 *   🚨 渡さない: events.master_change_events の insert (記録は 0026 の関数 = security definer が書く = 偽れない)・core.skus.version・core.sku_components の書き込み・
 *      切替の段階・門の記録・NE の観測の書き込み。core.sku_costs の削除は渡すが、今日より前の行は DB の trigger が拒む (0051)
 *      core.suppliers の update (仕入先の行の共有の鍵は 0051 の関数 core.lock_suppliers_for_share = security definer の実行だけ)
 *   ロールの設定 = statement_timeout 20s・lock_timeout 10s・idle_in_transaction_session_timeout 60s (画面の接続の設定と同じ。画面が付け忘れても長く持たない)
 *   ⑤-2a (0052・新商品の登録 lib/master-register.mjs・カードの知らせ lib/product-hub-outbox.mjs): 登録の関数 (ops.register_new_sku = 番号 → 登録の約束 →
 *   商品・SKU・状態・仕入先・原価・構成の依頼・知らせ → done を関数の中で)・新しいコードの決まり (ops.new_sku_code_problem)・SKU を足した commit の確かめ
 *   (ops.sku_registration_problem = 遅らせた trigger が呼び手の権限で呼ぶ)・知らせを借りる / 結果を書く関数 (ops.claim_card_events / finish_card_event) の実行。
 *   🚨 渡さない: core.products / core.skus の insert・知らせの insert (登録の関数だけが書く)・ops.create_sku_registration の実行 (登録の関数の中だけ)・
 *      登録の状態・履歴・backfill の印の表の書き込み (関数だけ)・知らせの状態 / 結果 / 借りの列の update (関数だけ・仮レビュー L7)
 *   ⑤-2b (0053・新商品の NE 登録の CSV lib/master-reg-csv.mjs・JAN lib/master-write.mjs): 新規登録の CSV の表・実機の確かめ・仕入先の登録の状態を読む /
 *   新規登録の CSV の関数 (ops.ne_reg_build / issue / declare / supersede / record_verified = 約束 reg_csv_*)・保存の中の ops.ne_reg_guard_on_save・
 *   JAN の関数 (ops.edit_sku_jan = 約束 jan_edit)・NE の元のコードを要る分だけ読む関数 (ops.ne_reg_ne_codes) の実行。
 *   🚨 渡さない: 新規登録の CSV の表・core.external_ids・仕入先の登録の状態の表の書き込み (関数だけ)・NE の元のコードの表の select・
 *      仕入先の関数 (ops.create_supplier ほか = 書くロールは ⑥ で決める)・照合の確かめの 3 つ (watch_writer だけ = create-watch-roles.mjs)・関数の中の部品
 *   ⑦-1 (0054・Amazon SKU の対応): 保存と削除の関数 (ops.save_amazon_sku_map / ops.delete_amazon_sku_map = 出品・対応・構成・保存の記録 done を関数の中で)・
 *   「未登録」の一覧と売上の公開のそろいの関数 (注文の表は読ませない) の実行と、対応の表・ASIN (catalog_items) を読むだけ。
 *   🚨 渡さない: core.amazon_sku_maps / core.listings / core.listing_components の書き込み (DELETE も = 墓標は消さない)・関数の中の部品 (ops.amazon_map_begin ほか)
 *   FBA の在庫 (10/5・apps/master-edit/fba-stock.mjs): snapshots の usage と、在庫の日次の 2 つの表 (FBA_STOCK_SELECT) の select だけ。
 *   🚨 mart (mart.v_sku_stock) は渡さない: 取得の時刻と内訳が無い・mart の usage を渡すと mart の関数も呼べる範囲が広がる。
 *      snapshots の関数 = 月の分割を作る 2 つ (呼び手の権限 = schema の CREATE が要る = master_edit では何もできない) と trigger の関数だけ
 * master_ops      (手の操作 scripts/company-db/master-cutover.mjs が env COMPANY_DB_MASTER_OPS_URL で使う): ops.set_master_cutover_phase の実行と、段階・記録を読むだけ。
 *   ⑤-2a: 切替の日の backfill (ops.registration_backfill_plan / backfill_sku_registrations) と、登録をやめる (ops.transition_sku_registration) の実行・登録の状態を読む
 * master_observer (⑤-2 の夜間ロードが NE のセットの構成の観測を書く env COMPANY_DB_MASTER_OBSERVER_URL): ops.record_ne_set_observations の実行だけ
 * master_gate     (まとめのロール・ログインできない): ops.record_legacy_gate_ack の実行と、段階・列ごとの持ち主 (0055 の ops.master_ownership_state・⑤-3b) を読むだけ。ログインは場所ごとのメンバー (#1563 仮レビュー Low 3):
 *   master_gate_render (Render の ⑤-3 の古い入口の門 env COMPANY_DB_MASTER_GATE_RENDER_URL) / master_gate_minipc (miniPC の門 env COMPANY_DB_MASTER_GATE_MINIPC_URL)。
 *   DB の関数がログインのロールと記録の場所 (host) を照らす = Render のログインで minipc を名乗れない
 * 構成の依頼を上げる (promoteComponentRequest) のは夜間ロード = 表の持ち主のロール (COMPANY_DB_URL)
 * 🆕 0058 (広げる道 PR-1): new_entry_gate (NOINHERIT のログイン・開く前のゲート env COMPANY_DB_NEW_ENTRY_GATE_URL = miniPC の .env・中原さんが入れる):
 *   新商品の開放の許可を出す / 取り消す (ops.grant_new_entry_lease / revoke_new_entry_lease) と、取り消した後に閉じたことを確かめる ops.new_entry_lease_valid の実行だけ。
 *   表は読まない (ゲートの診断の表の読み取りは watcher)。
 *   master_edit には active の持ち主表 (ops.master_ownership_active_map)・許可の表示用 (ops.new_entry_lease_valid)・鍵の入口 (ops.acquire_new_entry_locks)・
 *   配ったファイル (ops.ne_reg_file)、master_gate には門の記録の 2 版の実行を足す。
 *   広げる道の DB の持ち主の関数 (prepare / widen / cancel / 停止 / 保守の印 / private の _ の関数) はだれにも渡さない
 * 🆕 0066 (Company DB構想 20 の PR-3・知らせの名前空間): master_edit に名前空間つきで借りる関数 (ops.claim_outbox_events) の実行を足す。
 *   まとまりの知らせを書く部品 (ops.enqueue_group_snapshot)・形 / 値の確かめ・insert の trigger はだれにも渡さない
 * 🆕 0071 (新商品の下書きを消す): master_edit に ops.cancel_sku_draft と確かめ ops.sku_draft_cancel_check の実行 (DRAFT_CANCEL_FUNCTIONS)。表の権限は足さない (関数の中だけで書く)
 * 🆕 0074 (Amazon の出品一覧・まだつないでいない SKU): master_edit に 3 つの表の select (AMAZON_LISTING_SELECT) と、隠す・最後に売れた日の関数の実行 (AMAZON_LISTING_EDIT_FUNCTIONS)
 * 🆕 0075 (発注の設定の知らせ): master_edit に知らせの表と view の select (ORDER_OUTBOX_SELECT) と、積む・借りる・結果を書く・閉じる の実行 (ORDER_OUTBOX_EDIT_FUNCTIONS)
 * 🆕 0067 (Company DB構想 20 の PR-5・まとまりの DB): master_edit にまとまりの 5 つの関数 (まとめての登録を開く / 閉じる・子の廃止・名前を直す・
 *   quarantined の代表の採用) と確かめの 4 つ (新しいまとまりのコードの決まり・products.parent が company か・子の request_id・子の数の上限) の実行と、
 *   まとまりの表 (予約・軸・選択肢・子の選択肢・revision・まとめての登録・採用の記録) の読み取りを足す (表が無い DB = 0067 の前は付けない = 流し直すと付く)。
 *   🚨 渡さない: まとまりの表の書き込み (関数だけ)・関数の中の部品 (鍵・スナップショット・revision・約束・予約の事前検査と入れる関数 = DB の持ち主だけ)
 * 🆕 0072 (NE から消す CSV): master_edit に確かめ・作る・受け取る・取り下げ・取扱中止・一覧の 8 つの実行 (NE_DEL_FUNCTIONS)。表の権限は足さない (関数の中だけで書く・
 *   ファイルのバイト列は ops.ne_del_download だけ)。🚨 渡さない: 表の守り・行き先・CSV のバイト列を作る部品・参照の守りの部品 (NE_DEL_OWNER_ONLY_FUNCTIONS)
 * 🆕 0070 (NE から消えた商品をやめる): master_edit と master_ops に ops.cancel_sku_gone_from_ne と確かめの 2 つ (ops.sku_gone_from_ne_check / _list) の実行
 *   (GONE_FUNCTIONS)。🚨 渡さない: ops.master_ne_absence の表・照合 ② の記録の中の部品 (ops._record_ne_absence ほか = GONE_OWNER_ONLY_FUNCTIONS)
 * 🆕 0079 (要確認を確認済みにする・Company DB構想 22 の Q-1): master_edit に確認の関数と確かめ・一覧 (読むだけ) の実行 (QUARANTINE_CONFIRM_FUNCTIONS・
 *   名簿 MASTER_EDITORS の人 = 中原さんの決定 Q1 a)。master_ops には渡さない。表の権限は足さない (関数の中だけで書く)。
 *   🚨 渡さない: 確かめの表の 1 行を作る部品 (QUARANTINE_CONFIRM_OWNER_ONLY_FUNCTIONS)
 * 🆕 0084 (メーカー・中原さん 2026-10-17): master_edit に名簿・商品ごとのメーカー・記録の 3 つの表の select (MAKER_SELECT) と、名簿に足す・商品に付ける の 2 つの関数と
 *   名前の形の 2 つ (掃除・同じと見る形 = 読むだけ) の実行 (MAKER_EDIT_FUNCTIONS)。表が無い DB (0084 の前) では付けない (流し直すと付く = それまで画面は欄を出さない)。
 *   🚨 渡さない: 3 つの表の insert / update / delete (関数だけが書く)・守りの trigger・名前の問題の部品 (MAKER_OWNER_ONLY_FUNCTIONS)
 * 🆕 0085 (商品ごとの入出庫履歴と月別のまとめ・CompanyDB構想 26): master_edit に読むだけの関数 4 つの実行 (MOVEMENT_FUNCTIONS)。出荷の表の select は渡さない
 * 🆕 0086 (販売数のグラフ・10/17 中原さん): master_edit に読むだけの関数 1 つの実行 (SALES_CHART_FUNCTIONS)。注文の表の select は渡さない
 * 🆕 0088 (ブランド・商品の段階・中原さん 2026-10-17): master_edit にブランドの名簿・商品ごとのブランド・商品の段階と記録の表の select (BRAND_LIFE_SELECT) と、
 *   名簿に足す・自社ブランドの印・商品に付ける (ブランド・段階) の 4 つの関数と、初めて売った日・記録の始まりを読む 2 つ (注文・出荷の表は読ませない) の実行 (BRAND_LIFE_EDIT_FUNCTIONS)。
 *   表が無い DB (0088 の前) では付けない (流し直すと付く = それまで画面は欄を出さない)。
 *   🚨 渡さない: 表の insert / update / delete (関数だけが書く)・要求の控え ops.brand_requests の select・守りの trigger・商品の行を確かめる部品 (BRAND_LIFE_OWNER_ONLY_FUNCTIONS)
 *
 * 🚨 パスワード (#1563 仮レビュー Low 2): 流し直しても、もうあるロールのパスワードは変えない (門のロールは Render と miniPC の両方が使う =
 *    黙って変えると門の記録が書けなくなり、切替が進められない)。パスワードを付けるのは、ロールを初めて作るときと --rotate-password <ロール> を付けたときだけ
 *    (付けたロールの新しい接続文字列だけを出す。変えたら、そのロールを使う場所の env を同じ日に書き換える)
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --dry-run   # 流す文だけ見る
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs             # 作る / 権限をそろえる (新しいロールのパスワードだけ、この画面に出る)
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --rotate-password master_gate_render   # そのロールのパスワードだけ変える (何回でも付けられる)
 * 🚨 0054 まで migration を流した後に (⑤-2b・⑦-1 の表・関数が無いと止まる)。migration で表を足したら流し直す (権限は表ごとに付ける)
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';
import { urlFor } from './create-watch-roles.mjs';

/** 場所ごとの門のログイン (DB の関数 ops.record_legacy_gate_ack は session_user = 'master_gate_' || host を求める) */
export const GATE_LOGIN_ROLES = Object.freeze({ render: 'master_gate_render', minipc: 'master_gate_minipc' });
/** ログインできるロール (パスワードを持つ) */
export const MASTER_LOGIN_ROLES = Object.freeze(['master_edit', 'master_gate_minipc', 'master_gate_render', 'master_observer', 'master_ops', 'new_entry_gate']);
/** まとめのロール (ログインできない・パスワードなし) */
export const MASTER_GROUP_ROLES = Object.freeze(['master_gate']);
export const MASTER_EDIT_ROLES = Object.freeze([...MASTER_LOGIN_ROLES, ...MASTER_GROUP_ROLES].sort());
const CONN_LIMIT = { master_edit: 5, master_ops: 2, master_observer: 2, master_gate_render: 8, master_gate_minipc: 8, new_entry_gate: 2 };   // 門: プロセス・CLI が同時に記録を書く (⑤-3)
/** まとめのロールのメンバー (INHERIT = まとめのロールの権限をそのまま使う) */
const MEMBER_OF = { master_gate_render: 'master_gate', master_gate_minipc: 'master_gate' };
/** ロールごとの設定 (master_edit = 画面の接続と同じ値。apps/master-edit/router.mjs の connect) */
const ROLE_SETTINGS = {
  master_edit: { statement_timeout: '20s', lock_timeout: '10s', idle_in_transaction_session_timeout: '60s' },
  master_gate_render: { statement_timeout: '20s' }, master_gate_minipc: { statement_timeout: '20s' },
  master_observer: { statement_timeout: '20s' }, master_ops: { statement_timeout: '20s' }, new_entry_gate: { statement_timeout: '20s' },
};
/** 画面が読む表 */
export const MASTER_EDIT_SELECT = [
  'core.skus', 'core.products', 'core.sku_components', 'core.sku_costs', 'core.suppliers', 'core.supplier_skus', 'core.external_ids', 'core.listings', 'core.listing_components',
  'events.master_change_events',
  'ops.master_cutover_state', 'ops.master_edit_requests', 'ops.sku_component_requests', 'ops.sku_component_breaches', 'ops.ne_set_observations', 'ops.ne_set_observation_runs',
  'ops.ne_csv_exports', 'ops.ne_csv_export_rows', 'ops.master_decision_candidates', 'ops.master_decision_observations', 'ops.master_compare_runs',
  // ⑤-2a (0052): 登録の状態 (画面に出す)・backfill の印 (新商品の登録の前提)・カードの知らせ (読むだけ)
  'ops.master_registrations', 'ops.master_registration_backfill', 'ops.product_hub_outbox',
  // ⑤-2b (0053): 新規登録の CSV (ファイル・試み・商品・行・照合の確かめ)・実機の確かめ・仕入先の登録の状態 (読むだけ。NE の元のコードは関数 ops.ne_reg_ne_codes で)
  'ops.ne_reg_exports', 'ops.ne_reg_attempts', 'ops.ne_reg_export_items', 'ops.ne_reg_export_rows', 'ops.ne_reg_checks', 'ops.ne_csv_verified', 'ops.supplier_registrations',
  // ⑦-1 (0054): Amazon SKU の対応 (読むだけ)・ASIN
  'core.amazon_sku_maps', 'core.catalog_items',
];
/** FBA の在庫 (参考・読むだけ。10/5): 在庫の日次の取れた日と行 (source = fba_jp ほか)。schema snapshots の usage も渡す (この 2 つの表のためだけ) */
export const FBA_STOCK_SELECT = Object.freeze(['snapshots.stock_capture_days', 'snapshots.sku_stock_daily']);
/** 保存の経路で書く表・列 (lib/master-write.mjs の saveSku)。insert も列を絞る */
export const MASTER_EDIT_WRITE = [
  ['update (name, tax_rate, tax_class, handling, standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own)', 'core.skus'],
  // 🆕 0062 (2026-10-08): ロジザードの最初の値 (expiry_managed / inbound_date_managed) は下書きの間だけ (DB の trigger trg_master_edit_logizard_flags が断る)
  ['update (name, status, sales_class, parent_product_id, parent_set_by, expiry_managed, inbound_date_managed)', 'core.products'],
  ['insert (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id), update (is_primary)', 'core.supplier_skus'],
  ['insert (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id), update (valid_to), delete', 'core.sku_costs'],
  ['insert (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, error, started_at)', 'ops.master_edit_requests'],
  ['insert (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id), update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_requests'],
  ['update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_breaches'],
  // ⑤-2a (0052): 新商品の商品・SKU・カードの知らせの insert は渡さない (登録の関数 ops.register_new_sku だけが書く)
];
/** ⑤-2a (0052): 画面のロールが実行する関数 (security definer) */
export const REGISTER_FUNCTION = 'ops.register_new_sku(uuid, text, text, jsonb, text, jsonb)';
export const REGISTER_EDIT_FUNCTIONS = Object.freeze([REGISTER_FUNCTION, 'ops.new_sku_code_problem(text)', 'ops.sku_registration_problem(bigint, text)',
  'ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer)', 'ops.finish_card_event(uuid, text, text, jsonb, text)']);
/** ⑤-2a (0052): 運用のロールが実行する関数 (切替の日の backfill・登録をやめる) と読む表 */
export const REGISTER_OPS_FUNCTIONS = Object.freeze(['ops.registration_backfill_plan()', 'ops.backfill_sku_registrations(integer, text, text, text)',
  'ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text)']);
export const REGISTER_OPS_SELECT = Object.freeze(['ops.master_registrations', 'ops.master_registration_events', 'ops.master_registration_backfill']);
/** ⑤-2a (0052): だれにも渡さない (夜間ロード = 持ち主だけ・状態の行は登録の関数の中だけ) */
export const REGISTER_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.quarantine_unregistered_skus(text)', 'ops.create_sku_registration(bigint, text, text, text)']);
/** ⑤-2b (0053): 画面のロールが実行する関数 (security definer = 約束を書くのも表に書くのも関数の中だけ・#1571 R1 High 3) */
export const REG_CSV_EDIT_FUNCTIONS = Object.freeze(['ops.ne_reg_build(jsonb, bytea)', 'ops.ne_reg_issue(uuid, text, jsonb, bigint)',
  'ops.ne_reg_declare(uuid, text, jsonb, bigint, text, text, text, timestamptz, text)', 'ops.ne_reg_supersede(uuid, text, jsonb, bigint, text, text)',
  'ops.ne_reg_guard_on_save(bigint[], text, text)', 'ops.ne_reg_record_verified(uuid, text, jsonb, text, text, text, text, text, bigint)',
  'ops.edit_sku_jan(uuid, text, text, jsonb, bigint, jsonb, jsonb)', 'ops.ne_reg_ne_codes(text[])']);
/** ⑤-2b (0053): だれにも渡さない (関数の中の部品・trigger・仕入先の関数 = 書くロールは ⑥ で決める)。照合の確かめの 3 つは watch_writer (create-watch-roles.mjs) */
export const REG_CSV_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.ne_reg_lock_skus(bigint[])', 'ops.ne_reg_lock_export(bigint)', 'ops.ne_reg_export_skus(bigint)',
  'ops.ne_reg_canonical(bigint, date)', 'ops.ne_reg_supersede_built(bigint[], text, text)', 'ops.ne_reg_observation_hash(text)',
  'ops.reg_write_gate(jsonb, text[])', 'ops.open_reg_write(text, uuid, text, text, jsonb, bigint, bigint[], bigint[], text, jsonb)', 'ops.close_reg_write(jsonb, text)', 'ops.reg_hash(jsonb)',
  'ops.guard_reg_csv_write()', 'ops.guard_reg_csv_live()', 'core.guard_master_edit_jan()',
  'ops.create_supplier(uuid, text, text, jsonb, text, text, text, integer)', 'ops.declare_supplier_in_ne(uuid, text, jsonb, text, text, text)',
  'ops.deactivate_supplier(uuid, text, text, jsonb, text, text)',
  'ops.snapshot_ne_reg_targets(text)', 'ops.record_ne_registration_observations(jsonb)', 'ops.seal_ne_registration_run(text, text, text)', 'ops.record_ne_registration_check(text)']);
/** ⑤-2b (0053): 読むだけで、ほかのロールの一覧に入らない表 (流し直しのときに外す) */
export const REG_CSV_OTHER_TABLES = Object.freeze(['ops.supplier_registration_events', 'ops.v_ne_reg_targets', 'ops.ne_reg_compare_targets', 'ops.ne_reg_compare_runs', 'ops.ne_reg_compare_observations',
  'ops.ne_reg_compare_receipts', 'ops.master_ne_codes', 'ops.master_ne_code_mark']);
/** ⑦-1 (0054): 画面のロールが実行する関数 (Amazon SKU の対応の保存・削除・未登録の一覧・売上の公開のそろい) */
export const AMAZON_MAP_EDIT_FUNCTIONS = Object.freeze(['ops.save_amazon_sku_map(uuid, text, text, jsonb, text, jsonb)', 'ops.delete_amazon_sku_map(uuid, text, text, jsonb, text, jsonb)',
  'ops.amazon_map_unmapped_recent(date, integer)', 'ops.amazon_map_sales_coverage(date, integer)']);
/** ⑦-1 (0054): だれにも渡さない (保存の関数の中の部品・不変条件・守り) */
export const AMAZON_MAP_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.amazon_map_begin(text, uuid, text, text, jsonb, text, jsonb)', 'ops.amazon_map_components(jsonb)',
  'core.amazon_map_problem(bigint)', 'core.check_amazon_map_invariant()', 'core.guard_amazon_map_writer()', 'ops.guard_amazon_map_write()', 'ops.guard_master_edit_request_listing()']);
export const CUTOVER_FUNCTION = 'ops.set_master_cutover_phase(text, text, jsonb, text)';
export const OBSERVE_FUNCTION = 'ops.record_ne_set_observations(jsonb)';
export const ACK_FUNCTION = 'ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz, boolean, text)';
export const LOCK_SUPPLIERS_FUNCTION = 'core.lock_suppliers_for_share(bigint[])';
/** 画面の保存を始める (段階・持ち主表を DB で確かめて、取引の行を書く。これの後でないと画面のロールは書けない・#1563 R3 M2) */
export const BEGIN_WRITE_FUNCTION = 'ops.begin_master_write(uuid, text, text, jsonb, text, bigint, text, text, jsonb)';
/** 0058 (広げる道 PR-1): 画面のロールが実行する (表は読ませない = SECURITY DEFINER の関数の EXECUTE だけ・R1 H4)。無い DB (0058 の前) では付けない。
 *   new_entry_lease_valid は画面の表示用の読むだけ (保存の強制は register_new_sku などの DB の関数の中) */
export const WIDEN_EDIT_FUNCTIONS = Object.freeze(['ops.master_ownership_active_map()', 'ops.new_entry_lease_valid(text)', 'ops.ne_reg_file(bigint)', 'ops.acquire_new_entry_locks(text)']);
/** 0058 (§3.9 の 3・R16 H1): ops.ne_reg_exports の file_bytes を owner でない全部のロールから外す (表の SELECT → 列ごと)。「表に GRANT」の後に毎回流す = 流し直しても戻らない */
export const RESTRICT_FILE_BYTES_SQL = "do $$ begin if to_regprocedure('ops.restrict_ne_reg_file_bytes()') is not null then perform ops.restrict_ne_reg_file_bytes(); end if; end $$";
/** 0058: 門の記録の 2 版 (active / prepared を見た・capable)。master_gate に実行だけ */
export const ACK_V2_FUNCTION = 'ops.record_legacy_gate_ack_v2(text, text, text, jsonb, text, text, integer, timestamptz, text, text, text[], boolean, text)';
/** 0058 (§3.7): 開放の許可を出す / 取り消す = NOINHERIT のログイン new_entry_gate だけ (watch_writer には渡さない = 照合のコード 1 本で証拠から開放まで完結しない) */
export const LEASE_GATE_FUNCTIONS = Object.freeze(['ops.grant_new_entry_lease(text, text)', 'ops.revoke_new_entry_lease(text, text)']);
/** 0058 (#1645): new_entry_gate も取り消した後に閉じたことを確かめる (読むだけ)。WIDEN_EDIT_FUNCTIONS の 1 つ = master_edit にも付く */
export const LEASE_VALID_FUNCTION = 'ops.new_entry_lease_valid(text)';
/** 0058: だれにも渡さない (DB の持ち主だけ = prepare / cancel / 停止 / widen / 保守の印 / 判定の本体 / private の _ の関数 / trigger と部品) */
export const WIDEN_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.prepare_master_widen(integer, jsonb, text, jsonb, text)', 'ops.cancel_master_widen(uuid, text)',
  'ops.record_widen_manual_stop(uuid, text, text, text)', 'ops.widen_master_ownership(uuid, integer, text, jsonb)', 'ops._widen_judge(uuid, integer)',
  'ops.begin_master_maintenance(text)', 'ops.master_maintenance_active()', 'ops.stop_new_entry_for_restore(text)',
  'ops._new_entry_lease_ok(text)', 'ops._require_new_entry_lease(text)', 'ops.ne_code_seen(text)', 'ops._new_entry_gate_problems(text, bigint)', 'ops._new_entry_lease_shared_locks()',
  'ops.sku_kind_locked()', 'ops.sku_kind_shape_counts(integer)', 'ops.assert_sku_kind_shape_after_restore()', 'ops.session_is_db_owner()',
  'core.guard_sku_kind_locked()', 'core.check_sku_kind_shape()', 'ops.guard_master_ownership_widen()', 'ops.guard_new_entry_gate_results()', 'ops.restrict_ne_reg_file_bytes()',
  'ops.widen_amazon_map_counts(integer)', 'ops._widen_attempt_common(uuid, timestamptz)', 'ops.amazon_map_migration_window(uuid)']);   // 0059 (Amazon の対応の数 = 判定の本体と widen だけが使う)
/** 🆕 0066 (Company DB構想 20 の PR-3・知らせの名前空間): 画面のロールが実行する = 名前空間つきで借りる (PR-4 の product-hub のまとまりのカードの取り込みが使う)。
 *   無い DB (0066 の前) では付けない (流し直すと付く)。今の取り込み (ops.claim_card_events) は SKU の知らせだけを借りる = 付けなくても今の動きは変わらない */
export const OUTBOX_NS_EDIT_FUNCTIONS = Object.freeze(['ops.claim_outbox_events(text, text, text, uuid, bigint, integer, integer, integer)']);
/** 🆕 0066: だれにも渡さない (まとまりの知らせを書く部品 = PR-5 のまとまりの関数の中だけ・形 / 値の確かめ・insert の trigger) */
export const OUTBOX_NS_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.enqueue_group_snapshot(bigint, integer, jsonb, uuid, text)', 'ops.group_snapshot_shape_problem(jsonb)',
  'ops.group_snapshot_problem(smallint, bigint, integer, jsonb)', 'ops.guard_product_hub_outbox_group()']);
/**
 * 🆕 0071 (新商品の下書きを消す・中原さんの頼み 10/11): 消す関数 (ops.cancel_sku_draft) と確かめ (読むだけ) を master_edit (画面・名簿 MASTER_EDITORS の人) に実行だけ。
 *   無い DB (0071 の前) では付けない (流し直すと付く)
 */
export const DRAFT_CANCEL_FUNCTIONS = Object.freeze(['ops.cancel_sku_draft(uuid, text, text, bigint)', 'ops.sku_draft_cancel_check(bigint)']);
/** 🆕 0067 (Company DB構想 20 の PR-5・まとまり): 画面のロールが実行する (どれも products.parent の DB の active が company のときだけ書く = 今は断る)。無い DB (0067 の前) では付けない */
export const VARIATION_EDIT_FUNCTIONS = Object.freeze(['ops.variation_batch_open(uuid, text, text, jsonb, jsonb)', 'ops.variation_batch_close(uuid, text, jsonb, jsonb)',
  'ops.cancel_variation_child(uuid, text, text, jsonb, bigint)', 'ops.edit_variation_labels(uuid, text, text, jsonb, bigint, integer, jsonb)',
  'ops.adopt_ne_parent_for_quarantined(uuid, text, text, jsonb, bigint)', 'ops.variation_group_code_problem(text)', 'ops.variation_parent_company()',
  'ops.variation_sub_request_id(uuid, text)', 'ops.variation_max_children()',
  // 🆕 0069 (#1679 Codex R1 Medium 1): まとめての登録の要求の全部のハッシュを残す
  'ops.variation_batch_record_request(uuid, text, text)']);
/** 🆕 0067: だれにも渡さない (関数の中の部品・守りの trigger・予約の事前検査と入れる関数 = DB の持ち主だけ) */
export const VARIATION_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.variation_label_problem(text, integer)', 'ops.variation_lock_group(bigint)', 'ops.variation_lock_code(text)',
  'ops.guard_variation_write()', 'ops.check_variation_batch_closed()', 'core.check_parent_one_level()', 'ops.variation_existing_groups()', 'ops.variation_reservation_check()',
  'ops.reserve_existing_variation_groups(text)', 'ops._variation_group_resolve(bigint, text, text, boolean)', 'ops._variation_lock_existing_group(bigint)',
  'ops.variation_group_snapshot(bigint, integer, text)', 'ops._variation_bump(bigint, uuid, text)',
  'ops._open_variation_write(text, uuid, text, text, jsonb, bigint, bigint[], text, jsonb)', 'ops._variation_replay(uuid, text, text, text, bigint, bigint)']);
/** 🆕 0067: 画面のロールが読むまとまりの表 (書き込みは関数だけ)。無い DB では付けない */
export const VARIATION_SELECT = Object.freeze(['ops.variation_group_codes', 'core.variation_axes', 'core.variation_options', 'core.sku_variation_choices',
  'ops.variation_group_revisions', 'ops.variation_batches', 'ops.variation_parent_adoptions', 'ops.variation_batch_requests']);
/**
 * 🆕 0070 (NE から消えた商品をやめる・中原さんの決定 10/11): 動かす関数 (ops.cancel_sku_gone_from_ne) と確かめ (読むだけ) を、
 *   master_edit (画面・名簿 MASTER_DECISION_APPROVERS の人) と master_ops (CLI scripts/company-db/master-gone-from-ne.mjs) に実行だけ。表は読ませない。
 *   無い DB (0070 の前) では付けない (流し直すと付く)
 */
export const GONE_FUNCTIONS = Object.freeze(['ops.cancel_sku_gone_from_ne(uuid, text, text, bigint)', 'ops.sku_gone_from_ne_check(bigint)', 'ops.sku_gone_from_ne_list(text[])']);
/**
 * 🆕 0076 (「NE から消えた」の日数の許可・中原さんの決定 10/13 夜 B): 許可を足す関数は運用のロール master_ops (CLI scripts/company-db/master-gone-day-override.mjs) だけに実行。
 *   画面のロール (master_edit) には渡さない。表は読ませない。無い DB (0076 の前) では付けない (流し直すと付く)
 */
export const GONE_DAY_OVERRIDE_FUNCTIONS = Object.freeze(['ops.add_sku_gone_day_overrides(uuid, text, text, text[], integer, integer)']);
/** 🆕 0076: だれにも渡さない (表の守り・効いている許可を読む部品 = 確かめの中から持ち主の権限で呼ぶ) */
export const GONE_DAY_OVERRIDE_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_sku_gone_day_overrides()', 'ops.sku_gone_day_override_active(bigint)']);
/**
 * 🆕 0079 (要確認を確認済みにする・中原さんの決定 10/13 Q1 a): 確認の関数 (ops.confirm_quarantined_sku) と確かめ・一覧 (読むだけ) を master_edit (画面・名簿 MASTER_EDITORS の人) に実行だけ。
 *   master_ops (CLI) には渡さない。watcher の確かめ・一覧は migration と create-watch-roles.mjs が付ける。無い DB (0079 の前) では付けない (流し直すと付く)
 */
export const QUARANTINE_CONFIRM_FUNCTIONS = Object.freeze(['ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'ops.quarantine_confirm_check(bigint)',
  'ops.quarantine_confirm_list(text[])']);
/** 🆕 0079: だれにも渡さない (確かめの表の 1 行を作る部品 = 確かめの中から持ち主の権限で呼ぶ) */
export const QUARANTINE_CONFIRM_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.quarantine_confirm_item(text, boolean, jsonb, jsonb, text, text)',
  'ops.record_master_decision_approval_print()', 'ops.guard_master_decision_approval_prints()']);   // #1715 Codex R3: 承認の時の組の trigger と守り
/** 🆕 0070: だれにも渡さない (照合 ② の記録の中の部品・表の守り・定数) */
export const GONE_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops._record_ne_absence(bigint, jsonb)', 'ops.guard_master_ne_absence()', 'ops.ne_gone_required_runs()', 'ops.ne_gone_fresh_hours()',
  'ops.sku_ref_cancelled(bigint[])', 'ops.guard_sku_ref_not_cancelled()']);   // #1683 R1 High 2: やめた SKU への参照を断る trigger とその部品
/**
 * 🆕 0072 (NE から消す CSV・中原さんの決定 10/12): 画面 (master_edit・名簿 MASTER_DECISION_APPROVERS の人) に実行だけ。無い DB (0072 の前) では付けない (流し直すと付く)。
 *   master_ops (CLI) には渡さない (NE から消す CSV は画面だけ・「やめる」は 0070 の GONE_FUNCTIONS のまま)
 */
export const NE_DEL_FUNCTIONS = Object.freeze(['ops.ne_del_check(bigint, bigint[], jsonb)', 'ops.ne_del_check_many(bigint[], jsonb)', 'ops.ne_del_build(uuid, text, text, bigint[], jsonb)',
  'ops.ne_del_download(text, bigint)', 'ops.ne_del_withdraw(uuid, text, text, bigint)', 'ops.ne_del_refused_check(bigint)', 'ops.ne_handling_build(uuid, text, text, bigint[])',
  'ops.ne_del_item_status(bigint)', 'ops.ne_del_overview(bigint)']);
/** 🆕 0072: だれにも渡さない (表の守り・行き先・理由の部品・在庫の鍵・CSV のバイト列・参照の守りの部品) */
export const NE_DEL_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_ne_del_write()', 'ops.ne_del_item_transition_allowed(text, text)', 'ops._ne_del_problem(text, text)',
  'ops.ne_del_app_stock_keys()', 'ops.ne_del_csv_bytes(text[])', 'ops.ne_handling_csv_bytes(text[])', 'ops.sku_ref_ne_deleting(bigint[])', 'ops.restrict_ne_del_file_bytes()']);

/**
 * 🆕 0074 (Amazon の出品一覧と「まだつないでいない SKU」・中原さんの頼み 10/12): 画面のロール master_edit が読む表 (出品一覧の今の姿・見出し・隠す印) と、
 *   実行する関数 (隠す / 元に戻す = security definer・最後に売れた日 = 注文の表は読ませない)。表・関数が無い DB (0074 の前) では付けない (流し直すと付く)。
 *   🚨 渡さない: 出品一覧の表の書き込み (受け口 = 持ち主のロールだけ)・隠す印の表の書き込みと記録の表 (関数だけが書く)
 */
export const AMAZON_LISTING_SELECT = Object.freeze(['ops.amazon_listing_report', 'ops.amazon_listing_report_loads', 'ops.amazon_sku_hidden']);
export const AMAZON_LISTING_EDIT_FUNCTIONS = Object.freeze(['ops.set_amazon_sku_hidden(text, text, boolean, text, text)', 'ops.amazon_sku_last_sold(text[], date)']);
/**
 * 🆕 0075 (CompanyDB構想 21 C1・新商品の登録の発注の設定の知らせ): master_edit に知らせの表の select (画面の上の件数・届け待ちの画面) と、
 *   積む (登録と同じ取引の中だけ = DB が確かめる)・借りる・結果を書く・人が閉じる の 4 つの実行。表が無い DB (0075 の前) では付けない (流し直すと付く)。
 *   🚨 渡さない: 知らせの表の insert / update / delete (関数だけが書く)・中身の形の確かめの部品・守りの trigger
 */
export const ORDER_OUTBOX_SELECT = Object.freeze(['ops.order_settings_outbox', 'ops.order_settings_registrations', 'ops.v_order_settings_outbox_open']);
export const ORDER_OUTBOX_EDIT_FUNCTIONS = Object.freeze(['ops.enqueue_order_settings(uuid, text, jsonb)',
  'ops.claim_order_settings_events(text, text, uuid, uuid, integer, integer, integer, uuid[])', 'ops.finish_order_settings_event(uuid, text, text, jsonb, text)',
  'ops.dismiss_order_settings_event(uuid, text, text)']);
// 🆕 0089: ops.order_settings_payload_versions() = v2 の印 (アプリは「あるか」だけを見る = 実行権は持ち主だけ)
export const ORDER_OUTBOX_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.order_settings_payload_problem(jsonb)', 'ops.guard_order_settings_outbox()', 'ops.order_settings_payload_versions()']);
/**
 * 🆕 0087 (梱包の 縦・横・高さ・重さ・中原さん 2026-10-17): master_edit に人の値・記録の表とまとめた値のビューの select (PACKAGE_SELECT)・
 *   出どころの値 (夜間ロードが書く core.product_physicals) の select (PACKAGE_SOURCE_SELECT・ほかのロールの権限は触らない)・人の値を書く関数の実行 (PACKAGE_EDIT_FUNCTIONS)。
 *   表・関数が無い DB (0087 の前) では付けない (流し直すと付く = それまで画面は欄を出さない)。
 *   🚨 渡さない: 表の insert / update / delete (関数だけが書く)・守りの trigger (PACKAGE_OWNER_ONLY_FUNCTIONS)
 */
export const PACKAGE_SELECT = Object.freeze(['core.product_packages', 'ops.product_package_events', 'core.v_product_packages']);
export const PACKAGE_SOURCE_SELECT = Object.freeze(['core.product_physicals']);
export const PACKAGE_EDIT_FUNCTIONS = Object.freeze(['ops.set_product_package(uuid, text, bigint, integer, integer, integer, integer, bigint, text, text)']);
export const PACKAGE_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_package_tables()']);   // core.package_source_rank はビューの中で読む人の権限で呼ばれる = 外さない
/**
 * 🆕 0078 (ロジザードに送るバーコードの決め方・CompanyDB構想 23・中原さんの決定 10/13 夜): master_edit に決め方の表の select (商品の画面) と、
 *   決め方を書く関数の実行 (新商品の登録の取引の中・商品の画面)。表・関数が無い DB (0078 の前) では付けない (流し直すと付く)。
 *   🚨 渡さない: 表の insert / update / delete (関数だけが書く)・守りの trigger
 */
// 🆕 0080 (#1719 Codex R5): 答えの控え (ops.sku_logizard_barcode_requests) も読むだけ = 商品の画面の再送を見分ける (書くのは関数だけ)
// 🆕 0081: ロジザード登録の CSV の帳面 (ops.lz_new_exports / _items) も読むだけ (画面・ダウンロード・商品の画面の「CSV に出した」)
export const LZ_BARCODE_SELECT = Object.freeze(['ops.sku_logizard_barcode_rules', 'ops.sku_logizard_barcode_requests', 'ops.lz_new_exports', 'ops.lz_new_export_items']);
/**
 * 🆕 0080 (CompanyDB構想 23 v4・中原さん 10/13 夜「新商品登録の画面で入れてすぐにロジザードと NE に登録する」): 値を書く・
 *   値の持ち主 (画面のその場の確かめ・読むだけ) を master_edit に (ロジザード登録の CSV の関数は 2 本目)。0078 の「決め方だけ」の関数は画面から使わない = 持ち主だけ (下の OWNER_ONLY)。
 *   関数が無い DB (0080 の前) では付けない (流し直すと付く)
 */
// 🆕 #1719 R8: ops.lzbc_value_clash = 今の値の重なり (商品の画面の印・読むだけ)
export const LZ_BARCODE_EDIT_FUNCTIONS = Object.freeze(['ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone)', 'ops.lzbc_value_holder(bigint, text)',
  'ops.lzbc_value_clash(bigint, text)',
  // 🆕 0081: ロジザード登録の CSV を出す・取り消す (書くのは関数だけ)
  'ops.lz_new_export_issue(uuid, text, jsonb, timestamp with time zone, timestamp with time zone)', 'ops.lz_new_export_void(text, bigint, text)']);
export const LZ_BARCODE_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_sku_logizard_barcode_rules()', 'ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint)',
  'ops.lzbc_jan_ok(text)', 'ops.guard_sku_logizard_barcode_requests()', 'ops.guard_lz_new_exports()']);
/** 🆕 0084 (メーカー): 画面が読む表・画面が実行する関数・持ち主だけの部品 */
export const MAKER_SELECT = Object.freeze(['core.makers', 'core.product_makers', 'ops.product_maker_events']);
export const MAKER_EDIT_FUNCTIONS = Object.freeze(['ops.create_maker(uuid, text, text)', 'ops.set_product_maker(uuid, text, bigint, bigint, bigint, text, text)',
  'core.maker_name_clean(text)', 'core.maker_name_norm(text)']);
export const MAKER_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_maker_tables()', 'ops.maker_name_problem(text)']);
/**
 * 🆕 0085 (商品ごとの入出庫履歴と月別のまとめ・CompanyDB構想 26 の PR-1・PR-2): 読むだけの関数 4 つ (security definer) を master_edit に実行だけ。
 *   出荷の表 (core.shipments ほか)・取得の回の表 (ops.ingest_runs) の select は渡さない (送り状番号などまで読めてしまう)。関数が無い DB (0085 の前) では付けない (流し直すと付く)
 */
export const MOVEMENT_FUNCTIONS = Object.freeze(['ops.sku_stock_anchors(bigint, date)', 'ops.sku_shipment_totals(bigint, timestamp with time zone[])',
  'ops.sku_movement_shipments(bigint, timestamp with time zone, timestamp with time zone, text, integer)', 'ops.sku_shipments_monthly(bigint, date)']);
/**
 * 🆕 0086 (販売数のグラフ・商品の画面): 日 × モールの正味の販売数 (数量だけ・金額なし) を読むだけの関数 (security definer) を master_edit に実行だけ。
 *   注文の表 (core.orders・order_lines) の select は渡さない (注文番号・金額まで読めてしまう)。関数が無い DB (0086 の前) では付けない (流し直すと付く)
 */
export const SALES_CHART_FUNCTIONS = Object.freeze(['ops.sku_sales_daily(bigint, date)']);
/** 🆕 0088 (ブランド・商品の段階): 画面が読む表・画面が実行する関数・持ち主だけの部品 */
export const BRAND_LIFE_SELECT = Object.freeze(['core.brands', 'core.product_brands', 'core.product_lifecycle', 'ops.product_brand_events', 'ops.brand_flag_events', 'ops.product_lifecycle_events']);
export const BRAND_LIFE_EDIT_FUNCTIONS = Object.freeze(['ops.create_brand(uuid, text, text, boolean)', 'ops.set_brand_own(uuid, text, bigint, boolean, boolean, text)',
  'ops.set_product_brand(uuid, text, bigint, bigint, bigint, text, text)', 'ops.set_product_lifecycle(uuid, text, bigint, text, date, text, date, text, text)',
  'ops.sku_first_sold(bigint[])', 'ops.sales_records_from()']);
export const BRAND_LIFE_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_brand_life_tables()', 'ops.brand_life_target(bigint, text)']);
/**
 * 🆕 0099 (表示の項目 = 原産国・内容量・メーカー型番・消費期限の目安・Company DB構想 27 ④-1): master_edit に人の値・記録の表とまとめた値のビューの select (LABEL_SELECT)・
 *   人の値を書く関数の実行 (LABEL_EDIT_FUNCTIONS)。表・関数が無い DB (0099 の前) では付けない (流し直すと付く = それまで画面は欄を出さない)。
 *   🚨 渡さない: 表の insert / update / delete (関数だけが書く)・守りの trigger (LABEL_OWNER_ONLY_FUNCTIONS)。ビューは security_invoker でない = core.products・観測の表の権限は足さない
 */
export const LABEL_SELECT = Object.freeze(['core.product_label_facts', 'ops.product_label_fact_events', 'core.v_product_label_facts']);
export const LABEL_EDIT_FUNCTIONS = Object.freeze(['ops.set_product_label_facts(uuid, text, bigint, text, numeric, text, text, integer, bigint, text, text)']);
export const LABEL_OWNER_ONLY_FUNCTIONS = Object.freeze(['ops.guard_label_tables()']);

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/**
 * 流す文の一覧。pw = { ロール: パスワード } = パスワードを付けるロールだけ (無いロールのパスワードは変えない)。
 * 🚨 ロールを新しく作る文 (create role) には login を付けない: 付けるのは下の alter role (パスワードがあれば一緒に)。パスワードなしで作ったログインのロールは入れない
 */
export function masterEditRoleStatements({ dbName, pw = {} }) {
  const db = ident(dbName);
  const all = MASTER_EDIT_ROLES.join(', ');
  const s = [];
  for (const role of MASTER_EDIT_ROLES) {
    s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = '${role}') then create role ${role}; end if; end $$`);
  }
  for (const role of MASTER_GROUP_ROLES) {
    // まとめのロール = ログインできない・パスワードなし (前に login で作っていても外す)
    s.push(`alter role ${role} with nologin password null nocreaterole noinherit`);
  }
  for (const role of MASTER_LOGIN_ROLES) {
    // 🚨 nosuperuser などは書かない (create-watch-roles.mjs と同じ理由)。作った後に pg_roles で確かめる
    const inherit = MEMBER_OF[role] ? 'inherit' : 'noinherit';
    s.push(`alter role ${role} with login${pw[role] != null ? ` password ${lit(pw[role])}` : ''} nocreaterole ${inherit} connection limit ${CONN_LIMIT[role]}`);
    for (const [k, v] of Object.entries(ROLE_SETTINGS[role] || {})) s.push(`alter role ${role} set ${ident(k)} = ${lit(v)}`);
    s.push(`grant connect on database ${db} to ${role}`);
  }
  for (const [role, group] of Object.entries(MEMBER_OF)) s.push(`grant ${group} to ${role}`);
  // 前に付けた権限を外してから付け直す (流し直しで広い権限が残らない)
  for (const t of new Set([...MASTER_EDIT_SELECT, ...FBA_STOCK_SELECT, ...MASTER_EDIT_WRITE.map(([, t2]) => t2), 'events.master_change_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests', 'ops.master_cutover_events',
    'ops.master_write_sessions', 'ops.master_cutover_prereq_checks', ...REGISTER_OPS_SELECT, ...REG_CSV_OTHER_TABLES])) {
    s.push(`revoke all on ${t} from ${all}`);
  }
  for (const f of [CUTOVER_FUNCTION, OBSERVE_FUNCTION, ACK_FUNCTION, LOCK_SUPPLIERS_FUNCTION, BEGIN_WRITE_FUNCTION, ...REGISTER_EDIT_FUNCTIONS, ...REGISTER_OPS_FUNCTIONS, ...REGISTER_OWNER_ONLY_FUNCTIONS,
    ...REG_CSV_EDIT_FUNCTIONS, ...REG_CSV_OWNER_ONLY_FUNCTIONS, ...AMAZON_MAP_EDIT_FUNCTIONS, ...AMAZON_MAP_OWNER_ONLY_FUNCTIONS]) {
    s.push(`revoke all on function ${f} from public, ${all}`);
  }
  // master_edit
  for (const sc of ['core', 'ops', 'events']) s.push(`grant usage on schema ${sc} to master_edit`);
  for (const t of MASTER_EDIT_SELECT) s.push(`grant select on ${t} to master_edit`);
  s.push('grant usage on schema snapshots to master_edit');
  for (const t of FBA_STOCK_SELECT) s.push(`grant select on ${t} to master_edit`);
  for (const [priv, t] of MASTER_EDIT_WRITE) s.push(`grant ${priv} on ${t} to master_edit`);
  s.push(`grant execute on function ${LOCK_SUPPLIERS_FUNCTION} to master_edit`);
  s.push(`grant execute on function ${BEGIN_WRITE_FUNCTION} to master_edit`);
  for (const f of REGISTER_EDIT_FUNCTIONS) s.push(`grant execute on function ${f} to master_edit`);
  for (const f of REG_CSV_EDIT_FUNCTIONS) s.push(`grant execute on function ${f} to master_edit`);
  for (const f of AMAZON_MAP_EDIT_FUNCTIONS) s.push(`grant execute on function ${f} to master_edit`);
  s.push('grant usage on sequence core.master_version_seq to master_edit');   // version の既定値・0026 の bump_master_version の nextval (呼び手の権限)
  // master_ops
  s.push('grant usage on schema ops to master_ops');
  for (const t of ['ops.master_cutover_state', 'ops.master_cutover_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests']) s.push(`grant select on ${t} to master_ops`);
  s.push(`grant execute on function ${CUTOVER_FUNCTION} to master_ops`);
  for (const t of REGISTER_OPS_SELECT) s.push(`grant select on ${t} to master_ops`);
  for (const f of REGISTER_OPS_FUNCTIONS) s.push(`grant execute on function ${f} to master_ops`);
  // master_observer
  s.push('grant usage on schema ops to master_observer');
  s.push(`grant execute on function ${OBSERVE_FUNCTION} to master_observer`);
  // master_gate (まとめ。ログインは master_gate_render / master_gate_minipc が INHERIT で使う)
  s.push('grant usage on schema ops to master_gate');
  s.push('grant select on ops.master_cutover_state to master_gate');
  // ⑤-3b: 古い入口の門は列ごとの持ち主 (active と prepared) も読む。表は 0055 から = 無ければ付けない (流し直しで付く)。
  //   付いていない = 門は legacy_open 以外で「持ち主を読めない」= 全部閉じる (fail-closed)
  s.push(`do $$ begin if to_regclass('ops.master_ownership_state') is not null then execute 'revoke all on ops.master_ownership_state from ${all}'; execute 'grant select on ops.master_ownership_state to master_gate'; end if; end $$`);
  s.push(`grant execute on function ${ACK_FUNCTION} to master_gate`);
  // 0058 (広げる道 PR-1): 関数がある DB だけ (0058 の前は付けない = 流し直すと付く)。前に付けた実行権を外してから付け直す
  const ifFn = (sig, body) => `do $$ begin if to_regprocedure('${sig}') is not null then ${body} end if; end $$`;
  for (const f of [...WIDEN_EDIT_FUNCTIONS, ACK_V2_FUNCTION, ...LEASE_GATE_FUNCTIONS, ...WIDEN_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of WIDEN_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0066 (知らせの名前空間): 関数がある DB だけ。前に付けた実行権を外してから付け直す
  for (const f of [...OUTBOX_NS_EDIT_FUNCTIONS, ...OUTBOX_NS_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of OUTBOX_NS_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0071 (下書きを消す): 関数がある DB だけ。前に付けた実行権を外してから画面のロールに付け直す
  for (const f of DRAFT_CANCEL_FUNCTIONS) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of DRAFT_CANCEL_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0067 (まとまり): 関数・表がある DB だけ。前に付けた権限を外してから付け直す
  for (const f of [...VARIATION_EDIT_FUNCTIONS, ...VARIATION_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of VARIATION_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  for (const t of VARIATION_SELECT) {
    s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  }
  // 🆕 0070 (NE から消えた商品をやめる): 関数がある DB だけ。前に付けた実行権を外してから、画面 (master_edit) と CLI (master_ops) に付け直す
  for (const f of [...GONE_FUNCTIONS, ...GONE_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of GONE_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit, master_ops';`));
  // 🆕 0076 (日数の許可): 関数がある DB だけ。前に付けた実行権を外してから、運用のロール (master_ops) だけに付け直す
  for (const f of [...GONE_DAY_OVERRIDE_FUNCTIONS, ...GONE_DAY_OVERRIDE_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of GONE_DAY_OVERRIDE_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_ops';`));
  // 🆕 0079 (要確認を確認済みにする): 関数がある DB だけ。前に付けた実行権を外してから画面のロールに付け直す (watcher の確かめ・一覧は migration が付ける = ここでは外さない)
  for (const f of [...QUARANTINE_CONFIRM_FUNCTIONS, ...QUARANTINE_CONFIRM_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of QUARANTINE_CONFIRM_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0072 (NE から消す CSV): 関数がある DB だけ。前に付けた実行権を外してから画面のロールに付け直す (watcher の確かめ・一覧は migration が付ける = ここでは外さない)
  for (const f of [...NE_DEL_FUNCTIONS, ...NE_DEL_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of NE_DEL_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0074 (Amazon の出品一覧): 表・関数がある DB だけ。前に付けた権限を外してから画面のロールに付け直す (記録の表 ops.amazon_sku_hidden_events は読ませない)
  for (const t of [...AMAZON_LISTING_SELECT, 'ops.amazon_sku_hidden_events']) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; end if; end $$`);
  for (const t of AMAZON_LISTING_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of AMAZON_LISTING_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of AMAZON_LISTING_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0075 (発注の設定の知らせ): 表・関数がある DB だけ。前に付けた権限を外してから画面のロールに付け直す (表は読むだけ・書くのは関数だけ)
  for (const t of ORDER_OUTBOX_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...ORDER_OUTBOX_EDIT_FUNCTIONS, ...ORDER_OUTBOX_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of ORDER_OUTBOX_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0087 (梱包の大きさ・重さ): 表・ビュー・関数がある DB だけ。人の値の表・記録・ビューは前に付けた権限を外してから画面のロールに付け直す (読むだけ・書くのは関数だけ)。
  //   出どころの core.product_physicals は select を足すだけ (夜間ロード・ほかのロールの権限は変えない)。0087 の前の DB では付けない (表があっても画面は 0087 が無いと読まない)
  for (const t of PACKAGE_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const t of PACKAGE_SOURCE_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null and to_regclass('core.v_product_packages') is not null then execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...PACKAGE_EDIT_FUNCTIONS, ...PACKAGE_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of PACKAGE_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0078 (ロジザードのバーコードの決め方): 表・関数がある DB だけ。前に付けた権限を外してから画面のロールに付け直す (表は読むだけ・書くのは関数だけ)
  for (const t of LZ_BARCODE_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...LZ_BARCODE_EDIT_FUNCTIONS, ...LZ_BARCODE_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of LZ_BARCODE_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0084 (メーカー): 表・関数がある DB だけ。前に付けた権限を外してから画面のロールに付け直す (表は読むだけ・書くのは関数だけ)
  for (const t of MAKER_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...MAKER_EDIT_FUNCTIONS, ...MAKER_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of MAKER_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0085 (入出庫の履歴・月別のまとめ): 関数がある DB だけ。前に付けた実行権を外してから画面のロールに付け直す (読むだけ)
  for (const f of MOVEMENT_FUNCTIONS) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of MOVEMENT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0086 (販売数のグラフ): 同じ形 (関数がある DB だけ・前に付けた実行権を外してから画面のロールに付け直す)
  for (const f of SALES_CHART_FUNCTIONS) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of SALES_CHART_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0088 (ブランド・商品の段階): 表・関数がある DB だけ。前に付けた権限を外してから画面のロールに付け直す (表は読むだけ・書くのは関数だけ・要求の控えは読ませない)
  for (const t of [...BRAND_LIFE_SELECT, 'ops.brand_requests']) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; end if; end $$`);
  for (const t of BRAND_LIFE_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...BRAND_LIFE_EDIT_FUNCTIONS, ...BRAND_LIFE_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of BRAND_LIFE_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  // 🆕 0099 (表示の項目): 表・ビュー・関数がある DB だけ。人の値の表・記録・ビューは前に付けた権限を外してから画面のロールに付け直す (読むだけ・書くのは関数だけ)
  for (const t of LABEL_SELECT) s.push(`do $$ begin if to_regclass('${t}') is not null then execute 'revoke all on ${t} from ${all}'; execute 'grant select on ${t} to master_edit'; end if; end $$`);
  for (const f of [...LABEL_EDIT_FUNCTIONS, ...LABEL_OWNER_ONLY_FUNCTIONS]) s.push(ifFn(f, `execute 'revoke all on function ${f} from public, ${all}';`));
  for (const f of LABEL_EDIT_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to master_edit';`));
  s.push(ifFn(ACK_V2_FUNCTION, `execute 'grant execute on function ${ACK_V2_FUNCTION} to master_gate';`));
  // new_entry_gate (NOINHERIT のログイン): ops の usage と許可を出す / 取り消す関数の実行・取り消した後に閉じたことを確かめる ops.new_entry_lease_valid (読むだけ・#1645)。
  //   表は読まない (ゲートの診断の表の読み取りは watcher の接続で)。new_entry_lease_valid は WIDEN_EDIT_FUNCTIONS (上で全部のロールから外して master_edit に付け直す) なので、
  //   LEASE_GATE_FUNCTIONS には入れない (入れると上の revoke の対象が増える)。上の revoke の後にここで 1 行足す
  s.push('grant usage on schema ops to new_entry_gate');
  for (const f of LEASE_GATE_FUNCTIONS) s.push(ifFn(f, `execute 'grant execute on function ${f} to new_entry_gate';`));
  s.push(ifFn(LEASE_VALID_FUNCTION, `execute 'grant execute on function ${LEASE_VALID_FUNCTION} to new_entry_gate';`));
  // 0058: 上の「画面が読む表」の GRANT (ops.ne_reg_exports の表の SELECT) の後に、file_bytes を外して列ごとに付け直す (byte 列は ops.ne_reg_file だけ)
  s.push(RESTRICT_FILE_BYTES_SQL);
  // 🆕 0072 (#1690 Codex R1 Medium 1): NE から消す CSV の byte 列も同じ (byte 列は ops.ne_del_download だけ)
  s.push("do $$ begin if to_regprocedure('ops.restrict_ne_del_file_bytes()') is not null then perform ops.restrict_ne_del_file_bytes(); end if; end $$");
  return s;
}

/**
 * ロールを作る / 権限をそろえる (1 取引)。commit の前に属性を確かめ、違えば取り消す。
 * パスワードを付けるのは: まだ無いログインのロール (新しいパスワード) / rotate に入れたロール (新しいパスワード) / pw で渡したロール (その値・試験用)。
 * それ以外のもうあるロールのパスワードは変えない。戻り値の pw = 付けたロールのパスワードだけ
 */
export async function createMasterEditRoles(client, { pw = {}, rotate = [], dryRun = false } = {}) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない`);
  for (const r of rotate) if (!MASTER_LOGIN_ROLES.includes(r)) throw new Error(`--rotate-password に知らないロール: ${r} (${MASTER_LOGIN_ROLES.join(' / ')})`);
  const existing = new Set((await client.query('select rolname from pg_roles where rolname = any($1::text[])', [MASTER_LOGIN_ROLES])).rows.map((r) => r.rolname));
  const pws = {};
  for (const role of MASTER_LOGIN_ROLES) {
    if (pw[role] != null) pws[role] = pw[role];
    else if (!existing.has(role) || rotate.includes(role)) pws[role] = dryRun ? `<${role} の新しいパスワード>` : newPassword();
  }
  const stmts = masterEditRoleStatements({ dbName: info.db, pw: pws });
  if (dryRun) return { info, stmts, roles: [], pw: pws, created: MASTER_LOGIN_ROLES.filter((r) => !existing.has(r)) };
  await client.query('begin');
  try {
    for (const s of stmts) await client.query(s);
    const roles = (await client.query(`select r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, r.rolcanlogin, r.rolinherit, r.rolconnlimit,
        coalesce((select string_agg(g.rolname, ',' order by g.rolname) from pg_auth_members m join pg_roles g on g.oid = m.roleid where m.member = r.oid), '') as memberships
      from pg_roles r where r.rolname = any($1::text[]) order by r.rolname`, [MASTER_EDIT_ROLES])).rows;
    const bad = [];
    for (const r of roles) {
      const why = [];
      const login = MASTER_LOGIN_ROLES.includes(r.rolname);
      if (r.rolsuper) why.push('superuser'); if (r.rolcreaterole) why.push('createrole'); if (r.rolcreatedb) why.push('createdb'); if (r.rolbypassrls) why.push('bypassrls'); if (r.rolreplication) why.push('replication');
      if (login && !r.rolcanlogin) why.push('login できない');
      if (!login && r.rolcanlogin) why.push('まとめのロールなのに login できる');
      if (r.rolinherit !== !!MEMBER_OF[r.rolname]) why.push(r.rolinherit ? 'inherit' : 'noinherit (まとめのロールの権限を使えない)');
      if (login && Number(r.rolconnlimit) !== CONN_LIMIT[r.rolname]) why.push(`connection limit ${r.rolconnlimit}`);
      if (r.memberships !== (MEMBER_OF[r.rolname] || '')) why.push(`メンバー = (${r.memberships || 'なし'}) (期待 ${MEMBER_OF[r.rolname] || 'なし'})`);
      if (why.length) bad.push(`${r.rolname}: ${why.join(' / ')}`);
    }
    if (roles.length !== MASTER_EDIT_ROLES.length) bad.push(`ロールが ${roles.length} 件しか無い`);
    if (bad.length) throw new Error(`ロールの属性が期待と違う (取り消した): ${bad.join(' ; ')}`);
    await client.query('commit');
    return { info, stmts, roles, pw: pws, created: MASTER_LOGIN_ROLES.filter((r) => !existing.has(r)) };
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
}

/** --rotate-password <ロール> (何回でも) を読む */
export function parseRotateArgs(argv) {
  const out = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--rotate-password') {
      const v = argv[i + 1];
      if (!v || v.startsWith('--')) throw new Error('--rotate-password の後にロールの名前が要る');
      out.push(v); i++;
    }
  }
  return out;
}

const ENV_OF = {
  master_edit: ['COMPANY_DB_MASTER_EDIT_URL', 'Render (apps/master-edit)'],
  master_ops: ['COMPANY_DB_MASTER_OPS_URL', '切替の段階を進める手の操作'],
  new_entry_gate: ['COMPANY_DB_NEW_ENTRY_GATE_URL', '開く前のゲート (0058・miniPC の .env・開放の許可を出す / 取り消す・閉じたかを読むだけ)'],
  master_observer: ['COMPANY_DB_MASTER_OBSERVER_URL', '⑤-2 の夜間ロード (NE の観測)'],
  master_gate_render: ['COMPANY_DB_MASTER_GATE_RENDER_URL', 'Render の ⑤-3 の古い入口の門 (Render の env)'],
  master_gate_minipc: ['COMPANY_DB_MASTER_GATE_MINIPC_URL', 'miniPC の ⑤-3 の古い入口の門 (miniPC の .env)'],
};

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const rotate = parseRotateArgs(process.argv.slice(2));
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  const client = await openPgClient(base);
  try {
    const r = await createMasterEditRoles(client, { dryRun, rotate });
    if (dryRun) {
      for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';');
      console.log(`-- パスワードを付けるロール: ${Object.keys(r.pw).join(' / ') || 'なし (もうあるロールのパスワードは変えない)'}`);
      return;
    }
    console.log(`✅ ${MASTER_EDIT_ROLES.join(' / ')} を作った / 権限をそろえた (owner=${r.info.owner} db=${r.info.db})`);
    const set = Object.keys(r.pw);
    if (!set.length) { console.log('パスワードは変えていない (もうあるロールは今の env のまま)。変えるときは --rotate-password <ロール>'); return; }
    console.log('環境変数に足す / 書き換える (パスワードはこの画面にしか出ない。ここに出ないロールは今の env のまま):');
    for (const role of set) console.log(`${ENV_OF[role][0]}=${urlFor(base, role, r.pw[role])}      # ${role} = ${ENV_OF[role][1]}${r.created.includes(role) ? ' (新しく作った)' : ' (パスワードを変えた)'}`);
  } finally { await client.end(); }
}

const isMain = process.argv[1] && /create-master-edit-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
