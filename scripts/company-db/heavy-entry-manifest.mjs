/**
 * heavy-entry-manifest.mjs — Company DB の「重い入口」の棚卸し (heavy_entry_manifest・D-60 の PR 1a・Codex R-D60-v3-4 H2 / R-D60-v3-5 H3)
 *
 * 署名つきの固定の一覧。0056 の migration とは別に持つ (migration の一覧から 1 つ消えても、試験と --verify が気づく)。分け方:
 *   revoke      = 0056 で EXECUTE を全員 (PUBLIC・watcher・profit_reader・持ち主・権限の表に載った全部) から外した。権限の表 (proacl) が空でなければならない
 *   guard_later = 重いが、正当な呼び手がいる (か、外すかを人が決める) = この PR では権限を変えない。D-60 の共通の lock (company_db_heavy) に参加させるか外すかを後の PR で決める
 *   light       = 下の見つける規則にかかるが重くない (定数・policy の表だけ・DDL の補助・1 行の判定など)
 * 🚨 「repo の中から呼んでいない」は DB への直呼びを防いだ証明ではない = guard_later は今も DB に直接つなげば呼べる (PR 1a の範囲の外・後の PR で閉じる)。
 *
 * 🚨 保証の範囲 (#1601 Codex R1 M1): 「Company DB の全部の重い入口」ではない。機械で漏れを止めるのは次の 3 つだけ:
 *   (a) 関数 = 下の見つける規則 (期間の集計の関数・mart の関数・D-60 の関数に依る関数) + 手で足した既知の入口 (期間の形でない chunk の apply など)
 *   (b) view = mart / ops の全部の view (HEAVY_VIEWS に重い / 軽いと理由。pg_class と突き合わせる。権限は変えない)
 *   (c) アプリの側の重い処理 (DB の関数でない = HTTP の chunk・coverage の complete の計算・backup・見張り・夜間ロード) = APP_HEAVY_ENTRIES (ファイルがあることだけ試験)
 *   規則にかからない関数 (jsonb の batch を受ける ops の SECURITY DEFINER など) は、重くても機械では見つけない = 足すときは手で manifest に。
 *   D-60 の共通の lock (company_db_heavy) に参加させる相手の正本は後の PR (PR 4) で、この 3 つの一覧から始める。
 *
 * 見つける規則 (pg_proc との突き合わせ・これから足す関数の漏れ止め): core / mart / ops / raw / snapshots / events の関数 (trigger を除く) で
 *   ① schema が mart (読む口の集計の置き場) ② 入力の引数が期間・件数・保持の形 (RANGE_ARGS = 名前と型) ③ 本体が revoke の関数を名前で呼ぶ
 *   のどれか = manifest に無ければ「分けていない」で落とす。manifest に無い重い関数を手で足すのは構わない (規則より広くてよい)。
 *
 * 使い方 (本番の確かめ・読むだけ = カタログの SELECT だけ。重い関数は呼ばない):
 *   node -r dotenv/config scripts/company-db/heavy-entry-manifest.mjs --verify      # COMPANY_DB_URL で接続。問題が 1 つでもあれば exit 1。TEMP の権限の監査も出す (出すだけ)
 */
import { openPgClient } from './migrate.mjs';

/** reresolve の batch (D-60 PR 1b-0r) を作る migration の番号 = 1 か所だけ。🚨 PR #1605 の後に付け替えるときはここと migration のファイル名だけ
 *  (試験 test-company-db-reresolve-batch-pg.mjs が「ここ = 関数を作るファイルの実際の番号」を見る・付け替えの手順 = PR #1607 の本文) */
export const RERESOLVE_BATCH_MIGRATION = '0057';
const D60 = 'D-60 の利益の重い計算 (2026-10-01 に 93 日分で本番の Postgres を落とした)';
export const HEAVY_ENTRY_MANIFEST = Object.freeze([
  // ─── revoke (0056) ───
  { sig: 'mart.amazon_profit_daily_range(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・公開の入口。受け口は 503 (#1570) = 呼び手なし` },
  { sig: 'mart.amazon_profit_day_totals_range(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・公開の入口。受け口は 503 (#1570) = 呼び手なし` },
  { sig: 'mart._amazon_profit_totals(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・内部の部品 (日の合計)` },
  { sig: 'mart._amazon_profit_rows(smallint, text, text, date, date, mart.amazon_profit_finance_day[], mart.amazon_profit_ad_day[], mart.amazon_profit_ad_child[], mart.amazon_easy_ship_alloc_row[])', cls: 'revoke', reason: `${D60}・内部の部品 (行の本体・0050 で差し替え)` },
  { sig: 'mart._amazon_profit_finance_days(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・内部の部品 (日の決済の状態)` },
  { sig: 'mart._amazon_profit_ad_days(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・内部の部品 (広告の日の状態)` },
  { sig: 'mart._amazon_profit_ad_children(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・内部の部品 (広告の子の結び直し)` },
  { sig: 'mart._amazon_easy_ship_alloc(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・内部の部品 (Easy Ship の割り振り・期間の外の日も読む)` },
  { sig: 'mart.amazon_profit_assert_args(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}・引数の確かめ (利益の関数の中だけで使う・設計 13 の一覧)` },
  { sig: 'mart.finance_daily_sku_range(smallint, text, text, date, date)', cls: 'revoke', reason: `${D60}の材料 (日 × 正規化 SKU の財務)。呼ぶのは利益の関数だけ・watcher (0047 / 0050 の GRANT) は使っていない` },
  // ─── guard_later (重い・正当な呼び手がいる = 権限は変えない) ───
  { sig: 'mart.finance_daily_range(smallint, text, text, date, date)', cls: 'guard_later', public: true, reason: '日 × SKU の財務の期間の集計 (0044 / 0045)。受け口 GET /order-finance/daily (持ち主の接続・miniPC の突き合わせ) が使う。watcher にも明示の GRANT (0044)' },
  { sig: 'mart.ad_efficiency(smallint, text, text, date, date, boolean)', cls: 'guard_later', public: true, reason: '出品ごとの広告の効き目の期間の集計 (0038 / 0039)。人・AI が README の SQL で読む (アプリの呼び手は無い)' },
  { sig: 'mart.ad_efficiency_coverage(smallint, text, text, date, date)', cls: 'guard_later', public: true, reason: '広告の効き目のそろい (0039)。人・AI が README の SQL で読む' },
  { sig: 'mart.sku_activity(smallint, date, date)', cls: 'guard_later', public: true, reason: 'SKU ごとの動きの期間の集計 (0042・商品 360)。人・AI が README の SQL で読む' },
  { sig: 'mart.sku_activity_gaps(smallint, date, date)', cls: 'guard_later', public: true, reason: 'SKU ごとの動きの欠け (0042)。人・AI が読む' },
  { sig: 'mart.sales_expanded_to_skus(smallint, date, date)', cls: 'guard_later', public: true, reason: '売上日次を末端の SKU まで展開 (0042)。sku_activity の中と人・AI' },
  { sig: 'mart.listings_to_skus(smallint, bigint[])', cls: 'guard_later', public: true, reason: '出品 → SKU の展開 (0042)。sku_activity の中で使う (出品の配列が大きいと重い)' },
  { sig: 'mart.sales_daily_check(smallint, text, text, date, date)', cls: 'guard_later', public: true, reason: '売上日次の検算の期間の集計 (0021)。受け口 (router) が使う' },
  { sig: 'mart.refresh_sales_daily(smallint, text, text, integer, boolean, text)', cls: 'guard_later', public: true, reason: '売上日次の作り直し (書き込み・0021)。受け口 (router) が使う' },
  { sig: 'mart.build_sales_daily_dates(smallint, text, text, date[], text, text)', cls: 'guard_later', public: true, reason: '売上日次の日の組み立て (書き込み・0021)。refresh_sales_daily の中で使う' },
  { sig: 'mart.purge_sales_daily(smallint, integer)', cls: 'guard_later', public: true, reason: '売上日次の古い版の削除 (0021)。受け口 (router) が使う' },
  { sig: 'core.relink_shipments_bulk(smallint, bigint, integer)', cls: 'guard_later', public: true, reason: '伝票 → 注文の結び直し (最大 10 万件・一時の表 = TEMP・0017)。受け口 (router)・注文の送り手が使う' },
  { sig: 'core.reresolve_order_lines(smallint, text, date)', cls: 'guard_later', public: true, reason: '注文の行の SKU の解き直し (since = null で全部・一時の表 4 つ = TEMP・0024)。夜間ロード (load/engine) が使う' },
  // 🆕 RERESOLVE_BATCH_MIGRATION (D-60 PR 1b-0r) = 上限つきの batch の新しい署名。設計 13 の分け = Gw (門は PR 3a)・それまでは guard_later。PUBLIC の EXECUTE は作った取引で外した (持ち主だけ)。
  //   夜間ロードは 1b-0e まで旧い 3 引数を呼ぶ = 今は呼び手が無い (試験と apps/company-db/load/reresolve-batch.mjs の部品だけ)。migration = その版で作る (それより前の DB の試験は upTo で外す)
  { sig: 'core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer)', cls: 'guard_later', public: false, migration: RERESOLVE_BATCH_MIGRATION, reason: '注文の行の SKU の解き直しの窓の 1 batch (1b-0r・窓 ≦ 62 日・注文 ≦ 5,000・未解決の明細 ≦ 20,000・一時の表なし)。夜間ロードが 1b-0e で使う (Gw)' },
  { sig: 'core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer)', cls: 'guard_later', public: false, migration: RERESOLVE_BATCH_MIGRATION, reason: 'reresolve の retry の表 (skip した注文) の 1 batch (1b-0r・注文 ≦ 5,000・未解決の明細 ≦ 20,000・一時の表なし)。夜間ロードが 1b-0e で使う (Gw)' },
  { sig: 'core._reresolve_order_batch(smallint, text, bigint[], boolean, integer)', cls: 'guard_later', public: false, migration: RERESOLVE_BATCH_MIGRATION, reason: 'reresolve の 1 batch の共通の部品 (1b-0r・注文の配列 ≦ 5,000・lock の後の今の明細の累計 ≦ p_max_lines ≦ 20,000 = 直に呼ばれても上限を守る)。上の 2 つが呼ぶ (SECURITY INVOKER = PR 1b で runtime に部品の EXECUTE も要る・規則にはかからないが手で足した)' },
  { sig: 'core.merge_duplicate_suppliers()', cls: 'guard_later', public: true, reason: '仕入先の二重の寄せ (全部の仕入先・一時の表 = TEMP・0025 / 0027)。migration と試験が呼ぶ・人の保守の道具' },
  { sig: 'core.relink_shipments(smallint)', cls: 'guard_later', public: true, reason: '伝票 → 注文の全件の結び直し (0013)。アプリの呼び手は無い (試験と人の復旧の道具)。外すかは後の PR で決める' },
  { sig: 'core.relink_ad_spend_listings(smallint)', cls: 'guard_later', public: true, reason: '広告費の出品の結び直し (全件・0035)。広告費の取込 (ingest/ad-spend) が使う' },
  { sig: 'raw.purge_superseded_observations(text, integer)', cls: 'guard_later', public: true, reason: 'raw の古い観測の削除 (保持日数)。在庫の取込 (inventory/logizard) が使う' },
  // 期間の形でない既知の入口 (規則にはかからない・#1601 Codex R1 M1 で手で足した)。jsonb の大きさに SQL の上限は無い
  { sig: 'core.apply_order_finance_batch(smallint, text, text, text, bigint, text, text, jsonb)', cls: 'guard_later', public: true, reason: '財務の 1 注文の行の入れ替え (0043 / 0047)。POST /order-finance の chunk (ingest/order-finance) が注文ごとに呼ぶ = PR 4 で共通の lock の参加者' },
  { sig: 'core.apply_order_batch(smallint, text, text, text, bigint, jsonb, jsonb)', cls: 'guard_later', public: true, reason: '注文の 1 件の入れ替え (0013)。POST /orders の chunk が呼ぶ' },
  { sig: 'core.apply_shipment_batch(smallint, text, bigint, jsonb, jsonb)', cls: 'guard_later', public: true, reason: '出荷の 1 伝票の入れ替え (0013)。POST /shipments の chunk が呼ぶ' },
  { sig: 'ops.amazon_map_sales_coverage(date, integer)', cls: 'guard_later', public: false, reason: 'Amazon SKU の対応の売上のそろい (日数の範囲・SECURITY DEFINER・PUBLIC なし)。マスタの入力の画面 (master_edit) が使う' },
  { sig: 'ops.amazon_map_unmapped_recent(date, integer)', cls: 'guard_later', public: false, reason: 'Amazon SKU の対応の無い最近の売上 (日数の範囲・SECURITY DEFINER・PUBLIC なし)。マスタの入力の画面 (master_edit) が使う' },
  // ─── light (規則にかかるが重くない) ───
  { sig: 'mart.amazon_profit_composition_audit_since()', cls: 'light', public: true, reason: '定数 (0049 の適用の時刻)' },
  { sig: 'mart.amazon_account_fee_tax_rate(text)', cls: 'light', public: true, reason: '定数 (月の手数料の税の表)' },
  { sig: 'core.finance_coverage_state(smallint, text, text, text)', cls: 'light', public: true, reason: 'coverage の 1 行 (主キー) を読む。coverage の受け口・見張り' },
  { sig: 'core.finance_month_settled(smallint, text, text, date)', cls: 'light', public: true, reason: '1 か月の policy と coverage の判定 (policy の表と coverage の 1 行)' },
  { sig: 'core.finance_policy_gaps(smallint, text, text, date, date)', cls: 'light', public: true, reason: 'policy の表 (数行) だけを読む' },
  { sig: 'core.assert_finance_policy_covered(smallint, text, text, date, date)', cls: 'light', public: true, reason: 'policy の表 (数行) だけを読む' },
  { sig: 'snapshots.ensure_month_partitions(date, date)', cls: 'light', public: true, reason: '月の partition を作る DDL の補助 (データを読まない)' },
  { sig: 'snapshots.ensure_month_partitions_for(text[], date, date)', cls: 'light', public: true, reason: '月の partition を作る DDL の補助 (default から移す行はその月だけ)' },
  { sig: 'ops.cutover_ts_between(text, timestamp with time zone, timestamp with time zone)', cls: 'light', public: true, reason: '文字の時刻の判定 (表を読まない)' },
  { sig: 'ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer)', cls: 'light', public: false, reason: 'カードの出来事を件数の上限つきで借りる (SECURITY DEFINER・PUBLIC なし)' },
]);

export const MANIFEST_CLASSES = Object.freeze(['revoke', 'guard_later', 'light']);

/** (b) view の棚卸し (mart / ops の全部の view)。heavy = 全期間の事実の表をまとめる (条件なしで読むと重い) = 後の PR で読む口を絞るか共通の lock に。権限は変えない */
export const HEAVY_VIEWS = Object.freeze([
  { view: 'mart.v_finance_daily', cls: 'heavy', reason: '財務の全期間の日 × SKU の集計 (0043)。watcher も読める・measure-amazon-finance が全期間を測る' },
  { view: 'mart.v_finance_account_fees_monthly', cls: 'heavy', reason: '月の手数料の全期間の集計 (0043)' },
  { view: 'mart.v_order_finance_summary', cls: 'heavy', reason: '注文ごとの財務の集計 (0043・約 51 万注文)' },
  { view: 'mart.v_order_finance_uncovered', cls: 'heavy', reason: '財務の policy の外の注文 (0043)' },
  { view: 'mart.v_sales_daily', cls: 'heavy', reason: '売上日次の今の版 (0021・全期間)' },
  { view: 'mart.v_shipments_daily', cls: 'heavy', reason: '出荷の日次の集計 (全期間)' },
  { view: 'mart.v_shipments_unlinked', cls: 'heavy', reason: '注文に結びつかない出荷 (全期間)' },
  { view: 'mart.v_ad_spend_daily', cls: 'heavy', reason: '広告費の日次の集計 (0035・全期間)' },
  { view: 'mart.v_cross_mall_diff', cls: 'heavy', reason: 'モールをまたぐ差 (観測の表を広く読む)' },
  { view: 'mart.v_product_360', cls: 'light', reason: 'SKU 1 行 (マスタの大きさ = 数千行)' },
  { view: 'mart.v_listing_360', cls: 'light', reason: '出品 1 行 (マスタの大きさ)' },
  { view: 'mart.v_product_dq', cls: 'light', reason: 'v_product_360 の欠けだけ' },
  { view: 'mart.v_sku_stock', cls: 'light', reason: 'SKU の今の在庫 (最新の写しだけ)' },
  { view: 'mart.v_warehouse_stock_current', cls: 'light', reason: '倉庫の今の在庫 (最新の写しだけ)' },
  { view: 'mart.v_sku_cost_observed_effective', cls: 'light', reason: '観測の原価の有効な行 (SKU の大きさ)' },
  { view: 'mart.v_purchase_order_open', cls: 'light', reason: '開いている発注 (数百行)' },
  { view: 'mart.v_purchase_backorder_by_sku', cls: 'light', reason: '発注残 (SKU の大きさ)' },
  { view: 'ops.v_ne_reg_targets', cls: 'light', reason: 'NE 登録の対象 (少数)' },
  { view: 'ops.v_product_hub_outbox_open', cls: 'light', reason: '開いている outbox (少数)' },
  { view: 'ops.v_sku_available', cls: 'light', reason: 'SKU の使える状態 (SKU の大きさ)' },
  { view: 'ops.v_sku_distributable', cls: 'light', reason: '配れる SKU (SKU の大きさ)' },
]);
export const VIEW_SCHEMAS = Object.freeze(['mart', 'ops']);

/** (c) アプリの側の重い処理 (DB の関数・view でない)。PR 4 で共通の lock の参加者にする候補。試験はファイルがあることだけを見る */
export const APP_HEAVY_ENTRIES = Object.freeze([
  { name: 'POST /order-finance の chunk', file: 'apps/company-db/ingest/order-finance.mjs', reason: '注文ごとに core.apply_order_finance_batch・受領記録を書く' },
  { name: 'coverage の complete の計算', file: 'apps/company-db/ingest/finance-coverage.mjs', reason: 'computeReceiptDigest が受領記録 (約 51 万注文) を cursor で 1 回読む' },
  { name: 'バックアップ (論理の写し)', file: 'apps/company-db/backup/dump.mjs', reason: '全部の表を 1 つの repeatable read の取引で読む' },
  { name: '見張り (W1〜W14)', file: 'apps/company-db/watch/run.mjs', reason: 'watcher で毎朝、表と view を広く読む' },
  { name: '夜間ロード', file: 'apps/company-db/load/engine.mjs', reason: 'マスタの全部の入れ直し・reresolve_order_lines' },
]);
/** 期間・件数・保持の入力の引数 (名前 + 型。ops の p_from / p_to (状態の移り = text) は当たらない) */
export const RANGE_ARGS = Object.freeze({
  time: Object.freeze(['p_from', 'p_to', 'p_since', 'p_after', 'p_upto']),   // date / timestamp / timestamptz
  count: Object.freeze(['p_days', 'p_limit', 'p_keep_days']),                // smallint / integer / bigint
  dates: Object.freeze(['p_dates']),                                         // date[]
});
export const SCAN_SCHEMAS = Object.freeze(['core', 'mart', 'ops', 'raw', 'snapshots', 'events']);
export const revokeSigs = () => HEAVY_ENTRY_MANIFEST.filter((e) => e.cls === 'revoke').map((e) => e.sig);
/** その版までに作った関数の行 (migration の無い行 = 0056 より前からある)。upTo = null なら全部 */
export const manifestUpTo = (upTo) => (upTo ? HEAVY_ENTRY_MANIFEST.filter((e) => !e.migration || e.migration <= upTo) : HEAVY_ENTRY_MANIFEST);

/** 見つける規則にかかる関数 (pg_proc から。trigger を除く)。戻り = [{ oid, sig }] */
export async function heavyCandidates(db) {
  const names = revokeSigs().map((s) => s.slice(s.indexOf('.') + 1, s.indexOf('(')));
  const rows = (await db.query(`select p.oid::text as oid, n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as sig
      from pg_proc p join pg_namespace n on n.oid = p.pronamespace
     where n.nspname = any($1::text[]) and p.prokind = 'f' and p.prorettype <> 'trigger'::regtype
       and (n.nspname = 'mart' or p.prosrc ~ $5 or exists (
             select 1 from unnest(coalesce(p.proallargtypes, p.proargtypes::oid[]), p.proargnames,
                                  coalesce(p.proargmodes, array_fill('i'::"char", array[cardinality(coalesce(p.proallargtypes, p.proargtypes::oid[]))]))) a(t, nm, md)
              where md in ('i', 'b', 'v') and (
                    (nm = any($2::text[]) and t in ('date'::regtype, 'timestamp'::regtype, 'timestamptz'::regtype))
                 or (nm = any($3::text[]) and t in ('int2'::regtype, 'int4'::regtype, 'int8'::regtype))
                 or (nm = any($4::text[]) and t = 'date[]'::regtype))))
     order by 2`, [SCAN_SCHEMAS, RANGE_ARGS.time, RANGE_ARGS.count, RANGE_ARGS.dates, `\\m(${names.join('|')})\\M`])).rows;
  return rows;
}

/**
 * 問題の一覧 (空 = 期待どおり)。読むだけ (pg_proc・pg_roles)。
 *   ① manifest の形 (署名の重なり・分け方・理由) と、全部の署名が DB にある
 *   ② revoke = 権限の表が空 (null = 既定 = 持ち主 + PUBLIC も不可)・PUBLIC と superuser でない全部の役割で has_function_privilege が false
 *   ③ guard_later / light = PUBLIC が呼べるかが manifest のとおり (この PR で変えていない・あとから閉じたら manifest を直す)
 *   ④ 見つける規則にかかる関数が全部 manifest にある
 *   upTo (試験だけ) = その版までの DB を見る (manifest の migration がそれより後の行 = まだ作っていない関数は外す)。本番の --verify は付けない (全部の行)
 */
export async function heavyEntryFindings(db, { upTo = null } = {}) {
  const findings = [];
  const q = async (sql, p) => (await db.query(sql, p)).rows;
  const seen = new Set();
  const oidOf = new Map();
  for (const e of manifestUpTo(upTo)) {
    if (!MANIFEST_CLASSES.includes(e.cls)) findings.push(`manifest の分け方が不正: ${e.sig} (${e.cls})`);
    if (!e.reason) findings.push(`manifest の理由が無い: ${e.sig}`);
    if (e.cls !== 'revoke' && typeof e.public !== 'boolean') findings.push(`manifest に PUBLIC の期待が無い: ${e.sig}`);
    const o = (await q('select to_regprocedure($1)::oid::text as o', [e.sig]))[0].o;
    if (o == null) { findings.push(`manifest の関数が DB に無い: ${e.sig}`); continue; }
    if (seen.has(o)) findings.push(`manifest に同じ関数が 2 回: ${e.sig}`);
    seen.add(o); oidOf.set(e.sig, o);
  }
  for (const e of manifestUpTo(upTo)) {
    const o = oidOf.get(e.sig); if (o == null) continue;
    const r = (await q(`select p.proacl::text as acl, p.proacl is null as dflt, coalesce(cardinality(p.proacl), 0) as n, has_function_privilege('public', p.oid, 'execute') as pub
      from pg_proc p where p.oid = $1::oid`, [o]))[0];
    if (e.cls === 'revoke') {
      if (r.dflt) findings.push(`${e.sig}: 権限の表が既定 (持ち主 + PUBLIC が呼べる)`);
      else if (Number(r.n) > 0) findings.push(`${e.sig}: 権限の表が空でない (${r.acl})`);
      if (r.pub) findings.push(`${e.sig}: PUBLIC が呼べる`);
      const who = await q(`select r.rolname from pg_roles r where not r.rolsuper and has_function_privilege(r.oid, $1::oid, 'execute') order by 1`, [o]);
      if (who.length) findings.push(`${e.sig}: 呼べる役割がある (${who.map((x) => x.rolname).join(', ')})`);
    } else if (r.pub !== e.public) {
      findings.push(`${e.sig}: PUBLIC が呼べる = ${r.pub} (manifest の期待 ${e.public}。変えたなら manifest を直す)`);
    }
  }
  for (const v of HEAVY_VIEWS) {
    if (!['heavy', 'light'].includes(v.cls) || !v.reason) findings.push(`view の一覧の形が不正: ${v.view}`);
    if ((await q('select to_regclass($1)::oid::text as o', [v.view]))[0].o == null) findings.push(`view の一覧の view が DB に無い: ${v.view}`);
  }
  const views = (await q(`select n.nspname || '.' || c.relname as v from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('v', 'm') and n.nspname = any($1::text[]) order by 1`, [VIEW_SCHEMAS])).map((r) => r.v);
  const listed = new Set(HEAVY_VIEWS.map((v) => v.view));
  for (const v of views) if (!listed.has(v)) findings.push(`view の一覧で分けていない view: ${v} (heavy / light と理由を HEAVY_VIEWS に足す)`);
  for (const c of await heavyCandidates(db)) {
    if (!seen.has(c.oid)) findings.push(`manifest で分けていない関数: ${c.sig} (重い入口の規則にかかる = revoke / guard_later / light のどれかと理由を heavy-entry-manifest.mjs に足す。閉じるなら作った取引で全員から REVOKE)`);
  }
  return findings;
}

/**
 * TEMP の権限の監査 (出すだけ・何も変えない。R-D60-v3-5 H1 = 外すのは PR 1b)。読むだけ。
 *   戻り = { db, publicTemp, roles: [{ rolname, login, temp }], tempFunctions: [sig] (本体で一時の表を作る関数) }
 */
export async function tempPrivilegeAudit(db) {
  const q = async (sql, p) => (await db.query(sql, p)).rows;
  const d = (await q(`select current_database() as db, has_database_privilege('public', current_database(), 'temp') as pub`))[0];
  const roles = await q(`select r.rolname, r.rolcanlogin as login, has_database_privilege(r.oid, current_database(), 'temp') as temp
    from pg_roles r where not r.rolsuper and r.rolname !~ '^pg_' order by 1`);
  const tempFunctions = (await q(`select n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as sig
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = any($1::text[]) and p.prosrc ~* 'create\\s+(local\\s+)?temp(orary)?\\s+table' order by 1`, [SCAN_SCHEMAS])).map((r) => r.sig);
  return { db: d.db, publicTemp: d.pub, roles, tempFunctions };
}

async function main() {
  if (!process.argv.includes('--verify')) { console.log('使い方: node -r dotenv/config scripts/company-db/heavy-entry-manifest.mjs --verify'); return; }
  const url = process.env.COMPANY_DB_URL;
  if (!url) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  const c = await openPgClient(url);
  try {
    await c.query('begin read only');
    await c.query(`set local statement_timeout = '10s'`);
    // 🆕 DB に適用済みの最後の migration まで (manifest の migration がそれより後の行 = まだ作っていない関数は見ない = repo が DB より新しいときに ❌ にしない)
    const upTo = (await c.query('select max(version) as v from ops.schema_migrations')).rows[0].v || null;
    const f = await heavyEntryFindings(c, { upTo });
    const t = await tempPrivilegeAudit(c);
    await c.query('rollback');
    const n = (cls) => manifestUpTo(upTo).filter((e) => e.cls === cls).length;
    const later = HEAVY_ENTRY_MANIFEST.length - manifestUpTo(upTo).length;
    if (later) console.log(`(DB の migration は ${upTo} まで = それより後の migration で作る関数 ${later} 個は見ていない)`);
    console.log(`TEMP の権限の監査 (出すだけ・PR 1b で外す): db=${t.db} PUBLIC=${t.publicTemp ? 'TEMP あり' : 'なし'} / ${t.roles.map((r) => `${r.rolname}${r.login ? '' : '(nologin)'}=${r.temp ? 'TEMP' : '-'}`).join(' ')} / 一時の表を作る関数 ${t.tempFunctions.length}: ${t.tempFunctions.join(', ')}`);
    if (f.length) { console.log(`❌ 重い入口: ${f.length} 件`); for (const x of f) console.log(`   - ${x}`); process.exitCode = 1; return; }
    console.log(`✅ 重い入口: revoke ${n('revoke')} 個は誰も呼べない (権限の表が空・superuser でない役割は全部 false) / guard_later ${n('guard_later')} 個・light ${n('light')} 個は manifest のまま / 規則にかかる関数と mart・ops の view (${HEAVY_VIEWS.length}) は全部一覧にある`);
  } finally { await c.end(); }
}

const isMain = process.argv[1] && /heavy-entry-manifest\.mjs$/i.test(process.argv[1]);
if (isMain) main().catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
