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
  { sig: 'ops.amazon_sku_last_sold(text[], date)', cls: 'guard_later', public: false, since: '0074', reason: 'seller SKU (正規化) ごとの最後に売れた日 (0074・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。マスタの入力の「まだつないでいない Amazon の SKU」(apps/master-edit/amazon-unlinked.mjs の listUnlinked) が画面を開くたびに 1 回呼ぶ = 正当な呼び手がいる (閉じない)。' + 'p_since = JST の今日 - 364 日 (SOLD_LOOKBACK_DAYS 365 の固定・画面から変えられない)・p_norms = 絞り込んだ FBA の未接続の SKU (関数が 20,000 個を超えると 22023 で断る・本番は約 4,000)。Amazon 日本の 1 年の注文を ix_orders_mall_date (company_id, mall, scope_key, order_date_jst) で読み、明細は order_id・出品は主キーで引く = 1 年分を集計するので軽くはない (amazon_map_unmapped_recent の 7〜60 日より長い)。画面のロールの statement_timeout 20 秒が上限' },
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
  { sig: 'ops.claim_outbox_events(text, text, text, uuid, bigint, integer, integer, integer)', cls: 'light', public: false, since: '0066', reason: '名前空間つきの知らせを件数の上限つきで借りる (0066・SECURITY DEFINER・PUBLIC なし)' },
  { sig: 'ops.claim_order_settings_events(text, text, uuid, uuid, integer, integer, integer, uuid[])', cls: 'light', public: false, since: '0075', reason: '発注の設定の知らせを件数の上限 (200) つきで借りる (0075・SECURITY DEFINER・PUBLIC なし・master_edit と持ち主 = 夜間ロードの後ろの段)。知らせの表は新商品の数 (少数)・開いている知らせの部分 index' },
  { sig: 'ops.add_sku_gone_day_overrides(uuid, text, text, text[], integer, integer)', cls: 'light', public: false, since: '0076', reason: '「NE から消えた → やめる」の日数の許可を足す (0076・SECURITY DEFINER・PUBLIC なし・master_ops にだけ実行)。' + '呼び手 = scripts/company-db/master-gone-day-override.mjs --apply (人が中原さんの OK の後に流す CLI だけ・定期実行なし)。p_days は許可の日数 (1〜3) で期間の集計ではない = 規則 (② RANGE_ARGS の p_days) にかかるが重くない。' + '件数の上限 = p_codes は 1〜100 件 (超えると 22023)・p_valid_days は 1〜14・コードごとに core.skus と登録の状態を主キー / code_norm の索引で引いて許可の表に 1 行ずつ足すだけ (読む・書くのは最大 100 行)' },
  // 🆕 0079 (要確認を確認済みにする・Company DB構想 22 の Q-1)。規則にはかからない (期間・件数の引数なし) = 手で足した (10/12・10/13 に足し忘れた反省 = 新しい関数は全部ここで分ける)
  { sig: 'ops.quarantine_confirm_check(bigint)', cls: 'light', public: false, since: '0079', reason: '要確認の商品 1 件を確認済みにしてよいかの表 (0079・SECURITY DEFINER・PUBLIC なし・master_edit と watcher に実行)。' + 'SKU 1 件を主キーで引き、構成品 (セットは 20 品まで)・今日の原価・代表の仕入先・最新の封をした照合の回の観測 1 行・その SKU の判断の候補 (code_norm の索引) を読むだけ' },
  { sig: 'ops.quarantine_confirm_list(text[])', cls: 'light', public: false, since: '0079', reason: '要確認の一覧 (0079・SECURITY DEFINER・PUBLIC なし・master_edit と watcher に実行)。null = 要確認の全部 (本番 10/13 = 42 件・夜間ロードが NE で見つけた商品だけ・500 件を超えると 22023 too_many) / コードは 500 件まで (超えると 22023) = どちらも 500 件が上限。' + '1 件ずつ ops.quarantine_confirm_check (上) を呼ぶだけ = 一括の inspect・毎朝のまとめ (Q-3・Q-4) の材料' },
  { sig: 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', cls: 'light', public: false, since: '0079', reason: '要確認の商品 1 件を確認済みにする (0079・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。1 件 = 1 取引・確かめ (上) を 2 回と登録の状態の 1 行・履歴の 1 行・約束と保存の記録の 1 行ずつ' },
  { sig: 'ops.quarantine_confirm_item(text, boolean, jsonb, jsonb, text, text)', cls: 'light', public: false, since: '0079', reason: '確かめの表の 1 行を作る部品 (0079・定数の組み立てだけ・表を読まない・だれにも実行を渡さない)' },
  // 🆕 0085 (商品ごとの入出庫履歴と月別のまとめ・CompanyDB構想 26)。p_from / p_after / p_limit が規則にかかる = ここで分ける。🚨 番号を付け替えたら since も
  { sig: 'ops.sku_stock_anchors(bigint, date)', cls: 'light', public: false, since: '0085', reason: 'SKU 1 つのロジザードの毎日の在庫 (0085・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。取得の日 (日次は 13 か月の保持 = 400 行まで) × その日の SKU の行を ix_sku_stock_daily_sku で引く。商品の画面の「入出庫の履歴」を開くたびに 1 回' },
  { sig: 'ops.sku_shipment_totals(bigint, timestamp with time zone[])', cls: 'light', public: false, since: '0085', reason: 'SKU 1 つの出荷を取得時刻の区切り (1,000 個まで) ごとに足す (0085・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。明細は ix_shipment_lines_sku・伝票は主キー。本番を測った売れ筋 (明細 1,048 行) で出荷の 13 か月の読みが 0.4 秒' },
  { sig: 'ops.sku_movement_shipments(bigint, timestamp with time zone, timestamp with time zone, text, integer)', cls: 'light', public: false, since: '0085', reason: 'SKU 1 つの出荷の行 (伝票ごと・新しい順・p_limit は 1〜5,000 に丸める) (0085・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。ix_shipment_lines_sku + 主キー。画面のロールの statement_timeout 20 秒が上限' },
  { sig: 'ops.sku_shipments_monthly(bigint, date)', cls: 'light', public: false, since: '0085', reason: 'SKU 1 つの月ごとの受注数・売上数 (0085・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。画面は 13 か月 (p_from = 13 か月前の月初)。本番を測った売れ筋で 7 ms' },
  // 🆕 0086 (販売数のグラフ・10/17 中原さん)。p_from が規則にかかる = ここで分ける。🚨 番号を付け替えたら since も
  { sig: 'ops.sku_sales_daily(bigint, date)', cls: 'light', public: false, since: '0086', reason: 'SKU 1 つの日 × モールの正味の販売数 (0086・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行・金額は返さない)。' + 'その SKU を含むセット (上へ 5 段・セットの構成の表は数千行) と出品の構成 (ix_listing_components_sku) から、明細を ix_order_lines_sku_chart / ix_order_lines_listing_only_chart・注文を ix_orders_chart で引く (0092 = 要る列を持つ部分索引 = 索引だけで読む・0092 の前は ix_order_lines_sku / ix_order_lines_listing・主キー + 表の頁・会社の全部の売上を展開しない)。' + '期間は 2025-01-01 (p_from をそれより古くできない) 〜 今日 (JST) の決まった日付 = 返す行はその日数 × 8 モールまで (画面は 24 か月だけ頼む)。商品の画面の「販売数のグラフ」を開くたびに 1 回 (画面の期限 8 秒・ロールの statement_timeout 20 秒)' },
  // 0096 (NE へ送るの 1 ファイル・#1762)。p_since が規則にかかる = ここで分ける (0096 で足し忘れ = test-master-quarantine-confirm [H1] が落ちていた)
  { sig: 'ops._ne_upd_filler(text, bigint, text, text, jsonb, timestamp with time zone, jsonb)', cls: 'light', public: false, since: '0096', reason: 'NE へ送るの CSV で、その SKU が変えていない欄の 1 つのセルを今朝の NE の値で埋める部品 (0096・SECURITY DEFINER でない・だれにも実行を渡さない)。' + 'SKU 1 件の今朝の NE の値 1 行 (主キー)・その欄のこの 2 日の CSV の行 (code_norm の索引) を読むだけ。p_since = 今朝の取得の始め (期間の集計ではない)' },
  // 🆕 0098 (NE にあるので登録済みにする)。規則にはかからない (期間・件数の引数なし) = 手で足した (0079 と同じ・新しい関数は全部ここで分ける)。🚨 番号を付け替えたら since も
  { sig: 'ops.draft_in_ne_check(bigint)', cls: 'light', public: false, since: '0098', reason: '下書き・NE 登録待ち 1 件を「NE にあるので登録済みにする」してよいかの表 (0098・SECURITY DEFINER・PUBLIC なし・master_edit と watcher に実行)。' + 'SKU 1 件を主キーで引き、NE のコードの台帳 1 行・最新の封をした照合の回の観測 1 行・生きている NE 登録の CSV の品目 (SKU の索引)・セットの構成品 (20 品まで)・NE 登録の CSV の期待値 (ops.ne_reg_canonical) を読むだけ。NE 登録の CSV の画面は候補 (下書き 200 件まで) のうち NE にもうあるものと一部違う品目だけ呼ぶ' },
  { sig: 'ops.confirm_draft_in_ne(uuid, text, bigint, text)', cls: 'light', public: false, since: '0098', reason: '1 件を NE 確認済みにする (0098・SECURITY DEFINER・PUBLIC なし・master_edit にだけ実行)。1 件 = 1 取引・確かめ (上) を 2 回と登録の状態の 1 行・履歴の 1 行・一部違う品目の 1 行・約束と保存の記録の 1 行ずつ' },
  { sig: 'ops._draft_in_ne_problem(text, text)', cls: 'light', public: false, since: '0098', reason: '確かめの表の 1 行を作る部品 (0098・定数の組み立てだけ・表を読まない・だれにも実行を渡さない)' },
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
  { view: 'ops.v_order_settings_outbox_open', cls: 'light', since: '0075', reason: '届いていない発注の設定の知らせ (0075・少数・開いている知らせの部分 index)' },
  { view: 'ops.v_sku_available', cls: 'light', reason: 'SKU の使える状態 (SKU の大きさ)' },
  { view: 'ops.v_sku_distributable', cls: 'light', reason: '配れる SKU (SKU の大きさ)' },
  { view: 'purchasing.v_snapshot_status', cls: 'light', since: '0077', reason: '発注の設定の写しの今の世代 (0077・世代の表の 1 行)' },
]);
export const VIEW_SCHEMAS = Object.freeze(['mart', 'ops', 'purchasing']);

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
// 🆕 0077: purchasing (発注の設定の写し) も見る = これから関数を足したら漏れ止めにかかる (今は関数なし)
export const SCAN_SCHEMAS = Object.freeze(['core', 'mart', 'ops', 'raw', 'snapshots', 'events', 'purchasing']);
export const revokeSigs = () => HEAVY_ENTRY_MANIFEST.filter((e) => e.cls === 'revoke').map((e) => e.sig);

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
 *   ① manifest の形 (署名の重なり・分け方・理由) と、全部の署名が DB にある (🆕 since = 足した migration。その migration をまだ流していない DB では無くてよい)
 *   ② revoke = 権限の表が空 (null = 既定 = 持ち主 + PUBLIC も不可)・PUBLIC と superuser でない全部の役割で has_function_privilege が false
 *   ③ guard_later / light = PUBLIC が呼べるかが manifest のとおり (この PR で変えていない・あとから閉じたら manifest を直す)
 *   ④ 見つける規則にかかる関数が全部 manifest にある
 */
export async function heavyEntryFindings(db) {
  const findings = [];
  const q = async (sql, p) => (await db.query(sql, p)).rows;
  const seen = new Set();
  const oidOf = new Map();
  for (const e of HEAVY_ENTRY_MANIFEST) {
    if (!MANIFEST_CLASSES.includes(e.cls)) findings.push(`manifest の分け方が不正: ${e.sig} (${e.cls})`);
    if (!e.reason) findings.push(`manifest の理由が無い: ${e.sig}`);
    if (e.cls !== 'revoke' && typeof e.public !== 'boolean') findings.push(`manifest に PUBLIC の期待が無い: ${e.sig}`);
    if (e.since != null && (e.cls === 'revoke' || !/^\d{4}$/.test(e.since))) findings.push(`manifest の since が不正: ${e.sig} (${e.since})`);
    const o = (await q('select to_regprocedure($1)::oid::text as o', [e.sig]))[0].o;
    if (o == null) {
      // 🆕 0066: since = その migration で足した関数。まだ流していない DB (配ってから migration の本適用までの間・前の版の試験) では「無い」を問題にしない
      if (e.since && !(await q('select exists (select 1 from ops.schema_migrations where version = $1) as ok', [e.since]))[0].ok) continue;
      findings.push(`manifest の関数が DB に無い: ${e.sig}`); continue;
    }
    if (seen.has(o)) findings.push(`manifest に同じ関数が 2 回: ${e.sig}`);
    seen.add(o); oidOf.set(e.sig, o);
  }
  for (const e of HEAVY_ENTRY_MANIFEST) {
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
    if ((await q('select to_regclass($1)::oid::text as o', [v.view]))[0].o == null) {
      // 🆕 0075: since = その migration で足した view。まだ流していない DB では「無い」を問題にしない (関数の since と同じ)
      if (v.since && !(await q('select exists (select 1 from ops.schema_migrations where version = $1) as ok', [v.since]))[0].ok) continue;
      findings.push(`view の一覧の view が DB に無い: ${v.view}`);
    }
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
    const f = await heavyEntryFindings(c);
    const t = await tempPrivilegeAudit(c);
    await c.query('rollback');
    const n = (cls) => HEAVY_ENTRY_MANIFEST.filter((e) => e.cls === cls).length;
    console.log(`TEMP の権限の監査 (出すだけ・PR 1b で外す): db=${t.db} PUBLIC=${t.publicTemp ? 'TEMP あり' : 'なし'} / ${t.roles.map((r) => `${r.rolname}${r.login ? '' : '(nologin)'}=${r.temp ? 'TEMP' : '-'}`).join(' ')} / 一時の表を作る関数 ${t.tempFunctions.length}: ${t.tempFunctions.join(', ')}`);
    if (f.length) { console.log(`❌ 重い入口: ${f.length} 件`); for (const x of f) console.log(`   - ${x}`); process.exitCode = 1; return; }
    console.log(`✅ 重い入口: revoke ${n('revoke')} 個は誰も呼べない (権限の表が空・superuser でない役割は全部 false) / guard_later ${n('guard_later')} 個・light ${n('light')} 個は manifest のまま / 規則にかかる関数と mart・ops の view (${HEAVY_VIEWS.length}) は全部一覧にある`);
  } finally { await c.end(); }
}

const isMain = process.argv[1] && /heavy-entry-manifest\.mjs$/i.test(process.argv[1]);
if (isMain) main().catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
