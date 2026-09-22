/**
 * watch-checks.mjs — Company DB を毎朝見張る項目の **定義の正本** (設計 = AI_reference CompanyDB構想/09_AIが見張る仕組み_設計_20260922.md)
 *
 * ⭐約束:
 *   - 定義はここ (コード) だけに持つ。DB には結果しか残さない (既存の ai.watch_rules (0006) は使わない = 廃止候補)
 *   - 判定は 4 値 pass / breach / blocked / execution_error。重さ (info / warn / error) とは別の軸
 *   - 🚨 「行がある = そろっている」と読まない。前提 (完了の印) が無ければ blocked = pass にしない
 *   - 閾値・例外は人が決めて Git で変える (AI に決めさせない)。例外には 理由・責任・見直し期限 を書く
 *
 * 評価の本体 = apps/company-db/watch/checks.mjs (この定義を読んで SQL を流す)。
 * 変えたら CHECKS_VERSION を上げる (結果の表に版が残る = 後から「どの版の判定か」が分かる)。
 */

export const CHECKS_VERSION = 'v3';   // v2 (9/22): STOCK_SCOPES に since (監視の開始日) / v3 (9/22): W5 (解決できない在庫の差)・W6 (売れ筋 SKU の欠品) を追加

/** 09 は B-Faith (company 1) だけを見る (D-W8)。いろは (2) は対象外 */
export const COMPANY_ID = 1;

/**
 * 在庫の日次で「そろっているべき」source × scope (W1 / W2 の期待の一覧)。
 * dayOffset = 対象日 (評価の基準日 = JST の今日 からの日数)。logizard は 00:35 に前日を締める = 昨日が対象。
 * allowPartial = partial を許す例外 (理由・責任・見直し期限。until を過ぎたら例外は効かなくなる = 期限つき)
 * since = 監視の開始日 (YYYY-MM-DD)。W2 (欠測の履歴) はこの日より前を数えない = 在庫日次を作る前・バックフィルで埋まらない履歴を「欠測」として通知しない。
 *         W1 (今日の分) には効かない。無ければ窓の全部を見る。値は 9/22 に本番の取得記録を読んで決めた (最初に complete がそろった日)
 */
export const STOCK_SCOPES = [
  { source: 'logizard', scope: 'main', dayOffset: -1, since: '2026-09-19' },   // 在庫日次 (0011) が本番で動き始めた日
  { source: 'ne', scope: 'main', dayOffset: 0 },                               // 5/2 から全期間 complete (#1383 で 143 日ぶんバックフィル)
  { source: 'fba_jp', scope: 'jp', dayOffset: 0, since: '2026-09-22' },        // 9/9〜9/21 は fba.db の消失事故 (#1376) の跡で partial / missing。初めて complete になった日
  { source: 'fba_us', scope: 'us', dayOffset: 0, since: '2026-09-20', allowPartial: { reason: 'amazon_us の出品が Company DB に 0 件 = RESTOCK の 3 区分が無い (D-W6)', owner: '中原さん', until: '2026-12-31' } },   // 9/19 まで missing が点在 (同じ事故)
];

/** 注文の push が毎朝あるべきモール (完了印のあるもの) と scope (= mall-orders.mjs の MALL_SPECS と同じ。証跡の scope と食い違えば breach) */
export const ORDER_MALLS = [
  { mall: 'rakuten', scope: 'main' }, { mall: 'amazon', scope: 'jp' }, { mall: 'aupay', scope: 'main' }, { mall: 'linegift', scope: 'main' }, { mall: 'qoo10', scope: 'main' },
];

/** 在庫の差 (W3) の対象 */
export const STOCK_DIFF = { source: 'logizard', scope: 'main', calcVersion: 'lzdiff:v1' };

/** W2: 欠測を見る窓 (日)。窓から外れた欠測は「回復」ではなく「監視期間外」 */
export const W2_WINDOW_DAYS = 7;
/** W1: building のまま何分たてば「滞留」か */
export const BUILDING_STALE_MINUTES = 120;
/** W9: 公開の穴を見る日数 (昨日から何日さかのぼるか) */
export const W9_LOOKBACK_DAYS = 3;
/** W9: 変わった注文を送った朝、売上日次の watermark が push の開始からこれ以上遅れていれば「反映されていない」 */
export const W9_WATERMARK_SLACK_MINUTES = 15;

/** W5: 解決できない在庫の差 (昨日の差で SKU が分からなかった商品コード)。件数と数量の割合の両方が上限以内なら pass。上限超えが done の日で連続して続けば warn に上げる */
export const W5_MAX_UNRESOLVED = 10;
export const W5_MAX_UNRESOLVED_SHARE = 0.02;
export const W5_ESCALATE_DAYS = 3;
/** W6: 売れ筋 SKU の欠品 = 直近この日数に売れた SKU (取消を引いて 1 個以上) で 倉庫 + FBA JP の在庫が 0。案件は SKU ごと (新 = 発生・回復 = 解消・継続 = 翌日も 0) */
export const W6_SALES_DAYS = 28;
export const W6_SCOPE = { scope: 'jp', sources: ['logizard', 'fba_jp'] };   // 在庫の合算に使う source (v_sku_stock の warehouse_qty + fba_jp_available)
export const W6_INFO_UNTIL = '2026-10-06';   // 最初の 2 週間は info (件数の目安を見てから warn に。09 §4)

/** 実行器の全体の期限 (ms)。statement_timeout (1 文の期限) とは別 */
export const RUN_DEADLINE_MS = 5 * 60 * 1000;
/** 明細 (watch_result_items) に保存する上限 (行・バイト)。案件の管理には使わない = 判定は全件で行い、保存だけ抜粋 */
export const ITEMS_MAX_ROWS = 200;
export const ITEMS_MAX_BYTES = 64 * 1024;

/**
 * 項目。順番 = 評価の順 (depends は前に評価されている前提)。
 * depends = 'W1' は同じ scope の W1 が pass であること / 'W1:*' は W1 の全部の scope が pass であること (1 つでも違えば blocked)
 * issuePerItem = 案件を明細 (日付・SKU など) ごとに持つか (false = scope 全体で 1 つ)
 */
export const CHECKS = [
  { id: 'W1', version: 'v1', title: '在庫の取込の完了', severity: 'error', depends: [], issuePerItem: false,
    what: '期待する source × scope の対象日の stock_capture_days が complete (building の滞留も異常)',
    runbook: 'db/company/README.md「在庫を毎時写す」「在庫の日次を送る」。fba_us の partial は D-W6 の例外' },
  { id: 'W2', version: 'v2', title: '在庫の欠測の履歴', severity: 'warn', depends: [], issuePerItem: true,
    what: `直近 ${W2_WINDOW_DAYS} 日の missing / partial (行が無い日も)。監視の開始日 (STOCK_SCOPES の since) より前は数えない。同じ欠測は継続として 1 行`,
    runbook: 'README「在庫の日次を送る」の --from/--to で取り直す (元データがあれば)。無ければ missing のまま (申告済み)' },
  { id: 'W3', version: 'v1', title: '在庫の差の完了', severity: 'error', depends: ['W1'], issuePerItem: false,
    what: 'stock_diff_days の昨日が done (前日の欠測による skipped は仕様どおり = W2 へ)',
    runbook: 'README「在庫を毎時写す」の「締めをやり直す」。0022 が未適用なら migrate' },
  { id: 'W7', version: 'v1', title: '注文の取込の完了', severity: 'error', depends: [], issuePerItem: false,
    what: '今朝の push の証跡 (送り手が書く) と、送信があれば ops.ingest_runs の同じ run が success / complete。変更ゼロの朝は証跡だけで pass',
    runbook: 'README「注文を毎日送る」。証跡が無い = push が走っていない (daily-sync のログ)' },
  { id: 'W9', version: 'v1', title: '売上日次の公開', severity: 'warn', depends: ['W7'], issuePerItem: false,
    what: `session が閉じている・変わった注文を送った朝は watermark が進んでいる・注文のある日は昨日まで公開されている (直近 ${W9_LOOKBACK_DAYS} 日。注文ゼロの日は 0 件として扱う)`,
    runbook: 'README「売上の日次」の --refresh-sales / --check-sales' },
  { id: 'W5', version: 'v1', title: '解決できない在庫の差', severity: 'info', depends: ['W3'], issuePerItem: false,
    what: `昨日の stock_diff_days の unresolved_changed (SKU が分からず差をイベントにできなかった商品コード) が ${W5_MAX_UNRESOLVED} 件以下 かつ 数量の割合 (日次の元から計算) が ${W5_MAX_UNRESOLVED_SHARE * 100}% 以下。done の日で ${W5_ESCALATE_DAYS} 日続けば warn`,
    runbook: 'README「在庫を毎時写す」の「SKU が分からない商品コード」= core.skus に無い NE 商品コード → 商品マスタ (product-hub) に登録するか、ロジザード側の商品ID を直す' },
  { id: 'W6', version: 'v1', title: '売れ筋 SKU の欠品', severity: 'warn', depends: ['W1:*', 'W7:*'], issuePerItem: true,
    what: `直近 ${W6_SALES_DAYS} 日に売れた SKU (v_sales_daily。取消を引く) で 倉庫 + FBA JP の在庫 (v_sku_stock) が 0。案件は SKU ごと = 新 (発生) / 継続 (翌日も 0) / 回復 (解消)。${W6_INFO_UNTIL} までは info`,
    runbook: '発注 (仕入先発注補助) か FBA 補充。廃番 (handling = discontinued) は対象外' },
];

export const checkById = (id) => CHECKS.find((c) => c.id === id) || null;
