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

export const CHECKS_VERSION = 'v4';   // v2 (9/22): STOCK_SCOPES に since (監視の開始日) / v3 (9/22): W5 (解決できない在庫の差)・W6 (売れ筋 SKU の欠品) / v4 (9/23): W8 (注文の日次の異常)

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

/**
 * 注文の push が毎朝あるべきモール (完了印のあるもの) と scope (= mall-orders.mjs の MALL_SPECS と同じ。証跡の scope と食い違えば breach)。
 * 🚨 W8 の平常の標本は「その日の注文の取込が完了した」と確かめられる日だけ (行が無い = 0 件・少ない件数 を黙って平常に混ぜない。Codex #1412 R1/R2):
 *   ordersSince       = 注文の履歴が日ごとにそろっている始まりの日 (取込の対象範囲の始まり。最初の月は途中からなので翌月の 1 日)
 *   reconciledThrough = バックフィルの突合 (`mall-orders.mjs --reconcile --all` = miniPC の raw (モールから取り込んだ注文) と Company DB の 日ごとの 件数・明細・金額・取消 が全部一致 → `--mark-backfilled` の完了印) が済んだ最後の日。**人が突合の後に書く** (機械は書き換えない)
 *   → 標本日 D は (ordersSince ≤ D ≤ reconciledThrough) か、その翌朝 (as_of = D+1) の見張りで同じ scope の W7 が pass (ops.watch_results。同じ日に 2 回あれば最後の回) のとき「完了が確かめられた」とする。
 *     どちらも無い日は理由 unverified で除外 (有効標本が減れば blocked = 平常が決まらない)。翌々朝の W7 pass や ops.ingest_runs の success・complete は証跡にしない (Codex R3: chunk の処理完了は走査の完了ではない)
 *     限界 (受け入れている): (a) は人が書く範囲で、その後の取込の故障で無効にはならない (検算した過去は過去のまま) / (b) は daily-sync の契約 (上流の取得 → push → 見張り) の証跡で、モール API 側の欠落までは証明しない
 *   値の出どころ (9/23 本番を読んで): 最初の注文 楽天 2024-12-20 / Amazon 2024-12-29 / au PAY 2024-12-28 / LINE ギフト 2026-02-07 / Qoo10 2026-02-19。突合と完了印は 9/21〜9/22 (5 モール) = 9/21 までは全部一致
 */
export const ORDER_MALLS = [
  { mall: 'rakuten', scope: 'main', ordersSince: '2025-01-01', reconciledThrough: '2026-09-21' },
  { mall: 'amazon', scope: 'jp', ordersSince: '2025-01-01', reconciledThrough: '2026-09-21' },
  { mall: 'aupay', scope: 'main', ordersSince: '2025-01-01', reconciledThrough: '2026-09-21' },
  { mall: 'linegift', scope: 'main', ordersSince: '2026-02-07', reconciledThrough: '2026-09-21' },
  { mall: 'qoo10', scope: 'main', ordersSince: '2026-03-01', reconciledThrough: '2026-09-21' },
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
export const W6_MAX_UNEXPANDED_SHARE = 0.1;   // SKU に展開できない販売 (listing にも当たらない・構成が無い) が正味数量のこれを超えたら「販売履歴が不完全」= blocked (それ以下は観測に残して評価は続ける)

/**
 * W8: 注文の日次の異常 (モール × 昨日)。平常 = 同じ曜日の過去 W8_BASELINE_WEEKS 週のうち「取込の完了が確かめられた日」だけ (ORDER_MALLS の注釈。行が無い = 0 件も、確かめられた日なら正当なゼロ)。
 *   注文があるのに売上日次が未公開の日は除外 (売上 0 で平常を下に引かない)。
 *   件数・売上: |昨日 − 中央値| > W8_MAD_K × MAD かつ 絶対差 ≥ 下限 (件数 / 円) の両方で異常 (どちらか片方では騒がない)
 *   取消率・金額不明率: 昨日 > 中央値 + max(W8_MAD_K × MAD, 下限の差) で異常 (上に外れたときだけ)
 *   0 件: 昨日 0 件で平常の中央値 > 0 なら異常 (統計に関係なく。中央値が 0 = ふだんから注文の無い日が多いモールでは騒がない)
 *   小規模モール (平常 = 中央値が W8_SMALL_MALL_ORDERS_PER_DAY 件/日未満) は件数・売上・取消率・金額不明率の統計判定を外し、0 件・取消率の上限・金額不明率の上限だけ見る
 *   有効標本 (採用条件を満たし、未公開として除外されなかった日) が W8_MIN_SAMPLES 未満 = blocked (保留として通知に含める)。W8_INFO_UNTIL までは info
 */
export const W8_BASELINE_WEEKS = 8;
export const W8_MIN_SAMPLES = 4;
export const W8_MAD_K = 3;
export const W8_MIN_ABS_ORDERS = 20;
export const W8_MIN_ABS_SALES_JPY = 50000;
export const W8_MIN_RATE_DELTA = 0.05;          // 取消率・金額不明率の「差の下限」(MAD が 0 のとき騒がない)
export const W8_SMALL_MALL_ORDERS_PER_DAY = 30;
export const W8_SMALL_MAX_CANCEL_RATE = 0.3;    // 小規模モール: 取消率の上限
export const W8_SMALL_MAX_UNKNOWN_RATE = 0.5;   // 小規模モール: 金額不明率の上限
export const W8_INFO_UNTIL = '2026-10-07';

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
  { id: 'W6', version: 'v1', title: '売れ筋 SKU の欠品', severity: 'warn', depends: ['W1:*', 'W7:*', 'W9:*'], issuePerItem: true,   // W9:* = 公開済みの行があっても作り直しの失敗・watermark の遅れがあれば止まる (Codex #1406 R2)
    what: `直近 ${W6_SALES_DAYS} 日に売れた SKU (v_sales_daily。取消を引く。セットは listing_components で構成 SKU に展開) で 倉庫 + FBA JP の在庫 (v_sku_stock) が 0。窓の中に「注文があるのに未公開の日」や開いた session があれば blocked (未公開の売上を「売れていない」と読まない)。案件は SKU ごと = 新 (発生) / 継続 (翌日も 0) / 回復 (在庫が入った) / 監視対象外 (廃番・窓から外れた = 在庫は 0 のまま)。${W6_INFO_UNTIL} までは info`,
    runbook: '発注 (仕入先発注補助) か FBA 補充。廃番 (handling = discontinued) は対象外' },
  { id: 'W8', version: 'v1', title: '注文の日次の異常', severity: 'warn', depends: ['W7', 'W9'], issuePerItem: false,
    what: `モール × 昨日 の 件数・売上 (v_sales_daily)・取消率・金額不明の明細の割合 を、同じ曜日の過去 ${W8_BASELINE_WEEKS} 週のうち取込の完了が確かめられた日 (突合済みの範囲 / 翌朝の W7 pass。未公開の日は除外) の中央値 ± ${W8_MAD_K}×MAD かつ 絶対差 (件数 ≥ ${W8_MIN_ABS_ORDERS}・売上 ≥ ${W8_MIN_ABS_SALES_JPY} 円) で判定。昨日 0 件は平常の中央値 > 0 なら異常。有効標本 ${W8_MIN_SAMPLES} 未満は blocked。小規模モール (平常の中央値 ${W8_SMALL_MALL_ORDERS_PER_DAY} 件/日未満) は統計を外して 0 件・取消率・金額不明率だけ。${W8_INFO_UNTIL} までは info`,
    runbook: 'モールの管理画面で昨日の注文を確かめる (件数が少ない = 取込の抜けか本当に少ない / 取消率が高い = モール側の障害・在庫切れ / 金額不明 = 取込の項目の抜け)。README「注文を毎日送る」' },
];

export const checkById = (id) => CHECKS.find((c) => c.id === id) || null;
