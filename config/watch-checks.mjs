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

export const CHECKS_VERSION = 'v5';   // v2 (9/22): STOCK_SCOPES に since (監視の開始日) / v3 (9/22): W5 (解決できない在庫の差)・W6 (売れ筋 SKU の欠品) / v4 (9/23): W8 (注文の日次の異常) / v5 (9/23): W10 (回復していない取込の異常)

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

/**
 * W10: 回復していない取込の異常 (ops.ingest_runs)。「悪い run」= failed / partial / W10_STUCK_MINUTES を超えて running のまま。
 *   回復の決め方は取込の種類ごと (無関係な後続の成功で回復にしない。Codex D2):
 *     keys       = chunk で送る取込 (注文・出荷)。partial は失敗した行 (ops.ingest_chunks の result.failed = 全件) の今の行が、その run より新しい世代 (received_batch_seq > run の batch_seq) で取り直されていれば回復。
 *                  running のまま止まった run (送り手が途中で落ちた) は、送れなかった行が miniPC の outbox から次の run に引き継がれる = 同じ種類で世代が新しい run が閉じていれば (complete) 回復。受け取った chunk の失敗した行は同じく行ごとに確かめる
 *     generation = 毎時まるごと写す取込 (ロジザードの在庫)。同じか新しい世代 (checksum = 取得時刻の ISO) の run が success・complete なら回復 (在庫は状態の写し = 新しい世代が入れば古い世代の失敗は残らない)
 *   今朝の push の run (証跡 orders-<mall> が指す run) は W7 が見る = W10 は数えない (同じ失敗を 2 回出さない。翌朝も残っていれば W10 が出す)
 *   案件は種類ごとに 1 つ (issuePerItem = false)。同じ行が毎日失敗し続けても「新・回復」を毎日くり返さず「継続 N 日」になる。未回復の run は明細に並べる
 */
export const W10_KINDS = [
  ...ORDER_MALLS.map((m) => ({ source: m.mall, entity: 'orders', scope: m.scope, recovery: 'keys', keyTable: 'orders' })),   // ingest/orders.mjs (source_system = モール)
  { source: 'ne', entity: 'shipments', scope: 'main', recovery: 'keys', keyTable: 'shipments' },                              // ingest/shipments.mjs
  { source: 'logizard', entity: 'inventory', scope: 'main', recovery: 'generation' },                                          // inventory/logizard.mjs (毎時)
];
/** W10 が見ない種類 (ほかの項目・仕組みが見る)。ここにも W10_KINDS にも無い種類の悪い run は「other/*」で異常 (= 定義を足す) */
export const W10_DELEGATED = [
  { source: 'ne', entity: 'stock_daily', scope: 'main', reason: 'W1 / W2 (stock_capture_days) が見る。1 取引なので running / failed の行は残らない (partial は W1 の例外と同じ判定)' },
  { source: 'amazon', entity: 'stock_daily', scope: 'jp', reason: '同上 (fba_jp)' },
  { source: 'amazon', entity: 'stock_daily', scope: 'us', reason: '同上 (fba_us。partial は W1 の例外 D-W6)' },
  { source: 'sqlite_initial_load', entity: 'products', scope: 'render', reason: '夜間ロードは成功したときだけ行を作る (失敗は jobs-monitor と running.json)' },
];
/** W10 が見る run の始まり (started_at の JST の日)。これより前の run は数えない */
export const W10_SINCE = '2026-09-16';
export const W10_STUCK_MINUTES = 120;
export const W10_INFO_UNTIL = '2026-10-07';   // 最初の 2 週間は info (件数の目安を見てから error に)
/** 直せないと分かって受け入れた run (理由・責任を書く)。ここにある run は数えない */
export const W10_ACCEPTED_RUNS = [];   // 例: { runId: 'ship_…', reason: '…', owner: '中原さん' }

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
  { id: 'W10', version: 'v1', title: '回復していない取込の異常', severity: 'error', depends: [], issuePerItem: false,
    what: `${W10_SINCE} 以降の ops.ingest_runs で failed / partial / ${W10_STUCK_MINUTES} 分を超えて running のまま、かつ回復していないもの (注文・出荷 = 失敗した行が新しい世代で取り直されたか・止まった run の後に同じ種類の run が閉じたか / ロジザード = 同じか新しい世代の success)。今朝の push の run は W7 が見る。案件は取込の種類ごと。${W10_INFO_UNTIL} までは info`,
    runbook: 'README「AI が見張る」の W10。明細の run の failed_ranges / ops.ingest_chunks の result.failed を見て、直したら次の push が取り直す (直せないと決めたら W10_ACCEPTED_RUNS に理由と責任を書く)' },
];

export const checkById = (id) => CHECKS.find((c) => c.id === id) || null;
