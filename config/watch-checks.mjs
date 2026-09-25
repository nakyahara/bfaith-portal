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

export const CHECKS_VERSION = 'v10';   // v2 (9/22): STOCK_SCOPES に since (監視の開始日) / v3 (9/22): W5 (解決できない在庫の差)・W6 (売れ筋 SKU の欠品) / v4 (9/23): W8 (注文の日次の異常) / v5 (9/23): W10 (回復していない取込の異常) / v6 (9/23): W11 (注文と出荷の未リンク・発送遅れ) / v7 (9/23): W4 (在庫の純減の異常)・W12 (DB の容量) / v8 (9/23): W8 に祝日・年末年始 (NON_BUSINESS_DAYS) / v9 (9/24): W6 で NE のセット商品の SKU を構成品に展開 / v10 (9/25): W11 で Amazon の支払い待ち (Pending かつ NE で受注メール取込済のまま) を注文から 7 日未満は異常にしない

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
 *   祝日・年末年始 (NON_BUSINESS_DAYS) は平常の標本から外し、昨日がその日なら判定しない (平日と同じ動きとは言えない = 9/22 の偽の異常)。一覧の期限 (NON_BUSINESS_DAYS_UNTIL) を過ぎたら blocked
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
 *   回復の決め方は取込の種類ごと (無関係な後続の成功・世代が進んだだけ では回復にしない。Codex D2 / #1417 R1):
 *     keys       = chunk で送る取込 (注文・出荷)。partial = 失敗した行 (ops.ingest_chunks の result.failed = 全件) の 1 行ずつ、今の行の世代が run より新しく、かつ daily-sync の差分送信の証跡が持つ世代
 *                  (信頼できる世代 = 今朝の証跡 + W10 の記録の履歴) = 正規の送り手が送り直して当たった。止まった running・failed = 送れなかった行は Render から見えない = 自動では回復にしない
 *                  (突合 --reconcile で確かめて W10_ACCEPTED_RUNS に書く)。一度証明した回復は記録 (observed.proven) に残し、翌朝の送信の成否で戻さない
 *     generation = 毎時まるごと写す取込 (ロジザードの在庫)。同じか新しい世代 (checksum = 取得時刻の ISO) の run が success・complete なら回復 (在庫は状態の写し = 新しい世代が入れば古い世代の失敗は残らない)
 *     capture    = 在庫の日次。その run が指す日が今 complete / 例外つきの partial / 上書きされた (partial → complete に上がった) なら回復。W1 / W2 の窓の中の日は W1 / W2 が見る・STOCK_SCOPES の since より前の日は数えない
 *   今朝の push の run (証跡 orders-<mall> が指す run) は、W7 が判定した (pass / breach) ときだけ W7 に任せる (W7 が blocked = 別の実行・範囲・見送り なら W10 が数える)
 *   案件は種類ごとに 1 つ (issuePerItem = false)。同じ行が毎日失敗し続けても「新・回復」を毎日くり返さず「継続 N 日」になる。未回復の run は明細に並べる
 */
export const W10_KINDS = [
  ...ORDER_MALLS.map((m) => ({ source: m.mall, entity: 'orders', scope: m.scope, recovery: 'keys', keyTable: 'orders' })),   // ingest/orders.mjs (source_system = モール)
  { source: 'ne', entity: 'shipments', scope: 'main', recovery: 'keys', keyTable: 'shipments' },                              // ingest/shipments.mjs
  { source: 'logizard', entity: 'inventory', scope: 'main', recovery: 'generation' },                                          // inventory/logizard.mjs (毎時)
  // ingest/stock-daily.mjs (1 取引 = running / failed は残らない。partial の日は complete に上がるときだけ新しい run に差し替わる)。stockSource = STOCK_SCOPES の source
  { source: 'ne', entity: 'stock_daily', scope: 'main', recovery: 'capture', stockSource: 'ne' },
  { source: 'amazon', entity: 'stock_daily', scope: 'jp', recovery: 'capture', stockSource: 'fba_jp' },
  { source: 'amazon', entity: 'stock_daily', scope: 'us', recovery: 'capture', stockSource: 'fba_us' },
];
/** W10 が見ない種類 (ほかの項目・仕組みが見る)。ここにも W10_KINDS にも無い種類の悪い run は「other/*」で異常 (= 定義を足す) */
export const W10_DELEGATED = [
  { source: 'sqlite_initial_load', entity: 'products', scope: 'render', reason: '夜間ロードは成功したときだけ行を作る (失敗は jobs-monitor と running.json)' },
];
/** W10 が見る run の始まり (started_at の JST の日)。これより前の run は数えない */
export const W10_SINCE = '2026-09-16';
export const W10_STUCK_MINUTES = 120;
export const W10_INFO_UNTIL = '2026-10-07';   // 最初の 2 週間は info (件数の目安を見てから error に)
/**
 * 確かめて受け入れた run (理由・責任を書く)。ここにある run は数えない。書き方は 2 通り:
 *   { runId: 'ord_…', reason, owner }                                                  = run ごと
 *   { kind: 'rakuten.orders/main', startedBefore: '2026-09-22T12:00:00+09:00', reason, owner } = その種類でこの時刻より前に始まった run 全部 (突合 --reconcile --all で raw と一致を確かめた時刻)
 */
export const W10_ACCEPTED_RUNS = [];

/**
 * W11: 注文と出荷の未リンク・発送遅れ (モールごと。自社発送 = core.orders.shop_code あり = NE を通る注文)。注文日が W11_WINDOW_DAYS 日前〜W11_LAG_DAYS 日前のもの:
 *   A = モールでは出荷済み (shipped / delivered / returned) なのに NE の伝票が 1 つも結び付いていない (番号の合う伝票が結べていない も A)。全モール。1 件でも異常
 *   A' = モールでは出荷済みで、結び付いた伝票が **キャンセルだけ**。多くは同梱 (複数の注文 → 1 伝票) でまとめられた側 (NE は同梱元の伝票をキャンセルで残す) だが、
 *        同梱でない取消 (伝票を取り消して別の番号で作り直した など) と見分ける材料 (NE のキャンセル区分の原文・同梱先の伝票番号) を取っていない (D-30) = 1 件ずつは判定できない。
 *        → 毎回数えて明細に残し、件数が W11_CANCELLED_ONLY_MAX (9/23 本番の集計のおよそ 2 倍) を超えたら異常 (同梱でない取消が混ざっている疑い)。Codex #1419 R1
 *   B = 未発送アラートの無いモール (W11_UNSHIPPED_MALLS) で、モールで未発送 (notShipped の状態) かつ NE でも出荷していない (出荷確定日のある有効な伝票が無い) まま、
 *       **内容が最後に変わった取込の日** (source_updated_at。注文日より後なら) から W11_LAG_DAYS 日、または注文日から W11_B_MAX_DAYS 日たった = 要確認 (発送遅れの確定ではない。予約・入金待ち・鮮度の分からない状態を含む)
 *       🚨 source_updated_at は「状態が変わった時刻」ではなく「送る内容 (状態・金額・数量・SKU…) が最後に変わった取込の時刻」の近似 (同じ内容の再送 = same では動かない。
 *          Amazon の last_updated_date だけが変わっても送らない)。住所入力・支払いが後から済んだ注文はそこから数え直す一方、金額などの訂正でも数え直す = 最大 W11_LAG_DAYS 日遅れる。
 *          それが続いても注文日から W11_B_MAX_DAYS 日で必ず出す (黙って消え続けない。Codex #1419 R2)
 *   B2 = 同じモールで NE では出荷して W11_B2_GRACE_DAYS 日たつのにモールが未発送のまま = 出荷の通知 (送り状番号) がモールに届いていない。モールの状態が新しいと言えるモールだけ (b2)
 *   9/23 本番 (90 日): A は直近 35 日に 0 件 (35 日より前に 楽天 53・Qoo10 6・au PAY 1 = 窓の外) / A' = 楽天 79・Qoo10 44・au PAY 6・Amazon 2・LINE ギフト 0 / B は LINE ギフト 5 件 (NE でも未出荷)
 *   🚨 Amazon の注文レポートは Pending の次が Shipped (Unshipped が出ない = 0018) = 自社発送の未発送は new のまま → Amazon は new も「未発送」。LINE ギフトの new は受取人の住所入力待ちなど = 正当な待ち
 *   🚨 LINE ギフトは API に最後に見えた時刻 (last_seen_at。14 日見えなければ状態を固定) が Company DB に無い = 状態が新しいか分からない → B2 はしない (B は NE でも未出荷が決め手なので残す)
 *   P = Amazon の支払い待ち (9/25 追加): モールで Pending かつ結び付いた有効な伝票が全部 NE で「受注メール取込済」(まだ起票していない) のまま = 入金待ちの保留 (中原さん確認)。
 *       B の条件を満たしても注文日から W11_P_MAX_DAYS 日未満は異常にしない (observed.payment_pending_orders に残す)。W11_P_MAX_DAYS 日目から B に出す。NE で起票済みなのに出荷していない注文は今まで通り B
 *       🚨 NE の 1 (受注メール取込済) は入金待ち専用ではない = 入金済みなのに NE の起票が止まった注文も同じ形になる (Codex #1454 R1) → 待つのは短く:
 *          Amazon のコンビニ・ATM 払いの期限 (注文から 6 日) を過ぎても保留なら異常 = 7 日 (中原さん 9/25)
 *       🚨 Amazon の注文レポートでは「入金待ち」と「入金済み・出荷待ち」が同じ Pending = モールの状態だけでは見分けられない → NE の伝票の状態で見分ける
 *   🚨 楽天・au PAY はモールの状態を注文日から 7 日しか読み直さない・Qoo10 は取消が API に出ない = B / B2 は Amazon と LINE ギフトだけ (ほかのモールは既存の未発送アラート)
 */
export const W11_LAG_DAYS = 5;
export const W11_WINDOW_DAYS = 30;
export const W11_B2_GRACE_DAYS = 2;
export const W11_B_MAX_DAYS = 14;   // 内容が変わり続けても、注文日からこの日数で B に出す (安全網)
export const W11_P_MAX_DAYS = 7;    // 支払い待ち (P) を異常にしないのは注文日からこの日数の前日まで (この日数の日から B)。Amazon のコンビニ・ATM 払いの期限 6 日 + 1
export const W11_UNSHIPPED_MALLS = [
  { mall: 'amazon', notShipped: ['new', 'confirmed', 'ready', 'on_hold'], b2: true,    // 状態は最終更新日 (last_updated_date) で読み直す = 新しい
    paymentPending: { statusSource: 'Pending' } },   // P = 支払い待ち (上の注釈)。NE の伝票が全部 new (受注メール取込済) かは W11_ROWS の n_active_new で見る
  { mall: 'linegift', notShipped: ['confirmed', 'ready', 'on_hold'], b2: false },       // 状態が固定されたか分からない (上の注釈)
];
/**
 * A' (キャンセルの伝票だけ) の上限 = 評価の窓 (注文日 30 日前〜5 日前 = 両端を含めて 26 日) の件数。暫定値。
 * 算出 = 9/23 本番の集計 (w11-survey.mjs の既定 = 90 日前〜5 日前 = 86 日: 楽天 79・Qoo10 44・au PAY 6・Amazon 2・LINE ギフト 0) × 26 / 86 × 2 を切り上げ、最低 3 (小さいモールの 1〜2 件で騒がない)
 *   = 楽天 48 (47.8)・Qoo10 27 (26.6)・au PAY 4 (3.6)・Amazon 3 (1.2)・LINE ギフト 3 (0)。見直すときは w11-survey.mjs --days 30 で評価と同じ窓を数える
 * pass は「同梱だと確かめた」ではない (件数がふだんの範囲というだけ)
 */
export const W11_CANCELLED_ONLY_MAX = { rakuten: 48, qoo10: 27, aupay: 4, amazon: 3, linegift: 3 };
export const W11_INFO_UNTIL = '2026-10-07';

/**
 * 祝日・年末年始 (JST。内閣府の国民の祝日 + 年末年始 12/29〜1/3)。W4 は平常の標本から外し、昨日がこの日なら判定しない (平日と同じ動きとは言えない)。
 * **毎年足して NON_BUSINESS_DAYS_UNTIL を延ばす** (期限を過ぎたら W4 は blocked = 足し忘れに気づく)。倉庫の休業日と一致するかは現場の確認待ち
 * W4 と W8 が使う (9/22 の W8 amazon/jp の breach = シルバーウィークの祝日を平日の火曜と比べた偽の異常 が発端)
 */
export const NON_BUSINESS_DAYS = [
  // 🚨 W8 は過去 8 週 (約 57 日) を標本に使う = 今日より 2 か月以上前から一覧に入れておく (8/11 の山の日が平日として混ざっていた。Codex #1425 R1)
  '2025-12-29', '2025-12-30', '2025-12-31', '2026-01-01', '2026-01-02', '2026-01-03', '2026-01-12', '2026-02-11', '2026-02-23', '2026-03-20',
  '2026-04-29', '2026-05-04', '2026-05-05', '2026-05-06', '2026-05-03', '2026-07-20', '2026-08-11',
  '2026-09-21', '2026-09-22', '2026-09-23', '2026-10-12', '2026-11-03', '2026-11-23',
  '2026-12-29', '2026-12-30', '2026-12-31', '2027-01-01', '2027-01-02', '2027-01-03', '2027-01-11', '2027-02-11', '2027-02-23', '2027-03-21', '2027-03-22',
  '2027-04-29', '2027-05-03', '2027-05-04', '2027-05-05',
];
export const NON_BUSINESS_DAYS_UNTIL = '2027-05-31';   // この日まで一覧が足りている (as_of の昨日がこれより後なら W4 は blocked)

/**
 * W4: 在庫の純減の異常 (ロジザードの在庫の差 = events.inventory_events の source_system 'logizard_diff'・昨日の区間 (前日 → 昨日の最後の毎時の世代) を SKU で足したもの)。
 *   減った数 (out) = 減った SKU の減り分の和 / 増えた数 (in) = 増えた SKU の増え分の和 / 差し引き (net)。理由 (出荷・入荷・棚卸し・FBA 納品) は分からない:
 *   同じ日の入荷は SKU ごとに出荷と打ち消し合う・棚卸しの調整もふつうの増減として入る・棚移動は 0・良品と B 品は合算・区間は暦日ではなく毎時の最後の世代 (18 時ごろ) の間
 *   平常 = 同じ曜日の過去 W4_BASELINE_WEEKS 週のうち、差を作った (done) 日で・印の件数と中身が合い・祝日でない日。減った数は |昨日 − 中央値| > K × MAD かつ ≥ W4_MIN_ABS_QTY で異常
 *   (多すぎ = 大量の減少 / 少なすぎ = 出荷が在庫に反映されていない疑い)。差し引きは 中央値 − 昨日 > K × MAD かつ ≥ W4_MIN_ABS_QTY (大きく減った) だけ異常。有効標本 W4_MIN_SAMPLES 未満は blocked
 *   🚨 在庫の差は 9/20 から (logizard の since 9/19 の翌日)・過去は作れない (元の在庫の写しが残っていない) = 同じ曜日の標本 4 つがそろう 10/19〜10/25 ごろまで blocked
 */
export const W4_BASELINE_WEEKS = 8;
export const W4_MIN_SAMPLES = 4;
export const W4_MAD_K = 3;
export const W4_MIN_ABS_QTY = 500;
export const W4_INFO_UNTIL = '2026-11-02';   // 判定を始めて (10/19 ごろ) 2 週間は info

/**
 * W12: DB の容量 (Render の Company DB)。今の pg_database_size と、毎晩の締め (inventory/logizard.mjs の maintainInventory) が ops.job_runs に残す大きさの記録 (9/20 から) で、
 *   1 日あたりの増え方 = 直近 W12_HISTORY_DAYS 日の「日ごとの増え分」の中央値 (バックフィルのような一度きりの急増に引きずられない。9/21 → 9/22 に +1.5 GB)。
 *   容量 W12_DISK_BYTES まで W12_MIN_REMAINING_DAYS 日を切る、または今の大きさが W12_WARN_BYTES を超えたら異常。日ごとの増え分が W12_MIN_DELTAS 個に満たなければ blocked
 *   🚨 pg_database_size はストレージ全体 (WAL など) ではない = Render の容量の監視の代わりではない (Codex D2)
 */
export const W12_DISK_BYTES = 10 * 1024 ** 3;    // 06: Basic-1GB + ストレージ 10GB (プランを変えたらここも)
export const W12_WARN_BYTES = 7 * 1024 ** 3;     // D-34: 7 GB で通知
export const W12_MIN_REMAINING_DAYS = 90;
export const W12_HISTORY_DAYS = 14;
export const W12_MIN_DELTAS = 5;
export const W12_MAX_STALE_DAYS = 3;   // 最新の大きさの記録がこれより古ければ残り日数を推計しない (締めが止まっている = 古い増え方で pass にしない。Codex #1423 R1)。7 GB の判定は記録に関係なく続ける
export const W12_JOB_ID = 'company-db-inventory-hourly';
export const W12_INFO_UNTIL = '2026-10-07';

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
  { id: 'W6', version: 'v2', title: '売れ筋 SKU の欠品', severity: 'warn', depends: ['W1:*', 'W7:*', 'W9:*'], issuePerItem: true,   // W9:* = 公開済みの行があっても作り直しの失敗・watermark の遅れがあれば止まる (Codex #1406 R2)
    what: `直近 ${W6_SALES_DAYS} 日に売れた SKU (v_sales_daily。取消を引く。セットの出品は listing_components・NE のセット商品の SKU は sku_components で構成 SKU に展開 = セット自体は判定しない) で 倉庫 + FBA JP の在庫 (v_sku_stock) が 0。窓の中に「注文があるのに未公開の日」や開いた session があれば blocked (未公開の売上を「売れていない」と読まない)。案件は SKU ごと = 新 (発生) / 継続 (翌日も 0) / 回復 (在庫が入った) / 監視対象外 (廃番・窓から外れた = 在庫は 0 のまま)。${W6_INFO_UNTIL} までは info`,
    runbook: '発注 (仕入先発注補助) か FBA 補充。廃番 (handling = discontinued) は対象外' },
  { id: 'W8', version: 'v2', title: '注文の日次の異常', severity: 'warn', depends: ['W7', 'W9'], issuePerItem: false,
    what: `モール × 昨日 の 件数・売上 (v_sales_daily)・取消率・金額不明の明細の割合 を、同じ曜日の過去 ${W8_BASELINE_WEEKS} 週のうち取込の完了が確かめられた日 (突合済みの範囲 / 翌朝の W7 pass。未公開の日は除外) の中央値 ± ${W8_MAD_K}×MAD かつ 絶対差 (件数 ≥ ${W8_MIN_ABS_ORDERS}・売上 ≥ ${W8_MIN_ABS_SALES_JPY} 円) で判定。昨日 0 件は平常の中央値 > 0 なら異常。有効標本 ${W8_MIN_SAMPLES} 未満・昨日が祝日は blocked (祝日は標本からも外す)。小規模モール (平常の中央値 ${W8_SMALL_MALL_ORDERS_PER_DAY} 件/日未満) は統計を外して 0 件・取消率・金額不明率だけ。${W8_INFO_UNTIL} までは info`,
    runbook: 'モールの管理画面で昨日の注文を確かめる (件数が少ない = 取込の抜けか本当に少ない / 取消率が高い = モール側の障害・在庫切れ / 金額不明 = 取込の項目の抜け)。README「注文を毎日送る」' },
  { id: 'W10', version: 'v1', title: '回復していない取込の異常', severity: 'error', depends: [], issuePerItem: false,
    what: `${W10_SINCE} 以降の ops.ingest_runs で failed / partial / ${W10_STUCK_MINUTES} 分を超えて running のまま、かつ回復していないもの (注文・出荷 = 失敗した行が 1 行ずつ正規の差分送信の世代で当たった。止まった run は自動では回復にしない / ロジザード = 同じか新しい世代の success / 在庫の日次 = その日が今 complete か例外つき。W1 / W2 の窓の中は W1 / W2)。今朝の push の run は W7 が判定したときだけ W7 に任せる。案件は取込の種類ごと。${W10_INFO_UNTIL} までは info`,
    runbook: 'README「AI が見張る」の W10。明細の run の failed_ranges / ops.ingest_chunks の result.failed を見て、直したら次の push が取り直す (直せないと決めたら W10_ACCEPTED_RUNS に理由と責任を書く)' },
  { id: 'W11', version: 'v1', title: '注文と出荷の未リンク・発送遅れ', severity: 'warn', depends: ['W7'], issuePerItem: false,
    what: `自社発送の注文 (注文日 ${W11_WINDOW_DAYS} 日前〜${W11_LAG_DAYS} 日前) で、A = モールでは出荷済みなのに NE の伝票が結び付いていない (1 件でも異常) / A' = 結び付いた伝票がキャンセルだけ (多くは同梱。件数が上限を超えたら異常) / B = Amazon 自社発送・LINE ギフトでモールでも NE でも未発送のまま、内容が ${W11_LAG_DAYS} 日変わっていないか注文から ${W11_B_MAX_DAYS} 日 (要確認。Amazon で Pending かつ NE で受注メール取込済のまま = 支払い待ちは注文から ${W11_P_MAX_DAYS} 日未満は数えない) / B2 = NE で出荷して ${W11_B2_GRACE_DAYS} 日たつのに Amazon が未発送のまま。今朝の出荷の push が確かめられない・結び直しが途中なら blocked。${W11_INFO_UNTIL} までは info`,
    runbook: 'A = NE で注文番号を検索 (伝票が無い = NE に取り込まれていない・別の番号で起票) / B = セラーセントラル・LINE ギフトの管理画面で発送状況を確かめる / B2 = モールへの出荷通知 (送り状番号のアップロード) を確かめる。README「AI が見張る」の W11' },
  { id: 'W4', version: 'v1', title: '在庫の純減の異常', severity: 'warn', depends: ['W3'], issuePerItem: false,
    what: `昨日のロジザードの在庫の差 (SKU の和) の 減った数・差し引き を、同じ曜日の過去 ${W4_BASELINE_WEEKS} 週 (差を作った日・祝日を除く) の中央値 ± ${W4_MAD_K}×MAD かつ ${W4_MIN_ABS_QTY} 個以上の差で判定 (減った数は多すぎ・少なすぎ、差し引きは大きく減ったときだけ)。理由 (出荷・入荷・棚卸し・FBA 納品) は分からない。有効標本 ${W4_MIN_SAMPLES} 未満・昨日が祝日は blocked。${W4_INFO_UNTIL} までは info`,
    runbook: 'ロジザードの在庫の推移と、昨日の出荷 (NE)・入荷・棚卸し・FBA 納品を突き合わせる。README「在庫を毎時写す」「AI が見張る」の W4' },
  { id: 'W12', version: 'v1', title: 'DB の容量', severity: 'warn', depends: [], issuePerItem: false,
    what: `今の DB の大きさ (pg_database_size) と、直近 ${W12_HISTORY_DAYS} 日の日ごとの増え分の中央値から、容量 (${Math.round(W12_DISK_BYTES / 1024 ** 3)} GB) まで ${W12_MIN_REMAINING_DAYS} 日を切る・${Math.round(W12_WARN_BYTES / 1024 ** 3)} GB を超えたら異常。Render の容量の監視の代わりではない。${W12_INFO_UNTIL} までは info`,
    runbook: 'Render のダッシュボードで Postgres のディスクを確かめ、大きい表 (pg_total_relation_size) と整理 (raw の 30 日・日次の整理) を見る。足りなければプラン / ディスクを上げる (中原さん判断)' },
];

export const checkById = (id) => CHECKS.find((c) => c.id === id) || null;
