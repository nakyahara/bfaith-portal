/**
 * 定期実行・運用対象の台帳 (期待構成)
 *
 * ⭐ここが「何が動いているべきか」の正本。ここに載っていないものは監視されない。
 *   **新しい定期実行・期限つき人手作業・一時物を作るときは、実装前にここへ1エントリ足すこと。**
 *   (ルールの正本 = AI_reference『システム設計/定期実行ジョブ管理_20260801.md』)
 *
 * type ごとの意味:
 *   scheduled_job    — 定期実行。成功時に POST /apps/jobs-monitor/ping/<id> を打つ (dead-man方式)。
 *                      直近の予定時刻 (anchor_hour_jst:anchor_minute_jst) + grace_hours までに
 *                      成功が来なければ「締切超過」(予定時刻基準 = 成功が遅れても締切がドリフトしない)
 *                      ⭐partial_max_days を持つジョブは「1回で終わらない長時間バッチ」。
 *                        毎日の締切は status=partial (動いたが未完走) でも満たせる代わりに、
 *                        未完走が partial_max_days 回までは許容し、それを超えたら
 *                        「停滞 (stalled)」として要対応に出す (max = 許容する最大回数)。
 *                        これが無いと、実行時間の上限で毎回中断されるバッチは
 *                        「毎朝鳴りっぱなし」か「永遠に完走しなくても緑」の二択になる
 *   heartbeat        — 常駐ワーカー (数十秒〜数時間間隔) の生存監視。max_age_hours を超えて
 *                      生存 ping が途切れたら「止まっている」と判定する。
 *                      ⭐毎日決まった時刻に走るものは scheduled_job を使うこと。
 *                        常駐ワーカーをアンカー方式で見ると「報告直後に停止」を最大1日+猶予ぶん
 *                        見逃す (2026-08-05 Codexレビュー High)。
 *                      max_age_hours は ping 間引き間隔の2倍以上を目安に取る
 *   human_obligation — 人がやる期限つき作業 (OAuth再認可・APIキーローテ等)。完了時に ping。
 *                      次回期限 = 前回完了 + period_hours。warn_days 前から毎朝の要対応に出る
 *   temporary_asset  — 一時タスク・退避フォルダなど「いつか消すもの」。remove_by を過ぎたら要対応に出る
 *   (data_freshness = 「ジョブが動いたか」でなく「データが新しいか」の監視は PR-2 で追加予定)
 *
 * importance:
 *   P1 = 当日業務に直結 (締切超過は即時通知 + 6時間ごと再通知)
 *   P2 = 要対応だが1日は許容 (即時通知 + 24時間ごと再通知)
 *   P3 = 数日止まっても致命的でない (毎朝の要対応サマリのみ)
 *   TMP = 一時物 (毎朝の要対応サマリのみ)
 *
 * 監視のセマンティクス (重要):
 *   監視が見るのは「ok ping が期限内に来たか」だけ。status=fail の ping は記録されるが
 *   即時通知はしない — 失敗の内容はジョブ自身の GChat 通知が既に担っており、二重に鳴らすと
 *   通知疲れで本当に大事なものが埋もれる (2026-07 の Yahoo OAuth 期限切れ見逃しの教訓)。
 *   ここが守るのは「そもそも走らなかった / 途中で固まった / 誰も気づかず止まっていた」。
 */

export const JOBS_REGISTRY = [
  {
    id: 'picking-drive-poller',
    type: 'heartbeat',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'Drive出荷_noのピッキングリストCSVを2分間隔で自動取込 (RPAのPOST欠落・手動配置の自己回復)。PickingServer内の常駐ループで、独立したスケジュールタスクではない',
    where: 'miniPC PickingServer (apps/picking/drive-sync.js startDrivePoller)',
    schedule: '常駐 (120秒間隔・env PICKING_POLL_INTERVAL_SEC。生存 ping は1時間に1回へ間引き)',
    // ping間引き1時間の3倍。ポーリング失敗が続いた場合も ping が止まりここに出る
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'C:\\tools\\picking-service\\PickingServer.out.log の [picking-drive-poller] を確認。失敗台帳は picking.db pk_drive_imports。'
      + 'ping には miniPC の C:\\tools\\bfaith-picking\\.env に JOBS_MONITOR_TOKEN が必要。復旧は Restart-Service PickingServer',
  },
  {
    id: 'packing-drive-poller',
    type: 'heartbeat',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'Drive出荷_noの納品書CSV (納品書_出荷_XX.csv) を2分間隔で自動取込 (梱包支援アプリの入力)。'
      + 'ピッキング突合ok+出荷作業日=今日のみ自動確定し、mismatch/前日ファイルは台帳に残して手動承認へ回す。'
      + '④配送変更/再印刷の通知再送に加え、梱包資材の変更通知 outbox (pk_pack_material_events・undo猶予後に'
      + 'GChat送信・at-least-once) と観測ログ180日purgeもこのループの1ステップ (2026-08-24 PR#895)。'
      + 'PickingServer内の常駐ループで、独立したスケジュールタスクではない',
    where: 'miniPC PickingServer (apps/packing/drive-sync.js startPackingDrivePoller。env PACKING_ENABLED)',
    schedule: '常駐 (120秒間隔・env PACKING_POLL_INTERVAL_SEC。生存 ping は1時間に1回へ間引き)',
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'C:\\tools\\picking-service\\PickingServer.out.log の [packing-drive-poller] を確認。失敗台帳は picking.db pk_pack_drive_imports'
      + ' (取込画面 /apps/packing/admin/import にも要確認一覧が出る)。復旧は Restart-Service PickingServer',
  },
  // ─────────────── heartbeat (Render 常駐: inquiry-hub) ───────────────
  {
    id: 'inquiry-hub-sync',
    type: 'heartbeat',
    importance: 'P2',
    owner: '中原さん',
    purpose: '問い合わせ受信同期 (楽天R-Messe / Yahoo! / Gmail を15分間隔で取込)。止まると新着問い合わせが画面に出ず、スタッフが返信漏れに気づけない',
    where: 'Render bfaith-portal 常駐 (apps/inquiry-hub/sync/cron.js startInquiryHubSyncCron。env INQUIRY_HUB_SYNC_CRON_ENABLED)',
    schedule: '常駐 (15分間隔 + deep 日次JST05:37。生存 ping は1時間に1回へ間引き)',
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'Render ログの [inquiry-hub-cron] を確認。店舗単位の失敗は ⚙️運用管理 (/apps/inquiry-hub/admin) の同期状態・sync_errors。復旧は Render 再デプロイ',
  },
  {
    id: 'inquiry-hub-outbox',
    type: 'heartbeat',
    importance: 'P2',
    owner: '中原さん',
    purpose: '問い合わせ返信の送信ワーカー (outbox_replies を30秒間隔で処理)。止まると返信ジョブが送信されないまま滞留する (スタッフは送信済みのつもりになる)',
    where: 'Render bfaith-portal 常駐 (apps/inquiry-hub/sync/cron.js startInquiryHubOutboxCron。env INQUIRY_HUB_OUTBOX_CRON_ENABLED)',
    schedule: '常駐 (30秒間隔。生存 ping は1時間に1回へ間引き)',
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'Render ログの [inquiry-hub-outbox] を確認。要対応ジョブ (unknown/needs_review) は ⚙️運用管理 (/apps/inquiry-hub/admin)。復旧は Render 再デプロイ。'
      + '⚠️env INQUIRY_HUB_OUTBOX_CRON_ENABLED=true を入れるまでは ping が来ない (=有効化とこの台帳エントリはセット)',
  },
  // ─────────────── scheduled_job (miniPC Task Scheduler) ───────────────
  {
    id: 'mall-csv-fetch-all',
    type: 'scheduled_job',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'モール統計CSVの自動取得 (楽天RPP/データ/レビュー・Yahoo統計・Yahooレビュー(P2-Y)・auPAY・Qoo10)。分析データの入口',
    where: 'miniPC TaskScheduler [MallCsvFetchAll]',
    schedule: '毎日 05:30 (失敗モールは25分後に1回リトライ)',
    anchor_hour_jst: 5,
    anchor_minute_jst: 30,
    grace_hours: 5,
    lifecycle: 'permanent',
    runbook: 'scripts/mall-csv-fetcher/logs/fetch-all-*.log を確認。認証切れなら各モールの再ログイン手順 (Yahoo=デスクトップのYahoo-Relogin.bat)',
  },
  {
    id: 'rakuten-review-mail-send',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: '楽天レビュー フォロー/クーポンメールの正午送信 (らくらくーぽん置換 PR-C5)。'
      + '月次5%クーポンの自動発行 (ensure-monthly) → 当日12:00予定の action を ready 昇格 (plan) → at-most-once 送信 (send)。'
      + 'cutover 前は ownership=vendor のため送信0件で正常 (ping は ok)',
    where: 'miniPC TaskScheduler [RakutenReviewMailSend] (scripts/mall-csv-fetcher/run-rakuten-review-send.ps1)',
    schedule: '毎日 12:05',
    anchor_hour_jst: 12,
    anchor_minute_jst: 5,
    grace_hours: 3, // 15:00 までに ok が無ければ締切超過 (フォローは発送+21日の規約期限があるため翌日には気づきたい)
    lifecycle: 'permanent',
    runbook: 'AI_reference『らくらくーぽん置換_経緯引き継ぎ_20260717.md』§PR-C5。'
      + '手動再実行: DATA_DIR を設定して node apps/warehouse/send-rakuten-review-mails.js plan → send --limit N。'
      + 'ambiguous (結果不明) が出たら自動再送しない: delivery_attempts と実到達を確認して人が action を解決',
  },
  {
    id: 'warehouse-daily-sync',
    type: 'scheduled_job',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'モール受注取込→fact build→DQ→Render mirror 同期 (約45ステップ)。全業務データの土台。'
      + '日次出荷サマリ (出荷日×モール×配送方法) の再構築もここ。'
      + '最後に「楽天未発送アラート」「Yahoo未発送アラート」「auPAY未発送アラート」「Qoo10未発送アラート」'
      + '(前日12時の締めより前の注文で、まだ発送されていないものを GChat 通知) と'
      + '「Yahoo問い合わせ対応漏れ」(未返信+完了処理忘れの問い合わせを検知、該当時のみ通知) も走る',
    where: 'miniPC TaskScheduler [WarehouseDailySync + Retry1〜3 (同じidにping)]',
    schedule: '毎日 07:00 (retry 08:30 / 10:00 / 11:30)',
    anchor_hour_jst: 7,
    anchor_minute_jst: 0,
    grace_hours: 7, // retry3 (11:30) + 実行時間 + 余裕。14:00までに ok が無ければ締切超過
    lifecycle: 'permanent',
    runbook: 'logs/daily-sync-*.log を確認。個別ジョブ再実行は reference_daily_sync_manual_job_rerun (node -r dotenv/config)',
  },
  {
    id: 'mf-daily-sync',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'マネーフォワード会計データの日次同期 (mf-research)',
    where: 'miniPC TaskScheduler [MFDailySync]',
    schedule: '毎日 07:30',
    anchor_hour_jst: 7,
    anchor_minute_jst: 30,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'C:\\Users\\bfaith\\mf-research のログ確認。credentials.db の token 失効なら再認証',
  },
  {
    id: 'yamato-toi-fetch',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'ヤマトB2 追跡情報・荷物問い合わせCSVの取得',
    where: 'miniPC TaskScheduler [YamatoToiFetch8]',
    schedule: '毎日 08:00 (SYSTEM実行)',
    anchor_hour_jst: 8,
    anchor_minute_jst: 0,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'C:\\tools\\yamato-b2-fetcher のログ確認',
  },
  {
    id: 'logizard-nefuda-csv',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'ロジザード値札印刷CSVの日次生成 (入庫情報管理の一部)',
    where: 'miniPC TaskScheduler [Logizard-NefudaCSV]',
    schedule: '毎日 08:30',
    anchor_hour_jst: 8,
    anchor_minute_jst: 30,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'C:\\tools\\logizard-automation のログ確認',
  },
  {
    id: 'logizard-stock-hourly',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'ロジザード在庫スナップショットの毎時取込 (欠品LINE通知の他ロケ在庫表示 + Render mirror_logizard_stock)',
    where: 'miniPC TaskScheduler [LogizardZaikoHourly]',
    schedule: '毎日 09:00-18:00 の毎時00分 (10回。どれか1回の成功で当日ok。日中の停止は欠品通知の「HH:MM時点」表示でも見える)',
    anchor_hour_jst: 9,
    anchor_minute_jst: 0,
    grace_hours: 3,
    // partial_streak は「連続 partial ping 回数」(日数ではない)。毎時 ping なので 6 =
    // mirror push 失敗が7時間 (ほぼ丸一日分) 続いたら stalled (ローカルの欠品通知は生きている)
    partial_max_days: 6,
    lifecycle: 'permanent',
    runbook: 'C:\\Users\\bfaith\\bfaith-portal\\logs\\logizard-stock-hourly.log と C:\\tools\\logizard-automation\\logs を確認。手動再実行: powershell -File C:\\Users\\bfaith\\bfaith-portal\\scripts\\logizard-stock\\run-hourly.ps1',
  },
  // logizard-kinkyu-hokyu (miniPC 毎日08:50 の緊急在庫補充) は 2026-08-09 に台帳から撤去。
  //   miniPCのTaskScheduler [Logizard-KinkyuHokyu] は同日 /DISABLE 済み (削除はしていない)。
  //   運用は会社PCのStream Deck「朝一緊急補充」手動実行に移行 (紙印刷廃止→P-touchシールCSV+PDFを
  //   G:\共有ドライブ\ロジザード【緊急補充在庫】へ出力する新方式。手順=AI_reference
  //   『ロジザード作業自動化/緊急補充シールCSV_設定手順_20260809.md』)。
  //   miniPCで定期実行を再開する場合はこのエントリを復活させること (git履歴 2026-08-09 参照)。
  {
    id: 'qoo10-nyukin',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'Qoo10 入金確認 (画面ありブラウザ操作)',
    where: 'miniPC TaskScheduler [Qoo10Nyukin]',
    schedule: '毎日 07:00 / 12:30 (どちらかの成功でOK)',
    anchor_hour_jst: 7,
    anchor_minute_jst: 0,
    grace_hours: 8,
    lifecycle: 'permanent',
    runbook: 'reCAPTCHA が出たら miniPC の画面で1回手動ログイン',
  },
  {
    id: 'yahoo-coupon-rotate',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'Yahoo!ストアクーポンの定期入れ替え (期限7日前に自動再発行。作らない日も走る)',
    where: 'miniPC TaskScheduler [YahooCouponRotate]',
    schedule: '毎日 09:30',
    anchor_hour_jst: 9,
    anchor_minute_jst: 30,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'AI_reference『Yahooストアクーポン自動入れ替え_20260731.md』。2FAならYahoo-Relogin.bat',
  },
  {
    id: 'rakuten-coupon-rotate',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: '楽天ストアクーポンの定期入れ替え (クーポンAPI経由)',
    where: 'miniPC TaskScheduler [RakutenStoreCouponRotate]',
    schedule: '毎日 09:35',
    anchor_hour_jst: 9,
    anchor_minute_jst: 35,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'AI_reference『楽天ストアクーポン自動入れ替え_20260731.md』',
  },
  {
    id: 'aupay-coupon-rotate',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'au PAY ストアクーポンの月次入れ替え (毎月25日に翌月分。作らない日も走る)',
    where: 'miniPC TaskScheduler [AupayCouponRotate]',
    schedule: '毎日 09:45',
    anchor_hour_jst: 9,
    anchor_minute_jst: 45,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'AI_reference『auPAYストアクーポン自動入れ替え_20260731.md』',
  },
  {
    id: 'rakuten-point-campaign',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: '商品別ポイント変倍の月次自動設定 (シートの商品に毎月1日、当月1日12:00〜月末分を設定。2〜3日はリカバリ。他の日は何もしない)',
    where: 'miniPC TaskScheduler [RakutenPointCampaign]',
    schedule: '毎日 04:20',
    anchor_hour_jst: 4,
    anchor_minute_jst: 20,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'AI_reference『楽天ポイント変倍自動設定_20260804.md』。手動再実行は apply --live --force',
  },
  {
    id: 'rankcheck-runner',
    type: 'scheduled_job',
    importance: 'P3',
    owner: '中原さん',
    purpose: '楽天検索順位の日次チェック',
    where: 'miniPC TaskScheduler [RankCheckRunner]',
    schedule: '毎日 13:00',
    anchor_hour_jst: 13,
    anchor_minute_jst: 0,
    grace_hours: 12,
    lifecycle: 'permanent',
    runbook: 'C:\\tools\\rankcheck-runner のログ確認',
  },
  {
    id: 'product-idea-scout',
    type: 'scheduled_job',
    importance: 'P3',
    owner: '中原さん',
    purpose: '新商品企画スカウト (Keepaで月販50+のASIN詳細を収集。冪等・全件取得済みなら即終了)',
    where: 'miniPC TaskScheduler [ProductIdeaScout]',
    schedule: '毎日 14:00 (19時間で自主中断→翌日続きから。Task Scheduler の上限20hは保険)',
    anchor_hour_jst: 14,
    anchor_minute_jst: 0,
    grace_hours: 22, // 14:00開始 + 自主中断19h = 翌09:00終了。翌12:00までに実行報告が無ければ締切超過
    // Keepaのトークン補充律速で数日〜数週間かかることがある (14,534 ASIN を 7,200/日)。
    // 未完走7回までは許容し、8回目で「進んでいないのでは」と疑う
    partial_max_days: 7,
    lifecycle: 'permanent',
    runbook: 'C:\\Users\\bfaith\\product-idea-scout\\data\\products.log を確認 (残件は ping の note にも出る)。2度の停止事故の教訓で毎日実行化 (2026-08-01)',
  },
  {
    id: 'fba-tracking-input',
    type: 'scheduled_job',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'FBA納品の追跡番号をAmazonへ自動投入 (福山通運の出荷実績CSV → SP-API)。Seller Centralへの手入力を廃止',
    where: 'miniPC TaskScheduler [FbaTrackingInput] → scripts\\run-fba-tracking.bat',
    schedule: '毎日 22:00',
    anchor_hour_jst: 22,
    anchor_minute_jst: 0,
    // 🚨SP-APIは「当日23:59 JSTまで」しか受け付けない (2026-08-07 実測。status ではなく期限が効く)。
    // 22:00に走らなければ猶予は2時間しかないので、締切は短く取って早く気付く。
    grace_hours: 2,
    lifecycle: 'permanent',
    runbook:
      '① GChatの通知内容を見る (中断理由がそのまま書いてある)。' +
      '② 箱数と伝票枚数の不一致なら、いろはからの箱数と納品プランを突き合わせる。' +
      '③ 当日中に直せなければ翌日 Seller Central の「追跡情報の入力」から手入力する ' +
      '(画面は期限後でも受け付けることを確認済み)。' +
      '④ 手動再実行 = miniPCで scripts\\run-fba-tracking.bat (引数なし=プレビュー / --commit=本番)。' +
      'CSVは共有ドライブ 福山通運_istar2_upload\\追跡出力\\fukutsu_tuiseki.csv',
  },

  // ─────────────── human_obligation (人がやる期限つき作業) ───────────────
  // ─────────────── scheduled_job (Render 内 node-cron) ───────────────
  {
    id: 'ph-ne-intake',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'NE新商品 → product-hub ドラフト自動作成 (Notionカード自動作成停止後の新商品の入口)',
    where: 'Render bfaith-portal 内 node-cron (apps/product-hub/intake-cron.js、PH_INTAKE_CRON_ENABLED=1)',
    schedule: '毎日 09:30 (+10:30/12:00 に再実行 — daily-sync が retry で遅れた日も当日中に拾う)',
    anchor_hour_jst: 9,
    anchor_minute_jst: 30,
    grace_hours: 3,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「intake」を検索。mirror_too_small/mirror_empty = daily-sync 未完か同期途中 (miniPC側を確認)。'
      + '手動実行 = product-hub 一覧 (admin) の「NE取込を今すぐ実行」。2026-08-05 点火 (初回はシードのみ・翌日から自動作成)',
  },
  {
    id: 'product-links-reconcile',
    type: 'scheduled_job',
    importance: 'P3',
    owner: '中原さん',
    purpose: '商品リンク台帳 (/apps/product-links) の夜間照合。product-hub の画像フォルダURL・Canva は保存時に同一トランザクションで台帳へ写る (これが正)。'
      + 'この cron はフックが落ちた分の自己修復 + ドラフト削除分の由来外し。止まっても即障害ではないが、台帳と product-hub のズレが直らなくなる',
    where: 'Render bfaith-portal 内 node-cron (apps/product-links/cron.js。既定 ON、PRODUCT_LINKS_RECONCILE_ENABLED=false で停止)',
    schedule: '毎日 09:45 JST (ph-ne-intake 09:30 の後)。起動時に台帳が空なら初回バックフィルも同じ処理',
    anchor_hour_jst: 9,
    anchor_minute_jst: 45,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「product-links」を検索。手動実行 = /apps/product-links (admin) の「商品登録ハブから照合」。'
      + '正本 = AI_reference『システム設計/商品リンク台帳_要件定義_20260827.md』',
  },
  // ⭐2026-08-05 追加分 — 2026-08-01 の棚卸しは miniPC Task Scheduler だけが対象で、
  //   Render 内の node-cron / 常駐ワーカーはカテゴリごと台帳から漏れていた。
  //   同時に、これらが miniPC でも二重起動していたため Render 専用ガードを入れている
  //   (ping も Render 側からしか飛ばない)。
  {
    id: 'fba-daily-sync',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'FBA SKUマッピング同期 (Sheets「商品コード変換テーブル」→ sku_mapping + 他CH売上スナップショット)'
      + ' + 土台商品マスタ + 納品実績。補充計算の土台なので、止まると計算が古いマッピングのまま静かにズレる',
    where: 'Render bfaith-portal 内 node-cron (apps/fba-replenishment/router.js)',
    schedule: '毎日 06:00',
    anchor_hour_jst: 6,
    anchor_minute_jst: 0,
    grace_hours: 6,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「FBA-Cron」を検索。ok の基準はSKUマッピング同期の成否 (土台/納品実績は best-effort で note に出る)。'
      + 'GOOGLE_SERVICE_ACCOUNT_KEY 未設定/失効、Sheets の共有解除で落ちる。手動実行 = FBA在庫補充画面の同期ボタン',
  },
  {
    id: 'inbound-info-daily',
    type: 'scheduled_job',
    importance: 'P2',
    owner: '中原さん',
    purpose: '入庫情報の日次処理 (新商品追加 → 入荷予定 nefuda.csv 取得 → 値札印刷用CSV → 入荷予定リストPDF を Drive へ)。'
      + '現場が朝に印刷する紙の元データ',
    where: 'Render bfaith-portal 内 node-cron (apps/inbound-info/sync-job.js)',
    schedule: '毎日 09:00 (画面「⚙️自動実行」で変更可 — 変えたらこの台帳も直す)',
    anchor_hour_jst: 9,
    anchor_minute_jst: 0,
    // 画面設定で実行時刻を後ろへ動かせるので広めに取る (17:00 までの設定なら誤報しない)。
    // それより遅い時刻にするなら anchor_hour_jst 側を直すこと
    grace_hours: 8,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「inbound-info」を検索。ok の基準は nefuda.csv 取得と PDF 保存の両方'
      + ' (CSVが取れない日はPDFを上書きしない設計なので、CSV失敗はそのまま fail)。'
      + '手動実行 = 入庫情報管理画面の「最新の入荷予定を取得」「今すぐPDFを作成してDriveに保存」',
  },
  {
    id: 'rankcheck-csv-export',
    type: 'scheduled_job',
    importance: 'P3',
    owner: '中原さん',
    purpose: '楽天順位データの CSV 生成 → Google Drive 保存。止まっても当日業務は回るが、履歴が静かに欠ける',
    where: 'Render bfaith-portal 内 node-cron (apps/ranking-checker/scheduler.js)',
    schedule: '毎日 09:00',
    anchor_hour_jst: 9,
    anchor_minute_jst: 0,
    grace_hours: 12,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「CSV出力」を検索。順位取得そのものは miniPC 側 (rankcheck-runner) が担当 = そちらが先に止まっていないか確認',
  },
  {
    id: 'mgmt-auto-sync',
    type: 'heartbeat',
    importance: 'P3',
    owner: '中原さん',
    purpose: '売上分類別粗利の自動同期 (mirror の売上を管理会計テーブルへ取り込み + 未確定月の再計算)。'
      + '止まるとダッシュボードの数字だけが古くなる',
    where: 'Render bfaith-portal 内 setInterval (apps/mgmt-accounting/router.js、既定120分間隔)',
    schedule: '120分ごと (MGMT_AUTOSYNC_INTERVAL_MIN)',
    // 既定120分の3倍。MGMT_AUTOSYNC_INTERVAL_MIN を 200分より長くするならここも広げること
    max_age_hours: 6,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「mgmt-auto-sync」を検索。確定済み月はスキップされる (それ自体は正常)。手動実行 = 管理会計画面の再計算',
  },
  // ─────────────── heartbeat (常駐ワーカーの生存監視) ───────────────
  // 毎日決まった時刻に走るものではないので、アンカーではなく「最終生存報告からの経過」で見る。
  {
    id: 'po-email-dispatcher',
    type: 'heartbeat',
    importance: 'P1',
    owner: '中原さん',
    purpose: '発注メールの予約送信ワーカー。止まると予約したメールが送られないまま「予約済み」表示で残り、'
      + '仕入先に届いていないことに気づくのが遅れる',
    where: 'Render bfaith-portal 内 常駐ワーカー (apps/purchase-orders/email.js、60秒ごと)',
    schedule: '60秒ごと (生存 ping は1時間に1回へ間引き)',
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「po-email」を検索。ping は送信0件でも打つので、来ていない = ワーカー自体が止まっている。'
      + '送信ジョブが失敗した周期は ping を打たないので、失敗が続いた場合もここに出る。'
      + 'Render の再デプロイで復帰する。未送信の予約は queued のまま残るので復帰後に自動で送られる',
  },
  {
    id: 'warehouse-healthcheck',
    type: 'heartbeat',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'miniPC warehouse (wh.bfaith-wh.uk) の死活監視ループ。これが止まると miniPC 障害の一次検知が消える',
    where: 'Render bfaith-portal 内 node-cron (apps/warehouse/healthcheck.js、5分ごと)',
    schedule: '5分ごと (生存 ping は1時間に1回へ間引き)',
    max_age_hours: 3,
    lifecycle: 'permanent',
    runbook: 'Render Logs で「Healthcheck」を検索。⭐ここの ok は「監視ループが回っている」であって「miniPC が生きている」ではない'
      + ' (miniPC の異常はこのジョブ自身が GChat へ通知する)',
  },

  {
    id: 'yahoo-oauth-reauth',
    type: 'human_obligation',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'Yahoo!ショッピングAPI の refresh token 再認可。失効すると受注取込が止まる (2026-07-31に実際に失効)',
    where: 'VPS proxy (133.167.122.198) + ブラウザ',
    schedule: '28日ごと',
    period_hours: 28 * 24,
    warn_days: 5,
    lifecycle: 'permanent',
    runbook: 'auth-url取得→ブラウザで許可→code→token/init (project_yahoo_oauth_reauth メモリ / 2〜3分)。完了したら ping を打つ',
  },
  {
    id: 'aupay-api-key-rotation',
    type: 'human_obligation',
    importance: 'P2',
    owner: '中原さん',
    purpose: 'au PAY マーケット APIキーの月末ローテーション (miniPC .env 編集→翌朝daily-syncがVPSへ反映)',
    where: 'miniPC .env',
    schedule: '毎月末',
    period_hours: 35 * 24, // 月末±数日のゆらぎを許容
    warn_days: 5,
    lifecycle: 'permanent',
    runbook: 'project_aupay_api_key_rotation メモリ。完了したら ping を打つ',
  },

  // ─────────────── temporary_asset (期限つきの一時物) ───────────────
  {
    id: 'retired-tasks-cleanup',
    type: 'temporary_asset',
    importance: 'TMP',
    owner: '中原さん',
    purpose: '2026-08-01 に退役 (Disabled化) した7タスクの本体削除 + 退避XML (C:\\tmp\\retired-tasks-20260801) の削除',
    where: 'miniPC TaskScheduler + C:\\tmp',
    remove_by: '2026-09-01',
    lifecycle: 'temporary',
    runbook: '1ヶ月困らなかったら Unregister-ScheduledTask で削除し、このエントリも消す',
  },
  {
    id: 'aupay-coupon-handplaced-backup',
    type: 'temporary_asset',
    importance: 'TMP',
    owner: '中原さん',
    purpose: 'PR #653 マージ前に手置きしていたファイルの退避 (C:\\tmp\\aupay-coupon-handplaced-20260731)',
    where: 'miniPC C:\\tmp',
    remove_by: '2026-09-01',
    lifecycle: 'temporary',
    runbook: 'フォルダを削除し、このエントリも消す',
  },
];

/** 'YYYY-MM-DD' が実在する暦日か */
function isRealYmd(ymd) {
  const m = String(ymd || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return false;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const t = new Date(Date.UTC(y, mo - 1, d));
  return t.getUTCFullYear() === y && t.getUTCMonth() === mo - 1 && t.getUTCDate() === d;
}

/** id の一意性と必須項目をロード時に検証する (壊れた台帳で黙って監視が欠けるのを防ぐ) */
export function validateRegistry(registry = JOBS_REGISTRY) {
  const errs = [];
  const seen = new Set();
  const ID_RE = /^[a-z0-9][a-z0-9-]{1,63}$/;
  for (const e of registry) {
    const label = e.id || '(idなし)';
    if (!e.id || !ID_RE.test(e.id)) errs.push(`${label}: id が不正 (小文字英数とハイフンのみ)`);
    if (seen.has(e.id)) errs.push(`${label}: id が重複`);
    seen.add(e.id);
    if (!['scheduled_job', 'heartbeat', 'human_obligation', 'temporary_asset'].includes(e.type)) errs.push(`${label}: type が不正 (${e.type})`);
    if (!['P1', 'P2', 'P3', 'TMP'].includes(e.importance)) errs.push(`${label}: importance が不正 (${e.importance})`);
    for (const k of ['owner', 'purpose', 'runbook', 'where', 'schedule']) {
      if (e.type === 'temporary_asset' && k === 'schedule') continue; // 一時物に定期はない
      if (!e[k]) errs.push(`${label}: ${k} は必須`);
    }
    if (e.type === 'scheduled_job') {
      if (e.importance === 'TMP') errs.push(`${label}: scheduled_job の importance に TMP は使わない (一時ジョブでも P3 + temporary_asset で撤去管理)`);
      if (!(Number.isInteger(e.anchor_hour_jst) && e.anchor_hour_jst >= 0 && e.anchor_hour_jst <= 23)) {
        errs.push(`${label}: anchor_hour_jst (0〜23) は必須`);
      }
      if (e.anchor_minute_jst !== undefined
        && !(Number.isInteger(e.anchor_minute_jst) && e.anchor_minute_jst >= 0 && e.anchor_minute_jst <= 59)) {
        errs.push(`${label}: anchor_minute_jst が不正 (0〜59)`);
      }
      if (!(Number.isFinite(e.grace_hours) && e.grace_hours > 0 && e.grace_hours < 24)) {
        errs.push(`${label}: grace_hours は必須 (0より大きく24未満 — 次のアンカーを跨ぐ猶予は判定を壊す)`);
      }
      if (e.partial_max_days !== undefined
        && !(Number.isInteger(e.partial_max_days) && e.partial_max_days > 0)) {
        errs.push(`${label}: partial_max_days が不正 (正の整数。0だと partial を1回も許さない = 設定しないのと同じ)`);
      }
    }
    if (e.type !== 'scheduled_job' && e.partial_max_days !== undefined) {
      errs.push(`${label}: partial_max_days は scheduled_job だけの設定`);
    }
    if (e.type === 'heartbeat') {
      if (!(Number.isFinite(e.max_age_hours) && e.max_age_hours > 0)) {
        errs.push(`${label}: max_age_hours は必須 (正の数。ping 間隔の2倍以上を目安に)`);
      }
      if (e.anchor_hour_jst !== undefined || e.anchor_minute_jst !== undefined || e.grace_hours !== undefined) {
        errs.push(`${label}: heartbeat に anchor_hour_jst / anchor_minute_jst / grace_hours は使わない (経過時間で判定する)`);
      }
    } else if (e.max_age_hours !== undefined) {
      errs.push(`${label}: max_age_hours は heartbeat だけの設定`);
    }
    if (e.type === 'human_obligation') {
      if (!(Number.isFinite(e.period_hours) && e.period_hours > 0)) errs.push(`${label}: period_hours は必須`);
      if (e.warn_days !== undefined && !(Number.isFinite(e.warn_days) && e.warn_days > 0)) {
        errs.push(`${label}: warn_days が不正 (正の数)`);
      }
    }
    if (e.type === 'temporary_asset') {
      if (!isRealYmd(e.remove_by)) errs.push(`${label}: temporary_asset には実在する remove_by (YYYY-MM-DD) が必須`);
      if (e.lifecycle !== 'temporary') errs.push(`${label}: temporary_asset の lifecycle は temporary`);
    } else if (e.lifecycle !== 'permanent') {
      errs.push(`${label}: lifecycle は permanent (一時物は temporary_asset にする)`);
    }
  }
  return errs;
}
