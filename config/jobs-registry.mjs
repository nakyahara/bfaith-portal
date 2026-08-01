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
  // ─────────────── scheduled_job (miniPC Task Scheduler) ───────────────
  {
    id: 'mall-csv-fetch-all',
    type: 'scheduled_job',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'モール統計CSVの自動取得 (楽天RPP/データ/レビュー・Yahoo・auPAY・Qoo10)。分析データの入口',
    where: 'miniPC TaskScheduler [MallCsvFetchAll]',
    schedule: '毎日 05:30 (失敗モールは25分後に1回リトライ)',
    anchor_hour_jst: 5,
    anchor_minute_jst: 30,
    grace_hours: 5,
    lifecycle: 'permanent',
    runbook: 'scripts/mall-csv-fetcher/logs/fetch-all-*.log を確認。認証切れなら各モールの再ログイン手順 (Yahoo=デスクトップのYahoo-Relogin.bat)',
  },
  {
    id: 'warehouse-daily-sync',
    type: 'scheduled_job',
    importance: 'P1',
    owner: '中原さん',
    purpose: 'モール受注取込→fact build→DQ→Render mirror 同期 (約43ステップ)。全業務データの土台',
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
    schedule: '毎日 14:00 (最長20時間で打ち切り→翌日続きから)',
    anchor_hour_jst: 14,
    anchor_minute_jst: 0,
    grace_hours: 21, // 実行上限20hのため。翌日11:00までに ok が無ければ締切超過
    lifecycle: 'permanent',
    runbook: 'C:\\Users\\bfaith\\product-idea-scout\\data\\products.log を確認。2度の停止事故の教訓で毎日実行化 (2026-08-01)',
  },

  // ─────────────── human_obligation (人がやる期限つき作業) ───────────────
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
    if (!['scheduled_job', 'human_obligation', 'temporary_asset'].includes(e.type)) errs.push(`${label}: type が不正 (${e.type})`);
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
