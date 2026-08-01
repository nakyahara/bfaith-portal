/**
 * jobs-monitor 見張りの見張り (Google Apps Script)
 *
 * なぜ必要か: 見張り役 (Render の jobs-monitor) 自身が止まったら、また無音に戻る。
 * Google のインフラは miniPC とも Render とも独立しているので、ここから
 * /apps/jobs-monitor/health を定期的に見て、死んでいたら GChat + メールで知らせる。
 *
 * セットアップ (中原さん or Claude、5分):
 *   1. script.google.com で新規プロジェクト「jobs-monitor-watchdog」を作りこのファイルを貼る
 *   2. スクリプトプロパティに GCHAT_WEBHOOK_JOBS (要対応スペースのwebhook) を設定
 *   3. checkJobsMonitorHealth を10分おきの時間主導トリガーに登録
 *   4. 一度手動実行して権限を承認
 *
 * health は認証なし・情報最小 (ok と評価時刻だけ)。トークンは持たせない。
 */

var HEALTH_URL = 'https://bfaith-portal.onrender.com/apps/jobs-monitor/health';
// 連続で何回失敗したら知らせるか (Render の再デプロイ数分を誤報にしないため 2回 = 最大20分)
var FAIL_THRESHOLD = 2;
// 同じ障害の再通知間隔 (6時間)
var REALERT_MS = 6 * 3600 * 1000;

function checkJobsMonitorHealth() {
  var props = PropertiesService.getScriptProperties();
  var fails = Number(props.getProperty('consecutive_fails') || '0');
  var healthy = false;
  var detail = '';

  try {
    var res = UrlFetchApp.fetch(HEALTH_URL, { muteHttpExceptions: true, followRedirects: true });
    var code = res.getResponseCode();
    healthy = code === 200;
    detail = 'HTTP ' + code + ' ' + res.getContentText().slice(0, 120);
  } catch (e) {
    detail = String(e).slice(0, 200);
  }

  if (healthy) {
    // 復旧通知は「alerted が立っているか」だけで判定する (fails は既にリセットされているかも
    // しれないため、条件に混ぜると再試行経路が消える — Codex R3 high)。
    // 送れたときだけ alerted を消す = 送れなければ次周期の healthy でもう一度試みる
    if (props.getProperty('alerted') === '1') {
      if (!notify_('✅ 見張り役 (jobs-monitor) は復旧しました')) {
        props.setProperty('consecutive_fails', '0');
        return; // alerted は残す
      }
      props.deleteProperty('alerted');
      props.deleteProperty('last_alert_at');
    }
    props.setProperty('consecutive_fails', '0');
    return;
  }

  fails += 1;
  props.setProperty('consecutive_fails', String(fails));
  if (fails < FAIL_THRESHOLD) return;

  var last = Number(props.getProperty('last_alert_at') || '0');
  if (Date.now() - last < REALERT_MS) return;

  var sent = notify_(
    '🚨 *見張り役 (jobs-monitor) が応答しません* (連続' + fails + '回)\n' +
    '　' + detail + '\n' +
    '　→ この間、定期実行の締切超過は検知されていません。' +
    'Render の bfaith-portal が動いているか確認してください'
  );
  // ⭐送れたときだけ「通知済み」を進める。GChatもメールも失敗したら次周期 (10分後) に再試行
  //   (成否を見ずに進めると、通知経路の全滅時に6時間沈黙する — Codex R2 high)
  if (sent) {
    props.setProperty('alerted', '1');
    props.setProperty('last_alert_at', String(Date.now()));
  }
}

/** どこかの経路で届けられたら true を返す (呼び出し側は成功時だけ通知済み状態を進める) */
function notify_(text) {
  var webhook = PropertiesService.getScriptProperties().getProperty('GCHAT_WEBHOOK_JOBS');
  var sent = false;
  if (webhook) {
    try {
      var r = UrlFetchApp.fetch(webhook, {
        method: 'post',
        contentType: 'application/json; charset=UTF-8',
        payload: JSON.stringify({ text: text }),
        muteHttpExceptions: true,
      });
      // muteHttpExceptions だと 401/500 でも例外にならない — 2xx を確認してから成功扱いにする
      // (webhook失効時に「送れたつもり」でメールに落ちない事故の防止)
      sent = r.getResponseCode() >= 200 && r.getResponseCode() < 300;
    } catch (e) { /* メールにフォールバック */ }
  }
  // GChat 自体が死んでいる事故に備えた第2経路
  if (!sent) {
    try {
      MailApp.sendEmail(Session.getActiveUser().getEmail(), '[jobs-monitor] 見張り役ダウン', text);
      sent = true;
    } catch (e) { /* これ以上は打つ手なし */ }
  }
  return sent;
}
