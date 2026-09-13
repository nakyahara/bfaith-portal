/**
 * lib-notify.mjs — モールCSV自動取得の実行ログ保存 + GChatエラー通知 (mall-csv-fetcher)
 *
 * 方針 (無音停止禁止の原則):
 *   - console 出力を logs/<mall>-<ts>.log にも tee 保存 (DOMダンプ等の診断情報を後からAIが読める)
 *   - エラー時のみ GChat へ「AI調査用レポート」を送信 (成功時は通知しない)
 *   - 通知自体は fail-soft: webhook 未設定/送信失敗でも本体処理の成否を変えない
 *     (終端マーカー [NOTIFY:status=sent|skipped|failed] を出す — feedback_notify_status_terminal_marker)
 *   - tee 時に認証情報 env 値をマスク (万一 value が echo されても残さない)
 *
 * env:
 *   GCHAT_WEBHOOK_MALL_FETCH  通知先 (無ければ GCHAT_WEBHOOK にフォールバック)
 */
import fs from 'node:fs';
import os from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import './lib-env.mjs'; // 設定はリポジトリ直下の .env だけ (lib-env.mjs)

const __dirname = dirname(fileURLToPath(import.meta.url));
export const LOG_DIR = join(__dirname, 'logs');

const SECRET_ENV_KEYS = ['RMS_RLOGIN_ID', 'RMS_RLOGIN_PW', 'RMS_MEMBER_ID', 'RMS_MEMBER_PW'];

function maskSecrets(line) {
  let out = line;
  for (const k of SECRET_ENV_KEYS) {
    const v = process.env[k];
    if (v && v.length >= 4) out = out.split(v).join('***');
  }
  return out;
}

/**
 * console.log/warn/error を logs/<name>-<ts>.log に tee する。
 * 戻り値: { logPath, close() }。close() は flush のみ (console は戻さない — プロセス終了前提)
 */
export function initRunLog(name) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const logPath = join(LOG_DIR, `${name}-${ts}.log`);
  // appendFileSync: process.exit 直前の行も取りこぼさない (stream flush 待ち不要)。ログ量は小さい
  const wrap = (orig) => (...args) => {
    orig(...args);
    try {
      const line = args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
      fs.appendFileSync(logPath, maskSecrets(line) + '\n');
    } catch { /* tee失敗で本体を止めない */ }
  };
  console.log = wrap(console.log.bind(console));
  console.warn = wrap(console.warn.bind(console));
  console.error = wrap(console.error.bind(console));
  console.log(`[log] 実行ログ: ${logPath}`);
  return { logPath };
}

/** 通知先: GCHAT_WEBHOOK_MALL_FETCH → GCHAT_WEBHOOK (どちらもリポジトリ直下の .env から lib-env.mjs が読む)。
 *  miniPC では「⚠️ Warehouse日次同期」と同じスペースに届き、webhook のローテも直下 .env の 1 か所で済む */
function resolveWebhook() {
  return process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK || null;
}

/** GChat へテキスト送信 (fail-soft)。GChat の text 上限 4096 に収める */
export async function sendGChat(text, label = 'mall-csv-fetcher') {
  const webhook = resolveWebhook();
  if (!webhook) {
    console.warn(`[${label}] [NOTIFY:status=skipped] GCHAT_WEBHOOK 未設定のため通知スキップ (env または リポジトリ直下.env)`);
    return false;
  }
  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: String(text).slice(0, 4000) }),
    });
    if (!res.ok) {
      console.error(`[${label}] [NOTIFY:status=failed] GChat HTTP ${res.status}`);
      return false;
    }
    console.log(`[${label}] [NOTIFY:status=sent]`);
    return true;
  } catch (e) {
    console.error(`[${label}] [NOTIFY:status=failed] ${e.message}`);
    return false;
  }
}

const jstStamp = () => new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' JST';

/**
 * AI調査用エラーレポートを組み立てる。
 * failures: [{ reportType, ym, from, to, error, url?, screenshot? }]
 * outcomes: [{ spec, ym, status }] (成功/empty含む全結果)
 */
export function buildErrorReport({ mall, outcomes = [], failures = [], logPath = '', repro = '' }) {
  const L = [];
  // 「⚠️ Warehouse日次同期」と同じスペース・同じ見た目の流儀 (⚠️ prefix)
  L.push(`⚠️ *モールCSV自動取得エラー (${mall}) ${jstStamp()}*`);
  if (outcomes.length) {
    L.push(`結果: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ')}`);
  }
  for (const f of failures.slice(0, 5)) {
    L.push('');
    L.push(`❌ ${f.reportType} ${f.ym || ''} (${f.from || '?'}〜${f.to || '?'})`);
    L.push(String(f.error || '(no message)').slice(0, 400));
    if (f.url) L.push(`url: ${f.url}`);
    if (f.screenshot) L.push(`screenshot: ${f.screenshot}`);
  }
  if (failures.length > 5) L.push(`…他 ${failures.length - 5} 件 (ログ参照)`);
  L.push('');
  L.push('[AI調査ガイド]');
  L.push(`- host: ${os.hostname()} / 実行ログ(DOMダンプ・[date]候補・[poll]履歴行入り): ${logPath || '(なし)'}`);
  if (repro) L.push(`- 再現: ${repro}`);
  L.push('- 切り分け: FORM_VERIFY=画面DOM変化(ログの[DOM:reports-form-*]を確認) / HISTORY_TIMEOUT=生成遅延or履歴表記変化([poll]行を確認、RPP_LOOSE_MATCH=1で緩和可) / 2FA_REQUIRED=信頼端末切れ(MANUAL=1で手動再ログイン+信頼端末登録) / RMS_SESSION_UNSTABLE=ログインしてもセッション即失効(楽天側の利用規制/障害。連打防止で中止)');
  L.push('- 無停止手順: 管理画面から手動DL→incoming/ に置くだけ (次回daily-syncが取込)');
  // fetch-all の初回パスから呼ばれた場合はリトライ予告 (受け手が慌てて手動DLしないように)。
  // ただし blocked系 (2FA/セッション不安定/env不備) だけの通知には載せない — リトライされないのに
  // 「待てば直る」と誤読させる矛盾を防ぐ (Codex R1)
  const BLOCKED_RE = /2FA_REQUIRED|RMS_SESSION_UNSTABLE|AUTH_FAILED|ENV_MISSING/;
  const hasRetryable = failures.some((f) => !BLOCKED_RE.test(`${f.reportType || ''} ${f.error || ''}`));
  if (process.env.MALL_FETCH_WILL_RETRY && hasRetryable) {
    L.push(`- ♻️ 業務エラー分は約${process.env.MALL_FETCH_WILL_RETRY}分後に fetch-all が自動リトライします (回復すれば「リトライで回復」を通知。2FA_REQUIRED/RMS_SESSION_UNSTABLE/env不備は対象外)`);
  }
  return L.join('\n');
}
