/**
 * inquiry-hub 受信同期 cron (Render in-process。設計書§8.1)
 *
 * 設計原則 (rys-cron.js と同パターン):
 *   - node-cron で in-process schedule。Feature flag (INQUIRY_HUB_SYNC_CRON_ENABLED) で Dark Launch、default OFF
 *   - 通常同期: 15分間隔 (INQUIRY_HUB_SYNC_CRON で上書き可)
 *   - 深掘り同期: 日次 UTC 20:37 = JST 05:37 (INQUIRY_HUB_DEEP_CRON で上書き可)。
 *     lookback 365日で古い問い合わせへの追い返信を補完 (楽天は regDate フィルタのため。adapters/rakuten.js 参照)
 *   - 例外は飲み込む (cron 自体は throw しない。失敗は sync_errors / sync_state.consecutive_failures に記録される)
 *   - 多重起動防止: プロセス内 tickRunning ガード + エンジンの sync_state リース (BEGIN IMMEDIATE) の二段
 *   - 3連続失敗で GChat 通知 (GCHAT_WEBHOOK。未設定なら stdout のみ。fail-soft)
 *
 * 対応チャネル: 楽天 (miniPC passthrough経由) / Yahoo! (VPSプロキシ経由)。Gmail は追加時にこの tick へ組み込む。
 */
import cron from 'node-cron';
import { getDB } from '../db.js';
import { runSync } from './engine.js';
import { runOutboxPass } from '../outbox.js';
import { createRakutenAdapter, resolveRakutenTransportFromEnv, DEEP_LOOKBACK_DAYS } from './adapters/rakuten.js';
import { createYahooAdapter, resolveYahooTransportFromEnv, DEEP_LIST_LOOKBACK_DAYS } from './adapters/yahoo.js';
import { createGmailAdapter, resolveGmailTransportFromEnv, resolveRakutenMaskSmtpFromEnv } from './adapters/gmail.js';
import { sendGChatMessage } from '../../profit-analysis/gchat-client.js';
import { pingJobThrottled } from '../../jobs-monitor/ping-local.js';
import { buildCutoffNotice, cutoffNoticeCrons } from '../cutoff.js';

export const DEFAULT_SYNC_CRON = '*/15 * * * *';
// UTC 20:37 = JST 05:37。miniPC daily-sync (JST 07:00) 前の静かな時間帯で、かつ通常同期の
// 15分グリッド (:00/:15/:30/:45) と衝突しない分を選ぶ (同時発火だと tickRunning ガードで
// deep がスキップされ365日再照合が機能しない。Codex R1 high)
export const DEFAULT_DEEP_CRON = '37 20 * * *';
const FAILURE_NOTIFY_AT = 3;             // 設計§8.1: 3連続失敗で通知

let tickRunning = false;

/**
 * 店舗1件分の同期アダプターを env の transport 設定から生成する (cron tick / 運用管理画面の手動同期で共用)。
 * transport env が未設定のチャネルは null (呼び元でスキップ表示)
 */
export function buildAdapterForShop(shop, { deep = false } = {}) {
  if (shop.channel_type === 'rakuten') {
    const t = resolveRakutenTransportFromEnv();
    if (!t) return null;
    return createRakutenAdapter({
      ...t,
      expectedShopId: shop.account_identifier,
      ...(deep ? { lookbackDays: DEEP_LOOKBACK_DAYS } : {}),
    });
  }
  if (shop.channel_type === 'yahoo') {
    const t = resolveYahooTransportFromEnv();
    if (!t) return null;
    return createYahooAdapter({
      ...t,
      ...(deep ? { listLookbackDays: DEEP_LIST_LOOKBACK_DAYS } : {}),
    });
  }
  if (shop.channel_type === 'email') {
    const t = resolveGmailTransportFromEnv();
    if (!t) return null;
    // Gmail は after: がメッセージ受信日時で効くため deep (lookback) 補正は不要
    return createGmailAdapter(t);
  }
  return null;
}

/** cron式の分フィールドを展開 ('*', '*\/N', '数値', カンマ区切り のみ対応。それ以外は null=判定不能) */
function minuteSet(expr) {
  const field = String(expr).trim().split(/\s+/)[0];
  const out = new Set();
  for (const part of field.split(',')) {
    const step = part.match(/^\*\/(\d+)$/);
    if (part === '*') { for (let i = 0; i < 60; i++) out.add(i); }
    else if (step) { for (let i = 0; i < 60; i += Number(step[1])) out.add(i); }
    else if (/^\d+$/.test(part)) out.add(Number(part));
    else return null;
  }
  return out;
}

/** 2つのcron式が同じ「分」に発火し得るか (tickRunningガードでdeepが飲まれる構成の検出用) */
export function cronMinutesMayCollide(exprA, exprB) {
  const a = minuteSet(exprA), b = minuteSet(exprB);
  if (!a || !b) return false; // 判定不能な式は警告しない
  for (const m of a) if (b.has(m)) return true;
  return false;
}

/**
 * アダプターが getAuthStatus を持つ場合、shops.auth_expires_at / authentication_status を自動更新する
 * (Yahoo!: VPSプロキシの/healthがrefresh token失効日時を返す → 運用管理画面の期限警告に反映)。
 * 失敗しても同期結果には影響させない (fail-soft)
 */
export async function refreshShopAuthStatus(shop, adapter) {
  if (typeof adapter?.getAuthStatus !== 'function') return null;
  try {
    const a = await adapter.getAuthStatus();
    if (!a) return null;
    const db = getDB();
    db.prepare(`UPDATE shops SET
        auth_expires_at = COALESCE(?, auth_expires_at),
        authentication_status = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
      WHERE id = ?`)
      .run(a.authExpiresAt ? new Date(Date.parse(a.authExpiresAt)).toISOString().replace(/\.\d{3}Z$/, 'Z') : null,
        a.ok ? 'ok' : 'error', shop.id);
    return a;
  } catch (e) {
    console.warn(`[inquiry-hub-cron] 認証状態の取得失敗 (fail-soft) ${shop.shop_name}: ${e?.message || e}`);
    return null;
  }
}

/** 3連続失敗になった「ちょうどその回」だけ通知する (スパム防止)。失敗しても飲み込む */
async function maybeNotifyFailure(shop, consecutiveFailures, errorDetail) {
  if (consecutiveFailures !== FAILURE_NOTIFY_AT) return;
  const webhook = process.env.GCHAT_WEBHOOK;
  if (!webhook) return;
  try {
    await sendGChatMessage(webhook,
      `⚠️ *inquiry-hub 同期失敗*: ${shop.shop_name} (${shop.channel_type}) が ${consecutiveFailures} 回連続で失敗しています\n` +
      `直近エラー: ${String(errorDetail || '').slice(0, 300)}`);
  } catch (e) {
    console.error(`[inquiry-hub-cron] GChat通知失敗 (fail-soft): ${e?.message || e}`);
  }
}

/**
 * cron で 1 回 fire される実体。手動実行 (テスト・デバッグ) からも呼べる。
 * @param {object} opts { deep?: boolean, adapterFactory?: (shop, transportCfg, deep) => adapter (テスト用DI) }
 */
export async function runInquiryHubSyncTick(opts = {}) {
  const { deep = false, adapterFactory = null } = opts;
  if (tickRunning) {
    console.warn('[inquiry-hub-cron] 前回の tick が実行中のためスキップ');
    return { skipped: 'overlap' };
  }
  tickRunning = true;
  const t0 = Date.now();
  try {
    // チャネルごとの transport 解決。env 未設定のチャネルはスキップ (別チャネルの同期は止めない)
    const transports = {
      rakuten: resolveRakutenTransportFromEnv(),
      yahoo: resolveYahooTransportFromEnv(),
      email: resolveGmailTransportFromEnv(),
    };
    const db = getDB();
    const shops = db.prepare(`SELECT * FROM shops
      WHERE channel_type IN ('rakuten', 'yahoo', 'email') AND is_active = 1 AND executor = 'server'`).all();
    if (shops.length === 0) return { skipped: 'no_shops' };

    const results = [];
    for (const shop of shops) {
      // メールは初回バックフィルを cron にやらせない (2026-07-18 実測: d.nakahara箱は30日で2万通超
      // → 既定30日バックフィルが毎tickページ上限で失敗し、手動初回同期ともリースを取り合う)。
      // committed_until が付くまで (=初回を run-sync-gmail.mjs で明示実行するまで) はスキップ
      if (shop.channel_type === 'email') {
        const st = db.prepare('SELECT committed_until FROM sync_state WHERE shop_id = ?').get(shop.id);
        if (!st?.committed_until) {
          console.log(`[inquiry-hub-cron] SKIP ${shop.shop_name}: メールの初回同期は run-sync-gmail.mjs で実行してください (--backfill-days指定)`);
          results.push({ shopId: shop.id, shop: shop.shop_name, skipped: 'initial_backfill_required' });
          continue;
        }
      }
      const adapter = adapterFactory
        ? adapterFactory(shop, transports[shop.channel_type], deep)
        : buildAdapterForShop(shop, { deep });
      if (!adapter) {
        console.warn(`[inquiry-hub-cron] SKIP ${shop.shop_name} (${shop.channel_type}): transport env 未設定`);
        results.push({ shopId: shop.id, shop: shop.shop_name, skipped: 'no_transport' });
        continue;
      }
      const r = await runSync(shop.id, adapter);
      results.push({ shopId: shop.id, shop: shop.shop_name, ...r });
      await refreshShopAuthStatus(shop, adapter);
      if (r.ok) {
        console.log(`[inquiry-hub-cron] OK ${shop.shop_name}${deep ? ' (deep)' : ''} ` +
          `新規${r.stats.newInquiries} 新着msg${r.stats.newMessages} 再オープン${r.stats.reopened} (${Date.now() - t0}ms)`);
      } else if (r.skipped) {
        console.log(`[inquiry-hub-cron] SKIP ${shop.shop_name}: ${r.skipped}`);
      } else {
        console.error(`[inquiry-hub-cron] NG ${shop.shop_name}: ${r.error}`);
        const st = db.prepare('SELECT consecutive_failures FROM sync_state WHERE shop_id = ?').get(shop.id);
        await maybeNotifyFailure(shop, st?.consecutive_failures ?? 0, r.error);
      }
    }
    return { results };
  } catch (e) {
    console.error(`[inquiry-hub-cron] tick 失敗: ${e?.message || e}`);
    return { error: String(e?.message || e) };
  } finally {
    tickRunning = false;
  }
}

// ─── 送信ワーカー (outbox。設計書§8.3: 30秒間隔) ───

export const DEFAULT_OUTBOX_INTERVAL_MS = 30000;
let outboxRunning = false;

/** 送信アダプター一式を env から構築。送信モードはチャネル別
 * (INQUIRY_HUB_MAIL_SEND_MODE / INQUIRY_HUB_RAKUTEN_SEND_MODE。'live' 以外はすべて dryrun)。
 * INQUIRY_HUB_YAHOO_SEND_MODE も同様 */
export function buildSendAdapters() {
  const adapters = {};
  const gmailT = resolveGmailTransportFromEnv();
  if (gmailT) {
    adapters.email = createGmailAdapter({ ...gmailT,
      sendMode: process.env.INQUIRY_HUB_MAIL_SEND_MODE === 'live' ? 'live' : 'dryrun',
      // 楽天マスクアドレス (…@fw.rakuten.ne.jp) 宛だけ楽天公式SMTPで送る (2026-08-24 バウンス対策)
      rakutenSmtp: resolveRakutenMaskSmtpFromEnv() });
  }
  const rkT = resolveRakutenTransportFromEnv();
  if (rkT) {
    adapters.rakuten = createRakutenAdapter({ ...rkT,
      sendMode: process.env.INQUIRY_HUB_RAKUTEN_SEND_MODE === 'live' ? 'live' : 'dryrun' });
  }
  const yhT = resolveYahooTransportFromEnv();
  if (yhT) {
    adapters.yahoo = createYahooAdapter({ ...yhT,
      sendMode: process.env.INQUIRY_HUB_YAHOO_SEND_MODE === 'live' ? 'live' : 'dryrun' });
  }
  return adapters;
}

const dryrunPreviewedJobs = new Set(); // 同じジョブのDRYRUNログを30秒ごとに繰り返さない (プロセス内)

/**
 * DRYRUNプレビュー: pending ジョブを claim せず (状態を一切変えず)、組み立て検証とログだけ行う。
 * DRYRUNでジョブを消費すると「未送信なのに送信済み」になるため、claim自体をしない (Codexレビュー反映)
 */
async function dryrunPreviewPass(adapters) {
  const db = getDB();
  const jobs = db.prepare(`SELECT o.* FROM outbox_replies o
      JOIN inquiries i ON i.id = o.inquiry_id
      JOIN shops s ON s.id = i.shop_id
    WHERE o.status = 'pending' AND s.executor = 'server'
    ORDER BY o.id LIMIT 10`).all();
  let previewed = 0;
  for (const job of jobs) {
    if (dryrunPreviewedJobs.has(job.id)) continue;
    const adapter = adapters?.[job.channel_type];
    if (!adapter?.sendReply) continue;
    const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(job.inquiry_id);
    const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(inq.shop_id);
    try {
      // dryRun: true を明示 = アダプターがliveモードでもプレビュー経路からは絶対に実送信させない (Codexレビュー反映)
      await adapter.sendReply({ inquiry: inq, shop, bodyText: job.body_text, attachmentsJson: job.attachments_json, dryRun: true });
      console.log(`[inquiry-hub-outbox] DRYRUN #${job.id}: 組み立てOK (実送信はliveモードで)`);
    } catch (e) {
      console.warn(`[inquiry-hub-outbox] DRYRUN #${job.id}: 組み立てNG — ${String(e?.message || e).slice(0, 200)}`);
    }
    dryrunPreviewedJobs.add(job.id);
    previewed++;
  }
  return { mode: 'dryrun', previewed, pending: jobs.length };
}

/** outbox 1周 (テスト・手動実行からも呼べる)。多重実行はプロセス内ガード+ジョブ単位のリースの二段。
 * チャネル別モード: isLive=true のアダプターのジョブだけ実処理し、dryrunアダプターのチャネルは
 * claimせずプレビューのみ (状態不変)。opts.sendMode 'live' は全アダプターをlive扱い (テスト用) */
export async function runInquiryHubOutboxTick(opts = {}) {
  if (outboxRunning) return { skipped: 'overlap' };
  outboxRunning = true;
  try {
    const adapters = opts.adapters ?? buildSendAdapters();
    const forceLive = opts.sendMode === 'live';
    const liveAdapters = {}, dryrunAdapters = {};
    for (const [ch, ad] of Object.entries(adapters)) {
      if (forceLive || ad?.isLive) liveAdapters[ch] = ad;
      else dryrunAdapters[ch] = ad;
    }
    const preview = Object.keys(dryrunAdapters).length ? await dryrunPreviewPass(dryrunAdapters) : null;
    if (!Object.keys(liveAdapters).length) return preview ?? { mode: 'dryrun', previewed: 0, pending: 0 };
    const r = await runOutboxPass(liveAdapters, { executor: 'server' });
    if (r.processed > 0 || r.swept > 0) {
      console.log(`[inquiry-hub-outbox] pass: swept=${r.swept} processed=${r.processed} ` +
        r.results.map(x => `#${x.id}:${x.outcome}`).join(' '));
    }
    return preview ? { ...r, preview } : r;
  } catch (e) {
    console.error(`[inquiry-hub-outbox] pass 失敗: ${e?.message || e}`);
    return { error: String(e?.message || e) };
  } finally {
    outboxRunning = false;
  }
}

/** INQUIRY_HUB_OUTBOX_CRON_ENABLED='true'|'1' のときだけ 30秒間隔で送信ワーカーを回す (Dark Launch)。
 * 送信モードは INQUIRY_HUB_MAIL_SEND_MODE ('live' 以外はすべて dryrun = 実送信しない) */
export function startInquiryHubOutboxCron() {
  const enabled = process.env.INQUIRY_HUB_OUTBOX_CRON_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[inquiry-hub-outbox] INQUIRY_HUB_OUTBOX_CRON_ENABLED が未設定/false のため起動しない (Dark Launch)');
    return null;
  }
  const intervalMs = Number(process.env.INQUIRY_HUB_OUTBOX_INTERVAL_MS) || DEFAULT_OUTBOX_INTERVAL_MS;
  const mailMode = process.env.INQUIRY_HUB_MAIL_SEND_MODE === 'live' ? 'live' : 'dryrun';
  const rkMode = process.env.INQUIRY_HUB_RAKUTEN_SEND_MODE === 'live' ? 'live' : 'dryrun';
  const yhMode = process.env.INQUIRY_HUB_YAHOO_SEND_MODE === 'live' ? 'live' : 'dryrun';
  const timer = setInterval(() => {
    runInquiryHubOutboxTick()
      // dead-man 生存 ping (台帳 config/jobs-registry.mjs)。tick 内の個別失敗は別経路で
      // 通知されるため、ここは「ワーカーが回っていること」だけを1時間に1回報告する
      .then(() => pingJobThrottled('inquiry-hub-outbox', `送信ワーカー稼働中 (メール:${mailMode}/楽天:${rkMode}/Yahoo:${yhMode})`))
      .catch((e) => console.error('[inquiry-hub-outbox] 未捕捉例外:', e));
  }, intervalMs);
  timer.unref?.(); // プロセス終了を妨げない
  console.log(`[inquiry-hub-outbox] 送信ワーカー起動: ${intervalMs}ms間隔, モード= メール:${mailMode} / 楽天:${rkMode} / Yahoo:${yhMode} (dryrunは実送信しません)`);
  return timer;
}

/**
 * ⏰締め前通知 (2026-08-28)。ロジザードの締め (09:00 / 12:30 / 14:30 JST) の少し前に、
 * キャンセル・住所変更・日時指定の未対応をGChatへ流す。
 *
 * ⭐0件でも必ず送る — 通知が来なければ「仕組みが止まった」と気づけるようにするため
 *   (この通知そのものが dead-man を兼ねる)。
 * 既定は締めの15分前 (JST 08:45 / 12:15 / 14:15 = UTC 23:45 / 03:15 / 05:15)。
 */
// ⭐締め時刻の正本は cutoff.js の CUTOFF_TIMES。cron 式はそこから導出する
// (時刻とcronを別々に書くと、締めが変わったときに必ずずれる。Codexレビュー指摘)
export const DEFAULT_CUTOFF_CRONS = cutoffNoticeCrons();

/**
 * 締め前通知を1回送る (手動実行・テストからも呼べる)。
 * 失敗しても throw しない代わりに、**何で失敗したかを reason で返す**。
 * 呼び元は sent===true のときだけ生存 ping を打つ (Codexレビュー指摘)
 * @returns {{ sent: boolean, reason?: 'build_failed'|'no_webhook'|'send_failed', error?, text? }}
 */
export async function runCutoffNoticeTick({ webhook = process.env.GCHAT_WEBHOOK, nowMs = Date.now() } = {}) {
  const baseUrl = process.env.PUBLIC_BASE_URL || 'https://bfaith-portal.onrender.com';
  let text;
  try {
    text = buildCutoffNotice({ nowMs, baseUrl });
  } catch (e) {
    console.error(`[inquiry-hub-cutoff] ❌通知文の組み立てに失敗 (通知は出ていません): ${e?.message || e}`);
    return { sent: false, reason: 'build_failed', error: String(e?.message || e) };
  }
  if (!webhook) {
    console.warn(`[inquiry-hub-cutoff] ⚠️GCHAT_WEBHOOK 未設定のため誰にも届きません (stdout のみ):\n${text}`);
    return { sent: false, reason: 'no_webhook', text };
  }
  try {
    await sendGChatMessage(webhook, text);
    console.log('[inquiry-hub-cutoff] 締め前通知を送信しました');
    return { sent: true, text };
  } catch (e) {
    // 通知の失敗でプロセスを落とさない (次の締めでまた送る)。ping は打たないので監視には出る
    console.error(`[inquiry-hub-cutoff] ❌GChat送信失敗 (通知は届いていません): ${e?.message || e}`);
    return { sent: false, reason: 'send_failed', error: String(e?.message || e), text };
  }
}

/** INQUIRY_HUB_CUTOFF_CRON_ENABLED='true'|'1' のときだけ schedule する (Dark Launch) */
export function startInquiryHubCutoffCron() {
  const enabled = process.env.INQUIRY_HUB_CUTOFF_CRON_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[inquiry-hub-cutoff] INQUIRY_HUB_CUTOFF_CRON_ENABLED が未設定/false のためスケジュールしない (Dark Launch)');
    return null;
  }
  // カンマ区切りで上書き可 (締め時刻を変えたときはここと cutoff.js の CUTOFF_TIMES を揃える)
  const exprs = (process.env.INQUIRY_HUB_CUTOFF_CRONS || DEFAULT_CUTOFF_CRONS.join(','))
    .split(',').map(s => s.trim()).filter(Boolean);
  for (const expr of exprs) {
    if (!cron.validate(expr)) {
      console.error(`[inquiry-hub-cutoff] INQUIRY_HUB_CUTOFF_CRONS が不正: ${expr}`);
      return null;
    }
  }
  const tasks = exprs.map(expr => cron.schedule(expr, () => {
    runCutoffNoticeTick()
      // ⭐dead-man ping は「実際に送れたとき」だけ打つ (台帳 config/jobs-registry.mjs)。
      //   webhook 未設定や GChat 側の障害で誰にも届いていないのに ping を出すと、
      //   「動いているのに誰も見ていない」状態が緑のまま埋もれる (Codexレビュー指摘)。
      //   通知は1日3回なので間引かない
      .then(r => {
        if (r?.sent) return pingJobThrottled('inquiry-hub-cutoff', '締め前通知を送信', 0);
        console.warn(`[inquiry-hub-cutoff] 生存pingを送りません (理由: ${r?.reason || '不明'}) — 監視側で締切超過になります`);
        return null;
      })
      .catch((e) => console.error('[inquiry-hub-cutoff] 生存ping失敗:', e));
  }, { timezone: 'UTC' }));
  console.log(`[inquiry-hub-cutoff] 締め前通知をスケジュール: ${exprs.join(' / ')} (UTC)`);
  return tasks;
}

/** INQUIRY_HUB_SYNC_CRON_ENABLED='true'|'1' のときだけ schedule する (Dark Launch) */
export function startInquiryHubSyncCron() {
  const enabled = process.env.INQUIRY_HUB_SYNC_CRON_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[inquiry-hub-cron] INQUIRY_HUB_SYNC_CRON_ENABLED が未設定/false のためスケジュールしない (Dark Launch)');
    return null;
  }
  const syncExpr = process.env.INQUIRY_HUB_SYNC_CRON || DEFAULT_SYNC_CRON;
  const deepExpr = process.env.INQUIRY_HUB_DEEP_CRON || DEFAULT_DEEP_CRON;
  for (const [name, expr] of [['INQUIRY_HUB_SYNC_CRON', syncExpr], ['INQUIRY_HUB_DEEP_CRON', deepExpr]]) {
    if (!cron.validate(expr)) {
      console.error(`[inquiry-hub-cron] ${name} が不正: ${expr}`);
      return null;
    }
  }
  if (cronMinutesMayCollide(syncExpr, deepExpr)) {
    console.warn(`[inquiry-hub-cron] ⚠️ sync='${syncExpr}' と deep='${deepExpr}' が同じ分に発火し得ます。` +
      'tickRunningガードにより deep がスキップされる恐れがあるため、deep の分をずらしてください');
  }
  // timezone 明示 (host TZ 依存を避ける。rys-cron と同型)
  const tasks = [
    cron.schedule(syncExpr, () => {
      runInquiryHubSyncTick()
        // dead-man 生存 ping (台帳 config/jobs-registry.mjs)。店舗単位の失敗は sync_errors +
        // 3連続失敗GChat が担うので、ここは「同期cronが回っていること」だけを報告する
        .then(() => pingJobThrottled('inquiry-hub-sync', '受信同期cron稼働中 (15分間隔)'))
        .catch((e) => console.error('[inquiry-hub-cron] 未捕捉例外:', e));
    }, { timezone: 'UTC' }),
    cron.schedule(deepExpr, () => {
      runInquiryHubSyncTick({ deep: true }).catch((e) => console.error('[inquiry-hub-cron] 未捕捉例外 (deep):', e));
    }, { timezone: 'UTC' }),
  ];
  console.log(`[inquiry-hub-cron] スケジュール開始: sync='${syncExpr}' deep='${deepExpr}' (UTC)`);
  return tasks;
}
