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
import { createRakutenAdapter, resolveRakutenTransportFromEnv, DEEP_LOOKBACK_DAYS } from './adapters/rakuten.js';
import { createYahooAdapter, resolveYahooTransportFromEnv, DEEP_LIST_LOOKBACK_DAYS } from './adapters/yahoo.js';
import { sendGChatMessage } from '../../profit-analysis/gchat-client.js';

export const DEFAULT_SYNC_CRON = '*/15 * * * *';
// UTC 20:37 = JST 05:37。miniPC daily-sync (JST 07:00) 前の静かな時間帯で、かつ通常同期の
// 15分グリッド (:00/:15/:30/:45) と衝突しない分を選ぶ (同時発火だと tickRunning ガードで
// deep がスキップされ365日再照合が機能しない。Codex R1 high)
export const DEFAULT_DEEP_CRON = '37 20 * * *';
const FAILURE_NOTIFY_AT = 3;             // 設計§8.1: 3連続失敗で通知

let tickRunning = false;

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
    };
    const db = getDB();
    const shops = db.prepare(`SELECT * FROM shops
      WHERE channel_type IN ('rakuten', 'yahoo') AND is_active = 1 AND executor = 'server'`).all();
    if (shops.length === 0) return { skipped: 'no_shops' };

    const buildAdapter = (shop) => {
      const transportCfg = transports[shop.channel_type];
      if (!transportCfg) return null;
      if (shop.channel_type === 'rakuten') {
        return createRakutenAdapter({
          ...transportCfg,
          expectedShopId: shop.account_identifier,
          ...(deep ? { lookbackDays: DEEP_LOOKBACK_DAYS } : {}),
        });
      }
      return createYahooAdapter({
        ...transportCfg,
        ...(deep ? { listLookbackDays: DEEP_LIST_LOOKBACK_DAYS } : {}),
      });
    };

    const results = [];
    for (const shop of shops) {
      const adapter = adapterFactory
        ? adapterFactory(shop, transports[shop.channel_type], deep)
        : buildAdapter(shop);
      if (!adapter) {
        console.warn(`[inquiry-hub-cron] SKIP ${shop.shop_name} (${shop.channel_type}): transport env 未設定`);
        results.push({ shopId: shop.id, shop: shop.shop_name, skipped: 'no_transport' });
        continue;
      }
      const r = await runSync(shop.id, adapter);
      results.push({ shopId: shop.id, shop: shop.shop_name, ...r });
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
      runInquiryHubSyncTick().catch((e) => console.error('[inquiry-hub-cron] 未捕捉例外:', e));
    }, { timezone: 'UTC' }),
    cron.schedule(deepExpr, () => {
      runInquiryHubSyncTick({ deep: true }).catch((e) => console.error('[inquiry-hub-cron] 未捕捉例外 (deep):', e));
    }, { timezone: 'UTC' }),
  ];
  console.log(`[inquiry-hub-cron] スケジュール開始: sync='${syncExpr}' deep='${deepExpr}' (UTC)`);
  return tasks;
}
