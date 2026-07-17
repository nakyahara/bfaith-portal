// Yahoo! 受信同期の手動/cron実行ランナー (Step 2)
//
// 使い方 (VPSプロキシ経由。固定IP制限のためYahoo!直叩きはできない):
//   DATA_DIR=/data YAHOO_PROXY_URL=... YAHOO_PROXY_SECRET=... \
//     node apps/inquiry-hub/scripts/run-sync-yahoo.mjs [--deep] [--repair] [--backfill-days N] [--lookback-days N]
// 初回セットアップ (shops にYahoo!店舗行が無い場合のみ):
//   ... run-sync-yahoo.mjs --create-shop <セラーID> <店舗名>   (セラーID実測値: b-faith01)
//
// --deep            : 一覧スキャン幅を365日に広げる (古いトピックへの店舗側更新を補完。日次想定)
// --lookback-days N : スキャン幅の明示指定 (--deep より優先)
// --repair          : エンジンの修復同期 (直近3日を強制再照合。§8.1)
// --lease-minutes N : 同期リースの延長 (既定10分。全履歴バックフィルは2,432件×1.1秒≈45分かかるため
//                     初回のみ例: --backfill-days 3650 --lookback-days 3650 --lease-minutes 90)
// ⚠️ 本番 DATA_DIR は /data (Render)。/var/data ではない
import { initInquiryHubDB, getDB } from '../db.js';
import { runSync } from '../sync/engine.js';
import { createYahooAdapter, resolveYahooTransportFromEnv, DEEP_LIST_LOOKBACK_DAYS } from '../sync/adapters/yahoo.js';

const args = process.argv.slice(2);
const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
/** 数値フラグの厳格検証: 指定があるのに正の整数でなければ null を返す (呼び元で exit 2) */
const intArgOf = f => {
  if (!args.includes(f)) return undefined;
  const n = Number(argOf(f));
  return Number.isInteger(n) && n > 0 ? n : null;
};

// ⚠️ process.exit() は使わない (node24 + better-sqlite3 で libuv assertion クラッシュ実測)
async function main() {
  if (!process.env.DATA_DIR) {
    console.error('FATAL: DATA_DIR が未指定です (本番=/data)');
    return 2;
  }
  initInquiryHubDB();
  const db = getDB();

  if (args.includes('--create-shop')) {
    const i = args.indexOf('--create-shop');
    const accountId = args[i + 1], shopName = args[i + 2];
    if (!accountId || !shopName) {
      console.error('FATAL: --create-shop <セラーID> <店舗名> の2引数が必要です');
      return 2;
    }
    const r = db.prepare(`INSERT OR IGNORE INTO shops (channel_type, shop_name, account_identifier)
      VALUES ('yahoo', ?, ?)`).run(shopName, String(accountId));
    console.log(r.changes ? `shops にYahoo!店舗を作成: ${shopName} (${accountId})` : `既存: account_identifier=${accountId} (変更なし)`);
  }

  const transportCfg = resolveYahooTransportFromEnv();
  if (!transportCfg) {
    console.error('FATAL: YAHOO_PROXY_URL / YAHOO_PROXY_SECRET を設定してください (VPSプロキシ経由が必須)');
    return 2;
  }

  const shops = db.prepare(`SELECT * FROM shops
    WHERE channel_type = 'yahoo' AND is_active = 1 AND executor = 'server'`).all();
  if (shops.length === 0) {
    console.error('対象のYahoo!店舗がありません (--create-shop で作成してください)');
    return 2;
  }

  const backfillDays = intArgOf('--backfill-days');
  const lookbackDays = intArgOf('--lookback-days');
  const leaseMinutes = intArgOf('--lease-minutes');
  if (backfillDays === null || lookbackDays === null || leaseMinutes === null) {
    console.error('FATAL: --backfill-days / --lookback-days / --lease-minutes には正の整数を指定してください');
    return 2;
  }

  let hadFailure = false;
  for (const shop of shops) {
    const adapter = createYahooAdapter({
      ...transportCfg,
      ...(lookbackDays != null ? { listLookbackDays: lookbackDays }
        : args.includes('--deep') ? { listLookbackDays: DEEP_LIST_LOOKBACK_DAYS } : {}),
    });
    const t0 = Date.now();
    const r = await runSync(shop.id, adapter, {
      repair: args.includes('--repair'),
      ...(backfillDays != null ? { backfillDays } : {}),
      ...(leaseMinutes != null ? { leaseMinutes } : {}),
    });
    const ms = Date.now() - t0;
    if (r.ok) {
      console.log(`OK ${shop.shop_name}: 対象${r.stats.inquiries}件 新規${r.stats.newInquiries} 新着msg${r.stats.newMessages} 再オープン${r.stats.reopened} (${ms}ms)`);
    } else if (r.skipped) {
      console.log(`SKIP ${shop.shop_name}: ${r.skipped}${r.skipped === 'lease' ? ' (別ジョブが同期中)' : ''}`);
    } else {
      hadFailure = true;
      console.error(`NG ${shop.shop_name}: ${r.error}`);
    }
    const st = db.prepare('SELECT committed_until, consecutive_failures FROM sync_state WHERE shop_id = ?').get(shop.id);
    if (st) console.log(`   committed_until=${st.committed_until} consecutive_failures=${st.consecutive_failures}`);
  }
  return hadFailure ? 1 : 0;
}

process.exitCode = await main();
