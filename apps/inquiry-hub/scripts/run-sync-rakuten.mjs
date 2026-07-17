// 楽天 受信同期の手動/cron実行ランナー (Step 2)
//
// 使い方 (transportは環境変数から自動判別):
//   Render (楽天キーを置かない設計原則 → miniPC passthrough 経由。WAREHOUSE_URL等はRYS/mercariで設定済み):
//     DATA_DIR=/data node apps/inquiry-hub/scripts/run-sync-rakuten.mjs [--deep] [--repair] [--backfill-days N] [--lookback-days N]
//   miniPC/直接 (RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY がある環境):
//     DATA_DIR=... RAKUTEN_SERVICE_SECRET=... RAKUTEN_LICENSE_KEY=... node ...
// 初回セットアップ (shops に楽天店舗行が無い場合のみ):
//   ... run-sync-rakuten.mjs --create-shop <楽天shopId> <店舗名>
//
// --deep            : lookback を365日に広げて再照合 (60日より古い問い合わせへの追い返信を補完。日次想定)
// --lookback-days N : lookback 幅の明示指定 (--deep より優先。365日超の追い返信を拾いたい場合等)
// --repair          : エンジンの修復同期 (直近3日を強制再照合。§8.1)
// ⚠️ 認証情報は環境の1組のみ = 楽天1店舗運用が前提。複数店舗にする場合は店舗ごとの
//    認証情報管理 (shops拡張) が必要 (レスポンスshopId照合はアダプターが行う)
// ⚠️ 本番 DATA_DIR は /data (Render)。/var/data ではない
import { initInquiryHubDB, getDB } from '../db.js';
import { runSync } from '../sync/engine.js';
import { createRakutenAdapter, DEEP_LOOKBACK_DAYS } from '../sync/adapters/rakuten.js';

const args = process.argv.slice(2);
const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
/** 数値フラグの厳格検証: 指定があるのに正の整数でなければ null を返す (呼び元で exit 2) */
const intArgOf = f => {
  if (!args.includes(f)) return undefined;
  const raw = argOf(f);
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
};

// ⚠️ process.exit() は使わない: better-sqlite3 のハンドルが残った状態での強制終了は
// node 24 (miniPC実測) で libuv assertion クラッシュになり、終了コードが化ける。
// process.exitCode を設定してイベントループの自然終了に任せる
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
      console.error('FATAL: --create-shop <楽天shopId> <店舗名> の2引数が必要です');
      return 2;
    }
    const r = db.prepare(`INSERT OR IGNORE INTO shops (channel_type, shop_name, account_identifier)
      VALUES ('rakuten', ?, ?)`).run(shopName, String(accountId));
    console.log(r.changes ? `shops に楽天店舗を作成: ${shopName} (${accountId})` : `既存: account_identifier=${accountId} (変更なし)`);
  }

  // transport 自動判別: 楽天キーがあれば direct、なければ miniPC passthrough (warehouse)
  const { RAKUTEN_SERVICE_SECRET, RAKUTEN_LICENSE_KEY,
    WAREHOUSE_URL, WAREHOUSE_SERVICE_TOKEN, CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET } = process.env;
  let transportCfg;
  if (RAKUTEN_SERVICE_SECRET && RAKUTEN_LICENSE_KEY) {
    transportCfg = { transport: 'direct', serviceSecret: RAKUTEN_SERVICE_SECRET, licenseKey: RAKUTEN_LICENSE_KEY };
    console.log('transport=direct (RMS直接)');
  } else if (WAREHOUSE_URL && WAREHOUSE_SERVICE_TOKEN && CF_ACCESS_CLIENT_ID && CF_ACCESS_CLIENT_SECRET) {
    transportCfg = { transport: 'warehouse', warehouseUrl: WAREHOUSE_URL, serviceToken: WAREHOUSE_SERVICE_TOKEN,
      cfClientId: CF_ACCESS_CLIENT_ID, cfClientSecret: CF_ACCESS_CLIENT_SECRET };
    console.log('transport=warehouse (miniPC passthrough 経由)');
  } else {
    console.error('FATAL: 認証情報がありません。以下のどちらかを設定してください:');
    console.error('  direct:    RAKUTEN_SERVICE_SECRET + RAKUTEN_LICENSE_KEY');
    console.error('  warehouse: WAREHOUSE_URL + WAREHOUSE_SERVICE_TOKEN + CF_ACCESS_CLIENT_ID + CF_ACCESS_CLIENT_SECRET');
    return 2;
  }

  const shops = db.prepare(`SELECT * FROM shops
    WHERE channel_type = 'rakuten' AND is_active = 1 AND executor = 'server'`).all();
  if (shops.length === 0) {
    console.error('対象の楽天店舗がありません (--create-shop で作成してください)');
    return 2;
  }

  const backfillDays = intArgOf('--backfill-days');
  const lookbackDays = intArgOf('--lookback-days');
  if (backfillDays === null || lookbackDays === null) {
    console.error('FATAL: --backfill-days / --lookback-days には正の整数を指定してください');
    return 2;
  }

  let hadFailure = false;
  for (const shop of shops) {
    // 店舗ごとにアダプター生成: レスポンス shopId と account_identifier を照合し他店舗データの混入を防ぐ
    const adapter = createRakutenAdapter({
      ...transportCfg,
      expectedShopId: shop.account_identifier,
      ...(lookbackDays != null ? { lookbackDays }
        : args.includes('--deep') ? { lookbackDays: DEEP_LOOKBACK_DAYS } : {}),
    });
    const t0 = Date.now();
    const r = await runSync(shop.id, adapter, {
      repair: args.includes('--repair'),
      ...(backfillDays != null ? { backfillDays } : {}),
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
