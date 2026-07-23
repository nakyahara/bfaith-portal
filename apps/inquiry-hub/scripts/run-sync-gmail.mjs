// Gmail (メールチャネル) 受信同期の手動/cron実行ランナー (Step 3受信)
//
// 使い方 (env: INQUIRY_GMAIL_* 優先、無ければ PO_GMAIL_* にフォールバック):
//   DATA_DIR=/data node apps/inquiry-hub/scripts/run-sync-gmail.mjs [--repair] [--backfill-days N] [--lease-minutes N]
// 初回セットアップ (shops にメール店舗行が無い場合のみ):
//   ... run-sync-gmail.mjs --create-shop info@b-faith.biz メール
//
// ⚠️ 取込前に 📧メールルール (メールディーラー振り分けCSV) を取り込んでおくこと。
//    ルール未取込だとノイズメールも全部問い合わせとして入る (skipルールで除去される前提)
// ⚠️ 本番 DATA_DIR は /data (Render)。/var/data ではない
import { initInquiryHubDB, getDB } from '../db.js';
import { runSync } from '../sync/engine.js';
import { createGmailAdapter, resolveGmailTransportFromEnv } from '../sync/adapters/gmail.js';

const args = process.argv.slice(2);
const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
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
      console.error('FATAL: --create-shop <メールアドレス> <表示名> の2引数が必要です');
      return 2;
    }
    const r = db.prepare(`INSERT OR IGNORE INTO shops (channel_type, shop_name, account_identifier)
      VALUES ('email', ?, ?)`).run(shopName, String(accountId).toLowerCase());
    console.log(r.changes ? `shops にメール店舗を作成: ${shopName} (${accountId})` : `既存: account_identifier=${accountId} (変更なし)`);
  }

  const transportCfg = resolveGmailTransportFromEnv();
  if (!transportCfg) {
    console.error('FATAL: INQUIRY_GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN (または PO_GMAIL_*) を設定してください');
    return 2;
  }

  const ruleCount = db.prepare("SELECT COUNT(*) c FROM mail_rules WHERE is_active = 1 AND action = 'skip'").get().c;
  if (ruleCount === 0) {
    console.warn('⚠️ 有効なskipメールルールが0件です。ノイズメールも全部取り込まれます (📧メールルールタブでCSVを取り込んでから実行を推奨)');
  } else {
    console.log(`メールルール: skip ${ruleCount}件が有効`);
  }

  const shops = db.prepare(`SELECT * FROM shops
    WHERE channel_type = 'email' AND is_active = 1 AND executor = 'server'`).all();
  if (shops.length === 0) {
    console.error('対象のメール店舗がありません (--create-shop で作成してください)');
    return 2;
  }

  const backfillDays = intArgOf('--backfill-days');
  const leaseMinutes = intArgOf('--lease-minutes');
  if (backfillDays === null || leaseMinutes === null) {
    console.error('FATAL: --backfill-days / --lease-minutes には正の整数を指定してください');
    return 2;
  }

  let hadFailure = false;
  for (const shop of shops) {
    const adapter = createGmailAdapter(transportCfg);
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
