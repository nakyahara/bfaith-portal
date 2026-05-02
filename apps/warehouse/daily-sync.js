/**
 * daily-sync.js — 日次データ同期 + Google Chat通知
 *
 * 毎朝タスクスケジューラから実行。
 * SP-API（Amazon 7日分）と楽天RMS API（7日分）のデータを取得し、
 * 結果をGoogle Chatに通知する。
 */
import 'dotenv/config';
import { execSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');

const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK || 'https://chat.googleapis.com/v1/spaces/AAQAL5zHy-w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=yER7IJx_9CkKhYnzzre0WcWuqfgXc1oh8ldR35k01zE';

async function notify(text) {
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) {
    console.error('[通知エラー]', e.message);
  }
}

function runScript(scriptPath, label, timeoutMs = 600000) {
  const parts = scriptPath.split(' ');
  const filePath = path.join(PROJECT_DIR, parts[0]);
  const args = parts.slice(1).join(' ') || '7';
  console.log(`\n=== ${label} ===`);
  try {
    const output = execSync(`node "${filePath}" ${args}`, {
      cwd: PROJECT_DIR,
      timeout: timeoutMs,
      encoding: 'utf-8',
      env: {
        ...process.env,
        PATH: process.env.PATH,
        // 子プロセス側のSQLite busy_timeout を 60秒 に上書き（バッチ用）。
        // db.js の initDB() がこの env を参照する。
        WAREHOUSE_DB_BUSY_TIMEOUT_MS: process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS || '60000',
      },
    });
    console.log(output);
    // 最後の行から結果を抽出
    const lines = output.trim().split('\n');
    const lastLine = lines[lines.length - 1] || '';
    return { success: true, summary: lastLine };
  } catch (e) {
    console.error(`[${label}] エラー:`, e.message);
    return { success: false, summary: e.message.slice(0, 200) };
  }
}

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

async function main() {
  const startTime = new Date();
  // JST 固定の業務日付。子プロセスへ env で引き回す (UTC癖回避)
  const businessDate = toJstDate(startTime);
  process.env.WAREHOUSE_BUSINESS_DATE = businessDate;
  const dateStr = businessDate;
  console.log(`[DailySync] 開始: ${startTime.toISOString()} (business_date=${businessDate})`);

  const results = [];

  // NE API（商品マスタ + セット商品 + 受注7日分）
  const neResult = runScript('apps/warehouse/ne-api.js sync', 'NE API');
  results.push({ name: 'NE', ...neResult });

  // NE 取得成功直後に raw_ne_products → ne_stock_daily_snapshot へ複製 (履歴化)
  // ここで失敗しても後続は続ける (snapshot は付加的、欠損日は翌日また取れる)
  if (neResult.success) {
    const snapResult = runScript('apps/warehouse/snapshot-ne-stock.js', 'NE在庫スナップショット', 60000);
    results.push({ name: 'NE在庫snapshot', ...snapResult });
  } else {
    console.log('[DailySync] NE API 失敗のため在庫スナップショットをスキップ');
  }

  // SP-API
  const spResult = runScript('apps/warehouse/sp-api-orders.js', 'Amazon SP-API');
  results.push({ name: 'Amazon', ...spResult });

  // FBA 在庫スナップショット (RESTOCK + PLANNING) — daily_snapshots に履歴蓄積
  // 手動 /fetch-reports と lockfile で排他、既に走ってればスキップ (失敗扱いにしない)
  // SP-API レポート polling のため最大 15 分余裕
  const fbaSnapResult = runScript('apps/warehouse/snapshot-fba-stock.js', 'FBA在庫スナップショット', 900000);
  results.push({ name: 'FBA在庫snapshot', ...fbaSnapResult });

  // 楽天
  const rkResult = runScript('apps/warehouse/rakuten-orders.js', '楽天 RMS API');
  results.push({ name: '楽天', ...rkResult });

  // Yahoo!ショッピング（VPSプロキシ経由で遅延しやすいため60分）
  const yahooResult = runScript('apps/warehouse/yahoo-orders.js 7', 'Yahoo!ショッピング', 3600000);
  results.push({ name: 'Yahoo', ...yahooResult });

  // au PAY Market
  const aupayResult = runScript('apps/warehouse/mall-orders.js aupay', 'au PAY');
  results.push({ name: 'au PAY', ...aupayResult });

  // Qoo10
  const qoo10Result = runScript('apps/warehouse/mall-orders.js qoo10', 'Qoo10');
  results.push({ name: 'Qoo10', ...qoo10Result });

  // LINEギフト
  const linegiftResult = runScript('apps/warehouse/mall-orders.js linegift', 'LINEギフト');
  results.push({ name: 'LINEギフト', ...linegiftResult });

  // 統合商品マスタ再構築
  const mProductResult = runScript('apps/warehouse/rebuild-m-products.js', 'm_products 再構築');
  results.push({ name: 'm_products', ...mProductResult });

  // 販売集計テーブル再構築
  const fSalesResult = runScript('apps/warehouse/rebuild-f-sales.js', 'f_sales 再構築');
  results.push({ name: 'f_sales', ...fSalesResult });

  // 楽天 AM/AL/W → NE商品コード sku_map 再構築
  const rakutenSkuMapResult = runScript('apps/warehouse/rebuild-rakuten-sku-map.js', '楽天 sku_map 再構築');
  results.push({ name: '楽天sku_map', ...rakutenSkuMapResult });

  // Renderにミラーデータ送信
  const syncResult = runScript('apps/warehouse/sync-to-render.js', 'Render同期');
  results.push({ name: 'Render同期', ...syncResult });

  const endTime = new Date();
  const duration = Math.round((endTime - startTime) / 1000);

  // ─── APIトークン・認証情報 期限チェック ───
  const tokenWarnings = [];

  // Yahoo — VPSプロキシのhealthから確認
  try {
    const yahooProxyUrl = process.env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080';
    const yahooProxySecret = process.env.YAHOO_PROXY_SECRET || process.env.AUPAY_PROXY_SECRET || '';
    const healthRes = await fetch(`${yahooProxyUrl}/yahoo/health`, {
      headers: { 'X-Proxy-Secret': yahooProxySecret },
    });
    const health = await healthRes.json();
    if (health.refreshTokenExpiresAt) {
      const expiry = new Date(health.refreshTokenExpiresAt);
      const daysLeft = Math.floor((expiry - new Date()) / 86400000);
      if (daysLeft <= 3) {
        tokenWarnings.push(`🔴 Yahoo refresh token 残り${daysLeft}日（${expiry.toISOString().slice(0, 10)}）→ 再認可が必要！`);
      } else if (daysLeft <= 7) {
        tokenWarnings.push(`🟡 Yahoo refresh token 残り${daysLeft}日（${expiry.toISOString().slice(0, 10)}）`);
      }
    } else if (health.tokenExpiry) {
      tokenWarnings.push('🟡 Yahoo refresh token 期限不明（次回認可時に記録されます）');
    }
    if (!health.hasTokens) {
      tokenWarnings.push('🔴 Yahoo トークン未初期化 → 認可が必要！');
    }
  } catch (e) {
    tokenWarnings.push(`⚠️ Yahoo プロキシ接続失敗: ${e.message.slice(0, 100)}`);
  }

  // NE（ネクストエンジン） — ne-tokens.jsonの更新日から判定（2日以内に更新必要）
  try {
    const neTokenPath = path.join(PROJECT_DIR, 'data', 'ne-tokens.json');
    const fs = await import('fs');
    if (fs.existsSync(neTokenPath)) {
      const stat = fs.statSync(neTokenPath);
      const hoursSinceUpdate = (Date.now() - stat.mtimeMs) / 3600000;
      if (hoursSinceUpdate > 36) {
        tokenWarnings.push(`🔴 NE トークン ${Math.floor(hoursSinceUpdate)}時間未更新 → 期限切れの恐れ`);
      }
    } else {
      tokenWarnings.push('🔴 NE トークンファイルなし（data/ne-tokens.json）');
    }
  } catch {}

  // 楽天 — ライセンスキー（.envに設定、1年有効、手動で期限管理）
  if (!process.env.RAKUTEN_SERVICE_SECRET || !process.env.RAKUTEN_LICENSE_KEY) {
    tokenWarnings.push('🔴 楽天 SERVICE_SECRET / LICENSE_KEY 未設定');
  }

  // LINEギフト — トークンリフレッシュ可否で判定
  if (!process.env.LINEGIFT_ACCESS_TOKEN) {
    tokenWarnings.push('🔴 LINEギフト アクセストークン未設定');
  } else if (!process.env.LINEGIFT_REFRESH_TOKEN) {
    tokenWarnings.push('🟡 LINEギフト refresh token 未設定（自動更新不可）');
  }

  // Amazon SP-API — refresh tokenは無期限だが設定有無チェック
  if (!process.env.SP_API_REFRESH_TOKEN) {
    tokenWarnings.push('🔴 Amazon SP-API refresh token 未設定');
  }

  // au PAY — APIキー設定チェック
  if (!process.env.AUPAY_API_KEY) {
    tokenWarnings.push('🟡 au PAY APIキー未設定');
  }

  // Qoo10 — APIキー設定チェック
  if (!process.env.QOO10_CERT_KEY) {
    tokenWarnings.push('🟡 Qoo10 CERT_KEY 未設定');
  }

  // メルカリ — トークン設定チェック
  if (!process.env.MERCARI_API_TOKEN) {
    tokenWarnings.push('🟡 メルカリ APIトークン未設定');
  }

  // 通知メッセージ作成
  // 期限切れ系（🔴🟡）のみ通知に含め、設定チェック系は同期失敗時のみ表示
  const urgentWarnings = tokenWarnings.filter(w => w.startsWith('🔴') || w.startsWith('🟡'));
  const allOk = results.every(r => r.success) && urgentWarnings.length === 0;
  const icon = allOk ? '✅' : '⚠️';
  let msg = `${icon} *Warehouse日次同期 ${dateStr}* (${duration}秒)\n`;
  for (const r of results) {
    msg += `${r.success ? '✅' : '❌'} ${r.name}: ${r.summary}\n`;
  }
  if (urgentWarnings.length > 0) {
    msg += `\n*⏰ 認証情報アラート:*\n`;
    for (const w of urgentWarnings) msg += `${w}\n`;
  }

  console.log('\n' + msg);
  await notify(msg);

  console.log(`[DailySync] 完了: ${endTime.toISOString()}`);
}

main().catch(async (e) => {
  console.error('[DailySync] 致命的エラー:', e.message);
  await notify(`❌ *Warehouse日次同期 失敗*\n${e.message}`);
  process.exit(1);
});
