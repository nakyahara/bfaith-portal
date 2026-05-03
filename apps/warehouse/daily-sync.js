/**
 * daily-sync.js — 日次データ同期 + Google Chat通知
 *
 * 毎朝タスクスケジューラから実行。
 * SP-API（Amazon 7日分）と楽天RMS API（7日分）のデータを取得し、
 * 結果をGoogle Chatに通知する。
 */
import 'dotenv/config';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');

// retry-failed-jobs.js が読む state ファイル。リトライ対象失敗ジョブをここに記録する
const RETRY_STATE_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync-retry-state.json');
// retry-failed-jobs.js でリトライ可能なジョブ (idempotent + 一時的失敗想定)
const RETRYABLE_JOBS = ['f_sales', '楽天sku_map', 'Render同期'];

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

  // state 操作の失敗を集約して通知に出す配列 (握り潰しを排除)。
  // 起動時 cleanup / 書き込み失敗時の旧 state 削除 / 全成功時 cleanup の3経路で使う。
  const stateOpWarnings = [];

  // retry-state の前処理:
  //   - 別日 or 破損 → 即削除 (古い backlog を引きずらない)
  //   - 同日 → 一旦残す。今回 run の結果に応じて終了時に上書き or 削除する
  //     (失敗あり → 上書き / 全成功 → 削除)
  try {
    if (fs.existsSync(RETRY_STATE_FILE)) {
      let removeStale = false;
      try {
        const existing = JSON.parse(fs.readFileSync(RETRY_STATE_FILE, 'utf-8'));
        if (existing.run_date !== businessDate) removeStale = true;
        else console.log(`[DailySync] 同日の retry-state を検出 (run_date=${existing.run_date})。終了時に上書き or 削除予定`);
      } catch {
        // 破損 state は削除対象
        removeStale = true;
      }
      if (removeStale) {
        try {
          fs.unlinkSync(RETRY_STATE_FILE);
          console.log('[DailySync] 別日 or 破損した retry-state を削除');
        } catch (delErr) {
          // 削除失敗 = 古い backlog が残る = retry-failed-jobs が stale 内容で誤実行の恐れ
          stateOpWarnings.push(`🔴 起動時 retry-state cleanup 失敗 (${delErr.message})、retry が古い内容で誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
          console.error('[DailySync] retry-state 削除失敗:', delErr.message);
        }
      }
    }
  } catch (e) {
    stateOpWarnings.push(`🔴 起動時 retry-state チェック失敗 (${e.message})、retry 状態が不明。手動確認を: ${RETRY_STATE_FILE}`);
    console.error('[DailySync] retry-state チェック失敗:', e.message);
  }

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

  // 在庫スナップショット集計 (FBA + 自社倉庫の金額算出 → inv_daily_summary)
  // 上の NE/FBA スナップショットの後に必ず走る (失敗時も結果は no_source で記録)
  const invAggResult = runScript('apps/warehouse/snapshot-inventory-aggregate.js', '在庫スナップショット集計', 120000);
  results.push({ name: '在庫集計', ...invAggResult });

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

  // 楽天 AM/AL/W → NE商品コード sku_map 再構築 (f_sales と独立)
  const rakutenSkuMapResult = runScript('apps/warehouse/rebuild-rakuten-sku-map.js', '楽天 sku_map 再構築');
  results.push({ name: '楽天sku_map', ...rakutenSkuMapResult });

  // Renderにミラーデータ送信。
  // sync-to-render.js は f_sales_by_listing/by_product と f_rakuten_sku_map の両方を Render に送るため、
  // どちらかが失敗していると古い表が Render に押し付けられる → 両方成功時のみ実行
  // → retry-failed-jobs で復旧後に再試行
  let syncResult;
  const fSalesOk = fSalesResult.success;
  const skuMapOk = rakutenSkuMapResult.success;
  if (fSalesOk && skuMapOk) {
    syncResult = runScript('apps/warehouse/sync-to-render.js', 'Render同期');
  } else {
    const reasons = [];
    if (!fSalesOk) reasons.push('f_sales 失敗');
    if (!skuMapOk) reasons.push('楽天sku_map 失敗');
    console.log(`[DailySync] Render同期 スキップ (${reasons.join(', ')}、retry で復旧予定)`);
    syncResult = { success: false, summary: `⏸️ skipped (${reasons.join(', ')})` };
  }
  results.push({ name: 'Render同期', ...syncResult });

  // 月初の自動 月末確定値保存
  // 条件: 今日が月初 (JST) かつ アップストリームのデータ取得が全部成功してる
  // (ハンガリ式: FBA snapshot / 在庫集計 / Render同期 が全て成功 = mirror に最新データ揃ってる)
  // 失敗してれば skip + retry 待ちにすることで 「壊れた前月末値が履歴に残る」事故を防ぐ
  const todayJstDay = Number(businessDate.slice(8, 10)); // 'YYYY-MM-DD' の dd
  if (todayJstDay === 1) {
    const fbaSnapOk = fbaSnapResult.success;
    const invAggOk = invAggResult.success;
    const renderOk = syncResult.success;
    const blockingFails = [];
    if (!fbaSnapOk) blockingFails.push('FBA在庫snapshot');
    if (!invAggOk) blockingFails.push('在庫集計');
    if (!renderOk) blockingFails.push('Render同期');

    if (blockingFails.length === 0) {
      console.log('\n=== 月末確定値の自動保存 (前月末日) ===');
      try {
        const url = (process.env.RENDER_MIRROR_URL || 'https://bfaith-portal.onrender.com/apps/mirror').replace(/\/apps\/mirror\/?$/, '') + '/apps/inventory-monthly/api/save-month-end';
        const syncKey = process.env.MIRROR_SYNC_KEY;
        if (!syncKey) {
          results.push({ name: '月末確定値', success: false, summary: '⏸️ MIRROR_SYNC_KEY 未設定でスキップ' });
        } else {
          const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-sync-key': syncKey },
            body: JSON.stringify({}), // 省略時は前月末日を server 側で計算
            signal: AbortSignal.timeout(60000),
          });
          const data = await resp.json().catch(() => ({}));
          if (resp.ok && data.ok) {
            const { snapshot_date, totals, partial_categories } = data;
            const partialNote = (partial_categories && partial_categories.length > 0) ? ` (partial: ${partial_categories.join(',')})` : '';
            console.log(`[月末確定値] 保存成功: ${snapshot_date} 合計=¥${totals.total.toLocaleString('ja-JP')}${partialNote}`);
            results.push({ name: '月末確定値', success: true, summary: `${snapshot_date} 合計=¥${totals.total.toLocaleString('ja-JP')}${partialNote}` });
          } else {
            const err = data.error || `HTTP ${resp.status}`;
            console.error(`[月末確定値] 保存失敗:`, err);
            results.push({ name: '月末確定値', success: false, summary: err.slice(0, 200) });
          }
        }
      } catch (e) {
        console.error('[月末確定値] 例外:', e.message);
        results.push({ name: '月末確定値', success: false, summary: e.message.slice(0, 200) });
      }
    } else {
      const msg = `⏸️ skipped (上流失敗: ${blockingFails.join(',')})`;
      console.log(`[DailySync] 月末確定値スキップ: ${msg}`);
      results.push({ name: '月末確定値', success: false, summary: msg });
    }
  }

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

  // ─── retry-state 書き込み (リトライ対象の失敗があれば) ───

  const retryableFailed = results
    .filter(r => RETRYABLE_JOBS.includes(r.name) && !r.success)
    .map(r => r.name);

  let retryStateWritten = false;
  let retryStateError = null;
  if (retryableFailed.length > 0) {
    // 失敗あり → state 書き出し (retry_count=0 でリセット、retry-failed-jobs が拾う)。
    // 直接 writeFileSync。中断で破損しても retry-failed-jobs.loadState() が JSON
    // parse 失敗時に検出 + deleteState を呼ぶ self-healing 機構があるので tmp+rename 不要。
    try {
      fs.writeFileSync(RETRY_STATE_FILE, JSON.stringify({
        run_date: dateStr,
        started_at: startTime.toISOString(),
        remaining_jobs: retryableFailed,
        retry_count: 0,
        last_attempt_at: null,
      }, null, 2));
      retryStateWritten = true;
      console.log(`[DailySync] retry-state 書き込み: ${retryableFailed.join(', ')}`);
    } catch (e) {
      retryStateError = e.message;
      console.error('[DailySync] retry-state 書き込み失敗:', e.message);
      // 旧 same-day state を明示削除 (整合性優先、retry されない方がマシ)
      try {
        if (fs.existsSync(RETRY_STATE_FILE)) {
          fs.unlinkSync(RETRY_STATE_FILE);
          console.warn('[DailySync] 整合性確保のため旧 retry-state も削除');
        }
      } catch (delErr) {
        // 書き込み失敗 + 旧 state 削除も失敗 = stale state が残る危険な状態
        stateOpWarnings.push(`🔴 retry-state 書き込み失敗 + 旧 state 削除も失敗 (write: ${e.message}, del: ${delErr.message})、stale 内容で retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
        console.error('[DailySync] 旧 retry-state 削除も失敗:', delErr.message);
      }
    }
  } else {
    // 全成功 → 同日に残っている可能性がある古い state を削除
    // (例: 7:00 で失敗→state作成、同日中に手動再実行→全成功 のケース)
    try {
      if (fs.existsSync(RETRY_STATE_FILE)) {
        fs.unlinkSync(RETRY_STATE_FILE);
        console.log('[DailySync] 全成功のため残存していた retry-state を削除');
      }
    } catch (e) {
      // 削除失敗時、古い state が残って 8:30 retry が stale 内容で誤実行するリスクあり。
      stateOpWarnings.push(`🔴 全成功時の retry-state クリーンアップ失敗 (${e.message})、stale 内容で retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
      console.error('[DailySync] retry-state クリーンアップ失敗:', e.message);
    }
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
  if (retryableFailed.length > 0) {
    if (retryStateWritten) {
      msg += `\n🔄 自動再試行予定: ${retryableFailed.join(', ')} を本日 8:30 / 10:00 / 11:30 JST に再実行\n`;
    } else {
      msg += `\n⚠️ retry-state 書き込み失敗 (${retryStateError})、自動再試行されません。手動対応必要\n`;
    }
  }
  // state 操作の失敗 (起動時 cleanup / 書き込み失敗時旧削除 / 全成功時 cleanup) を通知
  for (const w of stateOpWarnings) {
    msg += `\n${w}\n`;
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
