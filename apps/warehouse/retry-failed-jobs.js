/**
 * retry-failed-jobs.js — daily-sync.js の失敗ジョブを自動再試行
 *
 * 朝7:00 の daily-sync で失敗した f_sales / 楽天sku_map / Render同期 を、
 * 8:30 / 10:00 / 11:30 JST (固定) に自動再実行する (Task Scheduler から3回起動)。
 *
 * 注: daily-sync の最悪完了時刻は 8:15 (Yahoo 60分タイムアウト想定)。8:30 が最早安全タイミング。
 *
 * 動作:
 *   1. data/daily-sync-retry-state.json があれば読む。なければ no-op で終了
 *   2. state.run_date が今日(JST)でなければ古い state とみなしてクリーンアップ
 *   3. remaining_jobs を依存順 (f_sales → 楽天sku_map → Render同期) で再実行
 *      - Render同期: 今回 f_sales / 楽天sku_map のどちらかを再実行して失敗した場合スキップ
 *   4. 全部成功 → state削除 + ✅復旧通知
 *      最大試行 (3回) 到達 → state削除 + 🔴最終失敗通知
 *      まだ残る → state更新 (次回 Task Scheduler が拾う)
 *
 * Task Scheduler 設定 (3つ):
 *   WarehouseDailySyncRetry1: 08:30 JST 毎日
 *   WarehouseDailySyncRetry2: 10:00 JST 毎日
 *   WarehouseDailySyncRetry3: 11:30 JST 毎日
 *   いずれも `node apps/warehouse/retry-failed-jobs.js` を実行。
 *   state ファイルが無ければ即時 no-op で終了するので、空振り起動は無害。
 */
import 'dotenv/config';
import fs from 'fs';
import { execFileSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const RETRY_STATE_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync-retry-state.json');

const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

const MAX_RETRY_COUNT = 3;

// ジョブ定義 (daily-sync.js と一致させる)
//   f_sales のみ retry 時は 30分 (初回 10分でタイムアウトした場合の余裕)
//   args 省略時は '7' (rebuild系の日数引数の既存挙動を維持)
const JOB_DEFINITIONS = {
  'f_sales':        { script: 'apps/warehouse/rebuild-f-sales.js',                timeoutMs: 1800000 },
  'sales_velocity': { script: 'apps/warehouse/rebuild-sales-velocity.js',         timeoutMs: 900000  },
  'pml_snapshot':   { script: 'apps/warehouse/build-product-management-snapshot.js', timeoutMs: 600000 },
  '楽天sku_map':    { script: 'apps/warehouse/rebuild-rakuten-sku-map.js',        timeoutMs: 600000  },
  'Render同期':     { script: 'apps/warehouse/sync-to-render.js',                 timeoutMs: 600000  },
  // Amazon Settlement/Ads: 一過性の SP-API fetch failed で落ちた際の自動復旧 (2026-07-13 に
  // Settlement が「JOB_DEFINITIONS 未登録のため未実行」→手動対応になった実績)。いずれも冪等で再実行安全。
  // Settlement の下流 (アカウントフィー build/sync) は翌朝 daily-sync が再集計する冪等設計のため
  // ここでは fetch 本体のみ再試行すれば十分。
  // ⚠️'Amazon finance build' は --month 引数が動的 (当月) なため未登録のまま (unhandled 通知で顕在化)
  'Amazon Settlement':     { script: 'apps/warehouse/fetch-amazon-settlements.js', args: ['--days', '14'], timeoutMs: 3600000 },
  'Amazon Ads (campaign)': { script: 'apps/warehouse/fetch-amazon-ads-campaign.js', args: [], timeoutMs: 1800000 },
  'Amazon Ads (SKU)':      { script: 'apps/warehouse/fetch-amazon-ads.js',          args: [], timeoutMs: 1800000 },
  // 'Amazon手数料' (2026-07-16 障害対応、incident_amazon_fee_coverage_no_retry):
  //   daily-sync の RETRYABLE_JOBS に入れるだけでは「未対応」🔴 になるため、ここにも定義必須。
  //   retry は 08:30/10:00/11:30 の空き枠で走るので daily(07:00, 10分) より timeout を 20分に延ばして余裕を取る。
  //   INSERT OR REPLACE + TTL/差分フィルタで再実行安全 (成功済み SKU は skip されるので負荷は自然縮小)。
  'Amazon手数料':          { script: 'apps/warehouse/fetch-amazon-fees.js',        args: ['--recent', '30'], timeoutMs: 1200000 },
  // DBバックアップ: 冪等 (当日分完成済み+元DB変化なしなら再利用し offsite 以降だけやり直す。
  // 先行ジョブの retry で DB が更新されていれば src_sig 不一致で自動的に作り直す)。月初最悪 ~5.5h
  'DBバックアップ':        { script: 'apps/warehouse/backup-warehouse.js',          args: [], timeoutMs: 21600000 },
};

// 実行順序 (依存関係順)。sales_velocity → pml_snapshot は f_sales と同じ raw + マスタ依存なので直後。
// Amazon系は他ジョブと独立なので先頭 (長時間ジョブを先に開始)
// DBバックアップは最後 (f_sales 等が同時に失敗していた場合、復旧後の最新状態を保存するため)
const RETRY_ORDER = ['Amazon Settlement', 'Amazon Ads (campaign)', 'Amazon Ads (SKU)', 'Amazon手数料', 'f_sales', 'sales_velocity', 'pml_snapshot', '楽天sku_map', 'Render同期', 'DBバックアップ'];

async function notify(text) {
  if (!GCHAT_WEBHOOK) {
    console.warn('[Retry] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定のため通知スキップ');
    return;
  }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    if (!res.ok) {
      console.error(`[Retry] [NOTIFY:status=failed] GChat HTTP ${res.status}`);
      return;
    }
    console.log('[Retry] [NOTIFY:status=sent]');
  } catch (e) {
    console.error('[Retry] [NOTIFY:status=failed] 通知エラー:', e.message);
  }
}

// execFileSync(shell:false): timeout時のWAL lock残留と引数のshell解釈を避ける
// (feedback_execfile_vs_execsync — daily-sync.js と同方針)
function runScript(scriptPath, label, timeoutMs, args = ['7']) {
  const filePath = path.join(PROJECT_DIR, scriptPath);
  console.log(`\n=== ${label} ===`);
  try {
    const output = execFileSync(process.execPath, [filePath, ...args], {
      cwd: PROJECT_DIR,
      timeout: timeoutMs,
      encoding: 'utf-8',
      // daily-sync.js と同じ 64MB (デフォルト 1MB は Settlement 系の stdout で超え、
      // 本体成功でも ENOBUFS で「失敗」誤記録になる — 構造監査 H-2)
      maxBuffer: 64 * 1024 * 1024,
      env: {
        ...process.env,
        PATH: process.env.PATH,
        // バッチ用の長め busy_timeout (db.js の initDB が参照)
        WAREHOUSE_DB_BUSY_TIMEOUT_MS: process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS || '60000',
      },
    });
    console.log(output);
    const lines = output.trim().split('\n');
    return { success: true, summary: lines[lines.length - 1] || '' };
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

/**
 * state ファイル読み込み。
 * 戻り値:
 *   { found: false }                              ファイル無し (no-op で終了)
 *   { found: true, state }                        正常読み込み
 *   { found: true, state: null, parseError }      破損 (呼び出し側で deleteState を試みる)
 */
function loadState() {
  if (!fs.existsSync(RETRY_STATE_FILE)) return { found: false };
  try {
    const json = fs.readFileSync(RETRY_STATE_FILE, 'utf-8');
    return { found: true, state: JSON.parse(json) };
  } catch (e) {
    console.error('[Retry] state file 読み込み失敗:', e.message);
    return { found: true, state: null, parseError: e.message };
  }
}

/**
 * state ファイル書き込み。成否を返す。
 * 失敗時は呼び出し側で「state 不整合の恐れあり」として通知すべき。
 */
function saveState(state) {
  try {
    fs.writeFileSync(RETRY_STATE_FILE, JSON.stringify(state, null, 2));
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

/**
 * state ファイル削除。成否を返す。
 * - ファイル無し: ok (成功扱い)
 * - 削除成功: ok
 * - 削除失敗: ok=false + error (呼び出し側で通知)
 *   stale state が残ると後続 retry が誤実行するため、失敗は明示する。
 */
function deleteState() {
  try {
    fs.unlinkSync(RETRY_STATE_FILE);
    return { ok: true };
  } catch (e) {
    if (e.code === 'ENOENT') return { ok: true };
    return { ok: false, error: e.message };
  }
}

async function main() {
  const loadResult = loadState();
  if (!loadResult.found) {
    console.log('[Retry] retry-state 無し → no-op');
    return;
  }
  if (loadResult.parseError) {
    // 破損 state → 削除試行 + 失敗時は通知
    console.warn('[Retry] 破損した retry-state を削除します');
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\n破損した retry-state を削除できず (${del.error})。手動削除を: ${RETRY_STATE_FILE}\nparse error: ${loadResult.parseError}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }
  const state = loadResult.state;

  const today = toJstDate(new Date());
  if (state.run_date !== today) {
    console.log(`[Retry] state は別日 (state.run_date=${state.run_date}, today=${today}) → クリーンアップして終了`);
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\n別日 retry-state を削除できず (${del.error})。手動でファイルを削除してください: ${RETRY_STATE_FILE}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }

  if (!Array.isArray(state.remaining_jobs) || state.remaining_jobs.length === 0) {
    console.log('[Retry] remaining_jobs 空 → クリーンアップして終了');
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\nremaining_jobs 空の retry-state を削除できず (${del.error})。手動でファイルを削除してください: ${RETRY_STATE_FILE}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }

  // JST 業務日付を子プロセスに伝える
  process.env.WAREHOUSE_BUSINESS_DATE = today;

  const retryCount = (state.retry_count || 0) + 1;
  const startedAt = new Date();
  console.log(`[Retry] 試行 ${retryCount}/${MAX_RETRY_COUNT}: ${state.remaining_jobs.join(', ')}`);

  const results = []; // {name, success, summary}

  for (const jobName of RETRY_ORDER) {
    if (!state.remaining_jobs.includes(jobName)) continue;

    // Render同期 fail-fast: 今回 f_sales / 楽天sku_map を試行して失敗した場合スキップ。
    //   どちらかが remaining_jobs に無い (= 既に成功済み) なら同方向はクリア扱い、
    //   今回試行して失敗していたら Render は古い表を押し付けないようスキップ。
    if (jobName === 'Render同期') {
      const fSalesAttempt = results.find(r => r.name === 'f_sales');
      const skuMapAttempt = results.find(r => r.name === '楽天sku_map');
      const fSalesFailed = fSalesAttempt && !fSalesAttempt.success;
      const skuMapFailed = skuMapAttempt && !skuMapAttempt.success;
      if (fSalesFailed || skuMapFailed) {
        const reasons = [];
        if (fSalesFailed) reasons.push('f_sales 再失敗');
        if (skuMapFailed) reasons.push('楽天sku_map 再失敗');
        console.log(`[Retry] Render同期 スキップ (${reasons.join(', ')}、次回再試行)`);
        results.push({ name: 'Render同期', success: false, summary: `⏸️ skipped (${reasons.join(', ')})` });
        continue;
      }
    }

    const def = JOB_DEFINITIONS[jobName];
    const result = runScript(def.script, jobName, def.timeoutMs, def.args);
    results.push({ name: jobName, ...result });
  }

  // fail-closed: remaining_jobs のうち runner に定義が無いジョブ (daily-sync の RETRYABLE_JOBS には
  // あるが JOB_DEFINITIONS/RETRY_ORDER 未登録、例: Amazon finance build) は上の
  // ループで実行されない。results に載らないと stillFailed=0 となり state削除→誤「復旧成功」で
  // サイレントに落ちる。未対応ジョブは失敗扱いで残し、最終的に 🔴 通知で顕在化させる。
  // Codex R2 High #2: JOB_DEFINITIONS だけでなく RETRY_ORDER 漏れも未対応扱いにする。
  //   定義はあるが RETRY_ORDER に無いと実行ループ (RETRY_ORDER を回す) に入らず、
  //   かつ従来判定では unhandled にも載らず、誤「✅復旧成功」でサイレントに落ちるため。
  const unhandled = state.remaining_jobs.filter(j => !JOB_DEFINITIONS[j] || !RETRY_ORDER.includes(j));
  for (const j of unhandled) {
    const reason = !JOB_DEFINITIONS[j] ? 'JOB_DEFINITIONS 未登録' : 'RETRY_ORDER 未登録';
    results.push({ name: j, success: false, summary: `⚠️ retry runner 未対応 (${reason}) のため未実行` });
  }

  const stillFailed = results.filter(r => !r.success).map(r => r.name);
  const justSucceeded = results.filter(r => r.success).map(r => r.name);
  const duration = Math.round((Date.now() - startedAt.getTime()) / 1000);

  if (stillFailed.length === 0) {
    // 全部成功 → state削除 + ✅復旧通知
    const del = deleteState();
    let msg = `🔄 *Warehouse自動再試行 ${retryCount}回目: ✅ 復旧成功* (${duration}秒)\n`;
    msg += `復旧したジョブ: ${justSucceeded.join(', ')}\n`;
    if (!del.ok) {
      msg += `\n🔴 retry-state クリーンアップ失敗 (${del.error})。後続 retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}\n`;
    }
    console.log('\n' + msg);
    await notify(msg);
  } else if (retryCount >= MAX_RETRY_COUNT) {
    // 最終失敗 → state削除 + 🔴最終通知 (手動対応必要)
    const del = deleteState();
    let msg = `🔴 *Warehouse自動再試行 失敗* (${retryCount}回試行)\n`;
    msg += `手動対応が必要なジョブ: ${stillFailed.join(', ')}\n`;
    if (justSucceeded.length > 0) msg += `今回復旧: ${justSucceeded.join(', ')}\n`;
    for (const r of results.filter(r => !r.success)) {
      msg += `❌ ${r.name}: ${r.summary}\n`;
    }
    if (!del.ok) {
      msg += `\n🔴 retry-state クリーンアップ失敗 (${del.error})。後続 retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}\n`;
    }
    console.log('\n' + msg);
    await notify(msg);
  } else {
    // 次回も試行 → state更新
    const sav = saveState({
      ...state,
      remaining_jobs: stillFailed,
      retry_count: retryCount,
      last_attempt_at: startedAt.toISOString(),
    });
    if (justSucceeded.length > 0) {
      let msg = `🔄 *Warehouse自動再試行 ${retryCount}回目: 部分復旧* (${duration}秒)\n`;
      msg += `復旧: ${justSucceeded.join(', ')}\n`;
      msg += `残り: ${stillFailed.join(', ')}（次回再試行予定）\n`;
      if (!sav.ok) {
        msg += `\n🔴 retry-state 書き込み失敗 (${sav.error})、次回 retry の retry_count / remaining_jobs が古いままになる恐れあり。手動確認を\n`;
      }
      console.log('\n' + msg);
      await notify(msg);
    } else if (!sav.ok) {
      // 全失敗だが state 更新失敗 → 通知
      const msg = `🔴 *Warehouse自動再試行 ${retryCount}回目: 全失敗 + state更新失敗*\n書き込み失敗 (${sav.error})、次回 retry が誤動作する恐れあり`;
      console.error(msg);
      await notify(msg);
    } else {
      // 何も復旧しなかったが state 更新は成功 → 連続失敗で煩くならないよう通知抑制
      console.log(`[Retry] 試行 ${retryCount} 全失敗、通知抑制 (state更新成功)`);
    }
  }
}

main().catch(async (e) => {
  console.error('[Retry] 致命的エラー:', e.message);
  await notify(`❌ *Warehouse自動再試行 実行エラー*\n${e.message}`);
  process.exit(1);
});
