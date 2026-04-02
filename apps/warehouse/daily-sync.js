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

function runScript(scriptPath, label) {
  const parts = scriptPath.split(' ');
  const filePath = path.join(PROJECT_DIR, parts[0]);
  const args = parts.slice(1).join(' ') || '7';
  console.log(`\n=== ${label} ===`);
  try {
    const output = execSync(`node "${filePath}" ${args}`, {
      cwd: PROJECT_DIR,
      timeout: 600000, // 10分タイムアウト
      encoding: 'utf-8',
      env: { ...process.env, PATH: process.env.PATH },
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

async function main() {
  const startTime = new Date();
  const dateStr = startTime.toISOString().slice(0, 10);
  console.log(`[DailySync] 開始: ${startTime.toISOString()}`);

  const results = [];

  // NE API（商品マスタ + セット商品 + 受注7日分）
  const neResult = runScript('apps/warehouse/ne-api.js sync', 'NE API');
  results.push({ name: 'NE', ...neResult });

  // SP-API
  const spResult = runScript('apps/warehouse/sp-api-orders.js', 'Amazon SP-API');
  results.push({ name: 'Amazon', ...spResult });

  // 楽天
  const rkResult = runScript('apps/warehouse/rakuten-orders.js', '楽天 RMS API');
  results.push({ name: '楽天', ...rkResult });

  const endTime = new Date();
  const duration = Math.round((endTime - startTime) / 1000);

  // 通知メッセージ作成
  const allOk = results.every(r => r.success);
  const icon = allOk ? '✅' : '⚠️';
  let msg = `${icon} *Warehouse日次同期 ${dateStr}* (${duration}秒)\n`;
  for (const r of results) {
    msg += `${r.success ? '✅' : '❌'} ${r.name}: ${r.summary}\n`;
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
