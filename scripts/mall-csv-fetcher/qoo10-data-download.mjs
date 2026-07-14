/**
 * Qoo10 Analytics xlsx 自動ダウンロード (mall-csv-fetcher P1-Q R1)
 *
 * 毎朝 seller.qoo10.jp/conversion から2ファイルを取得して incoming/qoo10-data/ に置く
 * (取込は import-qoo10-data.js):
 *   1. エクセルダウンロード → 店舗トラフィック/CVR (日付×チャネル52列+KPI)
 *   2. 日付・商品別エクセルダウンロード → 商品×日付×チャネルPV
 * 期間は D-14〜D-1 を毎日置換 (前日データは朝反映を実測済み。後日補正はscope置換で自動吸収)。
 * 月1回 (1日) は過去90日を14日窓×7回で再取得 (Codex: 補正実測が貯まるまでの保険)。
 * バックフィルは QDATA_FROM/QDATA_TO で任意窓 (遡及≥1.5年を実測済み)。
 *
 * 認証: storageState注入 (lib-qoo10-login)。**成功時は毎回stateを再保存して延命**。
 * 切れたら 2FA_REQUIRED → GChat通知 → 中原さんが MANUAL=1 で手動再ログイン (reCAPTCHA)。
 *
 * 実行: node scripts/mall-csv-fetcher/qoo10-data-download.mjs
 *   env: HEADLESS=1 / QDATA_FROM=YYYY-MM-DD / QDATA_TO=YYYY-MM-DD (バックフィル用)
 *        QDATA_LONG_REFRESH=1 (90日再取得を強制。通常は毎月1日に自動)
 */

import { mkdir, copyFile, writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import {
  openQoo10Context, gotoQoo10Page, ensureQsmLogin, saveQoo10State, ANALYTICS_BASE,
} from './lib-qoo10-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'qoo10-data') : null;

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);
function daysAgoIso(back) {
  const now = jstNow();
  return isoDate(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - back)));
}
const slash = (iso) => iso.split('-').join('/');

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `qdata_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] qdata_${label}  url=${page.url()}`);
}

async function logFetch(entry) {
  if (!WAREHOUSE_DATA_DIR) {
    console.log(`  [fetch-log] WAREHOUSE_DATA_DIR 未設定のためDB記録スキップ (${entry.report_type}: ${entry.status})`);
    return;
  }
  try {
    const { default: Database } = await import('better-sqlite3');
    const { ensureRppTables } = await import('../../apps/warehouse/rakuten-ads-rpp-lib.js');
    const db = new Database(join(WAREHOUSE_DATA_DIR, 'warehouse.db'));
    try {
      db.pragma('busy_timeout = 5000');
      ensureRppTables(db);
      db.prepare(`INSERT INTO report_fetch_log
        (fetched_at, mall, report_type, period_from, period_to, file_name, file_sha256, file_bytes, status, message)
        VALUES (?, 'qoo10', ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(new Date().toISOString(), entry.report_type, entry.period_from, entry.period_to,
             entry.file_name || null, entry.file_sha256 || null, entry.file_bytes ?? null,
             entry.status, String(entry.message || '').slice(0, 500));
    } finally {
      db.close();
    }
  } catch (e) {
    console.warn(`  ⚠ [fetch-log] DB記録失敗 (DLは成功扱い): ${e.message}`);
  }
}

/** /conversion で期間をセットして「適用」→ 表示期間が要求と一致するまで検証 */
async function setPeriod(page, from, to) {
  const wanted = `${slash(from)}~${slash(to)}`;
  const ok = await page.evaluate(([f, t]) => {
    const d1 = document.getElementById('filter-date1');
    const d2 = document.getElementById('filter-date2');
    if (!d1 || !d2) return false;
    const set = (el, v) => {
      const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
      setter.call(el, v);
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    };
    set(d1, f);
    set(d2, t);
    return true;
  }, [slash(from), slash(to)]);
  if (!ok) throw new Error('FORM_VERIFY: 期間入力 (filter-date1/2) が見つからず — SPA変更の疑い');
  await page.locator('button:has-text("適用"), a:has-text("適用")').first().click({ timeout: 10000 });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(3500);
  const body = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
  const period = body.match(/(\d{4}\/\d{2}\/\d{2})\s*~\s*(\d{4}\/\d{2}\/\d{2})/)?.[0]?.replace(/\s/g, '');
  if (period !== wanted) {
    throw new Error(`FORM_VERIFY: 期間が反映されない (要求 ${wanted} / 表示 ${period || '不明'})`);
  }
  // 「日別」であること (週別/月別に切り替わっていたら取込側が弾くが、早期検知)
  return period;
}

/** DL結果を保存 → 取込レシピ検証 → incoming投入 */
async function persistXlsx(buf, destName, { reportType, expectDaily, from, to }) {
  const dest = join(DL_DIR, destName);
  await writeFile(dest, buf);
  console.log(`  ✅ 取得成功: ${destName} (${buf.length} bytes)`);
  const { prepareQoo10File } = await import('../../apps/warehouse/qoo10-data-lib.js');
  const p = await prepareQoo10File(destName, buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らないファイル (${p.error})。画面仕様変更の疑い`);
  const hasType = (t) => p.preps.some((x) => x.type === t);
  if (!hasType(expectDaily)) throw new Error(`DL_VERIFY: 期待種別 ${expectDaily} が含まれない (${p.preps.map((x) => x.type).join(',')})`);
  if (p.dateFrom !== from || p.dateTo !== to) {
    throw new Error(`DL_VERIFY: 期間不一致 (要求 ${from}〜${to} / 実データ ${p.dateFrom}〜${p.dateTo})`);
  }
  console.log(`  [verify] ${p.label} (${p.dateFrom}〜${p.dateTo})`);
  for (const w of p.warnings || []) console.log(`  ⚠ ${w}`);
  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    await copyFile(dest, join(INCOMING_DIR, destName));
    console.log(`  → incoming投入: ${join(INCOMING_DIR, destName)}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ');
  }
  await logFetch({
    report_type: reportType, period_from: from, period_to: to,
    file_name: destName, file_sha256: createHash('sha256').update(buf).digest('hex'),
    file_bytes: buf.length, status: 'ok', message: '',
  });
}

/** 1窓 (from〜to) の2ファイルを取得 */
async function fetchWindow(page, from, to, outcomes) {
  console.log(`\n--- Analytics ${from}〜${to} ---`);
  await gotoQoo10Page(page, `${ANALYTICS_BASE}/conversion`, 'Analytics トラフィック/CV');
  await setPeriod(page, from, to);
  const ts = new Date().toISOString().replace(/[-:.]/g, '').replace('Z', '');

  // 1. 店舗版
  {
    const dlP = page.waitForEvent('download', { timeout: 90000 });
    dlP.catch(() => {});
    await page.locator('text=エクセルダウンロード').first().click({ timeout: 10000 });
    const dl = await dlP;
    const buf = await readFile(await dl.path());
    await persistXlsx(buf, `qoo10_cvr_d${from}_${to}_${ts}.xlsx`, {
      reportType: 'qdata_cvr', expectDaily: 'qoo10_cvr_daily', from, to,
    });
    outcomes.push({ spec: 'cvr', ym: from, status: 'ok' });
  }
  // 2. 日付・商品別
  {
    const dlP = page.waitForEvent('download', { timeout: 120000 });
    dlP.catch(() => {});
    await page.locator('text=日付・商品別エクセルダウンロード').first().click({ timeout: 10000 });
    const dl = await dlP;
    const buf = await readFile(await dl.path());
    await persistXlsx(buf, `qoo10_cvritem_d${from}_${to}_${ts}.xlsx`, {
      reportType: 'qdata_cvr_item', expectDaily: 'qoo10_item_traffic_daily', from, to,
    });
    outcomes.push({ spec: 'cvr_item', ym: from, status: 'ok' });
  }
}

/** 取得窓の計画: 通常=D-14〜D-1 / 毎月1日 or QDATA_LONG_REFRESH=1 は過去90日 (14日窓×7) */
function planWindows() {
  const fromEnv = (process.env.QDATA_FROM || '').trim();
  const toEnv = (process.env.QDATA_TO || '').trim();
  if (fromEnv || toEnv) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(fromEnv) || !/^\d{4}-\d{2}-\d{2}$/.test(toEnv) || fromEnv > toEnv) {
      throw new Error(`ENV: QDATA_FROM/TO が不正 (${fromEnv}〜${toEnv})`);
    }
    // 任意レンジを14日窓に分割 (Analyticsの取得上限が未知のため窓は固定14日)
    const wins = [];
    let cur = fromEnv;
    while (cur <= toEnv) {
      const curDt = new Date(cur + 'T00:00:00Z');
      const end = new Date(Math.min(curDt.getTime() + 13 * 86400000, new Date(toEnv + 'T00:00:00Z').getTime()));
      wins.push([cur, isoDate(end)]);
      cur = isoDate(new Date(end.getTime() + 86400000));
    }
    return wins;
  }
  const longRefresh = process.env.QDATA_LONG_REFRESH === '1' || jstNow().getUTCDate() === 1;
  if (longRefresh) {
    const wins = [];
    for (let back = 90; back >= 1; back -= 14) {
      wins.push([daysAgoIso(back), daysAgoIso(Math.max(back - 13, 1))]);
    }
    return wins;
  }
  return [[daysAgoIso(14), daysAgoIso(1)]];
}

async function main() {
  const runLog = initRunLog('qoo10-data');
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  let windows;
  try {
    windows = planWindows();
  } catch (e) {
    console.error(`FATAL: ${e.message}`);
    process.exitCode = 2;
    return;
  }

  let browser, context;
  try {
    ({ browser, context } = await openQoo10Context());
  } catch (e) {
    console.error(`⚠️ ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'qoo10', logPath: runLog.logPath,
      failures: [{ reportType: 'qdata(セッション)', error: e.message }],
      repro: 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/qoo10-login-spike.mjs (reCAPTCHA+サブIDを手動)',
    }), 'qoo10-data');
    process.exitCode = 1;
    return;
  }
  const page = await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message().slice(0, 120)}`); d.accept().catch(() => {}); });

  const outcomes = [];
  const failures = [];
  try {
    console.log('=== Qoo10 Analytics 自動DL ===');
    console.log(`取得窓: ${windows.map(([f, t]) => `${f}〜${t}`).join(' / ')}`);
    await ensureQsmLogin(page); // セッション検証 (切れていたら 2FA_REQUIRED throw)

    for (const [from, to] of windows) {
      try {
        await fetchWindow(page, from, to, outcomes);
      } catch (e) {
        console.error(`✗ ${from}〜${to}: ${e.message}`);
        await snap(page, `${from}_error`);
        await logFetch({ report_type: 'qdata_cvr', period_from: from, period_to: to, status: 'error', message: e.message });
        outcomes.push({ spec: 'cvr', ym: from, status: 'error' });
        failures.push({ reportType: 'qdata_cvr', ym: from, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `qdata_${from}_error.png`) });
        if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
      }
    }

    // 成功時はセッションを再保存 (rolling延命)。一部失敗でもセッション自体は有効なので保存する
    await saveQoo10State(context);

    console.log(`\n=== summary: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ') || '(対象なし)'} ===`);
    if (failures.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (incoming/qoo10-data/ に置くだけ。xlsxの開始日/終了日で自己記述)');
      await sendGChat(buildErrorReport({
        mall: 'qoo10', outcomes, failures, logPath: runLog.logPath,
        repro: 'node scripts/mall-csv-fetcher/qoo10-data-download.mjs',
      }), 'qoo10-data');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-qoo10-data.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'qoo10', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'qdata(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'qdata_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/qoo10-login-spike.mjs → 本ID/PW+reCAPTCHA+サブIDで手動ログイン (「✅storageState保存」を確認)'
        : 'node scripts/mall-csv-fetcher/qoo10-data-download.mjs',
    }), 'qoo10-data');
    process.exitCode = 1;
  } finally {
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }
}

main();
