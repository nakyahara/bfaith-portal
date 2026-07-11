/**
 * Yahoo!ストアクリエイターPro 統計CSV自動ダウンロード (mall-csv-fetcher P1-Y)
 *
 * 毎晩6種を取得して incoming/yahoo-data/ に置く (取込は import-yahoo-data.js):
 *   一括型 (デフォルト期間をそのままDL、毎晩全期間取り直しUPSERT):
 *     全体分析 (過去100日) / 流入・離脱 (100日) / お客様分析 (60日) / 速報 (当日時間帯別)
 *   1日単位型 (日付ピッカーを対象日にセットしてDL、昨日+一昨日ループ):
 *     商品分析 / 検索流入 — 期間集計型でCSVに日付が無いため、ファイル名に対象日を埋めて
 *     取込側 (dateFromFileName) に伝える契約
 *
 * ★実測 (2026-07-11):
 *   - 全ページ「CSVファイルをダウンロード」リンク (href=#、クリック→同期DL)
 *   - 日付ピッカーは #dailyInputDatepickerFrom / To (YYYY/MM/DD)。JSネイティブsetter+イベントで書き換え
 *   - ログインは .profile-yahoo セッション再利用のみ (ビジネスIDはパスワード廃止)。
 *     切れたら 2FA_REQUIRED → GChat通知 → 中原さんが MANUAL=1 で手動再ログイン
 *   - 認証判定は肯定証拠方式 (lib-yahoo-login.mjs、Codexレビュー反映)
 *
 * ガード: DL後に取込レシピ (prepareYahooFile) で実パース+種別/日付検証してから incoming 投入。
 *
 * 実行: node scripts/mall-csv-fetcher/yahoo-data-download.mjs
 *   env: HEADLESS=1 / YDATA_REPORTS=overall,inflow,user,flash,item,keyword / YDATA_ITEM_DAYS=2
 */

import { mkdir, copyFile, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openYahooContext, gotoStorePage, ensureStoreLogin, STORE_TOP_URL } from './lib-yahoo-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'yahoo-data') : null;

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);
const slashDate = (iso) => iso.split('-').join('/');

/** 1日単位型の対象日 (JST): 昨日+一昨日 (古い順)。集計遅れは翌朝の再取得で自己修復 */
function computeDailyTargetDates() {
  const days = Math.min(Math.max(parseInt(process.env.YDATA_ITEM_DAYS, 10) || 2, 1), 14);
  const now = jstNow();
  const dates = [];
  for (let back = days; back >= 1; back--) {
    dates.push(isoDate(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - back))));
  }
  return dates;
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `ydata_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] ydata_${label}  url=${page.url()}`);
}

// ─── report_fetch_log (楽天と同テーブル。mall='yahoo'、fail-soft) ───
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
        VALUES (?, 'yahoo', ?, ?, ?, ?, ?, ?, ?, ?)`)
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

/**
 * 1日単位型のCSVを取得する。
 * ⭐実測 (2026-07-11): 画面のdatepickerは独自実装でクリック/JSセッターのどちらでも
 * 再取得がトリガされず、CSVは常にデフォルト日 (最新日) が出た。
 * 一方 DL は `POST <画面URL>?itemReportCsv` (body: category/span/dateFrom/dateTo) で、
 * **日付は body パラメータで渡せる** → 画面操作せず POST を直接投げる (最も確実)。
 * ページのセッション cookie は page.request が引き継ぐ。
 */
async function postDailyCsv(page, spec, dateIso) {
  const want = slashDate(dateIso);
  const url = `${STORE_TOP_URL}/${spec.path}?${spec.csvQuery}`;
  // 日付パラメータ名は画面ごとに違う (実測: item=dateFrom/dateTo、keyword=since/until)
  const [kFrom, kTo] = spec.csvDateKeys;
  const form = { ...(spec.csvBody || {}), [kFrom]: want, [kTo]: want };
  const res = await page.request.post(url, {
    form,
    headers: {
      'x-requested-with': 'XMLHttpRequest',
      referer: `${STORE_TOP_URL}/${spec.path}`,
    },
    timeout: 90000,
  });
  if (!res.ok()) throw new Error(`DL_FAILED: CSV POST が HTTP ${res.status()} (${url})`);
  const buf = Buffer.from(await res.body());
  if (buf.length < 50) throw new Error(`DL_VERIFY: レスポンスが空 (${buf.length} bytes)`);
  // ⭐実測: レスポンスはJSON (画面JSがクライアント側でCSV化しているだけ)。
  // JSONを正規経路にする (商品分析は meta.since に日付が入り、CSVの「2未満」マスクも無い生値)
  const head = buf.slice(0, 8).toString('utf8').trimStart();
  if (!head.startsWith('{') && !head.startsWith('[')) {
    throw new Error('DL_VERIFY: レスポンスがJSONでない (画面仕様変更の疑い)');
  }
  console.log(`  [date] 対象日=${want} (POST ${spec.csvQuery}, ${buf.length} bytes JSON)`);
  return buf;
}

/** 一括型: 画面の「CSVファイルをダウンロード」リンクをクリックして同期DL */
async function clickCsvDownload(page) {
  const dlPromise = page.waitForEvent('download', { timeout: 90000 });
  dlPromise.catch(() => {});
  const link = page.locator('a', { hasText: /^(全ての)?CSVファイルをダウンロード$/ }).first();
  if (!(await link.isVisible().catch(() => false))) {
    throw new Error('FORM_VERIFY: 「CSVファイルをダウンロード」リンクが見つからず');
  }
  await link.click({ timeout: 10000 });
  const download = await dlPromise;
  return download;
}

/** DL結果 (Buffer) を downloads/ 保存 → レシピ検証 → incoming 投入 */
async function persistCsv(buf, suggested, spec, range) {
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  // 1日単位型はJSON + ファイル名に対象日を埋める (取込側 dateFromFileName との契約。
  // 商品分析は meta.since を持つが、検索流入JSONは meta が無いためファイル名が唯一の日付源)
  const dateTag = spec.daily ? `${range.from}_` : '';
  const ext = spec.daily ? 'json' : 'csv';
  const dest = join(DL_DIR, `yahoo_${spec.key}_${dateTag}${ts}.${ext}`);
  await writeFile(dest, buf);
  console.log(`  ✅ ダウンロード成功: ${basename(dest)} (元名: ${suggested || '(なし)'})`);

  const sha256 = createHash('sha256').update(buf).digest('hex');

  const { prepareYahooFile } = await import('../../apps/warehouse/yahoo-data-lib.js');
  const p = prepareYahooFile(basename(dest), buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らないCSV (${p.error})。画面仕様変更の疑い`);
  if (p.type !== spec.expectType) throw new Error(`DL_VERIFY: 期待種別 ${spec.expectType} と不一致 (${p.type})`);
  if (spec.daily && p.dateFrom !== range.from) {
    throw new Error(`DL_VERIFY: 期待日 ${range.from} と実CSV ${p.dateFrom} が不一致`);
  }
  console.log(`  [verify] ${p.label} ${p.records.length}件 (${p.dateFrom}〜${p.dateTo})`);

  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    await copyFile(dest, join(INCOMING_DIR, basename(dest)));
    console.log(`  → incoming投入: ${join(INCOMING_DIR, basename(dest))}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ');
  }

  await logFetch({
    report_type: spec.reportType, period_from: p.dateFrom, period_to: p.dateTo,
    file_name: basename(dest), file_sha256: sha256, file_bytes: buf.length,
    status: 'ok', message: '',
  });
  return 'ok';
}

// ─── レポート定義 ───
// path はストクリ相対。daily=1日単位型 (日付ピッカーを対象日に設定)
const SPECS = [
  { key: 'overall', label: '全体分析 (日次×デバイス、過去100日)', path: 'sales_manage/overall',
    reportType: 'ydata_overall', expectType: 'yahoo_store_device_daily' },
  { key: 'inflow', label: '流入・離脱 (日次、過去100日)', path: 'sales_manage/inflow_report',
    reportType: 'ydata_inflow', expectType: 'yahoo_inflow_daily' },
  { key: 'user', label: 'お客様分析 (属性、過去60日)', path: 'sales_manage/user_report',
    reportType: 'ydata_user_attr', expectType: 'yahoo_user_attr_daily' },
  { key: 'flash', label: '速報 (当日時間帯別)', path: 'sales_manage/store_flash',
    reportType: 'ydata_flash', expectType: 'yahoo_flash_hourly' },
  // 1日単位型: CSVはPOSTで日付指定 (実測。画面のdatepickerは効かない)
  { key: 'item', label: '商品分析 (商品×日次)', path: 'sales_manage/item_report',
    reportType: 'ydata_item_daily', expectType: 'yahoo_item_daily', daily: true,
    csvQuery: 'itemReportCsv', csvDateKeys: ['dateFrom', 'dateTo'],
    csvBody: { category: '1', span: 'daily', selectedWeeklyDateTo: '', selectedMonthlyDateTo: '' },
    csvFileName: 'b-faith01-item_report.csv' },
  { key: 'keyword', label: '検索流入 (KW×日次)', path: 'sales_manage/keyword_report',
    reportType: 'ydata_keyword_daily', expectType: 'yahoo_keyword_daily', daily: true,
    csvQuery: 'keywordReportCsv', csvDateKeys: ['since', 'until'],
    csvBody: { span: 'daily', categoryId: '1' },
    csvFileName: 'b-faith01-store_keyword.csv' },
];

async function fetchOne(page, spec, range) {
  const rangeLabel = range ? range.from : 'default';
  console.log(`\n--- ${spec.label} ${rangeLabel} ---`);
  if (spec.daily) {
    // 画面を開いてセッション/権限を確認してから CSV を POST (画面操作は不要)
    await gotoStorePage(page, `${STORE_TOP_URL}/${spec.path}`, spec.label);
    const buf = await postDailyCsv(page, spec, range.from);
    return persistCsv(buf, spec.csvFileName, spec, range);
  }
  await gotoStorePage(page, `${STORE_TOP_URL}/${spec.path}`, spec.label);
  const download = await clickCsvDownload(page);
  const { readFile: rf } = await import('node:fs/promises');
  const tmpPath = await download.path();
  const buf = await rf(tmpPath);
  return persistCsv(buf, download.suggestedFilename() || '', spec, range || {});
}

async function main() {
  const runLog = initRunLog('yahoo-data');
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const targets = (process.env.YDATA_REPORTS || 'overall,inflow,user,flash,item,keyword')
    .split(',').map((s) => s.trim()).filter(Boolean);
  const unknown = targets.filter((t) => !SPECS.some((s) => s.key === t));
  if (targets.length === 0 || unknown.length > 0) {
    console.error(`FATAL: YDATA_REPORTS には ${SPECS.map((s) => s.key).join('/')} を指定してください (不明: ${unknown.join(',') || '(空)'})`);
    process.exitCode = 2;
    return;
  }
  const specs = SPECS.filter((s) => targets.includes(s.key));

  let context;
  try {
    context = await openYahooContext();
  } catch (e) {
    console.error(`⚠️ ブラウザ起動失敗: ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'yahoo', logPath: runLog.logPath,
      failures: [{ reportType: 'ydata(ブラウザ起動)', error: e.message }],
      repro: '多重起動 (プロファイルロック) や Playwright 破損を確認: node scripts/mall-csv-fetcher/yahoo-data-download.mjs',
    }), 'yahoo-data');
    process.exitCode = 1;
    return;
  }
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  const outcomes = [];
  const failures = [];
  try {
    console.log('=== Yahoo!統計CSV 自動DL ===');
    console.log(`対象: ${specs.map((s) => s.key).join(', ')} / 1日単位型の対象日: ${computeDailyTargetDates().join(', ')}`);
    await ensureStoreLogin(page); // セッション検証 (切れていたら 2FA_REQUIRED throw)

    for (const spec of specs) {
      const ranges = spec.daily ? computeDailyTargetDates().map((d) => ({ from: d, to: d })) : [null];
      for (const range of ranges) {
        const ymLabel = range ? range.from : 'default';
        try {
          const status = await fetchOne(page, spec, range);
          outcomes.push({ spec: spec.key, ym: ymLabel, status });
        } catch (e) {
          console.error(`✗ ${spec.label} ${ymLabel}: ${e.message}`);
          await snap(page, `${spec.key}_${ymLabel}_error`);
          await logFetch({
            report_type: spec.reportType, period_from: range?.from || null, period_to: range?.to || null,
            status: 'error', message: e.message,
          });
          outcomes.push({ spec: spec.key, ym: ymLabel, status: 'error' });
          failures.push({ reportType: spec.reportType, ym: ymLabel, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `ydata_${spec.key}_${ymLabel}_error.png`) });
          if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
          break; // 同レポートの残り日はスキップ (他レポートは続行)
        }
      }
    }

    console.log(`\n=== summary: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ') || '(対象なし)'} ===`);
    if (failures.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (incoming/yahoo-data/ に置くだけ。1日単位型はファイル名に yahoo_item_YYYY-MM-DD 形式で日付を入れる)');
      await sendGChat(buildErrorReport({
        mall: 'yahoo', outcomes, failures, logPath: runLog.logPath,
        repro: 'node scripts/mall-csv-fetcher/yahoo-data-download.mjs',
      }), 'yahoo-data');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-yahoo-data.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'yahoo', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'ydata(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'ydata_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/yahoo-login-spike.mjs → Yahoo! JAPAN IDで手動ログイン (確認コードはメール受信)'
        : 'node scripts/mall-csv-fetcher/yahoo-data-download.mjs',
    }), 'yahoo-data');
    process.exitCode = 1;
  } finally {
    if (process.env.HEADLESS !== '1') {
      console.log('\n目視用にブラウザを120秒開いたままにします。Ctrl+Cで終了可。');
      await page.waitForTimeout(120000).catch(() => {});
    }
    await context.close().catch(() => {});
  }
}

main();
