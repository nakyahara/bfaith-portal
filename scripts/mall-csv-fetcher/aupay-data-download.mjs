/**
 * au PAYマーケット 分析CSV自動ダウンロード (mall-csv-fetcher P1-A)
 *
 * 毎朝3系統を取得して incoming/aupay-data/ に置く (取込は import-aupay-data.js):
 *   1. 店舗分析CSV (shopAnalyCsvDl、非同期ジョブ型):
 *      - 日次: D-3〜D-1 を1日1ジョブで insert (5種: 売上/参照元/検索/ページ/商品)
 *        (Codex R2: レンジ1本より1日1本×3が補正検知に有利)
 *      - 月次: 月初3日は前月分 (6種: 上記+リピート)。ADATA_MONTHLY=YYYY-MM[:YYYY-MM] で任意レンジ (バックフィル用)
 *      - insert は UI を使わず POST 直叩き (csrfToken+Struts形式、302=受理。実測でUIのボタン
 *        活性化は headless で不安定)。処理状況テーブルをポーリング → 処理完了 → DL
 *   2. プラチナマッチ広告 日次レポート: GET ?format=csv&aggregate=daily (直近35日、URLパラメータ直取り)
 *   3. プラチナマッチ 検索クエリ: 画面テーブルをスクレイプ → 週次スナップショットJSON
 *      (前週分トップ100・週次更新。月曜は更新前の可能性があるためスキップ)
 *
 * ガード: DL後に取込レシピ (prepareAupayFile) で実パース+種別/期間検証してから incoming 投入。
 * ジョブとinsertの対応付けは「insert直前後のファイル名差分」(fetch-all経由で排他実行が前提。
 * 生成名 <type>_<D|M>_<会員番号>_<受付ms>.csv には論理日付が無いため、CSV前文の抽出年月日が正)。
 *
 * 実行: node scripts/mall-csv-fetcher/aupay-data-download.mjs
 *   env: HEADLESS=1 / ADATA_PARTS=shop,pm,pmquery / ADATA_DAYS=3
 *        ADATA_MONTHLY=2026-01:2026-06 (月次バックフィル) / ADATA_PM_FROM/TO (PMバックフィル)
 */

import { mkdir, copyFile, writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import {
  openAupayContext, gotoWowmaPage, ensureWowmaLogin, gotoSsoConsole, WOWMA_BASE, assertLoginEnv,
} from './lib-aupay-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'aupay-data') : null;

const CSVDL_URL = `${WOWMA_BASE}shopAnalyCsvDl/index`;
const INSERT_URL = `${WOWMA_BASE}shopAnalyCsvDl/insert`;
const PM_CONSOLE = 'https://admin.ads.wowma.s4p.jp';

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);
const slashDate = (iso) => iso.split('-').join('/');
function daysAgoIso(back) {
  const now = jstNow();
  return isoDate(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - back)));
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `adata_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] adata_${label}  url=${page.url()}`);
}

// ─── report_fetch_log (楽天/Yahooと同テーブル。mall='aupay'、fail-soft) ───
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
        VALUES (?, 'aupay', ?, ?, ?, ?, ?, ?, ?, ?)`)
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

/** DL結果 (Buffer) を downloads/ 保存 → 取込レシピ検証 → incoming 投入 */
async function persistFile(buf, destName, { reportType, expectType, periodCheck, okMessage }) {
  const dest = join(DL_DIR, destName);
  await writeFile(dest, buf);
  console.log(`  ✅ 取得成功: ${destName} (${buf.length} bytes)`);

  const { prepareAupayFile } = await import('../../apps/warehouse/aupay-data-lib.js');
  const p = prepareAupayFile(destName, buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らないファイル (${p.error})。画面仕様変更の疑い`);
  if (p.type !== expectType) throw new Error(`DL_VERIFY: 期待種別 ${expectType} と不一致 (${p.type})`);
  if (periodCheck) {
    const err = periodCheck(p);
    if (err) throw new Error(`DL_VERIFY: ${err}`);
  }
  console.log(`  [verify] ${p.label} ${p.records.length}件 (${p.dateFrom}〜${p.dateTo})`);
  for (const w of p.warnings || []) console.log(`  ⚠ ${w}`);

  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    await copyFile(dest, join(INCOMING_DIR, destName));
    console.log(`  → incoming投入: ${join(INCOMING_DIR, destName)}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ');
  }
  await logFetch({
    report_type: reportType, period_from: p.dateFrom, period_to: p.dateTo,
    file_name: destName, file_sha256: createHash('sha256').update(buf).digest('hex'),
    file_bytes: buf.length, status: 'ok', message: okMessage || '',
  });
  return p;
}

// ============================================================
// 1. 店舗分析CSV (shopAnalyCsvDl)
// ============================================================
// dmpDataType: 1=売上 2=参照元 3=検索 4=商品 5=ページ 6=リピート 7=メルマガ (DOM順 1,2,3,5,6,4,7)
const TYPE_PREFIX = { sales: 'aupay_sales', referer: 'aupay_referer', search: 'aupay_search', page: 'aupay_page', product: 'aupay_product', rf: 'aupay_repeat_cohort' };
const DAILY_TYPES = ['1', '2', '3', '5', '4'];   // 5種 (リピートは月次専用、メルマガは未利用)
const MONTHLY_TYPES = ['1', '2', '3', '5', '6', '4']; // 6種 (+リピート)
const DAILY_PREFIXES = ['sales', 'referer', 'search', 'page', 'product'];
const MONTHLY_PREFIXES = [...DAILY_PREFIXES, 'rf'];

function expectedTypeFor(prefix, dm) {
  if (prefix === 'rf') return 'aupay_repeat_cohort_monthly';
  return `${TYPE_PREFIX[prefix]}_${dm === 'D' ? 'daily' : 'monthly'}`;
}

/** 処理状況テーブルの行 [{file, status, hasLink}] */
async function statusRows(page) {
  return page.evaluate(() => {
    const t = [...document.querySelectorAll('table')].find((tb) => /対象ファイル名/.test(tb.innerText));
    if (!t) return [];
    const out = [];
    for (const tr of t.querySelectorAll('tr')) {
      const firstCell = tr.querySelector('td');
      if (!firstCell) continue;
      const file = (firstCell.innerText || '').trim();
      if (!/^[a-z]+_[DM]_\d+_\d+\.csv$/.test(file)) continue;
      const text = (tr.innerText || '').replace(/\s+/g, ' ');
      let status = 'unknown';
      if (/処理完了（データなし）/.test(text)) status = 'no_data';
      else if (/処理完了/.test(text)) status = 'done';
      else if (/処理待ち|処理中/.test(text)) status = 'pending';
      else if (/エラー|失敗/.test(text)) status = 'error';
      const hasLink = [...tr.querySelectorAll('a')].some((a) => /ダウンロード/.test(a.innerText || ''));
      out.push({ file, status, hasLink });
    }
    return out;
  });
}

/** insert を POST 直叩き (実測済みStruts形式。302=受理)。UIのbtnSave活性化に依存しない */
async function submitShopInsert(page, { period, ymFrom, ymTo, start, end, types }) {
  const csrf = await page.evaluate(() => document.querySelector('input[name=csrfToken]')?.value || '');
  if (!csrf) throw new Error('FORM_VERIFY: csrfToken が取得できず (画面仕様変更の疑い)');
  const p = new URLSearchParams();
  p.append('csrfToken', csrf);
  p.append('targetDataPeriod', period === 'daily' ? '1' : '0');
  p.append('csvDlTargetYmFrom', ymFrom || '');
  p.append('csvDlTargetYmTo', ymTo || '');
  p.append('csvDlTargetStartYmd', start || '');
  p.append('csvDlTargetEndYmd', end || '');
  for (const t of ['1', '2', '3', '5', '6', '4', '7']) { // DOM順
    if (types.includes(t)) p.append('dmpDataType', t);
    p.append('_dmpDataType', 'on');
  }
  p.append('shopAnalyCsvDlSex', '0');
  for (let i = 0; i < 3; i++) p.append('_shopAnalyCsvDlSex', 'on');
  p.append('shopAnalyCsvDlAge', '0');
  for (let i = 0; i < 7; i++) p.append('_shopAnalyCsvDlAge', 'on');
  p.append('smartPremium', '0');
  for (let i = 0; i < 2; i++) p.append('_smartPremium', 'on');
  p.append('pageUrl', '');
  p.append('shopAnalyCsvDlLotNumber', '');
  const res = await page.request.post(INSERT_URL, {
    headers: { 'content-type': 'application/x-www-form-urlencoded', referer: CSVDL_URL },
    data: p.toString(),
    maxRedirects: 0,
    timeout: 60000,
  });
  if (res.status() !== 302) {
    throw new Error(`INSERT_FAILED: POST insert が HTTP ${res.status()} (302=受理 が来ない)`);
  }
}

/** insert 前後のファイル名差分で自ジョブを特定 (最大30秒待つ) */
async function waitNewFiles(page, beforeSet, expectedCount, label) {
  for (let i = 0; i < 6; i++) {
    await page.waitForTimeout(i === 0 ? 2500 : 5000);
    await gotoWowmaPage(page, CSVDL_URL, '店舗分析CSVダウンロード');
    const rows = await statusRows(page);
    const fresh = rows.filter((r) => !beforeSet.has(r.file)).map((r) => r.file);
    if (fresh.length >= expectedCount) {
      if (fresh.length > expectedCount) {
        throw new Error(`JOB_BIND: ${label} で新規ファイルが想定より多い (${fresh.length} > ${expectedCount})。並行操作の疑い — 中断`);
      }
      return fresh;
    }
  }
  throw new Error(`JOB_BIND: ${label} のジョブ行が処理状況に現れず (期待 ${expectedCount}件)`);
}

/** ファイル名で行を特定してダウンロード */
async function downloadStatusFile(page, fileName) {
  const dlPromise = page.waitForEvent('download', { timeout: 90000 });
  dlPromise.catch(() => {});
  const clicked = await page.evaluate((fn) => {
    const t = [...document.querySelectorAll('table')].find((tb) => /対象ファイル名/.test(tb.innerText));
    if (!t) return false;
    for (const tr of t.querySelectorAll('tr')) {
      const first = (tr.querySelector('td')?.innerText || '').trim();
      if (first !== fn) continue;
      const a = [...tr.querySelectorAll('a')].find((x) => /ダウンロード/.test(x.innerText || ''));
      if (a) { a.click(); return true; }
    }
    return false;
  }, fileName);
  if (!clicked) throw new Error(`DL_FAILED: ${fileName} のダウンロードリンクが見つからず`);
  const dl = await dlPromise;
  return readFile(await dl.path());
}

/** 月次レンジ (YYYY-MM 〜 YYYY-MM) の月リスト。
 *  ⚠️文字列インクリメントは不正月 (2026-13等) で無限ループする → 年月indexの整数で列挙 (Codex Medium) */
function monthsBetween(fromYm, toYm) {
  const idx = (ym) => {
    const m = ym.match(/^(\d{4})-(0[1-9]|1[0-2])$/);
    if (!m) throw new Error(`ENV: 不正な年月 ${ym} (YYYY-MM、月は01-12)`);
    return Number(m[1]) * 12 + Number(m[2]) - 1;
  };
  const a = idx(fromYm); const b = idx(toYm);
  if (a > b) throw new Error(`ENV: 月次レンジの from > to (${fromYm} > ${toYm})`);
  const out = [];
  for (let i = a; i <= b; i++) {
    out.push(`${Math.floor(i / 12)}-${String((i % 12) + 1).padStart(2, '0')}`);
  }
  return out;
}

async function fetchShopAnalytics(page, outcomes, failures) {
  await gotoWowmaPage(page, CSVDL_URL, '店舗分析CSVダウンロード');
  // run開始時点の既存ファイル (並行操作検出の基準)
  const initialFiles = new Set((await statusRows(page)).map((r) => r.file));

  // ジョブ計画
  const jobs = [];
  const days = Math.min(Math.max(parseInt(process.env.ADATA_DAYS, 10) || 3, 1), 14);
  for (let back = days; back >= 1; back--) {
    const d = daysAgoIso(back);
    jobs.push({ dm: 'D', label: `日次 ${d}`, from: d, to: d, types: DAILY_TYPES, prefixes: DAILY_PREFIXES,
      params: { period: 'daily', start: slashDate(d), end: slashDate(d) } });
  }
  const monthlyEnv = (process.env.ADATA_MONTHLY || '').trim();
  let monthlyRange = null;
  if (monthlyEnv) {
    const m = monthlyEnv.match(/^(\d{4}-(?:0[1-9]|1[0-2]))(?::(\d{4}-(?:0[1-9]|1[0-2])))?$/);
    if (!m) throw new Error(`ENV: ADATA_MONTHLY は YYYY-MM または YYYY-MM:YYYY-MM (月は01-12。got: ${monthlyEnv})`);
    monthlyRange = { from: m[1], to: m[2] || m[1] };
  } else if (jstNow().getUTCDate() <= 3) {
    // 月初3日は前月の月次を確定取得
    const now = jstNow();
    const prev = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));
    const ym = prev.toISOString().slice(0, 7);
    monthlyRange = { from: ym, to: ym };
  }
  if (monthlyRange) {
    if (monthlyRange.from > monthlyRange.to) throw new Error('ENV: ADATA_MONTHLY の from > to');
    jobs.push({ dm: 'M', label: `月次 ${monthlyRange.from}〜${monthlyRange.to}`,
      from: `${monthlyRange.from}-01`, to: `${monthlyRange.to}-01`,
      expectMonths: monthsBetween(monthlyRange.from, monthlyRange.to),
      types: MONTHLY_TYPES, prefixes: MONTHLY_PREFIXES,
      params: { period: 'monthly', ymFrom: slashDate(monthlyRange.from), ymTo: slashDate(monthlyRange.to) } });
  }
  console.log(`  ジョブ計画: ${jobs.map((j) => j.label).join(' / ')}`);

  // insert (直前後のファイル名差分で自ジョブを bind)
  for (const job of jobs) {
    const before = new Set((await statusRows(page)).map((r) => r.file));
    await submitShopInsert(page, { ...job.params, types: job.types });
    job.names = await waitNewFiles(page, before, job.types.length, job.label);
    console.log(`  [insert] ${job.label} → ${job.names.length}件受理 (${job.names.map((n) => n.split('_')[0]).join(',')})`);
  }

  // ポーリング (全ジョブ完了まで、最大15分)
  const allNames = new Set(jobs.flatMap((j) => j.names));
  let rows = [];
  const deadline = Date.now() + 15 * 60 * 1000;
  for (;;) {
    rows = await statusRows(page);
    const mine = rows.filter((r) => allNames.has(r.file));
    const pending = mine.filter((r) => r.status === 'pending' || r.status === 'unknown');
    const errored = mine.filter((r) => r.status === 'error');
    if (errored.length > 0) {
      throw new Error(`JOB_FAILED: 処理エラーのジョブ: ${errored.map((r) => r.file).join(', ')}`);
    }
    if (mine.length === allNames.size && pending.length === 0) break;
    if (Date.now() > deadline) {
      throw new Error(`JOB_TIMEOUT: 15分待っても未完了: ${pending.map((r) => r.file).join(', ') || '(行が見つからない)'}`);
    }
    console.log(`  [poll] 完了待ち ${allNames.size - pending.length}/${allNames.size} ...`);
    await page.waitForTimeout(20000);
    await gotoWowmaPage(page, CSVDL_URL, '店舗分析CSVダウンロード');
  }
  console.log(`  [poll] 全${allNames.size}ジョブ完了`);

  // 並行操作の検出 (Codex R2 Medium): このrun中に現れた「自分がbindしていない」新規ファイルが
  // あれば、bindの取り違えの可能性があるため通知に載せる (取込自体は期間検証で守られている)
  {
    const unclaimed = rows.map((r) => r.file).filter((f) => !allNames.has(f) && !initialFiles.has(f));
    if (unclaimed.length > 0) {
      const msg = `並行操作の疑い: このrun中に自ジョブ以外の新規ファイルが出現 (${unclaimed.slice(0, 5).join(', ')})。bindの取り違えが無いか要確認`;
      console.warn(`  ⚠ ${msg}`);
      failures.push({ reportType: 'adata_shop(job-bind)', error: msg, url: page.url() });
    }
  }

  // DL → 検証 → incoming
  const rowByFile = new Map(rows.map((r) => [r.file, r]));
  const ts = new Date().toISOString().replace(/[-:.]/g, '').replace('Z', '');
  for (const job of jobs) {
    for (const name of job.names) {
      const prefix = name.split('_')[0];
      const key = `shop_${prefix}_${job.dm}`;
      const row = rowByFile.get(name);
      try {
        if (!job.prefixes.includes(prefix)) {
          throw new Error(`JOB_BIND: 想定外の種別 ${prefix} のファイルが自ジョブに混入 (${name})`);
        }
        if (row.status === 'no_data') {
          console.log(`  [empty] ${job.label} ${prefix}: 処理完了（データなし）— 正常スキップ`);
          await logFetch({ report_type: `adata_${prefix}_${job.dm}`, period_from: job.from, period_to: job.to,
            status: 'empty', message: '処理完了（データなし）' });
          outcomes.push({ spec: key, ym: job.from, status: 'empty' });
          continue;
        }
        if (!row.hasLink) throw new Error(`DL_FAILED: ${name} は処理完了だがダウンロードリンクなし`);
        const buf = await downloadStatusFile(page, name);
        const destName = `aupay_${prefix}_${job.dm}_d${job.from}_${ts}.csv`;
        await persistFile(buf, destName, {
          reportType: `adata_${prefix}_${job.dm}`,
          expectType: expectedTypeFor(prefix, job.dm),
          periodCheck: (p) => {
            if (job.dm === 'D') {
              if (p.dateFrom !== job.from || p.dateTo !== job.to) {
                return `期待日 ${job.from} と実CSV ${p.dateFrom}〜${p.dateTo} が不一致`;
              }
            } else {
              if (p.dateFrom < job.from || p.dateTo > job.to) {
                return `期待範囲 ${job.from}〜${job.to} 外のデータ (${p.dateFrom}〜${p.dateTo})`;
              }
              // 欠け月は正常あり得る (店舗開設前など) → warning ログのみ
              const got = new Set(p.records.map((r) => r.date_jst.slice(0, 7)));
              const missing = (job.expectMonths || []).filter((ym) => !got.has(ym));
              if (missing.length > 0) console.log(`  ⚠ ${prefix}: データの無い月 ${missing.join(', ')} (店舗開設前/実績なしなら正常)`);
            }
            return null;
          },
        });
        outcomes.push({ spec: key, ym: job.from, status: 'ok' });
      } catch (e) {
        console.error(`✗ ${job.label} ${prefix}: ${e.message}`);
        await snap(page, `${prefix}_${job.dm}_error`);
        await logFetch({ report_type: `adata_${prefix}_${job.dm}`, period_from: job.from, period_to: job.to,
          status: 'error', message: e.message });
        outcomes.push({ spec: key, ym: job.from, status: 'error' });
        failures.push({ reportType: `adata_${prefix}_${job.dm}`, ym: job.from, error: e.message, url: page.url() });
      }
    }
  }
}

// ============================================================
// 2. プラチナマッチ広告 日次レポート (URLパラメータ付きCSV直GET)
// ============================================================
async function fetchPmReport(page, outcomes, failures) {
  const from = (process.env.ADATA_PM_FROM || '').trim() || daysAgoIso(35);
  const to = (process.env.ADATA_PM_TO || '').trim() || daysAgoIso(1);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to) || from > to) {
    throw new Error(`ENV: ADATA_PM_FROM/TO が不正 (${from}〜${to})`);
  }
  console.log(`\n--- PM広告 日次レポート ${from}〜${to} ---`);
  await gotoSsoConsole(page, 'sso/doPmadSso', `${PM_CONSOLE}/ext/shop/auth?auth=sso`,
    { expectHost: 'admin.ads.wowma.s4p.jp', expectText: 'プラチナマッチ', label: 'プラチナマッチ管理' });

  const url = `${PM_CONSOLE}/shop/report?format=csv&termType=custom&begin=${from}&end=${to}&aggregate=daily`;
  const res = await page.request.get(url, { timeout: 90000 });
  if (!res.ok()) throw new Error(`DL_FAILED: PMレポートCSVが HTTP ${res.status()}`);
  const ct = res.headers()['content-type'] || '';
  const buf = Buffer.from(await res.body());
  if (/text\/html/i.test(ct)) throw new Error(`DL_VERIFY: PMレポートCSVがHTMLを返した (ct=${ct}) — セッション/仕様変更の疑い`);
  const ts = new Date().toISOString().replace(/[-:.]/g, '').replace('Z', '');
  await persistFile(buf, `aupay_pmad_d${from}_${ts}.csv`, {
    reportType: 'adata_pm_ad_daily',
    expectType: 'aupay_pm_ad_daily',
    periodCheck: (p) => (p.dateFrom < from || p.dateTo > to)
      ? `期待範囲 ${from}〜${to} 外のデータ (${p.dateFrom}〜${p.dateTo})` : null,
  });
  outcomes.push({ spec: 'pm_ad', ym: from, status: 'ok' });
}

// ============================================================
// 3. プラチナマッチ 検索クエリ (週次スナップショット)
// ============================================================
/** 直近の「完了した週」の月曜 (JST)。画面は前週分を月曜12:00までに更新 */
function lastCompletedWeekMonday() {
  const now = jstNow();
  const dow = (now.getUTCDay() + 6) % 7; // 月曜=0
  const thisMonday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - dow));
  return isoDate(new Date(thisMonday.getTime() - 7 * 86400000));
}

async function fetchPmQuery(page, outcomes, failures) {
  console.log('\n--- PM検索クエリ (前週分トップ100) ---');
  if ((jstNow().getUTCDay() + 6) % 7 === 0) {
    // 月曜: 前週分への更新が12:00までで、朝の実行時点は前々週データの可能性 → 日付誤ラベル防止でスキップ
    console.log('  [skip] 月曜は更新前の可能性があるためスキップ (火曜以降に取得)');
    outcomes.push({ spec: 'pm_query', ym: 'monday', status: 'empty' });
    return;
  }
  const weekStart = lastCompletedWeekMonday();
  const weekEnd = isoDate(new Date(new Date(weekStart + 'T00:00:00Z').getTime() + 6 * 86400000));

  await gotoSsoConsole(page, 'sso/doPmadSso', `${PM_CONSOLE}/ext/shop/auth?auth=sso`,
    { expectHost: 'admin.ads.wowma.s4p.jp', expectText: 'プラチナマッチ', label: 'プラチナマッチ管理' });
  await page.goto(`${PM_CONSOLE}/shop/inflow_words`, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);
  const body = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
  if (!/検索クエリ/.test(body)) throw new Error('AUTH_VERIFY: 検索クエリ画面の証拠を確認できず');

  const rows = await page.evaluate(() => {
    const tables = [...document.querySelectorAll('table')];
    const t = tables.find((tb) => /キーワード/.test(tb.innerText) && /広告表示回数/.test(tb.innerText));
    if (!t) return null;
    const out = [];
    for (const tr of t.querySelectorAll('tr')) {
      const tds = [...tr.querySelectorAll('td')].map((td) => (td.innerText || '').trim());
      if (tds.length < 4) continue;
      let rank = null; let kw; let rest;
      if (tds.length >= 5 && /^\d+\.?$/.test(tds[0])) {
        rank = parseInt(tds[0], 10); kw = tds[1]; rest = tds.slice(2, 5);
      } else {
        const m = tds[0].match(/^(\d+)\.\s*(.+)$/s);
        if (m) { rank = parseInt(m[1], 10); kw = m[2].trim(); } else { kw = tds[0]; }
        rest = tds.slice(1, 4);
      }
      const num = (s) => { const n = Number(String(s).replace(/[,%\s]/g, '')); return Number.isFinite(n) ? n : null; };
      out.push({ rank, keyword: kw, impressions: num(rest[0]), clicks: num(rest[1]), ctr_pct: num(rest[2]) });
    }
    return out;
  });
  if (rows === null) throw new Error('DL_VERIFY: 検索クエリのテーブルが見つからず (画面仕様変更の疑い)');
  if (rows.length === 0) {
    console.log('  [empty] 検索クエリ 0件 — 正常スキップ');
    await logFetch({ report_type: 'adata_pm_query', period_from: weekStart, period_to: weekEnd, status: 'empty', message: '0件' });
    outcomes.push({ spec: 'pm_query', ym: weekStart, status: 'empty' });
    return;
  }
  // 画面に対象週の日付表示が無い → 前週として保存する前に、既取得週と内容が完全一致なら
  // 「画面が未更新 (前々週のまま)」とみなしてスキップ (日付誤ラベル防止 — Codex Medium)。
  // rows_sha256 は report_fetch_log.message に残し、次回照合に使う
  const rowsHash = createHash('sha256').update(JSON.stringify(rows)).digest('hex');
  if (WAREHOUSE_DATA_DIR) {
    try {
      const { default: Database } = await import('better-sqlite3');
      const db = new Database(join(WAREHOUSE_DATA_DIR, 'warehouse.db'), { readonly: true });
      try {
        const prev = db.prepare(`SELECT period_from, message FROM report_fetch_log
          WHERE mall='aupay' AND report_type='adata_pm_query' AND status='ok'
          ORDER BY fetched_at DESC LIMIT 1`).get();
        if (prev && prev.period_from !== weekStart && String(prev.message || '').includes(`rows_sha256=${rowsHash}`)) {
          console.log(`  [skip] 内容が既取得週 (${prev.period_from}) と完全一致 — 画面未更新の疑い。翌日再取得で自己修復`);
          await logFetch({ report_type: 'adata_pm_query', period_from: weekStart, period_to: weekEnd,
            status: 'empty', message: `画面未更新の疑い (${prev.period_from} と同一内容)` });
          outcomes.push({ spec: 'pm_query', ym: weekStart, status: 'empty' });
          return;
        }
      } finally { db.close(); }
    } catch (e) {
      console.warn(`  ⚠ 既取得週との照合スキップ (${e.message})`);
    }
  }
  const json = {
    kind: 'aupay_pm_query_weekly', week_start: weekStart, week_end: weekEnd,
    fetched_at: new Date().toISOString(), source: 'admin.ads.wowma.s4p.jp/shop/inflow_words', rows,
  };
  const ts = new Date().toISOString().replace(/[-:.]/g, '').replace('Z', '');
  await persistFile(Buffer.from(JSON.stringify(json, null, 1)), `aupay_pmquery_d${weekStart}_${ts}.json`, {
    reportType: 'adata_pm_query',
    expectType: 'aupay_pm_query_weekly',
    periodCheck: (p) => (p.dateFrom !== weekStart ? `week_start不一致 (${p.dateFrom} != ${weekStart})` : null),
    okMessage: `rows_sha256=${rowsHash}`,
  });
  outcomes.push({ spec: 'pm_query', ym: weekStart, status: 'ok' });
}

// ============================================================
async function main() {
  const runLog = initRunLog('aupay-data');
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const parts = (process.env.ADATA_PARTS || 'shop,pm,pmquery').split(',').map((s) => s.trim()).filter(Boolean);
  const unknown = parts.filter((t) => !['shop', 'pm', 'pmquery'].includes(t));
  if (parts.length === 0 || unknown.length > 0) {
    console.error(`FATAL: ADATA_PARTS には shop/pm/pmquery を指定してください (不明: ${unknown.join(',') || '(空)'})`);
    process.exitCode = 2;
    return;
  }
  try {
    assertLoginEnv();
  } catch (e) {
    console.error(`FATAL: ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'aupay', logPath: runLog.logPath,
      failures: [{ reportType: 'adata(env)', error: e.message }],
      repro: 'miniPCのリポジトリ直下 .env に WOWMA_LOGIN_ID / WOWMA_LOGIN_PW を記入',
    }), 'aupay-data');
    process.exitCode = 2;
    return;
  }

  let context;
  try {
    context = await openAupayContext();
  } catch (e) {
    console.error(`⚠️ ブラウザ起動失敗: ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'aupay', logPath: runLog.logPath,
      failures: [{ reportType: 'adata(ブラウザ起動)', error: e.message }],
      repro: '多重起動 (プロファイルロック) や Playwright 破損を確認: node scripts/mall-csv-fetcher/aupay-data-download.mjs',
    }), 'aupay-data');
    process.exitCode = 1;
    return;
  }
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  const outcomes = [];
  const failures = [];
  try {
    console.log('=== au PAYマーケット 分析CSV 自動DL ===');
    console.log(`対象: ${parts.join(', ')}`);
    await ensureWowmaLogin(page);

    for (const part of parts) {
      try {
        if (part === 'shop') await fetchShopAnalytics(page, outcomes, failures);
        else if (part === 'pm') await fetchPmReport(page, outcomes, failures);
        else if (part === 'pmquery') await fetchPmQuery(page, outcomes, failures);
      } catch (e) {
        // part単位の失敗 (JOB_TIMEOUT等) は記録して次のpartへ。認証系は全体停止
        console.error(`✗ ${part}: ${e.message}`);
        await snap(page, `${part}_error`);
        await logFetch({ report_type: `adata_${part}`, period_from: null, period_to: null, status: 'error', message: e.message });
        outcomes.push({ spec: part, ym: '-', status: 'error' });
        failures.push({ reportType: `adata_${part}`, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `adata_${part}_error.png`) });
        if (String(e.message).startsWith('2FA_REQUIRED') || String(e.message).startsWith('AUTH_')) throw e;
      }
    }

    console.log(`\n=== summary: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ') || '(対象なし)'} ===`);
    if (failures.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (incoming/aupay-data/ に置くだけ。CSVの抽出年月日で自己記述)');
      await sendGChat(buildErrorReport({
        mall: 'aupay', outcomes, failures, logPath: runLog.logPath,
        repro: 'node scripts/mall-csv-fetcher/aupay-data-download.mjs',
      }), 'aupay-data');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-aupay-data.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'aupay', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'adata(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'adata_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/aupay-login-spike.mjs → メール暗証番号を入力して端末記憶を再登録'
        : 'node scripts/mall-csv-fetcher/aupay-data-download.mjs',
    }), 'aupay-data');
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
