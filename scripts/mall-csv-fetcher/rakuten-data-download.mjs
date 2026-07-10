/**
 * 楽天RMS「データ分析」CSV自動ダウンロード (mall-csv-fetcher P1-R2)
 *
 * 毎晩2レポートを取得して incoming/rakuten-data/ に置く (取込は import-rakuten-data.js):
 *   1. 商品分析 (SKU×日次)   : datatool.rms.rakuten.co.jp/access/item → 「全商品CSV」→全件DL
 *      → fact_rakuten_item_daily。対象日 = 昨日+一昨日 (RDATA_ITEM_DAYS で変更可)。
 *      1日単位でしかDLできない (取込側ガードとも整合) ため日ごとにループ。
 *   2. 店舗日次 (分析用レポート): datatool.rms.rakuten.co.jp/datatool/data/ → 「CSVダウンロード」
 *      → fact_rakuten_store_daily。対象月 = 今月 (月初3日は先月も)。過去日は再DLで
 *      ベンチマーク等が埋まるため毎晩月全体を取り直して UPSERT。
 *
 * ★実測 (2026-07-10 miniPC headless):
 *   - 商品分析の期間入力は daterangepicker で fill 不可 → JSネイティブsetter+input/changeイベント
 *     で書き換わり、画面の「対象期間:」表示も追従する
 *   - 「全商品CSV」はモーダルを開く。デフォルトは「件数を指定(1,000)」→ **「全件」を明示選択**
 *     しないと上位1,000行で切られる (実測: 全件=1,372行 / 未選択=1,000行)
 *   - 「データ更新日」(=集計済み最新日) が画面に出る。それより新しい日は未集計 → 対象日を
 *     更新日でクランプ (RPP #467 の早朝境界と同じ構え。翌朝の再取得で埋まる)
 *   - 店舗日次の「CSVダウンロード」もモーダル (日次データタブ+ダウンロードボタン)。
 *     カレンダー入力 (calendar-base) も同じJSセッター方式で書き換え可
 *   - どちらも同期ダウンロード (履歴ページ経由なし、クリック→即downloadイベント)
 *
 * ガード:
 *   - フォーム読み戻し検証 (RPPのFORM_VERIFYと同方針): 日次radio/絞り込み・端末=すべて/
 *     キーワード空/全件radio を確認してからDL
 *   - DL後に取込レシピ (prepareDataFile) で実際にパースし、通らないCSVは incoming に置かない
 *   - 商品分析がちょうど1,000行 = 全件選択が効いていない疑い → エラー扱い
 *
 * 実行: node scripts/mall-csv-fetcher/rakuten-data-download.mjs
 *   env: HEADLESS=1 / RDATA_REPORTS=item,store で絞り込み / RDATA_ITEM_DAYS=2 (対象日数)
 *        RDATA_STORE_REPORT=<レポート名> で店舗日次のレポート選択を明示 (省略時は前回選択のまま)
 */

import { mkdir, copyFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openContext, ensureRmsLogin, safeHost, assertLoginEnv } from './lib-rakuten-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const ITEM_URL = 'https://datatool.rms.rakuten.co.jp/access/item';   // 商品ページ分析 (=商品分析CSV)
const STORE_URL = 'https://datatool.rms.rakuten.co.jp/datatool/data/'; // 分析用レポート (店舗日次)
const DD_URL = 'https://datatool.rms.rakuten.co.jp/datadownload';    // データダウンロードハブ (7種)

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'rakuten-data') : null;

function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
const isoDate = (dt) => dt.toISOString().slice(0, 10);
const slashDate = (iso) => iso.split('-').join('/');

/** 商品分析の対象日 (JST): 昨日+一昨日 (古い順)。毎晩2日分取り直すことで、
 *  早朝の集計遅れ (データ更新日が前々日のまま) で抜けた日も翌朝の再取得で自己修復する */
function computeItemTargetDates() {
  const days = Math.min(Math.max(parseInt(process.env.RDATA_ITEM_DAYS, 10) || 2, 1), 14);
  const now = jstNow();
  const dates = [];
  for (let back = days; back >= 1; back--) {
    dates.push(isoDate(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - back))));
  }
  return dates;
}

/** 店舗日次の対象月: 今月 (月初3日は先月も。RPPと同方針) */
function computeStoreTargetMonths() {
  const now = jstNow();
  const y = now.getUTCFullYear(), m = now.getUTCMonth(), d = now.getUTCDate();
  const backs = d <= 3 ? [1, 0] : [0];
  const months = [];
  for (const back of backs) {
    const first = new Date(Date.UTC(y, m - back, 1));
    const last = new Date(Date.UTC(y, m - back + 1, 0));
    const yesterday = new Date(Date.UTC(y, m, d - 1));
    if (first > yesterday) continue; // 月初1日実行の「今月」は実績なし
    months.push({ ym: isoDate(first).slice(0, 7), from: isoDate(first), to: isoDate(last) });
  }
  return months;
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rdata_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rdata_${label}  url=${page.url()}`);
}

/** datatool の画面に到達しているか確認 (セッション切れは再ログイン1回) */
async function gotoDatatool(page, url, label) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(2000);
  if (safeHost(page.url()) !== 'datatool.rms.rakuten.co.jp') {
    await ensureRmsLogin(page);
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(2000);
  }
  if (safeHost(page.url()) !== 'datatool.rms.rakuten.co.jp') {
    throw new Error(`${label}画面に到達できず: ${page.url()}`);
  }
}

/** 日付レンジ入力 (daterangepicker) をJSネイティブsetterで書き換える (実測: fill は widget に
 *  巻き戻される。setter+input/change+Enter で内部状態も追従し「対象期間:」表示が変わる) */
async function setRangeInput(page, markAttr, value) {
  const result = await page.evaluate(([attr, val]) => {
    const el = document.querySelector(`[${attr}="1"]`);
    if (!el) return { ok: false, reason: 'input not found' };
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(el, val);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
    el.blur();
    return { ok: true, value: el.value };
  }, [markAttr, value]).catch((e) => ({ ok: false, reason: e.message }));
  if (!result.ok) throw new Error(`FORM_VERIFY: 期間入力の書き換え失敗 (${result.reason})`);
  await page.waitForTimeout(2500); // 画面のデータ再取得待ち
  return page.evaluate((attr) => document.querySelector(`[${attr}="1"]`)?.value || '', markAttr);
}

/** ボタン/ラベルをテキスト完全一致でクリック (「ダウンロード」が「CSVダウンロード」に
 *  誤マッチしないよう exact match。モーダル内は DOM 後方に出るので last を使う) */
async function clickExact(page, texts, label, { last = false } = {}) {
  for (const t of texts) {
    const loc = page.locator('button, a, label', { hasText: new RegExp(`^\\s*${t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`) });
    const el = last ? loc.last() : loc.first();
    if (await el.isVisible().catch(() => false)) {
      await el.click({ timeout: 10000 });
      console.log(`  [click] ${label}: "${t}"${last ? ' (last)' : ''}`);
      return true;
    }
  }
  return false;
}

// ─── report_fetch_log (RPPと同テーブル。fail-soft) ───
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
        VALUES (?, 'rakuten', ?, ?, ?, ?, ?, ?, ?, ?)`)
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

/** DLファイルを取込レシピで検証 → downloads/ 保存 + incoming/ 投入 + fetch log */
async function persistDownload(download, reportType, range, expect) {
  const suggested = download.suggestedFilename() || '';
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const dest = join(DL_DIR, `${reportType}_${range.from}_${range.to}_${ts}.csv`); // reportType=rdata_* (cleanup regexと整合)
  await download.saveAs(dest);
  console.log(`  ✅ ダウンロード成功: ${basename(dest)} (元名: ${suggested || '(なし)'})`);

  const buf = await readFile(dest);
  const sha256 = createHash('sha256').update(buf).digest('hex');

  // 取込レシピで実際にパースして検証 — 通らないCSVは incoming に置かない (エラーで気づく)
  const { prepareDataFile } = await import('../../apps/warehouse/rakuten-data-lib.js');
  const p = prepareDataFile(suggested || basename(dest), buf);
  if (!p.ok) throw new Error(`DL_VERIFY: 取込レシピを通らないCSV (${p.error})。画面仕様変更の疑い`);
  if (p.type !== expect.type) throw new Error(`DL_VERIFY: 期待種別 ${expect.type} と不一致 (${p.type})`);
  if (expect.type === 'rakuten_item_daily' && p.records.length === 1000) {
    throw new Error('DL_VERIFY: ちょうど1,000行 = 「全件」選択が効いていない疑い (上位1,000件カット)。画面のモーダル仕様変更を確認');
  }
  // 実CSVの対象期間検証 (Codex R1 High): UI表示が追従して見えてもDLが別期間を返したら
  // 意図しないデータを黙って投入することになる → 不一致は error (incoming に置かない)
  if (expect.dateFrom && p.dateFrom !== expect.dateFrom) {
    throw new Error(`DL_VERIFY: 期待日 ${expect.dateFrom} と実CSV ${p.dateFrom} が不一致 (前回期間/クランプ後のCSVを掴んだ疑い)`);
  }
  // 店舗日次: 月初日から始まらないCSVは部分DL (取込はUPSERTのみで欠け日が残る) → error。
  // dateTo は未来日=未集計スキップ設計のため月末未満を許容 (Codex R2 Medium)。
  // loose=true (カテゴリ等、アクセスがあった日しか行が無いデータ) は月内チェックのみ
  if (expect.month) {
    const inMonth = p.dateFrom.slice(0, 7) === expect.month.ym && p.dateTo <= expect.month.to;
    const startsAtFirst = p.dateFrom === expect.month.from;
    if (!(inMonth && (expect.month.loose || startsAtFirst))) {
      throw new Error(`DL_VERIFY: 期待月 ${expect.month.ym} (${expect.month.from}〜) と実CSV ${p.dateFrom}〜${p.dateTo} が不一致 (部分CSVの疑い)`);
    }
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
    report_type: reportType, period_from: p.dateFrom, period_to: p.dateTo,
    file_name: basename(dest), file_sha256: sha256, file_bytes: buf.length,
    status: 'ok', message: '',
  });
  return dest;
}

/** ページの「データ更新日: YYYY/MM/DD」を読む (商品分析の集計済み最新日) */
async function readUpdateDate(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const m = body.match(/データ更新日[::]?\s*(\d{4}\/\d{2}\/\d{2})/);
  return m ? m[1].replace(/\//g, '-') : null;
}

// ─── レポート1: 商品分析 (SKU×日次) ───
async function fetchItemDaily(page, dateIso) {
  console.log(`\n--- 商品分析 (SKU×日次) ${dateIso} ---`);
  await gotoDatatool(page, ITEM_URL, '商品ページ分析');

  // フォーム検証: 日次radio選択済み + 絞り込み/端末=すべて + キーワード空 (デフォルトのはずだが読み戻し確認)
  const form = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const radios = [...document.querySelectorAll('input[type=radio]')].filter(vis);
    const checkedVals = radios.filter((r) => r.checked).map((r) => r.value);
    const dateEl = [...document.querySelectorAll('input[type=text]')].filter(vis)
      .find((e) => /^\d{4}\/\d{2}\/\d{2} - \d{4}\/\d{2}\/\d{2}$/.test(e.value));
    if (dateEl) dateEl.setAttribute('data-rdata-date', '1');
    const keywordEl = [...document.querySelectorAll('input[type=text]')].filter(vis)
      .find((e) => !/\d{4}\//.test(e.value) && e !== dateEl);
    return {
      checkedVals,
      hasDate: !!dateEl,
      keyword: keywordEl ? keywordEl.value : '',
    };
  });
  console.log(`  [form] checked=${JSON.stringify(form.checkedVals)} keyword="${form.keyword}"`);
  if (!form.hasDate) throw new Error('FORM_VERIFY: 日付レンジ入力が見つからず (画面変更の疑い)');
  if (!form.checkedVals.includes('daily')) throw new Error('FORM_VERIFY: 期間選択が「日次」になっていない');
  const banned = ['monthly', 'itemName', 'mngNumber', 'genre', 'pc', 'sdApp', 'sdWeb'];
  if (form.checkedVals.some((v) => banned.includes(v))) {
    throw new Error(`FORM_VERIFY: 絞り込み/端末がデフォルト(すべて)でない (checked=${form.checkedVals.join(',')})`);
  }
  if (form.keyword.trim() !== '') throw new Error(`FORM_VERIFY: キーワード欄が空でない ("${form.keyword}")`);

  // 集計済み最新日 (データ更新日) より新しい日は未集計 → スキップ (翌朝の再取得で埋まる)
  const updateDate = await readUpdateDate(page);
  console.log(`  [update] データ更新日=${updateDate || '(読取不能)'}`);
  if (updateDate && dateIso > updateDate) {
    console.log(`  [skip] ${dateIso} は未集計 (更新日=${updateDate})。翌朝の再取得で埋まる — 正常スキップ`);
    await logFetch({
      report_type: 'rdata_item_daily', period_from: dateIso, period_to: dateIso,
      status: 'empty', message: `未集計 (データ更新日=${updateDate})`,
    });
    return 'empty';
  }

  // 対象日をセット (JSセッター) → 読み戻し検証
  const want = `${slashDate(dateIso)} - ${slashDate(dateIso)}`;
  const got = await setRangeInput(page, 'data-rdata-date', want);
  if (got !== want) throw new Error(`FORM_VERIFY: 対象日が設定できず (期待 ${want} / 実際 ${got})`);
  const body = await page.locator('body').innerText().catch(() => '');
  const taisho = (body.match(/対象期間[::]?\s*([\d/ -]+)/) || [])[1]?.trim() || '';
  if (!taisho.startsWith(slashDate(dateIso))) {
    throw new Error(`FORM_VERIFY: 画面の対象期間が追従していない (期待 ${want} / 画面 ${taisho})`);
  }
  console.log(`  [date] 対象期間=${taisho}`);

  // 全商品CSV → モーダル → 全件 → ダウンロード (同期DL)
  if (!(await clickExact(page, ['全商品CSV'], '全商品CSVモーダル'))) {
    throw new Error('FORM_VERIFY: 「全商品CSV」ボタンが見つからず');
  }
  await page.waitForTimeout(1500);
  if (!(await clickExact(page, ['全件'], '全件radio', { last: true }))) {
    throw new Error('FORM_VERIFY: モーダルの「全件」が見つからず');
  }
  await page.waitForTimeout(500);
  const zenkenOk = await page.evaluate(() => {
    // 可視ラベルに限定 (モーダル外の同名要素への誤マッチ防止 — Codex R1 Low)
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const labels = [...document.querySelectorAll('label')].filter((l) => vis(l) && /全件/.test(l.textContent || ''));
    return labels.some((l) => {
      const input = l.querySelector('input[type=radio]')
        || (l.htmlFor ? document.getElementById(l.htmlFor) : null)
        || (l.previousElementSibling?.matches?.('input[type=radio]') ? l.previousElementSibling : null);
      return input && input.checked;
    });
  }).catch(() => false);
  if (!zenkenOk) throw new Error('FORM_VERIFY: 「全件」radioの選択を確認できず (1,000行カット防止のため中止)');

  const dlPromise = page.waitForEvent('download', { timeout: 90000 });
  dlPromise.catch(() => {}); // ボタン探索失敗でthrowする経路の未処理reject防止 (Codex R2 Medium)
  if (!(await clickExact(page, ['ダウンロード'], 'モーダルDL実行', { last: true }))) {
    throw new Error('FORM_VERIFY: モーダルの「ダウンロード」ボタンが見つからず');
  }
  const download = await dlPromise;
  await persistDownload(download, 'rdata_item_daily', { from: dateIso, to: dateIso },
    { type: 'rakuten_item_daily', dateFrom: dateIso });
  return 'ok';
}

// ─── レポート2: 店舗日次 (分析用レポート) ───
async function fetchStoreDaily(page, month) {
  console.log(`\n--- 店舗日次 (分析用レポート) ${month.ym} ---`);
  await gotoDatatool(page, STORE_URL, '分析用レポート');

  // レポート選択の確認 (列構成はレポート設定に依存 → DL後に prepareDataFile 検証で担保。
  // RDATA_STORE_REPORT 指定時のみ明示選択)
  const wantReport = (process.env.RDATA_STORE_REPORT || '').trim();
  const sel = await page.evaluate(() => {
    const el = document.querySelector('#js-select-report');
    return el ? { value: el.value, label: el.selectedOptions[0]?.text || '' } : null;
  }).catch(() => null);
  if (!sel) throw new Error('FORM_VERIFY: レポート選択 (#js-select-report) が見つからず');
  console.log(`  [report] 選択中: "${sel.label}" (value=${sel.value})`);
  if (wantReport && sel.label !== wantReport) {
    await page.selectOption('#js-select-report', { label: wantReport });
    await page.waitForTimeout(2500);
    console.log(`  [report] "${wantReport}" に切替`);
  }

  // 対象月をセット (calendar-base、JSセッター方式)
  const marked = await page.evaluate(() => {
    const el = document.querySelector('#calendar-base');
    if (!el) return false;
    el.setAttribute('data-rdata-date', '1');
    return true;
  });
  if (!marked) throw new Error('FORM_VERIFY: 期間入力 (#calendar-base) が見つからず');
  const want = `${slashDate(month.from)} - ${slashDate(month.to)}`;
  const got = await setRangeInput(page, 'data-rdata-date', want);
  if (got !== want) throw new Error(`FORM_VERIFY: 対象月が設定できず (期待 ${want} / 実際 ${got})`);
  console.log(`  [date] 対象期間=${got}`);

  // CSVダウンロード → モーダル → ダウンロード (同期DL)。CSVの中身 (データ対象期間/列構成)
  // は persistDownload の prepareDataFile 検証で確認する
  if (!(await clickExact(page, ['CSVダウンロード'], 'CSVダウンロードモーダル'))) {
    throw new Error('FORM_VERIFY: 「CSVダウンロード」ボタンが見つからず');
  }
  await page.waitForTimeout(1500);
  const dlPromise = page.waitForEvent('download', { timeout: 90000 });
  dlPromise.catch(() => {}); // ボタン探索失敗でthrowする経路の未処理reject防止 (Codex R2 Medium)
  if (!(await clickExact(page, ['ダウンロード'], 'モーダルDL実行', { last: true }))) {
    throw new Error('FORM_VERIFY: モーダルの「ダウンロード」ボタンが見つからず');
  }
  const download = await dlPromise;
  await persistDownload(download, 'rdata_store_daily', { from: month.from, to: month.to },
    { type: 'rakuten_store_daily', month });
  return 'ok';
}

// ─── データダウンロードハブ 7種 (mall-csv-fetcher P1-R3、実測 2026-07-10) ───
// select[name=name] でデータ種類を選び、期間 (calendar input) → CSVダウンロード → モーダル →
// ダウンロード (同期DL)。新規リピート系3種は期間指定が効かない固定window (as-is DL)。
const DD_SPECS = [
  // period: 'month'=対象月レンジ / 'day'=1日単位ループ / null=期間指定なし (固定window)
  { key: 'dd_store', label: '店舗データ', selectLabel: '店舗データ', period: 'month',
    reportType: 'rdata_dd_store', expectType: 'rakuten_store_device_daily' },
  { key: 'dd_sku', label: 'SKU別売上データ', selectLabel: 'SKU別売上データ', period: 'day',
    reportType: 'rdata_dd_sku', expectType: 'rakuten_sku_daily' },
  { key: 'dd_category', label: 'カテゴリページデータ', selectLabel: 'カテゴリページデータ', period: 'month',
    reportType: 'rdata_dd_category', expectType: 'rakuten_category_daily', monthLoose: true },
  { key: 'dd_campaign', label: 'キャンペーンデータ', selectLabel: 'キャンペーンデータ', period: 'month',
    reportType: 'rdata_dd_campaign', expectType: 'rakuten_campaigns', noDateVerify: true },
  { key: 'dd_purchaser', label: '新規・リピート購入者数（店舗別）', selectLabel: '新規・リピート購入者数（店舗別）', period: null,
    reportType: 'rdata_dd_purchaser_monthly', expectType: 'rakuten_purchaser_monthly' },
  { key: 'dd_item_purchaser', label: '新規・リピート購入者数（商品別）', selectLabel: '新規・リピート購入者数（商品別）', period: null,
    reportType: 'rdata_dd_item_purchaser', expectType: 'rakuten_item_purchaser_snapshot' },
  { key: 'dd_genre_purchaser', label: '新規・リピート購入者数（商品ジャンル別）', selectLabel: '新規・リピート購入者数（商品ジャンル別）', period: null,
    reportType: 'rdata_dd_genre_purchaser', expectType: 'rakuten_genre_purchaser_snapshot' },
];

/** データダウンロードハブで1種類×1期間をDL。range=null は固定windowデータ (期間設定なし) */
async function fetchDdOne(page, spec, range) {
  const rangeLabel = range ? (range.from === range.to ? range.from : `${range.from}〜${range.to}`) : '固定window';
  console.log(`\n--- ${spec.label} ${rangeLabel} ---`);
  await gotoDatatool(page, DD_URL, 'データダウンロード');

  // データ種類を選択 → 読み戻し検証
  await page.selectOption('select[name="name"]', { label: spec.selectLabel });
  await page.waitForTimeout(2000);
  const sel = await page.evaluate(() => {
    const el = document.querySelector('select[name="name"]');
    return el ? el.selectedOptions[0]?.text || '' : '';
  }).catch(() => '');
  if (sel !== spec.selectLabel) throw new Error(`FORM_VERIFY: データ種類を選択できず (期待 ${spec.selectLabel} / 実際 ${sel})`);
  console.log(`  [select] ${sel}`);

  if (range) {
    // 期間選択=日次 radio (デフォルト) を確認してから期間セット
    const dailyOk = await page.evaluate(() => {
      const radios = [...document.querySelectorAll('input[type=radio]')].filter((r) => !!(r.offsetParent || r.getClientRects().length));
      return radios.some((r) => r.value === 'daily' && r.checked);
    }).catch(() => false);
    if (!dailyOk) throw new Error('FORM_VERIFY: 期間選択が「日次」になっていない');
    const marked = await page.evaluate(() => {
      const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
      const el = [...document.querySelectorAll('input[type=text]')].filter(vis)
        .find((e) => /^\d{4}\/\d{2}\/\d{2} - \d{4}\/\d{2}\/\d{2}$/.test(e.value));
      if (!el) return false;
      el.setAttribute('data-rdata-date', '1');
      return true;
    });
    if (!marked) throw new Error('FORM_VERIFY: 期間入力が見つからず');
    const want = `${slashDate(range.from)} - ${slashDate(range.to)}`;
    const got = await setRangeInput(page, 'data-rdata-date', want);
    if (got !== want) throw new Error(`FORM_VERIFY: 期間が設定できず (期待 ${want} / 実際 ${got})`);
    console.log(`  [date] ${got}`);
  }

  const dlPromise = page.waitForEvent('download', { timeout: 90000 });
  dlPromise.catch(() => {});
  if (!(await clickExact(page, ['CSVダウンロード'], 'CSVダウンロードモーダル'))) {
    throw new Error('FORM_VERIFY: 「CSVダウンロード」ボタンが見つからず');
  }
  await page.waitForTimeout(1500);
  // モーダルの「ダウンロード」(出ない同期DLパターンにも対応: 見つからなくても download を待つ)
  await clickExact(page, ['ダウンロード'], 'モーダルDL実行', { last: true });
  const download = await dlPromise;

  const expect = { type: spec.expectType };
  if (range && !spec.noDateVerify) {
    if (range.from === range.to) expect.dateFrom = range.from;
    else expect.month = { ym: range.from.slice(0, 7), from: range.from, to: range.to, loose: !!spec.monthLoose };
  }
  await persistDownload(download, spec.reportType,
    range || { from: 'window', to: 'window' }, expect);
  return 'ok';
}

async function main() {
  const runLog = initRunLog('rakuten-data');
  try { assertLoginEnv(); } catch (e) {
    console.error(`⚠️ ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'rakuten-data', logPath: runLog.logPath,
      failures: [{ reportType: 'rdata(起動前)', error: e.message }],
      repro: 'scripts/mall-csv-fetcher/.env を確認 (記入は中原さん)',
    }), 'rakuten-data');
    process.exitCode = 2; // fetch直後の process.exit は libuv crash (#464) → exitCode
    return;
  }
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const targets = (process.env.RDATA_REPORTS || 'item,store,dd').split(',').map((s) => s.trim()).filter(Boolean);
  const unknown = targets.filter((t) => !['item', 'store', 'dd'].includes(t));
  if (targets.length === 0 || unknown.length > 0) {
    // typo で対象0件のまま exit 0 になるのを防ぐ (Codex R1 Low、RPP側の exit 2 規約に合わせる)
    console.error(`FATAL: RDATA_REPORTS には item / store / dd を指定してください (不明: ${unknown.join(',') || '(空)'})`);
    process.exitCode = 2;
    return;
  }
  const itemDates = targets.includes('item') ? computeItemTargetDates() : [];
  const storeMonths = targets.includes('store') ? computeStoreTargetMonths() : [];
  const ddSpecs = targets.includes('dd') ? DD_SPECS : [];

  // openContext 失敗 (プロファイルロック等) は fetch-all が「子が通知済み」とみなす exit 1 に
  // なるため、ここで自力通知してから落ちる (Codex R1 Medium: 通知漏れ防止)
  let context;
  try {
    context = await openContext();
  } catch (e) {
    console.error(`⚠️ ブラウザ起動失敗: ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'rakuten-data', logPath: runLog.logPath,
      failures: [{ reportType: 'rdata(ブラウザ起動)', error: e.message }],
      repro: '多重起動 (プロファイルロック) や Playwright 破損を確認: node scripts/mall-csv-fetcher/rakuten-data-download.mjs',
    }), 'rakuten-data');
    process.exitCode = 1;
    return;
  }
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  const outcomes = [];
  const failures = [];
  try {
    console.log('=== 楽天データ分析CSV 自動DL ===');
    console.log(`商品分析: ${itemDates.join(', ') || '(対象外)'} / 店舗日次: ${storeMonths.map((m) => m.ym).join(', ') || '(対象外)'} / データDL: ${ddSpecs.length}種`);
    await ensureRmsLogin(page);

    for (const dateIso of itemDates) {
      try {
        const status = await fetchItemDaily(page, dateIso);
        outcomes.push({ spec: 'item', ym: dateIso, status });
      } catch (e) {
        console.error(`✗ 商品分析 ${dateIso}: ${e.message}`);
        await snap(page, `item_${dateIso}_error`);
        await logFetch({ report_type: 'rdata_item_daily', period_from: dateIso, period_to: dateIso, status: 'error', message: e.message });
        outcomes.push({ spec: 'item', ym: dateIso, status: 'error' });
        failures.push({ reportType: 'rdata_item_daily', ym: dateIso, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `rdata_item_${dateIso}_error.png`) });
        if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
        break; // 同レポートの残り日も同原因の可能性大 (店舗日次は試す)
      }
    }

    for (const month of storeMonths) {
      try {
        const status = await fetchStoreDaily(page, month);
        outcomes.push({ spec: 'store', ym: month.ym, status });
      } catch (e) {
        console.error(`✗ 店舗日次 ${month.ym}: ${e.message}`);
        await snap(page, `store_${month.ym}_error`);
        await logFetch({ report_type: 'rdata_store_daily', period_from: month.from, period_to: month.to, status: 'error', message: e.message });
        outcomes.push({ spec: 'store', ym: month.ym, status: 'error' });
        failures.push({ reportType: 'rdata_store_daily', ym: month.ym, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `rdata_store_${month.ym}_error.png`) });
        if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
        break;
      }
    }

    // データダウンロードハブ 7種。1種の失敗で他種を止めない (種類ごとに独立した画面操作)
    for (const spec of ddSpecs) {
      // 対象期間: day=商品分析と同じ日ループ / month=店舗日次と同じ月ループ / null=固定window 1DL
      const ranges = spec.period === 'day' ? computeItemTargetDates().map((d) => ({ from: d, to: d }))
        : spec.period === 'month' ? computeStoreTargetMonths().map((m) => ({ from: m.from, to: m.to }))
        : [null];
      for (const range of ranges) {
        const ymLabel = range ? (range.from === range.to ? range.from : range.from.slice(0, 7)) : 'window';
        try {
          // 日単位DLはデータ更新日 (集計済み最新日) より新しい日をスキップ (商品分析と同じ構え)
          if (spec.period === 'day') {
            await gotoDatatool(page, DD_URL, 'データダウンロード');
            const upd = await readUpdateDate(page);
            if (upd && range.from > upd) {
              console.log(`\n--- ${spec.label} ${range.from} --- [skip] 未集計 (更新日=${upd})`);
              await logFetch({ report_type: spec.reportType, period_from: range.from, period_to: range.to, status: 'empty', message: `未集計 (データ更新日=${upd})` });
              outcomes.push({ spec: spec.key, ym: ymLabel, status: 'empty' });
              continue;
            }
          }
          const status = await fetchDdOne(page, spec, range);
          outcomes.push({ spec: spec.key, ym: ymLabel, status });
        } catch (e) {
          console.error(`✗ ${spec.label} ${ymLabel}: ${e.message}`);
          await snap(page, `${spec.key}_${ymLabel}_error`);
          await logFetch({
            report_type: spec.reportType, period_from: range?.from || null, period_to: range?.to || null,
            status: 'error', message: e.message,
          });
          outcomes.push({ spec: spec.key, ym: ymLabel, status: 'error' });
          failures.push({ reportType: spec.reportType, ym: ymLabel, error: e.message, url: page.url(), screenshot: join(OUT_DIR, `rdata_${spec.key}_${ymLabel}_error.png`) });
          if (String(e.message).startsWith('2FA_REQUIRED')) throw e;
          break; // 同種の残り期間はスキップ (他種は続行)
        }
      }
    }

    console.log(`\n=== summary: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ') || '(対象なし)'} ===`);
    if (failures.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (incoming/rakuten-data/ に置くだけ)');
      await sendGChat(buildErrorReport({
        mall: 'rakuten-data', outcomes, failures, logPath: runLog.logPath,
        repro: 'node scripts/mall-csv-fetcher/rakuten-data-download.mjs',
      }), 'rakuten-data');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-rakuten-data.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'rakuten-data', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'rdata(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'rdata_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs → 手動ログイン+「信頼できる端末」登録 (14日ごと)'
        : 'node scripts/mall-csv-fetcher/rakuten-data-download.mjs',
    }), 'rakuten-data');
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
