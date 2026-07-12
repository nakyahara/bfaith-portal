/**
 * Yahoo!ストアクリエイターPro 画面網羅調査 (P1-Y調査用ワンオフ)
 *
 * ログイン済みプロファイル (.profile-yahoo) でトップから各メニューをクリックして回り、
 * 到達URL・サブメニュー・CSV/ダウンロード系ボタンを記録する。
 * 「分析に使えるものはとりあえず全部取る」方針の候補洗い出し。
 *
 * 実行: HEADLESS=1 node scripts/mall-csv-fetcher/yahoo-survey.mjs
 *   env: YAHOO_STORE_ACCOUNT=b-faith01 (デフォルト)
 */
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openYahooContext, gotoStorePage, STORE_ACCOUNT, STORE_TOP_URL } from './lib-yahoo-login.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');
const ACCOUNT = STORE_ACCOUNT;
const TOP = STORE_TOP_URL;

const SECTIONS = (process.env.SURVEY_SECTIONS || '販売管理,集客・販促,利用明細,評価,注文管理').split(',');

async function dumpPage(page, label) {
  console.log(`\n===== [${label}] url=${page.url()}`);
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const links = [...document.querySelectorAll('a')]
      .map((a) => ({ t: (a.innerText || a.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 36), h: a.href || '', hid: !vis(a) }))
      .filter((l) => l.t && l.h && !/javascript:void/.test(l.h));
    const buttons = [...document.querySelectorAll('button, input[type=submit], input[type=button]')]
      .filter(vis).map((el) => (el.innerText || el.value || '').trim().replace(/\s+/g, ' ').slice(0, 30)).filter(Boolean);
    const inputs = [...document.querySelectorAll('input, select')].filter(vis).map((el) => ({
      tag: el.tagName.toLowerCase(), name: el.name || '', id: el.id || '', type: el.type || '', value: (el.value || '').slice(0, 30),
    }));
    return { links, buttons, inputs };
  }).catch(() => ({ links: [], buttons: [], inputs: [] }));
  if (info.inputs?.length) console.log(`  [inputs] ${JSON.stringify(info.inputs.slice(0, 20))}`);
  const seen = new Set();
  for (const l of info.links) {
    if (/store-info|topics|campaign\/detail|help|faq|manual|yahoo\.co\.jp\/$|business\.yahoo/.test(l.h)) continue;
    const k = l.h.slice(0, 110);
    if (seen.has(k)) continue;
    seen.add(k);
    console.log(`  ${l.hid ? 'H' : ' '} ${l.t} -> ${l.h.slice(0, 120)}`);
  }
  const csvBtns = info.buttons.filter((b) => /CSV|ダウンロード|出力/i.test(b));
  console.log(`  [buttons] ${JSON.stringify(info.buttons.slice(0, 30))}`);
  if (csvBtns.length) console.log(`  ⭐[csv-buttons] ${JSON.stringify(csvBtns)}`);
  await page.screenshot({ path: join(OUT_DIR, `ysurvey_${label.replace(/[^\w]/g, '_')}.png`), fullPage: false }).catch(() => {});
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const context = await openYahooContext();
  const page = context.pages()[0] || await context.newPage();
  try {
    // SURVEY_URLS=カンマ区切りの相対/絶対URL → 各ページを gotoStorePage (認証検証つき) で巡回
    const urlList = (process.env.SURVEY_URLS || '').split(',').map((s) => s.trim()).filter(Boolean);
    if (urlList.length) {
      for (const u of urlList) {
        const full = u.startsWith('http') ? u : `${TOP}/${u.replace(/^\//, '')}`;
        try {
          await gotoStorePage(page, full, u);
          await page.waitForTimeout(2500);
          const label = u.replace(/[^\w]/g, '_').slice(0, 40);
          await dumpPage(page, label);
          // SURVEY_DL=1: 「CSVファイルをダウンロード」リンクをクリックしてDL、保存名とヘッダを出力
          if (process.env.SURVEY_DL === '1') {
            const dlPromise = page.waitForEvent('download', { timeout: 60000 });
            dlPromise.catch(() => {});
            const link = page.locator('a', { hasText: /CSVファイルをダウンロード|全てのCSVファイルをダウンロード/ }).first();
            if (await link.isVisible().catch(() => false)) {
              await link.click({ timeout: 10000 }).catch((e) => console.log(`  [dl] click失敗: ${e.message.split('\n')[0]}`));
              try {
                const dl = await dlPromise;
                const dest = join(OUT_DIR, `ycsv_${label}_${dl.suggestedFilename() || 'file.csv'}`);
                await dl.saveAs(dest);
                console.log(`  ⭐[dl] 保存: ${dest} (元名: ${dl.suggestedFilename()})`);
              } catch {
                console.log('  [dl] downloadイベントが来ず (非同期生成/確認ダイアログ/0件の可能性)');
                await page.screenshot({ path: join(OUT_DIR, `ycsv_${label}_nodl.png`) }).catch(() => {});
              }
            } else {
              console.log('  [dl] CSVリンクが見つからず');
            }
          }
        } catch (e) {
          console.log(`\n===== [${u}] ✗ ${e.message.split('\n')[0]}`);
        }
      }
      return;
    }

    await gotoStorePage(page, TOP, 'top');
    await dumpPage(page, 'top');

    for (const section of SECTIONS) {
      try {
        await page.goto(TOP, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
        await page.waitForTimeout(2000);
        // hover でサブメニューが出るタイプに備え hover→リンク収集、なければ click→遷移
        const btn = page.locator(`button:has-text("${section}"), a:has-text("${section}")`).first();
        if (!(await btn.isVisible().catch(() => false))) {
          console.log(`\n===== [${section}] メニューが見つからず`);
          continue;
        }
        await btn.hover().catch(() => {});
        await page.waitForTimeout(1200);
        const sub = process.env.SURVEY_FORCE_CLICK === '1' ? [] : await page.evaluate(() => {
          const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
          return [...document.querySelectorAll('a')].filter(vis)
            .map((a) => ({ t: (a.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 36), h: a.href || '' }))
            .filter((l) => l.t && l.h && /pro\.store\.yahoo\.co\.jp/.test(l.h) && !/campaign\/detail/.test(l.h));
        }).catch(() => []);
        if (sub.length > 3) {
          console.log(`\n===== [${section}] hoverサブメニュー:`);
          const seen = new Set();
          for (const l of sub) {
            if (seen.has(l.h)) continue;
            seen.add(l.h);
            console.log(`    ${l.t} -> ${l.h.slice(0, 120)}`);
          }
        } else {
          await btn.click({ timeout: 8000 }).catch(() => {});
          await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
          await page.waitForTimeout(2000);
          await dumpPage(page, section);
        }
      } catch (e) {
        console.log(`✗ [${section}] ${e.message.split('\n')[0]}`);
      }
    }
  } finally {
    await context.close().catch(() => {});
  }
}

main();
