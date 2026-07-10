/**
 * RMS「データ分析」画面のDOM調査スパイク (mall-csv-fetcher P1-R2 自動DL用)
 *
 * ログイン→メインメニューから「データ分析」への導線を探し、画面構造 (URL/リンク/
 * コントロール) をダンプする。実装前の1回きりの調査用 (コミットするが本番では使わない)。
 *
 * 実行: HEADLESS=1 node scripts/mall-csv-fetcher/rakuten-data-spike.mjs
 *   env: SPIKE_URL=... で調査対象URLを直接指定 (2回目以降の深掘り用)
 *        SPIKE_CLICK=テキスト で到達後にそのテキストのリンク/ボタンをクリックして再ダンプ
 */
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openContext, ensureRmsLogin, dumpControls, dumpLinks, safeHost, assertLoginEnv } from './lib-rakuten-login.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rdata_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rdata_${label}  url=${page.url()}`);
}

/** 全frameからキーワードに合うリンクを列挙 (ホバーメニュー等の非表示リンクも含む) */
async function findLinks(page, keywords) {
  const found = [];
  for (const frame of page.frames()) {
    const links = await frame.evaluate((kws) => {
      const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
      return [...document.querySelectorAll('a, button')]
        .map((a) => ({
          text: ((a.innerText || a.textContent || '').trim()).replace(/\s+/g, ' ').slice(0, 40),
          href: a.href || '', hidden: !vis(a),
        }))
        .filter((l) => l.text && kws.some((k) => l.text.includes(k)));
    }, keywords).catch(() => []);
    for (const l of links) found.push({ ...l, frameUrl: frame.url().slice(0, 60) });
  }
  return found;
}

/** 日付レンジ入力の書き換えテスト (value が "A - B" 形式の可視inputへ) */
async function testSetDate(page, setDate) {
  const res = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const el = [...document.querySelectorAll('input[type=text]')].filter(vis)
      .find((e) => /^\d{4}\/\d{2}\/\d{2} - \d{4}\/\d{2}\/\d{2}$/.test(e.value));
    if (!el) return { ok: false, reason: 'not-found' };
    el.setAttribute('data-spike-date', '1');
    return { ok: true, before: el.value };
  });
  console.log(`  [setdate] 対象特定: ${JSON.stringify(res)}`);
  if (!res.ok) return;

  // 方式1: JSネイティブsetter + input/change イベント (React/Vue系対応)
  const jsSet = await page.evaluate((val) => {
    const el = document.querySelector('[data-spike-date="1"]');
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(el, val);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
    el.blur();
    return el.value;
  }, setDate).catch((e) => `ERR:${e.message}`);
  await page.waitForTimeout(3000);
  const after = await page.locator('[data-spike-date="1"]').evaluate((e) => e.value).catch(() => '');
  const body = await page.locator('body').innerText().catch(() => '');
  const taisho = (body.match(/対象期間[::]?\s*([\d/ -]+)/) || [])[1] || '(不明)';
  console.log(`  [setdate] jsSet直後="${jsSet}" 3秒後="${after}" 画面の対象期間="${taisho.trim()}"`);
  if (!after.startsWith(setDate.split(' ')[0])) {
    // 方式2: ピッカーUIを開いて構造をダンプ
    console.log('  [setdate] JS方式が効かず → ピッカーを開いてDOMダンプ');
    await page.locator('[data-spike-date="1"]').click().catch(() => {});
    await page.waitForTimeout(1500);
    const picker = await page.evaluate(() => {
      const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
      const cands = [...document.querySelectorAll('div')].filter((d) => vis(d)
        && /calendar|picker|date/i.test(d.className || '')
        && d.querySelectorAll('td, [class*=day], button').length > 5);
      return cands.slice(0, 3).map((d) => ({
        cls: (d.className || '').slice(0, 100),
        html: d.outerHTML.slice(0, 1500),
      }));
    }).catch(() => []);
    console.log(`  [picker] ${JSON.stringify(picker, null, 1).slice(0, 3000)}`);
    await snap(page, 'picker_open');
  }
}

async function main() {
  assertLoginEnv();
  await mkdir(OUT_DIR, { recursive: true });
  const context = await openContext();
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  try {
    await ensureRmsLogin(page);
    const targetUrl = (process.env.SPIKE_URL || '').trim();

    if (!targetUrl) {
      // ─── パス1: メインメニューから「データ分析」導線を探す ───
      console.log('\n=== mainmenu で分析系リンクを探索 ===');
      await snap(page, 'mainmenu');
      const cands = await findLinks(page, ['データ分析', '分析', '店舗カルテ']);
      console.log(`[found] ${JSON.stringify(cands, null, 1)}`);
      const dataLink = cands.find((l) => l.text.includes('データ分析') && l.href);
      if (dataLink) {
        console.log(`\n=== データ分析へ遷移: ${dataLink.href} ===`);
        await page.goto(dataLink.href, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
      } else if (cands[0]?.href) {
        console.log(`\n=== 候補先頭へ遷移: ${cands[0].href} ===`);
        await page.goto(cands[0].href, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
      } else {
        console.log('!! データ分析リンクが見つからず。全リンクをダンプ');
        await dumpLinks(page, 'mainmenu-all', 150);
        return;
      }
    } else {
      console.log(`\n=== 指定URLへ遷移: ${targetUrl} ===`);
      await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    }

    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(3000);
    console.log(`[arrived] url=${page.url()} host=${safeHost(page.url())}`);
    await snap(page, 'arrived');

    const clickText = (process.env.SPIKE_CLICK || '').trim();
    if (clickText) {
      console.log(`\n=== クリック: "${clickText}" ===`);
      let clicked = false;
      for (const frame of page.frames()) {
        const loc = frame.locator(`a:has-text("${clickText}"), button:has-text("${clickText}")`).first();
        if (await loc.isVisible().catch(() => false)) {
          await loc.click({ timeout: 10000 }).catch((e) => console.log(`  click失敗: ${e.message}`));
          clicked = true;
          break;
        }
      }
      if (!clicked) console.log('  クリック対象が見つからず');
      await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(3000);
      console.log(`[after-click] url=${page.url()}`);
      await snap(page, 'after_click');
    }

    // 日付レンジ入力の書き換えテスト (SPIKE_DL と独立して実行可)
    {
      const setDate = (process.env.SPIKE_SETDATE || '').trim();
      if (setDate) await testSetDate(page, setDate);
    }

    // DLテスト: SPIKE_DL=ボタンテキスト でクリック→ダウンロード保存+ヘッダ確認
    const dlText = (process.env.SPIKE_DL || '').trim();
    if (dlText) {
      console.log(`\n=== DLテスト: "${dlText}" ===`);
      // select指定 (SPIKE_SELECT=セレクタ::値)
      const selSpec = (process.env.SPIKE_SELECT || '').trim();
      if (selSpec) {
        const [sel, val] = selSpec.split('::');
        await page.selectOption(sel, { label: val }).catch(async () => {
          await page.selectOption(sel, val).catch((e) => console.log(`  select失敗: ${e.message}`));
        });
        await page.waitForTimeout(2500);
        const cur = await page.evaluate((s) => {
          const el = document.querySelector(s);
          return el ? { value: el.value, label: el.selectedOptions[0]?.text || '' } : null;
        }, sel);
        console.log(`  [select] ${sel} => ${JSON.stringify(cur)}`);
      }
      const dlPromise = page.waitForEvent('download', { timeout: 60000 }).catch(() => null);
      // '>>' 区切りで多段クリック (例: 全商品CSV>>全件>>ダウンロード = モーダル経由)
      for (const step of dlText.split('>>')) {
        let clicked = false;
        for (const frame of page.frames()) {
          for (const sel of [`button:has-text("${step}")`, `a:has-text("${step}")`, `label:has-text("${step}")`, `text="${step}"`]) {
            const loc = frame.locator(sel).last();
            if (await loc.isVisible().catch(() => false)) {
              await loc.click({ timeout: 10000 }).catch((e) => console.log(`  click失敗(${step}): ${e.message}`));
              clicked = true;
              break;
            }
          }
          if (clicked) break;
        }
        console.log(`  [step] "${step}" clicked=${clicked}`);
        await page.waitForTimeout(2000);
      }
      const dl = await dlPromise;
      if (dl) {
        const dest = join(OUT_DIR, `rdata_dltest_${dl.suggestedFilename() || 'file.bin'}`);
        await dl.saveAs(dest);
        console.log(`  ✅ DL成功: ${dest} (元名: ${dl.suggestedFilename()})`);
        const { readFile } = await import('node:fs/promises');
        const buf = await readFile(dest);
        console.log(`  bytes=${buf.length}`);
        const { parseCsvBuffer } = await import('../../apps/warehouse/rakuten-ads-rpp-lib.js');
        const rows = parseCsvBuffer(buf);
        for (let i = 0; i < Math.min(rows.length, 10); i++) {
          console.log(`  [csv-row${i}] (${rows[i].length}列) ${JSON.stringify(rows[i].slice(0, 12))}`);
        }
        console.log(`  [csv] 総行数=${rows.length}`);
      } else {
        console.log('  ✗ downloadイベントが来ず (非同期生成 or ダイアログ?)');
        await snap(page, 'dltest_nodownload');
      }
    }

    // 到達画面の構造をダンプ
    console.log('\n=== 到達画面の構造 ===');
    console.log(`frames: ${JSON.stringify(page.frames().map((f) => f.url().slice(0, 90)))}`);
    await dumpControls(page, 'rdata');
    const navLinks = await findLinks(page, ['商品', '日次', '月次', 'ダウンロード', 'レポート', 'CSV', '分析', '期間', '店舗']);
    console.log(`[nav-links] ${JSON.stringify(navLinks, null, 1)}`);
    const bodyText = await page.locator('body').innerText().catch(() => '');
    console.log(`[body-head] ${bodyText.replace(/\s+/g, ' ').slice(0, 600)}`);
  } catch (e) {
    console.error('[ERROR]', e.message);
    await snap(page, 'error');
  } finally {
    await context.close().catch(() => {});
  }
}

main();
