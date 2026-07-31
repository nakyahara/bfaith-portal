/**
 * au PAYマーケット ストアクーポン 画面調査 (P3-A 調査用ワンオフ / 読み取り専用)
 *
 * 目的: Yahoo!ストアクーポン自動入れ替え (yahoo-coupon-rotate.mjs) と同じ仕組みを
 *   au PAY マーケットにも作るため、Wow!manager のクーポン管理画面の実仕様を実測する。
 *
 * このスクリプトは**画面を開いて記録するだけ**。フォーム送信・発行・停止は一切しない。
 *   - クーポン管理 (配布型) 一覧の HTML / 表構造 / 行ごとのボタン実装
 *   - 「詳細」「コピー」「配布停止」がどう遷移するか (href / onclick / form action)
 *   - couponType の値ごとの画面
 *
 * 実行: HEADLESS=1 node scripts/mall-csv-fetcher/aupay-coupon-survey.mjs
 *   env: ACOUPON_TYPES=1,2 (調べる couponType。既定 1)
 *        ACOUPON_COPY_ID=987115 (このクーポンの詳細画面とコピー作成画面を開いて項目をダンプ)
 *
 * 実測 (2026-07-31):
 *   一覧 = /wmshopclient/shopCouponMgt/index?couponType=1
 *   行内リンクはすべて素の href (GETで開ける)
 *     詳細     = /wmshopclient/distributionCoupon/detail/<couponId>
 *     コピー   = /wmshopclient/distributionCoupon/copyCreate/<couponId>
 *     配布停止 = /wmshopclient/distributionCoupon/detailStop/<couponId>  ← 調査では絶対に開かない
 */

import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openAupayContext, gotoWowmaPage, ensureWowmaLogin, WOWMA_BASE } from './lib-aupay-login.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output', 'aupay-coupon');
// クーポン関連画面のみ。配布停止 (detailStop) は調査対象外として明示的に弾く
const COUPON_PATH_RE = /^\/wmshopclient\/(shopCouponMgt|distributionCoupon)\//;
const FORBIDDEN_PATH_RE = /detailStop|delete|stop/i;
const TYPES = (process.env.ACOUPON_TYPES || '1').split(',').map((s) => s.trim()).filter(Boolean);
const COPY_ID = (process.env.ACOUPON_COPY_ID || '').trim();

/** クーポン画面の外へ出ていないことを確認する (他の管理画面を触らないための保険) */
function assertCouponUrl(url, label) {
  let u;
  try { u = new URL(url); } catch { throw new Error(`URL_GUARD: ${label} のURLを解釈できない (${url})`); }
  if (u.hostname !== 'manager.wowma.jp') throw new Error(`URL_GUARD: ${label} が想定外のホスト (${u.hostname})`);
  if (!COUPON_PATH_RE.test(u.pathname)) throw new Error(`URL_GUARD: ${label} がクーポン画面の外 (${u.pathname})`);
  if (FORBIDDEN_PATH_RE.test(u.pathname)) throw new Error(`URL_GUARD: ${label} は調査で触らない画面 (${u.pathname})`);
}

/** クーポン一覧の全行を構造化して取り出す (行内リンクから couponId を得る) */
async function extractCouponRows(page) {
  return page.evaluate(() => {
    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const table = [...document.querySelectorAll('table')]
      .find((tb) => /クーポンキー/.test(tb.innerText) && /利用期間/.test(tb.innerText));
    if (!table) return null;
    const out = [];
    for (const tr of table.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll('td')].map((td) => norm(td.innerText));
      if (cells.length < 10) continue;
      const links = {};
      for (const a of tr.querySelectorAll('a')) {
        const m = (a.getAttribute('href') || '').match(/\/distributionCoupon\/(\w+)\/(\d+)/);
        if (m) links[m[1]] = m[2];
      }
      out.push({ cells, couponId: links.detail || links.copyCreate || null, links });
    }
    return out;
  });
}

/** 画面の構造を丸ごと記録する (HTML本体 + 表 + フォーム + ボタン) */
async function dumpPage(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();

    const tables = [...document.querySelectorAll('table')].map((tb, ti) => ({
      index: ti,
      headers: [...tb.querySelectorAll('th')].map((th) => norm(th.innerText)).slice(0, 30),
      rows: [...tb.querySelectorAll('tr')].slice(0, 6).map((tr) => ({
        cells: [...tr.querySelectorAll('td')].map((td) => norm(td.innerText).slice(0, 60)),
        // 行の中のボタン/リンクの実装をそのまま残す (コピー・詳細の押し方を知るため)
        controls: [...tr.querySelectorAll('a, button, input[type=submit], input[type=button]')].map((el) => ({
          text: norm(el.innerText || el.value).slice(0, 20),
          tag: el.tagName.toLowerCase(),
          href: el.getAttribute('href') || '',
          onclick: (el.getAttribute('onclick') || '').slice(0, 300),
          outer: el.outerHTML.slice(0, 300),
        })),
      })),
      rowCount: tb.querySelectorAll('tr').length,
    }));

    const forms = [...document.querySelectorAll('form')].map((f) => ({
      name: f.name || '', id: f.id || '', action: f.action || '', method: (f.method || '').toUpperCase(),
      fields: [...f.querySelectorAll('input, select, textarea')].map((el) => ({
        tag: el.tagName.toLowerCase(), name: el.name || '', id: el.id || '', type: el.type || '',
        value: String(el.value || '').slice(0, 60),
        checked: el.type === 'checkbox' || el.type === 'radio' ? !!el.checked : undefined,
        hidden: !vis(el),
        options: el.tagName === 'SELECT' ? [...el.options].map((o) => ({ v: o.value, t: norm(o.text).slice(0, 40) })).slice(0, 60) : undefined,
      })),
    }));

    const buttons = [...document.querySelectorAll('a, button, input[type=submit], input[type=button]')]
      .filter(vis)
      .map((el) => ({ text: norm(el.innerText || el.value).slice(0, 24), tag: el.tagName.toLowerCase(), href: el.getAttribute('href') || '', onclick: (el.getAttribute('onclick') || '').slice(0, 200) }))
      .filter((b) => b.text);

    // インラインJS: submit系の関数定義を拾う (Wow!manager は hidden を足して submit する実装が多い)
    const scripts = [...document.querySelectorAll('script:not([src])')]
      .map((s) => s.textContent || '')
      .filter((t) => /function\s+\w+|submit\(|action\s*=/.test(t))
      .map((t) => t.replace(/\s+/g, ' ').slice(0, 1500));

    return { title: document.title, tables, forms, buttons, scripts, bodyText: norm(document.body.innerText).slice(0, 3000) };
  });

  const html = await page.content();
  const safe = label.replace(/[^\w.-]/g, '_');
  await writeFile(join(OUT_DIR, `${safe}.html`), html, 'utf8');
  await writeFile(join(OUT_DIR, `${safe}.json`), JSON.stringify({ url: page.url(), ...info }, null, 2), 'utf8');
  await page.screenshot({ path: join(OUT_DIR, `${safe}.png`), fullPage: true }).catch(() => {});
  console.log(`  [dump] ${safe}: title="${info.title}" tables=${info.tables.length} forms=${info.forms.length} html=${html.length}B`);
  return info;
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  console.log('=== au PAY クーポン画面 調査 (読み取り専用) ===');
  const ctx = await openAupayContext();
  const page = ctx.pages()[0] || (await ctx.newPage());
  try {
    await ensureWowmaLogin(page);

    for (const t of TYPES) {
      const url = `${WOWMA_BASE}shopCouponMgt/index?couponType=${t}`;
      console.log(`\n--- couponType=${t} ---`);
      await gotoWowmaPage(page, url, `クーポン一覧 type=${t}`);
      assertCouponUrl(page.url(), `クーポン一覧 type=${t}`);
      const info = await dumpPage(page, `list_type${t}`);
      const main = info.tables.find((tb) => tb.headers.some((h) => /クーポン名|クーポンキー/.test(h))) || info.tables[0];
      if (main) {
        console.log(`  [headers] ${JSON.stringify(main.headers)}`);
        console.log(`  [rowCount] ${main.rowCount}`);
        for (const r of main.rows.slice(0, 3)) {
          console.log(`  [row] ${JSON.stringify(r.cells)}`);
          for (const c of r.controls) console.log(`     - "${c.text}" ${c.tag} href=${c.href} onclick=${c.onclick}`);
        }
      }
      console.log(`  [forms] ${JSON.stringify(info.forms.map((f) => ({ name: f.name, action: f.action, method: f.method, nFields: f.fields.length })))}`);

      const rows = await extractCouponRows(page);
      if (rows) {
        await writeFile(join(OUT_DIR, `rows_type${t}.json`), JSON.stringify(rows, null, 2), 'utf8');
        console.log(`  [rows] ${rows.length} 件 → rows_type${t}.json`);
        for (const r of rows) console.log(`    ${r.couponId} | ${r.cells.slice(1, 4).join(' | ')} | ${r.cells.slice(6, 11).join(' | ')}`);
      }
    }

    // 詳細 / コピー作成 の画面項目 (どちらも開くだけ。送信はしない)
    if (COPY_ID) {
      if (!/^\d+$/.test(COPY_ID)) throw new Error(`ACOUPON_COPY_ID が数値でない (${COPY_ID})`);
      for (const kind of ['detail', 'copyCreate']) {
        const url = `${WOWMA_BASE}distributionCoupon/${kind}/${COPY_ID}`;
        console.log(`\n--- ${kind}/${COPY_ID} ---`);
        await gotoWowmaPage(page, url, `${kind} 画面`);
        assertCouponUrl(page.url(), `${kind} 画面`);
        const info = await dumpPage(page, `${kind}_${COPY_ID}`);
        for (const f of info.forms) {
          console.log(`  [form] name=${f.name} action=${f.action} method=${f.method} fields=${f.fields.length}`);
          for (const fd of f.fields) {
            const opts = fd.options ? ` options=${JSON.stringify(fd.options.slice(0, 12))}${fd.options.length > 12 ? `(+${fd.options.length - 12})` : ''}` : '';
            console.log(`     ${fd.hidden ? 'H' : ' '} ${fd.tag}[${fd.type}] name=${fd.name} value="${fd.value}"${fd.checked === undefined ? '' : ` checked=${fd.checked}`}${opts}`);
          }
        }
        console.log(`  [buttons] ${JSON.stringify(info.buttons.filter((b) => /登録|確認|作成|次へ|発行|保存|戻/.test(b.text)))}`);
      }
    }
    console.log(`\n出力: ${OUT_DIR}`);
  } catch (e) {
    console.error('[FATAL]', e.message);
    await page.screenshot({ path: join(OUT_DIR, 'fatal.png'), fullPage: true }).catch(() => {});
    process.exitCode = 1;
  } finally {
    await ctx.close().catch(() => {});
  }
}

main();
