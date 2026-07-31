/** クーポン一覧の確認だけを行う (読み取り専用・作成なし) */
import { openYahooContext, gotoStorePage, STORE_TOP_URL } from './lib-yahoo-login.mjs';

const ctx = await openYahooContext();
const page = ctx.pages()[0] || (await ctx.newPage());
try {
  await gotoStorePage(page, `${STORE_TOP_URL}/coupon/index`, 'クーポン一覧');
  const rows = await page.evaluate(() => {
    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const out = [];
    for (const tr of document.querySelectorAll('tr')) {
      const tds = [...tr.querySelectorAll('td')];
      if (tds.length < 12) continue;
      const c = tds.map((td) => norm(td.innerText));
      if (!/^[A-Za-z0-9]{20,}$/.test(c[0])) continue;
      out.push({ id: c[0], title: c[1], discount: c[4], pubStart: c[7], pubEnd: c[8], state: c[11] });
    }
    return out;
  });
  console.log(`総件数: ${rows.length}`);
  console.log('\n=== 「点以上購入で」クーポン (新しい順に表示) ===');
  for (const r of rows.filter((x) => /点以上購入で/.test(x.title))) {
    console.log(`  ${r.pubStart} 〜 ${r.pubEnd}  ${r.discount.padEnd(8)} ${r.title}  [${r.state}]  ${r.id}`);
  }
  const aug = rows.filter((x) => /^2026\/08\/01/.test(x.pubStart));
  console.log(`\n★ 8/1開始のクーポン: ${aug.length} 件`);
  for (const r of aug) console.log(`  ${r.title} / ${r.discount} / ${r.state} / ${r.id}`);
} finally {
  await ctx.close();
}
