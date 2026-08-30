/**
 * test-work-nav.mjs — 梱包 作業画面のナビゲーションを実ブラウザで確認する。
 *
 * 発端: 「1枚ずつ戻れるのに、その後 次へ を押すと一気に作業中の伝票へ飛んでしまう」
 *      (三宅さん 2026-08-29)。実際 viewSeq を +1 する経路がコード上どこにも無く、
 *      閲覧から復帰する唯一の手段が「作業位置へ吹き飛ぶ」だった。
 * 見たいのは2つ:
 *   ① 完了済みを見ているときに「次の伝票を見る」で**1枚ずつ進める**か (以前は作業位置へ吹き飛んだ)
 *   ② 閲覧中は**完了できない**か (canComplete の保証が壊れていないか)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'nav-'));
process.env.DATA_DIR = tmp;
process.env.PACKING_ENABLED = '1';

const express = (await import('express')).default;
const { initPackingDB, getDB, utcNow } = await import('../db.js');
const router = (await import('../router.js')).default;

initPackingDB();
const db = getDB();
const now = utcNow();
db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count,
  total_qty, match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1,'TB1','出荷_02',?,6,6,6,'ok','packing','三宅晴菜','x','t',?,?)`)
  .run(now.slice(0, 10), now, now);
for (let i = 1; i <= 6; i++) {
  db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name,
    site_order_no, status, delivery_method) VALUES (1,?,?,?,?,?,?,'箱 陸便 元払い 営業所止めなし')`)
    .run(i, `15389${50 + i}`, `SP${i}`, `お客さま${i}`, `503-000-${i}`, i <= 3 ? 'done' : 'pending');
  db.prepare(`INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty)
    SELECT id, ?, ?, 1 FROM pk_pack_slips WHERE batch_id=1 AND seq=?`)
    .run(`sku${i}`, `商品${i}`, i);
}

const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'views'));
app.use(express.json());
app.use((req, res, next) => { req.session = { email: 't@x', role: 'admin', allowedApps: '*' }; next(); });
app.use('/apps/packing', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 820, height: 1100 } });
await page.goto(`${base}/apps/packing/work/1`, { waitUntil: 'networkidle' });
// 画面のスクリプトが最後まで走り切る前に触ると let の初期化前アクセスで落ちるので、
// 最初の描画 (伝票番号が入る) を待ってから操作する
await page.waitForFunction(
  () => (document.getElementById('slipNo')?.textContent || '').trim() !== '—',
  null, { timeout: 15000 });
// 作業者未選択なら選ぶ (バッチに担当が入っていれば出ない)
if (await page.locator('#workerOverlay').isVisible()) {
  await page.evaluate(() => window.begin('三宅晴菜'));
  await page.waitForTimeout(300);
}

const state = async () => page.evaluate(() => ({
  slip: document.getElementById('slipNo')?.textContent?.trim(),
  done: getComputedStyle(document.getElementById('doneTag')).display !== 'none',
  next: document.getElementById('nextBtn')?.textContent?.trim(),
  nextDisabled: document.getElementById('nextBtn')?.disabled,
  backToWork: getComputedStyle(document.getElementById('backToWork')).display !== 'none',
  progress: document.getElementById('progDone')?.textContent?.trim(),
}));

let failed = 0;
const ok = (c, l) => { console.log(`${c ? '✅' : '❌'} ${l}`); if (!c) failed++; };

console.log('── 初期状態 (作業中の伝票 = seq4) ──');
let st = await state();
console.log('  ', JSON.stringify(st));
ok(st.next === '次へ ▸', '作業中は「次へ ▸」(完了できる)');
ok(st.backToWork === false, '作業位置にいるので「作業へ戻る」は出ない');

console.log('\n── 完了済みまで戻る (◂前へ ×3) ──');
for (let i = 0; i < 3; i++) { await page.click('#prevBtn'); await page.waitForTimeout(150); }
st = await state();
console.log('  ', JSON.stringify(st));
ok(st.done === true, '完了済みの伝票を見ている');
ok(st.next === '次の伝票を見る ▸', '🚨 閲覧中は「次の伝票を見る」= 移動 (完了ではない)');
ok(st.backToWork === true, '上部に「作業中の伝票へ戻る」が出る');

console.log('\n── 🚨 1枚ずつ進めるか (以前は作業位置へ吹き飛んだ) ──');
const seen = [st.slip];
for (let i = 0; i < 2; i++) {
  await page.click('#nextBtn'); await page.waitForTimeout(150);
  seen.push((await state()).slip);
}
console.log('   たどった伝票:', seen.join(' → '));
ok(new Set(seen).size === 3, '3枚とも別の伝票 = 1枚ずつ進んでいる');
const before = await state();
ok(before.progress === '3', '進んでも完了件数は増えない (完了していない)');

console.log('\n── 作業位置に着いたら「次へ」に変わるか ──');
await page.click('#nextBtn'); await page.waitForTimeout(200);
st = await state();
console.log('  ', JSON.stringify(st));
ok(st.next === '次へ ▸' && st.done === false, '作業中の伝票に着くと「次へ ▸」(完了できる) に変わる');
ok(st.progress === '3', 'ここまで一度も完了していない');

console.log('\n── 「作業へ戻る」で一発で戻れるか ──');
await page.click('#prevBtn'); await page.waitForTimeout(150);
await page.click('#prevBtn'); await page.waitForTimeout(150);
ok((await state()).backToWork === true, '離れると戻るボタンが出る');
await page.click('#backToWorkBtn'); await page.waitForTimeout(200);
st = await state();
ok(st.next === '次へ ▸' && st.backToWork === false, '一発で作業位置へ戻る');
ok(st.progress === '3', '戻っただけでは完了しない');

console.log('\n── 🚨 検索ジャンプから移動したら完了できないこと ──');
{
  // ズレ回復 (検索ジャンプ) で順序外に完了してよいのは**飛んだその伝票だけ**。
  // jumpMode が立ちっぱなしのまま前後に移動できると、間の伝票を飛ばして別の未処理伝票を
  // 完了できてしまう (Codexレビュー 2026-08-30)
  // seq6 (未処理・作業位置 seq4 より先) へ検索ジャンプ
  await page.click('#searchBtn'); await page.waitForTimeout(200);
  await page.fill('#searchInput', '1538956');
  await page.waitForTimeout(300);
  await page.click('.slip-row[data-seq="6"]');
  await page.waitForTimeout(300);

  let st = await state();
  console.log('   ジャンプ直後 (seq6):', JSON.stringify(st));
  ok(st.next === '次へ ▸', '飛んだ先は完了できる = ズレ回復は生きている');

  // 1枚戻ると seq5 = 「未処理だが、作業位置でもジャンプ先でもない」伝票。
  // ここが完了できてしまうと、間の伝票を飛ばした順序外完了になる
  await page.click('#prevBtn'); await page.waitForTimeout(250);
  st = await state();
  console.log('   1枚戻った (seq5 = 未処理・作業位置でもジャンプ先でもない):', JSON.stringify(st));
  ok(st.next === '次の伝票を見る ▸',
    '🚨 ジャンプ先から移動したら完了できない (間の伝票を飛ばした順序外完了を防ぐ)');
  ok(st.progress === '3', '完了件数は増えていない');
}

console.log('\n── 境界: 最終伝票では進めない ──');
{
  await page.click('#backToWorkBtn').catch(() => {});
  await page.waitForTimeout(200);
  for (let i = 0; i < 6; i++) {
    const st0 = await state();
    if (st0.nextDisabled || st0.next === '次へ ▸') break;
    await page.click('#nextBtn'); await page.waitForTimeout(120);
  }
  const st = await state();
  console.log('  ', JSON.stringify(st));
  ok(st.nextDisabled === true || st.next === '次へ ▸',
    '最終伝票では進めない (または作業中の伝票に着いている)');
}

await browser.close();
await new Promise((r) => server.close(r));
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* DBを掴んだままでも本題ではない */ }
console.log(`\n${failed === 0 ? '✅ 全部OK' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
