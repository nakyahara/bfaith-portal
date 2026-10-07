import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * スタッフマスタ 管理画面 (apps/staff/views/admin.ejs) の画面の動き — ブラウザで実際に押して確かめる
 *
 * 実行: node scripts/test-staff-admin-ui.mjs   (Playwright の Chromium が要る: npx playwright install chromium)
 * 本物の router + 一時 DB を express で立て、管理者のセッションだけ差し込む。
 * 検証 (Codex #1379 R4 Medium 3 件):
 *   1. 別の行の有効/無効・保存の競合で画面を読み直さない (ほかの行の未保存の編集が消えない)
 *   2. 役割・PIN・有効/無効は送信中に押せない・通信が切れても元に戻る (役割の応答の順番の入れ替わりを防ぐ)
 *   3. 区分を いろは利用者 にすると PIN 欄が「—」になり、職員に戻すと「PIN設定」が出る
 * 検証 (Codex #1379 R5):
 *   4. サーバーには届いて反映済みで応答だけ消えた → 画面を DB とそろえる (届く前に切れた場合だけでなく)
 *      / 保存後はサーバーがそろえた値を出す / 画面を開いた後に別の端末で設定された PIN も、消す前に確かめる
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';
import { chromium } from 'playwright';
import { fileURLToPath } from 'url';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'staff-ui-test-'));
const db = await import('../apps/staff/db.js');
const { default: staffRouter } = await import('../apps/staff/router.js');
db.getStaffDB();

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const app = express();
app.set('view engine', 'ejs');
app.use((req, _res, next) => { req.session = { authenticated: true, role: 'admin', email: 'ui-test@example.com', displayName: '試験' }; next(); });
app.use(express.static(fileURLToPath(new URL('../public', import.meta.url))));
app.use('/apps/staff', express.json(), staffRouter);
const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
const URL_ = `http://127.0.0.1:${server.address().port}/apps/staff/`;

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
const dialogs = [];
let promptAnswer = '1234';
let confirmAnswer = true;
page.on('dialog', d => {
  dialogs.push(d.message());
  if (d.type() === 'prompt') return d.accept(promptAnswer);
  return confirmAnswer ? d.accept() : d.dismiss();
});
const pageErrors = [];
page.on('pageerror', e => pageErrors.push(e.message));

const byNo = no => db.getStaffByNo(no);
const row = no => page.locator(`tr[data-id="${byNo(no).id}"]`);
const open = async () => { await page.goto(URL_); await page.evaluate(() => { window.__notReloaded = true; }); };
const notReloaded = () => page.evaluate(() => window.__notReloaded === true);
const clearMsg = () => page.evaluate(() => { document.querySelector('#listMsg').textContent = ''; });

try {
  console.log('\n[1] 別の行の操作で画面を読み直さない');
  {
    await open();
    const a = row('20250901'), b = row('20250701');
    await a.locator('input[name=short_name]').fill('りっか(編集中)');
    await b.locator('.btn-active').click();
    await page.waitForFunction(id => document.querySelector(`tr[data-id="${id}"]`).dataset.active === '0', byNo('20250701').id);
    ok(await notReloaded(), '無効にしても画面を読み直さない');
    ok(await a.locator('input[name=short_name]').inputValue() === 'りっか(編集中)', 'ほかの行の未保存の入力が残る');
    ok(await a.locator('input[name=short_name]').evaluate(el => el.classList.contains('dirty')), 'ほかの行は未保存 (黄色) のまま');
    ok(await b.locator('.chip').textContent() === '無効' && await b.evaluate(tr => tr.classList.contains('inactive')), '無効にした行のチップと見た目が変わる');
    ok(await b.locator('.btn-active').textContent() === '有効に', 'ボタンが「有効に」になる');
    ok(byNo('20250701').active === 0 && !!byNo('20250701').left_on, 'DB も無効・退職日が入る');
    ok(await b.locator('input[name=left_on]').inputValue() === byNo('20250701').left_on, '退職日の欄も DB の値になる');
    ok(await page.locator('#segInactive').textContent() === '1' && await page.locator('#cntActive').textContent() === '12', '件数 (有効 12 / 無効 1) が書き換わる');
    ok(await b.getAttribute('data-version') === String(byNo('20250701').version), 'version が新しくなる');
    // 続けて有効に戻せる (version が古いままだと conflict になる)
    await b.locator('.btn-active').click();
    await page.waitForFunction(id => document.querySelector(`tr[data-id="${id}"]`).dataset.active === '1', byNo('20250701').id);
    ok(byNo('20250701').active === 1 && await notReloaded(), '続けて有効に戻せる (読み直さない)');

    // 保存の競合: 他の人が先に変えた → その行だけ最新にして、入れた欄は残す
    const c = row('0002');
    const before = byNo('0002');
    await c.locator('input[name=sort]').fill('35');
    db.updateStaff(before.id, { note: '他の人のメモ' }, 'other', before.version);
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await c.locator('.btn-save').click();
    await page.waitForFunction(() => /他の人が先に/.test(document.querySelector('#listMsg').textContent));
    ok(await notReloaded(), '保存の競合でも画面を読み直さない');
    ok(await a.locator('input[name=short_name]').inputValue() === 'りっか(編集中)', '競合しても、ほかの行の未保存の入力が残る');
    ok(await c.locator('input[name=note]').inputValue() === '他の人のメモ', '競合した行の触っていない欄は最新の値になる');
    ok(await c.locator('input[name=sort]').inputValue() === '35' && await c.locator('input[name=sort]').evaluate(el => el.classList.contains('dirty')), '自分が入れた欄は残り、未保存のまま');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await c.locator('.btn-save').click();
    await page.waitForFunction(() => /を保存しました/.test(document.querySelector('#listMsg').textContent));
    ok(byNo('0002').sort === 35 && byNo('0002').note === '他の人のメモ', 'もう一度「保存」で通る (他の人の変更も消さない)');

    // 未保存の行があるうちは「追加」させない (追加は画面を読み直すため)
    await page.fill('#n_no', '20261008'); await page.fill('#n_name', '試験 太郎');
    await page.click('#addBtn');
    ok(/未保存・保存中の行があります/.test(await page.locator('#addMsg').textContent()) && !byNo('20261008'), '未保存の行があると追加しない');
    ok(await notReloaded(), '追加を断ったときも読み直さない');
    await a.locator('input[name=short_name]').fill(byNo('20250901').short_name || '');
    await page.click('#addBtn');
    await page.waitForFunction(() => window.__notReloaded !== true);
    ok(!!byNo('20261008'), '未保存が無くなれば追加でき、画面を読み直す');
  }

  console.log('\n[2] 役割・PIN・有効/無効の送信中と通信の失敗');
  {
    await open();
    const r = row('20250901');
    const wh = r.locator('.role-cb[data-role=warehouse]'), of = r.locator('.role-cb[data-role=office]');
    // 送信中は押せない → 応答の順番の入れ替わりが起きない
    let release;
    const gate = new Promise(res => { release = res; });
    await page.route('**/roles', async route => { await gate; await route.continue(); });
    await of.check({ force: true });
    ok(await wh.isDisabled() && await of.isDisabled(), '役割の送信中は同じ行のチェックを押せない');
    ok(await r.locator('.btn-active').isDisabled(), '送信中は有効/無効も押せない');
    release();
    await page.waitForFunction(() => /役割を保存しました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute('**/roles');
    ok(!(await of.isDisabled()) && !(await r.locator('.btn-active').isDisabled()), '応答のあと押せるように戻る');
    ok(db.getStaffByNo('20250901').roles.join(',') === 'office,warehouse' && await of.isChecked(), '画面と DB の役割が同じ');
    // 通信が切れた → チェックを戻し、押せるように戻す
    await page.route('**/roles', route => route.abort());
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await of.uncheck({ force: true });
    await page.waitForFunction(() => /通信が途中で切れました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute('**/roles');
    ok(await of.isChecked(), '通信が切れたら役割のチェックを元に戻す');
    ok(!(await of.isDisabled()), '通信が切れても押せるように戻る');
    ok(db.getStaffByNo('20250901').roles.join(',') === 'office,warehouse', 'DB の役割は変わっていない');
    // PIN の通信が切れた
    await page.route('**/pin', route => route.abort());
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-pin').click();
    await page.waitForFunction(() => /通信が途中で切れました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute('**/pin');
    ok(!(await r.locator('.btn-pin').isDisabled()) && await r.locator('.pin-mark').textContent() === '未', 'PIN の通信が切れてもボタンが戻り、印は「未」のまま');
    // 有効/無効の通信が切れた
    await page.route('**/active', route => route.abort());
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-active').click();
    await page.waitForFunction(() => /通信が途中で切れました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute('**/active');
    ok(!(await r.locator('.btn-active').isDisabled()) && db.getStaffByNo('20250901').active === 1, '有効/無効の通信が切れてもボタンが戻る (DB は有効のまま)');
    ok(await notReloaded(), 'ここまで画面を読み直していない');
  }

  console.log('\n[3] 区分を変えたら PIN 欄を描き直す');
  {
    await open();
    const r = row('20250901');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-pin').click();
    await page.waitForFunction(() => /PIN を設定しました/.test(document.querySelector('#listMsg').textContent));
    ok(await r.locator('.pin-mark').textContent() === '🔑' && await r.locator('.btn-pin').textContent() === '再設定', 'PIN を設定すると 🔑 / 再設定');
    dialogs.length = 0;
    await r.locator('select[name=kind]').selectOption('iroha');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /を保存しました/.test(document.querySelector('#listMsg').textContent));
    ok(dialogs.some(m => /職員PIN は消えます/.test(m)), 'PIN を持つ人を いろは利用者 にするときは確かめる');
    ok(!db.getStaffByNo('20250901').pin_set, 'DB の PIN は消える');
    ok(await r.locator('td.w-pin').textContent() === '—' && await r.locator('.btn-pin').count() === 0, 'PIN 欄は「—」になる (古い 🔑 が残らない)');
    await r.locator('select[name=kind]').selectOption('part_time');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /を保存しました/.test(document.querySelector('#listMsg').textContent) && document.querySelector('.btn-pin'));
    ok(await r.locator('.pin-mark').textContent() === '未' && await r.locator('.btn-pin').textContent() === 'PIN設定', '職員に戻すと「未 / PIN設定」が出る (読み直さなくてよい)');
    promptAnswer = '5678';
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-pin').click();
    await page.waitForFunction(() => /PIN を設定しました/.test(document.querySelector('#listMsg').textContent));
    ok(!!db.getStaffByNo('20250901').pin_set && await r.locator('.pin-mark').textContent() === '🔑', '描き直したボタンでも PIN を設定できる');
    ok(await notReloaded(), 'ここまで画面を読み直していない');
  }
  console.log('\n[4] 応答だけ消えた / サーバーがそろえた値 / 別の端末で設定された PIN');
  {
    await open();
    const r = row('20241001');
    const id = byNo('20241001').id;
    // 届いて反映済みなのに応答だけ消えた (route.fetch でサーバーに送ってから、ブラウザへは abort)
    const lose = async route => { await route.fetch(); await route.abort(); };
    const wh = r.locator('.role-cb[data-role=warehouse]');
    ok(!(await wh.isChecked()), '前提: 高島 和美は倉庫なし');
    await page.route('**/roles', lose);
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await wh.check({ force: true });
    await page.waitForFunction(() => /通信が途中で切れました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute('**/roles');
    ok(byNo('20241001').roles.includes('warehouse'), 'DB には役割が入っている (届いていた)');
    ok(await wh.isChecked(), '画面も DB と同じ (「失敗」と決めつけてチェックを戻さない)');
    ok(!(await wh.isDisabled()), '押せるように戻る');

    const isSave = u => new URL(u).pathname === '/apps/staff/api/staff/' + id;
    await page.route(isSave, async route => (route.request().method() === 'POST' ? lose(route) : route.continue()));
    await r.locator('input[name=short_name]').fill('たかしま');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /通信が途中で切れました/.test(document.querySelector('#listMsg').textContent));
    await page.unroute(isSave);
    ok(byNo('20241001').short_name === 'たかしま', 'DB には保存されている (届いていた)');
    ok(!(await r.locator('input[name=short_name]').evaluate(el => el.classList.contains('dirty'))), '画面も保存済み (黄色が消える)');
    ok(await r.getAttribute('data-version') === String(byNo('20241001').version), 'version も DB と同じ → 次の保存が競合にならない');
    await r.locator('input[name=sort]').fill('75');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /を保存しました/.test(document.querySelector('#listMsg').textContent));
    ok(byNo('20241001').sort === 75, '続けて保存できる');

    // サーバーがそろえた値 (メールは小文字・前後の空白なし) を出す
    await page.check('#detailToggle');
    await r.locator('input[name=portal_email]').fill('  Takashima@B-Faith.BIZ ');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /を保存しました/.test(document.querySelector('#listMsg').textContent));
    ok(byNo('20241001').portal_email === 'takashima@b-faith.biz', 'DB は小文字・空白なし');
    ok(await r.locator('input[name=portal_email]').inputValue() === 'takashima@b-faith.biz'
      && !(await r.locator('input[name=portal_email]').evaluate(el => el.classList.contains('dirty'))), '画面も DB と同じ値で保存済み');

    // 画面を開いた後に、別の端末で PIN が設定された (PIN の設定は version を進めない)
    ok(await r.locator('.pin-mark').textContent() === '未', '前提: 画面では PIN 未設定');
    ok(db.setStaffPin(id, '2468', 'ipad').ok, '別の端末で PIN を設定');
    dialogs.length = 0;
    confirmAnswer = false;
    await r.locator('select[name=kind]').selectOption('iroha');
    await clearMsg();   // 前の操作の表示が残っていると、応答を待たずに進んでしまう
    await r.locator('.btn-save').click();
    await page.waitForFunction(() => /保存をやめました/.test(document.querySelector('#listMsg').textContent));
    confirmAnswer = true;
    ok(dialogs.some(m => /職員PIN は消えます/.test(m)), '画面で「未」でも、送る前に読み直して PIN があれば確かめる');
    ok(!!byNo('20241001').pin_set && byNo('20241001').kind !== 'iroha', '「やめる」なら PIN も区分もそのまま');
    ok(await r.locator('.pin-mark').textContent() === '🔑', 'PIN 欄も 🔑 に直る');
    ok(await r.locator('select[name=kind]').evaluate(el => el.classList.contains('dirty')) && !(await r.locator('.btn-save').isDisabled()), '区分の変更は未保存のまま残り、保存を押し直せる');
    ok(await notReloaded(), 'ここまで画面を読み直していない');
  }
  ok(pageErrors.length === 0, `画面の JS エラーなし${pageErrors.length ? ': ' + pageErrors.join(' / ') : ''}`);
} finally {
  await browser.close();
  server.close();
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
