/**
 * test-master-decisions-screen.mjs — マスタの判断の新しい画面 (10/5・マスタの入力と同じデザイン) の JS を、本物の router で描いたページで本当に動かす試験
 *
 * Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)・本物の router を express に載せ、Playwright (Chromium) で開く。決めるのも本当に流す。
 * 確かめること:
 *   1 流れの帯 (今日の差 → 決める → CSV → 取り込み) の数と「次にやること」・状態 / 理由の札の数・0 件の札は押せない・左の列の数
 *   2 行の右の「NE を直す」で 1 件決める (DB に入る・一覧から消える・次の行にフォーカス) → 「元に戻す」で判断待ちに戻る
 *   3 まとめて決める: Space で行を選ぶ → 下の帯 (件数・決められる数・決められないボタンは押せない) → 「差を残す」で全部入る・帯が消える
 *   4 1 件の窓: Enter で開く・Esc で閉じて元の行へ・決め方を選ぶと説明と「直す値」の欄が変わる・代表 (親) は空のまま = 親なし で決まる
 *   5 離れるときの確認: 直す値を入れたまま左の列の CSV へ → 確認 (1 件)・「ここに残る」で戻る・決めた後は聞かない / 選んだ四角だけなら聞かない
 *   6 絞る: 理由の札・SKU の欄 (/ で入る・Enter で絞る)・URL に残る (読み直しても同じ)
 *   7 CSV の画面: 判断の画面の「CSV を作る画面へ」で移る・作る → ファイルの札 (ダウンロードのリンク = api/csv/exports/N/file) → 確かめる → 結果を選ばずに申告 = 止める → 選んで申告
 *   8 名簿に無い人: 見るだけ (四角・決めるボタン・まとめての帯が無い・「見るだけです」)
 *   9 1440 / 1280 / 1024 幅: 判断・CSV・つかいかたの画面がページの横にはみ出さない・下の帯が画面の中
 *  11 (#1626 Codex R1) 結果の欄の読み上げ (status / 失敗は alert)・CSV の「次にやること」のボタンが送る (作る所・ファイル・届かなかった行)・
 *     書く操作の連打で POST は 1 回 (遅い通信でダブルクリック・押した直後にボタンが閉じる)・離れるときの確認が実機の確かめの欄を全部数えて最初の欄へ戻す
 *  10 部品はこの口から (マスタの入力の CSS・共通の動きを共有): 404 が無い・全体から探す (Ctrl+K) が開いて閉じる
 * 写しを撮る: env MASTER_DECISIONS_SHOTS=<フォルダ> を付けると 1440 / 1280 / 1024 幅の写しを置く (付けなければ撮らない)
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_DECISIONS_UI_SKIP=1 を付けたときだけ (黙って飛ばさない)
 * 使い方: node scripts/test-master-decisions-screen.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import express from 'express';

if (process.env.MASTER_DECISIONS_UI_SKIP === '1') { console.log('MASTER_DECISIONS_UI_SKIP=1 = 飛ばした'); process.exit(0); }
let chromium, browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / この試験だけ飛ばす = MASTER_DECISIONS_UI_SKIP=1`);
  process.exit(1);
}

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
const { default: router, __setPgClientFactory } = await import('../apps/master-decisions/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const SHOTS = process.env.MASTER_DECISIONS_SHOTS || '';
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });

// ── Company DB ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });

// 照合の回 = いま (画面の「今日の照合がまだ」・CSV の「今日の回」を本物の時刻で)
const nowMs = Date.now();
const ymd = new Date(nowMs).toISOString().slice(0, 10).replace(/-/g, '');
let seq = 0;
const runId = () => `mc_${ymd}T${String(++seq).padStart(9, '0')}Z_abcdef`;
const H = (label) => crypto.createHash('sha256').update(label).digest('hex');
const C = {};
const mk = (label, o) => {
  const code = o.sk.split(':')[1];
  C[label] = { fingerprint: H(label), subject_key: o.sk, code_norm: code, col: o.col, child: o.child ?? null, cls: o.cls || 'rule', reason_kind: o.reason, semantic: `${o.reason}@1`,
    print: { code_norm: code, sku_kind: o.kind || 'single', col: o.col, child: o.child ?? null, n: o.n ?? null, n_state: o.n_state ?? null, c: o.c ?? null, reason: { reason: o.reason }, label },
    resolutions: o.res, proposal: o.prop };
  return C[label];
};
const own = (label, sk, col, n, c, kind) => mk(label, { sk, col, n, c, kind, reason: 'company_owned', res: ['fix_ne', 'accept_difference'], prop: { op: 'set_ne_value', value: c } });
own('c1', 'cost:0726-000629-bk', 'cost', 820, 860);
own('c2', 'cost:0726-000630-wh', 'cost', 820, 860);
own('c3', 'cost:hn-500-acacia', 'cost', 1240, 1310);
own('p1', 'value:tn-2001', 'standard_price_jpy', 1480, 1580);
own('h1', 'value:kb-118', 'handling', 'active', 'discontinued');
own('s1', 'primary_supplier:gl-77', 'primary_supplier', '0100', '0135');
mk('t1', { sk: 'value:a8-honey-500', col: 'tax_rate', cls: 'ne_no_value', reason: 'tax_fallback', n: null, n_state: 'empty', c: 0.08, res: ['accept_difference', 'fix_ne'], prop: { op: 'set_ne_value', value: 0.08 } });
mk('n1', { sk: 'value:st-gift-03', col: 'name', kind: 'set', reason: 'load_rule:name_blank_to_code', n: '', c: 'st-gift-03', res: ['fix_ne', 'accept_difference'], prop: { op: 'set_ne_value', value: '母の日ギフト 3 点セット' } });
mk('m1', { sk: 'components:st-gift-03', col: 'components', child: 'hn-500-acacia', kind: 'set', reason: 'manual', n: 1, c: 2, res: ['accept_difference', 'fix_cdb'], prop: { op: 'decide_manual_priority' } });
mk('r1', { sk: 'parent:tn-2001-bk', col: 'parent', reason: 'parent_manual', n: 'tn-2001', c: 'tn-2000', res: ['accept_difference', 'fix_ne', 'fix_cdb'], prop: { op: 'decide_manual_priority' } });
mk('u1', { sk: 'value:zz-old-9', col: 'name', cls: 'spec_undecided', reason: 'spec_undecided', n: '旧パッケージ', c: '新パッケージ', res: ['spec', 'accept_difference'], prop: { op: 'decide_spec' } });
const ALL = Object.keys(C);
const R1 = runId();
await writeDecisions(db, { compareRunId: R1, observedAt: new Date(nowMs - 3 * 86400e3).toISOString(), decisions: ALL.map((l) => C[l]) });
const R2 = runId();
await writeDecisions(db, { compareRunId: R2, observedAt: new Date(nowMs - 60e3).toISOString(), decisions: ALL.map((l) => C[l]) });
// NE の元の書き方 (今日の回) = CSV にできる
const entries = [...new Set(ALL.map((l) => C[l].code_norm))].map((n) => ({ code_norm: n, kind: 'product', state: 'ok', ne_code: n.toUpperCase(), spellings: [n.toUpperCase()] }));
entries.push({ code_norm: 'tn-2000', kind: 'rep', state: 'ok', ne_code: 'TN-2000', spellings: ['TN-2000'] });
await db.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: R2, entries })]);

// ── ポータル (本物の router・セッションは Cookie で模擬) ──
process.env.COMPANY_DB_URL = 'postgres://test@localhost:5432/test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
__setPgClientFactory(async () => ({ query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} }));
const app = express();
app.set('view engine', 'ejs');
const notFound = [];
app.use((req, res, next) => {
  const s = /(?:^|;\s*)sess=([a-z]+)/.exec(String(req.headers.cookie || ''))?.[1] || req.headers['x-test-session'];
  req.session = s === 'approver' ? { authenticated: true, email: 'naka@test', displayName: '中原 大輔', role: 'user', allowedApps: ['master-decisions'] }
    : s === 'user' ? { authenticated: true, email: 'user@test', displayName: '利用者', role: 'user', allowedApps: ['master-decisions'] } : null;
  next();
});
app.use('/apps/master-decisions', router);
app.use((req, res) => { if (!req.path.startsWith('/apps/master-edit/') && req.path !== '/favicon.png') notFound.push(req.path); res.status(404).end(); });
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-decisions/`;
async function api(method, url, body) {
  const r = await fetch(ORIGIN + '/apps/master-decisions/' + url, { method, headers: { Accept: 'application/json', 'x-test-session': 'approver', ...(body ? { 'Content-Type': 'application/json', Origin: ORIGIN } : {}) }, body: body ? JSON.stringify(body) : undefined });
  return r.json();
}
const cand = async (label) => (await api('GET', 'api/candidates?status=any&view=all&limit=1000')).items.find((c) => c.fingerprint === C[label].fingerprint);

// 前もって 1 件 NE を直すと決めておく (CSV にできる承認 = 流れの帯の ③)
{
  const c = await cand('c3');
  const r = await api('POST', 'api/decisions', { kind: 'approved', resolution: 'fix_ne', items: [{ fingerprint: c.fingerprint, shown_last_seen_run: c.last_seen_run, shown_event_id: null }] });
  assert.equal(r.applied.length, 1);
}

async function newPage(session, width = 1440, height = 900) {
  const ctx = await browser.newContext({ viewport: { width, height }, locale: 'ja-JP', timezoneId: 'Asia/Tokyo' });
  await ctx.addCookies([{ name: 'sess', value: session, url: ORIGIN }]);
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => { if (m.type() === 'error' && !/404|Failed to load resource/.test(m.text())) errors.push(m.text()); });
  return { ctx, page, errors };
}
// 写し: 一覧の画面は上から全部 (上に戻してから・フォーカスの線を外して)。窓・下の帯が開いているときは見えている所だけ
const shot = async (page, name, { full = true } = {}) => {
  if (!SHOTS) return;
  if (full) await page.evaluate(() => { scrollTo(0, 0); if (document.activeElement && document.activeElement.blur && !document.querySelector('.drawer-bg.on')) document.activeElement.blur(); });
  await page.screenshot({ path: path.join(SHOTS, name), fullPage: full });
};
const settle = (page) => page.waitForFunction(() => !document.querySelector('#rows .loadrow') || /ありません/.test(document.querySelector('#rows').textContent));

let P;
try {
  P = await newPage('approver');
  const { page } = P;

  await ta('[1] 流れの帯の数と「次にやること」・状態 / 理由の札・0 件の札は押せない・左の列の数', async () => {
    await page.goto(BASE);
    await settle(page);
    assert.equal(await page.locator('#rows tr').count(), ALL.length - 1);   // c3 は承認済み
    assert.equal(await page.textContent('#st1-n'), String(ALL.length));
    assert.equal(await page.textContent('#st2-n'), String(ALL.length - 1));
    await page.waitForFunction(() => document.getElementById('st3-n').textContent === '1');
    assert.match(await page.getAttribute('#st-2', 'class'), /\bcur\b/);
    assert.match(await page.textContent('#next-t'), new RegExp(`判断待ちが ${ALL.length - 1} 件`));
    assert.equal(await page.textContent('#rail-n-decide'), String(ALL.length - 1));
    assert.equal(await page.isHidden('#stale'), true);
    assert.match(await page.textContent('#chips-status [data-status="pending"]'), new RegExp(String(ALL.length - 1)));
    assert.equal(await page.isDisabled('#chips-status [data-status="done"]'), true);   // 完了 0 件 = 押せない
    assert.equal(await page.isDisabled('#chips-status [data-status="approved"]'), false);
    // 行の見せ方: NE → 社内・円は 3 桁ごと・税率は %
    const row = page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: 'hn-500-acacia' }) });
    assert.equal(await row.count(), 0);   // 承認済みは判断待ちの一覧に出ない
    assert.match(await page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: 'tn-2001' }) }).first().textContent(), /1,480 円.*1,580 円/);
    assert.match(await page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: 'a8-honey-500' }) }).textContent(), /\(空\).*8%/);
    await shot(page, '01_判断_1440.png');
  });

  await ta('[2] 行の「NE を直す」で 1 件決める → 一覧から消える・次の行にフォーカス → 「元に戻す」で判断待ちに戻る', async () => {
    const row = page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: '0726-000629-bk' }) });
    const idx = Number(await row.getAttribute('data-i'));
    await row.locator('[data-q="fix_ne"]').click();
    await page.waitForSelector('#res .callout');
    assert.match(await page.textContent('#res'), /「NE を直す」と決めました 1 件/);
    const c = await cand('c1');
    assert.deepEqual([c.status, c.decision.resolution, c.decision.target.value, c.decision.actor], ['approved', 'fix_ne', 860, 'naka@test']);
    assert.equal(await page.locator('#rows a.rowlink', { hasText: '0726-000629-bk' }).count(), 0);
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.classList.contains('rowlink')), true);
    assert.equal(await page.evaluate(() => Number(document.activeElement.dataset.open)), idx);
    await page.waitForFunction(() => document.getElementById('st3-n').textContent === '2');   // CSV にできる承認が 1 件増えた
    await page.click('#undo');
    await page.waitForFunction(() => /元に戻しました 1 件/.test(document.getElementById('res').textContent));
    assert.equal((await cand('c1')).status, 'pending');
    assert.equal(await page.locator('#rows a.rowlink', { hasText: '0726-000629-bk' }).count(), 1);
    // 結果の欄は読み上げの欄 (status)。決められなかった = alert (#1626 Codex R1 L4)
    assert.deepEqual([await page.getAttribute('#res', 'role'), await page.getAttribute('#res', 'aria-live')], ['status', 'polite']);
    await page.route('**/api/decisions', (route) => route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'サーバーエラーが発生しました' }) }));
    await page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: '0726-000629-bk' }) }).locator('[data-q="fix_ne"]').click();
    await page.waitForFunction(() => /決められませんでした/.test(document.getElementById('res').textContent));
    assert.equal(await page.getAttribute('#res', 'role'), 'alert');
    await page.unroute('**/api/decisions');
    assert.equal((await cand('c1')).status, 'pending');
    await page.click('#res-x');
    assert.equal(await page.isHidden('#res'), true);
  });

  await ta('[3] まとめて決める: Space で選ぶ → 下の帯 (件数・決められる数・押せないボタン) → 差を残す で全部入る・帯が消える', async () => {
    assert.equal(await page.isHidden('#bulk'), true);
    for (const code of ['kb-118', 'zz-old-9']) {
      await page.locator('#rows a.rowlink', { hasText: code }).focus();
      await page.keyboard.press('Space');
    }
    assert.equal(await page.isVisible('#bulk'), true);
    assert.equal(await page.textContent('#selcount'), '2 件を選択中');
    assert.match(await page.textContent('#sel-break'), /NE を直せる 1 件 \(1 件は飛ばします\) · 差を残せる 2 件/);
    assert.equal(await page.isDisabled('#bulk [data-act="fix_ne"]'), false);
    // 帯は画面の中 (下に固定)
    await page.locator('#bulk').evaluate((el) => Promise.all(el.getAnimations().map((a) => a.finished)));
    const box = await page.locator('#bulk').boundingBox();
    assert.ok(box && box.y + box.height <= 900 + 1, '帯が画面の外 ' + JSON.stringify(box) + ' ' + JSON.stringify(await page.evaluate(() => [innerHeight, getComputedStyle(document.getElementById('bulk')).position, document.getElementById('bulk').getBoundingClientRect().bottom])));
    await shot(page, '02_判断_まとめて選ぶ_1440.png', { full: false });
    await page.fill('#note', 'まとめて残す');
    await page.click('#bulk [data-act="accept_difference"]');
    await page.waitForFunction(() => /「差を残す」と決めました 2 件/.test(document.getElementById('res').textContent));
    for (const l of ['h1', 'u1']) { const c = await cand(l); assert.deepEqual([c.status, c.decision.resolution, c.decision.note], ['approved', 'accept_difference', 'まとめて残す']); }
    assert.equal(await page.isHidden('#bulk'), true);
    assert.equal(await page.inputValue('#note'), '');
    // 1 件しか NE を直せない組 = fix_ne だけ選ぶと押せる、決められないもの (構成) だけだと押せない
    await page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: 'st-gift-03' }) }).filter({ hasText: '構成' }).locator('.ckb').check();
    assert.equal(await page.isDisabled('#bulk [data-act="fix_ne"]'), true);
    assert.equal(await page.isDisabled('#bulk [data-act="accept_difference"]'), false);
    await page.click('#sel-clear');
    assert.equal(await page.isHidden('#bulk'), true);
  });

  await ta('[4] 1 件の窓: Enter で開く・Esc で閉じて元の行へ・決め方で説明と直す値が変わる・代表 (親) は空 = 親なし', async () => {
    const link = page.locator('#rows a.rowlink', { hasText: 'tn-2001-bk' });
    await link.focus();
    await page.keyboard.press('Enter');
    await page.waitForSelector('#dr-bg.on #d-seg');
    assert.match(await page.textContent('#dr-t'), /tn-2001-bk/);
    assert.equal(await page.evaluate(() => document.getElementById('drawer').contains(document.activeElement)), true);
    await shot(page, '03_判断_1件の窓_1440.png', { full: false });
    await page.keyboard.press('Escape');
    assert.equal(await page.isHidden('#dr-bg'), true);
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.textContent), 'tn-2001-bk');
    // 行を押して開く → 差を残す を選ぶと直す値の欄が消える → NE を直す に戻すと出る
    await link.click();
    await page.waitForSelector('#dr-bg.on #d-seg');
    await page.click('#d-seg button[data-v="accept_difference"]');
    assert.equal(await page.isHidden('#d-val-wrap'), true);
    assert.match(await page.textContent('#d-what'), /社内の値として持つ/);
    await page.click('#d-seg button[data-v="fix_ne"]');
    assert.equal(await page.isVisible('#d-val-wrap'), true);
    assert.match(await page.getAttribute('#d-val', 'placeholder'), /空 = 親なし/);
    // Tab は窓の中だけで回る
    for (let i = 0; i < 12; i++) await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(() => document.getElementById('drawer').contains(document.activeElement)), true);
    await page.click('#d-ok');
    await page.waitForFunction(() => /「NE を直す」と決めました 1 件/.test(document.getElementById('res').textContent));
    const c = await cand('r1');
    assert.deepEqual([c.status, c.decision.resolution, c.decision.target.value], ['approved', 'fix_ne', null]);   // 親なし
    assert.match(await page.textContent('#detail'), /承認: NE を直す/);   // 窓は読み直す
    await page.keyboard.press('Escape');
  });

  await ta('[5] 離れるときの確認: 直す値を入れたまま左の列へ → 確認・ここに残る / 選んだ四角だけなら聞かない', async () => {
    const link = page.locator('#rows tr', { has: page.locator('a.rowlink', { hasText: 'st-gift-03' }) }).filter({ hasText: '構成' }).locator('a.rowlink');
    await link.click();
    await page.waitForSelector('#dr-bg.on #d-seg');
    await page.click('#d-seg button[data-v="fix_cdb"]');
    await page.fill('#d-val', '3');
    assert.equal(await page.isVisible('#unsaved'), true);
    await page.keyboard.press('Escape');   // 窓を閉じると入力も消える = 数えない
    assert.equal(await page.isHidden('#unsaved'), true);
    await link.click();
    await page.waitForSelector('#dr-bg.on #d-seg');
    await page.click('#d-seg button[data-v="fix_cdb"]');
    await page.fill('#d-val', '3');
    // ブラウザの戻る (窓を開いたまま) = 確認が出る
    await page.evaluate(() => history.back());
    await page.waitForSelector('#leave-bg.on');
    await page.click('#leave-stay');
    assert.equal(await page.isVisible('#dr-bg'), true);
    // 左の列の CSV (窓の後ろ。全体から探す と同じく画面を移る道) = 確認が出る
    await page.evaluate(() => document.querySelector('.rail a.nav[href$="/csv"]').click());
    await page.waitForSelector('#leave-bg.on');
    assert.match(await page.textContent('#leave-list'), /st-gift-03 構成 \/ hn-500-acacia の直す値「3」/);
    await page.click('#leave-stay');
    assert.equal(await page.isHidden('#leave-bg'), true);
    assert.equal(page.url().split('?')[0], BASE);
    await page.click('#d-ok');
    await page.waitForFunction(() => /「社内の値を直す」と決めました 1 件/.test(document.getElementById('res').textContent));
    assert.deepEqual([(await cand('m1')).decision.target.value], [3]);
    await page.keyboard.press('Escape');
    assert.equal(await page.isHidden('#unsaved'), true);
    // 選んだ四角だけ = 聞かずに移る
    await page.locator('#rows .ckb').first().check();
    await Promise.all([page.waitForURL(/\/csv$/), page.click('.rail a.nav[href$="/csv"]')]);
    await page.goBack();
    await settle(page);
  });

  await ta('[6] 絞る: 理由の札・SKU の欄 (/ で入る・Enter)・URL に残る', async () => {
    await page.click('#chips-reason [data-reason="company_owned"]');
    await page.waitForFunction(() => /reason=company_owned/.test(location.search));
    await page.waitForFunction(() => { const a = [...document.querySelectorAll('#rows a.rowlink')]; return a.length > 0 && a.every((x) => /^(0726-|tn-2001$|gl-77$)/.test(x.textContent)); }, null, { timeout: 5000 }).catch(async (e) => { throw new Error(e.message + ' ' + JSON.stringify(await page.evaluate(() => [location.href, [...document.querySelectorAll('#rows a.rowlink')].map((x) => x.textContent), document.getElementById('res').textContent]))); });
    const codes = await page.locator('#rows a.rowlink').allTextContents();
    assert.ok(codes.length >= 3 && codes.every((x) => /^(0726|tn-2001|gl-77)/.test(x)), codes.join(','));
    await page.locator('body').click({ position: { x: 5, y: 300 } });
    await page.keyboard.press('/');
    assert.equal(await page.evaluate(() => document.activeElement.id), 'q');
    await page.keyboard.type('000630');
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => /q=000630/.test(location.search));
    await page.waitForFunction(() => document.querySelectorAll('#rows a.rowlink').length === 1);
    await page.reload();
    await page.waitForFunction(() => document.querySelectorAll('#rows a.rowlink').length === 1);
    assert.equal(await page.inputValue('#q'), '000630');
    assert.match(await page.getAttribute('#chips-reason [data-reason="company_owned"]', 'class'), /\bon\b/);
    await page.goto(BASE);
    await settle(page);
  });

  await ta('[7] CSV の画面: 「CSV を作る画面へ」で移る・作る → ファイルの札 (ダウンロード) → 確かめる → 結果を選ばずに申告は止める → 選んで申告', async () => {
    // 判断待ちを全部決めて「次にやること」を CSV へ (残りは差を残す)
    await page.check('#all');
    await page.click('#bulk [data-act="accept_difference"]');
    await page.waitForFunction(() => /判断待ちの差はありません/.test(document.getElementById('rows').textContent));
    await page.waitForFunction(() => /CSV にできる承認が/.test(document.getElementById('next-t').textContent));
    assert.match(await page.getAttribute('#st-3', 'class'), /\bcur\b/);
    await shot(page, '04_判断_次はCSV_1440.png');
    await Promise.all([page.waitForURL(/\/csv$/), page.click('#next-a a[href="csv"]')]);
    await page.waitForSelector('[data-make="products:cost"]');
    assert.match(await page.getAttribute('#st-1', 'class'), /\bcur\b/);
    // 次にやること (作る) = 作る所へ送ってフォーカス (#1626 Codex R1 M1)
    const goneTo = async (sel, id) => {
      await page.click('#next-a ' + sel);
      await page.waitForFunction((x) => document.activeElement && document.activeElement.id === x, id);
      await page.waitForFunction((x) => { const r = document.getElementById(x).getBoundingClientRect(); return r.top >= 58 && r.top < innerHeight; }, id);
    };
    await page.evaluate(() => scrollTo(0, document.body.scrollHeight));
    await goneTo('[data-goto="make"]', 'make');
    // 遅い通信でダブルクリック = 作る POST は 1 回・送っている間はボタンが閉じる (#1626 Codex R1 M2)
    const posts = [];
    page.on('request', (rq) => { if (rq.method() === 'POST' && /\/api\/csv\//.test(rq.url())) posts.push(new URL(rq.url()).pathname); });
    await page.route('**/api/csv/exports', async (route) => { await new Promise((r) => setTimeout(r, 700)); await route.continue(); });
    await page.dblclick('[data-make="products:cost"]');
    await page.waitForFunction(() => document.querySelector('[data-make="products:cost"]').disabled && document.querySelector('[data-make="products:cost"]').getAttribute('aria-busy') === 'true');
    await page.waitForSelector('#exports .fcard');
    await page.unroute('**/api/csv/exports');
    assert.deepEqual(posts.filter((x) => /\/exports$/.test(x)).length, 1, posts.join(','));
    assert.equal(await page.locator('#exports .fcard').count(), 1);
    assert.match(await page.textContent('#msg'), /ファイル \d+ を作りました/);
    const id = (await page.textContent('#exports .fcard .fno')).replace(/\D/g, '');
    assert.equal(await page.getAttribute(`#file-${id} a[download]`, 'href'), `api/csv/exports/${id}/file`);
    const dl = await fetch(new URL(`api/csv/exports/${id}/file`, BASE + 'csv'), { headers: { 'x-test-session': 'approver' } });
    assert.equal(dl.status, 200); assert.match(await dl.text(), /genka_tnk/);
    assert.match(await page.getAttribute('#st-2', 'class'), /\bcur\b/);
    await page.evaluate(() => scrollTo(0, 0));
    await goneTo(`[data-goto="file-${id}"]`, `file-${id}`);
    // 確かめるの onclick を 2 回 (遅い通信) = POST は 1 回
    await page.route('**/check', async (route) => { await new Promise((r) => setTimeout(r, 700)); await route.continue(); });
    await page.evaluate((x) => { const b = document.querySelector('#file-' + x + ' [data-check]'); b.onclick({ stopPropagation() {} }); b.onclick({ stopPropagation() {} }); }, id);
    await page.waitForSelector(`#file-${id} [data-declare]`);
    await page.unroute('**/check');
    assert.equal(posts.filter((x) => /\/check$/.test(x)).length, 1, posts.join(','));
    await page.evaluate(() => scrollTo(0, document.body.scrollHeight));
    await goneTo(`[data-goto="file-${id}"]`, `file-${id}`);
    // 離れるときの確認: 申告のメモだけ → 「入力を確認する」でそのメモへ (#1626 Codex R1 L3)
    await page.fill(`[data-note-for="${id}"]`, '取込の履歴を見た');
    await page.evaluate(() => document.querySelector('.rail a.nav[href$="/apps/master-decisions/"]').click());
    await page.waitForSelector('#leave-bg.on');
    assert.match(await page.textContent('#leave-list'), new RegExp(`ファイル ${id} の申告のメモ`));
    await page.click('#leave-review');
    assert.equal(await page.evaluate(() => document.activeElement.dataset.noteFor), id);
    await page.fill(`[data-note-for="${id}"]`, '');
    // 実機の確かめ: 結果・ファイル番号を変えただけでも数える → 最初の変えた欄 (結果) へ戻る・既定に戻すと数えない
    await page.selectOption('#v-result', 'ng');
    await page.fill('#v-export', id);
    assert.equal(await page.textContent('#unsaved-n'), '未保存 1 件');
    await page.evaluate(() => document.querySelector('.rail a.nav[href$="/apps/master-decisions/"]').click());
    await page.waitForSelector('#leave-bg.on');
    assert.match(await page.textContent('#leave-list'), /実機で確かめた結果 \(結果・ファイル番号\)/);
    await page.click('#leave-review');
    assert.equal(await page.evaluate(() => document.activeElement.id), 'v-result');
    await page.selectOption('#v-key', await page.evaluate(() => document.querySelectorAll('#v-key option')[1].value));   // 既定 (最初) ではない種類
    await page.selectOption('#v-result', 'ok');
    await page.fill('#v-export', '');
    assert.equal(await page.isVisible('#unsaved'), true);   // 種類を変えた = まだ数える
    assert.match(await page.evaluate(() => window.MasterEdit.dirty().items.join()), /種類・項目/);
    await page.selectOption('#v-key', await page.evaluate(() => document.querySelector('#v-key option').value));
    assert.equal(await page.isHidden('#unsaved'), true);
    assert.match(await page.textContent('#next-t'), new RegExp(`ファイル ${id}.*今日のうちに NE に取り込んで`));
    await shot(page, '05_CSV_申告の前_1440.png');
    await page.click(`#file-${id} [data-declare]`);
    assert.match(await page.textContent('#msg'), /結果を 1 つ選んでください/);
    await page.click(`#file-${id} .reason-pick label:has(input[value="ok"])`);
    assert.equal(await page.isVisible('#unsaved'), true);   // 選んだまま = まだ申告していない
    await page.click(`#file-${id} [data-declare]`);
    await page.waitForFunction(() => /申告しました/.test(document.getElementById('msg').textContent));
    assert.equal(await page.isHidden('#unsaved'), true);
    assert.match(await page.textContent(`#file-${id}`), /取り込んだ \(申告済み\)/);
    // 届かなかった行がある (翌朝の照合の後) = 次にやること はそのファイルへ (届き方は画面だけ差し替えて見る)
    await page.route('**/api/csv/summary', async (route) => {
      const r = await route.fetch(); const j = await r.json();
      for (const e of j.exports) if (String(e.export_id) === id) e.row_states = { not_reflected: 1 };
      await route.fulfill({ response: r, json: j });
    });
    await page.reload();
    await page.waitForFunction(() => /届かなかった・確かめが要る行が 1 行/.test(document.getElementById('next-t').textContent));
    assert.match(await page.getAttribute('#next', 'class'), /\bwarn\b/);
    await goneTo(`[data-goto="file-${id}"]`, `file-${id}`);
    await page.unroute('**/api/csv/summary');
    await page.reload();
    await page.waitForSelector(`#file-${id}`);
    // 行を見る (右の窓)
    await page.click(`#file-${id} [data-open]`);
    await page.waitForSelector('#dr-bg.on table');
    assert.match(await page.textContent('#detail'), /HN-500-ACACIA|0726-000629-BK/);
    await page.keyboard.press('Escape');
    assert.equal(await page.isHidden('#dr-bg'), true);
  });

  await ta('[8] 名簿に無い人: 見るだけ (四角・決めるボタン・まとめての帯が無い)', async () => {
    const U = await newPage('user');
    try {
      await U.page.goto(BASE + '?status=any');
      await settle(U.page);
      assert.match(await U.page.textContent('#gate-top'), /見るだけです.*名簿の人だけ/);
      assert.equal(await U.page.locator('#rows .ckb').count(), 0);
      assert.equal(await U.page.locator('#rows [data-q]').count(), 0);
      assert.equal(await U.page.locator('#bulk').count(), 0);
      await U.page.locator('#rows a.rowlink').first().click();
      await U.page.waitForSelector('#dr-bg.on .callout.lock');
      assert.equal(await U.page.locator('#d-ok').count(), 0);
      assert.deepEqual(U.errors, []);
    } finally { await U.ctx.close(); }
  });

  await ta('[9] 1440 / 1280 / 1024 幅: 判断・CSV・つかいかた がページの横にはみ出さない・下の帯は画面の中', async () => {
    for (const w of [1440, 1280, 1024]) {
      const W = await newPage('approver', w, 800);
      try {
        for (const [url, name] of [['?status=any', '判断'], ['csv', 'CSV'], ['manual', 'つかいかた']]) {
          await W.page.goto(BASE + url);
          if (name === '判断') await settle(W.page); else await W.page.waitForLoadState('networkidle');
          const over = await W.page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
          assert.ok(over <= 1, `${w} 幅の${name}が横に ${over}px はみ出す`);
          if (name === '判断') {
            await W.page.locator('#rows .ckb').first().check();
            await W.page.locator('#bulk').evaluate((el) => Promise.all(el.getAnimations().map((a) => a.finished)));
            const b = await W.page.locator('#bulk').boundingBox();
            assert.ok(b && b.x >= 0 && b.x + b.width <= w + 1 && b.y + b.height <= 800 + 1, `${w} 幅で帯が画面の外`);
            await W.page.locator('#rows .ckb').first().uncheck();
          }
          if (w !== 1440 || name !== '判断') await shot(W.page, `${name === '判断' ? '01' : name === 'CSV' ? '06' : '07'}_${name}_${w}.png`);
        }
        assert.deepEqual(W.errors, []);
      } finally { await W.ctx.close(); }
    }
  });

  await ta('[10] 部品はこの口から: 404 が無い・全体から探す (Ctrl+K) が開いて Esc で閉じる・JS の誤りが無い', async () => {
    await page.goto(BASE);
    await settle(page);
    await page.keyboard.press('Control+k');
    await page.waitForSelector('#palette-bg.on');
    await page.keyboard.press('Escape');
    assert.equal(await page.isHidden('#palette-bg'), true);
    assert.deepEqual(notFound, []);
    assert.deepEqual(P.errors, []);
  });
} finally {
  if (P) await P.ctx.close().catch(() => {});
  await browser.close();
  server.close();
  await pg.close();
}
console.log(`\n${passed} 件 ${process.exitCode ? 'ok (NG あり)' : 'PASS'}`);
