/**
 * Yahoo!ストアクーポン 定期入れ替え (mall-csv-fetcher P3-Y)
 *
 * 何をするか:
 *   ストア全品「N点以上購入でX円割引」クーポン3本について、期限が近づいたら
 *   ①一覧から現行クーポンを見つけ ②「コピー」で新規発行フォームを開き
 *   ③金額を巡回リストの次の値にずらし ④次の掲載期間を入れて発行する。
 *
 * なぜ必要か (実測 2026-07-31):
 *   - Yahoo!ショッピングにクーポン発行APIは存在しない (公開API一覧にクーポンなし)
 *   - **公開後のクーポンは編集・削除ができない** → 期限ごとに作り直すしかない
 *   - 同額を延々と続けると恒常値引きになるため、金額を±5円で巡回させる (中原さんの運用)
 *
 * 安全設計:
 *   - 既定は dry-run (確認画面まで進んで内容を検証し、発行はしない)。--live で実発行
 *   - 遷移先URLを /pro.<store>/coupon/ 配下に限定 (カート設定権限で他画面を触らない)
 *   - 二重作成防止: 同じ開始日のクーポンが既にあればスキップ
 *   - Yahoo制約 (公開90日以内・開始は登録日から60日以内) を発行前に検証
 *   - 発行前にフォームの読み戻し検証 (意図した金額・期間・個数が入っているか)
 *
 * 実行:
 *   node scripts/mall-csv-fetcher/yahoo-coupon-rotate.mjs            # dry-run
 *   node scripts/mall-csv-fetcher/yahoo-coupon-rotate.mjs --live     # 実発行
 *   env: YCOUPON_IMAGE=xxx.jpg (画像ファイル名を全件上書き) / YCOUPON_PERIOD_DAYS=89
 *        YCOUPON_ONLY=qty2,qty3 (対象を絞る) / YCOUPON_DRIVE_DIR=gdrive:yahoo-coupon-captures
 */

import { mkdir, readdir } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openYahooContext, gotoStorePage, STORE_TOP_URL, STORE_ACCOUNT } from './lib-yahoo-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';
import { planCoupon, todayJst, parseCouponTitle } from './yahoo-coupon-lib.mjs';

const execFileP = promisify(execFile);
const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output', 'yahoo-coupon-rotate');

const LIVE = process.argv.includes('--live');
const PERIOD_DAYS = Math.max(1, Math.min(90, parseInt(process.env.YCOUPON_PERIOD_DAYS, 10) || 89));
const IMAGE_OVERRIDE = (process.env.YCOUPON_IMAGE || '').trim();
const ONLY = (process.env.YCOUPON_ONLY || '').split(',').map((s) => s.trim()).filter(Boolean);
const DRIVE_DIR = (process.env.YCOUPON_DRIVE_DIR || 'gdrive:yahoo-coupon-captures').trim();

/**
 * 対象クーポンの定義。
 *
 * cycle = 金額を2値で往復させる (中原さん決定 2026-07-31)。
 *   同額を続けると恒常値引きになるため金額を振る。一方でクーポン画像には金額が
 *   焼き込まれているため、**金額2種と画像2種を1対1で持ち交互に使う**ことで
 *   画像・説明文・実額が常に一致する。
 *   「今の金額の次」を選ぶ方式なので状態ファイル不要 (冪等)。
 *
 * images = 金額→画像ファイル名。**未登録の金額では発行しない** (ズレを構造的に防ぐ)。
 *   coupon50/100/150.jpg は登録済み。55/110/155円ぶんは画像を作って
 *   ストアエディタにアップロードすれば、ファイル名を合わせるだけで有効になる。
 */
const COUPON_DEFS = [
  {
    key: 'qty2', orderCount: 2, cycle: [55, 50],
    images: { 50: 'coupon50.jpg', 55: 'coupon55.jpg' },
    titleTemplate: '{orderCount}点以上購入で{amount}円割引',
    descriptionTemplate: '当店で{orderCount}点以上ご購入された場合、{amount}円引きになるクーポンです。',
  },
  {
    key: 'qty3', orderCount: 3, cycle: [110, 100],
    images: { 100: 'coupon100.jpg', 110: 'coupon110.jpg' },
    titleTemplate: '{orderCount}点以上購入で{amount}円割引',
    descriptionTemplate: '当店で{orderCount}点以上ご購入された場合、{amount}円引きになるクーポンです。',
  },
  {
    key: 'qty4', orderCount: 4, cycle: [155, 150],
    images: { 150: 'coupon150.jpg', 155: 'coupon155.jpg' },
    titleTemplate: '{orderCount}点以上購入で{amount}円割引',
    descriptionTemplate: '当店で{orderCount}点以上ご購入された場合、{amount}円引きになるクーポンです。',
  },
];

const COUPON_PATH_PREFIX = `/pro.${STORE_ACCOUNT}/coupon/`;
const START_HOUR = '000000'; // 00:00
const END_HOUR = '230000';   // 23:00 (現行運用と同じ)

/** 遷移先がクーポン画面配下であることを強制する (他の管理画面を絶対に触らない) */
function assertCouponUrl(url, label) {
  let u;
  try { u = new URL(url); } catch { throw new Error(`URL_GUARD: ${label} のURLを解釈できない (${url})`); }
  if (u.hostname !== 'pro.store.yahoo.co.jp') throw new Error(`URL_GUARD: ${label} が想定外のホスト (${u.hostname})`);
  if (!u.pathname.startsWith(COUPON_PATH_PREFIX)) {
    throw new Error(`URL_GUARD: ${label} がクーポン画面の外 (${u.pathname})`);
  }
}

async function snap(page, label) {
  const path = join(OUT_DIR, `${label}.png`);
  await page.screenshot({ path, fullPage: true }).catch(() => {});
  console.log(`  [snap] ${label}`);
  return path;
}

// ─────────────────────────── 一覧の取得 ───────────────────────────

/** クーポン一覧を100件表示で開き、行を構造化して返す */
async function fetchCouponRows(page) {
  await gotoStorePage(page, `${STORE_TOP_URL}/coupon/index`, 'クーポン一覧');
  assertCouponUrl(page.url(), 'クーポン一覧');

  const rows = await page.evaluate(() => {
    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const out = [];
    for (const tr of document.querySelectorAll('tr')) {
      const tds = [...tr.querySelectorAll('td')];
      if (tds.length < 12) continue;
      const cells = tds.map((td) => norm(td.innerText));
      // 1列目がクーポンID (32文字前後の英数) の行だけを拾う
      if (!/^[A-Za-z0-9]{20,}$/.test(cells[0])) continue;
      const dt = (s) => {
        const m = norm(s).match(/^(\d{4}\/\d{2}\/\d{2})\s+(\d{2}:\d{2})$/);
        return m ? { ymd: m[1], hm: m[2] } : { ymd: '', hm: '' };
      };
      out.push({
        couponId: cells[0],
        title: cells[1],
        kind: cells[3],
        discount: cells[4],
        gained: cells[5],
        used: cells[6],
        pubStartYmd: dt(cells[7]).ymd,
        pubEndYmd: dt(cells[8]).ymd,
        useStartYmd: dt(cells[9]).ymd,
        useEndYmd: dt(cells[10]).ymd,
        state: cells[11],
      });
    }
    return out;
  });
  console.log(`[list] クーポン ${rows.length} 件を取得`);
  return rows;
}

// ─────────────────────────── フォーム操作 ───────────────────────────

/**
 * 日付欄はdatepicker実装のことがあるため、fill → 読み戻し → 失敗ならJSネイティブsetterで再試行。
 * (楽天datatoolで確立した手法)
 */
async function setDateField(page, name, value) {
  const sel = `input[name="${name}"]`;
  await page.fill(sel, value, { timeout: 10000 }).catch(() => {});
  let got = await page.inputValue(sel).catch(() => '');
  if (got !== value) {
    await page.evaluate(({ n, v }) => {
      const el = document.querySelector(`input[name="${n}"]`);
      if (!el) return;
      const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
      setter.call(el, v);
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }, { n: name, v: value });
    got = await page.inputValue(sel).catch(() => '');
  }
  if (got !== value) throw new Error(`FORM_SET: ${name} に ${value} を設定できず (実際=${got})`);
}

async function setTextField(page, name, value) {
  const sel = `[name="${name}"]`;
  await page.fill(sel, String(value), { timeout: 10000 });
  const got = await page.inputValue(sel).catch(() => '');
  if (got !== String(value)) throw new Error(`FORM_SET: ${name} に ${value} を設定できず (実際=${got})`);
}

/** コピー画面を開く (一覧の「コピー」ボタンと同じ POST) */
async function openCopyForm(page, couponId) {
  const action = `${COUPON_PATH_PREFIX}index/new-edit/${couponId}`;
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {}),
    page.evaluate((act) => {
      const f = document.f;
      f.action = act;
      if (typeof doSubmit === 'function') doSubmit(f); else f.submit();
    }, action),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(800);
  assertCouponUrl(page.url(), 'コピー画面');
  if (!page.url().includes('/new-edit/')) {
    throw new Error(`NAV: コピー画面に遷移できていない (${page.url()})`);
  }
}

/** 計画どおりにフォームを書き換える */
async function applyPlan(page, plan) {
  await setTextField(page, 'Title', plan.title);
  await setTextField(page, 'Description', plan.description);
  await setTextField(page, 'DiscountPrice', String(plan.amount));
  const image = IMAGE_OVERRIDE || plan.image;
  if (image) await setTextField(page, 'Image', image);

  await setDateField(page, 'publish_start_date', plan.period.startYmd);
  await setDateField(page, 'publish_end_date', plan.period.endYmd);
  await setDateField(page, 'start_date', plan.period.startYmd);
  await setDateField(page, 'end_date', plan.period.endYmd);
  await page.selectOption('select[name="PublishStartHour"]', START_HOUR);
  await page.selectOption('select[name="PublishEndHour"]', END_HOUR);
  await page.selectOption('select[name="StartHour"]', START_HOUR);
  await page.selectOption('select[name="EndHour"]', END_HOUR);
}

/** 発行前の読み戻し検証。1つでも食い違ったら発行しない */
async function verifyForm(page, plan) {
  const got = await page.evaluate(() => {
    const v = (n) => {
      const el = document.querySelector(`[name="${n}"]`);
      return el ? String(el.value || '') : null;
    };
    const checked = (n) => {
      const el = document.querySelector(`[name="${n}"]:checked`);
      return el ? el.value : null;
    };
    return {
      Title: v('Title'), Description: v('Description'), DiscountPrice: v('DiscountPrice'),
      Image: v('Image'), OrderCount: v('OrderCount'),
      publish_start_date: v('publish_start_date'), publish_end_date: v('publish_end_date'),
      start_date: v('start_date'), end_date: v('end_date'),
      PublishStartHour: v('PublishStartHour'), PublishEndHour: v('PublishEndHour'),
      StartHour: v('StartHour'), EndHour: v('EndHour'),
      DiscountType: checked('DiscountType'), OrderType: checked('OrderType'),
      ItemDesignation: checked('ItemDesignation'), DispFlg: checked('DispFlg'),
      Combine: checked('Combine'), nDayFlg: checked('nDayFlg'),
    };
  });
  const want = {
    Title: plan.title,
    Description: plan.description,
    DiscountPrice: String(plan.amount),
    OrderCount: String(plan.orderCount),
    publish_start_date: plan.period.startYmd,
    publish_end_date: plan.period.endYmd,
    start_date: plan.period.startYmd,
    end_date: plan.period.endYmd,
    PublishStartHour: START_HOUR, PublishEndHour: END_HOUR,
    StartHour: START_HOUR, EndHour: END_HOUR,
    DiscountType: '1',      // 定額値引き
    OrderType: '2',         // 注文個数条件
    ItemDesignation: '3',   // ストア内全商品
    DispFlg: '1',           // 公開範囲=表示
    nDayFlg: '0',           // 期間指定 (獲得日起点ではない)
  };
  const diffs = [];
  for (const [k, w] of Object.entries(want)) {
    if (got[k] !== w) diffs.push(`${k}: 期待"${w}" 実際"${got[k]}"`);
  }
  if (diffs.length) throw new Error(`FORM_VERIFY: 入力内容が一致しない → ${diffs.join(' / ')}`);
  return got;
}

/** 「発行確認へ」= new-confirm へ送信。確認画面が出るだけで、まだ発行されない */
async function submitToConfirm(page) {
  const action = `${COUPON_PATH_PREFIX}index/new-confirm`;
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {}),
    page.evaluate((act) => {
      const f = document.f;
      f.action = act;
      if (typeof doSubmit === 'function') doSubmit(f); else f.submit();
    }, action),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(800);
  assertCouponUrl(page.url(), '確認画面');
  const body = await page.locator('body').innerText().catch(() => '');
  const compact = body.replace(/\s+/g, ' ');
  if (/エラー|入力してください|正しく入力|必須/.test(compact) && !/確認/.test(compact)) {
    throw new Error(`CONFIRM: 確認画面でエラー表示 → ${compact.slice(0, 300)}`);
  }
  return compact;
}

/**
 * 確認画面でクーポン画像が実際に読めているかを検証する。
 * 存在しないファイル名を指定すると画像が出ない → 「金額と画像がズレたクーポン」を
 * 発行してしまう事故を、発行前に止める。
 */
async function verifyConfirmImage(page) {
  return page.evaluate(() => {
    for (const tr of document.querySelectorAll('tr')) {
      const label = (tr.querySelector('th, td') || {}).innerText || '';
      if (!/クーポン画像/.test(label)) continue;
      const img = tr.querySelector('img');
      if (!img) return { ok: false, reason: '画像が表示されていない (ファイル名がストアエディタに存在しない可能性)' };
      const ok = !!(img.complete && img.naturalWidth > 0);
      return { ok, reason: `src=${img.src} 実サイズ=${img.naturalWidth}x${img.naturalHeight}`, src: img.src };
    }
    return { ok: false, reason: '確認画面にクーポン画像の行が見つからない' };
  }).catch((e) => ({ ok: false, reason: `検証失敗: ${e.message}` }));
}

/** 確認画面のフォーム/ボタン構造 (発行ボタン特定用に毎回ダンプして記録) */
async function dumpConfirmControls(page) {
  return page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    return {
      forms: [...document.querySelectorAll('form')].map((f) => ({ name: f.name || '', action: f.action || '', method: (f.method || '').toUpperCase() })),
      buttons: [...document.querySelectorAll('button, input[type=submit], input[type=button], a')]
        .filter((el) => vis(el))
        .map((el) => ({
          text: (el.innerText || el.value || '').replace(/\s+/g, ' ').trim().slice(0, 30),
          tag: el.tagName.toLowerCase(), type: el.type || '',
          onclick: (el.getAttribute('onclick') || '').slice(0, 200),
          href: el.getAttribute('href') || '',
        }))
        .filter((b) => b.text && /発行|登録|確定|OK|戻/.test(b.text)),
    };
  }).catch(() => ({ forms: [], buttons: [] }));
}

// ─────────────────────────── Drive アップロード (fail-soft) ───────────────────────────

/** スクショをDriveへ上げて共有リンクを返す。失敗しても通知本体は止めない */
async function uploadCapture(localPath, remoteName) {
  try {
    await execFileP('rclone', ['copyto', localPath, `${DRIVE_DIR}/${remoteName}`, '--drive-chunk-size', '8M'], { timeout: 120000 });
    const { stdout } = await execFileP('rclone', ['link', `${DRIVE_DIR}/${remoteName}`], { timeout: 60000 });
    const url = String(stdout || '').trim().split(/\s+/).pop();
    return /^https?:\/\//.test(url) ? url : null;
  } catch (e) {
    console.warn(`  ⚠ [capture] Driveアップロード失敗 (通知は継続): ${String(e.message).split('\n')[0]}`);
    return null;
  }
}

// ─────────────────────────── メイン ───────────────────────────

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const runLog = initRunLog('yahoo-coupon');
  console.log(`=== Yahoo!ストアクーポン 定期入れ替え (${LIVE ? '★LIVE=実発行' : 'dry-run'}) ===`);
  console.log(`期間=開始日+${PERIOD_DAYS}日 / 画像上書き=${IMAGE_OVERRIDE || '(なし)'}`);

  const ctx = await openYahooContext();
  const page = ctx.pages()[0] || (await ctx.newPage());
  // 発行ボタンが confirm ダイアログを出す実装のため、ハンドラが無いと Playwright は
  // 既定で dismiss (キャンセル) してしまい「押したのに発行されない」になる。
  // dry-run では発行ボタンを押さないので、ここに来るのは LIVE のときだけ。
  page.on('dialog', async (d) => {
    console.log(`  [dialog] ${d.type()}: ${d.message()}`);
    await d.accept().catch(() => {});
  });
  const results = [];
  const captures = [];

  try {
    const rows = await fetchCouponRows(page);
    const todayYmd = todayJst();

    const defs = ONLY.length ? COUPON_DEFS.filter((d) => ONLY.includes(d.key)) : COUPON_DEFS;
    const plans = defs.map((def) => planCoupon({ def, rows, periodDays: PERIOD_DAYS, todayYmd }));

    console.log('\n--- 計画 ---');
    for (const p of plans) {
      if (p.status === 'ready') {
        console.log(`  [${p.key}] ${p.source.title} (${p.source.amount}円, 〜${p.source.useEndYmd})`);
        console.log(`         → ${p.title} / ${p.amount}円 / ${p.period.startYmd} 00:00 〜 ${p.period.endYmd} 23:00`);
      } else {
        console.log(`  [${p.key}] ${p.status}: ${p.reason}`);
      }
    }

    for (const plan of plans) {
      if (plan.status !== 'ready') { results.push(plan); continue; }
      console.log(`\n### ${plan.key}: ${plan.title}`);
      try {
        // 一覧に戻ってからコピー (document.f が必要なため)
        await fetchCouponRows(page);
        await openCopyForm(page, plan.source.couponId);
        await applyPlan(page, plan);
        const verified = await verifyForm(page, plan);
        console.log('  [verify] 入力内容OK');
        captures.push({ key: plan.key, label: 'form', path: await snap(page, `${plan.key}_1_form`) });

        await submitToConfirm(page);
        console.log(`  [confirm] 確認画面 url=${page.url()}`);
        const img = await verifyConfirmImage(page);
        if (!img.ok) throw new Error(`CONFIRM_IMAGE: クーポン画像を確認できず → ${img.reason}`);
        console.log(`  [confirm] 画像OK (${img.reason})`);
        const controls = await dumpConfirmControls(page);
        console.log(`  [confirm] forms=${JSON.stringify(controls.forms)}`);
        console.log(`  [confirm] buttons=${JSON.stringify(controls.buttons)}`);
        captures.push({ key: plan.key, label: 'confirm', path: await snap(page, `${plan.key}_2_confirm`) });

        if (!LIVE) {
          results.push({ ...plan, status: 'dry-ok', verified, controls });
          console.log('  ✅ dry-run: 確認画面まで到達 (発行はしていません)');
          continue;
        }

        // ── LIVE: 確認画面の「発行」を押す ──
        const issued = await issueFromConfirm(page, controls);
        captures.push({ key: plan.key, label: 'done', path: await snap(page, `${plan.key}_3_done`) });
        results.push({ ...plan, status: 'issued', issued });
        console.log('  ✅ 発行しました');
      } catch (e) {
        await snap(page, `${plan.key}_error`);
        console.error(`  ❌ ${plan.key}: ${e.message}`);
        results.push({ ...plan, status: 'failed', error: e.message });
      }
    }

    // 発行後の実在確認 (LIVE時)
    if (LIVE && results.some((r) => r.status === 'issued')) {
      const after = await fetchCouponRows(page);
      for (const r of results.filter((x) => x.status === 'issued')) {
        const hit = after.find((row) => row.title === r.title && row.useStartYmd === r.period.startYmd);
        r.verifiedInList = !!hit;
        r.newCouponId = hit ? hit.couponId : null;
        console.log(`  [after] ${r.title}: ${hit ? `一覧に存在 (${hit.couponId})` : '⚠️一覧で確認できず'}`);
      }
      captures.push({ key: 'list', label: 'after', path: await snap(page, 'list_after') });
    }

    await notify(results, captures);
    const failed = results.filter((r) => r.status === 'failed' || r.status === 'error');
    process.exitCode = failed.length ? 1 : 0;
  } catch (e) {
    const is2fa = String(e.message).startsWith('2FA_REQUIRED');
    console.error('[FATAL]', e.message);
    const shot = await snap(page, 'fatal_error');
    await sendGChat(buildErrorReport({
      mall: 'yahoo-coupon',
      logPath: runLog?.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (クーポン入れ替え中止)' : 'クーポン入れ替え (共通処理)',
        error: e.message,
        url: page.url(),
        screenshot: shot,
      }],
      repro: is2fa
        ? 'ミニPCの画面で デスクトップの Yahoo-Relogin.bat をダブルクリック → Yahoo! JAPAN ID で再ログイン (確認コードはメール)'
        : 'node scripts/mall-csv-fetcher/yahoo-coupon-rotate.mjs (dry-run) で再現確認。403なら「カート設定権限」が外れていないか確認',
    }), 'yahoo-coupon').catch(() => {});
    process.exitCode = is2fa ? 3 : 1; // 3=手動対応必須
  } finally {
    await ctx.close().catch(() => {});
  }
}

/**
 * 確認画面から発行を確定する。
 * 実測 (2026-07-31): 発行ボタンは <a>発行</a> で onclick属性を持たない (JSリスナー)。
 * CSSセレクタ (:text-is) では子要素構造の都合で掴めなかったため、
 * DOM走査で innerText が「発行」ちょうどのリンクを特定して click する
 * (ボタン一覧ダンプと同じ探索ロジック = 見えているものを確実に押す)。
 */
async function issueFromConfirm(page, controls) {
  // どんな要素なのかを毎回記録しておく (仕様変更時の調査用)
  const dbg = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    return [...document.querySelectorAll('a, button, input')]
      .filter(vis)
      .filter((el) => (el.innerText || el.value || '').replace(/\s+/g, '') === '発行')
      .map((el) => ({ outer: el.outerHTML.slice(0, 300), parent: el.parentElement ? el.parentElement.outerHTML.slice(0, 300) : '' }));
  }).catch(() => []);
  console.log(`  [debug] 発行ボタン: ${JSON.stringify(dbg)}`);

  // 実測: 発行は <div class="btnBlL update"><a href="javascript:void(0)"><span>発行</span></a></div>。
  // ①DOMの el.click() では発火しない ②CSSセレクタだと画面外の同名要素まで拾って多重ヒットする
  // → 「可視かつテキストが発行ちょうど」の要素をDOM走査で一意に決め、印を付けてから
  //   Playwright のネイティブクリック (実マウスイベント) で押す。
  const tagged = await page.evaluate(() => {
    const MARK = 'data-coupon-issue-btn';
    for (const el of document.querySelectorAll(`[${MARK}]`)) el.removeAttribute(MARK);
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const els = [...document.querySelectorAll('a, button, input[type=submit], input[type=button]')]
      .filter(vis)
      .filter((el) => (el.innerText || el.value || '').replace(/\s+/g, '') === '発行');
    if (els.length !== 1) return { ok: false, count: els.length };
    els[0].setAttribute(MARK, '1');
    return { ok: true, count: 1 };
  });
  if (!tagged.ok) {
    throw new Error(`ISSUE: 発行ボタンを一意に特定できず (可視${tagged.count}件、候補=${JSON.stringify(controls.buttons)})`);
  }
  await page.locator('[data-coupon-issue-btn="1"]').click({ timeout: 15000 });

  // 実測: 1回目のクリックでは発行されず、HTMLモーダル「ご注意ください
  // (クーポンの利用開始日時以降は一切編集・削除ができません)」が開く。
  // その中の「発行」を押して初めて確定する = 2段階。
  await page.waitForTimeout(1200);
  const modal = await page.evaluate(() => {
    const MARK = 'data-coupon-modal-btn';
    for (const el of document.querySelectorAll(`[${MARK}]`)) el.removeAttribute(MARK);
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const dialogs = [...document.querySelectorAll('div')].filter((d) => {
      const t = d.innerText || '';
      return vis(d) && /ご注意ください/.test(t) && /キャンセル/.test(t);
    });
    if (!dialogs.length) return { ok: false, reason: 'モーダルが出ていない' };
    // 入れ子の最も内側 (ダイアログ本体) を選ぶ
    const inner = dialogs.reduce((a, b) => (a.contains(b) ? b : a));
    const btns = [...inner.querySelectorAll('a, button, input[type=submit], input[type=button]')]
      .filter(vis)
      .filter((el) => (el.innerText || el.value || '').replace(/\s+/g, '') === '発行');
    if (btns.length !== 1) return { ok: false, reason: `モーダル内の発行ボタンが${btns.length}件` };
    btns[0].setAttribute(MARK, '1');
    return { ok: true, text: (inner.innerText || '').replace(/\s+/g, ' ').slice(0, 160) };
  });
  if (!modal.ok) {
    throw new Error(`ISSUE: 確認モーダルの発行ボタンを特定できず (${modal.reason})`);
  }
  console.log(`  [modal] ${modal.text}`);
  await page.locator('[data-coupon-modal-btn="1"]').click({ timeout: 15000 });
  // クリック → (confirmダイアログ受諾) → 送信 → 完了画面、まで最大40秒待つ。
  // 「まだ発行は完了しておりません」が消えることを完了の肯定条件にする。
  const deadline = Date.now() + 40000;
  let compact = '';
  let done = false;
  while (Date.now() < deadline) {
    await page.waitForTimeout(1200);
    compact = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
    if (compact && !/まだ発行は完了して/.test(compact)) { done = true; break; }
  }
  await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
  assertCouponUrl(page.url(), '発行後');
  if (!done) {
    throw new Error('ISSUE: 発行ボタンを押したが確認画面のまま (発行されていない)');
  }
  return { url: page.url(), body: compact.slice(0, 300) };
}

/** GChat通知: 何をどう作ったかを人が読める形で + キャプチャのDriveリンク */
async function notify(results, captures) {
  const stamp = todayJst().replaceAll('/', '');
  const lines = [];
  lines.push(LIVE ? '*Yahoo!ストアクーポン 入れ替え完了*' : '*Yahoo!ストアクーポン 入れ替え (ドライラン)*');
  lines.push('');
  for (const r of results) {
    if (r.status === 'issued' || r.status === 'dry-ok') {
      lines.push(`*${r.title}*`);
      lines.push(`　値引き: *${r.amount}円* (前回 ${r.source.amount}円)`);
      lines.push(`　条件: ${r.orderCount}点以上・ストア全品`);
      lines.push(`　期間: ${r.period.startYmd} 00:00 〜 ${r.period.endYmd} 23:00`);
      if (r.status === 'issued') {
        lines.push(`　結果: ${r.verifiedInList ? `✅発行済み (${r.newCouponId})` : '⚠️発行したが一覧で未確認'}`);
      } else {
        lines.push('　結果: ドライラン (発行していません)');
      }
      lines.push('');
    } else if (r.status === 'skip') {
      lines.push(`⏭ ${r.key}: ${r.reason}`);
      lines.push('');
    } else {
      lines.push(`❌ ${r.key}: ${r.reason || r.error}`);
      lines.push('');
    }
  }

  // キャプチャをDriveへ (fail-soft)
  const links = [];
  for (const c of captures) {
    const url = await uploadCapture(c.path, `${stamp}_${c.key}_${c.label}.png`);
    if (url) links.push(`${c.key}/${c.label}: ${url}`);
  }
  if (links.length) {
    lines.push('*画面キャプチャ*');
    for (const l of links) lines.push(`　${l}`);
  } else if (captures.length) {
    lines.push(`_キャプチャはミニPCに保存: ${OUT_DIR}_`);
  }

  await sendGChat(lines.join('\n'), 'yahoo-coupon').catch((e) => console.warn(`  ⚠ GChat通知失敗: ${e.message}`));
}

main();
