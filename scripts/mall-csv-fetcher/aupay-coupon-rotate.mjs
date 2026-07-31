/**
 * au PAYマーケット ストアクーポン 月次入れ替え (mall-csv-fetcher P3-A)
 *
 * 何をするか:
 *   「商品N点以上で X円引き」クーポン4本 (2/3/4/5点) について、毎月25日に翌月分を
 *   ①一覧から現行クーポンを見つけ ②「コピー」で作成フォームを開き
 *   ③金額を巡回リストの次の値にずらし ④翌月1日〜翌月末の期間を入れて作成する。
 *
 * なぜ必要か (実測 2026-07-31):
 *   - au PAY マーケットにクーポン発行APIは無い → Wow!manager の画面操作が唯一の手段
 *   - 公開後のクーポンは編集できない (詳細画面から押せるのは「配布停止」だけ)
 *   - 中原さんが毎月末に手作業で4本作り直していた
 *
 * 安全設計 (Yahoo版 yahoo-coupon-rotate.mjs と同型):
 *   - 既定は dry-run (確認画面まで進んで内容を検証し、作成はしない)。--live で実作成
 *   - 遷移先URLを /wmshopclient/(shopCouponMgt|distributionCoupon)/ 配下に限定し、
 *     停止・削除系のパス (detailStop 等) へ入ろうとしたら即中止
 *   - 二重作成防止: 同じ利用開始日のクーポンが既にあればスキップ
 *   - 作成前にフォームの読み戻し検証 + 確認画面の表示内容の検証 (どちらか食い違えば作成しない)
 *   - クーポン名の金額と「割引種別」の実額が食い違う行はコピー元にしない
 *
 * 実行:
 *   node scripts/mall-csv-fetcher/aupay-coupon-rotate.mjs            # dry-run
 *   node scripts/mall-csv-fetcher/aupay-coupon-rotate.mjs --live     # 実作成
 *   env: ACOUPON_CREATE_DAY=25 (毎月何日に翌月分を作るか)
 *        ACOUPON_ONLY=qty2,qty3 (対象を絞る) / ACOUPON_DRIVE_DIR=gdrive:aupay-coupon-captures
 *        ACOUPON_NOTIFY=1 (dry-runでもGChat通知する。既定は --live のときだけ通知)
 */

import { mkdir } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openAupayContext, gotoWowmaPage, WOWMA_BASE } from './lib-aupay-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';
import {
  planCoupon, todayJst, parseCouponTitle, parseDiscountAmount, parseUsePeriod, findCreatedRow,
} from './aupay-coupon-lib.mjs';

const execFileP = promisify(execFile);
const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output', 'aupay-coupon-rotate');

const LIVE = process.argv.includes('--live');
// dry-run は動作確認で何度も回すので、既定では通知を出さない (本番実行は必ず通知する)
const NOTIFY = LIVE || process.env.ACOUPON_NOTIFY === '1';
const CREATE_DAY = Math.max(1, Math.min(28, parseInt(process.env.ACOUPON_CREATE_DAY, 10) || 25));
const ONLY = (process.env.ACOUPON_ONLY || '').split(',').map((s) => s.trim()).filter(Boolean);
const DRIVE_DIR = (process.env.ACOUPON_DRIVE_DIR || 'gdrive:aupay-coupon-captures').trim();

const LIST_URL = `${WOWMA_BASE}shopCouponMgt/index?couponType=1`;
const START_HOUR = '00'; // 分秒は画面が自動で 00 を入れる
const END_HOUR = '23';   // 分秒は画面が自動で 59 を入れる

// 画面の日付欄は jQuery UI datepicker の 'yy/mm/dd' (= 2026/08/01)。
// 一覧の表示や lib の内部表現はハイフン (2026-08-01) なので、入力時だけ変換する。
const DATE_FORMAT = 'yy/mm/dd';
const toSlash = (ymd) => String(ymd).replaceAll('-', '/');

/**
 * 対象クーポンの定義。
 *
 * cycle = 金額を2値で往復させる (中原さん決定 2026-07-31)。同額を続けると恒常値引きに
 *   なるため金額を振る。「今の金額の次」を選ぶ方式なので状態ファイル不要 (冪等)。
 *   Yahoo版と違い au PAY はクーポン画像を使っていない ("画像登録なし") ので、
 *   画像と金額のズレを心配する必要がない。
 */
const COUPON_DEFS = [
  { key: 'qty2', orderCount: 2, cycle: [30, 33] },
  { key: 'qty3', orderCount: 3, cycle: [50, 55] },
  { key: 'qty4', orderCount: 4, cycle: [100, 110] },
  { key: 'qty5', orderCount: 5, cycle: [150, 155] },
].map((d) => ({
  ...d,
  titleTemplate: 'ZACCA IZMで使える！商品{orderCount}点以上で{amount}円引きクーポン！',
  descriptionTemplate: 'ZACCA IZMで使える商品{orderCount}点以上購入の方限定の{amount}円引きクーポンです。',
}));

// クーポン関連画面のみ。停止・削除系のパスは踏まない
const COUPON_PATH_RE = /^\/wmshopclient\/(shopCouponMgt|distributionCoupon)\//;
const FORBIDDEN_PATH_RE = /stop|delete|remove/i;

function assertCouponUrl(url, label) {
  let u;
  try { u = new URL(url); } catch { throw new Error(`URL_GUARD: ${label} のURLを解釈できない (${url})`); }
  if (u.hostname !== 'manager.wowma.jp') throw new Error(`URL_GUARD: ${label} が想定外のホスト (${u.hostname})`);
  if (!COUPON_PATH_RE.test(u.pathname)) throw new Error(`URL_GUARD: ${label} がクーポン画面の外 (${u.pathname})`);
  if (FORBIDDEN_PATH_RE.test(u.pathname)) throw new Error(`URL_GUARD: ${label} が停止・削除系の画面 (${u.pathname})`);
}

async function snap(page, label) {
  const path = join(OUT_DIR, `${label}.png`);
  await page.screenshot({ path, fullPage: true }).catch(() => {});
  console.log(`  [snap] ${label}`);
  return path;
}

// ─────────────────────────── 一覧の取得 ───────────────────────────

/** 配布型クーポン一覧を開き、行を構造化して返す */
async function fetchCouponRows(page) {
  await gotoWowmaPage(page, LIST_URL, 'クーポン一覧');
  assertCouponUrl(page.url(), 'クーポン一覧');

  const raw = await page.evaluate(() => {
    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const table = [...document.querySelectorAll('table')]
      .find((tb) => /クーポンキー/.test(tb.innerText) && /利用期間/.test(tb.innerText));
    if (!table) return null;
    const out = [];
    for (const tr of table.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll('td')].map((td) => norm(td.innerText));
      if (cells.length < 12) continue;
      let couponId = null;
      for (const a of tr.querySelectorAll('a')) {
        const m = (a.getAttribute('href') || '').match(/\/distributionCoupon\/(?:detail|copyCreate)\/(\d+)/);
        if (m) { couponId = m[1]; break; }
      }
      // 列: 種類/クーポンキー/画像/クーポン名/ロック/広告掲載/付与・配布状態/利用状態/利用期間/利用条件/割引種別/...
      out.push({
        couponId,
        couponKey: cells[1],
        title: cells[3],
        distributionState: cells[6],
        useState: cells[7],
        usePeriodText: cells[8],
        discountText: cells[10],
      });
    }
    return out;
  });
  if (!raw) throw new Error('LIST_PARSE: クーポン一覧の表を見つけられない (画面仕様変更の疑い)');

  const rows = raw.map((r) => {
    const period = parseUsePeriod(r.usePeriodText) || { startYmd: '', endYmd: '' };
    return {
      ...r,
      parsed: parseCouponTitle(r.title),
      discountAmount: parseDiscountAmount(r.discountText),
      useStartYmd: period.startYmd,
      useEndYmd: period.endYmd,
    };
  });
  console.log(`[list] クーポン ${rows.length} 件 (うち「N点以上でX円引き」= ${rows.filter((r) => r.parsed).length} 件)`);
  return rows;
}

// ─────────────────────────── フォーム操作 ───────────────────────────

/**
 * 日付欄は readonly + jQuery UI datepicker のため fill が効かない。
 * ネイティブsetterで値を入れ、input/change を発火させる (楽天datatoolで確立した手法)。
 */
async function setDateField(page, name, value) {
  const sel = `input[name="${name}"]`;
  await page.evaluate(({ n, v }) => {
    const el = document.querySelector(`input[name="${n}"]`);
    if (!el) return;
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(el, v);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }, { n: name, v: value });
  const got = await page.inputValue(sel).catch(() => '');
  if (got !== value) throw new Error(`FORM_SET: ${name} に ${value} を設定できず (実際=${got})`);
}

async function setTextField(page, name, value) {
  const sel = `[name="${name}"]`;
  await page.fill(sel, String(value), { timeout: 10000 });
  const got = await page.inputValue(sel).catch(() => '');
  if (got !== String(value)) throw new Error(`FORM_SET: ${name} に ${value} を設定できず (実際=${got})`);
}

/**
 * ラジオは CSS で本体を隠してラベル側を見せる実装 (class=wm-radio)。
 * ラベルをクリックして、表示切替のJS (期間欄の開閉など) も一緒に走らせる。
 */
async function selectRadio(page, name, value) {
  const id = await page.evaluate(({ n, v }) => {
    const el = [...document.querySelectorAll(`input[name="${n}"]`)].find((x) => x.value === v);
    return el ? el.id : null;
  }, { n: name, v: value });
  if (!id) throw new Error(`FORM_SET: ラジオ ${name}=${value} が見つからない`);
  const label = page.locator(`label[for="${id}"]`);
  if (await label.count()) {
    await label.first().click({ timeout: 10000 });
  } else {
    await page.locator(`#${id}`).check({ timeout: 10000, force: true });
  }
  const checked = await page.evaluate((i) => !!document.getElementById(i)?.checked, id);
  if (!checked) throw new Error(`FORM_SET: ラジオ ${name}=${value} を選択できず`);
}

/** コピー作成画面を開く (一覧の「コピー」と同じ素のURL) */
async function openCopyForm(page, couponId) {
  if (!/^\d+$/.test(String(couponId))) throw new Error(`NAV: couponId が不正 (${couponId})`);
  const url = `${WOWMA_BASE}distributionCoupon/copyCreate/${couponId}`;
  await gotoWowmaPage(page, url, 'コピー作成画面');
  assertCouponUrl(page.url(), 'コピー作成画面');
  if (!page.url().includes('/copyCreate/')) throw new Error(`NAV: コピー作成画面に遷移できていない (${page.url()})`);
  // 日付欄の書式は datepicker の設定に従う。想定と違えば早期に止める
  const fmt = await page.evaluate(() => {
    try { return window.jQuery ? jQuery('#termDistributionTermStartDay_ctl').datepicker('option', 'dateFormat') : null; } catch { return null; }
  });
  if (fmt && fmt !== DATE_FORMAT) {
    throw new Error(`FORM_FORMAT: 日付欄の書式が想定外 (datepicker dateFormat=${fmt}、想定=${DATE_FORMAT})`);
  }
  console.log(`  [copy] ${url} (dateFormat=${fmt || '不明'})`);
}

/** 計画どおりにフォームを書き換える */
async function applyPlan(page, plan) {
  const p = plan.period;
  await setTextField(page, 'couponName', plan.title);
  await setTextField(page, 'couponExpln', plan.description);

  await selectRadio(page, 'dscntType', '0');            // 金額指定
  await setTextField(page, 'dscntTypeAmtDsgn', String(plan.amount));

  await selectRadio(page, 'distributionTerm', '0');     // 配布期間 = 期間指定
  await setDateField(page, 'termDistributionTermStartDay', toSlash(p.distStartYmd));
  await page.selectOption('select[name="termDistributionTermStartDayHour"]', START_HOUR);
  await setDateField(page, 'termDistributionTermEndDay', toSlash(p.distEndYmd));
  await page.selectOption('select[name="termDistributionTermDayHour"]', END_HOUR);

  await selectRadio(page, 'adDispctrl', '1');           // 広告面には表示させない (現行と同じ)

  await selectRadio(page, 'useTerm', '0');              // 利用期間 = 期間指定
  await setDateField(page, 'termDsgngrantTermStartDayDistribution', toSlash(p.useStartYmd));
  await page.selectOption('select[name="termDsgngrantTermStartDayDistributionHour"]', START_HOUR);
  await setDateField(page, 'termDsgngrantTermEndDayDistribution', toSlash(p.useEndYmd));
  await page.selectOption('select[name="termDsgngrantTermEndDayDistributionHour"]', END_HOUR);

  await selectRadio(page, 'bulkByUse', '0');            // まとめ買い指定
  await setTextField(page, 'bulkDsgnByUse', String(plan.orderCount));
}

/** 作成前の読み戻し検証。1つでも食い違ったら送信しない */
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
      couponName: v('couponName'), couponExpln: v('couponExpln'),
      dscntTypeAmtDsgn: v('dscntTypeAmtDsgn'), bulkDsgnByUse: v('bulkDsgnByUse'),
      termDistributionTermStartDay: v('termDistributionTermStartDay'),
      termDistributionTermEndDay: v('termDistributionTermEndDay'),
      termDsgngrantTermStartDayDistribution: v('termDsgngrantTermStartDayDistribution'),
      termDsgngrantTermEndDayDistribution: v('termDsgngrantTermEndDayDistribution'),
      termDistributionTermStartDayHour: v('termDistributionTermStartDayHour'),
      termDistributionTermDayHour: v('termDistributionTermDayHour'),
      termDsgngrantTermStartDayDistributionHour: v('termDsgngrantTermStartDayDistributionHour'),
      termDsgngrantTermEndDayDistributionHour: v('termDsgngrantTermEndDayDistributionHour'),
      maxUseSu: v('maxUseSu'),
      dscntType: checked('dscntType'), distributionTerm: checked('distributionTerm'),
      useTerm: checked('useTerm'), adDispctrl: checked('adDispctrl'),
      bulkByUse: checked('bulkByUse'), buyAmtByUse: checked('buyAmtByUse'),
      buyProductByUse: checked('buyProductByUse'), distributionType: checked('distributionType'),
      acquireMethod: checked('acquireMethod'), maxDistributionSu: checked('maxDistributionSu'),
      auPayShow: checked('auPayShow'), couponAfterGetPage: checked('couponAfterGetPage'),
      promotionalAutomation: checked('promotionalAutomation'),
    };
  });
  const p = plan.period;
  const want = {
    couponName: plan.title,
    couponExpln: plan.description,
    dscntTypeAmtDsgn: String(plan.amount),
    bulkDsgnByUse: String(plan.orderCount),
    termDistributionTermStartDay: toSlash(p.distStartYmd),
    termDistributionTermEndDay: toSlash(p.distEndYmd),
    termDsgngrantTermStartDayDistribution: toSlash(p.useStartYmd),
    termDsgngrantTermEndDayDistribution: toSlash(p.useEndYmd),
    termDistributionTermStartDayHour: START_HOUR,
    termDistributionTermDayHour: END_HOUR,
    termDsgngrantTermStartDayDistributionHour: START_HOUR,
    termDsgngrantTermEndDayDistributionHour: END_HOUR,
    dscntType: '0',            // 金額指定
    distributionTerm: '0',     // 配布=期間指定
    useTerm: '0',              // 利用=期間指定
    adDispctrl: '1',           // 広告面に表示させない
    bulkByUse: '0',            // まとめ買い指定
    buyAmtByUse: '1',          // 購入金額は指定しない
    buyProductByUse: '5',      // 対象商品は指定しない (全商品)
    distributionType: '1',     // 全ユーザー
    acquireMethod: '1',        // URL
    maxDistributionSu: '1',    // 配布上限なし
    auPayShow: '1',            // au PAY マーケットに表示
    couponAfterGetPage: '1',   // 獲得後は店舗トップへ
    promotionalAutomation: '0',
  };
  const diffs = [];
  for (const [k, w] of Object.entries(want)) {
    if (got[k] !== w) diffs.push(`${k}: 期待"${w}" 実際"${got[k]}"`);
  }
  if (diffs.length) throw new Error(`FORM_VERIFY: 入力内容が一致しない → ${diffs.join(' / ')}`);
  return got;
}

/** 「作成内容を確認する」= confirm へ送信。確認画面が出るだけで、まだ作成されない */
async function submitToConfirm(page) {
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {}),
    page.locator('input[name="btnSave_bottom"]').click({ timeout: 15000 }),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(800);
  assertCouponUrl(page.url(), '確認画面');
  const compact = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
  if (!page.url().includes('/confirm')) {
    // 入力エラーがあると copyCreate 画面に留まってエラー文言が出る
    const err = (compact.match(/[^。]*(入力してください|正しく入力|選択してください|エラー)[^。]*/) || [''])[0];
    throw new Error(`CONFIRM: 確認画面に進めなかった (${page.url()}) ${err.slice(0, 200)}`);
  }
  return compact;
}

/**
 * 確認画面の表示内容を検証する。
 * フォームの読み戻し (verifyForm) はブラウザ側の値を見ているだけなので、
 * サーバーが受け取った内容を確認画面の表示で突き合わせる。
 */
function verifyConfirmText(compact, plan) {
  const p = plan.period;
  const flat = compact.replace(/\s/g, '');
  const missing = [];
  // 日付は画面によって 2026/08/01 と 2026-08-01 の両表記がありうるのでどちらかが出ていればよい
  const day = (ymd) => [ymd, toSlash(ymd)];
  const need = [
    [[plan.title.replace(/\s/g, '')], 'クーポン名'],
    [[`${plan.amount}円`], '割引額'],
    [day(p.distStartYmd), '配布開始日'],
    [day(p.distEndYmd), '配布終了日'],
    [day(p.useStartYmd), '利用開始日'],
    [day(p.useEndYmd), '利用終了日'],
  ];
  for (const [needles, label] of need) {
    if (!needles.some((n) => flat.includes(n))) missing.push(`${label}(${needles[0]})`);
  }
  if (missing.length) {
    throw new Error(`CONFIRM_VERIFY: 確認画面に想定の内容が出ていない → ${missing.join(', ')}`);
  }
}

/**
 * 確認画面の「項目名 → 表示値」を全部読む。
 * 何を作ろうとしているかをログに残し、主要項目は verifyConfirmSummary で突き合わせる。
 */
async function readConfirmSummary(page) {
  return page.evaluate(() => {
    const norm = (s) => (s || '').replace(/[ \t]+/g, ' ').trim();
    const out = {};
    // 表組み (tr の2セル) と定義リスト風 (dt/dd) の両方に対応する
    for (const tr of document.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll('th, td')];
      if (cells.length !== 2) continue;
      const k = norm(cells[0].innerText);
      if (k) out[k] = norm(cells[1].innerText).replace(/\n+/g, ' / ');
    }
    for (const dl of document.querySelectorAll('dl')) {
      const dts = [...dl.querySelectorAll('dt')];
      const dds = [...dl.querySelectorAll('dd')];
      for (let i = 0; i < Math.min(dts.length, dds.length); i++) {
        const k = norm(dts[i].innerText);
        if (k && !(k in out)) out[k] = norm(dds[i].innerText).replace(/\n+/g, ' / ');
      }
    }
    return out;
  }).catch(() => ({}));
}

/**
 * 確認画面の項目を1つずつ突き合わせる (サーバーが解釈した結果の検証)。
 * 期待値は現行クーポン (2026-07分) の設定と同じ。1つでも違えば登録しない。
 */
function verifyConfirmSummary(summary, plan) {
  const p = plan.period;
  const flat = (s) => String(s || '').replace(/\s/g, '');
  const checks = [
    ['クーポンの名前', (v) => flat(v) === flat(plan.title)],
    ['クーポン説明', (v) => flat(v) === flat(plan.description)],
    ['割引種別', (v) => flat(v).includes(`金額指定${plan.amount}円`)],
    ['配布期間', (v) => flat(v).includes(p.distStartYmd) && flat(v).includes(p.distEndYmd)
      && flat(v).includes('00:00') && flat(v).includes('23:59')],
    ['利用期間', (v) => flat(v).includes(p.useStartYmd) && flat(v).includes(p.useEndYmd)
      && flat(v).includes('00:00') && flat(v).includes('23:59')],
    ['まとめ買い', (v) => flat(v).includes(`${plan.orderCount}点以上`)],
    ['配布種別', (v) => flat(v) === '全ユーザー'],
    ['自動販促', (v) => flat(v) === '利用しない'],
    ['広告表示制御', (v) => flat(v) === '表示させない'],
    ['最大配布枚数', (v) => flat(v) === '無制限'],
    ['購入金額', (v) => flat(v) === '指定しない'],
    ['購入商品', (v) => flat(v) === '指定しない'],
    ['クーポン画像登録', (v) => flat(v) === '画像登録なし'],
  ];
  const bad = [];
  for (const [k, isOk] of checks) {
    if (!(k in summary)) { bad.push(`${k}: 項目が見つからない`); continue; }
    if (!isOk(summary[k])) bad.push(`${k}: "${summary[k]}"`);
  }
  if (bad.length) throw new Error(`CONFIRM_VERIFY: 確認画面の内容が想定と違う → ${bad.join(' / ')}`);
}

/** 確認画面のフォーム/ボタン構造 (作成ボタン特定用に毎回ダンプして記録) */
async function dumpConfirmControls(page) {
  return page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    return {
      forms: [...document.querySelectorAll('form')].map((f) => ({ name: f.name || '', action: f.action || '', method: (f.method || '').toUpperCase() })),
      buttons: [...document.querySelectorAll('button, input[type=submit], input[type=button], a')]
        .filter(vis)
        .map((el) => ({
          text: (el.innerText || el.value || '').replace(/\s+/g, ' ').trim().slice(0, 30),
          tag: el.tagName.toLowerCase(), type: el.type || '', name: el.name || '',
          onclick: (el.getAttribute('onclick') || '').slice(0, 200),
          href: el.getAttribute('href') || '',
        }))
        .filter((b) => b.text && /作成|登録|確定|戻|修正/.test(b.text)),
    };
  }).catch(() => ({ forms: [], buttons: [] }));
}

/**
 * 確認画面から作成を確定する。
 * 実測 (2026-07-31): 確認画面の form は action=/distributionCoupon/updateOrCreate、
 *   確定ボタンは input[type=submit] name="btnSave_update" value="クーポンを登録する"。
 *   同じ画面に「作成内容を修正する」(name=btnBack_confirm) もあるので name で厳密に指定する。
 */
const CREATE_BUTTON_NAME = 'btnSave_update';
const CREATE_FORM_ACTION_RE = /\/wmshopclient\/distributionCoupon\/updateOrCreate$/;

async function createFromConfirm(page, controls) {
  const form = controls.forms.find((f) => CREATE_FORM_ACTION_RE.test(f.action || ''));
  if (!form) {
    throw new Error(`CREATE: 確認画面のフォームが想定外 (${JSON.stringify(controls.forms)})`);
  }
  const btn = page.locator(`input[name="${CREATE_BUTTON_NAME}"]`);
  const n = await btn.count();
  if (n !== 1) throw new Error(`CREATE: 登録ボタン (${CREATE_BUTTON_NAME}) が${n}件 (候補=${JSON.stringify(controls.buttons)})`);
  const label = (await btn.inputValue().catch(() => '')) || '';
  if (!/登録|作成/.test(label)) throw new Error(`CREATE: 登録ボタンの文言が想定外 ("${label}")`);

  console.log(`  [create] ボタン="${label}" を押します`);
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {}),
    btn.click({ timeout: 15000 }),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(1500);
  assertCouponUrl(page.url(), '作成後');
  const compact = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
  if (/confirm/.test(page.url()) && !/完了|受け付け/.test(compact)) {
    throw new Error(`CREATE: 作成ボタンを押したが確認画面のまま (${compact.slice(0, 200)})`);
  }
  return { url: page.url(), body: compact.slice(0, 300) };
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
  const runLog = initRunLog('aupay-coupon');
  console.log(`=== au PAY ストアクーポン 月次入れ替え (${LIVE ? '★LIVE=実作成' : 'dry-run'}) ===`);
  console.log(`作成日=毎月${CREATE_DAY}日 / 対象=${ONLY.length ? ONLY.join(',') : '全4本'}`);

  const ctx = await openAupayContext();
  const page = ctx.pages()[0] || (await ctx.newPage());
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
    const plans = defs.map((def) => planCoupon({ def, rows, todayYmd, createDay: CREATE_DAY }));

    console.log('\n--- 計画 ---');
    for (const p of plans) {
      if (p.status === 'ready') {
        console.log(`  [${p.key}] ${p.source.title} (${p.source.parsed.amount}円, 〜${p.source.useEndYmd})`);
        console.log(`         → ${p.title} / ${p.amount}円`);
        console.log(`         配布 ${p.period.distStartYmd} ${START_HOUR}:00 〜 ${p.period.distEndYmd} ${END_HOUR}:59`);
        console.log(`         利用 ${p.period.useStartYmd} ${START_HOUR}:00 〜 ${p.period.useEndYmd} ${END_HOUR}:59`);
        if (p.gapWarning) console.log(`         ⚠️ ${p.gapWarning}`);
      } else {
        console.log(`  [${p.key}] ${p.status}: ${p.reason}`);
      }
    }

    for (const plan of plans) {
      if (plan.status !== 'ready') { results.push(plan); continue; }
      console.log(`\n### ${plan.key}: ${plan.title}`);
      try {
        await openCopyForm(page, plan.source.couponId);
        await applyPlan(page, plan);
        const verified = await verifyForm(page, plan);
        console.log('  [verify] 入力内容OK');
        captures.push({ key: plan.key, label: 'form', path: await snap(page, `${plan.key}_1_form`) });

        const confirmText = await submitToConfirm(page);
        console.log(`  [confirm] 確認画面 url=${page.url()}`);
        const summary = await readConfirmSummary(page);
        const entries = Object.entries(summary);
        if (entries.length) {
          for (const [k, v] of entries) console.log(`     ${k}: ${v}`);
        } else {
          // 構造が読めない画面仕様変更に備え、表示テキストをそのまま記録しておく
          console.log(`     [confirm-text] ${confirmText.slice(0, 1200)}`);
        }
        verifyConfirmText(confirmText, plan);
        if (entries.length) verifyConfirmSummary(summary, plan);
        console.log(`  [confirm] 表示内容OK${entries.length ? ' (全項目照合)' : ' (テキスト照合のみ)'}`);
        const controls = await dumpConfirmControls(page);
        console.log(`  [confirm] forms=${JSON.stringify(controls.forms)}`);
        console.log(`  [confirm] buttons=${JSON.stringify(controls.buttons)}`);
        captures.push({ key: plan.key, label: 'confirm', path: await snap(page, `${plan.key}_2_confirm`) });

        if (!LIVE) {
          results.push({ ...plan, status: 'dry-ok', verified, controls });
          console.log('  ✅ dry-run: 確認画面まで到達 (作成はしていません)');
          continue;
        }

        const created = await createFromConfirm(page, controls);
        captures.push({ key: plan.key, label: 'done', path: await snap(page, `${plan.key}_3_done`) });
        results.push({ ...plan, status: 'created', created });
        console.log('  ✅ 作成しました');
      } catch (e) {
        await snap(page, `${plan.key}_error`);
        console.error(`  ❌ ${plan.key}: ${e.message}`);
        results.push({ ...plan, status: 'failed', error: e.message });
      }
    }

    // 作成後の実在確認 (LIVE時)
    if (LIVE && results.some((r) => r.status === 'created')) {
      const after = await fetchCouponRows(page);
      for (const r of results.filter((x) => x.status === 'created')) {
        const hit = findCreatedRow(after, {
          orderCount: r.orderCount, amount: r.amount, useStartYmd: r.period.useStartYmd,
        });
        r.verifiedInList = !!hit;
        r.newCouponId = hit ? hit.couponId : null;
        console.log(`  [after] ${r.title}: ${hit ? `一覧に存在 (${hit.couponId} ${hit.distributionState})` : '⚠️一覧で確認できず'}`);
      }
      captures.push({ key: 'list', label: 'after', path: await snap(page, 'list_after') });
    }

    // 毎日実行するため、何も起きなかった日 (全部skip) は通知しない
    const hasNews = results.some((r) => r.status !== 'skip');
    if (hasNews && NOTIFY) {
      await notify(results, captures);
    } else if (!hasNews) {
      console.log('\n(すべてスキップ = 作成タイミングではないため、GChat通知は送りません)');
    } else {
      console.log('\n(dry-run のためGChat通知は送りません。送るなら ACOUPON_NOTIFY=1)');
    }
    const failed = results.filter((r) => r.status === 'failed' || r.status === 'error');
    process.exitCode = failed.length ? 1 : 0;
  } catch (e) {
    const is2fa = String(e.message).startsWith('2FA_REQUIRED');
    console.error('[FATAL]', e.message);
    const shot = await snap(page, 'fatal_error');
    if (NOTIFY) await sendGChat(buildErrorReport({
      mall: 'aupay-coupon',
      logPath: runLog?.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (クーポン入れ替え中止)' : 'クーポン入れ替え (共通処理)',
        error: e.message,
        url: page.url(),
        screenshot: shot,
      }],
      repro: is2fa
        ? 'ミニPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/aupay-login-spike.mjs (メール暗証番号を入力)'
        : 'node scripts/mall-csv-fetcher/aupay-coupon-rotate.mjs (dry-run) で再現確認',
    }), 'aupay-coupon').catch(() => {});
    process.exitCode = is2fa ? 3 : 1; // 3=手動対応必須
  } finally {
    await ctx.close().catch(() => {});
  }
}

/** GChat通知: 何をどう作ったかを人が読める形で + キャプチャのDriveリンク */
async function notify(results, captures) {
  const stamp = todayJst().replaceAll('-', '');
  const lines = [];
  lines.push(LIVE ? '*au PAY ストアクーポン 入れ替え完了*' : '*au PAY ストアクーポン 入れ替え (ドライラン)*');
  lines.push('');
  for (const r of results) {
    if (r.status === 'created' || r.status === 'dry-ok') {
      lines.push(`*${r.title}*`);
      lines.push(`　値引き: *${r.amount}円* (前回 ${r.source.parsed.amount}円)`);
      lines.push(`　条件: ${r.orderCount}点以上・ストア全品`);
      lines.push(`　配布: ${r.period.distStartYmd} 00:00 〜 ${r.period.distEndYmd} 23:59`);
      lines.push(`　利用: ${r.period.useStartYmd} 00:00 〜 ${r.period.useEndYmd} 23:59`);
      if (r.gapWarning) lines.push(`　⚠️ ${r.gapWarning}`);
      if (r.status === 'created') {
        lines.push(`　結果: ${r.verifiedInList ? `✅作成済み (${r.newCouponId})` : '⚠️作成したが一覧で未確認'}`);
      } else {
        lines.push('　結果: ドライラン (作成していません)');
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

  await sendGChat(lines.join('\n'), 'aupay-coupon').catch((e) => console.warn(`  ⚠ GChat通知失敗: ${e.message}`));
}

main();
