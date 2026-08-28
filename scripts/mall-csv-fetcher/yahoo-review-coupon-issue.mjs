/**
 * yahoo-review-coupon-issue.mjs — レビューお礼クーポン (月次・非表示・定率5%) をストクリ画面操作で発行 (P2-Y PR-Y-C3)
 *
 * Yahoo にクーポン発行 API は無いため、既存の `yahoo-coupon-rotate.mjs` と同じ「一覧→コピー→編集→確認→発行」で作る。
 * ただしレビュー用は **非冪等な操作を絶対に二度走らせない** ことが最優先なので、台帳
 * (`yahoo_campaign_coupons`、apps/warehouse/yahoo-review-coupon-lib.js) の状態機械で守る:
 *   planned →(commit)→ submitting →[画面操作は1回だけ]→ 一覧を op-id で照合
 *     ├─ ちょうど1件 → 詳細URLを読んで issued
 *     └─ それ以外    → reconcile_required (**以後は照合のみ。作成は自動で再試行しない**)
 * 次回以降の実行は reconcile_required を見つけたら「照合だけ」して、見つかれば issued にする。
 *
 * コピー元 = 直近の自作クーポン (台帳の coupon_id) → 無ければ vendor のクーポン (env YAHOO_REVIEW_COUPON_SOURCE_ID)。
 * コピーすると「定率5% / ストア内全商品 / 公開範囲=非表示 / 併用不可」といった設定を引き継げるので、
 * 空フォームを埋めるより事故が起きにくい (rotate と同じ設計)。
 *
 * 実行:
 *   node scripts/mall-csv-fetcher/yahoo-review-coupon-issue.mjs                 # dry-run (確認画面まで。発行しない)
 *   node scripts/mall-csv-fetcher/yahoo-review-coupon-issue.mjs --live          # 実発行
 *   node scripts/mall-csv-fetcher/yahoo-review-coupon-issue.mjs --month 2026-09 # 対象月を明示 (既定=当月)
 *   node scripts/mall-csv-fetcher/yahoo-review-coupon-issue.mjs --reconcile-only # 照合だけ (作成しない)
 * env: WAREHOUSE_DATA_DIR (or DATA_DIR) / YAHOO_REVIEW_COUPON_SOURCE_ID / GCHAT_WEBHOOK
 * exit: 0=正常 (発行済み・作成不要・dry-run) / 1=要人手 (reconcile_required 等) / 2=env・引数エラー / 3=2FA
 */
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import Database from 'better-sqlite3';
import { openYahooContext, gotoStorePage, ensureStoreLogin, STORE_TOP_URL } from './lib-yahoo-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';
import {
  ensureYahooCouponLedger, monthlyCouponPeriod, makeOperationId, couponDescription, isValidCouponUrl,
  reserveMonth, markSubmitting, markIssued, markReconcileRequired, escalateStale, getCouponRow,
  couponUrlMatchesId, isUsableCopySource, COUPON_TITLE, COUPON_DISCOUNT_RATIO, EXPECTED_FORM, FORM_HOUR_START, FORM_HOUR_END,
} from '../../apps/warehouse/yahoo-review-coupon-lib.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, 'spike-output');
const COUPON_PATH_PREFIX = `${STORE_TOP_URL.replace('https://pro.store.yahoo.co.jp', '')}/coupon/`;
const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const LIVE = args.includes('--live');
const RECONCILE_ONLY = args.includes('--reconcile-only');
const DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const SOURCE_ID = (process.env.YAHOO_REVIEW_COUPON_SOURCE_ID || '').trim();
const nowIso = () => new Date().toISOString();
const jstMonth = () => new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 7);
const MONTH = getArg('--month') || jstMonth();

if (!DATA_DIR) { console.error('FATAL: WAREHOUSE_DATA_DIR (or DATA_DIR) が必要'); process.exit(2); }

// dry-run は台帳を一切更新しない (readonly 接続で例外になるのを防ぎ、「書かずに確認」を守る — Codex Y-C3 R3 High)。
// 更新内容はログに出すので、何が起きるはずだったかは分かる
function ledgerWrite(label, fn) {
  if (!LIVE) { console.log(`  [dry-run] 台帳更新はしない (${label})`); return false; }
  return fn();
}

function assertCouponUrl(url, label) {
  if (!/^https:\/\/pro\.store\.yahoo\.co\.jp\/pro\.[a-z0-9-]+\/coupon\//.test(String(url))) {
    throw new Error(`NAV: ${label} が想定外の URL (${url})`);
  }
}
async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `yrcoupon_${label}.png`), fullPage: true }).catch(() => {});
}

/** クーポン一覧 (ID・タイトル・割引・期間・状態)。実測: 一覧の hidden に説明文は無い (照合は詳細ページで行う) */
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
      if (!/^[A-Za-z0-9]{20,}$/.test(cells[0])) continue;
      const hidden = {};
      for (const inp of tr.querySelectorAll('input[type=hidden]')) {
        const m = String(inp.name || '').match(/\[([A-Za-z]+)\]$/);
        if (m) hidden[m[1]] = inp.value;
      }
      out.push({
        couponId: cells[0], title: cells[1],
        discountType: hidden.DiscountType || '', discountRatio: hidden.DiscountRatio || '',
        publicStatus: hidden.PublicStatus || '', state: cells[11],
        useStart: cells[9], useEnd: cells[10],
      });
    }
    return out;
  });
  console.log(`[list] クーポン ${rows.length} 件`);
  if (rows.length > 0 && !rows.some((r) => r.couponId && r.title)) {
    throw new Error('LIST_SHAPE: 一覧から ID/タイトルを読めない (画面仕様変更の疑い)');
  }
  return rows;
}

/**
 * op-id でクーポンを特定する (実測 2026-08-28: 一覧に説明文は出ないので詳細ページ GET で読む)。
 * 候補はタイトル一致の行だけに絞る (自作・vendor とも同じタイトル)。
 * 説明文がどの候補からも読めない場合は throw = 照合不能なので作成させない (fail-closed)。
 */
const MATCH_CANDIDATE_LIMIT = 20;
async function findIssuedByOpId(page, rows, operationId) {
  const candidates = rows.filter((r) => r.title === COUPON_TITLE && r.state !== '削除済み').slice(0, MATCH_CANDIDATE_LIMIT);
  console.log(`[match] タイトル一致の候補 ${candidates.length} 件を詳細で照合`);
  const hits = [];
  let readable = 0;
  for (const c of candidates) {
    const detail = await page.context().newPage();
    try {
      await detail.goto(`${STORE_TOP_URL}/coupon/edit/detail/${c.couponId}`, { waitUntil: 'domcontentloaded', timeout: 30000 });
      if (!detail.url().includes(c.couponId)) throw new Error('詳細ページが別のクーポン');
      const desc = await detail.evaluate(() => {
        const el = document.querySelector('[name="Description"]');
        return el ? String(el.value || '') : null;
      });
      if (desc !== null) readable++;
      if (desc && desc.includes(operationId)) hits.push(c);
    } catch (e) {
      console.warn(`  ⚠ 詳細を読めず (${c.couponId.slice(0, 8)}…): ${String(e.message).split('\n')[0]}`);
    } finally {
      await detail.close().catch(() => {});
    }
  }
  if (candidates.length > 0 && readable === 0) {
    throw new Error('MATCH_SHAPE: どの候補からも説明文を読めない。op-id で照合できないため作成しない (画面仕様変更の疑い)');
  }
  return hits;
}

/** 一覧の「URL」ポップアップから獲得URLを読む (実測: /coupon/edit/detail-url/{id} の textarea) */
async function readCouponUrl(page, couponId) {
  const popup = await page.context().newPage();
  try {
    await popup.goto(`${STORE_TOP_URL}/coupon/edit/detail-url/${couponId}`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (!popup.url().includes(couponId)) throw new Error(`URL_PAGE: 詳細URL画面が別のクーポン (${popup.url().slice(0, 80)})`);
    const url = await popup.evaluate(() => {
      for (const el of document.querySelectorAll('textarea, input[type=text]')) {
        const v = String(el.value || '').trim();
        if (/^https:\/\/shopping\.yahoo\.co\.jp\/coupon\//.test(v)) return v;
      }
      const m = document.body.innerHTML.match(/https:\/\/shopping\.yahoo\.co\.jp\/coupon\/[^"'\s<>]+/);
      return m ? m[0] : null;
    });
    if (url && !couponUrlMatchesId(url, couponId)) throw new Error(`URL_MISMATCH: 取得した獲得URLがクーポンIDと一致しない`);
    return url;
  } finally {
    await popup.close().catch(() => {});
  }
}

async function openCopyForm(page, couponId) {
  const action = `${COUPON_PATH_PREFIX}index/new-edit/${couponId}`;
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {}),
    page.evaluate((act) => { const f = document.f; f.action = act; if (typeof doSubmit === 'function') doSubmit(f); else f.submit(); }, action),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(800);
  assertCouponUrl(page.url(), 'コピー画面');
  if (!page.url().includes('/new-edit/')) throw new Error(`NAV: コピー画面に遷移できていない (${page.url()})`);
}

async function setField(page, name, value) {
  const sel = `[name="${name}"]`;
  await page.fill(sel, String(value), { timeout: 10000 }).catch(() => {});
  let got = await page.inputValue(sel).catch(() => '');
  if (got !== String(value)) {
    await page.evaluate(({ n, v }) => {
      const el = document.querySelector(`[name="${n}"]`);
      if (!el) return;
      const proto = el.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
      Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, v);
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }, { n: name, v: String(value) });
    got = await page.inputValue(sel).catch(() => '');
  }
  if (got !== String(value)) throw new Error(`FORM_SET: ${name} に設定できず (実際="${String(got).slice(0, 40)}")`);
}

/** 編集前のフォーム値 (コピー元から引き継いだ設定) を控える。これが編集で変わっていないことを後で要求する */
async function readInvariants(page) {
  return page.evaluate(() => {
    const v = (n) => { const el = document.querySelector(`[name="${n}"]`); return el ? String(el.value || '') : null; };
    const checked = (n) => { const el = document.querySelector(`[name="${n}"]:checked`); return el ? el.value : null; };
    return {
      DiscountType: checked('DiscountType'), DiscountRatio: v('DiscountRatio'), DiscountPrice: v('DiscountPrice'),
      ItemDesignation: checked('ItemDesignation'), DispFlg: checked('DispFlg'), OrderType: checked('OrderType'),
      Combine: checked('Combine'), nDayFlg: checked('nDayFlg'),
    };
  });
}

/** コピー元から引き継ぐ設定は触らず、タイトル・説明・期間だけ書き換える */
async function applyPlan(page, { period, operationId }) {
  await setField(page, 'Title', COUPON_TITLE);
  await setField(page, 'Description', couponDescription(operationId));
  await setField(page, 'publish_start_date', period.startYmd);
  await setField(page, 'publish_end_date', period.endYmd);
  await setField(page, 'start_date', period.startYmd);
  await setField(page, 'end_date', period.endYmd);
  // 時刻セレクトは HHMMSS 形式 (実測 '000000'/'230000')。台帳の '00'/'23' 表記とは別物
  for (const [n, v] of [['PublishStartHour', FORM_HOUR_START], ['PublishEndHour', FORM_HOUR_END], ['StartHour', FORM_HOUR_START], ['EndHour', FORM_HOUR_END]]) {
    await page.selectOption(`select[name="${n}"]`, v);
  }
}

/** 発行前の読み戻し。コピー元から引き継ぐはずの「定率5% / 非表示 / ストア内全商品」もここで確認する */
async function verifyForm(page, { period, operationId, invariants }) {
  const got = await page.evaluate(() => {
    const v = (n) => { const el = document.querySelector(`[name="${n}"]`); return el ? String(el.value || '') : null; };
    const checked = (n) => { const el = document.querySelector(`[name="${n}"]:checked`); return el ? el.value : null; };
    return {
      Title: v('Title'), Description: v('Description'),
      publish_start_date: v('publish_start_date'), publish_end_date: v('publish_end_date'),
      start_date: v('start_date'), end_date: v('end_date'),
      PublishStartHour: v('PublishStartHour'), PublishEndHour: v('PublishEndHour'),
      StartHour: v('StartHour'), EndHour: v('EndHour'),
      DiscountType: checked('DiscountType'), DiscountRatio: v('DiscountRatio'), DiscountPrice: v('DiscountPrice'),
      ItemDesignation: checked('ItemDesignation'), DispFlg: checked('DispFlg'), OrderType: checked('OrderType'),
      Combine: checked('Combine'), nDayFlg: checked('nDayFlg'),   // 不変条件の比較対象 (Codex Y-C3 R2 High: 読み戻し漏れ)
    };
  });
  const diffs = [];
  const want = {
    Title: COUPON_TITLE, Description: couponDescription(operationId),
    publish_start_date: period.startYmd, publish_end_date: period.endYmd,
    start_date: period.startYmd, end_date: period.endYmd,
    PublishStartHour: FORM_HOUR_START, PublishEndHour: FORM_HOUR_END,
    StartHour: FORM_HOUR_START, EndHour: FORM_HOUR_END,
  };
  for (const [k, w] of Object.entries(want)) if (got[k] !== w) diffs.push(`${k}: 期待"${w}" 実際"${got[k]}"`);
  // コピー元から引き継いだ設定 (割引種別・率・公開範囲・対象商品・併用可否) が編集で変わっていないこと。
  // 画面の値の意味 (DispFlg の 非表示 が 1 か 2 か 等) を決め打ちせずに済む = 仕様変更に強い (Codex Y-C3 R1 High)
  for (const k of ['DiscountType', 'DiscountRatio', 'DiscountPrice', 'ItemDesignation', 'DispFlg', 'OrderType', 'Combine', 'nDayFlg']) {
    if (got[k] !== invariants[k]) diffs.push(`${k} がコピー元から変化: "${invariants[k]}" → "${got[k]}"`);
  }
  // 実測した期待値そのものとも突き合わせる (2026-08-28 実測: 定率5% / 公開範囲=非表示 / ストア全品 / 併用不可 / 期間指定)。
  // コピー元が将来変わっても「レビュー用クーポンの条件」から外れたら止まる
  for (const [k, w] of Object.entries(EXPECTED_FORM)) {
    if (got[k] !== w) diffs.push(`${k}: 期待"${w}" 実際"${got[k]}" (レビュー用クーポンの条件から外れている)`);
  }
  // その上で、レビュー用として必須の条件 (定率 5% であること・定額が入っていないこと) を直接確認する
  if (String(got.DiscountRatio || '') !== String(COUPON_DISCOUNT_RATIO)) diffs.push(`DiscountRatio: 期待"${COUPON_DISCOUNT_RATIO}" 実際"${got.DiscountRatio}"`);
  if (got.DiscountPrice && !['0', ''].includes(String(got.DiscountPrice))) diffs.push(`DiscountPrice が入っている (定額に化けている恐れ): "${got.DiscountPrice}"`);
  if (diffs.length) throw new Error(`FORM_VERIFY: ${diffs.join(' / ')}`);
  console.log(`  [form] 検証OK (種別=${got.DiscountType} 定率${got.DiscountRatio}% / 公開範囲=${got.DispFlg} / 対象=${got.ItemDesignation} / 併用=${got.Combine})`);
  return got;
}

async function submitToConfirm(page) {
  const action = `${COUPON_PATH_PREFIX}index/new-confirm`;
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {}),
    page.evaluate((act) => { const f = document.f; f.action = act; if (typeof doSubmit === 'function') doSubmit(f); else f.submit(); }, action),
  ]);
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(800);
  assertCouponUrl(page.url(), '確認画面');
  const body = (await page.locator('body').innerText().catch(() => '')).replace(/\s+/g, ' ');
  if (/エラー|入力してください|正しく入力/.test(body) && !/確認/.test(body)) throw new Error(`CONFIRM: 確認画面でエラー → ${body.slice(0, 200)}`);
  return body;
}

/** 確認画面の「発行」→ モーダルの「発行」の 2 段階 (rotate.mjs の実測どおり) */
async function clickIssue(page) {
  const tagged = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const els = [...document.querySelectorAll('a, button, input[type=submit], input[type=button]')]
      .filter(vis).filter((el) => (el.innerText || el.value || '').replace(/\s+/g, '') === '発行');
    els.forEach((el, i) => { if (i === 0) el.setAttribute('data-yrc-issue', '1'); });
    return { count: els.length };
  });
  if (tagged.count !== 1) throw new Error(`ISSUE: 発行ボタンを一意に特定できず (可視 ${tagged.count} 件)`);
  await page.locator('[data-yrc-issue="1"]').click({ timeout: 15000 });
  await page.waitForTimeout(1200);
  const modal = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const dialogs = [...document.querySelectorAll('div')].filter((d) => vis(d) && /ご注意ください/.test(d.innerText || ''));
    if (!dialogs.length) return { ok: false, reason: 'モーダルが出ていない' };
    const inner = dialogs.reduce((a, b) => (a.contains(b) ? b : a));
    const btns = [...inner.querySelectorAll('a, button, input[type=submit], input[type=button]')]
      .filter(vis).filter((el) => (el.innerText || el.value || '').replace(/\s+/g, '') === '発行');
    if (btns.length !== 1) return { ok: false, reason: `モーダル内の発行ボタンが ${btns.length} 件` };
    btns[0].setAttribute('data-yrc-issue2', '1');
    return { ok: true };
  });
  if (!modal.ok) throw new Error(`ISSUE: ${modal.reason}`);
  await page.locator('[data-yrc-issue2="1"]').click({ timeout: 15000 });
  await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(1500);
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const runLog = initRunLog('yahoo-review-coupon');
  // dry-run は本番 DB を一切変更しない (readonly = DDL も走らない — Codex Y-C3 R2 High)
  const db = new Database(join(DATA_DIR, 'warehouse.db'), LIVE ? {} : { readonly: true });
  db.pragma('busy_timeout = 10000');
  if (LIVE) ensureYahooCouponLedger(db);
  const hasLedger = !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='yahoo_campaign_coupons'`).get();

  const escalated = LIVE && hasLedger ? escalateStale(db, { nowIso: nowIso() }) : [];
  if (escalated.length) {
    await sendGChat(`🔴 *Yahooレビュークーポン: 人手確認が必要* (${escalated.join(', ')})\n24時間 reconcile できていません。ストクリのクーポン一覧を確認し、台帳 yahoo_campaign_coupons を手で直してください。`, 'yahoo-review-coupon');
  }

  let row = hasLedger ? getCouponRow(db, MONTH) : null;
  console.log(`=== Yahooレビュークーポン ${MONTH} (${LIVE ? '★LIVE' : 'dry-run'}${RECONCILE_ONLY ? ' / 照合のみ' : ''}) ===`);
  console.log(`台帳: ${row ? `${row.status} (op=${row.operation_id}${row.coupon_id ? `, id=${row.coupon_id}` : ''})` : '未作成'}`);

  if (row?.status === 'issued') { console.log('発行済みのため何もしない'); db.close(); return; }
  if (row?.status === 'manual_intervention') {
    console.error('人手対応待ち (manual_intervention)。台帳を直してから再実行してください');
    db.close(); process.exitCode = 1; return;
  }

  let period, operationId;
  try {
    period = monthlyCouponPeriod(MONTH, nowIso());
  } catch (e) {
    console.error(`FATAL: ${e.message}`); db.close(); process.exitCode = 2; return;
  }
  if (!row) {
    if (RECONCILE_ONLY) { console.log('照合のみ指定だが台帳に行が無い (何もしない)'); db.close(); return; }
    operationId = makeOperationId(MONTH);
    if (LIVE) {
      reserveMonth(db, { month: MONTH, period, operationId, nowIso: nowIso() });
      row = getCouponRow(db, MONTH);
    } else {
      // dry-run は台帳に一切書かない (完全非破壊 — Codex Y-C3 R1 補足)
      row = { status: 'planned', operation_id: operationId };
      console.log('(dry-run のため台帳には書き込まない)');
    }
  } else {
    operationId = row.operation_id;
    period = { startYmd: row.coupon_start.slice(0, 10), endYmd: row.coupon_end.slice(0, 10), startHour: row.coupon_start.slice(11, 13), endHour: row.coupon_end.slice(11, 13), spanDays: 0 };
  }

  let context, page;
  try {
    context = await openYahooContext();
    page = context.pages()[0] || (await context.newPage());
    page.on('dialog', (d) => { console.log(`  [dialog] ${d.message().slice(0, 80)}`); d.accept().catch(() => {}); });
    await ensureStoreLogin(page);
    let rows = await fetchCouponRows(page);

    // ── 既に作られていないかを op-id で照合 (submitting / reconcile_required からの復帰もここ) ──
    const found = await findIssuedByOpId(page, rows, operationId);
    if (found.length === 1) {
      const url = await readCouponUrl(page, found[0].couponId);
      if (isValidCouponUrl(url)) {
        ledgerWrite(`issued id=${found[0].couponId}`, () => markIssued(db, { month: MONTH, couponId: found[0].couponId, couponUrl: url, nowIso: nowIso() }));
        console.log(`✅ 照合できた${LIVE ? 'ので issued' : ' (dry-run)'}: id=${found[0].couponId}`);
        if (LIVE) await sendGChat(`✅ *Yahooレビュークーポン ${MONTH} 発行済み* (定率${COUPON_DISCOUNT_RATIO}% / ${period.startYmd}〜${period.endYmd})`, 'yahoo-review-coupon');
      } else {
        ledgerWrite('reconcile_required (URL不明)', () => markReconcileRequired(db, { month: MONTH, note: `獲得URLが読めない (${String(url).slice(0, 60)})`, nowIso: nowIso() }));
        console.error('⚠ クーポンは存在するが獲得URLが読めない → reconcile_required');
        process.exitCode = 1;
      }
      db.close(); await context.close().catch(() => {}); return;
    }
    if (found.length > 1) {
      ledgerWrite('reconcile_required (重複)', () => markReconcileRequired(db, { month: MONTH, note: `同じ op-id のクーポンが ${found.length} 件`, nowIso: nowIso() }));
      console.error(`⚠ 同じ op-id が ${found.length} 件 → reconcile_required (人が1本消す)`);
      db.close(); await context.close().catch(() => {}); process.exitCode = 1; return;
    }
    if (RECONCILE_ONLY || row.status === 'reconcile_required' || row.status === 'submitting') {
      // 作成は自動で再試行しない (二重発行より未発行を選ぶ)
      ledgerWrite('reconcile_required (未検出)', () => markReconcileRequired(db, { month: MONTH, note: `一覧に op-id が見つからない (${row.status})`, nowIso: nowIso() }));
      console.error(`⚠ ${row.status} だが一覧に見つからない → 作成の再試行はしない。人が確認して台帳を直すか、別 op-id で作り直してください`);
      db.close(); await context.close().catch(() => {}); process.exitCode = 1; return;
    }

    // ── ここから作成 (planned のときだけ) ──
    const lastIssued = hasLedger
      ? db.prepare(`SELECT coupon_id FROM yahoo_campaign_coupons WHERE status = 'issued' AND coupon_id IS NOT NULL ORDER BY month DESC LIMIT 1`).get()?.coupon_id
      : null;
    // 直近の自作クーポン → 無ければ vendor のクーポン (env)。どちらも一覧に居ることを後で確認する
    const sourceId = (lastIssued && rows.some((r) => r.couponId === lastIssued)) ? lastIssued : SOURCE_ID;
    if (!sourceId) throw new Error('コピー元のクーポンIDが分からない (env YAHOO_REVIEW_COUPON_SOURCE_ID に vendor の月次クーポンIDを設定)');
    if (!rows.some((r) => r.couponId === sourceId)) throw new Error(`コピー元 ${sourceId} が一覧に無い (削除された?)`);
    const sourceRow = rows.find((r) => r.couponId === sourceId);
    if (!isUsableCopySource(sourceRow)) {
      throw new Error(`SOURCE: コピー元 ${sourceId} が定率${COUPON_DISCOUNT_RATIO}%でない (type=${sourceRow?.discountType} ratio=${sourceRow?.discountRatio})。別条件のクーポンを増やさないため中止`);
    }
    console.log(`コピー元: ${sourceId} (定率${sourceRow.discountRatio}%)`);
    await openCopyForm(page, sourceId);
    const invariants = await readInvariants(page);
    await applyPlan(page, { period, operationId });
    await verifyForm(page, { period, operationId, invariants });
    await submitToConfirm(page);
    await snap(page, `confirm_${MONTH}`);
    if (!LIVE) {
      console.log('dry-run のため発行しない (確認画面まで到達)。実発行は --live');
      db.close(); await context.close().catch(() => {}); return;
    }
    if (!markSubmitting(db, MONTH, nowIso())) {
      console.error('FATAL: submitting に遷移できない (別プロセスが進めた?)。発行しない');
      db.close(); await context.close().catch(() => {}); process.exitCode = 1; return;
    }
    try {
      await clickIssue(page);
    } catch (e) {
      markReconcileRequired(db, { month: MONTH, note: `発行操作が不明 (${String(e.message).slice(0, 120)})`, nowIso: nowIso() }); // ここは LIVE のみ到達
      throw e;
    }
    // ── 発行後の照合 (成否に関わらず一覧を取り直す) ──
    rows = await fetchCouponRows(page);
    const after = await findIssuedByOpId(page, rows, operationId);
    if (after.length === 1) {
      const url = await readCouponUrl(page, after[0].couponId);
      if (isValidCouponUrl(url)) {
        markIssued(db, { month: MONTH, couponId: after[0].couponId, couponUrl: url, nowIso: nowIso() });
        console.log(`✅ 発行成功: id=${after[0].couponId}`);
        await sendGChat(`✅ *Yahooレビュークーポン ${MONTH} を発行しました* (定率${COUPON_DISCOUNT_RATIO}% / ${period.startYmd}〜${period.endYmd} / 非表示)`, 'yahoo-review-coupon');
      } else {
        markReconcileRequired(db, { month: MONTH, note: '発行できたが獲得URLが読めない', nowIso: nowIso() });
        process.exitCode = 1;
      }
    } else {
      markReconcileRequired(db, { month: MONTH, note: `発行後の照合で ${after.length} 件`, nowIso: nowIso() });
      console.error(`⚠ 発行後の照合で ${after.length} 件 → reconcile_required (次回は照合のみ)`);
      await sendGChat(`⚠️ *Yahooレビュークーポン ${MONTH}: 発行結果が確認できません*\nストクリのクーポン一覧で「${operationId}」を含むクーポンを探し、あれば台帳を issued に、無ければ手で作ってください (自動では作り直しません)`, 'yahoo-review-coupon');
      process.exitCode = 1;
    }
    db.close();
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    console.error(`\n⚠️ ${err.message}`);
    if (page) await snap(page, 'error');
    try { db.close(); } catch { /* noop */ }
    // dry-run は外部への副作用ゼロ (通知も出さない)。手動実行なのでコンソールで足りる (Codex Y-C3 R4 High)
    if (LIVE) await sendGChat(buildErrorReport({
      mall: 'yahoo-review-coupon', logPath: runLog.logPath,
      failures: [{ reportType: is2fa ? '2FA_REQUIRED (Yahoo セッション切れ)' : 'coupon_issue', error: err.message, url: page ? page.url() : '', screenshot: join(OUT_DIR, 'yrcoupon_error.png') }],
      repro: is2fa ? 'miniPC の Yahoo-Relogin.bat で再ログイン' : 'node scripts/mall-csv-fetcher/yahoo-review-coupon-issue.mjs (dry-run) で再現。台帳 yahoo_campaign_coupons の status を確認',
    }), 'yahoo-review-coupon');
    process.exitCode = is2fa ? 3 : 1;
  } finally {
    if (context) await context.close().catch(() => {});
  }
}

main();
