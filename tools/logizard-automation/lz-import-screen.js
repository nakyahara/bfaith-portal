/**
 * lz-import-screen.js — ロジザードのインポート画面 [PM07/FM07_01] の操作 (マスタ正本切替 ③c-1b-2a)
 *
 * auto-barcode.js の runImport (2026-07-27〜 本番で使ってきた手順) から、**CSV のプレビューを作るところまで**を移した部品。
 * 実行ボタン (#FM07_01_executeBtn) は、ここでは**押さない** (プレビューの生成は非破壊 = 何も登録されない)。
 * 実行と結果の読み取りは ③c-1b-2b で足す (契約 v3 H4: 押す前に取込の状態を importing に・成功 = 総件数 = 行数・処理件数 + 処理不要件数 = 総件数・エラー件数 0)。
 *
 * プレビューが「できた」と言えるのは (Codex #1516 R1 Medium): ① ファイルを渡した後に処理が始まった (処理中の表示か画面の変化が見えた)
 *   ② 処理中の表示が消え、エラーのモーダルが無い ③ 画面 (#FM07_01_FORM) の中身が渡す前と変わった ④ ファイルの入力欄に今回のファイル名が 1 つ。
 *   どれかが欠ける = できたと確かめられない = 失敗 (成功にしない)。画面は captureDir に残す (本物の画面でのプレビューの目印を 2b で決めるため)。
 *
 * ログイン・セッションの鍵・ブラウザは呼び手が持つ (同じブラウザで「直前の書き出し → プレビュー」を続けて使う。契約 v3 H8)。
 *
 * ③c-1b-2b-1b (契約 v3 K6・K7・C):
 *   - 画面全体の「最初の OK」は押さない。押すのは、決まった文言を含むモーダルの中の OK だけ (markOkInDialog)。特定できない = 押さずに止める
 *     (プレビューのサーバーエラー「エラーが発生しました」を閉じるのも同じ)。
 *   - executeImport = 実行ボタン → 「ファイルアップロードを開始します」の OK → 今回押した後に新しく出た結果の表示を返す (読み方は lz-import-check.mjs)。
 *     押す操作はどれも止める旗 (import-guard.js) と持ち時間つき (guardedClick)。止めた = 待っているクリックもページを閉じて中断する。
 *     実行ボタンを押す関数を呼ぶ直前に onExecuteIssued() (呼び手が「押した」と記録する = その後の失敗は unknown。K7)。
 *     ブラウザの dialog はどれも承認しない (この段階では期待しない = 押さずに止める)。押した後は結果を待つ (止める旗が立っても結果は読む = B)。
 */
import fs from 'fs';
import path from 'path';
import { BASE, sessionLost, SessionLostError, errorShot } from './logizard-common.js';
import { StopError } from './import-guard.js';

export const IMPORT_FILETYPE_LABEL = '商品マスタ';
export const DAILY_PATTERN_LABEL = 'デイリー取込商品マスタ';

/** 処理中の表示が消えるのを待つ。エラーのモーダルが出た = throw (auto-barcode.js と同じ判定) */
export async function waitOverlayGone(page, label, timeoutMs) {
  const res = await page.waitForFunction(() => {
    const vis = (el) => {
      if (!el) return false;
      const s = getComputedStyle(el);
      if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false;
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    };
    const pop = document.getElementById('popup_overlay');
    if (vis(pop)) {
      const dlgs = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"]')].filter(vis);
      const t = dlgs.map((d) => d.innerText).join(' / ').replace(/\s+/g, ' ').trim();
      return 'MODAL:' + (t || '(不明なモーダル)').slice(0, 300);
    }
    if (vis(document.querySelector('.blockUI.blockOverlay'))) return false; // 処理中
    return 'READY';
  }, undefined, { timeout: timeoutMs }).then((h) => h.jsonValue()).catch(() => 'TIMEOUT');
  if (res === 'TIMEOUT') {
    await errorShot(page, `imp-overlay-timeout-${label}`);
    throw new Error(`${label}: 処理中表示が${Math.round(timeoutMs / 1000)}秒消えません`);
  }
  if (typeof res === 'string' && res.startsWith('MODAL:')) {
    await errorShot(page, `imp-modal-${label}`);
    throw new Error(`${label}: ロジザードがモーダル表示: ${res.slice(6)}`);
  }
}

/** select を「表示文言の完全一致」で選ぶ (value 直書きしない。画面の文言変更は明示エラーにする) */
export async function selectOptionByText(page, sel, label, what, { log = console.log, timeoutMs = 20000 } = {}) {
  const found = await page.waitForFunction(({ sel, label }) => {
    const s = document.querySelector(sel);
    if (!s) return false;
    const opt = [...s.options].find((o) => (o.textContent || '').trim() === label);
    return opt ? { v: opt.value } : false;
  }, { sel, label }, { timeout: timeoutMs }).then((h) => h.jsonValue()).catch(() => null);
  if (!found) {
    await errorShot(page, 'imp-select');
    throw new Error(`${what}: 選択肢「${label}」が見つかりません`);
  }
  await page.selectOption(sel, found.v);
  const now = await page.evaluate((sel) => {
    const s = document.querySelector(sel);
    const o = s && s.options[s.selectedIndex];
    return o ? (o.textContent || '').trim() : null;
  }, sel);
  if (now !== label) throw new Error(`${what}: 「${label}」を選択できませんでした (現在: ${now})`);
  log(`✔ ${what}: ${label}`);
  return found.v;
}

/**
 * 決まった文言を含むモーダルの中の OK を 1 つだけ特定して印 (data-lzimp-ok) をつける。
 * 文言を含む見えている箱のうち「見えている OK をちょうど 1 つ持つ」いちばん内側の箱 = そのモーダル (jQuery UI の本文とボタンの枠が別でも)。
 * @returns {Promise<{ ok: boolean, why?: string, text?: string }>}  ok = 印をつけた
 */
export async function markOkInDialog(page, needle) {
  return page.evaluate((needle) => {
    const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    document.querySelectorAll('[data-lzimp-ok]').forEach((e) => e.removeAttribute('data-lzimp-ok'));
    const okOf = (box) => [...box.querySelectorAll('input[type="button"],input[type="submit"],button')].filter(vis).filter((b) => String(b.tagName === 'INPUT' ? b.value : b.innerText).trim() === 'OK');
    const boxes = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"], [role="dialog"]')]
      .filter(vis).filter((el) => (el.innerText || '').includes(needle)).filter((el) => okOf(el).length === 1);
    const inner = boxes.filter((el) => !boxes.some((o) => o !== el && el.contains(o)));
    if (inner.length !== 1) return { ok: false, why: inner.length ? 'dialog_ambiguous' : 'dialog_or_ok_missing' };
    okOf(inner[0])[0].setAttribute('data-lzimp-ok', '1');
    return { ok: true, text: (inner[0].innerText || '').replace(/\s+/g, ' ').trim().slice(0, 300) };
  }, needle);
}

/**
 * 押す操作 (止める旗と持ち時間つき)。止めた = ページを閉じて待っているクリックを中断 (後から押されない)。
 * 持ち時間切れのときに締め切りを過ぎていた = 止め (StopError)。
 */
export async function guardedClick(page, target, where, guard) {
  const timeout = guard.check(where);
  const loc = typeof target === 'string' ? page.locator(target) : target;
  let stopWon = false;
  const stopP = new Promise((_, reject) => guard.onStop((r) => { stopWon = true; reject(new StopError(`${where}: 止めた (${r})`, r)); }));
  stopP.catch(() => { /* 下の race が受ける */ });
  const clickP = loc.click({ timeout });
  clickP.catch(() => { /* ページを閉じた後の拒否 */ });
  try {
    await Promise.race([clickP, stopP]);
  } catch (e) {
    if (stopWon || (e && e.stopped)) { await page.close().catch(() => {}); throw e && e.stopped ? e : new StopError(`${where}: 止めた (${guard.reason})`, guard.reason); }
    try { guard.check(where); } catch (g) { await page.close().catch(() => {}); throw g; }   // 持ち時間切れ = 締め切りを過ぎた
    throw e;
  }
}

const formText = (page) => page.evaluate(() => {
  const el = document.querySelector('#FM07_01_FORM') || document.body;
  return (el.innerText || '').replace(/\s+/g, ' ').trim();
});

async function capture(page, dir, name) {
  if (!dir) return;
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `${name}.html`), await page.content(), 'utf8');
    await page.screenshot({ path: path.join(dir, `${name}.png`), fullPage: true, timeout: 8000 });
  } catch { /* 画面の保存の失敗は本体に影響させない */ }
}

/**
 * インポート画面で、ファイル種類・取込パターンを選び、CSV のプレビューを作る (実行ボタンは押さない)。
 * @param {import('playwright-core').Page} page  ログイン済み
 * @param {object} opts
 * @param {string} opts.csvPath  取り込む CSV (lz-daily の変えない CSV)
 * @param {string} [opts.patternLabel]  取込パターン (既定 デイリー取込商品マスタ)
 * @param {string} [opts.captureDir]  プレビューの画面 (HTML・PNG) を残す場所
 * @param {string} [opts.base]  ロジザードの URL の土台 (試験で差し替える)
 * @param {number} [opts.startTimeoutMs]  ファイルを渡してから処理が始まるのを待つ時間
 * @returns {Promise<{ previewed: true, pattern: string, onlyAreaImport: object, confirmed: object }>}
 */
export async function previewImport(page, { csvPath, patternLabel = DAILY_PATTERN_LABEL, fileTypeLabel = IMPORT_FILETYPE_LABEL, log = console.log, captureDir = null, base = BASE, startTimeoutMs = 15000 } = {}) {
  // 想定外のダイアログ (alert / confirm) は閉じて失敗にする (プレビューでは何も承認しない)
  let unexpectedDialog = null;
  const onDialog = async (d) => { unexpectedDialog = `[${d.type()}] ${d.message().slice(0, 200)}`; await d.dismiss().catch(() => {}); };
  page.on('dialog', onDialog);
  try {
    await page.goto(`${base}/PM07/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (await sessionLost(page)) throw new SessionLostError('PM07 遷移時にセッション切れ');
    await page.click(`a[onclick*="openFunctionBar('FM07_01')"]`, { timeout: 15000 }).catch(() => {});
    await page.waitForSelector('#FM07_01_executeBtn', { state: 'visible', timeout: 15000 });
    await selectOptionByText(page, '#FM07_01_fileId', fileTypeLabel, 'ファイル種類', { log });
    await selectOptionByText(page, '#FM07_01_ptrnId', patternLabel, '取込パターン', { log });
    await waitOverlayGone(page, '取込パターン適用', 30000);
    const onlyArea = await page.evaluate(() => {
      const el = document.querySelector('#FM07_01_onlyAreaImport');
      return el ? { exists: true, disabled: el.disabled, checked: el.checked } : { exists: false };
    });
    log(`ℹ ログイン倉庫のデータのみ取り込む: ${JSON.stringify(onlyArea)}`);
    // プレビューの生成は非破壊。サーバーの汎用エラー (「エラーが発生しました」) は 1 回だけ閉じて再試行 (auto-barcode.js と同じ)
    await page.waitForTimeout(1000);
    const baseline = await formText(page);
    let started = false;
    for (let attempt = 1; ; attempt++) {
      try {
        await page.setInputFiles('#FM07_01_impFile', csvPath);
        // 処理が始まった (処理中の表示・モーダル・画面の変化のどれか) のを待つ = 始まらないまま「消えた」と読まない
        started = await page.waitForFunction((base) => {
          const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
          if (vis(document.querySelector('.blockUI.blockOverlay')) || vis(document.getElementById('popup_overlay'))) return true;
          const el = document.querySelector('#FM07_01_FORM') || document.body;
          return (el.innerText || '').replace(/\s+/g, ' ').trim() !== base;
        }, baseline, { timeout: startTimeoutMs }).then(() => true).catch(() => false);
        await waitOverlayGone(page, 'CSVプレビュー生成', 120000);
        break;
      } catch (e) {
        if (attempt >= 2 || !/エラーが発生しました/.test(e.message || '')) { await capture(page, captureDir, 'preview-failed'); throw e; }
        log('⚠ プレビュー生成でサーバーエラー → そのモーダルの OK だけを押して1回だけ再試行します');
        // 画面全体の「最初の OK」は押さない = 「エラーが発生しました」のモーダルの中の OK だけ。特定できない = 止める (K6)
        const m = await markOkInDialog(page, 'エラーが発生しました');
        if (!m.ok) { await capture(page, captureDir, 'preview-failed'); throw new Error(`プレビューのエラーのモーダルの OK を特定できない (${m.why}) = 押さずに止める`); }
        await page.click('[data-lzimp-ok="1"]', { timeout: 10000 });
        await page.waitForTimeout(5000);
      }
    }
    if (unexpectedDialog) throw new Error(`想定外ダイアログ: ${unexpectedDialog}`);
    if (await sessionLost(page)) throw new SessionLostError('プレビューの後にセッション切れ');
    const changed = (await formText(page)) !== baseline;
    const fileName = await page.evaluate(() => { const i = document.querySelector('#FM07_01_impFile'); return i && i.files && i.files.length === 1 ? i.files[0].name : null; });
    const confirmed = { started, changed, file: fileName === path.basename(csvPath) };
    await capture(page, captureDir, 'preview');
    if (!confirmed.started || !confirmed.changed || !confirmed.file) {
      throw new Error(`プレビューが作られたか確かめられない (処理の開始 ${confirmed.started}・画面の変化 ${confirmed.changed}・ファイル ${confirmed.file})`);
    }
    log('🧪 プレビューまで (実行ボタンは押していない)');
    return { previewed: true, pattern: patternLabel, onlyAreaImport: onlyArea, confirmed };
  } finally {
    page.off('dialog', onDialog);
  }
}

/** 結果を読む範囲 = フォームの文字 + 見えているモーダルの文字 (auto-barcode.js と同じ範囲) */
const resultAreaText = (page) => page.evaluate(() => {
  const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const form = document.querySelector('#FM07_01_FORM');
  const dlg = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"], [role="dialog"]')].filter(vis);
  const outer = dlg.filter((el) => !dlg.some((o) => o !== el && o.contains(el)));   // 入れ子は外側だけ (同じ文字を 2 回数えない)
  return [(form ? form.innerText : ''), ...outer.filter((el) => !(form && form.contains(el))).map((d) => d.innerText)].join('\n');
});
const countOf = (t, s) => t.split(s).length - 1;
const RESULT_LABELS = ['総件数', '処理件数', '処理不要件数', 'エラー件数'];

/**
 * 実行ボタン → 「ファイルアップロードを開始します」の OK → 今回押した後に新しく出た結果の表示 (C・K6・K7)。
 * プレビューを作った同じページで呼ぶ (previewImport の後)。
 * @param {import('playwright-core').Page} page
 * @param {object} o
 * @param {object} o.guard  import-guard.js createGuard の旗
 * @param {() => void} o.onExecuteIssued  実行ボタンを押す関数を呼ぶ直前に呼ぶ (呼び手が「押した」と記録する)
 * @returns {Promise<{ executeIssued: boolean, confirm: 'clicked'|'not_shown', resultText: string|null, reason: string|null }>}
 *   reason: null = 結果の表示を読んだ (中身は lz-import-check.mjs で判定) / result_timeout / session_lost / error_modal / unexpected_dialog / page_closed
 *   押す前の失敗は例外 (executeIssued: false のまま = 呼び手は failed_before_execute にできる)。押した後の失敗も例外 (e.executeIssued = true = unknown)
 */
export async function executeImport(page, { guard, onExecuteIssued, log = console.log, captureDir = null, confirmTimeoutMs = 15000, resultTimeoutMs = 180000, pollMs = 500 } = {}) {
  if (!guard || typeof onExecuteIssued !== 'function') throw new Error('guard と onExecuteIssued が要る');
  const out = { executeIssued: false, confirm: 'not_shown', resultText: null, reason: null };
  let unexpectedDialog = null;
  const onDialog = async (d) => { unexpectedDialog = `[${d.type()}] ${d.message().slice(0, 200)}`; await d.dismiss().catch(() => {}); guard.stop('unexpected_dialog'); };
  page.on('dialog', onDialog);
  try {
    // 押す前: 結果の表示がもうある = どれが今回か分からなくなる = 押さない
    if (countOf(await resultAreaText(page), 'インポート結果') > 0) throw new Error('押す前に結果の表示がもうある = 押さない');
    guard.check('実行ボタンの前');
    out.executeIssued = true;
    onExecuteIssued();
    await guardedClick(page, '#FM07_01_executeBtn', '実行ボタン', guard);
    // 取込を始める確認 (決まった文言のモーダルの中の OK だけ)。出ない = 押さずに結果を待つ (auto-barcode.js と同じ)
    const deadline = Date.now() + confirmTimeoutMs;
    while (Date.now() < deadline) {
      if (unexpectedDialog) break;
      const m = await markOkInDialog(page, 'ファイルアップロードを開始します');
      if (m.ok) {
        await guardedClick(page, '[data-lzimp-ok="1"]', '確認の OK', guard);
        out.confirm = 'clicked';
        log('💬 ファイルアップロードを開始します → OK');
        break;
      }
      if (m.why === 'dialog_ambiguous') throw new StopError('確認のモーダルが 2 つ以上 = 押さずに止める', 'confirm_ambiguous');
      // 決まった文言のない別のモーダル = 押さずに止める
      const other = await page.evaluate(() => {
        const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
        const pop = document.getElementById('popup_overlay');
        if (!vis(pop)) return null;
        const t = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"]')].filter(vis).map((d) => d.innerText).join(' / ');
        return /インポート結果/.test(t) ? null : t.replace(/\s+/g, ' ').slice(0, 300);
      });
      if (other) { out.reason = 'error_modal'; out.resultText = other; await capture(page, captureDir, 'result'); return out; }
      if (countOf(await resultAreaText(page), 'インポート結果') > 0) break;   // 確認なしで結果が出た
      await page.waitForTimeout(pollMs);
    }
    // 結果を待つ (押した後 = 止める旗が立っても読む)。4 つの見出しがそろい、読み直して同じになったら返す
    const until = Date.now() + resultTimeoutMs;
    let last = null;
    while (Date.now() < until) {
      if (page.isClosed()) { out.reason = 'page_closed'; return out; }
      if (unexpectedDialog) { out.reason = 'unexpected_dialog'; out.resultText = unexpectedDialog; break; }
      if (await sessionLost(page)) { out.reason = 'session_lost'; break; }
      const t = await resultAreaText(page);
      const at = t.indexOf('インポート結果');
      if (at >= 0 && RESULT_LABELS.every((l) => t.slice(at).includes(l))) {
        if (t === last) { out.resultText = t; break; }
        last = t;
      }
      await page.waitForTimeout(pollMs);
    }
    if (!out.resultText && !out.reason) out.reason = 'result_timeout';
    await capture(page, captureDir, 'result');
    return out;
  } catch (e) {
    if (e && typeof e === 'object') e.executeIssued = out.executeIssued;
    throw e;
  } finally {
    if (!page.isClosed()) page.off('dialog', onDialog);
  }
}
