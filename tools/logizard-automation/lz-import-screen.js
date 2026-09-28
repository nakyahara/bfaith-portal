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
 * 決まった文言のモーダルの中の OK を 1 つだけ特定して印 (data-lzimp-ok) をつける (K6・Codex #1521 R1)。
 *   1. 文言を含む文字 (見えているもの) の場所が ちょうど 1 つ (0 = absent / 2 つ以上 = ambiguous。OK の有無によらない)
 *   2. その場所から上へ、モーダルの枠 (role=dialog・class ui-dialog・id に popup) をたどる = そのモーダル。枠が無い = unidentified
 *   3. 枠の中の OK (入れ子の別の枠の中は数えない) が見えているものでちょうど 1 つ。0 / 2 つ以上 = unidentified
 *   4. 枠の中のほかの文字 (文言・ボタン・見出しの決まった語を除く) が多い = 別のモーダルが同じ枠にいるかもしれない = unidentified
 *   5. OK が無効 (disabled) = not_enabled (呼び手は待ってもう一度)
 * @returns {Promise<{ state: 'ready'|'absent'|'ambiguous'|'unidentified'|'not_enabled', why?: string, text?: string }>}
 */
export async function markOkInDialog(page, needle, { maxExtraChars = 10 } = {}) {
  return page.evaluate(({ needle, maxExtraChars }) => {
    const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    document.querySelectorAll('[data-lzimp-ok]').forEach((e) => e.removeAttribute('data-lzimp-ok'));
    const isRoot = (el) => el.nodeType === 1 && (el.getAttribute('role') === 'dialog' || el.classList.contains('ui-dialog') || /popup/i.test(el.id || ''));
    // 文言を含む文字の場所 (テキストのノード。大きな表でも速い)
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const places = [];
    for (let n = walker.nextNode(); n; n = walker.nextNode()) if (n.nodeValue.includes(needle) && vis(n.parentElement)) places.push(n.parentElement);
    if (!places.length) return { state: 'absent' };
    if (places.length > 1) return { state: 'ambiguous', why: `text_${places.length}` };
    let root = places[0];
    while (root && !isRoot(root)) root = root.parentElement;
    if (!root) return { state: 'unidentified', why: 'no_dialog_root' };
    const nested = [...root.querySelectorAll('*')].filter(isRoot);
    const own = (el) => !nested.some((r) => r.contains(el));
    if (!own(places[0])) return { state: 'unidentified', why: 'text_in_nested_dialog' };
    const label = (b) => String(b.tagName === 'INPUT' ? b.value : b.innerText).trim();
    const oks = [...root.querySelectorAll('input[type="button"],input[type="submit"],button')].filter(own).filter(vis).filter((b) => label(b) === 'OK');
    if (oks.length !== 1) return { state: 'unidentified', why: `ok_${oks.length}` };
    // 枠の中のほかの文字 (入れ子の枠の中も含む = 別のモーダルの文) を数える
    let extra = 0;
    const w2 = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let n = w2.nextNode(); n; n = w2.nextNode()) {
      if (n.parentElement === places[0] || !vis(n.parentElement)) continue;
      if (n.parentElement.closest('button')) continue;
      extra += n.nodeValue.replace(/\s+/g, '').replace(/^(OK|キャンセル|閉じる|確認|×|x)$/i, '').length;
    }
    extra += places[0].innerText.replace(/\s+/g, '').replace(needle.replace(/\s+/g, ''), '').length;
    if (extra > maxExtraChars) return { state: 'unidentified', why: `extra_text_${extra}` };
    if (oks[0].disabled) return { state: 'not_enabled' };
    oks[0].setAttribute('data-lzimp-ok', '1');
    return { state: 'ready', text: (root.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 300) };
  }, { needle, maxExtraChars });
}

/**
 * 押す操作 (止める旗と持ち時間つき)。止めた = ページを閉じて待っているクリックを中断 (後から押されない)。
 * 持ち時間切れのときに締め切りを過ぎていた = 止め (StopError)。
 * beforeClick = 最後の確かめ (旗・持ち時間) を通った直後・押す関数を呼ぶ直前に呼ぶ (例外 = 押さない)。maxWaitMs = 押せるようになるまで待つ上限。
 */
export async function guardedClick(page, target, where, guard, { beforeClick = null, maxWaitMs = 30000 } = {}) {
  const timeout = Math.min(guard.check(where), maxWaitMs);
  const loc = typeof target === 'string' ? page.locator(target) : target;
  if (beforeClick) beforeClick();
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
        if (m.state !== 'ready') { await capture(page, captureDir, 'preview-failed'); throw new Error(`プレビューのエラーのモーダルの OK を特定できない (${m.state}${m.why ? `・${m.why}` : ''}) = 押さずに止める`); }
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
const RESULT_RE = /インポート結果[\s\S]*?総件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?処理件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?処理不要件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?エラー件数\s*[:：]\s*[0-9][0-9,]*/;
const busyVisible = (page) => page.evaluate(() => {
  const el = document.querySelector('.blockUI.blockOverlay');
  if (!el) return false;
  const s = getComputedStyle(el); const r = el.getBoundingClientRect();
  return !(s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) && r.width > 0 && r.height > 0;
});

/**
 * 押した時刻と、結果の表示が最初に出た時刻をページの中で記録する (今回押した後に出た結果だけを受け取る・Codex #1521 R1 High)。
 * 実行ボタンのクリック (capture の段階) で __lzimpExecAt・「インポート結果」が本文に初めて出たときに __lzimpResAt。
 */
const installWatch = (page) => page.evaluate(() => {
  window.__lzimpExecAt = null; window.__lzimpResAt = null;
  const seen = () => { if (window.__lzimpResAt == null && (document.body.innerText || '').includes('インポート結果')) window.__lzimpResAt = performance.now(); };
  new MutationObserver(seen).observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true });
  document.addEventListener('click', (e) => { if (e.target && e.target.id === 'FM07_01_executeBtn' && window.__lzimpExecAt == null) window.__lzimpExecAt = performance.now(); }, true);
  seen();
  return window.__lzimpResAt;
});
const watchState = (page) => page.evaluate(() => ({ execAt: window.__lzimpExecAt, resAt: window.__lzimpResAt }));
const buttonReady = (page, sel) => page.evaluate((sel) => {
  const b = document.querySelector(sel);
  if (!b) return false;
  const s = getComputedStyle(b); const r = b.getBoundingClientRect();
  return !(s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) && r.width > 0 && r.height > 0 && !b.disabled;
}, sel);

/**
 * 実行ボタン → 「ファイルアップロードを開始します」の OK → 今回押した後に新しく出た結果の表示 (C・K6・K7)。
 * プレビューを作った同じページで呼ぶ (previewImport の後)。
 * 押す操作は、押せる状態 (見えている・無効でない) になるまで旗を見ながら待ち、押せる状態を確かめた直後に短い持ち時間で押す
 * (押す関数の中で長く待たない = 待っている間に出た古い結果や 2 つ目の確認を見落とさない。Codex #1521 R1 High)。
 * @param {import('playwright-core').Page} page
 * @param {object} o
 * @param {object} o.guard  import-guard.js createGuard の旗
 * @param {() => void} o.onExecuteIssued  実行ボタンの最後の確かめを通った直後・押す関数を呼ぶ直前に呼ぶ (呼び手が「押した」と記録する。例外 = 押さない)
 * @returns {Promise<{ executeIssued: boolean, confirm: 'clicked'|'not_shown', resultText: string|null, reason: string|null }>}
 *   reason: null = 結果の表示を読んだ (中身は lz-import-check.mjs で判定) / result_timeout / session_lost / error_modal / unexpected_dialog / page_closed / stale_result
 *   例外には必ず executeIssued (true = 押す関数を呼んだ = 呼び手は unknown / false = 押していない = failed_before_execute にできる)
 */
export async function executeImport(page, { guard, onExecuteIssued, log = console.log, captureDir = null, readyTimeoutMs = 15000, confirmTimeoutMs = 15000, resultTimeoutMs = 180000, pollMs = 500, stableReads = 3 } = {}) {
  if (!guard || typeof onExecuteIssued !== 'function') throw new Error('guard と onExecuteIssued が要る');
  const out = { executeIssued: false, confirm: 'not_shown', resultText: null, reason: null };
  let unexpectedDialog = null;
  const onDialog = async (d) => { unexpectedDialog = `[${d.type()}] ${d.message().slice(0, 200)}`; await d.dismiss().catch(() => {}); guard.stop('unexpected_dialog'); };
  page.on('dialog', onDialog);
  const stopHere = async (e) => { if (e && e.stopped && !page.isClosed()) await page.close().catch(() => {}); throw e; };
  try {
    // 押す前: 結果の表示がもうある = どれが今回か分からなくなる = 押さない
    if ((await installWatch(page)) != null || countOf(await resultAreaText(page), 'インポート結果') > 0) throw new Error('押す前に結果の表示がもうある = 押さない');
    // 実行ボタンが押せる状態になるまで (その間に結果が出た = 押さない)
    const readyUntil = Date.now() + readyTimeoutMs;
    for (;;) {
      try { guard.check('実行ボタンの前'); } catch (e) { await stopHere(e); }
      if ((await watchState(page)).resAt != null) throw new Error('押す前に結果の表示が出た = 押さない');
      if (await buttonReady(page, '#FM07_01_executeBtn')) break;
      if (Date.now() > readyUntil) throw new Error('実行ボタンが押せる状態にならない');
      await page.waitForTimeout(pollMs);
    }
    await guardedClick(page, '#FM07_01_executeBtn', '実行ボタン', guard, {
      maxWaitMs: 2000,
      beforeClick: () => { onExecuteIssued(); out.executeIssued = true; },
    });
    // 取込を始める確認 (決まった文言のモーダルの中の OK だけ)。出ない = 押さずに結果を待つ (auto-barcode.js と同じ)
    const confirmUntil = Date.now() + confirmTimeoutMs;
    while (Date.now() < confirmUntil) {
      if (unexpectedDialog) break;
      try { guard.check('確認の OK の前'); } catch (e) { await stopHere(e); }
      const m = await markOkInDialog(page, 'ファイルアップロードを開始します');
      if (m.state === 'ready') {
        await guardedClick(page, '[data-lzimp-ok="1"]', '確認の OK', guard, { maxWaitMs: 2000 });
        out.confirm = 'clicked';
        log('💬 ファイルアップロードを開始します → OK');
        break;
      }
      if (m.state === 'ambiguous' || m.state === 'unidentified') await capture(page, captureDir, 'confirm-unidentified');   // 本物の画面の形を後で見る
      if (m.state === 'ambiguous' || m.state === 'unidentified') await stopHere(new StopError(`確認のモーダルを 1 つに決められない (${m.state}${m.why ? `・${m.why}` : ''}) = 押さずに止める`, `confirm_${m.state}`));
      if (m.state === 'absent') {
        // 決まった文言のない別のモーダル = 押さずに止める
        const other = await page.evaluate(() => {
          const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
          const pop = document.getElementById('popup_overlay');
          if (!vis(pop)) return null;
          const t = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"]')].filter(vis).map((d) => d.innerText).join(' / ');
          return /インポート結果/.test(t) ? null : t.replace(/\s+/g, ' ').slice(0, 300);
        });
        if (other) { out.reason = 'error_modal'; out.resultText = other; await capture(page, captureDir, 'result'); return out; }
        if ((await watchState(page)).resAt != null) break;   // 確認なしで結果が出た
      }
      await page.waitForTimeout(pollMs);   // not_enabled / absent = 待ってもう一度
    }
    // 結果を待つ (押した後 = 止める旗が立っても読む)。処理中でない・4 つの見出しと数がそろう・続けて stableReads 回同じ
    const until = Date.now() + resultTimeoutMs;
    let last = null, same = 0;
    while (Date.now() < until) {
      if (page.isClosed()) { out.reason = 'page_closed'; return out; }
      if (unexpectedDialog) { out.reason = 'unexpected_dialog'; out.resultText = unexpectedDialog; break; }
      if (await sessionLost(page)) { out.reason = 'session_lost'; break; }
      const w = await watchState(page);
      if (w.resAt != null && w.execAt != null && w.resAt < w.execAt) { out.reason = 'stale_result'; break; }   // 押す前に出ていた結果
      const t = await resultAreaText(page);
      const at = t.indexOf('インポート結果');
      if (w.resAt != null && at >= 0 && RESULT_RE.test(t.slice(at)) && !(await busyVisible(page))) {
        same = t === last ? same + 1 : 1;
        last = t;
        if (same >= stableReads) { out.resultText = t; break; }
      } else { last = null; same = 0; }   // 条件が崩れたら数え直す (続けて同じ = 連続)
      await page.waitForTimeout(pollMs);
    }
    if (!out.resultText && !out.reason) out.reason = 'result_timeout';
    await capture(page, captureDir, 'result');
    return out;
  } catch (e) {
    // 例外は必ず Error にして executeIssued を載せる (文字列の throw も。Codex #1521 R1 Medium)
    const err = e instanceof Error ? e : new Error(String(e));
    err.executeIssued = out.executeIssued;
    throw err;
  } finally {
    if (!page.isClosed()) page.off('dialog', onDialog);
  }
}
