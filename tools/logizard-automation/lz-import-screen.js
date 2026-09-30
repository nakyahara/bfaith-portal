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
 *   - 画面全体の「最初の OK」は押さない。押すのは、決まった文言のモーダル (枠 = ui-dialog / role=dialog / jAlerts の確認の箱) の中の OK だけ (okInDialog)。
 *     本物のロジザード (2026-09-30 の実機の試験で見た) の取込の確認 = jAlerts の jConfirm:
 *       #popup_overlay (覆い) の兄弟に #popup_container > h1#popup_title + #popup_content.confirm > #popup_message (文言) + #popup_panel > #popup_ok・#popup_cancel
 *     特定できない = 押さずに止める。プレビューのサーバーエラー (「エラーが発生しました」) は OK を押さずに止める (画面を残す。Codex #1521 R6)。
 *   - executeImport = 実行ボタン → 「ファイルアップロードを開始します」の OK → 今回押した後に新しく出た結果の表示を返す (読み方は lz-import-check.mjs)。
 *     押す操作は、確かめと押すを**同じページの中の処理で**行う (その間に画面は変わらない。Codex #1521 R2)。止める旗 (import-guard.js) はページにも写し、押す処理の中でも見る。
 *     押すのは click 1 回だけ (mousedown / mouseup を出さない = 確かめと押すの間にページの処理が走らない)・押してよい最後の時刻 (旗の持ち時間) をページに渡す・
 *     ボタンがその場所で一番上 (覆われていない) ことを見る。止めたら、押す段階の間はページを閉じる (送った後の押す処理は取り消せない = 止めた後に押された = afterStop。Codex #1521 R3)。
 *     押す直前に onExecuteIssued() (呼び手が「押した」と記録する) → もう一度旗を見てから押す (その後の失敗は unknown。K7)。
 *     ブラウザの dialog はどれも承認しない (この段階では期待しない = 押さずに止める)。押した後は結果を待つ (止める旗が立っても結果は読む = B)。
 */
import fs from 'fs';
import path from 'path';
import { BASE, sessionLost, SessionLostError, errorShot } from './logizard-common.js';
import { StopError } from './import-guard.js';

export const IMPORT_FILETYPE_LABEL = '商品マスタ';
export const DAILY_PATTERN_LABEL = 'デイリー取込商品マスタ';
/** 取込を始める確認のモーダルの文言 (auto-barcode.js と同じ) */
const CONFIRM_TEXT = 'ファイルアップロードを開始します';
/** 本物の確認 (jAlerts の jConfirm) の文言の全文 (2026-09-30 の実機・半角の ?)。jConfirm の箱のときは空白を除いてこれと完全一致だけ (Codex #1553 R1 Medium) */
export const JCONFIRM_TEXT = 'ファイルアップロードを開始します。よろしいですか?';

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
    if ([...document.querySelectorAll('.blockUI.blockOverlay')].some(vis)) return false; // 処理中 (どれか 1 つでも。Codex #1521 R4)
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
 * 決まった文言のモーダルの中の OK を見つけ、click のときは**同じページの中の処理で押す** (確かめと押すの間に画面は変わらない = JavaScript は 1 本。Codex #1521 R2)。
 *   1. 見えている文字 (空白を除いてつなげたもの) に文言が ちょうど 1 回 (0 = absent / 2 回以上 = ambiguous。OK の有無によらない)
 *   2. モーダルの枠 (role=dialog・class ui-dialog・**jAlerts の確認の箱** のうち、本文に文言があるものが ちょうど 1 つ。popup のような共通の親は枠にしない)。
 *      jAlerts の確認の箱 = 実機どおりの形だけ (Codex #1553 R1 Medium):
 *        見えている #popup_overlay が箱の兄弟・箱の直下 = [h1#popup_title, div#popup_content] の 2 つだけ (この順)・#popup_content の class は confirm だけ・
 *        その直下 = [div#popup_message, div#popup_panel] の 2 つだけ・#popup_message と #popup_title の中に要素が無い (入力欄・入れ子の枠を入れない)・
 *        #popup_panel の直下 = [input#popup_ok (OK), input#popup_cancel (Cancel)] の 2 つの type=button だけ。
 *        形が違う = 枠にしない = unidentified (jAlert = .alert + OK だけ・jPrompt = .prompt + 入力欄 も枠にしない)。
 *        文言は #popup_message の文字 (空白を除く) が JCONFIRM_TEXT と完全一致だけ (呼び手が jconfirmExact で渡す。渡さない = jConfirm は押さない)。
 *      #popup_container の中 (自身も) は、この形を満たす箱そのものだけが候補 (形の違う箱に ui-dialog / role=dialog が付いても前の道に回さない。Codex #1553 R2)。
 *      前の道の枠 (ui-dialog / role=dialog) が #popup_container を中に含む = 候補にしない・中の #popup_container は形に依らず入れ子として本文とボタンから外す
 *      (形の違う箱を外側の枠で包んで押す、をさせない。二重の守り。Codex #1553 R3 Medium)。
 *      題 (#popup_title) を本文に数えないのは、形を満たした箱の直下の題だけ (ほかの枠の中の id=popup_title は数える)。文言は選んだ箱の中の #popup_message で照らす
 *      本文 = 枠の中の見えている文字を空白を除いてつなげたもの (入れ子の枠・ボタン・タイトルの帯 ui-dialog-titlebar・決まった語 (確認・お知らせ・メッセージ・×・閉じる・キャンセル) を除く)。
 *      改行や <br> で文が分かれても同じ (Codex #1521 R3 Medium)。無い = unidentified (文言が枠の外)・2 つ以上 = ambiguous
 *   3. 本文は「文言」と「。」「よろしいですか？」だけ (ほかの文字 = 別のものが同じ枠にいるかもしれない = unidentified)
 *   4. 枠の中の OK (入れ子の枠の中は数えない) が見えているものでちょうど 1 つ。無効 = not_enabled
 *   5. click のとき同じ処理の中で (gate): 処理中の表示が無い (busy)・止める旗がページに立っていない (stopped)・(requireNoResult) 結果の表示が出ていない (result_present)・
 *      押してよい最後の時刻 pressBy を過ぎていない (late = ページの処理が遅れて始まった。Codex #1521 R3 High)・
 *      OK がその場所で一番上にある (covered = 別のモーダルや覆いの下。Codex #1521 R3 High) → click を 1 回だけ出す = clicked
 *      (mousedown / mouseup は出さない = 確かめと押すの間にページの処理が走らない。Codex #1521 R3 High)
 * @returns {Promise<{ state: 'ready'|'clicked'|'absent'|'ambiguous'|'unidentified'|'not_enabled'|'busy'|'stopped'|'result_present'|'late'|'covered', why?: string, text?: string, at?: number }>}
 */
export async function okInDialog(page, needle, { click = false, requireNoResult = false, pressBy = null, jconfirmExact = null } = {}) {
  return page.evaluate(({ needle, click, requireNoResult, pressBy, jconfirmExact }) => {
    const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    // jAlerts の確認の箱 (jConfirm) = 実機どおりの形のときだけ枠 (2026-09-30 の実機の形。Codex #1553 R1 Medium)
    const is = (el, tag, id) => !!el && el.tagName === tag && el.id === id;
    const isJConfirm = (el) => {
      if (el.nodeType !== 1 || el.id !== 'popup_container') return false;
      const ov = document.getElementById('popup_overlay');
      if (!ov || !vis(ov) || ov.parentElement !== el.parentElement) return false;   // 見えている覆いが箱の兄弟
      const c = [...el.children];
      if (c.length !== 2 || !is(c[0], 'H1', 'popup_title') || !is(c[1], 'DIV', 'popup_content')) return false;
      if (c[1].classList.length !== 1 || !c[1].classList.contains('confirm')) return false;
      const parts = [...c[1].children];
      if (parts.length !== 2 || !is(parts[0], 'DIV', 'popup_message') || !is(parts[1], 'DIV', 'popup_panel')) return false;
      if (c[0].children.length || parts[0].children.length) return false;   // 題と文言に要素を入れない (入力欄・入れ子の枠)
      const btns = [...parts[1].children];
      const lab = (b) => String(b.value || '').trim();
      return btns.length === 2 && is(btns[0], 'INPUT', 'popup_ok') && is(btns[1], 'INPUT', 'popup_cancel') && btns.every((b) => b.type === 'button')
        && lab(btns[0]) === 'OK' && lab(btns[1]) === 'Cancel';
    };
    const isRoot = (el) => el.nodeType === 1 && (el.getAttribute('role') === 'dialog' || el.classList.contains('ui-dialog') || isJConfirm(el));
    // 枠の候補: jAlerts の箱の中 (自身も) は形を満たす箱そのものだけ = 形の違う箱に ui-dialog / role=dialog が付いても前の道に回らない (Codex #1553 R2 Medium)
    const candidate = (el) => (el.closest('#popup_container') ? isJConfirm(el) : (isRoot(el) && !el.querySelector('#popup_container')));
    const squash = (s) => String(s || '').replace(/\s+/g, '');
    const N = squash(needle);
    const bodyAll = squash(document.body.innerText);
    const count = bodyAll.split(N).length - 1;
    if (count === 0) return { state: 'absent' };
    if (count > 1) return { state: 'ambiguous', why: `text_${count}` };
    const ALLOWED = new Set(['確認', 'お知らせ', 'メッセージ', '×', '閉じる', 'キャンセル']);
    const bodyOf = (root) => {
      const nested = [...root.querySelectorAll('*')].filter((x) => isRoot(x) || x.id === 'popup_container');   // 中の jAlerts の箱は形に依らず入れ子
      const jTitle = isJConfirm(root) ? root.children[0] : null;   // 数えない題 = 形を満たした箱の直下の h1#popup_title だけ
      let t = '';
      const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
      for (let n = w.nextNode(); n; n = w.nextNode()) {
        const p = n.parentElement;
        if (!p || !vis(p) || nested.some((r) => r.contains(p))) continue;
        if (p.closest('button') || p.closest('.ui-dialog-titlebar') || (jTitle && jTitle.contains(p))) continue;
        const s = squash(n.nodeValue);
        if (!s || ALLOWED.has(s)) continue;
        t += s;
      }
      return { root, body: t, nested };
    };
    const hits = [...document.querySelectorAll('[role="dialog"], .ui-dialog, #popup_container')].filter(candidate).filter(vis).map(bodyOf).filter((x) => x.body.includes(N));
    if (!hits.length) return { state: 'unidentified', why: 'no_dialog_root' };
    if (hits.length > 1) return { state: 'ambiguous', why: `dialogs_${hits.length}` };
    const { root, body, nested } = hits[0];
    // jConfirm の箱 = 文言は実機の全文と完全一致だけ (空白を除く。全角の ？・質問なし・句点なし・後ろに足した文 = 押さない)
    if (isJConfirm(root) && (!jconfirmExact || squash(root.children[1].children[0].textContent) !== squash(jconfirmExact))) return { state: 'unidentified', why: 'jconfirm_text' };   // 選んだ箱の中の文言
    if (!/^[。．.!！?？]*(よろしいですか[？?]?)?[。．.!！?？]*$/.test(body.replace(N, ''))) return { state: 'unidentified', why: 'extra_text' };
    const own = (el) => !nested.some((r) => r.contains(el));
    const label = (b) => String(b.tagName === 'INPUT' ? b.value : b.innerText).trim();
    const oks = [...root.querySelectorAll('input[type="button"],input[type="submit"],button')].filter(own).filter(vis).filter((b) => label(b) === 'OK');
    if (oks.length !== 1) return { state: 'unidentified', why: `ok_${oks.length}` };
    const ok = oks[0];
    const text = (root.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 300);
    if (ok.disabled) return { state: 'not_enabled', text };
    if (!click) return { state: 'ready', text };
    if ([...document.querySelectorAll('.blockUI.blockOverlay')].some(vis)) return { state: 'busy', text };   // 処理中の表示はどれか 1 つでも (Codex #1521 R4)
    if (window.__lzimpStop) return { state: 'stopped', text };
    if (requireNoResult && (window.__lzimpResAt != null || (document.body.innerText || '').includes('インポート結果'))) return { state: 'result_present', text };
    if (pressBy != null && Date.now() > pressBy) return { state: 'late', text };
    ok.scrollIntoView({ block: 'center', inline: 'center' });
    const r = ok.getBoundingClientRect();
    const hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
    if (!hit || !(hit === ok || ok.contains(hit))) return { state: 'covered', text, why: hit ? String(hit.id || hit.className || hit.tagName).slice(0, 60) : 'none' };
    const at = Date.now();
    if (pressBy != null && at > pressBy) return { state: 'late', text };   // 位置の計算の間に過ぎた (Codex #1521 R4)
    ok.click();
    return { state: 'clicked', text, at };
  }, { needle, click, requireNoResult, pressBy, jconfirmExact });
}

/**
 * 実行ボタンを同じページの中の処理で確かめて押す (click を 1 回だけ。mousedown / mouseup は出さない)。
 * 見えている・無効でない・処理中でない・止める旗なし・結果の表示が出ていない・モーダルの覆い (popup_overlay・ui-widget-overlay) が無い・
 * 押してよい最後の時刻を過ぎていない・ボタンがその場所で一番上 (Codex #1521 R3 High)
 */
const clickExecuteInPage = (page, { pressBy }) => page.evaluate(({ pressBy }) => {
  const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const b = document.querySelector('#FM07_01_executeBtn');
  if (!b || !vis(b) || b.disabled) return { clicked: false, why: 'not_ready' };
  if ([...document.querySelectorAll('.blockUI.blockOverlay')].some(vis)) return { clicked: false, why: 'busy' };
  if (window.__lzimpStop) return { clicked: false, why: 'stopped' };
  if (window.__lzimpResAt != null || (document.body.innerText || '').includes('インポート結果')) return { clicked: false, why: 'result_present' };
  if (vis(document.getElementById('popup_overlay')) || [...document.querySelectorAll('.ui-widget-overlay')].some(vis)) return { clicked: false, why: 'modal_open' };
  if (Date.now() > pressBy) return { clicked: false, why: 'late' };
  b.scrollIntoView({ block: 'center', inline: 'center' });
  const r = b.getBoundingClientRect();
  const hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
  if (!hit || !(hit === b || b.contains(hit))) return { clicked: false, why: 'covered' };
  const at = Date.now();
  if (at > pressBy) return { clicked: false, why: 'late' };   // 位置の計算の間に過ぎた (Codex #1521 R4)
  b.click();
  return { clicked: true, at };
}, { pressBy });

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
    // プレビューの生成は非破壊。サーバーの汎用エラー (「エラーが発生しました」) も、OK は押さずに止める (画面を残す)。
    // 前は auto-barcode.js と同じく閉じて 1 回やり直していたが、エラーの枠に別の操作の OK が混ざる形を文言では見分けきれない (K6。Codex #1521 R5・R6)。
    // 本物のエラーのモーダルの形 (preview-failed.html) を見て、閉じてやり直すかは後で決める。影の取込と中原さんとの試験は、やり直しを人がする
    await page.waitForTimeout(1000);
    const baseline = await formText(page);
    let started = false;
    try {
      await page.setInputFiles('#FM07_01_impFile', csvPath);
      // 処理が始まった (処理中の表示・モーダル・画面の変化のどれか) のを待つ = 始まらないまま「消えた」と読まない
      started = await page.waitForFunction((base) => {
        const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
        if ([...document.querySelectorAll('.blockUI.blockOverlay')].some(vis) || vis(document.getElementById('popup_overlay'))) return true;
        const el = document.querySelector('#FM07_01_FORM') || document.body;
        return (el.innerText || '').replace(/\s+/g, ' ').trim() !== base;
      }, baseline, { timeout: startTimeoutMs }).then(() => true).catch(() => false);
      await waitOverlayGone(page, 'CSVプレビュー生成', 120000);
    } catch (e) {
      await capture(page, captureDir, 'preview-failed');   // OK は押さない
      throw e;
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
/** 結果の読みの 1 回分 = 結果を読む範囲の文字・確認の文が見えているか・処理中の表示があるか (同じ 1 回の読みで = その間に画面は変わらない。Codex #1521 R5) */
const readResultArea = (page) => page.evaluate((needle) => {
  const vis = (el) => { if (!el) return false; const s = getComputedStyle(el); if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const form = document.querySelector('#FM07_01_FORM');
  const dlg = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"], [role="dialog"]')].filter(vis);
  const outer = dlg.filter((el) => !dlg.some((o) => o !== el && o.contains(el)));
  const text = [(form ? form.innerText : ''), ...outer.filter((el) => !(form && form.contains(el))).map((d) => d.innerText)].join('\n');
  const squash = (x) => String(x || '').replace(/\s+/g, '');
  return { text, confirm: squash(document.body.innerText).includes(squash(needle)), busy: [...document.querySelectorAll('.blockUI.blockOverlay')].some(vis) };
}, CONFIRM_TEXT);
const RESULT_RE = /インポート結果[\s\S]*?総件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?処理件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?処理不要件数\s*[:：]\s*[0-9][0-9,]*[\s\S]*?エラー件数\s*[:：]\s*[0-9][0-9,]*/;

/**
 * 押した順番と、結果の表示が最初に出た順番をページの中で記録する (今回押した後に出た結果だけを受け取る・Codex #1521 R1 High)。
 * 時刻ではなく 1 ずつ増える番号 (同じ処理の中でも前後が決まる = performance.now は同じ値になりうる)。
 * 実行ボタンのクリック (capture の段階) で __lzimpExecAt (その前にその場で結果の有無を見る)・「インポート結果」が本文に初めて出たときに __lzimpResAt。
 */
const installWatch = (page) => page.evaluate(() => {
  window.__lzimpExecAt = null; window.__lzimpResAt = null; window.__lzimpSeq = 0;
  const tick = () => (window.__lzimpSeq += 1);
  const seen = () => { if (window.__lzimpResAt == null && (document.body.innerText || '').includes('インポート結果')) window.__lzimpResAt = tick(); };
  new MutationObserver(seen).observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true });
  document.addEventListener('click', (e) => { if (e.target && e.target.id === 'FM07_01_executeBtn' && window.__lzimpExecAt == null) { seen(); window.__lzimpExecAt = tick(); } }, true);
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
 * 押す操作は、押せる状態になるまで旗を見ながら待ち、押すときは**確かめと押すを同じページの中の処理で** (okInDialog・clickExecuteInPage)。
 * 止める旗はページにも写す (window.__lzimpStop) = 押す処理の中でも見る (Codex #1521 R2)。
 * @param {import('playwright-core').Page} page
 * @param {object} o
 * @param {object} o.guard  import-guard.js createGuard の旗
 * @param {() => void} o.onExecuteIssued  実行ボタンの最後の確かめを通った直後・押す関数を呼ぶ直前に呼ぶ (呼び手が「押した」と記録する。例外 = 押さない)
 * @param {number} [o.pressWindowMs]  押す処理をページに送ってから始まるまでの持ち時間の上限 (過ぎた = 押さない = late)
 * @returns {Promise<{ executeIssued: boolean, confirm: 'clicked'|'not_shown', resultText: string|null, reason: string|null, afterStop: null|'execute'|'confirm' }>}
 *   reason: null = 結果の表示を読んだ (中身は lz-import-check.mjs で判定) / result_timeout / session_lost / error_modal / unexpected_dialog / page_closed / stale_result /
 *     confirm_not_clicked (確認の OK を押していないのに確認と結果が出ている = 取込が始まったか分からない) / confirm_still_shown (押した後も確認が残っている・また出た)
 *   例外には必ず executeIssued (true = 押す関数を呼んだ = 呼び手は unknown / false = 押していない = failed_before_execute にできる)
 */
export async function executeImport(page, { guard, onExecuteIssued, log = console.log, captureDir = null, readyTimeoutMs = 15000, confirmTimeoutMs = 15000, resultTimeoutMs = 180000, pollMs = 500, stableReads = 3, pressWindowMs = 2000 } = {}) {
  if (!guard || typeof onExecuteIssued !== 'function') throw new Error('guard と onExecuteIssued が要る');
  try { guard.check('取込を始める前'); } catch (e) { e.executeIssued = false; e.afterStop = null; throw e; }   // もう止めてある = 何もしない
  const out = { executeIssued: false, confirm: 'not_shown', resultText: null, reason: null, afterStop: null };
  let unexpectedDialog = null;
  const onDialog = async (d) => { unexpectedDialog = `[${d.type()}] ${d.message().slice(0, 200)}`; await d.dismiss().catch(() => {}); guard.stop('unexpected_dialog'); };
  page.on('dialog', onDialog);
  const stopHere = async (e) => { if (e && e.stopped && !page.isClosed()) await page.close().catch(() => {}); throw e; };
  // 止めた = ページにも旗を写す (押す処理の中で見る) + 押す段階の間はページを閉じる (待っている押す処理を始めさせない。Codex #1521 R3 High)。
  // 送った後に始まった押す処理は取り消せない = 止めた時刻より後に押された (afterStop) を残し、押したとして扱う (executeIssued = true = 呼び手は unknown)
  let pressing = true, stoppedAt = null;
  guard.onStop(() => {
    if (stoppedAt == null) stoppedAt = Date.now();
    if (!pressing || page.isClosed()) return;
    page.evaluate(() => { window.__lzimpStop = true; }).catch(() => {});
    page.close().catch(() => {});
  });
  const afterStop = (at) => stoppedAt != null && Number.isFinite(at) && at >= stoppedAt;
  const pressBy = (where) => { let left; try { left = guard.check(where); } catch (e) { return stopHere(e); } return Date.now() + Math.min(left, pressWindowMs); };
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
    // 押す直前: 旗 → 呼び手の記録 → もう一度旗 (記録の間に止めた・締め切りを過ぎた = 押さない。Codex #1521 R2 High) → ページの中で確かめて押す
    guard.check('実行ボタンの前');
    onExecuteIssued();
    const executeBy = await pressBy('実行ボタンの前 (記録の後)');
    out.executeIssued = true;   // ここから先の例外 = 押したかもしれない
    const pressed = await clickExecuteInPage(page, { pressBy: executeBy });
    if (!pressed.clicked) {
      out.executeIssued = false;   // ページの中で押さなかったと分かった
      if (pressed.why === 'stopped') { try { guard.check('実行ボタン'); } catch (e) { await stopHere(e); } }
      throw new Error(pressed.why === 'result_present' ? '押す前に結果の表示が出た = 押さない' : `実行ボタンを押さなかった (${pressed.why})`);
    }
    if (afterStop(pressed.at)) { out.afterStop = 'execute'; log('⚠ 止めた後に実行ボタンが押された (送った後の押す処理は取り消せない) = 押したとして扱う'); }
    // 取込を始める確認 (決まった文言のモーダルの中の OK だけ)。出ない = 押さずに結果を待つ (auto-barcode.js と同じ)
    const confirmUntil = Date.now() + confirmTimeoutMs;
    while (Date.now() < confirmUntil) {
      try { guard.check('確認の OK の前'); } catch (e) { await stopHere(e); }   // 想定外の dialog も旗を止める = ここで止まる (ページを閉じる)
      if (unexpectedDialog) break;
      if ((await watchState(page)).resAt != null) break;   // 結果の表示が出た = 確認の OK は押さない (古い結果か確認なしの結果かは下で見る)
      const confirmBy = await pressBy('確認の OK を押す直前');
      const m = await okInDialog(page, CONFIRM_TEXT, { click: true, requireNoResult: true, pressBy: confirmBy, jconfirmExact: JCONFIRM_TEXT });
      if (m.state === 'clicked') {
        out.confirm = 'clicked';
        pressing = false;
        if (afterStop(m.at)) { out.afterStop = 'confirm'; log('⚠ 止めた後に確認の OK が押された (送った後の押す処理は取り消せない) = 押したとして扱う'); }
        log('💬 ファイルアップロードを開始します → OK');
        break;
      }
      if (m.state === 'stopped') { try { guard.check('確認の OK の前'); } catch (e) { await stopHere(e); } }
      if (m.state === 'late') log('⚠ 確認の OK: ページの処理が遅れて始まった (押してよい時刻を過ぎた) = 押さずにもう一度');
      if (m.state === 'result_present') break;
      if (m.state === 'ambiguous' || m.state === 'unidentified' || m.state === 'covered') await capture(page, captureDir, 'confirm-unidentified');   // 本物の画面の形を後で見る
      if (m.state === 'ambiguous' || m.state === 'unidentified' || m.state === 'covered') await stopHere(new StopError(`確認のモーダルを 1 つに決められない・覆われている (${m.state}${m.why ? `・${m.why}` : ''}) = 押さずに止める`, `confirm_${m.state}`));
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
    pressing = false;   // ここから先は押さない (止めてもページは閉じない = 結果を読む)
    // 結果を待つ (押した後 = 止める旗が立っても読む)。処理中でない・4 つの見出しと数がそろう・続けて stableReads 回同じ
    const until = Date.now() + resultTimeoutMs;
    let last = null, same = 0, confirmNow = false;
    while (Date.now() < until) {
      if (page.isClosed()) { out.reason = 'page_closed'; return out; }
      if (unexpectedDialog) { out.reason = 'unexpected_dialog'; out.resultText = unexpectedDialog; break; }
      if (await sessionLost(page)) { out.reason = 'session_lost'; break; }
      const w = await watchState(page);
      if (w.resAt != null && w.execAt != null && w.resAt < w.execAt) { out.reason = 'stale_result'; break; }   // 押す前に出ていた結果
      // 確認の文が出ている間は結果を受け取らない: 確認の OK を押していない = 取込が始まったか分からない = すぐ confirm_not_clicked (R4) /
      // 押した後に確認が残っている・また出た = 受け取らずに待つ (消えなければ confirm_still_shown。R5)。結果の文字・確認・処理中は同じ 1 回の読みで見る
      const r = await readResultArea(page);
      confirmNow = r.confirm;
      if (r.confirm && out.confirm !== 'clicked') { out.reason = 'confirm_not_clicked'; break; }
      const at = r.text.indexOf('インポート結果');
      if (!r.confirm && w.resAt != null && at >= 0 && RESULT_RE.test(r.text.slice(at)) && !r.busy) {
        same = r.text === last ? same + 1 : 1;
        last = r.text;
        if (same >= stableReads) { out.resultText = r.text; break; }
      } else { last = null; same = 0; }   // 条件が崩れたら数え直す (続けて同じ = 連続)
      await page.waitForTimeout(pollMs);
    }
    if (!out.resultText && !out.reason) out.reason = confirmNow ? 'confirm_still_shown' : 'result_timeout';
    await capture(page, captureDir, 'result');
    return out;
  } catch (e) {
    // 例外は必ず Error にして executeIssued を載せる (文字列の throw も。Codex #1521 R1 Medium)
    let err = e instanceof Error ? e : new Error(String(e));
    // 止めてページを閉じた後の失敗 (ページが閉じた など) = 止めた理由で返す
    if (!err.stopped && guard.isStopped()) err = new StopError(`止めた (${guard.reason}): ${String(err.message).slice(0, 200)}`, guard.reason);
    err.executeIssued = out.executeIssued;
    err.afterStop = out.afterStop;
    throw err;
  } finally {
    pressing = false;
    if (!page.isClosed()) page.off('dialog', onDialog);
  }
}
