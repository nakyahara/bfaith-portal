/**
 * lz-import-screen.js — ロジザードのインポート画面 [PM07/FM07_01] の操作 (マスタ正本切替 ③c-1b-2a)
 *
 * auto-barcode.js の runImport (2026-07-27〜 本番で使ってきた手順) から、**CSV のプレビューを作るところまで**を移した部品。
 * 実行ボタン (#FM07_01_executeBtn) は、ここでは**押さない** (プレビューの生成は非破壊 = 何も登録されない)。
 * 実行と結果の読み取りは ③c-1b-2b で足す (契約 v3 H4: 押す前に取込の状態を importing に・成功 = 総件数 = 行数・処理件数 + 処理不要件数 = 総件数・エラー件数 0)。
 *
 * ログイン・セッションの鍵・ブラウザは呼び手が持つ (同じブラウザで「直前の書き出し → プレビュー」を続けて使う。契約 v3 H8)。
 */
import { BASE, sessionLost, SessionLostError, errorShot } from './logizard-common.js';

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
 * インポート画面で、ファイル種類・取込パターンを選び、CSV のプレビューを作る (実行ボタンは押さない)。
 * @param {import('playwright-core').Page} page  ログイン済み
 * @param {object} opts
 * @param {string} opts.csvPath  取り込む CSV (lz-daily の変えない CSV)
 * @param {string} [opts.patternLabel]  取込パターン (既定 デイリー取込商品マスタ)
 * @returns {Promise<{ previewed: true, pattern: string, onlyAreaImport: object }>}
 */
export async function previewImport(page, { csvPath, patternLabel = DAILY_PATTERN_LABEL, fileTypeLabel = IMPORT_FILETYPE_LABEL, log = console.log } = {}) {
  // 想定外のダイアログ (alert / confirm) は閉じて失敗にする (プレビューでは何も承認しない)
  let unexpectedDialog = null;
  const onDialog = async (d) => { unexpectedDialog = `[${d.type()}] ${d.message().slice(0, 200)}`; await d.dismiss().catch(() => {}); };
  page.on('dialog', onDialog);
  try {
    await page.goto(`${BASE}/PM07/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
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
    for (let attempt = 1; ; attempt++) {
      try {
        await page.setInputFiles('#FM07_01_impFile', csvPath);
        await waitOverlayGone(page, 'CSVプレビュー生成', 120000);
        break;
      } catch (e) {
        if (attempt >= 2 || !/エラーが発生しました/.test(e.message || '')) throw e;
        log('⚠ プレビュー生成でサーバーエラー → モーダルを閉じて1回だけ再試行します');
        await page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first().click().catch(() => {});
        await page.waitForTimeout(5000);
      }
    }
    if (unexpectedDialog) throw new Error(`想定外ダイアログ: ${unexpectedDialog}`);
    if (await sessionLost(page)) throw new SessionLostError('プレビューの後にセッション切れ');
    log('🧪 プレビューまで (実行ボタンは押していない)');
    return { previewed: true, pattern: patternLabel, onlyAreaImport: onlyArea };
  } finally {
    page.off('dialog', onDialog);
  }
}
