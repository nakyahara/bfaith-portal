/**
 * lz-real-session.mjs — 本物のロジザードの操作の包み (試験・毎晩・影で共用。マスタ正本切替 ③c-1b-2b-2 契約 v3 C)
 *
 * miniPC の C:\tools\logizard-automation の部品を使い、**1 つのセッションの鍵 (logizard-session.lock)・1 つのブラウザとページ**の中で
 * 商品の書き出し → バーコードの書き出し → プレビュー → 実行 → 商品の書き出し → バーコードの書き出し を行う (別のログインをしない)。
 *   allowExecute = false (影) = executeImport を渡さない (影は押さない、を包みで保つ)
 *   barcode-export.js が無い = exportBarcodes を渡さない (= エンジンが押さない・K4)
 * capabilities は渡す ops から作る (包みの本当の ops と一致する)。
 * 部品の約束:
 *   logizard-common.js: loadEnv / assertLocalWriteDirs / acquireLock / releaseLock / launchBrowser / login
 *   shohin-export.js: exportShohinMaster(page, { dlDir, minRows }) → { buf }
 *   lz-import-screen.js: previewImport(page, { csvPath, ... }) / executeImport(page, o)
 *   barcode-export.js: exportBarcodeMaster(page, { dlDir }) → { buf } (固定の出力先に書かない)
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * @param {object} p
 * @param {string} p.automationDir  C:\tools\logizard-automation
 * @param {string} p.label          ログインの名乗り (記録に出る)
 * @param {boolean} p.allowExecute  押してよいか (影 = false)。渡さない = 例外 (決めてから呼ぶ)
 * @returns {{ withSession: (fn: (ops: object) => Promise<any>) => Promise<any>, capabilities: { exportBarcodes: boolean, executeImport: boolean } }}
 */
export function realSession({ automationDir, label, allowExecute }) {
  if (typeof allowExecute !== 'boolean') throw new Error('realSession: allowExecute (押してよいか) を true / false で渡す');
  if (!automationDir || !label) throw new Error('realSession: automationDir と label が要る');
  const hasBarcode = fs.existsSync(path.join(automationDir, 'barcode-export.js'));
  const capabilities = Object.freeze({ exportBarcodes: hasBarcode, executeImport: allowExecute });
  const withSession = async (fn) => {
    const imp = (f) => import(pathToFileURL(path.join(automationDir, f)).href);
    const common = await imp('logizard-common.js');
    const { exportShohinMaster } = await imp('shohin-export.js');
    const screen = await imp('lz-import-screen.js');
    const barcode = hasBarcode ? await imp('barcode-export.js') : null;
    common.loadEnv();   // ロジザードの ID とパスワード (C:\tools\logizard-automation\.env。中身は読まない)
    common.assertLocalWriteDirs();
    // 共通部品の login は ID かパスワードが無いと process.exit する = 鍵を取る前に確かめて例外にする (Codex #1516 R2 Medium)
    if (!process.env.LOGIZARD_USER_ID || !process.env.LOGIZARD_PASSWORD) throw new Error('ロジザードの ID かパスワードが .env に無い');
    common.acquireLock({ name: 'logizard-session.lock' });   // 取れない = その場で終わる (bat は最大 10 分待ってから呼ぶ)
    // 共通部品の launchBrowser などが process.exit しても鍵を返す (finally は通らないが exit の処理は走る。auto-barcode.js と同じ)
    const releaseOnExit = () => { try { common.releaseLock(); } catch { /* */ } };
    process.once('exit', releaseOnExit);
    try {
      // ブラウザの起動も鍵を返す finally の中 (起動に失敗しても鍵を残さない。Codex #1516 R1 Medium)
      const headless = (process.env.LOGIZARD_HEADLESS || '0') === '1';   // ほかの miniPC のロジザードの自動化と同じ決まり (.env を読んだ後に見る)
      const { browser, page } = await common.launchBrowser({ headless });
      try {
        await common.login(page, { label });
        const dlDir = path.join(automationDir, 'downloads');
        // 同じページを全部の操作に渡す (1 つのセッション = 取込の前後の書き出しが同じログインの中)
        const ops = {
          exportShohin: () => exportShohinMaster(page, { dlDir, minRows: 100 }),
          previewImport: (csvPath, o = {}) => screen.previewImport(page, { csvPath, ...o }),
        };
        if (allowExecute) ops.executeImport = (o) => screen.executeImport(page, o);
        if (barcode) ops.exportBarcodes = () => barcode.exportBarcodeMaster(page, { dlDir });
        return await fn(Object.freeze(ops));
      } finally {
        await browser.close().catch(() => {});
      }
    } finally {
      common.releaseLock();
      process.removeListener('exit', releaseOnExit);
    }
  };
  return { withSession, capabilities };
}
