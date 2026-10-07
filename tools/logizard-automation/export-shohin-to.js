/**
 * export-shohin-to.js — 商品マスタの全件を、好きな場所へ書き出すだけ (shohin-export.js の実機の確かめ・③c-1b の少数件の試験)
 *
 *   node export-shohin-to.js --out <保存先のファイル>
 *
 * auto-shohin-csv.js との違い: **本番の保存先 (LOGIZARD_SHOHIN_CSV_OUT)・Drive への転送・その日の成功の印には触らない**。
 * ロジザードは照会 (エクスポート) だけ = 業務データを変えない。セッションの鍵 (logizard-session.lock) は同じものを取る。
 * 保存先がすでにある = 断る (上書きしない)。
 * 終了コード: 0 = 書き出せた, 1 = 失敗
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { loadEnv, launchBrowser, login, acquireLock, releaseLock, assertLocalWriteDirs, DIR } from './logizard-common.js';
import { exportShohinMaster } from './shohin-export.js';

loadEnv();

const i = process.argv.indexOf('--out');
const OUT = i > 0 ? String(process.argv[i + 1] || '').trim() : '';
const HEADLESS = (process.env.LOGIZARD_HEADLESS || '0') === '1';
const MIN_ROWS = Math.max(1, Number(process.env.LOGIZARD_SHOHIN_CSV_MIN_ROWS || 100) || 100);
const log = (...a) => console.log(...a);

if (!OUT) { console.error('❌ --out <保存先のファイル> が要る'); process.exit(1); }
if (fs.existsSync(OUT)) { console.error(`❌ 保存先がすでにある (上書きしない): ${OUT}`); process.exit(1); }
const prod = (process.env.LOGIZARD_SHOHIN_CSV_OUT || '').trim();
if (prod && path.resolve(prod).toLowerCase() === path.resolve(OUT).toLowerCase()) { console.error('❌ 本番の保存先には書かない'); process.exit(1); }

let locked = false;
try {
  assertLocalWriteDirs();
  acquireLock({ name: 'logizard-session.lock' });
  locked = true;
  const { browser, page } = await launchBrowser({ headless: HEADLESS });
  try {
    await login(page, { label: '商品マスタの書き出し (確かめ)' });
    const r = await exportShohinMaster(page, { dlDir: path.join(DIR, 'downloads'), minRows: MIN_ROWS, log });
    fs.mkdirSync(path.dirname(path.resolve(OUT)), { recursive: true });
    fs.writeFileSync(OUT, r.buf, { flag: 'wx' });
    log(`💾 ${OUT} (${r.v.dataRows} 行 / ${r.v.header.length} 列 / sha256 ${crypto.createHash('sha256').update(r.buf).digest('hex')})`);
  } finally {
    await browser.close().catch(() => {});
  }
} catch (e) {
  console.error(`❌ 失敗: ${e.message}`);
  process.exitCode = 1;
} finally {
  if (locked) releaseLock();
}
