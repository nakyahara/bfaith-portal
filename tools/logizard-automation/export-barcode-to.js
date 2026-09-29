/**
 * export-barcode-to.js — SKU のバーコード情報の全件を、好きな場所へ書き出すだけ (barcode-export.js の実機の確かめ・③c-1b の少数件の試験)
 *
 *   node export-barcode-to.js --out <保存先のファイル> [--dry]
 *
 * auto-barcode.js の ② との違い: **バーコードマスタ.csv (共有ドライブ) には触らない**・直近 N 日ではなく全件 (登録日・開始日なし)。
 * ロジザードは照会 (エクスポート) だけ = 業務データを変えない。セッションの鍵 (logizard-session.lock) は同じものを取る。
 * 保存先がすでにある = 断る (上書きしない)。--dry = 条件の設定まで (実行ボタンを押さない・保存しない)。
 * 終了コード: 0 = 書き出せた (または --dry で条件まで), 1 = 失敗
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { loadEnv, launchBrowser, login, acquireLock, releaseLock, assertLocalWriteDirs, DIR } from './logizard-common.js';
import { exportBarcodeMaster } from './barcode-export.js';

loadEnv();

const i = process.argv.indexOf('--out');
const OUT = i > 0 ? String(process.argv[i + 1] || '').trim() : '';
const DRY = process.argv.includes('--dry');
const HEADLESS = (process.env.LOGIZARD_HEADLESS || '0') === '1';
const MIN_ROWS = Math.max(1, Number(process.env.LOGIZARD_BARCODE_MIN_ROWS || 100) || 100);
const log = (...a) => console.log(...a);

if (!DRY && !OUT) { console.error('❌ --out <保存先のファイル> が要る'); process.exit(1); }
if (OUT && fs.existsSync(OUT)) { console.error(`❌ 保存先がすでにある (上書きしない): ${OUT}`); process.exit(1); }
if (OUT && /バーコードマスタ\.csv$/i.test(OUT)) { console.error('❌ バーコードマスタ.csv (② の出力) には書かない'); process.exit(1); }

let locked = false;
try {
  assertLocalWriteDirs();
  acquireLock({ name: 'logizard-session.lock' });
  locked = true;
  const { browser, page } = await launchBrowser({ headless: HEADLESS });
  try {
    await login(page, { label: 'バーコードの書き出し (確かめ)' });
    const r = await exportBarcodeMaster(page, { dlDir: path.join(DIR, 'downloads'), minRows: MIN_ROWS, dry: DRY, log });
    if (!r.dry) {
      fs.mkdirSync(path.dirname(path.resolve(OUT)), { recursive: true });
      fs.writeFileSync(OUT, r.buf, { flag: 'wx' });
      log(`💾 ${OUT} (${r.v.dataRows} 行 / ${r.v.header.length} 列: ${r.v.header.join(',')} / sha256 ${crypto.createHash('sha256').update(r.buf).digest('hex')})`);
    }
  } finally {
    await browser.close().catch(() => {});
  }
} catch (e) {
  console.error(`❌ 失敗: ${e.message}`);
  process.exitCode = 1;
} finally {
  if (locked) releaseLock();
}
