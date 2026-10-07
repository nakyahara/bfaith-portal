/**
 * ロジザード自動化: エクスポート[FM08_01] → 種類=商品 / パターン=デフォルト → 商品マスタCSV
 * v0.1 (2026-09-01。auto-nyuka-csv.js / auto-nefuda.js を雛形に作成)
 * v0.2 (2026-09-28。画面の手順と CSV の検証を shohin-export.js に切り出した = 動きは同じ。マスタ正本切替 ③c-1b の取込が同じ手順を使う)
 * 正本 = bfaith-portal の tools/logizard-automation/ (各 PC へは deploy.mjs で写す)
 *
 * ⭐なぜこれが要るか (中原さん 2026-09-01):
 *   入荷受付チェック (iPad) は「期限管理商品なら確認のたびに有効期限を聞く」作りだが、
 *   **期限管理あり/なしの設定は入荷受付CSV [FA04_01] に出てこない** (58列を実測)。
 *   在庫データの有効期限から推定していたが、在庫ゼロの商品は在庫CSVに行が無く推定できない。
 *   → 「エクスポート[FM08_01]の商品のデフォルトをDLして有効期限区分を参照したら取れる」
 *
 * 使い方:
 *   .env に LOGIZARD_SHOHIN_CSV_OUT (保存先) を書いて  node auto-shohin-csv.js
 *   条件設定までの確認だけしたいときは  node auto-shohin-csv.js --dry
 *   その日すでに成功していれば何もしない             node auto-shohin-csv.js --once-per-day
 *
 * ⭐--once-per-day があるおかげで、入荷受付CSV の既存タスク (08:40 / 11:45) に
 *   1ステップ足すだけで済む (専用の定期実行を新設しない = 入口を増やさない)。
 *   商品マスタは日に何度も変わるものではないので1日1回で足りる。
 *
 * env:
 *   LOGIZARD_USER_ID / LOGIZARD_PASSWORD          … 既存と共通
 *   LOGIZARD_SHOHIN_CSV_OUT                       … 必須。保存先 (このPC内。Gドライブ直書き禁止)
 *   LOGIZARD_SHOHIN_CSV_RCLONE_DEST               … 任意。例 gdrive-nefuda:shohin_master.csv
 *   LOGIZARD_SHOHIN_CSV_MIN_ROWS                  … 任意。これ未満の行数なら失敗扱い (既定 100)
 *
 * 安全設計 (既存CSVを壊さないことを最優先。auto-nyuka-csv.js と同じ考え方):
 *   - 書き込み先は LOGIZARD_SHOHIN_CSV_OUT 1ファイルのみ
 *   - CSV を実際にパースして検証: Shift-JIS で厳密にデコードできる / 全行の列数がヘッダ一致 /
 *     必須列 (商品ID・有効期限区分) がある / 行数が下限以上 / 前回より半減していない
 *   - 1つでも落ちたら既存ファイルを温存して FAILED (空・部分CSV・エラーページで上書きしない)
 *   - 一時ファイルに書いて rename で差し替え
 *   - ロックは他のロジザード自動化と共有 (同一アカウントの同時ログインで追い出し合う事故を防ぐ)
 *   - エクスポートは照会系で業務データを変更しない → 失敗時は単純に再実行してよい
 *
 * 終了コード: 0=SUCCESS, 1=FAILED
 */
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import {
  loadEnv, launchBrowser, login, acquireLock, releaseLock, assertLocalWriteDirs, DIR,
} from './logizard-common.js';
import { exportShohinMaster, validateShohinCsv } from './shohin-export.js';

loadEnv();

const USER_ID = process.env.LOGIZARD_USER_ID;
const PASSWORD = process.env.LOGIZARD_PASSWORD;
const OUT_PATH = (process.env.LOGIZARD_SHOHIN_CSV_OUT || '').trim();
const RCLONE_DEST = (process.env.LOGIZARD_SHOHIN_CSV_RCLONE_DEST || '').trim();
const RCLONE_EXE = (process.env.RCLONE_EXE || 'C:\\tools\\rclone\\rclone.exe').trim();
const RCLONE_CONF = (process.env.RCLONE_CONF || 'C:\\tools\\rclone\\rclone.conf').trim();
const HEADLESS = (process.env.LOGIZARD_HEADLESS || '0') === '1';
const MIN_ROWS = Math.max(1, Number(process.env.LOGIZARD_SHOHIN_CSV_MIN_ROWS || 100) || 100);
const DRY = process.argv.includes('--dry');
const ONCE_PER_DAY = process.argv.includes('--once-per-day');
const STAMP_PATH = path.join(DIR, 'logs', 'shohin-last-success.txt');

/** JST の当日 (YYYY-MM-DD)。ロジザードの1日と揃える */
function jstToday() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' })
    .format(new Date());
}
function alreadyRanToday() {
  try { return fs.readFileSync(STAMP_PATH, 'utf8').trim() === jstToday(); } catch { return false; }
}
function markRanToday() {
  try {
    fs.mkdirSync(path.dirname(STAMP_PATH), { recursive: true });
    fs.writeFileSync(STAMP_PATH, jstToday());
  } catch (e) {
    // 記録できなくても取込自体は成功している。次の実行でもう一度取りに行くだけ
    log(`⚠️ 実行日の記録に失敗しました (${e.message})。次の実行でもう一度取得します`);
  }
}

if (!USER_ID || !PASSWORD) {
  console.error('❌ LOGIZARD_USER_ID / LOGIZARD_PASSWORD が未設定です');
  process.exit(1);
}
if (!OUT_PATH) {
  console.error('❌ LOGIZARD_SHOHIN_CSV_OUT (保存先) が未設定です');
  process.exit(1);
}

const DL_DIR = path.join(DIR, 'downloads');

const log = (...a) => console.log(...a);

function finalize(buf, { rows }) {
  const tmp = `${OUT_PATH}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, buf);
  fs.renameSync(tmp, OUT_PATH);
  log(`💾 保存: ${OUT_PATH} (${rows} 行)`);
  if (RCLONE_DEST) {
    if (!fs.existsSync(RCLONE_EXE)) throw new Error(`rclone が見つかりません: ${RCLONE_EXE}`);
    execFileSync(RCLONE_EXE, ['copyto', OUT_PATH, RCLONE_DEST, '--config', RCLONE_CONF, '--log-level', 'ERROR'],
      { timeout: 300000, shell: false });
    const out = execFileSync(RCLONE_EXE, ['size', RCLONE_DEST, '--config', RCLONE_CONF, '--json'],
      { encoding: 'utf8', timeout: 120000, shell: false });
    const remote = JSON.parse(out);
    if (Number(remote.bytes) !== buf.length) {
      throw new Error(`転送後のサイズが違います (ローカル ${buf.length} / Drive ${remote.bytes})`);
    }
    log(`☁️ 転送: ${RCLONE_DEST} (${remote.bytes} バイト・照合OK)`);
  }
  return { rows };
}

async function main() {
  assertLocalWriteDirs();
  fs.mkdirSync(DL_DIR, { recursive: true });
  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });

  const { browser, page } = await launchBrowser({ headless: HEADLESS });
  try {
    await login(page, { userId: USER_ID, password: PASSWORD, label: '商品マスタCSV' });

    // ── 書き出し (画面の手順と CSV の検証 = shohin-export.js) ──
    const r = await exportShohinMaster(page, { dlDir: DL_DIR, minRows: MIN_ROWS, dry: DRY, log });
    if (r.dry) return { dry: true };
    const { buf, v } = r;

    // ── 前回と比べる → 保存 ──
    const prev = fs.existsSync(OUT_PATH) ? validateShohinCsv(fs.readFileSync(OUT_PATH), { minRows: MIN_ROWS }) : null;
    if (prev?.ok && v.dataRows * 2 < prev.dataRows) {
      throw new Error(`商品数が前回の半分未満です (${prev.dataRows} → ${v.dataRows} 行)。`
        + '抽出条件の事故が疑われるため中止しました (既存CSVは温存)');
    }
    log(`📄 ${v.dataRows} 行 / ${v.header.length} 列`);
    // ⭐有効期限区分の内訳を毎回ログに出す。ロジザード側の表記が変わったら気付けるようにする
    log(`📅 有効期限区分: ${Object.entries(v.kubunCounts).map(([k, n]) => `${k}=${n}`).join(' / ')}`);
    return finalize(buf, { rows: v.dataRows });
  } finally {
    await browser.close().catch(() => {});
  }
}

// ⚠acquireLock の戻り値に依存しない (以前 undefined を返していて解放が走らなかったため)。
// ここに来た時点でロックは自分のものなので、finally では無条件に解放する
let locked = false;
try {
  // ⭐ロックを取る前に判定する (その日もう終わっているならロジザードに触らない)
  if (ONCE_PER_DAY && alreadyRanToday()) {
    log(`⏭ 今日 (${jstToday()}) はすでに取得済みのため何もしません`);
    process.exit(0);
  }
  acquireLock({ name: 'logizard-session.lock' });
  locked = true;
  const r = await main();
  if (r?.dry) log('🧪 dry-run 完了');
  else { markRanToday(); log(`✅ 完了 (${r.rows} 行)`); }
} catch (e) {
  console.error(`❌ 失敗: ${e.message}`);
  process.exitCode = 1;
} finally {
  if (locked) releaseLock();
}
