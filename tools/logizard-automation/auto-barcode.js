/**
 * ロジザード自動化: 入荷バーコード発行の連携 (① ② の 2 ステップ)
 * v0.1 (2026-07-27 作成。auto-hokyu.js / auto-zaiko.js / auto-nyuka.js を雛形に構成)
 * マスタ正本切替の切替の PR (L-23): ③ 毎日の商品マスタの取込 (GAS の CSV) を外した。毎日の商品マスタは miniPC の自動 (00:20)。
 *   GAS の ③ に戻すのは台帳 lz-gas-rollback の固定の版 (tag) を配るときだけ (このファイルに ③ を足し戻さない)。
 *
 * フロー:
 *   ① インポート[PM07/FM07_01]  ファイル種類=商品マスタ / 取込パターン=新商品バーコード登録
 *        ← G:\共有ドライブ\入荷バーコード発行\ロジザードアップロード\logizard_bc_upload.csv
 *   ② エクスポート[PM08/FM08_01] 種類=SKU / 抽出パターン=バーコード情報
 *        対象日=更新日 (N日前〜今日, 既定30日) / 対象データ=有効マスタ+無効マスタ
 *        → G:\共有ドライブ\入荷バーコード発行\バーコードマスタ.csv に上書き (検証NGなら既存温存)
 *
 * 使い方:
 *   node auto-barcode.js         … 本番実行 (①→②。途中で失敗したら以降は実行しない)
 *   node auto-barcode.js --dry   … 各画面の条件設定まで行い、実行ボタンは一切押さない試走。
 *                                  FM08画面のHTML/状態を captures/ に採取する (初回のセレクタ検証用)
 *   run-barcode.bat              … Stream Deck から叩く入口
 *   node auto-barcode.js --show-mode … 配った版の読み戻し (①② だけの見出しを出して終わる。ログイン・CSV・鍵・ブラウザに触らない)
 *   ※ 引数は --dry と --show-mode だけ (知らない引数は断る)。
 *   ※ ① ② の画面の手順は barcode-steps.js (2026-10-17〜。ポータルの「ロジザードに取り込む」ボタン = miniPC の bc-import-runner.js と同じ手順)。
 *     ボタンの道ができたので、この CLI (Stream Deck) は予備 (ボタンで取り込んだファイルをここでもう一度取り込まない)。
 *
 * マスタ正本切替 ③c-1b-3a (2026-09-28・決まりは barcode-mode.js):
 *   - JST 00:00〜01:30 は動かない (始めない・各ステップと実行ボタンの直前でも時刻を見る)。
 *     miniPC の毎日の商品マスタの取込 (00:15〜00:55) と同じ共通アカウントのため。
 *   - 切替の PR (L-23) から ①② だけ (設定に依らない)。前の設定 LOGIZARD_BC_DAILY が残っていても ③ はしない (消してよいと出す)。
 *
 * 安全設計:
 *   - 取込はフェイルクローズ: 完了確認 (インポート結果モーダルのエラー件数=0) が取れなければ後続に進まない。
 *     実行ボタンを押した後のセッション切れは再試行しない (取込済みか不明なため人が履歴を確認)
 *   - ②のFM08画面は実機DOM未採取のため、ID直指定→「選択肢/ラベル/行テキスト」からの自動特定の順で探し、
 *     見つからない・複数一致・設定後の読み戻し不一致はすべて中止する (誤った条件で出力しない)
 *   - バーコードマスタ.csv の上書きは zaiko と同じ多段検証つき:
 *     Shift-JIS厳密デコード / CSVパース / 必須列(商品ID・バーコード) / 全行の列数一致 / 行数下限。
 *     1つでも落ちたら既存ファイルを温存して FAILED。書き込みは一時ファイル→rename
 *   - 取込CSVは実行前にローカル検証 (存在 / 更新からの経過時間 / Shift-JIS / データ行>=1)。
 *     古いファイルの誤再取込を防ぐ (閾値は LOGIZARD_BC_MAX_AGE_HOURS、0で無効)
 *   - ログインは LOGIZARD_BC_USER_ID/PASSWORD があればそれを使い専用ロック。
 *     未設定なら共通アカウント (LOGIZARD_USER_ID) + 共通ロック (在庫/値札CSVと直列化)
 *
 * 終了コード: 0=SUCCESS/DRY_RUN, 1=FAILED
 * 設計書: G:\共有ドライブ\AI_reference\システム設計\ロジザード作業自動化\入荷バーコード連携_設計メモ_20260727.md
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import {
  LOG_DIR,
  loadEnv, requireIntEnv, acquireLock, releaseLock, launchBrowser, login,
  errorShot, writeResult,
  assertLocalWriteDirs, assertNoLogizardBrowserOpen,
} from './logizard-common.js';
import { parseCsv } from './csv-util.js';
import { resolveBarcodeMode, inNightBlock, nightBlockMessage, assertOutsideNightBlock, clickBudgetMs, asNightError } from './barcode-mode.js';
// ① ② の画面の手順 (dialog の扱い・夜の止めの押し方を含む) は barcode-steps.js (ポータルのボタンの道 bc-import-runner.js と同じ手順。2026-10-17)
import { createBarcodeSteps, makeWithRelogin, IMPORT1_PATTERN_DEFAULT } from './barcode-steps.js';

// 起動の形 (③c-1b-3a)。引数の確かめ → 配った版の読み戻し → 夜の止め は .env より前 (.env・CSV・鍵・ブラウザに触る前。Codex #1558 R3 Low)
let MODE;
try {
  MODE = resolveBarcodeMode({ env: {} });   // 引数だけ (.env の前の設定の注意は .env を読んだ後に作る)
} catch (e) {
  console.error(`❌ ${e.message}`);
  process.exit(1);
}
// 配った版の読み戻し (切替の手順 7・Codex #1558 R2 High): 何にも触らずに見出しを出して終わる (夜でも・.env が無くても)
if (MODE.showMode) {
  console.log(`ℹ ${MODE.label}`);
  console.log('ℹ ③ 毎日の商品マスタの取込: この版には無い (切替済み)');
  process.exit(0);
}
if (inNightBlock()) {
  console.error(`❌ ${nightBlockMessage()}`);
  process.exit(1);
}
loadEnv();
assertLocalWriteDirs();
MODE = resolveBarcodeMode();   // .env を読んだ後 = 前の設定 LOGIZARD_BC_DAILY の注意 (notes)
const DRY_RUN = MODE.dry;

// ---- 設定 (.env で上書き可) ----
const IMPORT1_CSV = (process.env.LOGIZARD_BC_IMPORT1
  || 'G:\\共有ドライブ\\入荷バーコード発行\\ロジザードアップロード\\logizard_bc_upload.csv').trim();
const EXPORT_OUT = (process.env.LOGIZARD_BC_OUT
  || 'G:\\共有ドライブ\\入荷バーコード発行\\バーコードマスタ.csv').trim();

const IMPORT_FILETYPE_LABEL = process.env.LOGIZARD_BC_FILETYPE || '商品マスタ';
const IMPORT1_PATTERN = process.env.LOGIZARD_BC_IMPORT1_PATTERN || IMPORT1_PATTERN_DEFAULT;
const EXPORT_TYPE_LABEL = process.env.LOGIZARD_BC_EXPORT_TYPE || 'SKU';
const EXPORT_PATTERN = process.env.LOGIZARD_BC_EXPORT_PATTERN || 'バーコード情報';

// エクスポート対象日 (更新日): N日前〜今日。手動運用の実績 (2026-07-27 スクショ) は約30日
const EXPORT_DAYS = requireIntEnv('LOGIZARD_BC_EXPORT_DAYS', 30, { min: 1, max: 365 });
// 取込CSVの鮮度ガード: 更新からこの時間を超えていたら中止 (0=チェックしない)。
// 既定120h=5日: 金曜夕方に生成→週明け実行の運用を通す (2026-07-27 初回実行で48hに引っかかった実績)。
// 「取込済みファイルの再取込」は下の内容ハッシュガードが本命で捕まえる
const MAX_AGE_HOURS = requireIntEnv('LOGIZARD_BC_MAX_AGE_HOURS', 120, { min: 0, max: 8760 });
// 取込済みCSVの再取込ガード: 前回成功時と内容が同一なら中止 (1で無効化)
const ALLOW_REIMPORT = (process.env.LOGIZARD_BC_ALLOW_REIMPORT || '0') === '1';
// エクスポートCSVのデータ行数下限 (これ未満なら既存バーコードマスタを温存して失敗扱い)
const MIN_ROWS = requireIntEnv('LOGIZARD_BC_MIN_ROWS', 1, { min: 0, max: 10000000 });
const HEADLESS = (process.env.LOGIZARD_HEADLESS || '0') === '1';

// ---- アカウント: 専用IDがあれば専用ロック、なければ共通アカウント+共通ロック ----
const BC_USER = (process.env.LOGIZARD_BC_USER_ID || '').trim();
const BC_PASS = process.env.LOGIZARD_BC_PASSWORD || '';
const useDedicated = !!(BC_USER && BC_PASS);

// ---- 取込済みCSVの記録 (同じ内容の二度取込を防ぐ。logs/barcode_imported.json) ----
const IMPORTED_STATE = path.join(LOG_DIR, 'barcode_imported.json');
function readImportedState() {
  try { return JSON.parse(fs.readFileSync(IMPORTED_STATE, 'utf8')); } catch { return {}; }
}
function recordImported(stepKey, info) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  const s = readImportedState();
  s[stepKey] = info;
  fs.writeFileSync(IMPORTED_STATE, JSON.stringify(s, null, 2), 'utf8');
}

// ---- 取込CSVの実行前ローカル検証 (ブラウザを開く前に落とす) ----
function precheckImportCsv(fullPath, what, stepKey) {
  if (!fs.existsSync(fullPath)) {
    throw new Error(`${what} がありません: ${fullPath}\n   (Gドライブ未接続、または生成ツール未実行の可能性)`);
  }
  const st = fs.statSync(fullPath);
  const ageHours = (Date.now() - st.mtimeMs) / 3600000;
  const mtimeJst = new Date(st.mtimeMs + 9 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 16);
  if (MAX_AGE_HOURS > 0 && ageHours > MAX_AGE_HOURS) {
    throw new Error(
      `${what} が古すぎます (更新 ${mtimeJst} JST = ${Math.round(ageHours)}時間前 > 上限${MAX_AGE_HOURS}時間)。\n` +
      '   古いファイルを再取込するとロジザード側の新しい商品情報を巻き戻す恐れがあるため中止しました。\n' +
      '   生成ツールで作り直すか、意図的なら .env の LOGIZARD_BC_MAX_AGE_HOURS を調整してください。'
    );
  }
  const body = fs.readFileSync(fullPath);
  if (!body.length) throw new Error(`${what} が空ファイルです: ${fullPath}`);
  let text;
  try {
    text = new TextDecoder('shift_jis', { fatal: true }).decode(body);
  } catch {
    throw new Error(`${what} をShift-JISとして読めません: ${fullPath}\n   (生成ツールの文字コード設定を確認してください。ロジザード取込はSHIFT-JIS前提)`);
  }
  let rows;
  try {
    rows = parseCsv(text).rows;
  } catch (e) {
    throw new Error(`${what} のCSV解析に失敗: ${e.message} (${fullPath})`);
  }
  if (rows.length < 2) {
    throw new Error(`${what} に取り込むデータ行がありません (${rows.length}行のみ): ${fullPath}`);
  }
  // 内容ハッシュによる二度取込ガード (本命)。前回成功時と同一内容なら取込済みとみなして
  // そのステップをスキップする (途中失敗からの再実行で、済んだ取込を繰り返さないため)
  const hash = crypto.createHash('sha256').update(body).digest('hex');
  const prev = readImportedState()[stepKey];
  const alreadyImported = !ALLOW_REIMPORT && !!prev && prev.sha256 === hash;
  console.log(`📥 ${what}: ${fullPath}`);
  console.log(`   └ 更新 ${mtimeJst} (JST) / データ${rows.length - 1}行`);
  if (alreadyImported) {
    console.log(`   └ ℹ 前回取込済み (${prev.importedAt}) と同一内容 → このステップはスキップします`);
    console.log('      (もう一度取り込みたい場合は .env に LOGIZARD_BC_ALLOW_REIMPORT=1)');
  }
  return { rows: rows.length - 1, mtimeJst, hash, alreadyImported, importedAt: prev ? prev.importedAt : null };
}

// ---- メイン前処理 ----
const startedAt = Date.now();
console.log(`===== 入荷バーコード連携 ${DRY_RUN ? '(--dry 試走)' : ''} =====`);
console.log(`ℹ ${MODE.label}`);
for (const n of MODE.notes) console.log(`ℹ ${n}`);
let pre1;
try {
  pre1 = precheckImportCsv(IMPORT1_CSV, '①取込CSV (新商品バーコード)', 'import1');
} catch (e) {
  console.error(`❌ ${e.message}`);
  process.exit(1);
}
if (!fs.existsSync(path.dirname(EXPORT_OUT))) {
  console.error(`❌ ②の保存先フォルダがありません: ${path.dirname(EXPORT_OUT)}`);
  process.exit(1);
}

acquireLock(useDedicated ? { name: 'logizard-barcode.lock' } : {});
process.on('exit', releaseLock);
assertNoLogizardBrowserOpen();

const { browser, page } = await launchBrowser({ headless: HEADLESS });
const steps = createBarcodeSteps({
  page, dry: DRY_RUN,
  cfg: { importFiletypeLabel: IMPORT_FILETYPE_LABEL, exportTypeLabel: EXPORT_TYPE_LABEL, exportPattern: EXPORT_PATTERN, exportDays: EXPORT_DAYS, minRows: MIN_ROWS, exportOut: EXPORT_OUT },
});
const { runImport, runExport, assertNoUnexpectedDialog } = steps;

// =====================================================================
// メイン
// =====================================================================
// ログインの設定。ログインのボタンを押す直前 (共通部品の中のリトライも) に夜の止めを見る (③c-1b-3a・Codex #1518 R1)
const loginOpts = () => ({
  ...(useDedicated ? { userId: BC_USER, password: BC_PASS, label: 'バーコード連携用アカウント' } : {}),
  beforeSubmit: () => clickBudgetMs('ログインのボタンの前'),   // 押す持ち時間を返す (共通部品が click の timeout にする)
});
// 各ステップとも「実行ボタンを押す前のセッション切れ」だけ1回再ログインして再試行する (barcode-steps.js)
const withRelogin = makeWithRelogin({ page, loginOpts });


// 夜の止め (③c-1b-3a) の見る所 = 起動の直後・ログインのボタンの前 (再ログイン・リトライも)・各ステップの前・
//   実行ボタンの前・取込 / 書き出しを始める確認の OK の前・ブラウザの確認 dialog の承認。
//   ボタンは「次の 00:00 の 2 秒前」までの持ち時間で押す (押せるようになるまでの待ちで 00:00 を越えない)。
//   00:00 をまたいだ後に残るのは、00:00 より前に始めた処理の結果の待ち (最長 180 秒) と後始末だけ。
//   miniPC の自動の取込は 00:15 から = 15 分の余白。
const result = { import1: null, export: null };
try {
  assertOutsideNightBlock('ログインの前');
  await login(page, loginOpts()).catch((err) => { throw asNightError(err, 'ログインのボタンの前'); });
  if (!useDedicated) console.log('ℹ 共通アカウントでログイン (専用にする場合は .env の LOGIZARD_BC_USER_ID/PASSWORD)');

  const jstNow = () => new Date(Date.now() + 9 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 16) + ' (JST)';
  assertOutsideNightBlock('①の前');
  if (pre1.alreadyImported) {
    console.log(`⏭ ①: 前回取込済み (${pre1.importedAt}) のためスキップ`);
    result.import1 = { status: 'SKIPPED', summary: `前回取込済み ${pre1.importedAt}` };
  } else {
    result.import1 = await withRelogin('①', () => runImport(IMPORT1_CSV, IMPORT1_PATTERN, '①新商品バーコード登録'));
    if (!DRY_RUN) recordImported('import1', { sha256: pre1.hash, path: IMPORT1_CSV, csvMtime: pre1.mtimeJst, importedAt: jstNow() });
  }
  assertOutsideNightBlock('②の前');
  result.export = await withRelogin('②', () => runExport());
  // ③ 毎日の商品マスタの取込は無い (切替の PR・L-23。miniPC の自動 00:20 が取り込む)

  assertNoUnexpectedDialog();
  result.status = DRY_RUN ? 'DRY_RUN' : 'SUCCESS';
  result.import1Csv = { path: IMPORT1_CSV, ...pre1 };
  result.elapsedSec = Math.round((Date.now() - startedAt) / 1000);
  writeResult('barcode', result);

  console.log('\n===== 結果 =====');
  console.log(`① 新商品バーコード取込 : ${result.import1.status}${result.import1.summary ? ` (${result.import1.summary})` : ''}`);
  console.log(`② バーコードマスタ出力 : ${result.export.status}${result.export.rows != null ? ` (${result.export.rows}行)` : ''}`);
  console.log('③ 毎日の商品マスタ     : この道具ではしない (miniPC の自動が毎晩取り込む)');
  console.log(`所要 ${result.elapsedSec}秒`);
  console.log(DRY_RUN ? '🧪 試走完了 (ロジザードには何も登録していません)' : '🎉 完了');
} catch (e) {
  const detail = e && e.message ? e.message : String(e);
  console.error('\n❌ 失敗:', detail);
  const done = [
    result.import1 ? '①済' : '①未',
    result.export ? '②済' : '②未',
  ].join(' ');
  console.error(`   進行状況: ${done} — 失敗したステップ以降は実行していません。`);
  await errorShot(page, 'bc-fatal');
  const night = !!(e && e.nightBlock) || steps.nightDialogSeen();   // 夜の止めで dialog を承認しなかった後の失敗も夜の止め
  if (night) console.error('   (夜の止め = 押す前に止めた。01:30 を過ぎてからもう一度押せば、済んだステップは同じ中身なら飛ばして続きから)');
  writeResult('barcode', {
    status: e && e.targetLocked ? 'TARGET_LOCKED' : night ? 'NIGHT_BLOCK' : 'FAILED',
    detail, progress: done, ...result,
    elapsedSec: Math.round((Date.now() - startedAt) / 1000),
  });
  process.exitCode = 1;
} finally {
  await browser.close().catch(() => {});
}
process.exit(process.exitCode || 0);
