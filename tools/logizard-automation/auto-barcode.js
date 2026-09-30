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
  ORIGIN, BASE, DIR, LOG_DIR, SHOT_DIR, CAPTURE_DIR, RUN_ID, UNIQ,
  loadEnv, requireIntEnv, acquireLock, releaseLock, launchBrowser, login,
  sessionLost, SessionLostError, errorShot, writeResult, jstDate,
  visibleModalText, assertLocalWriteDirs, assertNoLogizardBrowserOpen,
} from './logizard-common.js';
import { parseCsv } from './csv-util.js';
import { resolveBarcodeMode, inNightBlock, nightBlockMessage, assertOutsideNightBlock, clickBudgetMs, asNightError } from './barcode-mode.js';

loadEnv();
assertLocalWriteDirs();

// 起動の形 (③c-1b-3a)。夜の止めは何よりも先に見る (CSV・鍵・ブラウザに触る前)
let MODE;
try {
  MODE = resolveBarcodeMode();
} catch (e) {
  console.error(`❌ ${e.message}`);
  process.exit(1);
}
// 配った版の読み戻し (切替の手順 7・Codex #1558 R2 High): 何にも触らずに見出しを出して終わる (夜でも)
if (MODE.showMode) {
  console.log(`ℹ ${MODE.label}`);
  console.log('ℹ ③ 毎日の商品マスタの取込: この版には無い (切替済み)');
  for (const n of MODE.notes) console.log(`ℹ ${n}`);
  process.exit(0);
}
if (inNightBlock()) {
  console.error(`❌ ${nightBlockMessage()}`);
  process.exit(1);
}
const DRY_RUN = MODE.dry;

// ---- 設定 (.env で上書き可) ----
const IMPORT1_CSV = (process.env.LOGIZARD_BC_IMPORT1
  || 'G:\\共有ドライブ\\入荷バーコード発行\\ロジザードアップロード\\logizard_bc_upload.csv').trim();
const EXPORT_OUT = (process.env.LOGIZARD_BC_OUT
  || 'G:\\共有ドライブ\\入荷バーコード発行\\バーコードマスタ.csv').trim();

const IMPORT_FILETYPE_LABEL = process.env.LOGIZARD_BC_FILETYPE || '商品マスタ';
const IMPORT1_PATTERN = process.env.LOGIZARD_BC_IMPORT1_PATTERN || '新商品バーコード登録';
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

const { browser, context, page } = await launchBrowser({ headless: HEADLESS });

// ネイティブdialog: 実行/出力系のconfirmのみ承認。それ以外はdismissして必ずFAILEDにする
const DIALOG_CONFIRM_OK = /(実行|出力|取込|取り込み|インポート|エクスポート|アップロード|ダウンロード)[^。]{0,20}(よろしい|しますか|開始します)/;
let unexpectedDialog = null;
let nightDialog = false;
page.on('dialog', async (d) => {
  const msg = d.message();
  if (inNightBlock()) {
    // 夜の止め (③c-1b-3a): 実行を始める確認でも承認しない
    unexpectedDialog = `[${d.type()}] ${msg.slice(0, 200)}`;
    nightDialog = true;
    console.log(`🛑 夜の止め (00:00〜01:30) のため dialog を承認しない ${unexpectedDialog} → dismiss`);
    await d.dismiss().catch(() => {});
  } else if (d.type() === 'confirm' && DIALOG_CONFIRM_OK.test(msg)) {
    console.log(`💬 confirm "${msg.slice(0, 120)}" → accept`);
    await d.accept().catch(() => {});
  } else {
    unexpectedDialog = `[${d.type()}] ${msg.slice(0, 200)}`;
    console.log(`🛑 想定外dialog ${unexpectedDialog} → dismiss (実行はFAILEDになります)`);
    await d.dismiss().catch(() => {});
  }
});
// 処理を始めるボタン (実行・始める確認の OK・ログイン) を押す: 持ち時間 = 次の 00:00 の 2 秒前まで (押せるようになるまでの待ちも含む)。
// 持ち時間切れ・00:00 の直前 = 押さずに夜の止め (Codex #1518 R2)
async function nightClick(target, where) {
  const timeout = clickBudgetMs(where);
  try {
    await (typeof target === 'string' ? page.click(target, { timeout }) : target.click({ timeout }));
  } catch (e) {
    throw asNightError(e, where);
  }
}
function assertNoUnexpectedDialog() {
  if (!unexpectedDialog) return;
  const e = new Error(`${nightDialog ? nightBlockMessage(new Date(), 'dialog の承認') + ' ' : ''}想定外ダイアログが発生: ${unexpectedDialog}`);
  if (nightDialog) e.nightBlock = true;
  throw e;
}

// ---- 処理中オーバーレイ/エラーモーダルの待機 (auto-hokyu.js と同じ判定) ----
async function waitOverlayGone(label, timeoutMs) {
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
    await errorShot(page, `bc-overlay-timeout-${label}`);
    throw new Error(`${label}: 処理中表示が${Math.round(timeoutMs / 1000)}秒消えません`);
  }
  if (typeof res === 'string' && res.startsWith('MODAL:')) {
    await errorShot(page, `bc-modal-${label}`);
    throw new Error(`${label}: ロジザードがモーダル表示: ${res.slice(6)}`);
  }
}

// ---- select を「表示文言の完全一致」で選ぶ (value直書きしない。画面の文言変更は明示エラーにする) ----
async function selectOptionByText(sel, label, what, timeoutMs = 20000) {
  const found = await page.waitForFunction(({ sel, label }) => {
    const s = document.querySelector(sel);
    if (!s) return false;
    const opt = [...s.options].find((o) => (o.textContent || '').trim() === label);
    return opt ? { v: opt.value } : false;
  }, { sel, label }, { timeout: timeoutMs }).then((h) => h.jsonValue()).catch(() => null);
  if (!found) {
    const opts = await page.evaluate((sel) => {
      const s = document.querySelector(sel);
      return s ? [...s.options].map((o) => (o.textContent || '').trim()).filter(Boolean) : null;
    }, sel);
    await errorShot(page, `bc-select-${what}`);
    throw new Error(`${what}: 選択肢「${label}」が見つかりません (現在の選択肢: ${opts ? opts.join(' / ') || '(空)' : 'select要素なし'})`);
  }
  await page.selectOption(sel, found.v);
  const now = await page.evaluate((sel) => {
    const s = document.querySelector(sel);
    const o = s && s.options[s.selectedIndex];
    return o ? (o.textContent || '').trim() : null;
  }, sel);
  if (now !== label) throw new Error(`${what}: 「${label}」を選択できませんでした (現在: ${now})`);
  console.log(`✔ ${what}: ${label}`);
  return found.v;
}

// =====================================================================
// ステップ① 商品マスタCSV取込 [PM07/FM07_01] (③ 毎日の商品マスタは切替の PR で外した) (セレクタは auto-hokyu.js で本番実証済み)
// =====================================================================
async function runImport(csvPath, patternLabel, stepName) {
  await page.goto(`${BASE}/PM07/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
  if (await sessionLost(page)) throw new SessionLostError(`${stepName}: PM07遷移時にセッション切れ`);

  await page.click(`a[onclick*="openFunctionBar('FM07_01')"]`, { timeout: 15000 }).catch(() => {});
  await page.waitForSelector('#FM07_01_executeBtn', { state: 'visible', timeout: 15000 });

  await selectOptionByText('#FM07_01_fileId', IMPORT_FILETYPE_LABEL, `${stepName}: ファイル種類`);
  await selectOptionByText('#FM07_01_ptrnId', patternLabel, `${stepName}: 取込パターン`);
  await waitOverlayGone(`${stepName}-取込パターン適用`, 30000);

  // 「ログイン倉庫のデータのみ取り込む」: 商品マスタでは無効化(グレーアウト)されている想定。
  // 操作できる状態ならONにし、できなければ状態をログに残すだけにする
  const onlyArea = await page.evaluate(() => {
    const el = document.querySelector('#FM07_01_onlyAreaImport');
    return el ? { exists: true, disabled: el.disabled, checked: el.checked } : { exists: false };
  });
  if (onlyArea.exists && !onlyArea.disabled && !onlyArea.checked) {
    await page.check('#FM07_01_onlyAreaImport');
    console.log('✔ ログイン倉庫のデータのみ取り込む: ON');
  } else {
    console.log(`ℹ ログイン倉庫のデータのみ取り込む: ${JSON.stringify(onlyArea)}`);
  }

  // プレビュー生成は非破壊 (実行ボタンを押すまで何も登録されない) ため、
  // サーバーの汎用エラー (2026-07-27 ③初回で実測「エラーが発生しました。サポートセンターへ〜」) は
  // モーダルを閉じて1回だけ再試行する
  await page.waitForTimeout(1000);
  for (let attempt = 1; ; attempt++) {
    try {
      await page.setInputFiles('#FM07_01_impFile', csvPath);
      await waitOverlayGone(`${stepName}-CSVプレビュー生成`, 120000);
      break;
    } catch (e) {
      if (attempt >= 2 || !/エラーが発生しました/.test(e.message || '')) throw e;
      console.log(`⚠ ${stepName}: プレビュー生成でサーバーエラー → モーダルを閉じて1回だけ再試行します`);
      await page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first().click().catch(() => {});
      await page.waitForTimeout(5000);
    }
  }
  assertNoUnexpectedDialog();

  if (DRY_RUN) {
    console.log(`🧪 ${stepName}: --dry のため実行ボタンは押しません (条件設定までは成功)`);
    return { status: 'DRY_RUN' };
  }

  // 実行前のフォーム領域テキストを基準として保存 (過去表示の誤検知防止。auto-hokyu R4と同じ)
  const baseline = await page.locator('#FM07_01_FORM').innerText().catch(() => '');
  await nightClick('#FM07_01_executeBtn', `${stepName} (実行ボタンの前)`);   // 夜の止め (③c-1b-3a)

  const confirmShown = await page.getByText('ファイルアップロードを開始します')
    .waitFor({ state: 'visible', timeout: 15000 }).then(() => true).catch(() => false);
  if (confirmShown) {
    // この OK で取込が始まる = 押す直前にも夜の止めを見る (実行ボタンの後に 00:00 をまたいだとき。夜の止めは握りつぶさない・Codex #1518 R1・R2)
    try {
      console.log('💬 アップロード開始確認モーダル → OK');
      await nightClick(page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first(), `${stepName} (確認の OK の前)`);
    } catch (e) {
      if (e && e.nightBlock) throw e;
      console.log('ℹ アップロード開始確認モーダルの OK を押せず (そのまま続行)');
    }
  } else {
    console.log('ℹ アップロード開始確認モーダルは表示されず (そのまま続行)');
  }

  // 完了/エラーの検知 (auto-hokyu.js と同一ロジック: 基準に無い新出文言のみ採用)
  const importOutcome = await page.waitForFunction((base) => {
    if (location.href.includes('/SUSPENDED')) return 'SESSION_LOST';
    const vis = (el) => {
      if (!el) return false;
      const s = getComputedStyle(el);
      if (s.display === 'none' || s.visibility === 'hidden' || +s.opacity === 0) return false;
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    };
    const dlgText = [...document.querySelectorAll('.ui-dialog, [class*="DIALOG"], [class*="dialog"], [id*="popup"]')]
      .filter(vis).map((d) => d.innerText).join('\n');
    const el = document.querySelector('#FM07_01_FORM');
    const t = (el ? el.innerText : (document.body ? document.body.innerText : '')) + '\n' + dlgText;
    const isNew = (s) => s && !base.includes(s);
    const result = t.match(/インポート結果[\s\S]{0,200}?エラー件数\s*[::]?\s*(\d+)/);
    if (result && isNew('インポート結果')) {
      const summary = result[0].replace(/\s+/g, ' ').slice(0, 200);
      return (+result[1] > 0 ? 'ERROR:' : 'OK:') + summary;
    }
    const err = t.match(/[^\n]*(エラー|失敗|不正|取込できません)[^\n]*/);
    if (err && isNew(err[0])) return 'ERROR:' + err[0].slice(0, 300);
    const ok = t.match(/(取込|インポート|登録|アップロード)[^\n]{0,30}(完了|終了|しました)[^\n]{0,40}/);
    if (ok && isNew(ok[0])) return 'OK:' + ok[0];
    return false;
  }, baseline, { timeout: 180000 }).then((h) => h.jsonValue()).catch(() => 'TIMEOUT');

  if (importOutcome === 'SESSION_LOST') {
    throw new Error(`${stepName}: 取込中にセッション切れ。取込されたか不明のため停止します。ロジザードのインポート履歴で確認してください。`);
  }
  if (importOutcome.startsWith('ERROR:')) {
    await errorShot(page, 'bc-import-error');
    throw new Error(`${stepName}: 取込エラー: ${importOutcome.slice(6, 220)}`);
  }
  if (importOutcome === 'TIMEOUT') {
    await errorShot(page, 'bc-import-unverified');
    throw new Error(`${stepName}: 取込の完了表示を180秒確認できず停止。ロジザードのインポート履歴で取込状態を確認してください (後続ステップは実行していません)。`);
  }
  assertNoUnexpectedDialog();
  const summary = importOutcome.slice(3, 200);
  console.log(`✅ ${stepName} 取込完了: ${summary}`);
  await page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first().click().catch(() => {});
  return { status: 'SUCCESS', summary };
}

// =====================================================================
// ステップ② SKUエクスポート [PM08/FM08_01] → バーコードマスタ.csv 上書き
// ⚠ この画面は実機DOM未採取。ID直指定 → 選択肢/ラベル/行テキストからの自動特定の順で探し、
//   特定できない・読み戻し不一致は必ず中止する (誤条件で出力しない)
// =====================================================================

// 選択肢に label を含む可視selectを1つだけ特定してマーカーを付ける
async function markSelectHavingOption(label, mark) {
  return page.evaluate(({ label, mark }) => {
    const sels = [...document.querySelectorAll('select')]
      .filter((s) => s.offsetParent !== null)
      .filter((s) => [...s.options].some((o) => (o.textContent || '').trim() === label));
    if (sels.length !== 1) return { count: sels.length };
    sels[0].setAttribute('data-autobc', mark);
    return { count: 1, id: sels[0].id || null };
  }, { label, mark });
}

// FM08のフォームはAJAXで遅れて読み込まれる (2026-07-27 実機: 展開直後は「Now Loading」で
// select自体がまだDOMに無い) ため、見つかるまでポーリングして待つ
async function resolveSelect(preferredId, optionLabel, what, mark, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  let last = { count: 0 };
  while (Date.now() < deadline) {
    if (await page.locator(preferredId).count()) return preferredId;
    // ID命名が想定と違う場合: その選択肢を持つ可視selectがちょうど1つならそれを使う
    last = await markSelectHavingOption(optionLabel, mark);
    if (last.count === 1) {
      console.log(`ℹ ${what}: ${preferredId} が無いため選択肢「${optionLabel}」を持つselectを自動特定 (id=${last.id || '(無)'})`);
      return `select[data-autobc="${mark}"]`;
    }
    if (await sessionLost(page)) throw new SessionLostError(`${what}: フォーム読込待ちにセッション切れ`);
    await page.waitForTimeout(1000);
  }
  await errorShot(page, `bc-resolve-${mark}`);
  throw new Error(`${what}: 対象のselectを特定できません (${Math.round(timeoutMs / 1000)}秒待機 / ${preferredId} 無し / 「${optionLabel}」を持つselect=${last.count}個)。error-shots/をClaudeに渡してください。`);
}

// FM08_01フォーム内のラジオ/チェックボックスを一括採取してマーカーを付ける
let gatherSeq = 0;
async function gatherToggles(contSel) {
  gatherSeq++;
  return page.evaluate(({ contSel, seq }) => {
    const cont = document.querySelector(contSel) || document.body;
    const labelFor = (el) => {
      if (el.id) {
        const l = document.querySelector(`label[for="${el.id}"]`);
        if (l) return (l.textContent || '').trim();
      }
      const pl = el.closest('label');
      if (pl) return (pl.textContent || '').trim();
      let t = '';
      let n = el.nextSibling;
      for (let i = 0; n && i < 3 && t.replace(/\s/g, '').length < 10; i++, n = n.nextSibling) {
        t += n.textContent || '';
      }
      return t.replace(/\s+/g, ' ').trim().split(' ')[0] || '';
    };
    const rowOf = (el) => {
      const r = el.closest('tr, li, dd, dl') || el.parentElement?.parentElement;
      return r ? (r.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 80) : '';
    };
    return [...cont.querySelectorAll('input[type="radio"], input[type="checkbox"]')]
      .filter((el) => el.offsetParent !== null)
      .map((el, i) => {
        const markVal = `g${seq}_${i}`;
        el.setAttribute('data-autobc-t', markVal);
        return {
          mark: markVal, id: el.id || null, name: el.name || null, value: el.value,
          type: el.type, checked: el.checked, disabled: el.disabled,
          label: labelFor(el), row: rowOf(el),
        };
      });
  }, { contSel, seq: gatherSeq });
}

// rowKey(行テキスト) と label(選択肢文言) で1つに絞ってチェックする。曖昧なら中止
async function ensureToggle(contSel, rowKey, label, what, { required = true } = {}) {
  let list = await gatherToggles(contSel);
  const pick = (l) => l.filter((t) => t.label === label && (!rowKey || t.row.includes(rowKey)));
  let hits = pick(list);
  if (hits.length !== 1) {
    if (!required) {
      console.log(`⚠ ${what}: 対象を特定できず (候補${hits.length}個)。画面初期値のまま進めます`);
      return null;
    }
    await errorShot(page, 'bc-toggle');
    const dump = list.map((t) => `[${t.type}] row="${t.row.slice(0, 30)}" label="${t.label}" checked=${t.checked}`).join('\n   ');
    throw new Error(`${what}: 「${rowKey} / ${label}」を1つに特定できません (候補${hits.length}個)。\n   画面上の候補:\n   ${dump}`);
  }
  if (!hits[0].checked) {
    await page.check(`[data-autobc-t="${hits[0].mark}"]`);
    // 再描画に備えて読み戻して検証する
    list = await gatherToggles(contSel);
    hits = pick(list);
    if (hits.length !== 1 || !hits[0].checked) {
      await errorShot(page, 'bc-toggle-verify');
      throw new Error(`${what}: 「${label}」をチェックできませんでした (読み戻し不一致)`);
    }
  }
  console.log(`✔ ${what}: ${label}`);
  return hits[0];
}

async function dumpFm08(contSel, tag) {
  try {
    const dir = path.join(CAPTURE_DIR, `${RUN_ID}_barcode`);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `fm08_${tag}.html`), await page.content(), 'utf8');
    await page.screenshot({ path: path.join(dir, `fm08_${tag}.png`), fullPage: true, timeout: 8000 });
    const toggles = await gatherToggles(contSel);
    fs.writeFileSync(path.join(dir, `fm08_${tag}_toggles.json`), JSON.stringify(toggles, null, 2), 'utf8');
    console.log(`📸 FM08画面を採取: captures/${RUN_ID}_barcode/fm08_${tag}.*`);
  } catch (e) {
    console.log(`⚠ FM08採取に失敗 (${e.message})`);
  }
}

async function runExport() {
  await page.goto(`${BASE}/PM08/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
  if (await sessionLost(page)) throw new SessionLostError('②: PM08遷移時にセッション切れ');

  // 機能バーを展開 (既に展開済みならリンクは存在しても再クリックで閉じないよう、
  // まずフォームの有無を見る。メニューリンク自体が id="FM08_01" を持つ点に注意)
  const formAlready = await page.evaluate(() => !!document.querySelector('#FM08_01_fileId'));
  if (!formAlready) {
    await page.click(`a[onclick*="openFunctionBar('FM08_01')"]`, { timeout: 15000 }).catch(() => {});
  }

  // 種類=SKU → 抽出パターン=バーコード情報 (フォームはAJAX読込のため長めに待つ)
  const typeSel = await resolveSelect('#FM08_01_fileId', EXPORT_TYPE_LABEL, '②: 種類', 'typeSel', 60000);
  await selectOptionByText(typeSel, EXPORT_TYPE_LABEL, '②: 種類');
  await waitOverlayGone('種類適用', 30000);
  const ptrnSel = await resolveSelect('#FM08_01_ptrnId', EXPORT_PATTERN, '②: 抽出パターン', 'ptrnSel');
  await selectOptionByText(ptrnSel, EXPORT_PATTERN, '②: 抽出パターン');
  await waitOverlayGone('抽出パターン適用', 30000);

  // 条件設定のスコープ: FM08_01_FORM があればそこ、無ければFM08_01系の先祖、最後はbody
  const contSel = await page.evaluate(() => {
    if (document.querySelector('#FM08_01_FORM')) return '#FM08_01_FORM';
    const el = document.querySelector('[id^="FM08_01"]');
    let cur = el;
    while (cur && cur !== document.body) {
      if (cur.querySelectorAll('input[type="radio"], input[type="checkbox"]').length >= 4) {
        if (!cur.id) cur.id = 'autobc_fm08_scope';
        return `#${cur.id}`;
      }
      cur = cur.parentElement;
    }
    return 'body';
  });

  // ファイル形式=SHIFT-JIS (selectの場合のみ明示。テキスト表示なら値を確認)
  const sjis = await markSelectHavingOption('SHIFT-JIS', 'sjisSel');
  if (sjis.count === 1) {
    await selectOptionByText('select[data-autobc="sjisSel"]', 'SHIFT-JIS', '②: ファイル形式');
  } else {
    console.log(`ℹ ②: ファイル形式selectは特定できず (候補${sjis.count}個)。画面初期値のまま進めます`);
  }

  // ヘッダ=あり / 囲い文字=あり / 区切り文字=カンマ (初期値どおりのはず。直せる範囲で明示する)
  await ensureToggle(contSel, 'ヘッダ', 'あり', '②: ヘッダ', { required: false });
  await ensureToggle(contSel, '囲い文字', 'あり', '②: 囲い文字', { required: false });
  await ensureToggle(contSel, '区切り文字', 'カンマ', '②: 区切り文字', { required: false });

  // 対象日=更新日 (必須。特定できなければ中止)
  await ensureToggle(contSel, '対象日', '更新日', '②: 対象日', { required: true });

  // 対象期間: N日前〜今日。日付欄は「YYYY/MM/DD値を持つ可視テキスト入力がちょうど2つ」で特定する
  const fromDate = jstDate(EXPORT_DAYS);
  const toDate = jstDate(0);
  const dates = await page.evaluate((contSel) => {
    const cont = document.querySelector(contSel) || document.body;
    const els = [...cont.querySelectorAll('input[type="text"]')]
      .filter((el) => el.offsetParent !== null && /^\d{4}\/\d{2}\/\d{2}$/.test(el.value));
    els.forEach((el, i) => el.setAttribute('data-autobc-d', String(i)));
    return els.map((el, i) => ({ i, id: el.id || null, value: el.value }));
  }, contSel);
  if (dates.length !== 2) {
    await errorShot(page, 'bc-dates');
    await dumpFm08(contSel, 'dates-fail');
    throw new Error(`②: 対象日の日付欄を特定できません (日付形式の入力が${dates.length}個)。--dry採取のcaptures/をClaudeに渡してください。`);
  }
  for (const [idx, val] of [[0, fromDate], [1, toDate]]) {
    await page.fill(`[data-autobc-d="${idx}"]`, val);
    await page.dispatchEvent(`[data-autobc-d="${idx}"]`, 'change').catch(() => {});
  }
  const datesAfter = await page.evaluate(() =>
    [0, 1].map((i) => document.querySelector(`[data-autobc-d="${i}"]`)?.value || null));
  if (datesAfter[0] !== fromDate || datesAfter[1] !== toDate) {
    await errorShot(page, 'bc-dates-verify');
    throw new Error(`②: 対象期間を設定できませんでした (画面値: ${datesAfter.join(' 〜 ')})`);
  }
  console.log(`✔ ②: 対象日=更新日 ${fromDate} 〜 ${toDate}`);

  // 対象データ: 有効マスタ + 無効マスタ (必須)
  await ensureToggle(contSel, '', '有効マスタ', '②: 対象データ(有効)', { required: true });
  await ensureToggle(contSel, '', '無効マスタ', '②: 対象データ(無効)', { required: true });

  // 実行ボタンの特定
  let exeSel = '#FM08_01_executeBtn';
  if (!(await page.locator(exeSel).count())) {
    const r = await page.evaluate(() => {
      const cands = [...document.querySelectorAll('input[type="button"], input[type="submit"], button, a')]
        .filter((el) => el.offsetParent !== null)
        .filter((el) => ((el.value || el.textContent || '').replace(/\s+/g, '')) === '実行');
      if (cands.length !== 1) return { count: cands.length };
      cands[0].setAttribute('data-autobc', 'exe');
      return { count: 1, id: cands[0].id || null };
    });
    if (r.count !== 1) {
      await errorShot(page, 'bc-exe-btn');
      await dumpFm08(contSel, 'exe-fail');
      throw new Error(`②: 実行ボタンを特定できません (「実行」候補${r.count}個)`);
    }
    exeSel = '[data-autobc="exe"]';
    console.log(`ℹ ②: 実行ボタンを自動特定 (id=${r.id || '(無)'})`);
  }

  if (DRY_RUN) {
    await dumpFm08(contSel, 'dry');
    console.log('🧪 ②: --dry のため実行ボタンは押しません (条件設定までは成功)');
    return { status: 'DRY_RUN' };
  }

  // 実行 → 確認モーダルをOK → downloadイベントでCSVが直接落ちる (auto-nefuda.js と同方式)
  clickBudgetMs('② (実行ボタンの前)');   // 夜の止め (③c-1b-3a)。押す前に止めるなら download の待ちも始めない
  const downloadPromise = page.waitForEvent('download', { timeout: 180000 }).catch(() => null);
  await nightClick(exeSel, '② (実行ボタンの前)');

  let download = null;
  let okClicks = 0;
  const deadline = Date.now() + 180000;
  while (Date.now() < deadline) {
    const r = await Promise.race([
      downloadPromise.then((d) => ({ kind: 'dl', d })),
      page.waitForTimeout(1000).then(() => ({ kind: 'tick' })),
    ]);
    if (r.kind === 'dl') { download = r.d; break; }
    if (await sessionLost(page)) throw new SessionLostError('②: エクスポート実行中にセッション切れ');
    assertNoUnexpectedDialog();
    const msg = await visibleModalText(page);
    if (!msg) continue;
    if (/(0\s*件|対象.{0,8}(あり|ござい)ません)/.test(msg)) {
      await errorShot(page, 'bc-export-zero');
      throw new Error(`②: エクスポート対象が0件です: "${msg.slice(0, 150)}"。バーコードマスタ.csvは温存しました。期間(LOGIZARD_BC_EXPORT_DAYS)を確認してください。`);
    }
    if (/エラー|失敗/.test(msg)) {
      await errorShot(page, 'bc-export-error');
      throw new Error(`②: エクスポートでエラー表示: "${msg.slice(0, 200)}"`);
    }
    if (/よろしいですか|開始します/.test(msg)) {
      if (!/(エクスポート|出力|ダウンロード|ファイル|抽出|CSV)/i.test(msg)) {
        await errorShot(page, 'bc-export-unknown-confirm');
        throw new Error(`②: 想定外の確認モーダル: "${msg.slice(0, 200)}"。OKを押さずに中止しました (押さなければ何も実行されません)。この文言をClaudeに伝えてください。`);
      }
      if (okClicks >= 3) throw new Error(`②: 確認モーダルが繰り返し表示されます: "${msg.slice(0, 150)}"`);
      okClicks++;
      console.log(`💬 確認モーダル "${msg.slice(0, 80)}" → OK`);
      // 夜の止め (③c-1b-3a・Codex #1518 R1・R2)。夜の止め以外の押せなかったは今までどおり続ける
      await nightClick(page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first(), '② (確認の OK の前)').catch((e) => { if (e && e.nightBlock) throw e; });
      await page.waitForTimeout(500);
    }
  }
  if (!download) {
    await errorShot(page, 'bc-export-timeout');
    throw new Error('②: 180秒待ってもCSVダウンロードが始まりません');
  }

  const dlName = download.suggestedFilename();
  const rawPath = path.join(LOG_DIR, `barcode_export_${UNIQ}.csv`);
  fs.mkdirSync(LOG_DIR, { recursive: true });
  await download.saveAs(rawPath);
  const body = fs.readFileSync(rawPath);
  console.log(`⬇ ダウンロード: ${dlName} (${(body.length / 1024).toFixed(0)} KB)`);

  // ===== 中身の検証 (既存バーコードマスタを壊さないための砦。auto-zaiko.js と同水準) =====
  const head = body.slice(0, 2000).toString('latin1');
  if (/<html|<!DOCTYPE|user_id|SUSPENDED/i.test(head)) {
    throw new SessionLostError('②: CSVではなくHTML(ログイン/SUSPENDED)が返却されました');
  }
  let text;
  try {
    text = new TextDecoder('shift_jis', { fatal: true }).decode(body);
  } catch {
    throw new Error(`②: Shift-JISとして解釈できないバイトが含まれます (証跡: logs/${path.basename(rawPath)})。既存バーコードマスタは温存しました。`);
  }
  let parsed;
  try {
    parsed = parseCsv(text);
  } catch (e) {
    throw new Error(`②: ${e.message} (証跡: logs/${path.basename(rawPath)})。既存バーコードマスタは温存しました。`);
  }
  const rows = parsed.rows;
  if (rows.length < 1) throw new Error('②: CSVが空です。既存バーコードマスタは温存しました。');
  const header = rows[0];
  for (const col of ['商品ID', 'バーコード']) {
    if (!header.includes(col)) {
      throw new Error(`②: CSVヘッダに列「${col}」がありません (ヘッダ: ${header.join(',').slice(0, 120)})。抽出パターンが変わった可能性があります。既存バーコードマスタは温存しました。`);
    }
  }
  const badRow = rows.findIndex((r, i) => i > 0 && r.length !== header.length);
  if (badRow > 0) {
    throw new Error(`②: ${badRow + 1}行目の列数がヘッダと一致しません (証跡: logs/${path.basename(rawPath)})。既存バーコードマスタは温存しました。`);
  }
  const dataRows = rows.length - 1;
  if (dataRows < MIN_ROWS) {
    throw new Error(`②: データ行が想定より少ない (${dataRows}行 < 下限${MIN_ROWS}行)。既存バーコードマスタは温存しました。(下限は .env LOGIZARD_BC_MIN_ROWS)`);
  }
  let prevRows = null;
  if (fs.existsSync(EXPORT_OUT)) {
    try {
      prevRows = parseCsv(new TextDecoder('shift_jis').decode(fs.readFileSync(EXPORT_OUT))).rows.length - 1;
    } catch { /* 前回比はログ用途のみ */ }
  }

  // ===== 保存 (一時ファイル→rename。固定パスを更新できなければ成功にしない) =====
  const tmpPath = path.join(path.dirname(EXPORT_OUT), `.tmp_barcode_${UNIQ}.csv`);
  try {
    fs.writeFileSync(tmpPath, body, { flag: 'wx' });
    fs.renameSync(tmpPath, EXPORT_OUT);
  } catch (e) {
    try { fs.unlinkSync(tmpPath); } catch { /* 一時ファイルの後始末 */ }
    if (e.code === 'EBUSY' || e.code === 'EPERM' || e.code === 'EACCES') {
      let evidence = '';
      const alt = path.join(path.dirname(EXPORT_OUT), `${path.basename(EXPORT_OUT, '.csv')}_${UNIQ}.csv`);
      try {
        fs.writeFileSync(alt, body, { flag: 'wx' });
        evidence = ` 取得したCSVは ${alt} に保存しました。`;
      } catch { /* 証跡保存の失敗は本質ではない */ }
      const err = new Error(`②: ${path.basename(EXPORT_OUT)} を更新できません (${e.code})。ExcelなどでCSVを開いていないか確認して再実行してください。${evidence}`);
      err.targetLocked = true;
      throw err;
    }
    throw e;
  }
  console.log(`✅ ②: 保存 ${EXPORT_OUT}`);
  console.log(`   └ ${dataRows.toLocaleString()}行 / ${header.length}列 / ${(body.length / 1024).toFixed(0)} KB${prevRows !== null ? ` (前回 ${prevRows.toLocaleString()}行)` : ''}`);

  // 完了モーダルが残っていれば閉じる (ベストエフォート)
  await page.locator('input[type="button"][value*="OK"]:visible, button:has-text("OK"):visible').first().click().catch(() => {});
  return { status: 'SUCCESS', rows: dataRows, sizeKB: Math.round(body.length / 1024), fileName: dlName, prevRows };
}

// =====================================================================
// メイン
// =====================================================================
// ログインの設定。ログインのボタンを押す直前 (共通部品の中のリトライも) に夜の止めを見る (③c-1b-3a・Codex #1518 R1)
const loginOpts = () => ({
  ...(useDedicated ? { userId: BC_USER, password: BC_PASS, label: 'バーコード連携用アカウント' } : {}),
  beforeSubmit: () => clickBudgetMs('ログインのボタンの前'),   // 押す持ち時間を返す (共通部品が click の timeout にする)
});

// 各ステップとも「実行ボタンを押す前のセッション切れ」だけ1回再ログインして再試行する
async function withRelogin(stepName, fn) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (e && e.sessionLost && attempt === 1) {
        console.log(`⚠ ${stepName}: セッション切れ (${e.message}) → 再ログインして1回だけ再試行`);
        assertOutsideNightBlock(`${stepName} (再ログインの前)`);
        await login(page, loginOpts()).catch((err) => { throw asNightError(err, 'ログインのボタンの前'); });
        continue;
      }
      throw e;
    }
  }
}

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
  const night = !!(e && e.nightBlock) || nightDialog;   // 夜の止めで dialog を承認しなかった後の失敗も夜の止め
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
