/**
 * ロジザード自動化: エクスポート[FM08_01] → 種類=商品 / パターン=デフォルト → 商品マスタCSV
 * v0.1 (2026-09-01。auto-nyuka-csv.js / auto-nefuda.js を雛形に作成)
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
  loadEnv, launchBrowser, login, acquireLock, releaseLock, assertLocalWriteDirs,
  BASE, DIR, sessionLost, SessionLostError, errorShot, waitBlockUIGone, jstStamp,
} from './logizard-common.js';
import { parseCsv } from './csv-util.js';

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

// エクスポート画面の固定値 (2026-07-21 採取の FM08_01 画面。種類の value は 49 種)
const FILE_ID = '5';              // 商品
const PTRN_LABEL_RE = /デフォルト/;  // パターンは名前で選ぶ (value は環境ごとに振られるため)

// 取込側 (apps/inbound-check/product-master.js) が必須にしている列と揃える
const REQUIRED_COLS = ['商品ID', '有効期限区分'];
const DL_DIR = path.join(DIR, 'downloads');

const log = (...a) => console.log(...a);

/** JST の当日を画面の表記 (YYYY/MM/DD) で。--once-per-day 用の jstToday (YYYY-MM-DD) とは別物 */
function jstTodaySlash() {
  return new Intl.DateTimeFormat('ja-JP', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' })
    .format(new Date());
}

/**
 * 承認以外の通知モーダルを閉じる。⚠「エクスポート処理を行います」は絶対に触らない
 * (ここで OK を押すと、条件が固まる前にエクスポートが走ってしまう)
 */
async function dismissNotice(page) {
  const t = await page.evaluate(() => {
    const ov = document.getElementById('popup_overlay');
    if (!ov || ov.offsetParent === null) return null;
    const m = document.getElementById('popup_message');
    return (m ? m.innerText : '').replace(/\s+/g, ' ').trim();
  });
  if (t == null) return null;
  if (/エクスポート処理を行います/.test(t.replace(/\s+/g, ''))) return t;
  log(`💬 画面からの注意: ${t.slice(0, 120)}`);
  await page.click('#popup_cancel').catch(async () => { await page.click('#popup_ok').catch(() => {}); });
  await page.waitForFunction(() => {
    const ov = document.getElementById('popup_overlay');
    return !ov || ov.offsetParent === null;
  }, undefined, { timeout: 10000 }).catch(() => {});
  return t;
}

// ───────── CSV 検証 (既存ファイルを壊さないための最後の砦) ─────────
function validateCsv(buf) {
  if (!buf || buf.length === 0) return { ok: false, reason: '中身が空です' };
  const head = buf.slice(0, 2000).toString('latin1');
  if (/<html|<!DOCTYPE|user_id|SUSPENDED/i.test(head)) {
    return { ok: false, reason: 'CSVではなくHTML(ログイン/SUSPENDED)が返っています' };
  }
  let text;
  try {
    // Node 標準の TextDecoder で厳密にデコードする (miniPC に iconv-lite は無い)
    text = new TextDecoder('shift_jis', { fatal: true }).decode(buf);
  } catch (e) {
    return { ok: false, reason: `Shift-JIS として読めません (${e.message})` };
  }
  let rows;
  try {
    // ⚠csv-util.parseCsv は { rows, endedWithNewline } を返す (配列ではない)
    ({ rows } = parseCsv(text));
  } catch (e) {
    return { ok: false, reason: `CSVとして読めません (${e.message})` };
  }
  if (!Array.isArray(rows)) return { ok: false, reason: 'CSVの解析結果が想定と違います' };
  if (rows.length === 0) return { ok: false, reason: '行がありません' };
  const header = rows[0].map(h => String(h || '').trim());
  const missing = REQUIRED_COLS.filter(c => !header.includes(c));
  if (missing.length) {
    return { ok: false, reason: `必須列がありません: ${missing.join(', ')} (実際の先頭列: ${header.slice(0, 10).join(' / ')})` };
  }
  for (let i = 1; i < rows.length; i++) {
    if (rows[i].length === 1 && String(rows[i][0] || '').trim() === '') continue;
    if (rows[i].length !== header.length) {
      return { ok: false, reason: `${i + 1} 行目の列数が違います (ヘッダ ${header.length} / この行 ${rows[i].length})` };
    }
  }
  const dataRows = rows.length - 1;
  if (dataRows < MIN_ROWS) {
    return { ok: false, reason: `行数が少なすぎます (${dataRows} 行 < 下限 ${MIN_ROWS})。商品マスタが空になることは無いため中止しました` };
  }
  const iK = header.indexOf('有効期限区分');
  const counts = {};
  for (let i = 1; i < rows.length; i++) {
    if (rows[i].length !== header.length) continue;
    const k = String(rows[i][iK] || '').trim() || '(空欄)';
    counts[k] = (counts[k] || 0) + 1;
  }
  return { ok: true, dataRows, header, kubunCounts: counts };
}

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
  let dlPath = null;
  try {
    await login(page, { userId: USER_ID, password: PASSWORD, label: '商品マスタCSV' });

    // ── エクスポート [PM08 / FM08_01] ──
    await page.goto(`${BASE}/PM08/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (await sessionLost(page)) throw new SessionLostError('PM08 遷移時にセッション切れ');
    await page.click(`a[onclick*="openFunctionBar('FM08_01')"]`).catch(() => {});
    await page.waitForSelector('#FM08_01_fileId', { state: 'visible', timeout: 30000 });

    // 種類=商品 → 抽出パターンが AJAX で読み込まれるのを待つ
    await page.selectOption('#FM08_01_fileId', FILE_ID);
    const loaded = await page.waitForFunction(() => {
      const sel = document.getElementById('FM08_01_ptrnId');
      return !!sel && [...sel.options].some(o => o.value && o.value !== '');
    }, undefined, { timeout: 30000 }).then(() => true).catch(() => false);
    if (!loaded) {
      await errorShot(page, 'ptrn-not-loaded');
      throw new Error('種類=商品 を選んでも抽出パターンが読み込まれません');
    }

    // パターンは**名前で選ぶ** (value は環境ごとに振られるため固定できない)
    const ptrn = await page.evaluate((reSrc) => {
      const re = new RegExp(reSrc);
      const sel = document.getElementById('FM08_01_ptrnId');
      const all = [...sel.options].map(o => ({ value: o.value, text: (o.textContent || '').trim() }));
      const hit = all.find(o => o.value && re.test(o.text));
      return { hit: hit || null, all };
    }, PTRN_LABEL_RE.source);
    if (!ptrn.hit) {
      await errorShot(page, 'ptrn-default-missing');
      throw new Error(`抽出パターン「デフォルト」が見つかりません (候補: ${ptrn.all.map(o => o.text).join(' / ')})`);
    }
    await page.selectOption('#FM08_01_ptrnId', ptrn.hit.value);
    // パターン初期化 AJAX: 「出現」を短く待ってから消失を待つ (即座に待つと遅れて始まる初期化を見逃す)
    await page.waitForFunction(() => !!document.querySelector('.blockUI.blockOverlay'), undefined, { timeout: 3000 }).catch(() => {});
    await waitBlockUIGone(page, 'パターン選択', 30000);
    await page.waitForTimeout(2000);
    log(`📄 種類=商品 / パターン=${ptrn.hit.text} (value=${ptrn.hit.value})`);

    // 出力条件 (2026-09-01 実機で切り分け):
    //   - 🚨**保存ファイル名は必須**。空だと実行時に「条件入力に不備があります」で弾かれる
    //   - 🚨**日付は1年以内しか指定できない** (「1年以上離れた日付が指定されています」)。
    //     商品マスタは全件欲しいので**開始日を空にする** = 「登録日 ≦ 今日」= 全件。
    //     既定は「先月〜今日」なので、そのままだと直近1ヶ月に登録された商品しか出ない
    //   - 有効マスタ + 無効マスタ の両方 (取扱中止の商品も期限管理の設定は持つ)
    //   - 出荷区分 / 入荷区分 / 取引先の絞り込みは商品マスタでは disabled (触らない)
    const applyConditions = async () => {
      await page.check('#FM08_01_BR010_headerFlg1').catch(() => {});
      await page.selectOption('#FM08_01_BR010_charCodeTyp', '1').catch(() => {});
      await page.check('#FM08_01_BR010_targetDate1').catch(() => {});          // 登録日
      await page.fill('#FM08_01_BR010_fromTargetDate', '').catch(() => {});    // 開始なし = 全件
      await page.fill('#FM08_01_BR010_toTargetDate', jstTodaySlash()).catch(() => {});
      await page.check('#FM08_01_BR010_expStatus1').catch(() => {});           // 有効マスタ
      await page.check('#FM08_01_BR010_expStatus2').catch(() => {});           // 無効マスタ
      await page.fill('#FM08_01_fileName', 'shohin_master').catch(() => {});   // 必須
    };
    const readConditions = () => page.evaluate(() => ({
      fileId: document.getElementById('FM08_01_fileId')?.value,
      ptrnId: document.getElementById('FM08_01_ptrnId')?.value,
      header1: document.getElementById('FM08_01_BR010_headerFlg1')?.checked,
      charCode: document.getElementById('FM08_01_BR010_charCodeTyp')?.value,
      target1: document.getElementById('FM08_01_BR010_targetDate1')?.checked,
      from: document.getElementById('FM08_01_BR010_fromTargetDate')?.value,
      to: document.getElementById('FM08_01_BR010_toTargetDate')?.value,
      exp1: document.getElementById('FM08_01_BR010_expStatus1')?.checked,
      exp2: document.getElementById('FM08_01_BR010_expStatus2')?.checked,
      fileName: document.getElementById('FM08_01_fileName')?.value,
    }));
    const conditionsOk = c => c && c.fileId === FILE_ID && c.ptrnId === ptrn.hit.value
      && c.header1 === true && c.charCode === '1' && c.target1 === true
      && c.from === '' && c.to === jstTodaySlash()
      && c.exp1 === true && c.exp2 === true && !!c.fileName;

    // 設定 → 整定待ち → 読み戻し検証を最大3回 (遅延初期化が設定値を上書きする競合の検出と再設定)
    let condOk = false;
    for (let i = 0; i < 3 && !condOk; i++) {
      await applyConditions();
      await page.waitForTimeout(1500);
      // 日付を空にすると「1年以上離れた…」の注意モーダルが出ることがある。承認モーダル以外は閉じる
      await dismissNotice(page);
      const c = await readConditions();
      condOk = conditionsOk(c);
      if (!condOk) log(`⚠️ 条件が画面側で書き換えられたため再設定します (${i + 1}/3): ${JSON.stringify(c)}`);
    }
    if (!condOk) {
      await errorShot(page, 'condition-unstable');
      throw new Error('出力条件が確定できません (ヘッダ / 文字コード / 登録日 / 期間 / 有効・無効 / 保存ファイル名)。再実行してください');
    }

    if (DRY) {
      log('🧪 --dry のため実行せず終了します (条件設定までは成功)');
      return { dry: true };
    }

    // ── 実行 → 承認モーダル → download イベント ──
    await page.click('#FM08_01_executeBtn');
    const okBtn = page.locator('#popup_ok');
    const okVisible = await okBtn.waitFor({ state: 'visible', timeout: 30000 }).then(() => true).catch(() => false);
    if (!okVisible) {
      if (await sessionLost(page)) throw new SessionLostError('実行ボタン押下時にセッション切れ');
      await errorShot(page, 'no-confirm');
      throw new Error('承認モーダルが出ません (30秒待機)');
    }
    const confirmMsg = await page.locator('#popup_message').innerText().catch(() => '');
    if (!/エクスポート処理を行います/.test(confirmMsg.replace(/\s+/g, ''))) {
      await errorShot(page, 'unexpected-confirm');
      throw new Error(`想定外の承認モーダル: ${confirmMsg.replace(/\s+/g, ' ').slice(0, 200)} (中止しました)`);
    }

    const dlPromise = page.waitForEvent('download', { timeout: 300000 }).catch(() => null);
    await okBtn.click();
    const download = await dlPromise;
    if (!download) {
      if (await sessionLost(page)) throw new SessionLostError('エクスポート実行中にセッション切れ');
      await errorShot(page, 'no-download');
      throw new Error('300秒待ってもダウンロードが始まりません。既存CSVは温存しました');
    }
    log(`✅ ダウンロード開始: ${download.suggestedFilename()}`);

    dlPath = path.join(DL_DIR, `.dl_shohin_${jstStamp()}_${process.pid}.csv`);
    await download.saveAs(dlPath);
    const failure = await download.failure();
    if (failure) throw new Error(`ダウンロードが失敗しました: ${failure}。既存CSVは温存しました`);
    const buf = fs.readFileSync(dlPath);
    log(`⬇️ 取得: ${(buf.length / 1024).toFixed(1)} KB`);

    // ── 検証 → 保存 ──
    const v = validateCsv(buf);
    if (!v.ok) {
      await errorShot(page, 'invalid-csv');
      throw new Error(`CSVの検証に失敗: ${v.reason} (既存CSVは温存しました)`);
    }
    const prev = fs.existsSync(OUT_PATH) ? validateCsv(fs.readFileSync(OUT_PATH)) : null;
    if (prev?.ok && v.dataRows * 2 < prev.dataRows) {
      throw new Error(`商品数が前回の半分未満です (${prev.dataRows} → ${v.dataRows} 行)。`
        + '抽出条件の事故が疑われるため中止しました (既存CSVは温存)');
    }
    log(`📄 ${v.dataRows} 行 / ${v.header.length} 列`);
    // ⭐有効期限区分の内訳を毎回ログに出す。ロジザード側の表記が変わったら気付けるようにする
    log(`📅 有効期限区分: ${Object.entries(v.kubunCounts).map(([k, n]) => `${k}=${n}`).join(' / ')}`);
    return finalize(buf, { rows: v.dataRows });
  } finally {
    if (dlPath) { try { fs.unlinkSync(dlPath); } catch { /* 保存前に失敗していれば無い */ } }
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
