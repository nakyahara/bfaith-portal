/**
 * shohin-export.js — ロジザードの商品マスタの全件の書き出し (エクスポート[FM08_01] 種類=商品 / パターン=デフォルト / 全期間・有効 + 無効)
 *
 * auto-shohin-csv.js (00:20 の定時) から切り出した部品 (2026-09-28・マスタ正本切替 ③c-1b 契約 v3 H8)。
 * 取込 (③c-1b-2) は「直前の書き出し → 取込 → 直後の書き出し」を**同じブラウザ・同じセッションの鍵のまま**続けて使う。
 * → ログイン・鍵・保存先は呼び手が持つ。ここは画面の手順と、取れた CSV の検証だけ。
 *
 * 手順と検証は auto-shohin-csv.js v0.1 (2026-09-01) のまま (動きを変えない)。
 */
import fs from 'fs';
import path from 'path';
import { BASE, sessionLost, SessionLostError, errorShot, waitBlockUIGone, jstStamp } from './logizard-common.js';
import { parseCsv } from './csv-util.js';

// エクスポート画面の固定値 (2026-07-21 採取の FM08_01 画面。種類の value は 49 種)
export const SHOHIN_FILE_ID = '5';              // 商品
export const SHOHIN_PTRN_LABEL_RE = /デフォルト/;  // パターンは名前で選ぶ (value は環境ごとに振られるため)
// 取込側 (apps/inbound-check/product-master.js) が必須にしている列と揃える
export const SHOHIN_REQUIRED_COLS = ['商品ID', '有効期限区分'];

/** JST の当日を画面の表記 (YYYY/MM/DD) で */
export function jstTodaySlash(now = new Date()) {
  return new Intl.DateTimeFormat('ja-JP', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' })
    .format(now);
}

/**
 * 承認以外の通知モーダルを閉じる。⚠「エクスポート処理を行います」は絶対に触らない
 * (ここで OK を押すと、条件が固まる前にエクスポートが走ってしまう)
 */
async function dismissNotice(page, log) {
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
export function validateShohinCsv(buf, { minRows = 100 } = {}) {
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
  const missing = SHOHIN_REQUIRED_COLS.filter(c => !header.includes(c));
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
  if (dataRows < minRows) {
    return { ok: false, reason: `行数が少なすぎます (${dataRows} 行 < 下限 ${minRows})。商品マスタが空になることは無いため中止しました` };
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

/**
 * ログイン済みの page で商品マスタの全件を書き出す。
 * @param {import('playwright-core').Page} page  ログイン済み (呼び手がセッションの鍵を持っている)
 * @param {object} opts
 * @param {string} opts.dlDir  ダウンロードを一時に置く場所 (読んだら消す)
 * @param {number} [opts.minRows]  検証の下限の行数
 * @param {boolean} [opts.dry]  条件の設定まで (実行ボタンを押さない)
 * @param {(...a: any[]) => void} [opts.log]
 * @returns {Promise<{ dry: true } | { buf: Buffer, v: object, fileName: string }>}  検証に落ちた = throw (呼び手の既存ファイルは触らない)
 */
export async function exportShohinMaster(page, { dlDir, minRows = 100, dry = false, log = console.log } = {}) {
  fs.mkdirSync(dlDir, { recursive: true });
  let dlPath = null;
  try {
    // ── エクスポート [PM08 / FM08_01] ──
    await page.goto(`${BASE}/PM08/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (await sessionLost(page)) throw new SessionLostError('PM08 遷移時にセッション切れ');
    await page.click(`a[onclick*="openFunctionBar('FM08_01')"]`).catch(() => {});
    await page.waitForSelector('#FM08_01_fileId', { state: 'visible', timeout: 30000 });

    // 種類=商品 → 抽出パターンが AJAX で読み込まれるのを待つ
    await page.selectOption('#FM08_01_fileId', SHOHIN_FILE_ID);
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
    }, SHOHIN_PTRN_LABEL_RE.source);
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
    const today = jstTodaySlash();
    const applyConditions = async () => {
      await page.check('#FM08_01_BR010_headerFlg1').catch(() => {});
      await page.selectOption('#FM08_01_BR010_charCodeTyp', '1').catch(() => {});
      await page.check('#FM08_01_BR010_targetDate1').catch(() => {});          // 登録日
      await page.fill('#FM08_01_BR010_fromTargetDate', '').catch(() => {});    // 開始なし = 全件
      await page.fill('#FM08_01_BR010_toTargetDate', today).catch(() => {});
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
    const conditionsOk = c => c && c.fileId === SHOHIN_FILE_ID && c.ptrnId === ptrn.hit.value
      && c.header1 === true && c.charCode === '1' && c.target1 === true
      && c.from === '' && c.to === today
      && c.exp1 === true && c.exp2 === true && !!c.fileName;

    // 設定 → 整定待ち → 読み戻し検証を最大3回 (遅延初期化が設定値を上書きする競合の検出と再設定)
    let condOk = false;
    for (let i = 0; i < 3 && !condOk; i++) {
      await applyConditions();
      await page.waitForTimeout(1500);
      // 日付を空にすると「1年以上離れた…」の注意モーダルが出ることがある。承認モーダル以外は閉じる
      await dismissNotice(page, log);
      const c = await readConditions();
      condOk = conditionsOk(c);
      if (!condOk) log(`⚠️ 条件が画面側で書き換えられたため再設定します (${i + 1}/3): ${JSON.stringify(c)}`);
    }
    if (!condOk) {
      await errorShot(page, 'condition-unstable');
      throw new Error('出力条件が確定できません (ヘッダ / 文字コード / 登録日 / 期間 / 有効・無効 / 保存ファイル名)。再実行してください');
    }

    if (dry) {
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

    dlPath = path.join(dlDir, `.dl_shohin_${jstStamp()}_${process.pid}.csv`);
    await download.saveAs(dlPath);
    const failure = await download.failure();
    if (failure) throw new Error(`ダウンロードが失敗しました: ${failure}。既存CSVは温存しました`);
    const buf = fs.readFileSync(dlPath);
    log(`⬇️ 取得: ${(buf.length / 1024).toFixed(1)} KB`);

    // ── 検証 ──
    const v = validateShohinCsv(buf, { minRows });
    if (!v.ok) {
      await errorShot(page, 'invalid-csv');
      throw new Error(`CSVの検証に失敗: ${v.reason} (既存CSVは温存しました)`);
    }
    return { buf, v, fileName: download.suggestedFilename() };
  } finally {
    if (dlPath) { try { fs.unlinkSync(dlPath); } catch { /* 保存前に失敗していれば無い */ } }
  }
}
