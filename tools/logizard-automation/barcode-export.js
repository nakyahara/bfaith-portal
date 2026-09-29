/**
 * barcode-export.js — ロジザードの SKU のバーコード情報の全件を書き出す (マスタ正本切替 ③c-1b-2b・契約 v3 K4)
 *
 * 取込の試験 (scripts/logizard-import/lz-import-test.mjs) が、取り込む前と後にバーコードを書き出して
 * 「取込でバーコードが増えた・消えた・変わった」が無いことを確かめるための部品。
 *
 * 画面 = エクスポート [PM08 / FM08_01]・種類 = SKU・抽出パターン = バーコード情報 (2026-09-29 に probe-barcode-form.js で実機の部品を確かめた:
 *   部品の名前は商品マスタの書き出し (shohin-export.js) と同じ FM08_01_BR010_*。既定は「更新日・30 日前〜今日」)。
 * 全件にするため、商品マスタと同じく 対象日 = 登録日・開始日を空 (「登録日 ≦ 今日」= 全件。日付は 1 年以内しか指定できない)・有効 + 無効。
 * auto-barcode.js の ② (直近 N 日の更新分でバーコードマスタ.csv を上書き) とは別の書き出し = ② の動きは変えない。
 *
 * 約束 (lz-import-test.mjs realTestSession): exportBarcodeMaster(page, { dlDir, log }) → { buf, v, fileName }。固定の出力先には書かない。
 * 中身の検証に落ちた = invalidCsvError (code invalid_csv = 中身が壊れている / export_not_csv = HTML) を投げる。
 * 途中で切れた CSV (Codex #1530 R1 High): 行の途中 = 引用符・列の数で分かる / 行の切れ目 = 本物の書き出しは末尾が改行で終わらない (2026-09-29 に確かめた) ので、改行で終わる = 切れた疑い /
 *   下限 4,000 行 (9/29 は 5,188 行)。取込の試験は、同じ回の商品マスタの全商品がバーコードにあること (行の切れ目でちょうど切れても分かる) と前後の行の数も見る
 *   (lz-import-verify.mjs compareBarcodes の missing_in_*_barcode・rows_decreased / lz-import-test.mjs は直前が欠けていたら押さない)。
 * ロジザードは照会 (エクスポート) だけ = 業務データを変えない。セッションの鍵は呼び手が持つ。
 */
import fs from 'fs';
import path from 'path';
import { BASE, sessionLost, SessionLostError, errorShot, waitBlockUIGone, jstStamp } from './logizard-common.js';
import { parseCsv } from './csv-util.js';
import { jstTodaySlash, invalidCsvError } from './shohin-export.js';

export const BARCODE_TYPE_LABEL = 'SKU';
export const BARCODE_PATTERN_LABEL = 'バーコード情報';
export const BARCODE_REQUIRED_COLS = ['商品ID', 'バーコード'];
/** 閉じてよい注意文 (実機で出ることが分かっているものだけ)。日付を空にしたときの「1年以上離れた日付が指定されています」 */
export const KNOWN_NOTICES = Object.freeze([/1年以上離れた日付/]);

/**
 * 承認以外のモーダルが出ていたら閉じる。閉じるのは KNOWN_NOTICES の注意文だけ (キャンセル → 無ければ OK)。
 * 知らない文 = 何も押さずに止める (知らない確認を承認しない。Codex #1530 R1 Medium)。⚠「エクスポート処理を行います」は触らない (条件が固まる前にエクスポートが走る)
 * @returns {Promise<string|null>}  出ていた文 (無い = null)
 */
export async function dismissNotice(page, log) {
  const t = await page.evaluate(() => {
    const ov = document.getElementById('popup_overlay');
    if (!ov || ov.offsetParent === null) return null;
    const m = document.getElementById('popup_message');
    return (m ? m.innerText : '').replace(/\s+/g, ' ').trim();
  });
  if (t == null) return null;
  if (/エクスポート処理を行います/.test(t.replace(/\s+/g, ''))) return t;
  if (!KNOWN_NOTICES.some((re) => re.test(t))) throw new Error(`想定外のモーダル: ${t.slice(0, 200)} (何も押さずに止める)`);
  log(`💬 画面からの注意: ${t.slice(0, 120)}`);
  await page.click('#popup_cancel').catch(async () => { await page.click('#popup_ok').catch(() => {}); });
  await page.waitForFunction(() => {
    const ov = document.getElementById('popup_overlay');
    return !ov || ov.offsetParent === null;
  }, undefined, { timeout: 10000 }).catch(() => {});
  return t;
}

/**
 * 確かめの道具 (export-barcode-to.js) が書いてよい場所 = このフォルダの out\ の下だけ (共有ドライブ・ネットワークの場所・ほかのフォルダには書かない)。
 * 文字の比べに加えて、いちばん近くにある親フォルダの実体 (realpath) が out\ の実体の下か = ジャンクション・シンボリックリンクで外を指していても見分ける
 * (OS の一時フォルダは許さない = TEMP が共有ドライブを指す PC もある。Codex #1530 R1・R2 Medium)
 */
export function isAllowedOut(p, { dir }) {
  if (!p || !dir) return false;
  const inside = (f, root) => { const a = f.toLowerCase(), r = root.toLowerCase().replace(/[\\/]+$/, ''); return a.startsWith(r + '\\') || a.startsWith(r + '/'); };
  const root = path.resolve(dir, 'out'), f = path.resolve(p);
  if (!inside(f, root)) return false;
  let realRoot;
  try { realRoot = fs.realpathSync.native(root); } catch { return false; }   // out\ が無い = 書かない
  // out\ そのものがジャンクション・リンク = 外を指しうる = 書かない (Codex #1530 R3 Medium)。
  // 親フォルダの実体 + out と out の実体を比べる (短い名前 (8.3 形式) と長い名前の違いで正しい場所を断らないよう、両方とも実体で)
  let realParent;
  try { realParent = fs.realpathSync.native(path.dirname(root)); } catch { return false; }
  if (realRoot.toLowerCase() !== path.join(realParent, 'out').toLowerCase()) return false;
  let near = path.dirname(f);
  while (!fs.existsSync(near)) { const up = path.dirname(near); if (up === near) return false; near = up; }
  let realNear;
  try { realNear = fs.realpathSync.native(near); } catch { return false; }
  return realNear.toLowerCase() === realRoot.toLowerCase() || inside(realNear, realRoot);
}

/** CSV の引用符の形 (閉じ引用符の後の文字・引用符で始まらない欄の途中の引用符・閉じていない引用符 = 壊れ)。parseCsv は寛容なので別に見る (Codex #1530 R2 Medium) */
export function csvQuoteError(text) {
  let inQ = false, afterQ = false, fieldStart = true;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) { if (c === '"') { if (text[i + 1] === '"') { i++; continue; } inQ = false; afterQ = true; } continue; }
    if (afterQ) { if (c === ',' || c === '\r' || c === '\n') { afterQ = false; fieldStart = true; continue; } return 'after_quote'; }
    if (c === '"') { if (fieldStart) { inQ = true; fieldStart = false; continue; } return 'bare_quote'; }
    fieldStart = c === ',' || c === '\r' || c === '\n';
  }
  return inQ ? 'unterminated' : null;
}

/** 承認のモーダルの文 (空白を除いて完全一致)。「エクスポート処理を行います」(+「よろしいですか？」) だけ = ほかの文が足されていたら押さない */
export const EXPORT_CONFIRM_RE = /^エクスポート処理を行います[。．.]?(よろしいですか[？?]?)?$/;   // 実機 (9/29) = 「エクスポート処理を行います よろしいですか」
export const BARCODE_MIN_ROWS = 4000;   // 9/29 の全件 = 5,188 行
export const BARCODE_FILE_NAME = 'barcode_master';   // 保存ファイル名は必須 (空 = 「条件入力に不備があります」)

/**
 * 書き出したバーコードの CSV を確かめる (壊れた・HTML・見出しが違う・列の数が違う・少なすぎる = ok: false)
 * @returns {{ ok: boolean, reason?: string, dataRows?: number, header?: string[] }}
 */
export function validateBarcodeCsv(buf, { minRows = BARCODE_MIN_ROWS } = {}) {
  if (!buf || buf.length === 0) return { ok: false, reason: '中身が空です' };
  const head = buf.slice(0, 2000).toString('latin1');
  if (/<html|<!DOCTYPE|user_id|SUSPENDED/i.test(head)) return { ok: false, reason: 'CSVではなくHTML(ログイン/SUSPENDED)が返っています' };
  let text;
  try {
    text = new TextDecoder('shift_jis', { fatal: true }).decode(buf);
  } catch (e) {
    return { ok: false, reason: `Shift-JIS として読めません (${e.message})` };
  }
  if (/[\r\n]$/.test(text)) return { ok: false, reason: '末尾が改行で終わっています (本物の書き出しは改行で終わらない = 行の切れ目で切れた疑い)' };
  const qe = csvQuoteError(text);
  if (qe) return { ok: false, reason: `CSV の引用符の形が壊れています (${qe})` };
  let rows;
  try {
    ({ rows } = parseCsv(text));
  } catch (e) {
    return { ok: false, reason: `CSVとして読めません (${e.message})` };
  }
  if (!Array.isArray(rows) || rows.length === 0) return { ok: false, reason: '行がありません' };
  const header = rows[0].map((h) => String(h).trim());
  const missing = BARCODE_REQUIRED_COLS.filter((c) => header.indexOf(c) < 0);
  if (missing.length) return { ok: false, reason: `必須列がありません: ${missing.join(', ')} (実際の先頭列: ${header.slice(0, 10).join(' / ')})` };
  for (const c of BARCODE_REQUIRED_COLS) if (header.indexOf(c) !== header.lastIndexOf(c)) return { ok: false, reason: `列「${c}」が 2 つあります` };
  const blank = (r) => r.length === 1 && String(r[0] || '').trim() === '';   // 空の行は数えない (shohin-export.js と同じ)
  for (let i = 1; i < rows.length; i++) {
    if (blank(rows[i])) continue;
    if (rows[i].length !== header.length) return { ok: false, reason: `${i + 1} 行目の列数が違います (ヘッダ ${header.length} / この行 ${rows[i].length})` };
  }
  const dataRows = rows.slice(1).filter((r) => !blank(r)).length;
  if (dataRows < minRows) return { ok: false, reason: `行数が少なすぎます (${dataRows} 行 < 下限 ${minRows})` };
  return { ok: true, dataRows, header };
}

/**
 * ログイン済みの page で SKU のバーコード情報の全件を書き出す。
 * @param {import('playwright-core').Page} page  ログイン済み (呼び手がセッションの鍵を持っている)
 * @param {object} opts
 * @param {string} opts.dlDir  ダウンロードを一時に置く場所 (読んだら消す)
 * @param {number} [opts.minRows]
 * @param {boolean} [opts.dry]  条件の設定まで (実行ボタンを押さない)
 * @returns {Promise<{ dry: true } | { buf: Buffer, v: object, fileName: string }>}
 */
export async function exportBarcodeMaster(page, { dlDir, minRows = BARCODE_MIN_ROWS, dry = false, log = console.log } = {}) {
  fs.mkdirSync(dlDir, { recursive: true });
  let dlPath = null;
  try {
    await page.goto(`${BASE}/PM08/Index`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (await sessionLost(page)) throw new SessionLostError('PM08 遷移時にセッション切れ');
    await page.click(`a[onclick*="openFunctionBar('FM08_01')"]`).catch(() => {});
    await page.waitForSelector('#FM08_01_fileId', { state: 'visible', timeout: 30000 });

    // 種類 = SKU・抽出パターン = バーコード情報 (どちらも表示の文字の完全一致で選ぶ。value は環境で振られる)
    const type = await page.evaluate((label) => {
      const s = document.getElementById('FM08_01_fileId');
      const hit = [...s.options].filter((o) => (o.textContent || '').trim() === label);
      return hit.length === 1 ? hit[0].value : null;
    }, BARCODE_TYPE_LABEL);
    if (type == null) { await errorShot(page, 'bcx-type'); throw new Error(`種類「${BARCODE_TYPE_LABEL}」を 1 つに決められません`); }
    await page.selectOption('#FM08_01_fileId', type);
    const loaded = await page.waitForFunction(() => {
      const sel = document.getElementById('FM08_01_ptrnId');
      return !!sel && [...sel.options].some((o) => o.value && o.value !== '');
    }, undefined, { timeout: 30000 }).then(() => true).catch(() => false);
    if (!loaded) { await errorShot(page, 'bcx-ptrn-not-loaded'); throw new Error(`種類=${BARCODE_TYPE_LABEL} を選んでも抽出パターンが読み込まれません`); }
    const ptrn = await page.evaluate((label) => {
      const s = document.getElementById('FM08_01_ptrnId');
      const all = [...s.options].map((o) => ({ value: o.value, text: (o.textContent || '').trim() }));
      const hit = all.filter((o) => o.value !== '' && o.text === label);
      return { hit: hit.length === 1 ? hit[0] : null, all };
    }, BARCODE_PATTERN_LABEL);
    if (!ptrn.hit) { await errorShot(page, 'bcx-ptrn'); throw new Error(`抽出パターン「${BARCODE_PATTERN_LABEL}」を 1 つに決められません (候補: ${ptrn.all.map((o) => o.text).join(' / ')})`); }
    await page.selectOption('#FM08_01_ptrnId', ptrn.hit.value);
    await page.waitForFunction(() => !!document.querySelector('.blockUI.blockOverlay'), undefined, { timeout: 3000 }).catch(() => {});
    await waitBlockUIGone(page, 'パターン選択', 30000);
    await page.waitForTimeout(2000);
    log(`📄 種類=${BARCODE_TYPE_LABEL} (value=${type}) / パターン=${ptrn.hit.text} (value=${ptrn.hit.value})`);

    // 出力条件: ヘッダ あり・囲い文字 あり・SHIFT-JIS・カンマ・登録日・開始日なし (= 全件)・終わり = 今日・有効 + 無効・保存ファイル名
    const today = jstTodaySlash();
    const applyConditions = async () => {
      await page.check('#FM08_01_BR010_headerFlg1').catch(() => {});
      await page.check('#FM08_01_BR010_encFlg1').catch(() => {});
      await page.selectOption('#FM08_01_BR010_charCodeTyp', '1').catch(() => {});
      await page.check('#FM08_01_BR010_splitter1').catch(() => {});
      await page.check('#FM08_01_BR010_targetDate1').catch(() => {});          // 登録日
      await page.fill('#FM08_01_BR010_fromTargetDate', '').catch(() => {});    // 開始なし = 全件
      await page.fill('#FM08_01_BR010_toTargetDate', today).catch(() => {});
      await page.check('#FM08_01_BR010_expStatus1').catch(() => {});           // 有効マスタ
      await page.check('#FM08_01_BR010_expStatus2').catch(() => {});           // 無効マスタ
      await page.fill('#FM08_01_fileName', BARCODE_FILE_NAME).catch(() => {});
    };
    const readConditions = () => page.evaluate(() => {
      const g = (id) => document.getElementById(id);
      return {
        fileId: g('FM08_01_fileId')?.value, ptrnId: g('FM08_01_ptrnId')?.value,
        header1: g('FM08_01_BR010_headerFlg1')?.checked, enc1: g('FM08_01_BR010_encFlg1')?.checked,
        charCode: g('FM08_01_BR010_charCodeTyp')?.value, comma: g('FM08_01_BR010_splitter1')?.checked,
        target1: g('FM08_01_BR010_targetDate1')?.checked,
        from: g('FM08_01_BR010_fromTargetDate')?.value, to: g('FM08_01_BR010_toTargetDate')?.value,
        exp1: g('FM08_01_BR010_expStatus1')?.checked, exp2: g('FM08_01_BR010_expStatus2')?.checked,
        fileName: g('FM08_01_fileName')?.value,
      };
    });
    const conditionsOk = (c) => c && c.fileId === type && c.ptrnId === ptrn.hit.value
      && c.header1 === true && c.enc1 === true && c.charCode === '1' && c.comma === true && c.target1 === true
      && c.from === '' && c.to === today && c.exp1 === true && c.exp2 === true && c.fileName === BARCODE_FILE_NAME;

    // 設定 → 整定待ち → 読み戻して確かめる (最大 3 回。遅れて始まる初期化が値を上書きする競合)
    let condOk = false, c = null;
    for (let i = 0; i < 3 && !condOk; i++) {
      await applyConditions();
      await page.waitForTimeout(1500);
      await dismissNotice(page, log);   // 日付を空にすると「1年以上離れた…」の注意が出ることがある (承認のモーダルには触らない)
      c = await readConditions();
      condOk = conditionsOk(c);
      if (!condOk) log(`⚠️ 条件が画面側で書き換えられたため再設定します (${i + 1}/3): ${JSON.stringify(c)}`);
    }
    if (!condOk) { await errorShot(page, 'bcx-condition-unstable'); throw new Error(`出力条件が確定できません: ${JSON.stringify(c)}`); }
    if (dry) { log('🧪 --dry のため実行せず終了します (条件設定までは成功)'); return { dry: true }; }

    // ── 実行 → 承認のモーダル (「エクスポート処理を行います」だけ) → download ──
    await page.click('#FM08_01_executeBtn');
    const okBtn = page.locator('#popup_ok');
    const okVisible = await okBtn.waitFor({ state: 'visible', timeout: 30000 }).then(() => true).catch(() => false);
    if (!okVisible) {
      if (await sessionLost(page)) throw new SessionLostError('実行ボタン押下時にセッション切れ');
      await errorShot(page, 'bcx-no-confirm');
      throw new Error('承認モーダルが出ません (30秒待機)');
    }
    const confirmMsg = await page.locator('#popup_message').innerText().catch(() => '');
    if (!EXPORT_CONFIRM_RE.test(confirmMsg.replace(/\s+/g, ''))) {   // 完全一致 (知らない文が足されていたら押さない。Codex #1530 R3 Medium)
      await errorShot(page, 'bcx-unexpected-confirm');
      throw new Error(`想定外の承認モーダル: ${confirmMsg.replace(/\s+/g, ' ').slice(0, 200)} (中止しました)`);
    }
    const dlPromise = page.waitForEvent('download', { timeout: 300000 }).catch(() => null);
    await okBtn.click();
    const download = await dlPromise;
    if (!download) {
      if (await sessionLost(page)) throw new SessionLostError('エクスポート実行中にセッション切れ');
      await errorShot(page, 'bcx-no-download');
      throw new Error('300秒待ってもダウンロードが始まりません');
    }
    log(`✅ ダウンロード開始: ${download.suggestedFilename()}`);
    dlPath = path.join(dlDir, `.dl_barcode_${jstStamp()}_${process.pid}.csv`);
    await download.saveAs(dlPath);
    const failure = await download.failure();
    if (failure) throw new Error(`ダウンロードが失敗しました: ${failure}`);
    const buf = fs.readFileSync(dlPath);
    log(`⬇️ 取得: ${(buf.length / 1024).toFixed(1)} KB`);
    const v = validateBarcodeCsv(buf, { minRows });
    if (!v.ok) { await errorShot(page, 'bcx-invalid-csv'); throw invalidCsvError(v.reason); }
    return { buf, v, fileName: download.suggestedFilename() };
  } finally {
    if (dlPath) { try { fs.unlinkSync(dlPath); } catch { /* 保存前に失敗していれば無い */ } }
  }
}
