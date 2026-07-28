/**
 * 値札印刷用CSV (nefudaprint.csv) の組み立て → Google Drive 上書き保存 — apps/inbound-info
 *
 * 【経緯】2026-07-28 中原さん指示
 *   もともと GAS `processNefudaCsv` (時間ベーストリガー) が
 *     nefuda.csv + スプレッドシート「入数マスタ」(1cmGncY3uJTsbpYzkjyJDfMizRAHyXgc9KV1Jy0izu0k)
 *   から nefudaprint.csv を作っていた。入数の正が f_inbound_info (この画面) に移ったため、
 *   スプレッドシートを読まずポータル側で作るようにした。
 *   ⚠ 旧 GAS トリガー (processNefudaCsv / syncInzuMasterFromProductList) は止めること。
 *     両方動くと、最後に走った方の入数 (= 古いシートの値) で上書きされる。
 *
 * 【変換】nefuda.csv (4列) → nefudaprint.csv (6列)
 *   商品ID, 商品名, バーコード, 有効期限
 *     → 商品ID, 商品名, JAN, FNSKU, 有効期限, 入数
 *   - バーコードは英字を含めば FNSKU、数字だけなら JAN に振り分ける (空・想定外は両方空)
 *   - 入数は f_inbound_info から code_key で引く。未登録・未記入は空欄 (旧 GAS と同じ)
 *   - 出力は Shift_JIS / CRLF 区切り / 末尾改行なし / 必要な時だけ引用符 (旧 GAS と同じ)
 *     現場の値札印刷ソフトが読んでいるファイルなので、書式は旧 GAS に合わせる。
 *
 * このモジュールは「渡された行から作って上げる」だけで、nefuda.csv の取得はしない。
 * 取得〜生成を1回のダウンロード・1つのロックで通すため、呼び出しは nefuda-fetch.js に集約する
 * (Codex R1 High: 2回落とすと DB と値札CSVで別世代の nefuda.csv を掴み得る)。
 */
import iconv from 'iconv-lite';
import { irisuMap, saveNefudaPrintState, codeKey } from './db.js';
import { uploadCsvToDrive, probeFolderWritable } from '../fba-replenishment/drive-upload.js';

// 保存先は nefuda.csv と同じフォルダ。env で差し替え可 (nefuda-fetch.js と同じ既定値)
export const NEFUDA_PRINT_FOLDER_ID = () => process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA';
export const NEFUDA_PRINT_FILENAME = () => process.env.INBOUND_NEFUDA_PRINT_FILE || 'nefudaprint.csv';

export const OUT_HEADER = ['商品ID', '商品名', 'JAN', 'FNSKU', '有効期限', '入数'];

// 旧 GAS csvEscape_ と同じ規則: 「"」「,」「CR」「LF」を含む時だけ引用符で囲む
function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const hasAlpha = (v) => /[A-Za-z]/.test(String(v ?? '').trim());
const isDigitsOnly = (v) => /^[0-9]+$/.test(String(v ?? '').trim());
// Excel 等が数式として解釈し得る先頭文字 (Codex R1 Medium)。
// 旧 GAS と1バイトも変えない方針なので値は無害化せず、件数だけ数えて画面とログに出す。
const isFormulaLike = (v) => /^[=+\-@\t\r]/.test(String(v ?? ''));

/**
 * nefuda.csv のパース済み行から nefudaprint.csv のバイト列を組み立てる。
 * @param {Array<{商品コード:string, 商品名:*, バーコード:*, 有効期限:*}>} rows
 * @returns {{buffer:Buffer, rows:number, missingIrisu:number, formulaCells:number, text:string}}
 */
export function buildNefudaPrintCsv(rows) {
  const irisu = irisuMap();
  let missingIrisu = 0;
  let formulaCells = 0;
  const out = [OUT_HEADER];
  for (const r of rows) {
    const code = String(r.商品コード ?? '').trim();
    const barcode = String(r.バーコード ?? '').trim();
    // 英字を含む = Amazon の FNSKU、数字だけ = JAN。どちらでもなければ両方空
    const jan = !hasAlpha(barcode) && isDigitsOnly(barcode) ? barcode : '';
    const fnsku = hasAlpha(barcode) ? barcode : '';
    const n = irisu.get(codeKey(code));
    if (n == null) missingIrisu++;
    const line = [code, r.商品名 ?? '', jan, fnsku, r.有効期限 ?? '', n == null ? '' : n];
    formulaCells += line.filter(isFormulaLike).length;
    out.push(line);
  }
  // 旧 GAS と同じく CRLF 区切り・末尾改行なし
  const text = out.map((r) => r.map(csvEscape).join(',')).join('\r\n');
  return { buffer: iconv.encode(text, 'Shift_JIS'), rows: rows.length, missingIrisu, formulaCells, text };
}

/**
 * 組み立てた nefudaprint.csv を Drive に上書き保存し、結果を記録する。
 * ⚠ ロックは取らない。nefuda.csv を落とした呼び出し側 (nefuda-fetch.js) の
 *   runExclusive 区間の中で、同じダウンロード結果から呼ぶこと。
 * 生成・保存に失敗した場合は Drive 上の既存ファイルに触れないまま失敗を記録する。
 */
export async function uploadNefudaPrint(user, rows) {
  const filename = NEFUDA_PRINT_FILENAME();
  const folderId = NEFUDA_PRINT_FOLDER_ID();
  let built;
  try {
    built = buildNefudaPrintCsv(rows);
  } catch (e) {
    saveNefudaPrintState({ ok: false, error: e.message, filename, user });
    throw e;
  }
  let up;
  try {
    up = await uploadCsvToDrive(built.buffer, filename, folderId, 'text/csv');
  } catch (e) {
    // 書き込み権限が原因かを非破壊で切り分けて案内に添える (PDF 保存と同じ扱い)
    let hint = '';
    try {
      const probe = await probeFolderWritable(folderId);
      if (!probe.ok) hint = ` / 保存先の点検結果: ${probe.message || probe.stage}`;
    } catch { /* 点検自体が失敗しても元のエラーを返す */ }
    const msg = e.message + hint;
    saveNefudaPrintState({ ok: false, error: msg, filename, user });
    throw new Error(msg);
  }
  if (built.formulaCells > 0) {
    console.warn(`[inbound-info] ${filename}: 先頭が = + - @ (タブ・CRを含む) のセルが ${built.formulaCells} 個あります`
      + ' (Excel で開くと数式として解釈され得ます。値札ソフト向けの出力は旧GASと同じで無害化していません)');
  }
  const state = saveNefudaPrintState({
    ok: true, filename, rows: built.rows, missing_irisu: built.missingIrisu,
    formula_cells: built.formulaCells, bytes: built.buffer.length,
    file_id: up.fileId, folder_name: up.folderName, user,
  });
  return { ok: true, rows: built.rows, missingIrisu: built.missingIrisu, formulaCells: built.formulaCells,
           bytes: built.buffer.length, action: up.action, fileId: up.fileId, filename,
           folderName: up.folderName, saved_at: state.saved_at };
}
