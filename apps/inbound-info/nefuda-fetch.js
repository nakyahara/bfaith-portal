/**
 * 入荷予定 (nefuda.csv) の Drive 取得 — apps/inbound-info
 *
 * NE の値札発行用データ nefuda.csv (Shift-JIS、列: 商品ID/商品名/バーコード/有効期限) が
 * 共有ドライブ直下に置かれ、入荷のたびに上書き更新される運用。
 * これを「入荷予定」として取り込み、入庫情報一覧の絞り込みに使う。
 *
 * 取得経路は lib/drive-csv.js (packing-dispatch / purchase-orders と共通、
 * GOOGLE_SERVICE_ACCOUNT_KEY 認証・読み取り専用)。
 * サービスアカウントが対象共有ドライブのメンバー (閲覧者以上) である必要がある。
 *
 * 実行タイミング: 日次 cron (sync-job.js、JST 09:00) + UI「入荷予定を更新」ボタン。
 */
import { getDriveCsvInfo, downloadDriveCsv, vErr } from '../../lib/drive-csv.js';
import { parseCsv, decodeCp932 } from '../packing-dispatch/csv.js';
import { replaceSchedule, getJobSettings, saveNefudaPrintState } from './db.js';
import { runExclusive } from './job-lock.js';
import { uploadNefudaPrint, buildNefudaPrintCsv, NEFUDA_PRINT_FILENAME } from './nefuda-print.js';

// フォルダID は共有ドライブ直下 (2026-07-21 中原さん指定 URL の ID)。env で差し替え可。
const CFG = {
  label: '入荷予定 (nefuda.csv)',
  folderId: process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA',
  filename: process.env.INBOUND_NEFUDA_FILE || 'nefuda.csv',
  notFoundHint: '値札発行データ (nefuda.csv) が Drive に出力されているか確認してください。',
};

/** Drive 上の nefuda.csv の metadata (更新日時表示用、60秒キャッシュ) */
export async function getNefudaInfo() {
  return getDriveCsvInfo(CFG);
}

// CSV 契約 (fail-closed、Codex R1 Medium): 4列固定・ヘッダ名一意・行の列数一致。
// 未閉鎖引用符などの構文破損は「残り全部が1フィールドに飲み込まれる」ため列数チェックで検出される。
const REQUIRED_HEADERS = ['商品ID', '商品名', 'バーコード', '有効期限'];

/**
 * nefuda.csv の Buffer をパースして行配列に変換 (export はテスト用)。
 * データ行 0 件も取込拒否 (Codex R1 High): Drive 上書き途中・出力障害の一時的な
 * 空ファイルで既存の入荷予定スナップショットを全消去しないため。
 */
export function parseNefudaCsv(buffer) {
  const text = decodeCp932(buffer);
  const parseState = {};
  const all = parseCsv(text, parseState).filter((r) => r.some((f) => String(f).trim() !== ''));
  if (parseState.unclosedQuote) {
    throw vErr('nefuda.csv に閉じていない引用符があります (ファイル破損の可能性)。取込を中止し、前回の入荷予定を保持しました。');
  }
  if (all.length === 0) throw vErr('nefuda.csv が空です (ヘッダ行もありません)。前回の入荷予定を保持しました。');
  const header = all[0].map((h) => String(h).trim());
  for (const name of REQUIRED_HEADERS) {
    if (header.indexOf(name) === -1) {
      throw vErr(`nefuda.csv のヘッダに「${name}」列がありません (実際のヘッダ: ${header.join(' / ')})。前回の入荷予定を保持しました。`);
    }
    if (header.indexOf(name) !== header.lastIndexOf(name)) {
      throw vErr(`nefuda.csv のヘッダに「${name}」列が複数あります。前回の入荷予定を保持しました。`);
    }
  }
  const col = {
    code: header.indexOf('商品ID'),
    name: header.indexOf('商品名'),
    barcode: header.indexOf('バーコード'),
    expiry: header.indexOf('有効期限'),
  };
  const rows = [];
  for (let i = 1; i < all.length; i++) {
    const r = all[i];
    if (r.length !== header.length) {
      throw vErr(`nefuda.csv の ${i + 1} 行目の列数がヘッダと一致しません (${r.length}列/期待${header.length}列)。ファイル破損の可能性があるため取込を中止し、前回の入荷予定を保持しました。`);
    }
    const code = String(r[col.code] ?? '').trim();
    if (!code) continue;
    rows.push({
      商品コード: code,
      商品名: r[col.name],
      バーコード: r[col.barcode],
      有効期限: r[col.expiry],
    });
  }
  if (rows.length === 0) {
    throw vErr('nefuda.csv にデータ行がありません (ヘッダのみ)。出力障害の可能性があるため取込を中止し、前回の入荷予定を保持しました。');
  }
  return rows;
}

/**
 * Drive から nefuda.csv を落としてパースするところまで (export はロック外の共通部品)。
 * 値札印刷用CSV (nefuda-print.js) も同じ内容を正として使うのでここに切り出す。
 * ⚠ ロックは取らない。呼び出し側が runExclusive の中で使うこと。
 */
export async function downloadNefudaRows() {
  const dl = await downloadDriveCsv(CFG);
  return { rows: parseNefudaCsv(dl.buffer), file: dl };
}

/**
 * Drive から最新の nefuda.csv を取得して f_inbound_schedule を full-replace する。
 * cron と UI ボタンの共通実体。
 *
 * 同時実行は job-lock.js の共通 mutex で直列化する (PDF出力と同じロック。CSV更新と
 * PDF生成が交錯して古い内容で Drive を上書きするのを防ぐ — Codex pdf-R1 Medium)。
 */
export function refreshNefudaSchedule(user) {
  const run = async () => {
    let dl0;
    try {
      dl0 = await downloadNefudaRows();
    } catch (e) {
      recordPrintFetchFailure(user, e); // 値札CSVも作れていないので ⚙️タブに理由を残す
      throw e;
    }
    const { rows, file: dl } = dl0;
    const result = replaceSchedule(rows, {
      filename: dl.filename,
      fileModifiedTime: dl.modified_time,
      user,
    });
    return {
      ...result,
      // 値札印刷用CSV も「今落とした同じ nefuda.csv」から作る。落とし直すと DB と値札CSVで
      // 別世代を掴み得るため (Codex R1 High)。stale_file の時は作らない — 反映済みの方が
      // 新しい = 手元の rows は古い版なので、それで Drive を上書きすると巻き戻る。
      nefuda_print: result.ok ? await printFromRows(user, rows) : { skipped: 'stale_file' },
      file: { filename: dl.filename, modified_time: dl.modified_time, modified_time_jst: dl.modified_time_jst },
    };
  };
  return runExclusive(run);
}

/**
 * 値札印刷用CSV を作って Drive に保存する (画面設定が有効なときだけ)。
 * ここでの失敗は入荷予定の取り込み自体を失敗にしない (取り込みは既に済んでいるため)。
 * 結果は呼び出し元の戻り値と ⚙️タブの状態表示に出す。
 * ⚠ 呼び出し側の runExclusive 区間の中から呼ぶこと (uploadNefudaPrint はロックを取らない)。
 */
async function printFromRows(user, rows) {
  if (getJobSettings().nefuda_print_enabled === false) return { skipped: 'disabled' };
  try {
    const n = await uploadNefudaPrint(user, rows);
    return { ok: true, filename: n.filename, rows: n.rows, missing_irisu: n.missingIrisu,
             formula_cells: n.formulaCells, action: n.action };
  } catch (e) {
    console.error('[inbound-info] nefudaprint failed:', e.message);
    return { ok: false, filename: NEFUDA_PRINT_FILENAME(), error: e.message };
  }
}

/**
 * 入荷予定は取り込まず、値札印刷用CSV だけ作り直す (UI「今すぐ値札印刷用CSVを作成」ボタン)。
 * 入数を画面で直した後に刷り直したいケース用。nefuda.csv は最新を取り直す。
 */
export function regenerateNefudaPrint(user) {
  return runExclusive(async () => {
    let dl;
    try {
      dl = await downloadNefudaRows();
    } catch (e) {
      recordPrintFetchFailure(user, e);
      throw e;
    }
    return uploadNefudaPrint(user, dl.rows);
  });
}

/**
 * 保存せずに値札印刷用CSV の中身だけ組み立てる (UI「中身を確認 (保存しない)」)。
 * Drive は更新しないが、取得中に別経路のアップロードと交錯しないよう同じロックに入れる。
 * ⚠ 失敗しても f_inbound_nefuda_print_state は触らない。あの状態は「最後に保存した結果」で、
 *   保存を試みていないプレビューの失敗を書くと Drive 上のファイルが古いと誤解させるため
 *   (Codex R2 Medium への対応: ロックには入れるが状態記録はしない、が正しい扱い)。
 */
export function previewNefudaPrint() {
  return runExclusive(async () => {
    const { rows } = await downloadNefudaRows();
    return buildNefudaPrintCsv(rows);
  });
}

/**
 * nefuda.csv が取れない (未共有・破損・env未設定) 時に、値札CSVの状態へ理由を残す。
 * PDF と同じ考え方: Drive 上の既存ファイルには触らず「更新していない」ことを画面に出す。
 * 取得失敗はどの入口 (cron / 入荷予定取得ボタン / 今すぐ作成) でも同じ形で記録する。
 */
function recordPrintFetchFailure(user, e) {
  if (getJobSettings().nefuda_print_enabled === false) return;
  const filename = NEFUDA_PRINT_FILENAME();
  saveNefudaPrintState({
    ok: false, filename, user,
    error: `入荷予定(nefuda.csv)の取得に失敗したため、値札印刷用CSVは更新していません `
      + `(Drive上の${filename}は前回のまま): ${e.message}`,
  });
}
