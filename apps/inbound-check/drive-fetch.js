/**
 * 入荷受付チェック — Drive からの自動取込
 *
 * miniPC の `auto-nyuka-csv.js` が「入荷状況照会[FA04_01] / 受付済 / 当日から7日前まで」で出した CSV を
 * rclone で共有ドライブへ置き、それを Render 側が取りに来る (値札 CSV = nefuda.csv と同じ経路)。
 *
 * 設計の要点:
 *  - 取得は `lib/drive-csv.js` (packing-dispatch / purchase-orders / inbound-info と共通の基盤)
 *  - **同じ内容 (ハッシュ) なら取り込まない** — 取込側 (db.js importCsv) が duplicate_file を返すので、
 *    毎回の巡回で無駄なバッチを増やさない
 *  - CSV 生成時刻には Drive の modifiedTime を使う。⚠これは「ファイルが Drive 上で更新された時刻」であって
 *    改ざんできない値ではない (再アップロードやコピーで動く)。主たる新旧判定は取込側が見る**明細の更新日時**で、
 *    modifiedTime は明細が無い日 (0件CSV) の補助。未来時刻は受け付けない (取り込みが長期間止まるのを防ぐ)
 *  - 取込の fail-closed 判定は db.js 側が持つ (必須列・列数・暦日・0件正常・明細時刻の巻き戻り)。
 *    ここは「取ってきて渡す」だけに徹する
 */
import { getDriveCsvInfo, downloadDriveCsv, findDriveFile, vErr } from '../../lib/drive-csv.js';
import { importCsv, getActiveBatch } from './db.js';

const CFG = {
  label: '入荷受付CSV (nyuka_uketsuke.csv)',
  folderId: process.env.INBOUND_CHECK_DRIVE_FOLDER_ID || process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA',
  filename: process.env.INBOUND_CHECK_DRIVE_FILE || 'nyuka_uketsuke.csv',
  notFoundHint: 'miniPC の auto-nyuka-csv.js が Drive へ出力できているか確認してください '
    + '(ロジザード 入荷状況照会[FA04_01] → 受付済 → 出力)。',
};

export function driveConfig() {
  return { ...CFG };
}

/** Drive 上のファイルの情報 (更新日時の表示用。60秒キャッシュ) */
export async function getDriveInfo() {
  return getDriveCsvInfo(CFG);
}

/**
 * Drive から取得して取り込む。
 * @param {object} o { actor, source }  source は 'auto' (定期) か 'drive_retry' (画面の再取込)
 * @returns importCsv の結果 + { driveModifiedTime, fileName }
 */
export async function fetchAndImportFromDrive({ actor = null, source = 'auto' } = {}) {
  const info = await getDriveInfo();
  if (!info || !info.file_id) {
    throw vErr(`${CFG.filename} が Drive に見つかりません。${CFG.notFoundHint}`);
  }
  const buffer = await downloadDriveCsv(CFG);
  // ダウンロード後に metadata を取り直して照合する (60秒キャッシュ越しの info と
  // 実際に落とした本文の世代がずれるのを防ぐ — Codex R6 High-4)。
  // downloadDriveCsv 自身も DL 中の差し替えを1回リトライするが、ここは「表示・判定に使う時刻」を本文と揃えるため
  const after = await findDriveFile(CFG);
  const modified = after?.modified_time || after?.modifiedTime || null;
  if (!modified) throw vErr(`${CFG.filename} の更新日時を取得できませんでした (取込を中止しました)`);
  // 未来時刻は受け付けない (時計ずれや誤操作で以後の取込が全部拒否されるのを防ぐ)
  const skewMs = Date.parse(modified) - Date.now();
  if (Number.isFinite(skewMs) && skewMs > 10 * 60 * 1000) {
    throw vErr(`${CFG.filename} の更新日時が未来です (${modified})。時計を確認してください`);
  }
  const r = importCsv(buffer, {
    fileName: after?.name || info.name || CFG.filename,
    source,
    actor,
    generatedAt: modified,
  });
  return { ...r, driveModifiedTime: modified, fileName: after?.name || CFG.filename };
}

// プロセス内の実行中フラグ。cron の周期より処理が長引いても重ねない (Codex R6 High-3/Med-6)。
// ⚠ Render が複数インスタンスになった場合の重複は、DB 側の UNIQUE(file_hash) と immediate tx が受け止める
//    (同じファイルは duplicate_file になり、active の切替も単一トランザクション)
let _fetching = null;
let _lastOk = null;

/** 最後に取り込めた時刻 (画面と監視用)。null = このプロセスが起動してから一度も成功していない */
export function lastFetchSuccessAt() { return _lastOk; }

/**
 * 定期取込 (cron から呼ぶ)。例外は投げず結果を返す — 巡回を止めないため。
 * 「同じファイルなので取り込まなかった」は正常 (ok:false, error:'duplicate_file')。
 */
export async function runScheduledFetch({ actor = 'cron' } = {}) {
  if (_fetching) return _fetching;   // 実行中なら同じ結果を待つ (多重取得しない)
  _fetching = (async () => {
  try {
    const r = await fetchAndImportFromDrive({ actor, source: 'auto' });
    if (r.ok) {
      _lastOk = new Date().toISOString();
      console.log(`[inbound-check] 取込: ${r.slipCount}伝票 / ${r.rowCount}行 (batch=${r.batch.id})`);
    } else if (r.error === 'duplicate_file') {
      // 何も変わっていない = 正常 (Drive のファイルがまだ更新されていない)。ログは出さない
      _lastOk = new Date().toISOString();
    } else {
      console.warn(`[inbound-check] 取込しませんでした: ${r.message}`);
    }
    return r;
  } catch (e) {
    console.warn(`[inbound-check] Drive 取得に失敗: ${e.message}`);
    return { ok: false, error: 'drive_error', message: e.message };
  }
  })();
  try { return await _fetching; } finally { _fetching = null; }
}

/** 画面に出す現状 (最終取込・Drive の更新日時) */
export async function statusForView() {
  const active = getActiveBatch();
  let drive = null, driveError = null;
  try {
    drive = await getDriveInfo();
  } catch (e) {
    driveError = e.message;
  }
  return { active, drive, driveError, config: driveConfig(), lastFetchOkAt: _lastOk };
}
