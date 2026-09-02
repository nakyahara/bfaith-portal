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
import { getDriveCsvInfo, downloadDriveCsv, vErr } from '../../lib/drive-csv.js';
import { importCsv, getActiveBatch } from './db.js';
import { importProductMaster, productMasterStatus } from './product-master.js';
import crypto from 'crypto';

const CFG = {
  label: '入荷受付CSV (nyuka_uketsuke.csv)',
  folderId: process.env.INBOUND_CHECK_DRIVE_FOLDER_ID || process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA',
  filename: process.env.INBOUND_CHECK_DRIVE_FILE || 'nyuka_uketsuke.csv',
  notFoundHint: 'miniPC の auto-nyuka-csv.js が Drive へ出力できているか確認してください '
    + '(ロジザード 入荷状況照会[FA04_01] → 受付済 → 出力)。',
};

// 商品マスタ (エクスポート[FM08_01] 種類=商品 / パターン=デフォルト)。
// ⭐これを取る目的は「期限管理あり/なし」の正本を得ることだけ (入荷受付CSVには出てこない)。
// 商品マスタは日に何度も変わるものではないので、内容が変わったときだけ取り込む
const MASTER_CFG = {
  label: 'ロジザード商品マスタ (shohin_master.csv)',
  folderId: process.env.INBOUND_CHECK_DRIVE_FOLDER_ID || process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA',
  filename: process.env.INBOUND_CHECK_MASTER_FILE || 'shohin_master.csv',
  notFoundHint: 'miniPC の auto-shohin-csv.js が Drive へ出力できているか確認してください '
    + '(ロジザード エクスポート[FM08_01] → 種類=商品 / パターン=デフォルト)。',
};

export function driveConfig() {
  return { ...CFG };
}

export function masterDriveConfig() {
  return { ...MASTER_CFG };
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
  // downloadDriveCsv は { buffer, info } を返す (info は DL 直後に取り直した metadata)。
  // 本文と世代を必ず揃えるため、生成時刻はこの info から採る (Codex R6 High-4)
  // downloadDriveCsv は { ...info, buffer } を返す (info は DL 直後に「差し替わっていない」ことを
  // 確認済みの metadata)。本文と世代が必ず揃うので、生成時刻はここから採る (Codex R6 High-4)
  const dl = await downloadDriveCsv(CFG);
  const buffer = dl.buffer;
  const modified = dl.modified_time || dl.modifiedTime || null;
  if (!modified) throw vErr(`${CFG.filename} の更新日時を取得できませんでした (取込を中止しました)`);
  // 未来時刻は受け付けない (時計ずれや誤操作で以後の取込が全部拒否されるのを防ぐ)
  const skewMs = Date.parse(modified) - Date.now();
  if (Number.isFinite(skewMs) && skewMs > 10 * 60 * 1000) {
    throw vErr(`${CFG.filename} の更新日時が未来です (${modified})。時計を確認してください`);
  }
  const r = importCsv(buffer, {
    fileName: dl.name || info.name || CFG.filename,
    source,
    actor,
    generatedAt: modified,
  });
  return { ...r, driveModifiedTime: modified, fileName: dl.name || CFG.filename };
}

// ─── 商品マスタ (期限管理あり/なし) ───
// 同じ内容なら取り込まない。中身のハッシュで見るので、Drive 上で再アップロードされただけでは動かない
let _lastMasterHash = null;
let _lastMasterOk = null;

/**
 * Drive の商品マスタを取り込む。
 * @param {object} o { actor, force }  force=true で同じ内容でも取り込み直す (管理画面の「今すぐ」)
 * @returns {{ok, skipped?, message?, ...importProductMaster の結果}}
 */
export async function fetchAndImportProductMaster({ actor = 'cron', force = false } = {}) {
  const info = await getDriveCsvInfo(MASTER_CFG);
  if (!info || !info.file_id) {
    throw vErr(`${MASTER_CFG.filename} が Drive に見つかりません。${MASTER_CFG.notFoundHint}`);
  }
  const dl = await downloadDriveCsv(MASTER_CFG);
  const hash = crypto.createHash('sha256').update(dl.buffer).digest('hex');
  if (!force && hash === _lastMasterHash) {
    return { ok: true, skipped: true, message: '商品マスタは前回と同じ内容でした' };
  }
  const r = importProductMaster(dl.buffer, { actor });
  _lastMasterHash = hash;
  _lastMasterOk = new Date().toISOString();
  return { ...r, driveModifiedTime: dl.modified_time || dl.modifiedTime || null, fileName: dl.name || MASTER_CFG.filename };
}

/** 定期取込 (cron から)。例外は投げず結果を返す — 入荷CSVの巡回を止めないため */
let _fetchingMaster = null;
export async function runScheduledMasterFetch({ actor = 'cron' } = {}) {
  if (_fetchingMaster) return _fetchingMaster;
  _fetchingMaster = (async () => {
    try {
      const r = await fetchAndImportProductMaster({ actor });
      if (r.skipped) return r;
      console.log(`[inbound-check] 商品マスタ取込: ${r.total}件 (期限管理あり ${r.managed}件 / 変化 ${r.changed}件`
        + `${r.overroteManual ? ` / 手動設定を ${r.overroteManual}件 上書き` : ''})`);
      return r;
    } catch (e) {
      // 商品マスタが取れなくても、在庫からの推定と手動設定で動き続ける (作業は止めない)
      console.warn(`[inbound-check] 商品マスタの取得に失敗: ${e.message}`);
      return { ok: false, error: 'drive_error', message: e.message };
    }
  })();
  try { return await _fetchingMaster; } finally { _fetchingMaster = null; }
}

export function lastMasterFetchAt() { return _lastMasterOk; }

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
  let master = null, masterError = null;
  try {
    master = await getDriveCsvInfo(MASTER_CFG);
  } catch (e) {
    masterError = e.message;
  }
  return {
    active, drive, driveError, config: driveConfig(), lastFetchOkAt: _lastOk,
    master, masterError, masterConfig: masterDriveConfig(),
    masterStatus: productMasterStatus(), lastMasterOkAt: _lastMasterOk,
  };
}
