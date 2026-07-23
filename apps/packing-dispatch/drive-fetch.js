/**
 * Google Drive からヤマトB2「発行済データ」CSV 等を直接取得する (packing-dispatch キャリアCSV取込用)
 *
 * B2クラウドから自動DLされた CSV が Drive の固定フォルダに固定ファイル名で置かれる運用。
 * 手元にDLして手動アップロードする手間を省くため、Render サーバが Drive API (読み取り専用) で
 * 直接ダウンロードして importTrackingCsv に流す。
 *
 * Drive アクセスの実体は lib/drive-csv.js (purchase-orders のロジザード在庫CSV取込と共通)。
 * このファイルは「source 名 → 取得先」の対応表と、全 source 一括取得の薄いラッパのみを持つ。
 */
import { getDriveCsvInfo, downloadDriveCsv as downloadCsv, vErr } from '../../lib/drive-csv.js';

// source → 取得先。フォルダID/ファイル名は運用固定 (2026-07-18 中原さん指定)。env で差し替え可。
const DRIVE_SOURCES = {
  yamato_b2: {
    label: 'ヤマト B2 (ネコポス・発払い)',
    folderId: process.env.PD_DRIVE_FOLDER_YAMATO_B2 || '1X8LISS4Ck0mohW_7x7BSHNBebZF3VwPn',
    filename: process.env.PD_DRIVE_FILE_YAMATO_B2 || 'ネコ・60サイズ31項目4項目_発行済データ.csv',
    notFoundHint: 'B2クラウドからのDLが済んでいるか確認してください。',
  },
  yamato_b2_50: {
    label: 'ヤマト B2 (50サイズ専用)',
    folderId: process.env.PD_DRIVE_FOLDER_YAMATO_B2_50 || '1F_DWgsFs16002cLUK7_7o_GmM5_CDY31',
    filename: process.env.PD_DRIVE_FILE_YAMATO_B2_50 || '50サイズ31項目4項目_発行済データ.csv',
    notFoundHint: 'B2クラウドからのDLが済んでいるか確認してください。',
  },
  yupacketpuff: {
    label: 'ゆうパケットパフ',
    // ファイル名末尾のタイムスタンプは初回出力時のもので固定 (ゆうプリRが同名上書き更新する運用を実機確認済み 2026-07-18)。
    // 「＿」(全角) と「_」(半角) が混在している点に注意 — Drive 上の実ファイル名そのまま。
    folderId: process.env.PD_DRIVE_FOLDER_YUPACKETPUFF || '1V-4iZWnmi9E2Bi90a2JlTUzqsL3V_Nsu',
    filename: process.env.PD_DRIVE_FILE_YUPACKETPUFF || 'ゆうプリR出荷履歴＿2項目3項目_20250714102302.csv',
    notFoundHint: 'ゆうプリRからのDLが済んでいるか確認してください。',
  },
};

export const DRIVE_IMPORT_SOURCES = Object.keys(DRIVE_SOURCES);

function getSourceConfig(source) {
  const cfg = DRIVE_SOURCES[source];
  if (!cfg) throw vErr(`Drive 取込に対応していない source です: ${source}`);
  return cfg;
}

/**
 * 全 source の metadata をまとめて返す (UI の「更新日時」表示用、1 リクエストで完結)。
 * 個別失敗は他 source を巻き込まない ({ ok:false, message } で返す)。
 */
export async function getDriveCsvInfoAll() {
  const entries = await Promise.all(DRIVE_IMPORT_SOURCES.map(async (source) => {
    try {
      return [source, { ok: true, source, ...(await getDriveCsvInfo(getSourceConfig(source))) }];
    } catch (e) {
      return [source, { ok: false, message: e.message }];
    }
  }));
  return Object.fromEntries(entries);
}

/** CSV 本体をDLして Buffer 付き metadata を返す。 */
export async function downloadDriveCsv(source) {
  const info = await downloadCsv(getSourceConfig(source));
  return { source, ...info };
}
