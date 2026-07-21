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
import { replaceSchedule } from './db.js';

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

/**
 * nefuda.csv の Buffer をパースして行配列に変換 (export はテスト用)。
 * ヘッダは fail-closed: 「商品ID」列が無ければ取込拒否 (列順は問わずヘッダ名で解決)。
 */
export function parseNefudaCsv(buffer) {
  const text = decodeCp932(buffer);
  const all = parseCsv(text).filter((r) => r.some((f) => String(f).trim() !== ''));
  if (all.length === 0) throw vErr('nefuda.csv が空です (ヘッダ行もありません)。');
  const header = all[0].map((h) => String(h).trim());
  const col = {
    code: header.indexOf('商品ID'),
    name: header.indexOf('商品名'),
    barcode: header.indexOf('バーコード'),
    expiry: header.indexOf('有効期限'),
  };
  if (col.code === -1) {
    throw vErr(`nefuda.csv のヘッダに「商品ID」列がありません (実際のヘッダ: ${header.join(' / ')})`);
  }
  const rows = [];
  for (const r of all.slice(1)) {
    const code = String(r[col.code] ?? '').trim();
    if (!code) continue;
    rows.push({
      商品コード: code,
      商品名: col.name >= 0 ? r[col.name] : null,
      バーコード: col.barcode >= 0 ? r[col.barcode] : null,
      有効期限: col.expiry >= 0 ? r[col.expiry] : null,
    });
  }
  return rows;
}

/**
 * Drive から最新の nefuda.csv を取得して f_inbound_schedule を full-replace する。
 * cron と UI ボタンの共通実体。0件 (入荷予定なし) も正常として置換する。
 */
export async function refreshNefudaSchedule(user) {
  const dl = await downloadDriveCsv(CFG);
  const rows = parseNefudaCsv(dl.buffer);
  const result = replaceSchedule(rows, {
    filename: dl.filename,
    fileModifiedTime: dl.modified_time,
    user,
  });
  return {
    ...result,
    file: { filename: dl.filename, modified_time: dl.modified_time, modified_time_jst: dl.modified_time_jst },
  };
}
