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

// cron と UI ボタンの同時実行を直列化するプロセス内 mutex (Codex R1 Medium)。
// 単一プロセス前提 (Render 1 instance)。DB 側にも file_modified_time の鮮度ガードがあり二重防御。
let _refreshChain = Promise.resolve();

/**
 * Drive から最新の nefuda.csv を取得して f_inbound_schedule を full-replace する。
 * cron と UI ボタンの共通実体。
 */
export function refreshNefudaSchedule(user) {
  const run = async () => {
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
  };
  const p = _refreshChain.then(run, run);
  _refreshChain = p.catch(() => {}); // 失敗しても次の実行を塞がない
  return p;
}
