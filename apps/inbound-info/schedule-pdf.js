/**
 * 入荷予定リスト PDF の生成 → Google Drive 上書き保存 (apps/inbound-info)
 *
 * 画面の「🖨 印刷」と同じ内容 (入荷予定のみ・nefuda.csv の並び順) を PDF にして、
 * nefuda.csv と同じ共有ドライブフォルダに固定ファイル名で上書き保存する。
 *
 * 経路:
 *   listInbound({filter:'scheduled', limit:'all'})
 *     → python/render_schedule_pdf.py (reportlab、Docker の fonts-takao-gothic で日本語)
 *     → uploadCsvToDrive(..., 'application/pdf') で Drive 上書き
 *
 * 前提: サービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) が保存先の共有ドライブの
 *   **コンテンツ管理者** であること。nefuda.csv の読み取りは閲覧者権限でも通るが、
 *   PDF 保存には書き込み権限が必要 (失敗時は probeFolderWritable の結果を添えて返す)。
 *
 * INBOUND_PDF_FAKE=1 … python を呼ばずスタブPDFを返す (テスト用。'fail'=生成失敗を再現)
 */
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { listInbound, scheduleState, savePdfState } from './db.js';
import { uploadCsvToDrive, probeFolderWritable } from '../fba-replenishment/drive-upload.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.join(__dirname, 'python', 'render_schedule_pdf.py');

// 保存先は nefuda.csv と同じフォルダ (中原さん指定: 同じフォルダ内でいい)。env で差し替え可。
export const PDF_FOLDER_ID = () => process.env.INBOUND_NEFUDA_FOLDER_ID || '0AOG4tof0TAHFUk9PVA';
export const PDF_FILENAME = () => process.env.INBOUND_SCHEDULE_PDF_FILE || '入荷予定リスト.pdf';

// 画面の印刷と同じ列。width は mm (A4横 297mm - 余白16mm = 281mm に収まるようにする)
const COLUMNS = [
  { label: '商品コード', key: '商品コード', width: 26 },
  { label: '商品名', key: '商品名', width: 92 },
  { label: '入数', key: '入数', width: 12, align: 'right' },
  { label: 'BCシール', key: '入庫時BCシール貼りフラグ', width: 26 },
  { label: '直接ピックロケ', key: '直接ピックロケ保管', width: 26 },
  { label: 'BF保管荷姿', key: 'BF保管荷姿', width: 26 },
  { label: 'いろは在庫化', key: 'いろは在庫化作業有無', width: 22 },
  { label: 'memo', key: 'memo', width: 51 },
];

function pythonCmd() {
  if (process.env.RENDER) return path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  return process.platform === 'win32' ? 'python' : 'python3';
}

function jstStamp(iso) {
  const d = iso ? new Date(Date.parse(iso) + 9 * 3600 * 1000) : new Date(Date.now() + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}/${p(d.getUTCMonth() + 1)}/${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

/** python で PDF を生成 (stdin JSON → stdout PDF)。fax-pdf.js と同じ作り */
function renderViaPython(payload, timeoutMs) {
  return new Promise((resolve, reject) => {
    const cp = spawn(pythonCmd(), [SCRIPT], { stdio: ['pipe', 'pipe', 'pipe'] });
    const MAX_PDF = 40 * 1024 * 1024; // 想定は数百KB。異常出力でメモリを食い潰さないための保護
    const out = [];
    let outBytes = 0;
    let err = '';
    const timer = setTimeout(() => {
      cp.kill('SIGKILL');
      reject(new Error(`入荷予定PDFの生成がタイムアウトしました (${Math.round(timeoutMs / 1000)}秒)`));
    }, timeoutMs);
    cp.stdout.on('data', (d) => {
      outBytes += d.length;
      if (outBytes > MAX_PDF) {
        clearTimeout(timer);
        cp.kill('SIGKILL');
        return reject(new Error('PDF生成の出力が大きすぎます (40MB超)'));
      }
      out.push(d);
    });
    cp.stderr.on('data', (d) => { if (err.length < 65536) err += d; });
    cp.on('error', (e) => {
      clearTimeout(timer);
      reject(new Error(`PDF生成プロセス起動失敗: ${e.message} (ローカル実行には python + reportlab が必要)`));
    });
    cp.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error(`入荷予定PDFの生成に失敗 (exit ${code}): ${err.slice(0, 500)}`));
      const buf = Buffer.concat(out);
      if (buf.length < 100 || !buf.subarray(0, 5).equals(Buffer.from('%PDF-'))) {
        return reject(new Error(`PDF生成の出力が不正です (${buf.length} bytes): ${err.slice(0, 200)}`));
      }
      resolve(buf);
    });
    cp.stdin.on('error', () => {}); // 起動失敗時の EPIPE は close で拾う
    cp.stdin.end(JSON.stringify(payload));
  });
}

/** 入荷予定 (CSV順) の PDF バイナリを組み立てる。rows も返す (件数表示用) */
export async function buildSchedulePdf({ timeoutMs = 60000 } = {}) {
  const list = listInbound({ filter: 'scheduled', limit: 'all' });
  if (list.error) throw new Error(`入荷予定の件数が多すぎます (${list.total}件)`);
  const st = scheduleState();
  const meta = [
    `入荷予定 ${list.rows.length}件 (nefuda.csv の並び順)`,
    st ? `CSV取得 ${jstStamp(st.fetched_at)}` : 'CSV未取得',
    `PDF作成 ${jstStamp(null)}`,
  ];
  const payload = { title: '入荷予定リスト', meta, columns: COLUMNS, rows: list.rows };
  if (process.env.INBOUND_PDF_FAKE) {
    if (process.env.INBOUND_PDF_FAKE === 'fail') throw new Error('偽PDF生成エラー (テスト)');
    const stub = Buffer.from(`%PDF-1.4 fake\n${JSON.stringify({ rows: list.rows.length })}\n%%EOF\n`, 'utf8');
    return { buffer: stub, rows: list.rows.length };
  }
  const buffer = await renderViaPython(payload, timeoutMs);
  return { buffer, rows: list.rows.length };
}

// 同時実行の直列化 (cron と UI ボタンが重なっても Drive を二重更新しない)
let _chain = Promise.resolve();

/**
 * PDF を作って Drive に上書き保存する。cron / UI ボタン共通の実体。
 * 結果は f_inbound_pdf_state に記録し、画面に「最終保存」を出せるようにする。
 * @returns {Promise<{ok:true, rows:number, bytes:number, action:'updated'|'created', fileId:string, filename:string, folderName:string|null, saved_at:string}>}
 */
export function exportSchedulePdfToDrive(user) {
  const run = async () => {
    const filename = PDF_FILENAME();
    const folderId = PDF_FOLDER_ID();
    let built;
    try {
      built = await buildSchedulePdf();
    } catch (e) {
      savePdfState({ ok: false, error: e.message, filename, user });
      throw e;
    }
    let up;
    try {
      up = await uploadCsvToDrive(built.buffer, filename, folderId, 'application/pdf');
    } catch (e) {
      // 書き込み権限が原因かを非破壊で切り分けて、案内文に添える (SA は閲覧者だと保存できない)
      let hint = '';
      try {
        const probe = await probeFolderWritable(folderId);
        if (!probe.ok) hint = ` / 保存先の点検結果: ${probe.message || probe.stage}`;
      } catch { /* 点検自体が失敗しても元のエラーを返す */ }
      const msg = e.message + hint;
      savePdfState({ ok: false, error: msg, filename, user });
      throw new Error(msg);
    }
    const state = savePdfState({
      ok: true, filename, rows: built.rows, bytes: built.buffer.length,
      file_id: up.fileId, folder_name: up.folderName, user,
    });
    return { ok: true, rows: built.rows, bytes: built.buffer.length, action: up.action,
             fileId: up.fileId, filename, folderName: up.folderName, saved_at: state.saved_at };
  };
  const p = _chain.then(run, run);
  _chain = p.catch(() => {}); // 失敗しても次の実行を塞がない
  return p;
}
