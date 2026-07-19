/**
 * 発注書PDF生成の Node→Python ブリッジ (FAX送信用)。
 * python/render_order_pdf.py にJSONをstdinで渡し、PDFバイナリをstdoutで受け取る
 * (fba-replenishment/annotate-pdf.js と同じく Render では /app/venv の python を使う)。
 *
 * PO_PDF_FAKE=1: pythonを呼ばず固定のPDF様バイト列を返す (smokeテスト用。
 * 'fail'=生成失敗を再現)。ローカルWindowsでは python + reportlab があれば実生成できる。
 */
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.join(__dirname, 'python', 'render_order_pdf.py');

function pythonCmd() {
  if (process.env.RENDER) return path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  return process.platform === 'win32' ? 'python' : 'python3';
}

// 同時生成の上限 (プレビュー連打等でPython子プロセスを無制限に並列起動させない、Codex fax-R2 Low)
let activeRenders = 0;
const MAX_CONCURRENT = 3;

/**
 * @param {object} payload buildOrderFax() が組み立てた発注書データ (python側のJSONスキーマ)
 * @param {{ timeoutMs?: number }} opts
 * @returns {Promise<Buffer>} PDFバイナリ
 */
export async function renderOrderPdf(payload, { timeoutMs = 30000 } = {}) {
  if (process.env.PO_PDF_FAKE) {
    if (process.env.PO_PDF_FAKE === 'fail') throw new Error('偽PDF生成エラー (テスト)');
    // %PDF ヘッダを持つ最小スタブ (実PDFではないがMIME組立・保存・配信経路の検証には十分)
    return Buffer.from(`%PDF-1.4 fake\n${JSON.stringify({ po: payload.po_number, rows: payload.items.length })}\n%%EOF\n`, 'utf8');
  }
  if (activeRenders >= MAX_CONCURRENT) {
    throw new Error('PDF生成が混み合っています (同時実行上限)。数秒待ってからやり直してください');
  }
  activeRenders++;
  try {
    return await renderViaPython(payload, timeoutMs);
  } finally {
    activeRenders--;
  }
}

function renderViaPython(payload, timeoutMs) {
  return new Promise((resolve, reject) => {
    const cp = spawn(pythonCmd(), [SCRIPT], { stdio: ['pipe', 'pipe', 'pipe'] });
    const MAX_PDF = 20 * 1024 * 1024; // 発注書PDFの想定は数十KB。20MB超は異常出力としてkill (メモリ保護)
    const out = [];
    let outBytes = 0, err = '';
    const timer = setTimeout(() => { cp.kill('SIGKILL'); reject(new Error('発注書PDF生成がタイムアウトしました (30秒)')); }, timeoutMs);
    cp.stdout.on('data', (d) => {
      outBytes += d.length;
      if (outBytes > MAX_PDF) { clearTimeout(timer); cp.kill('SIGKILL'); return reject(new Error('PDF生成の出力が大きすぎます (20MB超) — 明細数を確認してください')); }
      out.push(d);
    });
    cp.stderr.on('data', (d) => { if (err.length < 65536) err += d; });
    cp.on('error', (e) => { clearTimeout(timer); reject(new Error(`PDF生成プロセス起動失敗: ${e.message} (ローカル実行には python + reportlab が必要です)`)); });
    cp.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error(`発注書PDF生成に失敗 (exit ${code}): ${err.slice(0, 500)}`));
      const buf = Buffer.concat(out);
      // 出力の破損検知 (空・PDFヘッダ欠落をそのまま送らない)
      if (buf.length < 100 || !buf.subarray(0, 5).equals(Buffer.from('%PDF-'))) {
        return reject(new Error(`PDF生成の出力が不正です (${buf.length} bytes): ${err.slice(0, 200)}`));
      }
      resolve(buf);
    });
    cp.stdin.on('error', () => {}); // 起動失敗時のEPIPEはcloseで拾う
    cp.stdin.end(JSON.stringify(payload));
  });
}
