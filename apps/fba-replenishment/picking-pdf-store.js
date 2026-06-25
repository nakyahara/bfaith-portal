/**
 * 注番済み TMP1 PDF の永続化 (DATA_DIR 上のファイル)。
 * sql.js は DB 全体をメモリに載せるため、PDF(バイナリ)は DB に入れずファイルで保持する。
 * run id をファイル名にし、直近 KEEP 件だけ残す (履歴と整合)。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const PDF_DIR = path.join(DATA_DIR, 'picking-prep-pdf');
const KEEP = 40;

const safeId = (runId) => String(runId).replace(/[^0-9]/g, ''); // パストラバーサル防止

export function savePickingPdf(runId, buffer) {
  const id = safeId(runId);
  if (!id) throw new Error('invalid runId');
  fs.mkdirSync(PDF_DIR, { recursive: true });
  fs.writeFileSync(path.join(PDF_DIR, `${id}.pdf`), buffer);
  prune();
}

// 存在すれば絶対パス、無ければ null
export function pickingPdfPath(runId) {
  const id = safeId(runId);
  if (!id) return null;
  const p = path.join(PDF_DIR, `${id}.pdf`);
  return fs.existsSync(p) ? p : null;
}

function prune() {
  try {
    const files = fs.readdirSync(PDF_DIR)
      .filter((f) => /^\d+\.pdf$/.test(f))
      .map((f) => ({ f, id: parseInt(f, 10) }))
      .sort((a, b) => b.id - a.id);
    for (const x of files.slice(KEEP)) {
      try { fs.unlinkSync(path.join(PDF_DIR, x.f)); } catch { /* ignore */ }
    }
  } catch { /* dir 無し等は無視 */ }
}
