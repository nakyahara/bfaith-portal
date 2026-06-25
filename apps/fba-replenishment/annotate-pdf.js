/**
 * TMP1 PDF 注番の Node→Python ブリッジ。
 * python/annotate_picking_pdf.py を子プロセスで実行し、納品プランNo列を追記したPDFを返す。
 * (aes-pdf-sorter と同じく Render では /app/venv の python を使う)
 */
import { spawn } from 'node:child_process';
import { promises as fsp } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.join(__dirname, 'python', 'annotate_picking_pdf.py');

function pythonCmd() {
  if (process.env.RENDER) return path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  return process.platform === 'win32' ? 'python' : 'python3';
}

/**
 * @param {Buffer} tmp1Buffer ロジザード TMP1 PDF
 * @param {Buffer} mappingCsvBuffer 商品ID→納品プランNo の CSV (SJIS/UTF-8 どちらでも可)
 * @param {{ timeoutMs?: number }} opts
 * @returns {Promise<{ pdfBuffer: Buffer, matched: number, total: number, unmatched: string[] }>}
 */
export async function annotatePickingPdf(tmp1Buffer, mappingCsvBuffer, { timeoutMs = 60000 } = {}) {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'pp-annot-'));
  const pdfIn = path.join(dir, 'in.pdf');
  const mapCsv = path.join(dir, 'map.csv');
  const pdfOut = path.join(dir, 'out.pdf');
  try {
    await fsp.writeFile(pdfIn, tmp1Buffer);
    await fsp.writeFile(mapCsv, mappingCsvBuffer);

    const stats = await new Promise((resolve, reject) => {
      const cp = spawn(pythonCmd(), [SCRIPT, '--pdf', pdfIn, '--mapping', mapCsv, '--out', pdfOut], {
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '', err = '';
      const timer = setTimeout(() => { cp.kill('SIGKILL'); reject(new Error('PDF注番がタイムアウトしました')); }, timeoutMs);
      cp.stdout.on('data', (d) => { out += d; });
      cp.stderr.on('data', (d) => { err += d; });
      cp.on('error', (e) => { clearTimeout(timer); reject(new Error(`PDF注番プロセス起動失敗: ${e.message}`)); });
      cp.on('close', (code) => {
        clearTimeout(timer);
        if (code !== 0) return reject(new Error(`PDF注番に失敗 (exit ${code}): ${(err || out).slice(0, 500)}`));
        try { resolve(JSON.parse(out || '{}')); } catch { resolve({}); }
      });
    });

    const pdfBuffer = await fsp.readFile(pdfOut);
    return {
      pdfBuffer,
      matched: stats.matched || 0,
      total: stats.total || 0,
      unmatched: Array.isArray(stats.unmatched) ? stats.unmatched : [],
    };
  } finally {
    fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}
