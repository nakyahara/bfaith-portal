/**
 * ABA週次レポートのファイル保管 (最新1本だけ)
 *
 * 蓄積しない設計 (2026-07-27 中原さん方針) の土台:
 *   - 週次レポートは DB に貯めず、最新週のレポートファイルだけ data/aba-reports/ に残す
 *   - 初めて照会された ASIN は router がこのファイルをその場でスキャンして行を拾う
 *   - ファイル名 = <week_start>.json / <week_start>.json.gz (DL時の圧縮のまま保存)
 */
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import { createGunzip } from 'zlib';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
export const REPORTS_DIR = path.join(DATA_DIR, 'aba-reports');

const WEEK_FILE_RE = /^(\d{4}-\d{2}-\d{2})\.json(\.gz)?$/;

export function ensureReportsDir() {
  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });
}

/**
 * レポートDL → ファイルへ保存 (bytesはDLのまま、tmp→rename で中途半端なファイルを残さない)
 * @returns {string} 保存先パス
 */
export async function downloadReportToFile(docUrl, isGzip, weekStart) {
  ensureReportsDir();
  const finalPath = path.join(REPORTS_DIR, `${weekStart}.json${isGzip ? '.gz' : ''}`);
  const tmpPath = `${finalPath}.tmp-${process.pid}`;
  const res = await fetch(docUrl, { signal: AbortSignal.timeout(15 * 60 * 1000) });
  if (!res.ok || !res.body) throw new Error(`レポートDL失敗: HTTP ${res.status}`);
  try {
    await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmpPath));
    fs.renameSync(tmpPath, finalPath);
  } catch (e) {
    try { fs.unlinkSync(tmpPath); } catch { /* 無ければ無視 */ }
    throw e;
  }
  return finalPath;
}

/** 保存済みレポートファイルの読み取りストリーム (gz透過) */
export function openReportStream(filePath) {
  const src = fs.createReadStream(filePath);
  return filePath.endsWith('.gz') ? src.pipe(createGunzip()) : src;
}

/** 最新週のレポートファイル。無ければ null */
export function findLatestReportFile() {
  ensureReportsDir();
  const files = fs.readdirSync(REPORTS_DIR)
    .map((name) => {
      const m = name.match(WEEK_FILE_RE);
      return m ? { week: m[1], path: path.join(REPORTS_DIR, name) } : null;
    })
    .filter(Boolean)
    .sort((a, b) => (a.week < b.week ? 1 : -1));
  return files[0] || null;
}

/** 最新 keepN 週分以外のレポートファイルと残骸tmpを削除 */
export function cleanupReportFiles(keepN = 1) {
  ensureReportsDir();
  const entries = fs.readdirSync(REPORTS_DIR);
  const weeks = entries
    .map((name) => { const m = name.match(WEEK_FILE_RE); return m ? { name, week: m[1] } : null; })
    .filter(Boolean)
    .sort((a, b) => (a.week < b.week ? 1 : -1));
  for (const f of weeks.slice(keepN)) {
    try { fs.unlinkSync(path.join(REPORTS_DIR, f.name)); } catch { /* 使用中等は次回 */ }
  }
  for (const name of entries) {
    if (name.includes('.tmp-')) {
      try { fs.unlinkSync(path.join(REPORTS_DIR, name)); } catch { /* 無視 */ }
    }
  }
}
