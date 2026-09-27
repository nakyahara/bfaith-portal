/**
 * lz-shadow-snapshot.mjs — ロジザード用 CSV の影運転の材料を miniPC で読む (読むだけ。③b-2a。apps/master-decisions/lz-snapshot.mjs)
 *
 * 使い方 (miniPC・リポジトリ直下):
 *   node scripts/company-db/lz-shadow-snapshot.mjs --out <ファイル> [--data-dir D]
 *   (--out - = 標準出力に JSON。要約の 1 行は標準エラーに出す)
 * env: DATA_DIR (warehouse.db) / COMPANY_DB_WATCH_URL (ロール watcher = select だけ)
 * 何も書かない (DB にも)。--out のファイルは新しく作るだけ (あれば上書きしない)
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { readNeForLz, readCdbNeCodes, joinLzSnapshot } from '../../apps/master-decisions/lz-snapshot.mjs';
import { connectWatcher } from '../../apps/company-db/master-compare/run.mjs';

export function parseArgs(argv) {
  const out = { out: null, dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--out') out.out = argv[++i];
    else if (a === '--data-dir') out.dataDir = argv[++i];
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!out.out) throw new Error('--out が要る (- = 標準出力)');
  return out;
}

/** 材料を読む。connect = () => Promise<{ db, close }> (Company DB を読まない試験では null) */
export async function takeLzSnapshot({ dataDir, connect, now = new Date() }) {
  const ne = readNeForLz(dataDir);
  let cdb = null;
  if (ne.ok && connect) {
    const c = await connect();
    try { cdb = await readCdbNeCodes(c.db); } finally { await c.close(); }
  }
  return joinLzSnapshot(ne, cdb, { takenAt: now.toISOString() });
}

export function summaryLine(s) {
  if (!s.ne.ok) return `❌ 影運転の材料: NE の取得を使えない (${s.ne.reason})`;
  const r = Object.entries(s.counts.reasons).map(([k, v]) => `${k} ${v}`).join('・');
  return `✅ 影運転の材料: NE の取得 ${s.ne.marks.products.at} (UTC) の ${s.counts.products} 件・元のコードあり ${s.counts.with_code} 件`
    + `${r ? ` (作れない: ${r})` : ''}・元のコードの印 = ${s.cdb.mark ? s.cdb.mark.compare_run_id : 'なし'}`;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) throw new Error('COMPANY_DB_WATCH_URL が無い');
    const s = await takeLzSnapshot({ dataDir, connect: () => connectWatcher(url) });
    const json = JSON.stringify(s);
    if (a.out === '-') process.stdout.write(json);
    else fs.writeFileSync(a.out, json, { flag: 'wx' });
    console.error(summaryLine(s));
    code = s.ne.ok ? 0 : 1;
  } catch (e) {
    console.error(`❌ 影運転の材料: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    code = 1;
  }
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
