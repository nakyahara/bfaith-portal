/**
 * evidence.mjs — 送り手 (miniPC) が「今朝なにをしたか」を、朝の見張り (apps/company-db/watch) に渡すための証跡 (設計 09 §2.1)
 *
 * ⭐なぜ要るか:
 *   見張りは「行がある = そろっている」と読まない。注文の取込が今朝ちゃんと終わったかは、送り手の結果 (run_id・件数・失敗) で確かめる。
 *   🚨 変更ゼロの朝は chunk を送らないので Render の ops.ingest_runs は作られない (pipeline.mjs) = 「走査は完了した・変わった注文は 0」を
 *   後から確かめられるのは、この証跡だけ。daily-sync の最後の行 (文字列の ✅) を読むのはやめる (形が変わると黙って壊れる)。
 *
 * 置き場所 = DATA_DIR/company-db-evidence/<JST の日付>/<name>.json。書くのは送り手 (この関数)・読むのは見張り。14 日で消す。
 * 🚨 証跡の書き込みは補助。失敗しても送り手の結果 (exit code・最後の行) は変えない (警告を 1 行出すだけ)。ただし黙らない。
 */
import fs from 'node:fs';
import path from 'node:path';
import { jstDateStr } from '../../../lib/jst-date.js';

export const EVIDENCE_DIRNAME = 'company-db-evidence';
export const EVIDENCE_KEEP_DAYS = 14;
const NAME_RE = /^[a-z0-9][a-z0-9_-]{0,60}$/;

export const evidenceDir = (dataDir, dateJst) => path.join(dataDir, EVIDENCE_DIRNAME, dateJst);

/**
 * 証跡を 1 つ書く (同じ日の同じ name は上書き = 再実行した回が正)。戻り値 = 書いたパス。失敗は null (警告を出す)。
 * @param {string} dataDir
 * @param {string} name  'orders-amazon' / 'stock-fba_jp' / 'shipments' (英数字・-・_)
 * @param {object} payload  JSON にできるもの。written_at / name は付け足す
 */
export function writeEvidence(dataDir, name, payload, { now = new Date(), warn = console.warn, keepDays = EVIDENCE_KEEP_DAYS } = {}) {
  try {
    if (!dataDir) throw new Error('DATA_DIR が無い');
    if (!NAME_RE.test(String(name))) throw new Error(`name が不正: ${String(name).slice(0, 40)}`);
    const date = jstDateStr(now);
    const dir = evidenceDir(dataDir, date);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `${name}.json`);
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ name, date, written_at: now.toISOString(), ...payload }, null, 1));
    fs.renameSync(tmp, file);   // 途中まで書いたファイルを見張りに読ませない
    purgeOldEvidence(dataDir, { now, keepDays });
    return file;
  } catch (e) {
    warn(`[company-db evidence] 証跡を書けなかった (${name}。送り手の結果は変えない): ${String(e && e.message).slice(0, 160)}`);
    return null;
  }
}

/** 保持期間を過ぎた日のフォルダを消す (失敗は無視 = 補助) */
export function purgeOldEvidence(dataDir, { now = new Date(), keepDays = EVIDENCE_KEEP_DAYS } = {}) {
  const root = path.join(dataDir, EVIDENCE_DIRNAME);
  let removed = 0;
  try {
    const limit = jstDateStr(new Date(now.getTime() - keepDays * 86400000));
    for (const d of fs.readdirSync(root)) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(d) && d < limit) { fs.rmSync(path.join(root, d), { recursive: true, force: true }); removed++; }
    }
  } catch { /* フォルダが無い・消せない = 補助なので黙る */ }
  return removed;
}

/**
 * その日の証跡を全部読む。壊れたファイルは { name, error } として残す (無かったことにしない)。
 * @returns {{ [name]: object }}
 */
export function readEvidence(dataDir, dateJst) {
  const dir = evidenceDir(dataDir, dateJst);
  const out = {};
  let files = [];
  try { files = fs.readdirSync(dir).filter((f) => f.endsWith('.json')); } catch { return out; }
  for (const f of files) {
    const name = f.slice(0, -5);
    try { out[name] = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')); }
    catch (e) { out[name] = { name, error: `証跡が読めない: ${String(e && e.message).slice(0, 120)}` }; }
  }
  return out;
}
