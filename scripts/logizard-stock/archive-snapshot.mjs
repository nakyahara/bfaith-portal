#!/usr/bin/env node
/**
 * archive-snapshot.mjs — ロジザード在庫 CSV (毎時ダウンロード) を日付つきで圧縮保存し、在庫の「履歴」を残す
 *
 * なぜ (2026-09-05 Company DB 実機確認 D-7):
 *   raw_lz_inventory は毎時「全置換」、ダウンロード CSV も同名で上書き → 在庫の時系列がどこにも無く、
 *   AI が「昨日あった在庫が今日ない」「動かない在庫」「棚卸のズレ」に気づく材料がゼロだった。
 *   本命は Company DB の追記表 (Phase 3) だが、それまで 0 円で履歴を始めるのがこのスクリプト。
 *
 * 何をするか (run-hourly.ps1 の step 2b、取込成功のあとに呼ばれる):
 *   1. CSV を <dest>/YYYY/MM/zaiko_YYYYMMDD_HHMM.csv.gz に gzip 保存 (時刻 = CSV の更新時刻 JST)。
 *      tmp → gunzip 検証 (バイト数一致) → rename の原子的確定
 *   2. manifest.jsonl に 1 行追記 (snapshot_at / rows / bytes / sha256 / file)
 *   3. 同じ CSV (sha256 + 更新時刻が前回と同一) は保存しない (ダウンロード失敗で古いファイルが残った回を
 *      「新しい在庫」として二重保存しないため)。更新時刻が 3 時間より古い CSV も保存しない (同じ理由)
 *   4. 世代管理: 毎時ファイルは KEEP_HOURLY_DAYS (既定 90 日) 保持、それより古い日は「その日の最後の 1 本」だけ残す
 *      (= 日次スナップショットは永久)。削除は snapshot 日付で判定 (ファイル名から)
 *   5. offsite (任意): rclone があり LOGIZARD_HISTORY_RCLONE_REMOTE (または BACKUP_RCLONE_REMOTE の兄弟
 *      <bucket>/logizard-history) が決まるときだけ、直近 2 日分を rclone copy (削除はしない)。失敗しても exit 0
 *
 * 使い方 (miniPC、repo ルートで。.env を読むため -r dotenv/config):
 *   node -r dotenv/config scripts/logizard-stock/archive-snapshot.mjs
 *   node scripts/logizard-stock/archive-snapshot.mjs --csv C:/tools/logizard-automation/out/logizard_zaikosu.csv --dest C:/Users/bfaith/bfaith-portal/data/logizard-history
 *   オプション: --dry-run / --no-offsite / --keep-hourly-days 90 / --stale-hours 3
 *
 * 終了コード: 0 = 保存した or 正当にスキップ / 1 = 保存に失敗 (ランナーは記録して続行) / 2 = 引数・パス不正
 * サイズ感: CSV 2.7MB → gz 約 0.2〜0.3MB。1 日 10 本 → 約 3MB/日、90 日で約 270MB + 日次永久 (年 100MB 弱)
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import crypto from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { Writable } from 'node:stream';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const DEFAULTS = {
  csv: process.env.LOGIZARD_ZAIKO_OUT || 'C:/tools/logizard-automation/out/logizard_zaikosu.csv',
  dest: path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'logizard-history'),
  keepHourlyDays: 90,
  staleHours: 3,
};

const FILE_RE = /^zaiko_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})\.csv\.gz$/;

/** JST の YYYYMMDD_HHMM (ファイル名用) と ISO (+09:00) を Date から作る */
export function jstStamp(date) {
  const j = new Date(date.getTime() + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  const ymd = `${j.getUTCFullYear()}${p(j.getUTCMonth() + 1)}${p(j.getUTCDate())}`;
  const hm = `${p(j.getUTCHours())}${p(j.getUTCMinutes())}`;
  const iso = `${j.getUTCFullYear()}-${p(j.getUTCMonth() + 1)}-${p(j.getUTCDate())}T${p(j.getUTCHours())}:${p(j.getUTCMinutes())}:${p(j.getUTCSeconds())}+09:00`;
  return { ymd, hm, iso, year: String(j.getUTCFullYear()), month: p(j.getUTCMonth() + 1) };
}

async function sha256File(p) {
  const h = crypto.createHash('sha256');
  await pipeline(fs.createReadStream(p), new Writable({ write(c, _e, cb) { h.update(c); cb(); } }));
  return h.digest('hex');
}

/** 行数 = 改行 (0x0A) の数 − ヘッダ 1 行。CP932 でも改行バイトは同じなので文字コードに依存しない */
async function countRows(p) {
  let n = 0; let last = 0;
  await pipeline(fs.createReadStream(p), new Writable({ write(c, _e, cb) { for (const b of c) if (b === 0x0a) n++; last = c[c.length - 1]; cb(); } }));
  if (last !== 0x0a) n++; // 最終行に改行が無いとき
  return Math.max(0, n - 1);
}

async function gzipVerified(src, dest, level = 6) {
  const tmp = `${dest}.pid${process.pid}.tmp`;
  await pipeline(fs.createReadStream(src), zlib.createGzip({ level }), fs.createWriteStream(tmp));
  let total = 0;
  await pipeline(fs.createReadStream(tmp), zlib.createGunzip(), new Writable({ write(c, _e, cb) { total += c.length; cb(); } }));
  const expected = fs.statSync(src).size;
  if (total !== expected) { try { fs.unlinkSync(tmp); } catch {} throw new Error(`gzip 検証NG: ${total} ≠ ${expected}`); }
  fs.renameSync(tmp, dest);
  return fs.statSync(dest).size;
}

export function readLastManifest(dest) {
  const mf = path.join(dest, 'manifest.jsonl');
  if (!fs.existsSync(mf)) return null;
  const lines = fs.readFileSync(mf, 'utf-8').trim().split(/\r?\n/).filter(Boolean);
  if (!lines.length) return null;
  try { return JSON.parse(lines[lines.length - 1]); } catch { return null; }
}

/** dest 配下の zaiko_*.csv.gz を列挙 (snapshot 日付つき) */
export function listSnapshots(dest) {
  const out = [];
  if (!fs.existsSync(dest)) return out;
  for (const y of fs.readdirSync(dest)) {
    const yd = path.join(dest, y);
    if (!/^\d{4}$/.test(y) || !fs.statSync(yd).isDirectory()) continue;
    for (const m of fs.readdirSync(yd)) {
      const md = path.join(yd, m);
      if (!/^\d{2}$/.test(m) || !fs.statSync(md).isDirectory()) continue;
      for (const f of fs.readdirSync(md)) {
        const mm = FILE_RE.exec(f);
        if (!mm) continue;
        out.push({ file: path.join(md, f), day: `${mm[1]}${mm[2]}${mm[3]}`, hm: `${mm[4]}${mm[5]}` });
      }
    }
  }
  return out.sort((a, b) => (a.day + a.hm).localeCompare(b.day + b.hm));
}

/**
 * 世代管理: keepHourlyDays より古い日は「その日の最後の 1 本」以外を削除。戻り値 = 削除したファイル
 * (今日 = now の JST 日付。境界: 90 日前ちょうどの日は保持)
 */
export function pruneHourly(dest, { now = new Date(), keepHourlyDays = DEFAULTS.keepHourlyDays, dryRun = false } = {}) {
  const cutoff = jstStamp(new Date(now.getTime() - keepHourlyDays * 86400000)).ymd; // この日より前 (<) が対象
  const byDay = new Map();
  for (const s of listSnapshots(dest)) {
    if (s.day >= cutoff) continue;
    if (!byDay.has(s.day)) byDay.set(s.day, []);
    byDay.get(s.day).push(s);
  }
  const removed = [];
  for (const [, files] of byDay) {
    files.sort((a, b) => a.hm.localeCompare(b.hm));
    const keep = files[files.length - 1];
    for (const f of files) {
      if (f === keep) continue;
      if (!dryRun) fs.unlinkSync(f.file);
      removed.push(f.file);
    }
  }
  return removed;
}

/** offsite 先: 明示 env > BACKUP_RCLONE_REMOTE の兄弟 (<remote>:<bucket>/logizard-history) > なし */
export function resolveOffsiteRemote(env = process.env) {
  const explicit = (env.LOGIZARD_HISTORY_RCLONE_REMOTE || '').trim();
  if (explicit) return explicit;
  const base = (env.BACKUP_RCLONE_REMOTE || '').trim(); // 例 gdrive:bfaith-backup/warehouse
  const m = /^([^:]+:[^/]+)\/.+$/.exec(base);
  return m ? `${m[1]}/logizard-history` : null;
}

function offsiteCopy(dest, remote, { rcloneConfig, log }) {
  const args = [];
  if (rcloneConfig) args.push('--config', rcloneConfig);
  // 直近 2 日に更新されたファイルだけを送る (既に同サイズ・同更新時刻で存在するものは rclone が飛ばす)。削除はしない
  args.push('copy', dest, remote, '--max-age', '2d', '--include', '/*/*/zaiko_*.csv.gz', '--include', '/manifest.jsonl', '--transfers', '4', '--timeout', '60s', '--retries', '1', '--max-duration', '2m', '--cutoff-mode', 'hard');
  try {
    execFileSync('rclone', args, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 150000, windowsHide: true });
    log(`offsite ok: ${remote}`);
    return true;
  } catch (e) {
    const tail = (e.stderr ? e.stderr.toString() : e.message).trim().split('\n').slice(-2).join(' | ');
    log(`offsite FAILED (続行): ${tail}`);
    return false;
  }
}

/**
 * 本体。戻り値 { action: 'archived'|'skipped', reason?, file?, rows?, bytes?, gzBytes?, pruned, offsite }
 */
export async function archiveSnapshot(opts = {}) {
  const csv = opts.csv || DEFAULTS.csv;
  const dest = opts.dest || DEFAULTS.dest;
  const now = opts.now || new Date();
  const keepHourlyDays = opts.keepHourlyDays ?? DEFAULTS.keepHourlyDays;
  const staleHours = opts.staleHours ?? DEFAULTS.staleHours;
  const dryRun = !!opts.dryRun;
  const log = opts.log || ((m) => console.log(`[lz-archive] ${m}`));

  if (!fs.existsSync(csv)) throw Object.assign(new Error(`CSV が見つかりません: ${csv}`), { code: 'ENOENT_CSV' });
  const st = fs.statSync(csv);
  if (st.size === 0) return { action: 'skipped', reason: 'CSV が 0 バイト', pruned: [], offsite: null };
  const ageH = (now.getTime() - st.mtimeMs) / 3600000;
  if (ageH > staleHours) return { action: 'skipped', reason: `CSV の更新時刻が ${ageH.toFixed(1)} 時間前 (>${staleHours}h) = 今回のダウンロード成果物ではない`, pruned: [], offsite: null };

  const sha = await sha256File(csv);
  const mtimeIso = new Date(st.mtimeMs).toISOString();
  const last = readLastManifest(dest);
  if (last && last.sha256 === sha && last.source_mtime === mtimeIso) {
    return { action: 'skipped', reason: `前回と同一 (sha256 + 更新時刻) — ${last.file}`, pruned: [], offsite: null };
  }

  const stamp = jstStamp(new Date(st.mtimeMs));
  const dir = path.join(dest, stamp.year, stamp.month);
  const name = `zaiko_${stamp.ymd}_${stamp.hm}.csv.gz`;
  const outFile = path.join(dir, name);
  const rows = await countRows(csv);
  if (dryRun) {
    log(`dry-run: ${outFile} (rows=${rows}, bytes=${st.size})`);
    return { action: 'archived', dryRun: true, file: outFile, rows, bytes: st.size, gzBytes: 0, pruned: pruneHourly(dest, { now, keepHourlyDays, dryRun: true }), offsite: null };
  }
  fs.mkdirSync(dir, { recursive: true });
  const gzBytes = await gzipVerified(csv, outFile);
  const rec = { archived_at: now.toISOString(), snapshot_at: stamp.iso, file: path.relative(dest, outFile).split(path.sep).join('/'), rows, bytes: st.size, gz_bytes: gzBytes, sha256: sha, source_mtime: mtimeIso };
  fs.appendFileSync(path.join(dest, 'manifest.jsonl'), JSON.stringify(rec) + '\n');
  log(`archived: ${rec.file} (rows=${rows}, ${(st.size / 1048576).toFixed(2)}MB → ${(gzBytes / 1024).toFixed(0)}KB)`);

  const pruned = pruneHourly(dest, { now, keepHourlyDays });
  if (pruned.length) log(`pruned ${pruned.length} hourly file(s) older than ${keepHourlyDays}d (daily last kept)`);

  let offsite = null;
  if (!opts.noOffsite) {
    const remote = resolveOffsiteRemote(opts.env || process.env);
    if (remote) offsite = offsiteCopy(dest, remote, { rcloneConfig: ((opts.env || process.env).BACKUP_RCLONE_CONFIG || '').trim(), log });
    else log('offsite: remote 未設定のためローカル保存のみ (LOGIZARD_HISTORY_RCLONE_REMOTE か BACKUP_RCLONE_REMOTE)');
  }
  return { action: 'archived', file: outFile, rows, bytes: st.size, gzBytes, pruned, offsite };
}

// ─── CLI ───
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const opts = {
    csv: getArg('--csv') || undefined,
    dest: getArg('--dest') || undefined,
    keepHourlyDays: getArg('--keep-hourly-days') ? Number(getArg('--keep-hourly-days')) : undefined,
    staleHours: getArg('--stale-hours') ? Number(getArg('--stale-hours')) : undefined,
    dryRun: args.includes('--dry-run'),
    noOffsite: args.includes('--no-offsite'),
  };
  if (opts.keepHourlyDays !== undefined && !(opts.keepHourlyDays >= 1 && opts.keepHourlyDays <= 3650)) { console.error('--keep-hourly-days は 1〜3650'); process.exit(2); }
  archiveSnapshot(opts).then((r) => {
    if (r.action === 'skipped') console.log(`[lz-archive] skipped: ${r.reason}`);
    process.exit(0);
  }).catch((e) => {
    console.error(`[lz-archive] FAILED: ${e.message}`);
    process.exit(e.code === 'ENOENT_CSV' ? 2 : 1);
  });
}
