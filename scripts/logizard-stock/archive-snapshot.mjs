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
 *   1. CSV を専用 tmp にコピーして固定し (途中で上書きされても manifest と gz が食い違わない)、
 *      <dest>/YYYY/MM/zaiko_YYYYMMDD_HHMM.csv.gz に gzip 保存 (時刻 = CSV の更新時刻 JST)。
 *      gz.tmp → gunzip して sha256 一致を検証 → rename の原子的確定。tmp は必ず片付ける
 *   2. manifest.jsonl に 1 行追記 (snapshot_at / rows / bytes / gz_bytes / sha256 / file)
 *   3. 二重保存しない: 前回と sha256 + 更新時刻が同一の CSV は skip (正常)。同名 gz が既にあり中身も同一なら
 *      成功扱い (クラッシュ後の再実行・手動実行との重なりで冪等)。同名で中身が違うときだけ衝突エラー
 *   4. 更新時刻が STALE_HOURS (既定 3h) より古い CSV、0 バイトの CSV は保存しない (ダウンロード失敗で残った
 *      旧ファイルを「新しい在庫」にしない)。これは「この時間の履歴が無い」ので exit 3 で呼び出し側に知らせる
 *   5. 世代管理: 毎時ファイルは KEEP_HOURLY_DAYS (既定 90 日) 保持、それより古い日は「その日の最後の 1 本」だけ残す
 *      (= 日次スナップショットは永久)。削除は snapshot 日付 (ファイル名) で判定
 *   6. offsite (任意): rclone があり LOGIZARD_HISTORY_RCLONE_REMOTE (または BACKUP_RCLONE_REMOTE の最終要素を
 *      logizard-history に置き換えた先) が決まるときだけ、履歴フォルダ全体を rclone copy (既存と同一のファイルは
 *      rclone が飛ばすので毎回全量でも軽い。障害が何日続いても復旧時に全部追いつく。削除はしない)。
 *      失敗しても保存自体は成功なので exit 4 で知らせるだけ
 *
 * 使い方 (miniPC、repo ルートで。.env を読むため -r dotenv/config):
 *   node -r dotenv/config scripts/logizard-stock/archive-snapshot.mjs
 *   node scripts/logizard-stock/archive-snapshot.mjs --csv C:/tools/logizard-automation/out/logizard_zaikosu.csv --dest C:/Users/bfaith/bfaith-portal/data/logizard-history
 *   オプション: --dry-run / --no-offsite / --keep-hourly-days 90 / --stale-hours 3
 *
 * 終了コード (run-hourly.ps1 が note に写す):
 *   0 = 保存した / 前回と同一で skip / 同名同内容が既にあった
 *   3 = 保存しなかった (CSV が古い・0 バイト) → 履歴にこの時間の穴がある
 *   4 = 保存はしたが offsite (rclone) に失敗
 *   1 = 保存に失敗 (gzip・検証・衝突) / 2 = 引数・パス不正
 * 最終行に機械可読の `RESULT action=... code=... offsite=...` を出す。
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

/** gz を展開しながら sha256 とバイト数を返す */
async function sha256Gunzip(gzPath) {
  const h = crypto.createHash('sha256'); let bytes = 0;
  await pipeline(fs.createReadStream(gzPath), zlib.createGunzip(), new Writable({ write(c, _e, cb) { h.update(c); bytes += c.length; cb(); } }));
  return { sha: h.digest('hex'), bytes };
}

/**
 * 行数の目安 = 改行 (0x0A) の数 − ヘッダ 1 行。CP932 でも改行バイトは同じなので文字コードに依存しない。
 * 引用フィールド内の改行は数えてしまうので「取込件数」の正は csv-import.js 側 (ここは manifest の参考値)
 */
async function countRows(p) {
  let n = 0; let last = 0;
  await pipeline(fs.createReadStream(p), new Writable({ write(c, _e, cb) { for (const b of c) if (b === 0x0a) n++; last = c[c.length - 1]; cb(); } }));
  if (last !== 0x0a) n++; // 最終行に改行が無いとき
  return Math.max(0, n - 1);
}

/** src を gzip して dest に原子的に置く。展開 sha256 が expectedSha と一致しなければ失敗。tmp は必ず片付ける */
async function gzipVerified(src, dest, expectedSha, level = 6) {
  const tmp = `${dest}.pid${process.pid}.tmp`;
  let done = false;
  try {
    await pipeline(fs.createReadStream(src), zlib.createGzip({ level }), fs.createWriteStream(tmp));
    const v = await sha256Gunzip(tmp);
    if (v.sha !== expectedSha) throw new Error(`gzip 検証NG: 展開 sha256 が元と不一致 (${v.bytes} bytes)`);
    fs.renameSync(tmp, dest);
    done = true;
    return fs.statSync(dest).size;
  } finally {
    if (!done) { try { fs.unlinkSync(tmp); } catch {} }
  }
}

export function readLastManifest(dest) {
  const mf = path.join(dest, 'manifest.jsonl');
  if (!fs.existsSync(mf)) return null;
  const lines = fs.readFileSync(mf, 'utf-8').trim().split(/\r?\n/).filter(Boolean);
  // 末尾が壊れていても (書き込み途中でクラッシュ) 直前の正しい行を使う
  for (let i = lines.length - 1; i >= 0; i--) { try { return JSON.parse(lines[i]); } catch {} }
  return null;
}

function manifestHas(dest, relFile) {
  const mf = path.join(dest, 'manifest.jsonl');
  if (!fs.existsSync(mf)) return false;
  return fs.readFileSync(mf, 'utf-8').split(/\r?\n/).some((l) => { try { return JSON.parse(l).file === relFile; } catch { return false; } });
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

/**
 * offsite 先: 明示 env > BACKUP_RCLONE_REMOTE の最終パス要素を logizard-history に置換 > なし
 *   gdrive:bfaith-backup/warehouse          → gdrive:bfaith-backup/logizard-history
 *   gdrive:company/bfaith-backup/warehouse  → gdrive:company/bfaith-backup/logizard-history
 *   gdrive:bfaith-backup (サブディレクトリ無し) → null (勝手にバケット直下へ置かない)
 */
export function resolveOffsiteRemote(env = process.env) {
  const explicit = (env.LOGIZARD_HISTORY_RCLONE_REMOTE || '').trim();
  if (explicit) return explicit;
  const base = (env.BACKUP_RCLONE_REMOTE || '').trim().replace(/\/+$/, '');
  const m = /^([^:]+:)(.*)\/[^/]+$/.exec(base); // remote名: と 最終要素の手前まで
  if (!m) return null;
  return `${m[1]}${m[2]}/logizard-history`;
}

/** 履歴フォルダ全体を rclone copy (既存同一はスキップされる、削除はしない)。戻り値 'ok' | 'failed' */
function offsiteCopy(dest, remote, { rcloneConfig, log }) {
  const args = [];
  if (rcloneConfig) args.push('--config', rcloneConfig);
  args.push('copy', dest, remote, '--include', '/*/*/zaiko_*.csv.gz', '--include', '/manifest.jsonl',
    '--transfers', '4', '--timeout', '60s', '--retries', '1', '--max-duration', '3m', '--cutoff-mode', 'hard');
  try {
    execFileSync('rclone', args, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 200000, windowsHide: true });
    log(`offsite ok: ${remote}`);
    return 'ok';
  } catch (e) {
    const tail = (e.stderr ? e.stderr.toString() : e.message).trim().split('\n').slice(-2).join(' | ');
    log(`offsite FAILED (保存は完了、次回に追いつく): ${tail}`);
    return 'failed';
  }
}

/**
 * 本体。戻り値:
 *   { action: 'archived'|'skipped', code: 'archived'|'exists_same'|'duplicate'|'stale'|'empty',
 *     reason?, file?, rows?, bytes?, gzBytes?, pruned: string[], offsite: 'ok'|'failed'|'skipped'|null }
 * 例外: ENOENT_CSV (CSV 無し) / COLLISION (同名 gz が別内容) / gzip・検証失敗
 */
export async function archiveSnapshot(opts = {}) {
  const csv = opts.csv || DEFAULTS.csv;
  const dest = opts.dest || DEFAULTS.dest;
  const now = opts.now || new Date();
  const keepHourlyDays = opts.keepHourlyDays ?? DEFAULTS.keepHourlyDays;
  const staleHours = opts.staleHours ?? DEFAULTS.staleHours;
  const dryRun = !!opts.dryRun;
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(`[lz-archive] ${m}`));

  if (!fs.existsSync(csv)) throw Object.assign(new Error(`CSV が見つかりません: ${csv}`), { code: 'ENOENT_CSV' });
  const st = fs.statSync(csv);
  if (st.size === 0) return { action: 'skipped', code: 'empty', reason: 'CSV が 0 バイト', pruned: [], offsite: null };
  const ageH = (now.getTime() - st.mtimeMs) / 3600000;
  if (ageH > staleHours) return { action: 'skipped', code: 'stale', reason: `CSV の更新時刻が ${ageH.toFixed(1)} 時間前 (>${staleHours}h) = 今回のダウンロード成果物ではない`, pruned: [], offsite: null };

  // 元 CSV を専用 tmp に固定してから hash / 行数 / gzip を取る (処理中に上書きされても manifest と gz が一致する)
  fs.mkdirSync(dest, { recursive: true });
  const work = path.join(dest, `.work-${process.pid}.csv`);
  let result;
  try {
    fs.copyFileSync(csv, work);
    const sha = await sha256File(work);
    const bytes = fs.statSync(work).size;
    const mtimeIso = new Date(st.mtimeMs).toISOString();
    const last = readLastManifest(dest);
    if (last && last.sha256 === sha && last.source_mtime === mtimeIso) {
      return { action: 'skipped', code: 'duplicate', reason: `前回と同一 (sha256 + 更新時刻) — ${last.file}`, pruned: [], offsite: null };
    }

    const stamp = jstStamp(new Date(st.mtimeMs));
    const dir = path.join(dest, stamp.year, stamp.month);
    const name = `zaiko_${stamp.ymd}_${stamp.hm}.csv.gz`;
    const outFile = path.join(dir, name);
    const relFile = `${stamp.year}/${stamp.month}/${name}`;
    const rows = await countRows(work);
    if (dryRun) {
      log(`dry-run: ${outFile} (rows=${rows}, bytes=${bytes})`);
      return { action: 'archived', code: 'archived', dryRun: true, file: outFile, rows, bytes, gzBytes: 0, pruned: pruneHourly(dest, { now, keepHourlyDays, dryRun: true }), offsite: null };
    }
    fs.mkdirSync(dir, { recursive: true });

    let gzBytes; let code = 'archived';
    if (fs.existsSync(outFile)) {
      // 同名が既にある = クラッシュ後の再実行 or 手動実行との重なり。中身が同じなら成功扱い、違えば衝突
      const v = await sha256Gunzip(outFile);
      if (v.sha !== sha) throw Object.assign(new Error(`同名の履歴 ${relFile} が別内容で存在 (既存 ${v.bytes} bytes)。手で確認して退避してから再実行`), { code: 'COLLISION' });
      gzBytes = fs.statSync(outFile).size; code = 'exists_same';
      log(`already archived (同名同内容): ${relFile}`);
    } else {
      gzBytes = await gzipVerified(work, outFile, sha);
      log(`archived: ${relFile} (rows=${rows}, ${(bytes / 1048576).toFixed(2)}MB → ${(gzBytes / 1024).toFixed(0)}KB)`);
    }
    if (!manifestHas(dest, relFile)) {
      const rec = { archived_at: now.toISOString(), snapshot_at: stamp.iso, file: relFile, rows, bytes, gz_bytes: gzBytes, sha256: sha, source_mtime: mtimeIso };
      fs.appendFileSync(path.join(dest, 'manifest.jsonl'), JSON.stringify(rec) + '\n');
    }

    const pruned = pruneHourly(dest, { now, keepHourlyDays });
    if (pruned.length) log(`pruned ${pruned.length} hourly file(s) older than ${keepHourlyDays}d (daily last kept)`);

    let offsite = 'skipped';
    if (!opts.noOffsite) {
      const remote = resolveOffsiteRemote(env);
      if (remote) offsite = offsiteCopy(dest, remote, { rcloneConfig: (env.BACKUP_RCLONE_CONFIG || '').trim(), log });
      else log('offsite: remote 未設定のためローカル保存のみ (LOGIZARD_HISTORY_RCLONE_REMOTE か BACKUP_RCLONE_REMOTE)');
    }
    result = { action: 'archived', code, file: outFile, rows, bytes, gzBytes, pruned, offsite };
  } finally {
    try { fs.unlinkSync(work); } catch {}
  }
  return result;
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
  if (opts.staleHours !== undefined && !(opts.staleHours > 0 && opts.staleHours <= 240)) { console.error('--stale-hours は 0 より大きく 240 以下'); process.exit(2); }
  archiveSnapshot(opts).then((r) => {
    if (r.action === 'skipped') console.log(`[lz-archive] skipped: ${r.reason}`);
    console.log(`RESULT action=${r.action} code=${r.code} offsite=${r.offsite ?? 'none'}${r.file ? ` file=${path.basename(r.file)}` : ''}`);
    if (r.code === 'stale' || r.code === 'empty') process.exit(3);
    if (r.offsite === 'failed') process.exit(4);
    process.exit(0);
  }).catch((e) => {
    console.error(`[lz-archive] FAILED: ${e.message}`);
    console.log(`RESULT action=error code=${e.code || 'error'}`);
    process.exit(e.code === 'ENOENT_CSV' ? 2 : 1);
  });
}
