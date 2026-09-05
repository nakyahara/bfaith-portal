/**
 * test-logizard-archive.mjs — scripts/logizard-stock/archive-snapshot.mjs のローカルテスト
 * 使い方: node scripts/test-logizard-archive.mjs (repo ルートで。一時ディレクトリで完結、rclone は呼ばない)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { archiveSnapshot, pruneHourly, listSnapshots, resolveOffsiteRemote, jstStamp, readLastManifest } from './logizard-stock/archive-snapshot.mjs';

let failures = 0;
function check(name, cond, extra = '') { if (cond) console.log(`PASS: ${name}`); else { failures++; console.log(`FAIL: ${name} ${extra}`); } }
const T = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-archive-test-'));
const dest = path.join(T, 'history');
const csv = path.join(T, 'logizard_zaikosu.csv');
const quiet = () => {};

// CP932 風のバイト列は不要 (行数は改行バイトで数える)。CRLF + ヘッダ + 3 行
function writeCsv(rows, mtime) {
  const body = ['在庫日,倉庫ID,商品ID,在庫数', ...rows.map((r, i) => `20260905,1,sku${i},${r}`)].join('\r\n') + '\r\n';
  fs.writeFileSync(csv, body, 'latin1');
  fs.utimesSync(csv, mtime, mtime);
}
const jst = (y, m, d, h, mi = 0) => new Date(Date.UTC(y, m - 1, d, h - 9, mi));

// ── T1: 初回 archive ──
writeCsv([10, 20, 30], jst(2026, 9, 5, 18, 0));
const r1 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 18, 5), noOffsite: true, log: quiet });
check('T1 archived', r1.action === 'archived', JSON.stringify(r1));
check('T1 ファイル名は JST の更新時刻 (2026/09/zaiko_20260905_1800.csv.gz)', r1.file.endsWith(path.join('2026', '09', 'zaiko_20260905_1800.csv.gz')), r1.file);
check('T1 rows = 3 (ヘッダ除く、CRLF)', r1.rows === 3, String(r1.rows));
check('T1 gzip を解くと元と同じバイト列', zlib.gunzipSync(fs.readFileSync(r1.file)).equals(fs.readFileSync(csv)));
const m1 = readLastManifest(dest);
check('T1 manifest に 1 行 (sha256 / rows / snapshot_at +09:00)', m1 && m1.rows === 3 && /^[0-9a-f]{64}$/.test(m1.sha256) && m1.snapshot_at === '2026-09-05T18:00:00+09:00', JSON.stringify(m1));
check('T1 tmp が残っていない', !fs.readdirSync(path.dirname(r1.file)).some((f) => f.endsWith('.tmp')));

// ── T2: 同じ CSV (更新時刻も同じ) は skip ──
const r2 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T2 同一 CSV は skipped (二重保存しない)', r2.action === 'skipped' && /同一/.test(r2.reason), JSON.stringify(r2));
check('T2 manifest は増えない', fs.readFileSync(path.join(dest, 'manifest.jsonl'), 'utf-8').trim().split('\n').length === 1);

// ── T3: 中身が同じでも更新時刻が新しければ保存 (「在庫が動かなかった」も履歴) ──
fs.utimesSync(csv, jst(2026, 9, 5, 19, 0), jst(2026, 9, 5, 19, 0));
const r3 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T3 更新時刻が新しければ archived (zaiko_20260905_1900)', r3.action === 'archived' && r3.file.endsWith('zaiko_20260905_1900.csv.gz'), JSON.stringify(r3));

// ── T4: 古い CSV (3h 超) は skip = ダウンロード失敗で残った旧ファイルを新しい在庫として保存しない ──
writeCsv([1], jst(2026, 9, 5, 12, 0));
const r4 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T4 3 時間より古い CSV は skipped', r4.action === 'skipped' && /時間前/.test(r4.reason), JSON.stringify(r4));

// ── T5: CSV 無し → ENOENT_CSV ──
let threw = null;
try { await archiveSnapshot({ csv: path.join(T, 'nope.csv'), dest, noOffsite: true, log: quiet }); } catch (e) { threw = e; }
check('T5 CSV 無しは code=ENOENT_CSV で throw', threw && threw.code === 'ENOENT_CSV');

// ── T6: dry-run は書かない ──
writeCsv([5, 6], jst(2026, 9, 5, 20, 0));
const before = listSnapshots(dest).length;
const r6 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 20, 5), noOffsite: true, dryRun: true, log: quiet });
check('T6 dry-run はファイルを作らない', r6.dryRun === true && listSnapshots(dest).length === before);

// ── T7: 世代管理 — 90 日より古い日は最後の 1 本だけ残す。境界日 (ちょうど 90 日前) は全部残す ──
{
  const mk = (y, m, d, hm) => { const dir = path.join(dest, String(y), String(m).padStart(2, '0')); fs.mkdirSync(dir, { recursive: true }); const f = path.join(dir, `zaiko_${y}${String(m).padStart(2, '0')}${String(d).padStart(2, '0')}_${hm}.csv.gz`); fs.writeFileSync(f, 'x'); return f; };
  const now = jst(2026, 9, 5, 20, 5);
  const old = [mk(2026, 5, 1, '0900'), mk(2026, 5, 1, '1200'), mk(2026, 5, 1, '1800'), mk(2026, 5, 2, '0900'), mk(2026, 5, 2, '1000')];
  const boundaryDay = jstStamp(new Date(now.getTime() - 90 * 86400000)).ymd; // 2026-06-07
  const bY = boundaryDay.slice(0, 4), bM = boundaryDay.slice(4, 6), bD = boundaryDay.slice(6, 8);
  const boundary = [mk(Number(bY), Number(bM), Number(bD), '0900'), mk(Number(bY), Number(bM), Number(bD), '1800')];
  const removed = pruneHourly(dest, { now, keepHourlyDays: 90 });
  check('T7 5/1 は 0900・1200 が消え 1800 が残る', !fs.existsSync(old[0]) && !fs.existsSync(old[1]) && fs.existsSync(old[2]));
  check('T7 5/2 は 0900 が消え 1000 が残る', !fs.existsSync(old[3]) && fs.existsSync(old[4]));
  check('T7 境界日 (90 日前ちょうど) は両方残る', boundary.every((f) => fs.existsSync(f)));
  check('T7 今日 (9/5) の毎時ファイルは全部残る', listSnapshots(dest).filter((s) => s.day === '20260905').length === 2);
  check('T7 削除件数 = 3', removed.length === 3, String(removed.length));
  const again = pruneHourly(dest, { now, keepHourlyDays: 90 });
  check('T7 2 回目は何も消さない (冪等)', again.length === 0);
}

// ── T8: offsite remote の解決 ──
check('T8 明示 env が最優先', resolveOffsiteRemote({ LOGIZARD_HISTORY_RCLONE_REMOTE: 'gd:x/y', BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse' }) === 'gd:x/y');
check('T8 BACKUP_RCLONE_REMOTE の兄弟に派生', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse' }) === 'gdrive:bfaith-backup/logizard-history');
check('T8 サブディレクトリ無しの remote からは派生しない (null)', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup' }) === null);
check('T8 未設定は null', resolveOffsiteRemote({}) === null);

// ── T9: jstStamp の日付境界 (UTC 23:30 = JST 翌日 08:30) ──
const s9 = jstStamp(new Date(Date.UTC(2026, 8, 4, 23, 30)));
check('T9 UTC 9/4 23:30 → JST 9/5 08:30', s9.ymd === '20260905' && s9.hm === '0830' && s9.iso === '2026-09-05T08:30:00+09:00', JSON.stringify(s9));

fs.rmSync(T, { recursive: true, force: true });
console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
