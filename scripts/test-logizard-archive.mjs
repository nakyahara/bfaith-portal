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
const manifestLines = () => fs.existsSync(path.join(dest, 'manifest.jsonl')) ? fs.readFileSync(path.join(dest, 'manifest.jsonl'), 'utf-8').trim().split('\n').filter(Boolean).length : 0;
const noTmp = () => !listSnapshots(dest).some(() => false) && !fs.readdirSync(dest).some((f) => f.startsWith('.work-')) && listSnapshots(dest).every((s) => !fs.readdirSync(path.dirname(s.file)).some((f) => f.endsWith('.tmp')));

// CP932 風のバイト列は不要 (行数は改行バイトで数える)。CRLF + ヘッダ + N 行
function writeCsv(rows, mtime) {
  const body = ['在庫日,倉庫ID,商品ID,在庫数', ...rows.map((r, i) => `20260905,1,sku${i},${r}`)].join('\r\n') + '\r\n';
  fs.writeFileSync(csv, body, 'latin1');
  fs.utimesSync(csv, mtime, mtime);
}
const jst = (y, m, d, h, mi = 0) => new Date(Date.UTC(y, m - 1, d, h - 9, mi));

// ── T1: 初回 archive ──
writeCsv([10, 20, 30], jst(2026, 9, 5, 18, 0));
const r1 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 18, 5), noOffsite: true, log: quiet });
check('T1 archived (code=archived, offsite=skipped)', r1.action === 'archived' && r1.code === 'archived' && r1.offsite === 'skipped', JSON.stringify(r1));
check('T1 ファイル名は JST の更新時刻 (2026/09/zaiko_20260905_1800.csv.gz)', r1.file.endsWith(path.join('2026', '09', 'zaiko_20260905_1800.csv.gz')), r1.file);
check('T1 rows = 3 (ヘッダ除く、CRLF)', r1.rows === 3, String(r1.rows));
check('T1 gzip を解くと元と同じバイト列', zlib.gunzipSync(fs.readFileSync(r1.file)).equals(fs.readFileSync(csv)));
const m1 = readLastManifest(dest);
check('T1 manifest に 1 行 (sha256 / rows / snapshot_at +09:00 / file は / 区切り)', m1 && m1.rows === 3 && /^[0-9a-f]{64}$/.test(m1.sha256) && m1.snapshot_at === '2026-09-05T18:00:00+09:00' && m1.file === '2026/09/zaiko_20260905_1800.csv.gz', JSON.stringify(m1));
check('T1 tmp / work ファイルが残っていない', noTmp());

// ── T2: 同じ CSV (更新時刻も同じ) は duplicate skip ──
const r2 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T2 同一 CSV は skipped code=duplicate (二重保存しない)', r2.action === 'skipped' && r2.code === 'duplicate', JSON.stringify(r2));
check('T2 manifest は増えない', manifestLines() === 1);

// ── T3: 中身が同じでも更新時刻が新しければ保存 (「在庫が動かなかった」も履歴) ──
fs.utimesSync(csv, jst(2026, 9, 5, 19, 0), jst(2026, 9, 5, 19, 0));
const r3 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T3 更新時刻が新しければ archived (zaiko_20260905_1900)', r3.action === 'archived' && r3.file.endsWith('zaiko_20260905_1900.csv.gz'), JSON.stringify(r3));

// ── T4: 古い CSV (3h 超) は stale skip、0 バイトは empty skip ──
writeCsv([1], jst(2026, 9, 5, 12, 0));
const r4 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 5), noOffsite: true, log: quiet });
check('T4 3 時間より古い CSV は skipped code=stale', r4.action === 'skipped' && r4.code === 'stale', JSON.stringify(r4));
fs.writeFileSync(csv, ''); fs.utimesSync(csv, jst(2026, 9, 5, 19, 30), jst(2026, 9, 5, 19, 30));
const r4b = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 19, 35), noOffsite: true, log: quiet });
check('T4 0 バイトは skipped code=empty', r4b.action === 'skipped' && r4b.code === 'empty', JSON.stringify(r4b));
check('T4 skip では manifest が増えない', manifestLines() === 2);

// ── T5: CSV 無し → ENOENT_CSV ──
let threw = null;
try { await archiveSnapshot({ csv: path.join(T, 'nope.csv'), dest, noOffsite: true, log: quiet }); } catch (e) { threw = e; }
check('T5 CSV 無しは code=ENOENT_CSV で throw', threw && threw.code === 'ENOENT_CSV');

// ── T6: dry-run は書かない ──
writeCsv([5, 6], jst(2026, 9, 5, 20, 0));
const before = listSnapshots(dest).length;
const r6 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 20, 5), noOffsite: true, dryRun: true, log: quiet });
check('T6 dry-run はファイルも manifest も作らない', r6.dryRun === true && listSnapshots(dest).length === before && manifestLines() === 2);
check('T6 dry-run 後も work ファイルが残らない', noTmp());

// ── T7: 同名 gz が既にある (クラッシュ後の再実行 / 手動実行との重なり) ──
{
  // 7a: 同内容 → exists_same で成功、manifest は重複しない
  writeCsv([7, 8, 9], jst(2026, 9, 5, 20, 0));
  const a = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 20, 5), noOffsite: true, log: quiet });
  check('T7a 初回は archived', a.code === 'archived');
  const mfPath = path.join(dest, 'manifest.jsonl');
  const mfBefore = fs.readFileSync(mfPath, 'utf-8');
  // manifest の最終行を消して「rename 成功・manifest 追記前にクラッシュ」を再現
  fs.writeFileSync(mfPath, mfBefore.trim().split('\n').slice(0, -1).join('\n') + '\n');
  const b = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 20, 6), noOffsite: true, log: quiet });
  check('T7a 同名同内容が既にあれば exists_same で成功 (rename 衝突で失敗しない)', b.action === 'archived' && b.code === 'exists_same', JSON.stringify(b));
  const mfAfter = fs.readFileSync(mfPath, 'utf-8').trim().split('\n');
  const lastRec = JSON.parse(mfAfter[mfAfter.length - 1]);
  check('T7a manifest 行が復元され重複しない (行数は元どおり、末尾は同じ file / sha256)', mfAfter.length === mfBefore.trim().split('\n').length && lastRec.file === '2026/09/zaiko_20260905_2000.csv.gz' && lastRec.sha256 === JSON.parse(mfBefore.trim().split('\n').pop()).sha256, JSON.stringify(lastRec));
  // 7b: 同名で別内容 → COLLISION
  writeCsv([7, 8, 9, 10], jst(2026, 9, 5, 20, 0)); // 同じ分・別内容
  let coll = null;
  try { await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 20, 7), noOffsite: true, log: quiet }); } catch (e) { coll = e; }
  check('T7b 同名で別内容は COLLISION で throw (上書きしない)', coll && coll.code === 'COLLISION', coll && coll.message);
  check('T7b 既存 gz は無傷 (3 行版のまま)', zlib.gunzipSync(fs.readFileSync(a.file)).toString('latin1').split('\r\n').filter(Boolean).length === 4);
  check('T7b 衝突後も work / tmp が残らない', noTmp());
}

// ── T8: manifest 末尾が壊れていても直前の正しい行を読み、次の追記で壊れた末尾を切り詰める ──
{
  const mfPath = path.join(dest, 'manifest.jsonl');
  const linesBefore = manifestLines();
  fs.appendFileSync(mfPath, '{"archived_at":"2026-09-05T11:0'); // 書き込み途中でクラッシュした行 (改行なし)
  const last = readLastManifest(dest);
  check('T8 壊れた末尾行を飛ばして直前の行を返す', last && last.file === '2026/09/zaiko_20260905_2000.csv.gz', JSON.stringify(last));
  // 壊れた末尾のまま次のスナップショットを保存 → 新しい行が壊れた行に連結されず、全行が有効な JSON になる
  writeCsv([11, 12], jst(2026, 9, 5, 21, 0));
  const r8 = await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 21, 5), noOffsite: true, log: quiet });
  const all = fs.readFileSync(mfPath, 'utf-8').split('\n').filter(Boolean);
  const allValid = all.every((l) => { try { JSON.parse(l); return true; } catch { return false; } });
  check('T8 壊れた末尾の後の追記で全行が有効な JSON (壊れた行は捨てられる)', r8.code === 'archived' && allValid && all.length === linesBefore + 1, `lines=${all.length} before=${linesBefore} valid=${allValid}`);
  check('T8 末尾行は新しいスナップショット', JSON.parse(all[all.length - 1]).file === '2026/09/zaiko_20260905_2100.csv.gz');
  // 正しい末尾行に改行が無いだけのケース → 改行を足して追記 (行は失われない)
  const cur = fs.readFileSync(mfPath, 'utf-8');
  fs.writeFileSync(mfPath, cur.replace(/\n$/, ''));
  writeCsv([13], jst(2026, 9, 5, 21, 30));
  await archiveSnapshot({ csv, dest, now: jst(2026, 9, 5, 21, 35), noOffsite: true, log: quiet });
  const all2 = fs.readFileSync(mfPath, 'utf-8').split('\n').filter(Boolean);
  check('T8 改行無しの正しい末尾行は残して追記 (行数 +1)', all2.length === all.length + 1 && all2.every((l) => { try { JSON.parse(l); return true; } catch { return false; } }), `lines=${all2.length}`);
}

// ── T9: 世代管理 — 90 日より古い日は最後の 1 本だけ残す。境界日 (ちょうど 90 日前) は全部残す ──
{
  const mk = (y, m, d, hm) => { const dir = path.join(dest, String(y), String(m).padStart(2, '0')); fs.mkdirSync(dir, { recursive: true }); const f = path.join(dir, `zaiko_${y}${String(m).padStart(2, '0')}${String(d).padStart(2, '0')}_${hm}.csv.gz`); fs.writeFileSync(f, 'x'); return f; };
  const now = jst(2026, 9, 5, 22, 5);
  const todayBefore = listSnapshots(dest).filter((s) => s.day === '20260905').length;
  const old = [mk(2026, 5, 1, '0900'), mk(2026, 5, 1, '1200'), mk(2026, 5, 1, '1800'), mk(2026, 5, 2, '0900'), mk(2026, 5, 2, '1000')];
  const boundaryDay = jstStamp(new Date(now.getTime() - 90 * 86400000)).ymd; // 2026-06-07
  const bY = boundaryDay.slice(0, 4), bM = boundaryDay.slice(4, 6), bD = boundaryDay.slice(6, 8);
  const boundary = [mk(Number(bY), Number(bM), Number(bD), '0900'), mk(Number(bY), Number(bM), Number(bD), '1800')];
  const removed = pruneHourly(dest, { now, keepHourlyDays: 90 });
  check('T9 5/1 は 0900・1200 が消え 1800 が残る', !fs.existsSync(old[0]) && !fs.existsSync(old[1]) && fs.existsSync(old[2]));
  check('T9 5/2 は 0900 が消え 1000 が残る', !fs.existsSync(old[3]) && fs.existsSync(old[4]));
  check('T9 境界日 (90 日前ちょうど) は両方残る', boundary.every((f) => fs.existsSync(f)));
  check('T9 今日 (9/5) の毎時ファイルは全部残る', listSnapshots(dest).filter((s) => s.day === '20260905').length === todayBefore && todayBefore >= 3, String(todayBefore));
  check('T9 削除件数 = 3', removed.length === 3, String(removed.length));
  const again = pruneHourly(dest, { now, keepHourlyDays: 90 });
  check('T9 2 回目は何も消さない (冪等)', again.length === 0);
}

// ── T10: offsite remote の解決 ──
check('T10 明示 env が最優先', resolveOffsiteRemote({ LOGIZARD_HISTORY_RCLONE_REMOTE: 'gd:x/y', BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse' }) === 'gd:x/y');
check('T10 BACKUP_RCLONE_REMOTE の最終要素を置換', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse' }) === 'gdrive:bfaith-backup/logizard-history');
check('T10 深いパスでも最終要素だけ置換', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:company/bfaith-backup/warehouse' }) === 'gdrive:company/bfaith-backup/logizard-history');
check('T10 末尾スラッシュは無視', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse/' }) === 'gdrive:bfaith-backup/logizard-history');
check('T10 サブディレクトリ無しの remote からは派生しない (null)', resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup' }) === null);
check('T10 未設定は null', resolveOffsiteRemote({}) === null);

// ── T11: jstStamp の日付境界 (UTC 23:30 = JST 翌日 08:30) ──
const s11 = jstStamp(new Date(Date.UTC(2026, 8, 4, 23, 30)));
check('T11 UTC 9/4 23:30 → JST 9/5 08:30', s11.ymd === '20260905' && s11.hm === '0830' && s11.iso === '2026-09-05T08:30:00+09:00', JSON.stringify(s11));

fs.rmSync(T, { recursive: true, force: true });
console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
