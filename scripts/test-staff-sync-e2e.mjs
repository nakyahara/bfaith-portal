/**
 * E2E: スタッフマスタ同期 (Render apps/staff → miniPC picking)
 *
 * 実行: node scripts/test-staff-sync-e2e.mjs
 * server.js を子プロセスで起動し (STAFF_EXPORT_TOKEN 付き)、picking 側の staff-sync が
 * 実 HTTP で取得して pk_workers に反映するところまでを通しで確認する。
 *
 */
import fs from 'fs'; import os from 'os'; import path from 'path'; import { spawn } from 'child_process';
import { pathToFileURL, fileURLToPath } from 'url';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3461, BASE = `http://127.0.0.1:${PORT}`;
const TOKEN = 'e2e-staff-token';
const RENDER_DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-render-'));
const PICK_DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-pick-'));

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: { ...process.env, DATA_DIR: RENDER_DATA, PORT: String(PORT), PORTAL_PASS: 'smoke', NODE_ENV: 'development',
    SESSION_SECRET: 's', STAFF_EXPORT_TOKEN: TOKEN, INBOUND_INFO_SYNC_ENABLED: 'false' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let logs = ''; child.stdout.on('data', d => logs += d); child.stderr.on('data', d => logs += d);
for (let i = 0; i < 120; i++) { try { const r = await fetch(`${BASE}/login`); if (r.status === 200) break; } catch {} await new Promise(r => setTimeout(r, 500)); }

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

// picking 側 (miniPC 相当) を別 DATA_DIR で初期化
process.env.DATA_DIR = PICK_DATA;
process.env.STAFF_EXPORT_TOKEN = TOKEN;
process.env.STAFF_SYNC_URL = BASE;
const { initPickingDB, listWorkers, addWorker } = await import(pathToFileURL(path.join(ROOT, 'apps/picking/db.js')).href);
const { syncStaff, getStaffSyncState } = await import(pathToFileURL(path.join(ROOT, 'apps/picking/staff-sync.js')).href);
initPickingDB();
addWorker('星 立夏');       // 既存の Notion 表記
addWorker('派遣 太郎');     // スタッフマスタに無い人

console.log('E2E: Render(staff) → miniPC(picking)');
const r = await syncStaff();
ok(r.ok, `実 HTTP で同期成功 (${r.error || ''})`);
ok(r.staffCount === 13, `スタッフ13名を取得 (${r.staffCount})`);
ok(r.linked === 1, `名前一致で「星 立夏」を紐付け (linked=${r.linked})`);
ok(r.added === 12, `残り12名を追加 (added=${r.added})`);
ok(r.warnings.some(w => w.includes('派遣 太郎')), `未登録者を警告 (${r.warnings.join(' / ')})`);
const ws = listWorkers();
ok(ws.length === 14, `名前タップ候補 14名 (13 + 派遣 太郎) = ${ws.length}`);
ok(ws.some(w => w.name === '中原 大輔' && w.staff_no === '0001'), 'staff_no が入る');
ok(ws.find(w => w.name === '有國 陽')?.source === 'staff', 'source=staff');
const st = getStaffSyncState();
ok(st.ok === 1 && st.staff_count === 13, '同期状態を記録');

// 2回目 (変化なし)
const r2 = await syncStaff();
ok(r2.ok && r2.added === 0 && r2.linked === 0 && r2.renamed === 0, '2回目は差分ゼロ (冪等)');

// 不正トークン
process.env.STAFF_EXPORT_TOKEN = 'wrong';
const r3 = await syncStaff();
ok(!r3.ok && /HTTP 401/.test(r3.error) && listWorkers().length === 14, '不正トークンは 401・作業者は消えない');

console.log(`\n${pass} PASS / ${fail} FAIL`);
if (fail) console.log(logs.slice(-1500));
child.kill();
process.exitCode = fail ? 1 : 0;
