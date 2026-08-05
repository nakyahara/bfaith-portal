/**
 * test-shipments-sync.mjs — mirror /api/sync の shipments_daily 受け口の検証テスト
 *
 * 壊れた件数をそのまま公開しないこと (SQLite は INTEGER 列にも文字列を入れられる) と、
 * 拒否したときに既存 mirror を消さないことを確認する。
 *
 * 実行: node apps/warehouse-mirror/test-shipments-sync.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import express from 'express';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipsync-test-'));
process.env.DATA_DIR = tmpDir;
process.env.ALLOW_INSECURE_MIRROR_SYNC = '1'; // 認証は本テストの対象外

const { initMirrorDB, getMirrorDB } = await import('./db.js');
const mirrorRouter = (await import('./router.js')).default;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initMirrorDB();
const db = getMirrorDB();

const app = express();
app.use('/apps/mirror', express.json({ limit: '32mb' }), mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/mirror`;

const row = (o = {}) => ({
  ship_date: '2026-08-04', shop_code: '4', shop_name: 'Amazon店', platform: 'amazon_fbm',
  delivery_id: '71', delivery_name: 'AES', slips: 400, cancelled_slips: 2, ...o,
});
const post = async (body) => {
  const res = await fetch(`${base}/api/sync`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const count = () => db.prepare('SELECT COUNT(*) n FROM mirror_shipments_daily').get().n;

console.log('\n── 正常系 ──');
{
  const r = await post({ shipments_daily: [row(), row({ ship_date: '2026-08-05', slips: 395, cancelled_slips: 0 })] });
  eq(r.status, 200, 'HTTP 200');
  eq(count(), 2, '2行保存された');
  const saved = db.prepare("SELECT slips, cancelled_slips, delivery_name FROM mirror_shipments_daily WHERE ship_date='2026-08-04'").get();
  eq([saved.slips, saved.cancelled_slips, saved.delivery_name], [400, 2, 'AES'], '値がそのまま入る');
}

console.log('\n── 不正な payload は 400 + mirror を消さない ──');
const cases = [
  ['ship_date が日付でない', [row({ ship_date: '2026-13-45' })]],
  ['ship_date が空', [row({ ship_date: '' })]],
  ['slips が文字列', [row({ slips: 'abc' })]],
  ['slips が負数', [row({ slips: -1 })]],
  ['slips が小数', [row({ slips: 1.5 })]],
  ['cancelled_slips > slips', [row({ slips: 3, cancelled_slips: 5 })]],
  ['キー重複', [row(), row()]],
  ['行が null', [null]],
  ['delivery_id が長すぎる', [row({ delivery_id: 'x'.repeat(33) })]],
];
for (const [label, rows] of cases) {
  const r = await post({ shipments_daily: rows });
  ok(r.status === 400, `${label} → 400`);
}
eq(count(), 2, '拒否されても既存の2行は残っている (全消し前に検証している)');

console.log('\n── 正当な0件は反映する ──');
{
  const r = await post({ shipments_daily: [] });
  eq(r.status, 200, 'HTTP 200');
  eq(count(), 0, '空配列は「全部消えた」として反映される');
}

await new Promise((r) => server.close(r));
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
