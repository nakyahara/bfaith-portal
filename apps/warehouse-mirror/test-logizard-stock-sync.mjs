/**
 * test-logizard-stock-sync.mjs — mirror /api/sync の logizard_stock 受け口の検証テスト
 *
 * 毎時の全置換 payload について、壊れた行をそのまま公開しないことと、
 * 拒否したときに既存 mirror を消さないこと (空配列=全消しの拒否を含む) を確認する。
 *
 * 実行: node apps/warehouse-mirror/test-logizard-stock-sync.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import express from 'express';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzsync-test-'));
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
  商品ID: 'hakkaspray100', 商品名: 'ハッカ油スプレー【大きいサイズ 100ml】', バーコード: 'X0014Q5RST',
  ブロック略称: 'R1FA', ロケ: '001-001-01', 品質区分名: '良品', 有効期限: '20280115', 入荷日: '',
  在庫数: 200, 引当数: 0, ロケ業務区分: '卸', 最終入荷日: '20260807', 最終出荷日: '20260814',
  在庫日: '20260815', ...o,
});
const CAPTURED = '2026-08-16T03:00:00.000Z';
const post = async (body) => {
  const res = await fetch(`${base}/api/sync`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const count = () => db.prepare('SELECT COUNT(*) n FROM mirror_logizard_stock').get().n;

console.log('\n── 正常系 (全置換) ──');
{
  const r = await post({ logizard_stock: { captured_at: CAPTURED, rows: [row(), row({ ロケ: '002-002-01', 在庫数: 160, 引当数: 10 })] } });
  eq(r.status, 200, 'HTTP 200');
  eq(count(), 2, '2行保存された');
  const saved = db.prepare("SELECT 在庫数, 引当数, captured_at FROM mirror_logizard_stock WHERE ロケ='002-002-01'").get();
  eq([saved['在庫数'], saved['引当数'], saved.captured_at], [160, 10, CAPTURED], '値と captured_at がそのまま入る');
}
{
  const r = await post({ logizard_stock: { captured_at: '2026-08-16T04:00:00.000Z', rows: [row({ 在庫数: 180 })] } });
  eq(r.status, 200, '次の時刻の snapshot で置換');
  eq(count(), 1, '全置換で1行になる');
}

console.log('\n── 不正な payload は 400 + mirror を消さない ──');
const cases = [
  ['rows が空配列 (全消しは拒否)', { captured_at: CAPTURED, rows: [] }],
  ['captured_at 無し', { rows: [row()] }],
  ['captured_at が日時でない', { captured_at: 'not-a-date', rows: [row()] }],
  ['captured_at が未来すぎる', { captured_at: '2099-01-01T00:00:00.000Z', rows: [row()] }],
  ['rows がオブジェクト', { captured_at: CAPTURED, rows: {} }],
  ['商品ID が空', { captured_at: CAPTURED, rows: [row({ 商品ID: ' ' })] }],
  ['商品ID が数値', { captured_at: CAPTURED, rows: [row({ 商品ID: 123 })] }],
  ['在庫数 が文字列', { captured_at: CAPTURED, rows: [row({ 在庫数: 'abc' })] }],
  ['引当数 が小数', { captured_at: CAPTURED, rows: [row({ 引当数: 1.5 })] }],
  ['行が null', { captured_at: CAPTURED, rows: [null] }],
];
for (const [label, payload] of cases) {
  const r = await post({ logizard_stock: payload });
  ok(r.status === 400, `${label} → 400`);
}
for (const [label, v] of [['文字列', 'abc'], ['数値', 1], ['null', null]]) {
  const r = await post({ logizard_stock: v });
  ok(r.status === 400, `logizard_stock が${label} → 400 (成功扱いで無視しない)`);
}
eq(count(), 1, '拒否されても既存行は残っている (全消し前に検証している)');

console.log('\n── /api/status に件数と時刻が出る ──');
{
  const res = await fetch(`${base}/api/status`);
  const s = await res.json();
  eq(s.logizard_stock_count, 1, '件数');
  eq(s.logizard_stock_captured_at, '2026-08-16T04:00:00.000Z', 'captured_at (--logizard-only の送信後検証が使う)');
  ok(!!s.logizard_stock_synced_at, '最終同期時刻');
}

await new Promise((r) => server.close(r));
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
