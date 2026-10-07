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

console.log('\n── 世代逆行・冪等再送・相乗り ──');
{
  const r = await post({ logizard_stock: { captured_at: CAPTURED, rows: [row()] } });   // 03:00 < 保存済み04:00
  ok(r.status === 409, `古い captured_at → 409 で巻き戻さない (実際 ${r.status})`);
  eq(count(), 1, '409 でも既存行は残る');
  const r2 = await post({ logizard_stock: { captured_at: '2026-08-16T04:00:00.000Z', rows: [row(), row({ ロケ: '009-009-09' })] } });
  eq(r2.status, 200, '同一世代の再送は冪等成功 (リトライを失敗扱いにしない)');
  eq(count(), 1, '同一世代では置換しない');
  const r3 = await post({ logizard_stock: { captured_at: '2026-08-16T05:00:00.000Z', rows: [row()] }, products: [] });
  ok(r3.status === 400, `他表と相乗り → 400 (単独POST限定) (実際 ${r3.status})`);
  eq(count(), 1, '相乗り拒否でも既存行は残る');
}

console.log('\n── /api/status に件数と時刻が出る ──');
{
  const res = await fetch(`${base}/api/status`);
  const s = await res.json();
  eq(s.logizard_stock_count, 1, '件数');
  eq(s.logizard_stock_captured_at, '2026-08-16T04:00:00.000Z', 'captured_at (--logizard-only の送信後検証が使う)');
  ok(!!s.logizard_stock_synced_at, '最終同期時刻');
  eq(s.logizard_stock_source_at, null, '素性を送らない古い送り手の世代は source_at が null');
}

console.log('\n── ブロック引当順と世代の素性 (2026-09-25) ──');
{
  const CAP = '2026-08-16T06:00:00.000Z';
  const r = await post({ logizard_stock: {
    captured_at: CAP, source_at: '2026-08-16T05:40:00.000Z', rows_read: 3, skipped_rows: 1,
    rows: [row({ ロケ: 'A-0', ブロック引当順: '0' }), row({ ロケ: 'A-2', ブロック引当順: '2' })],
  } });
  eq(r.status, 200, '素性つきで受ける');
  const orders = db.prepare('SELECT ロケ, ブロック引当順 FROM mirror_logizard_stock ORDER BY ロケ').all().map((x) => [x['ロケ'], x['ブロック引当順']]);
  eq(orders, [['A-0', '0'], ['A-2', '2']], '🚨 ブロック引当順は 0 も 0 のまま保存 (9999 に化けない)');
  const m = db.prepare('SELECT captured_at, source_at, rows_read, skipped_rows, row_count FROM mirror_logizard_stock_meta WHERE id = 1').get();
  eq([m.captured_at, m.source_at, m.rows_read, m.skipped_rows, m.row_count], [CAP, '2026-08-16T05:40:00.000Z', 3, 1, 2], '世代の素性が行と同じ世代で入る');
  const s = await (await fetch(`${base}/api/status`)).json();
  eq([s.logizard_stock_source_at, s.logizard_stock_rows_read, s.logizard_stock_skipped_rows], ['2026-08-16T05:40:00.000Z', 3, 1], '/api/status に出る');
}
{
  const bad = async (extra, label) => {
    const r = await post({ logizard_stock: { captured_at: '2026-08-16T07:00:00.000Z', rows: [row()], ...extra } });
    ok(r.status === 400, `${label} → 400 (実際 ${r.status})`);
  };
  await bad({ source_at: '2026-08-16T07:30:00.000Z' }, '在庫を取った時刻が取り込み完了より後');
  await bad({ source_at: 'きのう' }, '在庫を取った時刻が日時でない');
  await bad({ rows_read: -1 }, '行数が負');
  await bad({ skipped_rows: 1.5 }, '読み飛ばし行数が整数でない');
  eq(count(), 2, '拒否しても既存の世代は残る');
  const m = db.prepare('SELECT captured_at FROM mirror_logizard_stock_meta WHERE id = 1').get();
  eq(m.captured_at, '2026-08-16T06:00:00.000Z', '拒否しても世代の素性は前のまま');
}

await new Promise((r) => server.close(r));
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
