import { temporaryTestRoot } from '../../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-stock-draws.mjs — 他ロケ候補から「写しの後にピッキングで取る/取った数」を引く (2026-10-12 現場の要望)
 *
 * 実際にあったこと (10/12 出荷_32): P3FA-005-004-01 で 1 個足りず、他ロケ候補に P3FC-002-003-05 が出たので
 * そこから取った。ところが P3FC-002-003-05 は同じピッキングで後で取る棚で、行ったら取れなかった。
 * ロジザードの在庫の写し (毎時) はピッキング取込より前のもので、その引当がまだ入っていなかった。
 *
 * 守りたいこと:
 *   ① 写しの後に取り込んだピッキングの明細の数をそのロケから引く (残り 0 の棚は候補に出ない・残りがあれば減らして出す)
 *   ② 写しより前に取り込んだピッキングは引かない (写しの引当数に入っている = 二重に引かない)
 *   ③ 写しの後に欠品と報告された棚は出さない・他ロケで確保した数はそのロケから引く
 *   ④ 取消・無効のバッチ・別の商品は数えない / 写しの時刻が読めないときは何も引かない
 *   ⑤ 同じロケの期限違いは期限の近い順に引く (通知本文 = ロットごとの行)
 *
 * 実行: node apps/picking/tests/test-stock-draws.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-stock-draws-test-'));

const { initPickingDB, getDB, jstToday } = await import('../db.js');
const { pickingStockDraws } = await import('../service.js');
const { listStockCandidates, buildStockLocationsText, stockDrawKey } = await import('../stock-locations.js');

initPickingDB();
const db = getDB();
const today = jstToday();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

const SNAP = '2026-10-12T05:00:42.633Z';     // 14:00 の写し
const BEFORE = '2026-10-12T04:40:00Z';
const AFTER = '2026-10-12T05:20:00Z';

let tb = 0;
function mkBatch({ createdAt, status = 'picking', validity = 'valid', origin = 'import', lines }) {
  const tbNo = `TB${++tb}`;
  const id = Number(db.prepare(`INSERT INTO pk_batches
    (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
     delivery_method, invoice_soft, line_count, slip_count, total_qty, status, validity, origin,
     csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, 'AES《複数SKU複数個》', '出荷_32', ?, NULL, '混在', NULL, NULL, ?, 1, 9, ?, ?, ?, ?, 'test', ?, ?)`)
    .run(tbNo, today, lines.length, status, validity, origin, `sha-${tbNo}`, createdAt, createdAt).lastInsertRowid);
  const ins = db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, qty, status,
      done_at, shortage_qty, alt_block, alt_location, alt_qty)
    VALUES (?, ?, ?, ?, ?, '平串', ?, ?, ?, ?, ?, ?, ?)`);
  lines.forEach((l, i) => ins.run(id, i + 1, l.location, l.block, l.sku || 'hiragushi240', l.qty, l.status || 'pending',
    l.doneAt || null, l.shortageQty ?? null, l.altBlock || null, l.altLocation || null, l.altQty ?? null));
  return id;
}

// ロジザード在庫の写し (service-api の形)
const snapshot = (locations) => ({ ok: true, importedAt: SNAP, locations: locations.map((r) => ({ quality: '良品', allocated: 0, ...r })) });
const labels = (out) => out.rows.map((r) => `${r.label}=${r.free}`);

t('① 10/12 の事例: 同じピッキングで後で取る棚 (残り 0) は他ロケ候補に出ない', () => {
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  // 出荷_32: P3FA で 2 個 (1 個不足) → P3FC-002-003-05 でも 1 個取る予定 (写しの後に取込)
  mkBatch({ createdAt: AFTER, lines: [
    { block: 'P3FA', location: '00500401', qty: 2, status: 'pending' },
    { block: 'P3FC', location: '00200305', qty: 1, status: 'pending' },
  ] });
  const draws = pickingStockDraws('hiragushi240', SNAP);
  assert.deepEqual(draws.get(stockDrawKey('P3FC', '002-003-05')), { qty: 1, depleted: false });
  const data = snapshot([
    { block: 'P3FA', location: '005-004-01', free: 1 },
    { block: 'P3FC', location: '002-003-05', free: 1 },   // 写しの時点で 1 個 = このピッキングの分
    { block: 'R2FD', location: '002-014-02', free: 29 },
  ]);
  const out = listStockCandidates(data, { excludeBlock: 'P3FA', excludeLocation: '00500401', groupByLocation: true, draws });
  assert.deepEqual(labels(out), ['R2FD-002-014-02=29'], 'P3FC-002-003-05 は出ない');
  // 引かない従来の表示では出ていた (直す前の再現)
  assert.deepEqual(labels(listStockCandidates(data, { excludeBlock: 'P3FA', excludeLocation: '00500401', groupByLocation: true })),
    ['P3FC-002-003-05=1', 'R2FD-002-014-02=29']);
});

t('① 残りがある棚は減らした数で出す・ピック済みの明細も引く・SKU の大文字小文字/前後の空白は同じ扱い', () => {
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  mkBatch({ createdAt: AFTER, lines: [
    { block: 'P3FC', location: '00200305', qty: 2, status: 'done', doneAt: AFTER },
    { block: 'R2FD', location: '00201402', qty: 4, status: 'pending', sku: ' HIRAGUSHI240 ' },
  ] });
  const draws = pickingStockDraws('hiragushi240', SNAP);
  const out = listStockCandidates(snapshot([
    { block: 'P3FC', location: '002-003-05', free: 5 },
    { block: 'R2FD', location: '002-014-02', free: 4 },
  ]), { groupByLocation: true, draws });
  assert.deepEqual(labels(out), ['P3FC-002-003-05=3']);
});

t('② 写しより前に取り込んだピッキングは引かない (写しの引当数に入っている)', () => {
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  mkBatch({ createdAt: BEFORE, lines: [{ block: 'P3FC', location: '00200305', qty: 3, status: 'pending' }] });
  assert.equal(pickingStockDraws('hiragushi240', SNAP).size, 0);
});

t('③ 写しの後に欠品と報告された棚は出さない・他ロケで確保した数はそのロケから引く', () => {
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  // 写しより前の取込でも、欠品の報告が写しの後なら効く
  mkBatch({ createdAt: BEFORE, lines: [
    { block: 'P3FA', location: '00500401', qty: 2, status: 'shortage', doneAt: AFTER, shortageQty: 1,
      altBlock: 'R2FD', altLocation: '002-014-02', altQty: 1 },
  ] });
  // 写しより前の欠品報告は数えない (その後に補充されているかもしれない)
  mkBatch({ createdAt: BEFORE, lines: [
    { block: 'R2FD', location: '00300301', qty: 1, status: 'shortage', doneAt: BEFORE, shortageQty: 1,
      altBlock: 'R2FD', altLocation: '003-013-01', altQty: 1 },
  ] });
  const draws = pickingStockDraws('hiragushi240', SNAP);
  const out = listStockCandidates(snapshot([
    { block: 'P3FA', location: '005-004-01', free: 1 },
    { block: 'R2FD', location: '002-014-02', free: 29 },
    { block: 'R2FD', location: '003-003-01', free: 2 },
    { block: 'R2FD', location: '003-013-01', free: 30 },
  ]), { groupByLocation: true, draws });
  assert.deepEqual(labels(out), ['R2FD-003-013-01=30', 'R2FD-002-014-02=28', 'R2FD-003-003-01=2']);
});

t('④ 取消・無効のバッチ・別の商品・🔴再ピックのバッチは数えない / 写しの時刻が読めなければ何も引かない', () => {
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  mkBatch({ createdAt: AFTER, status: 'cancelled', lines: [{ block: 'P3FC', location: '00200305', qty: 1 }] });
  mkBatch({ createdAt: AFTER, validity: 'invalid', lines: [{ block: 'P3FC', location: '00200305', qty: 1 }] });
  mkBatch({ createdAt: AFTER, origin: 'repick', lines: [{ block: 'P3FC', location: '00200305', qty: 1 }] });
  mkBatch({ createdAt: AFTER, lines: [{ block: 'P3FC', location: '00200305', qty: 1, sku: 'hiragushi180' }] });
  assert.equal(pickingStockDraws('hiragushi240', SNAP).size, 0);
  mkBatch({ createdAt: AFTER, lines: [{ block: 'P3FC', location: '00200305', qty: 1 }] });
  assert.equal(pickingStockDraws('hiragushi240', null).size, 0);
  assert.equal(pickingStockDraws('hiragushi240', 'not-a-date').size, 0);
  assert.equal(pickingStockDraws('', SNAP).size, 0);
});

t('⑤ 同じロケの期限違いは期限の近い順に引く (通知本文はロットごとの行)', () => {
  const draws = new Map([[stockDrawKey('P3FC', '00200305'), { qty: 3, depleted: false }]]);
  const data = snapshot([
    { block: 'P3FC', location: '002-003-05', free: 5, expiry: '20280101' },
    { block: 'P3FC', location: '002-003-05', free: 2, expiry: '20270601' },
  ]);
  const raw = listStockCandidates(data, { draws });
  assert.deepEqual(raw.rows.map((r) => `${r.expiry}=${r.free}`), ['2028/01/01=4'], '近い期限 2 個を使い切り、残り 1 個を遠い期限から');
  const text = buildStockLocationsText(data, { draws, now: new Date(SNAP) });
  assert.match(text, /P3FC-002-003-05 → 4個/);
  assert.equal(listStockCandidates(data, { draws: null }).rows.length, 2, 'draws 無しは従来どおり');
});

// ─── 画面の API (GET /api/batches/:id/stock-locations) を通して、差し引いた候補が返ること ───
{
  const express = (await import('express')).default;
  const { createDevice } = await import('../db.js');
  db.exec('DELETE FROM pk_lines; DELETE FROM pk_batches;');
  const batchId = mkBatch({ createdAt: AFTER, lines: [
    { block: 'P3FA', location: '00500401', qty: 2, status: 'pending' },
    { block: 'P3FC', location: '00200305', qty: 1, status: 'pending' },
  ] });
  const token = createDevice('倉庫iPhone1', 'admin@b-faith.biz', 'worker');
  process.env.WAREHOUSE_URL = 'http://wh.local';
  process.env.WAREHOUSE_SERVICE_TOKEN = 'tkn';
  const realFetch = globalThis.fetch;
  let whOk = true;
  globalThis.fetch = async (url, opts) => {
    if (!String(url).startsWith('http://wh.local/')) return realFetch(url, opts);
    if (!whOk) return { ok: false, status: 503, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => snapshot([
      { block: 'P3FA', location: '005-004-01', free: 1 },
      { block: 'P3FC', location: '002-003-05', free: 1 },
      { block: 'R2FD', location: '002-014-02', free: 29 },
    ]) };
  };
  const router = (await import('../router.js')).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => { req.session = {}; next(); });
  app.use('/apps/picking', router);
  const server = app.listen(0, '127.0.0.1');
  await new Promise((r) => server.once('listening', r));
  const get = async (seq) => {
    const res = await realFetch(`http://127.0.0.1:${server.address().port}/apps/picking/api/batches/${batchId}/stock-locations?seq=${seq}`,
      { headers: { cookie: `pk_device=${token}` } });
    return { status: res.status, body: await res.json() };
  };
  try {
    const r = await get(1);
    assert.equal(r.status, 200);
    assert.equal(r.body.fetched, true);
    assert.deepEqual(r.body.rows.map((x) => `${x.label}=${x.free}`), ['R2FD-002-014-02=29'], 'P3FC-002-003-05 は候補に出ない');
    whOk = false;
    const down = await get(1);
    assert.equal(down.body.fetched, false, '在庫が取れないときは「取得できず」(候補ゼロと区別) のまま');
    passed++; console.log('  ok: 画面の API: 同じピッキングで後で取る棚は候補に出ない / 在庫が取れないときは取得できずのまま');
  } finally {
    server.close();
    globalThis.fetch = realFetch;
    delete process.env.WAREHOUSE_URL;
    delete process.env.WAREHOUSE_SERVICE_TOKEN;
  }
}

// ─── 欠品通知の他ロケ在庫も同じ数 (notifyShortage に stockDrawsFor を渡す) ───
{
  const { notifyShortage } = await import('../notify.js');
  process.env.WAREHOUSE_URL = 'http://wh.local';
  process.env.WAREHOUSE_SERVICE_TOKEN = 'tkn';
  process.env.PICKING_ALERT_WEBHOOK = 'https://chat.example/webhook';
  delete process.env.PICKING_SHORTAGE_LINE;
  const realFetch = globalThis.fetch;
  const sent = [];
  globalThis.fetch = async (url, opts) => {
    if (String(url).startsWith('http://wh.local/')) {
      return { ok: true, status: 200, json: async () => snapshot([
        { block: 'P3FC', location: '002-003-05', free: 1 },
        { block: 'R2FD', location: '002-014-02', free: 29 },
      ]) };
    }
    sent.push(JSON.parse(opts.body));
    return { ok: true, status: 200, text: async () => '' };
  };
  try {
    const info = {
      batch: { folder_name: '出荷_32', hikiate_class: 'AES《複数SKU複数個》' },
      line: { locationLabel: 'P3FA-005-004-01', block: 'P3FA', location: '00500401', sku: 'hiragushi240', product_name: '平串', qty: 2 },
      worker: '三宅晴菜', shortageQty: 1,
      stockDrawsFor: () => new Map([[stockDrawKey('P3FC', '00200305'), { qty: 1, depleted: false }]]),
    };
    await notifyShortage(info);
    assert.ok(sent[0].text.includes('・R2FD-002-014-02 → 29個'), sent[0].text);
    assert.ok(!sent[0].text.includes('P3FC-002-003-05'), sent[0].text);
    sent.length = 0;
    await notifyShortage({ ...info, stockDrawsFor: undefined });
    assert.ok(sent[0].text.includes('・P3FC-002-003-05 → 1個'), '渡さなければ従来どおり');
    passed++; console.log('  ok: 欠品通知の他ロケ在庫も差し引いた数');
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.WAREHOUSE_URL;
    delete process.env.WAREHOUSE_SERVICE_TOKEN;
    delete process.env.PICKING_ALERT_WEBHOOK;
  }
}

console.log(`\ntest-stock-draws: ${passed} passed`);
