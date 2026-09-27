/**
 * test-po-ne-codes.mjs — 発注アプリの入荷予定の商品ID を Company DB の NE の元の書き方で補う (M6。apps/purchase-orders/ne-codes.js)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.2「M6」契約 v1〜v3):
 *   1 書き方: canonical → Company DB (ok・鍵と同じ) → 今の予備。警告は Company DB の状態だけで決める。衝突は canonical があっても外す
 *   2 読み手: 印と元の書き方を 1 つの読み取りの取引で読む・印が無い = 使わない・7 日より古い印は stale
 *   3 期限: 応答しない相手でも期限で戻る・接続は捨てる (何度でも残らない)・つながらない = error・未設定 = not_configured
 * 使い方: node scripts/test-po-ne-codes.mjs
 */
import assert from 'node:assert/strict';
import net from 'node:net';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const N = await import('../apps/purchase-orders/ne-codes.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const ne = (o) => ({ ok: true, map: new Map(Object.entries(o)) });

console.log('test-po-ne-codes');

await ta('[1] 書き方の決め方: canonical → Company DB (ok・鍵と同じ) → 予備 / 警告は Company DB の状態だけ / 衝突は canonical があっても外す', async () => {
  const R = (key, canonical, e, fallback = key) => { const x = N.resolveLzCode({ key, canonical, fallback, ne: e }); return [x.productCode, x.caseSource, x.caseVerified, x.caseWarning, x.pasteBlocked]; };
  assert.deepEqual(R('abc', null, null), ['abc', 'fallback', false, null, null]);                 // 読めない = 今の動き
  assert.deepEqual(R('abc', null, { ok: false, reason: 'timeout' }), ['abc', 'fallback', false, null, null]);
  assert.deepEqual(R('abc', null, ne({})), ['abc', 'fallback', false, null, null]);                // Company DB に無い
  assert.deepEqual(R('abc', null, ne({ abc: { state: 'ok', ne_code: 'ABC' } })), ['ABC', 'ne_api', true, null, null]);
  assert.deepEqual(R('abc', 'Abc', ne({ abc: { state: 'ok', ne_code: 'ABC' } })), ['Abc', 'canonical', true, 'ne_api_differs', null]);
  assert.deepEqual(R('abc', 'ABC', ne({ abc: { state: 'ok', ne_code: 'ABC' } })), ['ABC', 'canonical', true, null, null]);
  assert.deepEqual(R('abc', null, ne({ abc: { state: 'collided', ne_code: null } })), ['abc', 'fallback', false, 'ne_code_collided', 'ne_code_collided']);
  assert.deepEqual(R('abc', 'ABC', ne({ abc: { state: 'collided', ne_code: null } })), ['ABC', 'canonical', true, 'ne_code_collided', 'ne_code_collided']);   // canonical があっても外す
  assert.deepEqual(R('abc', 'ABC', ne({ abc: { state: 'invalid', ne_code: null } })), ['ABC', 'canonical', true, 'ne_code_invalid', null]);   // canonical があっても注意
  assert.deepEqual(R('abc', null, ne({ abc: { state: 'ok', ne_code: 'XYZ' } })), ['abc', 'fallback', false, 'ne_code_invalid', null]);   // 鍵と違う書き方は使わない
  assert.equal(N.resolveLzCode({ key: 'abc', fallback: 'abc', ne: ne({ abc: { state: 'ok', ne_code: 'ABC' } }) }).neCode, 'ABC');
});

const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: () => {} });
const conn = async () => ({ query: (sql, p) => db.query(sql, p), end: async () => {} });

await ta('[2] 読み手: 印が無い = 使わない / 印と元の書き方を読む (kind = product だけ) / 7 日より古い印は stale / 1 つの読み取りの取引', async () => {
  let r = await N.readNeCodes({ connect: conn });
  assert.deepEqual([r.ok, r.reason], [false, 'no_mark']);
  await db.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20300110T220100000Z_aaaaaa', '2030-01-10T22:01:00Z', 0)`);
  await db.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: 'mc_20300110T220100000Z_aaaaaa', entries: [
    { code_norm: 'abc', kind: 'product', state: 'ok', ne_code: 'ABC', spellings: ['ABC'] },
    { code_norm: 'dup', kind: 'product', state: 'collided', ne_code: null, spellings: ['DUP', 'dup'] },
    { code_norm: 'abc', kind: 'rep', state: 'ok', ne_code: 'Abc', spellings: ['Abc'] },   // 代表の名札 = 使わない
  ] })]);
  const t0 = Date.parse('2030-01-11T00:00:00Z');
  r = await N.readNeCodes({ connect: conn, nowMs: t0 });
  assert.equal(r.ok, true);
  assert.deepEqual(r.mark, { compare_run_id: 'mc_20300110T220100000Z_aaaaaa', observed_at: '2030-01-10T22:01:00.000Z' });
  assert.equal(r.stale, false);
  assert.deepEqual([...r.map].sort(), [['abc', { state: 'ok', ne_code: 'ABC' }], ['dup', { state: 'collided', ne_code: null }]]);
  r = await N.readNeCodes({ connect: conn, nowMs: t0 + 8 * 86400000 });
  assert.equal(r.stale, true);
  // 1 つの読み取りの取引 (0041 は全部の入れ替えと印を同じ取引で書く = 同じ世代を読む)
  const seen = [];
  await N.loadNeCodes({ query: async (sql) => { seen.push(sql.split(' ').slice(0, 3).join(' ')); return { rows: sql.includes('mark') ? [{ compare_run_id: 'x', observed_at: new Date() }] : [] }; } });
  assert.deepEqual(seen, ['begin transaction isolation', 'select compare_run_id, observed_at', 'select code_norm, state,', 'commit']);
  // 状態の要約 (map は載せない)
  assert.deepEqual(N.neCodesStatus(r), { ok: true, reason: null, mark: r.mark, stale: true });
  assert.deepEqual(N.neCodesStatus({ ok: false, reason: 'timeout' }), { ok: false, reason: 'timeout' });
});

await ta('[3] 期限: 応答しない相手 (つながるが何も返さない) でも期限で戻る・接続を捨てる (5 回やっても残らない)', async () => {
  const open = new Set();
  // 相手は読むが何も返さない (読まないと切断の知らせ = end が届かず、閉じたことを数えられない)
  const server = net.createServer((sock) => { open.add(sock); sock.resume(); sock.on('close', () => open.delete(sock)); sock.on('error', () => {}); });
  await new Promise((res) => server.listen(0, '127.0.0.1', res));
  const port = server.address().port;
  try {
    for (let k = 0; k < 5; k++) {
      const t = Date.now();
      const r = await N.readNeCodes({ url: `postgres://u:p@127.0.0.1:${port}/x`, deadlineMs: 400 });
      const took = Date.now() - t;
      assert.deepEqual([r.ok, r.reason], [false, 'timeout']);
      assert.ok(took < 1500, `期限で戻らない: ${took}ms`);
      // pg の予備の時間切れ (期限の 2 倍) より前 = こちらが捨てたから閉じている
      await new Promise((res) => setTimeout(res, 100));
      assert.equal(open.size, 0, `接続が残った: ${open.size} (${k + 1} 回目)`);
    }
  } finally { server.close(); }
});

await ta('[4] 期限: 差し替えた接続が返らない = 期限で戻る / 期限の後に返った接続も捨てる / つながらない = error / 未設定 = not_configured', async () => {
  let ended = 0, release;
  const slow = () => new Promise((res) => { release = () => res({ query: async () => ({ rows: [] }), end: async () => { ended++; } }); });
  const t = Date.now();
  const r = await N.readNeCodes({ connect: slow, deadlineMs: 200 });
  assert.deepEqual([r.ok, r.reason], [false, 'timeout']);
  assert.ok(Date.now() - t < 1000);
  release();
  await new Promise((res) => setTimeout(res, 50));
  assert.equal(ended, 1, '期限の後に返った接続を捨てていない');
  // 閉じたポート = error (すぐ)
  const srv = net.createServer(); await new Promise((res) => srv.listen(0, '127.0.0.1', res)); const port = srv.address().port; await new Promise((res) => srv.close(res));
  const e = await N.readNeCodes({ url: `postgres://u:p@127.0.0.1:${port}/x`, deadlineMs: 2000 });
  assert.deepEqual([e.ok, e.reason], [false, 'error']);
  assert.deepEqual(await N.readNeCodes({ url: '' }), { ok: false, reason: 'not_configured' });
  // 要求ごとの読み手: 差し替えが投げても投げない
  N.setNeCodeReaderForTest(async () => { throw new Error('boom'); });
  const q = await N.readNeCodesForRequest();
  assert.deepEqual([q.ok, q.reason], [false, 'error']);
  N.setNeCodeReaderForTest(null);
});

await ta('[5] つないだ後に相手が切れても落ちない (pg の error を受ける) = error で返る・プロセスに投げない (Codex #1501 R1 High)', async () => {
  const caught = [];
  const onUncaught = (e) => caught.push(e);
  process.on('uncaughtException', onUncaught);
  // PostgreSQL のふり: 始めの挨拶に「認証 OK + 準備できた」を返し、最初の問い合わせで切る
  const server = net.createServer((sock) => {
    sock.on('error', () => {});
    let started = false;
    sock.on('data', () => {
      if (!started) { started = true; sock.write(Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5a, 0, 0, 0, 5, 0x49])); return; }
      sock.destroy();
    });
  });
  await new Promise((res) => server.listen(0, '127.0.0.1', res));
  try {
    const r = await N.readNeCodes({ url: `postgres://u:p@127.0.0.1:${server.address().port}/x`, deadlineMs: 2000 });
    assert.deepEqual([r.ok, r.reason], [false, 'error']);
    await new Promise((res) => setTimeout(res, 200));
    assert.equal(caught.length, 0, `プロセスに投げた: ${caught.map((e) => e.message).join(' / ')}`);
  } finally { process.off('uncaughtException', onUncaught); server.close(); }
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
