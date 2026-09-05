/**
 * 判定 API (judge-api) の結合テスト。
 *   node apps/postage/smoke-judge.mjs
 *
 * 一時ディレクトリに postage.db と packing-dispatch の出力履歴 (warehouse-mirror.db の fixture) を作り、
 * express に judge-router を載せて実際に HTTP で叩く。本番には触れない。
 */
import Database from 'better-sqlite3';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'postage-judge-'));
process.env.DATA_DIR = TMP;
delete process.env.POSTAGE_JUDGE_KEY;

let pass = 0, fail = 0;
const t = async (name, fn) => {
  try { await fn(); pass++; console.log(`  ok   ${name}`); }
  catch (e) { fail++; console.log(`  FAIL ${name}\n       ${e.message}`); }
};
const eq = (a, b, what) => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what || ''} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
};

// ── packing-dispatch の出力履歴 fixture ──
{
  const d = new Database(path.join(TMP, 'warehouse-mirror.db'));
  d.exec(`CREATE TABLE pd_shipment_tracking (
    ne_uketsuke_no TEXT PRIMARY KEY, shipping_method_code TEXT NOT NULL, shop_name TEXT, order_no TEXT,
    product_items_json TEXT, exported_at TEXT)`);
  const ins = d.prepare('INSERT INTO pd_shipment_tracking VALUES (?,?,?,?,?,?)');
  const items = (arr) => JSON.stringify(arr.map(([product_code, qty]) => ({ product_code, product_name: product_code, qty })));
  ins.run('S1', 'teikeigai', '楽天', 'r-1', items([['sku-a', 1]]), '2026-09-05T00:30:00.000Z');
  ins.run('S2', 'teikeigai', '楽天', 'r-2', items([['sku-nw', 1]]), '2026-09-05T00:30:00.000Z');   // 重さ未登録
  ins.run('S3', 'letterpack', '楽天', 'r-3', items([['sku-a', 1]]), '2026-09-05T00:30:00.000Z');  // 定形外ではない
  ins.run('S4', 'teikeigai', 'Yahoo', 'y-4', '{broken', '2026-09-05T00:30:00.000Z');              // 壊れた JSON
  ins.run('S5', 'teikeigai', 'Yahoo', 'y-5', items([['SKU-A', 2]]), '2026-09-05T00:30:00.000Z');  // 2個・大文字
  ins.run('S6', 'teikeigai', 'Yahoo', 'y-6', items([['sku-a', 1.5]]), '2026-09-05T00:30:00.000Z'); // 小数の数量
  d.close();
}

const { initPostageDB, getDB, closePostageDB } = await import('./db.js');
initPostageDB();
{
  const db = getDB();
  db.prepare("UPDATE pm_materials SET outer_length_mm=235, outer_width_mm=120, dims_verified=1, thickness_mm=1 WHERE material_code='chabuto'").run();
  const ins = db.prepare(`INSERT INTO pm_skus (sku_code, display_name, unit_weight_g, thickness_mm, default_material_code) VALUES (?,?,?,?,?)`);
  ins.run('sku-a', '商品A', 15, 1, 'chabuto');
  ins.run('sku-nw', '重さ未登録', null, 1, 'chabuto');
}

const { default: judgeRouter } = await import('./judge-router.js');
const app = express();
app.use('/apps/postage/judge-api', judgeRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE = `http://127.0.0.1:${server.address().port}/apps/postage/judge-api`;

const call = (body, { key = 'test-key', raw } = {}) => fetch(`${BASE}/batch`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', ...(key === null ? {} : { 'x-api-key': key }) },
  body: raw ?? JSON.stringify(body),
});

console.log(`一時ディレクトリ: ${TMP}\n`);
console.log('■ 認証');
await t('キー未設定なら 503 (fail-closed)', async () => {
  const r = await call({ slip_nos: ['S1'] });
  eq(r.status, 503); eq((await r.json()).error, 'postage_judge_key_unset');
});
process.env.POSTAGE_JUDGE_KEY = 'test-key';
await t('キー違いは 401', async () => { eq((await call({ slip_nos: ['S1'] }, { key: 'wrong' })).status, 401); });
await t('キー無しも 401', async () => { eq((await call({ slip_nos: ['S1'] }, { key: null })).status, 401); });
await t('health は料金表と構成データの有無を返す', async () => {
  const r = await fetch(`${BASE}/health`, { headers: { 'x-api-key': 'test-key' } });
  const j = await r.json();
  eq(r.status, 200); eq(j.composition_available, true); eq(!!j.tariff, true);
});

console.log('\n■ 入力の検証');
await t('伝票番号が空なら 400', async () => { eq((await call({ slip_nos: [] })).status, 400); });
await t('501 件は 400', async () => { eq((await call({ slip_nos: Array.from({ length: 501 }, (_, i) => `X${i}`) })).status, 400); });
await t('文字列でない伝票番号は 400', async () => { eq((await call({ slip_nos: [{ a: 1 }] })).status, 400); });
await t('date の形式違いは 400', async () => { eq((await call({ slip_nos: ['S1'], date: '2026/09/05' })).status, 400); });
await t('壊れた JSON は 400', async () => { eq((await call(null, { raw: '{oops' })).status, 400); });
await t('検証で弾いたときは判定ログに何も残らない', async () => {
  eq(getDB().prepare('SELECT COUNT(*) n FROM pm_print_decisions').get().n, 0);
});

console.log('\n■ 判定');
const r1 = await call({ slip_nos: ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S9', ' S1 '], source: 'launcher', batch_ref: '出荷_13' });
const j1 = await r1.json();
const by = Object.fromEntries((j1.results || []).map((x) => [x.slip_no, x]));
await t('200 で伝票ごとの結果が送った順に返る', async () => {
  eq(r1.status, 200, JSON.stringify(j1));
  eq(j1.results.map((x) => x.slip_no), ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S9', 'S1'], '前後の空白は寄せる');
});
await t('確定: 印字文言に区分と金額 (定形 50g以内 110円)', async () => {
  eq(by.S1.status, 'confirmed'); eq(by.S1.print_text, '定形 50g以内 110円');
  eq(by.S1.amount_yen, 110); eq(by.S1.material_name, '茶封筒'); eq(by.S1.weight_g, 20.5); eq(by.S1.thickness_mm, 2);
  eq(by.S1.status_label, '確定');
});
await t('不明: 理由つきで「不明 (…)」', async () => {
  eq(by.S2.status, 'unknown'); eq(by.S2.print_text, '不明 (商品の重さ未登録)'); eq(by.S2.amount_yen, null);
  eq(by.S2.reason, 'missing_weight'); eq(by.S2.detail, 'sku-nw');
});
await t('定形外でない伝票は対象外 (何も刷らない)', async () => {
  eq(by.S3.status, 'skipped'); eq(by.S3.print_text, ''); eq(by.S3.method_code, 'letterpack'); eq(by.S3.status_label, '対象外');
});
await t('壊れた構成は不明 (黙って確定しない)', async () => { eq(by.S4.status, 'unknown'); eq(by.S4.reason, 'no_lines'); });
await t('大文字の商品コードも当たる・数量 2 は重さも厚みも 2 倍', async () => {
  eq(by.S5.status, 'confirmed'); eq(by.S5.weight_g, 35.5); eq(by.S5.thickness_mm, 3);
});
await t('小数の数量は不明', async () => { eq(by.S6.status, 'unknown'); eq(by.S6.reason, 'no_lines'); });
await t('packing-dispatch に無い伝票は「商品構成が見つからない」', async () => {
  eq(by.S9.status, 'unknown'); eq(by.S9.reason, 'missing_composition');
  eq(by.S9.print_text, '不明 (商品構成が見つからない)'); eq(by.S9.detail, 'packing-dispatch に出力の記録が無い');
});
await t('集計が合う', async () => {
  eq(j1.summary, { total: 8, confirmed: 3, unknown: 4, skipped: 1, not_found: 1 });
});
await t('判定IDは 6桁日付-4文字 (読み間違えやすい 0/O/1/I/L を含まない)', async () => {
  for (const x of j1.results) {
    if (!/^\d{6}-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}$/.test(x.decision_id)) throw new Error(`${x.slip_no}: ${x.decision_id}`);
  }
  eq(new Set(j1.results.map((x) => x.decision_id)).size, 8, '全部違う');
});
await t('明細そのもの (lines) は応答に含めない (ランチャーには要らない)', async () => { eq('lines' in by.S1, false); });

console.log('\n■ 判定ログ');
await t('送った伝票が全部ログに残る (対象外も)', async () => {
  const rows = getDB().prepare('SELECT * FROM pm_print_decisions ORDER BY rowid').all();
  eq(rows.length, 8);
  eq(rows.map((r) => r.slip_no), ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S9', 'S1']);
  eq(rows.every((r) => r.source === 'launcher' && r.batch_ref === '出荷_13'), true);
});
await t('印字した文言・金額・構成が固定保存される', async () => {
  const r = getDB().prepare("SELECT * FROM pm_print_decisions WHERE slip_no='S5'").get();
  eq(r.print_text, '定形 50g以内 110円'); eq(r.amount_yen, 110); eq(r.band_code, 'teikei_50');
  eq(JSON.parse(r.lines_json), [{ sku_code: 'sku-a', qty: 2 }]);
  eq(r.composition_source, 'packing-dispatch'); eq(typeof r.tariff_version_id, 'number');
  eq(r.requested_by, 'judge-api:launcher');
});
await t('見つからない伝票は構成なし・出どころ NULL で残る', async () => {
  const r = getDB().prepare("SELECT * FROM pm_print_decisions WHERE slip_no='S9'").get();
  eq(r.lines_json, '[]'); eq(r.composition_source, null); eq(r.method_code, null);
});
await t('同じ伝票をもう一度送ると別の判定IDで別レコード (再印字)', async () => {
  const r = await call({ slip_nos: ['S1'] });
  const j = await r.json();
  eq(j.results[0].decision_id !== by.S1.decision_id, true);
  eq(getDB().prepare("SELECT COUNT(*) n FROM pm_print_decisions WHERE slip_no='S1'").get().n, 3);
});
await t('料金表が無い日 (改定前) は不明 (no_tariff)', async () => {
  const r = await call({ slip_nos: ['S1'], date: '2020-01-01' });
  const j = await r.json();
  eq(j.tariff, null); eq(j.results[0].reason, 'no_tariff');
});
await t('マスタを直せば次の呼び出しから効く (sku-nw に重さを入れる)', async () => {
  getDB().prepare("UPDATE pm_skus SET unit_weight_g=20 WHERE sku_code='sku-nw'").run();
  const j = await (await call({ slip_nos: ['S2'] })).json();
  eq(j.results[0].status, 'confirmed'); eq(j.results[0].print_text, '定形 50g以内 110円');
});
await t('判定ログの一覧: 不明が上・集計つき', async () => {
  const { listDecisions } = await import('./judge-service.js');
  const d = listDecisions({ date: new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10) });
  eq(d.rows[0].status, 'unknown');
  eq(d.summary.total, d.rows.length);
  eq(d.reasons.some((r) => r.reason === 'missing_composition'), true);
});

server.close();
closePostageDB();
try { fs.rmSync(TMP, { recursive: true, force: true }); }
catch { console.log(`  (一時ディレクトリを消せませんでした: ${TMP})`); }
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
