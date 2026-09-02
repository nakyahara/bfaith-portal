/**
 * 入荷受付チェック — Notion 作業カード sweep (notion-sync.js) のテスト
 *
 * 実行: node scripts/test-inbound-check-notion.mjs
 * Notion API は global.fetch をモックして再現する (実 API は叩かない)。
 *
 * 検証項目:
 *   1. env 未設定なら not_configured (fail-closed)
 *   2. 送信対象の抽出: いろは行きのみ / 取消済み・送信済み・ブロック中は除外
 *   3. カード作成: プロパティの組み立て (destination_id / 実数 / 取引先=仕入先マスタ /
 *      30日販売数 (by_product のみ) / 外部出し目安 (良品在庫のみ) / 有効期限 / 入数は送らない)
 *   4. スキーマ ensure: destination_id・有効期限 が無ければ DB に足す
 *   5. 送信前に取り消された行は送らない (直前の再チェック)
 *   6. 送信後の取消 → ステータス「取消」+ 元ステータス記録 + 要確認一覧
 *   7. 回収: destination_id で既存カードが見つかれば新規作成しない (API成功/DB記録失敗への備え)
 *   8. 4xx は再試行ブロック → 再送 (resetNotionRow) で解除
 *   9. アーカイブ済みカードの取消は「収束済み」として扱う
 *  10. 2回目の sweep は何もしない (冪等)
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-notion-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

// ─── fetch モック ───
const mock = {
  dbProps: {
    '名前': {}, 'ステータス': {}, '商品コード': {}, '数量': {}, '入庫日': {}, '入荷管理番号': {},
    'バーコード': {}, '取引先': {}, '仕入先': {}, '取扱区分': {}, '在庫化必要FLG': {}, '作業拠点': {},
    '過去30日販売数': {}, '外部出し目安': {}, '入数': {},
    // destination_id と 有効期限 は最初は無い → ensure が足すことを確認する
  },
  queryResults: new Map(),   // destination_id → page オブジェクト
  pageStates: new Map(),     // pageId → { archived, status }
  failCreate: null,          // { status } 次の POST /pages を1回失敗させる
  created: [],
  patchedPages: [],
  patchedDb: [],
};
let pageSeq = 0;
global.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body ? JSON.parse(opts.body) : null;
  const u = String(url);
  const respond = (status, obj) => ({ ok: status < 300, status, headers: { get: () => null }, json: async () => obj });
  if (u.endsWith('/databases/testdb') && method === 'GET') {
    return respond(200, { object: 'database', properties: { ...mock.dbProps } });
  }
  if (u.endsWith('/databases/testdb') && method === 'PATCH') {
    mock.patchedDb.push(body);
    for (const k of Object.keys(body.properties || {})) mock.dbProps[k] = {};
    return respond(200, { object: 'database' });
  }
  if (u.endsWith('/databases/testdb/query') && method === 'POST') {
    const destId = body?.filter?.number?.equals;
    const hit = mock.queryResults.get(destId);
    return respond(200, { object: 'list', results: hit ? [hit] : [] });
  }
  if (u.endsWith('/pages') && method === 'POST') {
    if (mock.failCreate) { const f = mock.failCreate; mock.failCreate = null; return respond(f.status, { object: 'error', message: 'mock fail ' + f.status, code: 'mock' }); }
    const id = 'page-' + (++pageSeq);
    mock.created.push({ id, props: body.properties });
    mock.pageStates.set(id, { archived: false, status: body.properties?.['ステータス']?.select?.name || null });
    return respond(200, { object: 'page', id, url: 'https://notion.so/' + id });
  }
  const mPage = u.match(/\/pages\/([^/?]+)$/);
  if (mPage && method === 'GET') {
    const st = mock.pageStates.get(mPage[1]) || { archived: false, status: null };
    return respond(200, { object: 'page', id: mPage[1], archived: st.archived, properties: { 'ステータス': { select: st.status ? { name: st.status } : null } } });
  }
  if (mPage && method === 'PATCH') {
    mock.patchedPages.push({ id: mPage[1], body });
    const st = mock.pageStates.get(mPage[1]) || { archived: false, status: null };
    if (body.properties?.['ステータス']?.select?.name) st.status = body.properties['ステータス'].select.name;
    mock.pageStates.set(mPage[1], st);
    return respond(200, { object: 'page', id: mPage[1] });
  }
  return respond(404, { object: 'error', message: 'unmocked ' + method + ' ' + u });
};

// ─── DB 準備 ───
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB } = await import('../apps/inbound-check/db.js');
const {
  runNotionSweep, collectUnsent, collectCancelPending, calcExternal, resetNotionRow, notionStatusForAdmin,
} = await import('../apps/inbound-check/notion-sync.js');

console.log('DATA_DIR =', process.env.DATA_DIR);

console.log('\n[1] env 未設定なら not_configured');
{
  delete process.env.NOTION_TOKEN;
  delete process.env.INBOUND_CHECK_NOTION_DB_ID;
  const r = await runNotionSweep({ actor: 'cron' });
  ok(r.ok === false && r.error === 'not_configured', 'not_configured で fail-closed');
}
process.env.NOTION_TOKEN = 'test-token';
process.env.INBOUND_CHECK_NOTION_DB_ID = 'testdb';

const db = getDB();
db.prepare(`INSERT INTO f_inbound_check_batches (id, source, file_name, file_hash, csv_generated_at, row_count, slip_count, imported_at, status)
  VALUES (1, 'manual_upload', 't.csv', 'hash1', '2026-09-02T00:00:00Z', 3, 1, '2026-09-02T00:00:00Z', 'active')`).run();
const insLine = db.prepare(`INSERT INTO f_inbound_check_lines
  (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, barcode, planned_qty, seq)
  VALUES (1, ?, 'AR001', 1, 1, ?, ?, ?, ?, ?, 1)`);
insLine.run('L1', 'PROD-A', 'prod-a', '商品A', '4501234567890', 10);
insLine.run('L2', 'PROD-B', 'prod-b', '商品B', '4500000000002', 5);
insLine.run('L3', 'PROD-C', 'prod-c', '商品C', '4500000000003', 7);

function seedDest({ product = 'PROD-A', code = 'prod-a', line = 'L1', qty = 10, actual = 8, dest = 'iroha', expiry = null, name = '商品A' } = {}) {
  const r = db.prepare(`INSERT INTO f_inbound_check_destinations
    (batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at, expiry_date, work_date, code_key, actual_qty)
    VALUES (1, ?, 'AR001', ?, ?, ?, ?, 'master', 'テスト作業者', ?, ?, '2026-09-02', ?, ?)`)
    .run(line, product, name, qty, dest, new Date().toISOString(), expiry, code, actual);
  return Number(r.lastInsertRowid);
}
const destRow = (id) => db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = ?').get(id);
const cancelDest = (id, reason = 'reopen') => db.prepare(
  "UPDATE f_inbound_check_destinations SET cancelled_at = ?, cancelled_by = 'test', cancel_reason = ? WHERE id = ?"
).run(new Date().toISOString(), reason, id);

// 参照データ (取引先・販売・在庫)
db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
  VALUES (1, 'PROD-A', '商品A', '単品', '取扱中', '確定', '0001', '2026-09-02T00:00:00Z')`).run();
db.exec('CREATE TABLE IF NOT EXISTS po_suppliers (supplier_code TEXT PRIMARY KEY, supplier_name TEXT NOT NULL)');
db.prepare("INSERT INTO po_suppliers (supplier_code, supplier_name) VALUES ('1', 'AMC')").run();
const today = new Date().toISOString().slice(0, 10);
const insSales = db.prepare(`INSERT INTO mirror_sales_daily (日付, 商品コード, モール, 数量, データ種別, チャネル, updated_at)
  VALUES (?, 'PROD-A', ?, ?, ?, '', ?)`);
insSales.run(today, 'rakuten', 30, 'by_product', today);
insSales.run(today, 'yahoo', 999, 'by_listing', today);   // by_listing は数えない
const insStock = db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
  VALUES ('PROD-A', ?, ?, ?, ?, ?)`);
insStock.run('良品', 100, 10, today, today);
insStock.run('不良品', 50, 0, today, today);   // 不良品は外部出しに数えない

console.log('\n[2] 送信対象の抽出');
const d1 = seedDest({ expiry: '2026-12' });
const d2 = seedDest({ product: 'PROD-B', code: 'prod-b', line: 'L2', dest: 'bfaith', name: '商品B' });
{
  const rows = collectUnsent(db);
  ok(rows.length === 1 && rows[0].id === d1, 'いろは行きだけが対象 (B-Faith 行きは送らない)');
}

console.log('\n[3] カード作成とプロパティ');
{
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.sent === 1 && r.errors === 0, `1件送信 (sent=${r.sent} errors=${r.errors})`);
  const row = destRow(d1);
  ok(!!row.notion_page_id && !!row.notion_synced_at, 'page_id と synced_at が記録される');
  ok(!!row.notion_payload && JSON.parse(row.notion_payload)['destination_id'], 'payload スナップショットが残る');
  const p = mock.created[0].props;
  ok(p['名前'].title[0].text.content === '商品A', '名前 = 商品名');
  ok(p['ステータス'].select.name === '未着手', 'ステータス = 未着手');
  ok(p['数量'].number === 8, '数量 = 実際に数えた数 (actual_qty)');
  ok(p['入庫日'].date.start === '2026-09-02', '入庫日 = 荷受け日 (work_date)');
  ok(p['destination_id'].number === d1, 'destination_id = 台帳の行ID');
  ok(p['バーコード'].rich_text[0].text.content === '4501234567890', 'バーコード = 取込行から');
  ok(p['取引先'].select.name === 'AMC', '取引先 = 仕入先マスタ (po_suppliers) の名称');
  ok(p['仕入先'].number === 1, '仕入先 = 仕入先コード (数値)');
  ok(p['取扱区分'].select.name === '取扱中', '取扱区分 = mirror_products');
  ok(p['在庫化必要FLG'].checkbox === true, '在庫化必要FLG = true (いろは行き=在庫化する)');
  ok(p['作業拠点'].select.name === 'いろは', '作業拠点 = いろは');
  ok(p['有効期限'].rich_text[0].text.content === '2026-12', '有効期限 = YYYY-MM も送れる (rich_text)');
  ok(p['過去30日販売数'].rich_text[0].text.content === '30個', '30日販売数 = by_product のみ合計');
  // フリー在庫 = 良品 100-10 = 90。keep = ceil(30/30*14) = 14 → 外部出し 76
  ok(p['外部出し目安'].rich_text[0].text.content === '外部施設に76個まで預けてOK', '外部出し目安 = 良品フリー在庫 − キープ数');
  ok(!('入数' in p), '入数は送らない (意味の違う数字を見せない — PR2 で units_per_container)');
}

console.log('\n[4] スキーマ ensure');
{
  ok(mock.patchedDb.length === 1, 'DB スキーマの PATCH は1回だけ');
  const added = Object.keys(mock.patchedDb[0]?.properties || {});
  ok(added.includes('destination_id') && added.includes('有効期限'), 'destination_id と 有効期限 を足した');
}

console.log('\n[5] 送信前に取り消された行は送らない');
{
  const d3 = seedDest({ product: 'PROD-C', code: 'prod-c', line: 'L3', name: '商品C' });
  cancelDest(d3);
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && mock.created.length === before, '取消済みは作成されない');
  ok(!destRow(d3).notion_page_id, 'page_id も付かない');
}

console.log('\n[6] 送信後の取消 → ステータス「取消」');
{
  const row = destRow(d1);
  mock.pageStates.set(row.notion_page_id, { archived: false, status: '作業中' });   // 現場が動かしていた想定
  cancelDest(d1, 'reopen');
  ok(collectCancelPending(db).length === 1, '取消の反映対象になる');
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.cancelled === 1, `取消を反映 (cancelled=${r.cancelled})`);
  const after = destRow(d1);
  ok(!!after.notion_cancelled_at, 'notion_cancelled_at が立つ');
  ok(after.notion_cancelled_prev_status === '作業中', '取消時の元ステータスを記録');
  ok(after.notion_synced_at !== null, 'synced_at は上書きされない');
  ok(mock.pageStates.get(row.notion_page_id).status === '取消', 'Notion 側は「取消」になる');
  const st = notionStatusForAdmin();
  ok(st.attention.some(a => a.id === d1), '着手後の取消は要確認一覧に出る');
}

console.log('\n[7] 回収 (API成功/DB記録失敗への備え)');
{
  const d4 = seedDest({ name: '商品A回収' });
  mock.queryResults.set(d4, { id: 'page-existing', object: 'page' });
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.recovered === 1 && mock.created.length === before, '既存カードを回収して新規作成しない');
  ok(destRow(d4).notion_page_id === 'page-existing', '回収したカードの page_id が記録される');
}

console.log('\n[8] 4xx はブロック → 再送で解除');
let d5;
{
  d5 = seedDest({ name: '商品A失敗' });
  mock.failCreate = { status: 400 };
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok === false && r.errors === 1 && r.blocked === 1, `4xx は失敗+ブロック (errors=${r.errors})`);
  const row = destRow(d5);
  ok(String(row.notion_next_retry_at).startsWith('9999-'), '自動では再試行しない (人が再送するまで)');
  ok(collectUnsent(db).every(x => x.id !== d5), '対象から外れる');
  resetNotionRow(d5);
  ok(collectUnsent(db).some(x => x.id === d5), '再送で対象に戻る');
  const r2 = await runNotionSweep({ actor: 'test' });
  ok(r2.ok && r2.sent === 1 && !!destRow(d5).notion_page_id, '再送で作成される');
}

console.log('\n[9] アーカイブ済みカードの取消は収束扱い');
{
  const row = destRow(d5);
  mock.pageStates.set(row.notion_page_id, { archived: true, status: '未着手' });
  cancelDest(d5, 'planned_changed');
  const before = mock.patchedPages.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.cancelled === 1, 'アーカイブ済みでも収束する');
  ok(mock.patchedPages.length === before, 'ステータス変更の PATCH は打たない');
  ok(String(destRow(d5).notion_cancelled_prev_status).includes('アーカイブ済み'), '状態にアーカイブ済みと記録');
}

console.log('\n[10] 冪等: もう一度回しても何もしない');
{
  const before = { created: mock.created.length, patched: mock.patchedPages.length };
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.sent === 0 && r.cancelled === 0 && r.recovered === 0, '送信も取消反映も 0');
  ok(mock.created.length === before.created && mock.patchedPages.length === before.patched, 'API も叩かない');
}

console.log('\n[calcExternal 単体]');
{
  ok(calcExternal(null, null).externalOk === null, '販売も在庫も無ければ計算しない');
  ok(calcExternal(null, 50).externalOk === 50, '販売0扱いならフリー在庫がそのまま外部出しOK');
  ok(calcExternal(30, 10).externalOk === 0, 'キープ数を割ると 0 (外部施設NG)');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail === 0 ? 0 : 1;
