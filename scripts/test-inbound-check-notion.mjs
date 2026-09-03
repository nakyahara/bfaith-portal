/**
 * 入荷受付チェック — Notion 作業カード sweep (notion-sync.js) のテスト
 *
 * 実行: node scripts/test-inbound-check-notion.mjs
 * Notion API は global.fetch をモックして再現する (実 API は叩かない)。
 *
 * 検証項目 (CodexコードレビューR1 の High/Medium シナリオを含む):
 *   1. env 未設定なら not_configured (fail-closed)
 *   2. 送信対象の抽出: いろは行きのみ
 *   3. カード作成: プロパティ組み立て / 台帳キーを**作成前に**DB保存
 *   4. スキーマ ensure: 台帳キー・destination_id・有効期限 を足す
 *   5. 送信前に取り消された行はカードを作らず「カード未作成」で終端
 *   6. 送信後の取消 → ステータス「取消」+ 元ステータス記録 + 要確認一覧
 *   7. 回収: 台帳キーで既存カードが見つかれば新規作成しない
 *   8. 4xx (429/409以外) はブロック→「再送」で解除。409/一時エラーは30分後に再試行
 *   9. アーカイブ済みカードの取消は収束扱い
 *  10. [R1 High1] 作成成功→記録前に停止→取消 の孤立カードを台帳キーで回収して「取消」へ
 *  11. [R1 High3] 作成 await 中に取り消された行は page を紐付けて同じ sweep 内で「取消」へ
 *  12. retry モード (30分巡回) は新規行を送らない / 失敗行だけ再試行する
 *  13. [R1 Med9] プロパティ型の不一致は行に書かず sweep 全体を1回で失敗させる
 *  14. 冪等: もう一度回しても何もしない
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
    '名前': { type: 'title' }, 'ステータス': { type: 'select' }, '商品コード': { type: 'rich_text' },
    '数量': { type: 'number' }, '入庫日': { type: 'date' }, '入荷管理番号': { type: 'rich_text' },
    'バーコード': { type: 'rich_text' }, '取引先': { type: 'select' }, '仕入先': { type: 'number' },
    '取扱区分': { type: 'select' }, '在庫化必要FLG': { type: 'checkbox' }, '作業拠点': { type: 'select' },
    '過去30日販売数': { type: 'rich_text' }, '外部出し目安': { type: 'rich_text' }, '入数': { type: 'number' },
    // 実DBと同じ型 (2026-09-02 実機で判明: この2つは select。rich_text 想定だと schema_mismatch で全停止した)
    '資材セットID': { type: 'select' }, '収納容器': { type: 'select' }, '備考': { type: 'rich_text' },
    // 台帳キー / destination_id / 有効期限 は最初は無い → ensure が足す
  },
  queryResults: new Map(),   // `${property}:${value}` → page
  pageStates: new Map(),     // pageId → { archived, status }
  failCreate: null,          // { status, times } POST /pages を times 回失敗させる
  onCreate: null,            // POST /pages の直前に呼ぶフック (レース再現用)
  failPatch: null,           // { id, status, times } 特定ページの PATCH を失敗させる
  missingPages: new Set(),   // GET /pages/:id を 404 にする (削除済みページの再現)
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
    return respond(200, { object: 'database', properties: JSON.parse(JSON.stringify(mock.dbProps)) });
  }
  if (u.endsWith('/databases/testdb') && method === 'PATCH') {
    mock.patchedDb.push(body);
    for (const [k, cfg] of Object.entries(body.properties || {})) mock.dbProps[k] = { type: Object.keys(cfg)[0] };
    return respond(200, { object: 'database' });
  }
  if (u.endsWith('/databases/testdb/query') && method === 'POST') {
    const f = body?.filter || {};
    const value = f.rich_text?.equals ?? f.number?.equals;
    const hit = mock.queryResults.get(`${f.property}:${value}`);
    const hits = hit ? (Array.isArray(hit) ? hit : [hit]) : [];
    // わざと2件ずつ返してページネーションを踏ませる (has_more を辿らない実装だと4枚目以降を見落とす)
    const start = Number(body.start_cursor || 0);
    const pageItems = hits.slice(start, start + 2);
    const hasMore = start + 2 < hits.length;
    return respond(200, { object: 'list', results: pageItems, has_more: hasMore, next_cursor: hasMore ? String(start + 2) : null });
  }
  if (u.endsWith('/pages') && method === 'POST') {
    if (mock.onCreate) { const h = mock.onCreate; mock.onCreate = null; h(); }
    if (mock.failCreate) {
      // times 回だけ失敗させる (5xx はクライアントが内部で3回リトライするため)
      const f = mock.failCreate;
      f.times = (f.times ?? 1) - 1;
      if (f.times <= 0) mock.failCreate = null;
      return respond(f.status, { object: 'error', message: 'mock fail ' + f.status, code: 'mock' });
    }
    const id = 'page-' + (++pageSeq);
    mock.created.push({ id, props: body.properties });
    mock.pageStates.set(id, { archived: false, status: body.properties?.['ステータス']?.select?.name || null });
    return respond(200, { object: 'page', id, url: 'https://notion.so/' + id });
  }
  const mPage = u.match(/\/pages\/([^/?]+)$/);
  if (mPage && method === 'GET') {
    if (mock.missingPages.has(mPage[1])) return respond(404, { object: 'error', message: 'Could not find page', code: 'object_not_found' });
    const st = mock.pageStates.get(mPage[1]) || { archived: false, status: null };
    return respond(200, { object: 'page', id: mPage[1], archived: st.archived, properties: { 'ステータス': { select: st.status ? { name: st.status } : null } } });
  }
  if (mPage && method === 'PATCH') {
    if (mock.failPatch && mock.failPatch.id === mPage[1]) {
      const f = mock.failPatch;
      f.times = (f.times ?? 1) - 1;
      if (f.times <= 0) mock.failPatch = null;
      return respond(f.status, { object: 'error', message: 'mock patch fail ' + f.status, code: 'mock' });
    }
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
const notionMod = await import('../apps/inbound-check/notion.js');
const {
  runNotionSweep, collectUnsent, collectCancelPending, calcExternal, resetNotionRow, notionStatusForAdmin,
  buildCardProperties,
} = await import('../apps/inbound-check/notion-sync.js');

console.log('DATA_DIR =', process.env.DATA_DIR);

console.log('\n[1] env 未設定なら not_configured');
{
  delete process.env.NOTION_TOKEN;
  delete process.env.INBOUND_CHECK_NOTION_DB_ID;
  const r = await runNotionSweep({ actor: 'cron-retry', mode: 'retry' });
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
// ⚠自前の CREATE で列名を想像しない — 本物の init でテーブルを作る (supplier_name と誤モックして
//   本番の no such column を素通りさせた 2026-09-02 の教訓)
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
db.prepare("INSERT INTO po_suppliers (supplier_code, name, created_at, updated_at) VALUES ('1', 'AMC', ?, ?)")
  .run(new Date().toISOString(), new Date().toISOString());
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
seedDest({ product: 'PROD-B', code: 'prod-b', line: 'L2', dest: 'bfaith', name: '商品B' });
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
  ok(!!row.notion_dedupe_key, '台帳キー (dedupe key) が DB に保存される');
  const p = mock.created[0].props;
  ok(p['台帳キー'].rich_text[0].text.content === row.notion_dedupe_key, 'カードの台帳キー = DB に先に保存した値');
  ok(p['名前'].title[0].text.content === '商品A', '名前 = 商品名');
  ok(p['ステータス'].select.name === '未着手', 'ステータス = 未着手');
  ok(p['数量'].number === 8, '数量 = 実際に数えた数 (actual_qty)');
  ok(p['入庫日'].date.start === '2026-09-02', '入庫日 = 荷受け日 (work_date)');
  ok(p['destination_id'].number === d1, 'destination_id = 台帳の行ID (表示用)');
  ok(p['バーコード'].rich_text[0].text.content === '4501234567890', 'バーコード = 取込行から');
  ok(p['取引先'].select.name === 'AMC', '取引先 = 仕入先マスタ (po_suppliers) の名称');
  ok(p['仕入先'].number === 1, '仕入先 = 仕入先コード (数値)');
  ok(p['取扱区分'].select.name === '取扱中', '取扱区分 = mirror_products');
  ok(!('在庫化必要FLG' in p), '在庫化必要FLG は送らない (FLGは廃止 — 2026-09-02)');
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
  ok(added.includes('台帳キー') && added.includes('destination_id') && added.includes('有効期限'),
    '台帳キー・destination_id・有効期限 を足した');
}

console.log('\n[5] 送信前に取り消された行は「カード未作成」で終端');
{
  const d3 = seedDest({ product: 'PROD-C', code: 'prod-c', line: 'L3', name: '商品C' });
  cancelDest(d3);
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && mock.created.length === before, '取消済みは作成されない');
  const row = destRow(d3);
  ok(!row.notion_page_id && !!row.notion_cancelled_at && row.notion_cancelled_prev_status === '(カード未作成)',
    '「カード未作成」で終端し、以後スキャンされない');
  ok(!notionStatusForAdmin().attention.some(a => a.id === d3), 'カード未作成は要確認一覧に出ない');
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
  ok(after.notion_synced_at === row.notion_synced_at, 'synced_at は上書きされない');
  ok(mock.pageStates.get(row.notion_page_id).status === '取消', 'Notion 側は「取消」になる');
  ok(notionStatusForAdmin().attention.some(a => a.id === d1), '着手後の取消は要確認一覧に出る');
}

console.log('\n[7] 回収 (作成成功→page_id 未保存 のやり直し)');
{
  const d4 = seedDest({ name: '商品A回収' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd999-fixed' WHERE id = ?").run(d4);
  mock.queryResults.set('台帳キー:d999-fixed', { id: 'page-existing', object: 'page' });
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.recovered === 1 && mock.created.length === before, '既存カードを台帳キーで回収して新規作成しない');
  ok(destRow(d4).notion_page_id === 'page-existing', '回収したカードの page_id が記録される');
}

console.log('\n[8] エラー分類: 4xx はブロック / 409・5xx は一時');
let d5;
{
  d5 = seedDest({ name: '商品A失敗' });
  mock.failCreate = { status: 422 };
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok === false && r.errors === 1 && r.blocked === 1, `422 は失敗+ブロック (errors=${r.errors})`);
  ok(String(destRow(d5).notion_next_retry_at).startsWith('9999-'), '自動では再試行しない (人が再送するまで)');
  ok(collectUnsent(db).every(x => x.id !== d5), '対象から外れる');
  ok(notionStatusForAdmin().blocked.some(b => b.id === d5), '管理画面の「エラーで止まっている」に出る');
  resetNotionRow(d5);
  ok(collectUnsent(db).some(x => x.id === d5), '再送で対象に戻る');
  mock.failCreate = { status: 409, times: 3 };   // 作成は失敗のたび再検索→再POSTするので3回分
  const r2 = await runNotionSweep({ actor: 'test' });
  const row = destRow(d5);
  ok(r2.errors === 1 && !String(row.notion_next_retry_at).startsWith('9999-'), '409 は一時エラー (30分後に再試行)');
  ok(notionStatusForAdmin().waitingRetry >= 1, '管理画面では「再試行待ち」として数える (ブロックとは別)');
  resetNotionRow(d5);
  const r3 = await runNotionSweep({ actor: 'test' });
  ok(r3.ok && r3.sent === 1 && !!destRow(d5).notion_page_id, '再送で作成される');
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

console.log('\n[10] [R1 High1] 孤立カード (作成成功→記録前に停止→取消) の回収');
{
  const d6 = seedDest({ name: '商品A孤立' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd777-orphan' WHERE id = ?").run(d6);
  mock.pageStates.set('page-orphan', { archived: false, status: '未着手' });
  mock.queryResults.set('台帳キー:d777-orphan', { id: 'page-orphan', object: 'page' });
  cancelDest(d6, 'reopen');   // DB 上は page_id なしのまま取消
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.cancelled >= 1, '孤立カードを回収して収束');
  const row = destRow(d6);
  ok(row.notion_page_id === 'page-orphan', '台帳キーで見つけた page を紐付ける');
  ok(mock.pageStates.get('page-orphan').status === '取消', '孤立カードも「取消」になる');
  ok(!!row.notion_cancelled_at, '取消反映済みとして終端');
}

console.log('\n[11] [R1 High3] 作成 await 中の取消は同じ sweep で「取消」へ');
{
  const d7 = seedDest({ name: '商品Aレース' });
  mock.onCreate = () => cancelDest(d7, 'reopen');   // createCard の最中に現場が取り消した想定
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.raced === 1, `競合として検出 (raced=${r.raced})`);
  const row = destRow(d7);
  ok(!!row.notion_page_id, '作ってしまったカードは紐付けて放置しない');
  ok(!!row.notion_cancelled_at && mock.pageStates.get(row.notion_page_id).status === '取消',
    '同じ sweep の後段でカードが「取消」になる');
}

console.log('\n[12] retry モード (30分巡回) は新規行を送らない');
{
  const d8 = seedDest({ name: '商品A新規' });
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'cron-retry', mode: 'retry' });
  ok(r.ok && mock.created.length === before && !destRow(d8).notion_page_id, '未失敗の新規行は 17:30 まで待つ');
  // 一時エラー (500×3回で諦め) を作って retry モードで拾えることを確認
  mock.failCreate = { status: 500, times: 3 };
  await runNotionSweep({ actor: 'test' });   // 500 → 一時エラー (next_retry 30分後)
  ok(!destRow(d8).notion_page_id && !!destRow(d8).notion_error, '一時エラーとして記録');
  db.prepare("UPDATE f_inbound_check_destinations SET notion_next_retry_at = '2000-01-01T00:00:00Z' WHERE id = ?").run(d8);
  const r2 = await runNotionSweep({ actor: 'cron-retry', mode: 'retry' });
  ok(r2.ok && r2.sent === 1 && !!destRow(d8).notion_page_id, '失敗済みの行は retry モードが再試行する');
}

console.log('\n[13] [R1 Med9] プロパティ型の不一致は sweep 全体を1回で失敗させる');
{
  const d9 = seedDest({ name: '商品A型違い' });
  notionMod._clearSchemaCache();
  mock.dbProps['destination_id'] = { type: 'rich_text' };   // 人が型を作り替えた想定
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok === false && r.error === 'schema_mismatch', 'schema_mismatch で止まる');
  const row = destRow(d9);
  ok(!row.notion_error && (row.notion_attempt_count == null || row.notion_attempt_count === 0),
    '行には個別エラーを書かない (300行に書き散らさない)');
  mock.dbProps['destination_id'] = { type: 'number' };
  notionMod._clearSchemaCache();
  const r2 = await runNotionSweep({ actor: 'test' });
  ok(r2.ok && r2.sent === 1, '型を直せばそのまま送れる');
}

console.log('\n[14] [R2 High1] 応答喪失 (作成成功したが失敗に見えた) は再検索で回収');
{
  const dA = seedDest({ name: '商品A応答喪失' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd555-lost' WHERE id = ?").run(dA);
  mock.onCreate = () => {
    mock.pageStates.set('page-lost', { archived: false, status: '未着手' });
    mock.queryResults.set('台帳キー:d555-lost', { id: 'page-lost', object: 'page' });
  };
  mock.failCreate = { status: 500, times: 1 };   // 1回目: Notion側では作成成功・応答だけ失敗
  const before = mock.created.length;
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.recovered === 1 && r.errors === 0, '失敗後の再検索で回収 (2枚目を作らない)');
  ok(mock.created.length === before, '再POSTしない');
  ok(destRow(dA).notion_page_id === 'page-lost', '回収した page が記録される');
}

console.log('\n[15] [R2 High1] 同じ台帳キーのカードが複数 → 要対応で止める');
{
  const dB = seedDest({ name: '商品A二重' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd333-dup' WHERE id = ?").run(dB);
  mock.pageStates.set('pgA', { archived: false, status: '未着手' });
  mock.pageStates.set('pgB', { archived: false, status: '未着手' });
  mock.queryResults.set('台帳キー:d333-dup', [{ id: 'pgA' }, { id: 'pgB' }]);
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.errors === 1 && r.blocked === 1, '二重は失敗+要対応として数える');
  const row = destRow(dB);
  ok(row.notion_page_id === 'pgA', '1枚目は記録する');
  ok(String(row.notion_error).includes('2 枚'), 'エラーに枚数が出る');
  ok(String(row.notion_next_retry_at).startsWith('9999-'), '人が整理するまで自動では触らない');
  ok(notionStatusForAdmin().blocked.some(b => b.id === dB), '管理画面のエラー一覧に出る');
  // 送信側の永久ブロック (9999) 中でも「取消」は通る (R3 High: 送信と取消の制御列を分離)
  cancelDest(dB, 'reopen');
  const r2 = await runNotionSweep({ actor: 'test' });
  ok(r2.cancelled >= 1 && !!destRow(dB).notion_cancelled_at, '送信ブロック中でも取消は反映される');
  ok(mock.pageStates.get('pgA').status === '取消', 'カード (pgA) は取消になる');
}

console.log('\n[16] [R2 High2] 他プロセスが lease を持っていたら実行しない');
{
  db.prepare(`INSERT INTO f_inbound_check_notion_lease (id, holder, expires_at) VALUES (1, 'other-process', ?)
    ON CONFLICT(id) DO UPDATE SET holder = excluded.holder, expires_at = excluded.expires_at`)
    .run(new Date(Date.now() + 5 * 60 * 1000).toISOString());
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok === false && r.error === 'already_running', '有効な他者 lease 中は already_running');
  db.prepare('DELETE FROM f_inbound_check_notion_lease').run();
}

console.log('\n[17] [R3 Medium] 孤立検索の空振りは3回まで再試行してから終端');
{
  const dC = seedDest({ name: '商品A検索遅延' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd888-lag' WHERE id = ?").run(dC);
  cancelDest(dC, 'reopen');
  const clearGate = () => db.prepare('UPDATE f_inbound_check_destinations SET notion_cancel_next_retry_at = NULL WHERE id = ?').run(dC);
  await runNotionSweep({ actor: 'test' });
  let row = destRow(dC);
  ok(!row.notion_cancelled_at && row.notion_cancel_attempt_count === 1, '1回目の空振りでは終端しない (検索遅延に備える)');
  clearGate(); await runNotionSweep({ actor: 'test' });
  clearGate(); await runNotionSweep({ actor: 'test' });
  row = destRow(dC);
  ok(!!row.notion_cancelled_at && row.notion_cancelled_prev_status === '(カード未作成)', '3回空振りで「カード未作成」終端');
}

console.log('\n[18] [R3 High] 孤立回収: 余分カードの取消に失敗したら終端しない');
{
  const dD = seedDest({ name: '商品A多重孤立' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd444-multi' WHERE id = ?").run(dD);
  mock.pageStates.set('pgC', { archived: false, status: '未着手' });
  mock.pageStates.set('pgD', { archived: false, status: '未着手' });
  mock.queryResults.set('台帳キー:d444-multi', [{ id: 'pgC' }, { id: 'pgD' }]);
  mock.failPatch = { id: 'pgD', status: 500, times: 3 };
  cancelDest(dD, 'reopen');
  const r = await runNotionSweep({ actor: 'test' });
  let row = destRow(dD);
  ok(r.errors >= 1 && !row.notion_cancelled_at && !!row.notion_cancel_error, '失敗を握り潰さず未終端のまま残す');
  resetNotionRow(dD);
  const r2 = await runNotionSweep({ actor: 'test' });
  row = destRow(dD);
  ok(r2.ok && !!row.notion_cancelled_at, '次の sweep で続きからやり直して終端');
  ok(mock.pageStates.get('pgC').status === '取消' && mock.pageStates.get('pgD').status === '取消', '2枚とも「取消」になる');
}

console.log('\n[18b] [R4 High] 二重カードが4枚以上でもページネーションで全部「取消」に倒す');
{
  const dF = seedDest({ name: '商品A四重孤立' });
  db.prepare("UPDATE f_inbound_check_destinations SET notion_dedupe_key = 'd999-four' WHERE id = ?").run(dF);
  for (const p of ['pgE', 'pgF', 'pgG', 'pgH']) mock.pageStates.set(p, { archived: false, status: '未着手' });
  mock.queryResults.set('台帳キー:d999-four', [{ id: 'pgE' }, { id: 'pgF' }, { id: 'pgG' }, { id: 'pgH' }]);
  cancelDest(dF, 'reopen');
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.cancelled >= 1 && !!destRow(dF).notion_cancelled_at, '4枚見つけて終端');
  ok(['pgE', 'pgF', 'pgG', 'pgH'].every(p => mock.pageStates.get(p).status === '取消'),
    '4枚全部が「取消」になる (page_size 固定だと3枚で止まる)');
}

console.log('\n[19] 取消対象のカードが削除済み (404) なら「カード消失」で収束');
{
  const dE = seedDest({ name: '商品A消失' });
  const rSend = await runNotionSweep({ actor: 'test' });
  ok(rSend.ok && !!destRow(dE).notion_page_id, '先に普通に送る');
  mock.missingPages.add(destRow(dE).notion_page_id);
  cancelDest(dE, 'reopen');
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && destRow(dE).notion_cancelled_prev_status === '(カード消失)', '404 は「カード消失」で終端 (エラーにしない)');
}

console.log('\n[20] 冪等: もう一度回しても何もしない');
{
  const before = { created: mock.created.length, patched: mock.patchedPages.length };
  const r = await runNotionSweep({ actor: 'test' });
  ok(r.ok && r.sent === 0 && r.cancelled === 0 && r.recovered === 0, '送信も取消反映も 0');
  ok(mock.created.length === before.created && mock.patchedPages.length === before.patched, 'API も叩かない');
}

console.log('\n[calcExternal 単体 + 新商品ラベル]');
{
  ok(calcExternal(null, null).externalOk === null, '販売も在庫も無ければ計算しない');
  // 販売も在庫実績も無い = 新商品 → 外部出し目安に「新商品」(中原さん 2026-09-02)
  const names = new Set(['名前', '外部出し目安', '過去30日販売数', '台帳キー', 'destination_id']);
  const row0 = { id: 99, product_id: 'NEW-1', product_name: '新商品テスト', actual_qty: 5, planned_qty: 5, ar_no: 'AR9', work_date: '2026-09-02', code_key: 'new-1' };
  const pNew = buildCardProperties(row0, { barcode: null, product: null, supplierName: null, ext: calcExternal(null, null), dedupeKey: 'd99-x', wm: null }, names);
  ok(pNew['外部出し目安'].rich_text[0].text.content === '新商品', '実績ゼロの初入荷は「新商品」');
  const pStock = buildCardProperties(row0, { barcode: null, product: null, supplierName: null, ext: calcExternal(null, 50), dedupeKey: 'd99-x', wm: null }, names);
  ok(pStock['外部出し目安'].rich_text[0].text.content === '外部施設に50個まで預けてOK', '在庫実績がある既存品は従来どおり数字');
  ok(calcExternal(null, 50).externalOk === 50, '販売0扱いならフリー在庫がそのまま外部出しOK');
  ok(calcExternal(30, 10).externalOk === 0, 'キープ数を割ると 0 (外部施設NG)');
}

console.log('\n[PR-B] 確定時タスクとの紐付け / アプリ正本なら Notion へ送らない');
{
  const IW = await import('../apps/iroha-work/db.js');
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const { createTaskForDestination } = await import('../apps/iroha-work/task-intake.js');
  const dId = seedDest({ product: 'PROD-A', line: 'L1', qty: 10, actual: 9 });
  const made = createTaskForDestination(destRow(dId), { actor: 'テスト' });
  ok(made.action === 'inserted' && TD.getTaskByDestination(dId).notion_page_id == null, '前提: 確定時のタスク (Notion ページなし)');
  const r = await runNotionSweep({ actor: 'manual', mode: 'full' });
  const d = destRow(dId);
  ok(r.ok && d.notion_page_id && TD.getTaskByDestination(dId).notion_page_id === d.notion_page_id, 'カードを作ったらタスクに notion_page_id が付く');
  // アプリ正本: 何もしない (Notion を 1 回も呼ばない・台帳も触らない)
  const dId2 = seedDest({ product: 'PROD-B', line: 'L2', qty: 5, actual: 5 });
  createTaskForDestination(destRow(dId2));
  IW.setMetaValue('source_of_truth', 'app');
  const origFetch = global.fetch;
  let calls = 0;
  global.fetch = async (...a) => { calls++; return origFetch(...a); };
  let r2;
  try { r2 = await runNotionSweep({ actor: 'cron', mode: 'full' }); } finally { global.fetch = origFetch; }
  ok(r2.ok && r2.skipped === 'app_mode' && calls === 0, 'アプリ正本の間は Notion を 1 回も呼ばない (ok・skipped=app_mode)');
  ok(destRow(dId2).notion_page_id == null && collectUnsent(db).some(x => x.id === dId2), '未送信のまま (行き先台帳は触らない)');
  ok(notionStatusForAdmin().source === 'app', '管理画面の状態に正本が出る');
  IW.setMetaValue('source_of_truth', null);
  const r3 = await runNotionSweep({ actor: 'manual', mode: 'full' });
  ok(r3.ok && destRow(dId2).notion_page_id && TD.getTaskByDestination(dId2).notion_page_id === destRow(dId2).notion_page_id, 'Notion 正本に戻せば送る (紐付けも付く)');

  // (a) 台帳に記録した後・紐付けの前に落ちた状態 → 次の sweep の先頭で補修 (Codex PR-B R1 #2)
  const { linkTaskToNotionPage, backfillTaskLinks, countLinkConflicts } = await import('../apps/iroha-work/task-intake.js');
  db.prepare('UPDATE f_iroha_tasks SET notion_page_id = NULL WHERE destination_id = ?').run(dId2);
  ok(TD.getTaskByDestination(dId2).notion_page_id == null, '前提: タスク側の紐付けだけ無い');
  const r4 = await runNotionSweep({ actor: 'manual', mode: 'full' });
  ok(r4.ok && r4.linked === 1 && TD.getTaskByDestination(dId2).notion_page_id === destRow(dId2).notion_page_id, '次の sweep が紐付けを補修する (linked=1)');
  // 同じページを別タスクが持っていたら黙らない (履歴に task_link_conflict)
  const dId3 = seedDest({ product: 'PROD-C', line: 'L3', qty: 7, actual: 7 });
  createTaskForDestination(destRow(dId3));
  const res = linkTaskToNotionPage(dId3, destRow(dId2).notion_page_id);
  ok(res === 'conflict' && countLinkConflicts() === 1 && TD.getTaskByDestination(dId3).notion_page_id == null, '別タスクが持つページは conflict として履歴に残す (上書きしない)');
  ok(backfillTaskLinks().conflicts === 0, '衝突は台帳側のページが無い限り再発しない');

  // (b) sweep の途中で正本がアプリに切り替わったら、それ以降のカードは作らない (Codex PR-B R1 #3)
  const dId4 = seedDest({ product: 'PROD-A', line: 'L1', qty: 10, actual: 10 });
  createTaskForDestination(destRow(dId4));
  createTaskForDestination(destRow(dId3));
  const unsentBefore = collectUnsent(db).map(x => x.id);
  ok(unsentBefore.includes(dId3) && unsentBefore.includes(dId4), '前提: 未送信 2 件');
  const createdBefore = mock.created.length;
  const orig2 = global.fetch;
  global.fetch = async (url, opts = {}) => {
    const resp = await orig2(url, opts);
    if (String(url).endsWith('/pages') && (opts.method || 'GET') === 'POST') IW.setMetaValue('source_of_truth', 'app');   // 1 枚目を作った直後に切替
    return resp;
  };
  let r5;
  try { r5 = await runNotionSweep({ actor: 'manual', mode: 'full' }); } finally { global.fetch = orig2; IW.setMetaValue('source_of_truth', null); }
  ok(r5.ok && r5.skipped === 'app_mode' && r5.aborted === true, '途中で切り替わったら打ち切り (ok・aborted)');
  ok(mock.created.length === createdBefore + 1, 'カードは 1 枚だけ作られた (2 枚目は作らない)');
  const sentOne = [dId3, dId4].filter(id => destRow(id).notion_page_id);
  ok(sentOne.length === 1 && TD.getTaskByDestination(sentOne[0]).notion_page_id === destRow(sentOne[0]).notion_page_id, '作った 1 枚は台帳とタスクに記録済み');
  ok(collectUnsent(db).length === 1 && destRow(collectUnsent(db)[0].id).notion_error == null, '残り 1 件は未送信のまま (エラー扱いにしない)');
  const { notionSweepRunning } = await import('../apps/inbound-check/notion-sync.js');
  ok(notionSweepRunning() === false, '終わったら実行中フラグは下りる');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail === 0 ? 0 : 1;
