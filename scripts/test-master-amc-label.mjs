/**
 * test-master-amc-label.mjs — 商品の画面の「AMC 商品ラベルのバーコード印字」(Company DB構想 21 A1・apps/master-edit/amc-label.mjs → apps/purchase-orders/barcode-labels.js)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration・夜間ロードで商品・出品・FNSKU・JAN を入れる・画面は master_edit のロール)。
 * 発注アプリの SQLite = 一時の DATA_DIR の warehouse-mirror.db (本番の DB に触らない)。本物の router (master-edit・purchase-orders) を HTTP 越しに通す。
 * 固定する契約:
 *   V 画面: 対象 (仕入先 AMC × 売上分類 1 × 単品・商品管理リストにある) だけ欄を出す・対象外は理由だけ (AMC でない・自社商品でない・リストにまだ無い・セット)・
 *     候補 = この商品 1 個だけの Amazon の出品の FNSKU (マーケット・出品 SKU つき) と この商品の JAN・まとめ売り / 組んだ出品の FNSKU は「選べない」・
 *     今の値が候補に無い = 「今の値のまま」・発注アプリの権限が無い = 値を出さない・名簿でない = 見るだけ (保存のボタンと JS なし)・入荷アプリと食い違う = 印
 *   W 保存: 候補を選ぶ = Company DB の値で書く (画面の値ではなく)・events と po_audit_log (だれ・via master-edit:sku・前と後・選んだ候補)・
 *     発注アプリの一覧 (GET /api/barcode-labels) に同じ値
 *   R 保存のときの確かめ直し (Codex R3 M5): 選んだ FNSKU が外された・出品がまとめ売りになった・値が違う・ほかの商品の JAN の行 = 409 (何も書かない)
 *   M 手で打つ: 理由が要る (400)・ほかの商品の JAN / FNSKU の値 = 409・通れば記録に理由・この商品の候補の値でも手入力の記録
 *   K 今の値のまま / 値なし: 状態とメモだけ直せる・版が違う = 409 (発注アプリのポップオーバーが先に直した = 上書きしない)
 *   K2 今の値のまま でも、発注アプリの今の値が Company DB でほかの商品の JAN / FNSKU なら 409 (Codex #1701 R1 H1)
 *   H2 書いた後に Company DB を読み直し、確かめた時から変わっていたら書き込みを戻す (要確認・409 recheck_failed・記録) (R1 H2)
 *   M 手で打つ形: FNSKU / JAN-8・13 (チェック数字) だけ・ほかの形は「例外の形」(manual_other) を明示 (R1 M1)・候補の値はいつも比べる (R1 L1)
 *   UI5 未登録 (行が無い) = 何も変えずに保存できる = 「未設定・値なし」の行ができる (Codex #1701 R3 M)
 *   P2 1 商品だけの読み方 (索引・NE オーバーレイ・重なり) = 発注アプリの一覧の決め方と同じ (R1 M2)
 *   G 門: 発注アプリの権限なし・名簿でない = 403・Origin・セット = 400・無い商品 = 404・対象外 = 409 not_target・本文の形 = 400
 *   U つかいかた (同じ PR)・画面の JS が読める・me-amclabel.js は me-sku.js の後
 * 使い方: node scripts/test-master-amc-label.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (360 秒)'); process.exit(3); }, 360e3);
killer.unref();

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'master-amc-label-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
(await import('../lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

// ── Company DB (PGlite・持ち主のロール deploy) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg);
const q = async (sql, params) => (await db.query(sql, params)).rows;

const AMZ = 'main@A1VC38T7YXB528';
const J1 = '4901234567894';   // amc1 の JAN
const J9 = '4900000000009';   // oth1 の JAN (ほかの商品・チェック数字も合う)
function makePlan() {
  const sku = (code, name, kind, salesClass) => ({ code, name, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass,
    cost: { jpy: 100, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
  const comp = (code, qty = 1) => ({ code, qty, resolution: 'exact', evidence: { source: 'test' } });
  const amazon = (listingCode, comps, fnsku) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title: listingCode, status: 'active',
    components: comps, asinCandidates: [], fnskuCandidates: fnsku ? [{ fnsku, source: 'fba_sku_attrs' }] : [], evidenceSource: 'test' });
  const jan = (skuCode, v) => ({ skuCode, attribute: 'jan', scope: 'item', valueText: v, source: 'product_hub', sourceRef: `test:${skuCode}`, observedAt: '2030-01-01T00:00:00.000Z' });
  return {
    skus: [
      sku('amc1', 'AMC の自社商品 1', 'single', 1), sku('amc2', 'AMC の自社商品 2', 'single', 1), sku('amc3', 'AMC の自社商品 3 (今の値が候補に無い)', 'single', 1),
      sku('amc4', 'AMC の自社商品 4 (手で打つ)', 'single', 1), sku('amc-c3', 'AMC の仕入商品 (売上分類 3)', 'single', 3), sku('oth1', 'ほかの仕入先の商品', 'single', 1),
      sku('fresh1', '商品管理リストにまだ無い', 'single', 1), sku('amc5', 'AMC の自社商品 5 (書いた後の読み直し)', 'single', 1), sku('amc6', 'AMC の自社商品 6 (未登録のまま保存)', 'single', 1), sku('set1', 'セット', 'set', null),
    ],
    variationGroups: [],
    setComponents: [{ parentCode: 'set1', childCode: 'amc1', qty: 2, source: 'ne' }],
    listings: [
      amazon('pr_amc1', [comp('amc1')], 'X00AMC0001'),
      amazon('pr_amc1_2p', [comp('amc1', 2)], 'X00AMC0002'),
      amazon('pr_mix', [comp('amc1'), comp('oth1')], 'X00MIX0001'),
      amazon('pr_amc2', [comp('amc2')], 'X00AMC0003'),
      amazon('pr_oth1', [comp('oth1')], 'X00OTH0001'),
      amazon('pr_amc5', [comp('amc5')], 'X00AMC0005'),
    ],
    observations: [jan('amc1', J1), jan('oth1', J9)], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ほかの仕入先' }],
    supplierSkus: [],
    primarySuppliers: [...['amc1', 'amc2', 'amc3', 'amc4', 'amc5', 'amc6', 'amc-c3', 'fresh1'].map((s) => ({ skuCode: s, supplierCode: '0001' })), { skuCode: 'oth1', supplierCode: '0002' }],
    reorder: { available: true, runId: 'pml_test' },
  };
}
{
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_amc', ownership: MASTER_OWNERSHIP, now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r.ok, true, r.error);
}
const extRow = async (value) => (await q('select external_id_row::text as r from core.external_ids where external_value = $1 and valid_to is null', [value]))[0]?.r;
const listingId = async (code) => (await q("select listing_id::text as id from core.listings where mall = 'amazon' and listing_code = $1", [code]))[0]?.id;
assert.ok(await extRow('X00AMC0001') && await extRow(J1), '夜間ロードが FNSKU・JAN を入れていない');

// ── 発注アプリの SQLite (商品管理リスト・ラベルの表・入荷アプリ) ──
const POD = await import('../apps/purchase-orders/db.js');
const BL = await import('../apps/purchase-orders/barcode-labels.js');
const podb = POD.getDB();
{
  const T0 = new Date(Date.now() - 86400e3).toISOString();
  podb.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, synced_at) VALUES (1, 'run_t', 'ok', ?, ?)`).run(T0.slice(0, 10), T0);
  const ins = podb.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 商品区分, 売上分類) VALUES ('run_t', ?, ?, ?, '取扱中', ?, ?)`);
  for (const [c, sup, kind, cls] of [['amc1', '0001', '', 1], ['amc2', '0001', '', 1], ['amc3', '0001', '', 1], ['amc4', '0001', '', 1], ['amc5', '0001', '', 1], ['amc6', '0001', '', 1], ['amc-c3', '0001', '', 3], ['oth1', '0002', '', 1], ['set1', '0001', 'セット', 1],
    // 1 商品だけの読み方 (Codex #1701 R1 M2) の照らし合わせ用: NE オーバーレイで仕入先が変わる行・大文字小文字だけ違う重なり (Company DB には無い)
    ['amc-ov', '0002', '', 1], ['ov-off', '0001', '', 1], ['Dup1', '0002', '', 1], ['dup1', '0001', '', 1]]) ins.run(c, c, sup, kind, cls);
  podb.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES ('1', 'amc1', 'amc1', 'AMC-0001', ?)").run(T0);
  // amc3 = 前からの行 (旧 Excel の取込・候補に無い値)
  podb.prepare(`INSERT INTO po_barcode_labels (product_key, product_code, status, barcode_value, barcode_type, note, source, version, created_at, updated_at, updated_by)
    VALUES ('amc3', 'amc3', 'printed', 'X00OLD0003', 'fnsku', '旧 Excel', 'csv', 1, ?, ?, 'csv@test')`).run(T0, T0);
  podb.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 入庫時BCシール貼りフラグ, source, created_at, updated_at) VALUES ('amc3', 'amc3', 'BCシール貼付必要', 'manual', ?, ?)`).run(T0, T0);
  podb.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 入庫時BCシール貼りフラグ, source, created_at, updated_at) VALUES ('amc1', 'amc1', '不要', 'manual', ?, ?)`).run(T0, T0);
}
const labelOf = (k) => podb.prepare('SELECT * FROM po_barcode_labels WHERE product_key = ?').get(k) || null;
const eventsOf = (k) => podb.prepare('SELECT * FROM po_barcode_label_events WHERE product_key = ? ORDER BY id').all(k);
const auditOf = (k) => podb.prepare("SELECT actor, action, detail_json, request_id FROM po_audit_log WHERE resource = ? ORDER BY id").all(`barcode:${k}`).map((r) => ({ ...r, detail: JSON.parse(r.detail_json) }));
const counts = () => ({ labels: podb.prepare('SELECT COUNT(*) AS n FROM po_barcode_labels').get().n, events: podb.prepare('SELECT COUNT(*) AS n FROM po_barcode_label_events').get().n,
  audit: podb.prepare("SELECT COUNT(*) AS n FROM po_audit_log WHERE action = 'barcode_label_write'").get().n });

// ── 画面 (本物の router・セッションは x-test-session) ──
const MR = await import('../apps/master-edit/router.mjs');
const POR = (await import('../apps/purchase-orders/router.js')).default;
process.env.COMPANY_DB_URL = 'postgres://deploy@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test';
MR.__setPgClientFactory(async (url) => {
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
MR.__setClock(() => Date.now());
MR.__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
const SESS = {
  'po-editor': { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] },
  editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  'po-viewer': { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] },
};
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = SESS[req.headers['x-test-session']] || null; next(); });
app.use('/apps/master-edit', MR.default);
app.use('/apps/purchase-orders', express.json(), POR);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'po-editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual', signal: AbortSignal.timeout(30e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
async function scriptsOf(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);
  const files = [];
  for (const m of all.filter((x) => /\bsrc="\/apps\/master-edit\/public\//.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'po-editor' }, signal: AbortSignal.timeout(30e3) });
    assert.equal(r.status, 200, src);
    const t = await r.text();
    new vm.Script(t); assert.ok(!/<%|%>/.test(t), 'EJS のタグが JS に残っている');
    files.push({ src, text: t });
  }
  return files;
}
const amcData = (html) => JSON.parse((/<script type="application\/json" id="me-amc">([\s\S]*?)<\/script>/.exec(html) || [])[1] || 'null');
const section = (html) => (/<section class="panel" id="sec-amc"[\s\S]*?<\/section>/.exec(html) || [''])[0];
const url = (code) => `/apps/master-edit/api/sku/${code}/amc-label`;
const uuid = () => crypto.randomUUID();

console.log('AMC 商品ラベルのバーコード印字 (マスタの入力 → 発注アプリ)');

await ta('[V1] 対象の商品: 欄 (いま・状態・刷る値・メモ・入荷アプリ)・候補 = 1 個だけの出品の FNSKU (マーケット・出品 SKU) と JAN・まとめ売り / 組んだ出品は「選べない」・未登録・AMC 管理番号・JS は me-sku.js の後', async () => {
  const r = await call('GET', '/apps/master-edit/sku/amc1');
  assert.equal(r.status, 200, r.text.slice(0, 300));
  const s = section(r.text);
  for (const w of ['AMC 商品ラベルのバーコード印字', '発注アプリに保存されます', 'id="amc-status"', 'id="amc-cands"', 'id="amc-note"', 'id="amc-save" disabled>ラベルの印字を保存',
    '入庫で BC シール: <b id="amc-seal">不要</b>', '未登録', 'AMC-0001', 'Amazon 日本 · 出品 SKU pr_amc1 · 出品中', 'この商品の JAN (Company DB)', '選べない FNSKU (2)',
    'まとめ売り (2 個)', 'ほかの商品と組んだ出品', '手で打つ', 'JAN・FNSKU の登録']) assert.ok(s.includes(w), `欄に「${w}」が無い`);
  const radios = [...s.matchAll(/name="amc-pick" value="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(radios, ['c0', 'c1', 'manual', 'none'], '候補は FNSKU 1 つ・JAN 1 つ + 手で打つ + 値なし');
  assert.ok(!s.includes('value="keep"'), '値が無いのに「今の値のまま」');
  assert.match(s, /name="amc-pick" value="none" checked/);
  assert.ok(!s.includes('X00OTH0001'), 'ほかの商品の FNSKU を候補に出した');
  assert.match(s, /id="amc-mismatch" hidden>/, '食い違いの印は出さない (未登録・シール不要)');
  const d = amcData(r.text);
  assert.deepEqual([d.code, d.canWrite, d.version, d.status, d.pick, d.cands.map((c) => [c.kind, c.value])], ['amc1', true, null, 'unset', 'none', [['fnsku', 'X00AMC0001'], ['jan', J1]]]);
  assert.deepEqual([d.cands[0].row, d.cands[0].listing_id], [await extRow('X00AMC0001'), await listingId('pr_amc1')]);
  const files = await scriptsOf(r.text);
  const iSku = files.findIndex((f) => f.src.includes('me-sku.js')), iAmc = files.findIndex((f) => f.src.includes('me-amclabel.js'));
  assert.ok(iSku >= 0 && iAmc > iSku, 'me-amclabel.js は me-sku.js の後 (MasterEdit.dirty を包む)');
  assert.ok(files[iAmc].text.includes("'/amc-label'"));
});

await ta('[V2] 対象外は理由だけ (欄・値・JS なし): AMC でない・自社商品でない・商品管理リストにまだ無い・セット', async () => {
  for (const [code, word] of [['oth1', '仕入先が AMC'], ['amc-c3', '自社商品 (売上分類 1) ではありません'], ['fresh1', '商品管理リストにまだ出ていません'], ['set1', 'セットです']]) {
    const r = await call('GET', `/apps/master-edit/sku/${code}`);
    assert.equal(r.status, 200, `${code}: ${r.text.slice(0, 200)}`);
    const s = section(r.text);
    assert.ok(s.includes('対象外') && s.includes(word), `${code} の理由「${word}」が無い`);
    assert.ok(!s.includes('id="amc-cands"') && !r.text.includes('me-amclabel.js') && !s.includes('X00'), `${code} に欄を出した`);
  }
});

await ta('[V3] 発注アプリの権限が無い = 値を出さない / 名簿でない = 見るだけ (保存のボタン・JS なし)', async () => {
  let r = await call('GET', '/apps/master-edit/sku/amc3', { session: 'editor' });
  let s = section(r.text);
  assert.match(s, /発注アプリの権限がないので、AMC ラベルの印字は出せません/);
  assert.ok(!s.includes('X00OLD0003') && !s.includes('id="amc-cands"') && !r.text.includes('me-amclabel.js'));
  r = await call('GET', '/apps/master-edit/sku/amc3', { session: 'po-viewer' });
  s = section(r.text);
  assert.ok(s.includes('X00OLD0003') && s.includes('見るだけです'), '名簿でない人に値は見せる');
  assert.ok(!s.includes('id="amc-save"') && !r.text.includes('me-amclabel.js'));
  assert.match(s, /name="amc-pick" value="keep" checked disabled/);
});

await ta('[V4] 今の値が候補に無い = 「今の値のまま」(候補に無い)・印字済みなのに入荷アプリで BC シールを貼る = 食い違いの印 (自動では直さない)', async () => {
  const r = await call('GET', '/apps/master-edit/sku/amc3');
  const s = section(r.text);
  assert.match(s, /name="amc-pick" value="keep" checked/);
  assert.ok(s.includes('候補に無い') && s.includes('X00OLD0003') && s.includes('印字済み'));
  assert.match(s, /id="amc-mismatch">/); assert.ok(s.includes('入荷アプリでは「入庫で BC シールを貼る」(BCシール貼付必要)'));
  assert.ok(s.includes('Company DB にこの商品の FNSKU (1 個だけの Amazon の出品) も JAN もありません'));
  const d = amcData(r.text);
  assert.deepEqual([d.version, d.status, d.value, d.pick, d.inbound], [1, 'printed', 'X00OLD0003', 'keep', { bc_seal: 'BCシール貼付必要', needed: true }]);
  assert.equal(podb.prepare("SELECT 入庫時BCシール貼りフラグ AS v FROM f_inbound_info WHERE code_key = 'amc3'").get().v, 'BCシール貼付必要', '入荷アプリの値を変えた');
});

await ta('[W1] 候補の FNSKU を選んで保存 = Company DB の値で行を作る・events・記録 (だれ・via・前と後・選んだ候補)・発注アプリの一覧に同じ値', async () => {
  const row = await extRow('X00AMC0001'); const lid = await listingId('pr_amc1');
  const body = { request_id: uuid(), version: null, status: 'requested', note: '10/13 に依頼', pick: { kind: 'fnsku', row, listing_id: lid, value: 'X00AMC0001' } };
  const r = await call('POST', url('amc1'), { body });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.label.status, r.j.label.barcodeValue, r.j.label.barcodeType, r.j.label.version], ['requested', 'X00AMC0001', 'fnsku', 1]);
  const l = labelOf('amc1');
  assert.deepEqual([l.status, l.barcode_value, l.barcode_type, l.note, l.source, l.updated_by], ['requested', 'X00AMC0001', 'fnsku', '10/13 に依頼', 'manual', 'naka@test']);
  assert.deepEqual(eventsOf('amc1').map((e) => [e.actor, e.source, e.from_status, e.to_status, e.barcode_value]), [['naka@test', 'manual', null, 'requested', 'X00AMC0001']]);
  const a = auditOf('amc1');
  assert.equal(a.length, 1);
  assert.deepEqual([a[0].actor, a[0].action, a[0].request_id, a[0].detail.via, a[0].detail.before, a[0].detail.after.barcode_value], ['naka@test', 'barcode_label_write', body.request_id, 'master-edit:sku', null, 'X00AMC0001']);
  assert.deepEqual(a[0].detail.pick, { kind: 'fnsku', external_id_row: row, listing_id: lid, listing_code: 'pr_amc1', mall: 'amazon', market: 'A1VC38T7YXB528', value: 'X00AMC0001' });
  const ov = await call('GET', '/apps/purchase-orders/api/barcode-labels');
  const t = ov.j.targets.find((x) => x.code === 'amc1');
  assert.deepEqual([t.label.status, t.label.barcodeValue], ['requested', 'X00AMC0001'], '発注アプリの一覧に出ない');
  // 画面: 今の値 = 候補 (いまの値の札・その候補が選ばれている)
  const s = section((await call('GET', '/apps/master-edit/sku/amc1')).text);
  assert.match(s, /name="amc-pick" value="c0" checked/); assert.ok(s.includes('いまの値'));
  // JAN を選ぶ (版 1 → 2・前と後)
  const jr = await call('POST', url('amc1'), { body: { version: 1, status: 'printed', note: '', pick: { kind: 'jan', row: await extRow(J1), value: J1 } } });
  assert.equal(jr.status, 200, jr.text);
  assert.deepEqual([labelOf('amc1').barcode_value, labelOf('amc1').barcode_type, labelOf('amc1').version, labelOf('amc1').note], [J1, 'jan', 2, null]);
  const a2 = auditOf('amc1').at(-1);
  assert.deepEqual([a2.detail.before.barcode_value, a2.detail.before.status, a2.detail.after.barcode_value, a2.detail.pick.kind], ['X00AMC0001', 'requested', J1, 'jan']);
});

await ta('[R1] 保存のときに Company DB で確かめ直す (Codex R3 M5): FNSKU が外された・出品がまとめ売りになった・値が違う・出品が違う・ほかの商品の JAN の行 = 409 (何も書かない)', async () => {
  const c0 = counts(); const before = labelOf('amc2');
  const row = await extRow('X00AMC0003'); const lid = await listingId('pr_amc2');
  const send = (pick) => call('POST', url('amc2'), { body: { version: null, status: 'printed', note: '', pick } });
  // 値が画面と違う (古い画面)
  let r = await send({ kind: 'fnsku', row, listing_id: lid, value: 'X00AMC9999' });
  assert.deepEqual([r.status, r.j.reason, r.j.field], [409, 'candidate_changed', 'value'], r.text);
  // 出品の番号が違う (ほかの出品の行を差し替えた)
  r = await send({ kind: 'fnsku', row, listing_id: await listingId('pr_amc1'), value: 'X00AMC0003' });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  // ほかの商品の出品の FNSKU の行
  r = await send({ kind: 'fnsku', row: await extRow('X00OTH0001'), listing_id: await listingId('pr_oth1'), value: 'X00OTH0001' });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  // まとめ売りの出品の FNSKU (画面に出さない行を送った)
  r = await call('POST', url('amc4'), { body: { version: null, status: 'printed', note: '', pick: { kind: 'fnsku', row: await extRow('X00AMC0002'), listing_id: await listingId('pr_amc1_2p'), value: 'X00AMC0002' } } });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  // ほかの商品の JAN の行
  r = await send({ kind: 'jan', row: await extRow(J9), value: J9 });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  // 画面を開いた後に出品がまとめ売りになった (構成の数量が 2)
  await pg.query('update core.listing_components set qty = 2 where listing_id = $1', [lid]);
  r = await send({ kind: 'fnsku', row, listing_id: lid, value: 'X00AMC0003' });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  assert.match(r.j.error, /まとめ売り \(2 個\)/);
  await pg.query('update core.listing_components set qty = 1 where listing_id = $1', [lid]);
  // 画面を開いた後に FNSKU が外された (valid_to)
  await pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [row]);
  r = await send({ kind: 'fnsku', row, listing_id: lid, value: 'X00AMC0003' });
  assert.deepEqual([r.status, r.j.reason], [409, 'candidate_gone'], r.text);
  assert.match(r.j.error, /もうこの商品の出品に付いていません/);
  assert.deepEqual(counts(), c0, '断った保存で何か書いた');
  assert.deepEqual(labelOf('amc2'), before);
  await pg.query('update core.external_ids set valid_to = null where external_id_row = $1', [row]);
  r = await send({ kind: 'fnsku', row, listing_id: lid, value: 'X00AMC0003' });
  assert.equal(r.status, 200, r.text);
});

await ta('[M1] 手で打つ: 理由が要る (400)・ほかの商品の JAN / FNSKU = 409・通れば記録に理由 (候補の値でも手入力の記録)', async () => {
  const c0 = counts();
  const send = (pick, code = 'amc4') => call('POST', url(code), { body: { version: null, status: 'printed', note: '', pick } });
  let r = await send({ kind: 'manual', value: 'X00NEW0004', reason: '' });
  assert.deepEqual([r.status, r.j.reason, r.j.field], [400, 'invalid_input', 'manual_reason'], r.text);
  r = await send({ kind: 'manual', value: 'X00 NEW', reason: 'x' });
  assert.deepEqual([r.status, r.j.field], [400, 'manual_value']);
  // 形 (Codex #1701 R1 M1): FNSKU (X0 + 英数 8) / JAN-8・JAN-13 (チェック数字まで)。ほかの形は「例外の形」(manual_other) を明示で
  for (const [pick, re] of [
    [{ kind: 'manual', value: '4901234567890', reason: 'x' }, /チェック数字が合いません/],
    [{ kind: 'manual', value: '49012345', reason: 'x' }, /チェック数字が合いません/],
    [{ kind: 'manual', value: 'ABC-123', reason: 'x' }, /FNSKU .* でも JAN .* でもありません/],
    [{ kind: 'manual', value: 'X00SHORT', reason: 'x' }, /でもありません/],
    [{ kind: 'manual_other', value: J1, reason: 'x' }, /JAN の形です/],
    [{ kind: 'manual_other', value: 'X00AMC0001', reason: 'x' }, /FNSKU の形です/],
    [{ kind: 'manual_other', value: '4901234567890', reason: 'x' }, /似ていますが形が合いません/],
    [{ kind: 'manual_other', value: 'X0ABC', reason: 'x' }, /似ていますが形が合いません/],
    [{ kind: 'manual_other', value: 'ABC-123', reason: '' }, /理由が要ります/],
  ]) {
    const x = await send(pick);
    assert.equal(x.status, 400, JSON.stringify(pick) + x.text);
    assert.match(x.j.error, re, JSON.stringify(pick));
  }
  r = await send({ kind: 'manual', value: J9, reason: 'JAN を刷った' });
  assert.deepEqual([r.status, r.j.reason, r.j.field], [409, 'value_of_other', 'manual_value'], r.text);
  assert.match(r.j.error, /ほかの商品 \(oth1\) の JAN/);
  r = await send({ kind: 'manual', value: 'x00oth0001', reason: '小文字で打った' });
  assert.deepEqual([r.status, r.j.reason], [409, 'value_of_other'], r.text);
  assert.match(r.j.error, /oth1/);
  r = await send({ kind: 'manual', value: 'X00MIX0001', reason: '組んだ出品の FNSKU' });
  assert.deepEqual([r.status, r.j.reason], [409, 'value_of_other'], '組んだ出品の FNSKU を 1 個のパッケージに刷らせた');
  assert.deepEqual(counts(), c0);
  r = await send({ kind: 'manual', value: 'x00new0004', reason: '米国の FNSKU (Company DB に無い)' });
  assert.equal(r.status, 200, r.text);
  const a = auditOf('amc4').at(-1);
  assert.deepEqual(a.detail.pick, { kind: 'manual', value: 'X00NEW0004', reason: '米国の FNSKU (Company DB に無い)', type: 'fnsku', known_in_company_db: false });
  assert.equal(labelOf('amc4').barcode_value, 'X00NEW0004', 'FNSKU は大文字にそろえる');
  // 例外の形 = 明示で選ぶ + 理由 (記録は manual_other)
  r = await call('POST', url('amc4'), { body: { version: labelOf('amc4').version, status: 'printed', note: '', pick: { kind: 'manual_other', value: 'AMC-LOT-77', reason: '昔の社内コードを刷った' } } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(auditOf('amc4').at(-1).detail.pick, { kind: 'manual_other', value: 'AMC-LOT-77', reason: '昔の社内コードを刷った', type: 'unknown', known_in_company_db: false });
  // この商品の候補の値を手で打った = 通す (記録は手入力・Company DB にある印)
  r = await call('POST', url('amc1'), { body: { version: labelOf('amc1').version, status: 'printed', note: '', pick: { kind: 'manual', value: 'X00AMC0001', reason: '候補を手で' } } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([auditOf('amc1').at(-1).detail.pick.kind, auditOf('amc1').at(-1).detail.pick.known_in_company_db], ['manual', true]);
});

await ta('[K1] 今の値のまま・値なし: 状態とメモだけ直す・発注アプリのポップオーバーが先に直した = 409 (上書きしない)・行が無いのに今のまま = 409', async () => {
  let r = await call('POST', url('amc3'), { body: { version: 1, status: 'printed', note: '確かめた', pick: { kind: 'keep' } } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([labelOf('amc3').barcode_value, labelOf('amc3').note, labelOf('amc3').version, labelOf('amc3').source], ['X00OLD0003', '確かめた', 2, 'manual']);
  assert.deepEqual(auditOf('amc3').at(-1).detail.pick, { kind: 'keep', value: 'X00OLD0003', known_in_company_db: false });
  // 発注アプリのポップオーバー (PUT) が先に直す → 古い版の保存 = 409 (今の値を返す)
  const put = await call('PUT', '/apps/purchase-orders/api/barcode-labels/amc3', { body: { productCode: 'amc3', status: 'printed', barcodeValue: 'X00OLD0003', note: '発注アプリで', version: 2 } });
  assert.equal(put.status, 200, put.text);
  const c0 = counts();
  r = await call('POST', url('amc3'), { body: { version: 2, status: 'not_needed', note: '', pick: { kind: 'none' } } });
  assert.deepEqual([r.status, r.j.reason, r.j.current.note], [409, 'stale', '発注アプリで'], r.text);
  r = await call('POST', url('amc3'), { body: { version: 2, status: 'printed', note: '', pick: { kind: 'keep' } } });
  assert.deepEqual([r.status, r.j.reason], [409, 'stale'], r.text);
  assert.deepEqual(counts(), c0);
  assert.equal(labelOf('amc3').note, '発注アプリで', '発注アプリの値を消した');
  // 値なし (版 3)
  r = await call('POST', url('amc3'), { body: { version: 3, status: 'not_needed', note: '', pick: { kind: 'none' } } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([labelOf('amc3').status, labelOf('amc3').barcode_value], ['not_needed', null]);
  // 行が無いのに「今の値のまま」/ 新しく作るのに版を送る
  r = await call('POST', url('amc2'), { body: { version: null, status: 'printed', note: '', pick: { kind: 'keep' } } });
  assert.equal(r.status, 409, r.text);   // amc2 は [R1] で行ができた = 版 null = 409
});

await ta('[K2] 今の値のまま (Codex #1701 R1 H1): 発注アプリの今の値が Company DB でほかの商品の FNSKU / JAN = 状態・メモだけの直しでも 409 value_of_other・値なし / 候補に直すのは通す', async () => {
  const v = labelOf('amc4').version;
  podb.prepare("UPDATE po_barcode_labels SET barcode_value = 'X00OTH0001', barcode_type = 'fnsku', version = version + 1 WHERE product_key = 'amc4'").run();   // 旧 Excel の取込で入った他の商品の値
  const c0 = counts();
  let r = await call('POST', url('amc4'), { body: { version: v + 1, status: 'printed', note: 'メモだけ', pick: { kind: 'keep' } } });
  assert.deepEqual([r.status, r.j.reason, r.j.field], [409, 'value_of_other', 'value'], r.text);
  assert.match(r.j.error, /ほかの商品 \(oth1\) の FNSKU/);
  podb.prepare("UPDATE po_barcode_labels SET barcode_value = ?, barcode_type = 'jan', version = version + 1 WHERE product_key = 'amc4'").run(J9);
  r = await call('POST', url('amc4'), { body: { version: v + 2, status: 'unset', note: '', pick: { kind: 'keep' } } });
  assert.deepEqual([r.status, r.j.reason], [409, 'value_of_other'], r.text);
  assert.deepEqual(counts(), c0, '他の商品の値のまま書いた');
  r = await call('POST', url('amc4'), { body: { version: v + 2, status: 'unset', note: '', pick: { kind: 'none' } } });
  assert.equal(r.status, 200, r.text);
  assert.equal(labelOf('amc4').barcode_value, null);
});

await ta('[H2] 書いた後に Company DB を読み直す (Codex #1701 R1 H2): 確かめと書くの間に FNSKU が外された / まとめ売りになった / 手で打った値がほかの商品の FNSKU になった = 書き込みを戻す (要確認)・409 recheck_failed・記録', async () => {
  const AMC = await import('../apps/master-edit/amc-label.mjs');
  // 保存の途中 (接続は画面のロール master_edit) で Company DB を変える = 持ち主のロールで書いて画面のロールに戻す
  const asOwner = async (fn) => { await pg.query('set role deploy'); try { return await fn(); } finally { await pg.query('set role master_edit'); } };
  const row = await extRow('X00AMC0003'); const lid = await listingId('pr_amc2');
  // ① 前の行がある (amc2) → 前の値に戻す
  const before = labelOf('amc2');
  AMC.__setAmcBeforeWrite(async () => asOwner(() => pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [row])));
  let r;
  try { r = await call('POST', url('amc2'), { body: { request_id: uuid(), version: before.version, status: 'requested', note: '頼んだ', pick: { kind: 'fnsku', row, listing_id: lid, value: 'X00AMC0003' } } }); }
  finally { AMC.__setAmcBeforeWrite(null); await pg.query('update core.external_ids set valid_to = null where external_id_row = $1', [row]); }
  assert.deepEqual([r.status, r.j.reason, r.j.reverted], [409, 'recheck_failed', true], r.text);
  assert.match(r.j.error, /保存を戻しました \(要確認\)/);
  const after = labelOf('amc2');
  assert.deepEqual([after.status, after.barcode_value, after.note, after.version], [before.status, before.barcode_value, before.note, before.version + 2], '前の値に戻っていない');
  const au = auditOf('amc2').slice(-2);
  assert.deepEqual(au.map((x) => x.action), ['barcode_label_write', 'barcode_label_revert']);
  assert.match(au[1].detail.why, /もうこの商品の出品に付いていません/);
  assert.deepEqual([au[1].detail.before.status, au[1].detail.after.status, au[1].detail.recheck.kind], ['requested', before.status, 'fnsku']);
  // ② 前の行が無い (amc5) → 未設定 + 要確認のメモ (依頼中 / 印字済みのまま残さない)
  const lid5 = await listingId('pr_amc5');
  AMC.__setAmcBeforeWrite(async () => asOwner(() => pg.query('update core.listing_components set qty = 2 where listing_id = $1', [lid5])));
  try { r = await call('POST', url('amc5'), { body: { version: null, status: 'printed', note: '', pick: { kind: 'fnsku', row: await extRow('X00AMC0005'), listing_id: lid5, value: 'X00AMC0005' } } }); }
  finally { AMC.__setAmcBeforeWrite(null); await pg.query('update core.listing_components set qty = 1 where listing_id = $1', [lid5]); }
  assert.deepEqual([r.status, r.j.reason], [409, 'recheck_failed'], r.text);
  assert.deepEqual([labelOf('amc5').status, labelOf('amc5').barcode_value, labelOf('amc5').note], ['unset', null, BL.RECHECK_NOTE]);
  assert.match(auditOf('amc5').at(-1).detail.why, /まとめ売り \(2 個\)/);
  // ③ 手で打った「Company DB に無い」値が、その間にほかの商品の出品の FNSKU になった
  const v5 = labelOf('amc5').version;
  const lidOth = await listingId('pr_oth1');
  AMC.__setAmcBeforeWrite(async () => asOwner(() => pg.query("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'listing', $1, 'amazon', 'fnsku', 'X00NEW0099', 'manual', 'human')", [lidOth])));
  try { r = await call('POST', url('amc5'), { body: { version: v5, status: 'printed', note: '', pick: { kind: 'manual', value: 'X00NEW0099', reason: '新しい FNSKU' } } }); }
  finally { AMC.__setAmcBeforeWrite(null); }
  assert.deepEqual([r.status, r.j.reason], [409, 'recheck_failed'], r.text);
  assert.deepEqual([labelOf('amc5').status, labelOf('amc5').barcode_value], ['unset', null]);
  assert.match(auditOf('amc5').at(-1).detail.why, /ほかの商品 \(oth1\) の FNSKU/);
  await pg.query("delete from core.external_ids where external_value = 'X00NEW0099'");
  // ④ 変わっていない = ふつうに通る
  r = await call('POST', url('amc5'), { body: { version: labelOf('amc5').version, status: 'printed', note: '', pick: { kind: 'fnsku', row: await extRow('X00AMC0005'), listing_id: lid5, value: 'X00AMC0005' } } });
  assert.equal(r.status, 200, r.text);
  // ⑤ 戻す前にほかの人が直した = 戻さない・revert_failed の記録
  const cur = labelOf('amc5');
  const rv = BL.revertLabelAudited({ productCode: 'amc5', version: cur.version - 1, before: null, actor: 't@test', via: 'master-edit:sku', why: '試験' });
  assert.deepEqual([rv.reverted, labelOf('amc5').barcode_value, auditOf('amc5').at(-1).action], [false, 'X00AMC0005', 'barcode_label_revert_failed']);
});

await ta('[H3] 書いた後の失敗の道 (Codex #1701 R2 High): 読み直しで例外 (権限・切断) = 確かめられない = 戻す (要確認・recheck_unavailable)・戻す書き込みで例外 = 記録 (revert_failed) と今の値を返す・どちらも「何も保存していない」500 にしない', async () => {
  const AMC = await import('../apps/master-edit/amc-label.mjs');
  const asOwner = async (fn) => { await pg.query('set role deploy'); try { return await fn(); } finally { await pg.query('set role master_edit'); } };
  await pg.query('create role amc_noread nologin');   // core を読めない = 読み直しが SQL の誤り (42501) で失敗する
  // ① 読み直しで例外 → 前の行 (amc5 = 印字済み X00AMC0005) に戻す
  const b5 = labelOf('amc5'); const lid5 = await listingId('pr_amc5');
  AMC.__setAmcBeforeWrite(async () => { await pg.query('set role amc_noread'); });
  let r;
  try { r = await call('POST', url('amc5'), { body: { request_id: uuid(), version: b5.version, status: 'requested', note: '頼む', pick: { kind: 'fnsku', row: await extRow('X00AMC0005'), listing_id: lid5, value: 'X00AMC0005' } } }); }
  finally { AMC.__setAmcBeforeWrite(null); await pg.query('set role deploy'); }
  assert.deepEqual([r.status, r.j.reason, r.j.saved, r.j.reverted], [409, 'recheck_unavailable', true, true], r.text);
  assert.match(r.j.error, /保存しましたが、書いた後に Company DB で確かめられませんでした.*保存を戻しました \(要確認\)/);
  assert.ok(!/何も保存していません/.test(r.j.error));
  const a5 = labelOf('amc5');
  assert.deepEqual([a5.status, a5.barcode_value, a5.note, a5.version], [b5.status, b5.barcode_value, b5.note, b5.version + 2]);
  const au5 = auditOf('amc5').slice(-2);
  assert.deepEqual(au5.map((x) => x.action), ['barcode_label_write', 'barcode_label_revert']);
  assert.match(au5[1].detail.why, /読み直せませんでした \(42501\)/);
  assert.deepEqual([r.j.current.status, r.j.current.version], [b5.status, b5.version + 2]);
  // ② 食い違い → 戻す書き込みそのものが失敗 (SQLite の誤り) = 戻せない・revert_failed を別の取引で記録・今の (確かめていない) 値を返す
  const b2 = labelOf('amc2'); const row2 = await extRow('X00AMC0003'); const lid2 = await listingId('pr_amc2');
  AMC.__setAmcBeforeWrite(async () => {
    podb.exec("CREATE TEMP TRIGGER amclabel_boom BEFORE UPDATE ON po_barcode_labels WHEN NEW.product_key = 'amc2' AND NEW.status <> 'requested' BEGIN SELECT RAISE(ABORT, 'disk I/O error (試験)'); END");
    await asOwner(() => pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [row2]));
  });
  try { r = await call('POST', url('amc2'), { body: { request_id: uuid(), version: b2.version, status: 'requested', note: '', pick: { kind: 'fnsku', row: row2, listing_id: lid2, value: 'X00AMC0003' } } }); }
  finally {
    AMC.__setAmcBeforeWrite(null);
    podb.exec('DROP TRIGGER IF EXISTS amclabel_boom');
    await pg.query('update core.external_ids set valid_to = null where external_id_row = $1', [row2]);
  }
  assert.deepEqual([r.status, r.j.reason, r.j.saved, r.j.reverted], [409, 'recheck_failed', true, false], r.text);
  assert.match(r.j.error, /保存を戻せませんでした \(戻す書き込みが失敗: disk I\/O error \(試験\)\)/);
  assert.match(r.j.error, /確かめていない値かもしれません \(要確認/);
  assert.deepEqual([r.j.current.status, r.j.current.version, labelOf('amc2').status, labelOf('amc2').version], ['requested', b2.version + 1, 'requested', b2.version + 1], '今の値を返していない');
  const au2 = auditOf('amc2').slice(-2);
  assert.deepEqual(au2.map((x) => x.action), ['barcode_label_write', 'barcode_label_revert_failed']);
  assert.match(au2[1].detail.error, /disk I\/O error/);
  assert.match(au2[1].detail.why, /もうこの商品の出品に付いていません/);
  assert.equal(au2[1].detail.after.status, 'requested');
});

await ta('[G1] 門: 発注アプリの権限なし・名簿でない = 403・Origin・セット = 400・無い商品 = 404・対象外 = 409 not_target・本文の形 = 400 (どれも何も書かない)', async () => {
  const c0 = counts();
  const ok = { version: null, status: 'unset', note: '', pick: { kind: 'none' } };
  assert.deepEqual([(await call('POST', url('amc2'), { body: ok, session: 'editor' })).j.reason], ['no_po_access']);
  assert.equal((await call('POST', url('amc2'), { body: ok, session: 'po-viewer' })).status, 403);
  assert.equal((await call('POST', url('amc2'), { body: ok, origin: false })).j.error, 'origin_mismatch');
  assert.deepEqual([(await call('POST', url('set1'), { body: ok })).status], [400]);
  assert.equal((await call('POST', url('no-such'), { body: ok })).status, 404);
  for (const code of ['oth1', 'amc-c3', 'fresh1']) {
    const r = await call('POST', url(code), { body: ok });
    assert.deepEqual([r.status, r.j.reason], [409, 'not_target'], `${code}: ${r.text}`);
  }
  for (const [b, field] of [[{ ...ok, status: 'done' }, 'status'], [{ ...ok, pick: { kind: 'magic' } }, 'value'], [{ ...ok, bogus: 1 }, undefined], [{ ...ok, version: '1' }, undefined],
    [{ ...ok, note: 'a\nb' }, 'note'], [{ ...ok, pick: { kind: 'fnsku', row: 'x', listing_id: '1' } }, 'value']]) {
    const r = await call('POST', url('amc2'), { body: b });
    assert.equal(r.status, 400, JSON.stringify(b));
    if (field) assert.equal(r.j.field, field, JSON.stringify(b));
  }
  assert.deepEqual(counts(), c0);
});

await ta('[P2] 1 商品だけの読み方 (Codex #1701 R1 M2): 商品管理リスト全体を読まない・NE オーバーレイ (使う / 朝の同期より古い = 使わない)・大文字小文字の重なりまで、発注アプリの一覧の決め方と同じ', async () => {
  const LG = await import('../apps/purchase-orders/logic.js');
  const all = ['amc1', 'amc2', 'amc3', 'amc4', 'amc5', 'amc-c3', 'oth1', 'set1', 'fresh1', 'amc-ov', 'ov-off', 'dup1', 'DUP1', ' Amc1 '];
  const same = (label) => {
    const { targets } = BL.loadBarcodeTargets();
    for (const c of all) {
      const t = BL.barcodeTargetOf(c);
      const k = c.trim().toLowerCase();
      assert.equal(t.target, targets.has(k), `${label}: ${c} の対象の判定が一覧と違う`);
      if (t.target) assert.deepEqual([t.code, t.name, t.vendorCode], [targets.get(k).code, targets.get(k).name, targets.get(k).vendorCode || null], `${label}: ${c}`);
    }
  };
  same('オーバーレイなし');
  assert.deepEqual([BL.barcodeTargetOf('amc-ov').reason, BL.barcodeTargetOf('dup1').target], ['not_amc', true]);
  const now = new Date().toISOString();
  podb.prepare("INSERT INTO po_ne_overlay_meta (id, uploaded_at, row_count, filename) VALUES (1, ?, 2, 't.csv')").run(now);
  podb.prepare("INSERT INTO po_ne_overlay_rows (product_key, product_code, 仕入先コード) VALUES ('amc-ov', 'amc-ov', '0001'), ('ov-off', 'ov-off', '0002')").run();
  try {
    same('オーバーレイあり');
    assert.deepEqual([BL.barcodeTargetOf('amc-ov').target, BL.barcodeTargetOf('ov-off').reason], [true, 'not_amc']);
    // 朝の NE 同期の方が新しい = オーバーレイは使わない (loadPmlMerged と同じ)
    podb.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at = ? WHERE id = 1').run(new Date(Date.now() + 60e3).toISOString());
    same('古いオーバーレイ');
    assert.deepEqual([BL.barcodeTargetOf('amc-ov').target, BL.barcodeTargetOf('ov-off').target], [false, true]);
    // 1 商品だけ = 全件を読む loadPmlMerged を呼ばない (索引で 1 商品の行だけ)
    const plan = podb.prepare(`EXPLAIN QUERY PLAN ${LG.PML_ONE_SQL}`).all('run_t', 'amc1').map((x) => x.detail).join(' ');
    assert.match(plan, /idx_mpsr_run_code_norm/, plan);
    assert.deepEqual(LG.loadPmlMergedFor('AMC1').rows.map((x) => x['商品コード']), ['amc1']);
  } finally {
    podb.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at = NULL WHERE id = 1').run();
    podb.prepare('DELETE FROM po_ne_overlay_rows').run(); podb.prepare('DELETE FROM po_ne_overlay_meta').run();
  }
});

await ta('[U1] つかいかた (同じ PR): AMC ラベルの節 (3 つを混ぜない・候補・手で打つ理由・確かめ直し・権限)', async () => {
  const m = await call('GET', '/apps/master-edit/manual');
  for (const w of ['<h2 id="amc-label">AMC 商品ラベルのバーコード印字 (発注アプリに保存)</h2>', '3 つを混ぜません', 'ラベルの印字を保存', '理由が要ります', 'この商品 1 個だけの出品', 'Company DB でもう一度確かめます', '名簿の人で、発注アプリの権限もある人', '「設定済み」', '例外の形', 'チェック数字まで確かめます', '書いた後にもう一度', '保存を戻して']) {
    assert.ok(m.text.includes(w), `つかいかたに「${w}」が無い`);
  }
});

await ta('[P1] 部品 (barcode-labels.js): 対象の判定は発注アプリと同じ・via は決まった値だけ・対象外は取引の中で断る', async () => {
  assert.deepEqual(['amc1', 'oth1', 'amc-c3', 'fresh1', 'set1'].map((c) => [BL.barcodeTargetOf(c).target, BL.barcodeTargetOf(c).reason]),
    [[true, null], [false, 'not_amc'], [false, 'not_own_product'], [false, 'missing_from_pml'], [false, 'set']]);
  const ov = BL.listBarcodeOverview();
  assert.deepEqual(ov.targets.map((t) => t.key).sort(), ['amc1', 'amc2', 'amc3', 'amc4', 'amc5', 'amc6', 'dup1', 'ov-off']);
  assert.throws(() => BL.writeLabelAudited({ productCode: 'amc2', status: 'unset', barcodeValue: '', note: '', version: null, actor: 'x', via: 'po-admin' }), /via/);
  assert.throws(() => BL.writeLabelAudited({ productCode: 'oth1', status: 'unset', barcodeValue: '', note: '', version: null, actor: 'x', via: 'master-edit:sku' }), (e) => e.status === 409 && e.reason === 'not_target');
  assert.equal(labelOf('oth1'), null);
});

// ── 画面の JS を本当に動かす (Playwright・Chromium)。無い = 失敗 (黙って飛ばさない)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ ──
console.log('\n画面の動き (Playwright)');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の動きの試験を飛ばした');
else {
  let browser;
  try { browser = await (await import('playwright')).chromium.launch(); } catch (e) { console.error(`  NG Playwright / Chromium を動かせない: ${e.message}`); process.exitCode = 1; }
  const SHOT = process.env.AMC_SHOT_DIR || null;   // 目で見る用の画面の写し (任意)
  const open = async (code, viewport = { width: 1440, height: 900 }) => {
    const ctx = await browser.newContext({ viewport, locale: 'ja-JP', hasTouch: viewport.width < 500 });
    await ctx.setExtraHTTPHeaders({ 'x-test-session': 'po-editor' });
    const p = await ctx.newPage();
    p.setDefaultTimeout(15e3);
    const errors = [];
    p.on('pageerror', (e) => errors.push(e.message));
    await p.goto(`${ORIGIN}/apps/master-edit/sku/${code}`, { waitUntil: 'load' });
    await p.waitForFunction(() => !!(window.MasterEdit && window.MasterEdit.dirty));
    return { ctx, p, errors };
  };
  const tu = async (name, fn) => {
    if (!browser) return;
    await ta(name, fn);
  };

  await tu('[UI1] 状態を変えると保存のボタンが開く・未保存に数える (離れるときの確認)・手で打つは理由が無いと送らない・理由を入れて保存 = 発注アプリに入り、画面を読み直す', async () => {
    const { ctx, p, errors } = await open('amc1');
    try {
      assert.equal(await p.isDisabled('#amc-save'), true, '開いたばかりで保存のボタンが開いている');
      const v0 = labelOf('amc1').version;
      await p.click('#amc-status button[data-v="requested"]');
      assert.equal(await p.isDisabled('#amc-save'), false);
      assert.match(await p.textContent('#amc-status-hint'), /AMC に印字を頼んだ/);
      assert.ok((await p.evaluate(() => window.MasterEdit.dirty().items)).some((x) => /AMC 商品ラベルの印字/.test(x)), '未保存に数えない');
      assert.equal(await p.evaluate(() => document.getElementById('sec-amc').classList.contains('po-dirty')), true);
      // 手で打つ: 欄が開く・形の目安・理由が空 = 送らない
      await p.check('input[name="amc-pick"][value="manual"]');
      assert.equal(await p.isVisible('#amc-manual'), true);
      await p.fill('#amc-mv', 'X00UI00001');
      assert.match(await p.textContent('#amc-mv-type'), /FNSKU の形/);
      await p.fill('#amc-mv', '4901234567890');
      assert.match(await p.textContent('#amc-mv-type'), /チェック数字が合いません/);
      await p.fill('#amc-mv', 'AMC-LOT-1');
      assert.match(await p.textContent('#amc-mv-type'), /「例外の形」に印/);
      await p.check('#amc-mx');
      assert.match(await p.textContent('#amc-mv-type'), /例外の形 \(理由を/);
      await p.uncheck('#amc-mx');
      await p.fill('#amc-mv', 'X00UI00001');
      const ev0 = eventsOf('amc1').length;
      await p.click('#amc-save');
      assert.match(await p.textContent('#amc-msg'), /理由が要ります/);
      assert.equal(await p.evaluate(() => document.querySelector('[data-row="amc.manual_reason"]').classList.contains('err')), true);
      assert.equal(eventsOf('amc1').length, ev0, '理由が無いのに送った');
      await p.fill('#amc-mr', '画面の試験');
      await Promise.all([p.waitForEvent('load'), p.click('#amc-save')]);   // 保存 → 読み直し
      const l = labelOf('amc1');
      assert.deepEqual([l.status, l.barcode_value, l.version], ['requested', 'X00UI00001', v0 + 1]);
      assert.equal(auditOf('amc1').at(-1).detail.pick.reason, '画面の試験');
      await p.waitForFunction(() => !!(window.MasterEdit && window.MasterEdit.dirty));
      assert.match(await p.textContent('#amc-now'), /依頼中[\s\S]*X00UI00001/);
      assert.equal(await p.isChecked('input[name="amc-pick"][value="keep"]'), true, '読み直した後は「今の値のまま」');
      assert.equal(await p.isDisabled('#amc-save'), true);
      assert.deepEqual(errors, []);
    } finally { await ctx.close(); }
  });

  await tu('[UI2] 候補を選んで保存 (Company DB の値)・印字済みにすると入荷アプリとの食い違いの印がその場で出る / 消える (自動では直さない)', async () => {
    const { ctx, p, errors } = await open('amc3');
    try {
      assert.equal(await p.isVisible('#amc-mismatch'), false);   // [K1] で「不要」
      await p.click('#amc-status button[data-v="printed"]');
      assert.equal(await p.isVisible('#amc-mismatch'), true, '食い違いの印が出ない');
      assert.match(await p.textContent('#amc-warn'), /刷る値が空です/);
      await p.click('#amc-status button[data-v="unset"]');
      assert.equal(await p.isVisible('#amc-mismatch'), false);
      await p.click('#amc-status button[data-v="printed"]');
      await p.fill('#amc-note', '画面から');
      // 候補が無い商品 = 手で打つ
      await p.check('input[name="amc-pick"][value="manual"]');
      await p.fill('#amc-mv', 'X00UI00003'); await p.fill('#amc-mr', '候補が無い');
      await Promise.all([p.waitForEvent('load'), p.click('#amc-save')]);
      assert.deepEqual([labelOf('amc3').status, labelOf('amc3').barcode_value, labelOf('amc3').note], ['printed', 'X00UI00003', '画面から']);
      await p.waitForFunction(() => !!(window.MasterEdit && window.MasterEdit.dirty));
      assert.equal(await p.isVisible('#amc-mismatch'), true, '読み直した後も印');
      assert.equal(podb.prepare("SELECT 入庫時BCシール貼りフラグ AS v FROM f_inbound_info WHERE code_key = 'amc3'").get().v, 'BCシール貼付必要', '入荷アプリの値を変えた');
      assert.deepEqual(errors, []);
    } finally { await ctx.close(); }
    // 候補を選ぶ (amc2 = FNSKU の候補 1 つ)
    const o = await open('amc2');
    try {
      assert.equal(await o.p.isChecked('input[name="amc-pick"][value="c0"]'), true, '今の値の候補が選ばれていない');
      await o.p.check('input[name="amc-pick"][value="none"]');
      await o.p.check('input[name="amc-pick"][value="c0"]');
      assert.equal(await o.p.isDisabled('#amc-save'), true, '元に戻したのに未保存');
      await o.p.click('#amc-status button[data-v="printed"]');
      await Promise.all([o.p.waitForEvent('load'), o.p.click('#amc-save')]);
      assert.deepEqual([labelOf('amc2').status, labelOf('amc2').barcode_value, auditOf('amc2').at(-1).detail.pick.kind], ['printed', 'X00AMC0003', 'fnsku']);
      assert.deepEqual(o.errors, []);
    } finally { await o.ctx.close(); }
  });

  await tu('[UI3] 開いた後に発注アプリで直された = 409 を知らせて上書きしない (画面を開き直す案内)', async () => {
    const { ctx, p } = await open('amc2');
    try {
      const cur = labelOf('amc2');
      const put = await call('PUT', '/apps/purchase-orders/api/barcode-labels/amc2', { body: { productCode: 'amc2', status: 'printed', barcodeValue: cur.barcode_value, note: '発注アプリが先', version: cur.version } });
      assert.equal(put.status, 200, put.text);
      await p.click('#amc-status button[data-v="unset"]');
      await p.click('#amc-save');
      await p.waitForFunction(() => /開き直す/.test(document.getElementById('amc-msg').textContent));
      assert.equal(labelOf('amc2').note, '発注アプリが先');
    } finally { await ctx.close(); }
  });

  await tu('[UI5] 未登録 (発注アプリに行が無い) = 何も変えずに「ラベルの印字を保存」できる = 未設定・値なしの行ができる・記録が残る・未保存には数えない (Codex #1701 R3 M)', async () => {
    assert.equal(labelOf('amc6'), null);
    const { ctx, p, errors } = await open('amc6');
    try {
      assert.match(await p.textContent('#amc-now'), /未登録/);
      assert.equal(await p.isChecked('input[name="amc-pick"][value="none"]'), true);
      assert.equal(await p.getAttribute('#amc-status', 'data-value'), 'unset');
      assert.equal(await p.isDisabled('#amc-save'), false, '未登録なのに保存のボタンが閉じている');
      assert.match(await p.textContent('#amc-status-hint'), /保存すると発注アプリに行ができます/);
      assert.equal(await p.evaluate(() => window.MasterEdit.dirty().n), 0, '何も変えていないのに未保存に数えた');
      await Promise.all([p.waitForEvent('load'), p.click('#amc-save')]);
      const l = labelOf('amc6');
      assert.deepEqual([l && l.status, l && l.barcode_value, l && l.version, l && l.source], ['unset', null, 1, 'manual']);
      assert.deepEqual(eventsOf('amc6').map((e) => [e.from_status, e.to_status]), [[null, 'unset']]);
      const a = auditOf('amc6');
      assert.deepEqual([a.length, a[0].action, a[0].detail.before, a[0].detail.after.status, a[0].detail.pick], [1, 'barcode_label_write', null, 'unset', { kind: 'none' }]);
      await p.waitForFunction(() => !!(window.MasterEdit && window.MasterEdit.dirty));
      assert.match(await p.textContent('#amc-now'), /未設定/);
      assert.equal(await p.isDisabled('#amc-save'), true, '行ができた後は変えるまで閉じる');
      assert.deepEqual(errors, []);
    } finally { await ctx.close(); }
  });

  for (const vp of [{ width: 390, height: 844 }, { width: 1024, height: 768 }]) {
    await tu(`[UI4] ${vp.width}px: 横にはみ出さない・候補の札が幅に収まる・押せる`, async () => {
      const { ctx, p, errors } = await open('amc1', vp);
      try {
        await p.locator('#sec-amc').scrollIntoViewIfNeeded();
        const m = await p.evaluate(() => {
          const sec = document.getElementById('sec-amc').getBoundingClientRect();
          const cards = [...document.querySelectorAll('#sec-amc .amc-cand, #sec-amc .seg, #amc-save')].map((e) => e.getBoundingClientRect());
          const el = document.getElementById('sec-amc'); const doc = document.documentElement.scrollWidth;
          el.style.display = 'none'; const without = document.documentElement.scrollWidth; el.style.display = '';
          const tall = Math.min(...[...document.querySelectorAll('#sec-amc .amc-cand')].map((e) => e.getBoundingClientRect().height));
          return { doc, without, vw: document.documentElement.clientWidth, sec: [sec.left, sec.right], maxRight: Math.max(...cards.map((r) => r.right)), minLeft: Math.min(...cards.map((r) => r.left)), tall };
        });
        // 画面全体の横幅は欄が無いときと同じ (この欄で横にはみ出さない。1024 以上は画面にも収まる)
        assert.ok(m.doc <= Math.max(m.vw, m.without) + 1, `この欄で横にはみ出した: ${JSON.stringify(m)}`);
        if (vp.width >= 1024) assert.ok(m.doc <= m.vw + 1, `横にはみ出した: ${JSON.stringify(m)}`);
        assert.ok(m.tall >= 44, `候補の札が低い (指で押しにくい): ${m.tall}`);
        assert.ok(m.maxRight <= m.sec[1] + 1 && m.minLeft >= m.sec[0] - 1, `欄の外にはみ出した: ${JSON.stringify(m)}`);
        await p.click('#amc-status button[data-v="unset"]');
        await p.check('input[name="amc-pick"][value="c0"]');
        assert.equal(await p.isDisabled('#amc-save'), false);
        if (SHOT) await p.locator('#sec-amc').screenshot({ path: path.join(SHOT, `amclabel-${vp.width}.png`) });
        assert.deepEqual(errors, []);
      } finally { await ctx.close(); }
    });
  }
  if (browser) await browser.close();
}

server.close();
await pg.close();
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows の握り */ }
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
