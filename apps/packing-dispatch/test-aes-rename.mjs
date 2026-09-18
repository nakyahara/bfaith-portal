import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * AES → Amazon Easy Ship (NE 発送方法区分 71 → 64、2026-09-18 切替) のテスト。
 *   node apps/packing-dispatch/test-aes-rename.mjs
 *
 * 守りたいこと: AES の伝票は「CSV の現行配送方法」でしか見分けられず、見分け損ねるとロックが黙って外れて
 * ネコポス等に書き換えて出力される (エラーは出ない)。新名・新 id・旧名・表記ゆれのどれでも AES として
 * 素通し (連動列 34/17/76/92 を触らない) になることを、取込 → 出力の通しで確かめる。
 * DATA_DIR を一時ディレクトリに向ける (本番mirrorには触れない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pd-aes-rename-test-'));

const { initMirrorDB, getMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();

let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };

// ─── 旧版が作った本番 DB を再現してから ensureSchema を通す (INSERT OR IGNORE では直らないことの確認) ───
const raw = getMirrorDB();
raw.exec(`CREATE TABLE IF NOT EXISTS pd_shipping_method (
  code TEXT PRIMARY KEY, name_csv TEXT NOT NULL, carrier_id TEXT NOT NULL, ne_carrier_id TEXT NOT NULL,
  yamato_bill_code TEXT, rank INTEGER, is_locked INTEGER NOT NULL DEFAULT 0, is_nekopos INTEGER NOT NULL DEFAULT 0)`);
const preExisting = raw.prepare(`SELECT COUNT(*) c FROM pd_shipping_method`).get().c;
if (preExisting === 0) {
  raw.prepare(`INSERT INTO pd_shipping_method (code,name_csv,carrier_id,ne_carrier_id,yamato_bill_code,rank,is_locked,is_nekopos)
    VALUES ('aes','AES','19','71','',NULL,1,0)`).run();
}

const { ensureSchema, EXPECTED_COL_COUNT, COL } = await import('./db.js');
const { importCsv, listOrders, beginExport, buildExportCsv } = await import('./service.js');
const { buildNeCsv, parseNeCsv } = await import('./csv.js');
const db = ensureSchema();

t('本番 DB の移行: 旧値 (AES / 71) の行が Amazon Easy Ship / 64 になる (code と lock は据え置き)', () => {
  const r = db.prepare(`SELECT * FROM pd_shipping_method WHERE code='aes'`).get();
  assert.equal(r.name_csv, 'Amazon Easy Ship');
  assert.equal(r.ne_carrier_id, '64');
  assert.equal(r.carrier_id, '19');
  assert.equal(r.is_locked, 1);
});

// ensureSchema はプロセス内で 1 回しか移行 SQL を流さない (schemaReady)。「再起動のたびに流れても壊れない」は
// 同じ DB を別プロセスで開き直して確かめる (Codex R1 Low)。
function restartAndEnsure() {
  const r = spawnSync(process.execPath, ['--input-type=module', '-e',
    "const { initMirrorDB } = await import('./apps/warehouse-mirror/db.js'); initMirrorDB();"
    + "const { ensureSchema, getSchemaError } = await import('./apps/packing-dispatch/db.js'); ensureSchema();"
    + "if (getSchemaError()) { console.error(getSchemaError()); process.exit(1); }"],
  { cwd: REPO_ROOT, env: process.env, encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
}
const aesRow = () => db.prepare(`SELECT name_csv, ne_carrier_id, carrier_id, is_locked FROM pd_shipping_method WHERE code='aes'`).get();
const setAes = (name, id) => db.prepare(`UPDATE pd_shipping_method SET name_csv=?, ne_carrier_id=? WHERE code='aes'`).run(name, id);

t('本番 DB の移行: 再起動して移行 SQL がもう一度流れても同じ (idempotent)', () => {
  restartAndEnsure();
  assert.deepEqual(aesRow(), { name_csv: 'Amazon Easy Ship', ne_carrier_id: '64', carrier_id: '19', is_locked: 1 });
  assert.equal(db.prepare(`SELECT COUNT(*) c FROM pd_shipping_method WHERE code='aes'`).get().c, 1);
});

t('本番 DB の移行: 片方だけ先に直されていても残りを取りこぼさない', () => {
  setAes('Amazon Easy Ship', '71'); restartAndEnsure();
  assert.equal(aesRow().ne_carrier_id, '64');
  setAes('AES', '64'); restartAndEnsure();
  assert.equal(aesRow().name_csv, 'Amazon Easy Ship');
});

t('本番 DB の移行: 旧値ではない行 (手で直した値) は触らない', () => {
  setAes('Easy Ship (手入力)', '99'); restartAndEnsure();
  assert.deepEqual([aesRow().name_csv, aesRow().ne_carrier_id], ['Easy Ship (手入力)', '99']);
  setAes('Amazon Easy Ship', '64'); // 以降のテスト用に戻す
});

// ─── 取込 → 出力の通し ───
// ネコポスのルールを持つ商品にしておく = ロックが外れると「ネコポスに書き換わる」のが見える
db.prepare(`INSERT INTO pd_shipping_rule
  (sku_key, product_code, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code)
  VALUES ('hakka10::::', 'hakka10', 'amazon', 1, NULL, 'nekopos', 'pasline3')`).run();

const HEADER = Array.from({ length: EXPECTED_COL_COUNT }, (_, i) => `col${i}`);
Object.assign(HEADER, {
  1: 'ショップ名', 2: '注文番号', 6: '配送先都道府県', 17: '配送会社id',
  34: '配送方法', 55: '品番', 63: '受注数', 76: 'ne配送会社id',
  91: '自動梱包機使用', 92: '配送方法別ヤマト請求顧客コード',
});

function row(orderNo, methodName, neCarrierId, carrierId = '19') {
  const r = Array.from({ length: EXPECTED_COL_COUNT }, () => '');
  r[COL.shop] = '雑貨イズムAmazon店';
  r[COL.orderNo] = orderNo;
  r[COL.pref] = '東京都';
  r[COL.carrierId] = carrierId;
  r[COL.uketsuke] = 'U' + orderNo;
  r[COL.shippingMethod] = methodName;
  r[COL.productCode] = 'hakka10';
  r[COL.qty] = '1';
  r[COL.neCarrierId] = neCarrierId;
  r[COL.packing] = '手動出荷';
  r[COL.yamatoBill] = '';
  return r;
}

const ROWS = [
  row('NEW-1', 'Amazon Easy Ship', '64'),            // 切替後の実物
  row('OLD-1', 'AES', '71'),                         // 切替前に NE で確定していた伝票 (数日混ざる)
  row('DRIFT-1', 'Amazon　Easy Ship', '64'),         // 名前の表記ゆれ (全角空白) でも id=64 で拾う
  row('NEKO-1', 'ヤマト(ネコポス)', '28', '99'),      // 対照: AES ではない普通の伝票
];

const summary = importCsv(buildNeCsv(HEADER, ROWS), 'logi_dispatch.csv', 'test');
const BATCH = summary.batch.batch_id;
const orders = listOrders(BATCH);
const byNo = new Map(orders.map((o) => [o.order_no, o]));

for (const no of ['NEW-1', 'OLD-1', 'DRIFT-1']) {
  t(`取込: ${no} は AES としてロックされる (要判断にもネコポスにもならない)`, () => {
    const o = byNo.get(no);
    assert.ok(o, `${no} が一覧に無い`);
    assert.equal(o.order_type, 'aes');
    assert.equal(o.shipping_method_code, 'aes');
    assert.equal(o.row_status, 'auto');
  });
}

t('取込: 対照のネコポス伝票は従来どおり通常判定 (AES 扱いに巻き込まれない)', () => {
  const o = byNo.get('NEKO-1');
  assert.equal(o.order_type === 'aes', false);
  assert.equal(o.shipping_method_code, 'nekopos');
});

t('出力: AES の行は連動列 34/17/76/92 を 1 文字も触らない (NE が出した値のまま)', () => {
  beginExport(BATCH);
  const { buffer } = buildExportCsv(BATCH);
  const out = parseNeCsv(buffer);
  assert.ok(out.ok, JSON.stringify(out.errors));
  const outBy = new Map(out.rows.map((r) => [r[COL.orderNo], r]));
  for (const src of ROWS.slice(0, 3)) {
    const o = outBy.get(src[COL.orderNo]);
    for (const c of [COL.shippingMethod, COL.carrierId, COL.neCarrierId, COL.yamatoBill]) {
      assert.equal(o[c], src[c], `${src[COL.orderNo]} の列${c} が書き換わった: ${src[c]} → ${o[c]}`);
    }
  }
  // 対照: ネコポスは梱包機マーカーが付く = 出力の書き換え自体は生きている
  assert.equal(outBy.get('NEKO-1')[COL.packing], 'pasline3つ折り');
});

console.log(`\n${passed} passed`);
