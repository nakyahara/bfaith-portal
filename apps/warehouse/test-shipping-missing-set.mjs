/**
 * test-shipping-missing-set.mjs — register の「送料未登録」にセットを出す
 *
 * 背景 (2026-09-10 中原さん):
 *   「送料未登録: 0」なのに、送料の無いセットが 146 件 (全部取扱中) あった。
 *   件数バッジ・一覧・CSV の 3 か所がどれも 単品/例外 だけを見ていたため、画面に 1 件も出ない。
 *   想定利益ではその 132 品番が shipping_master_missing で計算できていなかった。
 *
 * 🚨 この試験は SQL を書き写さない。書き写すと、本番の条件を戻しても試験は通る
 *    (test-sales-class-set.mjs の §3 はそうなっている)。**画面が叩く API を直接呼ぶ**。
 *
 * 実行: node apps/warehouse/test-shipping-missing-set.mjs
 * 本番 DB には触れない (一時 DATA_DIR に専用 warehouse.db を作り、終了時に削除)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipping-missing-set-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { rebuildMProducts } = await import('./rebuild-m-products.js');
const routerMod = await import('./router.js');
const router = routerMod.default || routerMod.router;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(a === b, `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

/** router に登録されたハンドラを直接呼ぶ (サーバは起こさない) */
function call(method, routePath, { query = {}, body = {}, params = {} } = {}) {
  const layer = router.stack.find(l => l.route && l.route.path === routePath && l.route.methods[method]);
  if (!layer) throw new Error(`${method.toUpperCase()} ${routePath} が router に無い`);
  return new Promise((resolve, reject) => {
    const res = {
      statusCode: 200, headers: {},
      setHeader(k, v) { this.headers[k] = v; }, set(k, v) { this.headers[k] = v; return this; },
      status(c) { this.statusCode = c; return this; },
      json(b) { resolve({ status: this.statusCode, body: b }); },
      send(b) { resolve({ status: this.statusCode, text: String(b) }); },
      attachment() { return this; }, type() { return this; },
    };
    try {
      const r = layer.route.stack[0].handle({ query, body, params, headers: {}, session: {} }, res, reject);
      if (r && typeof r.catch === 'function') r.catch(reject);
    } catch (e) { reject(e); }
  });
}

await initDB();
const db = getDB();
const NOW = '2026-09-10 12:00:00';

const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products
  (商品コード, 商品名, 原価, 売価, 取扱区分, 在庫数, 引当数, 消費税率, 作成日, synced_at)
  VALUES (?, ?, ?, ?, '取扱中', 0, 0, 10, '2026-01-01', ?)`);
const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products
  (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at)
  VALUES (?, ?, ?, ?, ?, ?)`);
const insShip = db.prepare(`INSERT OR REPLACE INTO product_shipping
  (sku, product_name, shipping_code, ship_method, ship_cost, note, synced_at)
  VALUES (?, ?, ?, ?, ?, '', ?)`);

// 品質ゲート (総件数 3,000件未満は反映中止) を通すためのダミー単品。
// 🚨 ダミーには送料を登録しておく。登録しないと 3,200 件が未登録に並び、件数で試験できない
db.transaction(() => {
  for (let i = 0; i < 3200; i++) {
    insNe.run(`filler-${i}`, `ダミー${i}`, 100, 200, NOW);
    insShip.run(`filler-${i}`, `ダミー${i}`, '501', 'ネコポス', 237, NOW);
  }
})();

// 単品
insNe.run('tan-ok', '送料登録済みの単品', 100, 300, NOW);
insShip.run('tan-ok', '送料登録済みの単品', '501', 'ネコポス', 237, NOW);
insNe.run('tan-miss', '送料未登録の単品', 100, 300, NOW);          // 従来から出ていたもの

// set-miss: 送料の無いセット。**NE の単品マスタには居ない** (セット名はセットの表にしか無い)
insSet.run('set-miss', '送料未登録のセット 2個セット', 600, 'tan-ok', 2, NOW);
// set-ok: セット商品コードで送料が登録済み
insSet.run('set-ok', '送料登録済みのセット', 600, 'tan-ok', 2, NOW);
insShip.run('set-ok', '送料登録済みのセット', '702', '宅急便60サイズ', 538, NOW);

const result = await rebuildMProducts();
ok(result.ok, `rebuild が成功する (${result.total}件)`);

const getMp = db.prepare('SELECT 商品区分, 送料, 送料コード FROM m_products WHERE 商品コード = ?');
eq(getMp.get('set-miss')?.商品区分, 'セット', 'set-miss はセットとして入る');
eq(getMp.get('set-miss')?.送料, null, 'set-miss の送料は NULL (構成品から導出しない)');
eq(getMp.get('set-ok')?.送料コード, '702', 'set-ok はセット商品コードで登録した送料コードを持つ');

// ───────────────────────── 1. 件数バッジ ─────────────────────────
console.log('\n── /api/missing/counts ──');
{
  const r = await call('get', '/api/missing/counts');
  eq(r.body.shipping, 2, '送料未登録 = 単品 1 + セット 1');

  // 旧条件ではセットが 1 件も拾えなかったことを固定する (これが「0 件」の原因)
  const oldCond = db.prepare(`SELECT COUNT(*) cnt FROM m_products
    WHERE 商品区分 IN ('単品', '例外') AND 送料 IS NULL AND 商品区分 = 'セット'`).get().cnt;
  eq(oldCond, 0, '旧条件ではセットが 0 件 = 画面に出なかった原因');
}

// ───────────────────────── 2. 一覧 ─────────────────────────
console.log('\n── /api/missing/prioritized?type=shipping ──');
{
  const r = await call('get', '/api/missing/prioritized', { query: { type: 'shipping' } });
  const rows = r.body.rows || [];
  const codes = rows.map(x => x.商品コード);
  ok(codes.includes('set-miss'), '送料の無いセットが一覧に出る');
  ok(codes.includes('tan-miss'), '送料の無い単品も従来どおり出る');
  ok(!codes.includes('set-ok'), '登録済みのセットは出ない');
  ok(!codes.includes('tan-ok'), '登録済みの単品は出ない');
  eq(rows.find(x => x.商品コード === 'set-miss')?.商品区分, 'セット',
    '一覧の行にセットと分かる区分が付く (画面はこれで「セット」バッジを出す)');
}

// ───────────────────────── 3. CSV ─────────────────────────
console.log('\n── /api/missing/download?type=shipping ──');
{
  const r = await call('get', '/api/missing/download', { query: { type: 'shipping' } });
  const text = r.text || '';
  ok(/set-miss/.test(text), 'CSV にもセットが出る (バッジ・一覧と同じ条件)');
  ok(!/set-ok/.test(text), 'CSV に登録済みのセットは出ない');
}

// ───────────────────────── 4. 画面から登録したら消える ─────────────────────────
console.log('\n── POST /api/shipping でセットを登録 ──');
{
  const r = await call('post', '/api/shipping', {
    body: { sku: 'set-miss', shipping_code: '501', ship_method: 'ネコポス', ship_cost: '237' },
  });
  ok(r.body.ok, '登録が通る');
  eq(getMp.get('set-miss')?.送料コード, '501', 'm_products に即時に入る (夜の rebuild を待たない)');

  const c = await call('get', '/api/missing/counts');
  eq(c.body.shipping, 1, '登録したセットはバッジから減る');
  const l = await call('get', '/api/missing/prioritized', { query: { type: 'shipping' } });
  ok(!l.body.rows.some(x => x.商品コード === 'set-miss'), '登録したセットは一覧から消える');

  // 🚨 セットは NE の単品マスタに居ないので、名前は m_products (セット名) から拾う
  const ps = db.prepare('SELECT product_name FROM product_shipping WHERE sku = ?').get('set-miss');
  eq(ps?.product_name, '送料未登録のセット 2個セット',
    '登録済みマスタ検索でセットの名前が空にならない');
}

// ───────────────────────── 5. 夜の rebuild をまたいでも残る ─────────────────────────
console.log('\n── rebuild をもう一度 ──');
{
  const r2 = await rebuildMProducts();
  ok(r2.ok, 'rebuild が成功する');
  eq(getMp.get('set-miss')?.送料コード, '501', '登録したセットの送料コードが rebuild 後も残る');
  const c = await call('get', '/api/missing/counts');
  eq(c.body.shipping, 1, 'rebuild 後もバッジは 1 (単品の未登録だけ)');
}

// ───────────────────────── 結果 ─────────────────────────
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });

console.log(`\n${failed === 0 ? '✅ 全テスト成功' : `❌ ${failed}件 失敗`}`);
process.exit(failed === 0 ? 0 : 1);
