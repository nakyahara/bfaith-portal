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
 *    ただし route.stack[0] のハンドラだけを呼ぶので、認証や HTTP の配線は見ていない。
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

/** CSV のデータ行数 (BOM とヘッダを除く) */
const csvRows = (text) => String(text || '').replace(/^﻿/, '').trim().split(/\r?\n/).slice(1);

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
const getMp = db.prepare('SELECT 商品区分, 商品名, 送料, 送料コード, 配送方法 FROM m_products WHERE 商品コード = ?');
const getPs = db.prepare('SELECT product_name, ship_cost FROM product_shipping WHERE sku = ?');
const postShip = (sku) => call('post', '/api/shipping', {
  body: { sku, shipping_code: '501', ship_method: 'ネコポス', ship_cost: '237' },
});

try {
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
  // set-blank: NE にも居るが NE 側の商品名が空文字 (空文字は「無い」扱いでセット名に落ちること)
  insNe.run('set-blank', '', 0, 900, NOW);
  insSet.run('set-blank', 'セット表にだけ名前がある', 900, 'tan-ok', 3, NOW);
  // set-both: NE の名前とセット表の名前が違う (NE が優先されること)
  insNe.run('set-both', 'NE での名前', 0, 900, NOW);
  insSet.run('set-both', 'セット表での名前', 900, 'tan-ok', 1, NOW);
  // ghost: どちらのマスタにも居ないが、前回の登録に名前が残っている (前回値に落ちること)
  insShip.run('ghost', '以前の名前', '702', '宅急便60サイズ', 538, NOW);

  const result = await rebuildMProducts();
  ok(result.ok, `rebuild が成功する (${result.total}件)`);

  eq(getMp.get('set-miss')?.商品区分, 'セット', 'set-miss はセットとして入る');
  eq(getMp.get('set-miss')?.送料, null, 'set-miss の送料は NULL (構成品から導出しない)');
  eq(getMp.get('set-ok')?.送料コード, '702', 'set-ok はセット商品コードで登録した送料コードを持つ');
  eq(getMp.get('set-blank')?.商品名, 'セット表にだけ名前がある', 'set-blank の m_products 名はセット表の名前');

  // ───────────────────────── 1. 件数バッジ ─────────────────────────
  console.log('\n── /api/missing/counts ──');
  {
    const r = await call('get', '/api/missing/counts');
    eq(r.body.shipping, 4, '送料未登録 = 単品 1 + セット 3 (set-miss / set-blank / set-both)');

    // 旧条件 (単品/例外のみ) ではセットが 1 件も拾えなかったことを固定する (これが「0 件」の原因)
    const oldCond = db.prepare(`SELECT COUNT(*) cnt FROM m_products
      WHERE 商品区分 IN ('単品', '例外') AND 送料 IS NULL AND 商品コード LIKE 'set-%'`).get().cnt;
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
    eq(csvRows(text).length, 4, 'CSV の行数はバッジと同じ');
  }

  // ───────────────────────── 4. 画面から登録したら消える ─────────────────────────
  console.log('\n── POST /api/shipping でセットを登録 ──');
  {
    const r = await postShip('set-miss');
    ok(r.body.ok, '登録が通る');
    const mp = getMp.get('set-miss');
    eq(mp?.送料コード, '501', 'm_products に即時に入る (夜の rebuild を待たない)');
    eq(mp?.送料, 237, '送料の金額も入る');
    eq(mp?.配送方法, 'ネコポス', '配送方法も入る');

    const c = await call('get', '/api/missing/counts');
    eq(c.body.shipping, 3, '登録したセットはバッジから減る');
    const l = await call('get', '/api/missing/prioritized', { query: { type: 'shipping' } });
    ok(!l.body.rows.some(x => x.商品コード === 'set-miss'), '登録したセットは一覧から消える');
  }

  // ───────────────────────── 5. 登録時の商品名 (NE → m_products → 前回値) ─────────────────────────
  console.log('\n── 登録時の商品名の出どころ ──');
  {
    // 🚨 セットは NE の単品マスタに居ないことがある。居なければ m_products (セット名) から拾う
    eq(getPs.get('set-miss')?.product_name, '送料未登録のセット 2個セット',
      'NE に居ないセット → m_products のセット名 (登録済みマスタ検索で空にならない)');

    await postShip('set-blank');
    eq(getPs.get('set-blank')?.product_name, 'セット表にだけ名前がある',
      'NE に居ても商品名が空文字なら「無い」扱いで m_products の名前に落ちる');

    await postShip('set-both');
    eq(getPs.get('set-both')?.product_name, 'NE での名前',
      'NE に名前があればそれが優先 (単品の従来どおり)');

    await postShip('ghost');
    eq(getPs.get('ghost')?.product_name, '以前の名前',
      'どちらのマスタにも居なければ前回の登録の名前を残す (上書きで消さない)');
    eq(getPs.get('ghost')?.ship_cost, 237, '前回値に落ちるのは名前だけ。送料は今回の値');
  }

  // ───────────────────────── 6. 夜の rebuild をまたいでも残る ─────────────────────────
  console.log('\n── rebuild をもう一度 ──');
  {
    const r2 = await rebuildMProducts();
    ok(r2.ok, 'rebuild が成功する');
    const mp = getMp.get('set-miss');
    eq(mp?.送料コード, '501', '登録したセットの送料コードが rebuild 後も残る');
    eq(mp?.送料, 237, '送料の金額も残る');
    eq(mp?.配送方法, 'ネコポス', '配送方法も残る');
    const c = await call('get', '/api/missing/counts');
    eq(c.body.shipping, 1, 'rebuild 後もバッジは 1 (単品の未登録だけ)');
  }

  // ───────────────────────── 7. LIMIT 200 の境界 ─────────────────────────
  console.log('\n── 未登録が 200 件を超えたとき ──');
  {
    // 🚨 セットを含めた結果、一覧の LIMIT 200 に届きうる。バッジと CSV は全件、一覧だけ 200 件。
    //    同順位 (実績なし) は商品コード順で安定させる (開くたびに違う 200 件が出ない)
    db.transaction(() => {
      for (let i = 0; i < 205; i++) {
        insSet.run(`bulk-${String(i).padStart(3, '0')}`, `まとめて未登録 ${i}`, 600, 'tan-ok', 2, NOW);
      }
    })();
    const r3 = await rebuildMProducts();
    ok(r3.ok, 'rebuild が成功する (205 件のセットを追加)');

    const c = await call('get', '/api/missing/counts');
    eq(c.body.shipping, 206, 'バッジは全件 (単品 1 + セット 205)');
    const csv = await call('get', '/api/missing/download', { query: { type: 'shipping' } });
    eq(csvRows(csv.text).length, 206, 'CSV も全件');

    const l = await call('get', '/api/missing/prioritized', { query: { type: 'shipping' } });
    const codes = l.body.rows.map(x => x.商品コード);
    eq(codes.length, 200, '一覧は 200 件で打ち切る (既存どおり)');
    // 選ばれる 200 件そのものを固定する (bulk-000〜199)。tan-miss は商品コード順で後ろなので外に出る
    const expected = Array.from({ length: 200 }, (_, i) => `bulk-${String(i).padStart(3, '0')}`);
    ok(JSON.stringify(codes) === JSON.stringify(expected),
      '選ばれる 200 件は商品コード順の先頭 200 件に固定される (同順位でも開くたびに変わらない)');
  }
} finally {
  // ───────────────────────── 結果 ─────────────────────────
  db.close();
  fs.rmSync(tmpDir, { recursive: true, force: true });
}

console.log(`\n${failed === 0 ? '✅ 全テスト成功' : `❌ ${failed}件 失敗`}`);
process.exit(failed === 0 ? 0 : 1);
