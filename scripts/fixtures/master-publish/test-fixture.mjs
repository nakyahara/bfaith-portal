/**
 * test-fixture.mjs — Company DB の写し (④a) の試験の材料 (scripts/test-master-publish.mjs)。
 * 🚨 PR の前のコード (origin/master c894ca64) で同じ材料を作り直した値 (GOLDEN) と比べるので、材料を変えたら前のコードで取り直す
 *   (取り直し方 = test-master-publish.mjs の GOLDEN の説明)。ここは本番のコードを import しない (前のコードからも使うため)
 */
import crypto from 'node:crypto';

export const T1 = '2026-09-25 07:01:14';   // 今回の NE の取得
export const T0 = '2026-09-20 07:01:00';   // 前の取得 (今回の取得に無い古い行)
/** 作り直しで書かれる・読まれる業務の表 (行を全部・時刻の列も含めてハッシュする) */
export const BUSINESS_TABLES = Object.freeze(['m_products', 'm_set_components', 'exception_genka', 'product_shipping', 'product_tax_rate', 'product_sales_class', 'm_reorder_setting']);
/** 作り直しを流す時刻 (updated_at などが同じになるように止める) */
export const FROZEN_AT = '2026-09-30T00:00:00.000Z';

/** NE の取得と上書き表 (3,100 件の埋め草 + 単品・セット・例外の決め方を一通り) */
export function seedNe(db, readNeRawRev) {
  const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード, 在庫数, 引当数, 消費税率, 作成日, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 3, 1, ?, '2026-01-01', ?)`);
  const insSet = db.prepare('INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)');
  db.transaction(() => {
    for (let i = 0; i < 3100; i++) insNe.run(`filler-${i}`, `ダミー${i}`, '0009', 100 + (i % 7), 300, '取扱中', '', 10, T1);   // 品質ゲート (3,000 件)
    // 単品
    insNe.run('s-ne', 'NE の単品', '0001', 100, 300, '取扱中', '', 10, T1);
    insNe.run('s-exc', '原価が空の単品', '0002', 0, 500, '取扱中', '', 10, T1);   // NE の原価 0 → 例外原価
    insNe.run('s-tax8', '軽減税率の単品', '0001', 80, 200, '取扱中', '', 8, T1);
    insNe.run('s-taxfb', '税率を補う単品', '0001', 90, 200, '取扱中', '', 0, T1);   // product_tax_rate で補う
    insNe.run('s-taxunk', '税率が決まらない単品', '0001', 90, 200, '取扱中', '', 0, T1);
    insNe.run('s-stop', '取扱中止の単品', '0003', 50, 100, '取扱中止', '', 10, T1);
    insNe.run('s-maker', 'メーカー中止の単品', '0003', 60, 100, 'ﾒｰｶｰ取扱中止', '', 10, T1);
    insNe.run('s-ship', '送料つきの単品', '0001', 70, 900, '取扱中', '', 10, T1);
    insNe.run('s-inh', '代表から送料を継ぐ単品', '0001', 75, 900, '取扱中', 'rep-x', 10, T1);
    insNe.run('s-sc', '売上分類 1 の単品', '0001', 40, 100, '取扱中', '', 10, T1);
    insNe.run('s-sc3', '売上分類 3 の単品', '0004', 45, 100, '取扱中', '', 8, T1);
    insNe.run('s-blank', '', '0001', 10, 20, '取扱中', '', 10, T1);
    insNe.run('s-gone', '今回の取得に無い単品', '0001', 10, 20, '取扱中', '', 10, T0);
    insNe.run('s-sup', '仕入先コードが 0 埋めでない単品', '1', 33, 99, '取扱中', '', 10, T1);
    insNe.run('full', 'ASCII の full', '0001', 11, 22, '取扱中', '', 10, T1);   // 正規化すると 'ｆｕｌｌ' と重なる (ロードは先の full を採る)
    insNe.run('ｆｕｌｌ', '全角の full', '0001', 12, 22, '取扱中', '', 10, T1);
    // セット (NE の商品にもある = 売価・仕入先を持つ)
    insNe.run('set-e', '', '0005', 0, 950, '取扱中', '', 10, T1);
    insNe.run('set-h', 'セット H', '0005', 0, 1200, '取扱中', '', 10, T1);
    insSet.run('set-a', 'セット A', 800, 's-ne', 2, T1); insSet.run('set-a', 'セット A', 800, 's-exc', 1, T1);
    insSet.run('set-b', 'セット B', 700, 's-ne', 1, T1); insSet.run('set-b', 'セット B', 700, 's-tax8', 1, T1);
    insSet.run('set-c', 'セット C', 600, 's-sc', 1, T1); insSet.run('set-c', 'セット C', 600, 's-sc3', 1, T1); insSet.run('set-c', 'セット C', 600, 's-stop', 1, T1);
    insSet.run('set-d', 'セット D', 500, 's-ne', 1, T1);
    insSet.run('set-e', '', 400, 's-tax8', 3, T1);
    insSet.run('set-f', 'セット F', 300, 's-ne', 1, T1); insSet.run('set-f', 'セット F', 300, 'ghost', 1, T1);
    insSet.run('set-g', 'セット G', 200, 's-sc3', 2, T1);
    insSet.run('set-h', 'セット H', 1100, 's-maker', 1, T1); insSet.run('set-h', 'セット H', 1100, 's-ship', 1, T1);
  })();
  db.prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('s-exc', 555, NULL, 'x'), ('set-d', 777, NULL, 'x'), ('ex-only', 321, '例外だけの商品', 'x')").run();
  db.prepare("INSERT OR REPLACE INTO product_tax_rate (sku, tax_rate, synced_at) VALUES ('s-taxfb', 0.08, 'x'), ('ex-only', 0.1, 'x')").run();
  db.prepare("INSERT OR REPLACE INTO product_sales_class (sku, sales_class, synced_at) VALUES ('s-sc', 1, 'x'), ('s-sc3', 3, 'x'), ('set-g', 2, 'x'), ('ex-only', 2, 'x')").run();
  db.prepare(`INSERT OR REPLACE INTO product_shipping (sku, shipping_code, ship_method, ship_cost, synced_at) VALUES
    ('s-ship', 'S1', 'ネコポス', 200, 'x'), ('rep-x', 'S2', '宅急便', 800, 'x'), ('set-h', 'S3', '宅急便コンパクト', 600, 'x'), ('ex-only', 'S1', 'ネコポス', 200, 'x')`).run();
  db.prepare("INSERT OR REPLACE INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES ('s-ne', 3, 'NE の単品', 'x', 'x'), ('set-a', 2, 'セット A', 'x', 'x')").run();
  markComplete(db, readNeRawRev);
}

/** NE の取込が最後まで終わった状態にする (ne-api.js と同じ: 印の時刻と、その時点の通し番号) */
export function markComplete(db, readNeRawRev) {
  const setMeta = (k, v) => db.prepare("INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, '')").run(k, v);
  setMeta('ne_api_products_complete_at', T1); setMeta('ne_api_products_complete_rev', String(readNeRawRev('products')));
  setMeta('ne_api_setproducts_complete_at', T1); setMeta('ne_api_setproducts_complete_rev', String(readNeRawRev('setproducts')));
}

/** 業務の表の行を全部 (時刻の列も) ハッシュする。表ごとと全体 */
export function tablesDigest(db, tables = BUSINESS_TABLES) {
  const per = {};
  for (const t of tables) {
    const lines = db.prepare(`SELECT * FROM main.${t}`).all().map((r) => JSON.stringify(Object.keys(r).sort().map((k) => [k, r[k]]))).sort();
    per[t] = { rows: lines.length, sha: crypto.createHash('sha256').update(lines.join('\n'), 'utf8').digest('hex') };
  }
  const all = crypto.createHash('sha256').update(tables.map((t) => `${t}:${per[t].sha}`).join('\n')).digest('hex');
  return { all, per };
}

/** この接続で今までに書き換えた行の数 (SQLite の total_changes()。TEMP の表・sync_meta も入る) */
export const totalChanges = (db) => db.prepare('SELECT total_changes() AS n').get().n;

/** fn の間だけ時計を止める (new Date() / Date.now() が FROZEN_AT を返す) */
export async function withFrozenDate(fn, iso = FROZEN_AT) {
  const Real = globalThis.Date;
  const t = Real.parse(iso);
  class Frozen extends Real {
    constructor(...a) { if (a.length) super(...a); else super(t); }
    static now() { return t; }
  }
  globalThis.Date = Frozen;
  try { return await fn(); } finally { globalThis.Date = Real; }
}

/**
 * 作り直しを 1 回、時計を止めて流し、業務の表のハッシュと書き換えた行の数を返す (PR の前と後で同じか = 何も変えていないことの確かめ)
 * @param {() => Promise<object>} rebuild
 */
export async function noopProbe(db, rebuild) {
  const before = totalChanges(db);
  const r = await withFrozenDate(rebuild);
  return { ok: r && r.ok, changes: totalChanges(db) - before, tables: tablesDigest(db) };
}
