/**
 * test-sales-class-set.mjs — セット商品の売上分類 導出／未登録一覧のテスト
 *
 * 対象:
 *   rebuild-m-products.js  … resolveSetSalesClass / セット行への導出適用
 *   router.js              … recalcSetSalesClass / refreshSetSalesClasses
 *                            + /api/missing/* のセット包含
 *
 * 背景 (2026-08-07):
 *   セットの原価・税率は構成品から導出しているのに売上分類だけ手動登録のみで、
 *   登録漏れのセットが m_products に NULL のまま残り amazon-accounting の
 *   「その他/未分類」に落ちていた。さらに register の未登録一覧は 単品/例外 のみを
 *   見ていたため、その漏れが画面から永久に見えなかった。
 *
 * 実行: node apps/warehouse/test-sales-class-set.mjs
 * 本番 DB には触れない (一時 DATA_DIR に専用 warehouse.db を作り、終了時に削除)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sales-class-set-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { resolveSetSalesClass, rebuildMProducts } = await import('./rebuild-m-products.js');
const { recalcSetSalesClass, refreshSetSalesClasses } = await import('./router.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(a === b, `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

await initDB();
const db = getDB();
const NOW = '2026-08-07 12:00:00';

// ───────────────────────── 1. resolveSetSalesClass (単体) ─────────────────────────
console.log('\n── resolveSetSalesClass ──');
{
  const C = (salesClass, componentExists = true) => ({ salesClass, componentExists });

  eq(resolveSetSalesClass([C(1)]), 1, '単一構成品 → その値');
  eq(resolveSetSalesClass([C(3), C(1)]), 1, '1を含む → 1 (自社優先)');
  eq(resolveSetSalesClass([C(3), C(2)]), 2, '2と3 → 2');
  eq(resolveSetSalesClass([C(3), C(3)]), 3, '3のみ → 3');
  eq(resolveSetSalesClass([C(4), C(2)]), 2, '4(輸出)混在でも MIN');

  eq(resolveSetSalesClass([C(1), C(null)]), null, '一部NULL → null (誤確定させない)');
  eq(resolveSetSalesClass([C(1), C(undefined)]), null, '一部undefined → null');
  eq(resolveSetSalesClass([C(3), C(1, false)]), null, '構成品がNEに無い → null');
  eq(resolveSetSalesClass([]), null, '構成品0件 → null');
  eq(resolveSetSalesClass(null), null, '非配列 → null');
  eq(resolveSetSalesClass([C(0)]), null, '値域外(0) → null');
  eq(resolveSetSalesClass([C(5)]), null, '値域外(5) → null');
  eq(resolveSetSalesClass([C('2')]), 2, '文字列の "2" は 2 として扱う (SQLite の型ゆれ耐性)');
}

// ───────────────────────── 2. rebuild でのセット導出 ─────────────────────────
console.log('\n── rebuildMProducts: セット売上分類の導出 ──');

const insNe = db.prepare(`INSERT OR REPLACE INTO raw_ne_products
  (商品コード, 商品名, 原価, 売価, 取扱区分, 在庫数, 引当数, 消費税率, 作成日, synced_at)
  VALUES (?, ?, ?, ?, '取扱中', 0, 0, 10, '2026-01-01', ?)`);
const insSet = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products
  (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at)
  VALUES (?, ?, ?, ?, ?, ?)`);
const insClass = db.prepare(`INSERT OR REPLACE INTO product_sales_class
  (sku, sales_class, 商品名, synced_at) VALUES (?, ?, '', ?)`);

// 品質ゲート (総件数 3,000件未満は反映中止) を通すためのダミー単品
{
  const tx = db.transaction(() => {
    for (let i = 0; i < 3200; i++) insNe.run(`filler-${i}`, `ダミー${i}`, 100, 200, NOW);
  });
  tx();
}

// 構成品 (単品)
insNe.run('comp-jisha', '自社商品', 100, 300, NOW);      // 売上分類 1
insNe.run('comp-shiire', '仕入れ商品', 100, 300, NOW);   // 売上分類 3
insNe.run('comp-mitouroku', '未登録商品', 100, 300, NOW); // 売上分類なし
insClass.run('comp-jisha', 1, NOW);
insClass.run('comp-shiire', 3, NOW);

// set-derive: 構成品すべて登録済み → MIN(1,3)=1 が導出される
insNe.run('set-derive', '導出セット', 0, 900, NOW);
insSet.run('set-derive', '導出セット', 900, 'comp-jisha', 1, NOW);
insSet.run('set-derive', '導出セット', 900, 'comp-shiire', 1, NOW);

// set-partial: 構成品の一部が未登録 → 導出せず NULL (未登録一覧に出す)
insNe.run('set-partial', '一部未登録セット', 0, 900, NOW);
insSet.run('set-partial', '一部未登録セット', 900, 'comp-jisha', 1, NOW);
insSet.run('set-partial', '一部未登録セット', 900, 'comp-mitouroku', 1, NOW);

// set-manual: セット自身に手動登録あり → 導出値(1)ではなく手動値(2)が勝つ
insNe.run('set-manual', '手動登録セット', 0, 900, NOW);
insSet.run('set-manual', '手動登録セット', 900, 'comp-jisha', 1, NOW);
insClass.run('set-manual', 2, NOW);

// set-orphan: 構成品が NE 商品マスタに存在しない → NULL
insNe.run('set-orphan', '孤児構成セット', 0, 900, NOW);
insSet.run('set-orphan', '孤児構成セット', 900, 'comp-nai', 1, NOW);

const result = await rebuildMProducts();
ok(result.ok, `rebuild が成功する (${result.total}件)`);

const getMp = db.prepare('SELECT 商品区分, 売上分類 FROM m_products WHERE 商品コード = ?');
eq(getMp.get('set-derive')?.売上分類, 1, 'set-derive: 構成品 MIN(1,3) から 1 を導出');
eq(getMp.get('set-partial')?.売上分類, null, 'set-partial: 一部未登録 → NULL のまま');
eq(getMp.get('set-manual')?.売上分類, 2, 'set-manual: 手動登録が導出より優先');
eq(getMp.get('set-orphan')?.売上分類, null, 'set-orphan: 構成品がNEに無い → NULL');
eq(getMp.get('comp-mitouroku')?.売上分類, null, '単品の未登録はそのまま NULL');
eq(getMp.get('set-derive')?.商品区分, 'セット', 'セットとして投入されている');
ok(result.log.some(l => /売上分類を構成品から導出: \d+件/.test(l)), 'ログに導出件数が出る');

// ───────────────────────── 3. 未登録一覧がセットを含む ─────────────────────────
console.log('\n── /api/missing/* のセット包含 ──');
{
  // router.js と同じ条件で直接検証する (HTTP を立てずに SQL 条件だけを見る)
  const missing = db.prepare(`
    SELECT 商品コード FROM m_products
    WHERE 商品区分 IN ('単品', '例外', 'セット') AND 売上分類 IS NULL
    ORDER BY 商品コード
  `).all().map(r => r.商品コード);

  ok(missing.includes('set-partial'), '一部未登録セットが未登録一覧に出る');
  ok(missing.includes('set-orphan'), '孤児構成セットが未登録一覧に出る');
  ok(missing.includes('comp-mitouroku'), '未登録の単品も従来どおり出る');
  ok(!missing.includes('set-derive'), '導出できたセットは一覧に出ない');
  ok(!missing.includes('set-manual'), '手動登録済みセットは一覧に出ない');

  // 旧条件 (単品/例外のみ) ではセットが1件も拾えなかったことを固定する
  const oldCond = db.prepare(`
    SELECT COUNT(*) cnt FROM m_products
    WHERE 商品区分 IN ('単品', '例外') AND 売上分類 IS NULL AND 商品コード LIKE 'set-%'
  `).get().cnt;
  eq(oldCond, 0, '旧条件ではセットが0件 = これが不可視だった原因');
}

// ───────────────────────── 4. 構成品登録の即時反映 ─────────────────────────
console.log('\n── refreshSetSalesClasses (rebuild を待たない即時反映) ──');
{
  // set-partial の欠けていた構成品を登録 → 親セットが 1 になる
  insClass.run('comp-mitouroku', 2, NOW);
  db.prepare('UPDATE m_products SET 売上分類 = 2 WHERE 商品コード = ?').run('comp-mitouroku');
  const updated = refreshSetSalesClasses(db, ['comp-mitouroku'], NOW);
  ok(updated >= 1, `親セットが更新される (${updated}件)`);
  eq(getMp.get('set-partial')?.売上分類, 1, 'set-partial が MIN(1,2)=1 に埋まる');
  eq(getMp.get('set-manual')?.売上分類, 2, '無関係なセットは変わらない');

  // 構成品の登録を外すと親セットも NULL に戻る (未登録一覧へ再出現)
  db.prepare('DELETE FROM product_sales_class WHERE sku = ?').run('comp-mitouroku');
  refreshSetSalesClasses(db, ['comp-mitouroku'], NOW);
  eq(getMp.get('set-partial')?.売上分類, null, '構成品の登録を外すと親セットも NULL に戻る');

  // セット自身の手動登録を外すと導出値に戻る
  db.prepare('DELETE FROM product_sales_class WHERE sku = ?').run('set-manual');
  recalcSetSalesClass(db, 'set-manual', NOW);
  eq(getMp.get('set-manual')?.売上分類, 1, '手動登録を外すと構成品からの導出値(1)に戻る');
}

// ───────────────────────── 結果 ─────────────────────────
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });

console.log(`\n${failed === 0 ? '✅ 全テスト成功' : `❌ ${failed}件 失敗`}`);
process.exit(failed === 0 ? 0 : 1);
