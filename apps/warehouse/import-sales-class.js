import Database from 'better-sqlite3';
import fs from 'fs';
import { legacyCliGate, runWithLegacyCliLock } from '../../lib/master-legacy-gate.mjs';

// 🚨 売上分類 (product_sales_class) はマスタ = 古い入口の門を通す (Company DB構想 10 §4 #10・14 §9 M2・契約 v3 H1)。
//    切替の段階が legacy_open のときだけ今までどおり。frozen 以降・段階が読めない = 何も書かないで終了コード 3
if (await legacyCliGate('cli:import-sales-class.js')) {
  const db = new Database('data/warehouse.db');
  const buf = fs.readFileSync('data/import/sales_class.csv');
  const text = buf.toString('utf-8');
  const lines = text.split(/\r?\n/).filter(l => l.trim());

  const stmt = db.prepare('INSERT OR REPLACE INTO product_sales_class (sku, sales_class, 商品名, synced_at) VALUES (?, ?, ?, ?)');
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  let count = 0, skipped = 0;

  const tx = db.transaction(() => {
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const sku = (cols[0] || '').trim().toLowerCase();
      if (!sku) { skipped++; continue; }
      const sc = parseInt(cols[4]);
      if (![1, 2, 3, 4].includes(sc)) { skipped++; continue; }
      const name = (cols[1] || '').trim().slice(0, 100);
      stmt.run(sku, sc, name, now);
      count++;
    }
  });
  // 書くところ = 段階の鍵を共有で持ったまま読み直し、legacy_open のときだけ書く (段階を変える関数は書き終わるまで待つ)
  const { ran } = await runWithLegacyCliLock('cli:import-sales-class.js', () => tx());
  if (ran) {
    console.log('取り込み完了:', count, '件, スキップ:', skipped, '件');
    console.log('product_sales_class:', db.prepare('SELECT COUNT(*) as cnt FROM product_sales_class').get().cnt, '件');
    const dist = db.prepare('SELECT sales_class, COUNT(*) as cnt FROM product_sales_class GROUP BY sales_class').all();
    for (const d of dist) console.log('  分類' + d.sales_class + ': ' + d.cnt + '件');
  }
  db.close();
}
