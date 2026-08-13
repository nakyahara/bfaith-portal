// picking スタンドアロン用の warehouse-mirror.db を miniPC ローカルで構築する。
// 画像解決 (images.js) が読む2テーブルだけを、同一マシンの warehouse.db から充填する:
//   - mirror_rakuten_sku_map        ← f_rakuten_sku_map (全列)
//   - mirror_rakuten_item_daily     ← fact_rakuten_item_daily の DISTINCT item_manage_number
//     (画像解決は「実在する商品管理番号か」しか見ないため、日次値はダミーの1行/番号で足りる)
// Render の mirror をコピーしなくて済む = render ssh 不要。SKUが増えたら再実行 (冪等)。
// 実行: cd C:\tools\bfaith-picking && node build-mirror.mjs
process.env.DATA_DIR = process.env.DATA_DIR || 'C:\\tools\\bfaith-picking\\data';
import BetterSqlite3 from 'better-sqlite3';
const { initMirrorDB } = await import('./apps/warehouse-mirror/db.js');

const SRC = 'C:/Users/bfaith/bfaith-portal/data/warehouse.db';
const src = new BetterSqlite3(SRC, { readonly: true });
const mdb = initMirrorDB();   // スキーマは正規のDDLで作らせる

// 1. SKUマップ (ne_code → rakuten_code)
const skuRows = src.prepare('SELECT rakuten_code, ne_code, source, updated_at FROM f_rakuten_sku_map').all();
const insSku = mdb.prepare(`
  INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at)
  VALUES (@rakuten_code, @ne_code, @source, @updated_at)
  ON CONFLICT(rakuten_code) DO UPDATE SET ne_code=excluded.ne_code,
    source=excluded.source, updated_at=excluded.updated_at
`);
mdb.transaction(() => { for (const r of skuRows) insSku.run(r); })();

// 2. 実在する商品管理番号 (画像解決のハイフン削り照合用)
const mns = src.prepare(`
  SELECT DISTINCT item_manage_number FROM fact_rakuten_item_daily
  WHERE item_manage_number IS NOT NULL AND trim(item_manage_number) <> ''
`).all();
// NOT NULL列 (source_run_id/source_row_hash/synced_at) はローカル構築とわかる値で埋める
const insMn = mdb.prepare(`
  INSERT OR IGNORE INTO mirror_rakuten_item_daily
    (date_jst, item_manage_number, source_run_id, source_row_hash, synced_at)
  VALUES ('2026-08-13', @item_manage_number, 'local-build-mirror', @item_manage_number, datetime('now'))
`);
mdb.transaction(() => { for (const r of mns) insMn.run(r); })();

console.log(`sku_map: ${skuRows.length} 行 / manage_numbers: ${mns.length} 件 を反映`);
console.log('check:',
  mdb.prepare('SELECT COUNT(*) c FROM mirror_rakuten_sku_map').get().c, '/',
  mdb.prepare('SELECT COUNT(DISTINCT item_manage_number) c FROM mirror_rakuten_item_daily').get().c);
