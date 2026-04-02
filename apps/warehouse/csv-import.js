/**
 * CSV投入スクリプト — warehouse.dbに各種CSVを投入
 *
 * better-sqlite3使用（ファイルベース、メモリ制限なし）
 * NEのCSVはcp932（Shift_JIS）エンコーディング。自動変換。
 *
 * 使い方:
 *   node apps/warehouse/csv-import.js products <CSVファイル>
 *   node apps/warehouse/csv-import.js sets <CSVファイル>
 *   node apps/warehouse/csv-import.js orders <CSV1> [<CSV2> ...]
 *   node apps/warehouse/csv-import.js skumap <CSVファイル>
 *   node apps/warehouse/csv-import.js logizard <CSVファイル>
 *   node apps/warehouse/csv-import.js shipping_rates <CSVファイル>
 *   node apps/warehouse/csv-import.js product_shipping <CSVファイル>
 *   node apps/warehouse/csv-import.js exception_genka <CSVファイル>
 */
import fs from 'fs';
import iconv from 'iconv-lite';
import { initDB, getDB, saveToFile, updateSyncMeta } from './db.js';

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }

// ─── CSV パース ───

function readCsvFile(filePath) {
  const buf = fs.readFileSync(filePath);
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    // UTF-8 BOM
    text = buf.toString('utf-8').substring(1);
  } else {
    // まずUTF-8として試す
    const utf8 = buf.toString('utf-8');
    // replacement charの出現率で判定（少数なら部分的な文字化けでUTF-8が正しい）
    const replacementCount = (utf8.match(/\ufffd/g) || []).length;
    const totalChars = utf8.length;
    if (replacementCount > totalChars * 0.01) {
      // 1%以上化ける → cp932
      text = iconv.decode(buf, 'cp932');
    } else {
      text = utf8;
    }
  }
  return parseCsv(text);
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = parseRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const values = parseRow(line);
    if (values.length === headers.length) rows.push(values);
  }
  return { headers, rows };
}

function parseRow(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; }
        else inQuotes = false;
      } else current += ch;
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ',') { values.push(current); current = ''; }
      else current += ch;
    }
  }
  values.push(current);
  return values;
}

// ─── 商品マスタ投入 ───

function importProducts(filePath) {
  console.log(`[Import] 商品マスタ読み込み: ${filePath}`);
  const { rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_products (
      商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分,
      代表商品コード, ロケーションコード, 配送業者, 発注ロット単位,
      最終仕入日, 商品分類タグ, 作成日, 在庫数, 引当数,
      最終更新日, 消費税率, 発注残数, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const code = row[0]?.trim();
      if (!code) continue;
      stmt.run(code, row[1]||'', row[2]||'', parseFloat(row[3])||0, parseFloat(row[4])||0,
        row[5]||'', row[6]||'', row[7]||'', row[8]||'', parseInt(row[9])||0,
        row[10]||'', row[11]||'', row[12]||'', parseInt(row[13])||0, parseInt(row[14])||0,
        row[15]||'', parseFloat(row[16])||0, parseInt(row[17])||0, now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('products_last_import', new Date().toISOString());
  updateSyncMeta('products_count', String(count));
  console.log(`[Import] 商品マスタ投入完了: ${count}件`);
  return count;
}

// ─── 受注明細投入 ───

function importOrders(filePath) {
  console.log(`[Import] 受注明細読み込み: ${filePath}`);
  const { rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  const stmt = db.prepare(`
    INSERT OR IGNORE INTO raw_ne_orders (
      伝票番号, 受注番号, 受注状態区分, 受注状態, 受注キャンセル,
      受注キャンセル日, 受注日, 店舗コード, 出荷確定日,
      明細行番号, レコードナンバー, キャンセル区分,
      商品コード, 商品名, 商品OP, 受注数, 引当数, 小計金額, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let inserted = 0, skipped = 0;
    for (const row of rows) {
      const denpyo = row[0]?.trim();
      const lineNo = parseInt(row[9]);
      if (!denpyo || isNaN(lineNo)) { skipped++; continue; }
      try {
        stmt.run(denpyo, row[1]||'', row[2]||'', row[3]||'', row[4]||'',
          row[5]||'', row[6]||'', row[7]||'', row[8]||'',
          lineNo, row[10]||'', row[11]||'', row[12]||'', row[13]||'',
          row[14]||'', parseInt(row[15])||0, parseInt(row[16])||0, parseFloat(row[17])||0, now());
        inserted++;
      } catch { skipped++; }
    }
    return { inserted, skipped };
  });

  const result = tx();
  console.log(`[Import] 受注明細投入完了: ${result.inserted}件挿入, ${result.skipped}件スキップ`);
  return result;
}

// ─── セット商品投入 ───

function importSetProducts(filePath) {
  console.log(`[Import] セット商品読み込み: ${filePath}`);
  const { rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_set_products (
      セット商品コード, セット商品名, セット販売価格,
      商品コード, 数量, セット在庫数, 代表商品コード, synced_at
    ) VALUES (?,?,?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const setCode = row[0]?.trim();
      const childCode = row[3]?.trim();
      if (!setCode || !childCode) continue;
      stmt.run(setCode, row[1]||'', parseFloat(row[2])||0, childCode,
        parseInt(row[4])||1, parseInt(row[5])||0, row[6]||'', now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('set_products_last_import', new Date().toISOString());
  updateSyncMeta('set_products_count', String(count));
  console.log(`[Import] セット商品投入完了: ${count}件`);
  return count;
}

// ─── SKUマッピング投入 ───

function importSkuMap(filePath) {
  console.log(`[Import] SKUマッピング読み込み: ${filePath}`);
  // ヘッダーに改行を含むカラムがあるため、通常のCSVパーサーではなく
  // 各行を直接パースする（A-E列の5列だけ使う）
  const buf = fs.readFileSync(filePath);
  const text = buf.toString('utf-8');
  const allLines = text.split(/\r?\n/);

  // ヘッダー部分をスキップ（最初のskuで始まる行以降）
  const rows = [];
  let headerFound = false;
  for (const line of allLines) {
    if (!headerFound) {
      if (line.startsWith('sku,')) { headerFound = true; continue; }
      continue;
    }
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('"') || !trimmed.includes(',')) continue;
    const parts = parseRow(trimmed);
    if (parts.length >= 5 && parts[0] && !parts[0].startsWith('直近') && !parts[0].startsWith('売上')) {
      rows.push(parts);
    }
  }

  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  db.exec('DELETE FROM sku_map');
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO sku_map (seller_sku, asin, 商品名, ne_code, 数量, synced_at)
    VALUES (?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const sku = row[0]?.trim();
      if (!sku) continue;
      stmt.run(sku, row[1]||'', row[2]||'', row[3]||'', parseInt(row[4])||1, now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('sku_map_last_import', new Date().toISOString());
  updateSyncMeta('sku_map_count', String(count));
  console.log(`[Import] SKUマッピング投入完了: ${count}件`);
  return count;
}

// ─── ロジザード在庫投入 ───

function importLogizard(filePath) {
  console.log(`[Import] ロジザード在庫読み込み: ${filePath}`);
  const { headers, rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();
  const col = (name) => headers.indexOf(name);

  db.exec('DELETE FROM raw_lz_inventory');
  const stmt = db.prepare(`
    INSERT INTO raw_lz_inventory (
      商品ID, 商品名, バーコード, ブロック略称, ロケ,
      品質区分名, 有効期限, 入荷日, 在庫数, 引当数,
      ロケ業務区分, 商品予備項目004, 最終入荷日, 最終出荷日,
      ブロック引当順, 在庫日, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const productId = row[col('商品ID')]?.trim();
      if (!productId) continue;
      stmt.run(productId, row[col('商品名')]||'', row[col('バーコード')]||'',
        row[col('ブロック略称')]||'', row[col('ロケ')]||'', row[col('品質区分名')]||'',
        row[col('有効期限')]||'', row[col('入荷日')]||'',
        parseInt(row[col('在庫数(引当数を含む)')])||0, parseInt(row[col('引当数')])||0,
        row[col('ロケ業務区分')]||'', row[col('商品予備項目００４')]||'',
        row[col('最終入荷日')]||'', row[col('最終出荷日')]||'',
        row[col('ブロック引当順')]||'', row[col('在庫日')]||'', now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('logizard_last_import', new Date().toISOString());
  updateSyncMeta('logizard_count', String(count));
  console.log(`[Import] ロジザード在庫投入完了: ${count}件`);
  return count;
}

// ─── 配送区分マスタ投入 ───

function importShippingRates(filePath) {
  console.log(`[Import] 配送区分マスタ読み込み: ${filePath}`);
  const { headers, rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();
  const col = (name) => headers.indexOf(name);

  db.exec('DELETE FROM shipping_rates');
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO shipping_rates (
      shipping_code, 大分類区分, 運送会社, 小分類区分名称, 梱包サイズ,
      最大重量, 追跡有無, 送料, 出荷作業料, 想定梱包資材費,
      想定人件費, 配送関係費合計, 備考, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, ?)
  `);

  // 固定位置: 0=空, 1=大分類区分, 2=運送会社, 3=配送方法コード, 4=小分類区分名称,
  // 5=梱包サイズ, 6=最大重量, 7=追跡有無, 8=集荷or持込, 9=配送日数, 10=配達方法,
  // 11=保管料, 12=出荷作業料, 13=想定梱包資材費, 14=送料（税込み）, 15=想定人件費, 16=配送関係費合計, 17=備考
  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const code = row[3]?.trim();
      if (!code) continue;
      stmt.run(code, row[1]||'', row[2]||'', row[4]||'', row[5]||'',
        row[6]||'', row[7]||'',
        parseFloat(row[14])||0, parseFloat(row[12])||0,
        parseFloat(row[13])||0, parseFloat(row[15])||0,
        parseFloat(row[16])||0, row[17]||'', now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('shipping_rates_last_import', new Date().toISOString());
  console.log(`[Import] 配送区分マスタ投入完了: ${count}件`);
  return count;
}

// ─── 商品別送料マスタ投入 ───

function importProductShipping(filePath) {
  console.log(`[Import] 商品別送料マスタ読み込み: ${filePath}`);
  const { rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  db.exec('DELETE FROM product_shipping');
  const stmt = db.prepare(`
    INSERT INTO product_shipping (sku, product_name, shipping_code, ship_method, ship_cost, note, synced_at)
    VALUES (?,?,?,?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const sku = row[0]?.trim();
      if (!sku) continue;
      stmt.run(sku, row[1]||'', row[2]||'', row[3]||'', parseFloat(row[4])||0, row[5]||'', now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('product_shipping_last_import', new Date().toISOString());
  console.log(`[Import] 商品別送料マスタ投入完了: ${count}件`);
  return count;
}

// ─── 特殊商品原価投入 ───

function importExceptionGenka(filePath) {
  console.log(`[Import] 特殊商品原価読み込み: ${filePath}`);
  const { rows } = readCsvFile(filePath);
  console.log(`[Import] データ行数: ${rows.length}`);
  const db = getDB();

  db.exec('DELETE FROM exception_genka');
  const stmt = db.prepare(`
    INSERT INTO exception_genka (sku, genka, 商品名, synced_at)
    VALUES (?,?,?, ?)
  `);

  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const sku = row[0]?.trim();
      if (!sku) continue;
      stmt.run(sku, parseFloat(row[1])||0, row[2]||'', now());
      count++;
    }
    return count;
  });

  const count = tx();
  updateSyncMeta('exception_genka_last_import', new Date().toISOString());
  console.log(`[Import] 特殊商品原価投入完了: ${count}件`);
  return count;
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];
  const files = args.slice(1);

  if (!command || files.length === 0) {
    console.log('使い方:');
    console.log('  node apps/warehouse/csv-import.js products <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js sets <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js orders <CSV1> [<CSV2> ...]');
    console.log('  node apps/warehouse/csv-import.js skumap <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js logizard <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js shipping_rates <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js product_shipping <CSVファイル>');
    console.log('  node apps/warehouse/csv-import.js exception_genka <CSVファイル>');
    process.exit(1);
  }

  await initDB();

  const handlers = {
    products: () => importProducts(files[0]),
    sets: () => importSetProducts(files[0]),
    skumap: () => importSkuMap(files[0]),
    logizard: () => importLogizard(files[0]),
    shipping_rates: () => importShippingRates(files[0]),
    product_shipping: () => importProductShipping(files[0]),
    exception_genka: () => importExceptionGenka(files[0]),
    orders: () => {
      let totalInserted = 0, totalSkipped = 0;
      for (const file of files) {
        const result = importOrders(file);
        totalInserted += result.inserted;
        totalSkipped += result.skipped;
      }
      updateSyncMeta('orders_last_import', new Date().toISOString());
      console.log(`[Import] 全ファイル合計: ${totalInserted}件挿入, ${totalSkipped}件スキップ`);
    },
  };

  if (handlers[command]) {
    handlers[command]();
  } else {
    console.error(`不明なコマンド: ${command}`);
    console.log(`有効なコマンド: ${Object.keys(handlers).join(', ')}`);
    process.exit(1);
  }
}

main().catch(e => {
  console.error('エラー:', e.message);
  process.exit(1);
});
