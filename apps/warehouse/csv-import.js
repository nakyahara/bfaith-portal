/**
 * CSV投入スクリプト — NE商品マスタ / NE受注明細をwarehouse.dbに投入
 *
 * NEのCSVはcp932（Shift_JIS）エンコーディング。
 * iconv-liteでUTF-8に変換してからパース・INSERT。
 *
 * 使い方:
 *   node apps/warehouse/csv-import.js products <CSVファイルパス>
 *   node apps/warehouse/csv-import.js orders <CSVファイルパス> [<CSVファイルパス2> ...]
 */
import fs from 'fs';
import iconv from 'iconv-lite';
import { initDB, getDB, saveToFile, updateSyncMeta } from './db.js';

// ─── CSV パース（cp932対応）───

function readCsvFile(filePath) {
  const buf = fs.readFileSync(filePath);
  const text = iconv.decode(buf, 'cp932');
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
    if (values.length === headers.length) {
      rows.push(values);
    }
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
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        values.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  values.push(current);
  return values;
}

// ─── 商品マスタ投入 ───

function importProducts(filePath) {
  console.log(`[Import] 商品マスタ読み込み: ${filePath}`);
  const { headers, rows } = readCsvFile(filePath);

  console.log(`[Import] ヘッダー: ${headers.join(', ')}`);
  console.log(`[Import] データ行数: ${rows.length}`);

  const db = getDB();

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_products (
      商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分,
      代表商品コード, ロケーションコード, 配送業者, 発注ロット単位,
      最終仕入日, 商品分類タグ, 作成日, 在庫数, 引当数,
      最終更新日, 消費税率, 発注残数, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
  `);

  let count = 0;
  for (const row of rows) {
    const code = row[0]?.trim();
    if (!code) continue;

    stmt.run([
      code,                          // 商品コード
      row[1] || '',                  // 商品名
      row[2] || '',                  // 仕入先コード
      parseFloat(row[3]) || 0,       // 原価
      parseFloat(row[4]) || 0,       // 売価
      row[5] || '',                  // 取扱区分
      row[6] || '',                  // 代表商品コード
      row[7] || '',                  // ロケーションコード
      row[8] || '',                  // 配送業者
      parseInt(row[9]) || 0,         // 発注ロット単位
      row[10] || '',                 // 最終仕入日
      row[11] || '',                 // 商品分類タグ
      row[12] || '',                 // 作成日
      parseInt(row[13]) || 0,        // 在庫数
      parseInt(row[14]) || 0,        // 引当数
      row[15] || '',                 // 最終更新日
      parseFloat(row[16]) || 0,      // 消費税率
      parseInt(row[17]) || 0,        // 発注残数
    ]);
    count++;
  }
  stmt.free();
  saveToFile();

  updateSyncMeta('products_last_import', new Date().toISOString());
  updateSyncMeta('products_count', String(count));

  console.log(`[Import] 商品マスタ投入完了: ${count}件`);
  return count;
}

// ─── 受注明細投入 ───

function importOrders(filePath) {
  console.log(`[Import] 受注明細読み込み: ${filePath}`);
  const { headers, rows } = readCsvFile(filePath);

  console.log(`[Import] ヘッダー: ${headers.join(', ')}`);
  console.log(`[Import] データ行数: ${rows.length}`);

  const db = getDB();

  const stmt = db.prepare(`
    INSERT OR IGNORE INTO raw_ne_orders (
      伝票番号, 受注番号, 受注状態区分, 受注状態, 受注キャンセル,
      受注キャンセル日, 受注日, 店舗コード, 出荷確定日,
      明細行番号, レコードナンバー, キャンセル区分,
      商品コード, 商品名, 商品OP, 受注数, 引当数, 小計金額,
      synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
  `);

  let inserted = 0;
  let skipped = 0;

  for (const row of rows) {
    const denpyo = row[0]?.trim();
    const lineNo = parseInt(row[9]);
    if (!denpyo || isNaN(lineNo)) {
      skipped++;
      continue;
    }

    try {
      stmt.run([
        denpyo,                        // 伝票番号
        row[1] || '',                  // 受注番号
        row[2] || '',                  // 受注状態区分
        row[3] || '',                  // 受注状態
        row[4] || '',                  // 受注キャンセル
        row[5] || '',                  // 受注キャンセル日
        row[6] || '',                  // 受注日
        row[7] || '',                  // 店舗コード
        row[8] || '',                  // 出荷確定日
        lineNo,                        // 明細行番号
        row[10] || '',                 // レコードナンバー
        row[11] || '',                 // キャンセル区分
        row[12] || '',                 // 商品コード
        row[13] || '',                 // 商品名
        row[14] || '',                 // 商品OP
        parseInt(row[15]) || 0,        // 受注数
        parseInt(row[16]) || 0,        // 引当数
        parseFloat(row[17]) || 0,      // 小計金額
      ]);
      inserted++;
    } catch (e) {
      // 重複は INSERT OR IGNORE でスキップされる
      skipped++;
    }
  }
  stmt.free();
  saveToFile();

  console.log(`[Import] 受注明細投入完了: ${inserted}件挿入, ${skipped}件スキップ`);
  return { inserted, skipped };
}

// ─── セット商品投入 ───

function importSetProducts(filePath) {
  console.log(`[Import] セット商品読み込み: ${filePath}`);
  const { headers, rows } = readCsvFile(filePath);

  console.log(`[Import] ヘッダー: ${headers.join(', ')}`);
  console.log(`[Import] データ行数: ${rows.length}`);

  const db = getDB();

  // 全件洗い替え（UPSERT）
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_set_products (
      セット商品コード, セット商品名, セット販売価格,
      商品コード, 数量, セット在庫数, 代表商品コード, synced_at
    ) VALUES (?,?,?,?,?,?,?, datetime('now'))
  `);

  let count = 0;
  for (const row of rows) {
    const setCode = row[0]?.trim();
    const childCode = row[3]?.trim();
    if (!setCode || !childCode) continue;

    stmt.run([
      setCode,                       // セット商品コード
      row[1] || '',                  // セット商品名
      parseFloat(row[2]) || 0,       // セット販売価格
      childCode,                     // 商品コード（構成品）
      parseInt(row[4]) || 1,         // 数量
      parseInt(row[5]) || 0,         // セット在庫数
      row[6] || '',                  // 代表商品コード
    ]);
    count++;
  }
  stmt.free();
  saveToFile();

  updateSyncMeta('set_products_last_import', new Date().toISOString());
  updateSyncMeta('set_products_count', String(count));

  console.log(`[Import] セット商品投入完了: ${count}件`);
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
    process.exit(1);
  }

  await initDB();

  if (command === 'products') {
    importProducts(files[0]);
  } else if (command === 'sets') {
    importSetProducts(files[0]);
  } else if (command === 'orders') {
    let totalInserted = 0;
    let totalSkipped = 0;
    for (const file of files) {
      const result = importOrders(file);
      totalInserted += result.inserted;
      totalSkipped += result.skipped;
    }
    updateSyncMeta('orders_last_import', new Date().toISOString());
    console.log(`[Import] 全ファイル合計: ${totalInserted}件挿入, ${totalSkipped}件スキップ`);
  } else {
    console.error(`不明なコマンド: ${command}`);
    console.log('有効なコマンド: products, sets, orders');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('エラー:', e.message);
  process.exit(1);
});
