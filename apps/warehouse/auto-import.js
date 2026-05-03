/**
 * 自動CSV取り込みスクリプト（better-sqlite3版）
 *
 * importフォルダを1分ごとに監視し、CSVファイルを検知したら自動投入する。
 * 投入完了後はdone/フォルダに移動。
 *
 * ファイル名ルール:
 *   products_*.csv / nedldata*.csv  → 商品マスタ（UPSERT上書き）
 *   orders_*.csv   / juchu*.csv     → 受注明細（追記蓄積）
 *   sets_*.csv     / set_*.csv      → セット商品（洗い替え）
 *   skumap*.csv    / sku_*.csv      → SKUマッピング（洗い替え）
 *   logi*.csv      / CZ*.csv        → ロジザード在庫（洗い替え）
 *   shipping_rates*.csv             → 配送区分マスタ
 *   product_shipping*.csv / 送料*.csv → 商品別送料
 *   exception_genka*.csv            → 特殊商品原価
 */
import fs from 'fs';
import path from 'path';
import iconv from 'iconv-lite';
import { initDB, getDB, saveToFile, updateSyncMeta } from './db.js';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const IMPORT_DIR = path.join(DATA_DIR, 'import');
const DONE_DIR = path.join(DATA_DIR, 'import', 'done');
const CHECK_INTERVAL = 60 * 1000;

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }

// ─── CSV パース ───

function readCsvFile(filePath) {
  const buf = fs.readFileSync(filePath);
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8').substring(1);
  } else {
    const utf8 = buf.toString('utf-8');
    const hasReplacementChar = utf8.includes('\ufffd');
    text = hasReplacementChar ? iconv.decode(buf, 'cp932') : utf8;
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

// ─── 投入処理 ───

function importProducts(filePath) {
  const { rows } = readCsvFile(filePath);
  const db = getDB();
  const stmt = db.prepare(`INSERT OR REPLACE INTO raw_ne_products (
    商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード,
    ロケーションコード, 配送業者, 発注ロット単位, 最終仕入日, 商品分類タグ,
    作成日, 在庫数, 引当数, 最終更新日, 消費税率, 発注残数, synced_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const code = (row[0]?.trim() || '').toLowerCase();
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
  updateSyncMeta('products_last_import', now());
  return count;
}

function importOrders(filePath) {
  const { rows } = readCsvFile(filePath);
  const db = getDB();
  const stmt = db.prepare(`INSERT OR IGNORE INTO raw_ne_orders (
    伝票番号, 受注番号, 受注状態区分, 受注状態, 受注キャンセル, 受注キャンセル日,
    受注日, 店舗コード, 出荷確定日, 明細行番号, レコードナンバー, キャンセル区分,
    商品コード, 商品名, 商品OP, 受注数, 引当数, 小計金額, synced_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => {
    let inserted = 0, skipped = 0;
    for (const row of rows) {
      const denpyo = row[0]?.trim();
      const lineNo = parseInt(row[9]);
      if (!denpyo || isNaN(lineNo)) { skipped++; continue; }
      try {
        stmt.run(denpyo, row[1]||'', row[2]||'', row[3]||'', row[4]||'',
          row[5]||'', row[6]||'', row[7]||'', row[8]||'',
          lineNo, row[10]||'', row[11]||'', (row[12] || '').toLowerCase(), row[13]||'',
          row[14]||'', parseInt(row[15])||0, parseInt(row[16])||0, parseFloat(row[17])||0, now());
        inserted++;
      } catch { skipped++; }
    }
    return { inserted, skipped };
  });
  return tx();
}

function importSetProducts(filePath) {
  const { rows } = readCsvFile(filePath);
  const db = getDB();
  const stmt = db.prepare(`INSERT OR REPLACE INTO raw_ne_set_products (
    セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, セット在庫数, 代表商品コード, synced_at
  ) VALUES (?,?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const setCode = (row[0]?.trim() || '').toLowerCase();
      const childCode = (row[3]?.trim() || '').toLowerCase();
      if (!setCode || !childCode) continue;
      stmt.run(setCode, row[1]||'', parseFloat(row[2])||0, childCode,
        parseInt(row[4])||1, parseInt(row[5])||0, row[6]||'', now());
      count++;
    }
    return count;
  });
  const count = tx();
  updateSyncMeta('set_products_last_import', now());
  return count;
}

function importSkuMap(filePath) {
  // SKU管理統合 Step 3b: sku_map writer 廃止。
  // SKUマスタ (m_sku_master + m_sku_components) を使用してください。
  console.warn(`[Auto] sku_map 書き込みは廃止されました (Step 3b)。SKUマスタ用CSVを使用してください。skipped: ${filePath}`);
  return 0;
}

function importLogizard(filePath) {
  const { headers, rows } = readCsvFile(filePath);
  const db = getDB();
  const col = (name) => headers.indexOf(name);
  db.exec('DELETE FROM raw_lz_inventory');
  const stmt = db.prepare(`INSERT INTO raw_lz_inventory (
    商品ID, 商品名, バーコード, ブロック略称, ロケ, 品質区分名, 有効期限, 入荷日,
    在庫数, 引当数, ロケ業務区分, 商品予備項目004, 最終入荷日, 最終出荷日,
    ブロック引当順, 在庫日, synced_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => {
    let count = 0;
    for (const row of rows) {
      const productId = (row[col('商品ID')]?.trim() || '').toLowerCase();
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
  updateSyncMeta('logizard_last_import', now());
  return count;
}

// ─── ファイル種類判定 ───

function detectType(filename) {
  const lower = filename.toLowerCase();
  if (lower.startsWith('products') || lower.startsWith('nedldata')) return 'products';
  if (lower.startsWith('orders') || lower.startsWith('juchu')) return 'orders';
  if (lower.startsWith('sets') || lower.startsWith('set_')) return 'sets';
  if (lower.startsWith('skumap') || lower.startsWith('sku_')) return 'skumap';
  if (lower.startsWith('logi') || lower.startsWith('cz')) return 'logizard';
  return null;
}

// ─── 監視ループ ───

function moveToDone(filePath) {
  if (!fs.existsSync(DONE_DIR)) fs.mkdirSync(DONE_DIR, { recursive: true });
  const filename = path.basename(filePath);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const dest = path.join(DONE_DIR, `${timestamp}_${filename}`);
  fs.renameSync(filePath, dest);
  return dest;
}

function checkAndImport() {
  if (!fs.existsSync(IMPORT_DIR)) return;
  const files = fs.readdirSync(IMPORT_DIR).filter(f => f.toLowerCase().endsWith('.csv')).sort();
  if (files.length === 0) return;

  for (const filename of files) {
    const filePath = path.join(IMPORT_DIR, filename);
    const stat1 = fs.statSync(filePath);
    if (Date.now() - stat1.mtimeMs < 5000) continue;

    const type = detectType(filename);
    if (!type) { console.log(`[Auto] 不明なファイル名: ${filename} → スキップ`); continue; }

    console.log(`[Auto] 検知: ${filename} → ${type}`);
    try {
      let result;
      if (type === 'products') { result = importProducts(filePath); console.log(`[Auto] 商品マスタ: ${result}件`); }
      else if (type === 'orders') { result = importOrders(filePath); console.log(`[Auto] 受注明細: ${result.inserted}件挿入, ${result.skipped}件スキップ`); }
      else if (type === 'sets') { result = importSetProducts(filePath); console.log(`[Auto] セット商品: ${result}件`); }
      else if (type === 'skumap') { result = importSkuMap(filePath); console.log(`[Auto] SKUマップ: ${result}件`); }
      else if (type === 'logizard') { result = importLogizard(filePath); console.log(`[Auto] ロジザード: ${result}件`); }
      const dest = moveToDone(filePath);
      console.log(`[Auto] 移動: → ${dest}`);
    } catch (e) {
      console.error(`[Auto] エラー (${filename}): ${e.message}`);
    }
  }
}

// ─── メイン ───

async function main() {
  console.log('[Auto] データウェアハウス自動取込を開始します');
  console.log(`[Auto] 監視フォルダ: ${IMPORT_DIR}`);
  console.log(`[Auto] チェック間隔: ${CHECK_INTERVAL / 1000}秒`);
  if (!fs.existsSync(IMPORT_DIR)) fs.mkdirSync(IMPORT_DIR, { recursive: true });
  await initDB();
  checkAndImport();
  setInterval(checkAndImport, CHECK_INTERVAL);
  console.log('[Auto] 監視中... (Ctrl+C で停止)');
}

main().catch(e => { console.error('[Auto] 起動エラー:', e.message); process.exit(1); });
