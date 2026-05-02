/**
 * 商品コード変換テーブルCSVから m_sku_master / m_sku_components を初期投入する。
 *
 * CSV仕様: ヘッダ19列、CP932エンコーディング
 *   col 0: sku            → seller_sku（小文字正規化）
 *   col 2: 商品名         → m_sku_master.商品名（A案で集約）
 *   col 3: NE商品コード   → ne_code（小文字正規化）
 *   col 4: 数量           → 数量（既定1）
 *
 * A案ロジック:
 *   同一 seller_sku に対して 商品名 が複数現れた場合、最初の行の商品名を採用。
 *   差異がある場合は警告ログに出力（インポート自体は通す）。
 *
 * 例外検知:
 *   - seller_sku 空 / NE商品コード 空 / 数量 0以下: 行スキップ＋警告
 *   - ne_code が raw_ne_products に存在しない: 警告のみ（DB CHECK制約は通る）
 *   - 同一 (seller_sku, ne_code) の重複: 数量を合算せず最初の1件採用＋警告
 *
 * 使い方:
 *   node apps/warehouse/import-sku-master.js <csv_path> [--dry-run] [--encoding utf-8|cp932]
 *
 * --dry-run: トランザクションをロールバック、結果のみ表示
 */
import fs from 'fs';
import path from 'path';
import iconv from 'iconv-lite';
import { getDB, initDB } from './db.js';

/**
 * RFC4180準拠の簡易CSVパーサ
 * - quoted field（"..."）対応、内部の "" は " にエスケープ展開
 * - quoted field 内のカンマ・改行はそのまま保持
 * - 空行はスキップ
 */
function parseCSV(text) {
  const rows = [];
  let row = [];
  let field = '';
  let i = 0;
  let inQuoted = false;
  const n = text.length;

  while (i < n) {
    const c = text[i];

    if (inQuoted) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuoted = false; i++; continue;
      }
      field += c; i++; continue;
    }

    if (c === '"') { inQuoted = true; i++; continue; }
    if (c === ',') { row.push(field); field = ''; i++; continue; }
    if (c === '\r') { i++; continue; }
    if (c === '\n') {
      row.push(field); field = '';
      if (row.length > 1 || row[0] !== '') rows.push(row);
      row = [];
      i++; continue;
    }
    field += c; i++;
  }
  // 最終行
  if (field !== '' || row.length > 0) {
    row.push(field);
    if (row.length > 1 || row[0] !== '') rows.push(row);
  }
  return rows;
}

const COL_SKU = 0;
const COL_NAME = 2;
const COL_NE = 3;
const COL_QTY = 4;

function normalizeCode(s) {
  return (s ?? '').toString().trim().toLowerCase();
}

/**
 * @param {string} csvPath
 * @param {object} opts { dryRun: boolean, encoding: string }
 * @returns {{
 *   masterCount: number,
 *   componentCount: number,
 *   skipped: number,
 *   warnings: string[],
 *   nameConflicts: Array<{seller_sku: string, names: string[]}>,
 *   missingNeCodes: Array<{seller_sku: string, ne_code: string}>,
 *   dupComponents: Array<{seller_sku: string, ne_code: string}>
 * }}
 */
export function importSkuMasterCSV(csvPath, opts = {}) {
  const { dryRun = false, encoding = 'utf-8' } = opts;
  if (!fs.existsSync(csvPath)) throw new Error(`CSVが見つかりません: ${csvPath}`);

  const buf = fs.readFileSync(csvPath);
  const text = iconv.decode(buf, encoding);
  const rows = parseCSV(text);

  const header = rows[0];
  if (!header || header.length < 5) {
    throw new Error('CSVヘッダが想定と違います');
  }

  // 集約用構造
  const masters = new Map(); // seller_sku -> { 商品名, names: Set<string> }
  const componentsBySku = new Map(); // seller_sku -> Map<ne_code, {数量, sort_order}>

  const warnings = [];
  const missingNeCodes = [];
  const dupComponents = [];
  let skipped = 0;

  // 行を集約（A案: 最初に現れた商品名を採用、差異あれば警告）
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    if (!row || row.length === 0) continue;

    const seller_sku = normalizeCode(row[COL_SKU]);
    const ne_code = normalizeCode(row[COL_NE]);
    const 商品名 = (row[COL_NAME] ?? '').toString().trim();
    const qtyRaw = (row[COL_QTY] ?? '').toString().trim();
    const 数量 = qtyRaw === '' ? 1 : parseInt(qtyRaw, 10);

    if (!seller_sku) { skipped++; warnings.push(`L${i + 1}: sku 空のためスキップ`); continue; }
    if (!ne_code) { skipped++; warnings.push(`L${i + 1}: ne_code 空のためスキップ (sku=${seller_sku})`); continue; }
    if (!商品名) { skipped++; warnings.push(`L${i + 1}: 商品名 空のためスキップ (sku=${seller_sku})`); continue; }
    if (!Number.isFinite(数量) || 数量 <= 0) { skipped++; warnings.push(`L${i + 1}: 数量不正(${qtyRaw}) のためスキップ (sku=${seller_sku})`); continue; }

    // master 集約
    if (!masters.has(seller_sku)) {
      masters.set(seller_sku, { 商品名, names: new Set([商品名]) });
    } else {
      const m = masters.get(seller_sku);
      m.names.add(商品名);
      // 商品名は最初の行優先（A案）→ 上書きしない
    }

    // components 集約
    if (!componentsBySku.has(seller_sku)) {
      componentsBySku.set(seller_sku, new Map());
    }
    const comps = componentsBySku.get(seller_sku);
    if (comps.has(ne_code)) {
      dupComponents.push({ seller_sku, ne_code });
      warnings.push(`L${i + 1}: (${seller_sku}, ${ne_code}) 重複、最初の1件のみ採用`);
      continue;
    }
    comps.set(ne_code, { 数量, sort_order: comps.size });
  }

  // 商品名 conflict 抽出
  const nameConflicts = [];
  for (const [sku, m] of masters.entries()) {
    if (m.names.size > 1) {
      nameConflicts.push({ seller_sku: sku, names: [...m.names] });
    }
  }

  // DB書き込み
  const db = getDB();
  const ins発生 = db.prepare(`
    INSERT OR REPLACE INTO m_sku_master (seller_sku, 商品名, created_by)
    VALUES (?, ?, 'csv-import')
  `);
  const insComp = db.prepare(`
    INSERT OR REPLACE INTO m_sku_components (seller_sku, ne_code, 数量, sort_order)
    VALUES (?, ?, ?, ?)
  `);
  const checkNe = db.prepare(`SELECT 1 FROM raw_ne_products WHERE 商品コード = ?`);

  let masterCount = 0;
  let componentCount = 0;

  const tx = db.transaction(() => {
    for (const [sku, m] of masters.entries()) {
      ins発生.run(sku, m.商品名);
      masterCount++;

      const comps = componentsBySku.get(sku);
      for (const [ne_code, info] of comps.entries()) {
        // ne_code 存在チェック（警告のみ、強制弾きはしない）
        if (!checkNe.get(ne_code)) {
          missingNeCodes.push({ seller_sku: sku, ne_code });
        }
        insComp.run(sku, ne_code, info.数量, info.sort_order);
        componentCount++;
      }
    }
    if (dryRun) {
      throw new Error('__DRY_RUN__'); // ロールバック誘発
    }
  });

  try { tx(); }
  catch (e) {
    if (e.message !== '__DRY_RUN__') throw e;
  }

  return {
    masterCount,
    componentCount,
    skipped,
    warnings,
    nameConflicts,
    missingNeCodes,
    dupComponents,
  };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('import-sku-master.js');
if (isMain) {
  const args = process.argv.slice(2);
  const csvPath = args.find(a => !a.startsWith('--'));
  const dryRun = args.includes('--dry-run');
  const encArg = args.find(a => a.startsWith('--encoding='));
  const encoding = encArg ? encArg.split('=')[1] : 'cp932';

  if (!csvPath) {
    console.error('Usage: node apps/warehouse/import-sku-master.js <csv_path> [--dry-run] [--encoding=utf-8|cp932]');
    process.exit(1);
  }

  await initDB();
  console.log(`[import] CSV: ${csvPath}`);
  console.log(`[import] encoding: ${encoding}`);
  console.log(`[import] dryRun: ${dryRun}`);

  const result = importSkuMasterCSV(csvPath, { dryRun, encoding });

  console.log('\n=== 結果 ===');
  console.log(`m_sku_master  : ${result.masterCount}件`);
  console.log(`m_sku_components: ${result.componentCount}件`);
  console.log(`スキップ      : ${result.skipped}件`);
  console.log(`商品名conflict: ${result.nameConflicts.length}件 (A案で最初の行採用)`);
  console.log(`ne_code欠落   : ${result.missingNeCodes.length}件 (raw_ne_productsに存在しないNE)`);
  console.log(`構成重複      : ${result.dupComponents.length}件`);

  if (result.nameConflicts.length > 0) {
    console.log('\n--- 商品名conflict サンプル (最大10件) ---');
    result.nameConflicts.slice(0, 10).forEach(c => {
      console.log(`[${c.seller_sku}]`);
      c.names.forEach(n => console.log(`  - ${n}`));
    });
  }

  if (result.missingNeCodes.length > 0) {
    console.log('\n--- ne_code欠落 サンプル (最大10件) ---');
    result.missingNeCodes.slice(0, 10).forEach(m => {
      console.log(`  ${m.seller_sku} -> ${m.ne_code}`);
    });
  }

  if (result.warnings.length > 0 && result.warnings.length <= 30) {
    console.log('\n--- 警告 ---');
    result.warnings.forEach(w => console.log(`  ${w}`));
  } else if (result.warnings.length > 30) {
    console.log(`\n--- 警告: ${result.warnings.length}件 (多すぎるので非表示) ---`);
  }

  process.exit(0);
}
