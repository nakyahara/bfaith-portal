/**
 * 楽天 AM/AL/W → NE商品コード マッピングを再構築
 *
 * 楽天RMS /items/all-skus から全SKUを取得し、m_products に対して
 * 3段階フォールバック（AM → AL → W）で NE商品コード を解決。
 * 結果を f_rakuten_sku_map に保存（DELETE + INSERT 全件置換）。
 *
 * 粗利分析アプリが mirror_sales_daily.商品コード と mirror_rakuten_sku_map.rakuten_code を
 * LEFT JOIN して未紐付けの楽天商品を解消できるようにする。
 *
 * 実行: node apps/warehouse/rebuild-rakuten-sku-map.js
 */
import 'dotenv/config';
import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const DB_PATH = path.join(PROJECT_DIR, 'data', 'warehouse.db');

const SERVICE_TOKEN = process.env.SERVICE_TOKEN;
const SERVICE_URL = process.env.WAREHOUSE_INTERNAL_URL || 'http://localhost:3000';

const INVALID_AL = new Set(['normal-inventory', 'normal-size', 'normal', '']);

// AM(1) > AL(2) > W(3): 小さいほど優先
const PRIORITY = { am: 1, al: 2, w: 3 };

async function fetchAllSkus() {
  const url = `${SERVICE_URL}/service-api/rakuten-rms/items/all-skus`;
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${SERVICE_TOKEN}` },
  });
  if (!res.ok) throw new Error(`Failed to fetch all-skus: HTTP ${res.status}`);
  const body = await res.json();
  if (!body.ok) throw new Error(`all-skus returned error: ${body.message || 'unknown'}`);
  return body.skus || [];
}

function ensureTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS f_rakuten_sku_map (
    rakuten_code TEXT PRIMARY KEY,
    ne_code      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated_at   TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_frskm_ne ON f_rakuten_sku_map(ne_code)');
}

function resolveSku(sku, productMap) {
  const am = (sku.systemSkuNumber || '').toLowerCase();
  const al = (sku.skuManageNumber || '').toLowerCase();
  const w  = (sku.itemNumber || '').toLowerCase();

  // Stage 1: AM
  if (am && productMap.has(am)) {
    return { ne_code: productMap.get(am), resolution: 'am' };
  }
  // Stage 2: AL（無意味値はスキップ）
  if (al && !INVALID_AL.has(al) && productMap.has(al)) {
    return { ne_code: productMap.get(al), resolution: 'al' };
  }
  // Stage 3: W
  if (w && productMap.has(w)) {
    return { ne_code: productMap.get(w), resolution: 'w' };
  }
  return null;
}

async function main() {
  if (!SERVICE_TOKEN) {
    throw new Error('SERVICE_TOKEN not set in .env');
  }

  console.log('[RakutenSkuMap] 開始');
  const startedAt = Date.now();

  // 1. m_products を読み込み（商品コード小文字化）
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  // バッチなのでロック競合時は待つ（常駐サーバ側の書き込み中に当たることがある）
  db.pragma('busy_timeout = 60000');
  const productMap = new Map();
  for (const p of db.prepare('SELECT 商品コード FROM m_products').all()) {
    productMap.set((p.商品コード || '').toLowerCase(), p.商品コード);
  }
  console.log(`[RakutenSkuMap] m_products: ${productMap.size}件ロード`);

  // 2. 楽天RMSから全SKU取得
  console.log('[RakutenSkuMap] RMSから全SKU取得中...');
  const skus = await fetchAllSkus();
  console.log(`[RakutenSkuMap] RMS取得: ${skus.length} SKU`);

  // 3. 各SKUを解決して (rakuten_code → ne_code + source) を収集
  //    複数SKUが同じrakuten_codeを持つ場合は source priority が高い方を優先
  const mappings = new Map(); // rakuten_code → { ne_code, source, priority }
  let resolvedCount = 0;
  let unresolvedCount = 0;

  for (const sku of skus) {
    const result = resolveSku(sku, productMap);
    if (!result) {
      unresolvedCount++;
      continue;
    }
    resolvedCount++;

    const am = (sku.systemSkuNumber || '').toLowerCase();
    const al = (sku.skuManageNumber || '').toLowerCase();
    const w  = (sku.itemNumber || '').toLowerCase();

    // 解決で使われたcodeは確実に登録（権威あり）
    // それ以外のcodeも同じne_codeに対応付ける（profit-analysisで任意コードから引けるように）
    const candidates = [];
    if (am) candidates.push({ code: am, src: 'am' });
    if (al && !INVALID_AL.has(al)) candidates.push({ code: al, src: 'al' });
    if (w) candidates.push({ code: w, src: 'w' });

    for (const c of candidates) {
      const existing = mappings.get(c.code);
      const newPriority = PRIORITY[c.src];
      if (!existing || newPriority < existing.priority) {
        mappings.set(c.code, { ne_code: result.ne_code, source: c.src, priority: newPriority });
      }
    }
  }

  console.log(`[RakutenSkuMap] 解決: ${resolvedCount} / 未解決: ${unresolvedCount}`);
  console.log(`[RakutenSkuMap] マッピング総数（dedupe後）: ${mappings.size}`);

  // 4. DELETE + INSERT で全件置換
  ensureTable(db);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const tx = db.transaction(() => {
    db.exec('DELETE FROM f_rakuten_sku_map');
    const stmt = db.prepare('INSERT INTO f_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, ?, ?)');
    for (const [code, info] of mappings) {
      stmt.run(code, info.ne_code, info.source, now);
    }
  });
  tx();

  // 5. 結果サマリ
  const bySource = db.prepare('SELECT source, COUNT(*) as n FROM f_rakuten_sku_map GROUP BY source').all();
  console.log(`[RakutenSkuMap] 保存内訳:`, bySource);

  const elapsed = Math.round((Date.now() - startedAt) / 1000);
  console.log(`[RakutenSkuMap] 完了 (${elapsed}秒): ${mappings.size}件保存`);

  db.close();
}

main().catch(e => {
  console.error('[RakutenSkuMap] エラー:', e.message);
  process.exit(1);
});
