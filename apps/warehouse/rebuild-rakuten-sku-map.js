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
import { buildMappings } from './rakuten-sku-map-build.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const DB_PATH = path.join(PROJECT_DIR, 'data', 'warehouse.db');

const SERVICE_TOKEN = process.env.SERVICE_TOKEN;
const SERVICE_URL = process.env.WAREHOUSE_INTERNAL_URL || 'http://localhost:3000';

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
  // ★manage_number (商品管理番号) を全行に持つ (2026-09-01)。
  //   W (商品番号) の行は 1 商品に 1 行しか作れないので、カラバリ 12 色のうち 11 色は W 行を持てず
  //   商品ページにたどり着けなかった (価格一括改定で発覚・楽天出品の 3 割)。AM/AL の行にも入れる
  const cols = db.prepare('PRAGMA table_info(f_rakuten_sku_map)').all().map((c) => c.name);
  if (!cols.includes('manage_number')) {
    db.exec('ALTER TABLE f_rakuten_sku_map ADD COLUMN manage_number TEXT');
    console.log('[RakutenSkuMap] 列を追加: manage_number');
  }
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

  // 3. 各SKUを解決して (rakuten_code → ne_code + source + manage_number) を収集
  //    複数SKUが同じrakuten_codeを持つ場合は source priority が高い方を優先 (組み立ては rakuten-sku-map-build.js)
  const { mappings, resolvedCount, unresolvedCount, withoutManageNumber } = buildMappings(skus, productMap);

  console.log(`[RakutenSkuMap] 解決: ${resolvedCount} / 未解決: ${unresolvedCount}`);
  console.log(`[RakutenSkuMap] マッピング総数（dedupe後）: ${mappings.size}`);
  if (withoutManageNumber > 0) {
    // 全SKU応答に manageNumber が無い = 経路のどこかが古い。ここで止めはしないが、気づけるように残す
    console.warn(`[RakutenSkuMap] ⚠️ manageNumber が無い SKU が ${withoutManageNumber} 件 (商品ページに届かない行になる)`);
  }

  // 4. DELETE + INSERT で全件置換
  ensureTable(db);
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const tx = db.transaction(() => {
    db.exec('DELETE FROM f_rakuten_sku_map');
    const stmt = db.prepare('INSERT INTO f_rakuten_sku_map (rakuten_code, ne_code, source, manage_number, updated_at) VALUES (?, ?, ?, ?, ?)');
    for (const [code, info] of mappings) {
      stmt.run(code, info.ne_code, info.source, info.manage_number, now);
    }
  });
  tx();

  // 5. 結果サマリ
  const bySource = db.prepare('SELECT source, COUNT(*) as n FROM f_rakuten_sku_map GROUP BY source').all();
  console.log(`[RakutenSkuMap] 保存内訳:`, bySource);
  const mnMissing = db.prepare('SELECT COUNT(*) as n FROM f_rakuten_sku_map WHERE manage_number IS NULL').get().n;
  console.log(`[RakutenSkuMap] manage_number 無し: ${mnMissing} 行`);

  const elapsed = Math.round((Date.now() - startedAt) / 1000);
  console.log(`[RakutenSkuMap] 完了 (${elapsed}秒): ${mappings.size}件保存`);

  db.close();
}

main().catch(e => {
  console.error('[RakutenSkuMap] エラー:', e.message);
  process.exit(1);
});
