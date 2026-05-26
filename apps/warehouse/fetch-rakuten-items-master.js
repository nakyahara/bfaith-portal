#!/usr/bin/env node
/**
 * fetch-rakuten-items-master.js — LINEギフト Phase 1 A-5a-1
 *   楽天 RMS items.search で全商品マスタを取得し、以下 2 表に投入する:
 *     - raw_rakuten_items_master   (生スナップショット、genreId 含む)
 *     - x_rakuten_item_product_map (manageNumber ↔ ne_product_code マッピング)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2 / §13-6
 * Codex 議論: Round 2-6 (Critical: 商品同定キー = manageNumber、items.search レスポンス検証済)
 *
 * 認証: RAKUTEN_SERVICE_SECRET + RAKUTEN_LICENSE_KEY (.env、ミニPC側で管理)
 *
 * 動作:
 *   1. items.search を cursorMark でページング、1 req/sec + 指数バックオフ
 *   2. 各商品の {manageNumber, itemNumber, title, genreId} を取得
 *   3. raw_rakuten_items_master に SCD2 UPSERT (source_hash 差分検知)
 *   4. m_products.商品コード との LOWER(TRIM()) マッチで ne_product_code を解決
 *   5. x_rakuten_item_product_map に SCD2 UPSERT (CONFIRMED / UNMAPPED)
 *
 * genre_id は raw_rakuten_items_master に保存される。
 * 次フェーズ (A-5a-2) で raw_rakuten_items_master.genre_id → m_product_classifications へ投入。
 *
 * 使い方:
 *   node apps/warehouse/fetch-rakuten-items-master.js                全件取得 (差分更新)
 *   node apps/warehouse/fetch-rakuten-items-master.js --dry-run      取得のみ、DB 書込なし
 *   node apps/warehouse/fetch-rakuten-items-master.js --max-pages=5  最大ページ数を制限 (smoke 用)
 *
 * 注意:
 *   - 全商品 (数千〜数万件) で 1 req/sec → 数十分〜1時間想定
 *   - 通常運用は週次差分 (設計書 §13-6 推奨)
 *   - 既存 apps/warehouse/rakuten-rms-service.js は HTTP proxy、本スクリプトは直接 API を叩く
 */
import 'dotenv/config';
import crypto from 'node:crypto';
import { initDB, getDB } from './db.js';

const SERVICE_SECRET = process.env.RAKUTEN_SERVICE_SECRET;
const LICENSE_KEY = process.env.RAKUTEN_LICENSE_KEY;
const SHOP_CODE = process.env.RAKUTEN_SHOP_CODE || 'b-faith';

const RMS_BASE = 'https://api.rms.rakuten.co.jp/es/2.0';
const RATE_LIMIT_MS = 1100;
const HITS_PER_PAGE = 100;
const MAX_RETRY = 6;
const RETRY_BASE_MS = 1000;
const JITTER_MAX_MS = 250;

function nowJstIso() {
  const d = new Date();
  d.setMinutes(d.getMinutes() + d.getTimezoneOffset() + 9 * 60);
  return d.toISOString().replace('Z', '+09:00');
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter() {
  return Math.floor(Math.random() * JITTER_MAX_MS);
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function esaToken() {
  return 'ESA ' + Buffer.from(`${SERVICE_SECRET}:${LICENSE_KEY}`).toString('base64');
}

/**
 * items.search 1 ページ取得 (cursorMark ベース)、指数バックオフ
 */
async function fetchItemsPage(cursorMark) {
  const url = `${RMS_BASE}/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${HITS_PER_PAGE}`;
  let lastErr;
  for (let attempt = 0; attempt < MAX_RETRY; attempt++) {
    try {
      const res = await fetch(url, { headers: { Authorization: esaToken() } });
      if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
        const wait = RETRY_BASE_MS * Math.pow(2, attempt) + jitter();
        console.warn(`  [retry] cursor=${cursorMark} HTTP ${res.status}, sleep ${wait}ms (attempt ${attempt + 1}/${MAX_RETRY})`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
      const wait = RETRY_BASE_MS * Math.pow(2, attempt) + jitter();
      console.warn(`  [retry] cursor=${cursorMark} ${e.message}, sleep ${wait}ms (attempt ${attempt + 1}/${MAX_RETRY})`);
      await sleep(wait);
    }
  }
  throw new Error(`fetchItemsPage failed after ${MAX_RETRY} retries: ${lastErr?.message}`);
}

/**
 * items.search 1 ページの 1 商品オブジェクトから必要 field を抽出
 * Reference: apps/mercari-sync/rakuten.js parseItem
 */
function pickItem(result) {
  const item = result?.item ?? result;
  return {
    manageNumber: String(item?.manageNumber ?? '').trim(),
    itemNumber: String(item?.itemNumber ?? item?.manageNumber ?? '').trim(),
    title: String(item?.title ?? '').trim(),
    genreId: String(item?.genreId ?? '').trim(),
    itemUrl: String(item?.itemUrl ?? '').trim(),
    raw: item,
  };
}

/**
 * m_products から商品コード一覧 (LOWER(TRIM())) を読込み、Map で返す
 */
function loadNeProductCodes(db) {
  const rows = db.prepare(`SELECT 商品コード FROM m_products`).all();
  const map = new Map();
  for (const r of rows) {
    const key = String(r['商品コード'] ?? '').toLowerCase().trim();
    if (key) map.set(key, r['商品コード']);
  }
  return map;
}

/**
 * raw_rakuten_items_master に UPSERT (SCD2)
 */
function upsertRawItem(db, item, syncedAt) {
  const sourceHashInput = `${item.manageNumber}\t${item.title}\t${item.genreId}\t1`;
  const sourceHash = sha256Hex(sourceHashInput);

  const existing = db
    .prepare(`SELECT id, source_hash
              FROM raw_rakuten_items_master
              WHERE rakuten_shop_code=? AND manage_number=? AND is_active=1 AND valid_to IS NULL`)
    .get(SHOP_CODE, item.manageNumber);

  if (existing && existing.source_hash === sourceHash) return { action: 'raw_unchanged' };

  if (existing) {
    db.prepare(`UPDATE raw_rakuten_items_master SET is_active=0, valid_to=?, synced_at=? WHERE id=?`)
      .run(syncedAt, syncedAt, existing.id);
  }

  db.prepare(`
    INSERT INTO raw_rakuten_items_master (
      rakuten_shop_code, manage_number, item_number, item_url,
      title, genre_id, is_active, source_payload_json, source_hash,
      valid_from, valid_to, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, ?)
  `).run(
    SHOP_CODE, item.manageNumber, item.itemNumber, item.itemUrl || null,
    item.title, item.genreId || null,
    JSON.stringify(item.raw), sourceHash,
    syncedAt, syncedAt
  );
  return { action: existing ? 'raw_updated' : 'raw_inserted' };
}

/**
 * x_rakuten_item_product_map に UPSERT (SCD2)
 */
function upsertMapping(db, item, neProductCode, mappingType, reason, syncedAt) {
  const existing = db
    .prepare(`SELECT id, ne_product_code, mapping_type, rakuten_item_name
              FROM x_rakuten_item_product_map
              WHERE rakuten_shop_code=? AND rakuten_item_code=? AND is_active=1 AND valid_to IS NULL`)
    .get(SHOP_CODE, item.manageNumber);

  const sameAsExisting = existing
    && existing.ne_product_code === (neProductCode ?? '')
    && existing.mapping_type === mappingType
    && (existing.rakuten_item_name ?? '') === item.title;

  if (sameAsExisting) return { action: 'map_unchanged' };

  if (existing) {
    db.prepare(`UPDATE x_rakuten_item_product_map SET is_active=0, valid_to=?, synced_at=?, updated_by=? WHERE id=?`)
      .run(syncedAt, syncedAt, 'system:fetch-rakuten-items-master', existing.id);
  }

  db.prepare(`
    INSERT INTO x_rakuten_item_product_map (
      rakuten_shop_code, rakuten_item_code, rakuten_item_url, rakuten_item_name,
      ne_product_code, ne_sku_code,
      mapping_type, confidence, reason, is_active,
      valid_from, valid_to, created_by, updated_by, synced_at
    ) VALUES (?, ?, ?, ?, ?, NULL, ?, 1.0, ?, 1, ?, NULL, ?, ?, ?)
  `).run(
    SHOP_CODE, item.manageNumber, item.itemUrl || null, item.title,
    neProductCode ?? '',
    mappingType, reason ?? null,
    syncedAt, 'system:fetch-rakuten-items-master', 'system:fetch-rakuten-items-master', syncedAt
  );
  return { action: existing ? 'map_updated' : 'map_inserted' };
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const maxPagesArg = args.find((a) => a.startsWith('--max-pages='));
  const maxPages = maxPagesArg ? Number(maxPagesArg.split('=')[1]) : Infinity;

  if (!SERVICE_SECRET || !LICENSE_KEY) {
    console.error('[fetch-rakuten-items-master] FATAL: RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY 未設定');
    process.exit(1);
  }

  console.log(`[fetch-rakuten-items-master] start (dryRun=${dryRun}, maxPages=${maxPages}, shop=${SHOP_CODE})`);

  initDB();
  const db = getDB();

  const neCodes = loadNeProductCodes(db);
  console.log(`  m_products: ${neCodes.size} 件 (LOWER+TRIM 済)`);

  const syncedAt = nowJstIso();
  const counters = { total: 0, pages: 0, unmapped: 0 };
  let cursorMark = '*';

  while (counters.pages < maxPages) {
    const body = await fetchItemsPage(cursorMark);
    const results = body?.results ?? body?.items ?? [];
    counters.pages += 1;

    if (results.length === 0) {
      console.log(`  [page ${counters.pages}] no results, stop`);
      break;
    }

    if (!dryRun) {
      const txn = db.transaction(() => {
        for (const result of results) {
          const item = pickItem(result);
          if (!item.manageNumber) continue;

          const r1 = upsertRawItem(db, item, syncedAt);
          counters[r1.action] = (counters[r1.action] ?? 0) + 1;

          const ciKey = item.manageNumber.toLowerCase();
          const neProductCode = neCodes.get(ciKey) ?? null;
          const mappingType = neProductCode ? 'CONFIRMED' : 'UNMAPPED';
          const reason = neProductCode ? null : 'no NE 商品コード match (m_products に未登録)';

          const r2 = upsertMapping(db, item, neProductCode, mappingType, reason, syncedAt);
          counters[r2.action] = (counters[r2.action] ?? 0) + 1;
          if (mappingType === 'UNMAPPED') counters.unmapped += 1;
          counters.total += 1;
        }
      });
      txn();
    } else {
      for (const result of results) {
        const item = pickItem(result);
        if (!item.manageNumber) continue;
        const ciKey = item.manageNumber.toLowerCase();
        if (!neCodes.has(ciKey)) counters.unmapped += 1;
        counters.total += 1;
      }
    }

    const nextCursorMark = body?.nextCursorMark ?? body?.next_cursor_mark ?? '';
    console.log(`  [page ${counters.pages}] results=${results.length}, total=${counters.total}, unmapped=${counters.unmapped}, next=${nextCursorMark ? '...' : 'END'}`);

    if (!nextCursorMark || nextCursorMark === cursorMark) break;
    cursorMark = nextCursorMark;
    await sleep(RATE_LIMIT_MS + jitter());
  }

  console.log('[fetch-rakuten-items-master] done');
  console.log(`  counters=${JSON.stringify(counters)}`);
}

main().catch((e) => {
  console.error('[fetch-rakuten-items-master] FATAL:', e);
  process.exit(1);
});
