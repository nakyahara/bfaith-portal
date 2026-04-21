#!/usr/bin/env node
/**
 * ranking-checker.json → ranking-checker.db 移行スクリプト
 *
 * Phase1 R2 変更:
 *   - client_id を JSON の `id` (UUID) から搬送。欠落時は
 *     `legacy:<keyword>::<product_code>` の決定的 ID を生成し、再実行時の
 *     二重挿入を防ぐ。完全に手掛かりが無ければ UUID をフォールバック。
 *   - 商品単位で失敗を集計。失敗があれば非0 exitでスクリプト全体を失敗にする。
 *
 * Usage:
 *   node scripts/migrate-json-to-sqlite.mjs [path/to/ranking-checker.json]
 *   DATA_DIR=/path node scripts/migrate-json-to-sqlite.mjs
 *
 * Env:
 *   DATA_DIR / RANKCHECK_DB_FILE / MIGRATE_PROGRESS_EVERY (default 100)
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import {
  countProducts, upsertProduct, upsertHistory, getDb, closeDb,
  generateClientId, DB_FILE,
} from '../apps/ranking-checker/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DEFAULT_JSON = path.join(DATA_DIR, 'ranking-checker.json');
const PROGRESS_EVERY = Math.max(1, parseInt(process.env.MIGRATE_PROGRESS_EVERY || '100', 10));

/**
 * legacy JSON の rank 値を DB 表現 (-1 / null / 1..100) へ正規化する。
 * rank_history 側の CHECK 制約と同じ範囲に収める。
 *   - null / undefined / '' → null (圏外/未計測)
 *   - 'error' → -1
 *   - 数値: -1 は -1、1..100 はそのまま、範囲外は null (圏外扱い)
 *   - その他の文字列 → -1 (解釈不能なのでエラー扱い)
 */
function encodeRank(v) {
  if (v === null || v === undefined || v === '') return null;
  // boolean/array/object を Number() に通すと true→1、[5]→5 のように coerce されるため、
  // 型を number/string に限定する。router.js の encodeImportRank と同等。
  if (typeof v !== 'number' && typeof v !== 'string') return null;
  if (v === 'error') return -1;
  const n = Number(v);
  if (Number.isFinite(n)) {
    const r = Math.round(n);
    if (r === -1) return -1;
    if (r >= 1 && r <= 100) return r;
    return null;
  }
  return -1;
}

function codeFromRakutenUrl(url) {
  if (!url) return '';
  const m = String(url).match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
  return m ? m[1] : '';
}

function normalizeOwnUrl(url) {
  if (!url) return '';
  return String(url).trim().toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .split('?')[0].split('#')[0]
    .replace(/\/+$/, '');
}

/**
 * 源となる identity フィールドから client_id を決める。router.js と同じ優先順位。
 *   1) p.client_id (DB形) があればそれを最優先
 *   2) p.id (UI UUID) があればそれを次優先
 *   3) keyword + 補完済みproduct_code + normalized(own_url) で決定的ID
 *      (product_code が欠落で own_url しか違わない商品同士が衝突するのを防ぐ)
 *   4) 最後の保険で UUID 新規
 */
function resolveClientId(p, effectiveCode, ownUrl) {
  if (p && typeof p.client_id === 'string' && p.client_id.length > 0) return p.client_id;
  if (p && typeof p.id === 'string' && p.id.length > 0) return p.id;
  const kw = p?.keyword || '';
  const code = effectiveCode || p?.product_code || '';
  const url = normalizeOwnUrl(ownUrl || p?.own_url || '');
  if (kw || code || url) {
    const h = crypto.createHash('sha1').update(`${kw}\u0000${code}\u0000${url}`).digest('hex');
    return `legacy-${h.slice(0, 16)}`;
  }
  return generateClientId();
}

async function main() {
  const jsonPath = process.argv[2] || DEFAULT_JSON;

  if (!fs.existsSync(jsonPath)) {
    console.error(`[migrate] JSONファイルが見つかりません: ${jsonPath}`);
    process.exit(1);
  }

  const stat = fs.statSync(jsonPath);
  console.log(`[migrate] 入力: ${jsonPath} (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);
  console.log(`[migrate] 出力DB: ${DB_FILE}`);

  const preCount = countProducts();
  console.log(`[migrate] 移行前のDB products件数: ${preCount}`);

  console.log(`[migrate] JSON読み込み中...`);
  const raw = fs.readFileSync(jsonPath, 'utf-8');
  const json = JSON.parse(raw);
  const products = json.products || [];
  console.log(`[migrate] JSON products件数: ${products.length}`);

  const db = getDb();
  let migratedProducts = 0;
  let migratedHistory = 0;
  let skippedHistory = 0;
  let failedProducts = 0;
  const failures = [];
  const startAt = Date.now();

  const upsertOne = db.transaction((p) => {
    let code = p.product_code || '';
    if (!code && p.own_url) code = codeFromRakutenUrl(p.own_url);

    // 補完後のcode・own_urlを client_id 生成材料に含める
    const clientId = resolveClientId(p, code, p.own_url);

    const productId = upsertProduct({
      client_id: clientId,
      keyword: p.keyword || '',
      product_code: code || null,
      own_url: p.own_url || null,
      yahoo_url: p.yahoo_url || null,
      amazon_url: p.amazon_url || null,
      amazon_asin: p.amazon_asin || null,
      competitor1_url: p.competitor1_url || null,
      competitor2_url: p.competitor2_url || null,
      review_count: p.review_count != null ? p.review_count : null,
    });

    const history = Array.isArray(p.history) ? p.history : [];
    let n = 0;
    for (const entry of history) {
      if (!entry || !entry.date) { skippedHistory++; continue; }
      upsertHistory({
        product_id: productId,
        date: entry.date,
        own_rank: encodeRank(entry.own_rank),
        competitor1_rank: encodeRank(entry.competitor1_rank),
        competitor2_rank: encodeRank(entry.competitor2_rank),
        yahoo_own_rank: encodeRank(entry.yahoo_own_rank),
        amazon_own_rank: encodeRank(entry.amazon_own_rank),
      });
      n++;
    }
    return n;
  });

  for (let i = 0; i < products.length; i++) {
    const p = products[i];
    if (!p || !p.keyword) {
      console.warn(`[migrate] skip index=${i} keyword欠落`);
      failedProducts++;
      failures.push({ index: i, reason: 'keyword欠落' });
      continue;
    }
    try {
      const historyAdded = upsertOne(p);
      migratedProducts++;
      migratedHistory += historyAdded;
    } catch (e) {
      failedProducts++;
      failures.push({ index: i, keyword: p.keyword, reason: e.message });
      console.error(`[migrate] 失敗 index=${i} keyword="${p.keyword}": ${e.message}`);
    }

    if ((i + 1) % PROGRESS_EVERY === 0 || i === products.length - 1) {
      const elapsed = ((Date.now() - startAt) / 1000).toFixed(1);
      console.log(`[migrate] ${i + 1}/${products.length} 商品処理済 ok=${migratedProducts} fail=${failedProducts} history=${migratedHistory} elapsed=${elapsed}s`);
    }
  }

  const postCount = countProducts();
  console.log(`\n[migrate] 完了`);
  console.log(`[migrate]   products: ${preCount} → ${postCount} (投入/更新 ${migratedProducts}, 失敗 ${failedProducts})`);
  console.log(`[migrate]   history 投入/更新: ${migratedHistory} 件  skip: ${skippedHistory} 件`);

  if (failedProducts > 0) {
    console.error(`\n[migrate] 失敗商品一覧 (先頭20件):`);
    for (const f of failures.slice(0, 20)) {
      console.error(`  index=${f.index} keyword="${f.keyword || '?'}" reason=${f.reason}`);
    }
  }

  closeDb();

  if (failedProducts > 0) {
    console.error(`\n[migrate] 失敗 ${failedProducts} 件のため非0 exit`);
    process.exit(2);
  }
}

main().catch(e => {
  console.error('[migrate] 致命的エラー:', e);
  process.exit(1);
});
