/**
 * record-m-products-history.js — m_products 変更差分を m_products_history に記録
 *
 * 背景 (Codex Round 17 指摘):
 *   - daily-sync は rebuild-m-products.js を毎日実行 (DELETE FROM m_products + INSERT 全件)
 *   - 旧 trigger 方式だと毎日全件 DELETE+INSERT が history に記録され、履歴の意味が崩れる
 *   - 本スクリプト: 「現在の m_products」 vs 「m_products_history の最新スナップショット」を比較し、
 *     主要列が変わった SKU のみ history に追記
 *
 * 実行タイミング:
 *   daily-sync.js 内で rebuild-m-products.js の直後に呼ぶ (m_products 確定後)
 *
 * 使い方:
 *   node apps/warehouse/record-m-products-history.js              # 通常実行
 *   node apps/warehouse/record-m-products-history.js --baseline   # 全件を 'BASELINE_RESET' で再投入
 *
 * トランザクション安全:
 *   1 トランザクションで diff 抽出 + INSERT 完了
 */

import 'dotenv/config';
import { initDB, getDB } from './db.js';

const TRACKED_COLUMNS = [
  '商品名', '商品区分', '取扱区分',
  '標準売価', '原価', '原価ソース', '原価状態',
  '消費税率', '税区分', '売上分類',
];

function parseArgs() {
  const args = process.argv.slice(2);
  return { baseline: args.includes('--baseline') };
}

function nowSql() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

async function main() {
  const args = parseArgs();
  await initDB();
  const db = getDB();

  // 0. m_products_history が存在しなければ作成 (db.js で作る方が良いが、念のため)
  db.exec(`CREATE TABLE IF NOT EXISTS m_products_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    商品コード TEXT NOT NULL,
    商品名 TEXT, 商品区分 TEXT, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT,
    消費税率 REAL, 税区分 TEXT, 売上分類 INTEGER,
    changed_at TEXT NOT NULL, operation TEXT NOT NULL, changed_by TEXT,
    原価_before REAL, 標準売価_before REAL, 消費税率_before REAL,
    changed_columns TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mph_code_changed ON m_products_history(商品コード, changed_at)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mph_changed ON m_products_history(changed_at)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mph_product_id ON m_products_history(product_id, changed_at)`);

  // 1. trigger 削除 (旧 trigger 方式の残骸を確実に廃止、Codex Round 17 指摘 2)
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_insert`);
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_update`);
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_delete`);

  const changedAt = nowSql();
  const changedBy = 'record-m-products-history';
  let inserted = 0;

  if (args.baseline) {
    // BASELINE_RESET モード: 現在の m_products 全件を BASELINE_RESET として記録
    console.log('[history] BASELINE_RESET モード: 現在の m_products 全件を記録');
    const stmt = db.prepare(`
      INSERT INTO m_products_history (
        product_id, 商品コード, 商品名, 商品区分, 取扱区分,
        標準売価, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 売上分類,
        changed_at, operation, changed_by, 原価_before, 標準売価_before, 消費税率_before, changed_columns
      )
      SELECT product_id, 商品コード, 商品名, 商品区分, 取扱区分,
        標準売価, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 売上分類,
        ?, 'BASELINE_RESET', ?, NULL, NULL, NULL, NULL
      FROM m_products
    `);
    const r = stmt.run(changedAt, changedBy);
    inserted = r.changes;
    console.log('[history] BASELINE_RESET 投入:', inserted, '件');
    process.exit(0);
  }

  // 2. 通常モード: 差分検出
  // 現在の m_products vs history の各 SKU の最新行 を比較

  // history の最新行を SKU 別に取得 (operation != 'DELETE' の最新)
  // SQLite の subquery で row_number() 相当
  const latestSnapshotSql = `
    WITH latest AS (
      SELECT h.*, ROW_NUMBER() OVER (PARTITION BY 商品コード ORDER BY changed_at DESC, history_id DESC) AS rn
      FROM m_products_history h
      WHERE operation != 'DELETE'
    )
    SELECT product_id, 商品コード, 商品名, 商品区分, 取扱区分,
           標準売価, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 売上分類
    FROM latest WHERE rn = 1
  `;
  const latestRows = db.prepare(latestSnapshotSql).all();
  const latestMap = new Map();
  for (const r of latestRows) latestMap.set(r.商品コード, r);

  // 現在の m_products
  const currentRows = db.prepare(`
    SELECT product_id, 商品コード, 商品名, 商品区分, 取扱区分,
           標準売価, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 売上分類
    FROM m_products
  `).all();

  // 比較
  const insertStmt = db.prepare(`
    INSERT INTO m_products_history (
      product_id, 商品コード, 商品名, 商品区分, 取扱区分,
      標準売価, 原価, 原価ソース, 原価状態, 消費税率, 税区分, 売上分類,
      changed_at, operation, changed_by, 原価_before, 標準売価_before, 消費税率_before, changed_columns
    ) VALUES (
      @product_id, @商品コード, @商品名, @商品区分, @取扱区分,
      @標準売価, @原価, @原価ソース, @原価状態, @消費税率, @税区分, @売上分類,
      @changed_at, @operation, @changed_by, @原価_before, @標準売価_before, @消費税率_before, @changed_columns
    )
  `);

  function diffColumns(prev, curr) {
    const changed = [];
    for (const col of TRACKED_COLUMNS) {
      const a = prev?.[col] ?? null;
      const b = curr?.[col] ?? null;
      // 数値比較は厳密に、それ以外は == でも OK だが文字列含むので Object.is で
      if (a !== b && !(typeof a === 'number' && typeof b === 'number' && Math.abs(a - b) < 1e-9)) {
        changed.push(col);
      }
    }
    return changed;
  }

  const txn = db.transaction(() => {
    let cnt = 0;
    const seenSkus = new Set();

    for (const curr of currentRows) {
      seenSkus.add(curr.商品コード);
      const prev = latestMap.get(curr.商品コード);
      if (!prev) {
        // 新規 SKU
        insertStmt.run({
          ...curr,
          changed_at: changedAt, operation: 'INSERT', changed_by: changedBy,
          原価_before: null, 標準売価_before: null, 消費税率_before: null, changed_columns: null,
        });
        cnt++;
        continue;
      }
      const changed = diffColumns(prev, curr);
      if (changed.length === 0) continue;
      insertStmt.run({
        ...curr,
        changed_at: changedAt, operation: 'UPDATE', changed_by: changedBy,
        原価_before: prev.原価, 標準売価_before: prev.標準売価, 消費税率_before: prev.消費税率,
        changed_columns: changed.join(','),
      });
      cnt++;
    }

    // DELETE 検出 (history にあって m_products に無い SKU)
    for (const [sku, prev] of latestMap) {
      if (seenSkus.has(sku)) continue;
      insertStmt.run({
        product_id: prev.product_id, '商品コード': sku,
        商品名: prev.商品名, 商品区分: prev.商品区分, 取扱区分: prev.取扱区分,
        標準売価: prev.標準売価, 原価: prev.原価, 原価ソース: prev.原価ソース, 原価状態: prev.原価状態,
        消費税率: prev.消費税率, 税区分: prev.税区分, 売上分類: prev.売上分類,
        changed_at: changedAt, operation: 'DELETE', changed_by: changedBy,
        原価_before: prev.原価, 標準売価_before: prev.標準売価, 消費税率_before: prev.消費税率,
        changed_columns: null,
      });
      cnt++;
    }

    return cnt;
  });

  inserted = txn();

  // サマリ
  console.log(`[history] 差分記録: ${inserted} 件 (${changedAt})`);
  if (inserted > 0) {
    const breakdown = db.prepare(`
      SELECT operation, COUNT(*) AS cnt
      FROM m_products_history WHERE changed_at = ?
      GROUP BY operation ORDER BY operation
    `).all(changedAt);
    console.table(breakdown);
  }
  process.exit(0);
}

main().catch(e => { console.error('[history] FATAL:', e?.message || e); if (e?.stack) console.error(e.stack); process.exit(1); });
