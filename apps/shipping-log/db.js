/**
 * 出荷実績ログ (shipping-log) — データ層
 *
 * 出荷_no フォルダの夜間掃除 GAS が、削除直前に納品書PDFから抽出した
 * 伝票情報 (出荷伝票NO / 管理番号 / モール注文番号) を受け取り append-only で保存する。
 * 「どの受注を・どのバッチ (フォルダ) で・いつ出荷したか」の唯一の永続記録。
 * 誤出荷管理 (f_mis_shipments) や NE 受注との突合は管理番号 (mgmt_no) / slip_no で JOIN する。
 *
 * 設計:
 *  - warehouse-mirror.db 同居 (packing-dispatch と同じ per-app fail-soft スキーマ方式)
 *  - PK は業務キー (ship_date, folder_name, slip_no)。GAS の再送・同日再実行は
 *    INSERT OR IGNORE で冪等に吸収する。run_id は provenance 列として保持
 *    (mart 系の run_id 先頭 PK 規約はswap用途のため、ここでは再送冪等性を優先)
 *  - append-only: UPDATE/DELETE は trigger で RAISE(ABORT)
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

let schemaReady = false;
let schemaError = null;
export function getSchemaError() { return schemaError; }

export function ensureSchema() {
  const db = getMirrorDB();
  if (schemaReady) return db;

  try {
    db.exec(`
      CREATE TABLE IF NOT EXISTS sl_shipping_slips (
        ship_date TEXT NOT NULL,          -- 出荷日 (JST, YYYY-MM-DD)
        folder_name TEXT NOT NULL,        -- 出荷_XX (バッチ)
        slip_no TEXT NOT NULL,            -- 出荷伝票NO (ロジザード SPxxxxxxxxxxx)
        mgmt_no TEXT,                     -- 管理番号 (NE伝票番号)
        mall_order_no TEXT,               -- モール注文番号 (Amazon: AESxxx-... 等、抽出できた場合のみ)
        source_file TEXT,                 -- 抽出元 納品書PDF ファイル名
        run_id TEXT NOT NULL,             -- GAS 実行ID (provenance)
        extracted_at TEXT,                -- GAS 側抽出時刻 (JST文字列)
        received_at TEXT NOT NULL,        -- サーバ受信時刻 (UTC ISO)
        PRIMARY KEY (ship_date, folder_name, slip_no)
      );
      CREATE INDEX IF NOT EXISTS idx_sl_slips_slip_no ON sl_shipping_slips(slip_no);
      CREATE INDEX IF NOT EXISTS idx_sl_slips_mgmt_no ON sl_shipping_slips(mgmt_no);
      CREATE INDEX IF NOT EXISTS idx_sl_slips_mall_order ON sl_shipping_slips(mall_order_no);
      CREATE TRIGGER IF NOT EXISTS trg_sl_slips_no_update
        BEFORE UPDATE ON sl_shipping_slips
        BEGIN SELECT RAISE(ABORT, 'sl_shipping_slips is append-only (UPDATE forbidden)'); END;
      CREATE TRIGGER IF NOT EXISTS trg_sl_slips_no_delete
        BEFORE DELETE ON sl_shipping_slips
        BEGIN SELECT RAISE(ABORT, 'sl_shipping_slips is append-only (DELETE forbidden)'); END;
    `);
    schemaReady = true;
    schemaError = null;
  } catch (e) {
    // fail-soft: mirror 本体を巻き込まない (2026-07-12 障害の教訓)。router 側で 503 を返す。
    schemaError = { message: String(e.message || e), code: e.code || null };
    console.error('[shipping-log] スキーマ初期化失敗 (mirror本体は継続):', e.message);
  }
  return db;
}

/**
 * 1フォルダ分の伝票行を冪等 INSERT する。
 * @param {{ runId: string, folderName: string, shipDate: string, extractedAt: string|null,
 *           rows: Array<{ slip_no: string, mgmt_no?: string, mall_order_no?: string, source_file?: string }> }} p
 * @returns {{ inserted: number, ignored: number }}
 */
export function ingestFolderSlips(p) {
  const db = ensureSchema();
  if (schemaError) {
    const err = new Error('shipping-log schema unavailable');
    err.code = 'SCHEMA_UNAVAILABLE';
    throw err;
  }
  const receivedAt = new Date().toISOString();
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO sl_shipping_slips
      (ship_date, folder_name, slip_no, mgmt_no, mall_order_no, source_file, run_id, extracted_at, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const tx = db.transaction((rows) => {
    let inserted = 0;
    for (const r of rows) {
      const info = stmt.run(
        p.shipDate, p.folderName, r.slip_no,
        r.mgmt_no || null, r.mall_order_no || null, r.source_file || null,
        p.runId, p.extractedAt || null, receivedAt
      );
      inserted += info.changes;
    }
    return inserted;
  });
  const inserted = tx(p.rows);
  return { inserted, ignored: p.rows.length - inserted };
}

/** 直近の伝票行 (動作確認・突合ジョブ用) */
export function recentSlips(limit) {
  const db = ensureSchema();
  if (schemaError) {
    const err = new Error('shipping-log schema unavailable');
    err.code = 'SCHEMA_UNAVAILABLE';
    throw err;
  }
  const n = Math.min(Math.max(Number(limit) || 50, 1), 500);
  return db.prepare(`
    SELECT ship_date, folder_name, slip_no, mgmt_no, mall_order_no, source_file, run_id, received_at
    FROM sl_shipping_slips
    ORDER BY received_at DESC, folder_name, slip_no
    LIMIT ?
  `).all(n);
}
