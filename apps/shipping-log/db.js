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
 *  - PK は slip_no 単独 (ロジザード出荷伝票NO は全社一意)。GAS の再送は日をまたいでも
 *    冪等: 同一 slip_no + 業務内容一致 → ignored、内容不一致 → conflict (409、削除させない)。
 *    (Codex R1 high #2/#3: INSERT OR IGNORE の黙殺と ship_date 入り PK の日跨ぎ非冪等を廃止)
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
        slip_no TEXT PRIMARY KEY,         -- 出荷伝票NO (ロジザード SPxxxxxxxxxxx、全社一意)
        ship_date TEXT NOT NULL,          -- 出荷日 (JST, YYYY-MM-DD。初回取込時の値を保持)
        folder_name TEXT NOT NULL,        -- 出荷_XX (バッチ)
        mgmt_no TEXT,                     -- 管理番号 (NE伝票番号。抽出できた場合のみ)
        mall_order_no TEXT,               -- モール注文番号 (Amazon: AESxxx-... 等、抽出できた場合のみ)
        source_file TEXT,                 -- 抽出元 納品書PDF ファイル名
        run_id TEXT NOT NULL,             -- GAS 実行ID (provenance)
        extracted_at TEXT,                -- GAS 側抽出時刻 (JST文字列)
        received_at TEXT NOT NULL         -- サーバ受信時刻 (UTC ISO)
      );
      CREATE INDEX IF NOT EXISTS idx_sl_slips_ship_date ON sl_shipping_slips(ship_date);
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

function requireSchema() {
  const db = ensureSchema();
  if (schemaError) {
    const err = new Error('shipping-log schema unavailable');
    err.code = 'SCHEMA_UNAVAILABLE';
    throw err;
  }
  return db;
}

/**
 * 既存行との業務内容比較。conflict になるのは:
 *  - 双方非NULLで値が異なる (別内容の再送)
 *  - 既存NULL・新規非NULL (append-only のため正しい新情報を保存できない → 409 で
 *    削除を止め人手確認へ。黙って捨てると恒久欠損になる — Codex R2 medium)
 * 既存非NULL・新規NULL (再送時の抽出劣化) のみ idempotent 扱いで既存値を正とする。
 */
function isConflict(existing, row) {
  const differs = (a, b) => (a != null && b != null && a !== b) || (a == null && b != null);
  return differs(existing.mgmt_no, row.mgmt_no) || differs(existing.mall_order_no, row.mall_order_no);
}

/**
 * 1フォルダ分の伝票行を冪等 INSERT する。
 * INSERT-first + PK 違反 catch でレース耐性 (mis-shipment の insertSingleMisShipment と同型)。
 * @param {{ runId: string, folderName: string, shipDate: string, extractedAt: string|null,
 *           rows: Array<{ slip_no: string, mgmt_no?: string|null, mall_order_no?: string|null, source_file?: string|null }> }} p
 * @returns {{ inserted: number, ignored: number, conflicts: Array<{ slip_no: string, existing_mgmt_no: string|null, existing_mall_order_no: string|null }> }}
 */
export function ingestFolderSlips(p) {
  const db = requireSchema();
  const receivedAt = new Date().toISOString();
  const insertStmt = db.prepare(`
    INSERT INTO sl_shipping_slips
      (slip_no, ship_date, folder_name, mgmt_no, mall_order_no, source_file, run_id, extracted_at, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const selectStmt = db.prepare('SELECT slip_no, mgmt_no, mall_order_no FROM sl_shipping_slips WHERE slip_no = ?');

  const tx = db.transaction((rows) => {
    let inserted = 0;
    let ignored = 0;
    const conflicts = [];
    for (const r of rows) {
      try {
        insertStmt.run(
          r.slip_no, p.shipDate, p.folderName,
          r.mgmt_no || null, r.mall_order_no || null, r.source_file || null,
          p.runId, p.extractedAt || null, receivedAt
        );
        inserted++;
      } catch (e) {
        if (e && String(e.code || '').startsWith('SQLITE_CONSTRAINT')) {
          const existing = selectStmt.get(r.slip_no);
          if (existing && isConflict(existing, r)) {
            conflicts.push({ slip_no: r.slip_no, existing_mgmt_no: existing.mgmt_no, existing_mall_order_no: existing.mall_order_no });
          } else {
            ignored++; // 再送 (内容一致 or 新規側が null) → 冪等
          }
        } else {
          throw e;
        }
      }
    }
    // conflict があってもここまでの INSERT は有効のままにする (append-only の事実は残す)。
    // GAS には 409 が返り、フォルダは削除されないので人間が調査できる。
    return { inserted, ignored, conflicts };
  });
  return tx(p.rows);
}

/** 直近の伝票行 (動作確認・突合ジョブ用) */
export function recentSlips(limit) {
  const db = requireSchema();
  const n = Math.min(Math.max(Number(limit) || 50, 1), 500);
  return db.prepare(`
    SELECT slip_no, ship_date, folder_name, mgmt_no, mall_order_no, source_file, run_id, received_at
    FROM sl_shipping_slips
    ORDER BY received_at DESC, slip_no
    LIMIT ?
  `).all(n);
}
