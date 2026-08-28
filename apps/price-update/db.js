/**
 * price-update / 監査テーブル pu_* (要件定義 v1.0 F6)
 *
 * warehouse-mirror.db の中に置く (purchase-orders の po_* / inventory-monthly と同方式)。
 * mirror_ プレフィクスの表だけが同期で作り直される対象なので、pu_* は同期・再初期化・backout の
 * いずれからも触られない。
 *
 * ★append-only をコードで担保する (要件 F6):
 *   ・pu_runs / pu_operations は INSERT のみ。UPDATE / DELETE を書かない
 *   ・状態の変化は pu_events に**追記**する。現在状態は「最後のイベント、無ければ初期状態」で導出する
 *   これは「あとから履歴を書き換えられない」ことを構造で保証するため。値付けの記録は、
 *   誤更新が起きた時に「何を根拠にいくらへ変えたか」を後から検証できないと意味がない。
 *
 * M1 (読み取り専用版) が使う状態:
 *   previewed        … 引き当て + ライブ価格 + 利益プレビューを記録した
 *   manual_required  … Amazon / auPAY / Qoo10 の手動更新対象
 *   manual_done      … 手動更新を済ませたと本人が記録した (取り消しは manual_required へ戻すイベント)
 * M2 以降で executing / accepted / confirmed / failed / conflict / unknown が加わる。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

let initialized = false;

/** M1 で使う状態 (M2 以降で増える) */
export const STATES = ['previewed', 'manual_required', 'manual_done'];

export function initPriceUpdate() {
  const db = getMirrorDB();
  createTables(db);
  initialized = true;
  return db;
}

/** DDL 単体 (テストが自前の DB ハンドルに対して呼べるように分けてある) */
export function createTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS pu_runs (
    run_id        TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL,
    created_by    TEXT NOT NULL,
    kind          TEXT NOT NULL CHECK(kind IN ('normal','recovery')),
    note          TEXT,
    ne_codes_json TEXT NOT NULL,
    limits_json   TEXT NOT NULL,
    source_run_id TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pu_runs_created ON pu_runs(created_at DESC)');

  // PK 先頭は run_id (run 単位の読み出しが主。append-only 表の作法)
  db.exec(`CREATE TABLE IF NOT EXISTS pu_operations (
    run_id                 TEXT NOT NULL,
    operation_id           TEXT NOT NULL,
    seq                    INTEGER NOT NULL,
    mall                   TEXT NOT NULL,
    ne_code                TEXT NOT NULL,
    row_kind               TEXT NOT NULL CHECK(row_kind IN ('single','set')),
    via_code               TEXT,
    product_name           TEXT,
    listing_code           TEXT,
    sku_code               TEXT,
    confidence             TEXT NOT NULL CHECK(confidence IN ('confirmed','rule','sales','unresolved')),
    price_source           TEXT,
    price_fetched_at       TEXT,
    expected_current_price INTEGER,
    new_price              INTEGER,
    cost_excl_tax          REAL,
    tax_rate               REAL,
    shipping_cost          REAL,
    fee_rate               REAL,
    initial_state          TEXT NOT NULL,
    guard_json             TEXT,
    product_url            TEXT,
    created_at             TEXT NOT NULL,
    PRIMARY KEY (run_id, operation_id)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pu_ops_ne ON pu_operations(ne_code)');

  // 状態遷移・手動更新の記録はすべてここに追記する (行の書き換えをしない)
  db.exec(`CREATE TABLE IF NOT EXISTS pu_events (
    run_id       TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    operation_id TEXT,
    at           TEXT NOT NULL,
    actor        TEXT NOT NULL,
    event        TEXT NOT NULL,
    detail_json  TEXT,
    PRIMARY KEY (run_id, seq)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pu_events_op ON pu_events(run_id, operation_id, seq)');
  return db;
}

export function getDB() {
  if (!initialized) initPriceUpdate();
  return getMirrorDB();
}

/** 一意ID (ms + 乱数)。時刻だけだと同一msの衝突がありうる */
export function newId(prefix) {
  const rnd = Math.random().toString(36).slice(2, 8);
  return `${prefix}-${Date.now().toString(36)}-${rnd}`;
}

/**
 * run を1件書く (operations もまとめて INSERT)。
 * @returns {string} run_id
 */
export function insertRun(db, { createdBy, kind = 'normal', note = null, neCodes, limits, sourceRunId = null, operations }) {
  const runId = newId('pur');
  const now = new Date().toISOString();
  const insRun = db.prepare(`INSERT INTO pu_runs
    (run_id, created_at, created_by, kind, note, ne_codes_json, limits_json, source_run_id)
    VALUES (?,?,?,?,?,?,?,?)`);
  const insOp = db.prepare(`INSERT INTO pu_operations
    (run_id, operation_id, seq, mall, ne_code, row_kind, via_code, product_name, listing_code, sku_code,
     confidence, price_source, price_fetched_at, expected_current_price, new_price,
     cost_excl_tax, tax_rate, shipping_cost, fee_rate, initial_state, guard_json, product_url, created_at)
    VALUES (@run_id,@operation_id,@seq,@mall,@ne_code,@row_kind,@via_code,@product_name,@listing_code,@sku_code,
     @confidence,@price_source,@price_fetched_at,@expected_current_price,@new_price,
     @cost_excl_tax,@tax_rate,@shipping_cost,@fee_rate,@initial_state,@guard_json,@product_url,@created_at)`);
  const insEvent = db.prepare(`INSERT INTO pu_events (run_id, seq, operation_id, at, actor, event, detail_json)
    VALUES (?,?,?,?,?,?,?)`);

  const tx = db.transaction(() => {
    insRun.run(runId, now, createdBy, kind, note, JSON.stringify(neCodes), JSON.stringify(limits), sourceRunId);
    let seq = 0;
    for (const op of operations) {
      insOp.run({
        run_id: runId,
        operation_id: op.operationId || newId('puo'),
        seq: seq++,
        mall: op.mall,
        ne_code: op.neCode,
        row_kind: op.rowKind,
        via_code: op.viaCode ?? null,
        product_name: op.productName ?? null,
        listing_code: op.listingCode ?? null,
        sku_code: op.skuCode ?? null,
        confidence: op.confidence,
        price_source: op.priceSource ?? null,
        price_fetched_at: op.priceFetchedAt ?? null,
        expected_current_price: op.expectedCurrentPrice ?? null,
        new_price: op.newPrice ?? null,
        cost_excl_tax: op.cost ?? null,
        tax_rate: op.taxRate ?? null,
        shipping_cost: op.shipping ?? null,
        fee_rate: op.feeRate ?? null,
        initial_state: op.initialState,
        guard_json: op.guard ? JSON.stringify(op.guard) : null,
        product_url: op.productUrl ?? null,
        created_at: now,
      });
    }
    insEvent.run(runId, 0, null, now, createdBy, 'run_created', JSON.stringify({ operations: operations.length }));
  });
  tx();
  return runId;
}

/** イベントを1件追記する (run 内で単調増加する seq を採る) */
export function appendEvent(db, runId, { operationId = null, actor, event, detail = null }) {
  const tx = db.transaction(() => {
    const row = db.prepare('SELECT COALESCE(MAX(seq), -1) AS m FROM pu_events WHERE run_id = ?').get(runId);
    const seq = row.m + 1;
    db.prepare(`INSERT INTO pu_events (run_id, seq, operation_id, at, actor, event, detail_json)
      VALUES (?,?,?,?,?,?,?)`)
      .run(runId, seq, operationId, new Date().toISOString(), actor, event, detail ? JSON.stringify(detail) : null);
    return seq;
  });
  return tx();
}

/** 現在状態の導出: 最後の状態イベント、無ければ initial_state */
export function currentStates(db, runId) {
  const ops = db.prepare('SELECT operation_id, initial_state FROM pu_operations WHERE run_id = ?').all(runId);
  const state = new Map(ops.map((o) => [o.operation_id, o.initial_state]));
  const events = db.prepare(`
    SELECT operation_id, event FROM pu_events
     WHERE run_id = ? AND operation_id IS NOT NULL ORDER BY seq
  `).all(runId);
  for (const e of events) {
    if (STATES.includes(e.event)) state.set(e.operation_id, e.event);
  }
  return state;
}

export function getRun(db, runId) {
  const run = db.prepare('SELECT * FROM pu_runs WHERE run_id = ?').get(runId);
  if (!run) return null;
  const operations = db.prepare('SELECT * FROM pu_operations WHERE run_id = ? ORDER BY seq').all(runId);
  const events = db.prepare('SELECT * FROM pu_events WHERE run_id = ? ORDER BY seq').all(runId);
  const states = currentStates(db, runId);
  return {
    ...run,
    neCodes: JSON.parse(run.ne_codes_json || '[]'),
    limits: JSON.parse(run.limits_json || '{}'),
    operations: operations.map((o) => ({ ...o, state: states.get(o.operation_id) || o.initial_state })),
    events,
  };
}

export function listRuns(db, limit = 50) {
  return db.prepare(`
    SELECT r.run_id, r.created_at, r.created_by, r.kind, r.note,
           (SELECT COUNT(*) FROM pu_operations o WHERE o.run_id = r.run_id) AS op_count
      FROM pu_runs r ORDER BY r.created_at DESC LIMIT ?
  `).all(limit);
}
