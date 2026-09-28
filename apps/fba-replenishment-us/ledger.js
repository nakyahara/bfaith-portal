/**
 * 米国の NE 伝票の台帳 (DATA_DIR/fba-us.db・better-sqlite3・WAL)。設計 = 米国FBA納品アプリ_設計方針 §12.6 (中原さん 9/27 = B)
 *
 * 米国用の NE 受注 CSV を出したら、ここに伝票を 1 行保存する (保存できてから CSV を返す)。
 * 日本の計算 (calculation-engine.js allocateForItems) と米国の配分が、ここから「押さえ中」の数を読んで倉庫在庫から引く。
 *
 * 状態: reserved (倉庫で押さえ中) → left (人が「倉庫から出た」) → arrived (人が「米国 FBA に載った」)
 *       reserved → cancelled (人が「取り消す」・NE で取り消したことを確かめて)。倉庫を出た後の取消は受けない
 * 倉庫から引く数 (構成品ごと) = reserved + (left かつ 倉庫在庫の時点 ≤ left_at)   ← 出た後の倉庫 CSV に替わるまで引き続ける
 * 米国 SKU の在庫に足す数 = reserved + left (arrived で外す)   ← 出た直後に米国のレポートに載るまでの再推奨を防ぐ
 *
 * Render だけ。miniPC (同じ server.js) では開かない = not_available (日本の計算は今までどおり)。
 * 台帳を作ったら印のファイル (fba-us.ledger-created) を置く。印があるのに DB が無い・読めない = error
 * (日本の計算は影の下書きの関所で止め、日本の画面に赤帯。空の台帳を正常な 0 件にしない)。
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { isRender } from '../../lib/is-render.js';

const STATUSES = ['reserved', 'left', 'arrived', 'cancelled'];
const TRANSITIONS = { left: ['reserved'], arrived: ['left'], cancelled: ['reserved'] };

let db = null;
let dbFile = null;
let forced = null;   // 試験用: { file } を渡すと Render でなくても開く

const dataDir = () => process.env.DATA_DIR || path.join(process.cwd(), 'data');
const markerOf = (file) => `${file.replace(/\.db$/, '')}.ledger-created`;

/** 試験用: 一時ファイルで開く (null で閉じて元に戻す) */
export function _useLedgerForTest(file) {
  if (db) { try { db.close(); } catch { /* noop */ } }
  db = null; dbFile = null; forced = file ? { file } : null;
}

function createTables(d) {
  d.exec(`CREATE TABLE IF NOT EXISTS us_ne_slips (
    order_no      TEXT PRIMARY KEY,
    seq           INTEGER NOT NULL UNIQUE,
    request_id    TEXT NOT NULL UNIQUE,
    content_hash  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('reserved', 'left', 'arrived', 'cancelled')),
    items_json    TEXT NOT NULL,        -- [{ sku, qty }] 米国 SKU の個数
    units_json    TEXT NOT NULL,        -- [{ code, qty, name }] 構成品 (NE 商品コード・小文字) の個数
    csv           BLOB NOT NULL,        -- 出した CSV そのもの (Shift_JIS)
    filename      TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    created_by    TEXT,
    left_at       TEXT, left_by TEXT,
    arrived_at    TEXT, arrived_by TEXT,
    cancelled_at  TEXT, cancelled_by TEXT,
    updated_at    TEXT NOT NULL
  )`);
  d.exec(`CREATE TABLE IF NOT EXISTS us_ne_slip_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    order_no   TEXT NOT NULL,
    event      TEXT NOT NULL,
    from_status TEXT, to_status TEXT,
    by         TEXT, note TEXT,
    at         TEXT NOT NULL
  )`);
  d.exec(`CREATE TABLE IF NOT EXISTS us_ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
  d.prepare(`INSERT OR IGNORE INTO us_ledger_meta (key, value) VALUES ('version', '0')`).run();
}

/**
 * 台帳を開く。Render でなければ null (not_available)。
 * @param {{create?: boolean}} o  create = 無ければ作る (米国の出力のときだけ)。読むだけのときは作らない
 */
function open({ create = false } = {}) {
  if (db) return db;
  if (!forced && !isRender()) return null;
  if (!forced && process.env.RENDER && !process.env.DATA_DIR) throw Object.assign(new Error('DATA_DIR が未設定 (永続ディスクなしでは米国の台帳を持てない)'), { code: 'US_LEDGER_NO_DATA_DIR' });
  const file = forced ? forced.file : path.join(dataDir(), 'fba-us.db');
  const marker = markerOf(file);
  const exists = fs.existsSync(file);
  if (!exists && fs.existsSync(marker)) throw Object.assign(new Error(`米国の台帳が消えている (${path.basename(file)} が無いのに作った印がある)。バックアップから戻すまで米国の伝票を数えられない`), { code: 'US_LEDGER_MISSING' });
  if (!exists && !create) return 'empty';
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const d = new Database(file);
  try {
    d.pragma('journal_mode = WAL');
    d.pragma('busy_timeout = 5000');
    if (exists) {
      // 🚨 既にあるファイルには表を作らない。必要な表・版が欠けていたら壊れている = error
      //   (作り直すと空の台帳 = 「押さえ中 0 件」になり、米国に押さえた在庫を日本に配る。Codex #1489 R1 High)
      const broken = checkIntact(d);
      if (broken) throw Object.assign(new Error(`米国の台帳が壊れている (${broken})。バックアップから戻すまで米国の伝票を数えられない`), { code: 'US_LEDGER_BROKEN' });
    } else {
      createTables(d);
    }
  } catch (e) {
    try { d.close(); } catch { /* noop */ }
    throw e;
  }
  if (!fs.existsSync(marker)) fs.writeFileSync(marker, new Date().toISOString());
  db = d; dbFile = file;
  return db;
}

/** 既にある台帳の表・版がそろっているか。欠けていれば理由 (文字列) */
function checkIntact(d) {
  const tables = new Set(d.prepare(`SELECT name FROM sqlite_master WHERE type = 'table'`).all().map((r) => r.name));
  for (const t of ['us_ne_slips', 'us_ne_slip_events', 'us_ledger_meta']) if (!tables.has(t)) return `表 ${t} が無い`;
  const v = d.prepare(`SELECT value FROM us_ledger_meta WHERE key = 'version'`).get();
  if (!v || !Number.isSafeInteger(Number(v.value))) return '版 (us_ledger_meta.version) が無い';
  return null;
}

const nowIso = () => new Date().toISOString();
const parseJson = (s) => { try { return JSON.parse(s); } catch { return null; } };
// 依頼の中身のハッシュ (同じ request_id の再送が同じ中身かを見る)。SKU は小文字・前後の空白なし・順は問わない
export const contentHashOf = (items) => crypto.createHash('sha256').update(JSON.stringify([...(items || [])].map((i) => [String(i && i.sku).trim().toLowerCase(), Number(i && i.qty)]).sort())).digest('hex');

function rowOut(r) {
  return {
    order_no: r.order_no, seq: r.seq, request_id: r.request_id, status: r.status,
    items: parseJson(r.items_json) || [], units: parseJson(r.units_json) || [], filename: r.filename,
    created_at: r.created_at, created_by: r.created_by, left_at: r.left_at, left_by: r.left_by,
    arrived_at: r.arrived_at, arrived_by: r.arrived_by, cancelled_at: r.cancelled_at, cancelled_by: r.cancelled_by,
  };
}

/**
 * 押さえ中の数 (構成品ごと) と米国 SKU の到着待ち。日本の計算・米国の配分が読む。
 * @param {{ warehouseAtMs: number|null }} o  倉庫在庫の時点 (ms)。null = 分からない → left も引き続ける (日本に多めに残す向き)
 * @returns {{ status: 'ok'|'not_available'|'error', error?: string, version: number|null,
 *            byCode: Map<string, number>, incomingBySku: Map<string, number>, count: number, units: number, slips: object[] }}
 */
export function readUsReserved({ warehouseAtMs = null } = {}) {
  const out = { status: 'ok', error: null, version: 0, byCode: new Map(), incomingBySku: new Map(), count: 0, units: 0, slips: [] };
  let d;
  try { d = open(); } catch (e) { return { ...out, status: 'error', error: String(e.message).slice(0, 200), version: null }; }
  if (d === null) return { ...out, status: 'not_available', version: null };
  if (d === 'empty') return out;   // まだ 1 枚も出していない (印も無い) = 正常な 0 件
  try {
    out.version = Number(d.prepare(`SELECT value FROM us_ledger_meta WHERE key = 'version'`).get()?.value || 0);
    const rows = d.prepare(`SELECT * FROM us_ne_slips WHERE status IN ('reserved', 'left') ORDER BY seq`).all();
    for (const r of rows) {
      const units = parseJson(r.units_json);
      const items = parseJson(r.items_json);
      if (!Array.isArray(units) || !Array.isArray(items)) throw new Error(`伝票 ${r.order_no} の中身が壊れている`);
      const leftMs = r.left_at ? Date.parse(r.left_at) : NaN;
      // left は「倉庫在庫の時点が left_at より後」になるまで倉庫から引き続ける。時点が分からない・left_at が壊れている = 引き続ける
      const holdWarehouse = r.status === 'reserved' || !(Number.isFinite(warehouseAtMs) && Number.isFinite(leftMs) && warehouseAtMs > leftMs);
      if (holdWarehouse) {
        out.count += 1;
        for (const u of units) {
          const q = Number(u.qty);
          if (!u.code || !Number.isSafeInteger(q) || q <= 0) throw new Error(`伝票 ${r.order_no} の構成品の数がおかしい`);
          out.byCode.set(u.code, (out.byCode.get(u.code) || 0) + q);
          out.units += q;
        }
      }
      for (const it of items) {
        const k = String(it.sku).trim().toLowerCase();
        out.incomingBySku.set(k, (out.incomingBySku.get(k) || 0) + Number(it.qty));
      }
      out.slips.push({ order_no: r.order_no, status: r.status, holds_warehouse: holdWarehouse, left_at: r.left_at });
    }
  } catch (e) {
    return { ...out, status: 'error', error: String(e.message).slice(0, 200), byCode: new Map(), incomingBySku: new Map(), count: 0, units: 0, slips: [] };
  }
  return out;
}

/** 同じ request_id の伝票 (冪等)。無ければ null */
export function findByRequest(requestId) {
  const d = open();
  if (!d || d === 'empty') return null;
  const r = d.prepare(`SELECT * FROM us_ne_slips WHERE request_id = ?`).get(String(requestId));
  return r ? { ...rowOut(r), content_hash: r.content_hash, csv: r.csv } : null;
}

/**
 * 伝票を保存する (1 取引)。order_no は呼ぶ側が seq を使って作る (makeOrderNo)。
 * @returns {{ order_no, seq }}
 */
export function insertSlip({ requestId, items, units, buildCsv, by }) {
  const d = open({ create: true });
  if (!d || d === 'empty') throw Object.assign(new Error('米国の台帳はこのサーバでは使えない (Render だけ)'), { code: 'US_LEDGER_NOT_AVAILABLE' });
  const hash = contentHashOf(items);
  const tx = d.transaction(() => {
    const seq = Number(d.prepare(`SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM us_ne_slips`).get().s);
    const now = new Date();
    const { orderNo, csv, filename } = buildCsv({ seq, now });
    d.prepare(`INSERT INTO us_ne_slips (order_no, seq, request_id, content_hash, status, items_json, units_json, csv, filename, created_at, created_by, updated_at)
               VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?, ?, ?, ?, ?)`)
      .run(orderNo, seq, String(requestId), hash, JSON.stringify(items), JSON.stringify(units), csv, filename, now.toISOString(), by || null, now.toISOString());
    d.prepare(`INSERT INTO us_ne_slip_events (order_no, event, from_status, to_status, by, at) VALUES (?, 'created', NULL, 'reserved', ?, ?)`).run(orderNo, by || null, now.toISOString());
    d.prepare(`UPDATE us_ledger_meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'version'`).run();
    return { order_no: orderNo, seq, csv, filename };
  });
  return tx();
}

/**
 * 状態を変える (期待の状態を検査して 1 取引・監査の記録)。
 * @returns {object} 変えた後の伝票
 */
export function transition(orderNo, to, { expect, by, note } = {}) {
  if (!TRANSITIONS[to]) throw Object.assign(new Error(`その状態には変えられない: ${to}`), { code: 'US_LEDGER_BAD_STATUS' });
  const d = open();
  if (!d || d === 'empty') throw Object.assign(new Error('米国の台帳が無い'), { code: 'US_LEDGER_NOT_AVAILABLE' });
  const tx = d.transaction(() => {
    const r = d.prepare(`SELECT * FROM us_ne_slips WHERE order_no = ?`).get(String(orderNo));
    if (!r) throw Object.assign(new Error(`伝票が無い: ${orderNo}`), { code: 'US_LEDGER_NOT_FOUND' });
    if (expect && r.status !== expect) throw Object.assign(new Error(`伝票 ${orderNo} は今「${r.status}」(画面が古い)。読み直してください`), { code: 'US_LEDGER_CONFLICT' });
    if (!TRANSITIONS[to].includes(r.status)) throw Object.assign(new Error(`伝票 ${orderNo} は「${r.status}」なので「${to}」にできない`), { code: 'US_LEDGER_BAD_TRANSITION' });
    const now = nowIso();
    const col = { left: ['left_at', 'left_by'], arrived: ['arrived_at', 'arrived_by'], cancelled: ['cancelled_at', 'cancelled_by'] }[to];
    d.prepare(`UPDATE us_ne_slips SET status = ?, ${col[0]} = ?, ${col[1]} = ?, updated_at = ? WHERE order_no = ? AND status = ?`).run(to, now, by || null, now, r.order_no, r.status);
    d.prepare(`INSERT INTO us_ne_slip_events (order_no, event, from_status, to_status, by, note, at) VALUES (?, 'transition', ?, ?, ?, ?, ?)`).run(r.order_no, r.status, to, by || null, note ? String(note).slice(0, 300) : null, now);
    d.prepare(`UPDATE us_ledger_meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'version'`).run();
    return rowOut(d.prepare(`SELECT * FROM us_ne_slips WHERE order_no = ?`).get(r.order_no));
  });
  return tx();
}

/** 画面の一覧 (新しい順・終わったものは直近 20 件だけ) */
export function listSlips() {
  const d = open();
  if (d === null) return { status: 'not_available', slips: [] };
  if (d === 'empty') return { status: 'ok', slips: [] };
  const open_ = d.prepare(`SELECT * FROM us_ne_slips WHERE status IN ('reserved', 'left') ORDER BY seq DESC`).all();
  const done = d.prepare(`SELECT * FROM us_ne_slips WHERE status IN ('arrived', 'cancelled') ORDER BY seq DESC LIMIT 20`).all();
  return { status: 'ok', slips: [...open_, ...done].map(rowOut) };
}

/** 保存した CSV (と中身) */
export function getSlip(orderNo) {
  const d = open();
  if (!d || d === 'empty') return null;
  const r = d.prepare(`SELECT * FROM us_ne_slips WHERE order_no = ?`).get(String(orderNo));
  return r ? { ...rowOut(r), csv: r.csv } : null;
}

export const LEDGER_STATUSES = STATUSES;
