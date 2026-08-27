/**
 * shohyo-links — MF仕訳用 証憑リンク集 (専用DB: DATA_DIR/shohyo-links.db)
 * 支払い先ごとのマイページURL・証憑保存先・引き落とし日等のマスタ。
 * 初回起動時にテーブルが空なら seed/vendors.json (Notion「支払い関係リンク先」移行データ) を投入する。
 */
import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let db = null;

const FIELDS = ['name', 'url', 'storage_path', 'card_type', 'payment_timing', 'debit_day', 'receipt_source', 'note', 'fetch_method', 'fetch_note'];

/** 取得方式 (Phase 0分類)。'' = 未分類 */
export const FETCH_METHODS = ['A', 'B', 'C', 'D', 'E', 'F'];

export function getShohyoDB() {
  if (db) return db;
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  fs.mkdirSync(dataDir, { recursive: true });
  db = new Database(path.join(dataDir, 'shohyo-links.db'));
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  db.exec(`CREATE TABLE IF NOT EXISTS vendor_links (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL,
    url            TEXT NOT NULL DEFAULT '',
    storage_path   TEXT NOT NULL DEFAULT '',
    card_type      TEXT NOT NULL DEFAULT '',
    payment_timing TEXT NOT NULL DEFAULT '',
    debit_day      TEXT NOT NULL DEFAULT '',
    receipt_source TEXT NOT NULL DEFAULT '',
    note           TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_vendor_links_name ON vendor_links(name)');
  // Phase 0 (証憑取得自動化の棚卸し) で追加した列。既存DBにも冪等に足す
  addColumnIfMissing('vendor_links', 'fetch_method', "TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('vendor_links', 'fetch_note', "TEXT NOT NULL DEFAULT ''");
  seedIfEmpty();
  applyClassificationPatch();
  return db;
}

/** 列が無ければ ALTER で追加。追加したときだけ true (warehouse/db.js の同名ヘルパ準拠) */
function addColumnIfMissing(table, column, typeClause) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  if (cols.some(c => c.name === column)) return false;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeClause}`);
  return true;
}

/**
 * seed/classification.json (Phase 0の機械分類) を「fetch_method が空の行だけ」に適用する。
 * 冪等: 手動で分類・修正済みの行は上書きしない。名前+URL一致で対象行を特定。
 */
export function applyClassificationPatch() {
  const file = path.join(__dirname, 'seed', 'classification.json');
  if (!fs.existsSync(file)) return 0;
  const entries = JSON.parse(fs.readFileSync(file, 'utf8'));
  // 未分類 (method='') でもnoteは投入したいので、両フィールドが未入力の行だけを対象にする
  const upd = db.prepare(`UPDATE vendor_links
    SET fetch_method = @fetch_method, fetch_note = @fetch_note, updated_at = @updated_at
    WHERE name = @name AND url = @url AND fetch_method = '' AND fetch_note = ''
      AND (@fetch_method != '' OR @fetch_note != '')`);
  let applied = 0;
  const tx = db.transaction((items) => {
    for (const e of items) {
      applied += upd.run({ ...e, updated_at: new Date().toISOString() }).changes;
    }
  });
  tx(entries);
  if (applied > 0) console.log(`[shohyo-links] classification patch applied to ${applied} rows`);
  return applied;
}

function seedIfEmpty() {
  const count = db.prepare('SELECT COUNT(*) AS c FROM vendor_links').get().c;
  if (count > 0) return;
  const seedFile = path.join(__dirname, 'seed', 'vendors.json');
  if (!fs.existsSync(seedFile)) return;
  const rows = JSON.parse(fs.readFileSync(seedFile, 'utf8'));
  const now = new Date().toISOString();
  const ins = db.prepare(`INSERT INTO vendor_links
    (name, url, storage_path, card_type, payment_timing, debit_day, receipt_source, note, created_at, updated_at)
    VALUES (@name, @url, @storage_path, @card_type, @payment_timing, @debit_day, @receipt_source, @note, @created_at, @updated_at)`);
  const tx = db.transaction((items) => {
    for (const r of items) {
      ins.run({
        name: r.name || '', url: r.url || '', storage_path: r.storage_path || '',
        card_type: r.card_type || '', payment_timing: r.payment_timing || '',
        debit_day: r.debit_day || '', receipt_source: r.receipt_source || '',
        note: r.note || '', created_at: now, updated_at: now,
      });
    }
  });
  tx(rows.filter(r => r.name || r.url));
  console.log(`[shohyo-links] seeded ${rows.length} vendor links`);
}

export function listLinks() {
  return getShohyoDB().prepare('SELECT * FROM vendor_links ORDER BY id').all();
}

export function getLink(id) {
  return getShohyoDB().prepare('SELECT * FROM vendor_links WHERE id = ?').get(id);
}

/** 入力を許可フィールドだけに絞り、文字列化+trim+長さ制限して返す */
function sanitize(body) {
  const out = {};
  for (const f of FIELDS) {
    if (body[f] === undefined || body[f] === null) continue;
    out[f] = String(body[f]).trim().slice(0, 2000);
  }
  return out;
}

export function createLink(body) {
  const data = sanitize(body);
  if (!data.name) throw new Error('name_required');
  const now = new Date().toISOString();
  const row = { url: '', storage_path: '', card_type: '', payment_timing: '', debit_day: '', receipt_source: '', note: '', ...data, created_at: now, updated_at: now };
  const res = getShohyoDB().prepare(`INSERT INTO vendor_links
    (name, url, storage_path, card_type, payment_timing, debit_day, receipt_source, note, created_at, updated_at)
    VALUES (@name, @url, @storage_path, @card_type, @payment_timing, @debit_day, @receipt_source, @note, @created_at, @updated_at)`).run(row);
  return getLink(res.lastInsertRowid);
}

export function updateLink(id, body) {
  const data = sanitize(body);
  if (data.name === '') throw new Error('name_required');
  const keys = Object.keys(data);
  if (!keys.length) return getLink(id);
  const sets = keys.map(k => `${k} = @${k}`).join(', ');
  const res = getShohyoDB().prepare(`UPDATE vendor_links SET ${sets}, updated_at = @updated_at WHERE id = @id`)
    .run({ ...data, updated_at: new Date().toISOString(), id });
  if (res.changes === 0) return null;
  return getLink(id);
}

export function deleteLink(id) {
  return getShohyoDB().prepare('DELETE FROM vendor_links WHERE id = ?').run(id).changes === 1;
}
