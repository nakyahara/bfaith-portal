/**
 * select-set (選べる〇種セットの明細展開) の DB 層。
 *
 * 設計 (easy-ship / shipping-work の規約に準拠):
 * - アプリ専用の select-set.db (better-sqlite3 / WAL)
 * - スキーマは PRAGMA user_version で版管理
 * - 日時カラムは UTC 'YYYY-MM-DDTHH:MM:SSZ'
 * - テーブル名は ss_ プレフィクス
 *
 * ここに持つのは「RMSから自動で取れないもの」だけ:
 *   ss_sets     … 選べるセットの商品コード一覧 (RMSを引くキー)
 *   ss_mappings … 選択肢 → 商品コード の手動補完 (ganesh・別名・文字数で切れたコード)
 *   ss_omake    … おまけの優先順位
 *   ss_rms_cache… RMS customizationOptions のキャッシュ (オンデマンド取得・TTLは service 側)
 * 選択肢そのものは RMS が正なので DB に写して持たない (写すと更新漏れが事故になる)。
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { normalizeValue } from './expand.js';
import { MANUAL_MAPPINGS, OMAKE_PRIORITY, SET_CODES } from './seed-data.js';

export const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'select-set.db');

const SCHEMA_VERSION = 2;

let db = null;

export function utcNow() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

export function initSelectSetDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) {
    try { db.close(); } catch {}
    db = null;
  }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  migrate();
  return db;
}

export function getDB() {
  if (!db) throw new Error('select-set DB が初期化されていません (initSelectSetDB を先に呼ぶ)');
  return db;
}

function migrate() {
  let v = db.pragma('user_version', { simple: true });
  if (v > SCHEMA_VERSION) {
    throw new Error(
      `select-set.db の user_version=${v} がコードの期待 ${SCHEMA_VERSION} より新しい (古いコードで起動している)`,
    );
  }
  while (v < SCHEMA_VERSION) {
    const next = v + 1;
    const step = MIGRATIONS[next];
    if (!step) throw new Error(`select-set migration v${next} が未定義`);
    db.transaction(() => {
      // Render のデプロイは新旧プロセスが重なるため、ロックを取ってから版数を読み直す
      if (db.pragma('user_version', { simple: true }) >= next) return;
      step();
      db.pragma(`user_version = ${next}`);
    }).immediate();
    const applied = db.pragma('user_version', { simple: true });
    if (applied < next) throw new Error(`select-set migration v${next} 適用後も user_version が ${applied} のまま`);
    v = applied;
  }
}

const MIGRATIONS = {
  1: () => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS ss_sets (
        set_code   TEXT PRIMARY KEY,
        label      TEXT NOT NULL DEFAULT '',
        is_active  INTEGER NOT NULL DEFAULT 1,
        note       TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS ss_mappings (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        set_code     TEXT NOT NULL,
        option_text  TEXT NOT NULL,
        -- 正規化キー。括弧・全角半角・空白・アンダースコア・＆/アンドの違いを吸収した形。
        -- 同じ意味の選択肢が二重登録されるのを DB レベルで防ぐ
        option_key   TEXT NOT NULL,
        product_code TEXT NOT NULL,
        note         TEXT,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ss_map_uq ON ss_mappings (set_code, option_key);
      CREATE INDEX IF NOT EXISTS ss_map_set_idx ON ss_mappings (set_code);

      -- おまけ (シークレットプレゼント) の優先順位。全セット共通・上から在庫のあるものを採る
      CREATE TABLE IF NOT EXISTS ss_omake (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        rank         INTEGER NOT NULL,
        product_code TEXT NOT NULL,
        note         TEXT,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ss_omake_rank_uq ON ss_omake (rank);
      CREATE UNIQUE INDEX IF NOT EXISTS ss_omake_code_uq ON ss_omake (LOWER(product_code));

      -- RMS customizationOptions のキャッシュ。error は取得失敗の記録 (失敗しても手動分で動かすため)
      CREATE TABLE IF NOT EXISTS ss_rms_cache (
        set_code   TEXT PRIMARY KEY,
        payload    TEXT,
        error      TEXT,
        fetched_at TEXT NOT NULL
      );
    `);
    seedInitialData();
  },

  // マスタ (セット・手動マッピング・おまけ) は Render 側が正で、miniPC は取りに行く。
  // その取得状況を持つための入れ物 (2026-08-08)。
  2: () => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS ss_meta (
        key        TEXT PRIMARY KEY,
        value      TEXT,
        updated_at TEXT NOT NULL
      );
    `);
  },
};

export function getMeta(key) {
  const r = getDB().prepare('SELECT value FROM ss_meta WHERE key = ?').get(String(key));
  return r ? r.value : null;
}

export function setMeta(key, value) {
  getDB().prepare(`
    INSERT INTO ss_meta (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `).run(String(key), value == null ? null : String(value), utcNow());
}

/** 初回だけ seed-data.js の内容を投入する (以後は管理画面で編集する) */
function seedInitialData() {
  const now = utcNow();
  const insSet = db.prepare(
    'INSERT OR IGNORE INTO ss_sets (set_code, label, is_active, note, created_at, updated_at) VALUES (?, ?, 1, ?, ?, ?)',
  );
  for (const code of SET_CODES) insSet.run(code, '', '初期投入', now, now);

  const insMap = db.prepare(`
    INSERT OR IGNORE INTO ss_mappings (set_code, option_text, option_key, product_code, note, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  for (const [setCode, rows] of Object.entries(MANUAL_MAPPINGS)) {
    for (const r of rows) {
      if (!r?.option || !r?.code) continue;
      insMap.run(setCode, r.option, normalizeValue(r.option), r.code, '初期投入', now, now);
    }
  }

  const insOmake = db.prepare(
    'INSERT OR IGNORE INTO ss_omake (rank, product_code, note, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
  );
  OMAKE_PRIORITY.forEach((code, i) => insOmake.run(i + 1, code, '初期投入 (原価の安い順)', now, now));
}

// ---- 読み取り ----

export function listSets({ includeInactive = false } = {}) {
  const where = includeInactive ? '' : 'WHERE is_active = 1';
  return getDB().prepare(`SELECT * FROM ss_sets ${where} ORDER BY set_code`).all();
}

export function getSet(setCode) {
  return getDB().prepare('SELECT * FROM ss_sets WHERE set_code = ?').get(String(setCode || '')) || null;
}

export function listMappings(setCode) {
  const d = getDB();
  return setCode
    ? d.prepare('SELECT * FROM ss_mappings WHERE set_code = ? ORDER BY option_text').all(setCode)
    : d.prepare('SELECT * FROM ss_mappings ORDER BY set_code, option_text').all();
}

export function listOmake() {
  return getDB().prepare('SELECT * FROM ss_omake ORDER BY rank').all();
}

export function getRmsCache(setCode) {
  return getDB().prepare('SELECT * FROM ss_rms_cache WHERE set_code = ?').get(String(setCode || '')) || null;
}

export function putRmsCache(setCode, payload, error) {
  getDB().prepare(`
    INSERT INTO ss_rms_cache (set_code, payload, error, fetched_at) VALUES (?, ?, ?, ?)
    ON CONFLICT(set_code) DO UPDATE SET payload = excluded.payload, error = excluded.error, fetched_at = excluded.fetched_at
  `).run(String(setCode), payload == null ? null : JSON.stringify(payload), error || null, utcNow());
}

// ---- 書き込み ----

export function upsertSet({ setCode, label = '', isActive = 1, note = '' }) {
  const now = utcNow();
  getDB().prepare(`
    INSERT INTO ss_sets (set_code, label, is_active, note, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(set_code) DO UPDATE SET label = excluded.label, is_active = excluded.is_active,
      note = excluded.note, updated_at = excluded.updated_at
  `).run(String(setCode).trim(), String(label || ''), isActive ? 1 : 0, String(note || ''), now, now);
}

export function deleteSet(setCode) {
  const d = getDB();
  d.transaction(() => {
    d.prepare('DELETE FROM ss_mappings WHERE set_code = ?').run(setCode);
    d.prepare('DELETE FROM ss_rms_cache WHERE set_code = ?').run(setCode);
    d.prepare('DELETE FROM ss_sets WHERE set_code = ?').run(setCode);
  })();
}

export function upsertMapping({ id, setCode, optionText, productCode, note = '' }) {
  const now = utcNow();
  const key = normalizeValue(optionText);
  if (!key) throw new Error('選択肢が空です');
  const d = getDB();
  if (id) {
    d.prepare(`
      UPDATE ss_mappings SET set_code = ?, option_text = ?, option_key = ?, product_code = ?, note = ?, updated_at = ?
      WHERE id = ?
    `).run(setCode, optionText, key, productCode, note, now, id);
    return id;
  }
  const r = d.prepare(`
    INSERT INTO ss_mappings (set_code, option_text, option_key, product_code, note, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(set_code, option_key) DO UPDATE SET
      option_text = excluded.option_text, product_code = excluded.product_code,
      note = excluded.note, updated_at = excluded.updated_at
  `).run(setCode, optionText, key, productCode, note, now, now);
  return r.lastInsertRowid;
}

export function deleteMapping(id) {
  getDB().prepare('DELETE FROM ss_mappings WHERE id = ?').run(id);
}

/**
 * マスタ (セット・手動マッピング・おまけ) を丸ごと入れ替える。
 * Render 側が正で miniPC が取りに行く運用のため、取得できた内容で完全に置き換える
 * (差分マージにすると「Renderで消した行がminiPCに残る」が起きる)。
 * ss_rms_cache は miniPC 側で育てるキャッシュなので触らない。
 */
export function replaceMaster(snapshot) {
  const problems = validateMasterSnapshot(snapshot);
  if (problems.length) {
    // 🚨 検証してから消す。以前は「両方空でなければ反映」だけだったので、
    //   mappings=[] の壊れた応答で既存マッピングとおまけを全消去できた
    //   (Codexレビュー4巡目 / 2026-08-08)
    throw new Error(`受け取ったマスタが不正です: ${problems.slice(0, 5).join(' / ')}`);
  }
  const { sets, mappings, omake } = snapshot;
  const now = utcNow();
  const d = getDB();

  // INSERT OR IGNORE ではなく素の INSERT。制約に触れたらトランザクションごと巻き戻り、
  // 既存マスタがそのまま残る (黙って行が欠けるより止まる方がよい)
  d.transaction(() => {
    d.prepare('DELETE FROM ss_mappings').run();
    d.prepare('DELETE FROM ss_sets').run();
    d.prepare('DELETE FROM ss_omake').run();

    const insSet = d.prepare(
      'INSERT INTO ss_sets (set_code, label, is_active, note, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
    );
    for (const s of sets) {
      insSet.run(String(s.set_code).trim(), String(s.label || ''),
        s.is_active === 0 ? 0 : 1, String(s.note || ''), s.created_at || now, s.updated_at || now);
    }

    const insMap = d.prepare(`
      INSERT INTO ss_mappings (set_code, option_text, option_key, product_code, note, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const m of mappings) {
      insMap.run(String(m.set_code).trim(), String(m.option_text),
        normalizeValue(m.option_text), String(m.product_code).trim(),
        String(m.note || ''), m.created_at || now, m.updated_at || now);
    }

    const insOmake = d.prepare(
      'INSERT INTO ss_omake (rank, product_code, note, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
    );
    omake.forEach((o, i) => {
      const code = typeof o === 'string' ? o : o.product_code;
      insOmake.run(i + 1, String(code).trim(), typeof o === 'string' ? '' : String(o.note || ''), now, now);
    });
  })();

  // 実際に入った件数を返す (受け取った配列の長さではなく)
  const count = (t) => d.prepare(`SELECT COUNT(*) AS n FROM ${t}`).get().n;
  const actual = { sets: count('ss_sets'), mappings: count('ss_mappings'), omake: count('ss_omake') };
  if (actual.sets !== sets.length || actual.mappings !== mappings.length || actual.omake !== omake.length) {
    throw new Error(`マスタの投入件数が合いません (受信 ${sets.length}/${mappings.length}/${omake.length}`
      + ` → 実際 ${actual.sets}/${actual.mappings}/${actual.omake})`);
  }
  return actual;
}

/**
 * 受け取ったマスタが丸ごと入れ替えてよい形か検証する。
 * 問題があれば理由の配列を返す (空なら正常)。
 * 全置換は取り返しがつかないので、ここで弾いて既存マスタを守る。
 */
export function validateMasterSnapshot(s) {
  const p = [];
  if (!s || typeof s !== 'object') return ['中身がありません'];
  for (const k of ['sets', 'mappings', 'omake']) {
    if (!Array.isArray(s[k])) p.push(`${k} が配列ではありません`);
  }
  if (p.length) return p;
  // このシステムでは手動マッピングもおまけも常に存在する。空で来たら異常とみなす
  // (Codexレビュー4巡目: sets だけ正常で mappings=[] の応答で全消去できた)
  if (!s.sets.length) p.push('セットが1件もありません');
  if (!s.mappings.length) p.push('手動マッピングが1件もありません');
  if (!s.omake.length) p.push('おまけ優先順位が1件もありません');
  if (s.sets.length > 500 || s.mappings.length > 20000 || s.omake.length > 200) {
    p.push('件数が想定を大きく超えています');
  }

  const setCodes = new Set();
  for (const x of s.sets) {
    const c = String(x?.set_code || '').trim();
    if (!c) { p.push('セット商品コードが空の行があります'); continue; }
    if (setCodes.has(c)) p.push(`セット商品コードが重複しています: ${c}`);
    setCodes.add(c);
  }

  const keys = new Set();
  for (const m of s.mappings) {
    const set = String(m?.set_code || '').trim();
    const opt = String(m?.option_text || '').trim();
    const code = String(m?.product_code || '').trim();
    if (!set || !opt || !code) { p.push('セット/選択肢/商品コードのいずれかが空の行があります'); continue; }
    if (!setCodes.has(set)) p.push(`マッピングの参照先セットがありません: ${set}`);
    const k = `${set}${normalizeValue(opt)}`;
    if (keys.has(k)) p.push(`同じ選択肢が重複しています: ${set} / ${opt}`);
    keys.add(k);
  }

  const codes = new Set();
  for (const o of s.omake) {
    const code = String(typeof o === 'string' ? o : o?.product_code || '').trim().toLowerCase();
    if (!code) { p.push('おまけの商品コードが空の行があります'); continue; }
    if (codes.has(code)) p.push(`おまけの商品コードが重複しています: ${code}`);
    codes.add(code);
  }
  return [...new Set(p)];
}

/** 現在のマスタ件数 (取得前後の比較に使う) */
export function masterCounts() {
  const d = getDB();
  const c = (t) => d.prepare(`SELECT COUNT(*) AS n FROM ${t}`).get().n;
  return { sets: c('ss_sets'), mappings: c('ss_mappings'), omake: c('ss_omake') };
}

/**
 * 取得したマスタが、今持っているものから不自然に減っていないか。
 * 全置換なので、Render側の事故で件数が激減したスナップショットをそのまま反映すると
 * 復旧が面倒になる。半分以下に減っていたら止めて人に判断させる。
 * @returns {string|null} 問題の説明 (問題なければ null)
 */
export function shrinkProblem(current, incoming) {
  const check = (name, before, after) => {
    if (before >= 5 && after < before * 0.5) {
      return `${name}が ${before} → ${after} と半分以下に減っています`;
    }
    return null;
  };
  return check('セット', current.sets, incoming.sets)
    || check('手動マッピング', current.mappings, incoming.mappings)
    || check('おまけ', current.omake, incoming.omake);
}

/** マスタ配信用のスナップショット (Render → miniPC) */
export function exportMaster() {
  const d = getDB();
  return {
    sets: d.prepare('SELECT set_code, label, is_active, note, created_at, updated_at FROM ss_sets ORDER BY set_code').all(),
    mappings: d.prepare('SELECT set_code, option_text, option_key, product_code, note, created_at, updated_at FROM ss_mappings ORDER BY set_code, option_text').all(),
    omake: d.prepare('SELECT rank, product_code, note FROM ss_omake ORDER BY rank').all(),
    exportedAt: utcNow(),
  };
}

/** おまけ優先順位を丸ごと置き換える (並べ替えは順番の入れ替えなので全置換が素直) */
export function replaceOmake(codes) {
  const now = utcNow();
  const list = [...new Set(codes.map((c) => String(c || '').trim()).filter(Boolean))];
  const d = getDB();
  d.transaction(() => {
    d.prepare('DELETE FROM ss_omake').run();
    const ins = d.prepare('INSERT INTO ss_omake (rank, product_code, note, created_at, updated_at) VALUES (?, ?, ?, ?, ?)');
    list.forEach((code, i) => ins.run(i + 1, code, '', now, now));
  })();
  return list.length;
}
