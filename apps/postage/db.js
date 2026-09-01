/**
 * 郵便料金判定 (postage) — DB 層
 *
 * 正本 = AI_reference『システム設計/定形外郵便_料金区分の自動判定と印字_要件定義_20260830.md』
 *
 * 目的 (PR1a): 商品の重さ・厚み・資材のマスタを持ち、定形外の伝票 1 通ごとに
 *   「定形110円 / 規格内◯円 / 規格外◯円 / 不明」を判定できるようにする。
 *   印字 (ラベル CSV への列追加) は PR1b。判定ログの永続化も PR1b。
 *
 * 設計 (Codex R1/R2 反映):
 *   - 専用DB postage.db を DATA_DIR に持つ (warehouse.db は読み取り専用で参照するだけ)
 *   - **金額は料金表 (pm_tariff_bands) にだけ置く**。マスタや判定ルールに金額を持たせない
 *     (郵便料金の改定で全レコード修正になるため)
 *   - 料金表は有効期間つき。過去日の再判定が壊れない
 *   - 商品重量は **未登録を許す** (NULL)。埋まっていない = 判定できない、を素直に表す。
 *     欠測を 0 やデフォルト値で埋めると、静かに間違った料金が出る
 *   - マスタは版管理せず updated_at のみ (R2 で削ぎ落とした層。1伝票=1通・状態機械なしと同じ判断)
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'postage.db');

const NOW = "strftime('%Y-%m-%dT%H:%M:%SZ','now')";

let db = null;

export function initPostageDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close 済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  // PRAGMA は接続単位。foreign_keys は SQLite 既定 OFF なので毎接続で明示
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  createTables();
  seedIfEmpty();
  return db;
}

export function getDB() {
  if (!db) initPostageDB();
  return db;
}

/** テストの後片付け用。Windows は開いたままだと WAL ファイルを消せない。 */
export function closePostageDB() {
  if (!db) return;
  try { db.close(); } catch { /* 既に閉じている場合は無視 */ }
  db = null;
}

function createTables() {
  // ─── 料金表 ───────────────────────────────────────────────
  // 金額の正本。改定時は新しい version を足し、旧 version に valid_to を入れる。
  db.exec(`CREATE TABLE IF NOT EXISTS pm_tariff_versions (
    tariff_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT NOT NULL,
    valid_from        TEXT NOT NULL,               -- 'YYYY-MM-DD'
    valid_to          TEXT,                        -- NULL = 現行
    source_url        TEXT,
    created_at        TEXT NOT NULL DEFAULT (${NOW}),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS pm_tariff_bands (
    tariff_band_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    tariff_version_id INTEGER NOT NULL REFERENCES pm_tariff_versions(tariff_version_id),
    mail_type         TEXT NOT NULL CHECK (mail_type IN ('teikei','kikakunai','kikakugai')),
    band_code         TEXT NOT NULL,
    display_name      TEXT NOT NULL,               -- シールに印字する文言
    max_weight_g      INTEGER NOT NULL CHECK (max_weight_g > 0),
    amount_yen        INTEGER NOT NULL CHECK (amount_yen >= 0),
    UNIQUE (tariff_version_id, band_code)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS ix_pm_tariff_bands_lookup
    ON pm_tariff_bands(tariff_version_id, mail_type, max_weight_g)`);

  // ─── 商品マスタ ───────────────────────────────────────────
  // Excel『定形外の重さ.xlsx』の取込先。重量・厚みは **NULL を許す**。
  // default_material_code = その商品をいつも入れる資材 (印字時点では資材が未確定なため、
  // ここが「予測」の根拠になる)。商品名サフィックス (_長3封 等) からも導出できる。
  db.exec(`CREATE TABLE IF NOT EXISTS pm_skus (
    sku_code              TEXT PRIMARY KEY,
    display_name          TEXT,
    unit_weight_g         REAL CHECK (unit_weight_g IS NULL OR unit_weight_g >= 0),
    thickness_mm          REAL CHECK (thickness_mm IS NULL OR thickness_mm >= 0),
    default_material_code TEXT REFERENCES pm_materials(material_code),
    material_source       TEXT CHECK (material_source IS NULL OR material_source IN ('explicit','name_suffix')),
    weight_source         TEXT CHECK (weight_source IS NULL OR weight_source IN ('measured','estimated','supplier')),
    note                  TEXT,
    updated_at            TEXT NOT NULL DEFAULT (${NOW}),
    updated_by            TEXT
  )`);

  // ─── 資材マスタ ───────────────────────────────────────────
  // 外寸は「サイズ区分 (定形 / 規格内 / 規格外) を決める」ためだけに使う。
  // 未測定は NULL。NULL のまま推測しない (判定は unknown に落ちる)。
  db.exec(`CREATE TABLE IF NOT EXISTS pm_materials (
    material_code     TEXT PRIMARY KEY,
    display_name      TEXT NOT NULL,
    tare_weight_g     REAL CHECK (tare_weight_g IS NULL OR tare_weight_g >= 0),
    outer_length_mm   REAL CHECK (outer_length_mm IS NULL OR outer_length_mm > 0),
    outer_width_mm    REAL CHECK (outer_width_mm IS NULL OR outer_width_mm > 0),
    dims_verified     INTEGER NOT NULL DEFAULT 0 CHECK (dims_verified IN (0,1)),
    active            INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    note              TEXT,
    updated_at        TEXT NOT NULL DEFAULT (${NOW}),
    updated_by        TEXT
  )`);

  // ─── 1通あたりの固定加算 (送り状シール・納品書など) ────────
  db.exec(`CREATE TABLE IF NOT EXISTS pm_overheads (
    code       TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    weight_g   REAL NOT NULL CHECK (weight_g >= 0),
    active     INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    note       TEXT,
    updated_at TEXT NOT NULL DEFAULT (${NOW}),
    updated_by TEXT
  )`);

  // ─── 設定 (境界マージン等) ────────────────────────────────
  db.exec(`CREATE TABLE IF NOT EXISTS pm_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    note       TEXT,
    updated_at TEXT NOT NULL DEFAULT (${NOW}),
    updated_by TEXT
  )`);

  // ─── 取込の記録 ───────────────────────────────────────────
  // 「取り込んだら黙って直っていた/壊れていた」を作らないための証跡。
  // 検証で弾いた行は pm_import_issues に残し、画面で直せるようにする。
  db.exec(`CREATE TABLE IF NOT EXISTS pm_import_runs (
    import_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name   TEXT NOT NULL,
    sheet_name    TEXT,
    row_count     INTEGER NOT NULL DEFAULT 0,
    applied_count INTEGER NOT NULL DEFAULT 0,
    issue_count   INTEGER NOT NULL DEFAULT 0,
    dry_run       INTEGER NOT NULL DEFAULT 1 CHECK (dry_run IN (0,1)),
    started_at    TEXT NOT NULL DEFAULT (${NOW}),
    finished_at   TEXT,
    imported_by   TEXT
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS pm_import_issues (
    issue_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    import_run_id INTEGER NOT NULL REFERENCES pm_import_runs(import_run_id) ON DELETE CASCADE,
    row_no        INTEGER,
    sku_code      TEXT,
    severity      TEXT NOT NULL CHECK (severity IN ('error','warn')),
    kind          TEXT NOT NULL,
    column_name   TEXT,
    raw_value     TEXT,
    message       TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS ix_pm_import_issues_run
    ON pm_import_issues(import_run_id, severity)`);
}

/**
 * 初回のみ投入する種データ。
 * 既にレコードがあれば触らない (デプロイのたびに人の修正を上書きしないため)。
 */
function seedIfEmpty() {
  const hasTariff = db.prepare('SELECT COUNT(*) n FROM pm_tariff_versions').get().n > 0;
  if (!hasTariff) seedTariff();

  const hasMaterials = db.prepare('SELECT COUNT(*) n FROM pm_materials').get().n > 0;
  if (!hasMaterials) seedMaterials();

  const hasOverhead = db.prepare('SELECT COUNT(*) n FROM pm_overheads').get().n > 0;
  if (!hasOverhead) {
    // 送り状シール = 0.5g (2026-08-30 中原さん実測)。1伝票=1通なので 1 通あたり 1 枚。
    db.prepare(`INSERT INTO pm_overheads (code, name, weight_g, note, updated_by)
      VALUES ('okurijo_seal', '送り状シール', 0.5, '2026-08-30 中原さん実測', 'seed')`).run();
  }

  const hasSettings = db.prepare('SELECT COUNT(*) n FROM pm_settings').get().n > 0;
  if (!hasSettings) {
    const ins = db.prepare('INSERT INTO pm_settings (key, value, note, updated_by) VALUES (?,?,?,?)');
    // 境界マージンは「率」ではなく「グラム」で持つ (Codex R1)。
    // 率にすると 50g で 2.5g・1kg で 50g となり、上の区分ほど過剰に警告が出る。
    ins.run('boundary_margin_g', '5',
      '推定重量がこの値以内で次の料金境界に届くなら「不明 (要実測)」にする', 'seed');
    ins.run('thickness_margin_mm', '1',
      '厚みがこの値以内でサイズ境界 (定形10mm / 規格内30mm) に届くなら「不明」にする', 'seed');
  }
}

/** 2024年10月改定の料金表 (2026-08-30 時点で現行)。*/
function seedTariff() {
  const vid = db.prepare(`INSERT INTO pm_tariff_versions (name, valid_from, source_url)
    VALUES (?, ?, ?)`).run(
    '2024年10月改定', '2024-10-01',
    'https://www.post.japanpost.jp/send/domestic/charge/list/one_two.html',
  ).lastInsertRowid;

  const ins = db.prepare(`INSERT INTO pm_tariff_bands
    (tariff_version_id, mail_type, band_code, display_name, max_weight_g, amount_yen)
    VALUES (?,?,?,?,?,?)`);
  const rows = [
    ['teikei',     'teikei_50',      '定形 50g以内',           50,   110],
    ['kikakunai',  'kikakunai_50',   '定形外 規格内 50g以内',   50,   140],
    ['kikakunai',  'kikakunai_100',  '定形外 規格内 100g以内',  100,  180],
    ['kikakunai',  'kikakunai_150',  '定形外 規格内 150g以内',  150,  270],
    ['kikakunai',  'kikakunai_250',  '定形外 規格内 250g以内',  250,  320],
    ['kikakunai',  'kikakunai_500',  '定形外 規格内 500g以内',  500,  510],
    ['kikakunai',  'kikakunai_1000', '定形外 規格内 1kg以内',   1000, 750],
    ['kikakugai',  'kikakugai_50',   '定形外 規格外 50g以内',   50,   260],
    ['kikakugai',  'kikakugai_100',  '定形外 規格外 100g以内',  100,  290],
    ['kikakugai',  'kikakugai_150',  '定形外 規格外 150g以内',  150,  390],
    ['kikakugai',  'kikakugai_250',  '定形外 規格外 250g以内',  250,  450],
    ['kikakugai',  'kikakugai_500',  '定形外 規格外 500g以内',  500,  660],
    ['kikakugai',  'kikakugai_1000', '定形外 規格外 1kg以内',   1000, 920],
    ['kikakugai',  'kikakugai_2000', '定形外 規格外 2kg以内',   2000, 1350],
    ['kikakugai',  'kikakugai_4000', '定形外 規格外 4kg以内',   4000, 1750],
  ];
  const tx = db.transaction(() => { for (const r of rows) ins.run(vid, ...r); });
  tx();
}

/**
 * 実出荷で使われている資材。2026-06〜08 の定形外 5,036 通では
 * 茶封筒 59.1% / 白プチ 23.5% / 白ビ袋 14.2% の 3 種類しか出ていない。
 *
 * 外寸は茶封筒のみ「長形3号 (235×120mm)」として入れる。これは商品名サフィックス `_長3封`
 * からの推定なので dims_verified=0 (要確認)。白プチ・白ビ袋は **測るまで NULL**。
 * NULL のままだとサイズ区分が決まらず判定は「不明」に落ちる — それが正しい挙動。
 */
function seedMaterials() {
  const ins = db.prepare(`INSERT INTO pm_materials
    (material_code, display_name, tare_weight_g, outer_length_mm, outer_width_mm, dims_verified, note, updated_by)
    VALUES (?,?,?,?,?,?,?,'seed')`);
  const tx = db.transaction(() => {
    ins.run('chabuto', '茶封筒', 5.0, 235, 120, 0, '長形3号 (235×120mm) と推定。要実測');
    ins.run('shiropuchi', '白プチ', 10.0, null, null, 0, '外寸 未測定');
    ins.run('shirobi', '白ビ袋', 11.0, null, null, 0, '外寸 未測定');
  });
  tx();
}

/** 商品名の末尾サフィックス → 資材コード。実データで資材列と 90.3% 一致した。 */
export const NAME_SUFFIX_TO_MATERIAL = {
  長3封: 'chabuto',
  梱機プ: 'shiropuchi',
  白プチ: 'shiropuchi',
  白ビ袋: 'shirobi',
  白ビ: 'shirobi',
};

/** Excel の資材列 (日本語表記) → 資材コード。 */
export const MATERIAL_NAME_TO_CODE = {
  茶封筒: 'chabuto',
  白プチ: 'shiropuchi',
  白ビ袋: 'shirobi',
};

export function getSetting(key, fallback) {
  const r = getDB().prepare('SELECT value FROM pm_settings WHERE key=?').get(key);
  if (!r) return fallback;
  const n = Number(r.value);
  return Number.isFinite(n) ? n : fallback;
}

/** 指定日に有効な料金表。日付を省略したら今日 (JST)。 */
export function getTariffVersionFor(dateStr) {
  const d = dateStr || jstToday();
  return getDB().prepare(`
    SELECT * FROM pm_tariff_versions
     WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
     ORDER BY valid_from DESC LIMIT 1
  `).get(d, d) || null;
}

export function getBands(tariffVersionId) {
  return getDB().prepare(`
    SELECT * FROM pm_tariff_bands WHERE tariff_version_id=?
     ORDER BY CASE mail_type WHEN 'teikei' THEN 1 WHEN 'kikakunai' THEN 2 ELSE 3 END, max_weight_g
  `).all(tariffVersionId);
}

export function getMaterialsMap() {
  const m = new Map();
  for (const r of getDB().prepare('SELECT * FROM pm_materials').all()) m.set(r.material_code, r);
  return m;
}

export function getOverheadTotalG() {
  const r = getDB().prepare('SELECT COALESCE(SUM(weight_g),0) g FROM pm_overheads WHERE active=1').get();
  return r.g;
}

/** JST の今日 (YYYY-MM-DD)。toISOString は UTC なので月初がずれる — 必ずこれを使う。 */
export function jstToday() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 3600 * 1000);
  return jst.toISOString().slice(0, 10);
}
