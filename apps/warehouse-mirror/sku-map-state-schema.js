/**
 * sku-map-state-schema.js — Amazon SKU の対の世代の状態の表 (mirror_sku_map_state) と trigger・足す列 (PR ⑦-0)
 * db.js の createTables から呼ぶ。受け口の決まりは sku-map-generation.js (こちらは表を作って確かめるだけ)。
 * 🚨 このファイルは import を持たない (db.js を読むアプリ = amazon-pricing の「書き込む経路が無い」試験が、たどった先を全部検査する。
 *    test-no-write-path.mjs の禁止語 (process・constructor・Buffer など) もここには書かない)
 * 🚨 trigger は CREATE TRIGGER IF NOT EXISTS なので、同じ名前のまま定義を変えても今ある DB には入らない。
 *    定義を変えるときは名前の版を上げ (_v1 → _v2)、前の名前を RETIRED_STATE_TRIGGERS に足す (新しいのを作った後に DROP する)。
 *    初期化は名前ごとに sqlite_master の定義と照らし、違えば投げる (= 初期化の失敗。受け口は 503)
 */

export const SKU_MAP_STATE_TABLE = 'mirror_sku_map_state';
const STATE_COLUMNS = Object.freeze([
  'id', 'activated', 'activated_at', 'activation_generation', 'generation', 'format', 'content_hash', 'master_rows', 'component_rows', 'applied_at',
]);
/** 状態の表の trigger (名前に版。定義を変えるときは版を上げる = 上の 🚨) */
export const STATE_TRIGGERS = Object.freeze([
  {
    name: 'trg_sku_map_state_no_delete_v1',
    body: `BEFORE DELETE ON mirror_sku_map_state
    BEGIN SELECT RAISE(ABORT, 'sku_map_state: 行は消せない (有効になった印は戻せない)'); END`,
  },
  {
    name: 'trg_sku_map_state_single_v1',
    body: `BEFORE INSERT ON mirror_sku_map_state
    WHEN EXISTS (SELECT 1 FROM mirror_sku_map_state)
    BEGIN SELECT RAISE(ABORT, 'sku_map_state: 行は 1 つだけ (入れ直しは不可)'); END`,
  },
  {
    name: 'trg_sku_map_state_forward_only_v1',
    body: `BEFORE UPDATE ON mirror_sku_map_state
    WHEN NEW.id IS NOT OLD.id
      OR NEW.activated IS NOT OLD.activated
      OR NEW.activated_at IS NOT OLD.activated_at
      OR NEW.activation_generation IS NOT OLD.activation_generation
      OR NEW.generation < OLD.generation
      OR (NEW.generation = OLD.generation AND (NEW.content_hash IS NOT OLD.content_hash OR NEW.format IS NOT OLD.format
          OR NEW.master_rows IS NOT OLD.master_rows OR NEW.component_rows IS NOT OLD.component_rows))
    BEGIN SELECT RAISE(ABORT, 'sku_map_state: 世代は下げられない・同じ世代の中身は変えられない'); END`,
  },
]);
/** 使わなくなった trigger の名前 (版を上げたら前の名前をここへ。新しいのを作った後に DROP TRIGGER IF EXISTS) */
const RETIRED_STATE_TRIGGERS = Object.freeze([]);

const squash = (s) => String(s).replace(/\s+/g, ' ').trim();

/**
 * 表と trigger を作る (何度呼んでもよい)。1 つの取引 = 途中で落ちたら何も残さない。
 *   mirror_sku_map_state: id = 1 の 1 行だけ。行がある = 有効になった (activated = 1 しか入らない)。
 *   trigger: 消す・入れ直す (INSERT OR REPLACE) ・世代を下げる・同じ世代で中身を変える・有効になった時刻を変える を止める。
 *   mirror_sku_resolved に構成の時刻の列を足す (世代つきのときだけ入る。古い送り方では NULL)。
 *   最後に「表の列・trigger の定義・足した列」が期待どおりかを確かめ、違えば投げる (= 初期化の失敗。db.js が skuMapGenerationInitError に残す)
 */
export function createSkuMapGenerationTables(db) {
  db.transaction(() => {
    db.exec(`CREATE TABLE IF NOT EXISTS mirror_sku_map_state (
      id                    INTEGER PRIMARY KEY CHECK (id = 1),
      activated             INTEGER NOT NULL CHECK (activated = 1),
      activated_at          TEXT NOT NULL,
      activation_generation INTEGER NOT NULL CHECK (typeof(activation_generation) = 'integer' AND activation_generation > 0),
      generation            INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation > 0),
      format                TEXT NOT NULL,
      content_hash          TEXT NOT NULL CHECK (length(content_hash) = 64),
      master_rows           INTEGER NOT NULL CHECK (master_rows > 0),
      component_rows        INTEGER NOT NULL CHECK (component_rows > 0),
      applied_at            TEXT NOT NULL
    )`);
    for (const t of STATE_TRIGGERS) db.exec(`CREATE TRIGGER IF NOT EXISTS ${t.name} ${t.body}`);
    for (const name of RETIRED_STATE_TRIGGERS) db.exec(`DROP TRIGGER IF EXISTS ${name}`);
    const cols = db.prepare('PRAGMA table_info(mirror_sku_resolved)').all().map((c) => c.name);
    if (!cols.includes('component_created_at')) db.exec('ALTER TABLE mirror_sku_resolved ADD COLUMN component_created_at TEXT');
    if (!cols.includes('component_updated_at')) db.exec('ALTER TABLE mirror_sku_resolved ADD COLUMN component_updated_at TEXT');
    verifySkuMapGenerationSchema(db);
  })();
}

/** 表の列・trigger の定義・足した列が期待どおりか (違えば投げる)。同じ名前で定義の違う trigger は IF NOT EXISTS では直らないのでここで気づく */
export function verifySkuMapGenerationSchema(db) {
  const stateCols = db.prepare(`PRAGMA table_info(${SKU_MAP_STATE_TABLE})`).all().map((c) => c.name);
  const missingState = STATE_COLUMNS.filter((c) => !stateCols.includes(c));
  if (missingState.length) throw new Error(`${SKU_MAP_STATE_TABLE} に列が無い: ${missingState.join(', ')}`);
  for (const t of STATE_TRIGGERS) {
    const row = db.prepare("SELECT tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND name = ?").get(t.name);
    if (!row) throw new Error(`trigger ${t.name} が無い`);
    if (row.tbl_name !== SKU_MAP_STATE_TABLE || squash(row.sql) !== squash(`CREATE TRIGGER ${t.name} ${t.body}`)) {
      throw new Error(`trigger ${t.name} の定義が期待と違う (同じ名前の古い定義が残っている = 名前の版を上げる)`);
    }
  }
  const resolvedCols = db.prepare('PRAGMA table_info(mirror_sku_resolved)').all().map((c) => c.name);
  const missingResolved = ['component_created_at', 'component_updated_at'].filter((c) => !resolvedCols.includes(c));
  if (missingResolved.length) throw new Error(`mirror_sku_resolved に列が無い: ${missingResolved.join(', ')}`);
}
