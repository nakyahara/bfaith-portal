/**
 * sku-map-state-reset.mjs — Amazon SKU の対の世代の受け口 (⑦-0) を「間違えて有効にしてしまった」ときの戻し (記録つき・手で 1 回)
 * 手順書 = db/company/README.md「Amazon SKU の対の世代の受け口」の「間違えて有効にしてしまったとき」。
 *
 * やること (--apply のときだけ。1 つの取引):
 *   ① 戻す前の状態の行・表と trigger の定義・2 表の行数とハッシュを JSON に控える (DATA_DIR/sku-map-state-resets/<時刻>.json・上書きしない)
 *   ② 記録の表 mirror_sku_map_state_resets (消せない・直せない) に 誰が・いつ・なぜ・控えの場所・前の状態 を 1 行
 *   ③ mirror_sku_map_state を DROP (trigger も一緒に消える。DROP の中の DELETE は trigger を起こさない) → 空で作り直す (有効でない)
 *   2 表 (mirror_sku_master / mirror_sku_resolved) の中身は触らない (次の世代なしの同期が今までどおり入れ替える)
 * 断る:
 *   - Render の env に SKU_MAP_ACTIVATION_ALLOWED=1 / SKU_MAP_REQUIRE_GENERATION=1 が残っている (戻してもすぐ有効に戻る / 世代なしを断り続ける)
 *   - --by と --reason が無い
 * 使い方 (Render の Shell。DATA_DIR は env にある):
 *   node apps/warehouse-mirror/sku-map-state-reset.mjs                                        # 見るだけ (何も書かない)
 *   node apps/warehouse-mirror/sku-map-state-reset.mjs --apply --by "名前" --reason "なぜ戻すか"
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readSkuMapState, stateForResponse, storedSkuMapHash, createSkuMapGenerationTables, skuMapReceiverSettings } from './sku-map-generation.js';

export class SkuMapResetRefused extends Error {
  constructor(code, message) { super(message); this.code = code; }
}

const AUDIT_DDL = [
  `CREATE TABLE IF NOT EXISTS mirror_sku_map_state_resets (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    reset_at       TEXT NOT NULL,
    reset_by       TEXT NOT NULL CHECK (length(trim(reset_by)) > 0),
    reason         TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    backup_file    TEXT NOT NULL,
    previous_state TEXT NOT NULL
  )`,
  `CREATE TRIGGER IF NOT EXISTS trg_sku_map_state_resets_no_delete_v1 BEFORE DELETE ON mirror_sku_map_state_resets
    BEGIN SELECT RAISE(ABORT, 'sku_map_state_resets: 記録は消せない'); END`,
  `CREATE TRIGGER IF NOT EXISTS trg_sku_map_state_resets_no_update_v1 BEFORE UPDATE ON mirror_sku_map_state_resets
    BEGIN SELECT RAISE(ABORT, 'sku_map_state_resets: 記録は直せない'); END`,
];

/** 控えに書く中身 (状態の行・表と trigger の定義・2 表の行数とハッシュ) */
function snapshot(db, state) {
  const count = (t) => db.prepare(`SELECT count(*) AS n FROM ${t}`).get().n;
  return {
    state: stateForResponse(state),
    schema: db.prepare("SELECT type, name, sql FROM sqlite_master WHERE tbl_name = 'mirror_sku_map_state' ORDER BY type, name").all(),
    tables: { mirror_sku_master: count('mirror_sku_master'), mirror_sku_resolved: count('mirror_sku_resolved'), stored_content_hash: storedSkuMapHash(db) },
  };
}

/**
 * @param {import('better-sqlite3').Database} db warehouse-mirror.db
 * @param {{ apply?: boolean, by?: string, reason?: string, env?: object, backupDir: string, now?: Date }} opts
 * @returns {{ action: 'none'|'dry_run'|'reset', before: object, backup_file?: string, audit_id?: number }}
 */
export function resetSkuMapState(db, { apply = false, by = '', reason = '', env = process.env, backupDir, now = new Date() }) {
  const settings = skuMapReceiverSettings(env);
  const state = readSkuMapState(db);   // 読めなければ投げる (分からないまま消さない)
  const before = stateForResponse(state);
  if (!state.activated) return { action: 'none', before };
  if (!apply) return { action: 'dry_run', before, snapshot: snapshot(db, state) };
  if (settings.activation_allowed || settings.require_generation) {
    throw new SkuMapResetRefused('RESET_ENV_STILL_SET',
      'Render の env に SKU_MAP_ACTIVATION_ALLOWED=1 / SKU_MAP_REQUIRE_GENERATION=1 が残っている → 先に Environment から消す (再起動する) → 新しい Shell でもう一度');
  }
  const who = String(by || '').trim(), why = String(reason || '').trim();
  if (!who || !why) throw new SkuMapResetRefused('RESET_NEEDS_BY_REASON', '--by (誰が) と --reason (なぜ) が要る');

  const at = now.toISOString();
  fs.mkdirSync(backupDir, { recursive: true });
  const backupFile = path.join(backupDir, `sku-map-state-reset-${at.replace(/[:.]/g, '-')}.json`);
  const snap = snapshot(db, state);
  fs.writeFileSync(backupFile, JSON.stringify({ reset_at: at, reset_by: who, reason: why, ...snap }, null, 2), { flag: 'wx' });   // 上書きしない

  const auditId = db.transaction(() => {
    for (const sql of AUDIT_DDL) db.exec(sql);
    const r = db.prepare('INSERT INTO mirror_sku_map_state_resets (reset_at, reset_by, reason, backup_file, previous_state) VALUES (?,?,?,?,?)')
      .run(at, who, why, backupFile, JSON.stringify(before));
    db.exec('DROP TABLE mirror_sku_map_state');
    createSkuMapGenerationTables(db);
    if (readSkuMapState(db).activated) throw new Error('作り直した後も有効のまま (戻せていない)');
    return Number(r.lastInsertRowid);
  }).immediate();
  return { action: 'reset', before, backup_file: backupFile, audit_id: auditId };
}

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

async function main() {
  const { initMirrorDB } = await import('./db.js');
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const db = initMirrorDB();
  const out = resetSkuMapState(db, {
    apply: process.argv.includes('--apply'), by: arg('--by'), reason: arg('--reason'), backupDir: path.join(dataDir, 'sku-map-state-resets'),
  });
  console.log(JSON.stringify(out, null, 2));
  if (out.action === 'none') console.log('有効になっていない (戻すものは無い)');
  if (out.action === 'dry_run') console.log('見るだけ (何も書いていない)。戻すなら --apply --by "名前" --reason "なぜ"');
  if (out.action === 'reset') console.log('戻した。次: GET /api/sync/sku-map/state が activated: false・次の朝の同期 (世代なし) が 200 になるのを見る');
  db.close();
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((e) => { console.error(`[sku-map-state-reset] ${e.code || 'ERROR'}: ${e.message}`); process.exitCode = 1; });
}
