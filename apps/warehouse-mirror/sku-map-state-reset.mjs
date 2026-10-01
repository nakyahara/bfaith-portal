/**
 * sku-map-state-reset.mjs — Amazon SKU の対の世代の受け口 (⑦-0) を「間違えて有効にしてしまった」ときの戻し (記録つき・手で 1 回)
 * 手順書 = db/company/README.md「Amazon SKU の対の世代の受け口」の「間違えて有効にしてしまったとき」。
 *
 * やること (--apply のときだけ。全部 1 つの取引 = BEGIN IMMEDIATE で書き込みの鍵を取ってから):
 *   ⓪ 鍵を取った後に状態を読み直し、--expect-generation / --expect-content-hash (見るだけの回に出た値) と同じかを確かめる。
 *      違えば (見た後に送り手が新しい世代を入れた・誰かが先に戻した) 何もしないで断る = 見ていない世代を消さない (Codex R1 Medium)
 *   ① 鍵の中で読んだ状態から、状態の行・表と trigger の定義・2 表の行数とハッシュを JSON に控える
 *      (DATA_DIR/sku-map-state-resets/<時刻>.json・上書きしない。status = pending → 取引が通れば committed・落ちれば aborted)
 *   ② 記録の表 mirror_sku_map_state_resets (消せない・直せない) に 誰が・いつ・なぜ・控えの場所・前の状態 (鍵の中で読んだもの) を 1 行
 *   ③ mirror_sku_map_state を DROP (trigger も一緒に消える。DROP の中の DELETE は trigger を起こさない) → 空で作り直す (有効でない)
 *   2 表 (mirror_sku_master / mirror_sku_resolved) の中身は触らない (次の世代なしの同期が今までどおり入れ替える)
 * 断る:
 *   - Render の env に SKU_MAP_ACTIVATION_ALLOWED=1 / SKU_MAP_REQUIRE_GENERATION=1 が残っている (戻してもすぐ有効に戻る / 世代なしを断り続ける)
 *   - --by・--reason・--expect-generation・--expect-content-hash が無い / 鍵の中で読んだ状態が期待と違う
 * 🚨 先に送り手を止め、処理中の要求が無いことを確かめてから (期待と違えば断るが、止めずに流すと何度も断られる)
 * 使い方 (Render の Shell。DATA_DIR は env にある):
 *   node apps/warehouse-mirror/sku-map-state-reset.mjs                     # 見るだけ (何も書かない)。今の世代とハッシュが出る
 *   node apps/warehouse-mirror/sku-map-state-reset.mjs --apply --by "名前" --reason "なぜ" --expect-generation <世代> --expect-content-hash <ハッシュ>
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { SKU_MAP_HASH_RE, parseSkuMapGeneration } from '../../lib/sku-map-canonical.js';
import { readSkuMapState, stateForResponse, storedSkuMapHash, createSkuMapGenerationTables, skuMapReceiverSettings } from './sku-map-generation.js';

export class SkuMapResetRefused extends Error {
  constructor(code, message) { super(message); this.code = code; }
}

/** 記録の表 (戻しの回だけが作る。試験も同じ定義を使う) */
export const SKU_MAP_RESET_AUDIT_DDL = Object.freeze([
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
]);

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
 * @param {{ apply?: boolean, by?: string, reason?: string, expectGeneration?: string, expectContentHash?: string,
 *   env?: object, backupDir: string, now?: Date, beforeLockForTest?: () => void }} opts
 *   beforeLockForTest = 試験だけが使う (確かめの後・鍵を取る前に送り手が世代を入れた場合を作る)
 * @returns {{ action: 'none'|'dry_run'|'reset', before: object, backup_file?: string, audit_id?: number }}
 */
export function resetSkuMapState(db, {
  apply = false, by = '', reason = '', expectGeneration, expectContentHash, env = process.env, backupDir, now = new Date(), beforeLockForTest = null,
}) {
  if (!apply) {
    const state = readSkuMapState(db);   // 読めなければ投げる (分からないまま消さない)
    if (!state.activated) return { action: 'none', before: stateForResponse(state) };
    return { action: 'dry_run', before: stateForResponse(state), snapshot: snapshot(db, state) };
  }
  const settings = skuMapReceiverSettings(env);
  if (settings.activation_allowed || settings.require_generation) {
    throw new SkuMapResetRefused('RESET_ENV_STILL_SET',
      'Render の env に SKU_MAP_ACTIVATION_ALLOWED=1 / SKU_MAP_REQUIRE_GENERATION=1 が残っている → 先に Environment から消す (再起動する) → 新しい Shell でもう一度');
  }
  const who = String(by || '').trim(), why = String(reason || '').trim();
  if (!who || !why) throw new SkuMapResetRefused('RESET_NEEDS_BY_REASON', '--by (誰が) と --reason (なぜ) が要る');
  const expGen = parseSkuMapGeneration(expectGeneration);
  if (expGen === null || typeof expectContentHash !== 'string' || !SKU_MAP_HASH_RE.test(expectContentHash)) {
    throw new SkuMapResetRefused('RESET_NEEDS_EXPECT', '--expect-generation (10 進) と --expect-content-hash (16 進 64 文字) が要る = 見るだけの回に出た今の値');
  }

  const at = now.toISOString();
  fs.mkdirSync(backupDir, { recursive: true });
  const backupFile = path.join(backupDir, `sku-map-state-reset-${at.replace(/[:.]/g, '-')}.json`);
  let backup = null;   // 控えに書いた中身 (取引の後に status を書き換える)
  if (beforeLockForTest) beforeLockForTest();
  let auditId;
  try {
    auditId = db.transaction(() => {
      // 鍵を取った後に読み直す。この後は送り手が世代を入れられない (BEGIN IMMEDIATE)
      const state = readSkuMapState(db);
      const current = stateForResponse(state);
      if (!state.activated) throw new SkuMapResetRefused('RESET_STATE_CHANGED', '有効になっていない (先に誰かが戻した?) → 何もしない');
      if (state.generation !== expGen || state.content_hash !== expectContentHash) {
        throw new SkuMapResetRefused('RESET_STATE_CHANGED',
          `見るだけの回の後に状態が変わった (今 = 世代 ${current.generation}・${current.content_hash} / 期待 = 世代 ${expGen}・${expectContentHash}) → 何もしない。送り手を止めて見るだけからやり直す`);
      }
      const pending = { status: 'pending', reset_at: at, reset_by: who, reason: why, ...snapshot(db, state) };
      fs.writeFileSync(backupFile, JSON.stringify(pending, null, 2), { flag: 'wx' });   // 上書きしない (同じ名前があれば投げる = 前の回の控えを触らない)
      backup = pending;   // 書けた後だけ (書けなかったときに、前の回の控えを aborted で上書きしない)
      for (const sql of SKU_MAP_RESET_AUDIT_DDL) db.exec(sql);
      const r = db.prepare('INSERT INTO mirror_sku_map_state_resets (reset_at, reset_by, reason, backup_file, previous_state) VALUES (?,?,?,?,?)')
        .run(at, who, why, backupFile, JSON.stringify(current));
      db.exec('DROP TABLE mirror_sku_map_state');
      createSkuMapGenerationTables(db);
      if (readSkuMapState(db).activated) throw new Error('作り直した後も有効のまま (戻せていない)');
      return Number(r.lastInsertRowid);
    }).immediate();
  } catch (e) {
    // 控えを書いた後に取引が落ちた = 何も戻していない。控えを「取りやめ」にする (戻したように読めないように)
    if (backup) {
      try {
        fs.writeFileSync(backupFile, JSON.stringify({ ...backup, status: 'aborted', error: String(e.message || e) }, null, 2));
      } catch { try { fs.unlinkSync(backupFile); } catch { /* 消せなければそのまま (status は pending) */ } }
    }
    throw e;
  }
  fs.writeFileSync(backupFile, JSON.stringify({ ...backup, status: 'committed', audit_id: auditId }, null, 2));
  return { action: 'reset', before: backup.state, backup_file: backupFile, audit_id: auditId };
}

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

async function main() {
  const { initMirrorDB } = await import('./db.js');
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const db = initMirrorDB();
  try {
    const out = resetSkuMapState(db, {
      apply: process.argv.includes('--apply'), by: arg('--by'), reason: arg('--reason'),
      expectGeneration: arg('--expect-generation'), expectContentHash: arg('--expect-content-hash'),
      backupDir: path.join(dataDir, 'sku-map-state-resets'),
    });
    console.log(JSON.stringify(out, null, 2));
    if (out.action === 'none') console.log('有効になっていない (戻すものは無い)');
    if (out.action === 'dry_run') {
      console.log('見るだけ (何も書いていない)。送り手を止め、処理中の要求が無いことを確かめてから:');
      console.log(`  --apply --by "名前" --reason "なぜ" --expect-generation ${out.before.generation} --expect-content-hash ${out.before.content_hash}`);
    }
    if (out.action === 'reset') console.log('戻した。次: GET /api/sync/sku-map/state が activated: false・次の朝の同期 (世代なし) が 200 になるのを見る');
  } finally { db.close(); }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((e) => { console.error(`[sku-map-state-reset] ${e.code || 'ERROR'}: ${e.message}`); process.exitCode = 1; });
}
