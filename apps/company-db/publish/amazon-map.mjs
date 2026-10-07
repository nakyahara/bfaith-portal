/**
 * amazon-map.mjs — Company DB の Amazon SKU の対応 → miniPC の古い表 (m_sku_master / m_sku_components) への写し (⑦-2 PR-A)
 *   設計 = AI_reference CompanyDB構想/16 §2「古い表への写し」・最小の計画 (2026-10-07) と Codex の計画レビュー R2・中原さんの決定 (10/7)
 *
 * なぜ: 切替の後、Amazon の seller SKU ↔ NE コードの正は Company DB (core.amazon_sku_maps の active + core.listing_components)。
 *   読み手 (f_sales・想定利益・手数料・Render の mirror → FBA 補充・GAS) は今のまま古い表を読む = 古い表を「Company DB の写し」にする。
 * 形 = **毎回、Company DB の今の active の対応にまるごと合わせる** (世代・前の中身を取っておく箱は持たない = 何度流しても同じ答え):
 *   1 持ち主を読む (DB の active の listing_components.amazon。config は見ない)。load (今の本番) = 何もしない (SQLite を開かない・書かない・exit 0)
 *   2 company = 実行の鍵 (warehouse.db の job_locks 'cdb-amazon-map-publish') を取ってから、watcher の読むだけの 1 取引 (REPEATABLE READ) で
 *     対応 (readCompanyAmazonMapCanon = active だけ・時刻は UTC の ISO・Z・ミリ秒) と変更の記録の最大の番号を読む
 *     (daily-sync・自動再試行・手の CLI は同じこのファイル = 同じ鍵。鍵は PG を読む前から SQLite の commit の後まで持つ)
 *   3 安全弁 (断る = 何も書かない・exit 1): 0 件 / 今の古い表の行数の 90% 未満 / 変更の記録の番号が前の写しより小さい (Company DB をバックアップから戻した?)
 *     / 受け手の決まり (validateSkuMap) に合わない。
 *     意図した大量削除だけは手の CLI の --allow-shrink --expect-hash <今回の Company DB のハッシュ> で通す (0 件と 90% だけ。--daily では使えない)。
 *     復元の後は、止める env (CDB_AMAZON_MAP_PUBLISH_PAUSE=1) を入れて --dry-run で差を人が見る → env を外して --accept-restore --expect-hash で再開 (README)
 *   4 SQLite の 1 取引 (IMMEDIATE) で差だけ入れる (消すのは構成 → 親・入れるのは親 → 構成)。commit の前に読み直してハッシュを照らす (違えば巻き戻す)・
 *     鍵がまだ自分のものかも照らす。sync_meta 'cdb_amazon_map_publish' にハッシュ・行数・変更の記録の番号を残す
 *   5 前の写しの後に古い表が誰かに書き換えられていた (今のハッシュ ≠ 前の写しのハッシュ) = ⚠️ で知らせて、そのまま Company DB の値で上書きする
 * 書かないもの: fba.db の sku_mapping (Sheet の写し・凍結)・Render (今の送り方 sync-to-render のまま)・Company DB (watcher = select だけ)
 * 作る値: created_by / updated_by (ハッシュの外) は、行を足す・直すときだけ Company DB の registered_by / changed_by を入れる (変わらない行は触らない)
 *
 * 使い方 (miniPC・リポジトリ直下):
 *   node apps/company-db/publish/amazon-map.mjs --daily                         daily-sync・自動再試行の回 (工程「CompanyDB写し(Amazon SKU)」)
 *   node -r dotenv/config apps/company-db/publish/amazon-map.mjs                急ぎのときに手で写す (同じ鍵・同じ安全弁)
 *   node -r dotenv/config apps/company-db/publish/amazon-map.mjs --dry-run      読んで差を数えるだけ (鍵も取らない・書かない。止めている間も流せる)
 *   ... --allow-shrink --expect-hash <H>     意図した大量削除 (0 件・90% 未満) を通す (手だけ。H = --dry-run が出す Company DB のハッシュ)
 *   ... --accept-restore --expect-hash <H>   Company DB を戻した後、差を人が見てから再開 (変更の記録の番号が戻ったのを通す。手だけ)
 * env: COMPANY_DB_WATCH_URL (watcher) / DATA_DIR (warehouse.db) / CDB_AMAZON_MAP_PUBLISH_PAUSE=1 (止める = ⚠️ 見送り・古い表は前の形のまま)
 * 終わり方: 写した・変わらない・持ち主が load・止めている = exit 0 / 断った・読めない (持ち主が company のはず)・鍵が取れない = exit 1 (daily-sync の retry に載る)。
 *   ping は打たない (台帳 warehouse-daily-sync の 1 工程。成否は daily-sync の要約・retry の通知に出る)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MASTER_OWNERSHIP } from '../../../config/master-ownership.mjs';
import { readOwnershipState } from '../load/ownership-state.mjs';
import { readCompanyAmazonMapCanon, AMAZON_MAP_OWNER_KEY } from '../../../lib/amazon-map-write.mjs';
import { fromMiniPcRows, skuMapDigest, validateSkuMap, SKU_MAP_HASH_RE, SKU_MAP_CANON_FORMAT } from '../../../lib/sku-map-canonical.js';
import { acquireLock, releaseLock } from '../../warehouse/job-locks.js';

export const STEP_NAME = 'CompanyDB写し(Amazon SKU)';   // daily-sync の工程の名前 = retry-failed-jobs.js の名前
export const META_KEY = 'cdb_amazon_map_publish';
export const LOCK_NAME = 'cdb-amazon-map-publish';
export const LOCK_TTL_MS = 20 * 60 * 1000;
export const PAUSE_ENV = 'CDB_AMAZON_MAP_PUBLISH_PAUSE';
/** 今の古い表の行数に比べてこれより少ない = 読み落とし・大量の墓標を疑う (意図したものは --allow-shrink) */
export const SHRINK_MIN_RATIO = 0.9;
export const EXIT = Object.freeze({ ok: 0, error: 1 });
const SAMPLE = 10;

const fail = (message, code) => Object.assign(new Error(message), { code });

// ─── 古い表 (SQLite) ───

/** 古い表の 2 つの表を読む (migrate の readLegacyAmazonMaps と同じ列) */
export function readLegacyRows(sqlite) {
  const masterRows = sqlite.prepare('SELECT seller_sku, 商品名, created_at, updated_at, created_by, updated_by FROM m_sku_master').all();
  const componentRows = sqlite.prepare('SELECT seller_sku, ne_code, 数量, sort_order, created_at, updated_at FROM m_sku_components').all();
  return { masterRows, componentRows };
}
/** 古い表の決まった並べ方のハッシュ (形が違う行がある = content_hash null・error) */
export function legacyDigestOf(sqlite) {
  const legacy = readLegacyRows(sqlite);
  try { return { ...skuMapDigest(fromMiniPcRows(legacy)), error: null }; } catch (e) { return { content_hash: null, master_rows: legacy.masterRows.length, component_rows: legacy.componentRows.length, error: e.message }; }
}
/** 前の写しの記録 (sync_meta)。無い = null / 読めない = { unreadable: true } */
export function readMeta(sqlite) {
  let v;
  try { v = sqlite.prepare('SELECT value FROM sync_meta WHERE key = ?').get(META_KEY)?.value; } catch { return null; }
  if (v == null || v === '') return null;
  try { const j = JSON.parse(v); return j && typeof j === 'object' ? j : { unreadable: true }; } catch { return { unreadable: true }; }
}
/** warehouse.db を読むだけで開いて前の写しの記録を読む (持ち主が読めないときの手がかり。ファイルが無い = null) */
export function readMetaReadonly(dataDir) {
  const file = path.join(dataDir, 'warehouse.db');
  if (!fs.existsSync(file)) return null;
  return import('better-sqlite3').then(({ default: Database }) => {
    const db = new Database(file, { readonly: true, fileMustExist: true });
    try { return readMeta(db); } finally { db.close(); }
  });
}

// ─── Company DB ───

const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
/** 持ち主 (DB の active の listing_components.amazon。記録が無い = load)。壊れていれば投げる (推測で決めない) */
export async function readAmazonOwner(db) {
  const st = await readOwnershipState(db);
  return { owner: st.active.map[AMAZON_MAP_OWNER_KEY] === 'company' ? 'company' : 'load', state: st.state, active_hash: st.active.hash };
}
/**
 * 写す材料を読む (🚨 呼ぶ側が REPEATABLE READ READ ONLY の取引を張る)。
 * canon = 決まった形の行 (active だけ) / by = seller_sku → { registered_by, changed_by } / watermark = 変更の記録の最大の番号 (表が無い = null)
 */
export async function readAmazonMapSource(db) {
  const cdbReadAt = (await rowsOf(db, `select to_char(clock_timestamp() at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as t`))[0].t;
  const owner = await readAmazonOwner(db);
  const hasEvents = (await rowsOf(db, `select to_regclass('events.master_change_events') is not null as ok`))[0].ok === true;
  const top = hasEvents ? (await rowsOf(db, 'select max(event_id)::text as n from events.master_change_events'))[0].n : null;
  if (owner.owner !== 'company') return { cdbReadAt, owner, canon: null, by: new Map(), watermark: top == null ? null : Number(top) };
  const canon = await readCompanyAmazonMapCanon(db);
  const by = new Map((await rowsOf(db, `select seller_sku, registered_by, changed_by from core.amazon_sku_maps where state = 'active'`)).map((r) => [r.seller_sku, r]));
  return { cdbReadAt, owner, canon, by, watermark: top == null ? null : Number(top) };
}

// ─── 差 ───

const mKey = (r) => r.seller_sku;
const cKey = (r) => JSON.stringify([r.seller_sku, r.ne_code]);
/**
 * 古い表 (今) → Company DB の形 (canon) への差。変わらない行は数えるだけ
 * @returns {{ master: { insert, update, delete, same }, components: { insert, update, delete, same } }}  各配列は行 (delete は鍵の行)
 */
export function planDiff(legacy, canon) {
  const curM = new Map(legacy.masterRows.map((r) => [r.seller_sku, r]));
  const curC = new Map(legacy.componentRows.map((r) => [cKey(r), r]));
  const wantM = new Map(canon.master.map((r) => [mKey(r), r]));
  const wantC = new Map(canon.components.map((r) => [cKey(r), r]));
  const out = { master: { insert: [], update: [], delete: [], same: 0 }, components: { insert: [], update: [], delete: [], same: 0 } };
  for (const [k, r] of curC) if (!wantC.has(k)) out.components.delete.push(r);
  for (const [k, r] of curM) if (!wantM.has(k)) out.master.delete.push(r);
  for (const [k, w] of wantM) {
    const c = curM.get(k);
    if (!c) out.master.insert.push(w);
    else if (c.商品名 !== w.name || c.created_at !== w.created_at || c.updated_at !== w.updated_at) out.master.update.push(w);
    else out.master.same++;
  }
  for (const [k, w] of wantC) {
    const c = curC.get(k);
    if (!c) out.components.insert.push(w);
    else if (c.数量 !== w.quantity || c.sort_order !== w.sort_order || c.created_at !== w.created_at || c.updated_at !== w.updated_at) out.components.update.push(w);
    else out.components.same++;
  }
  return out;
}
const countsOf = (p) => ({
  master: { inserted: p.master.insert.length, updated: p.master.update.length, deleted: p.master.delete.length, same: p.master.same },
  components: { inserted: p.components.insert.length, updated: p.components.update.length, deleted: p.components.delete.length, same: p.components.same },
});
const changedRows = (c) => c.master.inserted + c.master.updated + c.master.deleted + c.components.inserted + c.components.updated + c.components.deleted;
const samplesOf = (p) => ({
  added: p.master.insert.slice(0, SAMPLE).map((r) => r.seller_sku),
  changed: [...new Set([...p.master.update, ...p.components.insert, ...p.components.update, ...p.components.delete].map((r) => r.seller_sku))].slice(0, SAMPLE),
  removed: p.master.delete.slice(0, SAMPLE).map((r) => r.seller_sku),
});

/**
 * 安全弁 (断る理由の配列。空 = 写してよい)。
 *   invalid_canon (受け手の決まり・いつも断る) / empty (0 件) / shrunk (今の古い表の 90% 未満) = --allow-shrink で通す /
 *   watermark_backward (変更の記録の番号が前の写しより小さい・前の写しに番号があるのに今は無い) = --accept-restore で通す /
 *   meta_unreadable (前の写しの記録が読めない = 番号を比べられない。--accept-restore で通す) / expect_hash_mismatch (--expect-hash と今回のハッシュが違う)
 */
export function checkSafety({ canon, digest, legacyCount, meta, watermark, allowShrink = false, acceptRestore = false, expectHash = null }) {
  const problems = [];
  const detail = {};
  const issues = validateSkuMap(canon).filter((x) => !/^0 件/.test(x.problem));
  if (issues.length || !digest || !digest.content_hash) { problems.push('invalid_canon'); detail.invalid_canon = issues.length ? issues.slice(0, 5) : [{ where: '*', problem: digest?.error || 'ハッシュを作れない' }]; }
  const n = canon.master.length;
  if (n === 0 && !allowShrink) problems.push('empty');
  if (n > 0 && legacyCount > 0 && n < legacyCount * SHRINK_MIN_RATIO && !allowShrink) { problems.push('shrunk'); detail.shrunk = { legacy: legacyCount, company: n }; }
  if (meta && meta.unreadable && !acceptRestore) problems.push('meta_unreadable');
  else if (meta && Number.isSafeInteger(meta.watermark) && !acceptRestore && (!Number.isSafeInteger(watermark) || watermark < meta.watermark)) {
    problems.push('watermark_backward'); detail.watermark = { prev: meta.watermark, now: watermark ?? null };
  }
  if (expectHash && digest && digest.content_hash !== expectHash) { problems.push('expect_hash_mismatch'); detail.expect_hash = { expected: expectHash, now: digest.content_hash }; }
  return { problems, detail };
}

/**
 * SQLite の 1 取引 (IMMEDIATE) で、古い表を canon にまるごと合わせる。commit の前に読み直してハッシュを照らす・鍵がまだ自分のものかを照らす。
 * 違えば投げる (全部巻き戻る = 古い表は前のまま)。
 * @param {object} [p.lock]  acquireLock の戻り値 (鍵の持ち主を照らす。試験は null で照らさない)
 * @param {(sqlite) => void} [p.beforeCommit]  試験だけ (commit の前に落とす)
 * @returns {{ counts, changed, total_changes, tampered: boolean, legacy_before: object, samples }}
 */
export function applyCanon(sqlite, { canon, digest, by = new Map(), meta = null, metaValue, lock = null, beforeCommit = null }) {
  return sqlite.transaction(() => {
    const before = readLegacyRows(sqlite);
    let legacyBefore;
    try { legacyBefore = { ...skuMapDigest(fromMiniPcRows(before)), error: null }; } catch (e) { legacyBefore = { content_hash: null, error: e.message }; }
    const tampered = !!(meta && meta.content_hash && legacyBefore.content_hash !== meta.content_hash);
    const plan = planDiff(before, canon);
    let total = 0;
    const run = (stmt, ...args) => { total += stmt.run(...args).changes; };
    const delC = sqlite.prepare('DELETE FROM m_sku_components WHERE seller_sku = ? AND ne_code = ?');
    const delM = sqlite.prepare('DELETE FROM m_sku_master WHERE seller_sku = ?');
    for (const r of plan.components.delete) run(delC, r.seller_sku, r.ne_code);
    for (const r of plan.master.delete) run(delM, r.seller_sku);
    const insM = sqlite.prepare('INSERT INTO m_sku_master (seller_sku, 商品名, created_at, updated_at, created_by, updated_by) VALUES (?, ?, ?, ?, ?, ?)');
    const updM = sqlite.prepare('UPDATE m_sku_master SET 商品名 = ?, created_at = ?, updated_at = ?, updated_by = ? WHERE seller_sku = ?');
    for (const r of plan.master.insert) { const b = by.get(r.seller_sku) || {}; run(insM, r.seller_sku, r.name, r.created_at, r.updated_at, b.registered_by ?? null, b.changed_by ?? null); }
    for (const r of plan.master.update) { const b = by.get(r.seller_sku) || {}; run(updM, r.name, r.created_at, r.updated_at, b.changed_by ?? null, r.seller_sku); }
    const insC = sqlite.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)');
    const updC = sqlite.prepare('UPDATE m_sku_components SET 数量 = ?, sort_order = ?, created_at = ?, updated_at = ? WHERE seller_sku = ? AND ne_code = ?');
    for (const r of plan.components.insert) run(insC, r.seller_sku, r.ne_code, r.quantity, r.sort_order, r.created_at, r.updated_at);
    for (const r of plan.components.update) run(updC, r.quantity, r.sort_order, r.created_at, r.updated_at, r.seller_sku, r.ne_code);
    // 読み直して照らす (入れた中身 = Company DB の中身)
    const after = legacyDigestOf(sqlite);
    if (after.content_hash !== digest.content_hash) throw fail(`入れた後に読み直した古い表のハッシュ ${after.content_hash || after.error} が Company DB の ${digest.content_hash} と違う (巻き戻した)`, 'REREAD_MISMATCH');
    if (lock) {
      const h = sqlite.prepare('SELECT holder_id FROM job_locks WHERE job_name = ?').get(lock.jobName)?.holder_id ?? null;
      if (h !== lock.holderId) throw fail('実行の鍵が自分のものでない (時間切れで別の写しに取られた?) = 巻き戻した', 'LOCK_LOST');
    }
    sqlite.prepare('INSERT INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at')
      .run(META_KEY, JSON.stringify(metaValue), metaValue.published_at);
    if (beforeCommit) beforeCommit(sqlite);
    const counts = countsOf(plan);
    return { counts, changed: changedRows(counts), total_changes: total, tampered, legacy_before: legacyBefore, samples: samplesOf(plan) };
  }).immediate();
}

// ─── 1 回分 ───

const short = (h) => (h ? String(h).slice(0, 12) : '-');
const describe = (c) => `親 +${c.master.inserted} ~${c.master.updated} -${c.master.deleted} / 構成 +${c.components.inserted} ~${c.components.updated} -${c.components.deleted}`;

/**
 * 1 回分。connect = () => { db, close } (watcher)。getSqlite = 持ち主が company のときだけ呼ぶ (load の朝は SQLite を開かない)。
 * @returns {{ state: 'not_applied'|'applied'|'unchanged'|'refused'|'dry_run'|'lock_busy', code, line, counts?, problems?, detail?, digest? }}
 */
export async function runAmazonMapPublish({ connect, getSqlite, now = () => new Date(), dryRun = false, allowShrink = false, acceptRestore = false, expectHash = null,
  manual = false, lockTtlMs = LOCK_TTL_MS, beforeCommit = null, afterLock = null }) {
  // 持ち主を読むまでの失敗 (接続・持ち主の記録が壊れている) には印 ownerStage を付ける = 入口 (cli) が手がかりで ❌ / ⚠️ を決める
  const ownerStage = (e) => Object.assign(e instanceof Error ? e : new Error(String(e)), { ownerStage: true });
  let conn;
  try { conn = await connect(); } catch (e) { throw ownerStage(e); }
  const { db, close } = conn;
  let lock = null, sqlite = null;
  try {
    // 1 持ち主 (鍵の前に読むのは持ち主だけ。load = ここで終わる = SQLite を開かない)
    let owner;
    try {
      await db.query('begin transaction isolation level repeatable read read only');
      try { owner = await readAmazonOwner(db); } finally { try { await db.query('rollback'); } catch { /* */ } }
    } catch (e) { throw ownerStage(e); }
    if (owner.owner !== 'company') {
      return { state: 'not_applied', code: EXIT.ok, line: `⏭️ ${STEP_NAME}: 持ち主が load (Company DB の active) = 写さない (古い表は今のまま)` };
    }
    sqlite = await getSqlite();
    // 2 鍵 (PG の対応を読む前から SQLite の commit の後まで)。試し (--dry-run) は書かない = 鍵も取らない
    if (!dryRun) {
      lock = acquireLock(sqlite, LOCK_NAME, { ttlMs: lockTtlMs });
      if (!lock) return { state: 'lock_busy', code: EXIT.error, line: `❌ ${STEP_NAME}: 別の写しが動いている (鍵 ${LOCK_NAME}) = 何もしない (show-job-locks.js で確かめる)` };
      if (afterLock) await afterLock();
    }
    await db.query('begin transaction isolation level repeatable read read only');
    let src;
    try { src = await readAmazonMapSource(db); } finally { try { await db.query('rollback'); } catch { /* */ } }
    if (src.owner.owner !== 'company') {   // 鍵を取る間に load に戻った (取り消し) = 写さない
      return { state: 'not_applied', code: EXIT.ok, line: `⏭️ ${STEP_NAME}: 持ち主が load (Company DB の active) = 写さない (古い表は今のまま)` };
    }
    let digest;
    try { digest = { ...skuMapDigest(src.canon), error: null }; } catch (e) { digest = { content_hash: null, master_rows: src.canon.master.length, component_rows: src.canon.components.length, error: e.message }; }
    const meta = readMeta(sqlite);
    const legacyCount = sqlite.prepare('SELECT COUNT(*) AS n FROM m_sku_master').get().n;
    const { problems, detail } = checkSafety({ canon: src.canon, digest, legacyCount, meta, watermark: src.watermark, allowShrink, acceptRestore, expectHash });
    const base = { digest, watermark: src.watermark, legacy_rows: legacyCount, problems, detail };
    if (dryRun) {
      const plan = planDiff(readLegacyRows(sqlite), src.canon);
      const counts = countsOf(plan);
      return { ...base, state: 'dry_run', code: EXIT.ok, counts, samples: samplesOf(plan),
        line: `${problems.length ? '❌' : '✅'} ${STEP_NAME} (試し・書かない): ${problems.length ? `断る (${problems.join('・')})` : '写せる'} / Company DB 対応 ${src.canon.master.length} 件・構成 ${src.canon.components.length} 行・ハッシュ ${digest.content_hash || digest.error} / 差 ${describe(counts)} / 古い表 ${legacyCount} 件` };
    }
    if (problems.length) {
      return { ...base, state: 'refused', code: EXIT.error,
        line: `❌ ${STEP_NAME}: 断った (${problems.join('・')}) = 古い表は前のまま${detail.shrunk ? ` / 古い表 ${detail.shrunk.legacy} 件 → Company DB ${detail.shrunk.company} 件` : ''}${detail.watermark ? ` / 変更の記録の番号 ${detail.watermark.prev} → ${detail.watermark.now}` : ''}${problems.includes('empty') || problems.includes('shrunk') ? ' (意図した削除なら手で --allow-shrink --expect-hash)' : ''}${problems.includes('watermark_backward') || problems.includes('meta_unreadable') ? ' (Company DB を戻した? README の手順で差を見てから --accept-restore --expect-hash)' : ''}` };
    }
    const t = now();
    const metaValue = { format: SKU_MAP_CANON_FORMAT, content_hash: digest.content_hash, master_rows: digest.master_rows, component_rows: digest.component_rows,
      watermark: Number.isSafeInteger(src.watermark) ? src.watermark : null, cdb_read_at: src.cdbReadAt, published_at: t.toISOString(), by: manual ? 'manual' : 'daily',
      ...(allowShrink ? { allow_shrink: true } : {}), ...(acceptRestore ? { accept_restore: true } : {}) };
    const r = applyCanon(sqlite, { canon: src.canon, digest, by: src.by, meta, metaValue, lock, beforeCommit });
    const warn = r.tampered;
    const state = r.changed ? 'applied' : 'unchanged';
    const line = `${warn ? '⚠️' : '✅'} ${STEP_NAME}: ${r.changed ? `写した (${describe(r.counts)})` : '変わった行 0'} / 対応 ${digest.master_rows} 件・構成 ${digest.component_rows} 行・ハッシュ ${short(digest.content_hash)}`
      + (warn ? ` / 前の写しの後に古い表が書き換えられていた (${short(meta.content_hash)} → ${short(r.legacy_before.content_hash || r.legacy_before.error)}) = Company DB の値で上書きした` : '')
      + (allowShrink ? ' / --allow-shrink' : '') + (acceptRestore ? ' / --accept-restore' : '');
    return { ...base, state, code: EXIT.ok, counts: r.counts, changed: r.changed, total_changes: r.total_changes, tampered: r.tampered, samples: r.samples, line };
  } finally {
    if (lock && sqlite) { try { releaseLock(sqlite, lock); } catch { /* 時間切れで消える */ } }
    try { await close(); } catch { /* */ }
  }
}

// ─── 入口 ───

export function parseArgs(argv) {
  const out = { dataDir: null, daily: false, dryRun: false, allowShrink: false, acceptRestore: false, expectHash: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--daily' || a === '7') out.daily = true;   // '7' = 引数が無いときに daily-sync が足す
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--allow-shrink') out.allowShrink = true;
    else if (a === '--accept-restore') out.acceptRestore = true;
    else if (a === '--expect-hash') out.expectHash = String(argv[++i] ?? '');
    else throw fail(`知らない引数: ${a}`, 'ARGS');
  }
  if (out.expectHash != null && !SKU_MAP_HASH_RE.test(out.expectHash)) throw fail('--expect-hash は 64 桁の 16 進 (--dry-run が出す Company DB のハッシュ)', 'ARGS');
  if ((out.allowShrink || out.acceptRestore) && !out.expectHash) throw fail('--allow-shrink / --accept-restore には --expect-hash <今回の Company DB のハッシュ> が要る (--dry-run で見てから)', 'ARGS');
  if ((out.allowShrink || out.acceptRestore) && out.daily) throw fail('--allow-shrink / --accept-restore は手で流すときだけ (--daily では使えない)', 'ARGS');
  if (out.dryRun && (out.allowShrink || out.acceptRestore)) throw fail('--dry-run と --allow-shrink / --accept-restore は一緒に使わない', 'ARGS');
  return out;
}

const truthy = (v) => /^(1|true|yes|on)$/i.test(String(v ?? '').trim());

/**
 * 入口 (daily-sync・自動再試行・手)。戻り値 = { code, last }
 * 持ち主が読めない (未設定・届かない・記録が壊れている) とき: config (configured) か前の写しの記録が「company」を示す = ❌ exit 1 (retry に載る)。
 *   どちらも示さない (今の本番 = 写しを一度も使っていない) = ⚠️ exit 0 (今までの動きを変えない = f_sales の retry を止めない)
 * @param {object} [deps]  試験で差し替える (env・接続・warehouse.db・持ち主の設定・ログ)
 */
export async function cli(argv, { env = process.env, connectFor = null, openSqlite = null, readHintMeta = null, ownership = MASTER_OWNERSHIP, log = console.log, now = () => new Date(), run = runAmazonMapPublish, beforeCommit = null, afterLock = null } = {}) {
  let code = EXIT.error, last = '';
  try {
    const a = parseArgs(argv);
    const dataDir = (a.dataDir || env.DATA_DIR || '').trim();
    if (!dataDir) throw fail('DATA_DIR が無い (--data-dir でも可)', 'ARGS');
    if (truthy(env[PAUSE_ENV]) && !a.dryRun) {
      last = `⚠️ ${STEP_NAME}: 止めている (${PAUSE_ENV}=1) = 写さない・古い表は前の形のまま (差は --dry-run で見る・再開は env を外す)`;
      code = EXIT.ok;
    } else {
      const hint = async () => {
        if (ownership && ownership[AMAZON_MAP_OWNER_KEY] === 'company') return 'config';
        let m = null;
        try { m = await (readHintMeta ? readHintMeta(dataDir) : readMetaReadonly(dataDir)); } catch { m = { unreadable: true }; }
        return m ? 'meta' : null;
      };
      const unreadable = async (why) => {
        const h = await hint();
        if (h) return { code: EXIT.error, last: `❌ ${STEP_NAME}: 持ち主を読めない (${why}) = 写さない・古い表は前のまま (持ち主は company のはず = ${h === 'config' ? 'config が company' : '前に写した記録がある'})` };
        return { code: EXIT.ok, last: `⚠️ ${STEP_NAME}: 持ち主を読めない (${why}) = 写さない (config も前の写しの記録も load = 今の動きのまま)` };
      };
      const url = (env.COMPANY_DB_WATCH_URL || '').trim();
      if (!url) ({ code, last } = await unreadable('未設定 COMPANY_DB_WATCH_URL'));
      else {
        const connect = connectFor ? connectFor(url) : async () => (await import('../master-compare/run.mjs')).connectWatcher(url);
        const getSqlite = async () => (openSqlite ? openSqlite(dataDir) : (async () => {
          if (a.dataDir) process.env.DATA_DIR = dataDir;   // db.js は import の時に DATA_DIR を読む (この後で初めて import する)
          const { initDB } = await import('../../warehouse/db.js');
          return initDB();
        })());
        let r = null, ownerErr = null;
        try {
          r = await run({ connect, getSqlite, now, dryRun: a.dryRun, allowShrink: a.allowShrink, acceptRestore: a.acceptRestore, expectHash: a.expectHash, manual: !a.daily, beforeCommit, afterLock });
        } catch (e) {
          // 持ち主を読む前に落ちた (接続・持ち主の記録が壊れている) = 手がかりで決める / それより後 = ❌
          if (e && e.ownerStage) ownerErr = e;
          else throw e;
        }
        if (ownerErr) ({ code, last } = await unreadable(String(ownerErr.message || ownerErr).replace(/\s+/g, ' ').slice(0, 160)));
        else {
          code = r.code; last = r.line;
          if (a.dryRun && r.state === 'dry_run' && r.samples) log(`  足す: ${r.samples.added.join(', ') || '-'} / 直す: ${r.samples.changed.join(', ') || '-'} / 消す: ${r.samples.removed.join(', ') || '-'}${r.detail && Object.keys(r.detail).length ? ` / ${JSON.stringify(r.detail).slice(0, 400)}` : ''}`);
        }
      }
    }
  } catch (e) {
    last = `❌ ${STEP_NAME}: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = EXIT.error;
  }
  last = String(last).replace(/\s+/g, ' ');
  log(last);
  return { code, last };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  // top-level await にしない (fetch.mjs と同じ。動的に読む部品が読み込み返す輪で exit 13 にしない)
  cli(process.argv.slice(2)).then(({ code }) => {
    // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
    process.exitCode = code;
    setTimeout(() => process.exit(code), 10000).unref();
  }, (e) => {
    console.error(`❌ ${STEP_NAME}: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    process.exitCode = 1;
    setTimeout(() => process.exit(1), 10000).unref();
  });
}
