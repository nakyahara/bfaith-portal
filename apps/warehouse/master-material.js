/**
 * master-material.js — m_products の「作り直しの記録」と、送る材料の由来 (Company DB構想 10 §6.1.1 / PR ③a-2 の A)
 *
 * なぜ: 毎朝の照合 (③a-2) は「Company DB と今朝の NE の差」を、写しの遅れ・作り方の違い・本当の差に分ける。
 *   そのためには、送った材料 (m_products / m_set_components) が**どの NE の取得から・どの規則で**作られたかが要る。
 *   作り直しが失敗した朝や、作り直しの後に画面 (/register の送料・例外原価・税率・売上分類) で m_products が直された朝に、
 *   送信時点の NE の印を付けると由来を取り違える (Codex ③a-2 R0 High-1 / R1 H1)。
 * なにを:
 *   - readMasterMaterial: 送る形 (m_products + raw_ne_products の代表商品コード) の読み方。作り直しの記録と送り手が同じものを使う
 *   - 作り直しの札 (acquireRebuildLock): 作り始めから入れ替えまで、別の作り直しを入れない (staging は共有の表。Codex PR #1453 R1 High-1)
 *   - recordBuild: rebuild-m-products.js が入れ替えと**同じ取引**で m_products_builds に 1 行
 *     (NE の印を信用してよいか = 通し番号で判定・送る形のハッシュ・作り直しが値を決めたその場で集めた SKU ごとの理由)
 *   - readMaterialWithLineage: 送り手が products・set_components・最新の作り直しの記録を 1 つの読み取り取引で読み、ハッシュが同じときだけ由来を付ける
 */
import crypto from 'node:crypto';
import { materialDigest, contentHash } from './material-lineage.js';

/** 作り直しの規則の版 (rebuild-m-products.js の値の決め方を変えたら上げる。ロードの規則の指紋とは別) */
export const MASTER_BUILD_RULE_VERSION = 'mpb-v1';
export const BUILD_KEEP_DAYS = 60;
export const BUILD_ID_RE = /^mpb_\d{8}T\d{9}Z_[0-9a-f]{6}$/;
export const REBUILD_LOCK_KEY = 'm_products_rebuild_lock';
/** 札の期限 (作り直しは数十秒。落ちたまま残った札をこれより後なら取り直してよい) */
export const REBUILD_LOCK_STALE_MS = 30 * 60 * 1000;
const KINDS = { products: 'products', set_components: 'setproducts' };

export function makeBuildId(now = new Date()) {
  return `mpb_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;
}

/** 送る形の products / set_components (sync-to-render.js が送るのと同じ読み方) */
export function readMasterMaterial(db) {
  const products = db.prepare(`
    SELECT p.*, n.代表商品コード
    FROM m_products p
    LEFT JOIN raw_ne_products n ON p.商品コード = n.商品コード COLLATE NOCASE
  `).all();
  const set_components = db.prepare('SELECT * FROM m_set_components').all();
  return { products, set_components };
}

/**
 * NE の「最後まで取れた印」と通し番号。entity ごとに { at: 印の時刻, completeRev: 印を付けた時の番号, rev: 今の番号 }。
 * 通し番号 = raw_ne_* を書き換えた行の数 (db.js のトリガー。どの書き込み口でも同じ取引で増える)
 */
export function readNeMarks(db) {
  const get = (k) => db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value ?? null;
  const out = {};
  for (const [entity, kind] of Object.entries(KINDS)) {
    const cr = get(`ne_api_${kind}_complete_rev`);
    out[entity] = { at: get(`ne_api_${kind}_complete_at`) || null, completeRev: cr == null || cr === '' ? null : Number(cr), rev: Number(get(`ne_raw_${kind}_rev`) ?? 0) };
  }
  return out;
}

/**
 * 作り直しが読んだ NE の印を信用してよいか。作り始め (start) と入れ替えの取引の中 (end) の両方で
 *   印がある・印の番号 = 今の番号 (印の後に誰も書いていない) ・作り始めから番号も印も変わっていない、のときだけ信用する
 * @returns {{ value: string|null, note: null|'absent'|'written_after_complete'|'changed_during_build' }}
 */
export function judgeNeMark(start, end) {
  if (!start.at || start.completeRev == null) return { value: null, note: 'absent' };
  if (start.rev !== start.completeRev) return { value: null, note: 'written_after_complete' };
  if (end.rev !== start.rev || end.at !== start.at || end.completeRev !== start.completeRev) return { value: null, note: 'changed_during_build' };
  return { value: start.at, note: null };
}

export const STAGING_TABLES = Object.freeze(['m_products_staging', 'm_set_components_staging']);
/**
 * 作業用の表 (staging) を**この接続だけの TEMP 表**にする (同じ名前の TEMP 表は本物の表より先に使われる = プロセスごとに別の作業場)。
 * 定義は本物の表 (sqlite_master) と同じ。札の期限が切れて別の作り直しに取られた後に、止まっていた作り直しが再開しても、
 * 相手の作業場には書けない (Codex PR #1453 R2 High。共有の表を 1 つの長い書き込み取引で守ると、作り直しの 5 秒ほど他の書き込みが待たされる)
 */
export function ensurePrivateStaging(db) {
  for (const name of STAGING_TABLES) {
    if (db.prepare("SELECT 1 FROM sqlite_temp_master WHERE type = 'table' AND name = ?").get(name)) continue;
    const ddl = db.prepare("SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = ?").get(name)?.sql;
    if (!ddl) throw new Error(`作業用の表の定義が無い: ${name}`);
    db.exec(ddl.replace(/^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?/i, 'CREATE TEMP TABLE '));
  }
}
export function stagingIsPrivate(db) {
  return STAGING_TABLES.every((name) => !!db.prepare("SELECT 1 FROM sqlite_temp_master WHERE type = 'table' AND name = ?").get(name));
}

/** 作業用の表 (staging) の中身のハッシュ (札と TEMP の作業場の上での念のための確かめ) */
export function stagingHash(db) {
  return contentHash([
    ...db.prepare('SELECT * FROM m_products_staging').all().map((r) => ({ t: 'p', ...r })),
    ...db.prepare('SELECT * FROM m_set_components_staging').all().map((r) => ({ t: 's', ...r })),
  ]);
}

/**
 * 作り直しの札を取る (sync_meta の 1 行を 1 つの文で取る = 2 つのプロセスが同時に取れない)。取れなければ false。
 * 期限 (REBUILD_LOCK_STALE_MS) を過ぎた札 (落ちたまま残ったもの) は取り直してよい
 */
export function acquireRebuildLock(db, owner, { now = new Date(), staleMs = REBUILD_LOCK_STALE_MS } = {}) {
  const r = db.prepare(`INSERT INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    WHERE sync_meta.value IS NULL OR sync_meta.value = '' OR sync_meta.updated_at < ?`)
    .run(REBUILD_LOCK_KEY, owner, now.toISOString(), new Date(now.getTime() - staleMs).toISOString());
  return r.changes === 1;
}
export function holdsRebuildLock(db, owner) {
  return db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(REBUILD_LOCK_KEY)?.value === owner;
}
export function releaseRebuildLock(db, owner) {
  db.prepare('DELETE FROM sync_meta WHERE key = ? AND value = ?').run(REBUILD_LOCK_KEY, owner);
}

/**
 * 作り直しの記録を 1 行書く。**rebuild-m-products.js の入れ替えと同じ取引の中で呼ぶ** (失敗すれば入れ替えも巻き戻る)。
 * @param {object} p
 * @param {string} p.buildId 作り直しの札の持ち主と同じ ID
 * @param {ReturnType<typeof readNeMarks>} p.startMarks 作り始めに読んだ NE の印と通し番号
 * @param {object[]} p.reasons 作り直しが値を決めたその場で集めた SKU ごとの理由 (後から raw を読み直して推定しない。Codex PR #1453 R1 Medium-3)
 */
export function recordBuild(db, { buildId = makeBuildId(), startMarks, startedAt, reasons = [], dailySyncRunId = process.env.DAILY_SYNC_RUN_ID || null, now = new Date() }) {
  const endMarks = readNeMarks(db);
  const mp = judgeNeMark(startMarks.products, endMarks.products);
  const ms = judgeNeMark(startMarks.set_components, endMarks.set_components);
  // 「今回の NE の取得に無い古い行」は印を信用できるときだけ (作り始めの印で判定した理由を、信用できなければ落とす)
  const kept = mp.value ? reasons : reasons.filter((x) => x.reason !== 'not_in_latest_fetch');
  const counts = {};
  for (const x of kept) counts[x.reason] = (counts[x.reason] || 0) + 1;
  if (!mp.value) counts.not_in_latest_fetch = null;   // 判定できない
  const { products, set_components } = readMasterMaterial(db);
  const pd = materialDigest('products', products), sd = materialDigest('set_components', set_components);
  const publishedAt = now.toISOString();
  db.prepare(`INSERT INTO m_products_builds (
      build_id, daily_sync_run_id, started_at, published_at,
      ne_products_complete_at, ne_products_mark_note, ne_setproducts_complete_at, ne_setproducts_mark_note,
      products_rows, products_hash, set_components_rows, set_components_hash, rule_version, reason_counts, reasons
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(buildId, dailySyncRunId, startedAt, publishedAt, mp.value, mp.note, ms.value, ms.note,
      pd.row_count, pd.content_hash, sd.row_count, sd.content_hash, MASTER_BUILD_RULE_VERSION, JSON.stringify(counts), JSON.stringify(kept));
  const cutoff = new Date(now.getTime() - BUILD_KEEP_DAYS * 86400000).toISOString();
  db.prepare('DELETE FROM m_products_builds WHERE published_at < ?').run(cutoff);
  return { build_id: buildId, products: pd, set_components: sd, marks: { products: mp, set_components: ms }, reason_counts: counts };
}

/** 最新の作り直しの記録 (入れ替えた時刻の新しい順で 1 行) */
export function latestBuild(db) {
  return db.prepare('SELECT * FROM m_products_builds ORDER BY published_at DESC, build_id DESC LIMIT 1').get() || null;
}

/**
 * 送る材料と、その由来 (作り直しの記録) を **1 つの読み取り取引** で読む。
 * 由来を付けるのは、送る形のハッシュが**最新の**作り直しの記録と同じときだけ (過去の記録を探して代用しない。Codex R1 H1)。
 * 違えば build_id = null と、どちらが違ったか (作り直しの後に画面で直された など)。由来が不明なら NE の印も付けない
 * @returns {{ products: object[], set_components: object[], lineage: object }}
 */
export function readMaterialWithLineage(db) {
  let hasBuilds = true;
  try { db.prepare('SELECT 1 FROM m_products_builds LIMIT 1').get(); } catch { hasBuilds = false; }
  const read = () => {
    const m = readMasterMaterial(db);
    return { ...m, build: hasBuilds ? latestBuild(db) : null };
  };
  const { products, set_components, build } = db.inTransaction ? read() : db.transaction(read)();
  let lineage;
  if (!build) lineage = { build_id: null, reason: hasBuilds ? 'no_build_record' : 'no_build_table' };
  else {
    const pd = materialDigest('products', products), sd = materialDigest('set_components', set_components);
    const differs = [];
    if (pd.content_hash !== build.products_hash || pd.row_count !== build.products_rows) differs.push('products');
    if (sd.content_hash !== build.set_components_hash || sd.row_count !== build.set_components_rows) differs.push('set_components');
    lineage = differs.length
      ? { build_id: null, reason: 'changed_after_build', differs, latest_build_id: build.build_id }
      : {
        build_id: build.build_id, rule_version: build.rule_version, published_at: build.published_at, daily_sync_run_id: build.daily_sync_run_id,
        ne_products_complete_at: build.ne_products_complete_at, ne_products_mark_note: build.ne_products_mark_note,
        ne_setproducts_complete_at: build.ne_setproducts_complete_at, ne_setproducts_mark_note: build.ne_setproducts_mark_note,
      };
  }
  return { products, set_components, lineage };
}
