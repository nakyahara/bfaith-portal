/**
 * master-material.js — m_products の「作り直しの記録」と、送る材料の由来 (Company DB構想 10 §6.1.1 / PR ③a-2 の A)
 *
 * なぜ: 毎朝の照合 (③a-2) は「Company DB と今朝の NE の差」を、写しの遅れ・作り方の違い・本当の差に分ける。
 *   そのためには、送った材料 (m_products / m_set_components) が**どの NE の取得から・どの規則で**作られたかが要る。
 *   作り直しが失敗した朝や、作り直しの後に画面 (/register の送料・例外原価・税率・売上分類) で m_products が直された朝に、
 *   送信時点の NE の印を付けると由来を取り違える (Codex ③a-2 R0 High-1 / R1 H1)。
 * なにを:
 *   - readMasterMaterial: 送る形 (m_products + raw_ne_products の代表商品コード) の読み方。作り直しの記録と送り手が同じものを使う
 *   - recordBuild: rebuild-m-products.js が入れ替えと**同じ取引**で m_products_builds に 1 行 (読んだ NE の印・送る形のハッシュ・SKU ごとの採用理由)
 *   - readMaterialWithLineage: 送り手が products・set_components・最新の作り直しの記録を 1 つの読み取り取引で読み、ハッシュが同じときだけ由来を付ける
 */
import crypto from 'node:crypto';
import { materialDigest, contentHash } from './material-lineage.js';

/** 作り直しの規則の版 (rebuild-m-products.js の値の決め方を変えたら上げる。ロードの規則の指紋とは別) */
export const MASTER_BUILD_RULE_VERSION = 'mpb-v1';
export const BUILD_KEEP_DAYS = 60;
export const BUILD_ID_RE = /^mpb_\d{8}T\d{9}Z_[0-9a-f]{6}$/;
const NE_MARK_KEYS = { products: 'ne_api_products_complete_at', set_components: 'ne_api_setproducts_complete_at' };

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

/** NE の「最後まで取れた印」(無ければ null) */
export function readNeMarks(db) {
  const get = (k) => db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value || null;
  return { products: get(NE_MARK_KEYS.products), set_components: get(NE_MARK_KEYS.set_components) };
}

/** 作業用の表 (staging) の中身のハッシュ。入れ替えの取引の中で「自分が作ったものか」を確かめる (同時に走った作り直しが混ざっていないか) */
export function stagingHash(db) {
  return contentHash([
    ...db.prepare('SELECT * FROM m_products_staging').all().map((r) => ({ t: 'p', ...r })),
    ...db.prepare('SELECT * FROM m_set_components_staging').all().map((r) => ({ t: 's', ...r })),
  ]);
}

/**
 * SKU・列ごとの採用理由 (照合 ② の原因の証拠。Codex R1 H4)。入れ替えた後の m_products と raw から、入れ替えと同じ取引の中で作る。
 * 理由は 1 つの SKU に複数立ってよい。NE の印が分からない回は「今回の取得に無い古い行」を判定しない (unknown で数える)
 */
export function buildReasons(db, { neProductsMark }) {
  const reasons = [];
  // 例外原価 (単品は NE の原価が 0 / 空のとき・セットは例外が先)
  for (const r of db.prepare("SELECT 商品コード AS code, 商品区分 AS kind, 原価 AS cost FROM m_products WHERE 原価ソース = '例外'").all()) {
    reasons.push({ code: r.code, kind: r.kind, col: 'cost', reason: 'exception_cost', value: r.cost });
  }
  // 税率の補い (単品: NE の税率が空か 0 → product_tax_rate)
  for (const r of db.prepare(`
    SELECT p.商品コード AS code, p.消費税率 AS rate, n.消費税率 AS ne_rate
    FROM m_products p JOIN raw_ne_products n ON p.商品コード = n.商品コード COLLATE NOCASE
    WHERE p.商品区分 = '単品' AND p.消費税率 IS NOT NULL AND (n.消費税率 IS NULL OR n.消費税率 = 0)
  `).all()) {
    reasons.push({ code: r.code, kind: '単品', col: 'tax_rate', reason: 'tax_fallback', value: r.rate, source: 'product_tax_rate', ne_value: r.ne_rate });
  }
  // セット名が空欄 (Company DB は名前 = コード)
  for (const r of db.prepare("SELECT 商品コード AS code FROM m_products WHERE 商品区分 = 'セット' AND (商品名 IS NULL OR trim(商品名) = '')").all()) {
    reasons.push({ code: r.code, kind: 'セット', col: 'name', reason: 'set_name_blank' });
  }
  // 今回の NE の取得に無い古い行 (作り直しは raw の全行を読む = NE から消えた商品も毎晩また入る)
  let notInLatestUnknown = false;
  if (neProductsMark) {
    for (const r of db.prepare(`
      SELECT p.商品コード AS code, n.synced_at AS synced_at
      FROM m_products p JOIN raw_ne_products n ON p.商品コード = n.商品コード COLLATE NOCASE
      WHERE p.商品区分 = '単品' AND n.synced_at <> ?
    `).all(neProductsMark)) {
      reasons.push({ code: r.code, kind: '単品', col: '*', reason: 'not_in_latest_fetch', raw_synced_at: r.synced_at });
    }
  } else notInLatestUnknown = true;
  const counts = {};
  for (const x of reasons) counts[x.reason] = (counts[x.reason] || 0) + 1;
  if (notInLatestUnknown) counts.not_in_latest_fetch = null;   // 判定できない (NE の印が無い)
  return { reasons, counts };
}

/**
 * 作り直しの記録を 1 行書く。**rebuild-m-products.js の入れ替えと同じ取引の中で呼ぶ** (失敗すれば入れ替えも巻き戻る)。
 * @param {object} p
 * @param {{products: string|null, set_components: string|null}} p.startMarks 作り始めに読んだ NE の印
 * @param {string} p.startedAt
 * @param {string} p.expectedStagingHash 入れ替えの前に、この作り直しが作った staging のハッシュ
 */
export function recordBuild(db, { buildId = makeBuildId(), startMarks, startedAt, dailySyncRunId = process.env.DAILY_SYNC_RUN_ID || null, now = new Date() }) {
  // 読んだ NE の印: 作り始めと今で違えば、作り直しの途中で NE の商品が書き換わった (取込は必ず印を消す・書く) → 信用しない
  const endMarks = readNeMarks(db);
  const mark = (k) => {
    if (!startMarks[k]) return { value: null, note: 'absent' };
    if (startMarks[k] !== endMarks[k]) return { value: null, note: 'changed_during_build' };
    return { value: startMarks[k], note: null };
  };
  const mp = mark('products'), ms = mark('set_components');
  const { products, set_components } = readMasterMaterial(db);
  const pd = materialDigest('products', products), sd = materialDigest('set_components', set_components);
  const { reasons, counts } = buildReasons(db, { neProductsMark: mp.value });
  const publishedAt = now.toISOString();
  db.prepare(`INSERT INTO m_products_builds (
      build_id, daily_sync_run_id, started_at, published_at,
      ne_products_complete_at, ne_products_mark_note, ne_setproducts_complete_at, ne_setproducts_mark_note,
      products_rows, products_hash, set_components_rows, set_components_hash, rule_version, reason_counts, reasons
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(buildId, dailySyncRunId, startedAt, publishedAt, mp.value, mp.note, ms.value, ms.note,
      pd.row_count, pd.content_hash, sd.row_count, sd.content_hash, MASTER_BUILD_RULE_VERSION, JSON.stringify(counts), JSON.stringify(reasons));
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
 * 違えば build_id = null と、どちらが違ったか (作り直しの後に画面で直された・作り直しの記録が無い など)。由来が不明なら NE の印も付けない
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
