/**
 * material-lineage.js — Company DB の夜間ロードの「材料」の世代を残す (Company DB構想 10 §6 / PR ③a-1。Codex ③ 設計レビュー)
 *
 * なぜ: 夜間ロード (Render・02:00) は Render の mirror_products / mirror_set_components (= miniPC の daily-sync が前の朝に送った写し) を読む。
 *   毎朝の照合 (③a-2) で「Company DB と今朝の NE の差」が「写しの遅れ (正常)」なのか「ロードの誤り」なのかを見分けるには、
 *   **Company DB が実際に読んだ写しの中身** を後から特定できないといけない。
 * なにを:
 *   - 送る写し (products / set_components) を **Render の mirror が持つ形** (MATERIAL_COLUMNS + 空の埋め方) にそろえ、
 *     行の並びに依らない中身のハッシュ (sha256) と行数を出し、世代 ID を付ける
 *   - そろえた中身を miniPC の DATA_DIR/cdb-material/<世代 ID>.json.gz に控える (新しい KEEP 個だけ残す)
 *   - Render の受け手は mirror を入れ替えたのと同じ取引で、**入れた中身から同じ規則でハッシュを出し直し**、合えば mirror_material_generations に残す
 *   - 夜間ロードも**自分が読んだ中身から同じ規則でハッシュを出し**、世代と合うときだけ「その世代を読んだ」と Company DB (ops.load_materials) に記録
 *     (Render 側で mirror_products を書き換えるアプリがある = 会計アプリの税率・売上分類 / fba-profitability の原価の例外。受信時の照合だけでは足りない)
 * 🚨 ここで失敗しても Render への送信は止めない (業務の写しの方が大事)。控えが無い世代は照合で「判定できない」になるだけ
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

export const MATERIAL_DIR_NAME = 'cdb-material';
export const MATERIAL_KEEP = 14;
export const MATERIAL_FORMAT = 'cdb-material-v2';
/** 書きかけの控え (.tmp) をこれより古ければ消す (別の回が書いている最中のものは消さない) */
export const MATERIAL_STALE_TMP_MS = 60 * 60 * 1000;
/** 世代 ID: mat_<UTC の年月日T時分秒ミリ秒>Z_<products と set_components のハッシュ先頭 8 桁>_<乱数 6 桁> */
export const MATERIAL_ID_RE = /^mat_\d{8}T\d{9}Z_[0-9a-f]{8}_[0-9a-f]{6}$/;
export const MATERIAL_HASH_RE = /^[0-9a-f]{64}$/;

/**
 * 世代に付く時刻などの短い文字列: 64 文字以下で制御文字 (NUL など) を含まない文字列ならそのまま、空なら null、それ以外は undefined (= 不正)。
 * 🚨 NUL を含む文字列は SQLite には入るが PostgreSQL が拒む = 夜間ロードごと巻き戻る (Codex R2 M-1)。受け手と夜間ロードの両方で使う
 */
export function cleanMaterialText(v) {
  if (v == null) return null;
  return typeof v === 'string' && v.length <= 64 && !/[\u0000-\u001f\u007f]/.test(v) ? v : undefined;
}

/**
 * Render の mirror が持つ列 (updated_at = 受信時刻は除く)。
 * 🚨 apps/warehouse-mirror/router.js の /api/sync の INSERT と同じにする (試験 test-material-lineage [3] が mirror の表の列と突き合わせる)
 */
export const MATERIAL_COLUMNS = Object.freeze({
  products: Object.freeze([
    'product_id', '商品コード', '商品名', '商品区分', '取扱区分',
    '標準売価', '原価', '原価ソース', '原価状態',
    '送料', '送料コード', '配送方法', '消費税率', '税区分',
    '在庫数', '引当数', '仕入先コード', 'セット構成品数', '売上分類', '代表商品コード',
    'seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date',
  ]),
  set_components: Object.freeze(['セット商品コード', '構成商品コード', '数量', '構成商品名', '構成商品原価']),
});
/** 受け手が空を埋める値 (/api/sync の `?? 0`)。ここに無い列の空は null */
const MATERIAL_DEFAULTS = { products: { seasonality_flag: 0, new_product_flag: 0 }, set_components: {} };

/**
 * 行を Render の mirror が持つ形にそろえる: MATERIAL_COLUMNS だけ・undefined は null・有限でない数 (NaN / Infinity) は null
 * (JSON で送ると null になる = Render に入るのは null)・空は MATERIAL_DEFAULTS
 */
export function projectMaterialRows(entity, rows) {
  const cols = MATERIAL_COLUMNS[entity];
  if (!cols) throw new Error(`材料の種類が分からない: ${entity}`);
  const def = MATERIAL_DEFAULTS[entity];
  return (rows || []).map((r) => {
    const o = {};
    for (const c of cols) {
      let v = r?.[c];
      if (typeof v === 'number' && !Number.isFinite(v)) v = null;
      if (v === undefined || v === null) v = def[c] ?? null;
      o[c] = v;
    }
    return o;
  });
}

/** キーを並べた JSON (オブジェクトの鍵の順番に依らない)。有限でない数は null と区別する */
function stableStringify(v) {
  if (typeof v === 'number' && !Number.isFinite(v)) return JSON.stringify({ $nonfinite: String(v) });
  if (v === null || typeof v !== 'object') return JSON.stringify(v === undefined ? null : v);
  if (Array.isArray(v)) return `[${v.map(stableStringify).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stableStringify(v[k])}`).join(',')}}`;
}

/** 行の並びに依らない中身のハッシュ。各行を stableStringify → 並べ替え → 改行で繋いで sha256 */
export function contentHash(rows) {
  const lines = (rows || []).map(stableStringify).sort();
  return crypto.createHash('sha256').update(lines.join('\n'), 'utf8').digest('hex');
}

/** Render の mirror が持つ形にそろえてからのハッシュと行数 (送り手・受け手・夜間ロードが同じこれを使う) */
export function materialDigest(entity, rows) {
  const projected = projectMaterialRows(entity, rows);
  return { row_count: projected.length, content_hash: contentHash(projected) };
}

export function buildMaterialGeneration({ products, set_components, neProductsCompleteAt = null, neSetProductsCompleteAt = null, now = new Date() }) {
  const p = materialDigest('products', products);
  const s = materialDigest('set_components', set_components);
  const stamp = now.toISOString().replace(/[-:.]/g, '');   // 20260925T001011123Z
  const both = crypto.createHash('sha256').update(`${p.content_hash}:${s.content_hash}`).digest('hex');
  return {
    format: MATERIAL_FORMAT,
    generation_id: `mat_${stamp}_${both.slice(0, 8)}_${crypto.randomBytes(3).toString('hex')}`,
    created_at: now.toISOString(),
    products: { ...p, source_complete_at: neProductsCompleteAt || null },
    set_components: { ...s, source_complete_at: neSetProductsCompleteAt || null },
  };
}

function codedError(message, code) { return Object.assign(new Error(message), { code }); }

/** 古い世代 (新しい keep 個より前) と、書きかけのまま残った古い .tmp を消す。失敗しても投げない (次の回に) */
export function pruneMaterialDir(dir, { keep = MATERIAL_KEEP, nowMs = Date.now(), staleTmpMs = MATERIAL_STALE_TMP_MS } = {}) {
  let names;
  try { names = fs.readdirSync(dir); } catch { return; }
  const gens = names.filter((f) => /^mat_.*\.json\.gz$/.test(f)).sort();   // 名前 = 時刻順
  for (const f of gens.slice(0, Math.max(0, gens.length - keep))) {
    try { fs.unlinkSync(path.join(dir, f)); } catch { /* 消せなくても次の回に */ }
  }
  for (const f of names.filter((x) => /^mat_.*\.tmp$/.test(x))) {
    try {
      const file = path.join(dir, f);
      if (nowMs - fs.statSync(file).mtimeMs > staleTmpMs) fs.unlinkSync(file);
    } catch { /* 消せなくても次の回に */ }
  }
}

/**
 * 控えを DATA_DIR/cdb-material/<世代 ID>.json.gz に書く (中身は Render の mirror が持つ形)。書いたファイルの場所を返す。
 * - 中身が世代のハッシュと合わなければ書かない (MATERIAL_HASH_MISMATCH)
 * - 同じ名前の控えがあれば上書きしない (MATERIAL_SNAPSHOT_EXISTS)
 * - 書きかけは回ごとに別の名前の .tmp → rename。失敗したら .tmp を消す。成功しても失敗しても最後に古い世代と古い .tmp を片付ける
 */
export function saveMaterialSnapshot({ dataDir, generation, products, set_components, keep = MATERIAL_KEEP, nowMs = Date.now() }) {
  if (!dataDir) throw codedError('DATA_DIR が無い (控えを書けない)', 'NO_DATA_DIR');
  const id = generation?.generation_id;
  if (typeof id !== 'string' || !MATERIAL_ID_RE.test(id)) throw codedError(`世代 ID の形がおかしい: ${id}`, 'BAD_GENERATION_ID');
  const dir = path.join(dataDir, MATERIAL_DIR_NAME);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${id}.json.gz`);
  const tmp = path.join(dir, `${id}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`);
  try {
    const pp = projectMaterialRows('products', products);
    const ss = projectMaterialRows('set_components', set_components);
    if (contentHash(pp) !== generation.products?.content_hash || contentHash(ss) !== generation.set_components?.content_hash) {
      throw codedError(`控えにする中身が世代のハッシュと合わない: ${id}`, 'MATERIAL_HASH_MISMATCH');
    }
    if (fs.existsSync(file)) throw codedError(`同じ世代の控えが既にある (上書きしない): ${id}`, 'MATERIAL_SNAPSHOT_EXISTS');
    fs.writeFileSync(tmp, zlib.gzipSync(Buffer.from(JSON.stringify({ generation, products: pp, set_components: ss }), 'utf8')), { flag: 'wx' });
    if (fs.existsSync(file)) throw codedError(`同じ世代の控えが既にある (上書きしない): ${id}`, 'MATERIAL_SNAPSHOT_EXISTS');
    fs.renameSync(tmp, file);   // 途中で落ちても壊れた控えを本物の名前で残さない
    return file;
  } finally {
    try { fs.rmSync(tmp, { force: true }); } catch { /* 片付けで次の回に */ }
    pruneMaterialDir(dir, { keep, nowMs });
  }
}

/**
 * 控えを読む (照合 ③a-2 用)。無ければ null。次のどれかなら投げる (壊れた・取り違えた控えで判定しない = MATERIAL_HASH_MISMATCH):
 * 中身が控えの中の世代のハッシュと合わない / 控えの中の世代 ID が違う / expected (ops.load_materials の content_hash) と合わない
 */
export function readMaterialSnapshot({ dataDir, generationId, expected = null }) {
  if (typeof generationId !== 'string' || !MATERIAL_ID_RE.test(generationId)) throw codedError(`世代 ID の形がおかしい: ${generationId}`, 'BAD_GENERATION_ID');
  const file = path.join(dataDir, MATERIAL_DIR_NAME, `${generationId}.json.gz`);
  if (!fs.existsSync(file)) return null;
  const obj = JSON.parse(zlib.gunzipSync(fs.readFileSync(file)).toString('utf8'));
  const bad = (why) => codedError(`控えが使えない (${why}): ${generationId}`, 'MATERIAL_HASH_MISMATCH');
  if (obj.generation?.generation_id !== generationId) throw bad('中の世代 ID が違う');
  for (const entity of ['products', 'set_components']) {
    const h = contentHash(obj[entity]);
    if (h !== obj.generation?.[entity]?.content_hash) throw bad(`${entity} の中身がハッシュと合わない`);
    if (expected?.[entity] != null && h !== expected[entity]) throw bad(`${entity} が期待のハッシュと合わない`);
  }
  return obj;
}
