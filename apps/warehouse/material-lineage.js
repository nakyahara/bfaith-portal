/**
 * material-lineage.js — Company DB の夜間ロードの「材料」の世代を残す (Company DB構想 10 §6 / PR ③a-1。Codex ③ 設計レビュー)
 *
 * なぜ: 夜間ロード (Render・02:00) は Render の mirror_products / mirror_set_components (= miniPC の daily-sync が前の朝に送った写し) を読む。
 *   毎朝の照合 (③a-2) で「Company DB と今朝の NE の差」が「写しの遅れ (正常)」なのか「ロードの誤り」なのかを見分けるには、
 *   **Company DB が実際に読んだ写しの中身** を後から特定できないといけない。
 * なにを:
 *   - 送る写し (products / set_components) ごとに、行の並びに依らない中身のハッシュ (sha256) と行数を出し、世代 ID を付ける
 *   - 送った中身を miniPC の DATA_DIR/cdb-material/<世代 ID>.json.gz に控える (新しい KEEP 個だけ残す)
 *   - 世代は Render の受け手が mirror を入れ替えたのと同じ取引で mirror_material_generations に残す → 夜間ロードが読んで Company DB (ops.load_materials) に記録
 * 🚨 ここで失敗しても Render への送信は止めない (業務の写しの方が大事)。控えが無い世代は照合で「判定できない」になるだけ
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

export const MATERIAL_DIR_NAME = 'cdb-material';
export const MATERIAL_KEEP = 14;
export const MATERIAL_FORMAT = 'cdb-material-v1';

/** キーを並べた JSON (オブジェクトの鍵の順番に依らない) */
function stableStringify(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v === undefined ? null : v);
  if (Array.isArray(v)) return `[${v.map(stableStringify).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stableStringify(v[k])}`).join(',')}}`;
}

/** 行の並びに依らない中身のハッシュ。各行を stableStringify → 並べ替え → 改行で繋いで sha256 */
export function contentHash(rows) {
  const lines = (rows || []).map(stableStringify).sort();
  return crypto.createHash('sha256').update(lines.join('\n'), 'utf8').digest('hex');
}

/** 世代 ID: mat_<UTC の年月日時分秒>_<products のハッシュ先頭 8 桁> */
export function buildMaterialGeneration({ products, set_components, neProductsCompleteAt = null, neSetProductsCompleteAt = null, now = new Date() }) {
  const p = { row_count: (products || []).length, content_hash: contentHash(products) };
  const s = { row_count: (set_components || []).length, content_hash: contentHash(set_components) };
  const stamp = now.toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
  return {
    format: MATERIAL_FORMAT,
    generation_id: `mat_${stamp}_${p.content_hash.slice(0, 8)}`,
    created_at: now.toISOString(),
    products: { ...p, source_complete_at: neProductsCompleteAt },
    set_components: { ...s, source_complete_at: neSetProductsCompleteAt },
  };
}

/** 控えを DATA_DIR/cdb-material/<世代 ID>.json.gz に書き、新しい keep 個だけ残す。書いたファイルの場所を返す */
export function saveMaterialSnapshot({ dataDir, generation, products, set_components, keep = MATERIAL_KEEP }) {
  if (!dataDir) throw Object.assign(new Error('DATA_DIR が無い (控えを書けない)'), { code: 'NO_DATA_DIR' });
  const dir = path.join(dataDir, MATERIAL_DIR_NAME);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${generation.generation_id}.json.gz`);
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, zlib.gzipSync(Buffer.from(JSON.stringify({ generation, products, set_components }), 'utf8')));
  fs.renameSync(tmp, file);   // 途中で落ちても壊れた控えを本物の名前で残さない
  const all = fs.readdirSync(dir).filter((f) => /^mat_.*\.json\.gz$/.test(f)).sort();   // 名前 = 時刻順
  for (const f of all.slice(0, Math.max(0, all.length - keep))) {
    try { fs.unlinkSync(path.join(dir, f)); } catch { /* 消せなくても次の回に */ }
  }
  return file;
}

/** 控えを読む (照合 ③a-2 用)。無ければ null。中身のハッシュが世代と合わなければ投げる (壊れた控えで判定しない) */
export function readMaterialSnapshot({ dataDir, generationId }) {
  const file = path.join(dataDir, MATERIAL_DIR_NAME, `${generationId}.json.gz`);
  if (!fs.existsSync(file)) return null;
  const obj = JSON.parse(zlib.gunzipSync(fs.readFileSync(file)).toString('utf8'));
  if (contentHash(obj.products) !== obj.generation?.products?.content_hash || contentHash(obj.set_components) !== obj.generation?.set_components?.content_hash) {
    throw Object.assign(new Error(`控えの中身がハッシュと合わない: ${generationId}`), { code: 'MATERIAL_HASH_MISMATCH' });
  }
  return obj;
}
