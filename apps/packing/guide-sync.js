/**
 * guide-sync.js — 梱包指示の写し (miniPC ← Render・中原さん 2026-10-23・正 = Render の Company DB の 0101)
 *
 * 梱包指示 = 商品ごとの入れ方の写真 (3 枚まで・順番と一言) と注意点。商品の画面 (master-edit) と梱包の画面のカメラで入れる。
 * ここは Render の配りの口 (apps/packing-guide/api-router.mjs) から取りに行き、読むだけの写しを持つ:
 *   - 一覧 (manifest) を数分ごとに取る → 足りない写真だけ取る (sha256 で確かめる・DATA_DIR/packing-guides/<sha256>.jpg) →
 *     写真が全部そろったら SQLite の pk_pack_guides を丸ごと入れ替える (半端な版は出さない)
 *   - ⭐ fail-soft: 取れなくても前の写しで出し続ける (梱包を止めない)。取れた時刻と誤りを画面に出す (古いと分かるように)
 *   - 写真のファイルは消さない (中身は sha256 の名前 = 変わらない・数は少ない)
 * env: PACKING_GUIDE_KEY (Render の配りの口と同じ鍵・無い = 取りに行かない = 梱包指示は出ない)・
 *      PACKING_GUIDE_URL (既定 https://bfaith-portal.onrender.com/apps/packing-guide-api)・PACKING_GUIDE_POLL_SEC (既定 120)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { getDB, DATA_DIR, utcNow } from './db.js';
import { normSku } from '../../lib/sku-norm.js';

export const GUIDE_PHOTO_DIR = path.join(DATA_DIR, 'packing-guides');
const FETCH_TIMEOUT_MS = 20_000;
const MAX_GUIDES = 20_000;
/** これより古い写しは「古いかも」と画面に出す (分) */
export const GUIDE_STALE_MIN = 30;

const baseUrl = () => String(process.env.PACKING_GUIDE_URL || 'https://bfaith-portal.onrender.com/apps/packing-guide-api').replace(/\/+$/, '');
export const guideSyncEnabled = () => !!process.env.PACKING_GUIDE_KEY;

let fetchImpl = (...a) => fetch(...a);
/** 試験だけ: fetch を差し替える */
export function __setFetchForTest(fn) { fetchImpl = fn || ((...a) => fetch(...a)); }

const meta = {
  get: (k) => getDB().prepare('SELECT value FROM pk_pack_meta WHERE key = ?').get(k)?.value ?? null,
  set: (k, v) => getDB().prepare(`INSERT INTO pk_pack_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(k, String(v ?? '')),
};
const META = { okAt: 'guide_last_ok_at', error: 'guide_last_error', errorAt: 'guide_last_error_at', count: 'guide_count' };

/** 取得先が安全か (https・鍵を見知らぬ所へ送らない)。問題があれば理由 */
export function guideUrlProblem(url = baseUrl()) {
  let u;
  try { u = new URL(url); } catch { return 'PACKING_GUIDE_URL の形が違います'; }
  if (u.protocol !== 'https:' && !(u.protocol === 'http:' && ['127.0.0.1', 'localhost'].includes(u.hostname))) return 'PACKING_GUIDE_URL が https ではありません';
  return null;
}

async function get(pathPart, { asBuffer = false } = {}) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
  try {
    const r = await fetchImpl(baseUrl() + pathPart, { headers: { 'x-packing-guide-key': process.env.PACKING_GUIDE_KEY }, signal: ctl.signal });
    if (!r.ok) {
      let why = '';
      try { why = (await r.json())?.error || ''; } catch { /* */ }
      throw new Error(`HTTP ${r.status}${why ? ` (${why})` : ''}`);
    }
    return asBuffer ? Buffer.from(await r.arrayBuffer()) : await r.json();
  } finally { clearTimeout(t); }
}

const SHA_RE = /^[0-9a-f]{64}$/;
export const guidePhotoFile = (sha) => (SHA_RE.test(String(sha)) ? path.join(GUIDE_PHOTO_DIR, `${sha}.jpg`) : null);

/** 一覧の形を確かめて、写しの行にする (形が違えば投げる = 前の写しのまま) */
function rowsOf(body) {
  if (!body || body.ok !== true || !Array.isArray(body.guides)) throw new Error('一覧の形が違います');
  if (body.guides.length > MAX_GUIDES) throw new Error(`一覧が多すぎます (${body.guides.length})`);
  const seen = new Set();
  return body.guides.map((g) => {
    const norm = normSku(g?.code);
    if (!norm || seen.has(norm)) throw new Error(`商品コードの形が違うか重なっています (${g?.code})`);
    seen.add(norm);
    if (!Array.isArray(g.photos) || g.photos.length > 3) throw new Error(`写真の並びの形が違います (${g.code})`);
    const photos = g.photos.map((p) => {
      if (!SHA_RE.test(String(p?.sha256)) || !/^[1-9]\d{0,17}$/.test(String(p?.photo_id))) throw new Error(`写真の形が違います (${g.code})`);
      return { photo_id: String(p.photo_id), sha256: p.sha256, position: Number(p.position) || 0, caption: String(p.caption || '').slice(0, 100),
        width: Number(p.width) || null, height: Number(p.height) || null };
    });
    return { sku_norm: norm, code: String(g.code), name: g.name == null ? null : String(g.name), note: String(g.note || '').slice(0, 2000),
      photos, event_id: g.event_id == null ? null : String(g.event_id), updated_at: g.updated_at == null ? null : String(g.updated_at), updated_by: g.updated_by == null ? null : String(g.updated_by) };
  });
}

/** 足りない写真を取る (sha256 を確かめて、別名に書いてから名前を変える)。取れなければ投げる */
async function ensurePhoto(p) {
  const file = guidePhotoFile(p.sha256);
  if (fs.existsSync(file)) return false;
  const buf = await get(`/photos/${p.photo_id}`, { asBuffer: true });
  const sha = crypto.createHash('sha256').update(buf).digest('hex');
  if (sha !== p.sha256) throw new Error(`写真 ${p.photo_id} の中身が一覧と違います`);
  fs.mkdirSync(GUIDE_PHOTO_DIR, { recursive: true });
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, buf);
  fs.renameSync(tmp, file);
  return true;
}

let inflight = null;
/**
 * 1 回取る。戻り値 { ok, count?, fetched?, error?, skipped? }。投げない (失敗は記録して前の写しのまま)
 */
export function pullGuides() {
  if (inflight) return inflight;
  inflight = (async () => {
    if (!guideSyncEnabled()) return { ok: false, skipped: 'disabled' };
    const urlProblem = guideUrlProblem();
    if (urlProblem) { meta.set(META.error, urlProblem); meta.set(META.errorAt, utcNow()); return { ok: false, error: urlProblem }; }
    try {
      const rows = rowsOf(await get('/manifest'));
      let fetched = 0;
      for (const g of rows) for (const p of g.photos) if (await ensurePhoto(p)) fetched++;
      const db = getDB();
      const ins = db.prepare(`INSERT INTO pk_pack_guides (sku_norm, code, name, note, photos_json, event_id, updated_at, updated_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
      db.transaction(() => {
        db.prepare('DELETE FROM pk_pack_guides').run();
        for (const g of rows) {
          ins.run(g.sku_norm, g.code, g.name, g.note, JSON.stringify(g.photos.map(({ photo_id, ...x }) => x)), g.event_id, g.updated_at, g.updated_by);
        }
      })();
      meta.set(META.okAt, utcNow());
      meta.set(META.count, rows.length);
      meta.set(META.error, '');
      return { ok: true, count: rows.length, fetched };
    } catch (e) {
      const msg = String(e?.name === 'AbortError' ? '時間切れ' : (e?.message || e)).slice(0, 300);
      meta.set(META.error, msg);
      meta.set(META.errorAt, utcNow());
      return { ok: false, error: msg };
    }
  })().finally(() => { inflight = null; });
  return inflight;
}

/** 取得の様子 (画面・readyz)。enabled = 鍵がある / ageMin = 最後に取れてから何分 / stale = 古いかも */
export function guideSyncStatus(now = Date.now()) {
  if (!guideSyncEnabled()) return { enabled: false };
  const okAt = meta.get(META.okAt);
  const ageMin = okAt ? Math.floor((now - Date.parse(okAt)) / 60000) : null;
  return { enabled: true, okAt, ageMin, stale: ageMin == null || ageMin > GUIDE_STALE_MIN, error: meta.get(META.error) || null, count: Number(meta.get(META.count) || 0) };
}

/**
 * 商品コードの一覧 → 梱包指示 (写しにあるものだけ)。Map<sku_norm, { code, name, note, photos: [{ sha256, position, caption, width, height, url }], updated_at, updated_by }>
 * 写しが読めない = 空 (梱包は止めない)
 */
export function guidesFor(codes, { photoBase = '/apps/packing/guide-photos' } = {}) {
  const out = new Map();
  const norms = [...new Set((codes || []).map(normSku).filter(Boolean))];
  if (!norms.length) return out;
  try {
    const db = getDB();
    const stmt = db.prepare('SELECT * FROM pk_pack_guides WHERE sku_norm = ?');
    for (const n of norms) {
      const r = stmt.get(n);
      if (!r) continue;
      let photos = [];
      try { photos = JSON.parse(r.photos_json || '[]'); } catch { photos = []; }
      out.set(n, { code: r.code, name: r.name, note: r.note || '', updated_at: r.updated_at, updated_by: r.updated_by,
        photos: photos.filter((p) => SHA_RE.test(String(p.sha256))).map((p) => ({ ...p, url: `${photoBase}/${p.sha256}.jpg` })) });
    }
  } catch (e) {
    console.error('[packing] 梱包指示の写しを読めない (梱包は続ける):', e.message);
  }
  return out;
}

let timer = null;
/** 数分ごとに取る (起動の直後にも 1 回)。鍵が無い = 何もしない */
export function startGuidePoller() {
  if (timer || !guideSyncEnabled()) return false;
  const sec = Math.max(30, Number(process.env.PACKING_GUIDE_POLL_SEC) || 120);
  const tick = () => pullGuides().then((r) => { if (!r.ok && !r.skipped) console.warn(`[packing] 梱包指示の取得に失敗 (前の写しで続ける): ${r.error}`); });
  setTimeout(tick, 5_000).unref?.();
  timer = setInterval(tick, sec * 1000);
  timer.unref?.();
  return true;
}
