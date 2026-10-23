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

// ─── 🆕 2 段目: 梱包の画面のカメラで撮った写真を Render へ送る (送り待ち = pk_pack_guide_uploads) ───
//   iPad → このサーバー (撮ったらすぐ SQLite とファイルに残す) → Render の配りの口 POST /photos (鍵はこのサーバーだけが持つ)。
//   送れた = sent (写しを取り直す = 画面に出る) / 断られた (3 枚ある・単品でない・Company DB に無い・写真が読めない…) = failed (送り直さない・理由を画面に) /
//   つながらない・Render の誤り = pending のまま間をあけて送り直す (同じ request_id = 2 枚にならない)
export const GUIDE_UPLOAD_DIR = path.join(DATA_DIR, 'packing-guide-uploads');
const UPLOAD_BACKOFF_MIN = [1, 2, 5, 10, 30];
/** 断り (送り直しても同じ) = failed にする答え。それ以外の 4xx (鍵が違う 401 など) と 5xx は送り直す */
const FINAL_REASONS = new Set(['photos_full', 'not_single', 'not_found', 'bad_image', 'too_large', 'invalid_input', 'cancelled', 'request_reused']);

const uploadRow = (opId) => getDB().prepare('SELECT * FROM pk_pack_guide_uploads WHERE op_id = ?').get(opId);
const uploadView = (r) => (r ? { op_id: r.op_id, sku: r.sku, status: r.status, position: r.position, error: r.last_error || null, attempts: r.attempts } : null);

/**
 * 撮った写真を送り待ちに入れる (同じ op_id の押し直し = 前の行)。送るのは sendGuideUploads。
 * jpeg = 画面が縮めた JPEG (このサーバーは作り直さない = Render が作り直す)
 */
export function enqueueGuidePhoto({ opId, sku, caption = '', worker, batchId = null, jpeg }) {
  const prev = uploadRow(opId);
  if (prev) return uploadView(prev);
  const requestId = crypto.randomUUID();
  fs.mkdirSync(GUIDE_UPLOAD_DIR, { recursive: true });
  const file = path.join(GUIDE_UPLOAD_DIR, `${requestId}.jpg`);
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, jpeg);
  fs.renameSync(tmp, file);
  getDB().prepare(`INSERT INTO pk_pack_guide_uploads (op_id, request_id, sku, caption, worker, batch_id, file, status, created_at, next_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`).run(opId, requestId, sku, caption, worker, batchId, path.basename(file), utcNow(), utcNow());
  return uploadView(uploadRow(opId));
}

async function postJson(pathPart, body) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 60_000);
  try {
    const r = await fetchImpl(baseUrl() + pathPart, {
      method: 'POST', headers: { 'x-packing-guide-key': process.env.PACKING_GUIDE_KEY, 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: ctl.signal,
    });
    let j = null;
    try { j = await r.json(); } catch { /* */ }
    return { status: r.status, j: j || {} };
  } finally { clearTimeout(t); }
}

/** 1 行を送る。戻り値 = 送った後の行の様子 */
async function sendOne(row) {
  const db = getDB();
  const later = (msg) => {
    const wait = UPLOAD_BACKOFF_MIN[Math.min(row.attempts, UPLOAD_BACKOFF_MIN.length - 1)];
    db.prepare(`UPDATE pk_pack_guide_uploads SET attempts = attempts + 1, last_error = ?, next_at = ? WHERE op_id = ? AND status = 'pending'`)
      .run(String(msg).slice(0, 300), new Date(Date.now() + wait * 60000).toISOString().slice(0, 19) + 'Z', row.op_id);
  };
  let jpeg;
  try { jpeg = fs.readFileSync(path.join(GUIDE_UPLOAD_DIR, path.basename(row.file))); } catch {
    db.prepare(`UPDATE pk_pack_guide_uploads SET status = 'failed', last_error = ? WHERE op_id = ?`).run('写真のファイルがありません (撮り直してください)', row.op_id);
    return uploadView(uploadRow(row.op_id));
  }
  try {
    const r = await postJson('/photos', { request_id: row.request_id, code: row.sku, image: jpeg.toString('base64'), caption: row.caption, actor: row.worker });
    if (r.status === 200 && r.j.ok) {
      db.prepare(`UPDATE pk_pack_guide_uploads SET status = 'sent', sent_at = ?, position = ?, last_error = NULL, attempts = attempts + 1 WHERE op_id = ?`)
        .run(utcNow(), Number(r.j.position) || null, row.op_id);
    } else if (r.status >= 400 && r.status < 500 && FINAL_REASONS.has(r.j.reason)) {
      db.prepare(`UPDATE pk_pack_guide_uploads SET status = 'failed', last_error = ?, attempts = attempts + 1 WHERE op_id = ?`)
        .run(String(r.j.error || r.j.reason).slice(0, 300), row.op_id);
    } else {
      later(`HTTP ${r.status}${r.j.error ? ` (${r.j.error})` : ''}`);
    }
  } catch (e) {
    later(e?.name === 'AbortError' ? '時間切れ' : (e?.message || e));
  }
  return uploadView(uploadRow(row.op_id));
}

let sending = null;
/**
 * 送り待ちを送る (opId を渡すと、その 1 枚だけを今すぐ = 間をあける時刻を待たない)。1 つでも送れたら写しを取り直す。
 * 戻り値 = opId の行の様子 (渡したとき) / { sent, failed, pending }
 */
export async function sendGuideUploads({ opId = null } = {}) {
  if (!guideSyncEnabled() || guideUrlProblem()) return opId ? uploadView(uploadRow(opId)) : { sent: 0, failed: 0, pending: 0 };
  while (sending) await sending.catch(() => {});
  let done;
  sending = new Promise((r) => { done = r; });
  try {
    const db = getDB();
    const rows = opId
      ? [uploadRow(opId)].filter((r) => r && r.status === 'pending')
      : db.prepare(`SELECT * FROM pk_pack_guide_uploads WHERE status = 'pending' AND (next_at IS NULL OR next_at <= ?) ORDER BY created_at LIMIT 20`).all(utcNow());
    const out = { sent: 0, failed: 0, pending: 0 };
    for (const r of rows) {
      const v = await sendOne(r);
      out[v.status]++;
    }
    if (out.sent) await pullGuides();
    return opId ? uploadView(uploadRow(opId)) : out;
  } finally { sending = null; done(); }
}

/** 画面の様子: 商品ごとの送り待ちの数と、最近 (2 日) 断られたもの */
export function guideUploadsFor(codes) {
  const norms = new Set((codes || []).map(normSku).filter(Boolean));
  const out = {};
  if (!norms.size) return out;
  try {
    const since = new Date(Date.now() - 2 * 86400e3).toISOString().slice(0, 19) + 'Z';
    for (const r of getDB().prepare(`SELECT * FROM pk_pack_guide_uploads WHERE status = 'pending' OR (status = 'failed' AND created_at >= ?) ORDER BY created_at`).all(since)) {
      const k = normSku(r.sku);
      if (!norms.has(k)) continue;
      const o = out[k] || (out[k] = { pending: 0, failed: [] });
      if (r.status === 'pending') o.pending++;
      else o.failed.push({ error: r.last_error, worker: r.worker, at: r.created_at });
    }
  } catch (e) { console.error('[packing] 写真の送り待ちを読めない:', e.message); }
  return out;
}

let timer = null;
/** 数分ごとに送り待ちを送ってから写しを取る (起動の直後にも 1 回)。鍵が無い = 何もしない */
export function startGuidePoller() {
  if (timer || !guideSyncEnabled()) return false;
  const sec = Math.max(30, Number(process.env.PACKING_GUIDE_POLL_SEC) || 120);
  const tick = () => sendGuideUploads().catch((e) => console.warn(`[packing] 写真の送り待ちを送れない: ${e.message}`))
    .then(() => pullGuides()).then((r) => { if (!r.ok && !r.skipped) console.warn(`[packing] 梱包指示の取得に失敗 (前の写しで続ける): ${r.error}`); });
  setTimeout(tick, 5_000).unref?.();
  timer = setInterval(tick, sec * 1000);
  timer.unref?.();
  return true;
}
