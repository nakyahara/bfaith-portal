/**
 * マスタ取得 (miniPC ← Render)。
 *
 * マスタの「正」は Render 側。miniPC は必要になったときに取りに行って
 * ローカルの select-set.db に丸ごと反映する。
 *
 * ⭐ fail-soft: 取得に失敗しても前回取れた内容でそのまま動く。
 *   マスタは数十行しか無く、変わるのも年に数回なので、
 *   Render が落ちていても NEの展開が止まらないようにするのが狙い。
 *   代わりに「最終取得日時」を画面に出し、古くなったら気づけるようにする。
 *
 * 認証は既存の RENDER_MIRROR_URL / MIRROR_SYNC_KEY に相乗り (新しい秘密を増やさない)。
 */
import { isRender } from '../../lib/is-render.js';
import { getMeta, replaceMaster, setMeta } from './db.js';

const PULL_TTL_MS = Math.max(1, Number(process.env.SELECT_SET_MASTER_TTL_MIN) || 10) * 60 * 1000;
const FETCH_TIMEOUT_MS = 20_000;

export const META = {
  lastPulledAt: 'master_last_pulled_at',
  lastError: 'master_last_error',
  lastCounts: 'master_last_counts',
};

/** この環境がマスタを「持つ側」か「取りに行く側」か */
export function masterMode() {
  if (isRender()) return 'source';                       // Render = 正。ここで編集する
  if (!process.env.RENDER_MIRROR_URL) return 'standalone'; // 取得先が無い = 自前のDBで動く
  return 'replica';                                      // miniPC = Renderから取りに行く
}

/**
 * マスタ配信のURL。
 * 🚨 RENDER_MIRROR_URL は末尾にパスが付いている (実測: https://<host>/apps/mirror)。
 *   そのまま連結すると /apps/mirror/apps/select-set/... になって404になる (2026-08-08 に踏んだ)。
 *   絶対パスで解決して host だけを使う。
 */
function remoteUrl() {
  const explicit = String(process.env.SELECT_SET_MASTER_URL || '').trim();
  if (explicit) return explicit;
  const base = String(process.env.RENDER_MIRROR_URL || '').trim();
  if (!base) return '';
  try {
    return new URL('/apps/select-set/master-api/export', base).toString();
  } catch {
    return '';
  }
}

/**
 * Render からマスタを取得してローカルに反映する。
 * @returns {{ok:boolean, counts?:object, error?:string, skipped?:string}}
 */
export async function pullMaster() {
  if (masterMode() !== 'replica') return { ok: false, skipped: masterMode() };
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    const msg = 'MIRROR_SYNC_KEY 未設定のためマスタを取得できません';
    setMeta(META.lastError, msg);
    return { ok: false, error: msg };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(remoteUrl(), { headers: { 'x-sync-key': key }, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    if (!Array.isArray(body?.sets) || !Array.isArray(body?.mappings)) {
      throw new Error('レスポンスの形が想定と違います');
    }
    // 空のマスタで上書きしない。Render側の事故 (DB初期化直後など) を miniPC に伝播させないため
    if (body.sets.length === 0 && body.mappings.length === 0) {
      throw new Error('取得したマスタが空だったので反映しません');
    }
    const counts = replaceMaster(body);
    setMeta(META.lastPulledAt, new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'));
    setMeta(META.lastError, '');
    setMeta(META.lastCounts, JSON.stringify(counts));
    return { ok: true, counts };
  } catch (e) {
    const msg = String(e?.message || e).slice(0, 300);
    setMeta(META.lastError, msg);
    return { ok: false, error: msg };
  } finally {
    clearTimeout(timer);
  }
}

let inflight = null;

/**
 * 必要なら取得する (TTL内なら何もしない)。展開の前に呼ぶ。
 * 取得に失敗しても投げない (前回の内容で動く)。
 */
export async function ensureMasterFresh() {
  if (masterMode() !== 'replica') return { ok: true, skipped: masterMode() };
  const last = getMeta(META.lastPulledAt);
  if (last) {
    const age = Date.now() - Date.parse(last);
    if (Number.isFinite(age) && age >= 0 && age < PULL_TTL_MS) return { ok: true, cached: true };
  }
  if (inflight) return inflight;           // 同時リクエストで多重に取りに行かない
  inflight = pullMaster().finally(() => { inflight = null; });
  return inflight;
}

/** 画面表示用の取得状況 */
export function masterStatus() {
  const counts = getMeta(META.lastCounts);
  return {
    mode: masterMode(),
    lastPulledAt: getMeta(META.lastPulledAt) || null,
    lastError: getMeta(META.lastError) || null,
    lastCounts: counts ? JSON.parse(counts) : null,
    remote: masterMode() === 'replica' ? remoteUrl() : null,
    ttlMinutes: PULL_TTL_MS / 60000,
  };
}
