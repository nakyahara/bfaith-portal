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
import { getMeta, masterCounts, replaceMaster, setMeta, shrinkProblem } from './db.js';

const PULL_TTL_MS = Math.max(1, Number(process.env.SELECT_SET_MASTER_TTL_MIN) || 10) * 60 * 1000;
/** これより古いマスタでは展開しない (fail-soft の上限) */
const MAX_AGE_H = Math.max(1, Number(process.env.SELECT_SET_MASTER_MAX_AGE_H) || 24);
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
  const base = String(process.env.RENDER_MIRROR_URL || '').trim();
  let url = '';
  try {
    if (explicit) url = new URL(explicit).toString();
    else if (base) url = new URL('/apps/select-set/master-api/export', base).toString();
  } catch {
    return '';
  }
  return url;
}

/**
 * 取得先が安全か確かめる。
 * 🚨 ここには強い権限を持つ MIRROR_SYNC_KEY を載せるので、環境変数の設定ミスで
 *   平文や見知らぬホストへ送らないようにする (Codexレビュー4巡目 / 2026-08-08)。
 *   既定では RENDER_MIRROR_URL と同じホストしか許さない。
 *   別ホストを使いたいときは SELECT_SET_MASTER_ALLOWED_HOSTS にカンマ区切りで書く。
 */
export function checkRemoteUrl(url = remoteUrl()) {
  if (!url) return '取得先URLが設定されていません';
  let u;
  try { u = new URL(url); } catch { return '取得先URLの形式が不正です'; }
  if (u.protocol !== 'https:') return '取得先URLが https ではありません';
  const allowed = String(process.env.SELECT_SET_MASTER_ALLOWED_HOSTS || '')
    .split(',').map((h) => h.trim().toLowerCase()).filter(Boolean);
  if (!allowed.length) {
    try {
      const base = String(process.env.RENDER_MIRROR_URL || '').trim();
      if (base) allowed.push(new URL(base).host.toLowerCase());
    } catch { /* 判定できなければ下で弾く */ }
  }
  if (!allowed.length) return '許可するホストが決められません';
  if (!allowed.includes(u.host.toLowerCase())) {
    return `取得先ホスト ${u.host} は許可されていません`;
  }
  return null;
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
  const urlProblem = checkRemoteUrl();
  if (urlProblem) {
    setMeta(META.lastError, urlProblem);
    return { ok: false, error: urlProblem };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(remoteUrl(), { headers: { 'x-sync-key': key }, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    const snapshot = { sets: body?.sets, mappings: body?.mappings, omake: body?.omake };
    // 件数が不自然に減っていないか (Render側の事故を丸ごと反映しない)
    const shrink = Array.isArray(snapshot.sets) && Array.isArray(snapshot.mappings) && Array.isArray(snapshot.omake)
      ? shrinkProblem(masterCounts(), {
        sets: snapshot.sets.length, mappings: snapshot.mappings.length, omake: snapshot.omake.length,
      })
      : null;
    if (shrink) throw new Error(`${shrink}。取り込みを見送りました`);
    // 中身の検証は replaceMaster が行う (問題があれば投げるので既存マスタは残る)
    const counts = replaceMaster(snapshot);
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

/**
 * マスタが「使ってよい鮮度」か。
 * 🚨 fail-soft を無期限にすると、Render側で誤ったマッピングを直した直後に同期が壊れた場合、
 *   miniPC が修正前の商品へ展開し続ける (Codexレビュー4巡目 / 2026-08-08)。
 *   一度も取得できていない場合と、既定24時間を超えて古い場合は展開を止める。
 * @returns {string|null} 使えない理由 (使えるなら null)
 */
export function masterFreshnessProblem() {
  if (masterMode() !== 'replica') return null;
  const last = getMeta(META.lastPulledAt);
  if (!last) {
    return 'マスタをまだ一度も取得できていません (Render側との同期を確認してください)';
  }
  const ageH = (Date.now() - Date.parse(last)) / 3600000;
  if (!Number.isFinite(ageH)) return 'マスタの取得日時が読めません';
  if (ageH > MAX_AGE_H) {
    return `マスタが${Math.floor(ageH)}時間前のものです (許容 ${MAX_AGE_H}時間)。`
      + ' Render側との同期が止まっている可能性があるため展開を止めています';
  }
  return null;
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
    maxAgeHours: MAX_AGE_H,
    freshnessProblem: masterFreshnessProblem(),
  };
}
