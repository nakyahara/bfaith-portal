/**
 * search-token.mjs — 詳細検索の長い条件 (商品コード 500 件など) を短い印にする (Codex #1620 R1 M2)
 *
 * 長い条件を GET の URL に載せると、Node の既定の上限 (ヘッダ 16KB) で Express に届く前に HTTP 431 になる。
 * → 画面は長い条件を POST /api/search (保存と同じ Origin・JSON の守り) で送り、ここで印に変えて、URL は ?s=<印> だけにする。
 * 印 = 条件の中身の指紋 (同じ条件は同じ印)。表はプロセスの中だけ (Render の bfaith-portal は 1 プロセス・1 台 = 永続ディスクの SQLite を使うので増やせない)。
 * 期限 (24 時間)・再起動 (deploy) で消えた印は「条件の期限が切れました。もう一度検索してください」と画面に出す。
 */
import crypto from 'node:crypto';

export const TOKEN_TTL_MS = 24 * 3600e3;
const MAX_TOKENS = 5000;
const store = new Map();   // 印 → { cond, at }
export const TOKEN_RE = /^[A-Za-z0-9_-]{16,64}$/;

/** 条件 (詳細検索の項目だけ・決まった形) を置いて印を返す */
export function putSearch(cond, now = Date.now()) {
  const keys = Object.keys(cond).sort();
  const token = crypto.createHash('sha256').update(JSON.stringify(keys.map((k) => [k, cond[k]]))).digest('base64url').slice(0, 22);
  store.delete(token);
  store.set(token, { cond: { ...cond }, at: now });
  // 古いものから捨てる (期限切れ・多すぎ)
  for (const [k, v] of store) {
    if (store.size <= MAX_TOKENS && now - v.at <= TOKEN_TTL_MS) break;
    store.delete(k);
  }
  return token;
}
/** 印 → 条件。無い・期限切れ = null */
export function getSearch(token, now = Date.now()) {
  if (!TOKEN_RE.test(String(token || ''))) return null;
  const v = store.get(token);
  if (!v) return null;
  if (now - v.at > TOKEN_TTL_MS) { store.delete(token); return null; }
  return { ...v.cond };
}
/** 試験だけ */
export function __clearSearchTokens() { store.clear(); }
