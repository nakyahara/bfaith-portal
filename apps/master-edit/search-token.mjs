/**
 * search-token.mjs — 詳細検索の長い条件 (商品コード 500 件など) を短い印にする (Codex #1620 R1 M2)
 *
 * 長い条件を GET の URL に載せると、Node の既定の上限 (ヘッダ 16KB) で Express に届く前に HTTP 431 になる。
 * → 画面は長い条件を POST /api/search (保存と同じ Origin・JSON の守り) で送り、ここで印に変えて、URL は ?s=<印> だけにする。
 * 印 = 条件の中身の指紋 (同じ条件は同じ印)。表はプロセスの中だけ (Render の bfaith-portal は 1 プロセス・1 台 = 永続ディスクの SQLite を使うので増やせない)。
 * 期限 (24 時間)・再起動 (deploy) で消えた印は「条件の期限が切れました。もう一度検索してください」と画面に出す。
 * 🚨 表の大きさは 件数 と 合計バイト数 の両方で上限 (#1620 Codex R2 M1)。超えたら使った順の古いものから消す (LRU)。
 *    1 件の大きさは router (POST /api/search) が先に 64KB までに断っている
 */
import crypto from 'node:crypto';

export const TOKEN_TTL_MS = 24 * 3600e3;
let maxTokens = 5000;
let maxBytes = 16 * 1024 * 1024;
const store = new Map();   // 印 → { cond, at, bytes }。Map の順 = 古い (使っていない) 順
let totalBytes = 0;
export const TOKEN_RE = /^[A-Za-z0-9_-]{16,64}$/;

function drop(token) {
  const v = store.get(token);
  if (!v) return;
  totalBytes -= v.bytes;
  store.delete(token);
}

/** 条件 (詳細検索の項目だけ・決まった形) を置いて印を返す */
export function putSearch(cond, now = Date.now()) {
  const keys = Object.keys(cond).sort();
  const body = JSON.stringify(keys.map((k) => [k, cond[k]]));
  const token = crypto.createHash('sha256').update(body).digest('base64url').slice(0, 22);
  drop(token);
  const bytes = Buffer.byteLength(body) + 64;   // 64 = 印・時刻などの分 (目安)
  store.set(token, { cond: { ...cond }, at: now, bytes });
  totalBytes += bytes;
  // 古いものから捨てる (期限切れ・件数・合計バイト数)。いま置いた 1 件は残す
  for (const [k, v] of store) {
    if (k === token) break;
    if (store.size <= maxTokens && totalBytes <= maxBytes && now - v.at <= TOKEN_TTL_MS) break;
    drop(k);
  }
  return token;
}
/** 印 → 条件。無い・期限切れ = null。使った印は新しい側へ (LRU) */
export function getSearch(token, now = Date.now()) {
  if (!TOKEN_RE.test(String(token || ''))) return null;
  const v = store.get(token);
  if (!v) return null;
  if (now - v.at > TOKEN_TTL_MS) { drop(token); return null; }
  store.delete(token); store.set(token, v);   // 期限は置いた時から (読んでも延ばさない)。順だけ新しく
  return { ...v.cond };
}
/** 試験だけ */
export function __clearSearchTokens() { store.clear(); totalBytes = 0; }
export function __setTokenLimits({ tokens = 5000, bytes = 16 * 1024 * 1024 } = {}) { maxTokens = tokens; maxBytes = bytes; }
export function __tokenStats() { return { count: store.size, bytes: totalBytes }; }
