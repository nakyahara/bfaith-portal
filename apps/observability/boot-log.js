/**
 * boot-log.js — 起動診断用ログヘルパー
 *
 * 目的: Render再起動ループ vs 同一プロセス内の多重init を切り分ける。
 * すべての初期化ポイントに pid / ppid / kind を出力する。
 *
 *   pid 毎回変わる  → プロセスが再起動している (Renderの restart loop / OOM)
 *   pid 固定で複数出る → 同一プロセス内で重複init が走っている (import副作用など)
 */

function tag(kind) {
  return `pid=${process.pid} ppid=${process.ppid} kind=${kind}`;
}

export function bootStart(kind, label) {
  console.log(`[Boot] ${tag(kind)} ${label} 初期化開始`);
}

export function bootEnd(kind, label, extra = '') {
  const suffix = extra ? ` ${extra}` : '';
  console.log(`[Boot] ${tag(kind)} ${label} 初期化完了${suffix}`);
}

export function bootNote(kind, msg) {
  console.log(`[Boot] ${tag(kind)} ${msg}`);
}

export function bootFail(kind, label, err) {
  const msg = err?.message || String(err);
  console.error(`[Boot] ${tag(kind)} ${label} 初期化失敗: ${msg}`);
}
