/**
 * boot-log.js — 起動診断用ログヘルパー
 *
 * 目的: Render再起動ループ vs 同一プロセス内の多重init を切り分ける。
 *
 * Dockerコンテナでは pid=1 固定のため、プロセス寿命を区別する boot-id を生成する。
 *
 *   boot-id 毎回変わる → プロセスが再起動している (Renderの restart loop / OOM)
 *   boot-id 同じで init 複数回 → 同一プロセス内で重複init が走っている (import副作用)
 */
import { randomBytes } from 'crypto';

const BOOT_ID = randomBytes(4).toString('hex');
const BOOT_STARTED_AT = new Date().toISOString();

function tag(kind) {
  return `boot=${BOOT_ID} pid=${process.pid} ppid=${process.ppid} kind=${kind}`;
}

export function getBootId() { return BOOT_ID; }
export function getBootStartedAt() { return BOOT_STARTED_AT; }

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
