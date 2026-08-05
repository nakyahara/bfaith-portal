/**
 * 「今 Render 上で動いているか」の判定。
 *
 * miniPC (WarehouseServer) は同じ server.js を動かすため、Render 専用の定期実行を
 * 無条件に起動すると二重実行になる (2026-08-05 に FBA同期・healthcheck・inbound-info で実際に発生)。
 * その gate をここに集約する。
 *
 * Render はプラットフォーム側で RENDER=true を必ず入れる。miniPC では未設定。
 * ⭐許可リスト方式にしてある。「falsy な値を除外する」方式だと RENDER=tru のような
 *   打ち間違いまで Render 扱いになり、二重実行がまた静かに復活する (Codexレビュー Low)。
 */
const TRUTHY = new Set(['true', '1', 'yes', 'on']);

export function isRender(env = process.env) {
  return TRUTHY.has(String(env.RENDER ?? '').trim().toLowerCase());
}
