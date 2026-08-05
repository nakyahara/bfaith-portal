/**
 * 「今 Render 上で動いているか」の判定。
 *
 * miniPC (WarehouseServer) は同じ server.js を動かすため、Render 専用の定期実行を
 * 無条件に起動すると二重実行になる (2026-08-05 に FBA同期・healthcheck・inbound-info で実際に発生)。
 * その gate をここに集約する。
 *
 * Render はプラットフォーム側で RENDER=true を必ず入れる。miniPC では未設定。
 * ただの truthy 判定だと RENDER="false" / "0" を Render 扱いしてしまうので、
 * 「止める意図の値」を明示的に除外する。
 */
const FALSY = new Set(['', '0', 'false', 'off', 'no']);

export function isRender(env = process.env) {
  return !FALSY.has(String(env.RENDER ?? '').trim().toLowerCase());
}
