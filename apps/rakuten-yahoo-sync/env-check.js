/**
 * RakutenYahooSync (RYS) 用 env チェック (Codex Phase E R3 確定):
 *
 * 設計原則:
 *   - 楽天 RMS は miniPC proxy 経由 (mercari-sync 同型)。 Render に楽天キーは置かない。
 *   - Notion 連携は 2026-09-14 に廃止 (旧 Notion 商品マスターは削除済み)。RYS_NOTION_TOKEN /
 *     NOTION_PRODUCT_MASTER_DB_ID は必須から外し「RYS では未使用」として一覧に残す (env が残っていても健全性に影響しない)。
 *     🚨 Render から消してはいけない: product-hub の画像DB取込 (services/notion-image-import.js) と
 *     product-links が lib/notion-client.js の getConfig() 経由で **両方** を読んでいる (Codex PR-1 R1 Medium)。
 *     消してよくなるのは、画像DBの設定を商品マスターの設定から分離してから。
 *   - Yahoo OAuth は既存 vps-proxy 経由。
 *   - secret 値は UI / DB / log に絶対に出さない。 set?:true/false と形式メタのみ。
 */

const REQUIRED_ENVS = Object.freeze([
  // miniPC proxy (楽天 RMS 経由用、 ranking-checker / warehouse healthcheck と同じパターン)
  { key: 'WAREHOUSE_URL',           purpose: 'miniPC bfaith-portal の base URL (例 https://wh.bfaith-wh.uk)', sensitive: false },
  { key: 'WAREHOUSE_SERVICE_TOKEN', purpose: 'miniPC /service-api/* 認証 Bearer token',                       sensitive: true },
  { key: 'CF_ACCESS_CLIENT_ID',     purpose: 'Cloudflare Access Service Token (miniPC tunnel 突破)',           sensitive: true },
  { key: 'CF_ACCESS_CLIENT_SECRET', purpose: 'Cloudflare Access Service Token secret',                         sensitive: true },
  // Yahoo store ID (publish で必須)
  { key: 'YAHOO_SELLER_ID',         purpose: 'Yahoo!ショッピング store ID',                                    sensitive: false },
]);

const OPTIONAL_ENVS = Object.freeze([
  // Yahoo OAuth proxy (vps-proxy 既存)
  { key: 'YAHOO_PROXY_BASE_URL',    purpose: 'Yahoo OAuth proxy (VPS)',                                       sensitive: false },
  { key: 'YAHOO_PROXY_SECRET',      purpose: 'Yahoo proxy 認証',                                              sensitive: true },
  { key: 'YAHOO_TOKEN_MINT_SECRET', purpose: 'Yahoo /yahoo/access-token mint',                                sensitive: true },
  // RYS 制御
  { key: 'RYS_PUBLISH_ENABLED',     purpose: '実 publish ON/OFF kill-switch (default=0、 E-5 で 1 に flip)',  sensitive: false },
  { key: 'AUC_PREF_CODE',           purpose: 'ヤフオク発送地 prefecture code (default=27 大阪)',              sensitive: false },
  // E-7-a Yahoo baseline
  { key: 'YAHOO_DIFF_QUERIES',      purpose: 'Yahoo baseline 検証用 query 上書き (本番未設定、 設定中は baseline 確立も write も拒否)', sensitive: false },
]);

/**
 * RYS では使わなくなった env (2026-09-14 Notion 連携廃止)。設定されていても RYS は読まない。
 * ただし product-hub の画像DB取込が同じ 2 つを共用しているので、画面では「RYS では未使用・消さない」と出す。
 */
const RETIRED_ENVS = Object.freeze([
  { key: 'RYS_NOTION_TOKEN',            purpose: '(RYS では未使用 2026-09-14〜) 旧 Notion 商品マスターの integration token。🚨 product-hub の画像DB取込が共用中なので Render から消さない', sensitive: true },
  { key: 'NOTION_PRODUCT_MASTER_DB_ID', purpose: '(RYS では未使用 2026-09-14〜) 旧 Notion 商品マスター DB ID。🚨 product-hub の画像DB取込が共用中なので Render から消さない',              sensitive: false },
]);

function summarize(key, sensitive) {
  const raw = process.env[key];
  if (!raw || !String(raw).trim()) {
    return { key, set: false };
  }
  const v = String(raw).trim();
  // 非 sensitive は値そのもの表示可
  if (!sensitive) return { key, set: true, value: v };
  // sensitive は length のみ (Codex R4 Low 対応: 断片も露出させない)
  return { key, set: true, length: v.length };
}

/**
 * 全 env の充足状況を返す (UI 表示用)。
 */
export function inspectEnvStatus() {
  const required = REQUIRED_ENVS.map((e) => ({ ...summarize(e.key, e.sensitive), purpose: e.purpose, required: true }));
  const optional = OPTIONAL_ENVS.map((e) => ({ ...summarize(e.key, e.sensitive), purpose: e.purpose, required: false }));
  const retired = RETIRED_ENVS.map((e) => ({ ...summarize(e.key, e.sensitive), purpose: e.purpose, required: false, retired: true }));
  const missingRequired = required.filter((r) => !r.set).map((r) => r.key);
  return {
    required,
    optional,
    retired,
    healthy: missingRequired.length === 0,
    missing_required: missingRequired,
    publish_enabled: String(process.env.RYS_PUBLISH_ENABLED || '0').trim() === '1',
  };
}

/**
 * 起動時 / endpoint 呼び出し時に fail-closed したい場合に使う。
 */
export function assertRequiredEnvs() {
  const r = inspectEnvStatus();
  if (!r.healthy) {
    const err = new Error(`RYS required env not set: ${r.missing_required.join(', ')}`);
    err.statusCode = 503;
    err.missing = r.missing_required;
    throw err;
  }
  return r;
}
