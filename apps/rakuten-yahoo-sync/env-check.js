/**
 * RakutenYahooSync (RYS) 用 env チェック (Codex Phase E R3 確定):
 *
 * 設計原則:
 *   - 楽天 RMS は miniPC proxy 経由 (mercari-sync 同型)。 Render に楽天キーは置かない。
 *   - Notion は Render 直接叩く (公開 API、 IP 制限なし)。 専用 integration token を env で持つ。
 *   - Yahoo OAuth は既存 vps-proxy 経由。
 *   - secret 値は UI / DB / log に絶対に出さない。 set?:true/false と形式メタのみ。
 */

const REQUIRED_ENVS = Object.freeze([
  // miniPC proxy (楽天 RMS 経由用、 ranking-checker / warehouse healthcheck と同じパターン)
  { key: 'WAREHOUSE_URL',           purpose: 'miniPC bfaith-portal の base URL (例 https://wh.bfaith-wh.uk)', sensitive: false },
  { key: 'WAREHOUSE_SERVICE_TOKEN', purpose: 'miniPC /service-api/* 認証 Bearer token',                       sensitive: true },
  { key: 'CF_ACCESS_CLIENT_ID',     purpose: 'Cloudflare Access Service Token (miniPC tunnel 突破)',           sensitive: true },
  { key: 'CF_ACCESS_CLIENT_SECRET', purpose: 'Cloudflare Access Service Token secret',                         sensitive: true },
  // Notion (Render 直接、 RYS 専用 integration)
  { key: 'RYS_NOTION_TOKEN',        purpose: 'Notion 商品マスター読み取り専用 integration token (RYS 専用)',    sensitive: true },
  { key: 'NOTION_PRODUCT_MASTER_DB_ID', purpose: 'Notion 商品マスター DB ID',                                  sensitive: false },
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
  const missingRequired = required.filter((r) => !r.set).map((r) => r.key);
  return {
    required,
    optional,
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
