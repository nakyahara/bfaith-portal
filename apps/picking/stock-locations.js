/**
 * 欠品通知に載せる「同一SKUの他ロケーション在庫」の取得と整形。
 *
 * データ源 = warehouse サーバの service-api (GET /service-api/logizard-stock/locations)。
 * miniPC 稼働時は WAREHOUSE_URL=127.0.0.1:3000 のローカル呼び出しで、通知の瞬間に
 * クラウド往復は挟まない。中身はロジザード在庫CSVの毎時取込 (9〜18時) スナップショット
 * なので、リアルタイム在庫ではない。通知には取得時刻「HH:MM時点」を必ず添え、
 * 180分より古いときは古い旨の警告を付ける (0件と取得不能は絶対に同一表示にしない)。
 *
 * fail-soft: env未設定・タイムアウト・エラーでも欠品通知本体は止めない
 * (null を返し、通知には「取得できず」と出す)。
 */

const TIMEOUT_MS = 5000;
const MAX_LINES = 5;          // 通知に載せる他ロケの上限 (フリー在庫の多い順)
const STALE_WARN_MIN = 180;   // これより古いスナップショットは警告付きで表示

/**
 * ロケ表記の正規化。ロジザード在庫CSVのロケは "001-001-01" (区切りあり8桁)、
 * ピッキング側 (CS03002) の location は "00100101" (8桁数字)。数字だけに揃えて比較する。
 */
export function normalizeLocationDigits(loc) {
  return String(loc || '').replace(/\D/g, '');
}

/** warehouse 連携が設定済みか。未設定環境 (導入前のRender等) では通知に在庫行を一切出さない。 */
export function stockLookupConfigured() {
  return Boolean(String(process.env.WAREHOUSE_URL || '').trim() && process.env.WAREHOUSE_SERVICE_TOKEN);
}

/**
 * warehouse service-api から SKU の全ロケ在庫を取る。失敗は null (fail-soft)。
 * @returns {{importedAt: string|null, stockDate: string|null, locations: Array}|null}
 */
export async function fetchStockLocations(sku, fetchFn = fetch) {
  const base = String(process.env.WAREHOUSE_URL || '').trim().replace(/\/+$/, '');
  const token = process.env.WAREHOUSE_SERVICE_TOKEN;
  const code = String(sku || '').trim().toLowerCase();
  if (!base || !token || !code) return null;
  const headers = { Authorization: `Bearer ${token}` };
  // Cloudflare Access 越し (Render から呼ぶ構成) のときだけ必要。ローカル呼び出しでは無害
  if (process.env.CF_ACCESS_CLIENT_ID && process.env.CF_ACCESS_CLIENT_SECRET) {
    headers['CF-Access-Client-Id'] = process.env.CF_ACCESS_CLIENT_ID;
    headers['CF-Access-Client-Secret'] = process.env.CF_ACCESS_CLIENT_SECRET;
  }
  try {
    const res = await fetchFn(`${base}/service-api/logizard-stock/locations?code=${encodeURIComponent(code)}`, {
      headers, signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data || data.ok !== true || !Array.isArray(data.locations)) throw new Error('想定外のレスポンス形式');
    return data;
  } catch (e) {
    console.warn(`[picking-stock] 他ロケ在庫の取得失敗 (通知は在庫情報なしで送る): ${e.message}`);
    return null;
  }
}

/** ISO時刻を JST の「HH:MM」(同日) / 「M/D HH:MM」(別日) にする。解釈できなければ null。 */
function jstStamp(iso, now = new Date()) {
  const t = Date.parse(iso || '');
  if (!Number.isFinite(t)) return null;
  const jst = new Date(t + 9 * 3600 * 1000);
  const nowJst = new Date(now.getTime() + 9 * 3600 * 1000);
  const hm = `${String(jst.getUTCHours()).padStart(2, '0')}:${String(jst.getUTCMinutes()).padStart(2, '0')}`;
  if (jst.toISOString().slice(0, 10) === nowJst.toISOString().slice(0, 10)) return hm;
  return `${jst.getUTCMonth() + 1}/${jst.getUTCDate()} ${hm}`;
}

/**
 * 通知メッセージ用の他ロケ在庫セクションを組み立てる。
 * - 良品のみ・フリー在庫 (在庫数-引当数) が正のロケのみ・欠品報告のあったロケは除外
 * - フリー在庫の多い順に最大 MAX_LINES 件
 * @param data fetchStockLocations の戻り値 (null = 取得失敗)
 */
export function buildStockLocationsText(data, { excludeBlock, excludeLocation, now = new Date() } = {}) {
  if (!data) return '📍 他ロケ在庫: 取得できず';
  const excludeDigits = normalizeLocationDigits(excludeLocation);
  const candidates = (data.locations || [])
    .filter((r) => String(r.quality || '') === '良品')
    .filter((r) => !(excludeDigits
      && String(r.block || '') === String(excludeBlock || '')
      && normalizeLocationDigits(r.location) === excludeDigits))
    .filter((r) => Number(r.free) > 0);
  // 棚ロケ = 8桁数字に正規化できるもの。ZZZ-ZZZ-ZZ 等の仮想ロケは棚として案内できないので
  // 個別に並べず「棚以外」として合算だけ見せる (実データ 2026-08-16 で確認)
  const isShelf = (r) => normalizeLocationDigits(r.location).length === 8;
  const rows = candidates.filter(isShelf)
    .sort((a, b) => (Number(b.free) - Number(a.free)) || String(a.location).localeCompare(String(b.location)));
  const otherFree = candidates.filter((r) => !isShelf(r)).reduce((sum, r) => sum + Number(r.free), 0);

  const stamp = jstStamp(data.importedAt, now);
  const ageMin = Number.isFinite(Date.parse(data.importedAt || '')) ? (now.getTime() - Date.parse(data.importedAt)) / 60000 : null;
  const stale = ageMin === null || ageMin > STALE_WARN_MIN;
  const header = `📍 他ロケ在庫 (${stamp || `在庫日${data.stockDate || '不明'}`}時点${stale ? ' ⚠古い可能性' : ''})`;
  if (rows.length === 0 && otherFree <= 0) return `${header}: なし`;
  const lines = rows.slice(0, MAX_LINES).map((r) => {
    const loc = `${r.block ? `${r.block}-` : ''}${r.location}`;
    return `・${loc}: ${r.free}個${Number(r.allocated) > 0 ? ` (別途引当${r.allocated})` : ''}`;
  });
  if (rows.length > MAX_LINES) lines.push(`…他${rows.length - MAX_LINES}ロケ`);
  if (otherFree > 0) lines.push(`・棚以外 (仮想ロケ等): ${otherFree}個`);
  return [header, ...lines].join('\n');
}
