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
// ロケは丸めず全部表示する (2026-08-16 中原さん要望「…他14ロケ にしない」)。
// これは安全上限のみ — LINE本文5000字制限で返信ごと拒否されるのを防ぐ (通常は到達しない)
const MAX_LINES = 80;
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

function serviceApiConfig() {
  const base = String(process.env.WAREHOUSE_URL || '').trim().replace(/\/+$/, '');
  const token = process.env.WAREHOUSE_SERVICE_TOKEN;
  if (!base || !token) return null;
  const headers = { Authorization: `Bearer ${token}` };
  // Cloudflare Access 越し (Render から呼ぶ構成) のときだけ必要。ローカル呼び出しでは無害
  if (process.env.CF_ACCESS_CLIENT_ID && process.env.CF_ACCESS_CLIENT_SECRET) {
    headers['CF-Access-Client-Id'] = process.env.CF_ACCESS_CLIENT_ID;
    headers['CF-Access-Client-Secret'] = process.env.CF_ACCESS_CLIENT_SECRET;
  }
  return { base, headers };
}

/**
 * warehouse service-api から SKU の全ロケ在庫を取る。失敗は null (fail-soft)。
 * @returns {{importedAt: string|null, stockDate: string|null, name: string|null, locations: Array}|null}
 */
export async function fetchStockLocations(sku, fetchFn = fetch) {
  const cfg = serviceApiConfig();
  const code = String(sku || '').trim().toLowerCase();
  if (!cfg || !code) return null;
  try {
    const res = await fetchFn(`${cfg.base}/service-api/logizard-stock/locations?code=${encodeURIComponent(code)}`, {
      headers: cfg.headers, signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data || data.ok !== true || !Array.isArray(data.locations)) throw new Error('想定外のレスポンス形式');
    // 行の形が壊れていても整形側 (buildStockLocationsText) が throw しないよう、object 以外は落とす
    data.locations = data.locations.filter((r) => r && typeof r === 'object');
    return data;
  } catch (e) {
    console.warn(`[picking-stock] 他ロケ在庫の取得失敗 (通知は在庫情報なしで送る): ${e.message}`);
    return null;
  }
}

/**
 * warehouse service-api で商品名/商品ID/バーコードから SKU 候補を検索する (LINE在庫検索ボット用)。
 * 失敗は null (fail-soft)。
 * @returns {{importedAt: string|null, items: Array<{sku,name,free}>}|null}
 */
export async function fetchStockSearch(query, fetchFn = fetch) {
  const cfg = serviceApiConfig();
  const q = String(query || '').trim();
  if (!cfg || q.length < 2) return null;
  try {
    const res = await fetchFn(`${cfg.base}/service-api/logizard-stock/search?q=${encodeURIComponent(q)}`, {
      headers: cfg.headers, signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data || data.ok !== true || !Array.isArray(data.items)) throw new Error('想定外のレスポンス形式');
    data.items = data.items.filter((r) => r && typeof r === 'object' && r.sku);
    return data;
  } catch (e) {
    console.warn(`[picking-stock] 在庫検索の取得失敗: ${e.message}`);
    return null;
  }
}

/** ロジザードの有効期限 (YYYYMMDD) を「YYYY/MM/DD」にする。空・形式外は null (=表示しない)。 */
function formatExpiry(v) {
  const m = String(v || '').trim().match(/^(\d{4})(\d{2})(\d{2})$/);
  return m ? `${m[1]}/${m[2]}/${m[3]}` : null;
}

/**
 * ISO時刻を JST の「H時MM分」(同日) / 「M/D H時MM分」(別日) にする。解釈できなければ null。
 * ⚠「16:05」形式にしない — LINE (iOS) が「数字:数字」を時刻と誤認して勝手にリンク化する
 * (2026-08-16 実機で確認。ロケ行の「…01: 200個」も同様なので在庫表示はコロンを使わない)
 */
function jstStamp(iso, now = new Date()) {
  const t = Date.parse(iso || '');
  if (!Number.isFinite(t)) return null;
  const jst = new Date(t + 9 * 3600 * 1000);
  const nowJst = new Date(now.getTime() + 9 * 3600 * 1000);
  const hm = `${jst.getUTCHours()}時${String(jst.getUTCMinutes()).padStart(2, '0')}分`;
  if (jst.toISOString().slice(0, 10) === nowJst.toISOString().slice(0, 10)) return hm;
  return `${jst.getUTCMonth() + 1}/${jst.getUTCDate()} ${hm}`;
}

/**
 * 通知メッセージ用の他ロケ在庫セクションを組み立てる。
 * - 良品のみ・フリー在庫 (在庫数-引当数) が正のロケのみ・欠品報告のあったロケは除外
 * - フリー在庫の多い順に最大 MAX_LINES 件
 * @param data fetchStockLocations の戻り値 (null = 取得失敗)
 */
export function buildStockLocationsText(data, {
  excludeBlock, excludeLocation, now = new Date(),
  // stock-bot (GChat在庫検索) からの流用用: 見出しと表示行数を差し替えられる
  title = '他ロケ在庫', maxLines = MAX_LINES,
} = {}) {
  if (!data) return `📍 ${title}: 取得できず`;
  const excludeDigits = normalizeLocationDigits(excludeLocation);
  const candidates = (data.locations || [])
    .filter((r) => String(r.quality || '') === '良品')
    .filter((r) => !(excludeDigits
      && String(r.block || '') === String(excludeBlock || '')
      && normalizeLocationDigits(r.location) === excludeDigits))
    .filter((r) => Number(r.free) > 0);
  // 棚ロケ (8桁数字に正規化できるもの) を先に、それ以外 (ロジザード上の特殊ロケ。
  // 実データに ZZZ-ZZZ-ZZ が存在) を後ろに並べる。特殊ロケも実在のロケコードなので、
  // 造語 (「仮想ロケ」等) にせずそのままの名前で見せる。
  // 並び = ロケ合計フリー在庫の多い順。同ロケの期限違いロットは隣接させて期限の近い順 (先入先出)
  const isShelf = (r) => normalizeLocationDigits(r.location).length === 8;
  const locKey = (r) => `${r.block}${r.location}`;
  const locTotals = new Map();
  for (const r of candidates) locTotals.set(locKey(r), (locTotals.get(locKey(r)) || 0) + Number(r.free));
  const byLocThenExpiry = (a, b) => (locTotals.get(locKey(b)) - locTotals.get(locKey(a)))
    || String(a.location).localeCompare(String(b.location))
    || String(a.expiry || '9999').localeCompare(String(b.expiry || '9999'));
  const rows = [...candidates.filter(isShelf).sort(byLocThenExpiry), ...candidates.filter((r) => !isShelf(r)).sort(byLocThenExpiry)];

  const stamp = jstStamp(data.importedAt, now);
  const ageMin = Number.isFinite(Date.parse(data.importedAt || '')) ? (now.getTime() - Date.parse(data.importedAt)) / 60000 : null;
  const stale = ageMin === null || ageMin > STALE_WARN_MIN;
  const header = `📍 ${title} (${stamp || `在庫日${data.stockDate || '不明'}`}時点${stale ? ' ⚠古い可能性' : ''})`;
  if (rows.length === 0) return `${header}: なし`;
  const lines = rows.slice(0, maxLines).map((r) => {
    const block = String(r.block || '');
    const loc = String(r.location || '');
    // ZZZ ブロックはロケ名にもZZZが含まれる (ZZZ-ZZZ-ZZ) ため、重ねて「ZZZ-ZZZ-ZZZ-ZZ」にしない。
    // 前方一致は「-」境界まで見る (ZZZ2-… のような別ブロックを誤って省略しない)
    const dup = loc === block || loc.startsWith(`${block}-`);
    const label = block && !dup ? `${block}-${loc}` : (loc || block);
    // 補足は1つの括弧にまとめる: (期限2028/01/15・別途引当10)
    const notes = [];
    const expiry = formatExpiry(r.expiry);
    if (expiry) notes.push(`期限${expiry}`);
    if (Number(r.allocated) > 0) notes.push(`別途引当${r.allocated}`);
    // 区切りは「→」— 「01: 200」のようなコロンはLINEが時刻と誤認してリンク化する
    return `・${label} → ${r.free}個${notes.length > 0 ? ` (${notes.join('・')})` : ''}`;
  });
  if (rows.length > maxLines) lines.push(`…他${rows.length - maxLines}ロケ`);
  // LINEの本文上限5,000字を超えると返信ごと拒否される。超えそうなときだけ末尾から間引く
  // (通常の在庫数では到達しない安全弁。丸めない方針の例外)
  let text = [header, ...lines].join('\n');
  if (text.length > 4500) {
    let dropped = 0;
    while (lines.length > 1 && [header, ...lines].join('\n').length > 4400) { lines.pop(); dropped++; }
    lines.push(`…ほか${dropped}行 (文字数上限)`);
    text = [header, ...lines].join('\n');
  }
  return text;
}
