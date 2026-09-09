/**
 * Amazon FBM の SKU が Easy Ship かどうかを、ポータルの梱包サイズマスターに聞く。
 *
 * 🚨 マスターは **Render 側の easy-ship.db** にある (人がそこで登録する)。
 *    夜間バッチは miniPC で動くので、HTTPS で聞きに行くしかない。
 *    既存の拡張機能向け API (`/apps/easy-ship/ext-api`) をそのまま使う。
 *    新しい受け口も新しい env も増やさない (§16-15: env 名の食い違いで公開が 3 回止まった)。
 *
 * 🚨 **取れなかったら「自己配送」に倒さない**。倒すと、API が落ちた夜だけ
 *    全 FBM が自社の送料マスタで計算され、画面の数字が静かに変わる。
 *    取得に失敗したことを呼び出し側に返し、その夜の FBM は判定しない。
 */

const BULK_LIMIT = 200;          // ext-api の上限 (service.js bulkLookup)
const TIMEOUT_MS = 30_000;

/**
 * 1 往復に使ってよい時間。
 * 🚨 残り時間より長くしない。下限を 1 秒に切り上げると、残り 0.2 秒でも 1 秒待てることになり、
 *    期限を越える (Codex P2 2026-09-09)。呼ぶ前に期限は見ているので、ここは必ず正の値になる
 */
function remainingMs(deadline) {
  if (!deadline) return TIMEOUT_MS;
  return Math.max(1, Math.min(TIMEOUT_MS, deadline.getTime() - Date.now()));
}
/** 境界の計算そのものを試験で固定するための出口 (本番では使わない) */
export { remainingMs as __remainingMsForTest };

/**
 * ポータルのオリジン。publish.js と同じ env を使う (増やさない)。
 * 🚨 https 以外は使わない。トークンを平文で出さない
 */
export function easyshipBaseUrl(env = process.env) {
  const raw = String(env.RENDER_PORTAL_URL || env.RENDER_MIRROR_URL || '').trim();
  if (!raw) return '';
  let u;
  try { u = new URL(raw); } catch { return ''; }
  if (u.protocol !== 'https:') return '';
  return u.origin;
}

function chunk(list, size) {
  const out = [];
  for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size));
  return out;
}

/**
 * SKU の配列 → Map<sku(小文字), { sizeCode, sizeLabel, status }>。
 *
 * status: 'easyship'      … 有効な登録がある (Easy Ship で出している)
 *         'inactive'      … 登録はあるが無効
 *         'not_registered'… 登録が無い (= 自己配送とみなす。中原さん 2026-09-09)
 *
 * 🚨 **夜間バッチの期限を跨がない** (Codex P2 2026-09-09)。3,400 SKU = 18 往復あり、
 *    1 往復が 30 秒待つと 9 分ぶん期限を越えうる。1 往復ごとに残り時間で頭打ちにし、
 *    期限に達したら止める (止めた場合も「取れなかった」= 自己配送に倒さない)。
 *
 * @param {string[]} skus
 * @param {object} deps { fetchBulk, deadline } を差し替えると試験でネットワークを使わない
 * @returns {Promise<{ ok: boolean, map: Map, error: string|null, counts: object }>}
 */
export async function fetchEasyshipSizes(skus, deps = {}) {
  const map = new Map();
  const counts = { easyship: 0, inactive: 0, not_registered: 0 };
  const unique = [...new Set((skus || []).map((s) => String(s ?? '').trim()).filter(Boolean))];
  if (unique.length === 0) return { ok: true, map, error: null, counts };

  const deadline = deps.deadline || null;
  const fetchBulk = deps.fetchBulk || defaultFetchBulk(deps.env || process.env, deadline);
  const failed = (msg) => ({ ok: false, map: new Map(), error: msg,
    counts: { easyship: 0, inactive: 0, not_registered: 0 } });

  for (const part of chunk(unique, BULK_LIMIT)) {
    // 🚨 期限を跨いだら止める。残り時間で頭打ちにしても、往復の数だけ積み上がる
    if (deadline && Date.now() >= deadline.getTime()) {
      return failed('期限に達したので梱包サイズの照会を打ち切りました');
    }
    let res;
    try {
      res = await fetchBulk(part);
    } catch (e) {
      // 🚨 途中まで取れた分だけで判定しない。全部そろわなければ「取れなかった」
      return failed(e.message);
    }
    for (const r of res?.found || []) {
      map.set(String(r.sku).toLowerCase(), {
        status: 'easyship',
        sizeCode: r.packageSizeCode ?? null,
        sizeLabel: r.packageSizeLabel ?? null,
      });
      counts.easyship++;
    }
    for (const sku of res?.inactive || []) {
      map.set(String(sku).toLowerCase(), { status: 'inactive', sizeCode: null, sizeLabel: null });
      counts.inactive++;
    }
    for (const sku of res?.notFound || []) {
      map.set(String(sku).toLowerCase(), { status: 'not_registered', sizeCode: null, sizeLabel: null });
      counts.not_registered++;
    }
  }
  return { ok: true, map, error: null, counts };
}

/** 既定の取得 (ポータルの ext-api を叩く) */
function defaultFetchBulk(env, deadline = null) {
  const base = easyshipBaseUrl(env);
  const token = env.EASY_SHIP_EXT_TOKEN;
  // 🚨 設定が無いことを「登録が無い」と混同しない。名指しで落とす (§16-15)
  if (!base) throw new Error('RENDER_MIRROR_URL (または RENDER_PORTAL_URL) が未設定です');
  if (!token) throw new Error('EASY_SHIP_EXT_TOKEN が未設定です');
  const url = `${base}/apps/easy-ship/ext-api/api/v1/package-sizes/bulk-lookup`;
  return async (part) => {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': token },
      body: JSON.stringify({ skus: part }),
      // 🚨 固定の 30 秒だと期限を跨いで待ち続ける (publish.js と同じ作法)
      signal: AbortSignal.timeout(remainingMs(deadline)),
    });
    if (!res.ok) {
      const head = (await res.text().catch(() => '')).slice(0, 120).replace(/\s+/g, ' ');
      throw new Error(`梱包サイズマスターの照会が HTTP ${res.status} (${head})`);
    }
    const body = await res.json();
    // ext-api は { success, data } で包む (ext-router.js の api())
    return body?.data ?? body;
  };
}

/**
 * 梱包サイズを聞く必要がある SKU (Amazon の自社出荷ぶん)。
 *
 * 🚨 FBA には要らない (Amazon が配送する)。楽天にも要らない。
 *
 * 🚨 **直近の実行だけでは足りない**。列挙が partial だった夜は、世代ビルダーが
 *    前回の完全な実行から足りない出品を引き継ぐ (`mergeWithPreviousComplete`)。
 *    引き継いだぶんを聞き漏らすと、Easy Ship で出している出品が「登録が無い」= 自己配送
 *    と判定され、**違う送料で計算される** (Codex P1 2026-09-09)。
 *    → 直近の実行と、直近の**完全な**実行の両方から集める (重複は fetch 側で潰す)。
 */
export function loadEasyshipTargetSkus(db) {
  const runIds = [];
  const latest = db.prepare(`SELECT run_id FROM price_fetch_run
    WHERE mall = 'amazon' AND status IN ('ok', 'partial')
    ORDER BY started_at DESC, rowid DESC LIMIT 1`).get();
  if (latest) runIds.push(latest.run_id);
  // 引き継ぎ元 (build-generation.js loadLastCompleteRows と同じ選び方)
  const lastComplete = db.prepare(`SELECT run_id FROM price_fetch_run
    WHERE mall = 'amazon' AND listing_enum_status = 'ok' AND run_id <> ?
    ORDER BY started_at DESC, rowid DESC LIMIT 1`).get(latest?.run_id || '');
  if (lastComplete) runIds.push(lastComplete.run_id);
  if (runIds.length === 0) return [];

  const marks = runIds.map(() => '?').join(', ');
  return db.prepare(`SELECT DISTINCT mall_item_key FROM mall_price_snapshot
    WHERE run_id IN (${marks}) AND mall = 'amazon' AND fulfillment = 'FBM'`)
    .all(...runIds).map((r) => r.mall_item_key);
}
