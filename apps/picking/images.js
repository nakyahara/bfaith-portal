/**
 * 楽天白抜き商品画像の解決とキャッシュ (ピッキング画面用)。
 *
 * 方針 (実装計画§4・中原さん決定 2026-08-11): 商品画像は楽天RMSの白抜き画像
 * (whiteBgImage・検索サムネ用の別枠) を第一候補、無ければ1枚目 (images[0]) を使う。
 *
 * 解決チェーン (すべて既存部品の流用):
 *   ne_code (CSVの商品ID)
 *     → mirror_rakuten_sku_map で楽天SKUコード (無ければ ne_code をそのまま候補に)
 *     → ハイフン末尾を削りながら mirror_rakuten_item_daily に実在する商品管理番号を探す
 *       (apps/site-products/router.js resolveRakutenItemNumber と同方式)
 *     → miniPC /service-api/rakuten-rms/items/details-bulk (rakuten-rms-proxy 経由)
 *     → whiteBgImage.location → 完全URL化 → pk_product_images にキャッシュ
 *
 * 実行タイミング: CSV取込直後に「キャッシュに無いSKUだけ」fire-and-forget で解決
 * (1バッチ数十SKU = bulk 1〜2回。定期ジョブは作らない)。失敗は not_found / error として
 * キャッシュし、翌日以降の取込で再試行する。画面は URL が無ければプレースホルダ表示。
 */
import { getDB, utcNow, jstToday } from './db.js';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { fetchItemDetailsBulkDetailed } from '../rakuten-yahoo-sync/lib/rakuten-rms-proxy.js';

const norm = (s) => String(s ?? '').trim().toLowerCase();

/**
 * RMS の画像 location を完全URLへ (rakuten-yahoo-sync/lib/image-uploader.js の
 * normalizeRakutenImageUrl と同ロジック。あちらは sharp 等の重い依存を引き込むため転記)。
 */
// <img src> に直挿しするURLは楽天の画像ドメインに限定する
// (RMS応答が万一汚染されても、閲覧端末を任意ホストへ送信させない — Codex PR3.5 low)
function isAllowedImageHost(url) {
  try {
    const host = new URL(url).hostname;
    return host === 'rakuten.co.jp' || host.endsWith('.rakuten.co.jp')
      || host === 'r10s.jp' || host.endsWith('.r10s.jp');
  } catch {
    return false;
  }
}

export function normalizeImageUrl(loc) {
  if (typeof loc !== 'string') return null;
  const s = loc.trim();
  if (!s) return null;
  let url = null;
  if (/^https?:\/\//i.test(s)) url = s;
  else if (/^\/\//.test(s)) url = 'https:' + s;
  else if (s.startsWith('/')) {
    const slug = (process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim();
    url = `https://image.rakuten.co.jp/${slug}/cabinet${s}`;
  }
  return url && isAllowedImageHost(url) ? url : null;
}

/**
 * SKUコード → 実在する楽天商品管理番号。ハイフン末尾を最大3段削りながら探す
 * (SKU粒度のコードでは商品ページ・商品APIが404になるため。site-products で実測済み)。
 * @param itemNumbers Map<norm(manage_number), manage_number>
 */
export function resolveManageNumber(itemNumbers, rakutenCode) {
  if (!itemNumbers || !rakutenCode) return null;
  const parts = String(rakutenCode).split('-');
  const maxStrip = Math.min(3, parts.length - 1);
  for (let i = 0; i <= maxStrip; i++) {
    const candidate = parts.slice(0, parts.length - i).join('-');
    if (!candidate) break;
    const hit = itemNumbers.get(norm(candidate));
    if (hit) return hit;
  }
  return null;
}

/** RMS item payload から白抜き/1枚目のURLを取り出す。 */
export function extractImageUrls(item) {
  const white = normalizeImageUrl(item?.whiteBgImage?.location);
  const top = normalizeImageUrl(Array.isArray(item?.images) ? item.images[0]?.location : null);
  return { whiteBgUrl: white, topUrl: top };
}

/**
 * miniPC rakutenキュー混雑 (429 RATE_LIMIT_QUEUE_FULL) だけリトライする。
 * 朝の取込はdaily-sync等の長い楽天バッチと重なりやすく、即failすると
 * その日の画像が丸ごと欠ける (2026-08-14 実測: 15バッチ335件全滅)。
 * バックグラウンドの直列キュー内なので数分待っても作業には影響しない。
 */
const QUEUE_FULL_RE = /RATE_LIMIT_QUEUE_FULL|Too many pending/i;
const RETRY_MAX = 4;

/**
 * キュー混雑かどうかの判定は構造 (statusCode / body.error) を優先する。
 * メッセージ照合だけだと「500の本文にたまたま同じ文言」で誤リトライし得る (Codex high)。
 * statusCode を持たないエラー (プロキシ以外の例外) のみ文言でフォールバック判定。
 */
function isQueueFull(e) {
  if (!e) return false;
  // body.error はプロキシの構造化マーカーなので、transport の statusCode に関わらず混雑扱い
  if (e.statusCode === 429 || e.body?.error === 'RATE_LIMIT_QUEUE_FULL') return true;
  return e.statusCode == null && QUEUE_FULL_RE.test(String(e.message));
}

async function fetchWithQueueRetry(fetchDetails, manageNumbers, sleep) {
  const wait = sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  let lastErr = null;
  for (let attempt = 1; attempt <= RETRY_MAX; attempt++) {
    try {
      return await fetchDetails(manageNumbers);
    } catch (e) {
      if (!isQueueFull(e)) throw e;
      lastErr = e;
      if (attempt === RETRY_MAX) break;
      // 45秒×attempt + ジッター (同時刻に他クライアントと再突入しない)
      const delay = attempt * 45_000 + Math.floor(Math.random() * 15_000);
      console.warn(`[picking-images] rakutenキュー混雑 → ${Math.round(delay / 1000)}秒待って再試行 (${attempt}/${RETRY_MAX - 1})`);
      await wait(delay);
    }
  }
  throw lastErr;
}

/** mirror から解決用の索引を読む (デフォルト実装。テストでは差し替える)。 */
function loadMirrorMaps() {
  const mdb = getMirrorDB();
  const rakutenByNe = new Map();
  for (const r of mdb.prepare(
    'SELECT ne_code, rakuten_code FROM mirror_rakuten_sku_map ORDER BY updated_at ASC'
  ).all()) {
    if (r.ne_code) rakutenByNe.set(norm(r.ne_code), r.rakuten_code);
  }
  const itemNumbers = new Map();
  for (const r of mdb.prepare(
    "SELECT DISTINCT item_manage_number FROM mirror_rakuten_item_daily WHERE item_manage_number IS NOT NULL AND trim(item_manage_number) <> ''"
  ).all()) {
    itemNumbers.set(norm(r.item_manage_number), r.item_manage_number);
  }
  return { rakutenByNe, itemNumbers };
}

/** キャッシュ済みSKUの画像URLマップ: norm(ne_code) → {url, status}。表示用。 */
export function getImageMap(skus) {
  const keys = [...new Set(skus.map(norm).filter(Boolean))];
  if (keys.length === 0) return new Map();
  const db = getDB();
  const rows = db.prepare(
    `SELECT ne_code, white_bg_url, top_image_url, status FROM pk_product_images
     WHERE ne_code IN (${keys.map(() => '?').join(',')})`
  ).all(...keys);
  const map = new Map();
  for (const r of rows) {
    map.set(r.ne_code, { url: r.white_bg_url || r.top_image_url || null, status: r.status });
  }
  return map;
}

/**
 * 指定SKU群のうち未解決のものを解決してキャッシュする。
 * @returns {{requested, fetched, ok, notFound, errors}}
 */
// error 行の再試行間隔。実測 (2026-08-14) で朝の取込がminiPC rakutenキュー混雑
// (429 RATE_LIMIT_QUEUE_FULL) で全滅した際、旧仕様のJST翌日再試行では
// 「その日の作業画面はずっと画像なし」になったため、同日内に取り直せるよう30分に短縮
const ERROR_RETRY_MS = 30 * 60 * 1000;

export async function ensureImagesFor(skus, deps = {}) {
  const db = getDB();
  const all = [...new Set(skus.map(norm).filter(Boolean))];
  // ok は7日間キャッシュ (楽天側の画像差し替えに追従できるようTTLを置く)。
  // error は30分後に再試行 / not_found はJST当日中は再試行しない (翌日に取り直す)
  const need = all.filter((sku) => {
    const row = db.prepare('SELECT status, fetched_at FROM pk_product_images WHERE ne_code = ?').get(sku);
    if (!row) return true;
    const fetchedMs = Date.parse(row.fetched_at);
    if (!Number.isFinite(fetchedMs)) return true;
    if (row.status === 'ok') return (Date.now() - fetchedMs) > 7 * 24 * 3600 * 1000;
    if (row.status === 'error') return (Date.now() - fetchedMs) > ERROR_RETRY_MS;
    return jstToday(new Date(fetchedMs)) !== jstToday();
  });
  const stats = { requested: all.length, fetched: need.length, ok: 0, notFound: 0, errors: 0 };
  if (need.length === 0) return stats;

  const { rakutenByNe, itemNumbers } = (deps.loadMaps || loadMirrorMaps)();
  const mnBySku = new Map();
  for (const sku of need) {
    // 変換テーブルの楽天SKUコードを優先、無ければ ne_code 自体を候補にする
    // (product-hub 出品分は manageNumber = ne_code 小文字)
    const rakutenCode = rakutenByNe.get(sku) || sku;
    mnBySku.set(sku, resolveManageNumber(itemNumbers, rakutenCode));
  }

  const manageNumbers = [...new Set([...mnBySku.values()].filter(Boolean))];
  // mn → 対象SKU群 (チャンク失敗を該当SKUだけのerrorに落とすため。1つのmnに複数SKUがあり得る)
  const skusByMn = new Map();
  for (const [sku, mn] of mnBySku) {
    if (!mn) continue;
    if (!skusByMn.has(mn)) skusByMn.set(mn, []);
    skusByMn.get(mn).push(sku);
  }
  // チャンク単位で取得・リトライする (プロキシのチャンクと同じ50件 = 1呼び出し1リクエスト)。
  // fetchItemDetailsBulkDetailed 全体をリトライすると、途中チャンクの429で成功済み分まで
  // 再送してキュー混雑を自己増幅する (Codex medium)。失敗もチャンク内のSKUに限定して記録する
  const FETCH_CHUNK_SIZE = 50;
  const items = [];
  const failed = [];
  const chunkFailedSkus = new Set();
  const fetchDetails = deps.fetchDetails || fetchItemDetailsBulkDetailed;
  const upsertErr = db.prepare(`
    INSERT INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, status, fetched_at)
    VALUES (?, ?, NULL, NULL, 'error', ?)
    ON CONFLICT(ne_code) DO UPDATE SET manage_number = excluded.manage_number,
      status='error', fetched_at=excluded.fetched_at
  `);
  for (let i = 0; i < manageNumbers.length; i += FETCH_CHUNK_SIZE) {
    const chunk = manageNumbers.slice(i, i + FETCH_CHUNK_SIZE);
    try {
      const r = await fetchWithQueueRetry(fetchDetails, chunk, deps.sleep);
      if (Array.isArray(r?.items)) items.push(...r.items);
      if (Array.isArray(r?.failed)) failed.push(...r.failed);
    } catch (e) {
      // ここで throw すると台帳に何も残らず、画面を開くたびに再挑戦して連打してしまうため、
      // このチャンクのSKUを error として記録し、30分後の再試行に回す (黙殺しない方針)。
      // fetched_at はリトライ待機後の「今」を書く — 処理開始時刻だと待機ぶんTTLが目減りする (Codex medium)
      const stamp = utcNow();
      for (const mn of chunk) {
        for (const sku of (skusByMn.get(mn) || [])) {
          upsertErr.run(sku, mn, stamp);
          chunkFailedSkus.add(sku);
        }
      }
      console.warn(`[picking-images] RMSチャンク取得失敗 → ${chunk.length}件をerror記録 (30分後再試行): ${String(e.message).slice(0, 120)}`);
    }
  }
  stats.errors += chunkFailedSkus.size;
  const itemByMn = new Map();
  for (const it of items) {
    const mn = it?.manageNumber || it?.itemNumber;
    if (mn) itemByMn.set(norm(mn), it);
  }
  if (failed.length > 0) {
    console.warn(`[picking-images] RMS個別失敗 ${failed.length}件 (翌日再試行): ${failed.slice(0, 3).map((f) => f.manageNumber).join(', ')}…`);
  }

  const upsert = db.prepare(`
    INSERT INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, status, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(ne_code) DO UPDATE SET manage_number = excluded.manage_number,
      white_bg_url = excluded.white_bg_url, top_image_url = excluded.top_image_url,
      status = excluded.status, fetched_at = excluded.fetched_at
  `);
  // fetched_at は取得後の「今」(リトライ待機で処理開始から数分経ち得るため now を使い回さない)
  const stamp = utcNow();
  for (const sku of need) {
    if (chunkFailedSkus.has(sku)) continue;   // チャンク失敗として記録済み
    const mn = mnBySku.get(sku);
    if (!mn) { upsert.run(sku, null, null, null, 'not_found', stamp); stats.notFound++; continue; }
    const item = itemByMn.get(norm(mn));
    if (!item) {
      // items にも failed にも無い欠落応答は「不存在」と断定できないため error 扱い
      // (30分後再試行。not_found で恒久抑止すると部分応答・プロキシ不具合で永久に画像なしになる)
      upsert.run(sku, mn, null, null, 'error', stamp);
      stats.errors++;
      continue;
    }
    const { whiteBgUrl, topUrl } = extractImageUrls(item);
    if (!whiteBgUrl && !topUrl) { upsert.run(sku, mn, null, null, 'not_found', stamp); stats.notFound++; continue; }
    upsert.run(sku, mn, whiteBgUrl, topUrl, 'ok', stamp);
    stats.ok++;
  }
  return stats;
}

// 取込直後の fire-and-forget 用の直列キュー (連続取込で miniPC RMS を並列に叩かない。
// rakuten-client は miniPC 側で直列化されるが、こちらでも並べておくとタイムアウトしにくい)。
// 同一SKU集合が既にキューにいる間は積み直さない — 作業画面を開くたびに呼ばれるうえ、
// リトライ待機で先頭ジョブが数分残ることがあり、無制限に積むとTTL越えの再取得が連鎖する (Codex medium)
let _chain = Promise.resolve();
const _pendingKeys = new Set();
export function queueEnsureImages(skus, label = '', deps = undefined) {
  const set = [...new Set(skus.map(norm).filter(Boolean))].sort();
  const key = JSON.stringify(set);   // join(',')だと集合が違ってもキー衝突し得る (Codex low)
  if (set.length === 0 || _pendingKeys.has(key)) return _chain;
  _pendingKeys.add(key);
  _chain = _chain
    .then(() => ensureImagesFor(skus, deps))
    .then((stats) => {
      if (stats.fetched > 0) {
        console.log(`[picking-images] ${label} 解決 ${stats.fetched}件: 白抜き/1枚目=${stats.ok} なし=${stats.notFound} 失敗=${stats.errors}`);
      }
    })
    .catch((e) => console.warn(`[picking-images] ${label} 画像解決失敗 (作業は継続可能): ${e.message}`))
    .finally(() => _pendingKeys.delete(key));
  return _chain;
}
