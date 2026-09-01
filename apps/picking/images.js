/**
 * 楽天白抜き商品画像の解決とキャッシュ (ピッキング画面用)。
 *
 * 方針 (実装計画§4・中原さん決定 2026-08-11 → 2026-08-25 改訂): 商品画像は
 *   ① バリエーション画像 variants[SKU].images[0] (そのSKU自身の写真。香り違い・色違いで別写真)
 *   ② 楽天RMSの白抜き画像 (whiteBgImage・商品共通。バリエーションがある商品ではどれか1つの写真)
 *   ③ 1枚目 (images[0])
 * の順。②を全SKUに使うと「No.4 の行に No.8 の写真」が出て取り違えの原因になる (2026-08-25 現場指摘)。
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
 *
 * 🚨 1つの ne_code に**複数の楽天コード**が紐づく (W=商品番号 / AM=連携SKU番号 / AL=SKU管理番号。
 *    同じ1SKUの別名 — [[reference_rakuten_sku_code_aliases]])。AL は "394" のような連番なので
 *    単独では商品管理番号に解決できない。1つだけ選んで試すと、たまたま AL を掴んだ商品が
 *    「楽天に無い」と誤判定される (2026-09-01 実測: 画像なし407件中124件がこれ)。
 *    → 候補は**順に全部試す**。
 * @param itemNumbers Map<norm(manage_number), manage_number>
 * @param rakutenCode 文字列 または 候補の配列 (先頭から試す)
 */
export function resolveManageNumber(itemNumbers, rakutenCode) {
  if (!itemNumbers || !rakutenCode) return null;
  const candidates = Array.isArray(rakutenCode) ? rakutenCode : [rakutenCode];
  for (const code of candidates) {
    if (!code) continue;
    const parts = String(code).split('-');
    const maxStrip = Math.min(3, parts.length - 1);
    for (let i = 0; i <= maxStrip; i++) {
      const candidate = parts.slice(0, parts.length - i).join('-');
      if (!candidate) break;
      const hit = itemNumbers.get(norm(candidate));
      if (hit) return hit;
    }
  }
  return null;
}

/**
 * ne_code に紐づく楽天コードの候補を取り出す。
 * 🚨 文字列が入っていても**1文字ずつに分解しない** (旧形式の索引・テストの差し替え互換)。
 */
function codesOf(rakutenByNe, sku) {
  const raw = rakutenByNe?.get(sku);
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.filter(Boolean);
  if (typeof raw === 'object') return (raw.all || []).filter(Boolean);
  return [raw];
}

/**
 * ne_code に紐づく「バリエーション照合用」のコード (AL=variantsのキー / AM=merchantDefinedSkuId)。
 * structured=true なら索引が役割を持っている = **W を照合に使ってはいけない**。
 * AL/AM が1つも無い (W だけの) 商品でも structured のままにする —
 * ここで配列へフォールバックすると W が照合対象に戻り、兄弟SKUの画像を掴む (Codex R2 High)。
 */
function variantCodesOf(rakutenByNe, sku) {
  const raw = rakutenByNe?.get(sku);
  if (raw && !Array.isArray(raw) && typeof raw === 'object') {
    return { variantIds: raw.variantIds || [], merchantIds: raw.merchantIds || [], structured: true };
  }
  return { variantIds: [], merchantIds: [], structured: false };   // 旧形式・テスト差し替え
}

/**
 * RMS item payload からバリエーション/白抜き/1枚目のURLを取り出す。
 * @param codes このSKUを指し得るコード群 (楽天SKUコード・ne_code)。variants のキー (variantId) または
 *              merchantDefinedSkuId と大文字小文字を無視して一致したものをそのSKUの画像とする
 */
export function extractImageUrls(item, codes = []) {
  const white = normalizeImageUrl(item?.whiteBgImage?.location);
  const top = normalizeImageUrl(Array.isArray(item?.images) ? item.images[0]?.location : null);
  let variant = null;
  const variants = item?.variants;
  if (variants && typeof variants === 'object') {
    // 🚨 照合は**役割ごと**に分ける (Codex 2026-09-01 High)。
    //    AL = variants のキーそのもの / AM = merchantDefinedSkuId / W = 商品番号 (商品単位)。
    //    W は兄弟SKUで共有され得るので variants の照合に使ってはいけない —
    //    使うと「別の色の写真」を掴む。ne_code だけは両方の fallback (product-hub 出品分)。
    // 走査は**候補順**。variants の列挙順に任せると、先に並んだ別 variant が先に当たる
    const asObj = !Array.isArray(codes) && codes && typeof codes === 'object';
    const list = (v) => [...new Set((Array.isArray(v) ? v : [v]).map(norm).filter(Boolean))];
    const variantIds = asObj ? list(codes.variantIds ?? []) : [];
    const merchantIds = asObj ? list(codes.merchantIds ?? []) : [];
    // 配列渡し (従来の呼び出し・テスト) は役割不明なので両方に当てる
    const any = asObj ? list(codes.any ?? []) : list(codes ?? []);
    const entries = Object.entries(variants);
    const pick = (wanted, field) => {
      for (const w of wanted) {
        for (const [vid, v] of entries) {
          const got = field === 'vid' ? vid : v?.merchantDefinedSkuId;
          if (norm(got) !== w) continue;
          const url = normalizeImageUrl(Array.isArray(v?.images) ? v.images[0]?.location : null);
          if (url) return url;
        }
      }
      return null;
    };
    variant = pick(variantIds, 'vid')
      ?? pick(merchantIds, 'mds')
      ?? pick(any, 'vid')
      ?? pick(any, 'mds');
  }
  return { variantUrl: variant, whiteBgUrl: white, topUrl: top };
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

// mirror 索引の短時間キャッシュ。一覧GET・CSV・再取得POST が続けて呼ぶうえ、
// mirror_rakuten_item_daily は日次で伸び続けるため毎回の全件 DISTINCT は避ける (Codex R1)
const MIRROR_MAPS_TTL_MS = 60_000;
let _mapsCache = null;
export function clearMirrorMapsCache() { _mapsCache = null; }

/** mirror から解決用の索引を読む (デフォルト実装。テストでは差し替える)。60秒キャッシュ。 */
function loadMirrorMaps() {
  if (_mapsCache && Date.now() - _mapsCache.at < MIRROR_MAPS_TTL_MS) return _mapsCache.value;
  const mdb = getMirrorDB();
  // 1 ne_code に最大3行 (W=商品番号 / AM=連携SKU番号 / AL=SKU管理番号)。**全部**持つ:
  //   - 商品管理番号の解決は W が最も近い (多くの商品で manageNumber と同値) → 先に試す
  //   - バリエーション画像の照合には AL (variants のキーそのもの) と AM (merchantDefinedSkuId) が要る
  // 並びは W → AM → AL。同 source 内は updated_at 昇順 (後から入った方を後ろに)
  const SOURCE_RANK = { w: 0, am: 1, al: 2 };
  const byNe = new Map();
  for (const r of mdb.prepare(
    'SELECT ne_code, rakuten_code, source FROM mirror_rakuten_sku_map ORDER BY updated_at ASC'
  ).all()) {
    if (!r.ne_code || !r.rakuten_code) continue;
    const k = norm(r.ne_code);
    if (!byNe.has(k)) byNe.set(k, []);
    byNe.get(k).push({ code: r.rakuten_code, rank: SOURCE_RANK[norm(r.source)] ?? 9 });
  }
  const rakutenByNe = new Map();
  for (const [k, rows] of byNe) {
    const sorted = rows.slice().sort((a, b) => a.rank - b.rank);
    const codes = [];
    for (const x of sorted) {
      if (!codes.some((c) => norm(c) === norm(x.code))) codes.push(x.code);
    }
    // all = 商品管理番号の解決用 (W→AM→AL の順に試す)
    // variantIds (AL) / merchantIds (AM) = バリエーション画像の照合用。W は**入れない**
    rakutenByNe.set(k, {
      all: codes,
      variantIds: sorted.filter((x) => x.rank === 2).map((x) => x.code),
      merchantIds: sorted.filter((x) => x.rank === 1).map((x) => x.code),
    });
  }
  const itemNumbers = new Map();
  for (const r of mdb.prepare(
    "SELECT DISTINCT item_manage_number FROM mirror_rakuten_item_daily WHERE item_manage_number IS NOT NULL AND trim(item_manage_number) <> ''"
  ).all()) {
    itemNumbers.set(norm(r.item_manage_number), r.item_manage_number);
  }
  const value = { rakutenByNe, itemNumbers };
  _mapsCache = { at: Date.now(), value };
  return value;
}

/** キャッシュ済みSKUの画像URLマップ: norm(ne_code) → {url, status}。表示用。 */
export function getImageMap(skus) {
  const keys = [...new Set(skus.map(norm).filter(Boolean))];
  if (keys.length === 0) return new Map();
  const db = getDB();
  const rows = db.prepare(
    `SELECT ne_code, variant_image_url, white_bg_url, top_image_url, status FROM pk_product_images
     WHERE ne_code IN (${keys.map(() => '?').join(',')})`
  ).all(...keys);
  const map = new Map();
  for (const r of rows) {
    map.set(r.ne_code, { url: r.variant_image_url || r.white_bg_url || r.top_image_url || null, status: r.status });
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
  // force = TTL を無視して取り直す (管理画面の「再取得」— not_found は当日中は再試行しないため、
  // 楽天側に画像を追加した直後に反映させる手段が無いと現場が待たされる)
  if (deps.force) {
    const stats = { requested: all.length, fetched: all.length, ok: 0, notFound: 0, errors: 0 };
    if (all.length === 0) return stats;
    return resolveAndCache(db, all, deps, stats);
  }
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
  return resolveAndCache(db, need, deps, stats);
}

/** 指定SKUを実際に解決してキャッシュへ書く (ensureImagesFor の本体・force でも共用)。 */
async function resolveAndCache(db, need, deps, stats) {
  const { rakutenByNe, itemNumbers } = (deps.loadMaps || loadMirrorMaps)();
  const mnBySku = new Map();
  const codesBySku = new Map();   // バリエーション照合用: このSKUを指し得るコード群
  for (const sku of need) {
    // 変換テーブルの楽天SKUコード (W/AM/AL の全部) + ne_code 自体
    // (product-hub 出品分は manageNumber = ne_code 小文字)。
    // 🚨 バリエーション画像の照合は AL (variants のキー) と AM (merchantDefinedSkuId) の
    //    両方が要る。1つだけ渡すと「別の色の写真」が出続ける (2026-09-01)
    const codes = [...codesOf(rakutenByNe, sku), sku];
    mnBySku.set(sku, resolveManageNumber(itemNumbers, codes));
    // 画像の照合は役割ごと (W は使わない)。索引が役割を持っていれば AL/AM がゼロでも
    // 役割つきで渡す (配列に落とすと W が混ざる)。
    // 🚨 any には ne_code も入れない — ne_code が W と同値の商品で W が照合に戻る (Codex R3 High)。
    //    変換テーブルに行がある = AL/AM/W が分かっているので、ne_code を当てる必要はない。
    //    行が無い商品 (product-hub 出品分・manageNumber = ne_code) は structured=false 側で
    //    従来どおり配列 [sku] を渡すので、そちらは影響を受けない
    const roles = variantCodesOf(rakutenByNe, sku);
    codesBySku.set(sku, roles.structured
      ? { variantIds: roles.variantIds, merchantIds: roles.merchantIds, any: [] }
      : codes);
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
    INSERT INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
    VALUES (?, ?, NULL, NULL, NULL, 'error', ?)
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
    INSERT INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ne_code) DO UPDATE SET manage_number = excluded.manage_number,
      white_bg_url = excluded.white_bg_url, top_image_url = excluded.top_image_url,
      variant_image_url = excluded.variant_image_url,
      status = excluded.status, fetched_at = excluded.fetched_at
  `);
  // fetched_at は取得後の「今」(リトライ待機で処理開始から数分経ち得るため now を使い回さない)
  const stamp = utcNow();
  for (const sku of need) {
    if (chunkFailedSkus.has(sku)) continue;   // チャンク失敗として記録済み
    const mn = mnBySku.get(sku);
    if (!mn) { upsert.run(sku, null, null, null, null, 'not_found', stamp); stats.notFound++; continue; }
    const item = itemByMn.get(norm(mn));
    if (!item) {
      // items にも failed にも無い欠落応答は「不存在」と断定できないため error 扱い
      // (30分後再試行。not_found で恒久抑止すると部分応答・プロキシ不具合で永久に画像なしになる)
      upsert.run(sku, mn, null, null, null, 'error', stamp);
      stats.errors++;
      continue;
    }
    const { variantUrl, whiteBgUrl, topUrl } = extractImageUrls(item, codesBySku.get(sku) || [sku]);
    if (!variantUrl && !whiteBgUrl && !topUrl) { upsert.run(sku, mn, null, null, null, 'not_found', stamp); stats.notFound++; continue; }
    upsert.run(sku, mn, whiteBgUrl, topUrl, variantUrl, 'ok', stamp);
    stats.ok++;
    if (variantUrl) stats.variant = (stats.variant || 0) + 1;
  }
  return stats;
}

// 取込直後の fire-and-forget 用の直列キュー (連続取込で miniPC RMS を並列に叩かない。
// rakuten-client は miniPC 側で直列化されるが、こちらでも並べておくとタイムアウトしにくい)。
// 同一SKU集合が既にキューにいる間は積み直さない — 作業画面を開くたびに呼ばれるうえ、
// リトライ待機で先頭ジョブが数分残ることがあり、無制限に積むとTTL越えの再取得が連鎖する (Codex medium)
let _chain = Promise.resolve();
const _pendingKeys = new Set();
let _queueDepth = 0;

/** いまキューで画像解決が走っているか (管理画面の再取得を多重に走らせないため — Codex R1)。 */
export function isImageQueueBusy() { return _queueDepth > 0; }

export function queueEnsureImages(skus, label = '', deps = undefined) {
  const set = [...new Set(skus.map(norm).filter(Boolean))].sort();
  // force は「同じ集合でも取り直す」操作なのでキーを分ける (通常キューと重複排除を共有しない)
  const key = JSON.stringify([deps?.force ? 'force' : 'ttl', set]);
  if (set.length === 0 || _pendingKeys.has(key)) return _chain;
  _pendingKeys.add(key);
  _queueDepth++;
  _chain = _chain
    .then(() => ensureImagesFor(skus, deps))
    .then((stats) => {
      if (deps?.onStats) { try { deps.onStats(stats); } catch { /* 呼び出し側の都合は無視 */ } }
      if (stats.fetched > 0) {
        console.log(`[picking-images] ${label} 解決 ${stats.fetched}件: ok=${stats.ok} (うちバリエーション画像=${stats.variant || 0}) なし=${stats.notFound} 失敗=${stats.errors}`);
      }
    })
    .catch((e) => {
      if (deps?.onError) { try { deps.onError(e); } catch { /* 呼び出し側の都合は無視 */ } }
      console.warn(`[picking-images] ${label} 画像解決失敗 (作業は継続可能): ${e.message}`);
    })
    .finally(() => { _pendingKeys.delete(key); _queueDepth--; });
  return _chain;
}

/**
 * 管理画面の「画像を再取得」— 直列キューに載せて取り直し、結果を返す (Codex R1: router から
 * ensureImagesFor を直接呼ぶと既存キューを迂回し、連打・取込直後の解決と並列に RMS を叩く)。
 * @returns {Promise<{stats}|null>} 既に解決処理が走っていれば null (呼び出し側が 409 にする)
 */
export function requestForceRefresh(skus, label = 'admin再取得') {
  if (isImageQueueBusy()) return Promise.resolve(null);
  const set = [...new Set(skus.map(norm).filter(Boolean))];
  if (set.length === 0) return Promise.resolve({ requested: 0, ok: 0, notFound: 0, errors: 0, fetched: 0 });
  // 🚨 通常キューは失敗を warn ログに落として握り潰す (取込を止めないため) が、手動の再取得では
  //    失敗を黙ってキュー混雑 (409) に見せてはいけない — 例外をそのまま呼び出し元へ返す (Codex R2)
  let out = null;
  let err = null;
  const p = queueEnsureImages(set, label, {
    force: true,
    onStats: (st) => { out = st; },
    onError: (e) => { err = e; },
  });
  return p.then(() => {
    if (err) throw err;
    return out;
  });
}

// ═══ 画像が出ない商品の一覧 (管理画面・2026-08-31 中原さん依頼) ═══════════════
// 現場の用途 = 「ピッキング画面で写真が出ない商品」を潰す。楽天の商品ページを直せば直るものと、
// そもそも楽天に商品が無い (＝画像を出しようがない) ものを、商品管理番号つきで見分けられるようにする。

/** JST 日付を n 日ずらす (service.js の shiftDate と同じ。循環 import を避けてローカルに置く)。 */
function shiftDateLocal(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** 楽天の商品ページURL (店舗スラッグは env。商品管理番号が無ければ null)。 */
export function rakutenItemUrl(manageNumber) {
  if (!manageNumber) return null;
  const slug = (process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim();
  return `https://item.rakuten.co.jp/${slug}/${encodeURIComponent(manageNumber)}/`;
}

const STATUS_LABEL = {
  not_found: '楽天に画像が無い',
  error: '取得に失敗 (再取得で直ることがある)',
  uncached: 'まだ取得していない',
};

/**
 * 期間内のピッキング明細に出たSKUのうち、画像まわりに問題があるものを返す。
 *
 *  - missing        = 画像URLが1つも無い (画面がプレースホルダになる)
 *  - variantMissing = 同じ楽天商品 (manage_number) を2SKU以上で共有しているのに、そのSKU自身の
 *                     バリエーション画像が無く商品共通の写真で代用している商品。
 *                     「No.4 の行に No.8 の写真」が出て取り違えの原因になる (2026-08-25 現場指摘) ため、
 *                     画像はあっても直す対象として並べる
 *
 * 楽天の商品管理番号は キャッシュ済み manage_number を優先し、無ければ mirror から今その場で解決する
 * (キャッシュ時点では mirror に商品が無くても、後から出品されていることがある — 中原さん依頼 2026-08-31)。
 * mirror が使えない環境では管理番号なしで一覧だけ返す (fail-soft)。
 */
export function listMissingImages({ until = jstToday(), days = 30 } = {}) {
  const db = getDB();
  const to = until;
  const since = shiftDateLocal(to, -(Math.max(1, days) - 1));
  // 明細を1回読んで JS で集約する (SKUごとの相関サブクエリは索引が効かず、
  // 商品名だけ期間外・無効バッチ由来になる穴もあった — Codex R1)
  const agg = new Map();
  for (const l of db.prepare(`
    SELECT LOWER(TRIM(l.sku)) AS sku, l.product_name, l.qty, l.rowid AS rid, b.work_date
    FROM pk_lines l
    JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.validity = 'valid' AND b.origin != 'repick'
      AND b.work_date >= ? AND b.work_date <= ?
  `).iterate(since, to)) {
    if (!l.sku) continue;
    let a = agg.get(l.sku);
    if (!a) { a = { sku: l.sku, name: null, nameRid: -1, nameDate: null, lines: 0, qty: 0, lastDate: null }; agg.set(l.sku, a); }
    a.lines++;
    a.qty += l.qty || 0;
    if (a.lastDate == null || l.work_date > a.lastDate) a.lastDate = l.work_date;
    // 商品名は期間内で最後に使われたもの (無効バッチ・再ピック由来の名前は入らない)
    // 商品名は「作業日が新しい方」を採り、同日なら後に取り込んだ行 (rowid が大きい方)。
    // rowid だけで比べると、過去日分を後から取り込んだときに古い日の名前で上書きされる (Codex R2)
    if (l.product_name && (a.nameDate == null || l.work_date > a.nameDate
        || (l.work_date === a.nameDate && l.rid > a.nameRid))) {
      a.name = l.product_name; a.nameRid = l.rid; a.nameDate = l.work_date;
    }
  }
  const skus = [...agg.keys()];
  const cache = new Map();
  const CH = 400;
  for (let i = 0; i < skus.length; i += CH) {
    const chunk = skus.slice(i, i + CH);
    for (const r of db.prepare(
      `SELECT ne_code, manage_number, status, variant_image_url, white_bg_url, top_image_url, fetched_at
       FROM pk_product_images WHERE ne_code IN (${chunk.map(() => '?').join(',')})`
    ).all(...chunk)) cache.set(r.ne_code, r);
  }

  // mirror から楽天コード・商品管理番号を引く (fail-soft)
  let maps = null;
  try {
    maps = loadMirrorMaps();
  } catch (e) {
    console.warn(`[picking-images] mirror参照失敗 (管理番号なしで続行): ${e.message}`);
  }

  // ① まず全SKUの「最終的な商品管理番号」を確定する。
  //    キャッシュ済みを優先し、無ければ mirror から解決 (後から出品された商品を拾う)
  const rows = [];
  for (const sku of skus) {
    const a = agg.get(sku);
    const c = cache.get(sku) || null;
    // 画面・CSV に出す「楽天SKUコード」は人が楽天の管理画面で検索する手がかり。
    // 連番の AL だけ見せても引けないので、分かっているコード (W/AM/AL) を全部並べる
    const codes = maps ? codesOf(maps.rakutenByNe, sku) : [];
    const rakutenCode = codes.length > 0 ? codes.join(' / ') : null;
    const resolved = maps ? resolveManageNumber(maps.itemNumbers, [...codes, sku]) : null;
    rows.push({ ...a, cache: c, rakutenCode, manageNumber: c?.manage_number || resolved || null, resolved });
  }

  // ② 共有数 (= バリエーション商品かどうか) は、確定した管理番号で数える (Codex R1 High)。
  //    母集団は画像キャッシュ全体 — 兄弟SKUがこの期間に流れていなくても商品構造は変わらないため
  const shareCount = new Map();
  const bump = (mn) => { if (mn) shareCount.set(norm(mn), (shareCount.get(norm(mn)) || 0) + 1); };
  const counted = new Set();
  try {
    for (const r of db.prepare(
      "SELECT ne_code, manage_number FROM pk_product_images WHERE manage_number IS NOT NULL AND trim(manage_number) <> ''"
    ).iterate()) { bump(r.manage_number); counted.add(r.ne_code); }
  } catch { /* 表が無い環境 */ }
  for (const r of rows) {
    if (!counted.has(r.sku) && r.manageNumber) bump(r.manageNumber);   // キャッシュに無い分だけ足す
  }

  const missing = [];
  const variantMissing = [];
  const byStatus = { not_found: 0, error: 0, uncached: 0 };
  for (const r of rows) {
    const c = r.cache;
    const hasImage = !!(c && (c.variant_image_url || c.white_bg_url || c.top_image_url));
    const base = {
      sku: r.sku,
      name: r.name || null,
      lines: r.lines,
      qty: r.qty,
      lastDate: r.lastDate,
      manageNumber: r.manageNumber,
      rakutenCode: r.rakutenCode,
      itemUrl: rakutenItemUrl(r.manageNumber),
      fetchedAt: c?.fetched_at || null,
    };
    if (!hasImage) {
      const status = c?.status || 'uncached';
      byStatus[status] = (byStatus[status] || 0) + 1;
      missing.push({
        ...base,
        status,
        statusLabel: STATUS_LABEL[status] || status,
        // キャッシュ時は管理番号が引けなかったが今は引ける = 再取得すれば直る可能性がある
        retryable: status === 'error' || status === 'uncached' || (!c?.manage_number && !!r.resolved),
      });
      continue;
    }
    const sharedBy = r.manageNumber ? (shareCount.get(norm(r.manageNumber)) || 1) : 1;
    if (!c.variant_image_url && sharedBy >= 2) {
      variantMissing.push({ ...base, sharedBy, imageUrl: c.white_bg_url || c.top_image_url });
    }
  }
  const byLines = (a, b) => b.lines - a.lines || String(a.sku).localeCompare(String(b.sku));
  missing.sort(byLines);
  variantMissing.sort(byLines);
  return {
    since, until: to, days,
    summary: {
      skus: rows.length,
      missing: missing.length,
      variantMissing: variantMissing.length,
      byStatus,
      mirrorAvailable: !!maps,
      retryable: missing.filter((x) => x.retryable && x.manageNumber).length,
    },
    missing,
    variantMissing,
  };
}

/** 一覧の CSV (Excel で開く前提の BOM つき)。 */
export function missingImagesCsv(result) {
  // Excel は = + - @ (と先頭のタブ・CR) で始まるセルを数式として評価するため、'  を前置して無害化する。
  // 商品名は外部CSV (ロジザード/NE) 由来なので管理者向け出力でも素通ししない (Codex R1)
  const esc = (v) => {
    let s = v == null ? '' : String(v);
    if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`;
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = ['\uFEFF区分,商品コード,商品名,状態,楽天商品管理番号,楽天SKUコード,商品ページURL,明細数,数量,最終ピッキング日,共有SKU数'];
  for (const r of result.missing) {
    lines.push([
      '画像なし', r.sku, r.name, r.statusLabel, r.manageNumber, r.rakutenCode, r.itemUrl,
      r.lines, r.qty, r.lastDate, '',
    ].map(esc).join(','));
  }
  for (const r of result.variantMissing) {
    lines.push([
      'バリエーション画像なし', r.sku, r.name, 'この商品の写真が無く別バリエーションの写真を表示',
      r.manageNumber, r.rakutenCode, r.itemUrl, r.lines, r.qty, r.lastDate, r.sharedBy,
    ].map(esc).join(','));
  }
  return lines.join('\r\n') + '\r\n';
}
