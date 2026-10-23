/**
 * 楽天画像 URL → JPEG 変換 → Yahoo uploadItemImage 経由 upload (Phase E-5b)。
 *
 * 設計原則:
 *   - Yahoo は JPEG のみ + 2MB 以下を受け付ける (uploadItemImage 仕様)
 *   - sharp で download → JPEG 変換 → quality を 85 → 50 で段階的に圧縮
 *   - それでも 2MB 超なら長辺 2000px で resize + 再圧縮
 *   - upload 自体は yahoo-publish-proxy 経由 (vps-proxy が token + 署名付与)
 */

import sharp from 'sharp';
import { filterUploadableImageUrls } from './yahoo-image.js';
import { callUploadItemImage, callUploadLibImage } from './yahoo-publish-proxy.js';

const DEFAULT_DOWNLOAD_TIMEOUT_MS = 20_000;
const MAX_BYTES = 2 * 1024 * 1024;

export class ImageUploadError extends Error {
  constructor(message, { url = null, cause = null } = {}) {
    super(message);
    this.name = 'ImageUploadError';
    this.url = url;
    if (cause) this.cause = cause;
  }
}

/**
 * 楽天 RMS の画像 location を完全 URL に正規化。
 *   - https://... or http://... → そのまま
 *   - //image.rakuten.co.jp/... → https: 補完
 *   - /image3/12960221/foo.jpg (相対パス) → https://image.rakuten.co.jp/{shopSlug}/cabinet{path}
 *
 *   楽天 RMS の images[].location は相対パス (`/image3/...` 等) で返ってくる。
 *   正式 URL 構造 (AI_reference rakuten-linegift-sync 開発経緯.md 等で確認):
 *     https://image.rakuten.co.jp/{shopSlug}/cabinet{location}
 *
 *   shopSlug は b-faith01 ストアでは `b-faith` (env RAKUTEN_SHOP_SLUG で上書き可)。
 *
 *   2026-06-25 中原さん smoke で 2 連発判明:
 *     1. host 抜け → Failed to parse URL (PR #346 で normalize 追加)
 *     2. shopSlug + cabinet 抜け → HTTP 404 (本 PR で修正)
 */
export function normalizeRakutenImageUrl(loc, { shopSlug = null } = {}) {
  if (typeof loc !== 'string') return null;
  const s = loc.trim();
  if (!s) return null;
  // 2026-10-22: 古い説明文によくある http:// の楽天画像は https に直す。旧実装はそのまま通し、
  //   ダウンロードの安全確認 (https の楽天 host だけ) で「楽天 host ではありません」になって毎回出品が失敗していた。
  //   楽天以外の http は直さない (安全確認でそのまま弾く)
  if (/^http:\/\//i.test(s)) {
    try {
      const u = new URL(s);
      if (isRakutenImageHost(u.hostname)) { u.protocol = 'https:'; return u.toString(); }
    } catch (_) { /* 下でそのまま返す */ }
    return s;
  }
  if (/^https?:\/\//i.test(s)) return s;
  if (/^\/\//.test(s)) return 'https:' + s;
  if (s.startsWith('/')) {
    const slug = (shopSlug || process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim();
    return `https://image.rakuten.co.jp/${slug}/cabinet${s}`;
  }
  return null;
}

// R6 H-1: redirect SSRF 防御 — fetch のデフォルト follow は危険なので manual + hop 毎再検証。
// R6 H-2: download body 上限 — JPEG 変換前の raw bytes も cap (sharp 入力で memory DoS 防止)。
const DOWNLOAD_MAX_REDIRECTS = 3;
const DOWNLOAD_RAW_MAX_BYTES = 16 * 1024 * 1024; // 圧縮前の raw 画像は 2MB target だが余裕で 16MB cap

async function downloadImage(initialUrl, timeoutMs = DEFAULT_DOWNLOAD_TIMEOUT_MS) {
  let url = initialUrl;
  let res;
  for (let hop = 0; hop <= DOWNLOAD_MAX_REDIRECTS; hop += 1) {
    if (!isAllowedDownloadUrl(url)) {
      throw new ImageUploadError(
        `download host not allowed (SSRF guard hop=${hop}): ${url}`, { url }
      );
    }
    try {
      res = await fetch(url, {
        method: 'GET',
        redirect: 'manual',
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (e) {
      throw new ImageUploadError(`download network/timeout: ${e.message}`, { url, cause: e });
    }
    // 3xx → Location を絶対 URL 化して再 check
    if (res.status >= 300 && res.status < 400) {
      const loc = res.headers.get('location');
      if (!loc) {
        throw new ImageUploadError(`download HTTP ${res.status} without Location`, { url });
      }
      try {
        url = new URL(loc, url).toString();
      } catch (_) {
        throw new ImageUploadError(`download redirect to invalid Location: ${loc}`, { url });
      }
      continue;
    }
    break;
  }
  if (!res || res.status >= 300) {
    throw new ImageUploadError(
      `download HTTP ${res?.status || '?'} after ${DOWNLOAD_MAX_REDIRECTS} hops`, { url }
    );
  }
  if (!res.ok) {
    throw new ImageUploadError(`download HTTP ${res.status}`, { url });
  }
  // R6 H-2: Content-Length 事前拒否
  const cl = res.headers.get('content-length');
  if (cl) {
    const n = Number(cl);
    if (Number.isFinite(n) && n > DOWNLOAD_RAW_MAX_BYTES) {
      throw new ImageUploadError(
        `download body too large: Content-Length ${n} > ${DOWNLOAD_RAW_MAX_BYTES}`, { url }
      );
    }
  }
  // streaming で読み、 cap 超過したら abort
  if (!res.body) {
    throw new ImageUploadError(`download no body`, { url });
  }
  const reader = res.body.getReader();
  const chunks = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      total += value.length;
      if (total > DOWNLOAD_RAW_MAX_BYTES) {
        try { reader.cancel(); } catch (_) {}
        throw new ImageUploadError(
          `download body too large: ${total} > ${DOWNLOAD_RAW_MAX_BYTES} (streaming cap)`, { url }
        );
      }
      chunks.push(value);
    }
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c)));
}

/**
 * JPEG にする。透過のある PNG / GIF は白い背景に重ねてから (2026-10-22)。
 * 旧実装は flatten していなかったので、透過部分が黒くなっていた (sharp の既定)。
 */
function toJpeg(rawBuffer, quality, { resize = false } = {}) {
  let p = sharp(rawBuffer).rotate();
  if (resize) p = p.resize({ width: 2000, height: 2000, fit: 'inside', withoutEnlargement: true });
  return p.flatten({ background: '#ffffff' }).jpeg({ quality }).toBuffer();
}

async function toJpegUnderLimit(rawBuffer, { maxBytes = MAX_BYTES } = {}) {
  // 1st pass: 元解像度 + quality 85 → 50 で 5 step 圧縮
  let quality = 85;
  let out;
  try {
    out = await toJpeg(rawBuffer, quality);
  } catch (e) {
    throw new ImageUploadError(`jpeg convert failed: ${e.message}`, { cause: e });
  }
  while (out.length > maxBytes && quality > 50) {
    quality = Math.max(50, quality - 10);
    out = await toJpeg(rawBuffer, quality);
    if (quality === 50) break;
  }
  if (out.length <= maxBytes) return { buffer: out, quality };

  // 2nd pass: 長辺 2000px に resize + quality 80 → 50 で再試行 (Codex E-5b R1 M-2: floor 50 厳守 + resize 後も段階圧縮)
  let q2 = 80;
  out = await toJpeg(rawBuffer, q2, { resize: true });
  while (out.length > maxBytes && q2 > 50) {
    q2 = Math.max(50, q2 - 10);
    out = await toJpeg(rawBuffer, q2, { resize: true });
    if (q2 === 50) break;
  }
  if (out.length > maxBytes) {
    throw new ImageUploadError(`image still > ${maxBytes}B after resize + quality 50 (${out.length}B)`);
  }
  return { buffer: out, quality: q2 };
}

function imageFileName(itemCode, index) {
  // Yahoo の画像命名規約 (AI_reference 設計書 v6 §3.6 + Phase 0 実測):
  //   - メイン画像 (index=0): {item_code}.jpg
  //   - サブ画像 (index>=1):  {item_code}_{1..20}.jpg  ← 1 桁、 zero-padding なし
  // 旧実装の _01/_02 (2 桁 padding) は Yahoo が HTTP 400 (2026-06-25 smoke で発覚)。
  if (index === 0) return `${itemCode}.jpg`;
  return `${itemCode}_${index}.jpg`;
}

// uploadLibImage 用 filename (Yahoo it-14061 修正 2026-06-27): `{item_code}_lib_{N}.jpg`
function libImageFileName(itemCode, index) {
  return `${itemCode}_lib_${index + 1}.jpg`;
}

// Yahoo it-14061 修正 (2026-06-27): main は uploadItemImage、 desc は uploadLibImage (別 API)。
//   main:  uploadItemImage で `{itemCode}.jpg + _1..20` の最大 21 枚
//          (R14 2026-07-03 中原さん指摘: Yahoo 公式はメイン1+サブ20=21枚。
//           旧 MAIN_MAX=10 は Phase E の保守的決め打ちで、 楽天11枚の商品が誤って止まっていた。
//           ファイル名検証 (yahoo-publish-proxy) と imageFileName は元から _20 まで対応済み)
//   desc:  uploadLibImage で `{itemCode}_lib_1..20` の最大 20 枚
//          → additional1/sp_additional に貼れる Yahoo CDN URL
//          (uploadItemImage で得た URL は it-14061 で additional1 に貼れない)
const MAIN_MAX = 21;
const DESC_MAX = 20;

// Codex Phase E image-html R5 H-1: SSRF 防御. download 対象は **https: + 楽天 host のみ**。
//   楽天 cabinet 画像は `image.rakuten.co.jp` か `*.r10s.jp` ドメイン。
//   外部 URL や内部向け (localhost / 169.254.169.254 等) は弾く。
// 2026-10-22: www.rakuten.ne.jp/gold/ (楽天 GOLD = 店舗の独自 HTML 置き場) も楽天の画像。旧実装は弾いて出品が失敗していた
const ALLOWED_DOWNLOAD_HOSTS_SUFFIX = ['.rakuten.co.jp', '.r10s.jp', '.rakuten.ne.jp'];

function isRakutenImageHost(hostname) {
  const host = String(hostname || '').toLowerCase();
  // hostname が rakuten.co.jp / r10s.jp / rakuten.ne.jp の subdomain か exact match
  return ALLOWED_DOWNLOAD_HOSTS_SUFFIX.some((suffix) => {
    const s = suffix.startsWith('.') ? suffix.slice(1) : suffix;
    return host === s || host.endsWith(suffix);
  });
}

function isAllowedDownloadUrl(url) {
  if (typeof url !== 'string') return false;
  let u;
  try { u = new URL(url); } catch (_) { return false; }
  if (u.protocol !== 'https:') return false;
  return isRakutenImageHost(u.hostname);
}

/**
 * 楽天 images[] の GOLD 画像 (type='GOLD') は location が GOLD のフォルダからの相対パス。
 * normalizeRakutenImageUrl は相対パスを cabinet の URL にするので、GOLD は先に完全 URL にしておく
 * (旧実装は GOLD でも cabinet の URL を組み立てて 404 になっていた。mercari-sync/mapper.js と同じ組み立て)
 */
function resolveGoldImages(rakutenImages, { shopSlug = null } = {}) {
  if (!Array.isArray(rakutenImages)) return rakutenImages;
  const slug = (shopSlug || process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim();
  return rakutenImages.map((img) => {
    if (!img || typeof img !== 'object') return img;
    const loc = typeof img.location === 'string' ? img.location.trim() : '';
    if (String(img.type || '').toUpperCase() !== 'GOLD' || !loc || /^(https?:)?\/\//i.test(loc)) return img;
    return { ...img, location: `https://image.rakuten.co.jp/${slug}/gold${loc.startsWith('/') ? '' : '/'}${loc}` };
  });
}

/**
 * 楽天 images[] (メイン画像) + 説明文画像 URL list (salesDescription/productDescription.sp 由来) を
 * 統合 upload して、 楽天 URL → Yahoo CDN URL の map と Yahoo CDN hostname allowlist を返す。
 *
 * 設計 (Codex Phase E image-html R1-R3 stop, fail-closed):
 *   - main は images[] (filter + normalize) 最大 21 枚 (Yahoo 公式: メイン1+サブ20)、 `{itemCode}.jpg` + `_1..20`
 *   - desc は descriptionImageUrls (normalize 済を期待) 最大 20 枚、 `{itemCode}_lib_1..20` (uploadLibImage)
 *   - 上限超過 → throw (publish 停止、 中原さんが楽天で画像減らす)
 *   - upload 1 件失敗 → throw (旧 fallback の `<br>` 化は禁止: R1 H-1)
 *   - Yahoo response の <ModeA> から URL を抽出して urlMap に積む
 *
 * @param {object} args
 * @param {Array}    args.rakutenImages          楽天 images[] (`{location, alt, type}` 配列)
 * @param {string}   args.itemCode               Yahoo item_code
 * @param {string[]} [args.customPatterns=[]]    image_exclusion_patterns
 * @param {string[]} [args.descriptionImageUrls=[]] salesDescription/productDescription.sp から
 *                                                抽出済の normalize 済楽天 URL (PC/SP 統合 dedupe 後)
 * @returns {Promise<{uploaded, urlMap: Map<string,string>, allowedYahooHosts: string[], errors}>}
 */
export async function uploadRakutenImagesToYahoo({
  rakutenImages,
  itemCode,
  customPatterns = [],
  descriptionImageUrls = [],
} = {}) {
  if (!itemCode) throw new ImageUploadError('itemCode is required');

  // 1. main 画像: filter + normalize + dedupe + host check (順序保持)
  //    R5 H-1: 楽天 host 以外の URL は弾く (SSRF 防御)
  const filtered = filterUploadableImageUrls(resolveGoldImages(rakutenImages), { customPatterns });
  const mainUrls = [];
  const seen = new Set();
  for (const raw of filtered) {
    const u = normalizeRakutenImageUrl(raw);
    if (!u) continue;
    if (!isAllowedDownloadUrl(u)) continue; // 楽天 host 以外は silent skip
    if (!seen.has(u)) {
      seen.add(u);
      mainUrls.push(u);
    }
  }
  if (mainUrls.length > MAIN_MAX) {
    throw new ImageUploadError(
      `${itemCode}: メイン画像 ${mainUrls.length} 枚は上限 ${MAIN_MAX} 枚を超過しました。 楽天で画像を ${MAIN_MAX} 枚以下に減らしてください。`
    );
  }

  // 2. desc 画像: filter (Codex R4 H-1: customPatterns + builtin coupon/review を desc にも適用)
  //    + dedupe + main との overlap 排除
  // descriptionImageUrls は string URL の配列 (publish-executor で抽出済) なので、
  // filterUploadableImageUrls に「URL 文字列」 として渡せばよい。
  const descFiltered = filterUploadableImageUrls(descriptionImageUrls, { customPatterns });
  const excludedRakutenUrls = new Set(
    descriptionImageUrls.filter((u) => typeof u === 'string' && !descFiltered.includes(u))
  );
  const descUrls = [];
  const descSeen = new Set();
  for (const u of descFiltered) {
    if (!u || typeof u !== 'string') continue;
    // R5 H-1: SSRF 防御。 楽天 host 以外は throw (説明文 HTML から拾った URL に対する安全境界)
    if (!isAllowedDownloadUrl(u)) {
      throw new ImageUploadError(
        `${itemCode}: 説明文画像 URL が楽天 host ではありません (https + image.rakuten.co.jp / r10s.jp のみ許可): ${u}`,
        { url: u }
      );
    }
    // Yahoo it-14061 修正: desc 画像は uploadLibImage で 別 API に upload するため、
    // main URL と重複してても **別 upload する** (main URL は uploadItemImage に行くので
    // additional1/sp_additional には貼れない)。 つまり dedupe は desc 内のみで行う。
    if (!descSeen.has(u)) {
      descSeen.add(u);
      descUrls.push(u);
    }
  }
  if (descUrls.length > DESC_MAX) {
    throw new ImageUploadError(
      `${itemCode}: 説明文画像 ${descUrls.length} 枚は上限 ${DESC_MAX} 枚を超過しました。 ` +
        `楽天の販売説明文/スマホ用商品説明文の <img> を ${DESC_MAX} 枚以下に減らしてください。 ` +
        `該当 URL: ${descUrls.slice(0, 5).join(', ')}${descUrls.length > 5 ? ' ...' : ''}`
    );
  }

  // 3. 順次 upload — main は uploadItemImage、 desc は uploadLibImage
  //   main は商品画像 URL (item-shopping.c.yimg.jp) → caption 用 (実運用では caption に img なし)
  //   desc は追加画像 URL (shopping.c.yimg.jp/lib/...) → additional1/sp_additional 用
  const mainUrlMap = new Map();          // 楽天URL → Yahoo item URL
  const libUrlMap = new Map();           // 楽天URL → Yahoo lib URL
  const allowedYahooLibHosts = new Set();
  const errors = [];
  let uploaded = 0;

  for (let i = 0; i < mainUrls.length; i += 1) {
    const url = mainUrls[i];
    const fileName = imageFileName(itemCode, i);
    await uploadMainOne(url, fileName, itemCode, mainUrlMap);
    uploaded += 1;
  }
  for (let i = 0; i < descUrls.length; i += 1) {
    const url = descUrls[i];
    const fileName = libImageFileName(itemCode, i);
    await uploadLibOne(url, fileName, itemCode, libUrlMap, allowedYahooLibHosts);
    uploaded += 1;
  }

  return {
    uploaded,
    mainUrlMap,                           // 旧 urlMap (main 用、 caption rewriter で使う想定 — 実運用では使わない)
    libUrlMap,                            // additional1/sp_additional rewriter で使う
    allowedYahooLibHosts: [...allowedYahooLibHosts],
    excludedRakutenUrls: [...excludedRakutenUrls],
    errors,
  };
}

/**
 * main 画像 1 枚 upload (uploadItemImage) して mainUrlMap に積む。 fail-closed。
 */
async function uploadMainOne(rakutenUrl, fileName, itemCode, mainUrlMap) {
  const raw = await downloadImage(rakutenUrl);
  const { buffer, quality } = await toJpegUnderLimit(raw);
  const magicHex = Buffer.from(buffer.slice(0, 2)).toString('hex');
  console.log(`[rys-upload main] item=${itemCode} file=${fileName} ${buffer.length}B q=${quality} magic=${magicHex} src=${rakutenUrl}`);
  const result = await callUploadItemImage({ buffer, fileName, itemCode });
  if (!result.yahooUrl) {
    throw new ImageUploadError(
      `${rakutenUrl}: uploadItemImage OK but no Yahoo URL extracted (file=${fileName})`,
      { url: rakutenUrl }
    );
  }
  mainUrlMap.set(rakutenUrl, result.yahooUrl);
  console.log(`[rys-upload main] item=${itemCode} file=${fileName} OK → ${result.yahooUrl}`);
}

/**
 * desc (追加画像) 1 枚 upload (uploadLibImage) して libUrlMap / allowedYahooLibHosts に積む。 fail-closed。
 * uploadLibImage は Yahoo response が <Status>OK</Status> のみで URL を返さないので
 * VPS proxy 側で合成 (`https://shopping.c.yimg.jp/lib/{store}/{fileName}`) して JSON で返却する。
 */
async function uploadLibOne(rakutenUrl, fileName, itemCode, libUrlMap, allowedYahooLibHosts) {
  const raw = await downloadImage(rakutenUrl);
  const { buffer, quality } = await toJpegUnderLimit(raw);
  const magicHex = Buffer.from(buffer.slice(0, 2)).toString('hex');
  console.log(`[rys-upload lib] item=${itemCode} file=${fileName} ${buffer.length}B q=${quality} magic=${magicHex} src=${rakutenUrl}`);
  const result = await callUploadLibImage({ buffer, fileName, itemCode });
  if (!result.yahooUrl) {
    throw new ImageUploadError(
      `${rakutenUrl}: uploadLibImage OK but proxy did not return yahooUrl (file=${fileName})`,
      { url: rakutenUrl }
    );
  }
  libUrlMap.set(rakutenUrl, result.yahooUrl);
  try {
    const u = new URL(result.yahooUrl);
    if (u.protocol === 'https:') allowedYahooLibHosts.add(u.hostname.toLowerCase());
  } catch (_) { /* malformed URL, skip host allowlist */ }
  console.log(`[rys-upload lib] item=${itemCode} file=${fileName} OK → ${result.yahooUrl}`);
}

// テスト用 (透過の白背景化の確認)
export { toJpeg as _toJpegForTest, isAllowedDownloadUrl as _isAllowedDownloadUrlForTest, resolveGoldImages as _resolveGoldImagesForTest };
