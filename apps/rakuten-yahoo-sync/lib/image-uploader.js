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
import { callUploadItemImage, YahooProxyError } from './yahoo-publish-proxy.js';

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
  if (/^https?:\/\//i.test(s)) return s;
  if (/^\/\//.test(s)) return 'https:' + s;
  if (s.startsWith('/')) {
    const slug = (shopSlug || process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim();
    return `https://image.rakuten.co.jp/${slug}/cabinet${s}`;
  }
  return null;
}

async function downloadImage(url, timeoutMs = DEFAULT_DOWNLOAD_TIMEOUT_MS) {
  let res;
  try {
    res = await fetch(url, { method: 'GET', signal: AbortSignal.timeout(timeoutMs) });
  } catch (e) {
    throw new ImageUploadError(`download network/timeout: ${e.message}`, { url, cause: e });
  }
  if (!res.ok) {
    throw new ImageUploadError(`download HTTP ${res.status}`, { url });
  }
  return Buffer.from(await res.arrayBuffer());
}

async function toJpegUnderLimit(rawBuffer, { maxBytes = MAX_BYTES } = {}) {
  // 1st pass: 元解像度 + quality 85 → 50 で 5 step 圧縮
  let quality = 85;
  let out;
  try {
    out = await sharp(rawBuffer).rotate().jpeg({ quality }).toBuffer();
  } catch (e) {
    throw new ImageUploadError(`jpeg convert failed: ${e.message}`, { cause: e });
  }
  while (out.length > maxBytes && quality > 50) {
    quality = Math.max(50, quality - 10);
    out = await sharp(rawBuffer).rotate().jpeg({ quality }).toBuffer();
    if (quality === 50) break;
  }
  if (out.length <= maxBytes) return { buffer: out, quality };

  // 2nd pass: 長辺 2000px に resize + quality 80 → 50 で再試行 (Codex E-5b R1 M-2: floor 50 厳守 + resize 後も段階圧縮)
  let q2 = 80;
  out = await sharp(rawBuffer)
    .rotate()
    .resize({ width: 2000, height: 2000, fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: q2 })
    .toBuffer();
  while (out.length > maxBytes && q2 > 50) {
    q2 = Math.max(50, q2 - 10);
    out = await sharp(rawBuffer)
      .rotate()
      .resize({ width: 2000, height: 2000, fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality: q2 })
      .toBuffer();
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

/**
 * 楽天 images[] → filter → download → JPEG 変換 → uploadItemImage (順次)。
 *
 * @param {object} args
 * @param {Array} args.rakutenImages 楽天 raw images 配列 (location 含む)
 * @param {string} args.itemCode     Yahoo item_code (= 楽天 itemNumber)
 * @param {number} [args.maxImages=10] 最大 upload 枚数 (Yahoo 1 商品の上限を超えないため、 安全側)
 * @param {string[]} [args.customPatterns=[]] image_exclusion_patterns 由来のユーザー追加パターン
 * @returns {Promise<{uploaded: number, failed: number, errors: string[]}>}
 */
export async function uploadRakutenImagesToYahoo({ rakutenImages, itemCode, maxImages = 10, customPatterns = [] } = {}) {
  if (!itemCode) throw new ImageUploadError('itemCode is required');
  const filtered = filterUploadableImageUrls(rakutenImages, { customPatterns });
  if (filtered.length === 0) {
    return { uploaded: 0, failed: 0, errors: ['no_uploadable_image_after_filter'] };
  }
  // 楽天 RMS の location は相対パスで返ることがあるので host 補完してから download。
  // normalize で null になったもの (= 不正 URL) は除外し、 errors に残す。
  const normalized = [];
  const normalizeErrors = [];
  for (const raw of filtered) {
    const u = normalizeRakutenImageUrl(raw);
    if (u) normalized.push(u);
    else normalizeErrors.push(`${raw}: cannot normalize to absolute URL`);
  }
  if (normalized.length === 0) {
    return { uploaded: 0, failed: filtered.length, errors: ['no_normalizable_image_url', ...normalizeErrors] };
  }
  const limited = normalized.slice(0, maxImages);
  let uploaded = 0;
  let failed = 0;
  const errors = [];
  for (let i = 0; i < limited.length; i += 1) {
    const url = limited[i];
    const fileName = imageFileName(itemCode, i);
    try {
      const raw = await downloadImage(url);
      const { buffer, quality } = await toJpegUnderLimit(raw);
      // smoke gate 観測ログ: JPEG magic / size / filename 規約を VPS 送信前に確認
      //   (validateUploadFileName と JPEG magic / size は callUploadItemImage 内でも gate)
      const magicHex = Buffer.from(buffer.slice(0, 2)).toString('hex');
      console.log(`[rys-upload] item=${itemCode} idx=${i} file=${fileName} ${buffer.length}B q=${quality} magic=${magicHex}`);
      await callUploadItemImage({ buffer, fileName, itemCode });
      console.log(`[rys-upload] item=${itemCode} idx=${i} file=${fileName} OK`);
      uploaded += 1;
    } catch (e) {
      failed += 1;
      const msg = e instanceof YahooProxyError
        ? `${url}: yahoo_proxy ${e.message}`
        : (e instanceof ImageUploadError ? `${url}: ${e.message}` : `${url}: ${e.message || e}`);
      errors.push(msg);
    }
  }
  // normalize で除外したものも errors に積んで可視化 (failed カウントは uploaded されたもの以外)
  if (normalizeErrors.length > 0) {
    errors.push(...normalizeErrors);
    failed += normalizeErrors.length;
  }
  return { uploaded, failed, errors };
}
