/**
 * 楽天 → メルカリShops フィールドマッピング
 */
import { load } from 'cheerio';

const MAX_NAME_LEN = 130;
const MAX_DESC_LEN = 3000;
const MAX_IMAGES_CSV = 20;
const MAX_SKUS_CSV = 10;

export function stripHtml(html) {
  if (!html) return '';
  const $ = load(html);

  // 1. <img> 削除
  $('img').remove();

  // 2. テーブル処理
  $('table').each(function () {
    if ($(this).find('a').length > 0) {
      $(this).remove();
      return;
    }
    const textParts = [];
    $(this).find('tr').each(function () {
      $(this).find('br').replaceWith(', ');
      const th = $(this).find('th').first();
      const tds = $(this).find('td');
      if (th.length && tds.length) {
        const key = th.text().trim();
        const value = tds.last().text().trim().replace(/^,\s*|,\s*$/g, '');
        if (key && value) textParts.push(`${key}: ${value}`);
      } else if (tds.length >= 2) {
        const key = tds.first().text().trim();
        const value = tds.last().text().trim().replace(/^,\s*|,\s*$/g, '');
        if (key && value) textParts.push(`${key}: ${value}`);
      } else if (tds.length === 1) {
        const value = tds.first().text().trim().replace(/^,\s*|,\s*$/g, '');
        if (value) textParts.push(value);
      }
    });
    if (textParts.length) {
      $(this).replaceWith(textParts.join('\n') + '\n');
    } else {
      $(this).remove();
    }
  });

  // 3. <a> → テキストのみ
  $('a').each(function () { $(this).replaceWith($(this).text()); });

  // 4. <br> → 改行
  $('br').replaceWith('\n');

  let text = $.root().text();
  text = text.replace(/[ \t]+\n/g, '\n');
  text = text.replace(/\n{3,}/g, '\n\n');
  return text.trim();
}

function normalizeShopUrl(shopUrl) {
  if (shopUrl.includes('rakuten.co.jp/')) {
    return shopUrl.replace(/\/+$/, '').split('/').pop();
  }
  return shopUrl.replace(/\/+$/, '').replace(/^\/+/, '');
}

function buildSingleImageUrl(img, shopUrl) {
  const imgType = (img.type || '').toUpperCase();
  let imgPath = img.path || '';
  if (!imgPath) return '';
  if (imgPath.startsWith('http')) return imgPath;

  const shopName = normalizeShopUrl(shopUrl);
  if (imgType === 'CABINET') {
    if (!imgPath.startsWith('/')) imgPath = '/' + imgPath;
    return `https://image.rakuten.co.jp/${shopName}/cabinet${imgPath}`;
  } else if (imgType === 'GOLD') {
    return `https://image.rakuten.co.jp/${shopName}/gold${imgPath}`;
  }
  return '';
}

export function buildFullImageUrls(imagePaths, shopUrl, excludedPositions, excludedPatterns) {
  excludedPositions = excludedPositions || new Set();
  excludedPatterns = excludedPatterns || [];
  const urls = [];
  for (let i = 0; i < imagePaths.length && urls.length < MAX_IMAGES_CSV; i++) {
    if (excludedPositions.has(i + 1)) continue;
    const url = buildSingleImageUrl(imagePaths[i], shopUrl);
    if (!url) continue;
    if (excludedPatterns.some(p => url.toLowerCase().includes(p.toLowerCase()))) continue;
    urls.push(url);
  }
  return urls;
}

function sanitizeSkuKind(text) {
  const replacements = {
    '＆': '・', '&': '・', '＋': '+', '＝': '=',
    '／': '/', '＊': '*', '＃': '#', '％': '%', '＠': '@',
    '＜': '<', '＞': '>', '【': '[', '】': ']', '（': '(', '）': ')',
  };
  for (const [old, nw] of Object.entries(replacements)) {
    text = text.replaceAll(old, nw);
  }
  return text.length > 16 ? text.slice(0, 16) : text;
}

function buildVariantNameSuffix(selectorValues) {
  const values = Object.values(selectorValues || {}).filter(Boolean);
  return values.join(' / ');
}

function truncate(text, maxLen) {
  if (text.length > maxLen) return text.slice(0, maxLen - 1) + '…';
  return text;
}

export function rakutenItemToCsvRows(item, settings, categoryMapping) {
  const warnings = [];
  const shopUrl = settings.shop_url || '';
  const genreId = item.genreId || '';
  const mercariCategory = categoryMapping[genreId] || settings.default_mercari_category || '';

  const baseName = item.title || '';
  const description = truncate(stripHtml(item.description || ''), MAX_DESC_LEN);

  const excludedPositions = settings.excluded_image_positions || new Set();
  const excludedPatterns = settings.excluded_image_patterns || [];
  const imageUrls = buildFullImageUrls(item.imagePaths || [], shopUrl, excludedPositions, excludedPatterns);

  const variants = item.variants || [];
  const activeVariants = variants.filter(v => !v.hidden);

  const variantPrices = activeVariants
    .map(v => parseInt(v.price || 0))
    .filter(p => p > 0);

  let price = 0;
  if (variantPrices.length) {
    price = Math.min(...variantPrices);
    if (Math.min(...variantPrices) !== Math.max(...variantPrices)) {
      warnings.push(
        `SKU間で価格差あり（¥${Math.min(...variantPrices).toLocaleString()}〜¥${Math.max(...variantPrices).toLocaleString()}）。最安値 ¥${price.toLocaleString()} を採用します。`
      );
    }
  }

  if (price > 0 && price < 300) {
    warnings.push(`販売価格 ¥${price} → ¥300 に引き上げ（メルカリShops最低価格）`);
    price = 300;
  }

  const common = {};
  imageUrls.forEach((url, idx) => { common[`商品画像名_${idx + 1}`] = url; });
  common['商品説明'] = description;
  common['販売価格'] = price;
  common['商品の状態'] = 1;
  common['カテゴリID'] = mercariCategory;
  common['ブランドID'] = '';
  common['配送料の負担'] = parseInt(settings.shipping_cost_burden || '1');
  common['配送方法'] = parseInt(settings.shipping_method_code || '3');
  common['発送元の地域'] = settings.shipping_from_area_code || 'jp27';
  common['発送までの日数'] = parseInt(settings.days_to_ship_code || '2');
  common['商品ステータス'] = parseInt(settings.default_product_status || '1');

  if (activeVariants.length === 0) {
    const row = { ...common };
    row['商品名'] = truncate(baseName, MAX_NAME_LEN);
    row['SKU1_在庫数'] = 0;
    row['SKU1_商品管理コード'] = item.itemNumber || item.manageNumber || '';
    warnings.push('有効なバリエーションがありません（全て倉庫状態）。');
    return { rows: [row], warnings };
  }

  const totalPages = Math.ceil(activeVariants.length / MAX_SKUS_CSV);
  if (totalPages > 1) {
    warnings.push(`SKUが${activeVariants.length}件あるため、${totalPages}つの商品に分割して登録します。`);
  }

  const rows = [];
  for (let pageIdx = 0; pageIdx < totalPages; pageIdx++) {
    const chunk = activeVariants.slice(pageIdx * MAX_SKUS_CSV, (pageIdx + 1) * MAX_SKUS_CSV);
    const row = { ...common };

    if (totalPages > 1) {
      const suffix = ` (${pageIdx + 1}/${totalPages})`;
      row['商品名'] = truncate(baseName, MAX_NAME_LEN - suffix.length) + suffix;
    } else {
      row['商品名'] = truncate(baseName, MAX_NAME_LEN);
    }

    chunk.forEach((variant, idx) => {
      const variantName = buildVariantNameSuffix(variant.selectorValues || {});
      row[`SKU${idx + 1}_種類`] = sanitizeSkuKind(variantName);
      row[`SKU${idx + 1}_在庫数`] = 0;
      row[`SKU${idx + 1}_商品管理コード`] = variant.skuManageNumber || item.itemNumber || item.manageNumber || '';
      row[`SKU${idx + 1}_JANコード`] = '';
    });

    rows.push(row);
  }

  return { rows, warnings };
}
