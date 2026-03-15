/**
 * 楽天 RMS API クライアント（ItemAPI 2.0）
 */

const RMS_BASE = 'https://api.rms.rakuten.co.jp/es/2.0';
const REQUEST_INTERVAL = 1100; // ms

function makeEsaToken(serviceSecret, licenseKey) {
  return 'ESA ' + Buffer.from(`${serviceSecret}:${licenseKey}`).toString('base64');
}

function headers(serviceSecret, licenseKey) {
  return {
    'Authorization': makeEsaToken(serviceSecret, licenseKey),
    'Content-Type': 'application/json; charset=utf-8',
  };
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

export async function getAllItemCodes(serviceSecret, licenseKey) {
  const mapping = {}; // {itemNumber: manageNumber}
  let offset = 0;
  const hits = 100;

  while (true) {
    const url = `${RMS_BASE}/items/search?offset=${offset}&hits=${hits}`;
    const res = await fetch(url, { headers: headers(serviceSecret, licenseKey) });
    if (!res.ok) throw new Error(`RMS API HTTP ${res.status}`);
    const data = await res.json();

    const results = data.results || [];
    if (results.length === 0) break;

    for (const result of results) {
      const item = result.item || result;
      const manageNumber = item.manageNumber || '';
      const itemNumber = item.itemNumber || manageNumber;
      if (itemNumber) mapping[itemNumber] = manageNumber;
    }

    const numFound = data.numFound || 0;
    offset += hits;
    if (offset >= numFound) break;
    await sleep(REQUEST_INTERVAL);
  }

  return mapping;
}

function parseItem(raw) {
  const imagePaths = [];
  for (const img of (raw.images || [])) {
    const location = img.location || '';
    if (location) {
      imagePaths.push({ type: img.type || '', path: location });
    }
  }

  const variantsRaw = raw.variants || {};
  const variants = [];
  if (typeof variantsRaw === 'object' && !Array.isArray(variantsRaw)) {
    for (const [variantId, v] of Object.entries(variantsRaw)) {
      variants.push({
        variantId,
        skuManageNumber: v.merchantDefinedSkuId || '',
        price: parseInt(v.standardPrice || 0) || 0,
        selectorValues: v.selectorValues || {},
        hidden: Boolean(v.hidden),
      });
    }
  } else if (Array.isArray(variantsRaw)) {
    for (const v of variantsRaw) {
      variants.push({
        variantId: v.variantId || '',
        skuManageNumber: v.merchantDefinedSkuId || '',
        price: parseInt(v.standardPrice || 0) || 0,
        selectorValues: v.selectorValues || {},
        hidden: Boolean(v.hidden),
      });
    }
  }

  const pd = raw.productDescription || {};
  const description = typeof pd === 'object' ? (pd.pc || '') : '';

  return {
    manageNumber: raw.manageNumber || '',
    itemNumber: raw.itemNumber || raw.manageNumber || '',
    title: raw.title || '',
    genreId: String(raw.genreId || ''),
    description,
    imagePaths,
    variants,
  };
}

export async function getItemDetail(serviceSecret, licenseKey, manageNumber) {
  const url = `${RMS_BASE}/items/manage-numbers/${manageNumber}`;
  const res = await fetch(url, { headers: headers(serviceSecret, licenseKey) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const raw = await res.json();
  if (!raw) return null;
  return parseItem(raw);
}

export async function getItemDetailsBulk(serviceSecret, licenseKey, itemCodes) {
  // Try bulk-get first
  try {
    return await bulkGet(serviceSecret, licenseKey, itemCodes);
  } catch { /* fallback */ }

  // Fallback: individual fetch
  const results = [];
  for (const code of itemCodes) {
    try {
      const detail = await getItemDetail(serviceSecret, licenseKey, code);
      if (detail) results.push(detail);
    } catch (e) {
      results.push({ manageNumber: code, _error: e.message });
    }
    await sleep(REQUEST_INTERVAL);
  }
  return results;
}

async function bulkGet(serviceSecret, licenseKey, itemCodes) {
  const BATCH_SIZE = 50;
  const results = [];

  for (let i = 0; i < itemCodes.length; i += BATCH_SIZE) {
    const batch = itemCodes.slice(i, i + BATCH_SIZE);
    const url = `${RMS_BASE}/items/bulk-get`;
    const res = await fetch(url, {
      method: 'POST',
      headers: headers(serviceSecret, licenseKey),
      body: JSON.stringify({ manageNumbers: batch }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const rawItems = data.result || data.results || [];
    for (const raw of rawItems) {
      if (raw) results.push(parseItem(raw));
    }
    if (i + BATCH_SIZE < itemCodes.length) await sleep(REQUEST_INTERVAL);
  }
  return results;
}
