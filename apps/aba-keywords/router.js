/**
 * /aba-ext-api — Chrome拡張 (ABAキーワード) 用 API (miniPC 専用 mount)
 *
 * server.js 側で ENABLE_ABA_EXT === '1' のときだけ mount される
 * (ne-sync-control と同じ「Render には立てない」攻撃面削減パターン)。
 * 認証: x-api-key === ABA_EXT_TOKEN。未設定は fail-closed 503 (orders-lookup 準拠)。
 *
 * エンドポイント:
 *   GET  /reverse?asin=B0XXXXXXXX[&weeks=13] — ABA注文ワード逆引き (照会ASINは自動で監視対象化)
 *   POST /catalog { asins: [...] }           — 検索結果ページ用 BSR/梱包/ブランド (Catalog Items + cache)
 *   POST /offers  { asins: [...] }           — 出品者 (buy box)。ABA_EXT_OFFERS=1 のときだけ有効
 *   GET  /status                              — 取込済み週の一覧 (拡張オプション画面の接続テスト用)
 */
import express from 'express';
import SellingPartner from 'amazon-sp-api';
import { initAbaDB, getAbaDB } from './db.js';

const router = express.Router();

const ASIN_RE = /^[A-Z0-9]{10}$/;
const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
const CATALOG_TTL_MS = (parseInt(process.env.ABA_CATALOG_TTL_HOURS || '24', 10) || 24) * 3600 * 1000;
const OFFERS_TTL_MS = (parseInt(process.env.ABA_OFFERS_TTL_HOURS || '24', 10) || 24) * 3600 * 1000;
// Amazon.co.jp 本体の出品者ID (buy box が Amazon 本体かの判定用)。誤っていれば env で上書き
const AMAZON_JP_SELLER_ID = process.env.ABA_AMAZON_JP_SELLER_ID || 'A1AJ19H22SJLKA';

// ---- 認証 (fail-closed) ----
function requireExtToken(req, res, next) {
  const expected = process.env.ABA_EXT_TOKEN;
  if (!expected) return res.status(503).json({ error: 'aba_ext_token_unset' });
  if (req.headers['x-api-key'] !== expected) return res.status(401).json({ error: 'unauthorized' });
  next();
}
router.use(requireExtToken);

// body parser は認証の後 (未認可リクエストに parse させない。server.js の global parser からも除外済み)
router.use(express.json({ limit: '64kb' }));

// ---- DB 遅延初期化 (失敗時は 503、server 本体は道連れにしない) ----
let dbReady = false;
router.use((req, res, next) => {
  if (!dbReady) {
    try { initAbaDB(); dbReady = true; }
    catch (e) {
      console.error('[aba-ext] DB初期化失敗:', e.message);
      return res.status(503).json({ error: 'db_not_ready' });
    }
  }
  next();
});

// ---- SP-API (遅延生成・直列レート制御) ----
let spClient = null;
function getClient() {
  if (!process.env.SP_API_REFRESH_TOKEN || !process.env.SP_API_CLIENT_ID || !process.env.SP_API_CLIENT_SECRET) {
    throw new Error('SP-API 認証情報が未設定');
  }
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}

// レート制御: API種別ごとに前回呼び出しからの最小間隔を守る (helper一元化方針)
const lastCallAt = new Map();
async function throttle(key, minIntervalMs) {
  const prev = lastCallAt.get(key) || 0;
  const wait = prev + minIntervalMs - Date.now();
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastCallAt.set(key, Date.now());
}

function sanitizeAsins(input, max = 100) {
  if (!Array.isArray(input)) return null;
  const out = [...new Set(input.map(a => String(a || '').trim().toUpperCase()).filter(a => ASIN_RE.test(a)))];
  if (out.length === 0 || out.length > max) return null;
  return out;
}

// ============================================================
// GET /reverse — 注文ワード逆引き
// ============================================================
router.get('/reverse', (req, res) => {
  const asin = String(req.query.asin || '').trim().toUpperCase();
  if (!ASIN_RE.test(asin)) return res.status(400).json({ error: 'invalid_asin' });
  const weeksLimit = Math.min(52, Math.max(1, parseInt(req.query.weeks || '13', 10) || 13));

  const db = getAbaDB();

  // 照会実績 → 監視対象 (古い週の prune から保護)
  db.prepare(`
    INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count)
    VALUES (?, datetime('now'), datetime('now'), 1)
    ON CONFLICT(asin) DO UPDATE SET last_queried_at = datetime('now'), query_count = query_count + 1
  `).run(asin);

  const ingestedWeeks = db.prepare(
    'SELECT week_start, week_end FROM aba_weeks WHERE row_count > 0 ORDER BY week_start DESC LIMIT ?'
  ).all(weeksLimit);
  if (ingestedWeeks.length === 0) {
    return res.json({ asin, weeks: [], terms: [], note: 'ABAレポート未取込 (初回取込前)' });
  }
  const oldestWeek = ingestedWeeks[ingestedWeeks.length - 1].week_start;
  const latestWeek = ingestedWeeks[0].week_start;

  const hits = db.prepare(`
    SELECT week_start, department, search_term, search_frequency_rank,
           click_position, click_share, conversion_share, product_title
    FROM aba_search_terms
    WHERE asin = ? AND week_start >= ?
    ORDER BY week_start DESC
  `).all(asin, oldestWeek);

  const totalsStmt = db.prepare(`
    SELECT SUM(click_share) AS total_click, SUM(conversion_share) AS total_conv
    FROM aba_search_terms
    WHERE week_start = ? AND department = ? AND search_term = ?
  `);

  // (department, search_term) 単位に集計
  const byTerm = new Map();
  for (const row of hits) {
    const key = `${row.department}\x1f${row.search_term}`;
    if (!byTerm.has(key)) byTerm.set(key, { department: row.department, searchTerm: row.search_term, rows: [] });
    byTerm.get(key).rows.push(row);
  }

  const terms = [];
  for (const t of byTerm.values()) {
    // rows は week_start DESC 順
    const latest = t.rows[0];
    const prev = t.rows.find(r => r.week_start < latest.week_start) || null;
    const totals = totalsStmt.get(latest.week_start, latest.department, latest.search_term) || {};
    const rankChange = prev ? prev.search_frequency_rank - latest.search_frequency_rank : null; // 正=順位改善
    terms.push({
      searchTerm: t.searchTerm,
      department: t.department || null,
      latestWeek: latest.week_start,
      abaRank: latest.search_frequency_rank,
      prevWeek: prev ? prev.week_start : null,
      prevRank: prev ? prev.search_frequency_rank : null,
      weeklyChange: rankChange,
      weeklyChangePct: prev ? Math.round((prev.search_frequency_rank - latest.search_frequency_rank) / prev.search_frequency_rank * 10000) / 100 : null,
      clickPosition: latest.click_position,
      clickShare: latest.click_share,
      conversionShare: latest.conversion_share,
      top3ClickShare: totals.total_click ?? null,
      top3ConversionShare: totals.total_conv ?? null,
      productTitle: latest.product_title || null,
      weeksSeen: t.rows.length,
      firstSeen: t.rows[t.rows.length - 1].week_start,
      lastSeen: latest.week_start,
      isCurrent: latest.week_start === latestWeek,
      history: t.rows.slice(0, 13).map(r => ({ week: r.week_start, rank: r.search_frequency_rank })),
    });
  }
  // 最新週に出ている語 → ABA順位昇順、圏外の語 → 直近出現の新しい順
  terms.sort((a, b) => {
    if (a.isCurrent !== b.isCurrent) return a.isCurrent ? -1 : 1;
    if (a.isCurrent) return a.abaRank - b.abaRank;
    return a.lastSeen < b.lastSeen ? 1 : a.lastSeen > b.lastSeen ? -1 : a.abaRank - b.abaRank;
  });

  res.json({ asin, latestWeek, weeks: ingestedWeeks, terms });
});

// ============================================================
// POST /catalog — BSR / 梱包 / ブランド (Catalog Items API + キャッシュ)
// ============================================================

// 単位正規化
function toGrams(v, unit) {
  const u = String(unit || '').toLowerCase();
  if (u.startsWith('kilogram')) return Math.round(v * 1000);
  if (u.startsWith('gram')) return Math.round(v);
  if (u.startsWith('pound')) return Math.round(v * 453.592);
  if (u.startsWith('ounce')) return Math.round(v * 28.3495);
  return null;
}
function toCm(v, unit) {
  const u = String(unit || '').toLowerCase();
  if (u.startsWith('centimeter')) return Math.round(v * 10) / 10;
  if (u.startsWith('millimeter')) return Math.round(v / 10 * 10) / 10;
  if (u.startsWith('meter')) return Math.round(v * 100 * 10) / 10;
  if (u.startsWith('inch')) return Math.round(v * 2.54 * 10) / 10;
  return null;
}

function normalizeCatalogItem(item) {
  const summary = (item.summaries || []).find(s => s.marketplaceId === MARKETPLACE_ID) || (item.summaries || [])[0] || {};
  const ranksEntry = (item.salesRanks || []).find(s => s.marketplaceId === MARKETPLACE_ID) || (item.salesRanks || [])[0] || {};
  const ranks = [];
  for (const r of ranksEntry.displayGroupRanks || []) ranks.push({ title: r.title, rank: r.rank, type: 'display' });
  for (const r of ranksEntry.classificationRanks || []) ranks.push({ title: r.title, rank: r.rank, type: 'classification' });

  const attrs = item.attributes || {};
  const pickAttr = (name) => {
    const arr = attrs[name];
    if (!Array.isArray(arr) || arr.length === 0) return null;
    return arr.find(a => a.marketplace_id === MARKETPLACE_ID) || arr[0];
  };
  const weightAttr = pickAttr('item_package_weight');
  const dimsAttr = pickAttr('item_package_dimensions');
  const dims = dimsAttr ? {
    l: dimsAttr.length ? toCm(dimsAttr.length.value, dimsAttr.length.unit) : null,
    w: dimsAttr.width ? toCm(dimsAttr.width.value, dimsAttr.width.unit) : null,
    h: dimsAttr.height ? toCm(dimsAttr.height.value, dimsAttr.height.unit) : null,
  } : null;

  return {
    asin: item.asin,
    title: summary.itemName || null,
    brand: summary.brand || null,
    ranks,
    packageWeightG: weightAttr ? toGrams(Number(weightAttr.value), weightAttr.unit) : null,
    packageDimsCm: dims && (dims.l || dims.w || dims.h) ? dims : null,
  };
}

router.post('/catalog', async (req, res) => {
  const asins = sanitizeAsins(req.body?.asins);
  if (!asins) return res.status(400).json({ error: 'invalid_asins (1〜100件のASIN配列)' });

  const db = getAbaDB();
  const now = Date.now();
  const cacheGet = db.prepare('SELECT payload, fetched_at FROM aba_catalog_cache WHERE asin = ?');
  const cachePut = db.prepare('INSERT OR REPLACE INTO aba_catalog_cache (asin, payload, fetched_at) VALUES (?, ?, ?)');

  const result = {};
  const misses = [];
  for (const asin of asins) {
    const hit = cacheGet.get(asin);
    if (hit && now - Date.parse(hit.fetched_at) < CATALOG_TTL_MS) {
      result[asin] = JSON.parse(hit.payload);
    } else {
      misses.push(asin);
    }
  }

  let fetchErrors = 0;
  if (misses.length > 0) {
    let sp;
    try { sp = getClient(); }
    catch (e) { return res.status(502).json({ error: e.message, items: result }); }

    for (let i = 0; i < misses.length; i += 20) {
      const chunk = misses.slice(i, i + 20);
      try {
        // searchCatalogItems: 2req/秒 (burst 2)。安全側 600ms 間隔
        await throttle('catalog', 600);
        const resp = await sp.callAPI({
          operation: 'searchCatalogItems',
          endpoint: 'catalogItems',
          query: {
            identifiers: chunk.join(','),
            identifiersType: 'ASIN',
            marketplaceIds: [MARKETPLACE_ID],
            includedData: 'summaries,salesRanks,attributes',
            pageSize: 20,
          },
          options: { version: '2022-04-01' },
        });
        const ts = new Date().toISOString();
        for (const item of resp.items || []) {
          const norm = normalizeCatalogItem(item);
          result[norm.asin] = norm;
          cachePut.run(norm.asin, JSON.stringify(norm), ts);
        }
        // レポートに存在しないASIN (廃番等) も「なし」でキャッシュして連打を防ぐ
        for (const asin of chunk) {
          if (!result[asin]) {
            const empty = { asin, notFound: true };
            result[asin] = empty;
            cachePut.run(asin, JSON.stringify(empty), ts);
          }
        }
      } catch (e) {
        fetchErrors++;
        console.error('[aba-ext] catalog取得失敗:', e.message);
        for (const asin of chunk) if (!result[asin]) result[asin] = { asin, error: true };
      }
    }
  }
  res.json({ items: result, fetched: misses.length, cached: asins.length - misses.length, fetchErrors });
});

// ============================================================
// POST /offers — buy box 出品者 (既定OFF: ABA_EXT_OFFERS=1 で有効化)
// レート制限が厳しい (getItemOffersBatch ≈ 0.1req/秒) ため任意機能扱い
// ============================================================
router.post('/offers', async (req, res) => {
  if (process.env.ABA_EXT_OFFERS !== '1') return res.status(404).json({ error: 'offers_disabled' });
  const asins = sanitizeAsins(req.body?.asins, 60);
  if (!asins) return res.status(400).json({ error: 'invalid_asins (1〜60件のASIN配列)' });

  const db = getAbaDB();
  const now = Date.now();
  const cacheGet = db.prepare('SELECT payload, fetched_at FROM aba_offers_cache WHERE asin = ?');
  const cachePut = db.prepare('INSERT OR REPLACE INTO aba_offers_cache (asin, payload, fetched_at) VALUES (?, ?, ?)');

  const result = {};
  const misses = [];
  for (const asin of asins) {
    const hit = cacheGet.get(asin);
    if (hit && now - Date.parse(hit.fetched_at) < OFFERS_TTL_MS) result[asin] = JSON.parse(hit.payload);
    else misses.push(asin);
  }

  if (misses.length > 0) {
    let sp;
    try { sp = getClient(); }
    catch (e) { return res.status(502).json({ error: e.message, items: result }); }

    for (let i = 0; i < misses.length; i += 20) {
      const chunk = misses.slice(i, i + 20);
      try {
        await throttle('offers', 12000); // 0.1req/秒 → 12秒間隔で安全側
        const resp = await sp.callAPI({
          operation: 'getItemOffersBatch',
          endpoint: 'productPricing',
          body: {
            requests: chunk.map(asin => ({
              uri: `/products/pricing/v0/items/${asin}/offers`,
              method: 'GET',
              MarketplaceId: MARKETPLACE_ID,
              ItemCondition: 'New',
              CustomerType: 'Consumer',
            })),
          },
          options: { version: 'v0' },
        });
        const ts = new Date().toISOString();
        const responses = resp.responses || [];
        responses.forEach((r, idx) => {
          // 応答順はリクエスト順と保証されないため request.uri から ASIN を復元 (index は fallback)
          const uriMatch = String(r?.request?.uri || '').match(/items\/([A-Z0-9]{10})\/offers/i);
          const asin = uriMatch ? uriMatch[1].toUpperCase() : chunk[idx];
          const offers = r?.body?.payload?.Offers || [];
          const bb = offers.find(o => o.IsBuyBoxWinner) || null;
          const norm = {
            asin,
            buyBoxSellerId: bb ? (bb.SellerId || null) : null,
            isAmazon: bb ? bb.SellerId === AMAZON_JP_SELLER_ID : null,
            isFBA: bb ? !!bb.IsFulfilledByAmazon : null,
            offerCount: offers.length,
          };
          result[asin] = norm;
          cachePut.run(asin, JSON.stringify(norm), ts);
        });
      } catch (e) {
        console.error('[aba-ext] offers取得失敗:', e.message);
        for (const asin of chunk) if (!result[asin]) result[asin] = { asin, error: true };
      }
    }
  }
  res.json({ items: result });
});

// ============================================================
// GET /status — 接続テスト・取込状況
// ============================================================
router.get('/status', (req, res) => {
  const db = getAbaDB();
  const weeks = db.prepare(
    'SELECT week_start, week_end, term_count, row_count, ingested_at FROM aba_weeks ORDER BY week_start DESC LIMIT 8'
  ).all();
  const watch = db.prepare('SELECT COUNT(*) AS c FROM aba_watch_asins').get();
  res.json({ ok: true, weeks, watchAsins: watch.c, offersEnabled: process.env.ABA_EXT_OFFERS === '1' });
});

export default router;
