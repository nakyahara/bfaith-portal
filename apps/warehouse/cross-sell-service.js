/**
 * 同梱商品検索 サービスAPI
 * /service-api/cross-sell/* にマウント
 *
 * Render側からの呼び出し専用 (serviceAuth による Bearer トークン認証)。
 * /apps/warehouse/* はセッション認証が必要でサーバ間通信できないため、
 * このルートを Render→ミニPC のサーバ間通信窓口として用意する。
 */
import { Router } from 'express';
import { getDB } from './db.js';
import { okResponse, errorResponse } from './error-handler.js';

const router = Router();

// JST 基準で N 日前の YYYY-MM-DD を返す。
// toISOString は UTC なので、JST 早朝に走らせると 1 日前の日付になりカットオフがズレる。
function jstDateString(daysAgo) {
  const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
  const t = Date.now() + JST_OFFSET_MS - daysAgo * 24 * 60 * 60 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

// 商品コード/商品名のサジェスト (UI のオートコンプリート用)
router.get('/suggest', (req, res) => {
  const q = (req.query.q || '').trim();
  if (q.length < 2) {
    return okResponse(res, { result: [] });
  }
  const term = `%${q}%`;
  try {
    const db = getDB();
    const rows = db.prepare(
      `SELECT 商品コード, 商品名, 取扱区分
       FROM raw_ne_products
       WHERE 商品コード LIKE ? OR 商品名 LIKE ?
       ORDER BY (取扱区分 = '取扱中') DESC, 商品コード
       LIMIT 15`
    ).all(term, term);
    okResponse(res, { result: rows });
  } catch (e) {
    return errorResponse(res, {
      status: 500,
      error: 'DB_ERROR',
      message: e.message,
      requestId: req.requestId,
    });
  }
});

router.get('/search', (req, res) => {
  const code = (req.query.code || '').trim();
  const days = Math.max(1, Math.min(parseInt(req.query.days) || 90, 365));

  if (!code) {
    return errorResponse(res, {
      status: 400,
      error: 'VALIDATION_ERROR',
      message: 'code パラメータが必要です',
      requestId: req.requestId,
    });
  }

  const fromStr = jstDateString(days);

  // (商品コード × platform) 単位の中間集計を全件取る。LIMIT は掛けない。
  // 掛けると後段の global 再集計でランキングが欠落する。
  const sql = `
    WITH target_orders AS (
      SELECT DISTINCT 伝票番号
      FROM raw_ne_orders
      WHERE 商品コード = ?
        AND 受注日 >= ?
        AND キャンセル区分 = '有効'
    )
    SELECT
      o.商品コード,
      COALESCE(p.商品名, '') AS 商品名,
      COALESCE(s.platform, '') AS platform,
      COUNT(DISTINCT o.伝票番号) AS 同梱伝票数,
      SUM(o.受注数) AS 累計数量
    FROM raw_ne_orders o
    JOIN target_orders t ON o.伝票番号 = t.伝票番号
    LEFT JOIN shops s ON o.店舗コード = s.shop_code
    LEFT JOIN raw_ne_products p ON o.商品コード = p.商品コード
    WHERE o.商品コード != ?
      AND o.キャンセル区分 = '有効'
      AND o.受注日 >= ?
      AND COALESCE(s.platform, '') NOT IN ('_ignore', 'amazon_fbm')
      AND NOT EXISTS (
        SELECT 1
        FROM raw_ne_set_products c1
        JOIN raw_ne_set_products c2 ON c1.セット商品コード = c2.セット商品コード
        WHERE c1.商品コード = ?
          AND c2.商品コード = o.商品コード
      )
    GROUP BY o.商品コード, COALESCE(s.platform, '')
  `;

  let rows;
  try {
    const db = getDB();
    db.pragma('busy_timeout = 5000');
    rows = db.prepare(sql).all(code, fromStr, code, fromStr, code);
  } catch (e) {
    return errorResponse(res, {
      status: 500,
      error: 'DB_ERROR',
      message: e.message,
      requestId: req.requestId,
    });
  }

  const sortRanking = (a, b) =>
    b.同梱伝票数 - a.同梱伝票数
    || b.累計数量 - a.累計数量
    || a.商品コード.localeCompare(b.商品コード);

  // 全モール横断: 商品コードで集約
  const globalMap = new Map();
  // platform 別の対象伝票数 (検索商品が含まれる platform 別の伝票数)。
  // - 全体ランキングの分母: SUM(プラットフォーム別)
  // - モール別ランキングの分母: その platform の値
  // 1伝票=1店舗=1 platform なので、SUM で重複なく合算できる。
  const db = getDB();
  const platformTargets = db.prepare(
    `SELECT COALESCE(s.platform, '') AS platform, COUNT(DISTINCT o.伝票番号) AS cnt
     FROM raw_ne_orders o
     LEFT JOIN shops s ON o.店舗コード = s.shop_code
     WHERE o.商品コード = ?
       AND o.受注日 >= ?
       AND o.キャンセル区分 = '有効'
       AND COALESCE(s.platform, '') NOT IN ('_ignore', 'amazon_fbm')
     GROUP BY COALESCE(s.platform, '')`
  ).all(code, fromStr);

  const platformTargetMap = {};
  let totalTarget = 0;
  for (const r of platformTargets) {
    platformTargetMap[r.platform || 'unknown'] = r.cnt;
    totalTarget += r.cnt;
  }

  const pct = (num, den) => den > 0 ? Math.round((num / den) * 1000) / 10 : 0;

  for (const r of rows) {
    const key = r.商品コード;
    if (!globalMap.has(key)) {
      globalMap.set(key, { 商品コード: key, 商品名: r.商品名, 同梱伝票数: 0, 累計数量: 0 });
    }
    const g = globalMap.get(key);
    g.同梱伝票数 += r.同梱伝票数;
    g.累計数量 += r.累計数量;
  }
  // 全体: 同梱率 = その商品の合算同梱伝票数 / 全モール対象伝票数
  const global = [...globalMap.values()]
    .map(g => ({ ...g, 同梱率: pct(g.同梱伝票数, totalTarget) }))
    .sort(sortRanking)
    .slice(0, 50);

  // モール別: platform ごとに TOP 30、同梱率はそのモール内の対象伝票数を分母に
  const platformMap = new Map();
  for (const r of rows) {
    const pf = r.platform || 'unknown';
    if (!platformMap.has(pf)) platformMap.set(pf, []);
    platformMap.get(pf).push(r);
  }
  const byPlatform = {};
  for (const [pf, list] of platformMap.entries()) {
    const den = platformTargetMap[pf] || 0;
    byPlatform[pf] = list
      .map(r => ({ ...r, 同梱率: pct(r.同梱伝票数, den) }))
      .sort(sortRanking)
      .slice(0, 30);
  }

  const productRow = db.prepare(
    'SELECT 商品名 FROM raw_ne_products WHERE 商品コード = ?'
  ).get(code);

  okResponse(res, {
    result: {
      code,
      商品名: productRow?.商品名 || null,
      period: { days, from: fromStr },
      対象伝票数: totalTarget,
      platform対象伝票数: platformTargetMap,
      global,
      byPlatform,
    },
  });
});

export default router;
