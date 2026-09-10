/**
 * 粗利分析ダッシュボード — ルーター
 *
 * 4ビュー:
 *   1. 粗利ワースト商品ランキング
 *   2. 粗利率ボーダー帯商品
 *   3. 前月比悪化商品
 *   4. モール別粗利比較
 *
 * 管理会計の近似値。現行原価・現行料率ベースの管理指標。
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { jstYearMonth, addMonthsYm, lastDayOfMonthStr } from '../../lib/jst-date.js';
import { loadDimMall } from '../../lib/dim-mall.js';
import inventoryDecisionRouter from './inventory-decision.js';
import { queryPublished, csvCell } from '../expected-profit/query.js';
import { getExpectedProfitDB } from '../expected-profit/db.js';
import {
  normalizeAllowanceInput, upsertAllowance, revokeAllowance, STATE_LABEL,
} from '../expected-profit/allowance.js';

const router = Router();

// タブB (在庫整理・撤退判断支援) API
// feature flag INVENTORY_DECISION_ENABLED でのみ有効化。Dark Launch 段階では OFF 想定。
router.use('/api/inventory', inventoryDecisionRouter);

// ─── モール別手数料率 (監査PR-11: ハードコード→dim_mall.fee_rate_approx に集約。値は従来と同一) ───

// ─── 消費税率 → 税込換算の係数 ───
// 🚨 mirror_products.消費税率 は「小数」で入っている (0.1 = 10%, 0.08 = 8%)。
//    NE が返す整数 (10 / 8) は rebuild-m-products.js の TAX_RATES で小数へ変換され、
//    mirror へは変換せずそのまま送られる (sync-to-render.js は SELECT p.*)。
//    2026-09-07 まで `1 + (消費税率 || 10) / 100` と書いていたため 1 + 0.1/100 = 1.001 にしかならず、
//    税込原価がほぼ税抜のままだった (= 粗利が原価の約10%分 過大に出ていた)。
//    整数を渡してはいけないので、整数が来たら未知の単位として fallback する。
export const TAX_RATE_FALLBACK = 0.1;
export function taxMultiplier(rate) {
  // 数値でない (null / undefined / NaN / 文字列) と、想定外の単位 (1 以上 = 整数表記の疑い)、
  // 0 以下 (NE 未登録) は fallback。復元ではなく「異常値の代替」なので、
  // 整数 8 も 10% に倒れる (将来 mirror が整数表記へ変わったら、ここではなく単位側を直す)
  if (!Number.isFinite(rate) || rate <= 0 || rate >= 1) return 1 + TAX_RATE_FALLBACK;
  return 1 + rate;
}

/**
 * セット構成品の税率を1つに解決する (Amazon SKU→NE 展開で使う)。
 *
 * 税率混在・構成品の欠損は「正常値っぽい粗利を出さない」ため hard fail にする。
 * 🚨 テストと本番が同じ関数を通るように、ここから export している
 *    (テスト側でロジックを書き写すと、本番のガードを消しても PASS してしまう — Codex R2)
 *
 * @param {Array<number|null>} rates 構成品の消費税率 (mirror_products.消費税率 の生値)
 * @param {boolean} allFound 全構成品が商品マスタで見つかったか
 * @returns {{ok: true, multiplier: number} | {ok: false, reason: 'missing_component'|'mixed_tax_rate'}}
 */
export function resolveSetTax(rates, allFound) {
  if (!allFound) return { ok: false, reason: 'missing_component' };
  // NE 未登録 (null/0) は 10% 扱いに寄せてから混在を判定する。
  // ここで寄せないと「税率が分かる構成品」と混在扱いになり、hard fail が増える
  const unique = [...new Set(rates.map(r => r || TAX_RATE_FALLBACK))];
  if (unique.length > 1) return { ok: false, reason: 'mixed_tax_rate' };
  return { ok: true, multiplier: taxMultiplier(unique[0]) };
}

// ─── メイン画面 ───
router.get('/', (req, res) => {
  // Codex PR3 R1 High 1 反映: feature flag OFF 時は EJS レンダリング時点でタブBを出さない
  const featureFlagRaw = process.env.INVENTORY_DECISION_ENABLED;
  const featureFlagEnabled = featureFlagRaw === 'true' || featureFlagRaw === '1';
  res.render('profit-analysis', {
    title: '商品収益性ダッシュボード',
    username: req.session?.email,
    displayName: req.session?.displayName,
    featureFlagEnabled,
  });
});

// ─── API: 粗利データ取得 ───

/**
 * 粗利計算エンジン
 * mirror_sales_monthly(by_listing) + mirror_products + mirror_amazon_sku_fees を結合
 */
function calculateProfitData(db, { days = 30, mall = null } = {}) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // 1. 日次売上（by_listing: モール商品コード粒度で売上金額あり）
  let salesSql = `
    SELECT 商品コード as listing_code, モール, チャネル,
      SUM(数量) as 数量, SUM(売上金額) as 売上金額
    FROM mirror_sales_daily
    WHERE データ種別 = 'by_listing' AND 日付 >= ?
  `;
  const params = [cutoffStr];
  if (mall) { salesSql += ' AND モール = ?'; params.push(mall); }
  salesSql += ' GROUP BY 商品コード, モール, チャネル HAVING SUM(数量) > 0';

  const sales = db.prepare(salesSql).all(...params);

  // 2. NE商品コード粒度の売上（原価計算用、セット展開済み）
  let prodSalesSql = `
    SELECT 商品コード, モール, SUM(数量) as 数量
    FROM mirror_sales_daily
    WHERE データ種別 = 'by_product' AND 日付 >= ?
  `;
  const prodParams = [cutoffStr];
  if (mall) { prodSalesSql += ' AND モール = ?'; prodParams.push(mall); }
  prodSalesSql += ' GROUP BY 商品コード, モール';
  const prodSales = db.prepare(prodSalesSql).all(...prodParams);

  // 3. 商品マスタ（原価 + 送料）
  const products = db.prepare(`
    SELECT 商品コード, 商品名, 原価, 原価ソース, 原価状態, 標準売価, 消費税率, 売上分類, 送料
    FROM mirror_products
  `).all();
  const productMap = new Map();
  for (const p of products) {
    // f_sales_by_listingはLOWER()で格納されるのでキーも小文字化
    productMap.set(p.商品コード?.toLowerCase(), p);
  }

  // 4. SKUマップ（seller_skuもne_codeも小文字で統一、mirror_sku_resolved master only）
  const skuMap = db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
  const skuToNeMap = new Map();
  for (const m of skuMap) {
    const key = m.seller_sku?.toLowerCase();
    if (!skuToNeMap.has(key)) skuToNeMap.set(key, []);
    // 数量検証: NULL→1扱い、0/負数/非整数→null (invalid、計算除外)
    const rawQty = m.数量;
    const validQty = (rawQty == null) ? 1
      : (Number.isInteger(rawQty) && rawQty > 0) ? rawQty : null;
    skuToNeMap.get(key).push({ ne_code: m.ne_code?.toLowerCase(), qty: validQty, rawQty });
  }

  // 4b. 楽天SKUマップ（rakuten_code → ne_code、AM/AL/W 3段階フォールバック済み）
  const rakutenSkuMap = new Map();
  try {
    for (const m of db.prepare('SELECT rakuten_code, ne_code FROM mirror_rakuten_sku_map').all()) {
      rakutenSkuMap.set(m.rakuten_code?.toLowerCase(), m.ne_code?.toLowerCase());
    }
  } catch { /* テーブル未作成の場合はスキップ */ }

  // 5. Amazon手数料キャッシュ
  let feeMap = new Map();
  try {
    const fees = db.prepare('SELECT * FROM mirror_amazon_sku_fees').all();
    for (const f of fees) {
      feeMap.set(f.seller_sku?.toLowerCase(), f);
    }
  } catch { /* テーブルがまだない場合 */ }

  // 6. 粗利計算
  const results = [];

  for (const s of sales) {
    const mallId = s.モール;
    const listingCode = s.listing_code;
    const channel = s.チャネル || '';
    const revenue = s.売上金額 || 0;
    const qty = s.数量 || 0;

    if (revenue <= 0 || qty <= 0) continue;

    // 原価計算: 原価(税抜) × 税率 = 原価(税込)
    // 送料: Amazon FBA以外は送料を加算
    let costExTax = 0; // 原価(税抜)
    let taxRate = 1.1;  // デフォルト10%
    let shipping = 0;
    let costSource = '不明';
    let productName = listingCode;

    if (mallId === 'amazon') {
      // SKUマップでNE商品コードを取得 → 構成品原価合計
      const neEntries = skuToNeMap.get(listingCode);
      if (neEntries) {
        // 数量invalid な構成品があれば hard fail (cost不確定として警告)
        const hasInvalidQty = neEntries.some(e => e.qty === null);
        if (hasInvalidQty) {
          costExTax = 0;
          costSource = 'SKU→NE(数量不正・原価不確定)';
        } else {
          let totalCost = 0;
          let totalShip = 0;
          let allFound = true;
          const taxRates = [];
          for (const entry of neEntries) {
            const prod = productMap.get(entry.ne_code);
            if (prod) {
              if (prod.原価) totalCost += prod.原価 * entry.qty;
              // 生値のまま集める (fallback と混在判定は resolveSetTax が行う)
              taxRates.push(prod.消費税率);
              if (prod.送料) totalShip += prod.送料 * entry.qty;
              if (productName === listingCode && prod.商品名) productName = prod.商品名;
            } else {
              allFound = false;
            }
          }
          // 税率混在/部分欠損は hard fail (Codex指摘: 正常値っぽい粗利を出さない)
          const setTax = resolveSetTax(taxRates, allFound);
          if (!setTax.ok) {
            costExTax = 0;
            taxRate = 1 + TAX_RATE_FALLBACK;
            costSource = setTax.reason === 'missing_component'
              ? 'SKU→NE(部分欠損・原価不確定)'
              : 'SKU→NE(税率混在・原価不確定)';
          } else {
            costExTax = totalCost * qty;
            // FBMは商品マスタの送料、FBAは後でfba_feeを送料欄に入れる
            if (channel !== 'FBA') {
              shipping = totalShip * qty;
            }
            taxRate = setTax.multiplier;
            costSource = 'SKU→NE';
          }
        }
      }
    } else {
      // 非Amazon: listingCode = NE商品コード（楽天/Yahoo/auPAY等）
      let prod = productMap.get(listingCode);

      // 楽天のフォールバック: 直接マッチしなければ rakuten_sku_map で解決
      if (!prod && mallId === 'rakuten' && rakutenSkuMap.has(listingCode)) {
        const resolvedNeCode = rakutenSkuMap.get(listingCode);
        prod = productMap.get(resolvedNeCode);
        if (prod) costSource = 'rakuten_sku_map';
      }

      if (prod) {
        costExTax = (prod.原価 || 0) * qty;
        taxRate = taxMultiplier(prod.消費税率);
        shipping = (prod.送料 || 0) * qty;
        productName = prod.商品名 || listingCode;
        if (costSource === '不明') costSource = prod.原価ソース || 'NE';
      }
    }

    // 原価(税込) = 原価(税抜) × 税率
    const cost = costExTax * taxRate;

    // 手数料計算
    let platformFee = 0;
    let fbaFee = 0;

    if (mallId === 'amazon') {
      const feeData = feeMap.get(listingCode);
      if (feeData) {
        platformFee = (feeData.referral_fee || 0) * qty;
        // FBA: 配送代行手数料を送料欄に入れる
        if (channel === 'FBA') {
          shipping = (feeData.fba_fee || 0) * qty;
        }
      } else {
        platformFee = revenue * 0.15;
      }
    } else {
      const rate = loadDimMall(db).feeRateOf(mallId, 0.10);
      platformFee = revenue * rate;
    }

    // 粗利 = 売価 - PF手数料 - 送料(FBA配送代行 or 自社送料) - 原価(税込)
    const grossProfit = revenue - platformFee - shipping - cost;
    const grossMarginRate = revenue > 0 ? (grossProfit / revenue * 100) : 0;

    results.push({
      listing_code: listingCode,
      product_name: productName,
      mall: mallId,
      channel,
      qty,
      revenue: Math.round(revenue),
      cost: Math.round(cost),
      shipping: Math.round(shipping),
      platform_fee: Math.round(platformFee),
      gross_profit: Math.round(grossProfit),
      margin_rate: Math.round(grossMarginRate * 10) / 10,
      cost_source: costSource,
      has_fee_cache: mallId === 'amazon' ? feeMap.has(listingCode) : null,
    });
  }

  return results;
}

/**
 * 前月比計算用: 指定期間の粗利を月単位で集計
 */
function calculateMonthlyProfit(db, { months = 3, mall = null } = {}) {
  const results = {};

  // 監査M-2: toISOString月キーはUTC基準でJST月初9時間ずれる + setMonth(-i)は月末日発火で
  // 月スキップの古典バグがあるため、年月演算はaddMonthsYm(文字列演算)に置換
  const currentYm = jstYearMonth();
  for (let i = 0; i < months; i++) {
    const yearMonth = addMonthsYm(currentYm, -i);

    // その月の初日〜末日 (旧: new Date(y,m+1,0).toISOString() はJST実行で月末日の前日に化ける)
    const firstDay = `${yearMonth}-01`;
    const lastDay = lastDayOfMonthStr(yearMonth);

    let salesSql = `
      SELECT 商品コード as listing_code, モール, チャネル,
        SUM(数量) as 数量, SUM(売上金額) as 売上金額
      FROM mirror_sales_daily
      WHERE データ種別 = 'by_listing' AND 日付 >= ? AND 日付 <= ?
    `;
    const params = [firstDay, lastDay];
    if (mall) { salesSql += ' AND モール = ?'; params.push(mall); }
    salesSql += ' GROUP BY 商品コード, モール, チャネル HAVING SUM(数量) > 0';

    results[yearMonth] = db.prepare(salesSql).all(...params);
  }

  return results;
}

// ─── API エンドポイント ───

// 粗利ワースト / ボーダー帯 / モール別比較
router.get('/api/profit', (req, res) => {
  try {
    const db = getMirrorDB();
    const days = parseInt(req.query.days) || 30;
    const mall = req.query.mall || null;

    const data = calculateProfitData(db, { days, mall });

    // ソート: 粗利率昇順（ワースト順）
    data.sort((a, b) => a.margin_rate - b.margin_rate);

    // 集計サマリー
    const totalRevenue = data.reduce((s, d) => s + d.revenue, 0);
    const totalCost = data.reduce((s, d) => s + d.cost, 0);
    const totalShipping = data.reduce((s, d) => s + d.shipping, 0);
    const totalFee = data.reduce((s, d) => s + d.platform_fee, 0);
    const totalProfit = data.reduce((s, d) => s + d.gross_profit, 0);

    res.json({
      items: data,
      summary: {
        count: data.length,
        total_revenue: totalRevenue,
        total_cost: totalCost,
        total_shipping: totalShipping,
        total_fee: totalFee,
        total_profit: totalProfit,
        avg_margin_rate: totalRevenue > 0 ? Math.round(totalProfit / totalRevenue * 1000) / 10 : 0,
      },
      meta: { days, mall, generated_at: new Date().toISOString() },
    });
  } catch (e) {
    console.error('[ProfitAnalysis] Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// 前月比悪化商品
router.get('/api/profit/trend', (req, res) => {
  try {
    const db = getMirrorDB();
    const mall = req.query.mall || null;

    // 当月 vs 前月 の日次データから粗利を比較
    // 監査M-2: 旧toISOString月キーはRender(UTC)で毎月月初9時間、前月比較が1ヶ月ずれていた
    const thisMonth = jstYearMonth();
    const lastMonth = addMonthsYm(thisMonth, -1);

    const thisMonthData = calculateProfitData(db, { days: 30, mall });
    const lastMonthData = calculateProfitData(db, { days: 60, mall });

    // 先月分だけフィルタ（60日データから30日以上前のもの）
    const cutoff30 = new Date();
    cutoff30.setDate(cutoff30.getDate() - 30);

    // 商品×モールでグルーピング
    const thisMap = new Map();
    for (const d of thisMonthData) {
      const key = `${d.listing_code}__${d.mall}`;
      thisMap.set(key, d);
    }

    const lastMap = new Map();
    for (const d of lastMonthData) {
      const key = `${d.listing_code}__${d.mall}`;
      if (!thisMap.has(key)) {
        lastMap.set(key, d);
      }
    }

    // 悪化判定: 粗利額が前月比 -20% 以上減少
    const deteriorated = [];
    for (const [key, current] of thisMap) {
      const prev = lastMap.get(key);
      if (!prev) continue;
      if (prev.gross_profit <= 0) continue;

      const change = (current.gross_profit - prev.gross_profit) / Math.abs(prev.gross_profit) * 100;
      if (change <= -20) {
        deteriorated.push({
          ...current,
          prev_profit: prev.gross_profit,
          prev_margin_rate: prev.margin_rate,
          profit_change_pct: Math.round(change * 10) / 10,
        });
      }
    }

    deteriorated.sort((a, b) => a.profit_change_pct - b.profit_change_pct);

    res.json({
      items: deteriorated,
      meta: { this_month: thisMonth, last_month: lastMonth, mall },
    });
  } catch (e) {
    console.error('[ProfitAnalysis] Trend Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Amazon手数料キャッシュ状態
// ─── 想定利益 (単品販売シナリオ) ───
// 🚨 実績を使わない別系統。夜間に作った世代を読むだけで、ここでは計算しない。
//    正本 = AI_reference『商品別想定利益_要件定義_20260907.md』
/**
 * 「呼び方が間違っている」ことを 400 で返すための印。
 * 🚨 500 と混ぜない。画面のバグと本番障害を見分けられなくなる
 */
function badRequest(message) {
  const e = new Error(message);
  e.status = 400;
  return e;
}

/** 400 で返す誤りか。queryPublished が投げる「◯◯ が不正です」もここに含める */
function isBadRequest(e) {
  return e.status === 400 || /(state|絞り込み) が不正/.test(e.message);
}

/**
 * 一覧と CSV で **必ず同じ条件**を使うための組み立て (Codex R1)。
 * 🚨 2 か所に書き写すと、片方に絞り込みを足し忘れたときに、画面で絞ってから出した CSV に
 *    絞る前の行が入る。それを「絞り込んだ結果」として配ってしまうのがいちばん怖い。
 *    だから両方の入口がこの 1 つの関数を通る
 * 🚨 値の妥当性はここで見ない。queryPublished が知らない名前を投げる (黙って全件通さない)
 */
export function expectedProfitFilters(q = {}) {
  /**
   * 🚨 文字列でない値を **undefined に落とさない** (Codex R2)。`?stock=a&stock=b` は
   *    配列で届く。落とすと「在庫で絞ったつもりの CSV」に絞る前の行が入り、
   *    しかも 200 で返るので誰も気づかない。**外で弾かれない方向の誤りは人に返す**
   */
  const str = (name, v) => {
    if (v == null || v === '') return undefined;          // 未指定と「全部」は絞り込まない
    if (typeof v !== 'string') throw badRequest(`${name} が不正です: 値は 1 つだけ指定してください`);
    return v;
  };
  return {
    mall: str('mall', q.mall),
    expenseScope: str('scope', q.scope),
    state: str('state', q.state),
    // 在庫・取扱区分 (2026-09-10)。計算には入らない、一覧を絞るだけ
    handling: str('handling', q.handling),
    stock: str('stock', q.stock),
    rankOnly: q.rank_only !== '0',
    sort: q.sort === 'profit' ? 'profit' : 'margin',
    order: q.order === 'asc' ? 'asc' : 'desc',
  };
}

router.get('/api/expected-profit', (req, res) => {
  try {
    const r = queryPublished({
      ...expectedProfitFilters(req.query),
      fulfillment: req.query.fulfillment || undefined,
      salesClass: req.query.sales_class ? Number(req.query.sales_class) : undefined,
      // 件数だけ欲しいとき (選んでいない側の出荷区分) は並び替えも一覧もいらない
      countOnly: req.query.count_only === '1',
      limit: Math.min(Number(req.query.limit) || 500, 5000),
      offset: Number(req.query.offset) || 0,
    });
    res.json({ ok: true, ...r });
  } catch (e) {
    // 呼び方が不正なだけで 500 を返すと、画面のバグと本番障害の区別がつかない
    res.status(isBadRequest(e) ? 400 : 500).json({ ok: false, error: e.message });
  }
});

// ─── 承知のうえの赤字 (許容記録) ───
// 🚨 「意図した赤字」を要対応から外すための記録。期限と 1 個あたりの損失上限が必須で、
//    どちらかを外れたら自動で要対応に戻る (allowance.js)。無期限は登録できない
/**
 * 操作した人。
 * 🚨 'admin' に落とさない。セッションが読めていないのに管理者の操作として
 *    履歴に残ると、あとから「誰が赤字を許したのか」を追えなくなる
 */
function requireActor(req, res) {
  const actor = req.session?.email;      // ログイン時に入る唯一の識別子 (server.js)
  if (!actor) {
    res.status(401).json({ ok: false, error: 'ログインし直してください (操作した人を記録できません)' });
    return null;
  }
  return actor;
}

router.post('/api/expected-profit/allowance', (req, res) => {
  try {
    const actor = requireActor(req, res);
    if (!actor) return;
    // 🚨 検証と保存で同じ「今」を使う。別々に取ると、日付が変わる瞬間に
    //    「開始日は今日」で通した記録が「まだ始まっていない」扱いになる
    const now = new Date();
    const { errors, value } = normalizeAllowanceInput(req.body || {}, now);
    if (errors.length) return res.status(400).json({ ok: false, error: errors.join(' / '), errors });
    const saved = upsertAllowance(getExpectedProfitDB(), value, actor, now);
    res.json({ ok: true, allowance: saved });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/expected-profit/allowance/revoke', (req, res) => {
  try {
    const b = req.body || {};
    const key = {
      mall: String(b.mall || ''), shop_id: String(b.shop_id || ''),
      mall_item_key: String(b.mall_item_key || ''),
      expense_scope_version: String(b.expense_scope_version || ''),
    };
    if (!key.mall || !key.shop_id || !key.mall_item_key || !key.expense_scope_version) {
      return res.status(400).json({ ok: false, error: '出品の指定が足りません' });
    }
    const actor = requireActor(req, res);
    if (!actor) return;
    const revoked = revokeAllowance(getExpectedProfitDB(), key, actor);
    if (!revoked) return res.status(404).json({ ok: false, error: '有効な許容記録がありません' });
    res.json({ ok: true, allowance: revoked });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

/** Easy Ship の状態を日本語にする。英語のまま CSV に出すと現場で読めない */
const EASYSHIP_STATUS_LABEL = {
  easyship: 'Easy Ship',
  not_registered: '自己配送とみなし (梱包サイズ未登録)',
  inactive: '自己配送とみなし (登録が無効)',
  size_unmapped: 'サイズ区分が読めない',
  not_asked: 'サイズを聞けていない',
  lookup_failed: 'サイズを照会できなかった',
};
// 🚨 状態を足して言葉を足し忘れると CSV に内部の英語が出る。試験が一覧と突き合わせる
export { EASYSHIP_STATUS_LABEL as EASYSHIP_STATUS_LABEL_FOR_TEST };

// CSV 出力。🚨 数式インジェクション対策は外部由来の文字列列にだけ適用する (§9.4)
export const EXPECTED_PROFIT_CSV_COLS = [
  ['出品コード', 'mall_item_key', true], ['商品名', 'product_name', true], ['モール', 'mall', true],
  ['出荷', 'fulfillment', true], ['NE品番', 'ne_code', true], ['紐づけ方', 'ne_code_source', true],
  // 🚨 計算には入らない材料 (直す順番を決めるため)。在庫は NE の自社倉庫ぶんで、FBA 倉庫は含まない
  ['取扱区分', 'handling_class', true], ['在庫数(自社)', 'stock_qty'], ['引当数', 'stock_allocated_qty'],
  ['出せる在庫', 'stock_free'],
  // 🚨 Amazon の自社出荷を Easy Ship 料金で計算したか (混ざっているので行ごとに出す)
  ['Amazonの配送', 'easyship_status_label', true], ['EasyShipサイズ', 'easyship_size_code', true],
  ['EasyShip宛先', 'easyship_region', true],
  ['売価(税抜)', 'price_ex_tax'], ['売価(税込)', 'price_incl_tax'], ['送料収入(税抜)', 'postage_revenue_ex_tax'],
  ['原価(税抜)', 'cost_ex_tax'], ['原価の出所', 'cost_method', true], ['単品何個ぶん', 'unit_quantity'],
  // 🚨 どの配送で計算したかは「使った区分の名前」まで出す。コードだけでは追えない
  ['使った配送区分', 'shipping_rate_name', true], ['送料区分コード', 'shipping_code', true],
  ['配送区分の大分類', 'shipping_rate_category', true],
  ['配送方法(NE登録)', 'shipping_method', true], ['配送パターン(モール)', 'shipping_group', true],
  ['送料(税抜)', 'shipping_fee_ex_tax'], ['出荷作業料', 'shipping_work_ex_tax'],
  ['梱包資材費', 'shipping_material_ex_tax'], ['人件費', 'shipping_labor_ex_tax'],
  ['配送関係費 合計', 'shipping_total_ex_tax'],
  ['FBA配送代行', 'fba_fee_ex_tax'],
  ['販売手数料', 'referral_fee_ex_tax'], ['成約料', 'closing_fee_ex_tax'], ['基本成約料', 'per_item_fee_ex_tax'],
  ['手数料率', 'fee_rate_display'], ['手数料 合計', 'fee_total_ex_tax'],
  ['想定利益', 'expected_profit'], ['想定利益率', 'expected_margin_rate'],
  ['費用範囲', 'expense_scope_version', true], ['計算状態', 'calculation_status', true],
  ['計算できない理由', 'incomplete_reason', true],
  ['ランキング対象', 'rank_eligible_now'], ['対象外の理由', 'rank_exclusion_reason_now', true],
  ['価格の状態', 'price_status', true], ['原価の状態', 'cost_status', true],
  ['手数料の状態', 'fee_status', true], ['配送マスタの状態', 'shipping_master_status', true],
  ['送料収入の状態', 'shipping_revenue_status', true], ['表示時に失効', 'expired_now'],
  // 🚨 画面の「4 つの山」と同じ状態を CSV にも出す。画面と CSV で件数が食い違わないように
  ['監視状態', 'monitor_state_label', true], ['今回はじめて赤字', 'is_newly_negative'],
  ['許容の理由', 'allowance_reason', true], ['許容の上限(円)', 'allowance_cap'],
  ['許容の期限', 'allowance_until', true], ['許容を決めた人', 'allowance_decided_by', true],
  ['世代', 'generation_id', true], ['計算日時', 'built_at', true],
];

/** CSV 用に、状態と許容記録を 1 行の平らな値に開く */
export function expectedProfitCsvRow(row) {
  return {
    ...row,
    monitor_state_label: STATE_LABEL[row.monitor_state] || row.monitor_state || '',
    // 🚨 どちらかが読めなければ空にする。0 と「分からない」を混ぜない
    stock_free: (Number.isInteger(row.stock_qty) && Number.isInteger(row.stock_allocated_qty))
      ? row.stock_qty - row.stock_allocated_qty : null,
    easyship_status_label: EASYSHIP_STATUS_LABEL[row.easyship_status] || row.easyship_status || '',
    allowance_reason: row.allowance ? row.allowance.reason_code : '',
    allowance_cap: row.allowance ? row.allowance.loss_cap_yen : null,
    allowance_until: row.allowance ? row.allowance.valid_until : '',
    allowance_decided_by: row.allowance ? row.allowance.decided_by : '',
  };
}

router.get('/api/expected-profit.csv', (req, res) => {
  try {
    const r = queryPublished({
      // 🚨 一覧と同じ関数を通す。書き写さない (足し忘れると絞る前の行が CSV に入る)
      ...expectedProfitFilters(req.query),
      limit: 100000,
    });
    const out = [EXPECTED_PROFIT_CSV_COLS.map(c => csvCell(c[0])).join(',')];
    for (const row of r.rows) {
      const flat = expectedProfitCsvRow(row);
      out.push(EXPECTED_PROFIT_CSV_COLS
        .map(([, key, isText]) => csvCell(flat[key], { isExternalText: !!isText })).join(','));
    }
    const BOM = '﻿';                       // Excel が UTF-8 と分かるように
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition',
      `attachment; filename="expected-profit-${new Date().toISOString().slice(0, 10)}.csv"`);
    res.send(BOM + out.join('\r\n'));
  } catch (e) {
    // 🚨 一覧と同じ返し方にする (Codex R2)。CSV だけ 500 だと、絞り込みの書き間違いが
    //    本番障害として上がってくる
    res.status(isBadRequest(e) ? 400 : 500).json({ ok: false, error: e.message });
  }
});

router.get('/api/fee-status', (req, res) => {
  try {
    const db = getMirrorDB();
    let total = 0, fba = 0, fbm = 0, oldest = null;
    try {
      total = db.prepare('SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees').get().cnt;
      fba = db.prepare("SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees WHERE fulfillment_channel = 'FBA'").get().cnt;
      fbm = db.prepare("SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees WHERE fulfillment_channel = 'FBM'").get().cnt;
      oldest = db.prepare('SELECT MIN(fetched_at) as oldest FROM mirror_amazon_sku_fees').get().oldest;
    } catch { /* テーブル未作成 */ }

    res.json({ total, fba, fbm, oldest_fetch: oldest });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
