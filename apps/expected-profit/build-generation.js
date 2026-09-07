/**
 * 商品別 想定利益 — 世代ビルダー
 *
 * 正本 = §5.2.1 / §7.4 / §7.5 / §10.2
 *
 * 🚨 Codex R3 の受入条件:
 *   - partial 時は前回の完全集合と UNION する (部分列挙で見つからない = 削除ではない)
 *   - 引き継いだ行の期限を延長しない (コピー日時や計算日時から延ばさない)
 *   - 世代は検証を通ってから公開する
 */
import { getExpectedProfitDB } from './db.js';
import { buildRow } from './build-row.js';
import { feeCacheKey } from './calc.js';
import { storedSellerId } from './refresh-fees.js';
import { newGenerationId, nowIso } from './util.js';
import { hashRows } from './generation-hash.js';
import { nextSeq } from './db.js';

const MALLS = ['amazon', 'rakuten'];   // PR-1 の対象 (§12)

/**
 * 世代に入れる出品を集める。
 *
 * 🚨 今夜が partial なら、前回の完全集合と UNION する (§5.2.1)。
 *    引き継いだ行は **元の valid_until のまま**にする。ここで延ばすと、
 *    連続失敗しても永久に「新鮮」なままになる (Codex R2-3)。
 */
export function mergeWithPreviousComplete(currentRows, previousRows, enumStatus) {
  if (enumStatus === 'ok') return { rows: currentRows, carriedOver: 0 };
  const byKey = new Map(currentRows.map(r => [`${r.shop_id}${r.mall_item_key}`, r]));
  let carriedOver = 0;
  for (const p of previousRows) {
    const k = `${p.shop_id}${p.mall_item_key}`;
    if (byKey.has(k)) continue;
    // 🚨 valid_until も fetched_at もそのまま引き継ぐ (延ばさない)
    byKey.set(k, { ...p, _carried_over: 1 });
    carriedOver++;
  }
  return { rows: [...byKey.values()], carriedOver };
}

/** 直近の run と、その出品行 */
export function loadLatestRun(db, mall) {
  const run = db.prepare(`
    SELECT * FROM price_fetch_run WHERE mall = ? ORDER BY started_at DESC, rowid DESC LIMIT 1
  `).get(mall);
  if (!run) return null;
  const rows = db.prepare('SELECT * FROM mall_price_snapshot WHERE run_id = ?').all(run.run_id);
  return { run, rows };
}

/** 直近で完全列挙できた run の行 (UNION の相手) */
export function loadLastCompleteRows(db, mall, excludeRunId) {
  const run = db.prepare(`
    SELECT run_id FROM price_fetch_run
    WHERE mall = ? AND listing_enum_status = 'ok' AND run_id <> ?
    ORDER BY started_at DESC, rowid DESC LIMIT 1
  `).get(mall, excludeRunId || '');
  if (!run) return [];
  return db.prepare('SELECT * FROM mall_price_snapshot WHERE run_id = ?').all(run.run_id);
}

function loadFeeEstimates(db) {
  const rows = db.prepare('SELECT * FROM amazon_fee_estimate').all();
  const map = new Map();
  // 🚨 キーは共通関数だけで作る (重複実装を残さない — Codex R5)
  for (const r of rows) map.set(feeCacheKey(r), r);
  return map;
}

/**
 * 世代を作る。公開はしない (validate → publish は別ステップ)。
 *
 * @param {object} db expected-profit.db
 * @param {object} deps { warehouseInputs, now, codeVersion, sellerId, marketplaceId }
 */
export function buildGeneration(db, deps = {}) {
  const now = deps.now || new Date();
  const generationId = newGenerationId();
  const seq = nextSeq(db);
  const {
    products, shippingRates, skuMaps, masterFreshness,
  } = deps.warehouseInputs;

  const allRows = [];
  const mallsIncluded = [];
  const mallsDegraded = [];
  const perMall = {};

  for (const mall of (deps.malls || MALLS)) {
    const latest = loadLatestRun(db, mall);
    if (!latest) { mallsDegraded.push({ mall, reason: 'no_run' }); continue; }
    const enumStatus = latest.run.listing_enum_status;
    if (enumStatus === 'failed') {
      // 🚨 列挙が失敗した夜は、そのモールを世代に入れない (欠落した集合で上書きしない)
      mallsDegraded.push({ mall, reason: 'enum_failed' });
      continue;
    }
    const previous = enumStatus === 'ok' ? [] : loadLastCompleteRows(db, mall, latest.run.run_id);
    const merged = mergeWithPreviousComplete(latest.rows, previous, enumStatus);
    if (enumStatus !== 'ok') mallsDegraded.push({ mall, reason: enumStatus, carriedOver: merged.carriedOver });
    mallsIncluded.push(mall);

    const ctx = {
      generationId,
      now,
      codeVersion: deps.codeVersion || 'unknown',
      // 🚨 見積を引くキーに使う seller_id は、**DB に保存されている値**を使う。
      //    env と食い違うとキーが一致せず、見積を1件も引けない (実データで判明)
      // 🚨 見積を引くキーの seller_id は **覚え書き → env** の順 (Codex R7-3)。
      //    見積テーブルの DISTINCT から推測すると、旧セラーの行が残っただけで引けなくなる
      sellerId: storedSellerId(db, deps.sellerId),
      marketplaceId: deps.marketplaceId,
      products,
      shippingRates,
      skuMap: skuMaps[mall] || new Map(),
      feeEstimates: deps.feeEstimates || loadFeeEstimates(db),
      masterFreshness,
      runInfo: {
        listingEnumStatus: enumStatus === 'ok' ? 'ok' : 'partial',
        listingEnumValidUntil: latest.run.finished_at
          ? enumValidUntil(latest.run.finished_at) : null,
        priceRunId: latest.run.run_id,
      },
    };
    const rows = merged.rows.map(l => buildRow(l, ctx));
    perMall[mall] = {
      listings: merged.rows.length,
      carriedOver: merged.carriedOver,
      ok: rows.filter(r => r.calculation_status === 'ok').length,
      rankEligible: rows.filter(r => r.rank_eligible === 1).length,
    };
    allRows.push(...rows);
  }

  // 🚨 まとめ買いバリエーションの検出 (実データで判明・2026-09-07)
  //    楽天は「2個組・4個組・40個組」を別の商品管理番号で出しているが、
  //    対応表 (f_rakuten_sku_map) は数量を持たないので、全部が同じ NE 商品を指す。
  //    その結果、原価が1個ぶんのまま売価だけ跳ね上がり、利益率が現実離れする:
  //      売価 41,800 / 原価 750 → 86.7% (実際は40個ぶんの原価が要る)
  //      売価    798 / 原価 1,440 → -169% (実際は1個ぶんの原価でよい)
  //    件数は少ないが、**ランキングの最上位と最下位を占める**ので放置できない。
  //    数量が分からない以上、正しい利益は出せない → 参考値に落とす
  markQuantityVariationSuspects(allRows);

  const okCount = allRows.filter(r => r.calculation_status === 'ok').length;
  const rankCount = allRows.filter(r => r.rank_eligible === 1).length;
  // 🚨 ハッシュは送信側・受信側で同じ関数を通す (generation-hash.js)。
  //    ここで独自に並び替え・列選択をすると、正常な世代が公開できなくなる (Codex R5-1)
  const hash = hashRows(allRows);

  const insertGen = db.prepare(`
    INSERT INTO expected_profit_generation
      (generation_id, seq, built_at, local_status, remote_status, row_count, ok_count,
       incomplete_count, rank_eligible_count, content_hash, malls_included, malls_degraded)
    VALUES (?, ?, ?, 'building', 'not_sent', ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertRow = db.prepare(`
    INSERT INTO mart_listing_expected_profit
      (generation_id, mall, shop_id, mall_item_key, ne_code, product_name, sales_class, fulfillment,
       listing_status, price_incl_tax, price_ex_tax, postage_revenue_ex_tax, revenue_ex_tax, tax_rate,
       cost_ex_tax, cost_method, unit_quantity, shipping_code, shipping_method, shipping_fee_ex_tax, shipping_work_ex_tax,
       shipping_material_ex_tax, shipping_labor_ex_tax, shipping_total_ex_tax, fba_fee_ex_tax,
       referral_fee_ex_tax, closing_fee_ex_tax, per_item_fee_ex_tax, fee_total_ex_tax, fee_rate_display,
       fee_breakdown, expected_profit, expected_margin_rate, listing_enum_status, listing_enum_valid_until,
       price_status, price_valid_until, fee_status, fee_valid_until, cost_status, cost_valid_until,
       shipping_master_status, shipping_master_valid_until, shipping_revenue_status, scenario_fit,
       calculation_status, incomplete_reason, rank_eligible, rank_exclusion_reason, expense_scope_version,
       input_snapshot, formula_version, scenario_version, fee_rate_version, code_version, price_run_id, built_at)
    VALUES
      (@generation_id, @mall, @shop_id, @mall_item_key, @ne_code, @product_name, @sales_class, @fulfillment,
       @listing_status, @price_incl_tax, @price_ex_tax, @postage_revenue_ex_tax, @revenue_ex_tax, @tax_rate,
       @cost_ex_tax, @cost_method, @unit_quantity, @shipping_code, @shipping_method, @shipping_fee_ex_tax, @shipping_work_ex_tax,
       @shipping_material_ex_tax, @shipping_labor_ex_tax, @shipping_total_ex_tax, @fba_fee_ex_tax,
       @referral_fee_ex_tax, @closing_fee_ex_tax, @per_item_fee_ex_tax, @fee_total_ex_tax, @fee_rate_display,
       @fee_breakdown, @expected_profit, @expected_margin_rate, @listing_enum_status, @listing_enum_valid_until,
       @price_status, @price_valid_until, @fee_status, @fee_valid_until, @cost_status, @cost_valid_until,
       @shipping_master_status, @shipping_master_valid_until, @shipping_revenue_status, @scenario_fit,
       @calculation_status, @incomplete_reason, @rank_eligible, @rank_exclusion_reason, @expense_scope_version,
       @input_snapshot, @formula_version, @scenario_version, @fee_rate_version, @code_version, @price_run_id, @built_at)
  `);

  const tx = db.transaction(() => {
    insertGen.run(generationId, seq, nowIso(), allRows.length, okCount,
      allRows.length - okCount, rankCount, hash,
      JSON.stringify(mallsIncluded), JSON.stringify(mallsDegraded));
    for (const r of allRows) insertRow.run(r);
  });
  tx();

  return {
    generationId, seq,
    rowCount: allRows.length, okCount, rankEligibleCount: rankCount,
    mallsIncluded, mallsDegraded, perMall, contentHash: hash,
  };
}

/**
 * 同じ NE 商品を指す出品どうしで価格が大きくひらいていたら、
 * 「数量違いのまとめ買いバリエーション」を疑ってランキングから外す。
 *
 * 🚨 同じ NE を複数出品が指すこと自体は正常 (色違いなど。Codex R2-8)。
 *    区別できるのは「価格の開き」だけなので、閾値で線を引く。
 *    実測 (2026-09-07 楽天 5,592出品): 複数出品を持つ NE 商品 11件のうち、
 *    1.5倍以上ひらくのは 7件。いずれもまとめ買いだった。
 */
export const PRICE_SPREAD_LIMIT = 1.5;

export function markQuantityVariationSuspects(rows, limit = PRICE_SPREAD_LIMIT) {
  const groups = new Map();
  for (const r of rows) {
    if (!r.ne_code || !Number.isFinite(r.price_incl_tax)) continue;
    // 🚨 数量が分かっている出品は対象外 (Codex R7-2)。原価は既に数量倍してあるので、
    //    価格がひらいていても正しい。ここで外すと「数量2以上の FBA は載せる」に反する
    if (r.unit_quantity != null) continue;
    const key = `${r.mall}${r.ne_code}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(r);
  }
  let marked = 0;
  for (const group of groups.values()) {
    if (group.length < 2) continue;
    const prices = group.map(r => r.price_incl_tax).filter(p => p > 0);
    if (prices.length < 2) continue;
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    // 「1.5倍以上ひらいたら疑う」なので、ちょうど 1.5 倍も対象に含める (Codex R7-5)
    if (max < min * limit) continue;       // 色違いなど、価格が近いものは正常
    for (const r of group) {
      // 計算結果は残す (参考値として見える) が、ランキングには載せない
      r.rank_eligible = 0;
      r.rank_exclusion_reason = r.rank_exclusion_reason || 'quantity_variation_suspected';
      marked++;
    }
  }
  return marked;
}

/** 出品列挙の期限 = run の finished_at + 7日 (§15-8) */
export function enumValidUntil(finishedAt) {
  const d = new Date(String(finishedAt).replace(' ', 'T'));
  if (!Number.isFinite(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + 7);
  return d.toISOString();
}

// ────────────────────────────────────────────────────────────
// 公開前検証 (§10.2)
// ────────────────────────────────────────────────────────────

/**
 * 世代を公開してよいか。
 * 🚨 「入力不足による incomplete (正常)」と「世代全体を拒否すべき構造異常」を分ける。
 *    計算済み行だけ正しければ通る検証にしない
 */
export function validateGeneration(db, generationId, opts = {}) {
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(generationId);
  if (!gen) return { ok: false, errors: ['generation_not_found'] };
  const rows = db.prepare('SELECT * FROM mart_listing_expected_profit WHERE generation_id = ?').all(generationId);
  const errors = [];
  const warnings = [];

  // 1. 件数が manifest と合うか
  if (rows.length !== gen.row_count) errors.push(`row_count 不一致: ${rows.length} vs ${gen.row_count}`);
  // 2. 0行は公開しない
  if (rows.length === 0) errors.push('行が0件');
  // 3. 重複キー
  const keys = new Set(rows.map(r => `${r.mall}${r.shop_id}${r.mall_item_key}`));
  if (keys.size !== rows.length) errors.push(`重複キー ${rows.length - keys.size} 件`);
  // 4. 有限値 (NaN / Infinity を公開しない)
  const nonFinite = rows.filter(r => r.expected_profit != null && !Number.isFinite(r.expected_profit));
  if (nonFinite.length > 0) errors.push(`利益が有限値でない行 ${nonFinite.length} 件`);
  // 5. 費用内訳の照合 (ok の行だけ。誤差は円未満)
  for (const r of rows.filter(x => x.calculation_status === 'ok').slice(0, 100000)) {
    const expect = r.revenue_ex_tax - r.cost_ex_tax - r.shipping_total_ex_tax - r.fba_fee_ex_tax - r.fee_total_ex_tax;
    if (Math.abs(expect - r.expected_profit) > 1e-6) {
      errors.push(`内訳と利益が合わない: ${r.mall}/${r.mall_item_key}`);
      break;
    }
  }
  // 6. 集計と実際の行が食い違っていないか (これは構造異常)
  const okRows = rows.filter(r => r.calculation_status === 'ok');
  const actualRank = rows.filter(r => r.rank_eligible === 1).length;
  if (gen.rank_eligible_count != null && actualRank !== gen.rank_eligible_count) {
    errors.push(`ランキング対象の集計が行と合わない: 集計 ${gen.rank_eligible_count} / 実際 ${actualRank}`);
  }
  if (gen.ok_count != null && okRows.length !== gen.ok_count) {
    errors.push(`計算できた件数の集計が行と合わない: 集計 ${gen.ok_count} / 実際 ${okRows.length}`);
  }
  // 🚨 ランキング0件そのものは構造異常ではない (Codex R6-1)。
  //    列挙が partial の夜、全出品が Inactive の夜は、正しく0件になる。
  //    「理由が説明できない0件」だけを異常として扱う
  if (okRows.length > 0 && actualRank === 0) {
    const unexplained = okRows.filter(r => !r.rank_exclusion_reason);
    if (unexplained.length > 0) {
      errors.push(`ランキング対象が0件で、理由が付いていない行が ${unexplained.length} 件ある (適格判定の取り違えの疑い)`);
    } else {
      warnings.push(`ランキング対象が0件 (理由: ${[...new Set(okRows.map(r => r.rank_exclusion_reason))].slice(0, 5).join(', ')})`);
    }
  }
  // 7. 全モールが degraded なら公開しない
  const included = JSON.parse(gen.malls_included || '[]');
  if (included.length === 0) errors.push('取り込めたモールが0');

  // 警告 (公開は止めない)
  const okRatio = rows.length > 0 ? okRows.length / rows.length : 0;
  if (okRatio < (opts.minOkRatio ?? 0.30)) {
    warnings.push(`計算できた割合が低い: ${(okRatio * 100).toFixed(1)}%`);
  }

  const ok = errors.length === 0;
  db.prepare('UPDATE expected_profit_generation SET local_status = ?, validation_note = ? WHERE generation_id = ?')
    .run(ok ? 'validated' : 'rejected', JSON.stringify({ errors, warnings }), generationId);
  return { ok, errors, warnings };
}
