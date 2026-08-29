/**
 * monitor-fee-coverage.js — Amazon手数料キャッシュのカバー率監視 (SLO 監視)
 *
 * 「直近 N 日に売れた SKU のうち、amazon_sku_fees に十分新しいキャッシュがあるか」を測定し、
 * 件数ではなく **売上金額加重カバー率** を主指標として alert する。
 *
 * 設計判断 (Codex+Claude 議論結果、2026-05-15):
 *   - 件数カバー率だけだと「未カバー SKU が売上 TOP かどうか」が見えない (例: 件数93%でも金額95%超かも)
 *   - 売上金額加重カバー率 を warn/critical の主基準に
 *   - 未カバー TOP20 SKU を必ず表示 (粗利分析への実害が見える)
 *   - 鮮度分布 (売れた SKU 限定) で「fetch-amazon-fees の TTL ロジックが効いてるか」も見る
 *
 * 閾値:
 *   warn:     30日売上加重カバー率 < 98%
 *   critical: 30日売上加重カバー率 < 95%、または 売上 TOP20 に未カバー SKU が 1 件でもある
 *
 * 通知:
 *   warn/critical で GChat (env GCHAT_WEBHOOK)、無ければ skip
 *   exit code: 0=ok/warn (cron は赤くしない)、1=critical (cron 赤)
 *
 * 使い方:
 *   node apps/warehouse/monitor-fee-coverage.js                  daily cron 既定
 *   node apps/warehouse/monitor-fee-coverage.js --json           JSON 出力 (機械可読)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import { STALE_TTL_HOURS as TTL_HOURS } from './fee-cache-policy.js';

// TTL_HOURS = 鮮度しきい値 = stale/alert 閾値 (= SLO、168h)。fee-cache-policy.js に集約。
// ⚠️ 更新ジョブ fetch-amazon-fees.js は実効 96〜144h (基準120h) で意図的にこれより手前で refresh する (境界レース防止、揃えてはいけない)
const WARN_WEIGHTED_COVERAGE_PCT = 98;
const CRITICAL_WEIGHTED_COVERAGE_PCT = 95;
const TOP_N_UNCOVERED = 20;
const PRICE_DIFF_THRESHOLD = 0.20;               // fetch-amazon-fees と揃える
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;
function todayJstStr() { return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10); }  // GChat alert 用 (hardcode 回避、Codex #2)

function fmt(n) { return Math.round(n).toLocaleString(); }
function pct(num, denom) { return denom > 0 ? (100 * num / denom).toFixed(2) : '0.00'; }

/**
 * 直近 N 日の売れた SKU + 売上金額 (raw_sp_orders 起点) を集計し、キャッシュ状況と突合
 */
function buildCoverageReport(db, days) {
  const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // 1. 直近N日に売れた SKU 単位の sales summary + キャッシュ状況
  // Codex Round 1 #1 反映: latest_asin / latest_channel / latest_unit_price を raw_sp_orders 代表行から取得
  // Codex Round 1 #4 反映: age を hour 単位 (TTL_HOURS 比較で 7.9日が誤って fresh 扱いされるのを防止)
  // Codex Round 2 反映: latest_repr の SELECT に latest_unit_price も含め、tie-break を fetch-amazon-fees の
  //   orders_repr (ORDER BY purchase_date DESC, item_price DESC) と完全一致させる。MAX(item_price/quantity)
  //   を使うと「30日内のどこかで一度高値だった SKU」を monitor 側だけ price_diff 扱いして false critical 出る
  const rows = db.prepare(`
    WITH latest_repr AS (
      SELECT seller_sku, asin AS latest_asin,
        CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END AS latest_channel,
        item_price / NULLIF(quantity, 0) AS latest_unit_price
      FROM (
        SELECT seller_sku, asin, fulfillment_channel, item_price, quantity,
          ROW_NUMBER() OVER (PARTITION BY seller_sku ORDER BY purchase_date DESC, item_price DESC) AS rn
        FROM raw_sp_orders
        WHERE order_status NOT IN ('Cancelled') AND purchase_date >= ?
          AND item_price > 0 AND quantity > 0
      ) WHERE rn = 1
    ),
    sales AS (
      SELECT seller_sku,
        SUM(item_price) AS sales_jpy,
        SUM(quantity) AS units
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled') AND purchase_date >= ?
        AND item_price > 0 AND quantity > 0
      GROUP BY seller_sku
    )
    SELECT s.seller_sku, s.sales_jpy, s.units,
      lr.latest_unit_price, lr.latest_asin, lr.latest_channel,
      c.fetched_at, c.price_used, c.asin AS cached_asin, c.fulfillment_channel AS cached_channel,
      (julianday('now') - julianday(c.fetched_at)) * 24.0 AS age_hours,
      c.referral_fee_rate
    FROM sales s
    LEFT JOIN latest_repr lr ON s.seller_sku = lr.seller_sku
    LEFT JOIN amazon_sku_fees c ON s.seller_sku = c.seller_sku COLLATE NOCASE
    ORDER BY s.sales_jpy DESC
  `).all(cutoffStr, cutoffStr);

  // 2. カテゴリ分け (Codex Round 1 #1 反映: asin/channel ドリフトも uncovered 扱い)
  const total = { sku_count: rows.length, sales_jpy: 0 };
  const cached = { sku_count: 0, sales_jpy: 0 };           // キャッシュあり (TTL 内、asin/channel/価格すべて整合)
  const stale = { sku_count: 0, sales_jpy: 0 };            // キャッシュあるが TTL 超過
  const uncached = { sku_count: 0, sales_jpy: 0 };         // キャッシュ無し
  const priceDiff = { sku_count: 0, sales_jpy: 0 };        // 価格乖離大
  const asinDrift = { sku_count: 0, sales_jpy: 0 };        // asin ドリフト (Codex #1)
  const channelDrift = { sku_count: 0, sales_jpy: 0 };     // channel ドリフト (Codex #1)
  const uncoveredList = [];

  for (const r of rows) {
    total.sales_jpy += r.sales_jpy;
    const isCached = r.fetched_at != null;
    const ageOk = isCached && r.age_hours < TTL_HOURS;
    const priceDiffPct = (isCached && r.latest_unit_price > 0 && r.price_used > 0)
      ? Math.abs(r.latest_unit_price - r.price_used) / r.price_used : 0;
    const priceDiffLarge = priceDiffPct > PRICE_DIFF_THRESHOLD;
    // Codex Round 1 #1: fetch-amazon-fees の filterByTTLAndDiff と同じ判定で「covered か uncovered か」を揃える
    const asinChanged = isCached && r.latest_asin && r.cached_asin && r.latest_asin !== r.cached_asin;
    const channelChanged = isCached && r.latest_channel && r.cached_channel && r.latest_channel !== r.cached_channel;

    if (!isCached) {
      uncached.sku_count++;
      uncached.sales_jpy += r.sales_jpy;
      uncoveredList.push({ sku: r.seller_sku, sales: r.sales_jpy, units: r.units, reason: 'not_cached' });
    } else if (ageOk && !priceDiffLarge && !asinChanged && !channelChanged) {
      cached.sku_count++;
      cached.sales_jpy += r.sales_jpy;
    } else if (asinChanged) {
      asinDrift.sku_count++;
      asinDrift.sales_jpy += r.sales_jpy;
      uncoveredList.push({ sku: r.seller_sku, sales: r.sales_jpy, units: r.units, reason: `asin_changed_${r.cached_asin}→${r.latest_asin}` });
    } else if (channelChanged) {
      channelDrift.sku_count++;
      channelDrift.sales_jpy += r.sales_jpy;
      uncoveredList.push({ sku: r.seller_sku, sales: r.sales_jpy, units: r.units, reason: `channel_changed_${r.cached_channel}→${r.latest_channel}` });
    } else if (priceDiffLarge) {
      priceDiff.sku_count++;
      priceDiff.sales_jpy += r.sales_jpy;
      uncoveredList.push({ sku: r.seller_sku, sales: r.sales_jpy, units: r.units, reason: `price_diff_${(priceDiffPct*100).toFixed(0)}pct` });
    } else {
      stale.sku_count++;
      stale.sales_jpy += r.sales_jpy;
      uncoveredList.push({ sku: r.seller_sku, sales: r.sales_jpy, units: r.units, reason: `stale_${(r.age_hours/24).toFixed(1)}d` });
    }
  }

  // 3. カバー率
  const skuCoveragePct = total.sku_count > 0 ? cached.sku_count / total.sku_count * 100 : 100;
  const salesCoveragePct = total.sales_jpy > 0 ? cached.sales_jpy / total.sales_jpy * 100 : 100;

  // 4. TOP N 未カバー (sales 降順、既に rows は sales 降順なので uncoveredList も sales 降順)
  const topUncovered = uncoveredList.slice(0, TOP_N_UNCOVERED);

  // 5. 売上 TOP20 全体 (カバー有無関係なく) → このうち何件が未カバーか
  // Codex Round 1 #1: asin/channel ドリフトも uncovered 判定に含める (上のループと同じ条件)
  const top20Skus = rows.slice(0, 20);
  const top20UncoveredCount = top20Skus.filter(r => {
    const isCached = r.fetched_at != null;
    if (!isCached) return true;
    if (r.age_hours >= TTL_HOURS) return true;
    if (r.latest_asin && r.cached_asin && r.latest_asin !== r.cached_asin) return true;
    if (r.latest_channel && r.cached_channel && r.latest_channel !== r.cached_channel) return true;
    if (r.latest_unit_price > 0 && r.price_used > 0) {
      const d = Math.abs(r.latest_unit_price - r.price_used) / r.price_used;
      if (d > PRICE_DIFF_THRESHOLD) return true;
    }
    return false;
  }).length;

  return { days, total, cached, stale, uncached, priceDiff, asinDrift, channelDrift, skuCoveragePct, salesCoveragePct, topUncovered, top20UncoveredCount };
}

/**
 * GChat 通知 (best effort、失敗しても run は続行)
 */
async function sendGChat(text) {
  if (!GCHAT_WEBHOOK) return false;
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

// ─── メイン ───

async function main() {
  await initDB();
  const db = getDB();
  const isJson = process.argv.includes('--json');

  const r7 = buildCoverageReport(db, 7);
  const r30 = buildCoverageReport(db, 30);

  // 判定 (30日売上加重を主基準)
  let severity = 'ok';
  const reasons = [];
  if (r30.salesCoveragePct < CRITICAL_WEIGHTED_COVERAGE_PCT) {
    severity = 'critical';
    reasons.push(`30日売上加重カバー率 ${r30.salesCoveragePct.toFixed(2)}% < critical 閾値 ${CRITICAL_WEIGHTED_COVERAGE_PCT}%`);
  } else if (r30.salesCoveragePct < WARN_WEIGHTED_COVERAGE_PCT) {
    severity = 'warn';
    reasons.push(`30日売上加重カバー率 ${r30.salesCoveragePct.toFixed(2)}% < warn 閾値 ${WARN_WEIGHTED_COVERAGE_PCT}%`);
  }
  if (r30.top20UncoveredCount > 0) {
    severity = 'critical';
    reasons.push(`30日売上 TOP20 中 ${r30.top20UncoveredCount} 件が未カバー`);
  }

  if (isJson) {
    console.log(JSON.stringify({ severity, reasons, r7, r30 }, null, 2));
  } else {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('Amazon手数料キャッシュ カバー率監視');
    console.log('═══════════════════════════════════════════════════════════════');
    for (const r of [r7, r30]) {
      console.log(`\n📊 直近 ${r.days} 日 (${r.total.sku_count} SKU、売上 ¥${fmt(r.total.sales_jpy)})`);
      console.log(`  ✅ キャッシュ有 (TTL内) : ${r.cached.sku_count} SKU (件数 ${pct(r.cached.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.cached.sales_jpy)} (加重 ${pct(r.cached.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  ⏰ 鮮度切れ (>${TTL_HOURS / 24}d) : ${r.stale.sku_count} SKU (件数 ${pct(r.stale.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.stale.sales_jpy)} (加重 ${pct(r.stale.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  ❓ キャッシュ無し       : ${r.uncached.sku_count} SKU (件数 ${pct(r.uncached.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.uncached.sales_jpy)} (加重 ${pct(r.uncached.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  💰 価格乖離 >${PRICE_DIFF_THRESHOLD * 100}%   : ${r.priceDiff.sku_count} SKU (件数 ${pct(r.priceDiff.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.priceDiff.sales_jpy)} (加重 ${pct(r.priceDiff.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  🔁 ASIN 変更        : ${r.asinDrift.sku_count} SKU (件数 ${pct(r.asinDrift.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.asinDrift.sales_jpy)} (加重 ${pct(r.asinDrift.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  🚚 チャネル変更     : ${r.channelDrift.sku_count} SKU (件数 ${pct(r.channelDrift.sku_count, r.total.sku_count)}%) / 売上 ¥${fmt(r.channelDrift.sales_jpy)} (加重 ${pct(r.channelDrift.sales_jpy, r.total.sales_jpy)}%)`);
      console.log(`  → 売上加重カバー率: ${r.salesCoveragePct.toFixed(2)}% (件数 ${r.skuCoveragePct.toFixed(2)}%)`);
      console.log(`  → 売上 TOP20 中 未カバー: ${r.top20UncoveredCount}/20 件`);
    }

    if (r30.topUncovered.length > 0) {
      console.log(`\n🔍 30日 未カバー TOP ${Math.min(TOP_N_UNCOVERED, r30.topUncovered.length)} (売上降順):`);
      for (const u of r30.topUncovered) {
        console.log(`   ${u.sku.padEnd(40)} ¥${fmt(u.sales).padStart(8)} (${u.units}u、${u.reason})`);
      }
    }

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log(`判定: ${severity.toUpperCase()}`);
    if (reasons.length > 0) for (const r of reasons) console.log(`  - ${r}`);
    console.log('═══════════════════════════════════════════════════════════════');
  }

  // GChat 通知 (warn/critical のみ、Codex Round 1 #2 反映で日付を動的化)
  if (severity !== 'ok' && GCHAT_WEBHOOK) {
    const icon = severity === 'critical' ? '🚨' : '⚠️';
    let msg = `${icon} *Amazon手数料カバー率 ${severity.toUpperCase()}* (${todayJstStr()} daily cron)\n\n`;
    msg += reasons.map(r => `• ${r}`).join('\n');
    msg += `\n\n*30日サマリ*: 売上加重カバー率 ${r30.salesCoveragePct.toFixed(2)}% / 件数 ${r30.skuCoveragePct.toFixed(2)}%`;
    if (r30.topUncovered.length > 0) {
      msg += `\n\n*未カバー TOP 5*:\n` + r30.topUncovered.slice(0, 5).map(u => `• ${u.sku} ¥${fmt(u.sales)} (${u.reason})`).join('\n');
    }
    msg += `\n\n対応: \`node apps/warehouse/fetch-amazon-fees.js --recent 30\` を手動実行 or 翌日 cron 待ち`;
    await sendGChat(msg);
  }

  // exit code: 0 (ok/warn は cron 赤くしない)、1 (critical のみ cron 赤)
  // ⚠️ GChat通知(fetch/undici)後の process.exit() は Windows node で libuv assertion
  // (-1073740791) になり exit code を破壊する (GCHAT_WEBHOOK が 2026-07-06 に .env へ
  // 設定されて通知fetchが実際に走るようになった日から発症、#439 と同根)。
  // exitCode + 自然終了に変更 (undici socket は unref 済みで即終了する)。
  process.exitCode = severity === 'critical' ? 1 : 0;
}

main().catch(e => {
  console.error(`[monitor-fee-coverage] 致命的エラー: ${e.message}\n${e.stack}`);
  process.exitCode = 1;
});
