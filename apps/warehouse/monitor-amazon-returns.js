/**
 * monitor-amazon-returns.js — Amazon FBA の返品のアラート (控えめに・2026-10-22)
 *
 * 材料: amazon_fba_returns (fetch-amazon-returns.js が毎朝 UPSERT) と raw_sp_orders (販売数 = 返品率の分母)
 *
 * 見るもの (しきい値は下の定数):
 *   A. 返品が多い SKU: 直近 30 日 (JST) の返品数量が 3 個以上 かつ 返品率 (返品数量 / FBA の販売数量) が 10% 以上
 *      - 販売数量 = raw_sp_orders の FBA (fulfillment_channel = 'Amazon') かつ Amazon.co.jp の注文 (sales_channel。マルチチャネル発送 = 他モールの注文を FBA から出しただけ は数えない)・
 *        取り消し以外・同じ 30 日の購入日 (目安。返品は前の月の販売のこともある)
 *      - 販売が 0 (分母が無い) の SKU は返品率を出せないので A には出さない
 *   B. 同じ理由が急に増えた SKU: 直近 7 日の同じ理由の返品数量が 3 個以上 かつ その前の 28 日の 1 週あたりの 3 倍以上
 *      (前の 28 日が 0 なら 3 個以上で出す)
 *
 * 通知: 1 通にまとめて GChat (env GCHAT_WEBHOOK・無ければ送らない)。monitor-fee-coverage.js と同じ形
 *   🚨 同じ SKU (B は SKU × 理由) は 7 日のあいだ 2 回目を出さない (amazon_return_alerts_sent に送った日を残す。送れたときだけ残す)
 *   exit code: 0 (アラートがあっても cron は赤くしない)・1 = 読めない (表が無いなど)
 *
 * 使い方:
 *   node apps/warehouse/monitor-amazon-returns.js            # daily-sync から (返品の取得が成功した朝だけ)
 *   node apps/warehouse/monitor-amazon-returns.js --json     # 結果を JSON で出す (送らない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import { addDays, jstToday, isDirectRun } from './sp-api-report.js';
import { ensureReturnsTable } from './fetch-amazon-returns.js';

/** Amazon の売上の注文 (mall-orders-transform.mjs の AMAZON_SALES_CHANNEL と同じ値) */
const AMAZON_SALES_CHANNEL = 'Amazon.co.jp';

export const THRESHOLDS = Object.freeze({
  WINDOW_DAYS: 30,          // A: 見る期間
  MIN_RETURN_UNITS: 3,      // A: 返品数量の下限
  MIN_RETURN_RATE: 0.10,    // A: 返品率の下限 (10%)
  SPIKE_RECENT_DAYS: 7,     // B: 直近
  SPIKE_BASE_DAYS: 28,      // B: 比べる前の期間
  SPIKE_MIN_UNITS: 3,       // B: 直近の同じ理由の返品数量の下限
  SPIKE_FACTOR: 3,          // B: 前の 1 週あたりの何倍以上
  COOLDOWN_DAYS: 7,         // 同じものを 2 回目に出すまで
  TOP_N: 10,                // 1 通に載せる上限 (A・B それぞれ)
});

export function ensureAlertTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_return_alerts_sent (
    alert_key   TEXT PRIMARY KEY,     -- 'rate|<sku>' / 'reason|<sku>|<reason>'
    first_sent  TEXT NOT NULL,        -- JST の日付
    last_sent   TEXT NOT NULL
  )`);
}

/**
 * アラートの候補を出す (送った記録は見ない)
 * @returns {{ highRate: object[], reasonSpikes: object[] }}
 */
export function evaluateReturnAlerts(db, { today = jstToday(), t = THRESHOLDS } = {}) {
  const from30 = addDays(today, -t.WINDOW_DAYS);            // [today-30, today-1] = 30 日
  const yesterday = addDays(today, -1);
  const returns = db.prepare(`
    SELECT seller_sku, MAX(asin) AS asin, SUM(quantity) AS units
    FROM amazon_fba_returns WHERE return_date_jst >= ? AND return_date_jst <= ?
    GROUP BY seller_sku HAVING SUM(quantity) >= ?`).all(from30, yesterday, t.MIN_RETURN_UNITS);
  // 販売数量 (FBA)。purchase_date は UTC の ISO = JST の日付より 9 時間ずれる (目安なので許す)
  const sold = db.prepare(`
    SELECT COALESCE(SUM(quantity), 0) AS units FROM raw_sp_orders
    WHERE seller_sku = ? COLLATE NOCASE AND fulfillment_channel = 'Amazon' AND sales_channel = '${AMAZON_SALES_CHANNEL}'
      AND order_status NOT IN ('Cancelled', 'Canceled')
      AND substr(purchase_date, 1, 10) >= ? AND substr(purchase_date, 1, 10) <= ?`);
  const topReason = db.prepare(`
    SELECT reason, SUM(quantity) AS units FROM amazon_fba_returns
    WHERE seller_sku = ? AND return_date_jst >= ? AND return_date_jst <= ?
    GROUP BY reason ORDER BY units DESC, reason LIMIT 1`);
  const highRate = [];
  for (const r of returns) {
    const soldUnits = Number(sold.get(r.seller_sku, from30, yesterday).units) || 0;
    if (soldUnits <= 0) continue;
    const rate = r.units / soldUnits;
    if (rate < t.MIN_RETURN_RATE) continue;
    const top = topReason.get(r.seller_sku, from30, yesterday);
    highRate.push({ sku: r.seller_sku, asin: r.asin, returnUnits: r.units, soldUnits, rate, topReason: top?.reason ?? null, topReasonUnits: top?.units ?? 0 });
  }
  highRate.sort((a, b) => b.rate - a.rate || b.returnUnits - a.returnUnits || a.sku.localeCompare(b.sku));

  const recentFrom = addDays(today, -t.SPIKE_RECENT_DAYS);                  // [today-7, today-1]
  const baseFrom = addDays(recentFrom, -t.SPIKE_BASE_DAYS);                  // [today-35, today-8]
  const baseTo = addDays(recentFrom, -1);
  const spikes = db.prepare(`
    SELECT seller_sku, COALESCE(reason, '') AS reason,
      SUM(CASE WHEN return_date_jst >= ? AND return_date_jst <= ? THEN quantity ELSE 0 END) AS recent,
      SUM(CASE WHEN return_date_jst >= ? AND return_date_jst <= ? THEN quantity ELSE 0 END) AS base
    FROM amazon_fba_returns WHERE return_date_jst >= ? AND return_date_jst <= ?
    GROUP BY seller_sku, COALESCE(reason, '')`).all(recentFrom, yesterday, baseFrom, baseTo, baseFrom, yesterday);
  const weeks = t.SPIKE_BASE_DAYS / 7;
  const reasonSpikes = spikes
    .map((s) => ({ sku: s.seller_sku, reason: s.reason || null, recentUnits: s.recent, basePerWeek: s.base / weeks }))
    .filter((s) => s.recentUnits >= t.SPIKE_MIN_UNITS && s.recentUnits >= t.SPIKE_FACTOR * s.basePerWeek)
    .sort((a, b) => b.recentUnits - a.recentUnits || a.sku.localeCompare(b.sku));
  return { highRate, reasonSpikes };
}

export const alertKey = { rate: (a) => `rate|${a.sku}`, reason: (a) => `reason|${a.sku}|${a.reason ?? ''}` };

/** 1 通に載せる分 (それぞれ上から TOP_N)。送った記録はこの分だけに残す (載せなかった分は次の朝にまた候補) */
export function shownInMessage({ highRate, reasonSpikes }, { t = THRESHOLDS } = {}) {
  return { highRate: highRate.slice(0, t.TOP_N), reasonSpikes: reasonSpikes.slice(0, t.TOP_N) };
}

/** 送ってから COOLDOWN_DAYS 未満のものを外す */
export function filterCooldown(db, { highRate, reasonSpikes }, { today = jstToday(), t = THRESHOLDS } = {}) {
  ensureAlertTable(db);
  const get = db.prepare('SELECT last_sent FROM amazon_return_alerts_sent WHERE alert_key = ?');
  const fresh = (k) => { const r = get.get(k); return !r || r.last_sent <= addDays(today, -t.COOLDOWN_DAYS); };
  return { highRate: highRate.filter((a) => fresh(alertKey.rate(a))), reasonSpikes: reasonSpikes.filter((a) => fresh(alertKey.reason(a))) };
}

export function markSent(db, { highRate, reasonSpikes }, { today = jstToday() } = {}) {
  ensureAlertTable(db);
  const up = db.prepare(`INSERT INTO amazon_return_alerts_sent (alert_key, first_sent, last_sent) VALUES (?, ?, ?)
    ON CONFLICT(alert_key) DO UPDATE SET last_sent = excluded.last_sent`);
  db.transaction(() => {
    for (const a of highRate) up.run(alertKey.rate(a), today, today);
    for (const a of reasonSpikes) up.run(alertKey.reason(a), today, today);
  })();
}

const pct = (x) => `${(x * 100).toFixed(0)}%`;
export function buildMessage({ highRate, reasonSpikes }, { today = jstToday(), t = THRESHOLDS } = {}) {
  if (!highRate.length && !reasonSpikes.length) return null;
  let msg = `⚠️ *Amazon 返品の注意* (${today} daily)\n`;
  if (highRate.length) {
    msg += `\n*返品が多い SKU* (直近 ${t.WINDOW_DAYS} 日・${t.MIN_RETURN_UNITS} 個以上かつ返品率 ${pct(t.MIN_RETURN_RATE)} 以上)\n`;
    msg += highRate.slice(0, t.TOP_N).map((a) => `• ${a.sku}${a.asin ? ` (${a.asin})` : ''}: 返品 ${a.returnUnits} / 販売 ${a.soldUnits} = ${pct(a.rate)}${a.topReason ? `・多い理由 ${a.topReason} ${a.topReasonUnits}` : ''}`).join('\n');
    if (highRate.length > t.TOP_N) msg += `\n…ほか ${highRate.length - t.TOP_N} SKU`;
  }
  if (reasonSpikes.length) {
    msg += `\n\n*同じ理由の返品が急に増えた SKU* (直近 ${t.SPIKE_RECENT_DAYS} 日 vs その前 ${t.SPIKE_BASE_DAYS} 日の週平均)\n`;
    msg += reasonSpikes.slice(0, t.TOP_N).map((a) => `• ${a.sku}: ${a.reason || '(理由なし)'} 直近 ${a.recentUnits} 個 (前は週 ${a.basePerWeek.toFixed(1)} 個)`).join('\n');
    if (reasonSpikes.length > t.TOP_N) msg += `\n…ほか ${reasonSpikes.length - t.TOP_N} 件`;
  }
  msg += `\n\n(同じ SKU は ${t.COOLDOWN_DAYS} 日のあいだ再通知しない。明細 = warehouse.db の amazon_fba_returns)`;
  return msg;
}

async function sendGChat(text) {
  const hook = process.env.GCHAT_WEBHOOK;
  if (!hook) return false;
  try {
    const res = await fetch(hook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }) });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

async function main() {
  await initDB();
  const db = getDB();
  ensureReturnsTable(db);   // 初めての朝 (取得の前) でも読めるように
  const today = jstToday();
  const all = evaluateReturnAlerts(db, { today });
  const toSend = filterCooldown(db, all, { today });
  if (process.argv.includes('--json')) { console.log(JSON.stringify({ today, all, toSend }, null, 2)); return; }
  console.log(`[ReturnsAlert] 返品が多い SKU ${all.highRate.length} (新しく出す ${toSend.highRate.length}) / 理由が急に増えた ${all.reasonSpikes.length} (新しく出す ${toSend.reasonSpikes.length})`);
  for (const a of all.highRate.slice(0, 20)) console.log(`  [率] ${a.sku} 返品 ${a.returnUnits} / 販売 ${a.soldUnits} = ${pct(a.rate)} (${a.topReason || '-'})`);
  for (const a of all.reasonSpikes.slice(0, 20)) console.log(`  [急] ${a.sku} ${a.reason || '-'} 直近 ${a.recentUnits} (前は週 ${a.basePerWeek.toFixed(1)})`);
  const msg = buildMessage(toSend, { today });
  if (!msg) { console.log('[ReturnsAlert] ✅ 新しく出すアラートなし'); return; }
  if (!process.env.GCHAT_WEBHOOK) { console.log('[ReturnsAlert] ⚠️ GCHAT_WEBHOOK が無いので送らない (送った記録も残さない)'); return; }
  const sent = await sendGChat(msg);
  if (sent) markSent(db, shownInMessage(toSend), { today });   // 載せた分だけ (TOP_N を超えて載せなかった分は記録しない)
  // 最後の 1 行 = daily-sync の要約。送れなかったときは ⚠️ で始める (isWarnSummary = warn。Codex 統合レビュー P2)
  console.log(sent ? `[ReturnsAlert] ✅ 通知した (返品が多い ${toSend.highRate.length}・理由が急に増えた ${toSend.reasonSpikes.length})` : `⚠️ [ReturnsAlert] 通知を送れなかった (次の朝にまた出す・返品が多い ${toSend.highRate.length}・理由が急に増えた ${toSend.reasonSpikes.length})`);
}

if (await isDirectRun(import.meta.url)) {
  // 通知 (fetch) の直後の process.exit() は Windows node で libuv assertion になる = exitCode + 自然終了 (#439 と同じ)
  main().then(() => { if (process.exitCode == null) process.exitCode = 0; }).catch((e) => { console.error(`[ReturnsAlert] FATAL: ${e.message}`); process.exitCode = 1; });
}
