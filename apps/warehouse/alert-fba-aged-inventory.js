/**
 * alert-fba-aged-inventory.js — FBA の長期在庫 (長期保管料の対象に近づいている在庫) を **週に 1 回・1 通** で GChat に知らせる (2026-10-22)
 *
 * 材料 = 在庫計画の日 × SKU の履歴 (warehouse.db fba_planning_daily。毎朝のスナップショットが作る = apps/warehouse/fba-planning-history.js)。
 *   API は呼ばない。JP だけ。
 *
 * 控えめにする:
 *   - 送るのは ALERT_WEEKDAY_JST の曜日 (既定 = 月曜) だけ。同じ週に 2 回は送らない (fba_aged_alert_log に記録。daily-sync の再試行・手での再実行でも 1 通)
 *   - 1 通にまとめる (上位 MAX_LIST 件 + 合計)。該当が無ければ送らない
 *   - 対象 = 181 日以上の在庫が AGED_MIN_QTY 個以上 かつ その在庫金額 (個数 × 自分の価格) が AGED_MIN_VALUE_YEN 以上 (価格が分からなければ個数だけで)、
 *     または 長期保管料 (AIS) の見込みが AIS_MIN_YEN 以上
 *   - 履歴が古い (最新の日が MAX_AGE_DAYS 日より前)・無い → 送らない (⚠️ の 1 行だけ)
 * 通知: env GCHAT_WEBHOOK (monitor-fee-coverage.js と同じ)。無ければ送らずに skip
 * 終了コード: 0 (送れなかった・履歴が古いも 0 = 朝の daily-sync を赤くしない)。warehouse.db が読めないときだけ 1
 *
 * 使い方:
 *   node apps/warehouse/alert-fba-aged-inventory.js              # daily-sync から毎朝 (曜日でなければ何もしない)
 *   node apps/warehouse/alert-fba-aged-inventory.js --dry-run    # 文面を出すだけ (送らない・記録しない・曜日を問わない)
 *   node apps/warehouse/alert-fba-aged-inventory.js --force      # 曜日と「今週は送った」を問わずに送る
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ensurePlanningHistoryTables } from './fba-planning-history.js';

export const ALERT_WEEKDAY_JST = 1;          // 0 = 日曜 … 1 = 月曜
export const AGED_MIN_QTY = 10;              // 181 日以上の在庫がこの個数以上
export const AGED_MIN_VALUE_YEN = 30000;     // かつ その在庫金額 (個数 × 自分の価格) がこの金額以上
export const AIS_MIN_YEN = 1000;             // または 長期保管料 (AIS) の見込みがこの金額以上
export const MAX_AGE_DAYS = 3;               // 最新の履歴がこれより古ければ送らない
export const MAX_LIST = 15;                  // 1 通に載せる SKU の数
const GCHAT_MAX = 3900;

const jstDate = (ms) => new Date(ms + 9 * 3600000).toISOString().slice(0, 10);
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
export const jstWeekday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600000).getUTCDay();
/** その JST の日を含む週の月曜 (YYYY-MM-DD) = 「今週は送った」の鍵 */
export function weekKeyJst(nowMs = Date.now()) {
  const d = jstDate(nowMs);
  const wd = jstWeekday(nowMs);
  return addDays(d, -((wd + 6) % 7));
}
const fmt = (n) => Math.round(n).toLocaleString('ja-JP');
const sumKnown = (vals) => { const k = vals.filter((v) => v !== null && v !== undefined); return k.length ? k.reduce((a, b) => a + b, 0) : null; };

export function ensureAlertLog(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fba_aged_alert_log (
    week_key      TEXT PRIMARY KEY,
    sent_at       TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    sku_count     INTEGER NOT NULL
  )`);
}

/** 最新の JP の日 (今日まで)。無ければ null */
export function latestSnapshotDate(db, todayJst) {
  ensurePlanningHistoryTables(db);
  const r = db.prepare(`SELECT MAX(snapshot_date) AS d FROM fba_planning_daily_days WHERE market = 'jp' AND snapshot_date <= ?`).get(todayJst);
  return r && r.d ? r.d : null;
}

/**
 * その日の対象 SKU (金額の大きい順)
 * @returns {{ sku, available, aged181, over365, agedValue, aisEst, storageNext, units30, action, removalQty }[]}
 */
export function pickAgedCandidates(db, snapshotDate) {
  ensurePlanningHistoryTables(db);
  const rows = db.prepare(`SELECT amazon_sku, available, your_price, inv_age_181_270, inv_age_271_365, inv_age_365_plus,
      ais_est_181_210, ais_est_211_240, ais_est_241_270, ais_est_271_300, ais_est_301_330, ais_est_331_365, ais_est_365_plus,
      estimated_storage_cost_next_month, units_shipped_t30, recommended_action, recommended_removal_qty
    FROM fba_planning_daily WHERE snapshot_date = ? AND market = 'jp'`).all(snapshotDate);
  const out = [];
  for (const r of rows) {
    const aged181 = sumKnown([r.inv_age_181_270, r.inv_age_271_365, r.inv_age_365_plus]);
    const aisEst = sumKnown([r.ais_est_181_210, r.ais_est_211_240, r.ais_est_241_270, r.ais_est_271_300, r.ais_est_301_330, r.ais_est_331_365, r.ais_est_365_plus]);
    const agedValue = aged181 !== null && r.your_price !== null && r.your_price > 0 ? aged181 * r.your_price : null;
    const byQty = aged181 !== null && aged181 >= AGED_MIN_QTY && (agedValue === null || agedValue >= AGED_MIN_VALUE_YEN);
    const byAis = aisEst !== null && aisEst >= AIS_MIN_YEN;
    if (!byQty && !byAis) continue;
    out.push({ sku: r.amazon_sku, available: r.available, aged181, over365: r.inv_age_365_plus, agedValue, aisEst, storageNext: r.estimated_storage_cost_next_month,
      units30: r.units_shipped_t30, action: r.recommended_action, removalQty: r.recommended_removal_qty });
  }
  out.sort((a, b) => (b.agedValue ?? -1) - (a.agedValue ?? -1) || (b.aisEst ?? -1) - (a.aisEst ?? -1) || (b.aged181 ?? 0) - (a.aged181 ?? 0) || (a.sku < b.sku ? -1 : 1));
  return out;
}

/** 1 通の文面 */
export function buildAgedAlertMessage(cands, snapshotDate) {
  const tot = (k) => cands.reduce((s, c) => s + (c[k] ?? 0), 0);
  const anyKnown = (k) => cands.some((c) => c[k] !== null && c[k] !== undefined);
  let msg = `📦 *FBA 長期在庫 (週 1 回)* ${snapshotDate} 時点\n`;
  msg += `181 日以上の在庫が多い SKU: ${cands.length} 件 / 181 日以上 計 ${fmt(tot('aged181'))} 個`;
  if (anyKnown('over365')) msg += ` (うち 365 日超 ${fmt(tot('over365'))} 個)`;
  if (anyKnown('agedValue')) msg += ` / 在庫金額 約 ¥${fmt(tot('agedValue'))}`;
  if (anyKnown('aisEst')) msg += ` / 長期保管料の見込み ¥${fmt(tot('aisEst'))}`;
  msg += `\n`;
  const lines = cands.slice(0, MAX_LIST).map((c) => {
    const parts = [`181日以上 ${c.aged181 ?? '?'}個${c.over365 ? ` (365日超 ${c.over365})` : ''}`];
    if (c.available !== null && c.available !== undefined) parts.push(`在庫 ${c.available}`);
    if (c.units30 !== null && c.units30 !== undefined) parts.push(`30日販売 ${c.units30}個`);
    if (c.agedValue !== null) parts.push(`¥${fmt(c.agedValue)}`);
    if (c.aisEst !== null && c.aisEst > 0) parts.push(`長期保管料見込み ¥${fmt(c.aisEst)}`);
    if (c.action) parts.push(`推奨: ${c.action}${c.removalQty ? ` (${c.removalQty}個)` : ''}`);
    return `• ${c.sku} — ${parts.join(' / ')}`;
  });
  msg += `\n${lines.join('\n')}`;
  if (cands.length > MAX_LIST) msg += `\n… ほか ${cands.length - MAX_LIST} 件`;
  msg += `\n\n対象: 181 日以上が ${AGED_MIN_QTY} 個以上かつ ¥${fmt(AGED_MIN_VALUE_YEN)} 以上、または長期保管料の見込み ¥${fmt(AIS_MIN_YEN)} 以上 (warehouse.db fba_planning_daily)`;
  if (msg.length > GCHAT_MAX) msg = `${msg.slice(0, GCHAT_MAX)}\n…[切り詰め]`;
  return msg;
}

async function sendGChat(webhook, text, fetchImpl = fetch) {
  try {
    const res = await fetchImpl(webhook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }), signal: AbortSignal.timeout(30000) });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

/**
 * 本体 (試験で db・時刻・送り手を差し替えられる)。
 * @returns {{ status: string, lastLine: string, message?: string, candidates?: number }}
 */
export async function runAgedAlert(db, { nowMs = Date.now(), webhook = process.env.GCHAT_WEBHOOK, force = false, dryRun = false, send = sendGChat, log = console.log } = {}) {
  ensureAlertLog(db);
  const today = jstDate(nowMs);
  const week = weekKeyJst(nowMs);
  if (!force && !dryRun && jstWeekday(nowMs) !== ALERT_WEEKDAY_JST) return { status: 'not_today', lastLine: `⏭️ 長期在庫アラート: 送る曜日ではない (${'日月火水木金土'[ALERT_WEEKDAY_JST]}曜だけ)` };
  if (!force && !dryRun) {
    const sent = db.prepare('SELECT sent_at FROM fba_aged_alert_log WHERE week_key = ?').get(week);
    if (sent) return { status: 'already_sent', lastLine: `⏭️ 長期在庫アラート: 今週 (${week}〜) は送った (${sent.sent_at})` };
  }
  const snap = latestSnapshotDate(db, today);
  if (!snap) return { status: 'no_history', lastLine: '⚠️ 長期在庫アラート: 在庫計画の履歴 (fba_planning_daily) がまだ無い = 送らない' };
  if (snap < addDays(today, -MAX_AGE_DAYS)) return { status: 'stale', lastLine: `⚠️ 長期在庫アラート: 在庫計画の履歴が古い (最新 ${snap}) = 送らない` };
  const cands = pickAgedCandidates(db, snap);
  if (cands.length === 0) return { status: 'none', candidates: 0, lastLine: `✅ 長期在庫アラート: 該当なし (${snap})` };
  const message = buildAgedAlertMessage(cands, snap);
  log(message);
  if (dryRun) return { status: 'dry_run', candidates: cands.length, message, lastLine: `ℹ️ 長期在庫アラート: --dry-run (送らない) 該当 ${cands.length} 件 (${snap})` };
  if (!webhook) return { status: 'no_webhook', candidates: cands.length, message, lastLine: `⚠️ 長期在庫アラート: GCHAT_WEBHOOK 未設定のため送らない (該当 ${cands.length} 件)` };
  const ok = await send(webhook, message);
  if (!ok) return { status: 'send_failed', candidates: cands.length, message, lastLine: `⚠️ 長期在庫アラート: GChat に送れなかった (該当 ${cands.length} 件。明日の朝にもう一度試す)` };
  db.prepare('INSERT OR REPLACE INTO fba_aged_alert_log (week_key, sent_at, snapshot_date, sku_count) VALUES (?, ?, ?, ?)').run(week, new Date(nowMs).toISOString(), snap, cands.length);
  return { status: 'sent', candidates: cands.length, message, lastLine: `✅ 長期在庫アラート: 送った (該当 ${cands.length} 件・${snap})` };
}

async function main() {
  const { initDB, getDB } = await import('./db.js');
  await initDB();
  const argv = process.argv.slice(2);
  const r = await runAgedAlert(getDB(), { force: argv.includes('--force'), dryRun: argv.includes('--dry-run') });
  console.log(r.lastLine);
  process.exitCode = 0;   // GChat の fetch の直後に process.exit() しない (Windows の libuv assertion。monitor-fee-coverage.js と同じ)
}

const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
const isMain = !!process.argv[1] && foldCase(realPath(process.argv[1])) === foldCase(realPath(fileURLToPath(import.meta.url)));
if (isMain) main().catch((e) => {
  console.log(`❌ 長期在庫アラート: ${String(e.message).replace(/\s+/g, ' ').slice(0, 300)}`);
  process.exitCode = 1;
});
