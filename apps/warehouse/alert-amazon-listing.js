/**
 * alert-amazon-listing.js — Amazon の出品とカートのアラート (2026-10-22・AI の分析とアラート)。新しい API は呼ばない (warehouse.db を読むだけ)
 *
 *   --kind listing : 出品の状態の写し (fetch-amazon-listing-issues.js) の今日と前の回を比べ、
 *                    「直近 30 日に売れた or FBA の在庫がある SKU」が ① BUYABLE でなくなった (出品が見つからない を含む) ② 新しい ERROR の issue が出た を 1 通にまとめて GChat
 *   --kind buybox  : カート価格の写し fact_amazon_price_snapshot (fetch-amazon-prices.js) の今日と前の回を比べ、
 *                    「前の回は自社がカート (buybox_is_mine = 1) → 今日は他社 (= 0)」の SKU のうち直近 30 日に売れたものを 1 通にまとめて GChat。
 *                    差 = 自社価格 − カート価格 (今日)。🚨 カート不在 (NULL) は「取られた」にしない
 *
 * 決め:
 *   - 前の回 (比べる元) = **アラートを最後まで済ませた日** (amazon_alert_runs。送れた・送るものが無かった・webhook が無かった日) の写し。
 *     🚨 写しの「今日より前でいちばん新しい日」にしない (Codex R3 P2 × 2): 取得が失敗した朝 (アラートを流さなかった) の写しや、GChat に送れなかった日の写しを
 *     元にすると、その日に起きた変化が翌日の差から消えて二度と知らせられない。済ませていない日は飛ばし、済ませた日と比べ直す
 *   - 済ませた日が無い (初回) or 7 日より古い = 通知しない (全件を「新しい」と言わない) で、今日を「済ませた日」にする。比べるのは両方の日に行がある SKU だけ (片方に無い = 分からない = 言わない)
 *   - 同じ日・同じ SKU・同じ理由は 1 回だけ送る (amazon_alert_sent。送れたときだけ記録 = 手で流し直しても二重に鳴らない)
 *   - env GCHAT_WEBHOOK が無ければ送らない (要約に出すだけ)。終了コードは 0 (見つけたことは失敗ではない)・読めない = 1
 *   - 🚨 送った後に process.exit() しない (Windows node の libuv assertion・#439)
 *
 * 使い方:
 *   node apps/warehouse/alert-amazon-listing.js --kind listing [--dry-run]   (--dry-run = 送らない・記録しない)
 *   node apps/warehouse/alert-amazon-listing.js --kind buybox  [--dry-run]
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import { businessDate, addDays, loadWatchSkus, sendGChat, isMainModule } from './amazon-listing-common.js';

export const MAX_PREV_GAP_DAYS = 7;
export const MAX_LINES = 25;

export function ensureAlertTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_alert_sent (
    date_jst  TEXT NOT NULL,
    kind      TEXT NOT NULL,    -- listing | buybox
    sku_key   TEXT NOT NULL,
    reason    TEXT NOT NULL,    -- not_buyable / new_error:<issue_key> / buybox_lost
    sent_at   TEXT NOT NULL,
    PRIMARY KEY (date_jst, kind, sku_key, reason)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_alert_runs (
    kind        TEXT NOT NULL,      -- listing | buybox
    date_jst    TEXT NOT NULL,      -- この日の写しでアラートを済ませた (次の日からの比べる元)
    prev_date   TEXT,               -- 比べた元 (初回 = NULL)
    items       INTEGER NOT NULL,   -- 見つけた件数
    note        TEXT,
    finished_at TEXT NOT NULL,
    PRIMARY KEY (kind, date_jst)
  )`);
}

/** 比べる元 = 今日より前でアラートを済ませたいちばん新しい日 (gap 日以内)。無ければ null */
export function alertBaseline(db, kind, today, maxGap = MAX_PREV_GAP_DAYS) {
  ensureAlertTables(db);
  const d = db.prepare(`SELECT MAX(date_jst) AS d FROM amazon_alert_runs WHERE kind = ? AND date_jst < ?`).get(kind, today)?.d || null;
  return d && d >= addDays(today, -maxGap) ? d : null;
}
/**
 * 比べる元の日の並び (新しい順) = 今日より前・gap 日以内でアラートを済ませた日。
 * 🚨 SKU ごとに「この並びの中で行があるいちばん新しい日」と比べる (Codex R4 P2): 済ませた日の写しが一部の SKU を取りこぼしていても
 *   (失敗が 10% 以下で取得は成功扱い・カート価格の失敗した batch)、その SKU はもっと前に済ませた日の行と比べる = 取りこぼした日をはさんだ変化を落とさない
 */
export function alertBaselineDates(db, kind, today, maxGap = MAX_PREV_GAP_DAYS) {
  ensureAlertTables(db);
  return db.prepare(`SELECT date_jst AS d FROM amazon_alert_runs WHERE kind = ? AND date_jst < ? AND date_jst >= ? ORDER BY date_jst DESC`).all(kind, today, addDays(today, -maxGap)).map((r) => r.d);
}
export function markAlertDone(db, kind, date, { prev = null, items = 0, note = null, at = new Date().toISOString() } = {}) {
  ensureAlertTables(db);
  db.prepare(`INSERT INTO amazon_alert_runs (kind, date_jst, prev_date, items, note, finished_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(kind, date_jst) DO UPDATE SET prev_date = excluded.prev_date, items = excluded.items, note = excluded.note, finished_at = excluded.finished_at`)
    .run(kind, date, prev, items, note, at);
}

const hasTable = (db, name) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(name);
const yen = (n) => (n == null ? '-' : `¥${Math.round(n).toLocaleString('ja-JP')}`);


/**
 * 出品のアラートの中身を作る (送らない)。戻り値 { state, today, prev, notBuyable: [...], newErrors: [...] }
 *   state: 'no_today' (今日の写しが無い) | 'first' (比べる元が無い) | 'ok'
 *   prevDates を渡さなければ alertBaselineDates (アラートを済ませた日・新しい順) を使う。SKU ごとに行があるいちばん新しい日と比べる
 */
export function buildListingAlerts(db, { today, watch, prevDates }) {
  if (!hasTable(db, 'amazon_listing_fetch_runs') || !db.prepare(`SELECT 1 FROM amazon_listing_fetch_runs WHERE date_jst = ?`).get(today)) return { state: 'no_today', today, prev: null, notBuyable: [], newErrors: [] };
  const dates = (prevDates ?? alertBaselineDates(db, 'listing', today)).filter((d) => db.prepare(`SELECT 1 FROM amazon_listing_fetch_runs WHERE date_jst = ?`).get(d));
  if (!dates.length) return { state: 'first', today, prev: null, notBuyable: [], newErrors: [] };
  const rowsOf = (d) => new Map(db.prepare(`SELECT * FROM amazon_listing_status_daily WHERE date_jst = ?`).all(d).filter((r) => watch.has(r.sku_key)).map((r) => [r.sku_key, r]));
  const issuesOf = (d, sev) => {
    const m = new Map();
    for (const r of db.prepare(`SELECT sku_key, issue_key, code, severity, message, enforcement_actions FROM amazon_listing_issue_daily WHERE date_jst = ?${sev ? ' AND severity = ?' : ''}`).all(...(sev ? [d, sev] : [d]))) {
      if (!watch.has(r.sku_key)) continue;
      if (!m.has(r.sku_key)) m.set(r.sku_key, []);
      m.get(r.sku_key).push(r);
    }
    return m;
  };
  const t = rowsOf(today);
  // SKU ごとの比べる元 = 済ませた日の新しい順で、行があるいちばん新しい日の行と issue
  const p = new Map(), pIssues = new Map();
  for (const d of dates) {
    const rows = rowsOf(d);
    const iss = issuesOf(d, null);
    for (const [k, r] of rows) if (!p.has(k)) { p.set(k, r); pIssues.set(k, iss.get(k) || []); }
  }
  const tErr = issuesOf(today, 'ERROR');
  const notBuyable = [], newErrors = [];
  for (const [k, w] of watch) {
    const a = t.get(k), b = p.get(k);
    if (!a || !b) continue;
    const ctx = { sku_key: k, seller_sku: a.seller_sku, asin: a.asin || b.asin, units: w.units, sales_jpy: w.sales_jpy, fba_qty: w.fba_qty, prev_date: b.date_jst };
    if (b.is_buyable === 1 && a.is_buyable !== 1) notBuyable.push({ ...ctx, prev_status: b.status, status: a.status, suppressed: a.suppressed, reason: 'not_buyable' });
    const before = new Set(pIssues.get(k).map((i) => i.issue_key));
    for (const i of (tErr.get(k) || [])) {
      if (!before.has(i.issue_key)) newErrors.push({ ...ctx, issue_key: i.issue_key, code: i.code, message: i.message, actions: JSON.parse(i.enforcement_actions || '[]'), reason: `new_error:${i.issue_key}` });
    }
  }
  const bySales = (x, y) => (y.sales_jpy - x.sales_jpy) || (y.fba_qty - x.fba_qty) || (x.sku_key < y.sku_key ? -1 : 1);
  notBuyable.sort(bySales);
  newErrors.sort(bySales);
  return { state: 'ok', today, prev: dates[0], notBuyable, newErrors };
}

/**
 * カートを取られたアラートの中身を作る (送らない)。戻り値 { state, today, prev, lost: [...] }
 *   state: 'no_today' | 'first' | 'ok'
 */
export function buildBuyboxAlerts(db, { today, watch, prevDates }) {
  if (!hasTable(db, 'fact_amazon_price_snapshot') || !db.prepare(`SELECT 1 FROM fact_amazon_price_snapshot WHERE date_jst = ? LIMIT 1`).get(today)) return { state: 'no_today', today, prev: null, lost: [] };
  const dates = (prevDates ?? alertBaselineDates(db, 'buybox', today)).filter((d) => db.prepare(`SELECT 1 FROM fact_amazon_price_snapshot WHERE date_jst = ? LIMIT 1`).get(d));
  if (!dates.length) return { state: 'first', today, prev: null, lost: [] };
  // SKU ごとの比べる元 = 済ませた日の新しい順で、行がある (取れた) いちばん新しい日の行 (NULL = カート不在も「取れた」)。失敗した batch の SKU は行が無い = もっと前の日と比べる
  const prevOf = new Map();
  for (const d of dates) {
    for (const r of db.prepare(`SELECT seller_sku, buybox_is_mine, buybox_price, date_jst FROM fact_amazon_price_snapshot WHERE date_jst = ?`).all(d)) {
      const k = String(r.seller_sku).trim().toLowerCase();
      if (!prevOf.has(k)) prevOf.set(k, r);
    }
  }
  const lost = [];
  // 🚨 = 0 / = 1 で比べる (NULL = カート不在・不明 は「取られた」にも「自社だった」にもしない)
  for (const r of db.prepare(`SELECT seller_sku, asin, channel, my_price, buybox_price FROM fact_amazon_price_snapshot WHERE date_jst = ? AND buybox_is_mine = 0`).all(today)) {
    const k = String(r.seller_sku).trim().toLowerCase();
    const w = watch.get(k);
    if (!w || !(w.units > 0)) continue;   // 売れていない SKU は言わない (在庫だけでは足りない)
    const p = prevOf.get(k);
    if (!p || p.buybox_is_mine !== 1) continue;
    const diff = (r.my_price != null && r.buybox_price != null) ? Math.round((r.my_price - r.buybox_price) * 100) / 100 : null;
    lost.push({ sku_key: k, seller_sku: r.seller_sku, asin: r.asin, channel: r.channel, my_price: r.my_price, buybox_price: r.buybox_price, prev_buybox_price: p.buybox_price, prev_date: p.date_jst, diff, units: w.units, sales_jpy: w.sales_jpy, reason: 'buybox_lost' });
  }
  lost.sort((x, y) => (y.sales_jpy - x.sales_jpy) || (x.sku_key < y.sku_key ? -1 : 1));
  return { state: 'ok', today, prev: dates[0], lost };
}

/** 送った記録が無いものだけ残す */
export function filterUnsent(db, kind, date, items) {
  ensureAlertTables(db);
  const q = db.prepare(`SELECT 1 FROM amazon_alert_sent WHERE date_jst = ? AND kind = ? AND sku_key = ? AND reason = ?`);
  return items.filter((i) => !q.get(date, kind, i.sku_key, i.reason));
}
export function markSent(db, kind, date, items, sentAt = new Date().toISOString()) {
  ensureAlertTables(db);
  const ins = db.prepare(`INSERT OR IGNORE INTO amazon_alert_sent (date_jst, kind, sku_key, reason, sent_at) VALUES (?, ?, ?, ?, ?)`);
  db.transaction(() => { for (const i of items) ins.run(date, kind, i.sku_key, i.reason, sentAt); })();
}

const cut = (s, n) => { const x = String(s ?? '').replace(/\s+/g, ' ').trim(); return x.length > n ? `${x.slice(0, n - 1)}…` : x; };
const salesNote = (i) => `30日 ${i.units || 0}個 ${yen(i.sales_jpy)}${i.fba_qty ? ` / FBA在庫 ${i.fba_qty}` : ''}`;
function listLines(items, fmt) {
  const lines = items.slice(0, MAX_LINES).map(fmt);
  if (items.length > MAX_LINES) lines.push(`  …ほか ${items.length - MAX_LINES} 件`);
  return lines;
}

export function formatListingMessage({ today, prev, notBuyable, newErrors }) {
  const out = [`🚨 *Amazon 出品の問題* (${today}・前の回 ${prev}・売れている / FBA 在庫のある SKU だけ)`];
  if (notBuyable.length) {
    out.push('', `*BUYABLE でなくなった ${notBuyable.length} 件*`);
    out.push(...listLines(notBuyable, (i) => `• ${i.seller_sku}${i.asin ? ` (${i.asin})` : ''}: ${i.prev_status || '-'}${i.prev_date && i.prev_date !== prev ? ` (${i.prev_date})` : ''} → ${i.status || '(状態なし)'}${i.suppressed ? ' 【出品停止】' : ''} / ${salesNote(i)}`));
  }
  if (newErrors.length) {
    out.push('', `*新しい ERROR ${newErrors.length} 件*`);
    out.push(...listLines(newErrors, (i) => `• ${i.seller_sku}: [${i.code}] ${cut(i.message, 80)}${i.actions?.length ? ` 【${i.actions.join(',')}】` : ''} / ${salesNote(i)}`));
  }
  out.push('', '見る所: セラーセントラル「在庫 > 出品情報の品質管理」/ warehouse.db の amazon_listing_status_daily・amazon_listing_issue_daily');
  return out.join('\n');
}

export function formatBuyboxMessage({ today, prev, lost }) {
  const out = [`⚠️ *Amazon カートを取られた ${lost.length} 件* (${today}・前の回 ${prev} は自社がカート・直近 30 日に売れた SKU だけ)`];
  out.push(...listLines(lost, (i) => `• ${i.seller_sku}${i.asin ? ` (${i.asin})` : ''}: 自社 ${yen(i.my_price)} / カート ${yen(i.buybox_price)}${i.diff != null ? ` (差 ${i.diff > 0 ? '+' : ''}${yen(i.diff).replace('¥-', '-¥')})` : ''} / 30日 ${i.units}個 ${yen(i.sales_jpy)}`));
  out.push('', '差 = 自社価格 − カート価格 (税込・landed)。見る所: /apps/amazon-pricing・warehouse.db の fact_amazon_price_snapshot');
  return out.join('\n');
}

/**
 * 1 回分 (作る → 送っていないものだけ → 送る → 記録)。戻り値 { summary, exitCode }
 * 済ませた日 (amazon_alert_runs) にするのは: 初回 (比べる元が無い)・見つけたものが無い / もう送ってある・送れた・webhook が無い (送る先が無い)。
 * 🚨 GChat に送れなかった日・--dry-run は済ませた日にしない = 次の回も同じ比べる元から差を出す (その日の変化を落とさない)
 */
export async function runAlert(db, { kind, today, dryRun = false, send = sendGChat, webhook = process.env.GCHAT_WEBHOOK }) {
  if (kind !== 'listing' && kind !== 'buybox') return { exitCode: 1, summary: `❌ --kind が違う (${kind}) = listing | buybox` };
  ensureAlertTables(db);
  const watch = loadWatchSkus(db, { today });
  const label = kind === 'listing' ? '出品アラート' : 'カートアラート';
  const done = (prev, n, note) => { if (!dryRun) markAlertDone(db, kind, today, { prev, items: n, note }); };
  const built = kind === 'listing' ? buildListingAlerts(db, { today, watch }) : buildBuyboxAlerts(db, { today, watch });
  if (built.state === 'no_today') return { exitCode: 0, summary: `⚠️ ${label}: 今日 (${today}) の${kind === 'listing' ? '出品' : 'カート'}の写しが無い = 比べない` };
  if (built.state === 'first') {
    done(null, 0, 'first');
    return { exitCode: 0, summary: `✓ ${label}: 比べる元 (7 日以内にアラートを済ませた日の写し) が無い (初回) = 通知しない・今日を比べる元にする` };
  }
  let items, message;
  if (kind === 'listing') {
    const nb = filterUnsent(db, kind, today, built.notBuyable);
    const ne = filterUnsent(db, kind, today, built.newErrors);
    items = [...nb, ...ne];
    if (items.length) message = formatListingMessage({ today, prev: built.prev, notBuyable: nb, newErrors: ne });
  } else {
    items = filterUnsent(db, kind, today, built.lost);
    if (items.length) message = formatBuyboxMessage({ today, prev: built.prev, lost: items });
  }
  const found = kind === 'listing' ? built.notBuyable.length + built.newErrors.length : built.lost.length;
  const counts = kind === 'listing' ? `BUYABLE でなくなった ${built.notBuyable.length}・新しい ERROR ${built.newErrors.length}` : `カートを取られた ${built.lost.length}`;
  const head = `${label} (${today} vs ${built.prev}): ${counts}`;
  if (!items.length) { done(built.prev, found, 'nothing_new'); return { exitCode: 0, summary: `✓ ${head} / 新しく送るもの無し` }; }
  console.log(message);
  if (dryRun) return { exitCode: 0, summary: `✓ ${head} / --dry-run = 送らない` };
  if (!webhook) { done(built.prev, found, 'no_webhook'); return { exitCode: 0, summary: `⚠️ ${head} / GCHAT_WEBHOOK が無い = 送らない` }; }
  const ok = await send(message, { webhook });
  if (!ok) return { exitCode: 0, summary: `⚠️ ${head} / GChat に送れなかった (済ませた日にしない = 次の回も ${built.prev} と比べてまた送る)` };
  markSent(db, kind, today, items);
  done(built.prev, found, 'sent');
  return { exitCode: 0, summary: `✓ ${head} / ${items.length} 件を通知` };
}

async function main() {
  const argv = process.argv.slice(2);
  const ki = argv.indexOf('--kind');
  const kind = ki >= 0 ? argv[ki + 1] : null;
  await initDB();
  const db = getDB();
  const r = await runAlert(db, { kind, today: businessDate(), dryRun: argv.includes('--dry-run') });
  console.log(r.summary);   // 最後の 1 行 = daily-sync の朝の通知 (⚠️ で始まれば warn)
  process.exitCode = r.exitCode;
}

if (isMainModule(import.meta.url)) main().catch((e) => {
  console.error(`[AmazonAlert] FATAL: ${e.message}`);
  process.exitCode = 1;
});
