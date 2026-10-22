/**
 * fetch-amazon-ledger.js — Amazon FBA の在庫の台帳 (Inventory Ledger の詳細) を日ごとに取る (AI の分析用・2026-10-22)
 *
 * レポート: GET_LEDGER_DETAIL_VIEW_DATA (TSV・値は "" で囲まれていることがある)。dataStartTime / dataEndTime で期間を頼む
 *   列 (公式の見出し): Date, FNSKU, ASIN, MSKU, Title, Event Type, Reference ID, Quantity, Fulfillment Center, Disposition, Reason, Country,
 *                       Reconciled Quantity, Unreconciled Quantity (と新しいレポートには Date and Time)
 *   🚨 必須の列 (Date / FNSKU / Event Type / Quantity) が無ければ見出しを出して失敗
 *
 * ふだんは JST の昨日まで直近 30 日を毎朝取り直す (Reconciled / Unreconciled Quantity は紛失・破損の調査が進むと後から変わるため)。
 *   レポートは 1 回 (期間全体で 1 つ)。
 *   🚨 頼む期間は前後に 1 日ずつ広げる (Amazon が期間の境を UTC で切るか JST で切るか確かめられていない = 端の日が欠けないように)。
 *      保存するのは Date が期間 [from, to] の行だけ (外の行は数えて捨てる = 端の日を半端に書かない)
 *
 * 保存: amazon_inventory_ledger (この取得が作る)。**日ごとに置き換える** (その期間の日の行を消して入れ直す・行の無い日も「0 行で取れた」)
 *   同じ中身の行が 2 つあっても正しい (同じ日の同じ出荷が 2 回など) = 自然な鍵は作らず、line_no (レポートの中の順) を持つ
 *   取得の記録 = sp_report_fetch_days (世代が記録より古い取得ではその日を触らない)
 *   🚨 読めない行 (日付・数量・イベントの種類) が 1 つでもあれば、何も書かずに失敗
 *
 * ここでは保存まで (紛失・破損で補てんされていない在庫の検出は次の段)
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-ledger.js                     # 直近 30 日
 *   node apps/warehouse/fetch-amazon-ledger.js --days 90
 *   node apps/warehouse/fetch-amazon-ledger.js --from 2026-01-01 --to 2026-03-31
 *   node apps/warehouse/fetch-amazon-ledger.js --dry-run           # 期間を出すだけ (API を呼ばない)
 *   node apps/warehouse/fetch-amazon-ledger.js --check             # 取って検査し、要約と見本を出す (DB に書かない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import {
  fetchSpReport, createSpClient, isDirectRun, parseCommonArgs, jstStart, jstEnd, headerResolver, addDays, datesBetween, isRealDate,
  numOrNull, strOrNull, looseDateToJst, ensureReportFetchDays, dayWriteDecision, recordFetchDay, checkSaveArgs,
} from './sp-api-report.js';

export const REPORT_TYPE = 'GET_LEDGER_DETAIL_VIEW_DATA';
const DEFAULT_DAYS = 30;

export function ensureLedgerTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_inventory_ledger (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date            TEXT NOT NULL,      -- Date (YYYY-MM-DD に直したもの)
    event_datetime        TEXT,               -- Date and Time (レポートにあればそのまま)
    fnsku                 TEXT NOT NULL,
    asin                  TEXT,
    msku                  TEXT,               -- 出品者 SKU
    title                 TEXT,
    event_type            TEXT NOT NULL,      -- Shipments / CustomerReturns / Adjustments / Receipts / WhseTransfers / VendorReturns など
    reference_id          TEXT,
    quantity              INTEGER NOT NULL,   -- 増減 (負あり)
    fulfillment_center    TEXT,
    disposition           TEXT,               -- SELLABLE / DEFECTIVE / CUSTOMER_DAMAGED など
    reason                TEXT,               -- Adjustments の理由のコード (紛失・破損・見つかった など)
    country               TEXT,
    reconciled_quantity   INTEGER,
    unreconciled_quantity INTEGER,
    line_no               INTEGER NOT NULL,   -- レポートの中の行番号
    report_id             TEXT NOT NULL,
    generation            INTEGER NOT NULL,
    fetched_at            TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ail_date ON amazon_inventory_ledger(event_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ail_msku ON amazon_inventory_ledger(msku, event_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ail_fnsku ON amazon_inventory_ledger(fnsku, event_date)');
  ensureReportFetchDays(db);
}

const COLS = ['event_date', 'event_datetime', 'fnsku', 'asin', 'msku', 'title', 'event_type', 'reference_id', 'quantity', 'fulfillment_center',
  'disposition', 'reason', 'country', 'reconciled_quantity', 'unreconciled_quantity', 'line_no'];

/** 頼む期間 = [from の前の日の 00:00 JST, to の次の日の 23:59:59 JST] (今より後は今の 2 分前まで) */
export function requestWindow(from, to, nowMs = Date.now()) {
  const end = jstEnd(addDays(to, 1));
  const cap = new Date(nowMs - 120000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  return { dataStartTime: jstStart(addDays(from, -1)), dataEndTime: Date.parse(end) > nowMs - 120000 ? cap : end };
}

/**
 * TSV の行を検査して保存の形にする。読めない行が 1 つでもあれば例外 (何も書かない)。期間 [from, to] の外の行は捨てて数える
 * @returns {{ rows: object[], outside: number }}
 */
export function parseLedgerRows({ headers, rows }, { from, to }) {
  const h = headerResolver(headers);
  const col = {
    date: h('Date'), dateTime: h('Date and Time'), fnsku: h('FNSKU'), asin: h('ASIN'), msku: h('MSKU'), title: h('Title'), eventType: h('Event Type'),
    referenceId: h('Reference ID'), quantity: h('Quantity'), fc: h('Fulfillment Center'), disposition: h('Disposition'), reason: h('Reason'),
    country: h('Country'), reconciled: h('Reconciled Quantity'), unreconciled: h('Unreconciled Quantity'),
  };
  const missing = ['date', 'fnsku', 'eventType', 'quantity'].filter((k) => !col[k]);
  if (missing.length) throw new Error(`在庫の台帳のレポートに必須の列が無い (${missing.join(', ')})。見出し: ${JSON.stringify(headers).slice(0, 400)}`);
  const get = (r, k) => (col[k] ? strOrNull(r[col[k]]) : null);
  const signedInt = { min: -Infinity, integer: true };
  const out = []; let outside = 0;
  rows.forEach((r) => {
    const at = `行 ${r.__line}`;
    const rawDate = get(r, 'date');
    const d = looseDateToJst(rawDate);
    if (!d) throw new Error(`${at} の Date が日付として読めない: ${JSON.stringify(rawDate)}`);
    const fnsku = get(r, 'fnsku');
    if (!fnsku) throw new Error(`${at} に FNSKU が無い`);
    const eventType = get(r, 'eventType');
    if (!eventType) throw new Error(`${at} に Event Type が無い`);
    const quantity = numOrNull(get(r, 'quantity'), `${at} の Quantity`, signedInt);
    if (quantity === null) throw new Error(`${at} の Quantity が無い`);
    const row = {
      event_date: d, event_datetime: get(r, 'dateTime'), fnsku, asin: get(r, 'asin'), msku: get(r, 'msku'), title: get(r, 'title'),
      event_type: eventType, reference_id: get(r, 'referenceId'), quantity, fulfillment_center: get(r, 'fc'), disposition: get(r, 'disposition'),
      reason: get(r, 'reason'), country: get(r, 'country'),
      reconciled_quantity: numOrNull(get(r, 'reconciled'), `${at} の Reconciled Quantity`, signedInt),
      unreconciled_quantity: numOrNull(get(r, 'unreconciled'), `${at} の Unreconciled Quantity`, signedInt),
      line_no: r.__line,
    };
    if (d < from || d > to) { outside++; return; }
    out.push(row);
  });
  return { rows: out, outside };
}

/**
 * 期間 [from, to] を日ごとに置き換える (1 取引)
 * @returns {{ days: number, rows: number, outside: number, skippedOlder: string[], skippedSame: string[] }}
 */
export function saveLedger(db, report, { from, to, generation, reportId, now = () => new Date().toISOString() }) {
  checkSaveArgs({ generation, reportId });
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw new Error(`期間が不正: ${from}〜${to}`);
  ensureLedgerTable(db);
  const { rows, outside } = parseLedgerRows(report, { from, to });   // 例外ならここで止まる = 何も書かない
  const byDay = new Map();
  for (const r of rows) { const a = byDay.get(r.event_date) || []; a.push(r); byDay.set(r.event_date, a); }
  const ts = now();
  const del = db.prepare('DELETE FROM amazon_inventory_ledger WHERE event_date = ?');
  const ins = db.prepare(`INSERT INTO amazon_inventory_ledger (${COLS.join(', ')}, report_id, generation, fetched_at) VALUES (${COLS.map(() => '?').join(', ')}, ?, ?, ?)`);
  let nDays = 0, nRows = 0; const skippedOlder = [], skippedSame = [];
  db.transaction(() => {
    for (const d of datesBetween(from, to)) {
      const decision = dayWriteDecision(db, REPORT_TYPE, d, generation, reportId);
      if (decision === 'older') { skippedOlder.push(d); continue; }
      if (decision === 'same') { skippedSame.push(d); continue; }
      const dayRows = byDay.get(d) || [];   // 行の無い日 = 0 行で取れた
      del.run(d);
      for (const r of dayRows) { ins.run(...COLS.map((c) => r[c]), String(reportId), generation, ts); nRows++; }
      recordFetchDay(db, { reportType: REPORT_TYPE, date: d, generation, reportId, windowFrom: from, windowTo: to, rowCount: dayRows.length, fetchedAt: ts });
      nDays++;
    }
  }).immediate();
  return { days: nDays, rows: nRows, outside, skippedOlder, skippedSame };
}

export function parseArgs(argv = process.argv.slice(2), nowMs = Date.now()) {
  return parseCommonArgs(argv, { defaultDays: DEFAULT_DAYS, maxDays: 540, nowMs });
}

async function main() {
  const args = parseArgs();
  const win = requestWindow(args.from, args.to);
  console.log(`[Ledger] 期間: ${args.from}〜${args.to} (JST・頼むのは ${win.dataStartTime} 〜 ${win.dataEndTime})`);
  if (args.dryRun) { console.log('[Ledger] --dry-run のため API を呼ばない'); return; }
  const sp = await createSpClient();
  const rep = await fetchSpReport({ sp, reportType: REPORT_TYPE, format: 'tsv', ...win, pollIntervalMs: 30000, maxPolls: 50 });
  if (args.check) {
    const { rows, outside } = parseLedgerRows(rep, { from: args.from, to: args.to });
    const types = {};
    for (const r of rows) types[r.event_type] = (types[r.event_type] || 0) + 1;
    console.log(`[Ledger] --check: 見出し ${JSON.stringify(rep.headers)}`);
    console.log(`[Ledger] --check: ${rows.length} 行 (期間の外 ${outside} 行)・種類 ${JSON.stringify(types)}・見本 ${JSON.stringify(rows.slice(0, 2))}`);
    console.log(`[Ledger] ✅ 検査のみ (書かない) ${rows.length} 行`);
    return;
  }
  await initDB();
  const r = saveLedger(getDB(), rep, { from: args.from, to: args.to, generation: rep.generation, reportId: rep.reportId });
  console.log(`[Ledger] ✅ ${r.days} 日を置き換え (${r.rows} 行・期間の外で捨てた ${r.outside} 行)${r.skippedOlder.length ? ` / もっと新しい取得がある日 ${r.skippedOlder.length}` : ''} (${args.from}〜${args.to})`);
}

if (await isDirectRun(import.meta.url)) {
  main().then(() => { if (process.exitCode == null) process.exitCode = 0; }).catch((e) => { console.error('[Ledger] FATAL:', e?.message || e); process.exitCode = 1; });
}
