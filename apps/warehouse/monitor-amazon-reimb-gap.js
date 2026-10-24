/**
 * monitor-amazon-reimb-gap.js — FBA の倉庫で紛失・破損したのに Amazon から補てんされていない在庫を、**週に 1 回・1 通** で GChat に知らせる (2026-10-24)
 *   = 申請すればお金が戻る候補。API は呼ばない (毎朝の取得が warehouse.db に入れた 2 つの表を突き合わせるだけ)
 *
 * 材料:
 *   amazon_inventory_ledger (fetch-amazon-ledger.js。GET_LEDGER_DETAIL_VIEW_DATA の明細。直近 30 日を毎朝日ごとに置き換え)
 *     🚨 置き換えるのは取り直した 30 日だけ = それより古い日の行は消えずに残る (最後に取り直したときの中身のまま)。
 *        = 履歴は取り始めた日 (sp_report_fetch_days の最初の日) から貯まる。Reconciled / Unreconciled Quantity は 30 日を過ぎると更新されない
 *   amazon_fba_reimbursements (fetch-amazon-reimbursements.js。GET_FBA_REIMBURSEMENTS_DATA。直近 60 日を毎朝 UPSERT = 古い行も残る)
 *   fba_planning_daily (在庫計画の履歴。補てんの単価の履歴が無い SKU の売価だけ。無ければ数量だけ)
 *
 * 数え方 (FNSKU ごと。MSKU は見せるだけ):
 *   紛失 = 台帳の Event Type = Adjustments・Reason が LOST_REASONS (M)・数量が負 / 破損 = 同じく DAMAGED_REASONS (E)・数量が負
 *   それを古い順に、次の分で消していく (先入れ先出し。🚨 その日より後に起きた分でしか消さない = 取り始める前の紛失への補てんを今の紛失に当てる誤りは「出さない」側に倒れる):
 *     ① 同じ理由の数量が正の行 (破損の取り消しなど) ② 見つかった (FOUND_REASONS = F・数量が正) = 紛失だけを消す。消す相手が無ければ「余り」(日付・種類つき) として持つ
 *     ③ 補てん (Lost_Warehouse / Damaged_Warehouse・現金と在庫の両方 = quantity-reimbursed-total)。同じ種類を先に、無ければ別の種類も
 *     ④ 補てんの取り消し (数量が負・同じ日の補てんより後) = 取り消された補てん (original-reimbursement-id) が消した塊を戻す (ID が無ければ同じ種類の補てん → どれでも、新しい順)。
 *        戻す塊は、同じ種類でその発生日より後の「余り」で先に打ち消す (見つかった → Amazon が補てんを取り消した = 差し引きゼロ)
 *   残った分の発生日から: 待つ日数 (紛失 WAIT_DAYS_LOST・破損 WAIT_DAYS_DAMAGED = Amazon の自動の補てん・見つかるのを待つ) より前 = 「待っている」(載せない)
 *                      申請の期限 (発生から CLAIM_DEADLINE_DAYS 日) を過ぎた = 「期限切れの可能性」(件数だけ) / そのあいだ = 候補
 *   見込みの金額 = 個数 × 単価 (① その FNSKU (無ければ SKU) の最後の補てんの単価 amount-per-unit ② 在庫計画の売価 your_price ③ 分からなければ数量だけ)
 *   候補 = MIN_UNITS 個以上 かつ (金額が分かれば) MIN_VALUE_YEN 以上。金額の大きい順 (分からないものは後ろ)・期限まで URGENT_DAYS_LEFT 日以内は 🔴
 *
 * 控えめにする (alert-fba-aged-inventory.js と同じ形):
 *   - 送るのは ALERT_WEEKDAY_JST の曜日だけ。同じ週に 2 回は送らない (amazon_reimb_gap_alert_log。送る前に週の権利を取る = 同時に 2 つ走っても 1 通)。
 *     送れなかった週は、同じ週のうちは翌朝もう一度試す
 *   - 1 通にまとめる (上位 MAX_LIST 件 + 合計)。候補が無ければ送らない
 *   - 台帳・補てんの取得が古い (最後に取れた日が MAX_AGE_DAYS 日より前)・無い → 送らない (⚠️ の 1 行だけ)
 * 通知: env GCHAT_WEBHOOK (monitor-fee-coverage.js と同じ)。無ければ送らずに skip
 * 終了コード: 0 (候補があっても・送れなくても 0 = 朝の daily-sync を赤くしない)。warehouse.db が読めないときだけ 1
 *
 * 使い方:
 *   node apps/warehouse/monitor-amazon-reimb-gap.js --daily      # daily-sync から毎朝 (台帳と補てんの取得が両方成功した朝だけ。曜日でなければ何もしない)
 *   node apps/warehouse/monitor-amazon-reimb-gap.js --dry-run    # 文面を出すだけ (送らない・記録しない・曜日を問わない)
 *   node apps/warehouse/monitor-amazon-reimb-gap.js --json       # 結果を JSON で出す (送らない・記録しない)
 *   node apps/warehouse/monitor-amazon-reimb-gap.js --force      # 曜日と「今週は送った」を問わずに送る
 */
import 'dotenv/config';
import { addDays, jstToday, isDirectRun, ensureReportFetchDays } from './sp-api-report.js';
import { ensureLedgerTable, REPORT_TYPE as LEDGER_REPORT_TYPE } from './fetch-amazon-ledger.js';
import { ensureReimbursementsTable, REPORT_TYPE as REIMB_REPORT_TYPE } from './fetch-amazon-reimbursements.js';
import { weekKeyJst, jstWeekday, CLAIM_STALE_MS } from './alert-fba-aged-inventory.js';

export const ALERT_WEEKDAY_JST = 2;   // 0 = 日曜 … 2 = 火曜 (月曜の長期在庫アラートと重ねない)

export const RULES = Object.freeze({
  // 台帳の Reason (大文字にして比べる)。🚨 公式の一覧はセラーセントラルのヘルプ (ログインの内側) で確かめられていない = 第三者の資料の読み (M = 紛失・E = 倉庫で破損・F = 見つかった)
  LOST_REASONS: Object.freeze(['M']),
  DAMAGED_REASONS: Object.freeze(['E']),
  FOUND_REASONS: Object.freeze(['F']),
  // 補てんのレポートの reason (大文字小文字は問わない)
  REIMB_LOST_REASONS: Object.freeze(['Lost_Warehouse']),
  REIMB_DAMAGED_REASONS: Object.freeze(['Damaged_Warehouse']),
  WAIT_DAYS_LOST: 30,          // 紛失はこの日数まで待つ (見つかる・Amazon が補てんする。2025-01 時点の Amazon のフォーラムでは倉庫の紛失の自動の補てんは一時停止 = 手で申請)
  WAIT_DAYS_DAMAGED: 25,       // 破損はこの日数まで待つ (Amazon のフォーラム: 20 日以内に販売可に戻らなければ自動で補てん)
  CLAIM_DEADLINE_DAYS: 60,     // 申請の期限 = 在庫元帳に出てから 60 日以内 (Amazon のお知らせ「FBA自動補てんと補てん申請期間の変更について」)
  URGENT_DAYS_LEFT: 14,        // 期限まであと何日以内を 🔴 にするか
  LOOKBACK_DAYS: 120,          // 突き合わせに使う期間 (期限より長く見る = 古い紛失に当たる補てんを新しい紛失に当てない)
  MIN_UNITS: 1,                // FNSKU ごとの候補の個数の下限
  MIN_VALUE_YEN: 1000,         // 金額が分かるときの下限 (これより小さい候補は出さない)
  MAX_AGE_DAYS: 3,             // 台帳・補てんの最後に取れた日がこれより古ければ送らない
  MAX_LIST: 15,                // 1 通に載せる件数
  // Amazon の Unreconciled Quantity で候補の個数を削るか。🚨 意味が公式に確かめられていない・30 日を過ぎると更新されない = 既定は削らない (見せるだけ)
  TRUST_AMAZON_UNRECONCILED: false,
});
const GCHAT_MAX = 3900;

const DAY_MS = 86400000;
const daysBetween = (from, to) => Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / DAY_MS);
const fmt = (n) => Math.round(n).toLocaleString('ja-JP');
const up = (s) => String(s ?? '').trim().toUpperCase();
const tableExists = (db, name) => !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(name);

/** 週ごとの送信の記録。status = 'sending' / 'sent' / 'failed' (alert-fba-aged-inventory.js の fba_aged_alert_log と同じ意味) */
export function ensureGapAlertLog(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_reimb_gap_alert_log (
    week_key   TEXT PRIMARY KEY,
    status     TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    sent_at    TEXT,
    as_of      TEXT NOT NULL,     -- 判定した日 (JST)
    item_count INTEGER NOT NULL,
    units      INTEGER NOT NULL,
    value_yen  REAL               -- 見込みの金額の合計 (分かる分だけ)
  )`);
}

/**
 * その週の送信の権利を取る (確かめる → 印を付ける を 1 つの書き込みの取引で = 同時に走った 2 つ目は送らない)
 * @returns {{ ok: true } | { ok: false, status: 'already_sent'|'in_progress', row }}
 */
export function claimGapWeek(db, { week, asOf, itemCount, units, valueYen, nowMs, force = false }) {
  return db.transaction(() => {
    const row = db.prepare('SELECT status, claimed_at, sent_at FROM amazon_reimb_gap_alert_log WHERE week_key = ?').get(week);
    if (row && row.status === 'sending' && nowMs - Date.parse(row.claimed_at) < CLAIM_STALE_MS) return { ok: false, status: 'in_progress', row };
    if (row && row.status === 'sent' && !force) return { ok: false, status: 'already_sent', row };
    db.prepare(`INSERT INTO amazon_reimb_gap_alert_log (week_key, status, claimed_at, sent_at, as_of, item_count, units, value_yen) VALUES (?, 'sending', ?, NULL, ?, ?, ?, ?)
      ON CONFLICT(week_key) DO UPDATE SET status = 'sending', claimed_at = excluded.claimed_at, sent_at = NULL, as_of = excluded.as_of,
        item_count = excluded.item_count, units = excluded.units, value_yen = excluded.value_yen`)
      .run(week, new Date(nowMs).toISOString(), asOf, itemCount, units, valueYen);
    return { ok: true };
  }).immediate();
}

/** 台帳・補てんの取得の範囲 (sp_report_fetch_days)。取れた日が無ければ null */
export function fetchCoverage(db, reportType) {
  ensureReportFetchDays(db);
  const r = db.prepare('SELECT MIN(date_jst) AS f, MAX(date_jst) AS t FROM sp_report_fetch_days WHERE report_type = ?').get(reportType);
  return r && r.t ? { from: r.f, to: r.t } : null;
}

/** 補てんの行の種類: 'lost' / 'damaged' / 'any' (取り消しで元の種類が分からない) / null (倉庫の紛失・破損でない) */
export function reimbKind(row, rules = RULES) {
  const lost = new Set(rules.REIMB_LOST_REASONS.map(up));
  const dmg = new Set(rules.REIMB_DAMAGED_REASONS.map(up));
  const k = (v) => (lost.has(up(v)) ? 'lost' : dmg.has(up(v)) ? 'damaged' : null);
  const direct = k(row.reason);
  if (direct) return direct;
  if (/REVERS/.test(up(row.reason))) {   // Reimbursement_Reversal など = 元の補てんの種類で分ける
    const orig = k(row.original_reimbursement_type);
    if (orig) return orig;
    return row.original_reimbursement_type ? null : 'any';
  }
  return null;
}

/** 補てんの行の個数 (現金 + 在庫。取り消しは負) */
export function reimbUnits(row) {
  if (row.quantity_reimbursed_total !== null && row.quantity_reimbursed_total !== undefined) return Number(row.quantity_reimbursed_total) || 0;
  const c = row.quantity_reimbursed_cash, i = row.quantity_reimbursed_inventory;
  if ((c === null || c === undefined) && (i === null || i === undefined)) return 0;
  return (Number(c) || 0) + (Number(i) || 0);
}

/**
 * 1 つの FNSKU の出来事を古い順に突き合わせ、残った紛失・破損の塊を返す (純粋な関数。試験で直接呼ぶ)
 * @param {{ date: string, type: 'loss'|'restore'|'found'|'reimb', kind?: 'lost'|'damaged'|'any', qty: number, id?: string, origId?: string }[]} events  (id / origId = 補てんの ID と取り消された補てんの ID)
 * @returns {{ open: { date, kind, qty }[], foundSurplus: number }}
 */
export function matchEvents(events) {
  // 同じ日は 紛失 → 取り消しの戻し → 見つかった → 補てん → 補てんの取り消し の順 (同じ日の補てんとその取り消しを入力の順に頼らない。Codex R2 P2)
  const ORDER = { loss: 0, restore: 1, found: 2, reimb: 3, reversal: 4 };
  const typeOf = (e) => (e.type === 'reimb' && Number(e.qty) < 0 ? 'reversal' : e.type);
  const evs = [...events].sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : ORDER[typeOf(a)] - ORDER[typeOf(b)]));
  const open = [];             // 残っている塊 (古い順)
  const reimbursed = [];       // 補てんで消した塊 { date, kind, qty, id } (取り消しで戻す)
  const surplus = [];          // 消す相手が無かった「見つかった」「戻し」{ date, kind, qty } (補てんの後に来た分 = 後の取り消しで戻す塊を打ち消す)
  const consume = (n, pred) => {
    const taken = [];
    for (const p of open) {
      if (n <= 0) break;
      if (p.qty <= 0 || !pred(p)) continue;
      const q = Math.min(p.qty, n);
      p.qty -= q; n -= q;
      taken.push({ date: p.date, kind: p.kind, qty: q });
    }
    for (let i = open.length - 1; i >= 0; i--) if (open[i].qty <= 0) open.splice(i, 1);
    return { taken, rest: n };
  };
  const reopen = (piece) => {
    // 🚨 余りで打ち消せるのは、同じ種類で、その塊の発生日より後 (同じ日を含む) の余りだけ (前の見つかった分で後の紛失を消さない。Codex R2 P2)
    let q = piece.qty;
    for (const s of surplus) {
      if (q <= 0) break;
      if (s.qty <= 0 || s.kind !== piece.kind || s.date < piece.date) continue;
      const k = Math.min(s.qty, q); s.qty -= k; q -= k;
    }
    if (q <= 0) return;
    const i = open.findIndex((p) => p.date > piece.date);
    if (i < 0) open.push({ date: piece.date, kind: piece.kind, qty: q }); else open.splice(i, 0, { date: piece.date, kind: piece.kind, qty: q });
  };
  for (const e of evs) {
    const qty = Number(e.qty) || 0;
    if (qty === 0) continue;
    if (e.type === 'loss') { if (qty > 0) open.push({ date: e.date, kind: e.kind, qty }); continue; }
    if (e.type === 'restore' || e.type === 'found') {
      if (qty <= 0) continue;
      const kind = e.type === 'found' ? 'lost' : e.kind;   // 見つかった = 紛失だけを消す
      const { rest } = consume(qty, (p) => p.kind === kind);
      if (rest > 0) surplus.push({ date: e.date, kind, qty: rest });
      continue;
    }
    if (e.type === 'reimb') {
      if (qty > 0) {
        const a = consume(qty, (p) => e.kind === 'any' || p.kind === e.kind);
        const b = a.rest > 0 ? consume(a.rest, () => true) : { taken: [] };
        for (const p of [...a.taken, ...b.taken]) reimbursed.push({ ...p, id: e.id ?? null });
      } else {
        let n = -qty;
        // 戻す塊: 取り消された補てんの ID が分かれば、その補てんが消した塊だけ (無ければ戻さない = 台帳の外の紛失への補てん。Codex R2 P2)。
        //         分からなければ 同じ種類の補てんが消した塊 → どれでも。それぞれ新しい順
        const pickers = e.origId ? [(p) => p.id === e.origId] : [...(e.kind && e.kind !== 'any' ? [(p) => p.kind === e.kind] : []), () => true];
        for (const pick of pickers) {
          for (let i = reimbursed.length - 1; i >= 0 && n > 0; i--) {
            const p = reimbursed[i];
            if (!pick(p)) continue;
            const q = Math.min(p.qty, n);
            reopen({ date: p.date, kind: p.kind, qty: q });
            p.qty -= q; n -= q;
            if (p.qty <= 0) reimbursed.splice(i, 1);
          }
        }
      }
    }
  }
  return { open, foundSurplus: surplus.filter((x) => x.kind === 'lost').reduce((a, x) => a + x.qty, 0) };
}

/**
 * 補てんされていない紛失・破損の候補を出す (送った記録は見ない)
 * @returns {{ asOf, windowFrom, ledger, reimb, items: object[], pending: { items, units }, expired: { items, units, valueYen } }}
 */
export function evaluateReimbGap(db, { today = jstToday(), rules = RULES } = {}) {
  ensureLedgerTable(db);
  ensureReimbursementsTable(db);
  const ledger = fetchCoverage(db, LEDGER_REPORT_TYPE);
  const reimb = fetchCoverage(db, REIMB_REPORT_TYPE);
  const yesterday = addDays(today, -1);
  let windowFrom = addDays(today, -rules.LOOKBACK_DAYS);
  if (ledger && ledger.from > windowFrom) windowFrom = ledger.from;
  const lostSet = new Set(rules.LOST_REASONS.map(up)), dmgSet = new Set(rules.DAMAGED_REASONS.map(up)), foundSet = new Set(rules.FOUND_REASONS.map(up));
  const reasons = [...lostSet, ...dmgSet, ...foundSet];
  const rows = db.prepare(`SELECT id, event_date, fnsku, msku, asin, title, reason, quantity, unreconciled_quantity
    FROM amazon_inventory_ledger
    WHERE event_date >= ? AND event_date <= ? AND UPPER(TRIM(event_type)) = 'ADJUSTMENTS' AND UPPER(TRIM(COALESCE(reason, ''))) IN (${reasons.map(() => '?').join(', ')})
    ORDER BY event_date, id`).all(windowFrom, yesterday, ...reasons);
  const byFnsku = new Map();
  const info = (fnsku) => {
    let g = byFnsku.get(fnsku);
    if (!g) { g = { fnsku, msku: null, asin: null, title: null, events: [], lossRows: [] }; byFnsku.set(fnsku, g); }
    return g;
  };
  for (const r of rows) {
    const g = info(r.fnsku);
    if (r.msku) g.msku = r.msku;
    if (r.asin) g.asin = r.asin;
    if (r.title) g.title = r.title;
    const reason = up(r.reason);
    const q = Number(r.quantity) || 0;
    if (foundSet.has(reason)) { if (q > 0) g.events.push({ date: r.event_date, type: 'found', qty: q }); continue; }
    const kind = lostSet.has(reason) ? 'lost' : 'damaged';
    if (q < 0) {
      g.events.push({ date: r.event_date, type: 'loss', kind, qty: -q });
      g.lossRows.push({ date: r.event_date, kind, qty: -q, unrec: r.unreconciled_quantity === null || r.unreconciled_quantity === undefined ? null : Math.abs(Number(r.unreconciled_quantity)) });
    } else if (q > 0) g.events.push({ date: r.event_date, type: 'restore', kind, qty: q });
  }
  // 補てん。FNSKU が空なら SKU から台帳の FNSKU を引く (1 つに決まるときだけ)
  const skuToFnsku = new Map();
  for (const r of db.prepare(`SELECT DISTINCT fnsku, msku FROM amazon_inventory_ledger WHERE msku IS NOT NULL AND msku <> ''`).all()) {
    const k = r.msku.toLowerCase();
    skuToFnsku.set(k, skuToFnsku.has(k) && skuToFnsku.get(k) !== r.fnsku ? null : r.fnsku);
  }
  const reimbRows = db.prepare(`SELECT approval_date_jst, reimbursement_id, original_reimbursement_id, seller_sku, fnsku, reason, quantity_reimbursed_cash, quantity_reimbursed_inventory, quantity_reimbursed_total, original_reimbursement_type
    FROM amazon_fba_reimbursements WHERE approval_date_jst >= ? AND approval_date_jst <= ? ORDER BY approval_date_jst, approval_date, reimbursement_id`).all(windowFrom, today);
  for (const r of reimbRows) {
    const kind = reimbKind(r, rules);
    if (!kind) continue;
    const fnsku = r.fnsku || (r.seller_sku ? skuToFnsku.get(r.seller_sku.toLowerCase()) : null);
    if (!fnsku || !byFnsku.has(fnsku)) continue;   // 期間に紛失・破損の無い FNSKU への補てん = 突き合わせる相手が無い
    const qty = reimbUnits(r);
    if (qty) byFnsku.get(fnsku).events.push({ date: r.approval_date_jst, type: 'reimb', kind, qty, id: r.reimbursement_id, origId: r.original_reimbursement_id || null });
  }

  // 単価: ① 最後の補てんの単価 (FNSKU → SKU) ② 在庫計画の売価
  const lastReimbBy = (col) => db.prepare(`SELECT amount_per_unit FROM amazon_fba_reimbursements
    WHERE ${col} = ? ${col === 'seller_sku' ? 'COLLATE NOCASE' : ''} AND amount_per_unit > 0 AND (currency_unit IS NULL OR UPPER(currency_unit) = 'JPY')
    ORDER BY approval_date_jst DESC, reimbursement_id DESC LIMIT 1`);
  const byFn = lastReimbBy('fnsku'), bySku = lastReimbBy('seller_sku');
  const hasPlanning = tableExists(db, 'fba_planning_daily');
  const priceOf = hasPlanning ? db.prepare(`SELECT your_price FROM fba_planning_daily WHERE market = 'jp' AND amazon_sku = ? COLLATE NOCASE AND your_price > 0
    ORDER BY snapshot_date DESC LIMIT 1`) : null;
  const unitPrice = (g) => {
    const a = byFn.get(g.fnsku) || (g.msku ? bySku.get(g.msku) : null);
    if (a) return { unitPrice: Number(a.amount_per_unit), priceSource: 'reimb' };
    const p = priceOf && g.msku ? priceOf.get(g.msku) : null;
    if (p) return { unitPrice: Number(p.your_price), priceSource: 'price' };
    return { unitPrice: null, priceSource: null };
  };

  const items = [];
  const pending = { items: 0, units: 0 };
  const expired = { items: 0, units: 0, valueYen: 0 };
  for (const g of byFnsku.values()) {
    const { open } = matchEvents(g.events);
    if (!open.length) continue;
    const waitOf = (k) => (k === 'lost' ? rules.WAIT_DAYS_LOST : rules.WAIT_DAYS_DAMAGED);
    let claim = [];
    const wait = [], gone = [];
    for (const p of open) {
      const age = daysBetween(p.date, today);
      const daysLeft = rules.CLAIM_DEADLINE_DAYS - age;
      if (daysLeft < 0) gone.push(p);
      else if (age < waitOf(p.kind)) wait.push(p);
      else claim.push({ ...p, daysLeft });
    }
    const sum = (xs) => xs.reduce((s, p) => s + p.qty, 0);
    const price = unitPrice(g);
    if (wait.length) { pending.items++; pending.units += sum(wait); }
    if (gone.length) { expired.items++; expired.units += sum(gone); if (price.unitPrice) expired.valueYen += sum(gone) * price.unitPrice; }
    if (!claim.length) continue;
    // Amazon の未照合 (見せるだけ。TRUST_AMAZON_UNRECONCILED なら個数をそこまで削る): 候補の期間 (待つ日数を過ぎ・期限の前) の紛失・破損の行の合計
    const inRange = g.lossRows.filter((r) => { const age = daysBetween(r.date, today); return age >= waitOf(r.kind) && age <= rules.CLAIM_DEADLINE_DAYS; });
    const amazonUnreconciled = inRange.length && inRange.every((r) => r.unrec !== null) ? inRange.reduce((s, r) => s + r.unrec, 0) : null;
    if (rules.TRUST_AMAZON_UNRECONCILED && amazonUnreconciled !== null && sum(claim) > amazonUnreconciled) {
      // 塊そのものを削る (新しい方から) = 紛失・破損の内訳・期限・金額も削った後の塊から出す (Codex R1 P2)
      let keep = amazonUnreconciled;
      claim = claim.map((p) => { const q = Math.min(p.qty, keep); keep -= q; return { ...p, qty: q }; }).filter((p) => p.qty > 0);
      if (!claim.length) continue;
    }
    const units = sum(claim);
    if (units < rules.MIN_UNITS) continue;
    const value = price.unitPrice ? units * price.unitPrice : null;
    if (value !== null && value < rules.MIN_VALUE_YEN) continue;
    const daysLeft = Math.min(...claim.map((p) => p.daysLeft));
    items.push({
      fnsku: g.fnsku, msku: g.msku, asin: g.asin, title: g.title, units,
      lostUnits: sum(claim.filter((p) => p.kind === 'lost')), damagedUnits: sum(claim.filter((p) => p.kind === 'damaged')),
      firstDate: claim[0].date, deadline: addDays(claim[0].date, rules.CLAIM_DEADLINE_DAYS),   // open は古い順 = claim[0] がいちばん古い
      daysLeft, urgent: daysLeft <= rules.URGENT_DAYS_LEFT, ...price, value, amazonUnreconciled,
    });
  }
  items.sort((a, b) => (b.value ?? -1) - (a.value ?? -1) || a.daysLeft - b.daysLeft || b.units - a.units || (a.fnsku < b.fnsku ? -1 : 1));
  return { asOf: today, windowFrom, ledger, reimb, items, pending, expired };
}

/** 1 通の文面 (候補が無ければ null) */
export function buildGapMessage(result, { rules = RULES } = {}) {
  const { items, asOf, pending, expired, ledger } = result;
  if (!items.length) return null;
  const units = items.reduce((s, c) => s + c.units, 0);
  const known = items.filter((c) => c.value !== null);
  const urgent = items.filter((c) => c.urgent);
  let msg = `💴 *Amazon 補てん漏れの候補 (週 1 回)* ${asOf} 時点\n`;
  msg += `FBA の倉庫で紛失・破損し、まだ補てんされていない: ${items.length} 件 / 計 ${fmt(units)} 個`;
  if (known.length) msg += ` / 見込み 約 ¥${fmt(known.reduce((s, c) => s + c.value, 0))}${known.length < items.length ? ` (金額が分かる ${known.length} 件)` : ''}`;
  if (urgent.length) msg += `\n🔴 申請の期限まで ${rules.URGENT_DAYS_LEFT} 日以内: ${urgent.length} 件`;
  msg += `\n`;
  const lines = items.slice(0, rules.MAX_LIST).map((c) => {
    const what = [c.lostUnits ? `紛失 ${c.lostUnits}` : null, c.damagedUnits ? `破損 ${c.damagedUnits}` : null].filter(Boolean).join('・');
    const parts = [`${what} 個 (${c.firstDate}〜)`];
    if (c.value !== null) parts.push(`¥${fmt(c.value)} (${c.priceSource === 'reimb' ? '前の補てんの単価' : '売価'} ¥${fmt(c.unitPrice)})`);
    parts.push(`期限 ${c.deadline} (あと ${c.daysLeft} 日)`);
    if (c.amazonUnreconciled !== null) parts.push(`Amazon の未照合 ${c.amazonUnreconciled}`);
    const name = c.title ? ` ${String(c.title).slice(0, 24)}` : '';
    return `• ${c.urgent ? '🔴 ' : ''}${c.msku || '(SKU 不明)'} (${c.fnsku})${name} — ${parts.join(' / ')}`;
  });
  msg += `\n${lines.join('\n')}`;
  if (items.length > rules.MAX_LIST) msg += `\n… ほか ${items.length - rules.MAX_LIST} 件`;
  msg += '\n';
  if (pending.items) msg += `\n⏳ 待っている (発生から紛失 ${rules.WAIT_DAYS_LOST} 日・破損 ${rules.WAIT_DAYS_DAMAGED} 日の前): ${pending.items} 件 ${fmt(pending.units)} 個`;
  if (expired.items) msg += `\n⌛ 期限 (発生から ${rules.CLAIM_DEADLINE_DAYS} 日) を過ぎた可能性: ${expired.items} 件 ${fmt(expired.units)} 個${expired.valueYen ? ` (約 ¥${fmt(expired.valueYen)})` : ''}`;
  msg += `\n申請: セラーセントラル「在庫の問題と補てん」(在庫元帳に出てから ${rules.CLAIM_DEADLINE_DAYS} 日以内)。単価は目安 (補てんは仕入れ原価が基準)`;
  msg += `\n判定: 在庫元帳の紛失 (${rules.LOST_REASONS.join('/')})・破損 (${rules.DAMAGED_REASONS.join('/')}) − 見つかった (${rules.FOUND_REASONS.join('/')}) − 補てん (現金・在庫)${ledger ? `。台帳は ${ledger.from} から` : ''} (warehouse.db amazon_inventory_ledger / amazon_fba_reimbursements)`;
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
 * 本体 (試験で db・時刻・送り手を差し替えられる)
 * @returns {{ status: string, lastLine: string, message?: string, result?: object }}
 */
export async function runReimbGapAlert(db, { nowMs = Date.now(), webhook = process.env.GCHAT_WEBHOOK, force = false, dryRun = false, rules = RULES, send = sendGChat, log = console.log } = {}) {
  ensureGapAlertLog(db);
  const today = jstToday(nowMs);
  const week = weekKeyJst(nowMs);
  const L = '補てん漏れアラート';
  // 送る曜日でなくても、同じ週に送れなかった回 ('failed')・送っている途中で落ちた回 (古い 'sending') があれば、もう一度試す
  const wk = db.prepare('SELECT status, claimed_at FROM amazon_reimb_gap_alert_log WHERE week_key = ?').get(week);
  const retryFailed = !!wk && (wk.status === 'failed' || (wk.status === 'sending' && nowMs - Date.parse(wk.claimed_at) >= CLAIM_STALE_MS));
  if (!force && !dryRun && !retryFailed && jstWeekday(nowMs) !== ALERT_WEEKDAY_JST) return { status: 'not_today', lastLine: `⏭️ ${L}: 送る曜日ではない (${'日月火水木金土'[ALERT_WEEKDAY_JST]}曜だけ)` };
  if (!force && !dryRun) {
    const sent = db.prepare(`SELECT sent_at FROM amazon_reimb_gap_alert_log WHERE week_key = ? AND status = 'sent'`).get(week);
    if (sent) return { status: 'already_sent', lastLine: `⏭️ ${L}: 今週 (${week}〜) は送った (${sent.sent_at})` };
  }
  const result = evaluateReimbGap(db, { today, rules });
  if (!result.ledger || !result.reimb) return { status: 'no_data', result, lastLine: `⚠️ ${L}: ${!result.ledger ? '在庫の台帳' : '補てん'}の取得の記録がまだ無い = 送らない` };
  const staleBefore = addDays(today, -rules.MAX_AGE_DAYS);
  const stale = [['在庫の台帳', result.ledger], ['補てん', result.reimb]].filter(([, c]) => c.to < staleBefore);
  if (stale.length) return { status: 'stale', result, lastLine: `⚠️ ${L}: ${stale.map(([n, c]) => `${n}の取得が古い (最後 ${c.to})`).join('・')} = 送らない` };
  const units = result.items.reduce((s, c) => s + c.units, 0);
  const known = result.items.filter((c) => c.value !== null);
  const valueYen = known.length ? known.reduce((s, c) => s + c.value, 0) : null;
  const summary = `候補 ${result.items.length} 件 ${units} 個${valueYen !== null ? `・約 ¥${fmt(valueYen)}` : ''}・待っている ${result.pending.items} 件・期限を過ぎた可能性 ${result.expired.items} 件`;
  if (!result.items.length) return { status: 'none', result, lastLine: `✅ ${L}: 候補なし (${today}・待っている ${result.pending.items} 件・期限を過ぎた可能性 ${result.expired.items} 件)` };
  const message = buildGapMessage(result, { rules });
  log(message);
  if (dryRun) return { status: 'dry_run', result, message, lastLine: `ℹ️ ${L}: --dry-run (送らない) ${summary}` };
  if (!webhook) return { status: 'no_webhook', result, message, lastLine: `⚠️ ${L}: GCHAT_WEBHOOK 未設定のため送らない (${summary})` };
  const claim = claimGapWeek(db, { week, asOf: today, itemCount: result.items.length, units, valueYen, nowMs, force });
  if (!claim.ok) return { status: claim.status, result, lastLine: claim.status === 'in_progress' ? `⏭️ ${L}: ほかの実行が送っている途中 (${claim.row.claimed_at})` : `⏭️ ${L}: 今週 (${week}〜) は送った (${claim.row.sent_at})` };
  let ok = false;
  try { ok = await send(webhook, message); } catch (e) { console.error(`[GChat] 通知失敗: ${e.message}`); ok = false; }
  if (!ok) {
    db.prepare(`UPDATE amazon_reimb_gap_alert_log SET status = 'failed' WHERE week_key = ? AND status = 'sending'`).run(week);
    return { status: 'send_failed', result, message, lastLine: `⚠️ ${L}: GChat に送れなかった (${summary}。明日の朝にもう一度試す)` };
  }
  db.prepare(`UPDATE amazon_reimb_gap_alert_log SET status = 'sent', sent_at = ? WHERE week_key = ?`).run(new Date(nowMs).toISOString(), week);
  return { status: 'sent', result, message, lastLine: `✅ ${L}: 送った (${summary})` };
}

async function main() {
  const { initDB, getDB } = await import('./db.js');
  await initDB();
  const db = getDB();
  const argv = process.argv.slice(2);
  if (argv.includes('--json')) {
    const result = evaluateReimbGap(db);
    console.log(JSON.stringify({ ...result, message: buildGapMessage(result) }, null, 2));
    return;
  }
  const r = await runReimbGapAlert(db, { force: argv.includes('--force'), dryRun: argv.includes('--dry-run') });
  console.log(r.lastLine);
}

if (await isDirectRun(import.meta.url)) {
  // GChat の fetch の直後に process.exit() しない (Windows の libuv assertion。monitor-fee-coverage.js と同じ) = exitCode + 自然終了
  main().then(() => { if (process.exitCode == null) process.exitCode = 0; }).catch((e) => {
    console.log(`❌ 補てん漏れアラート: ${String(e.message).replace(/\s+/g, ' ').slice(0, 300)}`);
    process.exitCode = 1;
  });
}
