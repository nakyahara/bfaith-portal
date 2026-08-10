/**
 * au PAY マーケット 未発送アラート — 検知ロジック
 *
 * 目的 (楽天版 apps/rakuten-unshipped / Yahoo版 apps/yahoo-unshipped と同じ):
 *   弊社は12時締め。前日12:00までに発送できる状態になっていた注文は当日中に出荷されているはず。
 *   翌朝まだ未発送で残っていたら出荷漏れなので朝イチで通知する。
 *
 * ⭐️auPAY 固有の事情 (2026-08-10 に本番データで実測して確定):
 *   1. 🚨**payment_status は「入金済み」の意味ではない**。実データでは発送済み注文が Y、
 *      発送待ち注文が N になっており、**発送後に Y へ変わる**(売上確定) 挙動。
 *      入金の判定に使ってはいけない。
 *   2. 代わりに **order_status が状態をそのまま持っている**:
 *        「発送前入金待ち」= 前払いの入金待ち (発送できなくて当然 → 対象外)
 *        「発送待ち」      = 発送できる状態 (楽天の orderProgress=300 相当) ← これが対象
 *        「完了」          = 発送済み
 *        「キャンセル」    = 対象外
 *   3. **入金確認の時刻を持つフィールドが無い** (Yahoo と同じ)。注文日時 (order_date) で判定する。
 *      前払い (ATM決済・銀行ネット決済) は入金が遅れると誤検知しうるが、実測で90日に10件と
 *      ごく少数 (主体は au PAY かんたん決済 = 注文時に決済確定) なので実用上の影響は小さい。
 *      通知には決済方法と**発送期限 (shipping_timelimit_date)** を出して人が判断できるようにする。
 *   4. 🚨**DBだけで判定してはいけない**。daily-sync の auPAY 同期は直近7日窓なので、
 *      それより古い注文の状態は更新されない。実測では DB候補7件が全て 2020〜2021年の
 *      バックフィル分で、現在の状態は不明だった。API再確認は必須。
 *   5. auPAY の受注APIは**期間検索しかできない** (注文ID指定が無い)。
 *      候補の**注文日をピンポイントで検索**して最新状態を引く (1日=1リクエスト・0.9秒間隔)。
 *
 * 個人情報:
 *   注文者情報 (ordererName 等) は読まない・出さない。扱うのは注文ID・日時・金額・商品名のみ。
 */
import fs from 'fs';
import path from 'path';
import { parseStringPromise } from 'xml2js';
import { getDB } from '../warehouse/db.js';
import { jstDateStr } from '../../lib/jst-date.js';

/** 出荷の締め時刻 (時)。前日のこの時刻までに発送できる状態だった注文が対象 */
export const DEFAULT_CUTOFF_HOUR = 12;
/** DBから候補を拾う期間 (日) */
export const DEFAULT_SEARCH_DAYS = 180;
/** API で最新確認する「注文日」の上限 (1日1リクエスト・0.9秒間隔なので暴走を防ぐ) */
export const MAX_VERIFY_DAYS = 20;
/** 解消済みキャッシュの保持日数 */
export const RESOLVED_KEEP_DAYS = 200;
/** 解消済みでも、この日数が過ぎたら念のためもう一度APIで確認する */
export const RESOLVED_RECHECK_DAYS = 7;
/** API 呼び出しの間隔 (aupay-orders.js と同じペース) */
export const API_INTERVAL_MS = 900;

/** 発送すべき状態を表す order_status。ここが「発送待ち」以外なら対象外 */
export const READY_TO_SHIP_STATUS = '発送待ち';

const pad2 = n => String(n).padStart(2, '0');

/** 'YYYY-MM-DD' に日数を加算する (Date のローカルTZに依存しない) */
export function addDaysStr(ymd, delta) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd));
  if (!m) throw new Error(`addDaysStr: 日付形式が不正 (${ymd})`);
  const t = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) + delta * 86400000;
  const d = new Date(t);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

/**
 * auPAY の日時文字列 ('2026/08/09 12:34' / '2026/08/09') を Date にする。
 * ⚠️`new Date('2026/08/09 12:34')` は実行環境のローカルTZ依存なので使わない。
 *   JST 固定で組み立てる。
 */
export function parseAupayDatetime(s) {
  const m = /^(\d{4})\/(\d{2})\/(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/.exec(String(s || '').trim());
  if (!m) return null;
  const iso = `${m[1]}-${m[2]}-${m[3]}T${m[4] || '00'}:${m[5] || '00'}:${m[6] || '00'}+09:00`;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** 'YYYY/MM/DD HH:MM' → 'YYYYMMDD' (APIの検索キー) */
export function toApiYmd(orderDate) {
  const m = /^(\d{4})\/(\d{2})\/(\d{2})/.exec(String(orderDate || '').trim());
  return m ? `${m[1]}${m[2]}${m[3]}` : null;
}

/**
 * 判定の基準時刻をまとめて作る。
 * cutoffStr は DB の order_date と同じ 'YYYY/MM/DD HH:MM' 形式にする (桁が揃うので文字列比較でよい)。
 */
export function buildContext(now = new Date(), cutoffHour = DEFAULT_CUTOFF_HOUR) {
  const today = jstDateStr(now);
  const cutoffDate = addDaysStr(today, -1);
  const cutoffStr = `${cutoffDate.replace(/-/g, '/')} ${pad2(cutoffHour)}:00`;
  const cutoff = new Date(`${cutoffDate}T${pad2(cutoffHour)}:00:00+09:00`);
  return { today, cutoffDate, cutoff, cutoffHour, cutoffStr };
}

// ─── 解消済みキャッシュ ───
// DBは7日窓でしか更新されないので、古い注文は発送済みになっても DB 上は「発送待ち」のまま残る。
// API確認で解消と分かった注文を記録して次回の候補から外す。
// ⚠️auPAY には Yahoo の LastUpdateTime に相当するフィールドが無いので、
//   「確認から RESOLVED_RECHECK_DAYS 日経ったら再確認」だけで巻き戻り (発送取消など) に備える。

function dataDir() {
  return process.env.DATA_DIR || path.join(process.cwd(), 'data');
}

export function resolvedFilePath() {
  return path.join(dataDir(), 'aupay-unshipped-resolved.json');
}

/** @returns {Record<string,{resolvedAt:string}>} orderId → 解消を確認した日 */
export function loadResolved() {
  try {
    const obj = JSON.parse(fs.readFileSync(resolvedFilePath(), 'utf-8'));
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return {};
    const out = {};
    for (const [id, v] of Object.entries(obj)) {
      if (typeof v === 'string') out[id] = { resolvedAt: v };
      else if (v && typeof v === 'object' && typeof v.resolvedAt === 'string') out[id] = { resolvedAt: v.resolvedAt };
    }
    return out;
  } catch {
    // 無い/壊れている場合は空で続行 (キャッシュは最適化。判定の正しさには影響しない)
    return {};
  }
}

/** 古い記録を落として保存する。一時ファイル → rename で壊れたJSONを残さない */
export function saveResolved(map, today) {
  const cutoff = addDaysStr(today, -RESOLVED_KEEP_DAYS);
  const pruned = {};
  for (const [id, v] of Object.entries(map)) {
    const rec = typeof v === 'string' ? { resolvedAt: v } : v;
    if (rec && typeof rec.resolvedAt === 'string' && rec.resolvedAt >= cutoff) pruned[id] = { resolvedAt: rec.resolvedAt };
  }
  const file = resolvedFilePath();
  const tmp = `${file}.tmp`;
  try {
    const dir = dataDir();
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(tmp, JSON.stringify(pruned, null, 2));
    fs.renameSync(tmp, file);
    return { ok: true, kept: Object.keys(pruned).length };
  } catch (e) {
    try { fs.unlinkSync(tmp); } catch { /* 消せなくても次回上書きされる */ }
    console.warn(`[aupay-unshipped] 解消済みキャッシュを保存できませんでした: ${e.message}`);
    return { ok: false, error: e.message };
  }
}

/**
 * キャッシュを見て、この候補をスキップしてよいか。
 * ⚠️「一度解消したら永久に無視」にはしない。発送取消などで未発送に戻ることがあるため、
 *   確認から RESOLVED_RECHECK_DAYS 日過ぎたら必ずもう一度APIで見る。
 */
export function shouldSkipByCache(row, resolved, today) {
  const rec = resolved?.[row.order_id];
  if (!rec?.resolvedAt) return false;
  if (today && today >= addDaysStr(rec.resolvedAt, RESOLVED_RECHECK_DAYS)) return false;
  return true;
}

// ─── ① warehouse.db から候補を絞る ───

/**
 * 「発送できる状態なのに未発送」の候補をDBから拾う。
 * ⚠️条件は注文単位に集約してから適用する (明細ごとに状態が食い違った場合に、都合の悪い明細だけ
 *   落としてから集約すると古い状態のまま候補化してしまう)。
 * ⚠️ship_status は MIN を取る = 1件でも 'N' (未発送) の明細があれば候補に入れる (fail-safe)。
 *   確定は必ず API 側で行うので、ここで拾いすぎる分には害が無い。
 */
export function findCandidates(ctx, searchDays = DEFAULT_SEARCH_DAYS, resolved = {}) {
  const since = `${addDaysStr(ctx.today, -searchDays).replace(/-/g, '/')} 00:00`;
  const db = getDB();
  const rows = db.prepare(`
    SELECT * FROM (
      SELECT order_id,
             MIN(order_date)              AS order_date,
             MAX(order_status)            AS order_status,
             MIN(ship_status)             AS ship_status,
             MAX(cancel_status)           AS cancel_status,
             MAX(payment_status)          AS payment_status,
             MAX(settlement_name)         AS settlement_name,
             MAX(total_price)             AS total_price,
             MIN(shipping_timelimit_date) AS shipping_timelimit_date,
             COUNT(*)                     AS item_count,
             MAX(item_name)               AS item_name
        FROM raw_aupay_orders
       WHERE order_date >= ? AND order_date <= ?
       GROUP BY order_id
    )
    WHERE order_status = ?     -- 発送待ち = 発送できる状態
      AND ship_status  = 'N'   -- 未発送
      AND cancel_status <> 'C' -- キャンセル以外
    ORDER BY order_date
  `).all(since, ctx.cutoffStr, READY_TO_SHIP_STATUS);

  // 過去にAPIで「解消済み」と確認した注文は、再確認の期限が来るまで除外する。
  // 再確認組は新規候補の後ろに置く (先頭に混ざると確認枠を食い、新しい出荷漏れに到達できない)
  const fresh = [];
  const recheck = [];
  for (const r of rows) {
    if (!resolved[r.order_id]) { fresh.push(r); continue; }
    if (!shouldSkipByCache(r, resolved, ctx.today)) recheck.push(r);
  }
  return [...fresh, ...recheck];
}

// ─── ② 候補をAPIで最新確認 ───

function proxyConfig() {
  const base = process.env.AUPAY_PROXY_URL || 'http://133.167.122.198:8080';
  const secret = process.env.AUPAY_PROXY_SECRET || '';
  const shopId = process.env.AUPAY_SHOP_ID || '54318092';
  if (!secret) throw new Error('AUPAY_PROXY_SECRET が未設定です');
  return { base, secret, shopId };
}

const strVal = v => ((v && typeof v === 'object') ? (v._ ?? '') : (v ?? '')) + '';
const arrify = v => (v == null ? [] : (Array.isArray(v) ? v : [v]));

/** APIレスポンス(1注文)から必要な項目だけ取り出す (個人情報は触らない) */
export function extractOrderInfo(o) {
  if (!o || typeof o !== 'object') return null;
  const orderId = strVal(o.orderId);
  if (!orderId) return null;
  const details = arrify(o.detail).filter(Boolean);
  return {
    orderId,
    orderDate: strVal(o.orderDate),
    orderStatus: strVal(o.orderStatus),
    shipStatus: strVal(o.shipStatus),
    cancelStatus: strVal(o.cancelStatus),
    paymentStatus: strVal(o.paymentStatus),
    settlementName: strVal(o.settlementName),
    totalPrice: Number(strVal(o.totalPrice)) || 0,
    itemCount: details.length,
    firstItemName: strVal(details[0]?.itemName),
    shippingTimelimitDate: strVal(details[0]?.shippingTimelimitDate),
  };
}

/**
 * 最新状態でも「発送待ち × 未発送 × キャンセルでない」か。
 * DBが古いだけの注文 (すでに発送済み・キャンセル済み) をここで落とす。
 */
export function isStillUnshipped(info) {
  if (!info) return false;
  return info.orderStatus === READY_TO_SHIP_STATUS
    && info.shipStatus === 'N'
    && info.cancelStatus !== 'C';
}

/** 指定日の注文一覧を取る (ページング込み) */
async function fetchOrdersOfDay(ymd, { fetchImpl = fetch, sleepImpl } = {}) {
  const { base, secret, shopId } = proxyConfig();
  const PAGE_SIZE = 100;
  const out = [];
  let startCount = 1;
  for (let page = 1; ; page++) {
    const qs = new URLSearchParams({
      shopId, totalCount: String(PAGE_SIZE), startCount: String(startCount),
      startDate: ymd, endDate: ymd, dateType: '0', // dateType=0 = 受注日
    });
    const res = await fetchImpl(`${base}/wmshopapi/searchTradeInfoListProc?${qs}`, {
      headers: { 'X-Proxy-Secret': secret },
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`auPAYプロキシ ${res.status}: ${body.slice(0, 200)}`);
    }
    const parsed = await parseStringPromise(await res.text(), { explicitArray: false });
    const resp = parsed?.response;
    if (!resp || resp.result?.status !== '0') {
      const err = resp?.result?.error;
      throw new Error(`auPAY API error: code=${strVal(err?.code) || '?'} msg=${strVal(err?.message) || 'unknown'}`);
    }
    const list = arrify(resp.orderInfo).filter(Boolean);
    out.push(...list);
    const cnt = Number(resp.resultCount) || 0;
    if (cnt < PAGE_SIZE || list.length === 0) break;
    startCount += PAGE_SIZE;
    if (page >= 20) { // 1日で2,000件を超えることは無い想定。無限ループの保険
      console.warn(`[aupay-unshipped] ${ymd} のページ数が想定を超えたため打ち切りました`);
      break;
    }
    if (sleepImpl) await sleepImpl(API_INTERVAL_MS);
  }
  return out;
}

/**
 * 候補を API で最新確認する。auPAY は注文ID指定の取得ができないので、
 * **候補の注文日をピンポイントで検索**して引き当てる。
 * @returns {Promise<{alerts:object[], resolvedIds:string[], verifiedDays:number, apiFailed:number, truncated:boolean}>}
 */
export async function verifyCandidates(candidates, ctx, opts = {}) {
  const maxDays = Number.isFinite(opts.maxVerifyDays) ? opts.maxVerifyDays : MAX_VERIFY_DAYS;
  const fetchImpl = opts.fetchImpl || fetch;
  const sleepImpl = opts.sleepImpl || (ms => new Promise(r => setTimeout(r, ms)));

  if (candidates.length === 0) {
    return { alerts: [], resolvedIds: [], verifiedDays: 0, apiFailed: 0, truncated: false };
  }

  // 注文日ごとにまとめる (同じ日の候補は1リクエストで確認できる)
  const byDay = new Map();
  for (const c of candidates) {
    const ymd = toApiYmd(c.order_date);
    if (!ymd) continue; // 日付が壊れている行は確認できない (下で apiFailed に数える)
    if (!byDay.has(ymd)) byDay.set(ymd, []);
    byDay.get(ymd).push(c);
  }
  const days = [...byDay.keys()].sort();
  const truncated = days.length > maxDays;
  const targetDays = truncated ? days.slice(0, maxDays) : days;

  const alerts = [];
  const resolvedIds = [];
  let apiFailed = 0;
  // 日付が壊れていて確認できなかった候補
  apiFailed += candidates.filter(c => !toApiYmd(c.order_date)).length;

  for (let i = 0; i < targetDays.length; i++) {
    const ymd = targetDays[i];
    let list;
    try {
      list = await fetchOrdersOfDay(ymd, { fetchImpl, sleepImpl });
    } catch (e) {
      // 1日分の取得に失敗しても他の日は続ける (その日の候補は「確認できず」)
      console.warn(`[aupay-unshipped] ${ymd} の取得に失敗: ${e.message}`);
      apiFailed += byDay.get(ymd).length;
      continue;
    } finally {
      if (i < targetDays.length - 1) await sleepImpl(API_INTERVAL_MS);
    }

    const byId = new Map();
    for (const o of list) {
      const info = extractOrderInfo(o);
      if (info) byId.set(info.orderId, info);
    }
    for (const c of byDay.get(ymd)) {
      const info = byId.get(c.order_id);
      if (!info) { apiFailed++; continue; }
      if (!isStillUnshipped(info)) { resolvedIds.push(c.order_id); continue; }
      const orderedAt = parseAupayDatetime(info.orderDate) || parseAupayDatetime(c.order_date);
      const limitAt = parseAupayDatetime(info.shippingTimelimitDate || c.shipping_timelimit_date);
      alerts.push({
        orderId: c.order_id,
        orderedAt,
        orderDateRaw: info.orderDate || c.order_date,
        elapsedHours: orderedAt ? Math.round((ctx.now.getTime() - orderedAt.getTime()) / 36e5 * 10) / 10 : null,
        orderStatus: info.orderStatus,
        settlementName: info.settlementName || c.settlement_name || '(不明)',
        totalPrice: info.totalPrice || Number(c.total_price) || 0,
        itemCount: info.itemCount || Number(c.item_count) || 0,
        firstItemName: info.firstItemName || c.item_name || '',
        shippingTimelimit: info.shippingTimelimitDate || c.shipping_timelimit_date || '',
        // 発送期限を過ぎている日数 (マイナス = まだ猶予がある)
        limitOverdueDays: limitAt ? Math.floor((ctx.now.getTime() - limitAt.getTime()) / 86400000) : null,
      });
    }
  }

  alerts.sort((a, b) => (a.orderedAt?.getTime() ?? 0) - (b.orderedAt?.getTime() ?? 0));
  return { alerts, resolvedIds, verifiedDays: targetDays.length, apiFailed, truncated };
}

/**
 * 検知本体。
 * @param {{now?:Date, cutoffHour?:number, searchDays?:number, maxVerifyDays?:number, skipCacheWrite?:boolean}} opts
 */
export async function findUnshippedOrders(opts = {}) {
  const now = opts.now || new Date();
  const cutoffHour = Number.isFinite(opts.cutoffHour) ? opts.cutoffHour : DEFAULT_CUTOFF_HOUR;
  const searchDays = Number.isFinite(opts.searchDays) ? opts.searchDays : DEFAULT_SEARCH_DAYS;

  const base = buildContext(now, cutoffHour);
  const ctx = { ...base, now };

  const resolved = loadResolved();
  const candidates = findCandidates(ctx, searchDays, resolved);
  console.log(`[aupay-unshipped] DB候補: ${candidates.length}件`
    + ` (${searchDays}日以内・締め=${ctx.cutoffStr}・解消済み除外=${Object.keys(resolved).length}件)`);

  const r = await verifyCandidates(candidates, ctx, { maxVerifyDays: opts.maxVerifyDays });
  console.log(`[aupay-unshipped] API確認: ${r.verifiedDays}日分 → 未発送 ${r.alerts.length}件`
    + (r.resolvedIds.length ? ` / 解消済み ${r.resolvedIds.length}件` : '')
    + (r.apiFailed ? ` / 確認できず ${r.apiFailed}件` : ''));

  if (!opts.skipCacheWrite && r.resolvedIds.length > 0) {
    for (const id of r.resolvedIds) resolved[id] = { resolvedAt: ctx.today };
    saveResolved(resolved, ctx.today);
  }

  return { ...r, candidates: candidates.length, ctx };
}

// ─── GChat 本文 ───

/** 表示上限。超えた分は「他N件」と明示する (黙って切らない) */
export const MAX_LINES = 30;

function fmtJst(d) {
  if (!d) return '-';
  const t = new Date(d.getTime() + 9 * 3600 * 1000);
  return `${t.getUTCMonth() + 1}/${t.getUTCDate()} ${pad2(t.getUTCHours())}:${pad2(t.getUTCMinutes())}`;
}

const yen = n => `¥${Number(n || 0).toLocaleString('ja-JP')}`;

function truncate(s, n) {
  const str = String(s || '');
  return str.length > n ? `${str.slice(0, n)}…` : str;
}

function itemLabel(o) {
  if (!o.firstItemName) return '(商品名なし)';
  const head = truncate(o.firstItemName, 40);
  return o.itemCount > 1 ? `${head} ほか${o.itemCount - 1}点` : head;
}

/** 発送期限の表示 (過ぎていれば超過日数を出す) */
function limitLabel(o) {
  if (!o.shippingTimelimit) return '';
  if (o.limitOverdueDays === null || o.limitOverdueDays === undefined) return ` / 発送期限 ${o.shippingTimelimit}`;
  if (o.limitOverdueDays > 0) return ` / 発送期限 ${o.shippingTimelimit} (${o.limitOverdueDays}日超過)`;
  return ` / 発送期限 ${o.shippingTimelimit}`;
}

/**
 * GChat へ送る本文を組み立てる。
 * 0件の日も送る = 通知が来ないこと自体が「止まった」のサインになる。
 */
export function buildMessage({ alerts, candidates, apiFailed, truncated, ctx }) {
  const cutoffLabel = `${ctx.cutoffDate.slice(5).replace('-', '/')} ${pad2(ctx.cutoffHour ?? DEFAULT_CUTOFF_HOUR)}:00`;
  const lines = [];
  lines.push('*au PAY 未発送アラート*');
  lines.push(`前日 ${cutoffLabel} までに発送できる状態だったのに、まだ発送されていない注文です`);
  lines.push('');

  if (alerts.length === 0) {
    lines.push(`✅ 出荷漏れはありません (0件${candidates ? ` / 候補${candidates}件はすべて発送済み・キャンセル済みでした` : ''})`);
  } else {
    lines.push(`🚨 出荷漏れの可能性 *${alerts.length}件*`);
    for (const o of alerts.slice(0, MAX_LINES)) {
      lines.push(`・${o.orderId}  ${yen(o.totalPrice)}  ${o.settlementName}  [${o.orderStatus}]`);
      lines.push(`   注文 ${fmtJst(o.orderedAt)} (${o.elapsedHours}時間経過)${limitLabel(o)}`);
      lines.push(`   ${itemLabel(o)}`);
    }
    if (alerts.length > MAX_LINES) {
      lines.push(`  …他 ${alerts.length - MAX_LINES}件 (多すぎるため省略。Wow!managerで確認してください)`);
    }
  }

  if (apiFailed > 0) {
    lines.push('');
    lines.push(`⚠️ ${apiFailed}件は最新状態を確認できませんでした (本日中に再確認します)`);
  }
  if (truncated) {
    lines.push('');
    lines.push(`⚠️ 候補の注文日が多いため上限 ${MAX_VERIFY_DAYS}日分までしか確認していません (残りは次回へ)`);
  }

  return lines.join('\n');
}
