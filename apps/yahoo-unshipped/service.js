/**
 * Yahoo!ショッピング 未発送アラート — 検知ロジック
 *
 * 目的 (楽天版 apps/rakuten-unshipped と同じ):
 *   弊社は12時締め。前日12:00までに入金確認できていた注文は当日中に出荷されているはず。
 *   翌朝まだ未発送で残っていたら出荷漏れなので朝イチで通知する。
 *
 * ⭐️楽天と作りが違う理由 (2026-08-10 に本番データで実測して確定):
 *   1. **Yahooの受注APIは1件1秒** (VPSプロキシがレート制御)。毎朝30日分を全件取得すると
 *      数十分かかるので、**warehouse.db (daily-syncが同期済み) で候補を絞ってから、
 *      候補だけAPIで最新確認する**二段構えにしている。
 *   2. 🚨**DBだけで判定してはいけない**。daily-syncのYahoo同期は直近7日窓なので、
 *      それより古い注文の ship_status は更新されない。実測では DB候補9件のうち
 *      **7件が誤検知** (5件キャンセル済み・2件出荷済み) だった。API再確認は必須。
 *   3. Yahooは**入金日時 (PayDate) をAPIのFieldに含んでいない** (VPSプロキシ側で固定)。
 *      代わりに **LastUpdateTime が入金時刻の手がかり**になる — 入金されると PayStatus が
 *      0→1 に変わり、その時に LastUpdateTime が更新されるため。
 *      ただし住所変更などでも更新されるので、**これで候補を絞り込むと出荷漏れを見逃す**。
 *      → 候補には入れたうえで「締め後に動きがある注文」は参考枠に分けて出す。
 *
 * ステータスのコード体系 (実データで確認):
 *   OrderStatus  2=処理中 / 4=キャンセル / 5=完了
 *   PayStatus    0=未入金 / 1=入金済み
 *   ShipStatus   0=出荷不可 (未入金) / 1=出荷可能 (=未発送) / 3=出荷済み / 4=旧データの完了値
 *
 * 個人情報:
 *   注文者情報は取得も出力もしない。扱うのは注文ID・日時・金額・商品名のみ。
 */
import fs from 'fs';
import path from 'path';
import { parseStringPromise } from 'xml2js';
import { getDB } from '../warehouse/db.js';
import { jstDateStr } from '../../lib/jst-date.js';

/** 出荷の締め時刻 (時)。前日のこの時刻までに入金確認できた注文が対象 */
export const DEFAULT_CUTOFF_HOUR = 12;
/** DBから候補を拾う期間 (日)。これより古い未発送は拾わない */
export const DEFAULT_SEARCH_DAYS = 180;
/** APIで最新確認する候補数の上限 (1件1秒なので暴走を防ぐ) */
export const MAX_VERIFY = 60;
/** 解消済みキャッシュの保持日数 (これを過ぎた記録は捨てる) */
export const RESOLVED_KEEP_DAYS = 200;

export const ORDER_STATUS_LABEL = { 1: '予約中', 2: '処理中', 3: '保留', 4: 'キャンセル', 5: '完了' };
export const SHIP_STATUS_LABEL = { 0: '出荷不可', 1: '出荷可能', 2: '出荷処理中', 3: '出荷済み', 4: '完了(旧)' };

const pad2 = n => String(n).padStart(2, '0');

/** 'YYYY-MM-DD' に日数を加算する (Date のローカルTZに依存しない) */
export function addDaysStr(ymd, delta) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd));
  if (!m) throw new Error(`addDaysStr: 日付形式が不正 (${ymd})`);
  const t = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) + delta * 86400000;
  const d = new Date(t);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

/** Yahoo の日時文字列を Date にする。'2026-08-06T17:16:34+09:00' 形式 */
export function parseYahooDatetime(s) {
  if (!s || typeof s !== 'string') return null;
  const d = new Date(s.trim());
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * 判定の基準時刻をまとめて作る。
 * ⚠️cutoffStr は DB の値と**同じ形式 (+09:00 付き)** にすること。
 *   オフセットを省くと '...T12:00:00+09:00' > '...T12:00:00' となり、
 *   ちょうど締め時刻の注文が文字列比較で外れる (Codexレビュー Medium)。
 */
export function buildContext(now = new Date(), cutoffHour = DEFAULT_CUTOFF_HOUR) {
  const today = jstDateStr(now);
  const cutoffDate = addDaysStr(today, -1);
  const cutoffStr = `${cutoffDate}T${pad2(cutoffHour)}:00:00+09:00`;
  const cutoff = new Date(cutoffStr);
  return { today, cutoffDate, cutoff, cutoffHour, cutoffStr };
}

// ─── 解消済みキャッシュ ───
// DBは直近7日窓でしか更新されないので、古い注文は出荷済み・キャンセル済みになっても
// DB上は「未発送」のまま残り続ける。API確認で解消と分かった注文をここに記録して
// 次回以降の候補から外す。これが無いと、古い候補が毎日 MAX_VERIFY 枠を食い潰し、
// 新しい候補に永久に到達しない (Codexレビュー High)。

function dataDir() {
  return process.env.DATA_DIR || path.join(process.cwd(), 'data');
}

export function resolvedFilePath() {
  return path.join(dataDir(), 'yahoo-unshipped-resolved.json');
}

/** @returns {Record<string,string>} orderId → 解消を確認した日 (YYYY-MM-DD) */
export function loadResolved() {
  try {
    const raw = fs.readFileSync(resolvedFilePath(), 'utf-8');
    const obj = JSON.parse(raw);
    return (obj && typeof obj === 'object' && !Array.isArray(obj)) ? obj : {};
  } catch {
    // 無い/壊れている場合は空で続行 (キャッシュはあくまで最適化。判定の正しさには影響しない)
    return {};
  }
}

/** 古い記録を落として保存する。書けなくてもジョブは止めない */
export function saveResolved(map, today) {
  const cutoff = addDaysStr(today, -RESOLVED_KEEP_DAYS);
  const pruned = {};
  for (const [id, day] of Object.entries(map)) {
    if (typeof day === 'string' && day >= cutoff) pruned[id] = day;
  }
  try {
    const dir = dataDir();
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(resolvedFilePath(), JSON.stringify(pruned, null, 2));
    return { ok: true, kept: Object.keys(pruned).length };
  } catch (e) {
    console.warn(`[yahoo-unshipped] 解消済みキャッシュを保存できませんでした: ${e.message}`);
    return { ok: false, error: e.message };
  }
}

// ─── ① warehouse.db から候補を絞る ───

/**
 * 「入金済みなのに未発送」の候補をDBから拾う。
 * ここは絞り込みだけ — DBは7日窓でしか更新されないので、確定判定は API 側で行う。
 *
 * ⚠️条件は**注文単位に集約してから**適用する。明細ごとにステータスが食い違った場合に、
 *   都合の悪い明細だけ落としてから集約すると、古い状態のまま候補化してしまう (Codex Medium)。
 * ⚠️ship_status は MIN を取る = 1件でも「出荷可能」の明細があれば候補に入れる (fail-safe)。
 *   確定は必ず API 側で行うので、ここで拾いすぎる分には害が無い。
 * ⚠️last_update_time では絞らない。締め後に住所変更などがあった未発送注文まで
 *   落としてしまい、出荷漏れを見逃すため (Codexレビュー High)。締め後に動きがある注文は
 *   呼び出し側で参考枠に分ける。
 */
export function findCandidates(ctx, searchDays = DEFAULT_SEARCH_DAYS, resolved = {}) {
  const since = `${addDaysStr(ctx.today, -searchDays)}T00:00:00+09:00`;
  const db = getDB();
  const rows = db.prepare(`
    SELECT * FROM (
      SELECT order_id,
             MIN(order_time)       AS order_time,
             MAX(last_update_time) AS last_update_time,
             MAX(order_status)     AS order_status,
             MAX(pay_status)       AS pay_status,
             MIN(ship_status)      AS ship_status,
             MAX(total_price)      AS total_price
        FROM raw_yahoo_orders
       WHERE order_time >= ? AND order_time <= ?
       GROUP BY order_id
    )
    WHERE ship_status = '1'      -- 出荷可能 = 未発送
      AND pay_status  = '1'      -- 入金済み
      AND order_status <> '4'    -- キャンセル以外
    ORDER BY order_time
  `).all(since, ctx.cutoffStr);
  // 過去にAPIで「解消済み」と確認した注文は、DBが古いだけなので除外する
  return rows.filter(r => !resolved[r.order_id]);
}

// ─── ② 候補をAPIで最新確認 ───

function proxyConfig() {
  const base = process.env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080';
  const secret = process.env.YAHOO_PROXY_SECRET || process.env.AUPAY_PROXY_SECRET || '';
  if (!secret) throw new Error('YAHOO_PROXY_SECRET (または AUPAY_PROXY_SECRET) が未設定です');
  return { base, secret };
}

/** orderInfo のXMLから必要な項目だけ取り出す (個人情報は触らない) */
export function extractOrderInfo(parsedXml) {
  const oi = parsedXml?.ResultSet?.Result?.OrderInfo;
  if (!oi || typeof oi !== 'object') return null;
  let items = oi.Item || [];
  if (!Array.isArray(items)) items = [items];
  return {
    orderId: oi.OrderId || '',
    orderTime: oi.OrderTime || '',
    lastUpdateTime: oi.LastUpdateTime || '',
    orderStatus: String(oi.OrderStatus ?? ''),
    payStatus: String(oi.Pay?.PayStatus ?? ''),
    shipStatus: String(oi.Ship?.ShipStatus ?? ''),
    totalPrice: Number(oi.Detail?.TotalPrice ?? 0),
    itemCount: items.length,
    firstItemName: items[0]?.Title || '',
    totalUnits: items.reduce((s, i) => s + (Number(i?.Quantity) || 0), 0),
  };
}

/**
 * 最新状態でも「入金済み × 未発送 × キャンセル以外」か。
 * DBが古いだけの注文 (すでに出荷済み・キャンセル済み) をここで落とす。
 */
export function isStillUnshipped(info) {
  if (!info) return false;
  return info.orderStatus !== '4' && info.payStatus === '1' && info.shipStatus === '1';
}

/**
 * 候補をVPSプロキシ経由で最新確認する。
 * @returns {Promise<{alerts:object[], recent:object[], resolvedIds:string[], verified:number, apiFailed:number, truncated:boolean}>}
 *   alerts       … 締め後に何も動いていない = 確実に放置されている未発送
 *   recent       … 未発送だが締め後に動きがある (入金直後・変更直後の可能性) = 参考枠
 *   resolvedIds  … 出荷済み・キャンセル済みだった注文 (キャッシュに入れて次回除外する)
 */
export async function verifyCandidates(candidates, ctx, { maxVerify = MAX_VERIFY, fetchImpl = fetch } = {}) {
  const truncated = candidates.length > maxVerify;
  const targets = truncated ? candidates.slice(0, maxVerify) : candidates;
  if (targets.length === 0) {
    return { alerts: [], recent: [], resolvedIds: [], verified: 0, apiFailed: 0, truncated };
  }

  const { base, secret } = proxyConfig();
  const res = await fetchImpl(`${base}/yahoo/orderInfo`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': secret, 'Content-Type': 'application/json' },
    body: JSON.stringify({ orderIds: targets.map(c => c.order_id) }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Yahooプロキシ ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();

  const byId = new Map();
  for (const r of (data.results || [])) {
    try {
      const parsed = await parseStringPromise(r.xml, { explicitArray: false });
      const info = extractOrderInfo(parsed);
      if (info) byId.set(r.orderId, info);
    } catch {
      // パース不能は「確認できなかった」として apiFailed に数える (握り潰さない)
    }
  }

  const alerts = [];
  const recent = [];
  const resolvedIds = [];
  let apiFailed = 0;
  for (const c of targets) {
    const info = byId.get(c.order_id);
    if (!info) { apiFailed++; continue; }
    if (!isStillUnshipped(info)) { resolvedIds.push(c.order_id); continue; }

    const orderedAt = parseYahooDatetime(info.orderTime) || parseYahooDatetime(c.order_time);
    const updatedAt = parseYahooDatetime(info.lastUpdateTime);
    const row = {
      orderId: c.order_id,
      orderedAt,
      updatedAt,
      elapsedHours: orderedAt ? Math.round((ctx.now.getTime() - orderedAt.getTime()) / 36e5 * 10) / 10 : null,
      orderStatusLabel: ORDER_STATUS_LABEL[info.orderStatus] || `不明(${info.orderStatus})`,
      shipStatusLabel: SHIP_STATUS_LABEL[info.shipStatus] || `不明(${info.shipStatus})`,
      totalPrice: info.totalPrice,
      itemCount: info.itemCount,
      firstItemName: info.firstItemName,
      totalUnits: info.totalUnits,
    };
    // 締め後に動きがある注文は「今朝入金された」「さっき変更された」可能性がある。
    // 出荷漏れとは限らないので参考枠へ回す (ただし落とさない)
    const movedAfterCutoff = updatedAt && updatedAt.getTime() > ctx.cutoff.getTime();
    (movedAfterCutoff ? recent : alerts).push(row);
  }

  const byOrderedAt = (a, b) => (a.orderedAt?.getTime() ?? 0) - (b.orderedAt?.getTime() ?? 0);
  alerts.sort(byOrderedAt);
  recent.sort(byOrderedAt);
  return { alerts, recent, resolvedIds, verified: targets.length, apiFailed, truncated };
}

/**
 * 検知本体。
 * @param {{now?:Date, cutoffHour?:number, searchDays?:number, maxVerify?:number, skipCacheWrite?:boolean}} opts
 */
export async function findUnshippedOrders(opts = {}) {
  const now = opts.now || new Date();
  const cutoffHour = Number.isFinite(opts.cutoffHour) ? opts.cutoffHour : DEFAULT_CUTOFF_HOUR;
  const searchDays = Number.isFinite(opts.searchDays) ? opts.searchDays : DEFAULT_SEARCH_DAYS;

  const base = buildContext(now, cutoffHour);
  const ctx = { ...base, now };

  const resolved = loadResolved();
  const candidates = findCandidates(ctx, searchDays, resolved);
  console.log(`[yahoo-unshipped] DB候補: ${candidates.length}件`
    + ` (${searchDays}日以内・締め=${ctx.cutoffStr}・解消済み除外=${Object.keys(resolved).length}件)`);

  const r = await verifyCandidates(candidates, ctx, {
    maxVerify: Number.isFinite(opts.maxVerify) ? opts.maxVerify : MAX_VERIFY,
  });
  console.log(`[yahoo-unshipped] API確認: ${r.verified}件 → 未発送 ${r.alerts.length}件`
    + (r.recent.length ? ` / 締め後に動きあり ${r.recent.length}件` : '')
    + (r.resolvedIds.length ? ` / 解消済み ${r.resolvedIds.length}件` : '')
    + (r.apiFailed ? ` / 確認できず ${r.apiFailed}件` : ''));

  // 解消済みを記録して次回の候補から外す (これをしないと古い候補が枠を食い潰す)
  if (!opts.skipCacheWrite && r.resolvedIds.length > 0) {
    for (const id of r.resolvedIds) resolved[id] = ctx.today;
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

/**
 * GChat へ送る本文を組み立てる。
 * 0件の日も送る = 通知が来ないこと自体が「止まった」のサインになる。
 */
export function buildMessage({ alerts, recent = [], candidates, apiFailed, truncated, ctx }) {
  const cutoffLabel = `${ctx.cutoffDate.slice(5).replace('-', '/')} ${pad2(ctx.cutoffHour ?? DEFAULT_CUTOFF_HOUR)}:00`;
  const lines = [];
  lines.push('*Yahoo! 未発送アラート*');
  lines.push(`前日 ${cutoffLabel} までに入金確認できていて、まだ発送されていない注文です`);
  lines.push('');

  if (alerts.length === 0) {
    lines.push(`✅ 出荷漏れはありません (0件${candidates ? ` / 候補${candidates}件はすべて出荷済み・キャンセル済みでした` : ''})`);
  } else {
    lines.push(`🚨 出荷漏れの可能性 *${alerts.length}件*`);
    for (const o of alerts.slice(0, MAX_LINES)) {
      lines.push(`・${o.orderId}  ${yen(o.totalPrice)}  [${o.orderStatusLabel}/${o.shipStatusLabel}]`);
      lines.push(`   注文 ${fmtJst(o.orderedAt)} (${o.elapsedHours}時間経過) / 最終更新 ${fmtJst(o.updatedAt)}`);
      lines.push(`   ${itemLabel(o)}`);
    }
    if (alerts.length > MAX_LINES) {
      lines.push(`  …他 ${alerts.length - MAX_LINES}件 (多すぎるため省略。ストアクリエイターProで確認してください)`);
    }
  }

  if (recent.length > 0) {
    lines.push('');
    lines.push(`🕒 未発送だが締め後に動きあり ${recent.length}件 (今朝の入金・変更かもしれません)`);
    for (const o of recent.slice(0, MAX_LINES)) {
      lines.push(`・${o.orderId}  ${yen(o.totalPrice)}  注文 ${fmtJst(o.orderedAt)} / 更新 ${fmtJst(o.updatedAt)}  ${itemLabel(o)}`);
    }
    if (recent.length > MAX_LINES) lines.push(`  …他 ${recent.length - MAX_LINES}件`);
  }

  if (apiFailed > 0) {
    lines.push('');
    lines.push(`⚠️ ${apiFailed}件は最新状態を確認できませんでした (本日中に再確認します)`);
  }
  if (truncated) {
    lines.push('');
    lines.push(`⚠️ 候補が多いため上限 ${MAX_VERIFY}件までしか確認していません (残りは次回へ)`);
  }

  return lines.join('\n');
}
