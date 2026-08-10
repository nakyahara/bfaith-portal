/**
 * Qoo10 未発送アラート — 検知ロジック
 *
 * 目的 (楽天/Yahoo/auPAY版と同じ):
 *   弊社は12時締め。前日12:00より前の注文は当日中に出荷されているはず。
 *   翌朝まだ未発送で残っていたら出荷漏れなので朝イチで通知する。
 *   (Qoo10 は「いつ発送できる状態になったか」が取れないため注文日時で見る。下の ⚠️ 参照)
 *
 * ⭐️Qoo10 は3モールの中で一番シンプルに作れる (2026-08-10 に本番データで実測して確定):
 *   1. **daily-sync の Qoo10 同期は直近90日を毎回再取得**している (楽天=API直読み、
 *      Yahoo/auPAY=7日窓)。つまり DB が常に新しいので**API再確認が要らない**。
 *   2. 🚨**キャンセル済みは last_seen_at が古くなる**。Qoo10 の受注API
 *      (ShippingBasic.GetShippingInfo_v2) は shipping_status 1〜5 の注文しか返さないので、
 *      キャンセルされるとレスポンスから消え、DBの行だけが取り残される。
 *      **「今日の同期で確認できた注文」に絞れば、キャンセル済みは自動的に落ちる**。
 *      (実測: 今朝の同期で767件が確認され、取り残された行は last_seen_at が数週間〜数ヶ月前)
 *
 * shipping_status の体系 (実データで確認):
 *   'Awaiting shipping(1)' … 入金待ち (実測で全件コンビニ決済/決済方法なし) → 対象外
 *   'Seller confirm(3)'    … 販売者確認済み = 発送できる状態                → **検知対象**
 *   'On delivery(4)'       … 配送中 (発送済み)                              → 対象外
 *   'Delivered(5)'         … 配送完了                                        → 対象外
 *
 * ⚠️**「いつ発送できる状態になったか」は分からない** (payment_date は API が返しておらず全件空。
 *   Seller confirm に変わった時刻を持つフィールドも履歴も無い)。そのため判定は**注文日時**で行う。
 *   → 前払い・後払いで「締めの後に入金されて Seller confirm に上がった」注文も混ざりうる
 *     (例: 前日10:00注文 → 前日18:00入金 → 翌朝未発送。締め時点では発送不能だった)。
 *   通知は「締めより前の注文で、まだ発送されていないもの」という意味になる。
 *   決済方法を必ず本文に出して、人が判断できるようにしている (Codexレビュー High)。
 *   実測では前払い (コンビニ決済) は90日で25件と少数。
 *
 * 個人情報:
 *   注文者情報は読まない・出さない。扱うのは注文番号・日時・金額・商品名のみ。
 */
import { getDB } from '../warehouse/db.js';
import { jstDateStr } from '../../lib/jst-date.js';

/** 出荷の締め時刻 (時)。前日のこの時刻より前の注文が対象 */
export const DEFAULT_CUTOFF_HOUR = 12;
/** 発送できる状態を表す shipping_status (前方一致で見る) */
export const READY_TO_SHIP_PREFIX = 'Seller confirm';
/** 入金待ちを表す shipping_status (対象外。ログ・本文の説明用) */
export const AWAITING_PAYMENT_PREFIX = 'Awaiting shipping';

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
 * Qoo10 の日時文字列 ('2026-08-09 12:34:56') を Date にする。
 * ⚠️`new Date('2026-08-09 12:34:56')` は実行環境のローカルTZ依存なので使わない。JST 固定で組み立てる。
 */
export function parseQoo10Datetime(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/.exec(String(s || '').trim());
  if (!m) return null;
  const d = new Date(`${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6] || '00'}+09:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * 判定の基準時刻をまとめて作る。
 * cutoffStr は DB の order_date と同じ 'YYYY-MM-DD HH:MM:SS' 形式 (桁が揃うので文字列比較でよい)。
 */
export function buildContext(now = new Date(), cutoffHour = DEFAULT_CUTOFF_HOUR) {
  const today = jstDateStr(now);
  const cutoffDate = addDaysStr(today, -1);
  const cutoffStr = `${cutoffDate} ${pad2(cutoffHour)}:00:00`;
  const cutoff = new Date(`${cutoffDate}T${pad2(cutoffHour)}:00:00+09:00`);
  return { today, cutoffDate, cutoff, cutoffHour, cutoffStr };
}

// ─── データの鮮度チェック ───

/**
 * 「今日の同期で確認できた注文」が何件あるか。
 * 0件 = daily-sync の Qoo10 ステップが今日まだ走っていない or 失敗している。
 * その状態で判定すると、キャンセル済みの取り残しを出荷漏れとして誤報告するため、
 * 呼び出し側で fail させる (静かに「0件」と言わない)。
 */
export function countSeenToday(today) {
  const db = getDB();
  const row = db.prepare(`
    SELECT COUNT(*) AS n FROM raw_qoo10_orders
     WHERE is_frozen_after_horizon = 0 AND substr(last_seen_at, 1, 10) = ?
  `).get(today);
  return row?.n ?? 0;
}

// ─── 候補の抽出 (DBのみ。API再確認は不要) ───

/**
 * 「発送できる状態なのに未発送」の注文を拾う。
 *
 * ⚠️条件:
 *   - shipping_status が 'Seller confirm%' (= 発送できる状態)
 *   - **last_seen_at が今日** = 今朝の同期でも API に存在した = キャンセルされていない
 *   - is_frozen_after_horizon = 0 (90日境界を超えて更新されなくなった行は判定に使わない)
 *   - order_date が締め以前
 *
 * 明細行が複数あるので **pack_no (梱包単位 = 1発送)** でまとめる。
 * pack_no が無い行は order_id を代わりのキーにする。
 */
export function findUnshipped(ctx) {
  const db = getDB();
  return db.prepare(`
    SELECT COALESCE(NULLIF(CAST(pack_no AS TEXT), ''), order_id) AS group_key,
           MAX(CAST(pack_no AS TEXT))  AS pack_no,
           MIN(order_id)               AS order_id,
           MIN(order_date)             AS order_date,
           MAX(shipping_status)        AS shipping_status,
           MAX(payment_method)         AS payment_method,
           MAX(tracking_no)            AS tracking_no,
           MAX(last_seen_at)           AS last_seen_at,
           SUM(total)                  AS total,
           SUM(order_qty)              AS qty,
           COUNT(*)                    AS item_count,
           MIN(item_title)             AS item_title
      FROM raw_qoo10_orders
     WHERE is_frozen_after_horizon = 0
       AND shipping_status LIKE ?
       AND substr(last_seen_at, 1, 10) = ?
       AND order_date <= ?
     GROUP BY group_key
     ORDER BY MIN(order_date)
  `).all(`${READY_TO_SHIP_PREFIX}%`, ctx.today, ctx.cutoffStr);
}

/** 参考: 入金待ちで止まっている注文の件数 (対象外だが本文に添えると状況が分かる) */
export function countAwaitingPayment(ctx) {
  const db = getDB();
  const row = db.prepare(`
    SELECT COUNT(DISTINCT COALESCE(NULLIF(CAST(pack_no AS TEXT), ''), order_id)) AS n
      FROM raw_qoo10_orders
     WHERE is_frozen_after_horizon = 0
       AND shipping_status LIKE ?
       AND substr(last_seen_at, 1, 10) = ?
       AND order_date <= ?
  `).get(`${AWAITING_PAYMENT_PREFIX}%`, ctx.today, ctx.cutoffStr);
  return row?.n ?? 0;
}

/** 1件を通知用に整える */
export function summarize(row, ctx) {
  const orderedAt = parseQoo10Datetime(row.order_date);
  return {
    packNo: row.pack_no || row.group_key,
    orderId: row.order_id,
    orderedAt,
    elapsedHours: orderedAt ? Math.round((ctx.now.getTime() - orderedAt.getTime()) / 36e5 * 10) / 10 : null,
    shippingStatus: row.shipping_status,
    paymentMethod: row.payment_method || '(不明)',
    hasTracking: Boolean(String(row.tracking_no || '').trim()),
    total: Number(row.total) || 0,
    qty: Number(row.qty) || 0,
    itemCount: Number(row.item_count) || 0,
    itemTitle: row.item_title || '',
  };
}

/**
 * 検知本体。
 * @param {{now?:Date, cutoffHour?:number}} opts
 * @returns {{alerts:object[], awaitingPayment:number, seenToday:number, stale:boolean, ctx:object}}
 */
export function findUnshippedOrders(opts = {}) {
  const now = opts.now || new Date();
  const cutoffHour = Number.isFinite(opts.cutoffHour) ? opts.cutoffHour : DEFAULT_CUTOFF_HOUR;
  const base = buildContext(now, cutoffHour);
  const ctx = { ...base, now };

  const seenToday = countSeenToday(ctx.today);
  // 今日の同期が無い = キャンセル済みの取り残しを掴んでしまう状態。判定してはいけない
  const stale = seenToday === 0;
  if (stale) {
    console.warn('[qoo10-unshipped] 今日の同期で確認できた注文が0件 — Qoo10の受注同期が'
      + '走っていない可能性があるため判定を見送ります');
    return { alerts: [], awaitingPayment: 0, seenToday, stale, ctx };
  }

  const rows = findUnshipped(ctx);
  const alerts = rows.map(r => summarize(r, ctx));
  const awaitingPayment = countAwaitingPayment(ctx);
  console.log(`[qoo10-unshipped] 今日の同期で確認: ${seenToday}件 → 未発送 ${alerts.length}件`
    + (awaitingPayment ? ` / 入金待ち ${awaitingPayment}件 (対象外)` : ''));

  return { alerts, awaitingPayment, seenToday, stale, ctx };
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
  if (!o.itemTitle) return '(商品名なし)';
  const head = truncate(o.itemTitle, 40);
  return o.itemCount > 1 ? `${head} ほか${o.itemCount - 1}点` : head;
}

/**
 * GChat へ送る本文を組み立てる。
 * 0件の日も送る = 通知が来ないこと自体が「止まった」のサインになる。
 */
export function buildMessage({ alerts, awaitingPayment, seenToday, stale, ctx }) {
  const cutoffLabel = `${ctx.cutoffDate.slice(5).replace('-', '/')} ${pad2(ctx.cutoffHour ?? DEFAULT_CUTOFF_HOUR)}:00`;
  const lines = [];
  lines.push('*Qoo10 未発送アラート*');

  if (stale) {
    lines.push('⚠️ Qoo10の受注同期が今日まだ走っていないため、判定できませんでした');
    lines.push('(古いデータで判定すると、キャンセル済みの注文を出荷漏れとして誤報告するため見送りました)');
    return lines.join('\n');
  }

  lines.push(`前日 ${cutoffLabel} より前の注文で、まだ発送されていないものです`);
  lines.push('(Qoo10は「いつ発送できる状態になったか」が取れないため注文日時で見ています。'
    + '前払い・後払いは締めの後に入金された可能性があるので決済方法を併記しています)');
  lines.push('');

  if (alerts.length === 0) {
    lines.push(`✅ 出荷漏れはありません (0件 / 今朝の同期で ${seenToday}件を確認)`);
  } else {
    lines.push(`🚨 出荷漏れの可能性 *${alerts.length}件*`);
    for (const o of alerts.slice(0, MAX_LINES)) {
      lines.push(`・梱包番号 ${o.packNo}  ${yen(o.total)}  ${o.paymentMethod}${o.hasTracking ? '  [追跡番号あり]' : ''}`);
      lines.push(`   注文 ${fmtJst(o.orderedAt)} (${o.elapsedHours}時間経過)`);
      lines.push(`   ${itemLabel(o)}`);
    }
    if (alerts.length > MAX_LINES) {
      lines.push(`  …他 ${alerts.length - MAX_LINES}件 (多すぎるため省略。QSMで確認してください)`);
    }
  }

  if (awaitingPayment > 0) {
    lines.push('');
    lines.push(`💤 入金待ちで止まっている注文 ${awaitingPayment}件 (対象外。前払いの入金がまだのため発送できません)`);
  }

  return lines.join('\n');
}
