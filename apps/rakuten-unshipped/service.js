/**
 * 楽天 未発送アラート — 検知ロジック
 *
 * 目的:
 *   弊社は 12時締め。前日12:00までに「入金確認 (受注確定)」できていた注文は、
 *   当日中に出荷されているはず。翌朝まだ未発送で残っていたら出荷漏れなので朝イチで通知する。
 *
 * 判定に使うフィールド (2026-08-09 に本番データで実測して確定):
 *   orderProgress      100=注文確認待ち / 200=楽天処理中 / 300=発送待ち / 400=変更確定待ち
 *                      500=発送済 / 800,900=キャンセル
 *   orderFixDatetime   受注確定日時。**これが「入金確認できた時刻」**。
 *                      クレジットカードは注文の約30分後に入る。前払い(コンビニ・銀行振込)は
 *                      入金された時刻に入る (注文日時とは何日もズレる)。未入金なら null。
 *   shippingInstDatetime  発送指示日時。⚠️入金確認と同時刻で自動的に入るため
 *                      「発送済み」の判定には使えない。発送済み = orderProgress 500。
 *   deliveryDate       お届け日指定。未来日なら出荷しないのが正常 → 別枠 (hold) にする。
 *
 * 決済方法では絞らない:
 *   「入金確認ができている = 発送できる」が中原さんの定義。この条件にすると
 *   クレジットカード (注文時点で決済確定) は全部入った上で、入金済みの銀行振込・コンビニ前払い
 *   のような本物の漏れも同じ基準で拾える。決済方法は通知本文に出して区別できるようにする。
 *
 * 個人情報:
 *   注文者情報 (OrdererModel 等) は一切読まない・出さない。扱うのは注文番号・日時・金額・商品名のみ。
 */
import { rakutenRequest } from '../warehouse/rakuten-client.js';
import { jstDateStr } from '../../lib/jst-date.js';

/** 出荷の締め時刻 (時)。前日のこの時刻までに入金確認できた注文が対象 */
export const DEFAULT_CUTOFF_HOUR = 12;
/** searchOrder で遡る日数。前払いは注文から入金まで日が空くので広めに取る */
export const DEFAULT_SEARCH_DAYS = 30;
/** 未発送とみなす orderProgress (500=発送済、800/900=キャンセルは含めない) */
export const UNSHIPPED_PROGRESS = [100, 200, 300, 400];

export const PROGRESS_LABEL = {
  100: '注文確認待ち',
  200: '楽天処理中',
  300: '発送待ち',
  400: '変更確定待ち',
  500: '発送済',
  600: '支払手続き中',
  700: '支払手続き済',
  800: 'キャンセル確定',
  900: 'キャンセル',
};

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
 * RMS が返す日時文字列を Date にする。
 * ⚠️RMS は "2026-08-05T16:30:12+0900" (コロン無しオフセット) を返す。
 *   環境によっては Date.parse が通らないので、必ずここでコロンを補ってから渡す。
 */
export function parseRmsDatetime(s) {
  if (!s || typeof s !== 'string') return null;
  const normalized = s.trim().replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  const d = new Date(normalized);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * 判定の基準時刻をまとめて作る。
 * @param {Date} now 実行時刻
 * @param {number} cutoffHour 締め時刻 (時, JST)
 * @returns {{today:string, cutoffDate:string, cutoff:Date}}
 */
export function buildContext(now = new Date(), cutoffHour = DEFAULT_CUTOFF_HOUR) {
  const today = jstDateStr(now);
  const cutoffDate = addDaysStr(today, -1);
  const cutoff = new Date(`${cutoffDate}T${pad2(cutoffHour)}:00:00+09:00`);
  return { today, cutoffDate, cutoff, cutoffHour };
}

/**
 * 注文1件を分類する。
 * @returns {'alert'|'hold'|'skip'} alert=出荷漏れの可能性 / hold=お届け日指定で保留中 / skip=対象外
 */
export function classifyOrder(order, ctx) {
  const progress = Number(order?.orderProgress);
  // 発送済み・キャンセルは対象外 (searchOrder 側でも絞っているが二重で守る)
  if (!UNSHIPPED_PROGRESS.includes(progress)) return 'skip';

  const fixedAt = parseRmsDatetime(order?.orderFixDatetime);
  // 受注未確定 = 入金確認がまだ = 発送できなくて当然
  if (!fixedAt) return 'skip';
  // 締め後に入金確認された注文は、まだ出荷期限が来ていない
  if (fixedAt.getTime() > ctx.cutoff.getTime()) return 'skip';

  // お届け日指定が未来 = 今出荷しないのが正常。ただし消さずに参考として出す
  const deliveryDate = String(order?.deliveryDate || '').slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(deliveryDate) && deliveryDate > ctx.today) return 'hold';

  return 'alert';
}

/** 通知に必要な項目だけを抜き出す (個人情報は持ち回らない) */
export function summarizeOrder(order, ctx) {
  const items = [];
  for (const pkg of (order?.PackageModelList || [])) {
    for (const it of (pkg?.ItemModelList || [])) {
      items.push({
        name: it?.itemName || '',
        code: it?.itemNumber || '',
        units: Number(it?.units ?? 0),
      });
    }
  }
  const fixedAt = parseRmsDatetime(order?.orderFixDatetime);
  const orderedAt = parseRmsDatetime(order?.orderDatetime);
  return {
    orderNumber: order?.orderNumber || '',
    progress: Number(order?.orderProgress),
    progressLabel: PROGRESS_LABEL[Number(order?.orderProgress)] || `不明(${order?.orderProgress})`,
    settlementMethod: order?.SettlementModel?.settlementMethod || '(不明)',
    orderedAt,
    fixedAt,
    // 入金確認からの経過時間 (時)。基準は実行時刻
    elapsedHours: fixedAt ? Math.round((ctx.now.getTime() - fixedAt.getTime()) / 36e5 * 10) / 10 : null,
    totalPrice: Number(order?.totalPrice ?? 0),
    deliveryDate: String(order?.deliveryDate || '').slice(0, 10) || null,
    itemCount: items.length,
    firstItemName: items[0]?.name || '',
    totalUnits: items.reduce((s, i) => s + (Number.isFinite(i.units) ? i.units : 0), 0),
  };
}

// ─── RMS API ───

async function callRMS(endpoint, body) {
  const r = await rakutenRequest({ path: `/es/2.0/order/${endpoint}/`, method: 'POST', body });
  if (r.status < 200 || r.status >= 300) throw new Error(`RMS ${endpoint} HTTP ${r.status}`);
  const errs = (r.data?.MessageModelList || []).filter(m => m.messageType === 'ERROR');
  if (errs.length) {
    // レスポンス本文は出さない (個人情報が混ざり得る)。コードとメッセージだけ
    throw new Error(`RMS ${endpoint}: ${errs.map(e => `${e.messageCode} ${e.message}`).join(' / ')}`);
  }
  return r.data;
}

/** 未発送の注文番号を集める (注文日ベースで過去 searchDays 日) */
async function searchUnshippedOrderNumbers(startDate, endDate) {
  let numbers = [];
  let page = 1;
  let totalPages = 1;
  while (page <= totalPages) {
    const d = await callRMS('searchOrder', {
      dateType: 1, // 注文日
      startDatetime: `${startDate}T00:00:00+0900`,
      endDatetime: `${endDate}T23:59:59+0900`,
      orderProgressList: UNSHIPPED_PROGRESS,
      PaginationRequestModel: { requestRecordsAmount: 1000, requestPage: page },
    });
    numbers = numbers.concat(d.orderNumberList || []);
    totalPages = d.PaginationResponseModel?.totalPages || 1;
    page++;
    // 取り漏らしを黙って起こさないための保険 (1000件×20ページ = 2万件を超えることは無い想定)
    if (page > 20) {
      console.warn('[rakuten-unshipped] searchOrder のページ数が想定を超えたため打ち切りました');
      break;
    }
  }
  return numbers;
}

/**
 * 検知本体。
 * @param {{now?:Date, cutoffHour?:number, searchDays?:number}} opts
 * @returns {Promise<{alerts:object[], holds:object[], scanned:number, ctx:object}>}
 */
export async function findUnshippedOrders(opts = {}) {
  const now = opts.now || new Date();
  const cutoffHour = Number.isFinite(opts.cutoffHour) ? opts.cutoffHour : DEFAULT_CUTOFF_HOUR;
  const searchDays = Number.isFinite(opts.searchDays) ? opts.searchDays : DEFAULT_SEARCH_DAYS;

  const base = buildContext(now, cutoffHour);
  const ctx = { ...base, now };

  const endDate = base.today;
  const startDate = addDaysStr(base.today, -searchDays);

  const orderNumbers = await searchUnshippedOrderNumbers(startDate, endDate);
  console.log(`[rakuten-unshipped] 未発送の注文: ${orderNumbers.length}件 (${startDate}〜${endDate})`);

  const alerts = [];
  const holds = [];
  for (let i = 0; i < orderNumbers.length; i += 100) {
    const d = await callRMS('getOrder', {
      orderNumberList: orderNumbers.slice(i, i + 100),
      version: 7,
    });
    for (const o of (d.OrderModelList || [])) {
      const kind = classifyOrder(o, ctx);
      if (kind === 'skip') continue;
      (kind === 'alert' ? alerts : holds).push(summarizeOrder(o, ctx));
    }
  }

  const byFixedAt = (a, b) => (a.fixedAt?.getTime() ?? 0) - (b.fixedAt?.getTime() ?? 0);
  alerts.sort(byFixedAt);
  holds.sort((a, b) => String(a.deliveryDate).localeCompare(String(b.deliveryDate)) || byFixedAt(a, b));

  return { alerts, holds, scanned: orderNumbers.length, ctx };
}

// ─── GChat 本文 ───

/** 表示上限。超えた分は「他N件」と明示する (黙って切らない) */
export const MAX_LINES = 30;

function fmtJst(d, withDate = true) {
  if (!d) return '-';
  const t = new Date(d.getTime() + 9 * 3600 * 1000);
  const hm = `${pad2(t.getUTCHours())}:${pad2(t.getUTCMinutes())}`;
  return withDate ? `${t.getUTCMonth() + 1}/${t.getUTCDate()} ${hm}` : hm;
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
 * 0件の日も送る = 通知が来ないこと自体が「ジョブが止まった」のサインになる。
 */
export function buildMessage({ alerts, holds, ctx, scanned }) {
  const cutoffLabel = `${ctx.cutoffDate.slice(5).replace('-', '/')} ${pad2(ctx.cutoffHour ?? DEFAULT_CUTOFF_HOUR)}:00`;
  const lines = [];
  lines.push('*楽天 未発送アラート*');
  lines.push(`前日 ${cutoffLabel} までに入金確認できていて、まだ発送されていない注文です (未発送 ${scanned}件を確認)`);
  lines.push('');

  if (alerts.length === 0) {
    lines.push('✅ 出荷漏れはありません (0件)');
  } else {
    lines.push(`🚨 出荷漏れの可能性 *${alerts.length}件*`);
    for (const o of alerts.slice(0, MAX_LINES)) {
      lines.push(`・${o.orderNumber}  ${yen(o.totalPrice)}  ${o.settlementMethod}  [${o.progressLabel}]`);
      lines.push(`   入金確認 ${fmtJst(o.fixedAt)} (${o.elapsedHours}時間経過) / 注文 ${fmtJst(o.orderedAt)}`);
      lines.push(`   ${itemLabel(o)}`);
    }
    if (alerts.length > MAX_LINES) {
      lines.push(`  …他 ${alerts.length - MAX_LINES}件 (多すぎるため省略。RMSで確認してください)`);
    }
  }

  if (holds.length > 0) {
    lines.push('');
    lines.push(`📅 お届け日指定のため保留中 ${holds.length}件 (参考・対応不要のことが多い)`);
    for (const o of holds.slice(0, MAX_LINES)) {
      lines.push(`・${o.orderNumber}  お届け日 ${o.deliveryDate}  ${yen(o.totalPrice)}  ${itemLabel(o)}`);
    }
    if (holds.length > MAX_LINES) {
      lines.push(`  …他 ${holds.length - MAX_LINES}件`);
    }
  }

  return lines.join('\n');
}
