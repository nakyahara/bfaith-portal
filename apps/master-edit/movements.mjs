/**
 * movements.mjs — 商品ごとの入出庫履歴と月別のまとめ (CompanyDB構想 26 の PR-1・PR-2。読むだけ。書かない)
 *
 * 材料 (画面と同じ Render の中で読む):
 *   - 出荷 = Company DB の NE の伝票 (core.shipments / shipment_lines)。画面のロールは表を読めない = 読むだけの関数 (0083 仮) で:
 *       ops.sku_movement_shipments (伝票ごとの行) / ops.sku_shipment_totals (区間ごとの数) / ops.sku_shipments_monthly (月ごとの受注数・売上数)
 *   - ロジザードの毎日の在庫 (錨) = ops.sku_stock_anchors (締めた日・取得時刻・その SKU の数)
 *   - 仕入 (入荷) と発注 = 発注アプリの SQLite (warehouse-mirror.db の po_inbound_* / po_order*)。注文残と同じく発注アプリの権限がある人だけ (extras.mjs の readBackorderLines と同じ流儀)
 *   - いまのロジザード = 画面がもう読んでいる写し (extras.mjs の readWarehouseStock) = 最後の締めの後の分を合わせる
 *
 * 在庫数の作り方 (26 §4.1・中原さんの決定 Q1 = a / Q2 = a / Q3 = a / Q12):
 *   1. 錨 = ロジザードの毎日の在庫 (complete の日)。日 D の数 S_D と取得時刻 T_D
 *   2. 区間 (T_prev, T_D] の分かる動き = 出荷 (出荷確定の時刻で切る) と仕入 (入庫日 = 日付だけ → その日の取得の直前に置く)
 *   3. 「在庫の調整 (ずれ)」= (S_D − S_prev) − 区間の動きの合計。0 でなければ T_D に 1 行 (NE の「他勘定出庫」= 在庫ずれの調整と同じ役。Q12)
 *      間に取れなかった日 (missing / partial) があれば、区間を最後の complete の日まで広げて 1 行
 *   4. 在庫数 = S_prev から 1 行ずつ足し引き = 区間の最後は必ずロジザードの数と同じ
 *   5. 最後の締めの後 = 出荷・仕入を並べ、いまのロジザードの写しの時刻で合わせる (調整の行)
 *   6. ロジザードの日次が始まる前 = 行は出すが在庫数は空欄 (Q2 = a。NE の在庫で埋めない = 基準が途中で変わると数がつながらない)
 *   - 本番を測った結果 (PR-0・10/17): 30 単品 × 30 日で合わない区間 19%・そのうち前後でちょうど打ち消し合う組が 2 割 (出荷確定の時刻のずれ)。
 *     組を打ち消して消すと「区間の最後 = ロジザードの数」が崩れる = 消さずに、組になる調整には「時刻のずれの見込み」と書く
 *     (時刻のずれ = 「+N」の側の区間に N 個以上の出荷がある組だけ。7 区間まで先を見る = FBA 納品の出荷確定が数日遅れる分も)
 *   - 発注アプリの権限が無い人 = 仕入の行を出さない = 仕入の分は「在庫の調整」に入る (そう書く)
 *
 * ページ: 新しい順・出荷 200 行ずつ。区間の途中で切らない (切れ目 = 区間の始まりの取得時刻か、錨の前なら日の始まり) = どのページでも在庫数がそろう。
 *   「もっと前」= 前のページの切れ目 (before) から。出荷のある限り (2025-01-01 から) さかのぼれる。錨は 13 か月 (日次の保持) まで
 * 月別 (26 §3): 直近 13 か月。受注数 = NE の受注日・売上数 = 出荷確定日・仕入数 = 良品数 (入庫日)・発注数 = 発行した数 (減数・取消は引かない・移行の発注は除く)。
 *   金額: 仕入 = 良品数 × 仕入単価・発注 = 数 × 単価 (単価の無い行がある = 「一部」)。受注・売上の金額はまだ出せない (PR-3)。
 *   データの無い月は空欄 (0 と出さない): 出荷 = 2025-01 より前・仕入と発注 = 2026-07 (発注アプリを使い始めた月) より前
 */
import { getDB, normProductCode, normSupplierCode } from '../purchase-orders/db.js';
import { ListTimeoutError } from './deadline.mjs';

export const SHIPMENTS_FROM = '2025-01-01';      // Company DB の出荷の始まり (D-28)
export const PO_FROM = '2026-07-13';             // 発注アプリを使い始めた日 (それより前の仕入・発注は NE だけ = 持っていない。Q7 = a)
export const PAGE_ROWS = 200;
export const PAGE_CAP = 3000;                    // 1 つの区間 (ふつう 1 日) の出荷の行の上限 (超えたら在庫数を出さない)
export const MONTHS = 13;
export const PAIR_WINDOW = 7;
export const ADJUST_LABEL = '在庫の調整 (ずれ)';
const H = 3600e3;

const iso = (v) => (v == null ? null : v instanceof Date ? v.toISOString() : new Date(v).toISOString());
const ms = (v) => (v == null ? null : new Date(v).getTime());
/** 時刻 → JST の日 'YYYY-MM-DD' */
export const jstDay = (v) => new Date(ms(v) + 9 * H).toISOString().slice(0, 10);
/** JST の日の始まりの時刻 (ms) */
const dayStartMs = (ymd) => Date.parse(`${ymd}T00:00:00+09:00`);
/** その月の初め 'YYYY-MM-01' から n か月ずらす */
export function addMonths(ym01, n) {
  const [y, m] = ym01.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return d.toISOString().slice(0, 10);
}
/** NE の店舗名を短く (雑貨イズム楽天市場店 → 楽天市場)。表に略名の列は足さない (26 §2.1) */
export function shortShop(name) {
  const s = String(name || '').trim();
  if (!s) return '';
  return s.replace(/^雑貨イズム\s*/, '').replace(/店$/, '') || s;
}

/** DB の関数の誤り → 画面の言葉。関数が無い (0083 の前)・権限が無い (流し直しの前) = no_privilege */
export function fnError(e) {
  if (e instanceof ListTimeoutError) throw e;
  if (e && (e.code === '42883' || e.code === '42501')) return { ok: false, reason: 'no_privilege', error: '画面のロールに出荷を読む関数がまだありません (migration 0083 と create-master-edit-roles.mjs の流し直しが要ります)' };
  console.error(`[master-edit] 入出庫の履歴を読めない: ${e && e.message}`);
  return { ok: false, reason: 'error', error: '入出庫の履歴を読めません' };
}

// ─── 読む (DB・SQLite) ───

/** ロジザードの毎日の在庫 (錨)。[{ day, status, gen (ISO|null), qty (complete の日だけ) }] 日付の順 */
export async function readAnchors(db, skuId, fromDay = SHIPMENTS_FROM) {
  const r = await db.query('select snapshot_date::text as day, status, captured_at, qty from ops.sku_stock_anchors($1::bigint, $2::date)', [skuId, fromDay]);
  return r.rows.map((x) => ({ day: x.day, status: x.status, gen: iso(x.captured_at), qty: x.qty == null ? null : Number(x.qty) }));
}
/** 区切り (昇順の時刻) ごとの出荷の数。Map(区間の番号 1.. → 数)。区間 i = (bounds[i-1], bounds[i]] (0 始まりの配列で) */
export async function readShipmentTotals(db, skuId, bounds) {
  const m = new Map();
  if (bounds.length < 2) return m;
  for (let i = 0; i < bounds.length; i += 1000) {
    // 1,000 個までずつ (関数の上限)。つなぎ目は前の束の最後の区切りから
    const part = bounds.slice(i === 0 ? 0 : i - 1, i + 1000);
    if (part.length < 2) break;
    const off = i === 0 ? 0 : i - 1;
    const r = await db.query('select bucket, qty from ops.sku_shipment_totals($1::bigint, $2::timestamptz[])', [skuId, part]);
    for (const x of r.rows) if (x.bucket >= 1 && x.bucket < part.length) m.set(off + Number(x.bucket), (m.get(off + Number(x.bucket)) || 0) + Number(x.qty));
  }
  return m;
}
/** 出荷の行 (伝票ごと)。(after, before] の新しい順 */
export async function readShipments(db, skuId, { after = null, before = null, beforeSlip = null, limit = PAGE_ROWS } = {}) {
  const r = await db.query('select shipped_at, ship_date_jst::text as date, ne_slip_no, shop_code, shop_name, qty from ops.sku_movement_shipments($1::bigint, $2::timestamptz, $3::timestamptz, $4::text, $5::integer)',
    [skuId, after, before, beforeSlip, limit]);
  return r.rows.map((x) => ({ at: iso(x.shipped_at), date: x.date, slip: x.ne_slip_no, shopCode: x.shop_code, shopName: x.shop_name, qty: Number(x.qty) }));
}

/**
 * 発注アプリの仕入 (入荷) と発注 (その商品)。{ ok: true, receipts, orders } / { ok: false, error }
 *   仕入 = ロジザードの入荷の CSV の取り込み (訂正で消えた行 = superseded は除く・「対象外」にした入荷も倉庫には入っている = 入れる)。伝票 (ロジザードの伝票NO) ごとに足す
 *   発注 = 発行した発注 (下書きは除く)。移行の発注 (origin = migration) は月別では除く (NE の発注残の取り込み = 発行日が 7/13 に固まる)
 */
export function readPoForSku(code, { db = null } = {}) {
  try {
    const d = db || getDB();
    const key = normProductCode(code);
    const sup = new Map();
    for (const r of d.prepare('select supplier_code, name from po_suppliers').all()) sup.set(normSupplierCode(r.supplier_code), r.name);
    const byReceipt = new Map();
    for (const r of d.prepare(`select r.id as rid, r.source_key as slip, r.receipt_date, r.imported_at, i.supplier_code, i.good_qty, i.defective_qty, i.unit_cost, b.actor
        from po_inbound_items i join po_inbound_receipts r on r.id = i.receipt_id left join po_inbound_batches b on b.id = i.batch_id
       where i.product_key = ? and i.superseded = 0 order by r.id, i.id`).all(key)) {
      let x = byReceipt.get(r.rid);
      if (!x) {
        const date = /^\d{4}-\d{2}-\d{2}$/.test(r.receipt_date || '') ? r.receipt_date : (r.imported_at ? jstDay(r.imported_at) : null);
        x = { slip: r.slip, date, dateFromImport: !r.receipt_date, supplierCode: r.supplier_code || null, supplierName: r.supplier_code ? (sup.get(normSupplierCode(r.supplier_code)) || null) : null,
          qty: 0, defective: 0, amount: 0, costUnknown: false, actors: new Set() };
        byReceipt.set(r.rid, x);
      }
      const g = Number(r.good_qty) || 0;
      x.qty += g; x.defective += Number(r.defective_qty) || 0;
      if (g > 0) { if (r.unit_cost == null) x.costUnknown = true; else x.amount += g * Number(r.unit_cost); }
      if (r.actor) x.actors.add(r.actor);
    }
    const receipts = [...byReceipt.values()].filter((x) => x.date && (x.qty > 0 || x.defective > 0)).map((x) => ({ ...x, actor: [...x.actors].join('・') || null, actors: undefined }));
    const orders = d.prepare(`select o.po_number, o.issued_at, o.origin, i.qty, i.unit_cost from po_order_items i join po_orders o on o.id = i.order_id
       where i.product_key = ? and o.status = 'issued' and o.issued_at is not null order by o.issued_at`).all(key)
      .map((r) => ({ poNumber: r.po_number, issuedAt: iso(r.issued_at), origin: r.origin || null, qty: Number(r.qty) || 0, unitCost: r.unit_cost == null ? null : Number(r.unit_cost) }));
    return { ok: true, receipts, orders };
  } catch (e) {
    if (e instanceof ListTimeoutError) throw e;
    console.error(`[master-edit] 発注アプリの仕入・発注を読めない: ${e && e.message}`);
    return { ok: false, error: '発注アプリの台帳を読めません' };
  }
}

// ─── 計算 (純粋な関数) ───

/** 仕入の行を時刻に置く: その日の取得 (錨) があれば取得の直前・無ければその日の 12:00 (日付しか無い) */
export function placeReceipts(receipts, anchors) {
  const genOf = new Map(anchors.filter((a) => a.status === 'complete' && a.gen).map((a) => [a.day, ms(a.gen)]));
  return (receipts || []).map((r) => {
    const g = genOf.get(r.date);
    return { ...r, t: g != null ? g - 1 : Date.parse(`${r.date}T12:00:00+09:00`) };
  });
}

/**
 * 区間ごとの調整 (全部の complete の日)。
 * @param anchors   readAnchors の戻り
 * @param totals    Map(区間の番号 → 出荷の数) = readShipmentTotals (区切り = complete の日の取得時刻)
 * @param placed    placeReceipts の戻り (権限が無い = [])
 * @returns { cps: [{ day, t, qty }] complete の日, adj: [{ k, day, t, from, u, shipped, received, gaps: [日], hint }] } (k = 区間の番号 = cps の添字)
 */
export function adjustmentsOf(anchors, totals, placed) {
  const cps = anchors.filter((a) => a.status === 'complete' && a.gen).map((a) => ({ day: a.day, t: ms(a.gen), qty: a.qty })).sort((a, b) => a.t - b.t);
  const adj = [];
  for (let k = 1; k < cps.length; k++) {
    const p = cps[k - 1], d = cps[k];
    const shipped = totals.get(k) || 0;
    const received = placed.filter((r) => r.t > p.t && r.t <= d.t).reduce((s, r) => s + r.qty, 0);
    const u = (d.qty - p.qty) + shipped - received;
    const gaps = anchors.filter((a) => a.day > p.day && a.day < d.day).map((a) => a.day);
    adj.push({ k, day: d.day, t: d.t, from: p.day, u, shipped, received, gaps, hint: null });
  }
  // 時刻のずれの組: 「−N」と「+N」が 7 区間のうちに続き、+N の側の区間に N 個以上の出荷がある (出荷確定とロジザードの減りの時刻がずれた)
  const used = new Set();
  for (let i = 0; i < adj.length; i++) {
    const a = adj[i];
    if (!a.u || used.has(i)) continue;
    for (let j = i + 1; j < adj.length && j <= i + PAIR_WINDOW; j++) {
      const b = adj[j];
      if (used.has(j) || b.u !== -a.u) continue;
      const plus = a.u > 0 ? a : b;
      if (plus.shipped < Math.abs(a.u)) continue;
      used.add(i); used.add(j);
      a.hint = { pairDay: b.day }; b.hint = { pairDay: a.day };
      break;
    }
  }
  return { cps, adj };
}

/** 区間の番号: t が (cps[k-1].t, cps[k].t] なら k・最初の錨以前 = 0・最後の錨より後 = cps.length (最後の締めの後) */
function intervalOf(cps, t) {
  let lo = 0, hi = cps.length;   // cps[i].t < t となる i の数
  while (lo < hi) { const mid = (lo + hi) >> 1; if (cps[mid].t < t) lo = mid + 1; else hi = mid; }
  return lo;
}

/** 伝票番号の並び (DB の collate "C" と同じ = 文字のコードの順) */
const cmpSlip = (a, b) => { const x = String(a ?? ''), y = String(b ?? ''); return x < y ? -1 : x > y ? 1 : 0; };
const fmtMd = (ymd) => `${Number(ymd.slice(5, 7))}/${Number(ymd.slice(8, 10))}`;

/**
 * 1 ページの行を作る (新しい順)。
 * @param o.anchors / o.totals / o.receipts (null = 権限が無い・読めない) / o.ships = 窓の出荷 (readShipments・区間の途中で切れていない)
 * @param o.after / o.before = 窓 (ms・after は含まない・before は含む・null = 端なし)
 * @param o.live = { asOf, qty } いまのロジザード (最初のページだけ・無ければ null)
 * @param o.truncated = 窓のいちばん古い区間の出荷が上限で切れた (その区間の在庫数を出さない)
 * @param o.poNote = 仕入を出さない理由 (調整の理由に添える)
 */
export function buildMovementRows(o) {
  const placed = placeReceipts(o.receipts || [], o.anchors);
  const { cps, adj } = adjustmentsOf(o.anchors, o.totals, placed);
  const inWin = (t) => (o.after == null || t > o.after) && (o.before == null || t <= o.before);
  // 動き (時刻の順・同じ時刻は出荷 → 仕入 → 伝票番号の順)。表は同じ鍵のちょうど逆 (新しい順) = 在庫数の積み上げと表の並びがそろう (Codex R1)
  const moves = [
    ...o.ships.map((s) => ({ t: ms(s.at), seq: 0, kind: 'ship', delta: -s.qty, src: s })),
    ...placed.filter((r) => inWin(r.t)).map((r) => ({ t: r.t, seq: 1, kind: 'receipt', delta: r.qty, src: r })),
  ].sort((a, b) => a.t - b.t || a.seq - b.seq || cmpSlip(a.src.slip, b.src.slip));
  // 区間ごとの在庫数 (窓の中の区間 = 動きは全部この窓にある)
  const byK = new Map();
  for (const m of moves) { const k = intervalOf(cps, m.t); if (!byK.has(k)) byK.set(k, []); byK.get(k).push(m); }
  const oldestK = moves.length ? intervalOf(cps, moves[0].t) : null;
  const rows = [];
  for (const [k, ms2] of byK) {
    const startQty = k >= 1 && k <= cps.length ? cps[k - 1].qty : null;   // k = 0 = 錨の前 (空欄)
    let bal = startQty;
    const broken = o.truncated && k === oldestK;
    for (const m of ms2) {
      if (bal != null) bal += m.delta;
      m.balance = broken ? null : bal;
    }
    // 窓の中の区間の調整は、窓の動きから作り直す (別々に読んだ数がずれても、区間の最後 = ロジザードの数 がそろう)
    if (k >= 1 && k < cps.length && !broken) {
      const a = adj[k - 1];
      const sum = ms2.reduce((s, m) => s + m.delta, 0);
      a.u = cps[k].qty - cps[k - 1].qty - sum;
    }
  }
  // 最後の締めの後: いまのロジザードの写しの時刻で合わせる (写しの後の動きは、写しの数から数え直す)
  let liveU = null, lt = null;
  const last = cps.length ? cps[cps.length - 1] : null;
  if (o.live && last && Number.isFinite(ms(o.live.asOf)) && ms(o.live.asOf) > last.t && inWin(ms(o.live.asOf))) {
    lt = ms(o.live.asOf);
    // 最後の締めの後の区間が上限で切れた = 読めていない動きがある: 写しとの差 (調整) は出さない (架空の調整にしない・Codex R1)。
    //   写しの後の動きは、読めた中のいちばん古い行が写しより前なら全部読めている = 写しの数から数えられる
    const tailBroken = o.truncated && oldestK === cps.length;
    if (!tailBroken) {
      const expect = last.qty + moves.filter((m) => m.t > last.t && m.t <= lt).reduce((s, m) => s + m.delta, 0);
      liveU = Number(o.live.qty) - expect;
    }
    if (!tailBroken || (moves.length && moves[0].t <= lt)) {
      let bal = Number(o.live.qty);
      for (const m of moves) if (m.t > lt) { bal += m.delta; m.balance = bal; }
    }
  }
  for (const m of moves) {
    const s = m.src;
    if (m.kind === 'ship') {
      rows.push({ at: s.at, t: m.t, seq: 0, dateOnly: false, kind: 'ship', label: '出荷', in: null, out: s.qty, balance: m.balance, slip: s.slip, slipKind: 'ne',
        party: shortShop(s.shopName) || (s.shopCode ? `店舗 ${s.shopCode}` : ''), reason: '出荷確定', actor: null });
    } else {
      rows.push({ at: new Date(m.t).toISOString(), t: m.t, seq: 1, dateOnly: true, day: s.date, kind: 'receipt', label: '仕入', in: s.qty, out: null, balance: m.balance, slip: s.slip, slipKind: 'logizard',
        party: s.supplierName || s.supplierCode || '', reason: '仕入 (入荷)' + (s.defective > 0 ? ` · 不良品 ${s.defective} は入れていない` : '') + (s.dateFromImport ? ' · 入庫日が無いので取り込んだ日' : ''), actor: s.actor || null });
    }
  }
  // 調整の行 (窓の中・0 でない)
  for (const a of adj) {
    if (!a.u || !inWin(a.t)) continue;
    const why = ['ロジザードの毎日の在庫との差 (理由は分かりません)'];
    if (o.poNote) why.push(o.poNote);
    if (a.gaps.length) why.push(`${a.gaps.map(fmtMd).join('・')} は在庫が取れなかった日 = ${fmtMd(a.from)} からまとめて`);
    if (a.hint) why.push(`出荷確定の時刻のずれの見込み (${fmtMd(a.hint.pairDay)} の調整と打ち消し合う)`);
    rows.push({ at: new Date(a.t).toISOString(), t: a.t, seq: 2, dateOnly: false, kind: 'adjust', label: ADJUST_LABEL, in: a.u > 0 ? a.u : null, out: a.u < 0 ? -a.u : null,
      balance: cps[a.k].qty, slip: null, party: '', reason: why.join(' · '), actor: null, pair: !!a.hint });
  }
  if (liveU) {
    rows.push({ at: new Date(lt).toISOString(), t: lt, seq: 2, dateOnly: false, kind: 'adjust', label: ADJUST_LABEL, in: liveU > 0 ? liveU : null, out: liveU < 0 ? -liveU : null, balance: Number(o.live.qty), slip: null, party: '',
      reason: ['いまのロジザードの写しとの差 (理由は分かりません)', ...(o.poNote ? [o.poNote] : [])].join(' · '), actor: null, live: true });
  }
  rows.sort((a, b) => b.t - a.t || b.seq - a.seq || cmpSlip(b.slip, a.slip));
  return rows.map(({ t, seq, ...r }) => r);
}

/**
 * 窓の下の切れ目: 200 行目の時刻 t を含む区間の始まり (= t より前の最後の complete の取得時刻)。錨の前 = t の日の始まり (t より前)
 */
export function pageFloorOf(anchors, t) {
  const cps = anchors.filter((a) => a.status === 'complete' && a.gen).map((a) => ms(a.gen)).sort((a, b) => a - b);
  let b = null;
  for (const g of cps) if (g < t) b = g;
  if (b != null) return b;
  const d0 = dayStartMs(jstDay(t));
  return d0 < t ? d0 : dayStartMs(jstDay(t - 1));
}

/**
 * 1 ページを読む (DB + 発注アプリ)。
 * @param sku { sku_id, code }
 * @param opt.before ISO (この時刻まで = 前のページの切れ目)・opt.poAccess・opt.live ({ asOf, qty } | null)・opt.poReader (試験)
 * @returns { ok, rows, next (ISO|null), lastClose { day, at }, firstDay, liveAt, po: 'ok'|'denied'|'error', truncated }
 */
export async function readMovementsPage(db, sku, { before = null, poAccess = false, live = null, poReader = readPoForSku, pageRows = PAGE_ROWS, cap = PAGE_CAP } = {}) {
  // 続きの位置 = 'ISO' (区間の切れ目 = その時刻まで全部) か 'ISO|伝票番号' (上限で切れた = 同じ時刻はその伝票番号より前)
  const cur = before == null ? null : String(before);
  const bar = cur == null ? -1 : cur.indexOf('|');
  const beforeAt = cur == null ? null : bar >= 0 ? cur.slice(0, bar) : cur;
  const beforeSlip = bar >= 0 ? cur.slice(bar + 1) || null : null;
  const beforeMs = beforeAt == null ? null : ms(beforeAt);
  if (cur != null && (!/^\d{4}-\d{2}-\d{2}T/.test(beforeAt) || !Number.isFinite(beforeMs))) return { ok: false, reason: 'bad_cursor', error: '続きの位置が読めません (画面を開き直してください)' };
  const B = { before: beforeAt, beforeSlip };
  try {
    const anchors = await readAnchors(db, sku.sku_id);
    const gens = anchors.filter((a) => a.status === 'complete' && a.gen).map((a) => a.gen).sort((a, b) => ms(a) - ms(b));
    const totals = await readShipmentTotals(db, sku.sku_id, gens);
    const floor = new Date(dayStartMs(SHIPMENTS_FROM)).toISOString();
    const first = await readShipments(db, sku.sku_id, { after: floor, ...B, limit: pageRows + 1 });
    let ships = first, after = dayStartMs(SHIPMENTS_FROM), next = null, truncated = false;
    if (first.length > pageRows) {
      after = pageFloorOf(anchors, ms(first[pageRows - 1].at));
      ships = await readShipments(db, sku.sku_id, { after: new Date(after).toISOString(), ...B, limit: cap });
      truncated = ships.length >= cap;
      next = new Date(after).toISOString();
      if (truncated) {
        // 上限で切れた (1 つの区間に 3,000 伝票より多い = ふつうは起きない): 続きは読めた中でいちばん古い行 (時刻・伝票番号) の次から = 同じ時刻の残りも次のページで読む。
        //   仕入・調整の行は、その時刻ちょうどまでを次のページに回す。切れた区間の在庫数は出さない
        const o = ships[ships.length - 1];
        after = ms(o.at);
        next = `${new Date(after).toISOString()}|${o.slip}`;
      }
    }
    const po = poAccess ? poReader(sku.code) : null;
    const receipts = po && po.ok ? po.receipts : [];
    const poState = !poAccess ? 'denied' : po && po.ok ? 'ok' : 'error';
    // 続きがあるか: 切れ目より前に出荷・仕入・調整のどれかがある (無いのに「もっと前」を出さない)
    if (next && !truncated) {
      const older = (await readShipments(db, sku.sku_id, { after: floor, before: next, beforeSlip: null, limit: 1 })).length > 0;
      const placed = placeReceipts(receipts, anchors);
      const { adj } = adjustmentsOf(anchors, totals, placed);
      if (!older && !placed.some((r) => r.t <= after) && !adj.some((a) => a.u && a.t <= after)) next = null;
    }
    const poNote = poState === 'denied' ? '発注アプリの権限が無いので、仕入もここに入っています' : poState === 'error' ? '発注アプリを読めないので、仕入もここに入っています' : null;
    // 窓の外 (前のページより新しい) の仕入は出さない。窓の下 (after) より古い仕入も出さない (次のページ)
    const rows = buildMovementRows({ anchors, totals, receipts, ships, after: next ? after : null, before: beforeMs, live: cur == null ? live : null, truncated, poNote });
    const cps = anchors.filter((a) => a.status === 'complete' && a.gen);
    const lastA = cps.length ? cps[cps.length - 1] : null;
    return {
      ok: true, rows, next, truncated, po: poState,
      lastClose: lastA ? { day: lastA.day, at: lastA.gen } : null,
      firstDay: cps.length ? cps[0].day : null,
      liveAt: cur == null && live ? live.asOf : null,
      liveStale: cur == null && live ? !!live.stale : null,
    };
  } catch (e) {
    return fnError(e);
  }
}

/**
 * 月別のまとめ (純粋な関数)。months = 新しい月から MONTHS か月 ('YYYY-MM-01')。
 * @param ship [{ month: 'YYYY-MM-01', orderQty, shipQty }] (Company DB)
 * @param po { receipts, orders } | null (権限が無い・読めない = 仕入・発注の欄を出さない)
 */
export function buildMonthlySummary({ months, ship, po }) {
  const sm = new Map(ship.map((r) => [r.month, r]));
  const shipFrom = SHIPMENTS_FROM.slice(0, 7) + '-01';
  const poFrom = PO_FROM.slice(0, 7) + '-01';
  const rcv = new Map(), ord = new Map();
  if (po) {
    for (const r of po.receipts) {
      const m = r.date.slice(0, 7) + '-01';
      const x = rcv.get(m) || { qty: 0, amount: 0, unknown: false };
      x.qty += r.qty; x.amount += r.amount; if (r.costUnknown) x.unknown = true;
      rcv.set(m, x);
    }
    for (const o of po.orders) {
      if (o.origin === 'migration') continue;   // NE の発注残の取り込み (発行日が 7/13 に固まる) = 入れない
      const m = jstDay(o.issuedAt).slice(0, 7) + '-01';
      const x = ord.get(m) || { qty: 0, amount: 0, unknown: false };
      x.qty += o.qty; if (o.unitCost == null) x.unknown = true; else x.amount += o.qty * o.unitCost;
      ord.set(m, x);
    }
  }
  return months.map((m) => {
    const s = sm.get(m);
    const hasShip = m >= shipFrom;
    const row = { month: m.slice(0, 7), orderQty: hasShip ? (s ? s.orderQty : 0) : null, shipQty: hasShip ? (s ? s.shipQty : 0) : null, orderAmount: null, shipAmount: null };
    if (po) {
      const hasPo = m >= poFrom;
      const r = rcv.get(m), o = ord.get(m);
      Object.assign(row, {
        receiptQty: hasPo ? (r ? r.qty : 0) : null, receiptAmount: hasPo ? (r ? Math.round(r.amount) : 0) : null, receiptAmountPartial: !!(r && r.unknown),
        poQty: hasPo ? (o ? o.qty : 0) : null, poAmount: hasPo ? (o ? Math.round(o.amount) : 0) : null, poAmountPartial: !!(o && o.unknown),
      });
    }
    return row;
  });
}

/** 月別を読む。{ ok, months: [...], po: 'ok'|'denied'|'error' } */
export async function readMonthly(db, sku, { poAccess = false, now = new Date(), poReader = readPoForSku } = {}) {
  try {
    const cur = jstDay(now).slice(0, 7) + '-01';
    const months = Array.from({ length: MONTHS }, (_, i) => addMonths(cur, -i));
    const from = months[months.length - 1];
    const ship = (await db.query('select month::text as month, order_qty, ship_qty from ops.sku_shipments_monthly($1::bigint, $2::date)', [sku.sku_id, from])).rows
      .map((r) => ({ month: r.month, orderQty: Number(r.order_qty), shipQty: Number(r.ship_qty) }));
    const po = poAccess ? poReader(sku.code) : null;
    const poState = !poAccess ? 'denied' : po && po.ok ? 'ok' : 'error';
    return { ok: true, months: buildMonthlySummary({ months, ship, po: po && po.ok ? po : null }), po: poState, poFrom: PO_FROM, shipFrom: SHIPMENTS_FROM };
  } catch (e) {
    return fnError(e);
  }
}
