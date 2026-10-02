/**
 * amazon-settlement-v2.js — 決済レポート V2 (GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2) を V1 の形に並べ直す
 *
 * V1 (GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE) は 2026-11-11 に廃止。V2 は形が違う:
 *   - V1 = 金額の種類ごとに列 (price-type/amount・item-related-fee-type/amount・promotion-type/amount・other-amount …)
 *   - V2 = amount-type / amount-description / amount の 3 列に縦に並ぶ + posted-date-time (日付の書き方も違う)
 * 下流 (raw_amazon_settlement_lines → mart) は V1 の形と business_line_key (同じ決済の中で同じ行をまとめる鍵) で動いている →
 * V2 を V1 の列の TSV に並べ直して、今の取込 (prepareReportTsv) にそのまま通す。
 *
 * 並べ直しの規則 (2026-09-28 に V1 / V2 を 6 期間 突き合わせて決めた = business_line_key が全部一致):
 *   ① 品物の行: amount-type ItemPrice → price-type / ItemFees・Points・Item Fee Adjustment → item-related-fee-type / Promotion → promotion-type
 *      (種類の名前 = amount-description)。個数は V1 では品物の行に付かない
 *      税の取り直し (Order_Retrocharge / Refund_Retrocharge の Tax・ShippingTax だけ) も同じ規則 (2026-10-02 に V1 の 2026-01 の決済で確かめた)
 *   ② 注文の品物ごとに「個数だけの行」を 1 行作る (V1 は Order の品物ごとに金額の無い行 + quantity-purchased。ItemPrice / Principal の行から)
 *   ③ ポイントの行は V2 で order-item-code が空 → 同じ取引・注文・SKU・時刻の品物の行から補う (1 つに決まるときだけ)
 *   ④ 品物でない行 (補てん・手数料・その他) は other-amount。取引の種類の名前は V1 の書き方に直す:
 *        other-transaction → amount-description (REVERSAL_REIMBURSEMENT・Subscription Fee …)
 *        other-transaction - Reversal / - Correction → amount-description + ' - Reversal' / ' - Correction' (Storage Fee - Reversal …)
 *        AmazonFees / FBAFees → amount-type (Amazon Easy Ship Charges・FBA Removal Order: Return Fee …)。AmazonFees は item-related-fee (種類 = amount-type)
 *        それ以外 (Other・SAFE-T Reimbursement・BuyerRecharge …) → そのまま
 *      「Base fee」の直後に同じ中身の料金の部分 (Tax on fee・Discount on Fee など説明に fee を含む。次の Base fee の前まで) が続けば 1 行にまとめる
 *      (V1 は 本体 + 割引 + 税 の 1 行 = 8/20 の返送手数料 -115 + 60 - 5 = -60)。それ以外はまとめない (同じ中身の行が 2 行あることがある = 倉庫の破損など)
 *   ⑤ amount-type 'Other transactions' (SAFE-T・紛失の補てん) は V1 では price-type = 'SAFE-T Reimbursement'・order-item-code 空
 *   🚨 通すのは 6 期間で確かめた組み合わせだけ (KNOWN_* の一覧)。それ以外・料金の部分が本体の前に来る・日時が空 / 暦に無い は unknown に数える。
 *      呼び手 (fetch-amazon-settlements.js) は unknown が 1 つでもあるレポートを **取り込まない** (Codex #1508 R1 High:
 *      取り込んだ後で規則を直して入れ直すと business_line_key が変わり、古い行と新しい行が二重になる)。
 *      V2 のレポートは約 90 日取り直せる = 規則を足してから入れれば落ちない
 *   説明をそのまま取引の種類にする型 (other-transaction の FBA Inventory Reimbursement・other-transaction・Inbound Defect Fee) は
 *   説明の値を問わず通す (9 種類の説明で同じ規則を確かめた = 新しい補てんの種類で取込を止めない)
 */

export const V1_COLUMNS = ['settlement-id', 'settlement-start-date', 'settlement-end-date', 'deposit-date', 'total-amount', 'currency', 'transaction-type', 'order-id', 'merchant-order-id', 'adjustment-id', 'shipment-id', 'marketplace-name', 'shipment-fee-type', 'shipment-fee-amount', 'order-fee-type', 'order-fee-amount', 'fulfillment-id', 'posted-date', 'order-item-code', 'merchant-order-item-id', 'merchant-adjustment-item-id', 'sku', 'quantity-purchased', 'price-type', 'price-amount', 'item-related-fee-type', 'item-related-fee-amount', 'misc-fee-amount', 'other-fee-amount', 'other-fee-reason-description', 'promotion-id', 'promotion-type', 'promotion-amount', 'direct-payment-type', 'direct-payment-amount', 'other-amount'];
export const V2_COLUMNS = ['settlement-id', 'settlement-start-date', 'settlement-end-date', 'deposit-date', 'total-amount', 'currency', 'transaction-type', 'order-id', 'merchant-order-id', 'adjustment-id', 'shipment-id', 'marketplace-name', 'amount-type', 'amount-description', 'amount', 'fulfillment-id', 'posted-date', 'posted-date-time', 'order-item-code', 'merchant-order-item-id', 'merchant-adjustment-item-id', 'sku', 'quantity-purchased', 'promotion-id'];

// '2026/09/07 00:20:09 UTC' → '2026-09-07T00:20:09+00:00' (V1 の書き方)。空はそのまま (要るかは呼び手が決める)。形が違う・暦に無い日時は止める (黙って読み違えない)
export function v2DateTimeToV1(s) {
  if (s == null || s === '') return '';
  const m = String(s).match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2}) UTC$/);
  if (!m) throw new Error(`V2 の日時の形が想定と違う: ${s}`);
  const [y, mo, d, h, mi, se] = m.slice(1).map(Number);
  const t = new Date(Date.UTC(y, mo - 1, d, h, mi, se));
  if (t.getUTCFullYear() !== y || t.getUTCMonth() !== mo - 1 || t.getUTCDate() !== d || t.getUTCHours() !== h || t.getUTCMinutes() !== mi || t.getUTCSeconds() !== se) throw new Error(`V2 の日時が暦に無い: ${s}`);
  return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6]}+00:00`;
}

const toCents = (s) => { const n = Number(s); if (s === '' || s == null || !Number.isFinite(n)) throw new Error(`V2 の金額が数でない: ${s}`); return Math.round(n * 100); };
const fromCents = (c) => (c / 100).toFixed(2);

export function parseV2Tsv(text) {
  const lines = String(text).split(/\r?\n/);
  const header = lines[0].split('\t');
  const missing = V2_COLUMNS.filter((c) => !header.includes(c));
  if (missing.length) throw new Error(`V2 の列が足りない: ${missing.join(', ')}`);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t');
    const o = {};
    header.forEach((h, j) => (o[h] = cols[j] ?? ''));
    rows.push(o);
  }
  return { header, rows };
}

const ITEM_TYPES = {
  ItemPrice: ['price-type', 'price-amount'],
  ItemFees: ['item-related-fee-type', 'item-related-fee-amount'],
  Points: ['item-related-fee-type', 'item-related-fee-amount'],
  'Item Fee Adjustment': ['item-related-fee-type', 'item-related-fee-amount'],
  Promotion: ['promotion-type', 'promotion-amount'],
};
// 6 期間 (2026-06-29〜09-21) で確かめた組み合わせ。これ以外は unknown (= そのレポートは取り込まない)
// 🆕 2026-10-02: ItemPrice に Order_Retrocharge / Refund_Retrocharge (税の取り直し・その取り消し)。決済 12222191753 (2025-12-29〜2026-01-12) で
//   Order_Retrocharge の Tax / ShippingTax が来て、決済が丸ごと止まった。V1 の同じ決済では 取引 = Order_Retrocharge・price-type = Tax / ShippingTax・
//   注文番号・計上日・marketplace だけ (SKU・品物の番号・個数は空・個数だけの行は無い) = ① の規則 (② は Order の Principal だけ) のままで同じ形になる。
//   Retrocharge の中身は公式 (Finances API の RetrochargeEvent) で BaseTax / ShippingTax と 米国の代理徴収の源泉 (RetrochargeTaxWithheldList) だけ =
//   手数料・値引き・ポイントは来ない → ItemPrice だけに足す。源泉 (ItemWithheldTax など) は ITEM_TYPES に無い = 来たら unknown で止まる (足さない)
//   🚨 説明 (amount-description) も Tax / ShippingTax だけ通す (KNOWN_ITEM_DESC。Principal などは unknown = 止める。Codex #1582 R1 Medium)。
//   Refund_Retrocharge は実物を見ていない (手元の V1 にも無い) = Order_Retrocharge と同じ形の仮定。違う説明が来たら止まる
const KNOWN_ITEM_TX = { ItemPrice: ['Order', 'Refund', 'A-to-z Guarantee Refund', 'Chargeback Refund', 'Order_Retrocharge', 'Refund_Retrocharge'], ItemFees: ['Order', 'Refund', 'A-to-z Guarantee Refund', 'Chargeback Refund'], Points: ['Order', 'Refund'], Promotion: ['Order', 'Refund'], 'Item Fee Adjustment': ['Fee Adjustment'] };
// 品物の行で説明 (amount-description) まで決めている組み合わせ: 取引 → amount-type → 説明 (ここに無い取引は説明を問わない = 今まで通り)
const KNOWN_ITEM_DESC = { Order_Retrocharge: { ItemPrice: ['Tax', 'ShippingTax'] }, Refund_Retrocharge: { ItemPrice: ['Tax', 'ShippingTax'] } };
const isKnownItem = (tx, at, desc) => (KNOWN_ITEM_TX[at] || []).includes(tx) && (!KNOWN_ITEM_DESC[tx] || (KNOWN_ITEM_DESC[tx][at] || []).includes(desc));
// 料金 (本体 + 部分): 取引 → 料金の種類 (amount-type)
const KNOWN_FEE = { AmazonFees: ['Amazon Easy Ship Charges'], FBAFees: ['FBA Inventory Storage Fee', 'FBA Long Term Storage Fee', 'FBA Removal Order: Return Fee'] };
const FEE_PARTS = new Set(['Tax on fee', 'Discount on Fee']);   // 「Base fee」の後に続けてまとめる部分
// 説明をそのまま取引の種類にする型 (説明の値は問わない)
const DESC_AS_TX = { 'other-transaction': ['FBA Inventory Reimbursement', 'other-transaction', 'Inbound Defect Fee'] };
// そのほか: 取引 → amount-type → 説明
const KNOWN_MISC = { Other: { 'Other transactions': ['Reimbursement for Lost packages'] }, 'SAFE-T Reimbursement': { 'Other transactions': ['SAFE-T reimbursement'] }, 'other-transaction - Reversal': { 'other-transaction': ['Storage Fee'] }, 'other-transaction - Correction': { 'other-transaction': ['Storage Fee'] } };
const isKnownNonItem = (tx, at, desc) => (KNOWN_FEE[tx] && KNOWN_FEE[tx].includes(at) && desc === 'Base fee')
  || (DESC_AS_TX[tx] && DESC_AS_TX[tx].includes(at) && desc !== '')
  || !!(KNOWN_MISC[tx] && KNOWN_MISC[tx][at] && KNOWN_MISC[tx][at].includes(desc));
const COPY_COLUMNS = ['settlement-id', 'currency', 'transaction-type', 'order-id', 'merchant-order-id', 'adjustment-id', 'shipment-id', 'marketplace-name', 'fulfillment-id', 'order-item-code', 'merchant-order-item-id', 'merchant-adjustment-item-id', 'sku', 'promotion-id'];
// 「本体」と「税」を同じ手数料と見なす中身 (金額と説明以外の全部)
const PAIR_FIELDS = V2_COLUMNS.filter((c) => c !== 'amount' && c !== 'amount-description');
const sameExceptAmount = (a, b) => PAIR_FIELDS.every((c) => (a[c] ?? '') === (b[c] ?? ''));

function emptyV1() { return Object.fromEntries(V1_COLUMNS.map((c) => [c, ''])); }
function baseV1(r) {
  const o = emptyV1();
  for (const c of COPY_COLUMNS) o[c] = r[c] ?? '';
  if (!r['posted-date-time']) throw new Error(`V2 の明細の日時 (posted-date-time) が空: ${r['transaction-type']} / ${r['amount-type']} / ${r['order-id'] || r.sku || ''}`);   // 空だと月の集計から落ちる (Codex #1508 R1)
  o['posted-date'] = v2DateTimeToV1(r['posted-date-time']);
  return o;
}

/** V2 の行 → V1 の行 (列名は V1)。unknown = 規則に無い組み合わせ ([名前, 件数])・itemCodeUnresolved = ポイントの行で品物の番号が 1 つに決まらなかった数 */
export function v2RowsToV1Rows(rows) {
  const out = [];
  const unknown = new Map();
  const bump = (k) => unknown.set(k, (unknown.get(k) || 0) + 1);
  // ③ のための索引: (取引・注文・調整・SKU・時刻) → 品物の番号
  const itemCodes = new Map();
  const ikey = (r) => JSON.stringify([r['transaction-type'], r['order-id'], r['adjustment-id'], r.sku, r['posted-date-time']]);
  for (const r of rows) if (r['order-item-code']) { const k = ikey(r); const s = itemCodes.get(k) || new Set(); s.add(r['order-item-code']); itemCodes.set(k, s); }
  let itemCodeUnresolved = 0;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const isHeader = r['total-amount'] !== '' && r['transaction-type'] === '' && r['posted-date'] === '';
    if (isHeader) {
      const o = emptyV1();
      o['settlement-id'] = r['settlement-id']; o['total-amount'] = r['total-amount']; o.currency = r.currency;
      for (const c of ['settlement-start-date', 'settlement-end-date', 'deposit-date']) o[c] = v2DateTimeToV1(r[c]);
      out.push(o);
      continue;
    }
    if (r['transaction-type'] === '' && r['amount-type'] === '' && r.amount === '') continue;   // 空の行
    const tx = r['transaction-type'], at = r['amount-type'], desc = r['amount-description'];
    const map = ITEM_TYPES[at];
    if (map) {
      const o = baseV1(r);
      o[map[0]] = desc;
      o[map[1]] = fromCents(toCents(r.amount));
      if (!(KNOWN_ITEM_TX[at] || []).includes(tx)) bump(`${tx} | ${at}`);
      else if (!isKnownItem(tx, at, desc)) bump(`${tx} | ${at} | ${desc}`);   // 取引 × 種類は知っているが説明が違う (Retrocharge の Principal など)
      if (at === 'Points' && !o['order-item-code']) {
        const s = itemCodes.get(ikey(r));
        if (s && s.size === 1) o['order-item-code'] = [...s][0]; else itemCodeUnresolved++;
      }
      out.push(o);
      if (tx === 'Order' && at === 'ItemPrice' && desc === 'Principal') {   // ② 個数だけの行
        const q = baseV1(r);
        q['quantity-purchased'] = r['quantity-purchased'];
        out.push(q);
      }
      continue;
    }
    // ④ 品物でない行 (料金の部分が本体の前・単独で来たら unknown = まとめ方が V1 と違いうる)
    if (!isKnownNonItem(tx, at, desc)) bump(`${tx} | ${at} | ${desc}`);
    let cents = toCents(r.amount);
    if (desc === 'Base fee' && KNOWN_FEE[tx]) {
      while (i + 1 < rows.length) {
        const nx = rows[i + 1];
        if (!FEE_PARTS.has(nx['amount-description']) || !sameExceptAmount(r, nx)) break;
        cents += toCents(nx.amount); i++;
      }
    }
    const o = baseV1(r);
    if (tx === 'other-transaction') o['transaction-type'] = desc;
    else if (/^other-transaction - /.test(tx)) o['transaction-type'] = `${desc} - ${tx.slice('other-transaction - '.length)}`;
    else if (tx === 'AmazonFees' || tx === 'FBAFees') o['transaction-type'] = at;
    if (tx === 'AmazonFees') { o['item-related-fee-type'] = at; o['item-related-fee-amount'] = fromCents(cents); }
    else o['other-amount'] = fromCents(cents);
    if (at === 'Other transactions') { o['price-type'] = 'SAFE-T Reimbursement'; o['order-item-code'] = ''; }   // ⑤
    o['quantity-purchased'] = r['quantity-purchased'];
    out.push(o);
  }
  return { rows: out, unknown: [...unknown], itemCodeUnresolved };
}

export function v1RowsToTsv(rows) {
  return [V1_COLUMNS.join('\t'), ...rows.map((o) => V1_COLUMNS.map((c) => o[c] ?? '').join('\t'))].join('\n') + '\n';
}

export function convertV2TsvToV1Tsv(text) {
  const { rows } = parseV2Tsv(text);
  const r = v2RowsToV1Rows(rows);
  return { tsv: v1RowsToTsv(r.rows), unknown: r.unknown, itemCodeUnresolved: r.itemCodeUnresolved, v2Rows: rows.length, v1Rows: r.rows.length };
}
