/**
 * shohyo-links — 証憑 ↔ MF明細 の突合ルール (純関数・副作用なし)
 *
 * 判断はAIでなくルールで行う (2026-08-28 中原さん合意):
 *   「支払先一致・金額完全一致・日付±N日」の3キーが揃い、かつ明細側・証憑側の両方から
 *   相手が1件に決まるときだけ strong。迷ったら貼らない (誤添付ゼロ > 自動化率)。
 *
 * 戻り値: { kind: 'unique'|'ambiguous'|'none', strength: 'strong'|'weak'|null, candidates: [...], reason }
 *   strong = 3キー一致 (自動添付の対象)
 *   weak   = 支払先が分からず 金額+日付 だけで1件に絞れた (人の確認が要る)
 */
import { normalizeText, vendorKeys } from './mf-api.js';

const DAY_MS = 24 * 60 * 60 * 1000;

function dayDiff(a, b) {
  const ta = Date.parse(a), tb = Date.parse(b);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.abs(Math.round((ta - tb) / DAY_MS));
}

/** 証憑側の支払先キー。マスタ (vendor) があればそのキー、無ければ抽出した名前から */
export function voucherVendorKeys(voucher, vendors) {
  const v = voucher.vendor_id ? vendors.find(x => x.id === voucher.vendor_id) : null;
  if (v) return vendorKeys(v.name);
  if (voucher.vendor_name) return vendorKeys(voucher.vendor_name);
  return [];
}

/**
 * @param voucher      { doc_date, amount, vendor_id, vendor_name }
 * @param transactions MF明細 [{ id, date, value, side, content, memo, journalizing_status }]
 * @param vendors      支払い先マスタ (vendorKeys のキャッシュ用に _keys を持たせてよい)
 * @param taken        既に他の証憑が確定している明細IDの集合 (証憑側の一意性)
 */
export function matchVoucher(voucher, transactions, vendors, { dateWindowDays = 3, taken = new Set() } = {}) {
  const amount = Number(voucher.amount);
  if (!Number.isFinite(amount) || amount <= 0) return { kind: 'none', strength: null, candidates: [], reason: 'amount_missing' };

  const vkeys = voucherVendorKeys(voucher, vendors);
  const hasVendor = vkeys.length > 0;
  const hasDate = /^\d{4}-\d{2}-\d{2}$/.test(String(voucher.doc_date || ''));

  const candidates = [];
  for (const t of transactions) {
    if (t.side !== 'EXPENSE' || t.journalizing_status === 'excluded') continue;
    if (Number(t.value) !== amount) continue;
    const dd = hasDate ? dayDiff(t.date, voucher.doc_date) : null;
    if (hasDate && (dd === null || dd > dateWindowDays)) continue;
    const text = normalizeText([t.content, t.memo].filter(Boolean).join('|'));
    const vendorHit = hasVendor && vkeys.some(k => text.includes(k));
    if (hasVendor && !vendorHit) continue; // 支払先が分かっているのに違う相手には合わせない
    candidates.push({
      tx_id: t.id, date: t.date, value: t.value, content: t.content || '', status: t.journalizing_status,
      day_diff: dd, vendor_hit: vendorHit, taken: taken.has(t.id),
    });
  }

  const free = candidates.filter(c => !c.taken);
  if (!candidates.length) return { kind: 'none', strength: null, candidates: [], reason: hasVendor ? 'no_amount_vendor_match' : 'no_amount_match' };
  if (!free.length) return { kind: 'ambiguous', strength: null, candidates, reason: 'all_candidates_taken' };
  if (free.length > 1) {
    // 同日同額が複数 → 機械では区別できない。MFの一括自動添付は取引No順に付けてしまうが、ここでは付けない
    return { kind: 'ambiguous', strength: null, candidates: free, reason: 'multiple_candidates' };
  }
  const only = free[0];
  const strength = (hasVendor && hasDate) ? 'strong' : 'weak';
  return {
    kind: 'unique', strength, candidates: [only],
    reason: strength === 'strong' ? 'vendor+amount+date' : (hasVendor ? 'vendor+amount (no date)' : 'amount+date (vendor unknown)'),
  };
}

/** 証憑ファイル名の規約 `YYYY-MM-DD_金額_支払先[_ハッシュ8桁].pdf` から日付・金額・支払先を取り出す */
export function parseVoucherFileName(name) {
  const m = String(name || '').match(/^(\d{4}-\d{2}-\d{2})[_\s](\d{1,12})[_\s](.+?)(?:[_\s][0-9a-f]{8})?\.(pdf|jpe?g|png)$/i);
  if (!m) return null;
  return { doc_date: m[1], amount: Number(m[2]), vendor_name: m[3].trim() };
}
