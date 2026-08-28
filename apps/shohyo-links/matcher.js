/**
 * shohyo-links — 証憑 ↔ MF明細 の突合ルール (純関数・副作用なし)
 *
 * 判断はAIでなくルールで行う (2026-08-28 中原さん合意):
 *   「支払先一致・金額完全一致・日付±N日」の3キーが揃い、かつ 証憑→明細 と 明細→証憑 の
 *   両方向で相手が1件に決まるときだけ strong。迷ったら貼らない (誤添付ゼロ > 自動化率)。
 *
 * matchVoucher  … 1証憑の候補を出す (片方向)
 * matchBatch    … 受け箱の全証憑を一度に判定し、明細側からの一意性も見る (こちらをジョブで使う)
 *
 * 戻り値: { kind: 'unique'|'ambiguous'|'none', strength: 'strong'|'weak'|null, candidates: [...], reason }
 *   strong = 支払先が明細の content (カード会社が付ける加盟店名) に一致 + 金額 + 日付
 *   weak   = 支払先不明 / memo (人の自由記入) にしか一致しない / 短いキーでしか一致しない
 */
import { normalizeText, vendorKeys } from './mf-api.js';

const DAY_MS = 24 * 60 * 60 * 1000;
// 自動添付に使える支払先キーの最短長。「APPLE」が「APPLE JAPAN」に内包される類の衝突を減らす
const STRONG_KEY_MIN = 6;

function dayDiff(a, b) {
  const ta = Date.parse(a), tb = Date.parse(b);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.abs(Math.round((ta - tb) / DAY_MS));
}

/** 暦日として実在する YYYY-MM-DD か */
export function isValidDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(s || ''))) return false;
  const d = new Date(s + 'T00:00:00Z');
  return Number.isFinite(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

/** 証憑側の支払先キー。マスタ (vendor) があればそのキー、無ければ抽出した名前から */
export function voucherVendorKeys(voucher, vendors) {
  const v = voucher.vendor_id ? vendors.find(x => x.id === voucher.vendor_id) : null;
  if (v) return vendorKeys(v.name);
  if (voucher.vendor_name) return vendorKeys(voucher.vendor_name);
  return [];
}

// 加盟店名の中で「会社の名前」ではない語。カバー判定で無視する
const NOISE_TOKENS = new Set(['CO', 'LTD', 'INC', 'CORP', 'JP', 'JAPAN', 'KK', 'カブシキガイシャ', '株式会社', 'カ', 'ユ', 'ド', 'PAYPAY', 'PAY']);
const SEP = /[\s　()（）\[\]【】*＊・･,、.。/／\-_:：|]+/;

/** 加盟店名を語に分ける (正規化済み・数字を含む語と短い語は除く) */
function contentTokens(raw) {
  return String(raw || '').split(SEP).map(normalizeText).filter(t => t.length >= 3 && !/\d/.test(t) && !NOISE_TOKENS.has(t));
}

/**
 * 支払先の一致を判定する。
 * strong ('content') の条件 = 6文字以上のキーが加盟店名に含まれる かつ 加盟店名の全ての語が支払先の何かのキーで説明できる。
 * 「AMAZON」は「AMAZON WEB SERVICES」に含まれるが、WEB / SERVICES を説明できないので weak (別事業者の可能性)。
 * @returns 'content' / 'weak' (memoのみ・短いキーのみ・説明できない語がある) / null (不一致)
 */
function vendorHit(vkeys, t) {
  const content = normalizeText(t.content || '');
  const memo = normalizeText(t.memo || '');
  const strongKeys = vkeys.filter(k => k.length >= STRONG_KEY_MIN);
  if (strongKeys.some(k => content.includes(k))) {
    const toks = contentTokens(t.content);
    // 雑音語しか無い加盟店名 (決済事業者名だけ等) は strong にしない
    const covered = toks.length > 0 && toks.every(tok => vkeys.some(k => k.length >= 3 && (tok.includes(k) || k.includes(tok))));
    if (covered) return 'content';
    return 'weak';
  }
  if (vkeys.some(k => content.includes(k) || memo.includes(k))) return 'weak';
  return null;
}

/**
 * @param voucher      { doc_date, amount, vendor_id, vendor_name }
 * @param transactions MF明細 [{ id, date, value, side, content, memo, journalizing_status }]
 * @param vendors      支払い先マスタ
 * @param taken        他の証憑が既に確定 (添付済み/提案/登録待ち) している明細IDの集合
 */
export function matchVoucher(voucher, transactions, vendors, { dateWindowDays = 3, taken = new Set() } = {}) {
  const amount = Number(voucher.amount);
  if (!Number.isSafeInteger(amount) || amount <= 0) return { kind: 'none', strength: null, candidates: [], reason: 'amount_missing' };

  const vkeys = voucherVendorKeys(voucher, vendors);
  const hasVendor = vkeys.length > 0;
  const hasDate = isValidDate(voucher.doc_date);

  let candidates = [];
  const fallback = [];
  for (const t of transactions) {
    if (t.side !== 'EXPENSE' || t.journalizing_status === 'excluded') continue;
    if (Number(t.value) !== amount) continue;
    const dd = hasDate ? dayDiff(t.date, voucher.doc_date) : null;
    if (hasDate && (dd === null || dd > dateWindowDays)) continue;
    const hit = hasVendor ? vendorHit(vkeys, t) : null;
    const c = {
      tx_id: t.id, date: t.date, value: t.value, content: t.content || '', status: t.journalizing_status,
      day_diff: dd, vendor_hit: hit, taken: taken.has(t.id),
    };
    if (hasVendor && !hit) { fallback.push(c); continue; } // 支払先が分かっているのに違う相手には合わせない
    candidates.push(c);
  }
  // 支払先の表記が明細と合わない (AI/人が書いた名前がカタカナ加盟店名と一致しない等) ときは、
  // 金額+日付の候補を weak として見せる。自動添付はしないが、人が候補から選べる
  let vendorMismatch = false;
  if (hasVendor && !candidates.length && fallback.length && hasDate) { candidates = fallback; vendorMismatch = true; }

  const free = candidates.filter(c => !c.taken);
  if (!candidates.length) return { kind: 'none', strength: null, candidates: [], reason: hasVendor ? 'no_amount_vendor_match' : 'no_amount_match' };
  if (!free.length) return { kind: 'ambiguous', strength: null, candidates, reason: 'all_candidates_taken' };
  if (free.length > 1) {
    // 同日同額が複数 → 機械では区別できない。MFの一括自動添付は取引No順に付けてしまうが、ここでは付けない
    return { kind: 'ambiguous', strength: null, candidates: free, reason: 'multiple_candidates' };
  }
  const only = free[0];
  const strong = hasVendor && hasDate && only.vendor_hit === 'content' && !vendorMismatch;
  let reason = 'vendor+amount+date';
  if (!hasVendor) reason = 'amount+date (vendor unknown)';
  else if (vendorMismatch) reason = 'amount+date (vendor text mismatch)';
  else if (!hasDate) reason = 'vendor+amount (no date)';
  else if (only.vendor_hit === 'weak') reason = 'vendor(weak)+amount+date';
  return { kind: 'unique', strength: strong ? 'strong' : 'weak', candidates: [only], reason };
}

/**
 * 受け箱の全証憑を一度に判定する。証憑→明細で一意でも、同じ明細を他の証憑も一意に指していれば
 * (明細→証憑で複数) 両方 ambiguous にする。先着順で明細を奪わない。
 * @param items   [{ id, doc_date, amount, vendor_id, vendor_name }]
 * @param owners  Map<tx_id, Set<inbox_id>> 既に確定済みの明細とその所有者 (DBから)
 * @returns Map<inbox_id, result>
 */
export function matchBatch(items, transactions, vendors, { dateWindowDays = 3, owners = new Map() } = {}) {
  const first = new Map();
  for (const it of items) {
    // 自分以外が所有している明細は taken
    const taken = new Set([...owners.entries()].filter(([, set]) => [...set].some(id => id !== it.id)).map(([tx]) => tx));
    first.set(it.id, matchVoucher(it, transactions, vendors, { dateWindowDays, taken }));
  }
  // 明細→証憑 の次数
  const claims = new Map();
  for (const [id, m] of first) {
    if (m.kind !== 'unique') continue;
    const tx = m.candidates[0].tx_id;
    if (!claims.has(tx)) claims.set(tx, []);
    claims.get(tx).push(id);
  }
  const out = new Map();
  for (const [id, m] of first) {
    if (m.kind === 'unique' && claims.get(m.candidates[0].tx_id).length > 1) {
      out.set(id, { kind: 'ambiguous', strength: null, candidates: m.candidates, reason: 'multiple_vouchers_for_transaction' });
    } else {
      out.set(id, m);
    }
  }
  return out;
}

/** 証憑ファイル名の規約 `YYYY-MM-DD_金額_支払先[_ハッシュ8桁].pdf` から日付・金額・支払先を取り出す */
export function parseVoucherFileName(name) {
  const m = String(name || '').match(/^(\d{4}-\d{2}-\d{2})[_\s](\d{1,12})[_\s](.+?)(?:[_\s][0-9a-f]{8})?\.(pdf|jpe?g|png)$/i);
  if (!m || !isValidDate(m[1])) return null;
  return { doc_date: m[1], amount: Number(m[2]), vendor_name: m[3].trim() };
}
