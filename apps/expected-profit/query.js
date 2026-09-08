/**
 * 商品別 想定利益 — 画面用の読み取り
 *
 * 正本 = §9 / §15-9
 *
 * 🚨 Codex R3 の受入条件: **表示時にも失効を再判定する**。
 *    保存済みの status を信じると、バッチが止まった夜の翌日以降も「新鮮」なまま出る。
 *    世代は前夜のものでも、原価マスタの期限は今の時刻で切れうる。
 */
import { getExpectedProfitDB, getPublishedGeneration } from './db.js';
import { isExpired } from './util.js';
import { requiredInputs } from './calc.js';

/** 画面に出す状態の集計 */
export function summarize(rows) {
  const s = {
    total: rows.length,
    ok: 0, incomplete: 0,
    rankEligible: 0,
    expiredNow: 0,          // 表示時の再判定で失効した数
    byMall: {},
    byExclusion: {},
  };
  for (const r of rows) {
    if (r.calculation_status === 'ok') s.ok++; else s.incomplete++;
    if (r.rank_eligible_now) s.rankEligible++;
    if (r.expired_now) s.expiredNow++;
    s.byMall[r.mall] = (s.byMall[r.mall] || 0) + 1;
    if (!r.rank_eligible_now && r.rank_exclusion_reason_now) {
      s.byExclusion[r.rank_exclusion_reason_now] = (s.byExclusion[r.rank_exclusion_reason_now] || 0) + 1;
    }
  }
  return s;
}

/**
 * 保存済みの行に「今の時刻での失効」を重ねる。
 * 🚨 ここが表示時の再判定。保存時に ok だった入力でも、今日見たら期限切れかもしれない
 */
export function applyFreshnessNow(row, now = new Date()) {
  const { required, notApplicable } = requiredInputs(row);
  const naSet = new Set(notApplicable);
  const checks = [
    ['listing_enum', row.listing_enum_valid_until],
    ['price', row.price_valid_until],
    ['fee', row.fee_valid_until],
    ['cost', row.cost_valid_until],
    ['shipping_master', row.shipping_master_valid_until],
  ];
  let expiredNow = false;
  let firstExpired = null;
  for (const [name, validUntil] of checks) {
    if (naSet.has(name)) continue;                 // 経路上いらない入力は見ない
    if (!required.includes(name)) continue;
    // 期限そのものが無い入力 (簡易料率の手数料など) は判定しない
    if (validUntil == null) continue;
    if (isExpired(validUntil, now)) {
      expiredNow = true;
      if (!firstExpired) firstExpired = `${name}_expired_now`;
    }
  }
  const rankEligibleNow = row.rank_eligible === 1 && !expiredNow;
  return {
    ...row,
    expired_now: expiredNow ? 1 : 0,
    rank_eligible_now: rankEligibleNow ? 1 : 0,
    rank_exclusion_reason_now: rankEligibleNow ? null : (firstExpired || row.rank_exclusion_reason),
  };
}

/**
 * 公開中の世代から行を読む。
 *
 * @param {object} opts {
 *   mall, fulfillment, salesClass, rankOnly (既定 true), includeIncomplete,
 *   sort ('margin'|'profit'), order ('desc'|'asc'), limit, offset, now
 * }
 */
export function queryPublished(opts = {}) {
  const db = opts.db || getExpectedProfitDB();
  const now = opts.now || new Date();
  const published = getPublishedGeneration(db);
  if (!published) return { published: null, rows: [], summary: summarize([]), total: 0 };

  const where = ['generation_id = ?'];
  const params = [published.generation_id];
  if (opts.mall) { where.push('mall = ?'); params.push(opts.mall); }
  if (opts.fulfillment) { where.push('fulfillment = ?'); params.push(opts.fulfillment); }
  if (opts.salesClass != null) { where.push('sales_class = ?'); params.push(opts.salesClass); }
  // 🚨 費用範囲が違うものを既定で混ぜない (§4.8)。
  //    呼び出し側が指定しなくても self_v1 に倒す (画面が指定するだけでは契約にならない)。
  //    明示的に混ぜたいときだけ expenseScope: 'all' を渡す
  const scope = opts.expenseScope || 'self_v1';
  if (scope !== 'all') { where.push('expense_scope_version = ?'); params.push(scope); }

  const all = db.prepare(`SELECT * FROM mart_listing_expected_profit WHERE ${where.join(' AND ')}`).all(...params);
  const withFreshness = all.map(r => applyFreshnessNow(r, now));
  const summary = summarize(withFreshness);

  // 既定は「今の時刻でも適格な行」だけ
  const rankOnly = opts.rankOnly !== false;
  let rows = rankOnly ? withFreshness.filter(r => r.rank_eligible_now === 1) : withFreshness;
  if (opts.includeIncomplete === false) rows = rows.filter(r => r.calculation_status === 'ok');

  rows = sortRows(rows, opts.sort || 'margin', opts.order || 'desc');
  const total = rows.length;
  const offset = opts.offset || 0;
  const limit = opts.limit || 500;
  return {
    published,
    rows: rows.slice(offset, offset + limit),
    summary,
    total,
  };
}

/**
 * 並び替え。
 * 🚨 NULL は末尾に固定し、同率は安定した順序にする (§9.3)
 */
export function sortRows(rows, key = 'margin', order = 'desc') {
  const col = key === 'profit' ? 'expected_profit' : 'expected_margin_rate';
  const dir = order === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {
    const av = a[col];
    const bv = b[col];
    const an = av == null || !Number.isFinite(av);
    const bn = bv == null || !Number.isFinite(bv);
    if (an && bn) return tieBreak(a, b);
    if (an) return 1;            // NULL は常に末尾 (昇順でも降順でも)
    if (bn) return -1;
    if (av !== bv) return (av - bv) * dir;
    return tieBreak(a, b);
  });
}

function tieBreak(a, b) {
  // 利益率 → 利益額 → キー の順で安定させる
  const ap = Number.isFinite(a.expected_profit) ? a.expected_profit : -Infinity;
  const bp = Number.isFinite(b.expected_profit) ? b.expected_profit : -Infinity;
  if (ap !== bp) return bp - ap;
  const ak = `${a.mall}${a.shop_id}${a.mall_item_key}`;
  const bk = `${b.mall}${b.shop_id}${b.mall_item_key}`;
  return ak < bk ? -1 : (ak > bk ? 1 : 0);
}

/** CSV の1セルを安全にする */
export function csvCell(v, { isExternalText = false } = {}) {
  if (v == null) return '';
  let s = String(v);
  // 🚨 数式インジェクション対策は「外から来た文字列」にだけ適用する。
  //    負の利益額まで文字列化しない (§9.4)
  if (isExternalText && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  if (/[",\n\r]/.test(s)) s = `"${s.replace(/"/g, '""')}"`;
  return s;
}
