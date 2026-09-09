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
import { loadAllowances, classifyRow, rowAllowanceKey, isActionable } from './allowance.js';

/**
 * 画面に出す状態の集計。
 *
 * 🚨 「赤字 0 件」を言えるのは、判定できない行が 0 件のときだけ (Codex 相談 2026-09-09)。
 *    unknown を黒字の山に混ぜず、独立した件数として出し続ける
 */
export function summarize(rows) {
  const s = {
    total: rows.length,
    ok: 0, incomplete: 0,
    rankEligible: 0,
    expiredNow: 0,          // 表示時の再判定で失効した数
    byMall: {},
    byExclusion: {},
    // ─── 監視の 4 つの山 ───
    actionable: 0,          // 要対応 = unallowed + returned
    unallowed: 0,           // 許容していない想定赤字
    returned: 0,            // 許容が切れた・上限を超えて戻ってきた
    allowed: 0,             // 承知のうえの赤字 (許容中)
    breakeven: 0,           // ほぼトントン (0〜5%)
    positive: 0,            // 黒字
    unknown: 0,             // 判定できない
    newlyActionable: null,  // 今回はじめて要対応になった数 (前回世代が無ければ null)
    continuedActionable: null,
    worst: null,            // いちばん深い赤字 (要対応のうち)
  };
  for (const r of rows) {
    if (r.calculation_status === 'ok') s.ok++; else s.incomplete++;
    if (r.rank_eligible_now) s.rankEligible++;
    if (r.expired_now) s.expiredNow++;
    s.byMall[r.mall] = (s.byMall[r.mall] || 0) + 1;
    if (!r.rank_eligible_now && r.rank_exclusion_reason_now) {
      s.byExclusion[r.rank_exclusion_reason_now] = (s.byExclusion[r.rank_exclusion_reason_now] || 0) + 1;
    }
    switch (r.monitor_state) {
      case 'unallowed': s.unallowed++; break;
      case 'returned': s.returned++; break;
      case 'allowed': s.allowed++; break;
      case 'breakeven': s.breakeven++; break;
      case 'ok': s.positive++; break;
      case 'unknown': s.unknown++; break;
      default: break;
    }
    if (isActionable(r.monitor_state)) {
      s.actionable++;
      if (r.is_newly_actionable === 1) s.newlyActionable = (s.newlyActionable || 0) + 1;
      if (!s.worst || Number(r.expected_profit) < Number(s.worst.expected_profit)) {
        s.worst = {
          expected_profit: r.expected_profit,
          expected_margin_rate: r.expected_margin_rate,
          product_name: r.product_name,
          mall: r.mall,
          mall_item_key: r.mall_item_key,
          fulfillment: r.fulfillment,
        };
      }
    }
  }
  // 🚨 前回世代が無い夜は「今回はじめて」を出さない。0 件と「分からない」を混ぜない
  if (rows.some(r => r.is_newly_actionable != null)) {
    s.newlyActionable = s.newlyActionable || 0;
    s.continuedActionable = s.actionable - s.newlyActionable;
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
 * 直前の世代を返す (seq が 1 つ小さいもの)。無ければ null。
 * 🚨 「今回はじめて赤字になった」を言うために使う。件数だけ見ていると、
 *    同じ件数のまま中身が入れ替わったことに気づけない (変化の見落とし)
 */
export function getPreviousGeneration(db, currentSeq) {
  if (!Number.isFinite(Number(currentSeq))) return null;
  return db.prepare(`SELECT generation_id, seq FROM expected_profit_generation
    WHERE seq < ? ORDER BY seq DESC LIMIT 1`).get(currentSeq) || null;
}

/**
 * 前世代で赤字だった出品のキー集合。
 * 🚨 前世代の許容状況までは遡らない。ここで見たいのは「赤字が新しく出たか」だけ
 */
function previousNegativeKeys(db, generationId, scope) {
  const where = ['generation_id = ?', 'expected_profit < 0', "calculation_status = 'ok'"];
  const params = [generationId];
  if (scope && scope !== 'all') { where.push('expense_scope_version = ?'); params.push(scope); }
  const rows = db.prepare(`SELECT mall, shop_id, mall_item_key, expense_scope_version
    FROM mart_listing_expected_profit WHERE ${where.join(' AND ')}`).all(...params);
  const set = new Set();
  for (const r of rows) set.add(rowAllowanceKey(r));
  return set;
}

/** 状態フィルタ。画面の「4 つの山」と 1 対 1 にする */
const STATE_FILTERS = {
  actionable: s => s === 'unallowed' || s === 'returned',
  unallowed: s => s === 'unallowed',
  returned: s => s === 'returned',
  allowed: s => s === 'allowed',
  breakeven: s => s === 'breakeven',
  unknown: s => s === 'unknown',
  positive: s => s === 'ok',
};

/**
 * 公開中の世代から行を読む。
 *
 * @param {object} opts {
 *   mall, fulfillment, salesClass, rankOnly (既定 true), includeIncomplete,
 *   state ('actionable'|'unallowed'|'returned'|'allowed'|'breakeven'|'unknown'|'positive'),
 *   sort ('margin'|'profit'), order ('desc'|'asc'), limit, offset, now
 * }
 *
 * 🚨 state を渡したときは rankOnly を見ない。判定できない行を出す山 (unknown) が
 *    「ランキング対象だけ」で潰れてしまうため
 */
export function queryPublished(opts = {}) {
  const db = opts.db || getExpectedProfitDB();
  const now = opts.now || new Date();
  const published = getPublishedGeneration(db);
  if (!published) return { published: null, rows: [], summary: summarize([]), total: 0, previous: null };

  // 🚨 モールの絞り込みだけは SQL に入れない (Codex 相談 2026-09-09)。
  //    監視の件数は「選んでいる出荷区分の全モール」でなければ意味がない。
  //    絞り込んだ結果 0 件になったのを「赤字が無い」と読み違えさせないため、
  //    集計はモール絞り込みの**前**で取り、一覧だけをあとから絞る
  const where = ['generation_id = ?'];
  const params = [published.generation_id];
  if (opts.fulfillment) { where.push('fulfillment = ?'); params.push(opts.fulfillment); }
  if (opts.salesClass != null) { where.push('sales_class = ?'); params.push(opts.salesClass); }
  // 🚨 費用範囲が違うものを既定で混ぜない (§4.8)。
  //    呼び出し側が指定しなくても self_v1 に倒す (画面が指定するだけでは契約にならない)。
  //    明示的に混ぜたいときだけ expenseScope: 'all' を渡す
  const scope = opts.expenseScope || 'self_v1';
  if (scope !== 'all') { where.push('expense_scope_version = ?'); params.push(scope); }

  const all = db.prepare(`SELECT * FROM mart_listing_expected_profit WHERE ${where.join(' AND ')}`).all(...params);

  // ── 承知のうえの赤字 (許容記録) を重ねる ──
  const allowances = loadAllowances(db, { expenseScope: scope });
  // ── 前世代と比べて「今回はじめて」を出す ──
  const previous = getPreviousGeneration(db, published.seq);
  const prevNegative = previous ? previousNegativeKeys(db, previous.generation_id, scope) : null;

  const withFreshness = all.map(r => {
    const fresh = applyFreshnessNow(r, now);
    const key = rowAllowanceKey(fresh);
    const { state, reason, allowance } = classifyRow(fresh, allowances.get(key), now);
    return {
      ...fresh,
      monitor_state: state,
      monitor_reason: reason,
      allowance: allowance || null,
      // 前世代が無い夜は null。0 件と「分からない」を混ぜない
      is_newly_actionable: prevNegative == null ? null
        : (isActionable(state) && !prevNegative.has(key) ? 1 : 0),
    };
  });
  const summary = summarize(withFreshness);
  summary.mallFiltered = opts.mall || null;   // 画面が「全モール監視」と書けるように

  let rows = opts.mall ? withFreshness.filter(r => r.mall === opts.mall) : withFreshness;
  const stateFilter = opts.state ? STATE_FILTERS[opts.state] : null;
  if (opts.state && !stateFilter) throw new Error(`state が不正です: ${opts.state}`);
  if (stateFilter) {
    rows = rows.filter(r => stateFilter(r.monitor_state));
  } else {
    // 既定 (state 指定なし) は従来どおり「今の時刻でも適格な行」だけ
    const rankOnly = opts.rankOnly !== false;
    if (rankOnly) rows = rows.filter(r => r.rank_eligible_now === 1);
  }
  if (opts.includeIncomplete === false) rows = rows.filter(r => r.calculation_status === 'ok');

  rows = sortRows(rows, opts.sort || 'margin', opts.order || 'desc');
  const total = rows.length;
  const offset = opts.offset || 0;
  const limit = opts.limit || 500;
  return {
    published,
    previous: previous ? { generation_id: previous.generation_id, seq: previous.seq } : null,
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
