/**
 * 商品別 想定利益 — 「承知のうえの赤字」(許容記録)
 *
 * ねらい: 意図して赤字にしている出品 (在庫処分・集客など) を要対応から外す。
 *
 * 🚨 設計の要点 (Codex 相談 2026-09-09):
 *   - 許容フラグ 1 個では **期限切れも上限超過も検出できない**。
 *     必ず「1個あたりの損失上限」と「期限」を持たせ、外れたら自動で要対応へ戻す
 *   - 無期限は作らせない (valid_until は必須)
 *   - 判定できない行には許容を効かせない。数字が信用できないものを
 *     「承知のうえ」に見せると、本当の赤字を隠してしまう
 *   - 同じ商品でも **モール・出品コード・出荷区分の組でしか効かない**。
 *     自社出荷で許容した判断を FBA に自動適用しない (費用の範囲が違う)
 */
import { jstDateStr } from '../../lib/jst-date.js';

/** 理由。画面の選択肢と 1 対 1 にする (自由入力だけにすると集計できない) */
export const ALLOWANCE_REASONS = {
  stock_clearance: '在庫処分',
  customer_acquisition: '集客',
  partner_commitment: '取引先との約束',
  other: 'その他',
};

/** 「ほぼトントン」の上限。これ未満の黒字は、値上げ・送料改定で赤字に落ちる予備軍 */
export const BREAKEVEN_MAX_RATE = 0.05;

/** 許容の上限として受け付ける最大額。桁の打ち間違いで青天井にしない */
export const MAX_LOSS_CAP_YEN = 100000;

/** 出品を一意に指す文字列。区切りは品番に出てこない制御文字を使う */
export function allowanceKey(mall, shopId, mallItemKey, scope) {
  return [mall, shopId, mallItemKey, scope].join('');
}

/** 行から許容記録のキーを作る */
export function rowAllowanceKey(row) {
  return allowanceKey(row.mall, row.shop_id, row.mall_item_key, row.expense_scope_version);
}

/**
 * 有効な許容記録を読む (取り消し済みは除く)。
 * @returns {Map<string, object>} allowanceKey() -> 記録
 */
export function loadAllowances(db, opts = {}) {
  const where = ['revoked_at IS NULL'];
  const params = [];
  if (opts.expenseScope && opts.expenseScope !== 'all') {
    where.push('expense_scope_version = ?');
    params.push(opts.expenseScope);
  }
  const rows = db.prepare(
    `SELECT * FROM expected_profit_allowance WHERE ${where.join(' AND ')}`).all(...params);
  const map = new Map();
  for (const r of rows) {
    map.set(allowanceKey(r.mall, r.shop_id, r.mall_item_key, r.expense_scope_version), r);
  }
  return map;
}

/**
 * 1 行の状態を決める。
 *
 * 優先順位 (Codex 相談で確定):
 *   1. 数字が信用できない            → unknown  (許容記録があっても安全扱いしない)
 *   2. 黒字                          → ok / breakeven
 *   3. 赤字 × 許容なし               → unallowed
 *   4. 赤字 × 許容が効いている       → allowed
 *   5. 赤字 × 期限切れ or 上限超過   → returned (要対応へ自動復帰)
 *
 * @param {object} row     applyFreshnessNow() を通した行
 * @param {object} [allow] 有効な許容記録 (無ければ undefined)
 * @param {Date}   [now]
 * @returns {{state: string, reason: string|null, allowance: object|null}}
 */
export function classifyRow(row, allow, now = new Date()) {
  // ── 1. 判定できるか ──
  // 🚨 ランキング対象外の行をここで「黒字」とも「赤字」とも言わない。
  //    まとめ買い出品のように送料が単品前提のままの行は、利益が過大に出る
  const judgeable = row.calculation_status === 'ok'
    && row.expected_profit != null
    && Number.isFinite(Number(row.expected_profit))
    && !row.expired_now
    && row.rank_eligible_now === 1;
  if (!judgeable) {
    return {
      state: 'unknown',
      reason: row.rank_exclusion_reason_now || row.incomplete_reason || null,
      allowance: null,
    };
  }

  const profit = Number(row.expected_profit);
  const rate = row.expected_margin_rate == null ? null : Number(row.expected_margin_rate);

  // ── 2. 黒字 ──
  if (profit >= 0) {
    const breakeven = rate != null && Number.isFinite(rate) && rate < BREAKEVEN_MAX_RATE;
    return { state: breakeven ? 'breakeven' : 'ok', reason: null, allowance: null };
  }

  // ── 3〜5. 赤字 ──
  if (!allow) return { state: 'unallowed', reason: null, allowance: null };

  const today = jstDateStr(now);
  if (today < allow.valid_from) {
    // まだ始まっていない許容。効かせない (先の日付で登録して今日から隠す、をさせない)
    return { state: 'unallowed', reason: 'allowance_not_started', allowance: allow };
  }
  if (today > allow.valid_until) {
    return { state: 'returned', reason: 'allowance_expired', allowance: allow };
  }
  // 🚨 円未満は切り上げて比べる。−300.4 円を「上限 300 円以内」に通さない
  const loss = Math.ceil(-profit);
  if (loss > allow.loss_cap_yen) {
    return { state: 'returned', reason: 'allowance_cap_exceeded', allowance: allow };
  }
  return { state: 'allowed', reason: null, allowance: allow };
}

/** 要対応 (今日つぶすべきもの) か */
export function isActionable(state) {
  return state === 'unallowed' || state === 'returned';
}

/** 状態を日本語にする。英語のまま画面に出さない */
export const STATE_LABEL = {
  unallowed: '未許容の赤字',
  returned: '要対応に復帰',
  allowed: '承知のうえ',
  breakeven: 'ほぼトントン',
  ok: '黒字',
  unknown: '判定できない',
};

/** 復帰・不成立の理由を日本語にする */
export const ALLOWANCE_REASON_LABEL = {
  allowance_expired: '許容の期限が切れました',
  allowance_cap_exceeded: '決めた損失上限を超えました',
  allowance_not_started: '許容の開始日がまだ来ていません',
};

/**
 * 入力を検証して正規化する。
 * 🚨 画面の必須表示だけに頼らない。API に直接投げられても通さない
 */
export function normalizeAllowanceInput(input = {}) {
  const errors = [];
  const mall = String(input.mall || '').trim();
  const shopId = String(input.shop_id || '').trim();
  const key = String(input.mall_item_key || '').trim();
  const scope = String(input.expense_scope_version || '').trim();
  if (!mall) errors.push('モールが指定されていません');
  if (!shopId) errors.push('店舗が指定されていません');
  if (!key) errors.push('出品コードが指定されていません');
  if (scope !== 'self_v1' && scope !== 'fba_v1') errors.push('出荷区分は self_v1 か fba_v1 です');

  const reasonCode = String(input.reason_code || '').trim();
  if (!ALLOWANCE_REASONS[reasonCode]) errors.push('理由を選んでください');
  const reasonNote = String(input.reason_note || '').trim();
  if (!reasonNote) errors.push('狙いの説明を書いてください');
  if (reasonNote.length > 500) errors.push('狙いの説明は 500 文字までです');

  // 上限は「1個あたり何円までの損失を許すか」。正の整数
  const capRaw = input.loss_cap_yen;
  const cap = typeof capRaw === 'number' ? capRaw : parseInt(String(capRaw ?? '').replace(/[,\s]/g, ''), 10);
  if (!Number.isFinite(cap) || !Number.isInteger(cap) || cap < 0) {
    errors.push('損失上限は 0 以上の整数 (円) です');
  } else if (cap > MAX_LOSS_CAP_YEN) {
    errors.push(`損失上限は ${MAX_LOSS_CAP_YEN.toLocaleString('ja-JP')} 円までです`);
  }

  const validFrom = String(input.valid_from || '').trim() || jstDateStr();
  const validUntil = String(input.valid_until || '').trim();
  const dateRe = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRe.test(validFrom)) errors.push('開始日は YYYY-MM-DD です');
  // 🚨 無期限を作らせない。期限が無い許容は、二度と見直されない
  if (!validUntil) errors.push('期限は必須です (無期限にはできません)');
  else if (!dateRe.test(validUntil)) errors.push('期限は YYYY-MM-DD です');
  // 🚨 「過去の日付」を先に見る。開始日が既定 (今日) のときに
  //    「期限が開始日より前です」とだけ出ると、何を直せばいいのか伝わらない
  else if (validUntil < jstDateStr()) errors.push('過去の日付は期限にできません');
  else if (dateRe.test(validFrom) && validUntil < validFrom) errors.push('期限が開始日より前です');

  const decidedBy = String(input.decided_by || '').trim();
  if (!decidedBy) errors.push('決めた人を入れてください');

  return {
    errors,
    value: {
      mall, shop_id: shopId, mall_item_key: key, expense_scope_version: scope,
      reason_code: reasonCode, reason_note: reasonNote,
      loss_cap_yen: Number.isFinite(cap) ? cap : null,
      valid_from: validFrom, valid_until: validUntil,
      decided_by: decidedBy,
      review_by: String(input.review_by || '').trim() || null,
      snapshot_profit: input.snapshot_profit == null ? null : Number(input.snapshot_profit),
      snapshot_generation_id: String(input.snapshot_generation_id || '').trim() || null,
    },
  };
}

/** 登録・更新 (同じ出品に 2 本作らせない。上書きは履歴に残す) */
export function upsertAllowance(db, value, actor, now = new Date()) {
  const at = now.toISOString();
  const existing = db.prepare(`SELECT created_at, created_by FROM expected_profit_allowance
    WHERE mall = ? AND shop_id = ? AND mall_item_key = ? AND expense_scope_version = ?`)
    .get(value.mall, value.shop_id, value.mall_item_key, value.expense_scope_version);
  const row = {
    ...value,
    created_at: existing ? existing.created_at : at,
    created_by: existing ? existing.created_by : actor,
    updated_at: at,
    revoked_at: null,
    revoked_by: null,
  };
  const tx = db.transaction(() => {
    db.prepare(`INSERT OR REPLACE INTO expected_profit_allowance (
      mall, shop_id, mall_item_key, expense_scope_version,
      reason_code, reason_note, loss_cap_yen, valid_from, valid_until,
      decided_by, review_by, snapshot_profit, snapshot_generation_id,
      created_at, created_by, updated_at, revoked_at, revoked_by
    ) VALUES (
      @mall, @shop_id, @mall_item_key, @expense_scope_version,
      @reason_code, @reason_note, @loss_cap_yen, @valid_from, @valid_until,
      @decided_by, @review_by, @snapshot_profit, @snapshot_generation_id,
      @created_at, @created_by, @updated_at, @revoked_at, @revoked_by
    )`).run(row);
    writeLog(db, row, existing ? 'update' : 'create', actor, at);
  });
  tx();
  return row;
}

/** 取り消し (行は消さない。判断の履歴は残す) */
export function revokeAllowance(db, key, actor, now = new Date()) {
  const at = now.toISOString();
  const found = db.prepare(`SELECT * FROM expected_profit_allowance
    WHERE mall = ? AND shop_id = ? AND mall_item_key = ? AND expense_scope_version = ?
      AND revoked_at IS NULL`)
    .get(key.mall, key.shop_id, key.mall_item_key, key.expense_scope_version);
  if (!found) return null;
  const tx = db.transaction(() => {
    db.prepare(`UPDATE expected_profit_allowance SET revoked_at = ?, revoked_by = ?, updated_at = ?
      WHERE mall = ? AND shop_id = ? AND mall_item_key = ? AND expense_scope_version = ?`)
      .run(at, actor, at, key.mall, key.shop_id, key.mall_item_key, key.expense_scope_version);
    writeLog(db, found, 'revoke', actor, at);
  });
  tx();
  return { ...found, revoked_at: at, revoked_by: actor };
}

function writeLog(db, row, action, actor, at) {
  db.prepare(`INSERT INTO expected_profit_allowance_log
    (mall, shop_id, mall_item_key, expense_scope_version, action, payload, actor, acted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(row.mall, row.shop_id, row.mall_item_key, row.expense_scope_version,
      action, JSON.stringify(row), actor, at);
}
