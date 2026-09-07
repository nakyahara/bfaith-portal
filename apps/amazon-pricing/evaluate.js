/**
 * evaluate.js — 全出品に判定エンジンを当てて ap_evaluations に残す (= シャドー運用)。
 *
 * ★ここで作るのは「提案」だけ。実行の口 (ai.actions 相当) はこのバージョンには無い。
 *
 * いつ走るか:
 *   ・画面を開いたとき、その日の価格スナップショットに対する成功 run がまだ無ければ自動で 1 回 (page_open)
 *   ・「いま判定を作り直す」ボタン (manual)。同じ日でも force で作り直せる (方針を変えた直後など)
 *   ・cron は使わない (定期実行を増やすと台帳登録と監視が要る。判定は入力が日次なので開いた時で足りる)
 *
 * 同じ日の run を 2 本作らないよう、判定→保存を **1 つの immediate トランザクション**の中でやる
 * (2 人が同時に開いても、後の方は前の run を見て skip する)。
 */
import { RULE_VERSION, evaluateListing, describeInputs } from './engine.js';
import { loadListings, mirrorTablesAvailable } from './read-model.js';
import { insertRun, finishRun, getRun, runForSnapshot, insertEvaluations } from './db.js';

/** 360 行 → エンジン入力 */
export function inputOf(row) {
  return {
    mode: row.mode || 'off',
    my_price: row.my_price, buybox_price: row.buybox_price, buybox_is_mine: row.buybox_is_mine,
    floor_price: row.floor_price, ceiling_price: row.ceiling_price, offset_jpy: row.offset_jpy ?? 0,
    min_margin_rate: row.min_margin_rate,
    channel: row.channel, cost_incl_tax: row.cost_incl_tax, cost_missing_parts: row.cost_missing_parts,
    referral_fee_rate: row.referral_fee_rate, fba_fee: row.fba_fee, per_item_fee: row.per_item_fee,
    variable_closing_fee: row.variable_closing_fee, ship_cost: row.ship_cost, units_30d: row.units_30d,
  };
}

function policyOf(row) {
  if (!row.policy_updated_at) return null;
  return {
    mode: row.mode, floor_price: row.floor_price, ceiling_price: row.ceiling_price,
    offset_jpy: row.offset_jpy, min_margin_rate: row.min_margin_rate, updated_at: row.policy_updated_at,
  };
}

/**
 * @param {object} db better-sqlite3 の Database (型注釈で import を書くと静的検査に引っかかるので object)
 * @param {{trigger:'manual'|'page_open'|'test', actorId:string, force?:boolean}} opts
 * @returns {{skipped:boolean, run:object, summary?:object}}
 */
export function runEvaluation(db, { trigger, actorId, force = false }) {
  const avail = mirrorTablesAvailable(db);
  if (!avail.ok) {
    const e = new Error(`判定に必要な表がまだありません: ${avail.missing.join(', ')}`);
    e.code = 'NO_MIRROR';
    throw e;
  }
  const tx = db.transaction(() => {
    const rows = loadListings(db);
    const snapshotDate = rows.find((r) => r.snapshot_date_jst)?.snapshot_date_jst ?? null;
    if (!force) {
      const existing = runForSnapshot(db, snapshotDate, RULE_VERSION);
      if (existing) return { skipped: true, run: existing };
    }
    const runId = insertRun(db, { trigger, actorId, snapshotDate, ruleVersion: RULE_VERSION });
    // ★判定の保存は内側の savepoint に分ける。失敗したらそこだけ巻き戻し、run 自体は failed として**残す**
    //   (外側で throw すると run の INSERT ごとロールバックされ、失敗した事実が消える — Codex R1 Medium)
    const inner = db.transaction(() => {
      const summary = { by_action: { raise: 0, lower: 0, keep: 0, hold: 0 }, by_reason: {}, flags: {}, no_policy: 0, snapshot_date_jst: snapshotDate };
      const evals = rows.map((row) => {
        const result = evaluateListing(inputOf(row));
        summary.by_action[result.action] += 1;
        summary.by_reason[result.reasonCode] = (summary.by_reason[result.reasonCode] || 0) + 1;
        for (const f of result.flags) summary.flags[f] = (summary.flags[f] || 0) + 1;
        if (!row.policy_updated_at) summary.no_policy += 1;
        return {
          seller_sku: row.seller_sku, asin: row.asin,
          action: result.action, proposedPrice: result.proposedPrice, currentPrice: result.currentPrice,
          reasonCode: result.reasonCode, reasonText: result.reasonText, confidence: result.confidence,
          flags: result.flags, inputs: describeInputs(row, policyOf(row), snapshotDate),
        };
      });
      insertEvaluations(db, runId, evals, { ruleVersion: RULE_VERSION, snapshotDate });
      return summary;
    });
    try {
      const summary = inner();
      finishRun(db, runId, { status: 'success', listingsTotal: rows.length, summary });
      return { skipped: false, run: getRun(db, runId), summary };
    } catch (e) {
      const message = String(e?.message || e);
      finishRun(db, runId, { status: 'failed', listingsTotal: rows.length, error: message });
      return { skipped: false, run: getRun(db, runId), summary: null, error: message };
    }
  });
  return tx.immediate();
}

/** 画面を開いたついでの自動生成の主体。人の GET に紐づけない (Codex R1 Medium: GET は外部から誘導できる) */
export const PAGE_OPEN_ACTOR = 'system:page_open';

/**
 * 画面を開いたついでの自動生成。失敗しても画面は出す (エラーは戻り値で知らせる)。
 * ★表示用の絞り込み・LIMIT とは無関係に全出品を対象にする (feedback_開発プロセス)。
 * ★actor は開いた人ではなく system:page_open。誰が開いても同じ結果になる冪等な処理で、人の判断ではないため
 */
export function ensureEvaluation(db) {
  try {
    const r = runEvaluation(db, { trigger: 'page_open', actorId: PAGE_OPEN_ACTOR });
    if (r.error) console.error('[amazon-pricing] 自動判定が失敗として記録されました:', r.error);
    return r;
  } catch (e) {
    console.error('[amazon-pricing] 自動判定に失敗:', e?.message || e);
    return { skipped: true, run: null, error: e?.message || String(e) };
  }
}
