/**
 * 再設計 R8: 出品前チェック sweep。
 *
 * 背景 (中原さん指摘 2026-07-03):
 *   「出せる」タブは Notion 項目+カテゴリの静的判定のみで、 出品時に初めて走る
 *   実データ検査 (楽天RMS実値・税率整合・画像・バリエーション・lead time) で弾かれる
 *   商品が「出せる」に混ざる。 一括登録して初めて失敗が分かるのは体験が悪い。
 *
 * 解決:
 *   出品対象になりうる候補全件に evaluateItemForPublish(dryRun=false) を事前実行する。
 *   dryRun=false は readiness を jobs に persist する (publish はしない — 実 publish は
 *   publish-executor の仕事)。 listProductsForUI は jobs.readiness_blocked_reasons を
 *   一覧判定に合流させるので、 sweep 後は問題商品が理由付きで「修正必要」タブに移る。
 *
 * 設計:
 *   - 対象: status='candidate' かつ 除外されておらず Yahoo 未登録の候補 (limit で上限)
 *   - manageNumber は candidates 値 → 楽天 all-codes mapping の順で解決 (bulk-publish と同じ)
 *   - 1 件ずつ順次 + 小 sleep (楽天 RMS は miniPC proxy の rate 制御下だが、 念のため上ぶれ防止)
 *   - 個別エラーは飲んで続行 (該当商品は前回 readiness のまま = 出品時に弾かれる従来動作)。
 *     ただし全滅・過半数エラーは infrastructure 障害とみなし throw (pipeline を fail-closed)
 */

import { evaluateItemForPublish } from './publish-pipeline.js';
import { fetchAllItemCodes } from '../lib/rakuten-rms-proxy.js';

const DEFAULT_LIMIT = 400;
const SLEEP_MS = 150;

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

/**
 * sweep 対象の候補 (「出せる/修正必要」に出うる = done/excluded 以外の candidate)。
 * 2026-10-22: 並びを「まだ一度も検査していない → 検査が古い順」に変更。旧実装は item_code 順の先頭 400 件だけを
 *   毎回検査していたので、候補が 400 件を超えると 401 件目以降は一度も実データ検査されなかった
 *   (R8 の「出せるタブなのに出せない」がそのまま残る)。今は毎日 400 件ずつ回って全件を一巡する。
 */
export function pickSweepTargets(db, { limit = DEFAULT_LIMIT } = {}) {
  return db.prepare(`
    SELECT c.item_code, c.rakuten_manage_number
    FROM migration_candidates c
    LEFT JOIN migration_excluded e
      ON e.item_code = c.item_code AND e.restored_at IS NULL
    LEFT JOIN yahoo_registered_items r
      ON r.item_code = c.item_code
    LEFT JOIN jobs j
      ON j.item_code = c.item_code
    WHERE c.status = 'candidate'
      AND e.item_code IS NULL
      AND r.item_code IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM publish_idempotency pi
        WHERE pi.item_code = c.item_code AND pi.status = 'success'
      )
    ORDER BY (j.last_readiness_at IS NOT NULL), j.last_readiness_at, c.item_code
    LIMIT ?
  `).all(limit);
}

/**
 * 商品の状態ではなく基盤の障害 (miniPC / 楽天 RMS / DB) で止まった理由か。
 * evaluateItemForPublish はこれらを例外にせず blocked で返すので、数えないと「全件 取得失敗」の朝でも
 * sweep が成功扱いになっていた (監視には ok が届く)。
 */
const INFRA_REASON_HEADS = new Set(['rakuten_fetch_failed', 'image_patterns_load_failed', 'persist_failed']);
export function isInfraBlocked(result) {
  const reasons = Array.isArray(result?.reasons) ? result.reasons : [];
  return reasons.length > 0 && reasons.every((r) => INFRA_REASON_HEADS.has(String(r).split(':')[0]));
}

/**
 * @returns {{ picked, evaluated, okCount, blockedCount, errors, errorSamples: string[] }}
 * @throws 全滅 or 過半数エラー時 (miniPC/RMS 側障害の可能性 → pipeline fail-closed)
 */
export async function sweepReadiness({ db, limit = DEFAULT_LIMIT, deps = {} } = {}) {
  if (!db) throw new Error('sweepReadiness: db required');
  const _evaluate = deps.evaluateItemForPublish || evaluateItemForPublish;
  const _fetchAllItemCodes = deps.fetchAllItemCodes || fetchAllItemCodes;
  const _sleepMs = deps.sleepMs ?? SLEEP_MS;

  const targets = pickSweepTargets(db, { limit });
  const summary = { picked: targets.length, evaluated: 0, okCount: 0, blockedCount: 0, infraBlocked: 0, errors: 0, errorSamples: [] };
  if (targets.length === 0) return summary;

  let mapping = null; // itemNumber → manageNumber (必要になったら 1 回だけ取得)
  for (const t of targets) {
    let manageNumber = t.rakuten_manage_number || null;
    try {
      if (!manageNumber) {
        if (mapping === null) mapping = await _fetchAllItemCodes();
        manageNumber = mapping?.[t.item_code] || t.item_code;
      }
      // dryRun=false: readiness を jobs に persist (publish はしない)
      const r = await _evaluate({ db, itemCode: t.item_code, manageNumber, dryRun: false });
      summary.evaluated++;
      if (r.status === 'ok') summary.okCount++;
      else summary.blockedCount++;
      if (r.status !== 'ok' && isInfraBlocked(r)) {
        summary.infraBlocked++;
        if (summary.errorSamples.length < 5) summary.errorSamples.push(`${t.item_code}: ${String(r.reasons[0]).slice(0, 120)}`);
      }
    } catch (e) {
      summary.errors++;
      if (summary.errorSamples.length < 5) {
        summary.errorSamples.push(`${t.item_code}: ${String(e.message || e).slice(0, 120)}`);
      }
    }
    if (_sleepMs > 0) await sleep(_sleepMs);
  }

  // fail-closed 判定: 1 件も評価できない / 過半数エラー = infrastructure 障害
  if (summary.evaluated === 0) {
    throw new Error(`readiness sweep: 1件も評価できませんでした (picked=${summary.picked}, errors=${summary.errors}${summary.errorSamples[0] ? `, 例: ${summary.errorSamples[0]}` : ''})`);
  }
  if (summary.errors > summary.evaluated) {
    throw new Error(`readiness sweep: エラー多数 (evaluated=${summary.evaluated}, errors=${summary.errors}) — miniPC/楽天RMS 側障害の可能性${summary.errorSamples[0] ? ` (例: ${summary.errorSamples[0]})` : ''}`);
  }
  // 楽天から読めなかった (基盤の障害) が過半数 = その朝の結果は信用できない → 失敗にして監視に出す。
  //   書いた「取得失敗」の理由は次に成功した sweep で上書きされる
  const infraTotal = summary.errors + summary.infraBlocked;
  if (infraTotal * 2 > summary.picked) {
    throw new Error(`readiness sweep: 楽天から読めなかった商品が過半数 (${infraTotal}/${summary.picked}) — miniPC/楽天RMS 側障害の可能性${summary.errorSamples[0] ? ` (例: ${summary.errorSamples[0]})` : ''}`);
  }
  return summary;
}
