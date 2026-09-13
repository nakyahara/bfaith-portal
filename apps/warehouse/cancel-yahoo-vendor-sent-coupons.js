#!/usr/bin/env node
/**
 * cancel-yahoo-vendor-sent-coupons.js — らくらくーぽん Yahoo 版の切り替えで 1 回だけ使う。
 * vendor (らくらくーぽん) がもう送ったクーポンメールを、うちから **二重に送らない** ようにする。
 *
 * なぜ要るか:
 *   フォローメールは cutover の境目 (最終発送日) で「vendor が送る / うちが送る」がきれいに分かれる。
 *   ところがクーポンメールは「レビュー投稿日の翌日正午」に送るので、境目より後に発送した注文でも、
 *   vendor が止まる前にレビューが付いた分は **vendor が既に送っている**。境目の指定では避けられない。
 *   (中原さん 2026-09-10 判断「取り消す」。AI_reference 要件設計「Y4 補足」)
 *
 * 決め方 = **注文の最初のレビューの投稿日**。
 *   うちの予定時刻 (うちがレビューを見つけた次の正午) では決めない。取込の遅れや予定の付け直しで
 *   vendor の送信日とずれ、取り消し漏れ・取り消しすぎの両方が起きる (Codex R1 High 1)。
 *   vendor の最終送信が 9/12 正午なら、vendor が受け持ったのは「9/11 までに投稿されたレビュー」→ --reviews-through 2026-09-11
 *
 * やること (--live のとき、1 トランザクションで):
 *   1. yahoo_campaign_meta に vendor_coupon_reviews_through = <日付> を記録する。
 *      送信ゲート (rakuten-review-sender-lib.js の gateReason) が、最初のレビューの投稿日がこの日付以前の注文の
 *      クーポンを 'vendor_already_sent' で止める。このあとで作られる行・担当があとで決まる注文・
 *      送る直前の再判定にも効く (Codex R1 High 2)
 *   2. いまある ready / planned のクーポン行のうち同じ条件に当たるものを status=cancelled / reason=vendor_already_sent にする。
 *      ゲートだけでも送られないが、残すと毎日「止めた」に数えられ続けるので片付ける (消さない)
 *
 * 🚨 安全のための約束:
 *   - **cutover --live の前** (まだ shadow = 送信 0 通) に流す。先にゲートを効かせておけば、
 *     cutover と取り消しの間に送信ジョブが動いても二重送信にならない (Codex R1 High 3)
 *   - 既定は **試し** (DB を読み取り専用で開いて数えるだけ)。書き換えは `--live --expect <試しで出た件数>` のときだけ。
 *     書き換えの瞬間にもう一度数え、`--expect` と 1 件でも違えば何もせずに止まる
 *   - 送り始めた (claimed) クーポン行が 1 件でもあれば止まる (送信ジョブが動いている最中)
 *   - meta に別の日付がもう入っていたら止まる (上書きしない。直すなら手で)
 *   - 1 行ずつ「まだ ready/planned のままなら」だけ書き換え、変わった行数が合わなければ全部巻き戻す
 *   - 画面にもログにも注文番号・宛先は出さない (件数と投稿日の内訳だけ)
 *
 * 🚨 --reviews-through は **vendor が実際に最後に送った日の前日**。予定日ではなく、vendor の配信履歴で確かめた日で決める
 *   (2026-09 の切り替えでは vendor が 9/12 正午まで送って解約済み → 2026-09-11 で確定。解約済みなのでもう動かない)。
 *   一度記録した日付は上書きしない。記録のあとで vendor が送り続けたなど、日付を変える必要が出たら、
 *   まだ shadow で送信 0 のうちに yahoo_campaign_meta の vendor_coupon_reviews_through を手で直してから流し直す (Codex R4 Medium)
 *
 * 使い方 (miniPC):
 *   cd /d C:\Users\bfaith\bfaith-portal
 *   set DATA_DIR=C:\Users\bfaith\bfaith-portal\data
 *   node apps/warehouse/cancel-yahoo-vendor-sent-coupons.js --reviews-through 2026-09-11
 *   node apps/warehouse/cancel-yahoo-vendor-sent-coupons.js --reviews-through 2026-09-11 --live --expect 10
 *
 * 終了コード: 0 = 試しの表示 or 完了 / 1 = 件数が合わない・止める条件に当たった / 2 = 引数の誤り
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { isValidJstDate, jstDateOf, tablesFor } from './rakuten-review-campaign-lib.js';
import {
  VENDOR_COUPON_THROUGH_KEY, vendorCouponCovered, firstReviewDateSql, reviewRevisionsAvailable,
} from './rakuten-review-sender-lib.js';

export const REASON = 'vendor_already_sent';
const T = tablesFor('yahoo');
const ACTIONS = T.actions;
const META = T.meta;
const MAX_AGE_DAYS = 14;

/** --reviews-through の検証。実在する暦日で、今日 (JST) 以前・14 日前以後。@returns エラー文 or null */
export function validateReviewsThrough(through, nowIso = new Date().toISOString()) {
  if (!isValidJstDate(through)) return `--reviews-through は実在する日付 (YYYY-MM-DD) で: ${through}`;
  const today = jstDateOf(nowIso);
  if (through > today) return `--reviews-through が未来の日付 (${through} > 今日 ${today})`;
  const oldest = jstDateOf(new Date(Date.parse(nowIso) - MAX_AGE_DAYS * 86400000).toISOString());
  if (through < oldest) return `--reviews-through が古すぎる (${through} < ${oldest})。打ち間違いでないか`;
  return null;
}

const metaValue = (db, key) => db.prepare(`SELECT value FROM ${META} WHERE key = ?`).get(key)?.value ?? null;

/** 今の段階 (表示用) */
export function currentStage(db) {
  return metaValue(db, 'cutover_at') ? 'live' : (metaValue(db, 'coupon_cutover_at') ? 'coupon_only' : 'shadow');
}

/**
 * 取り消す行を数える (読むだけ)。条件 = クーポン / ready か planned / 送信ゲートと同じ vendorCouponCovered。
 * 担当 (ownership) は見ない: vendor が送ったなら、担当がどちらでもうちは送らない。
 * 最初の投稿日は送信ゲートと同じ SQL (版の履歴も見る = 取り込み直しで日付が上書きされても拾う)
 * @returns {{ ids: number[], byPostedDate: Record<string, number>, total: number, claimed: number, currentThrough: string|null, withRevisions: boolean }}
 */
export function findVendorSentCoupons(db, { reviewsThrough }) {
  const withRevisions = reviewRevisionsAvailable(T, db);
  const rows = db.prepare(`
    SELECT a.id, ${firstReviewDateSql(T, 'a.order_number', { withRevisions })} AS first_review_posted_at
      FROM ${ACTIONS} a
     WHERE a.action_type = 'coupon' AND a.status IN ('ready','planned')
     ORDER BY a.id`).all();
  const ids = []; const byPostedDate = {};
  for (const r of rows) {
    if (!vendorCouponCovered({ ...r, vendor_coupon_reviews_through: reviewsThrough })) continue;
    ids.push(r.id);
    const d = String(r.first_review_posted_at).slice(0, 10);
    byPostedDate[d] = (byPostedDate[d] || 0) + 1;
  }
  const claimed = db.prepare(`SELECT COUNT(*) AS n FROM ${ACTIONS} WHERE action_type = 'coupon' AND status = 'claimed'`).get().n;
  return { ids, byPostedDate, total: ids.length, claimed, currentThrough: metaValue(db, VENDOR_COUPON_THROUGH_KEY), withRevisions };
}

/**
 * 日付を記録し、当たる行を取り消す (書き換える)。条件は呼び出し側に任せず、ここでも確かめる。
 * @throws code = 'BAD_EXPECT' | 'BAD_THROUGH' | 'THROUGH_MISMATCH' | 'SENDER_ACTIVE' | 'EXPECT_MISMATCH' | 'CHANGED_DURING_UPDATE'
 */
export function cancelVendorSentCoupons(db, { reviewsThrough, expect, nowIso = new Date().toISOString() }) {
  if (!Number.isInteger(expect) || expect < 0) throw Object.assign(new Error('--expect は 0 以上の整数'), { code: 'BAD_EXPECT' });
  const bad = validateReviewsThrough(reviewsThrough, nowIso);
  if (bad) throw Object.assign(new Error(bad), { code: 'BAD_THROUGH' });
  const tx = db.transaction(() => {
    const found = findVendorSentCoupons(db, { reviewsThrough });
    if (found.currentThrough && found.currentThrough !== reviewsThrough) {
      throw Object.assign(new Error(`別の日付がもう記録されている (${found.currentThrough})。上書きはしない。直すなら ${META} を手で`), { code: 'THROUGH_MISMATCH' });
    }
    if (found.claimed > 0) {
      throw Object.assign(new Error(`送り始めたクーポン行が ${found.claimed} 件ある (送信ジョブが動いている)。終わってから`), { code: 'SENDER_ACTIVE' });
    }
    if (found.total !== expect) {
      throw Object.assign(new Error(`件数が変わった (いま ${found.total} 件 / --expect ${expect} 件)。もう一度試しで数えてから`), { code: 'EXPECT_MISMATCH', found });
    }
    const upd = db.prepare(`
      UPDATE ${ACTIONS} SET status = 'cancelled', status_reason = ?, updated_at = ?
       WHERE id = ? AND action_type = 'coupon' AND status IN ('ready','planned')`);
    let changed = 0;
    for (const id of found.ids) changed += upd.run(REASON, nowIso, id).changes;
    if (changed !== found.total) {
      throw Object.assign(new Error(`書き換えの途中で行の状態が変わった (${changed} / ${found.total})。全部巻き戻した`), { code: 'CHANGED_DURING_UPDATE' });
    }
    db.prepare(`INSERT OR REPLACE INTO ${META} (key, value) VALUES (?, ?)`).run(VENDOR_COUPON_THROUGH_KEY, reviewsThrough);
    db.prepare(`INSERT OR IGNORE INTO ${META} (key, value) VALUES (?, ?)`).run(`${VENDOR_COUPON_THROUGH_KEY}_set_at`, nowIso);
    return { cancelled: changed, byPostedDate: found.byPostedDate, stage: currentStage(db) };
  });
  return tx.immediate();
}

// ─── CLI ───
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const usage = (m) => { console.error(`FATAL: ${m}`); process.exit(2); };
  const DATA_DIR = (process.env.DATA_DIR || '').trim();
  if (!DATA_DIR) usage('DATA_DIR が要る (set DATA_DIR=C:\\Users\\bfaith\\bfaith-portal\\data)');
  const dbPath = path.join(DATA_DIR, 'warehouse.db');
  if (!fs.existsSync(dbPath)) usage(`warehouse.db が無い: ${dbPath}`);
  const through = getArg('--reviews-through');
  if (!through) usage('--reviews-through <YYYY-MM-DD> が要る (vendor の最終送信が受け持ったレビューの投稿日 = 最終送信日の前日)');
  const bad = validateReviewsThrough(through);
  if (bad) usage(bad);
  const live = args.includes('--live');
  let expect = null;
  if (live) {
    const raw = getArg('--expect');
    if (raw == null || !/^\d+$/.test(raw)) usage('--live には --expect <試しで出た件数> が要る');
    expect = Number(raw);
  }

  const db = new Database(dbPath, { readonly: !live, fileMustExist: true });
  try {
    const stage = currentStage(db);
    console.log(`[cancel-vendor-sent-coupons] 段階 = ${stage} / vendor が受け持ったレビュー = ${through} 投稿分まで`);
    if (stage !== 'shadow') console.log('  ⚠️ もう切り替え済み。本来は cutover --live の前に流す (送信ジョブが動く前にゲートを効かせる)');
    if (!live) {
      const f = findVendorSentCoupons(db, { reviewsThrough: through });
      console.log(`[試し] 取り消す行 ${f.total} 件 (レビュー投稿日ごと ${JSON.stringify(f.byPostedDate)})`
        + ` / 送り始めた行 ${f.claimed} 件 / 記録済みの日付 ${f.currentThrough ?? 'なし'}`
        + ` / 版の履歴 ${f.withRevisions ? 'あり' : '🚨なし (今の投稿日だけで判定)'}`);
      console.log(`  書き換えるには同じ引数に --live --expect ${f.total}`);
    } else {
      const r = cancelVendorSentCoupons(db, { reviewsThrough: through, expect });
      console.log(`[live] ✅ 取り消した ${r.cancelled} 件 (status=cancelled / reason=${REASON}) レビュー投稿日ごと ${JSON.stringify(r.byPostedDate)}`);
      console.log(`  送信ゲートに「${through} までに投稿されたレビューのクーポンは送らない」を記録した`);
    }
  } catch (e) {
    console.error(`[cancel-vendor-sent-coupons] 止めた: ${e.message}`);
    process.exitCode = 1;
  } finally {
    db.close();
  }
}
