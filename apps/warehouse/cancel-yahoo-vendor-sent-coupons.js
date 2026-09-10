#!/usr/bin/env node
/**
 * cancel-yahoo-vendor-sent-coupons.js — らくらくーぽん Yahoo 版の切り替え直後に 1 回だけ使う。
 * vendor (らくらくーぽん) が送ったはずのクーポンメールを、うちから **二重に送らない** よう取り消す。
 *
 * なぜ要るか:
 *   フォローメールは cutover の境目 (最終発送日) で「vendor が送る / うちが送る」がきれいに分かれる。
 *   ところがクーポンメールは「レビュー投稿の 1 日後」に送るので、境目より後に発送した注文でも、
 *   vendor が止まる前にレビューが付いた分は **vendor が既に送っている**。
 *   coupon の境目は follow の境目より後ろにできない (coupon_cutover_at ≤ cutover_at) ので、
 *   境目の指定では避けられない。切り替えの直後・最初の送信の前に、その分だけ取り消す。
 *   (中原さん 2026-09-10 判断「取り消す」。AI_reference 要件設計「Y4 補足」手順 3-c)
 *
 * 取り消す行の条件 (全部満たすもの):
 *   - action_type = 'coupon' で、status が 'ready' か 'planned' (まだ誰も送っていない・送り始めていない)
 *   - 注文のクーポン担当がうち = ownership 表の coupon_owner (無ければ owner) が 'self'。
 *     送信側のゲート (gateReason) と同じ表で決める。cutover --live の前は担当が全部 vendor なので、
 *     試しに限り「最終発送が境目 (--boundary) より後」で見込みを数える (cutoverPreview と同じ決め方)
 *   - 予定時刻 (scheduled_at) が vendor の最終送信 (--vendor-last-send) 以前 = vendor が送ったはず。
 *     うちのクーポン予定は「うちがレビューを見つけた次の正午」、vendor は「投稿の翌日正午 (取込は深夜)」で同じ日になる。
 *     cutover は ready の予定時刻を付け直さないので、過去の予定のまま残った分がそのまま当たる
 *   - 期限 (expires_at) がまだ切れていない (切れていれば放っておいても送られない)
 *   → status = 'cancelled' / status_reason = 'vendor_already_sent' にする (消さない。あとで数えられる)
 *   取り消した行は planCampaigns が作り直さない (dedupe_key が一意で INSERT OR IGNORE・更新は planned/ready だけ)
 *
 * 🚨 安全のための約束:
 *   - 既定は **試し** (数えるだけ)。書き換えは `--live --expect <試しで出た件数>` のときだけ。
 *     書き換えの瞬間にもう一度数え、`--expect` と 1 件でも違えば何もせずに止まる
 *   - `--live` は **cutover 済み (live) で、その境目が --boundary と同じ** ときだけ。
 *     shadow のうちに取り消すと、cutover の再計算との関係が読めなくなるので受け付けない
 *   - 1 トランザクションで、1 行ずつ「まだ ready/planned のままなら」だけ書き換える。
 *     変わった行数が合わなければ全部巻き戻す
 *   - 画面にもログにも注文番号・宛先は出さない (件数と予定日の内訳だけ)
 *
 * 使い方 (miniPC):
 *   cd /d C:\Users\bfaith\bfaith-portal
 *   set DATA_DIR=C:\Users\bfaith\bfaith-portal\data
 *   node apps/warehouse/cancel-yahoo-vendor-sent-coupons.js --boundary 2026-09-02T23:59:59+09:00 --vendor-last-send 2026-09-12T12:45:00+09:00
 *   node apps/warehouse/cancel-yahoo-vendor-sent-coupons.js --boundary ... --vendor-last-send ... --live --expect 10
 *
 * 終了コード: 0 = 試しの表示 or 取り消し完了 / 1 = 件数が合わない・live の条件を満たさない / 2 = 引数の誤り
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { parseCutoverArg } from './rakuten-review-campaign-lib.js';

export const REASON = 'vendor_already_sent';
const ACTIONS = 'yahoo_campaign_actions';
const CONTACTS = 'yahoo_order_contacts';
const OWNERSHIP = 'yahoo_order_campaign_ownership';
const META = 'yahoo_campaign_meta';

const jstDate = (iso) => new Date(Date.parse(iso) + 9 * 3600000).toISOString().slice(0, 10);

/** 今の段階と、cutover に使われた境目 (UTC ISO)。live でなければ cutoverAt は null */
export function currentCutover(db) {
  const get = (k) => db.prepare(`SELECT value FROM ${META} WHERE key = ?`).get(k)?.value || null;
  const cutoverAt = get('cutover_at');
  const couponCutoverAt = get('coupon_cutover_at') || cutoverAt;
  return { stage: cutoverAt ? 'live' : (couponCutoverAt ? 'coupon_only' : 'shadow'), cutoverAt, couponCutoverAt };
}

/**
 * 取り消す行を数える (読むだけ)。
 * 「うちが送る側の注文か」は、切り替え後 (live) は送信側と同じ ownership 表 (coupon_owner、無ければ owner)。
 * 切り替え前は担当がまだ全部 vendor なので、cutoverPreview と同じく「最終発送が境目より後」で見込みを数える
 * @returns {{ ids: number[], byDate: Record<string, number>, total: number, basis: 'ownership' | 'shipping_preview' }}
 */
export function findVendorSentCoupons(db, { boundaryIso, vendorLastSendIso, nowIso = new Date().toISOString() }) {
  const V = Date.parse(vendorLastSendIso); const N = Date.parse(nowIso);
  const basis = currentCutover(db).stage === 'live' ? 'ownership' : 'shipping_preview';
  const selfOrders = new Set();
  if (basis === 'ownership') {
    for (const o of db.prepare(`SELECT order_number FROM ${OWNERSHIP} WHERE COALESCE(coupon_owner, owner) = 'self'`).all()) {
      selfOrders.add(o.order_number);
    }
  } else {
    const B = Date.parse(boundaryIso);
    for (const c of db.prepare(`SELECT order_number, shipping_datetime FROM ${CONTACTS} WHERE shipping_datetime IS NOT NULL`).all()) {
      const t = Date.parse(c.shipping_datetime);
      if (Number.isFinite(t) && t > B) selfOrders.add(c.order_number);
    }
  }
  const rows = db.prepare(`
    SELECT id, order_number, scheduled_at, expires_at FROM ${ACTIONS}
     WHERE action_type = 'coupon' AND status IN ('ready','planned')
     ORDER BY id`).all();
  const ids = []; const byDate = {};
  for (const r of rows) {
    if (!selfOrders.has(r.order_number)) continue;
    const s = Date.parse(r.scheduled_at); const e = Date.parse(r.expires_at);
    if (!Number.isFinite(s) || !Number.isFinite(e)) continue;
    if (s > V) continue;          // vendor が止まったあとの予定 = うちが送るのが正しい
    if (e <= N) continue;         // もう期限切れ = 放っておいても送られない
    ids.push(r.id);
    const d = jstDate(r.scheduled_at); byDate[d] = (byDate[d] || 0) + 1;
  }
  return { ids, byDate, total: ids.length, basis };
}

/**
 * 取り消す (書き換える)。**呼ぶ前の条件は呼び出し側で確かめてある前提ではなく、ここでも確かめる**。
 * @throws code = 'NOT_LIVE' | 'BOUNDARY_MISMATCH' | 'EXPECT_MISMATCH' | 'CHANGED_DURING_UPDATE'
 */
export function cancelVendorSentCoupons(db, { boundaryIso, vendorLastSendIso, expect, nowIso = new Date().toISOString() }) {
  if (!Number.isInteger(expect) || expect < 0) throw Object.assign(new Error('--expect は 0 以上の整数'), { code: 'BAD_EXPECT' });
  const tx = db.transaction(() => {
    const cur = currentCutover(db);
    if (cur.stage !== 'live') {
      throw Object.assign(new Error(`まだ切り替えていない (段階 = ${cur.stage})。cutover --live のあとに実行する`), { code: 'NOT_LIVE' });
    }
    if (cur.couponCutoverAt !== boundaryIso) {
      throw Object.assign(new Error(`境目が cutover と違う (cutover の coupon 境目 = ${cur.couponCutoverAt} / 指定 = ${boundaryIso})`), { code: 'BOUNDARY_MISMATCH' });
    }
    const found = findVendorSentCoupons(db, { boundaryIso, vendorLastSendIso, nowIso });
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
    return { cancelled: changed, byDate: found.byDate };
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
  if (!getArg('--boundary') || !getArg('--vendor-last-send')) usage('--boundary <ISO+09:00> と --vendor-last-send <ISO+09:00> が要る');
  let boundaryIso; let vendorLastSendIso;
  try {
    boundaryIso = parseCutoverArg(getArg('--boundary'));
    vendorLastSendIso = parseCutoverArg(getArg('--vendor-last-send'));
  } catch (e) { usage(e.message); }
  if (Date.parse(vendorLastSendIso) <= Date.parse(boundaryIso)) usage('--vendor-last-send は --boundary より後の時刻');
  const live = args.includes('--live');

  const db = new Database(dbPath, { readonly: !live, fileMustExist: true });
  try {
    const cur = currentCutover(db);
    console.log(`[cancel-vendor-sent-coupons] 段階 = ${cur.stage} / 境目 = ${boundaryIso} / vendor 最終送信 = ${vendorLastSendIso}`);
    if (!live) {
      const f = findVendorSentCoupons(db, { boundaryIso, vendorLastSendIso });
      const how = f.basis === 'ownership' ? '担当表で数えた' : '見込み: 切り替え前なので発送日時で数えた';
      console.log(`[試し] 取り消す対象 ${f.total} 件 (${how}) 予定日ごと ${JSON.stringify(f.byDate)}`);
      console.log(f.basis === 'ownership'
        ? `  書き換えるには同じ引数に --live --expect ${f.total}`
        : '  🚨 cutover --live のあとにもう一度試しで数え、その件数を --expect に渡す');
    } else {
      const expectRaw = getArg('--expect');
      if (expectRaw == null || !/^\d+$/.test(expectRaw)) usage('--live には --expect <試しで出た件数> が要る');
      const r = cancelVendorSentCoupons(db, { boundaryIso, vendorLastSendIso, expect: Number(expectRaw) });
      console.log(`[live] ✅ 取り消した ${r.cancelled} 件 (status=cancelled / reason=${REASON}) 予定日ごと ${JSON.stringify(r.byDate)}`);
    }
  } catch (e) {
    console.error(`[cancel-vendor-sent-coupons] 止めた: ${e.message}`);
    process.exitCode = 1;
  } finally {
    db.close();
  }
}
