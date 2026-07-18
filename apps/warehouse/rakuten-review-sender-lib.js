/**
 * rakuten-review-sender-lib.js — フォロー/クーポンメール送信エンジン (mall-csv-fetcher P2 PR-C4)
 *
 * planner (PR-C1) が積んだ rakuten_campaign_actions の ready を at-most-once で送信する。
 *
 * at-most-once 契約 (campaign lib DDL コメントの明文化どおり、Codex C1-R1/R3):
 *   ① claim = 条件付き UPDATE (ready→claimed) + 変更行数=1 確認 (CAS)
 *   ② 同一トランザクションで delivery_attempts に outcome='ambiguous' を予約 commit
 *      (UNIQUE(action_id) が構造的に2通目を拒否。SMTP 呼び出しは commit の後)
 *   ③ SMTP 結果: accepted → sent / 明確な reject (SMTP 4xx/5xx応答) → failed_safe /
 *      timeout・切断・不明 → action=ambiguous のまま停止 (自動再送禁止、GChat で人に確認依頼)
 *
 * 送信ゲート (claim 前に全て再評価。1つでも欠けたら claim しない):
 *   ①expires_at > now ②contact 未purge・復号可能 ③suppression 未登録
 *   ④follow=レビュー不存在 (削除済み含む) / coupon=有効レビュー残存
 *   ⑤発送済み+follow は scheduled_at が現在の発送日から再計算した値と一致
 *   ⑥ownership owner='self' (行なし・vendor は送らない = fail-closed。shadow 中は全注文 vendor
 *     のため本エンジンが本番に居ても送信数は構造的に 0)
 *   ⑦coupon は当月の月次クーポン (rakuten_campaign_coupons status='issued'・期限内) が存在
 *
 * PII: 宛先はログ・GChat・戻り値に出さない (復号は送信直前のみ)。SMTPレスポンス全文も保存しない
 * (delivery_attempts には code と先頭120字の note のみ)。
 */
import crypto from 'node:crypto';
import { decryptEmail } from './rakuten-review-contacts-lib.js';
import { followScheduleFor, jstDateOf } from './rakuten-review-campaign-lib.js';
import { getRegisteredCoupon } from './rakuten-coupon-lib.js';
import {
  buildFollowMail, buildCouponMail, buildCouponMailLowRating, messageIdFor, SHOP_NAME, FROM_ADDRESS,
} from './rakuten-review-mail-lib.js';

export const ANSHIN_SMTP = { host: 'sub.fw.rakuten.ne.jp', port: 587 };

/** nodemailer transport (あんしんメルアドリレー、STARTTLS 必須)。env 鍵は値を持ち回らない */
export async function createAnshinTransport(env = process.env) {
  const user = (env.RAKUTEN_ANSHIN_SMTP_USER || '').trim();
  const pass = (env.RAKUTEN_ANSHIN_SMTP_PASS || '').trim();
  if (!user || !pass) {
    throw new Error('ANSHIN_SMTP_KEY_MISSING: RAKUTEN_ANSHIN_SMTP_USER / RAKUTEN_ANSHIN_SMTP_PASS が未設定 (miniPC リポジトリ直下 .env)');
  }
  const { default: nodemailer } = await import('nodemailer');
  return nodemailer.createTransport({
    host: ANSHIN_SMTP.host,
    port: ANSHIN_SMTP.port,
    secure: false,
    requireTLS: true,
    auth: { user, pass },
    connectionTimeout: 20000,
    greetingTimeout: 20000,
    socketTimeout: 30000,
  });
}

/** SMTP エラーの分類。responseCode 付き (サーバが明確に拒否) = rejected、それ以外 = unknown */
export function classifySendError(e) {
  const code = e?.responseCode;
  if (Number.isInteger(code) && code >= 400 && code < 600) return { kind: 'rejected', code };
  return { kind: 'unknown', code: null };
}

/**
 * 送信候補の選定 (claim はしない)。ready かつ送信ゲート①〜⑦を満たす action を返す。
 * @returns { eligible: [...], skipped: [{id, action_type, reason}] }
 */
export function selectEligibleActions(db, { nowIso = new Date().toISOString(), limit = 5 } = {}) {
  const month = jstDateOf(nowIso).slice(0, 7);
  const monthlyCoupon = getRegisteredCoupon(db, month);
  const couponUsable = !!(monthlyCoupon && monthlyCoupon.status === 'issued'
    && monthlyCoupon.pc_get_url && Date.parse(monthlyCoupon.coupon_end) > Date.parse(nowIso));

  const rows = db.prepare(`
    SELECT a.id, a.action_type, a.dedupe_key, a.order_number, a.scheduled_at, a.expires_at,
           c.masked_email_enc, c.masked_email_hash, c.purged_at, c.shipping_datetime,
           o.owner,
           EXISTS(SELECT 1 FROM rakuten_contact_suppressions s WHERE s.email_hash = c.masked_email_hash) AS is_suppressed,
           EXISTS(SELECT 1 FROM fact_rakuten_reviews r WHERE r.order_number = a.order_number) AS has_review_any,
           EXISTS(SELECT 1 FROM fact_rakuten_reviews r WHERE r.order_number = a.order_number AND r.is_deleted = 0) AS has_active_review,
           EXISTS(SELECT 1 FROM fact_rakuten_reviews r WHERE r.order_number = a.order_number AND r.is_deleted = 0 AND r.rating <= 2) AS has_low_active_review
      FROM rakuten_campaign_actions a
      LEFT JOIN rakuten_order_contacts c ON c.order_number = a.order_number
      LEFT JOIN rakuten_order_campaign_ownership o ON o.order_number = a.order_number
     WHERE a.status = 'ready' AND a.scheduled_at <= ? AND a.expires_at > ?
     ORDER BY a.scheduled_at, a.id
  `).all(nowIso, nowIso);

  const eligible = [], skipped = [];
  for (const a of rows) {
    if (eligible.length >= limit) break;
    let reason = null;
    if (a.owner !== 'self') reason = 'not_self_ownership'; // 行なし (null) も vendor も送らない
    else if (a.purged_at || !a.masked_email_enc) reason = 'contact_unavailable';
    else if (a.is_suppressed) reason = 'suppressed';
    else if (a.action_type === 'follow' && a.has_review_any) reason = 'review_exists';
    else if (!a.shipping_datetime) reason = 'not_shipped';
    else if (a.action_type === 'follow'
      && followScheduleFor(a.shipping_datetime)?.scheduledAt !== a.scheduled_at) reason = 'schedule_stale';
    else if (a.action_type === 'coupon' && !a.has_active_review) reason = 'review_deleted';
    else if (a.action_type === 'coupon' && !couponUsable) reason = 'no_monthly_coupon';
    if (reason) { skipped.push({ id: a.id, action_type: a.action_type, reason }); continue; }
    eligible.push({ ...a, monthlyCoupon: a.action_type === 'coupon' ? monthlyCoupon : null });
  }
  return { eligible, skipped, monthlyCouponReady: couponUsable };
}

/** claim (CAS) + attempt 予約を単一トランザクションで。@returns messageId | null (=claim負け) */
export function claimAction(db, action, nowIso = new Date().toISOString()) {
  const messageId = messageIdFor(action.id, action.dedupe_key);
  const claimToken = crypto.randomBytes(8).toString('hex');
  const tx = db.transaction(() => {
    const r = db.prepare(`
      UPDATE rakuten_campaign_actions
         SET status = 'claimed', claim_token = ?, claimed_at = ?, updated_at = ?
       WHERE id = ? AND status = 'ready'
    `).run(claimToken, nowIso, nowIso, action.id);
    if (r.changes !== 1) throw Object.assign(new Error('claim race lost'), { claimLost: true });
    db.prepare(`
      INSERT INTO rakuten_campaign_delivery_attempts (action_id, message_id, attempted_at, outcome)
      VALUES (?, ?, ?, 'ambiguous')
    `).run(action.id, messageId, nowIso);
  });
  try {
    tx.immediate();
    return messageId;
  } catch (e) {
    if (e.claimLost || /UNIQUE/i.test(String(e.message))) return null; // 既にattempt済み=絶対に送らない
    throw e;
  }
}

/** 送信結果の確定 (claimed → sent / failed_safe) */
export function finalizeAttempt(db, { actionId, outcome, smtpCode = null, note = null, nowIso = new Date().toISOString() }) {
  const status = outcome === 'accepted' ? 'sent' : 'failed_safe';
  const tx = db.transaction(() => {
    db.prepare(`
      UPDATE rakuten_campaign_delivery_attempts SET outcome = ?, smtp_code = ?, note = ?
       WHERE action_id = ?
    `).run(outcome, smtpCode != null ? String(smtpCode) : null, note ? String(note).slice(0, 120) : null, actionId);
    db.prepare(`
      UPDATE rakuten_campaign_actions SET status = ?, status_reason = NULL, updated_at = ?
       WHERE id = ? AND status = 'claimed'
    `).run(status, nowIso, actionId);
  });
  tx.immediate();
}

/** 結果不明の確定 (claimed → ambiguous、attempt は ambiguous のまま note のみ) */
export function markAmbiguous(db, { actionId, note = null, nowIso = new Date().toISOString() }) {
  const tx = db.transaction(() => {
    db.prepare(`
      UPDATE rakuten_campaign_delivery_attempts SET note = ? WHERE action_id = ?
    `).run(note ? String(note).slice(0, 120) : null, actionId);
    db.prepare(`
      UPDATE rakuten_campaign_actions SET status = 'ambiguous', status_reason = 'smtp_unknown', updated_at = ?
       WHERE id = ? AND status = 'claimed'
    `).run(nowIso, actionId);
  });
  tx.immediate();
}

/** action → メール本文 (宛先はここでは扱わない) */
export function buildMailForAction(action, nowIso = new Date().toISOString()) {
  if (action.action_type === 'follow') {
    return buildFollowMail({ orderNumber: action.order_number, shippingIso: action.shipping_datetime });
  }
  const ctx = { couponUrl: action.monthlyCoupon.pc_get_url, couponEndIso: action.monthlyCoupon.coupon_end };
  return action.has_low_active_review ? buildCouponMailLowRating(ctx) : buildCouponMail(ctx);
}

/**
 * ready の一括処理。sendFn は注入可能 (テスト用)。
 * sendFn({ to, from, subject, text, messageId }) → resolve = 受理 / reject = classifySendError で分類。
 * 結果不明が出たら即時中断 (人の確認が先 — カスケードさせない)。
 * @returns { sent, failedSafe, ambiguous, skipped, claimLost, details } (宛先・注文番号は含まない)
 */
export async function processReadyActions(db, { keys, sendFn, nowIso = new Date().toISOString(), limit = 5 }) {
  const { eligible, skipped } = selectEligibleActions(db, { nowIso, limit });
  const out = { sent: 0, failedSafe: 0, ambiguous: 0, skipped: skipped.length, claimLost: 0, details: [] };
  for (const action of eligible) {
    // 復号は claim 前 (復号できないものを claim して枯らさない)。宛先は変数スコープ外に出さない
    let to;
    try {
      to = decryptEmail(action.masked_email_enc, keys, action.order_number);
    } catch {
      out.skipped++;
      out.details.push({ id: action.id, result: 'skip', reason: 'contact_undecryptable' });
      continue;
    }
    const mail = buildMailForAction(action, nowIso);
    const messageId = claimAction(db, action, nowIso);
    if (!messageId) { out.claimLost++; out.details.push({ id: action.id, result: 'claim_lost' }); continue; }
    try {
      await sendFn({ to, from: `"${SHOP_NAME}" <${FROM_ADDRESS}>`, subject: mail.subject, text: mail.text, messageId });
      finalizeAttempt(db, { actionId: action.id, outcome: 'accepted', smtpCode: 250, nowIso });
      out.sent++;
      out.details.push({ id: action.id, result: 'sent', type: action.action_type });
    } catch (e) {
      const cls = classifySendError(e);
      if (cls.kind === 'rejected') {
        finalizeAttempt(db, { actionId: action.id, outcome: 'rejected', smtpCode: cls.code, note: String(e.message).slice(0, 120), nowIso });
        out.failedSafe++;
        out.details.push({ id: action.id, result: 'failed_safe', code: cls.code });
      } else {
        markAmbiguous(db, { actionId: action.id, note: String(e.message).slice(0, 120), nowIso });
        out.ambiguous++;
        out.details.push({ id: action.id, result: 'ambiguous' });
        break; // 結果不明は即中断 — 続行しない (設計: 自動再送禁止・人に確認)
      }
    }
  }
  return out;
}
