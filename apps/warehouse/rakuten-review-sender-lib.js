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
import { followScheduleFor, jstDateOf, tablesFor } from './rakuten-review-campaign-lib.js';
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

/** 月次クーポンの利用可否 (獲得URLは楽天ドメインの https のみ許可 — Codex C4-R1 Medium:
 *  台帳の誤登録で楽天外リンクがメールに載る事故を claim 前ゲートで遮断) */
export function couponUsableCheck(monthlyCoupon, nowIso) {
  if (!monthlyCoupon || monthlyCoupon.status !== 'issued' || !monthlyCoupon.pc_get_url) return false;
  if (!(Date.parse(monthlyCoupon.coupon_end) > Date.parse(nowIso))) return false;
  // PR-C5: 開始前のクーポンURLは配らない (当月途中に発行すると開始=発行+90分。その日の正午バッチが
  // 開始前のURLを送ると顧客が獲得できない → 翌日の正午まで待つ)
  if (!(Date.parse(monthlyCoupon.coupon_start) <= Date.parse(nowIso))) return false;
  try {
    const u = new URL(monthlyCoupon.pc_get_url);
    return u.protocol === 'https:' && u.hostname === 'coupon.rakuten.co.jp';
  } catch {
    return false;
  }
}

const gateSql = (T) => `
    SELECT a.id, a.action_type, a.dedupe_key, a.order_number, a.scheduled_at, a.expires_at, a.status,
           c.masked_email_enc, c.masked_email_hash, c.purged_at, c.shipping_datetime,
           o.owner, o.coupon_owner,
           EXISTS(SELECT 1 FROM ${T.suppressions} s WHERE s.email_hash = c.masked_email_hash) AS is_suppressed,
           EXISTS(SELECT 1 FROM ${T.reviews} r WHERE r.order_number = a.order_number) AS has_review_any,
           EXISTS(SELECT 1 FROM ${T.reviews} r WHERE r.order_number = a.order_number AND r.is_deleted = 0) AS has_active_review,
           EXISTS(SELECT 1 FROM ${T.reviews} r WHERE r.order_number = a.order_number AND r.is_deleted = 0 AND r.rating <= 2) AS has_low_active_review
      FROM ${T.actions} a
      LEFT JOIN ${T.contacts} c ON c.order_number = a.order_number
      LEFT JOIN ${T.ownership} o ON o.order_number = a.order_number`;

/** ゲート判定 (select 時と claim 時で同一ロジックを共有)。@returns null=通過 / 理由文字列 */
export function gateReason(a, { nowIso, couponUsable }) {
  if (a.status !== 'ready') return 'not_ready';
  if (!(a.scheduled_at <= nowIso && a.expires_at > nowIso)) return 'out_of_window';
  // 行なし (null) も vendor も送らない (fail-closed)。クーポンは coupon_owner (PR-C5、NULL なら owner と同じ)
  const owner = a.action_type === 'coupon' ? (a.coupon_owner ?? a.owner) : a.owner;
  if (owner !== 'self') return 'not_self_ownership';
  if (a.purged_at || !a.masked_email_enc) return 'contact_unavailable';
  if (a.is_suppressed) return 'suppressed';
  if (a.action_type === 'follow' && a.has_review_any) return 'review_exists';
  if (!a.shipping_datetime) return 'not_shipped';
  if (a.action_type === 'follow'
    && followScheduleFor(a.shipping_datetime)?.scheduledAt !== a.scheduled_at) return 'schedule_stale';
  if (a.action_type === 'coupon' && !a.has_active_review) return 'review_deleted';
  if (a.action_type === 'coupon' && !couponUsable) return 'no_monthly_coupon';
  return null;
}

/**
 * 送信候補の選定 (claim はしない)。ready かつ送信ゲート①〜⑦を満たす action を返す。
 * @returns { eligible: [...], skipped: [{id, action_type, reason}] }
 */
function selectEligibleActionsT(T, A, db, { nowIso = new Date().toISOString(), limit = 5 } = {}) {
  const month = jstDateOf(nowIso).slice(0, 7);
  const monthlyCoupon = A.monthlyCouponFor(db, month);
  const couponUsable = couponUsableCheck(monthlyCoupon, nowIso);

  const rows = db.prepare(`${gateSql(T)}
     WHERE a.status = 'ready' AND a.scheduled_at <= ? AND a.expires_at > ?
     ORDER BY a.scheduled_at, a.id
  `).all(nowIso, nowIso);

  const eligible = [], skipped = [];
  let limitHit = false;
  for (const a of rows) {
    if (eligible.length >= limit) { limitHit = true; break; }
    const reason = gateReason(a, { nowIso, couponUsable });
    if (reason) { skipped.push({ id: a.id, action_type: a.action_type, reason }); continue; }
    eligible.push({ ...a, monthlyCoupon: a.action_type === 'coupon' ? monthlyCoupon : null });
  }
  return { eligible, skipped, monthlyCouponReady: couponUsable, limitHit };
}

/**
 * ゲート付き claim: 単一トランザクション内で全ゲートを再評価してから ready→claimed + attempt 予約
 * (Codex C4-R1 High: select と claim の間に ownership/suppression/レビュー/purge/発送日が
 * 変わる TOCTOU を、書きロック (immediate tx) 内の再検証で塞ぐ)。
 * @returns { messageId, fresh } | { gateFailed: reason } | { claimLost: true }
 */
function claimActionGuardedT(T, A, db, actionId, nowIso = new Date().toISOString()) {
  const claimToken = crypto.randomBytes(8).toString('hex');
  const tx = db.transaction(() => {
    const a = db.prepare(`${gateSql(T)} WHERE a.id = ?`).get(actionId);
    if (!a) throw Object.assign(new Error('gone'), { gateFailed: 'not_found' });
    const month = jstDateOf(nowIso).slice(0, 7);
    const monthlyCoupon = a.action_type === 'coupon' ? A.monthlyCouponFor(db, month) : null;
    const couponUsable = a.action_type === 'coupon' ? couponUsableCheck(monthlyCoupon, nowIso) : true;
    const reason = gateReason(a, { nowIso, couponUsable });
    if (reason) throw Object.assign(new Error(reason), { gateFailed: reason });
    const messageId = messageIdFor(a.id, a.dedupe_key);
    const r = db.prepare(`
      UPDATE ${T.actions}
         SET status = 'claimed', claim_token = ?, claimed_at = ?, updated_at = ?
       WHERE id = ? AND status = 'ready'
    `).run(claimToken, nowIso, nowIso, a.id);
    if (r.changes !== 1) throw Object.assign(new Error('claim race lost'), { claimLost: true });
    db.prepare(`
      INSERT INTO ${T.attempts} (action_id, message_id, attempted_at, outcome)
      VALUES (?, ?, ?, 'ambiguous')
    `).run(a.id, messageId, nowIso);
    return { messageId, claimToken, fresh: { ...a, monthlyCoupon } };
  });
  try {
    return tx.immediate();
  } catch (e) {
    if (e.gateFailed) return { gateFailed: e.gateFailed };
    if (e.claimLost || /UNIQUE/i.test(String(e.message))) return { claimLost: true }; // attempt既存=絶対に送らない
    throw e;
  }
}

/** claimed のリース時間。これを超えた claimed だけを残留とみなす (Codex C4-R2 High:
 *  リース無しだと SMTP 送信中の正常な claim を並行プロセスが横取りしてしまう。
 *  SMTP の各タイムアウトは 20〜30 秒 → 15 分は十分な余裕) */
export const STALE_CLAIM_LEASE_MS = 15 * 60 * 1000;

/**
 * リース切れ claimed の回収 (Codex C4-R1 High: claim commit 後のクラッシュは
 * SMTP 実行有無を断定できない → ambiguous に収束させ、人が確認するまで送信を始めない)。
 * @returns { recovered, inFlight } — inFlight = リース内の claimed (回収しない。別プロセス送信中の可能性)
 */
function recoverStaleClaimsT(T, db, nowIso = new Date().toISOString(), leaseMs = STALE_CLAIM_LEASE_MS) {
  const cutoff = new Date(Date.parse(nowIso) - leaseMs).toISOString();
  const tx = db.transaction(() => {
    const stale = db.prepare(`SELECT id FROM ${T.actions} WHERE status = 'claimed' AND claimed_at <= ?`).all(cutoff);
    for (const { id } of stale) {
      db.prepare(`UPDATE ${T.attempts} SET note = 'stale_claim_recovered' WHERE action_id = ? AND note IS NULL`).run(id);
      db.prepare(`UPDATE ${T.actions} SET status = 'ambiguous', status_reason = 'stale_claim', updated_at = ? WHERE id = ? AND status = 'claimed'`).run(nowIso, id);
    }
    const inFlight = db.prepare(`SELECT COUNT(*) n FROM ${T.actions} WHERE status = 'claimed'`).get().n;
    return { recovered: stale.length, inFlight };
  });
  return tx.immediate();
}

/** 送信結果の確定 (claimed → sent / failed_safe)。claimToken を照合し、リース回収などで
 *  claim を失っていたら何も書かない (Codex C4-R2 High: 横取り後の上書きで矛盾状態を作らない)。
 *  @returns true=確定できた / false=claim を失っていた (attempt も触らない) */
function finalizeAttemptT(T, db, { actionId, outcome, claimToken, smtpCode = null, note = null, nowIso = new Date().toISOString() }) {
  // claimToken 必須 (Codex Y0-R2 High: NULL で照合を迂回できると別 worker の claim を確定できてしまう)
  if (!claimToken) throw new Error('finalizeAttempt: claimToken は必須');
  const status = outcome === 'accepted' ? 'sent' : 'failed_safe';
  const tx = db.transaction(() => {
    const r = db.prepare(`
      UPDATE ${T.actions} SET status = ?, status_reason = NULL, updated_at = ?
       WHERE id = ? AND status = 'claimed' AND claim_token = ?
    `).run(status, nowIso, actionId, claimToken);
    if (r.changes !== 1) return false;
    db.prepare(`
      UPDATE ${T.attempts} SET outcome = ?, smtp_code = ?, note = ?
       WHERE action_id = ?
    `).run(outcome, smtpCode != null ? String(smtpCode) : null, note ? String(note).slice(0, 120) : null, actionId);
    return true;
  });
  return tx.immediate();
}

/** 結果不明の確定 (claimed → ambiguous、attempt は ambiguous のまま note のみ)。claimToken 照合つき */
function markAmbiguousT(T, db, { actionId, claimToken, note = null, nowIso = new Date().toISOString() }) {
  if (!claimToken) throw new Error('markAmbiguous: claimToken は必須');
  const tx = db.transaction(() => {
    const r = db.prepare(`
      UPDATE ${T.actions} SET status = 'ambiguous', status_reason = 'smtp_unknown', updated_at = ?
       WHERE id = ? AND status = 'claimed' AND claim_token = ?
    `).run(nowIso, actionId, claimToken);
    if (r.changes !== 1) return false;
    db.prepare(`
      UPDATE ${T.attempts} SET note = ? WHERE action_id = ?
    `).run(note ? String(note).slice(0, 120) : null, actionId);
    return true;
  });
  return tx.immediate();
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
async function processReadyActionsT(T, A, db, { keys, sendFn, nowIso = new Date().toISOString(), limit = 5 }) {
  // リース切れ claimed の回収が先 (あれば送信を始めない — 人の確認が先)。
  // リース内の claimed (別プロセスが送信中の可能性) がある場合も開始しない (単一運転を前提とした保険)
  const { recovered: staleRecovered, inFlight } = recoverStaleClaimsT(T, db, nowIso);
  const out = { sent: 0, failedSafe: 0, ambiguous: 0, skipped: 0, claimLost: 0, gateFailed: 0, finalizeConflict: 0, recipientRetry: 0, releaseConflict: 0, staleRecovered, inFlight, details: [] };
  if (staleRecovered > 0 || inFlight > 0) {
    out.details.push({ result: staleRecovered > 0 ? 'stale_claims_recovered' : 'in_flight_claims_present', count: staleRecovered || inFlight });
    return out;
  }
  const { eligible, skipped, limitHit } = selectEligibleActionsT(T, A, db, { nowIso, limit });
  out.skipped = skipped.length;
  out.limitHit = limitHit;
  for (const action of eligible) {
    // claim は tx 内で全ゲート再評価 (TOCTOU 防止)。文面・宛先とも tx 内の最新スナップショットを使う
    const claim = claimActionGuardedT(T, A, db, action.id, nowIso);
    if (claim.gateFailed) { out.gateFailed++; out.details.push({ id: action.id, result: 'gate_failed', reason: claim.gateFailed }); continue; }
    if (claim.claimLost) { out.claimLost++; out.details.push({ id: action.id, result: 'claim_lost' }); continue; }
    // 復号は claim 後に fresh スナップショットから (Codex C4-R2 Medium: select 時の暗号文を使うと
    // 直前の contacts 同期で更新された宛先とズレる)。失敗=SMTP未実行が確定 → failed_safe に明示収束
    // 宛先解決はアダプタ (楽天=復号、Yahoo=API 即時取得→メモリのみ)。SMTP に触れる前なので
    // e.retryable=true の失敗は claim を解放して ready に戻す (PR-Y-0: contact_fetch_retryable、
    // 二重送信にはならない=attempt 行を消して次回の正午に再評価)。それ以外の失敗は failed_safe で終端
    let to;
    try {
      to = await A.resolveRecipient(db, claim.fresh, keys);
    } catch (e) {
      if (e && e.retryable) {
        // 解放できなければ (claim を失っている/attempt が触られている) ambiguous と同じく人の確認へ
        if (releaseClaimT(T, db, { actionId: action.id, claimToken: claim.claimToken, nowIso })) {
          out.recipientRetry++;
          out.details.push({ id: action.id, result: 'recipient_retry', reason: String(e.code || 'retryable') });
        } else {
          // 送信結果の確定競合 (finalizeConflict) とは別事象なので専用カウンタ (Codex Y0-R2 High)。
          // claim 喪失 / attempt 改変 = 別 worker や状態不明を排除できないので、ambiguous と同じく
          // バッチを即時中断して人の確認へ (Codex Y0-R3 High)
          out.releaseConflict++;
          out.details.push({ id: action.id, result: 'release_conflict', reason: String(e.code || 'retryable') });
          break;
        }
        continue;
      }
      const okF = finalizeAttemptT(T, db, { actionId: action.id, outcome: 'rejected', claimToken: claim.claimToken, note: (e && e.code) || 'contact_undecryptable', nowIso });
      if (okF) { out.failedSafe++; out.details.push({ id: action.id, result: 'failed_safe', reason: (e && e.code) || 'contact_undecryptable' }); }
      else { out.finalizeConflict++; out.details.push({ id: action.id, result: 'finalize_conflict', reason: (e && e.code) || 'contact_undecryptable' }); }
      continue;
    }
    const mail = A.buildMail(claim.fresh, nowIso);
    try {
      await sendFn({ to, from: A.fromHeader, subject: mail.subject, text: mail.text, messageId: claim.messageId });
      // note には固定分類のみ (SMTPエラー原文は宛先が混入し得るため保存しない — Codex C4-R1 Medium)
      const ok = finalizeAttemptT(T, db, { actionId: action.id, outcome: 'accepted', claimToken: claim.claimToken, smtpCode: null, nowIso });
      if (ok) { out.sent++; out.details.push({ id: action.id, result: 'sent', type: action.action_type }); }
      else { out.finalizeConflict++; out.details.push({ id: action.id, result: 'finalize_conflict' }); }
    } catch (e) {
      const cls = classifySendError(e);
      if (cls.kind === 'rejected') {
        const ok = finalizeAttemptT(T, db, { actionId: action.id, outcome: 'rejected', claimToken: claim.claimToken, smtpCode: cls.code, note: 'smtp_rejected', nowIso });
        if (ok) { out.failedSafe++; out.details.push({ id: action.id, result: 'failed_safe', code: cls.code }); }
        else { out.finalizeConflict++; out.details.push({ id: action.id, result: 'finalize_conflict' }); }
      } else {
        const ok = markAmbiguousT(T, db, { actionId: action.id, claimToken: claim.claimToken, note: 'smtp_unknown', nowIso });
        if (!ok) out.finalizeConflict++;
        out.ambiguous++;
        out.details.push({ id: action.id, result: 'ambiguous' });
        break; // 結果不明は即中断 — 続行しない (設計: 自動再送禁止・人に確認)
      }
    }
  }
  return out;
}

/** claim の解放 (PR-Y-0)。SMTP を一切呼んでいないことが確実な段階 (宛先解決の一時失敗) でだけ使う:
 *  claimed→ready に戻し、予約した attempt 行を消す (UNIQUE(action_id) を再び使えるようにする)。claimToken 照合つき */
function releaseClaimT(T, db, { actionId, claimToken, nowIso = new Date().toISOString() }) {
  // claimToken は必須 (Codex Y0-R1 High: token 無しで解放できると、別 worker が送信中の claim を
  // 横取り解放→再 claim→二重送信になる)。attempt 行が「予約のまま未使用」であることを先に検証し、
  // 1 行ちょうど消せなければ全体をロールバック (Codex Y0-R1 Medium)
  if (!claimToken) throw new Error('releaseClaim: claimToken は必須');
  const tx = db.transaction(() => {
    const att = db.prepare(`SELECT outcome, smtp_code, note FROM ${T.attempts} WHERE action_id = ?`).get(actionId);
    if (!att || att.outcome !== 'ambiguous' || att.smtp_code !== null || att.note !== null) {
      throw Object.assign(new Error('attempt_not_pristine'), { releaseFailed: true });
    }
    const r = db.prepare(`
      UPDATE ${T.actions} SET status = 'ready', claim_token = NULL, claimed_at = NULL, updated_at = ?
       WHERE id = ? AND status = 'claimed' AND claim_token = ?
    `).run(nowIso, actionId, claimToken);
    if (r.changes !== 1) throw Object.assign(new Error('claim_not_owned'), { releaseFailed: true });
    const d = db.prepare(`DELETE FROM ${T.attempts} WHERE action_id = ? AND outcome = 'ambiguous' AND smtp_code IS NULL AND note IS NULL`).run(actionId);
    if (d.changes !== 1) throw Object.assign(new Error('attempt_delete_failed'), { releaseFailed: true });
    return true;
  });
  try {
    return tx.immediate();
  } catch (e) {
    if (e.releaseFailed) return false;
    throw e;
  }
}

/** 楽天アダプタ (既定)。PR-Y-0 以前の挙動と同一 */
export const RAKUTEN_SENDER_ADAPTER = Object.freeze({
  mall: 'rakuten',
  monthlyCouponFor: (db, month) => getRegisteredCoupon(db, month),
  resolveRecipient: async (db, action, keys) => decryptEmail(action.masked_email_enc, keys, action.order_number),
  buildMail: (action, nowIso) => buildMailForAction(action, nowIso),
  fromHeader: `"${SHOP_NAME}" <${FROM_ADDRESS}>`,
});
const RT = tablesFor('rakuten');

// ─── 互換エクスポート (楽天束縛。既存の呼び出し側・テストは無変更) ───
export const selectEligibleActions = (db, o) => selectEligibleActionsT(RT, RAKUTEN_SENDER_ADAPTER, db, o);
export const claimActionGuarded = (db, actionId, nowIso) => claimActionGuardedT(RT, RAKUTEN_SENDER_ADAPTER, db, actionId, nowIso);
export const recoverStaleClaims = (db, nowIso, leaseMs) => recoverStaleClaimsT(RT, db, nowIso, leaseMs);
export const finalizeAttempt = (db, a) => finalizeAttemptT(RT, db, a);
export const markAmbiguous = (db, a) => markAmbiguousT(RT, db, a);
export const releaseClaim = (db, a) => releaseClaimT(RT, db, a);
export const processReadyActions = (db, o) => processReadyActionsT(RT, RAKUTEN_SENDER_ADAPTER, db, o);

/**
 * モール別送信エンジン (PR-Y-0)。adapter = { mall, monthlyCouponFor(db, month), resolveRecipient(db, action, keys) (async、
 * 一時失敗は err.retryable=true で throw)、buildMail(action, nowIso) → {subject, text}、fromHeader }
 */
export function createSenderEngine(adapter = RAKUTEN_SENDER_ADAPTER) {
  // 楽天以外は 4 つのモール固有項目を必須にする (Codex Y0-R1 Medium: 指定漏れを楽天の
  // クーポン台帳・文面・From で静かに補完すると、Yahoo が楽天の URL/署名で送ってしまう)
  const A = { ...adapter };
  if (!A.mall) throw new Error('createSenderEngine: adapter.mall は必須');
  if (A.mall === 'rakuten') {
    Object.assign(A, { ...RAKUTEN_SENDER_ADAPTER, ...adapter });
  } else {
    for (const k of ['monthlyCouponFor', 'resolveRecipient', 'buildMail', 'fromHeader']) {
      if (A[k] == null) throw new Error(`createSenderEngine(${A.mall}): adapter.${k} は必須 (楽天既定は継承しない)`);
    }
  }
  const T = tablesFor(A.mall);
  return {
    mall: A.mall, tables: T, adapter: A,
    selectEligibleActions: (db, o) => selectEligibleActionsT(T, A, db, o),
    claimActionGuarded: (db, actionId, nowIso) => claimActionGuardedT(T, A, db, actionId, nowIso),
    recoverStaleClaims: (db, nowIso, leaseMs) => recoverStaleClaimsT(T, db, nowIso, leaseMs),
    finalizeAttempt: (db, a) => finalizeAttemptT(T, db, a),
    markAmbiguous: (db, a) => markAmbiguousT(T, db, a),
    releaseClaim: (db, a) => releaseClaimT(T, db, a),
    processReadyActions: (db, o) => processReadyActionsT(T, A, db, o),
  };
}
