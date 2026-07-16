/**
 * inquiry-hub 返信送信 outbox (設計書§8.3) — チャネル非依存の送信ジョブ管理
 *
 * 大原則 (設計書冒頭): 外部への送信は exactly-once にできない。
 *   二重送信の確率を最小化し、結果不明 (unknown) は自動再送せず人間が解決する。
 *
 * 状態機械:
 *   pending ──claim──▶ sending ──成功──▶ sent
 *                        │──明確な拒否──▶ failed
 *                        │──結果不明 (タイムアウト等)──▶ unknown ──人手解決──▶ resolution 確定
 *                        └──リース切れ (ゾンビ)──▶ unknown (再送しない)
 *   pending ──rev競合──▶ needs_review (workerは二度と拾わない。人間が新しいジョブとして再作成)
 *
 * 送信アダプター契約 (設計書§5.2 最小IF):
 *   await adapter.sendReply({ inquiry, shop, bodyText, attachmentsJson }) → { externalReplyId? }
 *   - 外部APIが「受け付けなかった」と明確に分かる失敗は SendRejectedError を throw (→ failed。再送は安全)
 *   - それ以外の失敗 (タイムアウト・5xx・切断等、送信された可能性が残るもの) は通常の throw (→ unknown)
 */
import crypto from 'crypto';
import { getDB, logActivity, toUtcIso } from './db.js';

export const DEFAULT_SEND_LEASE_MINUTES = 5;

/** 外部が明確に拒否した失敗 (未送信が確定している) 用のエラー型 */
export class SendRejectedError extends Error {
  constructor(message) { super(message); this.name = 'SendRejectedError'; }
}

/**
 * 送信ジョブの作成 (送信ボタン押下)。client_operation_id で冪等
 * (同じ操作IDでの再POSTは既存ジョブを返すだけで二重登録しない。設計書§8.3)。
 *
 * @returns {{ id, created: boolean, conflict?: string }}
 */
export function createReplyJob({ inquiryId, channelType, bodyText, attachmentsJson = null,
  createdBy, clientOperationId, baseConversationRev }) {
  const db = getDB();
  if (!bodyText || !String(bodyText).trim()) throw new Error('本文が空です');
  if (!clientOperationId) throw new Error('clientOperationId は必須です');
  if (!Number.isInteger(baseConversationRev)) throw new Error('baseConversationRev は必須です');

  const tx = db.transaction(() => {
    const existing = db.prepare('SELECT id FROM outbox_replies WHERE client_operation_id = ?').get(clientOperationId);
    if (existing) return { id: existing.id, created: false };

    const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error(`inquiry ${inquiryId} が存在しません`);
    if (inq.channel_type !== channelType) throw new Error('channelType が問い合わせと一致しません');
    // 登録時点でのrev競合チェック (送信直前にもworkerが再チェックする。§7.5)
    if (inq.conversation_rev !== baseConversationRev) {
      return { id: null, created: false, conflict: `会話が更新されています (rev ${baseConversationRev} → ${inq.conversation_rev})。内容を確認してから再送信してください` };
    }
    const r = db.prepare(`INSERT INTO outbox_replies
        (inquiry_id, channel_type, client_operation_id, body_text, attachments_json, created_by, base_conversation_rev)
      VALUES (?,?,?,?,?,?,?)`)
      .run(inquiryId, channelType, clientOperationId, String(bodyText), attachmentsJson, createdBy, baseConversationRev);
    logActivity(inquiryId, { actorType: 'user', userId: createdBy, actionType: 'reply_created',
      operationId: clientOperationId, after: { outbox_id: r.lastInsertRowid } });
    return { id: r.lastInsertRowid, created: true };
  });
  return tx.immediate();
}

/**
 * ゾンビ回収 (設計書§8.3(a)): lease切れの sending を unknown に移す。
 * pending には戻さない (「外部送信成功後・DB更新前にクラッシュ」の可能性があるため、
 * 照合または人手確認なしの再送は絶対にしない)。
 */
export function sweepZombies({ now } = {}) {
  const db = getDB();
  const nowIso = toUtcIso(now ?? Date.now());
  const zombies = db.prepare(
    "SELECT id, inquiry_id, client_operation_id FROM outbox_replies WHERE status = 'sending' AND lease_until IS NOT NULL AND lease_until < ?")
    .all(nowIso);
  const tx = db.transaction(() => {
    for (const z of zombies) {
      const r = db.prepare(
        "UPDATE outbox_replies SET status = 'unknown', error_message = '送信中にワーカーが停止 (結果不明)' WHERE id = ? AND status = 'sending' AND lease_until < ?")
        .run(z.id, nowIso);
      if (r.changes > 0) {
        logActivity(z.inquiry_id, { actorType: 'system', actionType: 'reply_unknown',
          operationId: z.client_operation_id, after: { outbox_id: z.id, reason: 'lease expired' } });
      }
    }
  });
  tx.immediate();
  return zombies.length;
}

/** pending を1件 claim する (BEGIN IMMEDIATE + executor制約。設計書§8.3(b)/§5.3)。無ければ null */
function claimNext(db, executor, nowMs, leaseMinutes) {
  const tx = db.transaction(() => {
    const job = db.prepare(`SELECT o.* FROM outbox_replies o
        JOIN inquiries i ON i.id = o.inquiry_id
        JOIN shops s ON s.id = i.shop_id
      WHERE o.status = 'pending' AND s.executor = ?
      ORDER BY o.id LIMIT 1`).get(executor);
    if (!job) return null;
    const leaseToken = crypto.randomUUID();
    db.prepare("UPDATE outbox_replies SET status = 'sending', lease_token = ?, lease_until = ? WHERE id = ? AND status = 'pending'")
      .run(leaseToken, toUtcIso(nowMs + leaseMinutes * 60000), job.id);
    return { ...job, lease_token: leaseToken };
  });
  return tx.immediate();
}

/** lease_token 一致を条件にジョブを完了状態へ (ゾンビ処理との競合上書き防止) */
function finishJob(db, job, fields) {
  const sets = Object.keys(fields).map(k => `${k} = ?`).join(', ');
  return db.prepare(`UPDATE outbox_replies SET ${sets} WHERE id = ? AND lease_token = ? AND status = 'sending'`)
    .run(...Object.values(fields), job.id, job.lease_token).changes > 0;
}

/**
 * 送信ワーカー1周 (設計書§8.3)。cron/ランナーから定期実行する。
 *
 * @param {object} adapters チャネル別送信アダプター { email?, rakuten?, yahoo? }
 * @param {object} opts { executor?: 'server'|'runner', now?: エポックms (テスト用), leaseMinutes?, maxJobs? }
 * @returns {{ swept, processed, results: [{id, outcome}] }}
 */
export async function runOutboxPass(adapters, opts = {}) {
  const db = getDB();
  const executor = opts.executor ?? 'server';
  const nowMs = opts.now != null ? (typeof opts.now === 'number' ? opts.now : Date.parse(opts.now)) : Date.now();
  const nowIso = toUtcIso(nowMs);
  const leaseMinutes = opts.leaseMinutes ?? DEFAULT_SEND_LEASE_MINUTES;

  const swept = sweepZombies({ now: nowMs });
  const results = [];

  for (let n = 0; n < (opts.maxJobs ?? 20); n++) {
    const job = claimNext(db, executor, nowMs, leaseMinutes);
    if (!job) break;
    const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(job.inquiry_id);
    const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(inq.shop_id);

    // 送信直前のrev競合チェック → needs_review (workerは二度と拾わない。人間が新ジョブとして再作成。§8.3)
    if (inq.conversation_rev !== job.base_conversation_rev) {
      finishJob(db, job, { status: 'needs_review', lease_token: null, lease_until: null,
        error_message: `送信保留: 会話が更新されました (rev ${job.base_conversation_rev} → ${inq.conversation_rev})` });
      logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_needs_review',
        operationId: job.client_operation_id, after: { outbox_id: job.id } });
      results.push({ id: job.id, outcome: 'needs_review' });
      continue;
    }

    const adapter = adapters?.[job.channel_type];
    if (!adapter?.sendReply) {
      finishJob(db, job, { status: 'needs_review', lease_token: null, lease_until: null,
        error_message: `送信保留: ${job.channel_type} の送信アダプター未設定` });
      results.push({ id: job.id, outcome: 'needs_review' });
      continue;
    }

    try {
      const sent = await adapter.sendReply({
        inquiry: inq, shop, bodyText: job.body_text, attachmentsJson: job.attachments_json,
      });
      const extReplyId = sent?.externalReplyId || `op:${job.client_operation_id}`;
      const tx = db.transaction(() => {
        if (!finishJob(db, job, { status: 'sent', external_reply_id: extReplyId, sent_at: nowIso,
          lease_token: null, lease_until: null, error_message: null })) {
          // リースを失っていた (ゾンビ扱いでunknown化済み等) → メッセージ二重挿入を避けて何もしない
          return false;
        }
        // 送信済みメッセージとして会話に記録 (§8.3)。UNIQUE(inquiry, external_message_id) で冪等
        db.prepare(`INSERT OR IGNORE INTO inquiry_messages
            (inquiry_id, external_message_id, sender_type, sender_name, message_body_text,
             is_incoming, sent_by_user_id, outbox_id, sent_at, received_at)
          VALUES (?,?,'shop',?,?,0,?,?,?,?)`)
          .run(job.inquiry_id, extReplyId, job.created_by, job.body_text, job.created_by, job.id, nowIso, nowIso);
        // 自送信も conversation_rev++ (以後の送信操作は送信後の会話を基準にする)
        db.prepare(`UPDATE inquiries SET conversation_rev = conversation_rev + 1,
            last_message_at = CASE WHEN last_message_at IS NULL OR last_message_at < ? THEN ? ELSE last_message_at END,
            internal_status = CASE WHEN internal_status IN ('open','in_progress') THEN 'waiting_reply' ELSE internal_status END,
            updated_at = ? WHERE id = ?`)
          .run(nowIso, nowIso, nowIso, job.inquiry_id);
        logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_sent',
          operationId: job.client_operation_id, after: { outbox_id: job.id, external_reply_id: extReplyId } });
        return true;
      });
      results.push({ id: job.id, outcome: tx.immediate() ? 'sent' : 'lease_lost' });
    } catch (e) {
      const detail = String(e?.message || e).slice(0, 500);
      if (e instanceof SendRejectedError) {
        // 明確な拒否 = 未送信確定 → failed (UIで「未送信」明示。§8.3)
        finishJob(db, job, { status: 'failed', error_message: detail, lease_token: null, lease_until: null });
        db.prepare('INSERT INTO sync_errors (shop_id, inquiry_id, error_type, error_detail) VALUES (?,?,?,?)')
          .run(shop.id, job.inquiry_id, 'send_failed', detail);
        results.push({ id: job.id, outcome: 'failed' });
      } else {
        // 結果不明 → unknown。自動再送しない (§8.3)
        finishJob(db, job, { status: 'unknown', error_message: detail, lease_token: null, lease_until: null });
        db.prepare('INSERT INTO sync_errors (shop_id, inquiry_id, error_type, error_detail) VALUES (?,?,?,?)')
          .run(shop.id, job.inquiry_id, 'send_failed', detail);
        logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_unknown',
          operationId: job.client_operation_id, after: { outbox_id: job.id } });
        results.push({ id: job.id, outcome: 'unknown' });
      }
    }
  }
  return { swept, processed: results.length, results };
}

/**
 * unknown の人手解決 (設計書§8.3・画面§7.1#4)。
 * - confirmed_sent:     モール画面等で送信済みを確認した → sent 扱い (会話にも記録)
 * - confirmed_not_sent: 未送信を確認した → failed 扱い。再送はこの場合のみ (新ジョブとして)
 * - abandoned:          確認できない → 再送不可のまま終了
 */
export function resolveUnknown(outboxId, resolution, resolvedBy, { now } = {}) {
  const db = getDB();
  if (!['confirmed_sent', 'confirmed_not_sent', 'abandoned'].includes(resolution)) {
    throw new Error(`不正な resolution: ${resolution}`);
  }
  const nowIso = toUtcIso(now ?? Date.now());
  const tx = db.transaction(() => {
    const job = db.prepare("SELECT * FROM outbox_replies WHERE id = ? AND status = 'unknown'").get(outboxId);
    if (!job) throw new Error(`outbox ${outboxId} は unknown 状態ではありません`);
    const status = resolution === 'confirmed_sent' ? 'sent' : resolution === 'confirmed_not_sent' ? 'failed' : 'unknown';
    db.prepare('UPDATE outbox_replies SET status = ?, resolution = ?, resolved_by = ?, resolved_at = ? WHERE id = ?')
      .run(status, resolution, resolvedBy, nowIso, outboxId);
    if (resolution === 'confirmed_sent') {
      db.prepare(`INSERT OR IGNORE INTO inquiry_messages
          (inquiry_id, external_message_id, sender_type, sender_name, message_body_text,
           is_incoming, sent_by_user_id, outbox_id, sent_at, received_at)
        VALUES (?,?,'shop',?,?,0,?,?,?,?)`)
        .run(job.inquiry_id, job.external_reply_id || `op:${job.client_operation_id}`, job.created_by,
          job.body_text, job.created_by, job.id, job.sent_at || nowIso, job.sent_at || nowIso);
      db.prepare('UPDATE inquiries SET conversation_rev = conversation_rev + 1, updated_at = ? WHERE id = ?')
        .run(nowIso, job.inquiry_id);
    }
    logActivity(job.inquiry_id, { actorType: 'user', userId: resolvedBy, actionType: 'reply_resolved',
      operationId: job.client_operation_id, after: { outbox_id: job.id, resolution } });
    return { id: job.id, status, resolution };
  });
  return tx.immediate();
}

/** 送信結果不明/保留/失敗の一覧 (画面§7.1#4用) */
export function listOutboxIssues() {
  const db = getDB();
  return db.prepare(`SELECT o.*, i.subject, i.customer_name, i.channel_type AS inquiry_channel, s.shop_name
    FROM outbox_replies o
    JOIN inquiries i ON i.id = o.inquiry_id
    JOIN shops s ON s.id = i.shop_id
    WHERE o.status IN ('unknown', 'needs_review', 'failed')
    ORDER BY o.created_at DESC, o.id DESC LIMIT 200`).all();
}
