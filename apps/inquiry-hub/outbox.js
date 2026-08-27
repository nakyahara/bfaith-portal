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
 *   await adapter.sendReply({ inquiry, shop, bodyText, attachmentsJson }) → { externalReplyId?, externalThreadId?, warning? }
 *   - warning = 送信は成功したが人が確認すべき事象 (例: Gmailの送信元From置き換え)。sentのままerror_messageに表示
 *   - externalThreadId = 送信で新しく外部スレッドが生まれたときのID (このアプリ発の新規メール。compose.js)。
 *     受け取ったら仮の external_inquiry_id と差し替える = 以後の受信が同じチケットに合流する
 *   - 外部APIが「受け付けなかった」と明確に分かる失敗は SendRejectedError を throw (→ failed。再送は安全)
 *   - それ以外の失敗 (タイムアウト・5xx・切断等、送信された可能性が残るもの) は通常の throw (→ unknown)
 *   await adapter.completeInquiry?.({ inquiry }) → { ok } (任意。2026-08-26「送信して回答完了」)
 *   - complete_on_send のジョブで sendReply 成功の直後に呼ぶ = モール側も「回答完了」にする (Yahoo!)
 *   - 失敗しても返信は届いているので sent のまま。error_message に警告を残し、人がモール画面で完了にする
 */
import crypto from 'crypto';
import { getDB, logActivity, toUtcIso } from './db.js';
import { claimAttachmentsForJob, loadJobAttachments } from './reply-attachments.js';
import { blockedReplyDestination } from './no-reply.js';
import { attachExternalThread, markComposeThreadUnresolved } from './compose.js';
import { isComposeThread } from './compose-id.js';

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
export function createReplyJob({ inquiryId, channelType, bodyText, attachmentIds = [],
  createdBy, clientOperationId, baseConversationRev, completeOnSend = false }) {
  const db = getDB();
  if (!bodyText || !String(bodyText).trim()) throw new Error('本文が空です');
  if (!clientOperationId) throw new Error('clientOperationId は必須です');
  if (!Number.isInteger(baseConversationRev)) throw new Error('baseConversationRev は必須です');
  // 送信用添付 (2026-08-20) はメール+楽天のみ (Gmail=multipart / 楽天=R-Messe添付API。
  // Yahoo!は問い合わせAPIが添付非対応 — メールディーラーでも不可だった)
  if (attachmentIds.length && !['email', 'rakuten'].includes(channelType)) {
    throw new Error('このチャネルの返信は添付に対応していません (対応: メール・楽天)');
  }

  const tx = db.transaction(() => {
    const existing = db.prepare('SELECT id FROM outbox_replies WHERE client_operation_id = ?').get(clientOperationId);
    if (existing) return { id: existing.id, created: false };

    const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error(`inquiry ${inquiryId} が存在しません`);
    if (inq.channel_type !== channelType) throw new Error('channelType が問い合わせと一致しません');
    // 返信不可アドレス宛は作成時点で断る (2026-08-26 本番事故。no-reply.js 参照)。
    // 送信ワーカーにも同じガードがあるが、ここで止めれば「送信ジョブは作れたのに届かない」
    // という分かりにくい失敗を避けられる
    if (channelType === 'email') {
      const dest = blockedReplyDestination(inq.customer_identifier);
      if (dest) return { id: null, created: false, conflict: `${dest.reason}。${dest.guide}` };
    }
    // 同一問い合わせに未決着ジョブがある間は新規作成不可 (並行ワーカーでも1問い合わせ1送信を保証。Codexレビュー反映)
    // 未決着 = pending/sending/needs_review、および人手解決前の unknown
    const active = db.prepare(`SELECT id, status FROM outbox_replies
        WHERE inquiry_id = ? AND (status IN ('pending','sending','needs_review')
          OR (status = 'unknown' AND resolution IS NULL))`).get(inquiryId);
    if (active) {
      return { id: null, created: false, conflict: `この問い合わせには未決着の送信ジョブ (#${active.id}: ${active.status}) があります。解決または取消してから再作成してください` };
    }
    // 登録時点でのrev競合チェック (送信直前にもworkerが再チェックする。§7.5)
    if (inq.conversation_rev !== baseConversationRev) {
      return { id: null, created: false, conflict: `会話が更新されています (rev ${baseConversationRev} → ${inq.conversation_rev})。内容を確認してから再送信してください` };
    }
    const r = db.prepare(`INSERT INTO outbox_replies
        (inquiry_id, channel_type, client_operation_id, body_text, attachments_json, created_by, base_conversation_rev, complete_on_send)
      VALUES (?,?,?,?,?,?,?,?)`)
      .run(inquiryId, channelType, clientOperationId, String(bodyText), null, createdBy, baseConversationRev, completeOnSend ? 1 : 0);
    // 添付の紐付けと attachments_json の確定は同一トランザクション内で
    // (途中で失敗したら丸ごとロールバック = ジョブだけ残って添付が無い、を作らない)
    const atts = claimAttachmentsForJob(db, { inquiryId, outboxId: r.lastInsertRowid, attachmentIds });
    if (atts.length) {
      db.prepare('UPDATE outbox_replies SET attachments_json = ? WHERE id = ?')
        .run(JSON.stringify(atts.map(a => ({ id: a.id, name: a.name, size: a.size }))), r.lastInsertRowid);
    }
    logActivity(inquiryId, { actorType: 'user', userId: createdBy, actionType: 'reply_created',
      operationId: clientOperationId, after: { outbox_id: r.lastInsertRowid, ...(atts.length ? { attachments: atts.length } : {}) } });
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

/** pending を1件 claim する (BEGIN IMMEDIATE + executor制約 + チャネル制約。設計書§8.3(b)/§5.3)。無ければ null
 * channels: claim対象のチャネル一覧 (=送信アダプターを渡されたチャネルのみ)。
 * アダプターが無い/ドライランのチャネルのジョブは claim せず pending のまま残す
 * (誤って needs_review へ消費したり、他チャネルの処理を詰まらせたりしない) */
function claimNext(db, executor, nowMs, leaseMinutes, channels) {
  if (!channels.length) return null;
  const tx = db.transaction(() => {
    // 同一問い合わせに sending が存在する間はclaimしない (作成ガードに加えた多層防御。Codexレビュー反映)
    const job = db.prepare(`SELECT o.* FROM outbox_replies o
        JOIN inquiries i ON i.id = o.inquiry_id
        JOIN shops s ON s.id = i.shop_id
      WHERE o.status = 'pending' AND s.executor = ?
        AND o.channel_type IN (${channels.map(() => '?').join(',')})
        AND NOT EXISTS (SELECT 1 FROM outbox_replies o2 WHERE o2.inquiry_id = o.inquiry_id AND o2.status = 'sending')
      ORDER BY o.id LIMIT 1`).get(executor, ...channels);
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
  // 現在時刻は claim ごとに取り直す (先行ジョブの送信が長引くと後続のリースが最初から期限切れ近くになるため。
  // Codexレビュー反映)。opts.now はテスト用固定時計
  const tick = () => opts.now != null ? (typeof opts.now === 'number' ? opts.now : Date.parse(opts.now)) : Date.now();
  const leaseMinutes = opts.leaseMinutes ?? DEFAULT_SEND_LEASE_MINUTES;

  const swept = sweepZombies({ now: tick() });
  const results = [];
  // claim対象 = sendReply を持つアダプターが渡されたチャネルのみ (それ以外は pending のまま触らない)
  const channels = Object.keys(adapters || {}).filter(ch => typeof adapters[ch]?.sendReply === 'function');

  for (let n = 0; n < (opts.maxJobs ?? 20); n++) {
    const nowMs = tick();
    const nowIso = toUtcIso(nowMs);
    const job = claimNext(db, executor, nowMs, leaseMinutes, channels);
    if (!job) break;
    const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(job.inquiry_id);
    const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(inq.shop_id);

    // 状態更新+ログ+エラー記録は同一txにまとめ、finishJob が0件 (=リース喪失) なら副作用を出さない (Codexレビュー反映)
    const finishTx = (fields, sideEffects) => db.transaction(() => {
      if (!finishJob(db, job, fields)) return false;
      if (sideEffects) sideEffects();
      return true;
    }).immediate();

    // 送信直前のrev競合チェック → needs_review (workerは二度と拾わない。人間が新ジョブとして再作成。§8.3)
    if (inq.conversation_rev !== job.base_conversation_rev) {
      const done = finishTx({ status: 'needs_review', lease_token: null, lease_until: null,
        error_message: `送信保留: 会話が更新されました (rev ${job.base_conversation_rev} → ${inq.conversation_rev})` },
      () => logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_needs_review',
        operationId: job.client_operation_id, after: { outbox_id: job.id } }));
      results.push({ id: job.id, outcome: done ? 'needs_review' : 'lease_lost' });
      continue;
    }

    const adapter = adapters?.[job.channel_type];
    if (!adapter?.sendReply) {
      const done = finishTx({ status: 'needs_review', lease_token: null, lease_until: null,
        error_message: `送信保留: ${job.channel_type} の送信アダプター未設定` });
      results.push({ id: job.id, outcome: done ? 'needs_review' : 'lease_lost' });
      continue;
    }

    try {
      // 送信用添付の実体を読み出す。記録 (attachments_json) と実体が食い違うときは
      // 未送信確定の failed に落とす (添付漏れのまま送るより安全。実体はまだ送っていない)
      let attachments = [];
      if (job.attachments_json) {
        let wanted = -1;
        try { wanted = JSON.parse(job.attachments_json).length; } catch { /* 壊れた記録は下の不一致で拒否 */ }
        attachments = loadJobAttachments(job.id);
        if (attachments.length !== wanted) {
          throw new SendRejectedError('添付の実体が見つかりません (未送信)。添付し直して新しいジョブを作成してください');
        }
      }
      const sent = await adapter.sendReply({
        inquiry: inq, shop, bodyText: job.body_text, attachmentsJson: job.attachments_json, attachments,
      });
      // DRYRUNアダプターの結果はジョブを消費しない (sent確定させると未送信メールが送信済み扱いに
      // なり顧客返信が欠落する。Codexレビュー反映)。pending に戻して pass を終了 (同一passでの再claim防止)
      if (sent?.dryRun) {
        const done = finishTx({ status: 'pending', lease_token: null, lease_until: null,
          error_message: 'DRYRUN検証のみ実施 (未送信。INQUIRY_HUB_MAIL_SEND_MODE=live で実送信されます)' });
        results.push({ id: job.id, outcome: done ? 'dryrun' : 'lease_lost' });
        break;
      }
      const extReplyId = sent?.externalReplyId || `op:${job.client_operation_id}`;
      // モール側の「回答完了」(2026-08-26)。返信は確定しているので、ここの失敗は sent を崩さず警告に留める
      let externalCompleted = false, completeWarning = null;
      if (job.complete_on_send && typeof adapter.completeInquiry === 'function') {
        try {
          const c = await adapter.completeInquiry({ inquiry: inq, shop });
          externalCompleted = !!c?.ok;
        } catch (e) {
          completeWarning = `返信は送信済みですが、モール側の回答完了に失敗しました (モールの管理画面で「回答完了」にしてください): ${String(e?.message || e).slice(0, 200)}`;
          console.warn(`[outbox] #${job.id} completeInquiry 失敗 (sentは維持): ${e?.message || e}`);
        }
      }
      const warnings = [sent?.warning, completeWarning].filter(Boolean).map(String);
      const sentIso = toUtcIso(tick()); // 完了時刻は送信後に取り直す (長い送信でもsent_atを正確に)
      const tx = db.transaction(() => {
        // warning = 送信は成功したが要注意 (例: Gmailが送信元Fromを置き換えた → 楽天マスク
        // アドレス宛はバウンスする)。ジョブ履歴の error_message として表示して気付けるようにする
        if (!finishJob(db, job, { status: 'sent', external_reply_id: extReplyId, sent_at: sentIso,
          lease_token: null, lease_until: null, error_message: warnings.length ? warnings.join(' / ').slice(0, 500) : null })) {
          // リースを失っていた (ゾンビ扱いでunknown化済み等) → メッセージ二重挿入を避けて何もしない
          return false;
        }
        // 送信済みメッセージとして会話に記録 (§8.3)。UNIQUE(inquiry, external_message_id) で冪等
        db.prepare(`INSERT OR IGNORE INTO inquiry_messages
            (inquiry_id, external_message_id, sender_type, sender_name, message_body_text,
             is_incoming, sent_by_user_id, outbox_id, sent_at, received_at)
          VALUES (?,?,'shop',?,?,0,?,?,?,?)`)
          .run(job.inquiry_id, extReplyId, job.created_by, job.body_text, job.created_by, job.id, sentIso, sentIso);
        // 新規メール (compose.js) の仮ID 'compose:…' を実際のスレッドIDに差し替える。
        // これで次回の同期で同じスレッドとして合流し、顧客の返信も同じチケットに載る。
        // 同期が先に同じスレッドを取り込んでいた場合 (衝突) は差し替えられないので、
        // ジョブ履歴に警告を足して人が気付けるようにする (問い合わせ側は⚠️要確認になる)
        if (isComposeThread(inq.external_inquiry_id)) {
          let att = { attached: false };
          let attachError = null;
          if (!sent?.externalThreadId) {
            // スレッドIDが取れないと以後の受信が別チケットになる。黙って成功に見せない
            attachError = '送信先のメールスレッドIDを取得できませんでした';
            try { markComposeThreadUnresolved(db, job.inquiry_id, { error: attachError }); }
            catch (e2) { console.error(`[outbox] #${job.id} 要確認フラグも立てられませんでした: ${e2?.message || e2}`); }
          } else {
            try {
              att = attachExternalThread(db, job.inquiry_id, sent.externalThreadId) || att;
            } catch (e) {
              // 差し替えに失敗しても送信は完了している。黙って成功に見せず、印と警告を残す
              attachError = String(e?.message || e).slice(0, 200);
              try { markComposeThreadUnresolved(db, job.inquiry_id, { error: attachError }); }
              catch (e2) { console.error(`[outbox] #${job.id} 要確認フラグも立てられませんでした: ${e2?.message || e2}`); }
            }
          }
          const warn = att.conflictInquiryId
            ? `送信は完了しましたが、同じメールスレッドの問い合わせ #${att.conflictInquiryId} が既にあります`
              + ' (会話が2つに分かれています。内容を確認してください)'
            : attachError
              ? `送信は完了しましたが、メールスレッドの紐付けに失敗しました (${attachError})。`
                + 'この宛先からの返信は別の問い合わせとして届く可能性があります'
              : null;
          if (warn) {
            db.prepare("UPDATE outbox_replies SET error_message = COALESCE(error_message || ' / ', '') || ? WHERE id = ?")
              .run(warn, job.id);
            console.warn(`[outbox] #${job.id} ${warn}`);
          }
        }
        // 自送信も conversation_rev++ (以後の送信操作は送信後の会話を基準にする)
        // 送信後のステータス (メールディーラー準拠): 通常は「返信処理中」、
        // 「送信して完了にする」を選んでいれば「完了」まで進める
        if (job.complete_on_send) {
          db.prepare(`UPDATE inquiries SET conversation_rev = conversation_rev + 1,
              last_message_at = CASE WHEN last_message_at IS NULL OR last_message_at < ? THEN ? ELSE last_message_at END,
              internal_status = 'done', completed_at = COALESCE(completed_at, ?), is_unread = 0,
              delivery_failed_at = NULL,
              external_status = CASE WHEN ? THEN 'completed' ELSE external_status END,
              updated_at = ? WHERE id = ?`)
            .run(sentIso, sentIso, sentIso, externalCompleted ? 1 : 0, sentIso, job.inquiry_id);
        } else {
          db.prepare(`UPDATE inquiries SET conversation_rev = conversation_rev + 1,
              last_message_at = CASE WHEN last_message_at IS NULL OR last_message_at < ? THEN ? ELSE last_message_at END,
              internal_status = CASE WHEN internal_status IN ('open','in_progress','pending') THEN 'waiting_reply' ELSE internal_status END,
              delivery_failed_at = NULL,
              updated_at = ? WHERE id = ?`)
            .run(sentIso, sentIso, sentIso, job.inquiry_id);
        }
        logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_sent',
          operationId: job.client_operation_id, after: { outbox_id: job.id, external_reply_id: extReplyId,
            ...(externalCompleted ? { external_completed: true } : {}), ...(completeWarning ? { external_complete_failed: true } : {}) } });
        return true;
      });
      results.push({ id: job.id, outcome: tx.immediate() ? 'sent' : 'lease_lost' });
    } catch (e) {
      const detail = String(e?.message || e).slice(0, 500);
      const recordError = () => db.prepare('INSERT INTO sync_errors (shop_id, inquiry_id, error_type, error_detail) VALUES (?,?,?,?)')
        .run(shop.id, job.inquiry_id, 'send_failed', detail);
      if (e instanceof SendRejectedError) {
        // 明確な拒否 = 未送信確定 → failed (UIで「未送信」明示。§8.3)
        const done = finishTx({ status: 'failed', error_message: detail, lease_token: null, lease_until: null }, recordError);
        results.push({ id: job.id, outcome: done ? 'failed' : 'lease_lost' });
      } else {
        // 結果不明 → unknown。自動再送しない (§8.3)
        const done = finishTx({ status: 'unknown', error_message: detail, lease_token: null, lease_until: null }, () => {
          recordError();
          logActivity(job.inquiry_id, { actorType: 'system', actionType: 'reply_unknown',
            operationId: job.client_operation_id, after: { outbox_id: job.id } });
        });
        results.push({ id: job.id, outcome: done ? 'unknown' : 'lease_lost' });
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
    // resolution 確定後は不変の終端状態 (abandoned を後から confirmed_* に変更できない。Codexレビュー反映)
    const job = db.prepare("SELECT * FROM outbox_replies WHERE id = ? AND status = 'unknown' AND resolution IS NULL").get(outboxId);
    if (!job) throw new Error(`outbox ${outboxId} は未解決の unknown 状態ではありません`);
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

/**
 * pending / needs_review の送信ジョブを取消す (人間の操作)。
 * needs_review からの再送信は「取消 → 新しいジョブ作成」の手順で行う (設計書§8.3)。
 */
export function cancelJob(outboxId, cancelledBy) {
  const db = getDB();
  const tx = db.transaction(() => {
    const job = db.prepare("SELECT * FROM outbox_replies WHERE id = ? AND status IN ('pending','needs_review')").get(outboxId);
    if (!job) throw new Error(`outbox ${outboxId} は取消可能な状態 (pending/needs_review) ではありません`);
    db.prepare("UPDATE outbox_replies SET status = 'cancelled', resolved_by = ?, resolved_at = ? WHERE id = ? AND status IN ('pending','needs_review')")
      .run(cancelledBy, toUtcIso(Date.now()), outboxId);
    logActivity(job.inquiry_id, { actorType: 'user', userId: cancelledBy, actionType: 'reply_cancelled',
      operationId: job.client_operation_id, after: { outbox_id: job.id } });
    return { id: job.id, status: 'cancelled' };
  });
  return tx.immediate();
}

/** 送信結果不明/保留/失敗の一覧 (画面§7.1#4用)。resolution確定済み (終端) は載せない。
 * pending も含める (運用者が送信前に止められるように。Codexレビュー反映) */
export function listOutboxIssues() {
  const db = getDB();
  return db.prepare(`SELECT o.*, i.subject, i.customer_name, i.channel_type AS inquiry_channel, s.shop_name
    FROM outbox_replies o
    JOIN inquiries i ON i.id = o.inquiry_id
    JOIN shops s ON s.id = i.shop_id
    WHERE o.status IN ('pending', 'needs_review')
       OR (o.status IN ('unknown', 'failed') AND o.resolution IS NULL)
    ORDER BY o.created_at DESC, o.id DESC LIMIT 200`).all();
}
