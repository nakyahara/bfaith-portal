/**
 * inquiry-hub 受信同期エンジン (設計書§8.1/§8.2) — チャネル非依存の共通フレーム
 *
 * 責務: リース(多重起動防止) / 同期ウィンドウ(committed_until + オーバーラップ) /
 *       UPSERT(外部ID重複排除) / 状態遷移(顧客新着のみ) / エラー記録
 * チャネル固有のAPI呼び出しはアダプターに委譲する (Step 3以降で gmail/rakuten/yahoo を実装)。
 *
 * アダプター契約 (設計書§5.2 最小IF。fetchNew のみ本エンジンが使う):
 *   await adapter.fetchNew({ sinceIso, untilIso, cursor }) → {
 *     inquiries: [{
 *       externalInquiryId,                       // 必須。楽天inquiryNumber / Yahoo topicId / Gmail threadId
 *       customerName?, customerIdentifier?, subject?,
 *       orderNumber?, productCode?, productName?,
 *       externalStatus?, externalIsRead?,        // モール側状態 (表示専用カラムへ)
 *       receivedAt,                              // 必須 (新規作成時に使用)
 *       messages: [{
 *         externalMessageId?,                    // 無ければ syntheticMessageId() で決定的に採番
 *         senderType,                            // 'customer' | 'shop' | 'system'
 *         senderName?, bodyText?, bodyHtml?,
 *         isIncoming,                            // 1: 顧客→店舗
 *         sentAt?, receivedAt?,
 *         attachments?: [{ externalAttachmentId?, fileName?, contentType?, fileSize? }],
 *       }],
 *     }],
 *     nextCursor?,      // Gmail historyId 等。次回の fetchNew に渡す
 *     observedUntil?,   // アダプターが完全列挙を保証できる上限時刻。untilIso まで列挙しきれない場合に
 *                       //   必ず返すこと (committed_until はこの値までしか前進しない)。省略 = untilIso まで完全列挙した
 *   }
 *   ⚠️ 部分成功を返さないこと。ページ途中で失敗したら全体を throw する
 *      (committed_until はエンジンが「全件取り込み成功時のみ」前進させる。§8.1)
 *   ⚠️ fetchNew はリース期間 (既定10分) 内に完了すること。超過して別ジョブにリースを奪われた場合、
 *      本エンジンは lease_token の所有権確認によりコミットを破棄する (lease_lost)
 */
import crypto from 'crypto';
import { getDB, logActivity, toUtcIso } from '../db.js';

export const OVERLAP_MS = 60 * 60 * 1000;          // 取得ウィンドウのオーバーラップ (60分。§8.1)
export const DEFAULT_BACKFILL_DAYS = 30;           // 初回同期の遡り日数
export const DEFAULT_LEASE_MINUTES = 10;           // 同期リースの有効期間
export const REPAIR_DAYS = 3;                      // 修復同期の再照合日数 (§8.1)

/** 外部IDが無いメッセージ用の決定的synthetic ID (設計書§8.2。再同期で必ず同じ値になる) */
export function syntheticMessageId(externalInquiryId, isIncoming, sentAtIso, bodyText) {
  const h = crypto.createHash('sha256')
    .update(`${externalInquiryId}\n${isIncoming ? 1 : 0}\n${sentAtIso || ''}\n${bodyText || ''}`, 'utf8')
    .digest('hex').slice(0, 32);
  return `syn:${h}`;
}

/** 添付の決定的synthetic ID (external_attachment_id NOT NULL 制約用) */
export function syntheticAttachmentId(externalMessageId, fileName, fileSize) {
  const h = crypto.createHash('sha256')
    .update(`${externalMessageId}\n${fileName || ''}\n${fileSize ?? ''}`, 'utf8')
    .digest('hex').slice(0, 32);
  return `syn:${h}`;
}

/** sync_state 行の取得 (無ければ作成) */
function ensureSyncState(db, shopId) {
  db.prepare('INSERT OR IGNORE INTO sync_state (shop_id) VALUES (?)').run(shopId);
  return db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
}

/**
 * 同期リースの取得。BEGIN IMMEDIATE で原子的にclaimする (設計書§5.2)。
 * 取得できなければ null。取得できたら所有者トークンを返す
 * (期限切れ後に別ジョブへ移ったリースを、旧ジョブが解除/上書きしないため。Codexレビュー反映)。
 */
function acquireLease(db, shopId, nowMs, leaseMinutes) {
  const tx = db.transaction(() => {
    const st = ensureSyncState(db, shopId);
    if (st.lease_until && Date.parse(st.lease_until) > nowMs) return null;
    const token = crypto.randomUUID();
    db.prepare('UPDATE sync_state SET lease_until = ?, lease_token = ?, last_sync_started_at = ? WHERE shop_id = ?')
      .run(toUtcIso(nowMs + leaseMinutes * 60000), token, toUtcIso(nowMs), shopId);
    return token;
  });
  return tx.immediate();
}

/** リース解放 (所有している場合のみ) */
function releaseLease(db, shopId, token) {
  db.prepare('UPDATE sync_state SET lease_until = NULL, lease_token = NULL WHERE shop_id = ? AND lease_token = ?')
    .run(shopId, token);
}

/** 1チケット分のUPSERT + メッセージ/添付取り込み + 状態遷移。トランザクション内で呼ぶこと */
function ingestInquiry(db, shop, item, nowIso) {
  if (!item.externalInquiryId) throw new Error('adapter bug: externalInquiryId がありません');
  const stats = { newInquiry: false, newMessages: 0, newCustomerMessages: 0, reopened: false };

  let inq = db.prepare('SELECT * FROM inquiries WHERE channel_type = ? AND shop_id = ? AND external_inquiry_id = ?')
    .get(shop.channel_type, shop.id, item.externalInquiryId);

  const extStatus = item.externalStatus == null ? null : String(item.externalStatus);
  const extRead = item.externalIsRead == null ? null : (item.externalIsRead ? 1 : 0);

  if (!inq) {
    const r = db.prepare(`INSERT INTO inquiries (
        channel_type, shop_id, external_inquiry_id, customer_name, customer_identifier, subject,
        external_status, external_is_read, last_external_synced_at,
        order_number, product_code, product_name, received_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(shop.channel_type, shop.id, item.externalInquiryId,
        item.customerName ?? null, item.customerIdentifier ?? null, item.subject ?? null,
        extStatus, extRead, nowIso,
        item.orderNumber ?? null, item.productCode ?? null, item.productName ?? null,
        toUtcIso(item.receivedAt));
    inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(r.lastInsertRowid);
    stats.newInquiry = true;
  } else {
    // 外部状態は表示専用カラムへそのまま保存 (internal_statusには反映しない。§8.1)。
    // 顧客名・注文番号等はアダプターが値を返した場合のみ上書き (nullで消さない)
    db.prepare(`UPDATE inquiries SET
        external_status = COALESCE(?, external_status),
        external_is_read = COALESCE(?, external_is_read),
        last_external_synced_at = ?,
        customer_name = COALESCE(?, customer_name),
        customer_identifier = COALESCE(?, customer_identifier),
        subject = COALESCE(?, subject),
        order_number = COALESCE(?, order_number),
        product_code = COALESCE(?, product_code),
        product_name = COALESCE(?, product_name),
        updated_at = ?
      WHERE id = ?`)
      .run(extStatus, extRead, nowIso,
        item.customerName ?? null, item.customerIdentifier ?? null, item.subject ?? null,
        item.orderNumber ?? null, item.productCode ?? null, item.productName ?? null,
        nowIso, inq.id);
  }

  const insMsg = db.prepare(`INSERT OR IGNORE INTO inquiry_messages (
      inquiry_id, external_message_id, sender_type, sender_name,
      message_body_text, message_body_html, is_incoming, sent_at, received_at
    ) VALUES (?,?,?,?,?,?,?,?,?)`);
  const insAtt = db.prepare(`INSERT OR IGNORE INTO inquiry_attachments (
      inquiry_message_id, external_attachment_id, file_name, content_type, file_size
    ) VALUES (?,?,?,?,?)`);

  let maxMsgAt = null;
  for (const m of item.messages || []) {
    const sentAt = m.sentAt ? toUtcIso(m.sentAt) : null;
    const receivedAt = m.receivedAt ? toUtcIso(m.receivedAt) : sentAt;
    const extMsgId = m.externalMessageId
      || syntheticMessageId(item.externalInquiryId, m.isIncoming, sentAt || receivedAt, m.bodyText);
    const r = insMsg.run(inq.id, extMsgId, m.senderType, m.senderName ?? null,
      m.bodyText ?? null, m.bodyHtml ?? null, m.isIncoming ? 1 : 0, sentAt, receivedAt);
    const msgAt = receivedAt || sentAt;
    if (msgAt && (!maxMsgAt || msgAt > maxMsgAt)) maxMsgAt = msgAt;
    if (r.changes > 0) {
      stats.newMessages++;
      if (m.isIncoming) stats.newCustomerMessages++;
    }
    // 添付メタデータは既存メッセージでも毎回照合する
    // (初回レスポンスで添付が欠落していた場合に修復同期で取り込めるように。Codexレビュー反映)
    const msgRow = db.prepare('SELECT id FROM inquiry_messages WHERE inquiry_id = ? AND external_message_id = ?')
      .get(inq.id, extMsgId);
    for (const a of m.attachments || []) {
      const extAttId = a.externalAttachmentId || syntheticAttachmentId(extMsgId, a.fileName, a.fileSize);
      insAtt.run(msgRow.id, extAttId, a.fileName ?? null, a.contentType ?? null, a.fileSize ?? null);
    }
  }

  if (stats.newMessages > 0) {
    // 会話リビジョンは「新規メッセージ1件につき+1」(DBコメントの前提と一致させる。Codexレビュー反映)
    // + last_message_at 前進 (巻き戻さない)
    db.prepare(`UPDATE inquiries SET
        conversation_rev = conversation_rev + ?,
        last_message_at = CASE WHEN last_message_at IS NULL OR last_message_at < ? THEN ? ELSE last_message_at END,
        updated_at = ?
      WHERE id = ?`).run(stats.newMessages, maxMsgAt, maxMsgAt, nowIso, inq.id);
    db.prepare('UPDATE ai_drafts SET is_stale = 1 WHERE inquiry_id = ? AND is_stale = 0').run(inq.id);
  }
  if (stats.newCustomerMessages > 0) {
    db.prepare('UPDATE inquiries SET is_unread = 1 WHERE id = ?').run(inq.id);
    // internal_status を同期が触るのはこの1パターンのみ: 顧客新着で done → open (再問い合わせ。§8.1)
    if (inq.internal_status === 'done' && !stats.newInquiry) {
      db.prepare("UPDATE inquiries SET internal_status = 'open', completed_at = NULL WHERE id = ? AND internal_status = 'done'").run(inq.id);
      logActivity(inq.id, { actorType: 'system', actionType: 'status_change',
        before: { internal_status: 'done' }, after: { internal_status: 'open', reason: '顧客新着で再オープン' } });
      stats.reopened = true;
    }
  }
  return stats;
}

/**
 * 1店舗の受信同期を実行する (設計書§8.1のフロー)。
 *
 * @param {number} shopId
 * @param {object} adapter fetchNew を持つチャネルアダプター
 * @param {object} opts { now?: エポックms|ISO (テスト用時計), leaseMinutes?, backfillDays?, repair?: boolean }
 * @returns {{ ok: boolean, skipped?: string, error?: string, stats?: object }}
 */
export async function runSync(shopId, adapter, opts = {}) {
  const db = getDB();
  const nowMs = opts.now != null ? (typeof opts.now === 'number' ? opts.now : Date.parse(opts.now)) : Date.now();
  const nowIso = toUtcIso(nowMs);

  const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(shopId);
  if (!shop) return { ok: false, error: `shop ${shopId} が存在しません` };
  if (!shop.is_active) return { ok: false, skipped: 'inactive' };

  const leaseToken = acquireLease(db, shopId, nowMs, opts.leaseMinutes ?? DEFAULT_LEASE_MINUTES);
  if (!leaseToken) return { ok: false, skipped: 'lease' };

  const st = db.prepare('SELECT * FROM sync_state WHERE shop_id = ?').get(shopId);
  const backfillMs = (opts.backfillDays ?? DEFAULT_BACKFILL_DAYS) * 86400000;
  // 通常: committed_until − オーバーラップ。修復同期(repair): 直近 REPAIR_DAYS を強制再照合
  let sinceMs = st.committed_until ? Date.parse(st.committed_until) - OVERLAP_MS : nowMs - backfillMs;
  if (opts.repair) sinceMs = Math.min(sinceMs, nowMs - REPAIR_DAYS * 86400000);
  const sinceIso = toUtcIso(Math.max(0, sinceMs));

  try {
    const fetched = await adapter.fetchNew({ sinceIso, untilIso: nowIso, cursor: st.sync_cursor });
    const items = fetched?.inquiries || [];

    // committed_until はアダプターが完全列挙を保証した時刻まで (observedUntil。省略時は untilIso=now。§8.1)。
    // high-water mark なので単調増加 (既存値より後退させない。Codexレビュー反映)
    const observedIso = fetched?.observedUntil ? toUtcIso(fetched.observedUntil) : nowIso;
    const boundedObservedIso = observedIso < nowIso ? observedIso : nowIso;
    const committedIso = st.committed_until && st.committed_until > boundedObservedIso
      ? st.committed_until : boundedObservedIso;

    const totals = { inquiries: items.length, newInquiries: 0, newMessages: 0, reopened: 0 };
    const tx = db.transaction(() => {
      // 所有権確認: リースを奪われていたら (fetchNew がリース期間超過) コミットを破棄 (Codexレビュー反映)
      const cur = db.prepare('SELECT lease_token FROM sync_state WHERE shop_id = ?').get(shopId);
      if (cur?.lease_token !== leaseToken) {
        const e = new Error('リース期間超過により別ジョブへ移行済み。取り込みを破棄します');
        e.errorType = 'lease_lost';
        throw e;
      }
      for (const item of items) {
        const s = ingestInquiry(db, shop, item, nowIso);
        totals.newInquiries += s.newInquiry ? 1 : 0;
        totals.newMessages += s.newMessages;
        totals.reopened += s.reopened ? 1 : 0;
      }
      // 全件取り込み成功時のみ high-water mark を前進 (§8.1)
      db.prepare(`UPDATE sync_state SET
          committed_until = ?, observed_until = ?, sync_cursor = ?,
          last_sync_completed_at = ?, last_error = NULL, consecutive_failures = 0
        WHERE shop_id = ?`)
        .run(committedIso, observedIso, fetched?.nextCursor ?? st.sync_cursor, nowIso, shopId);
      db.prepare('UPDATE shops SET last_synced_at = ?, updated_at = ? WHERE id = ?').run(nowIso, nowIso, shopId);
    });
    tx.immediate();
    return { ok: true, stats: totals };
  } catch (e) {
    const detail = String(e?.message || e).slice(0, 1000);
    db.prepare(`INSERT INTO sync_errors (shop_id, error_type, error_detail) VALUES (?, ?, ?)`)
      .run(shopId, e?.errorType || 'fetch_failed', detail);
    // sync_state の失敗記録は所有者である間のみ (奪われた後に他ジョブの状態を汚さない)
    db.prepare(`UPDATE sync_state SET last_error = ?, consecutive_failures = consecutive_failures + 1
      WHERE shop_id = ? AND lease_token = ?`).run(detail, shopId, leaseToken);
    return { ok: false, error: detail, leaseLost: e?.errorType === 'lease_lost' || undefined };
  } finally {
    releaseLease(db, shopId, leaseToken);
  }
}

/** 全アクティブ店舗の同期状態サマリ (同期管理画面用) */
export function listSyncStatus() {
  const db = getDB();
  return db.prepare(`SELECT s.id AS shop_id, s.channel_type, s.shop_name, s.authentication_status,
      s.last_synced_at, st.committed_until, st.lease_until, st.last_error, st.consecutive_failures,
      (SELECT COUNT(*) FROM sync_errors e WHERE e.shop_id = s.id AND e.resolved = 0) AS open_errors
    FROM shops s LEFT JOIN sync_state st ON st.shop_id = s.id
    WHERE s.is_active = 1 ORDER BY s.channel_type, s.shop_name`).all();
}
