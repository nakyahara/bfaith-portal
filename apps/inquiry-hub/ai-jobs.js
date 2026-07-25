/**
 * AI返信案ジョブ基盤 (設計書§9、Step 7) — サーバー側ロジック
 *
 * 方式 (§9.1): ローカルClaude Codeエージェント (定時バッチ・サブスク内) が
 *   GET queue → POST claim → 生成 → POST result のHTTP契約だけで連携する。
 *   入力はサーバーが組み立てて渡す (エージェントに追加取得をさせない=インジェクション対策§9.2)。
 *
 * 安全設計 (§9.2):
 * - claim時に input_rev (conversation_rev) を記録し、result時に現在revと一致しなければ破棄して
 *   queued に戻す (新着を見ていない古い返信案の上書き防止)
 * - lease_until 超過の processing はサーバー側で自動的に queued へ戻す
 * - AIへ渡す顧客本文は電話番号・郵便番号・メールアドレスをマスク (個人情報の最小化)
 * - 出力検証: 型・長さ上限・URL/メールアドレスの許可リスト検査をしてから保存
 */
import crypto from 'crypto';
import { getDB, logActivity, toUtcIso } from './db.js';
import { listQa } from './templates.js';

export const AI_CLAIM_LEASE_MINUTES = 30;
const ENQUEUE_LIMIT = 30;          // 1回のsweepで新規投入する上限
const RECENT_DAYS = 14;            // 対象=直近14日に動きのある問い合わせ (初回に過去分を大量生成しない)
const DRAFT_BODY_MAX = 5000;

// 返信案に含めてよいURL/メールのドメイン許可リスト (それ以外の混入は保存拒否。§9.2)
const URL_ALLOW = ['b-faith.biz', 'rakuten.co.jp', 'rakuten.ne.jp', 'yahoo.co.jp', 'amazon.co.jp'];
const MAIL_ALLOW = ['info@b-faith.biz'];

/** AIに渡す本文の個人情報マスク (電話/郵便番号/メールアドレス)。注文番号 (例 373343-20260617-...) は保持。
 * 順序が重要: 電話番号を先にマスクしないと郵便番号の \d{3}-\d{4} が電話番号の一部を食う */
export function maskPii(text) {
  return String(text || '')
    .replace(/[\w.+-]+@[\w-]+\.[\w.-]+/g, '[メールアドレス]')
    .replace(/(?<![\d-])0\d{1,4}[-()\s]?\d{1,4}[-()\s]?\d{3,4}(?![\d-])/g, '[電話番号]')
    .replace(/(?<![\d-])〒?\s*\d{3}-\d{4}(?![\d-])/g, '[郵便番号]');
}

/** 期限切れ processing を queued へ戻す (§9.1) */
export function requeueExpiredAiJobs(nowMs = Date.now()) {
  const db = getDB();
  return db.prepare(`UPDATE ai_jobs SET status = 'queued', lease_token = NULL, lease_until = NULL
    WHERE status = 'processing' AND lease_until IS NOT NULL AND lease_until < ?`).run(toUtcIso(nowMs)).changes;
}

/**
 * 対象問い合わせを自動投入する:
 *   open/in_progress かつ 最後のメッセージが顧客 (=返信待ち) かつ
 *   現revの未stale返信案が無い かつ 未決着のAIジョブが無い、直近14日に動きあり。
 *   ai_needed (スタッフのAIフラグ) 優先 → 新しい順
 */
export function enqueueAiJobs({ nowMs = Date.now(), limit = ENQUEUE_LIMIT } = {}) {
  const db = getDB();
  const since = toUtcIso(nowMs - RECENT_DAYS * 86400000);
  const rows = db.prepare(`SELECT i.id, i.conversation_rev FROM inquiries i
    WHERE i.internal_status IN ('open', 'in_progress') AND i.is_archived = 0
      AND COALESCE(i.last_message_at, i.received_at) >= ?
      AND (SELECT m.is_incoming FROM inquiry_messages m WHERE m.inquiry_id = i.id
             ORDER BY COALESCE(m.received_at, m.sent_at) DESC, m.id DESC LIMIT 1) = 1
      AND NOT EXISTS (SELECT 1 FROM ai_drafts d WHERE d.inquiry_id = i.id
             AND d.input_rev = i.conversation_rev AND d.is_stale = 0)
      AND NOT EXISTS (SELECT 1 FROM ai_jobs j WHERE j.inquiry_id = i.id
             AND j.status IN ('queued', 'processing'))
    ORDER BY i.ai_needed DESC, COALESCE(i.last_message_at, i.received_at) DESC
    LIMIT ?`).all(since, limit);
  const ins = db.prepare('INSERT INTO ai_jobs (inquiry_id, input_rev) VALUES (?, ?)');
  const tx = db.transaction(() => { for (const r of rows) ins.run(r.id, r.conversation_rev); });
  tx.immediate();
  return rows.length;
}

/** queued 一覧 (sweep込み)。ランナーはこれを見て claim する */
export function listAiQueue({ nowMs = Date.now(), limit = 50 } = {}) {
  requeueExpiredAiJobs(nowMs);
  enqueueAiJobs({ nowMs });
  const db = getDB();
  return db.prepare(`SELECT j.id, j.inquiry_id, j.created_at, i.channel_type, i.subject, i.ai_needed
    FROM ai_jobs j JOIN inquiries i ON i.id = j.inquiry_id
    WHERE j.status = 'queued' ORDER BY i.ai_needed DESC, j.id LIMIT ?`).all(limit);
}

/** claim対象ジョブの入力ペイロードを組み立てる (PIIマスク済み。§9.2「入力はサーバーが組み立てる」) */
function buildJobPayload(db, job) {
  const inq = db.prepare(`SELECT i.*, s.shop_name, s.channel_type AS shop_channel FROM inquiries i
    JOIN shops s ON s.id = i.shop_id WHERE i.id = ?`).get(job.inquiry_id);
  const messages = db.prepare(`SELECT sender_type, is_incoming, message_body_text, COALESCE(received_at, sent_at) AS at
    FROM inquiry_messages WHERE inquiry_id = ? ORDER BY COALESCE(received_at, sent_at), id`).all(job.inquiry_id);
  return {
    jobId: job.id,
    inquiryId: job.inquiry_id,
    inputRev: inq.conversation_rev,
    channel: inq.channel_type,
    shopName: inq.shop_name,
    subject: maskPii(inq.subject),
    customerName: inq.customer_name || null, // 表示名のみ (識別子は渡さない)
    orderNumber: inq.order_number || null,
    productCode: inq.product_code || null,
    productName: inq.product_name || null,
    aiFlag: inq.ai_needed,
    messages: messages.map(m => ({
      from: m.is_incoming ? 'customer' : 'shop',
      at: m.at,
      body: maskPii(String(m.message_body_text || '').slice(0, 8000)),
    })),
  };
}

/**
 * claim: queued → processing (lease発行+input_rev記録)。入力ペイロードとQ&Aナレッジを返す。
 * @returns {{ jobs: [...], qa: [...] }}
 */
export function claimAiJobs(jobIds, { nowMs = Date.now(), leaseMinutes = AI_CLAIM_LEASE_MINUTES } = {}) {
  if (!Array.isArray(jobIds) || jobIds.length === 0 || jobIds.length > 10) {
    throw new Error('job_ids は1〜10件で指定してください');
  }
  const db = getDB();
  const claimed = [];
  const tx = db.transaction(() => {
    for (const id of jobIds) {
      const job = db.prepare("SELECT * FROM ai_jobs WHERE id = ? AND status = 'queued'").get(Number(id));
      if (!job) continue;
      const inq = db.prepare('SELECT conversation_rev FROM inquiries WHERE id = ?').get(job.inquiry_id);
      const leaseToken = crypto.randomUUID();
      db.prepare(`UPDATE ai_jobs SET status = 'processing', lease_token = ?, lease_until = ?, input_rev = ?
        WHERE id = ? AND status = 'queued'`)
        .run(leaseToken, toUtcIso(nowMs + leaseMinutes * 60000), inq.conversation_rev, job.id);
      claimed.push({ ...job, lease_token: leaseToken, input_rev: inq.conversation_rev });
    }
  });
  tx.immediate();
  const jobs = claimed.map(j => ({ ...buildJobPayload(db, j), inputRev: j.input_rev, leaseToken: j.lease_token }));
  // Q&Aナレッジ (メールディーラー移行52件+手動分。listQa は is_active=1 のみ返す)。回答品質の土台
  const qa = (listQa().rows || []).slice(0, 100)
    .map(q => ({ q: String(q.title || q.question || '').slice(0, 500), a: String(q.answer || '').slice(0, 1000) }));
  return { jobs, qa };
}

/** 出力検証 (§9.2): 型・長さ・URL/メール許可リスト。NGは理由を返す */
export function validateDraftOutput(d) {
  const s = v => (v == null ? '' : String(v));
  if (!s(d.draftBody).trim()) return '返信案本文 (draftBody) が空です';
  if (s(d.draftBody).length > DRAFT_BODY_MAX) return `返信案が長すぎます (${DRAFT_BODY_MAX}文字まで)`;
  for (const [k, max] of [['summary', 500], ['category', 100], ['notes', 1000], ['confirmationItems', 1000], ['modelInfo', 200]]) {
    if (s(d[k]).length > max) return `${k} が長すぎます (${max}文字まで)`;
  }
  const text = s(d.draftBody);
  for (const m of text.matchAll(/https?:\/\/([^\s/"'<>]+)/gi)) {
    const host = m[1].toLowerCase();
    if (!URL_ALLOW.some(a => host === a || host.endsWith('.' + a))) return `許可されていないURLが含まれています (${host})`;
  }
  for (const m of text.matchAll(/[\w.+-]+@[\w-]+\.[\w.-]+/g)) {
    if (!MAIL_ALLOW.includes(m[0].toLowerCase())) return `許可されていないメールアドレスが含まれています (${m[0]})`;
  }
  return null;
}

/**
 * result: lease_token一致 + input_rev==現在rev を検証して ai_drafts に保存。
 * rev不一致は結果破棄+queuedへ戻す (discarded)。検証NGは failed。
 * @returns {{ outcome: 'done'|'discarded'|'rejected'|'not_found' , reason?: string }}
 */
export function submitAiResult({ jobId, leaseToken, inputRev, summary, category, draftBody, notes, confirmationItems, modelInfo }) {
  const db = getDB();
  const tx = db.transaction(() => {
    const job = db.prepare("SELECT * FROM ai_jobs WHERE id = ? AND status = 'processing' AND lease_token = ?")
      .get(Number(jobId), String(leaseToken || ''));
    if (!job) return { outcome: 'not_found', reason: 'ジョブが processing+lease一致 で見つかりません (期限切れ回収済みの可能性)' };
    const inq = db.prepare('SELECT conversation_rev FROM inquiries WHERE id = ?').get(job.inquiry_id);
    if (Number(inputRev) !== job.input_rev || inq.conversation_rev !== job.input_rev) {
      // 新着があった → 古い会話への返信案は破棄して再生成させる (§9.1)
      db.prepare("UPDATE ai_jobs SET status = 'queued', lease_token = NULL, lease_until = NULL WHERE id = ?").run(job.id);
      return { outcome: 'discarded', reason: `会話が更新されています (rev ${job.input_rev} → ${inq.conversation_rev})` };
    }
    const bad = validateDraftOutput({ summary, category, draftBody, notes, confirmationItems, modelInfo });
    if (bad) {
      db.prepare("UPDATE ai_jobs SET status = 'failed', lease_token = NULL, lease_until = NULL, completed_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?").run(job.id);
      return { outcome: 'rejected', reason: bad };
    }
    db.prepare(`INSERT INTO ai_drafts (inquiry_id, ai_job_id, input_rev, summary, category, draft_body, notes, confirmation_items, model_info)
      VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(job.inquiry_id, job.id, job.input_rev,
        summary ? String(summary) : null, category ? String(category) : null, String(draftBody),
        notes ? String(notes) : null, confirmationItems ? String(confirmationItems) : null,
        modelInfo ? String(modelInfo) : null);
    db.prepare("UPDATE ai_jobs SET status = 'done', lease_token = NULL, lease_until = NULL, completed_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?").run(job.id);
    logActivity(job.inquiry_id, { actorType: 'ai', actionType: 'ai_draft', after: { job_id: job.id, rev: job.input_rev } });
    return { outcome: 'done' };
  });
  return tx.immediate();
}

/** ランナー側の生成失敗の記録 */
export function failAiJob({ jobId, leaseToken, error }) {
  const db = getDB();
  const r = db.prepare(`UPDATE ai_jobs SET status = 'failed', lease_token = NULL, lease_until = NULL,
      completed_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE id = ? AND status = 'processing' AND lease_token = ?`).run(Number(jobId), String(leaseToken || ''));
  if (r.changes) {
    const job = getDB().prepare('SELECT inquiry_id FROM ai_jobs WHERE id = ?').get(Number(jobId));
    getDB().prepare('INSERT INTO sync_errors (inquiry_id, error_type, error_detail) VALUES (?, ?, ?)')
      .run(job.inquiry_id, 'ai_failed', String(error || '').slice(0, 500));
  }
  return r.changes > 0;
}

/** バッチ実行ログ (§9.2 ai_runs) */
export function logAiRun({ runnerInfo, startedAt, finishedAt, claimed, done, failed, discarded, error }) {
  getDB().prepare(`INSERT INTO ai_runs (runner_info, started_at, finished_at, claimed, done, failed, discarded, error)
    VALUES (?,?,?,?,?,?,?,?)`)
    .run(String(runnerInfo || '').slice(0, 200), startedAt || null, finishedAt || null,
      Number(claimed) || 0, Number(done) || 0, Number(failed) || 0, Number(discarded) || 0,
      error ? String(error).slice(0, 500) : null);
}
