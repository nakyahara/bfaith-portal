/**
 * purchase-orders 発注書メール送信 (P15)
 *
 * 既存GAS (Drive監視→Gmail送信) の置換。要件v8 F-6:
 *  - outbox/ジョブ方式: ジョブ作成 (同期txn+dedup) と送信処理 (async) を分離。txn内でGmail APIを呼ばない
 *  - delivery_key を Message-ID ヘッダと本文 (整理番号) に埋め込み、lease切れの sending は
 *    Gmail照合 (rfc822msgid検索) で回復する。照合不能・不明時は自動再送しない
 *  - dry-run (既定): 宛先を社内アドレスへ強制変換して実送信 (件名に【DRYRUN】)。live切替は中原さん判断
 *  - 添付CSVはアプリの発注明細から生成 (Shift-JIS/CP932、Excel互換)。
 *    アメージングクラフト/ビーフリー等は対応表 (po_vendor_code_map) で先方管理番号列を付与
 *
 * env (Renderに中原さんが設定。Claudeはsecret非接触):
 *  - PO_GMAIL_CLIENT_ID / PO_GMAIL_CLIENT_SECRET / PO_GMAIL_REFRESH_TOKEN
 *    (scope: gmail.send + gmail.readonly。readonly は送信不明時の rfc822msgid 照合に使用)
 *  - PO_EMAIL_FAKE=1 でGmailを呼ばない偽送信 (smokeテスト用。'fail'=送信失敗を再現)
 */
import { createHash, randomUUID } from 'crypto';
import iconv from 'iconv-lite';
import { getDB } from './db.js';
import { getSetting, audit, jstToday } from './ledger.js';

const nowIso = () => new Date().toISOString();
const trimS = v => String(v == null ? '' : v).trim();

// ── 既定テンプレート (既存GASの文面を踏襲) ──
export const DEFAULT_SUBJECT_TPL = '【発注書】{{date}} {{name}}';
export const DEFAULT_BODY_TPL = `{{contact}}

お世話になっております。
B-Faith株式会社の発注担当の中原です。

添付の注文よろしくお願いいたします。

ご確認のほどよろしくお願いいたします。

。.。・.。゜+。。.。・.。゜+。。.。・.。゜+。。.。・.。゜+。。.。・.。*゜
B-Faith株式会社
発注担当：中原
〒564-0038 大阪府吹田市南清和園町41-36
TEL: 06-4860-7868
FAX: 06-7632-4190
Email: info＠b-faith.biz
URL: http://b-faith.biz
。.。・.。゜+。。.。・.。゜+。。.。・.。゜+。。.。・.。゜+。。.。・.。*゜`;

export function emailSettings() {
  return {
    mode: getSetting('email_mode') || 'dry_run',
    dryrunTo: getSetting('email_dryrun_to') || '',
    subjectTpl: getSetting('email_subject_template') || DEFAULT_SUBJECT_TPL,
    bodyTpl: getSetting('email_body_template') || DEFAULT_BODY_TPL,
    envReady: !!(process.env.PO_EMAIL_FAKE || (process.env.PO_GMAIL_CLIENT_ID && process.env.PO_GMAIL_CLIENT_SECRET && process.env.PO_GMAIL_REFRESH_TOKEN)),
  };
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** カンマ/セミコロン/読点区切りの宛先文字列 → 検証済み配列 */
export function parseAddresses(s) {
  const list = trimS(s).split(/[,;、]/).map(x => x.trim()).filter(Boolean);
  for (const a of list) if (!EMAIL_RE.test(a)) throw new Error(`メールアドレスが不正です: ${a}`);
  return list;
}

function ensureSama(s) {
  const t = trimS(s);
  if (!t) return 'ご担当者様';
  return /様$/.test(t) ? t : t + '様';
}

function render(tpl, data) {
  return String(tpl).replace(/\{\{\s*(\w+)\s*\}\}/g, (_, k) => (k in data ? String(data[k]) : ''));
}

/** CSVセル (Excel互換、injection対策: 数式に化ける先頭文字は ' 前置) */
function csvCell(v) {
  let s = String(v == null ? '' : v);
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

/**
 * 発注書メールのプレビューを組み立てる (送信適格チェック込み)。
 */
export function buildOrderEmail(orderId) {
  const db = getDB();
  const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(orderId);
  if (!order) throw new Error(`発注が存在しません: ${orderId}`);
  if (order.status !== 'issued') throw new Error('発注確定前のPOは送信できません');
  if (order.send_blocked) throw new Error('このPOは送信対象外です (移行PO等)');
  const boundary = getSetting('tracking_started_at');
  if (!boundary || !order.issued_at || order.issued_at < boundary) throw new Error('アプリ管理外 (legacy) のPOは送信できません');
  const sup = db.prepare('SELECT * FROM po_suppliers WHERE supplier_code=?').get(order.supplier_code);
  if (!sup) throw new Error(`仕入先マスタが未登録です: ${order.supplier_code}`);
  const to = parseAddresses(sup.email_to || '');
  if (!to.length) throw new Error(`仕入先にメールアドレスが未登録です: ${sup.name} (マスタ管理→宛先マスタ取込で登録してください)`);
  // NULL(未設定) も送信不可 (安全側)。宛先マスタ取込が send_method='email' を設定する
  if (sup.send_method !== 'email') throw new Error(`この仕入先の送信方法が email に設定されていません (現在: ${sup.send_method || '未設定'})。仕入先マスタで設定してください`);
  const cc = parseAddresses(sup.email_cc || '');

  const items = db.prepare('SELECT product_code, product_key, product_name, qty, unit_cost FROM po_order_items WHERE order_id=? ORDER BY id').all(orderId);
  if (!items.length) throw new Error('明細がありません');
  const vmap = new Map(db.prepare('SELECT product_key, vendor_code FROM po_vendor_code_map WHERE supplier_code=?')
    .all(order.supplier_code).map(r => [r.product_key, r.vendor_code]));
  const vendorColUsed = vmap.size > 0;
  const missingVendorCodes = [];
  const header = vendorColUsed ? ['商品コード', '先方管理番号', '商品名', '数量'] : ['商品コード', '商品名', '数量'];
  const lines = [header.map(csvCell).join(',')];
  let totalQty = 0, totalAmount = 0;
  for (const it of items) {
    totalQty += it.qty;
    totalAmount += it.unit_cost != null ? Math.round(it.unit_cost * it.qty) : 0;
    if (vendorColUsed) {
      const vc = vmap.get(it.product_key) || '';
      if (!vc) missingVendorCodes.push(it.product_code);
      lines.push([it.product_code, vc, it.product_name || '', it.qty].map(csvCell).join(','));
    } else {
      lines.push([it.product_code, it.product_name || '', it.qty].map(csvCell).join(','));
    }
  }
  const csvText = lines.join('\r\n');
  // CP932変換不能文字の検出 (黙って ? に化けるとプレビューと実添付が食い違う、Codex P15-R1 M10)
  if (iconv.decode(iconv.encode(csvText, 'cp932'), 'cp932') !== csvText) {
    const bad = [...new Set([...csvText].filter(ch => iconv.decode(iconv.encode(ch, 'cp932'), 'cp932') !== ch))].slice(0, 10);
    throw new Error(`添付CSVにShift-JISへ変換できない文字があります: ${bad.join(' ')} — 商品名等を確認してください`);
  }
  const st = emailSettings();
  const data = { date: jstToday(), name: sup.name, contact: ensureSama(sup.contact_name), po_number: order.po_number || `#${order.id}` };
  return {
    order, supplier: sup, to, cc,
    subject: render(st.subjectTpl, data),
    body: render(st.bodyTpl, data),
    rows: items.length, totalQty, totalAmount,
    csvText, attachmentName: `${(order.po_number || 'PO-' + order.id)}.csv`,
    vendorColUsed, missingVendorCodes,
    mode: st.mode, dryrunTo: st.dryrunTo, envReady: st.envReady,
  };
}

/** 'YYYY-MM-DDTHH:mm' (JST入力) → UTC ISO。過去・60日超は拒否 */
export function parseScheduleAt(s) {
  const t = trimS(s);
  if (!t) return null;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(t)) throw new Error(`予約日時の形式が不正です: ${t}`);
  const ms = Date.parse(t + ':00+09:00');
  if (!Number.isFinite(ms)) throw new Error(`予約日時が不正です: ${t}`);
  if (ms < Date.now() - 60000) throw new Error('予約日時が過去です。今すぐ送る場合は日時を空にしてください');
  if (ms > Date.now() + 60 * 86400000) throw new Error('予約は60日以内にしてください');
  return new Date(ms).toISOString();
}

/**
 * 送信ジョブの作成 (同期。Idempotency-Keyは呼び出し側=withCommandで包む)。
 * dedup: 同一PO+同一内容 (is_resend=0, 非dry-run) はUNIQUEで拒否。
 * 再送は resendOfJobId 必須 (同一POの sent 済み本送信ジョブのみ、上限3回、Codex P15-R1 High-3)。
 * scheduledAt (JST 'YYYY-MM-DDTHH:mm') を渡すと予約送信 (ディスパッチャが時刻到来で送信)。
 * @returns jobId
 */
export function createEmailJob(orderId, { resend = false, resendOfJobId = null, scheduledAt = null, actor = null } = {}) {
  const db = getDB();
  const st = emailSettings();
  if (!st.envReady) throw new Error('Gmail API のenv (PO_GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN) が未設定です。Renderの環境変数を設定してください');
  const p = buildOrderEmail(orderId);
  const dryRun = st.mode !== 'live';
  // live送信で先方管理番号の欠落があれば止める (仕入先側の誤処理防止、Codex P15-R1 M5)
  if (!dryRun && p.vendorColUsed && p.missingVendorCodes.length) {
    throw new Error(`先方管理番号が未登録の商品があります (${p.missingVendorCodes.slice(0, 5).join(', ')}${p.missingVendorCodes.length > 5 ? ' …' : ''})。対応表を更新してから送信してください`);
  }
  const scheduleIso = parseScheduleAt(scheduledAt);
  let to = p.to, cc = p.cc, subject = p.subject, body = p.body;
  if (dryRun) {
    if (!st.dryrunTo) throw new Error('dry-runの送信先 (email_dryrun_to) が未設定です。メール設定で社内アドレスを登録してください');
    subject = '【DRYRUN】' + subject;
    body = `※これはdry-run送信です。本来の宛先: TO=${p.to.join(', ')}${p.cc.length ? ' / CC=' + p.cc.join(', ') : ''}\n\n` + body;
    to = [st.dryrunTo]; cc = [];
  }
  const deliveryKey = 'DK' + randomUUID().replace(/-/g, '').slice(0, 20);
  const bodyWithKey = body + `\n\n整理番号: ${deliveryKey}`;
  // dedupハッシュは「本来の宛先・内容」から計算 (dry-run加工に依存しない)
  const contentHash = createHash('sha256').update([p.to.join(','), p.cc.join(','), p.subject, p.body, p.csvText].join('\x1f')).digest('hex');

  const tx = db.transaction(() => {
    let resendOf = null;
    if (resend) {
      const orig = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(Number(resendOfJobId));
      if (!orig) throw new Error('再送には元ジョブ (resendOfJobId) の指定が必要です');
      if (orig.order_id !== orderId) throw new Error('元ジョブが別の発注です');
      if (orig.status !== 'sent' || orig.is_dry_run) throw new Error('再送できるのは送信済み (本送信) のジョブだけです');
      const n = db.prepare('SELECT COUNT(*) AS n FROM po_email_jobs WHERE resend_of=?').get(orig.id).n;
      if (n >= 3) throw new Error('同じ送信への再送は3回までです');
      resendOf = orig.id;
    }
    let info;
    try {
      info = db.prepare(`INSERT INTO po_email_jobs
        (order_id, status, is_dry_run, scheduled_at, delivery_key, to_addr, cc_addr, subject, body, attachment_name, attachment_csv,
         content_hash, is_resend, resend_of, created_at, actor)
        VALUES (?,'queued',?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
        .run(orderId, dryRun ? 1 : 0, scheduleIso, deliveryKey, to.join(','), cc.join(',') || null, subject, bodyWithKey,
          p.attachmentName, p.csvText, contentHash, resend ? 1 : 0, resendOf, nowIso(), actor);
    } catch (e) {
      if (String(e.code || '').startsWith('SQLITE_CONSTRAINT')) {
        throw new Error('同じ内容の発注書は送信済み (または送信待ち) です。もう一度送る場合は「再送」を使ってください');
      }
      throw e;
    }
    const jobId = Number(info.lastInsertRowid);
    audit(db, { actorType: 'user', actor, action: dryRun ? 'email_dryrun_queued' : 'email_queued',
      resource: `order:${orderId}`,
      detail: { jobId, to, resendOf, scheduledAt: scheduleIso, deliveryKey, contentHash: contentHash.slice(0, 16) } });
    return jobId;
  });
  return tx.immediate();
}

/**
 * ジョブ1件の送信処理 (async)。lease (queued/failed→sending) をcommitしてからGmail APIを呼ぶ。
 * エラーの分類が二重送信防止の要 (Codex P15-R1 High-1):
 *  - Gmail送信要求「前」の失敗 (トークン取得等) → failed (再試行可)
 *  - Gmail送信要求「後」の失敗 → unknown (実際は届いている可能性。自動・通常再試行は不可、照合で回復)
 */
export async function processEmailJob(jobId) {
  const db = getDB();
  const leased = db.transaction(() => {
    const job = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!job) throw new Error(`ジョブが存在しません: ${jobId}`);
    if (job.status === 'sent') return { done: 'sent', job };
    if (job.status === 'sending') throw new Error('送信処理中です。時間を置いて「送信状態を照合」を実行してください');
    if (job.status === 'unknown') throw new Error('送信結果が不明なジョブです。「送信状態を照合」またはGmailの送信済みを確認してください (二重送信防止のため再試行できません)');
    if (job.status === 'cancelled') throw new Error('取消済みのジョブです');
    if (job.scheduled_at && job.scheduled_at > nowIso()) return { done: 'scheduled', job };
    if (job.attempt_count >= 5) throw new Error('再試行上限 (5回) に達しています。「再送」で新しいジョブを作成してください');
    if (job.status === 'failed') db.prepare("UPDATE po_email_jobs SET status='queued' WHERE id=?").run(jobId);
    db.prepare("UPDATE po_email_jobs SET status='sending', sending_started_at=?, attempt_count=attempt_count+1, error=NULL WHERE id=?")
      .run(nowIso(), jobId);
    return { done: false, job: db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId) };
  }).immediate();
  if (leased.done === 'sent') return { status: 'sent', gmailMessageId: leased.job.gmail_message_id };
  if (leased.done === 'scheduled') return { status: 'scheduled', scheduledAt: leased.job.scheduled_at };

  const job = leased.job;
  let phase = 'pre';
  try {
    const messageId = await sendViaGmail(job, p => { phase = p; });
    db.prepare("UPDATE po_email_jobs SET status='sent', sent_at=?, gmail_message_id=? WHERE id=?").run(nowIso(), messageId, jobId);
    audit(db, { actorType: 'system', actor: null, action: 'email_sent', resource: `order:${job.order_id}`,
      detail: { jobId, gmailMessageId: messageId, dryRun: !!job.is_dry_run, deliveryKey: job.delivery_key } });
    return { status: 'sent', gmailMessageId: messageId };
  } catch (e) {
    const msg = String(e.message).slice(0, 500);
    if (phase === 'post') {
      // 送信要求は飛んだ後の失敗 = 届いている可能性がある
      db.prepare("UPDATE po_email_jobs SET status='unknown', error=? WHERE id=?")
        .run(`送信結果不明: ${msg} — 「照合」で確認してください (自動再送はしません)`, jobId);
      audit(db, { actorType: 'system', actor: null, action: 'email_unknown', resource: `order:${job.order_id}`,
        detail: { jobId, error: msg, deliveryKey: job.delivery_key } });
      return { status: 'unknown', error: msg };
    }
    db.prepare("UPDATE po_email_jobs SET status='failed', error=? WHERE id=?").run(msg, jobId);
    return { status: 'failed', error: msg };
  }
}

/**
 * 照合 (async): lease切れ (10分) の sending と unknown をGmail検索 (rfc822msgid) で確認する。
 *  - 1通見つかった → sent / それ以外 (0件・照合不能) → unknown のまま (0件は未送信の証明にならない、
 *    Codex P15-R1 High-2)。再送したい場合は人間がGmail送信済みを確認して「未送信を確認した」を実行する
 */
export async function reconcileEmailJobs() {
  const db = getDB();
  const staleBefore = new Date(Date.now() - 10 * 60000).toISOString();
  const targets = db.prepare(`SELECT * FROM po_email_jobs
    WHERE status='unknown' OR (status='sending' AND sending_started_at < ?)`).all(staleBefore);
  const results = [];
  for (const job of targets) {
    try {
      const found = await searchGmailByMessageId(messageIdOf(job));
      if (found) {
        if (job.status === 'sending') db.prepare("UPDATE po_email_jobs SET status='unknown' WHERE id=?").run(job.id);
        db.prepare("UPDATE po_email_jobs SET status='sent', sent_at=?, gmail_message_id=?, error=NULL WHERE id=?").run(nowIso(), found, job.id);
        audit(db, { actorType: 'system', actor: null, action: 'email_reconciled_sent', resource: `order:${job.order_id}`,
          detail: { jobId: job.id, gmailMessageId: found, deliveryKey: job.delivery_key } });
        results.push({ jobId: job.id, result: 'sent (照合で送信済みを確認)' });
      } else {
        if (job.status === 'sending') db.prepare("UPDATE po_email_jobs SET status='unknown' WHERE id=?").run(job.id);
        db.prepare('UPDATE po_email_jobs SET error=? WHERE id=?')
          .run(`照合0件 (未送信の証明にはなりません)。Gmailの送信済みで整理番号 ${job.delivery_key} を確認し、無ければ「未送信を確認した」→再試行してください`, job.id);
        results.push({ jobId: job.id, result: 'unknown (0件。手動確認要)' });
      }
    } catch (e) {
      if (job.status === 'sending') db.prepare("UPDATE po_email_jobs SET status='unknown' WHERE id=?").run(job.id);
      db.prepare('UPDATE po_email_jobs SET error=? WHERE id=?')
        .run(`照合不能: ${String(e.message).slice(0, 200)} — Gmailで整理番号 ${job.delivery_key} を検索して手動確認してください`, job.id);
      results.push({ jobId: job.id, result: 'unknown (照合不能。手動確認要)' });
    }
  }
  return { checked: targets.length, results };
}

/** 人間がGmail送信済みを確認して「未送信」を宣言した場合のみ、unknown → queued (再試行可能) に戻す */
export function markUnsent(jobId, { actor = null } = {}) {
  const db = getDB();
  const tx = db.transaction(() => {
    const job = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!job) throw new Error(`ジョブが存在しません: ${jobId}`);
    if (job.status !== 'unknown') throw new Error('「未送信を確認した」は結果不明 (unknown) のジョブにのみ使えます');
    db.prepare("UPDATE po_email_jobs SET status='queued', error='人間確認: Gmail送信済みに存在しない → 再試行可' WHERE id=?").run(jobId);
    audit(db, { actorType: 'user', actor, action: 'email_marked_unsent', resource: `order:${job.order_id}`,
      detail: { jobId, deliveryKey: job.delivery_key } });
    return { status: 'queued' };
  });
  return tx.immediate();
}

/** 予約中 (queued) のジョブを取り消す */
export function cancelEmailJob(jobId, { actor = null } = {}) {
  const db = getDB();
  const tx = db.transaction(() => {
    const job = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!job) throw new Error(`ジョブが存在しません: ${jobId}`);
    if (job.status !== 'queued' && job.status !== 'failed') throw new Error('取消できるのは送信前 (queued/failed) のジョブだけです');
    db.prepare("UPDATE po_email_jobs SET status='cancelled' WHERE id=?").run(jobId);
    audit(db, { actorType: 'user', actor, action: 'email_cancelled', resource: `order:${job.order_id}`,
      detail: { jobId, scheduledAt: job.scheduled_at } });
    return { status: 'cancelled' };
  });
  return tx.immediate();
}

/**
 * 予約送信ディスパッチャ: 毎分、時刻が来た予約ジョブ (queued + scheduled_at <= now) を送信する。
 * unref() するのでプロセス終了を妨げない。多重起動しても lease が二重送信を防ぐ
 */
let dispatcherStarted = false;
export function startEmailDispatcher() {
  if (dispatcherStarted) return;
  dispatcherStarted = true;
  const timer = setInterval(async () => {
    try {
      const due = getDB().prepare("SELECT id FROM po_email_jobs WHERE status='queued' AND scheduled_at IS NOT NULL AND scheduled_at <= ?")
        .all(nowIso());
      for (const j of due) {
        try { await processEmailJob(j.id); } catch { /* leaseや検証エラーは次周期に任せる */ }
      }
    } catch { /* DB未初期化等。次周期に再試行 */ }
  }, 60000);
  timer.unref();
}

/** PO単位のジョブ履歴 */
export function listEmailJobs(orderId) {
  return getDB().prepare(`SELECT id, status, is_dry_run, scheduled_at, delivery_key, to_addr, cc_addr, subject, attempt_count,
      sending_started_at, gmail_message_id, error, created_at, sent_at, is_resend, resend_of
    FROM po_email_jobs WHERE order_id=? ORDER BY id DESC`).all(orderId);
}

// ── Gmail API (すべてasync。PO_EMAIL_FAKE で偽装可能) ──

const messageIdOf = job => `<${job.delivery_key}@po.bfaith.biz>`;

let tokenCache = { token: null, exp: 0 };
async function gmailAccessToken() {
  if (tokenCache.token && Date.now() < tokenCache.exp) return tokenCache.token;
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: process.env.PO_GMAIL_CLIENT_ID, client_secret: process.env.PO_GMAIL_CLIENT_SECRET,
      refresh_token: process.env.PO_GMAIL_REFRESH_TOKEN, grant_type: 'refresh_token',
    }),
    signal: AbortSignal.timeout(15000),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || !j.access_token) throw new Error(`Gmailトークン取得失敗 (HTTP ${res.status}): ${j.error_description || j.error || '不明'}`);
  tokenCache = { token: j.access_token, exp: Date.now() + Math.max(0, (j.expires_in || 3600) - 60) * 1000 };
  return tokenCache.token;
}

/**
 * Gmail送信。onPhase('post') を「送信要求を発した直後」に呼ぶ (以降の失敗=結果不明、二重送信防止の分類に使う)
 */
async function sendViaGmail(job, onPhase = () => {}) {
  if (process.env.PO_EMAIL_FAKE) {
    if (process.env.PO_EMAIL_FAKE === 'fail') throw new Error('偽送信エラー (テスト、要求前)');
    if (process.env.PO_EMAIL_FAKE === 'fail_unknown') { onPhase('post'); throw new Error('偽送信エラー (テスト、要求後)'); }
    onPhase('post');
    return 'fake-' + job.delivery_key;
  }
  const token = await gmailAccessToken();          // ここまでの失敗 = pre (再試行可)
  const mime = buildMime(job);                     // ヘッダ検証もここ (pre)
  const raw = Buffer.from(mime).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  onPhase('post');                                 // 以降のネットワーク断・タイムアウト = 結果不明
  const res = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw }),
    signal: AbortSignal.timeout(30000),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || !j.id) {
    // 4xxの明示拒否 = 「送信されていない」確定 → failed (再試行可)。5xxは送信済みの可能性が残る → unknown
    if (res.status < 500) onPhase('pre');
    throw new Error(`Gmail送信失敗 (HTTP ${res.status}): ${(j.error && j.error.message) || '不明'}`);
  }
  return j.id;
}

async function searchGmailByMessageId(rfcMessageId) {
  if (process.env.PO_EMAIL_FAKE) {
    if (process.env.PO_EMAIL_FAKE_SEARCH === 'error') throw new Error('偽照合エラー (テスト)');
    return process.env.PO_EMAIL_FAKE_SEARCH === 'found' ? 'fake-found' : null;
  }
  const token = await gmailAccessToken();
  const res = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages?q=' + encodeURIComponent(`rfc822msgid:${rfcMessageId}`), {
    headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(15000),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`Gmail照合失敗 (HTTP ${res.status}): ${(j.error && j.error.message) || '不明'} (scope gmail.readonly が必要)`);
  const list = j.messages || [];
  if (list.length > 1) throw new Error(`同じ整理番号のメールが${list.length}通あります。手動確認してください`);
  return list.length === 1 ? list[0].id : null;
}

/** ヘッダ値の改行・NUL拒否 (CR/LFヘッダインジェクション対策。テンプレ保存時にも検証するが最終防衛はここ) */
function assertHeaderSafe(name, v) {
  if (/[\r\n\0]/.test(String(v || ''))) throw new Error(`${name} に改行を含めることはできません`);
  return String(v || '');
}

/** 件名等の非ASCIIヘッダを RFC2047 (UTF-8/Base64) でエンコード。encoded-wordは75文字制限があるため分割してfold */
function encodeWord(s) {
  if (/^[\x20-\x7e]*$/.test(s)) return s;
  const words = [];
  let buf = '';
  for (const ch of s) {
    if (Buffer.byteLength(buf + ch, 'utf8') > 42) { words.push(buf); buf = ''; } // b64後~56文字+装飾<75
    buf += ch;
  }
  if (buf) words.push(buf);
  return words.map(w => `=?UTF-8?B?${Buffer.from(w, 'utf8').toString('base64')}?=`).join('\r\n '); // folding (継続行)
}

/** multipart/mixed MIME (本文UTF-8 + 添付CSV=CP932/Base64、ファイル名はASCII=PO番号.csv) */
export function buildMime(job) {
  const boundary = 'b_' + job.delivery_key;
  assertHeaderSafe('宛先', job.to_addr);
  assertHeaderSafe('CC', job.cc_addr);
  assertHeaderSafe('件名', job.subject);
  assertHeaderSafe('添付ファイル名', job.attachment_name);
  if (!/^[\x21-\x7e]+\.csv$/.test(job.attachment_name)) throw new Error(`添付ファイル名はASCIIの.csvのみ: ${job.attachment_name}`);
  const from = process.env.PO_MAIL_FROM && EMAIL_RE.test(process.env.PO_MAIL_FROM) ? process.env.PO_MAIL_FROM : null;
  const csvB64 = iconv.encode(job.attachment_csv, 'cp932').toString('base64').replace(/(.{76})/g, '$1\r\n');
  const bodyB64 = Buffer.from(job.body, 'utf8').toString('base64').replace(/(.{76})/g, '$1\r\n');
  const headers = [
    from ? `From: ${from}` : null,
    `To: ${job.to_addr}`,
    job.cc_addr ? `Cc: ${job.cc_addr}` : null,
    `Subject: ${encodeWord(job.subject)}`,
    `Date: ${new Date().toUTCString()}`,
    `Message-ID: ${messageIdOf(job)}`,
    'MIME-Version: 1.0',
    `Content-Type: multipart/mixed; boundary="${boundary}"`,
  ].filter(Boolean);
  return headers.join('\r\n') + '\r\n\r\n' +
    `--${boundary}\r\n` +
    'Content-Type: text/plain; charset="UTF-8"\r\nContent-Transfer-Encoding: base64\r\n\r\n' +
    bodyB64 + '\r\n' +
    `--${boundary}\r\n` +
    `Content-Type: text/csv; charset="Shift_JIS"; name="${job.attachment_name}"\r\n` +
    `Content-Disposition: attachment; filename="${job.attachment_name}"\r\n` +
    'Content-Transfer-Encoding: base64\r\n\r\n' +
    csvB64 + '\r\n' +
    `--${boundary}--\r\n`;
}
