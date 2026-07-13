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
import { getSetting, audit, jstToday, isYmd } from './ledger.js';

const nowIso = () => new Date().toISOString();
const trimS = v => String(v == null ? '' : v).trim();

// ── 既定テンプレート (既存GASの文面を踏襲) ──
export const DEFAULT_SUBJECT_TPL = '【発注書】{{date}} {{name}}';
export const DEFAULT_BODY_TPL = `{{contact}}

お世話になっております。
B-Faith株式会社の発注担当の中原です。

添付の注文よろしくお願いいたします。

希望納期：{{nouki}}

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

// 添付CSVの「発行担当者」列の既定 (既存GAS発注書の値。メール設定で変更可)
export const DEFAULT_ISSUER = '中原　大輔';

export function emailSettings() {
  return {
    mode: getSetting('email_mode') || 'dry_run',
    dryrunTo: getSetting('email_dryrun_to') || '',
    subjectTpl: getSetting('email_subject_template') || DEFAULT_SUBJECT_TPL,
    bodyTpl: getSetting('email_body_template') || DEFAULT_BODY_TPL,
    issuerName: getSetting('email_issuer_name') || DEFAULT_ISSUER,
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

/** 希望納期 'YYYY-MM-DD' → 'YYYY年M月D日' (メール本文用。実在日以外は呼び出し側で「指定なし」扱い) */
function fmtNouki(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimS(ymd));
  if (!m) return trimS(ymd);
  return `${m[1]}年${Number(m[2])}月${Number(m[3])}日`;
}

/** CSVセルの中身 (injection対策: 数式に化ける先頭文字は ' 前置)。プレビュー表示もこの値を使う (実添付と一致させる) */
function cellValue(v) {
  const s = String(v == null ? '' : v);
  return /^[=+\-@\t\r]/.test(s) ? "'" + s : s;
}
/** CSVセル (Excel互換の引用符エスケープ込み) */
function csvCell(v) {
  const s = cellValue(v);
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

  const items = db.prepare('SELECT product_code, product_key, product_name, qty, unit_cost, requested_date FROM po_order_items WHERE order_id=? ORDER BY id').all(orderId);
  if (!items.length) throw new Error('明細がありません');
  const vmap = new Map(db.prepare('SELECT product_key, vendor_code FROM po_vendor_code_map WHERE supplier_code=?')
    .all(order.supplier_code).map(r => [r.product_key, r.vendor_code]));
  const vendorColUsed = vmap.size > 0;
  const missingVendorCodes = [];
  const st = emailSettings();
  // 明細単位の希望納期 (実在日のみ採用。不正値はメールに出さない、Codex R1 Low)
  const reqOf = it => (isYmd(String(it.requested_date || '')) ? it.requested_date : null);
  const noukiColUsed = items.some(it => reqOf(it));
  const distinctDates = [...new Set(items.map(reqOf))];
  const commonNouki = noukiColUsed && distinctDates.length === 1 ? distinctDates[0] : null;

  // ── 添付CSV: 「いつもの発注書」フォーマット (既存GAS/NE発注書CSVと同一列構成。中原さん指定 2026-07-13) ──
  //   Header,発注伝票番号,発注日,仕入先名,発行担当者, ,合計金額,備考
  //    ,PO-2026-0044,2026-07-13,アメージングクラフト様,中原　大輔, ,1341960.00,
  //   --,--,--,--,--,--,--,--
  //   発注区分,商品コード,商品名,希望納期,発注単価,発注数,小計,備考(=先方管理番号。旧GASのH列追記と同じ)
  //   4列目=旧「商品option」(常に空欄だった) を希望納期に置換 (中原さん指定 2026-07-13)。未指定は空欄
  const money = v => Number(v).toFixed(2);
  const issuedJst = order.issued_at ? new Date(Date.parse(order.issued_at) + 9 * 3600000).toISOString().slice(0, 10) : jstToday();
  let totalQty = 0, totalAmount = 0, csvTotal = 0;
  const detailRows = [];
  for (const it of items) {
    totalQty += it.qty;
    if (it.unit_cost != null) { totalAmount += Math.round(it.unit_cost * it.qty); csvTotal += it.unit_cost * it.qty; }
    const vendor = vmap.get(it.product_key) || '';
    if (vendorColUsed && !vendor) missingVendorCodes.push(it.product_code);
    detailRows.push(['通常', it.product_code, it.product_name || '',
      reqOf(it) ? String(reqOf(it)).replace(/-/g, '/') : '',
      it.unit_cost == null ? '' : money(it.unit_cost), String(it.qty),
      it.unit_cost == null ? '' : money(it.unit_cost * it.qty), vendor]);
  }
  const rawRows = [
    ['Header', '発注伝票番号', '発注日', '仕入先名', '発行担当者', ' ', '合計金額', '備考'],
    [' ', order.po_number || `#${order.id}`, issuedJst, sup.name, st.issuerName, ' ', money(csvTotal), ''],
    ['--', '--', '--', '--', '--', '--', '--', '--'],
    ['発注区分', '商品コード', '商品名', '希望納期', '発注単価', '発注数', '小計', '備考'],
    ...detailRows,
  ];
  // 区切り行 (i===2) は固定文字列 (数式対策の ' を付けない)。それ以外は実添付と同じ変換を通す
  const csvText = rawRows.map((row, i) =>
    i === 2 ? '--,--,--,--,--,--,--,--' : row.map(csvCell).join(',')).join('\r\n');
  // csvRows = ポップアッププレビュー用の行列。数式対策 (' 前置) 後の値 = 実添付をExcelで開いた時と同じ中身 (Codex modal-R1 Medium)
  const csvRows = rawRows.map((row, i) => i === 2 ? row : row.map(cellValue));
  // CP932変換不能文字の検出 (黙って ? に化けるとプレビューと実添付が食い違う、Codex P15-R1 M10)
  if (iconv.decode(iconv.encode(csvText, 'cp932'), 'cp932') !== csvText) {
    const bad = [...new Set([...csvText].filter(ch => iconv.decode(iconv.encode(ch, 'cp932'), 'cp932') !== ch))].slice(0, 10);
    throw new Error(`添付CSVにShift-JISへ変換できない文字があります: ${bad.join(' ')} — 商品名等を確認してください`);
  }
  // 本文の希望納期: 全明細が同じ日付ならその日付、商品ごとに異なる/一部のみ指定なら添付CSVの列を案内、全て未指定なら「指定なし」
  const noukiText = !noukiColUsed ? '指定なし'
    : (commonNouki ? fmtNouki(commonNouki) : '商品ごとに指定しています (添付ファイルの「希望納期」列をご確認ください)');
  const data = {
    date: jstToday(), name: sup.name, contact: ensureSama(sup.contact_name), po_number: order.po_number || `#${order.id}`,
    nouki: noukiText,
  };
  let body = render(st.bodyTpl, data);
  // カスタム保存済みテンプレに {{nouki}} が無くても、希望納期の指定があれば必ず先方に伝わるよう末尾に追記
  if (noukiColUsed && !/\{\{\s*nouki\s*\}\}/.test(String(st.bodyTpl))) {
    body += `\n\n希望納期: ${noukiText}`;
  }
  return {
    order, supplier: sup, to, cc,
    subject: render(st.subjectTpl, data),
    body,
    rows: items.length, totalQty, totalAmount,
    csvText, csvRows, attachmentName: `${(order.po_number || 'PO-' + order.id)}.csv`,
    vendorColUsed, missingVendorCodes,
    mode: st.mode, dryrunTo: st.dryrunTo, envReady: st.envReady,
  };
}

/** 'YYYY-MM-DDTHH:mm' (JST入力) → UTC ISO。過去・60日超・実在しない日時 (2/30等) は拒否 */
export function parseScheduleAt(s) {
  const t = trimS(s);
  if (!t) return null;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(t)) throw new Error(`予約日時の形式が不正です: ${t}`);
  const ms = Date.parse(t + ':00+09:00');
  if (!Number.isFinite(ms)) throw new Error(`予約日時が不正です: ${t}`);
  // 実在検証: JSTへ戻して入力と往復比較 (2026-02-30 が3/2に正規化されて別時刻に送られるのを防ぐ)
  const back = new Date(ms + 9 * 3600000).toISOString().slice(0, 16);
  if (back !== t) throw new Error(`実在しない日時です: ${t}`);
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
export function createEmailJob(orderId, { resend = false, resendOfJobId = null, scheduledAt = null, expectedMode = null, actor = null } = {}) {
  const db = getDB();
  const st = emailSettings();
  // プレビュー時のモードと現在モードの一致を必須にする (プレビュー後に他所でliveへ切り替わっていても、
  // 「dry-runのつもり」の承認で本送信しない、Codex P15-R9 High)
  if (expectedMode !== st.mode) {
    throw new Error(`送信モードが変わっています (画面: ${expectedMode || '未指定'} / 現在: ${st.mode})。プレビューを開き直して内容を確認してください`);
  }
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
      let orig = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(Number(resendOfJobId));
      if (!orig) throw new Error('再送には元ジョブ (resendOfJobId) の指定が必要です');
      if (orig.order_id !== orderId) throw new Error('元ジョブが別の発注です');
      if (orig.status !== 'sent' || orig.is_dry_run) throw new Error('再送できるのは送信済み (本送信) のジョブだけです');
      // 再送の再送でも常にルート (最初の本送信) に正規化して数える (チェーンで3回上限を回避させない、Codex P15-R2 High)
      let hops = 0;
      while (orig.is_resend && orig.resend_of != null) {
        const parent = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(orig.resend_of);
        if (!parent || ++hops > 10) break;
        orig = parent;
      }
      // 上限は「実際に送った (可能性のある) 再送」で数える。取消と要求前失敗 (failed) は消費しない (Codex P15-R15 Medium)
      const n = db.prepare(`SELECT COUNT(*) AS n FROM po_email_jobs WHERE resend_of=?
        AND status IN ('queued','sending','sent','unknown')`).get(orig.id).n;
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
    if (job.attempt_count >= 5) throw new Error('再試行上限 (5回) に達しています。このジョブを「取消」し、新しく送信し直してください');
    if (job.status === 'failed') {
      // 移行DB等でdedupに漏れた併存ジョブがあっても再試行で二重送信しない (通常はインデックスが防ぐ)。
      // 再送ジョブ (is_resend=1) は元のsentジョブと同内容で併存するのが正常のため、この検査は通常ジョブのみ
      if (!job.is_resend && !job.is_dry_run) {
        const rival = db.prepare(`SELECT id, status FROM po_email_jobs
          WHERE order_id=? AND content_hash=? AND id<>? AND is_resend=0 AND is_dry_run=0
            AND status IN ('queued','sending','sent','unknown')`).get(job.order_id, job.content_hash, jobId);
        if (rival) throw new Error(`同じ内容の別ジョブ (#${rival.id}, ${rival.status}) が存在します。このジョブは取消してください`);
      }
      db.prepare("UPDATE po_email_jobs SET status='queued' WHERE id=?").run(jobId);
    }
    // generation = 送信試行の世代。照合はスナップショット時の世代と一致する場合のみ結果を適用する (競合検知)
    db.prepare("UPDATE po_email_jobs SET status='sending', sending_started_at=?, attempt_count=attempt_count+1, generation=generation+1, error=NULL WHERE id=?")
      .run(nowIso(), jobId);
    return { done: false, job: db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId) };
  }).immediate();
  if (leased.done === 'sent') return { status: 'sent', gmailMessageId: leased.job.gmail_message_id };
  if (leased.done === 'scheduled') return { status: 'scheduled', scheduledAt: leased.job.scheduled_at };

  const job = leased.job;
  // 完了の書き戻しも lease時の generation を条件にする (Codex P15-R7 High-2:
  // 長時間停止した旧世代の送信処理が復帰しても、reconcile→markUnsent→retryで進んだ新世代の状態を上書きしない)
  const gen = job.generation;
  // 状態確定と監査は同一txn (sent確定直後のクラッシュで監査だけ欠けない、Codex P15-R9 Medium)
  const finalize = db.transaction((setSql, params, action, detail) => {
    const changed = db.prepare(`UPDATE po_email_jobs ${setSql} WHERE id=? AND status='sending' AND generation=?`)
      .run(...params, jobId, gen).changes;
    if (changed === 0) {
      audit(db, { actorType: 'system', actor: null, action: 'email_stale_attempt_discarded', resource: `order:${job.order_id}`,
        detail: { jobId, gen, deliveryKey: job.delivery_key, attempted: action } });
      return false;
    }
    if (action) audit(db, { actorType: 'system', actor: null, action, resource: `order:${job.order_id}`, detail });
    return true;
  });
  // Gmail要求の直前にも世代を再検証 (Codex P15-R8 High) + 送信フェンス (Codex P15-R15 High):
  // lease から SEND_FENCE_MS を超えた試行は外部送信自体を拒否する。新世代が生まれる最短経路は
  // 「reconcileが sending を stale (10分) と判定 → unknown → markUnsent」なので、
  // SEND_FENCE_MS(5分) < STALE(10分) の不変条件により、旧プロセスが長時間停止から復帰しても
  // 新世代の開始後に旧世代が実送信することはない (check-then-act の隙を時間軸で閉じる)
  // フェンス時刻は永続化済みの sending_started_at を使う (プロセスローカル時刻はVM停止に弱い)。
  // ⚠️ 残余リスク (Codex P15-R16 で「クライアント側では厳密閉鎖不可」と確認済み):
  //   verifyFresh成功〜fetch開始の命令間でプロセスが15分以上停止し、復帰直後にfetchが飛ぶケースは
  //   原理的に閉じられない (Gmail APIに条件付き送信がないため)。markUnsent側の15分フェンスと合わせ、
  //   実務上の窓は命令レベルまで縮小済み。運用: デプロイ直後は「照合」で状態確認してから操作する
  const SEND_FENCE_MS = 5 * 60000;
  const verifyFresh = () => {
    const cur = db.prepare('SELECT status, generation, sending_started_at FROM po_email_jobs WHERE id=?').get(jobId);
    if (!cur || cur.status !== 'sending' || cur.generation !== gen) {
      throw new Error('世代交代を検知したため送信を中止しました (別の試行が進行中)');
    }
    if (!cur.sending_started_at || Date.now() - Date.parse(cur.sending_started_at) > SEND_FENCE_MS) {
      throw new Error('lease期限 (5分) を超えたため送信を中止しました。「照合」で状態を確認してください');
    }
  };
  let phase = 'pre';
  try {
    const messageId = await sendViaGmail(job, p => { phase = p; }, verifyFresh);
    const applied = finalize("SET status='sent', sent_at=?, gmail_message_id=?", [nowIso(), messageId],
      'email_sent', { jobId, gmailMessageId: messageId, dryRun: !!job.is_dry_run, deliveryKey: job.delivery_key });
    if (!applied) return { status: 'stale', error: '古い送信試行のため結果を破棄しました。「照合」で状態を確認してください' };
    return { status: 'sent', gmailMessageId: messageId };
  } catch (e) {
    const msg = String(e.message).slice(0, 500);
    if (phase === 'post') {
      // 送信要求は飛んだ後の失敗 = 届いている可能性がある
      const applied = finalize("SET status='unknown', error=?", [`送信結果不明: ${msg} — 「照合」で確認してください (自動再送はしません)`],
        'email_unknown', { jobId, error: msg, deliveryKey: job.delivery_key });
      return applied ? { status: 'unknown', error: msg } : { status: 'stale', error: '古い送信試行のため結果を破棄しました' };
    }
    const applied = finalize("SET status='failed', error=?", [msg],
      'email_failed', { jobId, gen, error: msg.slice(0, 200), deliveryKey: job.delivery_key });
    return applied ? { status: 'failed', error: msg } : { status: 'stale', error: '古い送信試行のため結果を破棄しました' };
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
  // Gmail検索 (async) はtxn外のため、結果の適用は「スナップショット時の generation と一致する場合のみ」
  // BEGIN IMMEDIATE txn 内で行う (Codex P15-R6 High)。markUnsent / 再lease は generation を進めるので、
  // 検索中に未送信宣言→再試行が始まっていたら古い照合結果は破棄される (新しい送信をsent扱いにしない)
  const applyFound = db.transaction((jobId, gen, found) => {
    const cur = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!cur) {
      audit(db, { actorType: 'system', actor: null, action: 'email_reconcile_missing_job', resource: `email_job:${jobId}`, detail: { found } });
      return '対象ジョブが存在しません (削除?)';
    }
    if (cur.status === 'sent') return 'sent (確定済み)';
    if (cur.generation !== gen || (cur.status !== 'sending' && cur.status !== 'unknown')) {
      // 終端状態 (sent/cancelled) には矛盾する再照合noteを書かない (監査ログには残す)
      db.prepare("UPDATE po_email_jobs SET error=? WHERE id=? AND status NOT IN ('sent','cancelled')")
        .run(`⚠️ 照合で送信済みメール (整理番号 ${cur.delivery_key}) を検出しましたが、状態が変化していたため未適用。再照合してください`, jobId);
      audit(db, { actorType: 'system', actor: null, action: 'email_reconcile_discarded', resource: `order:${cur.order_id}`,
        detail: { jobId, snapshotGen: gen, currentGen: cur.generation, currentStatus: cur.status, foundMessageId: found } });
      return `${cur.status} (照合中に状態変化 → 結果破棄。再照合してください)`;
    }
    if (cur.status === 'sending') db.prepare("UPDATE po_email_jobs SET status='unknown' WHERE id=?").run(jobId);
    db.prepare("UPDATE po_email_jobs SET status='sent', sent_at=?, gmail_message_id=?, error=NULL WHERE id=?").run(nowIso(), found, jobId);
    audit(db, { actorType: 'system', actor: null, action: 'email_reconciled_sent', resource: `order:${cur.order_id}`,
      detail: { jobId, gmailMessageId: found, deliveryKey: cur.delivery_key, wasStatus: cur.status } });
    return 'sent (照合で送信済みを確認)';
  });
  const applyNotFound = db.transaction((jobId, gen, msg) => {
    const cur = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!cur) return '対象ジョブが存在しません (削除?)';
    if (cur.generation !== gen || (cur.status !== 'sending' && cur.status !== 'unknown')) {
      return `${cur.status} (照合中に状態変化。そのまま)`;
    }
    if (cur.status === 'sending') db.prepare("UPDATE po_email_jobs SET status='unknown' WHERE id=?").run(jobId);
    db.prepare('UPDATE po_email_jobs SET error=? WHERE id=?').run(msg, jobId);
    return 'unknown (手動確認要)';
  });
  for (const job of targets) {
    try {
      const found = await searchGmailByMessageId(messageIdOf(job));
      if (found) {
        results.push({ jobId: job.id, result: applyFound.immediate(job.id, job.generation, found) });
      } else {
        results.push({ jobId: job.id, result: applyNotFound.immediate(job.id, job.generation,
          `照合0件 (未送信の証明にはなりません)。Gmailの送信済みで整理番号 ${job.delivery_key} を確認し、無ければ「未送信を確認した」→再試行してください`) });
      }
    } catch (e) {
      results.push({ jobId: job.id, result: applyNotFound.immediate(job.id, job.generation,
        `照合不能: ${String(e.message).slice(0, 200)} — Gmailで整理番号 ${job.delivery_key} を検索して手動確認してください`) });
    }
  }
  return { checked: targets.length, results };
}

/** 人間がGmail送信済みを確認して「未送信」を宣言した場合のみ、unknown → queued (再試行可能) に戻す。
 *  人間確認済みのため再試行カウントもリセットする (5回上限で復旧不能にならない、Codex P15-R3 Medium) */
export function markUnsent(jobId, { actor = null } = {}) {
  const db = getDB();
  const tx = db.transaction(() => {
    const job = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(jobId);
    if (!job) throw new Error(`ジョブが存在しません: ${jobId}`);
    if (job.status !== 'unknown') throw new Error('「未送信を確認した」は結果不明 (unknown) のジョブにのみ使えます');
    // 15分フェンス: 送信フェンス(5分)+fetchタイムアウト(30秒)より十分後まで待たせる
    // (停止中の旧送信プロセスが残っている間に新世代を作らせない。SEND_FENCE < STALE(10分) < ここ(15分) の順序が不変条件)
    if (job.sending_started_at && Date.now() - Date.parse(job.sending_started_at) < 15 * 60000) {
      throw new Error('送信試行から15分経過するまで「未送信を確認した」は実行できません (停止中の旧送信が残っている可能性があるため)。時間を置いて再度「照合」してください');
    }
    db.prepare("UPDATE po_email_jobs SET status='queued', attempt_count=0, generation=generation+1, error='人間確認: Gmail送信済みに存在しない → 再試行可' WHERE id=?").run(jobId);
    audit(db, { actorType: 'user', actor, action: 'email_marked_unsent', resource: `order:${job.order_id}`,
      detail: { jobId, deliveryKey: job.delivery_key, attemptReset: job.attempt_count } });
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
/**
 * 送信すべき queued ジョブを1周期分処理する:
 *  - 予約 (scheduled_at) が時刻到来したもの
 *  - 即時送信のつもりが送信前にプロセス再起動で取り残されたもの (scheduled_at NULL かつ作成から2分超。
 *    2分の猶予は、作成直後にAPI側が同期処理する正常経路との二重取り合いを避けるため、Codex P15-R14 High)
 */
export async function dispatchDueEmailJobs() {
  const now = nowIso();
  const orphanBefore = new Date(Date.now() - 2 * 60000).toISOString();
  const due = getDB().prepare(`SELECT id FROM po_email_jobs WHERE status='queued'
      AND ((scheduled_at IS NOT NULL AND scheduled_at <= ?) OR (scheduled_at IS NULL AND created_at <= ?))`)
    .all(now, orphanBefore);
  let processed = 0;
  for (const j of due) {
    try { await processEmailJob(j.id); processed++; }
    catch (e) { console.error(`[po-email] ジョブ #${j.id} の処理に失敗:`, e.message); }
  }
  return { due: due.length, processed };
}

let dispatcherStarted = false;
export function startEmailDispatcher() {
  if (dispatcherStarted) return;
  dispatcherStarted = true;
  let consecutiveErrors = 0;
  const tick = async () => {
    try {
      await dispatchDueEmailJobs();
      consecutiveErrors = 0;
    } catch (e) {
      // 失敗の握り潰しは運用検知を殺す: ログを残し、連続失敗は目立たせる
      consecutiveErrors++;
      console.error(`[po-email] ディスパッチャ失敗 (${consecutiveErrors}回連続):`, e.message);
    } finally {
      // setIntervalではなく完了後に再スケジュール (前周期の完了を待たず重複実行しない)
      const t = setTimeout(tick, 60000);
      t.unref();
    }
  };
  // 起動直後にも1回実行 (再起動で取り残された即時ジョブの回収。leaseが多重実行を防ぐ)
  const t0 = setTimeout(tick, 5000);
  t0.unref();
}

/** PO単位のジョブ履歴 */
export function listEmailJobs(orderId) {
  return getDB().prepare(`SELECT id, status, is_dry_run, scheduled_at, delivery_key, to_addr, cc_addr, subject, attempt_count,
      generation, sending_started_at, gmail_message_id, error, created_at, sent_at, is_resend, resend_of
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
async function sendViaGmail(job, onPhase = () => {}, verifyFresh = () => {}) {
  if (process.env.PO_EMAIL_FAKE) {
    verifyFresh();
    if (process.env.PO_EMAIL_FAKE === 'fail') throw new Error('偽送信エラー (テスト、要求前)');
    if (process.env.PO_EMAIL_FAKE === 'fail_unknown') { onPhase('post'); throw new Error('偽送信エラー (テスト、要求後)'); }
    onPhase('post');
    return 'fake-' + job.delivery_key;
  }
  const token = await gmailAccessToken();          // ここまでの失敗 = pre (再試行可)
  const mime = buildMime(job);                     // ヘッダ検証もここ (pre)
  const raw = Buffer.from(mime).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  verifyFresh();                                   // 外部送信の直前に世代を再検証 (旧世代なら送らない)
  onPhase('post');                                 // 以降のネットワーク断・タイムアウト = 結果不明
  const res = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw }),
    signal: AbortSignal.timeout(30000),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || !j.id) {
    // 要求後の失敗は原則 unknown。「未送信が仕様上確定する」コード (400=raw不正/401/403=認証) のみ failed に戻す
    // (408/429/5xx等は処理済みの可能性が残るため unknown、Codex P15-R10 High)
    if (res.status === 400 || res.status === 401 || res.status === 403) onPhase('pre');
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
