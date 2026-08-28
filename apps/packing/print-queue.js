/**
 * 🖨 送り状 自動印刷 — 印刷キュー (要件定義 送り状自動印刷_20260827 §6)
 *
 * miniPC が queued を積み、出荷PCの印刷エージェントが pull で取りに来る。
 * miniPC から出荷PCへは一切繋がない (出荷PCの固定IP・受信ポート開放が不要)。
 *
 *   queued ──lease──> leased ──PDFを渡す──> dispatched ──投入報告──> submitted ──> completed
 *      │                 │                     │                        │
 *      │                 └ lease期限切れ         └────── 報告が来ない ──────┴──> unknown
 *      │                   → queued に戻す (試行上限まで)
 *      └ 誰も取りに来ない → manual (人が手で刷る。自動では二度と配らない)
 *
 * 🚨 二重印刷を防ぐための一番大事な線引き = **PDFを渡した時点 (dispatched) で紙が出た可能性がある**。
 *    エージェントが「スプーラーに入れた」と報告する前に落ちても、紙はもう出ているかもしれない。
 *    そのため自動で配り直すのは **leased (PDFをまだ渡していない)** の間だけ。
 *    dispatched から先は、期限が切れても絶対に再配布せず unknown として人に知らせる
 *    (二重印刷より欠落を選ぶ。要件§6.1)。
 *
 * 🚨 滞留 (誰も取りに来ない) は queued のまま放置せず manual へ移してから人に知らせる。
 *    「手で刷ってください」と伝えた後にエージェントが復帰して同じジョブを刷ると二重になる。
 *
 * 状態遷移はすべて**1本の条件付きUPDATE (CAS)** で行い、変更行数が1でなければ失敗として扱う。
 * SELECTしてからUPDATEすると、その間に期限切れ→再lease が挟まって古い報告が新しい lease を
 * 乗っ取れる (別プロセスから同じDBを開く構成があるため実際に起こり得る)。
 */
import crypto from 'node:crypto';
import { getDB, utcNow } from './db.js';

/** lease の有効期間。切れたら別のエージェント (または再起動後の同じPC) が取り直せる */
export const LEASE_SEC = 120;
/** 同じジョブを何回まで配り直すか。超えたら failed にして人に知らせる (無限ループ防止) */
export const MAX_ATTEMPTS = 3;
/** queued のまま誰も取りに来ない = 出荷PCが寝ている/エージェント不在 → manual にして通知 */
export const STALE_QUEUED_SEC = 180;
/**
 * PDFを渡した後の「報告を受け付ける期限」= これを過ぎたら unknown。
 * ⭐lease の120秒をそのまま報告期限に使ってはいけない — 印刷に手間取っただけで
 *   正常に出た送り状の完了報告が弾かれ、「結果不明」と通知されて手動で刷られ二重になる。
 *   再配布の可否 (leased の120秒) と、報告の受付期限 (ここ) は別物として扱う。
 */
export const DISPATCHED_TIMEOUT_SEC = 300;

/** 自動で配り直してよい状態 (= まだPDFを渡していない) */
const REDISTRIBUTABLE = 'leased';
/** 人に知らせる必要がある状態。alerted_state と一致するまで繰り返し通知対象になる */
const ALERT_STATES = ['completed', 'failed', 'manual', 'unknown'];

/**
 * 引当分類 (slug) の日本語名。正本 = AI_reference ロジザード作業自動化/hikiate-patterns.csv。
 * 管理画面で「どの送り状のことか」が分かるように持つ (対応表の登録間違いを防ぐ)。
 */
export const SLUG_LABELS = Object.freeze({
  aes: 'AES (DENZOU) — 並び替え済PDF',
  nekoposu: 'ネコポス (ヤマトB2)',
  '50size': '50サイズ宅急便 (ヤマトB2・50専用アカウント)',
  '60size': '60サイズ以上宅急便 (ヤマトB2)',
  yupakepafu: 'ゆうパケットパフ (ゆうプリR)',
  teikeigai: '定形外 (汎用送り状・P-touch)',
  retapa: 'レターパック (汎用送り状・P-touch)',
});

/** 画面表示用のラベル (状態そのものを英語で見せない)。 */
export const PRINT_STATE_LABELS = {
  queued: '⏳ 印刷待ち',
  leased: '📤 出荷PCが受け取り中',
  dispatched: '📤 出荷PCへ送り状を渡した',
  submitted: '🖨 プリンターへ送信済み',
  completed: '✅ 印刷しました',
  failed: '❌ 印刷できませんでした',
  manual: '🙋 手で刷ってください (自動印刷は取り消し)',
  unknown: '❓ 結果不明 (実物を確認)',
};

const ms = (v) => { const t = Date.parse(v || ''); return Number.isFinite(t) ? t : null; };
const iso = (msVal) => new Date(msVal).toISOString().slice(0, 19) + 'Z';

/**
 * 自動印刷してよいか (= manifest 経路で特定し白紙検査も通ったか) の判定。
 * ⚠ 実際の安全境界は enqueuePrintJob の SQL 側にある (呼び出し側の確認忘れを許さないため)。
 *   ここは画面表示・説明用。
 */
export function isPrintable(reprintRow) {
  return !!reprintRow && reprintRow.pdf_printable === 1
    && reprintRow.pdf_by === 'manifest' && !!reprintRow.pdf_token;
}

/**
 * 印刷ジョブを積む。
 *
 * 🚨「manifest 経路で特定できたものだけ」という安全条件は**この関数のSQLの中**で見る。
 *   呼び出し側の if 文に任せると、将来別の入口が増えたときに位置推定のPDFが積まれ、
 *   別人の送り状が黙って印刷される。INSERT ... SELECT で元の再印刷行を直接条件にする。
 *
 * 1再印刷につき1ジョブ (UNIQUE) — 通知の再送やリトライで同じ送り状が二重に出ない。
 * @returns {{id: number, created: boolean}|null} 印刷してよい行でなければ null
 */
export function enqueuePrintJob(reprintId, { pdfSha256, slug = null }) {
  if (!Number.isInteger(reprintId) || typeof pdfSha256 !== 'string' || !/^[0-9a-f]{64}$/.test(pdfSha256)) {
    return null;
  }
  const db = getDB();
  const now = utcNow();
  // 🚨 出力先が決まっていないものは積まない。引当分類 (= 送り状発行ソフト) によって
  //    ヤマトB2 / DENZOU / ゆうプリR / 汎用送り状 と出す先が違うので、
  //    対応表に無い分類を「とりあえずどこかに刷る」と別のプリンターから他人の送り状が出る
  const printer = printerForSlug(slug);
  if (!printer) return { id: null, created: false, reason: slug ? 'no_route' : 'no_slug', slug };
  // 🚨 出力先は「名前」だけでなく「そのとき名前を持っていた端末」も焼き付ける。
  //    名前はPCごとのローカル名なので、後から別のPCが同じ名前を名乗ると、
  //    積んであったジョブが別の実機から出てしまう
  const owner = db.prepare(`SELECT p.device_id FROM pk_print_agent_printers p
    JOIN pk_pack_devices d ON d.id = p.device_id AND d.revoked_at IS NULL
    WHERE p.printer_name = ?`).get(printer);
  if (!owner) return { id: null, created: false, reason: 'no_agent', printer, slug };
  return db.transaction(() => {
    const info = db.prepare(`
      INSERT INTO pk_print_jobs (reprint_id, pdf_token, pdf_sha256, ne_slip_no, folder_name,
                                 slug, printer_name, target_device_id, state, created_at, updated_at)
      SELECT r.id, r.pdf_token, ?, r.ne_slip_no, r.folder_name, ?, ?, ?, 'queued', ?, ?
      FROM pk_pack_reprints r
      WHERE r.id = ?
        AND r.pdf_by = 'manifest'      -- 注文番号の完全一致で特定できたものだけ
        AND r.pdf_printable = 1        -- 白紙検査を通ったものだけ
        AND r.pdf_token IS NOT NULL
      ON CONFLICT(reprint_id) DO NOTHING
    `).run(pdfSha256, slug, printer, owner.device_id, now, now, reprintId);
    if (info.changes > 0) return { id: Number(info.lastInsertRowid), created: true, printer, slug };
    const row = db.prepare('SELECT id, printer_name FROM pk_print_jobs WHERE reprint_id=?').get(reprintId);
    return row ? { id: row.id, created: false, printer: row.printer_name, slug } : null;
  }).immediate();
}

/** 引当分類 (slug) → 出力先プリンター。対応表に無ければ null (= 自動印刷しない)。 */
export function printerForSlug(slug) {
  const s = normalizeSlug(slug);
  if (!s) return null;
  return getDB().prepare('SELECT printer_name FROM pk_print_routes WHERE slug=?')
    .get(s)?.printer_name ?? null;
}

/**
 * slug の表記ゆれを1か所で吸収する。
 * 🚨 読み取り側 (CSVファイル名) は小文字化しているので、登録側で 'AES' を許すと永久に
 *    一致せず、理由の見えない「印刷されない」になる。
 */
export function normalizeSlug(slug) {
  const s = String(slug || '').trim().toLowerCase();
  return s && Object.prototype.hasOwnProperty.call(SLUG_LABELS, s) ? s : null;
}

/** 対応表の全件 (管理画面)。 */
export function listPrintRoutes() {
  return getDB().prepare('SELECT * FROM pk_print_routes ORDER BY slug').all();
}

/** 対応表の登録・更新。printerName を空にすると削除 (= その分類は自動印刷しない)。 */
export function setPrintRoute(slug, printerName, actor, note = null) {
  const db = getDB();
  const s = normalizeSlug(slug);
  const p = String(printerName || '').trim();
  if (!s) return { ok: false, reason: 'bad_slug', message: '知らない引当分類です' };
  if (p.length > 120) return { ok: false, reason: 'bad_printer', message: 'プリンター名が長すぎます' };
  if (!p) { db.prepare('DELETE FROM pk_print_routes WHERE slug=?').run(s); return { ok: true, deleted: true }; }
  db.prepare(`INSERT INTO pk_print_routes (slug, printer_name, note, updated_by, updated_at)
    VALUES (?,?,?,?,?)
    ON CONFLICT(slug) DO UPDATE SET printer_name=excluded.printer_name, note=excluded.note,
      updated_by=excluded.updated_by, updated_at=excluded.updated_at`)
    .run(s, p, note ? String(note).slice(0, 200) : null, actor, utcNow());
  // その名前を登録している端末が無いと、ジョブは積まれても誰も取りに来ず滞留する。
  // 通知を待たずに設定ミスを気づけるよう、保存時に返して画面に出す
  const owner = db.prepare('SELECT device_id FROM pk_print_agent_printers WHERE printer_name=?').get(p);
  return { ok: true, orphan: !owner };
}

/**
 * 次のジョブを1件だけ原子的に lease して返す (無ければ null)。
 * 対象は queued と「lease が切れた **leased**」だけ。dispatched 以降は絶対に拾わない
 * (PDFを渡した = 紙が出たかもしれない → 配り直すと二重印刷になる)。
 *
 * @param device pk_pack_devices の行 (kind='agent'・printer_name 必須)
 */
export function reclaimExpiredLeases({ now = utcNow() } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const expired = db.prepare(
      `SELECT id, attempt_count FROM pk_print_jobs
       WHERE state=? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`
    ).all(REDISTRIBUTABLE, now);
    let requeued = 0;
    let failed = 0;
    for (const j of expired) {
      if (j.attempt_count >= MAX_ATTEMPTS) {
        failed += db.prepare(`UPDATE pk_print_jobs SET state='failed', error=?, finished_at=?,
          updated_at=?, lease_expires_at=NULL WHERE id=? AND state=?`)
          .run(`印刷エージェントが${MAX_ATTEMPTS}回とも報告しませんでした`, now, now, j.id, REDISTRIBUTABLE).changes;
      } else {
        // 再滞留も検知できるよう通知済みマークは消す (同じ事故の2回目を黙らせない)
        requeued += db.prepare(`UPDATE pk_print_jobs SET state='queued', lease_token=NULL,
          lease_expires_at=NULL, lease_device_id=NULL, alerted_state=NULL, updated_at=?
          WHERE id=? AND state=?`).run(now, j.id, REDISTRIBUTABLE).changes;
      }
    }
    return { requeued, failed };
  }).immediate();
}

export function leaseNextJob(device, { now = utcNow(), leaseSec = LEASE_SEC } = {}) {
  const db = getDB();
  const nowMs = ms(now);
  reclaimExpiredLeases({ now });
  return db.transaction(() => {
    // 🚨 その端末から出せるプリンター宛のジョブだけ渡す。出力先はジョブが持っている
    //    (引当分類ごとに決まる) ので、端末側の申告や既定プリンターでは決めない
    const printers = db.prepare('SELECT printer_name FROM pk_print_agent_printers WHERE device_id=?')
      .all(device.id).map((r) => r.printer_name);
    if (printers.length === 0) return null;
    // 積んだときの端末と、いまその名前を持っている端末の**両方**が一致したものだけ渡す。
    // 名前だけで照合すると、名前の持ち主が付け替わったときに別の実機から出る
    const job = db.prepare(`SELECT * FROM pk_print_jobs WHERE state='queued'
      AND target_device_id = ?
      AND printer_name IN (${printers.map(() => '?').join(',')}) ORDER BY id LIMIT 1`)
      .get(device.id, ...printers);
    if (!job) return null;
    const leaseToken = crypto.randomBytes(16).toString('base64url');
    const expiresAt = iso(nowMs + leaseSec * 1000);
    const upd = db.prepare(`UPDATE pk_print_jobs SET state=?, lease_device_id=?, lease_token=?,
      lease_expires_at=?, attempt_count=attempt_count+1, updated_at=?
      WHERE id=? AND state='queued'`)
      .run(REDISTRIBUTABLE, device.id, leaseToken, expiresAt, now, job.id);
    if (upd.changes !== 1) return null;   // 同時実行で他に取られた
    return {
      id: job.id,
      leaseToken,
      neSlipNo: job.ne_slip_no,
      folderName: job.folder_name,
      pdfSha256: job.pdf_sha256,
      slug: job.slug,
      // プリンター名は**サーバが引当分類から決めた**もの。エージェントはこの名前にだけ出す
      printerName: job.printer_name,
      attempt: job.attempt_count + 1,
      leaseExpiresAt: expiresAt,
    };
  }).immediate();
}

/**
 * PDFを渡してよい lease かを**読むだけ**で確かめる (状態は動かさない)。
 * PDFの実体確認 (存在・sha256) をこの後に済ませてから claim するため、検証で落ちたときに
 * 「1バイトも渡していないのに dispatched」という取りこぼしを作らない。
 */
export function findLeasedJob(jobId, { deviceId, leaseToken, now = utcNow() }) {
  if (!leaseToken) return null;   // lease token は必須 (端末認証だけでは渡さない)
  return getDB().prepare(`SELECT * FROM pk_print_jobs
    WHERE id=? AND state IN (?, 'dispatched')
      AND lease_device_id=? AND lease_token=? AND lease_expires_at > ?`)
    .get(jobId, REDISTRIBUTABLE, deviceId, leaseToken, now) || null;
}

/**
 * PDFを渡す直前に **dispatched へ進める** (CAS)。
 *
 * 🚨 ここが「自動で配り直してよい」の終わり。PDFを渡した後は、エージェントが投入報告の前に
 *    落ちても、紙はもう出ているかもしれないので二度と自動配布しない。
 *
 * 併せて lease の期限を**報告の受付期限**まで延ばす。lease の120秒は「他のエージェントに
 * 配り直してよいか」の期限であって、印刷に手間取ったエージェントの完了報告を弾くための
 * ものではない (弾くと、正常に出た送り状が「結果不明」になり手動で刷られて二重になる)。
 *
 * 同じ lease を持つエージェントの取り直し (通信断のリトライ) は許す — 別の端末には渡さない。
 */
export function claimPdfForPrint(jobId, { deviceId, leaseToken, now = utcNow() }) {
  const db = getDB();
  if (!leaseToken) return null;
  const reportDeadline = iso(ms(now) + DISPATCHED_TIMEOUT_SEC * 1000);
  return db.transaction(() => {
    const upd = db.prepare(`UPDATE pk_print_jobs SET state='dispatched', updated_at=?,
      lease_expires_at=?
      WHERE id=? AND state IN (?, 'dispatched')
        AND lease_device_id=? AND lease_token=? AND lease_expires_at > ?`)
      .run(now, reportDeadline, jobId, REDISTRIBUTABLE, deviceId, leaseToken, now);
    if (upd.changes !== 1) return null;
    return db.prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(jobId);
  }).immediate();
}

/**
 * PDFが消えている・中身が登録時と違う等で**渡せなかった**ときに、はっきり失敗にする。
 * dispatched に進める前に呼ぶこと (渡していないのに「結果不明」にしない)。
 */
export function failBeforeDispatch(jobId, { deviceId, leaseToken, error, now = utcNow() }) {
  const upd = getDB().prepare(`UPDATE pk_print_jobs SET state='failed', error=?, finished_at=?,
    updated_at=?, lease_expires_at=NULL
    WHERE id=? AND state=? AND lease_device_id=? AND lease_token=?`)
    .run(String(error || '').slice(0, 200) || '理由不明', now, now,
      jobId, REDISTRIBUTABLE, deviceId, leaseToken || '');
  return upd.changes === 1;
}

/**
 * 同じ端末・同じ lease による「もう一度同じ報告」を成功として返す (冪等)。
 * 応答が通信で失われただけの再送を 409 で弾くと、エージェントは復旧できずに紙だけが出る。
 */
function alreadyIn(jobId, state, deviceId, leaseToken) {
  const row = getDB().prepare('SELECT state, lease_device_id, lease_token FROM pk_print_jobs WHERE id=?')
    .get(jobId);
  return !!row && row.state === state && row.lease_device_id === deviceId
    && !!leaseToken && row.lease_token === leaseToken;
}

/**
 * スプーラーに投入した報告。dispatched からのみ進む (PDFを受け取っていない端末は報告できない)。
 * 報告の受付期限も延ばす — 投入から印刷完了までにさらに時間がかかることがあるため。
 */
export function markSubmitted(jobId, { deviceId, leaseToken, spoolJobId = null, now = utcNow() }) {
  const upd = getDB().prepare(`UPDATE pk_print_jobs SET state='submitted', spool_job_id=?,
    submitted_at=?, updated_at=?, lease_expires_at=?
    WHERE id=? AND state='dispatched' AND lease_device_id=? AND lease_token=? AND lease_expires_at > ?`)
    .run(spoolJobId ? String(spoolJobId).slice(0, 60) : null, now, now,
      iso(ms(now) + DISPATCHED_TIMEOUT_SEC * 1000), jobId, deviceId, leaseToken || '', now);
  if (upd.changes === 1) return { ok: true };
  if (alreadyIn(jobId, 'submitted', deviceId, leaseToken)) return { ok: true, replayed: true };
  return { ok: false, reason: 'not_leased_or_wrong_state' };
}

/**
 * 完了/失敗の報告。
 * - 成功は submitted からのみ (スプーラーに入れずに「刷れた」は受け付けない)
 * - 失敗は leased / dispatched / submitted から受け付ける
 * 失敗しても**自動では積み直さない** — 同じ理由で失敗し続けて紙を無駄にするより人に知らせる。
 */
export function markFinished(jobId, {
  deviceId, leaseToken, ok, error = null, uncertain = false, now = utcNow(),
}) {
  if (typeof ok !== 'boolean') return { ok: false, reason: 'bad_ok' };
  // 🚨「刷れなかった」と「紙が出たか分からない」を混ぜてはいけない。
  //   failed の通知は「フォルダから手動で印刷してください」と言うので、実は紙が出ていた
  //   ケースをここに流すと、現場がもう1枚刷って**二重印刷**になる。
  //   エージェントがスプーラーに渡した後に落ちた等、確信が持てない場合は uncertain=true で
  //   報告させ、unknown (=「実物を確認してください・自動では刷り直していません」) にする。
  const target = ok ? 'completed' : (uncertain ? 'unknown' : 'failed');
  const from = ok ? ["'submitted'"] : [`'${REDISTRIBUTABLE}'`, "'dispatched'", "'submitted'"];
  // lease_token は消さない — 再送を冪等に受けるための照合に要る (状態条件で遷移は止まる)
  const upd = getDB().prepare(`UPDATE pk_print_jobs SET state=?, error=?, finished_at=?, updated_at=?,
    lease_expires_at=NULL
    WHERE id=? AND state IN (${from.join(',')})
      AND lease_device_id=? AND lease_token=? AND lease_expires_at > ?`)
    .run(target, ok ? null : String(error || '').slice(0, 200) || '理由不明',
      now, now, jobId, deviceId, leaseToken || '', now);
  if (upd.changes === 1) return { ok: true, state: target };
  if (alreadyIn(jobId, target, deviceId, leaseToken)) return { ok: true, replayed: true, state: target };
  return { ok: false, reason: 'not_leased_or_wrong_state' };
}

/**
 * エージェントが復旧するときに現在の状態を確かめるための読み取り (自分が持つジョブのみ)。
 * スプーラーのジョブIDと各時刻も返す — 再起動したエージェントが「投入まで進んでいたのか」を
 * 判断できないと、再投入して二重にするか、諦めて欠落させるかの二択になる。
 */
export function getJobStatusFor(jobId, deviceId) {
  const row = getDB().prepare(`SELECT id, state, ne_slip_no, folder_name, printer_name,
    attempt_count, spool_job_id, error, created_at, updated_at, submitted_at, finished_at,
    lease_expires_at
    FROM pk_print_jobs WHERE id=? AND lease_device_id=?`).get(jobId, deviceId);
  return row || null;
}

/** エージェントの生存報告 (プリンター状態のメモつき)。 */
export function recordHeartbeat(deviceId, note = null) {
  getDB().prepare('UPDATE pk_pack_devices SET heartbeat_at=?, heartbeat_note=? WHERE id=?')
    .run(utcNow(), note ? String(note).slice(0, 200) : null, deviceId);
}

/**
 * 進まなくなったジョブを安全な状態へ移す (ポーラーから呼ぶ)。**通知はここではしない** —
 * 送信が失敗しても「通知済み」が確定して永久に鳴らなくなるのを避けるため、
 * 状態の確定 (ここ) と通知 (pendingAlerts → markAlerted) を分ける。
 */
export function sweepPrintJobs({ now = utcNow() } = {}) {
  const db = getDB();
  const nowMs = ms(now);
  // 🚨 期限切れ lease の回収を lease 取得のついでにしか行わないと、エージェントが全滅した後
  //    誰も /print/next を叩かず、leased のまま永久に残って通知も出ない (= 気づかない欠落)。
  //    ポーラーから必ず走らせる
  const reclaimed = reclaimExpiredLeases({ now });
  return db.transaction(() => {
    // ① queued のまま誰も取りに来ない = 出荷PCが寝ている / エージェント不在。
    //    🚨 queued のまま「手で刷って」と伝えると、復帰したエージェントが後から刷って二重になる。
    //       自動配布の対象から外してから知らせる
    // ⭐起算点は created_at ではなく updated_at (= queued になった時刻)。
    //   created_at で見ると、少し古いジョブは lease が切れて queued に戻った瞬間に
    //   同じ周回で manual にされ、試行上限に達する前に自動印刷を諦めてしまう
    const staleBefore = iso(nowMs - STALE_QUEUED_SEC * 1000);
    const stale = db.prepare("SELECT id FROM pk_print_jobs WHERE state='queued' AND updated_at <= ?")
      .all(staleBefore);
    for (const j of stale) {
      db.prepare(`UPDATE pk_print_jobs SET state='manual', finished_at=?, updated_at=?
        WHERE id=? AND state='queued'`).run(now, now, j.id);
    }
    // ② PDFを渡したのに報告が来ない → 結果不明。**自動再投入はしない** (二重印刷を避ける)。
    //    境界は claim/投入報告で延ばした lease_expires_at (= 報告の受付期限) と同じにする。
    //    別の値で見ると「報告は弾かれるのにまだ unknown でもない」死角ができる
    const stuck = db.prepare(`SELECT id FROM pk_print_jobs
      WHERE state IN ('dispatched','submitted') AND lease_expires_at <= ?`).all(now);
    for (const j of stuck) {
      db.prepare(`UPDATE pk_print_jobs SET state='unknown', finished_at=?, updated_at=?
        WHERE id=? AND state IN ('dispatched','submitted')`).run(now, now, j.id);
    }
    return { manual: stale.length, unknown: stuck.length, ...reclaimed };
  }).immediate();
}

/**
 * まだ人に伝えられていない結果を返す。送信に成功したら markAlerted を呼ぶこと。
 * 送信前に「通知済み」にしないので、webhook が落ちていた分は次の周回で再送される。
 */
export function pendingAlerts(limit = 20) {
  return getDB().prepare(`SELECT * FROM pk_print_jobs
    WHERE state IN (${ALERT_STATES.map(() => '?').join(',')})
      AND (alerted_state IS NULL OR alerted_state <> state)
    ORDER BY id LIMIT ?`).all(...ALERT_STATES, limit);
}

/** 通知を送れたことを記録する (同じ状態では二度鳴らさない)。 */
export function markAlerted(jobId, state) {
  getDB().prepare('UPDATE pk_print_jobs SET alerted_state=? WHERE id=? AND state=?')
    .run(state, jobId, state);
}

/** 状態に応じた通知文 (読み手ファースト — 何が起きて何をすればよいかだけ書く)。 */
export function alertTextFor(job) {
  const slip = job.ne_slip_no;
  const printer = job.printer_name || 'プリンター';
  switch (job.state) {
    case 'completed':
      return `🖨 ${slip} の送り状を印刷しました (${printer})`;
    case 'failed':
      return `⚠ ${slip} の送り状を印刷できませんでした (${String(job.error || '').slice(0, 80)})`
        + ' — フォルダから手動で印刷してください';
    case 'manual':
      return `⏳ ${slip} の送り状が印刷待ちのまま進みませんでした (出荷PCが寝ている / 印刷エージェントが動いていない可能性)。`
        + '**自動印刷は取り消したので二重には出ません** — フォルダから手動で印刷してください';
    case 'unknown':
      return `❓ ${slip} の送り状の印刷結果が不明です (${printer})。`
        + '二重印刷を避けるため自動では刷り直していません — 実物を確認してください';
    default:
      return null;
  }
}

/** 管理画面・デバッグ用。 */
export function listPrintJobs(limit = 50) {
  return getDB().prepare(`SELECT j.*, d.label AS device_label
    FROM pk_print_jobs j LEFT JOIN pk_pack_devices d ON d.id = j.lease_device_id
    ORDER BY j.id DESC LIMIT ?`).all(limit);
}
