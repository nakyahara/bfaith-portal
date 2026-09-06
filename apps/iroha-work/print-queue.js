/**
 * 🏷 保管箱ラベル 印刷キュー — いろは在庫化作業アプリ (iPad) → いろはPC Brother QL-800
 *
 * 中原さん 2026-09-06「この画面からブラザーのラベルプリンタ QL-800 から保管箱に貼るラベルを印字したい。
 * 『バーコード印字.lbx』のレイアウトで。入荷受付アプリでやった内容と全く同じだから参考にして」。
 *
 * 入荷受付チェックの値札印刷キュー (apps/inbound-check/print-queue.js, PR #1198) の移植。**設計・状態遷移・二重印刷の防ぎ方は同じ**:
 *
 *   queued ──lease (= ジョブJSONを渡す)──> leased ──投入報告──> submitted ──完了報告──> completed
 *      │                                     │                       │
 *      │                                     └──── 報告が来ない ─────┴──> unknown (実物を確認)
 *      │                                     └ 刷る前に失敗 (テンプレ無し等) ──> failed (もう一度押してよい)
 *      └ 誰も取りに来ない ──> manual (印刷係が動いていない。手で刷る)
 *
 * 🚨 PDF が無く、lease した時点でいろはPCにデータを渡している = 紙が出た可能性がある。したがって lease の期限が
 *    切れても**絶対に配り直さない** (queued へ戻さない)。二重印刷 (同じ商品のラベルが 2 枚出て別の箱に貼られる) より
 *    欠落を選び、人に実物を見てもらう。状態遷移はすべて 1 本の条件付き UPDATE (CAS)。
 *
 * 値札との違い:
 *   - 単位はカード (f_iroha_tasks) — 商品名・商品コード・バーコードは**カードの行から取る** (画面の値を信じない)
 *   - ラベルには「1 箱に何個」(packQty) と「期限」(expiry) を人が決めて載せる (作業仕様マスタの入数・入荷の有効期限が既定)
 *   - 枚数の既定 = 必要保管箱の数 (画面側 boxes_calc.boxes)
 *   - 通知は GCHAT_WEBHOOK_IROHA (notify.js) — 資材不足の連絡と同じ宛先
 *
 * エージェント側の契約 (JSON の形・状態コード) は 共有ドライブ『入荷バーコード発行\箱ラベル印刷係\README.md』が正。
 * ここを変えるときは agent.ps1 も直す。
 */
import crypto from 'crypto';
import { getDB } from './db.js';
import { getTask } from './tasks-db.js';

const utcNow = () => new Date().toISOString();
const ms = (v) => { const t = Date.parse(v || ''); return Number.isFinite(t) ? t : null; };
const iso = (msVal) => new Date(msVal).toISOString();

/** 1 ジョブの枚数の上限。エージェント側 (maxCopies 既定 50) と揃える — 入力ミスで 500 枚出ない */
export const MAX_COPIES = 50;
/** 期限の文字の上限 (ラベルの枠に収まる程度) */
export const MAX_EXPIRY_LEN = 40;
/** lease してから「報告を受け付ける期限」。切れたら unknown (自動では二度と配らない) */
export const REPORT_DEADLINE_SEC = 300;
/** queued のまま誰も取りに来ない = いろはPCが寝ている / エージェント不在 → manual にして人に知らせる */
export const STALE_QUEUED_SEC = 180;
/** エージェントの heartbeat (45 秒間隔) がこれ以内なら「オンライン」 */
export const AGENT_ONLINE_MS = 10 * 60 * 1000;
/** heartbeat の応答に載せる lease 秒 (エージェントは参考値として受け取るだけ) */
export const LEASE_SEC = 120;

export const PRINT_STATES = ['queued', 'leased', 'submitted', 'completed', 'failed', 'manual', 'unknown'];
/** まだ終わっていない = 同じカードに新しいジョブを積ませない状態 */
export const ACTIVE_STATES = ['queued', 'leased', 'submitted'];
/** 人に知らせる必要がある状態。alerted_state と一致するまで通知対象 */
const ALERT_STATES = ['completed', 'failed', 'manual', 'unknown'];

/** 画面表示用のラベル (状態そのものを英語で見せない)。iPad と管理画面で共用 */
export const PRINT_STATE_LABELS = Object.freeze({
  queued: '⏳ 印刷待ち',
  leased: '🖨 いろはPCが印刷中',
  submitted: '🖨 プリンターへ送信済み',
  completed: '✅ 印刷しました',
  failed: '⚠ 印刷できませんでした',
  manual: '🙋 印刷係が応答しません (自動印刷は取り消し)',
  unknown: '❓ 結果不明 (実物を確認)',
});

/**
 * バーコードの種別 (値札と同じ規則): 数字だけ = JAN、英字を含む英数字 = Amazon の FNSKU。
 * 箱ラベルのテンプレは CODE128 が 1 本なので、どちらも同じオブジェクトに入る (エージェントの config で両方を同じ名前に)
 */
export function barcodeTypeOf(barcode) {
  const s = String(barcode == null ? '' : barcode).trim();
  if (!s) return null;
  if (/^[0-9]+$/.test(s)) return 'jan';
  if (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s)) return 'fnsku';
  return null;
}

// ───────────────────────── 印刷エージェント (いろはPC) ─────────────────────────

/** 有効な印刷エージェント端末の一覧 (iPad の出力先選択・管理画面用) */
export function listPrintAgents({ now = utcNow() } = {}) {
  const rows = getDB().prepare(`SELECT id, label, printer_name, heartbeat_at, heartbeat_note, heartbeat_json, created_at, last_seen_at
    FROM f_iroha_app_devices WHERE kind = 'agent' AND revoked_at IS NULL ORDER BY id`).all();
  const nowMs = ms(now);
  return rows.map(r => {
    let hb = null;
    try { hb = r.heartbeat_json ? JSON.parse(r.heartbeat_json) : null; } catch { hb = null; }
    const hbMs = ms(r.heartbeat_at);
    return {
      id: r.id, label: r.label, printer_name: r.printer_name,
      heartbeat_at: r.heartbeat_at, heartbeat_note: r.heartbeat_note,
      online: hbMs != null && nowMs - hbMs <= AGENT_ONLINE_MS,
      bpac: hb ? hb.bpac !== false : null,
      paper_ok: hb && hb.paperFormatOk != null ? !!hb.paperFormatOk : null,
      version: hb ? hb.version || null : null,
      host: hb ? hb.host || null : null,
    };
  });
}

/**
 * ジョブの出力先を決める。targetDeviceId を指定すればその端末、未指定なら**有効なエージェントが 1 台だけのときに限り**それ。
 * 2 台以上あるときは iPad に選ばせる (勝手に選ぶと別の実機から出る)
 */
export function resolvePrintTarget(targetDeviceId = null) {
  const agents = listPrintAgents();
  if (targetDeviceId != null) {
    const a = agents.find(x => x.id === Number(targetDeviceId));
    if (!a) return { ok: false, error: 'no_agent', message: '指定された印刷係 (いろはPC) は登録されていません (解除された可能性があります)' };
    if (!a.printer_name) return { ok: false, error: 'no_printer', message: 'この印刷係には出力先プリンターが登録されていません' };
    return { ok: true, agent: a };
  }
  const usable = agents.filter(a => a.printer_name);
  if (usable.length === 0) return { ok: false, error: 'no_agent', message: 'ラベルを印刷する いろはPC が登録されていません (管理画面で印刷係を登録してください)' };
  if (usable.length > 1) return { ok: false, error: 'target_required', message: '印刷する PC を選んでください', agents: usable };
  return { ok: true, agent: usable[0] };
}

/** エージェントの生存報告。note は 200 字、詳細 (版・b-PAC・用紙) は JSON で 1000 字まで */
export function recordHeartbeat(deviceId, { note = null, version = null, bpac = null, host = null, paperFormat = null, paperFormatOk = null, printerReports = null } = {}) {
  const detail = {
    version: version == null ? null : String(version).slice(0, 40),
    bpac: typeof bpac === 'boolean' ? bpac : null,
    host: host == null ? null : String(host).slice(0, 60),
    paperFormat: paperFormat == null ? null : String(paperFormat).slice(0, 60),
    paperFormatOk: typeof paperFormatOk === 'boolean' ? paperFormatOk : null,
    printerReports: printerReports == null ? null : String(printerReports).slice(0, 120),
  };
  getDB().prepare('UPDATE f_iroha_app_devices SET heartbeat_at = ?, heartbeat_note = ?, heartbeat_json = ? WHERE id = ?')
    .run(utcNow(), note == null ? null : String(note).slice(0, 200), JSON.stringify(detail).slice(0, 1000), deviceId);
}

// ───────────────────────── 積む (iPad) ─────────────────────────

/**
 * 印刷ジョブを積む。
 *
 * @param {object} p
 *   taskId            … カード (商品名・商品コード・バーコードはカードの行から取る。画面の値を信じない)。終了したカードには積まない
 *   copies            … 枚数 (1..MAX_COPIES)
 *   packQty           … 1 箱に何個 (null/'' = 空欄で刷る)。整数以外は拒否
 *   extraPackQty      … 端数の箱の数 (null/'' = 端数なし)。あれば copies 枚のあとに **1 枚だけ**この数で刷る
 *                       (必要保管箱 6 箱 = 70×5＋10 → 70 個のラベル 5 枚 + 10 個のラベル 1 枚。中原さん 2026-09-06)
 *   expiry            … 期限の文字 (null/'' = 空欄で刷る)。MAX_EXPIRY_LEN 字まで・改行不可
 *   targetDeviceId    … 出力先エージェント (省略時は resolvePrintTarget の規則)
 *   clientRequestId   … 冪等 ID。同じ ID の再送は同じジョブを返す (二重タップ・応答消失で 2 枚出ない)
 *   acknowledgeUnknownJobId … 🚨直前のジョブが unknown (実物を確認) のときは必須。そのジョブ ID を「実物を見て、出ていなかった」の証跡として受け取る
 *   requestedBy / requestedDevice … 記録用
 * @returns {{ok:true, job, created:boolean}|{ok:false, error, message, job?}}
 */
export function enqueuePrintJob({ taskId, copies, packQty = null, extraPackQty = null, expiry = null, targetDeviceId = null, clientRequestId, acknowledgeUnknownJobId = null, requestedBy = null, requestedDevice = null }) {
  const db = getDB();
  const crid = String(clientRequestId || '').trim();
  if (!/^[A-Za-z0-9._:-]{8,80}$/.test(crid)) return { ok: false, error: 'bad_request', message: 'client_request_id が必要です' };
  const n = Number(copies);
  if (!Number.isSafeInteger(n) || n < 1 || n > MAX_COPIES) {
    return { ok: false, error: 'bad_copies', message: `枚数は 1〜${MAX_COPIES} で入力してください` };
  }
  let pack = '';
  if (packQty != null && String(packQty).trim() !== '') {
    const q = Number(packQty);
    if (!Number.isSafeInteger(q) || q < 1) return { ok: false, error: 'bad_pack_qty', message: '1 箱に入れる数は 1 以上の整数で入力してください (空欄なら印字しません)' };
    pack = String(q);
  }
  let extra = '';
  if (extraPackQty != null && String(extraPackQty).trim() !== '') {
    const q = Number(extraPackQty);
    if (!Number.isSafeInteger(q) || q < 1) return { ok: false, error: 'bad_extra_qty', message: '端数の箱の数は 1 以上の整数で入力してください (空欄なら端数のラベルは出しません)' };
    extra = String(q);
  }
  const exp = expiry == null ? '' : String(expiry).trim();
  if (exp.length > MAX_EXPIRY_LEN || /[\r\n]/.test(exp)) return { ok: false, error: 'bad_expiry', message: `期限は ${MAX_EXPIRY_LEN} 字まで (改行なし) で入力してください` };
  const tid = Number(taskId);
  if (!Number.isSafeInteger(tid) || tid < 1) return { ok: false, error: 'bad_request', message: 'カードの指定が不正です' };

  return db.transaction(() => {
    // 同じ冪等 ID の再送は「もう積んである」を成功として返す
    const dup = db.prepare('SELECT * FROM f_iroha_print_jobs WHERE client_request_id = ?').get(crid);
    if (dup) {
      // 同じ ID で中身が違う = 画面の不具合か ID の衝突。別のカードのジョブを「積めた」と返さない (Codex PR #1220 R1 中)
      const same = dup.task_id === tid && dup.copies === n && dup.pack_qty === pack && dup.extra_pack_qty === extra && dup.expiry_text === exp
        && (targetDeviceId == null || dup.target_device_id === Number(targetDeviceId));
      if (!same) return { ok: false, error: 'idempotency_conflict', message: '同じ依頼 ID で違う内容が送られました。画面を更新してもう一度発行してください', job: publicJob(dup) };
      return { ok: true, job: publicJob(dup), created: false, replayed: true };
    }

    const task = getTask(tid);
    if (!task) return { ok: false, error: 'not_found', message: 'このカードは見つかりません (一覧を更新してください)' };
    if (task.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードのラベルは出せません' };
    const barcode = String(task.barcode == null ? '' : task.barcode).trim();
    const type = barcodeTypeOf(barcode);
    if (!type) {
      return { ok: false, error: 'bad_barcode', message: barcode ? `バーコード「${barcode}」は JAN (数字のみ) でも FNSKU (英数字) でもないため印刷できません` : 'この商品はバーコードが登録されていないため印刷できません' };
    }
    const name = String(task.product_name == null ? '' : task.product_name).trim();
    if (!name) return { ok: false, error: 'bad_request', message: '商品名が空のため印刷できません' };

    // 同じカードのジョブがまだ終わっていなければ積まない (連打で 2 枚出ない)。終わっていれば新しいジョブを積める = 人が判断して押し直す
    const last = db.prepare('SELECT * FROM f_iroha_print_jobs WHERE task_id = ? ORDER BY id DESC LIMIT 1').get(tid);
    if (last && ACTIVE_STATES.includes(last.state)) {
      return { ok: false, error: 'in_progress', message: 'このカードのラベルは印刷中です (結果が出るまでお待ちください)', job: publicJob(last) };
    }
    // 🚨 直前が「❓ 結果不明」または「🙋 印刷係が応答しない (手で刷る扱い)」なら、そのジョブ ID を
    //    「実物を見て出ていなかった / まだ手で刷っていない」の証跡として受け取ってからしか積まない。
    //    manual を素通しすると、職員が P-touch Editor で手で刷るのと利用者の押し直しが競合して 2 枚になる (Codex PR #1220 R1 重要)
    let acked = null;
    if (last && (last.state === 'unknown' || last.state === 'manual')) {
      if (Number(acknowledgeUnknownJobId) !== last.id) {
        return last.state === 'unknown'
          ? { ok: false, error: 'confirm_unknown', message: '前回の印刷結果が不明です。QL-800 の実物を確認し、ラベルが出ていない場合だけ「実物を確認した」にチェックしてもう一度発行してください', job: publicJob(last) }
          : { ok: false, error: 'confirm_manual', message: '前回は印刷係が応答せず、手で刷る扱いになっています。まだ手で刷っていない (P-touch Editor で出していない) ことを確かめて、「手で刷っていない」にチェックしてもう一度発行してください', job: publicJob(last) };
      }
      acked = last.id;
    } else if (acknowledgeUnknownJobId != null) {
      // 🚨 証跡を付けて送ってきたのに、直前のジョブがもう unknown ではない (送る直前に遅延報告で completed になった / 画面が古い)
      return { ok: false, error: 'state_changed', message: last && last.state === 'completed'
        ? '前回のジョブは「✅ 印刷しました」に変わりました (遅れて結果が届きました)。そのラベルを使ってください'
        : '前回のジョブの状態が変わりました。画面を更新して確認してください', job: publicJob(last) };
    }

    const target = resolvePrintTarget(targetDeviceId);
    if (!target.ok) return target;
    const now = utcNow();
    if (acked != null) {
      // 🚨 人が「出ていない / 手で刷っていない」と確認して再発行する = 旧ジョブの結果はここで確定。
      //    以後、旧 lease の遅延報告を受け付けない (2 枚出る)。同じトランザクションで CAS (状態が動いていたらやり直し)
      const note = last.state === 'unknown' ? ' / 実物を確認して再発行 (出ていない)' : ' / 手で刷っていないことを確認して再発行';
      // ⭐同じ CAS で旧ジョブを「通知済み」にする — webhook の遅延・失敗で旧ジョブの「手で刷ってください」が
      //   再発行のあとに職員へ届くと、手刷り + 自動印刷で 2 枚になる (Codex PR #1220 R2 重要)
      const fix = db.prepare(`UPDATE f_iroha_print_jobs SET lease_token = NULL, acknowledged_at = ?, updated_at = ?, alerted_state = state,
        error = COALESCE(error, '') || ? WHERE id = ? AND state IN ('unknown', 'manual') AND acknowledged_at IS NULL`)
        .run(now, now, note, acked);
      if (fix.changes !== 1) {
        const cur = db.prepare('SELECT * FROM f_iroha_print_jobs WHERE id = ?').get(acked);
        return { ok: false, error: 'confirm_unknown', message: '前回のジョブの状態が変わりました。画面を更新してもう一度確認してください', job: publicJob(cur) };
      }
    }
    const info = db.prepare(`INSERT INTO f_iroha_print_jobs
      (client_request_id, task_id, product_code, product_name, barcode, barcode_type, pack_qty, extra_pack_qty, expiry_text, copies,
       printer_name, target_device_id, requested_by, requested_device, acknowledged_job_id, state, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`)
      .run(crid, tid, task.product_code || null, name, barcode, type, pack, extra, exp, n,
        target.agent.printer_name, target.agent.id, requestedBy, requestedDevice, acked, now, now);
    const job = db.prepare('SELECT * FROM f_iroha_print_jobs WHERE id = ?').get(Number(info.lastInsertRowid));
    return { ok: true, job: publicJob(job), created: true };
  }).immediate();
}

/** 実際に出る枚数 = 満杯の箱の枚数 + 端数の箱 (あれば 1 枚) */
export function totalCopiesOf(row) {
  return Number(row.copies || 0) + (String(row.extra_pack_qty || '').trim() ? 1 : 0);
}

/** iPad / 管理画面に返す形 (lease token は絶対に含めない) */
export function publicJob(row) {
  if (!row) return null;
  return {
    id: row.id, state: row.state, label: PRINT_STATE_LABELS[row.state] || row.state,
    task_id: row.task_id, product_code: row.product_code, product_name: row.product_name,
    barcode: row.barcode, barcode_type: row.barcode_type, pack_qty: row.pack_qty,
    extra_pack_qty: row.extra_pack_qty || '', total_copies: totalCopiesOf(row),
    expiry_text: row.expiry_text, copies: row.copies,
    printer_name: row.printer_name, target_device_id: row.target_device_id,
    requested_by: row.requested_by, error: row.error, acknowledged_job_id: row.acknowledged_job_id ?? null,
    acknowledged_at: row.acknowledged_at ?? null,
    created_at: row.created_at, updated_at: row.updated_at, submitted_at: row.submitted_at, finished_at: row.finished_at,
  };
}

/** カードごとの最新ジョブ (task_id → publicJob)。/api/state でカードに付ける */
export function latestJobsByTask() {
  const rows = getDB().prepare(`SELECT j.* FROM f_iroha_print_jobs j
    JOIN (SELECT task_id, MAX(id) AS id FROM f_iroha_print_jobs GROUP BY task_id) m ON m.id = j.id`).all();
  const map = new Map();
  for (const r of rows) map.set(r.task_id, publicJob(r));
  return map;
}

// ───────────────────────── エージェント側 (pull) ─────────────────────────

/**
 * 次のジョブを 1 件だけ原子的に lease して返す (無ければ null)。
 * その端末宛て (target_device_id) の queued だけ。**leased 以降は絶対に拾わない** (ジョブを渡した = 紙が出たかもしれない)。
 * 返す JSON の形はエージェント (agent.ps1 Test-JobData / New-LabelFields) が前提にしているもの + expiry
 */
export function leaseNextJob(device, { now = utcNow() } = {}) {
  const db = getDB();
  if (!device || device.kind !== 'agent' || !device.printer_name) return null;
  return db.transaction(() => {
    const job = db.prepare(`SELECT * FROM f_iroha_print_jobs WHERE state = 'queued' AND target_device_id = ?
      ORDER BY id LIMIT 1`).get(device.id);
    if (!job) return null;
    // 積んだときのプリンター名と、いま端末が持つ名前が違えば渡さない (名前を付け替えた後の古いジョブを別のプリンターから出さない)
    if (job.printer_name !== device.printer_name) {
      db.prepare(`UPDATE f_iroha_print_jobs SET state = 'manual', error = ?, finished_at = ?, updated_at = ? WHERE id = ? AND state = 'queued'`)
        .run(`出力先プリンター名が変わったため自動印刷を取り消しました (${job.printer_name} → ${device.printer_name})`, now, now, job.id);
      return null;
    }
    const leaseToken = crypto.randomBytes(16).toString('base64url');
    const deadline = iso(ms(now) + REPORT_DEADLINE_SEC * 1000);
    const upd = db.prepare(`UPDATE f_iroha_print_jobs SET state = 'leased', lease_device_id = ?, lease_token = ?,
      lease_expires_at = ?, leased_at = ?, updated_at = ? WHERE id = ? AND state = 'queued'`)
      .run(device.id, leaseToken, deadline, now, now, job.id);
    if (upd.changes !== 1) return null;   // 同時実行で他に取られた
    return {
      id: job.id,
      leaseToken,
      printerName: job.printer_name,
      productCode: job.product_code || '',
      productName: job.product_name,
      barcode: job.barcode,
      barcodeType: job.barcode_type,
      packQty: job.pack_qty == null ? '' : String(job.pack_qty),
      // 端数の箱: copies 枚のあとに 1 枚だけこの数で刷る (空なら刷らない)
      extraPackQty: job.extra_pack_qty == null ? '' : String(job.extra_pack_qty),
      expiry: job.expiry_text == null ? '' : String(job.expiry_text),
      copies: job.copies,
      taskId: job.task_id,
      requestedBy: job.requested_by || '',
      leaseExpiresAt: deadline,
    };
  }).immediate();
}

/** 同じ端末・同じ lease による「もう一度同じ報告」を成功として返す (冪等) */
function alreadyIn(jobId, state, deviceId, leaseToken) {
  const row = getDB().prepare('SELECT state, lease_device_id, lease_token FROM f_iroha_print_jobs WHERE id = ?').get(jobId);
  return !!row && row.state === state && row.lease_device_id === deviceId && !!leaseToken && row.lease_token === leaseToken;
}

export function markSubmitted(jobId, { deviceId, leaseToken, spoolJobId = null, now = utcNow() }) {
  const spool = spoolJobId == null || spoolJobId === '' ? null : String(spoolJobId).slice(0, 60);
  const db = getDB();
  return db.transaction(() => {
    const upd = db.prepare(`UPDATE f_iroha_print_jobs SET state = 'submitted', spool_job_id = ?, submitted_at = ?,
      updated_at = ?, lease_expires_at = ?
      WHERE id = ? AND state = 'leased' AND lease_device_id = ? AND lease_token = ? AND lease_expires_at > ?`)
      .run(spool, now, now, iso(ms(now) + REPORT_DEADLINE_SEC * 1000), jobId, deviceId, leaseToken || '', now);
    if (upd.changes === 1) return { ok: true };
    if (alreadyIn(jobId, 'submitted', deviceId, leaseToken)) {
      const cur = db.prepare('SELECT spool_job_id FROM f_iroha_print_jobs WHERE id = ?').get(jobId);
      if ((cur?.spool_job_id ?? null) === spool) return { ok: true, replayed: true };
      return { ok: false, reason: 'submission_conflict', message: `前回の投入報告 (spool ${cur?.spool_job_id ?? '-'}) と違う spool_job_id (${spool ?? '-'}) です` };
    }
    return { ok: false, reason: 'not_leased_or_wrong_state' };
  }).immediate();
}

/**
 * 完了/失敗の報告 (値札と同じ規則):
 *   - 成功 (ok:true) は submitted からのみ
 *   - 失敗 (ok:false) は leased / submitted から。**failed にできるのは、まだスプーラーに入れていない leased からだけ**。
 *     submitted からの失敗は uncertain の値にかかわらず unknown (紙が出ているかもしれない)
 *   - 期限切れで unknown に倒した後に同じ lease の報告が遅れて届いたときは受け付けて上書きする (unknown → completed / failed)。
 *     ただし人が実物を確認して再発行した後 (acknowledged_at = lease_token NULL) は旧 lease の報告を受け付けない
 */
export function markFinished(jobId, { deviceId, leaseToken, ok, error = null, uncertain = false, now = utcNow() }) {
  if (typeof ok !== 'boolean') return { ok: false, reason: 'bad_ok' };
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT state, submitted_at, lease_device_id, lease_token FROM f_iroha_print_jobs WHERE id = ?').get(jobId);
    if (!row) return { ok: false, reason: 'not_found' };
    const everSubmitted = row.state === 'submitted' || !!row.submitted_at;
    const target = ok ? 'completed' : ((uncertain || everSubmitted) ? 'unknown' : 'failed');
    const from = ok ? ['submitted', 'unknown'] : ['leased', 'submitted', 'unknown'];
    const upd = db.prepare(`UPDATE f_iroha_print_jobs SET state = ?, error = ?, finished_at = ?, updated_at = ?, lease_expires_at = NULL
      WHERE id = ? AND state = ? AND state IN (${from.map(() => '?').join(',')}) AND state <> ?
        AND lease_device_id = ? AND lease_token = ?
        AND (state = 'unknown' OR lease_expires_at > ?)`)
      .run(target, ok ? null : (String(error || '').slice(0, 200) || '理由不明'), now, now,
        jobId, row.state, ...from, target, deviceId, leaseToken || '', now);
    if (upd.changes === 1) return { ok: true, state: target };
    if (alreadyIn(jobId, target, deviceId, leaseToken)) return { ok: true, replayed: true, state: target };
    return { ok: false, reason: 'not_leased_or_wrong_state' };
  }).immediate();
}

/** エージェントが再起動したとき、掴んでいたジョブの現在の状態を確かめる (自分が lease したジョブのみ) */
export function getJobStatusFor(jobId, deviceId) {
  const row = getDB().prepare(`SELECT id, state, product_code, product_name, printer_name, copies, spool_job_id, error,
    created_at, updated_at, leased_at, submitted_at, finished_at, lease_expires_at
    FROM f_iroha_print_jobs WHERE id = ? AND lease_device_id = ?`).get(jobId, deviceId);
  return row || null;
}

// ───────────────────────── 見張り (常駐ワーカー) ─────────────────────────

/**
 * 進まなくなったジョブを安全な状態へ移す。**通知はここではしない** (状態の確定と通知を分ける)。
 *   ① queued のまま STALE_QUEUED_SEC → manual (印刷係が来ない。復帰した印刷係が後から刷って二重にならないよう先に外す)
 *   ② leased / submitted のまま報告期限を過ぎた → unknown (紙が出たかもしれない。自動では刷り直さない)
 */
export function sweepPrintJobs({ now = utcNow() } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const staleBefore = iso(ms(now) - STALE_QUEUED_SEC * 1000);
    const manual = db.prepare(`UPDATE f_iroha_print_jobs SET state = 'manual', finished_at = ?, updated_at = ?,
      error = '印刷係 (いろはPCのエージェント) が取りに来ませんでした'
      WHERE state = 'queued' AND updated_at <= ?`).run(now, now, staleBefore).changes;
    const unknown = db.prepare(`UPDATE f_iroha_print_jobs SET state = 'unknown', finished_at = ?, updated_at = ?,
      error = 'いろはPCから結果の報告が届きませんでした'
      WHERE state IN ('leased', 'submitted') AND lease_expires_at <= ?`).run(now, now, now).changes;
    return { manual, unknown };
  }).immediate();
}

/**
 * まだ人に伝えられていない結果 (送信に成功したら markAlerted を呼ぶ)。
 * 人が確認して再発行した旧ジョブ (acknowledged_at あり) は出さない — 古い「実物を確認 / 手で刷って」を今さら送らない (Codex PR #1220 R2)
 */
export function pendingAlerts(limit = 20) {
  return getDB().prepare(`SELECT * FROM f_iroha_print_jobs
    WHERE state IN (${ALERT_STATES.map(() => '?').join(',')}) AND (alerted_state IS NULL OR alerted_state <> state)
      AND acknowledged_at IS NULL
    ORDER BY id LIMIT ?`).all(...ALERT_STATES, limit);
}

export function markAlerted(jobId, state) {
  getDB().prepare('UPDATE f_iroha_print_jobs SET alerted_state = ? WHERE id = ? AND state = ?').run(state, jobId, state);
}

/** 状態に応じた通知文 (何が起きて何をすればよいかだけ) */
export function alertTextFor(job) {
  const who = `${job.product_name}${job.product_code ? ` (${job.product_code})` : ''} の保管箱ラベル ${totalCopiesOf(job)}枚`;
  const printer = job.printer_name || 'プリンター';
  switch (job.state) {
    case 'completed':
      return `🏷 ${who} を印刷しました (${printer})`;
    case 'failed':
      return `⚠ ${who} を印刷できませんでした (${String(job.error || '').slice(0, 80)}) — 紙は出ていません。iPad からもう一度「🏷 箱ラベル」を押せます`;
    case 'manual':
      return `🙋 ${who} が印刷待ちのまま進みませんでした (いろはPCが寝ている / 印刷係が動いていない可能性)。自動印刷は取り消したので二重には出ません — P-touch Editor で手で刷ってください`;
    case 'unknown':
      return `❓ ${who} の印刷結果が不明です (${printer})。二重印刷を避けるため自動では刷り直していません — QL-800 の実物を確認してください`;
    default:
      return null;
  }
}

/** 管理画面用 */
export function listPrintJobs(limit = 30) {
  return getDB().prepare(`SELECT j.*, d.label AS device_label FROM f_iroha_print_jobs j
    LEFT JOIN f_iroha_app_devices d ON d.id = j.target_device_id ORDER BY j.id DESC LIMIT ?`).all(limit);
}
