/**
 * 🏷 値札 (BCシール) 印刷キュー — 入荷受付チェック iPad → 倉庫PC Brother QL-700
 *
 * iPad の「🏷 シール発行」でジョブを積み、倉庫PCの印刷エージェント
 * (AI_reference『システム設計\_tools\倉庫PC_値札印刷エージェント\』agent.ps1) が pull で取りに来る。
 * Render から倉庫PCへは一切繋がない (固定IP・受信ポート不要)。
 *
 *   queued ──lease (= ジョブJSONを渡す)──> leased ──投入報告──> submitted ──完了報告──> completed
 *      │                                     │                       │
 *      │                                     └──── 報告が来ない ─────┴──> unknown (実物を確認)
 *      │                                     └ 刷る前に失敗 (テンプレ無し等) ──> failed (もう一度押してよい)
 *      └ 誰も取りに来ない ──> manual (印刷係が動いていない。手で刷る)
 *
 * 🚨 送り状の印刷キュー (apps/packing/print-queue.js) との違い = **PDF が無い**。
 *    ジョブ JSON そのものがデータなので、**lease した時点で倉庫PCにデータを渡している = 紙が出た
 *    可能性がある**。したがって lease の期限が切れても**絶対に配り直さない** (queued へ戻さない)。
 *    二重印刷 (同じ商品のシールが2枚出て別の箱に貼られる) より欠落を選び、人に実物を見てもらう。
 *    刷れなかったことが確かなとき (エージェントが刷る前に落ちた / テンプレが無い) はエージェント自身が
 *    `ok:false, uncertain:false` で報告してくるので failed になり、iPad からもう一度押せる。
 *
 * 状態遷移はすべて **1本の条件付き UPDATE (CAS)** で行い、変更行数が1でなければ失敗として扱う
 * (SELECT してから UPDATE すると、期限切れ→unknown が挟まった古い報告が乗っ取れる)。
 */
import crypto from 'crypto';
import { getDB, getProductForPrint, setProductBarcode, resolveBarcode } from './db.js';

const utcNow = () => new Date().toISOString();
const ms = (v) => { const t = Date.parse(v || ''); return Number.isFinite(t) ? t : null; };
const iso = (msVal) => new Date(msVal).toISOString();

/** 1ジョブの枚数の上限。エージェント側 (maxCopies 既定 50) と揃える — 入力ミスで500枚出ない */
export const MAX_COPIES = 50;
/**
 * lease してから「報告を受け付ける期限」。切れたら unknown (自動では二度と配らない)。
 * エージェントは印刷後スプーラーを最大 120 秒追いかけてから報告するので、それより十分長く取る
 */
export const REPORT_DEADLINE_SEC = 300;
/** queued のまま誰も取りに来ない = 倉庫PCが寝ている / エージェント不在 → manual にして人に知らせる */
export const STALE_QUEUED_SEC = 180;
/** エージェントの heartbeat (45秒間隔) がこれ以内なら「オンライン」と見なす */
export const AGENT_ONLINE_MS = 10 * 60 * 1000;
/** heartbeat の応答に載せる lease 秒 (エージェントは参考値として受け取るだけ) */
export const LEASE_SEC = 120;

export const PRINT_STATES = ['queued', 'leased', 'submitted', 'completed', 'failed', 'manual', 'unknown'];
/** まだ終わっていない = 同じ明細に新しいジョブを積ませない状態 */
export const ACTIVE_STATES = ['queued', 'leased', 'submitted'];
/** 人に知らせる必要がある状態。alerted_state と一致するまで通知対象になる */
const ALERT_STATES = ['completed', 'failed', 'manual', 'unknown'];

/** 画面表示用のラベル (状態そのものを英語で見せない)。iPad と管理画面で共用 */
export const PRINT_STATE_LABELS = Object.freeze({
  queued: '⏳ 印刷待ち',
  leased: '🖨 倉庫PCが印刷中',
  submitted: '🖨 プリンターへ送信済み',
  completed: '✅ 印刷しました',
  failed: '⚠ 印刷できませんでした',
  manual: '🙋 印刷係が応答しません (自動印刷は取り消し)',
  unknown: '❓ 結果不明 (実物を確認)',
});

/**
 * バーコードの種別。値札CSV (apps/inbound-info/nefuda-print.js) の JAN/FNSKU 振り分けと同じ規則:
 * 数字だけ = JAN、英字を含む英数字 = Amazon の FNSKU。それ以外 (空・記号入り) は積まない。
 * エージェント側 (Test-JobData) も同じ検査をするが、積む前に落とすほうが iPad に理由を返せる。
 */
export function barcodeTypeOf(barcode) {
  const s = String(barcode == null ? '' : barcode).trim();
  if (!s) return null;
  if (/^[0-9]+$/.test(s)) return 'jan';
  if (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s)) return 'fnsku';
  return null;
}

// ───────────────────────── 印刷エージェント (倉庫PC) ─────────────────────────

/** 有効な印刷エージェント端末の一覧 (iPad の出力先選択・管理画面用)。 */
export function listPrintAgents({ now = utcNow() } = {}) {
  const rows = getDB().prepare(`SELECT id, label, printer_name, heartbeat_at, heartbeat_note, heartbeat_json, created_at, last_seen_at
    FROM f_inbound_check_devices WHERE kind = 'agent' AND revoked_at IS NULL ORDER BY id`).all();
  const nowMs = ms(now);
  return rows.map(r => {
    let hb = null;
    try { hb = r.heartbeat_json ? JSON.parse(r.heartbeat_json) : null; } catch { hb = null; }
    const hbMs = ms(r.heartbeat_at);
    return {
      id: r.id, label: r.label, printer_name: r.printer_name,
      heartbeat_at: r.heartbeat_at, heartbeat_note: r.heartbeat_note,
      online: hbMs != null && nowMs - hbMs <= AGENT_ONLINE_MS,
      // b-PAC が無い / 用紙未登録 のエージェントはジョブを取らない or 刷れない。押す前に分かるよう返す
      bpac: hb ? hb.bpac !== false : null,
      paper_ok: hb && hb.paperFormatOk != null ? !!hb.paperFormatOk : null,
      version: hb ? hb.version || null : null,
      host: hb ? hb.host || null : null,
    };
  });
}

/**
 * ジョブの出力先を決める。targetDeviceId を指定すればその端末、未指定なら**有効なエージェントが
 * 1台だけのときに限り**それ。2台以上あるときは iPad に選ばせる (勝手に選ぶと別の実機から出る)。
 */
export function resolvePrintTarget(targetDeviceId = null) {
  const agents = listPrintAgents();
  if (targetDeviceId != null) {
    const a = agents.find(x => x.id === Number(targetDeviceId));
    if (!a) return { ok: false, error: 'no_agent', message: '指定された印刷エージェントは登録されていません (失効した可能性があります)' };
    if (!a.printer_name) return { ok: false, error: 'no_printer', message: 'この印刷エージェントには出力先プリンターが登録されていません' };
    return { ok: true, agent: a };
  }
  const usable = agents.filter(a => a.printer_name);
  if (usable.length === 0) return { ok: false, error: 'no_agent', message: '値札を印刷する倉庫PCが登録されていません (管理画面で印刷エージェントを登録してください)' };
  if (usable.length > 1) return { ok: false, error: 'target_required', message: '印刷する倉庫PCを選んでください', agents: usable };
  return { ok: true, agent: usable[0] };
}

/** エージェントの生存報告。note は 200 字、詳細 (版・b-PAC・用紙) は JSON で 1000 字まで残す */
export function recordHeartbeat(deviceId, { note = null, version = null, bpac = null, host = null, paperFormat = null, paperFormatOk = null, printerReports = null } = {}) {
  const detail = {
    version: version == null ? null : String(version).slice(0, 40),
    bpac: typeof bpac === 'boolean' ? bpac : null,
    host: host == null ? null : String(host).slice(0, 60),
    paperFormat: paperFormat == null ? null : String(paperFormat).slice(0, 60),
    paperFormatOk: typeof paperFormatOk === 'boolean' ? paperFormatOk : null,
    printerReports: printerReports == null ? null : String(printerReports).slice(0, 120),
  };
  getDB().prepare('UPDATE f_inbound_check_devices SET heartbeat_at = ?, heartbeat_note = ?, heartbeat_json = ? WHERE id = ?')
    .run(utcNow(), note == null ? null : String(note).slice(0, 200), JSON.stringify(detail).slice(0, 1000), deviceId);
}

// ───────────────────────── 積む (iPad) ─────────────────────────

/**
 * 印刷ジョブを積む。
 *
 * @param {object} p
 *   batchId / lineKey … active バッチの明細 (商品名・バーコード・商品IDはここから取る。画面の値を信じない)
 *   copies            … 枚数 (1..MAX_COPIES)
 *   packQty           … 入数 (null/'' = 空欄で刷る)。整数以外は拒否
 *   targetDeviceId    … 出力先エージェント (省略時は resolvePrintTarget の規則)
 *   clientRequestId   … 冪等ID。同じIDの再送は同じジョブを返す (二重タップ・応答消失で2枚出ない)
 *   acknowledgeUnknownJobId … 🚨直前のジョブが unknown (実物を確認) のときは必須。そのジョブ ID を「実物を見て、出ていなかった」
 *                             の証跡として受け取る。無ければ confirm_unknown で拒否 (誤タップ・古い画面・API直叩きで刷り直せない — Codex R1 High-2)
 *   requestedBy / requestedDevice … 記録用
 * @returns {{ok:true, job, created:boolean}|{ok:false, error, message, job?}}
 */
export function enqueuePrintJob({ batchId, lineKey, productCode = null, barcodeOverride = null, copies, packQty = null, targetDeviceId = null, clientRequestId, acknowledgeUnknownJobId = null, requestedBy = null, requestedDevice = null }) {
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
    if (!Number.isSafeInteger(q) || q < 1) return { ok: false, error: 'bad_pack_qty', message: '入数は1以上の整数で入力してください (空欄なら印字しません)' };
    pack = String(q);
  }
  // 出どころは2つ: 入荷受付伝票の明細 (batch_id + line_key) か、🔍 商品を探して (product_code)
  const source = productCode != null && String(productCode).trim() !== '' ? 'product' : 'line';
  const bid = Number(batchId);
  const key = String(lineKey || '').trim();
  if (source === 'line' && (!Number.isInteger(bid) || !key)) return { ok: false, error: 'bad_request', message: 'batch_id と line_key (または product_code) が必要です' };

  return db.transaction(() => {
    // 同じ冪等IDの再送は「もう積んである」を成功として返す
    const dup = db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE client_request_id = ?').get(crid);
    if (dup) return { ok: true, job: publicJob(dup), created: false, replayed: true };

    // 刷る内容 (商品名・商品ID・バーコード) は**画面の値ではなく DB から**取る
    let subject;
    if (source === 'line') {
      const active = db.prepare('SELECT id FROM f_inbound_check_batches WHERE status = ? ORDER BY id DESC LIMIT 1').get('active');
      if (!active || active.id !== bid) return { ok: false, error: 'stale_batch', message: '一覧が新しくなっています。画面を更新してからもう一度押してください' };
      const line = db.prepare('SELECT * FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(bid, key);
      if (!line) return { ok: false, error: 'not_found', message: 'この明細は一覧にありません' };
      // 刷る値は **その明細自身のバーコード** が最優先 (同じ商品が同じ伝票に複数行あって値が違うことがある。Codex R3 Medium-2)。
      // 明細に値が無い (ロジザード未登録) ときだけ、商品モードと同じ経路 (resolveBarcode: 在庫 → 入荷予定 → 控え) に落ちる —
      // 🔍 商品画面で入れた控えがあれば伝票からも刷れる (Codex R2 High-2)
      // 明細に値があるのに刷れない形 (記号入りなど) のときは、その値のまま拒否する — 他所の値でこっそり刷らない。
      // 明細が空のときだけ 在庫 → 入荷予定 → 控え に落ちる
      const own = String(line.barcode == null ? '' : line.barcode).trim();
      const bc = own ? { barcode: own } : resolveBarcode(line.code_key, db);
      subject = { codeKey: line.code_key, productCode: line.product_id, productName: line.product_name, barcode: bc ? bc.barcode : null, batchId: bid, lineKey: key };
    } else {
      // 商品マスタから。バーコードは取込行 → 在庫ミラー → 入荷予定 → 控え (f_inbound_check_barcodes) の順に探す。
      // 刷る値は **必ず DB (控え) の値**。画面から来た barcodeOverride は:
      //   - 控えが無い → 形式を検査して控えに保存し、その控えの値で刷る (次からは入力不要)
      //   - 控えがあって違う → 積まない (ダイアログを開いた後に別の人が入れた/直した = 古い入力で違うシールを出さない。Codex R1 High-2)
      const p = getProductForPrint(productCode);
      if (!p) return { ok: false, error: 'not_found', message: 'この商品は商品マスタにありません' };
      const override = String(barcodeOverride == null ? '' : barcodeOverride).trim();
      if (override && !barcodeTypeOf(override)) {
        return { ok: false, error: 'bad_barcode', message: `バーコード「${override}」は JAN (数字のみ) でも FNSKU (英数字) でもないため印刷できません` };
      }
      if (override && p.barcode && override !== p.barcode) {
        return { ok: false, error: 'state_changed', message: `この商品のバーコードは「${p.barcode}」で登録されています (画面を開いた後に別の人が入れた/直した)。画面を更新して確認してください` };
      }
      if (override && !p.barcode) {
        const saved = setProductBarcode(p.product_id, override, requestedDevice ? `${requestedDevice}/${requestedBy || ''}` : (requestedBy || null), { expected: null });
        if (!saved.ok) return saved;
        p.barcode = saved.barcode;
      }
      subject = { codeKey: p.code_key, productCode: p.product_id, productName: p.product_name, barcode: p.barcode, batchId: null, lineKey: null };
    }
    const barcode = String(subject.barcode == null ? '' : subject.barcode).trim();
    const type = barcodeTypeOf(barcode);
    if (!type) {
      return { ok: false, error: 'bad_barcode', message: barcode ? `バーコード「${barcode}」は JAN (数字のみ) でも FNSKU (英数字) でもないため印刷できません`
        : (source === 'line' ? 'この商品はロジザードにバーコードが登録されていないため印刷できません (🔍 商品から探す で JAN / FNSKU を入れると刷れます)' : 'この商品のバーコードが分かりません。JAN か FNSKU を入力してください') };
    }
    const name = String(subject.productName == null ? '' : subject.productName).trim();
    if (!name) return { ok: false, error: 'bad_request', message: '商品名が空のため印刷できません' };

    // 🚨 二重印刷の見張りは **商品単位** (code_key)。伝票から刷っても 🔍 商品画面から刷っても紙は同じ1枚なので、
    //    片方が進行中 / 結果不明なら、もう片方からも積まない (Codex R1 High-1)。伝票の作り直し (superseded) を
    //    またいでも同じ: 朝の伝票で結果不明のまま、午後の伝票から証跡なしに刷り直させない。
    //    「最新1件」ではなく **同じ商品の全ジョブ** を見る — 旧版は明細ごとに並行して積めたので、古い未確定 (leased/unknown) の
    //    後ろに別明細の completed が居ることがあり、最新1件だけ見ると隠れる (Codex R2 High-1)。
    //    終わっていれば (completed/failed/manual、unknown は証跡つき) 新しいジョブを積める = 人が判断して押し直す
    const ck = String(subject.codeKey == null ? '' : subject.codeKey).trim().toLowerCase();
    const scope = ck ? ['code_key = ?', [ck]] : ["source = 'line' AND batch_id = ? AND line_key = ?", [bid, key]];
    const active = db.prepare(`SELECT * FROM f_inbound_check_print_jobs WHERE ${scope[0]} AND state IN ('queued','leased','submitted') ORDER BY id DESC LIMIT 1`).get(...scope[1]);
    if (active) {
      return { ok: false, error: 'in_progress', message: 'この商品のシールは印刷中です (結果が出るまでお待ちください)', job: publicJob(active) };
    }
    // 🚨 未確認 (acknowledged_at IS NULL) の「❓ 結果不明」が1件でもあれば、そのジョブ ID を「実物を見て出ていなかった」の
    //    証跡として受け取ってからしか積まない。紙が出ていたのに刷り直すと同じ商品のシールが2枚になり、別の箱に貼られると棚が狂う (気づけない)
    const pending = db.prepare(`SELECT * FROM f_inbound_check_print_jobs WHERE ${scope[0]} AND state = 'unknown' AND acknowledged_at IS NULL ORDER BY id DESC LIMIT 1`).get(...scope[1]);
    let acked = null;
    if (pending) {
      if (Number(acknowledgeUnknownJobId) !== pending.id) {
        return { ok: false, error: 'confirm_unknown', message: '前回の印刷結果が不明です。QL-700 の実物を確認し、シールが出ていない場合だけ「実物を確認した」にチェックしてもう一度発行してください', job: publicJob(pending) };
      }
      acked = pending.id;
    } else if (acknowledgeUnknownJobId != null) {
      // 🚨 証跡を付けて送ってきたのに、もう未確認の unknown が無い (送る直前に遅延報告で completed になった / 別の画面で
      //    確認済み / 画面が古い)。「出ていない」という前提が崩れているので積まない — 旧印刷 + 新印刷の2枚になる (Codex R3 High)
      const ackRow = db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id = ?').get(Number(acknowledgeUnknownJobId));
      const last = db.prepare(`SELECT * FROM f_inbound_check_print_jobs WHERE ${scope[0]} ORDER BY id DESC LIMIT 1`).get(...scope[1]);
      const other = last && (!ackRow || last.id !== ackRow.id);
      const shown = ackRow || last;
      return { ok: false, error: 'state_changed', message: ackRow && ackRow.state === 'completed'
        ? '前回のジョブは「✅ 印刷しました」に変わりました (遅れて結果が届きました)。そのシールを使ってください'
        : other
          ? 'この商品の値札はその後に別の画面から発行され「' + (PRINT_STATE_LABELS[last.state] || last.state) + '」です。画面を更新して確認してください'
          : '前回のジョブの状態が変わりました。画面を更新して確認してください', job: shown ? publicJob(shown) : null };
    }

    const target = resolvePrintTarget(targetDeviceId);
    if (!target.ok) return target;
    const now = utcNow();
    if (acked != null) {
      // 🚨 人が「出ていない」と確認して再発行する = 旧ジョブの結果はここで確定。以後、旧 lease の遅延報告
      //    (unknown → completed) を受け付けると、新ジョブと合わせて2枚出る (Codex R2 High)。
      //    lease_token を消して報告の照合を通らなくし、同じトランザクションで CAS (状態が動いていたらやり直し)
      const fix = db.prepare(`UPDATE f_inbound_check_print_jobs SET lease_token = NULL, acknowledged_at = ?, updated_at = ?,
        error = COALESCE(error, '') || ' / 実物を確認して再発行 (出ていない)' WHERE id = ? AND state = 'unknown' AND acknowledged_at IS NULL`)
        .run(now, now, acked);
      if (fix.changes !== 1) {
        const cur = db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id = ?').get(acked);
        return { ok: false, error: 'confirm_unknown', message: '前回のジョブの状態が変わりました。画面を更新してもう一度確認してください', job: publicJob(cur) };
      }
    }
    const info = db.prepare(`INSERT INTO f_inbound_check_print_jobs
      (client_request_id, source, batch_id, line_key, code_key, product_code, product_name, barcode, barcode_type, pack_qty, copies,
       printer_name, target_device_id, requested_by, requested_device, acknowledged_job_id, state, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`)
      .run(crid, source, subject.batchId, subject.lineKey, subject.codeKey, subject.productCode, name, barcode, type, pack, n,
        target.agent.printer_name, target.agent.id, requestedBy, requestedDevice, acked, now, now);
    const job = db.prepare('SELECT * FROM f_inbound_check_print_jobs WHERE id = ?').get(Number(info.lastInsertRowid));
    return { ok: true, job: publicJob(job), created: true };
  }).immediate();
}

/** iPad / 管理画面に返す形 (lease token は絶対に含めない) */
export function publicJob(row) {
  if (!row) return null;
  return {
    id: row.id, state: row.state, label: PRINT_STATE_LABELS[row.state] || row.state,
    source: row.source || 'line', code_key: row.code_key,
    line_key: row.line_key, product_code: row.product_code, product_name: row.product_name,
    barcode: row.barcode, barcode_type: row.barcode_type, pack_qty: row.pack_qty, copies: row.copies,
    printer_name: row.printer_name, target_device_id: row.target_device_id,
    requested_by: row.requested_by, error: row.error, acknowledged_job_id: row.acknowledged_job_id ?? null,
    acknowledged_at: row.acknowledged_at ?? null,
    created_at: row.created_at, updated_at: row.updated_at, submitted_at: row.submitted_at, finished_at: row.finished_at,
  };
}

/** バッチ内の明細ごとの最新ジョブ (line_key → publicJob)。/api/state で行に付ける */
/**
 * 伝票の各明細に出す値札ジョブ (line_key → publicJob)。
 * 見張りは商品単位なので、見せる状態も商品単位: 同じ商品に **未確定 (進行中 / 未確認の結果不明)** のジョブがあれば
 * それを (別の明細・作り直す前の伝票・🔍 商品画面からのものでも) 出し、無ければその明細自身の最新を出す (Codex R2 Medium-2)
 */
export function latestJobsForBatch(batchId) {
  const db = getDB();
  const lines = db.prepare('SELECT line_key, code_key FROM f_inbound_check_lines WHERE batch_id = ?').all(batchId);
  const map = new Map();
  if (lines.length === 0) return map;
  const own = new Map();
  for (const r of db.prepare(`SELECT j.* FROM f_inbound_check_print_jobs j
    JOIN (SELECT line_key, MAX(id) AS id FROM f_inbound_check_print_jobs WHERE source = 'line' AND batch_id = ? GROUP BY line_key) m ON m.id = j.id`).all(batchId)) own.set(r.line_key, r);
  const pending = pendingJobsByProduct(db, lines.map(l => l.code_key));
  for (const l of lines) {
    const j = pending.get(String(l.code_key == null ? '' : l.code_key).trim().toLowerCase()) || own.get(l.line_key);
    if (j) map.set(l.line_key, publicJob(j));
  }
  return map;
}

/**
 * 同じ商品の未確定ジョブ (code_key → row)。**見張り (enqueuePrintJob) と同じ順で選ぶ**:
 * まず進行中 (queued/leased/submitted)、無ければ未確認の結果不明 (unknown かつ acknowledged_at IS NULL)。
 * まとめて MAX(id) にすると、古い進行中の後ろに新しい unknown が居るとき、画面は「実物を確認して再発行」を出すのに
 * 受付は in_progress を返す — 表示と受付が食い違う (Codex R3 Medium-1)
 */
function pendingJobsByProduct(db, codeKeys) {
  const keys = [...new Set((codeKeys || []).map(k => String(k == null ? '' : k).trim().toLowerCase()).filter(Boolean))];
  const map = new Map();
  const pick = (cond) => {
    for (let i = 0; i < keys.length; i += 500) {
      const part = keys.slice(i, i + 500);
      const ph = part.map(() => '?').join(',');
      for (const r of db.prepare(`SELECT j.* FROM f_inbound_check_print_jobs j
        JOIN (SELECT code_key, MAX(id) AS id FROM f_inbound_check_print_jobs
              WHERE code_key IN (${ph}) AND ${cond} GROUP BY code_key) m ON m.id = j.id`).all(...part)) {
        if (!map.has(r.code_key)) map.set(r.code_key, r);
      }
    }
  };
  pick("state IN ('queued','leased','submitted')");
  pick("state = 'unknown' AND acknowledged_at IS NULL");
  return map;
}

/** 商品ごとに見せるジョブ (code_key → publicJob): 未確定があればそれ、無ければ最新 (伝票からの発行も含む)。🔍 商品を探す画面の行に付ける */
export function latestJobsForProducts(codeKeys) {
  const db = getDB();
  const keys = [...new Set((codeKeys || []).map(k => String(k == null ? '' : k).trim().toLowerCase()).filter(Boolean))];
  const map = new Map();
  if (keys.length === 0) return map;
  const pending = pendingJobsByProduct(db, keys);
  for (let i = 0; i < keys.length; i += 500) {
    const part = keys.slice(i, i + 500);
    const ph = part.map(() => '?').join(',');
    for (const r of db.prepare(`SELECT j.* FROM f_inbound_check_print_jobs j
      JOIN (SELECT code_key, MAX(id) AS id FROM f_inbound_check_print_jobs WHERE code_key IN (${ph}) GROUP BY code_key) m ON m.id = j.id`).all(...part)) {
      map.set(r.code_key, publicJob(pending.get(r.code_key) || r));
    }
  }
  return map;
}

// ───────────────────────── エージェント側 (pull) ─────────────────────────

/**
 * 次のジョブを1件だけ原子的に lease して返す (無ければ null)。
 * その端末宛て (target_device_id) の queued だけ。**leased 以降は絶対に拾わない** (ジョブを渡した = 紙が出たかもしれない)。
 * 返す JSON の形はエージェント (agent.ps1 Test-JobData / New-LabelFields) が前提にしているものと同じ。
 */
export function leaseNextJob(device, { now = utcNow() } = {}) {
  const db = getDB();
  if (!device || device.kind !== 'agent' || !device.printer_name) return null;
  return db.transaction(() => {
    const job = db.prepare(`SELECT * FROM f_inbound_check_print_jobs WHERE state = 'queued' AND target_device_id = ?
      ORDER BY id LIMIT 1`).get(device.id);
    if (!job) return null;
    // 積んだときのプリンター名と、いま端末が持つ名前が違えば渡さない (名前を付け替えた後の古いジョブを
    // 別のプリンターから出さない)。manual に倒して人に知らせる
    if (job.printer_name !== device.printer_name) {
      db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'manual', error = ?, finished_at = ?, updated_at = ? WHERE id = ? AND state = 'queued'`)
        .run(`出力先プリンター名が変わったため自動印刷を取り消しました (${job.printer_name} → ${device.printer_name})`, now, now, job.id);
      return null;
    }
    const leaseToken = crypto.randomBytes(16).toString('base64url');
    const deadline = iso(ms(now) + REPORT_DEADLINE_SEC * 1000);
    const upd = db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'leased', lease_device_id = ?, lease_token = ?,
      lease_expires_at = ?, leased_at = ?, updated_at = ? WHERE id = ? AND state = 'queued'`)
      .run(device.id, leaseToken, deadline, now, now, job.id);
    if (upd.changes !== 1) return null;   // 同時実行で他に取られた
    return {
      id: job.id,
      leaseToken,
      printerName: job.printer_name,
      productCode: job.product_code,
      productName: job.product_name,
      barcode: job.barcode,
      barcodeType: job.barcode_type,
      packQty: job.pack_qty == null ? '' : String(job.pack_qty),
      copies: job.copies,
      lineKey: job.line_key || '',   // 商品モードは明細が無い (エージェントは参考情報としてしか使わない)
      requestedBy: job.requested_by || '',
      leaseExpiresAt: deadline,
    };
  }).immediate();
}

/** 同じ端末・同じ lease による「もう一度同じ報告」を成功として返す (冪等) */
function alreadyIn(jobId, state, deviceId, leaseToken) {
  const row = getDB().prepare('SELECT state, lease_device_id, lease_token FROM f_inbound_check_print_jobs WHERE id = ?').get(jobId);
  return !!row && row.state === state && row.lease_device_id === deviceId && !!leaseToken && row.lease_token === leaseToken;
}

/**
 * スプーラーに投入した報告。leased からのみ進む。報告の受付期限も延ばす (投入から印刷完了までさらに時間がかかる)。
 * 同じ lease の再送は replayed:true — ただし **spool_job_id が前回と違えば 409** (台帳の破損・別スプールへの二重投入を
 * 黙って成功にしない。Codex R1 Med)
 */
export function markSubmitted(jobId, { deviceId, leaseToken, spoolJobId = null, now = utcNow() }) {
  const spool = spoolJobId == null || spoolJobId === '' ? null : String(spoolJobId).slice(0, 60);
  const db = getDB();
  return db.transaction(() => {
    const upd = db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'submitted', spool_job_id = ?, submitted_at = ?,
      updated_at = ?, lease_expires_at = ?
      WHERE id = ? AND state = 'leased' AND lease_device_id = ? AND lease_token = ? AND lease_expires_at > ?`)
      .run(spool, now, now, iso(ms(now) + REPORT_DEADLINE_SEC * 1000), jobId, deviceId, leaseToken || '', now);
    if (upd.changes === 1) return { ok: true };
    if (alreadyIn(jobId, 'submitted', deviceId, leaseToken)) {
      const cur = db.prepare('SELECT spool_job_id FROM f_inbound_check_print_jobs WHERE id = ?').get(jobId);
      if ((cur?.spool_job_id ?? null) === spool) return { ok: true, replayed: true };
      return { ok: false, reason: 'submission_conflict', message: `前回の投入報告 (spool ${cur?.spool_job_id ?? '-'}) と違う spool_job_id (${spool ?? '-'}) です` };
    }
    return { ok: false, reason: 'not_leased_or_wrong_state' };
  }).immediate();
}

/**
 * 完了/失敗の報告。
 *   - 成功 (ok:true) は submitted からのみ (スプーラーに入れずに「刷れた」は受け付けない)
 *   - 失敗 (ok:false) は leased / submitted から。
 *     🚨 **failed (= もう一度押してよい) にできるのは、まだスプーラーに入れていない leased からだけ**。
 *        submitted (投入済み) からの失敗は uncertain の値にかかわらず unknown — 紙が出ているかもしれない (Codex R1 High-1)
 *   - ⭐期限切れで unknown に倒した後に**同じ lease の報告が遅れて届いた**ときは受け付けて上書きする
 *     (unknown → completed / failed)。エージェントが再起動後に台帳から報告してくる経路で、
 *     「実物を確認」より確かな情報なので捨てない。ただし一度でも投入済み (submitted_at あり) なら failed にはしない。
 *     completed / failed / manual からは動かさない。
 *     🚨 ただし人が実物を確認して再発行した後 (acknowledged_at あり = enqueue が lease_token を消している) は
 *        旧 lease の遅延報告を受け付けない — 受けると新ジョブと合わせて2枚出る (Codex R2 High)
 * 🚨「刷れなかった」と「紙が出たか分からない」を混ぜない — failed は「もう一度押してよい」を意味する
 */
export function markFinished(jobId, { deviceId, leaseToken, ok, error = null, uncertain = false, now = utcNow() }) {
  if (typeof ok !== 'boolean') return { ok: false, reason: 'bad_ok' };
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT state, submitted_at, lease_device_id, lease_token FROM f_inbound_check_print_jobs WHERE id = ?').get(jobId);
    if (!row) return { ok: false, reason: 'not_found' };
    const everSubmitted = row.state === 'submitted' || !!row.submitted_at;
    const target = ok ? 'completed' : ((uncertain || everSubmitted) ? 'unknown' : 'failed');
    const from = ok ? ['submitted', 'unknown'] : ['leased', 'submitted', 'unknown'];
    const upd = db.prepare(`UPDATE f_inbound_check_print_jobs SET state = ?, error = ?, finished_at = ?, updated_at = ?, lease_expires_at = NULL
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
    FROM f_inbound_check_print_jobs WHERE id = ? AND lease_device_id = ?`).get(jobId, deviceId);
  return row || null;
}

// ───────────────────────── 見張り (常駐ワーカー) ─────────────────────────

/**
 * 進まなくなったジョブを安全な状態へ移す。**通知はここではしない** (状態の確定と通知を分ける —
 * 送信に失敗しても「通知済み」が確定して永久に鳴らなくなるのを避ける)。
 *   ① queued のまま STALE_QUEUED_SEC → manual (印刷係が来ない。復帰した印刷係が後から刷って二重にならないよう先に外す)
 *   ② leased / submitted のまま報告期限を過ぎた → unknown (紙が出たかもしれない。自動では刷り直さない)
 */
export function sweepPrintJobs({ now = utcNow() } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const staleBefore = iso(ms(now) - STALE_QUEUED_SEC * 1000);
    const manual = db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'manual', finished_at = ?, updated_at = ?,
      error = '印刷係 (倉庫PCのエージェント) が取りに来ませんでした'
      WHERE state = 'queued' AND updated_at <= ?`).run(now, now, staleBefore).changes;
    const unknown = db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'unknown', finished_at = ?, updated_at = ?,
      error = '倉庫PCから結果の報告が届きませんでした'
      WHERE state IN ('leased', 'submitted') AND lease_expires_at <= ?`).run(now, now, now).changes;
    return { manual, unknown };
  }).immediate();
}

/** まだ人に伝えられていない結果 (送信に成功したら markAlerted を呼ぶ) */
export function pendingAlerts(limit = 20) {
  return getDB().prepare(`SELECT * FROM f_inbound_check_print_jobs
    WHERE state IN (${ALERT_STATES.map(() => '?').join(',')}) AND (alerted_state IS NULL OR alerted_state <> state)
    ORDER BY id LIMIT ?`).all(...ALERT_STATES, limit);
}

export function markAlerted(jobId, state) {
  getDB().prepare('UPDATE f_inbound_check_print_jobs SET alerted_state = ? WHERE id = ? AND state = ?').run(state, jobId, state);
}

/** 状態に応じた通知文 (読み手ファースト — 何が起きて何をすればよいかだけ) */
export function alertTextFor(job) {
  const who = `${job.product_name} (${job.product_code}) の値札 ${job.copies}枚`;
  const printer = job.printer_name || 'プリンター';
  switch (job.state) {
    case 'completed':
      return `🏷 ${who} を印刷しました (${printer})`;
    case 'failed':
      return `⚠ ${who} を印刷できませんでした (${String(job.error || '').slice(0, 80)}) — 紙は出ていません。iPad からもう一度「🏷 シール発行」を押せます`;
    case 'manual':
      return `🙋 ${who} が印刷待ちのまま進みませんでした (倉庫PCが寝ている / 印刷係が動いていない可能性)。自動印刷は取り消したので二重には出ません — P-touch Editor で手で刷ってください`;
    case 'unknown':
      return `❓ ${who} の印刷結果が不明です (${printer})。二重印刷を避けるため自動では刷り直していません — QL-700 の実物を確認してください`;
    default:
      return null;
  }
}

/** 管理画面用 */
export function listPrintJobs(limit = 30) {
  return getDB().prepare(`SELECT j.*, d.label AS device_label FROM f_inbound_check_print_jobs j
    LEFT JOIN f_inbound_check_devices d ON d.id = j.target_device_id ORDER BY j.id DESC LIMIT ?`).all(limit);
}
