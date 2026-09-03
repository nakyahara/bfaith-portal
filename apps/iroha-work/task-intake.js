/**
 * いろは在庫化作業アプリ — 入荷受付からのタスク生成 (PR-B)
 *
 * 入荷受付チェックで「いろはで在庫化」と確定した明細 (f_inbound_check_destinations) を、確定と**同じトランザクション**で
 * f_iroha_tasks に 1 枚入れる (要件 v1.1 §D: 17:30 の一括を待たない — あれは Notion 都合の待ちだった)。
 *   - 正本が Notion の間: 従来どおり 17:30 の sweep が Notion カードも作り、そのとき notion_page_id をタスクに紐付ける
 *     (linkTaskToNotionPage)。切替前の差分取込で同じカードが「DB 既存 destination との衝突」にならない
 *   - 正本がアプリ: sweep は何もしない。iPad はここで作ったタスクをそのまま見る
 * やり直し・再取込で行き先が取り消されたら tasks-db.requestCancellation (未着手・実績なしは自動取消、着手後は要確認)。
 * 呼び元 (inbound-check/db.js) のトランザクション内で呼ぶ — 同じ warehouse-mirror.db の同じ接続なので、確定と一緒にコミット/ロールバックされる。
 */
import { getDB } from './db.js';
import { DEFAULT_FACILITY } from './tasks.js';
import { getTaskByDestination, safeLogTaskEvent, requestCancellation } from './tasks-db.js';
import { normSupplierCode } from '../purchase-orders/db.js';

const utcNow = () => new Date().toISOString();
const tableExists = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
const codeKeyOf = (s) => String(s || '').trim().toLowerCase();

/**
 * 作成時に載せる商品情報 — Notion カード (notion-sync.buildCardProperties) と同じ出どころ: 商品マスタ (取扱区分・仕入先)、
 * 仕入先名、作業仕様 (資材・保管箱・入数・工程数・備考)、取込行のバーコード。無ければ null のまま (それが正常)。
 * 30日販売数・フリー在庫は載せない (一覧が表示のたびに live で引く — 作成時点の数を固定する意味が無い)
 */
export function enrichForDestination(db, dest, { barcode = null } = {}) {
  const key = codeKeyOf(dest.code_key || dest.product_id);
  let product = null;
  let supplierName = null;
  let wm = null;
  if (key && tableExists(db, 'mirror_products')) {
    product = db.prepare('SELECT 仕入先コード AS supplierCode, 取扱区分 AS handling FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ? LIMIT 1').get(key) || null;
  }
  if (product?.supplierCode != null && tableExists(db, 'po_suppliers')) {
    supplierName = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code = ?').get(normSupplierCode(product.supplierCode))?.name || null;
  }
  if (key && tableExists(db, 'f_iroha_work_master')) {
    wm = db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(key) || null;
  }
  let bc = barcode;
  if (!bc && tableExists(db, 'f_inbound_check_lines')) {
    bc = db.prepare('SELECT barcode FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(dest.batch_id, dest.line_key)?.barcode || null;
  }
  return { product, supplierName, wm, barcode: bc || null };
}

/**
 * 行き先 (いろは) 1 行からタスクを 1 枚作る。destination_id で冪等 (既にあれば作らない・書き換えない)。
 * @param dest f_inbound_check_destinations の行
 * @returns {{action:'inserted'|'exists'|'skipped', id:number|null}}
 */
export function createTaskForDestination(dest, { actor = null, barcode = null } = {}) {
  const db = getDB();
  if (!dest || dest.destination !== 'iroha') return { action: 'skipped', id: null };
  if (dest.cancelled_at) return { action: 'skipped', id: null };
  const existing = getTaskByDestination(dest.id);
  if (existing) return { action: 'exists', id: existing.id };
  const e = enrichForDestination(db, dest, { barcode });
  const now = utcNow();
  const qty = dest.actual_qty ?? dest.planned_qty ?? null;
  // payload は Notion 時代の props と同じキー名 (一覧の表示コードが props['入庫日'] 等を読むため)
  const payload = {
    '商品コード': dest.product_id || null, '数量': qty, '入庫日': dest.work_date || null, '入荷管理番号': dest.ar_no || null,
    'バーコード': e.barcode, '取引先': e.supplierName, '取扱区分': e.product?.handling || null, '作業拠点': 'いろは',
    '有効期限': dest.expiry_date || null, destination_id: dest.id, source: 'inbound_check',
  };
  const snapshot = e.wm ? {
    material_code: e.wm.material_code ?? null, storage_container: e.wm.storage_container ?? null,
    units_per_container: e.wm.units_per_container ?? null, process_count: e.wm.process_count ?? null,
    note: e.wm.note ?? null, video_url: e.wm.video_url ?? null, version: e.wm.version ?? null,
  } : null;
  const who = actor ? `inbound:${actor}` : 'inbound';
  const info = db.prepare(`INSERT INTO f_iroha_tasks
      (destination_id, status, facility_code, product_code, product_name, qty, arrival_date, ar_no, barcode, expiry, supplier, handling,
       master_snapshot, payload, version, created_at, created_by, updated_at, updated_by)
    VALUES (?, 'not_started', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    ON CONFLICT(destination_id) WHERE destination_id IS NOT NULL DO NOTHING`)
    .run(dest.id, DEFAULT_FACILITY, dest.product_id || null, dest.product_name || dest.product_id || null, qty,
      dest.work_date || null, dest.ar_no || null, e.barcode, dest.expiry_date || null, e.supplierName, e.product?.handling || null,
      snapshot ? JSON.stringify(snapshot) : null, JSON.stringify(payload), now, who, now, who);
  if (info.changes === 0) {
    const t = getTaskByDestination(dest.id);
    return { action: 'exists', id: t ? t.id : null };
  }
  const id = Number(info.lastInsertRowid);
  safeLogTaskEvent({ taskId: id, action: 'task_created', to: `inbound_check dest#${dest.id}${e.wm ? '' : ' (作業仕様なし)'}`, workerName: actor, ok: true });
  return { action: 'inserted', id };
}

/**
 * Notion カードを作った/回収したとき (Notion 正本の間) にタスクへ紐付ける。
 * 既にそのタスクに別のページが付いている、またはそのページが別のタスク (取込済み) に付いていれば触らない。
 * @returns 更新した行数 (0 = 紐付け対象なし)
 */
export function linkTaskToNotionPage(destinationId, pageId) {
  const db = getDB();
  if (destinationId == null || !pageId) return 0;
  try {
    return db.prepare('UPDATE f_iroha_tasks SET notion_page_id = ? WHERE destination_id = ? AND notion_page_id IS NULL')
      .run(String(pageId), Number(destinationId)).changes;
  } catch (e) {
    if (!/UNIQUE constraint failed: f_iroha_tasks\.notion_page_id/.test(e.message)) throw e;
    // 同じページが別のタスク (取込で作られた方) に付いている = 移行の整合が崩れている。黙って 0 にせず履歴に残して人に見せる (Codex PR-B R1 #2)
    const t = getTaskByDestination(destinationId);
    const other = db.prepare('SELECT id FROM f_iroha_tasks WHERE notion_page_id = ?').get(String(pageId));
    console.error(`[iroha-work] Notion ページ ${pageId} を dest#${destinationId} のタスク#${t?.id} に紐付けられません (タスク#${other?.id} が同じページを持っています)`);
    if (t) safeLogTaskEvent({ taskId: t.id, action: 'task_link_conflict', to: String(pageId), ok: false, error: `同じ Notion ページをタスク#${other?.id} が持っています (移行の整合を確認)` });
    return 'conflict';
  }
}

/**
 * 行き先台帳には notion_page_id があるのにタスクに無い行を埋める (カード作成→紐付けの間で落ちた分の修復。sweep の先頭で呼ぶ — Codex PR-B R1 #2)。
 * @returns {{linked:number, conflicts:number}}
 */
export function backfillTaskLinks() {
  const db = getDB();
  if (!tableExists(db, 'f_inbound_check_destinations')) return { linked: 0, conflicts: 0 };
  const rows = db.prepare(`SELECT t.destination_id, d.notion_page_id
    FROM f_iroha_tasks t JOIN f_inbound_check_destinations d ON d.id = t.destination_id
    WHERE t.notion_page_id IS NULL AND d.notion_page_id IS NOT NULL`).all();
  const out = { linked: 0, conflicts: 0 };
  for (const r of rows) {
    const res = linkTaskToNotionPage(r.destination_id, r.notion_page_id);
    if (res === 1) out.linked++;
    else if (res === 'conflict') out.conflicts++;
  }
  return out;
}

/**
 * いま衝突している紐付け (DB から直接): 行き先台帳にはページがあるのにタスクに無く、そのページを別のタスクが持っている。
 * 履歴 (task_link_conflict) は「起きた記録」、こちらは「今も解消していないもの」— 直せば 0 に戻る。管理画面の要確認に出す (Codex PR-B R2 #1)
 */
const LINK_CONFLICT_FROM = `FROM f_iroha_tasks t
    JOIN f_inbound_check_destinations d ON d.id = t.destination_id
    JOIN f_iroha_tasks o ON o.notion_page_id = d.notion_page_id AND o.id <> t.id
    WHERE t.notion_page_id IS NULL AND d.notion_page_id IS NOT NULL`;
export function listLinkConflicts(limit = 50) {
  const db = getDB();
  if (!tableExists(db, 'f_inbound_check_destinations')) return [];
  return db.prepare(`SELECT t.id AS task_id, t.destination_id, t.product_code, t.product_name, t.status, t.cancellation_requested_at, d.notion_page_id,
      d.cancelled_at AS destination_cancelled_at,
      o.id AS other_task_id, o.status AS other_status, o.product_code AS other_product_code, o.cancellation_requested_at AS other_cancellation_requested_at,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = t.id AND s.ended_at IS NULL) AS active_sessions,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = o.id AND s.ended_at IS NULL) AS other_active_sessions
    ${LINK_CONFLICT_FROM} ORDER BY t.id LIMIT ?`).all(Number(limit) || 50);
}
/** 総件数 (一覧は先頭だけなので、件数は別に数える — Codex PR-B R3 Low) */
export function countLinkConflicts() {
  const db = getDB();
  if (!tableExists(db, 'f_inbound_check_destinations')) return 0;
  return db.prepare(`SELECT COUNT(*) c ${LINK_CONFLICT_FROM}`).get().c;
}

// 統合で付け替える「タスクに紐づく記録」(task_id を持つ表)
const TASK_CHILD_TABLES = ['f_iroha_work_sessions', 'f_iroha_card_media', 'f_iroha_app_events', 'f_iroha_label_waits'];

/**
 * 紐付け衝突を統合する (管理者操作 — Codex PR-B R3 Medium / R4 で条件を固めた)。
 * 確定時に作ったタスク (inbound) と、Notion から取り込んだタスク (import) が同じカードを指しているとき、
 * 残す側 (keep) に 行き先 (destination_id) と Notion ページ (notion_page_id) の両方を集め、もう一方の
 * 作業時間・写真・履歴・ラベル待ちを残す側へ付け替えたうえで、もう一方を終了にする (未終了なら 終了:取消。既に終了なら理由はそのまま)。1 トランザクション。
 * 拒否する組み合わせ:
 *   - 残す側が終了していて、消える側は開いている (行き先だけが終了タスクへ移り、一覧から消える) → keep_closed。両方終了なら通す
 *   - 消える側で作業中の人がいる (活動中セッションを黙って別タスクへ移すと、その端末は終了できなくなる) → from_active
 * 消える側が着手済み (作業中/保留/棚入待ち) で残す側が未着手なら、残す側を作業中に上げて started_at を引き継ぐ。
 * 取消の要求 (行き先が取消済み / 消える側の cancellation_requested_at) は残す側へ引き継ぐ (取消済みの入荷の作業が続かない)。
 * @param taskId 確定側 (inbound) のタスク id (listLinkConflicts の task_id)
 * @param keep 'import' (取込側を残す。既定 — 現場が Notion 時代から動かしてきた方) | 'inbound' (確定側を残す)
 */
export function mergeLinkConflict({ taskId, keep = 'import', actor = null }) {
  const db = getDB();
  if (keep !== 'import' && keep !== 'inbound') return { ok: false, error: 'bad_request', message: 'keep は import / inbound のどちらかです' };
  return db.transaction(() => {
    const c = db.prepare(`SELECT t.id AS task_id, t.destination_id, d.notion_page_id, d.cancelled_at AS destination_cancelled_at, o.id AS other_task_id
      ${LINK_CONFLICT_FROM} AND t.id = ?`).get(Number(taskId));
    if (!c) return { ok: false, error: 'not_found', message: 'この衝突はもうありません (解消済みか、対象が変わりました)' };
    const intoId = keep === 'import' ? c.other_task_id : c.task_id;
    const fromId = keep === 'import' ? c.task_id : c.other_task_id;
    const row = (id) => db.prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(id);
    const activeOf = (id) => db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL').get(id).c;
    const into = row(intoId);
    const from = row(fromId);
    if (into.status === 'closed' && from.status !== 'closed') {
      return { ok: false, error: 'keep_closed', message: `残す側 (task#${intoId}) は終了しています。開いている側 (task#${fromId}) を残してください` };
    }
    const fromActive = activeOf(fromId);
    if (fromActive > 0) {
      return { ok: false, error: 'from_active', message: `消える側 (task#${fromId}) で作業中の人が ${fromActive} 人います。作業を終了してから統合するか、そちらを残してください` };
    }
    const now = utcNow();
    const who = actor || 'admin';
    // ① 消える側から行き先・ページを外す (UNIQUE を空けてから残す側に付ける)。取消の要求も残す側へ引き継ぐ
    db.prepare('UPDATE f_iroha_tasks SET destination_id = NULL, notion_page_id = NULL, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?').run(now, who, fromId);
    db.prepare(`UPDATE f_iroha_tasks SET destination_id = ?, notion_page_id = ?,
        cancellation_requested_at = COALESCE(cancellation_requested_at, ?), cancellation_source = COALESCE(cancellation_source, ?),
        version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?`)
      .run(c.destination_id, c.notion_page_id, from.cancellation_requested_at, from.cancellation_source, now, who, intoId);
    // ② 記録を残す側へ付け替え
    const moved = {};
    for (const t of TASK_CHILD_TABLES) {
      if (!tableExists(db, t)) continue;
      moved[t] = db.prepare(`UPDATE ${t} SET task_id = ? WHERE task_id = ?`).run(intoId, fromId).changes;
    }
    // ③ 消える側が着手済みで残す側が未着手なら、残す側を作業中へ (「作業記録を持つ未着手タスク」を作らない)
    let promoted = null;
    if (into.status === 'not_started' && ['in_progress', 'on_hold', 'ready_for_stocking'].includes(from.status)) {
      db.prepare('UPDATE f_iroha_tasks SET status = ?, started_at = COALESCE(started_at, ?, ?), version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?')
        .run('in_progress', from.started_at, now, now, who, intoId);
      promoted = `not_started→in_progress (消える側は ${from.status})`;
    }
    // ④ 消える側は終了 — 未終了なら 終了:取消。既に終了なら理由はそのまま。どこへ統合したか残す
    if (from.status !== 'closed') {
      db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'cancelled', closed_at = ?, closed_by = ?, hold_reason_code = NULL, hold_reason_note = NULL,
        cancellation_requested_at = NULL, migration_note = COALESCE(migration_note || ' / ', '') || ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?`)
        .run(now, who, `統合 → task#${intoId}`, now, who, fromId);
    } else {
      db.prepare("UPDATE f_iroha_tasks SET migration_note = COALESCE(migration_note || ' / ', '') || ? WHERE id = ?").run(`統合 → task#${intoId}`, fromId);
    }
    safeLogTaskEvent({ taskId: intoId, action: 'task_merge', from: `task#${fromId}`,
      to: `keep=${keep} dest#${c.destination_id} page=${c.notion_page_id} moved=${JSON.stringify(moved)}${promoted ? ' ' + promoted : ''}`, workerName: who, ok: true });
    // ⑤ 行き先が取消済みなら、残す側にも取消を伝える (未着手・実績なしは自動で終了:取消、着手後は要確認) — 同じトランザクション
    let cancellation = null;
    if (c.destination_cancelled_at) cancellation = requestCancellation({ destinationId: c.destination_id, source: 'inbound_reversal', actor: who });
    const keptNow = row(intoId);
    return { ok: true, kept: intoId, closed: fromId, moved, promoted, keptStatus: keptNow.status,
      cancellation: cancellation ? cancellation.action : null,
      note: '統合前から開いていた作業画面は一覧を読み直してください (古いタスク番号では操作できません)' };
  }).immediate();
}
