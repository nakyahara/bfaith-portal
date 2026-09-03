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
import { getTaskByDestination, safeLogTaskEvent } from './tasks-db.js';
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
    if (/UNIQUE constraint failed: f_iroha_tasks\.notion_page_id/.test(e.message)) return 0;   // 取込で別タスクが持っている
    throw e;
  }
}
