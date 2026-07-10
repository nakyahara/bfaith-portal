/**
 * purchase-orders 発注ライフサイクル台帳 (P13a)
 *
 * 数量イベント台帳 (po_item_events) への書込は本モジュールの appendPoItemEvent() のみが行う。
 * イベント登録・残数再検証・ヘッダ閉鎖状態 (closed_at) の再計算を BEGIN IMMEDIATE の同一
 * トランザクションで実行する (要件v8 §2-4)。
 *
 * 用語:
 *  - tracked: 移行境界 (po_settings.tracking_started_at) 以後に issued されたPO。残数管理対象
 *  - legacy : 境界以前の issued。履歴表示のみ、イベント登録は拒否
 *  - 残数   : v_po_item_balance (有効イベント = 非reversal かつ 未逆仕訳) から導出
 *  - closed : po_orders.closed_at IS NOT NULL。close_reason は保存せず導出
 *             (残0かつ有効cutoffあり→manual / なし→completed)
 */
import { getDB } from './db.js';

const nowIso = () => new Date().toISOString();

/** JSTの今日 (YYYY-MM-DD)。Date.toISOString()はUTCで日付がずれる既知の罠があるため自前組立 */
export function jstToday(d = new Date()) {
  const jst = new Date(d.getTime() + 9 * 3600000);
  return `${jst.getUTCFullYear()}-${String(jst.getUTCMonth() + 1).padStart(2, '0')}-${String(jst.getUTCDate()).padStart(2, '0')}`;
}

/** YYYY-MM-DD かつ実在日か */
export function isYmd(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split('-').map(Number);
  if (m < 1 || m > 12 || d < 1) return false;
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

const ACTOR_TYPES = new Set(['user', 'system', 'ai_agent', 'migration']);
const SHORTAGE_REASONS = new Set(['supplier_shortage', 'own_decision', 'cutoff', 'other']);

// ─── 設定 (現在値テーブル。変更は監査ログと同一txn) ───

export function getSetting(key) {
  const r = getDB().prepare('SELECT value FROM po_settings WHERE key=?').get(key);
  return r ? r.value : null;
}

export function setSetting(key, value, { actor = null, actorType = 'user', reason = null } = {}) {
  const db = getDB();
  db.transaction(() => {
    const old = db.prepare('SELECT value FROM po_settings WHERE key=?').get(key);
    const now = nowIso();
    db.prepare(`INSERT INTO po_settings (key, value, effective_at, changed_by, reason) VALUES (?,?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, effective_at=excluded.effective_at,
                changed_by=excluded.changed_by, reason=excluded.reason`)
      .run(key, String(value), now, actor, reason);
    audit(db, { actorType, actor, action: 'setting_change', resource: `setting:${key}`,
      detail: { old: old ? old.value : null, new: String(value), reason } });
  })();
}

/**
 * 移行境界の初期化。新コードの初回起動時に一度だけ現在時刻で確定する (要件v8: issueゲート内で記録したDB時刻)。
 * 以後この時刻以降に issued されたPOが tracked。設定済みなら何もしない。
 */
export function ensureTrackingStarted() {
  const db = getDB();
  db.transaction(() => {
    const cur = db.prepare("SELECT value FROM po_settings WHERE key='tracking_started_at'").get();
    if (cur) return;
    const now = nowIso();
    db.prepare("INSERT INTO po_settings (key, value, effective_at, changed_by, reason) VALUES ('tracking_started_at', ?, ?, 'system', 'P13a初回起動で境界確定')")
      .run(now, now);
    audit(db, { actorType: 'system', actor: null, action: 'tracking_boundary_set', resource: 'setting:tracking_started_at', detail: { value: now } });
  })();
  return getSetting('tracking_started_at');
}

/** POが残数管理対象 (tracked) か。正は境界時刻、tracking_mode列は発行時に固定される表示属性 */
export function isTracked(order) {
  if (!order || order.status === 'draft' || !order.issued_at) return false;
  const boundary = getSetting('tracking_started_at');
  if (!boundary) return false;
  return order.issued_at >= boundary;
}

// ─── 監査ログ ───

export function audit(db, { actorType = 'user', actor = null, action, resource, detail = null, requestId = null }) {
  db.prepare('INSERT INTO po_audit_log (occurred_at, actor_type, actor, action, resource, detail_json, request_id) VALUES (?,?,?,?,?,?,?)')
    .run(nowIso(), actorType, actor, action, resource, detail == null ? null : JSON.stringify(detail), requestId);
}

// ─── PO番号採番 (JST年ごと連番、欠番許容) ───

export function nextPoNumber(db, d = new Date()) {
  const year = Number(jstToday(d).slice(0, 4));
  const row = db.prepare(`INSERT INTO po_number_sequences (year, last_number) VALUES (?, 1)
                          ON CONFLICT(year) DO UPDATE SET last_number = last_number + 1
                          RETURNING last_number`).get(year);
  return `PO-${year}-${String(row.last_number).padStart(4, '0')}`;
}

// ─── 残数・閉鎖状態 ───

/** 明細1件の残数等 (view から)。無ければ null */
export function balanceOf(itemId, db = getDB()) {
  return db.prepare('SELECT * FROM v_po_item_balance WHERE order_item_id=?').get(itemId) || null;
}

/** PO全明細の残数一覧 */
export function orderBalances(orderId, db = getDB()) {
  return db.prepare('SELECT * FROM v_po_item_balance WHERE order_id=? ORDER BY order_item_id').all(orderId);
}

/** close_reason の導出: 残0かつ有効cutoffあり→manual / なし→completed / オープン→null */
export function deriveCloseReason(orderId, db = getDB()) {
  const rows = orderBalances(orderId, db);
  if (!rows.length || rows.some(r => r.remaining_qty > 0)) return null;
  return rows.some(r => r.cutoff_qty > 0) ? 'manual' : 'completed';
}

/**
 * ヘッダ閉鎖状態の共通再計算 (イベント書込と同一txn内で必ず呼ぶ。手動クローズも例外にしない):
 *  - 全明細残0 & closed_at NULL → closed_at セット (auto_close)
 *  - 残>0 & closed_at NOT NULL → closed_at クリア (auto_reopen、逆仕訳による復帰)
 */
function recomputeClosure(db, orderId, ctx) {
  const order = db.prepare('SELECT id, closed_at, status FROM po_orders WHERE id=?').get(orderId);
  if (!order) return { closed: false };
  const rows = orderBalances(orderId, db);
  const allZero = rows.length > 0 && rows.every(r => r.remaining_qty === 0);
  if (allZero && !order.closed_at) {
    const now = nowIso();
    db.prepare('UPDATE po_orders SET closed_at=?, updated_at=? WHERE id=?').run(now, now, orderId);
    audit(db, { actorType: ctx.actorType, actor: ctx.actor, action: 'order_closed', resource: `order:${orderId}`,
      detail: { reason: rows.some(r => r.cutoff_qty > 0) ? 'manual' : 'completed' } });
    return { closed: true };
  }
  if (!allZero && order.closed_at) {
    const now = nowIso();
    db.prepare('UPDATE po_orders SET closed_at=NULL, updated_at=? WHERE id=?').run(now, orderId);
    audit(db, { actorType: ctx.actorType, actor: ctx.actor, action: 'order_reopened', resource: `order:${orderId}`,
      detail: { by: ctx.action || 'reversal' } });
    return { closed: false, reopened: true };
  }
  return { closed: allZero };
}

/** 残0になった明細の disposition/next_* を履歴記録→クリア (要件F-2: 記録が先) */
function clearDispositionIfDone(db, itemId, ctx) {
  const bal = balanceOf(itemId, db);
  if (!bal || bal.remaining_qty !== 0) return;
  const item = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(itemId);
  const fields = ['remainder_disposition', 'next_expected_date', 'next_expected_qty', 'next_action_date'];
  const dirty = fields.filter(f => item[f] != null);
  if (!dirty.length) return;
  const changeId = `evt${ctx.eventId || ''}-${Date.now()}`;
  const ins = db.prepare('INSERT INTO po_item_history (order_item_id, change_id, field, old_value, new_value, note, created_at, actor) VALUES (?,?,?,?,?,?,?,?)');
  const now = nowIso();
  for (const f of dirty) ins.run(itemId, changeId, f, String(item[f]), null, '残数0につき自動クリア', now, ctx.actor);
  db.prepare('UPDATE po_order_items SET remainder_disposition=NULL, next_expected_date=NULL, next_expected_qty=NULL, next_action_date=NULL WHERE id=?').run(itemId);
}

// ─── イベント登録 (唯一の書込経路) ───

/**
 * 数量イベントを登録する。BEGIN IMMEDIATE で残数を再検証してから書き込む。
 * @returns {eventId, remaining, orderClosed}
 * @throws 前提条件違反 (draft/legacy/closed への通常イベント、残数超過、逆仕訳不変条件違反 等)
 */
export function appendPoItemEvent({
  orderItemId, eventType, qty, source = null, reasonCode = null, note = null,
  inboundItemId = null, reversesId = null, effectiveDate = null,
  actorType = 'user', actor = null,
}) {
  const db = getDB();
  if (!ACTOR_TYPES.has(actorType)) throw new Error(`actor_type不正: ${actorType}`);

  const tx = db.transaction(() => {
    const item = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(orderItemId);
    if (!item) throw new Error(`明細が存在しません: ${orderItemId}`);
    const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(item.order_id);

    // 前提条件 (要件R5 H-1): issued かつ tracked のみ。closed へは reversal だけ許可
    if (order.status !== 'issued') throw new Error('draft/未確定の明細にはイベントを登録できません');
    if (!isTracked(order)) throw new Error('移行境界以前の発注 (legacy) は残数管理の対象外です');
    if (order.closed_at && eventType !== 'reversal') throw new Error('完了済みの発注です。訂正は逆仕訳で行ってください');

    let ev;
    if (eventType === 'reversal') {
      if (!reversesId) throw new Error('逆仕訳には対象イベントIDが必要です');
      const orig = db.prepare('SELECT * FROM po_item_events WHERE id=?').get(reversesId);
      if (!orig) throw new Error(`逆仕訳対象が存在しません: ${reversesId}`);
      if (orig.order_item_id !== orderItemId) throw new Error('逆仕訳対象が別の明細です');
      if (orig.event_type === 'reversal') throw new Error('逆仕訳自体は逆仕訳できません (正しい値を再登録してください)');
      const dup = db.prepare('SELECT id FROM po_item_events WHERE reverses_id=?').get(reversesId);
      if (dup) throw new Error('既に逆仕訳済みのイベントです');
      if (!note || !String(note).trim()) throw new Error('逆仕訳には訂正理由 (note) が必要です');
      // 全量のみ。数量・業務日付は元イベントから引き継ぐ (期間実績の整合)
      ev = {
        qty: orig.qty, source: null, reason_code: 'correction', note: String(note).trim(),
        inbound_item_id: null, reverses_id: reversesId, effective_date: orig.effective_date,
      };
    } else {
      const q = Number(qty);
      if (!Number.isInteger(q) || q <= 0) throw new Error(`数量が不正です: ${qty}`);
      const eff = effectiveDate || jstToday();
      if (!isYmd(eff)) throw new Error(`業務日付が不正です: ${effectiveDate}`);
      const bal = balanceOf(orderItemId, db);
      if (q > bal.remaining_qty) throw new Error(`残数超過: 残${bal.remaining_qty}に対して${q}は登録できません`);
      if (eventType === 'receipt') {
        if (!source) throw new Error('入荷にはsource (manual/logizard/migration) が必要です');
        if ((source === 'logizard') !== (inboundItemId != null)) throw new Error('logizard入荷のみ入庫明細参照が必要です');
        if (reasonCode != null) throw new Error('入荷に理由コードは付けられません');
      } else if (eventType === 'shortage') {
        if (!SHORTAGE_REASONS.has(reasonCode)) throw new Error(`減数の理由コードが不正です: ${reasonCode}`);
        if (reasonCode === 'other' && !(note && String(note).trim())) throw new Error('理由=その他 の減数には note が必要です');
        if (source != null || inboundItemId != null) throw new Error('減数にsource/入庫参照は付けられません');
      } else if (eventType === 'cancel') {
        if (source != null || reasonCode != null || inboundItemId != null) throw new Error('取消にsource/理由コード/入庫参照は付けられません');
      } else {
        throw new Error(`イベント種別が不正です: ${eventType}`);
      }
      ev = {
        qty: q, source, reason_code: reasonCode, note: note ? String(note).trim() || null : null,
        inbound_item_id: inboundItemId, reverses_id: null, effective_date: eff,
      };
    }

    const info = db.prepare(`INSERT INTO po_item_events
      (order_item_id, event_type, qty, source, reason_code, note, inbound_item_id, reverses_id, effective_date, recorded_at, actor_type, actor)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(orderItemId, eventType, ev.qty, ev.source, ev.reason_code, ev.note, ev.inbound_item_id, ev.reverses_id, ev.effective_date, nowIso(), actorType, actor);
    const eventId = Number(info.lastInsertRowid);

    // 逆仕訳後は残数が負になり得ないか再確認 (多重操作の防波堤。トリガ+UNIQUEの外側の保険)
    const after = balanceOf(orderItemId, db);
    if (after.remaining_qty < 0) throw new Error('内部整合性エラー: 残数が負になります');

    clearDispositionIfDone(db, orderItemId, { eventId, actor });
    const closure = recomputeClosure(db, item.order_id, { actorType, actor, action: eventType });
    return { eventId, remaining: after.remaining_qty, orderClosed: !!closure.closed };
  });
  return tx.immediate();
}

/** 逆仕訳のショートハンド */
export function reverseEvent(eventId, { note, actorType = 'user', actor = null } = {}) {
  const db = getDB();
  const orig = db.prepare('SELECT * FROM po_item_events WHERE id=?').get(eventId);
  if (!orig) throw new Error(`イベントが存在しません: ${eventId}`);
  return appendPoItemEvent({
    orderItemId: orig.order_item_id, eventType: 'reversal', reversesId: eventId,
    note, actorType, actor,
  });
}

/**
 * 手動クローズ (要件C-2/H-3): 残数分を理由=打切(cutoff)の減数として登録→共通再計算で closed 化。
 * note 必須。close_reason は cutoff の存在から 'manual' と導出される。
 */
export function manualCloseOrder(orderId, { note, actorType = 'user', actor = null } = {}) {
  if (!note || !String(note).trim()) throw new Error('手動クローズには理由 (note) が必要です');
  const db = getDB();
  const tx = db.transaction(() => {
    const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(orderId);
    if (!order) throw new Error(`発注が存在しません: ${orderId}`);
    if (order.status !== 'issued') throw new Error('確定済みの発注のみクローズできます');
    if (!isTracked(order)) throw new Error('legacy発注はクローズ対象外です');
    if (order.closed_at) throw new Error('既に完了済みです');
    const rows = orderBalances(orderId, db).filter(r => r.remaining_qty > 0);
    if (!rows.length) throw new Error('残数がありません (自動クローズ対象)');
    const now = nowIso();
    const eff = jstToday();
    const ins = db.prepare(`INSERT INTO po_item_events
      (order_item_id, event_type, qty, source, reason_code, note, inbound_item_id, reverses_id, effective_date, recorded_at, actor_type, actor)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const r of rows) {
      ins.run(r.order_item_id, 'shortage', r.remaining_qty, null, 'cutoff', String(note).trim(), null, null, eff, now, actorType, actor);
      clearDispositionIfDone(db, r.order_item_id, { actor });
    }
    audit(db, { actorType, actor, action: 'manual_close', resource: `order:${orderId}`,
      detail: { note: String(note).trim(), items: rows.map(r => ({ id: r.order_item_id, cutoff: r.remaining_qty })) } });
    const closure = recomputeClosure(db, orderId, { actorType, actor, action: 'manual_close' });
    if (!closure.closed) throw new Error('内部整合性エラー: クローズに失敗しました');
    return { closed: true, cutoffItems: rows.length };
  });
  return tx.immediate();
}

// ─── 納期・分納予定・disposition の更新 (現在値+履歴を同一txn) ───

const PLAN_FIELDS = ['promised_date', 'next_expected_date', 'next_expected_qty', 'next_action_date', 'remainder_disposition'];

/**
 * 明細の納期・残数取り扱いを更新する。列間規則 (要件H-6):
 *  - awaiting_delivery      → next_expected_date + next_expected_qty 必須、next_action_date は NULL
 *  - awaiting_confirmation  → next_action_date 必須、next_expected_* は NULL
 *  - disposition NULL       → next_* すべて NULL
 *  - next_expected_qty ≤ 現在残数
 */
export function setItemPlan(orderItemId, patch, { note = null, actorType = 'user', actor = null } = {}) {
  const db = getDB();
  const tx = db.transaction(() => {
    const item = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(orderItemId);
    if (!item) throw new Error(`明細が存在しません: ${orderItemId}`);
    const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(item.order_id);
    if (order.status !== 'issued' || !isTracked(order)) throw new Error('確定済み (tracked) の明細のみ更新できます');
    if (order.closed_at) throw new Error('完了済みの発注です');

    const next = { ...item };
    for (const f of PLAN_FIELDS) if (f in patch) next[f] = patch[f] == null ? null : patch[f];

    // 型・形式
    for (const f of ['promised_date', 'next_expected_date', 'next_action_date']) {
      if (next[f] != null && !isYmd(String(next[f]))) throw new Error(`${f} が日付 (YYYY-MM-DD) ではありません: ${next[f]}`);
    }
    if (next.next_expected_qty != null) {
      const q = Number(next.next_expected_qty);
      if (!Number.isInteger(q) || q <= 0) throw new Error(`next_expected_qty が不正です: ${next.next_expected_qty}`);
      const bal = balanceOf(orderItemId, db);
      if (q > bal.remaining_qty) throw new Error(`次回予定数量が残数 (${bal.remaining_qty}) を超えています`);
      next.next_expected_qty = q;
    }
    // 列間規則
    const d = next.remainder_disposition;
    if (d != null && d !== 'awaiting_delivery' && d !== 'awaiting_confirmation') throw new Error(`disposition が不正です: ${d}`);
    if (d === 'awaiting_delivery') {
      if (next.next_expected_date == null || next.next_expected_qty == null) throw new Error('分納待ちには次回入荷予定日と数量が必要です');
      next.next_action_date = null;
    } else if (d === 'awaiting_confirmation') {
      if (next.next_action_date == null) throw new Error('確認中には期限 (next_action_date) が必要です');
      next.next_expected_date = null; next.next_expected_qty = null;
    } else {
      next.next_expected_date = null; next.next_expected_qty = null; next.next_action_date = null;
    }

    // 変更履歴 (change_id で束ねる) → 現在値更新
    const changed = PLAN_FIELDS.filter(f => String(item[f] ?? '') !== String(next[f] ?? ''));
    if (!changed.length) return { changed: 0 };
    const changeId = `plan-${orderItemId}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    const now = nowIso();
    const ins = db.prepare('INSERT INTO po_item_history (order_item_id, change_id, field, old_value, new_value, note, created_at, actor) VALUES (?,?,?,?,?,?,?,?)');
    for (const f of changed) ins.run(orderItemId, changeId, f, item[f] == null ? null : String(item[f]), next[f] == null ? null : String(next[f]), note, now, actor);
    db.prepare(`UPDATE po_order_items SET promised_date=?, next_expected_date=?, next_expected_qty=?, next_action_date=?, remainder_disposition=? WHERE id=?`)
      .run(next.promised_date, next.next_expected_date, next.next_expected_qty, next.next_action_date, next.remainder_disposition, orderItemId);
    return { changed: changed.length, changeId };
  });
  return tx.immediate();
}

// ─── 整合性検査 (3層のうちの全件検査。対象PO限定は orderId 指定) ───

/**
 * 台帳の不変条件を検査し、違反リストを返す (空配列=健全)。
 * 検出時、対象商品の発注提案からの隔離は呼び出し側 (P13b以降) が行う。
 */
export function checkLedgerIntegrity({ orderId = null } = {}) {
  const db = getDB();
  const issues = [];
  const boundary = getSetting('tracking_started_at');
  const scope = orderId ? 'AND o.id = ?' : '';
  const args = orderId ? [orderId] : [];

  // 1. 負残
  for (const r of db.prepare(`
    SELECT b.order_item_id, b.remaining_qty FROM v_po_item_balance b
    JOIN po_orders o ON o.id = b.order_id WHERE b.remaining_qty < 0 ${scope}`).all(...args)) {
    issues.push({ kind: 'negative_remaining', itemId: r.order_item_id, detail: r.remaining_qty });
  }
  // 2. closed なのに残>0 / 全明細残0なのにオープン (tracked のみ)
  if (boundary) {
    for (const r of db.prepare(`
      SELECT o.id, o.closed_at, MIN(b.remaining_qty) AS minr, MAX(b.remaining_qty) AS maxr
      FROM po_orders o JOIN v_po_item_balance b ON b.order_id = o.id
      WHERE o.status='issued' AND o.issued_at >= ? ${scope}
      GROUP BY o.id`).all(boundary, ...args)) {
      if (r.closed_at && r.maxr > 0) issues.push({ kind: 'closed_but_remaining', orderId: r.id });
      if (!r.closed_at && r.maxr === 0) issues.push({ kind: 'done_but_open', orderId: r.id });
    }
    // 3. 境界後 issued なのに tracking_mode 未設定 (発行経路の取りこぼし検知)
    for (const r of db.prepare(`
      SELECT o.id FROM po_orders o
      WHERE o.status='issued' AND o.issued_at >= ? AND (o.tracking_mode IS NULL OR o.po_number IS NULL) ${scope}`).all(boundary, ...args)) {
      issues.push({ kind: 'tracked_missing_attrs', orderId: r.id });
    }
  }
  // 4. disposition 残骸 (残0なのに awaiting_*) / 列間規則違反
  for (const r of db.prepare(`
    SELECT i.id, i.remainder_disposition AS d, i.next_expected_date AS ned, i.next_expected_qty AS neq, i.next_action_date AS nad,
           b.remaining_qty AS rem
    FROM po_order_items i JOIN v_po_item_balance b ON b.order_item_id = i.id
    JOIN po_orders o ON o.id = i.order_id
    WHERE (i.remainder_disposition IS NOT NULL OR i.next_expected_date IS NOT NULL OR i.next_action_date IS NOT NULL) ${scope}`).all(...args)) {
    if (r.rem === 0) issues.push({ kind: 'stale_disposition', itemId: r.id });
    else if (r.d === 'awaiting_delivery' && (r.ned == null || r.neq == null)) issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'awaiting_deliveryで次回予定なし' });
    else if (r.d === 'awaiting_confirmation' && r.nad == null) issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'awaiting_confirmation 期限なし' });
    else if (r.d == null && (r.ned != null || r.nad != null)) issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'dispositionなしでnext_*あり' });
  }
  return issues;
}
