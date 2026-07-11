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
import { createHash } from 'crypto';
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

/** actor_type の入口検証 (全書込関数で共通。SQLite CHECK由来の不親切なエラーにしない) */
function validateActor(actorType) {
  if (!ACTOR_TYPES.has(actorType)) throw new Error(`actor_type不正: ${actorType}`);
}

/** キー順に依存しない正規化JSON (冪等ハッシュ用。同内容ならキー順が違っても同一ハッシュ) */
export function stableStringify(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(stableStringify).join(',')}]`;
  const keys = Object.keys(v).sort();
  return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify(v[k])}`).join(',')}}`;
}

// ─── 設定 (現在値テーブル。変更は監査ログと同一txn) ───

// setSetting で変更できるキーのホワイトリスト (tracking_started_at は含めない=初回確定後は不変。
// DB側にも UPDATE/DELETE 拒否トリガあり)
const SETTABLE_KEYS = new Set(['backorder_source', 'email_mode', 'email_dryrun_to', 'email_subject_template', 'email_body_template']);

export function getSetting(key) {
  const r = getDB().prepare('SELECT value FROM po_settings WHERE key=?').get(key);
  return r ? r.value : null;
}

export function setSetting(key, value, { actor = null, actorType = 'user', reason = null } = {}) {
  validateActor(actorType);
  if (!SETTABLE_KEYS.has(key)) throw new Error(`設定キーが不正です (変更可能: ${[...SETTABLE_KEYS].join(', ')}): ${key}`);
  if (key === 'backorder_source' && value !== 'ne' && value !== 'app') throw new Error(`backorder_source は ne/app のみ: ${value}`);
  if (key === 'email_mode' && value !== 'dry_run' && value !== 'live') throw new Error(`email_mode は dry_run/live のみ: ${value}`);
  if (key === 'email_dryrun_to' && String(value).trim() && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value).trim())) {
    throw new Error(`email_dryrun_to がメールアドレスではありません: ${value}`);
  }
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
 * 移行境界の初期化。最初の発注確定トランザクションの中で一度だけ確定する (issueOrderのtxn内から呼ぶ。
 * 発行が失敗すれば境界もロールバックされ、成功した最初の発行と境界が同一コミットになる)。
 * 以後この時刻以降に issued されたPOが tracked。確定後は不変 (DBトリガでUPDATE/DELETE拒否)。
 * @param {string} at - 境界とする時刻 (発行時刻と同じ値を共有する)
 */
export function ensureTrackingStarted(at = nowIso()) {
  const db = getDB();
  db.transaction(() => {
    const cur = db.prepare("SELECT value FROM po_settings WHERE key='tracking_started_at'").get();
    if (cur) return;
    db.prepare("INSERT INTO po_settings (key, value, effective_at, changed_by, reason) VALUES ('tracking_started_at', ?, ?, 'system', 'P13a初回発行で境界確定')")
      .run(at, at);
    audit(db, { actorType: 'system', actor: null, action: 'tracking_boundary_set', resource: 'setting:tracking_started_at', detail: { value: at } });
  })();
  return getSetting('tracking_started_at');
}

// ─── write API の冪等化 (Codex P13a-R1 High-4) ───

/**
 * Idempotency-Key つきでコマンドを実行する。キー登録・業務処理・結果保存を同一トランザクションで行うため、
 * コミット前に落ちれば痕跡が残らず、コミット後の再送は保存済み結果をそのまま返す。
 *  - 同一キー+同一payload → 保存済み結果を返す (replay=true)
 *  - 同一キー+異なるpayload → 409 (err.status)
 * キー無しなら fn() をそのまま実行。
 */
export function withCommand({ idempotencyKey = null, payload = null }, fn) {
  if (!idempotencyKey) return { replay: false, result: fn() };
  if (String(idempotencyKey).length > 200) { const e = new Error('Idempotency-Key が長すぎます (200文字以内)'); e.status = 400; throw e; }
  const db = getDB();
  // キー順に依存しない正規化JSONでハッシュ (同内容の再送をキー順違いで409にしない)
  const hash = createHash('sha256').update(stableStringify(payload ?? null)).digest('hex');
  const tx = db.transaction(() => {
    const existing = db.prepare('SELECT * FROM po_commands WHERE idempotency_key=?').get(idempotencyKey);
    if (existing) {
      if (existing.request_hash !== hash) { const e = new Error('Idempotency-Key が異なる内容で再利用されています'); e.status = 409; throw e; }
      if (existing.result_json == null) { const e = new Error('同一キーの処理結果がありません (前回失敗)。新しいキーで再実行してください'); e.status = 409; throw e; }
      return { replay: true, result: JSON.parse(existing.result_json) };
    }
    db.prepare('INSERT INTO po_commands (idempotency_key, request_hash, created_at) VALUES (?,?,?)').run(idempotencyKey, hash, nowIso());
    const result = fn();
    db.prepare('UPDATE po_commands SET result_json=? WHERE idempotency_key=?').run(JSON.stringify(result ?? null), idempotencyKey);
    return { replay: false, result };
  });
  return tx.immediate();
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
 * ヘッダ閉鎖状態の遷移監査。closed_at 自体はイベントINSERTのAFTERトリガ (trg_po_events_closure) が
 * DB側で再計算する (直接SQLでも乖離しない)。ここでは遷移を検知して監査ログに残すだけ。
 */
function auditClosureChange(db, orderId, prevClosedAt, ctx) {
  const order = db.prepare('SELECT closed_at FROM po_orders WHERE id=?').get(orderId);
  const nowClosed = order.closed_at != null;
  if (!prevClosedAt && nowClosed) {
    const rows = orderBalances(orderId, db);
    audit(db, { actorType: ctx.actorType, actor: ctx.actor, action: 'order_closed', resource: `order:${orderId}`,
      detail: { reason: rows.some(r => r.cutoff_qty > 0) ? 'manual' : 'completed' } });
  } else if (prevClosedAt && !nowClosed) {
    audit(db, { actorType: ctx.actorType, actor: ctx.actor, action: 'order_reopened', resource: `order:${orderId}`,
      detail: { by: ctx.action || 'reversal' } });
  }
  return { closed: nowClosed };
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
    const closure = auditClosureChange(db, item.order_id, order.closed_at, { actorType, actor, action: eventType });
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
  validateActor(actorType);
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
    const closure = auditClosureChange(db, orderId, order.closed_at, { actorType, actor, action: 'manual_close' });
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
  validateActor(actorType);
  const db = getDB();
  const tx = db.transaction(() => {
    const item = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(orderItemId);
    if (!item) throw new Error(`明細が存在しません: ${orderItemId}`);
    const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(item.order_id);
    if (order.status !== 'issued' || !isTracked(order)) throw new Error('確定済み (tracked) の明細のみ更新できます');
    if (order.closed_at) throw new Error('完了済みの発注です');

    const next = { ...item };
    for (const f of PLAN_FIELDS) if (f in patch) next[f] = patch[f] == null ? null : patch[f];

    // 列間規則の正規化を先に行う (確認中へ切替時は next_expected_* を落としてから検証する。
    // 逆順だと切替前の古い next_expected_qty が残数チェックに引っかかる)
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
    // 型・形式 (正規化後の最終値に対して検証)
    for (const f of ['promised_date', 'next_expected_date', 'next_action_date']) {
      if (next[f] != null && !isYmd(String(next[f]))) throw new Error(`${f} が日付 (YYYY-MM-DD) ではありません: ${next[f]}`);
    }
    // 残数0の明細に扱い/予定は設定できない (公開APIから stale_disposition を作らせない、Codex P13b-R2 High-2)。
    // 回答納期 (promised_date) だけの更新は残数0でも許可する (履歴訂正用途)
    const bal = balanceOf(orderItemId, db);
    if (bal.remaining_qty === 0 && (next.remainder_disposition != null || next.next_expected_date != null
        || next.next_expected_qty != null || next.next_action_date != null)) {
      throw new Error('残数0の明細には残数の扱い・次回予定を設定できません');
    }
    if (next.next_expected_qty != null) {
      const q = Number(next.next_expected_qty);
      if (!Number.isInteger(q) || q <= 0) throw new Error(`next_expected_qty が不正です: ${next.next_expected_qty}`);
      if (q > bal.remaining_qty) throw new Error(`次回予定数量が残数 (${bal.remaining_qty}) を超えています`);
      next.next_expected_qty = q;
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

// ─── 発注残一覧 (P13b) ───

/**
 * tracked な発注の一覧+明細残数+要対応フラグ+サマリを返す (発注残ページ・ダッシュボード用)。
 * 遅延判定の基準日 = 回答納期 (promised) → 希望納期 (requested) の優先順。
 */
export function listBackorders() {
  const db = getDB();
  const boundary = getSetting('tracking_started_at');
  const summary = { openOrders: 0, remainingQty: 0, knownAmount: 0, unknownCostItems: 0, overdueItems: 0, attentionItems: 0 };
  if (!boundary) return { boundary: null, orders: [], summary };
  const today = jstToday();
  const orders = db.prepare(`
    SELECT id, supplier_code, supplier_name, po_number, note, requested_date, issued_at, closed_at, origin, send_blocked
    FROM po_orders WHERE status='issued' AND issued_at >= ? ORDER BY (closed_at IS NULL) DESC, issued_at DESC`).all(boundary);
  const itemsStmt = db.prepare(`
    SELECT i.id, i.product_code, i.product_name, i.qty, i.unit_cost,
           i.requested_date, i.promised_date, i.next_expected_date, i.next_expected_qty, i.next_action_date, i.remainder_disposition,
           b.received_qty, b.shortage_qty, b.cancelled_qty, b.cutoff_qty, b.remaining_qty,
           (SELECT COUNT(*) FROM po_item_events e WHERE e.order_item_id = i.id) AS event_count
    FROM po_order_items i JOIN v_po_item_balance b ON b.order_item_id = i.id
    WHERE i.order_id = ? ORDER BY i.id`);
  const out = [];
  for (const o of orders) {
    const items = itemsStmt.all(o.id).map(i => {
      const due = i.promised_date || i.requested_date || null;
      return {
        ...i,
        due,
        flags: {
          overdue: i.remaining_qty > 0 && !!due && due < today,
          unanswered: i.remaining_qty > 0 && !i.promised_date,
          needsDisposition: i.remaining_qty > 0 && i.event_count > 0 && !i.remainder_disposition,
          confirmOverdue: i.remainder_disposition === 'awaiting_confirmation' && !!i.next_action_date && i.next_action_date < today,
        },
      };
    });
    const open = !o.closed_at;
    const remainingQty = items.reduce((s, i) => s + i.remaining_qty, 0);
    // 発注残金額 = 明細ごとに ROUND(単価×残数) してから合計 (要件v8 F-1。単価は整数円運用のため実質差は出ない)
    const knownAmount = items.reduce((s, i) => s + (i.unit_cost != null ? Math.round(i.unit_cost * i.remaining_qty) : 0), 0);
    const unknownCostItems = items.filter(i => i.unit_cost == null && i.remaining_qty > 0).length;
    const overdueItems = items.filter(i => i.flags.overdue).length;
    // 要対応 = 遅延 / 納期未回答 / 残数の扱い未選択 / 確認期限超過 (バッジ表示と同じ集合、要件F-3/F-4)
    const attentionItems = items.filter(i => i.flags.overdue || i.flags.unanswered || i.flags.needsDisposition || i.flags.confirmOverdue).length;
    if (open) {
      summary.openOrders++;
      summary.remainingQty += remainingQty;
      summary.knownAmount += knownAmount;
      summary.unknownCostItems += unknownCostItems;
      summary.overdueItems += overdueItems;
      summary.attentionItems += attentionItems;
    }
    out.push({
      id: o.id, poNumber: o.po_number, supplierCode: o.supplier_code, supplierName: o.supplier_name,
      note: o.note, requestedDate: o.requested_date, issuedAt: o.issued_at, closedAt: o.closed_at,
      origin: o.origin, open, sendBlocked: !!o.send_blocked,
      closeReason: o.closed_at ? (items.some(i => i.cutoff_qty > 0) ? 'manual' : 'completed') : null,
      remainingQty, knownAmount, unknownCostItems, overdueItems, attentionItems,
      items,
    });
  }
  return { boundary, orders: out, summary };
}

/** 明細1件のイベント系列 (表示用。逆仕訳済み・逆仕訳自体も含めて時系列で返す) */
export function listItemEvents(orderItemId) {
  const db = getDB();
  return db.prepare(`
    SELECT e.*, (SELECT r.id FROM po_item_events r WHERE r.reverses_id = e.id) AS reversed_by
    FROM po_item_events e WHERE e.order_item_id = ? ORDER BY e.id`).all(orderItemId);
}

// ─── 整合性検査 (3層のうちの全件検査。対象PO限定は orderId 指定) ───

/**
 * 台帳の不変条件を検査する。
 * @returns {issues, warnings}
 *   issues   = 不変条件違反 (あってはならない状態。healthy判定に使う)
 *   warnings = 運用上の要対応 (違反ではない。例: 部分消込後に残数の扱い未選択)
 * 検出時、対象商品の発注提案からの隔離は呼び出し側 (P13b以降) が行う。
 */
export function checkLedgerIntegrity({ orderId = null } = {}) {
  const db = getDB();
  const issues = [];
  const warnings = [];
  const boundary = getSetting('tracking_started_at');
  const scope = orderId ? 'AND o.id = ?' : '';
  const args = orderId ? [orderId] : [];

  // 1. 負残
  for (const r of db.prepare(`
    SELECT b.order_item_id, b.remaining_qty FROM v_po_item_balance b
    JOIN po_orders o ON o.id = b.order_id WHERE b.remaining_qty < 0 ${scope}`).all(...args)) {
    issues.push({ kind: 'negative_remaining', itemId: r.order_item_id, detail: r.remaining_qty });
  }
  // 1b. 境界値そのものの形式検証 (不正値が入ると全PO判定が壊れるため最優先で報告)
  if (boundary && !(boundary.length === 24 && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(boundary))) {
    issues.push({ kind: 'invalid_boundary', detail: boundary });
  }
  // 2. draft なのに closed_at / issued なのに発行属性の欠落・形式不正
  const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
  for (const r of db.prepare(`
    SELECT o.id, o.status, o.closed_at, o.po_number, o.issued_at, o.tracking_mode FROM po_orders o
    WHERE 1=1 ${scope}`).all(...args)) {
    if (r.status === 'draft' && r.closed_at != null) issues.push({ kind: 'draft_with_closed_at', orderId: r.id });
    if (r.status === 'issued' && boundary && r.issued_at != null && r.issued_at >= boundary) {
      if (r.tracking_mode !== 'tracked' || !r.po_number || !/^PO-\d{4}-\d{4,}$/.test(r.po_number)
          || !ISO_RE.test(String(r.issued_at)) || !isYmd(String(r.issued_at).slice(0, 10))) {
        issues.push({ kind: 'tracked_missing_attrs', orderId: r.id });
      }
    }
    // GLOBは字面のみのため、実在時刻もここで検査 (2026-99-99 等)
    if (r.closed_at != null && !(ISO_RE.test(String(r.closed_at)) && isYmd(String(r.closed_at).slice(0, 10)))) {
      issues.push({ kind: 'invalid_timestamp', orderId: r.id, detail: `closed_at=${r.closed_at}` });
    }
    if (r.status === 'issued' && r.issued_at != null && !isYmd(String(r.issued_at).slice(0, 10))) {
      issues.push({ kind: 'invalid_timestamp', orderId: r.id, detail: `issued_at=${r.issued_at}` });
    }
  }
  // 2b. 逆仕訳の不変条件の検算 (通常はトリガ+UNIQUEが守る。メンテモード・移行後の検査用)
  for (const r of db.prepare(`
    SELECT v.id, v.reverses_id, v.order_item_id, v.qty, v.effective_date,
           o2.id AS orig_id, o2.event_type AS orig_type, o2.order_item_id AS orig_item, o2.qty AS orig_qty, o2.effective_date AS orig_eff
    FROM po_item_events v
    LEFT JOIN po_item_events o2 ON o2.id = v.reverses_id
    JOIN po_order_items i ON i.id = v.order_item_id JOIN po_orders o ON o.id = i.order_id
    WHERE v.event_type = 'reversal' ${scope}`).all(...args)) {
    if (!r.orig_id || r.orig_type === 'reversal' || r.orig_item !== r.order_item_id
        || r.orig_qty !== r.qty || r.orig_eff !== r.effective_date) {
      issues.push({ kind: 'invalid_reversal', eventId: r.id });
    }
  }
  for (const r of db.prepare(`
    SELECT v.reverses_id, COUNT(*) AS n FROM po_item_events v
    JOIN po_order_items i ON i.id = v.order_item_id JOIN po_orders o ON o.id = i.order_id
    WHERE v.event_type = 'reversal' ${scope}
    GROUP BY v.reverses_id HAVING COUNT(*) > 1`).all(...args)) {
    issues.push({ kind: 'duplicate_reversal', detail: { reversesId: r.reverses_id, count: r.n } });
  }
  if (boundary) {
    // 3. closed なのに残>0 / 全明細残0なのにオープン (tracked のみ)
    for (const r of db.prepare(`
      SELECT o.id, o.closed_at, MAX(b.remaining_qty) AS maxr
      FROM po_orders o JOIN v_po_item_balance b ON b.order_id = o.id
      WHERE o.status='issued' AND o.issued_at >= ? ${scope}
      GROUP BY o.id`).all(boundary, ...args)) {
      if (r.closed_at && r.maxr > 0) issues.push({ kind: 'closed_but_remaining', orderId: r.id });
      if (!r.closed_at && r.maxr === 0) issues.push({ kind: 'done_but_open', orderId: r.id });
    }
    // 4. legacy/draft にイベントが存在 (トリガの防波堤の検算)。issued_at IS NULL も未追跡として検出
    for (const r of db.prepare(`
      SELECT DISTINCT o.id FROM po_item_events e
      JOIN po_order_items i ON i.id = e.order_item_id JOIN po_orders o ON o.id = i.order_id
      WHERE (o.status <> 'issued' OR o.issued_at IS NULL OR o.issued_at < ?) ${scope}`).all(boundary, ...args)) {
      issues.push({ kind: 'events_on_untracked', orderId: r.id });
    }
  }
  // 4b. イベント意味論の検算 (テーブルCHECKと同値になるよう網羅。メンテモード・移行後検査用)
  for (const r of db.prepare(`
    SELECT e.id, e.event_type, e.source, e.reason_code, e.note, e.inbound_item_id, e.qty FROM po_item_events e
    JOIN po_order_items i ON i.id = e.order_item_id JOIN po_orders o ON o.id = i.order_id WHERE 1=1 ${scope}`).all(...args)) {
    const noteOk = !!(r.note && String(r.note).trim());
    const bad =
      !['receipt', 'shortage', 'cancel', 'reversal'].includes(r.event_type) ||
      !(Number.isInteger(r.qty) && r.qty > 0) ||
      (r.source != null && !['manual', 'logizard', 'migration'].includes(r.source)) ||
      (r.event_type === 'receipt' && (!r.source || r.reason_code != null || ((r.source === 'logizard') !== (r.inbound_item_id != null)))) ||
      (r.event_type === 'shortage' && (!['supplier_shortage', 'own_decision', 'cutoff', 'other'].includes(r.reason_code) || r.source != null || r.inbound_item_id != null || (r.reason_code === 'other' && !noteOk))) ||
      (r.event_type === 'cancel' && (r.reason_code != null || r.source != null || r.inbound_item_id != null)) ||
      (r.event_type === 'reversal' && (r.reason_code !== 'correction' || !noteOk || r.source != null || r.inbound_item_id != null));
    if (bad) issues.push({ kind: 'invalid_event_semantics', eventId: r.id, detail: r.event_type });
  }
  // 4c. tracked issued なのに明細ゼロ (直接INSERT等の異常系。JOIN v_po_item_balance では拾えない)
  if (boundary) {
    for (const r of db.prepare(`
      SELECT o.id FROM po_orders o
      WHERE o.status='issued' AND o.issued_at >= ?
        AND NOT EXISTS (SELECT 1 FROM po_order_items i WHERE i.order_id = o.id) ${scope}`).all(boundary, ...args)) {
      issues.push({ kind: 'issued_without_items', orderId: r.id });
    }
  }
  // 5. disposition/next_* の列間規則 (next_expected_qty 単独残骸も対象に含める)
  for (const r of db.prepare(`
    SELECT i.id, i.remainder_disposition AS d, i.next_expected_date AS ned, i.next_expected_qty AS neq, i.next_action_date AS nad,
           b.remaining_qty AS rem
    FROM po_order_items i JOIN v_po_item_balance b ON b.order_item_id = i.id
    JOIN po_orders o ON o.id = i.order_id
    WHERE (i.remainder_disposition IS NOT NULL OR i.next_expected_date IS NOT NULL
           OR i.next_expected_qty IS NOT NULL OR i.next_action_date IS NOT NULL) ${scope}`).all(...args)) {
    if (r.d != null && r.d !== 'awaiting_delivery' && r.d !== 'awaiting_confirmation') {
      issues.push({ kind: 'disposition_rule', itemId: r.id, detail: `不正なdisposition: ${r.d}` });
    } else if (r.rem === 0) {
      issues.push({ kind: 'stale_disposition', itemId: r.id });
    } else if (r.d === 'awaiting_delivery' && (r.ned == null || r.neq == null || r.nad != null)) {
      issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'awaiting_delivery: 次回予定なし or 期限が残存' });
    } else if (r.d === 'awaiting_confirmation' && (r.nad == null || r.ned != null || r.neq != null)) {
      issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'awaiting_confirmation: 期限なし or 次回予定が残存' });
    } else if (r.d == null) {
      issues.push({ kind: 'disposition_rule', itemId: r.id, detail: 'dispositionなしでnext_*あり' });
    } else if (r.neq != null && r.neq > r.rem) {
      issues.push({ kind: 'disposition_rule', itemId: r.id, detail: `次回予定数量${r.neq} > 残数${r.rem}` });
    }
  }
  // 5b. 納期系日付の実在検査 (GLOB/トリガは字面のみのため)
  for (const r of db.prepare(`
    SELECT i.id, i.promised_date AS pd, i.next_expected_date AS ned, i.next_action_date AS nad
    FROM po_order_items i JOIN po_orders o ON o.id = i.order_id
    WHERE (i.promised_date IS NOT NULL OR i.next_expected_date IS NOT NULL OR i.next_action_date IS NOT NULL) ${scope}`).all(...args)) {
    for (const [f, v] of [['promised_date', r.pd], ['next_expected_date', r.ned], ['next_action_date', r.nad]]) {
      if (v != null && !isYmd(String(v))) issues.push({ kind: 'invalid_date', itemId: r.id, detail: `${f}=${v}` });
    }
  }
  // 6. 実在しない業務日付 (GLOBは形式のみのため)
  for (const r of db.prepare(`
    SELECT e.id, e.effective_date FROM po_item_events e
    JOIN po_order_items i ON i.id = e.order_item_id JOIN po_orders o ON o.id = i.order_id WHERE 1=1 ${scope}`).all(...args)) {
    if (!isYmd(r.effective_date)) issues.push({ kind: 'invalid_effective_date', eventId: r.id, detail: r.effective_date });
  }
  // 6b. 訂正競合: 訂正版で無効化された入庫行に有効な割当が残っている (逆仕訳→再割当で解消するまで違反扱い)
  for (const r of db.prepare(`
    SELECT x.id, COALESCE(SUM(e.qty), 0) AS alloc
    FROM po_inbound_items x
    JOIN po_item_events e ON e.inbound_item_id = x.id AND e.event_type = 'receipt'
      AND NOT EXISTS (SELECT 1 FROM po_item_events rv WHERE rv.reverses_id = e.id)
    JOIN po_order_items i ON i.id = e.order_item_id JOIN po_orders o ON o.id = i.order_id
    WHERE x.superseded = 1 ${scope}
    GROUP BY x.id HAVING alloc > 0`).all(...args)) {
    issues.push({ kind: 'superseded_with_alloc', inboundItemId: r.id, detail: `割当${r.alloc}が無効化行に残存` });
  }
  // 7. [警告] 部分消込が始まった残数の扱いが未選択 (逆仕訳で再オープンした場合も含む要対応リスト)
  if (boundary) {
    for (const r of db.prepare(`
      SELECT i.id, b.remaining_qty AS rem FROM po_order_items i
      JOIN v_po_item_balance b ON b.order_item_id = i.id
      JOIN po_orders o ON o.id = i.order_id
      WHERE o.status='issued' AND o.closed_at IS NULL AND o.issued_at >= ?
        AND b.remaining_qty > 0 AND i.remainder_disposition IS NULL
        AND EXISTS (SELECT 1 FROM po_item_events e WHERE e.order_item_id = i.id) ${scope}`).all(boundary, ...args)) {
      warnings.push({ kind: 'remainder_without_disposition', itemId: r.id, detail: `残${r.rem}の扱い (分納待ち/減数/確認中) が未選択` });
    }
  }
  return { issues, warnings };
}
