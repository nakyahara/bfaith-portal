/**
 * 梱包資材の表示・現場登録 (要件定義 = AI_reference『梱包資材表示_調査メモ/要件定義_20260823.md』v1.7)
 *
 * 責務:
 *   - 判定: hidden (分類 hide_card) → held (④依頼中) → header (伝票指定) → rule (登録ルール)
 *           → candidates (分類別候補) → unknown。配送種別は print_header1 の完全一致辞書から導出
 *   - 登録/変更/取り消し: version CAS + op_id 冪等 (request_hash 再現) + HMAC undo token
 *   - 変更通知 outbox: at-least-once (claim_token 所有権 CAS・上限 = attempt<10 かつ 48h)
 *   - 表示観測ログ (pk_pack_material_views): 未レビュー値の検出・採用実績。完了時にスナップショット固定
 *
 * 設計判断:
 *   - delivery_method は判定に使わない (実データで「箱 陸便…」が AES/宅急便50/発払いを包含し区別不能。
 *     print_header1 が全伝票にあり 7 値で配送種別と 1:1 — 2026-08-24 実測)。生値は観測ログに保存のみ
 *   - AES はバッチ引当分類の aes_kind (分類マスタ完全一致) で aes_mail / aes_other に分岐。
 *     未知分類は unknown (fail-closed — 誤った候補提示・別キー登録を防ぐ)
 */
import crypto from 'crypto';
import { getDB, utcNow } from './db.js';
import { PackError } from './service.js';

/** 判定後の配送種別 (ルールキーの値域)。 */
export const DELIVERY_CODES = Object.freeze([
  'nekopos', 'yupacket_puff', 'teikeigai', 'letterpack',
  'takkyubin50', 'takkyubin60plus', 'aes_mail', 'aes_other',
]);
/** 辞書に保存する基底コード ('aes' は判定時に分岐)。 */
export const BASE_DELIVERY_CODES = Object.freeze([
  'nekopos', 'yupacket_puff', 'teikeigai', 'letterpack',
  'takkyubin50', 'takkyubin60plus', 'aes', 'unsupported',
]);

export const UNDO_SEC = Math.max(5, Number(process.env.PACKING_MATERIAL_UNDO_SEC) || 15);

// undo token の HMAC 鍵。未設定ならプロセス毎ランダム (再起動で undo 不能になるだけ・登録機能は無傷)
let UNDO_SECRET = process.env.PACKING_MATERIAL_UNDO_SECRET || '';
if (!UNDO_SECRET) {
  UNDO_SECRET = crypto.randomBytes(32).toString('hex');
  console.warn('[packing-materials] PACKING_MATERIAL_UNDO_SECRET 未設定 → プロセス毎のランダム鍵 (再起動を跨ぐ undo 再現は不可)');
}

/** 正規化: NFKC → trim → 連続空白を1つに。辞書キー (header/分類/配送方法生値) は全てこれを通す。 */
export function normalizeKeyText(s) {
  if (s == null) return '';
  return String(s).normalize('NFKC').trim().replace(/\s+/g, ' ');
}

/** SKU 1個のエンコード (combo_key 用)。小文字化し、[a-z0-9._-] 以外は %XX。 */
function encodeSku(sku) {
  const t = String(sku ?? '').trim().toLowerCase();
  if (!t) return null;
  return t.replace(/[^a-z0-9._-]/g, (ch) => Array.from(Buffer.from(ch, 'utf8'))
    .map((b) => `%${b.toString(16).toUpperCase().padStart(2, '0')}`).join(''));
}

/**
 * 商品構成キー。単品もアソートも同形式 'v1|sku*qty|…' (同一SKU合算・sku昇順)。
 * 空SKU・数量が正整数でない明細があれば null (登録不可 = line_invalid)。
 */
export function comboKeyOf(lines) {
  const agg = new Map();
  const names = new Map();
  for (const l of lines || []) {
    const enc = encodeSku(l.sku);
    const qty = Number(l.qty);
    if (!enc || !Number.isInteger(qty) || qty < 1) return null;
    agg.set(enc, (agg.get(enc) || 0) + qty);
    if (!names.has(enc)) names.set(enc, String(l.product_name || l.name || '').slice(0, 120));
  }
  if (agg.size === 0) return null;
  const keys = [...agg.keys()].sort();
  return {
    comboKey: 'v1|' + keys.map((k) => `${k}*${agg.get(k)}`).join('|'),
    comboDetail: JSON.stringify(keys.map((k) => ({ sku: k, qty: agg.get(k), name: names.get(k) || '' }))),
  };
}

/** undo token = HMAC(鍵, 'material-undo:<event_id>')。DB には保存しない (op_id 再送でも再現できる)。 */
export function undoTokenFor(eventId) {
  return crypto.createHmac('sha256', UNDO_SECRET).update(`material-undo:${eventId}`).digest('base64url');
}

function timingSafeEq(a, b) {
  const ba = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

/** op_id 冪等用: 正規化した要求ペイロードのハッシュ。 */
function requestHashOf(obj) {
  const norm = JSON.stringify(obj, Object.keys(obj).sort());
  return crypto.createHash('sha256').update(norm).digest('hex');
}

function isoPlusSec(iso, sec) {
  return new Date(Date.parse(iso) + sec * 1000).toISOString().slice(0, 19) + 'Z';
}

// ─── 判定 ───

/**
 * 配送種別の導出 (§3.1)。print_header1 → header_map (完全一致) → base → AES は分類で分岐。
 * @returns {{headerRaw:string, base:string|null, code:string, headerMaterial:string|null, reason:string|null}}
 *   code は DELIVERY_CODES のいずれか / 'unsupported' / 'unknown'
 */
export function resolveDelivery(db, printHeader1, hikiateClass) {
  const headerRaw = normalizeKeyText(printHeader1);
  if (!headerRaw) return { headerRaw, base: null, code: 'unknown', headerMaterial: null, reason: 'no_header' };
  const row = db.prepare(
    'SELECT base_delivery_code, material_code FROM pk_pack_header_map WHERE header_value = ?'
  ).get(headerRaw);
  if (!row) return { headerRaw, base: null, code: 'unknown', headerMaterial: null, reason: 'unreviewed_header' };
  const base = row.base_delivery_code;
  if (base === 'unsupported') {
    return { headerRaw, base, code: 'unsupported', headerMaterial: row.material_code || null, reason: 'unsupported' };
  }
  if (base !== 'aes') return { headerRaw, base, code: base, headerMaterial: row.material_code || null, reason: null };
  // AES: 分類マスタの aes_kind で分岐 (fail-closed — 未知は unknown)
  const cls = db.prepare('SELECT aes_kind FROM pk_pack_classes WHERE class_value = ?')
    .get(normalizeKeyText(hikiateClass));
  if (cls?.aes_kind === 'mail') return { headerRaw, base, code: 'aes_mail', headerMaterial: row.material_code || null, reason: null };
  if (cls?.aes_kind === 'other') return { headerRaw, base, code: 'aes_other', headerMaterial: row.material_code || null, reason: null };
  return { headerRaw, base, code: 'unknown', headerMaterial: null, reason: 'aes_class_unreviewed' };
}

function materialRow(db, code) {
  if (!code) return null;
  return db.prepare('SELECT code, name, color, image_file, is_active FROM pk_pack_materials WHERE code = ?').get(code) || null;
}

function toMaterialInfo(row) {
  if (!row) return null;
  return {
    code: row.code, name: row.name, color: row.color || null,
    imageUrl: row.image_file ? `/apps/packing/materials/${row.image_file}` : null,
    inactive: !row.is_active,
  };
}

/** 分類の有効候補 (active JOIN 後・sort順)。 */
export function classCandidates(db, classValue) {
  return db.prepare(`
    SELECT m.code, m.name, m.color, m.image_file, m.is_active
    FROM pk_pack_class_materials c JOIN pk_pack_materials m ON m.code = c.material_code
    WHERE c.class_value = ? AND m.is_active = 1
    ORDER BY c.sort_order, m.sort_order, m.code
  `).all(normalizeKeyText(classValue)).map(toMaterialInfo);
}

/**
 * 伝票1枚の資材判定 (§3)。表示のたびに計算する (結果は保存しない)。
 * slip: { seq, status, delivery_method, print_header1, lines: [{sku, qty, product_name}] }
 */
export function judgeSlip(db, { batchId, slip, hikiateClass }) {
  const classValue = normalizeKeyText(hikiateClass);
  const combo = comboKeyOf(slip.lines);
  const dv = resolveDelivery(db, slip.print_header1, hikiateClass);
  const ctx = {
    source: 'unknown', material: null, candidates: [],
    deliveryCode: dv.code, headerRaw: dv.headerRaw,
    comboKey: combo?.comboKey || null,
    ruleId: null, ruleVersion: null,
    canRegister: false, reason: dv.reason, headerRuleNote: null,
  };
  // 順(-1): カード非表示分類 (LINEギフト — 中原さん 2026-08-24)
  const cls = classValue
    ? db.prepare('SELECT hide_card FROM pk_pack_classes WHERE class_value = ?').get(classValue) : null;
  if (cls?.hide_card) { ctx.source = 'hidden'; ctx.reason = 'hidden_class'; return ctx; }
  // 順0: ④配送方法変更の依頼中 (提案値では判定しない — 要件§3)
  const held = db.prepare(`
    SELECT COUNT(*) c FROM pk_pack_ship_changes
    WHERE batch_id = ? AND slip_seq = ? AND status IN ('requested','accepted')
  `).get(batchId, slip.seq).c;
  if (held > 0) { ctx.source = 'held'; ctx.reason = 'ship_change_requested'; return ctx; }
  // 順1: 伝票指定 (header_map.material_code 非NULL)
  if (dv.headerMaterial) {
    const m = materialRow(db, dv.headerMaterial);
    if (m) {
      ctx.source = 'header';
      ctx.material = toMaterialInfo(m);
      ctx.reason = null;
      // 登録ルールと矛盾する場合は併記だけ (順1優先・現場に変更は促さない)
      if (combo && DELIVERY_CODES.includes(dv.code)) {
        const r = db.prepare(`
          SELECT r.material_code, m.name FROM pk_pack_material_rules r
          JOIN pk_pack_materials m ON m.code = r.material_code
          WHERE r.combo_key=? AND r.delivery_code=? AND r.status='active'
        `).get(combo.comboKey, dv.code);
        if (r && r.material_code !== dv.headerMaterial) ctx.headerRuleNote = r.name;
      }
      return ctx;
    }
  }
  // 配送種別が判定できない → 順4 (登録不可)
  if (!DELIVERY_CODES.includes(dv.code)) return ctx;
  // 順2: 登録ルール
  if (combo) {
    const r = db.prepare(`
      SELECT id, version, material_code FROM pk_pack_material_rules
      WHERE combo_key=? AND delivery_code=? AND status='active'
    `).get(combo.comboKey, dv.code);
    if (r) {
      const m = materialRow(db, r.material_code);
      ctx.source = 'rule';
      ctx.material = toMaterialInfo(m) || { code: r.material_code, name: r.material_code, color: null, imageUrl: null, inactive: true };
      ctx.ruleId = r.id;
      ctx.ruleVersion = r.version;
      ctx.canRegister = slip.status === 'pending' || slip.status === 'done';
      ctx.reason = null;
      return ctx;
    }
  }
  // 順3: 分類別候補 (active JOIN 後 1件以上)
  const candidates = classValue ? classCandidates(db, classValue) : [];
  ctx.candidates = candidates;
  if (!combo) { ctx.reason = 'line_invalid'; return ctx; }   // 候補は見せるが登録不可
  ctx.canRegister = slip.status === 'pending' || slip.status === 'done';
  if (candidates.length > 0) { ctx.source = 'candidates'; ctx.reason = null; return ctx; }
  // 順4 (候補なし・配送種別は判定済み): グリッドから登録可
  ctx.reason = 'no_candidates';
  return ctx;
}

/** 表示観測ログの upsert。完了済み行 (completed_at 非NULL) は判定列を更新しない (スナップショット保護)。 */
export function upsertView(db, { batchId, slip, hikiateClass, ctx, now }) {
  db.prepare(`
    INSERT INTO pk_pack_material_views
      (batch_id, slip_seq, delivery_raw, hikiate_class, delivery_code, header_raw, combo_key,
       source, material_code, rule_id, first_shown_at, last_shown_at)
    VALUES (@batch_id, @slip_seq, @delivery_raw, @hikiate_class, @delivery_code, @header_raw, @combo_key,
       @source, @material_code, @rule_id, @now, @now)
    ON CONFLICT(batch_id, slip_seq) DO UPDATE SET
      last_shown_at = excluded.last_shown_at,
      delivery_raw  = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.delivery_raw  ELSE pk_pack_material_views.delivery_raw  END,
      hikiate_class = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.hikiate_class ELSE pk_pack_material_views.hikiate_class END,
      delivery_code = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.delivery_code ELSE pk_pack_material_views.delivery_code END,
      header_raw    = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.header_raw    ELSE pk_pack_material_views.header_raw    END,
      combo_key     = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.combo_key     ELSE pk_pack_material_views.combo_key     END,
      source        = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.source        ELSE pk_pack_material_views.source        END,
      material_code = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.material_code ELSE pk_pack_material_views.material_code END,
      rule_id       = CASE WHEN pk_pack_material_views.completed_at IS NULL THEN excluded.rule_id       ELSE pk_pack_material_views.rule_id       END
  `).run({
    batch_id: batchId, slip_seq: slip.seq,
    delivery_raw: slip.delivery_method || null,
    hikiate_class: normalizeKeyText(hikiateClass) || null,
    delivery_code: ctx.deliveryCode,
    header_raw: ctx.headerRaw || null,
    combo_key: ctx.comboKey,
    source: ctx.source,
    material_code: ctx.material?.code || null,
    rule_id: ctx.ruleId,
    now,
  });
}

/**
 * state 表示用: バッチ全伝票の判定+観測ログ upsert。失敗しても梱包は止めない (fail-soft は呼び出し側)。
 */
export function materialsForState(state, hikiateClass) {
  const db = getDB();
  const now = utcNow();
  const out = {};
  const tx = db.transaction(() => {
    for (const slip of state.slips) {
      const ctx = judgeSlip(db, { batchId: state.batch.id, slip, hikiateClass });
      upsertView(db, { batchId: state.batch.id, slip, hikiateClass, ctx, now });
      out[slip.seq] = {
        source: ctx.source,
        material: ctx.material,
        candidates: ctx.candidates,
        deliveryCode: ctx.deliveryCode,
        headerRaw: ctx.headerRaw,
        ruleId: ctx.ruleId,
        ruleVersion: ctx.ruleVersion,
        canRegister: ctx.canRegister,
        reason: ctx.reason,
        headerRuleNote: ctx.headerRuleNote,
      };
    }
  });
  tx();
  return out;
}

/** 登録・変更グリッド用: active 資材の全一覧 (分類候補 code 集合つき)。 */
export function materialOptions(hikiateClass) {
  const db = getDB();
  const all = db.prepare(
    'SELECT code, name, color, image_file, is_active FROM pk_pack_materials WHERE is_active = 1 ORDER BY sort_order, code'
  ).all().map(toMaterialInfo);
  const candidateCodes = classCandidates(db, hikiateClass || '').map((m) => m.code);
  return { materials: all, candidateCodes };
}

// ─── 登録 / 変更 / 取り消し ───

function conflictBody(db, comboKey, deliveryCode) {
  const r = comboKey ? db.prepare(`
    SELECT r.id, r.version, r.material_code, m.name, m.color, m.image_file, m.is_active
    FROM pk_pack_material_rules r LEFT JOIN pk_pack_materials m ON m.code = r.material_code
    WHERE r.combo_key=? AND r.delivery_code=? AND r.status='active'
  `).get(comboKey, deliveryCode) : null;
  return {
    current: r ? {
      ruleId: r.id, ruleVersion: r.version,
      material: { code: r.material_code, name: r.name || r.material_code, color: r.color || null,
        imageUrl: r.image_file ? `/apps/packing/materials/${r.image_file}` : null, inactive: !r.is_active },
    } : null,
  };
}

function opReplayOrConflict(db, opId, reqHash) {
  const prev = db.prepare('SELECT * FROM pk_pack_material_events WHERE op_id = ?').get(opId);
  if (!prev) return null;
  if (prev.request_hash !== reqHash) throw new PackError(409, 'op_conflict', '同じ操作IDで異なる内容が送信されました');
  const body = prev.response_json ? JSON.parse(prev.response_json) : { ok: true, eventId: prev.id };
  if (prev.action === 'register' || prev.action === 'change') {
    body.undoToken = undoTokenFor(prev.id);
  }
  return body;
}

/**
 * 資材の登録 (register) / 変更 (change)。§5.1〜5.4。
 * 呼び出し前提: 認証済み・worker は有効名検証済み。
 */
export function registerMaterial({
  batchId, slipSeq, materialCode,
  expectedRuleId = null, expectedVersion = null, expectedDeliveryCode, expectedBefore = null,
  opId, worker,
}) {
  const db = getDB();
  if (!opId || typeof opId !== 'string' || opId.length > 80) throw new PackError(400, 'bad_op_id', 'op_id が不正です');
  if (!worker) throw new PackError(400, 'no_worker', '作業者を選択してください');
  const reqHash = requestHashOf({
    kind: 'register', batchId, slipSeq, materialCode,
    expectedRuleId, expectedVersion, expectedDeliveryCode, expectedBefore,
  });
  const now = utcNow();
  const tx = db.transaction(() => {
    const replay = opReplayOrConflict(db, opId, reqHash);
    if (replay) return replay;

    const batch = db.prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(batchId);
    if (!batch) throw new PackError(404, 'batch_not_found', 'バッチがありません');
    if (!['packing', 'paused', 'done'].includes(batch.status)) {
      throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
    }
    const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
    if (!slip) throw new PackError(404, 'slip_not_found', `伝票 ${slipSeq} がありません`);
    if (slip.status !== 'pending' && slip.status !== 'done') {
      throw new PackError(409, 'slip_not_registerable', `保留中・取消済みの伝票には登録できません (${slip.status})`);
    }
    // 引当分類 (picking 参照のみ)。ライン/非表示分類/④依頼中は登録不可
    let hikiateClass = null;
    if (batch.pk_batch_id) {
      try {
        hikiateClass = db.prepare('SELECT hikiate_class FROM pk_batches WHERE id = ?')
          .get(batch.pk_batch_id)?.hikiate_class ?? null;
      } catch { /* picking 未初期化環境 */ }
    }
    const c = String(hikiateClass || '');
    if (c.includes('PAS-LINE') || c.includes('MELT-LINE')) {
      throw new PackError(409, 'line_batch', '梱包機バッチでは資材登録は使いません');
    }
    const lines = db.prepare('SELECT sku, qty, product_name FROM pk_pack_lines WHERE slip_id = ? ORDER BY id').all(slip.id);
    const ctx = judgeSlip(db, { batchId, slip: { ...slip, lines }, hikiateClass });
    if (ctx.source === 'hidden') throw new PackError(409, 'hidden_class', 'この分類では資材表示を使いません');
    if (ctx.source === 'held') throw new PackError(409, 'ship_change_requested', '配送方法変更の依頼中は登録できません');
    if (ctx.source === 'header') throw new PackError(409, 'header_fixed', '伝票指定の資材は変更できません');
    if (!DELIVERY_CODES.includes(ctx.deliveryCode)) {
      throw new PackError(409, 'delivery_unknown', '配送方法を判定できないため登録できません (責任者に確認してください)');
    }
    if (ctx.deliveryCode !== expectedDeliveryCode) {
      throw PackErrorWithBody(409, 'context_changed', '配送種別の判定が画面表示時から変わりました。画面を更新してください',
        conflictBody(db, ctx.comboKey, ctx.deliveryCode));
    }
    if (!ctx.comboKey) throw new PackError(400, 'line_invalid', '明細の SKU/数量が不正なため登録できません');
    const material = materialRow(db, String(materialCode || ''));
    if (!material || !material.is_active) throw new PackError(400, 'bad_material', 'その資材は選択できません');

    const combo = comboKeyOf(lines);
    const current = db.prepare(`
      SELECT id, version, material_code FROM pk_pack_material_rules
      WHERE combo_key=? AND delivery_code=? AND status='active'
    `).get(combo.comboKey, ctx.deliveryCode) || null;

    let action;
    let ruleId;
    let ruleVersion;
    let beforeCode = null;
    if (expectedRuleId == null) {
      // 新規登録: 既に誰かが登録済みなら 409 + 現在値 (自動 change はしない — §5.1)
      if (current) throw PackErrorWithBody(409, 'conflict', '他の端末で登録されました', conflictBody(db, combo.comboKey, ctx.deliveryCode));
      const ins = db.prepare(`
        INSERT INTO pk_pack_material_rules
          (combo_key, combo_detail, delivery_code, material_code, status, version, created_by, created_at, updated_by, updated_at)
        VALUES (?, ?, ?, ?, 'active', 1, ?, ?, ?, ?)
      `).run(combo.comboKey, combo.comboDetail, ctx.deliveryCode, material.code, worker, now, worker, now);
      action = 'register';
      ruleId = Number(ins.lastInsertRowid);
      ruleVersion = 1;
    } else {
      if (!current || current.id !== expectedRuleId || current.version !== expectedVersion
          || (expectedBefore != null && current.material_code !== expectedBefore)) {
        throw PackErrorWithBody(409, 'conflict', '他の端末で変更されました', conflictBody(db, combo.comboKey, ctx.deliveryCode));
      }
      if (current.material_code === material.code) {
        // 同じ資材への変更 = 何もしない (イベントも通知も作らない)
        return { ok: true, noop: true, rule: { ruleId: current.id, ruleVersion: current.version, material: toMaterialInfo(material) } };
      }
      const upd = db.prepare(`
        UPDATE pk_pack_material_rules SET material_code=?, version=version+1, updated_by=?, updated_at=?
        WHERE id=? AND version=? AND status='active'
      `).run(material.code, worker, now, current.id, current.version);
      if (upd.changes !== 1) throw PackErrorWithBody(409, 'conflict', '他の端末で変更されました', conflictBody(db, combo.comboKey, ctx.deliveryCode));
      action = 'change';
      ruleId = current.id;
      ruleVersion = current.version + 1;
      beforeCode = current.material_code;
    }

    const undoExpires = isoPlusSec(now, UNDO_SEC);
    const ev = db.prepare(`
      INSERT INTO pk_pack_material_events
        (op_id, request_hash, action, rule_id, rule_version, combo_key, delivery_code,
         delivery_raw, hikiate_class, header_raw, batch_id, slip_seq, ne_slip_no, folder_name,
         shown_source, before_code, after_code, worker, created_at,
         undo_expires_at, notify_status, notify_due_at, next_attempt_at)
      VALUES (@op_id, @request_hash, @action, @rule_id, @rule_version, @combo_key, @delivery_code,
         @delivery_raw, @hikiate_class, @header_raw, @batch_id, @slip_seq, @ne_slip_no, @folder_name,
         @shown_source, @before_code, @after_code, @worker, @created_at,
         @undo_expires_at, @notify_status, @notify_due_at, @next_attempt_at)
    `).run({
      op_id: opId, request_hash: reqHash, action,
      rule_id: ruleId, rule_version: ruleVersion,
      combo_key: combo.comboKey, delivery_code: ctx.deliveryCode,
      delivery_raw: slip.delivery_method || null,
      hikiate_class: normalizeKeyText(hikiateClass) || null,
      header_raw: ctx.headerRaw || null,
      batch_id: batchId, slip_seq: slipSeq, ne_slip_no: slip.ne_slip_no, folder_name: batch.folder_name || null,
      shown_source: ctx.source,
      before_code: beforeCode, after_code: material.code,
      worker, created_at: now,
      undo_expires_at: undoExpires,
      // change のみ通知 outbox (undo 猶予後に送信・undo で cancelled)。register は通知なし (日次サマリで確認)
      notify_status: action === 'change' ? 'pending' : 'none',
      notify_due_at: action === 'change' ? undoExpires : null,
      next_attempt_at: action === 'change' ? undoExpires : null,
    });
    const eventId = Number(ev.lastInsertRowid);
    const responseBody = {
      ok: true, eventId, action,
      rule: { ruleId, ruleVersion, material: toMaterialInfo(material) },
      undoExpiresAt: undoExpires, undoSec: UNDO_SEC,
    };
    db.prepare('UPDATE pk_pack_material_events SET response_json=? WHERE id=?')
      .run(JSON.stringify(responseBody), eventId);
    return { ...responseBody, undoToken: undoTokenFor(eventId) };
  });
  return tx.immediate();
}

/** PackError に 409 応答へ同梱する現在値を付ける (router の api() が e.body を展開)。 */
function PackErrorWithBody(status, code, message, body) {
  const e = new PackError(status, code, message);
  e.body = body;
  return e;
}

/** 取り消し (§5.2)。成立条件を全て同一 txn 内で検証。 */
export function undoMaterial({ opId, eventId, undoToken, worker }) {
  const db = getDB();
  if (!opId) throw new PackError(400, 'bad_op_id', 'op_id が不正です');
  const reqHash = requestHashOf({ kind: 'undo', eventId });
  const now = utcNow();
  const tx = db.transaction(() => {
    const replay = opReplayOrConflict(db, opId, reqHash);
    if (replay) return replay;
    const ev = db.prepare('SELECT * FROM pk_pack_material_events WHERE id = ?').get(Number(eventId));
    if (!ev || (ev.action !== 'register' && ev.action !== 'change')) {
      throw new PackError(404, 'event_not_found', '取り消し対象がありません');
    }
    if (!timingSafeEq(undoTokenFor(ev.id), undoToken)) throw new PackError(403, 'bad_token', '取り消しトークンが不正です');
    if (ev.undone_at) throw new PackError(409, 'already_undone', 'すでに取り消し済みです');
    if (now > ev.undo_expires_at) {
      throw new PackError(409, 'undo_expired', '取り消し期限が過ぎました。「変更」からやり直してください');
    }
    if (ev.notify_status !== 'none' && ev.notify_status !== 'pending') {
      throw new PackError(409, 'undo_conflict', '通知処理が始まったため取り消せません。「変更」で戻してください');
    }
    const rule = db.prepare('SELECT * FROM pk_pack_material_rules WHERE id = ?').get(ev.rule_id);
    if (!rule || rule.version !== ev.rule_version) {
      throw new PackError(409, 'undo_conflict', 'その後変更されたため取り消せません。「変更」で戻してください');
    }
    let upd;
    if (ev.action === 'register') {
      upd = db.prepare(`
        UPDATE pk_pack_material_rules SET status='disabled', version=version+1, updated_by=?, updated_at=?
        WHERE id=? AND version=?
      `).run(worker || ev.worker, now, rule.id, rule.version);
    } else {
      upd = db.prepare(`
        UPDATE pk_pack_material_rules SET material_code=?, version=version+1, updated_by=?, updated_at=?
        WHERE id=? AND version=? AND status='active'
      `).run(ev.before_code, worker || ev.worker, now, rule.id, rule.version);
    }
    if (upd.changes !== 1) throw new PackError(409, 'undo_conflict', 'その後変更されたため取り消せません');
    if (ev.notify_status === 'pending') {
      const c = db.prepare(`
        UPDATE pk_pack_material_events SET notify_status='cancelled' WHERE id=? AND notify_status='pending'
      `).run(ev.id);
      if (c.changes !== 1) throw new PackError(409, 'undo_conflict', '通知処理が始まったため取り消せません');
    }
    db.prepare('UPDATE pk_pack_material_events SET undone_at=? WHERE id=?').run(now, ev.id);
    const ins = db.prepare(`
      INSERT INTO pk_pack_material_events
        (op_id, request_hash, action, rule_id, rule_version, combo_key, delivery_code,
         delivery_raw, hikiate_class, header_raw, batch_id, slip_seq, ne_slip_no, folder_name,
         before_code, after_code, worker, created_at, target_event_id, notify_status)
      VALUES (?, ?, 'undo', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'none')
    `).run(
      opId, reqHash, ev.rule_id, ev.rule_version + 1, ev.combo_key, ev.delivery_code,
      ev.delivery_raw, ev.hikiate_class, ev.header_raw, ev.batch_id, ev.slip_seq, ev.ne_slip_no, ev.folder_name,
      ev.after_code, ev.before_code, worker || ev.worker, now, ev.id,
    );
    const body = { ok: true, undone: true, eventId: Number(ins.lastInsertRowid) };
    db.prepare('UPDATE pk_pack_material_events SET response_json=? WHERE id=?')
      .run(JSON.stringify(body), Number(ins.lastInsertRowid));
    return body;
  });
  return tx.immediate();
}

// ─── 完了スナップショット (service.applyEvent から呼ばれる — fail-soft) ───

export function onSlipCompleted(db, batchId, slipSeq, now) {
  db.prepare(`
    UPDATE pk_pack_material_views
    SET completed_at=?, completed_source=source, completed_material_code=material_code, completed_rule_id=rule_id
    WHERE batch_id=? AND slip_seq=? AND completed_at IS NULL
  `).run(now, batchId, slipSeq);
}

export function onSlipCompletionCleared(db, batchId, slipSeq) {
  db.prepare(`
    UPDATE pk_pack_material_views
    SET completed_at=NULL, completed_source=NULL, completed_material_code=NULL, completed_rule_id=NULL
    WHERE batch_id=? AND slip_seq=?
  `).run(batchId, slipSeq);
}

// ─── 通知 outbox (§5.3・poller から呼ばれる) ───

const RETRY_MAX = 10;
const RETRY_WINDOW_H = 48;
const STALE_CLAIM_MIN = 5;
/** attempt_count (claim 時に加算済み) → 次回までの待ち秒。30s,1m,2m,5m,10m,…上限30m */
function backoffSec(attempt) {
  const table = [30, 60, 120, 300, 600];
  return table[Math.min(attempt - 1, table.length - 1)] ?? 1800;
}

const LIMIT_SQL = `(attempt_count < ${RETRY_MAX}
  AND datetime('now') < datetime(replace(COALESCE(resend_requested_at, created_at),'Z',''), '+${RETRY_WINDOW_H} hours'))`;

/**
 * outbox 1周期: sweep → stale 回収 → claim → 送信 (最大3件/周期)。
 * sendFn(text) → Promise<boolean> (未設定 = claim しない構成エラー)。
 */
export async function materialNotifyStep(sendFn) {
  const db = getDB();
  const now = utcNow();
  let rows;
  try {
    // 上限外 pending の終端化 (sweep)
    db.prepare(`
      UPDATE pk_pack_material_events SET notify_status='failed', notify_error='retry limit'
      WHERE notify_status='pending' AND NOT ${LIMIT_SQL}
    `).run();
    // stale sending の回収 (claim_token 失効)。上限内→pending / 上限外→failed
    db.prepare(`
      UPDATE pk_pack_material_events
      SET notify_status = CASE WHEN ${LIMIT_SQL} THEN 'pending' ELSE 'failed' END,
          notify_error = CASE WHEN ${LIMIT_SQL} THEN notify_error ELSE 'stale claim / retry limit' END,
          next_attempt_at = ?, claim_token = NULL
      WHERE notify_status='sending' AND claimed_at < ?
    `).run(now, isoPlusSec(now, -STALE_CLAIM_MIN * 60));
    if (!sendFn) return { configError: true };
    rows = db.prepare(`
      SELECT id FROM pk_pack_material_events
      WHERE notify_status='pending' AND next_attempt_at <= ? AND ${LIMIT_SQL}
      ORDER BY id LIMIT 3
    `).all(now);
  } catch { return { skipped: true }; }   // v10 未適用環境
  let sent = 0;
  for (const { id } of rows) {
    const claimToken = crypto.randomBytes(12).toString('hex');
    const claimed = db.prepare(`
      UPDATE pk_pack_material_events
      SET notify_status='sending', claim_token=?, claimed_at=?, attempt_count=attempt_count+1
      WHERE id=? AND notify_status='pending' AND next_attempt_at <= ? AND ${LIMIT_SQL}
    `).run(claimToken, utcNow(), id, now);
    if (claimed.changes !== 1) continue;
    const ev = db.prepare('SELECT * FROM pk_pack_material_events WHERE id = ?').get(id);
    let ok = false;
    let errMsg = null;
    try {
      ok = await sendFn(materialChangeText(db, ev));
      if (!ok) errMsg = 'webhook 未設定または送信失敗';
    } catch (e) { errMsg = String(e.message).slice(0, 200); }
    if (ok) {
      db.prepare(`
        UPDATE pk_pack_material_events SET notify_status='sent', notified_at=?, notify_error=NULL, claim_token=NULL
        WHERE id=? AND notify_status='sending' AND claim_token=?
      `).run(utcNow(), id, claimToken);
      sent += 1;
    } else {
      db.prepare(`
        UPDATE pk_pack_material_events
        SET notify_status = CASE WHEN ${LIMIT_SQL} THEN 'pending' ELSE 'failed' END,
            notify_error = ?, next_attempt_at = ?, claim_token = NULL
        WHERE id=? AND notify_status='sending' AND claim_token=?
      `).run(errMsg, isoPlusSec(utcNow(), backoffSec(ev.attempt_count)), id, claimToken);
    }
  }
  return { sent, claimedRows: rows.length };
}

/** 通知本文 (§5.3)。event ID を必ず含める (at-least-once の重複識別)。 */
export function materialChangeText(db, ev) {
  const detail = (() => {
    try {
      const rule = db.prepare('SELECT combo_detail FROM pk_pack_material_rules WHERE id = ?').get(ev.rule_id);
      const items = JSON.parse(rule?.combo_detail || '[]');
      return items.map((i) => `${i.name || i.sku} × ${i.qty}`).join(' / ');
    } catch { return ev.combo_key; }
  })();
  const nameOf = (code) => {
    if (!code) return '(なし)';
    try { return db.prepare('SELECT name FROM pk_pack_materials WHERE code = ?').get(code)?.name || code; }
    catch { return code; }
  };
  return [
    `📦 [資材変更 #${ev.id}] 梱包資材の登録が変更されました`,
    `・伝票: ${ev.folder_name || '-'} #${ev.slip_seq ?? '-'} (${ev.ne_slip_no || '-'})`,
    `・商品: ${detail}`,
    `・配送種別: ${ev.delivery_code}`,
    `・変更: ${nameOf(ev.before_code)} → ${nameOf(ev.after_code)}`,
    `・作業者: ${ev.worker}`,
    `・管理画面: https://picking.bfaith-wh.uk/apps/packing/admin/materials#event-${ev.id}`,
    '内容が正しいか確認してください。誤りは管理画面の「登録ルール」から修正できます。',
  ].join('\n');
}

/** 手動再送 (failed のみ)。§5.3 の遷移表どおり attempt をリセットし 48h 窓を張り直す。 */
export function manualResend(eventId, admin) {
  const db = getDB();
  const now = utcNow();
  const upd = db.prepare(`
    UPDATE pk_pack_material_events
    SET notify_status='pending', resend_requested_at=?, resend_by=?, next_attempt_at=?, attempt_count=0, notify_error=NULL
    WHERE id=? AND notify_status='failed'
  `).run(now, admin, now, Number(eventId));
  if (upd.changes !== 1) throw new PackError(409, 'not_failed', '再送できるのは failed の通知だけです');
  return { ok: true };
}

// ─── 日次サマリ・管理画面用の集計 ───

/** 日次サマリ用: 指定 JST 日の登録/変更/取消/通知失敗件数。 */
export function materialDailyCounts(workDateJst) {
  const db = getDB();
  try {
    // created_at は UTC。JST日付に丸めて比較 (+9h)
    const row = db.prepare(`
      SELECT
        SUM(CASE WHEN action='register' THEN 1 ELSE 0 END) AS registered,
        SUM(CASE WHEN action='change' THEN 1 ELSE 0 END) AS changed,
        SUM(CASE WHEN action='undo' THEN 1 ELSE 0 END) AS undone,
        SUM(CASE WHEN notify_status='failed' THEN 1 ELSE 0 END) AS notifyFailed
      FROM pk_pack_material_events
      WHERE date(replace(created_at,'Z',''), '+9 hours') = ?
    `).get(workDateJst);
    return {
      registered: row?.registered || 0, changed: row?.changed || 0,
      undone: row?.undone || 0, notifyFailed: row?.notifyFailed || 0,
    };
  } catch { return null; }   // v10 未適用
}

/** 観測ログの purge (180日・既存 cleanup から呼ぶ)。 */
export function purgeOldViews() {
  const db = getDB();
  try {
    db.prepare("DELETE FROM pk_pack_material_views WHERE last_shown_at < datetime('now', '-180 days')").run();
  } catch { /* v10 未適用 */ }
}

// ─── seed 投入 (CLI / テスト共用) ───

/**
 * seed オブジェクトを投入 (upsert)。
 * { materials: [{code,name,color,image_file,sort_order}], classes: [{class_value,aes_kind,hide_card,sort_order}],
 *   class_materials: [{class_value, codes: [..]}], header_map: [{header_value, base_delivery_code, material_code}] }
 */
export function seedMaterialsData(seed, actor = 'seed') {
  const db = getDB();
  const now = utcNow();
  const tx = db.transaction(() => {
    for (const m of seed.materials || []) {
      db.prepare(`
        INSERT INTO pk_pack_materials (code, name, color, image_file, sort_order, is_active, created_at, updated_at, updated_by)
        VALUES (@code, @name, @color, @image_file, @sort_order, 1, @now, @now, @actor)
        ON CONFLICT(code) DO UPDATE SET name=excluded.name, color=excluded.color,
          image_file=COALESCE(excluded.image_file, pk_pack_materials.image_file),
          sort_order=excluded.sort_order, updated_at=excluded.updated_at, updated_by=excluded.updated_by
      `).run({ code: m.code, name: m.name, color: m.color || null, image_file: m.image_file || null,
        sort_order: m.sort_order ?? 100, now, actor });
    }
    for (const c of seed.classes || []) {
      const cv = normalizeKeyText(c.class_value);
      if (!cv) continue;
      db.prepare(`
        INSERT INTO pk_pack_classes (class_value, aes_kind, hide_card, sort_order, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(class_value) DO UPDATE SET aes_kind=excluded.aes_kind, hide_card=excluded.hide_card,
          sort_order=excluded.sort_order, updated_at=excluded.updated_at, updated_by=excluded.updated_by
      `).run(cv, c.aes_kind || null, c.hide_card ? 1 : 0, c.sort_order ?? 100, now, actor);
    }
    for (const cm of seed.class_materials || []) {
      const cv = normalizeKeyText(cm.class_value);
      if (!cv) continue;
      db.prepare('DELETE FROM pk_pack_class_materials WHERE class_value = ?').run(cv);
      (cm.codes || []).forEach((code, i) => {
        db.prepare(`
          INSERT INTO pk_pack_class_materials (class_value, material_code, sort_order, updated_at, updated_by)
          VALUES (?, ?, ?, ?, ?)
        `).run(cv, code, (i + 1) * 10, now, actor);
      });
    }
    for (const h of seed.header_map || []) {
      const hv = normalizeKeyText(h.header_value);
      if (!hv) continue;
      db.prepare(`
        INSERT INTO pk_pack_header_map (header_value, base_delivery_code, material_code, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(header_value) DO UPDATE SET base_delivery_code=excluded.base_delivery_code,
          material_code=excluded.material_code, updated_at=excluded.updated_at, updated_by=excluded.updated_by
      `).run(hv, h.base_delivery_code, h.material_code || null, now, actor);
    }
    const fk = db.pragma('foreign_key_check');
    if (fk.length > 0) throw new Error(`seed の参照整合性エラー: ${JSON.stringify(fk.slice(0, 3))}`);
  });
  tx.immediate();
}
