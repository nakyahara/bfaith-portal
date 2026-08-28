/**
 * inquiry-hub 担当者マスタと権限マップ
 * (2026-08-28 中原さん要望「この権限マップをアプリに入れて、アプリから俺が登録できるようにしたい」)
 *
 * ⭐なぜ「人」でなく「権限」で持つのか (中原さん + Codex 議論の結論):
 *   AIトリアージに担当者名を選ばせない。AIが出すのは「この問い合わせに必要な権限」までで、
 *   誰に渡すかはこの表を見る決定的なルールが決める。
 *   - 人が増減・交代してもAI側のプロンプトを触らなくてよい
 *   - AIの誤りが「人違い」ではなく「権限違い」として現れるので検出しやすい
 *   - 誰が何を約束していいかが明文化されること自体に価値がある
 *
 * ⚠️ このマスタは担当の保存先ではない。担当は従来どおり inquiries.assigned_user_id
 *   (自由入力の TEXT) に入る。マスタに無い担当者名も保存でき、既存データは壊れない。
 */
import { getDB, BUILTIN_PERMISSIONS } from './db.js';

export const STAFF_NAME_MAX = 60;
export const STAFF_KEY_MAX = 200;
export const PERM_NAME_MAX = 60;
export const PERM_DESC_MAX = 300;
export const PERM_CODE_MAX = 16;
/** 権限マトリクスが横に伸びすぎない上限 (これ以上必要なら分類の設計から見直す) */
export const MAX_ACTIVE_STAFF = 50;
export const MAX_ACTIVE_PERMISSIONS = 40;
/** 返金上限額の上限 (打ち間違いで桁を増やしても業務範囲を超えないところで止める) */
const REFUND_LIMIT_MAX = 10000000;

const NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%SZ','now')";

// ─── 入力の正規化・検証 ───

/** 担当者キー (assigned_user_id に入る値)。メールアドレス想定だが強制はしない */
export function normalizeUserKey(input) {
  const v = String(input ?? '').normalize('NFKC').replace(/[\r\n\t]/g, '').trim();
  if (!v) throw new Error('担当者キー (メールアドレス等) が空です');
  if (v.length > STAFF_KEY_MAX) throw new Error(`担当者キーが長すぎます (${STAFF_KEY_MAX}文字まで)`);
  return v;
}

export function normalizeDisplayName(input) {
  const v = String(input ?? '').normalize('NFKC').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!v) throw new Error('表示名が空です');
  if (v.length > STAFF_NAME_MAX) throw new Error(`表示名が長すぎます (${STAFF_NAME_MAX}文字まで)`);
  return v;
}

/**
 * 返金上限額。空文字/未指定は null。
 * ⚠️ null は「無制限」ではなく「金額の決裁はできない (未設定)」。
 *   設定を忘れた人が無制限の決裁者になる fail-open を作らないため (Codexレビュー指摘)。
 *   判定は必ず refundLimitOf() を通す
 */
function normalizeRefundLimit(v) {
  if (v === undefined || v === null || String(v).trim() === '') return null;
  const n = Number(String(v).replace(/[,\s]/g, ''));
  if (!Number.isInteger(n) || n < 0 || n > REFUND_LIMIT_MAX) {
    throw new Error(`返金上限額は0〜${REFUND_LIMIT_MAX.toLocaleString('ja-JP')}円の整数で指定してください`);
  }
  return n;
}

function normalizeSortOrder(v, fallback) {
  if (v === undefined || v === null || v === '') return fallback;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0 || n > 100000) throw new Error('表示順は0〜100000の整数で指定してください');
  return n;
}

/** 権限コード。コード側が参照する識別子なので英数字と _ のみに絞る */
export function normalizePermissionCode(input) {
  const v = String(input ?? '').normalize('NFKC').trim();
  if (!v) throw new Error('権限コードが空です');
  if (v.length > PERM_CODE_MAX) throw new Error(`権限コードが長すぎます (${PERM_CODE_MAX}文字まで)`);
  if (!/^[A-Za-z0-9_]+$/.test(v)) throw new Error('権限コードは英数字と _ だけで指定してください');
  return v;
}

// ─── 担当者 ───

/**
 * 担当者一覧。
 * @param {object} opts { includeInactive?: boolean, withPermissions?: boolean }
 */
export function listStaff({ includeInactive = false, withPermissions = false } = {}) {
  const db = getDB();
  const rows = db.prepare(`SELECT * FROM staff_members
    WHERE ${includeInactive ? '1=1' : 'is_active = 1'}
    ORDER BY is_active DESC, sort_order, display_name, id`).all();
  if (!withPermissions) return rows;
  const grants = db.prepare('SELECT staff_id, permission_code FROM staff_permissions').all();
  const byStaff = new Map();
  for (const g of grants) {
    if (!byStaff.has(g.staff_id)) byStaff.set(g.staff_id, []);
    byStaff.get(g.staff_id).push(g.permission_code);
  }
  return rows.map(r => ({ ...r, permissions: byStaff.get(r.id) || [] }));
}

export function getStaff(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM staff_members WHERE id = ?').get(id) || null;
}

/** 作成。有効な担当者の user_key は重複させない */
export function createStaff({ userKey, displayName, refundLimitYen, note } = {}, createdBy = null) {
  const db = getDB();
  const key = normalizeUserKey(userKey);
  const name = normalizeDisplayName(displayName);
  const limit = normalizeRefundLimit(refundLimitYen);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM staff_members WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_STAFF) {
      throw new Error(`担当者は${MAX_ACTIVE_STAFF}人までです (使わない担当者を無効にしてください)`);
    }
    if (db.prepare('SELECT id FROM staff_members WHERE is_active = 1 AND user_key = ?').get(key)) {
      throw new Error(`同じ担当者キーが既に登録されています: ${key}`);
    }
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM staff_members WHERE is_active = 1').get().m;
    const r = db.prepare(`INSERT INTO staff_members
      (user_key, display_name, refund_limit_yen, note, sort_order, created_by) VALUES (?,?,?,?,?,?)`)
      .run(key, name, limit, note ? String(note).slice(0, 500) : null, (maxOrder ?? 0) + 10, createdBy);
    return { id: r.lastInsertRowid, userKey: key, displayName: name };
  }).immediate();
}

/** 更新 (指定したフィールドだけ変更する) */
export function updateStaff(id, { userKey, displayName, refundLimitYen, note, sortOrder } = {}) {
  const db = getDB();
  return db.transaction(() => {
    const s = getStaff(id);
    if (!s || !s.is_active) throw new Error('担当者が見つかりません');
    const key = userKey === undefined ? s.user_key : normalizeUserKey(userKey);
    const name = displayName === undefined ? s.display_name : normalizeDisplayName(displayName);
    const limit = refundLimitYen === undefined ? s.refund_limit_yen : normalizeRefundLimit(refundLimitYen);
    const order = normalizeSortOrder(sortOrder, s.sort_order);
    if (key !== s.user_key && db.prepare('SELECT id FROM staff_members WHERE is_active = 1 AND user_key = ? AND id != ?').get(key, id)) {
      throw new Error(`同じ担当者キーが既に登録されています: ${key}`);
    }
    db.prepare(`UPDATE staff_members SET user_key = ?, display_name = ?, refund_limit_yen = ?,
        note = ?, sort_order = ?, updated_at = ${NOW_SQL} WHERE id = ?`)
      .run(key, name, limit, note === undefined ? s.note : (note ? String(note).slice(0, 500) : null), order, id);
    return { id, userKey: key, displayName: name };
  }).immediate();
}

/**
 * 無効化 (論理削除)。権限も同時に全部外す。
 * ⚠️ 問い合わせ側の assigned_user_id は触らない — 担当は自由入力の値であって
 *   このマスタへの参照ではないため、消すと「誰が対応していたか」の履歴が失われる。
 * 既に無効な担当者への再実行は成功として返す (通信エラー後の再送で失敗に見せない = 冪等)
 */
export function deactivateStaff(id, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const s = getStaff(id);
    if (!s) throw new Error('担当者が見つかりません');
    if (!s.is_active) return { id, displayName: s.display_name, revoked: 0, alreadyInactive: true };
    const codes = db.prepare(`SELECT sp.permission_code, p.name FROM staff_permissions sp
      LEFT JOIN permissions p ON p.code = sp.permission_code WHERE sp.staff_id = ?`).all(id);
    const log = db.prepare(`INSERT INTO staff_permission_logs
      (staff_id, permission_code, permission_name, action, actor) VALUES (?,?,?,'revoke',?)`);
    for (const c of codes) log.run(id, c.permission_code, c.name || null, actorId);
    db.prepare('DELETE FROM staff_permissions WHERE staff_id = ?').run(id);
    db.prepare(`UPDATE staff_members SET is_active = 0, updated_at = ${NOW_SQL} WHERE id = ?`).run(id);
    return { id, displayName: s.display_name, revoked: codes.length };
  }).immediate();
}

// ─── 権限 ───

export function listPermissions({ includeInactive = false } = {}) {
  return getDB().prepare(`SELECT * FROM permissions
    WHERE ${includeInactive ? '1=1' : 'is_active = 1'}
    ORDER BY is_active DESC, kind, sort_order, code`).all();
}

export function getPermission(code) {
  return getDB().prepare('SELECT * FROM permissions WHERE code = ?').get(String(code || '')) || null;
}

/** 独自権限の追加 (既定の19件で足りないときだけ使う) */
export function createPermission({ code, kind, name, description } = {}) {
  const db = getDB();
  const c = normalizePermissionCode(code);
  if (!['decision', 'escalation', 'system'].includes(kind)) {
    throw new Error('種別は decision / escalation / system のいずれかです');
  }
  const n = String(name ?? '').trim();
  if (!n) throw new Error('権限名が空です');
  if (n.length > PERM_NAME_MAX) throw new Error(`権限名が長すぎます (${PERM_NAME_MAX}文字まで)`);
  return db.transaction(() => {
    const active = db.prepare('SELECT COUNT(*) AS c FROM permissions WHERE is_active = 1').get().c;
    if (active >= MAX_ACTIVE_PERMISSIONS) throw new Error(`権限は${MAX_ACTIVE_PERMISSIONS}個までです`);
    if (getPermission(c)) throw new Error(`同じコードの権限があります: ${c}`);
    const maxOrder = db.prepare('SELECT MAX(sort_order) AS m FROM permissions WHERE kind = ?').get(kind).m;
    db.prepare(`INSERT INTO permissions (code, kind, name, description, sort_order, is_builtin)
      VALUES (?,?,?,?,?,0)`).run(c, kind, n, String(description ?? '').slice(0, PERM_DESC_MAX) || null, (maxOrder ?? 0) + 10);
    return { code: c, name: n };
  }).immediate();
}

/**
 * 権限の編集。
 * ⚠️ 既定権限の名称・説明は変更できない — コード側 (トリアージのルーティング・AIプロンプト・監査画面) が
 *   D1 や S4 の意味を前提に動くので、「S4」を「在庫照会」に書き換えられると表示と実装が食い違う
 *   (Codexレビュー指摘)。社内向けの補足は local_note に書ける。独自権限は自由に編集できる。
 */
export function updatePermission(code, { name, description, localNote, sortOrder } = {}) {
  const db = getDB();
  const p = getPermission(code);
  if (!p) throw new Error('権限が見つかりません');
  if (p.is_builtin && (name !== undefined || description !== undefined)) {
    throw new Error('既定の権限は名前と説明を変更できません (補足は「社内メモ」に書いてください)');
  }
  const n = name === undefined ? p.name : String(name).trim();
  if (!n) throw new Error('権限名が空です');
  if (n.length > PERM_NAME_MAX) throw new Error(`権限名が長すぎます (${PERM_NAME_MAX}文字まで)`);
  const d = description === undefined ? p.description : (String(description).slice(0, PERM_DESC_MAX) || null);
  const note = localNote === undefined ? p.local_note : (String(localNote).slice(0, PERM_DESC_MAX) || null);
  db.prepare(`UPDATE permissions SET name = ?, description = ?, local_note = ?, sort_order = ?,
      updated_at = ${NOW_SQL} WHERE code = ?`)
    .run(n, d, note, normalizeSortOrder(sortOrder, p.sort_order), p.code);
  return { code: p.code, name: n };
}

/**
 * 権限の有効/無効。既定権限も無効にできる (使わない権限を画面から隠せる) が、
 * 削除はできない — コード側が参照する code が消えるとルーティングが壊れるため。
 * 無効にしても付与済みの行は消さない (再度有効にしたときに元に戻る)
 */
export function setPermissionActive(code, active) {
  const p = getPermission(code);
  if (!p) throw new Error('権限が見つかりません');
  getDB().prepare(`UPDATE permissions SET is_active = ?, updated_at = ${NOW_SQL} WHERE code = ?`)
    .run(active ? 1 : 0, p.code);
  return { code: p.code, isActive: !!active };
}

/** 削除。既定権限は削除できない */
export function deletePermission(code, actorId = null) {
  const db = getDB();
  const p = getPermission(code);
  if (!p) throw new Error('権限が見つかりません');
  if (p.is_builtin) throw new Error('既定の権限は削除できません (使わない場合は「無効にする」を使ってください)');
  return db.transaction(() => {
    // 付与されていた人ごとに剥奪ログを残す (「誰がいつ外したか」を残す設計。Codexレビュー指摘)。
    // 権限定義が消えても履歴が読めるように、権限名もその場で書き込む
    const holders = db.prepare('SELECT staff_id FROM staff_permissions WHERE permission_code = ?').all(p.code);
    const log = db.prepare(`INSERT INTO staff_permission_logs
      (staff_id, permission_code, permission_name, action, actor) VALUES (?,?,?,'revoke',?)`);
    for (const h of holders) log.run(h.staff_id, p.code, p.name, actorId);
    db.prepare('DELETE FROM staff_permissions WHERE permission_code = ?').run(p.code);
    db.prepare('DELETE FROM permissions WHERE code = ?').run(p.code);
    return { code: p.code, name: p.name, revoked: holders.length };
  }).immediate();
}

// ─── 付与 ───

/**
 * 担当者の権限をまとめて置き換える (画面のチェックボックスを保存する経路)。
 * 差分だけを履歴に残す (変更していない権限のログで埋まらないように)。
 * @param {number} staffId
 * @param {string[]} codes 付与後の権限コードの配列
 * @returns {{ granted: string[], revoked: string[] }}
 */
export function setStaffPermissions(staffId, codes, actorId = null) {
  const db = getDB();
  if (!Array.isArray(codes)) throw new Error('権限コードの配列を指定してください');
  return db.transaction(() => {
    const s = getStaff(Number(staffId));
    if (!s || !s.is_active) throw new Error('担当者が見つかりません');
    // 権限コードの検証。⚠️新規付与は「有効な権限」だけ許す —
    // 無効にした権限を、古い画面や直接APIから付け直せてしまうのを防ぐ (Codexレビュー指摘)。
    // 既に付与済みのものは無効化されていても保持する (再度有効にしたときに元に戻る)
    const nameOf = new Map(db.prepare('SELECT code, name, is_active FROM permissions').all().map(r => [r.code, r]));
    const now = new Set(db.prepare('SELECT permission_code FROM staff_permissions WHERE staff_id = ?')
      .all(s.id).map(r => r.permission_code));
    const want = new Set();
    for (const c of codes) {
      const code = String(c || '').trim();
      if (!code) continue;
      const p = nameOf.get(code);
      if (!p) throw new Error(`存在しない権限コードです: ${code}`);
      if (!p.is_active && !now.has(code)) throw new Error(`無効になっている権限は付与できません: ${code}`);
      want.add(code);
    }
    const granted = [...want].filter(c => !now.has(c));
    const revoked = [...now].filter(c => !want.has(c));
    const ins = db.prepare('INSERT OR IGNORE INTO staff_permissions (staff_id, permission_code, granted_by) VALUES (?,?,?)');
    const del = db.prepare('DELETE FROM staff_permissions WHERE staff_id = ? AND permission_code = ?');
    const log = db.prepare(`INSERT INTO staff_permission_logs
      (staff_id, permission_code, permission_name, action, actor) VALUES (?,?,?,?,?)`);
    for (const c of granted) { ins.run(s.id, c, actorId); log.run(s.id, c, nameOf.get(c)?.name || null, 'grant', actorId); }
    for (const c of revoked) { del.run(s.id, c); log.run(s.id, c, nameOf.get(c)?.name || null, 'revoke', actorId); }
    return { granted, revoked };
  }).immediate();
}

/**
 * その担当者が決裁できる金額 (円)。⭐安全側 (fail-closed) に倒す:
 *   - D2 を持っていない → 0円 (金額の決裁はできない)
 *   - D2 は持つが上限額が未設定 → 0円 + needsLimit=true (画面で設定を促す)
 * 「上限額が空 = 無制限」には絶対にしない (Codexレビュー指摘の fail-open)
 * @returns {{ limit: number, hasD2: boolean, needsLimit: boolean }}
 */
export function refundLimitOf(staffWithPermissions) {
  const perms = staffWithPermissions?.permissions || [];
  const hasD2 = perms.includes('D2');
  const raw = staffWithPermissions?.refund_limit_yen;
  if (!hasD2) return { limit: 0, hasD2: false, needsLimit: false };
  if (raw == null) return { limit: 0, hasD2: true, needsLimit: true };
  return { limit: Number(raw), hasD2: true, needsLimit: false };
}

/**
 * 画面の「保存」1回で基本情報と権限をまとめて更新する。
 * ⭐1トランザクションにする理由: 別々に保存すると「名前は変わったが権限は失敗」という
 *   中途半端な状態が残る。権限は事故に直結するので、途中で止まったら何も変えない。
 */
export function saveStaffWithPermissions(id, { userKey, displayName, refundLimitYen, note, sortOrder, permissions } = {}, actorId = null) {
  const db = getDB();
  return db.transaction(() => {
    const updated = updateStaff(id, { userKey, displayName, refundLimitYen, note, sortOrder });
    const perms = permissions === undefined ? null : setStaffPermissions(id, permissions, actorId);
    return { ...updated, permissions: perms };
  }).immediate();
}

/**
 * 必要権限をすべて満たす担当者を返す (トリアージのルーティングが使う土台)。
 * ⚠️ ここは「権限を持つ人」までしか出さない。実際に誰へ渡すかは、勤務中か・当番か・
 *   既に誰か着手していないか を見て決める必要がある (Codex指摘)。それは別レイヤーの仕事。
 * @param {string[]} requiredCodes
 * @returns {Array} 条件を満たす有効な担当者 (表示順)
 */
export function findStaffWithPermissions(requiredCodes = []) {
  const need = [...new Set((requiredCodes || []).map(c => String(c || '').trim()).filter(Boolean))];
  const staff = listStaff({ withPermissions: true });
  if (need.length === 0) return staff;
  // ⚠️無効にした権限は判定に使わない。要求された権限が無効なら「該当者なし」を返す (fail-closed)。
  // 「無効にしたのに、その権限を根拠に担当候補へ出続ける」を防ぐ (Codexレビュー指摘)
  const activeCodes = new Set(listPermissions().map(p => p.code));
  if (need.some(c => !activeCodes.has(c))) return [];
  return staff.filter(s => need.every(c => s.permissions.includes(c)));
}

/** 権限マップの俯瞰 (画面のマトリクス用)。行=担当者・列=権限 */
export function getPermissionMatrix() {
  const permissions = listPermissions({ includeInactive: true });
  const staff = listStaff({ withPermissions: true });
  return { permissions, staff, builtinCount: BUILTIN_PERMISSIONS.length };
}

/** 直近の権限変更履歴 (監査用) */
export function listPermissionLogs(limit = 50) {
  // 権限名は操作時点のスナップショット (l.permission_name) を優先する —
  // 権限定義を削除したあとでも履歴が読めるようにするため
  return getDB().prepare(`SELECT l.*, s.display_name,
      COALESCE(l.permission_name, p.name) AS permission_name
    FROM staff_permission_logs l
    LEFT JOIN staff_members s ON s.id = l.staff_id
    LEFT JOIN permissions p ON p.code = l.permission_code
    ORDER BY l.id DESC LIMIT ?`).all(Math.min(200, Math.max(1, Number(limit) || 50)));
}
