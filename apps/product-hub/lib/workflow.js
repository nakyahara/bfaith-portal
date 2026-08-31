/**
 * product-hub ワークフロー: 担当者 / 役割 / 工程マスタの操作 (2026-08-23)。
 *
 * 目的: 「ステータスごとに誰が何をするか」を画面で見えるようにする。
 * 構造は **工程 → 役割 → 人** の 3 段 (中原さん指定):
 *   工程「基本情報入力」→ 役割「商品登録者」→ 人 (複数可、うち 1 人が既定担当)
 * 人ではなく役割を工程に紐づけるので、担当者が入れ替わっても工程定義は無変更で済む。
 *
 * 設計上の約束:
 *   - **物理削除しない**。退職・契約終了は active=0 (担当履歴を壊さない)
 *   - 担当者名は LOWER(TRIM()) で一意。旧実装 (datalist 自由入力) の表記ゆれを繰り返さない
 *   - builtin の役割・工程はコードが参照するので無効化・削除させない (改名は可)
 *   - 検証エラーは e.status=400 を付けて投げ、router 側で JSON にする
 */
import { getDB, STAFF_KINDS, STAFF_COLORS } from '../db.js';

const KIND_VALUES = new Set(STAFF_KINDS.map((k) => k.value));
const MAX_NAME_LEN = 40;
const MAX_LABEL_LEN = 40;
const MAX_NOTE_LEN = 200;
const MAX_DESC_LEN = 300;
const MAX_STALL_DAYS = 365;
// 「a@b.c」程度の緩い検査。ここは ID 照合用でメール送信はしないので厳密さより誤判定回避を優先
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
const COLOR_RE = /^#[0-9a-fA-F]{6}$/;
const CODE_RE = /^[a-z][a-z0-9_]{1,29}$/;

function badRequest(message) {
  const e = new Error(message);
  e.status = 400;
  return e;
}

function trimOrNull(v, max) {
  const s = String(v == null ? '' : v).trim();
  if (s === '') return null;
  return max && s.length > max ? s.slice(0, max) : s;
}

/** 一意制約違反か (better-sqlite3 は code に SQLITE_CONSTRAINT_* を入れる) */
function isUniqueViolation(e) {
  return String(e?.code || '').startsWith('SQLITE_CONSTRAINT');
}

// ─── 担当者 ─────────────────────────────────────────────

/**
 * 担当者一覧 (役割つき)。役割は 1 クエリでまとめて引いてマージする (行ごとに引くと N+1)。
 */
export function listStaff({ includeInactive = false } = {}) {
  const db = getDB();
  const rows = db.prepare(`
    SELECT * FROM ph_staff ${includeInactive ? '' : 'WHERE active = 1'}
    ORDER BY active DESC, sort, id
  `).all();
  const roleRows = db.prepare(`
    SELECT sr.staff_id, sr.role_code, sr.is_default, r.label, r.sort
    FROM ph_staff_roles sr
    JOIN ph_roles r ON r.code = sr.role_code
    ORDER BY r.sort, r.code
  `).all();
  const byStaff = new Map();
  for (const r of roleRows) {
    if (!byStaff.has(r.staff_id)) byStaff.set(r.staff_id, []);
    byStaff.get(r.staff_id).push({ code: r.role_code, label: r.label, isDefault: r.is_default === 1 });
  }
  return rows.map((s) => ({ ...s, roles: byStaff.get(s.id) || [] }));
}

export function getStaff(id) {
  const db = getDB();
  const row = db.prepare('SELECT * FROM ph_staff WHERE id = ?').get(Number(id));
  if (!row) return null;
  row.roles = db.prepare(`
    SELECT sr.role_code AS code, sr.is_default, r.label
    FROM ph_staff_roles sr JOIN ph_roles r ON r.code = sr.role_code
    WHERE sr.staff_id = ? ORDER BY r.sort, r.code
  `).all(row.id).map((r) => ({ code: r.code, label: r.label, isDefault: r.is_default === 1 }));
  return row;
}

/** ポータルのログインメールから担当者を引く (「自分の作業」の絞り込み用) */
export function staffByPortalEmail(email) {
  const e = trimOrNull(email);
  if (!e) return null;
  return getDB().prepare(`
    SELECT * FROM ph_staff WHERE active = 1 AND LOWER(TRIM(portal_email)) = ?
  `).get(e.toLowerCase()) || null;
}

function validateStaffInput(input, { partial = false } = {}) {
  const out = {};
  if (!partial || input.name !== undefined) {
    const name = trimOrNull(input.name);
    if (!name) throw badRequest('担当者名を入力してください');
    if (name.length > MAX_NAME_LEN) throw badRequest(`担当者名は ${MAX_NAME_LEN} 文字までです`);
    out.name = name;
  }
  if (!partial || input.kind !== undefined) {
    const kind = trimOrNull(input.kind) || 'internal';
    if (!KIND_VALUES.has(kind)) throw badRequest('区分の指定が不正です');
    out.kind = kind;
  }
  if (!partial || input.portal_email !== undefined) {
    const email = trimOrNull(input.portal_email);
    if (email && !EMAIL_RE.test(email)) throw badRequest('ポータルのメールアドレスの形式が正しくありません');
    out.portal_email = email ? email.toLowerCase() : null;
  }
  if (!partial || input.color !== undefined) {
    const color = trimOrNull(input.color);
    if (color && !COLOR_RE.test(color)) throw badRequest('色は #rrggbb の形式で指定してください');
    out.color = color;
  }
  if (!partial || input.note !== undefined) {
    out.note = trimOrNull(input.note, MAX_NOTE_LEN);
  }
  if (input.sort !== undefined) {
    const n = Number(input.sort);
    if (!Number.isFinite(n)) throw badRequest('並び順は数値で指定してください');
    out.sort = Math.round(n);
  }
  return out;
}

export function createStaff(input) {
  const db = getDB();
  const v = validateStaffInput(input || {});
  // 色未指定なら登録順にパレットから割り当てる (かんばんで人を色で見分けるため、空を作らない)
  if (!v.color) {
    const n = db.prepare('SELECT COUNT(*) AS c FROM ph_staff').get().c;
    v.color = STAFF_COLORS[n % STAFF_COLORS.length];
  }
  if (v.sort === undefined) {
    v.sort = (db.prepare('SELECT COALESCE(MAX(sort), 0) AS m FROM ph_staff').get().m || 0) + 10;
  }
  try {
    const info = db.prepare(`
      INSERT INTO ph_staff (name, kind, portal_email, color, note, sort)
      VALUES (@name, @kind, @portal_email, @color, @note, @sort)
    `).run(v);
    return Number(info.lastInsertRowid);
  } catch (e) {
    if (isUniqueViolation(e)) {
      throw badRequest(`「${v.name}」はすでに登録されています (無効化した人も含みます)`);
    }
    throw e;
  }
}

export function updateStaff(id, input) {
  const db = getDB();
  const staffId = Number(id);
  const v = validateStaffInput(input || {}, { partial: true });
  const cols = Object.keys(v);
  if (cols.length === 0) return false;
  const setSql = cols.map((c) => `${c} = @${c}`).join(', ');
  try {
    const info = db.prepare(`
      UPDATE ph_staff SET ${setSql}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = @id
    `).run({ ...v, id: staffId });
    return info.changes === 1;
  } catch (e) {
    if (isUniqueViolation(e)) throw badRequest('同じ名前の担当者がすでに登録されています');
    throw e;
  }
}

/**
 * 有効/無効の切り替え。**削除は用意しない** — 割り当て履歴が残るため。
 * 無効化しても既存の割り当ては消さず、新規の自動割り当て対象から外れるだけ。
 */
export function setStaffActive(id, active) {
  const db = getDB();
  const on = active ? 1 : 0;
  const staffId = Number(id);
  return db.transaction(() => {
    const info = db.prepare(`
      UPDATE ph_staff SET active = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?
    `).run(on, staffId);
    // 無効化した人が役割の既定担当のままだと、新規ドラフトが退職者に割り当たる。
    // 役割自体は残し、既定フラグだけ落とす (誰が既定でもない状態は許容 = 未割り当てで出る)
    if (!on) db.prepare('UPDATE ph_staff_roles SET is_default = 0 WHERE staff_id = ?').run(staffId);
    return info.changes === 1;
  })();
}

/**
 * 担当者の役割をまとめて置き換える。
 * @param {Array<{code: string, isDefault?: boolean}>} roles
 *
 * is_default は役割ごとに 1 人 (partial unique index)。ここで「既定にする」と指定された
 * 役割は、**他の人の既定を先に外してから**立てる。UNIQUE 違反でエラーにすると
 * 「先に前任者の既定を外してください」という二度手間になるため、付け替えを正とする。
 */
export function setStaffRoles(id, roles) {
  const db = getDB();
  const staffId = Number(id);
  if (!db.prepare('SELECT 1 FROM ph_staff WHERE id = ?').get(staffId)) {
    throw badRequest('担当者が見つかりません');
  }
  const wanted = [];
  const seen = new Set();
  for (const r of Array.isArray(roles) ? roles : []) {
    const code = trimOrNull(r?.code);
    if (!code || seen.has(code)) continue;
    if (!db.prepare('SELECT 1 FROM ph_roles WHERE code = ?').get(code)) {
      throw badRequest(`役割「${code}」は存在しません`);
    }
    seen.add(code);
    wanted.push({ code, isDefault: r?.isDefault === true || r?.is_default === 1 });
  }
  db.transaction(() => {
    db.prepare('DELETE FROM ph_staff_roles WHERE staff_id = ?').run(staffId);
    const ins = db.prepare(`
      INSERT INTO ph_staff_roles (staff_id, role_code, is_default) VALUES (?, ?, ?)
    `);
    for (const w of wanted) {
      if (w.isDefault) {
        db.prepare('UPDATE ph_staff_roles SET is_default = 0 WHERE role_code = ?').run(w.code);
      }
      ins.run(staffId, w.code, w.isDefault ? 1 : 0);
    }
  })();
  return wanted.length;
}

/**
 * 担当者の作成/更新と役割の置き換えを **1 トランザクション**で行う (Codex R1 medium)。
 * 分けて実行すると、役割側で失敗したときに「担当者だけ作られた」「名前だけ変わって
 * 役割は旧いまま」という中途半端な状態が残り、画面は失敗表示なのに DB は変わっている、になる。
 * better-sqlite3 のトランザクションはネストすると savepoint になるので、
 * 内側の setStaffRoles のトランザクションと併用して問題ない。
 */
export function createStaffWithRoles(input) {
  const db = getDB();
  return db.transaction(() => {
    const id = createStaff(input);
    if (Array.isArray(input?.roles)) setStaffRoles(id, input.roles);
    return id;
  })();
}

export function updateStaffWithRoles(id, input) {
  const db = getDB();
  return db.transaction(() => {
    const ok = updateStaff(id, input);
    if (!ok) return false;
    if (Array.isArray(input?.roles)) setStaffRoles(id, input.roles);
    return true;
  })();
}

// ─── 役割 ───────────────────────────────────────────────

export function listRoles({ includeInactive = false } = {}) {
  const db = getDB();
  const rows = db.prepare(`
    SELECT * FROM ph_roles ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort, code
  `).all();
  // 各役割に「何人いるか」「既定は誰か」を添える (画面で穴を見つけるため)
  const counts = db.prepare(`
    SELECT sr.role_code,
           COUNT(*) AS member_count,
           MAX(CASE WHEN sr.is_default = 1 THEN s.name END) AS default_name
    FROM ph_staff_roles sr JOIN ph_staff s ON s.id = sr.staff_id AND s.active = 1
    GROUP BY sr.role_code
  `).all();
  const byCode = new Map(counts.map((c) => [c.role_code, c]));
  return rows.map((r) => ({
    ...r,
    memberCount: byCode.get(r.code)?.member_count || 0,
    defaultName: byCode.get(r.code)?.default_name || null,
  }));
}

export function createRole(input) {
  const db = getDB();
  const label = trimOrNull(input?.label, MAX_LABEL_LEN);
  if (!label) throw badRequest('役割名を入力してください');
  let code = trimOrNull(input?.code);
  if (code && !CODE_RE.test(code)) {
    throw badRequest('役割コードは英小文字・数字・_ で 2〜30 文字です');
  }
  if (!code) {
    // 日本語ラベルからは英数コードを作れないので連番で採る (コードは内部識別子で画面には出さない)
    const n = db.prepare('SELECT COUNT(*) AS c FROM ph_roles').get().c;
    code = `role_${n + 1}_${Date.now().toString(36).slice(-4)}`;
  }
  const sort = (db.prepare('SELECT COALESCE(MAX(sort), 0) AS m FROM ph_roles').get().m || 0) + 10;
  try {
    db.prepare('INSERT INTO ph_roles (code, label, sort, builtin) VALUES (?, ?, ?, 0)').run(code, label, sort);
  } catch (e) {
    if (isUniqueViolation(e)) throw badRequest('同じコードの役割がすでにあります');
    throw e;
  }
  return code;
}

export function updateRole(code, input) {
  const db = getDB();
  const role = db.prepare('SELECT * FROM ph_roles WHERE code = ?').get(String(code));
  if (!role) throw badRequest('役割が見つかりません');
  const sets = [];
  const params = { code: role.code };
  if (input?.label !== undefined) {
    const label = trimOrNull(input.label, MAX_LABEL_LEN);
    if (!label) throw badRequest('役割名を入力してください');
    sets.push('label = @label');
    params.label = label;
  }
  if (input?.sort !== undefined) {
    const n = Number(input.sort);
    if (!Number.isFinite(n)) throw badRequest('並び順は数値で指定してください');
    sets.push('sort = @sort');
    params.sort = Math.round(n);
  }
  if (input?.active !== undefined) {
    const on = input.active ? 1 : 0;
    // builtin は工程シードが参照する。無効化すると工程の担当ロールが空になり
    // 「誰のボールでもない工程」ができるので止める
    if (!on && role.builtin === 1) throw badRequest('この役割は工程が使っているため無効化できません (名前は変更できます)');
    // builtin でなくても、稼働中の工程が参照していれば同じことが起きる (Codex R1 medium)。
    // 「役割の候補からは消えているのに、その役割の既定担当だけ入り続ける」食い違いを作らない
    if (!on) {
      const used = db.prepare(`
        SELECT label FROM ph_steps WHERE active = 1 AND role_code = ? ORDER BY sort LIMIT 3
      `).all(role.code);
      if (used.length > 0) {
        throw badRequest(
          `工程「${used.map((u) => u.label).join('」「')}」が使っているため無効化できません。`
          + '先に工程の担当ロールを変えてください'
        );
      }
    }
    sets.push('active = @active');
    params.active = on;
  }
  if (sets.length === 0) return false;
  const info = db.prepare(`
    UPDATE ph_roles SET ${sets.join(', ')}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE code = @code
  `).run(params);
  return info.changes === 1;
}

// ─── 工程 ───────────────────────────────────────────────

/**
 * 工程一覧。役割名と既定担当者を join して返す (画面で「この工程は誰がやるか」を出すため)。
 */
export function listSteps({ includeInactive = false, track = null } = {}) {
  const db = getDB();
  const where = [];
  const params = [];
  if (!includeInactive) where.push('s.active = 1');
  if (track) {
    where.push('s.track = ?');
    params.push(String(track));
  }
  return db.prepare(`
    SELECT s.*, r.label AS role_label, r.active AS role_active,
           (SELECT st.name FROM ph_staff_roles sr JOIN ph_staff st ON st.id = sr.staff_id
             WHERE sr.role_code = s.role_code AND sr.is_default = 1 AND st.active = 1) AS default_staff_name,
           (SELECT st.color FROM ph_staff_roles sr JOIN ph_staff st ON st.id = sr.staff_id
             WHERE sr.role_code = s.role_code AND sr.is_default = 1 AND st.active = 1) AS default_staff_color,
           (SELECT COUNT(*) FROM ph_staff_roles sr JOIN ph_staff st ON st.id = sr.staff_id
             WHERE sr.role_code = s.role_code AND st.active = 1) AS member_count
    FROM ph_steps s
    LEFT JOIN ph_roles r ON r.code = s.role_code
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY s.track = 'image', s.sort, s.code
  `).all(...params);
}

export function updateStep(code, input) {
  const db = getDB();
  const step = db.prepare('SELECT * FROM ph_steps WHERE code = ?').get(String(code));
  if (!step) throw badRequest('工程が見つかりません');
  const sets = [];
  const params = { code: step.code };
  if (input?.label !== undefined) {
    const label = trimOrNull(input.label, MAX_LABEL_LEN);
    if (!label) throw badRequest('工程名を入力してください');
    sets.push('label = @label');
    params.label = label;
  }
  if (input?.role_code !== undefined) {
    const rc = trimOrNull(input.role_code);
    if (rc) {
      const role = db.prepare('SELECT active FROM ph_roles WHERE code = ?').get(rc);
      if (!role) throw badRequest('指定された役割は存在しません');
      if (role.active !== 1) throw badRequest('無効化された役割は工程に割り当てられません');
    }
    sets.push('role_code = @role_code');
    params.role_code = rc; // null = システム工程 (担当者を置かない)
  }
  if (input?.sort !== undefined) {
    const n = Number(input.sort);
    if (!Number.isFinite(n)) throw badRequest('並び順は数値で指定してください');
    sets.push('sort = @sort');
    params.sort = Math.round(n);
  }
  if (input?.stall_days !== undefined) {
    const raw = trimOrNull(input.stall_days);
    let days = null;
    if (raw != null) {
      const n = Number(raw);
      if (!Number.isFinite(n) || n <= 0) throw badRequest('滞留日数は 1 以上の数値か、空 (警告なし) にしてください');
      days = Math.min(MAX_STALL_DAYS, Math.round(n));
    }
    sets.push('stall_days = @stall_days');
    params.stall_days = days;
  }
  if (input?.description !== undefined) {
    sets.push('description = @description');
    params.description = trimOrNull(input.description, MAX_DESC_LEN);
  }
  if (input?.active !== undefined) {
    const on = input.active ? 1 : 0;
    // 廃止した TOP画像の工程は元に戻せない (戻すとボードに TOP 列・カードが復活する)。
    // **コードでなく属性で**見る (Codex R3 high): 管理画面から足したカスタム TOP 工程も同じ
    if (on && step.track === 'image' && step.image_kind !== 'detail') {
      throw badRequest('TOP画像の工程は 2026-08-31 に廃止しました (画像の工程は商品詳細 (LP) の 1 本です)');
    }
    if (!on && step.builtin === 1) {
      throw badRequest('この工程は本流の定義に含まれるため無効化できません (名前と担当ロールは変更できます)');
    }
    sets.push('active = @active');
    params.active = on;
  }
  if (sets.length === 0) return false;
  const run = db.transaction(() => {
    const info = db.prepare(`
      UPDATE ph_steps SET ${sets.join(', ')}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE code = @code
    `).run(params);
    // 画像トラックの組み込み工程は TOP/詳細 で対になっている。ラベル・並び順・滞留日数・説明は
    // 対の片方だけ変えると、ボードの「同じ段階を 1 列にまとめる」表示が段階ごと分裂して見えるので
    // **一括で**そろえる (Codex設計相談)。担当ロールと有効/無効は種別ごとに変えたい場合が
    // ありうる (TOP と詳細で承認者を分ける等) ので伝播しない
    if (step.track === 'image' && step.builtin === 1 && step.image_stage) {
      const shared = sets.filter((s) => /^(label|sort|stall_days|description) =/.test(s));
      if (shared.length > 0) {
        db.prepare(`
          UPDATE ph_steps SET ${shared.join(', ')}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          WHERE track = 'image' AND builtin = 1 AND image_stage = @image_stage AND code <> @code
        `).run({ ...params, image_stage: step.image_stage });
      }
    }
    return info.changes === 1;
  });
  return run();
}

/** 工程の追加 (運用で足りない作業が出たとき画面から足せるようにする) */
export function createStep(input) {
  const db = getDB();
  const label = trimOrNull(input?.label, MAX_LABEL_LEN);
  if (!label) throw badRequest('工程名を入力してください');
  const track = trimOrNull(input?.track) || 'main';
  if (track !== 'main' && track !== 'image') throw badRequest('トラックの指定が不正です');
  // 画像トラックは TOP/詳細 で別々に進むので、どちらの工程かを必ず指定させる。
  // 未指定を許すとゲート・ボードの種別分けからこぼれる (fail-safe で TOP 扱いにはするが意図が残らない)
  let imageKind = null;
  if (track === 'image') {
    imageKind = trimOrNull(input?.image_kind);
    // TOP画像の工程は 2026-08-31 に廃止 (LP に一本化)。作れるようにしておくと
    // ボードに TOP 列・TOP カードが復活して「1 商品 1 枚」が崩れる
    if (imageKind === 'top') {
      throw badRequest('TOP画像の工程は作れません (2026-08-31 に廃止。画像の工程は商品詳細 (LP) の 1 本です)');
    }
    if (imageKind !== 'detail') {
      throw badRequest('画像トラックの工程は種別 (商品詳細画像) を指定してください');
    }
  }
  const rc = trimOrNull(input?.role_code);
  if (rc && !db.prepare('SELECT 1 FROM ph_roles WHERE code = ? AND active = 1').get(rc)) {
    throw badRequest('指定された役割は存在しません');
  }
  const n = db.prepare('SELECT COUNT(*) AS c FROM ph_steps').get().c;
  const code = `step_${n + 1}_${Date.now().toString(36).slice(-4)}`;
  const sort = (db.prepare('SELECT COALESCE(MAX(sort), 0) AS m FROM ph_steps WHERE track = ?').get(track).m || 0) + 10;
  db.prepare(`
    INSERT INTO ph_steps (code, label, track, image_kind, role_code, sort, description, builtin)
    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
  `).run(code, label, track, imageKind, rc, sort, trimOrNull(input?.description, MAX_DESC_LEN));
  return code;
}

/**
 * 工程 → 役割 → 既定担当者 の対応表 (かんばんの列ヘッダー用)。
 * 「担当ロールはあるが人が 1 人もいない工程」を unassignedRoles で返し、画面で警告する。
 * これが無いと、工程を作ったのに誰も割り当てられていない状態に気づけない。
 */
export function workflowOverview() {
  const steps = listSteps();
  const unassignedRoles = [];
  for (const s of steps) {
    if (s.role_code && s.member_count === 0) {
      unassignedRoles.push({ step: s.label, role: s.role_label || s.role_code });
    }
  }
  return {
    main: steps.filter((s) => s.track === 'main'),
    image: steps.filter((s) => s.track === 'image'),
    unassignedRoles,
  };
}
