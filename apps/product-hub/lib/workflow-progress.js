/**
 * product-hub ワークフロー: 商品 × 工程の進捗 (2026-08-23)。
 *
 * 「いま誰のボールか」の実体。工程マスタ (ph_steps) が列の定義で、ここが 1 商品ごとの中身。
 *
 * 設計:
 *   - 行は**表示時に自己修復**で作る (ensureProgress)。工程を後から足しても既存ドラフトに行き渡る。
 *     ドラフト作成時に一括生成する方式だと、工程を追加した瞬間に過去分が穴だらけになる
 *   - 初回生成のときだけ、旧 status と画像の有無から「もう終わっている工程」を推定する。
 *     途中で足した工程まで done にしないよう、推定は**初回に限る**
 *   - 担当者は工程の既定担当を初期値に入れ、商品ごとに差し替えられる
 *   - product_drafts.status は**このPRでは触らない**。既存の出品ゲートや AI キューが status を
 *     見ているため、二重管理を承知で並行させる。status を工程から導出する切替は次段階 (PR4)
 */
import { getDB, logEvent } from '../db.js';

export const STEP_STATES = ['todo', 'doing', 'done', 'skip'];
export const STEP_STATE_LABELS = {
  todo: '未着手',
  doing: '作業中',
  done: '完了',
  skip: '対象外',
};

/**
 * 旧 status → 「もう終わっている本流工程」の推定表 (初回生成時のみ使う)。
 * 既存ドラフトを工程ボードに載せたとき、全部が先頭列に固まって使い物にならないのを防ぐ。
 * on_hold / excluded は工程の外に退避している状態なので何も done にしない。
 */
const STATUS_DONE_STEPS = {
  draft: [],
  ready_for_ai: ['basic_info'],
  review: ['basic_info', 'ai_generate'],
  approved: ['basic_info', 'ai_generate', 'desc_review', 'title_approve'],
  listed: ['basic_info', 'ai_generate', 'desc_review', 'title_approve', 'set_review'],
  expanded: ['basic_info', 'ai_generate', 'desc_review', 'title_approve', 'set_review', 'listing'],
  on_hold: [],
  excluded: [],
};

function badRequest(message) {
  const e = new Error(message);
  e.status = 400;
  return e;
}

const nowIso = () => new Date().toISOString();

/** 役割 → 既定担当者 id。無効化された人は既定から外れている (setStaffActive) */
function defaultAssigneeByRole(db) {
  const rows = db.prepare(`
    SELECT sr.role_code, sr.staff_id
    FROM ph_staff_roles sr JOIN ph_staff s ON s.id = sr.staff_id AND s.active = 1
    WHERE sr.is_default = 1
  `).all();
  return new Map(rows.map((r) => [r.role_code, r.staff_id]));
}

/**
 * 工程行の自己修復生成。足りない工程だけ INSERT する (既存行は触らない)。
 * @returns {number} 追加した行数
 */
export function ensureProgress(db, draftId) {
  const id = Number(draftId);
  const steps = db.prepare('SELECT code, track, role_code FROM ph_steps WHERE active = 1 ORDER BY track, sort').all();
  if (steps.length === 0) return 0;
  const existing = new Set(
    db.prepare('SELECT step_code FROM draft_step_progress WHERE draft_id = ?').all(id).map((r) => r.step_code)
  );
  const missing = steps.filter((s) => !existing.has(s.code));
  if (missing.length === 0) return 0;

  // 初回 (1 行も無い) だけ、旧 status と画像から済み工程を推定する
  const firstTime = existing.size === 0;
  let doneSet = new Set();
  if (firstTime) {
    const draft = db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(id);
    doneSet = new Set(STATUS_DONE_STEPS[draft?.status] || []);
    // 画像が既に登録されているなら画像トラックは終わっているとみなす。
    // 登録済みの商品を「画像未依頼」で並べると、外注に二重依頼させかねない
    const hasImages = db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? LIMIT 1').get(id);
    if (hasImages) for (const s of steps) if (s.track === 'image') doneSet.add(s.code);
  }

  const byRole = defaultAssigneeByRole(db);
  const ins = db.prepare(`
    INSERT OR IGNORE INTO draft_step_progress (draft_id, step_code, state, assignee_id, done_at, done_by)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const run = db.transaction(() => {
    let n = 0;
    for (const s of missing) {
      const done = doneSet.has(s.code);
      const info = ins.run(
        id, s.code,
        done ? 'done' : 'todo',
        s.role_code ? (byRole.get(s.role_code) ?? null) : null,
        done ? nowIso() : null,
        done ? 'migration' : null,
      );
      n += info.changes;
    }
    return n;
  });
  return run();
}

/**
 * 一覧・かんばん向けの一括自己修復。
 * 表示対象のうち **工程行が足りないものだけ** に ensureProgress を走らせる。
 * 全件に無条件で走らせると、毎回のページ表示で 500 件 × 工程数の INSERT を空打ちすることになる。
 * @returns {number} 修復したドラフト数
 */
export function ensureProgressForMany(db, draftIds) {
  const ids = (Array.isArray(draftIds) ? draftIds : []).map(Number).filter(Number.isInteger);
  if (ids.length === 0) return 0;
  const stepCount = db.prepare('SELECT COUNT(*) AS c FROM ph_steps WHERE active = 1').get().c;
  if (stepCount === 0) return 0;
  const placeholders = ids.map(() => '?').join(',');
  const have = new Map(db.prepare(`
    SELECT p.draft_id, COUNT(*) AS c
    FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    WHERE p.draft_id IN (${placeholders}) GROUP BY p.draft_id
  `).all(...ids).map((r) => [r.draft_id, r.c]));
  let fixed = 0;
  for (const id of ids) {
    if ((have.get(id) || 0) >= stepCount) continue;
    if (ensureProgress(db, id) > 0) fixed++;
  }
  return fixed;
}

/** state が done / skip でない最初の行 = いま進める番の工程 (全部終わっていれば null) */
function currentOf(rows) {
  return rows.find((r) => r.state !== 'done' && r.state !== 'skip') || null;
}

/**
 * 工程に入ってからの経過日数。
 * 起点は 開始日時 → 直前に完了した工程の完了日時 → ドラフト作成日時 の順で拾う。
 * updated_at を使わないのは、担当者を変えただけで滞留がリセットされてしまうため。
 */
function stalledDaysOf(rows, idx, createdAt) {
  const row = rows[idx];
  if (!row || row.stall_days == null) return null;
  let since = row.started_at;
  if (!since) {
    for (let i = idx - 1; i >= 0; i--) {
      if (rows[i].done_at) { since = rows[i].done_at; break; }
    }
  }
  if (!since) since = createdAt;
  const t = Date.parse(since);
  if (!Number.isFinite(t)) return null;
  const days = Math.floor((Date.now() - t) / 86400000);
  return days >= row.stall_days ? days : null;
}

/**
 * 1 商品の工程進捗。画面 (詳細・かんばん) がそのまま描ける形に整えて返す。
 */
export function progressOf(draftId, { db = null } = {}) {
  const conn = db || getDB();
  const id = Number(draftId);
  ensureProgress(conn, id);
  const draft = conn.prepare('SELECT created_at FROM product_drafts WHERE id = ?').get(id);
  const rows = conn.prepare(`
    SELECT p.draft_id, p.step_code, p.state, p.assignee_id, p.due_date, p.started_at,
           p.done_at, p.done_by, p.note, p.updated_at,
           s.label, s.track, s.sort, s.role_code, s.stall_days, s.description,
           r.label AS role_label,
           st.name AS assignee_name, st.color AS assignee_color, st.active AS assignee_active
    FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    LEFT JOIN ph_roles r ON r.code = s.role_code
    LEFT JOIN ph_staff st ON st.id = p.assignee_id
    WHERE p.draft_id = ?
    ORDER BY s.track = 'image', s.sort, s.code
  `).all(id);

  const main = rows.filter((r) => r.track === 'main');
  const image = rows.filter((r) => r.track === 'image');
  const current = currentOf(main);
  const imageCurrent = currentOf(image);
  const createdAt = draft?.created_at || null;
  return {
    main,
    image,
    current,
    imageCurrent,
    // 滞留は「いま止まっている工程」だけ見る。過去の工程を今さら赤くしても打ち手がない
    stalledDays: current ? stalledDaysOf(main, main.indexOf(current), createdAt) : null,
    imageStalledDays: imageCurrent ? stalledDaysOf(image, image.indexOf(imageCurrent), createdAt) : null,
    mainDone: main.length > 0 && !current,
    imageDone: image.length > 0 && !imageCurrent,
    doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
    totalCount: main.length,
  };
}

/**
 * 工程の状態・担当者・メモを更新する。
 * done にした時刻と操作者を残すのは、あとで「この工程は誰が何日で回しているか」を測るため。
 */
export function setStepState(draftId, stepCode, patch, actor) {
  const db = getDB();
  const id = Number(draftId);
  const code = String(stepCode || '');
  ensureProgress(db, id);
  const row = db.prepare(`
    SELECT p.*, s.label FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code
    WHERE p.draft_id = ? AND p.step_code = ?
  `).get(id, code);
  if (!row) throw badRequest('この商品にその工程がありません');

  const sets = [];
  const params = { draft_id: id, step_code: code };
  let event = null;

  if (patch?.state !== undefined) {
    const state = String(patch.state);
    if (!STEP_STATES.includes(state)) throw badRequest('状態の指定が不正です');
    if (state !== row.state) {
      sets.push('state = @state');
      params.state = state;
      // 作業中に入った時刻は最初の 1 回だけ記録する (往復しても起点を失わない)
      if (state === 'doing' && !row.started_at) {
        sets.push('started_at = @started_at');
        params.started_at = nowIso();
      }
      if (state === 'done') {
        sets.push('done_at = @done_at', 'done_by = @done_by');
        params.done_at = nowIso();
        params.done_by = actor || null;
      } else if (row.state === 'done') {
        // 差し戻し。完了の痕跡を消しておかないと「完了なのに未着手」の行が残る
        sets.push('done_at = NULL', 'done_by = NULL');
      }
      event = `${row.label}: ${STEP_STATE_LABELS[row.state]} → ${STEP_STATE_LABELS[state]}`;
    }
  }

  if (patch?.assignee_id !== undefined) {
    const raw = patch.assignee_id;
    let staffId = null;
    if (raw !== null && raw !== '' && raw !== undefined) {
      staffId = Number(raw);
      if (!Number.isInteger(staffId)) throw badRequest('担当者の指定が不正です');
      const st = db.prepare('SELECT name, active FROM ph_staff WHERE id = ?').get(staffId);
      if (!st) throw badRequest('担当者が見つかりません');
      // 無効化した人を新たに割り当てさせない (既に割り当たっている行はそのまま表示する)
      if (st.active !== 1) throw badRequest(`${st.name} は無効化されています`);
    }
    if (staffId !== row.assignee_id) {
      sets.push('assignee_id = @assignee_id');
      params.assignee_id = staffId;
      const name = staffId
        ? db.prepare('SELECT name FROM ph_staff WHERE id = ?').get(staffId).name
        : '未割り当て';
      event = `${event ? `${event} / ` : `${row.label}: `}担当 → ${name}`;
    }
  }

  if (patch?.note !== undefined) {
    sets.push('note = @note');
    const note = String(patch.note == null ? '' : patch.note).trim();
    params.note = note === '' ? null : note.slice(0, 300);
  }

  if (patch?.due_date !== undefined) {
    const due = String(patch.due_date == null ? '' : patch.due_date).trim();
    if (due !== '' && !/^\d{4}-\d{2}-\d{2}$/.test(due)) throw badRequest('期限は YYYY-MM-DD で指定してください');
    sets.push('due_date = @due_date');
    params.due_date = due === '' ? null : due;
  }

  if (sets.length === 0) return { changed: false };
  db.prepare(`
    UPDATE draft_step_progress SET ${sets.join(', ')}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE draft_id = @draft_id AND step_code = @step_code
  `).run(params);
  if (event) logEvent(db, id, 'step_changed', event, actor);
  return { changed: true };
}

/**
 * かんばん 1 枚分のデータ。列 (工程) ごとにカードを振り分けて返す。
 *
 * 現在工程の判定は SQL でなく JS で行う。ph_steps.sort は管理画面から編集できる数値で
 * 重複しうるため、「sort が最小の未完了行」を SQL で取ると同点のとき列が不定になる。
 *
 * @param {object} opts.assigneeId  この担当者のボールだけに絞る (null = 全部)
 * @param {boolean} opts.unassignedOnly 担当者が決まっていないカードだけ
 * @returns {{columns: Array, imageColumns: Array, doneCards: Array, total: number, truncated: boolean}}
 */
export function boardData(db, { assigneeId = null, unassignedOnly = false, limit = 800 } = {}) {
  const steps = db.prepare(`
    SELECT code, label, track, sort, role_code, stall_days FROM ph_steps
    WHERE active = 1 ORDER BY track = 'image', sort, code
  `).all();
  // 保留・除外は工程の外に退避している状態なので、ボードには載せない (列が汚れる)
  const drafts = db.prepare(`
    SELECT d.id, d.ne_code, d.name, d.status, d.created_at, d.updated_at,
      (SELECT drive_file_id FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_id,
      (SELECT drive_modified_time FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_mtime
    FROM product_drafts d
    WHERE d.status NOT IN ('on_hold', 'excluded')
    ORDER BY d.updated_at DESC
    LIMIT ?
  `).all(limit + 1);
  const truncated = drafts.length > limit;
  if (truncated) drafts.length = limit;

  ensureProgressForMany(db, drafts.map((d) => d.id));
  const summary = progressSummaryFor(db, drafts.map((d) => d.id));

  const columns = steps.filter((s) => s.track === 'main').map((s) => ({ ...s, cards: [] }));
  const imageColumns = steps.filter((s) => s.track === 'image').map((s) => ({ ...s, cards: [] }));
  const byCode = new Map([...columns, ...imageColumns].map((c) => [c.code, c]));
  const doneCards = [];

  for (const d of drafts) {
    const p = summary.get(d.id);
    if (!p) continue;
    const card = {
      id: d.id, ne_code: d.ne_code, name: d.name, status: d.status,
      first_image_id: d.first_image_id, first_image_mtime: d.first_image_mtime,
      current: p.current, imageCurrent: p.imageCurrent, imageDone: p.imageDone,
      stalledDays: p.stalledDays, doneCount: p.doneCount, totalCount: p.totalCount,
    };
    // 絞り込みは本流・画像のどちらかのボールが一致すれば残す。
    // 画像担当の人が「自分の分だけ」を見たとき、本流が別担当でも画像列に自分の札が要る
    if (assigneeId != null) {
      const hit = p.current?.assignee_id === assigneeId || p.imageCurrent?.assignee_id === assigneeId;
      if (!hit) continue;
    }
    if (unassignedOnly) {
      // システム工程 (担当ロールが無い = AI待ち) は「未割り当て」ではないので数えない
      const mainUnassigned = p.current && p.current.role_code && p.current.assignee_id == null;
      const imageUnassigned = p.imageCurrent && p.imageCurrent.role_code && p.imageCurrent.assignee_id == null;
      if (!mainUnassigned && !imageUnassigned) continue;
    }
    if (p.current) byCode.get(p.current.step_code)?.cards.push(card);
    else doneCards.push(card);
    if (p.imageCurrent) byCode.get(p.imageCurrent.step_code)?.cards.push(card);
  }

  // 停滞しているものを上に出す (打ち手が要るカードを埋もれさせない)
  const order = (a, b) => (b.stalledDays || 0) - (a.stalledDays || 0) || a.id - b.id;
  for (const c of columns) c.cards.sort(order);
  for (const c of imageColumns) c.cards.sort(order);

  return {
    columns,
    imageColumns,
    // 完了は溜まる一方なので直近だけ (全部見たいときは一覧から)
    doneCards: doneCards.slice(0, 30),
    doneTotal: doneCards.length,
    total: drafts.length,
    truncated,
  };
}

/**
 * 一覧・かんばん用に複数ドラフトの進捗をまとめて引く (行ごとに引くと N+1)。
 * @returns {Map<number, {current, image, ...}>}
 */
export function progressSummaryFor(db, draftIds) {
  const ids = (Array.isArray(draftIds) ? draftIds : []).map(Number).filter(Number.isInteger);
  const out = new Map();
  if (ids.length === 0) return out;
  const placeholders = ids.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT p.draft_id, p.step_code, p.state, p.assignee_id, p.done_at, p.started_at,
           s.label, s.track, s.sort, s.stall_days, s.role_code,
           st.name AS assignee_name, st.color AS assignee_color,
           -- 先頭工程が止まっている場合は前工程の完了日時が無いので、ドラフト作成日時を起点にする
           d.created_at AS draft_created_at
    FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    JOIN product_drafts d ON d.id = p.draft_id
    LEFT JOIN ph_staff st ON st.id = p.assignee_id
    WHERE p.draft_id IN (${placeholders})
    ORDER BY s.track = 'image', s.sort, s.code
  `).all(...ids);
  const byDraft = new Map();
  for (const r of rows) {
    if (!byDraft.has(r.draft_id)) byDraft.set(r.draft_id, []);
    byDraft.get(r.draft_id).push(r);
  }
  for (const id of ids) {
    const all = byDraft.get(id) || [];
    const main = all.filter((r) => r.track === 'main');
    const image = all.filter((r) => r.track === 'image');
    const current = currentOf(main);
    const imageCurrent = currentOf(image);
    const createdAt = all[0]?.draft_created_at || null;
    out.set(id, {
      current,
      imageCurrent,
      imageDone: image.length > 0 && !imageCurrent,
      stalledDays: current ? stalledDaysOf(main, main.indexOf(current), createdAt) : null,
      imageStalledDays: imageCurrent ? stalledDaysOf(image, image.indexOf(imageCurrent), createdAt) : null,
      doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
      totalCount: main.length,
    });
  }
  return out;
}
