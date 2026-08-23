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
// モール定義は定義専用ファイルから取る (mall-status.js を import すると循環する)
import { MALLS, LISTING_STEP_CODE as LISTING_STEP } from './malls-def.js';

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

function forbidden(message) {
  const e = new Error(message);
  e.status = 403;
  return e;
}

/** 競合 (別の人が先に動かした)。画面は読み直しを促す */
function conflict(message) {
  const e = new Error(message);
  e.status = 409;
  return e;
}

const nowIso = () => new Date().toISOString();

/** YYYY-MM-DD かつ実在する日付か (2026-99-99 や 2026-02-31 を弾く) */
function isRealDate(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return false;
  const y = Number(m[1]); const mo = Number(m[2]); const d = Number(m[3]);
  const dt = new Date(Date.UTC(y, mo - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === mo - 1 && dt.getUTCDate() === d;
}

/**
 * 工程を操作してよいか (Codex R1 high)。
 *
 * 2026-08-23 中原さん確認: **外注は契約終了済みで、担当者は全員ポータルにログインする**。
 * したがって「担当者本人か admin だけが動かせる」で運用が回る。
 *   - admin            … 全部できる (代理で進める・担当を付け替える・対象外にする)
 *   - 担当者本人        … 状態 (未着手/作業中/完了)・期限・メモ。対象外と他人への付け替えは不可
 *   - 未割り当ての工程  … 誰でも「自分が担当する」形で引き受けられる (放置を防ぐ)
 *   - 他人の担当工程    … 触れない
 * 誰の担当でもないシステム工程 (AI待ち) は、生成が終わったのに進まないときに
 * 人が手で進められるよう、未割り当てと同じ扱いにする。
 */
function assertStepPermission(db, row, patch, { isAdmin, actorStaffId }) {
  if (isAdmin) return;

  // 自分の担当工程
  if (row.assignee_id != null && row.assignee_id === actorStaffId) {
    if (patch?.state === 'skip') throw forbidden('「対象外」にできるのは管理者だけです');
    if (patch?.assignee_id !== undefined) {
      const to = patch.assignee_id === '' || patch.assignee_id == null ? null : Number(patch.assignee_id);
      // 自分の担当を外す (未割り当てに戻す) のは可。他人への付け替えは admin の操作
      if (to !== null && to !== actorStaffId) throw forbidden('担当者を他の人に変更できるのは管理者だけです');
    }
    return;
  }

  // 未割り当ての工程: **引き受けだけ**を許す (Codex R2 high)。
  // 状態や期限をいきなり変えられると「本人 + admin に限定」の方針が崩れ、
  // 誰がやったのかも残らない。1 クリック増えるが、担当が明確になる方を採る
  if (row.assignee_id == null) {
    if (!row.role_code) {
      // システム工程 (AI待ち)。手で進めるのは例外操作なので管理者に寄せる
      throw forbidden('この工程はシステムが進めます。手で進める必要があるときは管理者に依頼してください');
    }
    if (actorStaffId == null) {
      throw forbidden('あなたのポータルアカウントに担当者が紐づいていません (担当者・工程の画面で設定してください)');
    }
    const keys = Object.keys(patch || {}).filter((k) => patch[k] !== undefined && k !== 'expected_version');
    const onlyClaim = keys.length === 1 && keys[0] === 'assignee_id'
      && Number(patch.assignee_id) === actorStaffId;
    if (!onlyClaim) throw forbidden('先に「自分が担当する」を押してから操作してください');
    return;
  }

  const owner = db.prepare('SELECT name FROM ph_staff WHERE id = ?').get(row.assignee_id);
  throw forbidden(`この工程は ${owner?.name || '他の人'} さんの担当です。管理者に依頼してください`);
}

/** 役割 → 既定担当者 id。無効化された人は既定から外れている (setStaffActive) */
function defaultAssigneeByRole(db) {
  const rows = db.prepare(`
    SELECT sr.role_code, sr.staff_id
    FROM ph_staff_roles sr
    JOIN ph_staff s ON s.id = sr.staff_id AND s.active = 1
    -- 無効化された役割の既定担当は自動割り当てに使わない (Codex R1 medium:
    -- 画面の候補からは消えているのに新しい進捗にだけ入り続ける、という食い違いを防ぐ)
    JOIN ph_roles r ON r.code = sr.role_code AND r.active = 1
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
  const steps = db.prepare('SELECT code, track, image_kind, role_code FROM ph_steps WHERE active = 1 ORDER BY track, sort').all();
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
    // 画像が既に登録されているなら **TOP側**の画像トラックは終わっているとみなす。
    // 登録済みの商品を「画像未依頼」で並べると、外注に二重依頼させかねない。
    // 詳細側は推定しない — 画像が 1 枚あることは詳細画像が揃っている根拠にならず、
    // done にすると TOP しか無い商品が出品ゲートを素通りする (Codex R1 critical と同じ穴)。
    // 楽天登録済みの商品だけは詳細側も done (出品済みに「承認して」を出さない)
    const hasImages = db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? LIMIT 1').get(id);
    if (hasImages) for (const s of steps) if (s.track === 'image' && s.image_kind !== 'detail') doneSet.add(s.code);
    const listedRk = db.prepare(
      'SELECT 1 FROM draft_rakuten WHERE draft_id = ? AND registered_at IS NOT NULL'
    ).get(id);
    if (listedRk) for (const s of steps) if (s.track === 'image') doneSet.add(s.code);
  } else {
    // 後から足した工程は原則 todo で入れる (過去分を勝手に done にしない)。
    // 唯一の例外: **画像トラック**の工程で、その商品が既に楽天へ登録済みのとき。
    // 画像トラックは出品のゲートなので、出品が済んだ商品に「画像承認 未着手」が出ると
    // 承認者が「もう楽天に並んでいるのに何を承認するのか」と混乱する (2026-08-23 画像承認の追加時)
    const listed = db.prepare(
      'SELECT 1 FROM draft_rakuten WHERE draft_id = ? AND registered_at IS NOT NULL'
    ).get(id);
    if (listed) for (const s of missing) if (s.track === 'image') doneSet.add(s.code);
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

/**
 * 進捗行が 1 つも無いドラフトを埋める (ボードの絞り込みの前提)。
 * 保留・除外はボードに載せないので対象外。初回だけまとまった件数になるが、
 * 2 回目以降は該当 0 件なので 1 クエリで終わる。
 */
export function ensureMissingProgress(db, limit = 3000) {
  const ids = db.prepare(`
    SELECT d.id FROM product_drafts d
    WHERE d.status NOT IN ('on_hold', 'excluded')
      AND NOT EXISTS (SELECT 1 FROM draft_step_progress p WHERE p.draft_id = d.id)
    LIMIT ?
  `).all(limit).map((r) => r.id);
  for (const id of ids) ensureProgress(db, id);
  return ids.length;
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

/** 画像種別の表示名。ph_steps.image_kind ('top'/'detail') に対応する */
export const IMAGE_KIND_LABELS = { top: 'TOP画像', detail: '詳細画像' };

/**
 * 画像トラックの行を種別ごとに分ける。image_kind が NULL のカスタム工程は
 * TOP 側に寄せる (fail-safe: 出品ゲートから漏れる側には倒さない)
 */
function splitImageRows(imageRows) {
  return {
    top: imageRows.filter((r) => r.image_kind !== 'detail'),
    detail: imageRows.filter((r) => r.image_kind === 'detail'),
  };
}

/** 1 種別分のサマリー (current / done / 滞留)。excluded の種別は current を出さない */
function kindSummaryOf(rows, createdAt, excluded = false) {
  const current = excluded ? null : currentOf(rows);
  return {
    rows,
    current,
    excluded,
    done: !excluded && rows.length > 0 && !current,
    stalledDays: current ? stalledDaysOf(rows, rows.indexOf(current), createdAt) : null,
  };
}

/**
 * 1 商品の工程進捗。画面 (詳細・かんばん) がそのまま描ける形に整えて返す。
 * 画像トラックは TOP/詳細 で別々に進む (2026-08-24) ので、種別ごとのサマリーを返す。
 */
export function progressOf(draftId, { db = null } = {}) {
  const conn = db || getDB();
  const id = Number(draftId);
  ensureProgress(conn, id);
  const draft = conn.prepare('SELECT created_at, detail_images_excluded FROM product_drafts WHERE id = ?').get(id);
  const rows = conn.prepare(`
    SELECT p.draft_id, p.step_code, p.state, p.assignee_id, p.due_date, p.started_at,
           p.done_at, p.done_by, p.note, p.updated_at, p.version,
           s.label, s.track, s.image_kind, s.image_stage, s.sort, s.role_code, s.stall_days, s.description,
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
  const kinds = splitImageRows(image);
  const detailExcluded = draft?.detail_images_excluded === 1;
  const current = currentOf(main);
  const createdAt = draft?.created_at || null;
  const imageTop = kindSummaryOf(kinds.top, createdAt);
  const imageDetail = kindSummaryOf(kinds.detail, createdAt, detailExcluded);
  return {
    main,
    image,
    imageTop,
    imageDetail,
    detailExcluded,
    current,
    // 滞留は「いま止まっている工程」だけ見る。過去の工程を今さら赤くしても打ち手がない
    stalledDays: current ? stalledDaysOf(main, main.indexOf(current), createdAt) : null,
    mainDone: main.length > 0 && !current,
    // 画像トラック全体の決着 = TOP が済み、詳細も済みか対象外 (出品ゲートと同じ見方)。
    // 詳細工程 0 件は「済み」に数えない (対象外にしていないなら設定壊れ — Codex R2 high)
    imageDone: imageTop.done && (detailExcluded || imageDetail.done),
    doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
    totalCount: main.length,
  };
}

/** ブロック理由の「いま: ○○ (担当: △△)」部分 */
function kindBlockDetail(summary) {
  const cur = summary.current;
  if (!cur) return '未着手';
  const who = cur.assignee_name ? ` / 担当: ${cur.assignee_name}` : (cur.role_label ? ` / ${cur.role_label} 未割り当て` : '');
  return `${cur.label}${who}`;
}

/**
 * 楽天出品のゲート: 画像トラック (依頼 → 制作 → 登録 → 承認) が TOP/詳細 とも終わっていなければ理由を返す。
 * 詳細画像を作らない商品 (detail_images_excluded=1) は TOP だけで通る。
 * buildItemPayload の reasons に載せるので、「送信内容を確認」でも止まる理由が見える。
 * @returns {string|null} 止める理由 (通ってよければ null)
 */
export function imageTrackBlockReason(db, draftId) {
  const p = progressOf(draftId, { db });
  // fail-closed: TOP 側の工程が 1 つも無い状態 (工程の無効化・移行漏れ) で出品を通さない (Codex R1 high)。
  // progressOf → ensureProgress が行を自己修復するので、ここに来るのは設定が壊れているときだけ
  if (p.imageTop.rows.length === 0) {
    return '画像トラック (TOP画像) の工程が見つかりません。担当者・工程の設定を確認してください';
  }
  // TOP 全部が「対象外」はゲートを開けない (サムネイル無しの楽天出品はありえない — Codex設計相談 High)。
  // setStepState 側でも TOP の skip は拒否しているが、ゲートは独立に防御する
  if (p.imageTop.rows.every((r) => r.state === 'skip')) {
    return 'TOP画像の工程がすべて「対象外」になっています。TOP画像 (サムネイル) は楽天出品に必須です';
  }
  // 詳細側も同じ fail-closed (Codex R2 high): 対象外にしていないのに工程が 0 件なら
  // 「揃っている」ではなく「設定が壊れている」— ここで通すと詳細画像の確認が丸ごと飛ぶ
  if (!p.detailExcluded && p.imageDetail.rows.length === 0) {
    return '画像トラック (詳細画像) の工程が見つかりません。詳細画像を作らない商品なら「詳細画像は対象外」にするか、担当者・工程の設定を確認してください';
  }
  const blocked = [];
  if (!p.imageTop.done) {
    blocked.push(`${IMAGE_KIND_LABELS.top}: ${kindBlockDetail(p.imageTop)}`);
  }
  if (!p.detailExcluded && !p.imageDetail.done) {
    blocked.push(`${IMAGE_KIND_LABELS.detail}: ${kindBlockDetail(p.imageDetail)}`);
  }
  if (blocked.length === 0) return null;
  return `画像トラックが終わっていません (${blocked.join(' ／ ')})。画像承認まで済ませるか、`
    + '詳細画像を作らない商品なら詳細画面で「詳細画像は対象外」にしてください';
}

/**
 * 「詳細画像を作らない」フラグの切り替え (2026-08-24 中原さん: 単純な仕入れ商品は TOP のみ)。
 * detail 側の工程行は書き換えない — done の履歴を保ち、複数行更新の競合も避ける (Codex設計相談)。
 * 権限: admin か、その商品の詳細画像「依頼」工程の担当者本人 (作るかどうかを判断するのは依頼担当のため、
 * 工程の skip が admin 限定なのとは別に、このトグルだけ本人まで許す)
 */
export function setDetailImagesExcluded(draftId, excluded, actor, { isAdmin = false, actorStaffId = null } = {}) {
  const db = getDB();
  const id = Number(draftId);
  ensureProgress(db, id);
  const on = excluded ? 1 : 0;
  if (!isAdmin) {
    const requester = db.prepare(`
      SELECT p.assignee_id FROM draft_step_progress p
      JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.draft_id = ? AND s.track = 'image' AND s.image_kind = 'detail' AND s.image_stage = 'request'
    `).get(id);
    if (actorStaffId == null || requester?.assignee_id !== actorStaffId) {
      throw forbidden('「詳細画像は対象外」を切り替えられるのは、管理者か詳細画像の依頼工程の担当者だけです');
    }
  }
  const info = db.prepare(`
    UPDATE product_drafts SET detail_images_excluded = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ? AND detail_images_excluded = ?
  `).run(on, id, on === 1 ? 0 : 1);
  if (info.changes === 0) return { changed: false };   // 既に同じ値 (二重送信)
  logEvent(db, id, 'detail_images_excluded',
    on === 1 ? '詳細画像を「対象外」にしました (TOP画像のみで出品ゲートが開きます)' : '詳細画像の「対象外」を解除しました',
    actor);
  return { changed: true };
}

/**
 * 工程の状態・担当者・メモを更新する。
 * done にした時刻と操作者を残すのは、あとで「この工程は誰が何日で回しているか」を測るため。
 */
export function setStepState(
  draftId, stepCode, patch, actor,
  { isAdmin = false, actorStaffId = null, requireVersion = false } = {},
) {
  const db = getDB();
  const id = Number(draftId);
  const code = String(stepCode || '');
  ensureProgress(db, id);
  const row = db.prepare(`
    -- role_code は権限判定に使う (システム工程かどうか)。取り忘れると undefined になり、
    -- 通常の工程まで「システム工程」と誤判定して一般ユーザーが弾かれる
    SELECT p.*, s.label, s.role_code, s.track, s.image_kind FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code
    WHERE p.draft_id = ? AND p.step_code = ?
  `).get(id, code);
  if (!row) throw badRequest('この商品にその工程がありません');
  assertStepPermission(db, row, patch, { isAdmin, actorStaffId });
  // TOP画像 (サムネイル) は楽天出品に必須なので、admin でも工程単位の「対象外」にはできない。
  // 詳細画像を作らない商品は setDetailImagesExcluded (商品単位のフラグ) を使う
  if (patch?.state === 'skip' && row.track === 'image' && row.image_kind !== 'detail') {
    throw badRequest('TOP画像の工程は「対象外」にできません (サムネイルは楽天出品に必須です)');
  }

  const sets = [];
  const params = { draft_id: id, step_code: code };
  let event = null;
  let stateChanged = false;

  if (patch?.state !== undefined) {
    const state = String(patch.state);
    if (!STEP_STATES.includes(state)) throw badRequest('状態の指定が不正です');
    // 「出品・展開」の完了はモール側が正 (Codex R2 medium)。
    // ここを素通りさせると、状態セレクトから直接完了にして展開漏れを隠せてしまう
    if (state === 'done' && code === LISTING_STEP) {
      const prov = db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(id);
      if (prov?.provisional_code === 1) {
        throw badRequest('商品コードが仮のままです。ネクストエンジンの本コードに差し替えてから完了にしてください');
      }
      // **未完了の行を数える**のではなく、決着した行が全モール分あるかを見る (Codex R3)。
      // 行がまだ作られていないドラフト (詳細画面を開いていない・API直叩き) では
      // 「未完了 0 件」になり、1 モールも出していないのに完了できてしまう。
      // さらに現行のモールコードに限定し、DISTINCT で数える (Codex R4):
      // mall 列に CHECK を張っていない (将来の追加のため) ので、廃止済みのコードが
      // 残っていると行数だけでは 6 件に達してしまう
      const settled = db.prepare(`
        SELECT COUNT(DISTINCT mall) AS c FROM draft_mall_status
        WHERE draft_id = ? AND state IN ('done', 'skip')
          AND mall IN (${MALLS.map(() => '?').join(',')})
      `).get(id, ...MALLS.map((m) => m.code)).c;
      if (settled < MALLS.length) {
        throw badRequest(`まだ展開していないモールが ${MALLS.length - settled} 件あります。「モール別の展開状況」で進めると自動で完了になります`);
      }
    }
    if (state !== row.state) {
      stateChanged = true;
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
    // 書式だけでなく実在日付かも見る。2026-02-31 のような値は期限警告と並び替えを狂わせる
    if (due !== '' && !isRealDate(due)) throw badRequest('期限は実在する日付を YYYY-MM-DD で指定してください');
    sets.push('due_date = @due_date');
    params.due_date = due === '' ? null : due;
  }

  // 楽観ロック (Codex R1 → R2 → R3)。**単調増加の version** をトークンにする。
  // updated_at (ミリ秒精度) だと同一ミリ秒内の連続更新で値が変わらず、古い画面からの
  // 上書きをすり抜けさせる。version なら必ず変わる。
  // 画面から来る操作 (requireVersion) はトークン必須にし、省略による回避を塞ぐ。
  const expected = patch?.expected_version == null ? null : Number(patch.expected_version);
  if (requireVersion && !Number.isInteger(expected)) {
    throw conflict('画面が古い可能性があります。読み直してから操作してください');
  }
  const conds = ['draft_id = @draft_id', 'step_code = @step_code'];
  if (Number.isInteger(expected)) {
    if (expected !== row.version) {
      throw conflict('別の人がこの工程を先に更新しました。画面を読み直してください');
    }
    conds.push('version = @expected_version');
    params.expected_version = expected;
  } else if (stateChanged) {
    // トークン無しの内部呼び出し向けの保険 (状態の取り違えだけは防ぐ)
    conds.push('state = @prev_state');
    params.prev_state = row.state;
  }
  // 版数の検証は「更新する内容があるか」より**前**に済ませる (Codex R2 medium)。
  // 後ろに置くと、既に done の工程へ同じ状態を送る操作 (= 更新なし) が版数検査を素通りし、
  // 古い画面からの二重送信を CAS で止められない
  if (sets.length === 0) return { changed: false };
  const where = `WHERE ${conds.join(' AND ')}`;

  // UPDATE・監査ログ・**新しい版数の読み出し**を 1 トランザクションに閉じる。
  // 読み出しを外に出すと、その隙に別プロセスが更新した版数を返してしまい、
  // 画面が「自分が確認していない更新」の版数を持って次の楽観ロックをすり抜ける (Codex R4)
  const run = db.transaction(() => {
    const info = db.prepare(`
      UPDATE draft_step_progress
      SET ${sets.join(', ')}, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      ${where}
    `).run(params);
    if (info.changes !== 1) return null;
    if (event) logEvent(db, id, 'step_changed', event, actor);
    return db.prepare(
      'SELECT version, updated_at FROM draft_step_progress WHERE draft_id = ? AND step_code = ?'
    ).get(id, code);
  });
  const fresh = run();
  if (!fresh) {
    throw conflict('別の人がこの工程を先に更新しました。画面を読み直してください');
  }
  // 続けて操作できるよう、新しい版数を返す (画面が次の楽観ロックに使う)
  return { changed: true, version: fresh.version ?? null, updated_at: fresh.updated_at || null };
}

/**
 * かんばん 1 枚分のデータ。列 (工程) ごとにカードを振り分けて返す。
 *
 * ボードは 1 枚で、view で「本流の工程で並べる」「画像の工程で並べる」を切り替える
 * (2026-08-24 中原さん: 上下 2 段だと同じ商品が 2 箇所に出て 2 重管理に見える)。
 * どちらのビューでもカードには本流・画像 (TOP/詳細) 両方の進捗を出す。
 *
 * 現在工程の判定は SQL でなく JS で行う。ph_steps.sort は管理画面から編集できる数値で
 * 重複しうるため、「sort が最小の未完了行」を SQL で取ると同点のとき列が不定になる。
 *
 * @param {'main'|'image'} opts.view 列の軸。main = 本流の工程、image = 画像の工程 (カードは 商品×種別)
 * @param {object} opts.assigneeId  この担当者のボールだけに絞る (null = 全部)
 * @param {boolean} opts.unassignedOnly 担当者が決まっていないカードだけ
 * @returns {{view: string, columns: Array, doneCards: Array, doneTotal: number, total: number, truncated: boolean}}
 */
export function boardData(db, { view = 'main', assigneeId = null, unassignedOnly = false, limit = 800, mallSummary = null } = {}) {
  const steps = db.prepare(`
    SELECT code, label, track, image_kind, image_stage, sort, role_code, stall_days FROM ph_steps
    WHERE active = 1 ORDER BY track = 'image', sort, code
  `).all();

  // 進捗行がまだ 1 つも無いドラフトを先に埋める。これをやらないと、下の絞り込みが
  // draft_step_progress を見る以上、新規ドラフトが「自分のボール」に永久に出てこない
  ensureMissingProgress(db);

  // 絞り込みがあるときは **LIMIT の前に** 対象を絞る (Codex R1 medium)。
  // 全件から updated_at 順に 800 件取ってから絞ると、件数が増えたとき古い未完了商品が
  // 「自分の担当」「未割り当て」から消える = 担当漏れを探す画面として役に立たなくなる
  // 「現在工程」= 本流・画像TOP・画像詳細の系列ごとに、未完了 (todo/doing) のうち
  // sort→code が最小の行。JS 側の currentOf と**同じ並び順**を ROW_NUMBER() で再現する (Codex R2 medium)。
  // 「未完了工程のどれかを担当している」で絞ると、将来工程だけ担当している商品が
  // LIMIT を食い潰し、いま担当している古い商品がボードから消える
  // 画像ビューの候補は**画像工程だけ**から取る (Codex R1 medium):
  // 本流だけが条件一致する商品が LIMIT を食い潰し、あとの JS 絞り込みで消えると
  // 実際に画像を担当している商品がボードから欠落する
  const CURRENT_STEPS = `
    SELECT draft_id, assignee_id, role_code FROM (
      SELECT p.draft_id, p.assignee_id, s.role_code,
             ROW_NUMBER() OVER (
               PARTITION BY p.draft_id, s.track, COALESCE(s.image_kind, '')
               ORDER BY s.sort, s.code
             ) AS rn
      FROM draft_step_progress p
      JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.state IN ('todo', 'doing')
        ${view === 'image' ? "AND s.track = 'image'" : ''}
    ) WHERE rn = 1
  `;
  let candidateSql = '';
  const candidateParams = [];
  if (assigneeId != null) {
    candidateSql = `AND d.id IN (SELECT draft_id FROM (${CURRENT_STEPS}) WHERE assignee_id = ?)`;
    candidateParams.push(assigneeId);
  } else if (unassignedOnly) {
    // システム工程 (担当ロールなし) は「未割り当て」ではないので拾わない
    candidateSql = `AND d.id IN (
      SELECT draft_id FROM (${CURRENT_STEPS}) WHERE assignee_id IS NULL AND role_code IS NOT NULL
    )`;
  }

  // 保留・除外は工程の外に退避している状態なので、ボードには載せない (列が汚れる)
  const drafts = db.prepare(`
    SELECT d.id, d.ne_code, d.name, d.status, d.created_at, d.updated_at, d.detail_images_excluded,
      (SELECT drive_file_id FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_id,
      (SELECT drive_modified_time FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_mtime
    FROM product_drafts d
    WHERE d.status NOT IN ('on_hold', 'excluded')
    ${candidateSql}
    ORDER BY d.updated_at DESC
    LIMIT ?
  `).all(...candidateParams, limit + 1);
  const truncated = drafts.length > limit;
  if (truncated) drafts.length = limit;

  // 工程を後から足した場合の穴埋め (行はあるが足りないケース)
  ensureProgressForMany(db, drafts.map((d) => d.id));
  const summary = progressSummaryFor(db, drafts.map((d) => d.id));

  // 画像ビューの列は image_stage でまとめる (TOP/詳細 の同じ段階が 1 列)。
  // stage の無いカスタム工程は code 単独で 1 列にする — 黙って消さない
  let columns;
  if (view === 'image') {
    const byStage = new Map();
    for (const s of steps.filter((x) => x.track === 'image')) {
      const key = s.image_stage || `code:${s.code}`;
      if (!byStage.has(key)) {
        byStage.set(key, { key, label: s.label, sort: s.sort, role_code: s.role_code, stall_days: s.stall_days, stepCodes: [], cards: [] });
      }
      byStage.get(key).stepCodes.push(s.code);
    }
    columns = [...byStage.values()].sort((a, b) => a.sort - b.sort || String(a.key).localeCompare(String(b.key)));
  } else {
    columns = steps.filter((s) => s.track === 'main').map((s) => ({ ...s, cards: [] }));
  }
  const colByStep = new Map();
  for (const c of columns) {
    for (const code of (c.stepCodes || [c.code])) colByStep.set(code, c);
  }
  const doneCards = [];
  // 表示に残ったカードだけ後からモール状況を足す (全件に足すと無駄なクエリになる)
  const cardsById = new Map();

  // カードに常時出す画像側のサマリー (どちらのビューでも同じものを見せる = 2 重管理をなくす)
  const imageSummaryOf = (p, d) => ({
    top: {
      steps: p.imageTop.rows.map((r) => ({ state: r.state, label: r.label })),
      current: p.imageTop.current, done: p.imageTop.done, stalledDays: p.imageTop.stalledDays,
    },
    detail: d.detail_images_excluded === 1
      ? { excluded: true, steps: [], current: null, done: false, stalledDays: null }
      : {
        steps: p.imageDetail.rows.map((r) => ({ state: r.state, label: r.label })),
        current: p.imageDetail.current, done: p.imageDetail.done, stalledDays: p.imageDetail.stalledDays,
      },
  });

  for (const d of drafts) {
    const p = summary.get(d.id);
    if (!p) continue;
    const detailExcluded = d.detail_images_excluded === 1;
    const kinds = [
      { kind: 'top', s: p.imageTop, excluded: false },
      { kind: 'detail', s: p.imageDetail, excluded: detailExcluded },
    ];
    const baseCard = {
      id: d.id, ne_code: d.ne_code, name: d.name, status: d.status,
      first_image_id: d.first_image_id, first_image_mtime: d.first_image_mtime,
      current: p.current, stalledDays: p.stalledDays, doneCount: p.doneCount, totalCount: p.totalCount,
      image: imageSummaryOf(p, d),
    };

    if (view === 'image') {
      // カード = 商品 × 種別 (TOP/詳細 は別々に進むので別の列に出る)。
      // 絞り込みは**その種別の**現在工程を基準にする — 本流や他種別の一致で
      // 無関係な列にカードが出ると、担当者が自分の作業と誤読する (Codex設計相談)
      let anyOpen = false;
      for (const k of kinds) {
        if (k.excluded || k.s.rows.length === 0) continue;
        const cur = k.s.current;
        if (!cur) continue;
        anyOpen = true;
        if (assigneeId != null && cur.assignee_id !== assigneeId) continue;
        if (unassignedOnly && !(cur.role_code && cur.assignee_id == null)) continue;
        const card = { ...baseCard, kind: k.kind, kindCurrent: cur, kindStalledDays: k.s.stalledDays };
        colByStep.get(cur.step_code)?.cards.push(card);
        cardsById.set(d.id, card);
      }
      // 完了列 = TOP が済み、詳細も済みか対象外の商品 (絞り込み中は出さない — 「終わった分」は全員共通)。
      // TOP 全部 skip は「完了」に見せない (出品ゲートは拒否するので表示と食い違う — Codex R1 medium)
      if (!anyOpen && assigneeId == null && !unassignedOnly) {
        const topSettled = p.imageTop.rows.length > 0 && !p.imageTop.current
          && !p.imageTop.rows.every((r) => r.state === 'skip');
        // 詳細工程 0 件はゲートが拒否するので完了に見せない (Codex R2 high)
        const detailSettled = detailExcluded || (p.imageDetail.rows.length > 0 && !p.imageDetail.current);
        if (topSettled && detailSettled) doneCards.push(baseCard);
      }
      continue;
    }

    // 本流ビュー。絞り込みは本流・画像 (TOP/詳細) のどれかのボールが一致すれば残す。
    // 画像担当の人が「自分の分だけ」を見たとき、本流が別担当でもカードが要る
    const activeKindCurrents = kinds.filter((k) => !k.excluded).map((k) => k.s.current).filter(Boolean);
    if (assigneeId != null) {
      const hit = p.current?.assignee_id === assigneeId
        || activeKindCurrents.some((c) => c.assignee_id === assigneeId);
      if (!hit) continue;
    }
    if (unassignedOnly) {
      // システム工程 (担当ロールが無い = AI待ち) は「未割り当て」ではないので数えない
      const mainUnassigned = p.current && p.current.role_code && p.current.assignee_id == null;
      const imageUnassigned = activeKindCurrents.some((c) => c.role_code && c.assignee_id == null);
      if (!mainUnassigned && !imageUnassigned) continue;
    }
    if (p.current) colByStep.get(p.current.step_code)?.cards.push(baseCard);
    else doneCards.push(baseCard);
    cardsById.set(d.id, baseCard);
  }

  // 「出品・展開」のカードはモールの進み具合が要る (どこまで並んだかが本体なので)。
  // 循環 import を避けるため、呼び出し側から渡された関数で解決する
  if (view === 'main' && mallSummary && cardsById.size > 0) {
    const malls = mallSummary(db, [...cardsById.keys()]);
    for (const [id, card] of cardsById) card.malls = malls.get(id) || null;
  }

  // 停滞しているものを上に出す (打ち手が要るカードを埋もれさせない)
  const order = (a, b) => ((b.kindStalledDays ?? b.stalledDays) || 0) - ((a.kindStalledDays ?? a.stalledDays) || 0) || a.id - b.id;
  for (const c of columns) c.cards.sort(order);

  return {
    view,
    columns,
    // 完了は溜まる一方なので直近だけ (全部見たいときは一覧から)
    doneCards: doneCards.slice(0, 30),
    doneTotal: doneCards.length,
    total: drafts.length,
    truncated,
  };
}

/**
 * 一覧・かんばん用に複数ドラフトの進捗をまとめて引く (行ごとに引くと N+1)。
 * @returns {Map<number, {current, imageTop, imageDetail, ...}>}
 */
export function progressSummaryFor(db, draftIds) {
  const ids = (Array.isArray(draftIds) ? draftIds : []).map(Number).filter(Number.isInteger);
  const out = new Map();
  if (ids.length === 0) return out;
  const placeholders = ids.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT p.draft_id, p.step_code, p.state, p.assignee_id, p.done_at, p.started_at,
           s.label, s.track, s.image_kind, s.image_stage, s.sort, s.stall_days, s.role_code,
           st.name AS assignee_name, st.color AS assignee_color,
           -- 先頭工程が止まっている場合は前工程の完了日時が無いので、ドラフト作成日時を起点にする
           d.created_at AS draft_created_at, d.detail_images_excluded
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
    const kinds = splitImageRows(image);
    const detailExcluded = all[0]?.detail_images_excluded === 1;
    const current = currentOf(main);
    const createdAt = all[0]?.draft_created_at || null;
    const imageTop = kindSummaryOf(kinds.top, createdAt);
    const imageDetail = kindSummaryOf(kinds.detail, createdAt, detailExcluded);
    out.set(id, {
      current,
      imageTop,
      imageDetail,
      detailExcluded,
      imageDone: imageTop.done && (detailExcluded || imageDetail.done),
      stalledDays: current ? stalledDaysOf(main, main.indexOf(current), createdAt) : null,
      doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
      totalCount: main.length,
    });
  }
  return out;
}
