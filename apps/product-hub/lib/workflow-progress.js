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
 *   - **product_drafts.status は工程からの導出値** (2026-08-24 PR4 で切替)。
 *     手で遷移させるのは 保留/除外/再開 だけで、それ以外は工程・モールの変化時に
 *     recomputeDraftStatus が再計算して書く。status 列を残すのは、AI キュー (ready_for_ai) の
 *     claim/lease と一覧タブが status を引き続き参照するため (実体化された導出値)
 */
import { getDB, logEvent, gateReasons, imageTrackV2At, MATERIAL_STATUS_LABELS, GENERATION_BLOCK_CODES, CHECKING_REASON_LABELS } from '../db.js';
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

/** 工程の外に退避している状態 (再開するまで導出の対象外) */
export const ESCAPE_STATUSES = ['on_hold', 'excluded'];

/**
 * 工程 → status の導出 (PR4 2026-08-24)。STATUS_DONE_STEPS の逆写像。
 *
 * 判定に使うのは**組み込みの本流工程だけ**。管理画面から足したカスタム工程や
 * セット商品作成検討 (set_review) は status を左右しない — set_review は出品と並走する
 * 「検討」であって、止まっていても楽天出品や展開の妨げにならないため
 * (STATUS_DONE_STEPS で listed に set_review が入っているのは「listed まで進んだ商品なら
 * 検討も済んでいるはず」という初回推定であり、導出の必要条件ではない)。
 *
 * listed (楽天出品済み) だけは工程でなく実態で判定する: 楽天モールが done か、
 * draft_rakuten.registered_at (アプリからの登録記録) のどちらか。
 * 工程「出品・展開」は全モール決着で閉じるので expanded に対応する。
 */
export function deriveDraftStatus(db, draftId) {
  const id = Number(draftId);
  const rows = db.prepare(`
    SELECT step_code, state FROM draft_step_progress
    WHERE draft_id = ? AND step_code IN ('basic_info', 'ai_generate', 'desc_review', 'title_approve', 'listing')
  `).all(id);
  const settled = new Set(rows.filter((r) => r.state === 'done' || r.state === 'skip').map((r) => r.step_code));
  if (!settled.has('basic_info')) return 'draft';
  if (!settled.has('ai_generate')) return 'ready_for_ai';
  if (!settled.has('desc_review') || !settled.has('title_approve')) return 'review';
  if (settled.has('listing')) return 'expanded';
  const rakuten = db.prepare(`
    SELECT 1 FROM draft_mall_status WHERE draft_id = ? AND mall = 'rakuten' AND state = 'done'
  `).get(id) || db.prepare(`
    SELECT 1 FROM draft_rakuten WHERE draft_id = ? AND registered_at IS NOT NULL
  `).get(id);
  return rakuten ? 'listed' : 'approved';
}

/**
 * status を工程から再計算して書く。工程・モールを動かしたトランザクションの中から呼ぶ。
 * 保留・除外 (退避中) は触らない — 再開 (router の resume) だけが導出に戻す。
 * CAS (status = 読んだ値) で書くので、並行する別の再計算と競合しても後勝ちの取り違えは起きない。
 * @returns {{changed: boolean, status: string|null}}
 */
export function recomputeDraftStatus(db, draftId, { actor = null } = {}) {
  const id = Number(draftId);
  const draft = db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(id);
  if (!draft) return { changed: false, status: null };
  if (ESCAPE_STATUSES.includes(draft.status)) return { changed: false, status: draft.status };
  const next = deriveDraftStatus(db, id);
  if (next === draft.status) return { changed: false, status: draft.status };
  const info = db.prepare(`
    UPDATE product_drafts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ? AND status = ?
  `).run(next, id, draft.status);
  if (info.changes !== 1) return { changed: false, status: draft.status };
  logEvent(db, id, 'status_changed', `${draft.status} -> ${next} (工程から自動判定)`, actor || 'system');
  return { changed: true, status: next };
}

/** 「基本情報入力」を todo に差し戻す (ゲートの根拠が壊れたとき)。@returns 差し戻したか */
function resetBasicInfoStep(db, draftId, actor) {
  const info = db.prepare(`
    UPDATE draft_step_progress
    SET state = 'todo', done_at = NULL, done_by = NULL,
        version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE draft_id = ? AND step_code = 'basic_info' AND state IN ('done', 'skip')
  `).run(Number(draftId));
  if (info.changes === 1) {
    logEvent(db, Number(draftId), 'step_changed', '基本情報入力: 完了 → 未着手 (必須項目が外れたため自動差し戻し)', actor);
  }
  return info.changes === 1;
}

/**
 * 再開 (resume) 用: ゲートを再検証してから導出する。
 * 退避中は demoteIfGateBroken が no-op (status が ready_for_ai でない) なので、
 * 退避中に材料が壊された商品が再開の瞬間に AI キューへ入るのを防ぐ (Codex R1 High)。
 * 導出結果が ready_for_ai になるのに材料が無ければ「基本情報入力」を差し戻して導出し直す。
 * 呼び出し側のトランザクション内で使う想定。
 */
export function deriveWithGateCheck(db, draftId, actor) {
  const id = Number(draftId);
  ensureProgress(db, id);
  const next = deriveDraftStatus(db, id);
  if (next !== 'ready_for_ai') return next;
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  const reasons = draft ? gateReasons(db, draft) : [];
  if (reasons.length === 0) return next;
  resetBasicInfoStep(db, id, actor);
  logEvent(db, id, 'auto_demoted_to_draft', reasons.join(' / '), actor);
  return deriveDraftStatus(db, id);
}

/**
 * ゲート必須項目が後から壊された場合 (最後の画像削除など) の自動差し戻し。
 * PR4 で db.js から移設: status を直接書き換えるのではなく、根拠が壊れた
 * 「基本情報入力」工程を todo に戻し、status は導出で draft に落とす。
 * 対象は AI 生成前 (ready_for_ai) だけ — 生成後 (review 以降) は文章が既にあるので巻き戻さない。
 * @returns {string[]|null} 差し戻した場合はその理由、しなかった場合は null
 */
export function demoteIfGateBroken(db, draftId, actor) {
  const id = Number(draftId);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  if (!draft || draft.status !== 'ready_for_ai') return null;
  const reasons = gateReasons(db, draft);
  if (reasons.length === 0) return null;
  const run = db.transaction(() => {
    resetBasicInfoStep(db, id, actor);
    logEvent(db, id, 'auto_demoted_to_draft', reasons.join(' / '), actor);
    recomputeDraftStatus(db, id, { actor });
  });
  run();
  return reasons;
}

/**
 * 切替時の一回きりバックフィル (遅延実行)。画面表示・AIキューの入口から呼ばれ、
 * 未実施なら全ドラフトの status を工程から再計算して以後は何もしない。
 * 旧・手動遷移で listed/expanded になっていた商品は、楽天モール行に done の根拠を先に残す
 * (残さないと導出が approved へ巻き戻り、「楽天出品済み」の実態と食い違う)。
 */
const STATUS_BACKFILL_KEY = 'status_derive_backfilled';
export function maybeBackfillDerivedStatus(db) {
  const flag = db.prepare('SELECT value FROM ph_intake_state WHERE key = ?').get(STATUS_BACKFILL_KEY);
  if (flag?.value) return 0;
  const drafts = db.prepare(`
    SELECT id, status FROM product_drafts WHERE status NOT IN ('on_hold', 'excluded')
  `).all();
  let changed = 0;
  const now = nowIso();
  for (const d of drafts) {
    try {
      ensureProgress(db, d.id);
      if (d.status === 'listed' || d.status === 'expanded') {
        db.prepare(`
          INSERT OR IGNORE INTO draft_mall_status (draft_id, mall, state, listed_at)
          VALUES (?, 'rakuten', 'done', ?)
        `).run(d.id, now);
        db.prepare(`
          UPDATE draft_mall_status
          SET state = 'done', listed_at = COALESCE(listed_at, ?),
              version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          WHERE draft_id = ? AND mall = 'rakuten' AND state IN ('todo', 'doing')
        `).run(now, d.id);
      }
      if (recomputeDraftStatus(db, d.id, { actor: 'status-derive-backfill' }).changed) changed++;
    } catch (e) {
      // 1件の失敗で全体を止めない (次回呼び出しで再試行される)
      console.warn(`[product-hub] status backfill failed for draft ${d.id}:`, e.message);
      return changed;
    }
  }
  db.prepare(`INSERT INTO ph_intake_state (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
    .run(STATUS_BACKFILL_KEY, nowIso());
  return changed;
}

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
 *   - システム工程 (role_code なし = AI待ち・出品展開) … 手で進めるのは admin だけ
 *     (生成が終わったのに進まない、などの例外操作は管理者に寄せる)
 *   - boardClaim (かんばん D&D 専用・2026-08-27) … 未割り当ての人手工程に限り
 *     「自分が担当する + 状態変更」を 1 回の更新で許す (version 1 増・イベント 1 件)。
 *     admin 以外がボードで列を跨ぐたびに詳細画面で引き受けを押す手間をなくす
 */
function assertStepPermission(db, row, patch, { isAdmin, actorStaffId, boardClaim = false }) {
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
    const claimsSelf = Number(patch?.assignee_id) === actorStaffId;
    const onlyClaim = keys.length === 1 && keys[0] === 'assignee_id' && claimsSelf;
    // D&D の自動引き受け: 自分を担当に付けるのと同時に状態だけ変える (skip は本人でも不可なので同じく弾く)
    const boardClaimOk = boardClaim && claimsSelf && patch?.state !== 'skip'
      && keys.every((k) => k === 'assignee_id' || k === 'state');
    if (!onlyClaim && !boardClaimOk) throw forbidden('先に「自分が担当する」を押してから操作してください');
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
  const steps = db.prepare('SELECT code, track, image_kind, role_code, sort FROM ph_steps WHERE active = 1 ORDER BY track, sort').all();
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
    let statusKey = draft?.status;
    // 退避中 (on_hold/excluded) は STATUS_DONE_STEPS が空 = 全工程 todo で作られ、
    // 再開時に必ず draft へ巻き戻ってしまう (Codex R1 medium)。
    // 退避直前の status を監査ログ (status_changed 'X -> on_hold') から復元して推定に使う
    if (statusKey === 'on_hold' || statusKey === 'excluded') {
      const evs = db.prepare(`
        SELECT detail FROM draft_events
        WHERE draft_id = ? AND event = 'status_changed' ORDER BY id DESC LIMIT 20
      `).all(id);
      for (const e of evs) {
        const m = /^([a-z_]+) -> (on_hold|excluded)/.exec(e.detail || '');
        if (m && !ESCAPE_STATUSES.includes(m[1]) && STATUS_DONE_STEPS[m[1]]) { statusKey = m[1]; break; }
      }
    }
    doneSet = new Set(STATUS_DONE_STEPS[statusKey] || []);
    // 画像が既に登録されているなら **TOP側**の画像トラックは終わっているとみなす。
    // 登録済みの商品を「画像未依頼」で並べると、外注に二重依頼させかねない。
    // 詳細側は推定しない — 画像が 1 枚あることは詳細画像が揃っている根拠にならず、
    // done にすると TOP しか無い商品が出品ゲートを素通りする (Codex R1 critical と同じ穴)。
    // 楽天登録済みの商品だけは詳細側も done (出品済みに「承認して」を出さない)
    const hasImages = db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? LIMIT 1').get(id);
    if (hasImages) for (const s of steps) if (s.track === 'image' && s.image_kind !== 'detail') doneSet.add(s.code);
    // 楽天登録済みの根拠は registered_at だけでなく 導出 status / モール別状況の楽天 done も見る (Codex v2 R3 high:
    // 工程行がまだ無い既存の出品済み商品が、初回生成で詳細 v2 を全 todo で始めてしまう)
    const listedRk = db.prepare(`
      SELECT 1 WHERE EXISTS (SELECT 1 FROM draft_rakuten WHERE draft_id = @id AND registered_at IS NOT NULL)
         OR EXISTS (SELECT 1 FROM product_drafts WHERE id = @id AND status IN ('listed', 'expanded'))
         OR EXISTS (SELECT 1 FROM draft_mall_status WHERE draft_id = @id AND mall = 'rakuten' AND state = 'done')
    `).get({ id });
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
    // 途中に挿した画像工程 (例: 撮影依頼中 2026-08-25) は、その系列 (TOP/詳細) で
    // **先の工程に着手済み** (doing/done/skip) なら done で入れる (Codex R1 high:
    // todo で入れると currentOf が新工程を現在扱いし、ボード・出品ゲート・滞留判定が
    // 進行中のドラフトごと過去段階へ巻き戻る)。先が全部 todo なら従来どおり todo で入れる
    const existingStates = db.prepare(`
      SELECT p.state, s.sort, s.track, s.image_kind FROM draft_step_progress p
      JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.draft_id = ? AND s.track = 'image'
    `).all(id);
    const seriesOf = (s) => (s.image_kind === 'detail' ? 'detail' : 'top');
    for (const m of missing) {
      if (m.track !== 'image' || doneSet.has(m.code)) continue;
      const started = existingStates.some((e) => seriesOf(e) === seriesOf(m) && e.sort > m.sort && e.state !== 'todo');
      if (started) doneSet.add(m.code);
    }
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

/** ISO 文字列から今日までの経過日数 (0 以上の整数)。パースできなければ null */
function daysSinceIso(iso) {
  const t = Date.parse(iso || '');
  if (!Number.isFinite(t)) return null;
  return Math.max(0, Math.floor((Date.now() - t) / 86400000));
}

/** 画像種別の表示名。ph_steps.image_kind ('top'/'detail') に対応する */
export const IMAGE_KIND_LABELS = { top: 'TOP画像', detail: '商品詳細画像' };

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

/**
 * 「画像の制作はここまでで終わり」の境界 = ⑦Amazon登録依頼 (最終デザイン確認を兼ねる)。
 * これより後の ⑧楽天登録・⑨A+登録 は**作った画像をモールに載せる後工程**なので、
 * そこに来ていればカードは「済」にする (2026-09-01 中原さん:「画像で並べるステータスが
 * 楽天登録に移動したらカードは済にしてほしい」)。
 * 工程コードでなく image_stage で見るのは、管理画面での改名で壊れないため
 * (image_stage は「TOP/詳細の同じ段階を 1 列にまとめる安定キー」として置いてある)。
 */
export const IMAGE_MADE_BOUNDARY_STAGE = 'amazon';

/**
 * 商品詳細画像が作り終わっているか。
 * 🚨**並び順 (sort) に依存させない** (Codex R2/R3 medium): 管理画面で工程を並べ替え・追加できるので、
 * 「いま楽天登録にいる」「境界より前の行だけ見る」はどちらも崩れる (楽天を前に動かせば ⑥未完了でも
 * current が楽天になり、⑦自体を前に動かせば後ろに残った ⑥ を見落とす)。
 * 見るのは属性だけ:
 *   ① 楽天出品ゲートに数える工程 (listing_gate=1 = ①〜⑥) がすべて決着している (gateDone)
 *   ② 境界工程 ⑦Amazon登録依頼 (image_stage='amazon') が決着している
 * 境界の行が 1 つも無い (工程を消した) ときは「済」と偽らずに false へ倒す。
 * ただし全工程が決着していれば当然「作り終わっている」ので、そこは先に true。
 */
export function imageMadeOf(summary) {
  if (!summary || summary.excluded) return false;
  if (summary.done) return true;
  if (!summary.gateDone) return false;
  const boundary = (summary.rows || []).filter((r) => r.image_stage === IMAGE_MADE_BOUNDARY_STAGE);
  return boundary.length > 0 && boundary.every((r) => r.state === 'done' || r.state === 'skip');
}

/** 1 種別分のサマリー (current / done / 滞留)。excluded の種別は current を出さない */
function kindSummaryOf(rows, createdAt, excluded = false) {
  const current = excluded ? null : currentOf(rows);
  // 楽天出品ゲートに数える工程 (listing_gate=1) だけの決着。詳細 v2 の ⑦⑧⑨ は出品の後工程なので含めない
  const gateRows = rows.filter((r) => r.listing_gate !== 0);
  return {
    rows,
    current,
    excluded,
    done: !excluded && rows.length > 0 && !current,
    gateDone: !excluded && gateRows.length > 0 && !currentOf(gateRows),
    gateCurrent: excluded ? null : currentOf(gateRows),
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
           s.skippable, s.listing_gate,
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
    // 2026-08-31: TOP の工程は廃止したので、画像が終わったかは商品詳細 (LP) だけで決まる
    // (TOP画像そのものの有無は出品ゲート imageTrackBlockReason が画像の登録で見る)
    imageDone: detailExcluded || imageDetail.done,
    doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
    totalCount: main.length,
  };
}

/** 画像制作だけの保留 (draft_image_production.workflow_state)。行が無ければ active 扱い */
export function imageHoldOf(db, draftId) {
  const r = db.prepare('SELECT workflow_state, hold_note FROM draft_image_production WHERE draft_id = ?').get(Number(draftId));
  return { onHold: r?.workflow_state === 'on_hold', note: r?.hold_note || null };
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
  // 画像制作だけの保留 (2026-08-26)。工程が進んでいても保留中は出品しない
  const hold = imageHoldOf(db, draftId);
  if (hold.onHold) {
    return `画像制作が保留中です${hold.note ? ` (${hold.note})` : ''}。詳細画面の「画像制作」カードで保留を解除してください`;
  }
  // TOP画像は**工程ではなく画像が登録されているか**で見る (2026-08-31 中原さん決定 A)。
  // 工程は商品詳細 (LP) に一本化し、TOP の 4 工程は廃止した (RETIRED_TOP_STEP_CODES)。
  // サムネイル無しの楽天出品はありえないので、ここは引き続き fail-closed で止める
  // **枠1 (sort=0) がある**ことを見る (Codex R6 high): 画像が 1 行あるだけだと、
  // _top が無くて _01 だけ取り込まれた商品 (sort=1〜) が素通りする。
  // 枠1 = <商品コード>_top で、これが楽天のサムネイルになる (rakuten-listing の判定と同じ)
  const hasTopImage = db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND sort = 0 LIMIT 1').get(Number(draftId));
  if (!hasTopImage) {
    return 'TOP画像 (サムネイル) が登録されていません。画像フォルダに「_top」を置いて「フォルダから自動セット」で取り込んでください';
  }
  // 詳細側は fail-closed (Codex R2 high): 対象外にしていないのに工程が 0 件なら
  // 「揃っている」ではなく「設定が壊れている」— ここで通すと詳細画像の確認が丸ごと飛ぶ
  if (!p.detailExcluded && p.imageDetail.rows.length === 0) {
    return '画像トラック (詳細画像) の工程が見つかりません。詳細画像を作らない商品なら「詳細画像は対象外」にするか、担当者・工程の設定を確認してください';
  }
  const blocked = [];
  if (!p.detailExcluded && !p.imageDetail.gateDone) {
    blocked.push(`${IMAGE_KIND_LABELS.detail}: ${kindBlockDetail({ ...p.imageDetail, current: p.imageDetail.gateCurrent })}`);
  }
  if (blocked.length === 0) return null;
  return `画像の工程が終わっていません (${blocked.join(' ／ ')})。最後まで進めるか、`
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
  { isAdmin = false, actorStaffId = null, requireVersion = false, bypassGates = false, systemActor = false, boardClaim = false } = {},
) {
  const db = getDB();
  const id = Number(draftId);
  const code = String(stepCode || '');
  ensureProgress(db, id);
  const row = db.prepare(`
    -- role_code は権限判定に使う (システム工程かどうか)。取り忘れると undefined になり、
    -- 通常の工程まで「システム工程」と誤判定して一般ユーザーが弾かれる
    SELECT p.*, s.label, s.role_code, s.track, s.image_kind, s.skippable FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code
    WHERE p.draft_id = ? AND p.step_code = ?
  `).get(id, code);
  if (!row) throw badRequest('この商品にその工程がありません');
  // 画像制作だけの保留 (2026-08-26): 保留中は画像トラックの工程を動かせない (ボードの D&D も詳細画面もここを通る)。
  // 解除は詳細画面の「保留を解除」だけ (Codex R3 high: ゲートだけ閉じても工程が進むと「止める」にならない)
  if (row.track === 'image' && patch && Object.keys(patch).some((k) => k !== 'expected_version') && imageHoldOf(db, id).onHold) {
    throw badRequest('画像制作が保留中です。詳細画面の「画像制作」カードで保留を解除してから操作してください');
  }
  assertStepPermission(db, row, patch, { isAdmin, actorStaffId, boardClaim });
  // TOP画像 (サムネイル) は楽天出品に必須なので、admin でも工程単位の「対象外」にはできない。
  // 詳細画像を作らない商品は setDetailImagesExcluded (商品単位のフラグ) を使う
  if (patch?.state === 'skip' && row.track === 'image' && row.image_kind !== 'detail') {
    throw badRequest('TOP画像の工程は「対象外」にできません (サムネイルは楽天出品に必須です)');
  }
  // 2026-08-26 v2: 工程属性 skippable=0 は「対象外」にできない (詳細 ①〜⑥。⑦⑧⑨ は可)
  if (patch?.state === 'skip' && row.skippable === 0 && row.track === 'image') {
    throw badRequest(`「${row.label}」は対象外にできません`);
  }
  // 導出は skip も「決着」に数えるので、ゲートを迂回できる工程は skip を禁止する (Codex R1 medium):
  // basic_info の skip = 材料チェックなしで AI キュー入り / listing の skip = 展開せず「展開済み」表示
  if (patch?.state === 'skip' && code === 'basic_info') {
    throw badRequest('「基本情報入力」は対象外にできません (全商品で必須の工程です)');
  }
  if (patch?.state === 'skip' && code === LISTING_STEP) {
    throw badRequest('「出品・展開」は対象外にできません (展開しないモールは「モール別の展開状況」で対象外にすると自動で完了になります)');
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
    // 「基本情報入力」の完了 = AI 生成キューの入口 (PR4)。完了にした瞬間に status が
    // ready_for_ai へ導出されて夜間バッチが拾うので、生成の材料が揃っていなければ止める
    // (旧・手動遷移の「生成待ちにする」ボタンと同じゲートを工程側に引き継ぐ)
    if (state === 'done' && code === 'basic_info') {
      const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
      const reasons = draft ? gateReasons(db, draft) : [];
      if (reasons.length > 0) {
        throw badRequest(`完了にはまだ足りません: ${reasons.join(' / ')}`);
      }
    }
    // ⑧ 楽天登録 = 出品成功で自動完了する工程。人が (admin でも) 出品なしに done にできない (Codex R2 high)。
    // 例外 = 楽天登録の根拠 (アプリ経由の登録記録 / モール別状況の楽天 done) がある商品 (アプリ以前に手で出した商品)
    if (state === 'done' && code === 'imgd_rakuten' && !systemActor) {
      const evidence = db.prepare(`
        SELECT 1 WHERE EXISTS (SELECT 1 FROM draft_rakuten WHERE draft_id = @id AND registered_at IS NOT NULL)
           OR EXISTS (SELECT 1 FROM draft_mall_status WHERE draft_id = @id AND mall = 'rakuten' AND state = 'done')
      `).get({ id });
      if (!evidence) throw badRequest('「楽天登録」は楽天に出品すると自動で完了します (このアプリから出品するか、モール別の展開状況で楽天を完了にしてください。出さない商品は「対象外」)');
    }
    // 画像工程 v2 の完了条件 (2026-08-26)
    // ①③ の材料チェックは自社商品だけ (撮影・素材/商品情報は自社商品の画像制作カードでしか入力できない。
    // 仕入商品で詳細を作る場合は工程だけ進める)。⑥の順序は全商品
    const ownBrandDraft = (code === 'imgd_request' || code === 'imgd_material')
      ? db.prepare('SELECT own_brand FROM product_drafts WHERE id = ?').get(id)?.own_brand === 1 : false;
    // bypassGates = 移行 (Notion 画像DB 取り込み等) が「Notion 側で既に済んでいる段階」を写すときだけ。画面・D&D は必ずゲートを通る
    if (!bypassGates && state === 'done' && ((ownBrandDraft && (code === 'imgd_request' || code === 'imgd_material')) || code === 'imgd_review_2')) {
      const ip = db.prepare('SELECT material_status, product_info_text FROM draft_image_production WHERE draft_id = ?').get(id) || {};
      if (code === 'imgd_request') {
        // ① = 撮影要否の判断 + 商品情報 (1.5)。商品情報は v2 切替後に作られた商品だけ必須 (移行データは例外 — 中原さん決定 7)
        if (!ip.material_status) throw badRequest('完了にはまだ足りません: 撮影・素材ステータス (撮影不要/未発送/…) を設定してください');
        const v2At = imageTrackV2At(db);
        const d = db.prepare('SELECT created_at, source FROM product_drafts WHERE id = ?').get(id) || {};
        // 取り込み由来 (Notion 画像DB・商品マスター) は「移行データ」なので必須にしない (中原さん決定 7)
        if (v2At && (d.created_at || '') > v2At && d.source !== 'notion_import' && !String(ip.product_info_text || '').trim()) {
          throw badRequest('完了にはまだ足りません: 商品情報 (Amazon やパッケージを見て手入力) を入れてください');
        }
      }
      if (code === 'imgd_material' && ip.material_status !== 'ready' && ip.material_status !== 'not_required') {
        throw badRequest(`完了にはまだ足りません: 撮影・素材ステータスが「${MATERIAL_STATUS_LABELS[ip.material_status] || '未設定'}」です (素材完了 か 撮影不要 にしてください)`);
      }
      if (code === 'imgd_review_2') {
        // ⑥ は 田中確認 → 中原確認 の順 (サーバーで担保 — Codex R1)
        const r1 = db.prepare("SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'imgd_review_1'").get(id);
        if (r1 && r1.state !== 'done' && r1.state !== 'skip') throw badRequest('先に「社内確認 (田中)」を完了にしてください');
      }
    }
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
    // D&D の自動引き受けは、詳細画面で本人が正式に引き受けた操作と区別できるよう明記する (Codex R1)
    if (event) logEvent(db, id, 'step_changed', boardClaim ? `${event} (ボードD&D: 自動引き受け)` : event, actor);
    // 本流工程の状態が動いたら status を導出し直す (同一トランザクション = 食い違いを残さない)
    let draftStatus = null;
    if (stateChanged && row.track === 'main') {
      draftStatus = recomputeDraftStatus(db, id, { actor }).status;
    }
    // 2026-08-31: TOP画像の 4 工程を廃止したので、⑤⑥-2 からの自動追随も無くした。
    // (LP と TOP は同時進行で作るため工程を分けない — 中原さん。TOP の状態は
    //  「画像が登録されているか」で見る = imageTrackBlockReason)
    const freshRow = db.prepare(
      'SELECT version, updated_at FROM draft_step_progress WHERE draft_id = ? AND step_code = ?'
    ).get(id, code);
    return { ...freshRow, draftStatus };
  });
  const fresh = run();
  if (!fresh) {
    throw conflict('別の人がこの工程を先に更新しました。画面を読み直してください');
  }
  // 続けて操作できるよう、新しい版数を返す (画面が次の楽観ロックに使う)
  return {
    changed: true, version: fresh.version ?? null, updated_at: fresh.updated_at || null,
    draftStatus: fresh.draftStatus || null,
  };
}

/**
 * かんばんカードの D&D 移動 (2026-08-24 中原さん要望)。
 * 「落とした列がその商品のいまやる工程になる」ように工程をまとめて更新する:
 *   - 前方 (右) へ: 現在工程から移動先の手前までを順に done に (移動先は todo のまま = いまやる番)
 *   - 後方 (左) へ: 移動先の工程を todo に開け直す (間の done は触らない = currentOf が移動先を指す)
 *   - to='done' (完了列): 残りの工程を全部 done に。本流は listing の全モール決着チェックが効くので
 *     モール未決着なら失敗する (出品・展開はモール別ステータスが正、のルールを D&D でも維持)
 * 権限・ゲート (基本情報の材料チェック/システム工程は admin のみ/listing はモール正) は
 * setStepState を1工程ずつ通すことでそのまま効く。途中で弾かれたら全体をロールバック。
 * expectedCurrent = カードを掴んだ時点の現在工程 (ボード表示の CAS。ズレていたら 409)
 */
export function moveBoardCard(
  draftId, { view = 'main', kind = 'top', to = '', expectedCurrent = null } = {},
  actor, { isAdmin = false, actorStaffId = null } = {},
) {
  const db = getDB();
  const id = Number(draftId);
  const run = db.transaction(() => {
    ensureProgress(db, id);
    const rows = view === 'image'
      ? db.prepare(`
          SELECT p.step_code, p.state, s.image_stage FROM draft_step_progress p
          JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
          WHERE p.draft_id = ? AND s.track = 'image'
            AND ${kind === 'detail' ? "s.image_kind = 'detail'" : "(s.image_kind IS NULL OR s.image_kind != 'detail')"}
          ORDER BY s.sort, s.code
        `).all(id)
      : db.prepare(`
          SELECT p.step_code, p.state, NULL AS image_stage FROM draft_step_progress p
          JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
          WHERE p.draft_id = ? AND s.track = 'main'
          ORDER BY s.sort, s.code
        `).all(id);
    if (rows.length === 0) throw badRequest('工程が見つかりません');
    const rawIdx = rows.findIndex((r) => r.state !== 'done' && r.state !== 'skip');
    const currentCode = rawIdx === -1 ? null : rows[rawIdx].step_code;
    if ((expectedCurrent || null) !== currentCode) {
      throw conflict('ボードの表示が古くなっています。ページを読み直してください');
    }
    // 全工程決着 = 完了列のカード。expectedCurrent=null がその CAS 値で、
    // 差し戻し (後方移動) だけができる (Codex R1 medium: 完了からも戻せるように)
    const curIdx = rawIdx === -1 ? rows.length : rawIdx;

    let tIdx;
    if (to === 'done') {
      tIdx = rows.length;
    } else {
      // 列キー: 本流 = 工程 code / 画像ビュー = image_stage (段階)。stage の無いカスタム工程は code:xxx
      tIdx = rows.findIndex((r) => (view === 'image'
        ? (r.image_stage ? r.image_stage === to : `code:${r.step_code}` === to)
        : r.step_code === to));
      if (tIdx === -1) {
        // 画像ビューの列は TOP と 商品詳細 の工程を段階 (image_stage) でまとめたもの。
        // 片方にしか無い段階 (例: 「画像制作」= TOP のみ) へもう片方のカードを落とすと、
        // その種別には対応する工程が無いのでここに来る。「見つかりません」だけだと
        // 何が悪いのか分からないので、どちらの工程かを言う (2026-08-31 中原さん報告)
        const otherKind = kind === 'detail' ? 'top' : 'detail';
        const inOther = db.prepare(`
          SELECT label FROM ph_steps WHERE track = 'image' AND active = 1 AND image_stage = ?
            AND ${otherKind === 'detail' ? "image_kind = 'detail'" : "(image_kind IS NULL OR image_kind <> 'detail')"}
          LIMIT 1
        `).get(String(to));
        if (inOther) {
          throw badRequest(`この列 (${inOther.label}) は${otherKind === 'detail' ? '商品詳細画像' : 'TOP画像'}の工程です。`
            + `${kind === 'detail' ? '商品詳細画像' : 'TOP画像'}のカードは移動できません`);
        }
        throw badRequest('移動先の工程が見つかりません');
      }
    }
    if (tIdx === curIdx) return { changed: false };
    // 未割り当ての人手工程は「動かした本人が引き受けた」ことにして進める (2026-08-27 田中さん改善案:
    // 列を跨ぐたびに詳細画面で通過工程ぜんぶに「自分が担当する」を押さないと動かせなかった)。
    // 権限の方針 (本人 + admin のみ・未割り当ては誰でも引き受け可) は変えず、クリックを肩代わりするだけ。
    // 引き受けと状態変更は 1 回の setStepState (boardClaim) = version 1 増・イベント 1 件 (Codex R1)。
    // ここで付く担当者は「完了を確定した人」であって実作業者とは限らない — イベントに自動引き受けと明記して区別する。
    // 他人の担当工程・システム工程 (role_code なし) は従来どおり assertStepPermission が弾く (途中で弾かれたら全体ロールバック)
    const unassignedHuman = (code) => {
      if (isAdmin || actorStaffId == null) return false;
      const r = db.prepare(`
        SELECT p.assignee_id, s.role_code FROM draft_step_progress p
        JOIN ph_steps s ON s.code = p.step_code WHERE p.draft_id = ? AND p.step_code = ?
      `).get(id, code);
      return !!(r && r.assignee_id == null && r.role_code);
    };
    const setWithClaim = (code, state) => {
      const claim = unassignedHuman(code);
      setStepState(id, code, claim ? { assignee_id: actorStaffId, state } : { state }, actor,
        { isAdmin, actorStaffId, boardClaim: claim });
    };
    if (tIdx > curIdx) {
      for (let i = curIdx; i < tIdx; i++) setWithClaim(rows[i].step_code, 'done');
      // 移動先 (= いまやる番) も未割り当てなら移動者に付ける (Codex R1: ドラッグ = 「自分が次工程を持っていく」の意思表示。
      // 付けないと次の操作でまた「自分が担当する」が要る)。完了列 (tIdx = rows.length) には移動先が無い
      if (tIdx < rows.length && unassignedHuman(rows[tIdx].step_code)) {
        setStepState(id, rows[tIdx].step_code, { assignee_id: actorStaffId }, actor, { isAdmin, actorStaffId, boardClaim: true });
      }
    } else {
      setWithClaim(rows[tIdx].step_code, 'todo');
    }
    return { changed: true };
  });
  return run();
}

/** かんばんの列キー。本流ビューは工程コード、画像ビューは画像ステージのキー、終端は 'done' */
const BOARD_DONE_COL = 'done';

/**
 * かんばんの手動並び順を読む (2026-08-28)。
 * @returns {Map<string, {col: string, sort: number}>} キー = `${draft_id}|${kind}` (本流は kind='')
 */
export function loadBoardOrder(db, view, draftIds) {
  const ids = (Array.isArray(draftIds) ? draftIds : []).map(Number).filter(Number.isInteger);
  const out = new Map();
  if (ids.length === 0) return out;
  // SQLite の変数上限 (999) に当たらないよう分割して引く
  for (let i = 0; i < ids.length; i += 400) {
    const chunk = ids.slice(i, i + 400);
    const rows = db.prepare(`
      SELECT draft_id, kind, col, sort FROM ph_board_order
      WHERE view = ? AND draft_id IN (${chunk.map(() => '?').join(',')})
    `).all(view, ...chunk);
    for (const r of rows) out.set(`${r.draft_id}|${r.kind || ''}`, { col: r.col, sort: r.sort });
  }
  return out;
}

/** そのビューで実在する列キー (本流=工程コード / 画像=画像ステージ / 終端=done) */
export function boardColumnKeys(db, view) {
  const keys = new Set([BOARD_DONE_COL]);
  const rows = db.prepare(`SELECT code, track, image_stage FROM ph_steps WHERE active = 1`).all();
  for (const r of rows) {
    if (view === 'image') {
      if (r.track === 'image') keys.add(r.image_stage || `code:${r.code}`);
    } else if (r.track === 'main') {
      keys.add(r.code);
    }
  }
  return keys;
}

/**
 * 1 列ぶんの並び順を保存する (2026-08-28 中原さん要望)。
 *
 * 画面から**その列に見えているカードの順番**を受け取る。1 枚ぶんの差分だけを受け取る方式だと、
 * 既定順のカードと手動順のカードが混ざったとき「動かしたのに戻る」が再発するため。
 *
 * ただし画面に見えているのは列の一部でしかない (担当者・未割り当て・種別の絞り込み、
 * 完了列の直近 30 件、検索)。受け取った順番でそのまま 0 から振り直すと、**画面に出ていない
 * カードの順番を巻き込んで壊す** (Codex R1 高)。そこで既存の並びに**差し込む**:
 *   - 送られてきたカードは、そのカードたちが今占めている位置の中だけで入れ替える
 *   - 送られてこなかったカード (絞り込みで隠れている等) は今の位置のまま動かさない
 *   - 手動順をまだ持たないカードは、送信順で前後のカードの間に入る
 *
 * 競合は最後勝ち。表示順というやり直しの効く情報なので、409 を出して操作を止めない。
 *
 * @param {Array<{id: number, kind?: string}>} items 上から順のカード (画面に見えている分)
 */
export function saveBoardOrder(db, { view, col, items }) {
  if (view !== 'main' && view !== 'image') throw badRequest('ビューの指定が不正です');
  const colKey = String(col || '').trim();
  if (!colKey) throw badRequest('列が指定されていません');
  if (!boardColumnKeys(db, view).has(colKey)) throw badRequest(`列 ${colKey} は存在しません`);

  const seen = new Set();
  const sent = [];
  for (const raw of (Array.isArray(items) ? items : [])) {
    const id = Number(raw?.id);
    if (!Number.isInteger(id) || id <= 0) throw badRequest('カードの指定が不正です');
    let kind = '';
    if (view === 'image') {
      kind = String(raw?.kind ?? '');
      if (kind !== 'top' && kind !== 'detail') throw badRequest('画像の種別が不正です');
    }
    const key = `${id}|${kind}`;
    // 同じカードが 2 回来たら並びが決まらない (壊れた画面か改ざん)
    if (seen.has(key)) throw badRequest('同じカードが重複しています');
    seen.add(key);
    sent.push({ id, kind, key });
  }
  if (sent.length === 0) return { saved: 0 };
  // 実在しないドラフトは弾く (FK でも落ちるが、理由の分かるエラーにする)
  const exists = new Set(db.prepare(
    `SELECT id FROM product_drafts WHERE id IN (${sent.map(() => '?').join(',')})`
  ).all(...sent.map((x) => x.id)).map((r) => r.id));
  for (const x of sent) if (!exists.has(x.id)) throw badRequest(`商品 #${x.id} は存在しません`);

  return db.transaction(() => {
    const existing = db.prepare(`
      SELECT draft_id, kind FROM ph_board_order WHERE view = ? AND col = ? ORDER BY sort, draft_id, kind
    `).all(view, colKey).map((r) => ({ id: r.draft_id, kind: r.kind || '', key: `${r.draft_id}|${r.kind || ''}` }));
    const existingKeys = new Set(existing.map((x) => x.key));
    const sentKeys = new Set(sent.map((x) => x.key));

    // 既存の並びの「送られてきたカードが占めている位置」に、送信順で入れ直す。
    // 送信順のうち手動順をまだ持たないカードは、次に来る既存カードの直前に差し込む
    const merged = [];
    let i = 0;
    for (const cur of existing) {
      if (!sentKeys.has(cur.key)) { merged.push(cur); continue; }
      while (i < sent.length && !existingKeys.has(sent[i].key)) merged.push(sent[i++]);
      if (i < sent.length) merged.push(sent[i++]);
    }
    while (i < sent.length) merged.push(sent[i++]);

    const up = db.prepare(`
      INSERT INTO ph_board_order (view, draft_id, kind, col, sort, updated_at)
      VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      ON CONFLICT (view, draft_id, kind) DO UPDATE SET
        col = excluded.col, sort = excluded.sort, updated_at = excluded.updated_at
    `);
    merged.forEach((x, n) => up.run(view, x.id, x.kind, colKey, n));
    return { saved: sent.length, ordered: merged.length };
  })();
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
 * @param {boolean} opts.checkingOnly 「確認中」のカードだけ (2026-08-31: 情報待ちの商品を拾う)
 * @returns {{view: string, columns: Array, doneCards: Array, doneTotal: number, total: number, truncated: boolean, checkingTotal: number}}
 */
export function boardData(db, { view = 'main', assigneeId = null, unassignedOnly = false, checkingOnly = false, imageKind = null, limit = 800, mallSummary = null } = {}) {
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
  // 実際に画像を担当している商品がボードから欠落する。
  // 種別絞り込み時は候補もその種別に限定する (Codex R1 medium: kind=top&assignee=X で
  // 「詳細側だけ X 担当」の商品が候補・total・LIMIT を消費し、表示と件数がズレる)。
  // imageKind はホワイトリスト検証済みの値だけをリテラル展開する
  const kindSafe = view === 'image' && (imageKind === 'top' || imageKind === 'detail') ? imageKind : null;
  const CURRENT_STEPS = `
    SELECT draft_id, assignee_id, role_code FROM (
      SELECT p.draft_id, p.assignee_id, s.role_code,
             ROW_NUMBER() OVER (
               ${/* 系列の区分は splitImageRows と同じ「detail 以外は TOP」(Codex R3):
                    COALESCE(image_kind,'') だと NULL のカスタム画像工程が別系列になり、
                    JS 側の現在工程判定とズレて候補・total・LIMIT を誤消費する */''}
               PARTITION BY p.draft_id, s.track,
                 CASE WHEN s.track = 'image' AND s.image_kind = 'detail' THEN 'detail'
                      WHEN s.track = 'image' THEN 'top' ELSE '' END
               ORDER BY s.sort, s.code
             ) AS rn
      FROM draft_step_progress p
      JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.state IN ('todo', 'doing')
        ${view === 'image' ? "AND s.track = 'image'" : ''}
        ${/* TOP系列は image_kind IS NULL のカスタム画像工程も含む (splitImageRows と同じ区分 — Codex R2) */''}
        ${kindSafe === 'detail' ? "AND s.image_kind = 'detail'" : ''}
        ${kindSafe === 'top' ? "AND (s.image_kind IS NULL OR s.image_kind != 'detail')" : ''}
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

  // 保留・除外は工程の外に退避している状態なので、ボードには載せない (列が汚れる)。
  // 画像ビューでは「詳細画像は対象外」の商品も候補から外す (Codex R1 medium): 画像の工程は
  // 詳細 (LP) の 1 本なのでカードにならず、候補に残すと LIMIT を食って実際に作業がある商品が欠ける
  const drafts = db.prepare(`
    SELECT d.id, d.ne_code, d.name, d.status, d.created_at, d.updated_at, d.detail_images_excluded, d.image_priority, d.own_brand,
      d.generation_block_code, d.generation_block_reason,
      d.checking_reason_code, d.checking_note, d.checking_since,
      (SELECT workflow_state FROM draft_image_production ip WHERE ip.draft_id = d.id) AS image_workflow_state,
      (SELECT hold_note FROM draft_image_production ip WHERE ip.draft_id = d.id) AS image_hold_note,
      (SELECT material_status FROM draft_image_production ip WHERE ip.draft_id = d.id) AS material_status,
      (SELECT canva_url FROM draft_image_production ip WHERE ip.draft_id = d.id) AS canva_url,
      (SELECT CASE WHEN TRIM(COALESCE(product_info_text, '')) = '' THEN 0 ELSE 1 END FROM draft_image_production ip WHERE ip.draft_id = d.id) AS has_product_info,
      (SELECT drive_file_id FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_id,
      (SELECT drive_modified_time FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_mtime,
      ${/* TOP画像が作られたか (2026-09-01 カード表示用)。枠1 = sort=0 = <商品コード>_top が
            楽天のサムネイルになるので、出品ゲート imageTrackBlockReason と同じ判定にする。
            画像が 1 行あるだけの判定にすると、_01 だけ取り込まれた商品が「済」に見える */''}
      (SELECT 1 FROM draft_images i WHERE i.draft_id = d.id AND i.sort = 0 LIMIT 1) AS has_top_image,
      ${/* ボードから楽天に出品した結果 (2026-09-01)。出品・展開の列のカードだけが読む。
            registered_at があれば「登録済み」、無くて last_error があれば「失敗 (理由)」 */''}
      (SELECT registered_at FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_registered_at,
      (SELECT last_error FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_last_error,
      (SELECT listing_outcome FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_listing_outcome,
      (SELECT listing_attempt_at FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_listing_attempt_at
    FROM product_drafts d
    WHERE d.status NOT IN ('on_hold', 'excluded')
    ${candidateSql}
    ${checkingOnly ? 'AND d.checking_since IS NOT NULL' : ''}
    ${view === 'image' ? 'AND d.detail_images_excluded = 0' : ''}
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
    // 種別絞り込み時は、その系列に存在する工程だけから列を作る (Codex R1 medium:
    // 片側にしか無い段階 (例: 撮影依頼中=詳細のみ) の列が kind=top でも出て、
    // 落とせない空列になる)。区分は splitImageRows と同じ「detail 以外は TOP」
    for (const s of steps.filter((x) => x.track === 'image'
        && (!kindSafe || (kindSafe === 'detail' ? x.image_kind === 'detail' : x.image_kind !== 'detail')))) {
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
    // TOP画像は工程を廃止した (2026-08-31) ので **枠1 が登録されているか** が作成済みの判定。
    // 工程ベースの p.imageTop は active=0 で常に空 = 常に「未完了」に見えるため使わない
    top: { registered: d.has_top_image === 1 },
    detail: d.detail_images_excluded === 1
      ? { excluded: true, steps: [], current: null, done: false, made: false, stalledDays: null }
      : {
        steps: p.imageDetail.rows.map((r) => ({ state: r.state, label: r.label })),
        current: p.imageDetail.current, done: p.imageDetail.done,
        // made = 「画像はもう作り終わっている」(2026-09-01 中原さん:「画像で並べるステータスが
        // 楽天登録に移動したらカードは済にしてほしい」)。⑧楽天登録・⑨A+登録 は作った画像を
        // モールに載せる後工程なので、ここまで来たカードは制作としては終わっている。
        // ⑦Amazon登録依頼 は最終デザイン確認を兼ねるので「まだ」のまま (中原さんの線引き)
        made: imageMadeOf(p.imageDetail),
        stalledDays: p.imageDetail.stalledDays,
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
      id: d.id, ne_code: d.ne_code, name: d.name, status: d.status, image_priority: d.image_priority,
      first_image_id: d.first_image_id, first_image_mtime: d.first_image_mtime,
      current: p.current, stalledDays: p.stalledDays, doneCount: p.doneCount, totalCount: p.totalCount,
      image: imageSummaryOf(p, d),
      // 画像制作だけの保留 (2026-08-26)。画像ビューではバッジを出し、滞留の赤枠は付けない (止めているのは意図)
      imageOnHold: d.image_workflow_state === 'on_hold',
      imageHoldNote: d.image_hold_note || null,
      // 画像工程 v2 のカード情報 (2026-08-26): 撮影・素材ステータス / Canva / 商品情報の有無
      materialStatus: d.material_status || null,
      materialLabel: d.material_status ? (MATERIAL_STATUS_LABELS[d.material_status] || d.material_status) : null,
      canvaUrl: d.canva_url || null,
      hasProductInfo: d.has_product_info === 1,
      ownBrand: d.own_brand === 1,
      // ボードから楽天に出品した結果 (2026-09-01)。出品・展開の列でだけ使う
      rakutenRegisteredAt: d.rakuten_registered_at || null,
      rakutenLastError: d.rakuten_last_error || null,
      // 直近の試行: running=実行中 / failed=失敗 (やり直せる) / unknown=結果不明 (やり直し禁止) / null
      rakutenListingOutcome: d.rakuten_listing_outcome || null,
      rakutenListingAttemptAt: d.rakuten_listing_attempt_at || null,
      // 夜間自動化 (2026-08-28): AI が「人の確認待ち」にした理由。列は変えず (工程は AI情報入力待ちのまま)
      // カードに ⚠ で出す — on_hold にするとボードから消えて誰も気づかない
      genBlockCode: d.generation_block_code || null,
      genBlockReason: d.generation_block_reason || null,
      genBlockLabel: d.generation_block_code
        ? (GENERATION_BLOCK_CODES[d.generation_block_code] || d.generation_block_code) : null,
      // 確認中 (2026-08-31): 人が「情報待ち」で止めた印。genBlock と同じく列は変えずカードに出す。
      // days = 確認中にしてから何日経ったか (何日も動いていないカードを見つけるため)
      checking: d.checking_since ? {
        code: d.checking_reason_code,
        label: CHECKING_REASON_LABELS[d.checking_reason_code] || d.checking_reason_code || '確認中',
        note: d.checking_note || null,
        since: d.checking_since,
        days: daysSinceIso(d.checking_since),
      } : null,
    };

    if (view === 'image') {
      // カード = 商品 × 種別 (TOP/詳細 は別々に進むので別の列に出る)。
      // 絞り込みは**その種別の**現在工程を基準にする — 本流や他種別の一致で
      // 無関係な列にカードが出ると、担当者が自分の作業と誤読する (Codex設計相談)
      // 完了列も**種別ごと**に判定する (Codex R2 medium): TOP だけ先に終わった商品の
      // TOP カードはどの段階の列にも出ないので、完了列に出さないとボードから消えて
      // D&D で差し戻せない。「商品として完了」ではなく「この種別が決着」が完了列の意味
      for (const k of kinds) {
        if (k.excluded || k.s.rows.length === 0) continue;
        // 種別の絞り込み (2026-08-24 中原さん要望: TOP画像/商品詳細画像だけを見たい)。完了列にも効かせる
        if (imageKind && k.kind !== imageKind) continue;
        const cur = k.s.current;
        if (!cur) {
          // この種別は決着済み → 完了列 (絞り込み中は出さない — 「終わった分」は全員共通)。
          // TOP 全部 skip は「完了」に見せない (出品ゲートは拒否するので表示と食い違う — Codex R1 medium)
          if (assigneeId == null && !unassignedOnly
            && !(k.kind === 'top' && k.s.rows.every((r) => r.state === 'skip'))) {
            doneCards.push({ ...baseCard, kind: k.kind });
          }
          continue;
        }
        if (assigneeId != null && cur.assignee_id !== assigneeId) continue;
        if (unassignedOnly && !(cur.role_code && cur.assignee_id == null)) continue;
        const card = { ...baseCard, kind: k.kind, kindCurrent: cur, kindStalledDays: baseCard.imageOnHold ? null : k.s.stalledDays };
        colByStep.get(cur.step_code)?.cards.push(card);
        cardsById.set(d.id, card);
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

  // 並び順。既定は「停滞しているものを上」(打ち手が要るカードを埋もれさせない) だが、
  // 手で並べ替えたカードはその順を優先する (2026-08-28 中原さん要望)。
  // 手動順は「その列に置いたもの」だけ効かせる — 工程が変わって別の列に出たカードは
  // 既定順に戻す (別の列で付けた番号を持ち込むと、置いた覚えのない位置に割り込む)
  const manual = loadBoardOrder(db, view, drafts.map((d) => d.id));
  // 手動順の後片付け (Codex R2 中)。詳細画面から工程を進めた場合など、D&D を通らずに列が
  // 変わったカードは「前の列に置いた記録」が残る。表示には効かないが行が溜まり、あとで
  // その列を並べ替えたとき見えないカードとして差し込み位置を押し下げる。
  // **いま描画したカード**だけを対象に、記録の列と実際の列が食い違う行を消す
  // (絞り込みで描画していないカード・改ざんで入った他所の行も、次に出たときここで消える)
  {
    const actual = new Map();
    for (const c of columns) for (const card of c.cards) actual.set(`${card.id}|${card.kind || ''}`, c.code || c.key);
    for (const card of doneCards) actual.set(`${card.id}|${card.kind || ''}`, BOARD_DONE_COL);
    const stale = [...manual.entries()].filter(([k, m]) => actual.has(k) && actual.get(k) !== m.col).map(([k]) => k);
    if (stale.length > 0) {
      const del = db.prepare('DELETE FROM ph_board_order WHERE view = ? AND draft_id = ? AND kind = ?');
      db.transaction(() => {
        for (const k of stale) {
          const [id, kind] = k.split('|');
          del.run(view, Number(id), kind || '');
          manual.delete(k);
        }
      })();
    }
  }
  const manualIn = (colKey) => (c) => {
    const m = manual.get(`${c.id}|${c.kind || ''}`);
    return m && m.col === colKey ? m.sort : null;
  };
  // 既定順は 停滞日数 → id。確認中どうしは「長く待っている順」(忘れられているものほど上)
  const defaultOrder = (a, b) => {
    if (a.checking && b.checking) {
      const d = (b.checking.days || 0) - (a.checking.days || 0);
      if (d !== 0) return d;
    }
    return ((b.kindStalledDays ?? b.stalledDays) || 0) - ((a.kindStalledDays ?? a.stalledDays) || 0) || a.id - b.id;
  };
  const orderIn = (colKey, fallback) => {
    const mo = manualIn(colKey);
    return (a, b) => {
      // 確認中は**手動順より上**に置く (2026-08-31 スタッフ要望 / Codex R1)。
      // 「情報待ちのカードが埋もれる」が要望の本体なので、以前その列で手作業で決めた位置より
      // 優先する。手動順は確認中どうし・通常どうしの中では今まで通り効く
      const ca = a.checking ? 1 : 0;
      const cb = b.checking ? 1 : 0;
      if (ca !== cb) return cb - ca;
      const ma = mo(a); const mb = mo(b);
      if (ma != null && mb != null) return ma - mb;
      if (ma != null) return -1;      // 手で置いたカードは既定順のカードより上
      if (mb != null) return 1;
      return fallback(a, b);
    };
  };
  for (const c of columns) c.cards.sort(orderIn(c.code || c.key, defaultOrder));
  // 完了列は「直近から 30 件」。**先に 30 件を切ってから**手動順を当てる (Codex R2 高:
  // 先に並べ替えると、昔並べ替えた古い完了カードが新しい完了より上に居座り続けて
  // 「直近 30 件」でなくなる)。既定の並びは drafts の updated_at DESC のまま触らない —
  // Array#sort は安定 (ES2019) なので 0 を返せば元の順が保たれる
  const doneRecent = doneCards.slice(0, 30);
  doneRecent.sort(orderIn(BOARD_DONE_COL, () => 0));

  // 「確認中」チップの件数。**絞り込み・ビューと無関係の "商品" 件数**を出す (絞り込み中に
  // 0 と出ると確認中が無いように見え、拾うための入口がそこで消える)。
  // 画像ビューはカード = 商品×種別、かつ詳細画像を作らない商品はカードにならないので、
  // この数と画面上のカード枚数は一致しない (Codex R1 medium。チップの title に明記してある)
  const checkingTotal = db.prepare(`
    SELECT COUNT(*) AS c FROM product_drafts
    WHERE status NOT IN ('on_hold', 'excluded') AND checking_since IS NOT NULL
  `).get()?.c || 0;

  return {
    view,
    columns,
    // 完了は溜まる一方なので直近だけ (全部見たいときは一覧から)
    doneCards: doneRecent,
    doneTotal: doneCards.length,
    total: drafts.length,
    truncated,
    checkingTotal,
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
           ${/* listing_gate を落とすと kindSummaryOf の gateRows が「全工程」になり、gateDone が
                常に done と同じ意味になってしまう (ボードの「済」判定がこれを見る — 2026-09-01) */''}
           s.label, s.track, s.image_kind, s.image_stage, s.sort, s.stall_days, s.role_code, s.listing_gate,
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
      // 2026-08-31: TOP の工程は廃止したので、画像が終わったかは商品詳細 (LP) だけで決まる
    // (TOP画像そのものの有無は出品ゲート imageTrackBlockReason が画像の登録で見る)
    imageDone: detailExcluded || imageDetail.done,
      stalledDays: current ? stalledDaysOf(main, main.indexOf(current), createdAt) : null,
      doneCount: main.filter((r) => r.state === 'done' || r.state === 'skip').length,
      totalCount: main.length,
    });
  }
  return out;
}
