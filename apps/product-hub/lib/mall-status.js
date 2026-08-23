/**
 * product-hub: 商品 × モールの展開状況 (2026-08-23)。
 *
 * 工程「出品・展開」の中身。中原さん指定で**モールごとに状態を持つ**。
 * 全モールが done / skip になると、工程「出品・展開」を自動で完了にする
 * (人が二度同じことを押さなくて済むように、モール側を正とする一方向の連動)。
 *
 * 権限と楽観ロックは工程 (workflow-progress.js) と同じ約束:
 *   - admin は全部 / 担当者本人は状態・URL・メモ / 未割り当ては引き受けのみ / 他人のものは触れない
 *   - version をトークンにした楽観ロック。画面から来る操作はトークン必須
 */
import { getDB, logEvent } from '../db.js';
import { STEP_STATES, STEP_STATE_LABELS, progressOf } from './workflow-progress.js';

/**
 * 展開先モール。
 * Amazon は FBA 側の別管理 (SKU は Pricetar 発行の pr_xxx) なので含めない (中原さん 2026-08-23)。
 * hint は運用の目安を画面に出すためのもので、実際の進み方は人が state で管理する。
 */
export const MALLS = [
  { code: 'rakuten', label: '楽天', sort: 10, hint: 'このアプリから出品できます (出品すると自動で完了になります)' },
  { code: 'yahoo', label: 'Yahoo!', sort: 20, hint: 'RakutenYahooSync が楽天から同期します' },
  { code: 'aupay', label: 'au PAY', sort: 30, hint: '' },
  { code: 'mercari', label: 'メルカリShops', sort: 40, hint: 'mercari-sync が楽天から同期します' },
  { code: 'qoo10', label: 'Qoo10', sort: 50, hint: '' },
  { code: 'linegift', label: 'LINEギフト', sort: 60, hint: 'linegift-sync が楽天から同期します' },
];

export const MALL_CODES = new Set(MALLS.map((m) => m.code));
const MALL_LABELS = new Map(MALLS.map((m) => [m.code, m.label]));

/** 工程「出品・展開」のコード。モールが全部終わったらこの工程を完了にする */
export const LISTING_STEP_CODE = 'listing';

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
function conflict(message) {
  const e = new Error(message);
  e.status = 409;
  return e;
}

const nowIso = () => new Date().toISOString();

/** 商品コードがまだ仮 (セット派生で NE 未登録) か */
function isProvisional(db, draftId) {
  return db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(Number(draftId))?.provisional_code === 1;
}

/**
 * モール行の自己修復生成。工程と同じく表示時に足りない分だけ作る。
 * 既定担当は工程「出品・展開」の担当者 (モール別に後から差し替えられる)。
 */
export function ensureMallStatus(db, draftId) {
  const id = Number(draftId);
  const existing = new Set(
    db.prepare('SELECT mall FROM draft_mall_status WHERE draft_id = ?').all(id).map((r) => r.mall)
  );
  const missing = MALLS.filter((m) => !existing.has(m.code));
  if (missing.length === 0) return 0;

  // 出品・展開工程の担当者を初期値にする (工程が無い/未割り当てなら null)
  const owner = db.prepare(`
    SELECT assignee_id FROM draft_step_progress WHERE draft_id = ? AND step_code = ?
  `).get(id, LISTING_STEP_CODE)?.assignee_id ?? null;

  // 楽天は既にアプリから出品済みかもしれない。その場合は done で作る
  // (登録済みの商品を「未着手」で並べて二重出品させない)。
  // 掲載URLは商品コードから決まるので保存せず、画面側で組み立てる
  const rk = db.prepare('SELECT registered_at FROM draft_rakuten WHERE draft_id = ?').get(id);

  const ins = db.prepare(`
    INSERT OR IGNORE INTO draft_mall_status (draft_id, mall, state, assignee_id, listed_at)
    VALUES (?, ?, ?, ?, ?)
  `);
  return db.transaction(() => {
    let n = 0;
    for (const m of missing) {
      const listed = m.code === 'rakuten' && rk?.registered_at ? rk.registered_at : null;
      n += ins.run(id, m.code, listed ? 'done' : 'todo', owner, listed).changes;
    }
    return n;
  })();
}

/** 1 商品のモール別状況 (画面がそのまま描ける形) */
export function mallStatusOf(draftId, { db = null } = {}) {
  const conn = db || getDB();
  const id = Number(draftId);
  ensureMallStatus(conn, id);
  const rows = conn.prepare(`
    SELECT s.*, st.name AS assignee_name, st.color AS assignee_color, st.active AS assignee_active
    FROM draft_mall_status s
    LEFT JOIN ph_staff st ON st.id = s.assignee_id
    WHERE s.draft_id = ?
  `).all(id);
  const byCode = new Map(rows.map((r) => [r.mall, r]));
  // MALLS の順で並べ、コード定義に無い行 (廃止したモール) は落とす
  const list = MALLS.map((m) => ({ ...m, ...(byCode.get(m.code) || { mall: m.code, state: 'todo' }) }));
  const done = list.filter((r) => r.state === 'done').length;
  const skipped = list.filter((r) => r.state === 'skip').length;
  return {
    list,
    doneCount: done,
    skipCount: skipped,
    totalCount: list.length,
    allSettled: done + skipped === list.length,
  };
}

/** 権限。工程と同じ約束 (admin / 本人 / 未割り当ては引き受けのみ) */
function assertMallPermission(db, row, patch, { isAdmin, actorStaffId }) {
  if (isAdmin) return;
  if (row.assignee_id != null && row.assignee_id === actorStaffId) {
    if (patch?.state === 'skip') throw forbidden('「対象外」にできるのは管理者だけです');
    if (patch?.assignee_id !== undefined) {
      const to = patch.assignee_id === '' || patch.assignee_id == null ? null : Number(patch.assignee_id);
      if (to !== null && to !== actorStaffId) throw forbidden('担当者を他の人に変更できるのは管理者だけです');
    }
    return;
  }
  if (row.assignee_id == null) {
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
  throw forbidden(`このモールは ${owner?.name || '他の人'} さんの担当です。管理者に依頼してください`);
}

/**
 * モールの状態・担当者・URL・メモを更新する。
 * 全モールが決着したら工程「出品・展開」も完了にする (同じトランザクション内)。
 */
export function setMallState(
  draftId, mall, patch, actor,
  { isAdmin = false, actorStaffId = null, requireVersion = false } = {},
) {
  const db = getDB();
  const id = Number(draftId);
  const code = String(mall || '');
  if (!MALL_CODES.has(code)) throw badRequest('モールの指定が不正です');
  ensureMallStatus(db, id);
  const row = db.prepare('SELECT * FROM draft_mall_status WHERE draft_id = ? AND mall = ?').get(id, code);
  if (!row) throw badRequest('この商品にそのモールがありません');
  assertMallPermission(db, row, patch, { isAdmin, actorStaffId });

  const sets = [];
  const params = { draft_id: id, mall: code };
  let event = null;
  let stateChanged = false;

  if (patch?.state !== undefined) {
    const state = String(patch.state);
    if (!STEP_STATES.includes(state)) throw badRequest('状態の指定が不正です');
    // 仮コード (セット派生で NE 未登録) のまま「掲載済」にさせない (Codex R1 high 2026-08-23)。
    // 楽天 API だけ止めても、手で done にすれば工程が完了してしまい、
    // Yahoo など下流の展開 (人手 or sync) を誘発する
    if (state === 'done' && isProvisional(db, id)) {
      throw badRequest('商品コードが仮のままです。ネクストエンジンの本コードに差し替えてから掲載済にしてください');
    }
    if (state !== row.state) {
      stateChanged = true;
      sets.push('state = @state');
      params.state = state;
      if (state === 'done') {
        sets.push('listed_at = COALESCE(listed_at, @listed_at)');
        params.listed_at = nowIso();
      }
      event = `${MALL_LABELS.get(code)}: ${STEP_STATE_LABELS[row.state]} → ${STEP_STATE_LABELS[state]}`;
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
      if (st.active !== 1) throw badRequest(`${st.name} は無効化されています`);
    }
    if (staffId !== row.assignee_id) {
      sets.push('assignee_id = @assignee_id');
      params.assignee_id = staffId;
      const name = staffId ? db.prepare('SELECT name FROM ph_staff WHERE id = ?').get(staffId).name : '未割り当て';
      event = `${event ? `${event} / ` : `${MALL_LABELS.get(code)}: `}担当 → ${name}`;
    }
  }

  if (patch?.item_url !== undefined) {
    const url = String(patch.item_url == null ? '' : patch.item_url).trim();
    // 掲載URLは確認用のリンクとして画面に出す。javascript: 等を踏ませないよう http(s) だけ通す
    if (url !== '' && !/^https?:\/\//i.test(url)) throw badRequest('掲載URLは http(s):// で始まる必要があります');
    sets.push('item_url = @item_url');
    params.item_url = url === '' ? null : url.slice(0, 500);
  }

  if (patch?.note !== undefined) {
    const note = String(patch.note == null ? '' : patch.note).trim();
    sets.push('note = @note');
    params.note = note === '' ? null : note.slice(0, 300);
  }

  if (sets.length === 0) return { changed: false };

  const expected = patch?.expected_version == null ? null : Number(patch.expected_version);
  if (requireVersion && !Number.isInteger(expected)) {
    throw conflict('画面が古い可能性があります。読み直してから操作してください');
  }
  const conds = ['draft_id = @draft_id', 'mall = @mall'];
  if (Number.isInteger(expected)) {
    if (expected !== row.version) throw conflict('別の人がこのモールを先に更新しました。画面を読み直してください');
    conds.push('version = @expected_version');
    params.expected_version = expected;
  } else if (stateChanged) {
    conds.push('state = @prev_state');
    params.prev_state = row.state;
  }

  const run = db.transaction(() => {
    const info = db.prepare(`
      UPDATE draft_mall_status
      SET ${sets.join(', ')}, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE ${conds.join(' AND ')}
    `).run(params);
    if (info.changes !== 1) return null;
    if (event) logEvent(db, id, 'mall_changed', event, actor);
    const settled = syncListingStep(db, id, actor);
    const fresh = db.prepare(
      'SELECT version FROM draft_mall_status WHERE draft_id = ? AND mall = ?'
    ).get(id, code);
    return { version: fresh?.version ?? null, settled };
  });
  const result = run();
  if (!result) throw conflict('別の人がこのモールを先に更新しました。画面を読み直してください');
  return { changed: true, version: result.version, listingCompleted: result.settled };
}

/**
 * 全モールが決着したら工程「出品・展開」を完了にする (モール → 工程の一方向)。
 * 逆方向 (工程を完了にしたらモールも全部完了) はやらない。
 * 「まだ出していないモールがあるのに完了」を人が押せてしまうと、展開漏れが見えなくなる。
 * @returns {boolean} この呼び出しで工程を完了にしたか
 */
function syncListingStep(db, draftId, actor) {
  // 仮コードのまま工程を閉じない (全部 skip にすれば決着してしまうため、ここでも見る)
  if (isProvisional(db, draftId)) return false;
  const rows = db.prepare('SELECT mall, state FROM draft_mall_status WHERE draft_id = ?').all(draftId);
  const byCode = new Map(rows.map((r) => [r.mall, r.state]));
  const allSettled = MALLS.every((m) => {
    const s = byCode.get(m.code);
    return s === 'done' || s === 'skip';
  });
  if (!allSettled) {
    // 決着が崩れたら工程も開き直す (Codex R2 medium)。
    // モール側を正とする設計なので、「工程は完了なのに未展開のモールがある」を残さない
    const reopen = db.prepare(`
      UPDATE draft_step_progress
      SET state = 'todo', done_at = NULL, done_by = NULL,
          version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE draft_id = ? AND step_code = ? AND state = 'done'
    `).run(draftId, LISTING_STEP_CODE);
    if (reopen.changes === 1) {
      logEvent(db, draftId, 'step_changed', '出品・展開: 未展開のモールが出たので完了を取り消しました', actor);
    }
    return false;
  }
  const info = db.prepare(`
    UPDATE draft_step_progress
    SET state = 'done', done_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), done_by = ?,
        version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE draft_id = ? AND step_code = ? AND state != 'done'
  `).run(actor || 'system', draftId, LISTING_STEP_CODE);
  if (info.changes === 1) {
    logEvent(db, draftId, 'step_changed', '出品・展開: 全モールの展開が終わったので完了にしました', actor);
    return true;
  }
  return false;
}

/**
 * 楽天出品が成功したときに呼ぶ。楽天モールを完了にし、全モール決着なら工程も閉じる。
 * fail-soft: ここで失敗しても出品自体は成功しているので、例外は投げず false を返す。
 */
export function markRakutenListed(db, draftId, { itemUrl = null, actor = null } = {}) {
  try {
    ensureMallStatus(db, draftId);
    db.transaction(() => {
      db.prepare(`
        UPDATE draft_mall_status
        SET state = 'done', listed_at = COALESCE(listed_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            item_url = COALESCE(@item_url, item_url),
            version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE draft_id = @draft_id AND mall = 'rakuten' AND state != 'done'
      `).run({ draft_id: Number(draftId), item_url: itemUrl });
      syncListingStep(db, Number(draftId), actor);
    })();
    return true;
  } catch (e) {
    console.warn('[product-hub] 楽天モール状態の更新に失敗:', e.message);
    return false;
  }
}

/** 一覧・かんばん用にまとめて引く (行ごとに引くと N+1) */
export function mallSummaryFor(db, draftIds) {
  const ids = (Array.isArray(draftIds) ? draftIds : []).map(Number).filter(Number.isInteger);
  const out = new Map();
  if (ids.length === 0) return out;
  const placeholders = ids.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT draft_id, mall, state FROM draft_mall_status WHERE draft_id IN (${placeholders})
  `).all(...ids);
  const byDraft = new Map();
  for (const r of rows) {
    if (!byDraft.has(r.draft_id)) byDraft.set(r.draft_id, new Map());
    byDraft.get(r.draft_id).set(r.mall, r.state);
  }
  for (const id of ids) {
    const m = byDraft.get(id) || new Map();
    out.set(id, MALLS.map((mall) => ({
      code: mall.code, label: mall.label, state: m.get(mall.code) || 'todo',
    })));
  }
  return out;
}

/**
 * 出品ゲート: まだ出品してよい状態か (PR4 で追加予定のセット派生の仮コード対策と同じ入口)。
 * ここでは「工程の画像トラックが終わっているか」を見る。
 * 画像が揃っていない商品を楽天に出すと、後から差し替える手間の方が大きい。
 */
export function listingBlockReason(draftId, { db = null } = {}) {
  const conn = db || getDB();
  const p = progressOf(draftId, { db: conn });
  if (p.image.length > 0 && !p.imageDone) {
    const cur = p.imageCurrent;
    return `画像トラックが終わっていません (${cur ? cur.label : '未着手'})`;
  }
  return null;
}
