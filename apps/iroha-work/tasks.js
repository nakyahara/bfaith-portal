/**
 * いろは在庫化作業アプリ — タスクの状態モデル (要件定義 v1.1 §A、2026-09-03 確定)
 *
 * Notion の 12〜13 個の「ステータス」を 4 つの軸に分解する:
 *   状態 status × 終了理由 close_reason × 拠点 facility × 保留理由 hold_reason
 * このファイルは DB に触らない純粋な定義・写像・遷移ルール (テストしやすく、画面とサーバーで同じ規則を使う)。
 *
 * 中原さんの回答 (9/3):
 *   「次回」= 今日はやらない (未着手の後日) / 施設名 = 外部に預けた = 作業中 / 「作業完了」= いろはで作業済み・
 *   本社棚入前 (棚入待ち) / 棚入完了はいろは職員が操作 / 完了カードも DB に残す (一覧には出さない)
 */

/**
 * ⭐進捗は 4 つ (中原さん 2026-09-05 決定 = 案A)。「保留」は進捗ではなく **止まっている理由の札** (blocked_reason)。
 *   80 個まで作って止まっているカードは「作業中 (80/180) で、ラベル待ち」— 進捗が保留になったわけではない。
 *   進捗と理由を同じ列に混ぜない (要件 §V-1 と同じ教訓)。旧値 'on_hold' は起動時の移行で
 *   「作業中 or 未着手 + blocked_reason」に写す (db.js migrateOnHoldToBlocked)
 */
export const TASK_STATUSES = ['not_started', 'in_progress', 'ready_for_stocking', 'closed'];
/** 2026-09-05 まで進捗として使っていた旧値。DB の CHECK には残してある (移行が終わるまで)。新しく書かない */
export const LEGACY_ON_HOLD = 'on_hold';
export const STATUS_LABEL = {
  not_started: '未着手', in_progress: '作業中', ready_for_stocking: '棚入待ち', closed: '終了',
};
/** 一覧・カンバンに出す状態 (終了は履歴画面) */
export const OPEN_STATUSES = ['not_started', 'in_progress', 'ready_for_stocking'];

export const CLOSE_REASONS = ['stocked', 'cancelled', 'out_of_scope'];
export const CLOSE_LABEL = { stocked: '棚入完了', cancelled: '取消', out_of_scope: '在庫化対象外' };

/** 止まっている理由 (よく使う順)。「その他」はメモ必須 */
export const BLOCK_REASONS = ['label_shortage', 'materials_shortage', 'awaiting_instruction', 'other'];
export const BLOCK_LABEL = {
  label_shortage: 'ラベル待ち', materials_shortage: '資材不足', awaiting_instruction: '指示待ち', other: 'その他',
};
/** 画面のボタンに出す言い方 (利用者が押すので、状況の言葉で) */
export const BLOCK_BUTTON = {
  label_shortage: 'ラベルが足りない', materials_shortage: '資材が足りない', awaiting_instruction: '指示待ち', other: 'その他',
};
/** 進捗が止まれる状態。棚入待ち・終了は「もう作業しない」ので理由を持たない */
export const BLOCKABLE_STATUSES = ['not_started', 'in_progress'];
/** 旧名 (保留の理由)。中身は同じ — 移行コードとテストが読む */
export const HOLD_REASONS = BLOCK_REASONS;
export const HOLD_LABEL = BLOCK_LABEL;

/** 拠点 (f_iroha_facilities の初期値)。code は DB・API の値、name は Notion の施設名ステータスと一致させる */
export const FACILITIES = [
  { code: 'iroha', name: 'いろは', external: 0, sort_order: 0 },
  { code: 'rashinban', name: '羅針盤', external: 1, sort_order: 1 },
  { code: 'workcenter', name: 'ワークセンター', external: 1, sort_order: 2 },
  { code: 'jobsupport', name: 'ジョブサポ', external: 1, sort_order: 3 },
  { code: 'rehas', name: 'リハス', external: 1, sort_order: 4 },
];
export const DEFAULT_FACILITY = 'iroha';

/**
 * 許可遷移 (from → to)。自由な select にしない (要件 v1.1 §A・Codex R3)。
 * closed からの再開は職員 + 理由必須。
 */
export const TRANSITIONS = {
  not_started: ['in_progress', 'closed'],
  in_progress: ['ready_for_stocking', 'closed'],
  ready_for_stocking: ['closed', 'in_progress'],
  closed: ['in_progress'],
};
export function canTransition(from, to) {
  return Array.isArray(TRANSITIONS[from]) && TRANSITIONS[from].includes(to);
}
/**
 * 職員限定の遷移: 終了 (棚入完了・取消・対象外) / 終了からの再開 / 棚入待ちからのやり直し
 */
export function transitionNeedsStaff(from, to) {
  if (to === 'closed' || from === 'closed') return true;
  if (from === 'ready_for_stocking' && to === 'in_progress') return true;
  return false;
}
/** 遷移に付随して必須になる入力 */
export function transitionRequires(to) {
  if (to === 'closed') return { close_reason: true };
  return {};
}

/**
 * Notion ステータス → アプリの状態 (初期取込・差分取込の写像。要件 v1.1 §F)。
 * @returns {{status, close_reason?, hold_reason?, facility?, confidence:'exact'|'inferred'|'needs_review'|'rejected', note?}}
 *   confidence: exact = そのまま / inferred = 推定だが確からしい / needs_review = 職員確認が要る / rejected = 取り込めない
 */
export function mapLegacyStatus(legacy) {
  const s = String(legacy == null ? '' : legacy).trim();
  const fac = FACILITIES.find(f => f.name === s);
  if (fac) {
    // 施設名 = そこに預けている = 作業中。「いろは」だけは意味が未確認 (自社で作業中? 外部から戻った?) → 要確認
    return fac.code === 'iroha'
      ? { status: 'in_progress', facility: 'iroha', confidence: 'needs_review', note: '「いろは」ステータスの意味が未確認 (作業中と仮置き)' }
      : { status: 'in_progress', facility: fac.code, confidence: 'inferred', note: `${fac.name}に預けている = 作業中` };
  }
  switch (s) {
    case '': return { status: 'not_started', confidence: 'inferred', note: 'ステータス未設定 → 未着手' };
    case '未着手': return { status: 'not_started', confidence: 'exact' };
    case '作業中': return { status: 'in_progress', confidence: 'exact' };
    // 「中断」は進捗ではなく止まっている理由 (案A)。作業を始めてから止まったと読む
    case '資材不足で作業中断': return { status: 'in_progress', block_reason: 'materials_shortage', confidence: 'inferred', note: '「資材不足で作業中断」= 作業中 + 止まっている理由 (資材不足)' };
    case '次回': return { status: 'not_started', confidence: 'exact', note: '「次回」= 今日はやらない → 未着手 (後日)' };
    case '作業完了': return { status: 'ready_for_stocking', confidence: 'exact' };
    case '棚入完了': return { status: 'closed', close_reason: 'stocked', confidence: 'exact' };
    case '在庫化対象外': return { status: 'closed', close_reason: 'out_of_scope', confidence: 'exact' };
    case '取消': return { status: 'closed', close_reason: 'cancelled', confidence: 'exact' };
    default: return { status: null, confidence: 'rejected', note: `未知のステータス「${s}」` };
  }
}

/** 画面用: 状態と理由から表示ラベル (「終了 · 棚入完了」)。止まっている理由は別の札 (blockLabel) */
export function statusLabel(task) {
  const base = STATUS_LABEL[task.status] || (task.status === LEGACY_ON_HOLD ? '保留 (旧)' : task.status) || '—';
  if (task.status === 'closed' && task.close_reason) return `${base} · ${CLOSE_LABEL[task.close_reason] || task.close_reason}`;
  return base;
}

/** 止まっている理由の札の文言 (「ラベル待ちで止まっています」)。止まっていなければ null */
export function blockLabel(task) {
  if (!task || !task.blocked_reason) return null;
  return `${BLOCK_LABEL[task.blocked_reason] || task.blocked_reason}で止まっています`;
}

/**
 * 不変条件 (サービス層)。DB の CHECK と同じ規則:
 *   closed なら close_reason / closed_at が必須・それ以外は持たない
 *   止まっている理由 (blocked_reason) は 未着手・作業中 だけが持てる。「その他」はメモ必須
 *   旧列 hold_reason_code は使わない (移行で blocked_reason へ写した)
 */
export function validateTaskInvariants(task) {
  const problems = [];
  if (!TASK_STATUSES.includes(task.status)) problems.push(`status が不正: ${task.status}`);
  if (task.status === 'closed') {
    if (!CLOSE_REASONS.includes(task.close_reason)) problems.push('closed には close_reason が必要');
    if (!task.closed_at) problems.push('closed には closed_at が必要');
  } else if (task.close_reason || task.closed_at) {
    problems.push('closed 以外は close_reason / closed_at を持たない');
  }
  if (task.hold_reason_code) problems.push('hold_reason_code は使わない (止まっている理由は blocked_reason)');
  if (task.blocked_reason != null) {
    if (!BLOCK_REASONS.includes(task.blocked_reason)) problems.push(`blocked_reason が不正: ${task.blocked_reason}`);
    if (!BLOCKABLE_STATUSES.includes(task.status)) problems.push('棚入待ち・終了のカードは止まっている理由を持たない');
    if (task.blocked_reason === 'other' && !String(task.blocked_note || '').trim()) problems.push('止まっている理由「その他」にはメモが必要');
    if (!task.blocked_at) problems.push('止まっている理由には blocked_at (いつ止めたか) が必要');
  } else if (task.blocked_note != null || task.blocked_at != null || task.blocked_by != null) {
    // 4 列は一組。理由が無いのにメモ・時刻・人だけ残さない (DB の CHECK と同じ規則)
    problems.push('止まっていないのに blocked_note / blocked_at / blocked_by が残っている');
  }
  // ⭐できた数 (要件 §Y)。NULL = まだ数えていない。0 と区別するので「無い」を 0 に丸めない
  if (task.done_qty != null && (!Number.isInteger(task.done_qty) || task.done_qty < 0)) problems.push('できた数は 0 以上の整数');
  return problems;
}
