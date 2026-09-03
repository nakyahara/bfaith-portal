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

export const TASK_STATUSES = ['not_started', 'in_progress', 'on_hold', 'ready_for_stocking', 'closed'];
export const STATUS_LABEL = {
  not_started: '未着手', in_progress: '作業中', on_hold: '保留', ready_for_stocking: '棚入待ち', closed: '終了',
};
/** 一覧・カンバンに出す状態 (終了は履歴画面) */
export const OPEN_STATUSES = ['not_started', 'in_progress', 'on_hold', 'ready_for_stocking'];

export const CLOSE_REASONS = ['stocked', 'cancelled', 'out_of_scope'];
export const CLOSE_LABEL = { stocked: '棚入完了', cancelled: '取消', out_of_scope: '在庫化対象外' };

export const HOLD_REASONS = ['materials_shortage', 'label_shortage', 'awaiting_instruction', 'other'];
export const HOLD_LABEL = {
  materials_shortage: '資材不足', label_shortage: 'ラベル待ち', awaiting_instruction: '指示待ち', other: 'その他',
};

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
  not_started: ['in_progress', 'on_hold', 'closed'],
  in_progress: ['on_hold', 'ready_for_stocking', 'closed'],
  on_hold: ['not_started', 'in_progress', 'closed'],
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
  if (to === 'on_hold') return { hold_reason: true };
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
    case '資材不足で作業中断': return { status: 'on_hold', hold_reason: 'materials_shortage', confidence: 'exact' };
    case '次回': return { status: 'not_started', confidence: 'exact', note: '「次回」= 今日はやらない → 未着手 (後日)' };
    case '作業完了': return { status: 'ready_for_stocking', confidence: 'exact' };
    case '棚入完了': return { status: 'closed', close_reason: 'stocked', confidence: 'exact' };
    case '在庫化対象外': return { status: 'closed', close_reason: 'out_of_scope', confidence: 'exact' };
    case '取消': return { status: 'closed', close_reason: 'cancelled', confidence: 'exact' };
    default: return { status: null, confidence: 'rejected', note: `未知のステータス「${s}」` };
  }
}

/** 画面用: 状態と理由から表示ラベル (「保留 · 資材不足」「終了 · 棚入完了」) */
export function statusLabel(task) {
  const base = STATUS_LABEL[task.status] || task.status || '—';
  if (task.status === 'on_hold' && task.hold_reason_code) return `${base} · ${HOLD_LABEL[task.hold_reason_code] || task.hold_reason_code}`;
  if (task.status === 'closed' && task.close_reason) return `${base} · ${CLOSE_LABEL[task.close_reason] || task.close_reason}`;
  return base;
}

/** 状態が「終了」なら終了理由・時刻が必須、そうでなければ持たない (サービス層の不変条件) */
export function validateTaskInvariants(task) {
  const problems = [];
  if (!TASK_STATUSES.includes(task.status)) problems.push(`status が不正: ${task.status}`);
  if (task.status === 'closed') {
    if (!CLOSE_REASONS.includes(task.close_reason)) problems.push('closed には close_reason が必要');
    if (!task.closed_at) problems.push('closed には closed_at が必要');
  } else if (task.close_reason || task.closed_at) {
    problems.push('closed 以外は close_reason / closed_at を持たない');
  }
  if (task.status === 'on_hold' && !HOLD_REASONS.includes(task.hold_reason_code)) problems.push('on_hold には hold_reason_code が必要');
  if (task.status !== 'on_hold' && task.hold_reason_code) problems.push('on_hold 以外は hold_reason_code を持たない');
  if (task.hold_reason_code === 'other' && !String(task.hold_reason_note || '').trim()) problems.push('保留理由「その他」には備考が必要');
  return problems;
}
