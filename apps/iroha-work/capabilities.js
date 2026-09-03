/**
 * いろは在庫化作業アプリ — 画面に許す操作 (capabilities)
 *
 * 画面は「サーバーが返した許可リストにある操作だけ」を描く (default-deny)。
 * 「描いてから CSS で隠す」をしない — 下見 (読むだけ) で詳細を開けるようにしたとき、
 * 開始ボタン・写真の追加・作業のやり方の変更が漏れて出ないように (要件 v1.3 §P Q5 / Codex R4)。
 *
 * 正本ごとの許可:
 *   notion  = Notion のカードを読み書きする一覧・詳細 (状態変更・作業開始・写真・作業のやり方)
 *   app     = f_iroha_tasks が正本 (上に加えて 今日やる・外部準備OK・取消の判断・ラベル待ち・まとめて棚入完了)
 *   preview = 下見・履歴の詳細 (読むだけ。何も許さない)
 */
export const CAP = Object.freeze({
  STATUS_CHANGE: 'task.status.change',
  WORK_START: 'task.work.start',
  MEDIA_ADD: 'task.media.add',
  MASTER_EDIT: 'task.master.edit',
  PLAN_ASSIGN: 'task.plan.assign',
  EXTERNAL_READY: 'task.external_ready',
  CANCELLATION: 'task.cancellation',
  LABEL_WAIT_EDIT: 'task.label_wait.edit',
  BULK_STOCKED: 'tasks.bulk_stocked',
});

const CAPS_NOTION = Object.freeze([CAP.STATUS_CHANGE, CAP.WORK_START, CAP.MEDIA_ADD, CAP.MASTER_EDIT]);
const CAPS_APP = Object.freeze([...CAPS_NOTION, CAP.PLAN_ASSIGN, CAP.EXTERNAL_READY, CAP.CANCELLATION, CAP.LABEL_WAIT_EDIT, CAP.BULK_STOCKED]);
const CAPS_PREVIEW = Object.freeze([]);

/**
 * @param {'notion'|'app'|'preview'} mode
 * @returns {string[]} 新しい配列 (呼び出し側が壊しても共有定数は変わらない)
 */
export function capabilitiesFor(mode) {
  if (mode === 'app') return [...CAPS_APP];
  if (mode === 'notion') return [...CAPS_NOTION];
  return [...CAPS_PREVIEW];
}
