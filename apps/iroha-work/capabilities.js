/**
 * いろは在庫化作業アプリ — 画面に許す操作 (capabilities)
 *
 * 画面は「サーバーが返した許可リストにある操作だけ」を描く (default-deny)。
 * 「描いてから CSS で隠す」をしない — 下見 (読むだけ) で詳細を開けるようにしたとき、
 * 開始ボタン・写真の追加・作業のやり方の変更が漏れて出ないように (要件 v1.3 §P Q5 / Codex R4)。
 *
 * 正本ごとの許可:
 *   notion  = Notion のカードを読み書きする一覧・詳細 (状態変更・作業開始・写真・作業のやり方)
 *   app     = f_iroha_tasks が正本 (上に加えて 今日やる・外部準備OK・取消の判断・確認ずみ・ラベル待ち・まとめて棚入完了)
 *   preview = 下見・履歴の詳細 (読むだけ。何も許さない)
 *
 * ⭐取消の判断 (CANCELLATION) と 状態の確認ずみ (REVIEW_CLEAR) は、いまは職員がPCの管理画面から行う操作で、
 *   iPad の画面 (views/index.html) に導線がない。導線を足すときは、他の操作と同じく
 *   can() / stateCan() で「許可リストにあるときだけ描く」こと (描いてから隠さない — Codex PR1 R6)。
 */
export const CAP = Object.freeze({
  STATUS_CHANGE: 'task.status.change',
  WORK_START: 'task.work.start',
  MEDIA_ADD: 'task.media.add',
  MASTER_EDIT: 'task.master.edit',
  PLAN_ASSIGN: 'task.plan.assign',
  EXTERNAL_READY: 'task.external_ready',
  CANCELLATION: 'task.cancellation',
  REVIEW_CLEAR: 'task.review.clear',
  LABEL_WAIT_EDIT: 'task.label_wait.edit',
  BULK_STOCKED: 'tasks.bulk_stocked',
  // ⭐計画は職員だけ (要件 §W-1)。「いつやるか」と「どこが作業するか」を決めるのは職員の仕事で、
  //   利用者はボードで見るだけ。許可が無ければ札もボタンにせず、明日の計画の入口も描かない
  FACILITY_ASSIGN: 'task.facility.assign',
});

const CAPS_NOTION = Object.freeze([CAP.STATUS_CHANGE, CAP.WORK_START, CAP.MEDIA_ADD, CAP.MASTER_EDIT]);
const CAPS_APP = Object.freeze([...CAPS_NOTION, CAP.EXTERNAL_READY, CAP.CANCELLATION, CAP.REVIEW_CLEAR,
  CAP.LABEL_WAIT_EDIT, CAP.BULK_STOCKED]);
/** ⭐職員のときだけ足す = 計画 (いつ / どこが)。利用者の画面には札のボタンも計画の入口も描かない */
const CAPS_STAFF = Object.freeze([CAP.PLAN_ASSIGN, CAP.FACILITY_ASSIGN]);
const CAPS_PREVIEW = Object.freeze([]);

/**
 * @param {'notion'|'app'|'preview'} mode
 * @param {{staff?: boolean}} who staff = 職員モード中の端末、またはポータルの職員
 * @returns {string[]} 新しい配列 (呼び出し側が壊しても共有定数は変わらない)
 */
export function capabilitiesFor(mode, { staff = false } = {}) {
  if (mode === 'app') return staff ? [...CAPS_APP, ...CAPS_STAFF] : [...CAPS_APP];
  if (mode === 'notion') return [...CAPS_NOTION];
  return [...CAPS_PREVIEW];   // 下見・履歴は誰でも読むだけ (職員でも何も許さない)
}
