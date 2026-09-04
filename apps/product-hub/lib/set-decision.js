/**
 * 「セット展開判断」と NE 登録の進みの**語彙と表示名** (2026-09-04 要件定義 §4.2/§4.3)。
 *
 * ここに置く理由: 判断を記録する側 (services/set-derive.js) と、ボードのカードを組み立てる側
 * (lib/workflow-progress.js) の両方が使う。services 側に置くと循環参照になり、
 * 表示名を両方に書くとカードと詳細で言葉がズレる (今日の #1170 と同じ形の事故)。
 * 依存を持たない純粋なモジュールにしておく。
 */

/** ⑤を閉じられる判断 (hold は「保留」なので閉じない) */
export const SET_DECISIONS_CLOSING = ['create', 'existing', 'none'];
export const SET_DECISIONS = [...SET_DECISIONS_CLOSING, 'hold'];

/** 「作らない」の理由 (中原さん 2026-09-04: 自由入力ではなく選択式)。画面の表示名もここが正 */
export const SET_DECISION_REASONS = {
  shipping_loss: '送料負け (単価が低い)',
  low_demand: '需要が見込めない',
  supply_unstable: '仕入れ・在庫が安定しない',
  single_enough: '単品で十分',
  other: 'その他',
};

const DECISION_LABELS = {
  create: 'セットを作成',
  existing: '既存のセットあり',
  none: '作らない',
  hold: '保留',
};

/**
 * NE 登録の進みの表示名。カード・詳細・要対応ビューで同じ言葉を使う。
 * 'confirmed' は列に持たず provisional_code=0 から導出した**表示用**の値
 */
export const NE_STATE_LABELS = {
  not_requested: 'NE: 未要求',
  requested: 'NE: 要求済み・反映待ち',
  processing: 'NE: 処理中',
  confirmed: 'NE: 本コード確定',
  needs_action: 'NE: 要対応',
};

/**
 * 判断の一言表示。DB の行 (snake_case) と画面用オブジェクト (camelCase) の
 * どちらでも読めるようにする — 呼ぶ場所によってキーが違うと、片方だけ理由が消える
 * @param {{decision: string, reason_code?: string, reasonCode?: string,
 *   reason_text?: string, reasonText?: string}|null} row
 */
export function describeSetDecision(row) {
  if (!row || !row.decision) return '';
  const label = DECISION_LABELS[row.decision] || row.decision;
  const code = row.reason_code ?? row.reasonCode ?? null;
  const text = row.reason_text ?? row.reasonText ?? null;
  if (row.decision === 'none') {
    const why = code === 'other' ? (text || SET_DECISION_REASONS.other) : (SET_DECISION_REASONS[code] || '');
    return why ? `${label} (${why})` : label;
  }
  if (row.decision === 'hold' && text) return `${label} (${text})`;
  return label;
}
