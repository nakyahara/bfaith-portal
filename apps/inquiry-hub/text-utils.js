/**
 * inquiry-hub 本文テキストの整形ユーティリティ
 *
 * 一覧のプレビュー (router.js previewOf) と 締め前確認の検知 (cutoff.js) で共有する。
 * ⭐検知で引用を落とすのが重要: 過去のやり取りが引用でぶら下がっていると、
 *   店舗が前に送った「お届け先は〜」がキャンセル依頼として引っかかる (偽陽性の主因)。
 */

/** 引用ヘッダ (この行より後ろは過去のやり取り) */
const QUOTE_HEADER = /\n(?:\d{4}年\d{1,2}月\d{1,2}日.*?:|On .{0,60}wrote:|.{0,40}さんは書きました)/;
/** ただの区切り線 (装飾。これ自体は引用の合図とは限らない) */
const SEPARATOR = /\n(?:[-–—_]{3,}|={3,})/;

/**
 * 引用を落として「今回の発言」だけを返す。
 *
 * @param {string} text
 * @param {object} opts
 *   keepAfterSeparator: 区切り線 (---- など) より後ろを残すか。
 *     ⚠️ **検知では true にする**。フォームメールやモール通知は区切り線の下に本文が入ることがあり、
 *        ここで切ると「注文番号:123 ---- 住所を変更してください」の依頼を丸ごと落とす
 *        (Codexレビュー指摘)。一覧プレビューは見た目のため false (署名まで出すと読みにくい)
 * @returns {string} 改行は残す
 */
export function stripQuoted(text, { keepAfterSeparator = false } = {}) {
  let s = String(text || '').replace(/\r\n?/g, '\n');
  s = s.split(QUOTE_HEADER)[0];
  if (!keepAfterSeparator) s = s.split(SEPARATOR)[0];
  return s.split('\n')
    .filter(l => !/^\s*[>|｜]/.test(l))          // 引用行
    .join('\n');
}

/**
 * 一覧の本文プレビュー用に1行へ畳む。URLは仕分けの役に立たないので落とす。
 * @param {string} text
 * @param {number} max 文字数上限
 */
export function toPreviewLine(text, max = 110) {
  const body = stripQuoted(text)
    .split('\n')
    .join(' ')
    .replace(/https?:\/\/\S+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  return body.length > max ? body.slice(0, max) + '…' : body;
}

/**
 * 検知用の正規化: 全角/半角・大小文字のゆれを吸収し、空白を詰める。
 * 「お 届 け 先」のような区切りや、全角英数での表記ゆれで取りこぼさないため。
 * ⚠️ 空白を全部落とすので、位置に依存する判定には使わない
 */
export function normalizeForMatch(text) {
  return String(text || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[\s　]+/g, '');
}
