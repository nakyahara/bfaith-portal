/**
 * SP広告KW — 候補の正規化と、採用した KW の「コピーできる本文」(PR1・2026-09-23)
 *
 * 純粋なロジックだけ (DB・HTTP に触らない)。設計 = 『Amazon_SP広告KW自動生成_設計方針_20260922.md』§4.3 / §4.5。
 *
 * 🚨 コピーは**種類ごと・マッチタイプごとに分けて**出す (Codex R1 #12)。1 つの一覧にまとめると
 *   検索KW と 除外KW、完全一致とフレーズ一致 の区別が失われ、広告画面に貼るときに取り違える。
 * 🚨 コピーできても「Amazon に登録済み」ではない。この部品はそれを表す文言を持たない (呼び手も持たない)。
 */

export const MATCH_TYPES = ['exact', 'phrase', 'broad'];
export const MATCH_TYPE_JA = { exact: '完全一致', phrase: 'フレーズ一致', broad: '部分一致' };

/**
 * 候補 KW の正規化。空白を 1 つに寄せ、前後を落とし、小文字/大文字は保つ (Amazon は区別しないが、人が読む)。
 * 種そのもの・空・長すぎるもの (Amazon の KW は 80 文字が上限) は null
 */
export function normalizeKeyword(raw, { seed = null, maxLen = 80 } = {}) {
  const s = String(raw ?? '').replace(/[\x00-\x1f\x7f]/g, ' ').replace(/[\s　]+/g, ' ').trim();
  if (!s || s.length > maxLen) return null;
  if (seed && s.toLowerCase() === String(seed).toLowerCase().trim()) return null;
  return s;
}

/** 同じ語 (大小文字・空白の違い) は 1 つにまとめる。先に出た方の出典を残す */
export function dedupeKeywords(items) {
  const seen = new Map();
  for (const it of items || []) {
    const key = String(it.keyword || '').toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.set(key, it);
  }
  return [...seen.values()];
}

/**
 * 採用した検索 KW を、マッチタイプ別のコピー本文にする。
 * @param {Array<{keyword:string, match_type:string}>} adopted 採用済み (人が確定した語とマッチタイプ)
 * @returns {{blocks: Array<{match_type, label, count, text}>, total:number}}
 *   text = 1 行 1 語 (Amazon の広告画面の「キーワードを入力」に貼る形)。空のマッチタイプは block を出さない
 */
export function buildKeywordCopy(adopted) {
  const by = new Map(MATCH_TYPES.map((m) => [m, []]));
  for (const a of adopted || []) {
    const mt = MATCH_TYPES.includes(a.match_type) ? a.match_type : null;
    const kw = normalizeKeyword(a.keyword);
    if (!mt || !kw) continue;   // 型の無い採用は出さない (黙って exact に寄せない)
    if (!by.get(mt).some((x) => x.toLowerCase() === kw.toLowerCase())) by.get(mt).push(kw);
  }
  const blocks = [];
  for (const mt of MATCH_TYPES) {
    const list = by.get(mt);
    if (list.length === 0) continue;
    blocks.push({ match_type: mt, label: MATCH_TYPE_JA[mt], count: list.length, text: list.join('\n') });
  }
  return { blocks, total: blocks.reduce((a, b) => a + b.count, 0) };
}

/**
 * コピー本文の固定版 (ph_ad_kw_exports に残す中身)。同じ採否からは同じ文字列になる (照合用)
 */
export function exportSnapshot(adopted) {
  const copy = buildKeywordCopy(adopted);
  return {
    kind: 'search_keywords',
    total: copy.total,
    blocks: copy.blocks.map((b) => ({ match_type: b.match_type, count: b.count, text: b.text })),
  };
}
