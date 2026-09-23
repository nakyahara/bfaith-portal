/**
 * SP広告KW — 候補の正規化と、採用した KW / 商品ターゲットの「コピーできる本文」(PR1・2026-09-23、PR2-C で商品ターゲットを追加)
 *
 * 純粋なロジックだけ (DB・HTTP に触らない)。設計 = 『Amazon_SP広告KW自動生成_設計方針_20260922.md』§4.3 / §4.5。
 *
 * 🚨 コピーは**種類ごと・マッチタイプごとに分けて**出す (Codex R1 #12)。1 つの一覧にまとめると
 *   検索KW と 除外KW と 商品ターゲット、完全一致とフレーズ一致 の区別が失われ、広告画面に貼るときに取り違える。
 *   商品ターゲット (ASIN) はキーワードのブロックに混ぜない (別キャンペーン・別の入力欄)。
 * 🚨 コピーできても「Amazon に登録済み」ではない。この部品はそれを表す文言を持たない (呼び手も持たない)。
 */

import { ASIN_RE } from '../../../lib/asin.js';

export const MATCH_TYPES = ['exact', 'phrase', 'broad'];
export const MATCH_TYPE_JA = { exact: '完全一致', phrase: 'フレーズ一致', broad: '部分一致' };
/** コピー本文のブロック名。キーワードはマッチタイプ、商品ターゲットは 'product_targets' */
export const COPY_BLOCKS = [...MATCH_TYPES, 'product_targets'];
export const COPY_BLOCK_JA = { ...MATCH_TYPE_JA, product_targets: '商品ターゲット (ASIN)' };

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
 * @param {Array<{keyword:string, match_type:string, kind?:string}>} adopted 採用済み (人が確定した語とマッチタイプ)。kind が 'asin' のものは入れない
 * @returns {{blocks: Array<{match_type, label, count, text}>, total:number}}
 *   text = 1 行 1 語 (Amazon の広告画面の「キーワードを入力」に貼る形)。空のマッチタイプは block を出さない
 */
export function buildKeywordCopy(adopted) {
  const by = new Map(MATCH_TYPES.map((m) => [m, []]));
  for (const a of adopted || []) {
    if (a.kind && a.kind !== 'kw') continue;
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
 * 採用した商品ターゲット (ASIN) のコピー本文。1 行 1 ASIN。キーワードとは別ブロック (PR2-C)
 * @param {Array<{keyword:string, kind?:string}>} adopted kind='asin' の採用 (keyword に ASIN)
 * @returns {{match_type:'product_targets', label, count, text}|null}
 */
export function buildProductTargetCopy(adopted) {
  const asins = [];
  for (const a of adopted || []) {
    if (a.kind !== 'asin') continue;
    const asin = String(a.keyword || '').trim().toUpperCase();
    if (!ASIN_RE.test(asin) || asins.includes(asin)) continue;
    asins.push(asin);
  }
  return asins.length ? { match_type: 'product_targets', label: COPY_BLOCK_JA.product_targets, count: asins.length, text: asins.join('\n') } : null;
}

/**
 * コピー本文の固定版 (ph_ad_kw_exports に残す中身)。同じ採否からは同じ文字列になる (照合用)。
 * blocks = キーワードのマッチタイプ別 + (あれば) 商品ターゲット。total = 語 + ASIN の合計
 */
export function exportSnapshot(adopted) {
  const copy = buildKeywordCopy(adopted);
  const targets = buildProductTargetCopy(adopted);
  const blocks = copy.blocks.map((b) => ({ match_type: b.match_type, count: b.count, text: b.text }));
  if (targets) blocks.push({ match_type: targets.match_type, count: targets.count, text: targets.text });
  return {
    kind: 'ad_copy',
    total: copy.total + (targets ? targets.count : 0),
    keyword_total: copy.total,
    target_total: targets ? targets.count : 0,
    blocks,
  };
}
