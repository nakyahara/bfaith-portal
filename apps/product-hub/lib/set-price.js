/**
 * セットの売価の初期値 (2026-09-04 要件定義『セット商品工程』§4.4 / 決⑦)。
 *
 * 「単品売価 × 個数」の和を初期値として入れ、人はそこから値付けする。今までは空で、
 * 毎回ゼロから打ち直していた。**確定値ではなく叩き台**なので、人が変える前提。
 *
 * 単価の引き先は 3 つ。順番に意味がある:
 *   ①  アプリのドラフトの売価      … 人が売り場向けに決めた値。いちばん近い
 *   ①' 子SKU の売価 (draft_sku_prices) … バリエーションの SKU 単位で人が決めた値
 *   ②  NE mirror の標準売価        … まだアプリに無い商品コード (親以外を混ぜたセット) 用
 *
 * 🚨 1 件でも単価が引けなければ **合計を作らない (null)**。欠けたまま足すと
 *    「2 個セットなのに 1 個ぶんの値段」が初期値として入り、人が「入っている」と見て
 *    そのまま出品する。空欄なら必ず気づく (fail-closed)。
 */
import { mirrorReady } from './variation.js';

const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());

/** 正の有限数だけを値段として通す (0 円・マイナス・NaN は「無い」扱い) */
function priceOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * 商品コード 1 件の単価。
 * @returns {{value: number, source: 'draft'|'sku'|'ne'}|null} 引けなければ null
 */
export function unitPriceOf(db, neCode) {
  const code = norm(neCode);
  if (!code) return null;

  // 🚨 どの引き先でも「**全行が一致するときだけ**採用する」(variation.js と同じ扱い)。
  //    正規化 (LOWER(TRIM())) で複数行に当たることは実際にある — `product_drafts` の
  //    UNIQUE は生の ne_code にしか効かないので `ABC` と ` abc ` が同居できる。
  //    そこで `ORDER BY id LIMIT 1` を採ると、**どちらの値が出るかは運**になる。
  //    「どの単価か分からない」は引けないのと同じ (fail-closed)
  const agreedPrice = (values) => {
    const vals = new Set(values.map(priceOrNull));
    if (vals.size !== 1) return null;      // 割れている / 有効値と無効値の混在
    return [...vals][0];                   // 全部無効なら null
  };

  // ① アプリのドラフト。**セットは数えない** (parent_draft_id IS NULL = 単品だけ)。
  //    セットの売価を単価として拾うと、セットのセットのような値になる
  const drafts = db.prepare(`
    SELECT price FROM product_drafts
    WHERE LOWER(TRIM(ne_code)) = ? AND parent_draft_id IS NULL AND price IS NOT NULL
  `).all(code);
  const fromDraft = drafts.length > 0 ? agreedPrice(drafts.map((d) => d.price)) : null;
  if (fromDraft != null) return { value: fromDraft, source: 'draft' };
  // 値が割れているのに次の引き先へ落ちると「人が入れた値を無視して NE を採る」になる。
  // 人が直すべき状態なので、ここで止める (Codex high 2026-09-04)
  if (drafts.length > 0) return null;

  // ①' バリエーションの子SKU に付けた売価 (`draft_sku_prices`)。単品のドラフトが無くても、
  //     人が SKU ごとに決めた売価があればそれが「アプリの値」(Codex medium 2026-09-04)。
  //     どのドラフトに属する行かは問わないが、割れていたら採らない
  const skuRows = db.prepare('SELECT price FROM draft_sku_prices WHERE sku_code = ?').all(code);
  if (skuRows.length > 0) {
    const fromSku = agreedPrice(skuRows.map((r) => r.price));
    return fromSku == null ? null : { value: fromSku, source: 'sku' };
  }

  // ② NE mirror の標準売価 (税込)。mirror が未取込の環境では黙って null
  if (!mirrorReady(db)) return null;
  let rows;
  try {
    rows = db.prepare('SELECT 標準売価 FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?').all(code);
  } catch (_) {
    return null;
  }
  if (rows.length === 0) return null;
  const fromNe = agreedPrice(rows.map((r) => r.標準売価));
  return fromNe == null ? null : { value: fromNe, source: 'ne' };
}

/**
 * 構成から売価の初期値を出す。
 * @param {Array<{ne_code?: string, code?: string, qty: number}>} members
 * @returns {{total: number|null, lines: Array<{code: string, qty: number, unit: number|null,
 *   subtotal: number|null, source: 'draft'|'sku'|'ne'|null}>, missing: string[]}}
 *   total は全件の単価が引けたときだけ入る。missing = 引けなかった商品コード
 */
export function setPriceFromMembers(db, members) {
  const lines = [];
  const missing = [];
  for (const m of members || []) {
    const code = String(m?.ne_code ?? m?.code ?? '').trim();
    const qty = Number(m?.qty);
    const hit = unitPriceOf(db, code);
    if (!hit || !Number.isInteger(qty) || qty < 1) {
      missing.push(code);
      lines.push({ code, qty: Number.isInteger(qty) ? qty : 0, unit: hit?.value ?? null, subtotal: null, source: hit?.source ?? null });
      continue;
    }
    lines.push({ code, qty, unit: hit.value, subtotal: hit.value * qty, source: hit.source });
  }
  const total = (lines.length > 0 && missing.length === 0)
    // 円未満は出さない (楽天の売価は整数)。四捨五入は最後に 1 回だけ
    ? Math.round(lines.reduce((s, l) => s + l.subtotal, 0))
    : null;
  return { total, lines, missing };
}

/** 「単品 1,980 × 2 = 3,960円」のような 1 行。画面とイベントで同じ言葉を使う */
export function describeSetPrice({ total, lines }) {
  if (total == null || !lines?.length) return null;
  const parts = lines.map((l) => `${l.code} ${l.unit.toLocaleString()}円 × ${l.qty}`);
  return `${parts.join(' + ')} = ${total.toLocaleString()}円`;
}
