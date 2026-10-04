/**
 * listing-tax.mjs — 楽天の出品 (プレビュー・登録) に使う税率を決める (PR #1565 Codex R1 H5・中間レビュー High-1・Company DB構想 10 §4 #6)
 *
 *   段階を読めて legacy_open (切替前)   → { mode: 'legacy' } = 今までどおり draft_yahoo.tax_rate (buildItemPayload がそのまま読む)
 *   段階を読めて frozen 以降 かつ 税率の持ち主が C (⑤-3b) → Company DB の税率 (代表コードは構成の SKU から)。決められない = { mode: 'blocked' } = 出品を止める
 *   🚨 段階を読めない                   → { mode: 'blocked' } = 出品を止める (切替前の瞬断で Company DB の税率に黙って切り替えない・古い値でも送らない)
 *      プレビュー (preview: true = 楽天に送らない) だけは { mode: 'legacy', warning } = 今の値で見せて注意を添える (中間レビュー 2 回目 Low)
 * 段階は毎回読む (lib/master-legacy-gate.mjs の書き込みと同じ読み方。前の値は使わない)
 */
import { checkLegacyGate } from '../../../lib/master-legacy-gate.mjs';
import { resolveCdbDraftTax } from './cdb-tax-rate.mjs';

/** 税率の列 (⑤-3b: 段階が legacy_open 以外でも、税率の持ち主が load のあいだは今までどおり draft_yahoo の税率) */
export const TAX_COLS = Object.freeze(['skus.tax_rate']);
export async function resolveListingTax(db, draft, { gate = checkLegacyGate, preview = false } = {}) {
  let g;
  try { g = await gate({ purpose: 'write', cols: TAX_COLS }); } catch (e) { g = { writable: false, readable: false, error: String((e && e.message) || e) }; }
  if (!g || g.readable !== true) {
    if (preview) return { mode: 'legacy', warning: '切替の状態を読めないので、今の税率 (draft_yahoo) で見せています。登録はできません (少し待ってもう一度)' };
    return { mode: 'blocked', reason: `切替の状態を読めないので出品を止めています (少し待ってもう一度)${g && g.error ? `: ${g.error}` : ''}` };
  }
  if (g.writable === true) return { mode: 'legacy' };
  const r = await resolveCdbDraftTax(db, draft);
  if (!r.ok) {
    return { mode: 'blocked', reason: `税率を Company DB から決められないので出品を止めています (マスタは新しい画面で直します): ${r.reason}` };
  }
  return { mode: 'cdb', percent: r.percent, label: r.label };
}
