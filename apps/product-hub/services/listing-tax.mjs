/**
 * listing-tax.mjs — 楽天の出品 (プレビュー・登録) に使う税率を決める (PR #1565 Codex R1 H5・Company DB構想 10 §4 #6)
 *
 *   段階 legacy_open (切替前)           → { mode: 'legacy' } = 今までどおり draft_yahoo.tax_rate (buildItemPayload がそのまま読む)
 *   段階 frozen 以降 / 段階が読めない   → Company DB の税率 (代表コードは構成の SKU から)。決められない = { mode: 'blocked' } = 出品を止める
 * 段階は毎回読む (lib/master-legacy-gate.mjs の書き込みと同じ読み方。前の値は使わない)
 */
import { checkLegacyGate } from '../../../lib/master-legacy-gate.mjs';
import { resolveCdbDraftTax } from './cdb-tax-rate.mjs';

export async function resolveListingTax(db, draft, { gate = checkLegacyGate } = {}) {
  let g;
  try { g = await gate({ purpose: 'write' }); } catch { g = { writable: false, readable: false }; }
  if (g && g.writable === true) return { mode: 'legacy' };
  const r = await resolveCdbDraftTax(db, draft);
  if (!r.ok) {
    return { mode: 'blocked', reason: `税率を Company DB から決められないので出品を止めています (マスタは新しい画面で直します): ${r.reason}` };
  }
  return { mode: 'cdb', percent: r.percent, label: r.label };
}
