/**
 * profit-estimate.js — 1 個あたりの概算の利益 (純粋な関数だけ。DB も fetch も触らない・import もしない = ブラウザでもそのまま動く)
 *
 * 式 (中原さん 10/6「他の部分でいろいろ使っている利益計算」):
 *   利益 = 売価 − プラットフォーム手数料 (売価 × 手数料の率) − 原価 × (1 + 税率) − 配送料
 *   利益率 = 利益 / 売価
 *   ・売価は税込 (モールの設定価格・NE の売価 baika_tnk = Company DB の standard_price_jpy はどれも税込)
 *   ・原価は税抜 → × (1 + 税率) で税込にそろえる。税率が未登録なら 10% (DEFAULT_TAX_RATE)
 *   ・手数料 = round(売価 × 率) (整数円)
 *   ・🚨 原価・売価が無いときは 0 円として計算しない (gross = null)。Number(null) = 0 にすると「原価未登録」が「原価 0 円」に化けて、利益が大きく見える
 *
 * 使うところ (同じ関数 = 画面ごとに数字がずれない):
 *   - apps/price-update/pricing.js (参考の概算粗利。手数料の率は dim_mall.fee_rate_approx = モールごと)
 *   - apps/master-edit (マスタの入力: 一覧の「利益」「利益率」の列・並べ替え・CSV・1 つの商品の画面の「利益 (1 個あたり)」)。
 *     ブラウザには router が /apps/master-edit/public/profit-estimate.js でこのファイルをそのまま配る (public/me-profit.js が読む)
 */

/** 消費税率が未登録なら 10% (amazon-accounting・price-update と同じ扱い) */
export const DEFAULT_TAX_RATE = 0.10;

/**
 * プラットフォーム手数料の率 (売価に対する割合)。マスタの入力の利益はモールを決めない参考の値 = standard (10%) を使う。
 * 🚨 率を変える・モールごとに分けるときはここだけ直す (画面・一覧・CSV・つかいかたの数字はここから読む)。
 *    将来の例: { standard: 0.10, rakuten: 0.12, amazon: 0.15 } として呼ぶ側で feeRate: PLATFORM_FEE_RATES.rakuten
 */
export const PLATFORM_FEE_RATES = Object.freeze({ standard: 0.10 });

/**
 * 数値化。★null / undefined / 空文字は null のまま返す (素の Number() は null を 0 にする = 原価未登録が原価 0 円に化ける)
 */
export function toNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * 概算粗利 (参考表示用)。apps/price-update/pricing.js から 2026-10-06 にそのまま移した (中身は変えていない)。
 * @param {{price:number, cost:number|null, taxRate:number|null, feeRate:number, shipping:number|null}} p
 * @returns {{gross:number|null, rate:number|null, fee:number, costInclTax:number|null, shipping:number}}
 *   cost が無い商品は gross=null (「原価未登録」として表示側で区別する。0 円原価として計算しない)
 */
export function estimateGross({ price, cost, taxRate, feeRate, shipping }) {
  const p = toNum(price);
  const c = toNum(cost);
  const t = toNum(taxRate) ?? DEFAULT_TAX_RATE;
  const f = toNum(feeRate) ?? 0;
  const s = toNum(shipping) ?? 0;
  const fee = p == null ? 0 : Math.round(p * f);
  if (p == null || c == null) {
    return { gross: null, rate: null, fee, costInclTax: c == null ? null : c * (1 + t), shipping: s };
  }
  const costInclTax = c * (1 + t);
  const gross = p - costInclTax - fee - s;
  return { gross, rate: p > 0 ? gross / p : null, fee, costInclTax, shipping: s };
}

/**
 * 円の丸め (見せる数の共通・Codex #1632 R2 M): 銭 (円未満 2 桁) まで丸めてから円に四捨五入・−0 は 0。
 * 🚨 マスタの入力 (masterProfit) と価格改定の画面 (pricing.js の estimate.grossYen) はどちらもこれで円にする = 同じ商品・同じ値なら同じ円。
 *    素の Math.round(gross) は浮動小数の端で 1 円ずれる (売価 1,367・原価 645・10%・送料 520・手数料 10% = 0.5 円が 0.4999999999998863 → 0 円)。
 *    estimateGross の生の gross は変えない (判定に使う側はそのまま)
 */
export function grossYen(gross) {
  const g = toNum(gross);
  return g == null ? null : roundHalf(round2(g), 0);
}
/** 利益率の % (小数 1 桁の数)。端を落としてから 1 桁に (0.1235 → 12.4 / −0.0375 → −3.8)。−0 は 0。見せる所はどれもこれを使う */
export function ratePct1(rate) {
  const r = toNum(rate);
  return r == null ? null : roundHalf(roundHalf(r * 100, 4), 1);
}
/**
 * 四捨五入 (0.5 は 0 から遠い方へ = −51.5 → −52・−3.75 → −3.8)。digits = 小数の桁。−0 は 0。
 * 🚨 Math.round は −51.5 を −51 にする (正の方へ丸める) = 赤字が 1 円・0.1 ポイント小さく見える (Codex #1632 R3 M)。円・% を見せる丸めはどれもこれ
 */
export function roundHalf(v, digits = 0) {
  const k = 10 ** digits;
  return (Math.sign(v) * Math.round(Math.abs(v) * k)) / k + 0;
}

/** 計算できない理由 (画面・一覧・CSV で同じ言葉) */
export const PROFIT_MISSING_WORDS = Object.freeze({
  price: '標準売価が未入力',
  cost: '原価が未登録',
});

/**
 * マスタの入力の「利益 (1 個あたり)」。中身は estimateGross (上と同じ式)。
 * 売価が無い・0 円 / 原価が無い = ok:false (理由つき・0 として計算しない)。
 * 税率が無い = 10% として計算 (notes に tax_default)・配送料が無い = 0 円として計算 (notes に shipping_zero) = 画面で「仮に」と知らせる
 * @param {{price:any, cost:any, taxRate:any, shipping:any, feeRate?:number}} p
 * @returns {{ok:true, profit:number, rate:number, price:number, fee:number, feeRate:number, cost:number, taxRate:number, costInclTax:number, shipping:number, notes:string[]}
 *         | {ok:false, missing:string[], reason:string, notes:string[]}}
 */
export function masterProfit({ price, cost, taxRate, shipping, feeRate = PLATFORM_FEE_RATES.standard }) {
  const p = toNum(price);
  const c = toNum(cost);
  const notes = [];
  if (toNum(taxRate) == null) notes.push('tax_default');
  if (toNum(shipping) == null) notes.push('shipping_zero');
  const missing = [];
  if (p == null || p <= 0) missing.push('price');
  if (c == null) missing.push('cost');
  if (missing.length) {
    return { ok: false, missing, reason: `${missing.map((k) => PROFIT_MISSING_WORDS[k]).join('・')}なので計算できません`, notes };
  }
  const e = estimateGross({ price: p, cost: c, taxRate, feeRate, shipping });
  // 内訳の数 (円未満 2 桁まで = 浮動小数の端を落とす。原価 × 1.1 / 1.08 と送料の表の小数はこれで正確に出る)。
  // 利益 = その利益 (2 桁) を四捨五入した円 = 内訳の 1 行の等式が必ず成り立つ (Codex #1632 R1 Low: 税込原価だけ先に丸めると 1,000 − 100 − 17 − 0 = 884 になった)
  const gross = round2(e.gross);
  const profit = grossYen(e.gross);   // 価格改定の画面と同じ円の丸め
  return {
    ok: true, profit, gross, rate: e.rate, price: p, fee: e.fee, feeRate: toNum(feeRate) ?? 0, cost: c,
    taxRate: toNum(taxRate) ?? DEFAULT_TAX_RATE, costInclTax: round2(e.costInclTax), shipping: round2(e.shipping), notes,
  };
}

const yen = (v) => Number(v).toLocaleString('ja-JP', { maximumFractionDigits: 2 });
/** 円未満 2 桁に (0.1 × 3 などの浮動小数の端を落とす) */
function round2(v) { return roundHalf(v, 2); }
/** 利益率の見せ方 (小数 1 桁の %)。例 0.2698 → '27.0%' / −0.05 → '−5.0%' */
export function fmtProfitRate(rate) {
  const v = ratePct1(rate);
  if (v == null) return '—';
  return `${v < 0 ? '−' : ''}${Math.abs(v).toFixed(1)}%`;
}
/** 利益の見せ方 (円・円未満は 2 桁まで)。マイナスは「−」 */
export function fmtProfitYen(v) {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v < 0 ? '−' : ''}${yen(Math.abs(v))}`;
}
/**
 * 内訳の 1 行 (画面と試験で同じ文)。税込原価・配送料は円未満も出す (丸めない = 左の式と右の数が必ず合う)。
 * 例: 売価 1,000 − 手数料 100 (10%) − 税込原価 110 (100 × 1.10) − 配送料 520 = 利益 270 円
 *     売価 1,000 − 手数料 100 (10%) − 税込原価 16.5 (15 × 1.10) − 配送料 0 = 883.5 → 利益 884 円 (四捨五入)
 */
export function profitLine(r) {
  if (!r || !r.ok) return '';
  const pct = Math.round(r.feeRate * 1000) / 10;
  return `売価 ${yen(r.price)} − 手数料 ${yen(r.fee)} (${pct}%) − 税込原価 ${yen(r.costInclTax)} (${yen(r.cost)} × ${(1 + r.taxRate).toFixed(2)}) − 配送料 ${yen(r.shipping)} = ${Number.isInteger(r.gross) ? '' : `${fmtProfitYen(r.gross)} → `}利益 ${fmtProfitYen(r.profit)} 円${Number.isInteger(r.gross) ? '' : ' (四捨五入)'}`;
}
/** 「仮に」の知らせ (税率・配送料が未入力のとき) */
export function profitNoteWords(notes) {
  const out = [];
  if ((notes || []).includes('tax_default')) out.push(`税率が未入力なので ${Math.round(DEFAULT_TAX_RATE * 100)}% として計算`);
  if ((notes || []).includes('shipping_zero')) out.push('送料が未入力なので配送料 0 円として計算 (利益が多めに出ます)');
  return out;
}
