/**
 * 郵便料金の判定エンジン (純粋関数)。
 *
 * 正本 = AI_reference『定形外郵便_料金区分の自動判定と印字_要件定義_20260830.md』§6
 *
 * 設計の芯:
 *   **全部を自動で当てにいかない。** 自動で確定できない少数を、確実に「不明」として人へ返す。
 *   正しそうな誤印字より「不明」のほうがはるかに安全 (紙に出た金額は後から直せない)。
 *
 * DB に触らない。呼び出し側が masters を渡す → テストしやすく、印字経路が決まる前でも動く。
 */

// 郵便物の寸法条件 (mm / g)。出典 = 日本郵便『国内の料金表 (手紙・はがき)』
export const LIMITS = {
  teikei:    { minL: 140, maxL: 235, minW: 90, maxW: 120, maxT: 10, maxG: 50 },
  kikakunai: { maxL: 340, maxW: 250, maxT: 30, maxG: 1000 },
  kikakugai: { maxL: 600, maxSum3: 900, maxG: 4000 },
};

export const UNKNOWN_REASONS = {
  missing_sku: '商品がマスタに無い',
  missing_weight: '商品の重さ未登録',
  missing_material: '資材が決まらない',
  material_conflict: '明細ごとに資材が違う',
  missing_dims: '資材の外寸が未測定',
  missing_material_thickness: '資材の厚み未登録',
  missing_thickness: '商品の厚み未登録',
  missing_composition: '商品構成が見つからない',
  not_teikeigai: '定形外の伝票ではない',
  near_weight_boundary: '重量が料金の境界に近い (要実測)',
  near_thickness_boundary: '厚みがサイズの境界に近い (要実測)',
  over_maximum: '定形外の上限を超える (郵便で出せない)',
  no_tariff: '対象日に有効な料金表が無い',
  no_lines: '明細が無い',
};

/**
 * 1 通ぶんの判定。
 *
 * @param {{lines: Array<{sku_code: string, qty: number}>}} shipment
 * @param {{
 *   skus: Map<string, {unit_weight_g?: number, thickness_mm?: number, default_material_code?: string, display_name?: string}>,
 *   materials: Map<string, {tare_weight_g?: number, thickness_mm?: number, outer_length_mm?: number, outer_width_mm?: number, dims_verified?: number, display_name?: string}>,
 *   bands: Array<{mail_type: string, band_code: string, display_name: string, max_weight_g: number, amount_yen: number}>,
 *   overheadG: number,
 *   boundaryMarginG: number,
 *   thicknessMarginMm: number,
 * }} ctx
 * @returns {{status:'confirmed'|'unknown', reason?:string, detail?:string, ...}}
 */
export function judge(shipment, ctx) {
  const lines = (shipment?.lines || []).filter((l) => l && l.sku_code);
  if (!lines.length) return unknown('no_lines');

  const marginG = num(ctx.boundaryMarginG, 5);
  const marginMm = num(ctx.thicknessMarginMm, 1);
  const overheadG = num(ctx.overheadG, 0);

  // ── 1. 商品の重さ ──────────────────────────────────────
  let itemWeightG = 0;
  let totalQty = 0;
  const missingWeight = [];
  const missingSku = [];
  for (const l of lines) {
    // 切り捨ててから整数か見ると 1.9 が 1 として通り、重量も厚みも過少になる (= 黙って安い区分)。
    // 元の値が整数かを見る
    const qty = Number(l.qty);
    if (!Number.isInteger(qty) || qty < 1) return unknown('no_lines', `数量が不正: ${l.sku_code}=${l.qty}`);
    totalQty += qty;
    const s = ctx.skus.get(l.sku_code);
    if (!s) { missingSku.push(l.sku_code); continue; }
    if (!Number.isFinite(s.unit_weight_g) || s.unit_weight_g <= 0) { missingWeight.push(l.sku_code); continue; }
    itemWeightG += s.unit_weight_g * qty;
  }
  if (missingSku.length) return unknown('missing_sku', missingSku.join(', '));
  if (missingWeight.length) return unknown('missing_weight', missingWeight.join(', '));

  // ── 2. 資材 ────────────────────────────────────────────
  // 印字時点では実際に使う資材が確定していないので、マスタの既定資材で「予測」する。
  // 明細ごとに違う資材が出たら、どちらを使うか決められない → 不明。
  const matCodes = new Set();
  for (const l of lines) {
    const code = ctx.skus.get(l.sku_code)?.default_material_code;
    if (code) matCodes.add(code);
  }
  if (matCodes.size === 0) return unknown('missing_material');
  if (matCodes.size > 1) return unknown('material_conflict', [...matCodes].join(' / '));
  const materialCode = [...matCodes][0];
  const material = ctx.materials.get(materialCode);
  if (!material) return unknown('missing_material', materialCode);
  if (!Number.isFinite(material.tare_weight_g) || material.tare_weight_g <= 0) {
    return unknown('missing_material', `${material.display_name || materialCode} の自重が未登録`);
  }

  const weightG = round1(itemWeightG + material.tare_weight_g + overheadG);

  // 資材そのものの厚み (封筒 1mm・プチ袋 2mm)。未登録を 0 とみなすと、定形 10mm の境界で
  // 1〜2mm がそのまま 110円/140円 の差になるので、入るまで確定させない
  const materialThicknessMm = material.thickness_mm;
  if (!Number.isFinite(materialThicknessMm) || materialThicknessMm <= 0) {
    return unknown('missing_material_thickness', `${material.display_name || materialCode} の厚みが未登録`, { weightG, materialCode });
  }

  // ── 3. 厚み ────────────────────────────────────────────
  // 数量が複数なら重なるので合計する (薄いものを並べる場合もあるが、厚い側に倒したうえで
  // 境界に近ければ「不明」に落とすので、黙って安い区分にはならない)。資材の厚みも足す。
  const missingThickness = [];
  let thicknessMm = 0;
  for (const l of lines) {
    const s = ctx.skus.get(l.sku_code);
    if (!Number.isFinite(s.thickness_mm) || s.thickness_mm <= 0) { missingThickness.push(l.sku_code); continue; }
    thicknessMm += s.thickness_mm * Number(l.qty);
  }
  if (missingThickness.length) return unknown('missing_thickness', missingThickness.join(', '), { weightG, materialCode });
  thicknessMm = round1(thicknessMm + materialThicknessMm);
  // 数量が複数のときは重なり方が読めないぶん、厚みの安全幅を倍にする
  const effThicknessMargin = totalQty > 1 ? marginMm * 2 : marginMm;

  // ── 4. サイズ区分 ──────────────────────────────────────
  // 外寸は「入っている」だけでは使わない。**人が実測したもの (dims_verified=1) だけ** 判定に使う。
  // 推定値で確定させると、画面に「未測定」と出ているのに最安の定形110円が出てしまう
  const L = material.outer_length_mm;
  const W = material.outer_width_mm;
  if (!Number.isFinite(L) || !Number.isFinite(W) || Number(material.dims_verified) !== 1) {
    const why = (Number.isFinite(L) && Number.isFinite(W)) ? 'の外寸が未実測 (推定値では確定しません)' : 'の外寸';
    return unknown('missing_dims', `${material.display_name || materialCode} ${why}`, { weightG, thicknessMm, materialCode });
  }
  // 長辺・短辺は入力順に依存させない
  const longMm = Math.max(L, W);
  const shortMm = Math.min(L, W);

  const size = classifySize({ longMm, shortMm, thicknessMm, weightG, thicknessMargin: effThicknessMargin });
  if (size.unknown) {
    return unknown(size.unknown, size.detail, { weightG, thicknessMm, materialCode });
  }
  const mailType = size.mailType;

  // ── 5. 料金帯 ──────────────────────────────────────────
  const candidates = (ctx.bands || [])
    .filter((b) => b.mail_type === mailType)
    .sort((a, b) => a.max_weight_g - b.max_weight_g);
  if (!candidates.length) return unknown('no_tariff', mailType, { weightG, thicknessMm, materialCode });

  const band = candidates.find((b) => weightG <= b.max_weight_g);
  if (!band) {
    // 上限超。黙って最大料金に丸めない (郵便では出せないので人が判断する)
    return unknown('over_maximum', `${weightG}g > ${candidates[candidates.length - 1].max_weight_g}g`,
      { weightG, thicknessMm, materialCode, mailType });
  }

  // 境界まで marginG 以内なら確定させない。率ではなくグラムで見る (Codex R1)
  if (band.max_weight_g - weightG <= marginG) {
    return unknown('near_weight_boundary',
      `${weightG}g / 境界 ${band.max_weight_g}g (残り ${round1(band.max_weight_g - weightG)}g)`,
      { weightG, thicknessMm, materialCode, mailType });
  }

  return {
    status: 'confirmed',
    mailType,
    bandCode: band.band_code,
    displayName: band.display_name,
    amountYen: band.amount_yen,
    weightG,
    itemWeightG: round1(itemWeightG),
    materialTareG: material.tare_weight_g,
    overheadG,
    thicknessMm,
    materialThicknessMm,
    materialCode,
    materialName: material.display_name || materialCode,
  };
}

/**
 * サイズ区分の判定。
 * 定形 → 規格内 → 規格外 の順に、条件を満たす一番安いものを選ぶ。
 * 境界に近いときは確定させず「不明」にする。
 */
function classifySize({ longMm, shortMm, thicknessMm, weightG, thicknessMargin }) {
  const t = LIMITS.teikei;
  // 定形は最小寸法もある (小さすぎても定形にならない)
  const fitsTeikeiSize = longMm >= t.minL && longMm <= t.maxL && shortMm >= t.minW && shortMm <= t.maxW;
  if (fitsTeikeiSize && weightG <= t.maxG) {
    if (thicknessMm <= t.maxT) {
      // 10mm ぎりぎりで「定形」と言い切ると、少し膨らんだだけで実際は 140円 になる
      if (t.maxT - thicknessMm <= thicknessMargin) {
        return { unknown: 'near_thickness_boundary', detail: `厚さ ${thicknessMm}mm / 定形の上限 ${t.maxT}mm` };
      }
      return { mailType: 'teikei' };
    }
    // 10mm をわずかに超えただけなら、実物では収まっている可能性がある
    if (thicknessMm - t.maxT <= thicknessMargin) {
      return { unknown: 'near_thickness_boundary', detail: `厚さ ${thicknessMm}mm / 定形の上限 ${t.maxT}mm` };
    }
  }

  const kn = LIMITS.kikakunai;
  if (longMm <= kn.maxL && shortMm <= kn.maxW && weightG <= kn.maxG) {
    if (thicknessMm <= kn.maxT) {
      if (kn.maxT - thicknessMm <= thicknessMargin) {
        return { unknown: 'near_thickness_boundary', detail: `厚さ ${thicknessMm}mm / 規格内の上限 ${kn.maxT}mm` };
      }
      return { mailType: 'kikakunai' };
    }
    if (thicknessMm - kn.maxT <= thicknessMargin) {
      return { unknown: 'near_thickness_boundary', detail: `厚さ ${thicknessMm}mm / 規格内の上限 ${kn.maxT}mm` };
    }
  }

  const kg = LIMITS.kikakugai;
  const sum3 = longMm + shortMm + thicknessMm;
  if (longMm <= kg.maxL && sum3 <= kg.maxSum3 && weightG <= kg.maxG) return { mailType: 'kikakugai' };

  return { unknown: 'over_maximum', detail: `長辺 ${longMm}mm / 3辺計 ${round1(sum3)}mm / ${weightG}g` };
}

function unknown(reason, detail, extra = {}) {
  return { status: 'unknown', reason, reasonLabel: UNKNOWN_REASONS[reason] || reason, detail: detail || null, ...extra };
}

function num(v, fallback) { return Number.isFinite(Number(v)) ? Number(v) : fallback; }
function round1(v) { return Math.round(v * 10) / 10; }
