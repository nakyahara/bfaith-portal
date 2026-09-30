/**
 * master-set-rules.js — セットの税率・売上分類・取扱区分・原価を構成品から決める規則 (純粋な関数だけ。DB を読まない)
 *
 * 使うところ:
 *   - apps/warehouse/rebuild-m-products.js (miniPC の m_products の作り直し。NE の値の形で渡す)
 *   - apps/warehouse/router.js (/register の税率・売上分類の即時反映)
 *   - lib/master-write.mjs (マスタ入力画面 apps/master-edit。Company DB の値の形で渡す = 下の「Company DB の形」)
 * 規則を 2 つ書かないために 1 か所に置く (Company DB構想 14 §6 ⑤-1「セットの導く規則を lib に移して rebuild と共用」)。
 * 🚨 中身は 2026-09-30 に rebuild-m-products.js からそのまま移した。rebuild-m-products.js は同じ名前で export し直している (今までの import はそのまま動く)
 * 🚨 規則を変えたら apps/warehouse/master-material.js の MASTER_BUILD_RULE_VERSION も上げる (作り直しの規則の版)
 */

// 既知税率マスタ (税制変更時はここに1行追加するだけで全箇所追従する)
//   neRate: NEが返す整数 (raw_ne_products.消費税率)
//   decimal: m_products / product_tax_rate に格納する小数表現
//   category: 税区分 (会計上の意味を持つので機械的に生成しない。明示で持つ)
export const TAX_RATES = [
  { neRate: 10, decimal: 0.1,  category: 'STANDARD_10' },
  { neRate: 8,  decimal: 0.08, category: 'REDUCED_8' },
  // 将来例: { neRate: 12, decimal: 0.12, category: 'STANDARD_12' },
];
export const KNOWN_NE_RATES = TAX_RATES.map(t => t.neRate);
export const KNOWN_DECIMAL_RATES = TAX_RATES.map(t => t.decimal);

// 税率解決: NE側を優先、NE未登録(null/0)時のみ手動登録 (product_tax_rate) を使う
// neTaxNum: raw_ne_products.消費税率 (整数)
// manualTaxRate: product_tax_rate.tax_rate (小数)
// NE が想定外値 (TAX_RATES 未登録) の場合は UNKNOWN を返す (upstream 異常を隠さない)
export function resolveTaxRate(neTaxNum, manualTaxRate) {
  const byNe = TAX_RATES.find(t => t.neRate === neTaxNum);
  if (byNe) return { taxRate: byNe.decimal, taxCategory: byNe.category };
  // NE 未登録 (null / 0) 時のみ手動値にフォールバック
  if (neTaxNum == null || neTaxNum === 0) {
    const byManual = TAX_RATES.find(t => t.decimal === manualTaxRate);
    if (byManual) return { taxRate: byManual.decimal, taxCategory: byManual.category };
  }
  return { taxRate: null, taxCategory: 'UNKNOWN' };
}

// セット税率解決: 構成品を1つずつ resolveTaxRate に通し、全構成品が解決できたときだけ確定する。
// 構成品の NE 消費税率だけを直接見ると、NE 未登録(0)を product_tax_rate で救済した構成品を持つ
// セットが UNKNOWN に落ちる (単品は救済され、セットだけ税率 NULL になる非対称が起きる)。
// components: [{ neTaxRate, manualTaxRate, componentExists }]
//   componentExists=false (構成品が NE 商品マスタに存在しない) は上流異常なので、
//   product_tax_rate に値が残っていても UNKNOWN に倒す (欠損を税率で隠さない)
// 単一税率 → その税率 / 複数税率 → MIXED (最小値 = 軽減税率優先) / 1つでも未解決 → UNKNOWN
export function resolveSetTaxRate(components) {
  const decimals = new Set();
  for (const c of components) {
    if (c.componentExists === false) return { taxRate: null, taxCategory: 'UNKNOWN' };
    const { taxRate } = resolveTaxRate(c.neTaxRate, c.manualTaxRate);
    if (taxRate === null) return { taxRate: null, taxCategory: 'UNKNOWN' };
    decimals.add(taxRate);
  }
  if (decimals.size === 0) return { taxRate: null, taxCategory: 'UNKNOWN' };
  if (decimals.size === 1) {
    const def = TAX_RATES.find(t => t.decimal === [...decimals][0]);
    return def
      ? { taxRate: def.decimal, taxCategory: def.category }
      : { taxRate: null, taxCategory: 'UNKNOWN' };
  }
  return { taxRate: Math.min(...decimals), taxCategory: 'MIXED' };
}

// 売上分類の値域 (1=自社商品 / 2=取引先限定 / 3=仕入れ商品 / 4=輸出)
export const SALES_CLASSES = [1, 2, 3, 4];
// 4=輸出 は「仕入区分」ではなく販売チャネル属性で、1〜3 と直交する。
// amazon-accounting でも 4 は集計対象外 (excluded segment) として別扱いされている。
export const EXPORT_SALES_CLASS = 4;

// セット売上分類解決の決定表:
//   構成品がすべて 1〜3      → MIN を採用 (階層論理 1 > 2 > 3)
//   構成品がすべて 4         → 4
//   4 と 1〜3 が混在         → null (導出しない)
//   1つでも未登録 / NE に無い → null (導出しない)
//
// MIN の根拠 = amazon-accounting のセット按分と同じ業務ルール。
// 「自社商品(1)を含むセットは自社商品セットと見なす」という運用に合わせる。
// 4 混在を MIN で潰すと輸出セットが国内分類に落ちて会計処理を誤るため、
// ここだけは MIN を適用せず人の判断に回す (= 未登録一覧に出る)。
//   ※2026-08-07 時点の本番データに 売上分類=4 の商品は 0 件。将来の事故防止の予防線。
//
// 原価・税率はセットを構成品から導出しているのに、売上分類だけ手動登録のみだったため、
// セットの登録漏れが m_products に NULL のまま残り、amazon-accounting 側で
// 「その他/未分類」に落ちていた (2026-08-07 調査)。ここで同じ導出を入れて揃える。
//
// components: [{ salesClass, componentExists }]
//   1つでも解決できない構成品があれば null を返す (= 未登録一覧に出して人に登録させる)。
//   欠けた構成品が実は 1(自社) だった場合に MIN が誤って 3 等に確定するのを防ぐため、
//   「一部だけ分かる」状態では導出しない。componentExists=false (NE 商品マスタに無い)
//   も上流異常なので、product_sales_class に値が残っていても null に倒す。
//
// ネストセット (構成品がそれ自体セット) について:
//   呼び出し側は構成品の「手動登録値」(product_sales_class) だけを渡す。構成セットの
//   導出値は伝播しないので、親セットは null = 未登録一覧に出る (誤った値が静かに入らない)。
//   2026-08-07 時点の本番データにネストセットは 0 件。発生時は rebuild の品質チェックが警告する。
export function resolveSetSalesClass(components) {
  if (!Array.isArray(components) || components.length === 0) return null;
  const classes = [];
  for (const c of components) {
    if (c.componentExists === false) return null;
    const sc = Number(c.salesClass);
    if (!SALES_CLASSES.includes(sc)) return null;
    classes.push(sc);
  }
  const uniq = new Set(classes);
  if (uniq.has(EXPORT_SALES_CLASS) && uniq.size > 1) return null; // 輸出と国内分類の混在
  return Math.min(...classes);
}

// 取扱区分 (NE の値)。実データにあるのは 取扱中 / 取扱中止 / ﾒｰｶｰ取扱中止 の 3 つ (2026-09-10 実測)
export const HANDLING_ACTIVE = '取扱中';
export const HANDLING_STOPPED = '取扱中止';
export const HANDLING_MAKER_STOPPED = 'ﾒｰｶｰ取扱中止';

// セット取扱区分の決定表 (2026-09-14 中原さん指示):
//   NE のセット自身が 取扱中 以外 (取扱中止 等)   → その値のまま (NE で人が決めた止め方を上書きしない)
//   構成品に ﾒｰｶｰ取扱中止 が 1 つでもある        → ﾒｰｶｰ取扱中止 (再開の見込みがない方を優先)
//   構成品に 取扱中止 が 1 つでもある            → 取扱中止
//   構成品に それ以外の 取扱中でない値 がある     → その値 (値が増えた日に黙って取りこぼさない)
//   どれにも当たらない                          → NE のセットの値 (無ければ 取扱中 = 従来どおり)
//
// 構成品が 1 つでも止まっていれば、そのセットはもう組めない = 売れない。
// NE ではセット自身の取扱区分が 取扱中 のまま残っていることが多いので、ここで引き継ぐ。
//
// components: [{ handlingClass, componentExists }]
//   構成品が NE に無い / 取扱区分が空 のものは「分からない」なので、それだけではセットを止めない
//   (止めた扱いにすると、まだ売っているセットが「もう扱っていない」側に落ちる)。
//   🚨 ネストセット (構成品がそれ自体セット) は、構成セットの NE の値だけを見る (導出値は伝播しない)。
//      親 → 子セット → 止まった単品 では、子セットは止まるが親セットは取扱中のまま残る。
//      本番のネストセットは 0 件 (2026-09-14 実測)。発生したら rebuild の品質チェック (B7b) が警告する。
//
// 空白の扱い: 比べるときだけ前後の空白を除く。NE のセット自身の値を返すときは元の値のまま返す
//   (この関数は NE の値を「引き継ぐかどうか」だけを決め、NE の値そのものは書き換えない)。
//   空白だけの値は 未登録 (NULL) と同じに扱う (本番に 0 件 = 2026-09-14 実測)。
export function resolveSetHandlingClass(neSetStatus, components) {
  const raw = typeof neSetStatus === 'string' && neSetStatus.trim() ? neSetStatus : null;
  const own = raw ? raw.trim() : '';
  if (own && own !== HANDLING_ACTIVE) return raw;
  const stopped = new Set();
  for (const c of Array.isArray(components) ? components : []) {
    if (!c || c.componentExists === false) continue;
    const h = typeof c.handlingClass === 'string' ? c.handlingClass.trim() : '';
    if (h && h !== HANDLING_ACTIVE) stopped.add(h);
  }
  if (stopped.has(HANDLING_MAKER_STOPPED)) return HANDLING_MAKER_STOPPED;
  if (stopped.has(HANDLING_STOPPED)) return HANDLING_STOPPED;
  if (stopped.size > 0) return [...stopped].sort()[0];
  return raw ?? HANDLING_ACTIVE;
}

// セットの原価 (構成品の原価 × 数量の合計)。rebuild-m-products.js の作り直しと同じ決め方:
//   構成品が全部「原価 > 0」→ 合計 (小数 2 桁で丸め) = COMPLETE
//   1 つでも 0・空の構成品がある → 合計しない。1 つでも原価のある構成品があれば PARTIAL、無ければ MISSING (構成品 0 個も MISSING)
// components: [{ cost, qty }]  qty が空・0 なら 1 として数える (作り直しの `数量 || 1` と同じ)
export function setCostFromComponents(components) {
  let total = 0;
  let hasAll = true;
  let hasAny = false;
  const list = Array.isArray(components) ? components : [];
  for (const c of list) {
    const cost = c ? c.cost : null;
    if (cost > 0) {
      total += cost * (c.qty || 1);
      hasAny = true;
    } else {
      hasAll = false;
    }
  }
  if (hasAll && list.length > 0) return { status: 'COMPLETE', jpy: Math.round(total * 100) / 100 };
  return { status: hasAny ? 'PARTIAL' : 'MISSING', jpy: null };
}

// ─── Company DB の形 (lib/master-write.mjs・apps/master-edit) ───
// Company DB の SKU は 税率 = 0.08 / 0.1 / null、取扱区分 = 'active' / 'discontinued' / 'unknown'、売上分類 = 1〜4 / null。
// 上の NE の形の規則に写してから同じ関数で決める (規則を 2 つ書かない)。
// 夜間ロード (apps/company-db/load/sources.mjs の mapHandling / mapTaxRate / mapTaxClass) が作り直しの結果を写したのと同じ答えになることは
// scripts/test-master-edit.mjs が確かめる。構成品は Company DB の FK で必ずある (componentExists = true)

/** セットの税率・税区分 (構成品の税率から)。1 つでも未登録 (null) なら { taxRate: null, taxClass: UNKNOWN }・複数の税率なら MIXED (最小の税率) */
export function deriveSetTaxCdb(componentTaxRates) {
  const r = resolveSetTaxRate((componentTaxRates || []).map((rate) => ({
    neTaxRate: null, manualTaxRate: rate == null ? null : Number(rate), componentExists: true,
  })));
  return { taxRate: r.taxRate, taxClass: r.taxCategory };
}

/** セットの売上分類: 人が決めた値 (skus.set_sales_class_override) があればそれ。無ければ構成品の MIN (導けなければ null) */
export function deriveSetSalesClassCdb(override, componentSalesClasses) {
  if (override != null && SALES_CLASSES.includes(Number(override))) return Number(override);
  return resolveSetSalesClass((componentSalesClasses || []).map((sc) => ({ salesClass: sc, componentExists: true })));
}

const CDB_TO_NE_HANDLING = { active: HANDLING_ACTIVE, discontinued: HANDLING_STOPPED };
/**
 * セットの取扱区分 (skus.handling)。own = セット自身の値 (skus.handling_own: 'active' / 'discontinued' / null)、
 * componentHandlings = 構成品の skus.handling、current = 今のセットの skus.handling。
 *   own がある → resolveSetHandlingClass (セット自身が止まっていれば止まる・構成品が 1 つでも止まれば止まる・それ以外は取扱中)
 *   own が無い (夜間ロードが入れたセットで、まだ人が決めていない) → 構成品が止まっていれば止める。そうでなければ今の値のまま
 *     (🚨 セット自身が止まっているのか分からないので、止まっているセットを勝手に取扱中に戻さない)
 * 構成品の 'unknown' (NE の取扱区分が空) はそれだけでセットを止めない (作り直しの「分からないものは止めない」と同じ)
 */
export function deriveSetHandlingCdb(own, componentHandlings, current) {
  const comps = (componentHandlings || []).map((h) => ({ handlingClass: CDB_TO_NE_HANDLING[h] ?? null, componentExists: true }));
  if (own == null) {
    const stopped = comps.some((c) => c.handlingClass != null && c.handlingClass !== HANDLING_ACTIVE);
    return stopped ? 'discontinued' : (current ?? 'unknown');
  }
  const r = resolveSetHandlingClass(CDB_TO_NE_HANDLING[own] ?? null, comps);
  return r === HANDLING_ACTIVE ? 'active' : 'discontinued';
}

/** セットの原価 (構成品の原価 × 数量)。Company DB の原価は円の整数 = 合計も整数に丸める。{ status, jpy } (COMPLETE 以外は jpy = null) */
export function deriveSetCostCdb(components) {
  const r = setCostFromComponents((components || []).map((c) => ({ cost: c.costJpy == null ? null : Number(c.costJpy), qty: Number(c.qty) })));
  return r.status === 'COMPLETE' ? { status: 'COMPLETE', jpy: Math.round(r.jpy) } : r;
}

/**
 * セットの導く値をまとめて決める 1 つの関数 (画面の「今の構成」「依頼中の構成」・保存の確かめの全部がこれを使う。Codex ⑤-R0 Medium 8)
 * components: [{ code, qty, tax_rate, sales_class, handling, cost_jpy }]  cost_jpy = その日の原価 (COMPLETE / OVERRIDDEN 以外は null)
 * opts: { override = 売上分類の上書き (skus.set_sales_class_override)・handlingOwn = セット自身の取扱・currentHandling = 今の skus.handling・
 *         exceptionCost = 例外原価がある (構成品の合計の代わりに人が決めた原価を使う) }
 * 戻り値: { tax: { taxRate, taxClass }, salesClass, salesFromComponents, salesSource ('override' | 'components' | null), handling, cost: { status, jpy },
 *          blockers: [導けない = このままでは保存しない理由], warnings: [気をつけること] }
 * 導けない (blockers):
 *   - 構成品が無い
 *   - 税率が決まらない (構成品の税率が 1 つでも未入力)。税率に上書きは無い = 先に単品の税率を入れる
 *   - 売上分類が決まらない (構成品の売上分類が未入力・輸出 4 と 1〜3 の混在) で、上書きも無い
 *   - 原価を合計できない (構成品の原価が 0・未入力) で、例外原価も無い
 * 🚨 8% と 10% の混在 (MIXED) は導ける (低い方の 8%・作り直しと同じ) = 気をつけることだけ
 */
export function deriveSetCdb(components, opts = {}) {
  const list = Array.isArray(components) ? components : [];
  const tax = deriveSetTaxCdb(list.map((c) => c.tax_rate));
  const salesFromComponents = deriveSetSalesClassCdb(null, list.map((c) => c.sales_class));
  const override = opts.override != null && SALES_CLASSES.includes(Number(opts.override)) ? Number(opts.override) : null;
  const salesClass = override ?? salesFromComponents;
  const handling = deriveSetHandlingCdb(opts.handlingOwn ?? null, list.map((c) => c.handling), opts.currentHandling ?? null);
  const cost = deriveSetCostCdb(list.map((c) => ({ costJpy: c.cost_jpy, qty: c.qty })));
  const codes = (pred) => list.filter(pred).map((c) => c.code).join('・');
  const blockers = [];
  const warnings = [];
  if (!list.length) blockers.push('構成品がありません (構成を入れてください)');
  else {
    if (tax.taxRate == null) blockers.push(`構成品 ${codes((c) => c.tax_rate == null)} の税率が未入力なので、セットの税率が決まりません (先に単品の税率を入れてください)`);
    if (salesClass == null) {
      const missing = codes((c) => !SALES_CLASSES.includes(Number(c.sales_class)));
      blockers.push(`構成品から売上分類を導けません (${missing ? `未入力: ${missing}` : '輸出 4 と 1〜3 が混ざっている'})。「売上分類の上書き」を選んでください`);
    }
    if (cost.status !== 'COMPLETE' && !opts.exceptionCost) {
      blockers.push(`構成品 ${codes((c) => !(c.cost_jpy > 0))} の原価が無いので、セットの原価を合計できません (先に単品の原価を入れるか、例外原価を入れてください)`);
    }
  }
  if (tax.taxClass === 'MIXED') warnings.push('構成品の税率が 8% と 10% で混ざっています (セットの税率は低い方の 8%・MIXED)');
  const stopped = codes((c) => c.handling === 'discontinued');
  if (stopped) warnings.push(`中止の構成品 (${stopped}) があるので、セットも中止になります`);
  // 数量 1 の構成品を先頭に、は NE の決まりではない (中原さん 2026-10-01「必須ではない」) = 止めない・言わない。並びは入れたとおりに持つ
  return { tax, salesClass, salesFromComponents, salesSource: override != null ? 'override' : (salesFromComponents != null ? 'components' : null), handling, cost, blockers, warnings };
}

/**
 * 2 つの構成が同じか = 構成品・数量・並び・行の数まで全部同じ (Codex ⑤-R1 H2)。依頼の取り下げの判定と、NE の観測で依頼を上げるときの判定に使う。
 * rows = [{ key, qty, sort }]  key = sku_id (か正規化したコード)。並び = sort の小さい順。
 * 🚨 並びが決められない (sort が数でない・同じ sort が 2 つ) = 同じと言わない (上げない側に倒す)
 */
export function compositionEquals(a, b) {
  const norm = (rows) => {
    const list = Array.isArray(rows) ? rows : [];
    const sorts = list.map((r) => Number(r && r.sort));
    if (sorts.some((x) => !Number.isFinite(x)) || new Set(sorts).size !== sorts.length) return null;
    return [...list].sort((x, y) => Number(x.sort) - Number(y.sort)).map((r) => `${String(r.key)}|${Number(r.qty)}`);
  };
  const x = norm(a);
  const y = norm(b);
  if (!x || !y || x.length !== y.length) return false;
  return x.every((v, i) => v === y[i]);
}
