/**
 * m_products 統合商品マスタ 再構築スクリプト
 *
 * staging テーブルに投入 → 品質チェック → 本番反映
 * daily-sync.js から呼び出す or 単体実行可能
 */
import { getDB } from './db.js';

// ─── ヘルパー ───

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

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

// ─── 本番反映時の列リスト（Codex PR1 Round 3 High 反映: 明示列INSERT） ───
// 物理的な列順が異なるDBでも値が正しくマップされるよう、
// DELETE + INSERT INTO target (...) SELECT ... FROM staging で列名を明示する。
export const MP_COLS = [
  'product_id', '商品コード', '商品名', '商品区分', '取扱区分',
  '標準売価', '原価', '原価ソース', '原価状態',
  '送料', '送料コード', '配送方法',
  '消費税率', '税区分',
  '在庫数', '引当数', '仕入先コード', 'セット構成品数', '売上分類',
  'seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date',
  'updated_at',
];

export const MSC_COLS = [
  'セット商品コード', '構成商品コード', '数量', '構成商品名', '構成商品原価', 'updated_at',
];

function colList(cols) {
  return cols.map(c => `"${c}"`).join(', ');
}

// ─── launch_date 解決ヘルパー（PR ：launch_date 自動検出） ───
//   設計書§14: candidates.js の NEW_PRODUCT_WINDOW_DAYS と一対。
//   優先順位: 既存 m_products 値（手動 or 過去の自動引き継ぎ）→ NE 作成日 → null
//   NE goods_creation_date は "YYYY/MM/DD HH:MM:SS" 形式のことがあるので 'YYYY-MM-DD' へ正規化する。

/**
 * 任意形式の日付文字列から先頭の YYYY-MM-DD を抽出して正規化する。失敗時は null。
 * 月末超過 (2026-02-30) や 13月 (2026-13-01) などの不正日付も null に倒す
 * （Date.UTC の繰り上げで別日付として保存されるとデータ汚染になるため）。
 */
export function normalizeNeCreationDate(raw) {
  if (!raw) return null;
  const m = String(raw).match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (!m) return null;
  const y = parseInt(m[1], 10);
  const mo = parseInt(m[2], 10);
  const d = parseInt(m[3], 10);
  if (!y || !mo || !d) return null;
  const utc = Date.UTC(y, mo - 1, d);
  if (!Number.isFinite(utc)) return null;
  const probe = new Date(utc);
  if (probe.getUTCFullYear() !== y || probe.getUTCMonth() !== mo - 1 || probe.getUTCDate() !== d) {
    return null;
  }
  const moStr = String(mo).padStart(2, '0');
  const dStr = String(d).padStart(2, '0');
  return `${y}-${moStr}-${dStr}`;
}

/**
 * launch_date を解決する。優先順位は:
 *   1. 既存 carryover が valid（normalize 通過）→ それを採用
 *   2. NE 作成日が valid → それを採用
 *   3. どちらも invalid/null → null
 *
 * carryoverValue を素通しせず必ず normalize するのは、
 * 過去の実装やマイグレで `'broken'` `'2026-13-01'` 等が入っていた場合に
 * 永久に NE 作成日へフォールバックできなくなるのを防ぐため (Codex R2 Medium 反映)。
 */
export function resolveLaunchDate(carryoverValue, neCreationDate) {
  const carryoverValid = normalizeNeCreationDate(carryoverValue);
  if (carryoverValid) return carryoverValid;
  return normalizeNeCreationDate(neCreationDate);
}

/**
 * Phase C: staging → 本番テーブル反映
 *
 * ★ 重要: 明示列INSERT必須（SELECT * にしてはいけない）
 * テスト test-profit-schema.mjs Test 5 が回帰検知する。
 */
export function applyStagingToProduction(db) {
  const mpList = colList(MP_COLS);
  const mscList = colList(MSC_COLS);

  const tx = db.transaction(() => {
    db.exec('DELETE FROM m_products');
    db.exec("DELETE FROM sqlite_sequence WHERE name='m_products'");
    db.exec(`INSERT INTO m_products (${mpList}) SELECT ${mpList} FROM m_products_staging`);

    db.exec('DELETE FROM m_set_components');
    db.exec(`INSERT INTO m_set_components (${mscList}) SELECT ${mscList} FROM m_set_components_staging`);
  });
  tx();
}

// ─── メイン ───

export async function rebuildMProducts() {
  const db = getDB();
  const ts = now();
  const log = [];
  const warn = [];

  console.log('[m_products] 再構築開始...');

  // ─── Phase A: staging 投入 ───

  // A-carryover: 商品収益性ダッシュボード用の手動付与カラム（seasonality_flag 等）を
  //   既存 m_products から引き継ぐ。rebuild で上書きされないようにするため。
  //   （Codex PR1 review High #1 反映 + Round 3 Medium: PRAGMA事前チェックで空catch回避）
  const CARRYOVER_COLS = ['seasonality_flag', 'season_months', 'new_product_flag', 'new_product_launch_date'];
  const carryoverMap = new Map();
  const mpCols = db.prepare('PRAGMA table_info(m_products)').all().map(c => c.name);
  const hasCarryoverCols = CARRYOVER_COLS.every(c => mpCols.includes(c));
  if (hasCarryoverCols) {
    const rows = db.prepare(`
      SELECT 商品コード, seasonality_flag, season_months,
             new_product_flag, new_product_launch_date
      FROM m_products
    `).all();
    for (const r of rows) {
      const code = r.商品コード?.toLowerCase();
      if (!code) continue;
      carryoverMap.set(code, {
        seasonality_flag: r.seasonality_flag ?? 0,
        season_months: r.season_months ?? null,
        new_product_flag: r.new_product_flag ?? 0,
        new_product_launch_date: r.new_product_launch_date ?? null,
      });
    }
  }
  // 新カラムが未マイグレの旧DBでは carryoverMap は空のまま。デフォルト値で進む。

  function getCarryover(code) {
    return carryoverMap.get(code) || {
      seasonality_flag: 0, season_months: null,
      new_product_flag: 0, new_product_launch_date: null,
    };
  }

  // A0: staging クリア
  db.exec('DELETE FROM m_products_staging');
  db.exec('DELETE FROM m_set_components_staging');
  // AUTOINCREMENT リセット
  try { db.exec("DELETE FROM sqlite_sequence WHERE name='m_products_staging'"); } catch {}

  // セット商品コード一覧（後で除外に使う）
  const setCodeSet = new Set(
    db.prepare('SELECT DISTINCT セット商品コード FROM raw_ne_set_products').all()
      .map(r => r.セット商品コード?.toLowerCase())
      .filter(Boolean)
  );

  // A1: NE単品商品を投入
  //     seasonality_flag / season_months / new_product_flag / new_product_launch_date は
  //     carryoverMap から引き継ぐ（Codex PR1 review High #1 反映）
  const insertStaging = db.prepare(`
    INSERT INTO m_products_staging (
      商品コード, 商品名, 商品区分, 取扱区分,
      標準売価, 原価, 原価ソース, 原価状態,
      送料, 送料コード, 配送方法,
      消費税率, 税区分,
      在庫数, 引当数, 仕入先コード, セット構成品数, 売上分類,
      seasonality_flag, season_months, new_product_flag, new_product_launch_date,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const neProducts = db.prepare('SELECT * FROM raw_ne_products').all();
  const exceptionMap = new Map();
  for (const eg of db.prepare('SELECT * FROM exception_genka').all()) {
    exceptionMap.set(eg.sku?.toLowerCase(), eg);
  }
  const shippingMap = new Map();
  for (const ps of db.prepare('SELECT * FROM product_shipping').all()) {
    shippingMap.set(ps.sku?.toLowerCase(), ps);
  }
  // 売上分類マップ
  const salesClassMap = new Map();
  for (const sc of db.prepare('SELECT * FROM product_sales_class').all()) {
    salesClassMap.set(sc.sku?.toLowerCase(), sc.sales_class);
  }
  // 手動税率マップ (NE税率が未登録の単品・例外商品の補完用、resolveTaxRate で参照)
  const taxRateMap = new Map();
  try {
    for (const tr of db.prepare('SELECT * FROM product_tax_rate').all()) {
      taxRateMap.set(tr.sku?.toLowerCase(), tr.tax_rate);
    }
  } catch {} // テーブル未作成時はスキップ

  // 送料取得ヘルパー: 自分のコード → 代表商品コード の順で検索
  function getShipping(code, repCode) {
    const ps = shippingMap.get(code);
    if (ps) return ps;
    if (repCode && repCode.toLowerCase() !== code) {
      return shippingMap.get(repCode.toLowerCase()) || null;
    }
    return null;
  }

  let countSingle = 0;
  let countSetAsNE = 0;
  let countShipInherited = 0; // 代表コードから送料継承した件数

  for (const p of neProducts) {
    const code = p.商品コード?.toLowerCase();
    if (!code) continue;

    // セット商品コードに該当する場合はStep A2で投入
    if (setCodeSet.has(code)) {
      countSetAsNE++;
      continue;
    }

    const eg = exceptionMap.get(code);
    const ps = getShipping(code, p.代表商品コード);
    if (ps && !shippingMap.get(code)) countShipInherited++;

    let genka = null, genkaSource = '不明', genkaStatus = 'MISSING';
    if (p.原価 > 0) {
      genka = p.原価;
      genkaSource = 'NE';
      genkaStatus = 'COMPLETE';
    } else if (eg) {
      genka = eg.genka;
      genkaSource = '例外';
      genkaStatus = 'OVERRIDDEN';
    }

    const { taxRate, taxCategory } = resolveTaxRate(p.消費税率, taxRateMap.get(code));

    const co = getCarryover(code);
    const launchDate = resolveLaunchDate(co.new_product_launch_date, p.作成日);
    insertStaging.run(
      code, p.商品名, '単品', p.取扱区分,
      p.売価, genka, genkaSource, genkaStatus,
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      taxRate, taxCategory,
      p.在庫数, p.引当数, p.仕入先コード, null, salesClassMap.get(code) ?? null,
      co.seasonality_flag, co.season_months, co.new_product_flag, launchDate,
      ts
    );
    countSingle++;
  }
  log.push(`単品: ${countSingle}件（NE兼セット除外: ${countSetAsNE}件、送料継承: ${countShipInherited}件）`);

  // A2: セット商品を投入
  const setHeaders = db.prepare(`
    SELECT セット商品コード, MAX(セット商品名) as セット商品名, MAX(セット販売価格) as セット販売価格
    FROM raw_ne_set_products GROUP BY セット商品コード
  `).all();

  const setComponentsQuery = db.prepare(`
    SELECT sp.商品コード, sp.数量, p.原価, p.消費税率, p.商品名,
           p.商品コード IS NOT NULL AS ne_exists
    FROM raw_ne_set_products sp
    LEFT JOIN raw_ne_products p ON sp.商品コード = p.商品コード COLLATE NOCASE
    WHERE sp.セット商品コード = ?
  `);

  const insertComponentStaging = db.prepare(`
    INSERT INTO m_set_components_staging (セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  let countSet = 0;
  let countSetSalesDerived = 0; // 売上分類を構成品から導出したセット件数
  for (const sh of setHeaders) {
    const setCode = sh.セット商品コード?.toLowerCase();
    if (!setCode) continue;

    const components = setComponentsQuery.all(sh.セット商品コード);
    const eg = exceptionMap.get(setCode);
    const neInfo = db.prepare('SELECT * FROM raw_ne_products WHERE 商品コード = ? COLLATE NOCASE').get(setCode);
    const ps = getShipping(setCode, neInfo?.代表商品コード);

    // 原価計算
    let totalGenka = 0;
    let hasAllGenka = true;
    let hasAnyGenka = false;
    // 税率は単品と同じ解決順 (NE値優先 → NE未登録(null/0)時のみ product_tax_rate) を
    // 構成品ごとに適用してから集約する
    const componentTaxInputs = [];
    // 売上分類は構成品の登録値 (product_sales_class) を集約する
    const componentSalesInputs = [];

    for (const comp of components) {
      const compCode = comp.商品コード?.toLowerCase() || '';
      if (comp.原価 > 0) {
        totalGenka += comp.原価 * (comp.数量 || 1);
        hasAnyGenka = true;
      } else {
        hasAllGenka = false;
      }
      componentTaxInputs.push({
        neTaxRate: comp.消費税率,
        manualTaxRate: taxRateMap.get(compCode),
        componentExists: !!comp.ne_exists,
      });
      componentSalesInputs.push({
        salesClass: salesClassMap.get(compCode),
        componentExists: !!comp.ne_exists,
      });

      // 構成品staging投入
      insertComponentStaging.run(
        setCode, compCode, comp.数量 || 1,
        comp.商品名 || '', comp.原価 || null, ts
      );
    }

    let genka = null, genkaSource = '不明', genkaStatus = 'MISSING';
    if (eg) {
      genka = eg.genka;
      genkaSource = '例外';
      genkaStatus = 'OVERRIDDEN';
    } else if (hasAllGenka && components.length > 0) {
      genka = Math.round(totalGenka * 100) / 100;
      genkaSource = 'セット計算';
      genkaStatus = 'COMPLETE';
    } else if (hasAnyGenka) {
      genkaStatus = 'PARTIAL';
    }

    // 税区分 (構成品の税率から導出。MIXED は taxRate に最小値を入れる既存仕様を踏襲)
    // 構成品が1つでも解決できなければ UNKNOWN に倒し、上流異常を握り潰さない
    const { taxRate, taxCategory } = resolveSetTaxRate(componentTaxInputs);

    // 売上分類 (手動登録が最優先。無ければ構成品の MIN から導出)
    //   導出も効かない (構成品が未登録 / NE に無い) セットだけが NULL で残り、
    //   register の「分類未登録」に出る。
    const setSalesClass = salesClassMap.get(setCode) ?? resolveSetSalesClass(componentSalesInputs);
    if (salesClassMap.get(setCode) == null && setSalesClass != null) countSetSalesDerived++;

    // 取扱区分: NEに存在すればそこから、なければ取扱中
    const status = neInfo?.取扱区分 || '取扱中';

    const coSet = getCarryover(setCode);
    const setLaunchDate = resolveLaunchDate(coSet.new_product_launch_date, neInfo?.作成日);
    insertStaging.run(
      setCode, sh.セット商品名, 'セット', status,
      neInfo?.売価 ?? sh.セット販売価格 ?? null,
      genka, genkaSource, genkaStatus,
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      taxRate, taxCategory,
      neInfo?.在庫数 ?? null, neInfo?.引当数 ?? null, neInfo?.仕入先コード ?? null,
      components.length, setSalesClass,
      coSet.seasonality_flag, coSet.season_months, coSet.new_product_flag, setLaunchDate,
      ts
    );
    countSet++;
  }
  log.push(`セット: ${countSet}件（売上分類を構成品から導出: ${countSetSalesDerived}件）`);

  // A3: 例外商品（NE・セットに無いもののみ）
  let countException = 0;
  for (const [sku, eg] of exceptionMap) {
    // 既にstagingに入っているか確認
    const exists = db.prepare('SELECT 1 FROM m_products_staging WHERE 商品コード = ?').get(sku);
    if (exists) continue;

    const ps = shippingMap.get(sku);

    // 例外商品は NE に存在しないので neTaxNum=null。手動登録のみが税率ソース
    const { taxRate: exTaxRate, taxCategory: exTaxCategory } = resolveTaxRate(null, taxRateMap.get(sku));

    const coEx = getCarryover(sku);
    // 例外商品も resolveLaunchDate を通すことで、carryover に既存の不正値
    // ('broken' / '2026-13-01' 等) が入っていても staging に汚染が伝播しない。
    // NE 商品ではないので NE 作成日 のフォールバックはなく、carryover のみを正規化する。
    const exLaunchDate = resolveLaunchDate(coEx.new_product_launch_date, null);
    insertStaging.run(
      sku, eg.商品名 || '', '例外', '取扱中',
      null, eg.genka, '例外', 'OVERRIDDEN',
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      exTaxRate, exTaxCategory,
      null, null, null, null, salesClassMap.get(sku) ?? null,
      coEx.seasonality_flag, coEx.season_months, coEx.new_product_flag, exLaunchDate,
      ts
    );
    countException++;
  }
  log.push(`例外: ${countException}件`);

  // ─── Phase B: 品質チェック ───

  const checks = [];
  let fatal = false;

  // B1: 総件数
  const totalStaging = db.prepare('SELECT COUNT(*) as cnt FROM m_products_staging').get().cnt;
  checks.push(`総件数: ${totalStaging}`);
  if (totalStaging < 3000) {
    checks.push('❌ 総件数が3,000件未満 → 反映中止');
    fatal = true;
  }

  // B2: 商品区分別件数
  const typeCounts = db.prepare('SELECT 商品区分, COUNT(*) as cnt FROM m_products_staging GROUP BY 商品区分').all();
  for (const tc of typeCounts) checks.push(`  ${tc.商品区分}: ${tc.cnt}件`);

  // B3: 前回比
  const prevTotal = db.prepare('SELECT COUNT(*) as cnt FROM m_products').get().cnt;
  if (prevTotal > 0) {
    const ratio = totalStaging / prevTotal;
    if (ratio < 0.7 || ratio > 1.3) {
      checks.push(`⚠️ 前回比 ${Math.round(ratio * 100)}% (前回${prevTotal}件)`);
      warn.push(`前回比が±30%を超えています`);
    } else {
      checks.push(`前回比: ${Math.round(ratio * 100)}% (前回${prevTotal}件)`);
    }
  } else {
    checks.push('初回投入（前回データなし）');
  }

  // B4: 商品コード重複・NULL
  const nullCodes = db.prepare('SELECT COUNT(*) as cnt FROM m_products_staging WHERE 商品コード IS NULL').get().cnt;
  if (nullCodes > 0) { checks.push(`❌ 商品コードNULL: ${nullCodes}件`); fatal = true; }

  // B5: 原価状態NULL
  const nullStatus = db.prepare('SELECT COUNT(*) as cnt FROM m_products_staging WHERE 原価状態 IS NULL').get().cnt;
  if (nullStatus > 0) { checks.push(`❌ 原価状態NULL: ${nullStatus}件`); fatal = true; }

  // B6: 原価状態と原価値の整合
  const costMismatch1 = db.prepare("SELECT COUNT(*) as cnt FROM m_products_staging WHERE 原価状態 IN ('COMPLETE','OVERRIDDEN') AND 原価 IS NULL").get().cnt;
  if (costMismatch1 > 0) { checks.push(`❌ 原価状態COMPLETE/OVERRIDDENなのに原価NULL: ${costMismatch1}件`); fatal = true; }
  const costMismatch2 = db.prepare("SELECT COUNT(*) as cnt FROM m_products_staging WHERE 原価状態 IN ('MISSING','PARTIAL') AND 原価 IS NOT NULL").get().cnt;
  if (costMismatch2 > 0) { checks.push(`⚠️ 原価状態MISSING/PARTIALなのに原価あり: ${costMismatch2}件`); warn.push('原価状態不整合あり'); }

  // B7: セット構成品数の整合
  const setNoComp = db.prepare("SELECT COUNT(*) as cnt FROM m_products_staging WHERE 商品区分 = 'セット' AND (セット構成品数 IS NULL OR セット構成品数 = 0)").get().cnt;
  if (setNoComp > 0) { checks.push(`⚠️ セットなのに構成品数0/NULL: ${setNoComp}件`); warn.push('セット構成品数不整合'); }
  const nonSetWithComp = db.prepare("SELECT COUNT(*) as cnt FROM m_products_staging WHERE 商品区分 != 'セット' AND セット構成品数 IS NOT NULL").get().cnt;
  if (nonSetWithComp > 0) { checks.push(`⚠️ セット以外なのに構成品数あり: ${nonSetWithComp}件`); warn.push('非セットに構成品数'); }

  // B7b: ネストセット (構成品がそれ自体セット) の検知
  //   売上分類の導出は構成品の手動登録値しか見ないので、ネストが発生すると
  //   親セットは導出できず NULL (= 未登録一覧行き) になる。誤った値は入らないが、
  //   件数が増え続けたら再帰導出を実装する判断材料になるので可視化しておく。
  const nestedSets = db.prepare(`
    SELECT COUNT(DISTINCT c.セット商品コード) as cnt
    FROM m_set_components_staging c
    JOIN m_products_staging p ON p.商品コード = c.構成商品コード COLLATE NOCASE
    WHERE p.商品区分 = 'セット'
  `).get().cnt;
  if (nestedSets > 0) {
    checks.push(`⚠️ ネストセット（構成品がセット）: ${nestedSets}件 → 売上分類は導出されず未登録一覧に出ます`);
    warn.push('ネストセットあり（売上分類の導出対象外）');
  }

  // B8: m_set_components_staging の孤児チェック
  const orphanParent = db.prepare(`
    SELECT COUNT(DISTINCT セット商品コード) as cnt FROM m_set_components_staging
    WHERE セット商品コード NOT IN (SELECT 商品コード FROM m_products_staging)
  `).get().cnt;
  if (orphanParent > 0) { checks.push(`⚠️ 構成品の親がm_productsに無い: ${orphanParent}件`); warn.push('構成品孤児'); }

  // B9: 税区分と消費税率の整合
  const taxMismatch = db.prepare(`
    SELECT COUNT(*) as cnt FROM m_products_staging
    WHERE (税区分 = 'STANDARD_10' AND 消費税率 != 0.1)
       OR (税区分 = 'REDUCED_8' AND 消費税率 != 0.08)
  `).get().cnt;
  if (taxMismatch > 0) { checks.push(`⚠️ 税区分と消費税率不整合: ${taxMismatch}件`); warn.push('税区分不整合'); }

  // 品質チェックログ出力
  console.log('[m_products] 品質チェック:');
  for (const c of checks) console.log('  ' + c);

  if (fatal) {
    console.error('[m_products] ❌ 致命的エラーのため反映中止');
    return { ok: false, log, checks, warn, total: totalStaging };
  }

  // ─── Phase C: 本番反映 ───

  // 本番反映（明示列INSERT、列順破壊耐性あり）
  applyStagingToProduction(db);

  // WAL肥大化防止
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  const finalCount = db.prepare('SELECT COUNT(*) as cnt FROM m_products').get().cnt;
  const compCount = db.prepare('SELECT COUNT(*) as cnt FROM m_set_components').get().cnt;
  console.log(`[m_products] ✅ 反映完了: ${finalCount}件 (構成品: ${compCount}件)`);

  log.push(`反映完了: ${finalCount}件 (構成品: ${compCount}件)`);

  return { ok: true, log, checks, warn, total: finalCount, components: compCount };
}

// ─── 単体実行 ───

import { initDB } from './db.js';
import { pathToFileURL } from 'url';

// 部分一致だと「ファイル名に rebuild-m-products を含む別スクリプト」から import した
// だけでバッチ本体が走ってしまうため、エントリポイントの URL 完全一致で判定する
const isMain = !!process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  await initDB();
  const result = await rebuildMProducts();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(result.ok ? 0 : 1);
}
