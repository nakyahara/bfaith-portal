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

// ─── メイン ───

export async function rebuildMProducts() {
  const db = getDB();
  const ts = now();
  const log = [];
  const warn = [];

  console.log('[m_products] 再構築開始...');

  // ─── Phase A: staging 投入 ───

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
  const insertStaging = db.prepare(`
    INSERT INTO m_products_staging (
      商品コード, 商品名, 商品区分, 取扱区分,
      標準売価, 原価, 原価ソース, 原価状態,
      送料, 送料コード, 配送方法,
      消費税率, 税区分,
      在庫数, 引当数, 仕入先コード, セット構成品数, 売上分類, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
  // 手動税率マップ（例外商品等、NE税率がない場合用）
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

    const taxRate = p.消費税率 ? p.消費税率 / 100.0 : null;
    let taxCategory = 'UNKNOWN';
    if (p.消費税率 === 10) taxCategory = 'STANDARD_10';
    else if (p.消費税率 === 8) taxCategory = 'REDUCED_8';

    insertStaging.run(
      code, p.商品名, '単品', p.取扱区分,
      p.売価, genka, genkaSource, genkaStatus,
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      taxRate, taxCategory,
      p.在庫数, p.引当数, p.仕入先コード, null, salesClassMap.get(code) ?? null, ts
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
    SELECT sp.商品コード, sp.数量, p.原価, p.消費税率, p.商品名
    FROM raw_ne_set_products sp
    LEFT JOIN raw_ne_products p ON sp.商品コード = p.商品コード COLLATE NOCASE
    WHERE sp.セット商品コード = ?
  `);

  const insertComponentStaging = db.prepare(`
    INSERT INTO m_set_components_staging (セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  let countSet = 0;
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
    const taxRates = new Set();

    for (const comp of components) {
      if (comp.原価 > 0) {
        totalGenka += comp.原価 * (comp.数量 || 1);
        hasAnyGenka = true;
      } else {
        hasAllGenka = false;
      }
      if (comp.消費税率) taxRates.add(comp.消費税率);

      // 構成品staging投入
      insertComponentStaging.run(
        setCode, comp.商品コード?.toLowerCase() || '', comp.数量 || 1,
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

    // 税区分
    let taxCategory = 'UNKNOWN', taxRate = null;
    if (taxRates.size === 1) {
      const rate = [...taxRates][0];
      taxRate = rate / 100.0;
      taxCategory = rate === 10 ? 'STANDARD_10' : rate === 8 ? 'REDUCED_8' : 'UNKNOWN';
    } else if (taxRates.size > 1) {
      taxCategory = 'MIXED';
      taxRate = Math.min(...taxRates) / 100.0;
    }

    // 取扱区分: NEに存在すればそこから、なければ取扱中
    const status = neInfo?.取扱区分 || '取扱中';

    insertStaging.run(
      setCode, sh.セット商品名, 'セット', status,
      neInfo?.売価 ?? sh.セット販売価格 ?? null,
      genka, genkaSource, genkaStatus,
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      taxRate, taxCategory,
      neInfo?.在庫数 ?? null, neInfo?.引当数 ?? null, neInfo?.仕入先コード ?? null,
      components.length, salesClassMap.get(setCode) ?? null, ts
    );
    countSet++;
  }
  log.push(`セット: ${countSet}件`);

  // A3: 例外商品（NE・セットに無いもののみ）
  let countException = 0;
  for (const [sku, eg] of exceptionMap) {
    // 既にstagingに入っているか確認
    const exists = db.prepare('SELECT 1 FROM m_products_staging WHERE 商品コード = ?').get(sku);
    if (exists) continue;

    const ps = shippingMap.get(sku);

    // 手動登録税率を参照
    const manualTaxRate = taxRateMap.get(sku) ?? null;
    let exTaxRate = null, exTaxCategory = 'UNKNOWN';
    if (manualTaxRate !== null) {
      exTaxRate = manualTaxRate;
      if (manualTaxRate === 0.1) exTaxCategory = 'STANDARD_10';
      else if (manualTaxRate === 0.08) exTaxCategory = 'REDUCED_8';
    }

    insertStaging.run(
      sku, eg.商品名 || '', '例外', '取扱中',
      null, eg.genka, '例外', 'OVERRIDDEN',
      ps?.ship_cost ?? null, ps?.shipping_code ?? null, ps?.ship_method ?? null,
      exTaxRate, exTaxCategory,
      null, null, null, null, salesClassMap.get(sku) ?? null, ts
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

  const tx = db.transaction(() => {
    db.exec('DELETE FROM m_products');
    db.exec("DELETE FROM sqlite_sequence WHERE name='m_products'");
    db.exec('INSERT INTO m_products SELECT * FROM m_products_staging');

    db.exec('DELETE FROM m_set_components');
    db.exec('INSERT INTO m_set_components SELECT * FROM m_set_components_staging');
  });
  tx();

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

const isMain = !process.argv[1] || process.argv[1].includes('rebuild-m-products');
if (isMain && process.argv[1]?.includes('rebuild-m-products')) {
  await initDB();
  const result = await rebuildMProducts();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(result.ok ? 0 : 1);
}
