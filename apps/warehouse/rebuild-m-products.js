/**
 * m_products 統合商品マスタ 再構築スクリプト
 *
 * staging テーブルに投入 → 品質チェック → 本番反映
 * daily-sync.js から呼び出す or 単体実行可能
 */
import { getDB } from './db.js';
import { readNeMarks, judgeNeMark, stagingHash, recordBuild, makeBuildId, acquireRebuildLock, holdsRebuildLock, releaseRebuildLock, ensurePrivateStaging, stagingIsPrivate, latestBuild, publishOfBuild, MASTER_BUILD_RULE_VERSION } from './master-material.js';
import {
  TAX_RATES, KNOWN_NE_RATES, KNOWN_DECIMAL_RATES, resolveTaxRate, resolveSetTaxRate,
  SALES_CLASSES, EXPORT_SALES_CLASS, resolveSetSalesClass,
  HANDLING_ACTIVE, HANDLING_STOPPED, HANDLING_MAKER_STOPPED, resolveSetHandlingClass,
  setCostFromComponents, deriveSetValues,
} from '../../lib/master-set-rules.js';
import crypto from 'node:crypto';
import { normSku } from '../../lib/sku-norm.js';
import { ALL_LOAD } from '../company-db/load/ownership-state.mjs';
import { readCurrentPublish, makePublishResolver, mergeReasons, kindReason, handlingFromCdb, publishCols, applySideTables, verifyApplied, ownershipHash, writeSetPublishExpect, writeKindFrozen } from './master-publish.js';

// ─── ヘルパー ───

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// 税率・売上分類・取扱区分・原価のセットの決め方は lib/master-set-rules.js に移した (マスタ入力画面 apps/master-edit と同じ規則を共用する。Company DB構想 14 §6 ⑤-1)。
// 今までどおりここからも import できるように、同じ名前で export し直す
export {
  deriveSetValues,
  TAX_RATES, KNOWN_NE_RATES, KNOWN_DECIMAL_RATES, resolveTaxRate, resolveSetTaxRate,
  SALES_CLASSES, EXPORT_SALES_CLASS, resolveSetSalesClass,
  HANDLING_ACTIVE, HANDLING_STOPPED, HANDLING_MAKER_STOPPED, resolveSetHandlingClass,
};

/** Company DB の税率 (小数) → NE の整数 (構成品の入力を C の値に替えるとき。resolveTaxRate にそのまま渡せる形) */
export const neRateOfDecimal = (decimal) => TAX_RATES.find((t) => t.decimal === decimal)?.neRate ?? null;


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

/** 表の中身のハッシュ (updated_at を除く・行の順に依らない)。作り直しの「中身が同じ」を見る */
function contentHashOf(db, table, cols, order) {
  const h = crypto.createHash('sha256');
  for (const r of db.prepare(`SELECT ${colList(cols.filter((c) => c !== 'updated_at'))} FROM ${table} ORDER BY ${order}`).raw().iterate()) h.update(`${JSON.stringify(r)}\n`);
  return h.digest('hex');
}

/**
 * 前の作り直しから何も変わっていないか (#1564 Codex R2 Medium 5)。全部そろったときだけ skip = 入れ替えない・記録も足さない (業務の表は 1 行も書かない):
 *   前の作り直しの記録がある・規則の版が同じ / NE の取得の印 (時刻・番号) が信用でき、前の作り直しと同じ /
 *   写しの世代 (番号・持ち主) が前の作り直しと同じ / 作った中身 (updated_at を除く) が今の m_products・m_set_components と同じ /
 *   持ち主が C の列があれば、今の古い表・上書き表が世代のまま (入れた後の確かめが通り、ハッシュが前の作り直しの記録と同じ)
 * 🚨 毎朝の daily-sync は NE の取得が新しい (印が変わる) = 今までどおり入れ替える (updated_at も新しくなる)。何もしないのは同じ取得・同じ世代のやり直しだけ
 */
function unchangedSinceLastBuild(db, { startMarks, pub, ownership }) {
  let lb = null;
  try { lb = latestBuild(db); } catch { lb = null; }
  if (!lb) return { skip: false, why: 'no_build' };
  if (lb.rule_version !== MASTER_BUILD_RULE_VERSION) return { skip: false, why: 'rule_version' };
  const now = readNeMarks(db);
  const mp = judgeNeMark(startMarks.products, now.products), ms = judgeNeMark(startMarks.set_components, now.set_components);
  if (!mp.value || !ms.value) return { skip: false, why: 'ne_mark_untrusted' };
  if (mp.value !== lb.ne_products_complete_at || String(mp.rev) !== String(lb.ne_products_complete_rev)
    || ms.value !== lb.ne_setproducts_complete_at || String(ms.rev) !== String(lb.ne_setproducts_complete_rev)) return { skip: false, why: 'ne_changed' };
  const genNo = pub.generation ? pub.generation.generation_no : null, genOwner = pub.generation ? pub.generation.ownership_hash : null;
  if ((lb.cdb_publish_generation_no ?? null) !== genNo || (lb.cdb_publish_ownership_hash ?? null) !== genOwner) return { skip: false, why: 'generation_changed' };
  if (contentHashOf(db, 'm_products_staging', MP_COLS, '"商品コード"') !== contentHashOf(db, 'main.m_products', MP_COLS, '"商品コード"')) return { skip: false, why: 'products_changed' };
  if (contentHashOf(db, 'm_set_components_staging', MSC_COLS, '"セット商品コード", "構成商品コード"') !== contentHashOf(db, 'main.m_set_components', MSC_COLS, '"セット商品コード", "構成商品コード"')) {
    return { skip: false, why: 'components_changed' };
  }
  let applied = null;
  if (pub.active) {
    applied = verifyApplied(db, { publication: pub.publication, ownership, taxRates: TAX_RATES });
    if (!applied.ok || applied.applied_hash !== (lb.cdb_publish_applied_hash ?? null)) return { skip: false, why: 'applied_changed' };
  }
  return { skip: true, why: 'unchanged', build: lb, applied };
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
export function applyStagingToProduction(db, { build = null } = {}) {
  const mpList = colList(MP_COLS);
  const mscList = colList(MSC_COLS);

  const tx = db.transaction(() => {
    // 作り直しの記録を付ける回 (rebuildMProducts): 作り直しの札がまだ自分のものか (別の作り直しを入れていない) と、
    //   staging が品質チェックの前と同じかを確かめる (Codex PR #1453 R1 High-1)
    if (build && !stagingIsPrivate(db)) {
      throw Object.assign(new Error('作業用の表がこの接続の TEMP 表ではない (共有の表から入れ替えない)'), { code: 'STAGING_NOT_PRIVATE' });
    }
    if (build && !holdsRebuildLock(db, build.buildId)) {
      throw Object.assign(new Error('作り直しの札が自分のものでない (期限切れで別の作り直しに取られた?) → 入れ替えない'), { code: 'LOCK_LOST' });
    }
    if (build && stagingHash(db) !== build.expectedStagingHash) {
      throw Object.assign(new Error('作業用の表 (staging) が作った後に変わった (別の作り直しが同時に走った?) → 入れ替えない'), { code: 'STAGING_CHANGED' });
    }
    db.exec('DELETE FROM m_products');
    db.exec("DELETE FROM main.sqlite_sequence WHERE name='m_products'");   // main を明示 (staging が TEMP 表のとき、名前だけだと temp.sqlite_sequence を見る)
    db.exec(`INSERT INTO m_products (${mpList}) SELECT ${mpList} FROM m_products_staging`);

    db.exec('DELETE FROM m_set_components');
    db.exec(`INSERT INTO m_set_components (${mscList}) SELECT ${mscList} FROM m_set_components_staging`);
    // 上書き表を持ち主が C の列の値にそろえ、入れた後に読み直して世代と比べる (④a。持ち主が全部 load なら何もしない・読むだけ。master-publish.js)。
    //   違えば投げる = 入れ替えも巻き戻る (C の値が入っていない m_products を残さない)
    // 同じ NE の取得・同じ世代・同じ中身の作り直しは、ここまで来ない (rebuildMProductsLocked の unchangedSinceLastBuild = 何も書かない。#1564 Codex R2 Medium 5)。
    //   ここに来た作り直し (NE の取得が新しい・世代が新しい・中身が違う) は今までどおり全部入れ替える。上書き表は値が同じ行は触らない
    const pub = build ? build.publish : null;
    // C にあるセットの導き方の入力・構成品の行 (#1564 Codex R7 High)。m_products と同じ取引で入れ替える = 入れた後の確かめ (今・次の工程) が同じ決め方で導き直して比べる
    if (build) writeSetPublishExpect(db, build.setExpect || []);
    // 区分の持ち主が C: 前の行のまま にした SKU (C = セット・NE = 単品) の印。持ち主が load の今は書かない (表に触らない)
    if (build && pub && pub.owns('kind')) writeKindFrozen(db, build.kindFrozen || []);
    const side = pub ? applySideTables(db, pub) : null;
    const applied = pub ? verifyApplied(db, { publication: pub.publication, ownership: pub.ownership, taxRates: TAX_RATES, expected: pub.active ? pub.expected : null }) : null;
    if (applied && !applied.ok) {
      throw Object.assign(new Error(`Company DB の値が m_products に入っていない ${JSON.stringify(applied.problems.slice(0, 3))}`), { code: 'CDB_PUBLISH_VERIFY', applied });
    }
    // 作り直しの記録 (master-material.js)。入れ替えと同じ取引 = 記録に失敗すれば入れ替えも巻き戻る
    return build ? { ...recordBuild(db, { buildId: build.buildId, startMarks: build.startMarks, startedAt: build.startedAt, reasons: build.reasons,
      publish: pub && pub.generation ? { ...pub.generation, applied_hash: applied.applied_hash } : null }), side_tables: side, applied } : null;
  });
  return tx();
}

// ─── メイン ───

/**
 * @param {object} [opts]
 * @param {object} [opts.ownership]  列ごとの持ち主。試験だけ渡す (渡した持ち主と世代の持ち主が違えば止める)。
 *   渡さない (本番) = 今の写しの世代の持ち主 (epoch。Company DB の active か、切替の日の prepared。Codex #1564 R1 H1)。config は使わない
 */
export async function rebuildMProducts({ ownership = null } = {}) {
  const db = getDB();
  // 作り直しの札 (作り始めから入れ替えまで、別の作り直しを入れない = staging は共有の表。master-material.js。Codex PR #1453 R1 High-1)
  const buildId = makeBuildId();
  if (!acquireRebuildLock(db, buildId)) {
    console.error('[m_products] ❌ 別の作り直しが実行中 (作り直しの札がある) → 何もしない');
    return { ok: false, error: 'REBUILD_LOCKED', log: [], checks: ['❌ 別の作り直しが実行中'], warn: [] };
  }
  try {
    return await rebuildMProductsLocked(db, buildId, ownership);
  } finally {
    releaseRebuildLock(db, buildId);
  }
}

async function rebuildMProductsLocked(db, buildId, ownership) {
  const ts = now();
  const log = [];
  const warn = [];

  console.log('[m_products] 再構築開始...');
  // 作り始めに NE の印と通し番号を読む (入れ替えの取引の中でもう一度読み、印の後に書かれた・途中で変わった なら由来を信用しない。master-material.js)
  const startMarks = readNeMarks(db);
  const startedAt = new Date().toISOString();
  // 作業用の表はこの接続だけの TEMP 表 (プロセスごとに別の作業場。master-material.js。Codex PR #1453 R2 High)
  ensurePrivateStaging(db);
  // SKU・列ごとの採用理由。値を決めたその場で集める (後から raw を読み直して推定しない。照合 ② の原因の証拠。Codex PR #1453 R1 Medium-3)
  const reasons = [];
  const neRateKnown = (v) => TAX_RATES.some((x) => x.neRate === v);

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
  try { db.exec("DELETE FROM temp.sqlite_sequence WHERE name='m_products_staging'"); } catch {}   // 作業用の表は TEMP 表 (ensurePrivateStaging)

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

  // Company DB の写し (マスタ正本切替 ④a。master-publish.js): 持ち主が C の列だけ、今の世代の値を重ねる (同じ取引・記録の前)。
  //   🚨 持ち主が全部 load なら何もしない (値も理由も今までと同じ・何も書かない)。
  //   持ち主が C の列があるのに、同じ持ち主 (epoch) の世代が無い・値が欠ける・m_products の 2 つのコードが同じ C の SKU に当たる = 入れ替えない (前の m_products のまま)
  // 持ち主 (epoch) を決める: 試験で渡した持ち主 / 今の世代の持ち主 / 世代が無い = 全部 load。
  //   ただし前の作り直しが持ち主が C の列を使った (記録の持ち主のハッシュが全部 load でない) のに世代が無い = 止める
  //   (warehouse.db を戻した・印が消えた = C の値を NE の値に黙って戻さない)。config (configured) では決めない (Codex #1564 R1 H1)
  const head = readCurrentPublish(db, { values: false });
  let epochNote = ownership ? 'explicit' : null;
  if (!ownership) {
    if (head.generation && head.generation.state === 'verified') {
      try { ownership = JSON.parse(head.generation.ownership); epochNote = `generation ${head.generation.generation_no}`; }
      catch { ownership = null; }
      if (!ownership) {
        const msg = `❌ 写しの世代 ${head.generation.generation_no} の持ち主が読めない → 反映中止 (前の m_products のまま)`;
        console.error(`[m_products] ${msg}`);
        return { ok: false, error: 'CDB_PUBLISH_UNAVAILABLE', problem: 'generation_ownership_unreadable', log, checks: [msg], warn, total: 0 };
      }
    } else if ((() => { const lb = publishOfBuild(latestBuild(db)); return !!(lb && lb.ownership_hash && lb.ownership_hash !== ownershipHash(ALL_LOAD)); })()) {
      const msg = `❌ 前の作り直しは持ち主が Company DB の列を使ったのに、写しの世代が無い (${head.problem}) → 反映中止 (前の m_products のまま。C の値を NE に戻さない)`;
      console.error(`[m_products] ${msg}`);
      return { ok: false, error: 'CDB_PUBLISH_UNAVAILABLE', problem: 'no_generation', log, checks: [msg], warn, total: 0 };
    } else { ownership = ALL_LOAD; epochNote = `default (${head.problem ?? 'no_generation'})`; }
  }
  const needPublish = publishCols(ownership).length > 0;
  const staged = new Map();   // m_products に入れるコード → 商品区分 (A1〜A3 と同じ決め方)
  for (const p of neProducts) { const c = p.商品コード?.toLowerCase(); if (c && !setCodeSet.has(c)) staged.set(c, '単品'); }
  for (const c of setCodeSet) staged.set(c, 'セット');
  for (const c of exceptionMap.keys()) if (c && !staged.has(c)) staged.set(c, '例外');
  const pub = makePublishResolver({ ownership, publication: readCurrentPublish(db, { values: needPublish }), staged, taxRates: TAX_RATES });
  if (pub.problem) {
    const msg = `❌ 持ち主が Company DB の列 (${pub.cols.join('・')}) があるのに、使える写しが無い (${pub.problem}${pub.problemDetail ? ` ${JSON.stringify(pub.problemDetail).slice(0, 300)}` : ''}) → 反映中止 (前の m_products のまま)`;
    console.error(`[m_products] ${msg}`);
    return { ok: false, error: 'CDB_PUBLISH_UNAVAILABLE', problem: pub.problem, log, checks: [msg], warn, total: 0 };
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
    const skuReasons = [];   // この SKU の理由 (今までと同じ順。C の値で変わった列だけ後で差し替える)
    if (genkaSource === '例外') skuReasons.push({ code, kind: '単品', col: 'cost', reason: 'exception_cost', value: genka, ne_value: p.原価 ?? null });
    if (!neRateKnown(p.消費税率)) {
      skuReasons.push(taxRate != null
        ? { code, kind: '単品', col: 'tax_rate', reason: 'tax_fallback', value: taxRate, source: 'product_tax_rate', ne_value: p.消費税率 ?? null }
        : { code, kind: '単品', col: 'tax_rate', reason: 'tax_unresolved', ne_value: p.消費税率 ?? null });
    }
    if (startMarks.products.at && p.synced_at !== startMarks.products.at) skuReasons.push({ code, kind: '単品', col: '*', reason: 'not_in_latest_fetch', raw_synced_at: p.synced_at ?? null });

    // 今までの決め方の値 (NE・上書き表)。持ち主が C の列だけ Company DB の値に替える (④a。C が無ければ同じもの)
    const neV = {
      name: p.商品名, handling: p.取扱区分, price: p.売価, genka, genkaSource, genkaStatus,
      shipCost: ps?.ship_cost ?? null, shipCode: ps?.shipping_code ?? null, shipMethod: ps?.ship_method ?? null,
      taxRate, taxCategory, supplier: p.仕入先コード, salesClass: salesClassMap.get(code) ?? null,
    };
    // 商品区分 (区分の持ち主が C = C の区分。NE の単品を C がセット・例外とする SKU も C の区分で・値は C の値をそのまま。持ち主が load の今は '単品')
    const mk = pub.kindOf(code, '単品');
    const v = pub.overlay(code, neV);
    pub.expect(code, neV);
    reasons.push(...(v === neV ? skuReasons : mergeReasons(code, mk, neV, v, skuReasons, { cells: pub.peek(code)?.v, generationNo: pub.generation?.generation_no })));
    if (mk !== '単品') reasons.push(kindReason(code, '単品', mk, { cdbKind: pub.peek(code)?.kind, generationNo: pub.generation?.generation_no }));

    const co = getCarryover(code);
    const launchDate = resolveLaunchDate(co.new_product_launch_date, p.作成日);
    insertStaging.run(
      code, v.name, mk, v.handling,
      v.price, v.genka, v.genkaSource, v.genkaStatus,
      v.shipCost, v.shipCode, v.shipMethod,
      v.taxRate, v.taxCategory,
      p.在庫数, p.引当数, v.supplier, null, v.salesClass,
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
    SELECT sp.商品コード, sp.数量, p.原価, p.消費税率, p.商品名, p.取扱区分,
           p.商品コード IS NOT NULL AS ne_exists
    FROM raw_ne_set_products sp
    LEFT JOIN raw_ne_products p ON sp.商品コード = p.商品コード COLLATE NOCASE
    WHERE sp.セット商品コード = ?
  `);

  const insertComponentStaging = db.prepare(`
    INSERT INTO m_set_components_staging (セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  const setExpect = [];   // C にあるセットの導き方の入力・構成品の行 (#1564 Codex R7 High)
  let countSet = 0;
  let countSetSalesDerived = 0; // 売上分類を構成品から導出したセット件数
  let countSetHandlingDerived = 0; // 取扱区分を構成品から引き継いだセット件数
  const setHandlingSamples = [];   // ログに出す例 (どのセットが止まったかを後から追えるように)
  for (const sh of setHeaders) {
    const setCode = sh.セット商品コード?.toLowerCase();
    if (!setCode) continue;

    const components = setComponentsQuery.all(sh.セット商品コード);
    const eg = exceptionMap.get(setCode);
    // 商品区分 (区分の持ち主が C = C の区分)。NE のセットを C が単品・例外とする SKU = 構成の行を作らず (構成品数も空)・構成品から導かず C の値をそのまま
    const mk = pub.kindOf(setCode, 'セット');
    const asSet = mk === 'セット';
    const neInfo = db.prepare('SELECT * FROM raw_ne_products WHERE 商品コード = ? COLLATE NOCASE').get(setCode);
    const ps = getShipping(setCode, neInfo?.代表商品コード);

    // 構成品の入力 (今までの決め方)。税率は単品と同じ解決順 (NE値優先 → NE未登録(null/0)時のみ product_tax_rate) を
    // 構成品ごとに適用してから集約する / 売上分類は構成品の登録値 (product_sales_class) を集約する /
    // 取扱区分は構成品の NE の値を集約する (構成品が止まればセットも止まる)
    const neInputs = [];
    // セット自身が Company DB にあるか。無い (NE にしか無いセット = 切替の後に NE にだけ足された) = セットは全部 NE の道で作る
    //   (導いた値も構成品の名前・原価も NE の値。構成品が C にあっても C の値を使わない = 確かめも NE の値を待つ。Codex #1564 R1 H2)
    const selfC = pub.active ? pub.peek(setCode) : null;
    // Company DB の値で導き直すときの構成品の入力 (④a。セット自身が C にあるときだけ・持ち主が C の列だけ替える。NE に無い構成品は替えない = 今までどおり上流の異常として扱う)
    const cInputs = [];
    const expectComps = [];   // C にあるセットの構成品の行 (入れた後の確かめが m_set_components と比べる。#1564 Codex R7 High)
    for (const comp of components) {
      const compCode = comp.商品コード?.toLowerCase() || '';
      const exists = !!comp.ne_exists;
      const ne = {
        cost: comp.原価, qty: comp.数量,
        tax: { neTaxRate: comp.消費税率, manualTaxRate: taxRateMap.get(compCode), componentExists: exists },
        sales: { salesClass: salesClassMap.get(compCode), componentExists: exists },
        handling: { handlingClass: comp.取扱区分, componentExists: exists },
      };
      neInputs.push(ne);
      if (!asSet) continue;   // C の区分がセットでない = 構成の行を作らない (今までの決め方の値 neV のための入力だけ集める)
      let compName = comp.商品名 || '';
      let compCost = comp.原価 || null;
      const c = selfC && exists ? pub.of(compCode) : null;
      if (c) {
        const x = { ...ne };
        if (pub.has(c, 'cost')) { x.cost = c.v.cost ? c.v.cost.jpy : null; compCost = x.cost || null; }
        if (pub.has(c, 'tax_rate')) x.tax = { neTaxRate: neRateOfDecimal(c.v.tax_rate), manualTaxRate: undefined, componentExists: true };
        if (pub.has(c, 'sales_class')) x.sales = { salesClass: c.v.sales_class, componentExists: true };
        if (pub.has(c, 'handling')) x.handling = { handlingClass: handlingFromCdb(c.v.handling, comp.取扱区分), componentExists: true };
        if (pub.has(c, 'name')) compName = c.v.name;
        cInputs.push(x);
      } else {
        cInputs.push(ne);
      }

      pub.expectComponent(setCode, compCode, { name: comp.商品名 || '', cost: comp.原価 || null }, { fromCdb: !!c });
      expectComps.push({ c: compCode, qty: comp.数量 || 1, name: compName, cost: compCost, from_cdb: !!c });
      // 構成品staging投入 (構成商品名・構成商品原価は、持ち主が C なら C の値)
      insertComponentStaging.run(
        setCode, compCode, comp.数量 || 1,
        compName, compCost, ts
      );
    }

    // 原価・税区分・売上分類・取扱区分 (今までの決め方。deriveSetValues)
    const neD = deriveSetValues({ inputs: neInputs, eg, manualSalesClass: salesClassMap.get(setCode), neSetStatus: neInfo?.取扱区分 });

    const setReasons = [];   // このセットの理由 (今までと同じ順。C の値で変わった列だけ後で差し替える)
    if (neD.genkaSource === '例外') setReasons.push({ code: setCode, kind: 'セット', col: 'cost', reason: 'exception_cost', value: neD.genka });
    if (neD.taxCategory === 'MIXED' || neD.taxCategory === 'UNKNOWN') setReasons.push({ code: setCode, kind: 'セット', col: 'tax_rate', reason: 'set_tax_from_components', value: neD.taxRate, category: neD.taxCategory });
    if (!sh.セット商品名 || !String(sh.セット商品名).trim()) setReasons.push({ code: setCode, kind: 'セット', col: 'name', reason: 'set_name_blank' });
    if (neInfo && neInfo.売価 != null) setReasons.push({ code: setCode, kind: 'セット', col: 'price', reason: 'set_price_from_goods', value: neInfo.売価, set_master_value: sh.セット販売価格 ?? null });

    const neV = {
      name: sh.セット商品名, handling: neD.handling, price: neInfo?.売価 ?? sh.セット販売価格 ?? null,
      genka: neD.genka, genkaSource: neD.genkaSource, genkaStatus: neD.genkaStatus,
      shipCost: ps?.ship_cost ?? null, shipCode: ps?.shipping_code ?? null, shipMethod: ps?.ship_method ?? null,
      taxRate: neD.taxRate, taxCategory: neD.taxCategory, supplier: neInfo?.仕入先コード ?? null, salesClass: neD.salesClass,
    };
    let v = neV;
    if (selfC && !asSet) v = pub.overlay(setCode, neV);   // C の区分が単品・例外 = C の値をそのまま (構成品から導かない)
    else if (selfC) {
      // C の構成品の値で同じ決め方を通す (④a)。セット自身が C にあって原価の持ち主が C なら、セットの例外原価の行は使わない
      //   (C の原価の行が人の決めた原価ならそれを重ねる = overlay の set)。
      //   取扱区分はセット自身の C の値から。語は今までの決め方の値 (neD) に合わせる (C はセットの導いた値を持つ = 持ち主を替えただけで ﾒｰｶｰ取扱中止 → 取扱中止 にしない)
      const self = selfC;
      const cArgs = {
        inputs: cInputs,
        eg: pub.has(self, 'cost') ? null : eg,
        // 売上分類の持ち主が C = セットの手動の行 (product_sales_class) は使わない = 構成品の C の値から導くだけ (15 §5 の 2 の推奨・Codex R1 H3)
        manualSalesClass: pub.owns('sales_class') ? undefined : salesClassMap.get(setCode),
        neSetStatus: pub.has(self, 'handling') ? handlingFromCdb(self.v.handling, neD.handling) : neInfo?.取扱区分,
      };
      const cD = deriveSetValues(cArgs);
      // 導き方の入力と構成品の行を残す (作り直しの取引で m_set_publish_expect に入れる = 入れた後の確かめが同じ決め方で導き直して m_products と比べる)
      setExpect.push({ code: setCode, args: cArgs, components: expectComps });
      v = pub.overlay(setCode, {
        ...neV, genka: cD.genka, genkaSource: cD.genkaSource, genkaStatus: cD.genkaStatus,
        taxRate: cD.taxRate, taxCategory: cD.taxCategory, salesClass: cD.salesClass, handling: cD.handling,
      }, { set: true });
    }
    pub.expect(setCode, neV);
    reasons.push(...(v === neV ? setReasons : mergeReasons(setCode, mk, neV, v, setReasons, { cells: pub.peek(setCode)?.v, generationNo: pub.generation?.generation_no })));
    if (!asSet) reasons.push(kindReason(setCode, 'セット', mk, { cdbKind: pub.peek(setCode)?.kind, generationNo: pub.generation?.generation_no }));

    if (salesClassMap.get(setCode) == null && v.salesClass != null) countSetSalesDerived++;
    // 件数は「従来の式 (NE の値 || 取扱中) から値が変わったセット」= m_products で実際に変わる件数を数える
    const prevStatus = neInfo?.取扱区分 || HANDLING_ACTIVE;
    if (v.handling !== prevStatus) {
      countSetHandlingDerived++;
      if (setHandlingSamples.length < 5) setHandlingSamples.push(`${setCode}=${v.handling}`);
    }

    const coSet = getCarryover(setCode);
    const setLaunchDate = resolveLaunchDate(coSet.new_product_launch_date, neInfo?.作成日);
    insertStaging.run(
      setCode, v.name, mk, v.handling,
      v.price,
      v.genka, v.genkaSource, v.genkaStatus,
      v.shipCost, v.shipCode, v.shipMethod,
      v.taxRate, v.taxCategory,
      neInfo?.在庫数 ?? null, neInfo?.引当数 ?? null, v.supplier,
      asSet ? components.length : null, v.salesClass,
      coSet.seasonality_flag, coSet.season_months, coSet.new_product_flag, setLaunchDate,
      ts
    );
    countSet++;
  }
  log.push(`セット: ${countSet}件（売上分類を構成品から導出: ${countSetSalesDerived}件、`
    + `取扱区分を構成品から引き継ぎ: ${countSetHandlingDerived}件`
    + `${setHandlingSamples.length ? ` 例 ${setHandlingSamples.join(', ')}` : ''}）`);

  // A3: 例外商品（NE・セットに無いもののみ）
  let countException = 0;
  for (const [sku, eg] of exceptionMap) {
    // 既にstagingに入っているか確認
    const exists = db.prepare('SELECT 1 FROM m_products_staging WHERE 商品コード = ?').get(sku);
    if (exists) continue;

    const ps = shippingMap.get(sku);

    // 例外商品は NE に存在しないので neTaxNum=null。手動登録のみが税率ソース
    const { taxRate: exTaxRate, taxCategory: exTaxCategory } = resolveTaxRate(null, taxRateMap.get(sku));
    const exReasons = [];   // この商品の理由 (今までと同じ順)
    exReasons.push({ code: sku, kind: '例外', col: 'cost', reason: 'exception_cost', value: eg.genka });
    if (exTaxRate != null) exReasons.push({ code: sku, kind: '例外', col: 'tax_rate', reason: 'exception_tax_manual', value: exTaxRate });
    // 今までの決め方の値。持ち主が C の列だけ Company DB の値に替える (④a)
    const neV = {
      name: eg.商品名 || '', handling: '取扱中', price: null, genka: eg.genka, genkaSource: '例外', genkaStatus: 'OVERRIDDEN',
      shipCost: ps?.ship_cost ?? null, shipCode: ps?.shipping_code ?? null, shipMethod: ps?.ship_method ?? null,
      taxRate: exTaxRate, taxCategory: exTaxCategory, supplier: null, salesClass: salesClassMap.get(sku) ?? null,
    };
    const mk = pub.kindOf(sku, '例外');   // 区分の持ち主が C = C の区分 (C が単品・セットとする例外の商品も C の区分で)
    const v = pub.overlay(sku, neV);
    pub.expect(sku, neV);
    reasons.push(...(v === neV ? exReasons : mergeReasons(sku, mk, neV, v, exReasons, { cells: pub.peek(sku)?.v, generationNo: pub.generation?.generation_no })));
    if (mk !== '例外') reasons.push(kindReason(sku, '例外', mk, { cdbKind: pub.peek(sku)?.kind, generationNo: pub.generation?.generation_no }));

    const coEx = getCarryover(sku);
    // 例外商品も resolveLaunchDate を通すことで、carryover に既存の不正値
    // ('broken' / '2026-13-01' 等) が入っていても staging に汚染が伝播しない。
    // NE 商品ではないので NE 作成日 のフォールバックはなく、carryover のみを正規化する。
    const exLaunchDate = resolveLaunchDate(coEx.new_product_launch_date, null);
    insertStaging.run(
      sku, v.name, mk, v.handling,
      v.price, v.genka, v.genkaSource, v.genkaStatus,
      v.shipCost, v.shipCode, v.shipMethod,
      v.taxRate, v.taxCategory,
      null, null, v.supplier, null, v.salesClass,
      coEx.seasonality_flag, coEx.season_months, coEx.new_product_flag, exLaunchDate,
      ts
    );
    countException++;
  }
  log.push(`例外: ${countException}件`);
  // 区分の持ち主が C で C = セット・NE = 単品 / 例外を含む食い違い の SKU = 前の m_products・m_set_components の行のまま (fail-closed。設計 = 広げる道 v3 §4.1 b・v4)。
  //   C のセットの構成 (sku_components) は写さない列 = セットの行を作れない。前の行が無い = 載せない (非掲載・NE の値の行も残さない)。理由 = kind_c_set_ne_single_frozen
  const kindFrozen = [];
  {
    const cols = MP_COLS.filter((c) => c !== 'product_id');
    for (const code of pub.frozenCodes()) {
      const prev = db.prepare('SELECT 1 FROM main.m_products WHERE 商品コード = ?').get(code);
      const neKind = db.prepare('SELECT 商品区分 FROM m_products_staging WHERE 商品コード = ?').get(code)?.商品区分 ?? null;   // NE の表の形の区分 (今朝)
      db.prepare('DELETE FROM m_products_staging WHERE 商品コード = ?').run(code);
      db.prepare('DELETE FROM m_set_components_staging WHERE セット商品コード = ?').run(code);
      if (prev) {
        db.prepare(`INSERT INTO m_products_staging (${colList(cols)}) SELECT ${colList(cols)} FROM main.m_products WHERE 商品コード = ?`).run(code);
        db.prepare(`INSERT INTO m_set_components_staging (${colList(MSC_COLS)}) SELECT ${colList(MSC_COLS)} FROM main.m_set_components WHERE セット商品コード = ?`).run(code);
      }
      pub.forget(code);
      kindFrozen.push({ code, prev: !!prev });
      for (const x of reasons.filter((y) => y.code === code)) reasons.splice(reasons.indexOf(x), 1);   // NE の道の理由は使わない (前の行のまま / 載せない)
      const ck = pub.publication?.entries?.get(normSku(code))?.kind ?? null;
      reasons.push({ code, kind: neKind, col: 'kind', reason: 'kind_c_set_ne_single_frozen', owner_key: 'skus.sku_kind', cdb_value: { kind: ck }, value: prev ? 'previous_row' : 'omitted', ne_value: neKind, generation_no: pub.generation?.generation_no ?? null });
    }
  }
  // Company DB の写し (④a): 使った世代・重ねた SKU・Company DB にしか無い SKU (m_products に足さない = not_in_ne)
  let publishStats = null;
  if (pub.active) {
    publishStats = pub.stats();
    log.push(`Company DB の写し: 世代 ${pub.generation.generation_no} (列 ${pub.cols.join('・')})・重ねた SKU ${publishStats.used}・`
      + `Company DB にしか無い ${publishStats.not_in_ne} (足さない)・NE にしか無い ${publishStats.not_in_cdb} (写さない = NE の値のまま)`);
    if (publishStats.not_in_cdb) {
      // NE にしか無い SKU = Company DB にまだ無い (夜間ロードの前の新しい商品・切替の後に NE で直接登録された商品)。止めずに NE の値で作り、知らせる
      warn.push(`Company DB に無い SKU ${publishStats.not_in_cdb} 件は NE の値のまま (写していない): ${publishStats.not_in_cdb_codes.join(', ')}`);
      console.warn(`[m_products] ⚠️ Company DB に無い SKU ${publishStats.not_in_cdb} 件は NE の値のまま: ${publishStats.not_in_cdb_codes.join(', ')}`);
    }
    if (publishStats.kind_mismatch) {
      // NE と C で種類が違う SKU (NE でセットを単品にした等) = その SKU だけ写さない (NE の値のまま)。止めずに知らせる (#1564 の見直し M-5)
      warn.push(`NE と Company DB で種類が違う SKU ${publishStats.kind_mismatch} 件は NE の値のまま (写していない): ${publishStats.kind_mismatch_codes.join(', ')}`);
      console.warn(`[m_products] ⚠️ NE と Company DB で種類が違う SKU ${publishStats.kind_mismatch} 件は NE の値のまま: ${publishStats.kind_mismatch_codes.join(', ')}`);
    }
    if (publishStats.kind_c_single_ne_set) {
      // 区分の持ち主が C: 社内は単品・NE はセット = 単品として C の値で写した (構成は写さない・止めない)。NE は判断の一覧から NE の画面で直す
      const m = `社内は単品・NE はセットの SKU ${publishStats.kind_c_single_ne_set} 件は単品として写した (構成は写さない・NE は NE の画面で直す): ${publishStats.kind_c_single_ne_set_codes.join(', ')}`;
      warn.push(m); console.warn(`[m_products] ⚠️ ${m}`);
    }
    if (publishStats.kind_c_set_ne_single_frozen) {
      // 社内はセット・NE は単品 = 前の行のまま (fail-closed)。毎朝知らせる
      const om = kindFrozen.filter((x) => !x.prev).map((x) => x.code);
      const m = `社内と NE で区分が違う SKU ${publishStats.kind_c_set_ne_single_frozen} 件は前の行のまま (社内がセット・NE が単品 / 例外を含む。NE を社内の区分に直すまで)${om.length ? `・前の行が無い ${om.length} 件は載せない (${om.slice(0, 20).join(', ')})` : ''}: ${publishStats.kind_c_set_ne_single_frozen_codes.join(', ')}`;
      warn.push(m); console.warn(`[m_products] ⚠️ ${m}`);
    }
  }

  // この作り直しが作った staging のハッシュ (品質チェックの前に取る = チェックした中身と入れ替える中身が同じことを入れ替えの取引で確かめる)
  const myStagingHash = stagingHash(db);

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
    // 取扱区分も直接の構成品しか見ない (resolveSetHandlingClass)。孫の構成品が止まっても親セットは止まらない
    checks.push(`⚠️ ネストセット（構成品がセット）: ${nestedSets}件 → 売上分類は導出されず未登録一覧に出ます。`
      + '取扱区分は直接の構成品しか見ないので、構成セットの中の商品が止まっても親セットは取扱中のまま残ります');
    warn.push('ネストセットあり（売上分類の導出対象外・取扱区分は直接の構成品のみ）');
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

  // ─── 変わらない作り直しは入れ替えない (#1564 Codex R2 Medium 5) ───
  //   同じ NE の取得・同じ写しの世代・同じ中身・古い表が世代のまま = 業務の表も作り直しの記録も 1 行も書かない (updated_at も変わらない)
  const same = unchangedSinceLastBuild(db, { startMarks, pub, ownership });
  if (same.skip) {
    const finalCount = db.prepare('SELECT COUNT(*) as cnt FROM m_products').get().cnt;
    const compCount = db.prepare('SELECT COUNT(*) as cnt FROM m_set_components').get().cnt;
    const msg = `⏭️ 前の作り直し (${same.build.build_id}) から何も変わっていない (同じ NE の取得・同じ写しの世代・同じ中身) = 入れ替えない`;
    console.log(`[m_products] ${msg}`);
    log.push(msg);
    return { ok: true, skipped: 'unchanged', build_id: same.build.build_id, log, checks, warn, total: finalCount, components: compCount,
      publish: { generation_no: same.build.cdb_publish_generation_no ?? null, epoch: epochNote, active: pub.active, cols: pub.cols, stats: publishStats,
        side_tables: { exception_genka: { updated: 0, deleted: 0 }, product_shipping: { updated: 0, deleted: 0 }, m_reorder_setting: { updated: 0, inserted: 0, deleted: 0 } }, applied: same.applied } };
  }

  // ─── Phase C: 本番反映 ───

  // 本番反映（明示列INSERT、列順破壊耐性あり）+ 作り直しの記録 (m_products_builds。同じ取引)
  //   + 上書き表の既にある行を持ち主が C の列の値にそろえる (④a。同じ取引・記録の前。持ち主が全部 load なら何もしない)
  let build;
  try {
    build = applyStagingToProduction(db, { build: { buildId, startMarks, startedAt, expectedStagingHash: myStagingHash, reasons, publish: pub, setExpect, kindFrozen } });
  } catch (e) {
    if (!e || e.code !== 'CDB_PUBLISH_VERIFY') throw e;
    const msg = `❌ 入れた後の確かめで Company DB の値と違う → 反映中止 (巻き戻した = 前の m_products のまま): ${String(e.message).slice(0, 300)}`;
    console.error(`[m_products] ${msg}`);
    return { ok: false, error: 'CDB_PUBLISH_VERIFY', log, checks: [...checks, msg], warn, total: totalStaging, publish: { applied: e.applied } };
  }
  const markNote = (m) => (m.value ? m.value : `なし (${m.note})`);
  console.log(`[m_products] 作り直しの記録: ${build.build_id} (NE の印 単品 ${markNote(build.marks.products)} / セット ${markNote(build.marks.set_components)} / 理由 ${JSON.stringify(build.reason_counts)}`
    + ` / Company DB の写しの世代 ${build.publish?.generation_no ?? 'なし'})`);
  log.push(`作り直しの記録: ${build.build_id}`);
  if (pub.active) log.push(`上書き表を Company DB の値にそろえた: ${JSON.stringify(build.side_tables)} / 入れた後の確かめ: ${JSON.stringify(build.applied.counts)}`);
  if (pub.active && build.applied.counts.mixed_sets) {
    // C にあるセットの構成品に NE にしか無い単品がある = 導いた値 (原価・税・売上分類・取扱区分) に C と NE の値が混ざる。止めずに知らせる (#1564 の見直し L-2)
    const m = `Company DB にあるセットの構成品に NE にしか無い単品がある ${build.applied.counts.mixed_sets} 件 = 導いた値に C と NE の値が混ざる: ${build.applied.mixed_set_codes.join(', ')}`;
    warn.push(m);
    console.warn(`[m_products] ⚠️ ${m}`);
  }

  // WAL肥大化防止
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  const finalCount = db.prepare('SELECT COUNT(*) as cnt FROM m_products').get().cnt;
  const compCount = db.prepare('SELECT COUNT(*) as cnt FROM m_set_components').get().cnt;
  console.log(`[m_products] ✅ 反映完了: ${finalCount}件 (構成品: ${compCount}件)`);

  log.push(`反映完了: ${finalCount}件 (構成品: ${compCount}件)`);

  return { ok: true, log, checks, warn, total: finalCount, components: compCount,
    publish: { generation_no: build.publish?.generation_no ?? null, epoch: epochNote, active: pub.active, cols: pub.cols, stats: publishStats, side_tables: build.side_tables, applied: build.applied } };
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
