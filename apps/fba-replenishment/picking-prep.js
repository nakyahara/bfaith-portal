/**
 * FBA納品ピッキング準備 — コアロジック (純関数)
 *
 * GAS 5関数 (run / convertSkuToProductCodes / PL_writePickingList / importFBAcsvToSheets_v9 /
 * FBA_RunAllToday) を 1 つのサーバーサイドパイプラインに集約。中間シート(連番/商品コード連番)は
 * メモリ上の中間データに置き換える。
 *
 * 設計根拠 (Codex レビュー):
 *  #3 セット構成 qty を保持 (商品コード別必要数 = SKU数量 × component.qty)。ただしプラン別シートの
 *     「数量」は Amazon 納品プランの SKU 数量(J列)をそのまま使う (倉庫ピッキングは数量を持たない)。
 *  #4 FNSKU はプラン CSV の D列を正とする (マスタ由来で上書きしない)。
 *  #5 ラベル接頭辞はアップロード時のファイル名(スロット)由来。採番はファイル内の SKU 非空行順 1..n。
 *  #9 突合警告 (変換不可SKU / lz未存在コード / 複数ロケ / バーコード未登録) を集約して返す。
 */

// SKU 正規化 (db.js の normSku と同一: case 非依存の突き合わせ)
export const normSku = (v) => String(v ?? '').trim().toLowerCase();

// 商品コード正規化 (GAS PL_normCode_ 相当: 全角→半角, ダッシュ統一, 空白除去, 小文字化)
export function normCode(v) {
  if (v == null) return '';
  let t = String(v);
  t = t.replace(/[！-～]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0)); // 全角英数記号→半角
  t = t.replace(/　/g, ' ');                                                  // 全角SP→半角
  t = t.replace(/[−‐-―﹘﹣－]/g, '-');                  // 各種ダッシュ→ハイフン
  t = t.trim().replace(/\s+/g, '').toLowerCase();
  return t;
}

const safe = (v) => (v == null ? '' : String(v).replace(/[﻿]/g, '').trim());

// FBA 納品プラン CSV の列インデックス (0始まり、GAS 準拠)
const COL_SKU = 0;   // A: SKU
const COL_FNSKU = 3; // D: FNSKU
const COL_QTY = 9;   // J: 数量
const PLAN_DATA_START = 6; // 7行目から (index 6)

// ロジザード ピッキングリスト(lzpickinglist.csv) の列インデックス (GAS PL_ 準拠)
const LZ_BLOCK = 7;  // H: ブロック略称
const LZ_LOC = 8;    // I: ロケーション
const LZ_CODE = 9;   // J: 商品ID(=商品コード)
const LZ_NAME = 10;  // K: 商品名
const LZ_DATA_START = 1; // 1行目はヘッダ

/**
 * 1 プラン CSV をパースして製品行を抽出。SKU(A列) 非空行に 1..n を採番。
 * @param {string} labelPrefix ラベル接頭辞 (例: '危険','通常','通常プラン2')
 * @param {string[][]} rows parseCsv 済みの全行
 * @param {Set<string>} dodaiSet 土台商品 SKU の正規化済みセット
 * @returns {{ items: Array, rowCount:number }}
 */
export function parsePlanFile(labelPrefix, rows, dodaiSet) {
  const items = [];
  let seq = 0;
  for (let r = PLAN_DATA_START; r < rows.length; r++) {
    const row = rows[r] || [];
    const sku = safe(row[COL_SKU]);
    if (!sku) continue; // SKU 非空行のみ (GAS run() と同一の採番母集団)
    seq++;
    items.push({
      seq,
      sku,
      fnsku: safe(row[COL_FNSKU]),
      qty: safe(row[COL_QTY]),
      label: `${labelPrefix}_${seq}`,
      isDodai: dodaiSet.has(normSku(sku)),
    });
  }
  return { items, rowCount: items.length };
}

/**
 * mapping から商品コード構成 [{ne_code, qty}] を取り出す (セット 1対多展開)。
 */
export function codesForMapping(mapping) {
  if (!mapping) return [];
  let comps = [];
  if (mapping.set_components) {
    try {
      comps = typeof mapping.set_components === 'string'
        ? JSON.parse(mapping.set_components)
        : mapping.set_components;
    } catch { comps = []; }
  }
  if (!Array.isArray(comps)) comps = [];
  let list = comps
    .map(c => ({ ne_code: safe(c.ne_code), qty: parseInt(c.qty) || 1 }))
    .filter(c => c.ne_code);
  if (list.length === 0 && safe(mapping.ne_code)) {
    list = [{ ne_code: safe(mapping.ne_code), qty: 1 }];
  }
  return list;
}

/**
 * 全プラン製品行を商品コードに展開し、商品コード→{labels,dodai} の索引を作る。
 * @param {Array} planItems parsePlanFile の items を全スロット連結したもの
 * @param {Map<string,object>} mappingMap normSku(amazon_sku) → mapping
 * @returns {{ codeIndex: Map, expanded: Array, warnings: string[], unmappedSkus: string[] }}
 */
export function expandToCodes(planItems, mappingMap) {
  const codeIndex = new Map(); // normCode → { labels:Set<string>, dodai:'土台商品'|'' }
  const expanded = [];
  const warnings = [];
  const unmappedSkus = [];

  for (const item of planItems) {
    const mapping = mappingMap.get(normSku(item.sku));
    if (!mapping) {
      warnings.push(`${item.sku} (${item.label}): SKUマッピングなし — 商品コードに変換できません`);
      unmappedSkus.push(item.sku);
      continue;
    }
    const comps = codesForMapping(mapping);
    if (comps.length === 0) {
      warnings.push(`${item.sku} (${item.label}): NE商品コードなし — 変換できません`);
      unmappedSkus.push(item.sku);
      continue;
    }
    for (const comp of comps) {
      const key = normCode(comp.ne_code);
      if (!key) continue;
      expanded.push({
        code: comp.ne_code,
        label: item.label,
        sku: item.sku,
        isDodai: item.isDodai,
        compQty: comp.qty,
      });
      if (!codeIndex.has(key)) codeIndex.set(key, { labels: new Set(), dodai: '' });
      const entry = codeIndex.get(key);
      entry.labels.add(item.label);
      if (item.isDodai) entry.dodai = '土台商品';
    }
  }
  return { codeIndex, expanded, warnings, unmappedSkus };
}

/**
 * lzpickinglist を商品コードで突合し、納品ピッキングリストの行を作る。
 * @returns {{ rows: Array, warnings: string[], matchedCodes:Set }}
 */
export function buildPickingList(lzRows, codeIndex) {
  const rows = [];
  const warnings = [];
  const matchedCodes = new Set();      // codeIndex のうち lz に存在したもの

  for (let r = LZ_DATA_START; r < lzRows.length; r++) {
    const row = lzRows[r] || [];
    const block = safe(row[LZ_BLOCK]);
    const location = safe(row[LZ_LOC]);
    const code = safe(row[LZ_CODE]);
    const name = safe(row[LZ_NAME]);
    if (!block && !location && !code && !name) continue;

    const key = normCode(code);
    const entry = key ? codeIndex.get(key) : null;
    const planNo = entry && entry.labels.size
      ? Array.from(entry.labels).sort().join('\n')
      : '';
    const dodai = entry ? entry.dodai : '';
    if (entry) matchedCodes.add(key);

    rows.push({ block, location, code, name, planNo, dodai });
  }

  return { rows, warnings, matchedCodes };
}

/**
 * P-touch ラベル印刷用 CSV の行配列 (ヘッダ含む) を作る。
 * @param {Array} pickingRows buildPickingList の rows
 * @param {Map<string,string>} barcodeMap normCode → バーコード
 * @returns {{ csvRows: string[][], warnings: string[] }}
 */
export function buildLabelRows(pickingRows, barcodeMap) {
  const csvRows = [['商品ID', '納品プランNo', '商品名', 'バーコード', '土台商品']];
  const warnings = [];
  const missingBarcode = new Set();
  for (const row of pickingRows) {
    if (!row.code || !row.planNo) continue; // 商品ID と プランNo が揃う行のみ (GAS 準拠)
    const key = normCode(row.code);
    const barcode = barcodeMap.get(key) || '';
    if (!barcode) missingBarcode.add(key);
    const planNo = row.planNo.split('\n').map(p => p.trim()).filter(Boolean).join(' / ');
    csvRows.push([row.code, planNo, row.name, barcode, row.dodai]);
  }
  if (missingBarcode.size) {
    warnings.push(`バーコード未登録: ${missingBarcode.size}件 (${Array.from(missingBarcode).slice(0, 10).join(', ')}${missingBarcode.size > 10 ? ' …' : ''})`);
  }
  return { csvRows, warnings };
}

/**
 * プラン別 ラベル貼り作業シート行を作る。
 * 列レイアウト (現場の正本に合わせる): No / FNSKU / 商品名 / 数量 / ラベル貼り担当者 /
 *   ラベル貼り確認担当者 / 納品箱No / 期限管理商品
 *  - FNSKU: プラン CSV の D列を正 (Codex #4)
 *  - 商品名: SKUマップ (getSkuMappings) の product_name を使う (Amazonタイトルでなくマスタ商品名)
 *  - 数量: プラン CSV の J列(SKU数量)をそのまま
 *  - ラベル貼り担当者/確認担当者/納品箱No/期限管理商品: 現場手動記入のため空欄
 * @returns {Array<{no,fnsku,productName,qty}>}
 */
export function buildPlanSheet(planItems, mappingMap) {
  return planItems.map((item, i) => {
    const mapping = mappingMap.get(normSku(item.sku));
    return {
      no: i + 1,
      fnsku: item.fnsku,
      productName: mapping?.product_name || '',
      qty: item.qty,
    };
  });
}

/**
 * 展開済み商品コードのうち、lzpickinglist に存在しなかったもの (=倉庫ピッキング対象外) を警告化。
 */
export function findCodesNotInPicking(codeIndex, matchedCodes) {
  const missing = [];
  for (const [key, entry] of codeIndex) {
    if (!matchedCodes.has(key)) {
      missing.push({ code: key, labels: Array.from(entry.labels).sort().join(' / ') });
    }
  }
  return missing;
}
