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

// 納品予定日 'YYYY-MM-DD' → 'M月D日' (不正なら '')
export function formatDeliveryDateJa(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd ?? '').trim());
  if (!m) return '';
  return `${parseInt(m[2], 10)}月${parseInt(m[3], 10)}日`;
}

// Notion カード名: 「6月27日納品予定FBA納品ピッキング」。日付不正なら null。
export function buildPickingCardTitle(ymd) {
  const d = formatDeliveryDateJa(ymd);
  return d ? `${d}納品予定FBA納品ピッキング` : null;
}

// 'YYYY-MM-DD' が実在日付か (形式 + UTC往復検証。'2026-99-99' を弾く)
export function isValidDateYmd(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd ?? '').trim());
  if (!m) return false;
  const y = +m[1], mo = +m[2], d = +m[3];
  const dt = new Date(Date.UTC(y, mo - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === mo - 1 && dt.getUTCDate() === d;
}

// 商品コード正規化 (GAS PL_normCode_ 相当) — lib/sku-norm.js に全社共通化 (監査PR-10)。
// 本ファイル内でも使用するため import + 既存の import { normCode } 互換の re-export
import { normSku as normCode } from '../../lib/sku-norm.js';
export { normCode };

const safe = (v) => (v == null ? '' : String(v).replace(/[﻿]/g, '').trim());

// FBA 納品プラン CSV の列インデックス (0始まり、GAS 準拠)
const COL_SKU = 0;   // A: SKU
const COL_FNSKU = 3; // D: FNSKU
const COL_QTY = 9;   // J: 数量
const PLAN_DATA_START = 6; // 7行目から (index 6)

// ロジザード ピッキングリスト(lzpickinglist.csv) の列インデックス (GAS PL_ 準拠)
const LZ_BLOCK = 7;   // H: ブロック略称
const LZ_LOC = 8;     // I: ロケーション
const LZ_CODE = 9;    // J: 商品ID(=商品コード)
const LZ_NAME = 10;   // K: 商品名
const LZ_QTY = 13;    // N: 出荷引当 = ロケ別ピック数 (TMP1 PDF の「数量」列と同値。実データで全行一致確認済)
const LZ_EXPIRY = 17; // R: 有効期限 YYYYMMDD ('29991231'=無期限)
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

    rows.push({
      block, location, code, name, planNo, dodai,
      qty: safe(row[LZ_QTY]),       // ロケ別ピック数 (文字列のまま。数値検証は reconcilePdfWithLz)
      expiry: safe(row[LZ_EXPIRY]), // 有効期限 YYYYMMDD
    });
  }

  return { rows, warnings, matchedCodes };
}

// 旧5列ラベルCSV (buildLabelRows) は 2026-08-11 に廃止 — 固定名 fbanouhinbangoulist.csv は
// buildLabelRowsV2 の10列に一本化 (P-touch新テンプレート実機検証済み)。過去実行の旧5列DLは
// 履歴の保存済み result.labelCsvRows から再生成する (関数は不要)。

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

// ==========================================================================
// v2 シールCSV: TMP1 PDF の情報 (ブロック/ロケ/数量/残数/期限) を統合し、
// 「シール枚数 = ピッキングリスト行数」を fail-closed で保証する。
// 残数は lz CSV に存在しないため TMP1 PDF 抽出値を突合して取り込む。
// 突合は matchKey(ブロック|ロケ|商品ID) ごとの FIFO キュー + 数量一致検証。
// ==========================================================================

// ZZZ (ロケ未引当) 行のセンチネル
export const UNALLOCATED = 'UNALLOCATED';
// 未引当行のロケーション表示 (TMP1 PDF と同じ見た目)
export const UNALLOCATED_LOC_DISPLAY = 'ZZZ-ZZZ-ZZ';

export function normalizeBlock(v) {
  return String(v ?? '').trim().toUpperCase();
}

/**
 * ロケーション正規化: 既知2形式のみ受理し 'NNN-NNN-NN' 表示形式へ揃える。
 *  - lz CSV: '00101202' (8桁数字) / TMP1 PDF: '001-012-02'
 *  - ZZZ 未引当 ('ZZZZZZZZ' / 'ZZZ-ZZZ-ZZ') は UNALLOCATED センチネル
 *  - 未知形式は null (呼び出し側で fail-closed。数字だけに潰さない)
 */
export function normalizeLocation(v) {
  const s = String(v ?? '').trim().toUpperCase().replace(/[－ー―‐]/g, '-');
  if (!s) return null;
  if (/^Z+$/.test(s.replace(/-/g, ''))) return UNALLOCATED;
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 3)}-${s.slice(3, 6)}-${s.slice(6, 8)}`;
  if (/^\d{3}-\d{3}-\d{2}$/.test(s)) return s;
  return null;
}

/** 突合キー。ロケが未知形式なら null。 */
export function buildMatchKey(block, location, code) {
  const loc = normalizeLocation(location);
  return loc == null ? null : `${normalizeBlock(block)}|${loc}|${normCode(code)}`;
}

/** 非負整数の厳密パース。空・小数・単位付き等は null (0 扱いにしない)。 */
export function parseStrictNonNegInt(v) {
  const s = String(v ?? '').trim();
  if (!/^(0|[1-9]\d*)$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * 有効期限 (lz CSV R列 YYYYMMDD) の表示変換。
 *  - 空欄: 空表示 + 警告 (期限はピッキング突合の成立条件ではないため継続)
 *  - '29991231': 無期限センチネル → 空表示
 *  - 実在する8桁日付: 'YYYY/MM/DD'
 *  - 非空の不正値 (形式不正/存在しない日付): error (呼び出し側で422。黙って空欄化しない)
 */
export function formatExpiry(raw) {
  const s = String(raw ?? '').trim();
  if (!s) return { value: '', warn: 'empty' };
  if (s === '29991231') return { value: '' };
  if (!/^\d{8}$/.test(s)) return { error: `有効期限の形式が不正です: ${s}` };
  const y = +s.slice(0, 4), m = +s.slice(4, 6), d = +s.slice(6, 8);
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== m - 1 || dt.getUTCDate() !== d) {
    return { error: `有効期限が存在しない日付です: ${s}` };
  }
  return { value: `${y}/${String(m).padStart(2, '0')}/${String(d).padStart(2, '0')}` };
}

/**
 * TMP1 PDF 抽出行と lz ピッキング行の FIFO 突合。残数(zansu)を lz 行へ付与する。
 * 行順同一は実測確認済みだが、帳票変更・ファイル取り違えに備えて
 * ①総行数 ②キー毎件数 ③FIFO対応後の数量一致 の三段で fail-closed 検証する。
 * 残余リスク: 同一キー・同一数量・異なる残数の複数行は誤対応を検出できない
 * (行順が保たれる限り正しい。帳票のソート仕様変更時のみ影響)。
 * @param {Array} pdfItems [{page,item,block,location,productId,qty,zansu}] (数値検証済み)
 * @param {Array} lzRows2 buildPickingList の rows (qty/expiry 付き)
 * @returns {{ rows: Array, errors: string[] }} rows は lz 行順を維持
 */
export function reconcilePdfWithLz(pdfItems, lzRows2) {
  const errors = [];
  if (pdfItems.length !== lzRows2.length) {
    errors.push(`TMP1 PDFの行数(${pdfItems.length})とピッキングリストCSVの行数(${lzRows2.length})が一致しません`);
    return { rows: [], errors };
  }

  // lz 側: matchKey ごとのキュー (行順維持)
  const queues = new Map();
  for (let i = 0; i < lzRows2.length; i++) {
    const r = lzRows2[i];
    const key = buildMatchKey(r.block, r.location, r.code);
    if (key == null) {
      errors.push(`ピッキングリストCSV ${i + 1}行目: ロケーション形式を解釈できません: ${r.location}`);
      continue;
    }
    if (!queues.has(key)) queues.set(key, []);
    queues.get(key).push({ idx: i, row: r });
  }
  if (errors.length) return { rows: [], errors };

  // PDF 側キー化 + キー毎件数一致
  const pdfKeyed = [];
  const pdfKeyCount = new Map();
  for (const it of pdfItems) {
    const key = buildMatchKey(it.block, it.location, it.productId);
    if (key == null) {
      errors.push(`TMP1 PDF p${it.page + 1} ${it.item + 1}行目: ロケーション形式を解釈できません: ${it.location}`);
      continue;
    }
    pdfKeyed.push({ it, key });
    pdfKeyCount.set(key, (pdfKeyCount.get(key) || 0) + 1);
  }
  if (errors.length) return { rows: [], errors };
  for (const [key, q] of queues) {
    const c = pdfKeyCount.get(key) || 0;
    if (c !== q.length) errors.push(`突合不一致: ${key} — CSV ${q.length}行 / PDF ${c}行`);
  }
  for (const [key, c] of pdfKeyCount) {
    if (!queues.has(key)) errors.push(`PDFのみに存在する行: ${key} (${c}行)`);
  }
  if (errors.length) return { rows: [], errors };

  // FIFO 対応 + 数量一致 (lz行順で出力)
  const out = new Array(lzRows2.length);
  for (const { it, key } of pdfKeyed) {
    const { idx, row } = queues.get(key).shift();
    const lzQty = parseStrictNonNegInt(row.qty);
    if (lzQty == null) {
      errors.push(`ピッキングリストCSV ${idx + 1}行目 (${key}): 数量(出荷引当)が不正です: ${JSON.stringify(row.qty)}`);
      continue;
    }
    if (it.qty !== lzQty) {
      errors.push(`数量不一致: ${key} — CSV ${lzQty} / PDF ${it.qty} (TMP1 p${it.page + 1} ${it.item + 1}行目)`);
      continue;
    }
    out[idx] = { ...row, qty: lzQty, zansu: it.zansu, pdfPage: it.page, pdfItem: it.item };
  }
  if (errors.length) return { rows: [], errors };
  return { rows: out, errors };
}

export const LABEL_V2_HEADER = ['商品ID', '納品プランNo', '商品名', 'バーコード', '土台商品', 'ブロック', 'ロケーション', '数量', '残数', '有効期限'];

/**
 * v2 シールCSV行 (10列)。全 lz 行を出力する = シール枚数保証。
 *  - プランNo未解決行はスキップせず「プランなし」と印字 (中原さん決定)
 *  - 未引当(ZZZ)行も出力し、通常行の後ろにまとめる (件数は unallocatedCount で返す)
 * @param {Array} mergedRows reconcilePdfWithLz の rows
 * @param {Map<string,string>} barcodeMap normCode → バーコード
 */
export function buildLabelRowsV2(mergedRows, barcodeMap) {
  const warnings = [];
  const expiryErrors = [];
  const missingBarcode = new Set();
  const normalRows = [];
  const unallocRows = [];
  let planlessCount = 0;
  let expiryEmptyCount = 0;

  for (const row of mergedRows) {
    const key = normCode(row.code);
    const barcode = barcodeMap.get(key) || '';
    if (!barcode) missingBarcode.add(key);
    let planNo = String(row.planNo || '').split('\n').map(p => p.trim()).filter(Boolean).join(' / ');
    if (!planNo) { planNo = 'プランなし'; planlessCount++; }
    const loc = normalizeLocation(row.location);
    const isUnalloc = loc === UNALLOCATED;
    const exp = formatExpiry(row.expiry);
    if (exp.error) expiryErrors.push(`${row.code} (${row.block} ${row.location}): ${exp.error}`);
    else if (exp.warn === 'empty') expiryEmptyCount++;
    const rec = [
      row.code, planNo, row.name, barcode, row.dodai,
      normalizeBlock(row.block),
      isUnalloc ? UNALLOCATED_LOC_DISPLAY : loc,
      String(row.qty), String(row.zansu),
      exp.error ? '' : exp.value,
    ];
    (isUnalloc ? unallocRows : normalRows).push(rec);
  }

  if (missingBarcode.size) {
    warnings.push(`バーコード未登録: ${missingBarcode.size}件 (${Array.from(missingBarcode).slice(0, 10).join(', ')}${missingBarcode.size > 10 ? ' …' : ''})`);
  }
  if (planlessCount) warnings.push(`納品プランNo未解決行: ${planlessCount}件 — シールには「プランなし」と印字されます`);
  if (unallocRows.length) warnings.push(`ロケ未引当(ZZZ)行: ${unallocRows.length}件 — シールCSV末尾にまとめています`);
  if (expiryEmptyCount) warnings.push(`有効期限が空欄の行: ${expiryEmptyCount}件 (空欄のまま出力)`);

  return {
    csvRows: [LABEL_V2_HEADER, ...normalRows, ...unallocRows],
    warnings,
    expiryErrors,
    planlessCount,
    unallocatedCount: unallocRows.length,
  };
}
