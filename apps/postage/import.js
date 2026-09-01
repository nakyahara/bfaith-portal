/**
 * 重量マスタの取込 (Excel『定形外の重さ.xlsx』/ CSV)。
 *
 * 取込は「読んで入れる」だけにしない。**入れる前に検証して、弾いた行を残す**。
 * 実データ (2026-08-30 時点 800行) には次が混ざっていた:
 *   - 列ズレ 8行 …… 「送り状の重さ」「資材の厚み」列に `0.1cm` が入り「商品の厚み」が空
 *   - 商品コードの重複 29件
 *   - 単位つきの文字列 (`5g` `0.1cm`) と数値が同じ列に混在
 * これらを黙って取り込むと、静かに間違った料金が出る。
 *
 * 列は **ヘッダ名で引く** (位置で引かない)。列を足したり並べ替えても壊れないようにする。
 */
import ExcelJS from 'exceljs';
import fs from 'fs';
import { getDB, MATERIAL_NAME_TO_CODE, NAME_SUFFIX_TO_MATERIAL } from './db.js';

// ヘッダ名 → 内部キー。表記ゆれ (全角空白・前後空白) は normalizeHeader で吸収する
const HEADER_MAP = {
  商品コード: 'sku_code',
  商品名: 'display_name',
  商品の重さ: 'unit_weight_g',
  資材: 'material_name',
  資材の重さ: 'material_tare_g',
  送り状の重さ: 'overhead_g',
  資材の厚み: 'material_thickness',
  商品の厚み: 'thickness_mm',
};

const REQUIRED_HEADERS = ['商品コード'];

function normalizeHeader(v) {
  return String(v ?? '').normalize('NFKC').replace(/\s+/g, '').trim();
}
export function normalizeSku(v) {
  return String(v ?? '').normalize('NFKC').trim().toLowerCase();
}

/**
 * 「15」「15.0」「5g」「5 g」→ 15 / 5。
 * 「0.1cm」のように **長さの単位** が来たら null + 理由 (列ズレの検出)。
 */
export function parseGrams(raw) {
  if (raw === null || raw === undefined || raw === '') return { value: null };
  if (typeof raw === 'number') return Number.isFinite(raw) ? { value: raw } : { value: null, err: 'not_a_number' };
  const s = String(raw).normalize('NFKC').trim();
  if (!s) return { value: null };
  if (/(cm|mm|センチ|ミリ)/i.test(s)) return { value: null, err: 'length_unit_in_weight_column' };
  const m = s.match(/^([\d.]+)\s*(g|グラム)?$/i);
  if (!m) return { value: null, err: 'unparsable' };
  const n = Number(m[1]);
  return Number.isFinite(n) ? { value: n } : { value: null, err: 'unparsable' };
}

/**
 * 「0.1cm」「2cm」「3mm」「0.1」→ mm。単位なしは **cm とみなす** (実データが cm 表記で統一されているため)。
 * 「5g」のように重さの単位が来たら null + 理由。
 */
export function parseThicknessMm(raw) {
  if (raw === null || raw === undefined || raw === '') return { value: null };
  if (typeof raw === 'number') {
    return Number.isFinite(raw) ? { value: raw * 10, assumedCm: true } : { value: null, err: 'not_a_number' };
  }
  const s = String(raw).normalize('NFKC').trim();
  if (!s) return { value: null };
  if (/(^|[\d\s])(g|グラム)$/i.test(s)) return { value: null, err: 'weight_unit_in_thickness_column' };
  let m = s.match(/^([\d.]+)\s*mm$/i);
  if (m) return { value: Number(m[1]) };
  m = s.match(/^([\d.]+)\s*(cm|センチ)$/i);
  if (m) return { value: Number(m[1]) * 10 };
  m = s.match(/^([\d.]+)$/);
  if (m) return { value: Number(m[1]) * 10, assumedCm: true };
  return { value: null, err: 'unparsable' };
}

/** 商品名の末尾サフィックス (`… _長3封`) から資材コードを推定する。 */
export function materialFromName(name) {
  const m = String(name ?? '').normalize('NFKC').trim().match(/_([^_\s]+)$/);
  if (!m) return null;
  return NAME_SUFFIX_TO_MATERIAL[m[1].trim()] || null;
}

/** xlsx / csv → [{__row, 商品コード: …}, …] */
async function readRows(filePath) {
  const wb = new ExcelJS.Workbook();
  const isCsv = /\.csv$/i.test(filePath);
  if (isCsv) await wb.csv.readFile(filePath);
  else await wb.xlsx.readFile(filePath);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('シートが見つかりません');

  const headers = [];
  ws.getRow(1).eachCell({ includeEmpty: true }, (cell, col) => {
    headers[col] = HEADER_MAP[normalizeHeader(cell.value)] || null;
  });
  const missing = REQUIRED_HEADERS.filter((h) => !headers.includes(HEADER_MAP[h]));
  if (missing.length) throw new Error(`必須の列が見つかりません: ${missing.join(', ')}`);

  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, rowNo) => {
    if (rowNo === 1) return;
    const o = { __row: rowNo };
    let any = false;
    row.eachCell({ includeEmpty: true }, (cell, col) => {
      const key = headers[col];
      if (!key) return;
      // 数式セルは計算結果を使う。リッチテキストは平文化する
      let v = cell.value;
      if (v && typeof v === 'object') {
        if ('result' in v) v = v.result;
        else if ('richText' in v) v = v.richText.map((t) => t.text).join('');
        else if ('text' in v) v = v.text;
      }
      o[key] = v;
      if (v !== null && v !== undefined && v !== '') any = true;
    });
    if (any) rows.push(o);
  });
  return { rows, sheetName: ws.name };
}

/**
 * 取込。dryRun=true なら検証だけして DB を変更しない (既定)。
 * @returns {{importRunId, rowCount, appliedCount, issues, summary}}
 */
export async function importWeightFile(filePath, { dryRun = true, actor = 'system' } = {}) {
  if (!fs.existsSync(filePath)) throw new Error(`ファイルがありません: ${filePath}`);
  const db = getDB();
  const { rows, sheetName } = await readRows(filePath);

  const runId = db.prepare(`INSERT INTO pm_import_runs
    (source_name, sheet_name, row_count, dry_run, imported_by) VALUES (?,?,?,?,?)`)
    .run(filePath.split(/[\\/]/).pop(), sheetName, rows.length, dryRun ? 1 : 0, actor).lastInsertRowid;

  const issues = [];
  const addIssue = (row, sku, severity, kind, column, raw, message) =>
    issues.push({ row_no: row, sku_code: sku, severity, kind, column_name: column, raw_value: raw == null ? null : String(raw), message });

  const parsed = new Map();   // sku_code → 行
  const seenAt = new Map();   // sku_code → 最初に出た行番号

  for (const r of rows) {
    const sku = normalizeSku(r.sku_code);
    if (!sku) { addIssue(r.__row, null, 'error', 'missing_sku_code', '商品コード', r.sku_code, '商品コードが空'); continue; }

    // ── 重複 ──
    if (parsed.has(sku)) {
      const prev = parsed.get(sku);
      const same = prev.unit_weight_g === firstNum(r.unit_weight_g) && normStr(prev.display_name) === normStr(r.display_name);
      addIssue(r.__row, sku, same ? 'warn' : 'error', 'duplicate_sku', '商品コード', sku,
        same ? `${seenAt.get(sku)}行目と同じ内容の重複` : `${seenAt.get(sku)}行目と内容が違う重複 — どちらが正か決めてください`);
      if (!same) continue;  // 食い違う重複は取り込まない (どちらが正か決まるまで)
    }

    // ── 商品の重さ ──
    const w = parseGrams(r.unit_weight_g);
    if (w.err === 'length_unit_in_weight_column') {
      addIssue(r.__row, sku, 'error', 'column_shift', '商品の重さ', r.unit_weight_g,
        '重さの列に cm が入っています (列ズレの疑い)');
    } else if (w.err) {
      addIssue(r.__row, sku, 'error', 'unparsable_weight', '商品の重さ', r.unit_weight_g, '重さとして読めません');
    }

    // ── 送り状の重さ列 (本来は 1 通あたりの固定値。ここに商品ごとの値が入るのは誤入力) ──
    const oh = parseGrams(r.overhead_g);
    if (r.overhead_g !== null && r.overhead_g !== undefined && r.overhead_g !== '') {
      if (oh.err === 'length_unit_in_weight_column') {
        addIssue(r.__row, sku, 'error', 'column_shift', '送り状の重さ', r.overhead_g,
          '送り状の「重さ」に cm が入っています — 商品の厚みが1列ずれて入った可能性');
      } else {
        addIssue(r.__row, sku, 'warn', 'overhead_in_row', '送り状の重さ', r.overhead_g,
          '送り状シールは全通共通なので行ごとの値は使いません (設定側の 0.5g を使用)');
      }
    }

    // ── 厚み ──
    let th = parseThicknessMm(r.thickness_mm);
    if (th.err === 'weight_unit_in_thickness_column') {
      addIssue(r.__row, sku, 'error', 'column_shift', '商品の厚み', r.thickness_mm, '厚みの列に g が入っています (列ズレの疑い)');
      th = { value: null };
    } else if (th.err) {
      addIssue(r.__row, sku, 'error', 'unparsable_thickness', '商品の厚み', r.thickness_mm, '厚みとして読めません');
      th = { value: null };
    }
    // 「商品の厚み」が空で「資材の厚み」に値がある = 列ズレ。資材の厚みは商品ごとに変わらないため
    if (th.value === null && r.material_thickness !== null && r.material_thickness !== undefined && r.material_thickness !== '') {
      const mt = parseThicknessMm(r.material_thickness);
      if (mt.value !== null) {
        addIssue(r.__row, sku, 'error', 'column_shift', '資材の厚み', r.material_thickness,
          '商品の厚みが空で資材の厚みだけ入っています — 列が1つずれた可能性 (取り込みません)');
      }
    }

    // ── 資材 ──
    const matName = normStr(r.material_name);
    let materialCode = matName ? (MATERIAL_NAME_TO_CODE[matName] || null) : null;
    let materialSource = materialCode ? 'explicit' : null;
    if (matName && !materialCode) {
      addIssue(r.__row, sku, 'warn', 'unknown_material', '資材', matName, '資材マスタに無い名前です');
    }
    const fromName = materialFromName(r.display_name);
    if (!materialCode && fromName) { materialCode = fromName; materialSource = 'name_suffix'; }
    else if (materialCode && fromName && fromName !== materialCode) {
      addIssue(r.__row, sku, 'warn', 'material_mismatch', '資材', matName,
        `資材列 (${matName}) と商品名の末尾 (${fromName}) が食い違います — 資材列を採用`);
    }

    // ── 資材の重さ (マスタと突き合わせるだけ。行の値はマスタを上書きしない) ──
    const mt = parseGrams(r.material_tare_g);
    if (materialCode && mt.value !== null) {
      const master = db.prepare('SELECT tare_weight_g FROM pm_materials WHERE material_code=?').get(materialCode);
      if (master && Number.isFinite(master.tare_weight_g) && Math.abs(master.tare_weight_g - mt.value) > 0.05) {
        addIssue(r.__row, sku, 'warn', 'material_tare_mismatch', '資材の重さ', r.material_tare_g,
          `資材マスタは ${master.tare_weight_g}g です (行の値は使いません)`);
      }
    }

    parsed.set(sku, {
      sku_code: sku,
      display_name: normStr(r.display_name) || null,
      unit_weight_g: w.value,
      thickness_mm: th.value,
      default_material_code: materialCode,
      material_source: materialSource,
    });
    if (!seenAt.has(sku)) seenAt.set(sku, r.__row);
  }

  let applied = 0;
  if (!dryRun) {
    // 既存の手修正を壊さないため、**値がある列だけ** 更新する (NULL で上書きしない)。
    const up = db.prepare(`
      INSERT INTO pm_skus (sku_code, display_name, unit_weight_g, thickness_mm,
                           default_material_code, material_source, weight_source, updated_at, updated_by)
      VALUES (@sku_code, @display_name, @unit_weight_g, @thickness_mm,
              @default_material_code, @material_source, 'measured',
              strftime('%Y-%m-%dT%H:%M:%SZ','now'), @actor)
      ON CONFLICT(sku_code) DO UPDATE SET
        display_name          = COALESCE(excluded.display_name,          pm_skus.display_name),
        unit_weight_g         = COALESCE(excluded.unit_weight_g,         pm_skus.unit_weight_g),
        thickness_mm          = COALESCE(excluded.thickness_mm,          pm_skus.thickness_mm),
        default_material_code = COALESCE(excluded.default_material_code, pm_skus.default_material_code),
        material_source       = COALESCE(excluded.material_source,       pm_skus.material_source),
        updated_at            = excluded.updated_at,
        updated_by            = excluded.updated_by
    `);
    const insIssue = db.prepare(`INSERT INTO pm_import_issues
      (import_run_id, row_no, sku_code, severity, kind, column_name, raw_value, message)
      VALUES (?,?,?,?,?,?,?,?)`);
    const tx = db.transaction(() => {
      for (const row of parsed.values()) { up.run({ ...row, actor }); applied++; }
      for (const i of issues) insIssue.run(runId, i.row_no, i.sku_code, i.severity, i.kind, i.column_name, i.raw_value, i.message);
    });
    tx();
  } else {
    const insIssue = db.prepare(`INSERT INTO pm_import_issues
      (import_run_id, row_no, sku_code, severity, kind, column_name, raw_value, message)
      VALUES (?,?,?,?,?,?,?,?)`);
    db.transaction(() => {
      for (const i of issues) insIssue.run(runId, i.row_no, i.sku_code, i.severity, i.kind, i.column_name, i.raw_value, i.message);
    })();
  }

  db.prepare(`UPDATE pm_import_runs SET applied_count=?, issue_count=?, finished_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE import_run_id=?`).run(applied, issues.length, runId);

  const summary = {
    rows: rows.length,
    readable: parsed.size,
    withWeight: [...parsed.values()].filter((r) => r.unit_weight_g !== null).length,
    withThickness: [...parsed.values()].filter((r) => r.thickness_mm !== null).length,
    withMaterial: [...parsed.values()].filter((r) => r.default_material_code).length,
    materialFromSuffix: [...parsed.values()].filter((r) => r.material_source === 'name_suffix').length,
    errors: issues.filter((i) => i.severity === 'error').length,
    warns: issues.filter((i) => i.severity === 'warn').length,
  };
  return { importRunId: runId, dryRun, applied, issues, summary };
}

function normStr(v) { return v === null || v === undefined ? '' : String(v).trim(); }
function firstNum(v) { const p = parseGrams(v); return p.value; }
