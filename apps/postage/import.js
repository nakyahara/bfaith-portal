/**
 * 重量マスタの取込 (Excel『定形外の重さ.xlsx』/ CSV)。
 *
 * 取込は「読んで入れる」だけにしない。**入れる前に検証して、弾いた行を残す**。
 * 実データ (2026-08-30 時点 800行) には次が混ざっていた:
 *   - 列ズレ 8行 …… 「送り状の重さ」「資材の厚み」列に `0.1cm` が入り「商品の厚み」が空
 *   - 商品コードの重複 29件
 *   - 単位つきの文字列 (`5g` `0.1cm`) と数値が同じ列に混在
 * これらを黙って取り込むと、静かに間違った料金が出る。
 * 2026-09-05 の表では「資材の厚み」が資材ごとに系統的に埋まった (茶封筒 0.1cm / 白プチ 0.2cm)。
 * この列は資材の属性なので、資材ごとに集めてマスタと突き合わせる (行ごとには使わない)。
 *
 * 採否は **SKU 単位** で決める (Codex R1 P1)。
 *   - 1行でも要修正があれば、その商品は丸ごと取り込まない。
 *     「重さは読めなかったが厚みは読めた」を部分的に入れると、旧重量 + 新厚みという
 *     出所の違う組み合わせができ、静かに安い区分へ倒れる。
 *   - 同じ商品コードが複数行あるとき、判定に使う値が1つでも違えば取り込まない。
 *     ファイル順で先勝ちさせない。
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

// 商品コードだけを必須にすると、列名を変えたり別のCSVを選んでも「成功」してしまい、
// 中身は何も入っていないのに更新できたように見える
const REQUIRED_HEADERS = ['商品コード', '商品名', '商品の重さ', '商品の厚み'];

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
 * ただし「みなした」ことは呼び出し側に返す (assumedCm) — 表の書き方が変わると 3mm が 30mm になるため。
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

// 想定外に巨大なファイルで固まらないための上限
const MAX_ROWS = 20000;
// 資材の厚みを表から自動で入れる条件: この行数以上で同じ値 / 封筒〜プチ袋として妥当な範囲 (mm)
export const MATERIAL_THICKNESS_MIN_ROWS = 10;
export const MATERIAL_THICKNESS_RANGE_MM = [0.5, 10];

/** xlsx / csv → [{__row, sku_code: …}, …] */
async function readRows(filePath) {
  const wb = new ExcelJS.Workbook();
  const isCsv = /\.csv$/i.test(filePath);
  if (isCsv) await wb.csv.readFile(filePath);
  else await wb.xlsx.readFile(filePath);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('シートが見つかりません');
  if (ws.rowCount > MAX_ROWS) throw new Error(`行が多すぎます (${ws.rowCount}行 / 上限 ${MAX_ROWS}行)`);

  const headers = [];
  ws.getRow(1).eachCell({ includeEmpty: true }, (cell, col) => {
    headers[col] = HEADER_MAP[normalizeHeader(cell.value)] || null;
  });
  const missing = REQUIRED_HEADERS.filter((h) => !headers.includes(HEADER_MAP[h]));
  if (missing.length) throw new Error(`必須の列が見つかりません: ${missing.join(', ')}`);
  // 同じ意味の列が2つあると、どちらが採用されたか分からないまま取り込まれる
  const seen = new Set();
  for (const k of headers) {
    if (!k) continue;
    if (seen.has(k)) throw new Error(`同じ意味の列が2つあります: ${Object.keys(HEADER_MAP).find((n) => HEADER_MAP[n] === k)}`);
    seen.add(k);
  }

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
 * @returns {{importRunId, dryRun, applied, issues, summary}}
 */
export async function importWeightFile(filePath, {
  dryRun = true, actor = 'system', materialThicknessMinRows = MATERIAL_THICKNESS_MIN_ROWS,
} = {}) {
  if (!fs.existsSync(filePath)) throw new Error(`ファイルがありません: ${filePath}`);
  const db = getDB();
  const { rows, sheetName } = await readRows(filePath);

  const runId = db.prepare(`INSERT INTO pm_import_runs
    (source_name, sheet_name, row_count, dry_run, imported_by) VALUES (?,?,?,?,?)`)
    .run(baseName(filePath), sheetName, rows.length, dryRun ? 1 : 0, actor).lastInsertRowid;

  const issues = [];
  const addIssue = (row, sku, severity, kind, column, raw, message) =>
    issues.push({ row_no: row, sku_code: sku, severity, kind, column_name: column, raw_value: raw == null ? null : String(raw), message });

  // ── 1周目: 行ごとに読んで検証する。ここでは採否を決めない ──
  const bySku = new Map();   // sku_code → [{row, rec, hasError}]
  for (const r of rows) {
    const sku = normalizeSku(r.sku_code);
    if (!sku) { addIssue(r.__row, null, 'error', 'missing_sku_code', '商品コード', r.sku_code, '商品コードが空'); continue; }
    let hasError = false;
    const err = (...a) => { hasError = true; addIssue(...a); };

    // ── 商品の重さ ──
    const w = parseGrams(r.unit_weight_g);
    if (w.err === 'length_unit_in_weight_column') {
      err(r.__row, sku, 'error', 'column_shift', '商品の重さ', r.unit_weight_g, '重さの列に cm が入っています (列ズレの疑い)');
    } else if (w.err) {
      err(r.__row, sku, 'error', 'unparsable_weight', '商品の重さ', r.unit_weight_g, '重さとして読めません');
    } else if (w.value !== null && w.value <= 0) {
      // 欠測は NULL という設計。0 を実測値として入れると、境界から遠い軽量品として確定してしまう
      err(r.__row, sku, 'error', 'zero_weight', '商品の重さ', r.unit_weight_g,
        '重さ 0 は入力ミスとして扱います (未計測なら空欄にしてください)');
    }

    // ── 送り状の重さ列 (本来は 1 通あたりの固定値。行ごとの値は使わない) ──
    if (r.overhead_g !== null && r.overhead_g !== undefined && r.overhead_g !== '') {
      const oh = parseGrams(r.overhead_g);
      if (oh.err === 'length_unit_in_weight_column') {
        err(r.__row, sku, 'error', 'column_shift', '送り状の重さ', r.overhead_g,
          '送り状の「重さ」に cm が入っています — 商品の厚みが1列ずれて入った可能性');
      } else {
        addIssue(r.__row, sku, 'warn', 'overhead_in_row', '送り状の重さ', r.overhead_g,
          '送り状シールは全通共通なので行ごとの値は使いません (設定側の値を使用)');
      }
    }

    // ── 厚み ──
    let th = parseThicknessMm(r.thickness_mm);
    if (th.err === 'weight_unit_in_thickness_column') {
      err(r.__row, sku, 'error', 'column_shift', '商品の厚み', r.thickness_mm, '厚みの列に g が入っています (列ズレの疑い)');
      th = { value: null };
    } else if (th.err) {
      err(r.__row, sku, 'error', 'unparsable_thickness', '商品の厚み', r.thickness_mm, '厚みとして読めません');
      th = { value: null };
    } else if (th.value !== null && th.value <= 0) {
      err(r.__row, sku, 'error', 'zero_thickness', '商品の厚み', r.thickness_mm,
        '厚み 0 は入力ミスとして扱います (未計測なら空欄にしてください)');
      th = { value: null };
    } else if (th.value !== null && th.assumedCm) {
      addIssue(r.__row, sku, 'warn', 'thickness_unit_assumed', '商品の厚み', r.thickness_mm,
        `単位がないので cm とみなしました (${th.value}mm)。mm で書くなら "3mm" のように単位をつけてください`);
    }
    // ── 資材の厚み (資材の属性。行ごとの値でマスタを上書きしない。資材ごとに集めて後で突き合わせる) ──
    // 以前は「商品の厚みが空で資材の厚みだけある」を列ズレ扱いにしていたが、資材の厚みが資材ごとに
    // 系統的に埋まった表 (2026-09-05) で 26 件が誤検知になった。列ズレの本当の目印は
    // 「送り状の重さに cm」なので、そちらだけで弾く
    let matTh = { value: null };
    if (r.material_thickness !== null && r.material_thickness !== undefined && r.material_thickness !== '') {
      matTh = parseThicknessMm(r.material_thickness);
      if (matTh.err === 'weight_unit_in_thickness_column') {
        err(r.__row, sku, 'error', 'column_shift', '資材の厚み', r.material_thickness, '資材の厚みの列に g が入っています (列ズレの疑い)');
        matTh = { value: null };
      } else if (matTh.err || (matTh.value !== null && matTh.value <= 0)) {
        addIssue(r.__row, sku, 'warn', 'unparsable_material_thickness', '資材の厚み', r.material_thickness,
          '資材の厚みとして読めません (この列は資材マスタとの突き合わせにだけ使います)');
        matTh = { value: null };
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

    // ── 資材の重さ (マスタと突き合わせるだけ。行の値でマスタを上書きしない) ──
    const mt = parseGrams(r.material_tare_g);
    if (materialCode && mt.value !== null) {
      const master = db.prepare('SELECT tare_weight_g FROM pm_materials WHERE material_code=?').get(materialCode);
      if (master && Number.isFinite(master.tare_weight_g) && Math.abs(master.tare_weight_g - mt.value) > 0.05) {
        addIssue(r.__row, sku, 'warn', 'material_tare_mismatch', '資材の重さ', r.material_tare_g,
          `資材マスタは ${master.tare_weight_g}g です (行の値は使いません)`);
      }
    }

    const rec = {
      sku_code: sku,
      display_name: normStr(r.display_name) || null,
      unit_weight_g: w.value,
      thickness_mm: th.value,
      default_material_code: materialCode,
      material_source: materialSource,
      material_thickness_mm: matTh.value,   // 集計にだけ使う。pm_skus には入れない
    };
    if (!bySku.has(sku)) bySku.set(sku, []);
    bySku.get(sku).push({ row: r.__row, rec, hasError });
  }


  // ── 2周目: SKU 単位で採否を決める ──
  const parsed = new Map();
  for (const [sku, entries] of bySku) {
    const bad = entries.filter((e) => e.hasError);
    if (bad.length) {
      addIssue(bad[0].row, sku, 'error', 'row_rejected', null, null,
        `この商品は取り込みません (要修正の行: ${bad.map((e) => `${e.row}行目`).join(', ')})`);
      continue;
    }
    if (entries.length > 1) {
      // 判定に使う値がすべて一致していれば通す。1つでも違えばどちらが正か決まらない
      const key = (e) => JSON.stringify([e.rec.unit_weight_g, e.rec.thickness_mm, e.rec.default_material_code]);
      const at = entries.map((e) => `${e.row}行目`).join(', ');
      if (!entries.every((e) => key(e) === key(entries[0]))) {
        addIssue(entries[0].row, sku, 'error', 'duplicate_sku', '商品コード', sku,
          `同じ商品コードで中身が違う行があります (${at}) — どちらが正か決まるまで取り込みません`);
        continue;
      }
      addIssue(entries[0].row, sku, 'warn', 'duplicate_sku', '商品コード', sku, `同じ内容の重複 (${at})`);
    }
    parsed.set(sku, entries[0].rec);
  }

  // ── 資材の厚み: 表の値を資材ごとに集め、マスタと突き合わせる ──
  // マスタに値がある → 違う値が表にあれば注意 (行の値は使わない)
  // マスタが空     → 表の中で揃っていて、行数が十分で、封筒として妥当な値ならその値を入れる。
  //                  それ以外は人に返す (1 セルの誤入力が全商品の判定に効く列なので、自動で入れる条件は厳しめ)
  // **採用した SKU の行だけ** 数える (要修正・重複で見送った SKU の行を根拠にしない。Codex R1 P1)
  const thicknessByMaterial = new Map();   // material_code → Map(mm → 行数)
  for (const [sku, entries] of bySku) {
    if (!parsed.has(sku)) continue;
    for (const e of entries) {
      if (!e.rec.default_material_code || e.rec.material_thickness_mm === null) continue;
      const m = thicknessByMaterial.get(e.rec.default_material_code) || new Map();
      m.set(e.rec.material_thickness_mm, (m.get(e.rec.material_thickness_mm) || 0) + 1);
      thicknessByMaterial.set(e.rec.default_material_code, m);
    }
  }
  const materialThicknessToFill = [];   // [{material_code, mm}]
  for (const [code, dist] of thicknessByMaterial) {
    const master = db.prepare('SELECT display_name, thickness_mm FROM pm_materials WHERE material_code=?').get(code);
    if (!master) continue;
    const name = master.display_name || code;
    const sorted = [...dist.entries()].sort((a, b) => b[1] - a[1]);
    const distText = sorted.map(([mm, n]) => `${mm}mm×${n}行`).join(' / ');
    if (Number.isFinite(master.thickness_mm)) {
      for (const [mm, n] of sorted) {
        if (Math.abs(mm - master.thickness_mm) > 0.05) {
          addIssue(null, null, 'warn', 'material_thickness_mismatch', '資材の厚み', `${mm}mm`,
            `資材「${name}」の厚みはマスタでは ${master.thickness_mm}mm です (表では ${mm}mm が ${n}行。行の値は使いません)`);
        }
      }
    } else if (sorted.length > 1) {
      addIssue(null, null, 'warn', 'material_thickness_ambiguous', '資材の厚み', distText,
        `資材「${name}」の厚みが表の中で揃っていません (${distText})。資材マスタに手で入れてください`);
    } else {
      const [mm, n] = sorted[0];
      if (n < materialThicknessMinRows) {
        addIssue(null, null, 'warn', 'material_thickness_candidate', '資材の厚み', `${mm}mm`,
          `資材「${name}」の厚みがマスタに無く、表では ${mm}mm ですが ${n}行だけなので自動では入れません (${materialThicknessMinRows}行以上で入れます)。資材マスタに手で入れてください`);
      } else if (mm < MATERIAL_THICKNESS_RANGE_MM[0] || mm > MATERIAL_THICKNESS_RANGE_MM[1]) {
        addIssue(null, null, 'warn', 'material_thickness_candidate', '資材の厚み', `${mm}mm`,
          `資材「${name}」の厚み ${mm}mm は資材の厚みとして不自然です (${MATERIAL_THICKNESS_RANGE_MM[0]}〜${MATERIAL_THICKNESS_RANGE_MM[1]}mm の外)。自動では入れません`);
      } else {
        materialThicknessToFill.push({ material_code: code, mm });
        addIssue(null, null, 'warn', 'material_thickness_fill', '資材の厚み', `${mm}mm`,
          `資材「${name}」の厚みがマスタに無いので、表の値 ${mm}mm (${n}行で一致) を${dryRun ? '取り込むときに入れます' : '入れました'}`);
      }
    }
  }

  const insIssue = db.prepare(`INSERT INTO pm_import_issues
    (import_run_id, row_no, sku_code, severity, kind, column_name, raw_value, message)
    VALUES (?,?,?,?,?,?,?,?)`);
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
    // 資材の厚みは **空のときだけ** 入れる (人が入れた値を表で上書きしない)
    const fillMat = db.prepare(`UPDATE pm_materials SET thickness_mm=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'), updated_by=?
      WHERE material_code=? AND thickness_mm IS NULL`);
    db.transaction(() => {
      for (const row of parsed.values()) {
        const { material_thickness_mm, ...rec } = row;
        up.run({ ...rec, actor }); applied++;
      }
      for (const f of materialThicknessToFill) fillMat.run(f.mm, actor, f.material_code);
      for (const i of issues) insIssue.run(runId, i.row_no, i.sku_code, i.severity, i.kind, i.column_name, i.raw_value, i.message);
    })();
  } else {
    db.transaction(() => {
      for (const i of issues) insIssue.run(runId, i.row_no, i.sku_code, i.severity, i.kind, i.column_name, i.raw_value, i.message);
    })();
  }

  db.prepare(`UPDATE pm_import_runs SET applied_count=?, issue_count=?, finished_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE import_run_id=?`).run(applied, issues.length, runId);

  const summary = {
    rows: rows.length,
    readable: parsed.size,
    rejected: bySku.size - parsed.size,
    withWeight: [...parsed.values()].filter((r) => r.unit_weight_g !== null).length,
    withThickness: [...parsed.values()].filter((r) => r.thickness_mm !== null).length,
    withMaterial: [...parsed.values()].filter((r) => r.default_material_code).length,
    materialFromSuffix: [...parsed.values()].filter((r) => r.material_source === 'name_suffix').length,
    materialThicknessFilled: dryRun ? 0 : materialThicknessToFill.length,
    errors: issues.filter((i) => i.severity === 'error').length,
    warns: issues.filter((i) => i.severity === 'warn').length,
  };
  // 1件も取り込めないのに「成功」と見せない (違うファイルを選んだときに気づけない)
  if (parsed.size === 0) {
    throw new Error(`取り込める商品がありませんでした (${rows.length}行を読んで全て見送り)。列の名前や中身を確かめてください`);
  }
  return { importRunId: runId, dryRun, applied, issues, summary };
}

function normStr(v) { return v === null || v === undefined ? '' : String(v).trim(); }
function baseName(p) { return String(p).split(/[/\\]/).pop(); }
