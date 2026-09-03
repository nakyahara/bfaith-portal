/**
 * FBA納品 箱詰め記録 — 突合ロジック (純関数)
 *
 * 要件 F-1: 行の正本 = Excel の SKU 行。picking-prep 側は plan_no (通常_12 等) と
 * 数量の突合状況を補強する。識別キー = FNSKU (優先) → SKU → ASIN、予定数量は一致判定項目。
 *
 * 分類:
 *   matched      … picking 側に対応行があり数量も一致
 *   qty_mismatch … 対応行はあるが数量が違う (Excel が正、警告表示して作業可)
 *   excel_only   … picking 側に対応行がない (plan_no なしで作業可、警告表示)
 * ブロック (エラー):
 *   duplicate_identity … Excel 内で識別キーが重複 (どの行か特定できないため納品回を作れない)
 */

const norm = (s) => String(s ?? '').trim().toUpperCase();

/**
 * picking-prep の planSheets からルックアップ表を作る。
 * @param planSheets picking_run_history.result.planSheets ([{slotId, sheet, label, rows:[{no,fnsku,productName,qty}]}])
 */
export function buildPickingIndex(planSheets) {
  const byFnsku = new Map();
  for (const sheet of planSheets || []) {
    for (const row of sheet.rows || []) {
      const key = norm(row.fnsku);
      if (!key) continue;
      const entry = {
        slotId: sheet.slotId, sheetName: sheet.sheet, label: sheet.label,
        no: row.no, planNo: `${sheet.label}_${row.no}`,
        productName: row.productName || null,
        qty: Number(row.qty) || 0,
      };
      if (!byFnsku.has(key)) byFnsku.set(key, []);
      byFnsku.get(key).push(entry);
    }
  }
  return { byFnsku };
}

/**
 * Excel の1シート分の SKU 行を picking 側と突合し、fbx_rows 挿入用の行 + 突合レポートを返す。
 * @returns { ok, rows, issues: [{kind, ...}] } — kind 'duplicate_identity' があれば ok:false
 */
export function matchSheet(sheetInfo, pickingIndex) {
  const rows = [];
  const issues = [];
  // 重複検出は突合と同じ識別規則で行う (Codex PR1 #3): FNSKU があれば FNSKU 単独、
  // 無ければ SKU 単独。「同じFNSKUで別SKU」の2行が両方同じ picking 行に対応する事故を防ぐ
  const seenFnsku = new Map();
  const seenSku = new Map();

  for (const r of sheetInfo.skuRows) {
    const fnsku = norm(r.fnsku);
    const sku = norm(r.sku);
    if (!fnsku && !sku) {
      issues.push({ kind: 'missing_identity', excelRow: r.row });
      continue;
    }
    if (fnsku) {
      if (seenFnsku.has(fnsku)) {
        issues.push({ kind: 'duplicate_identity', excelRow: r.row, firstRow: seenFnsku.get(fnsku), identity: `fnsku:${fnsku}` });
        continue;
      }
      seenFnsku.set(fnsku, r.row);
    } else {
      if (seenSku.has(sku)) {
        issues.push({ kind: 'duplicate_identity', excelRow: r.row, firstRow: seenSku.get(sku), identity: `sku:${sku}` });
        continue;
      }
      seenSku.set(sku, r.row);
    }

    let matchState = 'excel_only';
    let planNo = null, sourceSlotId = null, pickingRowNo = null, pickingQty = null;
    const candidates = pickingIndex.byFnsku.get(fnsku) || [];
    if (candidates.length === 1) {
      const c = candidates[0];
      planNo = c.planNo; sourceSlotId = c.slotId; pickingRowNo = c.no; pickingQty = c.qty;
      matchState = c.qty === r.plannedQty ? 'matched' : 'qty_mismatch';
      if (matchState === 'qty_mismatch') {
        issues.push({ kind: 'qty_mismatch', excelRow: r.row, fnsku: r.fnsku, excelQty: r.plannedQty, pickingQty: c.qty });
      }
    } else if (candidates.length > 1) {
      // 同一FNSKUが複数プランシートに載る場合 (P1/P2 分割等)。数量一致する候補が1つならそれ、
      // 曖昧なら plan_no なし (excel_only 扱い) + 警告 — 自動で誤った plan_no を貼らない
      const exact = candidates.filter((c) => c.qty === r.plannedQty);
      if (exact.length === 1) {
        const c = exact[0];
        planNo = c.planNo; sourceSlotId = c.slotId; pickingRowNo = c.no; pickingQty = c.qty;
        matchState = 'matched';
      } else {
        issues.push({ kind: 'ambiguous', excelRow: r.row, fnsku: r.fnsku, candidates: candidates.map((c) => c.planNo) });
      }
    } else {
      issues.push({ kind: 'excel_only', excelRow: r.row, fnsku: r.fnsku, sku: r.sku });
    }

    rows.push({
      excelRow: r.row,
      sellerSku: r.sku,
      asin: r.asin || null,
      fnsku: r.fnsku,
      excelId: r.excelId || null,
      productName: r.productName || null,
      plannedQty: r.plannedQty,
      planNo, sourceSlotId, pickingRowNo, pickingQty,
      matchState,
    });
  }

  const blocking = issues.filter((i) => i.kind === 'duplicate_identity' || i.kind === 'missing_identity');
  return { ok: blocking.length === 0, rows, issues, blocking };
}

/**
 * パーサ出力全体 (複数シート) を突合し、createRun 用の groups とサマリを作る
 */
export function matchWorkbook(parsed, planSheets) {
  const index = buildPickingIndex(planSheets);
  const groups = [];
  const allIssues = [];
  let ok = true;
  for (const sheet of parsed.sheets) {
    const m = matchSheet(sheet, index);
    if (!m.ok) ok = false;
    allIssues.push(...m.issues.map((i) => ({ ...i, sheet: sheet.sheetName })));
    groups.push({
      sheetName: sheet.sheetName,
      packingGroupId: sheet.packingGroupId,
      // 箱コードの接頭辞。Amazon 側の箱名 (P1 - B1) と揃えたいが箱名行は数式なので、
      // グループラベル (梱包グループ：1) から「G1」を組み立てる
      displayName: groupDisplayName(sheet),
      boxCountHint: sheet.totalBoxes?.value ?? null,
      maxBoxColumns: sheet.maxBoxColumns ?? null,
      structure: {
        headerRow: sheet.headerRow, headers: sheet.headers, boxColumns: sheet.boxColumns,
        totalBoxes: sheet.totalBoxes, boxNameRow: sheet.boxNameRow, dimRows: sheet.dimRows,
        boxNames: sheet.boxNames || {},   // Amazon 側の箱名 ("P1 - B3")。欠番時の対応表示・箱札に使う (PR2)
      },
      rows: m.rows,
    });
  }
  // picking 側にあって Excel に無い FNSKU (納品予定から漏れた?) も警告に載せる
  const excelFnskus = new Set(groups.flatMap((g) => g.rows.map((r) => norm(r.fnsku))));
  for (const [fnsku, entries] of index.byFnsku) {
    if (!excelFnskus.has(fnsku)) {
      for (const e of entries) {
        allIssues.push({ kind: 'picking_only', fnsku, planNo: e.planNo, sheet: e.sheetName, qty: e.qty });
      }
    }
  }
  return { ok, groups, issues: allIssues };
}

/**
 * PR2.5: 添付した Excel のシート ↔ 納品回のグループ (picking のプラン別シート) を FNSKU の重なりで対応付ける。
 * 各シートは重なりが最大のグループへ。重なり 0 は対応不可 (ただし 1 シート×1 グループならそのまま対応)。
 * 2 シートが同じグループを取り合ったら曖昧として失敗 (本社が Excel を確認)
 * @param parsedSheets parse_packlist の sheets
 * @param groups [{ id, name, fnskus: string[] }]
 * @returns { ok, assignments: [{sheetIndex, sheetName, groupId, overlap}], unassignedGroups: [id], issues, message }
 */
export function matchExcelSheetsToGroups(parsedSheets, groups) {
  const gsets = groups.map((g) => ({ id: g.id, name: g.name, set: new Set((g.fnskus || []).map(norm).filter(Boolean)) }));
  const assignments = [];
  const issues = [];
  const taken = new Map();   // groupId → sheetIndex
  parsedSheets.forEach((sheet, sheetIndex) => {
    const fn = new Set((sheet.skuRows || []).map((r) => norm(r.fnsku)).filter(Boolean));
    const scored = gsets.map((g) => ({ id: g.id, name: g.name, overlap: [...fn].filter((x) => g.set.has(x)).length }))
      .sort((a, b) => b.overlap - a.overlap);
    let best = scored[0];
    if (!best || best.overlap === 0) {
      if (parsedSheets.length === 1 && gsets.length === 1) best = { ...scored[0], overlap: 0 };
      else { issues.push({ kind: 'unmatched_sheet', sheetName: sheet.sheetName, fnskus: fn.size }); return; }
    }
    if (scored[1] && scored[1].overlap === best.overlap && best.overlap > 0) {
      issues.push({ kind: 'ambiguous_sheet', sheetName: sheet.sheetName, candidates: [best.name, scored[1].name] });
      return;
    }
    if (taken.has(best.id)) {
      issues.push({ kind: 'group_conflict', sheetName: sheet.sheetName, groupName: best.name, otherSheet: parsedSheets[taken.get(best.id)].sheetName });
      return;
    }
    taken.set(best.id, sheetIndex);
    assignments.push({ sheetIndex, sheetName: sheet.sheetName, groupId: best.id, groupName: best.name, overlap: best.overlap });
  });
  const unassignedGroups = gsets.filter((g) => !taken.has(g.id)).map((g) => g.id);
  const ok = issues.length === 0 && assignments.length === parsedSheets.length;
  return {
    ok, assignments, unassignedGroups, issues,
    message: ok ? null : `Excel のシートを納品回のグループに対応付けできません: ${issues.map((i) => `${i.sheetName} (${i.kind})`).join(', ')}`,
  };
}

export function groupDisplayName(sheet) {
  const m = /：\s*(\S+)/.exec(sheet.packingGroupLabel || '');
  const n = m ? m[1] : '1';
  return `G${n}`;
}

/** 突合サマリ (fbx_runs.match_summary 用の軽量 JSON) */
export function summarizeMatch(matchResult) {
  const counts = {};
  for (const i of matchResult.issues) counts[i.kind] = (counts[i.kind] || 0) + 1;
  return {
    ok: matchResult.ok,
    groups: matchResult.groups.map((g) => ({ sheet: g.sheetName, packingGroupId: g.packingGroupId, rows: g.rows.length, boxes: g.boxCountHint })),
    issueCounts: counts,
  };
}

/** 読み上げ用の短縮名 (packing の知見: 全角記号・鍵括弧内の付随情報を落として先頭を使う) */
export function shortNameForSpeech(name, maxLen = 24) {
  let s = String(name || '').replace(/[【\[][^】\]]*[】\]]/g, ' ').replace(/[<＜][^>＞]*[>＞]/g, ' ');
  s = s.replace(/\s+/g, ' ').trim();
  if (!s) return '商品名なし';
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}
