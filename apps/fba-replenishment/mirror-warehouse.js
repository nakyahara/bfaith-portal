/**
 * ロジザード在庫の写し (Render の mirror_logizard_stock) から、FBA 補充の計算に渡す「倉庫在庫」を組み立てる。
 * 2026-09-25 (FBA 補充の自動決定 A2b)。**影の下書きの計算にだけ使う**。画面の倉庫在庫 (warehouse_inventory =
 * 手動 CSV) は触らない (Codex A2 設計レビュー High 3: 画面を替えるのは差を観測してから)。
 *
 * 手動 CSV の取り込み (warehouse-csv.js) と同じ規則で組み立てる:
 *   Y ロケ = ブロック略称 'YYY' かロケが Y で始まる / 出荷可能 = 在庫数 − 引当数 / 期限は YYYY-MM-DD に /
 *   最終入荷日は YYYYMMDD → YYYY-MM-DD / ブロック引当順 (0 は 0、無ければ 9999)
 * 並び順も db.js の getWarehouseLocationsByCode と同じ (卸し → 通販 → その他、ブロック引当順、ロケ)。
 * 空の文字列は手動 CSV の保存 (replaceWarehouseInventory) と同じく null として扱う (ロケ数の数え方・MAX がそろう)。
 * ⚠️ 1 か所だけ手動 CSV と違う: ブロック引当順 0 は 0 のまま (手動 CSV の保存は `0 || 9999` で 9999 になる。
 *    写しは A2a で「0 を保つ」と決めた。Codex A2b 設計レビュー Medium 6)
 *
 * 🚨 おかしな値は直さずに止める (負の在庫・負の引当・引当が在庫より多い = 在庫 − 引当 が実物を上回る・下回る。
 *    Codex A2b 設計レビュー High 2)。読み飛ばした行が 1 行でもあれば止める (どの商品が欠けたか分からない)。
 *    止める = FBA に回す数を出さない = 補充見送りの側に倒れる
 */

const norm = (v) => String(v ?? '').trim().toLowerCase();

export function normalizeExpiry(s) {
  const t = String(s || '').trim();
  if (!t) return '';
  if (/^\d{8}$/.test(t)) return `${t.slice(0, 4)}-${t.slice(4, 6)}-${t.slice(6, 8)}`;
  const m = t.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (m) return `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;
  return t;
}

/** 写しと素性を 1 回の読み取りトランザクションで取る (途中で次の世代に替わっても混ざらない) */
export function readMirrorWarehouse(mdb) {
  return mdb.transaction(() => {
    const meta = mdb.prepare('SELECT captured_at, source_at, rows_read, skipped_rows, row_count FROM mirror_logizard_stock_meta WHERE id = 1').get() || null;
    const rows = mdb.prepare(`
      SELECT 商品ID, 商品名, ブロック略称, ロケ, 有効期限, 在庫数, 引当数, ロケ業務区分, 最終入荷日, ブロック引当順, captured_at
      FROM mirror_logizard_stock`).all();
    return { meta, rows };
  })();
}

/**
 * @param {object} p
 * @param {object[]} p.rows   mirror_logizard_stock の行
 * @param {object|null} p.meta  mirror_logizard_stock_meta の行
 * @param {number} p.nowMs
 * @param {number} [p.maxAgeHours=3]
 * @returns {{ ok: boolean, reasons: string[], summaryRows?: object[], locationsByCode?: (code: string) => object[],
 *            baseAtMs?: number, capturedAt?: string, sourceAt?: string, stats?: object }}
 */
export function buildWarehouseFromMirror({ rows, meta, nowMs, maxAgeHours = 3 }) {
  const reasons = [];
  if (!meta) reasons.push('写しの素性が無い (miniPC が素性を送る前の世代)');
  const sourceMs = meta?.source_at ? Date.parse(meta.source_at) : NaN;
  if (meta && !Number.isFinite(sourceMs)) reasons.push('写しの「在庫を取った時刻」が不明 (毎時ランナー以外の取り込み・古い CSV)');
  if (Number.isFinite(sourceMs)) {
    if (sourceMs > nowMs + 60e3) reasons.push(`写しの在庫を取った時刻が未来 (${meta.source_at})`);
    else if (nowMs - sourceMs > maxAgeHours * 3600e3) reasons.push(`写しが古い (在庫を取った時刻 ${meta.source_at})`);
  }
  if (!Array.isArray(rows) || rows.length === 0) reasons.push('写しの行が 0 件');
  if (meta && rows && rows.length !== meta.row_count) reasons.push(`写しの行数が素性と合わない (${rows.length} / ${meta.row_count})`);
  if (meta && meta.skipped_rows !== 0) reasons.push(`取り込みで読み飛ばした行がある (${meta.skipped_rows ?? '不明'} 行)`);
  if (meta && meta.rows_read !== null && rows && meta.rows_read !== rows.length) reasons.push(`CSV の行数と写しの行数が合わない (${meta.rows_read} / ${rows.length})`);
  const caps = new Set((rows || []).map((r) => r.captured_at));
  if (caps.size > 1) reasons.push(`写しに複数の世代が混ざっている (${caps.size})`);
  if (meta && caps.size === 1 && !caps.has(meta.captured_at)) reasons.push('写しの行と素性の世代が違う');

  const items = [];
  let bad = 0; let badSample = null;
  for (const r of rows || []) {
    const qty = r['在庫数']; const res = r['引当数'];
    const code = String(r['商品ID'] ?? '').trim();
    if (!code || !Number.isInteger(qty) || !Number.isInteger(res) || qty < 0 || res < 0 || res > qty) {
      bad++; badSample ??= `${code || '(商品IDなし)'} 在庫${qty} 引当${res}`;
      continue;
    }
    const block = r['ブロック略称'] || '';
    const location = r['ロケ'] || '';
    const rawArrival = r['最終入荷日'] || '';
    const orderRaw = r['ブロック引当順'];
    const order = (orderRaw === null || orderRaw === undefined || String(orderRaw).trim() === '') ? 9999 : Number(orderRaw);
    items.push({
      logizard_code: code,
      product_name: r['商品名'] || '',
      location, block,
      quantity: qty,
      available_qty: qty - res,
      expiry_date: normalizeExpiry(r['有効期限']),
      is_y_location: (block === 'YYY' || location.toUpperCase().startsWith('Y')) ? 1 : 0,
      last_arrival_date: rawArrival.length === 8 ? `${rawArrival.slice(0, 4)}-${rawArrival.slice(4, 6)}-${rawArrival.slice(6, 8)}` : rawArrival,
      location_biz_type: r['ロケ業務区分'] || '',
      block_alloc_order: Number.isInteger(order) ? order : 9999,
    });
  }
  if (bad) reasons.push(`在庫数・引当数がおかしい行 ${bad} 行 (例 ${badSample})`);
  if (reasons.length) return { ok: false, reasons };

  // 合計 (db.getWarehouseSummary と同じ集計。キーは LOWER(TRIM(商品ID)))
  const groups = new Map();
  for (const it of items) {
    const k = norm(it.logizard_code);
    let g = groups.get(k);
    if (!g) {
      g = { logizard_code: it.logizard_code, product_name: null, warehouse_qty: 0, warehouse_available: 0,
        y_location_qty: 0, earliest_expiry: null, last_arrival_date: null, _locs: new Set() };
      groups.set(k, g);
    }
    if (it.logizard_code < g.logizard_code) g.logizard_code = it.logizard_code;       // MIN
    if (it.product_name && (g.product_name === null || it.product_name > g.product_name)) g.product_name = it.product_name;   // MAX (空は数えない)
    if (it.is_y_location) g.y_location_qty += it.quantity;
    else { g.warehouse_qty += it.quantity; g.warehouse_available += it.available_qty; }
    if (it.expiry_date && (g.earliest_expiry === null || it.expiry_date < g.earliest_expiry)) g.earliest_expiry = it.expiry_date;
    if (it.last_arrival_date && (g.last_arrival_date === null || it.last_arrival_date > g.last_arrival_date)) g.last_arrival_date = it.last_arrival_date;
    if (it.location) g._locs.add(it.location);   // COUNT(DISTINCT location) は空 (null) を数えない
  }
  const summaryRows = [...groups.values()].map(({ _locs, ...g }) => ({ ...g, location_count: _locs.size }))
    .sort((a, b) => (a.logizard_code < b.logizard_code ? -1 : a.logizard_code > b.logizard_code ? 1 : 0));

  // ロケ (db.getWarehouseLocationsByCode と同じ条件・並び)
  const bizRank = (t) => (t === '卸し' ? 0 : t === '通販' ? 1 : 2);
  const byCode = new Map();
  for (const it of items) {
    if (it.is_y_location || !(it.available_qty > 0)) continue;
    const k = norm(it.logizard_code);
    if (!byCode.has(k)) byCode.set(k, []);
    byCode.get(k).push({
      location: it.location || null, block: it.block || null, available_qty: it.available_qty,
      location_biz_type: it.location_biz_type || null, block_alloc_order: it.block_alloc_order, expiry_date: it.expiry_date || null,
    });
  }
  for (const list of byCode.values()) {
    list.sort((a, b) => bizRank(a.location_biz_type) - bizRank(b.location_biz_type)
      || a.block_alloc_order - b.block_alloc_order
      || ((a.location ?? '') < (b.location ?? '') ? -1 : (a.location ?? '') > (b.location ?? '') ? 1 : 0));
  }
  const locationsByCode = (code) => (byCode.get(norm(code)) || []).map((l) => ({ ...l }));

  return {
    ok: true, reasons: [], summaryRows, locationsByCode,
    baseAtMs: sourceMs, capturedAt: meta.captured_at, sourceAt: meta.source_at,
    stats: { rows: items.length, codes: summaryRows.length },
  };
}

/**
 * 写しから作った倉庫在庫と、画面の倉庫在庫 (手動 CSV) の差。構成品ごとの出荷可能在庫で比べる。
 * 増えた・減った・絶対差・片方にしか無い を分ける (符号つきの合計だけだと相殺して見えない。Codex A2b Low 8)
 */
export function diffWarehouse(mirrorSummary, manualSummary, { top = 20 } = {}) {
  const a = new Map(mirrorSummary.map((r) => [norm(r.logizard_code), r.warehouse_available || 0]));
  const b = new Map(manualSummary.map((r) => [norm(r.logizard_code), r.warehouse_available || 0]));
  let plus = 0, minus = 0, abs = 0, changed = 0, onlyMirror = 0, onlyManual = 0;
  const diffs = [];
  for (const k of new Set([...a.keys(), ...b.keys()])) {
    const x = a.get(k), y = b.get(k);
    if (x === undefined) onlyManual++;
    if (y === undefined) onlyMirror++;
    const d = (x ?? 0) - (y ?? 0);
    if (d === 0) continue;
    changed++; abs += Math.abs(d);
    if (d > 0) plus += d; else minus += -d;
    diffs.push({ code: k, mirror: x ?? null, manual: y ?? null, diff: d });
  }
  diffs.sort((p, q) => Math.abs(q.diff) - Math.abs(p.diff) || (p.code < q.code ? -1 : 1));
  return { codes_changed: changed, plus, minus, abs, only_mirror: onlyMirror, only_manual: onlyManual, top: diffs.slice(0, top) };
}
