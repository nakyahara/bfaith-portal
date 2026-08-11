/**
 * picking 業務ロジック — CS03002 (ピッキングリストCSV) の解析と取込。
 *
 * CS03002 = ロジザードZERO カンタン引当 [PS03/FS03_07] 実行時に出力されるCSV。
 *   - Shift-JIS / 全列ダブルクォート / 150列 / 1行 = 出荷伝票 × SKU の明細
 *   - 1ファイル = 1引当実行 = トータルピッキングバッチ番号 (TB…) 1つ
 * 列仕様の正本: AI_reference/システム設計/ピッキング支援システム_要件定義_20260811.md §4
 *
 * 列は名前で引く (ロジザードの列追加・順序変更に耐える)。必須列が無ければ fail-closed。
 */
import { parseCsv, decodeCp932 } from '../packing-dispatch/csv.js';
import { getDB, getBatchByTbNo, utcNow, jstToday } from './db.js';
import { suggestPatterns } from './patterns.js';

/** 業務エラー。router が status + message に変換する。 */
export class PkError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// CS03002 の必須列 (名前で参照する列のみ列挙。他の列は保持しない)
const REQUIRED_COLUMNS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];

/**
 * CS03002 のバッファを解析して取込プレビュー用の構造を返す。DB には触れない。
 * @returns {{
 *   tbNo, instructDate, invoiceSoft, deliveryMethod, composition,
 *   lines: [{location, block, sku, productName, barcode, qty}],   // 集約済み・ロケ昇順
 *   slipLines: [{slipNo, pickingNo, neSlipNo, sku, qty, location}],
 *   slipCount, totalQty, suggestions: string[]
 * }}
 * @throws {PkError} 400 — 形式不正 (列欠落・数量不正・複数TB等)
 */
export function parseCs03002(buffer) {
  if (!buffer || buffer.length === 0) throw new PkError(400, 'empty_file', 'ファイルが空です');
  const state = {};
  const rows = parseCsv(decodeCp932(buffer), state);
  if (state.unclosedQuote) {
    throw new PkError(400, 'broken_csv', 'CSVの引用符が閉じていません (ファイルが壊れている可能性があります)');
  }
  if (rows.length === 0) throw new PkError(400, 'no_rows', 'データ行がありません');

  const header = rows[0];
  const col = {};
  header.forEach((name, i) => { if (!(name in col)) col[name] = i; });
  const missing = REQUIRED_COLUMNS.filter((name) => !(name in col));
  if (missing.length > 0) {
    throw new PkError(400, 'missing_columns',
      `必須列がありません: ${missing.join(', ')}。CS03002 (ピッキングリストCSV) か確認してください`);
  }

  const dataRows = rows.slice(1).filter((r) => !(r.length === 1 && r[0] === ''));
  if (dataRows.length === 0) throw new PkError(400, 'no_rows', 'データ行がありません');
  const badRows = [];
  dataRows.forEach((r, i) => { if (r.length !== header.length) badRows.push(i + 2); });
  if (badRows.length > 0) {
    throw new PkError(400, 'ragged_rows',
      `列数が揃わない行があります (行 ${badRows.slice(0, 5).join(', ')}${badRows.length > 5 ? ' …' : ''})`);
  }

  const slipLines = [];
  for (const [i, r] of dataRows.entries()) {
    const rowNo = i + 2;
    const get = (name) => (r[col[name]] ?? '').trim();
    const qty = Number(get('出荷指示数'));
    if (!Number.isInteger(qty) || qty <= 0) {
      throw new PkError(400, 'bad_qty', `行${rowNo}: 出荷指示数「${get('出荷指示数')}」が正の整数ではありません`);
    }
    if (!get('ロケーション')) throw new PkError(400, 'no_location', `行${rowNo}: ロケーションが空です`);
    if (!get('商品ID')) throw new PkError(400, 'no_sku', `行${rowNo}: 商品IDが空です`);
    if (!get('出荷伝票NO')) throw new PkError(400, 'no_slip', `行${rowNo}: 出荷伝票NOが空です`);
    slipLines.push({
      tbNo: get('トータルピッキングバッチ番号'),
      instructDate: get('出荷指示日'),
      block: get('ブロック略称'),
      location: get('ロケーション'),
      sku: get('商品ID'),
      productName: get('商品名'),
      barcode: get('バーコード'),
      qty,
      pickingNo: get('ピッキングNO'),
      slipNo: get('出荷伝票NO'),
      neSlipNo: get('荷主出荷NO'),
      invoiceSoft: get('送り状発行ソフト名'),
      deliveryMethod: get('配送方法名'),
    });
  }

  // 1ファイル = 1引当 = 1TB が前提。複数TBが混ざるケース (まとめ引当の実データ) は
  // 未採取のため fail-closed (要件§10)。採取できたら分割取込に対応する
  const tbNos = [...new Set(slipLines.map((l) => l.tbNo).filter(Boolean))];
  if (tbNos.length === 0) {
    throw new PkError(400, 'no_tb_no', 'トータルピッキングバッチ番号が入っていません');
  }
  if (tbNos.length > 1) {
    throw new PkError(400, 'multiple_tb_no',
      `複数のトータルピッキングバッチ番号が含まれています (${tbNos.join(', ')})。1引当=1ファイルで出力してください`);
  }

  // ロケーション×SKU で集約 (トータルピッキング)。表示順 = ロケーション昇順 → SKU昇順。
  // 紙のトータルピッキングリストPDFと同順である前提 (PR1完了時に実物と突合する — 実装計画§10)
  const aggMap = new Map();
  for (const l of slipLines) {
    const key = `${l.location}\u0000${l.sku}`;
    const cur = aggMap.get(key);
    if (cur) {
      cur.qty += l.qty;
    } else {
      aggMap.set(key, {
        location: l.location, block: l.block, sku: l.sku,
        productName: l.productName, barcode: l.barcode, qty: l.qty,
      });
    }
  }
  const lines = [...aggMap.values()].sort((a, b) =>
    a.location < b.location ? -1 : a.location > b.location ? 1 :
    a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0);

  const composition = classifyComposition(slipLines);
  const first = slipLines[0];
  const preview = {
    tbNo: tbNos[0],
    instructDate: formatDate8(first.instructDate),
    invoiceSoft: first.invoiceSoft,
    deliveryMethod: first.deliveryMethod,
    composition,
    lines,
    slipLines,
    slipCount: new Set(slipLines.map((l) => l.slipNo)).size,
    totalQty: slipLines.reduce((s, l) => s + l.qty, 0),
  };
  preview.suggestions = suggestPatterns({
    invoiceSoft: preview.invoiceSoft,
    deliveryMethod: preview.deliveryMethod,
    composition,
  });
  return preview;
}

/**
 * 数量構成の判定 (引当パターンの3分類と同じ軸)。
 *   単品        = 全伝票が 1明細 × 1個
 *   1SKU複数個  = 全伝票が 1SKU (数量2以上を含む)
 *   アソート    = 全伝票が 複数SKU
 *   混在        = 上記が混ざる (まとめ引当)
 */
export function classifyComposition(slipLines) {
  const bySlip = new Map();
  for (const l of slipLines) {
    if (!bySlip.has(l.slipNo)) bySlip.set(l.slipNo, []);
    bySlip.get(l.slipNo).push(l);
  }
  const kinds = new Set();
  for (const lines of bySlip.values()) {
    const skus = new Set(lines.map((l) => l.sku));
    const totalQty = lines.reduce((s, l) => s + l.qty, 0);
    if (skus.size >= 2) kinds.add('アソート');
    else if (totalQty >= 2) kinds.add('1SKU複数個');
    else kinds.add('単品');
  }
  if (kinds.size === 1) return [...kinds][0];
  return '混在';
}

/** '20260811' → '2026-08-11'。形式外はそのまま返す (表示用なので落とさない)。 */
function formatDate8(s) {
  return /^\d{8}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}` : s;
}

/** ロケーション表示形式 (確定 2026-08-11): ブロック + 8桁を 3-3-2 区切り。例 P3FB-002-016-04 */
export function formatLocation(block, location) {
  const digits = /^\d{8}$/.test(location)
    ? `${location.slice(0, 3)}-${location.slice(3, 6)}-${location.slice(6, 8)}`
    : location;
  return block ? `${block}-${digits}` : digits;
}

/**
 * 取込の確定。tb_no が冪等キー。
 *   - 既存なし → 新規作成
 *   - 既存あり (ready・overwrite=true) → 明細を入れ替えて更新 (取り込み直し)
 *   - 既存あり (ready・overwrite なし) → 409 duplicate (画面が上書き確認を出す)
 *   - 既存あり (ready 以外) → 409 already_started (作業開始後の上書きは不可)
 * @returns {{batchId, replaced}}
 */
export function importBatch(preview, { hikiateClass, folderName, overwrite }, actor) {
  if (!hikiateClass || !String(hikiateClass).trim()) {
    throw new PkError(400, 'no_class', '引当分類を選択してください');
  }
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const existing = getBatchByTbNo(preview.tbNo);
    let batchId;
    let replaced = false;
    if (existing) {
      if (existing.status !== 'ready') {
        throw new PkError(409, 'already_started',
          `バッチ ${preview.tbNo} は既に作業が始まっています (${existing.status})。取り込み直しはできません`);
      }
      if (!overwrite) {
        throw new PkError(409, 'duplicate',
          `バッチ ${preview.tbNo} は取込済みです (${existing.hikiate_class} / ${existing.line_count}明細)`);
      }
      db.prepare('DELETE FROM pk_lines WHERE batch_id = ?').run(existing.id);
      db.prepare('DELETE FROM pk_slip_lines WHERE batch_id = ?').run(existing.id);
      db.prepare(`
        UPDATE pk_batches SET hikiate_class=?, folder_name=?, work_date=?, instruct_date=?,
          composition=?, delivery_method=?, invoice_soft=?,
          line_count=?, slip_count=?, total_qty=?, imported_by=?, updated_at=?
        WHERE id=?
      `).run(String(hikiateClass).trim(), folderName || null, jstToday(), preview.instructDate,
        preview.composition, preview.deliveryMethod, preview.invoiceSoft,
        preview.lines.length, preview.slipCount, preview.totalQty, actor, now, existing.id);
      batchId = existing.id;
      replaced = true;
    } else {
      const info = db.prepare(`
        INSERT INTO pk_batches
          (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
           delivery_method, invoice_soft, line_count, slip_count, total_qty,
           status, imported_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?)
      `).run(preview.tbNo, String(hikiateClass).trim(), folderName || null, jstToday(),
        preview.instructDate, preview.composition, preview.deliveryMethod, preview.invoiceSoft,
        preview.lines.length, preview.slipCount, preview.totalQty, actor, now, now);
      batchId = Number(info.lastInsertRowid);
    }

    const insLine = db.prepare(`
      INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    preview.lines.forEach((l, i) => {
      insLine.run(batchId, i + 1, l.location, l.block || null, l.sku,
        l.productName || null, l.barcode || null, l.qty);
    });

    const insSlip = db.prepare(`
      INSERT INTO pk_slip_lines (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const l of preview.slipLines) {
      insSlip.run(batchId, l.slipNo, l.pickingNo || null, l.neSlipNo || null, l.sku, l.qty, l.location);
    }
    return { batchId, replaced };
  })();
}
