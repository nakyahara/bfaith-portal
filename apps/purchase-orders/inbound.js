/**
 * purchase-orders ロジザード入庫実績の取込と突合 (P14)
 *
 * データソース: ロジザードからエクスポートしている「NE仕入れ取込用データ」CSV (Shift-JIS)。
 * 現在NE仕入取込に使っているファイルと同一 = 新しい運用は増えない (要件v8 F-5)。
 *
 * 行ID: CSVに安定した明細番号が無いため、line_key = sha1(型番|良品数|不良品数|単価) + 同一内容の出現順。
 * 訂正版: 同一伝票NOの再取込で「消えた/変わった行」は superseded=1 とし新行を追加する。
 * superseded 行に有効割当が残っていれば「訂正競合」(人間が逆仕訳→再割当で解消、解消まで警告)。
 * 割当 (消込) は appendPoItemEvent(source='logizard', inboundItemId) 経由のみ。
 * 容量 (割当合計≤良品数) はDBトリガ trg_po_events_inbound_capacity が最終防衛する。
 */
import { createHash } from 'crypto';
import iconv from 'iconv-lite';
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { getSetting, jstToday, isYmd, audit } from './ledger.js';

const nowIso = () => new Date().toISOString();

/** CSV 1ファイルのパース (router.js の parseCsv と同等の引用符対応ミニ実装) */
export function parseCsv(text) {
  const rows = [];
  let row = [], field = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else q = false;
      } else field += c;
    } else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      row.push(field); field = '';
      if (row.some(v => v !== '')) rows.push(row);
      row = [];
    } else field += c;
  }
  row.push(field);
  if (row.some(v => v !== '')) rows.push(row);
  if (q) throw new Error('CSVの引用符が閉じていません (ファイル破損の可能性)');
  return rows;
}

const trimS = v => String(v == null ? '' : v).trim();
const intOrNull = v => {
  const s = trimS(v).replace(/,/g, '');
  if (s === '') return null;
  const n = Number(s);
  return Number.isInteger(n) ? n : null;
};

/** Shift-JIS/UTF-8 自動判別デコード (既存P4の方式: 厳密UTF-8検証→失敗時のみSJIS)。
 *  実データはWindows由来のためCP932を明示 (丸数字・髙・﨑等のNEC/IBM拡張を正しく読む) */
export function decodeCsvBuffer(buf) {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buf);
  } catch {
    return iconv.decode(buf, 'cp932');
  }
}

// 「NE仕入れ取込用データ」の見出し (2026-07-10 サンプル確認済)。入庫日はカスタム追加予定のため任意
const REQUIRED_COLS = ['伝票NO', '型番', '取引先ID', '仕入単価', '良品数'];
const DATE_COL_CANDIDATES = ['入庫日', '入庫日付', '入荷日'];

function lineContentHash(r) {
  return createHash('sha1').update([r.productCode, r.goodQty, r.defectiveQty, r.unitCost == null ? '' : r.unitCost].join('|')).digest('hex').slice(0, 16);
}

/**
 * 入庫CSVを取り込む。冪等: 同一ファイル(ハッシュ)は再取込しない。同一伝票NOの再出力は差分適用
 * (不変行=そのまま / 新行=追加 / 消えた行=superseded。有効割当つきのsupersededは競合として返す)。
 * @returns {batchId, receipts, newItems, unchangedItems, supersededItems, conflicts, warnings, alreadyImported}
 */
export function importInboundCsv({ buffer, filename, actor = null }) {
  const db = getDB();
  const fileHash = createHash('sha256').update(buffer).digest('hex');
  const text = decodeCsvBuffer(buffer);
  const rows = parseCsv(text);
  if (rows.length < 2) throw new Error('CSVにデータ行がありません');
  const header = rows[0].map(trimS);
  const col = name => header.indexOf(name);
  for (const c of REQUIRED_COLS) {
    if (col(c) === -1) throw new Error(`必要な列が見つかりません: ${c} (「NE仕入れ取込用データ」のCSVを入れてください)`);
  }
  const iSlip = col('伝票NO'), iCode = col('型番'), iSup = col('取引先ID'), iCost = col('仕入単価');
  const iGood = col('良品数'), iBad = col('不良品数');
  const iDate = DATE_COL_CANDIDATES.map(col).find(x => x !== -1);
  const warnings = [];

  // 伝票NOごとに行をグルーピング (line_key = 内容ハッシュ + 同一内容の出現順)。
  // 識別・数量・単価の不正は「ファイル全体を拒否」する (Codex P14-R1 High-2:
  // 不正行だけスキップして訂正版を適用すると、正常な旧行を「消えた」と誤認してsupersede+偽競合を作るため)
  const errors = [];
  const bySlip = new Map();
  for (let n = 1; n < rows.length; n++) {
    const r = rows[n];
    const maxIdx = Math.max(iSlip, iCode, iSup, iCost, iGood, iBad === -1 ? 0 : iBad);
    if (r.length <= maxIdx) { errors.push(`行${n + 1}: 列数が不足しています (引用符ずれ?)`); continue; }
    const slip = trimS(r[iSlip]);
    const code = trimS(r[iCode]);
    if (!slip || !code) { errors.push(`行${n + 1}: 伝票NOまたは型番が空です`); continue; }
    const good = intOrNull(r[iGood]);
    if (good == null || good < 0) { errors.push(`行${n + 1}: 良品数が0以上の整数ではありません (${trimS(r[iGood])})`); continue; }
    let bad = 0;
    if (iBad !== -1 && trimS(r[iBad]) !== '') {
      bad = intOrNull(r[iBad]);
      if (bad == null || bad < 0) { errors.push(`行${n + 1}: 不良品数が0以上の整数ではありません (${trimS(r[iBad])})`); continue; }
    }
    const costN = trimS(r[iCost]).replace(/,/g, '');
    let cost = null;
    if (costN !== '') {
      cost = Number(costN);
      if (!Number.isFinite(cost) || cost < 0) { errors.push(`行${n + 1}: 仕入単価が不正です (${trimS(r[iCost])})`); continue; }
    }
    let date = iDate != null && iDate !== -1 ? trimS(r[iDate]).replace(/\//g, '-') : '';
    if (date && !isYmd(date)) { warnings.push(`行${n + 1}: 入庫日の形式が不正 (${date}) — 日付なし扱い`); date = ''; }
    const line = {
      supplierCode: normSupplierCode(r[iSup]), productCode: code, productKey: normProductCode(code),
      goodQty: good, defectiveQty: Math.max(0, bad ?? 0), unitCost: cost, date: date || null,
      payload: JSON.stringify(Object.fromEntries(header.map((h, k) => [h, trimS(r[k])]).filter(([, v]) => v !== ''))),
    };
    if (!bySlip.has(slip)) bySlip.set(slip, []);
    bySlip.get(slip).push(line);
  }
  if (errors.length) {
    throw new Error(`取込を中止しました (不正な行が${errors.length}件): ` + errors.slice(0, 5).join(' / ') + (errors.length > 5 ? ' …' : ''));
  }
  if (!bySlip.size) throw new Error('取り込める行がありません');

  const tx = db.transaction(() => {
    const dup = db.prepare('SELECT id FROM po_inbound_batches WHERE file_hash=?').get(fileHash);
    if (dup) return { alreadyImported: true, batchId: dup.id };
    const now = nowIso();
    const info = db.prepare('INSERT INTO po_inbound_batches (file_name, file_hash, row_count, receipt_count, imported_at, actor) VALUES (?,?,?,?,?,?)')
      .run(filename || null, fileHash, rows.length - 1, bySlip.size, now, actor);
    const batchId = Number(info.lastInsertRowid);

    let newItems = 0, unchangedItems = 0, supersededItems = 0;
    const conflicts = [];
    const allocOf = db.prepare(`
      SELECT COALESCE(SUM(e.qty),0) AS n FROM po_item_events e
      WHERE e.inbound_item_id = ? AND e.event_type='receipt'
        AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)`);

    for (const [slip, lines] of bySlip) {
      // 出現順つき line_key を確定
      const seen = new Map();
      for (const l of lines) {
        const h = lineContentHash({ productCode: l.productKey, goodQty: l.goodQty, defectiveQty: l.defectiveQty, unitCost: l.unitCost });
        const n = (seen.get(h) || 0) + 1;
        seen.set(h, n);
        l.lineKey = `${h}#${n}`;
      }
      const slipDate = lines.find(l => l.date)?.date || null;
      let receipt = db.prepare("SELECT * FROM po_inbound_receipts WHERE source='logizard' AND source_key=?").get(slip);
      if (!receipt) {
        const ri = db.prepare("INSERT INTO po_inbound_receipts (source, source_key, receipt_date, first_batch_id, last_batch_id, imported_at) VALUES ('logizard',?,?,?,?,?)")
          .run(slip, slipDate, batchId, batchId, now);
        receipt = { id: Number(ri.lastInsertRowid) };
      } else {
        db.prepare('UPDATE po_inbound_receipts SET last_batch_id=?, receipt_date=COALESCE(?, receipt_date) WHERE id=?')
          .run(batchId, slipDate, receipt.id);
      }
      const existing = db.prepare('SELECT * FROM po_inbound_items WHERE receipt_id=? AND superseded=0').all(receipt.id);
      const existingByKey = new Map(existing.map(x => [x.line_key, x]));
      const incomingKeys = new Set(lines.map(l => l.lineKey));
      // 新行の追加 / 不変行の維持
      const ins = db.prepare(`INSERT INTO po_inbound_items
        (receipt_id, line_key, batch_id, supplier_code, product_code, product_key, good_qty, defective_qty, unit_cost, payload_json, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
      for (const l of lines) {
        if (existingByKey.has(l.lineKey)) { unchangedItems++; continue; }
        // 過去にsupersededになった同一内容行が復活するケース: UNIQUE(receipt,line_key) に当たるため復活させる。
        // 監査が誤らないよう、原本・バッチ情報も今回の値へ更新する (Codex P14-R1 Medium-1)
        const dead = db.prepare('SELECT id FROM po_inbound_items WHERE receipt_id=? AND line_key=? AND superseded=1').get(receipt.id, l.lineKey);
        if (dead) {
          db.prepare(`UPDATE po_inbound_items SET superseded=0, superseded_batch_id=NULL,
                        batch_id=?, supplier_code=?, product_code=?, payload_json=?, created_at=? WHERE id=?`)
            .run(batchId, l.supplierCode || null, l.productCode, l.payload, now, dead.id);
          newItems++;
          continue;
        }
        ins.run(receipt.id, l.lineKey, batchId, l.supplierCode || null, l.productCode, l.productKey, l.goodQty, l.defectiveQty, l.unitCost, l.payload, now);
        newItems++;
      }
      // 消えた行 = superseded。割当が残っていれば競合
      for (const x of existing) {
        if (incomingKeys.has(x.line_key)) continue;
        db.prepare('UPDATE po_inbound_items SET superseded=1, superseded_batch_id=? WHERE id=?').run(batchId, x.id);
        supersededItems++;
        const alloc = allocOf.get(x.id).n;
        if (alloc > 0) conflicts.push({ inboundItemId: x.id, slip, productCode: x.product_code, allocated: alloc });
      }
    }
    audit(db, { actorType: 'user', actor, action: 'inbound_import', resource: `inbound_batch:${batchId}`,
      detail: { filename, receipts: bySlip.size, newItems, unchangedItems, supersededItems, conflicts: conflicts.length } });
    return { alreadyImported: false, batchId, receipts: bySlip.size, newItems, unchangedItems, supersededItems, conflicts };
  });
  const result = tx.immediate();
  return { ...result, warnings };
}

/** 入庫明細ごとの有効割当合計 */
function allocSums(db) {
  const map = new Map();
  for (const r of db.prepare(`
    SELECT e.inbound_item_id AS id, SUM(e.qty) AS n FROM po_item_events e
    WHERE e.inbound_item_id IS NOT NULL AND e.event_type='receipt'
      AND NOT EXISTS (SELECT 1 FROM po_item_events r2 WHERE r2.reverses_id = e.id)
    GROUP BY e.inbound_item_id`).all()) map.set(r.id, r.n);
  return map;
}

/**
 * 入庫消込ページのデータ: 未割当・一部割当の入庫明細 (+突合候補数) / 訂正競合 / 対象外。
 */
export function listInbound() {
  const db = getDB();
  const alloc = allocSums(db);
  const items = db.prepare(`
    SELECT i.*, r.source_key AS slip, r.receipt_date,
           (SELECT COUNT(*) FROM po_inbound_ignores g WHERE g.inbound_item_id = i.id AND g.revoked_at IS NULL) AS ignored
    FROM po_inbound_items i JOIN po_inbound_receipts r ON r.id = i.receipt_id
    ORDER BY r.receipt_date IS NULL, r.receipt_date DESC, r.source_key DESC, i.id`).all();
  const open = [], ignored = [], conflicts = [];
  for (const i of items) {
    const allocated = alloc.get(i.id) || 0;
    const row = {
      id: i.id, slip: i.slip, receiptDate: i.receipt_date, supplierCode: i.supplier_code,
      productCode: i.product_code, productKey: i.product_key,
      goodQty: i.good_qty, defectiveQty: i.defective_qty, unitCost: i.unit_cost,
      allocated, remainingCapacity: Math.max(0, i.good_qty - allocated),
    };
    if (i.superseded) {
      if (allocated > 0) conflicts.push({ ...row, kind: 'superseded_with_alloc' });
      continue; // 割当なしのsupersededは表示しない (訂正で消えた行)
    }
    if (i.ignored) { ignored.push(row); continue; }
    if (row.remainingCapacity > 0) open.push(row);
  }
  return { open, ignored, conflicts };
}

/**
 * 突合候補 (要件v8 F-5): 仕入先不一致は除外。スコア = 仕入先+商品一致80 / 納期±3日+20 /
 * 数量=残数ぴったり+10 / 過納(入庫残容量>PO残)-40。同点はFIFO (発行日昇順)。
 */
export function candidatesFor(inboundItemId) {
  const db = getDB();
  const boundary = getSetting('tracking_started_at');
  const item = db.prepare(`
    SELECT i.*, r.source_key AS slip, r.receipt_date FROM po_inbound_items i
    JOIN po_inbound_receipts r ON r.id = i.receipt_id WHERE i.id=?`).get(inboundItemId);
  if (!item) throw new Error(`入庫明細が存在しません: ${inboundItemId}`);
  if (!boundary) return { item, candidates: [] };
  const alloc = allocSums(db);
  const capacity = Math.max(0, item.good_qty - (alloc.get(item.id) || 0));
  const baseDate = item.receipt_date || jstToday();
  const rows = db.prepare(`
    SELECT oi.id AS order_item_id, oi.product_code, oi.unit_cost AS po_unit_cost,
           oi.requested_date, oi.promised_date, b.remaining_qty,
           o.id AS order_id, o.po_number, o.supplier_code, o.supplier_name, o.issued_at
    FROM po_order_items oi
    JOIN v_po_item_balance b ON b.order_item_id = oi.id
    JOIN po_orders o ON o.id = oi.order_id
    WHERE oi.product_key = ? AND o.status='issued' AND o.closed_at IS NULL AND o.issued_at >= ?
      AND b.remaining_qty > 0
    ORDER BY o.issued_at ASC`).all(item.product_key, boundary);
  const candidates = [];
  for (const r of rows) {
    // 仕入先不一致は候補から除外 (入庫側の取引先ID欠落時は除外しない)
    if (item.supplier_code && r.supplier_code && normSupplierCode(item.supplier_code) !== normSupplierCode(r.supplier_code)) continue;
    let score = 80;
    const due = r.promised_date || r.requested_date;
    if (due) {
      const d = Math.abs((Date.parse(baseDate) - Date.parse(due)) / 86400000);
      if (d <= 3) score += 20;
    }
    if (capacity === r.remaining_qty) score += 10;
    if (capacity > r.remaining_qty) score -= 40;
    const costDiff = item.unit_cost != null && r.po_unit_cost != null && Number(item.unit_cost) !== Number(r.po_unit_cost);
    candidates.push({
      orderItemId: r.order_item_id, orderId: r.order_id, poNumber: r.po_number,
      supplierCode: r.supplier_code, supplierName: r.supplier_name, issuedAt: r.issued_at,
      remaining: r.remaining_qty, due: due || null, poUnitCost: r.po_unit_cost,
      costDiff, score, suggestedQty: Math.min(capacity, r.remaining_qty),
    });
  }
  candidates.sort((a, b) => b.score - a.score || String(a.issuedAt).localeCompare(String(b.issuedAt)));
  return { item: { ...item, capacity }, candidates };
}

/** 対象外の指定/解除 (履歴型) */
export function setInboundIgnore(inboundItemId, { ignore, reason = null, actor = null }) {
  const db = getDB();
  const tx = db.transaction(() => {
    const item = db.prepare('SELECT id, superseded FROM po_inbound_items WHERE id=?').get(inboundItemId);
    if (!item) throw new Error(`入庫明細が存在しません: ${inboundItemId}`);
    const active = db.prepare('SELECT id FROM po_inbound_ignores WHERE inbound_item_id=? AND revoked_at IS NULL').get(inboundItemId);
    const now = nowIso();
    if (ignore) {
      if (active) return { changed: false };
      if (!reason || !String(reason).trim()) throw new Error('対象外には理由が必要です');
      // 割当が残る入庫を対象外にすると台帳と矛盾したまま画面から隠れる (Codex P14-R1 High-1)
      if (item.superseded) throw new Error('訂正で無効化された行は対象外にできません (競合の解消を先に)');
      const alloc = db.prepare(`SELECT COALESCE(SUM(e.qty),0) AS n FROM po_item_events e
        WHERE e.inbound_item_id=? AND e.event_type='receipt'
          AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id=e.id)`).get(inboundItemId).n;
      if (alloc > 0) throw new Error(`割当 (${alloc}) が残っています。先に発注残ページから逆仕訳してください`);
      db.prepare('INSERT INTO po_inbound_ignores (inbound_item_id, reason, created_at, actor) VALUES (?,?,?,?)')
        .run(inboundItemId, String(reason).trim(), now, actor);
    } else {
      if (!active) return { changed: false };
      db.prepare('UPDATE po_inbound_ignores SET revoked_at=?, revoked_by=? WHERE id=?').run(now, actor, active.id);
    }
    audit(db, { actorType: 'user', actor, action: ignore ? 'inbound_ignore' : 'inbound_unignore', resource: `inbound_item:${inboundItemId}`, detail: { reason } });
    return { changed: true };
  });
  return tx.immediate();
}
