/**
 * 仕入先発注補助システム (purchase-orders)
 *
 * 発注に必要な情報 (何を・どれだけ・条件を満たすか) を1画面に集約し、
 * 画面遷移なしで仕入先への発注リストを作る。
 *
 *   GET /                    発注ダッシュボード (仕入先カード + 商品検索)
 *   GET /supplier/:code      仕入先発注ワークスペース (要発注/ついで買い/掘り起こし/条件ゲージ/カート)
 *   GET /orders              発注履歴
 *   GET /admin               マスタ管理 (仕入先/発注条件/原料グループ/商品紐付け + CSV取込)
 *   GET/POST/DELETE /api/... 各種API (下記)
 *
 * データ: mirror_pml_snapshot_rows (published run, read-only) + po_* (本アプリ正本)。
 * 計算ロジックは logic.js (旧「発注対象商品」シート数式の移植) を参照。
 */
import { Router } from 'express';
import fs from 'fs';
import multer from 'multer';
import iconv from 'iconv-lite';
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { computeAll, loadPml, loadMasters, evaluateCondition } from './logic.js';

const router = Router();

const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR, limits: { fileSize: 8 * 1024 * 1024 } });

// ─── 共通ヘルパ ───
const he = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const jsonEmbed = obj => JSON.stringify(obj).replace(/</g, '\\u003c');
const nowIso = () => new Date().toISOString();

function fmtJst(iso) {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return he(iso);
  const j = new Date(t + 9 * 3600 * 1000);
  const p = n => String(n).padStart(2, '0');
  return `${j.getUTCFullYear()}-${p(j.getUTCMonth() + 1)}-${p(j.getUTCDate())} ${p(j.getUTCHours())}:${p(j.getUTCMinutes())}`;
}

/** CSV バッファ → 行配列 (UTF-8優先判定、失敗時 Shift_JIS)。RFC4180 対応の自前パーサ */
function parseCsvBuffer(buf) {
  let text;
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.slice(3).toString('utf8');
  } else {
    const utf8 = buf.toString('utf8');
    text = utf8.includes('�') ? iconv.decode(buf, 'Shift_JIS') : utf8;
  }
  const rows = [];
  let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQ = false;
      } else field += c;
    } else if (c === '"') inQ = true;
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
  return rows;
}

const trimS = v => String(v == null ? '' : v).trim();
const numOrNull = v => {
  const s = trimS(v);
  if (s === '') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

// ─── 発注 draft/issue の永続化 ───
function enrichItems(rawItems) {
  // 現 PML に存在する商品コードのみ許可し、商品名・原価をスナップショットする
  const { pub, rows } = loadPml();
  if (!pub) throw new Error('PMLスナップショット未同期のため保存できません');
  const byKey = new Map();
  for (const r of rows) byKey.set(normProductCode(r['商品コード']), r);
  const items = [];
  const seen = new Set();
  for (const it of rawItems) {
    const code = trimS(it.code);
    const key = normProductCode(code);
    const qty = Number(it.qty);
    if (!key) throw new Error('商品コードが空の明細があります');
    if (seen.has(key)) throw new Error(`商品コード重複: ${code}`);
    seen.add(key);
    if (!Number.isInteger(qty) || qty <= 0) throw new Error(`数量が不正です (${code}: ${it.qty})`);
    const r = byKey.get(key);
    if (!r) throw new Error(`PMLに存在しない商品コード: ${code}`);
    items.push({ code: r['商品コード'], key, name: r['商品名'] || '', qty, cost: r['原価'] == null ? null : Number(r['原価']) });
  }
  return { items, pmlAsOf: pub.as_of_date || null };
}

function upsertDraft(supplierCode, supplierName, rawItems, note) {
  const db = getDB();
  const { items, pmlAsOf } = enrichItems(rawItems);
  const now = nowIso();
  return db.transaction(() => {
    let order = db.prepare("SELECT id FROM po_orders WHERE supplier_code=? AND status='draft'").get(supplierCode);
    if (!order) {
      const info = db.prepare(
        'INSERT INTO po_orders (supplier_code, supplier_name, status, note, pml_as_of_date, created_at, updated_at) VALUES (?,?,?,?,?,?,?)'
      ).run(supplierCode, supplierName, 'draft', note || null, pmlAsOf, now, now);
      order = { id: Number(info.lastInsertRowid) };
    } else {
      db.prepare('UPDATE po_orders SET note=?, pml_as_of_date=?, updated_at=?, supplier_name=? WHERE id=?')
        .run(note || null, pmlAsOf, now, supplierName, order.id);
    }
    db.prepare('DELETE FROM po_order_items WHERE order_id=?').run(order.id);
    const ins = db.prepare('INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty, unit_cost) VALUES (?,?,?,?,?,?)');
    for (const it of items) ins.run(order.id, it.code, it.key, it.name, it.qty, it.cost);
    return order.id;
  })();
}

function getOrderWithItems(id) {
  const db = getDB();
  const order = db.prepare('SELECT * FROM po_orders WHERE id=?').get(id);
  if (!order) return null;
  order.items = db.prepare('SELECT product_code, product_name, qty, unit_cost FROM po_order_items WHERE order_id=? ORDER BY id').all(id);
  return order;
}

// ─── 商品データの API 送信形 ───
function productDto(p) {
  return {
    code: p.code, name: p.name, stock: p.stock, backOrder: p.backOrder,
    sales30: p.sales30, sales7: p.sales7, lot: p.lot, holdMonths: p.holdMonths,
    stockMonths: Math.round(p.stockMonths * 100) / 100, recQty: p.recQty,
    cost: p.cost, price: p.price, lastPurchase: p.lastPurchase ? String(p.lastPurchase).slice(0, 10) : '',
    conditionId: p.conditionId, materialGroupId: p.materialGroupId,
    capacityPerUnit: p.capacityPerUnit, recentIssued: p.recentIssued,
  };
}

function supplierWorkspaceData(code) {
  const { pub, bySupplier, masters } = computeAll();
  const g = bySupplier.get(code);
  const sm = masters.suppliers.get(code);
  if (!g && !sm) return null;
  const db = getDB();
  const draftRow = db.prepare("SELECT id, note FROM po_orders WHERE supplier_code=? AND status='draft'").get(code);
  const draft = draftRow ? {
    id: draftRow.id, note: draftRow.note || '',
    items: db.prepare('SELECT product_code AS code, qty FROM po_order_items WHERE order_id=? ORDER BY id').all(draftRow.id),
  } : null;
  const all = g ? [...g.targets, ...g.candidates, ...g.horikoshi] : [];
  const memberOf = (fieldVal, field) => all.filter(p => p[field] === fieldVal).map(p => p.code);
  const condIds = new Set(all.map(p => p.conditionId).filter(Boolean));
  const conditions = masters.conditions
    .filter(c => condIds.has(c.condition_id) || normSupplierCode(c.supplier_code) === code)
    .map(c => ({
      conditionId: c.condition_id, displayName: c.display_name, makerName: c.maker_name || '',
      conditionType: c.condition_type, conditionValue: c.condition_value, unit: c.unit || '',
      memberCodes: memberOf(c.condition_id, 'conditionId'),
    }));
  const matIds = new Set(all.map(p => p.materialGroupId).filter(Boolean));
  const materialGroups = [...matIds].map(id => {
    const m = masters.materialGroups.get(id);
    return {
      groupId: id, name: m ? m.name : id,
      minOrderQty: m ? m.min_order_qty : null, unit: m ? (m.unit || '') : '',
      memberCodes: memberOf(id, 'materialGroupId'),
    };
  });
  return {
    pub: pub ? { as_of_date: pub.as_of_date, status: pub.status, synced_at: pub.synced_at } : null,
    supplier: { code, name: (sm && sm.name) || (g && g.name) || `仕入先 ${code}`, memo: (sm && sm.order_memo) || '' },
    targets: g ? g.targets.map(productDto) : [],
    candidates: g ? g.candidates.map(productDto) : [],
    horikoshi: g ? g.horikoshi.map(productDto) : [],
    conditions, materialGroups, draft,
  };
}

// ═══════════════════════════ API ═══════════════════════════

router.get('/api/overview', (req, res) => {
  try {
    const { pub, bySupplier } = computeAll();
    const db = getDB();
    const draftCodes = new Set(db.prepare("SELECT supplier_code FROM po_orders WHERE status='draft'").all().map(r => r.supplier_code));
    const cards = [];
    for (const g of bySupplier.values()) {
      if (g.targets.length === 0 && !draftCodes.has(g.code)) continue;
      cards.push({
        code: g.code, name: g.name || `仕入先 ${g.code}`, memo: g.memo,
        targetCount: g.targets.length, estAmount: g.estAmount, hasDraft: draftCodes.has(g.code),
      });
    }
    cards.sort((a, b) => b.targetCount - a.targetCount || b.estAmount - a.estAmount);
    res.json({ ok: true, pub: pub ? { as_of_date: pub.as_of_date, status: pub.status } : null, cards });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/supplier/:code', (req, res) => {
  try {
    const code = normSupplierCode(req.params.code);
    const data = supplierWorkspaceData(code);
    if (!data) return res.status(404).json({ ok: false, error: '仕入先が見つかりません' });
    res.json({ ok: true, ...data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.post('/api/supplier/:code/draft', (req, res) => {
  try {
    const code = normSupplierCode(req.params.code);
    const { items, note, supplierName } = req.body || {};
    if (!Array.isArray(items)) return res.status(400).json({ ok: false, error: 'items が必要です' });
    if (items.length === 0) {
      const db = getDB();
      db.prepare("DELETE FROM po_orders WHERE supplier_code=? AND status='draft'").run(code);
      return res.json({ ok: true, deleted: true });
    }
    const id = upsertDraft(code, trimS(supplierName) || `仕入先 ${code}`, items, trimS(note));
    res.json({ ok: true, id });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/supplier/:code/issue', (req, res) => {
  try {
    const code = normSupplierCode(req.params.code);
    const { items, note, supplierName } = req.body || {};
    if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ ok: false, error: '発注明細が空です' });
    const db = getDB();
    const id = upsertDraft(code, trimS(supplierName) || `仕入先 ${code}`, items, trimS(note));
    const now = nowIso();
    db.prepare("UPDATE po_orders SET status='issued', issued_at=?, updated_at=? WHERE id=?").run(now, now, id);
    res.json({ ok: true, id });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.get('/api/orders', (req, res) => {
  try {
    const db = getDB();
    const rows = db.prepare(`
      SELECT o.id, o.supplier_code, o.supplier_name, o.status, o.note, o.created_at, o.issued_at,
             COUNT(i.id) AS sku_count, COALESCE(SUM(i.qty),0) AS total_qty,
             COALESCE(SUM(i.qty * COALESCE(i.unit_cost,0)),0) AS total_amount
      FROM po_orders o LEFT JOIN po_order_items i ON i.order_id = o.id
      GROUP BY o.id
      ORDER BY COALESCE(o.issued_at, o.updated_at) DESC
      LIMIT 300
    `).all();
    res.json({ ok: true, orders: rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/orders/:id', (req, res) => {
  try {
    const order = getOrderWithItems(Number(req.params.id));
    if (!order) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, order });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── マスタ CRUD ───
const MASTER_DEFS = {
  suppliers: {
    table: 'po_suppliers', pk: 'supplier_code',
    fromBody: b => ({ supplier_code: normSupplierCode(b.supplier_code), name: trimS(b.name), order_memo: trimS(b.order_memo) || null }),
    validate: r => { if (!r.supplier_code) return '仕入先コード必須'; if (!r.name) return '仕入先名必須'; return null; },
    csvHeader: ['仕入先コード', '仕入先名', '発注メモ'],
    fromCsv: c => ({ supplier_code: normSupplierCode(c[0]), name: trimS(c[1]), order_memo: trimS(c[2]) || null }),
  },
  conditions: {
    table: 'po_order_conditions', pk: 'condition_id',
    fromBody: b => ({
      condition_id: trimS(b.condition_id), supplier_code: normSupplierCode(b.supplier_code) || null,
      maker_name: trimS(b.maker_name) || null, display_name: trimS(b.display_name),
      condition_type: trimS(b.condition_type), condition_value: numOrNull(b.condition_value), unit: trimS(b.unit) || null,
    }),
    validate: r => {
      if (!r.condition_id) return '条件ID必須';
      if (!r.display_name) return '管理名必須';
      if (!r.condition_type) return '条件タイプ必須';
      if (r.condition_value == null || r.condition_value < 0) return '条件値が不正';
      return null;
    },
    csvHeader: ['条件ID', '仕入先コード', 'メーカー名', '管理名', '条件タイプ', '条件値', '単位'],
    fromCsv: c => ({
      condition_id: trimS(c[0]), supplier_code: normSupplierCode(c[1]) || null, maker_name: trimS(c[2]) || null,
      display_name: trimS(c[3]), condition_type: trimS(c[4]), condition_value: numOrNull(c[5]), unit: trimS(c[6]) || null,
    }),
  },
  materials: {
    table: 'po_material_groups', pk: 'group_id',
    fromBody: b => ({ group_id: trimS(b.group_id), name: trimS(b.name), min_order_qty: numOrNull(b.min_order_qty), unit: trimS(b.unit) || null }),
    validate: r => {
      if (!r.group_id) return '原料グループID必須';
      if (!r.name) return '原料グループ名必須';
      if (r.min_order_qty != null && r.min_order_qty < 0) return '最低発注量が不正';
      return null;
    },
    csvHeader: ['原料グループID', '原料グループ名', '最低発注量', '単位'],
    fromCsv: c => ({ group_id: trimS(c[0]), name: trimS(c[1]), min_order_qty: numOrNull(c[2]), unit: trimS(c[3]) || null }),
  },
  attrs: {
    table: 'po_product_attrs', pk: 'product_code',
    fromBody: b => ({
      product_code: trimS(b.product_code), product_key: normProductCode(b.product_code),
      condition_id: trimS(b.condition_id) || null, material_group_id: trimS(b.material_group_id) || null,
      capacity_per_unit: numOrNull(b.capacity_per_unit), case_group: trimS(b.case_group) || null, case_lot: numOrNull(b.case_lot),
    }),
    validate: r => {
      if (!r.product_code) return '商品コード必須';
      if (r.capacity_per_unit != null && r.capacity_per_unit <= 0) return '容量/個が不正';
      return null;
    },
    csvHeader: ['商品コード', '発注条件グループID', '原料グループID', '容量_per_個', 'ケースグループ', 'ケースロット'],
    fromCsv: c => ({
      product_code: trimS(c[0]), product_key: normProductCode(c[0]),
      condition_id: trimS(c[1]) || null, material_group_id: trimS(c[2]) || null,
      capacity_per_unit: numOrNull(c[3]), case_group: trimS(c[4]) || null, case_lot: numOrNull(c[5]),
    }),
  },
};

function upsertMasterRow(def, row) {
  const db = getDB();
  const cols = Object.keys(row);
  const now = nowIso();
  const sql = `INSERT INTO ${def.table} (${cols.join(',')}, created_at, updated_at)
               VALUES (${cols.map(() => '?').join(',')}, ?, ?)
               ON CONFLICT(${def.pk}) DO UPDATE SET
               ${cols.filter(c => c !== def.pk).map(c => `${c}=excluded.${c}`).join(', ')}, updated_at=excluded.updated_at`;
  db.prepare(sql).run(...cols.map(c => row[c]), now, now);
}

router.get('/api/masters/:kind', (req, res) => {
  const def = MASTER_DEFS[req.params.kind];
  if (!def) return res.status(404).json({ ok: false, error: 'unknown master' });
  try {
    const rows = getDB().prepare(`SELECT * FROM ${def.table} ORDER BY ${def.pk}`).all();
    res.json({ ok: true, rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.post('/api/masters/:kind', (req, res) => {
  const def = MASTER_DEFS[req.params.kind];
  if (!def) return res.status(404).json({ ok: false, error: 'unknown master' });
  try {
    const row = def.fromBody(req.body || {});
    const err = def.validate(row);
    if (err) return res.status(400).json({ ok: false, error: err });
    upsertMasterRow(def, row);
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.delete('/api/masters/:kind/:id', (req, res) => {
  const def = MASTER_DEFS[req.params.kind];
  if (!def) return res.status(404).json({ ok: false, error: 'unknown master' });
  try {
    const info = getDB().prepare(`DELETE FROM ${def.table} WHERE ${def.pk}=?`).run(req.params.id);
    res.json({ ok: true, deleted: info.changes });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.post('/api/masters/:kind/csv', upload.single('file'), (req, res) => {
  const def = MASTER_DEFS[req.params.kind];
  if (!def) return res.status(404).json({ ok: false, error: 'unknown master' });
  if (!req.file) return res.status(400).json({ ok: false, error: 'CSVファイルが必要です' });
  let buf;
  try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
  try {
    const rows = parseCsvBuffer(buf);
    if (rows.length < 2) return res.status(400).json({ ok: false, error: 'データ行がありません' });
    const header = rows[0].map(trimS);
    // 見出し検証: 定義した列名が先頭から一致すること (余剰列は無視)
    for (let i = 0; i < def.csvHeader.length; i++) {
      const expected = def.csvHeader[i];
      const optional = (def === MASTER_DEFS.suppliers && i === 2);
      if (header[i] !== expected && !(optional && header[i] === undefined)) {
        if (!(optional && !header[i])) {
          return res.status(400).json({ ok: false, error: `CSV見出し不一致: ${i + 1}列目は「${expected}」であるべき (実際: 「${header[i] || ''}」)` });
        }
      }
    }
    const db = getDB();
    let upserted = 0; const errors = [];
    db.transaction(() => {
      for (let i = 1; i < rows.length; i++) {
        const row = def.fromCsv(rows[i]);
        const err = def.validate(row);
        if (err) { errors.push(`行${i + 1}: ${err}`); continue; }
        upsertMasterRow(def, row);
        upserted++;
      }
      if (errors.length > 20) throw new Error(`エラー多数のため全件中止 (${errors.length}件)。例: ${errors.slice(0, 3).join(' / ')}`);
    })();
    res.json({ ok: true, upserted, skipped: errors.length, errors: errors.slice(0, 10) });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// 取扱中なのにどのグループにも未紐付けの商品 (登録漏れチェック用)
router.get('/api/attrs/unlinked', (req, res) => {
  try {
    const { rows } = loadPml();
    const { attrs } = loadMasters();
    const unlinked = [];
    for (const r of rows) {
      if (String(r['取扱区分'] || '') !== '取扱中') continue;
      const key = normProductCode(r['商品コード']);
      if (!attrs.has(key)) unlinked.push({ code: r['商品コード'], name: r['商品名'] || '', supplier: normSupplierCode(r['仕入先']) });
    }
    res.json({ ok: true, count: unlinked.length, rows: unlinked.slice(0, 500) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ═══════════════════════════ 画面 ═══════════════════════════

const CSS = `
  * { box-sizing: border-box; }
  body { font-family: 'Hiragino Sans', 'Yu Gothic UI', Meiryo, sans-serif; margin: 0; background: #f2f4f7; color: #1f2733; }
  header.app { background: #16324f; color: #fff; padding: 10px 18px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
  header.app h1 { font-size: 16px; margin: 0; }
  header.app a { color: #bcd6f0; text-decoration: none; font-size: 13px; }
  header.app a:hover { text-decoration: underline; }
  .fresh { font-size: 12px; color: #cfe0f2; margin-left: auto; }
  .wrap { padding: 16px 18px; max-width: 1500px; margin: 0 auto; }
  .warn { background: #fff3cd; border: 1px solid #e0c05a; color: #6b5308; padding: 8px 12px; border-radius: 6px; margin-bottom: 12px; font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 12px; }
  .card { background: #fff; border: 1px solid #dde3ea; border-radius: 8px; padding: 14px; display: block; text-decoration: none; color: inherit; }
  .card:hover { border-color: #4a7fb5; box-shadow: 0 2px 6px rgba(22,50,79,.12); }
  .card .nm { font-weight: 600; font-size: 14px; margin-bottom: 4px; }
  .card .memo { font-size: 11px; color: #8a6d1a; }
  .card .stats { display: flex; gap: 14px; margin-top: 8px; font-size: 12px; color: #4a5568; }
  .card .stats b { font-size: 17px; color: #16324f; }
  .badge { display: inline-block; font-size: 11px; border-radius: 10px; padding: 1px 8px; vertical-align: middle; }
  .b-draft { background: #e7f0fb; color: #205493; border: 1px solid #a9c6e8; }
  .b-issued { background: #e8f6ec; color: #1c7c3c; border: 1px solid #a3d9b3; }
  .b-warn { background: #fdecea; color: #9c2b23; border: 1px solid #efb7b2; }
  table.t { border-collapse: collapse; width: 100%; background: #fff; font-size: 12px; }
  table.t th, table.t td { border: 1px solid #dde3ea; padding: 4px 7px; }
  table.t th { background: #eef2f7; position: sticky; top: 0; white-space: nowrap; }
  table.t td.r, table.t th.r { text-align: right; font-variant-numeric: tabular-nums; }
  .sec { background: #fff; border: 1px solid #dde3ea; border-radius: 8px; margin-bottom: 14px; overflow: hidden; }
  .sec > h2 { font-size: 13px; margin: 0; padding: 8px 12px; background: #eef2f7; border-bottom: 1px solid #dde3ea; cursor: pointer; user-select: none; }
  .sec .bd { padding: 10px 12px; overflow-x: auto; }
  button { font: inherit; border-radius: 6px; border: 1px solid #c3ccd6; background: #fff; padding: 6px 14px; cursor: pointer; }
  button:hover { background: #f0f4f8; }
  button.pri { background: #16324f; border-color: #16324f; color: #fff; }
  button.pri:hover { background: #234a70; }
  button.ghost { border: none; background: none; color: #205493; padding: 2px 6px; }
  input[type=text], input[type=number], select, textarea { font: inherit; border: 1px solid #c3ccd6; border-radius: 5px; padding: 4px 7px; }
  input[type=number] { width: 84px; text-align: right; }
  .gauge { margin: 4px 0; font-size: 12px; }
  .gauge .bar { height: 8px; border-radius: 4px; background: #e3e8ee; overflow: hidden; margin-top: 2px; }
  .gauge .bar > div { height: 100%; background: #d9822b; }
  .gauge.met .bar > div { background: #2f9e44; }
  .muted { color: #7a8694; font-size: 11px; }
  .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 12px; flex-wrap: wrap; }
  #toast { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); background: #16324f; color: #fff; padding: 10px 22px; border-radius: 8px; display: none; z-index: 50; font-size: 13px; }
  .tabbar { display: flex; gap: 4px; margin-bottom: 12px; }
  .tabbar button.on { background: #16324f; color: #fff; border-color: #16324f; }
  .grid2 { display: grid; grid-template-columns: 1fr 340px; gap: 14px; align-items: start; }
  @media (max-width: 1000px) { .grid2 { grid-template-columns: 1fr; } }
  .cart { position: sticky; top: 10px; }
  .cart .tot { font-size: 13px; margin: 6px 0; }
  .cart .tot b { font-size: 18px; }
  pre.copy { background: #f6f8fa; border: 1px solid #dde3ea; border-radius: 6px; padding: 10px; font-size: 12px; max-height: 300px; overflow: auto; white-space: pre; }
`;

function pageShell(title, nav, body, script) {
  return `<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${he(title)}</title><style>${CSS}</style></head>
<body>
<header class="app">
  <h1>📦 発注補助</h1>
  <a href="/apps/purchase-orders">ダッシュボード</a>
  <a href="/apps/purchase-orders/orders">発注履歴</a>
  <a href="/apps/purchase-orders/admin">マスタ管理</a>
  <a href="/dashboard">← ポータル</a>
  ${nav || ''}
</header>
<div class="wrap">${body}</div>
<div id="toast"></div>
<script>
function toast(msg) {
  var t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  clearTimeout(t._h); t._h = setTimeout(function(){ t.style.display = 'none'; }, 2500);
}
function yen(n) { return '¥' + Math.round(n).toLocaleString('ja-JP'); }
function esc(s) { s = (s == null ? '' : String(s)); return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
${script || ''}
</script>
</body></html>`;
}

// ─── 画面1: ダッシュボード ───
router.get('/', (req, res) => {
  let data;
  try {
    const { pub, bySupplier, products } = computeAll();
    const db = getDB();
    const draftCodes = new Set(db.prepare("SELECT supplier_code FROM po_orders WHERE status='draft'").all().map(r => r.supplier_code));
    const cards = [];
    const others = [];
    for (const g of bySupplier.values()) {
      const c = {
        code: g.code, name: g.name || ('仕入先 ' + g.code), memo: g.memo,
        targetCount: g.targets.length, estAmount: g.estAmount, hasDraft: draftCodes.has(g.code),
        productCount: g.targets.length + g.candidates.length + g.horikoshi.length,
      };
      if (c.targetCount > 0 || c.hasDraft) cards.push(c); else others.push(c);
    }
    cards.sort((a, b) => b.targetCount - a.targetCount || b.estAmount - a.estAmount);
    others.sort((a, b) => b.productCount - a.productCount);
    const searchIndex = products.filter(p => p.active).map(p => ({ c: p.code, n: p.name, s: p.supplierCode }));
    data = { pub, cards, others, searchIndex };
  } catch (e) { return res.status(500).send('error: ' + he(e.message)); }

  const { pub, cards, others, searchIndex } = data;
  const stale = pub && pub.as_of_date ? (Date.now() - Date.parse(pub.as_of_date + 'T00:00:00+09:00')) > 3 * 86400000 : true;
  const freshNote = pub ? `データ: ${he(pub.as_of_date || '?')} 時点 (PML published)` : 'PML未同期';
  const cardHtml = c => `
    <a class="card" href="/apps/purchase-orders/supplier/${encodeURIComponent(c.code)}">
      <div class="nm">${he(c.name)} ${c.hasDraft ? '<span class="badge b-draft">下書きあり</span>' : ''}</div>
      ${c.memo ? `<div class="memo">📌 ${he(c.memo)}</div>` : ''}
      <div class="stats">
        <span>要発注 <b>${c.targetCount}</b> SKU</span>
        <span>推奨額 <b>¥${c.estAmount.toLocaleString('ja-JP')}</b></span>
      </div>
    </a>`;
  const body = `
    ${stale ? '<div class="warn">⚠️ PMLデータが古い可能性があります (as_of=' + he(pub ? pub.as_of_date : 'なし') + ')。daily-sync の状態を確認してください。</div>' : ''}
    <div class="toolbar">
      <input type="text" id="q" placeholder="商品コード / 商品名で検索 → 仕入先を探す" style="width:340px">
      <span class="muted">${freshNote}</span>
    </div>
    <div id="searchResult"></div>
    <h2 style="font-size:15px">発注が必要な仕入先 (${cards.length})</h2>
    <div class="cards">${cards.map(cardHtml).join('')}</div>
    <details style="margin-top:18px"><summary class="muted" style="cursor:pointer">その他の仕入先 (${others.length}) — 要発注なし</summary>
      <div class="cards" style="margin-top:10px">${others.map(cardHtml).join('')}</div>
    </details>`;
  const script = `
var IDX = ${jsonEmbed(searchIndex)};
var box = document.getElementById('q');
var out = document.getElementById('searchResult');
box.addEventListener('input', function() {
  var q = box.value.trim().toLowerCase();
  if (q.length < 2) { out.innerHTML = ''; return; }
  var hits = [];
  for (var i = 0; i < IDX.length && hits.length < 30; i++) {
    var p = IDX[i];
    if (p.c.toLowerCase().indexOf(q) >= 0 || (p.n && p.n.toLowerCase().indexOf(q) >= 0)) hits.push(p);
  }
  if (!hits.length) { out.innerHTML = '<div class="muted" style="margin-bottom:10px">該当なし</div>'; return; }
  var h = '<table class="t" style="margin-bottom:14px"><tr><th>商品コード</th><th>商品名</th><th>仕入先</th></tr>';
  for (var j = 0; j < hits.length; j++) {
    var p2 = hits[j];
    h += '<tr><td>' + esc(p2.c) + '</td><td>' + esc(p2.n) + '</td><td><a href="/apps/purchase-orders/supplier/' + encodeURIComponent(p2.s) + '">' + esc(p2.s) + ' →</a></td></tr>';
  }
  out.innerHTML = h + '</table>';
});`;
  res.send(pageShell('発注補助 — ダッシュボード', '', body, script));
});

// ─── 画面2: 仕入先ワークスペース ───
router.get('/supplier/:code', (req, res) => {
  let data;
  try {
    data = supplierWorkspaceData(normSupplierCode(req.params.code));
  } catch (e) { return res.status(500).send('error: ' + he(e.message)); }
  if (!data) return res.status(404).send('仕入先が見つかりません: ' + he(req.params.code));

  const body = `
    <div class="toolbar">
      <h2 style="margin:0;font-size:17px">${he(data.supplier.name)} <span class="muted">(コード ${he(data.supplier.code)})</span></h2>
      ${data.supplier.memo ? `<span class="badge b-warn">📌 ${he(data.supplier.memo)}</span>` : ''}
      <span class="muted">データ: ${he(data.pub ? data.pub.as_of_date : '未同期')} 時点</span>
    </div>
    <div id="condArea"></div>
    <div class="grid2">
      <div>
        <div class="sec"><h2 data-sec="targets">🔴 要発注 (<span id="cntTargets"></span>) — 在庫月数 ≦ 推奨保有月数</h2><div class="bd" id="secTargets"></div></div>
        <div class="sec"><h2 data-sec="cands">🟡 ついで買い候補 (<span id="cntCands"></span>) — 在庫月数が少ない順</h2><div class="bd" id="secCands"></div></div>
        <div class="sec"><h2 data-sec="hori">⚪ 掘り起こし (<span id="cntHori"></span>) — 在庫ゼロ / 販売実績なし</h2><div class="bd" id="secHori" style="display:none"></div></div>
      </div>
      <div class="cart sec"><h2>🛒 発注カート</h2><div class="bd" id="cartArea"></div></div>
    </div>
    <div class="sec" id="doneArea" style="display:none"><h2>✅ 発注リスト (コピーして NE 登録 / メール / FAX 原稿に)</h2><div class="bd" id="doneBody"></div></div>`;

  const script = `
var D = ${jsonEmbed(data)};
var CART = {}; // code -> qty
var byCode = {};
[].concat(D.targets, D.candidates, D.horikoshi).forEach(function(p){ byCode[p.code] = p; });

// 初期カート: draft があれば復元、なければ要発注全件を推奨量で
if (D.draft && D.draft.items.length) {
  D.draft.items.forEach(function(it){ if (byCode[it.code]) CART[it.code] = it.qty; });
} else {
  D.targets.forEach(function(p){ if (p.recQty && !p.recentIssued) CART[p.code] = p.recQty; });
}

function months(p){ return (Math.round(p.stockMonths * 100) / 100).toFixed(2); }
function issuedBadge(p){
  if (!p.recentIssued) return '';
  return ' <span class="badge b-issued" title="直近14日以内に発注確定済み (NE注残反映待ちの可能性)">発注済 ' + esc(String(p.recentIssued.issuedAt).slice(0,10)) + ' ×' + p.recentIssued.qty + '</span>';
}
function qtyCell(p) {
  var v = CART[p.code] || '';
  var step = p.lot > 0 ? p.lot : 1;
  return '<input type="number" min="0" step="' + step + '" data-code="' + esc(p.code) + '" value="' + v + '">';
}
function rowHtml(p, kind) {
  var rec = p.recQty ? p.recQty.toLocaleString('ja-JP') : '—';
  return '<tr>' +
    '<td>' + esc(p.code) + issuedBadge(p) + '</td>' +
    '<td>' + esc(p.name) + '</td>' +
    '<td class="r">' + months(p) + '</td>' +
    '<td class="r">' + p.sales30.toLocaleString('ja-JP') + '</td>' +
    '<td class="r">' + (p.stock + p.backOrder).toLocaleString('ja-JP') + (p.backOrder ? ' <span class="muted">(注残' + p.backOrder.toLocaleString('ja-JP') + ')</span>' : '') + '</td>' +
    '<td class="r">' + (p.lot || '—') + '</td>' +
    '<td class="r">' + (kind === 'hori' ? esc(p.lastPurchase || '—') : rec) + '</td>' +
    '<td class="r">' + (p.cost ? yen(p.cost) : '—') + '</td>' +
    '<td>' + qtyCell(p) + '</td></tr>';
}
function tableHtml(list, kind) {
  if (!list.length) return '<div class="muted">なし</div>';
  var h7 = kind === 'hori' ? '最終仕入日' : '推奨発注';
  var h = '<table class="t"><tr><th>商品コード</th><th>商品名</th><th class="r">在庫月数</th><th class="r">30日販売</th><th class="r">在庫+注残</th><th class="r">ロット</th><th class="r">' + h7 + '</th><th class="r">原価</th><th>発注数</th></tr>';
  list.forEach(function(p){ h += rowHtml(p, kind); });
  return h + '</table>';
}
function renderLists() {
  document.getElementById('secTargets').innerHTML = tableHtml(D.targets, 'tgt');
  document.getElementById('secCands').innerHTML = tableHtml(D.candidates.slice(0, 120), 'cand');
  document.getElementById('secHori').innerHTML = tableHtml(D.horikoshi, 'hori');
  document.getElementById('cntTargets').textContent = D.targets.length;
  document.getElementById('cntCands').textContent = D.candidates.length;
  document.getElementById('cntHori').textContent = D.horikoshi.length;
}
function cartItems() {
  var items = [];
  Object.keys(CART).forEach(function(code){
    var q = CART[code];
    if (q > 0 && byCode[code]) items.push({ code: code, qty: q, cost: byCode[code].cost, key: code.toLowerCase() });
  });
  return items;
}
function renderConditions() {
  var items = cartItems();
  var h = '';
  D.conditions.forEach(function(c) {
    var mem = {};
    c.memberCodes.forEach(function(x){ mem[x] = 1; });
    var sumQ = 0, sumA = 0;
    items.forEach(function(i){ if (mem[i.code]) { sumQ += i.qty; sumA += i.qty * (i.cost || 0); } });
    var label = esc(c.displayName) + (c.makerName ? ' <span class="muted">(' + esc(c.makerName) + ')</span>' : '');
    if (c.conditionType === '金額') {
      var met = sumA >= c.conditionValue;
      var pct = Math.min(100, c.conditionValue > 0 ? sumA / c.conditionValue * 100 : 100);
      h += '<div class="gauge' + (met ? ' met' : '') + '">' + label + ': ' + yen(sumA) + ' / ' + yen(c.conditionValue) +
        (met ? ' ✅' : ' — <b>あと ' + yen(c.conditionValue - sumA) + '</b>') +
        '<div class="bar"><div style="width:' + pct + '%"></div></div></div>';
    } else if (c.conditionType === '数量' && (c.unit === '個' || !c.unit)) {
      var met2 = sumQ >= c.conditionValue;
      var pct2 = Math.min(100, c.conditionValue > 0 ? sumQ / c.conditionValue * 100 : 100);
      h += '<div class="gauge' + (met2 ? ' met' : '') + '">' + label + ': ' + sumQ.toLocaleString('ja-JP') + ' / ' + c.conditionValue.toLocaleString('ja-JP') + ' 個' +
        (met2 ? ' ✅' : ' — <b>あと ' + (c.conditionValue - sumQ).toLocaleString('ja-JP') + ' 個</b>') +
        '<div class="bar"><div style="width:' + pct2 + '%"></div></div></div>';
    } else {
      h += '<div class="gauge">' + label + ': <span class="badge b-warn">条件 ' + esc(c.conditionType) + ' ' + c.conditionValue.toLocaleString('ja-JP') + esc(c.unit || '') + ' (手動確認)</span>' +
        ' <span class="muted">現在 ' + sumQ.toLocaleString('ja-JP') + '個 / ' + yen(sumA) + '</span></div>';
    }
  });
  D.materialGroups.forEach(function(m) {
    if (m.minOrderQty == null) return;
    var mem = {};
    m.memberCodes.forEach(function(x){ mem[x] = 1; });
    var sum = 0;
    items.forEach(function(i){
      var p = byCode[i.code];
      if (mem[i.code] && p && p.capacityPerUnit) sum += i.qty * p.capacityPerUnit;
    });
    if (!sum) return;
    var met = sum >= m.minOrderQty;
    var pct = Math.min(100, m.minOrderQty > 0 ? sum / m.minOrderQty * 100 : 100);
    h += '<div class="gauge' + (met ? ' met' : '') + '">原料: ' + esc(m.name) + ': ' + sum.toLocaleString('ja-JP') + ' / ' + m.minOrderQty.toLocaleString('ja-JP') + esc(m.unit || '') +
      (met ? ' ✅' : ' — <b>あと ' + (m.minOrderQty - sum).toLocaleString('ja-JP') + esc(m.unit || '') + '</b>') +
      '<div class="bar"><div style="width:' + pct + '%"></div></div></div>';
  });
  document.getElementById('condArea').innerHTML = h ? '<div class="sec"><h2>📏 発注条件</h2><div class="bd">' + h + '</div></div>' : '';
}
function renderCart() {
  var items = cartItems();
  var totQ = 0, totA = 0;
  var h = '';
  if (!items.length) h = '<div class="muted">カートは空です。各リストの「発注数」に数量を入れてください。</div>';
  else {
    h = '<table class="t">';
    items.forEach(function(i) {
      var p = byCode[i.code];
      totQ += i.qty; totA += i.qty * (p.cost || 0);
      h += '<tr><td>' + esc(String(p.name || '').slice(0, 60)) + '</td><td class="r">' + i.qty.toLocaleString('ja-JP') + '</td><td class="r">' + (p.cost ? yen(i.qty * p.cost) : '—') + '</td>' +
        '<td><button class="ghost" data-del="' + esc(i.code) + '">✕</button></td></tr>';
    });
    h += '</table>';
  }
  h += '<div class="tot">' + items.length + ' SKU / ' + totQ.toLocaleString('ja-JP') + ' 個 / <b>' + yen(totA) + '</b> <span class="muted">(原価)</span></div>';
  h += '<textarea id="orderNote" placeholder="メモ (任意)" style="width:100%;height:44px">' + esc(D.draft ? D.draft.note : '') + '</textarea>';
  h += '<div style="display:flex;gap:8px;margin-top:8px"><button id="btnSave">💾 下書き保存</button><button class="pri" id="btnIssue">✅ 発注確定</button></div>';
  document.getElementById('cartArea').innerHTML = h;
}
function renderAll(){ renderConditions(); renderCart(); }

document.addEventListener('input', function(ev) {
  var code = ev.target.getAttribute && ev.target.getAttribute('data-code');
  if (!code) return;
  var v = parseInt(ev.target.value, 10);
  if (!v || v <= 0) delete CART[code]; else CART[code] = v;
  renderAll();
});
document.addEventListener('click', function(ev) {
  var del = ev.target.getAttribute && ev.target.getAttribute('data-del');
  if (del) {
    delete CART[del];
    var inp = document.querySelector('input[data-code="' + del.replace(/"/g, '\\\\"') + '"]');
    if (inp) inp.value = '';
    renderAll();
    return;
  }
  var sec = ev.target.getAttribute && ev.target.getAttribute('data-sec');
  if (sec === 'hori') {
    var el = document.getElementById('secHori');
    el.style.display = el.style.display === 'none' ? '' : 'none';
  }
  if (ev.target.id === 'btnSave') save(false);
  if (ev.target.id === 'btnIssue') save(true);
});

function save(issue) {
  var items = cartItems().map(function(i){ return { code: i.code, qty: i.qty }; });
  if (issue && !items.length) { toast('カートが空です'); return; }
  if (issue && !confirm('この内容で発注確定しますか? (確定後は履歴に記録されます。NEへの発注登録は別途手動)')) return;
  var url = '/apps/purchase-orders/api/supplier/' + encodeURIComponent(D.supplier.code) + (issue ? '/issue' : '/draft');
  fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: items, note: document.getElementById('orderNote').value, supplierName: D.supplier.name }),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    if (issue) { showDone(j.id); toast('発注確定しました'); }
    else toast(j.deleted ? '下書きを削除しました' : '下書きを保存しました');
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
}
function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(function(){ toast('コピーしました'); },
    function(){ toast('コピーに失敗しました。手動で選択してください'); });
}
function showDone(orderId) {
  var items = cartItems();
  var lines = items.map(function(i){ return i.code + '\\t' + (byCode[i.code] ? byCode[i.code].name : '') + '\\t' + i.qty; });
  var text = lines.join('\\n');
  var area = document.getElementById('doneArea');
  var totA = 0;
  items.forEach(function(i){ totA += i.qty * (byCode[i.code].cost || 0); });
  document.getElementById('doneBody').innerHTML =
    '<div class="tot">発注 #' + orderId + ' — ' + items.length + ' SKU / 合計 ' + yen(totA) + ' <button class="pri" id="btnCopy">📋 リストをコピー</button> <a href="/apps/purchase-orders/orders">履歴を見る →</a></div>' +
    '<pre class="copy">' + esc('商品コード\\t商品名\\t数量\\n' + text) + '</pre>';
  area.style.display = '';
  document.getElementById('btnCopy').addEventListener('click', function(){ copyText(text); });
  area.scrollIntoView({ behavior: 'smooth' });
}

renderLists();
renderAll();`;
  res.send(pageShell(`発注 — ${data.supplier.name}`, '', body, script));
});

// ─── 画面3: 発注履歴 ───
router.get('/orders', (req, res) => {
  const body = `
    <h2 style="font-size:15px">発注履歴</h2>
    <div id="list">読み込み中…</div>
    <div class="sec" id="detail" style="display:none"><h2 id="detailTitle"></h2><div class="bd" id="detailBody"></div></div>`;
  const script = `
function load() {
  fetch('/apps/purchase-orders/api/orders').then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { document.getElementById('list').textContent = 'エラー: ' + j.error; return; }
    if (!j.orders.length) { document.getElementById('list').innerHTML = '<div class="muted">履歴はまだありません</div>'; return; }
    var h = '<table class="t"><tr><th>#</th><th>状態</th><th>仕入先</th><th class="r">SKU</th><th class="r">数量</th><th class="r">金額(原価)</th><th>確定日時</th><th>メモ</th><th></th></tr>';
    j.orders.forEach(function(o) {
      var st = o.status === 'issued' ? '<span class="badge b-issued">確定</span>' : '<span class="badge b-draft">下書き</span>';
      var when = o.issued_at ? new Date(o.issued_at).toLocaleString('ja-JP') : '—';
      h += '<tr><td>' + o.id + '</td><td>' + st + '</td>' +
        '<td><a href="/apps/purchase-orders/supplier/' + encodeURIComponent(o.supplier_code) + '">' + esc(o.supplier_name) + '</a></td>' +
        '<td class="r">' + o.sku_count + '</td><td class="r">' + o.total_qty.toLocaleString('ja-JP') + '</td><td class="r">' + yen(o.total_amount) + '</td>' +
        '<td>' + when + '</td><td>' + esc(o.note || '') + '</td>' +
        '<td><button class="ghost" data-id="' + o.id + '">明細</button></td></tr>';
    });
    document.getElementById('list').innerHTML = h + '</table>';
  });
}
document.addEventListener('click', function(ev) {
  var id = ev.target.getAttribute && ev.target.getAttribute('data-id');
  if (!id) return;
  fetch('/apps/purchase-orders/api/orders/' + id).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast(j.error); return; }
    var o = j.order;
    var lines = o.items.map(function(i){ return i.product_code + '\\t' + (i.product_name || '') + '\\t' + i.qty; });
    var text = lines.join('\\n');
    document.getElementById('detailTitle').textContent = '発注 #' + o.id + ' — ' + o.supplier_name;
    document.getElementById('detailBody').innerHTML =
      '<button class="pri" id="btnCopyDetail">📋 リストをコピー</button>' +
      '<pre class="copy">' + esc('商品コード\\t商品名\\t数量\\n' + text) + '</pre>';
    document.getElementById('detail').style.display = '';
    document.getElementById('btnCopyDetail').addEventListener('click', function() {
      navigator.clipboard.writeText(text).then(function(){ toast('コピーしました'); });
    });
  });
});
load();`;
  res.send(pageShell('発注補助 — 発注履歴', '', body, script));
});

// ─── 画面4: マスタ管理 ───
router.get('/admin', (req, res) => {
  const body = `
    <h2 style="font-size:15px">マスタ管理 <span class="muted">(本アプリが正本。旧スプシ「発注条件マスタ」は凍結してください)</span></h2>
    <div class="tabbar">
      <button data-tab="suppliers" class="on">仕入先</button>
      <button data-tab="conditions">発注条件グループ</button>
      <button data-tab="materials">原料グループ</button>
      <button data-tab="attrs">商品紐付け</button>
      <button data-tab="unlinked">未紐付けチェック</button>
    </div>
    <div class="sec"><div class="bd" id="tabBody">読み込み中…</div></div>`;
  const script = `
var TAB = 'suppliers';
var DEFS = {
  suppliers: { title: '仕入先', cols: [
    { k: 'supplier_code', l: '仕入先コード', pk: 1 }, { k: 'name', l: '仕入先名' }, { k: 'order_memo', l: '発注メモ (FAX/WEB/送料条件等)' } ] },
  conditions: { title: '発注条件グループ', cols: [
    { k: 'condition_id', l: '条件ID', pk: 1 }, { k: 'supplier_code', l: '仕入先コード' }, { k: 'maker_name', l: 'メーカー名' },
    { k: 'display_name', l: '管理名' }, { k: 'condition_type', l: '条件タイプ' }, { k: 'condition_value', l: '条件値', num: 1 }, { k: 'unit', l: '単位' } ] },
  materials: { title: '原料グループ', cols: [
    { k: 'group_id', l: '原料グループID', pk: 1 }, { k: 'name', l: '原料グループ名' }, { k: 'min_order_qty', l: '最低発注量', num: 1 }, { k: 'unit', l: '単位' } ] },
  attrs: { title: '商品紐付け', cols: [
    { k: 'product_code', l: '商品コード', pk: 1 }, { k: 'condition_id', l: '発注条件グループID' }, { k: 'material_group_id', l: '原料グループID' },
    { k: 'capacity_per_unit', l: '容量/個', num: 1 }, { k: 'case_group', l: 'ケースグループ' }, { k: 'case_lot', l: 'ケースロット', num: 1 } ] },
};
function render(rows) {
  var def = DEFS[TAB];
  var h = '<div class="toolbar">' +
    '<form id="csvForm" style="display:flex;gap:6px;align-items:center">' +
    '<input type="file" name="file" accept=".csv" required> <button type="submit">CSV一括取込 (upsert)</button>' +
    '<span class="muted">見出し: ' + def.cols.map(function(c){ return c.l; }).join(' / ').slice(0, 120) + '</span></form>' +
    '<input type="text" id="filter" placeholder="絞り込み" style="margin-left:auto"></div>';
  h += '<table class="t" id="mtable"><tr>' + def.cols.map(function(c){ return '<th>' + c.l + '</th>'; }).join('') + '<th></th></tr>';
  h += '<tr>' + def.cols.map(function(c){ return '<td><input type="text" style="width:98%" id="new_' + c.k + '"></td>'; }).join('') + '<td><button id="btnAdd">追加</button></td></tr>';
  rows.forEach(function(r) {
    h += '<tr data-row="1">' + def.cols.map(function(c) {
      return '<td' + (c.pk ? '' : ' contenteditable data-k="' + c.k + '"') + '>' + esc(r[c.k] == null ? '' : r[c.k]) + '</td>';
    }).join('') + '<td><button class="ghost" data-save="' + esc(r[def.cols[0].k]) + '">保存</button> <button class="ghost" data-rm="' + esc(r[def.cols[0].k]) + '">削除</button></td></tr>';
  });
  h += '</table><div class="muted" style="margin-top:6px">' + rows.length + ' 件 — セルを直接編集して「保存」。追加は最上行。</div>';
  document.getElementById('tabBody').innerHTML = h;
  document.getElementById('csvForm').addEventListener('submit', function(ev) {
    ev.preventDefault();
    var fd = new FormData(ev.target);
    fetch('/apps/purchase-orders/api/masters/' + TAB + '/csv', { method: 'POST', body: fd })
      .then(function(r){ return r.json(); }).then(function(j) {
        if (!j.ok) { toast('取込エラー: ' + j.error); return; }
        toast('取込完了: ' + j.upserted + '件' + (j.skipped ? ' (スキップ' + j.skipped + '件)' : ''));
        load();
      });
  });
  document.getElementById('filter').addEventListener('input', function(ev) {
    var q = ev.target.value.trim().toLowerCase();
    document.querySelectorAll('#mtable tr[data-row]').forEach(function(tr) {
      tr.style.display = !q || tr.textContent.toLowerCase().indexOf(q) >= 0 ? '' : 'none';
    });
  });
}
function renderUnlinked(j) {
  var h = '<div class="muted" style="margin-bottom:8px">取扱中なのに商品紐付け (発注条件/原料グループ等) が未登録の商品。グループ対象外の商品はそのままで問題ありません。全 ' + j.count + ' 件' + (j.count > 500 ? ' (先頭500件表示)' : '') + '</div>';
  h += '<table class="t"><tr><th>商品コード</th><th>商品名</th><th>仕入先</th></tr>';
  j.rows.forEach(function(r){ h += '<tr><td>' + esc(r.code) + '</td><td>' + esc(r.name) + '</td><td>' + esc(r.supplier) + '</td></tr>'; });
  document.getElementById('tabBody').innerHTML = h + '</table>';
}
function load() {
  document.getElementById('tabBody').textContent = '読み込み中…';
  if (TAB === 'unlinked') {
    fetch('/apps/purchase-orders/api/attrs/unlinked').then(function(r){ return r.json(); })
      .then(function(j){ if (j.ok) renderUnlinked(j); else document.getElementById('tabBody').textContent = j.error; });
    return;
  }
  fetch('/apps/purchase-orders/api/masters/' + TAB).then(function(r){ return r.json(); })
    .then(function(j){ if (j.ok) render(j.rows); else document.getElementById('tabBody').textContent = j.error; });
}
document.addEventListener('click', function(ev) {
  var t = ev.target;
  var tab = t.getAttribute && t.getAttribute('data-tab');
  if (tab) {
    TAB = tab;
    document.querySelectorAll('.tabbar button').forEach(function(b){ b.classList.toggle('on', b === t); });
    load();
    return;
  }
  if (t.id === 'btnAdd') {
    var def = DEFS[TAB], body = {};
    def.cols.forEach(function(c){ body[c.k] = document.getElementById('new_' + c.k).value; });
    post(body);
    return;
  }
  var saveKey = t.getAttribute && t.getAttribute('data-save');
  if (saveKey) {
    var def2 = DEFS[TAB], tr = t.closest('tr'), body2 = {};
    body2[def2.cols[0].k] = saveKey;
    tr.querySelectorAll('[data-k]').forEach(function(td){ body2[td.getAttribute('data-k')] = td.textContent; });
    post(body2);
    return;
  }
  var rmKey = t.getAttribute && t.getAttribute('data-rm');
  if (rmKey) {
    if (!confirm('削除しますか? ' + rmKey)) return;
    fetch('/apps/purchase-orders/api/masters/' + TAB + '/' + encodeURIComponent(rmKey), { method: 'DELETE' })
      .then(function(r){ return r.json(); }).then(function(j){ if (j.ok) { toast('削除しました'); load(); } else toast(j.error); });
  }
});
function post(body) {
  fetch('/apps/purchase-orders/api/masters/' + TAB, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (j.ok) { toast('保存しました'); load(); } else toast('エラー: ' + j.error);
  });
}
load();`;
  res.send(pageShell('発注補助 — マスタ管理', '', body, script));
});

export default router;
