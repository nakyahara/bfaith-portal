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
import { computeAll, loadPmlMerged, loadMasters, evaluateCondition } from './logic.js';

// ─── miniPC (WarehouseServer) service-api 呼び出し (オンデマンドFBA更新、product-management-list と同一エンドポイント) ───
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}
async function callWarehouse(fullPath, { method = 'GET', timeout = 30000 } = {}) {
  const requestId = `po-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const res = await fetch(`${WAREHOUSE_URL}${fullPath}`, {
    method, headers: { ...getServiceHeaders(), 'x-request-id': requestId },
    redirect: 'manual', signal: AbortSignal.timeout(timeout),
  });
  if (res.status === 302 || res.status === 303) throw new Error(`CF Access認証構成異常 (${res.status})`);
  if (res.status === 401 || res.status === 403) throw new Error(`認証失敗 HTTP ${res.status}`);
  if (!res.ok) { const t = await res.text().catch(() => ''); throw new Error(`ミニPC HTTP ${res.status}: ${t.slice(0, 200)}`); }
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('application/json')) throw new Error(`レスポンス形式異常 (ct=${ct || 'none'})`);
  return res.json();
}

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

/** CSV バッファ → 行配列。文字コードは厳密UTF-8判定→ダメなら Shift_JIS。RFC4180 対応の自前パーサ */
function decodeCsvBuffer(buf) {
  // UTF-8 BOM は除去
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    return buf.slice(3).toString('utf8');
  }
  // 厳密UTF-8デコードを試す。成功すれば UTF-8 (商品名に壊れ文字 � が含まれていても、
  //   バイト列として妥当な UTF-8 なら化けさせずそのまま採用する)。失敗時のみ Shift_JIS。
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buf);
  } catch {
    return iconv.decode(buf, 'Shift_JIS');
  }
}
function parseCsvBuffer(buf) {
  const text = decodeCsvBuffer(buf);
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
// 数値化。スプレッドシート由来の "20,000" 等のカンマ区切りも許容する。
const numOrNull = v => {
  const s = trimS(v).replace(/,/g, '');
  if (s === '') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

// ─── 発注 draft/issue の永続化 ───
function enrichItems(rawItems, supplierCode) {
  // 現 PML(+NEオーバーレイ) に存在し「かつ当該仕入先の」商品コードのみ許可し、商品名・原価をスナップショットする
  // (Codex R1 High: 仕入先突合なしだと別仕入先の商品を混入できる)
  const { pub, rows } = loadPmlMerged();
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
    if (normSupplierCode(r['仕入先']) !== supplierCode) {
      throw new Error(`仕入先が一致しない商品コード: ${code} (この商品の仕入先: ${r['仕入先'] || '未設定'})`);
    }
    items.push({ code: r['商品コード'], key, name: r['商品名'] || '', qty, cost: r['原価'] == null ? null : Number(r['原価']) });
  }
  return { items, pmlAsOf: pub.as_of_date || null };
}

/** 仕入先名はサーバ側で解決 (クライアント値は信用しない) */
function resolveSupplierName(supplierCode) {
  const db = getDB();
  const s = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplierCode);
  return (s && s.name) || `仕入先 ${supplierCode}`;
}

function insertItems(db, orderId, items) {
  const ins = db.prepare('INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty, unit_cost) VALUES (?,?,?,?,?,?)');
  for (const it of items) ins.run(orderId, it.code, it.key, it.name, it.qty, it.cost);
}

function upsertDraft(supplierCode, supplierName, rawItems, note) {
  const db = getDB();
  const { items, pmlAsOf } = enrichItems(rawItems, supplierCode);
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
    insertItems(db, order.id, items);
    return order.id;
  })();
}

/**
 * 発注確定 (Codex R2 High): draft を mutate せず issued order を新規 insert し、
 * 既存 draft の削除まで1トランザクションで行う。並行する draft 更新と混ざらない。
 */
function issueOrder(supplierCode, supplierName, rawItems, note) {
  const db = getDB();
  const { items, pmlAsOf } = enrichItems(rawItems, supplierCode);
  const now = nowIso();
  return db.transaction(() => {
    const info = db.prepare(
      "INSERT INTO po_orders (supplier_code, supplier_name, status, note, pml_as_of_date, created_at, updated_at, issued_at) VALUES (?,?,'issued',?,?,?,?,?)"
    ).run(supplierCode, supplierName, note || null, pmlAsOf, now, now, now);
    const id = Number(info.lastInsertRowid);
    insertItems(db, id, items);
    db.prepare("DELETE FROM po_orders WHERE supplier_code=? AND status='draft'").run(supplierCode);
    return id;
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
  const { pub, overlay, products, bySupplier, masters } = computeAll();
  const g = bySupplier.get(code);
  const sm = masters.suppliers.get(code);
  if (!g && !sm) return null;
  const db = getDB();
  const draftRow = db.prepare("SELECT id, note FROM po_orders WHERE supplier_code=? AND status='draft'").get(code);
  const draft = draftRow ? {
    id: draftRow.id, note: draftRow.note || '',
    items: db.prepare('SELECT product_code AS code, product_key AS key, product_name AS name, qty, unit_cost AS cost FROM po_order_items WHERE order_id=? ORDER BY id').all(draftRow.id),
  } : null;
  const all = g ? [...g.targets, ...g.candidates, ...g.horikoshi] : [];
  // draft 明細のうち現在の表示リスト外の商品 (PML更新で対象外化・取扱中止化した等) も
  // 黙って落とさず「リスト外」として返す (Codex R2 Medium)
  let draftExtras = [];
  const extraProducts = []; // 条件/原料グループ membership 集計にも含める (Codex R3 Medium)
  if (draft) {
    const listKeys = new Set(all.map(p => p.key));
    const byKey = new Map(products.map(p => [p.key, p]));
    draftExtras = draft.items.filter(it => !listKeys.has(it.key)).map(it => {
      const p = byKey.get(it.key);
      if (p) { extraProducts.push(p); return { ...productDto(p), extra: true }; }
      return { code: it.code, name: it.name || '', cost: it.cost, stock: null, backOrder: null, sales30: null, sales7: null, lot: null, stockMonths: null, recQty: null, extra: true, missing: true };
    });
  }
  const memberBase = [...all, ...extraProducts];
  const memberOf = (fieldVal, field) => memberBase.filter(p => p[field] === fieldVal).map(p => p.code);
  const condIds = new Set(memberBase.map(p => p.conditionId).filter(Boolean));
  const conditions = masters.conditions
    .filter(c => condIds.has(c.condition_id) || normSupplierCode(c.supplier_code) === code)
    .map(c => ({
      conditionId: c.condition_id, displayName: c.display_name, makerName: c.maker_name || '',
      conditionType: c.condition_type, conditionValue: c.condition_value, unit: c.unit || '',
      memberCodes: memberOf(c.condition_id, 'conditionId'),
    }));
  const matIds = new Set(memberBase.map(p => p.materialGroupId).filter(Boolean));
  const materialGroups = [...matIds].map(id => {
    const m = masters.materialGroups.get(id);
    return {
      groupId: id, name: m ? m.name : id,
      minOrderQty: m ? m.min_order_qty : null, unit: m ? (m.unit || '') : '',
      memberCodes: memberOf(id, 'materialGroupId'),
    };
  });
  return {
    pub: pub ? {
      as_of_date: pub.as_of_date, status: pub.status, synced_at: pub.synced_at,
      src_ne_products_synced_at: pub.src_ne_products_synced_at || null,
      fba_source_kind: pub.fba_source_kind || null, fba_fetched_at: pub.fba_fetched_at || null,
      src_fba_business_date: pub.src_fba_business_date || null,
    } : null,
    overlay,
    supplier: { code, name: (sm && sm.name) || (g && g.name) || `仕入先 ${code}`, memo: (sm && sm.order_memo) || '' },
    targets: g ? g.targets.map(productDto) : [],
    candidates: g ? g.candidates.map(productDto) : [],
    horikoshi: g ? g.horikoshi.map(productDto) : [],
    conditions, materialGroups, draft, draftExtras,
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
    const { items, note } = req.body || {};
    if (!Array.isArray(items)) return res.status(400).json({ ok: false, error: 'items が必要です' });
    if (items.length === 0) {
      const db = getDB();
      db.prepare("DELETE FROM po_orders WHERE supplier_code=? AND status='draft'").run(code);
      return res.json({ ok: true, deleted: true });
    }
    const id = upsertDraft(code, resolveSupplierName(code), items, trimS(note));
    res.json({ ok: true, id });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/supplier/:code/issue', (req, res) => {
  try {
    const code = normSupplierCode(req.params.code);
    const { items, note } = req.body || {};
    if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ ok: false, error: '発注明細が空です' });
    const id = issueOrder(code, resolveSupplierName(code), items, trimS(note));
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
    table: 'po_product_attrs', pk: 'product_key', normId: normProductCode,
    fromBody: b => ({
      product_key: normProductCode(b.product_code), product_code: trimS(b.product_code),
      condition_id: trimS(b.condition_id) || null, material_group_id: trimS(b.material_group_id) || null,
      capacity_per_unit: numOrNull(b.capacity_per_unit), case_group: trimS(b.case_group) || null, case_lot: numOrNull(b.case_lot),
    }),
    validate: r => {
      if (!r.product_code) return '商品コード必須';
      if (r.capacity_per_unit != null && r.capacity_per_unit <= 0) return '容量/個が不正';
      if (r.case_lot != null && r.case_lot <= 0) return 'ケースロットが不正';
      // 参照整合性 (Codex R1 Medium): 存在しないグループへの紐付けは静かに欠落するため fail-closed
      const db = getDB();
      if (r.condition_id && !db.prepare('SELECT 1 FROM po_order_conditions WHERE condition_id=?').get(r.condition_id)) {
        return `発注条件グループが未登録: ${r.condition_id}`;
      }
      if (r.material_group_id && !db.prepare('SELECT 1 FROM po_material_groups WHERE group_id=?').get(r.material_group_id)) {
        return `原料グループが未登録: ${r.material_group_id}`;
      }
      return null;
    },
    csvHeader: ['商品コード', '発注条件グループID', '原料グループID', '容量_per_個', 'ケースグループ', 'ケースロット'],
    fromCsv: c => ({
      product_key: normProductCode(c[0]), product_code: trimS(c[0]),
      condition_id: trimS(c[1]) || null, material_group_id: trimS(c[2]) || null,
      capacity_per_unit: numOrNull(c[3]), case_group: trimS(c[4]) || null, case_lot: numOrNull(c[5]),
    }),
  },
};

/** マスタ削除前の被参照チェック (dangling reference 防止、Codex R1 Medium) */
const DELETE_REF_CHECKS = {
  conditions: id => {
    const n = getDB().prepare('SELECT COUNT(*) c FROM po_product_attrs WHERE condition_id=?').get(id).c;
    return n ? `商品紐付け ${n} 件から参照されています。先に紐付けを外してください` : null;
  },
  materials: id => {
    const n = getDB().prepare('SELECT COUNT(*) c FROM po_product_attrs WHERE material_group_id=?').get(id).c;
    return n ? `商品紐付け ${n} 件から参照されています。先に紐付けを外してください` : null;
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
    const id = def.normId ? def.normId(req.params.id) : req.params.id;
    const refCheck = DELETE_REF_CHECKS[req.params.kind];
    if (refCheck) {
      const err = refCheck(id);
      if (err) return res.status(400).json({ ok: false, error: err });
    }
    const info = getDB().prepare(`DELETE FROM ${def.table} WHERE ${def.pk}=?`).run(id);
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
    // fail-closed (Codex R2 Medium): 1行でも検証エラーがあれば全件 rollback (マスタの部分反映を防ぐ)
    const db = getDB();
    let upserted = 0; const errors = [];
    try {
      db.transaction(() => {
        for (let i = 1; i < rows.length; i++) {
          const row = def.fromCsv(rows[i]);
          const err = def.validate(row);
          if (err) { errors.push(`行${i + 1}: ${err}`); continue; }
          upsertMasterRow(def, row);
          upserted++;
        }
        if (errors.length > 0) throw new Error('validation');
      })();
    } catch (e) {
      if (errors.length > 0) {
        return res.status(400).json({
          ok: false,
          error: `検証エラー ${errors.length} 件のため全件取り込みを中止しました (1件も反映していません)`,
          errors: errors.slice(0, 20),
        });
      }
      throw e;
    }
    res.json({ ok: true, upserted });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// ─── スプレッドシートCSV 自動判別取込 (生のダウンロードCSVをそのまま受ける) ───
// ヘッダー名の正規化 (空白/改行/全角空白/引用符を除去)。列名の表記ゆれを吸収する。
const normHeader = s => String(s == null ? '' : s).replace(/[\s　"']/g, '');
function colIndex(header, ...aliases) {
  const norm = header.map(normHeader);
  const wants = aliases.map(normHeader);
  for (let i = 0; i < norm.length; i++) if (wants.includes(norm[i])) return i;
  return -1;
}
const hasCol = (header, ...a) => colIndex(header, ...a) >= 0;

// 数値パース (カンマ許容)。非空なのに数値化できない値は warn に記録して null (黙って欠落させない)。
function numOrWarn(raw, warn, ctx) {
  const s = trimS(raw).replace(/,/g, '');
  if (s === '') return null;
  const n = Number(s);
  if (!Number.isFinite(n)) { if (warn) warn(`${ctx}: 数値でない値「${trimS(raw)}」を無視しました`); return null; }
  return n;
}

// 各シート形式の判別 + 抽出。商品マスタは1枚から「仕入先(重複排除)」と「商品紐付け」の2種を作る。
// extract(header, rows, warn): warn(msg) で行単位の注意を報告できる。
const IMPORT_RECIPES = [
  {
    key: 'materials', label: '原料グループマスタ',
    detect: h => hasCol(h, '原料グループID') && (hasCol(h, '最低発注量') || hasCol(h, '原料グループ名')),
    extract(header, rows, warn) {
      const iId = colIndex(header, '原料グループID'), iName = colIndex(header, '原料グループ名', '名称', '名前');
      const iMin = colIndex(header, '最低発注量'), iUnit = colIndex(header, '単位');
      const out = [];
      for (let r = 1; r < rows.length; r++) {
        const c = rows[r]; const id = trimS(c[iId]); if (!id) continue;
        out.push({ table: 'materials', row: { group_id: id, name: trimS(c[iName]) || id, min_order_qty: iMin >= 0 ? numOrWarn(c[iMin], warn, `原料グループマスタ ${r + 1}行目 最低発注量`) : null, unit: iUnit >= 0 ? (trimS(c[iUnit]) || null) : null } });
      }
      return out;
    },
  },
  {
    key: 'conditions', label: '発注条件マスタ',
    detect: h => hasCol(h, '条件ID') && hasCol(h, '条件タイプ'),
    extract(header, rows, warn) {
      const iId = colIndex(header, '条件ID'), iSup = colIndex(header, '仕入先コード', '仕入先');
      const iMaker = colIndex(header, 'メーカー名', 'メーカー'), iName = colIndex(header, '管理名', '商品グループID管理名', 'グループ管理名');
      const iType = colIndex(header, '条件タイプ'), iVal = colIndex(header, '条件値'), iUnit = colIndex(header, '単位');
      const out = [];
      for (let r = 1; r < rows.length; r++) {
        const c = rows[r]; const id = trimS(c[iId]); if (!id) continue;
        out.push({ table: 'conditions', row: {
          condition_id: id,
          supplier_code: iSup >= 0 ? (normSupplierCode(c[iSup]) || null) : null,
          maker_name: iMaker >= 0 ? (trimS(c[iMaker]) || null) : null,
          display_name: iName >= 0 ? (trimS(c[iName]) || id) : id,
          condition_type: trimS(c[iType]), condition_value: numOrWarn(c[iVal], warn, `発注条件マスタ ${r + 1}行目(${id}) 条件値`),
          unit: iUnit >= 0 ? (trimS(c[iUnit]) || null) : null,
        } });
      }
      return out;
    },
  },
  {
    key: 'shohin', label: '商品マスタ (→ 仕入先 + 商品紐付け)',
    // 商品コード + 発注特有の列 (グループID/容量/ケース) が揃うものだけ。NE商品マスタCSV(商品コード+仕入先のみ)は弾く。
    detect: h => hasCol(h, '商品コード') && (hasCol(h, '商品グループID') || hasCol(h, '発注条件グループID') || hasCol(h, '原料グループID') || hasCol(h, '容量/個') || hasCol(h, '容量') || hasCol(h, 'ケースグループ') || hasCol(h, 'ケースロット')),
    extract(header, rows, warn) {
      const iCode = colIndex(header, '商品コード'), iSup = colIndex(header, '仕入先コード', '仕入先'), iSupName = colIndex(header, '仕入れ先名', '仕入先名');
      const iGrp = colIndex(header, '商品グループID', '発注条件グループID'), iMat = colIndex(header, '原料グループID');
      const iCap = colIndex(header, '容量/個', '容量_per_個', '容量'), iCg = colIndex(header, 'ケースグループ'), iCl = colIndex(header, 'ケースロット');
      const out = []; const suppliers = new Map();
      for (let r = 1; r < rows.length; r++) {
        const c = rows[r]; const code = trimS(c[iCode]); if (!code) continue;
        if (iSup >= 0) {
          const sc = normSupplierCode(c[iSup]); const sn = iSupName >= 0 ? trimS(c[iSupName]) : '';
          if (sc && sc.toUpperCase() !== '#N/A') { if (!suppliers.has(sc) || (sn && !suppliers.get(sc))) suppliers.set(sc, sn); }
        }
        const cond = iGrp >= 0 ? (trimS(c[iGrp]) || null) : null, mat = iMat >= 0 ? (trimS(c[iMat]) || null) : null;
        const cap = iCap >= 0 ? numOrWarn(c[iCap], warn, `商品マスタ ${r + 1}行目(${code}) 容量/個`) : null;
        const cg = iCg >= 0 ? (trimS(c[iCg]) || null) : null, cl = iCl >= 0 ? numOrWarn(c[iCl], warn, `商品マスタ ${r + 1}行目(${code}) ケースロット`) : null;
        if (cond || mat || cap || cg || cl) {
          out.push({ table: 'attrs', row: {
            product_key: normProductCode(code), product_code: code,
            condition_id: cond, material_group_id: mat,
            capacity_per_unit: (cap != null && cap > 0) ? cap : null, case_group: cg, case_lot: (cl != null && cl > 0) ? cl : null,
          } });
        }
      }
      for (const [sc, sn] of suppliers) out.push({ table: 'suppliers', row: { supplier_code: sc, name: sn || `仕入先 ${sc}`, order_memo: null } });
      return out;
    },
  },
];

// bulk取込の最低限バリデーション (満たさない行はスキップし件数を報告。全件rollbackはしない)
function bulkValid(table, row) {
  if (table === 'materials') return !!row.group_id && !!row.name;
  if (table === 'conditions') return !!row.condition_id && !!row.display_name && !!row.condition_type && row.condition_value != null && row.condition_value >= 0;
  if (table === 'suppliers') return !!row.supplier_code && !!row.name;
  if (table === 'attrs') return !!row.product_code;
  return false;
}
// スキップ理由 (どの必須が欠けたか) を人間に返す
function bulkInvalidReason(table, row) {
  if (table === 'materials') return `${row.group_id || '(ID空)'} — 原料グループID/名称が不足`;
  if (table === 'conditions') {
    const bits = [];
    if (!row.condition_type) bits.push('条件タイプ空');
    if (row.condition_value == null) bits.push('条件値が数値でない');
    else if (row.condition_value < 0) bits.push('条件値が負');
    return `${row.condition_id || '(ID空)'} — ${bits.join('/') || '必須不足'}`;
  }
  if (table === 'suppliers') return `${row.supplier_code || '(コード空)'} — 仕入先コード/名称が不足`;
  if (table === 'attrs') return '商品コードが空';
  return '不正な行';
}
const TABLE_LABEL = { materials: '原料グループ', conditions: '発注条件グループ', suppliers: '仕入先', attrs: '商品紐付け' };

router.post('/api/import', upload.array('files', 12), (req, res) => {
  if (!req.files || !req.files.length) return res.status(400).json({ ok: false, error: 'CSVファイルを選択してください' });
  const classified = []; const fileErrors = []; const warnings = [];
  const warn = m => { if (warnings.length < 200) warnings.push(m); };
  for (const f of req.files) {
    let buf; try { buf = fs.readFileSync(f.path); } finally { try { fs.unlinkSync(f.path); } catch {} }
    let rows;
    try { rows = parseCsvBuffer(buf); } catch (e) { fileErrors.push(`${f.originalname}: 読込失敗 (${e.message})`); continue; }
    if (rows.length < 2) { fileErrors.push(`${f.originalname}: データ行がありません`); continue; }
    const header = rows[0].map(trimS);
    const recipe = IMPORT_RECIPES.find(rc => rc.detect(header));
    if (!recipe) { fileErrors.push(`${f.originalname}: 種類を判別できません (見出し: ${header.slice(0, 6).join(' / ')})`); continue; }
    // 列数ズレ検出 (未閉じクォート等で行が壊れていないか)。空行は無視。
    let mismatch = 0;
    for (let r = 1; r < rows.length; r++) {
      const c = rows[r]; if (c.length === 1 && trimS(c[0]) === '') continue;
      if (c.length !== header.length) mismatch++;
    }
    if (mismatch) warn(`${f.originalname}: 見出し${header.length}列に対し列数が違う行が${mismatch}件あります (CSVの破損の可能性、値ズレに注意)`);
    classified.push({ name: f.originalname, recipe, items: recipe.extract(header, rows, warn) });
  }
  if (fileErrors.length) return res.status(400).json({ ok: false, error: '取り込めないファイルがあります', errors: fileErrors });
  // 依存順 (原料/条件 → 仕入先 → 紐付け) に並べて1トランザクションで upsert
  const order = { materials: 1, conditions: 2, suppliers: 3, attrs: 4 };
  const all = classified.flatMap(c => c.items).sort((a, b) => order[a.table] - order[b.table]);
  const counts = { materials: 0, conditions: 0, suppliers: 0, attrs: 0 };
  const skipped = { materials: 0, conditions: 0, suppliers: 0, attrs: 0 };
  const db = getDB();
  try {
    db.transaction(() => {
      for (const it of all) {
        if (!bulkValid(it.table, it.row)) {
          skipped[it.table]++;
          warn(`${TABLE_LABEL[it.table]}: ${bulkInvalidReason(it.table, it.row)} — 1件スキップ`);
          continue;
        }
        upsertMasterRow(MASTER_DEFS[it.table], it.row);
        counts[it.table]++;
      }
    })();
  } catch (e) { return res.status(500).json({ ok: false, error: '取込中にエラー: ' + e.message }); }
  // dangling参照チェック (attrs が指す 発注条件グループ / 原料グループ が未登録)。取込は止めず警告のみ。
  try {
    const dCond = db.prepare(`SELECT COUNT(*) n FROM po_product_attrs a WHERE a.condition_id IS NOT NULL AND a.condition_id<>'' AND NOT EXISTS (SELECT 1 FROM po_order_conditions c WHERE c.condition_id=a.condition_id)`).get().n;
    const dMat = db.prepare(`SELECT COUNT(*) n FROM po_product_attrs a WHERE a.material_group_id IS NOT NULL AND a.material_group_id<>'' AND NOT EXISTS (SELECT 1 FROM po_material_groups g WHERE g.group_id=a.material_group_id)`).get().n;
    if (dCond) warn(`未登録の発注条件グループを参照している商品が${dCond}件あります (発注条件マスタ側の登録漏れの可能性)`);
    if (dMat) warn(`未登録の原料グループを参照している商品が${dMat}件あります (原料グループマスタ側の登録漏れの可能性)`);
  } catch {}
  const summary = Object.keys(counts).filter(k => counts[k] || skipped[k])
    .map(k => `${TABLE_LABEL[k]} ${counts[k]}件${skipped[k] ? ` (スキップ${skipped[k]})` : ''}`);
  res.json({ ok: true, counts, skipped, summary, warnings, files: classified.map(c => ({ name: c.name, type: c.recipe.label })) });
});

// ─── NE商品マスタCSV オーバーレイ (日中の最新化) ───
// NE管理画面から手動DLした商品マスタCSV (Shift-JIS) を取り込み、在庫数・注残数・原価等を最新値で上書き計算する。
// FBA在庫・販売数・推奨保有月数はPMLのまま。翌朝の同期より古くなったら自動で無視される。
const NE_CSV_COLS = {
  商品コード: 'product_code', 仕入先コード: '仕入先コード', 原価: '原価', 売価: '売価',
  取扱区分: '取扱区分', 発注ロット単位: '発注ロット単位', 最終仕入日: '最終仕入日',
  在庫数: '在庫数', 引当数: '引当数', 発注残数: '発注残数',
};
router.post('/api/ne-overlay/csv', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: 'CSVファイルが必要です' });
  let buf;
  try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
  try {
    const rows = parseCsvBuffer(buf);
    if (rows.length < 2) return res.status(400).json({ ok: false, error: 'データ行がありません' });
    const header = rows[0].map(trimS);
    const idx = {};
    for (const [jp, key] of Object.entries(NE_CSV_COLS)) {
      const i = header.findIndex(h => h.replace(/[\s"（(].*$/, '') === jp || h === jp);
      if (i >= 0) idx[key] = i;
    }
    if (idx.product_code == null || idx.在庫数 == null || idx.発注残数 == null) {
      return res.status(400).json({
        ok: false,
        error: 'NE商品マスタCSVの見出しが見つかりません (商品コード/在庫数/発注残数 が必要)。ネクストエンジンの商品マスタDL CSVをそのままアップロードしてください',
      });
    }
    // NE CSVの数値セルは "1,234" 形式もあり得るためカンマ除去してから数値化
    const numLoose = v => {
      const s = trimS(v).replace(/,/g, '');
      if (s === '') return null;
      const n = Number(s);
      return Number.isFinite(n) ? n : null;
    };
    const db = getDB();
    const now = nowIso();
    let count = 0;
    const errors = [];
    try {
      db.transaction(() => {
        db.prepare('DELETE FROM po_ne_overlay_rows').run();
        const ins = db.prepare(`INSERT OR REPLACE INTO po_ne_overlay_rows
          (product_key, product_code, 仕入先コード, 原価, 売価, 取扱区分, 発注ロット単位, 最終仕入日, 在庫数, 引当数, 発注残数)
          VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
        for (let i = 1; i < rows.length; i++) {
          const c = rows[i];
          const code = trimS(c[idx.product_code]);
          if (!code) continue;
          // 発注計算の根幹となる在庫数・発注残数が数値でない行は fail-closed (Codex Medium)
          const zaiko = numLoose(c[idx.在庫数]);
          const chuzan = numLoose(c[idx.発注残数]);
          if (zaiko == null) { errors.push(`行${i + 1} (${code}): 在庫数が数値ではありません`); continue; }
          if (chuzan == null) { errors.push(`行${i + 1} (${code}): 発注残数が数値ではありません`); continue; }
          ins.run(
            normProductCode(code), code,
            idx.仕入先コード != null ? (trimS(c[idx.仕入先コード]) || null) : null,
            idx.原価 != null ? numLoose(c[idx.原価]) : null,
            idx.売価 != null ? numLoose(c[idx.売価]) : null,
            idx.取扱区分 != null ? (trimS(c[idx.取扱区分]) || null) : null,
            idx.発注ロット単位 != null ? numLoose(c[idx.発注ロット単位]) : null,
            idx.最終仕入日 != null ? (trimS(c[idx.最終仕入日]) || null) : null,
            zaiko,
            idx.引当数 != null ? numLoose(c[idx.引当数]) : null,
            chuzan,
          );
          count++;
        }
        if (errors.length > 0) throw new Error('validation');
        if (count === 0) throw new Error('有効な行がありません');
        db.prepare(`INSERT INTO po_ne_overlay_meta (id, uploaded_at, row_count, filename) VALUES (1,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET uploaded_at=excluded.uploaded_at, row_count=excluded.row_count, filename=excluded.filename`)
          .run(now, count, req.file.originalname || null);
      })();
    } catch (e) {
      if (errors.length > 0) {
        return res.status(400).json({
          ok: false,
          error: `数値でない行が ${errors.length} 件あるため全件取り込みを中止しました (1件も反映していません)`,
          errors: errors.slice(0, 10),
        });
      }
      throw e;
    }
    res.json({ ok: true, rowCount: count, uploadedAt: now });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.delete('/api/ne-overlay', (req, res) => {
  try {
    const db = getDB();
    db.transaction(() => {
      db.prepare('DELETE FROM po_ne_overlay_rows').run();
      db.prepare('DELETE FROM po_ne_overlay_meta').run();
    })();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── オンデマンドFBA更新 (miniPCジョブ起動 + 状態プロキシ、product-management-list Part2 と同一) ───
router.post('/api/refresh-fba', async (req, res) => {
  try {
    const r = await callWarehouse('/service-api/fba/pml/fba-refresh', { method: 'POST', timeout: 30000 });
    res.json(r);
  } catch (e) { res.status(502).json({ error: 'ミニPCへの接続に失敗: ' + e.message }); }
});
router.get('/api/refresh-fba/jobs/:jobId', async (req, res) => {
  try {
    const r = await callWarehouse(`/service-api/jobs/${encodeURIComponent(req.params.jobId)}`, { timeout: 15000 });
    res.json(r);
  } catch (e) { res.status(502).json({ error: 'ジョブ状態の取得に失敗: ' + e.message }); }
});

// 取扱中なのにどのグループにも未紐付けの商品。新商品の登録漏れチェック用。
//   ?days=N (既定60): 登録日が直近N日以内の「新商品」だけに絞る (0=全件、日中に増えた分を検知)。
//   PMLは毎朝同期なので、NE登録された新商品は翌朝ここに自動で載る (このAPIは都度ライブ計算)。
router.get('/api/attrs/unlinked', (req, res) => {
  try {
    const daysParam = req.query.days == null ? 60 : parseInt(req.query.days, 10);
    const days = Number.isFinite(daysParam) ? daysParam : 60;
    // JST「今日0時」を基準に N 日前0時から (時刻依存の1日ズレを防ぐ、登録日は日単位)
    const jstMidnight = Date.parse(new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10) + 'T00:00:00+09:00');
    const since = days > 0 ? jstMidnight - (days - 1) * 86400000 : null;
    const { rows } = loadPmlMerged();
    const { attrs } = loadMasters();
    const all = [];
    let recentCount = 0;
    for (const r of rows) {
      if (String(r['取扱区分'] || '') !== '取扱中') continue;
      const key = normProductCode(r['商品コード']);
      if (attrs.has(key)) continue;
      const reg = r['登録日'] ? String(r['登録日']).slice(0, 10) : '';
      const regTs = reg ? Date.parse(reg + 'T00:00:00+09:00') : NaN;
      const isRecent = since != null && !Number.isNaN(regTs) && regTs >= since;
      if (isRecent) recentCount++;
      all.push({ code: r['商品コード'], name: r['商品名'] || '', supplier: normSupplierCode(r['仕入先']), reg, isRecent });
    }
    const filtered = since != null ? all.filter(x => x.isRecent) : all;
    filtered.sort((a, b) => (b.reg || '').localeCompare(a.reg || ''));
    res.json({ ok: true, totalUnlinked: all.length, recentCount, days, count: filtered.length, rows: filtered.slice(0, 500) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ═══════════════════════════ 画面 ═══════════════════════════

const CSS = `
  :root {
    --bg: #eef1f6; --card: #ffffff; --ink: #1f2733; --sub: #64748b; --line: #e6eaf0;
    --accent: #2563eb; --accent-d: #1d4ed8; --accent-soft: #eef4ff;
    --ok: #16a34a; --ok-soft: #e9f7ee; --warnc: #b45309; --warn-soft: #fff7e6; --danger: #dc2626; --danger-soft: #fdecec;
    --shadow: 0 1px 2px rgba(16,24,40,.06), 0 1px 3px rgba(16,24,40,.10);
    --shadow-h: 0 6px 18px rgba(37,99,235,.14);
  }
  * { box-sizing: border-box; }
  body { font-family: 'Hiragino Sans','Yu Gothic UI',Meiryo,system-ui,sans-serif; margin: 0; background: var(--bg); color: var(--ink); -webkit-font-smoothing: antialiased; }
  header.app { background: linear-gradient(90deg,#111c33,#1e3a63); color: #fff; padding: 0 20px; display: flex; align-items: center; gap: 4px; position: sticky; top: 0; z-index: 40; box-shadow: 0 1px 8px rgba(0,0,0,.18); }
  header.app h1 { font-size: 16px; margin: 0 18px 0 0; font-weight: 700; letter-spacing: .01em; display: flex; align-items: center; height: 52px; }
  header.app nav.tabs { display: flex; gap: 2px; height: 52px; }
  header.app nav.tabs a { color: #c8d6ec; text-decoration: none; font-size: 13.5px; padding: 0 14px; display: flex; align-items: center; border-bottom: 3px solid transparent; }
  header.app nav.tabs a:hover { color: #fff; background: rgba(255,255,255,.06); }
  header.app nav.tabs a.on { color: #fff; border-bottom-color: #5b9bff; font-weight: 600; }
  header.app .sp { margin-left: auto; }
  header.app a.back { color: #9fb3d4; text-decoration: none; font-size: 13px; padding: 0 8px; }
  header.app a.back:hover { color: #fff; }
  .wrap { padding: 20px 22px 60px; max-width: 1560px; margin: 0 auto; }
  h2.page { font-size: 19px; margin: 4px 0 16px; font-weight: 700; }
  .warn { background: var(--warn-soft); border: 1px solid #f0d089; color: var(--warnc); padding: 10px 14px; border-radius: 10px; margin-bottom: 14px; font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(268px, 1fr)); gap: 14px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 16px 16px 14px; display: block; text-decoration: none; color: inherit; box-shadow: var(--shadow); transition: transform .08s, box-shadow .12s, border-color .12s; }
  a.card:hover { border-color: #b9d0ff; box-shadow: var(--shadow-h); transform: translateY(-2px); }
  .card .nm { font-weight: 700; font-size: 15px; margin-bottom: 6px; line-height: 1.4; }
  .card .memo { font-size: 11.5px; color: var(--warnc); background: var(--warn-soft); border-radius: 6px; padding: 2px 7px; display: inline-block; margin-bottom: 6px; }
  .card .stats { display: flex; gap: 18px; margin-top: 10px; font-size: 12px; color: var(--sub); }
  .card .stats .n { font-size: 21px; color: var(--ink); font-weight: 700; display: block; line-height: 1.1; font-variant-numeric: tabular-nums; }
  .card .stats .n.acc { color: var(--accent); }
  .badge { display: inline-block; font-size: 11px; border-radius: 999px; padding: 2px 9px; vertical-align: middle; font-weight: 600; }
  .b-draft { background: var(--accent-soft); color: var(--accent-d); }
  .b-issued { background: var(--ok-soft); color: var(--ok); }
  .b-warn { background: var(--danger-soft); color: var(--danger); }
  table.t { border-collapse: separate; border-spacing: 0; width: 100%; background: var(--card); font-size: 12.5px; }
  table.t th, table.t td { border-bottom: 1px solid var(--line); padding: 8px 10px; text-align: left; }
  table.t th { background: #f7f9fc; color: var(--sub); font-weight: 600; position: sticky; top: 52px; white-space: nowrap; font-size: 11.5px; letter-spacing: .02em; z-index: 1; }
  table.t tbody tr:hover td { background: #f9fbff; }
  table.t td.r, table.t th.r { text-align: right; font-variant-numeric: tabular-nums; }
  .sec { background: var(--card); border: 1px solid var(--line); border-radius: 14px; margin-bottom: 16px; overflow: hidden; box-shadow: var(--shadow); }
  .sec > h2 { font-size: 14px; margin: 0; padding: 12px 16px; background: #f7f9fc; border-bottom: 1px solid var(--line); user-select: none; font-weight: 700; }
  .sec > h2[data-sec] { cursor: pointer; }
  .sec .bd { padding: 14px 16px; overflow-x: auto; }
  button { font: inherit; font-size: 13px; border-radius: 9px; border: 1px solid #cbd5e1; background: #fff; padding: 8px 15px; cursor: pointer; color: var(--ink); transition: background .1s, border-color .1s, box-shadow .1s; font-weight: 500; }
  button:hover { background: #f1f5f9; border-color: #94a3b8; }
  button.pri { background: var(--accent); border-color: var(--accent); color: #fff; box-shadow: 0 1px 2px rgba(37,99,235,.3); }
  button.pri:hover { background: var(--accent-d); border-color: var(--accent-d); }
  button.ok { background: var(--ok); border-color: var(--ok); color: #fff; }
  button.ok:hover { filter: brightness(.94); }
  button.ghost { border: none; background: none; color: var(--accent); padding: 3px 6px; font-weight: 500; }
  button.ghost:hover { background: var(--accent-soft); }
  button.sm { padding: 4px 10px; font-size: 12px; }
  button:disabled { opacity: .5; cursor: default; }
  input[type=text], input[type=number], select, textarea { font: inherit; font-size: 13px; border: 1px solid #cbd5e1; border-radius: 8px; padding: 7px 10px; background: #fff; color: var(--ink); }
  input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-soft); }
  input[type=number] { width: 88px; text-align: right; }
  input[type=file] { font-size: 12px; }
  .gauge { margin: 8px 0; font-size: 12.5px; }
  .gauge .bar { height: 9px; border-radius: 999px; background: #e8edf3; overflow: hidden; margin-top: 4px; }
  .gauge .bar > div { height: 100%; background: linear-gradient(90deg,#f0a94b,#e0872b); border-radius: 999px; transition: width .3s; }
  .gauge.met .bar > div { background: linear-gradient(90deg,#4ade80,#16a34a); }
  .muted { color: var(--sub); font-size: 12px; }
  .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; flex-wrap: wrap; }
  #toast { position: fixed; bottom: 22px; left: 50%; transform: translateX(-50%); background: #111c33; color: #fff; padding: 11px 24px; border-radius: 10px; display: none; z-index: 60; font-size: 13.5px; box-shadow: 0 8px 24px rgba(0,0,0,.25); }
  .tabbar { display: flex; gap: 6px; margin-bottom: 14px; flex-wrap: wrap; }
  .tabbar button { border-radius: 999px; background: #fff; }
  .tabbar button.on { background: var(--accent); color: #fff; border-color: var(--accent); box-shadow: 0 1px 3px rgba(37,99,235,.3); }
  .grid2 { display: grid; grid-template-columns: 1fr 360px; gap: 16px; align-items: start; }
  @media (max-width: 1000px) { .grid2 { grid-template-columns: 1fr; } }
  .cart { position: sticky; top: 68px; }
  .cart .tot { font-size: 13px; margin: 8px 0; }
  .cart .tot b { font-size: 19px; }
  pre.copy { background: #0f172a; color: #e2e8f0; border-radius: 10px; padding: 12px 14px; font-size: 12.5px; max-height: 320px; overflow: auto; white-space: pre; line-height: 1.5; }
  .import-zone { background: linear-gradient(135deg,#f0f6ff,#eef4ff); border: 1.5px dashed #9fbdf5; border-radius: 14px; padding: 18px 20px; margin-bottom: 18px; }
  .import-zone h3 { margin: 0 0 6px; font-size: 15px; }
  .import-zone .hint { font-size: 12.5px; color: var(--sub); margin-bottom: 12px; line-height: 1.6; }
  .import-zone .row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
  .pill-row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
  .pill { background: #fff; border: 1px solid var(--line); border-radius: 999px; padding: 3px 11px; font-size: 12px; color: var(--sub); }
`;

function pageShell(title, active, body, script) {
  const tab = (href, label, key) => `<a href="${href}"${active === key ? ' class="on"' : ''}>${label}</a>`;
  return `<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${he(title)}</title><style>${CSS}</style></head>
<body>
<header class="app">
  <h1>📦 発注補助</h1>
  <nav class="tabs">
    ${tab('/apps/purchase-orders', 'ダッシュボード', 'dash')}
    ${tab('/apps/purchase-orders/orders', '発注履歴', 'orders')}
    ${tab('/apps/purchase-orders/admin', 'マスタ管理', 'admin')}
  </nav>
  <a href="/dashboard" class="back sp">← ポータルに戻る</a>
</header>
<div class="wrap">${body}</div>
<div id="toast"></div>
<script>
function toast(msg) {
  var t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  clearTimeout(t._h); t._h = setTimeout(function(){ t.style.display = 'none'; }, 2800);
}
function yen(n) { return '¥' + Math.round(n).toLocaleString('ja-JP'); }
function esc(s) { s = (s == null ? '' : String(s)); return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
${script || ''}
</script>
</body></html>`;
}

/** データ鮮度の表示テキスト (NE=朝同期 or 手動CSV / FBA=朝同期 or live) */
function freshnessText(pub, overlay) {
  const ne = overlay && overlay.applied
    ? (overlay.mergedCount === 0
      ? `NE: ⚠️手動CSV ${fmtJst(overlay.uploaded_at)} — 1件も一致していません (${overlay.row_count}件中0件)。CSVを確認してください`
      : `NE: 🟢手動CSV ${fmtJst(overlay.uploaded_at)} 取込済 (反映 ${overlay.mergedCount}/${overlay.row_count}件)`)
    : `NE: ${pub && pub.src_ne_products_synced_at ? fmtJst(pub.src_ne_products_synced_at) + ' (朝同期)' : '—'}`;
  const fba = pub && pub.fba_source_kind === 'live'
    ? `FBA: 🟢${fmtJst(pub.fba_fetched_at)} 取得(最新)`
    : `FBA: ${pub && pub.src_fba_business_date ? he(String(pub.src_fba_business_date)) + ' (朝同期)' : '—'}`;
  return ne + ' ／ ' + fba;
}

// ─── 画面1: ダッシュボード ───
router.get('/', (req, res) => {
  let data;
  try {
    const { pub, overlay, bySupplier, products } = computeAll();
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
    data = { pub, overlay, cards, others, searchIndex };
  } catch (e) { return res.status(500).send('error: ' + he(e.message)); }

  const { pub, overlay, cards, others, searchIndex } = data;
  const stale = pub && pub.as_of_date ? (Date.now() - Date.parse(pub.as_of_date + 'T00:00:00+09:00')) > 3 * 86400000 : true;
  const freshNote = pub ? freshnessText(pub, overlay) : 'PML未同期';
  const cardHtml = c => `
    <a class="card" href="/apps/purchase-orders/supplier/${encodeURIComponent(c.code)}">
      <div class="nm">${he(c.name)} ${c.hasDraft ? '<span class="badge b-draft">下書きあり</span>' : ''}</div>
      ${c.memo ? `<div class="memo">📌 ${he(c.memo)}</div>` : ''}
      <div class="stats">
        <span><span class="n${c.targetCount ? ' acc' : ''}">${c.targetCount}</span>要発注 SKU</span>
        <span><span class="n">¥${c.estAmount.toLocaleString('ja-JP')}</span>推奨額(原価)</span>
      </div>
    </a>`;
  const body = `
    ${stale ? '<div class="warn">⚠️ PMLデータが古い可能性があります (as_of=' + he(pub ? pub.as_of_date : 'なし') + ')。daily-sync の状態を確認してください。</div>' : ''}
    <div class="toolbar">
      <input type="text" id="q" placeholder="商品コード / 商品名で検索 → 仕入先を探す" style="width:340px">
      <span class="muted">${freshNote}</span>
    </div>
    <div class="toolbar">
      <button id="btnFba">🔄 FBA在庫を今すぐ更新</button>
      <form id="neForm" style="display:flex;gap:6px;align-items:center">
        <input type="file" name="file" accept=".csv" required>
        <button type="submit">📥 NE最新CSVを取込</button>
      </form>
      ${overlay ? '<button id="btnNeClear" title="手動CSVの上書きをやめて朝同期の値に戻す">✕ CSV取込を解除</button>' : ''}
      <span id="opStatus" class="muted"></span>
    </div>
    <div id="searchResult"></div>
    <h2 class="page" style="font-size:16px">発注が必要な仕入先 <span class="muted">(${cards.length})</span></h2>
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
});

// ── FBA在庫 オンデマンド更新 (miniPCジョブ、数分かかる) ──
var opStatus = document.getElementById('opStatus');
function setStatus(msg) { opStatus.textContent = msg; }
var btnFba = document.getElementById('btnFba');
btnFba.addEventListener('click', function() {
  btnFba.disabled = true;
  setStatus('FBA更新ジョブを起動中...');
  fetch('/apps/purchase-orders/api/refresh-fba', { method: 'POST' })
    .then(function(r){ return r.json(); })
    .then(function(j) {
      if (!j.jobId) {
        setStatus(j.message || j.error || '起動できませんでした (既に実行中の可能性)。数分後に再読み込みしてください');
        btnFba.disabled = false;
        return;
      }
      setStatus('実行中... (Amazonのレポート生成に数分かかります)');
      pollFba(j.jobId, Date.now());
    })
    .catch(function(e){ setStatus('通信エラー: ' + e.message); btnFba.disabled = false; });
});
function pollFba(jobId, startTs) {
  if (Date.now() - startTs > 5 * 60 * 1000) {
    setStatus('まだ処理中です。数分後にページを再読み込みすると最新FBA在庫で表示されます');
    btnFba.disabled = false;
    return;
  }
  setTimeout(function() {
    fetch('/apps/purchase-orders/api/refresh-fba/jobs/' + encodeURIComponent(jobId))
      .then(function(r){ return r.json(); })
      .then(function(job) {
        if (job.error && !job.status) { setStatus('状態取得エラー: ' + job.error); btnFba.disabled = false; return; }
        if (job.status === 'completed') { setStatus('完了。最新データで再読み込みします...'); location.reload(); }
        else if (job.status === 'failed') { setStatus('失敗: ' + (job.error || '理由不明')); btnFba.disabled = false; }
        else {
          if (job.progress && job.progress.message) setStatus('実行中: ' + job.progress.message);
          pollFba(jobId, startTs);
        }
      })
      .catch(function(){ pollFba(jobId, startTs); });
  }, 8000);
}

// ── NE商品マスタCSV オーバーレイ取込 ──
document.getElementById('neForm').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  setStatus('NE CSVを取込中...');
  fetch('/apps/purchase-orders/api/ne-overlay/csv', { method: 'POST', body: fd })
    .then(function(r){ return r.json(); })
    .then(function(j) {
      if (!j.ok) { setStatus(''); alert('取込エラー: ' + j.error); return; }
      setStatus('取込完了 (' + j.rowCount + '件)。再読み込みします...');
      location.reload();
    })
    .catch(function(e){ setStatus('通信エラー: ' + e.message); });
});
var btnNeClear = document.getElementById('btnNeClear');
if (btnNeClear) btnNeClear.addEventListener('click', function() {
  if (!confirm('手動CSVの上書きを解除して、朝同期の値に戻しますか?')) return;
  fetch('/apps/purchase-orders/api/ne-overlay', { method: 'DELETE' })
    .then(function(r){ return r.json(); })
    .then(function(j){ if (j.ok) location.reload(); else alert(j.error); });
});`;
  res.send(pageShell('発注補助 — ダッシュボード', 'dash', body, script));
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
      <h2 class="page" style="margin:0">${he(data.supplier.name)} <span class="muted">(コード ${he(data.supplier.code)})</span></h2>
      ${data.supplier.memo ? `<span class="badge b-warn">📌 ${he(data.supplier.memo)}</span>` : ''}
      <span class="muted">${data.pub ? freshnessText(data.pub, data.overlay) : 'PML未同期'}</span>
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
[].concat(D.targets, D.candidates, D.horikoshi, D.draftExtras || []).forEach(function(p){ byCode[p.code] = p; });

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
      var extraBadge = p.extra ? ' <span class="badge b-warn" title="現在の要発注/候補リストに出てこない商品 (下書き保存後にPMLが更新された等)。確定エラーになる場合は ✕ で外してください">リスト外</span>' : '';
      h += '<tr><td>' + esc(String(p.name || '').slice(0, 60)) + extraBadge + '</td><td class="r">' + i.qty.toLocaleString('ja-JP') + '</td><td class="r">' + (p.cost ? yen(i.qty * p.cost) : '—') + '</td>' +
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
    // querySelector の attribute selector は商品コード中の記号でこわれるので全走査で消す
    document.querySelectorAll('input[data-code]').forEach(function(inp) {
      if (inp.getAttribute('data-code') === del) inp.value = '';
    });
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
    body: JSON.stringify({ items: items, note: document.getElementById('orderNote').value }),
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
    <h2 class="page">発注履歴</h2>
    <div class="sec"><div class="bd" id="list">読み込み中…</div></div>
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
  res.send(pageShell('発注補助 — 発注履歴', 'orders', body, script));
});

// ─── 画面4: マスタ管理 ───
router.get('/admin', (req, res) => {
  const body = `
    <h2 class="page">マスタ管理</h2>
    <div class="import-zone">
      <h3>📥 スプレッドシートから取り込む</h3>
      <div class="hint">
        「発注条件マスタ」スプレッドシートから <b>ダウンロードしたCSVをそのまま</b>ここに入れてください（文字コード・列名は自動で判別します）。<br>
        対応: <b>商品マスタ</b>（→ 仕入先＋商品紐付け）／ <b>発注条件マスタ</b> ／ <b>原料グループマスタ</b>。まとめて複数選択もできます。
      </div>
      <form id="importForm" class="row">
        <input type="file" name="files" accept=".csv" multiple required>
        <button type="submit" class="pri">取り込む</button>
        <span id="importStatus" class="muted"></span>
      </form>
      <div id="importResult" class="pill-row"></div>
    </div>

    <div class="tabbar">
      <button data-tab="suppliers" class="on">仕入先</button>
      <button data-tab="conditions">発注条件グループ</button>
      <button data-tab="materials">原料グループ</button>
      <button data-tab="attrs">商品紐付け</button>
      <button data-tab="unlinked">🆕 未紐付けの新商品</button>
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

// ── 一括取込 (自動判別) ──
document.getElementById('importForm').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  var st = document.getElementById('importStatus');
  st.textContent = '取込中...';
  document.getElementById('importResult').innerHTML = '';
  fetch('/apps/purchase-orders/api/import', { method: 'POST', body: fd })
    .then(function(r){ return r.json(); })
    .then(function(j) {
      st.textContent = '';
      if (!j.ok) {
        var d = (j.errors && j.errors.length) ? '\\n\\n' + j.errors.join('\\n') : '';
        alert('取込エラー: ' + j.error + d);
        return;
      }
      var pr = document.getElementById('importResult');
      var html = (j.summary || []).map(function(s){ return '<span class="pill">✅ ' + esc(s) + '</span>'; }).join('');
      var w = j.warnings || [];
      if (w.length) {
        html += '<div class="warn" style="margin-top:10px">⚠️ 注意 ' + w.length + '件<ul style="margin:6px 0 0;padding-left:18px">' +
          w.slice(0, 30).map(function(x){ return '<li>' + esc(x) + '</li>'; }).join('') +
          (w.length > 30 ? '<li>…ほか ' + (w.length - 30) + '件</li>' : '') + '</ul></div>';
      }
      pr.innerHTML = html;
      toast(w.length ? '取り込みました (注意' + w.length + '件)' : '取り込みました');
      load();
    })
    .catch(function(e){ st.textContent = ''; alert('通信エラー: ' + e.message); });
});

function render(rows) {
  var def = DEFS[TAB];
  var h = '<div class="toolbar"><span class="muted">' + rows.length + ' 件 — セルを直接編集して「保存」／最上行から追加</span>' +
    '<input type="text" id="filter" placeholder="絞り込み" style="margin-left:auto"></div>';
  h += '<table class="t" id="mtable"><thead><tr>' + def.cols.map(function(c){ return '<th' + (c.num ? ' class="r"' : '') + '>' + c.l + '</th>'; }).join('') + '<th></th></tr></thead><tbody>';
  h += '<tr>' + def.cols.map(function(c){ return '<td><input type="text" style="width:99%" id="new_' + c.k + '"></td>'; }).join('') + '<td><button class="pri sm" id="btnAdd">追加</button></td></tr>';
  rows.forEach(function(r) {
    h += '<tr data-row="1">' + def.cols.map(function(c) {
      return '<td' + (c.pk ? '' : ' contenteditable data-k="' + c.k + '"') + (c.num ? ' class="r"' : '') + '>' + esc(r[c.k] == null ? '' : r[c.k]) + '</td>';
    }).join('') + '<td style="white-space:nowrap"><button class="ghost" data-save="' + esc(r[def.cols[0].k]) + '">保存</button><button class="ghost" data-rm="' + esc(r[def.cols[0].k]) + '">削除</button></td></tr>';
  });
  h += '</tbody></table>';
  document.getElementById('tabBody').innerHTML = h;
  document.getElementById('filter').addEventListener('input', function(ev) {
    var q = ev.target.value.trim().toLowerCase();
    document.querySelectorAll('#mtable tr[data-row]').forEach(function(tr) {
      tr.style.display = !q || tr.textContent.toLowerCase().indexOf(q) >= 0 ? '' : 'none';
    });
  });
}
function renderUnlinked(j) {
  var h = '<div class="toolbar">' +
    '<span class="muted">取扱中なのに発注条件/原料グループ等が未登録の商品。' +
    '<b>新商品が入ると翌朝ここに自動で載ります</b>（グループ対象外の商品はそのままでOK）。</span>' +
    '<span style="margin-left:auto">表示: <select id="uDays">' +
      '<option value="30">直近30日の新商品</option>' +
      '<option value="60" selected>直近60日の新商品</option>' +
      '<option value="90">直近90日の新商品</option>' +
      '<option value="0">全部 (' + j.totalUnlinked + '件)</option>' +
    '</select></span></div>';
  h += '<div class="muted" style="margin-bottom:8px">該当 ' + j.count + ' 件' + (j.count > 500 ? ' (先頭500件表示)' : '') + '</div>';
  h += '<table class="t"><thead><tr><th>登録日</th><th>商品コード</th><th>商品名</th><th>仕入先</th></tr></thead><tbody>';
  if (!j.rows.length) h += '<tr><td colspan="4" class="muted">該当なし 🎉</td></tr>';
  j.rows.forEach(function(r){ h += '<tr><td>' + esc(r.reg || '—') + '</td><td>' + esc(r.code) + '</td><td>' + esc(r.name) + '</td><td>' + esc(r.supplier) + '</td></tr>'; });
  document.getElementById('tabBody').innerHTML = h + '</tbody></table>';
  document.getElementById('uDays').addEventListener('change', function(ev){ loadUnlinked(ev.target.value); });
}
function loadUnlinked(days) {
  document.getElementById('tabBody').textContent = '読み込み中…';
  fetch('/apps/purchase-orders/api/attrs/unlinked?days=' + encodeURIComponent(days == null ? 60 : days))
    .then(function(r){ return r.json(); })
    .then(function(j){ if (j.ok) renderUnlinked(j); else document.getElementById('tabBody').textContent = j.error; });
}
function load() {
  if (TAB === 'unlinked') { loadUnlinked(60); return; }
  document.getElementById('tabBody').textContent = '読み込み中…';
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
    var def = DEFS[TAB], b = {};
    def.cols.forEach(function(c){ b[c.k] = document.getElementById('new_' + c.k).value; });
    post(b);
    return;
  }
  var saveKey = t.getAttribute && t.getAttribute('data-save');
  if (saveKey != null) {
    var def2 = DEFS[TAB], tr = t.closest('tr'), b2 = {};
    b2[def2.cols[0].k] = saveKey;
    tr.querySelectorAll('[data-k]').forEach(function(td){ b2[td.getAttribute('data-k')] = td.textContent; });
    post(b2);
    return;
  }
  var rmKey = t.getAttribute && t.getAttribute('data-rm');
  if (rmKey != null) {
    if (!confirm('削除しますか? ' + rmKey)) return;
    fetch('/apps/purchase-orders/api/masters/' + TAB + '/' + encodeURIComponent(rmKey), { method: 'DELETE' })
      .then(function(r){ return r.json(); }).then(function(j){ if (j.ok) { toast('削除しました'); load(); } else toast(j.error); });
  }
});
function post(b) {
  fetch('/apps/purchase-orders/api/masters/' + TAB, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (j.ok) { toast('保存しました'); load(); } else toast('エラー: ' + j.error);
  });
}
load();`;
  res.send(pageShell('発注補助 — マスタ管理', 'admin', body, script));
});

export default router;
