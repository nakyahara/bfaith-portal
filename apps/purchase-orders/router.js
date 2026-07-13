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
import { loadDimMall } from '../../lib/dim-mall.js';
import fs from 'fs';
import multer from 'multer';
import iconv from 'iconv-lite';
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { computeAll, loadPml, loadPmlMerged, loadMasters, evaluateCondition } from './logic.js';
import {
  ensureTrackingStarted, nextPoNumber, isYmd, checkLedgerIntegrity, withCommand,
  appendPoItemEvent, reverseEvent, manualCloseOrder, setItemPlan, listBackorders, listItemEvents, balanceOf,
} from './ledger.js';
import { importInboundCsv, listInbound, candidatesFor, setInboundIgnore, parseCsv as parseCsvStrict } from './inbound.js';
import { importNeBackorderCsv } from './migration.js';
import {
  buildOrderEmail, createEmailJob, processEmailJob, reconcileEmailJobs, listEmailJobs, emailSettings, parseAddresses,
  markUnsent, cancelEmailJob, startEmailDispatcher,
} from './email.js';
import { getSetting, setSetting, audit, markCycleFbaJobDone } from './ledger.js';

startEmailDispatcher(); // 予約送信 (毎分、時刻が来たqueuedジョブを送信。unrefでプロセス終了は妨げない)

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
    // 明細単位の希望納期 (任意。空=指定なし)
    const rd = it.requestedDate == null ? '' : trimS(it.requestedDate);
    if (rd && !isYmd(rd)) throw new Error(`希望納期が日付 (YYYY-MM-DD) ではありません (${code}: ${it.requestedDate})`);
    const r = byKey.get(key);
    if (!r) throw new Error(`PMLに存在しない商品コード: ${code}`);
    if (normSupplierCode(r['仕入先']) !== supplierCode) {
      throw new Error(`仕入先が一致しない商品コード: ${code} (この商品の仕入先: ${r['仕入先'] || '未設定'})`);
    }
    items.push({ code: r['商品コード'], key, name: r['商品名'] || '', qty, cost: r['原価'] == null ? null : Number(r['原価']), requestedDate: rd || null });
  }
  return { items, pmlAsOf: pub.as_of_date || null };
}

/** 仕入先名はサーバ側で解決 (クライアント値は信用しない) */
function resolveSupplierName(supplierCode) {
  const db = getDB();
  const s = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplierCode);
  return (s && s.name) || `仕入先 ${supplierCode}`;
}

function insertItems(db, orderId, items, requestedDate = null) {
  // condition_id は発注時点のスナップショット。後で紐付けを変えても過去発注の月次上限カウントが動かない (Codex P8-2)
  // requested_date もINSERT時に確定する (発行済み明細はDBトリガで不変のため後からUPDATEできない)。
  // 明細単位の希望納期を優先し、無い明細のみヘッダ既定値 (旧API互換) を使う
  const condOf = db.prepare('SELECT condition_id FROM po_product_attrs WHERE product_key=?');
  const ins = db.prepare('INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty, unit_cost, condition_id, requested_date) VALUES (?,?,?,?,?,?,?,?)');
  for (const it of items) {
    const a = condOf.get(it.key);
    ins.run(orderId, it.code, it.key, it.name, it.qty, it.cost, (a && a.condition_id) || null, it.requestedDate || requestedDate);
  }
}

/** ヘッダに記録する希望納期: 全明細が同じ日付ならその値、混在/一部指定/全て未指定なら NULL (表示用) */
function headerReqDate(items, defaultDate) {
  const eff = items.map(it => it.requestedDate || defaultDate || null);
  const first = eff[0] || null;
  return first && eff.every(d => d === first) ? first : null;
}

function upsertDraft(supplierCode, supplierName, rawItems, note, requestedDate) {
  const db = getDB();
  const { items, pmlAsOf } = enrichItems(rawItems, supplierCode);
  if (requestedDate != null && requestedDate !== '' && !isYmd(String(requestedDate))) {
    throw new Error(`希望納期が日付 (YYYY-MM-DD) ではありません: ${requestedDate}`);
  }
  const reqDate = requestedDate ? String(requestedDate) : null;
  const hdrDate = headerReqDate(items, reqDate);
  const now = nowIso();
  return db.transaction(() => {
    let order = db.prepare("SELECT id FROM po_orders WHERE supplier_code=? AND status='draft'").get(supplierCode);
    if (!order) {
      const info = db.prepare(
        'INSERT INTO po_orders (supplier_code, supplier_name, status, note, pml_as_of_date, created_at, updated_at, requested_date) VALUES (?,?,?,?,?,?,?,?)'
      ).run(supplierCode, supplierName, 'draft', note || null, pmlAsOf, now, now, hdrDate);
      order = { id: Number(info.lastInsertRowid) };
    } else {
      db.prepare('UPDATE po_orders SET note=?, pml_as_of_date=?, updated_at=?, supplier_name=?, requested_date=? WHERE id=?')
        .run(note || null, pmlAsOf, now, supplierName, hdrDate, order.id);
    }
    db.prepare('DELETE FROM po_order_items WHERE order_id=?').run(order.id);
    insertItems(db, order.id, items, reqDate);
    return order.id;
  })();
}

/**
 * 発注確定 (Codex R2 High): draft を mutate せず issued order を新規 insert し、
 * 既存 draft の削除まで1トランザクションで行う。並行する draft 更新と混ざらない。
 *
 * P13a: issue と同時に PO番号採番 + tracking_mode='tracked' + 希望納期の明細スナップショットを行う
 * (移行境界 tracking_started_at 以後の発注=残数管理対象。db.js の issue gate トリガが不完全発行を拒否する)。
 */
function issueOrder(supplierCode, supplierName, rawItems, note, requestedDate) {
  const db = getDB();
  const { items, pmlAsOf } = enrichItems(rawItems, supplierCode);
  if (requestedDate != null && requestedDate !== '' && !isYmd(String(requestedDate))) {
    throw new Error(`希望納期が日付 (YYYY-MM-DD) ではありません: ${requestedDate}`);
  }
  const reqDate = requestedDate ? String(requestedDate) : null;
  const hdrDate = headerReqDate(items, reqDate);
  const tx = db.transaction(() => {
    const now = nowIso();
    // 移行境界の確定は発行と同一コミット (最初の発行が失敗すれば境界もロールバック)。発行時刻を共有する
    ensureTrackingStarted(now);
    const poNumber = nextPoNumber(db);
    // 発行済みPOへの明細INSERTはDBトリガが無条件拒否するため、draftでヘッダ+明細を作ってから issued へ遷移する
    // (既存draftはtxn冒頭で削除=仕入先ごとdraft1件のUNIQUEとも整合。失敗すればロールバックで復元)
    db.prepare("DELETE FROM po_orders WHERE supplier_code=? AND status='draft'").run(supplierCode);
    const info = db.prepare(
      `INSERT INTO po_orders (supplier_code, supplier_name, status, note, pml_as_of_date, created_at, updated_at)
       VALUES (?,?,'draft',?,?,?,?)`
    ).run(supplierCode, supplierName, note || null, pmlAsOf, now, now);
    const id = Number(info.lastInsertRowid);
    // 希望納期は明細単位でINSERT時にスナップショット (issued後は不変)。ヘッダは全明細同一のときのみ表示用に保持
    insertItems(db, id, items, reqDate);
    db.prepare(
      `UPDATE po_orders SET status='issued', issued_at=?, po_number=?, tracking_mode='tracked', requested_date=?, updated_at=? WHERE id=?`
    ).run(now, poNumber, hdrDate, now, id);
    return { id, poNumber };
  });
  return tx.immediate();
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
    selectableLow: p.selectableLow || null,
  };
}

function supplierWorkspaceData(code) {
  const { pub, overlay, products, bySupplier, masters } = computeAll();
  const g = bySupplier.get(code);
  const sm = masters.suppliers.get(code);
  if (!g && !sm) return null;
  const db = getDB();
  const draftRow = db.prepare("SELECT id, note, requested_date FROM po_orders WHERE supplier_code=? AND status='draft'").get(code);
  const draft = draftRow ? {
    id: draftRow.id, note: draftRow.note || '', requestedDate: draftRow.requested_date || '',
    // 旧形式draft (ヘッダに希望納期・明細はNULL) の互換: 明細値が無ければヘッダ値を実効値として返す (Codex 明細納期R1 High)
    items: db.prepare('SELECT product_code AS code, product_key AS key, product_name AS name, qty, unit_cost AS cost, requested_date AS requestedDate FROM po_order_items WHERE order_id=? ORDER BY id')
      .all(draftRow.id).map(it => ({ ...it, requestedDate: it.requestedDate || draftRow.requested_date || null })),
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
  // 全attrs (画面外の商品含む) で1件でも紐付けがある条件は「商品別条件」。
  // 画面内リストの memberCodes だけで判定すると、紐付け商品が今リストに出ていないだけの商品別条件を
  // 仕入先全体条件と誤判定してカート全体で達成扱いにしてしまう (Codex P5 R2 High)
  const globallyBoundCondIds = new Set();
  for (const a of masters.attrs.values()) if (a.condition_id) globallyBoundCondIds.add(a.condition_id);
  const conditions = masters.conditions
    .filter(c => condIds.has(c.condition_id) || normSupplierCode(c.supplier_code) === code)
    .map(c => ({
      conditionId: c.condition_id, displayName: c.display_name, makerName: c.maker_name || '',
      conditionType: c.condition_type, conditionValue: c.condition_value, unit: c.unit || '',
      memberCodes: memberOf(c.condition_id, 'conditionId'),
      // 商品紐付けが全attrsで0件の仕入先直付き条件 (例: 仕入先全体で¥50,000以上) だけカート全体を対象に評価する
      supplierWide: !globallyBoundCondIds.has(c.condition_id) && normSupplierCode(c.supplier_code) === code,
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
  // 今月 (JST) にこの仕入先へ発注確定済みの数量 (上限=出荷制限の月次累計判定用。毎月1日に自動リセット)
  // 条件グループは発注時スナップショット (i.condition_id) 優先。列追加前の旧明細のみ現在の紐付けで補完
  // 移行PO (NE発注残初期取込) は除外: 過去にNEで発注済みの分であり、今月の発注枠を消費していない
  const jstNow = new Date(Date.now() + 9 * 3600000);
  const monthStartIso = new Date(Date.parse(jstNow.toISOString().slice(0, 7) + '-01T00:00:00+09:00')).toISOString();
  const issuedMonth = {};
  const issuedByCond = {};
  let issuedTotal = 0;
  for (const r of db.prepare(`
    SELECT i.product_key k, COALESCE(i.condition_id, a.condition_id) cid, SUM(i.qty) q
    FROM po_order_items i
    JOIN po_orders o ON o.id = i.order_id
    LEFT JOIN po_product_attrs a ON a.product_key = i.product_key
    WHERE o.supplier_code = ? AND o.status = 'issued' AND o.issued_at >= ?
      AND (o.origin IS NULL OR o.origin <> 'migration')
    GROUP BY i.product_key, cid
  `).all(code, monthStartIso)) {
    issuedMonth[r.k] = (issuedMonth[r.k] || 0) + r.q;
    if (r.cid) issuedByCond[r.cid] = (issuedByCond[r.cid] || 0) + r.q;
    issuedTotal += r.q;
  }
  // グループ紐付けUI用の全グループ一覧 (この仕入先関連を先頭に)
  const supFirst = (aSup) => (normSupplierCode(aSup) === code ? 0 : 1);
  const allGroups = {
    conditions: masters.conditions
      .map(c => ({ id: c.condition_id, name: c.display_name, mine: supFirst(c.supplier_code) === 0 }))
      .sort((a, b) => (b.mine - a.mine) || a.id.localeCompare(b.id, 'ja')),
    materials: [...masters.materialGroups.values()]
      .map(m => ({ id: m.group_id, name: m.name }))
      .sort((a, b) => a.id.localeCompare(b.id, 'ja')),
  };
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
    conditions, materialGroups, draft, draftExtras, issuedMonth, issuedByCond, issuedTotal, allGroups,
    monthLabel: jstNow.toISOString().slice(0, 7),
  };
}

// ═══════════════════════════ API ═══════════════════════════

// ─── 発注サイクル (「✅発注確定済み」「×非表示」の基準) ───
// データ更新 (NE手動CSV取込/解除・FBA在庫更新) で po_cycle_reset_at を進め、それ以降に
// 発注確定した仕入先へダッシュボードで「✅発注確定済み」を表示する。
// ⚠️ 営業日 (as_of) での自動リセットは廃止 (中原さん要望 2026-07-13): 発注確定→翌日以降にメール送信、
// という日をまたぐ運用のため、リセットは「データ更新ボタンを押した時だけ」。朝のNE自動同期では消えない
function cycleStartIso(pub) {
  const marker = getSetting('po_cycle_reset_at') || '';
  if (marker) return marker;
  // markerが一度も無い場合のみ営業日0時にフォールバック (初回運用)
  if (pub && pub.as_of_date) {
    const ms = Date.parse(String(pub.as_of_date) + 'T00:00:00+09:00');
    if (Number.isFinite(ms)) return new Date(ms).toISOString();
  }
  return '';
}

// ─── ダッシュボード仕入先カードの×非表示 (サーバ保存 = 会社PC/自宅PC間で共有) ───
// 保存値: {"cycle": <保存時の po_cycle_reset_at>, "codes": [...]}
// 現在のサイクルIDと一致する場合のみ有効 = データ更新でサイクルが進むと自動失効 (明示クリア処理を散らさない導出方式)
const HIDDEN_SUPPLIERS_MAX = 500;
function readHiddenSuppliers() {
  try {
    const raw = getSetting('dashboard_hidden_suppliers');
    if (!raw) return [];
    const v = JSON.parse(raw);
    const curCycle = getSetting('po_cycle_reset_at') || '';
    if (!v || v.cycle !== curCycle || !Array.isArray(v.codes)) return [];
    return [...new Set(v.codes.map(c => normSupplierCode(c)).filter(Boolean))];
  } catch { return []; }
}

router.post('/api/dashboard/hidden', (req, res) => {
  try {
    const b = req.body || {};
    const db = getDB();
    let out = null;
    db.transaction(() => {
      const cur = readHiddenSuppliers();
      let codes;
      if (b.clear === true) {
        codes = [];
      } else {
        const code = normSupplierCode(b.code);
        if (!code) { out = { status: 400, body: { ok: false, error: '仕入先コードが必要です' } }; return; }
        if (b.hidden === true) {
          if (!db.prepare('SELECT 1 FROM po_suppliers WHERE supplier_code=?').get(code)) {
            out = { status: 400, body: { ok: false, error: `仕入先が未登録です: ${code}` } }; return;
          }
          codes = cur.includes(code) ? cur : cur.concat([code]);
          if (codes.length > HIDDEN_SUPPLIERS_MAX) { out = { status: 400, body: { ok: false, error: '非表示の仕入先が多すぎます' } }; return; }
        } else if (b.hidden === false) {
          codes = cur.filter(c => c !== code);
        } else {
          out = { status: 400, body: { ok: false, error: 'hidden (true/false) か clear:true が必要です' } }; return;
        }
      }
      const cycle = getSetting('po_cycle_reset_at') || '';
      setSetting('dashboard_hidden_suppliers', JSON.stringify({ cycle, codes }),
        { actor: actorOf(req), reason: 'ダッシュボード仕入先カードの非表示変更' });
      out = { status: 200, body: { ok: true, codes } };
    })();
    res.status(out.status).json(out.body);
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});
/** 今サイクルに発注確定した仕入先 (移行PO除く)。unsent = 発注書メール未送信 (本送信) のPO数 (email運用の仕入先のみ) */
function cycleIssuedSuppliers(cycleStart) {
  const db = getDB();
  return db.prepare(`
    SELECT code, MAX(name) AS name, COUNT(*) AS orders,
           SUM(is_unsent) AS unsent,
           GROUP_CONCAT(po_label) AS pos,
           GROUP_CONCAT(CASE WHEN is_unsent = 1 THEN po_label END) AS unsent_pos
    FROM (
      SELECT o.supplier_code AS code, o.supplier_name AS name, COALESCE(o.po_number, '#' || o.id) AS po_label,
             CASE WHEN COALESCE(o.send_blocked, 0) = 0 AND s.send_method = 'email'
                   AND NOT EXISTS (SELECT 1 FROM po_email_jobs e WHERE e.order_id = o.id AND e.status = 'sent' AND e.is_dry_run = 0)
                  THEN 1 ELSE 0 END AS is_unsent
      FROM po_orders o LEFT JOIN po_suppliers s ON s.supplier_code = o.supplier_code
      WHERE o.status = 'issued' AND (o.origin IS NULL OR o.origin <> 'migration') AND o.issued_at >= ?
    ) GROUP BY code`).all(cycleStart);
}
// at には「データ更新処理の開始時刻」を渡す (コミット後〜設定更新の間に確定した新規POを
// 誤って旧サイクル扱いにしない、Codex サイクルR2 Medium)
function bumpCycleReset(actor, reason, at = nowIso()) {
  setSetting('po_cycle_reset_at', at, { actor, reason });
}

router.get('/api/cycle-issued', (req, res) => {
  try {
    const { pub } = loadPmlMerged();
    const rows = cycleIssuedSuppliers(cycleStartIso(pub));
    res.json({
      ok: true,
      suppliers: rows.map(r => ({
        code: r.code, name: r.name, unsent: r.unsent,
        poNumbers: String(r.pos || '').split(',').filter(Boolean),
        unsentPoNumbers: String(r.unsent_pos || '').split(',').filter(Boolean),
      })),
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/overview', (req, res) => {
  try {
    const { pub, bySupplier } = computeAll();
    const db = getDB();
    const draftCodes = new Set(db.prepare("SELECT supplier_code FROM po_orders WHERE status='draft'").all().map(r => r.supplier_code));
    const cycleIssued = new Map(cycleIssuedSuppliers(cycleStartIso(pub)).map(r => [r.code, r]));
    const cards = [];
    for (const g of bySupplier.values()) {
      const ci = cycleIssued.get(g.code);
      if (g.targets.length === 0 && !draftCodes.has(g.code) && !ci) continue;
      cards.push({
        code: g.code, name: g.name || `仕入先 ${g.code}`, memo: g.memo,
        targetCount: g.targets.length, estAmount: g.estAmount, hasDraft: draftCodes.has(g.code),
        issuedCount: ci ? ci.orders : 0, unsentCount: ci ? ci.unsent : 0,
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
    const { items, note, requestedDate } = req.body || {};
    if (!Array.isArray(items)) return res.status(400).json({ ok: false, error: 'items が必要です' });
    if (items.length === 0) {
      const db = getDB();
      // 実際に削除された場合のみ deleted=true (下書きが無いときのクライアント側入力クリアを防ぐ、Codex R2 Medium)
      const info = db.prepare("DELETE FROM po_orders WHERE supplier_code=? AND status='draft'").run(code);
      return res.json({ ok: true, deleted: info.changes > 0 });
    }
    const id = upsertDraft(code, resolveSupplierName(code), items, trimS(note), requestedDate);
    res.json({ ok: true, id });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/supplier/:code/issue', (req, res) => {
  try {
    const code = normSupplierCode(req.params.code);
    const { items, note, requestedDate } = req.body || {};
    if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ ok: false, error: '発注明細が空です' });
    // Idempotency-Key があれば再送しても同じ発注を二重作成しない (レスポンス消失・リトライ対策)。
    // payload は明細を発行処理と同じ正規化 (normProductCode) + コード/数量順の最小形に (並び順・表記差で409にしない)
    // requestedDate は指定があるときだけキーを含める (未指定の発注は旧形式とハッシュ一致 = デプロイ跨ぎの再送でも409にしない)
    const canonicalItems = items.map(i => {
      const o = { code: normProductCode(i && i.code), qty: Number(i && i.qty) };
      const rd = trimS(i && i.requestedDate);
      if (rd) o.requestedDate = rd;
      return o;
    }).sort((a, b) => a.code.localeCompare(b.code) || a.qty - b.qty);
    const { replay, result } = withCommand(
      { idempotencyKey: trimS(req.get('Idempotency-Key')) || null, payload: { op: 'issue', code, items: canonicalItems, note: trimS(note), requestedDate: requestedDate || null } },
      () => issueOrder(code, resolveSupplierName(code), items, trimS(note), requestedDate)
    );
    res.json({ ok: true, id: result.id, poNumber: result.poNumber, replay });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

router.get('/api/orders', (req, res) => {
  try {
    const db = getDB();
    const rows = db.prepare(`
      SELECT o.id, o.supplier_code, o.supplier_name, o.status, o.note, o.created_at, o.issued_at,
             o.po_number, o.closed_at, o.tracking_mode,
             COUNT(i.id) AS sku_count, COALESCE(SUM(i.qty),0) AS total_qty,
             COALESCE(SUM(i.qty * COALESCE(i.unit_cost,0)),0) AS total_amount,
             COALESCE(SUM(b.remaining_qty),0) AS remaining_qty
      FROM po_orders o
      LEFT JOIN po_order_items i ON i.order_id = o.id
      LEFT JOIN v_po_item_balance b ON b.order_item_id = i.id
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

// ─── P13b: 発注残・消込 API ───
const actorOf = req => (req.session && (req.session.email || req.session.displayName)) || 'portal';

router.get('/api/backorders', (req, res) => {
  try { res.json({ ok: true, ...listBackorders() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// NE発注残CSVの初期取込 (移行PO作成)。commit='1' 以外はプレビューのみ (書込なし)。
// commit時はプレビュー応答の fileHash を要求し、プレビューしたCSVと同一内容であることを検証する (別ファイル誤取込防止)。
// 冪等性は ne_slip_number のUNIQUE+txn内スキップで担保 (取込済み伝票はスキップ) のため Idempotency-Key は不要
router.post('/api/backorders/ne-import', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'ファイルがありません' });
    let buf;
    try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
    const result = importNeBackorderCsv({
      buffer: buf,
      commit: !!(req.body && req.body.commit === '1'),
      expectedHash: (req.body && trimS(req.body.fileHash)) || null,
      expectedPlanHash: (req.body && trimS(req.body.planHash)) || null,
      actor: actorOf(req),
    });
    res.json({ ok: true, ...result });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.get('/api/items/:itemId/events', (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    if (!Number.isSafeInteger(itemId) || itemId <= 0) return res.status(400).json({ ok: false, error: 'itemIdが不正です' });
    res.json({ ok: true, events: listItemEvents(itemId), balance: balanceOf(itemId) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

/**
 * 消込イベント登録 (入荷/減数/取消)。部分入荷時は remainder (三択) を同一トランザクションで適用する。
 * body: { type, qty, reasonCode?, note?, effectiveDate?,
 *         remainder?: { action: 'await_delivery'|'shortage'|'await_confirmation',
 *                       nextExpectedDate?, nextExpectedQty?, nextActionDate?, reasonCode?, note? } }
 */
router.post('/api/items/:itemId/events', (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    if (!Number.isSafeInteger(itemId) || itemId <= 0) return res.status(400).json({ ok: false, error: 'itemIdが不正です' });
    const { type, qty, reasonCode, note, effectiveDate, remainder, inboundItemId } = req.body || {};
    if (!['receipt', 'shortage', 'cancel'].includes(type)) return res.status(400).json({ ok: false, error: 'typeが不正です' });
    const actor = actorOf(req);
    const db = getDB();
    // ロジザード入庫からの割当: source=logizard+入庫明細参照。業務日付の既定は入庫日
    let inbound = null;
    if (type === 'receipt' && inboundItemId != null) {
      const iid = Number(inboundItemId);
      if (!Number.isSafeInteger(iid) || iid <= 0) return res.status(400).json({ ok: false, error: 'inboundItemIdが不正です' });
      inbound = db.prepare('SELECT i.id, r.receipt_date FROM po_inbound_items i JOIN po_inbound_receipts r ON r.id=i.receipt_id WHERE i.id=?').get(iid);
      if (!inbound) return res.status(400).json({ ok: false, error: '入庫明細が存在しません' });
    }
    // 同一操作で登録する receipt と減数は業務日付を揃える (入庫日既定、Codex P14-R1 Medium-2)
    const eventDate = trimS(effectiveDate) || (inbound && inbound.receipt_date) || null;
    const run = () => db.transaction(() => {
      const ev = appendPoItemEvent({
        orderItemId: itemId, eventType: type, qty: Number(qty),
        source: type === 'receipt' ? (inbound ? 'logizard' : 'manual') : null,
        inboundItemId: inbound ? inbound.id : null,
        reasonCode: type === 'shortage' ? (reasonCode || null) : null,
        note: trimS(note) || null,
        effectiveDate: eventDate,
        actorType: 'user', actor,
      });
      // 残数が残る場合の三択 (要件F-2: 同一txnで必須。UI側も必須にするがサーバでも検証)
      let plan = null;
      if (ev.remaining > 0) {
        const r = remainder || {};
        if (r.action === 'await_delivery') {
          plan = setItemPlan(itemId, {
            remainder_disposition: 'awaiting_delivery',
            next_expected_date: trimS(r.nextExpectedDate) || null,
            next_expected_qty: r.nextExpectedQty != null && r.nextExpectedQty !== '' ? Number(r.nextExpectedQty) : ev.remaining,
          }, { actorType: 'user', actor });
        } else if (r.action === 'shortage') {
          const sh = appendPoItemEvent({
            orderItemId: itemId, eventType: 'shortage', qty: ev.remaining,
            reasonCode: r.reasonCode || 'supplier_shortage', note: trimS(r.note) || null,
            effectiveDate: eventDate, actorType: 'user', actor,
          });
          ev.remaining = sh.remaining; ev.orderClosed = sh.orderClosed;
        } else if (r.action === 'await_confirmation') {
          plan = setItemPlan(itemId, {
            remainder_disposition: 'awaiting_confirmation',
            next_action_date: trimS(r.nextActionDate) || null,
          }, { actorType: 'user', actor });
        } else {
          throw new Error('残数の扱い (分納待ち/減数で完了/確認中) を選択してください');
        }
      }
      return { ...ev, plan };
    }).immediate();
    const { replay, result } = withCommand(
      { idempotencyKey: trimS(req.get('Idempotency-Key')) || null, payload: { op: 'item_event', itemId, type, qty, reasonCode, note, effectiveDate, remainder, inboundItemId: inboundItemId ?? null } },
      run
    );
    res.json({ ok: true, ...result, replay });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

router.post('/api/events/:eventId/reverse', (req, res) => {
  try {
    const eventId = Number(req.params.eventId);
    if (!Number.isSafeInteger(eventId) || eventId <= 0) return res.status(400).json({ ok: false, error: 'eventIdが不正です' });
    const note = trimS((req.body || {}).note);
    const { replay, result } = withCommand(
      { idempotencyKey: trimS(req.get('Idempotency-Key')) || null, payload: { op: 'reverse', eventId, note } },
      () => {
        const r = reverseEvent(eventId, { note, actorType: 'user', actor: actorOf(req) });
        // 逆仕訳で残数が復活した場合、扱い (三択) は未選択になる → UIに要選択を伝える (要対応リストにも載る)
        const db = getDB();
        const ev = db.prepare('SELECT order_item_id FROM po_item_events WHERE id=?').get(eventId);
        const item = db.prepare('SELECT remainder_disposition FROM po_order_items WHERE id=?').get(ev.order_item_id);
        return { ...r, needsDisposition: r.remaining > 0 && item.remainder_disposition == null };
      }
    );
    res.json({ ok: true, ...result, replay });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

router.post('/api/orders/:id/close', (req, res) => {
  try {
    const orderId = Number(req.params.id);
    if (!Number.isSafeInteger(orderId) || orderId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    const note = trimS((req.body || {}).note);
    const { replay, result } = withCommand(
      { idempotencyKey: trimS(req.get('Idempotency-Key')) || null, payload: { op: 'close', orderId, note } },
      () => manualCloseOrder(orderId, { note, actorType: 'user', actor: actorOf(req) })
    );
    res.json({ ok: true, ...result, replay });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

/** 納期・分納予定・disposition の更新 (現在値+履歴を同一txn) */
router.patch('/api/items/:itemId/plan', (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    if (!Number.isSafeInteger(itemId) || itemId <= 0) return res.status(400).json({ ok: false, error: 'itemIdが不正です' });
    const b = req.body || {};
    const patch = {};
    for (const [k, col] of [['promisedDate', 'promised_date'], ['nextExpectedDate', 'next_expected_date'],
      ['nextExpectedQty', 'next_expected_qty'], ['nextActionDate', 'next_action_date'], ['disposition', 'remainder_disposition']]) {
      if (k in b) patch[col] = b[k] === '' ? null : b[k];
    }
    if (!Object.keys(patch).length) return res.status(400).json({ ok: false, error: '更新項目がありません' });
    const { replay, result } = withCommand(
      { idempotencyKey: trimS(req.get('Idempotency-Key')) || null, payload: { op: 'plan', itemId, patch, note: trimS(b.note) || null } },
      () => setItemPlan(itemId, patch, { note: trimS(b.note) || null, actorType: 'user', actor: actorOf(req) })
    );
    res.json({ ok: true, ...result, replay });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

// ─── P14: ロジザード入庫実績の取込・突合 ───
router.post('/api/inbound/import', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'ファイルがありません' });
    let buf;
    try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
    const result = importInboundCsv({ buffer: buf, filename: req.file.originalname, actor: actorOf(req) });
    res.json({ ok: true, ...result });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.get('/api/inbound', (req, res) => {
  try { res.json({ ok: true, ...listInbound() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/inbound/:id/candidates', (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isSafeInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    res.json({ ok: true, ...candidatesFor(id) });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/inbound/:id/ignore', (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isSafeInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    const b = req.body || {};
    const result = setInboundIgnore(id, { ignore: !!b.ignore, reason: trimS(b.reason) || null, actor: actorOf(req) });
    res.json({ ok: true, ...result });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// ─── P15: 発注書メール送信 ───
router.get('/api/orders/:id/email/preview', (req, res) => {
  try {
    const orderId = Number(req.params.id);
    if (!Number.isSafeInteger(orderId) || orderId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    const p = buildOrderEmail(orderId);
    res.json({
      ok: true,
      to: p.to, cc: p.cc, subject: p.subject, body: p.body,
      rows: p.rows, totalQty: p.totalQty, totalAmount: p.totalAmount,
      attachmentName: p.attachmentName, csvText: p.csvText,
      vendorColUsed: p.vendorColUsed, missingVendorCodes: p.missingVendorCodes,
      mode: p.mode, dryrunTo: p.dryrunTo, envReady: p.envReady,
      jobs: listEmailJobs(orderId),
    });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/orders/:id/email/send', async (req, res) => {
  try {
    const orderId = Number(req.params.id);
    if (!Number.isSafeInteger(orderId) || orderId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    const b = req.body || {};
    const resend = !!b.resend;
    const resendOfJobId = b.resendOfJobId != null ? Number(b.resendOfJobId) : null;
    const scheduledAt = trimS(b.scheduledAt) || null;
    const expectedMode = trimS(b.expectedMode) || null; // プレビュー時のモード (現在モードと不一致なら拒否)
    // 冪等キーは必須 (再送ジョブはdedup対象外のため、通信断後の再実行・自動リトライでの複数通送信をキーで防ぐ、Codex P15-R4 High)
    const idemKey = trimS(req.get('Idempotency-Key'));
    if (!idemKey) return res.status(400).json({ ok: false, error: 'Idempotency-Key ヘッダが必要です (画面からの送信では自動付与されます)' });
    const { replay, result: jobId } = withCommand(
      { idempotencyKey: idemKey, payload: { op: 'email_send', orderId, resend, resendOfJobId, scheduledAt, expectedMode } },
      () => createEmailJob(orderId, { resend, resendOfJobId, scheduledAt, expectedMode, actor: actorOf(req) })
    );
    const outcome = await processEmailJob(jobId);
    const jb = getDB().prepare('SELECT is_dry_run FROM po_email_jobs WHERE id=?').get(jobId);
    res.json({ ok: true, jobId, replay, dryRun: !!(jb && jb.is_dry_run), ...outcome, jobs: listEmailJobs(orderId) });
  } catch (e) { res.status(e.status === 409 ? 409 : 400).json({ ok: false, error: e.message }); }
});

router.post('/api/email-jobs/:id/retry', async (req, res) => {
  try {
    const jobId = Number(req.params.id);
    if (!Number.isSafeInteger(jobId) || jobId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    const outcome = await processEmailJob(jobId);
    res.json({ ok: true, jobId, ...outcome });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// unknown (結果不明) → 人間がGmail送信済みを確認して「未送信」を宣言した場合のみ再試行可能に戻す
router.post('/api/email-jobs/:id/mark-unsent', (req, res) => {
  try {
    const jobId = Number(req.params.id);
    if (!Number.isSafeInteger(jobId) || jobId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    if (trimS((req.body || {}).confirm) !== '未送信') return res.status(400).json({ ok: false, error: '確認のため confirm に「未送信」と入力してください' });
    res.json({ ok: true, ...markUnsent(jobId, { actor: actorOf(req) }) });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/email-jobs/:id/cancel', (req, res) => {
  try {
    const jobId = Number(req.params.id);
    if (!Number.isSafeInteger(jobId) || jobId <= 0) return res.status(400).json({ ok: false, error: 'idが不正です' });
    res.json({ ok: true, ...cancelEmailJob(jobId, { actor: actorOf(req) }) });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.post('/api/email/reconcile', async (req, res) => {
  try { res.json({ ok: true, ...(await reconcileEmailJobs()) }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/email/settings', (req, res) => {
  try { res.json({ ok: true, ...emailSettings() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.post('/api/email/settings', (req, res) => {
  try {
    const b = req.body || {};
    const actor = actorOf(req);
    const changed = [];
    // mode (dry_run/live) は専用API /api/email/mode でのみ変更 (誤操作でliveにしない、Codex P15-R1 M9)。
    // 全項目を1txnで適用 (途中の検証失敗で部分反映しない、Codex P15-R10 Medium)
    getDB().transaction(() => {
      for (const [key, val] of [['email_dryrun_to', b.dryrunTo],
        ['email_subject_template', b.subjectTpl], ['email_body_template', b.bodyTpl]]) {
        if (val == null) continue;
        setSetting(key, String(val), { actor, reason: 'メール設定画面から変更' });
        changed.push(key);
      }
    })();
    res.json({ ok: true, changed, ...emailSettings() });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// live切替は専用API+確認文字列必須。監査ログでも通常設定と区別される
router.post('/api/email/mode', (req, res) => {
  try {
    const b = req.body || {};
    const mode = trimS(b.mode);
    if (mode === 'live' && trimS(b.confirm) !== 'LIVE') {
      return res.status(400).json({ ok: false, error: '本番送信への切替は confirm に「LIVE」と入力してください' });
    }
    setSetting('email_mode', mode, { actor: actorOf(req), reason: mode === 'live' ? '⚠️本番送信へ切替' : 'dry-runへ切替' });
    res.json({ ok: true, ...emailSettings() });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// 宛先マスタCSV取込 (既存GASスプシ「仕入先ごとの発注メール送信先一覧」の生DL。仕入先名称で突合)
router.post('/api/email/recipients/csv', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'ファイルがありません' });
    let buf;
    try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
    const rows = parseCsvStrict(decodeCsvBuffer(buf));
    if (rows.length < 2) return res.status(400).json({ ok: false, error: 'データ行がありません' });
    const header = rows[0].map(s => String(s).trim());
    const iName = header.indexOf('仕入先名称');
    const iMail = header.indexOf('メールアドレス');
    const iContact = header.findIndex(h => h.startsWith('担当者名'));
    const iCc = header.indexOf('CCメールアドレス');
    if (iName === -1 || iMail === -1) return res.status(400).json({ ok: false, error: '「仕入先名称」「メールアドレス」列が必要です (宛先マスタのCSVを入れてください)' });
    const db = getDB();
    const norm = s => trimS(s).replace(/様$/, '');
    // 同名仕入先が複数ある場合は「どれか1つに黙って登録」せず曖昧エラーにする (誤送信防止、Codex P15-R1 High-4)
    const byName = new Map();
    for (const r of db.prepare('SELECT supplier_code, name FROM po_suppliers').all()) {
      const k = norm(r.name);
      if (!byName.has(k)) byName.set(k, []);
      byName.get(k).push(r.supplier_code);
    }
    const updated = [], unmatched = [], invalid = [], nonEmailMethod = [];
    const seenInCsv = new Set();
    db.transaction(() => {
      const upd = db.prepare("UPDATE po_suppliers SET email_to=?, email_cc=?, contact_name=?, send_method=COALESCE(send_method,'email'), updated_at=? WHERE supplier_code=?");
      const methodOf = db.prepare('SELECT send_method FROM po_suppliers WHERE supplier_code=?');
      for (let n = 1; n < rows.length; n++) {
        const r = rows[n];
        const name = trimS(r[iName]);
        if (!name) continue;
        const k = norm(name);
        if (seenInCsv.has(k)) { invalid.push(`${name}: CSV内で重複しています (2回目以降は無視)`); continue; }
        seenInCsv.add(k);
        const codes = byName.get(k) || [];
        if (codes.length === 0) { unmatched.push(name); continue; }
        if (codes.length > 1) { invalid.push(`${name}: 同名の仕入先が${codes.length}件あり特定できません (コード: ${codes.join(', ')}) — マスタで個別に登録してください`); continue; }
        try {
          const to = parseAddresses(r[iMail]);
          if (!to.length) { invalid.push(`${name}: メールアドレスが空`); continue; }
          const cc = iCc === -1 ? [] : parseAddresses(r[iCc]);
          upd.run(to.join(','), cc.join(',') || null, iContact === -1 ? null : trimS(r[iContact]) || null, nowIso(), codes[0]);
          updated.push({ name, code: codes[0] });
          // 既存の送信方法 (fax/web/none) は保護して上書きしない。ただし黙っていると「取込したのに送れない」ため警告する
          const m = methodOf.get(codes[0]).send_method;
          if (m && m !== 'email') nonEmailMethod.push(`${name} (送信方法=${m}のまま。メール送信するにはマスタでemailに変更)`);
        } catch (e) { invalid.push(`${name}: ${e.message}`); }
      }
      audit(db, { actorType: 'user', actor: actorOf(req), action: 'email_recipients_import', resource: 'master:suppliers',
        detail: { updated, unmatched, invalidCount: invalid.length, nonEmailMethod } });
    })();
    res.json({ ok: true, updated: updated.length, unmatched, invalid: invalid.concat(nonEmailMethod) });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// 先方管理番号対応表CSV取込 (アメージング/ビーフリー等。仕入先ごとに全置換)
router.post('/api/vendor-map/csv', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'ファイルがありません' });
    // 一時ファイルはどの終了経路でも削除する (検証エラーの繰り返しでアップロード領域を消費させない、Codex P15-R5 Medium)
    let buf;
    try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch {} }
    const supplierCode = normSupplierCode((req.body || {}).supplier_code);
    if (!supplierCode) return res.status(400).json({ ok: false, error: '仕入先コードが必要です' });
    const db = getDB();
    const sup = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplierCode);
    if (!sup) return res.status(400).json({ ok: false, error: `仕入先が未登録です: ${supplierCode}` });
    const rows = parseCsvStrict(decodeCsvBuffer(buf));
    if (rows.length < 2) return res.status(400).json({ ok: false, error: 'データ行がありません' });
    const header = rows[0].map(s => String(s).trim());
    let iVendor = header.indexOf('仕入先管理番号');
    let iOur = header.indexOf('弊社管理番号');
    const warnings = [];
    if (iVendor === -1 || iOur === -1) {
      // 別形式CSVの誤取込防止: 自動フォールバックせず、明示チェック時のみ位置 (A列/C列=GAS対応表と同じ) で読む
      if (String((req.body || {}).positional) !== '1') {
        return res.status(400).json({ ok: false, error: '見出し「仕入先管理番号」「弊社管理番号」が見つかりません。対応表のCSVか確認し、見出しなしのCSVなら「A列/C列で取り込む」にチェックしてください' });
      }
      iVendor = 0; iOur = 2;
      warnings.push('見出しなしのため A列=先方番号 / C列=弊社コード として取り込みました');
    }
    let count = 0, skipped = 0;
    const oldCount = db.prepare('SELECT COUNT(*) AS n FROM po_vendor_code_map WHERE supplier_code=?').get(supplierCode).n;
    db.transaction(() => {
      db.prepare('DELETE FROM po_vendor_code_map WHERE supplier_code=?').run(supplierCode);
      const ins = db.prepare('INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES (?,?,?,?,?)');
      const seen = new Set();
      for (let n = 1; n < rows.length; n++) {
        const vendor = trimS(rows[n][iVendor]);
        const our = trimS(rows[n][iOur]);
        if (!vendor || !our) { skipped++; continue; }
        const key = normProductCode(our);
        // 同一商品の重複を後勝ちで黙って上書きしない (誤った先方番号の混入防止、Codex P15-R4 Medium)
        if (seen.has(key)) throw new Error(`行${n + 1}: 商品コード ${our} が重複しています (大文字小文字・空白違い含む)。CSVを修正してください`);
        seen.add(key);
        ins.run(supplierCode, key, our, vendor, nowIso());
        count++;
      }
      // 誤CSV・空データで正常な対応表を消さない (0件はロールバック)
      if (count === 0) throw new Error('有効な行が0件のため取込を中止しました (既存の対応表は変更していません)');
      audit(db, { actorType: 'user', actor: actorOf(req), action: 'vendor_map_import', resource: `supplier:${supplierCode}`,
        detail: { oldCount, newCount: count, skipped } });
    })();
    res.json({ ok: true, supplier: sup.name, count, skipped, oldCount, warnings });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.get('/api/vendor-map', (req, res) => {
  try {
    const db = getDB();
    const rows = db.prepare(`SELECT m.supplier_code, s.name, COUNT(*) AS n FROM po_vendor_code_map m
      LEFT JOIN po_suppliers s ON s.supplier_code = m.supplier_code GROUP BY m.supplier_code`).all();
    res.json({ ok: true, maps: rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── 対応表の1件管理 (マスタ管理「先方番号対応表」タブ。CSV全置換と違い1商品ずつ追加・修正できる) ───

// 一覧 (商品名はPMLから解決。廃番等でPMLに無い商品は名前空欄のまま表示)
router.get('/api/vendor-map/entries', (req, res) => {
  try {
    const db = getDB();
    const supplier = normSupplierCode(req.query.supplier);
    if (!supplier) return res.status(400).json({ ok: false, error: '仕入先コードが必要です' });
    const sup = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplier);
    if (!sup) return res.status(400).json({ ok: false, error: `仕入先が未登録です: ${supplier}` });
    const nameByKey = new Map();
    for (const r of loadPml().rows) nameByKey.set(normProductCode(r['商品コード']), r['商品名'] || '');
    const rows = db.prepare('SELECT product_code, product_key, vendor_code, updated_at FROM po_vendor_code_map WHERE supplier_code=? ORDER BY product_code')
      .all(supplier).map(r => ({ ...r, name: nameByKey.get(r.product_key) || '' }));
    res.json({ ok: true, supplierName: sup.name, rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// 追加フォームの商品検索 (コード/商品名の部分一致、上限30件)。
// 仕入先指定時はその仕入先の商品のみ返す (他仕入先の商品はupsertでも拒否されるため候補に出さない)
router.get('/api/vendor-map/products', (req, res) => {
  try {
    const q = trimS(req.query.q).toLowerCase();
    if (!q) return res.json({ ok: true, rows: [] });
    const supplier = normSupplierCode(req.query.supplier);
    const hits = [];
    for (const r of loadPml().rows) {
      const code = trimS(r['商品コード']);
      if (!code) continue;
      const prodSup = normSupplierCode(r['仕入先']);
      if (supplier && prodSup !== supplier) continue; // 仕入先未設定 (空欄) の商品も候補に出さない (Codex R2 High)
      const name = r['商品名'] || '';
      if (code.toLowerCase().includes(q) || name.toLowerCase().includes(q)) hits.push({ code, name, supplier: prodSup });
    }
    hits.sort((a, b) => a.code.localeCompare(b.code));
    res.json({ ok: true, rows: hits.slice(0, 30), total: hits.length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

const VENDOR_CODE_MAX = 100; // 先方管理番号の実仕様は十数文字。誤貼り付けの肥大化 (監査ログ・添付CSVに複製される) を防ぐ

// 1件追加・更新 (upsert)。商品コードはPML実在+仕入先一致チェックでタイポ・誤仕入先登録を防ぐ (廃番をあえて残す用途はCSV取込で)。
// baseUpdatedAt: 楽観ロック (UIは常に送る。表示後に他者が変更/CSV全置換していたら409)。未指定なら無条件upsert (スクリプト/AI連携用)
router.post('/api/vendor-map/entry', (req, res) => {
  try {
    const db = getDB();
    const b = req.body || {};
    const supplier = normSupplierCode(b.supplier_code);
    const productCode = trimS(b.product_code);
    const vendorCode = trimS(b.vendor_code);
    if (!supplier) return res.status(400).json({ ok: false, error: '仕入先コードが必要です' });
    const sup = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(supplier);
    if (!sup) return res.status(400).json({ ok: false, error: `仕入先が未登録です: ${supplier}` });
    if (!productCode) return res.status(400).json({ ok: false, error: '商品コードが必要です' });
    if (productCode.length > 200) return res.status(400).json({ ok: false, error: '商品コードが長すぎます' });
    if (!vendorCode) return res.status(400).json({ ok: false, error: '先方管理番号が必要です' });
    if (/[\r\n]/.test(vendorCode)) return res.status(400).json({ ok: false, error: '先方管理番号に改行は使えません' });
    if (vendorCode.length > VENDOR_CODE_MAX) return res.status(400).json({ ok: false, error: `先方管理番号が長すぎます (${VENDOR_CODE_MAX}文字まで)` });
    const key = normProductCode(productCode);
    const pmlRow = loadPml().rows.find(r => normProductCode(r['商品コード']) === key);
    if (!pmlRow) return res.status(400).json({ ok: false, error: `商品マスタに無い商品コードです: ${productCode} (廃番等を登録する場合はCSV取込で)` });
    const prodSup = normSupplierCode(pmlRow['仕入先']);
    if (!prodSup) return res.status(400).json({ ok: false, error: `この商品は商品マスタで仕入先が未設定です: ${productCode} (先に商品マスタ側を直してください)` });
    if (prodSup !== supplier) {
      return res.status(400).json({ ok: false, error: `この商品の仕入先は ${prodSup} です (${sup.name} の対応表には登録できません)` });
    }
    let out = null;
    db.transaction(() => {
      const old = db.prepare('SELECT vendor_code, updated_at FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').get(supplier, key);
      if (b.baseUpdatedAt !== undefined) {
        // 楽観ロック: null=「新規のつもり」、文字列=「この版を見て編集した」
        if (b.baseUpdatedAt == null && old) {
          out = { status: 409, body: { ok: false, conflict: true, error: `この商品は既に登録されています (現在: ${old.vendor_code})。一覧のセルを編集してください`, current: old } };
          return;
        }
        if (b.baseUpdatedAt != null && (!old || old.updated_at !== b.baseUpdatedAt)) {
          out = { status: 409, body: { ok: false, conflict: true, error: old ? `他の画面で変更されています (現在: ${old.vendor_code})。最新を確認してください` : '他の画面で削除されています。最新を確認してください', current: old || null } };
          return;
        }
      }
      db.prepare(`INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES (?,?,?,?,?)
        ON CONFLICT(supplier_code, product_key) DO UPDATE SET product_code=excluded.product_code, vendor_code=excluded.vendor_code, updated_at=excluded.updated_at`)
        .run(supplier, key, productCode, vendorCode, nowIso());
      audit(db, { actorType: 'user', actor: actorOf(req), action: 'vendor_map_entry_upsert', resource: `supplier:${supplier}`,
        detail: { productCode, vendorCode, oldVendorCode: old ? old.vendor_code : null } });
      out = { status: 200, body: { ok: true, updated: !!old, oldVendorCode: old ? old.vendor_code : null } };
    })();
    res.status(out.status).json(out.body);
  } catch (e) {
    console.error('[purchase-orders] vendor-map entry upsert failed:', e);
    res.status(500).json({ ok: false, error: '内部エラーが発生しました' });
  }
});

// 1件削除。base=表示時のupdated_at (楽観ロック、未指定なら無条件)
router.delete('/api/vendor-map/entry', (req, res) => {
  try {
    const db = getDB();
    const supplier = normSupplierCode(req.query.supplier);
    const key = normProductCode(req.query.product);
    if (!supplier || !key) return res.status(400).json({ ok: false, error: '仕入先コードと商品コードが必要です' });
    let out = null;
    db.transaction(() => {
      const old = db.prepare('SELECT product_code, vendor_code, updated_at FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').get(supplier, key);
      if (!old) { out = { status: 404, body: { ok: false, error: '対象の対応がありません (既に削除済みかもしれません)' } }; return; }
      if (req.query.base != null && req.query.base !== '' && old.updated_at !== req.query.base) {
        out = { status: 409, body: { ok: false, conflict: true, error: `他の画面で変更されています (現在: ${old.vendor_code})。最新を確認してください`, current: old } };
        return;
      }
      db.prepare('DELETE FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').run(supplier, key);
      audit(db, { actorType: 'user', actor: actorOf(req), action: 'vendor_map_entry_delete', resource: `supplier:${supplier}`,
        detail: { productCode: old.product_code, vendorCode: old.vendor_code } });
      out = { status: 200, body: { ok: true } };
    })();
    res.status(out.status).json(out.body);
  } catch (e) {
    console.error('[purchase-orders] vendor-map entry delete failed:', e);
    res.status(500).json({ ok: false, error: '内部エラーが発生しました' });
  }
});

// P13a: 台帳の整合性検査 (負残/closed不整合/発行取りこぼし/disposition規則)。P13bで管理画面に表示する
router.get('/api/ledger/integrity', (req, res) => {
  try {
    let orderId = null;
    if (req.query.orderId != null && req.query.orderId !== '') {
      orderId = Number(req.query.orderId);
      if (!Number.isSafeInteger(orderId) || orderId <= 0) return res.status(400).json({ ok: false, error: 'orderIdが不正です' });
    }
    const { issues, warnings } = checkLedgerIntegrity({ orderId });
    res.json({ ok: true, healthy: issues.length === 0, issues, warnings });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── マスタ CRUD ───
const MASTER_DEFS = {
  suppliers: {
    table: 'po_suppliers', pk: 'supplier_code',
    fromBody: b => ({
      supplier_code: normSupplierCode(b.supplier_code), name: trimS(b.name), order_memo: trimS(b.order_memo) || null,
      email_to: parseAddresses(b.email_to || '').join(',') || null,
      email_cc: parseAddresses(b.email_cc || '').join(',') || null,
      contact_name: trimS(b.contact_name) || null,
      send_method: trimS(b.send_method) || null,
      lead_days: numOrNull(b.lead_days),
    }),
    validate: r => {
      if (!r.supplier_code) return '仕入先コード必須';
      if (!r.name) return '仕入先名必須';
      if (r.send_method && !['email', 'fax', 'web', 'none'].includes(r.send_method)) return '送信方法は email/fax/web/none';
      if (r.lead_days != null && (!Number.isInteger(r.lead_days) || r.lead_days < 0)) return 'リードタイムは0以上の整数';
      return null;
    },
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
  // 「選べる◯種セット」構成商品 (在庫を切らさない前提。在庫+注残≦最低在庫で要発注入り)
  selectable: {
    table: 'po_selectable_products', pk: 'product_key', normId: normProductCode,
    fromBody: b => ({
      product_key: normProductCode(b.product_code), product_code: trimS(b.product_code),
      set_names: trimS(b.set_names) || null, min_stock: numOrNull(b.min_stock),
    }),
    validate: r => {
      if (!r.product_code) return '商品コード必須';
      if (r.min_stock != null && r.min_stock < 0) return '最低在庫数が不正';
      return null;
    },
    csvHeader: ['商品コード', 'セット名', '最低在庫数'],
    fromCsv: c => ({
      product_key: normProductCode(c[0]), product_code: trimS(c[0]),
      set_names: trimS(c[1]) || null, min_stock: numOrNull(c[2]),
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

// アコーディオンからのグループ紐付け (既存attrsの容量/ケース等を保持したまま条件/原料だけ更新)
router.post('/api/attrs/bind', (req, res) => {
  try {
    const b = req.body || {};
    const code = trimS(b.product_code);
    if (!code) return res.status(400).json({ ok: false, error: '商品コード必須' });
    const key = normProductCode(code);
    // PMLに実在する商品のみ (API直叩きで任意コードに紐付けられるのを防ぐ、Codex P9 High)
    if (!loadPml().rows.some(r => normProductCode(r['商品コード']) === key)) {
      return res.status(400).json({ ok: false, error: `商品がPMLに存在しません: ${code}` });
    }
    const db = getDB();
    const cur = db.prepare('SELECT * FROM po_product_attrs WHERE product_key=?').get(key) || {};
    const row = {
      product_key: key, product_code: cur.product_code || code,
      condition_id: 'condition_id' in b ? (trimS(b.condition_id) || null) : (cur.condition_id || null),
      material_group_id: 'material_group_id' in b ? (trimS(b.material_group_id) || null) : (cur.material_group_id || null),
      capacity_per_unit: cur.capacity_per_unit != null ? cur.capacity_per_unit : null,
      case_group: cur.case_group || null,
      case_lot: cur.case_lot != null ? cur.case_lot : null,
    };
    const err = MASTER_DEFS.attrs.validate(row);
    if (err) return res.status(400).json({ ok: false, error: err });
    upsertMasterRow(MASTER_DEFS.attrs, row);
    res.json({ ok: true, row });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

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
    let rows = getDB().prepare(`SELECT * FROM ${def.table} ORDER BY ${def.pk}`).all();
    // 人間が読める名前を付与 (商品コード/IDだけでは探せない、ユーザビリティ)
    if (req.params.kind === 'attrs' || req.params.kind === 'selectable') {
      const nameByKey = new Map();
      for (const r of loadPml().rows) nameByKey.set(normProductCode(r['商品コード']), r['商品名'] || '');
      rows = rows.map(r => ({ ...r, 商品名: nameByKey.get(r.product_key) || '' }));
    } else if (req.params.kind === 'conditions') {
      const supName = new Map();
      for (const s of getDB().prepare('SELECT supplier_code, name FROM po_suppliers').all()) supName.set(s.supplier_code, s.name);
      rows = rows.map(r => ({ ...r, 仕入先名: supName.get(normSupplierCode(r.supplier_code)) || '' }));
    }
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

// 数値パース (カンマ許容)。非空なのに数値化不能/範囲外の値は warn に記録して null (黙って欠落させない)。
//   opts.min: これ未満(n<min)を却下 / opts.gt: これ以下(n<=gt)を却下 — DBのCHECK制約と揃える。
function numOrWarn(raw, warn, ctx, opts = {}) {
  const s = trimS(raw).replace(/,/g, '');
  if (s === '') return null;
  const n = Number(s);
  if (!Number.isFinite(n)) { if (warn) warn(`${ctx}: 数値でない値「${trimS(raw)}」を無視しました`); return null; }
  if (opts.min != null && n < opts.min) { if (warn) warn(`${ctx}: 範囲外の値「${n}」(${opts.min}以上が必要) を無視しました`); return null; }
  if (opts.gt != null && n <= opts.gt) { if (warn) warn(`${ctx}: 範囲外の値「${n}」(${opts.gt}より大きい値が必要) を無視しました`); return null; }
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
        out.push({ table: 'materials', row: { group_id: id, name: trimS(c[iName]) || id, min_order_qty: iMin >= 0 ? numOrWarn(c[iMin], warn, `原料グループマスタ ${r + 1}行目 最低発注量`, { min: 0 }) : null, unit: iUnit >= 0 ? (trimS(c[iUnit]) || null) : null } });
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
    key: 'selectable', label: '選べるセット構成商品',
    // 商品コード + セット名 だけのリスト (楽天/Yahooの項目選択肢セット)。商品マスタ判別より先に評価
    detect: h => hasCol(h, '商品コード') && hasCol(h, 'セット名', '選べるセット名', 'セット', '商品名（セット）') && !hasCol(h, '商品グループID') && !hasCol(h, '原料グループID'),
    extract(header, rows, warn) {
      const iCode = colIndex(header, '商品コード'), iSet = colIndex(header, 'セット名', '選べるセット名', 'セット', '商品名（セット）');
      const iMin = colIndex(header, '最低在庫数', '最低在庫', 'しきい値');
      // 同一商品が複数セットに出る場合はセット名を「、」で束ねて1行に
      const byKey = new Map();
      for (let r = 1; r < rows.length; r++) {
        const c = rows[r]; const code = trimS(c[iCode]); if (!code) continue;
        const key = normProductCode(code);
        const setName = iSet >= 0 ? trimS(c[iSet]) : '';
        const min = iMin >= 0 ? numOrWarn(c[iMin], warn, `選べるセット ${r + 1}行目(${code}) 最低在庫数`, { min: 0 }) : null;
        let cur = byKey.get(key);
        if (!cur) { cur = { product_key: key, product_code: code, sets: [], min_stock: null }; byKey.set(key, cur); }
        if (setName && !cur.sets.includes(setName)) cur.sets.push(setName);
        if (min != null) cur.min_stock = cur.min_stock == null ? min : Math.max(cur.min_stock, min);
      }
      return [...byKey.values()].map(v => ({ table: 'selectable', row: {
        product_key: v.product_key, product_code: v.product_code,
        set_names: v.sets.join('、') || null, min_stock: v.min_stock,
      } }));
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
        const cap = iCap >= 0 ? numOrWarn(c[iCap], warn, `商品マスタ ${r + 1}行目(${code}) 容量/個`, { gt: 0 }) : null;
        const cg = iCg >= 0 ? (trimS(c[iCg]) || null) : null, cl = iCl >= 0 ? numOrWarn(c[iCl], warn, `商品マスタ ${r + 1}行目(${code}) ケースロット`, { gt: 0 }) : null;
        if (cond || mat || cap || cg || cl) {
          out.push({ table: 'attrs', row: {
            product_key: normProductCode(code), product_code: code,
            condition_id: cond, material_group_id: mat,
            capacity_per_unit: cap, case_group: cg, case_lot: cl,
          } });
        }
      }
      // 仕入れ先名列が無い/空のときは placeholder 名。既存登録済みの名前を上書きしない (import側で判定)
      for (const [sc, sn] of suppliers) out.push({ table: 'suppliers', placeholder: !sn, row: { supplier_code: sc, name: sn || `仕入先 ${sc}`, order_memo: null } });
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
  if (table === 'selectable') return !!row.product_code;
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
  if (table === 'selectable') return '商品コードが空';
  return '不正な行';
}
const TABLE_LABEL = { materials: '原料グループ', conditions: '発注条件グループ', suppliers: '仕入先', attrs: '商品紐付け', selectable: '選べるセット構成' };

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
  const order = { materials: 1, conditions: 2, suppliers: 3, attrs: 4, selectable: 5 };
  const all = classified.flatMap(c => c.items).sort((a, b) => order[a.table] - order[b.table]);
  const counts = { materials: 0, conditions: 0, suppliers: 0, attrs: 0, selectable: 0 };
  const skipped = { materials: 0, conditions: 0, suppliers: 0, attrs: 0, selectable: 0 };
  const db = getDB();
  try {
    db.transaction(() => {
      for (const it of all) {
        if (!bulkValid(it.table, it.row)) {
          skipped[it.table]++;
          warn(`${TABLE_LABEL[it.table]}: ${bulkInvalidReason(it.table, it.row)} — 1件スキップ`);
          continue;
        }
        // 仕入れ先名なしCSV由来の placeholder 名 (仕入先 X) で既存の登録済み名を上書きしない
        if (it.table === 'suppliers' && it.placeholder &&
            db.prepare('SELECT 1 FROM po_suppliers WHERE supplier_code=?').get(it.row.supplier_code)) {
          counts.suppliers++;
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
    bumpCycleReset(actorOf(req), 'NE手動CSV取込で発注サイクル更新', now);
    res.json({ ok: true, rowCount: count, uploadedAt: now });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

router.delete('/api/ne-overlay', (req, res) => {
  try {
    const db = getDB();
    const startedAt = nowIso();
    db.transaction(() => {
      db.prepare('DELETE FROM po_ne_overlay_rows').run();
      db.prepare('DELETE FROM po_ne_overlay_meta').run();
    })();
    bumpCycleReset(actorOf(req), 'NE手動CSV解除で発注サイクル更新', startedAt);
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
    // FBA更新完了 = データが変わった時点で発注サイクルを更新 (jobId単位で一度だけ。冪等はDBの一意制約+同一txn)
    if (r && r.status === 'completed') {
      try { markCycleFbaJobDone(req.params.jobId, actorOf(req)); }
      catch (e) { console.error('[purchase-orders] FBAサイクル更新失敗:', e.message); } // ポーリング応答自体は返す
    }
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
    // 紐付けはあるが未登録グループを参照している商品 (取込警告が消えても見失わないよう常時可視化)
    const db = getDB();
    const dangling = db.prepare(`
      SELECT a.product_code AS code,
             CASE WHEN a.condition_id IS NOT NULL AND a.condition_id<>'' AND c.condition_id IS NULL THEN a.condition_id END AS missCond,
             CASE WHEN a.material_group_id IS NOT NULL AND a.material_group_id<>'' AND g.group_id IS NULL THEN a.material_group_id END AS missMat
      FROM po_product_attrs a
      LEFT JOIN po_order_conditions c ON c.condition_id = a.condition_id
      LEFT JOIN po_material_groups g ON g.group_id = a.material_group_id
      WHERE (a.condition_id IS NOT NULL AND a.condition_id<>'' AND c.condition_id IS NULL)
         OR (a.material_group_id IS NOT NULL AND a.material_group_id<>'' AND g.group_id IS NULL)
      LIMIT 500
    `).all();
    res.json({ ok: true, totalUnlinked: all.length, recentCount, days, count: filtered.length, rows: filtered.slice(0, 500), danglingCount: dangling.length, dangling });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── 商品別モール販売内訳 (売れ筋の在庫切れ防止の定点観測用) ───
// 速報 = mirror_f_sales_velocity_by_product_mall (NE受注ベース、毎朝、7/30日固定、全チャネル)
// 確定 = mirror_*_finance_sku_daily 6モール (精算/受注fact)。セット構成を展開して実出荷ピース数で数える
//        (supplier-sales と同じ数え方。3個セット1件=構成品3個)。期間は 30/90/180/365日
// 監査PR-11: MALL_LABELS ハードコードを dim_mall に集約 (値・フォールバック挙動は従来と同一:
// 未登録キーはキーをそのまま表示)。
// (監査PR-8: モール別テーブル定義 FIN_MALLS は v_mall_finance_daily_unified に集約され不要に)
router.get('/api/products/:code/mall-sales', (req, res) => {
  try {
    const code = trimS(req.params.code);
    if (!code) return res.status(400).json({ ok: false, error: '商品コードが必要です' });
    const daysParam = parseInt(req.query.days, 10);
    const days = [30, 90, 180, 365].includes(daysParam) ? daysParam : 90;
    const db = getDB();

    // 速報 (NE受注ベース・モール別 7/30日)
    const sokuho = db.prepare(`
      SELECT mall, qty_7d, qty_30d, as_of_date
      FROM mirror_f_sales_velocity_by_product_mall
      WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(?))
      ORDER BY qty_30d DESC, qty_7d DESC
    `).all(code).map(r => ({ mall: r.mall, label: loadDimMall(db).labelOf(r.mall), qty7: r.qty_7d, qty30: r.qty_30d, asOf: r.as_of_date }));

    // 確定 (finance fact)。期間 = 全モール fact の最新日から days 日
    // 監査PR-8: 6テーブル個別MAXを v_mall_finance_daily_unified 1本に集約
    const maxDate = db.prepare('SELECT MAX(date_jst) d FROM v_mall_finance_daily_unified').get()?.d || null;
    const finance = { available: !!maxDate, days, start: null, end: null, rows: [] };
    if (maxDate) {
      const end = maxDate;
      const start = new Date(Date.parse(end + 'T00:00:00Z') - (days - 1) * 86400000).toISOString().slice(0, 10);
      finance.start = start; finance.end = end;
      const acc = new Map(); // mallKey -> pieces
      const add = (k, pieces) => acc.set(k, (acc.get(k) || 0) + pieces);
      // このNEコード自身 + このNEコードを構成品に含むセット商品 (qty=構成数量)
      const keyQty = new Map([[code.toLowerCase().trim(), 1]]);
      for (const r of db.prepare('SELECT セット商品コード sc, 数量 q FROM mirror_set_components WHERE LOWER(TRIM(構成商品コード)) = LOWER(TRIM(?))').all(code)) {
        keyQty.set(String(r.sc).toLowerCase().trim(), r.q || 1);
      }
      const keys = [...keyQty.keys()];
      const ph = keys.map(() => '?').join(',');
      // 監査PR-8: 5モール個別クエリ → v_mall_finance_daily_unified 1クエリ
      // (view の ne_code/sku_key は LOWER(TRIM) 済み = 従来クエリと述語同一)
      const rows = db.prepare(`
        SELECT mall, ne_code ne, SUM(units_net_sold) u
        FROM v_mall_finance_daily_unified
        WHERE mall <> 'amazon' AND date_jst BETWEEN ? AND ? AND ne_code IN (${ph})
        GROUP BY mall, ne_code
      `).all(start, end, ...keys);
      for (const r of rows) add(r.mall, (r.u || 0) * (keyQty.get(r.ne) || 1));
      // Amazon: seller_sku を構成解決 (mirror_sku_resolved)。FBA/FBM は行(日)単位の手数料発生有無で推定
      // (SKU単位で合算してから判定すると期間内にFBA/FBM混在したSKUが全量FBAに寄る、Codex P7-1)
      const amzKeys = db.prepare('SELECT LOWER(TRIM(seller_sku)) k, quantity q FROM mirror_sku_resolved WHERE LOWER(TRIM(ne_code)) = LOWER(TRIM(?))').all(code);
      if (amzKeys.length) {
        const qBySku = new Map(amzKeys.map(r => [r.k, r.q || 1]));
        const keys2 = [...qBySku.keys()];
        const ph2 = keys2.map(() => '?').join(',');
        const rows2 = db.prepare(`
          SELECT sku_key k, is_fba isFba, SUM(units_net_sold) u
          FROM v_mall_finance_daily_unified
          WHERE mall = 'amazon' AND date_jst BETWEEN ? AND ? AND sku_key IN (${ph2})
          GROUP BY sku_key, is_fba
        `).all(start, end, ...keys2);
        for (const r of rows2) add(r.isFba ? 'amazon_fba' : 'amazon_fbm', (r.u || 0) * (qBySku.get(r.k) || 1));
      }
      finance.rows = [...acc.entries()]
        .map(([mall, pieces]) => ({ mall, label: loadDimMall(db).labelOf(mall), pieces: Math.round(pieces * 10) / 10 }))
        .filter(r => r.pieces > 0)
        .sort((a, b) => b.pieces - a.pieces);
    }
    res.json({ ok: true, code, sokuho: { rows: sokuho, asOf: sokuho.length ? sokuho[0].asOf : null }, finance });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ═══════════════════════════ 画面 ═══════════════════════════

const CSS = `
  :root {
    --bg: #eef1f6; --card: #ffffff; --ink: #1c2430; --sub: #526070; --line: #e6eaf0;
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
  .wrap { padding: 20px 22px 130px; max-width: 1560px; margin: 0 auto; }
  /* 下部固定の発注バー (カート廃止に伴い集計・条件・確定をここへ) */
  .foot { position: fixed; left: 0; right: 0; bottom: 0; z-index: 60; }
  .fbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; background: linear-gradient(90deg,#111c33,#1e3a63); color: #fff; padding: 10px 22px; box-shadow: 0 -4px 16px rgba(0,0,0,.22); }
  .fbar button { white-space: nowrap; }
  .fbar input { flex: 1 1 160px; }
  @media (max-width: 760px) { .fbar { padding: 8px 12px; gap: 8px; } .fbar input { order: 9; flex-basis: 100%; } }
  .fbar .ftot { font-size: 13px; white-space: nowrap; font-variant-numeric: tabular-nums; }
  .fbar .ftot b { font-size: 17px; }
  .fbar input { flex: 1; min-width: 100px; background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.25); color: #fff; border-radius: 8px; padding: 7px 10px; }
  .fbar input::placeholder { color: #9fb3d4; }
  .fghost { background: rgba(255,255,255,.1); color: #fff; border: 1px solid rgba(255,255,255,.28); border-radius: 8px; padding: 7px 12px; cursor: pointer; font-size: 13px; }
  .fghost:hover { background: rgba(255,255,255,.2); }
  .fghost .b-warn { margin-left: 2px; }
  .fpanel { background: var(--card); border-top: 1px solid var(--line); max-height: 45vh; overflow: auto; padding: 12px 22px; box-shadow: 0 -6px 20px rgba(0,0,0,.12); }
  h2.page { font-size: 19px; margin: 4px 0 16px; font-weight: 700; }
  .warn { background: var(--warn-soft); border: 1px solid #f0d089; color: var(--warnc); padding: 10px 14px; border-radius: 10px; margin-bottom: 14px; font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(268px, 1fr)); gap: 14px; }
  .card { position: relative; background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 16px 16px 14px; display: block; text-decoration: none; color: inherit; box-shadow: var(--shadow); transition: transform .08s, box-shadow .12s, border-color .12s; }
  .card .cdis { position: absolute; top: 8px; right: 8px; border: none; background: none; color: #9aa7b5; padding: 2px 7px; font-size: 13px; cursor: pointer; border-radius: 7px; }
  .card .cdis:hover { background: var(--danger-soft); color: var(--danger); }
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
  table.t { border-collapse: separate; border-spacing: 0; width: 100%; background: var(--card); font-size: 13.5px; }
  table.t th, table.t td { border-bottom: 1px solid var(--line); padding: 9px 10px; text-align: left; line-height: 1.5; }
  /* top:0 = スクロール領域 (.bd) の上端に貼り付く */
  table.t th { background: #f7f9fc; color: var(--sub); font-weight: 600; position: sticky; top: 0; white-space: nowrap; font-size: 12.5px; letter-spacing: .02em; z-index: 2; box-shadow: 0 1px 0 var(--line); }
  /* グループ見出し行も見出しのすぐ下に貼り付く (どのグループを見ているか迷子にならない) */
  tr.ghead td { position: sticky; top: 34px; z-index: 1; }
  table.t tbody tr:hover td { background: #f9fbff; }
  table.t td.r, table.t th.r { text-align: right; font-variant-numeric: tabular-nums; }
  /* 見出し追随: ページ全体のstickyはヘッダ構成やブラウザ差で不安定だったため、
     各セクションの中身(.bd)自体をスクロール領域にし、その中で th を固定する (確実に効く方式) */
  .sec { background: var(--card); border: 1px solid var(--line); border-radius: 14px; margin-bottom: 16px; overflow: visible; box-shadow: var(--shadow); }
  .sec > h2:first-child { border-radius: 13px 13px 0 0; }
  .sec > .bd { max-height: 74vh; overflow: auto; }
  .sec > .bd > table.t tr:last-child td { border-bottom: none; }
  .sec > h2 { font-size: 14px; margin: 0; padding: 12px 16px; background: #f7f9fc; border-bottom: 1px solid var(--line); user-select: none; font-weight: 700; }
  .sec > h2[data-sec] { cursor: pointer; }
  .sec .bd { padding: 14px 16px; overflow-x: auto; }
  button { font: inherit; font-size: 13px; border-radius: 9px; border: 1px solid #cbd5e1; background: #fff; padding: 8px 15px; cursor: pointer; color: var(--ink); transition: background .1s, border-color .1s, box-shadow .1s; font-weight: 500; }
  button:hover { background: #f1f5f9; border-color: #94a3b8; }
  button.pri { background: var(--accent); border-color: var(--accent); color: #fff; box-shadow: 0 1px 2px rgba(37,99,235,.3); }
  button.pri:hover { background: var(--accent-d); border-color: var(--accent-d); }
  button.ok { background: var(--ok); border-color: var(--ok); color: #fff; }
  button.ok:hover { filter: brightness(.94); }
  button.ghost, a.ghost { border: none; background: none; color: var(--accent); padding: 3px 6px; font-weight: 500; text-decoration: none; display: inline-block; }
  button.ghost:hover, a.ghost:hover { background: var(--accent-soft); }
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
  /* インラインゲージ (グループ見出し / アコーディオン / カート条件チェック用) */
  .gg { font-size: 13px; display: inline-flex; align-items: center; gap: 8px; }
  .gg .bar { width: 140px; height: 8px; border-radius: 999px; background: #e8edf3; overflow: hidden; display: inline-block; vertical-align: middle; flex: none; }
  .gg .bar > span { display: block; height: 100%; background: linear-gradient(90deg,#f0a94b,#e0872b); border-radius: 999px; transition: width .3s; }
  .gg.met .bar > span { background: linear-gradient(90deg,#4ade80,#16a34a); }
  .gg.over .bar > span { background: linear-gradient(90deg,#f87171,#dc2626); }
  .gg.over { color: var(--danger); }
  tr.ghead td { background: var(--accent-soft) !important; border-top: 2px solid #cfe0fb; padding: 9px 10px; }
  tr.ghead .gname { font-weight: 700; font-size: 13px; }
  tr.ghead .ggwrap { float: right; }
  a.pname { color: var(--accent); cursor: pointer; text-decoration: none; border-bottom: 1px dashed #b9d0ff; }
  a.pname:hover { color: var(--accent-d); border-bottom-style: solid; }
  a.copyv { color: inherit; cursor: copy; text-decoration: none; border-bottom: 1px dotted #c4cede; }
  a.copyv:hover { color: var(--accent-d); background: var(--accent-soft); }
  a.copyq { cursor: copy; margin-left: 5px; font-size: 12px; opacity: .5; text-decoration: none; }
  a.copyq:hover { opacity: 1; }
  tr.accrow > td { background: #fbfcff !important; padding: 0 10px 12px !important; }
  .accbox { border-left: 3px solid var(--accent); padding: 10px 14px; margin-top: 8px; background: #fff; border-radius: 0 10px 10px 0; box-shadow: var(--shadow); }
  .accbox .kv { display: flex; flex-wrap: wrap; gap: 4px 22px; font-size: 13px; margin-bottom: 8px; }
  .accbox .kv > span { color: var(--sub); }
  .accbox .kv b { color: var(--ink); font-variant-numeric: tabular-nums; margin-left: 4px; }
  .accbox .accg { margin-top: 10px; padding-top: 8px; border-top: 1px dashed var(--line); font-size: 13.5px; }
  table.t.sub { margin-top: 6px; font-size: 13px; }
  table.t.sub th { position: static; }
  a.need { color: var(--accent); cursor: pointer; font-variant-numeric: tabular-nums; text-decoration: none; border-bottom: 1px dashed #b9d0ff; }
  a.need:hover { color: var(--accent-d); border-bottom-style: solid; }
  button.needAll { font-size: 12.5px; padding: 5px 12px; border-radius: 8px; border: 1px solid #b9d0ff; background: var(--accent-soft); color: var(--accent-d); cursor: pointer; }
  button.needAll:hover { background: #dcebff; }
  .gadd { font-size: 13px; }
  .gaddItem { padding: 5px 8px; border-bottom: 1px dashed var(--line); font-size: 13px; }
  .gaddItem a { color: var(--accent); cursor: pointer; font-weight: 600; text-decoration: none; }
  .condsum { border-top: 1px solid var(--line); margin-top: 10px; padding-top: 8px; font-size: 12px; }
  .condsum .cline { margin: 7px 0 0; }
  .condsum .cline .nm2 { display: block; color: var(--sub); margin-bottom: 2px; }
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
    ${tab('/apps/purchase-orders/backorders', '発注残', 'backorders')}
    ${tab('/apps/purchase-orders/inbound', '入庫消込', 'inbound')}
    ${tab('/apps/purchase-orders/products', '全商品情報', 'products')}
    ${tab('/apps/purchase-orders/orders', '発注履歴', 'orders')}
    ${tab('/apps/purchase-orders/admin', 'マスタ管理', 'admin')}
  </nav>
  <a href="/" class="back sp">← ポータルに戻る</a>
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
function dlCsv(filename, rows) {
  var csv = rows.map(function(r) {
    return r.map(function(v) {
      var s = String(v == null ? '' : v);
      // CSV injection対策: 数式に化ける先頭文字は ' を前置 (文字列セルのみ)
      if (typeof v === 'string' && /^[=+\\-@\\t\\r]/.test(s)) s = "'" + s;
      return '"' + s.replace(/"/g, '""') + '"';
    }).join(',');
  }).join('\\r\\n');
  var blob = new Blob([new Uint8Array([0xEF, 0xBB, 0xBF]), csv], { type: 'text/csv' }); // BOM付きUTF-8 (Excel対応)
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = String(filename).replace(/[\\\\/:*?"<>|\\x00-\\x1f]/g, '_'); // ファイル名に使えない文字を無害化
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(a.href);
}
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
    const cycleIssued = new Map(cycleIssuedSuppliers(cycleStartIso(pub)).map(r => [r.code, r]));
    const cards = [];
    const others = [];
    for (const g of bySupplier.values()) {
      const ci = cycleIssued.get(g.code);
      const c = {
        code: g.code, name: g.name || ('仕入先 ' + g.code), memo: g.memo,
        targetCount: g.targets.length, estAmount: g.estAmount, hasDraft: draftCodes.has(g.code),
        issuedCount: ci ? ci.orders : 0, unsentCount: ci ? ci.unsent : 0,
        productCount: g.targets.length + g.candidates.length + g.horikoshi.length,
      };
      if (c.targetCount > 0 || c.hasDraft || c.issuedCount) cards.push(c); else others.push(c);
    }
    cards.sort((a, b) => b.targetCount - a.targetCount || b.estAmount - a.estAmount);
    others.sort((a, b) => b.productCount - a.productCount);
    const searchIndex = products.filter(p => p.active).map(p => ({ c: p.code, n: p.name, s: p.supplierCode }));
    const boSummary = listBackorders().summary;
    data = { pub, overlay, cards, others, searchIndex, boSummary, hiddenCodes: readHiddenSuppliers(), cycleMarker: getSetting('po_cycle_reset_at') || '' };
  } catch (e) { return res.status(500).send('error: ' + he(e.message)); }

  const { pub, overlay, cards, others, searchIndex, boSummary, hiddenCodes, cycleMarker } = data;
  // 発注サイクルの経過表示: ✅発注確定済み・×非表示はデータ更新ボタンまで保持される。長く放置したら注意を出す
  let cycleNote = '';
  if (cycleMarker) {
    const ms = Date.parse(cycleMarker);
    if (Number.isFinite(ms)) {
      const jst = new Date(ms + 9 * 3600000);
      const label = `${jst.getUTCMonth() + 1}/${jst.getUTCDate()} ${String(jst.getUTCHours()).padStart(2, '0')}:${String(jst.getUTCMinutes()).padStart(2, '0')}`;
      const days = Math.floor((Date.now() - ms) / 86400000);
      cycleNote = `🗓 発注サイクル: ${label} 開始 (✅発注確定済み・×非表示はデータ更新まで保持)` +
        (days >= 3 ? ` <span style="color:var(--danger)">⚠️ 前回のデータ更新から${days}日経過</span>` : '');
    }
  }
  const stale = pub && pub.as_of_date ? (Date.now() - Date.parse(pub.as_of_date + 'T00:00:00+09:00')) > 3 * 86400000 : true;
  const freshNote = pub ? freshnessText(pub, overlay) : 'PML未同期';
  const cardHtml = c => `
    <a class="card" data-sup="${he(c.code)}" data-supname="${he(c.name)}" href="/apps/purchase-orders/supplier/${encodeURIComponent(c.code)}">
      <button class="cdis" data-cdis="${he(c.code)}" title="この仕入先を非表示 (下の「非表示の仕入先」から戻せます)">✕</button>
      <div class="nm">${he(c.name)} ${c.hasDraft ? '<span class="badge b-draft">下書きあり</span>' : ''}${c.issuedCount ? ` <span class="badge b-issued" title="今サイクルで発注確定済み。FBA在庫更新やNE CSV取込 (データ更新) でこの表示はリセットされます">✅ 発注確定済み${c.issuedCount > 1 ? ' ×' + c.issuedCount : ''}</span>` : ''}${c.unsentCount ? ' <span class="badge b-warn" title="発注確定済みですが発注書メール (本送信) が未送信です">📧 メール未送信</span>' : ''}</div>
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
      ${cycleNote ? `<span class="muted" style="margin-left:12px">${cycleNote}</span>` : ''}
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
    ${boSummary.openOrders > 0 ? `
    <a href="/apps/purchase-orders/backorders" class="sec" style="display:block;text-decoration:none;padding:10px 14px;margin-bottom:4px">
      📦 <b>発注残 ${boSummary.openOrders}件</b>
      <span class="muted">残数量 ${boSummary.remainingQty.toLocaleString('ja-JP')} / 既知単価分 ¥${boSummary.knownAmount.toLocaleString('ja-JP')}${boSummary.unknownCostItems ? ` (単価未設定 ${boSummary.unknownCostItems}明細)` : ''}</span>
      ${boSummary.overdueItems ? `<span class="badge b-issued">🔴 納期超過 ${boSummary.overdueItems}明細</span>` : ''}
      ${boSummary.attentionItems ? `<span class="badge b-draft">⚠️ 要対応 ${boSummary.attentionItems}明細</span>` : ''}
      →
    </a>` : ''}
    <h2 class="page" style="font-size:16px">発注が必要な仕入先 <span class="muted">(<span id="cntSup">${cards.length}</span>)</span></h2>
    <div class="cards" id="mainCards">${cards.map(cardHtml).join('')}</div>
    <div id="dashDis" style="margin-top:12px"></div>
    <details style="margin-top:18px"><summary class="muted" style="cursor:pointer">その他の仕入先 (${others.length}) — 要発注なし</summary>
      <div class="cards" style="margin-top:10px">${others.map(cardHtml).join('')}</div>
    </details>`;
  const script = `
var IDX = ${jsonEmbed(searchIndex)};
// 仕入先カードの非表示 (サーバ保存=会社PC/自宅PCで共有。データ更新ボタンでサイクルが進むまで保持。
// 旧localStorage方式は営業日で勝手にリセット+PC間で共有されず廃止、中原さん要望 2026-07-13)
var HID = {};
${jsonEmbed(hiddenCodes)}.forEach(function(c){ HID[c] = 1; });
// サーバ保存に成功してから画面へ反映する (失敗時に見た目と保存状態がズレない)
function postHid(body, apply) {
  fetch('/apps/purchase-orders/api/dashboard/hidden', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    apply();
    applyHid();
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
}
function applyHid() {
  var hidden = [], visMain = 0;
  document.querySelectorAll('a.card[data-sup]').forEach(function(card) {
    var code = card.getAttribute('data-sup');
    var hide = !!HID[code];
    card.style.display = hide ? 'none' : '';
    if (hide) hidden.push({ code: code, name: card.getAttribute('data-supname') || code });
    else if (card.parentNode && card.parentNode.id === 'mainCards') visMain++;
  });
  var cnt = document.getElementById('cntSup');
  if (cnt) cnt.textContent = visMain;
  var seen = {};
  hidden = hidden.filter(function(x){ if (seen[x.code]) return false; seen[x.code] = 1; return true; });
  document.getElementById('dashDis').innerHTML = hidden.length
    ? '<span class="muted">🗑 非表示の仕入先 ' + hidden.length + ' 件:</span> ' +
      hidden.map(function(x) {
        return '<span class="badge" style="background:#f1f5f9;color:#334155;margin:2px 4px 2px 0">' + esc(x.name) +
          ' <a data-cundis="' + esc(x.code) + '" style="cursor:pointer;color:var(--accent)">↩戻す</a></span>';
      }).join('') +
      ' <a data-cundis="*" style="cursor:pointer;color:var(--accent);font-size:12px">全て戻す</a>'
    : '';
}
document.addEventListener('click', function(ev) {
  var cd = ev.target.getAttribute && ev.target.getAttribute('data-cdis');
  if (cd) {
    ev.preventDefault(); ev.stopPropagation(); // カード(リンク)への遷移を止める
    postHid({ code: cd, hidden: true }, function() {
      HID[cd] = 1;
      toast('非表示にしました (「非表示の仕入先」から戻せます。次のデータ更新まで保持)');
    });
    return;
  }
  var ud = ev.target.getAttribute && ev.target.getAttribute('data-cundis');
  if (ud) {
    if (ud === '*') postHid({ clear: true }, function(){ HID = {}; });
    else postHid({ code: ud, hidden: false }, function(){ delete HID[ud]; });
    return;
  }
});
applyHid();
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
// データ更新前の二段確認: (1)「✅発注確定済み」表示のリセット確認 → (2) 発注メール未送信の仕入先があれば追加確認
function confirmCycleReset(next) {
  fetch('/apps/purchase-orders/api/cycle-issued')
    .then(function(r){ return r.json(); })
    .then(function(j) {
      // fail-closed: 発注確定状況が確認できないときは更新を開始しない (Codex サイクルR1 Medium)
      if (!j || !j.ok) { alert('発注確定状況を確認できませんでした (' + ((j && j.error) || '不明なエラー') + ')。データ更新を中止しました。再試行してください'); return; }
      var sup = j.suppliers || [];
      var msg = sup.length
        ? 'データを更新すると、「✅発注確定済み」の表示がリセットされます。\\n対象: ' + sup.map(function(s){ return s.name; }).join('、') + '\\n※発注そのものは発注残・発注履歴に残ります。\\n\\n更新しますか?'
        : 'データを更新しますか?';
      if (!confirm(msg)) return;
      var unsent = sup.filter(function(s){ return s.unsent > 0; });
      if (unsent.length) {
        var lines = unsent.map(function(s){ return '・' + s.name + ' (' + s.unsentPoNumbers.join(', ') + ')'; }).join('\\n');
        if (!confirm('⚠️ 以下の仕入先は発注確定済みですが、発注書メールがまだ送信されていません:\\n\\n' + lines + '\\n\\nこのまま発注確定表示をリセットしてよろしいですか?')) return;
      }
      next();
    })
    .catch(function(e){ alert('通信エラー: ' + e.message + ' — 発注確定状況を確認できないため、データ更新を中止しました。再試行してください'); });
}
var btnFba = document.getElementById('btnFba');
btnFba.addEventListener('click', function() {
  confirmCycleReset(function() {
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
  confirmCycleReset(function() {
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
});
var btnNeClear = document.getElementById('btnNeClear');
if (btnNeClear) btnNeClear.addEventListener('click', function() {
  if (!confirm('手動CSVの上書きを解除して、朝同期の値に戻しますか?')) return;
  confirmCycleReset(function() {
    fetch('/apps/purchase-orders/api/ne-overlay', { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(j){ if (j.ok) location.reload(); else alert(j.error); });
  });
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
      <input type="text" id="pageQ" placeholder="🔍 商品コード / 商品名で絞り込み" style="margin-left:auto;min-width:240px">
    </div>
    <div class="toolbar" style="margin-top:6px">
      <button data-act="save">💾 下書き保存</button>
      <button class="pri" data-act="issue">✅ 発注確定</button>
      <span class="fsavedInd muted"></span>
    </div>
    <div class="sec"><h2 data-sec="targets">🔴 要発注 (<span id="cntTargets"></span>) — 発注金額の大きいグループ順。<span class="muted" style="font-weight:400">商品名クリックで詳細・◯ヶ月分計算・同グループ商品</span></h2><div class="bd" id="secTargets"></div></div>
    <div class="sec"><h2 data-sec="cands">🟡 追加発注候補 (<span id="cntCands"></span>) — 条件充足・同梱用、在庫月数が少ない順</h2><div class="bd" id="secCands"></div></div>
    <div class="sec"><h2 data-sec="hori">⚪ 掘り起こし (<span id="cntHori"></span>) — 在庫ゼロ / 販売実績なし</h2><div class="bd" id="secHori" style="display:none"></div></div>
    <div class="sec" id="doneArea" style="display:none"><h2>✅ 発注リスト (コピーして NE 登録 / メール / FAX 原稿に)</h2><div class="bd" id="doneBody"></div></div>
    <div class="foot">
      <div id="panelItems" class="fpanel" style="display:none"></div>
      <div id="panelConds" class="fpanel" style="display:none"></div>
      <div class="fbar">
        <button id="btnItems" class="fghost">📋 発注内容</button>
        <span class="ftot" id="fTot">—</span>
        <button id="btnConds" class="fghost">📏 条件 <span id="fCond"></span></button>
        <input type="text" id="orderNote" placeholder="メモ (任意)">
        <button data-act="save" id="btnSave">💾 下書き保存</button>
        <button class="pri" data-act="issue" id="btnIssue">✅ 発注確定</button>
        <span class="fsavedInd muted" id="fSaved"></span>
      </div>
    </div>`;

  const script = `
var D = ${jsonEmbed(data)};
var CART = {}; // code -> qty
var DATES = {}; // code -> 希望納期 'YYYY-MM-DD' (未指定はキーなし=納期なし)
var byCode = {};
[].concat(D.targets, D.candidates, D.horikoshi, D.draftExtras || []).forEach(function(p){ byCode[p.code] = p; });
var condById = {}; D.conditions.forEach(function(c){ condById[c.conditionId] = c; });
var matById = {}; D.materialGroups.forEach(function(m){ matById[m.groupId] = m; });

// ページ内検索語 (小文字)
var Q = '';
// 非表示にした要発注行 (code → 非表示時の発注数)。localStorage に当日データ(as_of)単位で保持 = 翌朝自動リセット
var DIS = new Map();
var DIS_KEY = 'po_dis:' + D.supplier.code + ':' + (D.pub ? D.pub.as_of_date : '');
try {
  (JSON.parse(localStorage.getItem(DIS_KEY) || '[]')).forEach(function(e){ DIS.set(e.code, e.qty || 0); });
} catch (e) {}
function saveDis() {
  try {
    var arr = [];
    DIS.forEach(function(qty, code){ arr.push({ code: code, qty: qty }); });
    localStorage.setItem(DIS_KEY, JSON.stringify(arr));
  } catch (e) {}
}

// 初期カート: draft があれば復元、なければ要発注全件を推奨量で (非表示分は除く)。
// 下書き明細に入っている商品は明示的に保存された発注なので、非表示より優先して復活させる (Codex P8-3)
if (D.draft && D.draft.items.length) {
  var disChanged = false;
  D.draft.items.forEach(function(it) {
    if (byCode[it.code]) {
      CART[it.code] = it.qty;
      if (it.requestedDate) DATES[it.code] = it.requestedDate;
    }
    if (DIS.has(it.code)) { DIS.delete(it.code); disChanged = true; }
  });
  if (disChanged) saveDis();
} else {
  D.targets.forEach(function(p){ if (p.recQty && !p.recentIssued && !DIS.has(p.code)) CART[p.code] = p.recQty; });
}

function months(p){ return p.stockMonths == null ? '—' : (Math.round(p.stockMonths * 100) / 100).toFixed(2); }
function issuedBadge(p){
  if (!p.recentIssued) return '';
  return ' <span class="badge b-issued" title="直近14日以内に発注確定済み (数量は台帳注残として在庫+注残に反映済み)">発注済 ' + esc(String(p.recentIssued.issuedAt).slice(0,10)) + ' ×' + p.recentIssued.qty + '</span>';
}
function qtyCell(p) {
  var v = CART[p.code] || '';
  var step = p.lot > 0 ? p.lot : 1;
  return '<input type="number" min="0" step="' + step + '" data-code="' + esc(p.code) + '" value="' + v + '">';
}
function dateCell(p) {
  return '<input type="date" data-date="' + esc(p.code) + '" value="' + esc(DATES[p.code] || '') + '" style="width:135px" title="この商品の希望納期 (空=指定なし)。仕入先への発注メールに載ります">';
}
function selBadge(p) {
  if (!p.selectableLow) return '';
  return ' <span class="badge" style="background:#ede9fe;color:#6d28d9" title="選べるセット構成商品 (' + esc(p.selectableLow.sets || '') + ')。在庫+注残が最低在庫 ' + p.selectableLow.minStock + ' 以下">🧩選べるセット構成の在庫減</span>';
}
// 要発注リストに「発注数を入れたから並んでいる」行 (本来は追加候補/掘り起こし)。renderTargetsで再構築
var ADDED = {};
function addedBadge(p) {
  if (!ADDED[p.code]) return '';
  return ' <span class="badge" style="background:var(--accent-soft);color:var(--accent-d)" title="要発注判定ではないが発注数が入っているためこのグループに表示中 (発注数を消すと戻ります)">＋追加</span>';
}
function rowHtml(p, kind) {
  var rec = p.recQty ? p.recQty.toLocaleString('ja-JP') : '—';
  return '<tr>' +
    '<td><a class="copyv" data-copy="' + esc(p.code) + '" title="クリックで商品コードをコピー">' + esc(p.code) + '</a>' + issuedBadge(p) + selBadge(p) + addedBadge(p) + '</td>' +
    '<td><a class="pname" data-acc="' + esc(p.code) + '" title="クリックで詳細・発注条件・同グループ商品">' + esc(p.name) + '</a></td>' +
    '<td class="r">' + months(p) + '</td>' +
    '<td class="r">' + p.sales30.toLocaleString('ja-JP') + '</td>' +
    '<td class="r">' + (p.stock + p.backOrder).toLocaleString('ja-JP') + (p.backOrder ? ' <span class="muted">(注残' + p.backOrder.toLocaleString('ja-JP') + ')</span>' : '') + '</td>' +
    '<td class="r">' + (p.lot || '—') + '</td>' +
    '<td class="r">' + (kind === 'hori' ? esc(p.lastPurchase || '—') : rec) + '</td>' +
    '<td class="r">' + (p.cost ? yen(p.cost) : '—') + '</td>' +
    '<td style="white-space:nowrap">' + qtyCell(p) + '<a class="copyq" data-copyq="' + esc(p.code) + '" title="クリックで発注数をコピー">📋</a></td>' +
    '<td>' + dateCell(p) + '</td>' +
    (kind === 'tgt' ? '<td><button class="ghost" data-dis="' + esc(p.code) + '" title="このリストから非表示 (下の「非表示」から戻せます)">✕</button></td>' : '') +
    '</tr>';
}
function matchQ(p) {
  return !Q || (p.code + ' ' + p.name).toLowerCase().indexOf(Q) >= 0;
}
// 検索なし時の描画上限 (DOMを軽くしてページ無応答を防ぐ。検索時は全件から探す)
var LIST_CAP = 150;
function tableHtml(list, kind) {
  var data = list.filter(matchQ);
  if (!data.length) return '<div class="muted">' + (Q ? '「' + esc(Q) + '」に一致なし' : 'なし') + '</div>';
  var capped = !Q && data.length > LIST_CAP;
  var rows = capped ? data.slice(0, LIST_CAP) : data;
  var h7 = kind === 'hori' ? '最終仕入日' : '推奨発注';
  var h = '<table class="t"><tr><th>商品コード</th><th>商品名</th><th class="r">在庫月数</th><th class="r">30日販売</th><th class="r">在庫+注残</th><th class="r">ロット</th><th class="r">' + h7 + '</th><th class="r">原価</th><th>発注数</th><th>希望納期</th></tr>';
  rows.forEach(function(p){ h += rowHtml(p, kind); });
  h += '</table>';
  if (capped) h += '<div class="muted" style="padding:8px 12px">先頭 ' + LIST_CAP + ' 件を表示 (全 ' + data.length + ' 件)。上の🔍検索で全件から絞り込めます</div>';
  return h;
}
// ── 要発注: 発注条件グループ/原料グループごとにまとめ、グループ計(推奨発注×原価)の大きい順 ──
function groupKeyOf(p){ return p.conditionId ? 'c:' + p.conditionId : (p.materialGroupId ? 'm:' + p.materialGroupId : ''); }
function groupName(k) {
  if (k.slice(0, 2) === 'c:') { var c = condById[k.slice(2)]; return c ? c.displayName + (c.makerName ? ' (' + c.makerName + ')' : '') : k.slice(2) + ' (条件未登録)'; }
  if (k.slice(0, 2) === 'm:') { var m = matById[k.slice(2)]; return m ? m.name : k.slice(2) + ' (原料未登録)'; }
  return '';
}
function groupMembers(k) {
  if (k.slice(0, 2) === 'c:') { var c = condById[k.slice(2)]; return c ? c.memberCodes : []; }
  if (k.slice(0, 2) === 'm:') { var m = matById[k.slice(2)]; return m ? m.memberCodes : []; }
  return [];
}
function renderTargets() {
  var visible = D.targets.filter(function(p){ return !DIS.has(p.code) && matchQ(p); });
  // 要発注判定外でも発注数が入っている商品は、所属グループの下に「＋追加」行として並べる
  // (アコーディオンで数量を決めて閉じたあとも、要発注リスト上でグループごとに見える)
  ADDED = {};
  var inTargets = {};
  visible.forEach(function(p){ inTargets[p.code] = 1; });
  Object.keys(CART).forEach(function(code) {
    if (!(CART[code] > 0) || inTargets[code] || DIS.has(code)) return;
    var p = byCode[code];
    if (!p || p.isTarget || p.selectableLow || p.extra) return;
    if (!matchQ(p)) return;
    ADDED[code] = 1;
    visible.push(p);
  });
  document.getElementById('cntTargets').textContent = visible.length;
  var h = '';
  if (!visible.length) h = '<div class="muted">' + (Q ? '「' + esc(Q) + '」に一致なし' : 'なし') + '</div>';
  else {
    var buckets = {}, order = [];
    visible.forEach(function(p) {
      var k = groupKeyOf(p) || 'solo:' + p.code;
      if (!buckets[k]) { buckets[k] = { key: k, items: [], amt: 0 }; order.push(k); }
      buckets[k].items.push(p);
      buckets[k].amt += (ADDED[p.code] ? (CART[p.code] || 0) : (p.recQty || 0)) * (p.cost || 0);
    });
    var list = order.map(function(k){ return buckets[k]; });
    list.sort(function(a, b){ return b.amt - a.amt; });
    h = '<table class="t"><tr><th>商品コード</th><th>商品名</th><th class="r">在庫月数</th><th class="r">30日販売</th><th class="r">在庫+注残</th><th class="r">ロット</th><th class="r">推奨発注</th><th class="r">原価</th><th>発注数</th><th>希望納期</th><th></th></tr>';
    list.forEach(function(b) {
      b.items.sort(function(x, y){ return ((y.recQty || 0) * (y.cost || 0)) - ((x.recQty || 0) * (x.cost || 0)); });
      if (b.key.slice(0, 5) !== 'solo:') {
        h += '<tr class="ghead"><td colspan="11"><span class="gname">' + (b.key[0] === 'm' ? '🧪 原料: ' : '📦 ') + esc(groupName(b.key)) + '</span>' +
          ' <span class="muted">' + b.items.length + '商品 / 推奨計 ' + yen(b.amt) + '</span>' +
          ' <button class="ghost" data-gdis="' + esc(b.key) + '" title="このグループの商品をまとめて非表示 (下の「非表示」から戻せます)">✕ グループごと非表示</button>' +
          '<span class="ggwrap" data-gauge="' + esc(b.key) + '"></span></td></tr>';
      }
      b.items.forEach(function(p){ h += rowHtml(p, 'tgt'); });
    });
    h += '</table>';
  }
  // 非表示にした行の復元リスト
  if (DIS.size) {
    h += '<div style="padding:10px 14px;border-top:1px solid var(--line)"><span class="muted">🗑 非表示 ' + DIS.size + ' 件:</span> ';
    DIS.forEach(function(qty, code) {
      var p = byCode[code];
      h += '<span class="badge" style="background:#f1f5f9;color:#334155;margin:2px 4px 2px 0">' + esc(code) + (p ? ' ' + esc(String(p.name).slice(0, 18)) : '') +
        ' <a data-undis="' + esc(code) + '" style="cursor:pointer;color:var(--accent)" title="リストに戻す">↩戻す</a></span>';
    });
    h += ' <a data-undis="*" style="cursor:pointer;color:var(--accent);font-size:12px">全て戻す</a></div>';
  }
  document.getElementById('secTargets').innerHTML = h;
}
function renderLists() {
  renderTargets(); // cntTargets は renderTargets 内で設定 (＋追加行を含むため)
  document.getElementById('secCands').innerHTML = tableHtml(D.candidates, 'cand');
  document.getElementById('secHori').innerHTML = tableHtml(D.horikoshi, 'hori');
  document.getElementById('cntCands').textContent = D.candidates.length;
  document.getElementById('cntHori').textContent = D.horikoshi.length;
}
function cartItems() {
  var items = [];
  Object.keys(CART).forEach(function(code){
    var q = CART[code];
    if (q > 0 && byCode[code]) items.push({ code: code, qty: q, cost: byCode[code].cost, key: code.toLowerCase(), requestedDate: DATES[code] || null });
  });
  return items;
}
// ── インラインゲージ (グループ見出し / アコーディオン / カート条件チェックで共通) ──
function barHtml(cur, req, label, remain) {
  var met = cur >= req;
  var pct = Math.min(100, req > 0 ? cur / req * 100 : 100);
  return '<span class="gg' + (met ? ' met' : '') + '">' + label + (met ? ' ✅' : ' — <b>' + remain + '</b>') +
    '<span class="bar"><span style="width:' + pct + '%"></span></span></span>';
}
// 今月(JST)発注確定済みの数量合計。上限=出荷制限は月次累計で判定し、毎月1日に自動リセットされる。
// 条件グループ別は発注時スナップショット (issuedByCond) を使う — 月中に紐付けを変えても過去分は動かない
function issuedMonthFor(cond) {
  if (cond.supplierWide) return D.issuedTotal || 0;
  return (D.issuedByCond && D.issuedByCond[cond.conditionId]) || 0;
}
// 上限ゲージ (出荷制限、今月確定済み+今回の累計)。上限以下=OK(緑)、超過=赤
function capBarHtml(cartQ, issuedQ, cap, unit) {
  var cur = cartQ + issuedQ;
  var over = cur > cap;
  var pct = Math.min(100, cap > 0 ? cur / cap * 100 : 100);
  var u = esc(unit || '個');
  return '<span class="gg' + (over ? ' over' : ' met') + '" title="今月(' + esc(D.monthLabel || '') + ')の発注確定分を含む累計。毎月1日にリセット">' +
    (issuedQ ? '今月済 ' + issuedQ.toLocaleString('ja-JP') + ' + 今回 ' + cartQ.toLocaleString('ja-JP') + ' = ' : '') +
    cur.toLocaleString('ja-JP') + ' / 上限 ' + cap.toLocaleString('ja-JP') + u + '/月' +
    (over ? ' — <b>⚠ ' + (cur - cap).toLocaleString('ja-JP') + u + ' 超過</b>' : ' ✅') +
    '<span class="bar"><span style="width:' + pct + '%"></span></span></span>';
}
function gaugeHtml(k) {
  var items = cartItems();
  if (k.slice(0, 2) === 'c:') {
    var c = condById[k.slice(2)];
    if (!c) return '<span class="badge b-warn">条件グループ未登録: ' + esc(k.slice(2)) + '</span>';
    var mem = {}; c.memberCodes.forEach(function(x){ mem[x] = 1; });
    var sumQ = 0, sumA = 0;
    items.forEach(function(i){ if (c.supplierWide || mem[i.code]) { sumQ += i.qty; sumA += i.qty * (i.cost || 0); } });
    if (c.conditionType === '金額')
      return barHtml(sumA, c.conditionValue, yen(sumA) + ' / ' + yen(c.conditionValue), 'あと ' + yen(Math.max(0, c.conditionValue - sumA)));
    if (c.conditionType === '数量' && (c.unit === '個' || !c.unit))
      return barHtml(sumQ, c.conditionValue, sumQ.toLocaleString('ja-JP') + ' / ' + c.conditionValue.toLocaleString('ja-JP') + ' 個', 'あと ' + Math.max(0, c.conditionValue - sumQ).toLocaleString('ja-JP') + ' 個');
    if (c.conditionType === '上限')
      return capBarHtml(sumQ, issuedMonthFor(c), c.conditionValue, c.unit);
    return '<span class="badge b-warn">条件 ' + esc(c.conditionType) + ' ' + c.conditionValue.toLocaleString('ja-JP') + esc(c.unit || '') + ' (手動確認)</span>' +
      ' <span class="muted">現在 ' + sumQ.toLocaleString('ja-JP') + '個 / ' + yen(sumA) + '</span>';
  }
  if (k.slice(0, 2) === 'm:') {
    var m = matById[k.slice(2)];
    if (!m) return '<span class="badge b-warn">原料グループ未登録: ' + esc(k.slice(2)) + '</span>';
    if (m.minOrderQty == null) return '<span class="muted">最低発注量 未設定</span>';
    var mem2 = {}; m.memberCodes.forEach(function(x){ mem2[x] = 1; });
    var sum = 0, noCap = 0;
    items.forEach(function(i) {
      var p = byCode[i.code];
      if (mem2[i.code] && p) {
        // 容量/個 未設定は 1個=1 として加算 (0のまま動かないより人間の期待に近い。件数は注記)
        sum += i.qty * (p.capacityPerUnit || 1);
        if (!p.capacityPerUnit) noCap++;
      }
    });
    return barHtml(sum, m.minOrderQty, sum.toLocaleString('ja-JP') + ' / ' + m.minOrderQty.toLocaleString('ja-JP') + esc(m.unit || ''),
      'あと ' + Math.max(0, m.minOrderQty - sum).toLocaleString('ja-JP') + esc(m.unit || '')) +
      (noCap ? ' <span class="badge b-warn" title="容量/個が未設定の商品は1個=1として計算しています (マスタ管理→商品紐付けで設定)">容量未設定' + noCap + '件=1換算</span>' : '');
  }
  return '';
}
function renderGauges() {
  document.querySelectorAll('[data-gauge]').forEach(function(el){ el.innerHTML = gaugeHtml(el.getAttribute('data-gauge')); });
}
// ── 商品名クリックで開く詳細アコーディオン ──
function kvHtml(label, val){ return '<span>' + label + '<b>' + val + '</b></span>'; }
function numFmt(v){ return v == null ? '—' : Number(v).toLocaleString('ja-JP'); }
// ◯ヶ月分保有に必要な数 = max(0, 30日販売×月数 − 在庫 − 注残)。
// ロットは無視した素の参照値 (中原さん指定)。実際の発注数はロットを見て人が決める
function needQty(p, m) {
  return Math.ceil(Math.max(0, (p.sales30 || 0) * m - ((p.stock || 0) + (p.backOrder || 0))));
}
function needCell(code) {
  return '<td class="r"><a class="need" data-nc="' + esc(code) + '" title="クリックで発注数に反映">—</a></td>';
}
function membersTable(k, selfCode) {
  // 自分自身も含めた全メンバーを表示 (★=この商品)
  var rows = groupMembers(k).map(function(c){ return byCode[c]; }).filter(Boolean);
  var h = '';
  if (!rows.length) h += '<div class="muted" style="margin-top:4px">この仕入先に同グループの商品はありません</div>';
  else {
    rows.sort(function(a, b) {
      if (a.code === selfCode) return -1;
      if (b.code === selfCode) return 1;
      return (b.sales30 || 0) - (a.sales30 || 0);
    });
    h += '<div style="margin-top:6px"><button class="needAll" title="このグループ全商品の「必要数」を発注数欄にまとめて入れる">📥 必要数を全て発注数へコピー</button></div>';
    h += '<table class="t sub"><tr><th>商品コード</th><th>商品名</th><th class="r">在庫月数</th><th class="r">30日販売</th><th class="r">在庫+注残</th><th class="r">ロット</th><th class="r">原価</th><th class="r">必要数</th><th>発注数</th></tr>';
    rows.forEach(function(p) {
      var isSelf = p.code === selfCode;
      h += '<tr' + (isSelf ? ' style="background:#f0f6ff"' : '') + '><td>' + (isSelf ? '★ ' : '') + '<a class="copyv" data-copy="' + esc(p.code) + '" title="クリックで商品コードをコピー">' + esc(p.code) + '</a>' + issuedBadge(p) + '</td><td>' + esc(p.name) + (isSelf ? ' <span class="muted">(この商品)</span>' : '') + '</td>' +
        '<td class="r">' + months(p) + '</td><td class="r">' + numFmt(p.sales30) + '</td>' +
        '<td class="r">' + numFmt((p.stock || 0) + (p.backOrder || 0)) + '</td>' +
        '<td class="r">' + (p.lot || '—') + '</td><td class="r">' + (p.cost ? yen(p.cost) : '—') + '</td>' +
        needCell(p.code) +
        '<td>' + qtyCell(p) + '</td></tr>';
    });
    h += '</table>';
  }
  // このグループに商品を追加 (リストを見て「あの商品も同じグループなのに入ってない」に画面内で対応)
  h += '<div class="gadd" style="margin-top:8px">➕ このグループに商品を追加: ' +
    '<input type="text" class="gaddQ" data-gkey="' + esc(k) + '" placeholder="商品コード / 商品名で検索" style="min-width:260px">' +
    '<div class="gaddR"></div></div>';
  return h;
}
function accHtml(p) {
  var h = '<div class="accbox">';
  h += '<div class="kv">' +
    kvHtml('在庫(引当なし) ', numFmt(p.stock)) + kvHtml('注残 ', numFmt(p.backOrder)) +
    kvHtml('7日販売 ', numFmt(p.sales7)) + kvHtml('30日販売 ', numFmt(p.sales30)) +
    kvHtml('在庫月数 ', months(p)) + kvHtml('推奨保有月数 ', p.holdMonths != null ? p.holdMonths : '—') +
    kvHtml('ロット ', p.lot || '—') + kvHtml('原価 ', p.cost ? yen(p.cost) : '—') + kvHtml('売価 ', p.price ? yen(p.price) : '—') +
    kvHtml('最終仕入日 ', esc(p.lastPurchase || '—')) +
    (p.capacityPerUnit ? kvHtml('容量/個 ', numFmt(p.capacityPerUnit)) : '') + '</div>';
  // ◯ヶ月分シミュレーション (旧GAS参照ツールの機能)。必要数は下の同グループ表の「必要数」列にも反映
  h += '<div class="accg">📐 <b><input type="number" class="simM" value="' + (p.holdMonths > 0 ? p.holdMonths : 2) + '" min="0.5" step="0.5" style="width:64px;text-align:right"> ヶ月分</b>を保有するのに必要な数: ' +
    '<a class="need" data-nc="' + esc(p.code) + '" title="クリックで発注数に反映" style="font-weight:700">—</a>' +
    ' <span class="muted">(30日販売 × 月数 − 在庫 − 注残。ロットは考慮しない参照値。クリックで発注数へ)</span></div>';
  var hasGroup = false;
  if (p.conditionId) {
    hasGroup = true;
    h += '<div class="accg">📦 発注条件グループ: <b>' + esc(groupName('c:' + p.conditionId)) + '</b> <span data-gauge="c:' + esc(p.conditionId) + '"></span>' +
      '<div class="muted" style="margin-top:6px">同グループの商品 — 発注数を入れるとまとめて条件充足に近づきます</div>' +
      membersTable('c:' + p.conditionId, p.code) + '</div>';
  }
  if (p.materialGroupId) {
    hasGroup = true;
    h += '<div class="accg">🧪 原料グループ: <b>' + esc(groupName('m:' + p.materialGroupId)) + '</b> <span data-gauge="m:' + esc(p.materialGroupId) + '"></span>' +
      '<div class="muted" style="margin-top:6px">同じ原料を使う商品 — 合わせて最低発注量(容量換算)を満たせます</div>' +
      membersTable('m:' + p.materialGroupId, p.code) + '</div>';
  }
  if (!hasGroup) h += '<div class="accg muted">発注条件グループ・原料グループ未設定 (単品発注)。下の「グループ紐付け」でその場で設定できます</div>';
  if (p.selectableLow) {
    h += '<div class="accg" style="color:#6d28d9">🧩 <b>選べるセット構成商品</b>: ' + esc(p.selectableLow.sets || '(セット名未設定)') +
      ' — 在庫+注残 ' + numFmt((p.stock || 0) + (p.backOrder || 0)) + ' が最低在庫 ' + p.selectableLow.minStock + ' 以下のため要発注に載せています (在庫を切らさない前提の商品)</div>';
  }
  // その場でグループ紐付けを変更 (保存先: 商品紐付けマスタ。容量/ケース設定は保持される)
  var condOpts = '<option value="">(なし)</option>' + (D.allGroups && D.allGroups.conditions || []).map(function(g) {
    return '<option value="' + esc(g.id) + '"' + (g.id === p.conditionId ? ' selected' : '') + '>' + esc(g.id) + ' — ' + esc(g.name) + (g.mine ? '' : ' (他仕入先)') + '</option>';
  }).join('');
  var matOpts = '<option value="">(なし)</option>' + (D.allGroups && D.allGroups.materials || []).map(function(g) {
    return '<option value="' + esc(g.id) + '"' + (g.id === p.materialGroupId ? ' selected' : '') + '>' + esc(g.id) + ' — ' + esc(g.name) + '</option>';
  }).join('');
  h += '<div class="accg">🔗 この商品の所属グループを変更: 発注条件 <select class="bindCond" style="max-width:260px">' + condOpts + '</select>' +
    ' 原料 <select class="bindMat" style="max-width:260px">' + matOpts + '</select>' +
    ' <button class="bindSave" data-bind="' + esc(p.code) + '">保存</button>' +
    ' <span class="muted">(グループに他の商品を足すのは各グループ表の下の「➕このグループに商品を追加」)</span></div>';
  h += '<div class="accg" style="display:flex;justify-content:flex-end;align-items:center;gap:10px">' +
    '<span class="muted">発注数を決めたら →</span>' +
    '<button class="pri accApply" data-apply="' + esc(p.code) + '">💾 要発注リストに反映して閉じる</button></div>';
  return h + '</div>';
}
// アコーディオン内の「必要数」列を月数入力に合わせて再計算
function updateSim(box) {
  if (!box) return;
  var mEl = box.querySelector('.simM');
  var m = mEl ? parseFloat(mEl.value) : NaN;
  box.querySelectorAll('a.need').forEach(function(a) {
    var p = byCode[a.getAttribute('data-nc')];
    if (!p || !(m > 0)) { a.textContent = '—'; a.removeAttribute('data-q'); return; }
    var q = needQty(p, m);
    if (q) { a.textContent = q.toLocaleString('ja-JP'); a.setAttribute('data-q', q); }
    else { a.textContent = '充足✓'; a.removeAttribute('data-q'); }
  });
}
// ── 下部固定バー (発注内容の集計・条件チェック・確定) ──
var openPanel = null; // 'items' | 'conds' | null
function renderPanelItems(items) {
  var h;
  if (!items.length) h = '<div class="muted">発注する商品がありません。各リストの「発注数」に数量を入れてください。</div>';
  else {
    h = '<table class="t"><tr><th>商品コード</th><th>商品名</th><th class="r">発注数</th><th class="r">金額(原価)</th><th>希望納期</th><th></th></tr>';
    items.forEach(function(i) {
      var p = byCode[i.code];
      var extraBadge = p.extra ? ' <span class="badge b-warn" title="現在の要発注/候補リストに出てこない商品 (下書き保存後にPMLが更新された等)。確定エラーになる場合は ✕ で外してください">リスト外</span>' : '';
      h += '<tr><td>' + esc(i.code) + '</td><td>' + esc(String(p.name || '').slice(0, 60)) + extraBadge + '</td>' +
        '<td class="r">' + i.qty.toLocaleString('ja-JP') + '</td><td class="r">' + (p.cost ? yen(i.qty * p.cost) : '—') + '</td>' +
        '<td>' + dateCell(p) + '</td>' +
        '<td><button class="ghost" data-del="' + esc(i.code) + '">✕</button></td></tr>';
    });
    h += '</table>';
  }
  document.getElementById('panelItems').innerHTML = h;
}
function renderPanelConds(items) {
  var inCart = {};
  items.forEach(function(i){ inCart[i.code] = 1; });
  var h = '';
  D.conditions.forEach(function(c) {
    // 仕入先全体条件 (商品紐付けなし) はカートに何か入っていれば常に表示、それ以外は発注内商品が関係する場合のみ
    var relevant = c.supplierWide ? items.length > 0 : c.memberCodes.some(function(x){ return inCart[x]; });
    if (!relevant) return;
    h += '<div class="cline"><span class="nm2">📦 ' + esc(c.displayName) + (c.supplierWide ? ' <span class="muted">(仕入先全体)</span>' : '') + '</span><span data-gauge="c:' + esc(c.conditionId) + '"></span></div>';
  });
  D.materialGroups.forEach(function(m) {
    if (m.minOrderQty == null) return;
    if (!m.memberCodes.some(function(x){ return inCart[x]; })) return;
    h += '<div class="cline"><span class="nm2">🧪 原料: ' + esc(m.name) + '</span><span data-gauge="m:' + esc(m.groupId) + '"></span></div>';
  });
  document.getElementById('panelConds').innerHTML = h ? '<div class="condsum" style="border-top:none;margin-top:0;padding-top:0"><b>📏 条件チェック</b> <span class="muted">(発注に入っている商品が関係する条件のみ)</span>' + h + '</div>'
    : '<div class="muted">発注中の商品に関係する発注条件はありません</div>';
}
// 条件の未達/手動確認リスト (確定前警告・バーのバッジで使用)
function condCheck() {
  var items = cartItems();
  var inCart = {};
  items.forEach(function(i){ inCart[i.code] = 1; });
  var unmet = [], manual = [];
  D.conditions.forEach(function(c) {
    var relevant = c.supplierWide ? items.length > 0 : c.memberCodes.some(function(x){ return inCart[x]; });
    if (!relevant) return;
    var mem = {}; c.memberCodes.forEach(function(x){ mem[x] = 1; });
    var sumQ = 0, sumA = 0;
    items.forEach(function(i){ if (c.supplierWide || mem[i.code]) { sumQ += i.qty; sumA += i.qty * (i.cost || 0); } });
    if (c.conditionType === '金額') {
      if (sumA < c.conditionValue) unmet.push(c.displayName + ': あと ' + yen(c.conditionValue - sumA));
    } else if (c.conditionType === '数量' && (c.unit === '個' || !c.unit)) {
      if (sumQ < c.conditionValue) unmet.push(c.displayName + ': あと ' + (c.conditionValue - sumQ).toLocaleString('ja-JP') + ' 個');
    } else if (c.conditionType === '上限') {
      var issuedQ = issuedMonthFor(c);
      var totalQ = sumQ + issuedQ;
      if (totalQ > c.conditionValue) unmet.push('🚫出荷制限超過 ' + c.displayName + ': 上限 ' + c.conditionValue.toLocaleString('ja-JP') + (c.unit || '個') + '/月 に対し ' +
        (issuedQ ? '今月済 ' + issuedQ.toLocaleString('ja-JP') + ' + 今回 ' + sumQ.toLocaleString('ja-JP') + ' = ' : '') + totalQ.toLocaleString('ja-JP') +
        ' (' + (totalQ - c.conditionValue).toLocaleString('ja-JP') + ' 超過)');
    } else {
      manual.push(c.displayName + ' (条件 ' + c.conditionType + ' ' + c.conditionValue.toLocaleString('ja-JP') + (c.unit || '') + ')');
    }
  });
  D.materialGroups.forEach(function(m) {
    if (m.minOrderQty == null) return;
    if (!m.memberCodes.some(function(x){ return inCart[x]; })) return;
    var mem = {}; m.memberCodes.forEach(function(x){ mem[x] = 1; });
    var sum = 0;
    items.forEach(function(i) {
      var p = byCode[i.code];
      if (mem[i.code] && p) sum += i.qty * (p.capacityPerUnit || 1); // 容量未設定=1換算 (ゲージと同じ)
    });
    if (sum < m.minOrderQty) unmet.push('原料 ' + m.name + ': あと ' + (m.minOrderQty - sum).toLocaleString('ja-JP') + (m.unit || ''));
  });
  return { unmet: unmet, manual: manual };
}
function renderBar() {
  var items = cartItems();
  var totQ = 0, totA = 0;
  items.forEach(function(i) {
    var p = byCode[i.code];
    totQ += i.qty; totA += i.qty * ((p && p.cost) || 0);
  });
  document.getElementById('fTot').innerHTML = items.length + ' SKU / ' + totQ.toLocaleString('ja-JP') + ' 個 / <b>' + yen(totA) + '</b>';
  var cc = condCheck();
  document.getElementById('fCond').innerHTML = cc.unmet.length
    ? '<span class="badge b-warn">⚠ ' + cc.unmet.length + '件未達</span>'
    : (cc.manual.length
      ? '<span class="badge" style="background:#fff7e6;color:#b45309">📝 手動確認' + cc.manual.length + '件</span>'
      : (items.length ? '<span class="badge b-issued">✅</span>' : ''));
  if (openPanel === 'items') renderPanelItems(items);
  if (openPanel === 'conds') renderPanelConds(items);
  updateSavedInd();
}
function renderAll(){ renderBar(); renderGauges(); }
function togglePanel(name) {
  openPanel = openPanel === name ? null : name;
  document.getElementById('panelItems').style.display = openPanel === 'items' ? '' : 'none';
  document.getElementById('panelConds').style.display = openPanel === 'conds' ? '' : 'none';
  renderAll();
}

document.addEventListener('input', function(ev) {
  var code = ev.target.getAttribute && ev.target.getAttribute('data-code');
  if (!code) return;
  var v = parseInt(ev.target.value, 10);
  if (!v || v <= 0) { delete CART[code]; clearDate(code); } else CART[code] = v;
  // 同じ商品の入力欄がリストとアコーディオンに複数出るので、他方も同期
  document.querySelectorAll('input[data-code]').forEach(function(inp) {
    if (inp !== ev.target && inp.getAttribute('data-code') === code) inp.value = ev.target.value;
  });
  renderAll();
});
// カートから外れた商品の希望納期も消す (数量を入れ直したときに古い日付が黙って復活しない、Codex 明細納期R1 Medium)
function clearDate(code) {
  delete DATES[code];
  document.querySelectorAll('input[data-date]').forEach(function(inp) {
    if (inp.getAttribute('data-date') === code) inp.value = '';
  });
}
// 商品ごとの希望納期 (空=指定なし)
document.addEventListener('input', function(ev) {
  var code = ev.target.getAttribute && ev.target.getAttribute('data-date');
  if (!code) return;
  var v = ev.target.value;
  if (v) DATES[code] = v; else delete DATES[code];
  document.querySelectorAll('input[data-date]').forEach(function(inp) {
    if (inp !== ev.target && inp.getAttribute('data-date') === code) inp.value = v;
  });
  updateSavedInd();
});
// ◯ヶ月分シミュレーションの月数変更
document.addEventListener('input', function(ev) {
  if (ev.target.classList && ev.target.classList.contains('simM')) updateSim(ev.target.closest('.accbox'));
});
// グループへの商品追加検索 (この仕入先の商品から、未所属のものを検索)
document.addEventListener('input', function(ev) {
  if (!ev.target.classList || !ev.target.classList.contains('gaddQ')) return;
  var q = ev.target.value.trim().toLowerCase();
  var res = ev.target.parentNode.querySelector('.gaddR');
  if (!q) { res.innerHTML = ''; return; }
  var k = ev.target.getAttribute('data-gkey');
  var members = {};
  groupMembers(k).forEach(function(c){ members[c] = 1; });
  var hits = [];
  Object.keys(byCode).some(function(code) {
    var p = byCode[code];
    if (members[code]) return false;
    if ((p.code + ' ' + p.name).toLowerCase().indexOf(q) < 0) return false;
    hits.push(p);
    return hits.length >= 8;
  });
  res.innerHTML = hits.length
    ? hits.map(function(p) {
        return '<div class="gaddItem"><a data-gadd="' + esc(p.code) + '" data-gkey="' + esc(k) + '">➕ 追加</a> <b>' + esc(p.code) + '</b> ' + esc(p.name) +
          ' <span class="muted">在庫月数 ' + months(p) + ' / 30日販売 ' + numFmt(p.sales30) + '</span></div>';
      }).join('')
    : '<div class="muted" style="margin-top:4px">一致なし (この仕入先の商品から検索しています)</div>';
});
// 入力確定時 (blur) に小数・指数表記等を CART の整数値に正規化して表示ズレを防ぐ
document.addEventListener('change', function(ev) {
  var code = ev.target.getAttribute && ev.target.getAttribute('data-code');
  if (!code) return;
  var norm = CART[code] || '';
  document.querySelectorAll('input[data-code]').forEach(function(inp) {
    if (inp.getAttribute('data-code') === code && String(inp.value) !== String(norm)) inp.value = norm;
  });
  // 「＋追加」行の発注数を 0/空 にしたら、確定時にリストから消す (入力途中は再描画しない=フォーカス保持)
  if (ADDED[code] && !(CART[code] > 0)) { renderLists(); renderAll(); }
});
// 指定商品のアコーディオンを開く (再描画後の復元にも使う)
function openAccFor(code) {
  if (!byCode[code]) return;
  var anchor = null;
  document.querySelectorAll('a.pname[data-acc]').forEach(function(a){ if (!anchor && a.getAttribute('data-acc') === code) anchor = a; });
  if (!anchor) return;
  var tr = anchor.closest('tr');
  var next = tr.nextElementSibling;
  if (next && next.classList.contains('accrow')) return; // 既に開いている
  var nr = document.createElement('tr');
  nr.className = 'accrow';
  var td = document.createElement('td');
  td.colSpan = tr.children.length;
  td.innerHTML = accHtml(byCode[code]);
  nr.appendChild(td);
  tr.parentNode.insertBefore(nr, tr.nextSibling);
  renderGauges();
  updateSim(td.querySelector('.accbox'));
}
function setQty(code, q) {
  CART[code] = q;
  document.querySelectorAll('input[data-code]').forEach(function(inp) {
    if (inp.getAttribute('data-code') === code) inp.value = q;
  });
}
document.addEventListener('click', function(ev) {
  var acc = ev.target.getAttribute && ev.target.getAttribute('data-acc');
  if (acc && byCode[acc]) {
    var tr = ev.target.closest('tr');
    var next = tr && tr.nextElementSibling;
    if (next && next.classList.contains('accrow')) { next.parentNode.removeChild(next); return; }
    openAccFor(acc);
    return;
  }
  // 必要数クリック → 発注数へ反映
  if (ev.target.classList && ev.target.classList.contains('need')) {
    var q = parseInt(ev.target.getAttribute('data-q'), 10);
    var nc = ev.target.getAttribute('data-nc');
    if (q > 0 && byCode[nc]) {
      setQty(nc, q);
      renderAll();
      toast(nc + ' の発注数を ' + q.toLocaleString('ja-JP') + ' にしました');
    }
    return;
  }
  // 必要数を全て発注数へコピー (グループ単位)
  if (ev.target.classList && ev.target.classList.contains('needAll')) {
    var accg = ev.target.closest('.accg');
    var applied = 0;
    accg.querySelectorAll('a.need').forEach(function(a) {
      var q2 = parseInt(a.getAttribute('data-q'), 10);
      var nc2 = a.getAttribute('data-nc');
      if (q2 > 0 && byCode[nc2]) { setQty(nc2, q2); applied++; }
    });
    renderAll();
    toast(applied ? applied + ' 商品に必要数を入れました (充足済みは変更なし)' : '必要数のある商品がありません (充足済み)');
    return;
  }
  // アコーディオンで決めた発注数を要発注リストへ反映して閉じる
  if (ev.target.classList && ev.target.classList.contains('accApply')) {
    var ownCode = ev.target.getAttribute('data-apply');
    renderLists(); renderAll(); // 再描画でアコーディオンは閉じ、発注数入り商品が各グループ配下に「＋追加」で並ぶ
    var anchor2 = null;
    document.querySelectorAll('a.pname[data-acc]').forEach(function(a){ if (!anchor2 && a.getAttribute('data-acc') === ownCode) anchor2 = a; });
    if (anchor2) anchor2.closest('tr').scrollIntoView({ behavior: 'smooth', block: 'center' });
    toast('発注数を入れた商品を要発注リストの各グループに並べました');
    return;
  }
  // グループへの商品追加 (検索結果から)
  var gadd = ev.target.getAttribute && ev.target.getAttribute('data-gadd');
  if (gadd && byCode[gadd]) {
    var gk = ev.target.getAttribute('data-gkey');
    var body = { product_code: byCode[gadd].code };
    if (gk.slice(0, 2) === 'c:') body.condition_id = gk.slice(2); else body.material_group_id = gk.slice(2);
    // このアコーディオンの持ち主 (再描画後に開き直す)
    var ownerTr = ev.target.closest('tr.accrow');
    var prevTr = ownerTr && ownerTr.previousElementSibling;
    var ownerA = prevTr && prevTr.querySelector('a.pname[data-acc]');
    var ownerCode = ownerA ? ownerA.getAttribute('data-acc') : null;
    fetch('/apps/purchase-orders/api/attrs/bind', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    }).then(function(r){ return r.json(); }).then(function(j) {
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      var p = byCode[gadd];
      if (gk.slice(0, 2) === 'c:') {
        if (p.conditionId && condById[p.conditionId]) condById[p.conditionId].memberCodes = condById[p.conditionId].memberCodes.filter(function(x){ return x !== p.code; });
        p.conditionId = gk.slice(2);
        if (condById[p.conditionId] && condById[p.conditionId].memberCodes.indexOf(p.code) < 0) condById[p.conditionId].memberCodes.push(p.code);
      } else {
        if (p.materialGroupId && matById[p.materialGroupId]) matById[p.materialGroupId].memberCodes = matById[p.materialGroupId].memberCodes.filter(function(x){ return x !== p.code; });
        p.materialGroupId = gk.slice(2);
        if (matById[p.materialGroupId] && matById[p.materialGroupId].memberCodes.indexOf(p.code) < 0) matById[p.materialGroupId].memberCodes.push(p.code);
      }
      renderLists(); renderAll();
      if (ownerCode) openAccFor(ownerCode);
      toast(p.code + ' をグループに追加しました');
    }).catch(function(e){ toast('通信エラー: ' + e.message); });
    return;
  }
  var del = ev.target.getAttribute && ev.target.getAttribute('data-del');
  if (del) {
    delete CART[del];
    clearDate(del);
    // querySelector の attribute selector は商品コード中の記号でこわれるので全走査で消す
    document.querySelectorAll('input[data-code]').forEach(function(inp) {
      if (inp.getAttribute('data-code') === del) inp.value = '';
    });
    renderAll();
    return;
  }
  // クリックでコピー (商品コード / 発注数)
  var cv = ev.target.getAttribute && ev.target.getAttribute('data-copy');
  if (cv) {
    navigator.clipboard.writeText(cv).then(function(){ toast('商品コードをコピー: ' + cv); }, function(){ toast('コピーに失敗しました'); });
    return;
  }
  var cq = ev.target.getAttribute && ev.target.getAttribute('data-copyq');
  if (cq) {
    var qv = String(CART[cq] || 0);
    navigator.clipboard.writeText(qv).then(function(){ toast('発注数をコピー: ' + qv); }, function(){ toast('コピーに失敗しました'); });
    return;
  }
  // 要発注リストから非表示 (✕)
  var dis = ev.target.getAttribute && ev.target.getAttribute('data-dis');
  if (dis) {
    DIS.set(dis, CART[dis] || 0);
    delete CART[dis];
    clearDate(dis);
    saveDis();
    renderLists(); renderAll();
    toast(dis + ' を非表示にしました (リスト下の「非表示」から戻せます)');
    return;
  }
  // グループごと非表示 (✕)
  var gdis = ev.target.getAttribute && ev.target.getAttribute('data-gdis');
  if (gdis) {
    var members3 = [];
    D.targets.forEach(function(p){ if (!DIS.has(p.code) && (groupKeyOf(p) || 'solo:' + p.code) === gdis) members3.push(p.code); });
    Object.keys(ADDED).forEach(function(code) {
      var p = byCode[code];
      if (p && (groupKeyOf(p) || 'solo:' + p.code) === gdis && members3.indexOf(code) < 0) members3.push(code);
    });
    if (!members3.length) return;
    if (!confirm('「' + groupName(gdis) + '」の ' + members3.length + ' 商品をまとめて非表示にしますか? (発注数も外れます。下の「非表示」から戻せます)')) return;
    members3.forEach(function(code) {
      DIS.set(code, CART[code] || 0);
      delete CART[code];
      clearDate(code);
    });
    saveDis();
    renderLists(); renderAll();
    toast(members3.length + ' 商品を非表示にしました');
    return;
  }
  // 非表示から戻す (発注数も復元)
  var undis = ev.target.getAttribute && ev.target.getAttribute('data-undis');
  if (undis) {
    var restore = undis === '*' ? [...DIS.keys()] : [undis];
    restore.forEach(function(code) {
      var qty = DIS.get(code);
      DIS.delete(code);
      if (qty > 0) CART[code] = qty;
    });
    saveDis();
    renderLists(); renderAll();
    toast(restore.length + ' 件をリストに戻しました');
    return;
  }
  // アコーディオンからのグループ紐付け保存
  if (ev.target.classList && ev.target.classList.contains('bindSave')) {
    var bcode = ev.target.getAttribute('data-bind');
    var box2 = ev.target.closest('.accg');
    var condSel = box2.querySelector('.bindCond').value;
    var matSel = box2.querySelector('.bindMat').value;
    ev.target.disabled = true;
    fetch('/apps/purchase-orders/api/attrs/bind', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_code: byCode[bcode] ? byCode[bcode].code : bcode, condition_id: condSel, material_group_id: matSel }),
    }).then(function(r){ return r.json(); }).then(function(j) {
      ev.target.disabled = false;
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      var p = byCode[bcode];
      if (p) {
        // 画面上のグループ membership も更新 (ゲージ・グループ分けに即時反映)
        if (p.conditionId && condById[p.conditionId]) condById[p.conditionId].memberCodes = condById[p.conditionId].memberCodes.filter(function(x){ return x !== p.code; });
        if (p.materialGroupId && matById[p.materialGroupId]) matById[p.materialGroupId].memberCodes = matById[p.materialGroupId].memberCodes.filter(function(x){ return x !== p.code; });
        p.conditionId = condSel; p.materialGroupId = matSel;
        if (condSel && condById[condSel] && condById[condSel].memberCodes.indexOf(p.code) < 0) condById[condSel].memberCodes.push(p.code);
        if (matSel && matById[matSel] && matById[matSel].memberCodes.indexOf(p.code) < 0) matById[matSel].memberCodes.push(p.code);
      }
      renderLists(); renderAll();
      openAccFor(bcode);
      toast('紐付けを保存しました' + ((condSel && !condById[condSel]) || (matSel && !matById[matSel]) ? ' (他仕入先グループのゲージは再読み込み後に表示)' : ''));
    }).catch(function(e){ ev.target.disabled = false; toast('通信エラー: ' + e.message); });
    return;
  }
  var sec = ev.target.getAttribute && ev.target.getAttribute('data-sec');
  if (sec === 'hori') {
    var el = document.getElementById('secHori');
    el.style.display = el.style.display === 'none' ? '' : 'none';
  }
  var idc = ev.target.id || (ev.target.closest && ev.target.closest('button') && ev.target.closest('button').id);
  if (idc === 'btnItems') togglePanel('items');
  if (idc === 'btnConds') togglePanel('conds');
  var act = ev.target.getAttribute && ev.target.getAttribute('data-act');
  if (act === 'save') save(false);
  if (act === 'issue') save(true);
});

// 発注確定の冪等キー: ノンス (成功時に更新) + 内容ハッシュ。
// 通信断後に同じ内容で再確定→同じキー (二重発注しない)。成功後に同内容を意図的に再発注→新ノンスで別キー
var ISSUE_NONCE = 'n' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
function contentHash(s) {
  // 32bit×2 (djb2 + FNV-1a) + 長さ。衝突してもサーバ側SHA-256の突合で409になるだけだが、
  // 正当な再試行が誤って拒否されないよう衝突耐性を上げておく
  var h1 = 5381, h2 = 0x811c9dc5;
  for (var i = 0; i < s.length; i++) {
    var c = s.charCodeAt(i);
    h1 = ((h1 << 5) + h1 + c) >>> 0;
    h2 = ((h2 ^ c) * 0x01000193) >>> 0;
  }
  return h1.toString(36) + h2.toString(36) + '-' + s.length;
}
// ── 保存済みインジケータ (下書き保存後に「◯SKU / 発注金額合計◯円」を常時表示、変更で未保存表示に) ──
var savedState = null, savedInfo = '';
var hasDraft = !!(D.draft && D.draft.items.length); // 空カート保存 (=削除) の確認ダイアログ用
function payloadNow() {
  return { items: cartItems().map(function(i){ return { code: i.code, qty: i.qty, requestedDate: i.requestedDate }; }),
    note: document.getElementById('orderNote').value };
}
function cartSummary(items) {
  var totA = 0;
  items.forEach(function(i){ var p = byCode[i.code]; totA += i.qty * ((p && p.cost) || 0); });
  return items.length + ' SKU / 発注金額合計 ' + yen(totA);
}
function updateSavedInd() {
  var h = '';
  if (savedState) {
    h = JSON.stringify(payloadNow()) === savedState
      ? savedInfo
      : '<span class="badge b-warn">未保存の変更あり</span>';
  }
  document.querySelectorAll('.fsavedInd').forEach(function(el){ el.innerHTML = h; });
}
document.addEventListener('input', function(ev) {
  if (ev.target.id === 'orderNote') updateSavedInd();
});
function save(issue) {
  var payload = payloadNow();
  var items = payload.items;
  if (issue && !items.length) { toast('発注する商品がありません'); return; }
  if (!issue && !items.length) {
    // 空カート保存 = 下書き削除。未保存のメモを黙って消さない (Codex R2 Medium)
    if (!hasDraft && !payload.note) { toast('保存する内容がありません'); return; }
    if (!confirm('カートが空です。保存すると下書きを削除し、メモの入力もクリアします。よろしいですか?')) return;
  }
  if (issue) {
    var cc = condCheck();
    var msg = 'この内容で発注確定しますか? (確定後は履歴に記録されます。NEへの発注登録は別途手動)';
    if (cc.unmet.length) {
      msg = '⚠️ 未達の発注条件があります:\\n・' + cc.unmet.join('\\n・') + '\\n\\nこのまま確定しますか?';
    }
    if (cc.manual.length) msg += '\\n\\n※手動確認が必要な条件: ' + cc.manual.join(' / ');
    var nd = items.filter(function(i){ return i.requestedDate; }).length;
    msg += '\\n\\n🗓 希望納期: ' + (nd ? nd + '\/' + items.length + '品目に指定あり (メールに記載されます)' : '指定なし');
    if (D.supplier.memo) msg += '\\n\\n📌 発注メモ: ' + D.supplier.memo;
    if (!confirm(msg)) return;
  }
  // 二重送信ガード (確定連打で同内容の発注が複数作られるのを防ぐ)
  var actBtns = document.querySelectorAll('button[data-act]');
  actBtns.forEach(function(b){ b.disabled = true; });
  var unlock = function(){ actBtns.forEach(function(b){ b.disabled = false; }); };
  var url = '/apps/purchase-orders/api/supplier/' + encodeURIComponent(D.supplier.code) + (issue ? '/issue' : '/draft');
  var summary = cartSummary(items);
  // 冪等キー: 内容から決定的に導出 (通信断後に同じ内容で再確定→同じキー=二重発注しない。内容を変えれば別キー)
  var headers = { 'Content-Type': 'application/json' };
  if (issue) headers['Idempotency-Key'] = 'issue-' + D.supplier.code + '-' + ISSUE_NONCE + '-' + contentHash(JSON.stringify(payload));
  fetch(url, {
    method: 'POST', headers: headers,
    body: JSON.stringify(payload),
  }).then(function(r){ return r.json().catch(function(){ return { ok: false, error: 'HTTP ' + r.status + ' — 確定されたか不明です。発注履歴を確認してください' }; }); }).then(function(j) {
    unlock();
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    if (issue) {
      ISSUE_NONCE = 'n' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); // 次回の意図的な同内容発注は別キーに
      showDone(j.id, j.poNumber); // CART を読むのでクリア前に
      CART = {}; DATES = {};
      document.querySelectorAll('input[data-code]').forEach(function(inp){ inp.value = ''; });
      document.querySelectorAll('input[data-date]').forEach(function(inp){ inp.value = ''; });
      document.getElementById('orderNote').value = '';
      hasDraft = false; // draft は確定で消費済み
      // 確定サマリをバーに常時表示。クリア後の空状態を基準に、以後の編集は「未保存の変更あり」(Codex R2 Medium)
      savedState = JSON.stringify(payloadNow());
      savedInfo = '✅ ' + new Date().toTimeString().slice(0, 5) + ' 発注確定 ' + esc(j.poNumber || ('#' + j.id)) + ' — ' + summary;
      renderAll();
      toast('発注確定しました (' + (j.poNumber || '#' + j.id) + ' — ' + summary + ')' + (j.replay ? ' ※前回確定分を再表示 (二重発注なし)' : ''));
    }
    else if (!items.length) {
      // draft削除APIは items 空のとき note を保存しない。画面の残留入力をDBと揃えてクリア (Codex R1 Medium)
      document.getElementById('orderNote').value = '';
      hasDraft = false;
      savedState = null; updateSavedInd();
      toast(j.deleted ? '下書きを削除しました' : '下書きはありません (入力をクリアしました)');
    }
    else {
      hasDraft = true;
      savedState = JSON.stringify(payload);
      savedInfo = '💾 ' + new Date().toTimeString().slice(0, 5) + ' 保存済み — ' + summary;
      updateSavedInd();
      toast('下書きを保存しました (' + summary + ')');
    }
  }).catch(function(e){ unlock(); toast('通信エラー: ' + e.message); });
}
function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(function(){ toast('コピーしました'); },
    function(){ toast('コピーに失敗しました。手動で選択してください'); });
}
function showDone(orderId, poNumber) {
  var items = cartItems();
  var lines = items.map(function(i){ return i.code + '\\t' + (byCode[i.code] ? byCode[i.code].name : '') + '\\t' + i.qty + '\\t' + (i.requestedDate || ''); });
  var text = lines.join('\\n');
  var area = document.getElementById('doneArea');
  var totA = 0;
  items.forEach(function(i){ totA += i.qty * (byCode[i.code].cost || 0); });
  document.getElementById('doneBody').innerHTML =
    '<div class="tot">発注 ' + esc(poNumber || ('#' + orderId)) + ' — ' + items.length + ' SKU / 合計 ' + yen(totA) +
    ' <button class="pri" id="btnCopy">📋 リストをコピー</button> <button id="btnCsv">⬇ CSVダウンロード</button>' +
    ' <a href="/apps/purchase-orders/backorders">発注残で管理 →</a> <a href="/apps/purchase-orders/orders">履歴を見る →</a></div>' +
    '<pre class="copy">' + esc('商品コード\\t商品名\\t数量\\t希望納期\\n' + text) + '</pre>';
  area.style.display = '';
  document.getElementById('btnCopy').addEventListener('click', function(){ copyText(text); });
  document.getElementById('btnCsv').addEventListener('click', function() {
    var rows = [['商品コード', '商品名', '数量', '希望納期', '原価', '金額']];
    items.forEach(function(i) {
      var p = byCode[i.code];
      rows.push([i.code, p ? p.name : '', i.qty, i.requestedDate || '', p && p.cost ? p.cost : '', p && p.cost ? Math.round(i.qty * p.cost) : '']);
    });
    dlCsv('発注_' + D.supplier.name + '_#' + orderId + '.csv', rows);
  });
  area.scrollIntoView({ behavior: 'smooth' });
}

// ── ページ内商品検索 (データから再描画。debounceで連打時の無応答を防ぐ) ──
var horiAutoOpened = false;
var qTimer = null;
function applySearch(raw) {
  Q = String(raw || '').trim().toLowerCase();
  var hori = document.getElementById('secHori');
  if (Q && hori.style.display === 'none') { hori.style.display = ''; horiAutoOpened = true; } // 検索時は掘り起こしも対象に
  if (!Q && horiAutoOpened) { hori.style.display = 'none'; horiAutoOpened = false; } // 検索クリアで元に戻す
  renderLists();
  renderAll();
}
document.addEventListener('input', function(ev) {
  if (ev.target.id !== 'pageQ') return;
  clearTimeout(qTimer);
  var v = ev.target.value;
  qTimer = setTimeout(function(){ applySearch(v); }, 250);
});

document.getElementById('orderNote').value = D.draft ? D.draft.note : '';
if (D.draft) {
  // 復元した下書き = 保存済み状態として表示 (以後の変更で「未保存の変更あり」に切り替わる)
  var p0 = payloadNow();
  savedState = JSON.stringify(p0);
  savedInfo = '💾 下書き保存済み — ' + cartSummary(p0.items);
}
renderLists();
renderAll();`;
  res.send(pageShell(`発注 — ${data.supplier.name}`, '', body, script));
});

// ─── 画面2b: 全商品情報 (旧「発注対象商品」シート相当。全商品の販売×在庫を一覧) ───
router.get('/products', (req, res) => {
  let rows, supNames, pub, overlay;
  try {
    const r = computeAll();
    pub = r.pub; overlay = r.overlay;
    supNames = {};
    for (const [code, s] of r.masters.suppliers) supNames[code] = s.name;
    rows = r.products.map(p => ({
      c: p.code, n: p.name, s: p.supplierCode, a: p.active ? 1 : 0,
      st: p.stock, b: p.backOrder, s7: p.sales7, s30: p.sales30,
      m: Math.round(p.stockMonths * 100) / 100, hm: p.holdMonths, lo: p.lot,
      co: p.cost, pr: p.price, lp: p.lastPurchase ? String(p.lastPurchase).slice(0, 10) : '',
      t: p.isTarget ? 1 : 0, h: p.isHorikoshi ? 1 : 0,
    }));
  } catch (e) { return res.status(500).send('error: ' + he(e.message)); }

  const body = `
    <div class="toolbar">
      <h2 class="page" style="margin:0">全商品情報 <span class="muted" id="cnt"></span></h2>
      <span class="muted">${pub ? freshnessText(pub, overlay) : 'PML未同期'}</span>
    </div>
    <div class="toolbar" style="gap:10px;flex-wrap:wrap">
      <input type="text" id="q" placeholder="商品コード / 商品名で検索" style="min-width:260px">
      <select id="fAct"><option value="1" selected>取扱中のみ</option><option value="">全部 (取扱中止含む)</option></select>
      <select id="fState"><option value="">状態: 全部</option><option value="t">🔴 要発注のみ</option><option value="h">⚪ 掘り起こしのみ</option></select>
      <select id="fSup"><option value="">仕入先: 全部</option></select>
      <span class="muted" style="margin-left:auto">列見出しクリックで並び替え</span>
    </div>
    <div class="sec"><div class="bd" id="tbl">読み込み中…</div></div>`;

  const script = `
var ROWS = ${jsonEmbed(rows)};
var SUP = ${jsonEmbed(supNames)};
var CAP = 1000;
var shown = CAP;
var sortKey = 's30', sortDir = -1;
// d: 初回クリック時の方向 (文字列列=昇順、数値/日付/状態列=降順)
var COLS = [
  { k: 'c', l: '商品コード', d: 1 }, { k: 'n', l: '商品名', d: 1 }, { k: 's', l: '仕入先', d: 1 }, { k: '_st', l: '状態', d: -1 },
  { k: 'st', l: '在庫', r: 1, d: -1 }, { k: 'b', l: '注残', r: 1, d: -1 }, { k: 's7', l: '7日販売', r: 1, d: -1 }, { k: 's30', l: '30日販売', r: 1, d: -1 },
  { k: 'm', l: '在庫月数', r: 1, d: -1 }, { k: 'hm', l: '推奨保有', r: 1, d: -1 }, { k: 'lo', l: 'ロット', r: 1, d: -1 },
  { k: 'co', l: '原価', r: 1, d: -1 }, { k: 'pr', l: '売価', r: 1, d: -1 }, { k: 'lp', l: '最終仕入日', d: -1 },
];
// 仕入先フィルタの選択肢 (商品が存在するコードのみ、名前順)
(function() {
  var codes = {};
  ROWS.forEach(function(r){ if (r.s) codes[r.s] = 1; });
  var list = Object.keys(codes).map(function(c){ return { c: c, n: SUP[c] || ('仕入先 ' + c) }; });
  list.sort(function(a, b){ return a.n.localeCompare(b.n, 'ja'); });
  var sel = document.getElementById('fSup');
  list.forEach(function(s) {
    var o = document.createElement('option');
    o.value = s.c; o.textContent = s.n;
    sel.appendChild(o);
  });
})();
function stateBadge(r) {
  if (!r.a) return '<span class="badge" style="background:#eceff3;color:#64748b">取扱中止</span>';
  if (r.t) return '<span class="badge b-warn">🔴 要発注</span>';
  if (r.h) return '<span class="badge" style="background:#f4f4f5;color:#52525b">⚪ 掘り起こし</span>';
  return '<span class="badge b-issued">取扱中</span>';
}
function filtered() {
  var q = document.getElementById('q').value.trim().toLowerCase();
  var act = document.getElementById('fAct').value;
  var stt = document.getElementById('fState').value;
  var sup = document.getElementById('fSup').value;
  return ROWS.filter(function(r) {
    if (act === '1' && !r.a) return false;
    if (stt === 't' && !r.t) return false;
    if (stt === 'h' && !r.h) return false;
    if (sup && r.s !== sup) return false;
    if (q && String(r.c).toLowerCase().indexOf(q) < 0 && String(r.n).toLowerCase().indexOf(q) < 0) return false;
    return true;
  });
}
function render() {
  var list = filtered();
  var k = sortKey;
  function stateRank(r){ return !r.a ? 0 : r.t ? 3 : r.h ? 1 : 2; }
  list.sort(function(a, b) {
    var x = k === '_st' ? stateRank(a) : a[k], y = k === '_st' ? stateRank(b) : b[k];
    if (x == null) x = -Infinity; if (y == null) y = -Infinity;
    if (typeof x === 'string' || typeof y === 'string') return String(x).localeCompare(String(y), 'ja') * sortDir;
    return (x - y) * sortDir;
  });
  document.getElementById('cnt').textContent = '(' + list.length.toLocaleString('ja-JP') + ' 件 / 全 ' + ROWS.length.toLocaleString('ja-JP') + ' 件)';
  var h = '<table class="t"><tr>' + COLS.map(function(c) {
    var arrow = c.k === sortKey ? (sortDir < 0 ? ' ▼' : ' ▲') : '';
    return '<th' + (c.r ? ' class="r"' : '') + ' data-sort="' + c.k + '" style="cursor:pointer">' + c.l + arrow + '</th>';
  }).join('') + '</tr>';
  list.slice(0, shown).forEach(function(r) {
    h += '<tr><td>' + esc(r.c) + '</td><td><a class="pname" data-acc="' + esc(r.c) + '" title="クリックでモール別販売内訳">' + esc(r.n) + '</a></td>' +
      '<td><a href="/apps/purchase-orders/supplier/' + encodeURIComponent(r.s) + '">' + esc(SUP[r.s] || r.s || '—') + '</a></td>' +
      '<td>' + stateBadge(r) + '</td>' +
      '<td class="r">' + (r.st == null ? '—' : r.st.toLocaleString('ja-JP')) + '</td>' +
      '<td class="r">' + (r.b ? r.b.toLocaleString('ja-JP') : '—') + '</td>' +
      '<td class="r">' + (r.s7 || 0).toLocaleString('ja-JP') + '</td>' +
      '<td class="r">' + (r.s30 || 0).toLocaleString('ja-JP') + '</td>' +
      '<td class="r">' + (r.m == null ? '—' : r.m.toFixed(2)) + '</td>' +
      '<td class="r">' + (r.hm || '—') + '</td>' +
      '<td class="r">' + (r.lo || '—') + '</td>' +
      '<td class="r">' + (r.co ? yen(r.co) : '—') + '</td>' +
      '<td class="r">' + (r.pr ? yen(r.pr) : '—') + '</td>' +
      '<td>' + esc(r.lp || '—') + '</td></tr>';
  });
  h += '</table>';
  if (list.length > shown) {
    h += '<div style="padding:10px 14px">先頭 ' + shown.toLocaleString('ja-JP') + ' 件を表示中 (残り ' + (list.length - shown).toLocaleString('ja-JP') + ' 件) ' +
      '<button class="ghost" id="btnMore">さらに ' + CAP.toLocaleString('ja-JP') + ' 件表示</button></div>';
  }
  document.getElementById('tbl').innerHTML = h;
}
// ── 商品名クリック → モール別販売内訳 (速報7/30日 + 確定30〜365日) ──
function mallBoxHtml(code, j, days) {
  var h = '<div class="accbox">';
  h += '<div style="font-size:13px"><b>📈 速報モール別 (NE受注ベース・毎朝更新' + (j.sokuho.asOf ? '、' + esc(j.sokuho.asOf) + ' 時点' : '') + ')</b></div>';
  if (!j.sokuho.rows.length) h += '<div class="muted" style="margin-top:4px">直近30日のNE受注なし (または速報マート未同期)</div>';
  else {
    var t7 = 0, t30 = 0;
    h += '<table class="t sub"><tr><th>モール</th><th class="r">7日</th><th class="r">30日</th></tr>';
    j.sokuho.rows.forEach(function(r) {
      t7 += r.qty7; t30 += r.qty30;
      h += '<tr><td>' + esc(r.label) + '</td><td class="r">' + r.qty7.toLocaleString('ja-JP') + '</td><td class="r">' + r.qty30.toLocaleString('ja-JP') + '</td></tr>';
    });
    h += '<tr><td><b>合計</b></td><td class="r"><b>' + t7.toLocaleString('ja-JP') + '</b></td><td class="r"><b>' + t30.toLocaleString('ja-JP') + '</b></td></tr></table>';
  }
  h += '<div class="accg"><b>🏪 長期モール別販売ピース数 (確定データ・セット構成展開)</b> ' +
    '期間: <select class="msDays" data-mscode="' + esc(code) + '">' +
    [30, 90, 180, 365].map(function(d){ return '<option value="' + d + '"' + (d === days ? ' selected' : '') + '>直近' + d + '日</option>'; }).join('') +
    '</select>';
  if (!j.finance.available) h += '<div class="muted" style="margin-top:4px">モール別確定データ未同期</div>';
  else {
    h += ' <span class="muted">(' + esc(j.finance.start) + ' 〜 ' + esc(j.finance.end) + '。精算/受注factをNEコードに名寄せ、3個セット1件=3個。メルカリ・卸は確定マート未整備のため上の速報のみ)</span>';
    if (!j.finance.rows.length) h += '<div class="muted" style="margin-top:4px">この期間の販売なし</div>';
    else {
      var tp = 0;
      h += '<table class="t sub"><tr><th>モール</th><th class="r">ピース数</th><th class="r">日平均</th></tr>';
      j.finance.rows.forEach(function(r) {
        tp += r.pieces;
        h += '<tr><td>' + esc(r.label) + '</td><td class="r">' + r.pieces.toLocaleString('ja-JP') + '</td><td class="r">' + (Math.round(r.pieces / j.finance.days * 100) / 100).toFixed(2) + '</td></tr>';
      });
      h += '<tr><td><b>合計</b></td><td class="r"><b>' + (Math.round(tp * 10) / 10).toLocaleString('ja-JP') + '</b></td><td class="r"><b>' + (Math.round(tp / j.finance.days * 100) / 100).toFixed(2) + '</b></td></tr></table>';
    }
  }
  h += '</div></div>';
  return h;
}
function loadMallSales(code, days, td) {
  td.innerHTML = '<div class="muted" style="padding:8px">モール別販売を読み込み中…</div>';
  fetch('/apps/purchase-orders/api/products/' + encodeURIComponent(code) + '/mall-sales?days=' + days)
    .then(function(r){ return r.json(); })
    .then(function(j) {
      if (!j.ok) { td.innerHTML = '<div class="muted" style="padding:8px">エラー: ' + esc(j.error) + '</div>'; return; }
      td.innerHTML = mallBoxHtml(code, j, days);
    })
    .catch(function(e){ td.innerHTML = '<div class="muted" style="padding:8px">通信エラー: ' + esc(e.message) + '</div>'; });
}
var qTimer = null;
document.addEventListener('input', function(ev) {
  if (ev.target.id !== 'q') return;
  clearTimeout(qTimer);
  qTimer = setTimeout(function(){ shown = CAP; render(); }, 250); // debounce (1万行フィルタ+1000行再描画の連打で固まるのを防ぐ)
});
document.addEventListener('change', function(ev) {
  if (['fAct','fState','fSup'].indexOf(ev.target.id) >= 0) { shown = CAP; render(); return; }
  if (ev.target.classList && ev.target.classList.contains('msDays')) {
    loadMallSales(ev.target.getAttribute('data-mscode'), parseInt(ev.target.value, 10), ev.target.closest('td'));
  }
});
document.addEventListener('click', function(ev) {
  if (ev.target.id === 'btnMore') { shown += CAP; render(); return; }
  var acc = ev.target.getAttribute && ev.target.getAttribute('data-acc');
  if (acc) {
    var tr = ev.target.closest('tr');
    var next = tr && tr.nextElementSibling;
    if (next && next.classList.contains('accrow')) { next.parentNode.removeChild(next); return; }
    var nr = document.createElement('tr');
    nr.className = 'accrow';
    var td = document.createElement('td');
    td.colSpan = tr.children.length;
    nr.appendChild(td);
    tr.parentNode.insertBefore(nr, tr.nextSibling);
    loadMallSales(acc, 90, td);
    return;
  }
  var k = ev.target.getAttribute && ev.target.getAttribute('data-sort');
  if (!k) return;
  var col = null;
  COLS.forEach(function(c){ if (c.k === k) col = c; });
  if (sortKey === k) sortDir = -sortDir; else { sortKey = k; sortDir = col ? col.d : -1; }
  shown = CAP;
  render();
});
render();`;
  res.send(pageShell('発注補助 — 全商品情報', 'products', body, script));
});

// ─── 画面3: 発注履歴 ───
// ─── 画面: 入庫消込 (P14) ───
router.get('/inbound', (req, res) => {
  const body = `
    <h2 class="page">入庫消込 <span class="muted" style="font-size:12px">ロジザードの「NE仕入れ取込用データ」CSVを取り込み、発注残と突合して消し込みます (NE仕入取込と同じファイルでOK)</span></h2>
    <div class="import-zone">
      <form id="inbForm" class="row" style="display:flex;gap:8px;align-items:center">
        <input type="file" name="file" accept=".csv" required>
        <button type="submit" class="pri">📥 入庫CSVを取り込む</button>
        <span id="inbStatus" class="muted"></span>
      </form>
      <div id="inbResult" class="pill-row"></div>
    </div>
    <div id="conflictArea"></div>
    <h2 class="page" style="font-size:15px">未割当の入庫 <span class="muted" id="openCount"></span></h2>
    <div class="sec"><div class="bd" id="openList" style="max-height:none">読み込み中…</div></div>
    <div id="ignoredArea"></div>`;
  const script = `
var API = '/apps/purchase-orders/api';
var INB = null;
function newIdemKey() { return (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : 'k' + Date.now() + '-' + Math.random().toString(36).slice(2); }
function jsonOrErr(r, isWrite) {
  return r.json().catch(function(){ return { ok: false, error: 'HTTP ' + r.status + (isWrite ? ' — 登録されたか不明です。画面を更新して確認してください' : ' — 取得に失敗しました') }; });
}
function post(url, body, idemKey) {
  var h = { 'Content-Type': 'application/json' };
  if (idemKey) h['Idempotency-Key'] = idemKey;
  return fetch(url, { method: 'POST', headers: h, body: JSON.stringify(body) }).then(function(r){ return jsonOrErr(r, true); });
}
function getJson(url) { return fetch(url).then(function(r){ return jsonOrErr(r, false); }); }
function fmtD(s) { return s ? String(s).slice(0, 10) : '—'; }

var LOAD_SEQ = 0;
function load() {
  var seq = ++LOAD_SEQ;
  getJson(API + '/inbound').then(function(j) {
    if (seq !== LOAD_SEQ) return;
    if (!j.ok) { document.getElementById('openList').innerHTML = '<div class="warn">エラー: ' + esc(j.error) + ' <button class="ghost" onclick="load()">再読込</button></div>'; return; }
    INB = j; render();
  }).catch(function(e) {
    if (seq !== LOAD_SEQ) return;
    document.getElementById('openList').innerHTML = '<div class="warn">通信エラー: ' + esc(e.message) + ' <button class="ghost" onclick="load()">再読込</button></div>';
  });
}

function render() {
  // 訂正競合 (最優先で目立たせる)
  var ca = document.getElementById('conflictArea');
  if (INB.conflicts.length) {
    var ch = '<div class="warn">🚨 <b>訂正競合 ' + INB.conflicts.length + '件</b> — 訂正版CSVで消えた入庫行に消込が残っています。発注残ページの該当明細の履歴から逆仕訳し、正しい行に再割当してください<table class="t">' +
      '<tr><th>伝票NO</th><th>商品</th><th class="r">割当済</th></tr>';
    INB.conflicts.forEach(function(c) {
      ch += '<tr><td>' + esc(c.slip) + '</td><td>' + esc(c.productCode) + '</td><td class="r">' + c.allocated + '</td></tr>';
    });
    ca.innerHTML = ch + '</table></div>';
  } else ca.innerHTML = '';

  document.getElementById('openCount').textContent = '(' + INB.open.length + '件)';
  if (!INB.open.length) {
    document.getElementById('openList').innerHTML = '<div class="muted">未割当の入庫はありません 🎉 (CSVを取り込むとここに出ます)</div>';
  } else {
    var h = '<table class="t"><tr><th>伝票NO</th><th>入庫日</th><th>仕入先</th><th>商品</th><th class="r">良品数</th><th class="r">不良</th><th class="r">割当済</th><th class="r">未割当</th><th class="r">単価</th><th></th></tr>';
    INB.open.forEach(function(i) {
      h += '<tr data-inb="' + i.id + '"><td>' + esc(i.slip) + '</td><td>' + fmtD(i.receiptDate) + '</td><td>' + esc(i.supplierCode || '—') + '</td>' +
        '<td><b>' + esc(i.productCode) + '</b></td>' +
        '<td class="r">' + i.goodQty + '</td><td class="r">' + (i.defectiveQty || '') + '</td>' +
        '<td class="r">' + i.allocated + '</td><td class="r"><b>' + i.remainingCapacity + '</b></td>' +
        '<td class="r">' + (i.unitCost != null ? i.unitCost : '—') + '</td>' +
        '<td style="white-space:nowrap"><button class="pri" data-match="' + i.id + '">🔗 発注残に割当</button> <button class="ghost" data-ign="' + i.id + '">🚫対象外</button></td></tr>' +
        '<tr id="mpanel-' + i.id + '" style="display:none"><td colspan="10" id="mpanelBody-' + i.id + '"></td></tr>';
    });
    document.getElementById('openList').innerHTML = h + '</table>';
  }
  var ia = document.getElementById('ignoredArea');
  if (INB.ignored.length) {
    var ih = '<h2 class="page" style="font-size:14px">対象外にした入庫 (' + INB.ignored.length + ')</h2><div class="sec"><div class="bd" style="max-height:none"><table class="t"><tr><th>伝票NO</th><th>商品</th><th class="r">良品数</th><th></th></tr>';
    INB.ignored.forEach(function(i) {
      ih += '<tr><td>' + esc(i.slip) + '</td><td>' + esc(i.productCode) + '</td><td class="r">' + i.goodQty + '</td>' +
        '<td><button class="ghost" data-unign="' + i.id + '">↩ 対象外を解除</button></td></tr>';
    });
    ia.innerHTML = ih + '</table></div></div>';
  } else ia.innerHTML = '';
}

function showMPanel(id, html) {
  document.querySelectorAll('tr[id^=mpanel-]').forEach(function(tr){ tr.style.display = 'none'; });
  document.querySelectorAll('td[id^=mpanelBody-]').forEach(function(td){ if (td.id !== 'mpanelBody-' + id) td.innerHTML = ''; });
  document.getElementById('mpanelBody-' + id).innerHTML = html;
  document.getElementById('mpanel-' + id).style.display = '';
}

// ── 突合候補パネル ──
function matchPanel(inbId) {
  getJson(API + '/inbound/' + inbId + '/candidates').then(function(j) {
    if (!j.ok) { toast(j.error); return; }
    if (!j.candidates.length) {
      showMPanel(inbId, '<div class="muted">この商品の発注残が見つかりません (仕入先不一致は除外)。手動発注や対象外を検討してください <button class="ghost" data-mclose="' + inbId + '">閉じる</button></div>');
      return;
    }
    var cap = j.item.capacity;
    var h = '<div class="import-zone" style="margin:6px 0"><b>🔗 割当先の発注を選択</b> (未割当 ' + cap + ')' +
      '<table class="t" style="margin-top:6px"><tr><th></th><th>PO番号</th><th>仕入先</th><th>発注日</th><th>納期</th><th class="r">発注残</th><th class="r">PO単価</th><th>備考</th></tr>';
    j.candidates.forEach(function(c, k) {
      h += '<tr><td><input type="radio" name="cand" value="' + c.orderItemId + '" data-remaining="' + c.remaining + '" data-sug="' + c.suggestedQty + '"' + (k === 0 ? ' checked' : '') + '></td>' +
        '<td>' + esc(c.poNumber || ('#' + c.orderId)) + '</td><td>' + esc(c.supplierName) + '</td><td>' + fmtD(c.issuedAt) + '</td><td>' + fmtD(c.due) + '</td>' +
        '<td class="r">' + c.remaining + '</td><td class="r">' + (c.poUnitCost != null ? c.poUnitCost : '—') + '</td>' +
        '<td>' + (c.costDiff ? '<span class="badge b-issued" title="入庫単価とPO単価が異なります (価格改定?)">⚠️単価差異</span>' : '') + (c.score >= 100 ? '' : '') + '</td></tr>';
    });
    h += '</table><div class="row" style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">' +
      '割当数量 <input type="number" id="mQty" min="1" max="' + cap + '" value="' + (j.candidates[0] ? j.candidates[0].suggestedQty : cap) + '" style="width:90px">' +
      ' <span id="mPrev" class="muted"></span></div>' +
      '<div id="mRemBox" style="margin-top:8px;display:none"><b>発注側の残りの扱い (必須)</b>: ' +
      '<label><input type="radio" name="mrem" value="await_delivery" checked> 🚚 分納待ち</label> ' +
      '<label><input type="radio" name="mrem" value="shortage"> ➖ 残りは減数で完了</label> ' +
      '<label><input type="radio" name="mrem" value="await_confirmation"> 📞 確認中</label>' +
      '<span id="mRemFields" style="margin-left:8px"></span></div>' +
      '<div style="margin-top:8px"><button class="pri" id="mGo">この発注に割り当てて消込</button> <button class="ghost" data-mclose="' + inbId + '">やめる</button></div></div>';
    showMPanel(inbId, h);
    var idemKey = newIdemKey();
    document.getElementById('mpanelBody-' + inbId).addEventListener('input', function(){ idemKey = newIdemKey(); });
    function sel() { return document.querySelector('input[name=cand]:checked'); }
    function refresh() {
      var c = sel();
      var q = Number(document.getElementById('mQty').value) || 0;
      var rem = Number(c.getAttribute('data-remaining'));
      var after = rem - q;
      document.getElementById('mPrev').textContent = (q > 0 ? '→ 発注残 ' + rem + ' → ' + after + ' / 入庫未割当 ' + cap + ' → ' + (cap - q) : '');
      document.getElementById('mRemBox').style.display = (q > 0 && q <= rem && q <= cap && after > 0) ? '' : 'none';
      mRemFields(after);
    }
    function mRemFields(after) {
      var v = (document.querySelector('input[name=mrem]:checked') || {}).value;
      var el = document.getElementById('mRemFields');
      if (v === 'await_delivery') el.innerHTML = '次回予定日 <input type="date" id="mRemDate"> 数量 <input type="number" id="mRemQty" min="1" value="' + Math.max(1, after) + '" style="width:80px">';
      else if (v === 'shortage') el.innerHTML = '理由 <select id="mRemReason"><option value="supplier_shortage">仕入先都合</option><option value="own_decision">自社判断</option><option value="other">その他</option></select> <input type="text" id="mRemNote" placeholder="メモ (その他は必須)" style="width:160px">';
      else el.innerHTML = '確認期限 <input type="date" id="mRemActDate">';
    }
    document.getElementById('mQty').addEventListener('input', refresh);
    document.querySelectorAll('input[name=cand]').forEach(function(x){ x.addEventListener('change', function() {
      document.getElementById('mQty').value = Math.min(cap, Number(sel().getAttribute('data-sug')) || cap);
      refresh();
    }); });
    document.querySelectorAll('input[name=mrem]').forEach(function(x){ x.addEventListener('change', refresh); });
    refresh();
    document.getElementById('mGo').addEventListener('click', function() {
      var c = sel();
      var q = Number(document.getElementById('mQty').value);
      var rem = Number(c.getAttribute('data-remaining'));
      var body = { type: 'receipt', qty: q, inboundItemId: Number(inbId) };
      if (q < rem) {
        var v = (document.querySelector('input[name=mrem]:checked') || {}).value;
        if (v === 'await_delivery') body.remainder = { action: 'await_delivery', nextExpectedDate: (document.getElementById('mRemDate') || {}).value, nextExpectedQty: (document.getElementById('mRemQty') || {}).value };
        else if (v === 'shortage') body.remainder = { action: 'shortage', reasonCode: (document.getElementById('mRemReason') || {}).value, note: (document.getElementById('mRemNote') || {}).value };
        else body.remainder = { action: 'await_confirmation', nextActionDate: (document.getElementById('mRemActDate') || {}).value };
      }
      var btn = this; btn.disabled = true;
      post(API + '/items/' + c.value + '/events', body, idemKey).then(function(j2) {
        btn.disabled = false;
        if (!j2.ok) { toast('エラー: ' + j2.error); return; }
        toast('割り当てました (発注残 ' + j2.remaining + (j2.orderClosed ? '、発注完了' : '') + ')');
        load();
      }).catch(function(e){ btn.disabled = false; toast('通信エラー: ' + e.message + ' — 同じ内容のまま再実行すれば二重登録なしで確認できます'); });
    });
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
}

document.getElementById('inbForm').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  document.getElementById('inbStatus').textContent = '取込中…';
  fetch(API + '/inbound/import', { method: 'POST', body: fd }).then(function(r){ return jsonOrErr(r, true); }).then(function(j) {
    document.getElementById('inbStatus').textContent = '';
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    var pills = [];
    if (j.alreadyImported) pills.push('⏭ このファイルは取込済みです (変更なし)');
    else {
      pills.push('✅ 伝票 ' + j.receipts + '件');
      pills.push('新規明細 ' + j.newItems);
      if (j.unchangedItems) pills.push('変更なし ' + j.unchangedItems);
      if (j.supersededItems) pills.push('訂正で無効化 ' + j.supersededItems);
      if (j.conflicts && j.conflicts.length) pills.push('🚨 訂正競合 ' + j.conflicts.length);
    }
    (j.warnings || []).forEach(function(w){ pills.push('⚠️ ' + w); });
    document.getElementById('inbResult').innerHTML = pills.map(function(p){ return '<span class="badge b-draft" style="margin:2px">' + esc(p) + '</span>'; }).join(' ');
    load();
  }).catch(function(e){ document.getElementById('inbStatus').textContent = ''; toast('通信エラー: ' + e.message); });
});

document.addEventListener('click', function(ev) {
  var t = ev.target;
  var g = function(a){ return t.getAttribute && t.getAttribute(a); };
  var v;
  if ((v = g('data-match'))) { matchPanel(v); return; }
  if ((v = g('data-mclose'))) { document.getElementById('mpanel-' + v).style.display = 'none'; return; }
  if ((v = g('data-ign'))) {
    showMPanel(v, '<b>🚫 この入庫を消込の対象外にする</b> (発注に紐づかない入庫: 返品・サンプル等) ' +
      '理由 <input type="text" id="ignReason" placeholder="例: 返品の再入庫" style="width:220px"> ' +
      '<button class="pri" id="ignGo">対象外にする</button> <button class="ghost" data-mclose="' + v + '">やめる</button>');
    document.getElementById('ignGo').addEventListener('click', function() {
      var reason = document.getElementById('ignReason').value;
      if (!reason.trim()) { toast('理由を入力してください'); return; }
      var btn = this; btn.disabled = true;
      post(API + '/inbound/' + v + '/ignore', { ignore: true, reason: reason }).then(function(j) {
        btn.disabled = false;
        if (!j.ok) { toast('エラー: ' + j.error); return; }
        toast('対象外にしました'); load();
      }).catch(function(e){ btn.disabled = false; toast('通信エラー: ' + e.message); });
    });
    return;
  }
  if ((v = g('data-unign'))) {
    t.disabled = true;
    post(API + '/inbound/' + v + '/ignore', { ignore: false }).then(function(j) {
      if (!j.ok) { t.disabled = false; toast('エラー: ' + j.error); return; }
      toast('対象外を解除しました'); load();
    }).catch(function(e){ t.disabled = false; toast('通信エラー: ' + e.message); });
    return;
  }
});
load();`;
  res.send(pageShell('発注補助 — 入庫消込', 'inbound', body, script));
});

// ─── 画面: 発注残 (P13b) ───
router.get('/backorders', (req, res) => {
  const body = `
    <h2 class="page">発注残 <span class="muted" style="font-size:12px">発注→入荷・減数・取消の消込を管理 (アプリ発注分+NE移行分。切替までNE登録は継続)</span></h2>
    <div id="integrity"></div>
    <div class="tabbar" id="boTabs">
      <button data-view="attention" class="on">⚠️ 要対応</button>
      <button data-view="open">オープン</button>
      <button data-view="closed">完了</button>
      <span class="muted" id="boCount" style="margin-left:8px"></span>
    </div>
    <div id="boList">読み込み中…</div>
    <details class="sec" style="margin-top:16px"><summary style="cursor:pointer;padding:8px 12px">🔁 NE発注残の初期取込 (移行)</summary>
      <div class="bd">
        <div class="muted" style="font-size:12px;margin-bottom:6px">
          NEの発注残エクスポートCSV (発注伝票番号・商品コード・発注数・注残計・発行日・仕入先cd) を、NE伝票ごとに移行POとして取り込みます。
          残数はNEの注残計に合わせて開始し、以後の消込はこのアプリで行います。移行POは発注書メールの対象外です。
          取込済み伝票は自動スキップされるため、同じCSVを入れ直しても二重登録になりません。
        </div>
        <input type="file" id="neImpFile" accept=".csv,text/csv">
        <button class="ghost" id="neImpPrev">プレビュー</button>
        <div id="neImpOut" style="margin-top:8px"></div>
      </div>
    </details>`;
  const script = `
var API = '/apps/purchase-orders/api';
var DATA = null, VIEW = 'attention', OPENED = {};
var REASONS = { supplier_shortage: '仕入先都合 (作れない)', own_decision: '自社判断', cutoff: '打切', other: 'その他', correction: '訂正' };
var EVLABEL = { receipt: '📥入荷', shortage: '➖減数', cancel: '🚫取消', reversal: '↩逆仕訳' };

function fmtD(s) { return s ? String(s).slice(0, 10) : '—'; }
function newIdemKey() { return (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : 'k' + Date.now() + '-' + Math.random().toString(36).slice(2); }
function jsonOrErr(r, isWrite) {
  // 非JSON応答 (proxy 502等) でも状況を正しく伝える (書込=成否不明 / 取得=単なる失敗)
  return r.json().catch(function(){
    return { ok: false, error: 'HTTP ' + r.status + (isWrite ? ' — 登録されたか不明です。画面を更新して確認してください' : ' — 取得に失敗しました') };
  });
}
function post(url, body, idemKey) {
  var h = { 'Content-Type': 'application/json' };
  if (idemKey) h['Idempotency-Key'] = idemKey;
  return fetch(url, { method: 'POST', headers: h, body: JSON.stringify(body) }).then(function(r){ return jsonOrErr(r, true); });
}
function patch(url, body, idemKey) {
  var h = { 'Content-Type': 'application/json' };
  if (idemKey) h['Idempotency-Key'] = idemKey;
  return fetch(url, { method: 'PATCH', headers: h, body: JSON.stringify(body) }).then(function(r){ return jsonOrErr(r, true); });
}
function getJson(url) { return fetch(url).then(function(r){ return jsonOrErr(r, false); }); }
// 書込フロー共通の通信例外ハンドラ: ボタンを復帰させ「成否不明・同じ操作で安全に再試行可 (冪等キー保持)」を案内
function onWriteErr(btn) {
  return function(e) {
    if (btn) btn.disabled = false;
    toast('通信エラー: ' + e.message + ' — 登録されたか不明です。同じ内容のまま再実行すれば二重登録なしで確認できます');
  };
}

var LOAD_SEQ = 0;
function load() {
  var seq = ++LOAD_SEQ; // 連続操作でGETが重なっても、最新要求以外の応答は破棄 (古い状態への巻き戻り防止)
  getJson(API + '/backorders').then(function(j) {
    if (seq !== LOAD_SEQ) return;
    if (!j.ok) {
      document.getElementById('boList').innerHTML = '<div class="warn">エラー: ' + esc(j.error) + ' <button class="ghost" onclick="load()">再読込</button></div>';
      return;
    }
    DATA = j; render();
  }).catch(function(e) {
    if (seq !== LOAD_SEQ) return;
    document.getElementById('boList').innerHTML = '<div class="warn">通信エラー: ' + esc(e.message) + ' <button class="ghost" onclick="load()">再読込</button></div>';
  });
  getJson(API + '/ledger/integrity').then(function(j) {
    if (!j.ok) return;
    var el = document.getElementById('integrity');
    var h = '';
    if (j.issues.length) h += '<div class="warn">🚨 台帳の整合性違反 ' + j.issues.length + '件: ' + esc(j.issues.slice(0,5).map(function(x){ return x.kind + (x.orderId ? ' order:' + x.orderId : x.itemId ? ' item:' + x.itemId : ''); }).join(' / ')) + (j.issues.length > 5 ? ' …' : '') + '</div>';
    el.innerHTML = h;
  }).catch(function(){ /* 整合性表示は補助情報。失敗しても一覧は出す */ });
}

function orderMatchesView(o) {
  if (VIEW === 'closed') return !o.open;
  if (VIEW === 'open') return o.open;
  return o.open && o.attentionItems > 0; // 要対応
}

function badge(cls, text, title) { return '<span class="badge ' + cls + '"' + (title ? ' title="' + esc(title) + '"' : '') + '>' + esc(text) + '</span>'; }

function itemFlagBadges(i) {
  var h = '';
  if (i.flags.overdue) h += badge('b-issued', '🔴 納期超過', '納期 ' + fmtD(i.due) + ' を過ぎて残数あり');
  if (i.remaining_qty > 0 && i.flags.unanswered) h += badge('b-draft', '納期未回答');
  if (i.flags.needsDisposition) h += badge('b-draft', '⚠️ 残数の扱い未選択');
  if (i.flags.confirmOverdue) h += badge('b-issued', '⏰ 確認期限超過');
  return h;
}

function dispText(i) {
  if (i.remainder_disposition === 'awaiting_delivery') return '🚚 分納待ち ' + fmtD(i.next_expected_date) + ' ×' + (i.next_expected_qty || '?');
  if (i.remainder_disposition === 'awaiting_confirmation') return '📞 確認中 (期限 ' + fmtD(i.next_action_date) + ')';
  return '';
}

function render() {
  var list = DATA.orders.filter(orderMatchesView);
  var counts = { attention: 0, open: 0, closed: 0 };
  DATA.orders.forEach(function(o) {
    if (o.open) { counts.open++; if (o.attentionItems > 0) counts.attention++; } else counts.closed++;
  });
  document.querySelectorAll('#boTabs button').forEach(function(b) {
    var v = b.getAttribute('data-view');
    b.classList.toggle('on', v === VIEW);
    b.textContent = (v === 'attention' ? '⚠️ 要対応' : v === 'open' ? 'オープン' : '完了') + ' (' + counts[v] + ')';
  });
  var s = DATA.summary;
  document.getElementById('boCount').textContent =
    'オープン' + s.openOrders + '件 / 残数量 ' + s.remainingQty.toLocaleString('ja-JP') +
    ' / 既知単価分 ' + yen(s.knownAmount) + (s.unknownCostItems ? ' (単価未設定 ' + s.unknownCostItems + '明細)' : '');
  if (!DATA.boundary) {
    document.getElementById('boList').innerHTML = '<div class="muted">まだ発注残管理の対象がありません。次にアプリで発注確定した分から管理が始まります (既存のNE発注残は下の初期取込で登録できます)。</div>';
    return;
  }
  if (!list.length) {
    document.getElementById('boList').innerHTML = '<div class="muted">' + (VIEW === 'attention' ? '要対応の発注はありません 🎉' : '該当する発注はありません') + '</div>';
    return;
  }
  var h = '';
  list.forEach(function(o) { h += orderHtml(o); });
  document.getElementById('boList').innerHTML = h;
  // 展開状態を復元 (aria-expanded も同期、Codex R2 Medium)
  Object.keys(OPENED).forEach(function(id) {
    if (!OPENED[id]) return;
    var el = document.getElementById('items-' + id);
    if (el) el.style.display = '';
    var h2 = document.querySelector('[data-toggle="' + id + '"]');
    if (h2) h2.setAttribute('aria-expanded', 'true');
  });
}

function orderHtml(o) {
  var st = o.open
    ? (o.remainingQty > 0 && o.items.some(function(i){ return i.received_qty > 0; }) ? badge('b-draft', '一部入荷') : badge('b-issued', 'オープン'))
    : badge('b-issued', o.closeReason === 'manual' ? '完了 (打切あり)' : '完了');
  var flags = '';
  if (o.overdueItems) flags += badge('b-issued', '🔴 遅延 ' + o.overdueItems + '明細');
  else if (o.attentionItems) flags += badge('b-draft', '⚠️ 要対応 ' + o.attentionItems + '明細');
  // 仕入先名はリンクにしない (以前は発注ワークスペースへ飛んでいて「確定した明細が消えた」ように
  // 見える誤解を生んだ、中原さん報告 2026-07-13)。行クリック=このPOの確定明細を展開。ワークスペースへは展開部の🛒から
  var h = '<div class="sec"><h2 style="cursor:pointer" data-toggle="' + o.id + '" tabindex="0" role="button" aria-expanded="false" title="クリックでこの発注の明細 (確定時の内容) を開閉">' +
    esc(o.poNumber || ('#' + o.id)) + ' — ' + esc(o.supplierName) + ' ' +
    st + (o.origin === 'migration' ? ' ' + badge('b-draft', '🔁 NE移行分', 'NE発注残の初期取込で作成 (発注書メール対象外)') : '') + ' ' + flags +
    '<span class="muted" style="font-weight:normal;font-size:12px;margin-left:8px">発注 ' + fmtD(o.issuedAt) + ' / 希望納期 ' + fmtD(o.requestedDate) +
    ' / 残 ' + o.remainingQty.toLocaleString('ja-JP') + ' (' + yen(o.knownAmount) + ')' + (o.note ? ' / 📝' + esc(o.note) : '') + '</span></h2>' +
    '<div class="bd" id="items-' + o.id + '" style="display:none;max-height:none">';
  h += '<table class="t"><tr><th>商品</th><th class="r">発注</th><th class="r">入荷済</th><th class="r">減数</th><th class="r">取消</th><th class="r">残</th><th>納期 (希望→回答)</th><th>残数の扱い</th><th></th></tr>';
  o.items.forEach(function(i) {
    h += '<tr data-item="' + i.id + '">' +
      '<td><b>' + esc(i.product_code) + '</b><div class="muted" style="font-size:11px">' + esc(i.product_name || '') + '</div>' + itemFlagBadges(i) + '</td>' +
      '<td class="r">' + i.qty + '</td><td class="r">' + i.received_qty + '</td><td class="r">' + i.shortage_qty + '</td><td class="r">' + i.cancelled_qty + '</td>' +
      '<td class="r"><b>' + i.remaining_qty + '</b></td>' +
      '<td>' + fmtD(i.requested_date) + ' → <span data-pedit="' + i.id + '" style="cursor:pointer;text-decoration:underline dotted" title="クリックで回答納期を入力">' + fmtD(i.promised_date) + '</span></td>' +
      '<td>' + dispText(i) + '</td>' +
      '<td class="r" style="white-space:nowrap">' +
        (o.open && i.remaining_qty > 0
          ? '<button class="ghost" data-act="receipt" data-item2="' + i.id + '">📥入荷</button> ' +
            '<button class="ghost" data-act="shortage" data-item2="' + i.id + '">➖減数</button> ' +
            '<button class="ghost" data-act="cancel" data-item2="' + i.id + '">🚫取消</button> ' +
            '<button class="ghost" data-disp="' + i.id + '" title="分納待ち/減数で完了/確認中 を設定">📋扱い</button> '
          : '') +
        (i.event_count > 0 ? '<button class="ghost" data-hist="' + i.id + '">履歴</button>' : '') +
      '</td></tr>' +
      '<tr id="panel-' + i.id + '" style="display:none"><td colspan="9" id="panelBody-' + i.id + '"></td></tr>';
  });
  h += '</table>';
  h += '<div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap" id="closeArea-' + o.id + '">' +
    (!o.sendBlocked ? '<button class="ghost" data-emailui="' + o.id + '">📧 発注書メール</button>' : '') +
    (o.open ? '<button class="ghost" data-closeui="' + o.id + '">✅ 残数を打切って完了にする (手動クローズ)</button>' : '') +
    '<a class="ghost" href="/apps/purchase-orders/supplier/' + encodeURIComponent(o.supplierCode) + '" title="この発注とは別に、新しい発注を作る画面へ移動します">🛒 ' + esc(o.supplierName) + ' の発注画面へ (新しい発注作業)</a>' +
    '</div><div id="emailArea-' + o.id + '"></div>';
  h += '</div></div>';
  return h;
}

// ── 発注書メールパネル (プレビュー→送信確認→送信。dry-run/再送/再試行/照合) ──
function emailPanel(orderId) {
  var area = document.getElementById('emailArea-' + orderId);
  area.innerHTML = '<div class="muted">読み込み中…</div>';
  getJson(API + '/orders/' + orderId + '/email/preview').then(function(j) {
    if (!j.ok) { area.innerHTML = '<div class="warn">📧 送信できません: ' + esc(j.error) + '</div>'; return; }
    var dry = j.mode !== 'live';
    var h = '<div class="import-zone" style="margin-top:6px"><b>📧 発注書メール</b> ' +
      (dry ? '<span class="badge b-draft" title="宛先を社内アドレスに差し替えて送ります。本番切替はマスタ管理→メール設定">🔒 dry-run中 → ' + esc(j.dryrunTo || '未設定') + '</span>'
           : '<span class="badge b-issued">本番送信 (live)</span>') +
      (!j.envReady ? ' <span class="warn" style="display:inline-block;padding:2px 6px">⚠️ Gmail env未設定 (送信不可)</span>' : '') +
      '<table class="t" style="margin-top:6px">' +
      '<tr><th>宛先</th><td>' + esc(j.to.join(', ')) + (j.cc.length ? ' / CC: ' + esc(j.cc.join(', ')) : '') + '</td></tr>' +
      '<tr><th>件名</th><td>' + esc(j.subject) + '</td></tr>' +
      '<tr><th>添付</th><td>' + esc(j.attachmentName) + ' — ' + j.rows + '行 / 合計 ' + j.totalQty.toLocaleString('ja-JP') + '個 / ' + yen(j.totalAmount) +
        (j.vendorColUsed ? ' / 先方管理番号列つき' : '') +
        (j.missingVendorCodes.length ? ' <span class="badge b-issued" title="' + esc(j.missingVendorCodes.join(', ')) + '">⚠️ 先方番号なし ' + j.missingVendorCodes.length + '件</span>' : '') + '</td></tr>' +
      '</table>' +
      '<details style="margin-top:6px"><summary>本文と添付の内容を確認</summary>' +
      '<pre class="copy" style="max-height:200px;overflow:auto">' + esc(j.body) + '</pre>' +
      '<pre class="copy" style="max-height:160px;overflow:auto">' + esc(j.csvText) + '</pre></details>' +
      '<div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">' +
      '<button class="pri" id="emGo-' + orderId + '">' + (dry ? '📧 dry-run送信 (自分宛)' : '📧 今すぐ送信') + '</button>' +
      '<label class="muted">⏰ 日時指定 <input type="datetime-local" id="emWhen-' + orderId + '"></label>' +
      '<button class="ghost" id="emSched-' + orderId + '">⏰ 予約する</button>' +
      (j.jobs.some(function(x){ return x.status === 'sent' && !x.is_dry_run; }) ? '<button class="ghost" id="emResend-' + orderId + '">↪ 再送として送る</button>' : '') +
      '<button class="ghost" data-emclose="' + orderId + '">閉じる</button></div>';
    if (j.jobs.length) {
      h += '<table class="t" style="margin-top:8px"><tr><th>#</th><th>状態</th><th>宛先</th><th>日時</th><th>結果/エラー</th><th></th></tr>';
      j.jobs.forEach(function(x) {
        var st = x.status === 'sent' ? '✅ 送信済' + (x.is_dry_run ? ' (dry-run)' : '') :
          x.status === 'failed' ? '❌ 失敗' :
          x.status === 'unknown' ? '❓ 結果不明' :
          x.status === 'sending' ? '⏳ 送信中' :
          x.status === 'cancelled' ? '🚫 取消' :
          (x.scheduled_at ? '⏰ 予約 ' + esc(String(new Date(new Date(x.scheduled_at).getTime() + 9 * 3600000).toISOString()).slice(0, 16).replace('T', ' ')) + ' (JST)' : x.status);
        h += '<tr><td>' + x.id + (x.is_resend ? ' ↪' + (x.resend_of || '') : '') + (x.generation > 1 ? ' <span class="muted" title="送信試行の世代 (照合の競合検知に使用)">g' + x.generation + '</span>' : '') + '</td><td>' + st + '</td><td>' + esc(x.to_addr) + '</td>' +
          '<td class="muted" style="font-size:11px">' + esc(String(x.sent_at || x.created_at).slice(0, 16).replace('T', ' ')) + '</td>' +
          '<td style="font-size:11px">' + esc(x.gmail_message_id || x.error || '') + '</td>' +
          '<td style="white-space:nowrap">' +
            (x.status === 'failed' ? '<button class="ghost" data-emretry="' + x.id + '" data-emorder="' + orderId + '">再試行</button> ' : '') +
            ((x.status === 'queued' || x.status === 'failed') ? '<button class="ghost" data-emcancel="' + x.id + '" data-emorder="' + orderId + '">取消</button> ' : '') +
            ((x.status === 'sending' || x.status === 'unknown') ? '<button class="ghost" data-emrec="' + orderId + '">照合</button> ' : '') +
            (x.status === 'unknown' ? '<button class="ghost" data-emunsent="' + x.id + '" data-emorder="' + orderId + '" title="Gmailの送信済みに無いことを確認してから">未送信を確認した</button>' : '') +
          '</td></tr>';
      });
      h += '</table>';
    }
    h += '</div>';
    area.innerHTML = h;
    var idemKey = newIdemKey();
    var whenEl = document.getElementById('emWhen-' + orderId);
    if (whenEl) whenEl.addEventListener('input', function(){ idemKey = newIdemKey(); });
    var lastSent = j.jobs.filter(function(x){ return x.status === 'sent' && !x.is_dry_run; })[0] || null;
    function doSend(resend, scheduledAt) {
      var when = scheduledAt ? ' (予約: ' + scheduledAt.replace('T', ' ') + ' JST)' : '';
      var msg = dry
        ? 'dry-run送信します (宛先は ' + (j.dryrunTo || '未設定') + ' に差し替え)' + when + '。よろしいですか?'
        : '⚠️ 本番送信です。仕入先 ' + j.to.join(', ') + ' に発注書が届きます' + when + '。送信しますか?';
      if (!confirm(msg)) return;
      var btn = document.getElementById('emGo-' + orderId);
      if (btn) btn.disabled = true;
      post(API + '/orders/' + orderId + '/email/send',
        { resend: !!resend, resendOfJobId: resend && lastSent ? lastSent.id : null, scheduledAt: scheduledAt || null,
          expectedMode: j.mode }, idemKey)
        .then(function(r2) {
          if (btn) btn.disabled = false;
          if (!r2.ok) { toast('エラー: ' + r2.error); emailPanel(orderId); return; }
          toast(r2.status === 'sent' ? '送信しました' + (r2.replay ? ' (前回分を再表示・二重送信なし)' : '') :
            r2.status === 'scheduled' ? '予約しました (' + (r2.scheduledAt ? new Date(new Date(r2.scheduledAt).getTime() + 9 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' JST' : '') + ')' :
            r2.status === 'unknown' ? '⚠️ 送信結果が不明です。「照合」で確認してください' :
            '送信失敗: ' + (r2.error || ''));
          emailPanel(orderId);
        }).catch(onWriteErr(btn));
    }
    var go = document.getElementById('emGo-' + orderId);
    if (go) go.addEventListener('click', function(){ doSend(false, null); });
    var sc = document.getElementById('emSched-' + orderId);
    if (sc) sc.addEventListener('click', function() {
      var v = whenEl && whenEl.value;
      if (!v) { toast('予約日時を入力してください'); return; }
      doSend(false, v);
    });
    var rs = document.getElementById('emResend-' + orderId);
    if (rs) rs.addEventListener('click', function(){ doSend(true, whenEl && whenEl.value ? whenEl.value : null); });
  }).catch(function(e){ area.innerHTML = '<div class="warn">通信エラー: ' + esc(e.message) + '</div>'; });
}

// ── 消込入力パネル ──
function actPanel(itemId, act) {
  var item = findItem(itemId);
  if (!item) return;
  var today = new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10);
  var isShort = act === 'shortage';
  var label = act === 'receipt' ? '入荷' : isShort ? '減数 (作れなかった分の消込)' : '取消';
  var h = '<div class="import-zone" style="margin:6px 0"><b>' + EVLABEL[act] + ' — 残 ' + item.remaining_qty + '</b><div class="row" style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">' +
    '数量 <input type="number" id="evQty" min="1" max="' + item.remaining_qty + '" value="' + item.remaining_qty + '" style="width:90px">' +
    ' 日付 <input type="date" id="evDate" value="' + today + '">' +
    (isShort ? ' 理由 <select id="evReason"><option value="supplier_shortage">仕入先都合 (作れない)</option><option value="own_decision">自社判断</option><option value="other">その他</option></select>' : '') +
    ' メモ <input type="text" id="evNote" placeholder="' + (isShort ? '理由=その他は必須' : '任意') + '" style="width:180px">' +
    '</div>' +
    '<div id="remBox" style="margin-top:8px;display:none"><b>残りの扱い (必須)</b>: ' +
      '<label><input type="radio" name="rem" value="await_delivery" checked> 🚚 分納待ち</label> ' +
      '<label><input type="radio" name="rem" value="shortage"> ➖ 残りは減数で完了</label> ' +
      '<label><input type="radio" name="rem" value="await_confirmation"> 📞 確認中</label>' +
      '<span id="remFields" style="margin-left:8px"></span></div>' +
    '<div style="margin-top:8px"><button class="pri" id="evGo">登録する</button> <button class="ghost" id="evCancel">やめる</button> <span id="evPrev" class="muted"></span></div></div>';
  showPanel(itemId, h);
  var qtyEl = document.getElementById('evQty');
  function refresh() {
    var q = Number(qtyEl.value) || 0;
    var remain = item.remaining_qty - q;
    document.getElementById('evPrev').textContent = q > 0 && q <= item.remaining_qty ? ('→ 登録後の残数 ' + remain) : '';
    document.getElementById('remBox').style.display = (remain > 0 && q > 0) ? '' : 'none';
    remFields();
  }
  function remFields() {
    var v = (document.querySelector('input[name=rem]:checked') || {}).value;
    var q = Number(qtyEl.value) || 0;
    var remain = item.remaining_qty - q;
    var el = document.getElementById('remFields');
    if (v === 'await_delivery') el.innerHTML = '次回予定日 <input type="date" id="remDate"> 数量 <input type="number" id="remQty" min="1" max="' + remain + '" value="' + remain + '" style="width:80px">';
    else if (v === 'shortage') el.innerHTML = '理由 <select id="remReason"><option value="supplier_shortage">仕入先都合</option><option value="own_decision">自社判断</option><option value="other">その他</option></select> <input type="text" id="remNote" placeholder="メモ (その他は必須)" style="width:160px">';
    else el.innerHTML = '確認期限 <input type="date" id="remActDate">';
  }
  // 冪等キー: パネルを開いた時点で発行し、同じ内容の再試行 (通信断後の再クリック) では同一キーを使う。
  // 入力を変えたらキーを更新する (同一キー+異内容は409になるため)
  var idemKey = newIdemKey();
  document.getElementById('panelBody-' + itemId).addEventListener('input', function(){ idemKey = newIdemKey(); });
  qtyEl.addEventListener('input', refresh);
  document.querySelectorAll('input[name=rem]').forEach(function(x){ x.addEventListener('change', remFields); });
  refresh();
  document.getElementById('evCancel').addEventListener('click', function(){ hidePanel(itemId); });
  document.getElementById('evGo').addEventListener('click', function() {
    var q = Number(qtyEl.value);
    var body = { type: act, qty: q, effectiveDate: document.getElementById('evDate').value,
      note: document.getElementById('evNote').value,
      reasonCode: isShort ? document.getElementById('evReason').value : null };
    if (item.remaining_qty - q > 0) {
      var v = (document.querySelector('input[name=rem]:checked') || {}).value;
      if (v === 'await_delivery') body.remainder = { action: 'await_delivery', nextExpectedDate: (document.getElementById('remDate') || {}).value, nextExpectedQty: (document.getElementById('remQty') || {}).value };
      else if (v === 'shortage') body.remainder = { action: 'shortage', reasonCode: (document.getElementById('remReason') || {}).value, note: (document.getElementById('remNote') || {}).value };
      else body.remainder = { action: 'await_confirmation', nextActionDate: (document.getElementById('remActDate') || {}).value };
    }
    var btn = document.getElementById('evGo');
    btn.disabled = true;
    post(API + '/items/' + itemId + '/events', body, idemKey).then(function(j) {
      btn.disabled = false;
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      toast(label + 'を登録しました (残 ' + j.remaining + (j.orderClosed ? '、発注完了' : '') + (j.replay ? ' ※前回の登録を再表示 (二重登録なし)' : '') + ')');
      load();
    }).catch(function(e){ btn.disabled = false; toast('通信エラー: ' + e.message + ' — 再クリックで安全に再試行できます'); });
  });
}

// ── 残数の扱い (三択) の単独設定パネル (逆仕訳後・後から変更する場合) ──
function dispPanel(itemId) {
  var item = findItem(itemId);
  if (!item) return;
  var cur = item.remainder_disposition;
  var h = '<div class="import-zone" style="margin:6px 0"><b>📋 残 ' + item.remaining_qty + ' の扱い</b>' +
    '<div style="margin-top:6px">' +
    '<label><input type="radio" name="dsp" value="await_delivery"' + (cur === 'awaiting_delivery' || !cur ? ' checked' : '') + '> 🚚 分納待ち</label> ' +
    '<label><input type="radio" name="dsp" value="shortage"> ➖ 減数で完了 (もう来ない)</label> ' +
    '<label><input type="radio" name="dsp" value="await_confirmation"' + (cur === 'awaiting_confirmation' ? ' checked' : '') + '> 📞 確認中</label>' +
    '<span id="dspFields" style="margin-left:8px"></span></div>' +
    '<div style="margin-top:8px"><button class="pri" id="dspGo">保存</button> <button class="ghost" id="dspCancel">やめる</button></div></div>';
  showPanel(itemId, h);
  var idemKey = newIdemKey();
  document.getElementById('panelBody-' + itemId).addEventListener('input', function(){ idemKey = newIdemKey(); });
  function fields() {
    var v = (document.querySelector('input[name=dsp]:checked') || {}).value;
    var el = document.getElementById('dspFields');
    if (v === 'await_delivery') el.innerHTML = '次回予定日 <input type="date" id="dspDate" value="' + (item.next_expected_date || '') + '"> 数量 <input type="number" id="dspQty" min="1" max="' + item.remaining_qty + '" value="' + (item.next_expected_qty || item.remaining_qty) + '" style="width:80px">';
    else if (v === 'shortage') el.innerHTML = '理由 <select id="dspReason"><option value="supplier_shortage">仕入先都合</option><option value="own_decision">自社判断</option><option value="other">その他</option></select> <input type="text" id="dspNote" placeholder="メモ (その他は必須)" style="width:160px">';
    else el.innerHTML = '確認期限 <input type="date" id="dspActDate" value="' + (item.next_action_date || '') + '">';
  }
  document.querySelectorAll('input[name=dsp]').forEach(function(x){ x.addEventListener('change', fields); });
  fields();
  document.getElementById('dspCancel').addEventListener('click', function(){ hidePanel(itemId); });
  document.getElementById('dspGo').addEventListener('click', function() {
    var v = (document.querySelector('input[name=dsp]:checked') || {}).value;
    var btn = document.getElementById('dspGo');
    btn.disabled = true;
    var done = function(j, msg) {
      btn.disabled = false;
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      toast(msg); load();
    };
    if (v === 'shortage') {
      post(API + '/items/' + itemId + '/events', {
        type: 'shortage', qty: item.remaining_qty,
        reasonCode: (document.getElementById('dspReason') || {}).value,
        note: (document.getElementById('dspNote') || {}).value,
      }, idemKey).then(function(j){ done(j, '残数を減数で消し込みました'); }).catch(onWriteErr(btn));
    } else if (v === 'await_delivery') {
      patch(API + '/items/' + itemId + '/plan', {
        disposition: 'awaiting_delivery',
        nextExpectedDate: (document.getElementById('dspDate') || {}).value,
        nextExpectedQty: (document.getElementById('dspQty') || {}).value,
      }, idemKey).then(function(j){ done(j, '分納待ちに設定しました'); }).catch(onWriteErr(btn));
    } else {
      patch(API + '/items/' + itemId + '/plan', {
        disposition: 'awaiting_confirmation',
        nextActionDate: (document.getElementById('dspActDate') || {}).value,
      }, idemKey).then(function(j){ done(j, '確認中に設定しました'); }).catch(onWriteErr(btn));
    }
  });
}

// ── 明細イベント履歴 (+逆仕訳) ──
var HIST_SEQ = 0;
function histPanel(itemId) {
  var seq = ++HIST_SEQ; // 古い応答が後から届いて別明細のパネルを上書きしないようにする
  getJson(API + '/items/' + itemId + '/events').then(function(j) {
    if (seq !== HIST_SEQ) return;
    if (!j.ok) { toast(j.error); return; }
    var h = '<table class="t"><tr><th>#</th><th>種別</th><th class="r">数量</th><th>業務日付</th><th>理由/メモ</th><th>登録</th><th></th></tr>';
    j.events.forEach(function(e) {
      var dead = e.reversed_by != null;
      var canRev = !dead && e.event_type !== 'reversal';
      h += '<tr' + (dead ? ' style="opacity:.5;text-decoration:line-through"' : '') + '><td>' + e.id + '</td>' +
        '<td>' + EVLABEL[e.event_type] + (e.event_type === 'reversal' ? ' → #' + e.reverses_id : '') + (dead ? ' (逆仕訳済)' : '') + '</td>' +
        '<td class="r">' + e.qty + '</td><td>' + esc(e.effective_date) + '</td>' +
        '<td>' + esc((REASONS[e.reason_code] || e.reason_code || '') + (e.note ? ' / ' + e.note : '')) + '</td>' +
        '<td class="muted" style="font-size:11px">' + esc(String(e.recorded_at).slice(0, 16).replace('T', ' ')) + ' ' + esc(e.actor || '') + '</td>' +
        '<td>' + (canRev ? '<button class="ghost" data-rev="' + e.id + '" data-revitem="' + itemId + '">↩逆仕訳</button>' : '') + '</td></tr>';
    });
    h += '</table><div id="revBox-' + itemId + '" style="margin-top:6px"></div><button class="ghost" data-histclose="' + itemId + '">閉じる</button>';
    showPanel(itemId, h);
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
}

function findItem(itemId) {
  var found = null;
  DATA.orders.forEach(function(o){ o.items.forEach(function(i){ if (i.id === Number(itemId)) found = i; }); });
  return found;
}
function showPanel(itemId, html) {
  HIST_SEQ++; // 取得中の履歴応答が新しいパネルを上書きしないよう、パネル切替でも世代を進める
  // 他明細のパネルDOMは中身ごと破棄する (evQty等のIDとradio name=rem が重複して誤送信するのを防ぐ)
  document.querySelectorAll('tr[id^=panel-]').forEach(function(tr){ tr.style.display = 'none'; });
  document.querySelectorAll('td[id^=panelBody-]').forEach(function(td){ if (td.id !== 'panelBody-' + itemId) td.innerHTML = ''; });
  document.getElementById('panelBody-' + itemId).innerHTML = html;
  document.getElementById('panel-' + itemId).style.display = '';
}
function hidePanel(itemId) { HIST_SEQ++; document.getElementById('panel-' + itemId).style.display = 'none'; }

function toggleOrder(v) {
  var el = document.getElementById('items-' + v);
  OPENED[v] = el.style.display === 'none';
  el.style.display = OPENED[v] ? '' : 'none';
  var h2 = document.querySelector('[data-toggle="' + v + '"]');
  if (h2) h2.setAttribute('aria-expanded', OPENED[v] ? 'true' : 'false');
}
// 見出しはEnter/Spaceでも開閉できるようにする (仕入先名<a>廃止でキーボード導線が残るように、Codex R1 Medium)
document.addEventListener('keydown', function(ev) {
  if (ev.key !== 'Enter' && ev.key !== ' ') return;
  var tg = ev.target.closest && ev.target.closest('[data-toggle]');
  if (tg) { ev.preventDefault(); toggleOrder(tg.getAttribute('data-toggle')); }
});
document.addEventListener('click', function(ev) {
  var t = ev.target;
  var g = function(a){ return t.getAttribute && t.getAttribute(a); };
  var v;
  if ((v = g('data-view'))) { VIEW = v; render(); return; }
  // 見出し内の子要素 (badge/日付span等) クリックでも展開する (Codex R1 Medium)。
  // 見出し内の操作要素 (回答納期編集span等の data-* 持ち) は除外して個別ハンドラに委ねる
  if (!g('data-pedit') && !g('data-emailui') && !g('data-closeui') && t.tagName !== 'A' && t.tagName !== 'BUTTON') {
    var tgl = t.closest && t.closest('[data-toggle]');
    if (tgl) { toggleOrder(tgl.getAttribute('data-toggle')); return; }
  }
  if ((v = g('data-toggle'))) {
    toggleOrder(v);
    return;
  }
  if ((v = g('data-act'))) { actPanel(g('data-item2'), v); return; }
  if ((v = g('data-disp'))) { dispPanel(v); return; }
  if ((v = g('data-hist'))) { histPanel(v); return; }
  if ((v = g('data-emailui'))) { emailPanel(v); return; }
  if ((v = g('data-emclose'))) { document.getElementById('emailArea-' + v).innerHTML = ''; return; }
  if ((v = g('data-emretry'))) {
    t.disabled = true;
    var emOrder = g('data-emorder');
    post(API + '/email-jobs/' + v + '/retry', {}).then(function(j) {
      toast(j.ok ? (j.status === 'sent' ? '送信しました' : '送信失敗: ' + (j.error || '')) : 'エラー: ' + j.error);
      emailPanel(emOrder);
    }).catch(onWriteErr(t));
    return;
  }
  if ((v = g('data-emrec'))) {
    t.disabled = true;
    post(API + '/email/reconcile', {}).then(function(j) {
      toast(j.ok ? '照合しました (' + j.checked + '件)' : 'エラー: ' + j.error);
      emailPanel(v);
    }).catch(onWriteErr(t));
    return;
  }
  if ((v = g('data-emcancel'))) {
    if (!confirm('この送信ジョブを取り消しますか?')) return;
    t.disabled = true;
    post(API + '/email-jobs/' + v + '/cancel', {}).then(function(j) {
      toast(j.ok ? '取り消しました' : 'エラー: ' + j.error);
      emailPanel(g('data-emorder'));
    }).catch(onWriteErr(t));
    return;
  }
  if ((v = g('data-emunsent'))) {
    if (!confirm('Gmailの「送信済み」を確認し、このメールが存在しないことを確認しましたか?\\n(存在するのに再試行すると二重送信になります)')) return;
    t.disabled = true;
    post(API + '/email-jobs/' + v + '/mark-unsent', { confirm: '未送信' }).then(function(j) {
      toast(j.ok ? '再試行できるようになりました (queued)' : 'エラー: ' + j.error);
      emailPanel(g('data-emorder'));
    }).catch(onWriteErr(t));
    return;
  }
  if ((v = g('data-histclose'))) { hidePanel(v); return; }
  // 逆仕訳: 理由入力 → 確定 (promptは使わない)
  if ((v = g('data-rev'))) {
    var itemId = g('data-revitem');
    var box = document.getElementById('revBox-' + itemId);
    box.innerHTML = '<b>↩ イベント#' + v + ' を逆仕訳</b> (全量が打ち消され、残数が戻ります) ' +
      '理由 <input type="text" id="revNote" placeholder="例: 数量の入力ミス" style="width:220px"> ' +
      '<button class="pri" id="revGo">逆仕訳を実行</button>';
    var revKey = newIdemKey();
    document.getElementById('revGo').addEventListener('click', function() {
      var note = document.getElementById('revNote').value;
      if (!note.trim()) { toast('訂正理由を入力してください'); return; }
      var btn = this; btn.disabled = true;
      post(API + '/events/' + v + '/reverse', { note: note }, revKey).then(function(j) {
        if (!j.ok) { btn.disabled = false; toast('エラー: ' + j.error); return; }
        toast('逆仕訳しました (残 ' + j.remaining + ')' + (j.needsDisposition ? ' — ⚠️ 残数の扱い (分納待ち/減数/確認中) を選択してください' : ''));
        load();
      }).catch(onWriteErr(btn));
    });
    return;
  }
  // 手動クローズ: 理由入力 → 確定
  if ((v = g('data-closeui'))) {
    var area = document.getElementById('closeArea-' + v);
    area.innerHTML = '<div class="import-zone"><b>✅ 手動クローズ</b> — 残数すべてを「打切」の減数として消し込み、この発注を完了にします。' +
      '<div style="margin-top:6px">理由 <input type="text" id="clNote" placeholder="例: 仕入先廃番のため" style="width:260px"> ' +
      '<button class="pri" id="clGo">クローズ実行</button> <button class="ghost" id="clCancel">やめる</button></div></div>';
    var clKey = newIdemKey();
    document.getElementById('clCancel').addEventListener('click', load);
    document.getElementById('clGo').addEventListener('click', function() {
      var note = document.getElementById('clNote').value;
      if (!note.trim()) { toast('理由を入力してください'); return; }
      var btn = this; btn.disabled = true;
      post(API + '/orders/' + v + '/close', { note: note }, clKey).then(function(j) {
        if (!j.ok) { btn.disabled = false; toast('エラー: ' + j.error); return; }
        toast('クローズしました (打切 ' + j.cutoffItems + '明細)');
        load();
      }).catch(onWriteErr(btn));
    });
    return;
  }
  // 回答納期の編集
  if ((v = g('data-pedit'))) {
    var item = findItem(v);
    var cell = t.parentNode;
    cell.innerHTML = fmtD(item.requested_date) + ' → <input type="date" id="pd-' + v + '" value="' + (item.promised_date || '') + '" style="width:130px"> ' +
      '<button class="pri" data-pdsave="' + v + '" data-pdkey="' + newIdemKey() + '">保存</button>';
    return;
  }
  if ((v = g('data-pdsave'))) {
    var val = document.getElementById('pd-' + v).value;
    t.disabled = true;
    patch(API + '/items/' + v + '/plan', { promisedDate: val || '' }, g('data-pdkey')).then(function(j) {
      if (!j.ok) { t.disabled = false; toast('エラー: ' + j.error); return; }
      toast('回答納期を保存しました');
      load();
    }).catch(onWriteErr(t));
    return;
  }
});

// ── NE発注残の初期取込 (プレビュー → 取込実行の2段階) ──
// プレビュー応答の fileHash (CSV本体) と planHash (DB由来の確定内容) を保持して確定時にサーバへ渡す
// (プレビュー後のファイル差し替え・DB変化を検出)。ファイル選択が変わったらプレビュー結果を無効化し、
// 世代カウンタで遅延応答 (旧ファイルのプレビュー結果) を破棄する (Codex R2 Low)
var NE_IMP_HASH = null, NE_IMP_PLAN = null, NE_IMP_GEN = 0;
function neImpPost(commit) {
  var f = document.getElementById('neImpFile').files[0];
  if (!f) { toast('CSVファイルを選んでください'); return; }
  if (commit && (!NE_IMP_HASH || !NE_IMP_PLAN)) { toast('先にプレビューで内容を確認してください'); return; }
  var gen = NE_IMP_GEN;
  var fd = new FormData();
  fd.append('file', f);
  if (commit) { fd.append('commit', '1'); fd.append('fileHash', NE_IMP_HASH); fd.append('planHash', NE_IMP_PLAN); }
  var out = document.getElementById('neImpOut');
  out.innerHTML = '<div class="muted">' + (commit ? '取込中…' : '確認中…') + '</div>';
  fetch(API + '/backorders/ne-import', { method: 'POST', body: fd }).then(function(r){ return jsonOrErr(r, commit); }).then(function(j) {
    if (gen !== NE_IMP_GEN) return; // ファイルが変わった後に届いた旧応答は破棄
    if (!j.ok) { NE_IMP_HASH = null; NE_IMP_PLAN = null; out.innerHTML = '<div class="warn" style="white-space:pre-wrap">' + esc(j.error) + '</div>'; return; }
    if (!j.commit) { NE_IMP_HASH = j.fileHash; NE_IMP_PLAN = j.planHash; }
    var h = '';
    if (j.commit) {
      h += j.created.length
        ? '<div><b>✅ 取り込みました</b>: 新規 ' + j.created.length + '伝票 / 明細 ' + j.items + ' / 残数計 ' + j.totalRemaining.toLocaleString('ja-JP') + (j.skipped.length ? ' / 取込済みスキップ ' + j.skipped.length + '件' : '') + '</div>'
        : '<div><b>新規取込なし</b> (全 ' + j.skipped.length + '伝票が取込済みでした)</div>';
    } else {
      h += '<div><b>プレビュー</b> (まだ登録されていません): 伝票 ' + j.orders + '件 / 明細 ' + j.items + ' / 残数計 ' + j.totalRemaining.toLocaleString('ja-JP') + '</div>';
    }
    if (j.skipped.length) h += '<div class="muted">取込済みスキップ: ' + esc(j.skipped.map(function(s){ return s.slip + '→' + (s.poNumber || ''); }).join(', ')) + '</div>';
    if (j.slips.length) {
      h += '<table class="t" style="margin-top:6px"><tr><th>NE伝票</th><th>仕入先</th><th>NE発行日</th><th class="r">明細</th><th class="r">残数</th><th class="r">金額 (既知単価分)</th></tr>';
      j.slips.forEach(function(s) {
        h += '<tr><td>' + esc(s.slip) + '</td><td>' + esc(s.supplierName) + '</td><td>' + esc(s.neDate) + '</td><td class="r">' + s.items + '</td><td class="r">' + s.remaining.toLocaleString('ja-JP') + '</td><td class="r">' + yen(s.knownAmount) + '</td></tr>';
      });
      h += '</table>';
    }
    if (j.warnings.length) h += '<div class="warn" style="margin-top:6px;white-space:pre-wrap;max-height:200px;overflow:auto">⚠️ ' + j.warnings.length + '件\\n' + esc(j.warnings.slice(0, 20).join('\\n')) + (j.warnings.length > 20 ? '\\n…他' + (j.warnings.length - 20) + '件' : '') + '</div>';
    if (!j.commit && j.slips.length) h += '<div style="margin-top:8px"><button class="pri" id="neImpGo">この内容で取込実行 (' + j.orders + '伝票)</button></div>';
    out.innerHTML = h;
    var go = document.getElementById('neImpGo');
    if (go) go.addEventListener('click', function(){ go.disabled = true; neImpPost(true); });
    if (j.commit) load();
  }).catch(function(e){
    if (gen !== NE_IMP_GEN) return;
    NE_IMP_HASH = null; NE_IMP_PLAN = null;
    out.innerHTML = '<div class="warn">通信エラー: ' + esc(e.message) + (commit ? ' — 取込済み伝票は自動スキップされるため、もう一度プレビュー→取込実行しても二重登録になりません' : '') + '</div>';
  });
}
document.getElementById('neImpPrev').addEventListener('click', function(){ neImpPost(false); });
document.getElementById('neImpFile').addEventListener('change', function(){
  NE_IMP_GEN++;
  NE_IMP_HASH = null; NE_IMP_PLAN = null;
  document.getElementById('neImpOut').innerHTML = '';
});
load();`;
  res.send(pageShell('発注補助 — 発注残', 'backorders', body, script));
});

router.get('/orders', (req, res) => {
  const body = `
    <h2 class="page">発注履歴</h2>
    <div class="toolbar">
      <label>並び順 <select id="ordSort">
        <option value="new">新しい順</option>
        <option value="supplier">仕入先ごと (各仕入先内は新しい順)</option>
      </select></label>
      <label>仕入先 <select id="ordSup"><option value="">すべて</option></select></label>
      <span class="muted" id="ordCount"></span>
    </div>
    <div class="sec"><div class="bd" id="list">読み込み中…</div></div>
    <div class="sec" id="detail" style="display:none"><h2 id="detailTitle"></h2><div class="bd" id="detailBody"></div></div>`;
  const script = `
var ORDERS = [];
function render() {
  var sort = document.getElementById('ordSort').value;
  var fsup = document.getElementById('ordSup').value;
  var list = ORDERS.filter(function(o){ return !fsup || String(o.supplier_code) === fsup; });
  document.getElementById('ordCount').textContent = list.length + ' 件';
  if (!list.length) { document.getElementById('list').innerHTML = '<div class="muted">該当する履歴はありません</div>'; return; }
  if (sort === 'supplier') {
    // グループの順序 = その仕入先の最新発注が新しい順 (サーバ応答が新しい順なので出現順を維持)
    var groups = new Map();
    list.forEach(function(o) {
      var k = String(o.supplier_code);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(o);
    });
    var h = '';
    groups.forEach(function(arr) {
      h += '<tr><td colspan="10" style="background:#f1f5f9;font-weight:600">🏭 ' + esc(arr[0].supplier_name) + ' <span class="muted" style="font-weight:400">(' + arr.length + '件)</span></td></tr>';
      arr.forEach(function(o){ h += rowHtml(o); });
    });
    document.getElementById('list').innerHTML = tableHead() + h + '</table>';
  } else {
    document.getElementById('list').innerHTML = tableHead() + list.map(rowHtml).join('') + '</table>';
  }
}
function tableHead() {
  return '<table class="t"><tr><th>PO番号</th><th>状態</th><th>仕入先</th><th class="r">SKU</th><th class="r">数量</th><th class="r">発注残</th><th class="r">金額(原価)</th><th>確定日時</th><th>メモ</th><th></th></tr>';
}
function rowHtml(o) {
  var st;
  if (o.status !== 'issued') st = '<span class="badge b-draft">下書き</span>';
  else if (o.tracking_mode !== 'tracked') st = '<span class="badge b-issued">確定</span>';
  else if (o.closed_at) st = '<span class="badge b-issued">完了</span>';
  else st = '<span class="badge b-draft">発注残 ' + o.remaining_qty + '</span>';
  var when = o.issued_at ? new Date(o.issued_at).toLocaleString('ja-JP') : '—';
  // 仕入先名クリック=このPOの明細 (以前はワークスペース行きで「確定した内容と違う」誤解を生んだ)
  var dtitle = o.status === 'issued' ? 'この発注の明細 (確定時の内容) を表示' : 'この下書きの明細 (未確定・保存で変わる) を表示';
  return '<tr><td>' + esc(o.po_number || ('#' + o.id)) + '</td><td>' + st + '</td>' +
    '<td><a data-id="' + o.id + '" tabindex="0" role="button" style="cursor:pointer" title="' + dtitle + '">' + esc(o.supplier_name) + '</a></td>' +
    '<td class="r">' + o.sku_count + '</td><td class="r">' + o.total_qty.toLocaleString('ja-JP') + '</td>' +
    '<td class="r">' + (o.tracking_mode === 'tracked' ? o.remaining_qty.toLocaleString('ja-JP') : '—') + '</td>' +
    '<td class="r">' + yen(o.total_amount) + '</td>' +
    '<td>' + when + '</td><td>' + esc(o.note || '') + '</td>' +
    '<td><button class="ghost" data-id="' + o.id + '">明細</button></td></tr>';
}
function load() {
  fetch('/apps/purchase-orders/api/orders').then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { document.getElementById('list').textContent = 'エラー: ' + j.error; return; }
    ORDERS = j.orders || [];
    if (!ORDERS.length) { document.getElementById('list').innerHTML = '<div class="muted">履歴はまだありません</div>'; return; }
    // 仕入先フィルタの選択肢 (履歴に登場する仕入先のみ、名前順)
    var seen = new Map();
    ORDERS.forEach(function(o){ if (!seen.has(String(o.supplier_code))) seen.set(String(o.supplier_code), o.supplier_name); });
    var opts = [...seen.entries()].sort(function(a, b){ return String(a[1]).localeCompare(String(b[1]), 'ja'); });
    var sel = document.getElementById('ordSup');
    var cur = sel.value;
    sel.innerHTML = '<option value="">すべて</option>' + opts.map(function(e) {
      return '<option value="' + esc(e[0]) + '">' + esc(e[1]) + '</option>';
    }).join('');
    sel.value = cur || '';
    render();
  });
}
document.getElementById('ordSort').addEventListener('change', render);
document.getElementById('ordSup').addEventListener('change', render);
// 仕入先名の疑似リンクはEnter/Spaceでも開ける (Codex R1 Low)。
// 「明細」ボタンはブラウザ標準のclick発火に任せる (keydownと二重実行しない、Codex R2 Low)
document.addEventListener('keydown', function(ev) {
  if (ev.key !== 'Enter' && ev.key !== ' ') return;
  if (ev.target.tagName === 'BUTTON') return;
  var id = ev.target.getAttribute && ev.target.getAttribute('data-id');
  if (id) { ev.preventDefault(); openDetail(id); }
});
document.addEventListener('click', function(ev) {
  var id = ev.target.getAttribute && ev.target.getAttribute('data-id');
  if (!id) return;
  openDetail(id);
});
function openDetail(id) {
  fetch('/apps/purchase-orders/api/orders/' + id).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast(j.error); return; }
    var o = j.order;
    var lines = o.items.map(function(i){ return i.product_code + '\\t' + (i.product_name || '') + '\\t' + i.qty; });
    var text = lines.join('\\n');
    // 下書きは未確定 =「確定時の明細」と表示しない (保存のたびに変わる内容、Codex R1 Medium)
    document.getElementById('detailTitle').textContent = (o.po_number || ('発注 #' + o.id)) + ' — ' + o.supplier_name +
      (o.status === 'issued' ? ' (確定時の明細)' : ' (下書き明細 — 未確定)');
    document.getElementById('detailBody').innerHTML =
      '<button class="pri" id="btnCopyDetail">📋 リストをコピー</button> <button id="btnCsvDetail">⬇ CSVダウンロード</button>' +
      '<a class="ghost" href="/apps/purchase-orders/supplier/' + encodeURIComponent(o.supplier_code) + '" title="この発注とは別に、新しい発注を作る画面へ移動します">🛒 ' + esc(o.supplier_name) + ' の発注画面へ (新しい発注作業)</a>' +
      '<pre class="copy">' + esc('商品コード\\t商品名\\t数量\\n' + text) + '</pre>';
    document.getElementById('detail').style.display = '';
    document.getElementById('detail').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    document.getElementById('btnCopyDetail').addEventListener('click', function() {
      navigator.clipboard.writeText(text).then(function(){ toast('コピーしました'); });
    });
    document.getElementById('btnCsvDetail').addEventListener('click', function() {
      var rows = [['商品コード', '商品名', '数量', '原価', '金額']];
      o.items.forEach(function(i) {
        rows.push([i.product_code, i.product_name || '', i.qty, i.unit_cost != null ? i.unit_cost : '', i.unit_cost != null ? Math.round(i.qty * i.unit_cost) : '']);
      });
      dlCsv('発注_' + o.supplier_name + '_#' + o.id + '.csv', rows);
    });
  });
}
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
        対応: <b>商品マスタ</b>（→ 仕入先＋商品紐付け）／ <b>発注条件マスタ</b> ／ <b>原料グループマスタ</b> ／ <b>選べるセット構成</b>（商品コード＋セット名＋最低在庫数）。まとめて複数選択もできます。
      </div>
      <form id="importForm" class="row">
        <input type="file" name="files" accept=".csv" multiple required>
        <button type="submit" class="pri">取り込む</button>
        <span id="importStatus" class="muted"></span>
      </form>
      <div id="importResult" class="pill-row"></div>
    </div>

    <div class="import-zone">
      <h3>📧 発注書メール設定</h3>
      <div class="hint">
        発注残ページの「📧発注書メール」から送信します。<b>既定は dry-run</b> (宛先を下の社内アドレスに差し替えて送信) — 内容を数回確認してから live に切り替えてください。<br>
        宛先は仕入先マスタの「発注書メール宛先」列 (下の宛先マスタCSVで一括登録可)。アメージングクラフト/ビーフリーは対応表を取り込むと添付CSVに先方管理番号列が付きます。<br>
        対応表の1件ずつの追加・修正・削除は下のタブ「📇 先方番号対応表」でできます (CSV取込は<b>仕入先ごと全置換</b>なので注意)。
      </div>
      <div class="row" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:6px">
        <label>モード <select id="emMode"><option value="dry_run">dry_run (社内宛て)</option><option value="live">live (本番送信)</option></select></label>
        <span id="emLiveWrap" style="display:none">確認 <input id="emLiveConfirm" placeholder="LIVE と入力" style="width:100px"></span>
        <button class="ghost" id="emModeApply">モード切替を適用</button>
        <label>dry-run宛先 <input type="text" id="emDryTo" placeholder="自分のメールアドレス" style="width:220px"></label>
        <span id="emEnv" class="muted"></span>
        <button class="ghost" id="emTplToggle">テンプレ編集</button>
        <button class="pri" id="emSave">宛先/テンプレを保存</button>
        <span id="emStatus" class="muted"></span>
      </div>
      <div id="emTpl" style="display:none;margin-top:6px">
        <div>件名テンプレ <input type="text" id="emSubject" style="width:420px"></div>
        <div style="margin-top:4px">本文テンプレ (変数: {{date}} {{name}} {{contact}} {{po_number}} {{nouki}}=希望納期)<br>
        <textarea id="emBody" rows="10" style="width:100%;max-width:640px"></textarea></div>
      </div>
      <div class="row" style="display:flex;gap:16px;flex-wrap:wrap;margin-top:10px;align-items:flex-end">
        <form id="recipForm">
          <div class="hint">宛先マスタCSV (スプシ「仕入先ごとの発注メール送信先一覧」の生DL。仕入先名称で突合)</div>
          <input type="file" name="file" accept=".csv" required>
          <button type="submit">📥 宛先マスタ取込</button>
        </form>
        <form id="vmapForm">
          <div class="hint">先方管理番号 対応表CSV (見出し「仕入先管理番号」「弊社管理番号」)</div>
          仕入先コード <input type="text" name="supplier_code" style="width:80px" required>
          <input type="file" name="file" accept=".csv" required>
          <label class="muted"><input type="checkbox" name="positional" value="1"> 見出しなし (A列/C列)</label>
          <button type="submit">📥 対応表取込</button>
        </form>
        <span id="vmapNow" class="muted"></span>
      </div>
      <div id="emResult" class="pill-row"></div>
    </div>

    <div class="tabbar">
      <button data-tab="suppliers" class="on">仕入先</button>
      <button data-tab="conditions">発注条件グループ</button>
      <button data-tab="materials">原料グループ</button>
      <button data-tab="attrs">商品紐付け</button>
      <button data-tab="selectable">🧩 選べるセット構成</button>
      <button data-tab="vendormap">📇 先方番号対応表</button>
      <button data-tab="unlinked">🆕 未紐付けの新商品</button>
    </div>
    <div class="sec"><div class="bd" id="tabBody">読み込み中…</div></div>`;
  const script = `
var TAB = 'suppliers';
// ro=読み取り専用の表示列 (サーバが名前解決)。dl=グループをID/名前どちらでも入力できるオートコンプリート
var DEFS = {
  suppliers: { title: '仕入先', cols: [
    { k: 'supplier_code', l: '仕入先コード', pk: 1 }, { k: 'name', l: '仕入先名' }, { k: 'order_memo', l: '発注メモ (FAX/WEB/送料条件等)' },
    { k: 'email_to', l: '発注書メール宛先 (カンマ区切り可)' }, { k: 'email_cc', l: 'CC' },
    { k: 'contact_name', l: '担当者名 (様は自動付与)' },
    { k: 'send_method', l: '発注方法', sel: [['', '未設定'], ['email', '📧 メール'], ['fax', '📠 FAX'], ['web', '🌐 WEBサイト'], ['none', '送信なし']] },
    { k: 'lead_days', l: '標準リードタイム日数', num: 1 } ] },
  conditions: { title: '発注条件グループ', cols: [
    { k: 'condition_id', l: '条件ID', pk: 1 }, { k: 'supplier_code', l: '仕入先コード' }, { k: '仕入先名', l: '仕入先名', ro: 1 }, { k: 'maker_name', l: 'メーカー名' },
    { k: 'display_name', l: '管理名' }, { k: 'condition_type', l: '条件タイプ (数量/金額/上限 他)' }, { k: 'condition_value', l: '条件値', num: 1 }, { k: 'unit', l: '単位' } ] },
  materials: { title: '原料グループ', cols: [
    { k: 'group_id', l: '原料グループID', pk: 1 }, { k: 'name', l: '原料グループ名' }, { k: 'min_order_qty', l: '最低発注量', num: 1 }, { k: 'unit', l: '単位' } ] },
  attrs: { title: '商品紐付け', cols: [
    { k: 'product_code', l: '商品コード', pk: 1 }, { k: '商品名', l: '商品名', ro: 1 },
    { k: 'condition_id', l: '発注条件グループ (名前で検索可)', dl: 'conds' }, { k: 'material_group_id', l: '原料グループ (名前で検索可)', dl: 'mats' },
    { k: 'capacity_per_unit', l: '容量/個', num: 1 }, { k: 'case_group', l: 'ケースグループ' }, { k: 'case_lot', l: 'ケースロット', num: 1 } ] },
  selectable: { title: '選べるセット構成商品 (在庫+注残≦最低在庫で要発注入り。最低在庫 空欄=既定10)', cols: [
    { k: 'product_code', l: '商品コード', pk: 1 }, { k: '商品名', l: '商品名', ro: 1 },
    { k: 'set_names', l: 'セット名 (複数は「、」区切り)' }, { k: 'min_stock', l: '最低在庫数', num: 1 } ] },
};

// グループ一覧 (datalist用)。「ID — 名前」形式で表示し、保存時にIDへ正規化。名前だけの入力もIDに解決
var GROUPS = null;
function ensureGroups(cb) {
  if (GROUPS) return cb();
  Promise.all([
    fetch('/apps/purchase-orders/api/masters/conditions').then(function(r){ return r.json(); }),
    fetch('/apps/purchase-orders/api/masters/materials').then(function(r){ return r.json(); }),
  ]).then(function(res) {
    GROUPS = {
      conds: (res[0].rows || []).map(function(x){ return { id: x.condition_id, name: x.display_name || '' }; }),
      mats: (res[1].rows || []).map(function(x){ return { id: x.group_id, name: x.name || '' }; }),
    };
    cb();
  }).catch(function(){ GROUPS = { conds: [], mats: [] }; cb(); });
}
function dlHtml() {
  if (!GROUPS) return '';
  var mk = function(id, list) {
    return '<datalist id="' + id + '">' + list.map(function(g){ return '<option value="' + esc(g.id + ' — ' + g.name) + '"></option>'; }).join('') + '</datalist>';
  };
  return mk('dl_conds', GROUPS.conds) + mk('dl_mats', GROUPS.mats);
}
function groupLabelOf(kind, id) {
  if (!id || !GROUPS) return id == null ? '' : id;
  var g = (GROUPS[kind] || []).filter(function(x){ return x.id === id; })[0];
  return g ? g.id + ' — ' + g.name : id;
}
function normGroupVal(kind, v) {
  v = String(v == null ? '' : v).trim();
  if (!v) return '';
  var dash = v.indexOf(' — ');
  if (dash >= 0) return v.slice(0, dash).trim(); // 「ID — 名前」→ ID
  if (GROUPS) {
    // 名前だけの入力は一意に決まる場合のみIDへ解決 (重複名は誤紐付けの元、Codex P9 Med)
    var byName = (GROUPS[kind] || []).filter(function(x){ return x.name === v; });
    if (byName.length === 1) return byName[0].id;
  }
  return v;
}

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
      GROUPS = null; // 取込でグループが増えた場合に datalist を更新
      load();
    })
    .catch(function(e){ st.textContent = ''; alert('通信エラー: ' + e.message); });
});

function selHtml(c, val, id) {
  return '<select' + (id ? ' id="' + id + '"' : ' data-k="' + c.k + '"') + '>' + c.sel.map(function(o) {
    return '<option value="' + esc(o[0]) + '"' + (String(val == null ? '' : val) === o[0] ? ' selected' : '') + '>' + esc(o[1]) + '</option>';
  }).join('') + '</select>';
}
function cellHtml(c, val) {
  if (c.ro) return '<td class="muted">' + esc(val == null ? '' : val) + '</td>'; // 表示専用 (商品名等)
  if (c.sel) return '<td>' + selHtml(c, val) + '</td>'; // 選択式 (発注方法等)
  if (c.dl) return '<td><input type="text" list="dl_' + c.dl + '" data-k="' + c.k + '" style="width:98%" value="' + esc(groupLabelOf(c.dl, val)) + '" placeholder="名前でもIDでも"></td>';
  return '<td' + (' contenteditable data-k="' + c.k + '"') + (c.num ? ' class="r"' : '') + '>' + esc(val == null ? '' : val) + '</td>';
}
function render(rows) {
  var def = DEFS[TAB];
  var h = dlHtml();
  h += '<div class="toolbar"><span class="muted">' + rows.length + ' 件 — セルを直接編集して「保存」／最上行から追加</span>' +
    '<input type="text" id="filter" placeholder="🔍 商品名・コード・グループ名で絞り込み" style="margin-left:auto;min-width:260px"></div>';
  h += '<table class="t" id="mtable"><thead><tr>' + def.cols.map(function(c){ return '<th' + (c.num ? ' class="r"' : '') + '>' + c.l + '</th>'; }).join('') + '<th></th></tr></thead><tbody>';
  h += '<tr>' + def.cols.map(function(c) {
    if (c.ro) return '<td class="muted">(自動)</td>';
    if (c.sel) return '<td>' + selHtml(c, '', 'new_' + c.k) + '</td>';
    return '<td><input type="text" style="width:98%" id="new_' + c.k + '"' + (c.dl ? ' list="dl_' + c.dl + '" placeholder="名前でもIDでも"' : '') + '></td>';
  }).join('') + '<td><button class="pri sm" id="btnAdd">追加</button></td></tr>';
  rows.forEach(function(r) {
    h += '<tr data-row="1">' + def.cols.map(function(c) {
      if (c.pk) return '<td>' + esc(r[c.k] == null ? '' : r[c.k]) + '</td>';
      return cellHtml(c, r[c.k]);
    }).join('') + '<td style="white-space:nowrap"><button class="ghost" data-save="' + esc(r[def.cols[0].k]) + '">保存</button><button class="ghost" data-rm="' + esc(r[def.cols[0].k]) + '">削除</button></td></tr>';
  });
  h += '</tbody></table>';
  document.getElementById('tabBody').innerHTML = h;
  document.getElementById('filter').addEventListener('input', function(ev) {
    var q = ev.target.value.trim().toLowerCase();
    document.querySelectorAll('#mtable tr[data-row]').forEach(function(tr) {
      var txt = tr.textContent.toLowerCase();
      tr.querySelectorAll('input[data-k]').forEach(function(inp){ txt += ' ' + inp.value.toLowerCase(); });
      tr.style.display = !q || txt.indexOf(q) >= 0 ? '' : 'none';
    });
  });
}
function renderUnlinked(j) {
  var h = '<div class="toolbar">' +
    '<span class="muted">取扱中なのに発注条件/原料グループ等が未登録の商品。' +
    '<b>新商品が入ると翌朝ここに自動で載ります</b>（グループ対象外の商品はそのままでOK）。</span>' +
    '<span style="margin-left:auto">表示: <select id="uDays">' +
      ['30:直近30日の新商品','60:直近60日の新商品','90:直近90日の新商品','0:全部 (' + j.totalUnlinked + '件)'].map(function(o){
        var v = o.split(':')[0];
        return '<option value="' + v + '"' + (String(j.days) === v ? ' selected' : '') + '>' + o.slice(o.indexOf(':') + 1) + '</option>';
      }).join('') +
    '</select></span></div>';
  h += '<div class="muted" style="margin-bottom:8px">該当 ' + j.count + ' 件' + (j.count > 500 ? ' (先頭500件表示)' : '') + '</div>';
  h += '<table class="t"><thead><tr><th>登録日</th><th>商品コード</th><th>商品名</th><th>仕入先</th></tr></thead><tbody>';
  if (!j.rows.length) h += '<tr><td colspan="4" class="muted">該当なし 🎉</td></tr>';
  j.rows.forEach(function(r){ h += '<tr><td>' + esc(r.reg || '—') + '</td><td>' + esc(r.code) + '</td><td>' + esc(r.name) + '</td><td>' + esc(r.supplier) + '</td></tr>'; });
  h += '</tbody></table>';
  // 未登録グループを参照している商品 (紐付けはあるが参照先が無い = マスタ側の登録漏れ)
  var dg = j.dangling || [];
  if (dg.length) {
    h += '<div class="warn" style="margin-top:18px">⚠️ 未登録グループを参照している商品が ' + dg.length + ' 件あります（発注条件/原料グループマスタの登録漏れ）</div>';
    h += '<table class="t"><thead><tr><th>商品コード</th><th>未登録の発注条件グループ</th><th>未登録の原料グループ</th></tr></thead><tbody>';
    dg.forEach(function(r){ h += '<tr><td>' + esc(r.code) + '</td><td>' + esc(r.missCond || '—') + '</td><td>' + esc(r.missMat || '—') + '</td></tr>'; });
    h += '</tbody></table>';
  }
  document.getElementById('tabBody').innerHTML = h;
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
  if (TAB === 'vendormap') { loadVmap(); return; }
  document.getElementById('tabBody').textContent = '読み込み中…';
  ensureGroups(function() {
    fetch('/apps/purchase-orders/api/masters/' + TAB).then(function(r){ return r.json(); })
      .then(function(j){ if (j.ok) render(j.rows); else document.getElementById('tabBody').textContent = j.error; });
  });
}

// ── 先方番号対応表タブ (仕入先ごとの一覧 + 1件ずつ追加/修正/削除。CSV取込は全置換なのでこちらが日常運用) ──
var VM_SUP = null;   // 選択中の仕入先コード (タブ再訪でも維持)
var VM_GEN = 0;      // 応答の巻き戻り防止 (仕入先を素早く切り替えた時に古い応答で描画しない)
function loadVmap() {
  var gen = ++VM_GEN;
  document.getElementById('tabBody').textContent = '読み込み中…';
  Promise.all([
    fetch('/apps/purchase-orders/api/masters/suppliers').then(function(r){ return r.json(); }),
    fetch(API_EM + '/vendor-map').then(function(r){ return r.json(); }),
  ]).then(function(res) {
    if (gen !== VM_GEN) return;
    if (!res[0].ok) { document.getElementById('tabBody').textContent = res[0].error || '仕入先の取得に失敗しました'; return; }
    var sups = res[0].rows || [];
    var counts = {};
    ((res[1].ok && res[1].maps) || []).forEach(function(m){ counts[m.supplier_code] = m.n; });
    if (!VM_SUP || !sups.some(function(s){ return String(s.supplier_code) === String(VM_SUP); })) {
      // 既定 = 対応表の件数が最多の仕入先 (通常アメージングクラフト)。対応表が空なら先頭の仕入先
      var best = null;
      sups.forEach(function(s){ if (counts[s.supplier_code] && (!best || counts[s.supplier_code] > counts[best])) best = s.supplier_code; });
      VM_SUP = best != null ? best : (sups[0] ? sups[0].supplier_code : null);
    }
    if (VM_SUP == null) { document.getElementById('tabBody').innerHTML = '<span class="muted">仕入先マスタが空です</span>'; return; }
    fetch(API_EM + '/vendor-map/entries?supplier=' + encodeURIComponent(VM_SUP))
      .then(function(r){ return r.json(); })
      .then(function(j) {
        if (gen !== VM_GEN) return;
        if (!j.ok) { document.getElementById('tabBody').textContent = j.error; return; }
        renderVmap(sups, counts, j.rows);
      })
      .catch(function(e){ if (gen === VM_GEN) document.getElementById('tabBody').textContent = '通信エラー: ' + e.message; });
  }).catch(function(e){ if (gen === VM_GEN) document.getElementById('tabBody').textContent = '通信エラー: ' + e.message; });
}
function renderVmap(sups, counts, rows) {
  var opts = sups.map(function(s) {
    var n = counts[s.supplier_code];
    return '<option value="' + esc(s.supplier_code) + '"' + (String(s.supplier_code) === String(VM_SUP) ? ' selected' : '') + '>' +
      esc(s.supplier_code + ' — ' + (s.name || '') + (n ? ' (' + n + '件)' : '')) + '</option>';
  }).join('');
  var h = '<div class="toolbar">仕入先 <select id="vmSup">' + opts + '</select>' +
    '<span class="muted" style="margin-left:10px">' + rows.length + ' 件 — 発注書メールの添付CSVに付く先方管理番号。セルを編集して「保存」</span>' +
    '<input type="text" id="vmFilter" placeholder="🔍 商品コード / 商品名 / 先方番号で絞り込み" style="margin-left:auto;min-width:280px"></div>';
  h += '<table class="t" id="vmTable"><thead><tr><th>商品コード</th><th>商品名</th><th>先方管理番号</th><th>更新日</th><th></th></tr></thead><tbody>';
  h += '<tr><td colspan="2"><input type="text" id="vmNewProd" list="vmProdDl" placeholder="🔍 商品コード / 商品名で検索して選択" style="width:98%"><datalist id="vmProdDl"></datalist></td>' +
    '<td><input type="text" id="vmNewVendor" placeholder="先方管理番号" style="width:98%"></td><td class="muted">—</td>' +
    '<td><button class="pri sm" id="vmAdd">追加</button></td></tr>';
  if (!rows.length) h += '<tr><td colspan="5" class="muted">この仕入先の対応表はまだ空です (上の行から追加、またはメール設定のCSV取込)</td></tr>';
  rows.forEach(function(r) {
    h += '<tr data-vmrow="1" data-vmbase="' + esc(r.updated_at || '') + '"><td>' + esc(r.product_code) + '</td><td class="muted">' + esc(r.name || '') + '</td>' +
      '<td contenteditable data-vmvendor>' + esc(r.vendor_code) + '</td>' +
      '<td class="muted">' + esc((r.updated_at || '').slice(0, 10)) + '</td>' +
      '<td style="white-space:nowrap"><button class="ghost" data-vmsave="' + esc(r.product_code) + '">保存</button>' +
      '<button class="ghost" data-vmrm="' + esc(r.product_code) + '">削除</button></td></tr>';
  });
  h += '</tbody></table>';
  document.getElementById('tabBody').innerHTML = h;
  document.getElementById('vmSup').addEventListener('change', function(ev){ VM_SUP = ev.target.value; loadVmap(); });
  document.getElementById('vmFilter').addEventListener('input', function(ev) {
    var q = ev.target.value.trim().toLowerCase();
    document.querySelectorAll('#vmTable tr[data-vmrow]').forEach(function(tr) {
      tr.style.display = !q || tr.textContent.toLowerCase().indexOf(q) >= 0 ? '' : 'none';
    });
  });
  // 商品検索オートコンプリート (選択中の仕入先の商品のみ)。世代番号で遅延応答の上書きを防ぐ
  var vmDeb = null, vmQGen = 0;
  document.getElementById('vmNewProd').addEventListener('input', function(ev) {
    var v = ev.target.value.trim();
    if (vmDeb) clearTimeout(vmDeb);
    var g = ++vmQGen; // 早期return (入力消去・候補選択) でも世代を進め、送信済み要求の遅延応答を無効化する (Codex R2 Low)
    if (v.length < 2 || v.indexOf(' — ') >= 0) return; // 候補選択後は再検索しない
    vmDeb = setTimeout(function() {
      var pg = VM_GEN; // 仕入先切替・再描画をまたいだ旧応答も破棄する (Codex R3 Low)
      fetch(API_EM + '/vendor-map/products?supplier=' + encodeURIComponent(VM_SUP) + '&q=' + encodeURIComponent(v))
        .then(function(r){ return r.json(); }).then(function(j) {
          if (!j.ok || g !== vmQGen || pg !== VM_GEN) return;
          var dl = document.getElementById('vmProdDl');
          if (dl) dl.innerHTML = j.rows.map(function(p) {
            return '<option value="' + esc(p.code + ' — ' + p.name) + '"></option>';
          }).join('');
        }).catch(function(){});
    }, 250);
  });
}
// baseUpdatedAt: 表示時のupdated_at (新規はnull)。表示後に他画面/CSV取込で変わっていたらサーバが409を返す
function vmPost(productCode, vendorCode, baseUpdatedAt) {
  fetch(API_EM + '/vendor-map/entry', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ supplier_code: VM_SUP, product_code: productCode, vendor_code: vendorCode, baseUpdatedAt: baseUpdatedAt }),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (j.ok) { toast(j.updated ? '更新しました' + (j.oldVendorCode ? ' (旧: ' + j.oldVendorCode + ')' : '') : '追加しました'); loadVmap(); }
    else { toast('エラー: ' + j.error); if (j.conflict) loadVmap(); }
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
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
  if (t.id === 'vmAdd') {
    var pv = document.getElementById('vmNewProd').value;
    var dash = pv.indexOf(' — ');
    var pcode = (dash >= 0 ? pv.slice(0, dash) : pv).trim(); // 「コード — 商品名」からコードを取り出す (手入力のコードだけでも可)
    var vcode = document.getElementById('vmNewVendor').value.trim();
    if (!pcode) { toast('商品を選択してください'); return; }
    if (!vcode) { toast('先方管理番号を入力してください'); return; }
    vmPost(pcode, vcode, null); // 新規のつもりで送る (既に登録済みならサーバが409で現在値を案内)
    return;
  }
  var vmSave = t.getAttribute && t.getAttribute('data-vmsave');
  if (vmSave != null) {
    var vmTr = t.closest('tr');
    vmPost(vmSave, vmTr.querySelector('[data-vmvendor]').textContent.trim(), vmTr.getAttribute('data-vmbase') || null);
    return;
  }
  var vmRm = t.getAttribute && t.getAttribute('data-vmrm');
  if (vmRm != null) {
    if (!confirm('この商品の対応を削除しますか? ' + vmRm + '\\n(発注書メールの添付CSVで先方管理番号が空欄になります)')) return;
    var vmRmBase = t.closest('tr').getAttribute('data-vmbase') || '';
    fetch(API_EM + '/vendor-map/entry?supplier=' + encodeURIComponent(VM_SUP) + '&product=' + encodeURIComponent(vmRm) + '&base=' + encodeURIComponent(vmRmBase), { method: 'DELETE' })
      .then(function(r){ return r.json(); }).then(function(j) {
        if (j.ok) { toast('削除しました'); loadVmap(); }
        else { toast('エラー: ' + j.error); if (j.conflict) loadVmap(); }
      }).catch(function(e){ toast('通信エラー: ' + e.message); });
    return;
  }
  if (t.id === 'btnAdd') {
    var def = DEFS[TAB], b = {};
    def.cols.forEach(function(c) {
      if (c.ro) return;
      var el = document.getElementById('new_' + c.k);
      b[c.k] = c.dl ? normGroupVal(c.dl, el.value) : el.value;
    });
    post(b);
    return;
  }
  var saveKey = t.getAttribute && t.getAttribute('data-save');
  if (saveKey != null) {
    var def2 = DEFS[TAB], tr = t.closest('tr'), b2 = {};
    b2[def2.cols[0].k] = saveKey;
    tr.querySelectorAll('[data-k]').forEach(function(el) {
      var k = el.getAttribute('data-k');
      var raw = (el.tagName === 'INPUT' || el.tagName === 'SELECT') ? el.value : el.textContent;
      var col = null;
      def2.cols.forEach(function(c){ if (c.k === k) col = c; });
      b2[k] = (col && col.dl) ? normGroupVal(col.dl, raw) : raw;
    });
    post(b2);
    return;
  }
  var rmKey = t.getAttribute && t.getAttribute('data-rm');
  if (rmKey != null) {
    if (!confirm('削除しますか? ' + rmKey)) return;
    fetch('/apps/purchase-orders/api/masters/' + TAB + '/' + encodeURIComponent(rmKey), { method: 'DELETE' })
      .then(function(r){ return r.json(); }).then(function(j) {
        if (j.ok) { toast('削除しました'); if (TAB === 'conditions' || TAB === 'materials') GROUPS = null; load(); }
        else toast(j.error);
      });
  }
});
function post(b) {
  fetch('/apps/purchase-orders/api/masters/' + TAB, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b),
  }).then(function(r){ return r.json(); }).then(function(j) {
    if (j.ok) {
      toast('保存しました');
      if (TAB === 'conditions' || TAB === 'materials') GROUPS = null; // グループ名キャッシュを更新
      load();
    } else toast('エラー: ' + j.error);
  });
}
load();

// ── 発注書メール設定 (P15) ──
var API_EM = '/apps/purchase-orders/api';
function emLoad() {
  fetch(API_EM + '/email/settings').then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) return;
    document.getElementById('emMode').value = j.mode;
    document.getElementById('emDryTo').value = j.dryrunTo;
    document.getElementById('emSubject').value = j.subjectTpl;
    document.getElementById('emBody').value = j.bodyTpl;
    document.getElementById('emEnv').textContent = j.envReady ? 'Gmail env: 🟢設定済' : 'Gmail env: ⚠️未設定 (Renderに PO_GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN)';
  });
  fetch(API_EM + '/vendor-map').then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) return;
    document.getElementById('vmapNow').textContent = j.maps.length
      ? '対応表: ' + j.maps.map(function(m){ return (m.name || m.supplier_code) + ' ' + m.n + '件'; }).join(' / ')
      : '対応表: 未登録';
  });
}
document.getElementById('emTplToggle').addEventListener('click', function() {
  var el = document.getElementById('emTpl');
  el.style.display = el.style.display === 'none' ? '' : 'none';
});
document.getElementById('emMode').addEventListener('change', function() {
  document.getElementById('emLiveWrap').style.display = this.value === 'live' ? '' : 'none';
});
document.getElementById('emModeApply').addEventListener('click', function() {
  var mode = document.getElementById('emMode').value;
  var btn = this; btn.disabled = true;
  fetch(API_EM + '/email/mode', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode: mode, confirm: document.getElementById('emLiveConfirm').value }) })
    .then(function(r){ return r.json(); }).then(function(j) {
      btn.disabled = false;
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      toast(j.mode === 'live' ? '⚠️ 本番送信モードに切り替えました' : 'dry-runモードに切り替えました');
      document.getElementById('emLiveConfirm').value = '';
      emLoad();
    }).catch(function(e){ btn.disabled = false; toast('通信エラー: ' + e.message); });
});
document.getElementById('emSave').addEventListener('click', function() {
  var btn = this; btn.disabled = true;
  fetch(API_EM + '/email/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dryrunTo: document.getElementById('emDryTo').value,
      subjectTpl: document.getElementById('emSubject').value, bodyTpl: document.getElementById('emBody').value }) })
    .then(function(r){ return r.json(); }).then(function(j) {
      btn.disabled = false;
      if (!j.ok) { toast('エラー: ' + j.error); return; }
      toast('メール設定を保存しました');
      emLoad();
    }).catch(function(e){ btn.disabled = false; toast('通信エラー: ' + e.message); });
});
document.getElementById('recipForm').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  fetch(API_EM + '/email/recipients/csv', { method: 'POST', body: fd }).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    var pills = ['✅ 宛先更新 ' + j.updated + '件'];
    if (j.unmatched.length) pills.push('⚠️ 名称不一致 ' + j.unmatched.length + '件: ' + j.unmatched.slice(0, 5).join('、') + (j.unmatched.length > 5 ? '…' : ''));
    (j.invalid || []).forEach(function(x){ pills.push('⚠️ ' + x); });
    document.getElementById('emResult').innerHTML = pills.map(function(p){ return '<span class="badge b-draft" style="margin:2px">' + esc(p) + '</span>'; }).join(' ');
    toast('宛先マスタを取り込みました');
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
});
document.getElementById('vmapForm').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  fetch(API_EM + '/vendor-map/csv', { method: 'POST', body: fd }).then(function(r){ return r.json(); }).then(function(j) {
    if (!j.ok) { toast('エラー: ' + j.error); return; }
    toast('対応表を取り込みました (' + j.supplier + ' ' + j.count + '件' + (j.skipped ? ', スキップ' + j.skipped : '') + ')');
    (j.warnings || []).forEach(function(w){ toast('⚠️ ' + w); });
    emLoad();
    if (TAB === 'vendormap') load(); // 取込後にタブ表示中なら一覧を更新
  }).catch(function(e){ toast('通信エラー: ' + e.message); });
});
emLoad();`;
  res.send(pageShell('発注補助 — マスタ管理', 'admin', body, script));
});

export default router;
