/**
 * 米国FBA在庫補充 (/apps/fba-replenishment-us)。
 *
 * PR1 (2026-09-24) = 見るだけ: 毎朝 miniPC が取った米国の RESTOCK / PLANNING を、取れたままの行から一覧にする。
 *   推奨・仮確定・出力・ピッキング準備・納品実績は後の PR (設計方針 = AI_reference システム設計/米国FBA納品アプリ_設計方針_20260924.md §8)。
 *   日本の FBA在庫補充 (apps/fba-replenishment) の表・計算・キャッシュには触らない (読むのは warehouse-mirror の SKU マスタの写しだけ)。
 *
 * データの流れ: miniPC daily-sync「FBA在庫snapshot」→ DATA_DIR/fba-us-reports/*.json (apps/warehouse/fba-us-reports-store.js)
 *   → miniPC GET /service-api/fba/us/reports/latest → この画面を開くたびに読む (Render には保存しない。15 行ほど)
 */
import express from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { buildUsInventoryView } from './us-view.js';
import { computeUsAllocation, parseComponents } from './allocation.js';
import { readUsReserved, findByRequest, insertSlip, transition, listSlips, getSlip, contentHashOf } from './ledger.js';
import { buildUsNeCsv } from './ne-csv.js';
import { validateStaItems, buildStaUsWorkbook } from './sta-excel.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

/** miniPC のサービス API を GET で呼ぶ (日本版 router.js の callMiniPC と同じ認証ヘッダ。読むだけなので 1 回だけ再試行) */
export async function fetchUsReportsFromMiniPC({ fetchImpl = fetch, timeout = 30000 } = {}) {
  const url = `${WAREHOUSE_URL}/service-api/fba/us/reports/latest`;
  const headers = {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
  let lastError;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetchImpl(url, { headers, redirect: 'manual', signal: AbortSignal.timeout(timeout) });
      const ct = res.headers.get('content-type') || '';
      if (res.status === 302 || res.status === 303) throw new Error(`miniPC の認証 (Cloudflare Access) の設定がおかしい (HTTP ${res.status})`);
      if (res.status === 401 || res.status === 403) throw new Error(`miniPC に断られた (HTTP ${res.status})`);
      if (res.status === 404) throw new Error('miniPC にこの口がまだ無い (HTTP 404 = miniPC の更新と WarehouseServer の再起動がまだ)');
      if (!res.ok || !ct.includes('application/json')) {
        const txt = await res.text().catch(() => '');
        throw Object.assign(new Error(`miniPC HTTP ${res.status}: ${txt.slice(0, 160)}`), { retryable: res.status >= 500 });
      }
      const body = await res.json();
      if (!body || body.ok !== true) throw new Error(`miniPC の応答の形がおかしい: ${JSON.stringify(body).slice(0, 160)}`);
      return body;
    } catch (e) {
      lastError = e;
      const retryable = e.retryable || e.name === 'TimeoutError' || /fetch failed|ECONNRESET|ETIMEDOUT/i.test(String(e.message));
      if (!retryable || attempt === 2) throw e;
      await new Promise((r) => setTimeout(r, 800));
    }
  }
  throw lastError;
}

/**
 * 米国 SKU → 自社の商品コード (NE 商品コード = ロジザード商品ID)。
 *   1) SKU マスタ (mirror_sku_resolved・source='master') = 構成と数量つき (例: cardstand-r-40 = cardstand-r × 40)
 *   2) 無ければ 商品コードと同じ文字列 (mirror_products) = 1 個として扱えるが、SKU マスタに未登録なので「要登録」と出す
 *   3) どちらも無ければ none
 * 写しを読めなければ unknown (エラーつき) = 「結びつかない」と混ぜない
 */
export function resolveUsSkus(skus, { mdb } = {}) {
  const out = new Map();
  if (!skus.length) return out;
  let db;
  try { db = mdb || getMirrorDB(); }
  catch (e) { for (const s of skus) out.set(s, { route: 'unknown', components: [], error: e.message }); return out; }
  try {
    const ph = skus.map(() => '?').join(',');
    const master = db.prepare(`SELECT lower(trim(seller_sku)) AS k, ne_code, quantity, 商品名 AS name, sort_order
      FROM mirror_sku_resolved WHERE source = 'master' AND lower(trim(seller_sku)) IN (${ph})
      ORDER BY k, sort_order, ne_code`).all(...skus);
    for (const r of master) {
      if (!out.has(r.k)) out.set(r.k, { route: 'master', components: [], name: r.name || null });
      out.get(r.k).components.push({ ne_code: r.ne_code, qty: r.quantity });
    }
    const rest = skus.filter((s) => !out.has(s));
    if (rest.length) {
      const ph2 = rest.map(() => '?').join(',');
      for (const r of db.prepare(`SELECT lower(trim(商品コード)) AS k, 商品コード AS code, 商品名 AS name FROM mirror_products WHERE lower(trim(商品コード)) IN (${ph2})`).all(...rest)) {
        out.set(r.k, { route: 'product_code', components: [{ ne_code: r.code, qty: 1 }], name: r.name || null });
      }
    }
    for (const s of skus) if (!out.has(s)) out.set(s, { route: 'none', components: [] });
  } catch (e) {
    for (const s of skus) out.set(s, { route: 'unknown', components: [], error: e.message });
  }
  return out;
}

const router = express.Router();

router.get('/', (req, res) => {
  res.render('fba-replenishment-us', { username: req.session && req.session.email, displayName: req.session && req.session.displayName });
});

router.get('/api/inventory', async (req, res) => {
  let payload;
  try {
    payload = await fetchUsReportsFromMiniPC();
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'minipc_unreachable', message: `miniPC から米国のレポートを読めませんでした: ${e.message}` });
  }
  try {
    const view = buildUsInventoryView(payload, { resolveSkus: (skus) => resolveUsSkus(skus) });
    res.json({ ok: true, ...view });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'build_failed', message: e.message });
  }
});

/**
 * PR2: 日本優先の配分 (表示だけ)。日本の表は **読む関数だけ** 呼ぶ (initDb・保存・同期 API は呼ばない)。
 * 🚨 日本の fba.db は日本の router が起動時に initDb() する。それが終わるまでは計算しない (isFbaDbReady)。
 */
export async function loadJpInputs() {
  const jp = await import('../fba-replenishment/db.js');
  if (!jp.isFbaDbReady()) {
    throw Object.assign(new Error('日本の FBA在庫補充の DB がまだ準備中です (起動直後)。少し待ってから読み直してください'), { code: 'JP_DB_NOT_READY' });
  }
  const { calcTargetDays, mergeRestockWithPlanning } = await import('../fba-replenishment/calculation-engine.js');
  const { norm } = await import('./allocation.js');
  const settings = jp.getSettings();
  const jpMappings = jp.getSkuMappings();
  const volumeOf = new Map(jpMappings.map((m) => [norm(m.amazon_sku), Number(m.per_unit_volume) || 0]));
  const planningByKey = new Map(jp.getPlanningLatest().map((p) => [norm(p.amazon_sku), p]));
  const fr = jp.getInputFreshness();
  return {
    jpRestock: jp.getRestockLatest(),
    // 日本の計算 (calculation-engine.js) と同じ入力・同じ設定で、SKU ごとの目標日数を出す
    jpTargetDaysOf: (r) => {
      const snap = mergeRestockWithPlanning(r, planningByKey.get(norm(r.amazon_sku)) || null);
      if (snap._gaps.units_sold_30d) return null;
      const perUnitVolume = snap.per_unit_volume || volumeOf.get(norm(r.amazon_sku)) || 0;
      return calcTargetDays(snap.units_sold_30d, perUnitVolume, snap, settings);
    },
    jpMappings,
    jpExcluded: new Set(jp.getReplenishmentExcluded().map((e) => norm(e.amazon_sku))),
    warehouse: jp.getWarehouseSummary(),
    selfShip: jp.getSelfShipSalesByCode(),
    pending: jp.getPendingFbaSlips(),
    freshness: { jpRestockSourceAt: fr.restock_source_at, jpRestockSourceMissing: fr.restock_source_missing, warehouseUploadedAt: fr.warehouse_uploaded_at },
  };
}

router.get('/api/allocation', async (req, res) => {
  let payload;
  try {
    payload = await fetchUsReportsFromMiniPC();
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'minipc_unreachable', message: `miniPC から米国のレポートを読めませんでした: ${e.message}` });
  }
  try {
    const { view, alloc } = await computeAll(payload);
    res.json({ ok: true, ...alloc, us_slips: summarizeUsReserved(alloc._usReserved) });
  } catch (e) {
    const status = (e.code === 'JP_DB_NOT_READY' || e.code === 'FBA_SHEETLESS_MISCONFIG') ? 503 : 500;   // Sheet なしのモードの設定の誤り (⑦-F) も 503
    res.status(status).json({ ok: false, error: e.code || 'allocation_failed', message: e.message });
  }
});

/** 米国の配分を、米国のレポート (payload) + 日本の表 + 米国の台帳の押さえ中 から出す (画面と NE CSV の検証で同じもの) */
async function computeAll(payload) {
  const view = buildUsInventoryView(payload, { resolveSkus: (skus) => resolveUsSkus(skus) });
  const jpInputs = await loadJpInputs();
  // 倉庫在庫の時点 = 日本の倉庫 CSV の取り込み時刻 (日本の画面の計算と同じ)。「倉庫から出た」伝票はこれより前に出ていれば引かない
  const whAt = jpInputs.freshness.warehouseUploadedAt ? new Date(String(jpInputs.freshness.warehouseUploadedAt).replace(' ', 'T')).getTime() : NaN;
  const usReserved = readUsReserved({ warehouseAtMs: Number.isFinite(whAt) ? whAt : null });
  const alloc = computeUsAllocation({
    usRows: view.rows, usRestockFetchedAt: view.restock_fetched_at,
    usLastAttempt: view.last_attempt, usSaveFailure: view.save_failure, usDupKeys: view.dup_keys,
    usReserved,
    ...jpInputs,
  });
  Object.defineProperty(alloc, '_usReserved', { value: usReserved, enumerable: false });
  return { view, alloc };
}
const summarizeUsReserved = (u) => ({ status: u.status, error: u.error || null, version: u.version ?? null, count: u.count || 0, units: u.units || 0 });

// ── 米国用 NE 受注 CSV (= 倉庫の在庫を押さえる。設計方針 §12.6) ──
// 米国の出力は 1 本ずつ (Render は 1 インスタンス)。miniPC への問い合わせは鍵の外で済ませ、鍵の中は 台帳の再読み → 検証 → 保存 だけ
let neQueue = Promise.resolve();
function withNeLock(fn) {
  const run = neQueue.then(fn, fn);
  neQueue = run.then(() => {}, () => {});
  return run;
}

/**
 * 出してよいかを確かめて、構成品ごとの個数を返す。断るときは例外 (code = US_NE_REJECTED・message = 理由)
 * 関所: 参考 (入力が古い・欠け) / 影響先の分からない日本 SKU がある / 要求した SKU が判定できない / 構成品が判定できない (期限管理品を含む) /
 *       要求の全行を構成品ごとに合算して「米国に回せる数 (日本の分・既に出した米国の伝票を引いた後)」を超える
 */
export function checkNeExport(rows, alloc, view) {
  const reasons = [];
  if (alloc.reference) reasons.push(`いまの数字は参考です (${(alloc.gates || []).map((g) => g.text).join(' / ')})`);
  if ((alloc.unattributed_jp_loose_count || 0) > 0) reasons.push(`構成が分からない日本の SKU が ${alloc.unattributed_jp_loose_count} 件あり、日本に残す数が分からない (SKU マスタに構成を登録してください)`);
  const usBy = new Map((alloc.us || []).map((x) => [x.sku.trim().toLowerCase(), x]));
  const codeBy = new Map((alloc.codes || []).map((b) => [b.code, b]));
  const viewBy = new Map(view.rows.map((r) => [r.sku.trim().toLowerCase(), r]));
  const need = new Map();
  for (const r of rows) {
    const k = r.sku.trim().toLowerCase();
    const u = usBy.get(k);
    const vr = viewBy.get(k);
    if (!u || u.status === 'unknown') { reasons.push(`${r.sku}: 判定できない (${u ? u.reason : '配分に無い'})`); continue; }
    const comps = vr && vr.mapping && ['master', 'product_code'].includes(vr.mapping.route) ? parseComponents((vr.mapping.components || []).map((c) => ({ ne_code: c.ne_code, qty: c.qty })), null) : null;
    if (!comps) { reasons.push(`${r.sku}: 構成が分からない`); continue; }
    for (const c of comps) need.set(c.code, (need.get(c.code) || 0) + c.qty * r.qty);
  }
  const units = [];
  for (const [code, qty] of need) {
    const b = codeBy.get(code);
    if (!b || (b.unknown && b.unknown.length) || !Number.isFinite(b.pool)) { reasons.push(`${code}: 判定できない${b && b.unknown ? ` (${b.unknown.join(' / ')})` : ''}`); continue; }
    if (qty > b.pool) reasons.push(`${code}: ${qty} 個は米国に回せる数 ${b.pool} 個を超えます (日本の分・既に出した米国の伝票を引いた後)`);
    units.push({ code, qty });
  }
  if (reasons.length) throw Object.assign(new Error(reasons.join(' / ')), { code: 'US_NE_REJECTED' });
  return units;
}

/** 構成品 (NE 商品コード) → 商品名 (NE 受注 CSV の商品名の列。無ければ空) */
function productNamesOf(codes) {
  const out = new Map();
  if (!codes.length) return out;
  try {
    const mdb = getMirrorDB();
    const ph = codes.map(() => '?').join(',');
    for (const r of mdb.prepare(`SELECT lower(trim(商品コード)) AS k, 商品名 AS name FROM mirror_products WHERE lower(trim(商品コード)) IN (${ph})`).all(...codes)) out.set(r.k, r.name || '');
  } catch { /* 商品名が無くても NE は商品コードで取り込める */ }
  return out;
}

router.post('/api/ne-csv', express.json({ limit: '64kb' }), async (req, res) => {
  const requestId = String((req.body && req.body.request_id) || '');
  if (!/^[A-Za-z0-9_-]{8,80}$/.test(requestId)) return res.status(400).json({ ok: false, error: 'bad_request_id', message: 'request_id が無い・形が違う' });
  const by = (req.session && (req.session.displayName || req.session.email)) || null;
  const sendCsv = (slip) => {
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename=${slip.filename}`);
    res.setHeader('X-US-Order-No', slip.order_no);
    res.send(slip.csv);
  };
  try {
    // 同じ依頼の再送 (通信が切れた・二度押し) は、鍵の前に保存済みの伝票を返す (同じ中身なら)
    const prior = findByRequest(requestId);
    if (prior) {
      if (prior.content_hash !== contentHashOf((req.body.items || []).map((i) => ({ sku: String(i.sku).trim(), qty: Number(i.qty) })))) {
        return res.status(409).json({ ok: false, error: 'request_reused', message: 'この request_id は別の中身で使われています (画面を読み直してください)' });
      }
      return sendCsv(prior);
    }
    const payload = await fetchUsReportsFromMiniPC();   // 鍵の外 (遅い)
    const slip = await withNeLock(async () => {
      const again = findByRequest(requestId);
      if (again) {
        // 同時に来た同じ request_id の依頼: 中身が違えば 409 (違う数で「成功」を返さない。Codex #1489 R1 Medium 1)
        if (again.content_hash !== contentHashOf(req.body && req.body.items)) throw Object.assign(new Error('この request_id は別の中身で使われています (画面を読み直してください)'), { code: 'US_NE_REUSED' });
        return again;
      }
      const { view, alloc } = await computeAll(payload);   // 鍵の中で台帳・日本の表を読み直す (直前に出た米国の伝票も引いた後で検証)
      if (alloc._usReserved.status === 'error') throw Object.assign(new Error(`米国の台帳を読めません: ${alloc._usReserved.error}`), { code: 'US_NE_REJECTED' });
      const dup = new Set((view.dup_keys && view.dup_keys.restock) || []);
      const known = new Map(view.rows.filter((r) => r.in_restock && !dup.has(r.sku.trim().toLowerCase())).map((r) => [r.sku.trim().toLowerCase(), r.sku]));
      const { rows, errors } = validateStaItems(req.body && req.body.items, known);
      if (errors.length) throw Object.assign(new Error(errors.join(' / ')), { code: 'US_NE_REJECTED' });
      const units = checkNeExport(rows, alloc, view);
      const names = productNamesOf(units.map((u) => u.code));
      const unitsNamed = units.map((u) => ({ ...u, name: names.get(u.code) || '' }));
      return insertSlip({
        requestId, by,
        items: rows.map((r) => ({ sku: r.sku, qty: r.qty })),
        units: unitsNamed,
        buildCsv: ({ seq, now }) => buildUsNeCsv(unitsNamed, { now, seq }),   // 保存と同じ取引の中で作る = 保存できてから CSV を返す
      });
    });
    return sendCsv(slip);
  } catch (e) {
    if (e.code === 'US_NE_REJECTED') return res.status(409).json({ ok: false, error: 'rejected', message: e.message });
    if (e.code === 'US_NE_REUSED') return res.status(409).json({ ok: false, error: 'request_reused', message: e.message });
    if (e.code === 'US_LEDGER_NOT_AVAILABLE') return res.status(503).json({ ok: false, error: 'not_available', message: e.message });
    return res.status(500).json({ ok: false, error: 'ne_csv_failed', message: e.message });
  }
});

router.get('/api/slips', (req, res) => {
  try { res.json({ ok: true, ...listSlips() }); }
  catch (e) { res.status(500).json({ ok: false, error: 'slips_failed', message: e.message }); }
});

router.post('/api/slips/:orderNo/transition', express.json({ limit: '8kb' }), (req, res) => {
  const to = String((req.body && req.body.to) || '');
  const expect = String((req.body && req.body.expect) || '');
  const by = (req.session && (req.session.displayName || req.session.email)) || null;
  try {
    const slip = transition(req.params.orderNo, to, { expect, by, note: req.body && req.body.note });
    res.json({ ok: true, slip });
  } catch (e) {
    const status = { US_LEDGER_NOT_FOUND: 404, US_LEDGER_CONFLICT: 409, US_LEDGER_BAD_TRANSITION: 409, US_LEDGER_BAD_STATUS: 400, US_LEDGER_NOT_AVAILABLE: 503 }[e.code] || 500;
    res.status(status).json({ ok: false, error: e.code || 'transition_failed', message: e.message });
  }
});

router.get('/api/slips/:orderNo/csv', (req, res) => {
  try {
    const s = getSlip(req.params.orderNo);
    if (!s) return res.status(404).json({ ok: false, message: '伝票が無い' });
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename=${s.filename}`);
    res.send(s.csv);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.code || 'slip_failed', message: e.message });
  }
});

// その伝票の中身 (保存した SKU・数量) で STA 用 Excel を作る = NE の伝票と Amazon のプランの数を合わせる
//   台帳を読めないときも例外を受けて 500 を返す (async のまま投げると Express 4 は応答しない。Codex #1489 R2 Medium)
router.get('/api/slips/:orderNo/sta', async (req, res) => {
  try {
    const s = getSlip(req.params.orderNo);
    if (!s) return res.status(404).json({ ok: false, message: '伝票が無い' });
    const buf = await buildStaUsWorkbook(s.items.map((i) => ({ sku: i.sku, qty: i.qty, expiry: null })));
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=US_STA_${s.order_no}.xlsx`);
    res.send(buf);
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

/**
 * 米国の STA に取り込む納品 Excel (テンプレートに SKU・数量を書いたもの) を返す。
 * 🚨 これは倉庫の在庫を押さえない (押さえるのは NE 受注 CSV を出したとき = 日本の伝票と同じ方式・次の PR)。
 * SKU は今の米国 RESTOCK にあるものだけ。同じレポートに 2 行ある SKU は数字が正しいか分からないので断る。
 */
router.post('/api/sta-excel', express.json({ limit: '64kb' }), async (req, res) => {
  let payload;
  try {
    payload = await fetchUsReportsFromMiniPC();
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'minipc_unreachable', message: `miniPC から米国のレポートを読めませんでした: ${e.message}` });
  }
  try {
    const view = buildUsInventoryView(payload, { resolveSkus: () => new Map() });
    const dup = new Set((view.dup_keys && view.dup_keys.restock) || []);
    const known = new Map(view.rows.filter((r) => r.in_restock && !dup.has(r.sku.trim().toLowerCase())).map((r) => [r.sku.trim().toLowerCase(), r.sku]));
    const { rows, errors } = validateStaItems(req.body && req.body.items, known);
    if (errors.length) return res.status(400).json({ ok: false, error: 'invalid_items', message: errors.join(' / ') });
    const buf = await buildStaUsWorkbook(rows);
    const day = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=US_STA_Manifest_${day}.xlsx`);
    res.send(buf);
  } catch (e) {
    res.status(500).json({ ok: false, error: 'sta_excel_failed', message: e.message });
  }
});

export default router;
