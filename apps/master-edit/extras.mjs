/**
 * extras.mjs — マスタの入力の画面に「参考」として出す、ほかのアプリの値 (読むだけ。書かない) (10/5 中原さん)
 *
 * 注文残 = 発注アプリの台帳 (apps/purchase-orders・Render の warehouse-mirror.db の po_*)。
 *   数え方は発注アプリと同じ関数: 合計 = logic.js loadLedgerBackorders (= v_ledger_backorder_by_product) / 内訳 = loadBackorderLines
 *   (= v_ledger_backorder_lines。by_product はこの明細を足すだけ)。対象 = 発行済み・追跡 (tracked)・閉じていない・残 > 0・境界以後。
 *   残 = 発注数 − (入庫 + 欠品 + 取消) (逆仕訳は効かない)。商品の鍵 = 発注アプリの normProductCode (前後の空白を除いて小文字)
 * 在庫 (ロジザード) = Company DB の毎時の取込 (apps/company-db/inventory-hourly.mjs) が読むのと同じ元・同じ関数
 *   (inventory/logizard.mjs readMirrorLogizardStock = warehouse-mirror.db の mirror_logizard_stock・miniPC が毎時 09〜18 時に全置換) を読み、
 *   日次の締め (closeStockDay → sku_stock_daily) と同じく 商品ID ごとに全部の行 (ロケ・品質区分) の在庫数を足して、コードの正規化 (norm_code) で SKU に当てる。
 *   Company DB の在庫の view (mart.*) を読まないのは、画面のロール (master_edit) に mart の権限が無いため (足すと mart の関数も呼べる範囲が広がる)。
 *   いつの写しか = 世代 (captured_at)。2 時間より前 = 古い。ただし写しが動くのは毎日 09〜18 時の毎時 00 分だけ (config/jobs-registry.mjs の
 *   logizard-stock-hourly) = その日の最後の回 (18 時台の写し) は、次の朝の最初の回 (09:00) と 1 時間の余裕の 10:00 までは古いと言わない
 *   (10/5 中原さん「夜に在庫の見出しが『古い 18:01』になる」)。18 時の回が抜けた (最後が 17 時台) 夜は 2 時間で古い。
 * どちらも読めないときは null (画面はその欄だけ「読めない」と出す)。
 */
import { loadLedgerBackorders, loadBackorderLines } from '../purchase-orders/logic.js';
import { normProductCode } from '../purchase-orders/db.js';
import { readMirrorLogizardStock } from '../company-db/inventory/logizard.mjs';
import { normSku } from '../../lib/sku-norm.js';
import path from 'node:path';

export const STOCK_STALE_MS = 2 * 3600e3;
/** ロジザードの在庫の写しが動く時間 (JST・毎時 00 分): logizard-stock-hourly の「毎日 09:00-18:00」 */
export const STOCK_FIRST_HOUR_JST = 9;
export const STOCK_LAST_HOUR_JST = 18;

/**
 * 写しが古いか。captured = 写しの時刻 (ISO)。
 *   ① 2 時間以内 = 古くない ② 最後の回 (18 時台) の写しなら、次の日の 10:00 (最初の回 09:00 + 1 時間) までは古くない ③ ほかは古い
 */
export function stockStale(captured, now = Date.now()) {
  const t = Date.parse(captured);
  if (!Number.isFinite(t)) return true;
  if (now - t <= STOCK_STALE_MS) return false;
  const j = new Date(t + 9 * 3600e3);
  if (j.getUTCHours() < STOCK_LAST_HOUR_JST) return true;
  const until = Date.UTC(j.getUTCFullYear(), j.getUTCMonth(), j.getUTCDate() + 1, STOCK_FIRST_HOUR_JST + 1) - 9 * 3600e3;
  return now >= until;
}

/** 注文残の合計 (全商品)。{ ok: true, map: Map(商品の鍵 → 数) } / { ok: false, error } */
export function readBackorders() {
  try {
    return { ok: true, map: loadLedgerBackorders() };
  } catch (e) {
    console.error(`[master-edit] 発注アプリの注文残を読めない: ${e && e.message}`);
    return { ok: false, error: '発注アプリの台帳を読めません' };
  }
}
/** その商品コードの注文残 (読めなければ null・無ければ 0) */
export function backorderOf(bo, code) {
  if (!bo || !bo.ok) return null;
  return Number(bo.map.get(normProductCode(code)) || 0);
}
/** 注文残がある商品の鍵 (絞り込み用) */
export function backorderKeys(bo) {
  return bo && bo.ok ? [...bo.map].filter(([, q]) => Number(q) > 0).map(([k]) => k) : [];
}
/** 1 つの商品の注文残の内訳。{ ok, lines, total } / { ok: false, error } */
export function readBackorderLines(code) {
  try {
    const lines = loadBackorderLines(code);
    return { ok: true, lines, total: lines.reduce((s, l) => s + Number(l.remaining || 0), 0) };
  } catch (e) {
    console.error(`[master-edit] 発注アプリの注文残の内訳を読めない: ${e && e.message}`);
    return { ok: false, error: '発注アプリの台帳を読めません' };
  }
}

const dataDir = () => process.env.DATA_DIR || path.join(process.cwd(), 'data');
let stockCache = null;   // { at: 読んだ時刻 ms, dir, out }
const STOCK_CACHE_MS = 60e3;   // 毎時の全置換なので 1 分は同じ値を使う (一覧を開くたびに 8,000 行を読まない)

/**
 * ロジザードの在庫 (SKU のコードの正規化 → 数)。
 * { ok: true, asOf: ISO, stale: bool, map: Map(code_norm → 数) } / { ok: false, error }
 */
export async function readWarehouseStock({ now = Date.now(), reader = readMirrorLogizardStock } = {}) {
  const dir = dataDir();
  if (stockCache && stockCache.dir === dir && now - stockCache.at < STOCK_CACHE_MS && stockCache.reader === reader) return withStale(stockCache.out, now);
  let out;
  try {
    const got = await reader(dir);
    if (!got) out = { ok: false, error: 'ロジザードの在庫の写しがまだありません' };
    else {
      const map = new Map();
      for (const r of got.rows) {
        const k = normSku(r['商品ID']);
        if (!k) continue;
        map.set(k, (map.get(k) || 0) + Number(r['在庫数'] || 0));
      }
      out = { ok: true, asOf: got.capturedAt, map };
    }
  } catch (e) {
    console.error(`[master-edit] ロジザードの在庫を読めない: ${e && e.message}`);
    out = { ok: false, error: 'ロジザードの在庫を読めません' };
  }
  stockCache = { at: now, dir, out, reader };
  return withStale(out, now);
}
function withStale(out, now) {
  if (!out.ok) return out;
  return { ...out, stale: stockStale(out.asOf, now) };
}
/** 試験だけ: 覚えた在庫を捨てる */
export function __clearStockCache() { stockCache = null; }
/** その SKU (code_norm) の在庫 (読めなければ null・写しに無ければ 0) */
export function stockOf(st, codeNorm) {
  if (!st || !st.ok) return null;
  return Number(st.map.get(codeNorm) || 0);
}
/** セットの「作れる数」(参考) = 構成品ごとの floor(在庫 / 数) の最小。構成が無い・読めない = null */
export function buildableOf(st, comps) {
  if (!st || !st.ok || !comps || !comps.length) return null;
  let n = Infinity;
  for (const c of comps) {
    const q = Number(c.qty);
    if (!(q > 0)) return null;
    n = Math.min(n, Math.floor(stockOf(st, c.code_norm) / q));
  }
  return Number.isFinite(n) ? n : null;
}
