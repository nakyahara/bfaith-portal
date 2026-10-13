/**
 * ne-delete-pml.mjs — NE から消す CSV (0072・#1690) の確かめだけが読む、商品管理リストの公開の回の在庫の列 (NE の在庫・引当・発注残 (NE の古い値)・FBA の在庫)
 *
 * 🚨 注残の契約 (docs/contracts/pml_backorder_authority.contract.md) の許可リストに入っているのはこのファイルだけ:
 *   mirror_pml_snapshot_rows の 注残数 (NE の発注残 = 業務では古い値) を読むのは、NE が「自分に発注残が残っている商品」の削除を断るので、
 *   その NE の値が 0 かを見るためだけ。業務の注残 (発注アプリの台帳) としては使わない (発注アプリの注残は ne-delete-stock.mjs が台帳で別に見る)。
 *   ほかの用途のコードをこのファイルに足さない (#1702 Codex: sales-qty.mjs 全体を許可すると後から足すコードも古い注残を読めてしまう)
 * 読む写し・読み取りの取引は sales-qty.mjs と同じ物 (試験の差し替え __setSalesMirrorProvider がこのファイルにも効く)
 * 呼び手 = apps/master-edit/ne-delete-stock.mjs だけ
 */
import { normProductCode } from '../purchase-orders/db.js';
import { salesMirrorDb, salesMirrorTx } from './sales-qty.mjs';

/** 1 回に問い合わせるコードの数 (SQLite の ? の数の上限より十分小さく) */
const CHUNK = 200;
/**
 * 総在庫数 = NE の在庫 + FBA の在庫 (build-product-management-snapshot.js) = NE の在庫 = 総在庫数 − FBA在庫数。
 *   いつの NE の在庫か = src_ne_products_synced_at (毎朝の NE の取得)。STOCK_PML_MAX_AGE_MS より古い・読めない = ok: false (= 消す CSV に入れない)。
 *   ポインタと明細を 1 つの読み取りの取引で (回の切り替えを挟んでも食い違わない)。
 * 戻り値 = { ok: true, runId, neSyncedAt, map: Map(normProductCode(コード) → { total, fba, allocated, onOrder }) } / { ok: false, error }
 *   Map に無いコード = 商品管理リストに無い (= 在庫を確かめられない)
 */
export const STOCK_PML_MAX_AGE_MS = 36 * 3600e3;
const intOrNull = (v) => (v == null || v === '' || !Number.isInteger(Number(v)) ? null : Number(v));
export async function readPmlStock(codes, { now = Date.now() } = {}) {
  const keys = [...new Set((codes || []).map((c) => normProductCode(c)).filter(Boolean))];
  try {
    const db = await salesMirrorDb();
    return salesMirrorTx(db, () => {
      const pub = db.prepare('select run_id, src_ne_products_synced_at from mirror_pml_published where id = 1').get();
      if (!pub || !pub.run_id) return { ok: false, error: '商品管理リストの写しがまだありません' };
      const at = Date.parse(String(pub.src_ne_products_synced_at ?? ''));
      if (!Number.isFinite(at)) return { ok: false, error: '商品管理リストの NE の在庫がいつのものか分かりません' };
      if (now - at > STOCK_PML_MAX_AGE_MS) return { ok: false, error: `商品管理リストの NE の在庫が古い (${pub.src_ne_products_synced_at})` };
      const map = new Map();
      for (let i = 0; i < keys.length; i += CHUNK) {
        const part = keys.slice(i, i + CHUNK);
        const rows = db.prepare(`select 商品コード as code, 総在庫数 as total, FBA在庫数 as fba, 引当数 as alloc, 注残数 as onorder from mirror_pml_snapshot_rows
          where run_id = ? and lower(trim(商品コード)) in (${part.map(() => '?').join(', ')})`).all(pub.run_id, ...part);
        for (const r of rows) map.set(normProductCode(r.code), { total: intOrNull(r.total), fba: intOrNull(r.fba), allocated: intOrNull(r.alloc), onOrder: intOrNull(r.onorder) });
      }
      return { ok: true, runId: String(pub.run_id), neSyncedAt: String(pub.src_ne_products_synced_at), map };
    });
  } catch (e) {
    console.error(`[master-edit] 商品管理リストの在庫を読めない: ${e && e.message}`);
    return { ok: false, error: '商品管理リストの在庫を読めません' };
  }
}
