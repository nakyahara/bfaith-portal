/**
 * 商品コード → 「期限管理商品か」(mirror DB を読むだけ)
 *
 * 正本 = ロジザード商品マスタの「有効期限区分」(01 = 無し / 02 以降 = あり)。入荷受付チェックが
 * 日次で取り込んで `f_inbound_check_product_flags` に入れている (apps/inbound-check/product-master.js)。
 * 人が直した分は source='manual' で同じ表に入る。
 *
 * 🚨 **「期限管理でない」と「分からない」を混ぜない** (managed: false と null を分ける)。
 *   混ぜると、マスタに無い商品の期限入力欄を黙って消してしまう。現場は入れず・本社も STA に入れられず、
 *   納品が止まって初めて分かる = 外で弾かれるまで誰も気づけない向きの間違い。
 *   [[feedback_where_missing_becomes_zero]] [[feedback_safe_side_is_directional]]
 *
 * 入荷受付チェックの productInfoMap (apps/inbound-check/db.js) も同じ 2 つの表を見るが、あちらは
 * 「分からない」を「管理しない」として扱う (荷受けを止めないため)。**答えが違うのは扱いだけで、規則は同じ**:
 *   ①実物 (在庫) に有効期限が入っている → 期限管理商品
 *   ②マスタ/手動の設定がある → その値
 *   ③どちらも無い → 分からない (こちらは null / あちらは「管理しない」)
 * ①を②より先に見るのは、設定が「管理しない」でも実物に期限が付いていたら実物を信じるため。
 */
import { getMirrorDB } from './db.js';

const CHUNK = 400;
const normCode = (s) => String(s ?? '').trim().toLowerCase();

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * @param {string[]} codes 商品コード (ロジザードの商品ID / NE の商品コード。大小文字は問わない)
 * @param {object} [db] mirror DB (差し替え可)
 * @returns {Map<string, {managed: boolean|null, source: 'stock'|'logizard'|'manual'|'unknown'}>}
 *   キーは小文字にした商品コード。managed: true=期限管理商品 / false=期限管理でない / null=分からない
 */
export function expiryManagedByCode(codes, db = getMirrorDB()) {
  const keys = [...new Set((codes || []).map(normCode).filter(Boolean))];
  const out = new Map(keys.map((k) => [k, { managed: null, source: 'unknown' }]));
  if (keys.length === 0) return out;
  const hasStock = tableExists(db, 'mirror_logizard_stock');
  const hasFlags = tableExists(db, 'f_inbound_check_product_flags');
  // どちらの表も無い = まだ同期していない → 全部「分からない」のまま返す (false に倒さない)
  if (!hasStock && !hasFlags) return out;
  for (let i = 0; i < keys.length; i += CHUNK) {
    const part = keys.slice(i, i + CHUNK);
    const ph = part.map(() => '?').join(',');
    if (hasFlags) {
      for (const r of db.prepare(`SELECT code_key, expiry_managed, source FROM f_inbound_check_product_flags
          WHERE code_key IN (${ph})`).all(...part)) {
        const m = out.get(normCode(r.code_key));
        if (m) { m.managed = !!r.expiry_managed; m.source = r.source === 'manual' ? 'manual' : 'logizard'; }
      }
    }
    if (hasStock) {
      // 実物に期限が付いている商品 (在庫がある分だけ見る。引当済みで 0 になった行は見ない = 入荷受付チェックと同じ)
      for (const r of db.prepare(`SELECT DISTINCT lower(trim(商品ID)) AS k FROM mirror_logizard_stock
          WHERE lower(trim(商品ID)) IN (${ph}) AND 在庫数 > 0 AND COALESCE(trim(有効期限), '') <> ''`).all(...part)) {
        const m = out.get(r.k);
        if (m) { m.managed = true; m.source = 'stock'; }   // 設定が「管理しない」でも実物を信じる
      }
    }
  }
  return out;
}
