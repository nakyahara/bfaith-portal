/**
 * master-capability.mjs — このコードが扱える「持ち主が company のキー」と、持ち主の読み方の版 (protocol) の定義元
 *   (広げる道 PR-0・設計 newentry_widen v10 §2 G6〜G8・Codex R1 M8 / R2 M5「capability / protocol の基盤を先に確定」)
 *
 * 持ち主表は 3 つ + 能力 1 つ:
 *   configured = config/master-ownership.mjs の MASTER_OWNERSHIP = 「次に prepare する予定」。🚨 これを変えて配っても何も切り替わらない
 *   prepared   = DB (ops.master_ownership_state.prepared_map)。人がコマンドで「次にこれにする」と記録したもの
 *   active     = DB (ops.master_ownership_state.active_map) = **持ち主の正**。夜間ロード・写し・古い入口の門・(PR-2 から) 画面の保存はこれに従う
 *   capable    = ここの COMPANY_CAPABLE = このコード (この build) が「持ち主が company」として正しく扱えるキー。
 *                DB の active / prepared に、ここに無いキーが company で入っている = このコードは古い (code_behind) = 書く側は閉じる (fail-closed)。
 *                (古い build を配り直した・配り漏れ = 知らない company のキーを今までの汎用の動きで書かない。設計 G8)
 *
 * キーを company にする順 (1 つのキー K):
 *   1. K を扱えるコード (夜間ロードの守り・写し・画面・門) と一緒に、ここの COMPANY_CAPABLE に K を足す PR を配る (全部の場所)
 *   2. configured (config/master-ownership.mjs) の K を 'company' にする (別のコミット・次の予定)
 *   3. 切替の手順 (prepare → … → activate / widen) で DB の active に K が入る = ここで初めて切り替わる
 *   🚨 configured の company は capable の中だけ (validateConfiguredCapable・試験 apps/company-db/test-master-capability.mjs)。
 *      capable から K を外すのは、DB の active / prepared に K が company で無いことを確かめてから (外した build は code_behind で止まる)
 *
 * protocol = 持ち主の読み方の版 (門の記録・readiness に出す。DB が最低の版を求める仕組みは PR-1 の後)。版を上げたら下の PROTOCOL_HISTORY に 1 行足す
 */
import crypto from 'node:crypto';
import { OWNED_COLUMNS, MASTER_OWNERSHIP } from './master-ownership.mjs';

/** 持ち主の読み方の版の説明 (版 → 何ができるコードか)。🚨 足すだけ (前の版の意味を変えない) */
export const PROTOCOL_HISTORY = Object.freeze({
  1: '画面・新商品・NE 登録の CSV・Amazon・product-hub の案内は、配った config の持ち主表を DB の active と段階の記録に照らす (違えば 409)。古い入口の門は DB の active ∪ prepared',
  2: '広げる道 PR-2: 書く取引の中で DB の active を読んで持ち主表にする (配った config は見ない)・capable の外の C のキー = 409 code_behind・読めない = 503・新規開始は DB の開放の許可 (ops.new_entry_lease_valid) と非常の止め・門の記録の持ち主表 = DB の active',
});
/** このコードの持ち主の読み方の版 */
export const MASTER_OWNER_PROTOCOL = 2;

/**
 * このコードが company として扱えるキー (2026-10-05 の切替で active にした 13 キー + skus.sku_kind)。
 *   skus.sku_kind = #1641 (夜間ロードの守り・正規化・写しの 9 升・照合 ② の区分の分類とゲートの数)。configured (config/master-ownership.mjs) は 'load' のまま
 * 🚨 load のままのキー (products.parent・sku_components・listing_components.amazon・suppliers.* の 4 つ) は、それぞれ扱いの PR で足す
 */
export const COMPANY_CAPABLE = Object.freeze([
  'external_ids.jan',
  'products.name',
  'products.sales_class',
  'products.status',
  'sku_costs',
  'skus.handling',
  'skus.name',
  'skus.reorder_months',
  'skus.shipping',
  'skus.sku_kind',
  'skus.standard_price',
  'skus.tax_class',
  'skus.tax_rate',
  'supplier_skus.is_primary',
]);

const fail = (problems) => Object.assign(new Error(`master-capability: ${problems.join(' / ')}`), { code: 'CAPABILITY_INVALID' });

/** 能力の一覧を確かめる (知らないキー・重なり・並びの乱れ = 落とす)。typo で「扱えるつもり」を作らない */
export function validateCapability(capable = COMPANY_CAPABLE, protocol = MASTER_OWNER_PROTOCOL) {
  const problems = [];
  if (!Array.isArray(capable)) throw fail(['能力の一覧が配列でない']);
  const known = new Set(OWNED_COLUMNS);
  const seen = new Set();
  for (const k of capable) {
    if (typeof k !== 'string' || !known.has(k)) problems.push(`知らないキー: ${k}`);
    if (seen.has(k)) problems.push(`重なり: ${k}`);
    seen.add(k);
  }
  const sorted = [...capable].sort();
  if (sorted.some((k, i) => k !== capable[i])) problems.push('キーの順に並べる (差分を読みやすく)');
  if (!Number.isSafeInteger(protocol) || protocol < 1 || !Object.hasOwn(PROTOCOL_HISTORY, String(protocol))) problems.push(`protocol が不正: ${protocol} (PROTOCOL_HISTORY に説明が要る)`);
  if (problems.length) throw fail(problems);
  return capable;
}

/**
 * 持ち主表 map (DB の active / prepared) の company のキーのうち、このコードが扱えないもの (= code_behind の理由)。
 * 知らないキー (OWNED_COLUMNS に無い = もっと新しいコードが足したキー) も company なら扱えない側。load のキーは見ない (load = 今までの動き)
 */
export function codeBehindKeys(map, capable = COMPANY_CAPABLE) {
  const can = new Set(capable);
  return Object.keys(map || {}).filter((k) => map[k] === 'company' && !can.has(k)).sort();
}

/** configured (次の予定) の company が capable の中か (外 = 予定がコードを追い越している)。戻り値 = 外のキー */
export function configuredBeyondCapable(configured = MASTER_OWNERSHIP, capable = COMPANY_CAPABLE) {
  return codeBehindKeys(configured, capable);
}
/** configured が capable の外に出ていれば落とす (試験・prepare の前の確かめで使う) */
export function validateConfiguredCapable(configured = MASTER_OWNERSHIP, capable = COMPANY_CAPABLE) {
  const beyond = configuredBeyondCapable(configured, capable);
  if (beyond.length) throw fail([`configured が company なのにこのコードが扱えないキー: ${beyond.join('・')} (COMPANY_CAPABLE に足す PR が先)`]);
  return configured;
}

/** 能力のハッシュ (キーの順によらない)。門の記録・readiness で「どの能力のコードか」を比べる */
export function capableHash(capable = COMPANY_CAPABLE) {
  return crypto.createHash('sha256').update(JSON.stringify([...capable].sort())).digest('hex');
}

/** 能力の指紋 (読み戻し・readiness に出す形) */
export function capabilityFingerprint() {
  return { protocol: MASTER_OWNER_PROTOCOL, capable_hash: capableHash(), capable: [...COMPANY_CAPABLE] };
}

/** 起動時にも検査する (壊れた一覧のまま門・画面が動かないように) */
validateCapability(COMPANY_CAPABLE, MASTER_OWNER_PROTOCOL);
