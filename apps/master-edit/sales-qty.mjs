/**
 * sales-qty.mjs — マスタの入力の画面に「参考」として出す、過去 7 日・30 日に売れた数 (読むだけ。書かない) (10/5 中原さん「過去 7 日と 30 日でいくつ売れたか」)
 *
 * 読み元 = Render の warehouse-mirror.db の **商品管理リストの公開の回** (mirror_pml_published → mirror_pml_snapshot_rows の 販売数7日_* / 販売数30日_*)。
 *   = 発注アプリ (apps/purchase-orders/logic.js loadPml の 販売数7日_合計・販売数30日_合計 = 推奨保有月数と掛ける数)・FBA 納品 (apps/fba-replenishment/db.js)・
 *     商品管理リストのシートと **同じ数** (二重に数えない・画面ごとに数が食い違わない)。モール別の内訳は仕入先の売れ筋の「速報」と同じ
 *     mirror_f_sales_velocity_by_product_mall (apps/purchase-orders/router.js の /api/products/:code/mall-sales・apps/supplier-sales/aggregate.js)。
 *   数え方 (miniPC の apps/warehouse/rebuild-sales-velocity.js が毎朝 daily-sync で作り、商品管理リストの回に入れて Render に送る):
 *     ・FBA = Amazon の注文 (SP-API・FBA・取消でない) を出品 SKU → NE の商品コードに当て、セットは構成品 × 数に展開
 *     ・FBA 以外 = NE の受注 (キャンセル区分 = 有効・_ignore の店を除く = 楽天・Yahoo・au PAY・Qoo10・LINE ギフト・Amazon 自社発送・卸 ほか全部)。NE はセットを構成品に分けて持つ
 *     ・日 = 注文日 (日本時間)。**今日は入れない**: 7 日 = 前日 (as_of) とその前 6 日・30 日 = 前日とその前 29 日 (as_of = src_velocity_as_of)
 *     ・🚨 セットで売れた分は構成品の数に入る (セットの商品コードには数が付かない)。「うちセット経由」はこの数え方では分けられない (元の数え方が分けて持たない)
 *   Company DB の mart.sku_activity (商品の動き・0042) を使わない理由: ① 数える範囲が違う (Company DB の注文は 6 モールだけ = 卸・メルカリ ほかの NE の店が入らない) =
 *     発注アプリ・商品管理リストと数が食い違う ② 読むには master_edit に mart の usage が要る = mart の重い関数 (sku_activity ほか・PUBLIC EXECUTE) も呼べる範囲が広がる
 *     (#1625 と同じ理由で渡さない) ③ mart の関数を使わずに同じ数え方を SQL で書き直す = 二重に作る
 * いつまでの数か = 公開の回の src_velocity_as_of (前日)。毎朝 07:00 の daily-sync で前日までに進む (11:30 まで自動の再試行)。
 *   古い = 前日より前 (正午までは前々日まで待つ = 朝の取込の前)。商品管理リストの回が作れない日 (販売速度が 2 日より古い・FBA と NE の重なり) は前の回のまま = 古いと出る
 * 読めない (写しがまだ無い・表が無い) = その欄だけ「読めない」(画面は出る)。Company DB の権限は使わない (create-master-edit-roles.mjs は変えない)
 */
import { normProductCode } from '../purchase-orders/db.js';

const YMD = /^\d{4}-\d{2}-\d{2}$/;
const DAY_MS = 864e5;

/** 写し (warehouse-mirror.db) の取り出し。試験は差し替える (better-sqlite3 の db を返す関数) */
let mirrorProvider = null;
export function __setSalesMirrorProvider(fn) { mirrorProvider = fn || null; }
async function mirrorDb() {
  if (mirrorProvider) return mirrorProvider();
  const { getMirrorDB } = await import('../warehouse-mirror/db.js');
  return getMirrorDB();
}

/** 'YYYY-MM-DD' に日を足す */
export function addDays(ymd, n) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd);
  return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]) + n * DAY_MS).toISOString().slice(0, 10);
}
const jstParts = (ms) => { const d = new Date(ms + 9 * 3600e3); return { ymd: d.toISOString().slice(0, 10), h: d.getUTCHours() }; };
const dayDiff = (a, b) => Math.round((Date.parse(`${a}T00:00:00Z`) - Date.parse(`${b}T00:00:00Z`)) / DAY_MS);

/**
 * 古いか: いつまでの数か (asOf) が前日でない。朝の取込 (07:00〜11:30 の再試行) の前 = 正午までは前々日までを古いと言わない
 * (夜中〜朝に毎日「古い」と出さない)。今日より先の日付 (時計のずれ) も古い扱いにはしない
 */
export function salesStale(asOf, nowMs) {
  const { ymd, h } = jstParts(nowMs);
  const lag = dayDiff(ymd, asOf);
  return lag > 2 || (lag === 2 && h >= 12);
}

/**
 * 公開の回 (いつまでの数か)。
 * { ok: true, runId, asOf: 'YYYY-MM-DD' (この日まで), from7, from30, stale } / { ok: false, error, reason: 'no_data' | 'error' }
 */
export async function readSalesRun({ now = Date.now() } = {}) {
  try {
    const db = await mirrorDb();
    const pub = db.prepare('select run_id, src_velocity_as_of from mirror_pml_published where id = 1').get();
    if (!pub || !pub.run_id) return { ok: false, error: '商品管理リスト (販売数) の写しがまだありません', reason: 'no_data' };
    const asOf = String(pub.src_velocity_as_of ?? '').slice(0, 10);
    if (!YMD.test(asOf)) return { ok: false, error: '販売数がいつまでの数か分かりません (商品管理リストの回に日付が無い)', reason: 'no_data' };
    return { ok: true, runId: String(pub.run_id), asOf, from7: addDays(asOf, -6), from30: addDays(asOf, -29), stale: salesStale(asOf, now) };
  } catch (e) {
    console.error(`[master-edit] 販売数 (商品管理リスト) を読めない: ${e && e.message}`);
    return { ok: false, error: '販売数 (商品管理リスト) を読めません', reason: 'error' };
  }
}

const n = (v) => (v == null || v === '' || !Number.isFinite(Number(v)) ? 0 : Number(v));
const rowOf = (r) => ({ d7: n(r.d7), d30: n(r.d30), d7fba: n(r.d7f), d7other: n(r.d7n), d30fba: n(r.d30f), d30other: n(r.d30n) });
const SELECT_COLS = `商品コード as code, 販売数7日_合計 as d7, 販売数30日_合計 as d30, 販売数7日_FBA as d7f, 販売数7日_FBA以外 as d7n, 販売数30日_FBA as d30f, 販売数30日_FBA以外 as d30n`;
/** 1 回に問い合わせるコードの数 (SQLite の ? の数の上限より十分小さく) */
const CHUNK = 200;

/**
 * 一覧のページの商品コードの販売数。Map(normProductCode(コード) → { d7, d30, d7fba, d7other, d30fba, d30other })。
 * Map に無いコード = 商品管理リストに無い (NE にまだ無い商品など。0 とは分ける)。読めない回 (run.ok でない) = null
 * 索引 idx_mpsr_run_code_norm (run_id, LOWER(TRIM(商品コード))) で、そのページのコードだけ引く (全部の商品を読まない)
 */
export async function salesOfCodes(run, codes) {
  if (!run || !run.ok) return null;
  const keys = [...new Set(codes.map((c) => normProductCode(c)).filter(Boolean))];
  const m = new Map();
  if (!keys.length) return m;
  try {
    const db = await mirrorDb();
    for (let i = 0; i < keys.length; i += CHUNK) {
      const part = keys.slice(i, i + CHUNK);
      const rows = db.prepare(`select ${SELECT_COLS} from mirror_pml_snapshot_rows where run_id = ? and lower(trim(商品コード)) in (${part.map(() => '?').join(', ')})`).all(run.runId, ...part);
      for (const r of rows) m.set(normProductCode(r.code), rowOf(r));
    }
    return m;
  } catch (e) {
    console.error(`[master-edit] 販売数 (一覧) を読めない: ${e && e.message}`);
    return null;
  }
}

/**
 * 1 つの商品の販売数と内訳。
 * { ok: true, run, row: { d7, d30, d7fba, d7other, d30fba, d30other } | null (商品管理リストに無い),
 *   malls: { ok, asOf, rows: [{ mall, label, d7, d30 }] } (モール別 = 速報。合計と同じ朝に作る・作れない朝は前の値のまま = asOf を別に持つ) } / { ok: false, error }
 */
export async function readSalesSku(run, code) {
  if (!run || !run.ok) return run || { ok: false, error: '読めない' };
  const key = normProductCode(code);
  try {
    const db = await mirrorDb();
    const r = db.prepare(`select ${SELECT_COLS} from mirror_pml_snapshot_rows where run_id = ? and lower(trim(商品コード)) = ?`).get(run.runId, key);
    let malls;
    try {
      const labels = new Map();
      try { for (const x of db.prepare('select mall_key, label, display_order from dim_mall').all()) labels.set(x.mall_key, { label: x.label, order: Number(x.display_order) }); } catch { /* 名前の表が無い = キーのまま */ }
      const rows = db.prepare('select mall, qty_7d, qty_30d, as_of_date from mirror_f_sales_velocity_by_product_mall where lower(trim(商品コード)) = ?').all(key);
      const asOfs = [...new Set(rows.map((x) => String(x.as_of_date ?? '').slice(0, 10)))];
      malls = {
        ok: true, asOf: asOfs.length === 1 ? asOfs[0] : asOfs.length ? asOfs.sort().at(-1) : null, mixed: asOfs.length > 1,
        rows: rows.map((x) => ({ mall: x.mall, label: labels.get(x.mall)?.label || (x.mall === 'other' ? 'その他 (店の登録なし)' : x.mall), d7: n(x.qty_7d), d30: n(x.qty_30d), order: labels.get(x.mall)?.order ?? 999 }))
          .sort((a, b) => b.d30 - a.d30 || b.d7 - a.d7 || a.order - b.order),
      };
    } catch (e) {
      console.error(`[master-edit] 販売数のモール別を読めない: ${e && e.message}`);
      malls = { ok: false, rows: [] };
    }
    return { ok: true, run, row: r ? rowOf(r) : null, malls };
  } catch (e) {
    console.error(`[master-edit] 販売数 (1 つの商品) を読めない: ${e && e.message}`);
    return { ok: false, error: '販売数 (商品管理リスト) を読めません' };
  }
}
