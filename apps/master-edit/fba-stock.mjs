/**
 * fba-stock.mjs — マスタの入力の画面に「参考」として出す FBA (日本) の在庫 (読むだけ。書かない) (10/5 中原さん「FBA の在庫も・何時時点か」)
 *
 * 読み元 = Company DB の在庫の日次 (snapshots.stock_capture_days + snapshots.sku_stock_daily の source = 'fba_jp'・scope = 'jp'。D2b-2)。
 *   - 朝の FBA のスナップショット (apps/warehouse/fba-report-snapshot.js) が SP-API の RESTOCK + PLANNING のレポートを取り、
 *     「取り終えた時刻」を captured_at に入れて送る (apps/company-db/push/stock-daily.mjs → ingest/stock-daily.mjs)。
 *     → **何時時点か = その日の captured_at** (毎朝 07:30〜08:40 ごろ)。日付 = その日の業務日 (snapshot_date)
 *   - 数え方は mart.v_sku_stock (と、それを使う mart.sku_activity = 商品の動き) と同じ:
 *       ① 最新の **complete** の日だけ (partial = RESTOCK が取れず FC 移管中・処理中・出荷待ちが分からない日 / missing = 取れなかった日 は使わない)
 *       ② SKU = 受け口が入れた sku_id (出品の構成が **1 SKU × 1 個** のときだけ入る)。1 つの SKU に出品 SKU が複数あれば足す
 *       ③ まとめ売り (1 SKU × N 個)・セットの出品 (複数の SKU) は sku_id = null = この SKU の数に入れない (二重に数えない・
 *          FBA の 1 個が SKU の 1 個ではない)。単品の画面だけ「この SKU を含む まとめ売り・セットの出品」として別に出す (合計には入れない)
 *       ④ NE のセットの SKU は、そのセットに 1 × 1 で当たる出品の在庫だけ (構成品へは展開しない = 構成品の在庫と二重に数えない)
 *   - 一覧の数 = 販売可能 (fba_available。v_sku_stock の fba_jp_available と同じ)。単品の画面に内訳 (FC 移管中・処理中・出荷待ち・入荷待ち)。
 *     FC の 3 区分が null の行 = その出品 SKU が RESTOCK に載っていなかった = 「不明」(0 と読まない)
 *   - 26 時間より前の取得 = 古い (朝の取得が 1 日抜けた)。読めない (権限が無い・日次がまだ無い) = その欄だけ「読めない」
 * 権限: 画面のロール master_edit に snapshots の usage と 2 つの表の select だけ (scripts/company-db/create-master-edit-roles.mjs の FBA_STOCK_SELECT)。
 *   mart.v_sku_stock を読まないのは、取得の時刻 (captured_at) と内訳が無いのと、mart の usage を渡すと mart の関数も呼べる範囲が広がるため。
 *   snapshots の関数は 2 つ (月の分割を作る = 呼び手の権限で CREATE が要る = master_edit では何もできない) と trigger の関数だけ
 * 米国 (fba_us) は出さない: 出品 SKU が Company DB の出品 (amazon_us) の構成に当たらず、NE のコードに結べない (10/5 の本番で 15 行とも sku_id なし)
 */
import { COMPANY_ID } from '../../lib/master-write.mjs';
import { ListTimeoutError } from './deadline.mjs';

export const FBA_STALE_MS = 26 * 3600e3;
export const FBA_SOURCE = Object.freeze({ source: 'fba_jp', scope: 'jp', mall: 'amazon', label: 'FBA (JP)' });
const DAY_STATUS_WORDS = Object.freeze({ partial: '一部だけ取れた (FC 移管中・処理中・出荷待ちが分からない)', missing: '取れなかった' });

const toIso = (v) => (v instanceof Date ? v.toISOString() : v == null ? null : new Date(v).toISOString());
const num = (v) => (v == null ? null : Number(v));

/**
 * 最新の complete の日 (と、それより新しい complete でない日)。
 * { ok: true, date: 'YYYY-MM-DD', asOf: ISO (取得の時刻), stale, newer: null | { date, status, words } } / { ok: false, error, reason: 'no_privilege' | 'no_data' | 'error' }
 */
export async function readFbaDay(db, { now = Date.now() } = {}) {
  if (!db) return { ok: false, error: 'Company DB につながらない', reason: 'error' };
  try {
    const priv = (await db.query(`select has_schema_privilege('snapshots', 'usage') as sch`)).rows[0];
    const tbl = priv && priv.sch ? (await db.query(`select has_table_privilege('snapshots.stock_capture_days', 'select') and has_table_privilege('snapshots.sku_stock_daily', 'select') as ok`)).rows[0] : null;
    if (!tbl || !tbl.ok) return { ok: false, error: '画面のロールに FBA の在庫を読む権限がまだ無い (create-master-edit-roles.mjs の流し直しが要る)', reason: 'no_privilege' };
    // 最新の complete の日と、最新の日 (complete / partial / missing のどれでも) を別の列で 1 行に (行の順に頼らない。#1625 Codex R1 Low)
    const x = (await db.query(`
      with d as (select snapshot_date, status, captured_at from snapshots.stock_capture_days
                  where company_id = $1 and source = $2 and scope_key = $3 and status in ('complete', 'partial', 'missing'))
      select (select snapshot_date::text from d where status = 'complete' order by snapshot_date desc limit 1) as done_date,
             (select captured_at from d where status = 'complete' order by snapshot_date desc limit 1) as done_at,
             (select snapshot_date::text from d order by snapshot_date desc limit 1) as last_date,
             (select status from d order by snapshot_date desc limit 1) as last_status`,
    [COMPANY_ID, FBA_SOURCE.source, FBA_SOURCE.scope])).rows[0];
    if (!x || !x.done_date) return { ok: false, error: 'FBA の在庫の日次 (全部取れた日) がまだありません', reason: 'no_data' };
    const asOf = toIso(x.done_at);
    const t = Date.parse(asOf);
    return {
      ok: true, date: x.done_date, asOf, stale: !Number.isFinite(t) || now - t > FBA_STALE_MS,
      newer: x.last_date && x.last_date > x.done_date ? { date: x.last_date, status: x.last_status, words: DAY_STATUS_WORDS[x.last_status] || x.last_status } : null,
    };
  } catch (e) {
    if (e instanceof ListTimeoutError) throw e;   // 期限切れ (CSV・全部コピー) は「読めない」にしない = 503 へ (#1627 Codex R4)
    console.error(`[master-edit] FBA の在庫を読めない: ${e && e.message}`);
    return { ok: false, error: e && e.code === '42501' ? '画面のロールに FBA の在庫を読む権限がまだ無い' : 'FBA の在庫を読めません', reason: e && e.code === '42501' ? 'no_privilege' : 'error' };
  }
}

/**
 * 一覧のページの SKU の FBA の在庫 (販売可能)。Map(sku_id 文字 → 販売可能の合計) = その日のレポートに 1 × 1 の出品の行がある SKU だけ。
 * 🚨 Map に無い SKU は **販売可能 0** (mart.v_sku_stock の coalesce(…, 0) と同じ。#1625 Codex R1 M)。「行が無い」は在庫の数とは別に (呼ぶ側の fba_row) 持つ
 * 読めない日 (day.ok でない) = null
 */
export async function fbaAvailableOf(db, day, skuIds) {
  if (!db || !day || !day.ok) return null;
  const m = new Map();
  if (!skuIds.length) return m;
  try {
    for (const r of (await db.query(`select sku_id::text as id, sum(fba_available)::bigint::text as n from snapshots.sku_stock_daily
        where company_id = $1 and source = $2 and scope_key = $3 and snapshot_date = $4::date and sku_id = any($5::bigint[]) group by sku_id`,
    [COMPANY_ID, FBA_SOURCE.source, FBA_SOURCE.scope, day.date, skuIds])).rows) m.set(r.id, Number(r.n));
    return m;
  } catch (e) {
    if (e instanceof ListTimeoutError) throw e;   // 期限切れ (CSV・全部コピー) は「読めない」にしない = 503 へ (#1627 Codex R4)
    console.error(`[master-edit] FBA の在庫 (一覧) を読めない: ${e && e.message}`);
    return null;
  }
}

/**
 * 1 つの SKU の FBA の在庫の内訳。
 * { ok: true, day, total: { available, transfer, processing, customer, inbound, unknown (FC の 3 区分が分からない出品 SKU の数), hasRow (1 × 1 の出品の行がその日にある。無ければ数は全部 0) },
 *   rows: [{ code, available, transfer, processing, customer, inbound_working, inbound_shipped, inbound_received }],
 *   bundles: [{ code, qty, others, available }] (この SKU を含む まとめ売り・セットの出品 = 合計に入れていない) } / { ok: false, error }
 */
export async function readFbaSku(db, day, skuId) {
  if (!day || !day.ok) return day || { ok: false, error: '読めない' };
  try {
    const p = [COMPANY_ID, FBA_SOURCE.source, FBA_SOURCE.scope, day.date];
    const rows = (await db.query(`select source_code as code, fba_available, fba_fc_transfer, fba_fc_processing, fba_customer_order,
        fba_inbound_working, fba_inbound_shipped, fba_inbound_received
      from snapshots.sku_stock_daily where company_id = $1 and source = $2 and scope_key = $3 and snapshot_date = $4::date and sku_id = $5::bigint order by source_code`, [...p, skuId])).rows
      .map((r) => ({ code: r.code, available: num(r.fba_available), transfer: num(r.fba_fc_transfer), processing: num(r.fba_fc_processing), customer: num(r.fba_customer_order),
        inbound_working: num(r.fba_inbound_working), inbound_shipped: num(r.fba_inbound_shipped), inbound_received: num(r.fba_inbound_received) }));
    const sum = (k) => rows.reduce((s, r) => s + (r[k] || 0), 0);
    // 行が無い = 販売可能 0 (mart.v_sku_stock と同じ)。hasRow = その日のレポートにこの SKU 1 個だけの出品の行があるか (数とは別に持つ)
    const total = {
      available: sum('available'), transfer: sum('transfer'), processing: sum('processing'), customer: sum('customer'),
      inbound: sum('inbound_working') + sum('inbound_shipped') + sum('inbound_received'),
      unknown: rows.filter((r) => r.transfer == null).length, hasRow: rows.length > 0,
    };
    // この SKU を含む まとめ売り・セットの出品 (Amazon 日本)。受け口と同じ出品の当て方 (core.resolve_listing_id) で、その日の sku_id の無い行に当てる。
    //   候補 = この SKU を構成に持つ出品だけ (数件) → その日の行は出品の正規化で絞ってから resolve_listing_id で確かめる (全行に関数を呼ばない)
    const bundles = (await db.query(`
      with mine as (
        select l.listing_id, l.listing_norm, lc.qty,
               (select count(*) from core.listing_components x where x.company_id = l.company_id and x.listing_id = l.listing_id) - 1 as others
          from core.listing_components lc join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id and l.mall = $6
         where lc.company_id = $1 and lc.sku_id = $5::bigint)
      select d.source_code as code, mine.qty::int as qty, mine.others::int as others, d.fba_available
        from snapshots.sku_stock_daily d join mine on mine.listing_norm = core.norm_code(d.source_code)
       where d.company_id = $1 and d.source = $2 and d.scope_key = $3 and d.snapshot_date = $4::date and d.sku_id is null
         and (mine.qty <> 1 or mine.others > 0)
         and core.resolve_listing_id($1::smallint, $6, d.source_code) = mine.listing_id
       order by d.source_code`, [...p, skuId, FBA_SOURCE.mall])).rows
      .map((r) => ({ code: r.code, qty: Number(r.qty), others: Number(r.others), available: num(r.fba_available) }));
    return { ok: true, day, total, rows, bundles };
  } catch (e) {
    if (e instanceof ListTimeoutError) throw e;   // 期限切れ (CSV・全部コピー) は「読めない」にしない = 503 へ (#1627 Codex R4)
    console.error(`[master-edit] FBA の在庫 (1 つの SKU) を読めない: ${e && e.message}`);
    return { ok: false, error: e && e.code === '42501' ? '画面のロールに FBA の在庫を読む権限がまだ無い' : 'FBA の在庫を読めません' };
  }
}
