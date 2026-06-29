/**
 * 商品管理リスト スナップショット生成（④）
 *
 * 現スプレッドシート「商品管理リスト」互換の完成行を warehouse 側で確定する。
 * Render/GAS/画面は published_run_id のみ参照 → 「画面とシートの数字が違う」事故を防ぐ。
 *
 * ソース (m_products 起点・全件 left join):
 *   m_products              … 商品名/商品区分/取扱区分/標準売価/原価/送料/仕入先
 *   raw_ne_products         … 自社在庫/引当/発注残(注残)/最終仕入日/発注ロット/代表コード/ロケーション/分類タグ/作成日(登録日)
 *   inv_daily_detail        … FBA在庫数 = fba_warehouse + fba_inbound(入荷待ち) (最新 business_date)
 *   f_sales_velocity_by_product … 7日/30日 × FBA/FBA以外 販売数 (③)
 *   m_reorder_setting       … 推奨保有月数 (②)
 *
 * 健全性ゲート (status):
 *   failed  … velocity 無し / FBA在庫日付 無し / ne_fba_overlap>0 / velocity as_of が VELOCITY_MAX_LAG_DAYS 超過
 *   partial … FBA在庫が FBA_MAX_LAG_DAYS 超過 (在庫がやや古い、閲覧は可)
 *   ok      … 上記以外
 *   published_run_id は status≠failed のときのみ切替。GAS は status='ok' のみ上書き(別途)。
 *
 * 使い方: node apps/warehouse/build-product-management-snapshot.js
 */
import crypto from 'crypto';
import { getDB, initDB } from './db.js';

const VELOCITY_MAX_LAG_DAYS = parseInt(process.env.PML_VELOCITY_MAX_LAG_DAYS || '2', 10);
const FBA_MAX_LAG_DAYS = parseInt(process.env.PML_FBA_MAX_LAG_DAYS || '3', 10);
const KEEP_RUNS = parseInt(process.env.PML_KEEP_RUNS || '5', 10);

function nowTs() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function jstToday() {
  const j = new Date(Date.now() + 9 * 3600 * 1000);
  return `${j.getUTCFullYear()}-${String(j.getUTCMonth() + 1).padStart(2, '0')}-${String(j.getUTCDate()).padStart(2, '0')}`;
}
function daysBetween(fromStr, toStr) {
  if (!fromStr) return null;
  const f = Date.parse(fromStr.slice(0, 10));
  const t = Date.parse(toStr);
  if (Number.isNaN(f) || Number.isNaN(t)) return null;
  return Math.floor((t - f) / 86400000);
}
function r1(n) { return n == null ? null : Math.round(n * 10) / 10; }

// 列順を固定 (checksum / 出力で一貫)。snapshot_rows の列と一致させる。
const COLUMNS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '商品区分', '売上分類', '最終仕入日', '在庫保管日数',
  '総在庫数', 'FBA在庫数', 'フリー在庫', '注残数', '引当数', '総在庫数_引当なし',
  '販売数7日_FBA', '販売数7日_FBA以外', '販売数7日_合計',
  '販売数30日_FBA', '販売数30日_FBA以外', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '想定見込み利益', '概算利益率',
  '代表商品コード', 'ロケーションコード', '商品分類タグ', '登録日',
];

export async function buildProductManagementSnapshot() {
  const db = getDB();
  const generatedAt = nowTs();
  const today = jstToday();
  const runId = `pml_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

  // ─── ソース watermark ───
  const neSynced = db.prepare('SELECT MAX(synced_at) AS v FROM raw_ne_products').get()?.v || null;
  const velAsOf = db.prepare('SELECT MAX(as_of_date) AS v FROM f_sales_velocity_by_product').get()?.v || null;
  const velCount = db.prepare('SELECT COUNT(*) AS c FROM f_sales_velocity_by_product').get()?.c || 0;
  const fbaBizDate = db.prepare("SELECT MAX(business_date) AS v FROM inv_daily_detail WHERE category = 'fba_warehouse'").get()?.v || null;
  const reorderUpdated = db.prepare('SELECT MAX(synced_at) AS v FROM m_reorder_setting').get()?.v || null;

  // ─── ne_fba_overlap (DQ ゲート: 本集計と同条件) ───
  let overlap = 0;
  try {
    const c30 = (() => { const d = new Date(Date.parse(today) - 30 * 86400000); return d.toISOString().slice(0, 10); })();
    const fbaIds = db.prepare(`SELECT DISTINCT amazon_order_id FROM raw_sp_orders WHERE fulfillment_channel='Amazon' AND order_status NOT IN ('Cancelled') AND date(purchase_date,'+9 hours') > ?`).all(c30).map(r => r.amazon_order_id);
    if (fbaIds.length) {
      const neSet = new Set(db.prepare(`
        SELECT DISTINCT o.受注番号 FROM raw_ne_orders o LEFT JOIN shops s ON o.店舗コード = s.shop_code
        WHERE o.キャンセル区分='有効' AND COALESCE(s.platform,'')<>'_ignore' AND SUBSTR(o.受注日,1,10) > ?
      `).all(c30).map(r => r.受注番号));
      for (const id of fbaIds) if (neSet.has(id)) overlap++;
    }
  } catch (e) { console.error('[pml-snapshot] overlap計測スキップ:', e.message); }

  // ─── status 判定 ───
  const velLag = daysBetween(velAsOf, today);
  const fbaLag = daysBetween(fbaBizDate, today);
  const reasons = [];
  let status = 'ok';
  if (velCount === 0) { status = 'failed'; reasons.push('velocity空'); }
  if (!fbaBizDate) { status = 'failed'; reasons.push('FBA在庫日付なし'); }
  if (overlap > 0) { status = 'failed'; reasons.push(`ne_fba_overlap=${overlap}`); }
  if (velLag != null && velLag > VELOCITY_MAX_LAG_DAYS) { status = 'failed'; reasons.push(`velocity ${velLag}日遅延`); }
  if (status !== 'failed' && fbaLag != null && fbaLag > FBA_MAX_LAG_DAYS) { status = 'partial'; reasons.push(`FBA在庫 ${fbaLag}日遅延`); }

  // ─── 行構築 (m_products 起点 left join) ───
  // FBA在庫数 = FBA倉庫内(fba_warehouse) + 入荷待ち(fba_inbound)。中原さん要望(2026-06)。
  //   fba_warehouse = available+fc_transfer+fc_processing+customer_order、fba_inbound = inbound_working+shipped+received。
  const fbaSub = fbaBizDate
    ? `LEFT JOIN (SELECT ne_code, SUM(qty) AS qty FROM inv_daily_detail WHERE category IN ('fba_warehouse','fba_inbound') AND business_date = ? GROUP BY ne_code) fba ON m.商品コード = fba.ne_code COLLATE NOCASE`
    : `LEFT JOIN (SELECT NULL AS ne_code, 0 AS qty) fba ON 1=0`;
  const params = fbaBizDate ? [fbaBizDate] : [];
  const srcRows = db.prepare(`
    SELECT
      m.商品コード, m.商品名, m.取扱区分, m.商品区分, m.売上分類,
      m.標準売価 AS 売価, m.原価, m.送料,
      COALESCE(ne.仕入先コード, m.仕入先コード) AS 仕入先,
      ne.最終仕入日, ne.在庫数 AS 自社在庫, ne.引当数, ne.発注残数 AS 注残数,
      ne.発注ロット単位, ne.代表商品コード, ne.ロケーションコード, ne.商品分類タグ, ne.作成日 AS 登録日,
      COALESCE(v.qty_7d_fba,0) AS s7f, COALESCE(v.qty_7d_nonfba,0) AS s7n, COALESCE(v.qty_7d_total,0) AS s7t,
      COALESCE(v.qty_30d_fba,0) AS s30f, COALESCE(v.qty_30d_nonfba,0) AS s30n, COALESCE(v.qty_30d_total,0) AS s30t,
      r.推奨保有月数,
      COALESCE(fba.qty, 0) AS fba_qty
    FROM m_products m
    LEFT JOIN raw_ne_products ne ON m.商品コード = ne.商品コード COLLATE NOCASE
    LEFT JOIN f_sales_velocity_by_product v ON m.商品コード = v.商品コード COLLATE NOCASE
    LEFT JOIN m_reorder_setting r ON m.商品コード = r.sku COLLATE NOCASE
    ${fbaSub}
    ORDER BY m.商品コード
  `).all(...params);

  const rows = srcRows.map(s => {
    const own = s.自社在庫 ?? 0;
    const alloc = s.引当数 ?? 0;
    const fba = s.fba_qty ?? 0;
    const total = own + fba;
    const profit = (s.売価 != null && s.原価 != null) ? s.売価 - s.原価 - (s.送料 ?? 0) : null;
    const margin = (profit != null && s.売価 != null && s.売価 !== 0) ? r1((profit / s.売価) * 100) : null;
    return {
      商品コード: s.商品コード, 商品名: s.商品名, 仕入先: s.仕入先, 取扱区分: s.取扱区分, 商品区分: s.商品区分, 売上分類: s.売上分類 ?? null,
      最終仕入日: s.最終仕入日 || null,
      在庫保管日数: daysBetween(s.最終仕入日, today),
      総在庫数: total, FBA在庫数: fba, フリー在庫: own - alloc, 注残数: s.注残数 ?? 0, 引当数: alloc,
      総在庫数_引当なし: total - alloc,
      販売数7日_FBA: s.s7f, 販売数7日_FBA以外: s.s7n, 販売数7日_合計: s.s7t,
      販売数30日_FBA: s.s30f, 販売数30日_FBA以外: s.s30n, 販売数30日_合計: s.s30t,
      発注ロット単位: s.発注ロット単位 ?? null, 推奨保有月数: s.推奨保有月数 ?? null,
      売価: s.売価 ?? null, 原価: s.原価 ?? null,
      想定見込み利益: profit == null ? null : Math.round(profit), 概算利益率: margin,
      代表商品コード: s.代表商品コード || null, ロケーションコード: s.ロケーションコード || null,
      商品分類タグ: s.商品分類タグ || null, 登録日: s.登録日 || null,
    };
  });

  // ─── payload_checksum (受信ペイロード相当: 列順固定・null='') ───
  const canonical = rows.map(row => COLUMNS.map(c => row[c] == null ? '' : String(row[c])).join('\t')).join('\n');
  const checksum = crypto.createHash('sha256').update(canonical).digest('hex');

  // ─── 書き込み (meta + rows を run_id で append、published を atomic 切替) ───
  const insMeta = db.prepare(`INSERT INTO product_management_snapshot_meta
    (run_id, as_of_date, generated_at, status, row_count, payload_checksum,
     src_ne_products_synced_at, src_velocity_as_of, src_fba_business_date, src_reorder_updated_at,
     fba_unmapped_qty, ne_fba_overlap, notes)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const insRow = db.prepare(`INSERT INTO product_management_snapshot_rows
    (run_id, ${COLUMNS.join(', ')}) VALUES (?, ${COLUMNS.map(() => '?').join(', ')})`);

  const tx = db.transaction(() => {
    insMeta.run(runId, velAsOf, generatedAt, status, rows.length, checksum,
      neSynced, velAsOf, fbaBizDate, reorderUpdated, null, overlap, reasons.join('; ') || null);
    for (const row of rows) insRow.run(runId, ...COLUMNS.map(c => row[c]));

    // published 切替 (failed 以外)
    if (status !== 'failed') {
      db.prepare(`INSERT INTO product_management_published (id, run_id, published_at)
        VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id, published_at=excluded.published_at`)
        .run(runId, generatedAt);
    }

    // 古い run の掃除 (published + 直近 KEEP_RUNS を温存)
    const keep = db.prepare(`SELECT run_id FROM product_management_snapshot_meta ORDER BY generated_at DESC LIMIT ?`).all(KEEP_RUNS).map(r => r.run_id);
    const pub = db.prepare('SELECT run_id FROM product_management_published WHERE id=1').get()?.run_id;
    if (pub) keep.push(pub);
    const keepSet = [...new Set(keep)];
    const placeholders = keepSet.map(() => '?').join(',');
    db.prepare(`DELETE FROM product_management_snapshot_rows WHERE run_id NOT IN (${placeholders})`).run(...keepSet);
    db.prepare(`DELETE FROM product_management_snapshot_meta WHERE run_id NOT IN (${placeholders})`).run(...keepSet);
  });
  tx();
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  console.log(`[pml-snapshot] ${status} run=${runId} rows=${rows.length} as_of=${velAsOf} fba=${fbaBizDate} overlap=${overlap}${reasons.length ? ' | ' + reasons.join('; ') : ''}`);
  return { ok: status !== 'failed', run_id: runId, status, row_count: rows.length, checksum, as_of: velAsOf, ne_fba_overlap: overlap, reasons };
}

// ─── 単体実行 ───
const isMain = process.argv[1]?.includes('build-product-management-snapshot');
if (isMain) {
  await initDB();
  const r = await buildProductManagementSnapshot();
  console.log('\n結果:', JSON.stringify(r, null, 2));
  process.exit(r.status === 'failed' ? 1 : 0);
}
