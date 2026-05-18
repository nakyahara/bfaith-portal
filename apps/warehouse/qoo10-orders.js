/**
 * qoo10-orders.js — Qoo10 受注 API 取得 (Phase 1 A-1、ミニPC側)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md
 * Codex 11 ラウンドレビュー済 (R11 で critical/high なし、A-1 GO 確定)
 *
 * 機能:
 *   - Qoo10 ShippingBasic.GetShippingInfo_v2 で 90 日チャンク取得 (API 上限制約)
 *   - Page=1 のみ取得 (Qoo10 API は Page>1 で空配列、全件 Page=1 で返る既知挙動)
 *   - status 1-5 全 fetch (raw 保持)、fact 側で Delivered(5) のみ採用
 *   - raw_qoo10_orders に 30 列保持 (65 列 API 中、Cart_Discount_xxx と SettlePrice 等含む)
 *   - 90 日境界 frozen horizon 管理 (last_seen_at + is_frozen_after_horizon、LINEギフト同型)
 *   - order_id = 'api:<orderNo>' で TEXT PK (旧 legacy:* と名前空間分離)
 *   - fail-closed: HTTP エラー / ResultCode != 0 / 必須キー欠落 / fetched>0 inserted=0 / skip_ratio>5%
 *
 * 旧 fetchQoo10 (apps/warehouse/mall-orders.js) は本 script に統合廃止予定。
 * 旧 raw_qoo10_orders (11 cols, order_id=packNo, 17,252 行) は
 * 別途 sql/qoo10/migrate_legacy_raw_qoo10_orders.sql で
 * 'legacy:<packNo>:<id>' + legacy_fields_missing=1 にマイグレ済前提。
 *
 * 使い方:
 *   node apps/warehouse/qoo10-orders.js              直近 90 日 × 3 chunk (= ~270 日) 取得
 *   node apps/warehouse/qoo10-orders.js 90           直近 90 日のみ (daily-sync 想定)
 *   node apps/warehouse/qoo10-orders.js --dry-run    取得のみ、DB 保存しない
 *
 * env:
 *   QOO10_CERT_KEY        Qoo10 API キー (固定、OAuth refresh 不要)
 */
import 'dotenv/config';
import { initDB, getDB, updateSyncMeta } from './db.js';

const API_HOST = 'https://api.qoo10.jp';
const API_PATH = '/GMKT.INC.Front.QAPIService/ebayjapan.qapi/ShippingBasic.GetShippingInfo_v2';
const QOO10_API_WINDOW_DAYS = 90;     // Qoo10 API の取得可能上限 (これを越えると再取得不能)
const FREEZE_SAFETY_MARGIN_DAYS = 14; // API 上限の手前 14 日で先に freeze する安全マージン
const CHUNK_DAYS = 89;                // 1 chunk 89 日 (API 上限 90 日 - 1 で安全側)
const DAILY_FETCH_DAYS = QOO10_API_WINDOW_DAYS;  // daily-sync では直近 90 日を毎回再取得
// freeze 境界 = today - (90 - 14) = today - 76 日
// 「order_date が 76 日以上前なら、もう daily fetch (90日) からも 14 日以内に外れる」→ 先に freeze
const FREEZE_HORIZON_DAYS = QOO10_API_WINDOW_DAYS - FREEZE_SAFETY_MARGIN_DAYS;
const PAGE_SIZE = 200;
const FETCH_INTERVAL_MS = 300;
const SHIPPING_STATS = ['1', '2', '3', '4', '5'];  // status 全部取得、fact 側で Delivered(5) フィルタ

function nowJstIso() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().replace('Z', '+09:00');
}
function jstDateString(daysBack = 0) {
  const d = new Date(Date.now() + 9 * 3600 * 1000 - daysBack * 86400 * 1000);
  return d.toISOString().slice(0, 10);
}
function jstYmdNoSep(date) {
  // Date オブジェクト → 'YYYYMMDD' (JST、Qoo10 API の search_Sdate/Edate 形式)
  const d = new Date(date.getTime() + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10).replace(/-/g, '');
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── API 呼び出し ───

async function fetchChunk(apiKey, stat, sdate, edate) {
  // sdate, edate: 'YYYYMMDD' (JST)、API は 90 日上限、Page=1 で全件
  const url = `${API_HOST}${API_PATH}?key=${apiKey}&ShippingStat=${stat}&search_Sdate=${sdate}&search_Edate=${edate}&Page=1&PageSize=${PAGE_SIZE}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`[qoo10] HTTP ${res.status} stat=${stat} ${sdate}-${edate}: ${(await res.text()).slice(0, 300)}`);
  }
  const data = await res.json();
  if (data.ResultCode !== 0) {
    // ResultCode != 0 = API 側エラー (key 不正 / rate limit / schema drift)
    // 全 stat × chunk で fatal にする。空 result は ResultCode=0 + ResultObject=null/[] で来る
    throw new Error(`[qoo10] APIエラー stat=${stat} ${sdate}-${edate}: ResultCode=${data.ResultCode} ${data.ResultMsg || data.ResultMessage || ''}`);
  }
  if (!data.ResultObject) return [];
  return Array.isArray(data.ResultObject) ? data.ResultObject : [data.ResultObject];
}

async function fetchAllOrders(apiKey, days) {
  const orders = [];
  const end = new Date();                // 今 (UTC)
  const start = new Date();
  start.setDate(start.getDate() - days);
  let chunkEnd = new Date(end);
  let chunkIdx = 0;
  while (chunkEnd > start) {
    let chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() - CHUNK_DAYS);
    if (chunkStart < start) chunkStart = new Date(start);
    const sdate = jstYmdNoSep(chunkStart);
    const edate = jstYmdNoSep(chunkEnd);
    let chunkTotal = 0;
    for (const stat of SHIPPING_STATS) {
      const items = await fetchChunk(apiKey, stat, sdate, edate);
      orders.push(...items);
      chunkTotal += items.length;
      await sleep(FETCH_INTERVAL_MS);
    }
    chunkIdx++;
    console.log(`[qoo10] chunk ${chunkIdx}: ${sdate}-${edate} = ${chunkTotal} 件 (累計 ${orders.length})`);
    chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() - 1);
    await sleep(FETCH_INTERVAL_MS);
  }
  return orders;
}

// ─── DDL (新スキーマ、設計書 §6-1) ───
// 旧 11 列スキーマは migrate_legacy_raw_qoo10_orders.sql で先に rename + backfill 済前提。
// 新 raw に対して CREATE IF NOT EXISTS のみ (idempotent)。
function ensureTable(db) {
  // 旧スキーマ (= sku_code 列で言うところの新フラグ列がない) が残っていたら fatal、migration 未実行
  const cols = db.prepare("PRAGMA table_info(raw_qoo10_orders)").all().map(c => c.name);
  if (cols.length > 0 && !cols.includes('legacy_fields_missing')) {
    throw new Error('[qoo10] 旧スキーマ raw_qoo10_orders を検出 (legacy_fields_missing 列なし) — sql/qoo10/migrate_legacy_raw_qoo10_orders.sql を先に実行してください');
  }
  db.exec(`CREATE TABLE IF NOT EXISTS raw_qoo10_orders (
    order_id            TEXT NOT NULL PRIMARY KEY CHECK (
      order_id LIKE 'legacy:%:%' OR order_id LIKE 'api:%'
    ),
    source_type         TEXT NOT NULL CHECK (
      source_type IN ('legacy_migration', 'api_v2', 'api_v3')
    ),
    source_order_key    TEXT NOT NULL,
    pack_no             INTEGER NOT NULL,
    seller_id           TEXT NOT NULL DEFAULT '',
    shipping_status     TEXT NOT NULL,
    payment_method      TEXT,
    item_code           TEXT NOT NULL,
    seller_item_code    TEXT NOT NULL,
    item_title          TEXT NOT NULL DEFAULT '',
    option_info         TEXT NOT NULL DEFAULT '',
    option_code         TEXT NOT NULL DEFAULT '',
    order_price         REAL NOT NULL DEFAULT 0,
    order_qty           INTEGER NOT NULL DEFAULT 0,
    discount            REAL NOT NULL DEFAULT 0,
    total               REAL NOT NULL DEFAULT 0,
    settle_price        REAL NOT NULL DEFAULT 0,
    seller_discount         REAL NOT NULL DEFAULT 0,
    cart_discount_seller    REAL NOT NULL DEFAULT 0,
    cart_discount_qoo10     REAL NOT NULL DEFAULT 0,
    voucher_code            TEXT NOT NULL DEFAULT '',
    shipping_rate       REAL NOT NULL DEFAULT 0,
    shipping_rate_type  TEXT NOT NULL DEFAULT '',
    shipping_way        TEXT NOT NULL DEFAULT '',
    delivery_company    TEXT NOT NULL DEFAULT '',
    tracking_no         TEXT NOT NULL DEFAULT '',
    order_date          TEXT NOT NULL,
    payment_date        TEXT,
    shipping_date       TEXT,
    delivered_date      TEXT,
    oversea_consignment TEXT NOT NULL DEFAULT 'N',
    cod_price           REAL NOT NULL DEFAULT 0,
    related_order       TEXT NOT NULL DEFAULT '',
    branch_name         TEXT NOT NULL DEFAULT '',
    first_seen_at         TEXT NOT NULL,
    last_seen_at          TEXT NOT NULL,
    last_api_snapshot_at  TEXT NOT NULL,
    is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0,
    legacy_fields_missing  INTEGER NOT NULL DEFAULT 0,
    synced_at           TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_order_date ON raw_qoo10_orders(order_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_status     ON raw_qoo10_orders(shipping_status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_sku        ON raw_qoo10_orders(seller_item_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_pack       ON raw_qoo10_orders(pack_no)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_frozen     ON raw_qoo10_orders(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_legacy     ON raw_qoo10_orders(legacy_fields_missing) WHERE legacy_fields_missing = 1');
  db.exec('CREATE INDEX IF NOT EXISTS idx_qoo10_source     ON raw_qoo10_orders(source_type)');
}

// ─── UPSERT (frozen 行更新禁止、LINEギフト同型) ───

const INSERT_SQL = `
INSERT INTO raw_qoo10_orders (
  order_id, source_type, source_order_key, pack_no, seller_id,
  shipping_status, payment_method,
  item_code, seller_item_code, item_title, option_info, option_code,
  order_price, order_qty, discount, total, settle_price,
  seller_discount, cart_discount_seller, cart_discount_qoo10, voucher_code,
  shipping_rate, shipping_rate_type, shipping_way, delivery_company, tracking_no,
  order_date, payment_date, shipping_date, delivered_date,
  oversea_consignment, cod_price, related_order, branch_name,
  first_seen_at, last_seen_at, last_api_snapshot_at, is_frozen_after_horizon,
  legacy_fields_missing, synced_at
) VALUES (
  @order_id, @source_type, @source_order_key, @pack_no, @seller_id,
  @shipping_status, @payment_method,
  @item_code, @seller_item_code, @item_title, @option_info, @option_code,
  @order_price, @order_qty, @discount, @total, @settle_price,
  @seller_discount, @cart_discount_seller, @cart_discount_qoo10, @voucher_code,
  @shipping_rate, @shipping_rate_type, @shipping_way, @delivery_company, @tracking_no,
  @order_date, @payment_date, @shipping_date, @delivered_date,
  @oversea_consignment, @cod_price, @related_order, @branch_name,
  @first_seen_at, @last_seen_at, @last_api_snapshot_at, @is_frozen_after_horizon,
  @legacy_fields_missing, @synced_at
)
ON CONFLICT(order_id) DO UPDATE SET
  -- frozen 行は UPDATE 禁止 (§12 設計、LINEギフト同型)
  source_type           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.source_type           ELSE excluded.source_type           END,
  source_order_key      = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.source_order_key      ELSE excluded.source_order_key      END,
  pack_no               = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.pack_no               ELSE excluded.pack_no               END,
  seller_id             = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.seller_id             ELSE excluded.seller_id             END,
  shipping_status       = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.shipping_status       ELSE excluded.shipping_status       END,
  payment_method        = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.payment_method        ELSE excluded.payment_method        END,
  item_code             = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.item_code             ELSE excluded.item_code             END,
  seller_item_code      = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.seller_item_code      ELSE excluded.seller_item_code      END,
  item_title            = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.item_title            ELSE excluded.item_title            END,
  option_info           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.option_info           ELSE excluded.option_info           END,
  option_code           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.option_code           ELSE excluded.option_code           END,
  order_price           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.order_price           ELSE excluded.order_price           END,
  order_qty             = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.order_qty             ELSE excluded.order_qty             END,
  discount              = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.discount              ELSE excluded.discount              END,
  total                 = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.total                 ELSE excluded.total                 END,
  settle_price          = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.settle_price          ELSE excluded.settle_price          END,
  seller_discount       = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.seller_discount       ELSE excluded.seller_discount       END,
  cart_discount_seller  = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.cart_discount_seller  ELSE excluded.cart_discount_seller  END,
  cart_discount_qoo10   = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.cart_discount_qoo10   ELSE excluded.cart_discount_qoo10   END,
  voucher_code          = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.voucher_code          ELSE excluded.voucher_code          END,
  shipping_rate         = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.shipping_rate         ELSE excluded.shipping_rate         END,
  shipping_rate_type    = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.shipping_rate_type    ELSE excluded.shipping_rate_type    END,
  shipping_way          = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.shipping_way          ELSE excluded.shipping_way          END,
  delivery_company      = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.delivery_company      ELSE excluded.delivery_company      END,
  tracking_no           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.tracking_no           ELSE excluded.tracking_no           END,
  order_date            = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.order_date            ELSE excluded.order_date            END,
  payment_date          = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.payment_date          ELSE excluded.payment_date          END,
  shipping_date         = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.shipping_date         ELSE excluded.shipping_date         END,
  delivered_date        = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.delivered_date        ELSE excluded.delivered_date        END,
  oversea_consignment   = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.oversea_consignment   ELSE excluded.oversea_consignment   END,
  cod_price             = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.cod_price             ELSE excluded.cod_price             END,
  related_order         = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.related_order         ELSE excluded.related_order         END,
  branch_name           = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.branch_name           ELSE excluded.branch_name           END,
  -- first_seen_at は新規時のみ設定 (UPSERT で温存)
  -- Codex R2 High #1 反映: audit 列も frozen 時は保護 (last_seen_at が動くと freeze 後 raw UPDATE 禁止が破綻)
  last_seen_at          = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.last_seen_at          ELSE excluded.last_seen_at          END,
  last_api_snapshot_at  = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.last_api_snapshot_at  ELSE excluded.last_api_snapshot_at  END,
  -- is_frozen_after_horizon / legacy_fields_missing はここでは触らない
  synced_at             = CASE WHEN raw_qoo10_orders.is_frozen_after_horizon = 1 THEN raw_qoo10_orders.synced_at             ELSE excluded.synced_at             END
WHERE raw_qoo10_orders.is_frozen_after_horizon = 0
`;

function toNum(v, defaultV = 0) {
  if (v == null || v === '') return defaultV;
  const n = Number(v);
  return Number.isFinite(n) ? n : defaultV;
}
function toInt(v, defaultV = 0) {
  if (v == null || v === '') return defaultV;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : defaultV;
}
function toStr(v, defaultV = '') {
  if (v == null) return defaultV;
  return String(v);
}

// Codex R1 Medium #3 反映: outer transaction で raw 反映 + 検証を atomic 化するため、
// この関数は自前 transaction を持たない。caller 側 (main) で BEGIN/COMMIT/ROLLBACK を制御する。
function insertOrdersNoTx(db, apiOrders, snapshotAt) {
  const ins = db.prepare(INSERT_SQL);
  // Codex R3 High #1 反映: run().changes で実 DB 変更件数を分離。
  //   - mutated: INSERT 新規 + UPDATE 成立 (run().changes > 0)
  //   - skippedFrozenConflict: ON CONFLICT WHERE で frozen 行が no-op (changes === 0)
  // Codex R4 High #1 反映: mutableCandidates (= order_date >= watermark の件数) も別カウント。
  //   fail-closed 判定は mutableCandidates > 0 && mutated === 0 でのみ FATAL、
  //   「直近 76 日新規受注ゼロ → frozen 帯のみ返る正常ケース」を誤発火させない。
  const watermark = jstDateString(FREEZE_HORIZON_DAYS);
  let mutated = 0, skippedFrozenConflict = 0, skippedInvalid = 0, mutableCandidates = 0;
  const skippedExamples = [];

  {
    for (const o of apiOrders) {
      // 必須キー欠落チェック (fail-closed)
      // 旧 fetchQoo10 は packNo を主キー使用していたが、grain 崩壊バグの原因。
      // 新スキーマでは orderNo を主キーに、packNo は同梱単位 (pack_no 列) に降格。
      const orderNo = toStr(o.orderNo).trim();
      const packNoStr = toStr(o.packNo).trim();
      if (!orderNo) {
        skippedInvalid++;
        if (skippedExamples.length < 5) {
          skippedExamples.push({
            reason: 'orderNo missing',
            packNo: packNoStr,
            shipping_status: toStr(o.shippingStatus),
            present_keys: Object.keys(o).slice(0, 20),
          });
        }
        continue;
      }
      if (!packNoStr) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'packNo missing', orderNo });
        continue;
      }
      const packNo = parseInt(packNoStr, 10);
      if (!Number.isFinite(packNo) || packNo <= 0) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'packNo non-numeric', orderNo, packNo: packNoStr });
        continue;
      }
      const sellerItemCode = toStr(o.sellerItemCode).trim();
      const itemCode = toStr(o.itemCode).trim();
      if (!sellerItemCode && !itemCode) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'sellerItemCode AND itemCode both empty', orderNo });
        continue;
      }
      const orderDate = toStr(o.orderDate).trim();
      if (!orderDate) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'orderDate empty', orderNo });
        continue;
      }
      const shippingStatus = toStr(o.shippingStatus).trim();
      if (!shippingStatus) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'shippingStatus empty', orderNo });
        continue;
      }

      const row = {
        order_id: `api:${orderNo}`,
        source_type: 'api_v2',
        source_order_key: orderNo,
        pack_no: packNo,
        seller_id: toStr(o.sellerID),
        shipping_status: shippingStatus,
        payment_method: toStr(o.PaymentMethod) || null,
        item_code: itemCode || sellerItemCode,
        seller_item_code: (sellerItemCode || itemCode).toLowerCase(),  // m_products との突合用 (sku case 正規化)
        item_title: toStr(o.itemTitle),
        option_info: toStr(o.option),
        option_code: toStr(o.optionCode),
        order_price: toNum(o.orderPrice),
        order_qty: toInt(o.orderQty),
        discount: toNum(o.discount),
        total: toNum(o.total),
        settle_price: toNum(o.SettlePrice),
        seller_discount: toNum(o.SellerDiscount),
        cart_discount_seller: toNum(o.Cart_Discount_Seller),
        cart_discount_qoo10: toNum(o.Cart_Discount_Qoo10),
        voucher_code: toStr(o.VoucherCode),
        shipping_rate: toNum(o.shippingRate),
        shipping_rate_type: toStr(o.shippingRateType),
        shipping_way: toStr(o.shippingWay),
        delivery_company: toStr(o.deliveryCompany),
        tracking_no: toStr(o.trackingNo),
        order_date: orderDate,
        payment_date: toStr(o.paymentDate) || null,
        shipping_date: toStr(o.shippingDate) || null,
        delivered_date: toStr(o.deliveredDate) || null,
        oversea_consignment: toStr(o.OverseaConsignment) || 'N',
        cod_price: toNum(o.cod_price),
        related_order: toStr(o.RelatedOrder),
        branch_name: toStr(o.branchName),
        first_seen_at: snapshotAt,
        last_seen_at: snapshotAt,
        last_api_snapshot_at: snapshotAt,
        is_frozen_after_horizon: 0,
        legacy_fields_missing: 0,
        synced_at: snapshotAt,
      };
      // mutableCandidates 集計: 可変帯 (order_date >= watermark) は freeze 対象外、
      // この件数が 0 なら「全件 frozen 帯」となり mutated===0 でも fatal にしない。
      if (orderDate.slice(0, 10) >= watermark) mutableCandidates++;

      const r = ins.run(row);
      if (r.changes > 0) mutated++;
      else skippedFrozenConflict++;
    }
  }
  return { mutated, skippedFrozenConflict, skippedInvalid, mutableCandidates, skippedExamples };
}

// ─── frozen 切替 (§12 設計、order_date 基準、Codex R1 High #1 反映 2026-05-18) ───
// LINEギフトは last_seen_at 基準だが、Qoo10 は daily で直近 90 日を毎回再取得するため
// last_seen_at 基準だと「API range 外退場 + 14日」というズレた境界になる。
// → Qoo10 では order_date 基準で「order_date < today - 76日 (= API 上限 90 - 14 安全マージン)」を freeze。
// これにより daily fetch range の最外周 14 日に入る前に freeze、以後 raw UPDATE 禁止。
// order_date は 'YYYY-MM-DD HH:MM:SS' または 'YYYY-MM-DD HH:MM' 形式、substr(...,1,10) で日付直接比較。
function applyHorizonFreeze(db) {
  const watermark = jstDateString(FREEZE_HORIZON_DAYS);
  const result = db.prepare(`
    UPDATE raw_qoo10_orders
    SET is_frozen_after_horizon = 1
    WHERE is_frozen_after_horizon = 0
      AND substr(order_date, 1, 10) < ?
  `).run(watermark);
  return { changes: result.changes, watermark };
}

// ─── fail-closed 評価 ───
// Codex R3 High #1: 「mutated (実 DB 変更)」 と「frozen no-op」を分離評価。
// Codex R4 High #1: mutableCandidates (可変帯 = order_date >= watermark の件数) を見て
//   「全件 frozen 帯」の正常ケースを誤 fatal させない。
function evaluateFetchResult(label, fetched, mutated, skippedFrozenConflict, skippedInvalid, mutableCandidates) {
  const skipRatio = fetched > 0 ? skippedInvalid / fetched : 0;
  console.log(`[qoo10] ${label}: fetched=${fetched} mutated=${mutated} mutable_candidates=${mutableCandidates} skipped_frozen_noop=${skippedFrozenConflict} skipped_invalid=${skippedInvalid} (skip_ratio=${(skipRatio * 100).toFixed(1)}%)`);
  // 可変帯 (today-76 以降) の行があったのに 1 件も反映されなかった = 異常 (API 障害 / schema drift)
  if (mutableCandidates > 0 && mutated === 0) {
    console.error(`[qoo10] FATAL: 可変帯 ${mutableCandidates} 件 fetched だが mutated=0、API 障害 / schema drift の可能性`);
    return 'fatal';
  }
  // mutableCandidates===0 = 直近 76 日新規受注ゼロの正常 no-op (店舗閑散期、長期未受注 SKU 等)
  if (mutableCandidates === 0 && fetched > 0) {
    console.warn(`[qoo10] NOTE: 可変帯 0 件 / fetched=${fetched} (全件 frozen 帯)、76 日以上新規受注なし — 正常な no-op として継続`);
  }
  if (skipRatio > 0.05) {
    console.error(`[qoo10] FATAL: skip_ratio=${(skipRatio * 100).toFixed(1)}% > 5%`);
    return 'fatal';
  }
  return 'ok';
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const daysArg = args.find(a => /^\d+$/.test(a));
  const days = daysArg ? parseInt(daysArg, 10) : 270;  // default = 90 × 3 chunk

  const apiKey = process.env.QOO10_CERT_KEY;
  if (!apiKey) {
    console.error('[qoo10] QOO10_CERT_KEY 未設定 — exit 1');
    process.exit(1);
  }

  if (!isDryRun) await initDB();
  const db = !isDryRun ? getDB() : null;
  if (db) ensureTable(db);

  console.log(`[qoo10] 受注取得開始 (直近 ${days} 日、chunk=${CHUNK_DAYS}日、status=${SHIPPING_STATS.join(',')})`);
  const orders = await fetchAllOrders(apiKey, days);
  console.log(`[qoo10] 取得完了: ${orders.length} 件`);

  if (isDryRun) {
    console.log('[qoo10] --dry-run 指定: DB 保存スキップ、終了');
    return;
  }

  const snapshotAt = nowJstIso();

  // Codex R1 Medium #3 反映: raw 反映 + 検証 + freeze 切替を 1 outer transaction で atomic 化。
  // fatal 判定時は ROLLBACK して部分書き込みを残さない (fail-closed)。
  let mutated = 0, skippedFrozenConflict = 0, skippedInvalid = 0, mutableCandidates = 0, skippedExamples = [];
  let frozenChanges = 0, frozenWatermark = null;
  db.exec('BEGIN IMMEDIATE');
  try {
    const r = insertOrdersNoTx(db, orders, snapshotAt);
    mutated = r.mutated; skippedFrozenConflict = r.skippedFrozenConflict;
    skippedInvalid = r.skippedInvalid; mutableCandidates = r.mutableCandidates;
    skippedExamples = r.skippedExamples;

    const verdict = evaluateFetchResult('fetchQoo10', orders.length, mutated, skippedFrozenConflict, skippedInvalid, mutableCandidates);
    if (skippedExamples.length > 0) {
      console.log('[qoo10] skip 例 (先頭5):');
      for (const e of skippedExamples) console.log(`  ${JSON.stringify(e)}`);
    }
    if (verdict === 'fatal') {
      throw new Error('FAIL_CLOSED: 取得結果が fail-closed 条件にヒット、raw 反映を ROLLBACK');
    }

    const fr = applyHorizonFreeze(db);
    frozenChanges = fr.changes; frozenWatermark = fr.watermark;
    db.exec('COMMIT');
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch (_) { /* tx 既に閉じてる可能性、無視 */ }
    console.error(`[qoo10] outer transaction ROLLBACK: ${e.message}`);
    process.exit(1);
  }

  if (frozenChanges > 0) console.log(`[qoo10] frozen 切替: ${frozenChanges} 行を is_frozen_after_horizon=1 に (watermark=${frozenWatermark})`);
  else console.log(`[qoo10] frozen 切替対象なし (watermark=${frozenWatermark})`);

  updateSyncMeta('qoo10_last_sync', snapshotAt);
  console.log(`[qoo10] 完了: mutated=${mutated} / mutable_candidates=${mutableCandidates} / frozen_noop=${skippedFrozenConflict} / skipped_invalid=${skippedInvalid} / frozen_changes=${frozenChanges}`);
}

main().catch(e => {
  console.error(`[qoo10] 致命的エラー: ${e.message}\n${e.stack}`);
  process.exit(1);
});
