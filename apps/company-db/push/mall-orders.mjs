#!/usr/bin/env node
/**
 * mall-orders.mjs — miniPC のモールの注文 (warehouse.db の raw_*_orders) を Company DB (Render Postgres) に送る。D5b (08 §4.1 / §4.7 / §9 D5)。楽天 (D5b-1) / Amazon (D5b-2。--mall amazon。元 = raw_sp_orders、128 万注文) / au PAY・LINE ギフト (D5b-3。--mall aupay / --mall linegift。どちらも年 1 万注文前後) / Qoo10 (D5b-4。--mall qoo10。API の行だけ = 2026-02-19 以降) / Yahoo (D5b-5。--mall yahoo。2026-09-26 に D-32 を a = 入れる に。年 5 万注文前後)
 *
 * 流れは伝票 (ne-shipments.mjs) と同じ共通部 (pipeline.mjs): 台帳 (種類 'order:<mall>') の指紋で差分を決め、outbox から chunk で送り、失敗・stale は次回また送る。
 * 送った後、注文が入ったので伝票との結び直し (POST /shipments/relink = core.relink_shipments_bulk) を回す。
 *
 * 使い方 (miniPC。daily-sync の 1 ステップ = 楽天 RMS API の取込の後):
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental                        → 範囲 (注文日 2025-01-01 以降 = D-28、または追跡中) のうち指紋が変わった注文
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --from 2025-01-01 --to 2025-02-28 --no-relink   → 注文日の範囲だけ・送るだけ (初回のバックフィルを 2 か月ずつ。最後に --relink を 1 回)
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --incremental --dry-run             → 送らずに件数と例だけ
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --reconcile --days 90               → 日ごとの注文数・明細数・商品代の合計を raw と Render で突き合わせる (差があれば exit 1)
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --reset-ledger                      → 台帳の指紋を空にする
 *   node apps/company-db/push/mall-orders.mjs --relink                                            → 伝票との結び直しだけ
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --refresh-sales [--all]              → 売上日次 (mart.sales_daily。0021) の作り直しだけ (ふだんは push の後に自動で回る。--all = 全部の日)
 *   node apps/company-db/push/mall-orders.mjs --mall rakuten --check-sales --days 90              → 公開中の売上日次と材料 (core.orders) の食い違いを見る (差があれば exit 1)
 *   node apps/company-db/push/mall-orders.mjs --mall amazon --incremental --require-backfilled   → daily-sync 用 (Amazon。台帳にバックフィルの完了印が無ければ送らずに「バックフィル前」と出して exit 0)
 *   node apps/company-db/push/mall-orders.mjs --mall amazon --mark-backfilled                     → バックフィルの完了印 (全期間を流して --reconcile --all が一致したのを見てから)
 *
 * env: DATA_DIR / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CDB_PUSH_CHUNK (伝票と同じ)
 * 🚨 秘密は表示しない。🚨 daily-sync の runScript は引数が無いと '7' を足すので、必ず --mall などの引数を付けて呼ぶ。warehouse.db は読むだけ
 */
import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { openLedger } from './ledger.mjs';
import { runPush, summarizePush, splitWindows, isDate, jstDate, DEFAULT_CHUNK, MAX_CHUNK, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { buildRakutenOrder, RAKUTEN_TRANSFORM_VERSION, RAKUTEN_SENTINEL, buildAmazonOrder, AMAZON_TRANSFORM_VERSION, AMAZON_SALES_CHANNEL,
  buildAupayOrder, AUPAY_TRANSFORM_VERSION, AUPAY_COLUMNS, aupayDatetimeToIso, buildLinegiftOrder, LINEGIFT_TRANSFORM_VERSION, LINEGIFT_COLUMNS, isLinegiftJst,
  buildQoo10Order, QOO10_TRANSFORM_VERSION, QOO10_COLUMNS, isQoo10Jst, isQoo10ApiKey,
  buildYahooOrder, YAHOO_TRANSFORM_VERSION, YAHOO_COLUMNS, isYahooJst, isYahooOrderNo } from './mall-orders-transform.mjs';
import { syncBase } from './ne-shipments.mjs';
import { writeEvidence } from './evidence.mjs';

export const DEFAULT_FLOOR = '2025-01-01';          // D-28
/**
 * 台帳の meta (種類ごと): '1' = 初回のバックフィルが全期間そろった (人が --reconcile --all を見てから --mark-backfilled で付ける)。
 * 🚨 指紋の件数では判定しない: 1 か月だけ流した・途中で落ちた時点で件数は 0 でなくなる (Codex D5b-2 R1 #1)。--reset-ledger は指紋だけ空にして meta は残す = 完了印は消えない (R1 #2)
 */
export const BACKFILL_DONE_KEY = 'backfill_done';
export const isBackfillDone = (ledger) => ledger.getMeta(BACKFILL_DONE_KEY) === '1';
export const MALL_SPECS = {
  rakuten: {
    label: '楽天の注文', scope: 'main', transformVersion: RAKUTEN_TRANSFORM_VERSION,
    /** raw を注文番号順に流し読みして、注文ごとに { key, no, rows } を返す (同じ接続の 1 statement = 1 スナップショット) */
    iterate: function* (warehouse) {
      const it = warehouse.prepare('select * from raw_rakuten_orders order by order_number, item_detail_id').iterate();
      let cur = null;
      for (const row of it) {
        const no = String(row.order_number ?? '');
        if (cur && cur.no === no) { cur.rows.push(row); continue; }
        if (cur) yield cur;
        cur = { key: `rakuten|main|${no}`, no, rows: [row], order_date: String(row.order_date ?? '') };
      }
      if (cur) yield cur;
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    /**
     * floor 以降に出荷確定した楽天の伝票 (raw_ne_order_base の店舗 1 = core.ne_shops。NE 受注番号 = 楽天の注文番号) が参照する、**楽天側の注文日** が floor より前の注文番号 (D-28)。
     * 🚨 古いかどうかは NE の受注日ではなく楽天の order_date で見る (両方の日付が一致する保証は無い。Codex R4 #2)。表が無ければ null (呼ぶ側が警告)。呼ぶ側の読み取り取引の中で呼ぶ
     */
    referencedByShipments: (warehouse, floor) => {
      if (!warehouse.prepare(`select name from sqlite_master where type = 'table' and name = 'raw_ne_order_base'`).get()) return null;
      return new Set(warehouse.prepare(`select distinct b.受注番号 as no from raw_ne_order_base b join raw_rakuten_orders r on r.order_number = b.受注番号
         where b.店舗コード = '1' and b.受注番号 is not null and b.出荷確定日 >= ? and r.order_date < ?`).all(`${floor} 00:00:00`, floor).map((x) => String(x.no)));
    },
    build: (group, ctx, stats) => buildRakutenOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString(), stats }),
    /** 突合の材料 (miniPC 側): 注文日ごとの 注文数 / 明細数 / 商品代 (goods_price) の合計 / 取消の注文数。raw と同じ式を Render (GET /orders/daily) が持つ */
    dailySql: `with o as (
        select order_number, substr(min(order_date), 1, 10) as d, max(order_status) as st, max(goods_price) as gp, count(*) as n_lines
          from raw_rakuten_orders group by order_number)
      select d as order_date, count(*) as orders, sum(n_lines) as lines,
             sum(case when gp is null or gp = ${RAKUTEN_SENTINEL} or gp < 0 then 0 else round(gp) end) as items_amount_jpy,
             sum(case when st in (800, 900) then 1 else 0 end) as cancelled
        from o where d >= ? and d <= ? group by d`,
  },
  /**
   * Amazon (D5b-2)。元 = raw_sp_orders (注文 ID 単位で最新の状態に置き換わる current 表。追記ログ raw_sp_orders_log は 60 日で回転するので使わない)。
   * 🚨 sales_channel が Amazon.co.jp でない注文 (マルチチャネル発送 = 他モールの注文を FBA から出しただけ) は送らない (飛ばして数える。突合の式も同じ条件)。
   * title は読まない (要らない列を運ばない)。purchase_date は取込側が '+09:00' の ISO8601 にそろえている = 文字列のまま JST の日付で比べられる
   */
  amazon: {
    label: 'Amazon の注文', scope: 'jp', transformVersion: AMAZON_TRANSFORM_VERSION,
    iterate: function* (warehouse, st) {
      const it = warehouse.prepare(`select amazon_order_id, purchase_date, last_updated_date, order_status, fulfillment_channel, sales_channel, asin, seller_sku, quantity,
          item_price, item_tax, shipping_price, shipping_tax, promotion_discount, currency, item_status, synced_at from raw_sp_orders order by amazon_order_id, id`).iterate();
      let cur = null;
      const emit = function* (g) { if (g.rows.every((r) => r.sales_channel === AMAZON_SALES_CHANNEL)) yield g; else if (st) st.skippedNonAmazon = (st.skippedNonAmazon || 0) + 1; };
      for (const row of it) {
        const no = String(row.amazon_order_id ?? '');
        if (cur && cur.no === no) { cur.rows.push(row); continue; }
        if (cur) yield* emit(cur);
        cur = { key: `amazon|jp|${no}`, no, rows: [row], order_date: String(row.purchase_date ?? '') };
      }
      if (cur) yield* emit(cur);
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    /** floor 以降に出荷確定した Amazon 自社発送の伝票 (NE の店舗 4。受注番号 = amazon_order_id) が参照する、**Amazon 側の注文日** が floor より前の注文 (D-28。実測 90 注文) */
    referencedByShipments: (warehouse, floor) => {
      if (!warehouse.prepare(`select name from sqlite_master where type = 'table' and name = 'raw_ne_order_base'`).get()) return null;
      return new Set(warehouse.prepare(`select distinct b.受注番号 as no from raw_ne_order_base b join raw_sp_orders s on s.amazon_order_id = b.受注番号
         where b.店舗コード = '4' and b.受注番号 is not null and b.出荷確定日 >= ? and s.purchase_date < ?`).all(`${floor} 00:00:00`, floor).map((x) => String(x.no)));
    },
    build: (group, ctx, stats) => buildAmazonOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString(), stats }),
    /** 突合の材料: 注文日 (JST) ごとの 注文数 / 明細数 / 商品代 / 取消の注文数。Amazon.co.jp の注文だけ (NULL を含め全行がそうである注文 = iterate と同じ条件)。
     *  🚨 整形 (buildAmazonOrder) と同じ前処理をしてから判定する: 金額は NULL → 0・四捨五入してから > 0、状態は前後の空白を除く (Codex R2 #1。NULL や 0.1 円で両側が食い違わない)。
     *  商品代は整形と同じ規則: 金額の分からない (item_price が 0) 取消でない明細が 1 つでも残る注文は null (= 0 として足す)。それ以外は金額のある行の合計 */
    dailySql: `with o as (
        select amazon_order_id, substr(min(purchase_date), 1, 10) as d, max(trim(coalesce(order_status, ''))) as st, count(*) as n_lines,
               sum(case when round(coalesce(item_price, 0)) > 0 then round(item_price) else 0 end) as priced_amt,
               sum(case when round(coalesce(item_price, 0)) > 0 then 0 when trim(coalesce(order_status, '')) = 'Cancelled' or trim(coalesce(item_status, '')) = 'Cancelled' then 0 else 1 end) as unknown_live,
               sum(case when sales_channel = '${AMAZON_SALES_CHANNEL}' then 0 else 1 end) as other_channel
          from raw_sp_orders group by amazon_order_id)
      select d as order_date, count(*) as orders, sum(n_lines) as lines, sum(case when unknown_live > 0 then 0 else priced_amt end) as items_amount_jpy,
             sum(case when st = 'Cancelled' then 1 else 0 end) as cancelled
        from o where other_channel = 0 and d >= ? and d <= ? group by d`,
  },
  /**
   * au PAY マーケット (D5b-3)。元 = raw_aupay_orders。🚨 氏名・住所・電話・メール・自由記述の列がある表 → 要る列だけを select (AUPAY_COLUMNS)。
   * order_date は 'YYYY/MM/DD HH:MM' (JST) → 範囲の比較のために group.order_date は ISO8601 にそろえる
   */
  aupay: {
    label: 'au PAY の注文', scope: 'main', transformVersion: AUPAY_TRANSFORM_VERSION,
    iterate: function* (warehouse) {
      const it = warehouse.prepare(`select ${AUPAY_COLUMNS.join(', ')} from raw_aupay_orders order by order_id, order_detail_id`).iterate();
      let cur = null;
      for (const row of it) {
        const no = String(row.order_id ?? '');
        if (cur && cur.no === no) {
          // 範囲は先頭の明細の日時で決める → 後ろの明細の日時が先頭と違う注文 (整形は「行によって違う」で例外にする) も必ず整形に渡す (Codex D5b-3 R4 #1)
          if ((row.order_date ?? null) !== (cur.rows[0].order_date ?? null)) cur.invalidDate = true;
          cur.rows.push(row); continue;
        }
        if (cur) yield cur;
        // 🚨 注文日時が読めない注文を黙って範囲の外に落とさない: invalidDate の印を付けると、どの mode でも範囲に入り build が例外にする (= 整形できない ❌。Codex D5b-3 R1 #1)
        let iso = ''; try { iso = aupayDatetimeToIso(row.order_date) || ''; } catch { iso = ''; }
        cur = { key: `aupay|main|${no}`, no, rows: [row], order_date: iso, invalidDate: !iso };
      }
      if (cur) yield cur;
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    referencedByShipments: (warehouse, floor) => {
      if (!warehouse.prepare(`select name from sqlite_master where type = 'table' and name = 'raw_ne_order_base'`).get()) return null;
      return new Set(warehouse.prepare(`select distinct b.受注番号 as no from raw_ne_order_base b join raw_aupay_orders a on a.order_id = b.受注番号
         where b.店舗コード = '5' and b.受注番号 is not null and b.出荷確定日 >= ? and replace(substr(a.order_date, 1, 10), '/', '-') < ?`).all(`${floor} 00:00:00`, floor).map((x) => String(x.no)));
    },
    build: (group, ctx) => buildAupayOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString() }),
    /** 突合の材料: 注文日 (JST) ごとの 注文数 / 明細数 / 商品代 (total_sale_price) / 取消の注文数 (cancel_status = 'C' か order_status = 'キャンセル') */
    dailySql: `with o as (
        select order_id, replace(substr(min(order_date), 1, 10), '/', '-') as d, count(*) as n_lines, max(round(coalesce(total_sale_price, 0))) as amt,
               max(case when trim(coalesce(cancel_status, '')) = 'C' or trim(coalesce(order_status, '')) = 'キャンセル' then 1 else 0 end) as c
          from raw_aupay_orders group by order_id)
      select d as order_date, count(*) as orders, sum(n_lines) as lines, sum(amt) as items_amount_jpy, sum(c) as cancelled
        from o where d >= ? and d <= ? group by d`,
  },
  /**
   * LINE ギフト (D5b-3)。元 = raw_linegift_orders (1 行 = 1 注文 = 1 商品)。🚨 LINE の ID・送付先の氏名・住所・電話の列がある表 → 要る列だけを select (LINEGIFT_COLUMNS)。
   * raw は 2026-02-07 以降だけ (それより前の注文は無い = 出荷が参照する古い注文は辿れないので referencedByShipments は持たない)
   */
  linegift: {
    label: 'LINE ギフトの注文', scope: 'main', transformVersion: LINEGIFT_TRANSFORM_VERSION,
    iterate: function* (warehouse) {
      for (const row of warehouse.prepare(`select ${LINEGIFT_COLUMNS.join(', ')} from raw_linegift_orders order by order_id`).iterate()) {
        const no = String(row.order_id ?? '');
        const at = row.bought_at_jst;   // 🚨 原値のまま検証する (String() に通さない): TEXT の列にも BLOB や数値は入り得て、整形は原値を拒む = 文字列化してから見ると片方だけ通る (Codex D5b-3 R4 #2)
        const okJst = isLinegiftJst(at);   // 範囲・突合は先頭 10 文字を JST の日付として使う = '+09:00' の形で実在する日時だけ受ける (build も同じ関数で拒む。R1 #1 / #2・R2 #1)
        yield { key: `linegift|main|${no}`, no, rows: [row], order_date: okJst ? at : '', invalidDate: !okJst };
      }
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    build: (group, ctx) => buildLinegiftOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString() }),
    dailySql: `select substr(bought_at_jst, 1, 10) as order_date, count(*) as orders, count(*) as lines, sum(round(coalesce(selling_price, 0))) as items_amount_jpy,
             sum(case when trim(coalesce(status, '')) = 'cancel' then 1 else 0 end) as cancelled
        from raw_linegift_orders where substr(bought_at_jst, 1, 10) >= ? and substr(bought_at_jst, 1, 10) <= ? group by 1`,
  },
  /**
   * Qoo10 (D5b-4)。元 = raw_qoo10_orders の **API の行だけ** (source_type 'api_%'。1 行 = 1 注文 = 1 商品。2026-02-19 以降)。
   * 🚨 旧データの行 (legacy_migration。鍵がカート番号に潰れている・2026-02〜05 は API の行と二重) は送らない = 飛ばして数える (突合の式も同じ条件)。
   * 範囲の判定に使う order_date は、整形と同じ関数 (isQoo10Jst。原値のまま) で検証する。raw が 2026-02 以降だけなので referencedByShipments は持たない
   */
  qoo10: {
    label: 'Qoo10 の注文', scope: 'main', transformVersion: QOO10_TRANSFORM_VERSION,
    iterate: function* (warehouse, st) {
      if (st) st.skippedLegacy = warehouse.prepare(`select count(*) as n from raw_qoo10_orders where source_type is null or substr(source_type, 1, 4) <> 'api_'`).get().n;
      // 🚨 範囲・台帳の鍵に使う値は、整形が検証するのと同じ関数・原値のまま・同じ行の集合で検証する (#1363 の約束)。
      //   invalid = 注文日時が読めない / 鍵の形が違う (前後の空白・order_id と不一致) / 同じ注文番号の行が複数 → どの mode でも範囲に入れて build で例外にする (Codex D5b-4 R1 #1 / #2)
      const groupOf = (rows) => {
        const row = rows[0];
        const okDt = isQoo10Jst(row.order_date), okKey = isQoo10ApiKey(row);
        const no = okKey ? row.source_order_key : String(row.source_order_key ?? '');
        return { key: `qoo10|main|${no}`, no, rows, order_date: okDt ? `${row.order_date.slice(0, 10)}T${row.order_date.slice(11)}+09:00` : '', invalidDate: !okDt || !okKey || rows.length !== 1 };
      };
      let cur = null;   // 同じ source_order_key の行 (api_v2 と api_v3 の並存など) はまとめて渡す = 片方だけ範囲の外でも重複に気づく
      for (const row of warehouse.prepare(`select ${QOO10_COLUMNS.join(', ')} from raw_qoo10_orders where substr(source_type, 1, 4) = 'api_' order by source_order_key, order_id`).iterate()) {
        if (cur && cur[0].source_order_key === row.source_order_key) { cur.push(row); continue; }
        if (cur) yield groupOf(cur);
        cur = [row];
      }
      if (cur) yield groupOf(cur);
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    build: (group, ctx, stats) => buildQoo10Order(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString(), stats }),
    /** 突合の材料: 注文日 (JST) ごとの 注文数 / 明細数 (= 注文数) / 商品代 (order_price × order_qty。order_price が 0 なら「分からない」= 0 として足す) / 取消 (API に出てこない = 常に 0)。API の行だけ */
    dailySql: `select substr(order_date, 1, 10) as order_date, count(*) as orders, count(*) as lines,
             sum(case when round(coalesce(order_price, 0)) > 0 then round(order_price) * order_qty else 0 end) as items_amount_jpy, 0 as cancelled
        from raw_qoo10_orders where substr(source_type, 1, 4) = 'api_' and substr(order_date, 1, 10) >= ? and substr(order_date, 1, 10) <= ? group by 1`,
  },
  /**
   * Yahoo!ショッピング (D5b-5)。元 = raw_yahoo_orders (1 行 = 注文 × 明細。2025-01-01 から = floor と同じ)。個人情報の列はこの表に無い。
   * 範囲の判定に使う order_time と注文番号は、整形と同じ関数 (isYahooJst / isYahooOrderNo。原値のまま) で検証する。
   * 後ろの明細の order_time が先頭と違う注文 (整形は「行によって違う」で例外) も必ず整形に渡す (#1363 の約束)。raw が floor からなので referencedByShipments は持たない
   */
  yahoo: {
    label: 'Yahoo の注文', scope: 'main', transformVersion: YAHOO_TRANSFORM_VERSION,
    // 🚨 売上日次 (mart.sales_daily) には公開しない (中原さん 2026-09-26): モール負担の値引 (TotalMallCouponDiscount) を取込が取っていない = null を
    //   mart が 0 として「払った額」を出すと、約 1 割の注文で払った額が実際より多くなる (#1465 Codex R1 P2)。取込で取れるようになるまで止める
    salesDaily: false,
    iterate: function* (warehouse) {
      let cur = null;
      for (const row of warehouse.prepare(`select ${YAHOO_COLUMNS.join(', ')} from raw_yahoo_orders order by order_id, line_id`).iterate()) {
        if (cur && cur.rows[0].order_id === row.order_id) {
          if ((row.order_time ?? null) !== (cur.rows[0].order_time ?? null)) cur.invalidDate = true;
          cur.rows.push(row); continue;
        }
        if (cur) yield cur;
        const okNo = isYahooOrderNo(row.order_id), okDt = isYahooJst(row.order_time);
        const no = okNo ? row.order_id : String(row.order_id ?? '');
        cur = { key: `yahoo|main|${no}`, no, rows: [row], order_date: okDt ? row.order_time : '', invalidDate: !okNo || !okDt };
      }
      if (cur) yield cur;
    },
    dateOf: (group) => group.order_date.slice(0, 10),
    build: (group, ctx) => buildYahooOrder(group.rows, { fallbackSourceUpdatedAt: ctx.startedAt.toISOString() }),
    /** 突合の材料: 注文日 (JST) ごとの 注文数 / 明細数 / 商品代 (Σ unit_price × quantity。UnitPrice は店のクーポン値引き後) / 取消の注文数 (order_status = '4') */
    dailySql: `with o as (
        select order_id, substr(min(order_time), 1, 10) as d, count(*) as n_lines, sum(round(coalesce(unit_price, 0)) * coalesce(quantity, 0)) as amt,
               max(case when trim(coalesce(order_status, '')) = '4' then 1 else 0 end) as c
          from raw_yahoo_orders group by order_id)
      select d as order_date, count(*) as orders, sum(n_lines) as lines, sum(amt) as items_amount_jpy, sum(c) as cancelled
        from o where d >= ? and d <= ? group by d`,
  },
};

/** 素の MALL_SPECS[x] は 'toString' のような継承プロパティも拾う → 自前の鍵だけ (Codex D5b-2 R2 #2) */
export const specOf = (mall) => (typeof mall === 'string' && Object.hasOwn(MALL_SPECS, mall) ? MALL_SPECS[mall] : null);

/** 突合の純粋な比較 */
export function diffDailyOrders(localRows, remoteRows) {
  const L = new Map(localRows.map((r) => [r.order_date, r])), R = new Map(remoteRows.map((r) => [r.order_date, r]));
  const mismatched = [], onlyLocal = [], onlyRemote = [];
  let matched = 0;
  for (const [k, l] of L) {
    const rr = R.get(k);
    if (!rr) { onlyLocal.push(l); continue; }
    const diffs = [];
    for (const f of ['orders', 'lines', 'items_amount_jpy', 'cancelled']) if (Number(l[f]) !== Number(rr[f])) diffs.push(`${f} ${l[f]}≠${rr[f]}`);
    if (diffs.length) mismatched.push({ key: k, diffs }); else matched++;
  }
  for (const [k, rr] of R) if (!L.has(k)) onlyRemote.push(rr);
  return { compared: L.size + onlyRemote.length, matched, mismatched, onlyLocal, onlyRemote };
}

export async function reconcileOrdersDaily({ mall, warehouse, fetchImpl = fetch, base, syncKey, from, to, log = console.log }) {
  const spec = specOf(mall); if (!spec) throw new Error(`知らないモール: ${mall}`);
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const total = { ok: true, windows: [], matched: 0, mismatched: [], onlyLocal: [], onlyRemote: [], localOrders: 0, remoteOrders: 0 };
  for (const [a, b] of splitWindows(from, to)) {
    const local = warehouse.prepare(spec.dailySql).all(a, b);
    const res = await fetchImpl(`${base}/orders/daily?mall=${mall}&scope=${spec.scope}&from=${a}&to=${b}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`Render の日次が取れない (${a}〜${b}): HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const remote = (await res.json()).rows;
    if (!Array.isArray(remote)) throw new Error('Render の応答に rows が無い');
    const d = diffDailyOrders(local, remote);
    const lo = local.reduce((s, x) => s + Number(x.orders), 0), ro = remote.reduce((s, x) => s + Number(x.orders), 0);
    const ok = d.mismatched.length === 0 && d.onlyLocal.length === 0 && d.onlyRemote.length === 0;
    log(`[company-db reconcile ${spec.label}] ${a}〜${b}: 日 miniPC ${local.length} / Render ${remote.length}、注文 miniPC ${lo} / Render ${ro}、一致 ${d.matched} / 不一致 ${d.mismatched.length} / miniPC だけ ${d.onlyLocal.length} / Render だけ ${d.onlyRemote.length} ${ok ? '✅' : '❌'}`);
    for (const m of d.mismatched.slice(0, 30)) log(`  不一致 ${m.key}: ${m.diffs.join(', ')}`);
    for (const x of d.onlyLocal.slice(0, 10)) log(`  miniPC だけ ${x.order_date}: 注文 ${x.orders}`);
    for (const x of d.onlyRemote.slice(0, 10)) log(`  Render だけ ${x.order_date}: 注文 ${x.orders}`);
    total.windows.push({ from: a, to: b, ok, ...d, localOrders: lo, remoteOrders: ro });
    total.ok = total.ok && ok; total.matched += d.matched; total.mismatched.push(...d.mismatched); total.onlyLocal.push(...d.onlyLocal); total.onlyRemote.push(...d.onlyRemote);
    total.localOrders += lo; total.remoteOrders += ro;
  }
  log(`${total.ok ? '✅' : '❌'} 突合 ${spec.label} ${from}〜${to}: ${total.ok ? '全部一致' : `差 ${total.mismatched.length + total.onlyLocal.length + total.onlyRemote.length} 日`} (注文 miniPC ${total.localOrders} / Render ${total.remoteOrders}、${total.windows.length} 窓)`);
  return total;
}

// 1 回の結び直しで見る伝票の数。9/16 の本番実測: 2,000 件 = 0.3 秒 / 回 (0016 の式結合は 20,000 件で planner が反転し 60 秒超 → 0017 で等結合に)。
//   受け口の上限は 100,000。env CDB_RELINK_LIMIT / CLI --relink-limit で変えられる。時間予算 10 分・回数上限 200 と組で (5,000 × 200 = 100 万件 > 伝票 51 万)
export const DEFAULT_RELINK_LIMIT = 5000;

/**
 * 伝票 → 注文の結び直し (Render の core.relink_shipments_bulk を shipment_id の順に回す)。
 * 戻り値 = { linked, examined, calls, complete, next }。maxCalls で打ち切ったら complete = false と続きの位置 next (Codex D5b-1 R1 #6)
 */
export async function relinkShipments({ fetchImpl = fetch, base, syncKey, limit = DEFAULT_RELINK_LIMIT, maxCalls = 200, after = 0, budgetMs = Infinity, now = () => Date.now(), onProgress = () => {}, log = console.log, beforeCall = () => {} }) {
  const started = now();
  let linked = 0, examined = 0, calls = 0, complete = false, reason = null;
  for (;;) {
    if (calls >= maxCalls) { reason = 'max_calls'; break; }
    if (now() - started >= budgetMs) { reason = 'budget'; break; }   // 時間予算 (daily-sync の 30 分より手前で区切り、続きは次の run。Codex D5b-1 R3 #1)
    beforeCall();   // lock の中で回すときは HTTP のたびに持ち主を確かめ心拍を打つ (奪われていたら LockLostError)
    const res = await fetchImpl(`${base}/shipments/relink`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': syncKey }, body: JSON.stringify({ after, limit }), signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`結び直しが失敗: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const j = await res.json(); calls++;
    linked += Number(j.linked || 0); examined += Number(j.examined || 0);
    if (!j.examined || j.last_id == null) { complete = true; break; }
    after = Number(j.last_id);
    onProgress(after);   // 成功のたびに続きの位置を残す (途中で落ちても・殺されても、済んだ所からやり直す。Codex R3 #1)
  }
  log(`[company-db relink] 伝票 ${examined} 件を見て ${linked} 件を注文に結んだ (${calls} 回${complete ? '' : `。${reason === 'budget' ? '時間切れ' : '回数の上限'}で打ち切り = 続きは shipment_id > ${after}`})`);
  return { linked, examined, calls, complete, next: complete ? 0 : after, reason };
}

export const DEFAULT_RELINK_BUDGET_MS = 10 * 60 * 1000;   // 結び直しの時間予算 (daily-sync のステップは 30 分。push 自体の後に回すので 10 分で区切る。env CDB_RELINK_BUDGET_MS)

export const RELINK_PENDING_KEY = 'relink_pending';   // '1' = 結び直しが要る (注文を送った・前回が失敗か打ち切り)。完了で '0' (Codex D5b-1 R1 #5)
export const RELINK_NEXT_KEY = 'relink_next';         // 走査中の続きの位置 (shipment_id)。走り終えたら 0。注文を送っても触らない (Codex R4 #1)
export const RELINK_RESCAN_KEY = 'relink_rescan';     // '1' = 走査の途中で (または前回の走査の後に) 注文が入った → 今の走査を走り終えたら先頭からもう一度。先頭からの走査を始めるときに '0'
/** 注文を送る run が最初の chunk の直前 (世代を取る取引) に書く印 = HTTP より先に「結び直しが要る・先頭も見直す」を永続化 (応答を失って run が落ちても消えない。Codex R2 #1 / #2) */
export const RELINK_META_ON_SEND = { [RELINK_PENDING_KEY]: '1', [RELINK_RESCAN_KEY]: '1' };

/**
 * push の後の結び直し (**送り手の lock の中** = runPush の afterSend から呼ぶ。Codex R2 #3: 別の run が印を消せない)。
 *   台帳の relink_pending が '1' なら回す (印は注文を送る前に付く)。走査は **続きの位置 (relink_next) から** 走り終え、その間に注文が入っていれば (relink_rescan) 先頭からもう一度
 *   = 予算で打ち切った走査が、毎朝の変更注文で先頭へ戻り続けない (Codex R4 #1)。
 *   失敗 → 印は残る (run は ❌ = retry の対象) / 打ち切り (回数・時間予算) → 続きの位置を残す / 完了 → 印を消す (持ち主の確認と同じ取引)
 * 戻り値 = { ran, pending, result: { linked, examined, calls, passes, complete, next, reason }, error }
 */
export async function relinkAfterPush({ ledger, owner = null, fetchImpl = fetch, base, syncKey, limit = DEFAULT_RELINK_LIMIT, maxCalls = 200, budgetMs = DEFAULT_RELINK_BUDGET_MS, log = console.log, now = () => new Date(), mustOwn = () => {} }) {
  if (ledger.getMeta(RELINK_PENDING_KEY) !== '1') return { ran: false, pending: false, result: null, error: null };
  const started = now().getTime();
  const total = { linked: 0, examined: 0, calls: 0, passes: 0, complete: false, next: 0, reason: null };
  const saveNext = (next) => ledger.setMeta({ [RELINK_NEXT_KEY]: String(next) }, { owner, at: now() });   // 持ち主の確認と同じ取引
  try {
    let after = Number(ledger.getMeta(RELINK_NEXT_KEY)) || 0;
    for (;;) {
      if (after === 0) ledger.setMeta({ [RELINK_RESCAN_KEY]: '0' }, { owner, at: now() });   // 先頭からの走査 = ここまでに入った注文を全部見る (この後に入る注文はまた '1' が付く)
      const r = await relinkShipments({ fetchImpl, base, syncKey, limit, maxCalls: maxCalls - total.calls, after, budgetMs: budgetMs - (now().getTime() - started), now: () => now().getTime(), onProgress: saveNext, log, beforeCall: mustOwn });
      total.linked += r.linked; total.examined += r.examined; total.calls += r.calls; total.passes++;
      if (!r.complete) {
        saveNext(r.next); total.next = r.next; total.reason = r.reason;
        log(`[company-db relink] ${r.reason === 'budget' ? `時間予算 ${Math.round(budgetMs / 1000)} 秒` : `${maxCalls} 回`}で打ち切り。次の run で shipment_id > ${r.next} から続ける`);
        return { ran: true, pending: true, result: total, error: null };
      }
      if (ledger.getMeta(RELINK_RESCAN_KEY) === '1') { after = 0; saveNext(0); log('[company-db relink] 走査の途中で注文が入っていたので先頭からもう一度'); continue; }
      ledger.setMeta({ [RELINK_PENDING_KEY]: '0', [RELINK_NEXT_KEY]: '0' }, { owner, at: now() });
      total.complete = true;
      return { ran: true, pending: false, result: total, error: null };
    }
  } catch (e) {
    if (e && e.code === 'LOCK_LOST') throw e;
    log(`[company-db relink] 失敗: ${e.message} → 次の run でやり直す (台帳の印はそのまま)`);
    return { ran: true, pending: true, result: null, error: e.message };
  }
}

/** 1 モールを送る (pipeline.runPush の種類ごとの設定) */
export async function pushOrders({ mall, warehouse, ledger, base, syncKey, floor = DEFAULT_FLOOR, from = null, to = null, relink = true, relinkLimit = Number(process.env.CDB_RELINK_LIMIT) || DEFAULT_RELINK_LIMIT, relinkMaxCalls = 200, relinkBudgetMs = Number(process.env.CDB_RELINK_BUDGET_MS) || DEFAULT_RELINK_BUDGET_MS, ...rest }) {
  const spec = specOf(mall); if (!spec) throw new Error(`知らないモール: ${mall}`);
  const mode = from && to ? 'range' : 'incremental';
  const logf = rest.log || console.log;
  const stats = { sentinel: 0, negative: 0, zeroPrice: 0, zeroQtyLive: 0, partialAmountOrders: 0, skippedNonAmazon: 0, skippedLegacy: 0, referenced: null, referencedCount: null };
  // 範囲 (incremental) = 注文日が floor 以降 / 追跡中 / **floor 以降に出荷確定した伝票が参照する注文** (D-28 = 出荷から辿れる古い注文も入れる。Codex D5b-1 R3 #2)。--from/--to は注文日の期間だけ
  // invalidDate = 範囲の判定に使う値が読めない注文 (spec の iterate が付ける。注文日時のほか、Qoo10 は鍵の形・同じ注文番号の重複も)。範囲の判定ができないので必ず build に渡して例外にする (range でも incremental でも ❌ になる)
  const inRange = (g, fps) => g.invalidDate === true || (mode === 'range' ? (g.order_date >= from && g.order_date < `${to}T99`) : (g.order_date >= floor || fps.has(g.key) || (stats.referenced != null && stats.referenced.has(g.no))));
  const iterate = function* (wh, st) {
    if (mode !== 'range' && spec.referencedByShipments) {
      st.referenced = spec.referencedByShipments(wh, floor);   // raw の読み取り取引の中 (同じ snapshot)
      st.referencedCount = st.referenced ? st.referenced.size : null;
      if (st.referenced == null) logf(`[company-db push ${spec.label}] ⚠️ raw_ne_order_base が無いので「範囲内の出荷が参照する古い注文」を範囲に入れられない`);
    }
    yield* spec.iterate(wh, st);
  };
  return runPush({
    kind: `order:${mall}`, label: spec.label, warehouse, ledger, base, syncKey, mode, stats,
    scopeLabel: mode === 'range' ? `注文日 ${from}〜${to}` : `注文日 ${floor} 以降 + 追跡中 + ${floor} 以降の出荷が参照する注文`,
    paths: { post: '/orders', status: `/orders/status?mall=${mall}&scope=${spec.scope}`, receipt: '/orders/receipt', keys: `/orders/keys?mall=${mall}&scope=${spec.scope}` },
    countOf: (j) => ({ count: j && j.counts ? j.counts.orders : undefined, maxBatchSeq: j && j.counts && j.counts.max_batch_seq != null ? Number(j.counts.max_batch_seq) : null }),
    keysOf: (j) => (j && Array.isArray(j.keys) ? j.keys.map((no) => `${mall}|${spec.scope}|${no}`) : null),
    iterate, inScope: inRange, build: (g, ctx) => spec.build(g, ctx, stats), transformVersion: spec.transformVersion,
    // 伝票との結び直し: 「要る」の印は最初の chunk の直前 (世代を取る取引) に書き、送り終えた後に lock の中で回す (Codex R2 #1〜#3)。続きの位置は HTTP 成功のたびに・時間予算で区切る (R3 #1)
    metaOnFirstChunk: relink ? RELINK_META_ON_SEND : null,
    afterSend: relink ? (ctx) => relinkAfterPush({ ledger, owner: ctx.owner, fetchImpl: ctx.fetchImpl, base, syncKey, limit: relinkLimit, maxCalls: relinkMaxCalls, budgetMs: relinkBudgetMs, log: ctx.log, now: ctx.now, mustOwn: ctx.mustOwn }) : null,
    ...rest,
  });
}

/**
 * 売上の日次 mart.sales_daily (0021。08 §4.5 / §9 D7a) を作り直す。どの日を作り直すかは Render の DB が自分で見つける (注文の updated_at)。
 * 送り手は「変わった日付」を渡さない = この呼び出しが落ちても・注文の push が途中で落ちても、次の回が全部拾う (Render 側の watermark は全部終わった回でしか進まない)。
 * remaining > 0 の間、呼び直す。回 (session) の続きは Render の DB が覚えている = 時間予算か回数の上限で打ち切っても (complete = false = 失敗扱い)、次の run は続きからやる (先頭に戻らない。Codex D7a R1 #2)。
 * 0021 がまだ適用されていなければ { skipped: 'not_migrated' } (注文の push 自体は失敗にしない。最後の行に出すので黙った緑にはならない)
 */
export const DEFAULT_SALES_LIMIT = 31;                     // 1 回の呼び出しで作る日数。実測 (2026-09-20 本番): 楽天 31 日 = 1.2〜1.5 万注文で平均 1.1 秒・最大 1.5 秒 / au PAY 最大 0.4 秒。Amazon は約 7.5 万注文 = 単純比例で 7 秒前後の見込み (Render の 60 秒の枠に十分)
export const DEFAULT_SALES_BUDGET_MS = 10 * 60 * 1000;     // env CDB_SALES_BUDGET_MS
export async function refreshSalesDaily({ mall, fetchImpl = fetch, base, syncKey, limit = DEFAULT_SALES_LIMIT, reset = false, maxCalls = 100, budgetMs = DEFAULT_SALES_BUDGET_MS, now = () => Date.now(), log = console.log }) {
  const spec = specOf(mall); if (!spec) throw new Error(`知らないモール: ${mall}`);
  if (spec.salesDaily === false) throw new Error(`${mall} の売上日次は止めている (salesDaily = false。モール負担を取込が取っていない = 払った額が出せない)`);
  if (!base) throw new Error('Render の宛先が無い (RENDER_MIRROR_URL)');
  const started = now();
  let calls = 0, dates = 0, rows = 0, remaining = null, purged = null, reason = null, catchUp = false, staleAtStart = false;
  while (true) {
    if (calls >= maxCalls) { reason = 'calls'; break; }
    if (calls > 0 && now() - started >= budgetMs) { reason = 'budget'; break; }
    const res = await fetchImpl(`${base}/orders/sales-daily/refresh`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': syncKey },
      body: JSON.stringify({ mall, scope: spec.scope, limit, reset: reset && calls === 0 }), signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });   // reset は最初の 1 回だけ (渡すたびに最初からになる)
    if (res.status === 409) {
      const j = await res.json().catch(() => ({}));
      if (j && j.error === 'not_migrated') return { ok: true, skipped: 'not_migrated', complete: false, calls, dates, rows, remaining: null, purged: null };
      throw new Error(`売上日次の作り直しが失敗: HTTP 409 ${JSON.stringify(j).slice(0, 200)}`);
    }
    if (!res.ok) throw new Error(`売上日次の作り直しが失敗: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const j = await res.json(); calls++;
    if (!Number.isInteger(j.remaining) || !Number.isInteger(j.dates_built)) throw new Error('売上日次の作り直しの応答の形が違う');
    dates += j.dates_built; rows += Number(j.n_rows || 0); remaining = j.remaining; if (j.purged != null) purged = j.purged;
    // resumed = 「その呼び出しより前から開いていた回の続き」。この run が自分で開いた回でも、2 回目以降の呼び出しは resumed = true で返る
    // → 「前の run が途中で止めた回」かどうかは **この run の最初の呼び出し** でだけ分かる
    if (calls === 1) staleAtStart = j.resumed === true;
    if (remaining === 0) {
      // 前の run が途中で止めた回の続きを終えた → その回の開始より後に動いた注文は次の回でないと拾えない。もう 1 回ぶんだけ回して追いつく (Codex D7a R2)。
      // 🚨 この run が開いた回 (32 日以上で呼び出しが複数になっただけ) では追いつかない: この run の push は回を開く前に終わっている。別の run が並行して動かした注文は、次の回が拾う (watermark = 回の開始時刻 − 15 分。Codex #1373 R1)。
      //    追いつくと、直前に入れた注文は全部 watermark − 15 分の内側なので **同じ日を丸ごともう 1 周作る** (2026-09-20 au PAY のバックフィル = 631 日を 2 周して「1262 日ぶん」と出た。結果は正しいが無駄と、紛らわしい日数)
      if (staleAtStart && !catchUp) { catchUp = true; remaining = null; continue; }
      break;
    }
    if (j.dates_built === 0) throw new Error(`売上日次の作り直しが進まない (残り ${remaining} 日なのに 0 日しか作られなかった)`);
  }
  const complete = remaining === 0;
  log(`[company-db sales-daily ${mall}] ${dates} 日ぶんを作り直した (${rows} 行・${calls} 回${complete ? '' : `。${reason === 'budget' ? '時間切れ' : '回数の上限'}で打ち切り = 残り ${remaining} 日は次の run`}${purged ? `・古い行 ${purged} 行を整理` : ''})`);
  return { ok: complete, skipped: null, complete, calls, dates, rows, remaining, purged, reason };
}
/** 1 行の要約 (朝の通知に出る最後の行へ足す) */
export const salesNote = (s) => (!s ? '' : s.error ? ` / ❌ 売上日次の作り直しに失敗 (${String(s.error).slice(0, 120)}。次の run が拾う)` : s.skipped === 'not_migrated' ? ' / ⏭️ 売上日次は未適用 (migration 0021 を当てる)'
  : s.complete ? ` / 売上日次 ${s.dates} 日` : ` / ⚠️ 売上日次 ${s.dates} 日 (打ち切り・残り ${s.remaining} 日は次の run)`);

/**
 * 単独 --relink を打ち切ったときの再開コマンド。数字だけで組む (パスは載せない = シェルごとの引用の違い (末尾の \ や PowerShell の $) で壊れる余地を無くす。
 * 単独 --relink は DATA_DIR を要らなくしたので --data-dir は不要。Codex #1347 R2 #1 / R3 #1)
 */
export function resumeCommand({ next, limit }) {
  return `node apps/company-db/push/mall-orders.mjs --relink --relink-after ${next} --relink-limit ${limit}`;
}

export function parseArgs(argv) {
  const out = { mall: null, incremental: false, dryRun: false, force: false, reconcile: false, relink: false, noRelink: false, all: false, resetLedger: false, requireBackfilled: false, markBackfilled: false, refreshSales: false, checkSales: false, noSales: false, from: null, to: null, days: null, dataDir: null, chunk: null, relinkLimit: null, relinkAfter: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    // 値を取るオプションに値が無い (末尾・次が別のオプション) のは入力の誤り = 既定値に黙って戻さない (--relink-after だけ書くと先頭に戻る。Codex #1347 R2 #2)
    const val = () => { const v = argv[++i]; if (v === undefined || String(v).startsWith('--')) throw new Error(`${a} に値が無い`); return v; };
    if (a === '--mall') out.mall = val();
    else if (a === '--relink-limit') out.relinkLimit = val();   // 1 回の結び直しで見る伝票の数 (既定 DEFAULT_RELINK_LIMIT / env CDB_RELINK_LIMIT)
    else if (a === '--relink-after') out.relinkAfter = val();   // 単独 --relink の続きの位置 (shipment_id。打ち切ったときに表示される)
    else if (a === '--incremental') out.incremental = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--force') out.force = true;
    else if (a === '--reconcile') out.reconcile = true;
    else if (a === '--relink') out.relink = true;
    else if (a === '--no-relink') out.noRelink = true;   // 送るだけ (バックフィルの窓。最後に --relink を 1 回)
    else if (a === '--all') out.all = true;
    else if (a === '--reset-ledger') out.resetLedger = true;
    else if (a === '--require-backfilled') out.requireBackfilled = true;   // daily-sync 用: 台帳にバックフィルの完了印が無ければ送らずに exit 0 (最後の行に「バックフィル前」)
    else if (a === '--refresh-sales') out.refreshSales = true;             // 売上日次 (mart.sales_daily) の作り直しだけ (--mall 必須。--all で全部の日)。DATA_DIR 不要
    else if (a === '--check-sales') out.checkSales = true;                 // 公開中の売上日次と材料の食い違いを見る (--mall と --from/--to か --days。差があれば exit 1)。DATA_DIR 不要
    else if (a === '--no-sales') out.noSales = true;                       // push の後の売上日次の作り直しを飛ばす (バックフィルの窓。最後に --refresh-sales を 1 回)
    else if (a === '--mark-backfilled') out.markBackfilled = true;         // 初回のバックフィルが全期間そろった (--reconcile --all が一致) のを見てから人が付ける完了印
    else if (a === '--from') out.from = val();
    else if (a === '--to') out.to = val();
    else if (a === '--days') out.days = val();
    else if (a === '--data-dir') out.dataDir = val();
    else if (a === '--chunk') out.chunk = val();
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.incremental && (out.from || out.to)) throw new Error('--incremental と --from/--to は一緒に指定しない (範囲を流すなら --from/--to だけ。証跡の mode を取り違えない)');
  return out;
}

async function main() {
  const a = parseArgs(process.argv.slice(2));
  const base = syncBase(), syncKey = process.env.MIRROR_SYNC_KEY || '';
  const relinkLimit = a.relinkLimit != null ? Number(a.relinkLimit) : (Number(process.env.CDB_RELINK_LIMIT) || DEFAULT_RELINK_LIMIT);
  if (!Number.isInteger(relinkLimit) || relinkLimit < 1 || relinkLimit > 100000) throw new Error(`--relink-limit が不正: ${a.relinkLimit ?? process.env.CDB_RELINK_LIMIT} (1〜100000)`);
  if (a.relink && !a.mall) {
    // 単独の結び直し: 続きの位置は --relink-after で渡す (打ち切ったら再開のコマンドを表示。台帳には書かない = 台帳は push の run のもの。Codex #1347 R1 #1)。時間予算も同じ
    const after = a.relinkAfter != null ? Number(a.relinkAfter) : 0;
    if (!Number.isSafeInteger(after) || after < 0) throw new Error(`--relink-after が不正: ${a.relinkAfter} (0 以上の整数。安全整数の範囲)`);   // 2^53 超は丸まって別の位置になる (Codex #1347 R2 #3)
    const budgetMs = Number(process.env.CDB_RELINK_BUDGET_MS) || DEFAULT_RELINK_BUDGET_MS;
    const t0 = Date.now();
    const r = await relinkShipments({ base, syncKey, limit: relinkLimit, after, budgetMs });
    console.log(`  (${relinkLimit} 件ずつ・shipment_id > ${after} から・${Math.round((Date.now() - t0) / 1000)} 秒)`);
    if (r.complete) console.log(`✅ 結び直し: ${r.linked} 件 (見た伝票 ${r.examined}、${r.calls} 回)`);
    else console.log(`⚠️ 結び直し: ${r.linked} 件 (見た伝票 ${r.examined}、${r.calls} 回) ${r.reason === 'budget' ? '時間予算' : '回数の上限'}で打ち切り。続きは:\n  ${resumeCommand({ next: r.next, limit: relinkLimit })}`);
    process.exitCode = r.complete ? 0 : 1;
    return;
  }
  if (a.refreshSales || a.checkSales) {
    if (a.refreshSales && a.checkSales) throw new Error('--refresh-sales と --check-sales は別々に流す (片方だけが実行される形にしない)');
    if (a.incremental || a.relink || a.reconcile || a.resetLedger || a.markBackfilled || a.dryRun) throw new Error('--refresh-sales / --check-sales はほかの操作と一緒に指定しない');
    // 売上日次だけ (Render を叩くだけ = DATA_DIR 不要)
    if (!a.mall || !specOf(a.mall)) throw new Error(`--mall を指定する (${Object.keys(MALL_SPECS).join(' / ')})`);
    if (a.refreshSales && specOf(a.mall).salesDaily === false) throw new Error(`${a.mall} の売上日次は止めている (モール負担の値引を取込が取っていない = 払った額が出せない。README「Yahoo の注文」)`);
    if (a.refreshSales) {
      const s = await refreshSalesDaily({ mall: a.mall, base, syncKey, reset: a.all, budgetMs: Number(process.env.CDB_SALES_BUDGET_MS) || DEFAULT_SALES_BUDGET_MS });
      console.log(s.skipped === 'not_migrated' ? '⏭️ 売上日次は未適用 (migration 0021 を当てる)' : s.complete ? `✅ 売上日次 (${a.mall}): ${s.dates} 日ぶんを作り直した (${s.rows} 行)` : `⚠️ 売上日次 (${a.mall}): ${s.dates} 日で打ち切り・残り ${s.remaining} 日 (もう一度 --refresh-sales を流す)`);
      process.exitCode = s.skipped || s.complete ? 0 : 1;
      return;
    }
    let from = a.from, to = a.to;
    if ((from && !to) || (!from && to)) throw new Error('--from と --to は組で');
    if (!from) { const days = a.days != null ? Number(a.days) : 90; if (!Number.isInteger(days) || days < 1 || days > 400) throw new Error('--days は 1〜400'); from = jstDate(-(days - 1)); to = jstDate(0); }
    if (!isDate(from) || !isDate(to) || from > to) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
    const res = await fetch(`${base}/orders/sales-daily/check?mall=${a.mall}&scope=${specOf(a.mall).scope}&from=${from}&to=${to}`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!res.ok) throw new Error(`売上日次の検算が取れない: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
    const j = await res.json();
    console.log(`[company-db sales-daily ${a.mall}] ${from}〜${to} 公開中: ${j.published.dates} 日 / 明細 ${j.published.lines} / 商品代 ${j.published.items_amount_jpy} 円 (うち取消 ${j.published.cancelled_items_amount_jpy}) / 売上 ${j.published.sales_jpy} 円 / 金額の分からない明細 ${j.published.lines_amount_unknown} / 未解決の明細 ${j.published.lines_unresolved}`);
    for (const d of j.diffs.slice(0, 20)) console.log(`  食い違い ${d.date_jst}${d.is_published ? '' : ' (未公開)'}: 明細 ${d.src_lines} / ${d.pub_lines}・商品代 ${d.src_items_amount_jpy} / ${d.pub_items_amount_jpy}・取消 ${d.src_cancelled_amount_jpy} / ${d.pub_cancelled_amount_jpy}・売上 ${d.src_sales_jpy} / ${d.pub_sales_jpy} (材料 / 公開中)`);
    console.log(j.diffs.length ? `❌ 売上日次 (${a.mall}): ${j.diffs.length} 日が材料と食い違う (注文が動いた後で作り直していない日を含む → --refresh-sales)` : `✅ 売上日次 (${a.mall}): 材料と一致`);
    process.exitCode = j.diffs.length ? 1 : 0;
    return;
  }
  // ここから先は warehouse.db と台帳を開く (単独 --relink は Render を叩くだけなので DATA_DIR 不要 = 再開コマンドにパスを載せなくてよい。Codex #1347 R3 #1)
  const dataDir = (process.env.DATA_DIR || a.dataDir || '').trim();
  if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
  if (!a.mall || !specOf(a.mall)) throw new Error(`--mall を指定する (${Object.keys(MALL_SPECS).join(' / ')})`);
  const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${a.chunk ?? process.env.CDB_PUSH_CHUNK} (1〜${MAX_CHUNK})`);
  if ((a.from && !a.to) || (!a.from && a.to)) throw new Error('--from と --to は組で');
  if (a.from && (!isDate(a.from) || !isDate(a.to) || a.from > a.to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });   // 読むだけ
  const ledger = openLedger(dataDir, { kind: `order:${a.mall}` });
  try {
    if (a.markBackfilled) {
      // 早すぎる印は検出できない (人が突合を見てから付ける約束) が、1 件も送っていない台帳に付けるのは明らかな誤り
      if (ledger.countConfirmed() === 0 && !a.force) throw new Error(`台帳 (${a.mall}) に送付確認済みが 1 件も無い = バックフィルをまだ流していない。流して --reconcile --all が一致してから付ける (それでも付けるなら --force)`);
      ledger.putMeta(BACKFILL_DONE_KEY, '1'); console.log(`台帳 (${a.mall}) にバックフィルの完了印を付けた (送付確認済み ${ledger.countConfirmed()} 件)。以後 daily-sync の --require-backfilled が送る`);
      return;
    }
    if (a.resetLedger) { const n = ledger.resetFingerprints(); console.log(`台帳 (${a.mall}) の指紋を空にした: ${n} 件 (鍵は残す)。次の --incremental で全部送り直す ('same' が返るだけ)`); return; }
    if (a.reconcile) {
      let from = a.from, to = a.to;
      if (a.all) { from = DEFAULT_FLOOR; to = jstDate(0); }
      else if (!from) { const days = a.days != null ? Number(a.days) : 90; if (!Number.isInteger(days) || days < 1 || days > 730) throw new Error('--days は 1〜730'); from = jstDate(-(days - 1)); to = jstDate(0); }
      const rr = await reconcileOrdersDaily({ mall: a.mall, warehouse, base, syncKey, from, to });
      process.exitCode = rr.ok ? 0 : 1;
      return;
    }
    if (!a.incremental && !a.from) throw new Error('--incremental か --from/--to を指定する (daily-sync は --incremental)');
    // 初回のバックフィル (Amazon は 128 万注文 = 数時間) の前に daily-sync の 30 分の枠で全件を送り始めない。バックフィルは人が --from/--to で流す (README)。
    // 🚨 黙って緑にしない: 最後の行 (= 朝の通知に出る要約) に「バックフィル前」と書く
    if (a.requireBackfilled && a.incremental && !a.dryRun && !isBackfillDone(ledger)) {
      console.log(`⏭️ Company DB ${MALL_SPECS[a.mall].label} push: 初回のバックフィル前 (台帳に完了印が無い。送付確認済み ${ledger.countConfirmed()} 件) なので送らない → db/company/README.md の手順で --from/--to のバックフィルを最後まで流し、--reconcile --all が一致したら --mark-backfilled`);
      writeEvidence(dataDir, `orders-${a.mall}`, { kind: 'orders', mall: a.mall, scope: MALL_SPECS[a.mall].scope, mode: 'incremental', ok: null, skipped: 'not_backfilled', confirmed: ledger.countConfirmed() });
      return;
    }
    const pushStartedAt = new Date();
    const r = await pushOrders({ mall: a.mall, warehouse, ledger, base, syncKey, chunkSize, dryRun: a.dryRun, force: a.force, from: a.from, to: a.to, relink: !a.noRelink, relinkLimit });
    if (r.stats && (r.stats.sentinel || r.stats.negative)) console.log(`  金額を null にした: 番兵 (-9999) ${r.stats.sentinel} 個 / 負 ${r.stats.negative} 個`);
    if (a.mall === 'qoo10' && r.stats) console.log(`  Qoo10: 旧データの行 (legacy_migration。鍵がカート番号) で送らなかった ${r.stats.skippedLegacy} 行 / 単価 0 を「分からない」にした注文 ${r.stats.zeroPrice}`);
    if (a.mall === 'amazon' && r.stats && (r.stats.zeroPrice || r.stats.zeroQtyLive || r.stats.skippedNonAmazon)) console.log(`  Amazon: 金額 0 を null にした明細 ${r.stats.zeroPrice} 行 (範囲の中) / うち取消でない明細の金額が分からず合計を null にした注文 ${r.stats.partialAmountOrders} / 取消でないのに数量 0 の明細 ${r.stats.zeroQtyLive} 行 / Amazon.co.jp 以外 (マルチチャネル発送) で送らなかった注文 ${r.stats.skippedNonAmazon}`);
    const rl = r.afterSend || { ran: false, pending: false, result: null, error: null };   // 結び直しは runPush の中 (lock の中) で済んでいる
    const relinkNote = !rl.ran ? '' : rl.error ? ` / ❌ 伝票の結び直しに失敗 (${rl.error.slice(0, 120)}。次の run でやり直す)` : ` / 伝票の結び直し ${rl.result.linked} 件${rl.pending ? ' (打ち切り。次の run で続きから)' : ''}`;
    // 売上日次の作り直し: 注文を送れた・送る物が無かった どちらでも回す (前の回の取りこぼしを拾う)。別の送り手が走っている・dry-run・--no-sales のときは回さない
    let sales = null;
    if (!r.dryRun && !r.lockedBy && !a.noSales && MALL_SPECS[a.mall].salesDaily !== false) {
      try { sales = await refreshSalesDaily({ mall: a.mall, base, syncKey, budgetMs: Number(process.env.CDB_SALES_BUDGET_MS) || DEFAULT_SALES_BUDGET_MS }); }
      catch (e) { sales = { ok: false, error: e.message }; }
    }
    console.log(summarizePush(r, MALL_SPECS[a.mall].label) + relinkNote + salesNote(sales));
    const success = r.lockedBy ? false : (r.dryRun ? r.transformErrors.length === 0 : (r.ok && (!sales || sales.ok)));
    // 朝の見張り (apps/company-db/watch) に渡す証跡。🚨 変更ゼロの朝は Render に run が作られない = 「走査は完了した・変わった注文は 0」を後から確かめられるのはこれだけ (設計 09 §2.1)
    if (a.incremental && !a.dryRun) writeEvidence(dataDir, `orders-${a.mall}`, evidenceOf(a.mall, r, { startedAt: pushStartedAt, success, relink: rl, sales }));
    process.exitCode = success ? 0 : 1;
  } catch (e) {
    if (a.incremental && !a.dryRun) writeEvidence(dataDir, `orders-${a.mall}`, { kind: 'orders', mall: a.mall, scope: MALL_SPECS[a.mall].scope, mode: 'incremental', ok: false, error: String(e && e.message).slice(0, 300) });
    throw e;
  } finally { ledger.close(); warehouse.close(); }
}

/** 証跡の形 (見張りの W7 / W9 が読む)。件数は数だけ (注文の中身は入れない) */
export function evidenceOf(mall, r, { startedAt, success, relink = null, sales = null } = {}) {
  return {
    kind: 'orders', mall, scope: MALL_SPECS[mall].scope, mode: r.mode, ok: !!success, push_ok: !!r.ok, locked: !!r.lockedBy,
    run_id: r.runId ?? null, batch_seq: r.batchSeq ?? null, started_at: startedAt ? startedAt.toISOString() : null,
    scanned: r.scanned, in_scope: r.inScope, unchanged: r.unchanged, changed: r.changed, sent: r.sent, applied: r.applied, same: r.same, stale: r.stale,
    failed: Array.isArray(r.failed) ? r.failed.length : 0, transform_errors: Array.isArray(r.transformErrors) ? r.transformErrors.length : 0,
    ledger_reset: r.ledgerReset || null, ledger_rebuilt: r.ledgerRebuilt || null,
    relink: relink && relink.ran ? { ok: !relink.error, linked: relink.result ? relink.result.linked : null, pending: !!relink.pending } : null,
    sales: sales ? { ok: !!sales.ok, complete: !!sales.complete, dates: sales.dates ?? null, skipped: sales.skipped || null, error: sales.error ? String(sales.error).slice(0, 200) : null } : null,
  };
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
// 落ちたときの最後の行は 1 行にする (Render のデプロイ中の 502 は本文が HTML = そのまま出すと何十行にもなる。2026-09-21 に本番のバックフィルで出た)。
// 🚨 fetch の直後に process.exit() しない: Windows の Node は libuv の assertion で異常終了して終了コードが 127 になる (#1386)。exitCode を置いて自然に終わらせ、保険に 10 秒後 (unref = ループを延ばさない)
if (isMain) main().catch((e) => {
  console.error(`❌ Company DB 注文 push: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
  process.exitCode = 1;
  setTimeout(() => process.exit(1), 10000).unref();
});
