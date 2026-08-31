/**
 * warehouse-mirror API
 *
 * ミニPCからデータを受信し、mirrorテーブルに格納。
 * ツール用のデータ参照APIも提供。
 *
 * エンドポイント:
 *   POST /api/sync          — ミニPCからデータ受信（APIキー認証）
 *   GET  /api/products      — mirror_products 検索
 *   GET  /api/sales/monthly — mirror_sales_monthly 検索
 *   GET  /api/sales/daily   — mirror_sales_daily 検索
 *   GET  /api/status        — 同期状態
 */
import { Router } from 'express';
import crypto from 'crypto';
import { initMirrorDB, getMirrorDB, yahooInitError, aupayDataInitError, qoo10DataInitError, rakutenReviewInitError, logizardStockInitError, skuMapInitError } from './db.js';
import { bootStart, bootEnd, bootFail } from '../observability/boot-log.js';
import {
  STORE_BENCH_COLS, STORE_DEVICE_BASE_COLS, STORE_DEVICE_OPT_COLS, CATEGORY_DEMO_COLS,
} from '../../lib/rakuten-dd-columns.js';

// 楽天データダウンロード7種の列合成 (mall-csv-fetcher P1-R3。miniPC側と共有定義)
const DD_STORE_ALL_COLS = [...STORE_DEVICE_BASE_COLS, ...STORE_BENCH_COLS, ...STORE_DEVICE_OPT_COLS];
const DD_CATEGORY_DEMO_COLS = CATEGORY_DEMO_COLS;

const router = Router();

// DB初期化
let dbReady = false;
// 初期化失敗の理由。⚠️ここが落ちると mirror 全体が 503 になり、全モールの sync と
// 分析アプリが停止する (2026-07-12 に実際に発生)。503 応答に原因を載せて自己診断できるようにする
let initError = null;
bootStart('mirror-db', 'warehouse-mirror.db');
(async () => {
  // ⭐2026-07-12 障害の真因: 初期化はboot時に1回きりで、デプロイ直後の一過性失敗
  // (旧インスタンスとのpersistent disk同居でのlock等) でも dbReady=false のまま
  // インスタンスの寿命いっぱい 503 を返し続けた (再デプロイまで復旧しない)。
  // → リトライで一過性失敗を自己回復させる。恒久失敗なら最終エラーを保持して503+原因
  const delaysMs = [0, 3000, 10000, 30000, 60000, 120000];
  for (let attempt = 0; attempt < delaysMs.length; attempt++) {
    if (delaysMs[attempt] > 0) await new Promise((r) => setTimeout(r, delaysMs[attempt]));
    try {
      initMirrorDB();
      dbReady = true;
      initError = null;
      bootEnd('mirror-db', `warehouse-mirror.db (attempt ${attempt + 1})`);
      return;
    } catch (e) {
      initError = {
        message: String(e.message || e),
        code: e.code || null,
        attempt: attempt + 1,
        at: String(e.stack || '').split('\n').slice(0, 3).join(' | '),
      };
      console.error(`[Mirror] DB初期化失敗 (attempt ${attempt + 1}/${delaysMs.length}):`, e.message);
    }
  }
  bootFail('mirror-db', 'warehouse-mirror.db', new Error(initError?.message || 'init failed'));
})();

function ensureDB(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'mirror DB 未初期化', init_error: initError });
  next();
}

// 同期APIキー認証 (二重防御: server.js 側の requireSyncKeyStrict が一次防御)
function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    // dev で明示フラグがあれば skip、そうでなければ拒否
    if (process.env.ALLOW_INSECURE_MIRROR_SYNC === '1') return next();
    return res.status(503).json({ error: 'mirror_sync_key_unset' });
  }
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

router.use(ensureDB);

// ─── POST /api/sync ───
// ミニPCからデータを受信して一括反映

router.post('/api/sync', requireSyncKey, (req, res) => {
  const db = getMirrorDB();
  const { products, set_components, sales_monthly, sales_daily, meta } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const log = [];

  // logizard_stock は単独POST限定 (毎時の全置換 payload)。他表と相乗りすると、この分岐より
  // 前の表だけ反映された状態で 400/503 になり「HTTP失敗なのに一部反映」の混在が残るため、
  // 入口で拒否する (Codex R2 Low-1)
  if (req.body.logizard_stock !== undefined) {
    const others = Object.keys(req.body).filter((k) => k !== 'logizard_stock' && k !== 'meta');
    if (others.length > 0) {
      return res.status(400).json({ error: `logizard_stock は単独で送信してください (同時指定: ${others.join(', ')})` });
    }
  }

  try {
    // products（全件置換）
    //   Codex PR1 review High #2 反映: seasonality_flag/season_months/new_product_flag/
    //   new_product_launch_date を列リストに含めて、ミニPC側の手動設定値を保持する。
    if (products && products.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_products');
        const stmt = db.prepare(`INSERT INTO mirror_products (
          product_id, 商品コード, 商品名, 商品区分, 取扱区分,
          標準売価, 原価, 原価ソース, 原価状態,
          送料, 送料コード, 配送方法, 消費税率, 税区分,
          在庫数, 引当数, 仕入先コード, セット構成品数, 売上分類, 代表商品コード,
          seasonality_flag, season_months, new_product_flag, new_product_launch_date,
          updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const p of products) {
          stmt.run(p.product_id, p.商品コード, p.商品名, p.商品区分, p.取扱区分,
            p.標準売価, p.原価, p.原価ソース, p.原価状態,
            p.送料, p.送料コード, p.配送方法, p.消費税率, p.税区分,
            p.在庫数, p.引当数, p.仕入先コード, p.セット構成品数, p.売上分類 ?? null, p.代表商品コード ?? null,
            p.seasonality_flag ?? 0, p.season_months ?? null,
            p.new_product_flag ?? 0, p.new_product_launch_date ?? null,
            now);
        }
      });
      tx();
      log.push(`products: ${products.length}件`);
    }

    // set_components（全件置換）
    if (set_components && set_components.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_set_components');
        const stmt = db.prepare(`INSERT INTO mirror_set_components (
          セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at
        ) VALUES (?,?,?,?,?,?)`);
        for (const c of set_components) {
          stmt.run(c.セット商品コード, c.構成商品コード, c.数量, c.構成商品名, c.構成商品原価, now);
        }
      });
      tx();
      log.push(`set_components: ${set_components.length}件`);
    }

    // sku_resolved + sku_master（SKUマスタ由来のペア。1 transaction で atomic に適用）
    //
    // fail-closed + ペア整合 設計 (2026-06-07 FBA SKUマスタ直結 PR1、Codex round1〜4 反映):
    //   ・両者は同一 m_sku_master スナップショット由来 (sku_resolved=seller_sku×ne_code 派生、
    //     sku_master=1 SKU 1 行)。FBA 在庫補充の mirror 直読み / recent-missing-candidates が
    //     「新しい master + 古い components」(またはその逆) を消費すると、誤った/欠落 NE コードを
    //     掴む穴になるため、受信側でも必ずペアで整合させる。
    //   ・各キー: N>0 → 全件置換 (DELETE→INSERT) / 0 件 + clear flag → 明示 clear (DELETE のみ) /
    //     0 件 + flag 無し → 異常。**片側でも異常ならペア全体を未反映** (前回 mirror を保持)。
    //   ・反映する場合は mirror_sku_resolved と mirror_sku_master の DELETE/INSERT を
    //     **1 つの db.transaction** にまとめ、片方 commit 後にもう片方が失敗して split snapshot が
    //     残る事故 (round4 High) を防ぐ。
    //   ・clear flag (clear_sku_resolved / clear_sku_master) は daily sync では絶対に付与しない手動オペ専用。
    //   過去事故 (2026-05-08〜10 Yahoo proxy regression で mirror 全空 / 2026-05-06 worktree 別 DB 分裂) の再発防止。
    const hasResolved = req.body.sku_resolved !== undefined && Array.isArray(req.body.sku_resolved);
    const hasMaster = req.body.sku_master !== undefined && Array.isArray(req.body.sku_master);
    if (hasResolved || hasMaster) {
      const resolved = hasResolved ? req.body.sku_resolved : null;
      const masterRows = hasMaster ? req.body.sku_master : null;
      const resolvedClear = req.body.meta?.clear_sku_resolved === true;
      const masterClear = req.body.meta?.clear_sku_master === true;
      // 各キーの「意図」を分類し、ペアとして整合する組合せだけ反映する (Codex round5 High)。
      //   'apply'   : N>0 件 → 全件置換
      //   'clear'   : 0 件 + clear flag → 明示クリア (手動オペ専用)
      //   'abnormal': 0 件 + flag 無し → 上流異常
      //   'absent'  : キー自体が payload に無い
      // 整合する組合せは (apply, apply) と (clear, clear) のみ。それ以外 (片側のみ / apply×clear /
      //   abnormal を含む) は split snapshot を作るため両テーブルとも未反映で前回 mirror を保持する。
      const intentOf = (has, rows, clear) =>
        !has ? 'absent' : (rows.length > 0 ? 'apply' : (clear ? 'clear' : 'abnormal'));
      const resolvedIntent = intentOf(hasResolved, resolved, resolvedClear);
      const masterIntent = intentOf(hasMaster, masterRows, masterClear);
      const pairConsistent =
        (resolvedIntent === 'apply' && masterIntent === 'apply') ||
        (resolvedIntent === 'clear' && masterIntent === 'clear');
      if (!pairConsistent) {
        log.push(`sku_resolved/sku_master: ペア不整合のため反映拒否 (resolved=${resolvedIntent} master=${masterIntent}、前回 mirror を保持)`);
        console.warn(`[Mirror] sku_resolved/sku_master ペア不整合 (resolved=${resolvedIntent}, master=${masterIntent}) → 両テーブルとも前回値を保持 (split snapshot 防止)`);
      } else {
        const tx = db.transaction(() => {
          if (hasResolved) {
            db.exec('DELETE FROM mirror_sku_resolved');
            if (resolved.length > 0) {
              const stmt = db.prepare(`INSERT INTO mirror_sku_resolved (
                seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, synced_at
              ) VALUES (?,?,?,?,?,?,?,?)`);
              // 旧送信側 (sort_order 未送信) との互換: payload は seller_sku 内で sort_order 昇順に
              // 並んでいる前提なので、sort_order が欠落している行は到着順の連番で補完する
              // (全行 0 に潰さない)。新送信側は sort_order を明示するのでそのまま使う。
              const seqBySku = new Map();
              for (const r of resolved) {
                let sortOrder = r.sort_order;
                if (sortOrder == null) {
                  const n = seqBySku.get(r.seller_sku) ?? 0;
                  sortOrder = n;
                  seqBySku.set(r.seller_sku, n + 1);
                }
                stmt.run(
                  r.seller_sku,
                  r.ne_code,
                  r.quantity ?? r.数量 ?? 1,
                  r.source,
                  r.商品名 ?? null,
                  r.source_updated_at ?? null,
                  sortOrder,
                  now
                );
              }
            }
          }
          if (hasMaster) {
            db.exec('DELETE FROM mirror_sku_master');
            if (masterRows.length > 0) {
              const stmt = db.prepare(`INSERT INTO mirror_sku_master (
                seller_sku, 商品名, source_created_at, source_updated_at, synced_at
              ) VALUES (?,?,?,?,?)`);
              for (const r of masterRows) {
                stmt.run(
                  r.seller_sku,
                  r.商品名 ?? null,
                  r.source_created_at ?? null,
                  r.source_updated_at ?? null,
                  now
                );
              }
            }
          }
        });
        tx();
        if (hasResolved) log.push(`sku_resolved: ${resolved.length}件${resolvedClear && resolved.length === 0 ? ' (explicit clear)' : ''}`);
        if (hasMaster) log.push(`sku_master: ${masterRows.length}件${masterClear && masterRows.length === 0 ? ' (explicit clear)' : ''}`);
      }
    }

    // inv_daily_detail (D-1c 詳細層、差分sync UPSERT + 古い行クリーン)
    // ペイロード: { inv_daily_detail: [...rows], meta: {
    //   inv_daily_detail_clear_old: true,                      // 365日より古い行 housekeeping
    //   inv_daily_detail_clear_dates: ['YYYY-MM-DD', ...],    // この sync で再投入する日付を先に DELETE
    //                                                          // (Codex R3 #2 対応: 同日再集計で消えた SKU の stale 行を残さない)
    // } }
    if (req.body.inv_daily_detail && Array.isArray(req.body.inv_daily_detail)) {
      const rows = req.body.inv_daily_detail;
      const clearOld = req.body.meta?.inv_daily_detail_clear_old === true;
      const clearDates = Array.isArray(req.body.meta?.inv_daily_detail_clear_dates) ? req.body.meta.inv_daily_detail_clear_dates : null;
      const tx = db.transaction(() => {
        if (clearOld) {
          // 365日より古い行を削除 (Render disk 圧迫抑制)
          db.prepare(`DELETE FROM mirror_inv_daily_detail WHERE business_date < date('now','-365 days')`).run();
        }
        if (clearDates && clearDates.length > 0) {
          // 同日 stale 防止: 送信元 (miniPC) が「今回 sync で再投入する日付」を明示
          //   → mirror 側でその日付を先に全削除 → 後続 chunk の UPSERT で新値だけ残る
          //   → 古い detail 行 (再集計で消えた SKU 等) を残さない
          // 初回 chunk のみ meta が乗ってる想定なので、後続 chunk でも複数回呼ばれることはない (はず)
          const placeholders = clearDates.map(() => '?').join(',');
          db.prepare(`DELETE FROM mirror_inv_daily_detail WHERE business_date IN (${placeholders})`).run(...clearDates);
        }
        const stmt = db.prepare(`
          INSERT INTO mirror_inv_daily_detail (
            business_date, market, category, source_system, source_item_code, ne_code,
            qty, unit_cost, total_value, cost_status, cost_source, resolution_method,
            is_bundle_expanded, component_qty,
            product_name, source_product_name, supplier_code, product_type, handling_class,
            sales_class, representative_product_code, order_lot_size,
            seasonality_flag, season_months, new_product_flag, new_product_launch_date,
            last_sold_date, sales_7d_qty, sales_30d_qty, sales_90d_qty,
            sales_7d_value, sales_30d_value, sales_90d_value,
            working_first_seen, fba_unfulfillable_qty,
            reserved_qty, pending_order_qty, location_code, last_purchase_date,
            snapshot_run_id, synced_at
          )
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(business_date, market, category, source_system, source_item_code, ne_code) DO UPDATE SET
            qty=excluded.qty, unit_cost=excluded.unit_cost, total_value=excluded.total_value,
            cost_status=excluded.cost_status, cost_source=excluded.cost_source, resolution_method=excluded.resolution_method,
            is_bundle_expanded=excluded.is_bundle_expanded, component_qty=excluded.component_qty,
            product_name=excluded.product_name, source_product_name=excluded.source_product_name,
            supplier_code=excluded.supplier_code, product_type=excluded.product_type, handling_class=excluded.handling_class,
            sales_class=excluded.sales_class, representative_product_code=excluded.representative_product_code, order_lot_size=excluded.order_lot_size,
            seasonality_flag=excluded.seasonality_flag, season_months=excluded.season_months,
            new_product_flag=excluded.new_product_flag, new_product_launch_date=excluded.new_product_launch_date,
            last_sold_date=excluded.last_sold_date, sales_7d_qty=excluded.sales_7d_qty,
            sales_30d_qty=excluded.sales_30d_qty, sales_90d_qty=excluded.sales_90d_qty,
            sales_7d_value=excluded.sales_7d_value, sales_30d_value=excluded.sales_30d_value, sales_90d_value=excluded.sales_90d_value,
            working_first_seen=excluded.working_first_seen, fba_unfulfillable_qty=excluded.fba_unfulfillable_qty,
            reserved_qty=excluded.reserved_qty, pending_order_qty=excluded.pending_order_qty,
            location_code=excluded.location_code, last_purchase_date=excluded.last_purchase_date,
            snapshot_run_id=excluded.snapshot_run_id, synced_at=excluded.synced_at
        `);
        for (const r of rows) {
          stmt.run(
            r.business_date, r.market || 'jp', r.category, r.source_system, r.source_item_code, r.ne_code,
            r.qty ?? 0, r.unit_cost ?? null, r.total_value ?? null, r.cost_status, r.cost_source ?? null, r.resolution_method ?? null,
            r.is_bundle_expanded ?? 0, r.component_qty ?? null,
            r.product_name ?? null, r.source_product_name ?? null, r.supplier_code ?? null, r.product_type ?? null, r.handling_class ?? null,
            r.sales_class ?? null, r.representative_product_code ?? null, r.order_lot_size ?? null,
            r.seasonality_flag ?? null, r.season_months ?? null, r.new_product_flag ?? null, r.new_product_launch_date ?? null,
            r.last_sold_date ?? null, r.sales_7d_qty ?? null, r.sales_30d_qty ?? null, r.sales_90d_qty ?? null,
            r.sales_7d_value ?? null, r.sales_30d_value ?? null, r.sales_90d_value ?? null,
            r.working_first_seen ?? null, r.fba_unfulfillable_qty ?? null,
            r.reserved_qty ?? null, r.pending_order_qty ?? null, r.location_code ?? null, r.last_purchase_date ?? null,
            r.snapshot_run_id ?? null, now
          );
        }
      });
      tx();
      log.push(`inv_daily_detail: ${rows.length}件 (${clearOld ? 'cutoff済み' : 'append'})`);
    }

    // inv_daily_summary（全件置換、PR-C 日次在庫スナップショット）
    if (req.body.inv_daily_summary && Array.isArray(req.body.inv_daily_summary)) {
      const rows = req.body.inv_daily_summary;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_inv_daily_summary');
        const stmt = db.prepare(`INSERT INTO mirror_inv_daily_summary (
          business_date, market, category, total_qty, total_value,
          resolved_count, unresolved_count, cost_missing_count,
          source_status, source_row_count, captured_at, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const r of rows) {
          stmt.run(
            r.business_date,
            r.market || 'jp',
            r.category,
            r.total_qty ?? 0,
            r.total_value ?? null,
            r.resolved_count ?? 0,
            r.unresolved_count ?? 0,
            r.cost_missing_count ?? 0,
            r.source_status,
            r.source_row_count ?? null,
            r.captured_at ?? null,
            now
          );
        }
      });
      tx();
      log.push(`inv_daily_summary: ${rows.length}件`);
    }

    // shipments_daily（全件置換、日次出荷サマリ）
    //   1年分でも数千行なので毎回まるごと差し替える。miniPC 側で --all 再構築しているため、
    //   出荷取消・出荷日訂正による「減り」も置換で正しく反映される。
    if (req.body.shipments_daily !== undefined) {
      // キーがあるのに配列でない = 送信側の不具合。黙って無視すると「同期できていない」ことに
      // 気付けないので 400 で落とす (Codex R4 medium)
      if (!Array.isArray(req.body.shipments_daily)) {
        return res.status(400).json({ error: 'shipments_daily は配列である必要があります' });
      }
      const rows = req.body.shipments_daily;
      // 全消し前に全行を検証する。SQLite は INTEGER 列にも文字列を入れられるので、
      // 送信側の不具合で "abc" や 負数、cancelled > slips が入ると画面の集計が壊れる。
      // 1行でもおかしければ payload ごと 400 で拒否し、mirror は前回状態のまま残す。
      const isDate = (s) => {
        if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
        const d = new Date(`${s}T00:00:00Z`);
        // Invalid Date に toISOString() すると throw するので先に判定する
        return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
      };
      const seen = new Set();
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const where = `shipments_daily[${i}]`;
        if (!r || typeof r !== 'object') return res.status(400).json({ error: `${where}: 行が不正です` });
        if (!isDate(r.ship_date)) return res.status(400).json({ error: `${where}: ship_date が不正です (${r.ship_date})` });
        const shop = String(r.shop_code ?? '');
        const dlv = String(r.delivery_id ?? '');
        if (shop.length > 32 || dlv.length > 32) return res.status(400).json({ error: `${where}: shop_code/delivery_id が長すぎます` });
        if (String(r.shop_name ?? '').length > 200 || String(r.delivery_name ?? '').length > 200) {
          return res.status(400).json({ error: `${where}: 名称が長すぎます` });
        }
        const slips = r.slips;
        const cancelled = r.cancelled_slips ?? 0;
        if (!Number.isInteger(slips) || slips < 0) return res.status(400).json({ error: `${where}: slips が非負整数ではありません (${slips})` });
        if (!Number.isInteger(cancelled) || cancelled < 0) return res.status(400).json({ error: `${where}: cancelled_slips が非負整数ではありません (${cancelled})` });
        if (cancelled > slips) return res.status(400).json({ error: `${where}: cancelled_slips > slips (${cancelled} > ${slips})` });
        const key = `${r.ship_date}\u001f${shop}\u001f${dlv}`;
        if (seen.has(key)) return res.status(400).json({ error: `${where}: キーが重複しています (${key.replace(/\u001f/g, '/')})` });
        seen.add(key);
      }
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_shipments_daily');
        const stmt = db.prepare(`INSERT INTO mirror_shipments_daily (
          ship_date, shop_code, shop_name, platform, delivery_id, delivery_name,
          slips, cancelled_slips, source_updated_at, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)`);
        for (const r of rows) {
          stmt.run(
            r.ship_date,
            String(r.shop_code ?? ''),
            r.shop_name ?? null,
            r.platform ?? null,
            String(r.delivery_id ?? ''),
            r.delivery_name ?? null,
            r.slips ?? 0,
            r.cancelled_slips ?? 0,
            r.updated_at ?? null,
            now
          );
        }
      });
      tx();
      log.push(`shipments_daily: ${rows.length}件`);
    }

    // logizard_stock（全件置換、ロジザード在庫スナップショット・毎時）
    //   送信側 = sync-to-render.js --logizard-only (毎時ランナー)。取込90分以内のときだけ送ってくる。
    //   ⚠ この payload は logizard_stock 単独の POST で送ること。他表と相乗りすると、
    //   この分岐の 400/503 でそれより前の表は反映済み・以降は未処理の混在状態になる
    if (req.body.logizard_stock !== undefined) {
      // fail-soft: この表のDDLが失敗していても他表を道連れにしない (2026-07-12 障害の教訓)。
      // 同一POSTに相乗りした他テーブルを巻き込まないよう、この表だけ 503 で拒否する
      if (logizardStockInitError) {
        return res.status(503).json({ error: 'logizard_stock 表の初期化に失敗しています', init_error: logizardStockInitError });
      }
      const p = req.body.logizard_stock;
      if (!p || typeof p !== 'object' || !Array.isArray(p.rows)) {
        return res.status(400).json({ error: 'logizard_stock は { captured_at, rows[] } である必要があります' });
      }
      const capturedMs = typeof p.captured_at === 'string' ? Date.parse(p.captured_at) : NaN;
      if (!Number.isFinite(capturedMs) || capturedMs > Date.now() + 24 * 3600 * 1000) {
        return res.status(400).json({ error: 'logizard_stock.captured_at が日時として不正です (ISO形式・未来すぎない値が必要)' });
      }
      const rows = p.rows;
      // 全置換なので空配列は受けない (取得失敗による全消しの防御。倉庫在庫ゼロは現実に起きない)
      if (rows.length === 0) {
        return res.status(400).json({ error: 'logizard_stock.rows が空です (全消しは受け付けません)' });
      }
      if (rows.length > 100000) {
        return res.status(400).json({ error: `logizard_stock.rows が多すぎます (${rows.length}件)` });
      }
      // 全消し前に全行を検証する。1行でもおかしければ payload ごと 400 で拒否し、mirror は前回状態のまま残す
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const where = `logizard_stock[${i}]`;
        if (!r || typeof r !== 'object') return res.status(400).json({ error: `${where}: 行が不正です` });
        if (typeof r['商品ID'] !== 'string' || !r['商品ID'].trim()) {
          return res.status(400).json({ error: `${where}: 商品ID が不正です` });
        }
        if (!Number.isInteger(r['在庫数']) || !Number.isInteger(r['引当数'])) {
          return res.status(400).json({ error: `${where}: 在庫数/引当数 が整数ではありません (${r['在庫数']}/${r['引当数']})` });
        }
      }
      // 世代逆行の防止 (Codex R2 Medium-1): 手動再実行・遅延で古い snapshot が後から届いても
      // 巻き戻さない。同一世代の再送は冪等成功 (リトライを 409 で失敗扱いにしない)
      const currentCaptured = db.prepare('SELECT MAX(captured_at) AS c FROM mirror_logizard_stock').get()?.c || null;
      const currentMs = currentCaptured ? Date.parse(currentCaptured) : NaN;
      if (Number.isFinite(currentMs) && capturedMs < currentMs) {
        return res.status(409).json({ error: `logizard_stock.captured_at が保存済みより古い snapshot です (${p.captured_at} < ${currentCaptured})` });
      }
      if (Number.isFinite(currentMs) && capturedMs === currentMs) {
        log.push(`logizard_stock: 同一世代 (${p.captured_at}) のため変更なし`);
      } else {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_logizard_stock');
        const stmt = db.prepare(`INSERT INTO mirror_logizard_stock (
          商品ID, 商品名, バーコード, ブロック略称, ロケ, 品質区分名, 有効期限, 入荷日,
          在庫数, 引当数, ロケ業務区分, 最終入荷日, 最終出荷日, 在庫日, captured_at, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const r of rows) {
          stmt.run(
            r['商品ID'], r['商品名'] ?? null, r['バーコード'] ?? null,
            r['ブロック略称'] ?? null, r['ロケ'] ?? null, r['品質区分名'] ?? null,
            r['有効期限'] ?? null, r['入荷日'] ?? null,
            r['在庫数'], r['引当数'],
            r['ロケ業務区分'] ?? null, r['最終入荷日'] ?? null, r['最終出荷日'] ?? null,
            r['在庫日'] ?? null, p.captured_at, now
          );
        }
      });
      tx();
      log.push(`logizard_stock: ${rows.length}件`);
      }
    }

    // rakuten_sku_map（全件置換）
    if (req.body.rakuten_sku_map && req.body.rakuten_sku_map.length > 0) {
      const rskmData = req.body.rakuten_sku_map;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_rakuten_sku_map');
        const stmt = db.prepare(`INSERT INTO mirror_rakuten_sku_map (
          rakuten_code, ne_code, source, updated_at
        ) VALUES (?,?,?,?)`);
        for (const m of rskmData) {
          stmt.run(m.rakuten_code, m.ne_code, m.source, now);
        }
      });
      tx();
      log.push(`rakuten_sku_map: ${rskmData.length}件`);
    }

    // amazon_sku_fees（全件置換）
    if (req.body.amazon_sku_fees && req.body.amazon_sku_fees.length > 0) {
      const feesData = req.body.amazon_sku_fees;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_amazon_sku_fees');
        const stmt = db.prepare(`INSERT INTO mirror_amazon_sku_fees (
          seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate,
          fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
        for (const f of feesData) {
          stmt.run(f.seller_sku, f.asin, f.fulfillment_channel,
            f.referral_fee, f.referral_fee_rate,
            f.fba_fee, f.variable_closing_fee, f.per_item_fee,
            f.total_fee, f.price_used, f.fetched_at);
        }
      });
      tx();
      log.push(`amazon_sku_fees: ${feesData.length}件`);
    }

    // sales_monthly（初回チャンクでDELETE、以降は追記）
    if (sales_monthly && sales_monthly.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_monthly) db.exec('DELETE FROM mirror_sales_monthly');
        const stmt = db.prepare(`INSERT INTO mirror_sales_monthly (
          月, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_monthly) {
          stmt.run(s.月, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_monthly: ${sales_monthly.length}件`);
    }

    // sales_daily（初回チャンクでDELETE、以降は追記）
    if (sales_daily && sales_daily.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_daily) db.exec('DELETE FROM mirror_sales_daily');
        const stmt = db.prepare(`INSERT INTO mirror_sales_daily (
          日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_daily) {
          stmt.run(s.日付, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_daily: ${sales_daily.length}件`);
    }

    // stock_monthly_snapshot（PR2a 追加、商品収益性ダッシュボード タブB GMROI用）
    //   初回チャンクで meta.clear_stock_snapshot=true → DELETE、以降は追記
    //   Codex PR2a review Medium #1 反映: 空配列でも clear_stock_snapshot だけは処理する
    //   （ミニPC側で対象月内の在庫が0件になった時に mirror 側の stale を消すため）
    if (req.body.stock_monthly_snapshot !== undefined) {
      const snapshotData = req.body.stock_monthly_snapshot;
      const tx = db.transaction(() => {
        if (meta?.clear_stock_snapshot) db.exec('DELETE FROM mirror_stock_monthly_snapshot');
        if (snapshotData.length > 0) {
          const stmt = db.prepare(`INSERT INTO mirror_stock_monthly_snapshot (
            年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at
          ) VALUES (?,?,?,?,?,?,?)`);
          for (const s of snapshotData) {
            stmt.run(s.年月, s.商品コード, s.月末在庫数 ?? 0, s.月末引当数 ?? 0,
              s.snapshot_source || null, s.captured_at || null, now);
          }
        }
      });
      tx();
      log.push(`stock_monthly_snapshot: ${snapshotData.length}件${meta?.clear_stock_snapshot ? ' (clear)' : ''}`);
    }

    // velocity_mall（速報モール別: 商品コード×mall×7/30日数量）
    //   payload: req.body.velocity_mall = { as_of_date, rows: [{商品コード, mall, qty_7d, qty_30d}] }
    //   注文ベース・毎朝再構築。fail-closed: rows 欠落/空は反映せず前回 mirror を保持
    //   （sync 失敗時に速報モール別が全消えするのを防ぐ）。
    if (req.body.velocity_mall && typeof req.body.velocity_mall === 'object' && Array.isArray(req.body.velocity_mall.rows)) {
      const vm = req.body.velocity_mall;
      const rows = vm.rows;
      if (rows.length === 0) {
        log.push('velocity_mall: 0件 → 送信スキップ扱い（前回 mirror を保持）');
      } else {
        const tx = db.transaction(() => {
          db.exec('DELETE FROM mirror_f_sales_velocity_by_product_mall');
          const stmt = db.prepare(`INSERT INTO mirror_f_sales_velocity_by_product_mall
            (商品コード, mall, qty_7d, qty_30d, as_of_date, synced_at) VALUES (?,?,?,?,?,?)`);
          for (const r of rows) {
            stmt.run(r.商品コード, r.mall, r.qty_7d ?? 0, r.qty_30d ?? 0, vm.as_of_date || '', now);
          }
        });
        tx();
        log.push(`velocity_mall: ${rows.length}件 (as_of=${vm.as_of_date || '?'})`);
      }
    }

    // 商品管理リスト スナップショット (⑤) — checksum 検証 → atomic swap
    //   payload: req.body.pml_snapshot = { run_id, status, as_of_date, generated_at,
    //     payload_checksum, row_count, src_*, ne_fba_overlap, rows: [...] }
    //   fail-closed: status='failed' / rows欠落 / checksum不一致 / 件数不一致 は反映せず前回 published を保持。
    if (req.body.pml_snapshot && typeof req.body.pml_snapshot === 'object') {
      const p = req.body.pml_snapshot;
      const PML_COLS = [
        '商品コード','商品名','仕入先','取扱区分','商品区分','売上分類','最終仕入日','在庫保管日数',
        '総在庫数','FBA在庫数','フリー在庫','注残数','引当数','総在庫数_引当なし',
        '販売数7日_FBA','販売数7日_FBA以外','販売数7日_合計',
        '販売数30日_FBA','販売数30日_FBA以外','販売数30日_合計',
        '発注ロット単位','推奨保有月数','売価','原価','想定見込み利益','概算利益率',
        '代表商品コード','ロケーションコード','商品分類タグ','登録日',
      ];
      const rows = Array.isArray(p.rows) ? p.rows : null;
      let reason = null;
      // fail-closed: checksum / row_count は必須。欠落・空・不一致は反映しない (前回 published 保持)。
      if (!p.run_id || !p.status) reason = 'run_id/status 欠落';
      else if (p.status === 'failed') reason = `status=failed (${p.run_id})`;
      else if (!rows) reason = 'rows 欠落';
      else if (!Number.isInteger(p.row_count) || p.row_count !== rows.length) reason = `件数不一致/欠落 meta=${p.row_count} actual=${rows.length}`;
      else if (typeof p.payload_checksum !== 'string' || p.payload_checksum.length === 0) reason = 'payload_checksum 欠落';
      else {
        // checksum 再計算 (送信元 build-product-management-snapshot.js と同一規約: 列順固定・null='' ・tab/改行)
        const canonical = rows.map(r => PML_COLS.map(c => r[c] == null ? '' : String(r[c])).join('\t')).join('\n');
        const recomputed = crypto.createHash('sha256').update(canonical).digest('hex');
        if (recomputed !== p.payload_checksum) {
          reason = `checksum不一致 (recompute≠送信元)`;
        } else if ((() => {
          // 巻き戻し防止: 既存 published より古い generated_at の run では上書きしない。
          // (daily の full sync が、後発の on-demand live run を古い daily run で潰す事故を防ぐ)
          const cur = db.prepare('SELECT run_id, generated_at FROM mirror_pml_published WHERE id=1').get();
          if (cur && cur.run_id !== p.run_id && cur.generated_at && p.generated_at && p.generated_at < cur.generated_at) {
            reason = `古いrun skip (incoming generated_at=${p.generated_at} < current ${cur.generated_at}, 巻き戻し防止)`;
            return true;
          }
          return false;
        })()) {
          // reason 設定済 (skip)
        } else {
          const swap = db.transaction(() => {
            db.exec('DELETE FROM mirror_pml_snapshot_rows');
            const ins = db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, ${PML_COLS.join(', ')})
              VALUES (?, ${PML_COLS.map(() => '?').join(', ')})`);
            for (const r of rows) ins.run(p.run_id, ...PML_COLS.map(c => r[c] ?? null));
            db.prepare(`INSERT INTO mirror_pml_published
              (id, run_id, status, as_of_date, generated_at, payload_checksum, row_count,
               src_ne_products_synced_at, src_velocity_as_of, src_fba_business_date, src_reorder_updated_at,
               ne_fba_overlap, published_at, synced_at,
               fba_source_kind, fba_source_run_id, fba_fetched_at, fba_latest_row_count)
              VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id, status=excluded.status,
               as_of_date=excluded.as_of_date, generated_at=excluded.generated_at,
               payload_checksum=excluded.payload_checksum, row_count=excluded.row_count,
               src_ne_products_synced_at=excluded.src_ne_products_synced_at, src_velocity_as_of=excluded.src_velocity_as_of,
               src_fba_business_date=excluded.src_fba_business_date, src_reorder_updated_at=excluded.src_reorder_updated_at,
               ne_fba_overlap=excluded.ne_fba_overlap, published_at=excluded.published_at, synced_at=excluded.synced_at,
               fba_source_kind=excluded.fba_source_kind, fba_source_run_id=excluded.fba_source_run_id,
               fba_fetched_at=excluded.fba_fetched_at, fba_latest_row_count=excluded.fba_latest_row_count`)
              .run(p.run_id, p.status, p.as_of_date || null, p.generated_at || null, p.payload_checksum || null, rows.length,
                p.src_ne_products_synced_at || null, p.src_velocity_as_of || null, p.src_fba_business_date || null,
                p.src_reorder_updated_at || null, p.ne_fba_overlap ?? null, p.generated_at || now, now,
                p.fba_source_kind || null, p.fba_source_run_id || null, p.fba_fetched_at || null, p.fba_latest_row_count ?? null);
          });
          swap();
          log.push(`pml_snapshot: ${rows.length}件 (run=${p.run_id}, status=${p.status})`);
        }
      }
      if (reason) log.push(`pml_snapshot: スキップ (${reason}) — 前回 published 保持`);
    }

    // 同期状態更新
    db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run('last_sync', now, now);
    if (meta) {
      for (const [k, v] of Object.entries(meta)) {
        db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run(k, String(v), now);
      }
    }

    // WALチェックポイント
    try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

    console.log('[Mirror] 同期完了:', log.join(', '));
    res.json({ ok: true, log, synced_at: now });
  } catch (e) {
    console.error('[Mirror] 同期エラー:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── Phase 1 #1-4a: POST /api/sync/:entity/chunk (entity-driven contract sync) ───
//
// 受信処理:
//   1. 認証 (x-sync-key)
//   2. payload validation (sync_run_id / contract_version / chunk_index 等)
//   3. checksum 検証 (改ざん検知)
//   4. is_first=true なら scope clear (entity 別 clear strategy)
//   5. payload を mirror テーブルに INSERT OR REPLACE
//   6. sync_run_chunks に ledger 記録 (received_at)
//   7. apply 成功で applied_at を更新
//   8. is_last=true なら status response
//
// 構造監査 H-4 (2026-07-19): date_range clear の multi-chunk run は 4-5 を staging 化。
//   chunk 毎の受信は sync_stage_rows へ溜めるだけ (live 表は無傷のまま)。全 chunk 到着した
//   chunk の tx 内で「scope DELETE → stage 全行 INSERT → applied マーカー」を原子的に実行する。
//   途中失敗・run 放棄でも live 表に「clear済+部分データ」が露出しない。
//   single-chunk run (元々 atomic) と no_clear (MF finalize 契約) は従来経路のまま。
//
// Phase 1 #1-4 で sync_contracts に登録済みの entity のみ受信
// (現状: amazon_finance_sku_daily v1)
//
// payload 構造 (POST body):
// {
//   sync_run_id: "amazon_finance_sku_daily-v1-2026-05-08T0731",
//   contract_version: 1,
//   scope_from: "2026-04-01",
//   scope_to: "2026-04-30",
//   chunk_index: 0,
//   chunk_count: 5,
//   is_first: true,
//   is_last: false,
//   row_count: 5000,
//   payload_checksum: "sha256...",
//   meta: { clear_amazon_finance_dates: ["2026-04-01", ...] },  // first chunk のみ
//   payload: { rows: [...] }
// }
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// Insert/ledger statement キャッシュ (DB 接続単位、Codex Round 13 #2)
// モジュール変数だと DB 切替時に古い connection の statement を再利用してしまうため WeakMap
const _stmtCache = new WeakMap();  // db => { stmts by entity }
function getStmtBundle(db) {
  let bundle = _stmtCache.get(db);
  if (!bundle) {
    bundle = {};
    _stmtCache.set(db, bundle);
  }
  return bundle;
}

function getAmazonFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonFinanceInsert) {
    b.amazonFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_finance_sku_daily (
        date_jst, seller_sku, asin_norm, product_name,
        units_ordered, units_refunded_customer, units_marketplace_guarantee,
        units_a_to_z_refund, units_net_sold,
        sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy,
        commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
        shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy,
        warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy,
        refund_principal_jpy, reversal_reimbursement_jpy,
        misc_fee_jpy, other_fee_jpy, other_amount_jpy,
        unit_cost_snapshot, cost_snapshot_date_jst, latest_unit_cost_reference,
        cogs_amount, profit_amount, is_cost_complete, cost_status,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @seller_sku, @asin_norm, @product_name,
        @units_ordered, @units_refunded_customer, @units_marketplace_guarantee,
        @units_a_to_z_refund, @units_net_sold,
        @sales_principal_jpy, @sales_shipping_jpy, @sales_giftwrap_jpy, @sales_tax_jpy,
        @commission_jpy, @fba_fulfillment_jpy, @fba_storage_jpy, @closing_fee_jpy,
        @shipping_chargeback_jpy, @giftwrap_chargeback_jpy, @promotion_jpy,
        @warehouse_damage_jpy, @warehouse_lost_jpy, @safe_t_jpy,
        @refund_principal_jpy, @reversal_reimbursement_jpy,
        @misc_fee_jpy, @other_fee_jpy, @other_amount_jpy,
        @unit_cost_snapshot, @cost_snapshot_date_jst, @latest_unit_cost_reference,
        @cogs_amount, @profit_amount, @is_cost_complete, @cost_status,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonFinanceInsert;
}

// Amazon 広告費 SKU 別 (amazon-dashboard PR-A)
function getAmazonAdsSkuInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonAdsSkuInsert) {
    b.amazonAdsSkuInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_ads_sku_daily (
        date_jst, mall, campaign_id, ad_type, target, target_granularity,
        clicks, impressions, ad_cost, ad_sales, ad_units,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @mall, @campaign_id, @ad_type, @target, @target_granularity,
        @clicks, @impressions, @ad_cost, @ad_sales, @ad_units,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonAdsSkuInsert;
}

// Amazon 広告費 キャンペーン単位 (amazon-dashboard PR-A)
function getAmazonAdsCampaignInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonAdsCampaignInsert) {
    b.amazonAdsCampaignInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_ads_campaign_daily (
        date_jst, mall, campaign_id, campaign_name, ad_type, campaign_status,
        clicks, impressions, ad_cost,
        ad_sales_1d, ad_sales_7d, ad_sales_14d, ad_sales_30d, ad_units_1d,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @mall, @campaign_id, @campaign_name, @ad_type, @campaign_status,
        @clicks, @impressions, @ad_cost,
        @ad_sales_1d, @ad_sales_7d, @ad_sales_14d, @ad_sales_30d, @ad_units_1d,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonAdsCampaignInsert;
}

// カート価格スナップショット (amazon-dashboard PR-D)
function getAmazonPriceSnapshotInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonPriceSnapshotInsert) {
    b.amazonPriceSnapshotInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_price_snapshot_daily (
        date_jst, seller_sku, asin, channel, my_price, buybox_price, buybox_is_mine,
        fetched_at, source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @seller_sku, @asin, @channel, @my_price, @buybox_price, @buybox_is_mine,
        @fetched_at, @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonPriceSnapshotInsert;
}

// 楽天RPP広告費 月次×商品 (mall-csv-fetcher P1)
function getRakutenAdsRppInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenAdsRppInsert) {
    b.rakutenAdsRppInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_ads_rpp (
        date_jst, month_ym, item_manage_number, raw_sku_code,
        clicks, ad_cost_yen, cpc_actual, ctr_pct, bid_cpc_yen, item_cpc_yen,
        sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
        sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
        source_report_type, report_start, report_end,
        attribution_window_hours, is_tax_included, imported_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @month_ym, @item_manage_number, @raw_sku_code,
        @clicks, @ad_cost_yen, @cpc_actual, @ctr_pct, @bid_cpc_yen, @item_cpc_yen,
        @sales_720h_yen, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
        @sales_12h_yen, @orders_12h, @sales_720h_new_yen, @sales_720h_repeat_yen,
        @source_report_type, @report_start, @report_end,
        @attribution_window_hours, @is_tax_included, @imported_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenAdsRppInsert;
}

// 楽天RPP広告費 日次×キャンペーン合計 (mall-csv-fetcher P1)
function getRakutenAdsRppDailyInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenAdsRppDailyInsert) {
    b.rakutenAdsRppDailyInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_ads_rpp_daily (
        date_jst, campaign_id, campaign_name,
        clicks, ad_cost_yen, ad_cost_discounted_yen, cpc_actual, ctr_pct,
        sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
        sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
        source_report_type, attribution_window_hours, is_tax_included, imported_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @campaign_id, @campaign_name,
        @clicks, @ad_cost_yen, @ad_cost_discounted_yen, @cpc_actual, @ctr_pct,
        @sales_720h_yen, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
        @sales_12h_yen, @orders_12h, @sales_720h_new_yen, @sales_720h_repeat_yen,
        @source_report_type, @attribution_window_hours, @is_tax_included, @imported_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenAdsRppDailyInsert;
}

// 楽天データ分析 SKU×日次 (mall-csv-fetcher P1-R2)
function getRakutenItemDailyInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenItemDailyInsert) {
    b.rakutenItemDailyInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_item_daily (
        date_jst, item_manage_number, raw_sku_code,
        sales_yen, orders, units, access_users, unique_users, cvr_pct, aov_yen,
        buyers_total, buyers_new, buyers_repeat, nonbuyer_access,
        review_posts, review_avg, review_total,
        stay_seconds, bounce_count, exit_count, exit_rate_pct,
        favorites_added, favorites_total, stock_qty,
        item_name, genre_path, item_id, catalog_id, item_number,
        is_tax_included, imported_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @item_manage_number, @raw_sku_code,
        @sales_yen, @orders, @units, @access_users, @unique_users, @cvr_pct, @aov_yen,
        @buyers_total, @buyers_new, @buyers_repeat, @nonbuyer_access,
        @review_posts, @review_avg, @review_total,
        @stay_seconds, @bounce_count, @exit_count, @exit_rate_pct,
        @favorites_added, @favorites_total, @stock_qty,
        @item_name, @genre_path, @item_id, @catalog_id, @item_number,
        @is_tax_included, @imported_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenItemDailyInsert;
}

// 楽天データ分析 店舗×日次 (mall-csv-fetcher P1-R2)
function getRakutenStoreDailyInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenStoreDailyInsert) {
    b.rakutenStoreDailyInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_store_daily (
        date_jst,
        sales_all_yen, sales_pc_yen, sales_app_yen, sales_sp_yen,
        orders_all, orders_pc, orders_app, orders_sp,
        access_all, access_pc, access_app, access_sp,
        cvr_all_pct, cvr_pc_pct, cvr_app_pct, cvr_sp_pct,
        aov_all_yen, aov_pc_yen, aov_app_yen, aov_sp_yen,
        bench_top10_sales_yen, bench_top10_orders, bench_top10_access, bench_top10_cvr_pct, bench_top10_aov_yen,
        bench_class_label, bench_class_sales_yen, bench_class_orders, bench_class_access, bench_class_cvr_pct, bench_class_aov_yen,
        tax_out_yen, shipping_yen, coupon_store_yen, coupon_rakuten_yen,
        free_ship_coupon_yen, wrapping_yen, settlement_fee_yen,
        is_tax_included, imported_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst,
        @sales_all_yen, @sales_pc_yen, @sales_app_yen, @sales_sp_yen,
        @orders_all, @orders_pc, @orders_app, @orders_sp,
        @access_all, @access_pc, @access_app, @access_sp,
        @cvr_all_pct, @cvr_pc_pct, @cvr_app_pct, @cvr_sp_pct,
        @aov_all_yen, @aov_pc_yen, @aov_app_yen, @aov_sp_yen,
        @bench_top10_sales_yen, @bench_top10_orders, @bench_top10_access, @bench_top10_cvr_pct, @bench_top10_aov_yen,
        @bench_class_label, @bench_class_sales_yen, @bench_class_orders, @bench_class_access, @bench_class_cvr_pct, @bench_class_aov_yen,
        @tax_out_yen, @shipping_yen, @coupon_store_yen, @coupon_rakuten_yen,
        @free_ship_coupon_yen, @wrapping_yen, @settlement_fee_yen,
        @is_tax_included, @imported_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenStoreDailyInsert;
}

// ─── 楽天データダウンロードハブ 7種 (mall-csv-fetcher P1-R3) ───
// 列定義は lib/rakuten-dd-columns.js を miniPC 側と共有。insert/normalize は宣言から生成
const RAKUTEN_DD_TABLE_SPECS = {
  rakuten_store_device_daily: {
    table: 'mirror_rakuten_store_device_daily',
    required: ['date_jst', 'device'],
    cols: ['date_jst', 'device',
      ...DD_STORE_ALL_COLS,
      'is_tax_included', 'imported_at'],
    defaults: { is_tax_included: 1 },
    validate: (r, HttpErrorCls) => {
      if (!['all', 'pc', 'app', 'sp'].includes(r.device)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `unknown device: ${r.device}` });
      }
    },
  },
  rakuten_sku_daily: {
    table: 'mirror_rakuten_sku_daily',
    required: ['date_jst', 'sku_key'],
    cols: ['date_jst', 'sku_key', 'raw_sku_mgmt_number', 'item_manage_number',
      'sales_yen', 'orders', 'units',
      'system_sku_number', 'item_number', 'catalog_id', 'item_name',
      'sku_attr1', 'sku_attr2', 'sku_attr3', 'is_tax_included', 'imported_at'],
    defaults: { sales_yen: 0, orders: 0, units: 0, is_tax_included: 1 },
  },
  rakuten_category_daily: {
    table: 'mirror_rakuten_category_daily',
    required: ['date_jst', 'category_key', 'device'],
    cols: ['date_jst', 'category_key', 'device', 'hierarchy', 'category_name', 'category_url',
      'access_users', 'unique_users', 'stay_seconds', 'bounce_count', 'exit_count', 'exit_rate_pct',
      ...DD_CATEGORY_DEMO_COLS, 'imported_at'],
    defaults: { access_users: 0 },
    validate: (r, HttpErrorCls) => {
      if (!['all', 'pc', 'app', 'sp'].includes(r.device)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `unknown device: ${r.device}` });
      }
    },
  },
  rakuten_campaigns: {
    table: 'mirror_rakuten_campaigns',
    required: ['campaign_type', 'campaign_name', 'start_at', 'date_jst'],
    cols: ['campaign_type', 'campaign_name', 'start_at', 'end_at', 'date_jst', 'imported_at'],
  },
  rakuten_purchaser_monthly: {
    table: 'mirror_rakuten_purchaser_monthly',
    required: ['date_jst'],
    cols: ['date_jst', 'new_buyers', 'new_aov_yen', 'new_sales_yen', 'new_orders', 'new_units',
      'repeat_buyers', 'repeat_aov_yen', 'repeat_sales_yen', 'repeat_orders', 'repeat_units',
      'is_tax_included', 'imported_at'],
    defaults: { is_tax_included: 1 },
    validate: (r, HttpErrorCls) => {
      if (!/^\d{4}-\d{2}-01$/.test(r.date_jst)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `date_jst must be month start (got ${r.date_jst})` });
      }
    },
  },
  rakuten_item_purchaser_snapshot: {
    table: 'mirror_rakuten_item_purchaser_snapshot',
    required: ['date_jst', 'item_manage_number'],
    cols: ['date_jst', 'item_manage_number', 'item_name', 'item_url', 'price_yen', 'is_suspended',
      'new_buyers', 'repeat_buyers', 'repeat_rate_pct', 'window_from', 'window_to', 'imported_at'],
  },
  rakuten_genre_purchaser_snapshot: {
    table: 'mirror_rakuten_genre_purchaser_snapshot',
    required: ['date_jst', 'genre_name'],
    cols: ['date_jst', 'genre_name', 'new_buyers', 'repeat_buyers', 'repeat_rate_pct',
      'new_avg_purchase_yen', 'repeat_avg_purchase_yen', 'avg_purchase_count', 'avg_purchase_yen',
      'window_from', 'window_to', 'imported_at'],
  },
};

// Yahoo!ストクリ統計 6種 (mall-csv-fetcher P1-Y)。楽天dd と同じ宣言生成機構を使う
const YAHOO_DATA_TABLE_SPECS = {
  yahoo_store_device_daily: {
    table: 'mirror_yahoo_store_device_daily',
    required: ['date_jst', 'device'],
    cols: ['date_jst', 'device', 'sales_yen', 'pageviews', 'is_tax_included', 'imported_at'],
    defaults: { is_tax_included: 1 },
    validate: (r, HttpErrorCls) => {
      if (!['all', 'pc', 'sp', 'app'].includes(r.device)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `unknown device: ${r.device}` });
      }
    },
  },
  yahoo_inflow_daily: {
    table: 'mirror_yahoo_inflow_daily',
    required: ['date_jst'],
    cols: ['date_jst', 'inflow_visitors', 'purchase_visitors', 'purchase_ratio_pct',
      'exit_visitors', 'exit_ratio_pct', 'imported_at'],
  },
  yahoo_user_attr_daily: {
    table: 'mirror_yahoo_user_attr_daily',
    required: ['date_jst', 'gender', 'age_band', 'buyer_class'],
    cols: ['date_jst', 'gender', 'age_band', 'buyer_class', 'visitors', 'imported_at'],
  },
  yahoo_flash_hourly: {
    table: 'mirror_yahoo_flash_hourly',
    required: ['date_jst', 'hour_slot', 'device'],
    cols: ['date_jst', 'hour_slot', 'device', 'sales_yen', 'orders', 'units', 'buyers',
      'purchase_rate_pct', 'aov_yen', 'pageviews', 'visitors', 'avg_pages', 'is_tax_included', 'imported_at'],
    defaults: { is_tax_included: 1 },
    validate: (r, HttpErrorCls) => {
      if (!['all', 'pc', 'sp', 'app'].includes(r.device)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `unknown device: ${r.device}` });
      }
    },
  },
  yahoo_item_daily: {
    table: 'mirror_yahoo_item_daily',
    required: ['date_jst', 'item_code'],
    cols: ['date_jst', 'item_code', 'sub_code', 'raw_item_code', 'item_name',
      'sales_yen', 'orders', 'units', 'buyers', 'avg_purchase_rate_pct', 'favorites', 'cart_adds',
      'pv_premium_ship', 'pv_normal', 'visitors', 'category_contribution', 'is_tax_included', 'imported_at'],
    defaults: { sub_code: '', is_tax_included: 1 },
  },
  yahoo_keyword_daily: {
    table: 'mirror_yahoo_keyword_daily',
    required: ['date_jst', 'keyword'],
    cols: ['date_jst', 'keyword', 'rank', 'inflow', 'sales_yen', 'orders', 'units',
      'avg_order_rate_pct', 'avg_order_aov_yen', 'avg_units_aov_yen', 'is_tax_included', 'imported_at'],
    defaults: { is_tax_included: 1 },
  },
};
// au PAYマーケット分析 13種 (mall-csv-fetcher P1-A)。楽天dd/Yahoo と同じ宣言生成機構を使う
const AUPAY_SALES_COLS = ['date_jst', 'segment_code', 'segment_raw', 'sales_yen', 'coupon_store_yen', 'coupon_mall_yen',
  'point_ponta_au_yen', 'point_used_yen', 'orders', 'units', 'visits', 'buyer_uu', 'visitor_uu', 'pageviews',
  'cvr_pct', 'avg_units', 'avg_price_yen', 'avg_visits', 'avg_pv',
  'coupon_store_rate_pct', 'coupon_mall_rate_pct', 'point_rate_pct', 'is_tax_included', 'imported_at'];
const AUPAY_REFERER_COLS = ['date_jst', 'segment_code', 'segment_raw', 'channel', 'visits', 'visitor_uu',
  'sales_yen', 'orders', 'units', 'cvr_pct', 'avg_units', 'avg_price_yen', 'is_tax_included', 'imported_at'];
const AUPAY_SEARCH_COLS = ['date_jst', 'segment_code', 'segment_raw', 'keyword', 'visits', 'visitor_uu', 'imported_at'];
const AUPAY_PAGE_COLS = ['date_jst', 'page_url', 'pageviews', 'orders', 'via_orders', 'bounce_rate_pct', 'exit_rate_pct', 'imported_at'];
const AUPAY_PRODUCT_COLS = ['date_jst', 'segment_code', 'segment_raw', 'lot_number', 'product_name', 'category',
  'sales_yen', 'orders', 'units', 'avg_units', 'avg_price_yen', 'coupon_store_yen', 'coupon_mall_yen',
  'buyer_uu', 'visits', 'visitor_uu', 'pageviews', 'cvr_visit_pct', 'cvr_uu_pct', 'is_tax_included', 'imported_at'];
const AUPAY_DATA_TABLE_SPECS = {};
for (const grain of ['daily', 'monthly']) {
  AUPAY_DATA_TABLE_SPECS[`aupay_sales_${grain}`] = {
    table: `mirror_aupay_sales_${grain}`, required: ['date_jst', 'segment_code'],
    cols: AUPAY_SALES_COLS, defaults: { is_tax_included: 1 },
  };
  AUPAY_DATA_TABLE_SPECS[`aupay_referer_${grain}`] = {
    table: `mirror_aupay_referer_${grain}`, required: ['date_jst', 'segment_code', 'channel'],
    cols: AUPAY_REFERER_COLS, defaults: { is_tax_included: 1 },
  };
  AUPAY_DATA_TABLE_SPECS[`aupay_search_${grain}`] = {
    table: `mirror_aupay_search_${grain}`, required: ['date_jst', 'segment_code', 'keyword'],
    cols: AUPAY_SEARCH_COLS,
  };
  AUPAY_DATA_TABLE_SPECS[`aupay_page_${grain}`] = {
    table: `mirror_aupay_page_${grain}`, required: ['date_jst', 'page_url'],
    cols: AUPAY_PAGE_COLS,
  };
  AUPAY_DATA_TABLE_SPECS[`aupay_product_${grain}`] = {
    table: `mirror_aupay_product_${grain}`, required: ['date_jst', 'segment_code', 'lot_number'],
    cols: AUPAY_PRODUCT_COLS, defaults: { is_tax_included: 1 },
  };
}
AUPAY_DATA_TABLE_SPECS.aupay_repeat_cohort_monthly = {
  table: 'mirror_aupay_repeat_cohort_monthly',
  required: ['date_jst', 'segment_code', 'target_month_jst'],
  cols: ['date_jst', 'segment_code', 'segment_raw', 'target_month_jst', 'orders', 'imported_at'],
  validate: (r, HttpErrorCls) => {
    if (!/^\d{4}-\d{2}-01$/.test(String(r.target_month_jst))) {
      throw new HttpErrorCls(400, { error: 'bad_row', message: `bad target_month_jst: ${r.target_month_jst}` });
    }
  },
};
AUPAY_DATA_TABLE_SPECS.aupay_pm_ad_daily = {
  table: 'mirror_aupay_pm_ad_daily', required: ['date_jst'],
  cols: ['date_jst', 'impressions', 'clicks', 'ctr_pct', 'cpc_yen', 'gmv_via_ad_yen', 'roas', 'cost_yen',
    'is_tax_included', 'imported_at'],
  defaults: { is_tax_included: 1 },
};
AUPAY_DATA_TABLE_SPECS.aupay_pm_query_weekly = {
  table: 'mirror_aupay_pm_query_weekly', required: ['date_jst', 'keyword'],
  cols: ['date_jst', 'keyword', 'rank', 'impressions', 'clicks', 'ctr_pct', 'imported_at'],
};
const AUPAY_DATA_ENTITY_NAMES = new Set(Object.keys(AUPAY_DATA_TABLE_SPECS));

// Qoo10 Analytics 4種 (mall-csv-fetcher P1-Q R1)
const QOO10_DATA_TABLE_SPECS = {
  qoo10_traffic_channel_daily: {
    table: 'mirror_qoo10_traffic_channel_daily',
    required: ['date_jst', 'channel'],
    cols: ['date_jst', 'channel', 'pv', 'imported_at'],
  },
  qoo10_cvr_daily: {
    table: 'mirror_qoo10_cvr_daily',
    required: ['date_jst'],
    cols: ['date_jst', 'pv_total', 'visitors', 'cart_adds', 'orders', 'cvr_pct', 'channel_pv_diff', 'imported_at'],
  },
  qoo10_item_traffic_daily: {
    table: 'mirror_qoo10_item_traffic_daily',
    required: ['date_jst', 'item_no', 'channel'],
    cols: ['date_jst', 'item_no', 'channel', 'pv', 'imported_at'],
  },
  qoo10_items: {
    table: 'mirror_qoo10_items',
    required: ['item_no'],
    cols: ['item_no', 'seller_code_raw', 'seller_code', 'item_name', 'brand', 'attr_date_jst', 'imported_at'],
  },
};
const QOO10_DATA_ENTITY_NAMES = new Set(Object.keys(QOO10_DATA_TABLE_SPECS));

// 楽天レビュー 日次集計 (mall-csv-fetcher P2 PR-A)。★非PII集計のみ受ける (本文/注文番号/URLは列自体が無い)
const RAKUTEN_REVIEW_TABLE_SPECS = {
  rakuten_review_daily: {
    table: 'mirror_rakuten_review_daily',
    required: ['date_jst', 'review_type', 'rating', 'item_id'],
    cols: ['date_jst', 'review_type', 'item_id', 'item_name', 'rating', 'review_count'],
    validate: (r, HttpErrorCls) => {
      if (r.review_type !== 'item' && r.review_type !== 'shop') {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `bad review_type: ${r.review_type}` });
      }
      // contract整合 (Codex R1 low): item=正整数の商品ID / shop=0 固定。欠落を0へ黙って補完しない
      const itemId = Number(r.item_id);
      if (!Number.isInteger(itemId) || (r.review_type === 'item' ? itemId <= 0 : itemId !== 0)) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `bad item_id for ${r.review_type}: ${r.item_id}` });
      }
      const rating = Number(r.rating);
      if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `bad rating: ${r.rating}` });
      }
      const cnt = Number(r.review_count);
      if (!Number.isInteger(cnt) || cnt < 1) {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `bad review_count: ${r.review_count}` });
      }
    },
  },
};
const RAKUTEN_REVIEW_ENTITY_NAMES = new Set(Object.keys(RAKUTEN_REVIEW_TABLE_SPECS));

// SKUマップ 2種 (価格一括改定ツール PR1、2026-08-28)。モール出品コード → NEコード の手動 map。
// full_snapshot = 毎 run 全置換 (miniPC で削除された map を mirror に残さない)
// 許可ストア。増えたらここに足す (未知の store_id を黙って受けると引き当て粒度が崩れる)
const SKU_MAP_ALLOWED_STORES = new Set(['b-faith01']);
const SKU_MAP_RESOLUTION_SOURCES = new Set(['manual', 'auto_pattern', 'fallback_parent']);

// 値の意味検証 (Codex R1 High #3)。前後空白・表記ゆれ・タイポをここで止める。
// ★ne_code が実在の商品かは Render 側では判定できない (NE商品マスタは miniPC のみ)。
//   参照整合性は送信元 sync-sku-maps.js が m_products と突合して担保し、
//   価格改定の実行時 (PR4/PR5) にも再検証する — 「mirror に入った = 正しい」とはしない
function makeSkuMapValidator(keyCol) {
  return (r, HttpErrorCls) => {
    for (const col of [keyCol, 'ne_code', 'store_id']) {
      const v = r[col];
      if (typeof v !== 'string' || v !== v.trim() || v === '') {
        throw new HttpErrorCls(400, { error: 'bad_row', message: `${col} は前後空白なしの非空文字列が必要: ${JSON.stringify(v)}` });
      }
    }
    if (!SKU_MAP_ALLOWED_STORES.has(r.store_id)) {
      throw new HttpErrorCls(400, { error: 'bad_row', message: `未知の store_id: ${r.store_id}` });
    }
    if (!SKU_MAP_RESOLUTION_SOURCES.has(r.resolution_source)) {
      throw new HttpErrorCls(400, { error: 'bad_row', message: `未知の resolution_source: ${r.resolution_source}` });
    }
  };
}

const SKU_MAP_TABLE_SPECS = {
  yahoo_sku_map: {
    table: 'mirror_yahoo_sku_map',
    // store_id も必須 (default で黙って埋めない。PK の一部なので取り違えると別ストアの行を上書きする)
    required: ['store_id', 'yahoo_key', 'ne_code', 'resolution_source'],
    cols: ['store_id', 'yahoo_key', 'ne_code', 'resolution_source', 'notes', 'created_at', 'updated_at'],
    pkCols: ['store_id', 'yahoo_key'],
    validate: makeSkuMapValidator('yahoo_key'),
  },
  aupay_sku_map: {
    table: 'mirror_aupay_sku_map',
    required: ['store_id', 'aupay_key', 'ne_code', 'resolution_source'],
    cols: ['store_id', 'aupay_key', 'ne_code', 'resolution_source', 'notes', 'created_at', 'updated_at'],
    pkCols: ['store_id', 'aupay_key'],
    validate: makeSkuMapValidator('aupay_key'),
  },
  // 送料マスタ (25行程度)。配送方法ごとの配送関係費合計を持つ。
  // 価格一括改定がモール別の粗利を出すのに使う (2026-08-31)
  shipping_rates: {
    table: 'mirror_shipping_rates',
    required: ['shipping_code', '小分類区分名称'],
    cols: ['shipping_code', '大分類区分', '運送会社', '小分類区分名称', '梱包サイズ', '最大重量',
      '追跡有無', '送料', '出荷作業料', '想定梱包資材費', '想定人件費', '配送関係費合計', '備考'],
    pkCols: ['shipping_code'],
    validate: (r, HttpErrorCls) => {
      // 金額は数値でなければ受けない (文字列が入ると粗利計算が黙って壊れる)
      for (const col of ['送料', '出荷作業料', '想定梱包資材費', '想定人件費', '配送関係費合計']) {
        const v = r[col];
        if (v === null || v === undefined || v === '') continue;
        if (!Number.isFinite(Number(v)) || Number(v) < 0) {
          throw new HttpErrorCls(400, { error: 'bad_row', message: `${col} は 0 以上の数値が必要: ${JSON.stringify(v)}` });
        }
      }
    },
  },
};
const SKU_MAP_ENTITY_NAMES = new Set(Object.keys(SKU_MAP_TABLE_SPECS));

// full_snapshot で「何割まで減ってよいか」。これを超える減少は明示承認 (meta.allow_shrink) が要る。
// 0件だけを弾いても「本来500件が3件」の欠損 snapshot は通ってしまう (Codex R1 High #2)
const FULL_SNAPSHOT_MAX_SHRINK_RATIO = 0.2;

// 楽天dd + Yahoo + au PAY分析 + Qoo10 + 楽天レビュー を1つのレジストリに統合 (getRakutenDdInsert/normalizeRakutenDdRow が参照)
const DD_ALL_TABLE_SPECS = { ...RAKUTEN_DD_TABLE_SPECS, ...YAHOO_DATA_TABLE_SPECS, ...AUPAY_DATA_TABLE_SPECS, ...QOO10_DATA_TABLE_SPECS, ...RAKUTEN_REVIEW_TABLE_SPECS, ...SKU_MAP_TABLE_SPECS };

function getRakutenDdInsert(db, entityKey) {
  const b = getStmtBundle(db);
  const cacheKey = `ddInsert_${entityKey}`;
  if (!b[cacheKey]) {
    const spec = DD_ALL_TABLE_SPECS[entityKey];
    const cols = [...spec.cols, 'source_run_id', 'source_row_hash', 'synced_at'];
    b[cacheKey] = db.prepare(`INSERT OR REPLACE INTO ${spec.table}
      (${cols.join(', ')}) VALUES (${cols.map((c) => '@' + c).join(', ')})`);
  }
  return b[cacheKey];
}

function normalizeRakutenDdRow(entityKey, r) {
  const spec = DD_ALL_TABLE_SPECS[entityKey];
  const out = {};
  for (const k of spec.required) {
    const v = r[k];
    if (v === null || v === undefined || String(v).trim() === '') {
      throw new HttpError(400, { error: 'bad_row', message: `missing required key: ${k}` });
    }
  }
  if (r.date_jst !== undefined && !/^\d{4}-\d{2}-\d{2}$/.test(r.date_jst)) {
    throw new HttpError(400, { error: 'bad_row', message: `bad date_jst: ${r.date_jst}` });
  }
  if (spec.validate) spec.validate(r, HttpError);
  for (const k of spec.cols) {
    out[k] = r[k] ?? (spec.defaults && k in spec.defaults ? spec.defaults[k] : null);
  }
  out.source_run_id = r.source_run_id;
  out.source_row_hash = r.source_row_hash;
  out.synced_at = r.synced_at;
  return out;
}

// アカウント単位フィー月次 (amazon-dashboard PR-C)
function getAmazonAccountFeesInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonAccountFeesInsert) {
    b.amazonAccountFeesInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_account_fees_monthly (
        date_jst, fee_type, amount_jpy, row_count,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @fee_type, @amount_jpy, @row_count,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonAccountFeesInsert;
}

function getRakutenFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenFinanceInsert) {
    b.rakutenFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_finance_sku_daily (
        date_jst, rakuten_code, ne_code, sku_resolution, product_name,
        units_ordered, units_cancelled, units_net_sold,
        allocated_units_cancelled, units_cancelled_same_day_matched,
        allocation_method, cancel_exceeds_ordered_warning,
        sales_principal_jpy_incl, sales_postage_jpy_incl,
        coupon_shop_jpy_incl, coupon_all_jpy_incl, promotion_jpy_incl,
        refund_amount_jpy_incl, allocated_refund_amount_jpy_incl, refund_amount_same_day_matched_jpy_incl,
        mall_fee_jpy_incl,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        gross_sales_jpy_incl, net_sales_jpy_incl,
        variable_margin_jpy_incl, refund_adjusted_net_sales_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @rakuten_code, @ne_code, @sku_resolution, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @allocated_units_cancelled, @units_cancelled_same_day_matched,
        @allocation_method, @cancel_exceeds_ordered_warning,
        @sales_principal_jpy_incl, @sales_postage_jpy_incl,
        @coupon_shop_jpy_incl, @coupon_all_jpy_incl, @promotion_jpy_incl,
        @refund_amount_jpy_incl, @allocated_refund_amount_jpy_incl, @refund_amount_same_day_matched_jpy_incl,
        @mall_fee_jpy_incl,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @gross_sales_jpy_incl, @net_sales_jpy_incl,
        @variable_margin_jpy_incl, @refund_adjusted_net_sales_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenFinanceInsert;
}

function getYahooFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.yahooFinanceInsert) {
    b.yahooFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_yahoo_finance_sku_daily (
        date_jst, yahoo_sku_key, ne_code, variant_key, resolution_method,
        unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        sales_principal_jpy_incl, sales_postage_jpy_incl, gross_sales_jpy_incl,
        net_sales_before_point_jpy_incl, listing_sales_estimated_jpy_incl,
        coupon_shop_jpy_incl, use_point_jpy_incl,
        mall_fee_jpy_incl, mall_fee_calc_method, mall_fee_estimate_delta_jpy,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_partial_jpy_incl, variable_margin_full_jpy_incl,
        refund_adjusted_net_sales_jpy_incl, margin_confidence, margin_full_finalized_at,
        pay_charge_audit_jpy_incl, ship_charge_audit_jpy_incl, discount_audit_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @yahoo_sku_key, @ne_code, @variant_key, @resolution_method,
        @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @sales_principal_jpy_incl, @sales_postage_jpy_incl, @gross_sales_jpy_incl,
        @net_sales_before_point_jpy_incl, @listing_sales_estimated_jpy_incl,
        @coupon_shop_jpy_incl, @use_point_jpy_incl,
        @mall_fee_jpy_incl, @mall_fee_calc_method, @mall_fee_estimate_delta_jpy,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_partial_jpy_incl, @variable_margin_full_jpy_incl,
        @refund_adjusted_net_sales_jpy_incl, @margin_confidence, @margin_full_finalized_at,
        @pay_charge_audit_jpy_incl, @ship_charge_audit_jpy_incl, @discount_audit_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.yahooFinanceInsert;
}

function getAupayFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.aupayFinanceInsert) {
    b.aupayFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_aupay_finance_sku_daily (
        date_jst, aupay_sku_key, ne_code, variant_key, resolution_method,
        unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        sales_principal_jpy_incl, postage_allocated_jpy_incl, gross_sales_jpy_incl,
        net_sales_after_coupon_jpy_incl, request_price_jpy_incl,
        coupon_shop_jpy_incl,
        gift_point_jpy_incl, use_ponta_point_jpy_incl, use_au_point_jpy_incl,
        premium_member_point_jpy_incl, point_cost_pending_jpy_incl,
        tax_normal_sales_jpy_incl, tax_reduced_sales_jpy_incl, tax_free_sales_jpy_incl,
        mall_fee_jpy_incl, mall_fee_rate_applied, mall_fee_calc_method, mall_fee_estimate_delta_jpy,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_partial_jpy_incl, variable_margin_full_jpy_incl,
        refund_adjusted_net_sales_jpy_incl, margin_confidence, margin_full_finalized_at,
        before_discount_jpy_incl, detail_discount_jpy_incl, charge_allocated_jpy_incl,
        item_option_jpy_incl, gift_wrapping_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        order_count, line_count,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @aupay_sku_key, @ne_code, @variant_key, @resolution_method,
        @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @sales_principal_jpy_incl, @postage_allocated_jpy_incl, @gross_sales_jpy_incl,
        @net_sales_after_coupon_jpy_incl, @request_price_jpy_incl,
        @coupon_shop_jpy_incl,
        @gift_point_jpy_incl, @use_ponta_point_jpy_incl, @use_au_point_jpy_incl,
        @premium_member_point_jpy_incl, @point_cost_pending_jpy_incl,
        @tax_normal_sales_jpy_incl, @tax_reduced_sales_jpy_incl, @tax_free_sales_jpy_incl,
        @mall_fee_jpy_incl, @mall_fee_rate_applied, @mall_fee_calc_method, @mall_fee_estimate_delta_jpy,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_partial_jpy_incl, @variable_margin_full_jpy_incl,
        @refund_adjusted_net_sales_jpy_incl, @margin_confidence, @margin_full_finalized_at,
        @before_discount_jpy_incl, @detail_discount_jpy_incl, @charge_allocated_jpy_incl,
        @item_option_jpy_incl, @gift_wrapping_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @order_count, @line_count,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.aupayFinanceInsert;
}

function getLinegiftFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.linegiftFinanceInsert) {
    b.linegiftFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_linegift_finance_sku_daily (
        date_jst, sku_code, ne_code, parent_item_code, variant_key, resolution_method,
        unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        sales_principal_jpy_incl, gross_sales_jpy_incl,
        mall_fee_jpy_incl, mall_fee_calc_method, mall_fee_estimate_delta_jpy,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_jpy_incl, refund_adjusted_net_sales_jpy_incl,
        margin_confidence, margin_full_finalized_at,
        recognized_on_jst, bought_date_jst, delivered_lag_days, received_lag_days,
        is_delivery_by_hand, delivery_agent,
        first_seen_in_api_at, last_seen_in_api_at, is_frozen_after_horizon,
        cost_status, is_cost_complete, data_quality_score,
        order_count, line_count,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @sku_code, @ne_code, @parent_item_code, @variant_key, @resolution_method,
        @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @sales_principal_jpy_incl, @gross_sales_jpy_incl,
        @mall_fee_jpy_incl, @mall_fee_calc_method, @mall_fee_estimate_delta_jpy,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_jpy_incl, @refund_adjusted_net_sales_jpy_incl,
        @margin_confidence, @margin_full_finalized_at,
        @recognized_on_jst, @bought_date_jst, @delivered_lag_days, @received_lag_days,
        @is_delivery_by_hand, @delivery_agent,
        @first_seen_in_api_at, @last_seen_in_api_at, @is_frozen_after_horizon,
        @cost_status, @is_cost_complete, @data_quality_score,
        @order_count, @line_count,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.linegiftFinanceInsert;
}

// LINEギフト v1.2 (PR-H): orders sync (集計用 subset)
function getLinegiftOrdersInsert(db) {
  const b = getStmtBundle(db);
  if (!b.linegiftOrdersInsert) {
    b.linegiftOrdersInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_linegift_orders (
        order_id, status, sku_code, parent_item_code,
        selling_price, fee,
        bought_at_jst, bought_date_jst, received_date_jst,
        is_frozen_after_horizon, source_run_id, synced_at
      ) VALUES (
        @order_id, @status, @sku_code, @parent_item_code,
        @selling_price, @fee,
        @bought_at_jst, @bought_date_jst, @received_date_jst,
        @is_frozen_after_horizon, @source_run_id, @synced_at
      )
    `);
  }
  return b.linegiftOrdersInsert;
}

function getQoo10FinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.qoo10FinanceInsert) {
    b.qoo10FinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_qoo10_finance_sku_daily (
        date_jst, sku_code, ne_code, qoo10_item_id, parent_item_code, variant_key,
        resolution_method, match_tier, unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        gmv_list_price_jpy_incl, customer_paid_jpy_incl, net_settlement_api_jpy_incl,
        platform_fee_jpy_incl, mall_fee_calc_method, settle_price_formula_scope,
        extra_fee_oversea_jpy_incl, cod_fee_jpy_incl,
        megawari_order_count, megawari_discount_amount_jpy_incl,
        megapo_order_count, megapo_discount_amount_jpy_incl,
        other_promo_order_count, other_promo_discount_jpy_incl,
        total_platform_promo_jpy_incl, qoo10_cart_discount_jpy_incl, seller_discount_api_jpy_incl,
        shop_promo_burden_jpy_incl, shop_promo_burden_status,
        domestic_non_cod_line_count, domestic_non_cod_formula_match_count,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_jpy_incl, variable_margin_full_jpy_incl,
        margin_confidence, margin_full_finalized_at,
        delivered_lag_days, shipping_lag_days, oversea_count, payment_methods_json,
        first_seen_in_api_at, last_seen_in_api_at, is_frozen_after_horizon,
        cost_status, is_cost_complete, data_quality_score,
        order_count, line_count,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @sku_code, @ne_code, @qoo10_item_id, @parent_item_code, @variant_key,
        @resolution_method, @match_tier, @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @gmv_list_price_jpy_incl, @customer_paid_jpy_incl, @net_settlement_api_jpy_incl,
        @platform_fee_jpy_incl, @mall_fee_calc_method, @settle_price_formula_scope,
        @extra_fee_oversea_jpy_incl, @cod_fee_jpy_incl,
        @megawari_order_count, @megawari_discount_amount_jpy_incl,
        @megapo_order_count, @megapo_discount_amount_jpy_incl,
        @other_promo_order_count, @other_promo_discount_jpy_incl,
        @total_platform_promo_jpy_incl, @qoo10_cart_discount_jpy_incl, @seller_discount_api_jpy_incl,
        @shop_promo_burden_jpy_incl, @shop_promo_burden_status,
        @domestic_non_cod_line_count, @domestic_non_cod_formula_match_count,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_jpy_incl, @variable_margin_full_jpy_incl,
        @margin_confidence, @margin_full_finalized_at,
        @delivered_lag_days, @shipping_lag_days, @oversea_count, @payment_methods_json,
        @first_seen_in_api_at, @last_seen_in_api_at, @is_frozen_after_horizon,
        @cost_status, @is_cost_complete, @data_quality_score,
        @order_count, @line_count,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.qoo10FinanceInsert;
}

function getFSalesByListingInsert(db) {
  const b = getStmtBundle(db);
  if (!b.fSalesByListingInsert) {
    // PR #156 hotfix: mirror_f_sales_by_listing は英語列 (date_jst 等) で統一、router の date_range strategy 互換
    b.fSalesByListingInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_f_sales_by_listing (
        date_jst, month_ym, mall, item_code, channel,
        item_name, units, sales_jpy_incl, order_count, data_source, source_updated_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @month_ym, @mall, @item_code, @channel,
        @item_name, @units, @sales_jpy_incl, @order_count, @data_source, @source_updated_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.fSalesByListingInsert;
}

function getLedgerInsert(db) {
  const b = getStmtBundle(db);
  if (!b.ledgerInsert) {
    b.ledgerInsert = db.prepare(`
      INSERT INTO sync_run_chunks
        (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum,
         contract_version, scope_from, scope_to, received_at, applied_at, mf_source_run_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
  }
  return b.ledgerInsert;
}

// ─── MF Phase 1a: insert factory + 列定義 (ENTITY_REGISTRY より先に必要) ───
// 子 MF mirror テーブル共通: INSERT OR REPLACE (run_id+業務PK でユニーク、idempotent)
// ※ 親 mf_publish_runs にはこの factory を使わない (下の makeMfPublishRunsUpsertFactory 参照)
function makeMfInsertFactory(table, cols) {
  return (db) => {
    const b = getStmtBundle(db);
    const cacheKey = '_mfInsert_' + table;
    if (!b[cacheKey]) {
      const colsList = cols.join(', ');
      const placeholders = cols.map(c => '@' + c).join(', ');
      b[cacheKey] = db.prepare(`INSERT OR REPLACE INTO ${table} (${colsList}) VALUES (${placeholders})`);
    }
    return b[cacheKey];
  };
}

// 親 mf_publish_runs 専用 UPSERT (設計監査 2026-07-06 S-3/V-1 対応):
// SQLite の INSERT OR REPLACE は内部的に DELETE+INSERT なので、親行の REPLACE が
// ON DELETE CASCADE で同 run_id の子 mart 全行を巻き添え削除していた。
// sender (sync-mf-marts-to-render.js) は毎回「miniPC 最新 success run」を選ぶため
// 同一 run_id の再送が常態で、再送が途中失敗すると子 mart が消えたまま
// status='pending_sync' に戻り、過去 run が無ければ経営ダッシュボードが空になる
// (feedback_mf_atomic_publish の「DELETE→INSERT 空テーブル」と同型の穴)。
// → UPDATE 型 UPSERT に変更し、既存の status / finalized_at は保持する:
//   ・既存 run が success のまま再送されても view から消えない (子行は row 単位で
//     OR REPLACE され、同一 run_id の内容は決定的なので混在しても実害なし)
//   ・新規 run は従来通り status='pending_sync' で INSERT され finalize で flip
//   ・前回途中失敗 (pending_sync) の再送も従来通り finalize まで進めば success
function makeMfPublishRunsUpsertFactory(cols) {
  return (db) => {
    const b = getStmtBundle(db);
    const cacheKey = '_mfUpsert_mirror_mf_publish_runs';
    if (!b[cacheKey]) {
      const colsList = cols.join(', ');
      const placeholders = cols.map(c => '@' + c).join(', ');
      const updatable = cols.filter(c => !['run_id', 'status', 'finalized_at'].includes(c));
      const setList = updatable.map(c => `${c} = excluded.${c}`).join(', ');
      b[cacheKey] = db.prepare(
        `INSERT INTO mirror_mf_publish_runs (${colsList}) VALUES (${placeholders})
         ON CONFLICT(run_id) DO UPDATE SET ${setList}`
      );
    }
    return b[cacheKey];
  };
}

const MF_PUBLISH_RUNS_COLS = [
  'run_id', 'scope', 'status', 'started_at', 'finished_at',
  'error_message', 'source_run_hash', 'synced_at', 'finalized_at'
];
const MF_EXECUTIVE_TOP_COLS = [
  'run_id', 'snapshot_date', 'current_month_ym',
  'sales_mtd_excl_tax', 'gross_profit_mtd_excl_tax', 'operating_income_mtd_excl_tax',
  'sales_month_end_forecast', 'gross_profit_month_end_forecast', 'operating_income_month_end_forecast',
  'forecast_status',
  'yoy_sales_pct', 'yoy_gross_profit_pct', 'yoy_operating_income_pct',
  'cash_balance_total', 'cash_balance_json', 'danger_signals_json',
  'data_window_from', 'data_window_to', 'reliability_label',
  'source_row_hash', 'synced_at'
];
const MF_PL_MONTHLY_COLS = [
  'run_id', 'month_ym', 'role_key', 'amount_excl_tax', 'tax_amount',
  'line_count', 'is_realized_only', 'source_row_hash', 'synced_at'
];
const MF_CHANNEL_SALES_COLS = [
  'run_id', 'month_ym', 'channel_key', 'channel_display_name',
  'gross_sales_excl_tax', 'pf_fee_excl_tax', 'ad_cost_excl_tax', 'fba_fee_excl_tax',
  'net_sales_after_pf_excl_tax', 'unmapped_amount_excl_tax', 'mapping_coverage_pct',
  'source_row_hash', 'synced_at'
];
const MF_CASH_EVENTS_DAILY_COLS = [
  'run_id', 'movement_date', 'bank_account_key', 'direction',
  'amount_excl_tax', 'event_count', 'source_row_hash', 'synced_at'
];
const MF_BALANCE_SNAPSHOT_COLS = [
  'run_id', 'month_ym', 'account_key', 'account_name', 'sub_account_name',
  'role_key', 'closing_balance_excl_tax', 'source_row_hash', 'synced_at'
];
const MF_ANOMALY_SIGNALS_COLS = [
  'run_id', 'signal_id', 'detected_at', 'signal_code', 'signal_key',
  'severity', 'severity_rank', 'title', 'description',
  'observed_value', 'threshold_value', 'related_entity_key',
  'recommended_action', 'source_mart',
  'source_row_hash', 'synced_at'
];
// Phase 1d-2: 期 (FY) 累計 + 平均残高
const MF_FY_SUMMARY_COLS = [
  'run_id', 'fy_number', 'fy_start_ym', 'fy_end_ym',
  'cumulative_through_ym', 'months_in_cumulative', 'is_fy_completed',
  'sales_cum', 'cogs_cum', 'gross_profit_cum', 'sgae_cum', 'operating_income_cum',
  'non_op_revenue_cum', 'non_op_expense_cum', 'ordinary_income_cum',
  'personnel_cost_cum',
  'ar_average', 'inventory_average', 'ap_average', 'total_asset_average',
  'ar_opening', 'ar_closing', 'inventory_opening', 'inventory_closing',
  'ap_opening', 'ap_closing', 'total_asset_opening', 'total_asset_closing',
  'cash_closing', 'short_loan_closing', 'long_loan_closing',
  'current_liab_closing', 'total_equity_closing',
  'source_row_hash', 'synced_at'
];
// Phase 1d-3b: BS section別月末
const MF_BS_MONTHLY_COLS = [
  'run_id', 'month_ym',
  'cash_total', 'ar_total', 'inventory_total', 'other_current_asset', 'current_asset_total',
  'tangible_fixed_asset', 'investment_other', 'fixed_asset_total',
  'total_asset',
  'ap_total', 'short_loan_total', 'other_current_liab', 'current_liab_total',
  'long_loan_total', 'other_fixed_liab', 'fixed_liab_total',
  'total_liab',
  'capital', 'retained', 'total_equity',
  'bs_other_total',
  'retained_balance', 'current_period_profit', 'display_total_equity',
  'source_row_hash', 'synced_at'
];
// Phase 1d-3b: BS 細目
const MF_BS_SUBACCOUNT_COLS = [
  'run_id', 'month_ym', 'account_name', 'sub_account_name', 'role_key', 'section',
  'closing_balance_excl_tax', 'is_hub_null_sub', 'source_row_hash', 'synced_at'
];

// ─── Entity Registry (entity-driven dispatch、楽天 #R-3b で導入) ───
// 新 entity (Yahoo / メルカリ等) 追加時はここに 1 エントリ追加するだけで
// chunk endpoint / backout endpoint が自動対応する
//
// clear_strategy:
//   'date_range' (default): 第1 chunk で meta[clear_meta_key] の date 配列を DELETE
//   'no_clear':             clear をスキップ (run_id ベース append-only 用、MF Phase 1a)
//
// requires_parent_run (MF Phase 1a):
//   true:  meta.mf_source_run_id を含み、Render 側で mirror_mf_publish_runs(run_id) 存在チェック
const ENTITY_REGISTRY = {
  amazon_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_finance_dates',
    getInsertStmt: getAmazonFinanceInsert,
    normalizeRow: (r) => normalizeAmazonFinanceRow(r),
  },
  // Amazon 広告費 2 entity (amazon-dashboard PR-A、2026-07-06)
  amazon_ads_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_ads_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_ads_sku_dates',
    getInsertStmt: getAmazonAdsSkuInsert,
    normalizeRow: (r) => normalizeAmazonAdsSkuRow(r),
  },
  amazon_ads_campaign_daily: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_ads_campaign_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_ads_campaign_dates',
    getInsertStmt: getAmazonAdsCampaignInsert,
    normalizeRow: (r) => normalizeAmazonAdsCampaignRow(r),
  },
  // アカウント単位フィー月次 (amazon-dashboard PR-C)。date_jst = 月初日
  amazon_account_fees_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_account_fees_monthly',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_account_fees_dates',
    getInsertStmt: getAmazonAccountFeesInsert,
    normalizeRow: (r) => normalizeAmazonAccountFeesRow(r),
  },
  // カート(Buy Box)価格 日次スナップショット (amazon-dashboard PR-D)
  amazon_price_snapshot_daily: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_price_snapshot_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_price_snapshot_dates',
    getInsertStmt: getAmazonPriceSnapshotInsert,
    normalizeRow: (r) => normalizeAmazonPriceSnapshotRow(r),
  },
  // 楽天RPP広告費 2 entity (mall-csv-fetcher P1、2026-07-09)
  // monthly は date_jst=月初日 YYYY-MM-01 を clear キーに使う (account_fees 月次と同方針)
  rakuten_ads_rpp_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_ads_rpp',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_ads_rpp_months',
    getInsertStmt: getRakutenAdsRppInsert,
    normalizeRow: (r) => normalizeRakutenAdsRppRow(r),
  },
  rakuten_ads_rpp_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_ads_rpp_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_ads_rpp_daily_dates',
    getInsertStmt: getRakutenAdsRppDailyInsert,
    normalizeRow: (r) => normalizeRakutenAdsRppDailyRow(r),
  },
  // 楽天RMSデータ分析 2 entity (mall-csv-fetcher P1-R2、2026-07-10)
  rakuten_item_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_item_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_item_daily_dates',
    getInsertStmt: getRakutenItemDailyInsert,
    normalizeRow: (r) => normalizeRakutenItemDailyRow(r),
  },
  rakuten_store_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_store_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_store_daily_dates',
    getInsertStmt: getRakutenStoreDailyInsert,
    normalizeRow: (r) => normalizeRakutenStoreDailyRow(r),
  },
  // 楽天データダウンロードハブ 7 entity (mall-csv-fetcher P1-R3、2026-07-10)
  rakuten_store_device_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_store_device_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_store_device_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_store_device_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_store_device_daily', r),
  },
  rakuten_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_sku_daily_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_sku_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_sku_daily', r),
  },
  rakuten_category_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_category_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_category_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_category_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_category_daily', r),
  },
  rakuten_campaigns: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_campaigns',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_campaign_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_campaigns'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_campaigns', r),
  },
  rakuten_purchaser_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_purchaser_monthly',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_purchaser_months',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_purchaser_monthly'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_purchaser_monthly', r),
  },
  rakuten_item_purchaser_snapshot: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_item_purchaser_snapshot',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_item_purchaser_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_item_purchaser_snapshot'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_item_purchaser_snapshot', r),
  },
  rakuten_genre_purchaser_snapshot: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_genre_purchaser_snapshot',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_genre_purchaser_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_genre_purchaser_snapshot'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_genre_purchaser_snapshot', r),
  },
  // Yahoo!ストクリ統計 6 entity (mall-csv-fetcher P1-Y、2026-07-11)
  yahoo_store_device_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_store_device_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_store_device_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_store_device_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_store_device_daily', r),
  },
  yahoo_inflow_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_inflow_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_inflow_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_inflow_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_inflow_daily', r),
  },
  yahoo_user_attr_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_user_attr_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_user_attr_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_user_attr_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_user_attr_daily', r),
  },
  yahoo_flash_hourly: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_flash_hourly',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_flash_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_flash_hourly'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_flash_hourly', r),
  },
  yahoo_item_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_item_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_item_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_item_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_item_daily', r),
  },
  yahoo_keyword_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_keyword_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_keyword_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'yahoo_keyword_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('yahoo_keyword_daily', r),
  },
  // Qoo10 Analytics 4 entity (mall-csv-fetcher P1-Q R1、2026-07-14)。qoo10_items=マスタ (no_clear)
  ...Object.fromEntries(Object.keys(QOO10_DATA_TABLE_SPECS).map((key) => [key, {
    contract_version: 1,
    mirror_table: QOO10_DATA_TABLE_SPECS[key].table,
    clear_strategy: key === 'qoo10_items' ? 'no_clear' : 'date_range',
    ...(key === 'qoo10_items' ? {} : { clear_meta_key: `clear_${key.replace(/_daily$/, '_dates')}` }),
    getInsertStmt: (db) => getRakutenDdInsert(db, key),
    normalizeRow: (r) => normalizeRakutenDdRow(key, r),
  }])),
  // SKUマップ 2 entity (価格一括改定ツール PR1、2026-08-28)。全置換 = full_snapshot
  ...Object.fromEntries(Object.keys(SKU_MAP_TABLE_SPECS).map((key) => [key, {
    contract_version: 1,
    mirror_table: SKU_MAP_TABLE_SPECS[key].table,
    clear_strategy: 'full_snapshot',
    getInsertStmt: (db) => getRakutenDdInsert(db, key),
    normalizeRow: (r) => normalizeRakutenDdRow(key, r),
  }])),
  // 楽天レビュー 日次集計 1 entity (mall-csv-fetcher P2 PR-A、2026-07-16)
  rakuten_review_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_review_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_review_daily_dates',
    getInsertStmt: (db) => getRakutenDdInsert(db, 'rakuten_review_daily'),
    normalizeRow: (r) => normalizeRakutenDdRow('rakuten_review_daily', r),
  },
  // au PAYマーケット分析 13 entity (mall-csv-fetcher P1-A、2026-07-13)
  // 月次 entity は date_jst=月初日を clear キーに使う (rakuten_purchaser_monthly と同方針)
  ...Object.fromEntries(Object.keys(AUPAY_DATA_TABLE_SPECS).map((key) => [key, {
    contract_version: 1,
    mirror_table: AUPAY_DATA_TABLE_SPECS[key].table,
    clear_strategy: 'date_range',
    clear_meta_key: `clear_${key.replace(/_daily$/, '_dates').replace(/_monthly$/, '_months').replace(/_weekly$/, '_dates')}`,
    getInsertStmt: (db) => getRakutenDdInsert(db, key),
    normalizeRow: (r) => normalizeRakutenDdRow(key, r),
  }])),
  rakuten_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_finance_dates',
    getInsertStmt: getRakutenFinanceInsert,
    normalizeRow: (r) => normalizeRakutenFinanceRow(r),
  },
  yahoo_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_finance_dates',
    getInsertStmt: getYahooFinanceInsert,
    normalizeRow: (r) => normalizeYahooFinanceRow(r),
  },
  aupay_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_aupay_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_aupay_finance_dates',
    getInsertStmt: getAupayFinanceInsert,
    normalizeRow: (r) => normalizeAupayFinanceRow(r),
  },
  linegift_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_linegift_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_linegift_finance_dates',
    getInsertStmt: getLinegiftFinanceInsert,
    normalizeRow: (r) => normalizeLinegiftFinanceRow(r),
  },
  // LINEギフト v1.2 (PR-H): orders 集計用 subset (PII 除外)
  // 90日 frozen horizon 越えの行は miniPC 側でも UPDATE 禁止なので no_clear で OK
  linegift_orders: {
    contract_version: 1,
    mirror_table: 'mirror_linegift_orders',
    clear_strategy: 'no_clear',
    getInsertStmt: getLinegiftOrdersInsert,
    normalizeRow: (r) => normalizeLinegiftOrdersRow(r),
  },
  qoo10_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_qoo10_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_qoo10_finance_dates',
    getInsertStmt: getQoo10FinanceInsert,
    normalizeRow: (r) => normalizeQoo10FinanceRow(r),
  },
  f_sales_by_listing: {
    contract_version: 1,
    mirror_table: 'mirror_f_sales_by_listing',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_f_sales_by_listing_dates',
    getInsertStmt: getFSalesByListingInsert,
    normalizeRow: (r) => normalizeFSalesByListingRow(r),
  },

  // ─── MF Phase 1a (Codex 5ラウンド確定設計) ─────────────────────────
  // 受信順序契約 (sync-to-render.js 側で保証):
  //   1. mf_publish_runs を最初に送信 (status='pending_sync' で親 row 作成)
  //   2. 6 mart entities (children) 全部送信
  //   3. POST /api/sync/mf/runs/:run_id/finalize で status='success' に flip
  // VIEW v_mirror_mf_*_latest は status='success' の最新 run のみ公開
  mf_publish_runs: {
    contract_version: 1,
    mirror_table: 'mirror_mf_publish_runs',
    clear_strategy: 'no_clear',
    requires_parent_run: false,
    // OR REPLACE 禁止: CASCADE で子 mart が消える (makeMfPublishRunsUpsertFactory のコメント参照)
    getInsertStmt: makeMfPublishRunsUpsertFactory(MF_PUBLISH_RUNS_COLS),
    normalizeRow: (r) => normalizeMfPublishRunRow(r),
  },
  mf_executive_top: {
    contract_version: 1,
    mirror_table: 'mirror_mf_executive_top',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_executive_top', MF_EXECUTIVE_TOP_COLS),
    normalizeRow: (r) => normalizeMfExecutiveTopRow(r),
  },
  mf_pl_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_pl_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_pl_monthly', MF_PL_MONTHLY_COLS),
    normalizeRow: (r) => normalizeMfPlMonthlyRow(r),
  },
  mf_channel_sales: {
    contract_version: 1,
    mirror_table: 'mirror_mf_channel_sales',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_channel_sales', MF_CHANNEL_SALES_COLS),
    normalizeRow: (r) => normalizeMfChannelSalesRow(r),
  },
  mf_cash_events_daily: {
    contract_version: 1,
    mirror_table: 'mirror_mf_cash_events_daily',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_cash_events_daily', MF_CASH_EVENTS_DAILY_COLS),
    normalizeRow: (r) => normalizeMfCashEventsDailyRow(r),
  },
  mf_balance_snapshot_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_balance_snapshot_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_balance_snapshot_monthly', MF_BALANCE_SNAPSHOT_COLS),
    normalizeRow: (r) => normalizeMfBalanceSnapshotRow(r),
  },
  mf_anomaly_signals: {
    contract_version: 1,
    mirror_table: 'mirror_mf_anomaly_signals',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_anomaly_signals', MF_ANOMALY_SIGNALS_COLS),
    normalizeRow: (r) => normalizeMfAnomalySignalsRow(r),
  },
  // Phase 1d-2: 期 (FY) 累計 + 平均残高
  mf_fy_summary: {
    contract_version: 1,
    mirror_table: 'mirror_mf_fy_summary',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_fy_summary', MF_FY_SUMMARY_COLS),
    normalizeRow: (r) => normalizeMfFySummaryRow(r),
  },
  // Phase 1d-3b: BS section別月末
  mf_bs_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_bs_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_bs_monthly', MF_BS_MONTHLY_COLS),
    normalizeRow: (r) => normalizeMfBsMonthlyRow(r),
  },
  // Phase 1d-3b: BS 細目
  mf_bs_subaccount_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_bs_subaccount_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_bs_subaccount_monthly', MF_BS_SUBACCOUNT_COLS),
    normalizeRow: (r) => normalizeMfBsSubaccountRow(r),
  },
};

// HttpError: tx 内から throw して outer catch で http response に変換
class HttpError extends Error {
  constructor(status, body) { super(body.error || 'http_error'); this.status = status; this.body = body; }
}

router.post('/api/sync/:entity/chunk', requireSyncKey, async (req, res) => {
  const entity = req.params.entity;
  const body = req.body || {};
  const {
    sync_run_id, contract_version, scope_from, scope_to,
    chunk_index, chunk_count, is_first, is_last, row_count,
    payload_checksum, meta = {}, payload = {}
  } = body;

  // ────────── 1. payload validation ──────────
  if (!sync_run_id || typeof sync_run_id !== 'string') {
    return res.status(400).json({ error: 'sync_run_id required' });
  }
  if (!Number.isInteger(contract_version)) {
    return res.status(400).json({ error: 'contract_version must be integer' });
  }
  if (!Number.isInteger(chunk_index) || chunk_index < 0) {
    return res.status(400).json({ error: 'chunk_index must be non-negative integer' });
  }
  if (!Number.isInteger(chunk_count) || chunk_count <= 0) {
    return res.status(400).json({ error: 'chunk_count must be positive integer' });
  }
  if (chunk_index >= chunk_count) {
    return res.status(400).json({ error: `chunk_index=${chunk_index} >= chunk_count=${chunk_count}` });
  }
  if (typeof payload_checksum !== 'string' || payload_checksum.length === 0) {
    return res.status(400).json({ error: 'payload_checksum required' });
  }
  if (!DATE_RE.test(scope_from) || !DATE_RE.test(scope_to)) {
    return res.status(400).json({ error: 'scope_from/scope_to must be YYYY-MM-DD' });
  }
  if (scope_from > scope_to) {
    return res.status(400).json({ error: `scope_from=${scope_from} > scope_to=${scope_to}` });
  }
  const isFirstExpected = chunk_index === 0;
  const isLastExpected = chunk_index === chunk_count - 1;
  if (Boolean(is_first) !== isFirstExpected) {
    return res.status(400).json({ error: `is_first=${is_first} mismatch chunk_index=${chunk_index} (expected ${isFirstExpected})` });
  }
  if (Boolean(is_last) !== isLastExpected) {
    return res.status(400).json({ error: `is_last=${is_last} mismatch chunk_index=${chunk_index}/${chunk_count} (expected ${isLastExpected})` });
  }
  const rows = Array.isArray(payload?.rows) ? payload.rows : null;
  if (rows === null) {
    return res.status(400).json({ error: 'payload.rows must be array' });
  }
  if (!Number.isInteger(row_count) || row_count !== rows.length) {
    return res.status(400).json({ error: `row_count=${row_count} != payload.rows.length=${rows.length}` });
  }

  // ────────── 2. entity / contract_version 値域確認 (Codex Round 12 #5) ──────────
  const entityCfg = ENTITY_REGISTRY[entity];
  if (!entityCfg) {
    return res.status(400).json({ error: `unsupported entity: ${entity}` });
  }
  // Yahoo!表の初期化が fail-soft で失敗している場合、Yahoo entity だけ503 (原因つき)。
  // 他モールは通常どおり動く (2026-07-12 の全停止障害の再発防御)
  if (entity.startsWith('yahoo_') && yahooInitError) {
    return res.status(503).json({ error: 'yahoo tables init failed', init_error: yahooInitError });
  }
  // au PAY分析表も同様 (⚠️既存の aupay_finance_sku_daily は fail-soft 対象外なので巻き込まない)
  if (AUPAY_DATA_ENTITY_NAMES.has(entity) && aupayDataInitError) {
    return res.status(503).json({ error: 'aupay data tables init failed', init_error: aupayDataInitError });
  }
  if (QOO10_DATA_ENTITY_NAMES.has(entity) && qoo10DataInitError) {
    return res.status(503).json({ error: 'qoo10 data tables init failed', init_error: qoo10DataInitError });
  }
  if (RAKUTEN_REVIEW_ENTITY_NAMES.has(entity) && rakutenReviewInitError) {
    return res.status(503).json({ error: 'rakuten review tables init failed', init_error: rakutenReviewInitError });
  }
  if (SKU_MAP_ENTITY_NAMES.has(entity) && skuMapInitError) {
    return res.status(503).json({ error: 'sku map tables init failed', init_error: skuMapInitError });
  }
  if (contract_version !== entityCfg.contract_version) {
    return res.status(400).json({
      error: 'contract_version_mismatch',
      message: `entity=${entity} requires contract_version=${entityCfg.contract_version} (got ${contract_version})`,
    });
  }

  // ────────── 3. checksum 検証 + first chunk meta 形式 ──────────
  const crypto = await import('node:crypto');
  const payloadStr = JSON.stringify(payload);
  const computedChecksum = crypto.createHash('sha256').update(payloadStr).digest('hex');
  if (computedChecksum !== payload_checksum) {
    console.error(`[Mirror] checksum mismatch req entity=${entity} chunk=${chunk_index}`);
    return res.status(400).json({
      error: 'payload_checksum mismatch',
      expected: payload_checksum, computed: computedChecksum,
    });
  }
  let clearDates = null;
  const clearStrategy = entityCfg.clear_strategy || 'date_range';  // backward compat
  // full_snapshot (SKUマップ 2種): 毎 run で mirror 表を全置換する。分割 run は
  // 「clear 済みだが後続 chunk が届かない」窓で表が欠けるため、単一 chunk のみ受ける
  // (対象は数百行のマスタ。増えて分割されたら 400 で気づける = 黙って壊れない)
  if (clearStrategy === 'full_snapshot' && chunk_count !== 1) {
    return res.status(400).json({
      error: 'full_snapshot_requires_single_chunk',
      message: `entity=${entity} は full_snapshot のため chunk_count=1 のみ受信可 (got ${chunk_count})`,
    });
  }
  let snapshotGeneration = null;
  if (clearStrategy === 'full_snapshot') {
    // 世代 (単調増加)。古い run の遅延到着で新しい map を巻き戻さない (Codex R1 High #1)。
    // 値は送信元 (miniPC) が sync_snapshot_generations で採番する — 時刻ベースにしない
    snapshotGeneration = meta.snapshot_generation;
    if (!Number.isInteger(snapshotGeneration) || snapshotGeneration < 1) {
      return res.status(400).json({
        error: 'snapshot_generation_required',
        message: `entity=${entity} は meta.snapshot_generation (1以上の整数) が必要`,
      });
    }
    // 監査メタは受け側で付け直すが、送信元の申告と食い違う run は事故なので先に弾く (Codex R1 Medium #5)
    for (const r of rows) {
      if (!r || typeof r !== 'object') {
        return res.status(400).json({ error: 'bad_row', message: 'row is not an object' });
      }
      if (r.source_run_id !== undefined && r.source_run_id !== sync_run_id) {
        return res.status(400).json({
          error: 'source_run_id_mismatch',
          message: `row.source_run_id=${r.source_run_id} != sync_run_id=${sync_run_id}`,
        });
      }
    }
    // PK 重複は 400 (Codex R2 High)。保存は INSERT OR REPLACE なので、同じキーを 2 回含む
    // snapshot は「行数は足りているのに実際は半分」になり、減少ゲートも世代表の row_count も
    // すり抜ける (監査上も欠損が見えない)。送信側にも同じ検査があるが、受け側でも必ず見る
    {
      const spec = DD_ALL_TABLE_SPECS[entity];
      const keyCols = spec.pkCols || spec.cols.filter((c) => c === 'store_id' || c.endsWith('_key'));
      const seen = new Set();
      for (const r of rows) {
        const pk = JSON.stringify(keyCols.map((c) => r[c]));
        if (seen.has(pk)) {
          return res.status(400).json({
            error: 'duplicate_primary_key',
            message: `entity=${entity} の snapshot に PK 重複: ${pk} (全置換で行が黙って減る)`,
          });
        }
        seen.add(pk);
      }
    }
    // 空 snapshot は fail-closed。送信側の事故 (source 表の消失・SELECT ミス) で mirror を
    // 黙って空にしない。全消しは「消える件数を言い当てられること」まで要求する (Codex R1 Medium #6)
    if (rows.length === 0 && meta.allow_empty_snapshot !== true) {
      return res.status(400).json({
        error: 'empty_snapshot_rejected',
        message: `entity=${entity} の full_snapshot が 0 件。意図的なら meta.allow_empty_snapshot=true と meta.expected_deleted_rows を付けてください`,
      });
    }
  }
  if (is_first && clearStrategy === 'date_range') {
    clearDates = meta[entityCfg.clear_meta_key];
    if (!Array.isArray(clearDates) || clearDates.length === 0) {
      return res.status(400).json({ error: `first chunk requires meta.${entityCfg.clear_meta_key} (non-empty array)` });
    }
    for (const d of clearDates) {
      if (!DATE_RE.test(d)) return res.status(400).json({ error: `invalid date in clear list: ${d}` });
    }
  }

  // MF Phase 1a (Codex review #88 反映):
  //   - 全 MF entity (mf_publish_runs 親 + 6 mart 子) で meta.mf_source_run_id を要求 + ledger 保存
  //   - 子 entity は parent.status='pending_sync' を tx 内で再 check (race 防止)
  //   - finalize 時に ledger.mf_source_run_id == finalize_run_id を cross-check
  // ⚠️判定は entity 名 (mf_*) で行う。旧実装の「clear_strategy==='no_clear' ⇒ MF」は
  // MF以外の no_clear マスタ (linegift_orders / qoo10_items) を誤って400にする (2026-07-14 発覚)
  const isMfEntity = entity.startsWith('mf_');
  let mfSourceRunIdForLedger = null;
  if (isMfEntity) {
    const mfSourceRunId = meta.mf_source_run_id;
    if (is_first && !Number.isInteger(mfSourceRunId)) {
      return res.status(400).json({ error: `MF entity '${entity}' first chunk requires meta.mf_source_run_id (integer)` });
    }
    if (Number.isInteger(mfSourceRunId)) {
      mfSourceRunIdForLedger = mfSourceRunId;
    }
  }

  // ────────── 4. tx 実行 (BEGIN IMMEDIATE で write 直列化、Codex Round 12 #1 #3) ──────────
  const db = getMirrorDB();
  const now = new Date().toISOString();
  const requestId = `sync-${Date.now().toString(36)}`;
  const insertStmt = entityCfg.getInsertStmt(db);
  const insertLedger = getLedgerInsert(db);

  // 構造監査 H-4: date_range clear の multi-chunk run は staging 経由で原子的に apply する。
  // single-chunk run は従来どおり (clear+INSERT が元々単一 tx で atomic)。
  // no_clear (MF 等) は独自の finalize 契約があるため対象外。
  const useStaging = clearStrategy === 'date_range' && chunk_count > 1;

  // 全 chunk 到着時に呼ぶ: scope DELETE → stage 全行 INSERT → stage 掃除 → applied マーカー。
  // 呼び出し元 tx 内で実行される (単一 tx で「clear済+部分データ」を外部に見せない)。
  // 既 applied なら null (chunk 再送 replay での二重 apply 防止)。
  const applyStagedRun = () => {
    const already = db.prepare(`SELECT applied_at FROM sync_run_applied WHERE run_id = ? AND entity = ?`).get(sync_run_id, entity);
    if (already) return null;
    const metaRow = db.prepare(`SELECT clear_dates_json FROM sync_stage_meta WHERE run_id = ? AND entity = ?`).get(sync_run_id, entity);
    // stage coverage は ledger の staged フラグを正とする (sync_stage_rows の行数だと
    // row_count=0 の空 chunk を「stage 欠落」と誤判定して永久 409 になる — Codex R2 High #1)
    const stageChunks = db.prepare(`SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ? AND staged = 1`).get(sync_run_id, entity).c;
    // 全 chunk が新経路で stage されている時だけ apply する。不足 = デプロイ跨ぎで一部 chunk が
    // 旧コードにより live 直接 apply された混在 run。clear 実施有無を事後に確定できず、
    // 推測での救済 (clear せず追加 INSERT 等) は取り違えると欠損/重複を作るため、
    // 明示的に 409 で失敗させて新 run_id での再同期を要求する (fail-closed、Codex R1)
    if (stageChunks !== chunk_count || !metaRow) {
      throw new HttpError(409, {
        error: 'staged_run_incomplete_stage',
        message: `staged run の stage 内容が不完全 (stage_chunks=${stageChunks}/${chunk_count}, meta=${metaRow ? 'ok' : 'missing'})。` +
          `デプロイ跨ぎ等の混在 run のため apply できません。新しい sync_run_id で再同期してください`,
        entity, sync_run_id,
      });
    }
    const dates = JSON.parse(metaRow.clear_dates_json);
    const placeholders = dates.map(() => '?').join(',');
    const clearedRows = db.prepare(`DELETE FROM ${entityCfg.mirror_table} WHERE date_jst IN (${placeholders})`).run(...dates).changes;
    const stageRows = db.prepare(`SELECT row_json FROM sync_stage_rows WHERE run_id = ? AND entity = ? ORDER BY chunk_index, row_idx`).all(sync_run_id, entity);
    for (const s of stageRows) {
      insertStmt.run(entityCfg.normalizeRow(JSON.parse(s.row_json)));
    }
    db.prepare(`DELETE FROM sync_stage_rows WHERE run_id = ? AND entity = ?`).run(sync_run_id, entity);
    db.prepare(`DELETE FROM sync_stage_meta WHERE run_id = ? AND entity = ?`).run(sync_run_id, entity);
    db.prepare(`INSERT INTO sync_run_applied (run_id, entity, applied_at, cleared_rows, applied_rows) VALUES (?,?,?,?,?)`)
      .run(sync_run_id, entity, now, clearedRows, stageRows.length);
    // ledger: staged 中は applied_at=NULL で受信記録のみ。live 反映が確定したここで一括更新
    db.prepare(`UPDATE sync_run_chunks SET applied_at = ? WHERE run_id = ? AND entity = ?`).run(now, sync_run_id, entity);
    return { clearedRows, appliedRows: stageRows.length };
  };

  let result;
  try {
    db.transaction(() => {
      // 4a. run 単位の不変条件: 既存 chunk があれば contract_version / scope / chunk_count 一致 (#2)
      const runFirst = db.prepare(`
        SELECT contract_version, scope_from, scope_to, chunk_count
        FROM sync_run_chunks WHERE run_id = ? AND entity = ? LIMIT 1
      `).get(sync_run_id, entity);
      if (runFirst) {
        if (runFirst.contract_version !== contract_version
            || runFirst.scope_from !== scope_from
            || runFirst.scope_to !== scope_to
            || runFirst.chunk_count !== chunk_count) {
          throw new HttpError(409, {
            error: 'run_invariant_mismatch',
            message: 'run 内の chunk で contract_version / scope / chunk_count が一致しません',
            run_existing: runFirst,
            received: { contract_version, scope_from, scope_to, chunk_count },
          });
        }
      }

      // 4b. 同一 chunk_index 既存確認 (idempotent re-send 短絡 / 不一致は 409、Codex Round 12 #3)
      const existing = db.prepare(`
        SELECT chunk_count, row_count, payload_checksum, scope_from, scope_to
        FROM sync_run_chunks WHERE run_id = ? AND entity = ? AND chunk_index = ?
      `).get(sync_run_id, entity, chunk_index);
      if (existing) {
        if (existing.payload_checksum !== payload_checksum
            || existing.row_count !== row_count
            || existing.chunk_count !== chunk_count) {
          throw new HttpError(409, {
            error: 'chunk_resend_mismatch',
            existing, received: { payload_checksum, row_count, chunk_count },
          });
        }
        // staged run: 全 chunk 受信済みなのに未 apply (直前の apply tx が 500 で失敗した後の再送等) なら
        // ここで apply を再試行する (applyStagedRun は applied マーカーで冪等)
        let appliedNow = null;
        if (useStaging) {
          const received = db.prepare(`SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?`).get(sync_run_id, entity).c;
          if (received === chunk_count) appliedNow = applyStagedRun();
        }
        result = { replayed: true, appliedNow };
        return;
      }

      // 4c. scope clear (is_first かつ この run でまだ何も apply 無い時のみ、#1 race 解消版)
      // MF Phase 1a: clear_strategy='no_clear' は scope clear をスキップ (run_id ベース append-only)
      // H-4: staged run は live clear を apply 時 (全 chunk 到着) まで遅延。ここでは clear_dates の
      // 持ち越し保存と、放置された旧 run の stage 残骸掃除のみ行う
      let didClear = false;
      let clearedRows = 0;
      if (is_first && clearStrategy === 'date_range') {
        const priorCount = db.prepare(`
          SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?
        `).get(sync_run_id, entity).c;
        if (priorCount === 0) {
          if (useStaging) {
            // 同一 entity の別 run の stage 残骸を掃除 — ただし「最終受信から 24h 超」の放棄確定 run のみ
            // (無条件削除だと進行中の並行 run の stage を破壊し、正常 run を強制 409 にする — Codex R2 High #2)
            const abandonCutoff = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
            const staleRuns = db.prepare(`
              SELECT DISTINCT run_id FROM (
                SELECT run_id FROM sync_stage_rows WHERE entity = ? AND run_id <> ?
                UNION SELECT run_id FROM sync_stage_meta WHERE entity = ? AND run_id <> ?
              ) cand
              WHERE COALESCE((
                SELECT MAX(c.received_at) FROM sync_run_chunks c WHERE c.run_id = cand.run_id AND c.entity = ?
              ), '') < ?
            `).all(entity, sync_run_id, entity, sync_run_id, entity, abandonCutoff);
            for (const sr of staleRuns) {
              db.prepare(`DELETE FROM sync_stage_rows WHERE entity = ? AND run_id = ?`).run(entity, sr.run_id);
              db.prepare(`DELETE FROM sync_stage_meta WHERE entity = ? AND run_id = ?`).run(entity, sr.run_id);
            }
            // applied マーカーの経年掃除 (30日超)
            const cutoff = new Date(Date.now() - 30 * 86400000).toISOString();
            db.prepare(`DELETE FROM sync_run_applied WHERE entity = ? AND applied_at < ?`).run(entity, cutoff);
          } else {
            const placeholders = clearDates.map(() => '?').join(',');
            clearedRows = db.prepare(`
              DELETE FROM ${entityCfg.mirror_table} WHERE date_jst IN (${placeholders})
            `).run(...clearDates).changes;
            didClear = true;
          }
        }
      }
      // 4c-2. full_snapshot (SKUマップ 2種): 全行 DELETE → 同一 tx 内でこの chunk を INSERT。
      // 単一 chunk 限定なので clear と投入が原子的に入れ替わり、途中で空になる窓が無い
      if (is_first && clearStrategy === 'full_snapshot') {
        const priorCount = db.prepare(`
          SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?
        `).get(sync_run_id, entity).c;
        if (priorCount === 0) {
          // (a) 世代の単調性: 古い snapshot の遅延到着を 409 で拒否する
          const genRow = db.prepare('SELECT generation, run_id FROM mirror_snapshot_generations WHERE entity = ?').get(entity);
          if (genRow && snapshotGeneration <= genRow.generation) {
            throw new HttpError(409, {
              error: 'stale_snapshot',
              message: `entity=${entity} は generation=${genRow.generation} (run=${genRow.run_id}) を適用済み。received=${snapshotGeneration} は古いので拒否`,
              entity, applied_generation: genRow.generation, received_generation: snapshotGeneration,
            });
          }
          // (b) 件数ゲート: 大幅に減る snapshot は明示承認が要る
          const liveCount = db.prepare(`SELECT COUNT(*) AS c FROM ${entityCfg.mirror_table}`).get().c;
          if (liveCount > 0) {
            if (rows.length === 0) {
              // 全消しは「消える件数を言い当てられること」を条件にする (誤爆した client では通らない)
              if (meta.expected_deleted_rows !== liveCount) {
                throw new HttpError(400, {
                  error: 'empty_snapshot_confirmation_mismatch',
                  message: `全消しには meta.expected_deleted_rows=${liveCount} (現在の行数) が必要 (received=${JSON.stringify(meta.expected_deleted_rows)})`,
                  entity, live_rows: liveCount,
                });
              }
            } else if (rows.length < liveCount * (1 - FULL_SNAPSHOT_MAX_SHRINK_RATIO) && meta.allow_shrink !== true) {
              throw new HttpError(400, {
                error: 'snapshot_shrink_rejected',
                message: `entity=${entity} が ${liveCount} 件 → ${rows.length} 件へ大幅減少 (許容 ${Math.round(FULL_SNAPSHOT_MAX_SHRINK_RATIO * 100)}%)。`
                  + ' 欠損 snapshot の可能性があるため拒否。意図した削除なら meta.allow_shrink=true',
                entity, live_rows: liveCount, incoming_rows: rows.length,
              });
            }
          }
          clearedRows = db.prepare(`DELETE FROM ${entityCfg.mirror_table}`).run().changes;
          didClear = true;
        }
      }
      // H-4: clear_dates の持ち越し保存は priorCount に依存させない (chunk 0 が後着する run —
      // 再送順序の乱れ等 — で meta 未保存のまま恒久 409 になるのを防ぐ — Codex R3 High)。
      // replay は 4b で短絡済みなので、ここに来る chunk 0 は常に新規受信
      if (is_first && clearStrategy === 'date_range' && useStaging) {
        db.prepare(`INSERT OR REPLACE INTO sync_stage_meta (run_id, entity, clear_dates_json, created_at) VALUES (?,?,?,?)`)
          .run(sync_run_id, entity, JSON.stringify(clearDates), now);
      }

      // 4c-mf. MF entity の場合、tx 内で parent.status='pending_sync' を再 check (race 防止)
      // Codex review #88: success/failed flip 後の追加 INSERT/REPLACE を完全拒否
      if (entityCfg.requires_parent_run) {
        if (!Number.isInteger(mfSourceRunIdForLedger)) {
          throw new HttpError(400, {
            error: 'mf_source_run_id_missing',
            message: `MF mart entity '${entity}' chunk_index=${chunk_index} requires meta.mf_source_run_id (integer) on every chunk`,
          });
        }
        const parent = db.prepare(`SELECT run_id, status FROM mirror_mf_publish_runs WHERE run_id = ?`).get(mfSourceRunIdForLedger);
        if (!parent) {
          throw new HttpError(409, {
            error: 'parent_run_not_found',
            message: `mirror_mf_publish_runs.run_id=${mfSourceRunIdForLedger} が存在しません。先に entity='mf_publish_runs' を送信してください。`,
            entity, sync_run_id, mf_source_run_id: mfSourceRunIdForLedger,
          });
        }
        if (parent.status !== 'pending_sync') {
          throw new HttpError(409, {
            error: 'parent_run_not_pending',
            message: `mirror_mf_publish_runs.run_id=${mfSourceRunIdForLedger} status=${parent.status} (pending_sync のみ受信可、success/failed 確定済 run の追加変更を拒否)`,
            entity, sync_run_id, mf_source_run_id: mfSourceRunIdForLedger, parent_status: parent.status,
          });
        }
        // sync_run_id 内で mf_source_run_id 一貫性チェック (異 run 混入防止)
        if (runFirst) {
          const firstMfRow = db.prepare(`
            SELECT mf_source_run_id FROM sync_run_chunks
            WHERE run_id = ? AND entity = ? LIMIT 1
          `).get(sync_run_id, entity);
          if (firstMfRow && firstMfRow.mf_source_run_id !== mfSourceRunIdForLedger) {
            throw new HttpError(409, {
              error: 'mf_source_run_id_mismatch_within_sync_run',
              message: `sync_run_id 内で異なる mf_source_run_id が混入 (first=${firstMfRow.mf_source_run_id}, current=${mfSourceRunIdForLedger})`,
              entity, sync_run_id,
            });
          }
        }
      }

      // 4d. row INSERT (staged run は live 表ではなく stage へ。apply は全 chunk 到着時に一括)
      if (useStaging) {
        const stageInsert = db.prepare(`INSERT OR REPLACE INTO sync_stage_rows (run_id, entity, chunk_index, row_idx, row_json) VALUES (?,?,?,?,?)`);
        for (let i = 0; i < rows.length; i++) {
          stageInsert.run(sync_run_id, entity, chunk_index, i, JSON.stringify(rows[i]));
        }
      } else if (clearStrategy === 'full_snapshot') {
        // 監査メタ (source_run_id / source_row_hash / synced_at) は受け側で付け直す。
        // 送信元の申告をそのまま保存すると、監査値が偽装・欠落しても気づけない (Codex R1 Medium #5)
        const spec = DD_ALL_TABLE_SPECS[entity];
        for (const r of rows) {
          const norm = entityCfg.normalizeRow(r);
          norm.source_run_id = sync_run_id;
          norm.source_row_hash = crypto.createHash('sha256')
            .update(JSON.stringify(spec.cols.map((c) => norm[c]))).digest('hex').slice(0, 16);
          norm.synced_at = now;
          insertStmt.run(norm);
        }
      } else {
        for (const r of rows) {
          insertStmt.run(entityCfg.normalizeRow(r));
        }
      }
      // full_snapshot の世代を確定 (この tx が commit されて初めて「適用済み」になる)
      if (is_first && clearStrategy === 'full_snapshot') {
        db.prepare(`INSERT INTO mirror_snapshot_generations (entity, generation, run_id, row_count, applied_at)
          VALUES (?,?,?,?,?)
          ON CONFLICT(entity) DO UPDATE SET generation = excluded.generation, run_id = excluded.run_id,
            row_count = excluded.row_count, applied_at = excluded.applied_at`)
          .run(entity, snapshotGeneration, sync_run_id, rows.length, now);
      }

      // 4e. ledger insert (mf_source_run_id NULL=非MF entity)。
      // staged chunk は live 未反映なので applied_at=NULL、applyStagedRun が反映確定時に一括 UPDATE する
      insertLedger.run(sync_run_id, entity, chunk_index, chunk_count, row_count,
                       payload_checksum, contract_version, scope_from, scope_to, now,
                       useStaging ? null : now,
                       mfSourceRunIdForLedger);
      if (useStaging) {
        // staged フラグ (stage coverage 判定の正。0 行 chunk も staged としてカウントされる)
        db.prepare(`UPDATE sync_run_chunks SET staged = 1 WHERE run_id = ? AND entity = ? AND chunk_index = ?`)
          .run(sync_run_id, entity, chunk_index);
      }

      // 4f. staged run: この chunk で全 chunk 揃ったら同一 tx 内で原子的に apply
      let appliedNow = null;
      if (useStaging) {
        const received = db.prepare(`SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?`).get(sync_run_id, entity).c;
        if (received === chunk_count) appliedNow = applyStagedRun();
      }

      result = { applied: true, didClear, clearedRows, staged: useStaging, appliedNow };
    }).immediate();  // BEGIN IMMEDIATE で write 直列化 (cross-process 安全)
  } catch (e) {
    if (e instanceof HttpError) {
      return res.status(e.status).json({ ...e.body, request_id: requestId });
    }
    console.error(`[Mirror] sync chunk error req=${requestId}: ${e.message}`);
    return res.status(500).json({ error: e.message, request_id: requestId });
  }

  if (result.replayed) {
    const replayApplied = result.appliedNow ? ` staged_apply=${result.appliedNow.appliedRows}rows/cleared=${result.appliedNow.clearedRows}` : '';
    console.log(`[Mirror] sync chunk replayed (idempotent) req=${requestId} entity=${entity} run=${sync_run_id} chunk=${chunk_index}${replayApplied}`);
    return res.json({ ok: true, request_id: requestId, sync_run_id, entity, chunk_index, replayed: true, staged_apply: result.appliedNow || undefined });
  }

  const stagedInfo = result.staged
    ? (result.appliedNow ? ` staged_apply=${result.appliedNow.appliedRows}rows/cleared=${result.appliedNow.clearedRows}` : ' staged')
    : ` cleared=${result.didClear ? result.clearedRows : 'no'}`;
  console.log(`[Mirror] sync chunk applied req=${requestId} entity=${entity} run=${sync_run_id} chunk=${chunk_index}/${chunk_count} rows=${rows.length}${stagedInfo}`);

  // ────────── 5. is_last なら欠番チェック ──────────
  if (is_last) {
    const present = db.prepare(`
      SELECT chunk_index, row_count FROM sync_run_chunks
      WHERE run_id = ? AND entity = ? ORDER BY chunk_index
    `).all(sync_run_id, entity);
    const presentSet = new Set(present.map(p => p.chunk_index));
    const missing = [];
    for (let i = 0; i < chunk_count; i++) if (!presentSet.has(i)) missing.push(i);
    const totalRows = present.reduce((s, p) => s + p.row_count, 0);
    if (missing.length > 0) {
      console.warn(`[Mirror] sync is_last but missing chunks: ${missing.join(',')} run=${sync_run_id}`);
      return res.status(409).json({
        ok: false, request_id: requestId, sync_run_id, entity,
        status: 'incomplete',
        chunks_received: present.length, expected: chunk_count,
        missing_chunks: missing, rows_received: totalRows,
      });
    }
    return res.json({
      ok: true, request_id: requestId, sync_run_id, entity, status: 'completed',
      chunks_received: present.length, expected: chunk_count, rows_received: totalRows,
      staged_apply: result.appliedNow || undefined,
    });
  }

  return res.json({ ok: true, request_id: requestId, sync_run_id, entity, chunk_index });
});

// row 列正規化 (mirror_yahoo_finance_sku_daily 用、Yahoo Phase 1 Y-3b)
function normalizeYahooFinanceRow(r) {
  return {
    date_jst: r.date_jst, yahoo_sku_key: r.yahoo_sku_key,
    ne_code: r.ne_code ?? null,
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    sales_postage_jpy_incl: r.sales_postage_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_before_point_jpy_incl: r.net_sales_before_point_jpy_incl ?? 0,
    listing_sales_estimated_jpy_incl: r.listing_sales_estimated_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    use_point_jpy_incl: r.use_point_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? 0,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'estimated_10pct',
    mall_fee_estimate_delta_jpy: r.mall_fee_estimate_delta_jpy ?? null,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_partial_jpy_incl: r.variable_margin_partial_jpy_incl ?? 0,
    variable_margin_full_jpy_incl: r.variable_margin_full_jpy_incl ?? null,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'partial',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    pay_charge_audit_jpy_incl: r.pay_charge_audit_jpy_incl ?? 0,
    ship_charge_audit_jpy_incl: r.ship_charge_audit_jpy_incl ?? 0,
    discount_audit_jpy_incl: r.discount_audit_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_aupay_finance_sku_daily 用、au PAY Phase 1 A-2)
function normalizeAupayFinanceRow(r) {
  return {
    date_jst: r.date_jst, aupay_sku_key: r.aupay_sku_key,
    ne_code: r.ne_code ?? null,
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    postage_allocated_jpy_incl: r.postage_allocated_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_after_coupon_jpy_incl: r.net_sales_after_coupon_jpy_incl ?? 0,
    request_price_jpy_incl: r.request_price_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    gift_point_jpy_incl: r.gift_point_jpy_incl ?? 0,
    use_ponta_point_jpy_incl: r.use_ponta_point_jpy_incl ?? 0,
    use_au_point_jpy_incl: r.use_au_point_jpy_incl ?? 0,
    premium_member_point_jpy_incl: r.premium_member_point_jpy_incl ?? 0,
    point_cost_pending_jpy_incl: r.point_cost_pending_jpy_incl ?? 0,
    tax_normal_sales_jpy_incl: r.tax_normal_sales_jpy_incl ?? 0,
    tax_reduced_sales_jpy_incl: r.tax_reduced_sales_jpy_incl ?? 0,
    tax_free_sales_jpy_incl: r.tax_free_sales_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? null,
    mall_fee_rate_applied: r.mall_fee_rate_applied ?? null,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'unknown',
    mall_fee_estimate_delta_jpy: r.mall_fee_estimate_delta_jpy ?? null,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_partial_jpy_incl: r.variable_margin_partial_jpy_incl ?? 0,
    variable_margin_full_jpy_incl: r.variable_margin_full_jpy_incl ?? null,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'partial',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    before_discount_jpy_incl: r.before_discount_jpy_incl ?? 0,
    detail_discount_jpy_incl: r.detail_discount_jpy_incl ?? 0,
    charge_allocated_jpy_incl: r.charge_allocated_jpy_incl ?? 0,
    item_option_jpy_incl: r.item_option_jpy_incl ?? 0,
    gift_wrapping_jpy_incl: r.gift_wrapping_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    order_count: r.order_count ?? 0,
    line_count: r.line_count ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_linegift_finance_sku_daily 用、LINEギフト Phase 1 A-3)
function normalizeLinegiftFinanceRow(r) {
  return {
    date_jst: r.date_jst, sku_code: r.sku_code,
    ne_code: r.ne_code ?? null,
    parent_item_code: r.parent_item_code ?? '',
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? 0,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'actual_api',
    mall_fee_estimate_delta_jpy: r.mall_fee_estimate_delta_jpy ?? null,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_jpy_incl: r.variable_margin_jpy_incl ?? 0,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'provisional_full_candidate',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    // LINEギフト 特有 audit
    recognized_on_jst: r.recognized_on_jst ?? null,
    bought_date_jst: r.bought_date_jst ?? null,
    delivered_lag_days: r.delivered_lag_days ?? null,
    received_lag_days: r.received_lag_days ?? null,
    is_delivery_by_hand: r.is_delivery_by_hand ?? null,
    delivery_agent: r.delivery_agent ?? null,
    // 90日境界 frozen horizon
    first_seen_in_api_at: r.first_seen_in_api_at ?? null,
    last_seen_in_api_at: r.last_seen_in_api_at ?? null,
    is_frozen_after_horizon: r.is_frozen_after_horizon ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    order_count: r.order_count ?? 0,
    line_count: r.line_count ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// LINEギフト v1.2 (PR-H): orders 集計用 subset normalize
// ★ PII 列 (user_name, address_*, delivery_*, sku_name, parent_item_name) は受け取らない
function normalizeLinegiftOrdersRow(r) {
  if (!r.order_id) throw new Error('linegift_orders: order_id is required');
  if (!r.bought_at_jst) throw new Error(`linegift_orders: bought_at_jst is required (order_id=${r.order_id})`);
  if (!r.bought_date_jst) throw new Error(`linegift_orders: bought_date_jst is required (order_id=${r.order_id})`);
  return {
    order_id: String(r.order_id),
    status: r.status ?? null,
    sku_code: r.sku_code ?? null,
    parent_item_code: r.parent_item_code ?? null,
    selling_price: r.selling_price ?? null,
    fee: r.fee ?? null,
    bought_at_jst: r.bought_at_jst,
    bought_date_jst: r.bought_date_jst,
    received_date_jst: r.received_date_jst ?? null,
    is_frozen_after_horizon: r.is_frozen_after_horizon ?? 0,
    source_run_id: r.source_run_id ?? null,
    synced_at: r.synced_at || new Date().toISOString(),
  };
}

// row 列正規化 (mirror_f_sales_by_listing 用、PR #156 hotfix で英語列統一)
// miniPC f_sales_by_listing は日本語列だが、sync runner 側で英語キーに mapping 済み payload を送る
// Codex R1 M2 反映: ?? + 必須列バリデーション
function normalizeFSalesByListingRow(r) {
  const date_jst = r.date_jst ?? r.date ?? r['日付'];
  const month_ym = r.month_ym ?? r.month ?? r['月'];
  const mall = r.mall ?? r['モール'];
  const item_code = r.item_code ?? r['モール商品コード'];
  // 必須列バリデーション: null/undefined のみ拒否 (空文字は許可、f_sales_by_listing には実例あり)
  if (date_jst == null) throw new Error('normalizeFSalesByListingRow: date_jst is required');
  if (month_ym == null) throw new Error('normalizeFSalesByListingRow: month_ym is required');
  if (mall == null) throw new Error('normalizeFSalesByListingRow: mall is required');
  if (item_code == null) throw new Error('normalizeFSalesByListingRow: item_code is required');
  return {
    date_jst, month_ym, mall, item_code,
    channel: r.channel ?? r['チャネル'] ?? '',
    item_name: r.item_name ?? r['商品名'] ?? null,
    units: r.units ?? r.qty ?? r['数量'] ?? 0,
    sales_jpy_incl: r.sales_jpy_incl ?? r.sales_jpy ?? r['売上金額'] ?? null,
    order_count: r.order_count ?? r['注文数'] ?? null,
    data_source: r.data_source ?? r['データソース'] ?? null,
    source_updated_at: r.source_updated_at ?? r.updated_at,
    source_run_id: r.source_run_id,
    source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_qoo10_finance_sku_daily 用、Qoo10 Phase 1 A-3)
function normalizeQoo10FinanceRow(r) {
  return {
    date_jst: r.date_jst, sku_code: r.sku_code,
    ne_code: r.ne_code ?? null,
    qoo10_item_id: r.qoo10_item_id ?? null,
    parent_item_code: r.parent_item_code ?? '',
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    match_tier: r.match_tier ?? 'unresolved',
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    gmv_list_price_jpy_incl: r.gmv_list_price_jpy_incl ?? 0,
    customer_paid_jpy_incl: r.customer_paid_jpy_incl ?? 0,
    net_settlement_api_jpy_incl: r.net_settlement_api_jpy_incl ?? 0,
    platform_fee_jpy_incl: r.platform_fee_jpy_incl ?? 0,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'actual_api',
    settle_price_formula_scope: r.settle_price_formula_scope ?? 'domestic_non_cod',
    extra_fee_oversea_jpy_incl: r.extra_fee_oversea_jpy_incl ?? 0,
    cod_fee_jpy_incl: r.cod_fee_jpy_incl ?? 0,
    megawari_order_count: r.megawari_order_count ?? 0,
    megawari_discount_amount_jpy_incl: r.megawari_discount_amount_jpy_incl ?? 0,
    megapo_order_count: r.megapo_order_count ?? 0,
    megapo_discount_amount_jpy_incl: r.megapo_discount_amount_jpy_incl ?? 0,
    other_promo_order_count: r.other_promo_order_count ?? 0,
    other_promo_discount_jpy_incl: r.other_promo_discount_jpy_incl ?? 0,
    total_platform_promo_jpy_incl: r.total_platform_promo_jpy_incl ?? 0,
    qoo10_cart_discount_jpy_incl: r.qoo10_cart_discount_jpy_incl ?? 0,
    seller_discount_api_jpy_incl: r.seller_discount_api_jpy_incl ?? 0,
    shop_promo_burden_jpy_incl: r.shop_promo_burden_jpy_incl ?? 0,
    shop_promo_burden_status: r.shop_promo_burden_status ?? 'pending_settlement_csv',
    domestic_non_cod_line_count: r.domestic_non_cod_line_count ?? 0,
    domestic_non_cod_formula_match_count: r.domestic_non_cod_formula_match_count ?? 0,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_jpy_incl: r.variable_margin_jpy_incl ?? 0,
    variable_margin_full_jpy_incl: r.variable_margin_full_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'partial_pending_settlement_csv',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    delivered_lag_days: r.delivered_lag_days ?? null,
    shipping_lag_days: r.shipping_lag_days ?? null,
    oversea_count: r.oversea_count ?? 0,
    payment_methods_json: r.payment_methods_json ?? null,
    first_seen_in_api_at: r.first_seen_in_api_at ?? null,
    last_seen_in_api_at: r.last_seen_in_api_at ?? null,
    is_frozen_after_horizon: r.is_frozen_after_horizon ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    order_count: r.order_count ?? 0,
    line_count: r.line_count ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_finance_sku_daily 用、Phase 1a #R-3b)
function normalizeRakutenFinanceRow(r) {
  return {
    date_jst: r.date_jst, rakuten_code: r.rakuten_code,
    ne_code: r.ne_code ?? null,
    sku_resolution: r.sku_resolution,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    // Phase 1b 按分関連 (旧 mirror payload なら DEFAULT 0 / 'no_refund')
    allocated_units_cancelled: r.allocated_units_cancelled ?? 0,
    units_cancelled_same_day_matched: r.units_cancelled_same_day_matched ?? 0,
    allocation_method: r.allocation_method ?? 'no_refund',
    cancel_exceeds_ordered_warning: r.cancel_exceeds_ordered_warning ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    sales_postage_jpy_incl: r.sales_postage_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    coupon_all_jpy_incl: r.coupon_all_jpy_incl ?? 0,
    promotion_jpy_incl: r.promotion_jpy_incl ?? 0,
    refund_amount_jpy_incl: r.refund_amount_jpy_incl ?? 0,
    allocated_refund_amount_jpy_incl: r.allocated_refund_amount_jpy_incl ?? 0,
    refund_amount_same_day_matched_jpy_incl: r.refund_amount_same_day_matched_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? 0,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_jpy_incl: r.net_sales_jpy_incl ?? 0,
    variable_margin_jpy_incl: r.variable_margin_jpy_incl ?? 0,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_amazon_finance_sku_daily 用)
function normalizeAmazonFinanceRow(r) {
  return {
    date_jst: r.date_jst, seller_sku: r.seller_sku, asin_norm: r.asin_norm || '',
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_refunded_customer: r.units_refunded_customer ?? 0,
    units_marketplace_guarantee: r.units_marketplace_guarantee ?? 0,
    units_a_to_z_refund: r.units_a_to_z_refund ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy: r.sales_principal_jpy ?? 0,
    sales_shipping_jpy: r.sales_shipping_jpy ?? 0,
    sales_giftwrap_jpy: r.sales_giftwrap_jpy ?? 0,
    sales_tax_jpy: r.sales_tax_jpy ?? 0,
    commission_jpy: r.commission_jpy ?? 0,
    fba_fulfillment_jpy: r.fba_fulfillment_jpy ?? 0,
    fba_storage_jpy: r.fba_storage_jpy ?? 0,
    closing_fee_jpy: r.closing_fee_jpy ?? 0,
    shipping_chargeback_jpy: r.shipping_chargeback_jpy ?? 0,
    giftwrap_chargeback_jpy: r.giftwrap_chargeback_jpy ?? 0,
    promotion_jpy: r.promotion_jpy ?? 0,
    warehouse_damage_jpy: r.warehouse_damage_jpy ?? 0,
    warehouse_lost_jpy: r.warehouse_lost_jpy ?? 0,
    safe_t_jpy: r.safe_t_jpy ?? 0,
    refund_principal_jpy: r.refund_principal_jpy ?? 0,
    reversal_reimbursement_jpy: r.reversal_reimbursement_jpy ?? 0,
    misc_fee_jpy: r.misc_fee_jpy ?? 0,
    other_fee_jpy: r.other_fee_jpy ?? 0,
    other_amount_jpy: r.other_amount_jpy ?? 0,
    unit_cost_snapshot: r.unit_cost_snapshot ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference: r.latest_unit_cost_reference ?? null,
    cogs_amount: r.cogs_amount ?? 0,
    profit_amount: r.profit_amount ?? 0,
    is_cost_complete: r.is_cost_complete ?? 0,
    cost_status: r.cost_status,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// 広告 entity 共通: 必須キーの欠落を 400 で拒否 (String(undefined)="undefined" の混入防止、Codex R1 Medium #4)
function requireAdKey(r, field) {
  const v = r[field];
  const s = v === null || v === undefined ? '' : String(v).trim();
  if (s === '') {
    throw new HttpError(400, { error: 'bad_row', message: `${field} is required (got ${JSON.stringify(v)})` });
  }
  return s;
}

// row 列正規化 (mirror_amazon_ads_sku_daily 用、amazon-dashboard PR-A)
// target は受信側でも trim + LOWER を保証 (送信元の正規化漏れによる PK 重複・按分漏れ防止)
function normalizeAmazonAdsSkuRow(r) {
  return {
    date_jst: r.date_jst, mall: (r.mall || 'amazon'),
    campaign_id: requireAdKey(r, 'campaign_id'), ad_type: r.ad_type || 'SP',
    target: requireAdKey(r, 'target').toLowerCase(),
    target_granularity: requireAdKey(r, 'target_granularity'),
    clicks: r.clicks ?? 0, impressions: r.impressions ?? 0,
    ad_cost: r.ad_cost ?? 0, ad_sales: r.ad_sales ?? 0, ad_units: r.ad_units ?? 0,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_amazon_ads_campaign_daily 用、amazon-dashboard PR-A)
function normalizeAmazonAdsCampaignRow(r) {
  return {
    date_jst: r.date_jst, mall: (r.mall || 'amazon'),
    campaign_id: requireAdKey(r, 'campaign_id'), campaign_name: r.campaign_name || '',
    ad_type: r.ad_type || 'SP', campaign_status: r.campaign_status || '',
    clicks: r.clicks ?? 0, impressions: r.impressions ?? 0, ad_cost: r.ad_cost ?? 0,
    ad_sales_1d: r.ad_sales_1d ?? 0, ad_sales_7d: r.ad_sales_7d ?? 0,
    ad_sales_14d: r.ad_sales_14d ?? 0, ad_sales_30d: r.ad_sales_30d ?? 0,
    ad_units_1d: r.ad_units_1d ?? 0,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_amazon_price_snapshot_daily 用、amazon-dashboard PR-D)
function normalizeAmazonPriceSnapshotRow(r) {
  const isMine = r.buybox_is_mine;
  return {
    date_jst: r.date_jst,
    seller_sku: requireAdKey(r, 'seller_sku'),
    asin: (r.asin === null || r.asin === undefined) ? '' : String(r.asin),
    channel: r.channel ?? null,
    my_price: r.my_price ?? null,
    buybox_price: r.buybox_price ?? null,
    buybox_is_mine: (isMine === 0 || isMine === 1) ? isMine : null,
    fetched_at: r.fetched_at ?? null,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_ads_rpp 用、mall-csv-fetcher P1)
// 月次 grain: date_jst=月初日 と month_ym の整合を受信側でも保証 (ズレると clear と実 row が食い違う)
function normalizeRakutenAdsRppRow(r) {
  const dateJst = requireAdKey(r, 'date_jst');
  const monthYm = requireAdKey(r, 'month_ym');
  if (!/^\d{4}-\d{2}-01$/.test(dateJst) || dateJst.slice(0, 7) !== monthYm) {
    throw new HttpError(400, { error: 'bad_row', message: `date_jst must be month start of month_ym (got date_jst=${dateJst}, month_ym=${monthYm})` });
  }
  return {
    date_jst: dateJst, month_ym: monthYm,
    // 送信元の正規化漏れによる PK 重複防止 (feedback_sku_case_normalization)
    item_manage_number: requireAdKey(r, 'item_manage_number').toLowerCase(),
    raw_sku_code: r.raw_sku_code || '',
    clicks: r.clicks ?? 0, ad_cost_yen: r.ad_cost_yen ?? 0,
    cpc_actual: r.cpc_actual ?? null, ctr_pct: r.ctr_pct ?? null,
    bid_cpc_yen: r.bid_cpc_yen ?? null, item_cpc_yen: r.item_cpc_yen ?? null,
    sales_720h_yen: r.sales_720h_yen ?? 0, orders_720h: r.orders_720h ?? 0,
    cvr_720h_pct: r.cvr_720h_pct ?? null, roas_720h_pct: r.roas_720h_pct ?? null,
    sales_12h_yen: r.sales_12h_yen ?? null, orders_12h: r.orders_12h ?? null,
    sales_720h_new_yen: r.sales_720h_new_yen ?? null, sales_720h_repeat_yen: r.sales_720h_repeat_yen ?? null,
    source_report_type: r.source_report_type || 'rpp_product_monthly',
    report_start: r.report_start ?? null, report_end: r.report_end ?? null,
    attribution_window_hours: r.attribution_window_hours ?? 720,
    is_tax_included: r.is_tax_included ?? 1,
    imported_at: r.imported_at ?? null,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_ads_rpp_daily 用、mall-csv-fetcher P1)
// campaign_id は「すべての広告」集計だと空文字が正 (requireAdKey は使わない)
function normalizeRakutenAdsRppDailyRow(r) {
  return {
    date_jst: r.date_jst,
    campaign_id: (r.campaign_id === null || r.campaign_id === undefined) ? '' : String(r.campaign_id).trim(),
    campaign_name: r.campaign_name || '',
    clicks: r.clicks ?? 0, ad_cost_yen: r.ad_cost_yen ?? 0,
    ad_cost_discounted_yen: r.ad_cost_discounted_yen ?? null,
    cpc_actual: r.cpc_actual ?? null, ctr_pct: r.ctr_pct ?? null,
    sales_720h_yen: r.sales_720h_yen ?? 0, orders_720h: r.orders_720h ?? 0,
    cvr_720h_pct: r.cvr_720h_pct ?? null, roas_720h_pct: r.roas_720h_pct ?? null,
    sales_12h_yen: r.sales_12h_yen ?? null, orders_12h: r.orders_12h ?? null,
    sales_720h_new_yen: r.sales_720h_new_yen ?? null, sales_720h_repeat_yen: r.sales_720h_repeat_yen ?? null,
    source_report_type: r.source_report_type || 'rpp_all_daily',
    attribution_window_hours: r.attribution_window_hours ?? 720,
    is_tax_included: r.is_tax_included ?? 1,
    imported_at: r.imported_at ?? null,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_item_daily 用、mall-csv-fetcher P1-R2)
function normalizeRakutenItemDailyRow(r) {
  const dateJst = requireAdKey(r, 'date_jst');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateJst)) {
    throw new HttpError(400, { error: 'bad_row', message: `bad date_jst: ${dateJst}` });
  }
  return {
    date_jst: dateJst,
    // 送信元の正規化漏れによる PK 重複防止 (feedback_sku_case_normalization)
    item_manage_number: requireAdKey(r, 'item_manage_number').toLowerCase(),
    raw_sku_code: r.raw_sku_code || '',
    sales_yen: r.sales_yen ?? 0, orders: r.orders ?? 0, units: r.units ?? 0,
    access_users: r.access_users ?? 0, unique_users: r.unique_users ?? null,
    cvr_pct: r.cvr_pct ?? null, aov_yen: r.aov_yen ?? null,
    buyers_total: r.buyers_total ?? null, buyers_new: r.buyers_new ?? null,
    buyers_repeat: r.buyers_repeat ?? null, nonbuyer_access: r.nonbuyer_access ?? null,
    review_posts: r.review_posts ?? null, review_avg: r.review_avg ?? null, review_total: r.review_total ?? null,
    stay_seconds: r.stay_seconds ?? null, bounce_count: r.bounce_count ?? null,
    exit_count: r.exit_count ?? null, exit_rate_pct: r.exit_rate_pct ?? null,
    favorites_added: r.favorites_added ?? null, favorites_total: r.favorites_total ?? null,
    stock_qty: r.stock_qty ?? null,
    item_name: r.item_name ?? null, genre_path: r.genre_path ?? null,
    item_id: r.item_id ?? null, catalog_id: r.catalog_id ?? null,
    item_number: r.item_number ?? null,
    is_tax_included: r.is_tax_included ?? 1,
    imported_at: r.imported_at ?? null,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_store_daily 用、mall-csv-fetcher P1-R2)
// ベンチマーク列 (bench_*) は RMS 側が非公開日は空 → null が正
function normalizeRakutenStoreDailyRow(r) {
  const dateJst = requireAdKey(r, 'date_jst');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateJst)) {
    throw new HttpError(400, { error: 'bad_row', message: `bad date_jst: ${dateJst}` });
  }
  return {
    date_jst: dateJst,
    sales_all_yen: r.sales_all_yen ?? 0, sales_pc_yen: r.sales_pc_yen ?? null,
    sales_app_yen: r.sales_app_yen ?? null, sales_sp_yen: r.sales_sp_yen ?? null,
    orders_all: r.orders_all ?? 0, orders_pc: r.orders_pc ?? null,
    orders_app: r.orders_app ?? null, orders_sp: r.orders_sp ?? null,
    access_all: r.access_all ?? 0, access_pc: r.access_pc ?? null,
    access_app: r.access_app ?? null, access_sp: r.access_sp ?? null,
    cvr_all_pct: r.cvr_all_pct ?? null, cvr_pc_pct: r.cvr_pc_pct ?? null,
    cvr_app_pct: r.cvr_app_pct ?? null, cvr_sp_pct: r.cvr_sp_pct ?? null,
    aov_all_yen: r.aov_all_yen ?? null, aov_pc_yen: r.aov_pc_yen ?? null,
    aov_app_yen: r.aov_app_yen ?? null, aov_sp_yen: r.aov_sp_yen ?? null,
    bench_top10_sales_yen: r.bench_top10_sales_yen ?? null, bench_top10_orders: r.bench_top10_orders ?? null,
    bench_top10_access: r.bench_top10_access ?? null, bench_top10_cvr_pct: r.bench_top10_cvr_pct ?? null,
    bench_top10_aov_yen: r.bench_top10_aov_yen ?? null,
    bench_class_label: r.bench_class_label ?? null,
    bench_class_sales_yen: r.bench_class_sales_yen ?? null, bench_class_orders: r.bench_class_orders ?? null,
    bench_class_access: r.bench_class_access ?? null, bench_class_cvr_pct: r.bench_class_cvr_pct ?? null,
    bench_class_aov_yen: r.bench_class_aov_yen ?? null,
    tax_out_yen: r.tax_out_yen ?? null, shipping_yen: r.shipping_yen ?? null,
    coupon_store_yen: r.coupon_store_yen ?? null, coupon_rakuten_yen: r.coupon_rakuten_yen ?? null,
    free_ship_coupon_yen: r.free_ship_coupon_yen ?? null, wrapping_yen: r.wrapping_yen ?? null,
    settlement_fee_yen: r.settlement_fee_yen ?? null,
    is_tax_included: r.is_tax_included ?? 1,
    imported_at: r.imported_at ?? null,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_amazon_account_fees_monthly 用、amazon-dashboard PR-C)
const ACCOUNT_FEE_TYPES = new Set(['storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'other_account_fee']);
function normalizeAmazonAccountFeesRow(r) {
  const feeType = requireAdKey(r, 'fee_type');
  if (!ACCOUNT_FEE_TYPES.has(feeType)) {
    throw new HttpError(400, { error: 'bad_row', message: `unknown fee_type: ${feeType}` });
  }
  return {
    date_jst: r.date_jst, fee_type: feeType,
    amount_jpy: r.amount_jpy ?? 0, row_count: r.row_count ?? 0,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// ─── MF Phase 1a: row 列正規化 (7 entity 分) ─────────────────────────
function normalizeMfPublishRunRow(r) {
  return {
    run_id: r.run_id, scope: r.scope, status: r.status,
    started_at: r.started_at, finished_at: r.finished_at ?? null,
    error_message: r.error_message ?? null,
    source_run_hash: r.source_run_hash ?? null, synced_at: r.synced_at,
    finalized_at: r.finalized_at ?? null,
  };
}
function normalizeMfExecutiveTopRow(r) {
  return {
    run_id: r.run_id, snapshot_date: r.snapshot_date, current_month_ym: r.current_month_ym,
    sales_mtd_excl_tax: r.sales_mtd_excl_tax ?? 0,
    gross_profit_mtd_excl_tax: r.gross_profit_mtd_excl_tax ?? 0,
    operating_income_mtd_excl_tax: r.operating_income_mtd_excl_tax ?? 0,
    sales_month_end_forecast: r.sales_month_end_forecast ?? 0,
    gross_profit_month_end_forecast: r.gross_profit_month_end_forecast ?? 0,
    operating_income_month_end_forecast: r.operating_income_month_end_forecast ?? 0,
    forecast_status: r.forecast_status || '会計確定待ち',
    yoy_sales_pct: r.yoy_sales_pct ?? null,
    yoy_gross_profit_pct: r.yoy_gross_profit_pct ?? null,
    yoy_operating_income_pct: r.yoy_operating_income_pct ?? null,
    cash_balance_total: r.cash_balance_total ?? 0,
    cash_balance_json: r.cash_balance_json || '[]',
    danger_signals_json: r.danger_signals_json || '[]',
    data_window_from: r.data_window_from ?? null,
    data_window_to: r.data_window_to ?? null,
    reliability_label: r.reliability_label || '業務速報',
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfPlMonthlyRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym, role_key: r.role_key,
    amount_excl_tax: r.amount_excl_tax ?? 0, tax_amount: r.tax_amount ?? 0,
    line_count: r.line_count ?? 0, is_realized_only: r.is_realized_only ?? 1,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfChannelSalesRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    channel_key: r.channel_key, channel_display_name: r.channel_display_name,
    gross_sales_excl_tax: r.gross_sales_excl_tax ?? 0,
    pf_fee_excl_tax: r.pf_fee_excl_tax ?? 0,
    ad_cost_excl_tax: r.ad_cost_excl_tax ?? 0,
    fba_fee_excl_tax: r.fba_fee_excl_tax ?? 0,
    net_sales_after_pf_excl_tax: r.net_sales_after_pf_excl_tax ?? 0,
    unmapped_amount_excl_tax: r.unmapped_amount_excl_tax ?? 0,
    mapping_coverage_pct: r.mapping_coverage_pct ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfCashEventsDailyRow(r) {
  return {
    run_id: r.run_id, movement_date: r.movement_date,
    bank_account_key: r.bank_account_key, direction: r.direction,
    amount_excl_tax: r.amount_excl_tax ?? 0, event_count: r.event_count ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfBalanceSnapshotRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    account_key: r.account_key, account_name: r.account_name,
    sub_account_name: r.sub_account_name ?? null, role_key: r.role_key ?? null,
    closing_balance_excl_tax: r.closing_balance_excl_tax ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfAnomalySignalsRow(r) {
  return {
    run_id: r.run_id, signal_id: r.signal_id, detected_at: r.detected_at,
    signal_code: r.signal_code, signal_key: r.signal_key ?? null,
    severity: r.severity, severity_rank: r.severity_rank ?? null,
    title: r.title, description: r.description,
    observed_value: r.observed_value ?? null,
    threshold_value: r.threshold_value ?? null,
    related_entity_key: r.related_entity_key ?? null,
    recommended_action: r.recommended_action ?? null,
    source_mart: r.source_mart ?? null,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-2: mf_fy_summary
function normalizeMfFySummaryRow(r) {
  return {
    run_id: r.run_id,
    fy_number: r.fy_number,
    fy_start_ym: r.fy_start_ym,
    fy_end_ym: r.fy_end_ym,
    cumulative_through_ym: r.cumulative_through_ym,
    months_in_cumulative: r.months_in_cumulative ?? 0,
    is_fy_completed: r.is_fy_completed ?? 0,
    sales_cum: r.sales_cum ?? 0,
    cogs_cum: r.cogs_cum ?? 0,
    gross_profit_cum: r.gross_profit_cum ?? 0,
    sgae_cum: r.sgae_cum ?? 0,
    operating_income_cum: r.operating_income_cum ?? 0,
    non_op_revenue_cum: r.non_op_revenue_cum ?? 0,
    non_op_expense_cum: r.non_op_expense_cum ?? 0,
    ordinary_income_cum: r.ordinary_income_cum ?? 0,
    personnel_cost_cum: r.personnel_cost_cum ?? 0,
    ar_average: r.ar_average ?? 0,
    inventory_average: r.inventory_average ?? 0,
    ap_average: r.ap_average ?? 0,
    total_asset_average: r.total_asset_average ?? 0,
    ar_opening: r.ar_opening ?? 0,
    ar_closing: r.ar_closing ?? 0,
    inventory_opening: r.inventory_opening ?? 0,
    inventory_closing: r.inventory_closing ?? 0,
    ap_opening: r.ap_opening ?? 0,
    ap_closing: r.ap_closing ?? 0,
    total_asset_opening: r.total_asset_opening ?? 0,
    total_asset_closing: r.total_asset_closing ?? 0,
    cash_closing: r.cash_closing ?? 0,
    short_loan_closing: r.short_loan_closing ?? 0,
    long_loan_closing: r.long_loan_closing ?? 0,
    current_liab_closing: r.current_liab_closing ?? 0,
    total_equity_closing: r.total_equity_closing ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-3b: mf_bs_monthly
function normalizeMfBsMonthlyRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    cash_total: r.cash_total ?? 0,
    ar_total: r.ar_total ?? 0,
    inventory_total: r.inventory_total ?? 0,
    other_current_asset: r.other_current_asset ?? 0,
    current_asset_total: r.current_asset_total ?? 0,
    tangible_fixed_asset: r.tangible_fixed_asset ?? 0,
    investment_other: r.investment_other ?? 0,
    fixed_asset_total: r.fixed_asset_total ?? 0,
    total_asset: r.total_asset ?? 0,
    ap_total: r.ap_total ?? 0,
    short_loan_total: r.short_loan_total ?? 0,
    other_current_liab: r.other_current_liab ?? 0,
    current_liab_total: r.current_liab_total ?? 0,
    long_loan_total: r.long_loan_total ?? 0,
    other_fixed_liab: r.other_fixed_liab ?? 0,
    fixed_liab_total: r.fixed_liab_total ?? 0,
    total_liab: r.total_liab ?? 0,
    capital: r.capital ?? 0,
    retained: r.retained ?? 0,
    total_equity: r.total_equity ?? 0,
    bs_other_total: r.bs_other_total ?? 0,
    retained_balance: r.retained_balance ?? 0,
    current_period_profit: r.current_period_profit ?? 0,
    display_total_equity: r.display_total_equity ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-3b: mf_bs_subaccount_monthly
function normalizeMfBsSubaccountRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    account_name: r.account_name,
    sub_account_name: r.sub_account_name ?? '',
    role_key: r.role_key ?? null,
    section: r.section,
    closing_balance_excl_tax: r.closing_balance_excl_tax ?? 0,
    is_hub_null_sub: r.is_hub_null_sub ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}

// ─── Phase 1 #1-4a: GET /api/sync/runs/:run_id (status 確認) ───
router.get('/api/sync/runs/:run_id', requireSyncKey, (req, res) => {
  const db = getMirrorDB();
  const chunks = db.prepare(`
    SELECT entity, chunk_index, chunk_count, row_count, payload_checksum,
           received_at, applied_at
    FROM sync_run_chunks WHERE run_id = ? ORDER BY entity, chunk_index
  `).all(req.params.run_id);
  if (chunks.length === 0) return res.status(404).json({ error: 'run not found' });
  const summary = {
    run_id: req.params.run_id,
    chunk_count_expected: chunks[0].chunk_count,
    chunk_count_received: chunks.length,
    row_count_received: chunks.reduce((s, c) => s + c.row_count, 0),
    all_applied: chunks.every(c => c.applied_at !== null),
  };
  res.json({ summary, chunks });
});

// ─── Phase 1 #1-4a: POST /api/sync/runs/:run_id/backout (run_id 単位 DELETE) ───
//
// 安全性ガード (Codex Round 11 指摘 #1): INSERT OR REPLACE で旧 run の値が
// 上書きされている可能性があるため、scope-overlap する後続 run があれば
// 409 で refuse する。?force=1 で明示 override 可能 (運用判断)。
router.post('/api/sync/runs/:run_id/backout', requireSyncKey, (req, res) => {
  const runId = req.params.run_id;
  const force = req.query.force === '1' || req.body?.force === true;
  const db = getMirrorDB();

  // 対象 run の chunk 集約 (entity × scope)
  const targetChunks = db.prepare(`
    SELECT DISTINCT entity, scope_from, scope_to, MAX(received_at) AS max_received_at
    FROM sync_run_chunks WHERE run_id = ?
    GROUP BY entity, scope_from, scope_to
  `).all(runId);
  if (targetChunks.length === 0) return res.status(404).json({ error: 'run not found' });

  // 各 entity-scope について、scope-overlap する後続 run を探す
  const conflicts = [];
  for (const t of targetChunks) {
    const overlap = db.prepare(`
      SELECT DISTINCT run_id, scope_from, scope_to, MAX(received_at) AS max_received_at
      FROM sync_run_chunks
      WHERE entity = ?
        AND run_id <> ?
        AND scope_from <= ?
        AND scope_to   >= ?
        AND received_at > ?
      GROUP BY run_id, scope_from, scope_to
      ORDER BY max_received_at
    `).all(t.entity, runId, t.scope_to, t.scope_from, t.max_received_at);
    for (const o of overlap) conflicts.push({ entity: t.entity, target_scope: [t.scope_from, t.scope_to], conflicting: o });
  }

  if (conflicts.length > 0 && !force) {
    return res.status(409).json({
      error: 'backout_unsafe_overlap',
      message: 'scope-overlap する後続 run があるため backout で旧 run の上書き値を消失させる可能性があります。?force=1 で override 可能',
      run_id: runId,
      conflicts,
    });
  }

  const entities = [...new Set(targetChunks.map(t => t.entity))];
  // full_snapshot entity は backout 非対応 (Codex R1 Medium #7)。
  // この run の行を消しても直前の snapshot は戻らず、表が空になるだけで「取り消し」にならない。
  // ledger だけ消えて実データが残る中途半端な状態を作らないよう、ここで断る。
  // 戻したい時は正しい map を miniPC 側から再送する (世代が上がるので順序も守られる)
  const fullSnapshotEntities = entities.filter((e) => ENTITY_REGISTRY[e]?.clear_strategy === 'full_snapshot');
  if (fullSnapshotEntities.length > 0) {
    return res.status(409).json({
      error: 'backout_not_supported_for_full_snapshot',
      message: `full_snapshot entity (${fullSnapshotEntities.join(', ')}) は backout できません。直前の snapshot は保存していないため、正しい内容を送信元から再送してください`,
      run_id: runId, entities: fullSnapshotEntities,
    });
  }
  const deleted = {};
  const tx = db.transaction(() => {
    for (const entity of entities) {
      const cfg = ENTITY_REGISTRY[entity];
      if (cfg) {
        // MF entity (clear_strategy='no_clear') は source_run_id 列を持たないので skip
        // backout は MF 専用 endpoint (Phase 2 で追加予定) で対応
        if (cfg.clear_strategy === 'no_clear') {
          deleted[entity] = 0;
          continue;
        }
        const info = db.prepare(`
          DELETE FROM ${cfg.mirror_table} WHERE source_run_id = ?
        `).run(runId);
        deleted[entity] = info.changes;
      }
    }
    db.prepare(`DELETE FROM sync_run_chunks WHERE run_id = ?`).run(runId);
  });
  tx();

  console.log(`[Mirror] backout run=${runId} deleted=${JSON.stringify(deleted)} force=${force} conflicts=${conflicts.length}`);
  res.json({ ok: true, run_id: runId, deleted, force, conflicts_overridden: conflicts.length });
});

// ─── Phase 1 #1-7: POST /api/sync/runs/:run_id/rebuild-marts (downstream mart rebuild trigger) ───
//
// 現状 noop: ledger 確認 + ログだけ (Phase 2 で mart_amazon_sku_* を実装する際に中身を実装)。
// 完了判定は Render 側の sync_run_chunks のみで行う (sync_runs テーブルは miniPC 側のみ
// で Render mirror には存在しない、Codex Round 1 #medium-1 対応):
//   - run_id 存在
//   - chunk 数 = chunk_count (received_count 一致)
//   - chunk_index が 0..chunk_count-1 連続 (欠番なし、Codex Round 1 #medium-2 対応)
//   - 全 chunk の applied_at が NOT NULL
// 将来 Phase 2 で mart 実装時、ここで:
//   - 該当 entity の mart を rebuild (例: mart_amazon_sku_daily, mart_amazon_sku_monthly)
//   - rebuild 結果を sync_run に紐付けて記録
// 失敗しても呼び出し元の sync 結果には影響させない (warn のみ) 設計の明示。
router.post('/api/sync/runs/:run_id/rebuild-marts', requireSyncKey, (req, res) => {
  const runId = req.params.run_id;
  const db = getMirrorDB();

  // ledger 確認 (run_id 存在 + 全 chunk applied + chunk_index 連続)
  const chunks = db.prepare(`
    SELECT entity, chunk_index, chunk_count, applied_at
    FROM sync_run_chunks WHERE run_id = ? ORDER BY entity, chunk_index
  `).all(runId);
  if (chunks.length === 0) {
    return res.status(404).json({ error: 'run not found', run_id: runId });
  }
  const entitiesByName = {};
  for (const c of chunks) {
    if (!entitiesByName[c.entity]) entitiesByName[c.entity] = { chunks: [], expectedCount: c.chunk_count };
    entitiesByName[c.entity].chunks.push(c);
  }
  const incomplete = [];
  for (const [entity, info] of Object.entries(entitiesByName)) {
    const allApplied = info.chunks.every(c => c.applied_at !== null);
    const presentSet = new Set(info.chunks.map(c => c.chunk_index));
    const missingIndexes = [];
    for (let i = 0; i < info.expectedCount; i++) {
      if (!presentSet.has(i)) missingIndexes.push(i);
    }
    if (!allApplied || info.chunks.length !== info.expectedCount || missingIndexes.length > 0) {
      incomplete.push({
        entity,
        received: info.chunks.length,
        expected: info.expectedCount,
        missing_chunks: missingIndexes,
        all_applied: allApplied,
      });
    }
  }
  if (incomplete.length > 0) {
    return res.status(409).json({
      error: 'rebuild_blocked_incomplete_sync',
      message: 'sync が未完了の entity が含まれているため rebuild を保留',
      run_id: runId,
      incomplete,
    });
  }

  // 現状 noop (Phase 2 で実装、Phase 1 では trigger 経路の確保のみ)
  const triggered = Object.keys(entitiesByName);
  console.log(`[Mirror] rebuild-marts triggered (noop) run=${runId} entities=${triggered.join(',')}`);
  res.json({
    ok: true,
    run_id: runId,
    triggered_entities: triggered,
    rebuilt: [],
    note: 'Phase 1 #1-7: rebuild trigger 経路のみ整備、mart 実装は Phase 2 で追加',
  });
});

// ─── 監査PR-4後続: GET /api/sync/mf/runs/:run_id/status ─────────────────
// sender (sync-mf-marts-to-render.js) が送信前に「mirror が既に success か」を照会し、
// 確定済 run の再送 (子 chunk が parent_run_not_pending 409 で弾かれる) を送信前に
// スキップするための read-only 照会。requireSyncKey (sync 系と同じ鍵、server.js 側でも二重防御)。
router.get('/api/sync/mf/runs/:run_id/status', requireSyncKey, (req, res) => {
  const runId = parseInt(req.params.run_id, 10);
  if (!Number.isInteger(runId) || runId <= 0) {
    return res.status(400).json({ error: 'run_id must be positive integer' });
  }
  const db = getMirrorDB();
  const row = db.prepare(`
    SELECT run_id, status, source_run_hash, finalized_at, synced_at
    FROM mirror_mf_publish_runs WHERE run_id = ?
  `).get(runId);
  if (!row) return res.status(404).json({ error: 'run_not_found', run_id: runId });
  res.json(row);
});

// ─── MF Phase 1a: POST /api/sync/mf/runs/:run_id/finalize ─────────────────
// 全 entity の chunk 受信完了を検証し、mirror_mf_publish_runs.status を
// 'pending_sync' → 'success' に flip。VIEW v_mirror_mf_*_latest がこの瞬間に活性化。
//
// body: { entity_run_ids: { mf_publish_runs: 'sync_run_id...', mf_executive_top: '...', ... } }
// idempotent: status='success' 既にだったら 200 で no-op
const MF_REQUIRED_ENTITIES = [
  'mf_publish_runs', 'mf_executive_top', 'mf_pl_monthly', 'mf_channel_sales',
  'mf_cash_events_daily', 'mf_balance_snapshot_monthly', 'mf_anomaly_signals',
];
// Phase 1d-2: optional entity (旧 sync (deploy 前) との互換のため、欠けても finalize 通過)
//   将来 (Phase 1d-3 以降で sync 側完全移行後) に MF_REQUIRED_ENTITIES へ昇格予定
const MF_OPTIONAL_ENTITIES = ['mf_fy_summary', 'mf_bs_monthly', 'mf_bs_subaccount_monthly'];
router.post('/api/sync/mf/runs/:run_id/finalize', requireSyncKey, (req, res) => {
  const runId = parseInt(req.params.run_id, 10);
  if (!Number.isInteger(runId) || runId <= 0) {
    return res.status(400).json({ error: 'run_id must be positive integer' });
  }
  const body = req.body || {};
  const entityRunIds = body.entity_run_ids || {};
  const requestId = `mf-finalize-${Date.now().toString(36)}`;
  const db = getMirrorDB();

  const parent = db.prepare(`SELECT run_id, status, scope FROM mirror_mf_publish_runs WHERE run_id = ?`).get(runId);
  if (!parent) {
    return res.status(404).json({ error: 'parent_run_not_found', run_id: runId, request_id: requestId });
  }
  if (parent.status === 'success') {
    console.log(`[Mirror] mf finalize idempotent (already success) req=${requestId} run=${runId}`);
    return res.json({ ok: true, run_id: runId, status: 'success', already_finalized: true, request_id: requestId });
  }
  if (parent.status !== 'pending_sync') {
    return res.status(409).json({
      error: 'invalid_status_for_finalize',
      message: `run_id=${runId} status=${parent.status}, finalize is only valid from 'pending_sync'`,
      request_id: requestId,
    });
  }

  // 検証 + status flip を tx 内で atomic に (Codex review #88: race 防止)
  const incomplete = [];
  let updatedChanges = 0;
  let alreadyFinalized = false;
  const now = new Date().toISOString();

  try {
    db.transaction(() => {
      // tx 内で再 status check (BEGIN IMMEDIATE で書き込み直列化、他の chunk INSERT が完了済を保証)
      const parent2 = db.prepare(`SELECT run_id, status FROM mirror_mf_publish_runs WHERE run_id = ?`).get(runId);
      if (!parent2) {
        throw new HttpError(404, { error: 'parent_run_not_found', run_id: runId });
      }
      if (parent2.status === 'success') {
        alreadyFinalized = true;
        return;
      }
      if (parent2.status !== 'pending_sync') {
        throw new HttpError(409, {
          error: 'invalid_status_for_finalize',
          message: `run_id=${runId} status=${parent2.status}`,
        });
      }

      // Phase 1d-2: required + optional 両方 validate (optional は欠けても通過)
      const allEntitiesToCheck = [
        ...MF_REQUIRED_ENTITIES.map(e => ({ entity: e, optional: false })),
        ...MF_OPTIONAL_ENTITIES.map(e => ({ entity: e, optional: true })),
      ];
      for (const { entity, optional } of allEntitiesToCheck) {
        const syncRunId = entityRunIds[entity];
        if (!syncRunId || typeof syncRunId !== 'string') {
          if (optional) {
            console.log(`[Mirror] mf finalize req=${requestId} run=${runId}: optional entity '${entity}' not provided, skipping`);
            continue;
          }
          incomplete.push({ entity, reason: 'missing_entity_run_id_in_request_body' });
          continue;
        }
        const chunks = db.prepare(`
          SELECT chunk_index, chunk_count, applied_at, mf_source_run_id
          FROM sync_run_chunks WHERE run_id = ? AND entity = ?
          ORDER BY chunk_index
        `).all(syncRunId, entity);
        if (chunks.length === 0) {
          if (optional) {
            console.log(`[Mirror] mf finalize req=${requestId} run=${runId}: optional entity '${entity}' has no chunks, skipping`);
            continue;
          }
          incomplete.push({ entity, sync_run_id: syncRunId, reason: 'no_chunks_received' });
          continue;
        }
        // Codex Medium fix: chunks の mf_source_run_id が finalize の run_id と一致するか cross-check
        const mismatched = chunks.find(c => c.mf_source_run_id !== runId);
        if (mismatched) {
          incomplete.push({
            entity, sync_run_id: syncRunId,
            reason: 'mf_source_run_id_mismatch',
            expected: runId, got: mismatched.mf_source_run_id,
          });
          continue;
        }
        const expected = chunks[0].chunk_count;
        const applied = chunks.every(c => c.applied_at !== null);
        const indexes = chunks.map(c => c.chunk_index);
        const missing = [];
        for (let i = 0; i < expected; i++) if (!indexes.includes(i)) missing.push(i);
        if (chunks.length !== expected || missing.length > 0 || !applied) {
          incomplete.push({
            entity, sync_run_id: syncRunId,
            received: chunks.length, expected,
            missing_chunks: missing, all_applied: applied,
          });
        }
      }
      if (incomplete.length > 0) {
        throw new HttpError(409, {
          error: 'finalize_blocked_incomplete',
          message: '一部 entity の sync が未完了 / cross-check 失敗のため finalize を拒否',
          run_id: runId, incomplete,
        });
      }

      // 全検証 PASS、status を atomic flip
      updatedChanges = db.prepare(`
        UPDATE mirror_mf_publish_runs
           SET status = 'success', finalized_at = ?
         WHERE run_id = ? AND status = 'pending_sync'
      `).run(now, runId).changes;
    }).immediate();
  } catch (e) {
    if (e instanceof HttpError) {
      return res.status(e.status).json({ ...e.body, request_id: requestId });
    }
    console.error(`[Mirror] mf finalize error req=${requestId}: ${e.message}`);
    return res.status(500).json({ error: e.message, request_id: requestId });
  }

  if (alreadyFinalized) {
    console.log(`[Mirror] mf finalize idempotent (already success) req=${requestId} run=${runId}`);
    return res.json({ ok: true, run_id: runId, status: 'success', already_finalized: true, request_id: requestId });
  }
  if (updatedChanges !== 1) {
    return res.status(500).json({
      error: 'finalize_update_failed',
      run_id: runId, request_id: requestId,
    });
  }

  console.log(`[Mirror] mf finalize success req=${requestId} run=${runId} scope=${parent.scope}`);
  res.json({ ok: true, run_id: runId, status: 'success', finalized_at: now, request_id: requestId });
});

// ─── GET /api/products ───

router.get('/api/products', (req, res) => {
  const db = getMirrorDB();
  const { search, status, type, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM mirror_products WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (商品コード LIKE ? OR 商品名 LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  if (status) { sql += ' AND 取扱区分 = ?'; params.push(status); }
  if (type) { sql += ' AND 商品区分 = ?'; params.push(type); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = db.prepare(countSql).get(...params)?.cnt || 0;
  sql += ' ORDER BY 商品コード LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: db.prepare(sql).all(...params), total });
});

// ─── GET /api/sales/monthly ───

router.get('/api/sales/monthly', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, months = '6' } = req.query;
  let sql = 'SELECT * FROM mirror_sales_monthly WHERE 1=1';
  const params = [];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 月 DESC, 数量 DESC LIMIT 1000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/sales/daily ───

router.get('/api/sales/daily', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, days = '30' } = req.query;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - parseInt(days));
  let sql = 'SELECT * FROM mirror_sales_daily WHERE 日付 >= ?';
  const params = [cutoff.toISOString().slice(0, 10)];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 日付 DESC, 数量 DESC LIMIT 5000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/status ───

router.get('/api/status', (req, res) => {
  const db = getMirrorDB();
  const status = {};
  try {
    for (const r of db.prepare('SELECT key, value, updated_at FROM mirror_sync_status').all()) {
      status[r.key] = { value: r.value, updated_at: r.updated_at };
    }
    status.products_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_products').get().cnt;
    status.sales_monthly_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_monthly').get().cnt;
    status.sales_daily_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_daily').get().cnt;
    try { status.amazon_sku_fees_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees').get().cnt; } catch { status.amazon_sku_fees_count = 0; }
    try { status.rakuten_sku_map_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_rakuten_sku_map').get().cnt; } catch { status.rakuten_sku_map_count = 0; }
    // Codex PR2a Round 4 非ブロッカー #3 反映: stock_snapshot 件数も同期検証に乗せる
    try { status.stock_snapshot_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get().cnt; } catch { status.stock_snapshot_count = 0; }
    // 商品管理リスト snapshot (⑤): published run の行数 + run_id を検証用に公開
    try {
      const pub = db.prepare('SELECT run_id, status, payload_checksum FROM mirror_pml_published WHERE id=1').get();
      status.pml_published_run_id = pub?.run_id ?? null;
      status.pml_published_status = pub?.status ?? null;
      status.pml_snapshot_count = pub ? db.prepare('SELECT COUNT(*) as cnt FROM mirror_pml_snapshot_rows WHERE run_id=?').get(pub.run_id).cnt : 0;
    } catch { status.pml_published_run_id = null; status.pml_snapshot_count = 0; }
    try {
      const r = db.prepare(`SELECT
        COUNT(*) AS cnt,
        SUM(CASE WHEN source='master' THEN 1 ELSE 0 END) AS master_cnt,
        SUM(CASE WHEN source='auto'   THEN 1 ELSE 0 END) AS auto_cnt,
        MAX(synced_at) AS synced
        FROM mirror_sku_resolved`).get();
      status.sku_resolved_count = r.cnt;
      status.sku_resolved_master_count = r.master_cnt ?? 0;
      status.sku_resolved_auto_count = r.auto_cnt ?? 0;
      status.sku_resolved_synced_at = r.synced ?? null;
    } catch {
      status.sku_resolved_count = 0;
    }

    // inv_daily_summary (PR-C 日次在庫スナップショット)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MAX(business_date) AS latest_date
        FROM mirror_inv_daily_summary
      `).get();
      status.inv_daily_summary_count = r.cnt;
      status.inv_daily_summary_latest_date = r.latest_date;
    } catch {
      status.inv_daily_summary_count = 0;
    }
    // shipments_daily (日次出荷サマリ)。届いたか・いつの分まで来ているかを同期直後に確認できるようにする
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MIN(ship_date) AS oldest, MAX(ship_date) AS latest,
               SUM(slips) AS slips, MAX(synced_at) AS synced_at
        FROM mirror_shipments_daily
      `).get();
      status.shipments_daily_count = r.cnt;
      status.shipments_daily_oldest_date = r.oldest;
      status.shipments_daily_latest_date = r.latest;
      status.shipments_daily_slips = r.slips;
      status.shipments_daily_synced_at = r.synced_at;
    } catch {
      status.shipments_daily_count = 0;
    }
    // logizard_stock (ロジザード在庫スナップショット・毎時)。--logizard-only の送信後検証が読む
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MAX(captured_at) AS captured_at, MAX(synced_at) AS synced_at
        FROM mirror_logizard_stock
      `).get();
      status.logizard_stock_count = r.cnt;
      status.logizard_stock_captured_at = r.captured_at;
      status.logizard_stock_synced_at = r.synced_at;
    } catch {
      status.logizard_stock_count = 0;
    }
    // inv_daily_detail (D-1c 詳細層)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MIN(business_date) AS oldest, MAX(business_date) AS latest
        FROM mirror_inv_daily_detail
      `).get();
      status.inv_daily_detail_count = r.cnt;
      status.inv_daily_detail_oldest_date = r.oldest;
      status.inv_daily_detail_latest_date = r.latest;
    } catch {
      status.inv_daily_detail_count = 0;
    }
    // mirror_sku_master (商品コード変換テーブル差分検出用)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MAX(source_updated_at) AS latest, MAX(synced_at) AS synced
        FROM mirror_sku_master
      `).get();
      status.sku_master_count = r.cnt;
      status.sku_master_latest_updated_at = r.latest;
      status.sku_master_synced_at = r.synced;
    } catch {
      status.sku_master_count = 0;
    }
  } catch {}
  res.json(status);
});

// ─── GET /api/sku-master/recent-missing-candidates ───
// マスタ登録ツール (m_sku_master) に登録済みで、Google Sheets「商品コード変換テーブル」
// にまだ載っていない SKU を、Sheets 側 GAS から日次でチェックするための専用 endpoint。
//
// 認証: x-read-token (MIRROR_READ_TOKEN 環境変数。Render env にのみ置く read-only token)
//   ・WAREHOUSE_API_KEY (miniPC の master write 権限キー) とは完全分離
//   ・キー未設定なら 503 (fail-closed)
//
// レスポンス:
//   {
//     since_days: 7,
//     server_time_utc: 'YYYY-MM-DDTHH:MM:SS.fffZ',
//     mirror_last_synced_at: 'YYYY-MM-DD HH:MM:SS' | null,    // ★SKUペアの有効鮮度 (pair一致時=masterSyncedAt / 不一致時=null)
//     mirror_global_last_synced_at: 'YYYY-MM-DD HH:MM:SS' | null, // mirror_sync_status.last_sync (観測用)
//     mirror_sku_master_synced_at: 'YYYY-MM-DD HH:MM:SS' | null, // mirror_sku_master の最新 synced_at
//     mirror_sku_resolved_synced_at: 'YYYY-MM-DD HH:MM:SS' | null, // mirror_sku_resolved の最新 synced_at
//     pair_consistent: <boolean>,                            // master/components の synced_at 一致 (不一致なら items 空)
//     count: <number>,
//     items: [ { seller_sku, 商品名, source_updated_at: 'ISO 8601 Z', components: [{ne_code, quantity, sort_order}] }, ... ]
//   }
//
// 仕様メモ (Codex review 2026-05-13 + 2026-06-07 PR1 反映):
//   ・since_days は固定 7、パラメータ受け付けない (誤用防止)
//   ・GAS 側はレスポンスの mirror_last_synced_at が当日かを必ず検証 (古ければ処理停止)。
//     この endpoint の mirror_last_synced_at は global last_sync ではなく SKUペアの有効鮮度を返すので、
//     マスタペアがスキップ/不整合の同期では古い値/null になり、GAS の既存 staleness チェックが自動停止する。
//   ・Cache-Control: no-store
//   ・件数 0 でも 200 で返す (差分判定は呼び出し側)
function requireReadToken(req, res, next) {
  const token = process.env.MIRROR_READ_TOKEN;
  if (!token) {
    return res.status(503).json({ error: 'mirror_read_token_unset' });
  }
  // header only (Codex review 2026-05-13 medium #3): query fallback は URL / アクセスログ /
  // 監視 / 例外メッセージ / ブラウザ履歴に token を残すため受け付けない。
  const provided = req.headers['x-read-token'];
  if (!provided || provided !== token) {
    return res.status(401).json({ error: 'invalid_read_token' });
  }
  next();
}

// 商品管理リスト専用の read-only トークン (既存 MIRROR_READ_TOKEN とは分離=権限分離)。
// Render env PML_READ_TOKEN にのみ置く。未設定なら 503 (fail-closed)、header only。
function requirePmlReadToken(req, res, next) {
  const token = process.env.PML_READ_TOKEN;
  if (!token) {
    return res.status(503).json({ error: 'pml_read_token_unset' });
  }
  const provided = req.headers['x-read-token'];
  if (!provided || provided !== token) {
    return res.status(401).json({ error: 'invalid_read_token' });
  }
  next();
}

// 商品管理リスト オンデマンドFBA更新の「起動(書き込み系アクション)」専用トークン。
// 読み取り専用の PML_READ_TOKEN とは権限分離する (read token 所持者が更新を起動できないように)。
// Render env PML_REFRESH_TOKEN にのみ置く。未設定なら 503 (fail-closed)、header(x-refresh-token) only。
function requirePmlRefreshToken(req, res, next) {
  const token = process.env.PML_REFRESH_TOKEN;
  if (!token) return res.status(503).json({ error: 'pml_refresh_token_unset' });
  const provided = req.headers['x-refresh-token'];
  if (!provided || provided !== token) return res.status(401).json({ error: 'invalid_refresh_token' });
  next();
}

// ─── オンデマンドFBA更新 (Part2) GAS用トリガ ───
// GAS はセッションを持てないため、専用 PML_REFRESH_TOKEN (x-refresh-token) で認証して miniPC にプロキシする。
// (ブラウザ管理UIは /apps/product-management-list 側の session-gated proxy を使う)
const WH_URL_PML = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function whServiceHeadersPml() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}
async function callWhPml(fullPath, { method = 'GET', timeout = 30000 } = {}) {
  const r = await fetch(`${WH_URL_PML}${fullPath}`, {
    method, headers: whServiceHeadersPml(), redirect: 'manual', signal: AbortSignal.timeout(timeout),
  });
  if (!r.ok) { const t = await r.text().catch(() => ''); throw new Error(`warehouse HTTP ${r.status}: ${t.slice(0, 200)}`); }
  const ct = r.headers.get('content-type') || '';
  if (!ct.includes('application/json')) throw new Error(`warehouse 応答形式異常 (ct=${ct || 'none'})`);
  return r.json();
}
router.post('/api/pml/refresh-fba', requirePmlRefreshToken, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  try { res.json(await callWhPml('/service-api/fba/pml/fba-refresh', { method: 'POST', timeout: 30000 })); }
  catch (e) { res.status(502).json({ error: 'ミニPC接続失敗: ' + e.message }); }
});
router.get('/api/pml/refresh-fba/jobs/:jobId', requirePmlRefreshToken, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  try { res.json(await callWhPml(`/service-api/jobs/${encodeURIComponent(req.params.jobId)}`, { timeout: 15000 })); }
  catch (e) { res.status(502).json({ error: 'ジョブ状態取得失敗: ' + e.message }); }
});

// ─── GET /api/pml/published ───
// 商品管理リスト snapshot の published run を GAS 向けに返す read-only endpoint (⑥ GAS が読む)。
// 認証: x-read-token (専用 PML_READ_TOKEN)。商品管理リスト専用の固定レスポンス (汎用DB読み取りにしない)。
// GAS 側ゲート: status='ok' かつ payload_checksum を行から再計算して一致 かつ 鮮度OK のときだけシート上書き。
//   Cache-Control: no-store。published 無しは 200 + ok:false で返す (呼び出し側で判定)。
const PML_COLS_OUT = [
  '商品コード','商品名','仕入先','取扱区分','商品区分','売上分類','最終仕入日','在庫保管日数',
  '総在庫数','FBA在庫数','フリー在庫','注残数','引当数','総在庫数_引当なし',
  '販売数7日_FBA','販売数7日_FBA以外','販売数7日_合計',
  '販売数30日_FBA','販売数30日_FBA以外','販売数30日_合計',
  '発注ロット単位','推奨保有月数','売価','原価','想定見込み利益','概算利益率',
  '代表商品コード','ロケーションコード','商品分類タグ','登録日',
];
// 注残数のアプリ台帳差替 (SSoT化 2026-07-13): 発注はNEに登録しなくなったため、NE由来の注残数は
// legacy (更新されない古い値が残る)。正本 = purchase-orders 台帳 (v_ledger_backorder_by_product)。
// 設定 backorder_source='ne' (緊急ロールバック) のときだけ差替しない。
// po_* 未初期化などで差し替えられない場合は fail-closed (500) — 古いNE値を黙って配らない。
function substituteLedgerBackorder(db, rows) {
  const src = (() => {
    try { return db.prepare("SELECT value FROM po_settings WHERE key='backorder_source'").get()?.value || 'app'; }
    catch { return 'app'; } // po_settings 不在 (発注アプリ未初期化) は既定 'app' → 下のprepareで失敗して500
  })();
  if (src === 'ne') return { rows, source: 'ne_legacy' };
  const zan = new Map(db.prepare('SELECT product_key, backorder_qty FROM v_ledger_backorder_by_product').all()
    .map(r => [r.product_key, r.backorder_qty]));
  return {
    rows: rows.map(r => ({ ...r, 注残数: zan.get(String(r.商品コード == null ? '' : r.商品コード).trim().toLowerCase()) || 0 })),
    source: 'app_ledger',
  };
}
router.get('/api/pml/published', requirePmlReadToken, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  // 正本ビュー (v_ledger_backorder_by_product) は purchase-orders の初期化で作られる。
  // cold start直後のGASアクセスでビュー未作成500にならないよう初期化を保証 (Codex SSoT-R2 Low)。
  // ⚠️動的import: 静的importだと相互依存になる (purchase-orders/db.js は本モジュールの隣 db.js を import する)
  try { (await import('../purchase-orders/db.js')).getDB(); }
  catch (e) { console.error('[Mirror] purchase-orders 初期化失敗 (注残差替の前提):', e.message); }
  const db = getMirrorDB();
  try {
    const snap = db.transaction(() => {
      const pub = db.prepare('SELECT * FROM mirror_pml_published WHERE id=1').get();
      if (!pub) return { ok: false, reason: 'no_published' };
      const raw = db.prepare(`SELECT ${PML_COLS_OUT.join(', ')} FROM mirror_pml_snapshot_rows WHERE run_id=? ORDER BY 商品コード`).all(pub.run_id);
      const { rows, source } = substituteLedgerBackorder(db, raw);
      return { pub, rows, backorderSource: source };
    })();
    if (snap.ok === false) {
      return res.json({ ok: false, reason: snap.reason, columns: PML_COLS_OUT });
    }
    const { pub, rows, backorderSource } = snap;
    // 注残数を差し替えたので checksum も差替後の行で再計算する (GAS は受信行から再計算して一致検証するため。
    // 算出規約は ingest 側の検証・build-product-management-snapshot.js と同一: 列順固定・null=''・tab/改行)
    const canonical = rows.map(r => PML_COLS_OUT.map(c => r[c] == null ? '' : String(r[c])).join('\t')).join('\n');
    const checksum = crypto.createHash('sha256').update(canonical).digest('hex');
    res.json({
      ok: true,
      run_id: pub.run_id,
      status: pub.status,                       // GAS は 'ok' のみ上書き
      as_of_date: pub.as_of_date,
      generated_at: pub.generated_at,
      published_at: pub.published_at,
      synced_at: pub.synced_at,
      payload_checksum: checksum,               // GAS は受信行から再計算し一致検証 (注残数差替後の値)
      backorder_source: backorderSource,        // 'app_ledger' = 注残数はアプリ発注台帳 / 'ne_legacy' = ロールバック中
      ne_payload_checksum: pub.payload_checksum, // 参考: miniPC生成時 (NE注残数) のchecksum
      row_count: pub.row_count,
      actual_row_count: rows.length,
      ne_fba_overlap: pub.ne_fba_overlap,
      // FBA鮮度 (Part2): daily=朝の日次 / live=オンデマンドRESTOCK。GAS/UI で「FBA在庫はHH:MM時点」を出す。
      fba_source_kind: pub.fba_source_kind || 'daily',
      fba_source_run_id: pub.fba_source_run_id || null,
      fba_fetched_at: pub.fba_fetched_at || null,
      fba_latest_row_count: pub.fba_latest_row_count ?? null,
      watermarks: {
        ne_products_synced_at: pub.src_ne_products_synced_at,
        velocity_as_of: pub.src_velocity_as_of,
        fba_business_date: pub.src_fba_business_date,
        reorder_updated_at: pub.src_reorder_updated_at,
      },
      server_time_utc: new Date().toISOString(),
      columns: PML_COLS_OUT,                    // GAS の列順・checksum 再計算用 (null='' tab/改行)
      rows,
    });
  } catch (e) {
    console.error('[Mirror] /api/pml/published エラー:', e.message);
    res.status(500).json({ error: e.message });
  }
});

router.get('/api/sku-master/recent-missing-candidates', requireReadToken, (req, res) => {
  const db = getMirrorDB();
  const SINCE_DAYS = 7; // 固定 (Codex review #2: パラメータ受け付けない)

  try {
    // mirror_sku_master.source_updated_at は 'YYYY-MM-DDTHH:MM:SS.fffZ' (UTC ISO 8601、m_sku_master.updated_at 素通し)
    // 'datetime' 関数は ISO 8601 を解釈できるので直接比較可
    //
    // GAS 側の「商品コード変換テーブル」直接追記運用 (2026-05-15 仕様変更) のため、
    // components (NE商品コード + 数量) を JSON 配列で返す。セット商品 (1 SKU が複数 NE) は
    // GAS 側で components.length 行に展開して書き込む。
    // master / components / 鮮度timestamp を 1 read transaction で読む (Codex round6 High)。
    //   GET 中に sync commit が挟まると、別々の autocommit SELECT では
    //   「旧 master snapshot + 新 components snapshot」(またはその逆) の read-side split が成立し、
    //   GAS / FBA 直読みが「片側だけ新しい」入力を消費し得る。同一スナップショットを保証する。
    const snap = db.transaction(() => {
      const masters = db.prepare(`
        SELECT seller_sku, 商品名, source_updated_at
        FROM mirror_sku_master
        WHERE source_updated_at IS NOT NULL
          AND source_updated_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-7 days')
        ORDER BY source_updated_at DESC, seller_sku
      `).all();
      // mirror_sku_resolved には source='master' の component 行が入っている。
      // sort_order でセット構成順を再現 (代表 ne_code = sort_order 0)。
      // GAS は components を順に展開して書き込むため、ne_code 順に潰すと代表/構成順がずれる。
      let resolvedRows = [];
      if (masters.length > 0) {
        const placeholders = masters.map(() => '?').join(',');
        resolvedRows = db.prepare(`
          SELECT seller_sku, ne_code, quantity, sort_order
          FROM mirror_sku_resolved
          WHERE seller_sku IN (${placeholders})
            AND source = 'master'
          ORDER BY seller_sku, sort_order, ne_code
        `).all(...masters.map(m => m.seller_sku));
      }
      const lastSyncRow = db.prepare(`SELECT value FROM mirror_sync_status WHERE key='last_sync'`).get();
      const masterSyncRow = db.prepare(`SELECT MAX(synced_at) AS s FROM mirror_sku_master`).get();
      const resolvedSyncRow = db.prepare(`SELECT MAX(synced_at) AS s FROM mirror_sku_resolved`).get();
      return {
        masters,
        resolvedRows,
        mirrorLastSync: lastSyncRow?.value ?? null,
        masterSyncedAt: masterSyncRow?.s ?? null,
        resolvedSyncedAt: resolvedSyncRow?.s ?? null,
      };
    });
    const { masters, resolvedRows, mirrorLastSync, masterSyncedAt, resolvedSyncedAt } = snap();

    // master と components の最新 synced_at が一致しないのは split snapshot
    // (旧バージョン由来 or 手動 DB 変更)。受信側は対で atomic 更新するので通常は必ず一致する。
    // fail-closed: 不一致なら items を空で返し、GAS が「新 master + 古い components」等を
    // 商品コード変換テーブルに書き込まないよう停止させる (今回の発端バグと同種の欠落防止)。
    const pairConsistent = masterSyncedAt === resolvedSyncedAt;

    const componentsBySku = new Map();
    if (pairConsistent) {
      for (const r of resolvedRows) {
        if (!componentsBySku.has(r.seller_sku)) componentsBySku.set(r.seller_sku, []);
        componentsBySku.get(r.seller_sku).push({ ne_code: r.ne_code, quantity: r.quantity, sort_order: r.sort_order });
      }
    } else {
      console.warn(`[sku-master/recent-missing-candidates] pair stale: master_synced=${masterSyncedAt} resolved_synced=${resolvedSyncedAt} → items 空で返す (GAS 停止、split snapshot 消費防止)`);
    }
    const items = pairConsistent ? masters.map(m => ({
      seller_sku: m.seller_sku,
      商品名: m['商品名'],
      source_updated_at: m.source_updated_at,
      components: componentsBySku.get(m.seller_sku) || [],
    })) : [];

    // mirror_last_synced_at は GAS が live append の鮮度判定 (当日/26h 以内か) に使う値。
    // この endpoint は SKU 専用なので、global な mirror_sync_status.last_sync ではなく
    // 「SKU ペアの有効鮮度」を返す (Codex PR1 round7 adjudication)。
    //   マスタペアがスキップされた同期 (送信側 masterPairOk=false) では last_sync は進むが
    //   mirror_sku_master/_resolved の synced_at は古いまま → masterSyncedAt を返せば GAS の
    //   既存 staleness チェックが自動的に stale 判定して停止する (GAS 契約変更も時間閾値も不要)。
    //   pair 不整合時は null (= GAS は停止)。global 値は観測用に別フィールドで併記。
    const effectiveSkuSyncedAt = pairConsistent ? masterSyncedAt : null;

    res.setHeader('Cache-Control', 'no-store');
    res.json({
      since_days: SINCE_DAYS,
      server_time_utc: new Date().toISOString(),
      mirror_last_synced_at: effectiveSkuSyncedAt,
      mirror_global_last_synced_at: mirrorLastSync,
      mirror_sku_master_synced_at: masterSyncedAt,
      mirror_sku_resolved_synced_at: resolvedSyncedAt,
      pair_consistent: pairConsistent,
      count: items.length,
      items,
    });
  } catch (e) {
    console.error('[sku-master/recent-missing-candidates] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /api/download/:table ───
// CSVダウンロード

router.get('/api/download/:table', (req, res) => {
  const db = getMirrorDB();
  const table = req.params.table;
  const allowed = ['products', 'set_components', 'sales_monthly', 'sales_daily'];
  if (!allowed.includes(table)) return res.status(400).json({ error: '無効なテーブル名' });

  const tableName = 'mirror_' + table;
  const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
  if (!rows.length) return res.status(404).json({ error: 'データなし' });

  const headers = Object.keys(rows[0]);
  const escapeCsv = v => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g, '""') + '"' : s; };
  const lines = [headers.join(','), ...rows.map(r => headers.map(h => escapeCsv(r[h])).join(','))];

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="mirror_${table}.csv"`);
  res.send('\uFEFF' + lines.join('\r\n'));
});

export default router;
