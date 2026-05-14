/**
 * sync-to-render.js — ミニPCからRenderにミラーデータを送信
 *
 * 送信データ:
 *   - m_products（全件、約7,000件）
 *   - m_set_components（全件、約2,500件）
 *   - f_sales_by_product 月次集計（24ヶ月分）
 *   - f_sales_by_listing 月次集計（24ヶ月分）
 *   - f_sales_by_product 日次集計（直近90日分）
 *   - f_sales_by_listing 日次集計（直近90日分）
 *
 * daily-sync.js から呼び出す or 単体実行可能。
 */
import { getDB } from './db.js';

const RENDER_URL = process.env.RENDER_MIRROR_URL || 'https://bfaith-portal.onrender.com/apps/mirror';
const SYNC_KEY = process.env.MIRROR_SYNC_KEY || '';

// Phase 1a.1 (Issue #82) 完了: KEY 必須に揃える (Render 側 ALLOW_INSECURE 解除済)
// 起動時に MIRROR_SYNC_KEY 未設定なら fail-fast (sync 全部 401 になるので)
if (!SYNC_KEY) {
  console.error('FATAL: MIRROR_SYNC_KEY env が未設定です。.env に追加してください。');
  console.error('       (Phase 1a.1 で Render の ALLOW_INSECURE_MIRROR_SYNC は解除済、KEY なしでは sync 401)');
  process.exit(2);
}
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK || 'https://chat.googleapis.com/v1/spaces/AAQAL5zHy-w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=yER7IJx_9CkKhYnzzre0WcWuqfgXc1oh8ldR35k01zE';

async function notify(text) {
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) { console.error('[通知エラー]', e.message); }
}

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

/**
 * 月末在庫スナップショットの sync payload を構築する
 *
 * ★ 設計上の注意:
 *   - SELECT 失敗（テーブル未作成・DBエラー等）時は { fetched: false, parts: [] }
 *     → 呼び出し側は "送信しない" ことで mirror を誤クリアしない
 *   - SELECT 成功して 0 件の時は clear-only part を返す
 *     → 呼び出し側が送信すると mirror 側の stale データが消える
 *   - SELECT 成功して N 件の時はチャンク分割した parts を返す
 *     → 初回 chunk のみ meta.clear_stock_snapshot=true
 *
 * test-profit-schema.mjs Test 9 から直接呼び出され、回帰検知に使われる。
 *
 * @param {Database} db better-sqlite3 インスタンス
 * @param {string} monthCutoff 'YYYY-MM' 以降の年月を対象
 * @param {number} chunkSize 1チャンクの最大件数（default 20000）
 * @returns {{ fetched: boolean, parts: Array<{payload, label}>, error?: string, count?: number }}
 */
export function buildStockSnapshotSyncParts(db, monthCutoff, chunkSize = 20000) {
  // Codex PR2a Round 4 非ブロッカー #2 反映: chunkSize <= 0 で無限ループ防止
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`chunkSize は正の整数: ${chunkSize}`);
  }
  let rows;
  try {
    rows = db.prepare(`
      SELECT 年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at
      FROM stock_monthly_snapshot
      WHERE 年月 >= ?
    `).all(monthCutoff);
  } catch (e) {
    return { fetched: false, parts: [], error: e.message };
  }

  const parts = [];
  if (rows.length === 0) {
    // SELECT 成功して 0件 → clear-only part で mirror 側の stale を消す
    parts.push({
      payload: { stock_monthly_snapshot: [], meta: { clear_stock_snapshot: true } },
      label: '在庫スナップショット(空、clear のみ)',
    });
  } else {
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const isFirst = i === 0;
      parts.push({
        payload: { stock_monthly_snapshot: chunk, meta: isFirst ? { clear_stock_snapshot: true } : undefined },
        label: `在庫スナップショット ${i + 1}-${Math.min(i + chunkSize, rows.length)}`,
      });
    }
  }
  return { fetched: true, parts, count: rows.length };
}

export async function syncToRender() {
  const db = getDB();
  const ts = now();
  console.log('[Sync→Render] 開始...');

  // 日付計算
  //   注: setMonth(-24) + `WHERE >= months24agoStr` の境界は「現在月 + 過去24ヶ月 = 25ヶ月分」。
  //       既存の sales_monthly も同じ境界扱いで、PR2a の stock_monthly_snapshot も揃えている。
  const today = new Date();
  const months24ago = new Date(today);
  months24ago.setMonth(months24ago.getMonth() - 24);
  const months24agoStr = months24ago.toISOString().slice(0, 7); // YYYY-MM

  const days90ago = new Date(today);
  days90ago.setDate(days90ago.getDate() - 90);
  const days90agoStr = days90ago.toISOString().slice(0, 10); // YYYY-MM-DD

  // 1. products（代表商品コードをraw_ne_productsからJOIN）
  const products = db.prepare(`
    SELECT p.*, n.代表商品コード
    FROM m_products p
    LEFT JOIN raw_ne_products n ON p.商品コード = n.商品コード COLLATE NOCASE
  `).all();
  console.log(`[Sync→Render]   products: ${products.length}件`);

  // 2. set_components
  const set_components = db.prepare('SELECT * FROM m_set_components').all();
  console.log(`[Sync→Render]   set_components: ${set_components.length}件`);

  // 2b-3. inv_daily_summary（PR-B: 日次在庫スナップショット集計）
  //   小規模 (1日3行 × 365日 = 1,095/年)、毎回全件送って Render mirror を完全置換
  let inv_daily_summary = [];
  try {
    inv_daily_summary = db.prepare(`
      SELECT business_date, market, category, total_qty, total_value,
             resolved_count, unresolved_count, cost_missing_count,
             source_status, source_row_count, captured_at
      FROM inv_daily_summary
      ORDER BY business_date, market, category
    `).all();
    console.log(`[Sync→Render]   inv_daily_summary: ${inv_daily_summary.length}件`);
  } catch (e) {
    console.log(`[Sync→Render]   inv_daily_summary: 取得失敗（スキップ）: ${e.message}`);
  }

  // 2b-4. inv_daily_detail (D-1c: 詳細層、差分sync)
  //   毎回 直近7日分のみ送信 → Render側で UPSERT
  //   Render側は受信時に「365日より古い行 DELETE」も実行 (古い分は捨てる)
  //   1日 5,000-6,000行 × 7日 = 約 35,000行/送信 → ~5MB ペイロード
  let inv_daily_detail = [];
  try {
    inv_daily_detail = db.prepare(`
      SELECT * FROM inv_daily_detail
      WHERE business_date >= date('now','-7 days')
      ORDER BY business_date, market, category, source_system, source_item_code, ne_code
    `).all();
    console.log(`[Sync→Render]   inv_daily_detail (直近7日): ${inv_daily_detail.length}件`);
  } catch (e) {
    console.log(`[Sync→Render]   inv_daily_detail: 取得失敗（スキップ）: ${e.message}`);
  }

  // 2b-2. sku_resolved (master only、m_sku_components 直結)
  //   SKU管理統合 Phase 4-B (2026-05-04): sku_map / v_sku_resolved 経由を撤廃
  //   m_sku_components + m_sku_master から直接取得
  //   - source は常に 'master' (auto fallback は Step 4-0 で撤廃済み)
  //   - 商品名: m_sku_master.商品名 (社内独自名)
  //   - source_updated_at: m_sku_master.updated_at
  let sku_resolved = [];
  try {
    sku_resolved = db.prepare(`
      SELECT
        c.seller_sku,
        c.ne_code,
        c.数量 AS quantity,
        'master' AS source,
        m.商品名,
        m.updated_at AS source_updated_at
      FROM m_sku_components c
      INNER JOIN m_sku_master m ON c.seller_sku = m.seller_sku
      ORDER BY c.seller_sku, c.sort_order
    `).all();
    console.log(`[Sync→Render]   sku_resolved: ${sku_resolved.length}件 (全 master、m_sku_components 直結)`);
  } catch (e) {
    console.log(`[Sync→Render]   sku_resolved: 取得失敗（スキップ）: ${e.message}`);
  }

  // 2b-2b. sku_master (1 SKU = 1 行、Sheets 商品コード変換テーブルとの差分検出用)
  //   sku_resolved は seller_sku × ne_code 粒度 (派生) なので、SKU 一覧用途には不自然
  //   m_sku_master.{seller_sku, 商品名, created_at, updated_at} をそのまま 1 SKU 1 行で送る
  //   (Codex review 2026-05-13: A''案 = master の mirror を独立に持つ)
  //
  // ★ fail-closed 設計 (Codex round 1 + round 2 反映):
  //   ・SELECT 失敗 → payload に sku_master キーを含めない → mirror 前回値保持
  //   ・SELECT 成功 0 件 → 通常運用で m_sku_master が 0 件になることはない (新規初期化のみ)
  //     worktree で空 DB を見ている / DB 壊れ / 初期セットアップ前 等の異常を示唆
  //     → **送信スキップ** + GChat warn 通知 + mirror 前回値保持
  //     daily sync では絶対に自動 clear flag を立てない (round 2 high: 自動付与は防御の素通し)
  //   ・SELECT 成功 N>0 件 → 通常通り全件置換 payload を送る
  //
  //   過去事故 (2026-05-08〜10 Yahoo VPS proxy regression: 取得失敗を「空配列の正常同期」として
  //   流し mirror 3 日間全空文字) + (2026-05-06 worktree で別 DB が静かに分裂) の再発防止。
  //   mirror_sku_master を明示的に全消ししたい運用 (リセット時等) は別手段で行う想定:
  //     例) curl で meta.clear_sku_master=true を付けた payload を直送、or 手動 DB 削除。
  // sku_master_state は後段の verify / allMatch / 通知判定で参照
  //   'ok'             : SELECT 成功かつ N>0 件、payload 送信
  //   'empty_skipped'  : SELECT 成功 0 件、送信スキップ (異常検知ポイント)
  //   'select_failed'  : SELECT 失敗 (schema drift / DB lock 等)、送信スキップ
  let sku_master = null;
  let sku_master_state = 'ok';
  try {
    const rows = db.prepare(`
      SELECT
        seller_sku,
        商品名,
        created_at AS source_created_at,
        updated_at AS source_updated_at
      FROM m_sku_master
      ORDER BY seller_sku
    `).all();
    if (rows.length === 0) {
      sku_master_state = 'empty_skipped';
      console.warn('[Sync→Render]   sku_master: SELECT 成功 0件 → 送信スキップ (worktree/別DB/壊れたDB の可能性、mirror は前回値を保持)');
      await notify('⚠️ *Sync sku_master 異常*\nm_sku_master が 0 件。送信スキップ。worktree 別 DB / 壊れた DB / 初期セットアップ前の可能性を疑ってください。');
    } else {
      sku_master = rows;
      console.log(`[Sync→Render]   sku_master: ${rows.length}件 (1 SKU = 1 行)`);
    }
  } catch (e) {
    sku_master_state = 'select_failed';
    console.error(`[Sync→Render]   sku_master: 取得失敗（送信スキップ、mirror は前回値を保持）: ${e.message}`);
    await notify(`⚠️ *Sync sku_master 取得失敗*\n${e.message.slice(0, 300)}\n送信スキップ、mirror は前回値を保持。schema drift / DB lock を疑ってください。`);
  }

  // 2c. amazon_sku_fees（手数料キャッシュ）
  let amazon_sku_fees = [];
  try {
    amazon_sku_fees = db.prepare('SELECT * FROM amazon_sku_fees').all();
    console.log(`[Sync→Render]   amazon_sku_fees: ${amazon_sku_fees.length}件`);
  } catch {
    console.log(`[Sync→Render]   amazon_sku_fees: テーブル未作成（スキップ）`);
  }

  // 2d. rakuten_sku_map（楽天AM/AL/W→NE商品コード マッピング）
  let rakuten_sku_map = [];
  try {
    rakuten_sku_map = db.prepare('SELECT rakuten_code, ne_code, source, updated_at FROM f_rakuten_sku_map').all();
    console.log(`[Sync→Render]   rakuten_sku_map: ${rakuten_sku_map.length}件`);
  } catch {
    console.log(`[Sync→Render]   rakuten_sku_map: テーブル未作成（スキップ）`);
  }

  // 2e. stock_monthly_snapshot（月末在庫スナップショット、直近24ヶ月分）
  //     商品収益性ダッシュボード タブB の GMROI / 移動平均在庫 計算に使う
  //     buildStockSnapshotSyncParts は SELECT 失敗を fetched=false で表し、
  //     呼び出し側（Part 1b 送信）で送信スキップ判定を行う
  const stockSyncPlan = buildStockSnapshotSyncParts(db, months24agoStr);
  if (stockSyncPlan.fetched) {
    console.log(`[Sync→Render]   stock_monthly_snapshot: ${stockSyncPlan.count}件`);
  } else {
    console.log(`[Sync→Render]   stock_monthly_snapshot: 取得失敗（スキップ）: ${stockSyncPlan.error}`);
  }

  // 3. sales_monthly（24ヶ月分、by_product + by_listing）
  const salesMonthlyProduct = db.prepare(`
    SELECT SUBSTR(日付, 1, 7) as 月, 商品コード, モール, MAX(商品名) as 商品名,
      SUM(数量) as 数量, SUM(直接販売数) as 直接販売数, SUM(セット経由数) as セット経由数,
      NULL as 売上金額, NULL as 注文数, 'by_product' as データ種別, '' as チャネル
    FROM f_sales_by_product
    WHERE SUBSTR(日付, 1, 7) >= ?
    GROUP BY 月, 商品コード, モール
  `).all(months24agoStr);

  const salesMonthlyListing = db.prepare(`
    SELECT 月, モール商品コード as 商品コード, モール, MAX(商品名) as 商品名,
      SUM(数量) as 数量, 0 as 直接販売数, 0 as セット経由数,
      SUM(売上金額) as 売上金額, SUM(注文数) as 注文数, 'by_listing' as データ種別, チャネル
    FROM f_sales_by_listing
    WHERE 月 >= ?
    GROUP BY 月, モール商品コード, モール, チャネル
  `).all(months24agoStr);

  const sales_monthly = [...salesMonthlyProduct, ...salesMonthlyListing];
  console.log(`[Sync→Render]   sales_monthly: ${sales_monthly.length}件 (product: ${salesMonthlyProduct.length}, listing: ${salesMonthlyListing.length})`);

  // 4. sales_daily（直近90日、by_product + by_listing）
  const salesDailyProduct = db.prepare(`
    SELECT 日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
      NULL as 売上金額, NULL as 注文数, 'by_product' as データ種別, '' as チャネル
    FROM f_sales_by_product
    WHERE 日付 >= ?
  `).all(days90agoStr);

  const salesDailyListing = db.prepare(`
    SELECT 日付, モール商品コード as 商品コード, モール, 商品名, 数量,
      0 as 直接販売数, 0 as セット経由数,
      売上金額, 注文数, 'by_listing' as データ種別, チャネル
    FROM f_sales_by_listing
    WHERE 日付 >= ?
  `).all(days90agoStr);

  const sales_daily = [...salesDailyProduct, ...salesDailyListing];
  console.log(`[Sync→Render]   sales_daily: ${sales_daily.length}件 (product: ${salesDailyProduct.length}, listing: ${salesDailyListing.length})`);

  // 分割送信（各パートを個別にPOST、8MB以下に収める）
  const headers = { 'Content-Type': 'application/json' };
  if (SYNC_KEY) headers['x-sync-key'] = SYNC_KEY;

  async function sendPart(data, label) {
    const json = JSON.stringify(data);
    const sizeMB = (json.length / 1024 / 1024).toFixed(1);
    console.log(`[Sync→Render]   送信: ${label} (${sizeMB}MB)`);
    const response = await fetch(`${RENDER_URL}/api/sync`, {
      method: 'POST', headers, body: json,
      signal: AbortSignal.timeout(120000),
    });
    if (!response.ok) {
      const err = await response.text().catch(() => '');
      throw new Error(`${label}: HTTP ${response.status} ${err.slice(0, 200)}`);
    }
    return response.json();
  }

  try {
    // Part 1: マスタデータ
    //   sku_master は SELECT 成功 & N>0 件のときだけ payload に含める (fail-closed)。
    //   SELECT 失敗 / 0 件 / その他異常時は payload に乗せない → 受信側は前回 mirror を保持。
    //   明示的な全消し (clear_sku_master flag) は daily sync では絶対に発行しない (round 2 high)。
    const masterPart = {
      products, set_components, sku_resolved, amazon_sku_fees, rakuten_sku_map, inv_daily_summary,
    };
    if (Array.isArray(sku_master) && sku_master.length > 0) {
      masterPart.sku_master = sku_master;
    }
    await sendPart(masterPart, 'マスタ');

    // Part 1c: inv_daily_detail (D-1c、直近7日、~17MB なので chunk 分割)
    // 初回チャンクの meta:
    //   - inv_daily_detail_clear_old=true : Render 側で365日より古い行を削除 (housekeeping)
    //   - inv_daily_detail_clear_dates=[...] : この sync で送信する全 business_date を Render 側で先に DELETE
    //     → 同日再集計で SKU が消えた場合の stale detail 行を mirror に残さない (Codex R3 #2 対応)
    const detailChunkSize = 5000;
    const clearDates = [...new Set(inv_daily_detail.map(r => r.business_date).filter(Boolean))];
    if (inv_daily_detail.length === 0) {
      await sendPart({ inv_daily_detail: [], meta: { inv_daily_detail_clear_old: true } }, 'inv_daily_detail (空 / 古い行クリーンのみ)');
    } else {
      for (let i = 0; i < inv_daily_detail.length; i += detailChunkSize) {
        const chunk = inv_daily_detail.slice(i, i + detailChunkSize);
        const isFirst = i === 0;
        await sendPart(
          { inv_daily_detail: chunk, meta: isFirst ? { inv_daily_detail_clear_old: true, inv_daily_detail_clear_dates: clearDates } : undefined },
          `inv_daily_detail ${i + 1}-${Math.min(i + detailChunkSize, inv_daily_detail.length)}`
        );
      }
    }

    // Part 1b: 月末在庫スナップショット（PR2a 追加、タブB GMROI用）
    //   件数は最大 商品数 × 25ヶ月（現在月+過去24ヶ月、sales_monthly と同じ境界扱い）。
    //   約 7,000 × 25 = 175,000 が上限イメージ。
    //
    //   ★ buildStockSnapshotSyncParts() が clear/chunk の意味論を担保:
    //     - fetched=false → parts=[] で送信なし（mirror は前回状態を保持）
    //     - fetched=true, 0件 → parts に clear-only 1件（mirror stale を消す）
    //     - fetched=true, N件 → parts は初回 clear + 残り chunk
    //   Test 9（test-profit-schema.mjs）が回帰検知する
    for (const part of stockSyncPlan.parts) {
      await sendPart(part.payload, part.label);
    }

    // Part 2: 月次集計（チャンク分割、9MB以下に収める）
    const monthlyChunkSize = 20000;
    for (let i = 0; i < sales_monthly.length; i += monthlyChunkSize) {
      const chunk = sales_monthly.slice(i, i + monthlyChunkSize);
      const isFirst = i === 0;
      await sendPart(
        { sales_monthly: chunk, meta: isFirst ? { clear_monthly: true } : undefined },
        `月次 ${i + 1}-${Math.min(i + monthlyChunkSize, sales_monthly.length)}`
      );
    }

    // Part 3: 日次集計（チャンク分割）
    const dailyChunkSize = 20000;
    for (let i = 0; i < sales_daily.length; i += dailyChunkSize) {
      const chunk = sales_daily.slice(i, i + dailyChunkSize);
      const isFirst = i === 0;
      await sendPart(
        { sales_daily: chunk, meta: isFirst ? { clear_daily: true } : undefined },
        `日次 ${i + 1}-${Math.min(i + dailyChunkSize, sales_daily.length)}`
      );
    }

    // Part 4: 最終メタデータ
    await sendPart({
      meta: { source: 'minipc', synced_at: ts, products_count: products.length,
        sales_monthly_count: sales_monthly.length, sales_daily_count: sales_daily.length }
    }, 'メタデータ');

    // Part 5: Render側のデータ件数を検証
    console.log('[Sync→Render]   検証中...');
    const statusRes = await fetch(`${RENDER_URL}/api/status`, { signal: AbortSignal.timeout(30000) });
    const status = await statusRes.json();

    const verify = {
      products: { sent: products.length, received: status.products_count || 0 },
      monthly: { sent: sales_monthly.length, received: status.sales_monthly_count || 0 },
      daily: { sent: sales_daily.length, received: status.sales_daily_count || 0 },
      stock_snapshot: { sent: stockSyncPlan.count ?? 0, received: status.stock_snapshot_count || 0, fetched: stockSyncPlan.fetched },
      // Codex round 3 high #1+#2 反映: sku_master の状態を verify に乗せ、
      // 異常 (skip/失敗) を全体成功扱いから除外する
      sku_master: {
        state: sku_master_state,
        sent: Array.isArray(sku_master) ? sku_master.length : 0,
        received: status.sku_master_count ?? 0,
      },
    };

    // stock_snapshot: fetched=false（SELECT失敗で同期スキップ）なら検証対象外。
    // fetched=true なら送信件数と受信件数が一致すべき。
    const stockMatch = !stockSyncPlan.fetched
      || verify.stock_snapshot.sent === verify.stock_snapshot.received;

    // sku_master:
    //   'ok'             → 送信件数と受信件数の一致を要求
    //   'empty_skipped'  → 上流異常 (worktree/空DB)、allMatch=false にする
    //   'select_failed'  → schema drift / DB lock、allMatch=false にする
    const skuMasterMatch = sku_master_state === 'ok'
      && verify.sku_master.sent === verify.sku_master.received;

    const allMatch = verify.products.sent === verify.products.received
      && verify.monthly.sent === verify.monthly.received
      && verify.daily.sent === verify.daily.received
      && stockMatch
      && skuMasterMatch;

    if (allMatch) {
      console.log(`[Sync→Render] ✅ 検証OK — 全データ一致`);
      const stockLine = stockSyncPlan.fetched
        ? `\n月末在庫: ${verify.stock_snapshot.received}件`
        : `\n月末在庫: 取得スキップ`;
      await notify(`✅ *Render同期完了*\n商品マスタ: ${verify.products.received}件\n月次集計: ${verify.monthly.received}件\n日次集計: ${verify.daily.received}件${stockLine}\nSKUマスタ: ${verify.sku_master.received}件\n同期時刻: ${ts}`);
    } else {
      console.log(`[Sync→Render] ⚠️ 検証NG — データ不一致`);
      console.log(`  products: 送信${verify.products.sent} / 受信${verify.products.received}`);
      console.log(`  monthly: 送信${verify.monthly.sent} / 受信${verify.monthly.received}`);
      console.log(`  daily: 送信${verify.daily.sent} / 受信${verify.daily.received}`);
      console.log(`  stock_snapshot: 送信${verify.stock_snapshot.sent} / 受信${verify.stock_snapshot.received} / fetched=${stockSyncPlan.fetched}`);
      console.log(`  sku_master: 状態=${sku_master_state} / 送信${verify.sku_master.sent} / 受信${verify.sku_master.received}`);
      const skuMasterLine = sku_master_state === 'ok'
        ? `SKUマスタ: ${verify.sku_master.sent}→${verify.sku_master.received}`
        : `SKUマスタ: 状態=${sku_master_state} (送信スキップ、mirror=${verify.sku_master.received})`;
      await notify(`⚠️ *Render同期 データ不一致*\n商品: ${verify.products.sent}→${verify.products.received}\n月次: ${verify.monthly.sent}→${verify.monthly.received}\n日次: ${verify.daily.sent}→${verify.daily.received}\n在庫: ${verify.stock_snapshot.sent}→${verify.stock_snapshot.received}${stockSyncPlan.fetched ? '' : ' (skipped)'}\n${skuMasterLine}`);
    }

    // ok の判定方針 (Codex round 4 high 反映):
    //   既存テーブル (products/monthly/daily/stock_snapshot) の count 不一致は notify のみで ok:true を維持
    //     (intermittent な count drift で daily-sync が常時 retry になるのを避ける、従来動作)
    //   sku_master は **状態異常も count mismatch も** すべて ok:false にする (新 PR スコープ)
    //     SELECT 失敗 / 0 件スキップ / Render 側 partial apply / status 件数不一致 のいずれも
    //     daily-sync 側で retry-failed-jobs に拾わせ、再発リスクを早く気付ける状態にする
    const sku_master_failure =
      sku_master_state !== 'ok' ||
      verify.sku_master.sent !== verify.sku_master.received;
    return { ok: !sku_master_failure, verify };
  } catch (e) {
    console.error(`[Sync→Render] ❌ 送信失敗:`, e.message);
    await notify(`❌ *Render同期失敗*\n${e.message.slice(0, 200)}`);
    return { ok: false, error: e.message };
  }
}

// 単体実行
import { initDB } from './db.js';
const isMain = process.argv[1]?.includes('sync-to-render');
if (isMain) {
  await initDB();
  const result = await syncToRender();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(result.ok ? 0 : 1);
}
