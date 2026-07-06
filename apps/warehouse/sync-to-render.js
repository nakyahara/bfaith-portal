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

// MIRROR_SYNC_KEY 必須 (Render 側 ALLOW_INSECURE 解除済、KEY なしでは sync 401)。
// ※ モジュール先頭では process.exit しない: 本ファイルは WarehouseServer (fba-service の
//   オンデマンドFBA更新) からも import されるため、import 時点で落とすとサーバごと停止する。
//   実際に同期を行う関数の入口で fail-fast する。
function requireSyncKey() {
  if (!SYNC_KEY) {
    throw new Error('MIRROR_SYNC_KEY env が未設定です (.env に追加してください)。KEY なしでは Render sync が 401 になります。');
  }
}
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

async function notify(text) {
  if (!GCHAT_WEBHOOK) {
    console.warn('[sync-to-render] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定のため通知スキップ');
    return;
  }
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) { console.error('[sync-to-render] [NOTIFY:status=failed]', e.message); }
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
  requireSyncKey();
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
  // 2b-2b. sku_master (1 SKU = 1 行、Sheets 商品コード変換テーブルとの差分検出用)
  //   sku_resolved は seller_sku × ne_code 粒度 (派生)、sku_master は 1 SKU 1 行 (一覧/差分用)。
  //   両者は同一 m_sku_master スナップショット由来 (Codex review 2026-05-13: A''案)。
  //
  // ★ fail-closed + 一貫スナップショット設計 (2026-06-07 FBA SKUマスタ直結 PR1、Codex round1〜3 反映):
  //   ・両 SELECT を 1 つの db.transaction で読む。別々の autocommit SELECT だと間にマスタ更新が
  //     commit され「旧 components + 新 master」の不整合 payload が成立する (round3 High)。
  //     transaction で同一スナップショットを保証する。better-sqlite3 transaction は同期実行なので
  //     GChat notify(async) は transaction の外で行う。
  //   ・state: 'ok'(成功&N>0) / 'empty_skipped'(成功0件) / 'select_failed'(SELECT失敗)。
  //     非ok のテーブルは payload に乗せず mirror 前回値を保持 (受信側 DELETE→INSERT の全消し事故防止)。
  //     さらに後段で「両方 ok のときだけ対で送る」(masterPairOk) ため、片側更新による鮮度ずれも防ぐ。
  //   ・明示全消し (clear_sku_resolved / clear_sku_master flag) は daily sync では絶対に発行しない。
  //   過去事故 (2026-05-08〜10 Yahoo proxy regression で mirror 全空 / 2026-05-06 worktree 別 DB 分裂) の再発防止。
  let sku_resolved = null;
  let sku_resolved_state = 'ok';
  let sku_master = null;
  let sku_master_state = 'ok';
  let masterSelectError = null;
  try {
    const readMasterSnapshot = db.transaction(() => {
      const resolvedRows = db.prepare(`
        SELECT
          c.seller_sku,
          c.ne_code,
          c.数量 AS quantity,
          'master' AS source,
          m.商品名,
          m.updated_at AS source_updated_at,
          c.sort_order
        FROM m_sku_components c
        INNER JOIN m_sku_master m ON c.seller_sku = m.seller_sku
        ORDER BY c.seller_sku, c.sort_order
      `).all();
      const masterRows = db.prepare(`
        SELECT
          seller_sku,
          商品名,
          created_at AS source_created_at,
          updated_at AS source_updated_at
        FROM m_sku_master
        ORDER BY seller_sku
      `).all();
      return { resolvedRows, masterRows };
    });
    const { resolvedRows, masterRows } = readMasterSnapshot();
    if (resolvedRows.length > 0) {
      sku_resolved = resolvedRows;
      console.log(`[Sync→Render]   sku_resolved: ${resolvedRows.length}件 (全 master、m_sku_components 直結)`);
    } else {
      sku_resolved_state = 'empty_skipped';
    }
    if (masterRows.length > 0) {
      sku_master = masterRows;
      console.log(`[Sync→Render]   sku_master: ${masterRows.length}件 (1 SKU = 1 行)`);
    } else {
      sku_master_state = 'empty_skipped';
    }
  } catch (e) {
    // transaction は atomic に失敗するため、どちらの SELECT が落ちても両方 select_failed 扱い
    sku_resolved_state = 'select_failed';
    sku_master_state = 'select_failed';
    masterSelectError = e.message;
    console.error(`[Sync→Render]   sku_resolved/sku_master: 取得失敗（送信スキップ、mirror は前回値を保持）: ${e.message}`);
  }
  // notify は transaction 外で await。state ごとに 1 回だけ通知。
  if (sku_resolved_state === 'empty_skipped') {
    console.warn('[Sync→Render]   sku_resolved: SELECT 成功 0件 → 送信スキップ (worktree/別DB/壊れたDB の可能性、mirror は前回値を保持)');
    await notify('⚠️ *Sync sku_resolved 異常*\nm_sku_components が 0 件。送信スキップ。worktree 別 DB / 壊れた DB / 初期セットアップ前の可能性を疑ってください。');
  }
  if (sku_master_state === 'empty_skipped') {
    console.warn('[Sync→Render]   sku_master: SELECT 成功 0件 → 送信スキップ (worktree/別DB/壊れたDB の可能性、mirror は前回値を保持)');
    await notify('⚠️ *Sync sku_master 異常*\nm_sku_master が 0 件。送信スキップ。worktree 別 DB / 壊れた DB / 初期セットアップ前の可能性を疑ってください。');
  }
  if (masterSelectError !== null) {
    await notify(`⚠️ *Sync sku_resolved/sku_master 取得失敗*\n${masterSelectError.slice(0, 300)}\n送信スキップ、mirror は前回値を保持。schema drift / DB lock を疑ってください。`);
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
    //   sku_resolved / sku_master は SELECT 成功 & N>0 件のときだけ payload に含める (fail-closed)。
    //   SELECT 失敗 / 0 件 / その他異常時は payload に乗せない → 受信側は前回 mirror を保持。
    //   明示的な全消し (clear_sku_resolved / clear_sku_master flag) は daily sync では絶対に発行しない (round 2 high)。
    const masterPart = {
      products, set_components, amazon_sku_fees, rakuten_sku_map, inv_daily_summary,
    };
    // sku_resolved と sku_master は同一の m_sku_master スナップショット由来。
    // 「両方とも state=ok かつ N>0」のときだけ対で送る (Codex PR1 review round2 High)。
    //   片方だけ mirror を更新すると、recent-missing-candidates (GAS が読む) が
    //   「新しい master + 古い/欠落 components」を返し、商品コード変換テーブルに誤った/欠落 NE コードを
    //   書き込む穴になる (今回の発端バグと同種)。どちらか異常なら両方スキップし前回 mirror を保持。
    //   ※ どちらか異常時は下段の sku_master_failure / sku_resolved_failure で run 自体が ok:false になり retry される。
    const masterPairOk = sku_resolved_state === 'ok' && sku_master_state === 'ok';
    if (masterPairOk && Array.isArray(sku_resolved) && sku_resolved.length > 0) {
      masterPart.sku_resolved = sku_resolved;
    }
    if (masterPairOk && Array.isArray(sku_master) && sku_master.length > 0) {
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

    // Part 3d: 商品管理リスト スナップショット (④ の published run)
    //   fail-closed: published が指す run の meta が status≠'failed' のときだけ送る (skip はエラーでない)。
    //   送信(HTTP)エラーは握り潰さず throw させ、Render同期 job 全体を失敗させて retry に乗せる
    //   (サイレント成功で mirror が stale のまま成功通知される事故を防ぐ、Codex⑤R1 High)。
    let pmlSync = { state: 'skipped', run_id: null, count: 0 };
    {
      const PML_COLS = [
        '商品コード','商品名','仕入先','取扱区分','商品区分','売上分類','最終仕入日','在庫保管日数',
        '総在庫数','FBA在庫数','フリー在庫','注残数','引当数','総在庫数_引当なし',
        '販売数7日_FBA','販売数7日_FBA以外','販売数7日_合計',
        '販売数30日_FBA','販売数30日_FBA以外','販売数30日_合計',
        '発注ロット単位','推奨保有月数','売価','原価','想定見込み利益','概算利益率',
        '代表商品コード','ロケーションコード','商品分類タグ','登録日',
      ];
      const pub = db.prepare('SELECT run_id FROM product_management_published WHERE id=1').get();
      const pmeta = pub ? db.prepare('SELECT * FROM product_management_snapshot_meta WHERE run_id=?').get(pub.run_id) : null;
      if (pmeta && pmeta.status !== 'failed') {
        const pmlRows = db.prepare(`SELECT ${PML_COLS.join(', ')} FROM product_management_snapshot_rows WHERE run_id=? ORDER BY 商品コード`).all(pmeta.run_id);
        await sendPart({
          pml_snapshot: {
            run_id: pmeta.run_id, status: pmeta.status, as_of_date: pmeta.as_of_date,
            generated_at: pmeta.generated_at, payload_checksum: pmeta.payload_checksum, row_count: pmeta.row_count,
            src_ne_products_synced_at: pmeta.src_ne_products_synced_at, src_velocity_as_of: pmeta.src_velocity_as_of,
            src_fba_business_date: pmeta.src_fba_business_date, src_reorder_updated_at: pmeta.src_reorder_updated_at,
            ne_fba_overlap: pmeta.ne_fba_overlap, rows: pmlRows,
          },
        }, `商品管理リスト snapshot (${pmlRows.length}件, ${pmeta.status})`);
        pmlSync = { state: 'sent', run_id: pmeta.run_id, count: pmlRows.length };
      } else {
        console.log('[Sync→Render]   商品管理リスト snapshot: 送信スキップ (published 無し or failed)');
      }
    }

    // Part 3e: 販売速度 モール別 (速報モール別マート)。
    //   fail-closed: SELECT 成功 & 0件超 のときだけ送る。0件/失敗時は payload に乗せない
    //   → 受信側は前回 mirror を保持 (sync 失敗で速報モール別が全消えするのを防ぐ)。
    try {
      const vmRows = db.prepare(
        'SELECT 商品コード, mall, qty_7d, qty_30d FROM f_sales_velocity_by_product_mall'
      ).all();
      if (vmRows.length > 0) {
        const vmAsOf = db.prepare('SELECT MAX(as_of_date) AS v FROM f_sales_velocity_by_product_mall').get()?.v || '';
        await sendPart({ velocity_mall: { as_of_date: vmAsOf, rows: vmRows } },
          `販売速度モール別 (${vmRows.length}件, as_of=${vmAsOf})`);
      } else {
        console.log('[Sync→Render]   velocity_mall: 0件 → 送信スキップ (mirror は前回値を保持)');
      }
    } catch (e) {
      console.warn(`[Sync→Render]   velocity_mall: 取得失敗（送信スキップ、mirror は前回値を保持）: ${e.message}`);
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
      // FBA 在庫補充の mirror 直読み入力。sku_master と同様 state + count 突合を verify に乗せる。
      sku_resolved: {
        state: sku_resolved_state,
        sent: Array.isArray(sku_resolved) ? sku_resolved.length : 0,
        received: status.sku_resolved_count ?? 0,
      },
      // 商品管理リスト snapshot (⑤)。skip(published無し/failed)は対象外、sent は run_id+件数一致を要求。
      pml: {
        state: pmlSync.state, sent: pmlSync.count, received: status.pml_snapshot_count ?? 0,
        sent_run: pmlSync.run_id, received_run: status.pml_published_run_id ?? null,
      },
    };
    // pmlMatch: skip は OK、sent は mirror の published run_id と件数が一致していること (allMatch より前に評価)
    const pmlMatch = pmlSync.state !== 'sent'
      || (status.pml_published_run_id === pmlSync.run_id && (status.pml_snapshot_count ?? 0) === pmlSync.count);

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

    // sku_resolved も sku_master と同方針: 状態 ok かつ 送受信件数一致のときだけ match
    const skuResolvedMatch = sku_resolved_state === 'ok'
      && verify.sku_resolved.sent === verify.sku_resolved.received;

    const allMatch = verify.products.sent === verify.products.received
      && verify.monthly.sent === verify.monthly.received
      && verify.daily.sent === verify.daily.received
      && stockMatch
      && skuMasterMatch
      && skuResolvedMatch
      && pmlMatch;

    if (allMatch) {
      console.log(`[Sync→Render] ✅ 検証OK — 全データ一致`);
      const stockLine = stockSyncPlan.fetched
        ? `\n月末在庫: ${verify.stock_snapshot.received}件`
        : `\n月末在庫: 取得スキップ`;
      const pmlLine = pmlSync.state === 'sent' ? `\n商品管理リスト: ${verify.pml.received}件 (run=${verify.pml.received_run})` : `\n商品管理リスト: 送信スキップ`;
      await notify(`✅ *Render同期完了*\n商品マスタ: ${verify.products.received}件\n月次集計: ${verify.monthly.received}件\n日次集計: ${verify.daily.received}件${stockLine}\nSKUマスタ: ${verify.sku_master.received}件\nSKU解決: ${verify.sku_resolved.received}件${pmlLine}\n同期時刻: ${ts}`);
    } else {
      console.log(`[Sync→Render] ⚠️ 検証NG — データ不一致`);
      console.log(`  products: 送信${verify.products.sent} / 受信${verify.products.received}`);
      console.log(`  monthly: 送信${verify.monthly.sent} / 受信${verify.monthly.received}`);
      console.log(`  daily: 送信${verify.daily.sent} / 受信${verify.daily.received}`);
      console.log(`  stock_snapshot: 送信${verify.stock_snapshot.sent} / 受信${verify.stock_snapshot.received} / fetched=${stockSyncPlan.fetched}`);
      console.log(`  sku_master: 状態=${sku_master_state} / 送信${verify.sku_master.sent} / 受信${verify.sku_master.received}`);
      console.log(`  sku_resolved: 状態=${sku_resolved_state} / 送信${verify.sku_resolved.sent} / 受信${verify.sku_resolved.received}`);
      const skuMasterLine = sku_master_state === 'ok'
        ? `SKUマスタ: ${verify.sku_master.sent}→${verify.sku_master.received}`
        : `SKUマスタ: 状態=${sku_master_state} (送信スキップ、mirror=${verify.sku_master.received})`;
      const skuResolvedLine = sku_resolved_state === 'ok'
        ? `SKU解決: ${verify.sku_resolved.sent}→${verify.sku_resolved.received}`
        : `SKU解決: 状態=${sku_resolved_state} (送信スキップ、mirror=${verify.sku_resolved.received})`;
      const pmlLineNg = pmlSync.state === 'sent'
        ? `商品管理リスト: 送信run=${verify.pml.sent_run}(${verify.pml.sent}件) / mirror run=${verify.pml.received_run}(${verify.pml.received}件)`
        : `商品管理リスト: 送信スキップ`;
      console.log(`  pml: ${pmlLineNg}`);
      await notify(`⚠️ *Render同期 データ不一致*\n商品: ${verify.products.sent}→${verify.products.received}\n月次: ${verify.monthly.sent}→${verify.monthly.received}\n日次: ${verify.daily.sent}→${verify.daily.received}\n在庫: ${verify.stock_snapshot.sent}→${verify.stock_snapshot.received}${stockSyncPlan.fetched ? '' : ' (skipped)'}\n${skuMasterLine}\n${skuResolvedLine}\n${pmlLineNg}`);
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
    // sku_resolved は FBA 在庫補充の mirror 直読み入力になるため、状態異常 (送信スキップ) も
    // 送受信件数不一致も ok:false にして daily-sync 側で retry-failed-jobs に拾わせる (sku_master と同方針)。
    const sku_resolved_failure =
      sku_resolved_state !== 'ok' ||
      verify.sku_resolved.sent !== verify.sku_resolved.received;
    if (sku_resolved_failure) {
      console.warn(`[Sync→Render] sku_resolved 状態異常: ${sku_resolved_state} → ok:false`);
    }
    // 商品管理リスト snapshot: 送信した(state='sent')のに mirror の published run_id/件数が一致しないなら失敗
    //   (送信HTTPエラーは上で throw 済。pmlMatch は allMatch より前で評価済み = 成功通知判定にも反映済み)。
    const pml_failure = !pmlMatch;
    if (pml_failure) {
      console.warn(`[Sync→Render] pml_snapshot 反映不一致: 送信run=${pmlSync.run_id}(${pmlSync.count}件) / mirror run=${status.pml_published_run_id}(${status.pml_snapshot_count}件) → ok:false`);
    }
    return { ok: !(sku_master_failure || sku_resolved_failure || pml_failure), verify };
  } catch (e) {
    console.error(`[Sync→Render] ❌ 送信失敗:`, e.message);
    await notify(`❌ *Render同期失敗*\n${e.message.slice(0, 200)}`);
    return { ok: false, error: e.message };
  }
}

// ─── pml-only 同期 (オンデマンドFBA更新 ④) ───
// 公開中のPMLスナップショット(meta+rows)だけを送り、mirror で atomic swap させる。
// 重い月次/日次/inv_daily_detail は送らない(数秒)。daily の full sync とは独立に走れる
// (mirror 側は run 単位 atomic swap、他テーブルとは無関係)。FBA鮮度メタも一緒に送る。
const PML_SYNC_COLS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '商品区分', '売上分類', '最終仕入日', '在庫保管日数',
  '総在庫数', 'FBA在庫数', 'フリー在庫', '注残数', '引当数', '総在庫数_引当なし',
  '販売数7日_FBA', '販売数7日_FBA以外', '販売数7日_合計',
  '販売数30日_FBA', '販売数30日_FBA以外', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '想定見込み利益', '概算利益率',
  '代表商品コード', 'ロケーションコード', '商品分類タグ', '登録日',
];

export async function syncPmlSnapshotOnly() {
  requireSyncKey();
  const db = getDB();
  const pub = db.prepare('SELECT run_id FROM product_management_published WHERE id=1').get();
  if (!pub) return { state: 'skipped', reason: 'published無し' };
  const pmeta = db.prepare('SELECT * FROM product_management_snapshot_meta WHERE run_id=?').get(pub.run_id);
  if (!pmeta || pmeta.status === 'failed') return { state: 'skipped', reason: `status=${pmeta?.status || 'none'}` };
  const rows = db.prepare(`SELECT ${PML_SYNC_COLS.join(', ')} FROM product_management_snapshot_rows WHERE run_id=? ORDER BY 商品コード`).all(pmeta.run_id);

  const headers = { 'Content-Type': 'application/json' };
  if (SYNC_KEY) headers['x-sync-key'] = SYNC_KEY;

  console.log(`[pml-only-sync] 送信: run=${pmeta.run_id} rows=${rows.length} fba=${pmeta.fba_source_kind || 'daily'}${pmeta.fba_fetched_at ? ' @' + pmeta.fba_fetched_at : ''}`);
  const resp = await fetch(`${RENDER_URL}/api/sync`, {
    method: 'POST', headers,
    body: JSON.stringify({
      pml_snapshot: {
        run_id: pmeta.run_id, status: pmeta.status, as_of_date: pmeta.as_of_date,
        generated_at: pmeta.generated_at, payload_checksum: pmeta.payload_checksum, row_count: pmeta.row_count,
        src_ne_products_synced_at: pmeta.src_ne_products_synced_at, src_velocity_as_of: pmeta.src_velocity_as_of,
        src_fba_business_date: pmeta.src_fba_business_date, src_reorder_updated_at: pmeta.src_reorder_updated_at,
        ne_fba_overlap: pmeta.ne_fba_overlap,
        fba_source_kind: pmeta.fba_source_kind, fba_source_run_id: pmeta.fba_source_run_id,
        fba_fetched_at: pmeta.fba_fetched_at, fba_latest_row_count: pmeta.fba_latest_row_count,
        rows,
      },
    }),
    signal: AbortSignal.timeout(120000),
  });
  if (!resp.ok) {
    const err = await resp.text().catch(() => '');
    throw new Error(`pml-only sync: HTTP ${resp.status} ${err.slice(0, 200)}`);
  }
  await resp.json().catch(() => ({}));

  // 検証: mirror の published run_id + 件数が一致 (fail-closed)
  const statusRes = await fetch(`${RENDER_URL}/api/status`, { signal: AbortSignal.timeout(30000) });
  const status = await statusRes.json();
  if (status.pml_published_run_id !== pmeta.run_id || (status.pml_snapshot_count ?? 0) !== rows.length) {
    throw new Error(`pml-only sync 検証失敗: mirror run=${status.pml_published_run_id}(${status.pml_snapshot_count}件) / sent run=${pmeta.run_id}(${rows.length}件)`);
  }
  console.log(`[pml-only-sync] ✅ 検証OK run=${pmeta.run_id} rows=${rows.length}`);
  return { state: 'sent', run_id: pmeta.run_id, count: rows.length, fba_source_kind: pmeta.fba_source_kind, fba_fetched_at: pmeta.fba_fetched_at };
}

// 単体実行
import { initDB } from './db.js';
const isMain = process.argv[1]?.includes('sync-to-render');
if (isMain) {
  await initDB();
  const pmlOnly = process.argv.includes('--pml-only');
  const result = pmlOnly ? await syncPmlSnapshotOnly() : await syncToRender();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(result.ok === false ? 1 : 0);
}
