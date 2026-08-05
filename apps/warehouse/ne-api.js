/**
 * NE（ネクストエンジン）API連携 — warehouse.db自動投入
 *
 * 機能:
 *   1. 認証（初回 or トークン期限切れ時）
 *   2. 商品マスタ取得 → raw_ne_products UPSERT
 *   3. 受注データ取得 → raw_ne_orders INSERT
 *
 * 使い方:
 *   node apps/warehouse/ne-api.js auth <callback_url>  → 初回認証
 *   node apps/warehouse/ne-api.js products             → 商品マスタ全件取得
 *   node apps/warehouse/ne-api.js orders [days]        → 受注データ取得（デフォルト7日）
 *   node apps/warehouse/ne-api.js sync                 → 商品マスタ + 受注7日分
 *
 * トークン:
 *   data/ne-tokens.json に保存。APIレスポンスで自動更新される。
 *   2日以内の周期で実行すればトークンが切れない。
 *
 * 料金:
 *   月1,000回以内は無料。日次運用で約240回/月の見込み。
 */
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { initDB, getDB, updateSyncMeta } from './db.js';
import { makeNeOrdersUpserter } from './ne-orders-upsert.js';
import { makeNeOrderBaseUpserter, toOrderBaseRow, NE_ORDER_BASE_FIELDS } from './ne-order-base-upsert.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const TOKEN_FILE = path.join(DATA_DIR, 'ne-tokens.json');

const NE_API_BASE = 'https://api.next-engine.org';
const CLIENT_ID = process.env.NE_CLIENT_ID;
const CLIENT_SECRET = process.env.NE_CLIENT_SECRET;
const REDIRECT_URI = process.env.NE_REDIRECT_URI || 'https://localhost:3000/callback';

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }

// ─── トークン管理 ───

function loadTokens() {
  try {
    return JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf-8'));
  } catch {
    return null;
  }
}

function saveTokens(tokens) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
  console.log('[NE] トークン保存完了');
}

// ─── 認証 ───

async function authenticate(callbackUrl) {
  // callbackUrl: https://localhost:3000/callback?uid=xxx&state=yyy
  const url = new URL(callbackUrl);
  const uid = url.searchParams.get('uid');
  const state = url.searchParams.get('state');

  if (!uid || !state) {
    throw new Error('callback URLにuid/stateが含まれていません');
  }

  console.log('[NE] 認証開始: uid=' + uid);

  const params = new URLSearchParams({
    uid,
    state,
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
  });

  const res = await fetch(NE_API_BASE + '/api_neauth', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
  });

  const data = await res.json();
  if (data.result !== 'success') {
    throw new Error('認証失敗: ' + JSON.stringify(data));
  }

  const tokens = {
    access_token: data.access_token,
    refresh_token: data.refresh_token,
    updated_at: now(),
  };
  saveTokens(tokens);
  console.log('[NE] 認証成功');
  return tokens;
}

// ─── API呼び出し ───

// 他モジュール (ne-sync-runner 等) から再利用するため export 化 (2026-06-06 構成 4 PR)
export { callNE, loadTokens, saveTokens };
async function callNE(endpoint, params = {}) {
  const tokens = loadTokens();
  if (!tokens) throw new Error('トークンがありません。先に認証してください: node ne-api.js auth <callback_url>');

  const body = new URLSearchParams({
    access_token: tokens.access_token,
    refresh_token: tokens.refresh_token,
    wait_flag: '1',
    ...params,
  });

  const res = await fetch(NE_API_BASE + endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  const data = await res.json();

  // トークン自動更新
  if (data.access_token && data.refresh_token) {
    saveTokens({
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      updated_at: now(),
    });
  }

  if (data.result === 'error') {
    throw new Error('NE API エラー: ' + (data.message || JSON.stringify(data)));
  }

  return data;
}

// ─── 商品マスタ取得 ───

async function fetchProducts() {
  console.log('[NE] 商品マスタ取得開始');
  await initDB();
  const db = getDB();
  const ts = now();

  const fields = 'goods_id,goods_name,goods_supplier_id,goods_cost_price,goods_selling_price,goods_merchandise_name,goods_representation_id,goods_location,goods_delivery_name,goods_lot,goods_last_time_supplied_date,goods_tag,goods_creation_date,stock_quantity,stock_allocation_quantity,goods_last_modified_date,goods_tax_rate,stock_remaining_order_quantity';

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_products (
      商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分,
      代表商品コード, ロケーションコード, 配送業者, 発注ロット単位,
      最終仕入日, 商品分類タグ, 作成日, 在庫数, 引当数,
      最終更新日, 消費税率, 発注残数, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  let offset = 0;
  let total = 0;
  const LIMIT = 1000;

  while (true) {
    const data = await callNE('/api_v1_master_goods/search', {
      fields,
      limit: String(LIMIT),
      offset: String(offset),
    });

    const items = data.data || [];
    if (items.length === 0) break;

    const tx = db.transaction(() => {
      for (const item of items) {
        const code = (item.goods_id || '').toLowerCase();
        if (!code) continue;
        stmt.run(
          code,
          item.goods_name || '',
          item.goods_supplier_id || '',
          parseFloat(item.goods_cost_price) || 0,
          parseFloat(item.goods_selling_price) || 0,
          item.goods_merchandise_name || '',
          (item.goods_representation_id || '').toLowerCase(),
          item.goods_location || '',
          item.goods_delivery_name || '',
          parseInt(item.goods_lot) || 0,
          item.goods_last_time_supplied_date || '',
          item.goods_tag || '',
          item.goods_creation_date || '',
          parseInt(item.stock_quantity) || 0,
          parseInt(item.stock_allocation_quantity) || 0,
          item.goods_last_modified_date || '',
          parseFloat(item.goods_tax_rate) || 0,
          parseInt(item.stock_remaining_order_quantity) || 0,
          ts
        );
        total++;
      }
    });
    tx();

    console.log(`[NE] 商品マスタ: ${total}件取得 (offset: ${offset})`);
    offset += LIMIT;

    if (items.length < LIMIT) break;
  }

  updateSyncMeta('ne_api_products_last', now());
  updateSyncMeta('ne_api_products_count', String(total));
  console.log(`[NE] 商品マスタ取得完了: ${total}件`);
  return total;
}

// ─── セット商品取得 ───

async function fetchSetProducts() {
  console.log('[NE] セット商品取得開始');
  await initDB();
  const db = getDB();
  const ts = now();

  const fields = 'set_goods_id,set_goods_name,set_goods_selling_price,set_goods_detail_goods_id,set_goods_detail_quantity,set_goods_representation_id';

  // 全ページを先にメモリへ取得してから、DELETE + INSERT を単一トランザクションで実行する。
  // DELETE を先に commit してからページ毎に挿入すると、途中の API エラー / 親 timeout kill で
  // raw_ne_set_products が空 or 部分状態のまま残り、下流の m_products rebuild が
  // セット商品を単品扱いで通してしまう (2026-07-19 構造監査 H-1)。
  const allItems = [];
  let offset = 0;
  const LIMIT = 1000;

  while (true) {
    const data = await callNE('/api_v1_master_setgoods/search', {
      fields,
      limit: String(LIMIT),
      offset: String(offset),
    });

    const items = data.data || [];
    if (items.length === 0) break;
    allItems.push(...items);

    console.log(`[NE] セット商品: ${allItems.length}件取得 (offset: ${offset})`);
    offset += LIMIT;

    if (items.length < LIMIT) break;
  }

  // tx 前に有効行へ正規化。「API は要素を返すが必須キーが全滅」(仕様変更等) でも
  // 空 commit しないよう、有効行 0 件 + 既存データありなら洗い替えせず中断 (全消し防止)
  const validRows = [];
  for (const item of allItems) {
    const setCode = (item.set_goods_id || '').toLowerCase();
    const childCode = (item.set_goods_detail_goods_id || '').toLowerCase();
    if (!setCode || !childCode) continue;
    validRows.push([
      setCode,
      item.set_goods_name || '',
      parseFloat(item.set_goods_selling_price) || 0,
      childCode,
      parseInt(item.set_goods_detail_quantity) || 1,
      0,  // セット在庫数（APIでは取得不可、stock APIが必要）
      (item.set_goods_representation_id || '').toLowerCase(),
      ts,
    ]);
  }
  if (validRows.length === 0) {
    const cur = db.prepare('SELECT COUNT(*) AS c FROM raw_ne_set_products').get().c;
    if (cur > 0) {
      throw new Error(`セット商品 API の有効行が 0 件 (raw ${allItems.length} 件 / 現行 ${cur} 件保持)。全消し回避のため洗い替えを中断`);
    }
  }

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_set_products (
      セット商品コード, セット商品名, セット販売価格,
      商品コード, 数量, セット在庫数, 代表商品コード, synced_at
    ) VALUES (?,?,?,?,?,?,?,?)
  `);

  let total = 0;
  const tx = db.transaction(() => {
    db.exec('DELETE FROM raw_ne_set_products');
    for (const row of validRows) {
      stmt.run(...row);
      total++;
    }
  });
  tx();

  updateSyncMeta('ne_api_setproducts_last', now());
  updateSyncMeta('ne_api_setproducts_count', String(total));
  console.log(`[NE] セット商品取得完了: ${total}件`);
  return total;
}

// ─── 受注データ取得（base + row JOIN）───

async function fetchOrders(days = 7) {
  console.log(`[NE] 受注データ取得開始（直近${days}日）`);
  await initDB();
  const db = getDB();
  const ts = now();

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  const startStr = startDate.toISOString().slice(0, 10) + ' 00:00:00';
  const endStr = endDate.toISOString().slice(0, 10) + ' 23:59:59';

  // Step 1: 受注ベース（伝票レベル）を取得 → Mapに保持 + raw_ne_order_base へ UPSERT
  //   fields に配送方法 (delivery_id/name) を含むので、明細 JOIN 用の情報を取るついでに
  //   伝票粒度テーブルも同じレスポンスから埋まる (API 呼び出しは増えない)。
  console.log('[NE] Step 1: 受注ベース取得');
  const baseMap = new Map(); // 伝票番号 → base情報
  const upsertOrderBase = makeNeOrderBaseUpserter(db);
  let baseInserted = 0;
  let baseUpdated = 0;

  let baseOffset = 0;
  while (true) {
    const data = await callNE('/api_v1_receiveorder_base/search', {
      fields: NE_ORDER_BASE_FIELDS,
      'receive_order_date-gte': startStr,
      'receive_order_date-lte': endStr,
      limit: '1000',
      offset: String(baseOffset),
    });
    const items = data.data || [];
    if (items.length === 0) break;

    for (const item of items) {
      const id = item.receive_order_id || '';
      if (id) baseMap.set(id, item);
    }
    const tx = db.transaction(() => {
      for (const item of items) {
        const row = toOrderBaseRow(item);
        if (!row) continue;
        const outcome = upsertOrderBase(row, ts);
        if (outcome === 'inserted') baseInserted++;
        else if (outcome === 'updated') baseUpdated++;
      }
    });
    tx();
    console.log(`[NE] 受注ベース: ${baseMap.size}件 (offset: ${baseOffset})`);
    baseOffset += 1000;
    if (items.length < 1000) break;
  }
  console.log(`[NE] 受注ベース保存: ${baseInserted}件挿入, ${baseUpdated}件更新`);

  // Step 2: 受注明細を取得し、baseとJOINしてINSERT
  console.log('[NE] Step 2: 受注明細取得 + JOIN');
  const rowFields = 'receive_order_row_receive_order_id,receive_order_row_shop_cut_form_id,receive_order_row_no,receive_order_row_goods_id,receive_order_row_goods_name,receive_order_row_goods_option,receive_order_row_quantity,receive_order_row_stock_allocation_quantity,receive_order_row_sub_total_price,receive_order_row_cancel_flag';

  // 監査 S-5/V-3: INSERT OR IGNORE → 状態更新つき UPSERT (キャンセル・状態変化を反映)
  const upsertOrderRow = makeNeOrdersUpserter(db);

  let rowOffset = 0;
  let total = 0;
  let inserted = 0;
  let updated = 0;
  let unchanged = 0;
  let errors = 0;

  while (true) {
    const data = await callNE('/api_v1_receiveorder_row/search', {
      fields: rowFields,
      'receive_order_date-gte': startStr,
      'receive_order_date-lte': endStr,
      limit: '1000',
      offset: String(rowOffset),
    });

    const items = data.data || [];
    if (items.length === 0) break;

    const tx = db.transaction(() => {
      for (const item of items) {
        const denpyo = item.receive_order_row_receive_order_id || '';
        const lineNo = parseInt(item.receive_order_row_no) || 0;
        if (!denpyo || !lineNo) continue;

        // baseからJOIN
        const base = baseMap.get(denpyo) || {};

        try {
          const outcome = upsertOrderRow(
            denpyo,
            item.receive_order_row_shop_cut_form_id || base.receive_order_shop_cut_form_id || '',
            base.receive_order_order_status_id || '',
            base.receive_order_order_status_name || '',
            // NE API は非キャンセル受注に cancel_type_id='0' (文字列) を返す。'0' は JS で
            // truthy のため、素の truthy 判定だと全 API 取込行が「キャンセル」に誤ラベルされる
            // (2026-07-06 実測: 伝票1485081 = 有効な新規受注で cancel_type_id="0"。106,740行が誤ラベル)。
            // 実害は f_sales には無し (フィルタは行レベルのキャンセル区分) だが、UPSERT 化後は
            // この列も更新されるため、誤ラベルの churn を止める。
            (base.receive_order_cancel_type_id && base.receive_order_cancel_type_id !== '0')
              ? 'キャンセル' : '有効な受注です。',
            base.receive_order_cancel_date || '',
            base.receive_order_date || '',
            base.receive_order_shop_id || '',
            base.receive_order_send_date || '',
            lineNo,
            '',  // レコードナンバー
            item.receive_order_row_cancel_flag === '1' ? 'キャンセル' : '有効',
            (item.receive_order_row_goods_id || '').toLowerCase(),
            item.receive_order_row_goods_name || '',
            item.receive_order_row_goods_option || '',
            parseInt(item.receive_order_row_quantity) || 0,
            parseInt(item.receive_order_row_stock_allocation_quantity) || 0,
            parseFloat(item.receive_order_row_sub_total_price) || 0,
            ts
          );
          if (outcome === 'inserted') inserted++;
          else if (outcome === 'updated') updated++;
          else unchanged++;
        } catch (e) {
          // UPSERT 化により重複は例外を投げない — ここに来るのは実エラー。
          // 無音 skip は PR #155 (¥0 が 2 週間潜伏) の轍なので必ず可視化する。
          errors++;
          if (errors <= 3) console.error(`[NE] 受注明細 UPSERT エラー (伝票=${denpyo} 行=${lineNo}): ${e.message}`);
        }
        total++;
      }
    });
    tx();

    console.log(`[NE] 受注明細: ${total}件処理, ${inserted}件挿入, ${updated}件更新 (offset: ${rowOffset})`);
    rowOffset += 1000;
    if (items.length < 1000) break;
  }

  console.log(`[NE] 受注データ取得完了: base ${baseMap.size}件, row ${total}件処理, ${inserted}件挿入, ${updated}件更新(キャンセル/状態変化), ${unchanged}件変化なし, ${errors}件エラー`);
  // Codex R1 high: 成功メタの更新は「エラー0件」を確認してから (失敗した同期を成功済みと記録しない)
  if (errors > 0) {
    throw new Error(`[NE] 受注明細 UPSERT で ${errors} 件のエラー (ログ先頭3件参照)`);
  }
  updateSyncMeta('ne_api_orders_last', now());
  updateSyncMeta('ne_api_orders_range', `${startStr} ~ ${endStr}`);
  return { total, inserted, updated, unchanged };
}

// ─── 受注ベース取得（出荷確定日ベース）───

/** JST の YYYY-MM-DD (toISOString は UTC なので +9h してから切り出す) */
function jstDate(offsetDays = 0) {
  const t = Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

/**
 * 出荷確定日ベースで受注ベース（伝票粒度）を取得し raw_ne_order_base へ UPSERT する。
 *
 * fetchOrders は「受注日」窓なので、受注から日が空いて出荷確定した伝票を取りこぼす
 * (2026-08-05 実測: 7月出荷確定分の 0.23% がラグ7日以上)。出荷件数を数える用途では
 * この取りこぼしがそのまま日次件数の欠損になるため、出荷確定日ベースでも取り直す。
 * 出荷確定済みの伝票は 1 日 800〜1,000 件程度なので、limit 10000 なら通常 API 1 回で済む。
 *
 * @param {number} days 何日前まで遡るか (既定 14。当日を含む)
 */
async function fetchOrderBaseByShipDate(days = 14) {
  console.log(`[NE] 受注ベース取得開始（出荷確定日ベース・直近${days}日）`);
  return fetchOrderBaseRange(`${jstDate(-(days - 1))} 00:00:00`, `${jstDate(0)} 23:59:59`);
}

/**
 * 更新日ベースで受注ベースを取得し raw_ne_order_base へ UPSERT する。
 *
 * 出荷確定日ベースの取得だけでは「いったん出荷確定した後で確定を取り消された伝票」を
 * 二度と拾えず、古い出荷確定日が残って件数が過大になる (Codex R1 high)。
 * NE の受注ベースには receive_order_last_modified_date があり、確定取消・出荷日訂正・
 * 配送方法変更のいずれでも更新されるので、更新日窓で取り直せば全部追随できる。
 * 実測 (2026-08-05): 直近 1.5 日で 3,852 件 = API 1 回。
 *
 * @param {number} days 何日前まで遡るか (既定 3)
 */
async function fetchOrderBaseByModified(days = 3) {
  console.log(`[NE] 受注ベース取得開始（更新日ベース・直近${days}日）`);
  const from = `${jstDate(-(days - 1))} 00:00:00`;
  const to = `${jstDate(0)} 23:59:59`;
  return fetchOrderBase(
    { 'receive_order_last_modified_date-gte': from, 'receive_order_last_modified_date-lte': to },
    { label: '更新日', splitRange: [from, to], splitKey: 'receive_order_last_modified_date' }
  );
}

const NE_BASE_LIMIT = 10000;

/**
 * 出荷確定日の任意区間で受注ベースを取得し raw_ne_order_base へ UPSERT する。
 * バックフィル (backfill-ne-order-base.js) からも使う。
 *
 * NE の受注検索はソート順を指定できない (実測: `-asc` 演算子は拒否される) ため、
 * offset ページングは取得中の更新でページ境界がずれて取りこぼす恐れがある (Codex R1 medium)。
 * そこで **1 レスポンスに収まらなかった区間は日付で二分割して取り直す**。
 * 1 日あたり 800〜1,000 件なので、実運用では分割は起きない (バックフィルの繁忙月のみ)。
 *
 * @param {string} startStr 'YYYY-MM-DD HH:MM:SS'
 * @param {string} endStr   'YYYY-MM-DD HH:MM:SS'
 * @param {{ updateMeta?: boolean }} [opts] updateMeta=false で sync_meta を汚さない
 *   (バックフィルは過去区間を舐めるだけなので「最終同期」を書き換えてはいけない)
 */
export async function fetchOrderBaseRange(startStr, endStr, opts = {}) {
  return fetchOrderBase(
    { 'receive_order_send_date-gte': startStr, 'receive_order_send_date-lte': endStr },
    { ...opts, label: '出荷確定日', splitRange: [startStr, endStr] }
  );
}

/**
 * 受注ベース取得の共通処理 (検索条件だけ差し替える)。
 * @param {Record<string,string>} where NE API の検索条件
 * @param {{ updateMeta?: boolean, label?: string, splitRange?: [string, string] }} opts
 */
async function fetchOrderBase(where, opts = {}) {
  const updateMeta = opts.updateMeta !== false;
  const label = opts.label || '受注ベース';
  await initDB();
  const db = getDB();
  const ts = now();
  const upsertOrderBase = makeNeOrderBaseUpserter(db);

  const stats = { fetched: 0, inserted: 0, updated: 0, unchanged: 0, requests: 0 };

  const save = (items) => {
    const tx = db.transaction(() => {
      for (const item of items) {
        const row = toOrderBaseRow(item);
        if (!row) continue;
        const outcome = upsertOrderBase(row, ts);
        if (outcome === 'inserted') stats.inserted++;
        else if (outcome === 'updated') stats.updated++;
        else stats.unchanged++;
        stats.fetched++;
      }
    });
    tx();
  };

  /** 1区間を取得。LIMIT に達したら日付で二分割して取り直す */
  const fetchWindow = async (cond, from, to, depth = 0) => {
    const data = await callNE('/api_v1_receiveorder_base/search', {
      fields: NE_ORDER_BASE_FIELDS, limit: String(NE_BASE_LIMIT), offset: '0', ...cond,
    });
    stats.requests++;
    const items = data.data || [];

    if (items.length < NE_BASE_LIMIT) {
      save(items);
      return;
    }
    // LIMIT ちょうど = 取りこぼしの可能性。日付で割れるなら割って取り直す
    const halves = from && to ? splitWindow(from, to) : null;
    if (!halves || depth >= 8) {
      // これ以上割れない (同一日 or 深すぎ) → offset ページングで拾い切る。
      // 順序保証がないので取りこぼしうる。起きたら日次件数が過少になるため必ず警告を出す。
      console.warn(`[NE] ⚠ ${label} 区間 ${from}〜${to} が1回で取り切れません (${NE_BASE_LIMIT}件上限)。offset ページングにフォールバックします`);
      save(items);
      let offset = NE_BASE_LIMIT;
      while (true) {
        const more = await callNE('/api_v1_receiveorder_base/search', {
          fields: NE_ORDER_BASE_FIELDS, limit: String(NE_BASE_LIMIT), offset: String(offset), ...cond,
        });
        stats.requests++;
        const rows = more.data || [];
        if (rows.length === 0) break;
        save(rows);
        if (rows.length < NE_BASE_LIMIT) break;
        offset += NE_BASE_LIMIT;
      }
      return;
    }
    const [a, b] = halves;
    console.log(`[NE] ${label} 区間を分割: ${a[0]}〜${a[1]} / ${b[0]}〜${b[1]}`);
    await fetchWindow({ ...cond, [`${opts.splitKey || 'receive_order_send_date'}-gte`]: a[0], [`${opts.splitKey || 'receive_order_send_date'}-lte`]: a[1] }, a[0], a[1], depth + 1);
    await fetchWindow({ ...cond, [`${opts.splitKey || 'receive_order_send_date'}-gte`]: b[0], [`${opts.splitKey || 'receive_order_send_date'}-lte`]: b[1] }, b[0], b[1], depth + 1);
  };

  const [rangeFrom, rangeTo] = opts.splitRange || [null, null];
  await fetchWindow(where, rangeFrom, rangeTo);

  console.log(`[NE] 受注ベース取得完了(${label}): ${stats.fetched}件処理, ${stats.inserted}件挿入, ${stats.updated}件更新, ${stats.unchanged}件変化なし (API ${stats.requests}回)`);
  if (updateMeta) {
    updateSyncMeta('ne_api_orderbase_last', now());
    updateSyncMeta('ne_api_orderbase_range', `${label}: ${rangeFrom || where['receive_order_last_modified_date-gte'] || ''} ~ ${rangeTo || ''}`);
  }
  return stats;
}

/**
 * 'YYYY-MM-DD HH:MM:SS' の区間を日付境界で2つに割る。同一日なら null (割れない)。
 * @returns {[[string,string],[string,string]] | null}
 */
export function splitWindow(startStr, endStr) {
  const d0 = startStr.slice(0, 10);
  const d1 = endStr.slice(0, 10);
  if (d0 === d1) return null;
  const t0 = Date.parse(`${d0}T00:00:00Z`);
  const t1 = Date.parse(`${d1}T00:00:00Z`);
  const mid = new Date(t0 + Math.floor((t1 - t0) / 2 / 86400000) * 86400000).toISOString().slice(0, 10);
  const midNext = new Date(Date.parse(`${mid}T00:00:00Z`) + 86400000).toISOString().slice(0, 10);
  return [
    [startStr, `${mid} 23:59:59`],
    [`${midNext} 00:00:00`, endStr],
  ];
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!CLIENT_ID || !CLIENT_SECRET) {
    console.error('[NE] 環境変数が不足: NE_CLIENT_ID, NE_CLIENT_SECRET');
    process.exit(1);
  }

  if (command === 'auth') {
    const callbackUrl = args[1];
    if (!callbackUrl) {
      console.log('使い方: node apps/warehouse/ne-api.js auth <callback_url>');
      console.log('');
      console.log('手順:');
      console.log('1. NEメイン画面 → 上部メニュー「アプリ」→「B-Faith データ連携」をクリック');
      console.log('2. リダイレクトされたURLをコピー');
      console.log('3. node apps/warehouse/ne-api.js auth "コピーしたURL"');
      process.exit(1);
    }
    await authenticate(callbackUrl);
  } else if (command === 'products') {
    await fetchProducts();
  } else if (command === 'orders') {
    const days = parseInt(args[1]) || 7;
    await fetchOrders(days);
  } else if (command === 'setproducts') {
    await fetchSetProducts();
  } else if (command === 'order-base') {
    const days = parseInt(args[1]) || 14;
    await fetchOrderBaseByShipDate(days);
  } else if (command === 'order-base-modified') {
    const days = parseInt(args[1]) || 3;
    await fetchOrderBaseByModified(days);
  } else if (command === 'sync') {
    await fetchProducts();
    await fetchSetProducts();
    await fetchOrders(7);
    // 出荷確定日ベースの取り直し (受注日窓から漏れた出荷を拾う。API 1回程度)
    await fetchOrderBaseByShipDate(14);
    // 更新日ベースの取り直し (確定取消・出荷日訂正・配送方法変更を追随。API 1回程度)
    await fetchOrderBaseByModified(3);
  } else {
    console.log('使い方:');
    console.log('  node apps/warehouse/ne-api.js auth <callback_url>  → 初回認証');
    console.log('  node apps/warehouse/ne-api.js products             → 商品マスタ取得');
    console.log('  node apps/warehouse/ne-api.js setproducts          → セット商品取得');
    console.log('  node apps/warehouse/ne-api.js orders [days]        → 受注データ取得');
    console.log('  node apps/warehouse/ne-api.js order-base [days]    → 受注ベース取得（出荷確定日ベース、既定14日）');
    console.log('  node apps/warehouse/ne-api.js order-base-modified [days] → 受注ベース取得（更新日ベース、既定3日）');
    console.log('  node apps/warehouse/ne-api.js sync                 → 全て（商品+セット+受注7日+受注ベース14日+更新分3日）');
    process.exit(1);
  }
}

// CLI として直接実行された時のみ main() を起動 (import 時は起動しない、2026-06-06 構成 4 PR)
const isDirectRun = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isDirectRun) {
  main().catch(e => {
    console.error('[NE] エラー:', e.message);
    process.exit(1);
  });
}
