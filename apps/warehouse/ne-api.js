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

  // 全件洗い替え
  db.exec('DELETE FROM raw_ne_set_products');

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_ne_set_products (
      セット商品コード, セット商品名, セット販売価格,
      商品コード, 数量, セット在庫数, 代表商品コード, synced_at
    ) VALUES (?,?,?,?,?,?,?,?)
  `);

  let offset = 0;
  let total = 0;
  const LIMIT = 1000;

  while (true) {
    const data = await callNE('/api_v1_master_setgoods/search', {
      fields,
      limit: String(LIMIT),
      offset: String(offset),
    });

    const items = data.data || [];
    if (items.length === 0) break;

    const tx = db.transaction(() => {
      for (const item of items) {
        const setCode = (item.set_goods_id || '').toLowerCase();
        const childCode = (item.set_goods_detail_goods_id || '').toLowerCase();
        if (!setCode || !childCode) continue;
        stmt.run(
          setCode,
          item.set_goods_name || '',
          parseFloat(item.set_goods_selling_price) || 0,
          childCode,
          parseInt(item.set_goods_detail_quantity) || 1,
          0,  // セット在庫数（APIでは取得不可、stock APIが必要）
          (item.set_goods_representation_id || '').toLowerCase(),
          ts
        );
        total++;
      }
    });
    tx();

    console.log(`[NE] セット商品: ${total}件取得 (offset: ${offset})`);
    offset += LIMIT;

    if (items.length < LIMIT) break;
  }

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

  // Step 1: 受注ベース（伝票レベル）を取得 → Mapに保持
  console.log('[NE] Step 1: 受注ベース取得');
  const baseFields = 'receive_order_id,receive_order_shop_cut_form_id,receive_order_date,receive_order_shop_id,receive_order_order_status_id,receive_order_order_status_name,receive_order_send_date,receive_order_cancel_type_id,receive_order_cancel_date';
  const baseMap = new Map(); // 伝票番号 → base情報

  let baseOffset = 0;
  while (true) {
    const data = await callNE('/api_v1_receiveorder_base/search', {
      fields: baseFields,
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
    console.log(`[NE] 受注ベース: ${baseMap.size}件 (offset: ${baseOffset})`);
    baseOffset += 1000;
    if (items.length < 1000) break;
  }

  // Step 2: 受注明細を取得し、baseとJOINしてINSERT
  console.log('[NE] Step 2: 受注明細取得 + JOIN');
  const rowFields = 'receive_order_row_receive_order_id,receive_order_row_shop_cut_form_id,receive_order_row_no,receive_order_row_goods_id,receive_order_row_goods_name,receive_order_row_goods_option,receive_order_row_quantity,receive_order_row_stock_allocation_quantity,receive_order_row_sub_total_price,receive_order_row_cancel_flag';

  const stmt = db.prepare(`
    INSERT OR IGNORE INTO raw_ne_orders (
      伝票番号, 受注番号, 受注状態区分, 受注状態, 受注キャンセル,
      受注キャンセル日, 受注日, 店舗コード, 出荷確定日,
      明細行番号, レコードナンバー, キャンセル区分,
      商品コード, 商品名, 商品OP, 受注数, 引当数, 小計金額, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  let rowOffset = 0;
  let total = 0;
  let inserted = 0;

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
          stmt.run(
            denpyo,
            item.receive_order_row_shop_cut_form_id || base.receive_order_shop_cut_form_id || '',
            base.receive_order_order_status_id || '',
            base.receive_order_order_status_name || '',
            base.receive_order_cancel_type_id ? 'キャンセル' : '有効な受注です。',
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
          inserted++;
        } catch {
          // 重複はスキップ
        }
        total++;
      }
    });
    tx();

    console.log(`[NE] 受注明細: ${total}件処理, ${inserted}件挿入 (offset: ${rowOffset})`);
    rowOffset += 1000;
    if (items.length < 1000) break;
  }

  updateSyncMeta('ne_api_orders_last', now());
  updateSyncMeta('ne_api_orders_range', `${startStr} ~ ${endStr}`);
  console.log(`[NE] 受注データ取得完了: base ${baseMap.size}件, row ${total}件処理, ${inserted}件挿入`);
  return { total, inserted };
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
  } else if (command === 'sync') {
    await fetchProducts();
    await fetchSetProducts();
    await fetchOrders(7);
  } else {
    console.log('使い方:');
    console.log('  node apps/warehouse/ne-api.js auth <callback_url>  → 初回認証');
    console.log('  node apps/warehouse/ne-api.js products             → 商品マスタ取得');
    console.log('  node apps/warehouse/ne-api.js setproducts          → セット商品取得');
    console.log('  node apps/warehouse/ne-api.js orders [days]        → 受注データ取得');
    console.log('  node apps/warehouse/ne-api.js sync                 → 全て（商品+セット+受注7日分）');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('[NE] エラー:', e.message);
  process.exit(1);
});
