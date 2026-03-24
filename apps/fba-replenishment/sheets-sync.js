/**
 * Google Sheets同期 — SKUマッピング取得
 *
 * スプレッドシート「商品コード変換テーブル」シートのA〜E列を取得し、
 * sku_mappingテーブルに全件同期する。
 *
 * A: sku (Amazon SKU)
 * B: asin
 * C: 商品名（作業指示含む）
 * D: NE商品コード（= ロジザードの商品ID）
 * E: 数量（セット商品の場合の個数）
 * ...
 * M: FBA以外 直近7日 売上数（ARRAYFORMULA+VLOOKUP）
 * N: FBA以外 直近30日 売上数（ARRAYFORMULA+VLOOKUP）
 *
 * 同じSKUが複数行 = セット商品（例: SKU-A → NE商品X x1 + NE商品Y x2）
 */
import { google } from 'googleapis';
import { upsertSkuMappings } from './db.js';

const SPREADSHEET_ID = process.env.FBA_SPREADSHEET_ID || '1NruozyuL_lwdnk3WqtlRvwpB1frrbqSRVEDN9l6Uh50';
const SHEET_NAME = '商品コード変換テーブル';
const RANGE = `${SHEET_NAME}!A:N`;

async function getAuth() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です');

  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  return auth;
}

/**
 * スプレッドシートからSKUマッピングを取得してDBに同期
 */
export async function syncSkuMappings() {
  console.log('[Sheets] SKUマッピング同期開始...');

  const auth = await getAuth();
  const sheets = google.sheets({ version: 'v4', auth });

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: RANGE,
  });

  const rows = response.data.values;
  if (!rows || rows.length < 2) {
    throw new Error('スプレッドシートにデータがありません');
  }

  // ヘッダー行をスキップ
  const dataRows = rows.slice(1).filter(r => r[0]); // SKUが空の行を除外

  // SKUごとにグループ化（セット商品対応）
  const skuGroups = {};
  for (const row of dataRows) {
    const sku = (row[0] || '').trim();
    const asin = (row[1] || '').trim();
    const productName = (row[2] || '').trim();
    const neCode = (row[3] || '').trim();
    const qty = parseInt(row[4] || 1);
    // M列(index 12) = FBA以外7日売上, N列(index 13) = FBA以外30日売上
    const nonFbaSales7d = parseInt(row[12] || 0);
    const nonFbaSales30d = parseInt(row[13] || 0);

    if (!sku) continue;

    if (!skuGroups[sku]) {
      skuGroups[sku] = {
        amazon_sku: sku,
        asin: asin,
        product_name: productName,
        non_fba_sales_7d: nonFbaSales7d,
        non_fba_sales_30d: nonFbaSales30d,
        components: [],
      };
    }

    // セット構成を追加
    if (neCode) {
      skuGroups[sku].components.push({
        ne_code: neCode,
        qty: qty,
      });
    }
  }

  // DB用のマッピングデータに変換
  const mappings = Object.values(skuGroups).map(group => {
    const isSet = group.components.length > 1;
    const primaryComponent = group.components[0] || {};

    return {
      amazon_sku: group.amazon_sku,
      asin: group.asin,
      product_name: group.product_name,
      ne_code: primaryComponent.ne_code || null,
      logizard_code: primaryComponent.ne_code || null, // NE商品コード = ロジザード商品ID
      is_set: isSet,
      set_components: isSet ? group.components : null,
      non_fba_sales_7d: group.non_fba_sales_7d || 0,
      non_fba_sales_30d: group.non_fba_sales_30d || 0,
    };
  });

  // DB同期（全件UPSERT）
  const count = upsertSkuMappings(mappings);
  console.log(`[Sheets] SKUマッピング同期完了: ${count}件 (セット商品: ${mappings.filter(m => m.is_set).length}件)`);

  return {
    total: count,
    sets: mappings.filter(m => m.is_set).length,
    singles: mappings.filter(m => !m.is_set).length,
  };
}
