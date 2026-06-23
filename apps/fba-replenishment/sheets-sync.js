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
import { upsertSkuMappings, saveNonFbaSalesSnapshot, replaceDodaiMaster, getDodaiMaster } from './db.js';

const SPREADSHEET_ID = process.env.FBA_SPREADSHEET_ID || '1NruozyuL_lwdnk3WqtlRvwpB1frrbqSRVEDN9l6Uh50';
const SHEET_NAME = '商品コード変換テーブル';
const RANGE = `${SHEET_NAME}!A:N`;

// 土台商品マスタ (ピッキング準備): SKU が「土台商品」かを判定するシート。
// GAS DODAI_SS_ID / DODAI_SHEET_NAME と同一。A列=SKU, F列=1 なら土台商品。
const DODAI_SPREADSHEET_ID = process.env.FBA_DODAI_SPREADSHEET_ID || '1_j18AwLYoic92ZYN7S7xjQQKHsGu010bq_7TgHpjqzQ';
const DODAI_SHEET_NAME = process.env.FBA_DODAI_SHEET_NAME || 'FBA土台商品管理シート';
const DODAI_RANGE = `${DODAI_SHEET_NAME}!A:F`;

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
    const primaryComponent = group.components[0] || {};
    // 複数商品コード or 単品でもqty>1 → セット扱い
    const hasMultipleComponents = group.components.length > 1;
    const hasQtyMultiplier = group.components.length === 1 && (primaryComponent.qty || 1) > 1;
    const isSet = hasMultipleComponents || hasQtyMultiplier;

    return {
      amazon_sku: group.amazon_sku,
      asin: group.asin,
      product_name: group.product_name,
      ne_code: primaryComponent.ne_code || null,
      logizard_code: primaryComponent.ne_code || null, // NE商品コード = ロジザード商品ID
      is_set: isSet,
      // 常にcomponentsを保存（qty情報を保持するため）
      set_components: group.components.length > 0 ? group.components : null,
      non_fba_sales_7d: group.non_fba_sales_7d || 0,
      non_fba_sales_30d: group.non_fba_sales_30d || 0,
    };
  });

  // DB同期（全件UPSERT）
  const count = upsertSkuMappings(mappings);

  // 他CH売上スナップショット保存（UPSERT: 手動同期時はその日のデータを上書き）
  const snapCount = saveNonFbaSalesSnapshot(mappings);
  console.log(`[Sheets] SKUマッピング同期完了: ${count}件 (セット商品: ${mappings.filter(m => m.is_set).length}件, 売上スナップショット: ${snapCount}件)`);

  return {
    total: count,
    sets: mappings.filter(m => m.is_set).length,
    singles: mappings.filter(m => !m.is_set).length,
    snapshots: snapCount,
  };
}

/**
 * 土台商品マスタをスプレッドシートから取得して picking_dodai_master に同期。
 * GAS buildDodaiSet_ 相当: A列=SKU, F列(index5)=1 なら土台商品。
 * 読み取り成功時のみ DB を全置換する (失敗時は既存値を保持 = fail-safe)。
 * 誤設定で全消去しないためのガード (Codex High):
 *   - 0 件 → throw (列ずれ/権限/タブ違いの可能性が高い)
 *   - 前回比 50% 未満への急減 → throw (err.needConfirm=true)。手動は force:true で承認可、
 *     cron は force なしのため急減時は置換せず既存を保持 (= 自動全消去しない)。
 * @param {{ force?: boolean }} opts force=true で急減ガードを無視して置換 (手動の明示承認時のみ)
 */
export async function syncDodaiMaster({ force = false } = {}) {
  console.log('[Sheets] 土台商品マスタ同期開始...');
  const auth = await getAuth();
  const sheets = google.sheets({ version: 'v4', auth });

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: DODAI_SPREADSHEET_ID,
    range: DODAI_RANGE,
  });
  const rows = response.data.values || [];

  // ヘッダ有無に関わらず全行走査 (ヘッダ行は F列≠'1' で自然に除外される)。SKU 重複は排除。
  const seen = new Set();
  const skuRows = [];
  for (const row of rows) {
    const sku = String(row?.[0] ?? '').trim();
    const flag = String(row?.[5] ?? '').trim();
    if (sku && flag === '1' && !seen.has(sku)) {
      seen.add(sku);
      skuRows.push({ sku });
    }
  }

  if (skuRows.length === 0) {
    throw new Error(`土台商品(F列=1)が0件でした。シート「${DODAI_SHEET_NAME}」の列構成/共有設定を確認してください（既存マスタは保持しました）。`);
  }

  // 急減ガード: 前回件数から半減以下は誤設定(列ずれ/タブ違い/フィルタ崩れ)の可能性。既存を保持して承認を求める。
  const prev = (getDodaiMaster() || []).length;
  if (!force && prev > 0 && skuRows.length < prev * 0.5) {
    const err = new Error(`土台商品が前回(${prev}件)から大きく減少(${skuRows.length}件)します。シートの列/タブ/共有設定を確認してください。意図的な場合は再実行で承認してください（既存マスタは保持しました）。`);
    err.needConfirm = true;
    err.prev = prev;
    err.count = skuRows.length;
    throw err;
  }

  const r = replaceDodaiMaster(skuRows, { filename: `sheet:${DODAI_SHEET_NAME}`, uploadedBy: 'sheet-sync' });
  console.log(`[Sheets] 土台商品マスタ同期完了: ${r.count}件 (前回 ${r.prev}件)`);
  return r;
}
