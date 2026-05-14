/**
 * SKUマスタ → 商品コード変換テーブル 直接追記
 *
 * 用途:
 *   bfaith-portal「マスタ登録」(m_sku_master) に登録済みで、このスプレッドシートの
 *   「商品コード変換テーブル」シート (A列に SKU) にまだ載っていない SKU を、
 *   毎朝 1 回検出して **「商品コード変換テーブル」本体に直接追記** する。
 *
 * 追記時の列マッピング:
 *   A列: seller_sku
 *   C列: 商品名
 *   D列: NE商品コード
 *   E列: 数量
 *   B列, F列以降: 触らない (既存セル保護)
 *
 * セット商品 (1 SKU が複数 NE コード) は components.length 行に展開して書き込む。
 * 同じ A列 SKU が既存にあれば skip (重複防止)。
 *
 * セットアップ (Script Properties):
 *   MIRROR_API_BASE     = https://bfaith-portal.onrender.com/apps/mirror
 *   MIRROR_READ_TOKEN   = (Render env の値と同じ)
 *   SOURCE_SHEET_NAME   = 商品コード変換テーブル        (差分判定 + 追記先 を兼ねる)
 *   SOURCE_SKU_COLUMN   = 1                              (SKU が入っている列番号、A列なら 1)
 *   SOURCE_HEADER_ROWS  = 1                              (ヘッダ行数。0 なら全行を SKU として扱う)
 *   WRITE_MODE          = dry_run                        (★必須★ dry_run | live。最初は必ず dry_run)
 *
 * トリガー:
 *   関数: runDailyCheck / 時間主導型 / 日付ベース / 午前7時〜8時
 *   (miniPC daily-sync 07:00 完了後に動かす)
 *
 * 安全装置:
 *   - WRITE_MODE=dry_run のときは Logger.log でシミュレートのみ、シート無変更
 *   - LockService で多重起動防止
 *   - mirror freshness (sku_master 最終同期 26h 超え) なら throw
 *   - レスポンス契約検証 (since_days = 7、items 配列)
 *   - 重複チェック: 既に A列にある seller_sku は skip
 *   - 一度に追記する行数の上限 (ML_MAX_NEW_ROWS) を超えたら throw (誤爆防止)
 *
 * Codex review (PR #124 round 1〜5) の方針:
 *   - WAREHOUSE_API_KEY は絶対に Script Properties に入れない (Render の read-only token のみ)
 *   - 失敗黙殺禁止 — throw で Apps Script 実行履歴のエラー欄に出す
 */

const SINCE_DAYS = 7;
const MIRROR_FRESHNESS_MAX_HOURS = 26;
const ML_MAX_NEW_ROWS = 500; // 1 回の実行で追記する行数の上限。超えたら throw

function runDailyCheck() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000)) {
    throw new Error('LockService: 他の実行が走っているためスキップ');
  }
  try {
    main_();
  } finally {
    lock.releaseLock();
  }
}

function main_() {
  const props = PropertiesService.getScriptProperties();
  const base = props.getProperty('MIRROR_API_BASE');
  const token = props.getProperty('MIRROR_READ_TOKEN');
  const sourceSheetName = props.getProperty('SOURCE_SHEET_NAME') || '商品コード変換テーブル';
  const sourceSkuColumn = parseInt(props.getProperty('SOURCE_SKU_COLUMN') || '1', 10);
  const sourceHeaderRows = parseInt(props.getProperty('SOURCE_HEADER_ROWS') || '1', 10);
  const writeMode = (props.getProperty('WRITE_MODE') || 'dry_run').toLowerCase();

  if (!base) throw new Error('Script Property MIRROR_API_BASE が未設定');
  if (!token) throw new Error('Script Property MIRROR_READ_TOKEN が未設定');
  if (!(sourceSkuColumn >= 1)) throw new Error('SOURCE_SKU_COLUMN は 1 以上の整数');
  if (writeMode !== 'dry_run' && writeMode !== 'live') {
    throw new Error('WRITE_MODE は dry_run か live のどちらか: ' + writeMode);
  }

  // 1. Render mirror から「直近7日に登録/更新された SKU + components」を取得
  const url = base.replace(/\/+$/, '') + '/api/sku-master/recent-missing-candidates';
  const res = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { 'x-read-token': token },
    muteHttpExceptions: true,
    followRedirects: false,
  });
  const code = res.getResponseCode();
  const body = res.getContentText();
  if (code !== 200) {
    throw new Error('Mirror API error: HTTP ' + code + ' / body=' + body.slice(0, 300));
  }
  const data = JSON.parse(body);

  // 2. レスポンス契約検証 (server 側 drift 検知)
  if (data.since_days !== SINCE_DAYS) {
    throw new Error('Mirror endpoint contract drift: since_days=' + data.since_days + ' (expected ' + SINCE_DAYS + ')');
  }
  if (!Array.isArray(data.items)) {
    throw new Error('Mirror endpoint contract drift: items is not array');
  }

  // 3. mirror freshness 検証 (両系列を見る)
  const masterSync = data.mirror_sku_master_synced_at;
  const overallSync = data.mirror_last_synced_at;
  if (!masterSync) throw new Error('mirror_sku_master_synced_at が空 — Render mirror に sku_master が未投入の可能性');
  if (!overallSync) throw new Error('mirror_last_synced_at が空 — Render mirror の同期メタが未取得');
  const masterAge = ageHours_(masterSync);
  const overallAge = ageHours_(overallSync);
  if (masterAge === null) throw new Error('mirror_sku_master_synced_at がパース不能: ' + masterSync);
  if (overallAge === null) throw new Error('mirror_last_synced_at がパース不能: ' + overallSync);
  if (masterAge > MIRROR_FRESHNESS_MAX_HOURS) {
    throw new Error('Mirror が stale (sku_master): 最終同期 ' + masterSync + ' (' + masterAge.toFixed(1) + 'h 前)');
  }
  if (overallAge > MIRROR_FRESHNESS_MAX_HOURS) {
    throw new Error('Mirror が stale (全体): 最終同期 ' + overallSync + ' (' + overallAge.toFixed(1) + 'h 前)');
  }

  const masterItems = data.items;

  // 4. シート側「商品コード変換テーブル」既存 SKU の集合を作る (正規化済み)
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName(sourceSheetName);
  if (!sourceSheet) throw new Error('参照シートが見つからない: ' + sourceSheetName);
  const lastRow = sourceSheet.getLastRow();
  const existingSkuSet = new Set();
  if (lastRow > sourceHeaderRows) {
    const range = sourceSheet.getRange(sourceHeaderRows + 1, sourceSkuColumn, lastRow - sourceHeaderRows, 1);
    const vals = range.getValues();
    for (const row of vals) {
      const norm = normalizeSku_(row[0]);
      if (norm) existingSkuSet.add(norm);
    }
  }

  // 5. 差分計算 + components 展開
  //    各 missing item の components[] を 1 行ずつに展開
  //    components が空の場合は 1 行だけ追加 (NE/数量は空)
  const missingItems = [];
  for (const it of masterItems) {
    const norm = normalizeSku_(it.seller_sku);
    if (!norm) continue;
    if (existingSkuSet.has(norm)) continue;
    missingItems.push(it);
  }

  // 行展開: [A列, B列(空), C列, D列, E列] の 5 列ぴったり
  const newRows = [];
  for (const it of missingItems) {
    const sku = String(it.seller_sku ?? '');
    const name = String(it['商品名'] ?? '');
    const comps = Array.isArray(it.components) ? it.components : [];
    if (comps.length === 0) {
      // components が無い (= mirror_sku_resolved に未投入の SKU) → SKU + 商品名のみ追記
      newRows.push([sku, '', name, '', '']);
    } else {
      for (const c of comps) {
        newRows.push([sku, '', name, String(c.ne_code ?? ''), c.quantity ?? '']);
      }
    }
  }

  if (newRows.length > ML_MAX_NEW_ROWS) {
    throw new Error('追記行数 ' + newRows.length + ' が上限 ' + ML_MAX_NEW_ROWS + ' 超過。誤爆防止のため停止');
  }

  // 6. 書き込み (dry_run なら log のみ、live ならシート追記)
  const summary = {
    write_mode: writeMode,
    fetched_master_skus: masterItems.length,
    existing_skus_in_sheet: existingSkuSet.size,
    missing_skus: missingItems.length,
    new_rows_to_write: newRows.length,
    mirror_sku_master_synced_at: masterSync,
  };
  console.log(JSON.stringify(summary));

  if (newRows.length === 0) {
    console.log('追記なし、終了');
    return;
  }

  if (writeMode === 'dry_run') {
    console.log('[DRY-RUN] 以下の行を追記する予定 (実際には書き込まない):');
    for (const r of newRows) {
      console.log('  A=' + r[0] + ' | C=' + r[2] + ' | D=' + r[3] + ' | E=' + r[4]);
    }
    console.log('[DRY-RUN] 本番に切り替えるには Script Property WRITE_MODE=live にしてください');
    return;
  }

  // live 書き込み:
  //   - 1〜5 列目を一括 setValues で書く (B列は空文字'')
  //   - 既存の F列以降は触らない
  //   - 追記する行は元々 lastRow より下なので、B列の空文字上書きで既存値破壊なし
  const startRow = lastRow + 1;
  const range = sourceSheet.getRange(startRow, 1, newRows.length, 5);
  range.setValues(newRows);
  console.log('[LIVE] ' + newRows.length + ' 行を行 ' + startRow + ' から追記しました');
}

function normalizeSku_(v) {
  if (v === null || v === undefined) return '';
  return String(v).replace(/　/g, ' ').trim().toLowerCase();
}

/** 'YYYY-MM-DD HH:MM:SS' (UTC) を Date に */
function parseSyncedAt_(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
}

function ageHours_(s) {
  const d = parseSyncedAt_(s);
  if (!d) return null;
  return (Date.now() - d.getTime()) / 36e5;
}
