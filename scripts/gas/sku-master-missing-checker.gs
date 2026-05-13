/**
 * SKUマスタ × 商品コード変換テーブル 差分チェッカー
 *
 * 用途:
 *   bfaith-portal「マスタ登録」(m_sku_master) に登録済みで、
 *   このスプレッドシートの「商品コード変換テーブル」シート (A列に SKU) にまだ載っていない
 *   SKU を、毎朝 1 回検出して「変換テーブル未登録SKU」シートに上書き出力する。
 *
 * セットアップ:
 *   1. このスプレッドシートに紐付いた Apps Script (拡張機能 → Apps Script) にこのファイルを貼り付け
 *   2. プロジェクトの設定 → スクリプト プロパティ で以下を設定:
 *        MIRROR_API_BASE      = https://bfaith-portal.onrender.com/apps/mirror
 *        MIRROR_READ_TOKEN    = (Render dashboard で発行した read-only token、PR と一緒に渡す)
 *        SOURCE_SHEET_NAME    = 商品コード変換テーブル        (差分判定で参照するシート名)
 *        SOURCE_SKU_COLUMN    = 1                              (SKU が入っている列番号、A列なら 1)
 *        SOURCE_HEADER_ROWS   = 1                              (ヘッダ行数。0 なら全行を SKU として扱う)
 *        OUTPUT_SHEET_NAME    = 変換テーブル未登録SKU         (出力先シート名、無ければ自動作成)
 *   3. トリガー → 関数: runDailyCheck / イベントソース: 時間主導型 / 日付ベース / 午前7時〜8時
 *      (miniPC daily-sync が 07:00、Render mirror 同期完了後の 07:30 想定)
 *
 * Codex review (2026-05-13) 反映:
 *   - mirror freshness を必ず検証 (当日同期されてなければ stop)
 *   - LockService で多重起動防止
 *   - setValues 一発で書込、appendRow 連打しない
 *   - SKU は文字列正規化 (trim + lowercase)
 *   - 失敗時は実行履歴 (Apps Script の実行ログ) でエラー検出可能にする (throw)
 *   - WAREHOUSE_API_KEY は絶対に Script Properties に入れない (Render の read-only token のみ)
 */

const SINCE_DAYS = 7; // Mirror endpoint 固定値、ここはチェック用
const MIRROR_FRESHNESS_MAX_HOURS = 26; // 06:00 同期後 26h で stale 判定 (daily 1 回想定で 1日+α)

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
  const outputSheetName = props.getProperty('OUTPUT_SHEET_NAME') || '変換テーブル未登録SKU';

  if (!base) throw new Error('Script Property MIRROR_API_BASE が未設定');
  if (!token) throw new Error('Script Property MIRROR_READ_TOKEN が未設定');
  if (!(sourceSkuColumn >= 1)) throw new Error('SOURCE_SKU_COLUMN は 1 以上の整数');

  // 1. Render mirror から「直近7日に登録/更新された SKU」を取得
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

  // 2. mirror freshness 検証 (Codex review: stale mirror を正常扱いしない)
  const masterSync = data.mirror_sku_master_synced_at;
  if (!masterSync) {
    throw new Error('mirror_sku_master_synced_at が空 — Render mirror に sku_master が未投入の可能性');
  }
  // 'YYYY-MM-DD HH:MM:SS' (mirror 側 server.js のローカル文字列、UTC) を Date に
  const syncDate = parseSyncedAt_(masterSync);
  if (!syncDate) {
    throw new Error('mirror_sku_master_synced_at がパース不能: ' + masterSync);
  }
  const ageHours = (Date.now() - syncDate.getTime()) / 36e5;
  if (ageHours > MIRROR_FRESHNESS_MAX_HOURS) {
    throw new Error(
      'Mirror が stale: sku_master 最終同期 ' + masterSync + ' (' + ageHours.toFixed(1) + 'h 前) — ' +
      'miniPC daily-sync の失敗を疑ってください'
    );
  }

  const masterItems = Array.isArray(data.items) ? data.items : [];

  // 3. シート側「商品コード変換テーブル」の SKU 集合を作る (正規化済み)
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName(sourceSheetName);
  if (!sourceSheet) {
    throw new Error('参照シートが見つからない: ' + sourceSheetName);
  }
  const lastRow = sourceSheet.getLastRow();
  const sourceSet = new Set();
  if (lastRow > sourceHeaderRows) {
    const range = sourceSheet.getRange(sourceHeaderRows + 1, sourceSkuColumn, lastRow - sourceHeaderRows, 1);
    const vals = range.getValues();
    for (const row of vals) {
      const norm = normalizeSku_(row[0]);
      if (norm) sourceSet.add(norm);
    }
  }

  // 4. 差分計算
  const missing = [];
  for (const it of masterItems) {
    const norm = normalizeSku_(it.seller_sku);
    if (!norm) continue;
    if (sourceSet.has(norm)) continue;
    missing.push(it);
  }

  // 5. 出力シートに上書き
  let out = ss.getSheetByName(outputSheetName);
  if (!out) {
    out = ss.insertSheet(outputSheetName);
  }
  out.clear();

  const headers = ['seller_sku', '商品名', '登録/更新日 (JST)', '検出日時 (JST)', 'mirror同期 (UTC)'];
  const nowJst = formatJst_(new Date());
  const rows = missing.map(it => [
    String(it.seller_sku ?? ''),
    String(it['商品名'] ?? ''),
    formatJst_(it.source_updated_at),
    nowJst,
    masterSync,
  ]);

  out.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold');
  if (rows.length > 0) {
    out.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }
  out.setFrozenRows(1);
  // 注意書きセル
  out.getRange(1, headers.length + 2)
    .setValue('直近 ' + SINCE_DAYS + ' 日に登録/更新された SKU のうち、「' + sourceSheetName +
              '」(A列='+sourceSkuColumn+'列目) に未記載のもの。毎朝07:30頃、自動上書き。')
    .setFontColor('#666');
  // 列幅
  out.autoResizeColumns(1, headers.length);

  // 6. 実行ログ
  console.log(JSON.stringify({
    fetched: masterItems.length,
    source_skus_in_sheet: sourceSet.size,
    missing: missing.length,
    mirror_sku_master_synced_at: masterSync,
  }));
}

function normalizeSku_(v) {
  if (v === null || v === undefined) return '';
  return String(v).replace(/　/g, ' ').trim().toLowerCase();
}

/** 'YYYY-MM-DD HH:MM:SS' (mirror 側で new Date().toISOString().replace('T', ' ').slice(0,19)) を Date に */
function parseSyncedAt_(s) {
  if (!s) return null;
  // mirror 側は UTC を YYYY-MM-DD HH:MM:SS で出している (Z 無し) ので、UTC として解釈
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
}

/** ISO 8601 (UTC Z) or Date を JST 'YYYY-MM-DD HH:MM:SS' に。失敗時は元の文字列 */
function formatJst_(v) {
  if (!v) return '';
  let d;
  if (v instanceof Date) d = v;
  else {
    d = new Date(String(v));
    if (isNaN(d.getTime())) return String(v);
  }
  return Utilities.formatDate(d, 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
}
