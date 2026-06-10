/**
 * 商品管理リスト 毎朝スプレッドシート上書き (⑥) — Google Apps Script
 *
 * Render mirror の /api/pml/published を読み、status/checksum/鮮度/件数を検証してから
 * 現スプレッドシート「商品管理リスト」を安全に上書きする。
 *
 * 安全装置 (feedback_gas_sheet_direct_write_safeguards + 設計v4):
 *   - LockService で同時実行防止
 *   - WRITE_MODE = 'dry_run' / 'live' (初期は dry_run で並走検証)
 *   - 反映条件: ok=true / status='ok' / actual_row_count==row_count / 件数が下限〜上限内 /
 *     鮮度(as_of_date が MAX_LAG_DAYS 以内) / payload_checksum を受信行から再計算して一致
 *   - 本番は「先に新データを書く → 成功後に余剰行だけ clear」(clear→write はしない=途中失敗で空にしない)
 *   - 上書き前に当日バックアップシートを作成
 *
 * セットアップ:
 *   スクリプトプロパティ (ファイル > プロジェクトのプロパティ / Apps Script の「スクリプト プロパティ」):
 *     ENDPOINT_URL   = https://<render>/apps/warehouse-mirror/api/pml/published  (実際の mount path に合わせる)
 *     READ_TOKEN     = <MIRROR_READ_TOKEN と同じ値>
 *     SPREADSHEET_ID = <商品管理リストのスプレッドシートID>
 *     SHEET_NAME     = 商品管理リスト        (上書き対象シート)
 *     WRITE_MODE     = dry_run               (検証OKまで dry_run、その後 live)
 *   トリガー: main を時間主導型で毎朝 (daily-sync 07:00 / snapshot 生成後の 08:00 など、時刻分離)
 */

var MAX_LAG_DAYS = 2;       // as_of_date が今日からこの日数以内なら鮮度OK
var MIN_ROWS = 1000;        // 全商品コードbaseなので極端に少ない=異常 (調整可)
var MAX_ROWS = 20000;       // 暴走ガード

function main() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) { Logger.log('別実行中のためスキップ'); return; }
  try {
    var props = PropertiesService.getScriptProperties();
    var endpoint = props.getProperty('ENDPOINT_URL');
    var token = props.getProperty('READ_TOKEN');
    var ssId = props.getProperty('SPREADSHEET_ID');
    var sheetName = props.getProperty('SHEET_NAME') || '商品管理リスト';
    var writeMode = props.getProperty('WRITE_MODE') || 'dry_run';
    if (!endpoint || !token || !ssId) throw new Error('スクリプトプロパティ未設定 (ENDPOINT_URL/READ_TOKEN/SPREADSHEET_ID)');

    // 1. 取得
    var resp = UrlFetchApp.fetch(endpoint, {
      method: 'get',
      headers: { 'x-read-token': token },
      muteHttpExceptions: true,
    });
    var code = resp.getResponseCode();
    if (code !== 200) throw new Error('HTTP ' + code + ': ' + resp.getContentText().slice(0, 200));
    var data = JSON.parse(resp.getContentText());

    // 2. 検証 (fail-closed)
    if (!data.ok) throw new Error('published なし: ' + (data.reason || ''));
    if (data.status !== 'ok') throw new Error('status=' + data.status + ' (ok以外は上書きしない)');
    var cols = data.columns;
    var rows = data.rows || [];
    if (!cols || !cols.length) throw new Error('columns 欠落');
    if (typeof data.row_count !== 'number' || data.row_count !== rows.length || data.actual_row_count !== rows.length) {
      throw new Error('件数不一致 row_count=' + data.row_count + ' actual=' + data.actual_row_count + ' rows=' + rows.length);
    }
    if (rows.length < MIN_ROWS || rows.length > MAX_ROWS) throw new Error('件数が範囲外: ' + rows.length + ' (' + MIN_ROWS + '〜' + MAX_ROWS + ')');
    if (!freshEnough_(data.as_of_date)) throw new Error('鮮度NG as_of_date=' + data.as_of_date);

    // checksum 再計算 (サーバと同一規約: 列順固定・null='' ・tab/改行・sha256)
    var recomputed = sha256Hex_(rows.map(function (r) {
      return cols.map(function (c) { return (r[c] === null || r[c] === undefined) ? '' : String(r[c]); }).join('\t');
    }).join('\n'));
    if (data.payload_checksum && recomputed !== data.payload_checksum) {
      throw new Error('checksum 不一致 (改ざん/欠落の疑い)');
    }

    // 3. 2D 配列構築 (ヘッダー + データ、列順固定)
    var values = [cols.slice()];
    for (var i = 0; i < rows.length; i++) {
      var row = [];
      for (var j = 0; j < cols.length; j++) {
        var v = rows[i][cols[j]];
        row.push(v === null || v === undefined ? '' : v);
      }
      values.push(row);
    }

    Logger.log('検証OK: run=' + data.run_id + ' status=' + data.status + ' rows=' + rows.length + ' as_of=' + data.as_of_date + ' mode=' + writeMode);
    if (writeMode !== 'live') { Logger.log('[dry_run] 上書きスキップ (WRITE_MODE=live で本番反映)'); return; }

    // 4. 本番反映
    var ss = SpreadsheetApp.openById(ssId);
    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error('シートが見つからない: ' + sheetName);

    backup_(ss, sheet, data.as_of_date);

    var numRows = values.length;
    var numCols = cols.length;
    // 先に新データを書く (clear→write はしない: 途中失敗で本番が空になるのを防ぐ)
    sheet.getRange(1, 1, numRows, numCols).setValues(values);
    // 旧データの余剰行だけ後から clear (新データより下)
    var lastRow = sheet.getLastRow();
    if (lastRow > numRows) {
      sheet.getRange(numRows + 1, 1, lastRow - numRows, Math.max(numCols, sheet.getLastColumn())).clearContent();
    }
    SpreadsheetApp.flush();
    Logger.log('本番上書き完了: ' + (numRows - 1) + '行 (run=' + data.run_id + ')');
  } catch (e) {
    Logger.log('ERROR: ' + e.message);
    throw e; // トリガー失敗として記録 (Apps Script の実行ログ/通知で検知)
  } finally {
    lock.releaseLock();
  }
}

function freshEnough_(asOf) {
  if (!asOf) return false;
  var tz = 'Asia/Tokyo';
  var today = new Date(Utilities.formatDate(new Date(), tz, 'yyyy/MM/dd'));
  var d = new Date(asOf.replace(/-/g, '/'));
  var lag = Math.floor((today.getTime() - d.getTime()) / 86400000);
  return lag >= 0 && lag <= MAX_LAG_DAYS;
}

function backup_(ss, sheet, asOf) {
  var name = '_backup_' + (asOf || Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd'));
  var old = ss.getSheetByName(name);
  if (old) ss.deleteSheet(old);
  var copy = sheet.copyTo(ss);
  copy.setName(name);
  copy.hideSheet();
  // 古いバックアップ掃除 (最新7個温存)
  var backups = ss.getSheets().filter(function (s) { return s.getName().indexOf('_backup_') === 0; })
    .sort(function (a, b) { return a.getName() < b.getName() ? 1 : -1; });
  for (var i = 7; i < backups.length; i++) ss.deleteSheet(backups[i]);
}

function sha256Hex_(str) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  var hex = '';
  for (var i = 0; i < bytes.length; i++) {
    var b = (bytes[i] + 256) % 256;
    hex += (b < 16 ? '0' : '') + b.toString(16);
  }
  return hex;
}
