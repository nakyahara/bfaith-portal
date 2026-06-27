/**
 * 商品管理リスト 毎朝シート上書き (⑥) — Google Apps Script【部分上書き版】
 *
 * Render mirror の /api/pml/published を読み、status/checksum/鮮度/件数を検証してから、
 * 既存「商品管理リスト」シートの【指定列だけ】を 商品コード をキーに更新する。
 * 他の列・関数・行レイアウトには一切触れない（行の追加/削除もしない）。
 *
 * 更新する列 (シート見出し → DB項目):
 *   FBA在庫数                 → FBA在庫数
 *   FBA直近7日販売数合計       → 販売数7日_FBA
 *   FBA直近30日販売数合計      → 販売数30日_FBA
 *   FBA以外直近7日販売数合計   → 販売数7日_FBA以外
 *   FBA以外直近30日販売数合計  → 販売数30日_FBA以外
 *   商品名                    → 商品名
 *   仕入先                    → 仕入先
 *   取扱区分                  → 取扱区分
 *   商品区分(1自社/2AMC/3仕入) → 売上分類   ※見出しは「商品区分」で始まる多行セル
 *   推奨保有在庫              → 推奨保有月数
 *
 * 安全装置: LockService / WRITE_MODE dry_run|live / checksum(全列再計算)一致 / 鮮度 /
 *   件数一致 / 上書き前バックアップ / 指定列以外は不変 / DBに無い商品コードの行は据え置き。
 *
 * スクリプトプロパティ:
 *   ENDPOINT_URL   = https://<render>/apps/mirror/api/pml/published  (実際の mount に合わせる)
 *   READ_TOKEN     = <MIRROR_READ_TOKEN と同じ値>
 *   SPREADSHEET_ID = 15L_BU6WrXNX8aMblTs4yBqwkDFJOg0oDRKDTRHovLDE
 *   SHEET_NAME     = 商品管理リスト
 *   WRITE_MODE     = dry_run   (検証OKまで dry_run、その後 live)
 *   GCHAT_WEBHOOK  = (任意) 失敗時に通知する Google Chat Webhook URL。未設定なら通知なし
 */

var MAX_LAG_DAYS = 2;
var KEY_HEADER = '商品コード';
var MIN_ROWS = 1000;          // payload 行数の下限 (全商品base想定。極端に少ない=異常→反映しない)
var MAX_ROWS = 20000;         // 暴走ガード
var MIN_MATCH_RATE = 0.5;     // シート行のうち DB と一致した割合の下限 (キー形式ズレ等の検知)

// シート見出し(空白/改行除去後) → DB項目。prefix:true は「で始まる」で照合(多行見出し用)。
var TARGETS = [
  { hdr: 'FBA在庫数',                field: 'FBA在庫数' },
  { hdr: 'FBA直近7日販売数合計',     field: '販売数7日_FBA' },
  { hdr: 'FBA直近30日販売数合計',    field: '販売数30日_FBA' },
  { hdr: 'FBA以外直近7日販売数合計', field: '販売数7日_FBA以外' },
  { hdr: 'FBA以外直近30日販売数合計',field: '販売数30日_FBA以外' },
  { hdr: '商品名',                   field: '商品名' },
  { hdr: '仕入先',                   field: '仕入先' },
  { hdr: '取扱区分',                 field: '取扱区分' },
  { hdr: '商品区分',                 field: '売上分類', prefix: true, must: '自社' },
  { hdr: '推奨保有在庫',             field: '推奨保有月数' },
];

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
    var resp = UrlFetchApp.fetch(endpoint, { method: 'get', headers: { 'x-read-token': token }, muteHttpExceptions: true });
    var code = resp.getResponseCode();
    if (code !== 200) throw new Error('HTTP ' + code + ': ' + resp.getContentText().slice(0, 200));
    var data = JSON.parse(resp.getContentText());

    // 2. 検証 (fail-closed)
    if (!data.ok) throw new Error('published なし: ' + (data.reason || ''));
    if (data.status !== 'ok') throw new Error('status=' + data.status + ' (ok以外は上書きしない)');
    var cols = data.columns, rows = data.rows || [];
    if (!cols || !cols.length) throw new Error('columns 欠落');
    if (typeof data.row_count !== 'number' || data.row_count !== rows.length || data.actual_row_count !== rows.length) {
      throw new Error('件数不一致 row_count=' + data.row_count + ' actual=' + data.actual_row_count + ' rows=' + rows.length);
    }
    if (rows.length < MIN_ROWS || rows.length > MAX_ROWS) throw new Error('件数が範囲外: ' + rows.length + ' (' + MIN_ROWS + '〜' + MAX_ROWS + ')');
    if (!freshEnough_(data.as_of_date)) throw new Error('鮮度NG as_of_date=' + data.as_of_date);
    var recomputed = sha256Hex_(rows.map(function (r) {
      return cols.map(function (c) { return (r[c] === null || r[c] === undefined) ? '' : String(r[c]); }).join('\t');
    }).join('\n'));
    if (typeof data.payload_checksum !== 'string' || !data.payload_checksum) throw new Error('payload_checksum 欠落 (fail-closed)');
    if (recomputed !== data.payload_checksum) throw new Error('checksum 不一致 (改ざん/欠落の疑い)');

    // 3. DB を 商品コード(小文字) でマップ
    var dbMap = {};
    for (var i = 0; i < rows.length; i++) {
      var key = String(rows[i][KEY_HEADER] == null ? '' : rows[i][KEY_HEADER]).trim().toLowerCase();
      if (key) dbMap[key] = rows[i];
    }

    // 4. シートの見出し行と商品コード列を特定
    var ss = SpreadsheetApp.openById(ssId);
    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) throw new Error('シートが見つからない: ' + sheetName);
    var lastRow = sheet.getLastRow(), lastCol = sheet.getLastColumn();
    if (lastRow < 2) throw new Error('シートにデータ行がない');
    var header = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function (h) { return String(h == null ? '' : h).replace(/\s/g, ''); });

    var keyCol = header.indexOf(KEY_HEADER) + 1;
    if (keyCol === 0) throw new Error('シートに「' + KEY_HEADER + '」列が無い');

    // 各ターゲット列の位置を解決。候補が 0 でも 2以上でも fail-closed (誤列上書き防止)。
    var resolved = [];
    for (var t = 0; t < TARGETS.length; t++) {
      var tg = TARGETS[t], hits = [];
      for (var h = 0; h < header.length; h++) {
        var ok = tg.prefix ? header[h].indexOf(tg.hdr) === 0 : header[h] === tg.hdr;
        if (ok && tg.must && header[h].indexOf(tg.must) < 0) ok = false;
        if (ok) hits.push(h);
      }
      if (hits.length !== 1) throw new Error('見出し「' + tg.hdr + '」の一致が ' + hits.length + ' 列 (1列であるべき。誤上書き防止のため中止)');
      resolved.push({ col: hits[0] + 1, field: tg.field, hdr: tg.hdr });
    }

    // 5. 商品コード列を読み、行→DB行 を対応付け
    var n = lastRow - 1;
    var keyVals = sheet.getRange(2, keyCol, n, 1).getValues();
    var matched = 0, unmatchedRows = 0;
    var dbRowForSheetRow = new Array(n);
    for (var rIdx = 0; rIdx < n; rIdx++) {
      var k = String(keyVals[rIdx][0] == null ? '' : keyVals[rIdx][0]).trim().toLowerCase();
      if (k && dbMap[k]) { dbRowForSheetRow[rIdx] = dbMap[k]; matched++; }
      else { dbRowForSheetRow[rIdx] = null; if (k) unmatchedRows++; }
    }

    Logger.log('検証OK run=' + data.run_id + ' status=' + data.status + ' as_of=' + data.as_of_date
      + ' / シート行=' + n + ' 一致=' + matched + ' DB未一致(据え置き)=' + unmatchedRows + ' mode=' + writeMode);
    if (matched === 0) throw new Error('商品コードが1件も一致しない (キー列/大小文字を確認)');
    if (matched / n < MIN_MATCH_RATE) throw new Error('一致率が低い: ' + matched + '/' + n + ' (< ' + MIN_MATCH_RATE + ')。キー形式ズレ等の疑いで中止');
    if (writeMode !== 'live') { Logger.log('[dry_run] 書き込みスキップ'); return; }

    // 6. 本番反映: バックアップ → 各ターゲット列だけ更新 (一致行のみ値差し替え、非一致は現状維持)
    backup_(ss, sheet, data.as_of_date);
    for (var c = 0; c < resolved.length; c++) {
      var col = resolved[c].col, field = resolved[c].field;
      var cur = sheet.getRange(2, col, n, 1).getValues(); // 現状値 (非一致行はこのまま戻す)
      for (var rr = 0; rr < n; rr++) {
        var dbr = dbRowForSheetRow[rr];
        if (dbr) { var v = dbr[field]; cur[rr][0] = (v === null || v === undefined) ? '' : v; }
      }
      sheet.getRange(2, col, n, 1).setValues(cur);
    }
    SpreadsheetApp.flush();
    Logger.log('部分上書き完了: ' + resolved.length + '列 × 一致' + matched + '行 (run=' + data.run_id + ')');
  } catch (e) {
    Logger.log('ERROR: ' + e.message);
    notifyGChat_('🔴 *商品管理リスト シート上書き失敗*\n' + e.message);
    throw e;
  } finally {
    lock.releaseLock();
  }
}

// 失敗時 GChat 通知 (任意。スクリプトプロパティ GCHAT_WEBHOOK 未設定ならスキップ)。
// 通知自体の失敗で元エラーを覆い隠さないよう best-effort。
function notifyGChat_(text) {
  try {
    var url = PropertiesService.getScriptProperties().getProperty('GCHAT_WEBHOOK');
    if (!url) return;
    UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', payload: JSON.stringify({ text: text }), muteHttpExceptions: true });
  } catch (e2) { Logger.log('GChat通知失敗: ' + e2.message); }
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
  var backups = ss.getSheets().filter(function (s) { return s.getName().indexOf('_backup_') === 0; })
    .sort(function (a, b) { return a.getName() < b.getName() ? 1 : -1; });
  for (var i = 7; i < backups.length; i++) ss.deleteSheet(backups[i]);
}

function sha256Hex_(str) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  var hex = '';
  for (var i = 0; i < bytes.length; i++) { var b = (bytes[i] + 256) % 256; hex += (b < 16 ? '0' : '') + b.toString(16); }
  return hex;
}
