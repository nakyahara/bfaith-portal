/**
 * 商品管理リスト 毎朝シート上書き (⑥) — Google Apps Script【部分上書き / Sheets API版】
 *
 * Render mirror の /api/pml/published を読み、status/checksum/鮮度/件数を検証してから、
 * 既存「商品管理リスト」シートの【指定列だけ】を 商品コード をキーに更新する。
 * 他の列・関数・行レイアウトには一切触れない（行の追加/削除もしない）。
 *
 * ★書き込みは Advanced Sheets Service (Sheets API) の values.batchUpdate を使う。
 *   SpreadsheetApp.setValues を列ごとに呼ぶと、数式が多い大きいシートでは毎回フル再計算が走り
 *   「Service Spreadsheets timed out」になるため、10列を1回のAPI呼び出しでまとめて書く。
 *   → 事前に「サービス」で Google Sheets API を有効化しておくこと(下記セットアップ)。
 *
 * 更新する列 (シート見出し → DB項目):
 *   FBA在庫数 / FBA直近7日販売数合計 / FBA直近30日販売数合計 / FBA以外直近7日販売数合計 /
 *   FBA以外直近30日販売数合計 / 商品名 / 仕入先 / 取扱区分 / 商品区分(=売上分類) / 推奨保有在庫
 *
 * セットアップ:
 *   1) Apps Script 左の「サービス(＋)」→ "Google Sheets API" を追加 (識別子 Sheets)。
 *   2) スクリプトプロパティ:
 *      ENDPOINT_URL   = https://<render>/apps/mirror/api/pml/published
 *      READ_TOKEN     = <Render env PML_READ_TOKEN と同じ値>
 *      SPREADSHEET_ID = 15L_BU6WrXNX8aMblTs4yBqwkDFJOg0oDRKDTRHovLDE
 *      SHEET_NAME     = 商品管理リスト
 *      WRITE_MODE     = dry_run   (検証OKまで dry_run、その後 live)
 *      GCHAT_WEBHOOK  = (任意) 失敗時通知の Google Chat Webhook URL
 */

var MAX_LAG_DAYS = 2;
var KEY_HEADER = '商品コード';
var MIN_ROWS = 1000;
var MAX_ROWS = 20000;
var MIN_MATCH_RATE = 0.5;
var FETCH_RETRY = 3;          // 一過性 5xx (Render の 502/503/504) のリトライ回数
var FETCH_RETRY_WAIT_MS = 4000;

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

    // 1. 取得 (一過性 5xx はリトライ)
    var data = fetchPublished_(endpoint, token);

    // 2. 検証 (fail-closed)
    if (!data.ok) throw new Error('published なし: ' + (data.reason || ''));
    if (data.status !== 'ok') throw new Error('status=' + data.status + ' (ok以外は上書きしない)');
    var cols = data.columns, rows = data.rows || [];
    if (!cols || !cols.length) throw new Error('columns 欠落');
    if (typeof data.row_count !== 'number' || data.row_count !== rows.length || data.actual_row_count !== rows.length) {
      throw new Error('件数不一致 row_count=' + data.row_count + ' actual=' + data.actual_row_count + ' rows=' + rows.length);
    }
    if (rows.length < MIN_ROWS || rows.length > MAX_ROWS) throw new Error('件数が範囲外: ' + rows.length);
    if (!freshEnough_(data.as_of_date)) throw new Error('鮮度NG as_of_date=' + data.as_of_date);
    var recomputed = sha256Hex_(rows.map(function (r) {
      return cols.map(function (c) { return (r[c] === null || r[c] === undefined) ? '' : String(r[c]); }).join('\t');
    }).join('\n'));
    if (typeof data.payload_checksum !== 'string' || !data.payload_checksum) throw new Error('payload_checksum 欠落');
    if (recomputed !== data.payload_checksum) throw new Error('checksum 不一致');

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
      if (hits.length !== 1) throw new Error('見出し「' + tg.hdr + '」の一致が ' + hits.length + ' 列 (1列であるべき。中止)');
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
    if (matched === 0) throw new Error('商品コードが1件も一致しない');
    if (matched / n < MIN_MATCH_RATE) throw new Error('一致率が低い: ' + matched + '/' + n);
    if (writeMode !== 'live') { Logger.log('[dry_run] 書き込みスキップ'); return; }

    if (typeof Sheets === 'undefined') {
      throw new Error('Advanced Sheets Service 未有効。Apps Script 左「サービス(＋)」→ Google Sheets API を追加してください');
    }

    // 6. 本番反映: 全対象列の現在値を先読み → 軽量バックアップ → Sheets API で1回でまとめ書き。
    var curArrays = [];
    for (var c = 0; c < resolved.length; c++) {
      curArrays[c] = sheet.getRange(2, resolved[c].col, n, 1).getValues();
    }
    backupColumns_(ss, data.as_of_date, resolved, keyVals, curArrays, n);

    var dataReqs = [];
    for (var c2 = 0; c2 < resolved.length; c2++) {
      var arr = curArrays[c2], field = resolved[c2].field;
      for (var rr = 0; rr < n; rr++) {
        var dbr = dbRowForSheetRow[rr];
        if (dbr) { var v = dbr[field]; arr[rr][0] = (v === null || v === undefined) ? '' : v; }
      }
      var a1 = colA1_(resolved[c2].col);
      dataReqs.push({ range: "'" + sheetName + "'!" + a1 + '2:' + a1 + lastRow, values: arr });
    }
    Sheets.Spreadsheets.Values.batchUpdate({ valueInputOption: 'RAW', data: dataReqs }, ssId);
    Logger.log('部分上書き完了(Sheets API): ' + resolved.length + '列 × 一致' + matched + '行');
  } catch (e) {
    Logger.log('ERROR: ' + e.message);
    notifyGChat_('🔴 *商品管理リスト シート上書き失敗*\n' + e.message);
    throw e;
  } finally {
    lock.releaseLock();
  }
}

// /api/pml/published 取得。一過性 5xx (Render 502/503/504) は数秒待ってリトライ。
function fetchPublished_(endpoint, token) {
  var lastErr = '';
  for (var attempt = 1; attempt <= FETCH_RETRY; attempt++) {
    var resp = UrlFetchApp.fetch(endpoint, { method: 'get', headers: { 'x-read-token': token }, muteHttpExceptions: true });
    var code = resp.getResponseCode();
    if (code === 200) return JSON.parse(resp.getContentText());
    lastErr = 'HTTP ' + code + ': ' + resp.getContentText().slice(0, 150);
    if (code >= 500 && code < 600 && attempt < FETCH_RETRY) { Utilities.sleep(FETCH_RETRY_WAIT_MS); continue; }
    break; // 4xx は即中止 (トークン違い等、待っても直らない)
  }
  throw new Error(lastErr);
}

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

// 列番号(1始まり) → A1 列記号 (例 1→A, 27→AA)
function colA1_(col) {
  var s = '';
  while (col > 0) { var m = (col - 1) % 26; s = String.fromCharCode(65 + m) + s; col = Math.floor((col - 1) / 26); }
  return s;
}

// 軽量バックアップ: 商品コード + 対象列の「現在値」だけを隠しシートに退避。
function backupColumns_(ss, asOf, resolved, keyVals, curArrays, n) {
  var name = '_pmlbak_' + (asOf || Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd'));
  var old = ss.getSheetByName(name);
  if (old) ss.deleteSheet(old);
  var bak = ss.insertSheet(name);
  bak.hideSheet();
  var headers = ['商品コード'];
  for (var c = 0; c < resolved.length; c++) headers.push(resolved[c].hdr);
  var out = [headers];
  for (var r = 0; r < n; r++) {
    var row = [keyVals[r][0]];
    for (var c2 = 0; c2 < resolved.length; c2++) row.push(curArrays[c2][r][0]);
    out.push(row);
  }
  bak.getRange(1, 1, out.length, headers.length).setValues(out);
  var baks = ss.getSheets().filter(function (s) { return s.getName().indexOf('_pmlbak_') === 0; })
    .sort(function (a, b) { return a.getName() < b.getName() ? 1 : -1; });
  for (var i = 7; i < baks.length; i++) ss.deleteSheet(baks[i]);
}

function sha256Hex_(str) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  var hex = '';
  for (var i = 0; i < bytes.length; i++) { var b = (bytes[i] + 256) % 256; hex += (b < 16 ? '0' : '') + b.toString(16); }
  return hex;
}
