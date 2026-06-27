/**
 * 商品管理リスト 毎朝シート上書き (⑥) — Google Apps Script【全 Sheets API 版 / v4】
 *
 * 既存「商品管理リスト」シートの【指定10列だけ】を 商品コード をキーに更新する。
 * 他の列・関数・行レイアウトには一切触れない（行の追加/削除もしない）。
 *
 * ★なぜ全 Sheets API か:
 *   このブックは数式(IMPORTRANGE等)が非常に重く、SpreadsheetApp の setValues / insertSheet /
 *   copyTo はブック全体の同期再計算に巻き込まれて "Service Spreadsheets timed out" になる。
 *   そこでシート入出力は全て Advanced Sheets Service(Sheets API REST) の Values.get/batchGet/
 *   batchUpdate で行い、SpreadsheetApp のグリッド/構造操作を一切使わない。
 *   バックアップもシートを作らず Drive 上の JSON ファイルに退避する。
 *
 * 更新する列 (シート見出し → DB項目):
 *   FBA在庫数 / FBA直近7日販売数合計 / FBA直近30日販売数合計 / FBA以外直近7日販売数合計 /
 *   FBA以外直近30日販売数合計 / 商品名 / 仕入先 / 取扱区分 / 商品区分(=売上分類) / 推奨保有在庫
 *
 * セットアップ:
 *   1) Apps Script 左「サービス(＋)」→ "Google Sheets API" を追加 (識別子 Sheets)。
 *   2) スクリプトプロパティ:
 *      ENDPOINT_URL   = https://<render>/apps/mirror/api/pml/published
 *      READ_TOKEN     = <Render env PML_READ_TOKEN と同じ値>
 *      SPREADSHEET_ID = 15L_BU6WrXNX8aMblTs4yBqwkDFJOg0oDRKDTRHovLDE
 *      SHEET_NAME     = 商品管理リスト
 *      WRITE_MODE     = dry_run   (検証OKまで dry_run、その後 live)
 *      GCHAT_WEBHOOK  = (任意) 失敗時通知の Google Chat Webhook URL
 *   3) 初回実行時の承認で Drive(バックアップJSON用) と Sheets の権限を許可。
 */

var MAX_LAG_DAYS = 2;
var KEY_HEADER = '商品コード';
var MIN_ROWS = 1000;
var MAX_ROWS = 20000;
var MIN_MATCH_RATE = 0.5;
var FETCH_RETRY = 3;
var FETCH_RETRY_WAIT_MS = 4000;
var BACKUP_KEEP = 7;
var MAX_WRITE_RANGES = 2000;  // 一致/非一致が異常断片化したら中止 (通常は数十)

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
    if (typeof Sheets === 'undefined') {
      throw new Error('Advanced Sheets Service 未有効。Apps Script 左「サービス(＋)」→ Google Sheets API を追加してください');
    }
    var q = "'" + sheetName.replace(/'/g, "''") + "'!";

    // 1. データ取得 (一過性 5xx はリトライ)
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

    // 3. payload に必須列(商品コード + 各DB項目)が揃っているか fail-closed 検証
    //    (checksum は payload 自体の整合のみ。列が消えても気付かず空文字上書きするのを防ぐ)
    var colSet = {};
    for (var cc = 0; cc < cols.length; cc++) colSet[cols[cc]] = true;
    if (!colSet[KEY_HEADER]) throw new Error('payload に列なし: ' + KEY_HEADER);
    for (var tf = 0; tf < TARGETS.length; tf++) {
      if (!colSet[TARGETS[tf].field]) throw new Error('payload に必要列なし: ' + TARGETS[tf].field);
    }

    // 4. DB を 商品コード(小文字) でマップ。各行に必須フィールドが実在するか検証(欠落=空文字上書き防止、
    //    null は許容=未登録の空欄)。商品コード重複は曖昧なので fail-closed。
    var reqFields = [KEY_HEADER];
    for (var rf = 0; rf < TARGETS.length; rf++) reqFields.push(TARGETS[rf].field);
    var dbMap = {};
    var dupKeys = [];
    for (var i = 0; i < rows.length; i++) {
      var rowObj = rows[i];
      for (var rf2 = 0; rf2 < reqFields.length; rf2++) {
        if (!Object.prototype.hasOwnProperty.call(rowObj, reqFields[rf2])) {
          throw new Error('payload行にフィールド欠落: ' + reqFields[rf2] + ' (row index ' + i + ')');
        }
      }
      var key = String(rowObj[KEY_HEADER] == null ? '' : rowObj[KEY_HEADER]).trim().toLowerCase();
      if (!key) continue;
      if (dbMap[key]) dupKeys.push(key);
      dbMap[key] = rowObj;
    }
    if (dupKeys.length) throw new Error('DB商品コード重複 ' + dupKeys.length + '件 (例: ' + dupKeys.slice(0, 5).join(', ') + ')');

    // 4. 見出し行を Sheets API で読み、列位置を解決
    var hdrResp = Sheets.Spreadsheets.Values.get(ssId, q + '1:1');
    var rawHeader = (hdrResp.values && hdrResp.values[0]) ? hdrResp.values[0] : [];
    var header = rawHeader.map(function (h) { return String(h == null ? '' : h).replace(/\s/g, ''); });

    // キー列も候補!==1で fail-closed (誤認は行対応の誤りに直結)
    var keyHits = [];
    for (var hk = 0; hk < header.length; hk++) if (header[hk] === KEY_HEADER) keyHits.push(hk);
    if (keyHits.length !== 1) throw new Error('シートの「' + KEY_HEADER + '」列の一致が ' + keyHits.length + ' 列 (1列であるべき)');
    var keyIdx = keyHits[0];

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

    // 5. 商品コード列を Sheets API で読み行数 n を確定
    var keyA1 = colA1_(keyIdx + 1);
    var keyResp = Sheets.Spreadsheets.Values.get(ssId, q + keyA1 + '2:' + keyA1, { majorDimension: 'COLUMNS' });
    var keyCol = (keyResp.values && keyResp.values[0]) ? keyResp.values[0] : [];
    var n = keyCol.length;
    if (n < 1) throw new Error('シートにデータ行がない');
    var lastRow = n + 1;

    // 6. 対象10列の現在値を Sheets API batchGet (1回、COLUMNS) → n 長にパディング
    var getRanges = resolved.map(function (r) { var a = colA1_(r.col); return q + a + '2:' + a + lastRow; });
    var bg = Sheets.Spreadsheets.Values.batchGet(ssId, { ranges: getRanges, majorDimension: 'COLUMNS' });
    var curCols = (bg.valueRanges || []).map(function (vr) { return (vr.values && vr.values[0]) ? vr.values[0] : []; });
    for (var ci = 0; ci < curCols.length; ci++) { while (curCols[ci].length < n) curCols[ci].push(''); }

    // 7. 行→DB行 の対応付け
    var matched = 0, unmatchedRows = 0;
    var dbForRow = new Array(n);
    for (var rIdx = 0; rIdx < n; rIdx++) {
      var k = String(keyCol[rIdx] == null ? '' : keyCol[rIdx]).trim().toLowerCase();
      if (k && dbMap[k]) { dbForRow[rIdx] = dbMap[k]; matched++; }
      else { dbForRow[rIdx] = null; if (k) unmatchedRows++; }
    }

    Logger.log('検証OK run=' + data.run_id + ' status=' + data.status + ' as_of=' + data.as_of_date
      + ' / シート行=' + n + ' 一致=' + matched + ' DB未一致(据え置き)=' + unmatchedRows + ' mode=' + writeMode);
    if (matched === 0) throw new Error('商品コードが1件も一致しない');
    if (matched / n < MIN_MATCH_RATE) throw new Error('一致率が低い: ' + matched + '/' + n);
    if (writeMode !== 'live') { Logger.log('[dry_run] 書き込みスキップ'); return; }

    // 8. バックアップ(Drive JSON、シートは触らない) → Sheets API で10列を1回書き込み
    backupToDrive_(ssId, sheetName, data, resolved, keyCol, curCols, n);

    // 一致行(DBにある行)だけを連続区間ごとに書く。非一致行は range に含めない=数式も含め完全に不変。
    var changed = 0;
    var dataReqs = [];
    for (var c2 = 0; c2 < resolved.length; c2++) {
      var field = resolved[c2].field, a1 = colA1_(resolved[c2].col);
      var rr = 0;
      while (rr < n) {
        if (!dbForRow[rr]) { rr++; continue; }
        var start = rr;
        var vals = [];
        while (rr < n && dbForRow[rr]) {
          var v = dbForRow[rr][field]; v = (v === null || v === undefined) ? '' : v;
          if (String(curCols[c2][rr]) !== String(v)) changed++;
          vals.push([v]);
          rr++;
        }
        dataReqs.push({ range: q + a1 + (start + 2) + ':' + a1 + (start + 1 + vals.length), majorDimension: 'ROWS', values: vals });
      }
    }
    if (dataReqs.length === 0) { Logger.log('更新対象なし'); return; }
    if (dataReqs.length > MAX_WRITE_RANGES) {
      throw new Error('書込range断片化が異常: ' + dataReqs.length + ' (>' + MAX_WRITE_RANGES + ')。一致/非一致が細かく交互の疑いで中止');
    }
    Sheets.Spreadsheets.Values.batchUpdate({ valueInputOption: 'RAW', data: dataReqs }, ssId);
    Logger.log('部分上書き完了(Sheets API): 一致' + matched + '行 / 書込range' + dataReqs.length + ' / 変更セル' + changed);
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
    break;
  }
  throw new Error(lastErr);
}

// バックアップ: 対象列の現在値を Drive 上の JSON に退避 (シート構造を一切触らない)。最新 BACKUP_KEEP 世代。
function backupToDrive_(ssId, sheetName, data, resolved, keyCol, curCols, n) {
  var headers = [KEY_HEADER];
  for (var c = 0; c < resolved.length; c++) headers.push(resolved[c].hdr);
  var bakRows = [];
  for (var r = 0; r < n; r++) {
    var row = [keyCol[r]];
    for (var c2 = 0; c2 < resolved.length; c2++) row.push(curCols[c2][r]);
    bakRows.push(row);
  }
  var payload = {
    timestamp: new Date().toISOString(),
    spreadsheetId: ssId, sheetName: sheetName,
    as_of_date: data.as_of_date, source_run_id: data.run_id, source_checksum: data.payload_checksum,
    headers: headers, rows: bakRows,
  };
  var name = 'pmlbak_' + (data.as_of_date || Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd')) + '.json';
  var dup = DriveApp.getFilesByName(name);
  while (dup.hasNext()) dup.next().setTrashed(true);
  DriveApp.createFile(name, JSON.stringify(payload), 'application/json');
  // 古いバックアップ掃除 (最新 BACKUP_KEEP 世代)
  var files = [];
  var it = DriveApp.searchFiles('title contains "pmlbak_" and trashed = false');
  while (it.hasNext()) { var f = it.next(); if (f.getName().indexOf('pmlbak_') === 0) files.push(f); }
  files.sort(function (a, b) { return a.getName() < b.getName() ? 1 : -1; });
  for (var i = BACKUP_KEEP; i < files.length; i++) files[i].setTrashed(true);
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

function sha256Hex_(str) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  var hex = '';
  for (var i = 0; i < bytes.length; i++) { var b = (bytes[i] + 256) % 256; hex += (b < 16 ? '0' : '') + b.toString(16); }
  return hex;
}
