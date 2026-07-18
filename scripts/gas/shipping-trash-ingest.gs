/**
 * 出荷_no 掃除 + 出荷実績吸い上げ (shipping-log 連携版)
 *
 * 毎夕トリガー (18:16) で実行。各 出荷_XX フォルダについて:
 *   1. 納品書PDF → Docs 変換でテキスト抽出し、伝票ごとに
 *      出荷伝票NO (SPxxx) / 管理番号 / モール注文番号 をパース
 *   2. Render (bfaith-portal) の POST /apps/shipping-log/api/ingest へ送信
 *   3. ★200 が返ったフォルダのみ★ ファイルをゴミ箱へ移動
 *      (抽出失敗・送信失敗のフォルダはファイルを残して GChat に通知)
 *
 * 事前設定 (スクリプトプロパティ):
 *   SHIPPING_INGEST_BASE : 例 https://bfaith-portal.onrender.com/apps/shipping-log/api
 *   SHIPPING_INGEST_TOKEN: Render env SHIPPING_LOG_INGEST_TOKEN と同じ値
 *   DRY_RUN              : "1" なら抽出+送信のみ、ファイルは削除しない (動作確認用)
 *   SHIPPING_GCHAT_WEBHOOK: 通知先 webhook URL (任意)
 * 事前設定 (サービス): 「Drive API」(v2) を追加しておくこと
 *
 * サーバ側契約: apps/shipping-log/router.js (Bearer fail-closed、冪等 INSERT OR IGNORE)。
 * 再実行・再送は業務キー (ship_date, folder, slip_no) で重複排除されるため安全。
 */
var ROOT_FOLDER_ID = "110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh";

function trashAllFilesInFolder() {
  var props = PropertiesService.getScriptProperties();
  var base = props.getProperty("SHIPPING_INGEST_BASE");
  var token = props.getProperty("SHIPPING_INGEST_TOKEN");
  var dryRun = props.getProperty("DRY_RUN") === "1";
  if (!base || !token) throw new Error("SHIPPING_INGEST_BASE / SHIPPING_INGEST_TOKEN が未設定です");

  var runId = Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyyMMdd-HHmmss");
  var results = [];
  var subFolders = DriveApp.getFolderById(ROOT_FOLDER_ID).getFolders();
  while (subFolders.hasNext()) {
    var folder = subFolders.next();
    var r = processFolder_(folder, base, token, runId, dryRun);
    if (r) results.push(r);
  }
  notifyResult_(results, runId, dryRun);
}

/** 1フォルダ分: 抽出→POST→(200のときのみ)ゴミ箱。結果オブジェクトを返す */
function processFolder_(folder, base, token, runId, dryRun) {
  var files = [];
  var it = folder.getFiles();
  while (it.hasNext()) files.push(it.next());
  if (files.length === 0) return null; // 未使用フォルダ

  var result = { name: folder.getName(), files: files.length, slips: 0, status: "" };
  try {
    var invoices = files.filter(function (f) {
      return /納品書/.test(f.getName()) && f.getMimeType() === "application/pdf";
    });
    if (invoices.length === 0) {
      result.status = "skip: 納品書PDFなし(誤配置?)";
      return result; // 消さずに残す
    }

    var rows = [];
    invoices.forEach(function (inv) {
      extractSlipsFromPdf_(inv).forEach(function (s) {
        rows.push({ slip_no: s.slipNo, mgmt_no: s.mgmtNo, mall_order_no: s.mallOrderNo, source_file: inv.getName() });
      });
    });
    if (rows.length === 0) {
      result.status = "error: 納品書から伝票番号を1件も抽出できず";
      return result; // 消さない
    }

    var payload = {
      run_id: runId,
      folder: folder.getName(),
      ship_date: Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyy-MM-dd"),
      extracted_at: Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyy-MM-dd HH:mm:ss"),
      rows: rows,
    };
    var resp = UrlFetchApp.fetch(base + "/ingest", {
      method: "post",
      contentType: "application/json",
      headers: { Authorization: "Bearer " + token },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    });
    if (resp.getResponseCode() !== 200) {
      result.status = "error: ingest HTTP " + resp.getResponseCode() + " " + resp.getContentText().slice(0, 200);
      return result; // 消さない (翌日の実行で再送される。サーバ側で冪等)
    }
    result.slips = rows.length;

    if (dryRun) {
      result.status = "dry-run: " + rows.length + "伝票送信、削除せず";
    } else {
      files.forEach(function (f) { f.setTrashed(true); });
      result.status = "ok: " + rows.length + "伝票送信→" + files.length + "ファイル削除";
    }
  } catch (e) {
    result.status = "error: " + e.message; // 消さずに残す
  }
  return result;
}

/** 納品書PDF→一時Docs変換→テキスト抽出→伝票ごとにパース */
function extractSlipsFromPdf_(pdfFile) {
  var tempDoc = Drive.Files.copy(
    { title: "_tmp_" + pdfFile.getName() },
    pdfFile.getId(),
    { convert: true, ocr: true, ocrLanguage: "ja", supportsAllDrives: true }
  );
  var text;
  try {
    text = DocumentApp.openById(tempDoc.id).getBody().getText();
  } finally {
    Drive.Files.remove(tempDoc.id, { supportsAllDrives: true });
  }

  // 「納品書」で伝票単位にブロック分割し、出荷伝票NO (SPxxx) をアンカーに抽出
  var slips = [];
  var blocks = text.split(/納品書/).slice(1);
  blocks.forEach(function (block) {
    var slipNo = (block.match(/SP\d{8,}/) || [""])[0];
    if (!slipNo) return;
    // 管理番号: 単独で現れる6〜8桁 (郵便番号・電話・JANはハイフン付き/桁数違いで除外される想定)
    var mgmtNo = (block.match(/(?:^|\s)(\d{6,8})(?:\s|$)/m) || ["", ""])[1];
    // モール注文番号: Amazon (AES prefix) / 楽天形式。他モールは dry-run で実物確認して追加する
    var mallOrderNo = (block.match(/[A-Z]{2,4}\d{3}-\d{7}-\d{7}/) ||
                       block.match(/\b\d{6}-\d{8}-\d{10}\b/) ||
                       [""])[0];
    slips.push({ slipNo: slipNo, mgmtNo: mgmtNo || null, mallOrderNo: mallOrderNo || null });
  });
  return slips;
}

function notifyResult_(results, runId, dryRun) {
  var errors = results.filter(function (r) { return /^(error|skip)/.test(r.status); });
  var totalSlips = results.reduce(function (a, r) { return a + r.slips; }, 0);
  var lines = ["*出荷ファイル吸い上げ " + (dryRun ? "[DRY-RUN] " : "") + runId + "*",
               "処理: " + results.length + "フォルダ / 送信: " + totalSlips + "伝票"];
  if (errors.length > 0) {
    lines.push("⚠️ 要確認 (ファイル残置):");
    errors.forEach(function (r) { lines.push("・" + r.name + " → " + r.status); });
  }
  var webhook = PropertiesService.getScriptProperties().getProperty("SHIPPING_GCHAT_WEBHOOK");
  if (webhook && (errors.length > 0 || dryRun)) {
    UrlFetchApp.fetch(webhook, {
      method: "post", contentType: "application/json",
      payload: JSON.stringify({ text: lines.join("\n") }),
      muteHttpExceptions: true,
    });
  }
  Logger.log(lines.join("\n"));
}
