/**
 * 出荷_no 掃除 + 出荷実績吸い上げ (shipping-log 連携版)
 *
 * 毎夕トリガー (18:16) で実行。各 出荷_XX フォルダについて:
 *   1. 納品書PDF → Docs 変換でテキスト抽出し、伝票ごとに
 *      出荷伝票NO (SPxxx) / 管理番号 / モール注文番号 をパース
 *   2. Render (bfaith-portal) の POST /apps/shipping-log/api/ingest へ送信
 *   3. ★送信が完全に成功したフォルダのみ★ ファイルをゴミ箱へ移動
 *      (部分抽出・送信失敗・conflict のフォルダはファイルを残して GChat に通知)
 *
 * 削除してよい条件 (すべて満たすこと):
 *   - 納品書の全ブロックから出荷伝票NOが抽出できた (部分抽出は削除しない)
 *   - HTTP 200 かつ body.ok === true かつ inserted + ignored === total === 送信行数
 *
 * 事前設定 (スクリプトプロパティ):
 *   SHIPPING_INGEST_BASE : 例 https://bfaith-portal.onrender.com/apps/shipping-log/api
 *   SHIPPING_INGEST_TOKEN: Render env SHIPPING_LOG_INGEST_TOKEN と同じ値
 *   DRY_RUN              : "1" なら抽出+送信のみ、ファイルは削除しない (動作確認用)
 *   SHIPPING_GCHAT_WEBHOOK: 通知先 webhook URL (任意)
 * 事前設定 (サービス): 「Drive API」(v2) を追加しておくこと
 *
 * サーバ側契約: apps/shipping-log/router.js (Bearer fail-closed、slip_no PK で冪等、
 * 内容不一致は 409)。再実行・日跨ぎ再送は slip_no で重複排除されるため安全。
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

/** 1フォルダ分: 抽出→POST→(完全成功のときのみ)ゴミ箱。結果オブジェクトを返す */
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

    // ── 抽出 (完全性チェック付き: ブロック数 = 抽出伝票数 でなければ削除しない) ──
    var rows = [];
    var incomplete = [];
    invoices.forEach(function (inv) {
      var ex = extractSlipsFromPdf_(inv);
      if (ex.blockCount !== ex.slips.length) {
        incomplete.push(inv.getName() + " (" + ex.slips.length + "/" + ex.blockCount + "伝票)");
      }
      ex.slips.forEach(function (s) {
        rows.push({ slip_no: s.slipNo, mgmt_no: s.mgmtNo, mall_order_no: s.mallOrderNo, source_file: inv.getName() });
      });
    });
    if (incomplete.length > 0) {
      result.status = "error: 部分抽出 " + incomplete.join(", ") + " → 削除見送り";
      return result; // 消さない
    }
    if (rows.length === 0) {
      result.status = "error: 納品書から伝票番号を1件も抽出できず";
      return result; // 消さない
    }
    // フォルダ全体で slip_no が一意であること (Codex R3 high: 別伝票のSP番号を
    // OCRが同一に誤認すると、サーバ側で ignored に化けて記録が欠落したまま削除される)
    var slipNos = distinct_(rows.map(function (r) { return r.slip_no; }));
    if (slipNos.length !== rows.length) {
      result.status = "error: 伝票番号の重複検出 (" + rows.length + "行中" + slipNos.length +
                      "種、OCR誤認の可能性) → 削除見送り";
      return result; // 消さない
    }

    // ── 送信 (200 + body 検証。ステータスだけでは信頼しない) ──
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
    var verdict = verifyIngestResponse_(resp, rows.length);
    if (!verdict.ok) {
      result.status = "error: " + verdict.reason;
      return result; // 消さない (再送はサーバ側で冪等)
    }
    result.slips = rows.length;

    // ── 削除 (個別に成否を記録。途中失敗は残置扱いで通知) ──
    if (dryRun) {
      result.status = "dry-run: " + rows.length + "伝票送信、削除せず";
    } else {
      var trashFailed = [];
      files.forEach(function (f) {
        try { f.setTrashed(true); }
        catch (e) { trashFailed.push(f.getName()); }
      });
      result.status = trashFailed.length === 0
        ? "ok: " + rows.length + "伝票送信→" + files.length + "ファイル削除"
        : "warn: 送信済みだが削除失敗あり → 残置: " + trashFailed.join(", ");
    }
  } catch (e) {
    result.status = "error: " + e.message; // 消さずに残す
  }
  return result;
}

/** ingest レスポンス検証。200 + ok:true + 件数一致のみ削除許可 (Codex R1 medium #6) */
function verifyIngestResponse_(resp, sentCount) {
  var code = resp.getResponseCode();
  var text = resp.getContentText();
  if (code !== 200) return { ok: false, reason: "ingest HTTP " + code + " " + text.slice(0, 200) };
  var body;
  try { body = JSON.parse(text); }
  catch (e) { return { ok: false, reason: "ingest 応答がJSONではない: " + text.slice(0, 120) }; }
  if (body.ok !== true) return { ok: false, reason: "ingest ok!==true: " + text.slice(0, 200) };
  if (body.total !== sentCount || (body.inserted + body.ignored) !== sentCount) {
    return { ok: false, reason: "ingest 件数不一致 sent=" + sentCount + " total=" + body.total +
                                " inserted=" + body.inserted + " ignored=" + body.ignored };
  }
  return { ok: true };
}

/**
 * 納品書PDF→一時Docs変換→テキスト抽出→伝票ごとにパース。
 * 完全抽出の判定 (呼び出し側で削除拒否に使う):
 *  - 各ブロックの distinct SP番号がちょうど1件 (0件=見出しはあるが番号欠落、
 *    2件以上=「納品書」見出しのOCR欠けで2伝票が1ブロックに融合 — Codex R2 high)
 *  - PDF全文の distinct SP番号数 === 抽出伝票数 (ブロック分割自体の欠陥検知)
 * @returns {{ blockCount: number, slips: Array<{slipNo,mgmtNo,mallOrderNo}> }}
 */
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
  var blocks = text.split(/納品書/).slice(1);
  var slips = [];
  blocks.forEach(function (block) {
    var found = distinct_(block.match(/SP\d{8,14}/g) || []);
    if (found.length !== 1) return; // 0件 or 融合ブロック → 差分として呼び出し側が検知
    slips.push({
      slipNo: found[0],
      mgmtNo: extractMgmtNo_(block),
      mallOrderNo: extractMallOrderNo_(block),
    });
  });
  // 全文の distinct SP番号数と突合。分割やブロック判定の欠陥で取りこぼした伝票を検知する
  var allSlipNos = distinct_(text.match(/SP\d{8,14}/g) || []);
  var effectiveBlockCount = Math.max(blocks.length, allSlipNos.length);
  return { blockCount: effectiveBlockCount, slips: slips };
}

function distinct_(arr) {
  var seen = {};
  var out = [];
  arr.forEach(function (v) { if (!seen[v]) { seen[v] = true; out.push(v); } });
  return out;
}

/**
 * 管理番号 (NE伝票番号、現行7桁): ブロック内の「単独で現れる7桁数字」の distinct 候補が
 * ちょうど1つのときのみ採用。曖昧なら null (誤った番号を恒久保存するより欠損を選ぶ。
 * slip_no があれば後段でロジザード/NEから復元可能)。(Codex R1 high #4)
 */
function extractMgmtNo_(block) {
  var m = block.match(/(?:^|\s)\d{7}(?=\s|$)/gm) || [];
  var distinct = {};
  m.forEach(function (s) { distinct[s.trim()] = true; });
  var keys = Object.keys(distinct);
  return keys.length === 1 ? keys[0] : null;
}

/** モール注文番号: Amazon (AES prefix) / 楽天形式。他モールは dry-run で実物確認して追加する */
function extractMallOrderNo_(block) {
  var m = block.match(/[A-Z]{2,4}\d{3}-\d{7}-\d{7}/) ||  // Amazon (AESxxx-xxxxxxx-xxxxxxx)
          block.match(/\b\d{6}-\d{8}-\d{10}\b/);          // 楽天
  return m ? m[0] : null;
}

function notifyResult_(results, runId, dryRun) {
  var errors = results.filter(function (r) { return /^(error|skip|warn)/.test(r.status); });
  var totalSlips = results.reduce(function (a, r) { return a + r.slips; }, 0);
  var lines = ["*出荷ファイル吸い上げ " + (dryRun ? "[DRY-RUN] " : "") + runId + "*",
               "処理: " + results.length + "フォルダ / 送信: " + totalSlips + "伝票"];
  if (errors.length > 0) {
    lines.push("⚠️ 要確認 (ファイル残置あり):");
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
