/**
 * 出荷_no 掃除 + 出荷実績吸い上げ (shipping-log 連携版)
 *
 * 毎夕トリガー (18:16) で実行。各 出荷_XX フォルダについて:
 *   1. 納品書PDF → Docs 変換でテキスト抽出し、伝票ごとに
 *      出荷伝票NO (SPxxx) / 管理番号 / モール注文番号 をパース
 *   2. Render (bfaith-portal) の POST /apps/shipping-log/api/ingest へ送信
 *   3. 完全成功 → ファイルをゴミ箱へ / 失敗 → ルート直下の「_要確認」へ隔離移動
 *
 * ★出荷_XX フォルダは毎晩必ず空になる★ (成功=ゴミ箱、失敗=隔離)。
 * 失敗分を作業フォルダに残すと翌朝の出荷作業と混ざって混乱するため、
 * 隔離フォルダ _要確認/<実行ID>_<出荷_XX>/ に退避し GChat に通知する。
 * 隔離分は毎回の実行冒頭で自動リトライし (取込は冪等)、成功したら片付ける。
 *
 * 削除してよい条件 (すべて満たすこと):
 *   - 納品書の全ブロックから出荷伝票NOが抽出できた (部分抽出・伝票融合・番号重複は不可)
 *   - HTTP 200 + body.ok===true + inserted+ignored===total===送信行数
 *   - ignored がすべて同一フォルダからの再送 (別フォルダ由来=OCR誤認の疑いは不可)
 *
 * 事前設定 (スクリプトプロパティ):
 *   SHIPPING_INGEST_BASE : 例 https://bfaith-portal.onrender.com/apps/shipping-log/api
 *   SHIPPING_INGEST_TOKEN: Render env SHIPPING_LOG_INGEST_TOKEN と同じ値
 *   DRY_RUN              : "1" なら抽出+送信のみ、ファイル移動・削除しない (動作確認用)
 *   SHIPPING_GCHAT_WEBHOOK: 通知先 webhook URL (任意)
 * 事前設定 (サービス): 「Drive API」(v2) を追加しておくこと
 *
 * サーバ側契約: apps/shipping-log/router.js (Bearer fail-closed、slip_no PK で冪等、
 * 内容不一致は 409)。再実行・日跨ぎ再送・隔離リトライは slip_no で重複排除されるため安全。
 */
var ROOT_FOLDER_ID = "110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh";
var QUARANTINE_NAME = "_要確認";
var SHIP_FOLDER_RE = /^出荷_\d+$/;
var QUARANTINE_SUB_RE = /^(\d{8})-\d{6}_(出荷_\d+)$/; // <yyyyMMdd-HHmmss>_<出荷_XX>

function trashAllFilesInFolder() {
  var props = PropertiesService.getScriptProperties();
  var cfg = {
    base: props.getProperty("SHIPPING_INGEST_BASE"),
    token: props.getProperty("SHIPPING_INGEST_TOKEN"),
    dryRun: props.getProperty("DRY_RUN") === "1",
  };
  if (!cfg.base || !cfg.token) throw new Error("SHIPPING_INGEST_BASE / SHIPPING_INGEST_TOKEN が未設定です");

  var runId = Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyyMMdd-HHmmss");
  var root = DriveApp.getFolderById(ROOT_FOLDER_ID);
  var quarantine = getOrCreateSubFolder_(root, QUARANTINE_NAME);
  var results = [];

  // 1) 前回までの隔離分をリトライ (冪等なので再送しても安全)
  retryQuarantine_(quarantine, cfg, results);

  // 2) 当日分: 出荷_XX のみ処理 (_要確認 等は対象外)
  var subFolders = root.getFolders();
  while (subFolders.hasNext()) {
    var folder = subFolders.next();
    if (!SHIP_FOLDER_RE.test(folder.getName())) continue;
    var r = processBatch_(folder, folder.getName(), todayJst_(), cfg, quarantine, runId);
    if (r) results.push(r);
  }
  notifyResult_(results, runId, cfg.dryRun);
}

/**
 * 1バッチ分: 抽出→POST→成功はゴミ箱 / 失敗は隔離へ移動。
 * @param {Folder} folder 実ファイルのあるフォルダ (出荷_XX or 隔離サブフォルダ)
 * @param {string} batchName 業務上のフォルダ名 (出荷_XX。隔離リトライ時は元の名前)
 * @param {string} shipDate YYYY-MM-DD (隔離リトライ時は隔離時の日付)
 * @param {Folder|null} quarantine 失敗時の隔離先ルート (隔離リトライ時は null=移動済みなので何もしない)
 */
function processBatch_(folder, batchName, shipDate, cfg, quarantine, runId) {
  var files = [];
  var it = folder.getFiles();
  while (it.hasNext()) files.push(it.next());
  if (files.length === 0) return null; // 未使用フォルダ

  var result = { name: batchName, files: files.length, slips: 0, status: "" };
  var failReason = null;
  try {
    var invoices = files.filter(function (f) {
      return /納品書/.test(f.getName()) && f.getMimeType() === "application/pdf";
    });
    var rows = [];
    if (invoices.length === 0) {
      failReason = "納品書PDFなし(誤配置?)";
    } else {
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
      var slipNos = distinct_(rows.map(function (r) { return r.slip_no; }));
      if (incomplete.length > 0) {
        failReason = "部分抽出 " + incomplete.join(", ");
      } else if (rows.length === 0) {
        failReason = "納品書から伝票番号を1件も抽出できず";
      } else if (slipNos.length !== rows.length) {
        // 別々の伝票のSP番号をOCRが同一値に誤認すると ignored に化けて記録が欠落するため削除不可
        failReason = "伝票番号の重複検出 (" + rows.length + "行中" + slipNos.length + "種、OCR誤認の可能性)";
      }
    }

    if (!failReason) {
      var payload = {
        run_id: runId,
        folder: batchName,
        ship_date: shipDate,
        extracted_at: Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyy-MM-dd HH:mm:ss"),
        rows: rows,
      };
      var resp = UrlFetchApp.fetch(cfg.base + "/ingest", {
        method: "post",
        contentType: "application/json",
        headers: { Authorization: "Bearer " + cfg.token },
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
      });
      var verdict = verifyIngestResponse_(resp, rows.length, batchName);
      if (!verdict.ok) failReason = verdict.reason;
      else result.slips = rows.length;
    }
  } catch (e) {
    failReason = e.message;
  }

  // ── 後始末: 出荷_XX を必ず空にする (成功=ゴミ箱 / 失敗=隔離移動)。DRY_RUN は何もしない ──
  if (cfg.dryRun) {
    result.status = failReason
      ? "dry-run NG: " + failReason + " (本番なら隔離)"
      : "dry-run OK: " + result.slips + "伝票送信 (本番なら削除)";
    return result;
  }
  if (!failReason) {
    var t = trashFiles_(files, quarantine, runId, result.name);
    if (t.quarantined.length === 0 && t.left.length === 0) {
      result.status = "ok: " + result.slips + "伝票送信→" + files.length + "ファイル削除";
    } else {
      // 送信済みなのでデータは安全。ファイルの行き先を正確に報告する (隔離済みと残置は別物)
      var parts = [];
      if (t.quarantined.length > 0) parts.push("隔離: " + t.quarantined.join(", "));
      if (t.left.length > 0) parts.push("★フォルダに残置 (要手動削除): " + t.left.join(", "));
      result.status = "warn: 送信済みだが削除失敗 → " + parts.join(" / ");
    }
  } else if (quarantine) {
    var movedInfo = moveAllToQuarantine_(files, quarantine, runId, result.name);
    result.status = "error: " + failReason + " → " + movedInfo;
  } else {
    // 隔離リトライ中の失敗: すでに隔離内なのでそのまま残す
    result.status = "error(隔離継続): " + failReason;
  }
  return result;
}

/**
 * 成功バッチのゴミ箱移動。ゴミ箱に入らないファイルは隔離へ退避を試みる。
 * @returns {{ quarantined: string[], left: string[] }} 隔離できたもの / フォルダに残ってしまったもの
 */
function trashFiles_(files, quarantine, runId, batchName) {
  var quarantined = [];
  var left = [];
  var sub = null;
  files.forEach(function (f) {
    try { f.setTrashed(true); return; }
    catch (e) { /* 次で隔離を試みる */ }
    try {
      if (!sub && quarantine) sub = getOrCreateSubFolder_(quarantine, runId + "_" + batchName);
      if (sub) { f.moveTo(sub); quarantined.push(f.getName()); }
      else left.push(f.getName()); // 隔離リトライ中 (quarantine=null) は隔離サブに残る
    } catch (e2) {
      left.push(f.getName()); // 両方失敗 → 元フォルダに残置 (通知で要手動対応と明示)
    }
  });
  return { quarantined: quarantined, left: left };
}

/** 失敗バッチの全ファイルを _要確認/<runId>_<出荷_XX>/ へ移動 */
function moveAllToQuarantine_(files, quarantine, runId, batchName) {
  var sub = getOrCreateSubFolder_(quarantine, runId + "_" + batchName);
  var moved = 0;
  var left = [];
  files.forEach(function (f) {
    try { f.moveTo(sub); moved++; }
    catch (e) { left.push(f.getName()); }
  });
  return left.length === 0
    ? "隔離へ" + moved + "ファイル移動 (フォルダは空)"
    : "隔離移動失敗あり→フォルダに残置: " + left.join(", ");
}

/** 隔離フォルダの各バッチを再処理。成功したらファイルをゴミ箱に入れサブフォルダも片付ける */
function retryQuarantine_(quarantine, cfg, results) {
  var subs = quarantine.getFolders();
  while (subs.hasNext()) {
    var sub = subs.next();
    var m = QUARANTINE_SUB_RE.exec(sub.getName());
    if (!m) continue; // 人間が置いた別物には触らない
    var shipDate = m[1].slice(0, 4) + "-" + m[1].slice(4, 6) + "-" + m[1].slice(6, 8);
    var r = processBatch_(sub, m[2], shipDate, cfg, null, // quarantine=null: 失敗してもここに留める
      Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyyMMdd-HHmmss"));
    if (r) {
      r.name = "隔離リトライ:" + sub.getName();
      results.push(r);
      // 成功して空になったサブフォルダは片付ける (DRY_RUN 時は残す)
      if (!cfg.dryRun && /^ok:/.test(r.status) && !sub.getFiles().hasNext()) {
        try { sub.setTrashed(true); } catch (e) { /* 残っても実害なし */ }
      }
    }
  }
}

/**
 * ingest レスポンス検証。削除許可の条件:
 *  - HTTP 200 + ok:true + total===sent + inserted+ignored===sent
 *  - すべての ignored が「同一フォルダからの再送」であること。別フォルダ由来の ignored は
 *    過去取込済み伝票への OCR 誤認の疑いがあるため削除見送り
 */
function verifyIngestResponse_(resp, sentCount, folderName) {
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
  var foreign = (body.ignored_details || []).filter(function (d) { return d.folder_name !== folderName; });
  if (foreign.length > 0 || (body.ignored > 0 && !body.ignored_details)) {
    var names = foreign.map(function (d) { return d.slip_no + "(既存:" + d.folder_name + ")"; }).join(", ");
    return { ok: false, reason: "ignored に別フォルダ由来あり (OCR誤認の疑い): " + (names || "詳細なし") };
  }
  return { ok: true };
}

/**
 * 納品書PDF→一時Docs変換→テキスト抽出→伝票ごとにパース。
 * 完全抽出の判定 (呼び出し側で削除拒否に使う):
 *  - 各ブロックの distinct SP番号がちょうど1件 (0件=見出しはあるが番号欠落、
 *    2件以上=「納品書」見出しのOCR欠けで2伝票が1ブロックに融合)
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
    // 後片付けは「ゴミ箱行き」にする。files.delete (完全削除) は共有ドライブでは
    // 管理者権限が必要で、コンテンツ管理者だと File not found 相当で拒否される (2026-07-18 実測)。
    // ゴミ箱なら作成者権限で可能、30日で自動消滅する。
    try {
      DriveApp.getFileById(tempDoc.id).setTrashed(true);
    } catch (e) {
      // 万一ゴミ箱行きも失敗しても抽出は続行 (_tmp_ 名の一時Docが残るだけで実害なし)
      Logger.log("一時Docのゴミ箱移動失敗: " + tempDoc.id + " " + e.message);
    }
  }

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
  var allSlipNos = distinct_(text.match(/SP\d{8,14}/g) || []);
  var effectiveBlockCount = Math.max(blocks.length, allSlipNos.length);
  return { blockCount: effectiveBlockCount, slips: slips };
}

/**
 * 管理番号 (NE伝票番号、現行7桁): ブロック内の「単独で現れる7桁数字」の distinct 候補が
 * ちょうど1つのときのみ採用。曖昧なら null (誤った番号を恒久保存するより欠損を選ぶ。
 * slip_no があれば後段でロジザード/NEから復元可能)。
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

function getOrCreateSubFolder_(parent, name) {
  var it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : parent.createFolder(name);
}

function todayJst_() {
  return Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyy-MM-dd");
}

function distinct_(arr) {
  var seen = {};
  var out = [];
  arr.forEach(function (v) { if (!seen[v]) { seen[v] = true; out.push(v); } });
  return out;
}

function notifyResult_(results, runId, dryRun) {
  var errors = results.filter(function (r) { return /^(error|warn|dry-run NG)/.test(r.status); });
  var totalSlips = results.reduce(function (a, r) { return a + r.slips; }, 0);
  var lines = ["*出荷ファイル吸い上げ " + (dryRun ? "[DRY-RUN] " : "") + runId + "*",
               "処理: " + results.length + "フォルダ / 送信: " + totalSlips + "伝票"];
  if (errors.length > 0) {
    lines.push("⚠️ 要確認 (" + QUARANTINE_NAME + " フォルダを確認):");
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
