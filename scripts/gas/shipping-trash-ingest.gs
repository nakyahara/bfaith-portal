/**
 * 出荷_no 掃除 + 出荷実績吸い上げ (shipping-log 連携版)
 *
 * 毎夕トリガー (18:16) で実行。各 出荷_XX フォルダについて:
 *   1. 納品書PDF → Docs 変換でテキスト抽出し、伝票ごとに
 *      出荷伝票NO (SPxxx) / 管理番号 / モール注文番号 をパース
 *   2. Render (bfaith-portal) の POST /apps/shipping-log/api/ingest へ送信
 *   2.5. ピッキングリストPDF → 総合計/ページ内合計を検算付きで抽出し
 *        POST /apps/shipping-log/api/ingest-picking へ送信 (付加情報。
 *        失敗しても伝票取込は成立扱いだが、証跡保全のためピッキングPDFのみ隔離)
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
  // ピッキングリストPDF (伝票とは独立に処理。失敗時はこのPDFのみ隔離し伝票削除は止めない)
  var pickingAllFiles = files.filter(function (f) {
    return /ピッキングリスト/.test(f.getName()) && f.getMimeType() === "application/pdf";
  });
  var pickingFail = null;
  var pickingNote = "";
  try {
    var invoices = files.filter(function (f) {
      return /納品書/.test(f.getName()) && f.getMimeType() === "application/pdf";
    });
    // 同名・同内容 (md5) の納品書PDFは複製 (過去の変換ジャンク・二重アップロード) と
    // みなし1つに絞る (2026-07-18: Drive APIがv3だった時期の実行が元と同名のPDFコピーを
    // 残しており、全バッチが「2ファイル×同一伝票」で重複検出になった)。
    // 同名でも内容が異なれば両方処理され、伝票番号重複の検出で隔離へ倒れる (Codex R2 medium)
    invoices = dedupeByContent_(invoices);
    var rows = [];
    if (invoices.length === 0 && pickingAllFiles.length > 0) {
      // ピッキングPDFのみ残存 (隔離リトライで伝票は取込済み・ピッキングだけ失敗したケース)
      // → 伝票フェーズはスキップして下のピッキングフェーズだけ実行
    } else if (invoices.length === 0) {
      failReason = "納品書PDFなし(誤配置?)";
    } else {
      var incomplete = [];
      invoices.forEach(function (inv) {
        var ex = extractSlipsFromPdf_(inv);
        // 完全性: 全文の伝票番号数 = マージ後の伝票数、かつ複数伝票が融合したブロックが無いこと
        if (ex.slips.length !== ex.expected || ex.fusionBlocks > 0) {
          incomplete.push(inv.getName() + " (抽出" + ex.slips.length + "/全文" + ex.expected +
                          (ex.fusionBlocks > 0 ? "、融合ブロック" + ex.fusionBlocks : "") + ")");
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
        // ファイル内はマージ済みなので、ここに来るのは複数の納品書PDF間の重複 (再印刷など)
        failReason = "複数ファイル間で伝票番号の重複検出 (" + rows.length + "行中" + slipNos.length + "種)";
      }
    }

    if (!failReason && rows.length > 0) {
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

  // ── ピッキングリストフェーズ (伝票フェーズが成立した場合のみ。失敗時は当該PDFだけ隔離) ──
  if (!failReason && pickingAllFiles.length > 0) {
    var pk = ingestPickingLists_(pickingAllFiles, batchName, shipDate, cfg, runId);
    pickingFail = pk.fail;
    pickingNote = pk.note;
  } else if (!failReason) {
    pickingNote = "ピッキングリストなし";
  }

  // ── 後始末: 出荷_XX を必ず空にする (成功=ゴミ箱 / 失敗=隔離移動)。DRY_RUN は何もしない ──
  if (cfg.dryRun) {
    result.status = failReason
      ? "dry-run NG: " + failReason + " (本番なら隔離)"
      : pickingFail
        ? "dry-run NG: 伝票OK(" + result.slips + "件) / ピッキングNG: " + pickingFail + " (本番ならPDFのみ隔離)"
        : "dry-run OK: " + result.slips + "伝票送信 / " + pickingNote + " (本番なら削除)";
    return result;
  }
  if (!failReason) {
    var filesToTrash = files;
    var pickingInfo = pickingNote ? " / " + pickingNote : "";
    if (pickingFail) {
      // ピッキングPDFのみ隔離して証跡保全。サブフォルダ名は通常隔離と同形式にして
      // 翌晩の retryQuarantine_ に乗せる (納品書なし+ピッキングありの分岐で再処理される)
      var pickIds = {};
      pickingAllFiles.forEach(function (f) { pickIds[f.getId()] = true; });
      var asideInfo = quarantine
        ? moveAllToQuarantine_(pickingAllFiles, quarantine, runId, batchName)
        : "隔離サブフォルダに残置 (翌晩リトライ)";
      filesToTrash = files.filter(function (f) { return !pickIds[f.getId()]; });
      pickingInfo = " / ★ピッキングNG: " + pickingFail + " → " + asideInfo;
    }
    var t = trashFiles_(filesToTrash, quarantine, runId, result.name);
    if (t.quarantined.length === 0 && t.left.length === 0) {
      result.status = (pickingFail ? "warn: " : "ok: ") +
        result.slips + "伝票送信→" + filesToTrash.length + "ファイル削除" + pickingInfo;
    } else {
      // 送信済みなのでデータは安全。ファイルの行き先を正確に報告する (隔離済みと残置は別物)
      var parts = [];
      if (t.quarantined.length > 0) parts.push("隔離: " + t.quarantined.join(", "));
      if (t.left.length > 0) parts.push("★フォルダに残置 (要手動削除): " + t.left.join(", "));
      result.status = "warn: 送信済みだが削除失敗 → " + parts.join(" / ") + pickingInfo;
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
 * ピッキングリストPDF群を抽出→POST /ingest-picking。伝票フェーズ成功後にのみ呼ばれる。
 * 失敗は { fail } で返し、呼び出し側が当該PDFのみ隔離する (伝票の削除は妨げない)。
 */
function ingestPickingLists_(pickingFilesAll, batchName, shipDate, cfg, runId) {
  try {
    // 同名・同内容 (md5) の複製は1つに絞る。同名で内容が異なる場合は両方送られ、
    // サーバ側の source_file 重複 400 → 隔離 (安全側)
    var picks = dedupeByContent_(pickingFilesAll);
    var rows = picks.map(function (f) {
      var ex = parsePickingText_(ocrPdfText_(f));
      return {
        source_file: f.getName(),
        total_qty: ex.totalQty,
        pages: ex.pages,
        page_totals: ex.pageTotals,
        work_date_on_list: ex.workDate,
        printed_at: ex.printedAt,
      };
    });
    var resp = UrlFetchApp.fetch(cfg.base + "/ingest-picking", {
      method: "post",
      contentType: "application/json",
      headers: { Authorization: "Bearer " + cfg.token },
      payload: JSON.stringify({
        run_id: runId,
        folder: batchName,
        ship_date: shipDate,
        extracted_at: Utilities.formatDate(new Date(), "Asia/Tokyo", "yyyy-MM-dd HH:mm:ss"),
        rows: rows,
      }),
      muteHttpExceptions: true,
    });
    var code = resp.getResponseCode();
    var body = null;
    try { body = JSON.parse(resp.getContentText()); } catch (e) { /* 下で non-JSON として扱う */ }
    if (code !== 200 || !body || body.ok !== true || (body.inserted + body.ignored) !== rows.length) {
      return { fail: "ingest-picking HTTP " + code + " " + resp.getContentText().slice(0, 200), note: "" };
    }
    var totalSum = rows.reduce(function (a, r) { return a + r.total_qty; }, 0);
    return { fail: null, note: "ピッキング合計" + totalSum + "個" + (rows.length > 1 ? " (" + rows.length + "リスト)" : "") };
  } catch (e) {
    return { fail: e.message, note: "" };
  }
}

/**
 * トータルピッキングリストのOCRテキストから総合計・ページ内合計を抽出する。
 *
 * GAS の Docs 変換 OCR の実出力 (2026-07-19 実測) は「総合計\n67\nページ内合計\n35」の
 * ようにラベル直後に値が分離して並ぶ。この形式のみを受理し、
 * 全ページの総合計が同値・ページ内合計の総和が総合計に一致する検算を必須とする。
 * 検算が合わない/形式が想定外なら throw → 呼び出し側でPDF隔離+GChat通知
 * (誤った合計を保存するより人手確認を選ぶ。全文の数字走査によるフォールバックは
 * 無関係な数字が偶然検算を通る偽解リスクがあるため置かない — Codex high ×2)。
 * @returns {{ totalQty: number, pages: number, pageTotals: number[], workDate: string|null, printedAt: string|null }}
 */
function parsePickingText_(text) {
  var pages = (text.match(/ページ内合計/g) || []).length;
  if (pages === 0) throw new Error("ピッキングリスト形式でない (『ページ内合計』が見つからない)");

  var totals = extractLabelAdjacentTotals_(text, pages);

  // 出力日時 (日付+時刻の並び)。作業日は最頻出の日付を採用 (賞味期限等の紛れ込み対策)、
  // タイなら null (参考値なので誤値より欠損を選ぶ)
  var pm = text.match(/(\d{4}\/\d{2}\/\d{2}) (\d{2}:\d{2}:\d{2})/);
  var printedAt = pm ? pm[1].replace(/\//g, "-") + " " + pm[2] : null;
  var counts = {};
  (text.match(/\d{4}\/\d{2}\/\d{2}/g) || []).forEach(function (d) { counts[d] = (counts[d] || 0) + 1; });
  var best = null, bestN = 0, tie = false;
  Object.keys(counts).forEach(function (d) {
    if (counts[d] > bestN) { best = d; bestN = counts[d]; tie = false; }
    else if (counts[d] === bestN) tie = true;
  });
  var workDate = best && !tie ? best.replace(/\//g, "-") : null;

  return {
    totalQty: totals.total,
    pages: pages,
    pageTotals: totals.pageTotals,
    workDate: workDate,
    printedAt: printedAt,
  };
}

var PICKING_MAX_TOTAL = 99999; // サーバ側 MAX_TOTAL_QTY と同じ上限

/**
 * ラベル直後の数値抽出。各ページに「総合計 <T>」「ページ内合計 <p>」が1組ずつ
 * 出る前提で、T が全ページ同値・Σp === T のときのみ採用、それ以外はすべて throw。
 * 値は改行を挟むことがある ([\s　]*)。数値は純粋な数字トークンのみ受理し、
 * 直後にカンマや数字が続く形 (「6,7」等の想定外OCR) は不成立扱い
 * (Codex high: カンマ除去の寛容さが偽解を作る)。
 */
function extractLabelAdjacentTotals_(text, pages) {
  var grand = matchLabelNumbers_(text, /総合計[\s　]*(\d+)(?![\d,.])/g);
  var pageTotals = matchLabelNumbers_(text, /ページ内合計[\s　]*(\d+)(?![\d,.])/g);
  var reason = null;
  if (grand.length !== pages || pageTotals.length !== pages) {
    reason = "ラベル直後の数値件数が不一致 (総合計" + grand.length + "件・ページ内合計" + pageTotals.length + "件・" + pages + "ページ。OCR形式が想定外の可能性)";
  } else {
    var total = grand[0];
    for (var i = 1; i < grand.length; i++) {
      if (grand[i] !== total) { reason = "総合計がページ間で不一致 (" + grand.join(",") + ")"; break; }
    }
    var sum = pageTotals.reduce(function (a, b) { return a + b; }, 0);
    if (!reason && (total < 1 || total > PICKING_MAX_TOTAL || sum !== total)) {
      reason = "検算不一致 (総合計" + total + " vs ページ内合計の和" + sum + ")";
    }
  }
  if (reason) throw new Error("ピッキング総合計を特定できず (" + reason + ")");
  return { total: grand[0], pageTotals: pageTotals };
}

function matchLabelNumbers_(text, re) {
  var out = [];
  var m;
  while ((m = re.exec(text)) !== null) out.push(parseInt(m[1], 10));
  return out;
}

/** 動作確認用: 指定フォルダ (省略時 出荷_01) のピッキングリストを抽出してログに出す。削除・送信しない */
function debugPickingPreview(folderName) {
  var root = DriveApp.getFolderById(ROOT_FOLDER_ID);
  var it = root.getFoldersByName(folderName || "出荷_01");
  if (!it.hasNext()) { Logger.log("フォルダなし"); return; }
  var files = it.next().getFiles();
  while (files.hasNext()) {
    var f = files.next();
    if (!/ピッキングリスト/.test(f.getName()) || f.getMimeType() !== "application/pdf") continue;
    try {
      Logger.log(f.getName() + " → " + JSON.stringify(parsePickingText_(ocrPdfText_(f))));
    } catch (e) {
      Logger.log(f.getName() + " → 抽出失敗: " + e.message);
    }
  }
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
 * 納品書PDF→一時Docs変換→テキスト抽出→伝票ごとにパース (同一伝票の複数ブロックはマージ)。
 * 完全抽出の判定 (呼び出し側で削除拒否に使う):
 *  - fusionBlocks === 0 (1ブロックに複数伝票のSPが混ざる=「納品書」見出しのOCR欠け)
 *  - slips.length === expected (PDF全文の distinct SP番号数と一致)
 * @returns {{ expected: number, fusionBlocks: number, slips: Array<{slipNo,mgmtNo,mallOrderNo}> }}
 */
function extractSlipsFromPdf_(pdfFile) {
  var text = ocrPdfText_(pdfFile);

  // OCR変換ではレイアウト都合で1伝票が複数ブロックに割れる (「納品書」の語が
  // 伝票あたり2回出る等、2026-07-18 DRY-RUN実測)。同一SP番号のブロックはマージし、
  // フィールドは全断片から候補を集めて「distinct 1件のときのみ採用」とする。
  var blocks = text.split(/納品書/).slice(1);
  var bySlip = {};   // slipNo -> { mgmt: {候補}, mall: {候補} }
  var order = [];    // 出現順維持
  var fusionBlocks = 0;
  blocks.forEach(function (block) {
    var found = distinct_(block.match(/SP\d{8,14}/g) || []);
    if (found.length === 0) return;               // SPを含まない断片は無視 (レイアウト分割の余り)
    if (found.length > 1) { fusionBlocks++; return; } // 複数伝票が融合したブロック → 呼び出し側で削除拒否
    var slipNo = found[0];
    if (!bySlip[slipNo]) { bySlip[slipNo] = { mgmt: {}, mall: {} }; order.push(slipNo); }
    // 管理番号候補 (単独で現れる7桁数字)。断片をまたいで distinct 収集
    (block.match(/(?:^|\s)\d{7}(?=\s|$)/gm) || []).forEach(function (v) { bySlip[slipNo].mgmt[v.trim()] = true; });
    var mall = extractMallOrderNo_(block);
    if (mall) bySlip[slipNo].mall[mall] = true;
  });
  var slips = order.map(function (slipNo) {
    var mg = Object.keys(bySlip[slipNo].mgmt);
    var ml = Object.keys(bySlip[slipNo].mall);
    // 候補が複数=曖昧なら null (誤った番号を恒久保存するより欠損を選ぶ。
    // slip_no があれば後段でロジザード/NEから復元可能)
    return {
      slipNo: slipNo,
      mgmtNo: mg.length === 1 ? mg[0] : null,
      mallOrderNo: ml.length === 1 ? ml[0] : null,
    };
  });
  // 全文の distinct SP番号数が「この納品書に存在する伝票数」の正。マージ後と一致しなければ不完全
  var allSlipNos = distinct_(text.match(/SP\d{8,14}/g) || []);
  return { expected: allSlipNos.length, fusionBlocks: fusionBlocks, slips: slips };
}

/** Drive API v2 の md5Checksum で内容同一性を判定。取れない場合は null (=複製と断定しない) */
function fileMd5_(f) {
  try {
    var meta = Drive.Files.get(f.getId(), { supportsAllDrives: true });
    if (meta && meta.md5Checksum) return meta.md5Checksum;
  } catch (e) { /* null を返して呼び出し側で「複製扱いしない」に倒す */ }
  return null;
}

/**
 * 同名・同内容 (md5) のPDF複製を1つに絞る共通ヘルパー。
 * md5 が取れないファイルは複製と断定できないため除外しない (Codex R2 medium:
 * サイズ代用だと同名同サイズ異内容を取りこぼしたまま削除しうる)。
 * 除外しなかった同名ファイルは抽出側の重複検出 (伝票番号重複 / source_file 重複 400)
 * に引っかかり隔離へ倒れる = 安全側。
 */
function dedupeByContent_(files) {
  var seen = {};
  return files.filter(function (f) {
    var md5 = fileMd5_(f);
    if (!md5) return true;
    var k = f.getName() + "::" + md5;
    if (seen[k]) return false;
    seen[k] = true;
    return true;
  });
}

/** PDF → 一時Docs変換 (OCR) → 全文テキスト。納品書・ピッキングリスト共通 */
function ocrPdfText_(pdfFile) {
  var tempDoc = Drive.Files.copy(
    { title: "_tmp_" + pdfFile.getName() },
    pdfFile.getId(),
    { convert: true, ocr: true, ocrLanguage: "ja", supportsAllDrives: true }
  );
  try {
    return DocumentApp.openById(tempDoc.id).getBody().getText();
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
