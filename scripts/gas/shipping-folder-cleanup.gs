/**
 * 出荷_no 掃除 (cleanup 専用版)
 *
 * 毎夕トリガー (18:16) で Drive「出荷_no」直下の 出荷_XX フォルダを空にする。
 * ★出荷_XX フォルダは毎晩必ず空になる★ — 前日のファイルが残ると翌朝の出荷作業と
 * 混ざって現場が混乱するため、掃除そのものが業務要件。
 *
 * 【2026-08-26 変更】伝票・ピッキングの「吸い上げ」(shipping-log への送信) を全廃した。
 * 納品書・ピッキングの情報は梱包支援システム (apps/packing) / スマホピッキング
 * (apps/picking) の Drive ポーラーが本番系として取り込んでいるので、PDF から OCR で
 * 再抽出する必要がなくなったため (中原さん判断)。
 * これに伴い _要確認 への隔離・GChat 通知・Render 側 apps/shipping-log の取込 API も撤去。
 * 旧版 (吸い上げつき) は git 履歴 scripts/gas/shipping-trash-ingest.gs を参照。
 *
 * 失敗時の気づき方: 例外を throw するので Apps Script 標準の「実行失敗通知メール」が
 * スクリプト所有者に届く。ゴミ箱へ入れられなかったファイルはフォルダに残るため、
 * 翌晩の実行で自動的に再試行される。
 *
 * 事前設定 (スクリプトプロパティ):
 *   DRY_RUN : "1" なら件数を数えるだけでゴミ箱に入れない (動作確認用)
 *
 * 注意: 共有ドライブでは files.delete (完全削除) に管理者ロールが要るため、
 * 削除は必ず setTrashed(true) (ゴミ箱へ) を使う。
 */
var ROOT_FOLDER_ID = "110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh";
var QUARANTINE_NAME = "_要確認";
var SHIP_FOLDER_RE = /^出荷_\d+$/;
var QUARANTINE_SUB_RE = /^(\d{8})-\d{6}_(出荷_\d+)$/; // 旧版が残した隔離サブフォルダ

/** トリガーのエントリポイント (関数名は旧版から変更しないこと。トリガーの紐付けが切れる) */
function trashAllFilesInFolder() {
  var dryRun = PropertiesService.getScriptProperties().getProperty("DRY_RUN") === "1";
  var root = DriveApp.getFolderById(ROOT_FOLDER_ID);
  var lines = [];
  var folders = 0;
  var trashed = 0;
  var failed = [];

  var it = root.getFolders();
  while (it.hasNext()) {
    var folder = it.next();
    var name = folder.getName();
    if (SHIP_FOLDER_RE.test(name)) {
      var r = trashFilesIn_(folder, dryRun);
      if (r.total === 0) continue; // 未使用フォルダは黙って飛ばす
      folders++;
      trashed += r.trashed;
      lines.push(name + ": " + r.trashed + "/" + r.total + "ファイル" + (dryRun ? " (dry-run)" : ""));
      r.failed.forEach(function (n) { failed.push(name + "/" + n); });
    } else if (name === QUARANTINE_NAME) {
      var q = cleanupQuarantine_(folder, dryRun);
      trashed += q.trashed;
      if (q.total > 0) lines.push(QUARANTINE_NAME + ": " + q.trashed + "/" + q.total + "ファイル (旧隔離分の片付け)");
      q.failed.forEach(function (n) { failed.push(QUARANTINE_NAME + "/" + n); });
    }
  }

  Logger.log("出荷_no 掃除" + (dryRun ? " [DRY-RUN]" : "") + ": " + folders + "フォルダ / " +
             trashed + "ファイルをゴミ箱へ\n" + lines.join("\n"));
  if (failed.length > 0) {
    // 残ったファイルは翌晩の実行で再試行される。恒久的に消せない場合は毎日メールが来るので気づける
    throw new Error("ゴミ箱に入れられなかったファイルがあります (翌晩リトライ): " + failed.join(", "));
  }
}

/** フォルダ直下のファイルを全部ゴミ箱へ。サブフォルダには触らない */
function trashFilesIn_(folder, dryRun) {
  var files = [];
  var it = folder.getFiles();
  while (it.hasNext()) files.push(it.next());

  var trashed = 0;
  var failed = [];
  if (!dryRun) {
    files.forEach(function (f) {
      try { f.setTrashed(true); trashed++; }
      catch (e) { failed.push(f.getName()); }
    });
  }
  return { total: files.length, trashed: trashed, failed: failed };
}

/**
 * 旧版が _要確認 に残した隔離サブフォルダ (<yyyyMMdd-HHmmss>_出荷_XX) を片付ける。
 * 人が手で置いたフォルダ・ファイルには一切触らない (名前が上の形式のものだけ)。
 * 吸い上げを廃止した今、隔離されていたファイルは保全する理由がないのでゴミ箱へ。
 */
function cleanupQuarantine_(quarantine, dryRun) {
  var total = 0;
  var trashed = 0;
  var failed = [];
  var subs = quarantine.getFolders();
  while (subs.hasNext()) {
    var sub = subs.next();
    if (!QUARANTINE_SUB_RE.test(sub.getName())) continue;
    var r = trashFilesIn_(sub, dryRun);
    total += r.total;
    trashed += r.trashed;
    r.failed.forEach(function (n) { failed.push(sub.getName() + "/" + n); });
    // 空になったサブフォルダ自体も片付ける
    if (!dryRun && r.failed.length === 0 && !sub.getFiles().hasNext()) {
      try { sub.setTrashed(true); } catch (e) { /* 空フォルダが残っても実害なし */ }
    }
  }
  return { total: total, trashed: trashed, failed: failed };
}
