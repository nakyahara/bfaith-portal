#!/usr/bin/env node
/**
 * send-yahoo-review-mails.js — Yahoo 版 フォロー/クーポンメール送信 CLI (PR-Y-C4)
 *
 * 楽天版 (send-rakuten-review-mails.js) と同じ状態機械を、Yahoo アダプタ + Gmail 送信で動かす。
 * shadow 中は ownership が全注文 vendor のため send しても構造的に 0 通 (fail-closed)。
 * 実送信が始まるのは cutover (PR-Y-C5) で ownership に self が現れてから。
 *
 * サブコマンド:
 *   plan                      送信対象の件数・スキップ理由 (DB 読み取りのみ、送信なし)
 *   verify-from --to <addr>   **LIVE ゲート**: 1 通実送信し、送信済みメッセージの From を読み戻して
 *                             info@ のまま出ているか確認し、台帳に記録する (90日有効)。
 *                             send-as エイリアスが外れると Gmail は From を黙って差し替えるため
 *   send-test --to <addr> [--template follow|coupon|coupon-low]
 *                             ダミーデータで文面を実送信 (DB 不使用、action を消費しない)
 *   send [--limit N] [--notify]
 *                             ready を at-most-once で実送信 (既定 limit=5)。
 *                             結果不明 (ambiguous) が出たら即中断 + GChat 通知
 *   suppress --email <addr> [--reason <text>]   配信停止の登録 (HMAC のみ保存)
 *   unsuppress --email <addr> --by <名前>       解除 (誰が解除したかを残す)
 *   suppress-import --file <csv> [--live]       vendor の除外対象者 CSV を取込 (原本は削除)
 *   suppress-stats                              登録件数
 *
 * env: DATA_DIR / YAHOO_PROXY_URL・YAHOO_PROXY_SECRET (send: 宛先取得) /
 *      YAHOO_SUPPRESS_HMAC_KEY (配信停止の照合。未設定なら 1 通も送らない) /
 *      INQUIRY_GMAIL_* または PO_GMAIL_* (送信) / GCHAT_WEBHOOK_MALL_FETCH (通知、任意)
 * exit code: 0=成功 / 1=失敗 (ambiguous を含む) / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { createCampaignEngine } from './rakuten-review-campaign-lib.js';
import { createSenderEngine } from './rakuten-review-sender-lib.js';
import { ensureYahooReviewTables } from './yahoo-review-lib.js';
import { ensureYahooCampaignSources } from './yahoo-review-campaign-adapter.js';
import { ensureYahooCouponLedger } from './yahoo-review-coupon-lib.js';
import { createYahooSenderAdapter } from './yahoo-review-sender-adapter.js';
import { TEMPLATE_BUILDERS, sampleContext, SHOP_NAME, FROM_ADDRESS, messageIdFor } from './yahoo-review-mail-lib.js';
import {
  createGmailSender, assertFromVerified, recordFromVerification, ensureFromVerificationLedger, invalidateFromVerification,
} from './yahoo-mail-send-lib.js';
import {
  loadYahooSuppressKey, addSuppression, releaseSuppression, suppressionStats, extractEmails,
} from './yahoo-review-suppression-lib.js';

const args = process.argv.slice(2);
const getArg = (flag) => { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : null;

/** テスト送信の宛先は社内ドメイン限定 (Codex Y-C4 R1 Low: 購入者アドレスを打ち込むと
 *  ログ・送信済みトレイに PII が残る。テストに社外宛が要る場面は無い) */
const INTERNAL_DOMAIN = '@b-faith.biz';
function internalToOrExit(flagLabel) {
  const to = getArg('--to');
  if (!to || !/^[^\s@]+@[^\s@]+$/.test(to)) { console.error(`FATAL: ${flagLabel} には --to <メールアドレス> が必要`); process.exit(2); }
  if (!to.toLowerCase().endsWith(INTERNAL_DOMAIN)) {
    console.error(`FATAL: --to は社内ドメイン (${INTERNAL_DOMAIN}) 宛のみ。購入者アドレスへのテスト送信はできません`);
    process.exit(2);
  }
  return to;
}

const CE = createCampaignEngine('yahoo');
const SE = createSenderEngine(createYahooSenderAdapter());

function openDb() {
  const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
  if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
  const dbPath = path.join(DATA_DIR, 'warehouse.db');
  if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
  ensureYahooReviewTables(db);
  ensureYahooCampaignSources(db);
  CE.ensureCampaignTables(db);
  ensureYahooCouponLedger(db);
  ensureFromVerificationLedger(db);
  return db;
}

async function notifyOperator(text) {
  const webhook = (process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK || '').trim();
  if (!webhook) {
    console.error('[notify] ⚠️GCHAT_WEBHOOK_MALL_FETCH 未設定のため GChat 通知できません (このログが唯一の通知です)');
    return false;
  }
  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) { console.error(`[notify] ⚠️GChat 通知失敗 (HTTP ${res.status})。このログが唯一の通知です`); return false; }
    return true;
  } catch (e) {
    console.error(`[notify] ⚠️GChat 通知失敗 (${e.message})。このログが唯一の通知です`);
    return false;
  }
}

try {
  if (mode === 'plan') {
    const db = openDb();
    try {
      const { eligible, skipped, monthlyCouponReady } = SE.selectEligibleActions(db, { limit: 1000 });
      const ready = db.prepare(`SELECT action_type, COUNT(*) n FROM ${SE.tables.actions} WHERE status = 'ready' GROUP BY action_type`).all();
      const claimed = db.prepare(`SELECT COUNT(*) n FROM ${SE.tables.actions} WHERE status = 'claimed'`).get().n;
      console.log(`[Yahoo send-plan] ready 総数: ${ready.map((r) => `${r.action_type}=${r.n}`).join(' / ') || '0'}`);
      if (claimed > 0) console.log(`[Yahoo send-plan] ⚠️claimed 残留 ${claimed}件 (前回クラッシュの疑い。次回 send 冒頭で ambiguous に回収されます)`);
      console.log(`[Yahoo send-plan] 今すぐ送信可能 (全ゲート通過): ${eligible.length}件`);
      const reasons = new Map();
      for (const s of skipped) reasons.set(s.reason, (reasons.get(s.reason) || 0) + 1);
      for (const [reason, n] of [...reasons.entries()].sort((a, b) => b[1] - a[1])) console.log(`  skip ${reason}: ${n}件`);
      console.log(`[Yahoo send-plan] 当月の月次クーポン: ${monthlyCouponReady ? '✅発行済み' : '❌未発行/期間外 (coupon は送れない — yahoo-review-coupon-issue.mjs)'}`);
      // 表示は実際の LIVE ゲートと同じ判定で出す (Codex Y-C4 R1 Low: 行があるだけで ✅ にすると
      // 不一致・期限切れでも「送れる」と読めてしまう)
      try {
        const v = assertFromVerified(db, FROM_ADDRESS);
        console.log(`[Yahoo send-plan] From 検証: ✅ ${v.verified_at.slice(0, 10)} に ${v.observed_from} で確認`);
      } catch (e) {
        console.log(`[Yahoo send-plan] From 検証: ❌ ${e.message} → verify-from を実行してください`);
      }
      console.log('[Yahoo send-plan] ※shadow中は ownership=vendor のため not_self_ownership が正常 (送信0が期待値)');
    } finally { db.close(); }
  } else if (mode === 'verify-from') {
    const to = internalToOrExit('verify-from');
    const db = openDb();
    try {
      const sender = createGmailSender({ fromAddress: FROM_ADDRESS });
      const nowIso = new Date().toISOString();
      // 実送信の前に検証を無効化する。読み戻しに失敗して途中で落ちても、
      // 古い成功レコードで LIVE ゲートを通せないようにする (Codex Y-C4 R1 Medium)
      invalidateFromVerification(db, FROM_ADDRESS, nowIso);
      const r = await sender.verifyFrom({
        to,
        subject: `【検証】${SHOP_NAME} Yahoo レビューメールの差出人確認`,
        text: `これは差出人 (From) が ${FROM_ADDRESS} のまま送れているかを確認するためのテストです。\n実行時刻: ${nowIso}\n認証情報: ${sender.credentialSource}*\n`,
        messageId: `<yrc-verify-${Date.now()}@b-faith.biz>`,
      });
      recordFromVerification(db, { fromAddress: FROM_ADDRESS, observedFrom: r.observedFrom, messageId: r.gmailMessageId, nowIso, note: r.ok ? null : 'mismatch' });
      if (!r.ok) {
        console.error(`[verify-from] ❌ From が ${r.observedFrom} に置き換わっています (send-as エイリアス未設定)。LIVE 送信はできません`);
        process.exitCode = 1;
      } else {
        console.log(`[verify-from] ✅ From = ${r.observedFrom} のまま送信されました (指定の社内アドレスに1通届いています)`);
      }
    } finally { db.close(); }
  } else if (mode === 'send-test') {
    const to = internalToOrExit('send-test');
    const tplArg = getArg('--template');
    const templates = tplArg ? [tplArg] : Object.keys(TEMPLATE_BUILDERS);
    const sender = createGmailSender({ fromAddress: FROM_ADDRESS });
    for (const tpl of templates) {
      const builder = TEMPLATE_BUILDERS[tpl];
      if (!builder) { console.error(`FATAL: 未知のテンプレート '${tpl}' (follow / coupon / coupon-low)`); process.exit(2); }
      const mail = builder(sampleContext(tpl));
      const r = await sender.sendMail({
        to,
        from: `"${SHOP_NAME}" <${FROM_ADDRESS}>`,
        subject: `【TEST】${mail.subject}`,
        text: `※これはテスト送信です (ダミーデータ)。\n\n${mail.text}`,
        messageId: messageIdFor(`test-${tpl}`, String(Date.now())),
      });
      console.log(`[send-test] ${tpl} → 送信受理 (gmail id=${r.gmailMessageId})`); // 宛先はログに出さない
    }
  } else if (mode === 'send') {
    const limitRaw = getArg('--limit');
    if (limitRaw != null && (!/^\d+$/.test(limitRaw) || +limitRaw < 1 || +limitRaw > 2000)) {
      console.error('FATAL: --limit は 1〜2000 の整数'); process.exit(2);
    }
    const limit = limitRaw != null ? +limitRaw : 5;
    const notifySummary = args.includes('--notify');
    const db = openDb();
    try {
      // LIVE ゲート: From が info@ のまま出ることを実測済みでなければ 1 通も送らない
      assertFromVerified(db, FROM_ADDRESS);
      const sender = createGmailSender({ fromAddress: FROM_ADDRESS });
      const result = await SE.processReadyActions(db, {
        // Yahoo は PII 非保持 = 復号鍵は使わない。keys には配信停止照合の HMAC 鍵を渡す
        // (無ければここで throw = 1通も送らない)
        keys: loadYahooSuppressKey(),
        limit,
        sendFn: ({ to, from, subject, text, messageId }) => sender.sendMail({ to, from, subject, text, messageId }),
      });
      if (result.staleRecovered > 0) {
        await notifyOperator(`⚠️ Yahooレビューメール: リース切れの claimed 残留 ${result.staleRecovered}件を ambiguous に回収しました。実際の到達を確認するまで送信は開始しません (今回の送信は 0件で中断)`);
        console.error(`[send] ⚠️claimed 残留 ${result.staleRecovered}件を回収。到達確認が済むまで送信しません`);
        process.exitCode = 1;
      } else if (result.inFlight > 0) {
        console.error(`[send] ⚠️リース内の claimed が ${result.inFlight}件あります (別の send が実行中の可能性)。今回は送信しません`);
        process.exitCode = 1;
      } else {
        console.log(`[send] 送信 ${result.sent} / 明確な失敗 ${result.failedSafe} / 結果不明 ${result.ambiguous} / skip ${result.skipped} / ゲート再評価落ち ${result.gateFailed} / claim競り負け ${result.claimLost} / 宛先再試行 ${result.recipientRetry}`);
        if (notifySummary && (result.sent > 0 || result.failedSafe > 0 || result.finalizeConflict > 0)) {
          const byType = { follow: 0, coupon: 0 };
          for (const d of result.details) if (d.result === 'sent') byType[d.type] = (byType[d.type] || 0) + 1;
          await notifyOperator(`📮 Yahooレビューメール (自作) 送信: フォロー ${byType.follow}件 / クーポン ${byType.coupon}件`
            + (result.failedSafe > 0 ? ` / 明確な失敗 ${result.failedSafe}件 (delivery_attempts を確認)` : '')
            + (result.limitHit ? ` / ⚠️上限 ${limit} 件に到達 (残りは翌日の正午)` : ''));
        }
        if (result.releaseConflict > 0) {
          await notifyOperator(`⚠️ Yahooレビューメール: 宛先解決の再試行で claim を解放できない action が ${result.releaseConflict}件 (別プロセスの介入か状態不明)。送信を中断しました`);
          console.error(`[send] ⚠️release_conflict ${result.releaseConflict}件で中断。delivery_attempts を確認してください`);
          process.exitCode = 1;
        }
        if (result.finalizeConflict > 0) {
          console.error(`[send] ⚠️確定競合 ${result.finalizeConflict}件 (送信後に claim を失った=別プロセスの介入痕跡)`);
          process.exitCode = 1;
        }
        if (result.ambiguous > 0) {
          await notifyOperator(`⚠️ Yahooレビューメール送信で結果不明 (ambiguous) ${result.ambiguous}件。自動再送はしません。miniPCで delivery_attempts と実際の到達を確認してください`);
          console.error('[send] ⚠️結果不明が発生したため中断しました。実際の到達を確認するまで再実行しないでください');
          process.exitCode = 1;
        }
      }
    } finally { db.close(); }
  } else if (mode === 'suppress') {
    const email = getArg('--email');
    const reason = getArg('--reason') || 'customer_request';
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) { console.error('FATAL: suppress には --email <アドレス> が必要'); process.exit(2); }
    const db = openDb();
    try {
      const r = addSuppression(db, { email, reason, source: 'manual', key: loadYahooSuppressKey() });
      console.log(`[suppress] 登録しました (HMAC ${r.hash.slice(0, 12)}… / 生アドレスは保存していません)`);
    } finally { db.close(); }
  } else if (mode === 'unsuppress') {
    const email = getArg('--email');
    const by = getArg('--by');
    if (!email || !by) { console.error('FATAL: unsuppress には --email <アドレス> --by <解除した人> が必要'); process.exit(2); }
    const db = openDb();
    try {
      console.log(releaseSuppression(db, { email, by, key: loadYahooSuppressKey() }) ? '[unsuppress] 解除しました' : '[unsuppress] 該当なし (登録されていないか既に解除済み)');
    } finally { db.close(); }
  } else if (mode === 'suppress-import') {
    // vendor (らくらくフォロー) の「除外対象者」CSV 取込。HMAC 化して登録し、原本は消す
    // (要件設計 §PII を含む一時物: ダウンロードフォルダに生アドレスを残さない)
    const file = getArg('--file');
    const live = args.includes('--live');
    if (!file || !fs.existsSync(file)) { console.error('FATAL: suppress-import には --file <CSVパス> が必要'); process.exit(2); }
    // 読んだファイルと消すファイルが同一であることを保証するため、開いたハンドルを持ち続ける
    // (パス文字列で読み直すと、間の差し替え・symlink 付け替えで別のファイルを消し得る — Codex Y-C4 R3 Medium)
    const fd = fs.openSync(file, 'r');
    let emails = [];
    let stBefore;
    try {
      stBefore = fs.fstatSync(fd);
      if (!stBefore.isFile()) { console.error('FATAL: --file は通常ファイルを指してください'); process.exit(2); }
      const buf = fs.readFileSync(fd); // fd から読む (部分読みにならない。パス再解決もしない)
      const { default: iconv } = await import('iconv-lite');
      // cp932 と utf8 の両方を試し、拾えたアドレスが多い方を採る (vendor の CSV は cp932)
      const cands = [iconv.decode(buf, 'cp932'), buf.toString('utf8')].map(extractEmails);
      emails = cands[0].length >= cands[1].length ? cands[0] : cands[1];
      console.log(`[suppress-import] ${path.basename(file)} から ${emails.length} 件のアドレスを検出`);
      // 0 件で原本だけ消すと、除外対象が登録されないまま生アドレスが失われる = 配信停止の抜け道
      // (文字化け・形式変更・ファイル取り違えで普通に起きる — Codex Y-C4 R3 High)
      if (emails.length === 0) {
        console.error('FATAL: アドレスを1件も抽出できませんでした。原本は削除していません (ファイル・文字コードを確認してください)');
        process.exit(2);
      }
      if (!live) {
        console.log('[suppress-import] dry-run のため登録も原本削除もしていません (--live で実行)');
      } else {
        const db = openDb();
        let added = 0, reactivated = 0;
        try {
          const key = loadYahooSuppressKey();
          const tx = db.transaction(() => {
            for (const e of emails) {
              const r = addSuppression(db, { email: e, reason: 'vendor_exclusion_import', source: 'vendor_csv', key });
              if (r.inserted) added++; else if (r.reactivated) reactivated++;
            }
          });
          tx.immediate();
        } finally { db.close(); }
        console.log(`[suppress-import] ✅ 新規 ${added} 件 / 停止に戻した ${reactivated} 件 / 既に停止中 ${emails.length - added - reactivated} 件`);
        // 登録が済んでから原本 (生アドレス) を消す。読んだものと同じファイルか最終確認する
        const stNow = fs.statSync(file);
        if (stNow.ino !== stBefore.ino || stNow.dev !== stBefore.dev || stNow.size !== stBefore.size) {
          console.error(`[suppress-import] ⚠️取り込み後にファイルが差し替わっています。削除しませんでした: ${file}`);
          process.exitCode = 1;
        } else {
          fs.unlinkSync(file);
          console.log(`[suppress-import] 原本を削除しました: ${file}`);
        }
      }
    } finally { fs.closeSync(fd); }
  } else if (mode === 'suppress-stats') {
    const db = openDb();
    try {
      const st = suppressionStats(db);
      console.log(`[suppress-stats] 有効 ${st.active}件 / 解除済み ${st.released}件`);
      for (const b of st.bySource) console.log(`  ${b.source}: ${b.n}件`);
    } finally { db.close(); }
  } else {
    console.error(`FATAL: unknown mode '${mode ?? '(なし)'}' (plan / verify-from / send-test / send / suppress / unsuppress / suppress-import / suppress-stats)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
}
