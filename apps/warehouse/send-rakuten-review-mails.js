#!/usr/bin/env node
/**
 * send-rakuten-review-mails.js — フォロー/クーポンメール送信 CLI (mall-csv-fetcher P2 PR-C4)
 *
 * shadow 中は ownership が全注文 vendor のため send しても構造的に 0 通 (fail-closed)。
 * 実送信が始まるのは cutover (PR-C5) で ownership に self が現れてから。
 * daily-sync には配線しない (cutover まで手動実行のみ。cron 化は C5 で判断)。
 *
 * サブコマンド:
 *   plan                      送信対象の件数・スキップ理由の確認 (DB読み取りのみ、送信なし)
 *   verify-smtp               SMTP 接続+認証の確認 (メールは送らない)
 *   send-test --to <addr> [--template follow|coupon|coupon-low]
 *                             ダミーデータで文面を実送信 (DB 不使用、action を消費しない)。
 *                             文面確認・疎通確認用。既定は全3テンプレを順に送る
 *   send [--limit N]          ready を at-most-once で実送信 (既定 limit=5 の極小運転)。
 *                             結果不明 (ambiguous) が出たら即中断+GChat 通知
 *
 * env: DATA_DIR (plan/send) / RAKUTEN_ANSHIN_SMTP_USER・PASS (verify-smtp/send-test/send) /
 *      CONTACTS_ENC_KEY・CONTACTS_HMAC_KEY (send) / GCHAT_WEBHOOK_MALL_FETCH (ambiguous通知、任意)
 * exit code: 0=成功 / 1=失敗 (ambiguous発生を含む) / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { loadContactKeys, ensureContactTables } from './rakuten-review-contacts-lib.js';
import { ensureCampaignTables } from './rakuten-review-campaign-lib.js';
import { ensureCouponRegistry } from './rakuten-coupon-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';
import { TEMPLATE_BUILDERS, sampleContext, SHOP_NAME, FROM_ADDRESS } from './rakuten-review-mail-lib.js';
import { createAnshinTransport, selectEligibleActions, processReadyActions } from './rakuten-review-sender-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : null;

function openDb() {
  const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
  if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
  const dbPath = path.join(DATA_DIR, 'warehouse.db');
  if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
  ensureRakutenReviewTables(db);
  ensureContactTables(db);
  ensureCampaignTables(db);
  ensureCouponRegistry(db);
  return db;
}

async function notifyAmbiguous(count) {
  const webhook = (process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK || '').trim();
  if (!webhook) return;
  try {
    await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({
        text: `⚠️ 楽天レビューメール送信で結果不明 (ambiguous) ${count}件。自動再送はしません。miniPCで delivery_attempts と実際の到達を確認してください`,
      }),
      signal: AbortSignal.timeout(10000),
    });
  } catch { /* 通知失敗は本処理を止めない */ }
}

try {
  if (mode === 'plan') {
    const db = openDb();
    try {
      const { eligible, skipped, monthlyCouponReady } = selectEligibleActions(db, { limit: 1000 });
      const ready = db.prepare(`SELECT action_type, COUNT(*) n FROM rakuten_campaign_actions WHERE status = 'ready' GROUP BY action_type`).all();
      console.log(`[send-plan] ready 総数: ${ready.map((r) => `${r.action_type}=${r.n}`).join(' / ') || '0'}`);
      console.log(`[send-plan] 今すぐ送信可能 (全ゲート通過): ${eligible.length}件`);
      const reasons = new Map();
      for (const s of skipped) reasons.set(s.reason, (reasons.get(s.reason) || 0) + 1);
      for (const [reason, n] of [...reasons.entries()].sort((a, b) => b[1] - a[1])) {
        console.log(`  skip ${reason}: ${n}件`);
      }
      console.log(`[send-plan] 当月の月次クーポン: ${monthlyCouponReady ? '✅発行済み' : '❌未発行 (coupon は送れない — manage-rakuten-review-coupon.js issue-monthly)'}`);
      console.log('[send-plan] ※shadow中は ownership=vendor のため not_self_ownership が正常 (送信0が期待値)');
    } finally { db.close(); }
  } else if (mode === 'verify-smtp') {
    const transport = await createAnshinTransport();
    await transport.verify();
    console.log(`[verify-smtp] ✅ ${'sub.fw.rakuten.ne.jp'}:587 接続+STARTTLS+認証 OK (メールは送っていない)`);
    transport.close();
  } else if (mode === 'send-test') {
    const to = getArg('--to');
    if (!to || !/^[^\s@]+@[^\s@]+$/.test(to)) { console.error('FATAL: send-test には --to <メールアドレス> が必要'); process.exit(2); }
    const tplArg = getArg('--template');
    const templates = tplArg ? [tplArg] : Object.keys(TEMPLATE_BUILDERS);
    const transport = await createAnshinTransport();
    try {
      for (const tpl of templates) {
        const builder = TEMPLATE_BUILDERS[tpl];
        if (!builder) { console.error(`FATAL: 未知のテンプレート '${tpl}' (follow / coupon / coupon-low)`); process.exit(2); }
        const mail = builder(sampleContext(tpl));
        const info = await transport.sendMail({
          from: `"${SHOP_NAME}" <${FROM_ADDRESS}>`,
          to,
          subject: `【TEST】${mail.subject}`,
          text: `※これはテスト送信です (ダミーデータ)。\n\n${mail.text}`,
        });
        console.log(`[send-test] ${tpl} → 送信受理 (${info.response?.slice(0, 60) || 'OK'})`);
      }
    } finally { transport.close(); }
  } else if (mode === 'send') {
    const limitRaw = getArg('--limit');
    if (limitRaw != null && (!/^\d+$/.test(limitRaw) || +limitRaw < 1 || +limitRaw > 200)) {
      console.error('FATAL: --limit は 1〜200 の整数'); process.exit(2);
    }
    const limit = limitRaw != null ? +limitRaw : 5;
    const db = openDb();
    const keys = loadContactKeys();
    const transport = await createAnshinTransport();
    try {
      const result = await processReadyActions(db, {
        keys,
        limit,
        sendFn: ({ to, from, subject, text, messageId }) =>
          transport.sendMail({ from, to, subject, text, messageId }),
      });
      console.log(`[send] 送信 ${result.sent} / 明確な失敗 ${result.failedSafe} / 結果不明 ${result.ambiguous} / skip ${result.skipped} / claim競り負け ${result.claimLost}`);
      if (result.ambiguous > 0) {
        await notifyAmbiguous(result.ambiguous);
        console.error('[send] ⚠️結果不明が発生したため中断しました。実際の到達を確認するまで再実行しないでください');
        process.exitCode = 1;
      }
    } finally {
      transport.close();
      db.close();
    }
  } else {
    console.error(`FATAL: unknown mode '${mode ?? '(なし)'}' (plan / verify-smtp / send-test / send)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
}
