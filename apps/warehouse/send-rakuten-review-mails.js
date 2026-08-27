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

/** ambiguous 系の人手確認依頼。通知は安全機構の一部 (Codex C4-R1 Medium) — 失敗を握り潰さず
 *  stderr に明示する (本処理は既に exitCode 1 の経路でのみ呼ばれる)。@returns 通知成功か */
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
    if (!res.ok) {
      console.error(`[notify] ⚠️GChat 通知失敗 (HTTP ${res.status})。このログが唯一の通知です`);
      return false;
    }
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
      const { eligible, skipped, monthlyCouponReady } = selectEligibleActions(db, { limit: 1000 });
      const ready = db.prepare(`SELECT action_type, COUNT(*) n FROM rakuten_campaign_actions WHERE status = 'ready' GROUP BY action_type`).all();
      const claimed = db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_actions WHERE status = 'claimed'`).get().n;
      console.log(`[send-plan] ready 総数: ${ready.map((r) => `${r.action_type}=${r.n}`).join(' / ') || '0'}`);
      if (claimed > 0) console.log(`[send-plan] ⚠️claimed 残留 ${claimed}件 (前回クラッシュの疑い。次回 send 冒頭で ambiguous に回収されます)`);
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
    if (limitRaw != null && (!/^\d+$/.test(limitRaw) || +limitRaw < 1 || +limitRaw > 2000)) {
      console.error('FATAL: --limit は 1〜2000 の整数'); process.exit(2);
    }
    // 上限 2000 (PR-C5: 正午の定時運転はフォロー ~250-900件/日)。既定 5 は手動の極小運転用
    const limit = limitRaw != null ? +limitRaw : 5;
    const notifySummary = args.includes('--notify');
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
      if (result.staleRecovered > 0) {
        await notifyOperator(`⚠️ 楽天レビューメール: リース切れの claimed 残留 ${result.staleRecovered}件を ambiguous に回収しました。実際の到達を確認するまで送信は開始しません (今回の送信は 0件で中断)`);
        console.error(`[send] ⚠️claimed 残留 ${result.staleRecovered}件を回収。到達確認が済むまで送信しません`);
        process.exitCode = 1;
      } else if (result.inFlight > 0) {
        console.error(`[send] ⚠️リース内の claimed が ${result.inFlight}件あります (別の send が実行中の可能性)。今回は送信しません`);
        process.exitCode = 1;
      } else {
        console.log(`[send] 送信 ${result.sent} / 明確な失敗 ${result.failedSafe} / 結果不明 ${result.ambiguous} / skip ${result.skipped} / ゲート再評価落ち ${result.gateFailed} / claim競り負け ${result.claimLost}`);
        // 定時運転の日次サマリ (PR-C5、--notify)。何か送った/失敗した日だけ通知 (0件の日は静か)
        if (notifySummary && (result.sent > 0 || result.failedSafe > 0 || result.finalizeConflict > 0)) {
          const byType = { follow: 0, coupon: 0 };
          for (const d of result.details) if (d.result === 'sent') byType[d.type] = (byType[d.type] || 0) + 1;
          await notifyOperator(`📮 楽天レビューメール (自作) 送信: フォロー ${byType.follow}件 / クーポン ${byType.coupon}件`
            + (result.failedSafe > 0 ? ` / 明確な失敗 ${result.failedSafe}件 (delivery_attempts を確認)` : '')
            + (result.limitHit ? ` / ⚠️上限 ${limit} 件に到達 (残りは翌日の正午)` : ''));
        }
        if (result.releaseConflict > 0) {
          await notifyOperator(`⚠️ 楽天レビューメール: 宛先解決の再試行で claim を解放できない action が ${result.releaseConflict}件 (別プロセスの介入か状態不明)。送信を中断しました。delivery_attempts を確認してください`);
          console.error(`[send] ⚠️release_conflict ${result.releaseConflict}件で中断。delivery_attempts を確認してください`);
          process.exitCode = 1;
        }
        if (result.finalizeConflict > 0) {
          console.error(`[send] ⚠️確定競合 ${result.finalizeConflict}件 (送信後に claim を失った=別プロセスの介入痕跡)。delivery_attempts を確認してください`);
          process.exitCode = 1;
        }
        if (result.ambiguous > 0) {
          await notifyOperator(`⚠️ 楽天レビューメール送信で結果不明 (ambiguous) ${result.ambiguous}件。自動再送はしません。miniPCで delivery_attempts と実際の到達を確認してください`);
          console.error('[send] ⚠️結果不明が発生したため中断しました。実際の到達を確認するまで再実行しないでください');
          process.exitCode = 1;
        }
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
