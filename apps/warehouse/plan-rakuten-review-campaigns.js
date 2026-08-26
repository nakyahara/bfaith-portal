#!/usr/bin/env node
/**
 * plan-rakuten-review-campaigns.js — フォロー/クーポン planner CLI (mall-csv-fetcher P2 PR-C1)
 *
 * shadow 運用: 「らくらくーぽんなら何をいつ送るか」を rakuten_campaign_actions に記録するだけ。
 * **送信は一切しない** (SMTP・クーポン発行APIのコードはこのCLIに存在しない)。
 * らくらくーぽん実績との突合 (PR-C2) の材料を毎日積む。
 *
 * サブコマンド:
 *   plan     計画の作成+状態遷移 (既定。daily-sync から毎朝実行)
 *   stats    状態サマリ (PIIなし)
 *   vendor   らくらくーぽん実績の日次件数を転記 (PR-C2 突合の材料)
 *            例: vendor --date 2026-07-18 --follow 512 --coupon 14
 *   report   shadow 突合レポート (--days 14 既定 / --from --to 指定可)
 *   cutover  shadow → LIVE の切替 (PR-C5、一度だけ)。既定は dry-run (影響件数の表示のみ)
 *            段階1 (クーポン先行): cutover --coupon-only --coupon-at 2026-07-25T00:00:00+09:00 [--live]
 *              フォローは vendor のまま、クーポンメールだけ自作が送り始める (vendor のクーポン経路が故障中のため)
 *            段階2 (フォロー切替、解約後): cutover --follow-at 2026-09-02T23:59:59+09:00 [--live]
 *              段階1済みなら --coupon-at は省略 (確定済みの値を引き継ぐ)
 *            例: cutover --follow-at 2026-09-02T23:59:59+09:00 --coupon-at 2026-07-25T00:00:00+09:00 [--live]
 *              --follow-at : この時刻より後に最終発送された注文のフォローメールを自作が送る
 *                            (らくらくーぽん最終稼働日の10日前 23:59:59 = 空白も二重送信も出ない境目)
 *              --coupon-at : 同じくクーポンメールの境目 (省略時 = --follow-at)。らくらくーぽんの
 *                            クーポンメールが止まった日以降を早く引き取るために別指定できる
 *            ⭐境目を過ぎてから実行する (未来の境目は拒否。ownership は一度決めたら覆らないため)。
 *            live 後は毎日 12:05 の RakutenReviewMailSend (scripts/mall-csv-fetcher/run-rakuten-review-send.ps1)
 *            が実送信する。cutover を戻す手段は CLI に無い (rakuten_campaign_meta を手で直す)
 *
 * env: DATA_DIR (必須)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import {
  ensureCampaignTables, planCampaigns, campaignStats,
  recordVendorDailyBatch, shadowComparisonReport, jstDateOf, REPORT_MAX_DAYS,
  getCutover, parseCutoverArg, cutoverPreview, applyCutover, applyCouponCutover,
} from './rakuten-review-campaign-lib.js';
import { ensureContactTables } from './rakuten-review-contacts-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : 'plan';

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
// attempts/grants の REFERENCES を実際に強制する (SQLite は接続ごとに有効化が必要 — Codex C1-R2)。
// PR-C4 の sender 接続でも必ず有効化すること
db.pragma('foreign_keys = ON');
// planner は contacts / reviews を読むため、初回実行順に依らず全テーブルを冪等に確保する
ensureRakutenReviewTables(db);
ensureContactTables(db);
ensureCampaignTables(db);

try {
  if (mode === 'plan') {
    const c = planCampaigns(db);
    const phase = { live: 'LIVE', coupon_only: 'クーポン先行LIVE/フォローshadow', shadow: 'shadow' }[getCutover(db).stage];
    console.log(`[campaign] plan完了 (${phase}): フォロー新規 ${c.followInserted} / クーポン新規 ${c.couponInserted} / ` +
      `ready昇格 ${c.promotedReady} / 抑止 ${c.suppressed} / 期限切れ ${c.expired} / 取消 ${c.cancelled} / ` +
      `再スケジュール ${c.rescheduled} / ownership追加 ${c.ownershipInserted}`);
  } else if (mode === 'stats') {
    const s = campaignStats(db);
    const cut = getCutover(db);
    console.log(`coupon_epoch: ${s.epoch || '(未設定=plan未実行)'}`);
    console.log(`cutover: ${cut.stage === 'live' ? `LIVE (follow=${cut.cutoverAt} / coupon=${cut.couponCutoverAt})`
      : cut.stage === 'coupon_only' ? `段階1=クーポン先行 (coupon=${cut.couponCutoverAt} / フォローは shadow)` : 'shadow (未実施)'}`);
    console.log(`予定超過で待機中 (planned): ${s.dueOverdue}件 / 今後24時間の送信予定 (planned): ${s.dueNext24h}件 / 本日(JST) ready昇格: ${s.readyTodayJst}件`);
    for (const row of s.byStatus) {
      console.log(`  ${row.action_type} ${row.status}${row.reason ? ` (${row.reason})` : ''}: ${row.n}件`);
    }
    if (s.byStatus.length === 0) console.log('  (action なし)');
  } else if (mode === 'vendor') {
    const date = getArg('--date');
    const follow = getArg('--follow');
    const coupon = getArg('--coupon');
    if (!date || (follow == null && coupon == null)) {
      console.error('FATAL: vendor --date YYYY-MM-DD と --follow N / --coupon N の少なくとも一方が必要');
      process.exitCode = 2;
    } else {
      // 2項目は単一トランザクションで転記 (片方だけ保存される部分更新を防ぐ — Codex C2-R1 High)
      const entries = [];
      if (follow != null) entries.push({ dateJst: date, actionType: 'follow', count: Number(follow) });
      if (coupon != null) entries.push({ dateJst: date, actionType: 'coupon', count: Number(coupon) });
      recordVendorDailyBatch(db, entries);
      console.log(`[campaign] vendor実績を記録: ${date} follow=${follow ?? '(変更なし)'} coupon=${coupon ?? '(変更なし)'}`);
    }
  } else if (mode === 'report') {
    const todayJst = jstDateOf(new Date().toISOString());
    let from = getArg('--from'), to = getArg('--to');
    if ((from == null) !== (to == null)) {
      // 片方だけの指定を黙って捨てない (Codex C2-R1 Medium)
      console.error('FATAL: --from と --to は両方指定してください (片方だけは不可)');
      process.exitCode = 2;
      throw Object.assign(new Error('usage'), { silent: true });
    }
    if (!from || !to) {
      const daysRaw = getArg('--days');
      if (daysRaw != null && (!/^\d+$/.test(daysRaw) || +daysRaw < 1 || +daysRaw > REPORT_MAX_DAYS)) {
        console.error(`FATAL: --days は 1〜${REPORT_MAX_DAYS} の整数で指定してください (指定値: ${daysRaw})`);
        process.exitCode = 2;
        throw Object.assign(new Error('usage'), { silent: true });
      }
      const days = daysRaw != null ? +daysRaw : 14;
      to = todayJst;
      from = jstDateOf(new Date(Date.parse(`${todayJst}T00:00:00+09:00`) - (days - 1) * 86400000).toISOString());
    }
    const rep = shadowComparisonReport(db, { fromJst: from, toJst: to });
    console.log(`=== shadow突合レポート ${rep.fromJst} 〜 ${rep.toJst} (JST) ===`);
    console.log('日付        | follow vendor/shadow/差 | coupon vendor/shadow/差');
    if (rep.days.length === 0) console.log('  (期間内のデータなし)');
    for (const d of rep.days) {
      const f = `${d.follow_vendor ?? '-'} / ${d.follow_shadow} / ${d.follow_diff ?? '-'}`;
      const c = `${d.coupon_vendor ?? '-'} / ${d.coupon_shadow} / ${d.coupon_diff ?? '-'}`;
      console.log(`${d.date_jst}  | ${f.padEnd(23)} | ${c}`);
    }
    console.log('--- would-send にならなかった予定の内訳 (期間内 scheduled) ---');
    if (rep.reasons.length === 0) console.log('  (なし)');
    for (const r of rep.reasons) {
      console.log(`  ${r.action_type} ${r.status}${r.reason ? ` (${r.reason})` : ''}: ${r.n}件`);
    }
    console.log('※ vendor件数は「vendor --date ... --follow N --coupon N」で転記 (らくらくーぽん メール配信履歴画面)');
  } else if (mode === 'cutover') {
    const couponOnly = args.includes('--coupon-only');
    const live = args.includes('--live');
    const cur = getCutover(db);
    const fmt = (iso) => `${iso} (JST ${new Date(Date.parse(iso) + 9 * 3600000).toISOString().slice(0, 19).replace('T', ' ')})`;
    const usage = (msg) => { console.error(`FATAL: ${msg}`); process.exitCode = 2; throw Object.assign(new Error('usage'), { silent: true }); };
    const stageLabel = cur.stage === 'live' ? `LIVE 済み (follow=${cur.cutoverAt})`
      : cur.stage === 'coupon_only' ? `段階1 (クーポン先行、coupon=${cur.couponCutoverAt})` : 'shadow';
    let cutoverAt, couponCutoverAt;
    if (couponOnly) {
      if (getArg('--follow-at')) usage('--coupon-only と --follow-at は同時に指定できない');
      if (!getArg('--coupon-at')) usage('cutover --coupon-only には --coupon-at <ISO日時+オフセット> が必要 (例: 2026-07-25T00:00:00+09:00)');
      couponCutoverAt = parseCutoverArg(getArg('--coupon-at'));
      cutoverAt = null;
    } else {
      if (!getArg('--follow-at')) usage('cutover には --follow-at <ISO日時+オフセット> が必要 (例: 2026-09-02T23:59:59+09:00)。クーポンだけ先行するなら --coupon-only --coupon-at');
      cutoverAt = parseCutoverArg(getArg('--follow-at'));
      couponCutoverAt = getArg('--coupon-at') ? parseCutoverArg(getArg('--coupon-at')) : (cur.couponCutoverAt || cutoverAt);
    }
    console.log(`[cutover] ${live ? '🔴 LIVE' : 'dry-run'} ${couponOnly ? '(段階1: クーポンのみ)' : '(フォロー+クーポン)'} — 現在: ${stageLabel}`);
    console.log(`[cutover] フォロー境目: ${cutoverAt ? `${fmt(cutoverAt)} より後の最終発送 → self` : '(変更なし = vendor のまま)'}`);
    console.log(`[cutover] クーポン境目: ${fmt(couponCutoverAt)} より後の最終発送 → self`);
    const pv = cutoverPreview(db, { cutoverAt: cutoverAt || '9999-12-31T00:00:00.000Z', couponCutoverAt });
    console.log(`[cutover] 影響: 再判定対象の ownership 行 ${pv.shadowRows} 件 / 発送済み contacts ${pv.shipped} 件のうち ` +
      `フォロー self ${pv.followSelf} / クーポン self ${pv.couponSelf} / 両方 vendor ${pv.vendorBoth}`);
    console.log(`[cutover] 切替後に自作の送信対象になる action (期限内): フォロー ready ${pv.followReady} + planned ${pv.followPlanned} / ` +
      `クーポン ready ${pv.couponReady} + planned ${pv.couponPlanned}`);
    if (!live) {
      console.log('[cutover] dry-run のため何も変更していない。実行は --live を付ける (一度だけ・戻せない)');
    } else {
      const r = couponOnly ? applyCouponCutover(db, { couponCutoverAt }) : applyCutover(db, { cutoverAt, couponCutoverAt });
      console.log(`[cutover] ✅ 適用: shadow 行削除 ${r.shadowDeleted} / 段階1行の引き継ぎ ${r.stage1Migrated ?? 0} / ownership 再判定 ${r.ownershipInserted} / ready昇格 ${r.promotedReady}`);
      console.log('[cutover] 次の 12:05 (RakutenReviewMailSend) から自作が送信する。当月の月次クーポンが未発行なら manage-rakuten-review-coupon.js ensure-monthly --live を先に');
    }
  } else {
    console.error(`FATAL: unknown mode '${mode}' (plan / stats / vendor / report / cutover)`);
    process.exitCode = 2;
  }
} catch (e) {
  if (!e.silent) { // silent=usage エラー (メッセージ・exitCode=2 は throw 前に設定済み)
    console.error(`FATAL: ${e.message}`);
    process.exitCode = 1;
  }
} finally {
  db.close();
}
