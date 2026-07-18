#!/usr/bin/env node
/**
 * manage-rakuten-review-coupon.js — 月次クーポン発行 CLI (mall-csv-fetcher P2 PR-C3)
 *
 * らくらくーぽん置換の「次回購入に使える5%割引クーポン」(月次・非公開) を RMS クーポンAPIで
 * 発行・管理する。**メール送信はしない** (獲得URLの配布=メールは PR-C4)。
 * shadow 期間中は test-cycle での動作確認のみが想定用途。issue-monthly --live は
 * cutover 準備 (vendor のクーポンと自作のクーポンは別物として併存できる — 非公開のため
 * URL を配らない限り顧客には見えない)。
 *
 * サブコマンド:
 *   issue-monthly --month YYYY-MM [--live]   月次クーポンの発行。既定は dry-run
 *                                            (検証済みXMLと台帳状態を表示するだけ。API を呼ばない)
 *   test-cycle                               テストクーポンで発行→検索確認→削除の疎通確認
 *                                            (開始3日後・issueCount 1・非公開。実発行するが即削除)
 *   status                                   台帳 (rakuten_campaign_coupons) の一覧
 *
 * env: DATA_DIR (必須) / RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY (API呼び出し時)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import {
  MONTHLY_COUPON_DEFAULTS, buildIssueXml, buildDeleteXml, monthlyCouponParams,
  ensureCouponRegistry, getRegisteredCoupon, recordIssuedCoupon,
  issueCoupon, deleteCoupon, searchCouponByCode, maskCode, maskUrl,
} from './rakuten-coupon-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const hasFlag = (f) => args.includes(f);
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : null;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
db.pragma('foreign_keys = ON');
ensureCouponRegistry(db);

try {
  if (mode === 'issue-monthly') {
    const month = getArg('--month');
    if (!month) { console.error('FATAL: --month YYYY-MM が必要'); process.exitCode = 2; }
    else {
      const params = monthlyCouponParams(month); // 形式・期間の検証込み (不正は throw)
      const built = buildIssueXml(params);
      if (!built.ok) throw new Error(`パラメータ検証NG: ${built.errors.join(' / ')}`);
      const existing = getRegisteredCoupon(db, month);
      console.log(`[coupon] 対象月 ${month}: 開始 ${params.couponStartDate} / 終了 ${params.couponEndDate}`);
      console.log(`[coupon] 台帳: ${existing ? `発行済み (code=${maskCode(existing.coupon_code)}, issued_at=${existing.issued_at})` : '未発行'}`);
      if (!hasFlag('--live')) {
        console.log('[coupon] dry-run (APIは呼んでいない)。送信予定XML:');
        console.log(built.xml.replace(/^/gm, '  '));
        console.log('[coupon] 実発行は --live を付ける');
      } else if (existing) {
        console.error(`FATAL: ${month} は台帳に発行済み。再発行するなら RMS でクーポン削除+台帳行の手動削除を先に (二重発行防止)`);
        process.exitCode = 1;
      } else {
        const r = await issueCoupon(built.xml);
        if (!r.ok || !r.couponCode || !r.pcGetUrl) {
          throw new Error(`発行失敗: HTTP ${r.http} / ${r.systemStatus} ${r.message} / ${r.errors.map((e) => `${e.code}:${e.message}`).join(', ') || '(詳細なし)'}`);
        }
        recordIssuedCoupon(db, {
          month, couponCode: r.couponCode, pcGetUrl: r.pcGetUrl,
          couponStart: params.couponStartDate, couponEnd: params.couponEndDate,
        });
        console.log(`[coupon] ✅発行成功: code=${maskCode(r.couponCode)} url=${maskUrl(r.pcGetUrl)} (フル値は台帳に記録済み)`);
      }
    }
  } else if (mode === 'test-cycle') {
    // 開始3日後 (編集不可制約=開始60分前を回避)・issueCount 1・非公開。発行→検索→削除
    const jstNow = new Date(Date.now() + 9 * 3600 * 1000);
    const day = (add) => new Date(jstNow.getTime() + add * 86400000).toISOString().slice(0, 10);
    const params = {
      ...MONTHLY_COUPON_DEFAULTS,
      couponName: '【動作検証用】テストクーポン',
      couponCaption: 'システム動作検証用。獲得URLは配布していません。',
      couponStartDate: `${day(3)}T00:00:00+09:00`,
      couponEndDate: `${day(4)}T23:59:59+09:00`,
      issueCount: 1,
    };
    const built = buildIssueXml(params);
    if (!built.ok) throw new Error(`パラメータ検証NG: ${built.errors.join(' / ')}`);
    console.log('[test-cycle] 1/3 発行...');
    const issued = await issueCoupon(built.xml);
    if (!issued.ok || !issued.couponCode) {
      throw new Error(`発行失敗: HTTP ${issued.http} / ${issued.errors.map((e) => `${e.code}:${e.message}`).join(', ') || issued.message}`);
    }
    console.log(`[test-cycle] 発行OK: ${maskCode(issued.couponCode)}`);
    console.log('[test-cycle] 2/3 検索確認...');
    const found = await searchCouponByCode(issued.couponCode);
    const hit = found.allCount === 1; // couponCode は requests エコーに常に出るため allCount で判定
    console.log(`[test-cycle] 検索: ${hit ? 'OK (allCount=1)' : `NG (allCount=${found.allCount}, ${found.message})`}`);
    console.log('[test-cycle] 3/3 削除...');
    const delBuilt = buildDeleteXml(issued.couponCode);
    if (!delBuilt.ok) throw new Error(delBuilt.errors.join(' / '));
    const del = await deleteCoupon(delBuilt.xml);
    if (!del.ok) throw new Error(`削除失敗: ${del.errors.map((e) => `${e.code}:${e.message}`).join(', ') || del.message}。RMS画面から手動削除が必要 (code先頭=${maskCode(issued.couponCode)})`);
    const gone = await searchCouponByCode(issued.couponCode);
    const cleaned = gone.allCount === 0;
    console.log(`[test-cycle] 削除OK・消滅確認: ${cleaned ? '✅ (allCount=0)' : `⚠️検索にまだ出る (allCount=${gone.allCount})`}`);
    console.log('[test-cycle] ✅ 発行→検索→削除のフルサイクル成功');
  } else if (mode === 'status') {
    const rows = db.prepare(`SELECT month, coupon_code, coupon_start, coupon_end, issued_at FROM rakuten_campaign_coupons ORDER BY month`).all();
    if (rows.length === 0) console.log('台帳: 発行記録なし');
    for (const r of rows) {
      console.log(`${r.month}: code=${maskCode(r.coupon_code)} 期間=${r.coupon_start}〜${r.coupon_end} 発行=${r.issued_at}`);
    }
  } else {
    console.error(`FATAL: unknown mode '${mode ?? '(なし)'}' (issue-monthly / test-cycle / status)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
} finally {
  db.close();
}
