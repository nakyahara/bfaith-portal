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
  ensureCouponRegistry, getRegisteredCoupon, reserveMonth, markIssued, releaseReservation, notePendingAmbiguous,
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
      console.log(`[coupon] 台帳: ${existing ? `${existing.status} (code=${maskCode(existing.coupon_code)}, reserved_at=${existing.reserved_at})` : '未発行'}`);
      if (!hasFlag('--live')) {
        console.log('[coupon] dry-run (APIは呼んでいない。台帳テーブルの CREATE IF NOT EXISTS のみ実行)。送信予定XML:');
        console.log(built.xml.replace(/^/gm, '  '));
        console.log('[coupon] 実発行は --live を付ける');
      } else if (existing) {
        console.error(existing.status === 'pending'
          ? `FATAL: ${month} は pending (前回の結果不明)。coupon.search で実発行有無を確認し、台帳を手動解決してから (note: ${existing.note || '-'})`
          : `FATAL: ${month} は発行済み。再発行するなら RMS でクーポン削除+台帳行の手動削除を先に (二重発行防止)`);
        process.exitCode = 1;
      } else {
        // 予約 → API (リトライなし) → 確定/解放。結果不明は pending を残して停止 (多重発行防止)
        if (!reserveMonth(db, { month, couponStart: params.couponStartDate, couponEnd: params.couponEndDate })) {
          throw new Error(`${month} は他プロセスが予約済み (台帳を確認)`);
        }
        let r;
        try {
          r = await issueCoupon(built.xml);
        } catch (e) {
          notePendingAmbiguous(db, month, `結果不明 (${String(e.message).slice(0, 120)})。coupon.search で確認要`);
          throw new Error(`発行結果が不明 (${e.message})。台帳は pending のまま。RMS/coupon.search で実発行有無を確認してから手動解決 (自動再発行はしない)`);
        }
        if (r.ok && r.couponCode && r.pcGetUrl) {
          markIssued(db, { month, couponCode: r.couponCode, pcGetUrl: r.pcGetUrl });
          console.log(`[coupon] ✅発行成功: code=${maskCode(r.couponCode)} url=${maskUrl(r.pcGetUrl)} (フル値は台帳に記録済み)`);
        } else if (r.http >= 500 || r.systemStatus == null) {
          // 5xx・解釈不能レスポンス = 発行済みか不明 → pending を残す
          notePendingAmbiguous(db, month, `結果不明 (HTTP ${r.http} ${r.message || ''})。coupon.search で確認要`);
          throw new Error(`発行結果が不明 (HTTP ${r.http})。台帳は pending のまま。確認後に手動解決を`);
        } else {
          // 明確な拒否 (4xx + エラーコード) = 未発行が確定 → 予約解放
          releaseReservation(db, month);
          throw new Error(`発行失敗 (未発行確定・予約解放): HTTP ${r.http} / ${r.systemStatus} ${r.message} / ${r.errors.map((e) => `${e.code}:${e.message}`).join(', ') || '(詳細なし)'}`);
        }
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
    const hitOk = found.ok && found.allCount === 1; // couponCode は requests エコーに常に出るため allCount で判定
    console.log(`[test-cycle] 検索: ${hitOk ? 'OK (allCount=1)' : `NG (allCount=${found.allCount}, ${found.message})`}`);
    // 検索NGでもクリーンアップの削除は必ず試みる
    console.log('[test-cycle] 3/3 削除...');
    const delBuilt = buildDeleteXml(issued.couponCode);
    if (!delBuilt.ok) throw new Error(delBuilt.errors.join(' / '));
    const del = await deleteCoupon(delBuilt.xml);
    const delOk = del.ok;
    if (!delOk) console.error(`[test-cycle] 削除失敗: ${del.errors.map((e) => `${e.code}:${e.message}`).join(', ') || del.message}。RMS画面から手動削除が必要 (code先頭=${maskCode(issued.couponCode)})`);
    const gone = await searchCouponByCode(issued.couponCode);
    const goneOk = gone.ok && gone.allCount === 0;
    console.log(`[test-cycle] 消滅確認: ${goneOk ? '✅ (allCount=0)' : `NG (allCount=${gone.allCount})`}`);
    // 偽陽性防止 (Codex C3-R1 Medium): 全工程OKのときだけ成功終了
    if (!(hitOk && delOk && goneOk)) {
      throw new Error(`フルサイクル不成立 (検索=${hitOk} 削除=${delOk} 消滅=${goneOk})`);
    }
    console.log('[test-cycle] ✅ 発行→検索→削除のフルサイクル成功');
  } else if (mode === 'status') {
    const rows = db.prepare(`SELECT month, status, coupon_code, coupon_start, coupon_end, issued_at, note FROM rakuten_campaign_coupons ORDER BY month`).all();
    if (rows.length === 0) console.log('台帳: 発行記録なし');
    for (const r of rows) {
      console.log(`${r.month}: [${r.status}] code=${maskCode(r.coupon_code)} 期間=${r.coupon_start}〜${r.coupon_end} 発行=${r.issued_at || '-'}${r.note ? ` ⚠️${r.note}` : ''}`);
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
