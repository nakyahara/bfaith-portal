#!/usr/bin/env node
/**
 * manage-rakuten-store-coupon.js — 楽天ストアクーポン 定期入れ替え CLI
 *
 * 何をするか:
 *   雑貨イズム楽天市場店の「N点以上ご購入でX円引きクーポン」4本 (3/4/5/6点) を、
 *   期限が近づいたら次の期間ぶんとして自動で作り直す。金額は毎回ずらす (±10円の2値往復)。
 *   毎日実行して、作るのは「次期間の開始まで残り leadDays 日」になった1回だけ。
 *
 * なぜ API で完結できるか (Yahoo!版との違い):
 *   楽天には公式のクーポン発行API (es/1.0/coupon/{issue,search,delete}) があるため、
 *   画面自動操作が要らない (= RMS の自動化検知とも無関係)。発行後も開始60分前までなら削除できる。
 *
 * サブコマンド:
 *   list                現況のストアクーポンを一覧表示 (API GET のみ)
 *   rotate              入れ替え計画を表示 (既定は dry-run = 発行しない)
 *   rotate --live       実際に発行する
 *   test-cycle          本番と同じ形のテストクーポンを1本だけ**非公開**で発行し、
 *                       設定の読み戻しを確認して削除する (XML要素順・期間上限の実測用)
 *
 * env:
 *   RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY  (必須)
 *   GCHAT_WEBHOOK          通知先 (未設定なら通知しない)
 *   RCOUPON_LEAD_DAYS      次期間の開始まで残り何日で作るか (既定 7)
 *   RCOUPON_MAX_SPAN_DAYS  掲載期間の上限日数 (既定 91 = 月初〜3ヶ月後の月末)
 *   RCOUPON_ONLY           対象を絞る (例 qty3,qty4)
 *
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import {
  buildIssueXml, buildDeleteXml, issueCoupon, deleteCoupon, searchAllCoupons, searchCouponByCode, maskCode,
} from './rakuten-coupon-lib.js';
import {
  STORE_COUPON_DEFS, STORE_COUPON_FIXED, planStoreCoupon, planToIssueParams, todayJst, isoToYmd,
  findExistingForStart, validatePeriod,
} from './rakuten-store-coupon-lib.js';

const args = process.argv.slice(2);
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : null;
const LIVE = args.includes('--live');
const LEAD_DAYS = Math.max(0, Math.min(30, parseInt(process.env.RCOUPON_LEAD_DAYS, 10) || 7));
const MAX_SPAN_DAYS = Math.max(1, Math.min(92, parseInt(process.env.RCOUPON_MAX_SPAN_DAYS, 10) || 91));
const ONLY = (process.env.RCOUPON_ONLY || '').split(',').map((s) => s.trim()).filter(Boolean);
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

if (!process.env.RAKUTEN_SERVICE_SECRET || !process.env.RAKUTEN_LICENSE_KEY) {
  console.error('FATAL: RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY が未設定');
  process.exit(2);
}

/** GChat 通知 (best effort) */
async function sendGChat(text) {
  if (!GCHAT_WEBHOOK) { console.warn('[GChat] GCHAT_WEBHOOK 未設定のため通知スキップ'); return false; }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }),
    });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

/** 現況のストアクーポン (定額・受注・利用個数条件つき) を新しい順に */
function storeCouponsOf(coupons) {
  return coupons
    .filter((c) => c.discountType === 1 && c.itemType === 4 && c.orderCountCond != null)
    .sort((a, b) => Date.parse(b.couponStartDate || 0) - Date.parse(a.couponStartDate || 0));
}

async function cmdList() {
  const { allCount, coupons } = await searchAllCoupons();
  const store = storeCouponsOf(coupons);
  console.log(`=== 楽天クーポン 全 ${allCount} 件 / ストアクーポン (N点以上・定額) ${store.length} 件 ===`);
  for (const c of store.slice(0, 20)) {
    console.log(`  ${isoToYmd(c.couponStartDate)}〜${isoToYmd(c.couponEndDate)}  ${c.orderCountCond}点以上 ${String(c.discountFactor).padStart(4)}円  `
      + `公開=${c.displayFlag} 取得=${c.getCount} ${maskCode(c.couponCode)}  「${c.couponName}」`);
  }
  if (store.length > 20) console.log(`  … 他 ${store.length - 20} 件`);
}

/**
 * 発行 1本。戻り値は結果オブジェクト (throw しない)。
 *
 * 発行の直前に現況を取り直して重複と時刻制約を見直す。
 *   - 計画時の検索から時間が経っており、その間に人が画面から作っているかもしれない
 *     (このCLIを二重起動した場合も同じ) → 同じ開始日のものがあれば発行しない
 *   - 遅延リカバリでは開始を「今+90分」に置くため、待たされると楽天の60分制約を割り得る
 */
async function issueOne(plan) {
  let fresh;
  try {
    fresh = await searchAllCoupons();
  } catch (e) {
    return { ...plan, status: 'failed', error: `発行直前の現況確認に失敗したため発行を見送り (${e.message})` };
  }
  const dup = findExistingForStart(fresh.coupons, plan.orderCount, plan.period.startYmd, plan.couponName);
  if (dup) {
    return { ...plan, status: 'skip', reason: `発行直前の確認で同じ開始日のクーポンを検出 (${dup.discountFactor}円) — 二重発行を回避` };
  }
  const errs = validatePeriod({ startIso: plan.period.startIso, endIso: plan.period.endIso, maxSpanDays: MAX_SPAN_DAYS });
  if (errs.length > 0) {
    return { ...plan, status: 'failed', error: `発行直前の再検証NG: ${errs.join(' / ')}` };
  }
  const params = planToIssueParams(plan);
  const built = buildIssueXml(params);
  if (!built.ok) return { ...plan, status: 'failed', error: `パラメータ検証NG: ${built.errors.join(' / ')}` };
  let r;
  try {
    r = await issueCoupon(built.xml); // maxAttempts=1 (非冪等POSTは再送しない)
  } catch (e) {
    return { ...plan, status: 'unknown', error: `発行結果が不明 (${e.message})。coupon.search で実発行の有無を確認すること` };
  }
  if (r.ok && r.couponCode) {
    return { ...plan, status: 'issued', couponCode: r.couponCode };
  }
  // 明確な業務エラー (4xx + NG/errors) = 未発行確定。それ以外は結果不明として人の確認に回す
  const clearFailure = !r.ok && r.http >= 400 && r.http < 500 && (r.systemStatus === 'NG' || r.errors.length > 0);
  const detail = `HTTP ${r.http} / ${r.systemStatus || '-'} ${r.message || ''} / ${r.errors.map((e) => `${e.code}:${e.message}`).join(', ') || '(詳細なし)'}`;
  return clearFailure
    ? { ...plan, status: 'failed', error: `発行失敗 (未発行確定): ${detail}` }
    : { ...plan, status: 'unknown', error: `発行結果が不明: ${detail}。coupon.search で確認すること` };
}

async function cmdRotate() {
  console.log(`=== 楽天ストアクーポン 定期入れ替え (${LIVE ? '★LIVE=実発行' : 'dry-run'}) ===`);
  console.log(`作成タイミング=開始の${LEAD_DAYS}日前 / 期間上限=${MAX_SPAN_DAYS}日`);
  const { coupons } = await searchAllCoupons();
  const todayYmd = todayJst();
  const defs = ONLY.length > 0 ? STORE_COUPON_DEFS.filter((d) => ONLY.includes(d.key)) : STORE_COUPON_DEFS;
  const plans = defs.map((def) => planStoreCoupon({
    def, coupons, todayYmd, leadDays: LEAD_DAYS, maxSpanDays: MAX_SPAN_DAYS,
  }));

  console.log('\n--- 計画 ---');
  for (const p of plans) {
    if (p.status === 'ready') {
      console.log(`  [${p.key}] 現行 ${p.prevAmount}円 (〜${isoToYmd(p.source.couponEndDate)})`);
      console.log(`         → 「${p.couponName}」 ${p.amount}円 / ${p.period.startIso} 〜 ${p.period.endIso} (${p.period.spanDays}日)`);
      if (p.amountReset) console.log(`         ⚠️ 現行の${p.prevAmount}円は巡回リスト外のため ${p.amount}円 にリセット`);
      if (p.period.delayed) console.log(`         ⚠️ 前回の期限を過ぎてからの作成 (空白期間あり)`);
      if (p.period.truncated) console.log(`         ⚠️ 期間上限${MAX_SPAN_DAYS}日で終了日を切り詰め`);
    } else {
      console.log(`  [${p.key}] ${p.status}: ${p.reason}`);
    }
  }

  const ready = plans.filter((p) => p.status === 'ready');
  if (!LIVE) {
    if (ready.length > 0) {
      console.log('\n--- 送信予定XML (先頭1本) ---');
      const built = buildIssueXml(planToIssueParams(ready[0]));
      console.log(built.ok ? built.xml.replace(/^/gm, '  ') : `検証NG: ${built.errors.join(' / ')}`);
    }
    console.log(`\ndry-run: 発行していません (${ready.length}本が発行対象)。実行するには --live を付ける`);
    return plans.some((p) => p.status === 'error') ? 1 : 0;
  }

  const results = [];
  for (const plan of ready) {
    console.log(`\n### ${plan.key}: ${plan.couponName}`);
    const r = await issueOne(plan);
    results.push(r);
    if (r.status === 'issued') console.log(`  ✅ 発行 (${maskCode(r.couponCode)})`);
    else console.error(`  ❌ ${r.status}: ${r.error}`);
    // 結果不明が出たら以降は止める (状況が読めないまま発行を重ねない)
    if (r.status === 'unknown') {
      console.error('  ⛔ 結果不明のため残りの発行を中止します');
      break;
    }
  }
  for (const p of plans) if (p.status !== 'ready') results.push(p);

  // 発行後の実在確認。ここで失敗しても「発行できたこと」は通知したいので握り潰す
  if (results.some((r) => r.status === 'issued')) {
    let after = null;
    try {
      after = await searchAllCoupons();
    } catch (e) {
      console.error(`  ⚠️ 発行後の確認に失敗 (発行そのものは成功している可能性が高い): ${e.message}`);
    }
    for (const r of after ? results.filter((x) => x.status === 'issued') : []) {
      const hit = after.coupons.find((c) => c.couponCode === r.couponCode);
      r.verified = !!hit;
      r.verifiedDetail = hit
        ? `${hit.orderCountCond}点以上 ${hit.discountFactor}円 ${isoToYmd(hit.couponStartDate)}〜${isoToYmd(hit.couponEndDate)} 公開=${hit.displayFlag}`
        : null;
      // 意図した設定で登録されたか (点数・金額・期間・公開)
      r.settingsOk = !!hit && hit.orderCountCond === r.orderCount && hit.discountFactor === r.amount
        && isoToYmd(hit.couponStartDate) === r.period.startYmd && isoToYmd(hit.couponEndDate) === r.period.endYmd
        && hit.displayFlag === STORE_COUPON_FIXED.displayFlag;
      console.log(`  [after] ${r.couponName}: ${hit ? `${r.settingsOk ? '✅' : '⚠️設定差異'} ${r.verifiedDetail}` : '⚠️一覧で確認できず'}`);
    }
  }

  // 何も起きなかった日 (全skip) は通知しない
  const hasNews = results.some((r) => r.status !== 'skip');
  if (hasNews) await notify(results);
  return results.some((r) => r.status === 'failed' || r.status === 'unknown' || r.status === 'error') ? 1 : 0;
}

async function notify(results) {
  const lines = ['*楽天ストアクーポン 入れ替え*', ''];
  for (const r of results) {
    if (r.status === 'issued') {
      lines.push(`*${r.couponName}*`);
      lines.push(`　値引き: *${r.amount}円* (前回 ${r.prevAmount}円)`);
      lines.push(`　期間: ${r.period.startIso.slice(0, 16).replace('T', ' ')} 〜 ${r.period.endIso.slice(0, 16).replace('T', ' ')}`);
      lines.push(`　結果: ${r.verified ? (r.settingsOk ? '✅発行済み' : '⚠️発行したが設定に差異あり — 要確認') : '⚠️発行したが一覧で未確認'}`);
      if (r.amountReset) lines.push(`　⚠️ 現行の${r.prevAmount}円が巡回リスト外だったため ${r.amount}円 にリセットしました`);
      if (r.period.delayed) lines.push(`　⚠️ 前回の期限を過ぎてからの作成です (空白期間が発生しました)`);
      if (r.period.truncated) lines.push(`　⚠️ 期間上限で終了日を切り詰めました`);
      lines.push('');
    } else if (r.status === 'skip') {
      // 通知には出さない (毎日実行のため)
    } else {
      lines.push(`❌ ${r.key}: ${r.reason || r.error}`);
      lines.push('');
    }
  }
  lines.push('_楽天はクーポンAPIで発行しています。開始60分前までなら削除できます_');
  await sendGChat(lines.join('\n'));
}

/**
 * 本番と同じ形のテストクーポンを**非公開**で1本だけ発行し、読み戻して削除する。
 * XML要素順 (otherConditions の位置) と期間上限を実機で確かめるための工程。
 */
async function cmdTestCycle() {
  const { coupons } = await searchAllCoupons();
  const todayYmd = todayJst();
  const def = STORE_COUPON_DEFS.find((d) => d.key === (ONLY[0] || 'qty3'));
  const plan = planStoreCoupon({ def, coupons, todayYmd, leadDays: 60, maxSpanDays: MAX_SPAN_DAYS });
  if (plan.status !== 'ready') {
    console.error(`[test-cycle] 計画が ready でない (${plan.status}: ${plan.reason})`);
    if (!plan.period) { console.error('[test-cycle] 期間も計算できないため中止'); return 1; }
    console.error('[test-cycle] 期間だけは計算できているので、その期間でXML検証を続行します');
  }
  // 本番と同じ形 (RS004・定額・期間) で、公開だけを止めたテストクーポン
  const params = {
    ...STORE_COUPON_FIXED,
    couponName: '【動作検証用】削除予定テストクーポン',
    couponStartDate: plan.period.startIso,
    couponEndDate: plan.period.endIso,
    discountFactor: plan.amount ?? def.cycle[0],
    orderCountCond: def.orderCount,
    displayFlag: 0, // 非公開 (獲得URLを配らない限り誰にも見えない)
    issueCount: 1,  // 万一公開されても1回しか使われない
  };
  const built = buildIssueXml(params);
  if (!built.ok) { console.error(`[test-cycle] 検証NG: ${built.errors.join(' / ')}`); return 1; }
  console.log('[test-cycle] 送信XML:');
  console.log(built.xml.replace(/^/gm, '  '));

  console.log('[test-cycle] 1/4 発行...');
  const issued = await issueCoupon(built.xml);
  if (!issued.ok || !issued.couponCode) {
    console.error(`[test-cycle] ❌発行失敗: HTTP ${issued.http} / ${issued.systemStatus} ${issued.message} / `
      + `${issued.errors.map((e) => `${e.code}:${e.message}`).join(', ') || '(詳細なし)'}`);
    return 1;
  }
  console.log(`[test-cycle] ✅発行OK (${maskCode(issued.couponCode)})`);

  console.log('[test-cycle] 2/4 設定の読み戻し...');
  const after = await searchAllCoupons();
  const hit = after.coupons.find((c) => c.couponCode === issued.couponCode);
  if (hit) {
    console.log(`[test-cycle] 読み戻し: ${hit.orderCountCond}点以上 / ${hit.discountFactor}円 / `
      + `${hit.couponStartDate}〜${hit.couponEndDate} / 公開=${hit.displayFlag} / 併用=${hit.combineFlag}`);
    const ok = hit.orderCountCond === params.orderCountCond && hit.discountFactor === params.discountFactor
      && hit.couponStartDate === params.couponStartDate && hit.couponEndDate === params.couponEndDate;
    console.log(`[test-cycle] ${ok ? '✅ 意図どおり (RS004・金額・期間すべて一致)' : '⚠️ 意図と差異あり'}`);
  } else {
    console.error('[test-cycle] ⚠️一覧で見つからない');
  }

  console.log('[test-cycle] 3/4 削除...');
  const delBuilt = buildDeleteXml(issued.couponCode);
  let delOk = false;
  if (!delBuilt.ok) console.error(`[test-cycle] 削除XML検証NG: ${delBuilt.errors.join(' / ')}`);
  else {
    try {
      const del = await deleteCoupon(delBuilt.xml);
      delOk = del.ok;
      if (!delOk) console.error(`[test-cycle] 削除失敗: ${del.errors.map((e) => `${e.code}:${e.message}`).join(', ') || del.message}`);
    } catch (e) { console.error(`[test-cycle] 削除呼び出しエラー: ${e.message}`); }
  }
  if (!delOk) console.error(`[test-cycle] ⚠️手動削除が必要: couponCode=${issued.couponCode} (非公開・利用上限1のテストクーポン)`);

  console.log('[test-cycle] 4/4 消滅確認...');
  const gone = await searchCouponByCode(issued.couponCode);
  const goneOk = gone.ok && gone.allCount === 0;
  console.log(`[test-cycle] ${goneOk ? '✅消滅を確認' : `⚠️まだ残っている (allCount=${gone.allCount})`}`);
  return (hit && delOk && goneOk) ? 0 : 1;
}

try {
  let code = 0;
  if (mode === 'list') code = (await cmdList()) ?? 0;
  else if (mode === 'rotate') code = await cmdRotate();
  else if (mode === 'test-cycle') code = await cmdTestCycle();
  else {
    console.error(`FATAL: unknown mode '${mode ?? '(なし)'}' (list / rotate [--live] / test-cycle)`);
    code = 2;
  }
  process.exitCode = code;
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
}
