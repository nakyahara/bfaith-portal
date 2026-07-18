/**
 * rakuten-coupon-lib.js — RMS クーポンAPI (es/1.0/coupon/*) ライブラリ (mall-csv-fetcher P2 PR-C3)
 *
 * らくらくーぽん置換 (設計書v1.2 §4 C3) の「共通キャンペーンクーポン」発行部。
 * **このPRではメール送信をしない** (クーポンの発行・削除・台帳記録のみ。メールは PR-C4)。
 *
 * 仕様の正本 = RMS WEB SERVICE 公式リファレンス coupon.issue (2026-07-18 実測で全確認):
 *   - POST https://api.rms.rakuten.co.jp/es/1.0/coupon/issue、Content-Type: application/xml
 *   - ルート <request><couponIssueRequest><coupon>、要素順は公式サンプルどおり
 *   - **必須フィールドが多い**: purchaseHistoryCond/genderCond/ageRangeCond/birthmonthCond/
 *     multiPrefectureCond も必須 (欠けると errors ではなく一律
 *     「Request data is wrong format」400 が返る — 公開クライアント実装 (.NET/Go) は旧仕様で全滅した罠)
 *   - couponStartDate は「現在の60分後〜30日後」の範囲必須。開始60分前を過ぎると編集・削除不可
 *   - 成功レスポンス = couponCode + pcGetUrl (獲得URL)。エラーは errors/error/code で返る
 *
 * 月次クーポンの運用値 (らくらくーぽん転記、coupon/search 実測):
 *   定率5% (discountType 2 / factor 5)、非公開 (displayFlag 0=限定公開)、1人1回
 *   (memberAvailMaxCount 1)、併用不可 (combineFlag 0)、受注対象 (itemType 4)、
 *   全利用回数無制限 (issueCount 0)、月初発行〜翌々月末有効
 *
 * クーポンコード・獲得URLは「持っている人が獲得できる」半機密 → コンソールには先頭3文字+***で
 * マスク表示、フル値は台帳 (rakuten_campaign_coupons) のみ (C4 のメール文面が参照する)。
 */
import { rakutenRequest } from './rakuten-client.js';

export const MONTHLY_COUPON_DEFAULTS = {
  couponName: '次回購入に使える5％割引クーポン',
  couponCaption: '雑貨イズム楽天市場店で使える5％OFFクーポンです。\n他のクーポンとの併用はできません。',
  issueCount: 0,          // 全利用回数無制限
  itemType: 4,            // 受注 (全商品)
  discountType: 2,        // 定率
  discountFactor: 5,      // 5%
  memberAvailMaxCount: 1, // 1ユーザ1回
  combineFlag: 0,         // 併用不可
  displayFlag: 0,         // 限定公開 (獲得URL配布のみ)
};

const DAY_MS = 86400000;

export function xmlEscape(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

const JST_DT_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+09:00$/;

/** coupon.issue リクエストXML (公式サンプルの完全形・要素順厳守)。検証込み */
export function buildIssueXml(p) {
  const errors = [];
  const name = String(p.couponName ?? '');
  const caption = String(p.couponCaption ?? '');
  if (!name) errors.push('couponName は必須');
  if (name.length > 60) errors.push(`couponName が60文字超 (${name.length})`);
  if (caption.length > 200) errors.push(`couponCaption が200文字超 (${caption.length})`);
  if (!JST_DT_RE.test(p.couponStartDate)) errors.push(`couponStartDate は YYYY-MM-DDThh:mm:ss+09:00 形式 (${p.couponStartDate})`);
  if (!JST_DT_RE.test(p.couponEndDate)) errors.push(`couponEndDate は YYYY-MM-DDThh:mm:ss+09:00 形式 (${p.couponEndDate})`);
  if (JST_DT_RE.test(p.couponStartDate) && JST_DT_RE.test(p.couponEndDate)
      && Date.parse(p.couponEndDate) < Date.parse(p.couponStartDate) + 5 * 60000) {
    errors.push('couponEndDate は couponStartDate+5分以降が必要');
  }
  for (const [k, lo, hi] of [['issueCount', 0, 999999], ['itemType', 1, 5], ['discountType', 1, 4],
    ['memberAvailMaxCount', 0, 999999], ['combineFlag', 0, 1], ['displayFlag', 0, 1]]) {
    const v = p[k];
    if (!Number.isInteger(v) || v < lo || v > hi) errors.push(`${k} が不正 (${v})`);
  }
  if (p.discountType === 2 && (!Number.isInteger(p.discountFactor) || p.discountFactor < 1 || p.discountFactor > 99)) {
    errors.push(`定率 (discountType=2) の discountFactor は 1〜99 (${p.discountFactor})`);
  }
  if (errors.length > 0) return { ok: false, errors };

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<request>
    <couponIssueRequest>
        <coupon>
            <couponName>${xmlEscape(name)}</couponName>
            <couponCaption>${xmlEscape(caption)}</couponCaption>
            <couponStartDate>${p.couponStartDate}</couponStartDate>
            <couponEndDate>${p.couponEndDate}</couponEndDate>
            <issueCount>${p.issueCount}</issueCount>
            <itemType>${p.itemType}</itemType>
            <discountType>${p.discountType}</discountType>
            <discountFactor>${p.discountFactor}</discountFactor>
            <memberAvailMaxCount>${p.memberAvailMaxCount}</memberAvailMaxCount>
            <purchaseHistoryCond>
                <type>0</type>
            </purchaseHistoryCond>
            <multiRankCond>
                <rankCond>0</rankCond>
            </multiRankCond>
            <genderCond>NONE</genderCond>
            <ageRangeCond>
                <lowerBound>0</lowerBound>
                <upperBound>0</upperBound>
            </ageRangeCond>
            <birthmonthCond>0</birthmonthCond>
            <multiPrefectureCond>
                <prefectureCond>NONE</prefectureCond>
            </multiPrefectureCond>
            <combineFlag>${p.combineFlag}</combineFlag>
            <displayFlag>${p.displayFlag}</displayFlag>
        </coupon>
    </couponIssueRequest>
</request>`;
  return { ok: true, xml };
}

export function buildDeleteXml(couponCode) {
  if (!/^[A-Za-z0-9-]{4,32}$/.test(String(couponCode))) {
    return { ok: false, errors: [`couponCode が不正 (${String(couponCode).slice(0, 8)}…)`] };
  }
  return {
    ok: true,
    xml: `<?xml version="1.0" encoding="UTF-8"?>
<request>
    <couponDeleteRequest>
        <coupon>
            <couponCode>${xmlEscape(couponCode)}</couponCode>
        </coupon>
    </couponDeleteRequest>
</request>`,
  };
}

/** coupon.* レスポンスXMLの共通パース。errors があれば ok=false (systemStatus OK でも errors が返る仕様)。
 *  ⚠️ search レスポンスは <status><requests> に検索条件の couponCode がそのままエコーされる
 *  (実機実測 2026-07-18)。couponCode をヒット判定に使うと常に一致してしまうため、
 *  status ブロックを除外してから抽出し、ヒット有無は allCount で判定すること */
export function parseCouponResult(xmlText) {
  const t = String(xmlText ?? '');
  const systemStatus = (t.match(/<systemStatus>([^<]*)<\/systemStatus>/) || [])[1] || null;
  const message = (t.match(/<message>([^<]*)<\/message>/) || [])[1] || null;
  const errors = [...t.matchAll(/<error>\s*<code>([^<]*)<\/code>\s*<message>([^<]*)<\/message>\s*<\/error>/g)]
    .map((m) => ({ code: m[1], message: m[2] }));
  const withoutStatus = t.replace(/<status>[\s\S]*?<\/status>/, '');
  const couponCode = (withoutStatus.match(/<couponCode>([^<]+)<\/couponCode>/) || [])[1] || null;
  const pcGetUrl = (withoutStatus.match(/<pcGetUrl>([^<]+)<\/pcGetUrl>/) || [])[1]?.replace(/&amp;/g, '&') || null;
  const allCountRaw = (t.match(/<allCount>(\d+)<\/allCount>/) || [])[1];
  return {
    ok: systemStatus === 'OK' && errors.length === 0,
    systemStatus, message, errors, couponCode, pcGetUrl,
    allCount: allCountRaw != null ? Number(allCountRaw) : null,
  };
}

export const maskCode = (s) => (s ? `${String(s).slice(0, 3)}***` : '(なし)');
export const maskUrl = (s) => (s ? String(s).replace(/getkey=[^&]+/, 'getkey=***') : '(なし)');

// ─── 月次クーポンのパラメータ計算 ───
/** 'YYYY-MM' + オフセット月 → その月の1日 (JST壁時計) の Date.parse 可能文字列 */
function monthStartJst(month, addMonths = 0) {
  const [y, m] = month.split('-').map(Number);
  const total = (y * 12 + (m - 1)) + addMonths;
  const yy = Math.floor(total / 12);
  const mm = (total % 12) + 1;
  return `${yy}-${String(mm).padStart(2, '0')}-01`;
}

/**
 * 月次クーポン (対象月の月初〜翌々月末) のパラメータ。
 * couponStartDate は API 制約 (now+60分〜now+30日) に収まるよう調整:
 *   - 月初がまだ先 → 月初 00:00 JST (30日超先ならエラー)
 *   - 月初を過ぎている (当月途中の発行) → now+61分 (分単位切り上げ)
 */
export function monthlyCouponParams(month, nowIso = new Date().toISOString()) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(String(month))) throw new Error(`月の指定が不正 (${month}、YYYY-MM)`);
  const nowMs = Date.parse(nowIso);
  const monthStartMs = Date.parse(`${monthStartJst(month)}T00:00:00+09:00`);
  const minStartMs = nowMs + 61 * 60000;
  if (monthStartMs > nowMs + 30 * DAY_MS) {
    throw new Error(`couponStartDate は30日先まで (対象月 ${month} の月初が遠すぎる)。発行は月初の30日前以降に`);
  }
  let startIso;
  if (monthStartMs >= minStartMs) {
    startIso = `${monthStartJst(month)}T00:00:00+09:00`;
  } else {
    // 当月途中: now+61分を分単位に切り上げて JST 表記に
    const t = new Date(Math.ceil(minStartMs / 60000) * 60000 + 9 * 3600 * 1000);
    startIso = `${t.toISOString().slice(0, 16).replace('T', 'T')}:00+09:00`.replace(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})/, '$1T$2');
  }
  // 終了 = 翌々月末 23:59:59 JST (= 3ヶ月後の月初の前日)
  const endMs = Date.parse(`${monthStartJst(month, 3)}T00:00:00+09:00`) - 1000;
  const endJstDate = new Date(endMs + 9 * 3600 * 1000).toISOString().slice(0, 10);
  const endIso = `${endJstDate}T23:59:59+09:00`;
  if (Date.parse(endIso) <= nowMs) throw new Error(`対象月 ${month} の有効期限 (${endIso}) が過去`);
  return { ...MONTHLY_COUPON_DEFAULTS, couponStartDate: startIso, couponEndDate: endIso, month };
}

// ─── 台帳 (C4 のメール文面が獲得URLを参照する正本) ───
export function ensureCouponRegistry(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_campaign_coupons (
    month        TEXT PRIMARY KEY CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    coupon_code  TEXT NOT NULL UNIQUE,
    pc_get_url   TEXT NOT NULL,
    coupon_start TEXT NOT NULL,
    coupon_end   TEXT NOT NULL,
    issued_at    TEXT NOT NULL,
    note         TEXT
  )`);
}

export function getRegisteredCoupon(db, month) {
  return db.prepare(`SELECT * FROM rakuten_campaign_coupons WHERE month = ?`).get(month) || null;
}

/** 発行成功後の台帳記録。同月の二重発行は PK が拒否 (上書きしない — 事故時は手動で行削除が明示操作) */
export function recordIssuedCoupon(db, { month, couponCode, pcGetUrl, couponStart, couponEnd, nowIso = new Date().toISOString(), note = null }) {
  db.prepare(`
    INSERT INTO rakuten_campaign_coupons (month, coupon_code, pc_get_url, coupon_start, coupon_end, issued_at, note)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(month, couponCode, pcGetUrl, couponStart, couponEnd, nowIso, note);
}

// ─── API 呼び出し (rakuten-client の直列キューに乗せる) ───
async function callCouponApi(path, xml) {
  const r = await rakutenRequest({
    path, method: xml ? 'POST' : 'GET', rawBody: xml,
    headers: xml ? { 'Content-Type': 'application/xml; charset=utf-8' } : {},
    timeoutMs: 30000,
  });
  const parsed = parseCouponResult(typeof r.data === 'string' ? r.data : JSON.stringify(r.data));
  return { http: r.status, ...parsed };
}

export const issueCoupon = (xml) => callCouponApi('/es/1.0/coupon/issue', xml);
export const deleteCoupon = (xml) => callCouponApi('/es/1.0/coupon/delete', xml);
export const searchCouponByCode = (code) => callCouponApi(`/es/1.0/coupon/search?couponCode=${encodeURIComponent(code)}`);
