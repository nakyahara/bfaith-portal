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
// XML 1.0 で許可されないコードポイント (タブ・LF・CR は許可)
const hasXmlInvalidChar = (s) => { for (let i = 0; i < s.length; i++) { const c = s.charCodeAt(i); if (c < 0x20 && c !== 0x09 && c !== 0x0A && c !== 0x0D) return true; } return false; };

/** coupon.issue リクエストXML (公式サンプルの完全形・要素順厳守)。検証込み */
export function buildIssueXml(p) {
  const errors = [];
  const name = String(p.couponName ?? '');
  const caption = String(p.couponCaption ?? '');
  if (!name) errors.push('couponName は必須');
  if (name.length > 60) errors.push(`couponName が60文字超 (${name.length})`);
  if (caption.length > 200) errors.push(`couponCaption が200文字超 (${caption.length})`);
  // XML 1.0 が許さない制御文字が混じると不正なXMLになる (エスケープでは救えない)
  if (hasXmlInvalidChar(name)) errors.push('couponName にXMLで使えない制御文字が含まれる');
  if (hasXmlInvalidChar(caption)) errors.push('couponCaption にXMLで使えない制御文字が含まれる');
  if (!JST_DT_RE.test(p.couponStartDate)) errors.push(`couponStartDate は YYYY-MM-DDThh:mm:ss+09:00 形式 (${p.couponStartDate})`);
  if (!JST_DT_RE.test(p.couponEndDate)) errors.push(`couponEndDate は YYYY-MM-DDThh:mm:ss+09:00 形式 (${p.couponEndDate})`);
  // 形式が合っていても 2026-99-99 のような実在しない日時は弾く (正規表現だけでは通ってしまう)
  for (const k of ['couponStartDate', 'couponEndDate']) {
    if (JST_DT_RE.test(p[k]) && !Number.isFinite(Date.parse(p[k]))) errors.push(`${k} が実在しない日時 (${p[k]})`);
  }
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
  if (p.discountType === 1 && (!Number.isInteger(p.discountFactor) || p.discountFactor < 1 || p.discountFactor > 999999)) {
    errors.push(`定額 (discountType=1) の discountFactor は 1〜999999円 (${p.discountFactor})`);
  }
  // 利用個数条件 (RS004)。省略時は otherConditions ごと出力しない (RS001/RS002 は楽天側で自動設定)
  const oc = p.orderCountCond;
  if (oc != null && (!Number.isInteger(oc) || oc < 1 || oc > 999999999)) {
    errors.push(`orderCountCond (利用個数 RS004) は 1〜999999999 の整数 (${oc})`);
  }
  if (errors.length > 0) return { ok: false, errors };

  // 「N個以上購入で」条件。実測 (2026-07-31 coupon.search) では店舗の全品クーポンが
  //   RS001=0,RS001=1 (PC/モバイル) + RS002=0 (通常購入) + RS004=N
  // を持つが、RS001/RS002 は「設定のない場合は自動的に設定される」(公式) ため RS004 だけ送る。
  // 要素順は .NET 公式クライアントの宣言順に合わせて displayFlag の後 (items/otherConditions が末尾)。
  const otherConditionsXml = oc == null ? '' : `
            <otherConditions>
                <otherCondition>
                    <conditionTypeCode>RS004</conditionTypeCode>
                    <startValue>${oc}</startValue>
                </otherCondition>
            </otherConditions>`;

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
            <displayFlag>${p.displayFlag}</displayFlag>${otherConditionsXml}
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
 *   - 月初を過ぎている (当月途中の発行) → now+90分 (分単位切り上げ。
 *     Codex C3-R1 Medium: 61分だとキュー待ち・タイムアウトで送信時に60分条件を割り得る)
 */
export function monthlyCouponParams(month, nowIso = new Date().toISOString()) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(String(month))) throw new Error(`月の指定が不正 (${month}、YYYY-MM)`);
  const nowMs = Date.parse(nowIso);
  const monthStartMs = Date.parse(`${monthStartJst(month)}T00:00:00+09:00`);
  const minStartMs = nowMs + 90 * 60000;
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
// at-most-once 発行の予約方式 (Codex C3-R1 Critical/High):
//   ① reserveMonth = 月行を status='pending' で原子的に INSERT (PK衝突=他プロセスが予約/発行済み→中止)
//   ② API 呼び出し (リトライなし — 非冪等 POST を自動再送すると多重発行する)
//   ③ 成功 → markIssued / 明確な失敗 → releaseReservation / 結果不明 (timeout等)
//      → pending を残して人が coupon.search で確認 (自動再発行させない)
const COUPON_REGISTRY_DDL = `CREATE TABLE IF NOT EXISTS rakuten_campaign_coupons (
    month        TEXT PRIMARY KEY CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    status       TEXT NOT NULL CHECK (status IN ('pending','issued')),
    coupon_code  TEXT UNIQUE,
    pc_get_url   TEXT,
    coupon_start TEXT NOT NULL,
    coupon_end   TEXT NOT NULL,
    reserved_at  TEXT NOT NULL,
    issued_at    TEXT,
    note         TEXT
  )`;

export function ensureCouponRegistry(db) {
  db.exec(COUPON_REGISTRY_DDL);
  // 移行 (2026-08-26 miniPC 本番で発覚): PR-C3 の初期ドラフト (status/reserved_at なし・coupon_code NOT NULL)
  // で作られた表が CREATE IF NOT EXISTS で残っていた。旧表のままだと reserveMonth が「no such column:
  // status」で落ち、sender は永久に no_monthly_coupon。行が無ければ作り直す。行があれば人が移行する
  // (自動で捨てない — 発行済みクーポンの正本を失わないため)
  const cols = db.prepare(`PRAGMA table_info(rakuten_campaign_coupons)`).all().map((c) => c.name);
  if (!cols.includes('status') || !cols.includes('reserved_at')) {
    const n = db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_coupons`).get().n;
    if (n > 0) {
      throw new Error(`rakuten_campaign_coupons が旧スキーマ (${cols.join(',')}) で ${n} 行あります。手動で移行してください (status='issued', reserved_at=issued_at を補う)`);
    }
    db.exec(`DROP TABLE rakuten_campaign_coupons`);
    db.exec(COUPON_REGISTRY_DDL);
  }
}

export function getRegisteredCoupon(db, month) {
  return db.prepare(`SELECT * FROM rakuten_campaign_coupons WHERE month = ?`).get(month) || null;
}

/** 発行予約 (原子的)。@returns true=予約成功 / false=既に行がある (pending含む) */
export function reserveMonth(db, { month, couponStart, couponEnd, nowIso = new Date().toISOString() }) {
  try {
    db.prepare(`
      INSERT INTO rakuten_campaign_coupons (month, status, coupon_start, coupon_end, reserved_at)
      VALUES (?, 'pending', ?, ?, ?)
    `).run(month, couponStart, couponEnd, nowIso);
    return true;
  } catch (e) {
    if (/UNIQUE|PRIMARY/i.test(String(e.message))) return false;
    throw e;
  }
}

/** 発行成功の確定 (pending → issued)。@returns 更新できたか */
export function markIssued(db, { month, couponCode, pcGetUrl, nowIso = new Date().toISOString() }) {
  const r = db.prepare(`
    UPDATE rakuten_campaign_coupons
       SET status = 'issued', coupon_code = ?, pc_get_url = ?, issued_at = ?
     WHERE month = ? AND status = 'pending'
  `).run(couponCode, pcGetUrl, nowIso, month);
  return r.changes === 1;
}

/** 明確な失敗時の予約解放 (結果不明のときは呼ばない — pending を残して人が確認) */
export function releaseReservation(db, month) {
  return db.prepare(`DELETE FROM rakuten_campaign_coupons WHERE month = ? AND status = 'pending'`).run(month).changes;
}

/** 結果不明 (timeout等) の注記 (pending は残す) */
export function notePendingAmbiguous(db, month, note) {
  db.prepare(`UPDATE rakuten_campaign_coupons SET note = ? WHERE month = ? AND status = 'pending'`).run(note, month);
}

// ─── API 呼び出し (rakuten-client の直列キューに乗せる) ───
// maxAttempts: 発行/削除は 1 (Codex C3-R1 Critical: 非冪等 POST の自動リトライは
// タイムアウト時点で楽天側発行済みでも再送し多重発行する)。search (GET) のみリトライ許可
async function callCouponApi(path, xml, maxAttempts) {
  const r = await rakutenRequest({
    path, method: xml ? 'POST' : 'GET', rawBody: xml,
    headers: xml ? { 'Content-Type': 'application/xml; charset=utf-8' } : {},
    timeoutMs: 30000,
    maxAttempts,
  });
  const parsed = parseCouponResult(typeof r.data === 'string' ? r.data : JSON.stringify(r.data));
  return { http: r.status, ...parsed };
}

export const issueCoupon = (xml) => callCouponApi('/es/1.0/coupon/issue', xml, 1);
export const deleteCoupon = (xml) => callCouponApi('/es/1.0/coupon/delete', xml, 1);
export const searchCouponByCode = (code) => callCouponApi(`/es/1.0/coupon/search?couponCode=${encodeURIComponent(code)}`, undefined, 4);

// ─── クーポン一覧の取得 (店舗クーポン入れ替えが「現況」を読むための GET) ───

const xmlUnescape = (s) => String(s)
  .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
  .replace(/&apos;/g, "'").replace(/&amp;/g, '&');

/**
 * coupon.search のレスポンスXMLを配列に変換する。
 * ⚠️ <status><requests> に検索条件がエコーされるため、status ブロックを除いてから抽出する
 *    (parseCouponResult と同じ罠。2026-07-18 実測)。
 */
export function parseCouponSearchList(xmlText) {
  const t = String(xmlText ?? '');
  const systemStatus = (t.match(/<systemStatus>([^<]*)<\/systemStatus>/) || [])[1] || null;
  const allCountRaw = (t.match(/<allCount>(\d+)<\/allCount>/) || [])[1];
  const body = t.replace(/<status>[\s\S]*?<\/status>/, '');
  const get = (blk, tag) => {
    const m = blk.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
    return m ? xmlUnescape(m[1]) : '';
  };
  const num = (blk, tag) => {
    const v = get(blk, tag);
    return v === '' ? null : Number(v);
  };
  const coupons = [...body.matchAll(/<coupon>([\s\S]*?)<\/coupon>/g)].map((m) => {
    const blk = m[1];
    const conds = [...blk.matchAll(/<otherCondition>\s*<conditionTypeCode>([^<]+)<\/conditionTypeCode>\s*<startValue>([^<]*)<\/startValue>\s*<\/otherCondition>/g)]
      .map((c) => ({ code: c[1], value: c[2] }));
    const rs004 = conds.find((c) => c.code === 'RS004');
    return {
      couponCode: get(blk, 'couponCode'),
      couponName: get(blk, 'couponName'),
      couponStartDate: get(blk, 'couponStartDate'),
      couponEndDate: get(blk, 'couponEndDate'),
      itemType: num(blk, 'itemType'),
      discountType: num(blk, 'discountType'),
      discountFactor: num(blk, 'discountFactor'),
      issueCount: num(blk, 'issueCount'),
      memberAvailMaxCount: num(blk, 'memberAvailMaxCount'),
      combineFlag: num(blk, 'combineFlag'),
      displayFlag: num(blk, 'displayFlag'),
      couponStatus: num(blk, 'couponStatus'),
      getCount: num(blk, 'getCount'),
      // 利用個数条件 (「N点以上購入で」)。無い場合は null
      orderCountCond: rs004 ? Number(rs004.value) : null,
      regDate: get(blk, 'regDate'),
    };
  });
  return { ok: systemStatus === 'OK', systemStatus, allCount: allCountRaw != null ? Number(allCountRaw) : null, coupons };
}

/** クーポンを全ページ取得する (GET なのでリトライ可)。maxPages は暴走防止 */
export async function searchAllCoupons({ hits = 50, maxPages = 20 } = {}) {
  const out = [];
  let allCount = null;
  for (let page = 1; page <= maxPages; page++) {
    const r = await rakutenRequest({
      path: `/es/1.0/coupon/search?hits=${hits}&page=${page}`, method: 'GET', timeoutMs: 30000, maxAttempts: 4,
    });
    const text = typeof r.data === 'string' ? r.data : JSON.stringify(r.data);
    const parsed = parseCouponSearchList(text);
    if (!parsed.ok) throw new Error(`coupon.search 失敗 (HTTP ${r.status} / ${parsed.systemStatus})`);
    allCount = parsed.allCount ?? allCount;
    out.push(...parsed.coupons);
    if (parsed.coupons.length === 0 || (allCount != null && out.length >= allCount)) break;
  }
  // 全件取り切れないまま打ち切ると「現況」を誤読し、重複を見落として二重発行しかねない → fail-closed
  if (allCount != null && out.length < allCount) {
    throw new Error(`coupon.search を全件取得できなかった (${out.length}/${allCount}件、hits=${hits} maxPages=${maxPages})`);
  }
  return { allCount, coupons: out };
}
