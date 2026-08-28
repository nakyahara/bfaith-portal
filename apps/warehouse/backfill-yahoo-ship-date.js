#!/usr/bin/env node
/**
 * backfill-yahoo-ship-date.js — raw_yahoo_orders の ship_date / social_gift_type を後から埋める (P2-Y PR-Y-C2)
 *
 * なぜ要るか:
 *   - PR-Y-B (2026-08-27) より前に取り込んだ注文には ship_date が無く、Yahoo レビューメールの planner
 *     (VIEW yahoo_order_contacts) が「未発送」とみなす → フォロー予定が立たず、らくらくーぽん (vendor) の
 *     配信実績と突合できない (shadow 比較が全部 0 になる)
 *   - 受注取込は「受注日の直近 7 日」窓なので、受注から 8 日以上あとに発送された注文 (実測で 0.3% 程度) も
 *     ship_date が入らないまま残る
 * どちらも「注文 ID 単位で orderInfo を引き直して ship_date だけ UPDATE する」で解決する。
 * **明細行は触らない** (yahoo-orders.js の DELETE→INSERT と違い、金額・SKU を壊さない)。
 *
 * 実行:
 *   node apps/warehouse/backfill-yahoo-ship-date.js --days 25 --limit 3000     # 突合開始前の一括バックフィル
 *   node apps/warehouse/backfill-yahoo-ship-date.js --days 60 --limit 60       # daily-sync の毎日の穴埋め
 *   オプション: --dry-run (対象件数だけ表示) / --data-dir
 * env: DATA_DIR / YAHOO_PROXY_URL / YAHOO_PROXY_SECRET
 * exit: 0=成功 (対象0件含む) / 1=API 失敗が多すぎる / 2=env エラー
 *
 * 冪等: ship_date が埋まった注文は次回の対象にならない。中断しても続きから再開できる。
 * PII: orderInfo が返すのは ShipDate / SocialGiftType / ShipStatus など非PIIのみを見る (氏名・メールは読まない・保存しない)。
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { selectShipDateBackfillTargets } from './yahoo-review-campaign-adapter.js';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const num = (v, d) => { const n = Number.parseInt(v ?? '', 10); return Number.isFinite(n) && n > 0 ? n : d; };
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const DAYS = Math.min(num(getArg('--days'), 30), 400);
const LIMIT = Math.min(num(getArg('--limit'), 60), 5000);
const isDryRun = args.includes('--dry-run');
const PROXY_URL = (process.env.YAHOO_PROXY_URL || '').trim().replace(/\/$/, '');
const PROXY_SECRET = (process.env.YAHOO_PROXY_SECRET || '').trim();

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
if (!PROXY_URL || !PROXY_SECRET) { console.error('FATAL: YAHOO_PROXY_URL / YAHOO_PROXY_SECRET is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

/** <Tag>value</Tag> / CDATA の最初の一致 (無ければ null、空タグは '') */
function xmlText(text, tag) {
  const m = String(text).match(new RegExp(`<${tag}>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([^<]*))</${tag}>`));
  if (!m) return null;
  return m[1] != null ? m[1] : (m[2] || '');
}
const isYmd = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));

const db = new Database(dbPath);
db.pragma('busy_timeout = 10000');

// 対象: 直近 DAYS 日の注文で ship_date がまだ無いもの (キャンセルは除外 = 送信対象にならないので引く価値がない)。
// 新しい注文から埋める (突合に効くのは直近のため)
const targets = selectShipDateBackfillTargets(db, { days: DAYS, limit: LIMIT });

console.log(`=== Yahoo ship_date バックフィル (直近 ${DAYS} 日 / 上限 ${LIMIT} 件) ===`);
console.log(`対象: ${targets.length} 注文${isDryRun ? ' [DRY RUN]' : ''}`);

let updated = 0, unshipped = 0, apiError = 0, noChange = 0;
// ship_status も更新する (orderInfo の Ship.ShipStatus は注文単位。通常取込 yahoo-orders.js も全明細に同じ値を入れる)。
// これが無いと「7日窓の外で部分発送→出荷完了になった注文」を永久に未発送と判定してしまう
const upd = db.prepare(`UPDATE raw_yahoo_orders
     SET ship_date = COALESCE(@shipDate, ship_date),
         social_gift_type = COALESCE(@gift, social_gift_type),
         ship_status = COALESCE(@shipStatus, ship_status)
   WHERE order_id = @orderId`);

if (targets.length > 0 && !isDryRun) {
  const BATCH = 50;
  for (let i = 0; i < targets.length; i += BATCH) {
    const batch = targets.slice(i, i + BATCH);
    let data;
    try {
      const res = await fetch(`${PROXY_URL}/yahoo/orderInfo`, {
        method: 'POST',
        headers: { 'X-Proxy-Secret': PROXY_SECRET, 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderIds: batch }),
        signal: AbortSignal.timeout(300000), // VPS 側は 1.1 秒間隔で直列化 → 50 件で最大 ~60 秒
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      data = await res.json();
    } catch (e) {
      console.error(`  ⚠ batch ${i + 1}-${i + batch.length} 失敗: ${e.message}`);
      apiError += batch.length;
      continue;
    }
    const tx = db.transaction((rows) => {
      for (const r of rows) {
        const xml = String(r.xml || '');
        if (/<Error>/.test(xml)) { apiError++; continue; }
        const shipDate = xmlText(xml, 'ShipDate');
        const gift = xmlText(xml, 'SocialGiftType');
        const shipStatus = xmlText(xml, 'ShipStatus');
        if (!isYmd(shipDate)) { unshipped++; upd.run({ orderId: r.orderId, shipDate: null, gift: gift || null, shipStatus: shipStatus || null }); continue; }
        const c = upd.run({ orderId: r.orderId, shipDate, gift: gift || null, shipStatus: shipStatus || null });
        if (c.changes > 0) updated++; else noChange++;
      }
    });
    tx(data.results || []);
    console.log(`  ${Math.min(i + BATCH, targets.length)}/${targets.length} … 発送日あり ${updated} / 未発送 ${unshipped} / エラー ${apiError}`);
  }
}
db.close();

console.log(`=== summary: 更新 ${updated} / 未発送のまま ${unshipped} / API失敗 ${apiError} / 対象外 ${noChange} ===`);
if (!isDryRun && targets.length > 0 && apiError > targets.length * 0.5) {
  console.error('FATAL: API 失敗が半数を超えた (プロキシ/トークンを確認)');
  process.exitCode = 1;
} else {
  process.exitCode = 0;
}
