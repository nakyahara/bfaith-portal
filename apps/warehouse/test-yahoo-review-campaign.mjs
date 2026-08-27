#!/usr/bin/env node
/** test-yahoo-review-campaign.mjs — PR-Y-C1 スモーク: raw_yahoo_orders → VIEW yahoo_order_contacts → planner (MALL_TABLES.yahoo)。実行: node apps/warehouse/test-yahoo-review-campaign.mjs */
import Database from 'better-sqlite3';
import { ensureYahooCampaignSources, yahooContactStats } from './yahoo-review-campaign-adapter.js';
import { createCampaignEngine } from './rakuten-review-campaign-lib.js';
import { ensureYahooReviewTables, importYahooReviewFile, HEADER_COLS } from './yahoo-review-lib.js';
import iconv from 'iconv-lite';
import crypto from 'node:crypto';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

const db = new Database(':memory:');
ensureYahooCampaignSources(db); ensureYahooCampaignSources(db); // 冪等 (VIEW 作り直し)
ensureYahooReviewTables(db);
const Y = createCampaignEngine('yahoo');
Y.ensureCampaignTables(db);

const ins = db.prepare(`INSERT INTO raw_yahoo_orders (order_id, order_time, order_status, ship_status, line_id, item_id, quantity, synced_at, ship_date, social_gift_type)
  VALUES (@id, @t, @st, @ship, @line, @item, 1, '2026-08-27T00:00:00Z', @sd, @gift)`);
const o = (id, t, st, ship, sd, gift = '0', lines = 1) => { for (let i = 1; i <= lines; i++) ins.run({ id, t, st, ship, line: i, item: `it${i}`, sd, gift }); };
o('Y-shipped',   '2026-08-15T10:00:00+09:00', '5', '3', '2026-08-16', '0', 2); // 発送済み・2明細
o('Y-unshipped', '2026-08-26T10:00:00+09:00', '2', '1', null);                 // 未発送
o('Y-cancel',    '2026-08-15T10:00:00+09:00', '4', '3', '2026-08-16');         // キャンセル
o('Y-gift',      '2026-08-15T10:00:00+09:00', '5', '3', '2026-08-16', '2');    // ソーシャルギフト
o('Y-noshipdate','2026-08-15T10:00:00+09:00', '5', '3', null);                 // Y-B 前の取込で ship_date 無し
o('Y-old',       '2026-07-01T10:00:00+09:00', '5', '3', '2026-07-02');         // 21日超

console.log('=== 1. VIEW yahoo_order_contacts ===');
{
  const rows = Object.fromEntries(db.prepare(`SELECT * FROM yahoo_order_contacts`).all().map((r) => [r.order_number, r]));
  check('注文単位に集約 (2明細→1行)・キャンセル/ギフトは除外', Object.keys(rows).sort().join(',') === 'Y-noshipdate,Y-old,Y-shipped,Y-unshipped', Object.keys(rows).join(','));
  check('発送済みは shipping_datetime = ship_date + T00:00:00+09:00', rows['Y-shipped'].shipping_datetime === '2026-08-16T00:00:00+09:00');
  check('未発送 / ship_date 無しは NULL', rows['Y-unshipped'].shipping_datetime === null && rows['Y-noshipdate'].shipping_datetime === null);
  check('宛先列はプレースホルダ (PII なし)', rows['Y-shipped'].masked_email_enc === '(api)' && rows['Y-shipped'].masked_email_hash === null && rows['Y-shipped'].purged_at === null);
  const st = yahooContactStats(db);
  check('stats: orders 4 / shipped 2 / cancelled 1 / social_gift 1', st.orders === 4 && st.shipped === 2 && st.cancelled === 1 && st.social_gift === 1, JSON.stringify(st));
}

console.log('=== 2. planner (shadow) ===');
{
  const NOW = '2026-08-27T00:00:00.000Z'; // 8/27 09:00 JST
  const c = Y.planCampaigns(db, { nowIso: NOW, couponEpochOverride: '2026-08-01T00:00:00.000Z' });
  const a = (id) => db.prepare(`SELECT action_type, status, status_reason, scheduled_at, expires_at FROM yahoo_campaign_actions WHERE order_number = ? ORDER BY action_type`).all(id);
  check('発送済み → フォロー planned (発送10日後 8/26 12:00 JST = 03:00Z)', a('Y-shipped')[0]?.status === 'planned' && a('Y-shipped')[0].scheduled_at === '2026-08-26T03:00:00.000Z', JSON.stringify(a('Y-shipped')));
  check('期限 = 発送日 JST 23:59:59 + 21日', a('Y-shipped')[0].expires_at === '2026-09-06T14:59:59.000Z');
  check('未発送・ship_date 無しは action なし (発送待ち)', a('Y-unshipped').length === 0 && a('Y-noshipdate').length === 0);
  check('キャンセル・ギフトは action なし', a('Y-cancel').length === 0 && a('Y-gift').length === 0);
  check('21日超は expired_before_plan', a('Y-old')[0]?.status === 'expired' && a('Y-old')[0].status_reason === 'expired_before_plan');
  check('ownership は shadow (vendor)、母集合 4 件', db.prepare(`SELECT COUNT(*) n FROM yahoo_order_campaign_ownership WHERE owner = 'vendor' AND reason = 'shadow'`).get().n === 4, JSON.stringify(c));
  // 予定時刻到来 → ready (masked_email_enc が '(api)' なので「宛先あり」として昇格する)
  const c2 = Y.planCampaigns(db, { nowIso: '2026-08-27T04:00:00.000Z' });
  check('予定到来で ready 昇格 (プレースホルダ宛先で待たされない)', a('Y-shipped')[0].status === 'ready', JSON.stringify(c2));
  // レビュー到着 → フォロー抑止 + クーポン action
  const csv = [HEADER_COLS, ['20260827', '5', '商品', 'it1', 'Y-shipped', 't', 'good', '0', '0', '0']].map((r) => r.map((v) => `"${v}"`).join(',')).join('\r\n') + '\r\n';
  const buf = iconv.encode(csv, 'Shift_JIS');
  importYahooReviewFile(db, { name: 'manual.csv', buffer: buf, sha256: crypto.createHash('sha256').update(buf).digest('hex'), nowIso: '2026-08-27T05:00:00.000Z' });
  const c3 = Y.planCampaigns(db, { nowIso: '2026-08-27T06:00:00.000Z' });
  check('レビュー到着 → フォロー suppressed(review_exists) + クーポン planned (次の12:00)',
    a('Y-shipped').find((x) => x.action_type === 'follow').status === 'suppressed'
    && a('Y-shipped').find((x) => x.action_type === 'coupon')?.status === 'planned'
    && a('Y-shipped').find((x) => x.action_type === 'coupon').scheduled_at === '2026-08-28T03:00:00.000Z', JSON.stringify(a('Y-shipped')) + JSON.stringify(c3));
  // 発送日が後から入った (Y-B 前の注文が再取得で ship_date 付きに) → フォロー action 生成
  db.prepare(`UPDATE raw_yahoo_orders SET ship_date = '2026-08-20' WHERE order_id = 'Y-noshipdate'`).run();
  Y.planCampaigns(db, { nowIso: '2026-08-27T07:00:00.000Z' });
  check('ship_date が後から入った注文はその時点で planned', a('Y-noshipdate')[0]?.status === 'planned' && a('Y-noshipdate')[0].scheduled_at === '2026-08-30T03:00:00.000Z');
  // 楽天テーブルには一切触れていない
  check('楽天テーブルは作られない', !db.prepare(`SELECT 1 FROM sqlite_master WHERE name = 'rakuten_campaign_actions'`).get());
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
