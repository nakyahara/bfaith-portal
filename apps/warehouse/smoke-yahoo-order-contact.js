#!/usr/bin/env node
/**
 * smoke-yahoo-order-contact.js — VPS proxy /yahoo/orderContact の疎通確認 (PR-Y-B デプロイ時に実行)
 *   raw_yahoo_orders の直近注文 1 件で呼び、メールアドレスは **ドメインと長さだけ** 表示する (値は出さない)。
 *   ついでに /yahoo/orderInfo に ShipDate / SocialGiftType が出ているかも確認。
 * 実行: node apps/warehouse/smoke-yahoo-order-contact.js  (env: DATA_DIR, YAHOO_PROXY_URL, YAHOO_PROXY_SECRET)
 * exit: 0=OK / 1=NG
 */
import 'dotenv/config';
import path from 'node:path';
import Database from 'better-sqlite3';
import { fetchYahooOrderContact } from './yahoo-order-contact-lib.js';

const DATA_DIR = (process.env.DATA_DIR || '').trim();
if (!DATA_DIR) { console.error('FATAL: DATA_DIR が必要'); process.exit(2); }
const db = new Database(path.join(DATA_DIR, 'warehouse.db'), { readonly: true });
const row = db.prepare(`SELECT order_id FROM raw_yahoo_orders WHERE ship_status = '3' ORDER BY order_time DESC LIMIT 1`).get()
  || db.prepare(`SELECT order_id FROM raw_yahoo_orders ORDER BY order_time DESC LIMIT 1`).get();
db.close();
if (!row) { console.error('FATAL: raw_yahoo_orders に注文が無い'); process.exit(1); }

try {
  const c = await fetchYahooOrderContact(row.order_id);
  const dom = c.email.split('@')[1];
  console.log(`✅ orderContact OK: order=${row.order_id} status=${c.orderStatus} ship=${c.shipStatus} shipDate=${c.shipDate || '(未発送)'} gift=${c.socialGiftType} email=[len ${c.email.length}, @${dom}]`);
} catch (e) {
  console.error(`❌ orderContact NG: ${e.code} retryable=${e.retryable} ${e.message}`);
  process.exit(1);
}
// orderInfo の新フィールド
const proxyUrl = process.env.YAHOO_PROXY_URL.replace(/\/$/, '');
const res = await fetch(`${proxyUrl}/yahoo/orderInfo?orderId=${encodeURIComponent(row.order_id)}`, { headers: { 'X-Proxy-Secret': process.env.YAHOO_PROXY_SECRET } });
const xml = await res.text();
const has = (t) => new RegExp(`<${t}>`).test(xml);
console.log(`${has('ShipDate') && has('SocialGiftType') ? '✅' : '❌'} orderInfo に ShipDate=${has('ShipDate')} SocialGiftType=${has('SocialGiftType')} (Bill* は含まない: ${!/<BillMailAddress>/.test(xml)})`);
if (!(has('ShipDate') && has('SocialGiftType')) || /<BillMailAddress>/.test(xml)) process.exit(1);
