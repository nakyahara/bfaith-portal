#!/usr/bin/env node
/**
 * test-rakuten-review-contacts.mjs — PR-B スモークテスト (contacts暗号化+suppression+purge+削除検知)
 * DBはtemp (本番DB不触)。鍵はテスト用に生成。
 * 実行: node apps/warehouse/test-rakuten-review-contacts.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import iconv from 'iconv-lite';
import {
  ensureContactTables, loadContactKeys, encryptEmail, decryptEmail, hmacEmail, hmacOrderKey,
  extractContact, upsertContacts, purgeExpiredContacts, addSuppression, isSuppressed, computeDeleteAt,
} from './rakuten-review-contacts-lib.js';
import { ensureRakutenReviewTables, importReviewFile, parseWindowMarker } from './rakuten-review-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rrcontacts-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureContactTables(db);
ensureRakutenReviewTables(db);

const testEnv = {
  CONTACTS_ENC_KEY: crypto.randomBytes(32).toString('hex'),
  CONTACTS_HMAC_KEY: crypto.randomBytes(32).toString('hex'),
};
const keys = loadContactKeys(testEnv);

console.log('=== 1. 鍵と暗号化ヘルパ ===');
{
  let threw = false;
  try { loadContactKeys({}); } catch (e) { threw = /CONTACTS_KEY_MISSING/.test(e.message); }
  check('鍵未設定は fail-fast', threw);
  const email = 'Test+Masked@ANshin.rakuten.co.jp ';
  const enc = encryptEmail(email, keys);
  check('暗号文に平文が含まれない', !enc.includes('anshin') && !enc.includes('Masked'));
  check('復号でtrim+小文字化された平文が戻る', decryptEmail(enc, keys) === 'test+masked@anshin.rakuten.co.jp');
  const enc2 = encryptEmail(email, keys);
  check('毎回異なる暗号文 (IVランダム)', enc !== enc2);
  check('HMACは大文字小文字・空白ゆれで同一', hmacEmail(email, keys) === hmacEmail('test+masked@anshin.rakuten.co.jp', keys));
  const tampered = enc.replace(/.$/, (c) => (c === 'A' ? 'B' : 'A'));
  let authFail = false;
  try { decryptEmail(tampered, keys); } catch { authFail = true; }
  check('改ざん暗号文は復号エラー (GCM認証)', authFail);
}

console.log('=== 2. extractContact (getOrder応答のallowlist抽出) ===');
const orderFixture = {
  orderNumber: '373343-20260701-0000000001',
  orderDatetime: '2026-07-01T10:00:00+0900',
  orderProgress: 700,
  OrdererModel: {
    emailAddress: 'abc123@anshin.rakuten.co.jp',
    familyName: '漏れてはいけない氏名', firstName: '名前', zipCode1: '5640038PII', phoneNumber1: '09098765432PII',
  },
  PackageModelList: [
    { ShippingModelList: [{ shippingDate: '2026-07-03' }, { shippingDate: '2026-07-05' }] },
    { ShippingModelList: [{ shippingDate: '2026-07-04' }] },
  ],
};
{
  const rec = extractContact(orderFixture, keys);
  check('抽出できる', !!rec);
  check('最終発送日が採用される (複数配送)', rec.shipping_datetime === '2026-07-05T12:00:00+09:00');
  check('delete_at = 発送+35日', rec.contact_delete_at.slice(0, 10) === '2026-08-09');
  const s = JSON.stringify(rec);
  check('氏名・電話・郵便番号がレコードに含まれない', !s.includes('漏れて') && !s.includes('09098765432PII') && !s.includes('5640038PII'));
  check('平文メールがレコードに含まれない', !s.includes('abc123@'));
  check('order_key_hmacが決定的', rec.order_key_hmac === hmacOrderKey(orderFixture.orderNumber, keys));

  const noNum = extractContact({ OrdererModel: {} }, keys);
  check('注文番号なしは null (スキップ)', noNum === null);
  const noShip = extractContact({ ...orderFixture, PackageModelList: [] }, keys);
  check('未発送は注文日起点で delete_at', noShip.shipping_datetime === null && noShip.contact_delete_at.slice(0, 10) === '2026-08-05');
}

console.log('=== 3. upsert / purge / suppression ===');
{
  const rec = extractContact(orderFixture, keys);
  const r1 = upsertContacts(db, [rec]);
  check('insert される', r1.inserted === 1 && r1.updated === 0, JSON.stringify(r1));
  const r2 = upsertContacts(db, [{ ...rec, order_progress: 500 }]);
  check('再取得は update', r2.inserted === 0 && r2.updated === 1, JSON.stringify(r2));

  // 期限切れ行を作って purge
  const old = extractContact({ ...orderFixture, orderNumber: '373343-20260101-0000000009', PackageModelList: [{ ShippingModelList: [{ shippingDate: '2026-01-05' }] }] }, keys);
  upsertContacts(db, [old]);
  const purged = purgeExpiredContacts(db, '2026-07-16T00:00:00Z');
  check('期限超過だけ purge される', purged === 1);
  const row = db.prepare(`SELECT masked_email_enc, masked_email_hash, purged_at FROM rakuten_order_contacts WHERE order_number = ?`).get(old.order_number);
  check('purge後は暗号文NULL・HMACは残る', row.masked_email_enc === null && !!row.masked_email_hash && !!row.purged_at);
  // purge済み行への再UPSERTで暗号文が復活しない
  upsertContacts(db, [old]);
  const row2 = db.prepare(`SELECT masked_email_enc FROM rakuten_order_contacts WHERE order_number = ?`).get(old.order_number);
  check('purge済み行に再取得しても暗号文は復活しない', row2.masked_email_enc === null);

  const hash = addSuppression(db, 'ABC123@anshin.rakuten.co.jp', 'テスト停止', keys);
  check('suppression 登録 (表記ゆれ吸収)', isSuppressed(db, hmacEmail('abc123@anshin.rakuten.co.jp ', keys)));
  const supRow = db.prepare(`SELECT * FROM rakuten_contact_suppressions WHERE email_hash = ?`).get(hash);
  check('suppression に生アドレスが無い', !JSON.stringify(supRow).includes('abc123@'));
}

console.log('=== 4. レビュー削除検知 (窓2回連続不在) ===');
const cp932 = (s) => iconv.encode(s, 'Shift_JIS');
const HEADER = '"レビュータイプ","商品名","レビュー詳細URL","評価","投稿時間","タイトル","レビュー本文","フラグ","注文番号","未対応フラグ"';
const itemUrl = (token) => `https://review.rakuten.co.jp/item/1/373343_10000355/${token}_1_999/`;
const row = (url, rating = 5, ts = '2026/7/10 9:00:00') =>
  ['商品レビュー', '商品A', url, rating, ts, '', '本文', '0', '373343-20260701-0000000001', ''].map((v) => `"${v}"`).join(',');
const csvOf = (rows) => cp932([HEADER, ...rows].join('\r\n') + '\r\n');
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
{
  check('窓マーカーがパースできる', JSON.stringify(parseWindowMarker('rreview_csv_d2026-07-01_2026-07-16_x.csv')) === '{"from":"2026-07-01","to":"2026-07-16"}');
  check('マーカーなしは null', parseWindowMarker('manual.csv') === null);

  const urlKeep = itemUrl('keep');
  const urlGone = itemUrl('gone');
  // 1回目: 2件とも存在
  let buf = csvOf([row(urlKeep), row(urlGone)]);
  importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t1.csv', buffer: buf, sha256: sha(buf) });
  // 2回目: gone が消える → 不在1
  buf = csvOf([row(urlKeep)]);
  let out = importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t2.csv', buffer: buf, sha256: sha(buf) });
  check('不在1回目は missed=1 / deleted=0', out.results[0].missed === 1 && out.results[0].deleted === 0, JSON.stringify(out.results[0]));
  let g = db.prepare(`SELECT is_deleted, miss_count FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlGone);
  check('miss_count=1・未削除', g.miss_count === 1 && g.is_deleted === 0);
  // 3回目: まだ消えている → 2回連続不在 = 削除確定
  buf = csvOf([row(urlKeep, 4)]);
  out = importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t3.csv', buffer: buf, sha256: sha(buf) });
  check('2回連続不在で deleted=1', out.results[0].deleted === 1, JSON.stringify(out.results[0]));
  g = db.prepare(`SELECT is_deleted, miss_count FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlGone);
  check('is_deleted=1 になる', g.is_deleted === 1 && g.miss_count === 2);
  const rev = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_review_revisions WHERE review_url = ? AND is_deleted = 1`).get(urlGone).c;
  check('削除 revision が積まれる', rev === 1);
  // 削除済み行は以降の不在対象にならない + 集計から除外される
  buf = csvOf([row(urlKeep, 3)]);
  out = importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t4.csv', buffer: buf, sha256: sha(buf) });
  check('削除済みは再カウントしない', out.results[0].missed === 0 && out.results[0].deleted === 0, JSON.stringify(out.results[0]));
  // 再出現 → 自己修復
  buf = csvOf([row(urlKeep, 3), row(urlGone, 5)]);
  out = importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t5.csv', buffer: buf, sha256: sha(buf) });
  g = db.prepare(`SELECT is_deleted, miss_count FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlGone);
  check('再出現で is_deleted=0 / miss_count=0 に自己修復', g.is_deleted === 0 && g.miss_count === 0);
  // 窓マーカーなしファイルでは不在カウントしない
  buf = csvOf([row(urlKeep, 2, '2026/7/10 9:00:01')]);
  out = importReviewFile(db, { name: 'manual-upload.csv', buffer: buf, sha256: sha(buf) });
  check('マーカーなしは削除検知しない', out.results[0].missed === 0 && out.results[0].deleted === 0, JSON.stringify(out.results[0]));
  // 窓外のレビューは不在対象にならない
  const urlOld = itemUrl('old');
  buf = csvOf([row(urlOld, 5, '2026/6/1 9:00:00')]);
  importReviewFile(db, { name: 'rreview_csv_d2026-06-01_2026-06-30_t6.csv', buffer: buf, sha256: sha(buf) });
  buf = csvOf([row(urlKeep, 3, '2026/7/10 9:00:02')]);
  out = importReviewFile(db, { name: 'rreview_csv_d2026-07-01_2026-07-16_t7.csv', buffer: buf, sha256: sha(buf) });
  const o = db.prepare(`SELECT miss_count FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlOld);
  check('窓外レビューは不在カウントされない', o.miss_count === 0);
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== ${passed} passed / ${failed} failed ===`);
process.exit(failed > 0 ? 1 : 0);
