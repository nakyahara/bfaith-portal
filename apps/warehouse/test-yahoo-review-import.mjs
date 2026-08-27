#!/usr/bin/env node
/**
 * test-yahoo-review-import.mjs — PR-Y-A スモーク (ZIP 解釈・identity/revision・低評価通知キュー・衝突 fail-closed・削除検知・冪等)
 * 実行: node apps/warehouse/test-yahoo-review-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import {
  ensureYahooReviewTables, prepareYahooReviewFile, importYahooReviewFile, reviewIdentityFor, revisionHashFor, parseWindowMarker, HEADER_COLS,
} from './yahoo-review-lib.js';
import { createCampaignEngine } from './rakuten-review-campaign-lib.js';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

function csvOf(rows) {
  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  return [HEADER_COLS, ...rows].map((r) => r.map(esc).join(',')).join('\r\n') + '\r\n';
}
function zipOf(rows, entryName = '20260530_20260827_ItemReview.csv') {
  const z = new AdmZip();
  z.addFile(entryName, iconv.encode(csvOf(rows), 'Shift_JIS'));
  return z.toBuffer();
}
const row = (date, rating, code, order, title, body, v = 0, img = 0, like = 0) => [date, String(rating), `商品 ${code}`, code, order, title, body, String(v), String(img), String(like)];
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'yreview-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
ensureYahooReviewTables(db); ensureYahooReviewTables(db);

console.log('=== 1. ZIP/CSV 解釈 ===');
{
  const rows = [
    row('20260827', 5, 'golden-jojoba-1000', 'b-faith01-10287955', '迅速', '迅速な対応でした。'),
    row('20260826', 2, 'lemon-oil', 'b-faith01-10287900', '', '香りが弱い', 0, 1),
    row('20260826', 2, 'lemon-oil', 'b-faith01-10287900', '', '香りが弱い', 0, 1), // 完全同一行 (マージ)
  ];
  const p = prepareYahooReviewFile('20260530_20260827_ItemReview.zip', zipOf(rows));
  check('ZIP → cp932 CSV → 2 レコード (同一行はマージ)', p.ok && p.records.length === 2 && p.warnings.length === 1, p.error || JSON.stringify(p.warnings));
  check('日付正規化・posted_at・review_url', p.ok && p.dateFrom === '2026-08-26' && p.dateTo === '2026-08-27'
    && p.records[0].posted_at === '2026-08-27 00:00:00' && p.records[0].review_url === `yahoo:${reviewIdentityFor('b-faith01-10287955', 'golden-jojoba-1000')}`);
  check('identity は注文ID+商品コードで決定的、revision は内容で変わる',
    reviewIdentityFor('a', 'b') === reviewIdentityFor(' a ', 'b') && revisionHashFor(p.records[0]) !== revisionHashFor({ ...p.records[0], body: 'x' }));
  const bad = prepareYahooReviewFile('x.zip', zipOf([row('20260231', 5, 'c', 'o', '', '')]));
  check('実在しない評価日は拒否', !bad.ok && /評価日/.test(bad.error));
  const badHdr = new AdmZip(); badHdr.addFile('a.csv', iconv.encode('"評価日","評価点数"\r\n"20260101","5"\r\n', 'Shift_JIS'));
  check('ヘッダ不一致は拒否', !prepareYahooReviewFile('a.zip', badHdr.toBuffer()).ok);
  const two = new AdmZip(); two.addFile('a.csv', Buffer.from('x')); two.addFile('b.csv', Buffer.from('y'));
  check('ZIP エントリ 2 本は拒否', !prepareYahooReviewFile('two.zip', two.toBuffer()).ok);
  const trav = new AdmZip(); trav.addFile('../evil.csv', iconv.encode(csvOf([]), 'Shift_JIS'));
  check('path traversal エントリは拒否', !prepareYahooReviewFile('t.zip', trav.toBuffer()).ok);
  check('素の CSV も受理', prepareYahooReviewFile('manual.csv', iconv.encode(csvOf(rows.slice(0, 1)), 'Shift_JIS')).ok);
  check('窓マーカー', JSON.stringify(parseWindowMarker('yreview_d2026-05-30_2026-08-27_x.zip')) === '{"from":"2026-05-30","to":"2026-08-27"}' && parseWindowMarker('20260530_20260827_ItemReview.zip') === null);
}

console.log('=== 2. 取込: identity/revision・低評価キュー・冪等 ===');
const T1 = '2026-08-28T00:00:00.000Z';
{
  const rows = [
    row('20260827', 5, 'p1', 'o1', 't', 'good'),
    row('20260826', 2, 'p2', 'o2', 't', 'bad', 0, 1),
    row('20260820', 4, 'p3', 'o3', 't', 'ok'),
  ];
  const buf = zipOf(rows);
  const r = importYahooReviewFile(db, { name: 'yreview_d2026-05-30_2026-08-27_a.zip', buffer: buf, sha256: sha(buf), nowIso: T1 });
  check('初回取込 3 件 insert', r.status === 'ok' && r.results[0].inserted === 3 && r.results[0].deleted === 0, JSON.stringify(r.results[0]));
  check('★2 は初回観測で通知キューへ (kind=first)', r.newLowRatings.length === 1
    && db.prepare(`SELECT kind FROM yahoo_review_low_notify_queue`).get().kind === 'first');
  check('revision 表に 3 行 (PK identity+hash)', db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_review_revisions`).get().n === 3);
  const r2 = importYahooReviewFile(db, { name: 'yreview_d2026-05-30_2026-08-27_a.zip', buffer: buf, sha256: sha(buf), nowIso: T1 });
  check('同一 sha256 は duplicate (変更なし)', r2.status === 'duplicate' && db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_reviews`).get().n === 3);
  // 編集: o3 が ★4→★1 (遷移通知)、o1 は本文編集 (通知なし)
  const rows2 = [row('20260827', 5, 'p1', 'o1', 't', 'good!!'), row('20260826', 2, 'p2', 'o2', 't', 'bad', 0, 1), row('20260820', 1, 'p3', 'o3', 't', 'terrible')];
  const buf2 = zipOf(rows2);
  const r3 = importYahooReviewFile(db, { name: 'yreview_d2026-05-30_2026-08-27_b.zip', buffer: buf2, sha256: sha(buf2), nowIso: '2026-08-29T00:00:00.000Z' });
  check('編集は update (2) / 変化なし (1)、identity は増えない', r3.results[0].updated === 2 && r3.results[0].unchanged === 1 && db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_reviews`).get().n === 3);
  check('★3以上→★2以下の遷移だけ通知 (kind=transition)、既に★2の o2 は再通知しない',
    r3.newLowRatings.length === 1 && r3.newLowRatings[0].transition === true
    && db.prepare(`SELECT COUNT(*) n FROM yahoo_review_low_notify_queue WHERE kind = 'transition'`).get().n === 1
    && db.prepare(`SELECT COUNT(*) n FROM yahoo_review_low_notify_queue`).get().n === 2);
  check('revision が 5 行 (編集 2 件分が追加)', db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_review_revisions`).get().n === 5);
  check('current 行は最新の版', db.prepare(`SELECT rating, body FROM fact_yahoo_reviews WHERE order_number = 'o3'`).get().rating === 1);
}

console.log('=== 3. identity 衝突は fail-closed ===');
{
  const rows = [row('20260827', 5, 'p9', 'o9', 'a', 'first review'), row('20260827', 3, 'p9', 'o9', 'b', 'second review')];
  const p = prepareYahooReviewFile('c.zip', zipOf(rows));
  check('prepare: 同一注文×商品で内容が違う行 → records から外し conflicts に', p.ok && p.records.length === 0 && p.conflicts.length === 1);
  const buf = zipOf(rows);
  const r = importYahooReviewFile(db, { name: 'manual.zip', buffer: buf, sha256: sha(buf), nowIso: '2026-08-29T01:00:00.000Z' });
  const rowc = db.prepare(`SELECT conflict FROM fact_yahoo_reviews WHERE order_number = 'o9'`).get();
  check('取込: conflict=1 で保持し conflict 通知をキューへ', r.status === 'ok' && rowc.conflict === 1
    && db.prepare(`SELECT kind FROM yahoo_review_low_notify_queue WHERE review_identity = ?`).get(reviewIdentityFor('o9', 'p9')).kind === 'conflict');
}

console.log('=== 4. 削除検知 (検証済み全量スナップショットのみ) ===');
{
  // o2 が消えた全量 (窓内)。1 回目 miss、2 回目 (別日) で deleted。窓外 (o3 は 8/20、窓 8/25〜) は対象外
  const rowsA = [row('20260827', 5, 'p1', 'o1', 't', 'good!!')];
  const bufA = zipOf(rowsA);
  const rA = importYahooReviewFile(db, { name: 'yreview_d2026-08-25_2026-08-29_c.zip', buffer: bufA, sha256: sha(bufA), nowIso: '2026-08-30T00:00:00.000Z' });
  check('1 回目不在 → miss+1 (o2 のみ。窓外の o3/衝突 o9 は数えない)', rA.results[0].missed === 1 && rA.results[0].deleted === 0
    && db.prepare(`SELECT miss_count, is_deleted FROM fact_yahoo_reviews WHERE order_number = 'o2'`).get().miss_count === 1, JSON.stringify(rA.results[0]));
  const rA2 = importYahooReviewFile(db, { name: 'yreview_d2026-08-25_2026-08-29_d.zip', buffer: zipOf(rowsA, 'x.csv'), sha256: 'different', nowIso: '2026-08-30T02:00:00.000Z' });
  check('同日 2 回目は数えない', rA2.results[0].missed === 0 && rA2.results[0].deleted === 0);
  const rB = importYahooReviewFile(db, { name: 'yreview_d2026-08-25_2026-08-31_e.zip', buffer: zipOf(rowsA, 'y.csv'), sha256: 'different2', nowIso: '2026-08-31T00:00:00.000Z' });
  check('別日 2 回連続不在 → is_deleted=1 + deleted revision', rB.results[0].deleted === 1
    && db.prepare(`SELECT is_deleted FROM fact_yahoo_reviews WHERE order_number = 'o2'`).get().is_deleted === 1
    && db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_review_revisions WHERE is_deleted = 1`).get().n === 1);
  const rNoMarker = importYahooReviewFile(db, { name: 'manual2.zip', buffer: zipOf([], 'z.csv'), sha256: 'different3', nowIso: '2026-09-01T00:00:00.000Z' });
  check('窓マーカー無し (手動DL) は削除検知しない', rNoMarker.status === 'ok' && rNoMarker.results[0].missed === 0 && rNoMarker.results[0].deleted === 0);
  const rBack = importYahooReviewFile(db, { name: 'yreview_d2026-08-25_2026-09-01_f.zip', buffer: zipOf([...rowsA, row('20260826', 2, 'p2', 'o2', 't', 'bad', 0, 1)], 'w.csv'), sha256: 'different4', nowIso: '2026-09-02T00:00:00.000Z' });
  check('再出現で自己修復 (is_deleted=0, miss_count=0)', db.prepare(`SELECT is_deleted, miss_count FROM fact_yahoo_reviews WHERE order_number = 'o2'`).get().is_deleted === 0 && rBack.status === 'ok');
}

console.log('=== 5. planner (MALL_TABLES.yahoo) が fact_yahoo_reviews を読める ===');
{
  db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
    order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
  db.exec(`CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)`);
  const Y = createCampaignEngine('yahoo');
  Y.ensureCampaignTables(db);
  db.prepare(`INSERT INTO yahoo_order_contacts (order_number, shipping_datetime, masked_email_enc, fetched_at) VALUES ('o1', '2026-08-20T12:00:00+09:00', '(api)', ?), ('o5', '2026-08-25T12:00:00+09:00', '(api)', ?)`).run(T1, T1);
  const c = Y.planCampaigns(db, { nowIso: '2026-09-02T01:00:00.000Z', couponEpochOverride: '2026-08-01T00:00:00.000Z' });
  const st = (o) => db.prepare(`SELECT action_type, status, status_reason FROM yahoo_campaign_actions WHERE order_number = ? ORDER BY action_type`).all(o);
  check('レビュー済み注文 o1: フォロー suppressed(review_exists) + クーポン action 生成', JSON.stringify(st('o1')) === JSON.stringify([
    { action_type: 'coupon', status: 'planned', status_reason: null }, { action_type: 'follow', status: 'suppressed', status_reason: 'review_exists' }]), JSON.stringify(st('o1')));
  check('未レビュー注文 o5: フォロー planned', st('o5')[0].action_type === 'follow' && st('o5')[0].status === 'planned', JSON.stringify(c));
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(tmp, { recursive: true, force: true });
process.exit(failed > 0 ? 1 : 0);
