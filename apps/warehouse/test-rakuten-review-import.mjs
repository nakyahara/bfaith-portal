#!/usr/bin/env node
/**
 * test-rakuten-review-import.mjs — 楽天レビューCSV取込のスモークテスト (mall-csv-fetcher P2 PR-A)
 *
 * 実DL (2026-07-16 reviews_*.csv) の実測ヘッダ・実データ形を再現した合成フィクスチャで、
 * パース〜UPSERT〜revision〜冪等〜低評価検知〜集計projectionを検証する。DBはtemp (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-rakuten-review-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import iconv from 'iconv-lite';
import { ensureRakutenReviewTables, importReviewFile, prepareReviewFile } from './rakuten-review-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'rreview-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureRakutenReviewTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const cp932 = (s) => iconv.encode(s, 'Shift_JIS');

// ─── フィクスチャ (実測: CP932、全列クォート、ヘッダ1行目) ───
const HEADER = '"レビュータイプ","商品名","レビュー詳細URL","評価","投稿時間","タイトル","レビュー本文","フラグ","注文番号","未対応フラグ"';
const itemUrl = (token, itemId, revId) => `https://review.rakuten.co.jp/item/1/373343_${itemId}/${token}_1_${revId}/`;
const shopUrl = (token) => `https://review.rakuten.co.jp/shop/4/373343_373343/${token}_1_1/`;
function row({ type = '商品レビュー', name = 'テスト商品 100ml', url, rating = 5, ts = '2026/7/10 9:07:30', title = '', body = 'よい商品でした。', flag = '0', order = '373343-20260705-0100000001', todo = '' }) {
  return [type, name, url, rating, ts, title, body, flag, order, todo].map((v) => `"${String(v).replace(/"/g, '""')}"`).join(',');
}
const csvOf = (rows) => cp932([HEADER, ...rows].join('\r\n') + '\r\n');

console.log('=== 1. prepareReviewFile: パースとガード ===');
{
  const buf = csvOf([
    row({ url: itemUrl('709q-aaaa-bbbb', 10000355, 4187768790), rating: 4, ts: '2026/7/16 12:12:13' }),
    row({ type: 'ショップレビュー', name: '', url: shopUrl('709q-aaaa-bbbb'), rating: 5, order: '373343-20260705-0100000001' }),
  ]);
  const p = prepareReviewFile('reviews_a.csv', buf);
  check('正常CSVがパースできる', p.ok, p.error);
  check('2件のレコード', p.ok && p.records.length === 2);
  const item = p.records.find((r) => r.review_type === 'item');
  const shop = p.records.find((r) => r.review_type === 'shop');
  check('item_id がURLから抽出される', item?.item_id === 10000355, `got ${item?.item_id}`);
  check('shop レビューは item_id null', shop?.item_id === null);
  check('投稿時間が正規化される (ゼロ埋め)', item?.posted_at === '2026-07-16 12:12:13' && item?.date_jst === '2026-07-16');
  check('date range', p.dateFrom === '2026-07-10' && p.dateTo === '2026-07-16', `${p.dateFrom}〜${p.dateTo}`);
}
{
  const p = prepareReviewFile('x.csv', cp932('"別のヘッダ","列"\r\n"a","b"\r\n'));
  check('ヘッダ不一致は拒否', !p.ok && /ヘッダ不一致/.test(p.error));
}
{
  const u = itemUrl('709q-dup1-dup1', 111, 222);
  const p = prepareReviewFile('dup.csv', csvOf([row({ url: u }), row({ url: u })]));
  check('完全同一行の重複はマージ+警告', p.ok && p.records.length === 1 && p.warnings.length === 1, p.error);
  const p2 = prepareReviewFile('dup2.csv', csvOf([row({ url: u, rating: 5 }), row({ url: u, rating: 4 })]));
  check('同一URLで内容が違えばエラー', !p2.ok && /内容が異なる/.test(p2.error));
}
{
  const p = prepareReviewFile('bad.csv', csvOf([row({ url: itemUrl('t', 1, 2), rating: 9 })]));
  check('評価が1-5外は拒否', !p.ok && /評価が不正/.test(p.error));
  const p2 = prepareReviewFile('bad2.csv', csvOf([row({ url: itemUrl('t', 1, 2), ts: '2026/02/30 1:00:00' })]));
  check('実在しない日付は拒否', !p2.ok && /投稿時間が不正/.test(p2.error));
  const p3 = prepareReviewFile('bad3.csv', csvOf([row({ url: 'https://evil.example.com/x' })]));
  check('review.rakuten.co.jp 以外のURLは拒否', !p3.ok && /URLが不正/.test(p3.error));
  const p4 = prepareReviewFile('bad4.csv', csvOf([row({ url: 'https://review.rakuten.co.jp/other/format/x/' })]));
  check('item_id が抽出できない商品レビューURLは拒否 (mirror contract整合)', !p4.ok && /item_id を抽出できない/.test(p4.error));
}

console.log('=== 2. importReviewFile: UPSERT / revision / 冪等 / 低評価検知 ===');
const urlA = itemUrl('709q-imp1-aaaa', 10000355, 1001);
const urlLow = itemUrl('709q-imp1-bbbb', 10000016, 1002);
{
  const buf = csvOf([
    row({ url: urlA, rating: 5, body: '最初の本文', ts: '2026/7/10 9:00:00' }),
    row({ url: urlLow, rating: 2, name: '肉球クリーム 30g', body: 'いまいちでした', ts: '2026/7/11 10:00:00', order: '373343-20260711-0200000002' }),
  ]);
  const out = importReviewFile(db, { name: 'imp1.csv', buffer: buf, sha256: sha(buf) });
  check('取込成功', out.status === 'ok', JSON.stringify(out.results));
  check('insert=2', out.results[0].inserted === 2);
  check('新規★2以下が検知される', out.newLowRatings.length === 1 && out.newLowRatings[0].rating === 2);
  check('低評価通知に本文・注文番号が含まれない',
    !JSON.stringify(out.newLowRatings).includes('いまいち') && !JSON.stringify(out.newLowRatings).includes('373343-2026'));
  const q = db.prepare(`SELECT COUNT(*) c FROM rakuten_review_low_notify_queue`).get().c;
  check('通知キューに取込tx内で積まれる', q === 1, `queue=${q}`);
  const qCols = db.prepare(`PRAGMA table_info(rakuten_review_low_notify_queue)`).all().map((c) => c.name);
  check('通知キューに本文・注文番号列が無い', !qCols.includes('body') && !qCols.includes('order_number'));
  const cnt = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_reviews`).get().c;
  const rev = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_review_revisions`).get().c;
  check('fact 2行 / revision 2行 (初回)', cnt === 2 && rev === 2, `fact=${cnt} rev=${rev}`);

  // 同一ファイル再投入 = duplicate スキップ
  const out2 = importReviewFile(db, { name: 'imp1.csv', buffer: buf, sha256: sha(buf) });
  check('同一sha256はduplicateスキップ', out2.status === 'duplicate');
}
{
  // 内容変化なしの再取得 (別ファイル): last_seen のみ更新、revision 増えない、低評価も再通知しない
  const buf = csvOf([
    row({ url: urlLow, rating: 2, name: '肉球クリーム 30g', body: 'いまいちでした', ts: '2026/7/11 10:00:00', order: '373343-20260711-0200000002' }),
  ]);
  const before = db.prepare(`SELECT last_seen_at FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlLow).last_seen_at;
  const out = importReviewFile(db, { name: 'imp2.csv', buffer: buf, sha256: sha(buf) });
  check('変化なし取込は ok / unchanged=1', out.status === 'ok' && out.results[0].unchanged === 1);
  check('既存★2は再通知しない', out.newLowRatings.length === 0);
  const rev = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_review_revisions WHERE review_url = ?`).get(urlLow).c;
  check('revision は増えない', rev === 1, `rev=${rev}`);
  const after = db.prepare(`SELECT last_seen_at FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlLow).last_seen_at;
  check('last_seen_at が更新される', after >= before);
}
{
  // 編集検知: 同URLで本文・評価が変わる → UPDATE + revision append (低評価化しても新規扱いにはしない)
  const buf = csvOf([
    row({ url: urlA, rating: 1, body: '編集後の本文 (低評価に変更)', ts: '2026/7/10 9:00:00' }),
  ]);
  const out = importReviewFile(db, { name: 'imp3.csv', buffer: buf, sha256: sha(buf) });
  check('編集は update=1', out.status === 'ok' && out.results[0].updated === 1, JSON.stringify(out.results));
  check('編集での低評価化は「新規」通知に含めない (Codex: 編集で再付与しない と整合)', out.newLowRatings.length === 0);
  const cur = db.prepare(`SELECT rating, body FROM fact_rakuten_reviews WHERE review_url = ?`).get(urlA);
  check('current が最新状態', cur.rating === 1 && cur.body.includes('編集後'));
  const rev = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_review_revisions WHERE review_url = ?`).get(urlA).c;
  check('revision が2行 (初回+編集)', rev === 2, `rev=${rev}`);
  const revCols = db.prepare(`PRAGMA table_info(fact_rakuten_review_revisions)`).all().map((c) => c.name);
  check('revision 表に本文・タイトル列が無い (PII最小化)', !revCols.includes('body') && !revCols.includes('title'));
}
{
  // 壊れた行を含むファイルは1行もDBに入らない (1ファイル=1tx)
  const okUrl = itemUrl('709q-tx-okok', 999, 3001);
  const buf = csvOf([
    row({ url: okUrl }),
    row({ url: itemUrl('709q-tx-ngng', 998, 3002), rating: 7 }),
  ]);
  const out = importReviewFile(db, { name: 'imp4.csv', buffer: buf, sha256: sha(buf) });
  check('不正行を含むファイルは error', out.status === 'error');
  const cnt = db.prepare(`SELECT COUNT(*) c FROM fact_rakuten_reviews WHERE review_url = ?`).get(okUrl).c;
  check('正常行も入らない (原子性)', cnt === 0);
}

console.log('=== 3. 集計projection (sync-rakuten-review-daily の SELECT と同型) ===');
{
  const rows = db.prepare(`
    SELECT date_jst, review_type, COALESCE(item_id, 0) AS item_id,
           CASE WHEN review_type = 'item' THEN MAX(item_name) ELSE NULL END AS item_name,
           rating, COUNT(*) AS review_count
    FROM fact_rakuten_reviews
    WHERE is_deleted = 0 AND date_jst >= '2026-07-01' AND date_jst <= '2026-07-31'
    GROUP BY date_jst, review_type, COALESCE(item_id, 0), rating
    ORDER BY date_jst, review_type, item_id, rating
  `).all();
  check('集計行が返る', rows.length >= 2, `rows=${rows.length}`);
  const cols = new Set(rows.flatMap((r) => Object.keys(r)));
  check('projection に本文/注文番号/URL列が無い (非PII)',
    !cols.has('body') && !cols.has('order_number') && !cols.has('review_url'));
  const low = rows.find((r) => r.item_id === 10000016 && r.rating === 2);
  check('★2 の集計が正しい', low && low.review_count === 1, JSON.stringify(low));
  // urlA は編集で★1になった → ★1 に計上され★5にはいない
  const edited = rows.find((r) => r.item_id === 10000355);
  check('編集後の★で集計される', edited && edited.rating === 1, JSON.stringify(edited));
}

console.log('=== 4. 取込ログ ===');
{
  const logs = db.prepare(`SELECT status, COUNT(*) c FROM raw_rakuten_review_import_log GROUP BY status ORDER BY status`).all();
  const m = Object.fromEntries(logs.map((l) => [l.status, l.c]));
  check('取込ログが揃う (ok/duplicate/error)', m.ok === 3 && m.duplicate === 1 && m.error === 1, JSON.stringify(m));
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== ${passed} passed / ${failed} failed ===`);
process.exit(failed > 0 ? 1 : 0);
