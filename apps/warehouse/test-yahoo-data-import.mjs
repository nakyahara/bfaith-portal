#!/usr/bin/env node
/**
 * test-yahoo-data-import.mjs — Yahoo!統計CSV/JSON取込のスモークテスト (mall-csv-fetcher P1-Y)
 *
 * 実DL物 (2026-07-11 取得) の実測ヘッダ/JSON構造を再現した合成フィクスチャで、
 * 判別〜ガード〜UPSERT〜冪等〜原子性を検証する。DBはtemp (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-yahoo-data-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import iconv from 'iconv-lite';
import { ensureYahooDataTables, importYahooFile, prepareYahooFile } from './yahoo-data-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ydata-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureYahooDataTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const sjis = (s) => iconv.encode(s, 'Shift_JIS');
const jsonBuf = (o) => Buffer.from(JSON.stringify(o), 'utf8');
const CRLF = String.fromCharCode(13, 10);

console.log('=== 1. 全体分析 (日次×デバイス → 縦持ち) ===');
{
  const header = ['日付', 'PC売上合計値', 'スマホ売上合計値', 'アプリ売上合計値', '合算値売上合計値',
    'PCページビュー', 'スマホページビュー', 'アプリページビュー', '合算値ページビュー'];
  const csv = sjis([header.join(','),
    '2026-07-10,30378,38853,331273,400504,243,269,2802,3314',
    '2026-07-09,20194,11714,96492,128400,203,263,1740,2206'].join('\r\n'));
  const r = importYahooFile(db, { name: 'yahoo_overall_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_yahoo_store_device_daily ORDER BY date_jst, device`).all();
  check('2日×4デバイス=8行', rows.length === 8, `got ${rows.length}`);
  const a = rows.find(x => x.date_jst === '2026-07-10' && x.device === 'app');
  check('アプリ売上が縦持ちに', a?.sales_yen === 331273 && a?.pageviews === 2802, JSON.stringify(a));
  const all = rows.find(x => x.date_jst === '2026-07-10' && x.device === 'all');
  check('合算値=all', all?.sales_yen === 400504);
}

console.log('=== 2. 流入・離脱 + 冪等 ===');
{
  const csv = sjis(['日付,流入全体（訪問者数）_合算値,自ストア購入（訪問者数）_合算値,自ストア購入（構成比）_合算値,離脱全体（訪問者数）_合算値,離脱全体（構成比）_合算値',
    '2026/07/10,1544,176,10.804,1453,89.196'].join('\r\n'));
  const r1 = importYahooFile(db, { name: 'yahoo_inflow_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r1.status === 'ok');
  const row = db.prepare(`SELECT * FROM fact_yahoo_inflow_daily WHERE date_jst='2026-07-10'`).get();
  check('率はREAL保持', row?.purchase_ratio_pct === 10.804 && row?.inflow_visitors === 1544);
  const r2 = importYahooFile(db, { name: 'again.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('同一sha256は duplicate', r2.status === 'duplicate', r2.status);
}

console.log('=== 3. お客様分析 (属性マトリクス → 縦持ち) ===');
{
  const cols = ['日付'];
  for (const g of ['全体', '男性', '女性']) {
    for (const age of ['全体', '20代', '30代']) {
      for (const cls of ['新規', 'リピーター（ライト）', 'リピーター（ヘビー）', '合計']) {
        cols.push(`性別（${g}）-${age}-${cls}`);
      }
    }
  }
  const vals = cols.slice(1).map((_, i) => String(i + 1));
  const csv = sjis([cols.join(','), ['20260710', ...vals].join(',')].join('\r\n'));
  const r = importYahooFile(db, { name: 'yahoo_user_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const n = db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_user_attr_daily`).get().n;
  check('36属性が縦持ち行に', n === 36, `got ${n}`);
  const one = db.prepare(`SELECT visitors FROM fact_yahoo_user_attr_daily WHERE date_jst='2026-07-10' AND gender='男性' AND age_band='30代' AND buyer_class='新規'`).get();
  check('属性パースの列対応', one?.visitors === 21, `got ${one?.visitors}`);
  check('日付YYYYMMDDを正規化', !!db.prepare(`SELECT 1 FROM fact_yahoo_user_attr_daily WHERE date_jst='2026-07-10'`).get());
}

console.log('=== 4. 速報 (時間帯×デバイス、日付forward-fill) ===');
{
  const metrics = ['売上合計値（税込）', '注文数合計', '注文点数合計', '注文者数合計', '平均購買率', '平均客単価', 'ページビュー', '訪問者数', '平均回遊ページ数'];
  const devs = ['合算値', 'PC', 'スマホ', 'アプリ'];
  const header = ['日付', '時間帯', ...metrics.flatMap(m => devs.map(d => `${m}（${d}）`))];
  const mk = (date, hour, base) => [date, hour, ...metrics.flatMap(() => devs.map((_, i) => String(base + i)))];
  const csv = sjis([header.map(h => `"${h}"`).join(','),
    mk('2026/07/11', '18:00 ～ 18:59', 10).map(v => `"${v}"`).join(','),
    mk('', '17:00 ～ 17:59', 20).map(v => `"${v}"`).join(',')].join('\r\n'));
  const r = importYahooFile(db, { name: 'yahoo_flash_x.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_yahoo_flash_hourly ORDER BY hour_slot, device`).all();
  check('2時間帯×4デバイス=8行', rows.length === 8, `got ${rows.length}`);
  const ff = db.prepare(`SELECT COUNT(*) n FROM fact_yahoo_flash_hourly WHERE date_jst='2026-07-11'`).get().n;
  check('日付空欄はforward-fill', ff === 8, `got ${ff}`);
  const h = rows.find(x => x.hour_slot === '17:00～17:59' && x.device === 'all');
  check('時間帯の空白除去+値', h?.sales_yen === 20, JSON.stringify(h));
}

console.log('=== 5. 商品分析JSON (meta.since=日付、sub=code=total正規化) ===');
{
  const j = {
    meta: { since: '20260709', until: '20260709' },
    results: [
      { name: '商品A', code: 'ITEM-A', subCode: 'sub=code=total', gmv: '7335', orderCount: '2', orderQuantity: '5',
        orderer: '2', cvr: '11.1', pv: '33', uu: '18', excellentDeliveryPv: '26', notExcellentDeliveryPv: '7',
        favoriteCount: '848', cartCount: '20', categoryContributionRank: '1' },
      { name: '商品B', code: 'item-b', subCode: 'RED', gmv: '0', orderCount: '0', orderQuantity: '0',
        orderer: '0', cvr: '0.0', pv: '3', uu: '2', excellentDeliveryPv: '1', notExcellentDeliveryPv: '2',
        favoriteCount: '5', cartCount: '0', categoryContributionRank: '-1' },
    ],
  };
  const buf = jsonBuf(j);
  const r = importYahooFile(db, { name: 'yahoo_item_d2026-07-09_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('JSON取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_yahoo_item_daily ORDER BY item_code`).all();
  check('2行', rows.length === 2, `got ${rows.length}`);
  const a = rows.find(x => x.item_code === 'item-a');
  check('date=meta.since', a?.date_jst === '2026-07-09');
  check('sub=code=total → 空文字', a?.sub_code === '');
  check('SKU正規化+raw保持', a?.raw_item_code === 'ITEM-A' && a?.item_code === 'item-a');
  check('値マッピング (gmv/uu/favorite)', a?.sales_yen === 7335 && a?.visitors === 18 && a?.favorites === 848);
  const b = rows.find(x => x.item_code === 'item-b');
  check('サブコードは小文字化して保持', b?.sub_code === 'red');
  // 複数日レンジは拒否
  const p = prepareYahooFile('x.json', jsonBuf({ meta: { since: '20260701', until: '20260710' }, results: [] }));
  check('複数日meta → error', !p.ok && /複数日/.test(p.error), p.error);
}

console.log('=== 6. 検索流入JSON (配列、日付はファイル名) ===');
{
  const arr = [
    { keyword: 'ハッカ油', pv: '24', pvRank: '1', gmv: '3060', orderCount: '2', orderQuantity: '2',
      orderRate: '8.3', orderUnitPrice: '1530', orderProductPrice: '1530' },
    { keyword: 'アロマ', pv: '8', pvRank: '2', gmv: '0', orderCount: '0', orderQuantity: '0',
      orderRate: '0.0', orderUnitPrice: '-', orderProductPrice: '-' },
  ];
  const buf = jsonBuf(arr);
  const r = importYahooFile(db, { name: 'yahoo_keyword_d2026-07-09_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('JSON取込ok', r.status === 'ok', JSON.stringify(r.results));
  const rows = db.prepare(`SELECT * FROM fact_yahoo_keyword_daily ORDER BY rank`).all();
  check('2語', rows.length === 2, `got ${rows.length}`);
  check('date=ファイル名', rows[0].date_jst === '2026-07-09');
  check('KW値マッピング', rows[0].keyword === 'ハッカ油' && rows[0].inflow === 24 && rows[0].sales_yen === 3060);
  check('"-" は NULL 化', rows[1].avg_order_aov_yen === null, JSON.stringify(rows[1]));
  // ファイル名に日付が無ければエラー (黙って当日扱いしない)
  const p = prepareYahooFile('yahoo_keyword_nodate.json', buf);
  check('ファイル名に日付なし → error', !p.ok && /ファイル名から対象日/.test(p.error), p.error);
}

console.log('=== 7. 種別判別の失敗ケース ===');
{
  const p1 = prepareYahooFile('x.csv', sjis('あ,い,う\n1,2,3'));
  check('不明CSV → 判別不能', !p1.ok && /判別できません/.test(p1.error), p1.error);
  const p2 = prepareYahooFile('x.json', Buffer.from('{"foo":1}', 'utf8'));
  check('不明JSON → 判別不能', !p2.ok && /判別できません/.test(p2.error), p2.error);
  const p3 = prepareYahooFile('x.json', Buffer.from('{broken', 'utf8'));
  check('壊れJSON → パース失敗', !p3.ok && /JSONパース失敗/.test(p3.error), p3.error);
}

console.log('=== 8. 再取込 (値変動) = UPSERT 上書き ===');
{
  const arr = [{ keyword: 'ハッカ油', pv: '99', pvRank: '1', gmv: '9999', orderCount: '3', orderQuantity: '3',
    orderRate: '9.9', orderUnitPrice: '3333', orderProductPrice: '3333' }];
  const buf = jsonBuf(arr);
  const r = importYahooFile(db, { name: 'yahoo_keyword_d2026-07-09_v2.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('再取込ok', r.status === 'ok');
  check('updatedカウント', r.results[0].updated === 1 && r.results[0].inserted === 0, JSON.stringify(r.results[0]));
  const row = db.prepare(`SELECT * FROM fact_yahoo_keyword_daily WHERE date_jst='2026-07-09' AND keyword='ハッカ油'`).get();
  check('値が上書き', row.inflow === 99 && row.sales_yen === 9999);
  const other = db.prepare(`SELECT * FROM fact_yahoo_keyword_daily WHERE keyword='アロマ'`).get();
  check('他行は残る (UPSERTなので集合置換でない)', !!other);
}

console.log('=== 8b. 同一内容・別日のJSONは両方取り込める (Codex R1 High) ===');
{
  const arr = [{ keyword: '同一内容KW', pv: '5', pvRank: '1', gmv: '0', orderCount: '0', orderQuantity: '0',
    orderRate: '0.0', orderUnitPrice: '-', orderProductPrice: '-' }];
  const buf = jsonBuf(arr);
  const r1 = importYahooFile(db, { name: 'yahoo_keyword_d2026-06-01_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
  const r2 = importYahooFile(db, { name: 'yahoo_keyword_d2026-06-02_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('6/1 取込ok', r1.status === 'ok', r1.status);
  check('同一sha256でも別日なら取込ok (duplicate扱いしない)', r2.status === 'ok', r2.status);
  const days = db.prepare(`SELECT DISTINCT date_jst FROM fact_yahoo_keyword_daily WHERE keyword='同一内容KW' ORDER BY date_jst`).all().map(x => x.date_jst);
  check('2日分が保存される', JSON.stringify(days) === JSON.stringify(['2026-06-01', '2026-06-02']), JSON.stringify(days));
  const r3 = importYahooFile(db, { name: 'yahoo_keyword_d2026-06-01_again.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('同一sha256+同一日は duplicate', r3.status === 'duplicate', r3.status);
}

console.log('=== 8c. ファイル名の不正日付は拒否 (Codex R1 Low) ===');
{
  const buf = jsonBuf([{ keyword: 'x', pv: '1', pvRank: '1', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0', orderUnitPrice: '-', orderProductPrice: '-' }]);
  const p = prepareYahooFile('yahoo_keyword_d2026-99-99_x.json', buf);
  check('存在しない日付 → error', !p.ok && /ファイル名から対象日/.test(p.error), p.error);
}

console.log('=== 8d. 一括型は実行日が変わっても sha256 のみで冪等 (Codex R2 High) ===');
{
  const header = '日付,流入全体（訪問者数）_合算値,自ストア購入（訪問者数）_合算値,自ストア購入（構成比）_合算値,離脱全体（訪問者数）_合算値,離脱全体（構成比）_合算値';
  const csv = sjis(header + CRLF + '2026/05/01,100,10,10.0,90,90.0');
  const r1 = importYahooFile(db, { name: 'yahoo_inflow_20260711T010101.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('1回目ok', r1.status === 'ok', r1.status);
  // 翌日の実行 (ファイル名のタイムスタンプだけ違う) は同一内容なので duplicate であるべき
  const r2 = importYahooFile(db, { name: 'yahoo_inflow_20260712T020202.csv', buffer: csv, sha256: sha(csv), source: 'test' });
  check('翌日実行でも duplicate (タイムスタンプを日付と誤認しない)', r2.status === 'duplicate', r2.status);
}

console.log('=== 8e. 検索KW 表記ゆれ合算 (末尾全角空白) ===');
{
  // インセンス と インセンス　(末尾全角空白) は trim 後同一 → 従来 dupGuard が当日分を丸ごと弾いていた
  const arr = [
    { keyword: 'インセンス', pv: '3', pvRank: '18', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0.0', orderUnitPrice: '-', orderProductPrice: '-' },
    { keyword: 'インセンス　', pv: '2', pvRank: '39', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0.0', orderUnitPrice: '-', orderProductPrice: '-' },
  ];
  const p = prepareYahooFile('yahoo_keyword_d2026-06-10_x.json', jsonBuf(arr));
  check('表記ゆれ合算で ok:true', p.ok === true, JSON.stringify(p.error));
  check('1語に合算', p.records?.length === 1, `got ${p.records?.length}`);
  check('inflow=5(加算) rank=18(最小)', p.records?.[0].inflow === 5 && p.records?.[0].rank === 18, JSON.stringify(p.records?.[0]));
}

console.log('=== 8f. 3行合算 + 率再計算 (売上あり) ===');
{
  const arr = [
    { keyword: 'アロマ', pv: '10', pvRank: '5', gmv: '5000', orderCount: '2', orderQuantity: '3', orderRate: '20.0', orderUnitPrice: '2500', orderProductPrice: '1666' },
    { keyword: 'アロマ ', pv: '6', pvRank: '8', gmv: '3000', orderCount: '1', orderQuantity: '2', orderRate: '16.6', orderUnitPrice: '3000', orderProductPrice: '1500' },
    { keyword: 'アロマ　', pv: '4', pvRank: '3', gmv: '2000', orderCount: '1', orderQuantity: '1', orderRate: '25.0', orderUnitPrice: '2000', orderProductPrice: '2000' },
  ];
  const p = prepareYahooFile('yahoo_keyword_d2026-06-11_x.json', jsonBuf(arr));
  check('3行→1語合算', p.ok && p.records?.length === 1, JSON.stringify(p.error || p.records?.length));
  const k = p.records?.[0];
  // inflow=20 sales=10000 orders=4 units=6 rank=3 / rate=4/20=20.0 aov=10000/4=2500 uaov=10000/6=1667
  check('合算値+率再計算', k?.inflow === 20 && k?.sales_yen === 10000 && k?.orders === 4 && k?.units === 6 && k?.rank === 3
    && k?.avg_order_rate_pct === 20 && k?.avg_order_aov_yen === 2500 && k?.avg_units_aov_yen === 1667, JSON.stringify(k));
}

console.log('=== 8g. マスク値(2未満=null)混入合算 → 汚染メトリクスの率は null ===');
{
  const arr = [
    { keyword: 'マスクKW', pv: '2未満', pvRank: '10', gmv: '1000', orderCount: '1', orderQuantity: '1', orderRate: '', orderUnitPrice: '1000', orderProductPrice: '1000' },
    { keyword: 'マスクKW　', pv: '10', pvRank: '5', gmv: '2000', orderCount: '1', orderQuantity: '1', orderRate: '10.0', orderUnitPrice: '2000', orderProductPrice: '2000' },
  ];
  const p = prepareYahooFile('yahoo_keyword_d2026-06-12_x.json', jsonBuf(arr));
  check('合算 ok', p.ok && p.records?.length === 1, JSON.stringify(p.error));
  const k = p.records?.[0];
  // inflow=null+10=10(下限), orders=1+1=2, sales=3000。inflow汚染→rate=null。sales/ordersはクリーン→aov=3000/2=1500
  check('inflow下限=10 orders=2', k?.inflow === 10 && k?.orders === 2, JSON.stringify(k));
  check('inflow汚染で rate=null', k?.avg_order_rate_pct === null, JSON.stringify(k));
  check('クリーンな aov は算出', k?.avg_order_aov_yen === 1500, JSON.stringify(k));
}

console.log('=== 8h. 200字切詰の別KW衝突はファイル全体を拒否 (stale row/静かな破損を防ぐ) ===');
{
  const long1 = 'あ'.repeat(200) + 'X';
  const long2 = 'あ'.repeat(200) + 'Y';  // 先頭200字一致・201字目のみ差の別KW (切詰後は衝突)
  const arr = [
    { keyword: long1, pv: '5', pvRank: '1', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0', orderUnitPrice: '-', orderProductPrice: '-' },
    { keyword: long2, pv: '3', pvRank: '2', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0', orderUnitPrice: '-', orderProductPrice: '-' },
    { keyword: '正常KW', pv: '7', pvRank: '3', gmv: '0', orderCount: '0', orderQuantity: '0', orderRate: '0', orderUnitPrice: '-', orderProductPrice: '-' },
  ];
  const p = prepareYahooFile('yahoo_keyword_d2026-06-13_x.json', jsonBuf(arr));
  // 部分除外は UPSERT 再取込で DB に stale row を残すため、全体拒否で人が気づける形にする
  check('切詰衝突はファイル全体を ok:false で拒否', p.ok === false && /切詰|重複/.test(p.error || ''), JSON.stringify(p.error));
}

console.log('=== 9. 取込ログ ===');
{
  const logs = db.prepare(`SELECT status, COUNT(*) n FROM raw_yahoo_data_import_log GROUP BY status`).all();
  const m = Object.fromEntries(logs.map(x => [x.status, x.n]));
  check('ok/duplicate が記録', m.ok >= 6 && m.duplicate >= 1, JSON.stringify(m));
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== 結果: ${passed} passed / ${failed} failed ===`);
process.exitCode = failed > 0 ? 1 : 0;
