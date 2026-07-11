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
  const r = importYahooFile(db, { name: 'yahoo_item_2026-07-09_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
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
  const r = importYahooFile(db, { name: 'yahoo_keyword_2026-07-09_x.json', buffer: buf, sha256: sha(buf), source: 'test' });
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
  const r = importYahooFile(db, { name: 'yahoo_keyword_2026-07-09_v2.json', buffer: buf, sha256: sha(buf), source: 'test' });
  check('再取込ok', r.status === 'ok');
  check('updatedカウント', r.results[0].updated === 1 && r.results[0].inserted === 0, JSON.stringify(r.results[0]));
  const row = db.prepare(`SELECT * FROM fact_yahoo_keyword_daily WHERE date_jst='2026-07-09' AND keyword='ハッカ油'`).get();
  check('値が上書き', row.inflow === 99 && row.sales_yen === 9999);
  const other = db.prepare(`SELECT * FROM fact_yahoo_keyword_daily WHERE keyword='アロマ'`).get();
  check('他行は残る (UPSERTなので集合置換でない)', !!other);
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
