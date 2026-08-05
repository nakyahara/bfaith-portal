/**
 * test-volume-api.mjs — 出荷件数ダッシュボード (view-router) の smoke テスト
 *
 * 一時 DATA_DIR に mirror DB を作り、mirror_shipments_daily に既知データを入れて
 * /api/volume /api/options /api/volume.csv を実際に HTTP で叩く。
 *
 * 実行: node apps/shipping-log/test-volume-api.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import express from 'express';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipvol-api-test-'));
process.env.DATA_DIR = tmpDir;

const { initMirrorDB, getMirrorDB } = await import('../warehouse-mirror/db.js');
const viewRouter = (await import('./view-router.js')).default;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initMirrorDB();
const db = getMirrorDB();

// 8/4: Amazon AES 400 (うちキャンセル2) / Amazon ネコポス 7 / 楽天 ネコポス 240
// 8/5: Amazon AES 395 / 楽天 ネコポス 144
const rows = [
  ['2026-08-04', '4', '雑貨イズムAmazon店', 'amazon_fbm', '71', 'AES', 400, 2],
  ['2026-08-04', '4', '雑貨イズムAmazon店', 'amazon_fbm', '28', 'ヤマト(ネコポス)', 7, 0],
  ['2026-08-04', '1', '雑貨イズム楽天市場店', 'rakuten', '28', 'ヤマト(ネコポス)', 240, 0],
  ['2026-08-05', '4', '雑貨イズムAmazon店', 'amazon_fbm', '71', 'AES', 395, 0],
  ['2026-08-05', '1', '雑貨イズム楽天市場店', 'rakuten', '28', 'ヤマト(ネコポス)', 144, 0],
];
const ins = db.prepare(`INSERT INTO mirror_shipments_daily
  (ship_date, shop_code, shop_name, platform, delivery_id, delivery_name, slips, cancelled_slips, source_updated_at, synced_at)
  VALUES (?,?,?,?,?,?,?,?,?,?)`);
for (const r of rows) ins.run(...r, null, '2026-08-05T07:00:00Z');

const app = express();
app.use('/apps/shipping-log', viewRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/shipping-log`;
const get = async (p) => {
  const res = await fetch(base + p);
  const text = await res.text();
  return { status: res.status, text, json: (() => { try { return JSON.parse(text); } catch { return null; } })() };
};

console.log('\n── /api/options ──');
{
  const r = await get('/api/options');
  eq(r.status, 200, 'HTTP 200');
  eq(r.json.malls.map(m => m.shop_name), ['雑貨イズムAmazon店', '雑貨イズム楽天市場店'], 'モール選択肢は件数の多い順');
  eq(r.json.methods.map(m => m.delivery_name), ['AES', 'ヤマト(ネコポス)'], '配送方法選択肢');
  eq([r.json.min_date, r.json.max_date], ['2026-08-04', '2026-08-05'], 'データ範囲');
}

console.log('\n── /api/volume ──');
{
  const r = await get('/api/volume?from=2026-08-04&to=2026-08-05');
  eq(r.json.total, 1186, '合計 (400+7+240+395+144)');
  eq(r.json.basis, 'shipped', '既定は出荷確定ベース');
  eq(r.json.cancelled, 2, 'キャンセルは内数として別に返る');
  eq(r.json.days.map(d => d.total), [647, 539], '日別合計');
  eq(r.json.malls[0], { name: '雑貨イズムAmazon店', n: 802 }, 'モール別 最多は Amazon');
  eq(r.json.methods[0], { name: 'AES', n: 795 }, '配送方法別 最多は AES');
  eq(r.json.days[0].methods['AES'], 400, '日 × 配送方法 (8/4 AES)');
  eq(r.json.avgPerDay, 593, '1日平均');
}

console.log('\n── basis=valid (キャンセルを除く) ──');
{
  const r = await get('/api/volume?from=2026-08-04&to=2026-08-05&basis=valid');
  eq(r.json.total, 1184, 'キャンセル2件を引いた合計');
  eq(r.json.days[0].malls['雑貨イズムAmazon店'], 405, '8/4 Amazon = 400-2+7');
}

console.log('\n── 絞り込み (Amazon × AES = 中原さんの見たい軸) ──');
{
  const r = await get('/api/volume?from=2026-08-04&to=2026-08-05&mall=4&method=71');
  eq(r.json.total, 795, 'Amazon の AES だけ');
  eq(r.json.days.map(d => d.total), [400, 395], '日別 AES 件数');
  const r2 = await get('/api/volume?from=2026-08-04&to=2026-08-05&mall=4');
  eq(r2.json.total, 802, 'モールだけの絞り込み (AES 以外の Amazon も含む)');
}

console.log('\n── 月別 (granularity=month) ──');
{
  // 7月分を足して月またぎを作る: 7/31 楽天 ネコポス 100 (1日だけ)
  db.prepare(`INSERT INTO mirror_shipments_daily
    (ship_date, shop_code, shop_name, platform, delivery_id, delivery_name, slips, cancelled_slips, source_updated_at, synced_at)
    VALUES ('2026-07-31','1','雑貨イズム楽天市場店','rakuten','28','ヤマト(ネコポス)',100,0,NULL,'2026-08-05T07:00:00Z')`).run();

  const r = await get('/api/volume?from=2026-07-01&to=2026-08-05&granularity=month');
  eq(r.json.granularity, 'month', '粒度が month で返る');
  eq(r.json.days.map(d => d.date), ['2026-07', '2026-08'], 'バケットは YYYY-MM');
  eq(r.json.days.map(d => d.total), [100, 1186], '月ごとの合計');
  eq(r.json.days.map(d => d.work_days), [1, 2], '出荷があった日数 (稼働日)');
  eq(r.json.days.map(d => d.avg_per_day), [100, 593], '月ごとの1日平均 (稼働日で割る)');
  eq(r.json.days[1].malls['雑貨イズムAmazon店'], 802, '月 × モール');
  eq(r.json.days[1].methods['AES'], 795, '月 × 配送方法');
  eq(r.json.total, 1286, '期間合計');
  eq(r.json.avgPerBucket, 643, '月平均 (1286 / 2ヶ月)');
  eq(r.json.workDays, 3, '期間内で出荷があった日数');
  eq(r.json.avgPerDay, 429, '1日平均は稼働日3日で割る (暦日ではない)');

  const day = await get('/api/volume?from=2026-07-01&to=2026-08-05');
  eq(day.json.granularity, 'day', '既定は日別のまま');
  eq(day.json.total, 1286, '粒度を変えても合計は同じ');

  const csv = await get('/api/volume.csv?from=2026-07-01&to=2026-08-05&granularity=month');
  const lines = csv.text.trim().split('\r\n');
  eq(lines[0].replace('﻿', ''), '出荷月,モール,配送方法,件数,うちキャンセル,出荷があった日数,1日あたり平均', '月別CSVのヘッダ');
  ok(lines.some(l => l.startsWith('2026-07,雑貨イズム楽天市場店,ヤマト(ネコポス),100,0,1,100')), '月別CSVの行 (平均つき)');
  ok(lines.some(l => l.startsWith('2026-08,雑貨イズムAmazon店,AES,795,2,2,398')), '月にまたがる行が畳まれている');

  db.prepare("DELETE FROM mirror_shipments_daily WHERE ship_date='2026-07-31'").run();
}

console.log('\n── 期間・不正入力 ──');
{
  const r = await get('/api/volume?from=2026-08-05&to=2026-08-04');
  eq([r.json.from, r.json.to], ['2026-08-04', '2026-08-05'], 'from > to は入れ替えて解釈する');
  eq((await get('/api/volume?from=abc&to=2026-08-05')).status, 400, '日付の形式が不正 → 400');
  eq((await get('/api/volume?from=2026-99-99&to=2026-08-05')).status, 400, '存在しない日付 → 400');
  eq((await get('/api/volume?from=2026-02-31&to=2026-08-05')).status, 400, '2/31 → 400');
  eq((await get('/api/volume?from=2000-01-01&to=2026-08-05')).status, 400, '期間が長すぎる → 400');
  eq((await get('/api/volume')).status, 200, '未指定は既定期間 (直近30日)');
  const r3 = await get("/api/volume?from=2026-08-04&to=2026-08-05&mall=4') OR 1=1--");
  eq(r3.json.total, 0, 'SQL を仕込んだ mall 値は単に一致なし (プレースホルダ)');
}

console.log('\n── CSV の数式インジェクション対策 ──');
{
  // NE 由来の配送方法名に = で始まる値が入っても Excel が式として評価しないこと
  db.prepare(`INSERT INTO mirror_shipments_daily
    (ship_date, shop_code, shop_name, platform, delivery_id, delivery_name, slips, cancelled_slips, source_updated_at, synced_at)
    VALUES ('2026-08-06','1','=cmd|calc!A1','rakuten','99','@SUM(1+1)',3,0,NULL,'2026-08-06T07:00:00Z')`).run();
  const r = await get('/api/volume.csv?from=2026-08-06&to=2026-08-06');
  const line = r.text.trim().split('\r\n')[1];
  ok(line.includes("'=cmd|calc!A1") || line.includes('"\'=cmd|calc!A1"'), '先頭 = のセルは \' で無害化される');
  ok(line.includes("'@SUM(1+1)"), '先頭 @ のセルも無害化される');
  db.prepare("DELETE FROM mirror_shipments_daily WHERE ship_date='2026-08-06'").run();
}

console.log('\n── CSV ──');
{
  const r = await get('/api/volume.csv?from=2026-08-04&to=2026-08-05&mall=4&method=71');
  eq(r.status, 200, 'HTTP 200');
  // fetch の text() は BOM を落とすのでバイト列で確認する (Excel が UTF-8 と判定するのに必要)
  const raw = new Uint8Array(await (await fetch(base + '/api/volume.csv?from=2026-08-04&to=2026-08-05')).arrayBuffer());
  eq([raw[0], raw[1], raw[2]], [0xEF, 0xBB, 0xBF], 'UTF-8 BOM 付き (Excel で文字化けしない)');
  ok(r.text.trim().startsWith('出荷日,モール,配送方法,件数,うちキャンセル'), 'ヘッダ行');
  const lines = r.text.trim().split('\r\n');
  eq(lines.length, 3, '絞り込みが CSV にも効く (ヘッダ + 2日分)');
  ok(lines[1].includes('AES') && lines[1].includes('400'), '8/4 の AES 行');
}

// close を待たずに process.exit すると Windows の node が
// 「Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)」で異常終了コードを返す
await new Promise((r) => server.close(r));
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
