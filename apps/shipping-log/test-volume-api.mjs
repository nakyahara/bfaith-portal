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

console.log('\n── 期間・不正入力 ──');
{
  const r = await get('/api/volume?from=2026-08-05&to=2026-08-04');
  eq([r.json.from, r.json.to], ['2026-08-04', '2026-08-05'], 'from > to は入れ替えて解釈する');
  const r2 = await get('/api/volume?from=abc&to=2026-08-05');
  eq(r2.status, 200, '不正な日付でも 500 にしない (既定期間で返す)');
  const r3 = await get("/api/volume?from=2026-08-04&to=2026-08-05&mall=4') OR 1=1--");
  eq(r3.json.total, 0, 'SQL を仕込んだ mall 値は単に一致なし (プレースホルダ)');
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
