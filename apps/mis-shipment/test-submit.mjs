/**
 * 誤出荷の新規登録 (POST /api/submissions) を、本物の router + 本物の DDL で通して確かめる。
 *
 *   node apps/mis-shipment/test-submit.mjs
 *
 * miniPC の注文検索は、このファイルの中で立てる偽の lookup サーバで置き換える。
 * 偽サーバは miniPC の orders-lookup-router.js と同じ形 (mall = shops.platform の生の値) を返す。
 *
 * 見ているもの:
 *   - Amazon の注文 (NE の店舗 4 = platform 'amazon_fbm') が登録できること
 *     (2026-10-06 まで、DB の CHECK で落ちて応答が返らず「登録中…」のまま固まっていた)
 *   - DB の選択肢に無い NE の店舗 (ヤフオク・卸 など) は「その他」で登録されること
 *   - 登録で思わぬ失敗をしても、応答が返ること (固まらないこと)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';

// 本番の server.js と同じ扱い (ログに出すだけで落とさない)。
// これが無いと、ハンドラの中の例外でテストごと落ちて「固まる」が再現しない。
process.on('unhandledRejection', (reason) => {
  console.log(`  (unhandledRejection: ${String(reason && reason.message || reason).split('\n')[0]})`);
});

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mis-submit-test-'));
process.env.DATA_DIR = dataDir;
process.env.WAREHOUSE_LOOKUP_TOKEN = 'test-token';

let failures = 0;
function check(label, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    console.log(`  ✅ ${label}: ${a}`);
  } else {
    failures++;
    console.log(`  ❌ ${label}: ${a}  (期待: ${e})`);
  }
}

// ── 偽の miniPC lookup ──────────────────────────────────────
// 注文番号 → shops.platform の生の値
const PLATFORM_OF = {
  '503-0000000-0000001': 'amazon_fbm',
  '373343-20261006-0000001': 'rakuten',
  'yauc-1': 'yahoo_auction',
  'whole-1': 'wholesale',
};
const lookupServer = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const id = u.searchParams.get('order_id');
  res.setHeader('Content-Type', 'application/json');
  const platform = PLATFORM_OF[id];
  if (!platform) return res.end(JSON.stringify({ found: false }));
  res.end(JSON.stringify({
    found: true, mall: platform, mall_order_id: id, slip_no: '1550246', matched_by: 'order_no',
    sku: 'pr_test', product_name: 'テスト商品', ordered_qty: 1, order_date: '2026-10-05', line_count: 1,
  }));
});
await new Promise((r) => lookupServer.listen(0, '127.0.0.1', r));
process.env.WAREHOUSE_URL = `http://127.0.0.1:${lookupServer.address().port}`;

// ── 本物の router を立てる (env を決めてから import する) ─────────
const { default: express } = await import('express');
const { initMirrorDB, getMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();
const { default: router } = await import('./router.js');

const app = express();
app.use((req, _res, next) => { req.session = { email: 'staff@example.com', role: 'user' }; next(); });
app.use('/apps/mis-shipment', express.json(), router);
const appServer = await new Promise((r) => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
const BASE = `http://127.0.0.1:${appServer.address().port}/apps/mis-shipment/api`;

function record(orderId, extra = {}) {
  return {
    client_submission_id: crypto.randomUUID(),
    occurred_on: '2026-10-06',
    mall_order_id: orderId,
    order_id_unknown: false,
    manual_mall: null,
    mis_type: 'wrong_item',
    qty_affected: 1,
    loss_amount_jpy: 0,
    process_stage: 'packing',
    reporter_note: null,
    ...extra,
  };
}

/** 固まったら「固まった」と分かるように、3 秒で打ち切る。 */
async function post(payload) {
  try {
    const res = await fetch(BASE + '/submissions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(3000),
    });
    return { status: res.status, data: await res.json().catch(() => null) };
  } catch (e) {
    return { status: e.name === 'TimeoutError' ? 'HANG' : 'ERR:' + e.message, data: null };
  }
}
function mallOf(id) {
  return getMirrorDB().prepare('SELECT mall FROM f_mis_shipments WHERE id = ?').get(id)?.mall ?? null;
}

console.log('Amazon の注文 (platform amazon_fbm)');
let r = await post({ mix_up: false, records: [record('503-0000000-0000001')] });
check('応答', r.status, 201);
check('保存されたモール', r.data && mallOf(r.data.id), 'amazon');

console.log('楽天の注文 (いままでも通っていたもの)');
r = await post({ mix_up: false, records: [record('373343-20261006-0000001')] });
check('応答', r.status, 201);
check('保存されたモール', r.data && mallOf(r.data.id), 'rakuten');

console.log('DB の選択肢に無い NE の店舗 (ヤフオク)');
r = await post({ mix_up: false, records: [record('yauc-1')] });
check('応答', r.status, 201);
check('保存されたモール', r.data && mallOf(r.data.id), 'other');

console.log('テレコ: Amazon + 卸');
r = await post({ mix_up: true, records: [record('503-0000000-0000001', { mis_type: 'mix_up' }), record('whole-1', { mis_type: 'mix_up' })] });
check('応答', r.status, 201);
check('保存されたモール', r.data && r.data.ids && r.data.ids.map(mallOf), ['amazon', 'other']);

console.log('登録で思わぬ失敗をしても固まらない (DB の CHECK に当たる長い SKU)');
PLATFORM_OF['long-sku-1'] = 'rakuten';
const origEnd = lookupServer.listeners('request')[0];
lookupServer.removeAllListeners('request');
lookupServer.on('request', (req, res) => {
  const id = new URL(req.url, 'http://x').searchParams.get('order_id');
  if (id !== 'long-sku-1') return origEnd(req, res);
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ found: true, mall: 'rakuten', sku: 'x'.repeat(101), product_name: 'p', ordered_qty: 1, order_date: '2026-10-05' }));
});
r = await post({ mix_up: false, records: [record('long-sku-1')] });
check('応答 (固まらずエラーが返る)', r.status, 500);
check('エラーの中身', r.data, { error: 'server_error' });

appServer.close();
lookupServer.close();
try { getMirrorDB().close(); } catch (_) { /* 閉じられなくても結果には関係ない */ }
fs.rmSync(dataDir, { recursive: true, force: true });

console.log(failures === 0 ? '\n全部通りました' : `\n${failures} 件失敗`);
process.exitCode = failures === 0 ? 0 : 1;
