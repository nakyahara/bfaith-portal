/** Chrome拡張向けAPI (x-api-key認証) のHTTP経由テスト */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'es-ext-'));
process.env.DATA_DIR = tmp;
delete process.env.EASY_SHIP_EXT_TOKEN;
delete process.env.EASY_SHIP_SKU_CASE_INSENSITIVE;
delete process.env.EASY_SHIP_ALLOW_ORDER_ID_LOGGING;

const { default: express } = await import(pathToFileURL(path.join(repo, 'node_modules/express/index.js')));
const dbMod = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/db.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/service.js')));
const { default: extRouter } = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/ext-router.js')));

dbMod.initEasyShipDB();
const db = dbMod.getDB();
svc.createMaster(
  {
    sku: 'nanairosks-2new',
    packageSizeCode: 'SIZE_60',
    packageSizeLabel: '60サイズ (26 cm x 19 cm x 11 cm)',
    amazonOptionValue: '84797239-91e6-4101-a8b8-9b86c45482e7',
  },
  'test',
);

const app = express();
app.use('/apps/easy-ship/ext-api', extRouter);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/apps/easy-ship/ext-api`;

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) {
    pass++;
    console.log(`  ok - ${n}`);
  } else {
    fail++;
    console.log(`  NG - ${n}`);
  }
};

async function req(method, p, { key, body } = {}) {
  const res = await fetch(base + p, {
    method,
    headers: {
      connection: 'close', // keep-aliveソケットを残すとWindowsで終了時にlibuv assertになる
      ...(key ? { 'x-api-key': key } : {}),
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  return { status: res.status, json: await res.json().catch(() => null) };
}

// fail-closed: トークン未設定なら503
let r = await req('GET', '/api/v1/ping', { key: 'anything' });
ok(r.status === 503, 'EASY_SHIP_EXT_TOKEN未設定は503 (fail-closed)');

process.env.EASY_SHIP_EXT_TOKEN = 'test-token-1234567890';

r = await req('GET', '/api/v1/ping');
ok(r.status === 401, 'キー無しは401');
r = await req('GET', '/api/v1/ping', { key: 'wrong' });
ok(r.status === 401, '不正キーは401');
r = await req('GET', '/api/v1/ping', { key: 'test-token-1234567890' });
ok(r.status === 200 && r.json?.data?.pong === true, '正しいキーでping成功');

r = await req('GET', '/api/v1/package-sizes/nanairosks-2new', { key: 'test-token-1234567890' });
ok(
  r.status === 200 && r.json?.data?.amazonOptionValue === '84797239-91e6-4101-a8b8-9b86c45482e7',
  '単一SKU照会 (単体版と互換のレスポンス形)',
);
r = await req('GET', '/api/v1/package-sizes/nope-999', { key: 'test-token-1234567890' });
ok(r.status === 404 && r.json?.error?.code === 'SKU_NOT_FOUND', '未登録はSKU_NOT_FOUND');

r = await req('POST', '/api/v1/package-sizes/bulk-lookup', {
  key: 'test-token-1234567890',
  body: { skus: ['nanairosks-2new', 'nope-999'] },
});
ok(
  r.status === 200 && r.json?.data?.found?.length === 1 && r.json?.data?.notFound?.[0] === 'nope-999',
  'bulk-lookupが found/notFound を返す',
);
r = await req('POST', '/api/v1/package-sizes/bulk-lookup', { key: 'test-token-1234567890', body: { skus: [] } });
ok(r.status === 400 && r.json?.error?.code === 'VALIDATION_ERROR', 'skus空は400');

r = await req('POST', '/api/v1/logs', {
  key: 'test-token-1234567890',
  body: {
    entries: [
      {
        sku: 'nanairosks-2new',
        action: 'autofill',
        result: 'success',
        pageUrl: 'https://sellercentral.amazon.co.jp/easyship/bulkscheduling?x=1',
        orderId: '250-0000000-0000000',
      },
    ],
  },
});
ok(r.status === 200 && r.json?.data?.saved === 1, 'ログ保存が成功する');
const log = db.prepare("SELECT * FROM es_operation_logs WHERE action='autofill' ORDER BY id DESC").get();
ok(!String(log.message ?? '').includes('250-0000000'), '注文番号は既定で保存されない');
ok(log.page_url === 'https://sellercentral.amazon.co.jp/easyship/bulkscheduling', 'pageUrlはクエリ除去');

// Windowsで handle close 中の process.exit が libuv assert になるため、
// すべて閉じたうえで exitCode を設定して自然終了させる
await new Promise((resolve) => server.close(resolve));
try {
  db.close();
} catch {}
console.log(`\n${pass} pass / ${fail} fail`);
process.exitCode = fail ? 1 : 0;
