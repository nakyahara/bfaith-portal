/**
 * yahoo-publish-proxy.js callUploadItemImage / validateUploadFileName の unit test。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/yahoo-publish-proxy.test.mjs
 *
 * Phase E upload contract rewrite で旧 multipart 透過 forward から JSON 受け渡し +
 * VPS proxy 側 FormData 再構築 に変更したため、 bfaith-portal 側 contract の単体検証:
 *   - JSON body で POST してる (mock fetch で req.body を JSON.parse して確認)
 *   - fileName 規約 ({item_code}.jpg / {item_code}_N.jpg) + itemCode 完全結合
 *   - JPEG magic FF D8 + 2MB 未満の smoke gate
 *   - proxy 200 + <Result><Status>OK</Status><Id>xxx</Id></Result> で resolve
 *   - proxy 400 で YahooProxyError
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  callUploadItemImage,
  callUploadLibImage,
  validateUploadFileName,
  validateUploadLibFileName,
  YahooProxyError,
} from './yahoo-publish-proxy.js';

// 環境変数 stub
process.env.YAHOO_PROXY_BASE_URL = 'http://127.0.0.1:9999';
process.env.YAHOO_PROXY_SECRET = 'test-secret';
process.env.YAHOO_SELLER_ID = 'b-faith';

function jpegBuffer(size = 1024) {
  const buf = Buffer.alloc(size);
  buf[0] = 0xFF;
  buf[1] = 0xD8;
  buf[size - 2] = 0xFF;
  buf[size - 1] = 0xD9;
  return buf;
}

// ── validateUploadFileName ─────────────────────────────────────────
test('validateUploadFileName accepts main {itemCode}.jpg', () => {
  validateUploadFileName('aburatoishioil100.jpg', 'aburatoishioil100');
});

test('validateUploadFileName accepts sub {itemCode}_1.jpg .. _20.jpg', () => {
  for (const i of [1, 5, 10, 20]) {
    validateUploadFileName(`aburatoishioil100_${i}.jpg`, 'aburatoishioil100');
  }
});

test('validateUploadFileName rejects sub _21 (out of range)', () => {
  assert.throws(
    () => validateUploadFileName('aburatoishioil100_21.jpg', 'aburatoishioil100'),
    YahooProxyError,
  );
});

test('validateUploadFileName rejects _0 and _01 (zero padding / zero index)', () => {
  assert.throws(() => validateUploadFileName('abc_0.jpg', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadFileName('abc_01.jpg', 'abc'), YahooProxyError);
});

test('validateUploadFileName rejects non-jpg extension', () => {
  assert.throws(() => validateUploadFileName('abc.png', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadFileName('abc.jpeg', 'abc'), YahooProxyError);
});

test('validateUploadFileName rejects path separator and control chars', () => {
  assert.throws(() => validateUploadFileName('../abc.jpg', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadFileName('a/b.jpg', 'b'), YahooProxyError);
  assert.throws(() => validateUploadFileName('abc\n.jpg', 'abc'), YahooProxyError);
});

test('validateUploadFileName rejects fileName not matching itemCode (prefix abuse)', () => {
  // Codex R2 High-1: startsWith() の弱さを regex 完全結合で防ぐ
  assert.throws(() => validateUploadFileName('abcx.jpg', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadFileName('abcx_1.jpg', 'abc'), YahooProxyError);
});

test('validateUploadFileName rejects invalid itemCode chars', () => {
  assert.throws(() => validateUploadFileName('abc!.jpg', 'abc!'), YahooProxyError);
  assert.throws(() => validateUploadFileName('a b.jpg', 'a b'), YahooProxyError);
});

// ── callUploadItemImage : JSON body + 200 OK 経路 ──────────────────
test('callUploadItemImage sends JSON {fileName,itemCode,bufferBase64} and parses <Result><Id>', async () => {
  const buf = jpegBuffer(2048);
  const captured = { url: null, method: null, body: null, headers: null };
  const origFetch = global.fetch;
  global.fetch = async (url, opts) => {
    captured.url = url;
    captured.method = opts.method;
    captured.headers = opts.headers;
    captured.body = opts.body;
    return new Response(
      '<?xml version="1.0"?><ResultSet><Result><Status>OK</Status><Id>aburatoishioil100.jpg</Id></Result></ResultSet>',
      { status: 200, headers: { 'Content-Type': 'application/xml' } },
    );
  };
  try {
    await callUploadItemImage({ buffer: buf, fileName: 'aburatoishioil100.jpg', itemCode: 'aburatoishioil100' });
  } finally {
    global.fetch = origFetch;
  }
  assert.equal(captured.url, 'http://127.0.0.1:9999/yahoo/uploadItemImage');
  assert.equal(captured.method, 'POST');
  assert.equal(captured.headers['Content-Type'], 'application/json');
  assert.equal(captured.headers['X-Proxy-Secret'], 'test-secret');
  const json = JSON.parse(captured.body);
  assert.equal(json.fileName, 'aburatoishioil100.jpg');
  assert.equal(json.itemCode, 'aburatoishioil100');
  assert.equal(typeof json.bufferBase64, 'string');
  // base64 decode → 元 buffer 一致
  assert.deepEqual(Buffer.from(json.bufferBase64, 'base64'), buf);
});

// ── callUploadItemImage : gate ─────────────────────────────────────
test('callUploadItemImage rejects non-Buffer input', async () => {
  await assert.rejects(
    callUploadItemImage({ buffer: 'not buffer', fileName: 'abc.jpg', itemCode: 'abc' }),
    (e) => e instanceof YahooProxyError && /buffer/.test(e.message),
  );
});

test('callUploadItemImage rejects invalid JPEG magic', async () => {
  const bad = Buffer.alloc(10); // FF D8 なし
  await assert.rejects(
    callUploadItemImage({ buffer: bad, fileName: 'abc.jpg', itemCode: 'abc' }),
    (e) => e instanceof YahooProxyError && /JPEG magic/.test(e.message),
  );
});

test('callUploadItemImage rejects image >= 2MB', async () => {
  const huge = Buffer.alloc(2 * 1024 * 1024);
  huge[0] = 0xFF;
  huge[1] = 0xD8;
  await assert.rejects(
    callUploadItemImage({ buffer: huge, fileName: 'abc.jpg', itemCode: 'abc' }),
    (e) => e instanceof YahooProxyError && /too large/.test(e.message),
  );
});

test('callUploadItemImage rejects bad fileName (prefix abuse)', async () => {
  const buf = jpegBuffer(1024);
  await assert.rejects(
    callUploadItemImage({ buffer: buf, fileName: 'abcx.jpg', itemCode: 'abc' }),
    (e) => e instanceof YahooProxyError && /does not match itemCode/.test(e.message),
  );
});

// ── callUploadItemImage : proxy 400 ────────────────────────────────
test('callUploadItemImage throws YahooProxyError on proxy HTTP 400', async () => {
  const buf = jpegBuffer(1024);
  const origFetch = global.fetch;
  global.fetch = async () => new Response('{"error":"bad fileName"}', { status: 400 });
  try {
    await assert.rejects(
      callUploadItemImage({ buffer: buf, fileName: 'aburatoishioil100.jpg', itemCode: 'aburatoishioil100' }),
      (e) => e instanceof YahooProxyError && e.status === 400,
    );
  } finally {
    global.fetch = origFetch;
  }
});

// ── validateUploadLibFileName ──────────────────────────────────────
test('validateUploadLibFileName: accepts {itemCode}_lib_N.jpg (N=1..20)', () => {
  for (const n of [1, 5, 10, 20]) {
    validateUploadLibFileName(`aburatoishioil100_lib_${n}.jpg`, 'aburatoishioil100');
  }
});

test('validateUploadLibFileName: rejects N=0 / 21', () => {
  assert.throws(() => validateUploadLibFileName('aburatoishioil100_lib_0.jpg', 'aburatoishioil100'), YahooProxyError);
  assert.throws(() => validateUploadLibFileName('aburatoishioil100_lib_21.jpg', 'aburatoishioil100'), YahooProxyError);
});

test('validateUploadLibFileName: rejects format mismatch', () => {
  assert.throws(() => validateUploadLibFileName('abc.jpg', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadLibFileName('abc_1.jpg', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadLibFileName('abc_lib_1.png', 'abc'), YahooProxyError);
  assert.throws(() => validateUploadLibFileName('../abc_lib_1.jpg', 'abc'), YahooProxyError);
});

test('validateUploadLibFileName: rejects itemCode mismatch', () => {
  assert.throws(() => validateUploadLibFileName('abc_lib_1.jpg', 'def'), YahooProxyError);
});

// ── callUploadLibImage ────────────────────────────────────────────
test('callUploadLibImage: sends JSON {fileName,itemCode,bufferBase64} and parses proxy JSON', async () => {
  const buf = jpegBuffer(2048);
  const captured = { url: null, body: null, headers: null };
  const origFetch = global.fetch;
  global.fetch = async (url, opts) => {
    captured.url = url;
    captured.body = opts.body;
    captured.headers = opts.headers;
    return new Response(
      JSON.stringify({ ok: true, yahooUrl: 'https://shopping.c.yimg.jp/lib/b-faith/aburatoishioil100_lib_1.jpg', body: '<ResultSet><Status>OK</Status></ResultSet>' }),
      { status: 200, headers: { 'Content-Type': 'application/json' } },
    );
  };
  try {
    const r = await callUploadLibImage({ buffer: buf, fileName: 'aburatoishioil100_lib_1.jpg', itemCode: 'aburatoishioil100' });
    assert.equal(r.yahooUrl, 'https://shopping.c.yimg.jp/lib/b-faith/aburatoishioil100_lib_1.jpg');
  } finally {
    global.fetch = origFetch;
  }
  assert.equal(captured.url, 'http://127.0.0.1:9999/yahoo/uploadLibImage');
  assert.equal(captured.headers['Content-Type'], 'application/json');
  const json = JSON.parse(captured.body);
  assert.equal(json.fileName, 'aburatoishioil100_lib_1.jpg');
  assert.equal(json.itemCode, 'aburatoishioil100');
  assert.equal(typeof json.bufferBase64, 'string');
});

test('callUploadLibImage: proxy 200 with no yahooUrl throws', async () => {
  const buf = jpegBuffer(2048);
  const origFetch = global.fetch;
  global.fetch = async () => new Response(JSON.stringify({ ok: true, body: 'x' }), { status: 200 });
  try {
    await assert.rejects(
      callUploadLibImage({ buffer: buf, fileName: 'aburatoishioil100_lib_1.jpg', itemCode: 'aburatoishioil100' }),
      (e) => e instanceof YahooProxyError && /did not return yahooUrl/.test(e.message),
    );
  } finally {
    global.fetch = origFetch;
  }
});

test('callUploadLibImage: rejects bad fileName (validateUploadLibFileName)', async () => {
  const buf = jpegBuffer(1024);
  await assert.rejects(
    callUploadLibImage({ buffer: buf, fileName: 'aburatoishioil100.jpg', itemCode: 'aburatoishioil100' }),
    (e) => e instanceof YahooProxyError && /lib_N\.jpg/.test(e.message),
  );
});

// ── callUploadItemImage : Yahoo XML <Error> ────────────────────────
test('callUploadItemImage throws on Yahoo XML <Error> body even with HTTP 200', async () => {
  const buf = jpegBuffer(1024);
  const origFetch = global.fetch;
  global.fetch = async () => new Response(
    '<?xml version="1.0"?><ResultSet><Error><Code>NG</Code><Message>quota exceeded</Message></Error></ResultSet>',
    { status: 200 },
  );
  try {
    await assert.rejects(
      callUploadItemImage({ buffer: buf, fileName: 'aburatoishioil100.jpg', itemCode: 'aburatoishioil100' }),
      (e) => e instanceof YahooProxyError && /Yahoo XML <Error>/.test(e.message),
    );
  } finally {
    global.fetch = origFetch;
  }
});
