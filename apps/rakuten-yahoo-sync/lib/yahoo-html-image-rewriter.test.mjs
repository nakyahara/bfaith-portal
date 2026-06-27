/**
 * yahoo-image-extract + yahoo-html-image-rewriter + html-sanitize (img mode) の unit test.
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/yahoo-html-image-rewriter.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { extractRakutenImageUrls } from './yahoo-image-extract.js';
import {
  rewriteAndSanitizeYahooHtml,
  YahooHtmlRewriteError,
} from './yahoo-html-image-rewriter.js';
import { sanitizeProductHtml } from './html-sanitize.js';
import { extractYahooImageUrl } from './yahoo-publish-proxy.js';

process.env.RAKUTEN_SHOP_SLUG = 'b-faith';

// ── extractRakutenImageUrls ──────────────────────────────────────
test('extractRakutenImageUrls: 楽天 absolute URL を順序保持で抽出', () => {
  const html = `
    <img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg" width="100%"><br>
    <img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-02.jpg" width="100%">
  `;
  const out = extractRakutenImageUrls(html);
  assert.equal(out.length, 2);
  assert.equal(out[0], 'https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg');
  assert.equal(out[1], 'https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-02.jpg');
});

test('extractRakutenImageUrls: 相対パスを normalize して https に正規化', () => {
  const html = `<img src="/image3/12960221/aburatoishioil100-01.jpg">`;
  const out = extractRakutenImageUrls(html);
  assert.deepEqual(out, ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg']);
});

test('extractRakutenImageUrls: 重複は 1 度のみ', () => {
  const html = `<img src="/foo.jpg"><img src="/foo.jpg">`;
  const out = extractRakutenImageUrls(html);
  assert.equal(out.length, 1);
});

test('extractRakutenImageUrls: empty / null safe', () => {
  assert.deepEqual(extractRakutenImageUrls(''), []);
  assert.deepEqual(extractRakutenImageUrls(null), []);
});

// ── extractYahooImageUrl ─────────────────────────────────────────
test('extractYahooImageUrl: <Url><ModeA>URL</ModeA></Url> から URL 抽出', () => {
  const xml = `<?xml version="1.0"?><ResultSet><Result><Status>OK</Status><Id>x</Id><Name>x</Name>
    <Url><ModeA>https://item-shopping.c.yimg.jp/i/a/b-faith01_aburatoishioil100_1</ModeA></Url>
  </Result></ResultSet>`;
  assert.equal(
    extractYahooImageUrl(xml),
    'https://item-shopping.c.yimg.jp/i/a/b-faith01_aburatoishioil100_1',
  );
});

test('extractYahooImageUrl: ModeA 単独 (Url 階層なし) は null', () => {
  const xml = `<ResultSet><Result><ModeA>https://foo.jp/x</ModeA></Result></ResultSet>`;
  assert.equal(extractYahooImageUrl(xml), null);
});

test('extractYahooImageUrl: 不正入力 null safe', () => {
  assert.equal(extractYahooImageUrl(null), null);
  assert.equal(extractYahooImageUrl(''), null);
});

// ── sanitizeProductHtml (img mode) ───────────────────────────────
test('sanitizeProductHtml: allowImgFromHosts なしは img 削除 (現状仕様)', () => {
  const html = '<img src="https://example.com/x.jpg"><br>text';
  const out = sanitizeProductHtml(html);
  assert.equal(out.includes('<img'), false);
  assert.match(out, /text/);
});

test('sanitizeProductHtml: allowImgFromHosts 指定なら該当 host の img を残す', () => {
  const html = '<img src="https://item-shopping.c.yimg.jp/i/a/foo" width="100%" border="0">';
  const out = sanitizeProductHtml(html, { allowImgFromHosts: ['item-shopping.c.yimg.jp'] });
  assert.match(out, /<img[^>]*src="https:\/\/item-shopping\.c\.yimg\.jp\/i\/a\/foo"/);
  assert.match(out, /width="100%"/);
  assert.match(out, /border="0"/);
});

test('sanitizeProductHtml: allow 外 host の img は削除', () => {
  const html = '<img src="https://image.rakuten.co.jp/b-faith/cabinet/foo.jpg">';
  const out = sanitizeProductHtml(html, { allowImgFromHosts: ['item-shopping.c.yimg.jp'] });
  assert.equal(out.includes('<img'), false);
});

test('sanitizeProductHtml: http (非 https) img は削除 (host whitelist 指定時)', () => {
  const html = '<img src="http://item-shopping.c.yimg.jp/i/a/foo">';
  const out = sanitizeProductHtml(html, { allowImgFromHosts: ['item-shopping.c.yimg.jp'] });
  assert.equal(out.includes('<img'), false);
});

test('sanitizeProductHtml: img の style/onclick 等の不正属性は剥がす', () => {
  const html = '<img src="https://item-shopping.c.yimg.jp/i/a/foo" style="color:red" onclick="x()">';
  const out = sanitizeProductHtml(html, { allowImgFromHosts: ['item-shopping.c.yimg.jp'] });
  assert.equal(out.includes('style'), false);
  assert.equal(out.includes('onclick'), false);
});

test('sanitizeProductHtml: host 比較は lower-case', () => {
  const html = '<img src="https://Item-Shopping.c.Yimg.JP/i/a/foo">';
  const out = sanitizeProductHtml(html, { allowImgFromHosts: ['item-shopping.c.yimg.jp'] });
  assert.match(out, /<img/);
});

// ── rewriteAndSanitizeYahooHtml ──────────────────────────────────
test('rewriteAndSanitizeYahooHtml: 楽天 URL を Yahoo URL に置換し sanitize', () => {
  const rakutenHtml = '<img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg" width="100%" border="0"><br><br>';
  const urlMap = new Map([
    ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg',
     'https://item-shopping.c.yimg.jp/i/a/b-faith01_aburatoishioil100_1_d_20260627'],
  ]);
  const out = rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, {
    allowedYahooHosts: ['item-shopping.c.yimg.jp'],
  });
  assert.match(out, /<img[^>]*src="https:\/\/item-shopping\.c\.yimg\.jp\/i\/a\/b-faith01_aburatoishioil100_1_d_20260627"/);
  assert.match(out, /width="100%"/);
  assert.match(out, /<br>/);
});

test('rewriteAndSanitizeYahooHtml: urlMap に無い楽天 img は fail-closed', () => {
  const rakutenHtml = '<img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/notmapped.jpg">';
  const urlMap = new Map();
  assert.throws(
    () => rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, {
      allowedYahooHosts: ['item-shopping.c.yimg.jp'],
    }),
    YahooHtmlRewriteError,
  );
});

test('rewriteAndSanitizeYahooHtml: 既に Yahoo URL の img は pass through', () => {
  const html = '<img src="https://item-shopping.c.yimg.jp/i/a/foo">';
  const out = rewriteAndSanitizeYahooHtml(html, new Map(), {
    allowedYahooHosts: ['item-shopping.c.yimg.jp'],
  });
  assert.match(out, /item-shopping\.c\.yimg\.jp/);
});

test('rewriteAndSanitizeYahooHtml: 相対パス img を normalize → urlMap で置換', () => {
  const rakutenHtml = '<img src="/image3/12960221/aburatoishioil100-01.jpg">';
  const urlMap = new Map([
    ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg',
     'https://item-shopping.c.yimg.jp/i/a/foo'],
  ]);
  const out = rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, {
    allowedYahooHosts: ['item-shopping.c.yimg.jp'],
  });
  assert.match(out, /item-shopping\.c\.yimg\.jp/);
});

test('rewriteAndSanitizeYahooHtml: allowedYahooHosts 未指定は throw', () => {
  assert.throws(
    () => rewriteAndSanitizeYahooHtml('<img src="x">', new Map()),
    YahooHtmlRewriteError,
  );
});

test('rewriteAndSanitizeYahooHtml: empty html は空文字', () => {
  assert.equal(rewriteAndSanitizeYahooHtml('', new Map(), { allowedYahooHosts: ['x'] }), '');
});

test('rewriteAndSanitizeYahooHtml: excludedRakutenUrls の img は silent 削除 (R4 H-1)', () => {
  const rakutenHtml = `
    <img src="https://image.rakuten.co.jp/b-faith/cabinet/coupon/imgrc0122590661.jpg" width="100%">
    <img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg">
  `;
  const urlMap = new Map([
    ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg',
     'https://item-shopping.c.yimg.jp/i/a/foo'],
  ]);
  const excludedRakutenUrls = [
    'https://image.rakuten.co.jp/b-faith/cabinet/coupon/imgrc0122590661.jpg',
  ];
  const out = rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, {
    allowedYahooHosts: ['item-shopping.c.yimg.jp'],
    excludedRakutenUrls,
  });
  // coupon は silent 削除、 aburatoishioil100-01 は置換
  assert.equal(out.includes('coupon'), false);
  assert.match(out, /item-shopping\.c\.yimg\.jp\/i\/a\/foo/);
});

// ── SSRF 防御 (R5 H-1): uploadRakutenImagesToYahoo の host 許可 ──
//   image-uploader の isAllowedDownloadUrl 動作確認 (直接 export してないので uploader 経由)
import { uploadRakutenImagesToYahoo, ImageUploadError } from './image-uploader.js';

test('uploadRakutenImagesToYahoo: desc URL が楽天 host 以外なら throw (SSRF 防御)', async () => {
  await assert.rejects(
    uploadRakutenImagesToYahoo({
      rakutenImages: [],
      itemCode: 'aburatoishioil100',
      descriptionImageUrls: ['https://evil.example.com/foo.jpg'],
    }),
    (e) => e instanceof ImageUploadError && /楽天 host/.test(e.message),
  );
});

test('uploadRakutenImagesToYahoo: desc URL が http (非 https) は throw', async () => {
  await assert.rejects(
    uploadRakutenImagesToYahoo({
      rakutenImages: [],
      itemCode: 'aburatoishioil100',
      descriptionImageUrls: ['http://image.rakuten.co.jp/b-faith/cabinet/foo.jpg'],
    }),
    (e) => e instanceof ImageUploadError && /楽天 host/.test(e.message),
  );
});

test('uploadRakutenImagesToYahoo: localhost は throw (SSRF 防御)', async () => {
  await assert.rejects(
    uploadRakutenImagesToYahoo({
      rakutenImages: [],
      itemCode: 'aburatoishioil100',
      descriptionImageUrls: ['https://localhost/foo.jpg', 'https://169.254.169.254/foo.jpg'],
    }),
    (e) => e instanceof ImageUploadError && /楽天 host/.test(e.message),
  );
});

test('rewriteAndSanitizeYahooHtml: aburatoishioil100 のリアルケース (6 枚)', () => {
  const rakutenHtml = `<img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg" width="100%" border="0"><br><br><br>
<img src="https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-02.jpg" width="100%" border="0"><br><br><br>`;
  const urlMap = new Map([
    ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-01.jpg',
     'https://item-shopping.c.yimg.jp/i/a/b-faith01_aburatoishioil100_1'],
    ['https://image.rakuten.co.jp/b-faith/cabinet/image3/12960221/aburatoishioil100-02.jpg',
     'https://item-shopping.c.yimg.jp/i/a/b-faith01_aburatoishioil100_2'],
  ]);
  const out = rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, {
    allowedYahooHosts: ['item-shopping.c.yimg.jp'],
  });
  assert.match(out, /b-faith01_aburatoishioil100_1/);
  assert.match(out, /b-faith01_aburatoishioil100_2/);
  assert.equal(out.includes('image.rakuten.co.jp'), false);
});
