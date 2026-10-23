/**
 * 画像の URL と JPEG 変換 (2026-10-22 修正の再発防止)。
 * 実行: node --test apps/rakuten-yahoo-sync/lib/image-uploader-url.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import {
  normalizeRakutenImageUrl, _toJpegForTest, _isAllowedDownloadUrlForTest, _resolveGoldImagesForTest,
} from './image-uploader.js';

test('http:// の楽天画像は https に直す (楽天以外の http はそのまま = 安全確認で弾く)', () => {
  assert.equal(normalizeRakutenImageUrl('http://image.rakuten.co.jp/b-faith/cabinet/a.jpg'), 'https://image.rakuten.co.jp/b-faith/cabinet/a.jpg');
  assert.equal(normalizeRakutenImageUrl('http://evil.example.com/a.jpg'), 'http://evil.example.com/a.jpg');
  assert.equal(_isAllowedDownloadUrlForTest('http://evil.example.com/a.jpg'), false);
  assert.equal(_isAllowedDownloadUrlForTest(normalizeRakutenImageUrl('http://image.rakuten.co.jp/x/a.jpg')), true);
  assert.equal(_isAllowedDownloadUrlForTest('https://www.rakuten.ne.jp/gold/b-faith/a.jpg'), true);
  assert.equal(_isAllowedDownloadUrlForTest('https://rakuten.ne.jp.evil.com/a.jpg'), false);
});

test('GOLD のメイン画像は gold の URL にする (cabinet にしない)', () => {
  const [g, c] = _resolveGoldImagesForTest([{ type: 'GOLD', location: '/img/a.jpg' }, { type: 'CABINET', location: '/b.jpg' }], { shopSlug: 'b-faith' });
  assert.equal(g.location, 'https://image.rakuten.co.jp/b-faith/gold/img/a.jpg');
  assert.equal(c.location, '/b.jpg');
  assert.equal(normalizeRakutenImageUrl(c.location, { shopSlug: 'b-faith' }), 'https://image.rakuten.co.jp/b-faith/cabinet/b.jpg');
});

test('透過 PNG を JPEG にすると背景は白 (黒にしない)', async () => {
  const png = await sharp({ create: { width: 4, height: 4, channels: 4, background: { r: 0, g: 0, b: 0, alpha: 0 } } }).png().toBuffer();
  const jpg = await _toJpegForTest(png, 85);
  const { data } = await sharp(jpg).raw().toBuffer({ resolveWithObject: true });
  assert.ok(data[0] > 240 && data[1] > 240 && data[2] > 240, `rgb=${data[0]},${data[1]},${data[2]}`);
});
