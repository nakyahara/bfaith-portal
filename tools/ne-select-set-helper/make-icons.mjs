/**
 * 拡張機能のアイコンPNGを生成する。
 *   node tools/ne-select-set-helper/make-icons.mjs
 *
 * 画像編集ソフトを使わずに済むよう、依存なしで最小限のPNGを書き出す
 * (Node標準の zlib だけ)。図柄は「プレゼント箱」= 選べるセットのおまけを表す。
 * 色を変えたいときはこのファイルの COLOR を直して再実行する。
 */
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'icons');

const COLOR = { r: 0x19, g: 0x71, b: 0xc2 };   // ポータルのプライマリ色に合わせる
const SIZES = [16, 32, 48, 128];

function crc32(buf) {
  let c;
  const table = crc32.table || (crc32.table = (() => {
    const t = new Int32Array(256);
    for (let n = 0; n < 256; n++) {
      c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      t[n] = c;
    }
    return t;
  })());
  let crc = -1;
  for (let i = 0; i < buf.length; i++) crc = (crc >>> 8) ^ table[(crc ^ buf[i]) & 0xff];
  return (crc ^ -1) >>> 0;
}

function chunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, 'ascii'), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}

/** RGBA のピクセル配列から PNG のバイト列を作る */
function encodePng(size, pixels) {
  const raw = Buffer.alloc((size * 4 + 1) * size);
  let p = 0;
  for (let y = 0; y < size; y++) {
    raw[p++] = 0; // フィルタなし
    for (let x = 0; x < size; x++) {
      const i = (y * size + x) * 4;
      raw[p++] = pixels[i];
      raw[p++] = pixels[i + 1];
      raw[p++] = pixels[i + 2];
      raw[p++] = pixels[i + 3];
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0);
  ihdr.writeUInt32BE(size, 4);
  ihdr[8] = 8;   // bit depth
  ihdr[9] = 6;   // color type: RGBA
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk('IHDR', ihdr),
    chunk('IDAT', zlib.deflateSync(raw, { level: 9 })),
    chunk('IEND', Buffer.alloc(0)),
  ]);
}

/** 角丸の内側か (アンチエイリアスなしの単純判定) */
function inRoundedRect(x, y, size, radius) {
  const r = radius;
  const cx = Math.min(Math.max(x, r), size - 1 - r);
  const cy = Math.min(Math.max(y, r), size - 1 - r);
  const dx = x - cx;
  const dy = y - cy;
  return dx * dx + dy * dy <= r * r;
}

function draw(size) {
  const px = Buffer.alloc(size * size * 4);
  const u = size / 128;                    // 128基準の比率
  const radius = Math.round(24 * u);
  const bandHalf = Math.max(1, Math.round(9 * u));   // リボンの太さの半分
  const center = (size - 1) / 2;
  const bowR = Math.max(1, Math.round(13 * u));
  const bowY = Math.round(38 * u);
  const bowDx = Math.round(15 * u);

  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const i = (y * size + x) * 4;
      if (!inRoundedRect(x, y, size, radius)) {
        px[i + 3] = 0;                     // 角の外は透明
        continue;
      }
      let white = false;
      // 十字のリボン
      if (Math.abs(x - center) <= bandHalf) white = true;
      if (Math.abs(y - center) <= bandHalf) white = true;
      // 上部のリボン結び (円2つ)
      for (const sx of [-bowDx, bowDx]) {
        const dx = x - (center + sx);
        const dy = y - bowY;
        if (dx * dx + dy * dy <= bowR * bowR) white = true;
      }
      if (white) {
        px[i] = 255; px[i + 1] = 255; px[i + 2] = 255; px[i + 3] = 255;
      } else {
        px[i] = COLOR.r; px[i + 1] = COLOR.g; px[i + 2] = COLOR.b; px[i + 3] = 255;
      }
    }
  }
  return px;
}

fs.mkdirSync(OUT_DIR, { recursive: true });
for (const size of SIZES) {
  const file = path.join(OUT_DIR, `icon${size}.png`);
  fs.writeFileSync(file, encodePng(size, draw(size)));
  console.log(`${file} (${size}x${size})`);
}
console.log('完了');
