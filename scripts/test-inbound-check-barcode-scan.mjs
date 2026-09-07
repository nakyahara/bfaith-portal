/**
 * 📷 カメラのバーコード読み取り (apps/inbound-check/views/products.html) — デコーダの実物テスト
 *
 * 実行: node scripts/test-inbound-check-barcode-scan.mjs
 *
 * 🚨なぜこのテストが要るか (2026-09-07):
 *   最初の実装は `BarcodeDetector` (Shape Detection API) を使っていたが、**Safari は未実装**なので
 *   iPad ではカメラが一度も起動しなかった。「対応していません」の分岐に必ず入る = 現場では死んだボタン。
 *   実機を持たずに「読める」と言い切らないために、**本物の wasm に本物のバーコード画像を食わせて
 *   桁まで一致することを毎回確かめる**。
 *
 * 検証項目:
 *   1. 配信するファイルが node_modules に実在する (静的マウントのパスと一致するか)
 *   2. wasm が読み込め、生成した JAN (EAN-13) を桁まで正しく復号できる
 *   3. 画面が渡すのと同じ形 (RGBA の ImageData) と同じ formats 指定で読める
 *   4. 白紙・ノイズでは何も返さない (誤読で違う商品を出さない)
 *   5. products.html が BarcodeDetector に依存していない (退行防止)
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const require = createRequire(import.meta.url);

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

// ─── EAN-13 を描く (テスト用の最小エンコーダ) ────────────────────────────────
// 実機のカメラ映像は用意できないので、規格どおりのバーコード画像を作って読ませる。
const L = ['0001101', '0011001', '0010011', '0111101', '0100011', '0110001', '0101111', '0111011', '0110111', '0001011'];
const G = ['0100111', '0110011', '0011011', '0100001', '0011101', '0111001', '0000101', '0010001', '0001001', '0010111'];
const R = ['1110010', '1100110', '1101100', '1000010', '1011100', '1001110', '1010000', '1000100', '1001000', '1110100'];
const PARITY = ['LLLLLL', 'LLGLGG', 'LLGGLG', 'LLGGGL', 'LGLLGG', 'LGGLLG', 'LGGGLL', 'LGLGLG', 'LGLGGL', 'LGGLGL'];

/** チェックデジット (13桁目)。テストで使う JAN が本物であることを保証する */
function ean13CheckDigit(first12) {
  let sum = 0;
  for (let i = 0; i < 12; i++) sum += Number(first12[i]) * (i % 2 === 0 ? 1 : 3);
  return String((10 - (sum % 10)) % 10);
}

/** EAN-13 の 95 モジュール (0=白 / 1=黒) */
function ean13Modules(code) {
  const d = code.split('').map(Number);
  const parity = PARITY[d[0]];
  let bits = '101';
  for (let i = 0; i < 6; i++) bits += (parity[i] === 'L' ? L : G)[d[i + 1]];
  bits += '01010';
  for (let i = 0; i < 6; i++) bits += R[d[i + 7]];
  bits += '101';
  return bits;
}

/**
 * ImageData 相当 ({data: RGBA, width, height}) を作る。画面が canvas から取るものと同じ形。
 * quiet = 左右の余白 (モジュール数。規格は 9 以上)
 */
function renderEan13(code, { scale = 3, height = 90, quiet = 12 } = {}) {
  const bits = ean13Modules(code);
  const width = (bits.length + quiet * 2) * scale;
  const data = new Uint8ClampedArray(width * height * 4).fill(255);
  for (let i = 0; i < width * height; i++) data[i * 4 + 3] = 255;
  for (let m = 0; m < bits.length; m++) {
    if (bits[m] !== '1') continue;
    for (let x = (quiet + m) * scale; x < (quiet + m + 1) * scale; x++) {
      for (let y = 0; y < height; y++) {
        const o = (y * width + x) * 4;
        data[o] = 0; data[o + 1] = 0; data[o + 2] = 0;
      }
    }
  }
  return { data, width, height };
}

function blank(width = 400, height = 120) {
  const data = new Uint8ClampedArray(width * height * 4).fill(255);
  for (let i = 0; i < width * height; i++) data[i * 4 + 3] = 255;
  return { data, width, height };
}

function noise(width = 400, height = 120, seed = 7) {
  const data = new Uint8ClampedArray(width * height * 4);
  let s = seed;
  for (let i = 0; i < width * height; i++) {
    s = (s * 1103515245 + 12345) & 0x7fffffff;   // 毎回同じ絵にする (落ちたときに再現できるように)
    const v = s % 256;
    data[i * 4] = v; data[i * 4 + 1] = v; data[i * 4 + 2] = v; data[i * 4 + 3] = 255;
  }
  return { data, width, height };
}

// ─── 1. 配信するファイルが実在するか ────────────────────────────────────────
// 🚨 server.js の静的マウントと同じ相対パスを見る。ここがずれると本番で 404 になり、
//    画面には「読み取りプログラムを読み込めませんでした」しか出ない
console.log('[1] 配信ファイル (静的マウントの実体)');
const IIFE_REL = 'iife/reader/index.js';
const WASM_REL = 'reader/zxing_reader.wasm';
let distDir = null;
{
  // server.js と同じ解き方 (exports 経由)。node_modules の配置が変わっても同じ場所を指す
  distDir = path.dirname(path.dirname(require.resolve('zxing-wasm/reader/zxing_reader.wasm')));
  ok(fs.existsSync(path.join(distDir, IIFE_REL)), `${IIFE_REL} がある (script タグで読む本体)`);
  ok(fs.existsSync(path.join(distDir, WASM_REL)), `${WASM_REL} がある (locateFile で指す先)`);
  const server = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  ok(/app\.use\('\/vendor\/zxing-wasm', express\.static\(/.test(server)
    && /resolve\('zxing-wasm\/reader\/zxing_reader\.wasm'\)/.test(server),
    'server.js が /vendor/zxing-wasm を node_modules の zxing-wasm/dist にマウントしている');
  const mountAt = server.indexOf("'/vendor/zxing-wasm'");
  ok(mountAt > 0 && !/maxAge|immutable/.test(server.slice(mountAt, mountAt + 200)),
    'キャッシュ期限を付けていない (JS だけ新しく wasm が古い、という組み合わせを作らない)');
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
  ok(!!pkg.dependencies['zxing-wasm'], 'zxing-wasm が dependencies にある (Render の本番インストールに含まれる)');
}

// ─── 2〜4. 本物の wasm で復号する ───────────────────────────────────────────
console.log('\n[2] 本物の wasm で JAN (EAN-13) を復号する');
const { prepareZXingModule, readBarcodesFromImageData } = await import('zxing-wasm/reader');
// Node には fetch(file://) が無いので、画面の locateFile の代わりに wasm を直接渡す
prepareZXingModule({ overrides: { wasmBinary: fs.readFileSync(path.join(distDir, WASM_REL)) }, fireImmediately: true });

// 画面と同じ formats 指定 (products.html の READER_OPTIONS と揃えること)
const READER_OPTIONS = {
  formats: ['EAN13', 'EAN8', 'UPCA', 'UPCE', 'Code128', 'Code39', 'ITF'],
  tryHarder: true,
  maxNumberOfSymbols: 1,
};

{
  const jan = '490123456789' + ean13CheckDigit('490123456789');
  ok(jan === '4901234567894', `テスト用 JAN のチェックデジットが正しい (${jan})`);
  const r = await readBarcodesFromImageData(renderEan13(jan), READER_OPTIONS);
  ok(r.length === 1 && r[0].text === jan, `JAN を桁まで正しく読める (読み取り=${r[0]?.text} / 期待=${jan})`);
  ok(r[0] && r[0].format === 'EAN13', `フォーマットが EAN13 と分かる (${r[0]?.format})`);
  ok(r[0] && r[0].isValid !== false, 'isValid が false でない (チェックデジット検証を通っている)');
}

console.log('\n[3] 画面が渡すのと同じ条件で読めるか');
{
  // iPad のカメラ映像を canvas に落とすと、バーコードは画面のごく一部。小さめ・余白ありでも読めること
  const small = await readBarcodesFromImageData(renderEan13('4901234567894', { scale: 2, height: 60 }), READER_OPTIONS);
  ok(small.length === 1 && small[0].text === '4901234567894', '小さめ (2px/モジュール・高さ60) でも読める');
  // 別の JAN でも通る = たまたま1つ読めただけではない
  const jan2 = '456789012345' + ean13CheckDigit('456789012345');
  const r2 = await readBarcodesFromImageData(renderEan13(jan2), READER_OPTIONS);
  ok(r2.length === 1 && r2[0].text === jan2, `別の JAN も読める (${jan2})`);
  // maxNumberOfSymbols=1 でも 1件返る (画面は先頭1件しか使わない)
  ok(r2.length <= 1, '1フレームから返るのは最大1件 (画面は先頭だけ使う)');
}

console.log('\n[4] 誤読しないか (違う商品を出さないための下限)');
{
  ok((await readBarcodesFromImageData(blank(), READER_OPTIONS)).length === 0, '白紙からは何も読まない');
  ok((await readBarcodesFromImageData(noise(), READER_OPTIONS)).length === 0, 'ノイズからは何も読まない');
}

// ─── 5. 画面側が BarcodeDetector に戻っていないか ──────────────────────────
console.log('\n[5] 画面が BarcodeDetector に依存していない (退行防止)');
{
  const html = fs.readFileSync(path.join(ROOT, 'apps/inbound-check/views/products.html'), 'utf8');
  ok(!/new BarcodeDetector|'BarcodeDetector' in window/.test(html),
    'products.html が BarcodeDetector を使っていない (Safari に無いので iPad で必ず失敗する)');
  ok(/\/vendor\/zxing-wasm\/iife\/reader\/index\.js/.test(html), '配信パスの本体を読み込んでいる');
  ok(/\/vendor\/zxing-wasm\/reader\/zxing_reader\.wasm/.test(html), 'wasm の場所を locateFile で明示している');
  for (const f of READER_OPTIONS.formats) {
    if (!html.includes(`'${f}'`)) { ok(false, `画面の formats に ${f} がある`); break; }
  }
  ok(READER_OPTIONS.formats.every((f) => html.includes(`'${f}'`)), '画面とテストで formats 指定が揃っている');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
