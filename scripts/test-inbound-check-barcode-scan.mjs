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
 *   6. **本番と同じ経路** (Express の静的配信 → Content-Type → locateFile → wasm コンパイル → 復号)。
 *      wasmBinary を直接注入するテストは、いちばん壊れやすいこの経路を通らない (Codex #1233 R1 P2)
 *   7. 安全弁の**挙動** (2フレーム一致 / 例外で連続が切れる / 8回で打ち切り)。
 *      「その行が書いてあるか」を正規表現で見るだけでは守れない (Codex #1233 R2)
 *   8. **映像が届いているかの見張り** (実時間)。1コマも来ない / 途中で止まった のどちらも打ち切る。
 *      🚨「解析できたか」で見てはいけない — 黒一色でも止まっていてもデコード結果は空配列で返るので、
 *      正常扱いされて安全弁が発火しない (Codex #1235 R1 P1)
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

// ─── 6. 本番と同じ経路で通す ────────────────────────────────────────────────
// 🚨 ここが今回いちばん壊れやすい: マウントのパス / Content-Type / locateFile のどれかが
//    ずれると、画面には「読み取りプログラムを読み込めませんでした」しか出ない。
//    wasm は `application/wasm` で返らないと WebAssembly.instantiateStreaming が通らない。
console.log('\n[6] 本番と同じ経路 (Express 配信 → Content-Type → locateFile → wasm)');
{
  const express = (await import('express')).default;
  const app = express();
  app.use('/vendor/zxing-wasm', express.static(distDir));
  const srv = app.listen(0, '127.0.0.1');
  await new Promise((r) => srv.once('listening', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  try {
    const js = await fetch(`${base}/vendor/zxing-wasm/${IIFE_REL}`);
    ok(js.status === 200 && /javascript/.test(js.headers.get('content-type') || ''),
      `本体が配信される (${js.status} / ${js.headers.get('content-type')})`);
    const wasmRes = await fetch(`${base}/vendor/zxing-wasm/${WASM_REL}`);
    ok(wasmRes.status === 200 && wasmRes.headers.get('content-type') === 'application/wasm',
      `wasm が application/wasm で返る (${wasmRes.headers.get('content-type')}) = instantiateStreaming が通る`);
    ok((await fetch(`${base}/vendor/zxing-wasm/../../package.json`)).status === 404,
      '配信ディレクトリの外へは出られない');

    // IIFE をブラウザと同じように評価する。中の Emscripten は globalThis.window があるときだけ
    // fetch でファイルを取りに行くので、Node でもそこを通すために window を置く
    const hadWindow = 'window' in globalThis;
    if (!hadWindow) globalThis.window = globalThis;
    try {
      const src = await js.text();
      // eslint-disable-next-line no-new-func
      new Function(src + '\n;globalThis.__ZX = ZXingWASM;')();
      const ZX = globalThis.__ZX;
      ok(!!(ZX && ZX.readBarcodesFromImageData), 'グローバル ZXingWASM が生える (script タグで読める形)');
      // 画面と同じ locateFile。ここで wasm を実 URL から取ってコンパイルする
      await ZX.prepareZXingModule({
        overrides: { locateFile: (f, prefix) => (String(f).endsWith('.wasm') ? `${base}/vendor/zxing-wasm/${WASM_REL}` : prefix + f) },
        fireImmediately: true,
      });
      const jan = '4901234567894';
      const r = await ZX.readBarcodesFromImageData(renderEan13(jan), READER_OPTIONS);
      ok(r.length === 1 && r[0].text === jan, `配信された wasm で JAN を復号できる (${r[0]?.text})`);
      // 🚨 wasm が 404 なら **その場で失敗する** こと (フレームループまで遅れて出ると握りつぶされる)
      ZX.purgeZXingModule();
      let threw = false;
      try {
        await ZX.prepareZXingModule({
          overrides: { locateFile: () => `${base}/vendor/zxing-wasm/nope.wasm` },
          fireImmediately: true,
        });
      } catch { threw = true; }
      ok(threw, 'wasm が取れないときは prepareZXingModule が失敗する (画面はここでバナーを出す)');
    } finally {
      if (!hadWindow) delete globalThis.window;
    }
  } finally {
    srv.close();
  }
}

// ─── 7. 安全弁の挙動 (別の商品を出さないための判断そのもの) ─────────────────
console.log('\n[7] 採否の判断 (public/js/barcode-scan-decide.js)');
{
  const src = fs.readFileSync(path.join(ROOT, 'public/js/barcode-scan-decide.js'), 'utf8');
  // 画面と同じもの (script タグで読まれる形) をそのまま評価して使う
  const sandbox = {};
  new Function('globalThis', 'with (globalThis) { ' + src + ' }')(sandbox);
  const D = sandbox.BarcodeScanDecide;
  ok(!!(D && D.step && D.initialState), 'script タグで読める形 (グローバルに生える)');

  const codes = (...v) => ({ kind: 'codes', codes: v.map((x) => (typeof x === 'string' ? { text: x } : x)) });
  const run = (events) => {
    let st = D.initialState();
    const actions = [];
    for (const e of events) { const r = D.step(st, e); st = r.state; actions.push(r.action + (r.value ? ':' + r.value : '')); }
    return actions;
  };

  ok(run([codes('4901234567894')]).join() === 'continue', '1フレームだけでは確定しない');
  ok(run([codes('4901234567894'), codes('4901234567894')]).join() === 'continue,accept:4901234567894',
    '同じ値が2フレーム続けば確定する');
  ok(run([codes('4901234567894'), codes('9999999999999'), codes('4901234567894')]).join() === 'continue,continue,continue',
    '違う値を挟んだら確定しない (A → B → A)');
  // 🚨 Codex #1233 R2: 例外を挟んでも「連続」とみなしてはいけない
  ok(run([codes('4901234567894'), { kind: 'error' }, codes('4901234567894')]).join() === 'continue,continue,continue',
    '例外を挟んだら連続が切れる (A → 例外 → A で確定しない)');
  ok(run([codes('4901234567894'), { kind: 'idle' }, codes('4901234567894')]).join() === 'continue,continue,continue',
    '映像が来ないフレームを挟んでも連続が切れる');
  ok(run([codes('4901234567894'), codes(), codes('4901234567894')]).join() === 'continue,continue,continue',
    '読めないフレームを挟んでも連続が切れる');

  const errs = Array.from({ length: D.MAX_FAIL_STREAK }, () => ({ kind: 'error' }));
  const a = run(errs);
  ok(a[a.length - 1] === 'abort' && a.slice(0, -1).every((x) => x === 'continue'),
    `例外が ${D.MAX_FAIL_STREAK} 回続いたら打ち切る (それまでは続ける)`);
  {
    let st = D.initialState();
    let last = null;
    for (const e of errs) { last = D.step(st, e); st = last.state; }
    ok(last.reason === 'decode', '打ち切りの理由が decode (映像が出ない case と言い分けられる)');
  }
  ok(run([...errs.slice(0, D.MAX_FAIL_STREAK - 1), codes('4901234567894'), ...errs.slice(0, D.MAX_FAIL_STREAK - 1)])
    .every((x) => x === 'continue'), '途中で1回でも解析できたら失敗の数え直し (たまの失敗で止めない)');

  // 値の選び方
  ok(D.pickValue([{ text: '4901234567894', isValid: false }]) === null,
    'チェックデジットが合わない読み取り (isValid=false) は使わない');
  ok(D.pickValue([{ text: '123' }]) === null, '短すぎる値は使わない');
  ok(D.pickValue([{ text: 'X00ABCD123' }]) === 'X00ABCD123', 'FNSKU (英数字) も読める');
  ok(D.pickValue([{ text: ' 4901234567894 ' }]) === '4901234567894', '前後の空白は落とす');
  ok(D.pickValue([{ text: '49-0123' }]) === null, '記号を含む値は使わない');
  ok(D.pickValue([]) === null && D.pickValue(null) === null, '空でも落ちない');
}

// ─── 8. 映像が届いているかの見張り (実時間) ───────────────────────────────
// 🚨 2026-09-07 実機の症状 (readyState は進んだが表示は黒) をそのまま再発させないための下限。
//    デコード結果で判断すると、黒一色でも「見つからない (空配列)」で返るので永久に正常扱いになる
console.log('\n[8] 映像が届いているかの見張り');
{
  const src = fs.readFileSync(path.join(ROOT, 'public/js/barcode-scan-decide.js'), 'utf8');
  const sandbox = {};
  new Function('globalThis', 'with (globalThis) { ' + src + ' }')(sandbox);
  const D = sandbox.BarcodeScanDecide;
  const T = D.NO_VIDEO_TIMEOUT_MS;
  ok(T >= 5000 && T <= 15000, `打ち切りまでの時間が現実的 (${T}ms)`);

  // ① 1コマも来ない (今回の実機症状)
  const w0 = D.newVideoWatch(0);
  ok(D.videoStalled(w0, T - 1) === false && D.videoStalled(w0, T) === true,
    '映像が1コマも来なければ打ち切る (デコーダの読み込みが終わっていなくても効く)');

  // ② 途中で止まった (1コマ出たあと固まる)
  const w1 = D.noteFrame(D.newVideoWatch(0), 3000);
  ok(D.videoStalled(w1, 3000 + T - 1) === false && D.videoStalled(w1, 3000 + T) === true,
    '1コマ来たあと止まったら、その時点から数えて打ち切る');
  ok(D.videoStalled(w1, T) === false, '1コマ来ていれば、開始からの経過だけでは打ち切らない');

  // ③ 届き続けている間は打ち切らない
  let w2 = D.newVideoWatch(0);
  let stalled = false;
  for (let t = 250; t <= 60000; t += 250) { w2 = D.noteFrame(w2, t); if (D.videoStalled(w2, t)) stalled = true; }
  ok(!stalled, '映像が届き続けている間は打ち切らない (60秒回しても発火しない)');

  // ④ 解析できなかった (空配列) は「映像が来た」ことの証明にならない
  const after = D.step(D.initialState(), { kind: 'codes', codes: [] });
  ok(after.action === 'continue' && !('idleStreak' in after.state),
    '🚨 デコード結果の空配列は見張りに影響しない (黒い映像を正常扱いしない)');

  // ⑤ 🚨 トラックが muted のあいだは「届いた」に数えない (Codex #1235 R2 P1)。
  //    カメラが映像を出せなくても video は黒いコマを再生し続け、currentTime も進むため
  let c = D.newFrameCursor();
  c = D.markArrived(c, 'p1', true);
  const cMuted = D.markArrived(c, 'p2', false);
  ok(cMuted === c, 'muted のあいだは新しいコマとして数えない');
  let wm = D.noteFrame(D.newVideoWatch(0), 1000);
  for (let t = 1250; t <= 1000 + T; t += 250) { /* muted なので noteFrame しない */ }
  ok(D.videoStalled(wm, 1000 + T) === true,
    'muted が続けば「映像が来ていない」として打ち切れる (黒いコマで誤魔化されない)');
}

// ─── 8b. 見張りと解析で印を分ける (映像は正常なのに読めない、を防ぐ) ────────
// 🚨 1つの印を共有すると、見張りが先に新しいコマを観測した瞬間に解析側が「新しくない」と
//    判断して飛ばし、映像は出ているのにいつまでも読み取れなくなる (Codex #1235 R2 P2)
console.log('\n[8b] フレームの受け渡し (見張り / 解析)');
{
  const src = fs.readFileSync(path.join(ROOT, 'public/js/barcode-scan-decide.js'), 'utf8');
  const sandbox = {};
  new Function('globalThis', 'with (globalThis) { ' + src + ' }')(sandbox);
  const D = sandbox.BarcodeScanDecide;

  let c = D.newFrameCursor();
  ok(D.nextDecode(c).decode === false, 'コマが届く前は解析しない');
  c = D.markArrived(c, 'p1', true);
  const r1 = D.nextDecode(c);
  ok(r1.decode === true, '届いたコマは解析する');
  c = r1.cursor;
  ok(D.nextDecode(c).decode === false, '🚨 同じコマは2回解析しない (「2回続けて一致」が実質1回にならない)');

  // 見張りが先に何度も観測しても、解析は最新の1コマを1回だけ読む
  c = D.markArrived(c, 'p2', true);
  c = D.markArrived(c, 'p3', true);
  const r2 = D.nextDecode(c);
  ok(r2.decode === true && r2.cursor.decoded === 'p3',
    '🚨 見張りが先に進んでも解析は止まらない (最新のコマを読む)');
  ok(D.nextDecode(r2.cursor).decode === false, '読んだあとは次のコマが来るまで解析しない');

  // 待機 (新しいコマなし) では一致回数を消さない = 2コマで確定できる
  let st = D.initialState();
  const hit = { text: '4901234567894' };
  st = D.step(st, { kind: 'codes', codes: [hit] }).state;         // 1コマ目
  const res = D.step(st, { kind: 'codes', codes: [hit] });        // 待機を挟んで2コマ目
  ok(res.action === 'accept' && res.value === '4901234567894',
    '🚨 待機を挟んでも2コマ目で確定できる (待つだけで一致回数を消さない)');
}

// ─── 9. 画面が安全弁を使っているか (退行防止) ──────────────────────────────
console.log('\n[9] 画面が安全弁を通している (退行防止)');
{
  const html = fs.readFileSync(path.join(ROOT, 'apps/inbound-check/views/products.html'), 'utf8');
  ok(/fireImmediately:\s*true/.test(html),
    'prepareZXingModule を fireImmediately で待つ (取得・コンパイルの失敗をフレーム解析まで持ち越さない)');
  ok(/\/js\/barcode-scan-decide\.js/.test(html) && /BarcodeScanDecide\.step\(/.test(html),
    '採否の判断を画面に埋め直していない (切り出したものを使っている)');
  ok(/const GUIDE = \{/.test(html) && /GUIDE\.x \* vw/.test(html) && /GUIDE\.x \* 100/.test(html),
    '🚨 描く枠と切り出す範囲が同じ数字 (GUIDE) から作られている');
  ok(/drawImage\(v, sx, sy, sw, sh,/.test(html),
    '映像全体ではなく枠の中だけを解析している (棚の別ラベルを読まない)');
  ok(/object-fit:\s*contain/.test(html) && /style\.aspectRatio/.test(html),
    '映像を切り取らずに出している (枠の % と映像の % がずれない)');

  // 🚨 2026-09-07 実機: カメラは開いたのに映像が黒いままだった。原因は下の3つ
  const openAt = html.indexOf('getUserMedia({ video:');
  const loadAt = html.indexOf('await loadDecoder()');
  ok(openAt > 0 && loadAt > openAt,
    '🚨 カメラを先に開けてからデコーダを読む (先に wasm を落とすと iOS の「押した」扱いが切れて再生されない)');
  const onAt = html.indexOf("$('#scanBox').classList.add('on')");
  const srcAt = html.indexOf('v.srcObject = scanStream');
  ok(onAt > 0 && srcAt > onAt,
    '🚨 画面に出してから stream をつなぐ (display:none のまま挿すと iOS は再生を始めない)');
  ok(/v\.play\(\)\.catch\(/.test(html) && /v\.muted = true/.test(html) && /v\.playsInline = true/.test(html),
    '🚨 autoplay 属性に頼らず play() を呼ぶ + muted / playsInline をプロパティでも立てる');
  // 🚨 play() は「再生が始まるまで」返らない。await の後ろに見張りを置くと、始まらない端末で
  //    Promise が pending のままになり 8 秒たってもカメラを解放できない (Codex #1235 R2 P1)
  ok(!/await v\.play\(\)/.test(html), '🚨 play() を await しない (返ってこない端末で見張りごと止まる)');
  ok(/カメラの映像が出ませんでした/.test(html), '映像が来ないときの案内を出している');

  // 🚨 Codex #1235 R1: 見張りは「デコーダの読み込みより前」に始まっていないと、
  //    wasm の取得が固まったときにカメラを掴んだまま無期限に止まる
  const watchAt = html.indexOf('scanWatchTimer = setInterval(');
  // onloadedmetadata の中にも同じ呼び出しがあるので、**単独で呼んでいる後ろの方**を見る
  const playAt = html.lastIndexOf('v.play().catch(');
  ok(watchAt > 0 && watchAt < loadAt,
    '🚨 映像の見張りをデコーダ読み込みより前に始めている (wasm が固まってもカメラを掴んだままにしない)');
  ok(playAt > 0 && watchAt < playAt,
    '🚨 映像の見張りを play() より前に始めている (再生が始まらない端末でも8秒で解放できる)');
  ok(/BarcodeScanDecide\.videoStalled\(/.test(html) && /BarcodeScanDecide\.markArrived\(/.test(html),
    '🚨 新しいコマが届いたかで判定している (解析できたかで見ない)');
  ok(/track\.readyState === 'live' && !track\.muted/.test(html),
    '🚨 トラックが muted / ended のあいだは「届いた」に数えない (黒いコマで誤魔化されない)');
  ok(/track\.readyState === 'ended'/.test(html) && /カメラが切れました/.test(html),
    'カメラが切れたら待たずにやめる (他アプリに取られた等)');
  ok(/requestVideoFrameCallback/.test(html) && /presentedFrames/.test(html),
    'コマ固有の識別子 (presentedFrames) を使う。使えない端末は currentTime で代用');
  ok(/BarcodeScanDecide\.nextDecode\(frames\)/.test(html) && /plan\.decode/.test(html),
    '🚨 見張りとは別の印で「まだ解析していないコマ」だけを読む');
  ok(/let ev = null;/.test(html) && /if \(!ev\) \{/.test(html),
    '🚨 新しいコマが無いだけの待機では状態を触らない (連続一致を消さない)');
  const stopFn = html.slice(html.indexOf('function stopScan()'), html.indexOf('async function onScanned'));
  ok(/clearInterval\(scanWatchTimer\)/.test(stopFn), 'stopScan が見張りを止める (閉じた後に鳴らない)');
  // 閉じた直後にデコーダ読み込みが失敗しても、後からエラーを残さない
  const catchAt = html.indexOf('catch (e) {', loadAt);
  const catchBody = html.slice(catchAt, catchAt + 400);
  ok(/gen !== scanGen/.test(catchBody) && catchBody.indexOf('gen !== scanGen') < catchBody.indexOf('banner('),
    '🚨 デコーダの失敗より先に「もう閉じられたか」を見る (閉じた後にエラーだけ残さない)');
  ok(/playError = playError \|\| e/.test(html), 'あとから来た play() の失敗も捨てない (現場調査に使う)');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
