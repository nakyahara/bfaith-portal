/**
 * 📷 カメラの確認ページ (apps/inbound-check/views/camera-test.html)
 *
 * 実行: node scripts/test-inbound-check-camera-test-page.mjs
 *
 * 🚨なぜこのページが要るか (2026-09-07):
 *   iPad だけカメラのトラックが 9ms で ended になる件で、「アプリの作りが悪いのか」
 *   「端末がカメラを渡していないのか」を切り分けられなかった。
 *   **アプリの読み取り機能を一切通さない**最小のページを基準として置く。
 *
 * このテストが守るのは「**切り分けの道具が、切り分けを濁していないこと**」:
 *   条件を付けない / 取得は1試行1回 / 途中終了を正常と言わない / 断定しない /
 *   デコーダを読まない / **実ルーターから素通しで配られる**。
 *   ここに何か足すと、このページで再現しなかったときの読み方が壊れる。
 *
 * 🚨 ルートは**本物の router から**確かめる (Codex #1242 R1 P2)。
 *    テスト内で偽のルートを作ると、本物が消えても・認証必須になっても通ってしまう。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-camtest-'));

let pass = 0, fail = 0;
const ok = (cond, label) => {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
};

// ─── 1. 本物の router から、認証なしで配られるか ───────────────────────────
console.log('[1] 実ルーター経由の配信');
const { default: router } = await import('../apps/inbound-check/router.js');
const app = express();
app.use('/apps/inbound-check', router);
const srv = app.listen(0, '127.0.0.1');
await new Promise((r) => srv.once('listening', r));
const base = `http://127.0.0.1:${srv.address().port}`;

const res = await fetch(`${base}/apps/inbound-check/camera-test`);
const html = await res.text();
ok(res.status === 200, `本物のルートが素通しで配る (${res.status}。登録前・不調の端末からこそ開かれる)`);
ok(/text\/html/.test(res.headers.get('content-type') || ''), 'HTML として返る');
ok((res.headers.get('content-security-policy') || '').includes("frame-ancestors 'none'"),
  '外のサイトに埋め込ませない (frame-ancestors none)');
// 比べる相手として、認証が要るページはちゃんと弾かれること (素通しが広がっていない確認)
const guarded = await fetch(`${base}/apps/inbound-check/products`, { redirect: 'manual' });
ok(guarded.status !== 200, `読み取り画面のほうは素通しになっていない (${guarded.status})`);
srv.close();

// ─── 2. 取得の条件に余計なものを混ぜていないか ─────────────────────────────
console.log('\n[2] 取得の条件');
const callAt = html.indexOf('await navigator.mediaDevices.getUserMedia(');
const call = callAt < 0 ? '' : html.slice(callAt, callAt + 120);
// 既定 (「カメラを開く」) は向きを指定しない。押したときだけ背面を名指しする
ok(call.includes('video: wanted === null ? true : wanted'), '既定はいちばん緩い条件 (向きを指定しない)');
ok(!/width:|height:|deviceId/.test(call), '解像度や機器は指定しない (条件で落ちる余地を作らない)');
ok(/facingMode: \{ exact: 'environment' \}/.test(html) && /runTest\(null\)/.test(html),
  '背面カメラは押したときだけ名指しで試す (読み取り画面が使う条件を確かめられる)');
ok((html.match(/mediaDevices\.getUserMedia\(/g) || []).length === 1,
  '取得を呼ぶ箇所は1つだけ (再試行や条件の切り替えを持たない)');

// ─── 3. 診断を濁すものが入っていないか ─────────────────────────────────────
console.log('\n[3] 切り分けを濁さない');
ok(!/BarcodeScanDecide|zxing|readBarcodes|vendor\//.test(html), 'アプリの読み取りロジックを通さない');
ok(!/visibilitychange|pagehide/.test(html), '離脱イベントで勝手に止めない (止めたのが誰か分からなくなる)');
ok(/var mine = \+\+run/.test(html) && /if \(mine !== run\)/.test(html),
  '🚨 1試行につき取得1回。連打や「取得待ち中に止める」で複数の取得を並走させない');
ok(/releaseStream\(s\)/.test(html),
  '🚨 止めた後に遅れて届いたストリームはその場で解放する (掴みっぱなしにしない)');
ok(/function setBusy\(on\)/.test(html) && /startBtn\.disabled = on; backBtn\.disabled = on/.test(html),
  '取得待ち・観察中は**両方の**ボタンを押せなくする');

// ─── 4. 観測の中身 ─────────────────────────────────────────────────────────
console.log('\n[4] 観測');
ok(/addEventListener\('ended'/.test(html) && /addEventListener\('mute'/.test(html),
  'ended と mute を**イベントで**拾う (stop() では ended イベントは出ないので、誰が止めたか分かる)');
ok(/endedAt = Math\.round\(now\(\) - grantedAt\)/.test(html),
  '🚨 終了までの時間は**取得できてから**で測る (許可待ちを含めない)');
ok(/document\.visibilityState/.test(html), '表示状態も残す (裏に回った瞬間に切れていないか)');
ok(/isSecureContext/.test(html), 'https かどうかも出す (http だとカメラは使えない)');
ok(/window\.navigator\.standalone/.test(html), 'ホーム画面から開いたかを出す');
ok(/id="where"/.test(html) && /ホーム画面のアプリの中<\/b>で開いています/.test(html),
  '🚨 押す前に、どちらで開いているかを大きく出す (読み取り画面と同じ開き方でないと比べられない)');
ok(/進み=/.test(html) && !/コマ=/.test(html),
  '「進み」と呼ぶ (実際のコマ数ではなく 0.5 秒ごとの更新検出回数なので)');

// ─── 5. 言い切らない ───────────────────────────────────────────────────────
console.log('\n[5] 結果の言い方');
const finishAt = html.indexOf('function finish(');
const finish = finishAt < 0 ? '' : html.slice(finishAt, html.indexOf('startBtn.addEventListener', finishAt));
ok(/sawEnded \|\| \(track && track\.readyState === 'ended'\)/.test(finish),
  '🚨 途中で終了したものを「正常」と言わない (終了を先に見る)');
ok(finish.indexOf('sawEnded') < finish.indexOf('progress >= 3'),
  '終了の判定が「映像が進んだ」の判定より先に来る');
ok(/今回の条件では再現しませんでした/.test(finish) && !/端末は問題なし/.test(finish),
  '🚨 「端末側」「アプリ側」と断定しない (この1回で分かるのは今回の条件だけ)');
// 🚨 最初だけ進んで途中で固まったものを「再現しませんでした」にしない (Codex #1242 R2)
ok(/var stillGoing = stalledFor !== null && stalledFor <= 1500/.test(finish)
  && /progress >= 3 && !sawMute && stillGoing/.test(finish),
  '🚨 観察の終わりまで進み続けたことまで確かめてから「再現しませんでした」と言う');
ok(/Math\.round\(lastProgressAt - grantedAt\)/.test(finish),
  '「最後に進んだのは」は実際の最終進行時刻から出す (判定時点までの時間ではない)');
ok(/同じ開き方/.test(html), '同じ開き方・同じカメラで比べるよう案内する');

// ─── 6. 出す情報 ───────────────────────────────────────────────────────────
console.log('\n[6] 出す情報');
ok(!/deviceId|groupId/.test(html), '端末を特定しうる値 (deviceId / groupId) は出さない');
ok(!/track\.label/.test(html), 'カメラ名 (自由文字列) は出さない');
ok(/UA' \+ m\[1\]/.test(html) && /UA版不明/.test(html), 'UA はそのまま出さず、版だけ (取れないことも書く)');
ok(/保存も送信もしません/.test(html), '映像を保存・送信しないと書いてある');
ok(/文字の部分だけ/.test(html) && !/画面を撮って送ってください/.test(html),
  '🚨 結果を送るときは「カメラを止めて文字だけ」と案内する (映像に人や伝票が写り込まないように)');
ok(/await navigator\.clipboard\.writeText/.test(html),
  'コピーの成否を待ってから「コピーしました」と言う');

// ─── 7. 取得が重ならないこと (実際に動かして確かめる) ──────────────────────
// 🚨 試行番号で結果を捨てても、**未完了の getUserMedia が重なること自体は防げない** (Codex #1244 R1)。
//    重なるとカメラ終了の切り分けに別の要因が入るので、ここは実挙動で守る
console.log('\n[7] 取得は1試行1回 (保留中に別のボタンを押しても増えない)');
{
  const vm = await import('node:vm');
  const script = html.slice(html.indexOf('<script>') + 8, html.lastIndexOf('</script>'));

  const el = () => ({
    disabled: false, innerHTML: '', textContent: '', className: '', style: {}, srcObject: null,
    readyState: 0, videoWidth: 0, videoHeight: 0, currentTime: 0, muted: false, playsInline: false,
    _on: {},
    addEventListener(k, f) { (this._on[k] = this._on[k] || []).push(f); },
    click() { (this._on.click || []).forEach((f) => f()); },
    play() { return Promise.resolve(); },
  });
  /** ページを1つ動かす。standalone の名乗り方を変えられる */
  const boot = ({ standalone = false, displayMode = false } = {}) => {
    const nodes = {};
    for (const id of ['log', 'v', 'verdict', 'start', 'startBack', 'stop', 'copy', 'where']) nodes[id] = el();
    const state = { calls: 0, settle: null };
    const ctx = {
      console,
      document: { getElementById: (id) => nodes[id] || null, visibilityState: 'visible' },
      navigator: {
        userAgent: 'Mozilla/5.0 (iPad; CPU OS 18_5 like Mac OS X) AppleWebKit/605.1.15',
        maxTouchPoints: 5,
        mediaDevices: { getUserMedia: () => { state.calls++; return new Promise((r, j) => { state.settle = { r, j }; }); } },
        clipboard: { writeText: async () => {} },
        standalone,
      },
      location: { protocol: 'https:' },
      setInterval: () => 1, clearInterval: () => {}, setTimeout: () => 1, clearTimeout: () => {},
      alert: () => {},
    };
    ctx.window = ctx;
    ctx.window.performance = { now: () => Date.now() };
    ctx.window.screen = { width: 768, height: 1024 };
    ctx.window.matchMedia = () => ({ matches: displayMode });
    ctx.window.isSecureContext = true;
    ctx.window.innerWidth = 768; ctx.window.innerHeight = 954;
    ctx.window.navigator = ctx.navigator;
    vm.createContext(ctx);
    vm.runInContext(script, ctx, { timeout: 5000 });
    return { nodes, state };
  };

  // 🚨 開き方の判定は2通りの名乗り方があり、どちらでも「アプリの中」と出ないと誤誘導になる
  {
    const a = boot({ standalone: true });
    ok(/ホーム画面のアプリの中/.test(a.nodes.where.innerHTML) && a.nodes.where.className.includes('app'),
      'navigator.standalone だけでも「アプリの中」と分かる');
    const b = boot({ displayMode: true });
    ok(/ホーム画面のアプリの中/.test(b.nodes.where.innerHTML) && b.nodes.where.className.includes('app'),
      'display-mode: standalone だけでも「アプリの中」と分かる');
    ok(!/読み取り画面と同じ条件/.test(a.nodes.where.innerHTML),
      '開き方しか分からないので「同じ条件」とまでは言わない');
  }

  const { nodes, state } = boot();
  const calls = () => state.calls;

  // 🚨 押す前に「どちらで開いているか」が出ている (2回続けて Safari で測ってしまった件)
  ok(/ブラウザで開いています/.test(nodes.where.innerHTML)
    && /アプリの中で<\/b>試してください/.test(nodes.where.innerHTML)
    && !/Safari/.test(nodes.where.innerHTML),
    '🚨 ブラウザで開いていることが押す前に分かる (ブラウザ名は名指ししない)');
  ok(nodes.where.className.includes('browser'), '色でも分かる (ブラウザ = 注意色)');

  nodes.start.click();                       // 1回目 — 取得は保留のまま
  await new Promise((r) => setImmediate(r));
  ok(calls() === 1, `押したら取得が1回だけ走る (${calls()})`);
  ok(nodes.start.disabled === true && nodes.startBack.disabled === true,
    '取得待ちのあいだは両方のボタンが押せない');

  nodes.startBack.click();                   // 🚨 保留中に別のボタン
  nodes.start.click();                       // 🚨 保留中に連打
  await new Promise((r) => setImmediate(r));
  ok(calls() === 1, `🚨 保留中に別のボタンを押しても取得は増えない (${calls()})`);

  nodes.stop.click();                        // 🚨 取得待ちのまま「止める」
  nodes.start.click();
  await new Promise((r) => setImmediate(r));
  ok(calls() === 1, `🚨 取得待ちのまま止めて押し直しても、決着するまで取得は増えない (${calls()})`);

  state.settle.j(Object.assign(new Error('x'), { name: 'NotAllowedError' }));   // 1回目が決着
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  ok(nodes.start.disabled === false && nodes.startBack.disabled === false,
    '決着したらボタンが戻る (押せないままにならない)');
  nodes.start.click();
  await new Promise((r) => setImmediate(r));
  ok(calls() === 2, `決着したあとは次の取得ができる (${calls()})`);
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
