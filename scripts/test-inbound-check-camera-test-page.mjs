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
ok(/startBtn\.disabled = true/.test(html), '取得待ち・観察中はボタンを押せなくする');

// ─── 4. 観測の中身 ─────────────────────────────────────────────────────────
console.log('\n[4] 観測');
ok(/addEventListener\('ended'/.test(html) && /addEventListener\('mute'/.test(html),
  'ended と mute を**イベントで**拾う (stop() では ended イベントは出ないので、誰が止めたか分かる)');
ok(/endedAt = Math\.round\(now\(\) - grantedAt\)/.test(html),
  '🚨 終了までの時間は**取得できてから**で測る (許可待ちを含めない)');
ok(/document\.visibilityState/.test(html), '表示状態も残す (裏に回った瞬間に切れていないか)');
ok(/isSecureContext/.test(html), 'https かどうかも出す (http だとカメラは使えない)');
ok(/window\.navigator\.standalone/.test(html), 'ホーム画面から開いたかを出す');
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

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
