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
 *   条件を付けない / 停止処理や離脱イベントを持たない / デコーダを読まない / 素通しで配られる。
 *   ここに何か足すと、このページで再現しなかったときに「アプリのせい」と言い切れなくなる。
 */
import express from 'express';
import path from 'path';

const app = express();
const dir = path.join(process.cwd(), 'apps/inbound-check/views');
app.get('/camera-test', (req, res) => res.sendFile(path.join(dir, 'camera-test.html')));
const srv = app.listen(0, '127.0.0.1');
await new Promise((r) => srv.once('listening', r));
const res = await fetch(`http://127.0.0.1:${srv.address().port}/camera-test`);
const html = await res.text();

// getUserMedia より後ろの部分 (呼び出しの条件に余計なものが混ざっていないか見る)
const afterCall = html.split('getUserMedia')[2] || '';

const checks = [
  [res.status === 200, '素通しで配られる (登録前・不調の端末からこそ開かれる)'],
  [/getUserMedia\(\{ video: true, audio: false \}\)/.test(html), 'いちばん緩い条件だけを使う'],
  [!/facingMode/.test(afterCall), '条件で落ちる余地を作らない (facingMode も解像度も指定しない)'],
  [/addEventListener\('ended'/.test(html) && /addEventListener\('mute'/.test(html),
    'ended と mute を拾う (いつ切れたか・映像が止まったかが分かる)'],
  [!/BarcodeScanDecide|zxing|readBarcodes/.test(html), 'アプリの読み取りロジックを通さない'],
  [!/stopScan|visibilitychange|pagehide/.test(html), '停止処理や離脱イベントを持たない (切り分けを濁さない)'],
  [/isSecureContext/.test(html), 'https かどうかも出す (http だとカメラは使えない)'],
  [/window\.navigator\.standalone/.test(html), 'ホーム画面から開いたかを出す'],
  [!/deviceId|groupId/.test(html), '端末を特定しうる値 (deviceId / groupId) は出さない'],
  [!/log\('UA: ' \+ ua\)/.test(html) && /OS \(\\d\+\)/.test(html), 'UA はそのまま出さず、版だけ出す'],
  [/コマ=/.test(html), '映像のコマが進んでいるかを出す (開けたか、ではなく届いているか)'],
];

let ng = 0;
for (const [ok, label] of checks) {
  if (ok) console.log('  ✓ ' + label);
  else { console.log('  ✗ ' + label); ng++; }
}
console.log(`\n${checks.length - ng} PASS / ${ng} FAIL`);
srv.close();
process.exitCode = ng ? 1 : 0;
