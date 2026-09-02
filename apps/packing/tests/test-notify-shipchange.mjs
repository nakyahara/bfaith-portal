/**
 * test-notify-shipchange.mjs — 配送方法変更の通知本文
 *
 * 発端: 「サイズ変更依頼がきたとき、ヤマトの元伝票の削除もしないといけないので、
 *        名前で検索するより伝票番号 (もしくはお客様管理番号 SP〜) が分かったほうが早い」
 *        (三宅さん 2026-08-29)
 *
 * 実データで確認したこと:
 *   送り状アップロードCSVの「お客様管理番号」= 出荷伝票NO (SP…) から SP を除いた数字。
 *   出荷_07 の 33件すべてで一致 (33/33)。
 *   → 通知にこの番号を載せれば、そのままヤマトB2の検索キーになる。
 *
 * 実行: node apps/packing/tests/test-notify-shipchange.mjs
 */
import http from 'node:http';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// webhook を受けるだけのサーバ (本文を取り出す)
const received = [];
const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => { body += c; });
  req.on('end', () => {
    received.push(JSON.parse(body).text);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{"ok":true}');
  });
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
process.env.PACKING_SHIP_CHANGE_WEBHOOK = `http://127.0.0.1:${server.address().port}/hook`;

const { notifyShipChange } = await import('../notify.js');

const base = {
  folderName: '出荷_11', neSlipNo: '1537302',
  currentMethod: '箱 陸便 元払い 営業所止めなし', proposedMethod: '宅急便60サイズ',
  reason: '入らない', worker: '星立夏',
  lines: [{ name: '蒟蒻洗顔スポンジ 6P_クリ箱', sku: 'konjac6', qty: 2 }],
};

console.log('── 元の送り状を消すための番号が載るか ──');
{
  await notifyShipChange({ ...base, slipNo: 'SP00110364189' });
  const t = received.at(-1);
  console.log(t.split('\n').map((l) => '   ' + l).join('\n'));
  ok(t.includes('00110364189'), 'お客様管理番号 (SPを除いた数字) が載る');
  ok(t.includes('SP00110364189'), '出荷伝票NO (SP付き) も併記される — 納品書と突き合わせられる');
  ok(/元の送り状/.test(t), '何に使う番号かが書いてある');
  ok(t.includes('1537302'), '従来のNE伝票番号も残っている');
}

console.log('\n── 🚨 形が違う番号を「お客様管理番号」として出さない ──');
{
  // 採番が変わったときに、接頭辞を機械的に剥がした別物を検索キーとして出すと、
  // 事務が違う伝票を消しかねない。確認できている SP+数字 だけを通す
  for (const bad of ['XX00123', 'SPABC', '', null, 123, 'SP']) {
    received.length = 0;
    await notifyShipChange({ ...base, slipNo: bad });
    const t = received.at(-1);
    ok(!/お客様管理番号/.test(t), `${JSON.stringify(bad)} は番号行を出さない`);
  }
  ok(received.at(-1).includes('1537302'), '番号行が出なくても通知自体は届く');
}

console.log('\n── 番号が無くても落ちない ──');
{
  received.length = 0;
  await notifyShipChange(base);   // slipNo を渡さない
  ok(received.length === 1, 'slipNo 未指定でも送信される');
  ok(!/お客様管理番号/.test(received[0]), '番号行だけが消える');
}

console.log('\n── 📦 ネコポス二枚出し (2026-09-02): 事務のやることが1行目で分かる ──');
{
  received.length = 0;
  await notifyShipChange({
    ...base, currentMethod: 'ヤマト(ネコポス)', proposedMethod: 'ネコポス二枚出し',
    slipNo: 'SP00110364189',
  });
  const t = received.at(-1);
  console.log(t.split('\n').map((l) => '   ' + l).join('\n'));
  ok(/ネコポス二枚出しの依頼/.test(t), '1行目が「二枚出し」の依頼だと分かる');
  ok(!/配送方法の変更依頼/.test(t), '「配送方法の変更」とは言わない (方法は変わらない)');
  ok(/2個口/.test(t), '2個口 (送り状2枚) だと書いてある');
  ok(t.includes('00110364189'), 'お客様管理番号は二枚出しでも載る');
}

await new Promise((r) => server.close(r));
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
