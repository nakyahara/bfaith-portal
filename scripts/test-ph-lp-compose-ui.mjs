import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-compose-ui-');
/**
 * LP 構成の AI 生成 — 詳細画面 (PR1-d・設計 §4.6)
 * 実行: node scripts/test-ph-lp-compose-ui.mjs
 *
 * ここで守りたいのは:
 *   ① 機能フラグが無ければ**画面にも出さない** (押せないボタンを置かない)
 *   ② 最初の表示はサーバーが埋める (読み込み直後に押せる/押せないがちらつかない)
 *   ③ done のときだけ本文が入り、押せない理由はそのまま文章で出る
 *   ④ 🚨 **AI が書いた本文を埋め込んでも HTML が壊れない** — 出力は AI 由来なので
 *      `</script>` を含みうる。ここが抜けると詳細画面が丸ごと壊れる
 */
process.env.PH_SERVICE_TOKEN = 'test-token-lp-ui';
process.env.PH_LP_COMPOSE_ENABLED = '1';

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const express = (await import('express')).default;
const { default: router } = await import('../apps/product-hub/router.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const app = express();
app.use((req, _res, next) => { req.session = { email: 'nakahara@x', role: 'admin' }; next(); });
app.use('/apps/product-hub', router);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;

const getDetail = async (id) => {
  const res = await fetch(`${base}/detail/${id}`);
  return { status: res.status, html: await res.text() };
};
/** 画面に埋め込んだ初期状態 (<script type="application/json" id="lpc-json">) を読む */
const embedded = (html) => {
  const m = html.match(/<script type="application\/json" id="lpc-json">([\s\S]*?)<\/script>/);
  return m ? JSON.parse(m[1].replace(/\\u003c/g, '<')) : null;
};

const draftId = Number(db.prepare(
  `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-UI-1', 'ハッカ油スプレー 100ml', 'test')`
).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, 'FILEIDTOP001', '2026-09-30T00:00:00.000Z')`).run(draftId);

console.log('① 仕様書も商品情報も無いうちは「押せない理由」が出る');
{
  const { status, html } = await getDetail(draftId);
  eq(status, 200, '詳細画面が開く');
  ok(html.includes('id="lpc-btn"'), 'ボタンがある');
  ok(html.includes('📝 商品分析を準備'), '🚨 既存の定型文ボタンは残っている (退避路)');
  const s = embedded(html);
  ok(s && s.enabled === true, '初期状態が埋め込まれている');
  eq(s.job, null, 'まだ依頼は無い');
  ok(/仕様書/.test(s.blocked || ''), `押せない理由が仕様書のこと (${s.blocked})`);
  eq(s.spec, null, '仕様書はまだ無い');
}

const spec = lp.importSpec(db, {
  kind: 'product_analysis', title: 'LP制作システム',
  body: '## 出力形式\n取込互換\t厳守\t必須: 出力内に「AI画像生成プロンプト 出力テンプレート V2.2」を含める\n',
  sheetTitles: ['出力形式'], actor: 'nakahara@x',
}).spec;

console.log('② 仕様書はあるが商品情報が無い');
{
  const s = embedded((await getDetail(draftId)).html);
  ok(/商品情報/.test(s.blocked || ''), `押せない理由が商品情報のこと (${s.blocked})`);
  ok(s.spec && s.spec.title === 'LP制作システム', '仕様書の名前が出る (古ければ人が上げ直せる)');
}

console.log('③ 商品情報を入れると押せるようになる');
db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`)
  .run(draftId, '天然ハッカ油 100ml。虫除け・消臭に。');
{
  const s = embedded((await getDetail(draftId)).html);
  eq(s.blocked, null, '押せない理由が無い');
  eq(s.job, null, 'まだ依頼は無い');
}

console.log('④ 依頼すると「作っています」の状態が埋まる');
const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
const req = lp.requestJob(db, {
  draft, spec, idempotencyKey: 'ui-key-0001', actor: 'nakahara@x',
  productInfo: '天然ハッカ油 100ml。虫除け・消臭に。', colorVariations: '',
  images: [{ file_id: 'FILEIDTOP001', modified_time: '2026-09-30T00:00:00.000Z' }],
});
ok(req.ok, '依頼できる');
{
  const s = embedded((await getDetail(draftId)).html);
  eq(s.job.status, 'queued', '状態が queued');
  eq(s.job.output_text, null, '🚨 まだ本文は返さない');
}

console.log('⑤ できたら本文と lint が入る');
const claim = lp.claimJob(db, { runnerRunId: 'ui-run-1' });
const gen = lp.reserveGeneration(db, claim.job.job_id, {
  leaseToken: claim.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION,
});
lp.recordImageServed(db, claim.job.job_id, {
  leaseToken: claim.job.lease_token, fileId: 'FILEIDTOP001', sha256: 'a'.repeat(64), bytes: 1234,
});
// 🚨 AI の出力は何でも入りうる。HTML を壊す文字列を**わざと**混ぜて埋め込みを確かめる
const EVIL = '</script><script>window.__lpcPwned = 1;</script>';
const OUT = compositionFor('ハッカ油スプレー 100ml').replace('## バッジ・補足\n100ml', `## バッジ・補足\n100ml ${EVIL}`);
const sub = lp.submitResult(db, gen.generation_id, {
  packetHash: claim.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: { ok: true },
});
eq(sub.status, 'done', '結果を受け取れる');
{
  const { html } = await getDetail(draftId);
  const s = embedded(html);
  eq(s.job.status, 'done', '状態が done');
  ok(s.job.output_text && s.job.output_text.includes('# 0枚目｜サムネイル'), '本文が入る');
  ok(s.job.output_text === OUT, '本文はそのまま (切り詰めない・\u0024{OUT.length} 文字)'.replace('\u0024{OUT.length}', String(OUT.length)));
  ok(s.job.lint && s.job.lint.source === 'server', '🚨 lint はサーバーの結果 (PR1-c)');
  eq(s.job.review_rounds, 1, '検品の巡回数が出る');
  ok(typeof s.job.elapsed_sec === 'number', '所要時間が出る');

  console.log('⑤b 🚨 AI の本文で HTML が壊れない');
  // 🚨 文字列としての window.__lpcPwned は残る (本文なので当然)。
  //    見るのは**それが実行される形で出ていないこと**
  ok(!html.includes('</script><script>window.__lpcPwned'), '🚨 生の </script><script> が出ていない (script がそこで閉じない)');
  ok(html.includes('\\u003c/script>'), '< が \\u003c に置き換わっている');
  // 埋め込みブロックの後ろが壊れていないこと = 後続のタグが残っている
  const after = html.slice(html.indexOf('id="lpc-json"'));
  ok(after.includes('prompt-templates-json'), '🚨 埋め込みの後ろの画面が壊れていない');
}

console.log('⑥ 機能フラグが無ければ画面に出さない');
{
  process.env.PH_LP_COMPOSE_ENABLED = '';
  const { html } = await getDetail(draftId);
  // 🚨 文言ではなく**要素が無いこと**を見る (同じ文言は常騐の JS 側にも入っている)
  ok(!html.includes('id="lpc-btn"'), '🚨 無効なら押せないボタンを置かない');
  ok(!html.includes('id="lpc"'), 'ブロックごと出さない');
  ok(!html.includes('id="lpc-json"'), '初期状態も埋め込まない');
  ok(html.includes('📝 商品分析を準備'), '既存の定型文ボタンは出る (今までどおり使える)');
  process.env.PH_LP_COMPOSE_ENABLED = '1';
}

console.log('⑥b 🚨 裏面情報だけの商品でも押せる (画面と API で判定が食い違わない)');
{
  // 最初の表示を API と別の組み方で作っていたときは、裏面情報だけの商品が
  // **画面ではずっと押せない** (API では押せる) という食い違いになっていた
  const d3 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-UI-3', 'ハッカ油スプレー 300ml', 'test')`
  ).run().lastInsertRowid);
  // 商品情報は空、裏面情報だけ入れる
  db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text, back_info_text) VALUES (?, '', ?)`)
    .run(d3, '原材料: ハッカ油、エタノール。内容量 300ml。火気厳禁。');
  const html = (await getDetail(d3)).html;
  const s3 = embedded(html);
  eq(s3.blocked, null, '🚨 裏面情報だけでも押せる');
  // API 側と同じ判定になっていること
  const viaApi = await (await fetch(`${base}/api/drafts/${d3}/lp-compose`)).json();
  eq(viaApi.blocked, s3.blocked, '🚨 画面の最初の表示と API の判定が一致する');
}

console.log('⑦ 失敗・成否不明も画面に出る');
{
  const d2 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-UI-2', 'ハッカ油スプレー 200ml', 'test')`
  ).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`).run(d2, '天然ハッカ油 200ml。');
  const dr2 = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(d2);
  const r2 = lp.requestJob(db, {
    draft: dr2, spec, idempotencyKey: 'ui-key-0002', actor: 'nakahara@x',
    productInfo: '天然ハッカ油 200ml。', colorVariations: '', images: [],
  });
  ok(r2.ok, '2 件目を受け付ける');
  const c2 = lp.claimJob(db, { runnerRunId: 'ui-run-2' });
  lp.failJob(db, c2.job.job_id, { leaseToken: c2.job.lease_token, code: 'MATERIAL_TOO_THIN', message: '商品情報が商品名程度しかありません' });
  const s = embedded((await getDetail(d2)).html);
  eq(s.job.status, 'failed', '失敗が出る');
  ok((s.job.error || '').includes('商品情報'), `理由がそのまま出る (${s.job.error})`);
  eq(s.job.output_text, null, '本文は無い');
  // 🚨 失敗の説明を出す箱は、「できた」ときの箱 (lpc-result) の**外**にある。
  //    中に置くと失敗時はその箱ごと隠れて見えない (codex exec review P2)
  const html3 = (await getDetail(d2)).html;
  const failAt = html3.indexOf('id="lpc-fail"');
  const resultAt = html3.indexOf('id="lpc-result"');
  ok(failAt > 0 && resultAt > 0 && failAt < resultAt, '🚨 失敗の箱は「できた」の箱より前 (= 外側) にある');
}

server.close();
console.log(fail ? `\n❌ ${pass} 件成功 / ${fail} 件失敗` : `\n✅ ${pass} 件成功 / 0 件失敗`);
process.exit(fail ? 1 : 0);
