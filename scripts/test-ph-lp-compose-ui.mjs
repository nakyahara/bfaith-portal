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
// 役割はヘッダで切り替えられるようにする (admin 限定のカードを確かめるため)
app.use((req, _res, next) => { req.session = { email: 'nakahara@x', role: req.get('X-Test-Role') || 'admin' }; next(); });
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

/**
 * 🚨 描画した HTML の中のインライン script を全部 JS として読む。
 *    文字列に生の改行を入れただけでそのブロックが丸ごと死ぬのに、
 *    サーバーのテストは HTML しか見ないので気づけない (codex exec review P1 で実際に踏んだ)。
 */
function checkInlineJs(html, where) {
  const blocks = [...html.matchAll(/<script(?![^>]*\stype=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
  ok(blocks.length > 0, `${where}: インライン script がある`);
  let bad = 0;
  blocks.forEach((code, i) => {
    try { new Function(code); } catch (e) { bad++; console.log(`    script[${i}]: ${e.message}`); }
  });
  eq(bad, 0, `🚨 ${where}: すべての script ブロックが JS として読める`);
}

const draftId = Number(db.prepare(
  `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-UI-1', 'ハッカ油スプレー 100ml', 'test')`
).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, 'FILEIDTOP001', '2026-09-30T00:00:00.000Z')`).run(draftId);

console.log('⓪ 🚨 詳細画面のインライン JS が構文エラーになっていない');
{
  // 🚨 これが無いと、文字列の中に生の改行を入れただけで
  //    **その script ブロックが丸ごと死ぬ** (既存のボタンも動かなくなる)。
  //    サーバーのテストは HTML しか見ないので気づけない — 実際に踏んだ (codex exec review P1)。
  const { html } = await getDetail(draftId);
  checkInlineJs(html, '詳細画面');
}

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
  leaseToken: claim.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION,
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
  // 🚨 本文は、ランナーが「実モデル = 頼んだモデル」を付けるまで出さない (codex #1591 R2 High)
  const pre = embedded((await getDetail(draftId)).html);
  ok(pre.job.status === 'done' && pre.job.model_check === null && pre.job.output_text === null, '🚨 実モデルの確認前は本文を画面に出さない (確認中)');
  lp.recordModelCheck(db, { runnerRunId: 'ui-run-1', actualModels: ['claude-opus-5-5'] });
}
{
  const { html } = await getDetail(draftId);
  const s = embedded(html);
  eq(s.job.status, 'done', '状態が done');
  ok(s.job.output_text && s.job.output_text.includes('# 0枚目｜サムネイル'), '本文が入る');
  ok(s.job.output_text === OUT, '本文はそのまま (切り詰めない・\u0024{OUT.length} 文字)'.replace('\u0024{OUT.length}', String(OUT.length)));
  ok(s.job.lint && s.job.lint.source === 'server', '🚨 lint はサーバーの結果 (PR1-c)');
  eq(s.job.review_rounds, 1, '検品の巡回数が出る');
  ok(typeof s.job.elapsed_sec === 'number', '所要時間が出る');
  // 測定台帳 (設計 §7.1b) に書き写す値が画面に出ていること。
  //    画面は作らない方針なので、台帳に要る値だけを出す
  ok(html.includes('id=\"lpc-meas\"'), '🚨 測定用の行の置き場がある');
  ok(typeof s.job.packet_hash === 'string' && s.job.packet_hash.length === 64, '台帳に要る packet_hash が渡っている');
  ok(typeof s.job.spec_id === 'number', '台帳に要る spec_id が渡っている');
  ok(s.job.within_deadline === true || s.job.within_deadline === false, '台帳に要る「期限内」が渡っている');

  console.log('⑤a モデル名がボタンと結果に出る (2026-10-02)');
  eq(s.model, lp.DEFAULT_MODEL, 'いま押したら使われるモデル (サーバの設定) が渡っている');
  eq(s.model_label, 'Opus 5.5', 'ボタンに出す名前');
  ok(html.includes('🤖 構成をAIに作らせる (Opus 5.5)</button>'), '🚨 最初の表示 (EJS) のボタンにもモデル名が出る');
  eq(s.job.model, lp.DEFAULT_MODEL, 'この依頼で予約されたモデルが渡っている (台帳に書き写す)');
  eq(s.job.model_label, 'Opus 5.5', '結果に出す名前');
  ok(s.job.model_check === 'match' && s.job.actual_model === 'claude-opus-5-5', 'ランナーが付けた確認が画面に渡る (一致・実モデル)');
  ok(html.includes("'モデル確認済み'") || html.includes('モデル確認済み'), '画面に確認の文言がある');

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
  db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, 'FILEIDBACK01', '2026-09-30T00:00:00.000Z')`).run(d3);
  const html = (await getDetail(d3)).html;
  const s3 = embedded(html);
  eq(s3.blocked, null, '🚨 裏面情報だけでも押せる');
  // API 側と同じ判定になっていること
  const viaApi = await (await fetch(`${base}/api/drafts/${d3}/lp-compose`)).json();
  eq(viaApi.blocked, s3.blocked, '🚨 画面の最初の表示と API の判定が一致する');
}

console.log('⑥c 🚨 商品画像が無ければ押す前に止める (2026-10-02 の 1 件目は 4 分待って IMAGES_UNAVAILABLE)');
{
  const d4 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-UI-4', '吹き出し シール 20種120枚入', 'test')`
  ).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`).run(d4, '吹き出しの形のシール。20 種類 120 枚入り。');
  const s4 = embedded((await getDetail(d4)).html);
  ok(String(s4.blocked || '').includes('商品画像がありません'), `画像が無ければ押せない (${s4.blocked})`);
  const viaApi4 = await (await fetch(`${base}/api/drafts/${d4}/lp-compose`)).json();
  eq(viaApi4.blocked, s4.blocked, '🚨 画面の最初の表示と API の判定が一致する');
  const post4 = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004' }),
  });
  eq(post4.status, 409, '🚨 画面を通さずに POST しても受け付けない');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE draft_id = ?').get(d4).n, 0, '依頼は作られない');

  console.log('⑥d 白抜きだけでも押せる・白抜きが先頭 (2026-10-02 fukidashiseal: 白抜きしか無くて押せなかった)');
  db.prepare(`INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_modified_time) VALUES (?, 'FILEIDWHITE01', '2026-10-01T00:00:00.000Z')`).run(d4);
  const s4w = embedded((await getDetail(d4)).html);
  eq(s4w.blocked, null, '🚨 白抜きがあれば押せる');
  const post4w = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004w' }),
  });
  eq(post4w.status, 200, '依頼できる');
  const pk = JSON.parse(db.prepare('SELECT packet_json FROM ph_lp_compose_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(d4).packet_json);
  eq(pk.images.map((im) => im.file_id), ['FILEIDWHITE01'], 'packet の画像 = 白抜き');
  // 商品画像も入れると、白抜きが先頭・続けて TOP から (同じファイルは 1 回だけ)
  db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, 'FILEIDTOP004', '2026-10-01T00:00:00.000Z')`).run(d4);
  db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 1, 'FILEIDWHITE01', '2026-10-01T00:00:00.000Z')`).run(d4);
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE draft_id = ?`).run(d4);
  const post4b = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004b' }),
  });
  eq(post4b.status, 200, 'もう一度依頼できる');
  const pk2 = JSON.parse(db.prepare('SELECT packet_json FROM ph_lp_compose_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(d4).packet_json);
  eq(pk2.images.map((im) => im.file_id), ['FILEIDWHITE01', 'FILEIDTOP004'], '白抜きが先頭・続けて TOP・重複は 1 回');
  eq(pk2.images.map((im) => im.role), ['white_bg', 'slot:1'], '🚨 画像に役割が残る (白抜き / 画像タブの番号・codex #1592 Medium)');
  eq(pk2.packet_version, 4, 'packet の版 = 4');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE draft_id = ?`).run(d4);

  console.log('⑥e 白抜き + 商品画像 6 枚 → 白抜き + 1〜5 (合わせて 6 枚)・画面に並びが出る (codex #1592 High・Low)');
  db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(d4);
  for (let i = 0; i < 6; i++) {
    db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, ?, ?, '2026-10-01T00:00:00.000Z')`).run(d4, i, 'FILEIDSLOT0' + (i + 1));
  }
  const s6 = embedded((await getDetail(d4)).html);
  eq((s6.image_plan || []).map((im) => im.label), ['白抜き', '1 TOP', '2', '3', '4', '5'], '🚨 押す前に「AI に渡す画像」の並びが画面に渡る');
  // 画面の JS (lpcImagesText) を描画済みの HTML から切り出して実際に動かす (codex #1592 R2 High)
  const html6pre = (await getDetail(d4)).html;
  const src = html6pre.slice(html6pre.indexOf('// lpc-images-text:start'), html6pre.indexOf('// lpc-images-text:end'));
  ok(src.includes('function lpcImagesText'), '画面の JS から切り出せる');
  const lpcImagesText = new Function(src + '\nreturn lpcImagesText;')();
  const t6 = lpcImagesText(s6, false);
  ok(t6.includes('この依頼で AI に渡した画像: 白抜き → 1 TOP\n'), `終わった前回の依頼の並び (${JSON.stringify(t6)})`);
  ok(t6.includes('もう一度押すと渡す画像: 白抜き → 1 TOP → 2 → 3 → 4 → 5'), '🚨 押し直したときに渡すいまの並びも出す (前回の並びだけを出さない)');
  eq(lpcImagesText({ job: null, image_plan: s6.image_plan, blocked: null }, false).split('\n')[0], '押すと AI に渡す画像: 白抜き → 1 TOP → 2 → 3 → 4 → 5', '依頼が無ければいまの並びだけ');
  eq(lpcImagesText({ job: null, image_plan: [], blocked: '商品画像がありません' }, false), '', '押せないときは出さない');
  const post6 = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004c' }),
  });
  eq(post6.status, 200, '依頼できる');
  const pk6 = JSON.parse(db.prepare('SELECT packet_json FROM ph_lp_compose_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(d4).packet_json);
  eq(pk6.images.map((im) => im.file_id), ['FILEIDWHITE01', 'FILEIDSLOT01', 'FILEIDSLOT02', 'FILEIDSLOT03', 'FILEIDSLOT04', 'FILEIDSLOT05'],
    '🚨 packet = 白抜き + 1〜5 (6 枚目は入らない)');
  const sj = embedded((await getDetail(d4)).html);
  eq(sj.job.images.map((im) => im.label), ['白抜き', '1 TOP', '2', '3', '4', '5'], '依頼の後は「この依頼で渡した画像」(受付時に固定) が画面に渡る');
  eq(sj.job.images[0].file_id, 'FILEIDWHITE01', '測定行に書く file_id も渡る');
  // POST の応答も初期表示と同じ形 (同じキーの再送に終わった依頼が返る場合・codex #1592 R3 Medium)
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE draft_id = ?`).run(d4);
  const again = await (await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004c' }),
  })).json();
  ok(again.ok && again.created === false && again.job.status === 'cancelled', '同じキーの再送は終わった依頼を返す');
  eq((again.image_plan || []).map((im) => im.label), ['白抜き', '1 TOP', '2', '3', '4', '5'], '🚨 POST の応答にも画像の並びがある');
  ok('blocked' in again && 'spec' in again, 'POST の応答にも押せない理由・仕様書がある');
  ok(lpcImagesText(again, false).includes('もう一度押すと渡す画像: 白抜き → 1 TOP'), '🚨 POST の応答をそのまま描いても「もう一度押すと渡す画像」が出る');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'queued' WHERE draft_id = ? AND idempotency_key = 'ui-key-0004c'`).run(d4);
  const tj = lpcImagesText(sj, true);
  ok(tj.startsWith('この依頼で AI に渡した画像: 白抜き → 1 TOP → 2 → 3 → 4 → 5') && !tj.includes('もう一度押すと'), '作っている間はその依頼の並びだけ');
  const html6 = (await getDetail(d4)).html;
  ok(html6.includes('id="lpc-images"'), '画像の並びを出す置き場がある');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE draft_id = ?`).run(d4);

  console.log('⑥f 素材画像 (画像フォルダの下のフォルダ・2026-10-02 中原さん)');
  // 画像フォルダの URL がある商品は、押したときに Drive からサブフォルダの画像を読む
  db.prepare('UPDATE product_drafts SET drive_folder_url = ? WHERE id = ?').run('https://drive.google.com/drive/folders/1MtcKdnRZPf1iqKiNxMJ1ODDPJE3vX9JR', d4);
  const sF = embedded((await getDetail(d4)).html);
  eq(sF.materials_folder, true, '画像フォルダがあれば「押したときに素材を読む」が画面に渡る');
  ok(lpcImagesText(sF, false).includes('+ 素材 (画像フォルダの下のフォルダから最大 10 枚・押したときに読む)'), '押す前の案内に素材のことが出る');
  // 🚨 Drive が読めなければ依頼を作らない (素材なしで作った結果を素材ありと同じ測定に混ぜない)
  const savedKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  const nBefore = db.prepare('SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE draft_id = ?').get(d4).n;
  const postF = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004f' }),
  });
  const jF = await postF.json();
  ok(postF.status === 502 && jF.code === 'materials_unavailable' && /もう一度押してください/.test(jF.error), `🚨 Drive が読めなければ 502・理由を出す (${jF.error})`);
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE draft_id = ?').get(d4).n, nBefore, '🚨 依頼は作られない');
  // 同じキーの再送 (前の依頼がある) は Drive を読まずに前の依頼を返す
  const postRe = await fetch(`${base}/api/drafts/${d4}/lp-compose`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'ui-key-0004c' }),
  });
  ok(postRe.status === 200 && (await postRe.json()).created === false, '同じキーの再送は Drive を読まずに前の依頼を返す (Drive が落ちていても通る)');
  if (savedKey !== undefined) process.env.GOOGLE_SERVICE_ACCOUNT_KEY = savedKey;
  // 素材つきの依頼の並び (場所と名前・入らなかった数)
  const withMat = {
    blocked: null, materials_folder: true, image_plan: sF.image_plan,
    job: {
      status: 'done', materials_omitted: 3,
      images: [
        { role: 'white_bg', label: '白抜き', file_id: 'FILEIDWHITE01' },
        { role: 'material:1', label: '素材1', file_id: 'FILEIDMAT001', folder: '素材/使用イメージ', name: '玄関.jpg' },
        { role: 'material:2', label: '素材2', file_id: 'FILEIDMAT002', folder: '素材', name: 'パーツ.png' },
      ],
    },
  };
  const tM = lpcImagesText(withMat, false);
  ok(tM.includes('この依頼で AI に渡した画像: 白抜き\n'), `商品画像の並びに素材を混ぜない (${JSON.stringify(tM)})`);
  ok(tM.includes('素材 (2 枚): 素材1 素材/使用イメージ/玄関.jpg / 素材2 素材/パーツ.png'), '素材は場所と名前つきで別の行');
  ok(tM.includes('⚠️ 上限 (素材 10 枚) で入らなかった素材 3 枚'), '🚨 入らなかった素材の数を出す');
  db.prepare('UPDATE product_drafts SET drive_folder_url = NULL WHERE id = ?').run(d4);
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
    productInfo: '天然ハッカ油 200ml。', colorVariations: '', images: [{ file_id: 'FILEIDUI2001' }],
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

console.log('⑧ 仕様書の取込カード (一覧画面・admin だけ)');
{
  const getList = async (role) => {
    const res = await fetch(`${base}/list`, { headers: { 'X-Test-Role': role } });
    return { status: res.status, html: await res.text() };
  };
  const a2 = await getList('admin');
  eq(a2.status, 200, '一覧画面が開く');
  ok(a2.html.includes('id="lpspec-file"'), '🚨 admin には .xlsx を上げる欄がある (これが無いと curl しか無い)');
  ok(a2.html.includes('id="lpspec-upload-btn"'), '取り込むボタンがある');
  ok(/いまの版/.test(a2.html), 'いまの版が出る');
  ok(a2.html.includes('LP制作システム'), '仕様書の名前が出る');
  checkInlineJs(a2.html, '一覧画面');

  const s2 = await getList('staff');
  ok(!s2.html.includes('id="lpspec-file"'), '🚨 admin でなければ出さない (仕様書 = AI への指示そのもの)');
  ok(!s2.html.includes('id="lpspec-upload-btn"'), 'ボタンも出さない');
  checkInlineJs(s2.html, '一覧画面 (staff)');
}

server.close();
console.log(fail ? `\n❌ ${pass} 件成功 / ${fail} 件失敗` : `\n✅ ${pass} 件成功 / 0 件失敗`);
process.exit(fail ? 1 : 0);
