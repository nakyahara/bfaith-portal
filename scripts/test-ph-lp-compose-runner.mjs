import { temporaryTestDataDir } from './test-temp-dir.mjs';
const tmp = await temporaryTestDataDir(import.meta.url, 'test-lp-runner-');
/**
 * LP 構成の実行役 — 固定機能 CLI (scripts/ph-nightly/phlp.mjs) の通し (段階1・PR1-b)
 * 実行: node scripts/test-ph-lp-compose-runner.mjs
 * 設計 = 正本 AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』§4.5
 *
 * 本物の service-api をローカルに立てて、実行役がやる手順をそのまま通す:
 *   queue → claim → images → reserve → result → clean
 *
 * ここで守りたいのは:
 *   ① Claude に渡るのは材料だけ。**lease と packet_hash は CLI の中に閉じる**
 *   ② 画像は ./phlp images でしか落とせず、保存名も img-ID-n.jpg 固定
 *   ③ 証跡 (sha256) は CLI が記録する。Claude に手で書かせない
 *   ④ ファイル引数は work 直下の決まった名前だけ (パス付き・別名・symlink は拒否)
 *   ⑤ 予約の前は fail / release、予約の後は result --rejected
 *   ⑥ clean がその依頼の一時ファイルを全部消す (rm は使えない)
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

process.env.PH_SERVICE_TOKEN = 'test-token-lp-runner';
process.env.PH_LP_COMPOSE_ENABLED = '1';

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const express = (await import('express')).default;
const { default: router, serviceApiRouter } = await import('../apps/product-hub/router.js');

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PHLP = path.join(HERE, 'ph-nightly', 'phlp.mjs');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// ── 本物の service-api をローカルに立てる ──
const app = express();
app.use((req, _res, next) => { req.session = { email: 'nakahara@x', role: 'admin' }; next(); });
app.use('/apps/product-hub', router);
app.use('/apps/product-hub/service-api', serviceApiRouter);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const BASE = `http://127.0.0.1:${server.address().port}/apps/product-hub/service-api`;

// 実行役の作業ディレクトリ (work/ に相当)
const work = path.join(tmp || process.cwd(), 'work');
fs.mkdirSync(work, { recursive: true });

/**
 * ./phlp <args> を work/ で走らせる (シムと同じ呼び方)。
 * 🚨 spawnSync は使えない — service-api を**同じプロセス**に立てているので、
 *    親が同期で止まると子の HTTP が返ってこない (行き詰まる)。
 */
const phlp = (...args) => new Promise((resolve) => {
  const child = spawn(process.execPath, [PHLP, ...args], {
    cwd: work,
    env: { ...process.env, PH_LP_BASE: BASE, PH_SERVICE_TOKEN: 'test-token-lp-runner', PH_LP_RETRIES: '1' },
  });
  let so = '', se = '';
  child.stdout.on('data', (d) => { so += d; });
  child.stderr.on('data', (d) => { se += d; });
  child.on('close', (code) => {
    let json = null;
    try { json = JSON.parse(so); } catch { /* テキストのまま */ }
    resolve({ code, out: so, err: se, json });
  });
});

const exists = (n) => fs.existsSync(path.join(work, n));

// ── 材料 ──
const spec = lp.importSpec(db, {
  kind: 'product_analysis', title: 'LP制作システム',
  body: '## 出力形式\n取込互換\t厳守\t必須: 出力内に「AI画像生成プロンプト 出力テンプレート V2.2」を含める\n'.repeat(20),
  sheetTitles: ['出力形式'], actor: 'nakahara@x',
}).spec;
const draftId = Number(db.prepare(
  `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-RUN-1', 'ハッカ油スプレー 100ml', 'test')`
).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`)
  .run(draftId, '天然ハッカ油 100ml。虫除け・消臭に。');
// 画像は Drive を叩くので取れない。packet には入るので「枚数」と「取れなかったときの振る舞い」を見る
db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, 'FILEIDTOP001', '2026-09-30T00:00:00.000Z')`).run(draftId);

console.log('① queue — 仕事が無ければ claimable 0');
const q0 = await phlp('queue');
eq(q0.code, 0, 'queue が通る');
eq(q0.json.queue.claimable, 0, 'まだ仕事なし');
ok(q0.json.queue.enabled === true, 'enabled が返る (ランナーがこれを見て Claude を起動するか決める)');

console.log('② claim — 材料は出すが lease は出さない');
const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
const req = lp.requestJob(db, {
  draft, spec, productInfo: '天然ハッカ油 100ml。虫除け・消臭に。', colorVariations: '■カラバリ\nなし (単品)',
  images: [{ file_id: 'FILEIDTOP001', modified_time: '2026-09-30T00:00:00.000Z' }],
  idempotencyKey: 'runner-test-0001', actor: 'nakahara@x',
});
ok(req.ok && req.created, '画面から依頼を受け付けた');
const jid = String(req.job.id);

eq((await phlp('claim')).code, 2, '--run が無ければ断る');
const cl = await phlp('claim', '--run', 'lp-test-1');
eq(cl.code, 0, 'claim が通る');
eq(cl.json.job_id, req.job.id, '押した依頼が来る');
ok(cl.out.indexOf('lease_token') === -1, '🚨 lease_token を標準出力に出さない (Claude に見せない)');
ok(cl.out.indexOf('packet_hash') === -1, '🚨 packet_hash も出さない');
ok(cl.out.indexOf('test-token-lp-runner') === -1, '🚨 service token を出さない');
ok(exists(`spec-${jid}.md`), '仕様書の全文をファイルに落とす');
ok(exists(`lease-${jid}.json`), 'lease は CLI が持つファイルに閉じる');
ok(fs.readFileSync(path.join(work, `spec-${jid}.md`), 'utf8').includes('取込互換'), '仕様書の中身が入っている');
eq(cl.json.packet.images, 1, '商品画像の枚数が分かる');
ok(cl.json.packet.product_info.includes('ハッカ油'), '商品情報が来る');

console.log('③ images — Drive が無い環境では取れないことが分かる');
const im = await phlp('images', jid);
ok(im.code !== 0, 'Drive が無ければ失敗する (0 枚で黙って進まない)');

console.log('④ ファイル引数は決まった名前だけ');
fs.writeFileSync(path.join(work, `out-${jid}.md`), '# LP制作システム V2.1\n\n## ⑦ AI画像生成プロンプト\n…', 'utf8');
fs.writeFileSync(path.join(work, 'secret.txt'), 'トークンのつもり', 'utf8');
eq((await phlp('result', jid, '--accepted', '--file', 'secret.txt')).code, 2, '🚨 別名のファイルは読まない');
eq((await phlp('result', jid, '--accepted', '--file', `../out-${jid}.md`)).code, 2, '🚨 パス付きは読まない');
eq((await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`)).code, 2, '予約していなければ result を出せない');

console.log('⑤ 予約の前は fail / release、後は result');
const rel = await phlp('release', jid, '--reason', '一時障害');
eq(rel.code, 0, '予約前は手放せる');
eq(rel.json.status, 'queued', 'キューに戻る');
const cl2 = await phlp('claim', '--run', 'lp-test-2');
eq(cl2.json.job_id, req.job.id, 'もう一度掴める');
const rv = await phlp('reserve', jid);
eq(rv.code, 0, '予約できる');
ok(rv.json.generation_id > 0, 'generation_id が返る');
eq((await phlp('fail', jid, '--code', 'OTHER', '--message', 'x')).code, 1, '🚨 予約後に fail は通らない');
eq((await phlp('release', jid, '--reason', 'x')).code, 1, '🚨 予約後に release も通らない');
eq((await phlp('fail', jid, '--code', 'へんなコード')).code, 2, '知らない code は断る');

console.log('⑥ result — lint と証跡はサーバが見る');
// lint を通していない accepted は CLI が止める (AI 枠を無駄にしない)
eq((await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--rounds', '1')).code, 2,
  '🚨 lint なしの accepted は CLI が止める');
fs.writeFileSync(path.join(work, `lint-${jid}.json`), JSON.stringify({ ok: false, checks: {} }), 'utf8');
eq((await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--lint', `lint-${jid}.json`, '--rounds', '1')).code, 2,
  '🚨 lint.ok が false でも止める');
fs.writeFileSync(path.join(work, `lint-${jid}.json`), JSON.stringify({ ok: true, checks: {} }), 'utf8');

const bad = await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--lint', `lint-${jid}.json`, '--rounds', '1');
eq(bad.code, 1, '🚨 画像を取得していなければサーバが断る (packet に画像があるため)');
// 🚨 作業ディレクトリに証跡を書いても通らない (偽造できたのを塞いだ)
fs.writeFileSync(path.join(work, `imgs-${jid}.json`), JSON.stringify(
  [{ file: `img-${jid}-1.jpg`, bytes: 1234, file_id: 'FILEIDTOP001', sha256: 'a'.repeat(64) }]), 'utf8');
eq((await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--lint', `lint-${jid}.json`, '--rounds', '1')).code, 1,
  '🚨 証跡を手で置いても通らない (サーバの記録を見る・codex exec review P1)');
// サーバが実際に配ったときだけ通る
lp.recordImageServed(db, req.job.id, { fileId: 'FILEIDTOP001', sha256: 'b'.repeat(64), bytes: 4321 });
const good = await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--lint', `lint-${jid}.json`, '--rounds', '1');
eq(good.code, 0, 'サーバが配っていれば通る');
eq(good.json.status, 'done', 'done になる');
eq(good.json.receipt.images[0].sha256, 'b'.repeat(64), '🚨 証跡はサーバの記録 (実行役が書いた値ではない)');

console.log('⑦ clean — 一時ファイルを全部消す (rm は使えない)');
fs.writeFileSync(path.join(work, `_lp_review_${jid}.md`), 'レビュー用', 'utf8');
const cleaned = await phlp('clean', jid);
eq(cleaned.code, 0, 'clean が通る');
for (const n of [`spec-${jid}.md`, `lease-${jid}.json`, `out-${jid}.md`, `imgs-${jid}.json`, `lint-${jid}.json`, `_lp_review_${jid}.md`]) {
  ok(!exists(n), `${n} が消えた`);
}
ok(exists('secret.txt'), '🚨 関係ないファイルは消さない');

console.log('⑧ checkreview — 実体ファイルかを見る (phlpreview が先に呼ぶ)');
eq((await phlp('checkreview', jid)).code, 2, '無ければ断る');
fs.writeFileSync(path.join(work, `_lp_review_${jid}.md`), 'x'.repeat(100), 'utf8');
eq((await phlp('checkreview', jid)).code, 0, 'あれば通る');

console.log('⑨ 壊れた ID');
for (const bad2 of ['0', '1abc', '-1', 'abc']) {
  eq((await phlp('reserve', bad2)).code, 2, `"${bad2}" は断る`);
}

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
