import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor, fixture, FIXTURES } from './fixtures/lp-compose/index.mjs';
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

// 🚨 lease と材料は work/ の外 (codex exec review P2)。
//    work/ は Claude に Read/Write/Edit を許しているので、そこに置くと読める・書き換えられる
const state = path.join(tmp || process.cwd(), 'state');

/**
 * ./phlp <args> を work/ で走らせる (シムと同じ呼び方)。
 * 🚨 spawnSync は使えない — service-api を**同じプロセス**に立てているので、
 *    親が同期で止まると子の HTTP が返ってこない (行き詰まる)。
 */
const phlpWith = (extraEnv, ...args) => new Promise((resolve) => {
  const child = spawn(process.execPath, [PHLP, ...args], {
    cwd: work,
    env: { ...process.env, PH_LP_BASE: BASE, PH_SERVICE_TOKEN: 'test-token-lp-runner', PH_LP_RETRIES: '1', PH_LP_STATE_DIR: state, ...extraEnv },
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

const phlp = (...args) => phlpWith({}, ...args);

const exists = (n) => fs.existsSync(path.join(work, n));
const inState = (n) => fs.existsSync(path.join(state, n));

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
ok(!exists(`lease-${jid}.json`), '🚨 lease を作業ディレクトリに置かない (Claude が読める・書き換えられる・codex exec review P2)');
ok(inState(`lease-${jid}.json`), 'lease は work/ の外 (state/) に閉じる');
ok(fs.readFileSync(path.join(work, `spec-${jid}.md`), 'utf8').includes('取込互換'), '仕様書の中身が入っている');
eq(cl.json.packet.images, 1, '商品画像の枚数が分かる');
ok(cl.json.packet.product_info.includes('ハッカ油'), '商品情報が来る');
// 🚨 スタッフの定型文と同じ指示文が実行役に届く (設計 §5)。
//    届かないと、実行役はスキルの言い回しを読むことになり、測定が「同じ入力の比較」にならない
ok(cl.json.instruction && cl.json.instruction.includes('⑦ AI画像生成プロンプトのみを出力'),
  '🚨 claim にスタッフと同じ指示文が付いてくる');

console.log('③ images — Drive が無い環境では取れないことが分かる');
const im = await phlp('images', jid);
ok(im.code !== 0, 'Drive が無ければ失敗する (0 枚で黙って進まない)');
// 🚨 404 を「ここで終わり」の合図に使わない (codex exec review P2)。
//    枚数は packet から決めるので、取れなかった枚があれば必ず失敗する
eq(im.json.expected, 1, '🚨 packet の枚数を期待値にする');

console.log('④ ファイル引数は決まった名前だけ');
// 🚨 PR1-c から **lint はサーバが実行してそれが正本**。
//    ダミー文字列では accepted を受け取ってもらえないので、本当に通る本文を置く
fs.writeFileSync(path.join(work, `out-${jid}.md`), compositionFor('ハッカ油スプレー 100ml'), 'utf8');
fs.writeFileSync(path.join(work, 'secret.txt'), 'トークンのつもり', 'utf8');
eq((await phlp('result', jid, '--accepted', '--file', 'secret.txt')).code, 2, '🚨 別名のファイルは読まない');
eq((await phlp('result', jid, '--accepted', '--file', `../out-${jid}.md`)).code, 2, '🚨 パス付きは読まない');
// 🚨 形が合っていても **他の依頼の** ファイルは読まない (codex exec review P1)。
//    work に前の依頼のファイルが残っていると、別の商品の構成を書き戻せた
const other = String(Number(jid) + 1);
fs.writeFileSync(path.join(work, `out-${other}.md`), '別の依頼の構成', 'utf8');
fs.writeFileSync(path.join(work, `lint-${other}.json`), '{"ok":true}', 'utf8');
eq((await phlp('result', jid, '--accepted', '--file', `out-${other}.md`, '--lint', `lint-${other}.json`)).code, 2,
  '🚨 他の依頼の out-*.md は読まない (codex exec review P1)');
fs.writeFileSync(path.join(work, `reason-${other}.txt`), '別の理由', 'utf8');
eq((await phlp('result', jid, '--rejected', '--reason-file', `reason-${other}.txt`)).code, 2,
  '🚨 他の依頼の reason-*.txt も読まない');
eq((await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`)).code, 2, '予約していなければ result を出せない');

console.log('⑤ 予約の前は fail / release、後は result');
const rel = await phlp('release', jid, '--reason', '一時障害');
eq(rel.code, 0, '予約前は手放せる');
eq(rel.json.status, 'queued', 'キューに戻る');
// ランナーが決めた run id (PH_LP_RUN_ID) が job に写る。Claude が --run に何を書いても変わらない
// (ランナーはこの id で「実際に本回答を書いたモデル」を付ける・codex #1591 High)
const cl2 = await phlpWith({ PH_LP_RUN_ID: 'lpr-20261002-160000-abcdef' }, 'claim', '--run', 'lp-test-2');
eq(cl2.json.job_id, req.job.id, 'もう一度掴める');
eq(db.prepare('SELECT runner_run_id FROM ph_lp_compose_jobs WHERE id = ?').get(req.job.id).runner_run_id, 'lpr-20261002-160000-abcdef',
  '🚨 run id はランナーの PH_LP_RUN_ID (Claude の --run ではない)');
// モデルはランナーが PH_LP_MODEL で渡す (claude --model と同じ値)。Claude の申告は使わない (2026-10-02)
const rvNoEnv = await phlp('reserve', jid);
ok(rvNoEnv.code !== 0 && /PH_LP_MODEL/.test(rvNoEnv.err + rvNoEnv.out),
  '🚨 PH_LP_MODEL が無ければ予約しない (claim の応答のモデルで代わりに予約しない・codex #1591 Medium)');
const rvBad = await phlpWith({ PH_LP_MODEL: 'claude-opus-5' }, 'reserve', jid);
eq(rvBad.code, 1, '🚨 サーバの設定と違うモデルでは予約できない');
eq(rvBad.json?.code, 'bad_model', 'bad_model で断られる');
const rv = await phlpWith({ PH_LP_MODEL: lp.DEFAULT_MODEL }, 'reserve', jid, '--model', 'claude-opus-5');
eq(rv.code, 0, '予約できる');
ok(rv.json.generation_id > 0, 'generation_id が返る');
eq(db.prepare('SELECT model FROM ph_lp_compose_generations WHERE id = ?').get(rv.json.generation_id).model, lp.DEFAULT_MODEL,
  '🚨 記録されるのはランナーが渡したモデル (--model を書いても使わない)');
eq((await phlp('fail', jid, '--code', 'OTHER', '--message', 'x')).code, 1, '🚨 予約後に fail は通らない');
eq((await phlp('release', jid, '--reason', 'x')).code, 1, '🚨 予約後に release も通らない');
eq((await phlp('fail', jid, '--code', 'へんなコード')).code, 2, '知らない code は断る');

console.log('⑤b lint — サーバが正本。出す前に自分で直せる (PR1-c)');
{
  const good = await phlp('lint', jid, '--file', `out-${jid}.md`);
  eq(good.code, 0, 'lint を呼べる');
  eq(good.json.lint.ok, true, '通る本文は ok');
  // 通らない本文に入れ替えると、**コマンドとしても失敗する** (通ったつもりで先へ進ませない)
  fs.writeFileSync(path.join(work, `out-${jid}.md`), fixture(FIXTURES.legacyV21), 'utf8');
  const bad = await phlp('lint', jid, '--file', `out-${jid}.md`);
  eq(bad.code, 1, '🚨 lint が通らなければ exit 1');
  eq(bad.json.lint.ok, false, '何が足りないかを出す');
  ok(bad.out.indexOf('lease_token') === -1, '🚨 lint の出力にも lease を混ぜない');
  eq((await phlp('lint', jid, '--file', `out-${other}.md`)).code, 2, '🚨 他の依頼の本文は lint しない');
  fs.writeFileSync(path.join(work, `out-${jid}.md`), compositionFor('ハッカ油スプレー 100ml'), 'utf8');
}

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
const leaseTok = db.prepare('SELECT lease_token FROM ph_lp_compose_jobs WHERE id = ?').get(req.job.id).lease_token;
lp.recordImageServed(db, req.job.id, { leaseToken: leaseTok, fileId: 'FILEIDTOP001', sha256: 'b'.repeat(64), bytes: 4321 });
const good = await phlp('result', jid, '--accepted', '--file', `out-${jid}.md`, '--lint', `lint-${jid}.json`, '--rounds', '1');
eq(good.code, 0, 'サーバが配っていれば通る');
eq(good.json.status, 'done', 'done になる');
eq(good.json.receipt.images[0].sha256, 'b'.repeat(64), '🚨 証跡はサーバの記録 (実行役が書いた値ではない)');

console.log('⑥b reviewdata — 検品に渡すデータを CLI が組み立てる (codex exec review P1)');
fs.writeFileSync(path.join(work, `_lp_review_${jid}.md`), '# LP構成案 全文', 'utf8');
eq((await phlp('reviewdata', jid)).code, 2, '🚨 seen-ID.md が無ければ検品を始めない (見ずに書いた構成案を通さない)');
fs.writeFileSync(path.join(work, `seen-${jid}.md`), 'img-1: 透明ボトル、白ラベル「100mL」', 'utf8');
const rd = await phlp('reviewdata', jid);
eq(rd.code, 0, 'seen があれば通る');
ok(rd.out.includes('ハッカ油スプレー 100ml'), '🚨 材料① (商品名) が入る');
ok(rd.out.includes('天然ハッカ油 100ml'), '🚨 材料① (商品情報) が入る — これが無いと Codex は食い違いを見られない');
ok(rd.out.includes('白ラベル'), '材料② (画像から読み取ったこと) が入る');
ok(rd.out.includes('# LP構成案 全文'), '構成案が入る');
ok(rd.out.indexOf('lease_token') === -1 && rd.out.indexOf('packet_hash') === -1, '🚨 lease は混ざらない');

console.log('⑦ clean — 一時ファイルを全部消す (rm は使えない)');
fs.writeFileSync(path.join(work, `_lp_review_${jid}.md`), 'レビュー用', 'utf8');
const cleaned = await phlp('clean', jid);
eq(cleaned.code, 0, 'clean が通る');
for (const n of [`spec-${jid}.md`, `out-${jid}.md`, `imgs-${jid}.json`, `seen-${jid}.md`, `lint-${jid}.json`, `_lp_review_${jid}.md`]) {
  ok(!exists(n), `${n} が消えた`);
}
ok(!inState(`lease-${jid}.json`), 'state/ の lease も消える');
ok(exists('secret.txt'), '🚨 関係ないファイルは消さない');

console.log('⑧ checkreview — 実体ファイルかを見る (phlpreview が先に呼ぶ)');
eq((await phlp('checkreview', jid)).code, 2, '無ければ断る');
fs.writeFileSync(path.join(work, `_lp_review_${jid}.md`), 'x'.repeat(100), 'utf8');
eq((await phlp('checkreview', jid)).code, 0, 'あれば通る');

console.log('⑨ 壊れた ID');
for (const bad2 of ['0', '1abc', '-1', 'abc']) {
  eq((await phlp('reserve', bad2)).code, 2, `"${bad2}" は断る`);
}

console.log('⑩ 素材画像 — 何番が素材かを Claude に伝え、検品にも素材の一覧を渡す (2026-10-02)');
{
  const dM = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-RUN-M', 'ハッカ油スプレー 200ml', 'test')`).run().lastInsertRowid);
  const draftM = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(dM);
  const reqM = lp.requestJob(db, {
    draft: draftM, spec, productInfo: '天然ハッカ油 200ml。', colorVariations: '',
    images: [
      { file_id: 'FILEIDTOPM01', role: 'slot:1' },
      { file_id: 'FILEIDMATM01', role: 'material', folder: '素材/使用イメージ', name: '玄関.jpg' },
      { file_id: 'FILEIDMATM02', role: 'material', folder: '素材', name: 'パーツ.png' },
    ],
    idempotencyKey: 'runner-test-mat-1', actor: 'nakahara@x',
  });
  ok(reqM.ok, '素材つきの依頼を受け付けた');
  const mid = String(reqM.job.id);
  const clM = await phlpWith({ PH_LP_RUN_ID: 'lpr-20261002-170000-mmmmmm' }, 'claim', '--run', 'lp-test-m');
  eq(clM.json.job_id, reqM.job.id, '素材つきの依頼が来る');
  const list = clM.json.packet.image_list || [];
  ok(list.length === 3 && list[0].kind === 'product' && list[0].file === `img-${mid}-1.jpg`, '1 番は商品画像 (img-ID-1.jpg)');
  ok(list[1].kind === 'material' && list[1].folder === '素材/使用イメージ' && list[1].name === '玄関.jpg' && list[1].file === `img-${mid}-2.jpg`,
    '🚨 2 番は素材 (場所と名前つき) — Claude が「使用素材」に書く名前');
  ok(list[2].kind === 'material' && list[2].role === 'material:2', '3 番も素材');
  eq(clM.json.packet.materials_omitted, 0, '入らなかった素材の数も出る');
  ok(clM.out.indexOf('FILEIDMATM01') === -1, '🚨 素材の file_id も Claude に見せない (証跡は CLI が持つ)');
  fs.writeFileSync(path.join(work, `seen-${mid}.md`), 'img-1: 透明ボトル。img-2: 玄関でスプレー。img-3: キャップの部品。', 'utf8');
  fs.writeFileSync(path.join(work, `_lp_review_${mid}.md`), '# 構成案\n' + 'x'.repeat(100), 'utf8');
  const rdM = await phlp('reviewdata', mid);
  ok(rdM.code === 0 && rdM.out.includes('うち素材 2 枚'), `検品の材料① に素材の枚数 (${rdM.out.split('\n').find((l) => l.includes('画像:'))})`);
  ok(rdM.out.includes('[素材の一覧 (n = img-ID-n.jpg)]') && rdM.out.includes('2: 素材/使用イメージ/玄関.jpg') && rdM.out.includes('3: 素材/パーツ.png'),
    '🚨 検品の材料① に素材の一覧 (使用素材に無い素材を書いていないかを見るため)');
  ok(String(clM.json.packet.image_guide || '').includes('2枚目: 素材画像 (素材1・素材/使用イメージ/玄関.jpg)'),
    '🚨 claim に添付画像の説明 (スタッフの ChatGPT 版と同じ文) が出る');
  eq((await phlp('release', mid, '--reason', '試験の片付け')).code, 0, '片付け (予約前なので手放せる)');
  await phlp('clean', mid);
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(reqM.job.id);   // 次の claim が拾わないように

  // 商品 6 + 素材 10 = 16 枚の依頼: 新しい phlp は 16 枚と言って掴み、16 枚を落としにいく (codex #1593 Low)
  const d16 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-RUN-16', 'ハッカ油スプレー 16', 'test')`).run().lastInsertRowid);
  const r16 = lp.requestJob(db, {
    draft: db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(d16), spec, productInfo: '天然ハッカ油。', colorVariations: '',
    images: [
      ...Array.from({ length: 6 }, (_, i) => ({ file_id: 'FILEIDP16X0' + i, role: i === 0 ? 'white_bg' : 'slot:' + i })),
      ...Array.from({ length: 10 }, (_, i) => ({ file_id: 'FILEIDM16X' + String(i).padStart(2, '0'), role: 'material', folder: '素材', name: `m${i}.jpg` })),
    ],
    idempotencyKey: 'runner-test-16-1', actor: 'nakahara@x',
  });
  ok(r16.ok && JSON.parse(r16.job.packet_json).images.length === 16, '16 枚の依頼を受け付けた');
  const cl16 = await phlpWith({ PH_LP_RUN_ID: 'lpr-20261003-090100-sixtee' }, 'claim', '--run', 'lp-test-16');
  eq(cl16.json.job_id, r16.job.id, '🚨 新しい phlp (16 枚と言う) は 16 枚の依頼を掴む');
  const im16 = await phlp('images', String(r16.job.id));
  eq(im16.json?.expected, 16, '🚨 16 枚を落としにいく (6 枚で止めない・この試験は Drive が無いので 1 枚目で失敗する)');
  await phlp('release', String(r16.job.id), '--reason', '試験の片付け');
  await phlp('clean', String(r16.job.id));
}

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
