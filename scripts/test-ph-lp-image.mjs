import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-image-');
/**
 * LP 画像の生成 — 段階2 (apps/product-hub/lib/lp-image.js・2026-10-04)
 * 実行: node scripts/test-ph-lp-image.mjs
 *
 * OpenAI と Drive は偽物に差し替える。確かめたいのは:
 *   ① 月の上限 (既定 3,000 円) を fail-closed で守る — 呼ぶ前に取り置き、超えるなら呼ばない
 *   ② 失敗の費用の扱い: 断られた (4xx) = 請求なし / 結果が分からない (通信断・5xx・再起動) = 見込みのまま
 *   ③ キー・機能フラグ・読めない設定では動かない。品質段に xhigh / max を使わせない
 *   ④ prompt と参考画像は受付時に固定・1 商品に動いている依頼は 1 つ・同じキーの再送は前の依頼
 */
process.env.PH_LP_COMPOSE_ENABLED = '1';
process.env.PH_LP_COMPOSE_DAILY_CAP = '200';   // 構成を作った商品を 1 日にたくさん作る (PR-E の試験で 20 を超える)
const ENV = { PH_LP_IMAGE_ENABLED: '1', OPENAI_LP_IMAGE_API_KEY: 'sk-test-lp-image', PH_LP_MONTHLY_BUDGET_JPY: '3000' };
for (const [k, v] of Object.entries(ENV)) process.env[k] = v;

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const li = await import('../apps/product-hub/lib/lp-image.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const T0 = Date.parse('2026-10-04T03:00:00Z');   // JST 2026-10-04 12:00
const FOLDER = '1MtcKdnRZPf1iqKiNxMJ1ODDPJE3vX9JR';

const spec = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式'], actor: 't' }).spec;
let seqNo = 0;
/** 構成ができた (done・実モデル一致) 商品を 1 つ作る */
function makeComposed(output, { shootMode = 'none', priority = '自社商品（重要度：高）', images = [{ file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }, { file_id: 'FILEIDTOP0001', role: 'slot:1', modified_time: '2026-10-01T00:00:00.000Z' }] } = {}) {
  seqNo++;
  const name = 'ハッカ油スプレー';
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url, image_priority, created_by) VALUES (?, ?, ?, ?, 't')`)
    .run('LPIMG' + seqNo, name, 'https://drive.google.com/drive/folders/' + FOLDER, priority).lastInsertRowid);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  const r = lp.requestJob(db, { draft, spec, idempotencyKey: 'key-img-' + seqNo, actor: 't', productInfo: 'ハッカ油', colorVariations: '', images, now: T0 });
  const c = lp.claimJob(db, { runnerRunId: 'run-img-' + seqNo, maxImages: 16, now: T0 });
  if (!c.job || c.job.job_id !== r.job.id) throw new Error('claim mismatch');
  const g = lp.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: T0 });
  for (const im of JSON.parse(r.job.packet_json).images) {
    lp.recordImageServed(db, c.job.job_id, { leaseToken: c.job.lease_token, fileId: im.file_id, sha256: 'a'.repeat(64), bytes: 10, now: T0 });
  }
  const s = lp.submitResult(db, g.generation_id, { packetHash: c.job.packet_hash, verdict: 'accepted', output: output ?? compositionFor(name), lint: { ok: true }, reviewRounds: 1, now: T0 });
  if (s.status !== 'done') throw new Error('submit: ' + JSON.stringify(s));
  lp.recordModelCheck(db, { runnerRunId: 'run-img-' + seqNo, actualModels: ['claude-opus-5-5'], now: T0 });
  // 画像制作の新フロー PR-E: 撮影判定が決まっていないと作れない。ここで作る商品は「撮影不要」(撮影が要る場合は ⑫ で試す)
  if (shootMode !== 'unset') dbmod.setShootMode(db, id, shootMode, { actor: 't' });
  return { draft, composeJobId: r.job.id };
}

console.log('① 設定 (fail-closed・品質段は low / medium / high だけ)');
{
  const c = li.lpImageConfig(ENV);
  ok(c.usable && c.model === 'gpt-image-2.5-flare' && c.quality === 'medium' && c.budget === 3000 && c.size === '1200x1200',
    '既定 = flare・medium・1200x1200・上限は書いた値 (中原さん「全部おすすめ」= 3,000 円)');
  const noBudget = li.lpImageConfig({ PH_LP_IMAGE_ENABLED: '1', OPENAI_LP_IMAGE_API_KEY: 'k' });
  ok(!noBudget.usable && /月の上限が書かれていません/.test(noBudget.error), '🚨 月の上限が書かれていなければ作らない (検討 §6 層2・中原さん 2026-10-04)');
  ok(c.safety === 150 && c.spendable === 2850, '安全幅 = 5% (最低 100 円)・呼んでよいのは 2,850 円まで');
  ok(li.lpImageConfig({ ...ENV, PH_LP_MONTHLY_BUDGET_JPY: '500' }).spendable === 400, '小さい上限でも安全幅は最低 100 円');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_ENABLED: '' }).usable, '機能フラグが無ければ使えない');
  ok(!li.lpImageConfig({ PH_LP_IMAGE_ENABLED: '1' }).usable, '🚨 キーが無ければ使えない');
  ok(/OPENAI_LP_IMAGE_API_KEY/.test(li.lpImageConfig({ PH_LP_IMAGE_ENABLED: '1', OPENAI_API_KEY: 'sk-other' }).error), '🚨 問い合わせハブのキー名 (OPENAI_API_KEY) は読まない (名前を分ける・検討 §6 層1)');
  for (const q of ['xhigh', 'max', 'auto', 'MEDIUM']) ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_QUALITY: q }).usable, `🚨 品質段 ${q} は使わせない (読めない設定 = 止める)`);
  ok(li.lpImageConfig({ ...ENV, PH_LP_IMAGE_QUALITY: 'high' }).quality === 'high', 'high までは選べる');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_MODEL: 'dall-e-3' }).usable, '知らないモデルは止める');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_MONTHLY_BUDGET_JPY: '3000円' }).usable, '読めない上限は止める (既定に戻さない)');
  eq(li.lpImageConfig({ ...ENV, PH_LP_MONTHLY_BUDGET_JPY: '500' }).budget, 500, '上限は変えられる');
}

console.log('② 費用の計算 (検討 §5.1 の単価・取り置きも確定も 1 ドル 170 円)');
{
  // 文 1,000 + 画像 3,000 tok 入力・出力 439 tok (medium) → (1000×5 + 3000×8 + 439×30)/1e6 × 170
  eq(li.costJpyFromUsage({ input_tokens: 4000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } }), 8, '円に直す');
  eq(li.costJpyFromUsage(null), null, 'usage が無ければ null (見込み額のまま)');
  // 🚨 内訳がそろって数が合うときだけ使う (#1612 R4 High)。安い単価で数えて取り置きが戻るのを防ぐ
  eq(li.costJpyFromUsage({ input_tokens: 24000, output_tokens: 4000, input_tokens_details: {} }), null, '🚨 内訳が無ければ使わない (取り置き額のまま)');
  eq(li.costJpyFromUsage({ input_tokens: 24000, output_tokens: 4000 }), null, '内訳そのものが無くても使わない');
  eq(li.costJpyFromUsage({ input_tokens: 5000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } }), null, '🚨 内訳の合計が合わなければ使わない');
  eq(li.costJpyFromUsage({ input_tokens: -4000, output_tokens: 439, input_tokens_details: { text_tokens: -1000, image_tokens: -3000 } }), null, '負の数は使わない');
  eq(li.costJpyFromUsage({ input_tokens: 4000.5, output_tokens: 439, input_tokens_details: { text_tokens: 1000.5, image_tokens: 3000 } }), null, '整数でなければ使わない');
  ok(Number.isInteger(li.reserveJpy({ quality: 'medium', refs: 1, promptBytes: 123 })), '取り置きは整数の円 (金額は整数の決まり・切り上げ)');
  eq(li.jstMonth(Date.parse('2026-10-31T15:30:00Z')), '2026-11', '月は JST で区切る');
  // 取り置き (#1612 R2): 文は UTF-8 のバイト数。medium・参考 2 枚・4,000 バイト → (4000×5 + 12000×8 + 4000×30)/1e6 × 170
  eq(li.reserveJpy({ quality: 'medium', refs: 2, promptBytes: 4000 }), 41, '取り置きの計算 (medium・参考 2 枚・4,000 バイト)');
  ok(li.reserveJpy({ quality: 'high', refs: 4, promptBytes: 4000 }) > li.reserveJpy({ quality: 'medium', refs: 4, promptBytes: 4000 }), '品質段が上がれば取り置きも上がる');
  ok(li.reserveJpy({ quality: 'medium', refs: 4, promptBytes: 4000 }) > li.reserveJpy({ quality: 'medium', refs: 1, promptBytes: 4000 }), '参考画像が多ければ上がる');
  ok(li.reserveJpy({ quality: 'medium', refs: 2, promptBytes: 4000 }) > li.costJpyFromUsage({ input_tokens: 4000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } }), '🚨 取り置きは実際の額より大きい');
  eq(li.reserveJpy({ quality: 'max', refs: 2, promptBytes: 10 }), null, '知らない品質段は取り置けない (= 作らない)');
}

console.log('③ 作る画像の一覧 (prompt と参考画像)');
{
  const out = compositionFor('ハッカ油スプレー').replace(/## 使用素材\n提供された実物商品画像/, '## 使用素材\n提供された実物商品画像／素材/使用イメージ/玄関.jpg');
  const packet = { images: [
    { file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }, { file_id: 'FILEIDTOP0001', role: 'slot:1' }, { file_id: 'FILEIDSLOT002', role: 'slot:2' },
    { file_id: 'FILEIDMAT0001', role: 'material:1', folder: '素材/使用イメージ', name: '玄関.jpg' },
    { file_id: 'FILEIDMAT0002', role: 'material:2', folder: '素材', name: '使わない.jpg' },
  ] };
  const plan = li.buildImagePlan({ outputText: out, packet });
  ok(!plan.error && plan.images.length === 3, `構成の画像を全部 (${plan.images.length} 枚)`);
  const p0 = plan.images[0];
  ok(p0.prompt.includes('【共通の決まり】') && p0.prompt.includes('# 商品再現ルール') && p0.prompt.includes('【この画像の指示: 0枚目｜サムネイル】'), '共通の決まり + その画像の指示');
  const noMat = plan.images.find((im) => !im.refs.some((r) => /^material:/.test(r.role || '')));
  eq(noMat.refs.map((r) => r.file_id), ['FILEIDWHITE01', 'FILEIDTOP0001'], '商品の写真は白抜き → TOP の 2 枚まで (3 枚目は渡さない)');
  const withMat = plan.images.find((im) => im.refs.some((r) => r.file_id === 'FILEIDMAT0001'));
  ok(!!withMat, '「使用素材」に名前が出てくる素材を参考に渡す');
  ok(!plan.images.some((im) => im.refs.some((r) => r.file_id === 'FILEIDMAT0002')), '使わない素材は渡さない');
  ok(plan.images.every((im) => im.refs.length <= li.MAX_REFS), '参考画像は 4 枚まで (API の上限)');
  ok(withMat.prompt.includes('素材 (素材/使用イメージ/玄関.jpg)'), 'prompt に参考画像の中身を書く');
  ok(li.buildImagePlan({ outputText: 'ただの文章', packet }).error, '画像のブロックが無ければ作らない');
  eq(noMat.refs[0].modified_time, '2026-10-01T00:00:00.000Z', '参考画像は Drive の更新日時も固定する (作る前に照らす)');
  const plan2 = li.buildImagePlan({ outputText: out, packet, refTimes: { FILEIDTOP0001: '2026-10-03T00:00:00.000Z' } });
  eq(plan2.images.find((im) => !im.refs.some((r) => /^material:/.test(r.role || ''))).refs[1].modified_time, '2026-10-03T00:00:00.000Z', '受付時に Drive から取り直した更新日時を使う (packet の空欄を埋める)');
  ok(Buffer.byteLength(plan.images[0].prompt, 'utf8') > plan.images[0].prompt.length, '日本語の prompt はバイト数のほうが大きい (取り置きはバイト数で見る)');
  // 重要度で枚数を決める (検討 §4・#1612 R3 Medium)
  eq(li.imageLimitForPriority('自社商品（重要度：高）'), 8, '高 = 全部 (上限 8)');
  eq(li.imageLimitForPriority('取扱先限定商品（重要度：高）'), 8, '取扱先限定 (高) も全部');
  eq(li.imageLimitForPriority('仕入商品（重要度：低）'), 1, '🚨 低 = 1 枚目だけ');
  eq(li.imageLimitForPriority('仕入れ商品（重要度：激低_白抜）'), 1, '激低 = 1 枚目だけ');
  eq(li.imageLimitForPriority(null), 1, '🚨 重要度が決まっていなければ 1 枚目だけ (お金を使いすぎない側)');
  const one = li.buildImagePlan({ outputText: out, packet, maxImages: 1 });
  ok(one.images.length === 1 && one.images[0].no === 1, '1 枚だけなら「1枚目」(FV) を作る (サムネイルではない)');
  ok(plan.images.every((im) => im.est_jpy > 0) && withMat.est_jpy > noMat.est_jpy, '1 枚ずつ取り置き額を出す (素材を渡す画像は高い)');
}

console.log('③b 重要度が低い商品は 1 枚目だけ・Drive に問い合わせるのは使う画像だけ (#1612 R3 Medium)');
{
  const Lo = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const st = li.imageStateFor(db, { draft: Lo.draft, folderId: FOLDER, env: ENV, now: T0 });
  eq(st.planned_count, 1, '🚨 低の商品は 1 枚だけ作る (取り置きも 1 枚分)');
  const Hi = makeComposed(undefined, { images: [
    { file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' },
    { file_id: 'FILEIDTOP0001', role: 'slot:1', modified_time: '2026-10-01T00:00:00.000Z' },
    { file_id: 'FILEIDUNUSED3', role: 'slot:2', modified_time: '2026-10-01T00:00:00.000Z' },
    { file_id: 'FILEIDUNUSEDM', role: 'material', folder: '素材', name: '使わない.jpg', modified_time: '2026-10-01T00:00:00.000Z' },
  ] });
  eq(li.imageRefCandidates(db, Hi.draft, ENV).sort(), ['FILEIDTOP0001', 'FILEIDWHITE01'], '🚨 Drive に問い合わせるのは実際に参考に渡す画像だけ (使わない 3 枚目・素材が消えていても止めない)');
}

console.log('④ 受け付け (押せない理由・固定・二重にしない)');
const A = makeComposed();
{
  const okState = li.imageStateFor(db, { draft: A.draft, folderId: FOLDER, env: ENV, now: T0 });
  eq(okState.blocked, null, '構成ができていて画像フォルダがあれば押せる');
  eq(okState.planned_count, 3, '作る枚数が画面に渡る');
  ok(okState.planned_reserve_jpy > 0, `押したときに取り置く額 (最大の額) が画面に渡る (${okState.planned_reserve_jpy} 円)`);
  ok(/画像フォルダ/.test(li.imageBlockReason(db, { draft: A.draft, folderId: null, env: ENV, now: T0 })), '画像フォルダが無ければ押せない (保存先が無い)');
  ok(/PH_LP_IMAGE_ENABLED/.test(li.imageBlockReason(db, { draft: A.draft, folderId: FOLDER, env: {}, now: T0 })), '機能フラグが無ければ押せない');
  const noCompose = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('X1', 'x', 't')`).run().lastInsertRowid));
  ok(/構成を作って/.test(li.imageBlockReason(db, { draft: noCompose, folderId: FOLDER, env: ENV, now: T0 })), '構成ができていなければ押せない');
  eq(li.requestImageJob(db, { draft: A.draft, folderId: FOLDER, idempotencyKey: 'x', env: ENV, now: T0 }).code, 'bad_request', 'キーの形');
  const Z = makeComposed(undefined, { images: [{ file_id: 'FILEIDNOTIME1', role: 'white_bg' }] });
  const rz = li.requestImageJob(db, { draft: Z.draft, folderId: FOLDER, idempotencyKey: 'img-key-z001', env: ENV, now: T0 });
  ok(rz.code === 'not_ready' && /更新日時/.test(rz.error), '🚨 更新日時の分からない参考画像があれば受け付けない (#1612 R2 Medium)');
  eq(li.imageRefCandidates(db, Z.draft, ENV), ['FILEIDNOTIME1'], '受付の前に Drive で日時を取り直す候補');
  ok(li.requestImageJob(db, { draft: Z.draft, folderId: FOLDER, idempotencyKey: 'img-key-z002', refTimes: { FILEIDNOTIME1: '2026-10-01T00:00:00.000Z' }, env: ENV, now: T0 }).ok, '取り直した日時があれば受け付ける');
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'cancelled' WHERE draft_id = ?`).run(Z.draft.id);
  const r1 = li.requestImageJob(db, { draft: A.draft, folderId: FOLDER, idempotencyKey: 'img-key-0001', actor: 'u@x', env: ENV, now: T0 });
  ok(r1.ok && r1.created && r1.job.status === 'queued', '受け付ける');
  ok(r1.job.model === 'gpt-image-2.5-flare' && r1.job.quality === 'medium' && r1.job.folder_id === FOLDER, 'モデル・品質段・保存先を受付時に固定');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_images WHERE image_job_id = ?').get(r1.job.id).n, 3, '画像 3 枚分の prompt を固定');
  ok(db.prepare('SELECT MIN(est_jpy) AS m FROM ph_lp_images WHERE image_job_id = ?').get(r1.job.id).m > 0, '取り置き額も 1 枚ずつ固定');
  const again = li.requestImageJob(db, { draft: A.draft, folderId: FOLDER, idempotencyKey: 'img-key-0001', env: ENV, now: T0 });
  ok(again.ok && !again.created && again.job.id === r1.job.id, '同じキーの再送は前の依頼');
  eq(li.requestImageJob(db, { draft: A.draft, folderId: FOLDER, idempotencyKey: 'img-key-0002', env: ENV, now: T0 }).code, 'already_running', '🚨 動いている間は 2 つ目を受けない');
}

/** 偽物の部品 */
function fakeDeps(over = {}) {
  const calls = { generate: [], upload: [], ensure: [], ref: [] };
  const deps = {
    getDB: () => db,
    env: ENV,
    now: () => T0,
    sleep: async () => {},
    fetchRef: async (fileId) => { calls.ref.push(fileId); return { buf: Buffer.from('ref-' + fileId), mime: 'image/jpeg' }; },
    ensureFolder: async (parentId) => { calls.ensure.push(parentId); return 'AIFOLDER00001'; },
    upload: async ({ folderId, name, buf }) => { calls.upload.push({ folderId, name, len: buf.length }); return 'DRIVEOUT' + String(calls.upload.length).padStart(4, '0'); },
    generate: async (a) => {
      calls.generate.push({ model: a.model, quality: a.quality, size: a.size, refs: a.refs.length, key: a.apiKey, prompt: a.prompt.slice(0, 40) });
      return { buf: Buffer.from('PNGDATA'), usage: { input_tokens: 4000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } } };
    },
    ...over,
  };
  return { deps, calls };
}
const usageOf = (jobId) => db.prepare(`SELECT u.* FROM ph_ai_usage u JOIN ph_lp_images i ON i.id = u.ref_id WHERE i.image_job_id = ? ORDER BY u.id`).all(jobId);
const imagesOf = (jobId) => db.prepare('SELECT * FROM ph_lp_images WHERE image_job_id = ? ORDER BY seq').all(jobId);
const jobOf = (jobId) => db.prepare('SELECT * FROM ph_lp_image_jobs WHERE id = ?').get(jobId);

console.log('⑤ 作る係 — うまくいく');
{
  const jobId = jobOf(db.prepare('SELECT id FROM ph_lp_image_jobs ORDER BY id DESC LIMIT 1').get().id).id;
  const { deps, calls } = fakeDeps();
  const w = li.createLpImageWorker(deps);
  await w.kick();
  const ims = imagesOf(jobId);
  ok(ims.every((im) => im.status === 'done' && im.drive_file_id), '3 枚とも作って Drive に保存');
  eq(jobOf(jobId).status, 'done', '依頼は done');
  ok(calls.generate.every((g) => g.model === 'gpt-image-2.5-flare' && g.quality === 'medium' && g.size === '1200x1200' && g.key === 'sk-test-lp-image'),
    '受付時のモデル・品質段・サイズと専用キーで呼ぶ');
  ok(calls.generate.every((g) => g.refs === 2), '参考画像 (白抜き + TOP) を渡す');
  ok(calls.ensure.every((p) => p === FOLDER), '保存先は商品の画像フォルダの中の「AI初稿」');
  ok(calls.upload[0].name.startsWith(A.draft.ne_code + '_AI初稿_') && calls.upload[0].name.endsWith('_0枚目.png'), `名前に商品コードと何枚目か (${calls.upload[0].name})`);
  const us = usageOf(jobId);
  ok(us.length === 3 && us.every((u) => u.status === 'charged' && u.cost_jpy === 8 && u.out_tokens === 439), '台帳に 1 枚ずつ実額で記録 (charged)');
  eq(li.monthUsage(db, T0).used_jpy, 24, '今月の使った額 = 実額の合計');
  const st = li.imageStateFor(db, { draft: A.draft, folderId: FOLDER, env: ENV, now: T0 });
  ok(st.job.status === 'done' && st.job.images.length === 3 && st.job.cost_jpy === 24, '画面に画像と費用が渡る');
  const evs = db.prepare('SELECT event FROM draft_events WHERE draft_id = ? ORDER BY id').all(A.draft.id).map((r) => r.event);
  ok(evs.includes('lp_image_requested') && evs.includes('lp_image_done'), '商品の履歴に残る');
  ok(dbmod.imageRefOfFileId(db, 'DRIVEOUT0001'), '🚨 作った画像はサムネイルの口で見せてよい (登録済みと同じ扱い)');
}

console.log('⑥ 作る係 — お金の上限で止める (fail-closed)');
{
  const B = makeComposed();
  const r = li.requestImageJob(db, { draft: B.draft, folderId: FOLDER, idempotencyKey: 'img-key-b001', env: ENV, now: T0 });
  ok(r.ok, '受け付ける');
  // 上限を実際の取り置き額から組む: 1 枚目 = 使った額 + 取り置き1 ≤ 上限 → 実額 8。2 枚目 = 使った額 + 8 + 取り置き2 ≤ 上限 → 実額 8。
  // 3 枚目 = 使った額 + 16 + 取り置き3 > 上限 で止まる
  const est = imagesOf(r.job.id).map((im) => im.est_jpy);
  const used0 = li.monthUsage(db, T0).used_jpy;
  const cap = Math.ceil(used0 + 8 + est[1]) + 1;
  ok(used0 + est[0] <= cap && used0 + 16 + est[2] > cap, `上限の組み立て (使った ${used0} 円・取り置き ${est.join(' / ')} 円・上限 ${cap} 円)`);
  // 呼んでよい額 = 上限 − 安全幅 (この大きさでは最低の 100 円)。cap を呼んでよい額にする
  const env = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(cap + 100) };
  eq(li.lpImageConfig(env).spendable, cap, '呼んでよい額 = cap');
  const { deps, calls } = fakeDeps({ env });
  await li.createLpImageWorker(deps).kick();
  const ims = imagesOf(r.job.id);
  eq(ims.map((im) => im.status), ['done', 'done', 'skipped'], '🚨 取り置きが上限を超える 1 枚は呼ばない (作った分は残す)');
  ok(/今月の上限/.test(ims[2].error) && calls.generate.length === 2, '理由を残す・API は 2 回だけ');
  eq(jobOf(r.job.id).status, 'partial', '依頼は一部だけ (partial)');
  ok(/今月の上限/.test(li.imageBlockReason(db, { draft: B.draft, folderId: FOLDER, env, now: T0 }) || ''), '上限に近ければ押す前に止める');
}

console.log('⑦ 作る係 — 失敗の費用の扱い');
{
  const C = makeComposed();
  const r = li.requestImageJob(db, { draft: C.draft, folderId: FOLDER, idempotencyKey: 'img-key-c001', env: ENV, now: T0 });
  let n = 0;
  const { deps } = fakeDeps({
    generate: async () => {
      n++;
      if (n === 1) throw Object.assign(new Error('blocked'), { status: 400, code: 'moderation_blocked' });
      if (n === 2) throw Object.assign(new Error('socket hang up'), { transient: true });
      return { buf: Buffer.from('PNG'), usage: null };
    },
  });
  await li.createLpImageWorker(deps).kick();
  const ims = imagesOf(r.job.id);
  const us = usageOf(r.job.id);
  ok(ims[0].status === 'failed' && /内容で断られました/.test(ims[0].error) && us[0].status === 'unknown' && us[0].cost_jpy === null,
    '🚨 内容で断られた (moderation) は出力の後のこともあるので取り置き額のまま数える (#1612 R1 High)・ほかの画像は続ける');
  ok(ims[1].status === 'failed' && us[1].status === 'unknown' && us[1].cost_jpy === null, '🚨 結果が分からない失敗 (通信断) は見込み額のまま数える');
  ok(ims[2].status === 'done' && us[2].status === 'charged' && us[2].cost_jpy === ims[2].est_jpy, '🚨 usage が無い成功は取り置き額 (最大の額) で記録');
  ok(us.every((u, k) => u.est_jpy === ims[k].est_jpy), '台帳の取り置き額 = 受付時に固定した額');
  eq(jobOf(r.job.id).status, 'partial', '一部だけ');
}

console.log('⑧ 作る係 — キーが使えない・混んでいる・Drive に保存できない');
{
  const D = makeComposed();
  const r = li.requestImageJob(db, { draft: D.draft, folderId: FOLDER, idempotencyKey: 'img-key-d001', env: ENV, now: T0 });
  const { deps, calls } = fakeDeps({ generate: async () => { throw Object.assign(new Error('bad key'), { status: 401 }); } });
  await li.createLpImageWorker(deps).kick();
  const ims = imagesOf(r.job.id);
  eq(ims.map((im) => im.status), ['failed', 'skipped', 'skipped'], '🚨 401 = キーが使えない → 残りは呼ばない');
  ok(/OPENAI_LP_IMAGE_API_KEY/.test(ims[0].error), '理由にキーの設定名');
  eq(jobOf(r.job.id).status, 'failed', '1 枚もできなければ failed');

  const E = makeComposed();
  const r2 = li.requestImageJob(db, { draft: E.draft, folderId: FOLDER, idempotencyKey: 'img-key-e001', env: ENV, now: T0 });
  let n = 0;
  let slept = 0;
  const f2 = fakeDeps({
    sleep: async () => { slept++; },
    generate: async () => { n++; if (n === 1) throw Object.assign(new Error('rate'), { status: 429 }); return { buf: Buffer.from('P'), usage: null }; },
    upload: async ({ name }) => { if (name.includes('_02')) throw new Error('drive down'); return 'DRIVEE' + name.length + 'XXXX'; },
  });
  await li.createLpImageWorker(f2.deps).kick();
  const ims2 = imagesOf(r2.job.id);
  ok(ims2[0].status === 'done' && slept === 1, '429 (混んでいる) は少し待ってやり直す');
  ok(ims2[1].status === 'failed' && /Drive に保存できませんでした/.test(ims2[1].error) && Number(ims2[1].cost_jpy) > 0, '作れたが保存できない = 失敗 (費用は払った)');
  eq(ims2[2].status, 'skipped', '🚨 保存できなければ残りは作らない (お金を使い続けない・#1612 R1 Medium)');
  eq(usageOf(r2.job.id)[1].status, 'charged', '保存できなくても台帳は charged');
}

console.log('⑧b 「AI初稿」は作る前に用意する・参考画像の差し替え (#1612 R1 Medium)');
{
  const H = makeComposed();
  const r = li.requestImageJob(db, { draft: H.draft, folderId: FOLDER, idempotencyKey: 'img-key-h001', env: ENV, now: T0 });
  const { deps, calls } = fakeDeps({ ensureFolder: async () => { throw new Error('insufficient permissions'); } });
  await li.createLpImageWorker(deps).kick();
  eq(calls.generate.length, 0, '🚨 「AI初稿」を作れなければ 1 枚も作らない (お金を使う前に止める)');
  eq(imagesOf(r.job.id).map((im) => im.status), ['failed', 'skipped', 'skipped'], '残りも止める');
  ok(usageOf(r.job.id).every((u) => u.status === 'failed' && u.cost_jpy === 0), 'まだ呼んでいないので 0 円');
  ok(/AI初稿/.test(imagesOf(r.job.id)[0].error), '理由を残す');

  const I = makeComposed(undefined, { images: [{ file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }] });
  const r2 = li.requestImageJob(db, { draft: I.draft, folderId: FOLDER, idempotencyKey: 'img-key-i001', env: ENV, now: T0 });
  const seenExpected = [];
  let n = 0;
  const f2 = fakeDeps({ fetchRef: async (id, opt) => { seenExpected.push(opt?.expectedModifiedTime); n++; if (n === 1) throw Object.assign(new Error('changed'), { changed: true }); return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  await li.createLpImageWorker(f2.deps).kick();
  ok(seenExpected.every((x) => x === '2026-10-01T00:00:00.000Z'), '参考画像は受付時の更新日時で照らす');
  const ims = imagesOf(r2.job.id);
  ok(ims[0].status === 'failed' && /差し替わりました/.test(ims[0].error) && usageOf(r2.job.id)[0].cost_jpy === 0, '🚨 差し替わっていたら作らない (0 円)');
  ok(ims.slice(1).every((im) => im.status === 'done'), 'ほかの画像は続ける');
}

console.log('⑧c 取り置きを超えた請求・保存の ID・時間の上限 (#1612 R2)');
{
  const K = makeComposed();
  const r = li.requestImageJob(db, { draft: K.draft, folderId: FOLDER, idempotencyKey: 'img-key-k001', env: ENV, now: T0 });
  const est = imagesOf(r.job.id).map((im) => im.est_jpy);
  const used0 = li.monthUsage(db, T0).used_jpy;
  // 1 枚目だけ取り置きの 3 倍かかったことにする → 2 枚目は呼んでよい額を超えて止まる
  const bigOut = Math.ceil((est[0] * 3) / 170 / 30 * 1_000_000);
  const capK = Math.ceil(used0 + est[0]) + 1;   // 1 枚目は呼べる / 1 枚目の実額 (取り置きの 3 倍) の後は 2 枚目を呼べない
  const envK = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(capK + 100) };
  let n = 0;
  const fk = fakeDeps({ env: envK, generate: async () => { n++; return { buf: Buffer.from('P'), usage: { input_tokens: 0, output_tokens: bigOut, input_tokens_details: { text_tokens: 0, image_tokens: 0 } } }; } });
  await li.createLpImageWorker(fk.deps).kick();
  const u = usageOf(r.job.id);
  ok(u[0].status === 'charged' && u[0].cost_jpy > est[0] && /取り置き/.test(u[0].error || ''), '🚨 取り置きを超えた請求も実額のまま正直に記録する (目印つき)');
  eq(imagesOf(r.job.id).map((im) => im.status), ['done', 'skipped', 'skipped'], '🚨 超えた分を数えて、次の 1 枚からは止まる');
  eq(n, 1, 'API は 1 回だけ');

  const L = makeComposed();
  const r2 = li.requestImageJob(db, { draft: L.draft, folderId: FOLDER, idempotencyKey: 'img-key-l001', env: ENV, now: T0 });
  const fl = fakeDeps({ upload: async () => undefined });
  await li.createLpImageWorker(fl.deps).kick();
  const imsL = imagesOf(r2.job.id);
  ok(imsL[0].status === 'failed' && /ID が返ってきませんでした/.test(imsL[0].error) && imsL[0].drive_file_id === null, '🚨 保存の ID が返らなければ「できました」にしない (#1612 R2 Low)');
  ok(imsL.slice(1).every((im) => im.status === 'skipped'), '残りは作らない');

  const M = makeComposed();
  const r3 = li.requestImageJob(db, { draft: M.draft, folderId: FOLDER, idempotencyKey: 'img-key-m001', env: ENV, now: T0 });
  const fm = fakeDeps({ timeouts: { ref: 30 }, fetchRef: () => new Promise(() => {}) });   // 返ってこない Drive
  const w = li.createLpImageWorker(fm.deps);
  await w.kick();
  ok(!w.isRunning(), '🚨 Drive が返ってこなくても作る係は止まり続けない (時間の上限・#1612 R2 Medium)');
  ok(imagesOf(r3.job.id).every((im) => im.status === 'failed' && /終わりませんでした/.test(im.error)), '時間切れは失敗として残す (呼ぶ前なので 0 円)');
  ok(usageOf(r3.job.id).every((x) => x.cost_jpy === 0), 'まだ呼んでいないので 0 円');
}

console.log('⑨ 再起動の片付け (recover)');
{
  const F = makeComposed();
  const r = li.requestImageJob(db, { draft: F.draft, folderId: FOLDER, idempotencyKey: 'img-key-f001', env: ENV, now: T0 });
  // 1 枚目を「作っている途中」で止まったことにする
  const first = imagesOf(r.job.id)[0];
  db.prepare(`UPDATE ph_lp_images SET status = 'running' WHERE id = ?`).run(first.id);
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'running' WHERE id = ?`).run(r.job.id);
  db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, ref_id, model, quality, status, est_jpy) VALUES ('lp_image', ?, ?, ?, 'gpt-image-2.5-flare', 'medium', 'reserved', 15)`)
    .run(li.jstMonth(T0), F.draft.id, first.id);
  // もう 1 つ: 期限内 (入れ替え中の古いプロセスがまだ作っている) は触らない
  const alive = imagesOf(r.job.id)[2];
  db.prepare(`UPDATE ph_lp_images SET status = 'running', claimed_by = 'w-old', lease_until = ? WHERE id = ?`).run(new Date(T0 + 5 * 60_000).toISOString(), alive.id);
  const { deps, calls } = fakeDeps();
  const w = li.createLpImageWorker(deps);
  w.recover();
  eq(imagesOf(r.job.id)[2].status, 'running', '🚨 期限内の「作っている途中」は中断にしない (#1612 R1 Medium)');
  db.prepare(`UPDATE ph_lp_images SET status = 'queued', claimed_by = NULL, lease_until = NULL WHERE id = ?`).run(alive.id);
  ok(imagesOf(r.job.id)[0].status === 'failed' && /途中で止まりました/.test(imagesOf(r.job.id)[0].error), '途中の画像は失敗 (自動で作り直さない)');
  eq(usageOf(r.job.id)[0].status, 'unknown', '🚨 費用は見込みのまま (請求されたものとして数える)');
  await w.kick();
  eq(imagesOf(r.job.id).map((im) => im.status), ['failed', 'done', 'done'], '残りは続けて作る');
  eq(calls.generate.length, 2, '途中の 1 枚は呼び直さない');
}

console.log('⑨b 再起動の直後に期限内で残った画像は、期限の少し後に 1 回だけ見回る (#1612 R4 Medium)');
{
  const W = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r = li.requestImageJob(db, { draft: W.draft, folderId: FOLDER, idempotencyKey: 'img-key-w001', env: ENV, now: T0 });
  const im = imagesOf(r.job.id)[0];
  // 前のプロセスが掴んだまま再起動した (期限は 15 分後)
  db.prepare(`UPDATE ph_lp_images SET status = 'running', claimed_by = 'w-old', lease_until = ? WHERE id = ?`).run(new Date(T0 + 15 * 60_000).toISOString(), im.id);
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'running' WHERE id = ?`).run(r.job.id);
  db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, ref_id, model, quality, status, est_jpy) VALUES ('lp_image', ?, ?, ?, 'gpt-image-2.5-flare', 'medium', 'reserved', 30)`).run(li.jstMonth(T0), W.draft.id, im.id);
  let nowMs = T0;
  const timers = [];
  const fw = fakeDeps({ now: () => nowMs, setTimer: (fn, ms) => { timers.push({ fn, ms }); return timers.length; }, clearTimer: () => {} });
  const w = li.createLpImageWorker(fw.deps);
  await w.kick();
  eq(imagesOf(r.job.id)[0].status, 'running', '期限内なので片付けない (前のプロセスがまだ作っているかもしれない)');
  ok(timers.length >= 1 && Math.abs(timers[timers.length - 1].ms - (15 * 60_000 + 5_000)) < 1_000, `🚨 いちばん早い期限の少し後に 1 回だけ起こす (${timers[timers.length - 1]?.ms} ms)`);
  nowMs = T0 + 15 * 60_000 + 6_000;   // 期限が切れた
  await timers[timers.length - 1].fn();
  await new Promise((res) => setImmediate(res));
  while (w.isRunning()) await new Promise((res) => setImmediate(res));
  ok(imagesOf(r.job.id)[0].status === 'failed' && /途中で止まりました/.test(imagesOf(r.job.id)[0].error), '🚨 起きたときに期限切れを片付ける (画面を開かなくても残り続けない)');
  eq(jobOf(r.job.id).status, 'failed', '依頼も終わる (もう一度押せる)');
  eq(usageOf(r.job.id)[0].status, 'unknown', '費用は取り置き額のまま (請求されたものとして)');
}

console.log('⑩ OpenAI の呼び方 (本物の部品を偽の fetch で)');
{
  let seen = null;
  const fakeFetch = async (url, init) => {
    seen = { url, init };
    return { ok: true, status: 200, json: async () => ({ data: [{ b64_json: Buffer.from('IMG').toString('base64') }], usage: { input_tokens: 10, output_tokens: 20 } }) };
  };
  const got = await li.openaiGenerateImage({ apiKey: 'sk-x', model: 'gpt-image-2.5-flare', quality: 'medium', size: '1200x1200', prompt: 'p',
    refs: [{ buf: Buffer.from('a'), mime: 'image/jpeg', filename: 'ref-1.jpg' }, { buf: Buffer.from('b'), mime: 'image/png', filename: 'ref-2.png' }] }, { fetchImpl: fakeFetch });
  ok(seen.url === 'https://api.openai.com/v1/images/edits' && seen.init.headers.Authorization === 'Bearer sk-x', '参考画像があれば /v1/images/edits');
  const fd = seen.init.body;
  ok(fd.getAll('image[]').length === 2 && fd.get('quality') === 'medium' && fd.get('size') === '1200x1200' && fd.get('model') === 'gpt-image-2.5-flare' && fd.get('output_format') === 'png',
    'multipart: image[] × 2・quality・size・model・png');
  ok(got.buf.toString() === 'IMG' && got.usage.output_tokens === 20, '画像と usage を返す');
  await li.openaiGenerateImage({ apiKey: 'sk-x', model: 'gpt-image-2.5-flare', quality: 'medium', size: '1200x1200', prompt: 'p', refs: [] }, { fetchImpl: fakeFetch });
  ok(seen.url === 'https://api.openai.com/v1/images/generations' && JSON.parse(seen.init.body).quality === 'medium', '参考画像が無ければ /v1/images/generations');
  const errFetch = async () => ({ ok: false, status: 400, json: async () => ({ error: { code: 'moderation_blocked', message: 'no' } }) });
  let e1 = null; try { await li.openaiGenerateImage({ apiKey: 'k', model: 'm', quality: 'medium', size: 's', prompt: 'p' }, { fetchImpl: errFetch }); } catch (e) { e1 = e; }
  ok(e1 && e1.status === 400 && e1.code === 'moderation_blocked' && !e1.transient, '断られたら status と code を付けて投げる');
  const netFetch = async () => { throw new Error('ECONNRESET'); };
  let e2 = null; try { await li.openaiGenerateImage({ apiKey: 'k', model: 'm', quality: 'medium', size: 's', prompt: 'p' }, { fetchImpl: netFetch }); } catch (e) { e2 = e; }
  ok(e2 && e2.transient === true, '通信断は「結果が分からない」として投げる');
}

console.log('⑪ 画面の口 (router)');
{
  process.env.PH_SERVICE_TOKEN = 'test-token-lp-image';
  const express = (await import('express')).default;
  const { default: router } = await import('../apps/product-hub/router.js');
  const app = express();
  app.use((req, _res, next) => { req.session = { email: 'nakahara@x', role: 'admin' }; next(); });
  app.use('/apps/product-hub', router);
  const server = app.listen(0);
  await new Promise((r) => server.once('listening', r));
  const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;
  const G = makeComposed();
  const st = await (await fetch(`${base}/api/drafts/${G.draft.id}/lp-images`)).json();
  ok(st.ok && st.enabled && st.blocked === null && st.planned_count === 3, 'GET: 押せる・作る枚数');
  const html = await (await fetch(`${base}/detail/${G.draft.id}`)).text();
  ok(html.includes('id="lpi-btn"') && html.includes('id="lpi-json"'), '詳細画面に「🖼 画像を作る」がある');
  const bad = await fetch(`${base}/api/drafts/${G.draft.id}/lp-images`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'x' }) });
  eq(bad.status, 400, 'POST: キーの形が違えば 400');
  const savedKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  const nBefore = db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE draft_id = ?').get(G.draft.id).n;
  const p502 = await fetch(`${base}/api/drafts/${G.draft.id}/lp-images`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'img-key-g001' }) });
  const j502 = await p502.json();
  ok(p502.status === 502 && j502.code === 'refs_unavailable', '🚨 参考画像の更新日時を Drive から取れなければ受け付けない (502)');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE draft_id = ?').get(G.draft.id).n, nBefore, '依頼は作られない');
  if (savedKey !== undefined) process.env.GOOGLE_SERVICE_ACCOUNT_KEY = savedKey;
  const thumb = await fetch(`${base}/api/thumb/NOTREGISTERED0001`);
  eq(thumb.status, 404, '登録されていない ID のサムネイルは見せない (今までどおり)');
  process.env.PH_LP_IMAGE_ENABLED = '';
  const html2 = await (await fetch(`${base}/detail/${G.draft.id}`)).text();
  ok(!html2.includes('id="lpi-btn"'), '機能フラグが無ければボタンを置かない');
  process.env.PH_LP_IMAGE_ENABLED = '1';
  server.close();
}

// ─── 画像制作の新フロー PR-E (2026-10-09 スタッフ要望 ⑤・設計 §3.6) ─────────
const ipOf = (draftId) => db.prepare('SELECT shoot_mode, material_status FROM draft_image_production WHERE draft_id = ?').get(draftId) || {};
const setMaterial = (draftId, m) => db.prepare('UPDATE draft_image_production SET material_status = ? WHERE draft_id = ?').run(m, draftId);
const draftImagesOf = (draftId) => db.prepare('SELECT COUNT(*) AS n FROM draft_images WHERE draft_id = ?').get(draftId).n;
const stateOf = (draft, env = ENV) => li.imageStateFor(db, { draft, folderId: FOLDER, env, now: T0 });

console.log('⑫ 作れる条件 — 撮影判定が決まっていること・撮影が要るなら素材完了 (設計 §3.6)');
{
  const U = makeComposed(undefined, { shootMode: 'unset' });   // 撮影判定がまだ
  const why = li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 });
  ok(/撮影判定がまだ/.test(why || '') && /撮影不要 \/ 社内撮影 \/ カメラマン撮影/.test(why || ''), `🚨 撮影判定がまだなら作らない・何を選べばよいかを出す (${why})`);
  const r0 = li.requestImageJob(db, { draft: U.draft, folderId: FOLDER, idempotencyKey: 'img-key-u001', env: ENV, now: T0 });
  ok(r0.code === 'not_ready' && /撮影判定/.test(r0.error), '🚨 API (受付) も同じ判定で断る');
  let st = stateOf(U.draft);
  eq(st.blocked, why, '画面に渡す押せない理由 = API の理由 (同じ判定)');
  eq(st.checklist.map((x) => [x.key, x.ok]), [['compose', false]], 'チェックリスト: 撮影判定がまだなら「仮LP構成と撮影判定ができている」は未了 (撮影が要るか分からないので素材の行は出さない)');

  dbmod.setShootMode(db, U.draft.id, 'inhouse', { actor: 't' });
  const why2 = li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 });
  ok(/社内撮影/.test(why2 || '') && /素材完了/.test(why2 || '') && /いま: 未設定/.test(why2 || ''), `🚨 社内撮影なら素材が揃うまで作らない・次にすること (素材完了にする) と今の値を出す (${why2})`);
  st = stateOf(U.draft);
  eq(st.checklist.map((x) => [x.key, x.ok]), [['compose', true], ['material', false]], 'チェックリスト: 構成と撮影判定は済・素材は未了');
  ok(st.checklist[1].text.includes('素材が揃った'), 'チェックリストの文 (ラフ「上のチェックがそろうと押せます」)');
  setMaterial(U.draft.id, 'internal_prep');
  ok(/いま: 社内準備/.test(li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 }) || ''), '素材完了の手前 (社内準備) でもまだ作らない');
  setMaterial(U.draft.id, 'ready');
  eq(li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 }), null, '素材完了になれば作れる');
  eq(stateOf(U.draft).checklist.every((x) => x.ok), true, 'チェックリストが全部そろう');

  dbmod.setShootMode(db, U.draft.id, 'none', { actor: 't' });
  dbmod.setShootMode(db, U.draft.id, 'photographer', { actor: 't' });   // 撮影不要の「素材完了」は撮影済みの印にならない (setShootMode が未設定に戻す)
  setMaterial(U.draft.id, 'shipped');
  ok(/カメラマン撮影/.test(li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 }) || '') && /商品発送済み/.test(li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 }) || ''),
    'カメラマン撮影も同じ (発送済みではまだ作らない)');
  dbmod.setShootMode(db, U.draft.id, 'none', { actor: 't' });
  eq(ipOf(U.draft.id).material_status, 'not_required', '撮影不要にすると素材ステータスは「撮影不要」(PR-A の決まり)');
  eq(li.imageBlockReason(db, { draft: U.draft, folderId: FOLDER, env: ENV, now: T0 }), null, '撮影不要ならそのまま作れる (素材を待たない)');
  eq(stateOf(U.draft).checklist.map((x) => x.key), ['compose'], '撮影不要なら素材の行は出さない');
  const noCompose = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('X2', 'x', 't')`).run().lastInsertRowid));
  dbmod.setShootMode(db, noCompose.id, 'none', { actor: 't' });
  eq(stateOf(noCompose).checklist.map((x) => x.ok), [false], '撮影判定だけ済んでいても構成が無ければ未了');
}

console.log('⑬ 1 枚ずつ作り直す (受付で固めた prompt と参考画像・予算台帳・版・Drive に新しいファイル)');
const P = makeComposed();
let pJob;
{
  const r = li.requestImageJob(db, { draft: P.draft, folderId: FOLDER, idempotencyKey: 'img-key-p001', actor: 'u@x', env: ENV, now: T0 });
  pJob = r.job.id;
  await li.createLpImageWorker(fakeDeps().deps).kick();
  const st = stateOf(P.draft);
  eq(st.cards.map((c) => [c.no, c.current?.version, c.checked, c.pending]), [[0, 1, false, false], [1, 1, false, false], [2, 1, false, false]], 'できた画像が 1 枚ずつのカード (v1・未確認)');
  ok(st.cards.every((c) => c.regen_blocked === null && c.regen_est_jpy === imagesOf(pJob).find((im) => im.id === c.root_id).est_jpy), '再生成を押せる・1 枚の取り置き額 = 受付時に固めた額');
  eq([st.checked_count, st.checkable_count], [0, 3], '確認済み 0 / 3');
  eq(st.job.ai_folder_id, 'AIFOLDER00001', '「Drive で開く」の AI初稿 フォルダ');
}
{
  const before = stateOf(P.draft);
  const target = before.cards[1];
  const root = imagesOf(pJob).find((im) => im.id === target.root_id);
  // 🚨 構成を作り直した (別の構成になった) 後でも、作り直しは受付で固めた prompt を使う (構成を読み直さない)
  db.prepare(`UPDATE ph_lp_compose_jobs SET output_text = ? WHERE id = ?`).run('# 1枚目｜別物\n## 画像の役割\n別物', P.composeJobId);
  const used0 = li.monthUsage(db, T0).used_jpy;
  const r = li.requestImageRegen(db, { draft: P.draft, imageId: target.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p001', actor: 'img@x', env: ENV, now: T0 });
  ok(r.ok && r.created && r.job.regen_of_image_id === root.id && r.job.status === 'queued', '1 枚だけの依頼を受け付ける (元の画像を指す)');
  const nim = imagesOf(r.job.id);
  ok(nim.length === 1 && nim[0].prompt === root.prompt && nim[0].refs_json === root.refs_json && nim[0].est_jpy === root.est_jpy && nim[0].version === 2,
    '🚨 prompt・参考画像 (更新日時ごと)・取り置き額は元の行のまま写す・版は v2');
  ok(r.job.model === jobOf(pJob).model && r.job.quality === jobOf(pJob).quality && r.job.ai_folder_id === 'AIFOLDER00001', '受付のときのモデル・品質段・AI初稿 フォルダで作る');
  eq(li.monthUsage(db, T0).used_jpy, used0, '受付ではまだ取り置かない (作る係が呼ぶ直前に 1 枚分を取り置く)');
  const again = li.requestImageRegen(db, { draft: P.draft, imageId: target.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p001', env: ENV, now: T0 });
  ok(again.ok && !again.created && again.job.id === r.job.id, '🚨 同じキーの再送 (二重押し・通信のやり直し) は前の依頼を返す');
  const stale = li.requestImageRegen(db, { draft: P.draft, imageId: target.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p002', env: ENV, now: T0 });
  ok(stale.code === 'conflict' && /もう作り直しています/.test(stale.error), '🚨 古い画面 (別の人が先に作り直した) から押しても 2 回目は作らない');
  const st = stateOf(P.draft);
  const pend = st.cards[1];
  const dup = li.requestImageRegen(db, { draft: P.draft, imageId: pend.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p003', env: ENV, now: T0 });
  ok(dup.code === 'already_running' && /いま作り直しています/.test(dup.error), '作っている途中の 1 枚はもう一度押せない');
  ok(pend.pending && pend.head_version === 2 && pend.regen_blocked && st.regen_running, '画面: 作り直し中 (v2 を作っている)');
  ok(/いま作っています/.test(li.imageBlockReason(db, { draft: P.draft, folderId: FOLDER, env: ENV, now: T0 }) || ''), '🚨 作り直しが動いている間は「全部作る」も押せない (動いている依頼は 1 つの作法)');
  eq(li.requestImageJob(db, { draft: P.draft, folderId: FOLDER, idempotencyKey: 'regen-key-p001', env: ENV, now: T0 }).code, 'bad_request', '作り直しのキーで「全部作る」を送っても作り直しの依頼を返さない');
  ok(st.cards[0].regen_blocked === null, 'ほかの 1 枚は同時に作り直せる (順番に作る)');
  eq(db.prepare(`SELECT event FROM draft_events WHERE draft_id = ? ORDER BY id DESC LIMIT 1`).get(P.draft.id).event, 'lp_image_regen_requested', '商品の履歴に残る');

  const { deps, calls } = fakeDeps();
  await li.createLpImageWorker(deps).kick();
  eq(calls.generate.length, 1, 'API は 1 回だけ (1 枚分)');
  ok(calls.generate[0].prompt === root.prompt.slice(0, 40) && calls.generate[0].refs === JSON.parse(root.refs_json).length, '元の prompt と参考画像で作る');
  ok(calls.ensure.length === 0 && calls.upload[0].folderId === 'AIFOLDER00001' && /_1枚目_v2\.png$/.test(calls.upload[0].name), `🚨 同じ「AI初稿」に新しいファイル (_v2) で保存 (${calls.upload[0].name})`);
  const u = db.prepare(`SELECT * FROM ph_ai_usage WHERE ref_id = ?`).all(nim[0].id);
  ok(u.length === 1 && u[0].status === 'charged' && u[0].est_jpy === root.est_jpy && u[0].cost_jpy === 8, '🚨 予算台帳に 1 枚分を取り置き → 実額で確定 (全部作るときと同じ台帳)');
  eq(li.monthUsage(db, T0).used_jpy, used0 + 8, '今月の使った額に足される');
  const st2 = stateOf(P.draft);
  const c1 = st2.cards[1];
  ok(c1.current.version === 2 && c1.versions === 2 && !c1.pending && c1.current.drive_file_id !== root.drive_file_id, '画面には最新の版 (v2)・版は 2 つ');
  ok(imagesOf(pJob).find((im) => im.id === root.id).drive_file_id === root.drive_file_id && dbmod.imageRefOfFileId(db, root.drive_file_id) && dbmod.imageRefOfFileId(db, c1.current.drive_file_id),
    '🚨 前の版のファイルも消さない (どちらもサムネイルの口で見せられる)');
  eq(draftImagesOf(P.draft.id), 0, '🚨 作った画像は draft_images (楽天・Amazon に流れる本番画像) に入れない (設計 §3.6・検討 §9-7)');
  eq(jobOf(r.job.id).status, 'done', '作り直しの依頼も done で閉じる');
  eq(st2.job.id, pJob, '画面の「生成した画像」は全部作ったときの依頼のまま (作り直しは版として重ねる)');
  ok(st2.cards[0].current.version === 1 && st2.cards[2].current.version === 1, 'ほかの画像は v1 のまま');
}

console.log('⑬b 確認済み (誰がいつ・作り直したら外れる・古い版は確認できない)');
{
  let st = stateOf(P.draft);
  const c0 = st.cards[0], c1 = st.cards[1];
  const r = li.setImageChecked(db, { draft: P.draft, imageId: c0.current.id, checked: true, actor: 'kakunin@x', now: T0 });
  ok(r.ok && r.changed, '確認を付ける');
  st = stateOf(P.draft);
  ok(st.cards[0].checked && st.cards[0].current.checked_by === 'kakunin@x' && st.cards[0].current.checked_at === new Date(T0).toISOString(), '誰がいつ確認したかを残す');
  eq([st.checked_count, st.checkable_count], [1, 3], '確認済み 1 / 3');
  ok(li.setImageChecked(db, { draft: P.draft, imageId: c0.current.id, checked: true, actor: 'x', now: T0 }).changed === false, '同じ操作の送り直しは変えない (確認した人は最初の人のまま)');
  const oldVer = imagesOf(pJob).find((im) => im.id === c1.root_id);
  const rOld = li.setImageChecked(db, { draft: P.draft, imageId: oldVer.id, checked: true, actor: 'x', now: T0 });
  ok(rOld.code === 'conflict' && /作り直されています/.test(rOld.error), '🚨 前の版 (v1) を出していた古い画面から確認しても、見ていない v2 を確認済みにしない');
  ok(li.setImageChecked(db, { draft: P.draft, imageId: c1.current.id, checked: true, actor: 'kakunin@x', now: T0 }).ok, '最新の版 (v2) は確認できる');
  eq(stateOf(P.draft).checked_count, 2, '確認済み 2 / 3');
  // 作り直したら確認は外れる
  const rr = li.requestImageRegen(db, { draft: P.draft, imageId: c1.current.id, folderId: FOLDER, idempotencyKey: 'regen-key-p010', actor: 'img@x', env: ENV, now: T0 });
  st = stateOf(P.draft);
  ok(rr.ok && !st.cards[1].checked && st.cards[1].current.checked_at === null && st.cards[0].checked, '🚨 作り直しを受け付けたら、その 1 枚の確認は外れる (ほかの画像の確認はそのまま)');
  const rPend = li.setImageChecked(db, { draft: P.draft, imageId: st.cards[1].current.id, checked: true, actor: 'x', now: T0 });
  ok(rPend.code === 'conflict' && /作り直しています/.test(rPend.error), '作り直している途中は確認できない');
  // 作り直しが失敗 (内容で断られた) → 前の版を出したまま理由を出す。前の版を確認し直せる・もう一度作り直せる
  const f = fakeDeps({ generate: async () => { throw Object.assign(new Error('blocked'), { status: 400, code: 'moderation_blocked' }); } });
  await li.createLpImageWorker(f.deps).kick();
  st = stateOf(P.draft);
  const cf = st.cards[1];
  ok(cf.current.version === 2 && cf.head_version === 3 && cf.head_status === 'failed' && /内容で断られました/.test(cf.head_error) && !cf.pending, '作り直せなかったら前の版 (v2) を出したまま、理由を出す');
  ok(li.setImageChecked(db, { draft: P.draft, imageId: cf.current.id, checked: true, actor: 'x', now: T0 }).ok, '前の版で良ければ確認し直せる');
  ok(li.setImageChecked(db, { draft: P.draft, imageId: cf.current.id, checked: false, actor: 'x', now: T0 }).changed && !stateOf(P.draft).cards[1].checked, '確認を外せる');
  ok(cf.regen_blocked === null, '失敗した版からもう一度作り直せる');
  eq(li.setImageChecked(db, { draft: P.draft, imageId: cf.current.id, checked: 'yes', actor: 'x', now: T0 }).code, 'bad_request', 'checked は true / false だけ');
  const other = makeComposed();
  eq(li.setImageChecked(db, { draft: other.draft, imageId: cf.current.id, checked: true, actor: 'x', now: T0 }).code, 'not_found', '🚨 ほかの商品の画像は確認できない');
  eq(li.requestImageRegen(db, { draft: other.draft, imageId: cf.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-o001', env: ENV, now: T0 }).code, 'not_found', '🚨 ほかの商品の画像は作り直せない');
  eq(li.requestImageRegen(db, { draft: P.draft, imageId: cf.head_id, folderId: FOLDER, idempotencyKey: 'x', env: ENV, now: T0 }).code, 'bad_request', 'キーの形');
}

console.log('⑬c 作り直しも上限・撮影の段取り・設定を通す (fail-closed)');
{
  let st = stateOf(P.draft);
  const c2 = st.cards[2];
  const est = c2.regen_est_jpy;
  const used = li.monthUsage(db, T0).used_jpy;
  // 呼んでよい額 (上限 − 安全幅 100 円) = 使った額 + 取り置き − 1 → 1 枚分が入らない
  const envTight = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(used + est - 1 + 100) };
  const r = li.requestImageRegen(db, { draft: P.draft, imageId: c2.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p020', env: envTight, now: T0 });
  ok(r.code === 'not_ready' && /今月の上限/.test(r.error) && new RegExp(`この 1 枚の取り置き 約 ${est} 円`).test(r.error), `🚨 1 枚分の取り置きが上限を超えるなら作り直さない (${r.error})`);
  ok(/今月の上限/.test(stateOf(P.draft, envTight).cards[2].regen_blocked || ''), '画面の再生成ボタンも同じ理由で押せない');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE idempotency_key = ?').get('regen-key-p020').n, 0, '依頼は作られない');
  const envOk = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(used + est + 100) };
  eq(stateOf(P.draft, envOk).cards[2].regen_blocked, null, 'ちょうど入るなら押せる');
  // 待ちの画像 (まだ取り置いていない) も数える: ほかの商品の「全部作る」が待っている
  const Q = makeComposed();
  const rq = li.requestImageJob(db, { draft: Q.draft, folderId: FOLDER, idempotencyKey: 'img-key-q001', env: ENV, now: T0 });
  ok(rq.ok, '(ほかの商品が待ち)');
  const r2 = li.requestImageRegen(db, { draft: P.draft, imageId: c2.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p021', env: envOk, now: T0 });
  ok(r2.code === 'not_ready' && /待ちの画像/.test(r2.error), `🚨 まだ取り置いていない待ちの画像の分も数える (${r2.error})`);
  db.prepare(`UPDATE ph_lp_images SET status = 'skipped' WHERE image_job_id = ?`).run(rq.job.id);
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'cancelled' WHERE id = ?`).run(rq.job.id);
  // 受付は通っても、作る係が掴むときにもう一度上限を見る (受付の後で使った額が増えた)
  const r3 = li.requestImageRegen(db, { draft: P.draft, imageId: c2.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p022', env: envOk, now: T0 });
  ok(r3.ok, '受け付ける');
  db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, ref_id, model, quality, status, est_jpy, cost_jpy) VALUES ('lp_image', ?, ?, NULL, 'gpt-image-2.5-flare', 'medium', 'charged', 5, 5)`).run(li.jstMonth(T0), Q.draft.id);
  const fw = fakeDeps({ env: envOk });
  await li.createLpImageWorker(fw.deps).kick();
  const im3 = imagesOf(r3.job.id)[0];
  ok(fw.calls.generate.length === 0 && im3.status === 'skipped' && /今月の上限/.test(im3.error), '🚨 作る係が掴むときに上限を超えるなら呼ばない (fail-closed・API は 0 回)');
  st = stateOf(P.draft);
  ok(st.cards[2].current.version === 1 && st.cards[2].head_status === 'skipped' && /今月の上限/.test(st.cards[2].head_error), '画面: 前の版を出したまま、作れなかった理由');
  // 設定が使えない・撮影の段取り・画像フォルダ
  ok(/PH_LP_IMAGE_ENABLED/.test(stateOf(P.draft, { ...ENV, PH_LP_IMAGE_ENABLED: '' }).cards[0].regen_blocked || ''), '機能フラグが無ければ作り直せない');
  dbmod.setShootMode(db, P.draft.id, 'inhouse', { actor: 't' });
  ok(/素材完了/.test(stateOf(P.draft).cards[0].regen_blocked || ''), '🚨 撮影が要ると判定を変えたら、素材が揃うまで作り直しも止める');
  eq(li.requestImageRegen(db, { draft: P.draft, imageId: stateOf(P.draft).cards[0].head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p030', env: ENV, now: T0 }).code, 'not_ready', 'API も同じ判定');
  dbmod.setShootMode(db, P.draft.id, 'none', { actor: 't' });
  ok(/画像フォルダ/.test(li.requestImageRegen(db, { draft: P.draft, imageId: stateOf(P.draft).cards[0].head_id, folderId: null, idempotencyKey: 'regen-key-p031', env: ENV, now: T0 }).error || ''), '画像フォルダが無ければ作り直せない');
  // あとで「全部作る」をしたら、前の依頼の画像は古い (作り直し・確認とも断る)
  const oldHead = stateOf(P.draft).cards[0];
  const rAll = li.requestImageJob(db, { draft: P.draft, folderId: FOLDER, idempotencyKey: 'img-key-p002', env: ENV, now: T0 });
  ok(rAll.ok, 'もう一度全部作る');
  // 全部作っている途中 (1 枚目はできた・残りは待ち) は、できた 1 枚も作り直せない (動いている依頼は 1 つの作法)
  const firstNew = imagesOf(rAll.job.id)[0];
  db.prepare(`UPDATE ph_lp_images SET status = 'done', drive_file_id = 'DRIVEMIDRUN01' WHERE id = ?`).run(firstNew.id);
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'running' WHERE id = ?`).run(rAll.job.id);
  const mid = stateOf(P.draft).cards[0];
  ok(!mid.pending && /いま全部作っています/.test(mid.regen_blocked || ''), `🚨 全部作っている途中は、できた 1 枚も作り直せない (${mid.regen_blocked})`);
  eq(li.requestImageRegen(db, { draft: P.draft, imageId: mid.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p035', env: ENV, now: T0 }).code, 'already_running', 'API も 409 (already_running)');
  await li.createLpImageWorker(fakeDeps().deps).kick();
  st = stateOf(P.draft);
  ok(st.job.id === rAll.job.id && st.cards.every((c) => c.current.version === 1 && !c.checked) && st.checked_count === 0, '全部作り直したら新しい画像 (v1・確認は付け直し)');
  ok((li.requestImageRegen(db, { draft: P.draft, imageId: oldHead.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p040', env: ENV, now: T0 }).error || '').includes('古くなっています')
    && li.setImageChecked(db, { draft: P.draft, imageId: oldHead.current.id, checked: true, actor: 'x', now: T0 }).code === 'conflict', '🚨 古い画面の前の依頼の画像は、作り直しも確認もできない');
  ok(st.compose_changed === false, '構成が変わっていなければ「構成が変わりました」は出さない');
  const P2 = makeComposed();
  li.requestImageJob(db, { draft: P2.draft, folderId: FOLDER, idempotencyKey: 'img-key-p201', env: ENV, now: T0 });
  await li.createLpImageWorker(fakeDeps().deps).kick();
  eq(stateOf(P2.draft).compose_changed, false, '(作ったばかりは構成と同じ)');
  // 画像を作った後で LP構成を直した (PR-B の編集版)。直した本文は lint を通った形のつもりで、見出しだけ変える
  const edited = compositionFor('ハッカ油スプレー').replace('サムネイル', 'サムネイル (直した)');
  db.prepare(`INSERT INTO ph_lp_compose_edits (draft_id, base_job_id, slots_json, output_text, edited_by) VALUES (?, ?, '[]', ?, 't')`).run(P2.draft.id, P2.composeJobId, edited);
  ok(stateOf(P2.draft).compose_changed === true, '画像を作った後で構成を直したら「もう一度作ると反映されます」を出す (LP構成の一覧と同じ決め方)');
  // 直した後で 1 枚だけ作り直しても、それは前の指示のままなので「反映済み」にしない
  const cP2 = stateOf(P2.draft).cards[0];
  const rr2 = li.requestImageRegen(db, { draft: P2.draft, imageId: cP2.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-p201', env: ENV, now: Date.now() + 60_000 });
  await li.createLpImageWorker(fakeDeps({ now: () => Date.now() + 60_000 }).deps).kick();
  ok(rr2.ok && stateOf(P2.draft).compose_changed === true, '🚨 直した後の 1 枚の作り直しは、構成が反映された印にしない (受付で固めた前の指示で作るため)');
}

console.log('⑬d Codex PR-E R1: AI初稿 を作り直しが用意した・取り置き済みを二重に数えない');
{
  // 全部作るときに「AI初稿」を用意できなかった (Drive の失敗・0 円) → 後で 1 枚の作り直しが用意した
  const N = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const rn = li.requestImageJob(db, { draft: N.draft, folderId: FOLDER, idempotencyKey: 'img-key-n001', env: ENV, now: T0 });
  await li.createLpImageWorker(fakeDeps({ ensureFolder: async () => { throw new Error('insufficient permissions'); } }).deps).kick();
  let st = stateOf(N.draft);
  ok(jobOf(rn.job.id).status === 'failed' && st.job.ai_folder_id === null && st.cards[0].current === null, '(全部作るは失敗・フォルダなし)');
  const rr = li.requestImageRegen(db, { draft: N.draft, imageId: st.cards[0].head_id, folderId: FOLDER, idempotencyKey: 'regen-key-n001', env: ENV, now: T0 });
  ok(rr.ok && rr.job.ai_folder_id === null, '作れなかった 1 枚も再生成で作れる (フォルダは作る係が用意する)');
  await li.createLpImageWorker(fakeDeps({ ensureFolder: async () => 'AIFOLDERNEW01' }).deps).kick();
  st = stateOf(N.draft);
  ok(st.cards[0].current && st.cards[0].current.version === 2 && st.job.ai_folder_id === 'AIFOLDERNEW01', '🚨 作り直しが用意した「AI初稿」を「Drive で開く」に出す');
  ok(st.job.status === 'failed' && st.flow_status === 'done' && st.flow_cost_jpy === 8 && st.job.cost_jpy === 0,
    '🚨 失敗した 1 枚を作り直せたら、画面の出来ぐあいは「できました」・費用も作り直しの分を足す (依頼の記録は failed のまま・Codex 名指し1 M)');
  // 取り置き済み (台帳に行がある) の画像は、待ちの画像として二重に数えない
  const cN = st.cards[0];
  const used = li.monthUsage(db, T0).used_jpy;
  const envJ = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(used + cN.regen_est_jpy + 100) };
  const Wq = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const rw = li.requestImageJob(db, { draft: Wq.draft, folderId: FOLDER, idempotencyKey: 'img-key-wq01', env: ENV, now: T0 });
  const wim = imagesOf(rw.job.id)[0];
  ok(/待ちの画像/.test(stateOf(N.draft, envJ).cards[0].regen_blocked || ''), '(待ちの画像は数える)');
  // 取り置き済み (reserved) の行がある待ちの画像: used_jpy に入っているので、待ちの額には数えない
  db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, ref_id, model, quality, status, est_jpy) VALUES ('lp_image', ?, ?, ?, 'gpt-image-2.5-flare', 'medium', 'reserved', ?)`).run(li.jstMonth(T0), Wq.draft.id, wim.id, wim.est_jpy);
  const used2 = li.monthUsage(db, T0).used_jpy;
  eq(used2, used + wim.est_jpy, '(取り置きは今月の使った額に入っている)');
  const envJ2 = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(used2 + cN.regen_est_jpy + 100) };
  eq(stateOf(N.draft, envJ2).cards[0].regen_blocked, null, '🚨 取り置き済みの画像は待ちの画像に数えない (used に入っている分を二重に数えない・Codex 名指し3 L)');
  ok(/今月の上限/.test(stateOf(N.draft, { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(used2 + cN.regen_est_jpy + 99) }).cards[0].regen_blocked || ''), '取り置きの分は used で数えている (1 円足りなければ止まる)');
  db.prepare(`UPDATE ph_ai_usage SET status = 'failed', cost_jpy = 0 WHERE ref_id = ?`).run(wim.id);
  db.prepare(`UPDATE ph_lp_images SET status = 'skipped' WHERE image_job_id = ?`).run(rw.job.id);
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'cancelled' WHERE id = ?`).run(rw.job.id);
}

console.log('⑬e Codex PR-E 名指し1: 受付の後で撮影の段取りが崩れたら、作る係も呼ばない');
{
  const S1 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r1 = li.requestImageJob(db, { draft: S1.draft, folderId: FOLDER, idempotencyKey: 'img-key-s101', env: ENV, now: T0 });
  ok(r1.ok, '(撮影不要で受け付けた)');
  dbmod.setShootMode(db, S1.draft.id, 'inhouse', { actor: 't' });   // 待っている間に「社内撮影」へ
  const f1 = fakeDeps();
  await li.createLpImageWorker(f1.deps).kick();
  const im1 = imagesOf(r1.job.id)[0];
  ok(f1.calls.generate.length === 0 && im1.status === 'skipped' && /素材完了/.test(im1.error || ''), '🚨 待っている間に撮影が要ると変えたら、掴むときに止める (API は呼ばない)');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_ai_usage WHERE ref_id = ?').get(im1.id).n, 0, '台帳にも取り置かない');
  const S2 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r2 = li.requestImageJob(db, { draft: S2.draft, folderId: FOLDER, idempotencyKey: 'img-key-s201', env: ENV, now: T0 });
  // 参考画像を Drive から取っている間に変わった
  const f2 = fakeDeps({ fetchRef: async (id) => { dbmod.setShootMode(db, S2.draft.id, 'photographer', { actor: 't' }); return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  await li.createLpImageWorker(f2.deps).kick();
  const im2 = imagesOf(r2.job.id)[0];
  const u2 = db.prepare('SELECT * FROM ph_ai_usage WHERE ref_id = ?').get(im2.id);
  ok(f2.calls.generate.length === 0 && im2.status === 'failed' && /カメラマン撮影/.test(im2.error || '') && u2.status === 'failed' && u2.cost_jpy === 0,
    '🚨 参考画像を取っている間に変わっても、呼ぶ直前に止める (0 円で確定)');
}

console.log('⑬f Codex PR-E 名指し2: 出力 0 の usage・掴んでから呼ぶまでに月をまたいだ');
{
  eq(li.costJpyFromUsage({ input_tokens: 0, output_tokens: 0, input_tokens_details: { text_tokens: 0, image_tokens: 0 } }), null, '🚨 出力 0 の usage は使わない (取り置き額のまま・0 円で全部戻さない)');
  const NOV = Date.parse('2026-10-31T15:00:00Z');   // JST 2026-11-01 00:00
  // 参考画像を取っている間に月が変わった → 取り置きを新しい月へ移して作る
  const M1 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r1 = li.requestImageJob(db, { draft: M1.draft, folderId: FOLDER, idempotencyKey: 'img-key-mo01', env: ENV, now: T0 });
  let nowMs = T0;
  const f1 = fakeDeps({ now: () => nowMs, fetchRef: async () => { nowMs = NOV; return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  await li.createLpImageWorker(f1.deps).kick();
  const im1 = imagesOf(r1.job.id)[0];
  const u1 = db.prepare('SELECT * FROM ph_ai_usage WHERE ref_id = ?').get(im1.id);
  ok(im1.status === 'done' && u1.month === '2026-11' && u1.status === 'charged', `🚨 呼んだ月 (新しい月) に数える (${u1.month})`);
  // 新しい月の上限がもう埋まっていたら呼ばない (0 円)
  const M2 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r2 = li.requestImageJob(db, { draft: M2.draft, folderId: FOLDER, idempotencyKey: 'img-key-mo02', env: ENV, now: T0 });
  db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, model, quality, status, est_jpy, cost_jpy) VALUES ('lp_image', '2026-11', ?, 'gpt-image-2.5-flare', 'medium', 'charged', 2840, 2840)`).run(M2.draft.id);
  nowMs = T0;
  const f2 = fakeDeps({ now: () => nowMs, fetchRef: async () => { nowMs = NOV; return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  await li.createLpImageWorker(f2.deps).kick();
  const im2 = imagesOf(r2.job.id)[0];
  const u2 = db.prepare('SELECT * FROM ph_ai_usage WHERE ref_id = ?').get(im2.id);
  ok(f2.calls.generate.length === 0 && im2.status === 'failed' && /月が変わりました/.test(im2.error || '') && u2.status === 'failed' && u2.cost_jpy === 0,
    '🚨 移した先の月で上限を超えるなら呼ばない (0 円で確定)');
  db.prepare(`DELETE FROM ph_ai_usage WHERE month = '2026-11' AND draft_id IN (?, ?)`).run(M1.draft.id, M2.draft.id);   // 後の試験 (実際の時計) に混ぜない
}

console.log('⑬g Codex PR-E 名指し3: 429 の待ちの間に撮影の段取りが崩れた・古いタブの「全部作る」');
{
  const S3 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r3 = li.requestImageJob(db, { draft: S3.draft, folderId: FOLDER, idempotencyKey: 'img-key-s301', env: ENV, now: T0 });
  let n3 = 0;
  const f3 = fakeDeps({
    sleep: async () => { dbmod.setShootMode(db, S3.draft.id, 'inhouse', { actor: 't' }); },
    generate: async () => { n3++; throw Object.assign(new Error('rate'), { status: 429 }); },
  });
  await li.createLpImageWorker(f3.deps).kick();
  const im3 = imagesOf(r3.job.id)[0];
  ok(n3 === 1 && im3.status === 'failed' && /素材完了/.test(im3.error || ''), '🚨 429 の待ちの間に撮影が要ると変えたら、やり直しで呼ばない');
  // 古いタブ: 画面が見ていた「全部作る」の依頼と今の依頼が違えば断る
  const O = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const o1 = li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o101', expectedJobId: null, env: ENV, now: T0 });
  ok(o1.ok && o1.created, '初めての「全部作る」(画面は依頼なしを見ていた)');
  await li.createLpImageWorker(fakeDeps().deps).kick();
  const o2 = li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o102', expectedJobId: null, env: ENV, now: T0 });
  ok(o2.code === 'conflict' && /先に画像を作りました/.test(o2.error), '🚨 依頼なしを見ていた古いタブからは、もう一度全部作らない');
  eq(li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o103', expectedJobId: o1.job.id - 1, env: ENV, now: T0 }).code, 'conflict', '前の依頼を見ていた古いタブも断る');
  const again = li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o101', expectedJobId: null, env: ENV, now: T0 });
  ok(again.ok && !again.created && again.job.id === o1.job.id, '同じキーの送り直しは、古い expected でも前の依頼を返す (通信のやり直し)');
  const o4 = li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o104', expectedJobId: o1.job.id, env: ENV, now: T0 });
  ok(o4.ok && o4.created, '今の依頼を見ていればもう一度全部作れる');
  await li.createLpImageWorker(fakeDeps().deps).kick();
  // ほかの人が 1 枚作り直した後の古い画面 (全部作るの依頼は同じ) からも、全部はもう一度作らない (Codex 名指し5 M)
  const before = stateOf(O.draft);
  eq(before.latest_job_id, o4.job.id, '(画面が見ている最新の依頼)');
  const ro = li.requestImageRegen(db, { draft: O.draft, imageId: before.cards[0].head_id, folderId: FOLDER, idempotencyKey: 'regen-key-o401', env: ENV, now: T0 });
  await li.createLpImageWorker(fakeDeps().deps).kick();
  const nO = db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE draft_id = ?').get(O.draft.id).n;
  const o5 = li.requestImageJob(db, { draft: O.draft, folderId: FOLDER, idempotencyKey: 'img-key-o105', expectedJobId: before.latest_job_id, env: ENV, now: T0 });
  ok(ro.ok && o5.code === 'conflict' && db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE draft_id = ?').get(O.draft.id).n === nO && stateOf(O.draft).latest_job_id === ro.job.id,
    '🚨 ほかの人が 1 枚作り直した後の古い画面からは、全部をもう一度作らない (最新の依頼 = 作り直しの依頼)');
}

console.log('⑬h Codex PR-E 名指し4: 片付けられた後に戻ってきた作る係・掴んだ後で上限に達した');
{
  const waitFor = async (cond) => { for (let k = 0; k < 500 && !cond(); k++) await new Promise((res) => setTimeout(res, 2)); };
  // 作る係 A が参考画像の途中で止まり、期限切れで B が片付けた後に A が戻ってきた
  const L1 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const rl = li.requestImageJob(db, { draft: L1.draft, folderId: FOLDER, idempotencyKey: 'img-key-l101', env: ENV, now: T0 });
  let release; const gate = new Promise((res) => { release = res; }); let atRef = false;
  const fa = fakeDeps({ fetchRef: async () => { atRef = true; await gate; return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  const pA = li.createLpImageWorker(fa.deps).kick();
  await waitFor(() => atRef);
  const fb = fakeDeps({ now: () => T0 + 20 * 60_000 });
  li.createLpImageWorker(fb.deps).recover();
  const iml = imagesOf(rl.job.id)[0];
  ok(iml.status === 'failed' && /途中で止まりました/.test(iml.error), '(B が期限切れを片付けた)');
  release(); await pA;
  const ul = db.prepare('SELECT * FROM ph_ai_usage WHERE ref_id = ?').get(iml.id);
  ok(fa.calls.generate.length === 0 && imagesOf(rl.job.id)[0].status === 'failed' && ul.status === 'unknown',
    '🚨 片付けられた後に戻ってきた作る係は呼ばない (台帳は片付けた側の記録のまま)');
  // A が掴んで取り置いた後、ほかの画像の実額が取り置きを超えて上限に達した → A は呼ばない
  const L2 = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  const r2 = li.requestImageJob(db, { draft: L2.draft, folderId: FOLDER, idempotencyKey: 'img-key-l201', env: ENV, now: T0 });
  let release2; const gate2 = new Promise((res) => { release2 = res; }); let atRef2 = false;
  const fa2 = fakeDeps({ fetchRef: async () => { atRef2 = true; await gate2; return { buf: Buffer.from('r'), mime: 'image/jpeg' }; } });
  const pA2 = li.createLpImageWorker(fa2.deps).kick();
  await waitFor(() => atRef2);
  const cap = li.lpImageConfig(ENV).spendable;
  const over = cap - li.monthUsage(db, T0).used_jpy + 1;
  const overId = Number(db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, model, quality, status, est_jpy, cost_jpy) VALUES ('lp_image', ?, ?, 'gpt-image-2.5-flare', 'medium', 'charged', ?, ?)`).run(li.jstMonth(T0), L2.draft.id, over, over).lastInsertRowid);
  release2(); await pA2;
  const im2 = imagesOf(r2.job.id)[0];
  const u2 = db.prepare('SELECT * FROM ph_ai_usage WHERE ref_id = ?').get(im2.id);
  ok(fa2.calls.generate.length === 0 && im2.status === 'failed' && /今月の上限/.test(im2.error || '') && u2.status === 'failed' && u2.cost_jpy === 0,
    '🚨 掴んだ後で上限に達したら、呼ぶ直前に止める (0 円で確定)');
  db.prepare('DELETE FROM ph_ai_usage WHERE id = ?').run(overId);   // 後の試験に混ぜない
}

console.log('⑬i Codex PR-E 名指し6: 全部作るが失敗して 1 枚の作り直しでできたときの「構成が変わりました」');
{
  const failFolder = fakeDeps({ ensureFolder: async () => { throw new Error('insufficient permissions'); } });
  const LATER = Date.now() + 60_000;   // 直した (編集版の作成時刻 = 今) より後
  const editOf = (d, jobId) => db.prepare(`INSERT INTO ph_lp_compose_edits (draft_id, base_job_id, slots_json, output_text, edited_by) VALUES (?, ?, '[]', ?, 't')`)
    .run(d.id, jobId, compositionFor('ハッカ油スプレー').replace('サムネイル', 'サムネイル (直した)'));
  // A: 前の構成で作れた → 構成を直す → 直した構成で全部作るが失敗 → その 1 枚を作り直してできた = いま出ているのは直した構成の画像
  const FA = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  li.requestImageJob(db, { draft: FA.draft, folderId: FOLDER, idempotencyKey: 'img-key-fa01', env: ENV, now: T0 });
  await li.createLpImageWorker(fakeDeps().deps).kick();
  editOf(FA.draft, FA.composeJobId);
  ok(stateOf(FA.draft).compose_changed === true, '(直した後は「構成が変わりました」)');
  const ra2 = li.requestImageJob(db, { draft: FA.draft, folderId: FOLDER, idempotencyKey: 'img-key-fa02', env: ENV, now: LATER });
  await li.createLpImageWorker(failFolder.deps).kick();
  ok(ra2.ok && jobOf(ra2.job.id).status === 'failed', '(直した構成で全部作るは失敗)');
  const ca = stateOf(FA.draft).cards[0];
  li.requestImageRegen(db, { draft: FA.draft, imageId: ca.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-fa01', env: ENV, now: LATER });
  await li.createLpImageWorker(fakeDeps({ now: () => LATER }).deps).kick();
  const sa = stateOf(FA.draft);
  ok(sa.cards[0].current && sa.compose_changed === false, '🚨 失敗した全部作るの画像を作り直してできたら、その依頼 (直した構成) を手がかりにする (「構成が変わりました」を出さない)');
  // B: 全部作るが失敗 → 構成を直す → 前の指示のまま 1 枚作り直してできた = いま出ているのは前の構成の画像
  const FB = makeComposed(undefined, { priority: '仕入商品（重要度：低）' });
  li.requestImageJob(db, { draft: FB.draft, folderId: FOLDER, idempotencyKey: 'img-key-fb01', env: ENV, now: T0 });
  await li.createLpImageWorker(failFolder.deps).kick();
  editOf(FB.draft, FB.composeJobId);
  const cb = stateOf(FB.draft).cards[0];
  li.requestImageRegen(db, { draft: FB.draft, imageId: cb.head_id, folderId: FOLDER, idempotencyKey: 'regen-key-fb01', env: ENV, now: LATER });
  await li.createLpImageWorker(fakeDeps({ now: () => LATER }).deps).kick();
  const sb = stateOf(FB.draft);
  ok(sb.cards[0].current && sb.compose_changed === true, '🚨 直す前の指示のまま作り直した画像しか無ければ「構成が変わりました」を出す');
}

console.log('⑭ 画面の口 (router・本番の経路) と権限 (管理者ではなく実際に押す役割で)');
let uiServer, uiBase, uiSession;
{
  const wf = await import('../apps/product-hub/lib/workflow.js');
  const express = (await import('express')).default;
  const { default: router } = await import('../apps/product-hub/router.js');
  const app = express();
  uiSession = { email: 'nakahara@x', role: 'admin' };
  app.use((req, _res, next) => { req.session = uiSession; next(); });
  app.use('/apps/product-hub', router);
  uiServer = app.listen(0);
  await new Promise((r) => uiServer.once('listening', r));
  uiBase = `http://127.0.0.1:${uiServer.address().port}`;
  const noRole = wf.createStaff({ name: '画像 役割なし', kind: 'internal', portal_email: 'lpi-norole@b-faith.biz' });
  const imgStaff = wf.createStaff({ name: '画像 画像登録者', kind: 'internal', portal_email: 'lpi-img@b-faith.biz' });
  db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(imgStaff);
  const R = makeComposed();
  li.requestImageJob(db, { draft: R.draft, folderId: FOLDER, idempotencyKey: 'img-key-r001', env: ENV, now: Date.now() });
  await li.createLpImageWorker(fakeDeps({ now: () => Date.now() }).deps).kick();
  const api = `${uiBase}/apps/product-hub/api/drafts/${R.draft.id}/lp-images`;
  const post = async (url, body) => { const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: res.status, json: await res.json() }; };
  let st = await (await fetch(api)).json();
  ok(st.ok && st.cards.length === 3 && st.can_edit === true && Array.isArray(st.checklist), 'GET: カード・チェックリスト・押せる人か');
  const c0 = st.cards[0];
  const nJobs = () => db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE draft_id = ?').get(R.draft.id).n;
  const jobs0 = nJobs();
  uiSession = { email: 'lpi-norole@b-faith.biz', displayName: '役割なし', role: 'user' };
  const f1 = await post(`${api}/${c0.current.id}/check`, { checked: true });
  const f2 = await post(`${api}/${c0.head_id}/regenerate`, { idempotency_key: 'regen-key-r001' });
  const f3 = await post(api, { idempotency_key: 'img-key-r002' });
  const html0 = await (await fetch(`${uiBase}/apps/product-hub/detail/${R.draft.id}`)).text();
  ok(f1.status === 403 && f2.status === 403 && f3.status === 403 && /画像登録者/.test(f2.json.error || ''), '🚨 画像の役割が無い担当者は 確認・再生成・全部作る とも 403');
  ok(nJobs() === jobs0 && !stateOf(R.draft).cards[0].checked, '値は変わらない・依頼も作られない');
  const lpiJson0 = JSON.parse((html0.match(/<script type="application\/json" id="lpi-json">([\s\S]*?)<\/script>/) || [])[1] || 'null');
  ok(lpiJson0 && lpiJson0.can_edit === false, '画面 (画像の箱の初期状態) にも「押せない人」と渡す (ボタンを押せない形で出す)');
  uiSession = { email: 'lpi-img@b-faith.biz', displayName: '画像登録者', role: 'user' };
  const okc = await post(`${api}/${c0.current.id}/check`, { checked: true });
  ok(okc.status === 200 && okc.json.cards[0].checked && okc.json.checked_count === 1 && okc.json.cards[0].current.checked_by === 'lpi-img@b-faith.biz', '画像登録者 (管理者でない) は確認できる (誰が = ポータルのメール)');
  const bad = await post(`${api}/${c0.current.id}/check`, { checked: 'true' });
  const badKey = await post(`${api}/${c0.head_id}/regenerate`, { idempotency_key: 'x' });
  const nf = await post(`${api}/99999999/regenerate`, { idempotency_key: 'regen-key-r404' });
  ok(bad.status === 400 && badKey.status === 400 && nf.status === 404, '形の違う値は 400・無い画像は 404');
  const nJ = nJobs();
  const fe1 = await post(api, { idempotency_key: 'img-key-r010', expected_job_id: 'x' });
  const fe2 = await post(api, { idempotency_key: 'img-key-r011', expected_job_id: null });
  ok(fe1.status === 400 && fe2.status === 409 && fe2.json.code === 'conflict' && nJobs() === nJ, '🚨 全部作る: expected_job_id の形が違えば 400・画面が見ていた依頼と違えば 409 (Drive を読む前・依頼は作らない)');
  const fe3 = await post(api, { idempotency_key: 'img-key-r012' });
  ok(fe3.status === 409 && /画面が古い/.test(fe3.json.error || '') && nJobs() === nJ, '🚨 全部作る: expected_job_id を送ってこない古い画面は、もう作った依頼があれば 409 (Codex 名指し4 H)');
  // 本番の作る係は Drive の鍵が無いので参考画像の段で止まる (OpenAI は呼ばない・0 円)
  const savedKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  const rg = await post(`${api}/${c0.head_id}/regenerate`, { idempotency_key: 'regen-key-r005' });
  ok(rg.status === 200 && rg.json.created === true && rg.json.cards[0].head_version === 2 && !rg.json.cards[0].checked, '画像登録者は再生成できる (v2・確認は外れる)');
  const rg2 = await post(`${api}/${c0.head_id}/regenerate`, { idempotency_key: 'regen-key-r006' });
  ok(rg2.status === 409 && rg2.json.code === 'conflict', '🚨 古い画面 (v1 を出したまま) からの 2 回目は 409');
  const regenImg = () => db.prepare(`SELECT i.* FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id WHERE j.idempotency_key = 'regen-key-r005'`).get();
  for (let k = 0; k < 200 && ['queued', 'running'].includes(regenImg().status); k++) await new Promise((res) => setTimeout(res, 10));
  const gi = regenImg();
  ok(gi.status === 'failed' && /参考画像/.test(gi.error || '') && db.prepare('SELECT cost_jpy FROM ph_ai_usage WHERE ref_id = ?').get(gi.id)?.cost_jpy === 0,
    '本番の経路: 作る係が台帳に取り置いて動く (この試験では Drive が無いので呼ぶ前に止まり 0 円)');
  if (savedKey !== undefined) process.env.GOOGLE_SERVICE_ACCOUNT_KEY = savedKey;
  uiSession = { email: 'nakahara@x', role: 'admin' };
  const html = await (await fetch(`${uiBase}/apps/product-hub/detail/${R.draft.id}`)).text();
  ok(['id="lpi-checks"', 'id="lpi-head"', 'id="lpi-count"', 'id="lpi-checked"', 'id="lpi-folder"', 'id="lpi-stale"', 'id="lpi-grid"'].every((s) => html.includes(s)), '詳細画面に チェックリスト・生成した画像・確認済み・Drive で開く の置き場');
  ok(html.indexOf('id="lpi"') > html.indexOf('id="ipf-step-gen"') && html.indexOf('id="lpi"') < html.indexOf('id="ipf-step-manage"'), '置き場は「4 AI画像生成」の段の中');
  globalThis.__lpiDraft = R.draft;
}

console.log('⑮ 画面の JS (印の間を切り出して偽の document で動かす・送り先は本番の router)');
{
  const fsm = await import('node:fs');
  const pathm = await import('node:path');
  const src = fsm.readFileSync(pathm.join(pathm.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8');
  const chunk = src.slice(src.indexOf('/* @lp-image-ui:start'), src.indexOf('/* @lp-image-ui:end */'));
  ok(chunk.length > 1000 && !chunk.includes('<%'), '画面の JS を切り出せる (EJS の値を含まない)');
  const ui = new Function(chunk + '\nreturn { lpiView, lpiCardView, initLpImages, lpiRunning };')();

  // 純粋関数: 押せない理由とチェックリスト
  const base = { enabled: true, usable: true, model: 'gpt-image-2.5-flare', quality: 'medium', budget_jpy: 3000, used_jpy: 35, planned_count: 3, planned_reserve_jpy: 90, can_edit: true, job: null, cards: [], checked_count: 0, checkable_count: 0 };
  let v = ui.lpiView({ ...base, blocked: '撮影判定がまだです。…', checklist: [{ key: 'compose', ok: false, text: '仮LP構成と撮影判定ができている' }] });
  ok(v.mainDisabled && v.statusText === '上のチェックがそろうと押せます。撮影判定がまだです。…' && v.checks[0].ok === false, '🚨 チェックがそろわなければ押せない・「上のチェックがそろうと押せます」+ 理由');
  v = ui.lpiView({ ...base, blocked: '今月の上限 …', checklist: [{ key: 'compose', ok: true, text: 'x' }] });
  ok(v.mainDisabled && v.statusText === '今月の上限 …', 'チェックがそろっていて押せないときは理由だけ');
  v = ui.lpiView({ ...base, blocked: null, checklist: [{ ok: true, text: 'x' }] });
  ok(!v.mainDisabled && v.mainLabel === '🖼 画像を作る (3 枚・取り置き 約 90 円)' && v.metaText.includes('今月 35 円 / 上限 3,000 円'), '押せる・枚数と取り置き額・今月の使用額 / 上限');
  v = ui.lpiView({ ...base, blocked: null, checklist: [], job: { status: 'failed', images: [], cost_jpy: 0 }, flow_status: 'done', flow_cost_jpy: 8 });
  ok(v.statusText.startsWith('画像を押すと Drive で開きます') && v.metaText.includes('この画像で 8 円'), '🚨 出来ぐあいと費用はカードの最新の版で出す (全部作るが失敗でも、作り直せたら「できました」)');
  v = ui.lpiView({ ...base, blocked: null, can_edit: false, checklist: [] });
  ok(v.mainDisabled && /画像登録者/.test(v.statusText), '役割の無い人には押せない形で理由を出す');
  const cardBase = { root_id: 5, seq: 2, no: 1, name: 'FV', head_id: 9, head_version: 2, head_status: 'done', head_error: null, pending: false, checked: false, regen_est_jpy: 41, regen_blocked: null,
    current: { id: 9, version: 2, drive_file_id: 'DRVFILE00009', checked_at: null, checked_by: null } };
  let cv = ui.lpiCardView(cardBase, base, false);
  ok(cv.label === '1枚目' && cv.role === 'FV' && cv.versionText === 'v2' && cv.checkLabel === '確認' && cv.regenLabel === '再生成 (約 41 円)' && !cv.checkDisabled && !cv.regenDisabled
    && cv.openUrl === 'https://drive.google.com/file/d/DRVFILE00009/view' && cv.imgSrc.includes('/api/thumb/DRVFILE00009'), 'カード: 番号・役割・版・確認・再生成 (約 N 円)・Drive で開く');
  cv = ui.lpiCardView({ ...cardBase, pending: true, head_version: 3 }, base, false);
  ok(cv.regenLabel === '作成中…' && cv.regenDisabled && cv.checkDisabled && cv.placeholder === '作り直しています…' && !cv.imgSrc, '作り直し中は 作成中… で押せない');
  cv = ui.lpiCardView({ ...cardBase, head_version: 3, head_status: 'failed', head_error: '内容で断られました' }, base, false);
  ok(cv.error === '作り直せませんでした: 内容で断られました' && cv.imgSrc && !cv.checkDisabled, '作り直せなかったら前の版を出したまま理由');
  cv = ui.lpiCardView({ ...cardBase, regen_blocked: '今月の上限 …' }, base, false);
  ok(cv.regenDisabled && cv.regenTitle === '今月の上限 …', '再生成を押せない理由をボタンに出す');
  cv = ui.lpiCardView({ ...cardBase, checked: true, current: { ...cardBase.current, checked_by: 'a@x', checked_at: '2026-10-09T01:02:03Z' } }, base, false);
  ok(cv.checkLabel === '✓ 確認済み' && cv.checkTitle.includes('a@x') && cv.checkTitle.includes('2026-10-09 01:02'), '確認済み (誰がいつ)');
  cv = ui.lpiCardView({ ...cardBase, current: null, head_version: 1, head_status: 'failed', head_error: '作れませんでした (500)' }, base, false);
  ok(cv.checkDisabled && !cv.regenDisabled && cv.error === '作れませんでした (500)' && cv.placeholder === '作れませんでした', '1 枚もできなかった画像は確認できない・再生成で作れる');

  // 偽の document に載せて、本番の router へ送る
  const mk = (tag) => {
    const e = {
      tagName: String(tag).toUpperCase(), children: [], style: {}, dataset: {}, hidden: false, disabled: false, listeners: {}, _text: '',
      get textContent() { return this._text + this.children.map((c) => c.textContent).join(''); },
      set textContent(x) { this._text = String(x); this.children = []; },
      appendChild(c) { this.children.push(c); return c; },
      addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); },
      async click() { for (const fn of this.listeners.click || []) await fn(); },
    };
    return e;
  };
  const all = (e, pred, out = []) => { if (pred(e)) out.push(e); for (const c of e.children) all(c, pred, out); return out; };
  const R = globalThis.__lpiDraft;
  const ids = ['lpi', 'lpi-json', 'lpi-btn', 'lpi-status', 'lpi-meta', 'lpi-grid', 'lpi-checks', 'lpi-head', 'lpi-count', 'lpi-checked', 'lpi-folder', 'lpi-stale'];
  const els = Object.fromEntries(ids.map((id) => [id, mk('div')]));
  els.lpi.dataset.draftId = String(R.id);
  const initial = await (await fetch(`${uiBase}/apps/product-hub/api/drafts/${R.id}/lp-images`)).json();
  els['lpi-json']._text = JSON.stringify(initial);
  const sent = []; const timers = []; let keyN = 0; let net = 'ok'; const asked = [];
  const doc = { getElementById: (id) => els[id] || null, createElement: mk };
  const ctl = ui.initLpImages(doc, {
    fetchJson: async (url, opt) => {
      sent.push([url, opt ? opt.body : null]);
      if (net === 'throw') throw new Error('net');
      const res = await fetch(uiBase + url, opt && opt.method === 'POST' ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(opt.body) } : {});
      return { status: res.status, json: await res.json() };
    },
    confirm: (m) => { asked.push(m); return false; },
    setTimer: (fn, ms) => { timers.push({ fn, ms }); return timers.length; }, clearTimer: () => {},
    newKey: () => 'ui-key-' + String(++keyN).padStart(4, '0'), isHidden: () => false,
  });
  const cardsEl = () => els['lpi-grid'].children;
  const btnOf = (k, act) => all(cardsEl()[k], (e) => e.dataset.lpiAct === act)[0];
  ok(cardsEl().length === 3 && !els['lpi-head'].hidden, '画面: 生成した画像のカードが 3 枚・見出しを出す');
  ok(els['lpi-checked'].textContent === '確認済み 0 / 3' && els['lpi-count'].textContent.startsWith('できました'), `画面: 確認済み N/M (${els['lpi-checked'].textContent})`);
  ok(!els['lpi-folder'].hidden && els['lpi-folder'].href === 'https://drive.google.com/drive/folders/AIFOLDER00001', '画面: Drive で開く (AI初稿)');
  ok(els['lpi-checks'].children.length === 1 && els['lpi-checks'].children[0].dataset.ok === '1', '画面: チェックリスト (撮影不要なので 1 行・済)');
  ok(els['lpi-btn'].textContent.startsWith('🖼 もう一度画像を作る (全部'), '画面: 「もう一度画像を作る (全部)」');
  // 2 枚目を確認 → サーバの値で描き直す
  await btnOf(1, 'check').click();
  ok(sent.at(-1)[0].endsWith(`/lp-images/${initial.cards[1].current.id}/check`) && sent.at(-1)[1].checked === true, '確認: 出していた版の行に checked=true を送る');
  ok(els['lpi-checked'].textContent === '確認済み 1 / 3' && btnOf(1, 'check').textContent === '✓ 確認済み', '確認: 確認済み 1 / 3・ボタンが ✓ 確認済み');
  await btnOf(1, 'check').click();
  ok(sent.at(-1)[1].checked === false && els['lpi-checked'].textContent === '確認済み 0 / 3', 'もう一度押すと外す');
  // 3 枚目を再生成: 通信が切れたら同じキーで送り直す (二重にならない)
  const delKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  net = 'throw';
  await btnOf(2, 'regen').click();
  ok(all(cardsEl()[2], (e) => /届いたか分かりません/.test(e._text)).length === 1 && !btnOf(2, 'regen').disabled, '再生成: 届いたか分からなければカードに出して押し直せる');
  net = 'ok';
  await btnOf(2, 'regen').click();
  const regenSends = sent.filter((s) => /\/regenerate$/.test(s[0]));
  ok(regenSends.length === 2 && regenSends[0][1].idempotency_key === regenSends[1][1].idempotency_key && regenSends[1][0].endsWith(`/lp-images/${initial.cards[2].head_id}/regenerate`),
    '🚨 再生成: 送り直しは同じキー (受付は 1 回だけ)・送り先は出していた版');
  const after = ctl.state();
  ok(after.cards[2].head_version === 2 && db.prepare(`SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE regen_of_image_id = ?`).get(initial.cards[2].root_id).n === 1, '作り直しの依頼は 1 つだけ');
  ok(after.cards[2].pending && timers.length > 0 && timers.at(-1).ms === 5000, '作っている間は 5 秒おきに見に行く');
  for (let k = 0; k < 200 && ['queued', 'running'].includes(db.prepare('SELECT status FROM ph_lp_images WHERE id = ?').get(after.cards[2].head_id).status); k++) await new Promise((res) => setTimeout(res, 10));
  await ctl.poll();
  ok(/作り直せませんでした: 参考画像/.test(cardsEl()[2].textContent) && btnOf(2, 'regen').textContent.startsWith('再生成 (約'), '作り直せなかった理由をカードに出し、もう一度押せる');
  // 別の画面が先に作り直した → この画面 (古い版を出したまま) の再生成は 409。理由をカードに出して最新を取り直す
  const headNow = ctl.state().cards[2].head_id;
  const ext = await fetch(`${uiBase}/apps/product-hub/api/drafts/${R.id}/lp-images/${headNow}/regenerate`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idempotency_key: 'regen-key-ext001' }) });
  ok(ext.status === 200, '(別の画面で先に作り直す)');
  const n0 = sent.length;
  await btnOf(2, 'regen').click();
  ok(sent[n0][0].endsWith(`/lp-images/${headNow}/regenerate`) &&/もう作り直しています/.test(cardsEl()[2].textContent) && sent.length === n0 + 2 && sent[n0 + 1][1] === null,
    '🚨 古い画面からの再生成は断られ、理由をカードに出して最新の状況を取り直す (409 → GET)');
  ok(db.prepare('SELECT COUNT(*) AS n FROM ph_lp_image_jobs WHERE regen_of_image_id = ?').get(initial.cards[2].root_id).n === 2, '作り直しの依頼は増えない (別の画面の 1 つだけ)');
  for (let k = 0; k < 200 && db.prepare("SELECT COUNT(*) AS n FROM ph_lp_images WHERE status IN ('queued','running')").get().n > 0; k++) await new Promise((res) => setTimeout(res, 10));
  if (delKey !== undefined) process.env.GOOGLE_SERVICE_ACCOUNT_KEY = delKey;
  // 全部作り直しは確かめる (やめたら送らない)
  const nAll = sent.length;
  await els['lpi-btn'].click();
  ok(asked.length === 1 && /もう一度全部作ります/.test(asked[0]) && sent.length === nAll, '全部の作り直しは確かめる・やめたら送らない');
  // 古いタブ (前の依頼を見ている) で「全部作る」→ 画面が見ていた依頼を送り、409 で理由を出して最新を取り直す
  const latestNow = ctl.state().latest_job_id;
  els['lpi-json']._text = JSON.stringify({ ...ctl.state(), latest_job_id: latestNow - 1 });
  const sentOld = [];
  const ctlOld = ui.initLpImages(doc, { fetchJson: async (url, opt) => { sentOld.push([url, opt ? opt.body : null]); const res = await fetch(uiBase + url, opt && opt.method === 'POST' ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(opt.body) } : {}); return { status: res.status, json: await res.json() }; },
    confirm: () => true, setTimer: () => 0, clearTimer: () => {}, newKey: () => 'ui-key-old-0001', isHidden: () => false });
  await els['lpi-btn'].click();
  ok(sentOld[0][1].expected_job_id === latestNow - 1 && /先に画像を作りました/.test(els['lpi-status'].textContent) && sentOld.length === 2 && sentOld[1][1] === null && ctlOld.state().job.id === ctl.state().job.id,
    '🚨 古いタブの「全部作る」は画面が見ていた依頼を送り、409 の理由を出して最新を取り直す');
  // 押せない人の画面
  els['lpi-json']._text = JSON.stringify({ ...initial, can_edit: false });
  ui.initLpImages(doc, { fetchJson: async () => { throw new Error('送らない'); }, confirm: () => true, setTimer: () => 0, clearTimer: () => {}, newKey: () => 'k', isHidden: () => false });
  ok(els['lpi-btn'].disabled && all(els['lpi-grid'], (e) => e.dataset.lpiAct).every((b) => b.disabled) && /画像登録者/.test(els['lpi-status'].textContent), '役割の無い人の画面: 全部のボタンを押せない形で理由を出す');
  uiServer.close();
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
