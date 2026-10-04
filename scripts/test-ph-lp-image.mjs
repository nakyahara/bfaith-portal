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
const ENV = { PH_LP_IMAGE_ENABLED: '1', OPENAI_LP_IMAGE_API_KEY: 'sk-test-lp-image' };
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
function makeComposed(output, { images = [{ file_id: 'FILEIDWHITE01', role: 'white_bg' }, { file_id: 'FILEIDTOP0001', role: 'slot:1' }] } = {}) {
  seqNo++;
  const name = 'ハッカ油スプレー';
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url, created_by) VALUES (?, ?, ?, 't')`)
    .run('LPIMG' + seqNo, name, 'https://drive.google.com/drive/folders/' + FOLDER).lastInsertRowid);
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
  return { draft, composeJobId: r.job.id };
}

console.log('① 設定 (fail-closed・品質段は low / medium / high だけ)');
{
  const c = li.lpImageConfig(ENV);
  ok(c.usable && c.model === 'gpt-image-2.5-flare' && c.quality === 'medium' && c.budget === 3000 && c.size === '1200x1200',
    '既定 = flare・medium・月 3,000 円・1200x1200 (中原さん「全部おすすめ」)');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_ENABLED: '' }).usable, '機能フラグが無ければ使えない');
  ok(!li.lpImageConfig({ PH_LP_IMAGE_ENABLED: '1' }).usable, '🚨 キーが無ければ使えない');
  ok(/OPENAI_LP_IMAGE_API_KEY/.test(li.lpImageConfig({ PH_LP_IMAGE_ENABLED: '1', OPENAI_API_KEY: 'sk-other' }).error), '🚨 問い合わせハブのキー名 (OPENAI_API_KEY) は読まない (名前を分ける・検討 §6 層1)');
  for (const q of ['xhigh', 'max', 'auto', 'MEDIUM']) ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_QUALITY: q }).usable, `🚨 品質段 ${q} は使わせない (読めない設定 = 止める)`);
  ok(li.lpImageConfig({ ...ENV, PH_LP_IMAGE_QUALITY: 'high' }).quality === 'high', 'high までは選べる');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_IMAGE_MODEL: 'dall-e-3' }).usable, '知らないモデルは止める');
  ok(!li.lpImageConfig({ ...ENV, PH_LP_MONTHLY_BUDGET_JPY: '3000円' }).usable, '読めない上限は止める (既定に戻さない)');
  eq(li.lpImageConfig({ ...ENV, PH_LP_MONTHLY_BUDGET_JPY: '500' }).budget, 500, '上限は変えられる');
}

console.log('② 費用の計算 (検討 §5.1 の単価・少なく数えないよう 1 ドル 160 円)');
{
  // 文 1,000 + 画像 3,000 tok 入力・出力 439 tok (medium) → (1000×5 + 3000×8 + 439×30)/1e6 × 160
  eq(li.costJpyFromUsage({ input_tokens: 4000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } }), 6.75, '円に直す');
  eq(li.costJpyFromUsage(null), null, 'usage が無ければ null (見込み額のまま)');
  eq(li.jstMonth(Date.parse('2026-10-31T15:30:00Z')), '2026-11', '月は JST で区切る');
  // 取り置き = これ以上はかからない額 (#1612 R1 High)。medium・参考 2 枚・4,000 字 → (6000×5 + 6000×8 + 2000×30)/1e6 × 170
  eq(li.reserveJpy({ quality: 'medium', refs: 2, promptChars: 4000 }), 23.46, '取り置きの計算 (medium・参考 2 枚・4,000 字)');
  ok(li.reserveJpy({ quality: 'high', refs: 4, promptChars: 4000 }) > li.reserveJpy({ quality: 'medium', refs: 4, promptChars: 4000 }), '品質段が上がれば取り置きも上がる');
  ok(li.reserveJpy({ quality: 'medium', refs: 4, promptChars: 4000 }) > li.reserveJpy({ quality: 'medium', refs: 1, promptChars: 4000 }), '参考画像が多ければ上がる');
  ok(li.reserveJpy({ quality: 'medium', refs: 2, promptChars: 4000 }) > li.costJpyFromUsage({ input_tokens: 4000, output_tokens: 439, input_tokens_details: { text_tokens: 1000, image_tokens: 3000 } }), '🚨 取り置きは実際の額より大きい');
  eq(li.reserveJpy({ quality: 'max', refs: 2, promptChars: 10 }), null, '知らない品質段は取り置けない (= 作らない)');
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
  ok(plan.images.every((im) => im.est_jpy > 0) && withMat.est_jpy > noMat.est_jpy, '1 枚ずつ取り置き額を出す (素材を渡す画像は高い)');
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
  ok(calls.upload[0].name.startsWith('LPIMG1_AI初稿_') && calls.upload[0].name.endsWith('_0枚目.png'), `名前に商品コードと何枚目か (${calls.upload[0].name})`);
  const us = usageOf(jobId);
  ok(us.length === 3 && us.every((u) => u.status === 'charged' && u.cost_jpy === 6.75 && u.out_tokens === 439), '台帳に 1 枚ずつ実額で記録 (charged)');
  eq(li.monthUsage(db, T0).used_jpy, 20.25, '今月の使った額 = 実額の合計');
  const st = li.imageStateFor(db, { draft: A.draft, folderId: FOLDER, env: ENV, now: T0 });
  ok(st.job.status === 'done' && st.job.images.length === 3 && st.job.cost_jpy === 20.25, '画面に画像と費用が渡る');
  const evs = db.prepare('SELECT event FROM draft_events WHERE draft_id = ? ORDER BY id').all(A.draft.id).map((r) => r.event);
  ok(evs.includes('lp_image_requested') && evs.includes('lp_image_done'), '商品の履歴に残る');
  ok(dbmod.imageRefOfFileId(db, 'DRIVEOUT0001'), '🚨 作った画像はサムネイルの口で見せてよい (登録済みと同じ扱い)');
}

console.log('⑥ 作る係 — お金の上限で止める (fail-closed)');
{
  const B = makeComposed();
  const r = li.requestImageJob(db, { draft: B.draft, folderId: FOLDER, idempotencyKey: 'img-key-b001', env: ENV, now: T0 });
  ok(r.ok, '受け付ける');
  // 上限を実際の取り置き額から組む: 1 枚目 = 使った額 + 取り置き1 ≤ 上限 → 実額 6.75。2 枚目 = 使った額 + 6.75 + 取り置き2 ≤ 上限 → 実額 6.75。
  // 3 枚目 = 使った額 + 13.5 + 取り置き3 > 上限 で止まる
  const est = imagesOf(r.job.id).map((im) => im.est_jpy);
  const used0 = li.monthUsage(db, T0).used_jpy;
  const cap = Math.ceil(used0 + 6.75 + est[1]) + 1;
  ok(used0 + est[0] <= cap && used0 + 13.5 + est[2] > cap, `上限の組み立て (使った ${used0} 円・取り置き ${est.join(' / ')} 円・上限 ${cap} 円)`);
  const env = { ...ENV, PH_LP_MONTHLY_BUDGET_JPY: String(cap) };
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
  const thumb = await fetch(`${base}/api/thumb/NOTREGISTERED0001`);
  eq(thumb.status, 404, '登録されていない ID のサムネイルは見せない (今までどおり)');
  process.env.PH_LP_IMAGE_ENABLED = '';
  const html2 = await (await fetch(`${base}/detail/${G.draft.id}`)).text();
  ok(!html2.includes('id="lpi-btn"'), '機能フラグが無ければボタンを置かない');
  process.env.PH_LP_IMAGE_ENABLED = '1';
  server.close();
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
