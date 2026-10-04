import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor, fixture, FIXTURES } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-compose-lib-');
/**
 * LP 構成の AI 生成 — 依頼・固定 packet・予約・結果 (apps/product-hub/lib/lp-compose.js・段階1)
 * 実行: node scripts/test-ph-lp-compose-lib.mjs
 * 設計 = 正本 AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』§4.3b の遷移表。
 *
 * 検証したいのは Codex R1〜R4 が突いた所:
 *   ・受付時に固定した packet が claim で変わらない (仕様書が差し替わっても・画像が増えても)
 *   ・AI を呼んだ後の終了は必ず generation を確定させる (fail / release は予約の前だけ)
 *   ・成否不明 (lease 切れ) は needs_review で止まり、自動で作り直さない
 *   ・応答断の再送は保存済みの receipt を返す (二重に書かない)
 *   ・終端では必ず completed_at が入る (3 分の合格ラインを後から計算できる)
 */
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');

process.env.PH_LP_COMPOSE_ENABLED = '1';

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(a === b, `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const T0 = Date.parse('2026-10-01T00:00:00Z');
const min = (n) => T0 + n * 60_000;
const mkDraft = (ne, name) => {
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'test')`).run(ne, name).lastInsertRowid);
  return db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
};
const args = (draft, spec, key, extra = {}) => ({
  draft, spec, idempotencyKey: key, actor: 'u@x', now: T0,
  productInfo: '■商品情報\nハッカ油 100ml',
  colorVariations: '■カラバリ\nなし (単品)',
  images: [{ file_id: 'FILEID000001', modified_time: '2026-09-30T00:00:00Z' }],
  ...extra,
});

console.log('⓪ モデル (決める場所は Render の PH_LP_COMPOSE_MODEL だけ・2026-10-02)');
{
  const saved = process.env.PH_LP_COMPOSE_MODEL;
  delete process.env.PH_LP_COMPOSE_MODEL;
  eq(lp.lpComposeModel(), 'claude-opus-5-5[1m]', '既定は Opus 5.5 (1M)');
  process.env.PH_LP_COMPOSE_MODEL = 'claude-sonnet-5';
  eq(lp.lpComposeModel(), 'claude-sonnet-5', '環境変数で変えられる');
  process.env.PH_LP_COMPOSE_MODEL = '';
  eq(lp.lpComposeModel(), lp.DEFAULT_MODEL, '空は未設定と同じ (既定)');
  for (const bad of ['claude-opus-5-5[1m]\n', ' claude-opus-5', 'claude-opus-5-5 ', 'gpt-5.6-sol', 'claude-opus-5 --dangerously-skip-permissions', 'claude-opus-5-5[2m]', ' ']) {
    process.env.PH_LP_COMPOSE_MODEL = bad;
    // 🚨 黙って既定に戻さない (戻すと queue・reserve・model-check が既定で揃って「一致」になる・codex #1591 R2 Medium)
    eq(lp.lpComposeModel(), null, `🚨 設定してあるのに読めない値は使わない (止める): ${JSON.stringify(bad)}`);
  }
  // 読めない設定では押せない・掴まない・予約しない
  ok(lp.requestBlockReason({ draft: { name: 'x' }, productInfo: 'x', spec: { id: 1 }, images: [{ file_id: 'FILEID000001' }] }) === lp.MODEL_CONFIG_ERROR, '🚨 ボタンは押せない (理由を出す)');
  eq(lp.claimJob(db, { runnerRunId: 'run-cfg' }).code, 'bad_config', '🚨 claim しない');
  eq(lp.queueSummary(db).model, null, 'queue のモデルは null (ランナーは「使えるモデルが来ない」で止まる)');
  eq(lp.reserveGeneration(db, 1, { leaseToken: 'x', model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION }).code, 'bad_config', '🚨 予約しない');
  eq(lp.jobStateFor(db, 999999).model_label, null, '画面のモデル名は出さない');
  if (saved === undefined) delete process.env.PH_LP_COMPOSE_MODEL; else process.env.PH_LP_COMPOSE_MODEL = saved;
  eq(lp.modelLabel('claude-opus-5-5[1m]'), 'Opus 5.5', '表示名: Opus 5.5');
  eq(lp.modelLabel('claude-opus-5'), 'Opus 5', '表示名: Opus 5');
  eq(lp.modelLabel('claude-sonnet-5'), 'Sonnet 5', '表示名: Sonnet 5');
  eq(lp.modelLabel('claude-haiku-4-5-20251001'), 'Haiku 4.5', '表示名: 日付は落とす');
  eq(lp.modelLabel('なにか'), 'なにか', '読めない形はそのまま');
}

console.log('① 仕様書の取り込み (追記専用・同じ中身は版を増やさない)');
const s1 = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式'], actor: 'u@x' });
ok(s1.ok && s1.created, '取り込める');
const s1b = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式'], actor: 'u@x' });
ok(s1b.ok && !s1b.created && s1b.spec.id === s1.spec.id, '同じ中身なら版を増やさず既存を返す');
const sTabs = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式', '追加タブ'], actor: 'u@x' });
ok(sTabs.ok && sTabs.created, '🚨 本文が同じでもタブが変われば別の版 (実効入力が変わるため・R4 #4)');
ok(lp.importSpec(db, { kind: 'simple_lp', title: 't', body: 'b', actor: 'u' }).code === 'bad_kind', '段階1 は product_analysis だけ');
ok(lp.importSpec(db, { kind: 'product_analysis', title: 't', body: '   ', actor: 'u' }).code === 'bad_request', '空の仕様書は拒む');
eq(lp.latestSpec(db).id, sTabs.spec.id, 'いまの版 = 最大 id');

console.log('② 受付 — packet をここで固定する');
const dA = mkDraft('LP-A', 'ハッカ油スプレー 100ml');
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0001', { productInfo: '' })).code === 'not_ready', '商品情報が無ければ受け付けない');
// 🚨 画像が無いと AI は必ず IMAGES_UNAVAILABLE で止まる (2026-10-02 の 1 件目)。押す前に止める
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0001', { images: [] })).code === 'not_ready', '🚨 商品画像が無ければ受け付けない');
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0001', { images: undefined })).code === 'not_ready', '🚨 images の渡し忘れも「無い」と同じ (受け付けない)');
ok(lp.requestBlockReason({ draft: dA, productInfo: 'x', spec: sTabs.spec, images: [] }).includes('商品画像がありません'), '押せない理由を画面に出せる');
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0001', { images: [{}, { file_id: '' }, { file_id: '   ' }] })).code === 'not_ready',
  '🚨 空の ID しか無い画像も「無い」(packet に入る枚数で見る・codex #1591 Low)');
const r1 = lp.requestJob(db, args(dA, sTabs.spec, 'key-0001'));
ok(r1.ok && r1.created, '受け付ける');
const packet1 = JSON.parse(r1.job.packet_json);
eq(packet1.spec_id, sTabs.spec.id, 'packet に受付時の仕様書の版が入る');
eq(packet1.images.length, 1, '画像も packet に固定される');
const r1b = lp.requestJob(db, args(dA, sTabs.spec, 'key-0001'));
ok(r1b.ok && !r1b.created && r1b.job.id === r1.job.id, '同じ idempotency_key は同じ依頼を返す (二重クリック)');
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0002')).code === 'already_running', '動いている間は 2 件目を受け付けない');
ok(r1.job.measurement_deadline_at === new Date(min(3)).toISOString(), '測定の期限 = 受付 + 3 分で固定 (R4 #2)');

console.log('②b 🚨 指示文はスタッフの定型文と同じもの 1 つだけ (設計 §5)');
{
  // 段階1 の測定は「**同じ入力**から作った二つを比べる」のが前提。
  // 指示文が片方だけ違うと、比べているのが「AI の力の差」なのか
  // 「指示文の差」なのか分からなくなるので、正本を 1 つに固定する。
  const pt = await import('../apps/product-hub/lib/prompt-templates.js');
  const packet = JSON.parse(r1.job.packet_json);
  ok(!!packet.instruction, 'packet に指示文が入っている');
  eq(packet.instruction, pt.PRODUCT_ANALYSIS_INSTRUCTION, '🚨 packet の指示文 = 定型文の正本');
  // スタッフが ChatGPT に貼る文の中に、そのまま入っていること
  const staff = pt.buildProductAnalysisPrompt({ name: 'ハッカ油スプレー 100ml' }, { product_info_text: '天然ハッカ油' }, '');
  ok(staff.includes(packet.instruction),
    '🚨 スタッフの定型文に同じ文がそのまま入っている (二重に持っていない)');
  eq(packet.packet_version, 4, 'packet の版が上がっている (形が変わった・4 = 素材画像)');
  // 指示文も packet_hash の中 = 後から差し替えられない
  const again = lp.buildPacket({
    draft: dA, productInfo: packet.product_info, colorVariations: packet.color_variations,
    images: packet.images, spec: sTabs.spec,
  });
  eq(again.hash, r1.job.packet_hash, '同じ材料なら hash も同じ');
  const tampered = lp.buildPacket({
    draft: dA, productInfo: packet.product_info, colorVariations: packet.color_variations,
    images: packet.images, spec: { ...sTabs.spec, hash: 'ちがう' },
  });
  ok(tampered.hash !== r1.job.packet_hash, '材料が違うなら hash も違う');
}

console.log('③ claim — 受付時の材料をそのまま返す');
// 受付のあとに仕様書を更新し、画像も増やす (= 実運用で起きること)
const s2 = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.3 (更新後)', actor: 'u@x' });
ok(s2.created && s2.spec.id > sTabs.spec.id, '受付後に仕様書が更新された');
const c1 = lp.claimJob(db, { runnerRunId: 'run-1', now: min(0.2) });
ok(c1.ok && c1.job, 'claim できる');
eq(c1.job.spec.id, sTabs.spec.id, '🚨 claim は「最新版」ではなく受付時の版を返す (R3 #3)');
eq(c1.job.spec.body, '本文 V2.2', '仕様書の全文も受付時のもの');
eq(c1.job.packet_hash, r1.job.packet_hash, 'packet は受付時のまま');
ok(lp.claimJob(db, { runnerRunId: 'run-2', now: min(0.2) }).job === null, '同じ依頼を 2 つの実行役が掴まない');

console.log('④ 予約の前は fail / release が使える');
const rel = lp.releaseJob(db, c1.job.job_id, { leaseToken: c1.job.lease_token, reason: '一時障害', now: min(0.4) });
eq(rel.status, 'queued', '予約前は手放せる (queued に戻る)');
const c2 = lp.claimJob(db, { runnerRunId: 'run-3', now: min(0.6) });
ok(c2.ok && c2.job.job_id === c1.job.job_id, '戻った依頼をまた掴める');

console.log('⑤ 予約 — AI を呼ぶ前に必ず通す');
// モデルはサーバの設定 (PH_LP_COMPOSE_MODEL) だけで決まる。claim がそれを返し、違うモデルの予約は断る
eq(c2.job.model, lp.DEFAULT_MODEL, 'claim の応答にモデルが入る');
eq(lp.queueSummary(db, min(0.7)).model, lp.DEFAULT_MODEL, 'queue の応答にもモデルが入る (ランナーは claude 起動前に知る)');
eq(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(0.8) }).code,
  'bad_model', '🚨 設定と違うモデルの予約は断る (別の条件で作ったものが測定に混ざらない)');
eq(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: lp.DEFAULT_MODEL + ' ', promptVersion: lp.PROMPT_VERSION, now: min(0.8) }).code,
  'bad_model', '🚨 空白付きは同じモデルに畳まない (識別子は形を直接見る)');
eq(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, promptVersion: lp.PROMPT_VERSION, now: min(0.8) }).code,
  'bad_request', 'モデル無しは断る');
const g1 = lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(0.8) });
ok(g1.ok && g1.generation_id > 0, '予約できる');
ok(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(0.8) }).code === 'already_reserved', '1 依頼 1 回');
ok(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: 'ふるい版', now: min(0.8) }).code === 'bad_prompt_version', '🚨 実行役の prompt 版が違えば AI を呼ぶ前に断る (コード R1 #6)');
ok(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION + ' ', now: min(0.8) }).code === 'bad_prompt_version', '🚨 prompt_version も空白付きを同じに畳まない (codex #1591 Low)');
ok(lp.failJob(db, c2.job.job_id, { leaseToken: c2.job.lease_token, code: 'x', now: min(0.9) }).code === 'already_reserved',
  '🚨 予約後に fail は使えない (result で rejected を出す・§4.3b)');
ok(lp.releaseJob(db, c2.job.job_id, { leaseToken: c2.job.lease_token, now: min(0.9) }).code === 'already_reserved',
  '🚨 予約後に release は使えない (二重に呼べてしまう)');

console.log('⑥ 結果 — accepted');
// 🚨 PR1-c から **lint はサーバが実行してそれが正本**なので、
// accepted を受け取らせるには **本当に lint を通る本文** が要る (ダミー文字列では通らない)。
// draft 名は 'ハッカ油スプレー NL' なので、共通の接頭辞を入れておけば検査 17 を通る
const OUT = compositionFor('ハッカ油スプレー');
// packet に画像があるので accepted には証跡が要る (codex exec review R2 P2)
const LINT = { ok: true, checks: {} };
// 証跡はサーバが配ったときに記録する (実行役からは受け取らない)
// 🚨 記録は claim の lease に紐づく (codex exec review P2)。lease_token と時刻が要る
const serve = (job, now) => lp.recordImageServed(db, job.job_id,
  { leaseToken: job.lease_token, fileId: 'FILEID000001', sha256: '0'.repeat(64), bytes: 999, now });
serve(c2.job, min(0.6));
ok(lp.submitResult(db, g1.generation_id, { packetHash: 'ちがう', verdict: 'accepted', output: OUT, lint: LINT, now: min(1) }).code === 'packet_mismatch',
  '材料が予約時と違えば受け取らない');
const sub1 = lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, reviewRounds: 1, now: min(1) });
eq(sub1.status, 'done', '受け取ると done');
const jDone = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(c2.job.job_id);
eq(jDone.output_text, OUT, '本文が保存される');
ok(!!jDone.completed_at, '🚨 終端では completed_at が必ず入る (R4 #2)');
// 🚨 本文を画面に出すのは、ランナーが「実モデル = 頼んだモデル」を付けてから (codex #1591 R2 High)
{
  const pre = lp.jobStateFor(db, dA.id, { now: min(1.1) }).job;
  ok(pre.status === 'done' && pre.model_check === null && pre.output_text === null, '🚨 実モデルの確認前は本文を出さない (確認中)');
  lp.recordModelCheck(db, { runnerRunId: 'run-3', actualModels: ['claude-opus-5-5'], now: min(1.2) });
  const post = lp.jobStateFor(db, dA.id, { now: min(1.3) }).job;
  ok(post.status === 'done' && post.model_check === 'match' && post.output_text === OUT, '一致が付いたら本文を出す');
}
ok(jDone.completed_at <= jDone.measurement_deadline_at, '3 分以内に終わったと後から計算できる');
const sub1b = lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, reviewRounds: 1, now: min(1.5) });
ok(sub1b.ok && sub1b.already, '同じ結果の再送は保存済みを返す (応答断のリトライ)');
ok(lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'rejected', reason: 'ちがう内容', now: min(1.5) }).code === 'already_finalized',
  '別の内容では上書きできない');

// 🚨 packet に画像があるのに証跡が無い accepted は受け取らない (codex exec review R2 P2)
const dZ = mkDraft('LP-Z', 'ハッカ油スプレー 30ml');   // 証跡なしの系
lp.requestJob(db, args(dZ, s2.spec, 'key-0001', { now: min(5) }));
const cZ = lp.claimJob(db, { runnerRunId: 'run-z', now: min(5) });
const gZ = lp.reserveGeneration(db, cZ.job.job_id, { leaseToken: cZ.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(5) });
eq(lp.submitResult(db, gZ.generation_id, { packetHash: cZ.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(6) }).code,
  'bad_request', '🚨 サーバが画像を配っていなければ accepted を受け取らない');
eq(lp.submitResult(db, gZ.generation_id, {
  packetHash: cZ.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'f'.repeat(64), bytes: 1 }] }, now: min(6),
}).code, 'bad_request', '🚨 実行役が証跡を送ってきても使わない (偽造できた・codex exec review P1)');
eq(lp.submitResult(db, gZ.generation_id, { packetHash: cZ.job.packet_hash, verdict: 'accepted', output: OUT, now: min(6) }).code,
  'bad_request', '🚨 lint が無ければ accepted を受け取らない');
eq(lp.submitResult(db, gZ.generation_id, { packetHash: cZ.job.packet_hash, verdict: 'accepted', output: OUT, lint: { ok: false }, now: min(6) }).code,
  'bad_request', '🚨 lint.ok が false でも受け取らない');
eq(lp.submitResult(db, gZ.generation_id, { packetHash: cZ.job.packet_hash, verdict: 'rejected', reason: '作れなかった', now: min(6) }).status,
  'failed', 'rejected は証跡が無くてよい (作れなかったので)');

// 🚨 証跡は claim ごとにリセットされる (codex exec review P1)。
// 前の実行役が画像を落としてから手放した場合に、次の実行役が
// 画像を一度も見ずに accepted を出せてはいけない
const dY = mkDraft('LP-Y', 'ハッカ油スプレー 40ml');   // 証跡の使い回し
lp.requestJob(db, args(dY, s2.spec, 'key-0001', { now: min(7) }));
const cY1 = lp.claimJob(db, { runnerRunId: 'run-y1', now: min(7) });
serve(cY1.job, min(7));                                    // 1 人目の実行役が画像を見た
lp.releaseJob(db, cY1.job.job_id, { leaseToken: cY1.job.lease_token, now: min(7.5) });
const cY2 = lp.claimJob(db, { runnerRunId: 'run-y2', now: min(8) });   // 2 人目が掴む
const gY = lp.reserveGeneration(db, cY2.job.job_id, { leaseToken: cY2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(8) });
eq(lp.submitResult(db, gY.generation_id, { packetHash: cY2.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(8.5) }).code,
  'bad_request', '🚨 前の実行役の証跡を使い回せない (claim でリセット・codex exec review P1)');
serve(cY2.job, min(8));
eq(lp.submitResult(db, gY.generation_id, { packetHash: cY2.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(8.5) }).status,
  'done', '自分で見たなら通る');

// 🚨 手放した後に、飛んでいた取得が完走しても次の実行役の証跡にならない (codex exec review P2)。
// lease を見ずに記録していたときは、A の遅い取得が B の証跡になり、
// B は画像を一度も見ずに accepted を出せた
const dLap = mkDraft('LP-LAP', 'ハッカ油スプレー 60ml'); // lease の取り直し
lp.requestJob(db, args(dLap, s2.spec, 'key-0001', { now: min(9) }));
const cLap1 = lp.claimJob(db, { runnerRunId: 'run-lap1', now: min(9) });
lp.releaseJob(db, cLap1.job.job_id, { leaseToken: cLap1.job.lease_token, now: min(9.2) });
const cLap2 = lp.claimJob(db, { runnerRunId: 'run-lap2', now: min(9.3) });
ok(cLap2.job && cLap2.job.lease_token !== cLap1.job.lease_token, '掴み直すと lease は別物');
eq(serve(cLap1.job, min(9.4)).code, 'lease_lost',
  '🚨 古い lease の取得は記録しない (codex exec review P2)');
const gLap = lp.reserveGeneration(db, cLap2.job.job_id, { leaseToken: cLap2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(9.5) });
eq(lp.submitResult(db, gLap.generation_id, { packetHash: cLap2.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(9.6) }).code,
  'bad_request', '🚨 古い lease の取得では accepted を出せない');
serve(cLap2.job, min(9.7));
eq(lp.submitResult(db, gLap.generation_id, { packetHash: cLap2.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: LINT, now: min(9.8) }).status,
  'done', '自分で見たなら通る');

console.log('⑦ 結果 — rejected (lint / 検品が通らなかった)');
const dB = mkDraft('LP-B', 'ハッカ油スプレー 50ml');
const r2 = lp.requestJob(db, args(dB, s2.spec, 'key-0001', { now: min(10) }));
const c3 = lp.claimJob(db, { runnerRunId: 'run-4', now: min(10) });
const g2 = lp.reserveGeneration(db, c3.job.job_id, { leaseToken: c3.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(10) });
const sub2 = lp.submitResult(db, g2.generation_id, { packetHash: c3.job.packet_hash, verdict: 'rejected', lint: { missing: ['# 0枚目｜サムネイル'] }, reviewRounds: 2, reason: '2 巡で通らなかった', now: min(11) });
eq(sub2.status, 'failed', '通らなければ failed');
const jRej = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(c3.job.job_id);
eq(jRej.error_code, 'rejected', '理由が残る');
ok(!!jRej.completed_at, 'rejected でも completed_at が入る');
eq(db.prepare('SELECT status FROM ph_lp_compose_generations WHERE id = ?').get(g2.generation_id).status, 'rejected',
  '🚨 generation も確定する (reserved のまま残さない)');
eq(jRej.output_text, null, '構成を送らなければ何も残らない (書けなかったとき)');

console.log('⑦b 🚨 チェックを通らなかった構成も残す (2026-10-04 中原さん「A」: くらべっこなので捨てない)');
{
  const dR = mkDraft('LP-REJ', 'ハッカ油スプレー REJ');
  lp.requestJob(db, args(dR, s2.spec, 'key-rej-1', { now: min(5012) }));
  const cR = lp.claimJob(db, { runnerRunId: 'lpr-20261004-120000-rejrej', now: min(5012) });
  const gR = lp.reserveGeneration(db, cR.job.job_id, { leaseToken: cR.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(5012) });
  const DRAFT = '# LP制作システム\n(チェックで指摘が残った構成の下書き)';
  eq(lp.submitResult(db, gR.generation_id, { packetHash: cR.job.packet_hash, verdict: 'rejected', output: 'x'.repeat(lp.OUTPUT_MAX + 1), reason: 'r', now: min(5013) }).code,
    'too_large', '大きすぎる下書きは断る');
  const sR = lp.submitResult(db, gR.generation_id, { packetHash: cR.job.packet_hash, verdict: 'rejected', output: DRAFT, reviewRounds: 2, reason: '2 巡目に high が 3 件 (縦の配分が 100% を超える ほか)', now: min(5013) });
  eq(sR.status, 'failed', 'rejected は failed のまま');
  eq(db.prepare('SELECT output_text FROM ph_lp_compose_jobs WHERE id = ?').get(cR.job.job_id).output_text, DRAFT, '🚨 書いた構成を残す');
  const pre = lp.jobStateFor(db, dR.id, { now: min(5013.1) }).job;
  ok(pre.draft_text === null && pre.output_text === null, '実モデルの確認前は出さない');
  lp.recordModelCheck(db, { runnerRunId: 'lpr-20261004-120000-rejrej', actualModels: ['claude-opus-5-5'], now: min(5013.2) });
  const st = lp.jobStateFor(db, dR.id, { now: min(5013.3) }).job;
  ok(st.status === 'failed' && st.draft_text === DRAFT, '🚨 一致したら「チェックを通らなかった構成」として画面に出す');
  eq(st.output_text, null, '「できた本文」(done 用) には出さない (参考扱いを混ぜない)');
  ok((st.error || '').includes('縦の配分'), '指摘されたこと (reason) も画面に渡る');
  const again = lp.submitResult(db, gR.generation_id, { packetHash: cR.job.packet_hash, verdict: 'rejected', output: DRAFT, reviewRounds: 2, reason: '2 巡目に high が 3 件 (縦の配分が 100% を超える ほか)', now: min(5014) });
  ok(again.ok && again.already, '同じ内容の再送は保存済みを返す');
  // 不一致なら出さない
  const dR2 = mkDraft('LP-REJ2', 'ハッカ油スプレー REJ2');
  lp.requestJob(db, args(dR2, s2.spec, 'key-rej-2', { now: min(5015) }));
  const cR2 = lp.claimJob(db, { runnerRunId: 'lpr-20261004-120100-rejre2', now: min(5015) });
  const gR2 = lp.reserveGeneration(db, cR2.job.job_id, { leaseToken: cR2.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(5015) });
  lp.submitResult(db, gR2.generation_id, { packetHash: cR2.job.packet_hash, verdict: 'rejected', output: DRAFT, reason: 'r', now: min(5016) });
  lp.recordModelCheck(db, { runnerRunId: 'lpr-20261004-120100-rejre2', actualModels: ['claude-sonnet-5'], now: min(5016.1) });
  eq(lp.jobStateFor(db, dR2.id, { now: min(5016.2) }).job.draft_text, null, '🚨 別のモデルが書いた下書きは出さない');
}

// 🚨 packet の画像を**全部**見ていなければ accepted は出せない (codex exec review P1)。
// 1 枚でも配っていればよいにしていたときは、途中の枚で落ちた実行役が
// 残りを見ずに accepted を出せた (= 欠けた材料で書いた構成が測定に混ざる)
const dAll = mkDraft('LP-ALL', 'ハッカ油スプレー 3 枚');
lp.requestJob(db, args(dAll, s2.spec, 'key-0001', {
  now: min(10),
  images: [{ file_id: 'FILEID000001' }, { file_id: 'FILEID000002' }],
}));
const cAll = lp.claimJob(db, { runnerRunId: 'run-all', now: min(10) });
const gAll = lp.reserveGeneration(db, cAll.job.job_id, { leaseToken: cAll.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(10) });
serve(cAll.job, min(10.1));                                 // 1 枚目だけ見た
eq(lp.submitResult(db, gAll.generation_id, { packetHash: cAll.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(10.2) }).code,
  'bad_request', '🚨 2 枚中 1 枚しか見ていなければ accepted を受け取らない (codex exec review P1)');
eq(lp.submitResult(db, gAll.generation_id, { packetHash: cAll.job.packet_hash, verdict: 'rejected', reason: '画像が取れなかった', now: min(10.2) }).status,
  'failed', 'rejected は証跡が揃わなくても出せる (作れなかったという報告)');

// 🚨 巨大な自己申告 lint を送っても、**サーバの結果を追い出せない** (codex exec review P2)。
// 以前は合計が LINT_MAX を超えると実行役の申告にフォールバックしていた
const dBig = mkDraft('LP-BIG', 'ハッカ油スプレー 80ml');
lp.requestJob(db, args(dBig, s2.spec, 'key-0001', { now: min(11) }));
const cBig = lp.claimJob(db, { runnerRunId: 'run-big', now: min(11) });
const gBig = lp.reserveGeneration(db, cBig.job.job_id, { leaseToken: cBig.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(11) });
serve(cBig.job, min(11.1));
const fatLint = { ok: true, checks: {}, filler: 'x'.repeat(lp.LINT_MAX - 200) };
eq(lp.submitResult(db, gBig.generation_id, {
  packetHash: cBig.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: fatLint, now: min(11.2),
}).status, 'done', '巨大な lint を送っても accepted は通る');
{
  const stored = JSON.parse(db.prepare('SELECT lint_json FROM ph_lp_compose_jobs WHERE id = ?').get(cBig.job.job_id).lint_json);
  eq(stored.source, 'server', '🚨 保存された lint は**サーバの結果** (実行役の申告に戻らない)');
  eq(stored.ok, true, 'サーバの判定が入る');
  ok(stored.checks && stored.checks['7'] === true, '検査の内訳も残る');
  eq(stored.runner_lint_dropped, true, '入り切らなければ落とすのは**参考値の方**');
  ok(JSON.stringify(stored).length <= lp.LINT_MAX, 'LINT_MAX に収まる');
}
// 普通の大きさなら、実行役の申告も横に残る
{
  const dSml = mkDraft('LP-SML', 'ハッカ油スプレー 90ml');
  lp.requestJob(db, args(dSml, s2.spec, 'key-0001', { now: min(12) }));
  const c = lp.claimJob(db, { runnerRunId: 'run-sml', now: min(12) });
  const g = lp.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(12) });
  serve(c.job, min(12.1));
  lp.submitResult(db, g.generation_id, { packetHash: c.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: { ok: true, mine: 1 }, now: min(12.2) });
  const stored = JSON.parse(db.prepare('SELECT lint_json FROM ph_lp_compose_jobs WHERE id = ?').get(c.job.job_id).lint_json);
  eq(stored.source, 'server', '正本はサーバ');
  eq(stored.runner_lint.mine, 1, '実行役の申告は横に残る (測定で読める)');
}

console.log('⑧ 成否不明 (lease 切れ) は needs_review で止まる');
const dC = mkDraft('LP-C', 'ハッカ油スプレー 200ml');
lp.requestJob(db, args(dC, s2.spec, 'key-0001', { now: min(20) }));
const c4 = lp.claimJob(db, { runnerRunId: 'run-5', now: min(20) });
lp.reserveGeneration(db, c4.job.job_id, { leaseToken: c4.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(20) });
lp.recoverExpired(db, min(20 + lp.LEASE_MIN + 1));
const jNr = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(c4.job.job_id);
eq(jNr.status, 'needs_review', '🚨 予約後に結果が来なければ needs_review (自動で作り直さない)');
ok(!!jNr.completed_at, 'needs_review でも completed_at が入る');
ok(jNr.completed_at > jNr.measurement_deadline_at, '3 分を超えたと後から分かる (R4 #2)');
ok(lp.claimJob(db, { runnerRunId: 'run-6', now: min(100) }).job === null, 'needs_review は勝手に拾い直さない');

console.log('⑨ 予約前の lease 切れは failed (AI 枠は使っていない)');
const dD = mkDraft('LP-D', 'ハッカ油スプレー 500ml');
lp.requestJob(db, args(dD, s2.spec, 'key-0001', { now: min(30) }));
const c5 = lp.claimJob(db, { runnerRunId: 'run-7', now: min(30) });
lp.recoverExpired(db, min(30 + lp.LEASE_MIN + 1));
eq(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(c5.job.job_id).status, 'failed', '予約前なら failed');

console.log('⑩ 仕様書の版が消えた依頼は claim で止める');
const dE = mkDraft('LP-E', 'ハッカ油スプレー 1L');
const r5 = lp.requestJob(db, args(dE, s2.spec, 'key-0001', { now: min(40) }));
// packet と job 行を**揃えて**別の hash にする (packet 側の自己検査は通るが、仕様書の行と合わない状態)。
// 片方だけ書き換えると R8 #1 の突き合わせが先に落として packet_tampered になる
const bogus = 'b'.repeat(64);
const p5 = { ...JSON.parse(r5.job.packet_json), spec_hash: bogus };
db.prepare('UPDATE ph_lp_compose_jobs SET spec_hash = ?, packet_json = ?, packet_hash = ? WHERE id = ?')
  .run(bogus, JSON.stringify(p5), lp.sha256(lp.canonicalJson(p5)), r5.job.id);
ok(lp.claimJob(db, { runnerRunId: 'run-8', now: min(41) }).job === null, '仕様書の行と hash が合わなければ claim しない');
eq(db.prepare('SELECT error_code FROM ph_lp_compose_jobs WHERE id = ?').get(r5.job.id).error_code, 'spec_changed', '理由が残る');

console.log('⑪ 機能フラグ (fail-closed)');
process.env.PH_LP_COMPOSE_ENABLED = '0';
const dF = mkDraft('LP-F', 'ハッカ油スプレー 2L');
ok(lp.requestJob(db, args(dF, s2.spec, 'key-0001', { now: min(50) })).code === 'disabled', 'フラグが無ければ受け付けない');
ok(lp.claimJob(db, { runnerRunId: 'run-9', now: min(50) }).code === 'disabled', 'フラグが無ければ claim もしない');
process.env.PH_LP_COMPOSE_ENABLED = '1';

console.log('⑫ 画面に返す状態');
const st = lp.jobStateFor(db, dA.id, { now: min(60) });
eq(st.job.status, 'done', 'いちばん新しい依頼を返す');
eq(st.job.output_text, OUT, 'done なら本文を返す');
eq(st.job.within_deadline, true, '期限内だったか');
const stRej = lp.jobStateFor(db, dB.id, { now: min(60) });
eq(stRej.job.output_text, null, 'done 以外は本文を返さない');

console.log('⑬ コードレビュー R1 の修正');
// #1 期限切れ処理が正常結果を上書きしない (SELECT と UPDATE の間で done になった job を塗り潰さない)
const dG = mkDraft('LP-G', 'ハッカ油スプレー 5L');
lp.requestJob(db, args(dG, s2.spec, 'key-0001', { now: min(70) }));
const c6 = lp.claimJob(db, { runnerRunId: 'run-10', now: min(70) });
const g6 = lp.reserveGeneration(db, c6.job.job_id, { leaseToken: c6.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(70) });
serve(c6.job, min(70));
lp.submitResult(db, g6.generation_id, { packetHash: c6.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: LINT, now: min(71) });
// 実モデルの確認 (一致) も付けておく。付けないと 15 分後に「未確認」で needs_review に落ちる (それは ⑩ で見る)
lp.recordModelCheck(db, { runnerRunId: 'run-10', actualModels: ['claude-opus-5-5'], now: min(71.5) });
lp.recoverExpired(db, min(70 + lp.LEASE_MIN + 5));
eq(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(c6.job.job_id).status, 'done',
  '🚨 期限切れ処理は done を上書きしない (R1 #1)');

// #2 終わった job は結果で復活できない。ただし needs_review からの復旧は受ける
const dH = mkDraft('LP-H', 'ハッカ油スプレー 10L');
lp.requestJob(db, args(dH, s2.spec, 'key-0001', { now: min(80) }));
const c7 = lp.claimJob(db, { runnerRunId: 'run-11', now: min(80) });
const g7 = lp.reserveGeneration(db, c7.job.job_id, { leaseToken: c7.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(80) });
serve(c7.job, min(80));
db.prepare("UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?").run(c7.job.job_id);
eq(lp.submitResult(db, g7.generation_id, { packetHash: c7.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(81) }).code, 'job_finalized',
  '🚨 cancelled の依頼を結果で done に戻せない (R1 #2)');
db.prepare("UPDATE ph_lp_compose_jobs SET status = 'needs_review' WHERE id = ?").run(c7.job.job_id);
eq(lp.submitResult(db, g7.generation_id, { packetHash: c7.job.packet_hash, verdict: 'accepted', output: OUT, lint: LINT, now: min(82) }).status, 'done',
  'needs_review からの復旧は受ける (AI 枠を使っているので取りこぼさない)');

// #5 壊れた実行役が DB を肥らせたり例外を漏らしたりできない
const dI = mkDraft('LP-I', 'ハッカ油スプレー 20L');
lp.requestJob(db, args(dI, s2.spec, 'key-0001', { now: min(90) }));
const c8 = lp.claimJob(db, { runnerRunId: 'run-12', now: min(90) });
const g8 = lp.reserveGeneration(db, c8.job.job_id, { leaseToken: c8.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(90) });
serve(c8.job, min(90));
const cyc = {}; cyc.self = cyc;
eq(lp.submitResult(db, g8.generation_id, { packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT, lint: cyc, now: min(91) }).code, 'bad_request',
  '🚨 JSON にできない lint は bad_request (例外を漏らさない・R1 #5)');
eq(lp.submitResult(db, g8.generation_id, { packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT, lint: { big: 'x'.repeat(lp.LINT_MAX) }, now: min(91) }).code, 'bad_request',
  '大きすぎる lint も断る');
// 証跡は黙って直さない (切り捨てると「何を見て作ったか」として信用できない・R2 #4)
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: Array.from({ length: 50 }, (_, i) => ({ file_id: 'FILEID000001', sha256: 'a'.repeat(64), bytes: 1 })) }, now: min(91),
}).code, 'bad_request', '🚨 receipt の枚数超過は切り捨てず断る (R2 #4)');
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'abc', bytes: 1 }] }, now: min(91),
}).code, 'bad_request', 'sha256 が 16 進 64 桁でなければ断る');
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'a'.repeat(64) }] }, now: min(91),
}).code, 'bad_request', 'bytes が無ければ断る');
// R6: 末尾の改行を弾く (JS の `$` は multiline でなくても文字列末尾の改行の直前に一致する)
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: `${'d'.repeat(64)}\n`, bytes: 1 }] }, now: min(91),
}).code, 'bad_request', '🚨 sha256 の末尾改行を弾く (R6)');
ok(lp.requestJob(db, args(mkDraft('LP-L', 'テスト3'), s2.spec, 'key-0001\n', { now: min(96) })).code === 'bad_request',
  '🚨 idempotency_key の末尾改行を弾く (R6)');
ok(lp.requestJob(db, args({ ...dA, id: '12\n' }, s2.spec, 'key-000v', { now: min(96) })).code === 'bad_request',
  '🚨 ID の末尾改行を弾く (R6)');

const eqj = (x, y, l) => eq(JSON.stringify(x), JSON.stringify(y), l);
console.log('⑭ 材料固定を中身から確かめる (R7)');
// #1 claim は保存済みの hash を信じず、packet と仕様書を中身から計算し直して照合する
const dM = mkDraft('LP-M', 'ハッカ油スプレー 3L');
const rM = lp.requestJob(db, args(dM, s2.spec, 'key-0001', { now: min(100) }));
db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = ? WHERE id = ?`)
  .run(JSON.stringify({ ...JSON.parse(rM.job.packet_json), name: 'すり替えた商品名' }), rM.job.id);
ok(lp.claimJob(db, { runnerRunId: 'run-20', now: min(101) }).job === null, '🚨 packet を直接書き換えられたら claim しない (R7 #1)');
eq(db.prepare('SELECT error_code FROM ph_lp_compose_jobs WHERE id = ?').get(rM.job.id).error_code, 'packet_tampered', '理由が残る');

// #2 証跡に「渡していない画像」を混ぜられない
const dN = mkDraft('LP-N', 'ハッカ油スプレー 4L');
lp.requestJob(db, args(dN, s2.spec, 'key-0001', { now: min(110) }));
const cN = lp.claimJob(db, { runnerRunId: 'run-21', now: min(110) });
const gN = lp.reserveGeneration(db, cN.job.job_id, { leaseToken: cN.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(110) });
serve(cN.job, min(110));
// 🚨 このテストは PR1-c で書き直した。以前は「OTHERFILE999 を混ぜると bad_request」を
//    見ているつもりだったが、実際には `lint` を渡していなかったから落ちていただけで、
//    **証跡の検査は何も見ていなかった** (P1 で証跡をサーバ記録に移したときから)。
//    いまの保証は「**送られた receipt はそもそも使わない**」なので、そちらを固定する。
const subN = lp.submitResult(db, gN.generation_id, {
  packetHash: cN.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: LINT,
  receipt: { images: [{ file_id: 'OTHERFILE999', sha256: 'e'.repeat(64), bytes: 1 }] }, now: min(111),
});
eq(subN.status, 'done', 'サーバが配っていれば通る');
eq(subN.receipt.images.length, 1, '証跡は 1 枚');
eq(subN.receipt.images[0].file_id, 'FILEID000001',
  '🚨 証跡は**サーバが配った記録**。送ってきた OTHERFILE999 は使われない (R7 #2 の今の形)');

console.log('⑯ R9 の修正');
// #1 同じキーの再送は、材料の検証より先に既存 job を返す
//    (受付後に商品情報が消えても、通信リトライが not_ready にならない)
const dQ = mkDraft('LP-Q', 'ハッカ油スプレー 8L');
const rQ = lp.requestJob(db, args(dQ, s2.spec, 'key-0001', { now: min(140) }));
ok(rQ.ok && rQ.created, '受け付ける');
const rQ2 = lp.requestJob(db, args(dQ, s2.spec, 'key-0001', { now: min(141), productInfo: '' }));
ok(rQ2.ok && !rQ2.created && rQ2.job.id === rQ.job.id,
  '🚨 受付後に商品情報が消えても、同じキーの再送は既存の依頼を返す (R9 #1)');
// #2 packet の画像は重複させない (証跡と 1 対 1 で対応させるため)
const dR = mkDraft('LP-R', 'ハッカ油スプレー 9L');
const rR = lp.requestJob(db, args(dR, s2.spec, 'key-0001', {
  now: min(150),
  images: [{ file_id: 'FILEID000001' }, { file_id: 'FILEID000001' }, { file_id: 'FILEID000002' }],
}));
eq(JSON.parse(rR.job.packet_json).images.length, 2, '🚨 packet の画像は重複を取り除く (R9 #2)');
// この節で作った依頼をキューに残さない (後の節の claim が先に拾ってしまう)
db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled', completed_at = ? WHERE id IN (?, ?)`)
  .run(new Date(min(151)).toISOString(), rQ.job.id, rR.job.id);

console.log('⑮ R8 の修正');
// #1 packet と job 行の突き合わせ (それぞれの hash が正しくても、job の spec_id だけ書き換えれば別の仕様書を渡せた)
const dO = mkDraft('LP-O', 'ハッカ油スプレー 6L');
const rO = lp.requestJob(db, args(dO, sTabs.spec, 'key-0001', { now: min(120) }));
db.prepare('UPDATE ph_lp_compose_jobs SET spec_id = ?, spec_hash = ? WHERE id = ?').run(s2.spec.id, s2.spec.hash, rO.job.id);
ok(lp.claimJob(db, { runnerRunId: 'run-30', now: min(121) }).job === null, '🚨 job の spec_id だけすり替えても claim しない (R8 #1)');
eq(db.prepare('SELECT error_code FROM ph_lp_compose_jobs WHERE id = ?').get(rO.job.id).error_code, 'packet_tampered', '理由が残る');

// #2 同じ画像を並べて枚数を水増しできない
const dP = mkDraft('LP-P', 'ハッカ油スプレー 7L');
lp.requestJob(db, args(dP, s2.spec, 'key-0001', { now: min(130) }));
const cP = lp.claimJob(db, { runnerRunId: 'run-31', now: min(130) });
const gP = lp.reserveGeneration(db, cP.job.job_id, { leaseToken: cP.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(130) });
eq(lp.submitResult(db, gP.generation_id, {
  packetHash: cP.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [
    { file_id: 'FILEID000001', sha256: 'f'.repeat(64), bytes: 1 },
    { file_id: 'FILEID000001', sha256: 'f'.repeat(64), bytes: 1 },
  ] }, now: min(131),
}).code, 'bad_request', '🚨 同じ画像を 2 回並べられない (R8 #2)');
const subBig = lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, lint: LINT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'b'.repeat(64), bytes: 999, extra: 'x' }] }, now: min(91),
});
eq(subBig.status, 'done', '正しい証跡なら受け取れる');
eq(subBig.receipt.images.length, 1, '証跡が残る');
ok(subBig.receipt.images.every((im) => im.extra === undefined), '許した項目だけ残す');
// 🚨 証跡だけ違う再送は「同じ結果」に見なさない (R2 #1)
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'c'.repeat(64), bytes: 999 }] }, now: min(92),
}).code, 'already_finalized', '🚨 画像の証跡が違う再送は別物として断る (R2 #1)');
// posInt: "12abc" のような値を ID として通さない (R2 #2)
ok(lp.requestJob(db, args({ ...dA, id: '12abc' }, s2.spec, 'key-000z', { now: min(95) })).code === 'bad_request',
  '🚨 "12abc" を 12 として通さない (R2 #2)');
// 正規化で別入力を同一視しない (R3)
ok(lp.requestJob(db, args({ ...dA, id: ' 12' }, s2.spec, 'key-000w', { now: min(95) })).code === 'bad_request',
  '🚨 " 12" を 12 として通さない (R3)');
eq(lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'B'.repeat(64), bytes: 1 }] }, now: min(93),
}).code, 'bad_request', '🚨 sha256 の大文字は受けない (R3)');
// spec.hash を省いた照合の回避を防ぐ (R2 #3)
ok(lp.requestJob(db, args(mkDraft('LP-K', 'テスト2'), { ...s2.spec, hash: '' }, 'key-0001', { now: min(95) })).code === 'bad_request',
  '🚨 spec.hash を省くと通らない (R2 #3)');

// #4 不正な ID は受け付けない
ok(lp.requestJob(db, args({ ...dA, id: 'abc' }, s2.spec, 'key-000x', { now: min(95) })).code === 'bad_request',
  '🚨 商品 ID が数でなければ断る (R1 #4)');
ok(lp.requestJob(db, args(mkDraft('LP-J', 'テスト'), { ...s2.spec, id: 99999 }, 'key-0001', { now: min(95) })).code === 'bad_request',
  '存在しない仕様書の版は断る');

console.log('⑳ 🚨 古い版の packet は claim しない (codex exec review P2)');
{
  // 版が上がる = 渡す材料の形が変わった。v1 には instruction が無いので、
  // そのまま渡すと**スタッフと違う指示文で作ったものが測定に混ざる** (設計 §5 / §7.1)。
  // 先にキューを空にして、試す依頼を 1 件だけにする
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled', completed_at = ? WHERE status = 'queued'`)
    .run(new Date(min(199)).toISOString());
  const dOld = mkDraft('LP-OLD', 'ハッカ油スプレー 15ml');
  const rOld = lp.requestJob(db, args(dOld, s2.spec, 'key-old01', { now: min(200) }));
  ok(rOld.ok, '依頼は通る');
  // 受付済みの行を v1 に差し替える (デプロイをまたいだ古い依頼の再現)
  const old = JSON.parse(db.prepare('SELECT packet_json FROM ph_lp_compose_jobs WHERE id = ?').get(rOld.job.id).packet_json);
  delete old.instruction;
  old.packet_version = 1;
  db.prepare('UPDATE ph_lp_compose_jobs SET packet_json = ?, packet_hash = ?, packet_version = 1 WHERE id = ?')
    .run(JSON.stringify(old), lp.sha256(lp.canonicalJson(old)), rOld.job.id);
  ok(lp.claimJob(db, { runnerRunId: 'run-old', now: min(201) }).job === null, '🚨 古い版は掏ませない');
  const row = db.prepare('SELECT status, error_code, error FROM ph_lp_compose_jobs WHERE id = ?').get(rOld.job.id);
  eq(row.status, 'failed', '理由を残して failed にする');
  eq(row.error_code, 'packet_outdated', 'error_code が packet_outdated');
  ok((row.error || '').includes('もう一度依頼'), '人に何をすればよいかを書く');
}

console.log('⑩ 実際に本回答を書いたモデル (ランナーが後から付ける・codex #1591 High)');
{
  const spec = lp.latestSpec(db);
  const mk = (ne, run, t) => {
    const d = mkDraft(ne, 'ハッカ油スプレー ' + ne);   // lint (検査 17) は商品名の接頭辞を見る
    const r = lp.requestJob(db, args(d, spec, 'key-mc-' + ne, { now: min(t) }));
    const c = lp.claimJob(db, { runnerRunId: run, now: min(t + 0.1) });
    ok(r.ok && c.job && c.job.job_id === r.job.id, `${ne}: 受付と claim`);
    const g = lp.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(t + 0.2) });
    ok(g.ok, `${ne}: 予約`);
    return { draft: d, gid: g.generation_id, job: c.job };
  };
  const a = mk('MC-A', 'lpr-20261002-150000-aaaaaa', 200);
  for (const bad of ['', 'lpr x', 'lpr-1\n', 'x'.repeat(81), 12]) {
    eq(lp.recordModelCheck(db, { runnerRunId: bad, actualModels: [] }).code, 'bad_request', `run id の形が違えば断る: ${JSON.stringify(bad)}`);
  }
  for (const bad of ['claude-opus-5-5', null, {}]) {
    eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150000-aaaaaa', actualModels: bad }).code, 'bad_request', `actual_models は配列: ${JSON.stringify(bad)}`);
  }
  for (const bad of [['claude-opus-5-5[1m]'], ['claude-opus-5-5 '], ['gpt-5.6-sol'], ['claude-opus-5-5\n'], Array(11).fill('claude-opus-5-5')]) {
    eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150000-aaaaaa', actualModels: bad }).code, 'bad_request', `形の違うモデルは断る: ${JSON.stringify(bad).slice(0, 60)}`);
  }
  eq(db.prepare('SELECT model_check FROM ph_lp_compose_generations WHERE id = ?').get(a.gid).model_check, null, '断った呼び出しでは何も付かない');
  eq(lp.jobStateFor(db, a.draft.id, { now: min(200.5) }).job.model_check, null, '付くまでは null (画面は「確認中」)');
  eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-other', actualModels: ['claude-opus-5-5'] }).updated, 0, '別の run id の generation には付かない');
  const ra = lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150000-aaaaaa', actualModels: ['claude-opus-5-5'] });
  ok(ra.ok && ra.updated === 1 && ra.checks[0].model_check === 'match', '🚨 [1m] を除いて頼んだモデルと同じなら一致');
  eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150000-aaaaaa', actualModels: ['claude-sonnet-5'] }).updated, 0, '🚨 一度付けたら書き換えない');
  const sa = lp.jobStateFor(db, a.draft.id, { now: min(200.6) }).job;
  ok(sa.model_check === 'match' && sa.actual_model === 'claude-opus-5-5', '画面の状態に出る (一致・実モデル)');

  const b = mk('MC-B', 'lpr-20261002-150100-bbbbbb', 210);
  const rb = lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150100-bbbbbb', actualModels: ['claude-opus-5-5', 'claude-sonnet-5', 'claude-opus-5-5'] });
  eq(rb.checks[0].model_check, 'mismatch', '🚨 1 つでも違うモデルが本回答を書いていれば不一致');
  eq(lp.jobStateFor(db, b.draft.id, { now: min(210.5) }).job.actual_model, 'claude-opus-5-5,claude-sonnet-5', '実モデルは重ねずに全部残す');

  const c = mk('MC-C', 'lpr-20261002-150200-cccccc', 220);
  eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150200-cccccc', actualModels: [] }).checks[0].model_check, 'unknown', '🚨 読めなければ未確認 (一致とは扱わない)');
  eq(lp.jobStateFor(db, c.draft.id, { now: min(220.5) }).job.actual_model, null, '未確認なら実モデルは空');

  // 結果と確認の順番によらず、一致でなければ使わない (codex #1591 R2 High)
  const submit = (x, t) => {
    serve(x.job, min(t));
    return lp.submitResult(db, x.gid, { packetHash: x.job.packet_hash, verdict: 'accepted', output: compositionFor(x.draft.name), lint: LINT, reviewRounds: 1, now: min(t + 0.1) });
  };
  const sb = submit(b, 211);
  eq(sb.status, 'needs_review', '🚨 先に不一致が付いていた依頼は、結果が届いても done にしない');
  const stB = lp.jobStateFor(db, b.draft.id, { now: min(212) }).job;
  ok(stB.status === 'needs_review' && stB.error_code === 'model_mismatch' && stB.output_text === null, '🚨 不一致は needs_review・本文を出さない');
  ok((stB.error || '').includes('claude-sonnet-5'), `理由に実モデルが出る (${stB.error})`);

  const d = mk('MC-D', 'lpr-20261002-150300-dddddd', 230);
  eq(submit(d, 231).status, 'done', '確認前の結果はいったん done');
  eq(lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150300-dddddd', actualModels: ['claude-sonnet-5'], now: min(232) }).checks[0].model_check, 'mismatch', '後から不一致');
  const stD = lp.jobStateFor(db, d.draft.id, { now: min(233) }).job;
  ok(stD.status === 'needs_review' && stD.error_code === 'model_mismatch' && stD.output_text === null, '🚨 done の後に不一致が付いたら needs_review に落ちて本文を出さない');
  const reD = lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150300-dddddd', actualModels: ['claude-opus-5-5'], now: min(234) });
  ok(reD.ok && reD.updated === 0 && reD.already === true && reD.checks[0].model_check === 'mismatch', '🚨 再送には付いている結果を返す (一致に塗り替えない・codex #1591 R2 Medium)');

  const e = mk('MC-E', 'lpr-20261002-150400-eeeeee', 240);
  eq(submit(e, 241).status, 'done', '確認が来ない依頼もいったん done');
  eq(lp.requestJob(db, args(e.draft, spec, 'key-mc-E-again', { now: min(242) })).code, 'already_running',
    '🚨 確認待ちの間は再依頼を受けない (AI 枠の二重使用・先の結果が見えなくなるのを防ぐ・codex #1591 R4 Medium)');
  eq(lp.jobStateFor(db, e.draft.id, { now: min(241 + lp.MODEL_CHECK_WAIT_MIN - 1) }).job.status, 'done', `${lp.MODEL_CHECK_WAIT_MIN} 分たつまでは待つ (確認中)`);
  const stE = lp.jobStateFor(db, e.draft.id, { now: min(241.2 + lp.MODEL_CHECK_WAIT_MIN) }).job;
  ok(stE.status === 'needs_review' && stE.error_code === 'model_unverified' && stE.model_check === 'unknown' && stE.output_text === null,
    `🚨 ${lp.MODEL_CHECK_WAIT_MIN} 分たっても確認が来なければ未確認で閉じる (確認中のまま残さない)`);
  ok(lp.requestJob(db, args(e.draft, spec, 'key-mc-E-again2', { now: min(258) })).ok, '閉じた後は再依頼できる (ずっと押せなくはならない)');
  {
    // 後の試験 (F) の claim がこれを掴まないように片付ける
    const ce = lp.claimJob(db, { runnerRunId: 'run-e-again', now: min(258.1) });
    lp.failJob(db, ce.job.job_id, { leaseToken: ce.job.lease_token, code: 'other', message: '試験の片付け', now: min(258.2) });
  }

  // 🚨 ランナーが止まっていて、15 分を過ぎてから再送が**最初に**届いても「一致」にしない (codex #1591 R3 Medium)
  const fx = mk('MC-F', 'lpr-20261002-150500-ffffff', 260);
  eq(submit(fx, 261).status, 'done', 'F: いったん done');
  const late = lp.recordModelCheck(db, { runnerRunId: 'lpr-20261002-150500-ffffff', actualModels: ['claude-opus-5-5'], now: min(261.2 + lp.MODEL_CHECK_WAIT_MIN + 1) });
  ok(late.ok && late.updated === 0 && late.already === true && late.checks[0].model_check === 'unknown',
    `🚨 ${lp.MODEL_CHECK_WAIT_MIN} 分を過ぎた再送は先に閉じてから見る (一致にしない・付いている「未確認」を返す)`);
  const stF = lp.jobStateFor(db, fx.draft.id, { now: min(280) }).job;
  ok(stF.status === 'needs_review' && stF.output_text === null, '本文は出ない');

  // 「完了」の記録は一致してから (codex #1591 R3 Low)
  const evs = (draftId) => db.prepare('SELECT event FROM draft_events WHERE draft_id = ? ORDER BY id').all(draftId).map((r) => r.event);
  ok(evs(dA.id).includes('lp_compose_result') && evs(dA.id).includes('lp_compose_done'), '一致した依頼: 結果を受け取った → 完了 の順に残る');
  ok(evs(dA.id).indexOf('lp_compose_result') < evs(dA.id).indexOf('lp_compose_done'), '完了は一致の後');
  ok(!evs(d.draft.id).includes('lp_compose_done') && evs(d.draft.id).includes('lp_compose_model_mismatch'), '🚨 後から不一致: 完了は残らず、不一致が残る');
  ok(!evs(b.draft.id).includes('lp_compose_done') && evs(b.draft.id).includes('lp_compose_model_mismatch'), '🚨 先に不一致: 完了は残らず、不一致が残る');
  ok(!evs(e.draft.id).includes('lp_compose_done') && evs(e.draft.id).includes('lp_compose_model_unverified'), '🚨 未確認で閉じた: 完了は残らず、未確認が残る');

  // 白抜きも証跡の対象 (配って見ていなければ accepted にしない・codex #1592 Low)
  const dW = mkDraft('MC-W', 'ハッカ油スプレー MC-W');
  const rW = lp.requestJob(db, args(dW, spec, 'key-mc-W', {
    now: min(3000),
    images: [{ file_id: 'FILEIDWHITE01', role: 'white_bg' }, { file_id: 'FILEID000001', role: 'slot:1' }, { file_id: 'FILEIDWHITE01', role: 'slot:2' }],
  }));
  const pW = JSON.parse(rW.job.packet_json);
  ok(pW.images.length === 2 && pW.images[0].role === 'white_bg' && pW.images[1].role === 'slot:1', '白抜きが先頭・同じファイルのスロットは 1 回 (役割は先に来た白抜き)');
  eq(lp.imagePlan(pW.images).map((im) => im.label).join(' → '), '白抜き → 1 TOP', '画面に出す並び');
  eq(lp.buildPacket({ draft: dW, productInfo: 'x', colorVariations: '', images: [{ file_id: 'FILEIDX00001', role: 'evil' }], spec }).packet.images[0].role, null, '知らない役割は残さない');
  const cW = lp.claimJob(db, { runnerRunId: 'lpr-20261002-150600-wwwwww', now: min(3000.1) });
  const gW = lp.reserveGeneration(db, cW.job.job_id, { leaseToken: cW.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(3000.2) });
  lp.recordImageServed(db, cW.job.job_id, { leaseToken: cW.job.lease_token, fileId: 'FILEID000001', sha256: '1'.repeat(64), bytes: 10, now: min(3000.3) });
  const subW1 = lp.submitResult(db, gW.generation_id, { packetHash: cW.job.packet_hash, verdict: 'accepted', output: compositionFor(dW.name), lint: LINT, reviewRounds: 1, now: min(3000.4) });
  ok(!subW1.ok && /見ていません/.test(subW1.error || ""), `🚨 白抜きを配っていなければ accepted にしない (${subW1.error})`);
  lp.recordImageServed(db, cW.job.job_id, { leaseToken: cW.job.lease_token, fileId: 'FILEIDWHITE01', sha256: '2'.repeat(64), bytes: 10, now: min(3000.5) });
  const subW2 = lp.submitResult(db, gW.generation_id, { packetHash: cW.job.packet_hash, verdict: 'accepted', output: compositionFor(dW.name), lint: LINT, reviewRounds: 1, now: min(3000.6) });
  ok(subW2.ok && subW2.status === 'done', '白抜きも配って見ていれば受け取る');
  ok(subW2.receipt.images.map((im) => im.file_id).sort().join(',') === 'FILEID000001,FILEIDWHITE01', '証跡に白抜きも載る');
}

console.log('⑭ 素材画像 = 画像フォルダの下のフォルダの画像 (2026-10-02 中原さん「商品 6 + 素材 10」)');
{
  eq(lp.MAX_PRODUCT_IMAGES, 6, '商品画像は 6 枚まで');
  eq(lp.MAX_MATERIAL_IMAGES, 10, '素材画像は 10 枚まで');
  eq(lp.MAX_IMAGES, 16, '配るのは合わせて 16 枚まで');
  const products = Array.from({ length: 7 }, (_, i) => ({ file_id: 'FILEIDPROD0' + i, role: i === 0 ? 'white_bg' : 'slot:' + i }));
  const materials = Array.from({ length: 12 }, (_, i) => ({ file_id: 'FILEIDMATE' + String(i).padStart(2, '0'), role: 'material', name: `m${i}.jpg`, folder: i < 6 ? '素材' : '素材/使用イメージ' }));
  const dM = mkDraft('MAT-1', 'ハッカ油スプレー MAT');
  const rM = lp.requestJob(db, args(dM, lp.latestSpec(db), 'key-mat-1', { now: min(4000), images: [...products, ...materials, { file_id: 'FILEIDPROD00', role: 'material', name: 'dup.jpg' }] }));
  ok(rM.ok, '受け付ける');
  const pM = JSON.parse(rM.job.packet_json);
  eq(pM.packet_version, 4, 'packet の版 = 4 (素材が入った)');
  eq(pM.images.length, 16, '🚨 商品 6 + 素材 10 = 16 枚');
  eqj(pM.images.slice(0, 6).map((im) => im.role), ['white_bg', 'slot:1', 'slot:2', 'slot:3', 'slot:4', 'slot:5'], '先に商品画像 (6 枚まで・7 枚目は入らない)');
  eqj(pM.images.slice(6).map((im) => im.role), Array.from({ length: 10 }, (_, i) => 'material:' + (i + 1)), '続けて素材 (material:1〜10 を振り直す)');
  ok(pM.images[6].name === 'm0.jpg' && pM.images[6].folder === '素材', '素材は名前と場所つき');
  eq(pM.materials_omitted, 2, '🚨 上限で入らなかった素材の数が残る (12 − 10)');
  ok(!pM.images.some((im) => im.name === 'dup.jpg'), '商品画像と同じファイルの素材は入れない (重複)');
  eqj(lp.imagePlan(pM.images).slice(5, 8).map((im) => im.label), ['5', '素材1', '素材2'], '画面に出す名前: 素材N');
  ok(lp.imagePlan(pM.images)[6].folder === '素材' && lp.imagePlan(pM.images)[6].name === 'm0.jpg', '画面の並びにも名前と場所');
  ok(lp.isMaterialRole('material') && lp.isMaterialRole('material:10') && !lp.isMaterialRole('material:0') && !lp.isMaterialRole('materialx'), '素材の役割の形');
  const dM2 = mkDraft('MAT-2', 'ハッカ油スプレー MAT2');
  eq(lp.requestJob(db, args(dM2, lp.latestSpec(db), 'key-mat-2', { now: min(4001), images: materials })).code, 'not_ready',
    '🚨 素材だけで商品画像が無ければ受け付けない (商品を再現できない)');

  // 添付画像の説明 = AI にもスタッフにも同じ文 (くらべっこを公平にする・codex #1593 High・中原さん「A」)
  const g = pM.image_guide;
  ok(typeof g === 'string' && g.startsWith('【添付画像の説明】'), 'packet に添付画像の説明が入る (受付時に固定・hash で守られる)');
  ok(g.includes('1枚目: 商品画像 (白抜き)') && g.includes('2枚目: 商品画像 (1 TOP)'), '何枚目が商品画像か');
  ok(g.includes('7枚目: 素材画像 (素材1・素材/m0.jpg)') && g.includes('16枚目: 素材画像 (素材10・'), '何枚目が素材か (場所と名前つき)');
  ok(g.includes('「使用素材」') && g.includes('上の一覧に無い素材を作ったり'), '素材の使い方の決まりが入る');
  ok(g.includes('ほかに 2 枚ありましたが添付していません'), '入らなかった素材の数');
  eq(lp.jobStateFor(db, dM.id, { now: min(4000.5) }).job.image_guide, g, '🚨 画面 (スタッフのコピー用) にも同じ文が渡る');
  const g0 = lp.buildImageGuide([{ file_id: 'X', role: 'white_bg' }]);
  ok(g0.includes('1枚目: 商品画像 (白抜き)') && g0.includes('素材画像はありません') && !g0.includes('使用素材'), '素材が無ければ素材の決まりは書かない');
  eq(lp.buildImageGuide([]), '', '画像が無ければ空');

  // 実行役が落とせる枚数 (codex #1593 Medium): 言わない古い phlp (6 枚まで) には 16 枚の依頼を掴ませない
  const cOld = lp.claimJob(db, { runnerRunId: 'run-old-phlp', now: min(4000.6) });
  ok(!cOld.job || cOld.job.job_id !== rM.job.id, '🚨 枚数を言わない実行役は 16 枚の依頼を掴まない');
  if (cOld.job) lp.releaseJob(db, cOld.job.job_id, { leaseToken: cOld.job.lease_token, now: min(4000.65) });
  const cOld2 = lp.claimJob(db, { runnerRunId: 'run-old-phlp', maxImages: 6, now: min(4000.7) });
  ok(!cOld2.job || cOld2.job.job_id !== rM.job.id, '6 枚と言う実行役も掴まない');
  if (cOld2.job) lp.releaseJob(db, cOld2.job.job_id, { leaseToken: cOld2.job.lease_token, now: min(4000.75) });
  eq(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(rM.job.id).status, 'queued', '掴まれなかった依頼は queued のまま (新しい phlp が拾う)');
  // 🚨 6 枚以下でも素材つきなら、枚数を言わない古い phlp には掴ませない (古いスキルは素材を知らない・codex #1593 R2 Medium)
  const dS = mkDraft('MAT-S', 'ハッカ油スプレー MATS');
  const rS = lp.requestJob(db, args(dS, lp.latestSpec(db), 'key-mat-s', { now: min(4000.61), images: [{ file_id: 'FILEIDSMALL1', role: 'slot:1' }, { file_id: 'FILEIDSMALLM', role: 'material', name: 's.jpg', folder: '素材' }] }));
  ok(rS.ok, '商品 1 + 素材 1 の依頼');
  for (let i = 0; i < 20; i++) {
    const c = lp.claimJob(db, { runnerRunId: 'run-old-phlp-s', now: min(4000.62) });
    ok(!c.job || c.job.job_id !== rS.job.id, i === 0 ? '🚨 枚数を言わない古い phlp は、素材つきなら 2 枚でも掴まない' : '(続き)');
    if (!c.job) { ok(c.too_many_images >= 1 && /素材つき/.test(c.error || ''), `掴めない理由を返す (${c.error})`); break; }
    lp.releaseJob(db, c.job.job_id, { leaseToken: c.job.lease_token, now: min(4000.62) });
    db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(c.job.job_id);
  }
  eq(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(rS.job.id).status, 'queued', '素材つきの依頼は queued のまま');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(rS.job.id);   // 下の claim が拾わないように
  let cNew = null;
  for (let i = 0; i < 20; i++) {
    cNew = lp.claimJob(db, { runnerRunId: 'lpr-20261003-090000-nnnnnn', maxImages: 16, now: min(4000.8) });
    if (!cNew.job || cNew.job.job_id === rM.job.id) break;
    lp.releaseJob(db, cNew.job.job_id, { leaseToken: cNew.job.lease_token, now: min(4000.8) });
    db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(cNew.job.job_id);
  }
  eq(cNew.job?.job_id, rM.job.id, '16 枚と言う実行役は掴む');

  // 16 枚の配布 → 証跡 → accepted (codex #1593 Low)
  const gM = lp.reserveGeneration(db, cNew.job.job_id, { leaseToken: cNew.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: min(4000.9) });
  ok(gM.ok, '予約できる');
  pM.images.forEach((im, i) => {
    if (i < 15) lp.recordImageServed(db, cNew.job.job_id, { leaseToken: cNew.job.lease_token, fileId: im.file_id, sha256: String(i % 10).repeat(64), bytes: 10 + i, now: min(4001) });
  });
  const s15 = lp.submitResult(db, gM.generation_id, { packetHash: cNew.job.packet_hash, verdict: 'accepted', output: compositionFor(dM.name), lint: LINT, reviewRounds: 1, now: min(4001.1) });
  ok(!s15.ok && /16 枚のうち 1 枚を見ていません/.test(s15.error || ''), `🚨 15 枚しか配っていなければ受け取らない (${s15.error})`);
  const r16 = lp.recordImageServed(db, cNew.job.job_id, { leaseToken: cNew.job.lease_token, fileId: pM.images[15].file_id, sha256: 'e'.repeat(64), bytes: 99, now: min(4001.2) });
  ok(r16.ok && r16.count === 16, '16 枚目 (素材10) の配布も記録される (6 枚で切らない)');
  const s16 = lp.submitResult(db, gM.generation_id, { packetHash: cNew.job.packet_hash, verdict: 'accepted', output: compositionFor(dM.name), lint: LINT, reviewRounds: 1, now: min(4001.3) });
  ok(s16.ok && s16.receipt.images.length === 16, '16 枚全部配っていれば受け取る・証跡も 16 枚');

  // 先の検査 (Drive を読む前・codex #1593 Medium)
  const dP = mkDraft('MAT-P', 'ハッカ油スプレー MATP');
  const pre = (extra) => lp.requestPrecheck(db, { draft: dP, productInfo: 'x', spec: lp.latestSpec(db), images: [{ file_id: 'FILEIDPRE001', role: 'slot:1' }], idempotencyKey: 'key-pre-0001', now: min(4002), ...extra });
  eq(pre(), null, '受け付けられるなら null (このあと Drive を読む)');
  eq(pre({ idempotencyKey: 'x' }).code, 'bad_request', 'キーの形が違えば Drive を読まずに断る');
  eq(pre({ images: [] }).code, 'not_ready', '押せない理由があれば Drive を読まずに断る');
  ok(lp.requestJob(db, args(dP, lp.latestSpec(db), 'key-pre-0001', { now: min(4002), images: [{ file_id: 'FILEIDPRE001', role: 'slot:1' }] })).ok, '依頼する');
  ok(pre().prior === true, '同じキーの再送は prior (Drive を読まずに前の依頼を返す)');
  eq(pre({ idempotencyKey: 'key-pre-0002' }).code, 'already_running', '動いている依頼があれば Drive を読まずに断る');
  process.env.PH_LP_COMPOSE_ENABLED = '0';
  eq(pre({ idempotencyKey: 'key-pre-0003' }).code, 'disabled', '機能 OFF なら Drive を読まずに断る');
  process.env.PH_LP_COMPOSE_ENABLED = '1';
}

console.log('⑮ 素材の一覧 (Drive の画像フォルダのサブフォルダを何階層下まで辿る)');
{
  const { listDriveFolderMaterialImages } = await import('../apps/product-hub/services/rakuten-listing.js');
  // 作り物の Drive: ROOT の直下 = 商品画像 (素材ではない)。サブフォルダの下が素材
  const FOLDER = 'application/vnd.google-apps.folder';
  const tree = {
    ROOT00000001: [{ id: 'IMGROOT00001', name: 'top.jpg', mimeType: 'image/jpeg' }, { id: 'FOLDERA00001', name: '素材', mimeType: FOLDER }, { id: 'FOLDERB00001', name: 'イメージ', mimeType: FOLDER }],
    FOLDERA00001: [{ id: 'IMGA00000002', name: 'b.jpg', mimeType: 'image/jpeg' }, { id: 'IMGA00000001', name: 'a.jpg', mimeType: 'image/jpeg' }, { id: 'FOLDERA10001', name: '深い', mimeType: FOLDER }, { id: 'TXT000000001', name: 'メモ.txt', mimeType: 'text/plain' }],
    FOLDERA10001: [{ id: 'IMGDEEP00001', name: 'x.png', mimeType: 'image/png' }, { id: 'ROOT00000001', name: 'ループ', mimeType: FOLDER }],
    FOLDERB00001: [{ id: 'IMGB00000001', name: 'c.jpg', mimeType: 'image/jpeg' }],
  };
  const fakeDrive = {
    calls: 0,
    files: {
      list: async ({ q }) => {
        fakeDrive.calls++;
        const parent = /'([^']+)' in parents/.exec(q)[1];
        const kids = tree[parent] || [];
        const files = q.includes(`mimeType = '${FOLDER}'`) ? kids.filter((k) => k.mimeType === FOLDER)
          : q.includes("mimeType contains 'image/'") ? kids.filter((k) => k.mimeType.startsWith('image/')) : kids;
        return { data: { files: files.map((f) => ({ ...f, modifiedTime: '2026-10-01T00:00:00Z' })) } };
      },
    },
  };
  const got = await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive });
  eqj(got.map((g) => g.folder + '/' + g.name).sort(), ['イメージ/c.jpg', '素材/a.jpg', '素材/b.jpg', '素材/深い/x.png'].sort(), '🚨 サブフォルダの画像を何階層下まで拾う (根の画像・画像でないものは入らない)');
  ok(!got.some((g) => g.id === 'IMGROOT00001'), '🚨 根 (商品の画像フォルダの直下) の画像は素材にしない');
  const keys = got.map((g) => g.folder + '\u0000' + g.name);
  eqj(keys, [...keys].sort(), '並びは「フォルダ → 名前」順 (同じ材料なら毎回同じ)');
  ok(got.every((g) => g.modifiedTime && g.id), '更新日時と ID がある');
  // 歯止め: 一部だけの一覧を「全部」として渡さない (throw する)
  let e1 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive, limits: { maxDepth: 6, maxFolders: 2, maxImages: 500 } }); } catch (e) { e1 = e; }
  ok(e1 && /サブフォルダが 2 個/.test(e1.message), `🚨 フォルダが多すぎれば止める (${e1?.message})`);
  let e2 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive, limits: { maxDepth: 1, maxFolders: 60, maxImages: 500 } }); } catch (e) { e2 = e; }
  ok(e2 && /1 階層より深く/.test(e2.message), `🚨 深すぎれば止める (${e2?.message})`);
  let e3 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive, limits: { maxDepth: 6, maxFolders: 60, maxImages: 2 } }); } catch (e) { e3 = e; }
  ok(e3 && /2 (枚|件)を超えて/.test(e3.message), `🚨 画像が多すぎれば止める (${e3?.message})`);
  let e4 = null; try { await listDriveFolderMaterialImages("x' or '1'='1", { drive: fakeDrive }); } catch (e) { e4 = e; }
  ok(e4 && /ID の形/.test(e4.message), '🚨 フォルダ ID の形を確かめる (Drive の検索式に混ぜない)');
  // 時間・回数・ページの進み方・形のおかしい ID (codex #1593 Medium / Low)
  let e5 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive, limits: { deadlineMs: 0 } }); } catch (e) { e5 = e; }
  ok(e5 && /秒で読み終わりませんでした/.test(e5.message), `🚨 時間の上限で止める (${e5?.message})`);
  let e6 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: fakeDrive, limits: { maxRequests: 3 } }); } catch (e) { e6 = e; }
  ok(e6 && /3 回より多く/.test(e6.message), `🚨 呼び出し回数の上限で止める (${e6?.message})`);
  const loopDrive = { files: { list: async () => ({ data: { files: [], nextPageToken: 'SAME' } }) } };
  let e7 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: loopDrive }); } catch (e) { e7 = e; }
  ok(e7 && /ページが進みません/.test(e7.message), `🚨 同じ次ページが続けば止める (${e7?.message})`);
  const badIdDrive = { files: { list: async ({ q }) => ({ data: { files: q.includes("vnd.google-apps.folder") && q.includes('ROOT00000001') ? [{ id: "bad id'", name: 'x' }] : [] } }) } };
  let e8 = null; try { await listDriveFolderMaterialImages('ROOT00000001', { drive: badIdDrive }); } catch (e) { e8 = e; }
  ok(e8 && /形のおかしいフォルダ ID/.test(e8.message), `🚨 形のおかしい子フォルダ ID は飛ばさず止める (${e8?.message})`);
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
