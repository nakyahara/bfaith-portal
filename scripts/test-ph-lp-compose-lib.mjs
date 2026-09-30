import { temporaryTestDataDir } from './test-temp-dir.mjs';
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
const r1 = lp.requestJob(db, args(dA, sTabs.spec, 'key-0001'));
ok(r1.ok && r1.created, '受け付ける');
const packet1 = JSON.parse(r1.job.packet_json);
eq(packet1.spec_id, sTabs.spec.id, 'packet に受付時の仕様書の版が入る');
eq(packet1.images.length, 1, '画像も packet に固定される');
const r1b = lp.requestJob(db, args(dA, sTabs.spec, 'key-0001'));
ok(r1b.ok && !r1b.created && r1b.job.id === r1.job.id, '同じ idempotency_key は同じ依頼を返す (二重クリック)');
ok(lp.requestJob(db, args(dA, sTabs.spec, 'key-0002')).code === 'already_running', '動いている間は 2 件目を受け付けない');
ok(r1.job.measurement_deadline_at === new Date(min(3)).toISOString(), '測定の期限 = 受付 + 3 分で固定 (R4 #2)');

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
const g1 = lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(0.8) });
ok(g1.ok && g1.generation_id > 0, '予約できる');
ok(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(0.8) }).code === 'already_reserved', '1 依頼 1 回');
ok(lp.reserveGeneration(db, c2.job.job_id, { leaseToken: c2.job.lease_token, model: 'claude-opus-5', promptVersion: 'ふるい版', now: min(0.8) }).code === 'bad_prompt_version', '🚨 実行役の prompt 版が違えば AI を呼ぶ前に断る (コード R1 #6)');
ok(lp.failJob(db, c2.job.job_id, { leaseToken: c2.job.lease_token, code: 'x', now: min(0.9) }).code === 'already_reserved',
  '🚨 予約後に fail は使えない (result で rejected を出す・§4.3b)');
ok(lp.releaseJob(db, c2.job.job_id, { leaseToken: c2.job.lease_token, now: min(0.9) }).code === 'already_reserved',
  '🚨 予約後に release は使えない (二重に呼べてしまう)');

console.log('⑥ 結果 — accepted');
const OUT = '# LP制作システム V2.1\n\n## ⑦ AI画像生成プロンプト\n… 本文 …';
ok(lp.submitResult(db, g1.generation_id, { packetHash: 'ちがう', verdict: 'accepted', output: OUT, now: min(1) }).code === 'packet_mismatch',
  '材料が予約時と違えば受け取らない');
const sub1 = lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'accepted', output: OUT, lint: { ok: true }, reviewRounds: 1, receipt: { images: [{ file_id: 'FILEID000001', sha256: 'a'.repeat(64), bytes: 12345 }] }, now: min(1) });
eq(sub1.status, 'done', '受け取ると done');
const jDone = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(c2.job.job_id);
eq(jDone.output_text, OUT, '本文が保存される');
ok(!!jDone.completed_at, '🚨 終端では completed_at が必ず入る (R4 #2)');
ok(jDone.completed_at <= jDone.measurement_deadline_at, '3 分以内に終わったと後から計算できる');
const sub1b = lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'accepted', output: OUT, lint: { ok: true }, reviewRounds: 1, receipt: { images: [{ file_id: 'FILEID000001', sha256: 'a'.repeat(64), bytes: 12345 }] }, now: min(1.5) });
ok(sub1b.ok && sub1b.already, '同じ結果の再送は保存済みを返す (応答断のリトライ)');
ok(lp.submitResult(db, g1.generation_id, { packetHash: c2.job.packet_hash, verdict: 'rejected', reason: 'ちがう内容', now: min(1.5) }).code === 'already_finalized',
  '別の内容では上書きできない');

console.log('⑦ 結果 — rejected (lint / 検品が通らなかった)');
const dB = mkDraft('LP-B', 'ハッカ油スプレー 50ml');
const r2 = lp.requestJob(db, args(dB, s2.spec, 'key-0001', { now: min(10) }));
const c3 = lp.claimJob(db, { runnerRunId: 'run-4', now: min(10) });
const g2 = lp.reserveGeneration(db, c3.job.job_id, { leaseToken: c3.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(10) });
const sub2 = lp.submitResult(db, g2.generation_id, { packetHash: c3.job.packet_hash, verdict: 'rejected', lint: { missing: ['# 0枚目｜サムネイル'] }, reviewRounds: 2, reason: '2 巡で通らなかった', now: min(11) });
eq(sub2.status, 'failed', '通らなければ failed');
const jRej = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(c3.job.job_id);
eq(jRej.error_code, 'rejected', '理由が残る');
ok(!!jRej.completed_at, 'rejected でも completed_at が入る');
eq(db.prepare('SELECT status FROM ph_lp_compose_generations WHERE id = ?').get(g2.generation_id).status, 'rejected',
  '🚨 generation も確定する (reserved のまま残さない)');

console.log('⑧ 成否不明 (lease 切れ) は needs_review で止まる');
const dC = mkDraft('LP-C', 'ハッカ油スプレー 200ml');
lp.requestJob(db, args(dC, s2.spec, 'key-0001', { now: min(20) }));
const c4 = lp.claimJob(db, { runnerRunId: 'run-5', now: min(20) });
lp.reserveGeneration(db, c4.job.job_id, { leaseToken: c4.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(20) });
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
const g6 = lp.reserveGeneration(db, c6.job.job_id, { leaseToken: c6.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(70) });
lp.submitResult(db, g6.generation_id, { packetHash: c6.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1, now: min(71) });
lp.recoverExpired(db, min(70 + lp.LEASE_MIN + 5));
eq(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(c6.job.job_id).status, 'done',
  '🚨 期限切れ処理は done を上書きしない (R1 #1)');

// #2 終わった job は結果で復活できない。ただし needs_review からの復旧は受ける
const dH = mkDraft('LP-H', 'ハッカ油スプレー 10L');
lp.requestJob(db, args(dH, s2.spec, 'key-0001', { now: min(80) }));
const c7 = lp.claimJob(db, { runnerRunId: 'run-11', now: min(80) });
const g7 = lp.reserveGeneration(db, c7.job.job_id, { leaseToken: c7.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(80) });
db.prepare("UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?").run(c7.job.job_id);
eq(lp.submitResult(db, g7.generation_id, { packetHash: c7.job.packet_hash, verdict: 'accepted', output: OUT, now: min(81) }).code, 'job_finalized',
  '🚨 cancelled の依頼を結果で done に戻せない (R1 #2)');
db.prepare("UPDATE ph_lp_compose_jobs SET status = 'needs_review' WHERE id = ?").run(c7.job.job_id);
eq(lp.submitResult(db, g7.generation_id, { packetHash: c7.job.packet_hash, verdict: 'accepted', output: OUT, now: min(82) }).status, 'done',
  'needs_review からの復旧は受ける (AI 枠を使っているので取りこぼさない)');

// #5 壊れた実行役が DB を肥らせたり例外を漏らしたりできない
const dI = mkDraft('LP-I', 'ハッカ油スプレー 20L');
lp.requestJob(db, args(dI, s2.spec, 'key-0001', { now: min(90) }));
const c8 = lp.claimJob(db, { runnerRunId: 'run-12', now: min(90) });
const g8 = lp.reserveGeneration(db, c8.job.job_id, { leaseToken: c8.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(90) });
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
const gN = lp.reserveGeneration(db, cN.job.job_id, { leaseToken: cN.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(110) });
eq(lp.submitResult(db, gN.generation_id, {
  packetHash: cN.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [{ file_id: 'OTHERFILE999', sha256: 'e'.repeat(64), bytes: 1 }] }, now: min(111),
}).code, 'bad_request', '🚨 渡していない画像を証跡に混ぜられない (R7 #2)');
eq(lp.submitResult(db, gN.generation_id, {
  packetHash: cN.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1,
  receipt: { images: [{ file_id: 'FILEID000001', sha256: 'e'.repeat(64), bytes: 1 }] }, now: min(111),
}).status, 'done', '渡した画像なら通る');

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
const gP = lp.reserveGeneration(db, cP.job.job_id, { leaseToken: cP.job.lease_token, model: 'claude-opus-5', promptVersion: lp.PROMPT_VERSION, now: min(130) });
eq(lp.submitResult(db, gP.generation_id, {
  packetHash: cP.job.packet_hash, verdict: 'accepted', output: OUT,
  receipt: { images: [
    { file_id: 'FILEID000001', sha256: 'f'.repeat(64), bytes: 1 },
    { file_id: 'FILEID000001', sha256: 'f'.repeat(64), bytes: 1 },
  ] }, now: min(131),
}).code, 'bad_request', '🚨 同じ画像を 2 回並べられない (R8 #2)');
const subBig = lp.submitResult(db, g8.generation_id, {
  packetHash: c8.job.packet_hash, verdict: 'accepted', output: OUT, reviewRounds: 1,
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

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
