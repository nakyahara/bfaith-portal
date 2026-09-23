import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-adkw-ai-');
/**
 * SP広告KW の夜間 AI — 依頼・固定 packet・予約・結果 (apps/product-hub/lib/ad-kw-ai.js・PR3a)
 * 実行: node scripts/test-ph-ad-kw-ai.mjs
 * 設計 = 正本 §5「PR3 実装計画 v2 / v2.1」の検証一覧 (応答断の再送・同時再送・破棄後の再送・期限切れ token・予約後の成否不明 ほか)
 */
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const ak = await import('../apps/product-hub/lib/ad-keywords.js');
const ai = await import('../apps/product-hub/lib/ad-kw-ai.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const T0 = Date.parse('2026-09-24T17:40:00Z');   // JST 9/25 02:40
const min = (n) => T0 + n * 60_000;

// ── 材料: 自社商品・仕様・サジェストの観測 2 語・ABA (ASIN → 語) の観測 1 語・語 → ASIN の自動取得・採用 (語を直した KW と ASIN)
const mkDraft = (ne, own = 1) => Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand, asin) VALUES (?, ?, 'test', ?, NULL)`).run(ne, 'ハッカ油スプレー ' + ne, own).lastInsertRowid);
const dId = mkDraft('AI-1');
db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, '容量', '100ml', 1), (?, '原材料', 'ハッカ油・エタノール', 2)`).run(dId, dId);
const draftOf = (id) => db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
const req1 = ak.ensureRequest(db, draftOf(dId), { idempotencyKey: 'r1', actor: 'u@x' }).request;
const insEv = db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, fetched_at) VALUES (?, ?, ?, ?, ?, '[]', ?)`);
const evS = Number(insEv.run(req1.id, 'suggest', 'ハッカ油', 'success', '{}', '2026-09-23T01:00:00Z').lastInsertRowid);
const evF = Number(insEv.run(req1.id, 'suggest', '失敗した種', 'failed', '{}', null).lastInsertRowid);
const evA = Number(insEv.run(req1.id, 'aba', 'B0COMPAAA1', 'success', JSON.stringify({ week_start: '2026-09-13' }), '2026-09-24T00:00:00Z').lastInsertRowid);
const evT = Number(insEv.run(req1.id, 'aba', '*top_asins*', 'success', '{}', '2026-09-24T00:00:00Z').lastInsertRowid);
const insC = db.prepare(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
const cSpray = Number(insC.run(req1.id, 'kw', 'ハッカ油 スプレー', 'ハッカ油 スプレー', 'observed', evS, JSON.stringify([{ evidence_id: evS, seed: 'ハッカ油', source: 'base' }]), 1, 'a1').lastInsertRowid);
const cMushi = Number(insC.run(req1.id, 'kw', 'ハッカ油 虫除け', 'ハッカ油 虫除け', 'observed', evS, JSON.stringify([{ evidence_id: evS, seed: 'ハッカ油', source: 'hiragana:む' }]), 1, 'a2').lastInsertRowid);
insC.run(req1.id, 'kw', '失敗だけの語', '失敗だけの語', 'observed', evF, JSON.stringify([{ evidence_id: evF, seed: '失敗した種', source: 'base' }]), 1, 'a3');
const cAba = Number(insC.run(req1.id, 'kw', 'はっか油 ゴキブリ', 'はっか油 ゴキブリ', 'observed', evA, JSON.stringify([{ evidence_id: evA, seed: 'B0COMPAAA1', source: 'aba', week_start: '2026-09-13' }]), 1, 'b1').lastInsertRowid);
const cAsin = Number(insC.run(req1.id, 'asin', 'B0AUTOAAA1', 'B0AUTOAAA1', 'observed', evT, JSON.stringify([{ evidence_id: evT, seed: '*top_asins*', source: 'aba', mode: 'top_asins' }]), 2, 'asin|1').lastInsertRowid);
const insD = db.prepare(`INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type, actor) VALUES (?, ?, ?, ?, ?, ?)`);
insD.run(cSpray, req1.id, 'adopt', 'ハッカ油 スプレー 携帯', 'exact_phrase', 'u@x');   // 人が語を直して採用
insD.run(cAsin, req1.id, 'adopt', 'B0AUTOAAA1', null, 'auto:aba');

console.log('[1] 機能フラグ OFF: 受付・claim・reserve を受けない');
{
  delete process.env.AD_KW_AI_ENABLED;
  eq(ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j1' }).code, 'ai_disabled', '受付 → ai_disabled');
  eq(ai.claimAiJob(db, { runnerRunId: 'r' }).code, 'ai_disabled', 'claim → ai_disabled');
  const st = ak.stateForDraft(db, draftOf(dId), { configured: true });
  eq([st.ai.enabled, st.ai.can_request, !!st.ai.reason_disabled], [false, false, true], '画面: 準備中 (押せない)');
  process.env.AD_KW_AI_ENABLED = '1';
  process.env.AD_KW_AI_DAILY_CAP = '3';
  ok(ai.aiSchemaReady(db), 'evidence の CHECK に ai が入っている (新しい DB)');
}

let job1, packet1;
console.log('[2] 受付: 材料 (packet) を固定する');
{
  const r = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j1', actor: 'u@x' });
  ok(r.ok && !r.reused && r.job.status === 'queued', '受け付けた (queued)');
  job1 = r.job;
  packet1 = JSON.parse(job1.packet_json);
  eq(packet1.observations.map((o) => [o.obs_id, o.value, o.sources]), [['o1', 'ハッカ油 スプレー', ['suggest']], ['o2', 'ハッカ油 虫除け', ['suggest']], ['o3', 'はっか油 ゴキブリ', ['aba']]],
    '観測 = サジェストと ABA (ASIN → 語) だけ。失敗した取得回だけの語・語 → ASIN の自動取得は入らない');
  eq(packet1.observations[2].week_start, '2026-09-13', 'ABA の観測は対象週つき');
  eq(packet1.adopted.map((a) => [a.value, a.match_type]), [['ハッカ油 スプレー 携帯', 'exact_phrase']], '採用語は人が直した値');
  eq(packet1.adopted_asins, ['B0AUTOAAA1'], '採用中の競合 ASIN');
  eq(packet1.product.specs.map((s) => [s.key, s.value]), [['容量', '100ml'], ['原材料', 'ハッカ油・エタノール']], '商品の仕様 (allowlist)');
  eq(packet1.seeds, ['ハッカ油'], '種 (失敗した種は入らない)');
  eq(job1.packet_hash, ai.sha256(job1.packet_json), 'packet_hash = 固定した packet の hash');
  const again = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j1' });
  ok(again.ok && again.reused && again.job.id === job1.id, '同じキー + 同じ材料 = 同じ依頼');
  eq(ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j2' }).code, 'active_exists', '別のキーでも、動いている依頼があれば 409 (同じ依頼に active は 1 つ)');
  insD.run(cMushi, req1.id, 'reject', null, null, 'u@x');   // 採否が変わる → 同じキーで別の材料
  eq(ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j1' }).code, 'key_conflict', '同じキーで材料が変わった → 409');
  const st = ak.stateForDraft(db, draftOf(dId), { configured: true });
  ok(st.ai.enabled && st.ai.active && st.ai.active.id === job1.id && st.ai.jobs[0].stale === true && st.ai.can_request === false, '画面: 待ち・材料が変わった印 (stale)・押せない (active あり)');
}

let claim1;
console.log('[3] claim: lease と固定 packet');
{
  claim1 = ai.claimAiJob(db, { runnerRunId: 'night-1', now: min(0) });
  ok(claim1.ok && claim1.job && claim1.job.job_id === job1.id && claim1.job.lease_token.length === 32, 'lease token はサーバー発行');
  eq(claim1.job.packet, packet1, '固定した packet をそのまま返す (受付後に採否が変わっても)');
  eq(ai.claimAiJob(db, { runnerRunId: 'night-1', now: min(1) }).job, null, 'ほかに無ければ null (二重に取らない)');
  eq(ai.queueSummary(db, min(1)).running, 1, '要約: running 1');
}

let gen1;
console.log('[4] reserve: AI を呼ぶ前の予約 (1 job 1 回)');
{
  eq(ai.reserveGeneration(db, job1.id, { leaseToken: 'x'.repeat(32), model: 'claude-sonnet-5', promptVersion: 'p1', now: min(2) }).code, 'lease_lost', '別の token は拒否');
  eq(ai.reserveGeneration(db, job1.id, { leaseToken: claim1.job.lease_token, model: '', promptVersion: 'p1', now: min(2) }).code, 'bad_request', 'model が無ければ拒否');
  const r = ai.reserveGeneration(db, job1.id, { leaseToken: claim1.job.lease_token, model: 'claude-sonnet-5', promptVersion: 'p1', now: min(2) });
  ok(r.ok && r.generation_id > 0, '予約できた');
  gen1 = r.generation_id;
  eq(ai.reserveGeneration(db, job1.id, { leaseToken: claim1.job.lease_token, model: 'claude-sonnet-5', promptVersion: 'p1', now: min(3) }).code, 'already_reserved', '2 回目の予約は拒否 (1 job 1 回)');
  eq(ai.releaseAiJob(db, job1.id, { leaseToken: 'y'.repeat(32), now: min(3) }).code, 'lease_lost', '別の token の release は拒否');
}

console.log('[5] result: 検証 → 候補・提案記録・job 完了・予約の最終処分を 1 txn');
const output1 = { keywords: [
  { keyword: 'ハッカ油 スプレー', basis_obs_ids: ['o1'], reason: '観測語そのもの', match_hint: 'exact' },     // 既存候補 (観測あり)
  { keyword: 'ハッカ油 ルームスプレー', basis_obs_ids: ['o1', 'o2'], reason: '用途を広げた語' },               // 新しい語 (AI だけ)
  { keyword: 'ハッカ油 スプレー', reason: '重複' },                                                            // 重複
  { keyword: 'ハッカ油 携帯', basis_obs_ids: ['o99'] },                                                        // 無い根拠 ID
  { keyword: 'B0COMPBBB2' },                                                                                     // ASIN
  { keyword: '除外したい語', kind: 'negative' },                                                                // 除外 KW は PR3 では受けない
  { keyword: 'x'.repeat(81) },                                                                                   // 長すぎる
  { keyword: 'はっか油 ゴキブリ', basis_obs_ids: ['o3'], match_hint: 'bogus' },                                 // ABA の観測語・hint は参考 (不正は null)
] };
{
  eq(ai.submitGenerationResult(db, gen1, { packetHash: 'deadbeef', output: output1, now: min(5) }).code, 'packet_mismatch', '別の packet の結果は拒否 (確定しない)');
  eq(db.prepare('SELECT status FROM ph_ad_kw_ai_generations WHERE id = ?').get(gen1).status, 'reserved', '拒否しても予約はそのまま');
  const sprayBefore = db.prepare('SELECT observed_json, observed_count FROM ph_ad_kw_candidates WHERE id = ?').get(cSpray);
  const r = ai.submitGenerationResult(db, gen1, { packetHash: job1.packet_hash, output: output1, now: min(6) });
  ok(r.ok && r.receipt.disposition === 'accepted' && r.receipt.accepted === 3 && r.receipt.rejected === 5 && r.receipt.result_kind === 'partial' && r.receipt.new_candidates === 1, '一部受理 (3 受理・5 棄却・新しい候補 1)');
  eq(r.receipt.reasons, { duplicate: 1, bad_basis: 1, asin_not_allowed: 1, kind_not_allowed: 1, bad_keyword: 1 }, '棄却の理由ごとの数');
  const j = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(job1.id);
  eq([j.status, j.result_kind, j.accepted, j.rejected, j.lease_token], ['done', 'partial', 3, 5, null], 'job = done (lease を放す)');
  const g = db.prepare('SELECT * FROM ph_ad_kw_ai_generations WHERE id = ?').get(gen1);
  ok(g.status === 'accepted' && g.payload_hash && JSON.parse(g.receipt_json).accepted === 3, '予約の最終処分 = accepted・payload と receipt を保存');
  const props = db.prepare('SELECT value, observed, basis_obs_ids, match_hint, candidate_id FROM ph_ad_kw_ai_proposals WHERE job_id = ? ORDER BY id').all(job1.id);
  eq(props.map((p) => [p.value, p.observed, JSON.parse(p.basis_obs_ids), p.match_hint]),
    [['ハッカ油 スプレー', 'observed', ['o1'], 'exact'], ['ハッカ油 ルームスプレー', 'ai_only', ['o1', 'o2'], null], ['はっか油 ゴキブリ', 'observed', ['o3'], null]],
    '提案記録: 観測あり / AI だけ はサーバーが packet と照合して決める・hint は参考 (不正は null)');
  eq(db.prepare('SELECT observed_json, observed_count FROM ph_ad_kw_candidates WHERE id = ?').get(cSpray), sprayBefore, '既存候補の観測記録 (observed_json) は変えない');
  const newC = db.prepare(`SELECT * FROM ph_ad_kw_candidates WHERE request_id = ? AND value = 'ハッカ油 ルームスプレー'`).get(req1.id);
  ok(newC && newC.origin === 'ai' && newC.observed_json === '[]' && newC.observed_count === 0, '新しい語 = 候補 origin=ai・観測なし (count 0)');
  ok(!db.prepare('SELECT 1 FROM ph_ad_kw_decisions WHERE candidate_id = ?').get(newC.id), 'AI の案に採否を作らない (初期状態 未採用)');
  const latest = ak.latestDecisionsOf(db, req1.id).get(cSpray);
  eq([latest.decision, latest.keyword], ['adopt', 'ハッカ油 スプレー 携帯'], '既存候補の採否 (人が直した採用) はそのまま');
  const ev = db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'ai'`).get(req1.id);
  ok(ev && ev.seed === `ai:job${job1.id}` && ev.status === 'partial', '材料 = source=ai の生成回 (partial)');
  // 応答断のあとの再送 / 別の内容
  const replay = ai.submitGenerationResult(db, gen1, { packetHash: job1.packet_hash, output: JSON.parse(JSON.stringify(output1)), now: min(60 * 24) });
  ok(replay.ok && replay.replay && JSON.stringify(replay.receipt) === g.receipt_json, '同じ payload の再送 (翌日でも) = 保存済みの receipt・二重に保存しない');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_ai_proposals WHERE job_id = ?').get(job1.id).n, 3, '再送で提案が増えない');
  eq(ai.submitGenerationResult(db, gen1, { packetHash: job1.packet_hash, output: { keywords: [] }, now: min(7) }).code, 'already_finalized', '確定後に別の内容は 409');
  eq(ai.failAiJob(db, job1.id, { leaseToken: claim1.job.lease_token, code: 'network', now: min(8) }).code, 'lease_lost', '遅れて届いた fail は done を変えない');
  const st = ak.stateForDraft(db, draftOf(dId), { configured: true });
  ok(st.ai.proposals[newC.id] && st.ai.proposals[newC.id].observed === 'ai_only' && JSON.stringify(st.ai.proposals[newC.id].basis) === '["ハッカ油 スプレー","ハッカ油 虫除け"]',
    '画面: 候補 → 提案 (理由・AI だけ・根拠の語)');
  ok(st.candidates.some((c) => c.id === newC.id && c.origin === 'ai'), '画面: AI の候補も候補表に並ぶ');
}

console.log('[6] 期限切れ: 予約後 → needs_review (自動で作り直さない)・遅れて届いた結果は履歴として受ける / 予約前 → retry_wait → 上限で failed');
{
  const r2 = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j3', actor: 'u@x' });
  ok(r2.ok, '次の依頼');
  const c2 = ai.claimAiJob(db, { runnerRunId: 'night-2', now: min(100) });
  const g2 = ai.reserveGeneration(db, r2.job.id, { leaseToken: c2.job.lease_token, model: 'claude-sonnet-5', promptVersion: 'p1', now: min(101) }).generation_id;
  eq(ai.claimAiJob(db, { runnerRunId: 'night-3', now: min(100 + 41) }).job, null, '41 分後の claim: 期限切れを回収 (取らない)');
  eq(db.prepare('SELECT status, error_code FROM ph_ad_kw_ai_jobs WHERE id = ?').get(r2.job.id), { status: 'needs_review', error_code: 'lease_expired_after_reserve' }, '予約後の期限切れ = needs_review');
  eq(ai.reserveGeneration(db, r2.job.id, { leaseToken: c2.job.lease_token, model: 'm', promptVersion: 'p', now: min(142) }).code, 'lease_lost', '期限後の reserve は不可');
  const late = ai.submitGenerationResult(db, g2, { packetHash: r2.job.packet_hash, output: { keywords: [{ keyword: 'ハッカ油 お風呂' }] }, now: min(60 * 20) });
  ok(late.ok && late.receipt.accepted === 1 && db.prepare('SELECT status FROM ph_ad_kw_ai_jobs WHERE id = ?').get(r2.job.id).status === 'done', '予約済みの結果は lease 切れ後でも受ける (復旧・AI を再実行しない)');
  // 予約前の期限切れ
  const r3 = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'j4' });
  let t = min(2000);
  for (let i = 1; i <= 3; i++) {
    const c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
    ok(c.job && c.job.job_id === r3.job.id, `claim ${i} 回目`);
    t += 41 * 60_000;
    ai.queueSummary(db, t);   // 回収
    const s = db.prepare('SELECT status, retries, next_run_at FROM ph_ad_kw_ai_jobs WHERE id = ?').get(r3.job.id);
    if (i < 3) { ok(s.status === 'retry_wait' && s.retries === i && Date.parse(s.next_run_at) > t, `予約前の期限切れ ${i} 回目 → retry_wait (次回時刻つき)`); t = Date.parse(s.next_run_at) + 1000; }
    else eq([s.status, s.retries], ['failed', 3], '3 回目 → failed');
  }
}

console.log('[7] fail / release: 再試行の可否はサーバーが code で決める');
{
  const mk = (key) => ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: key }).job;
  let t = min(5000);
  const jA = mk('f1'); let c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  eq(ai.failAiJob(db, jA.id, { leaseToken: c.job.lease_token, code: 'quota', message: 'limit', now: t + 60_000 }).status, 'retry_wait', 'quota (予約前) → retry_wait');
  eq(ai.failAiJob(db, jA.id, { leaseToken: c.job.lease_token, code: 'quota', now: t + 70_000 }).code, 'lease_lost', '同じ token の 2 回目は不可');
  db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?`).run(jA.id);
  const jB = mk('f2'); t += 3 * 3600_000; c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  eq(ai.failAiJob(db, jB.id, { leaseToken: c.job.lease_token, code: 'billing', now: t + 1000 }).status, 'failed', 'billing (課金経路の不一致) → failed (再試行しない)');
  const jC = mk('f3'); t += 3600_000; c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  ai.reserveGeneration(db, jC.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t + 1000 });
  eq(ai.failAiJob(db, jC.id, { leaseToken: c.job.lease_token, code: 'network', now: t + 2000 }).status, 'needs_review', '予約後の失敗 → needs_review (成否不明)');
  const rv = ai.reviewAiJob(db, draftOf(dId), jC.id, 'u@x');
  ok(rv.ok && db.prepare('SELECT status, reviewed_by FROM ph_ad_kw_ai_jobs WHERE id = ?').get(jC.id).reviewed_by === 'u@x', '人が確認済みにできる');
  const jD = mk('f4'); t += 3600_000; c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  eq(ai.releaseAiJob(db, jD.id, { leaseToken: c.job.lease_token, now: t + 1000 }).status, 'queued', '予約前の release → queued');
  c = ai.claimAiJob(db, { runnerRunId: 'n', now: t + 2000 });
  ai.reserveGeneration(db, jD.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t + 3000 });
  eq(ai.releaseAiJob(db, jD.id, { leaseToken: c.job.lease_token, now: t + 4000 }).status, 'needs_review', '予約後の release → needs_review');
  eq(ai.reserveGeneration(db, jD.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t + 5000 }).code, 'lease_lost', 'needs_review の job に予約できない');
}

console.log('[8] 出力が壊れている → rejected・job は failed (AI を呼び直さない)');
{
  db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE request_id = ? AND status IN ('queued', 'retry_wait', 'running', 'needs_review')`).run(req1.id);
  const j = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'bad1' }).job;
  const t = min(20000);
  const c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  const g = ai.reserveGeneration(db, j.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t }).generation_id;
  const r = ai.submitGenerationResult(db, g, { packetHash: j.packet_hash, output: { text: 'not json keywords' }, now: t + 1000 });
  ok(r.ok && r.receipt.disposition === 'rejected', 'receipt = rejected');
  eq(db.prepare('SELECT status, result_kind, error_code FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id), { status: 'failed', result_kind: 'rejected', error_code: 'invalid_output' }, 'job = failed / rejected');
  eq(db.prepare('SELECT status FROM ph_ad_kw_ai_generations WHERE id = ?').get(g).status, 'rejected', '予約 = rejected (最終)');
  eq(ai.validateOutput({ keywords: new Array(61).fill({ keyword: 'a' }) }, packet1).fatal !== null, true, '61 件は外側で棄却');
}

console.log('[9] 依頼の取消・置き換え・自社商品の解除 → 破棄を永続化 (再送で復活しない)');
{
  const j = ai.requestAiJob(db, draftOf(dId), req1.id, { idempotencyKey: 'c1' }).job;
  const t = min(30000);
  const c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
  const g = ai.reserveGeneration(db, j.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t }).generation_id;
  ak.cancelRequest(db, draftOf(dId), req1.id, 'u@x');
  eq(db.prepare('SELECT status FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id).status, 'cancelled', '依頼を取り消すと AI の依頼も cancelled');
  const out = { keywords: [{ keyword: 'ハッカ油 寝室' }] };
  const r = ai.submitGenerationResult(db, g, { packetHash: j.packet_hash, output: out, now: t + 1000 });
  ok(r.ok && r.receipt.disposition === 'discarded' && r.receipt.reason === 'request_cancelled', '結果は破棄 (理由つき)');
  eq(db.prepare('SELECT status, discard_reason FROM ph_ad_kw_ai_generations WHERE id = ?').get(g), { status: 'discarded', discard_reason: 'request_cancelled' }, '破棄を永続化');
  const again = ai.submitGenerationResult(db, g, { packetHash: j.packet_hash, output: out, now: t + 2000 });
  ok(again.ok && again.replay && again.receipt.disposition === 'discarded', '再送 = 同じ破棄の receipt (復活しない)');
  ok(!db.prepare(`SELECT 1 FROM ph_ad_kw_candidates WHERE value = 'ハッカ油 寝室'`).get(), '候補は作らない');
  // 自社商品の解除
  const d2 = mkDraft('AI-2');
  const rq2 = ak.ensureRequest(db, draftOf(d2), { idempotencyKey: 'r', actor: 'u' }).request;
  const e2 = Number(insEv.run(rq2.id, 'suggest', 'ミント', 'success', '{}', '2026-09-23T01:00:00Z').lastInsertRowid);
  insC.run(rq2.id, 'kw', 'ミント スプレー', 'ミント スプレー', 'observed', e2, JSON.stringify([{ evidence_id: e2, seed: 'ミント', source: 'base' }]), 1, 'a');
  const j2 = ai.requestAiJob(db, draftOf(d2), rq2.id, { idempotencyKey: 'x' }).job;
  const c2 = ai.claimAiJob(db, { runnerRunId: 'n', now: t + 5000 });
  ok(c2.job && c2.job.job_id === j2.id, '別商品の依頼を取った');
  db.prepare('UPDATE product_drafts SET own_brand = 0 WHERE id = ?').run(d2);
  eq(ai.reserveGeneration(db, j2.id, { leaseToken: c2.job.lease_token, model: 'm', promptVersion: 'p', now: t + 6000 }).code, 'parent_invalid', '自社商品を外したら予約できない');
  eq(db.prepare('SELECT status, error_code FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j2.id), { status: 'cancelled', error_code: 'not_own_brand' }, 'job は cancelled (not_own_brand)');
  // 材料ゼロ
  const d3 = mkDraft('AI-3');
  const rq3 = ak.ensureRequest(db, draftOf(d3), { idempotencyKey: 'r', actor: 'u' }).request;
  eq(ai.requestAiJob(db, draftOf(d3), rq3.id, { idempotencyKey: 'x' }).code, 'no_material', '観測 0 語は受け付けない (AI の一般知識だけで案を出さない)');
}

console.log('[10] 1 日の予約の上限 (サーバーで数える = ランナーの再起動で消えない)');
{
  const day = new Date(min(40000) + 9 * 3600_000).toISOString().slice(0, 10);
  const used = db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?').get(day).n;
  process.env.AD_KW_AI_DAILY_CAP = String(used);
  const d4 = mkDraft('AI-4');
  const rq4 = ak.ensureRequest(db, draftOf(d4), { idempotencyKey: 'r', actor: 'u' }).request;
  const e4 = Number(insEv.run(rq4.id, 'suggest', 'ハッカ', 'success', '{}', '2026-09-23T01:00:00Z').lastInsertRowid);
  insC.run(rq4.id, 'kw', 'ハッカ 飴', 'ハッカ 飴', 'observed', e4, JSON.stringify([{ evidence_id: e4, seed: 'ハッカ', source: 'base' }]), 1, 'a');
  const j4 = ai.requestAiJob(db, draftOf(d4), rq4.id, { idempotencyKey: 'x' }).job;
  const c4 = ai.claimAiJob(db, { runnerRunId: 'n', now: min(40000) });
  eq(ai.reserveGeneration(db, j4.id, { leaseToken: c4.job.lease_token, model: 'm', promptVersion: 'p', now: min(40000) }).code, 'daily_cap', `上限 (${used}) に達したら予約できない`);
  process.env.AD_KW_AI_DAILY_CAP = '10';
  const q = ai.queueSummary(db, min(40001));
  ok(q.running === 1 && q.daily_cap === 10 && typeof q.oldest_wait_min !== 'undefined', '要約: running・上限・最古の待ち');
}

console.log('[12] Codex #1429 R1: AI の語があとで観測されたら材料に入る / 新しい依頼の提案を表示 / 容量 / 旧材料の判定材料');
{
  process.env.AD_KW_AI_DAILY_CAP = '100';
  const d5 = mkDraft('AI-5');
  const rq5 = ak.ensureRequest(db, draftOf(d5), { idempotencyKey: 'r', actor: 'u' }).request;
  const e5 = Number(insEv.run(rq5.id, 'suggest', 'ラベンダー', 'success', '{}', '2026-09-23T01:00:00Z').lastInsertRowid);
  const c5 = Number(insC.run(rq5.id, 'kw', 'ラベンダー オイル', 'ラベンダー オイル', 'observed', e5, JSON.stringify([{ evidence_id: e5, seed: 'ラベンダー', source: 'base' }]), 1, 'a').lastInsertRowid);
  const run = (key, output, t) => {
    const j = ai.requestAiJob(db, draftOf(d5), rq5.id, { idempotencyKey: key }).job;
    const c = ai.claimAiJob(db, { runnerRunId: 'n', now: t });
    const g = ai.reserveGeneration(db, j.id, { leaseToken: c.job.lease_token, model: 'm', promptVersion: 'p', now: t }).generation_id;
    return { j, c, g, submit: (tt) => ai.submitGenerationResult(db, g, { packetHash: j.packet_hash, output, now: tt }) };
  };
  // 旧依頼: 予約のあと止まる (needs_review) → 新依頼が完了 → 旧依頼の結果が遅れて届く
  const old = run('old', { keywords: [{ keyword: 'ラベンダー 枕', reason: 'OLD' }] }, min(50000));
  ai.releaseAiJob(db, old.j.id, { leaseToken: old.c.job.lease_token, now: min(50001) });   // → needs_review
  ai.reviewAiJob(db, draftOf(d5), old.j.id, 'u');
  const neu = run('new', { keywords: [{ keyword: 'ラベンダー 枕', reason: 'NEW' }] }, min(50010));
  ok(neu.submit(min(50011)).ok, '新しい依頼の結果');
  ok(old.submit(min(50020)).ok, '古い依頼の結果が遅れて届く (履歴として受ける)');
  const makura = db.prepare(`SELECT id FROM ph_ad_kw_candidates WHERE request_id = ? AND value = 'ラベンダー 枕'`).get(rq5.id).id;
  let st = ak.stateForDraft(db, draftOf(d5), { configured: true });
  eq([st.ai.proposals[makura].reason, st.ai.proposals[makura].job_id, st.ai.proposals[makura].count], ['NEW', neu.j.id, 2], '表示は新しい依頼の提案 (古い結果で上書きしない)・提案 2 回');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_ai_proposals WHERE candidate_id = ?').get(makura).n, 2, '両方の提案が履歴に残る');
  // AI の語が、あとでサジェストに観測される → 次の材料に入る
  const e6 = Number(insEv.run(rq5.id, 'suggest', 'ラベンダー 枕', 'success', '{}', '2026-09-25T01:00:00Z').lastInsertRowid);
  db.prepare('UPDATE ph_ad_kw_candidates SET observed_json = ?, observed_count = 1 WHERE id = ?').run(JSON.stringify([{ evidence_id: e6, seed: 'ラベンダー 枕', source: 'base' }]), makura);
  const p5 = ai.buildPacket(db, draftOf(d5), rq5.id);
  ok(p5.observations.some((o) => o.value === 'ラベンダー 枕'), 'origin=ai の語でも、観測されたら材料に入る (origin では除外しない)');
  st = ak.stateForDraft(db, draftOf(d5), { configured: true });
  ok(st.candidates.find((c) => c.id === makura).observed.length === 1, '画面の候補も観測つき (AI 提案のグループから外れる)');
  // 容量: 採用語が多くても観測が材料に残る (上限つき)
  const insMany = db.transaction(() => {
    for (let i = 0; i < 150; i++) {
      const cid = Number(insC.run(rq5.id, 'kw', 'ラベンダー ' + 'あ'.repeat(60) + i, 'ラベンダー ' + 'あ'.repeat(60) + i, 'observed', e5, '[]', 0, 'z' + String(i).padStart(3, '0')).lastInsertRowid);
      insD.run(cid, rq5.id, 'adopt', 'ラベンダー ' + 'あ'.repeat(60) + i, 'exact_phrase', 'u');
    }
  });
  insMany();
  const big = ai.buildPacket(db, draftOf(d5), rq5.id);
  ok(big.adopted.length === 100 && big.limits.omitted_adopted === 50 && big.observations.length >= 1 && !big.limits.too_large && Buffer.byteLength(ai.canonicalJson(big)) <= ai.PACKET_MAX_BYTES,
    `採用語は 100 まで (省いた ${big.limits.omitted_adopted})・観測は残る・総量は上限内 (${Buffer.byteLength(ai.canonicalJson(big))} bytes)`);
  const rq = ai.requestAiJob(db, draftOf(d5), rq5.id, { idempotencyKey: 'big' });
  ok(rq.ok, '受け付けられる (材料不足と言わない)');
  const j0 = st.ai.jobs[0];
  ok('packet_decision_version' in j0 && 'stale_product' in j0, '画面に「固定した採否版」と「商品情報が変わったか」を渡す (採否の保存のたびに画面で比べ直す)');
  process.env.AD_KW_AI_DAILY_CAP = '10';
}

console.log('[11] evidence の CHECK を広げる作り直し (本番 = PR2-C の定義 → ai を足す)');
{
  const Database = (await import('better-sqlite3')).default;
  const path = await import('node:path');
  const mdb = new Database(path.join(process.env.DATA_DIR, 'adkw-ai-migration.db'));
  mdb.exec(`
    CREATE TABLE ph_ad_kw_requests (id INTEGER PRIMARY KEY AUTOINCREMENT);
    INSERT INTO ph_ad_kw_requests (id) VALUES (1);
    CREATE TABLE ph_ad_kw_evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES ph_ad_kw_requests(id) ON DELETE CASCADE,
      source TEXT NOT NULL CHECK (source IN ('suggest', 'aba', 'input')), seed TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('success', 'partial', 'empty', 'failed')),
      options_json TEXT NOT NULL DEFAULT '{}', coverage_json TEXT NOT NULL, raw_json TEXT NOT NULL, error TEXT, fetched_at TEXT, created_by TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    INSERT INTO ph_ad_kw_evidence (id, request_id, source, seed, status, coverage_json, raw_json) VALUES (70, 1, 'aba', 'B0X', 'success', '{}', '[]');
    CREATE TABLE ph_ad_kw_candidates (id INTEGER PRIMARY KEY AUTOINCREMENT);
    CREATE TABLE ph_ad_kw_decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id INTEGER NOT NULL);
  `);
  const m = dbmod.migrateAdKwCheckConstraints(mdb);
  eq(m.migrated, ['ph_ad_kw_evidence'], '材料の表だけ作り直す');
  ok(mdb.prepare('SELECT id FROM ph_ad_kw_evidence').get().id === 70, '行と id はそのまま');
  let okAi = true; try { mdb.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json) VALUES (1, 'ai', 'ai:job1', 'success', '{}', '[]')`).run(); } catch { okAi = false; }
  ok(okAi, '作り直したあと source=ai を受け付ける');
  eq(dbmod.migrateAdKwCheckConstraints(mdb).migrated, [], '二度目は何もしない');
  mdb.close();
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
