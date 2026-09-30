import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-compose-');
/**
 * LP 構成の AI 生成 — 段階1 のスキーマの不変条件 (apps/product-hub/db.js)
 * 実行: node scripts/test-ph-lp-compose.mjs
 * 設計 = 正本 AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』§4.1
 *
 * ここで守りたいのは 5 つ:
 *   ① 仕様書は追記専用 (UPDATE / DELETE をトリガーで拒む・Codex R4 #4)
 *      — 宣言だけだと spec_hash の照合が通ったまま AI への実効入力が変わり、job の再現性が消える
 *   ② 同じ商品で動いている依頼は 1 つだけ (queued / running)。終わった依頼は次を妨げない
 *   ③ 二重クリック・通信リトライで job が増えない (draft_id + idempotency_key)
 *   ④ 測定用の 2 列 — measurement_deadline_at は必須 (受付時に固定)、completed_at は後から入る
 *      (Codex R4 #2: lease 40 分と 3 分の合格ラインが接続されていないと所要時間の母数がぶれる)
 *   ⑤ 1 job 1 generation。job を消せば generation も消えるが、**draft を消しても job は残る**
 *      (Codex R3 #5: 段階1 は測定が目的なので、draft の削除で実験記録を失わない)
 */
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const throws = (l, fn, want) => {
  try { fn(); fail++; console.log(`  ✗ ${l} — 例外が出なかった`); }
  catch (e) {
    if (!want || String(e.message).includes(want)) { pass++; console.log(`  ✓ ${l}`); }
    else { fail++; console.log(`  ✗ ${l} — 想定と違う例外: ${e.message}`); }
  }
};

const insSpec = db.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, sheet_titles_json, imported_by)
  VALUES ('product_analysis', ?, ?, ?, ?, 'nakahara@x')`);
const insJob = db.prepare(`INSERT INTO ph_lp_compose_jobs
  (draft_id, idempotency_key, status, packet_json, packet_hash, packet_version, spec_id, spec_hash,
   requested_by, measurement_deadline_at)
  VALUES (?, ?, ?, '{}', 'p1', 1, ?, 'h1', 'u@x', '2026-10-01T00:03:00.000Z')`);
const insGen = db.prepare(`INSERT INTO ph_lp_compose_generations
  (job_id, packet_hash, lease_token, status, model, prompt_version, reserved_day)
  VALUES (?, 'p1', 'lt-1', ?, 'claude-opus-5', 'lp-compose-v1', '2026-10-01')`);

console.log('① 仕様書は追記専用');
const specId = Number(insSpec.run('LP制作システム', '本文 V2.2', 'hash-1', '["出力形式","AIプロンプトV2.2"]').lastInsertRowid);
ok(specId > 0, '仕様書を取り込める');
throws('同じ中身を上げ直しても行が増えない', () => insSpec.run('LP制作システム', '本文 V2.2', 'hash-1', '[]'), 'UNIQUE');
ok(Number(insSpec.run('LP制作システム', '本文 V2.3', 'hash-2', '[]').lastInsertRowid) > specId, '中身が変われば新しい版として入る');
throws('UPDATE は拒まれる', () => db.prepare('UPDATE ph_lp_specs SET body = ? WHERE id = ?').run('すり替え', specId), '追記専用');
throws('DELETE は拒まれる', () => db.prepare('DELETE FROM ph_lp_specs WHERE id = ?').run(specId), '追記専用');
throws('段階1 は product_analysis だけ',
  () => db.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, sheet_titles_json, imported_by)
    VALUES ('simple_lp', 't', 'b', 'hash-9', '[]', 'u')`).run(), 'CHECK');

console.log('② 同じ商品で動いている依頼は 1 つだけ');
const draftId = Number(db.prepare(
  `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-1', 'ハッカ油スプレー 100ml', 'test')`
).run().lastInsertRowid);
const job1 = Number(insJob.run(draftId, 'key-0001', 'queued', specId).lastInsertRowid);
ok(job1 > 0, '1 件目を受け付ける');
throws('動いている間は 2 件目を受け付けない', () => insJob.run(draftId, 'key-0002', 'queued', specId), 'UNIQUE');
db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'done', completed_at = '2026-10-01T00:02:00.000Z' WHERE id = ?`).run(job1);
const job2 = Number(insJob.run(draftId, 'key-0002', 'queued', specId).lastInsertRowid);
ok(job2 > job1, '終わった依頼は次を妨げない');

console.log('③ 二重クリック・リトライで job が増えない');
throws('同じ idempotency_key は 2 回受け付けない', () => insJob.run(draftId, 'key-0001', 'failed', specId), 'UNIQUE');

console.log('④ 測定用の列');
throws('measurement_deadline_at は必須', () => db.prepare(`INSERT INTO ph_lp_compose_jobs
  (draft_id, idempotency_key, status, packet_json, packet_hash, packet_version, spec_id, spec_hash, requested_by)
  VALUES (?, 'key-0009', 'queued', '{}', 'p', 1, ?, 'h', 'u')`).run(draftId, specId), 'NOT NULL');
throws('未知の status は入らない', () => insJob.run(draftId + 1, 'key-0003', 'weird', specId), 'CHECK');
throws('存在しない仕様書の版は張れない', () => insJob.run(draftId + 2, 'key-0004', 'queued', 99999), 'FOREIGN KEY');
const j1 = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(job1);
ok(j1.completed_at <= j1.measurement_deadline_at, '期限内に終わったかを completed_at から後で計算できる');

console.log('⑤ generation と削除の連鎖');
ok(Number(insGen.run(job2, 'reserved').lastInsertRowid) > 0, 'AI を呼ぶ前に予約できる');
throws('1 job 1 generation', () => insGen.run(job2, 'reserved'), 'UNIQUE');
throws('未知の generation status は入らない', () => insGen.run(job1, 'weird'), 'CHECK');
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(draftId);
ok(db.prepare('SELECT COUNT(*) n FROM ph_lp_compose_jobs WHERE draft_id = ?').get(draftId).n === 2,
  '🚨 draft を消しても測定記録 (job) は残る');
db.prepare('DELETE FROM ph_lp_compose_jobs WHERE id = ?').run(job2);
ok(db.prepare('SELECT COUNT(*) n FROM ph_lp_compose_generations WHERE job_id = ?').get(job2).n === 0,
  'job を消せば generation も消える');

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
