import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-ad-ops-');
/**
 * 📣 広告の進み (2026-09-28) — 記録 (append-only・検証・先に変えた人との衝突) / 実績の結びつけ / 食い違い / 調整からの日数
 * 実行: node scripts/test-ph-ad-ops.mjs
 */
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const ao = await import('../apps/product-hub/lib/ad-ops.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
// 今日 = JST 2026-09-28 12:00
const NOW = Date.parse('2026-09-28T03:00:00Z');
const mkDraft = (ne, { own = 1, asin = null, amazonUrl = null, status = 'draft', parent = null } = {}) => Number(db.prepare(`
  INSERT INTO product_drafts (ne_code, name, created_by, own_brand, asin, amazon_url, status, parent_draft_id) VALUES (?, ?, 'test', ?, ?, ?, ?, ?)
`).run(ne, '商品 ' + ne, own, asin, amazonUrl, status, parent).lastInsertRowid);
const draftOf = (id) => db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
const rec = (id, body, actor = 'a@b-faith.biz') => ao.recordAdOps(db, draftOf(id), body, actor, { now: NOW });
const rowOf = (id, now = NOW) => ao.adOpsRows(db, { now }).rows.find((r) => r.id === id);

// ── mirror の偽データ
const sync = '2026-09-28T00:00:00Z';
const skuMap = db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, sort_order, synced_at) VALUES (?, ?, ?, 'master', ?, ?)`);
const fees = db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fetched_at) VALUES (?, ?, ?)`);
const ad = db.prepare(`INSERT INTO mirror_amazon_ads_sku_daily
  (date_jst, mall, campaign_id, ad_type, target, target_granularity, clicks, impressions, ad_cost, ad_sales, ad_units, source_run_id, source_row_hash, synced_at)
  VALUES (?, 'amazon', ?, 'SP', ?, ?, ?, ?, ?, ?, ?, 'run', ?, ?)`);
let h = 0;
const adRow = (date, campaign, target, g, { imp = 100, clicks = 5, cost = 200, sales = 1000, units = 1 } = {}) =>
  ad.run(date, campaign, target, g, clicks, imp, cost, sales, units, 'h' + (h++), sync);
const camp = db.prepare(`INSERT INTO mirror_amazon_ads_campaign_daily
  (date_jst, mall, campaign_id, campaign_name, ad_type, campaign_status, source_run_id, source_row_hash, synced_at)
  VALUES (?, 'amazon', ?, ?, 'SP', ?, 'run', ?, ?)`);

// A = NE コードから SKU (単品と 2 個セット) / 詰め合わせ SKU は数えない
const A = mkDraft('chlorellap');
skuMap.run('pr_A1', 'chlorellap', 1, 0, sync);
skuMap.run('pr_A2', 'chlorellap', 2, 0, sync);
skuMap.run('pr_MIX', 'chlorellap', 1, 0, sync);
skuMap.run('pr_MIX', 'otherthing', 1, 1, sync);
// B = ASIN (URL から) → SKU。NE コードは mirror に無い
const B = mkDraft('hakka-set', { amazonUrl: 'https://www.amazon.co.jp/dp/B0TESTBBB1?th=1' });
fees.run('PR_B1', 'B0TESTBBB1', sync);
// C = 結びつかない
const C = mkDraft('nolink');
// D = 自社でない / E = 除外 → 表に出ない
const D = mkDraft('notown', { own: 0 });
const E = mkDraft('excluded1', { status: 'excluded' });

// 実績: 最新日 9/27。A は pr_a1 が 9/27 まで・pr_a2 は 8/1 (期間外)・詰め合わせは 9/27
adRow('2026-09-27', 'c1', 'pr_a1', 'sku', { cost: 300, sales: 1500 });
adRow('2026-09-10', 'c2', 'pr_a1', 'sku', { cost: 100, sales: 0, units: 0 });
adRow('2026-08-01', 'c2', 'pr_a2', 'sku', { cost: 999 });
adRow('2026-09-27', 'c9', 'pr_mix', 'sku', { cost: 5000 });
// B は 9/15 が最後 (直近 7 日 = 9/21〜9/27 は表示 0)
adRow('2026-09-15', 'c3', 'pr_b1', 'sku', { cost: 50, sales: 0, units: 0 });
camp.run('2026-09-26', 'c1', 'クロレラ 手動', 'ENABLED', 'x1', sync);
camp.run('2026-09-27', 'c1', 'クロレラ 手動 (改名)', 'ENABLED', 'x2', sync);

console.log('■ 対象と結びつけ');
{
  const r = ao.adOpsRows(db, { now: NOW });
  const ids = r.rows.map((x) => x.id);
  ok(ids.includes(A) && ids.includes(B) && ids.includes(C), '自社商品は全部出る');
  ok(!ids.includes(D) && !ids.includes(E), '自社でない商品・除外した商品は出ない');
  eq(r.actualLatest, '2026-09-27', '実績の最新日');
  eq(r.actualFrom, '2026-08-29', '実績の期間の始まり (最新日から 30 日)');
  eq(r.actualStale, false, '実績は新しい');
  const a = rowOf(A);
  eq(a.skuCount, 2, 'A: 構成がこの商品だけの SKU 2 つ (詰め合わせは数えない)');
  eq(a.actual.cost, 400, 'A: 期間内の広告費 (期間外の 999 と詰め合わせの 5000 は入らない)');
  eq(a.actual.sales, 1500, 'A: 売上');
  eq(a.actual.acos, 26.7, 'A: ACOS = 400/1500');
  eq(a.actual.active, true, 'A: 直近 7 日に表示あり');
  eq(a.actual.lastActive, '2026-09-27', 'A: 最後に表示');
  eq(a.actual.campaigns.map((c) => [c.name, c.cost, c.active]), [['クロレラ 手動 (改名)', 300, true], ['(名前不明 c2)', 100, false]], 'A: キャンペーン (最新の名前・費用順・名前が無いものは id)');
  const b = rowOf(B);
  eq(b.asin, 'B0TESTBBB1', 'B: ASIN は Amazon URL から');
  eq(b.skuCount, 1, 'B: ASIN → SKU で結びつく');
  eq([b.actual.cost, b.actual.active, b.actual.lastActive], [50, false, '2026-09-15'], 'B: 表示が止まっている');
  const c = rowOf(C);
  eq([c.linked, c.actual.lastActive], [false, null], 'C: 結びつかない');
}

console.log('■ 食い違い');
{
  eq(rowOf(A).warn != null, true, 'A: 未着手なのに表示あり → 警告');
  eq(rowOf(B).warn, null, 'B: 未着手で表示なし → 警告なし');
  eq(rowOf(C).warn, null, 'C: 結びつかない商品は判定しない');
  eq(ao.adOpsRows(db, { now: NOW }).rows[0].id, A, '警告の行が先頭');
  eq(ao.adOpsRows(db, { now: NOW }).counts.warn, 1, '警告の数');
}

console.log('■ 記録の検証');
{
  eq(rec(D, { kind: 'stage', stage: 'kw_ready' }).code, 'not_own_brand', '自社でない商品は記録できない');
  eq(rec(A, { kind: 'x' }).code, 'bad_kind', '種類が不正');
  eq(rec(A, { kind: 'stage', stage: 'bogus' }).code, 'bad_stage', '段階が不正');
  eq(rec(A, { kind: 'stage', stage: 'running' }).code, 'bad_types', '出稿中はキャンペーンの種類が要る');
  eq(rec(A, { kind: 'stage', stage: 'running', campaign_types: ['auto', 'tv'] }).code, 'bad_types', '知らない種類は拒む');
  eq(rec(A, { kind: 'stage', stage: 'kw_ready', campaign_types: ['auto'] }).code, 'bad_types', '出稿中以外は種類を受けない');
  eq(rec(A, { kind: 'stage', stage: 'stopped' }).code, 'bad_memo', '停止は理由が要る');
  eq(rec(A, { kind: 'stage', stage: 'kw_ready', happened_on: '2026-09-29' }).code, 'bad_date', '未来の日付は拒む (JST の今日 = 9/28)');
  eq(rec(A, { kind: 'stage', stage: 'kw_ready', happened_on: '2026-02-30' }).code, 'bad_date', '実在しない日付は拒む');
  eq(rec(A, { kind: 'stage', stage: 'kw_ready', memo: 'x'.repeat(501) }).code, 'bad_memo', 'メモは 500 文字まで');
  eq(rec(A, { kind: 'adjust' }).code, 'not_running', '出稿中でなければ「調整した」は記録できない');
  eq(rec(A, { kind: 'stage', stage: 'none' }).code, 'no_change', '未着手 → 未着手 (メモなし) は変更なし');
  eq(db.prepare('SELECT COUNT(*) AS n FROM ph_ad_ops_events').get().n, 0, '拒んだ記録は残らない');
}

console.log('■ 記録 → 段階・食い違い・調整からの日数');
{
  const r1 = rec(A, { kind: 'stage', stage: 'running', campaign_types: ['product_target', 'auto'], happened_on: '2026-09-01', base_stage_event_id: 0 });
  ok(r1.ok, '出稿中を記録できる');
  eq(ao.AD_OPS_CAMPAIGN_TYPES.filter((t) => JSON.parse(r1.event.campaign_types).includes(t)), ['auto', 'product_target'], '種類は決まった順で保存');
  const a = rowOf(A);
  eq([a.stage, a.stageOn, a.campaignTypes], ['running', '2026-09-01', ['auto', 'product_target']], '段階・開始日・種類');
  eq(a.warn, null, '出稿中 + 表示あり → 警告なし');
  eq([a.sinceKind, a.sinceAdjust, a.adjustStale], ['running', 27, true], '調整の記録が無い → 出稿から 27 日 (14 日以上は目立たせる)');
  // 別の人が先に変えていたら止める
  const stale = rec(A, { kind: 'stage', stage: 'stopped', memo: '赤字', base_stage_event_id: 0 });
  eq([stale.code, stale.status], ['stale', 409], '画面が古い (見ていた段階の行が最新でない) → 409');
  // 調整
  ok(rec(A, { kind: 'adjust', memo: '入札 40→30', happened_on: '2026-09-20' }).ok, '「調整した」を記録できる');
  const a2 = rowOf(A);
  eq([a2.lastAdjustOn, a2.lastAdjustMemo, a2.sinceKind, a2.sinceAdjust, a2.adjustStale], ['2026-09-20', '入札 40→30', 'adjust', 8, false], '調整から 8 日');
  eq(a2.history.map((x) => x.kind), ['adjust', 'stage'], '履歴 (新しい順)');
  // B を出稿中にする → 表示 0 で警告
  ok(rec(B, { kind: 'stage', stage: 'running', campaign_types: ['manual_kw'], base_stage_event_id: 0 }).ok, 'B を出稿中に');
  ok(/表示が 0/.test(rowOf(B).warn || ''), 'B: 出稿中なのに直近 7 日の表示 0 → 警告');
  // 停止 → 出し直し: 前の調整は数えない
  const baseA = rowOf(A).stageEventId;
  ok(rec(A, { kind: 'stage', stage: 'stopped', memo: '赤字', happened_on: '2026-09-22', base_stage_event_id: baseA }).ok, '停止を記録');
  ok(/表示されています/.test(rowOf(A).warn || ''), 'A: 停止なのに表示あり → 警告');
  eq(ao.adStagesByDraft(db).get(A), 'stopped', 'カードの札 = いまの段階');
  ok(rec(A, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-25', base_stage_event_id: rowOf(A).stageEventId }).ok, '出し直し');
  const a3 = rowOf(A);
  eq([a3.sinceKind, a3.sinceAdjust], ['running', 3], '出し直す前の調整 (9/20) は数えない = 出稿から 3 日');
  // 未着手に戻す → 札は出さない
  ok(rec(C, { kind: 'stage', stage: 'kw_ready', base_stage_event_id: 0 }).ok, 'C を KW作成済みに');
  ok(rec(C, { kind: 'stage', stage: 'none', memo: '取り消し', base_stage_event_id: rowOf(C).stageEventId }).ok, 'C を未着手に戻す');
  eq(ao.adStagesByDraft(db).has(C), false, '未着手はカードに札を出さない');
  eq(rowOf(C).history.length, 2, '戻しても履歴は残る');
}

console.log('■ append-only');
{
  let threw = 0;
  try { db.prepare('UPDATE ph_ad_ops_events SET memo = ?').run('x'); } catch { threw++; }
  try { db.prepare('DELETE FROM ph_ad_ops_events').run(); } catch { threw++; }
  eq(threw, 2, 'UPDATE / DELETE はトリガーで止まる');
  let chk = 0;
  try { db.prepare(`INSERT INTO ph_ad_ops_events (draft_id, kind, stage, happened_on) VALUES (?, 'adjust', 'running', '2026-09-28')`).run(A); } catch { chk++; }
  try { db.prepare(`INSERT INTO ph_ad_ops_events (draft_id, kind, stage, happened_on) VALUES (?, 'stage', NULL, '2026-09-28')`).run(A); } catch { chk++; }
  eq(chk, 2, 'kind と stage の組み合わせは CHECK で止まる');
}

console.log('■ 実績が古いとき・無いとき');
{
  const later = Date.parse('2026-10-05T03:00:00Z');   // 最新日 9/27 から 8 日
  const r = ao.adOpsRows(db, { now: later });
  eq(r.actualStale, true, '実績の最新日が 4 日より古い → 古い');
  eq(r.rows.filter((x) => x.warn).length, 0, '古いときは食い違いを出さない');
}

console.log('■ SP広告KW の進み');
{
  const snap = JSON.stringify({ name: 'x', ne_code: 'chlorellap', asin: null });
  const req = Number(db.prepare(`INSERT INTO ph_ad_kw_requests (draft_id, idempotency_key, status, product_snapshot_json, input_hash) VALUES (?, 'k1', 'review_ready', ?, 'h')`).run(A, snap).lastInsertRowid);
  const ev = Number(db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json) VALUES (?, 'suggest', 's', 'success', '{}', '{}')`).run(req).lastInsertRowid);
  const cand = (kind, v) => Number(db.prepare(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, sort_key) VALUES (?, ?, ?, ?, 'observed', ?, '[]', ?)`).run(req, kind, v, v, ev, v).lastInsertRowid);
  const dec = (c, d) => db.prepare(`INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision) VALUES (?, ?, ?)`).run(c, req, d);
  const k1 = cand('kw', 'クロレラ'), k2 = cand('kw', 'クロレラ 粒'), k3 = cand('asin', 'B0COMP0001');
  dec(k1, 'adopt'); dec(k2, 'adopt'); dec(k2, 'reject'); dec(k3, 'adopt');
  eq(rowOf(A).kw && [rowOf(A).kw.adoptedKw, rowOf(A).kw.adoptedAsin, rowOf(A).kw.copiedOn], [1, 1, null], '採用 = 最新の採否で数える (却下に変えた語は数えない)');
  db.prepare(`INSERT INTO ph_ad_kw_exports (request_id, draft_id, kind, decision_version, body_json, body_hash, copied_json) VALUES (?, ?, 'ad_copy', 1, '{}', 'h', ?)`)
    .run(req, A, JSON.stringify({ exact: '2026-09-26T16:00:00.000Z', phrase: '2026-09-25T01:00:00.000Z' }));
  eq(rowOf(A).kw.copiedOn, '2026-09-27', 'コピーした日 = いちばん新しいコピー (JST)');
  eq(rowOf(B).kw, null, '依頼の無い商品は未依頼');
}

console.log(`\n${fail ? '✗' : '✓'} ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
