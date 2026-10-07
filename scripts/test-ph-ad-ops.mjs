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
  eq([a.skuCount, a.skippedSkus], [2, 1], 'A: 構成がこの商品だけの SKU 2 つ (詰め合わせ 1 つは数えない)');
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
  ok(rec(A, { kind: 'adjust', memo: '入札 40→30', happened_on: '2026-09-20', base_stage_event_id: rowOf(A).stageEventId }).ok, '「調整した」を記録できる');
  const a2 = rowOf(A);
  eq([a2.lastAdjustOn, a2.lastAdjustMemo, a2.sinceKind, a2.sinceAdjust, a2.adjustStale], ['2026-09-20', '入札 40→30', 'adjust', 8, false], '調整から 8 日');
  eq(a2.history.map((x) => x.kind), ['adjust', 'stage'], '履歴 (新しい順)');
  // B を出稿中にする → 表示 0 で警告
  ok(rec(B, { kind: 'stage', stage: 'running', campaign_types: ['manual_kw'], base_stage_event_id: 0 }).ok, 'B を今日 (9/28) 出稿中に');
  eq([rowOf(B).warn, rowOf(B).waitingActual], [null, true], 'R3: 実績 (9/27 まで) が出稿開始日の翌日に届いていない → 判定を待つ (警告しない)');
  ok(rec(B, { kind: 'stage', stage: 'running', campaign_types: ['manual_kw'], happened_on: '2026-09-01', base_stage_event_id: rowOf(B).stageEventId }).ok, 'B の開始日を 9/1 に訂正');
  ok(/表示が 0/.test(rowOf(B).warn || '') && !rowOf(B).waitingActual, 'B: 出稿中なのに直近 7 日の表示 0 → 警告');
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

console.log('■ Codex R1 の指摘');
{
  // #1 ASIN 経由でも詰め合わせ SKU は数えない / 2 つの商品に結びつく SKU はどちらにも数えない
  const F = mkDraft('fff', { asin: 'B0TESTFFF1' });
  skuMap.run('pr_F1', 'fff', 1, 0, sync);
  fees.run('pr_F1', 'B0TESTFFF1', sync);
  skuMap.run('pr_FMIX', 'fff', 1, 0, sync); skuMap.run('pr_FMIX', 'ggg-other', 1, 1, sync);
  fees.run('pr_FMIX', 'B0TESTFFF1', sync);          // 同じ ASIN に詰め合わせの SKU
  adRow('2026-09-27', 'cf', 'pr_f1', 'sku', { cost: 10 });
  adRow('2026-09-27', 'cf', 'pr_fmix', 'sku', { cost: 7000 });
  adRow('2026-09-27', 'cf', 'b0testfff1', 'asin', { cost: 3000 });
  const f = rowOf(F);
  eq([f.skuCount, f.skippedSkus, f.actual.cost], [1, 2, 10], '#1 ASIN から来た詰め合わせ SKU は数えない・詰め合わせが混ざる ASIN の行も数えない (数えなかった = SKU 1 + ASIN 1)');
  const H1 = mkDraft('hhh', { asin: 'B0TESTHHH1' });
  const H2 = mkDraft('hhh2', { asin: 'B0TESTHHH1' });   // 同じ ASIN を 2 つの商品に入れてしまった
  fees.run('pr_H', 'B0TESTHHH1', sync);
  adRow('2026-09-27', 'ch', 'pr_h', 'sku', { cost: 20 });
  eq([rowOf(H1).actual.cost, rowOf(H2).actual.cost, rowOf(H1).skippedSkus], [0, 0, 2], '#1 2 つの商品に結びつく SKU / ASIN はどちらにも数えない (同じ実績を 2 行に出さない・SKU 1 + ASIN 1)');
  // #2 ASIN が空でも、NE コード → SKU → ASIN の ASIN 粒度の行を拾う
  const G = mkDraft('ggg');
  skuMap.run('pr_G1', 'ggg', 1, 0, sync);
  fees.run('pr_G1', 'B0TESTGGG1', sync);
  adRow('2026-09-27', 'cg', 'b0testggg1', 'asin', { cost: 55 });
  const g = rowOf(G);
  eq([g.actual.cost, g.actual.active, g.asin], [55, true, 'B0TESTGGG1'], '#2 ASIN 欄が空でも SKU の ASIN の行を拾う');
  // #6 不正な月・日
  eq(rec(G, { kind: 'stage', stage: 'kw_ready', happened_on: '2026-13-01' }).code, 'bad_date', '#6 13 月は bad_date (例外にしない)');
  eq(rec(G, { kind: 'stage', stage: 'kw_ready', happened_on: '2026-00-10' }).code, 'bad_date', '#6 0 月は bad_date');
  // #3 調整も見ていた段階で照合する
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-01', base_stage_event_id: 0 }).ok, 'G を出稿中に');
  const seen = rowOf(G).stageEventId;
  ok(rec(G, { kind: 'stage', stage: 'stopped', memo: '止めた', happened_on: '2026-09-10', base_stage_event_id: seen }).ok, '別の人が止める');
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-12', base_stage_event_id: rowOf(G).stageEventId }).ok, '別の人が出し直す');
  const st3 = rec(G, { kind: 'adjust', memo: '古い画面から', base_stage_event_id: seen });
  eq([st3.code, st3.status], ['stale', 409], '#3 古い画面からの「調整した」は 409');
  eq(rec(G, { kind: 'adjust', memo: 'x' }).code, 'stale', '#3 見ていた段階を送らない調整も 409');
  // #4 後から記録した過去の調整で最終調整日が巻き戻らない
  const base = rowOf(G).stageEventId;
  ok(rec(G, { kind: 'adjust', memo: '9/20 の調整', happened_on: '2026-09-20', base_stage_event_id: base }).ok, '9/20 の調整');
  ok(rec(G, { kind: 'adjust', memo: '記録漏れの 9/15', happened_on: '2026-09-15', base_stage_event_id: base }).ok, 'あとから 9/15 の調整を記録');
  eq([rowOf(G).lastAdjustOn, rowOf(G).lastAdjustMemo, rowOf(G).sinceAdjust], ['2026-09-20', '9/20 の調整', 8], '#4 最終調整は実施日の新しい方 (9/20)');
  // #5 出稿中のまま種類を足しても、開始日・調整の数え方はリセットしない
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto', 'manual_kw'], base_stage_event_id: base }).ok, '出稿中のまま種類を足す (日付は今日)');
  const g5 = rowOf(G);
  eq([g5.stageOn, g5.campaignTypes, g5.lastAdjustOn, g5.sinceKind, g5.sinceAdjust], ['2026-09-12', ['auto', 'manual_kw'], '2026-09-20', 'adjust', 8], '#5 開始日 9/12 のまま・調整から 8 日のまま');
  eq(g5.history.length, 5, '履歴は新しい 5 件');
}

console.log('■ Codex R2 の指摘');
{
  // #2 出稿開始日の訂正: 出稿中のまま日付を送れば訂正 (後ろへも前へも)。送らなければ今の開始日のまま
  const G = db.prepare(`SELECT id FROM product_drafts WHERE ne_code = 'ggg'`).get().id;
  const base = () => rowOf(G).stageEventId;
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto', 'manual_kw'], happened_on: '2026-09-15', memo: '開始日を訂正', base_stage_event_id: base() }).ok, '開始日を 9/12 → 9/15 に訂正');
  eq([rowOf(G).stageOn, rowOf(G).sinceKind, rowOf(G).sinceAdjust], ['2026-09-15', 'adjust', 8], '#2 後ろの日付へ訂正できる・調整 (9/20) はそのまま効く');
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto'], memo: '種類だけ直す', base_stage_event_id: base() }).ok, '日付を送らずに種類だけ直す');
  eq(rowOf(G).stageOn, '2026-09-15', '#2 日付を送らない更新では開始日は今日にならない');
  ok(rec(G, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-22', memo: '開始は 9/22 だった', base_stage_event_id: base() }).ok, '調整より後の日へ訂正');
  eq([rowOf(G).stageOn, rowOf(G).sinceKind, rowOf(G).sinceAdjust], ['2026-09-22', 'running', 6], '#2 開始より前の調整では数えない');
  // #1 数えない SKU が混ざる ASIN にだけ実績がある → 除外を数え、「表示 0」の警告は出さない
  const K = mkDraft('kkk');
  skuMap.run('pr_K1', 'kkk', 1, 0, sync);
  fees.run('pr_K1', 'B0TESTKKK1', sync);
  skuMap.run('pr_KX', 'kkk', 1, 0, sync); skuMap.run('pr_KX', 'zzz-other', 1, 1, sync);
  fees.run('pr_KX', 'B0TESTKKK1', sync);   // 同じ ASIN に詰め合わせ
  adRow('2026-09-27', 'ck', 'b0testkkk1', 'asin', { cost: 999 });
  ok(rec(K, { kind: 'stage', stage: 'running', campaign_types: ['auto'], base_stage_event_id: 0 }).ok, 'K を出稿中に');
  const k = rowOf(K);
  eq([k.linked, k.actual.cost, k.skippedSkus >= 2, k.warn], [true, 0, true, null], '#1 ASIN の除外も数える・集計が不完全なら「表示 0」を言わない');
}

console.log('■ Codex R3 の指摘 (段階を記録した日より後の実績で判定)');
{
  const L = mkDraft('lll');
  skuMap.run('pr_L1', 'lll', 1, 0, sync);
  adRow('2026-09-26', 'cl', 'pr_l1', 'sku', { cost: 30 });
  adRow('2026-09-27', 'cl', 'pr_l1', 'sku', { cost: 30 });
  ok(rec(L, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-01', base_stage_event_id: 0 }).ok, 'L 出稿中 (9/1)');
  ok(rec(L, { kind: 'stage', stage: 'stopped', memo: '止めた', happened_on: '2026-09-27', base_stage_event_id: rowOf(L).stageEventId }).ok, 'L 9/27 に停止');
  eq(rowOf(L).warn, null, '停止した日 (9/27) までの表示では警告しない');
  ok(rec(L, { kind: 'stage', stage: 'stopped', memo: '本当は 9/25 に止めた', happened_on: '2026-09-25', base_stage_event_id: rowOf(L).stageEventId }).ok, 'L 停止日を 9/25 に訂正');
  ok(/記録した日より後にも/.test(rowOf(L).warn || ''), '停止した日より後 (9/26・9/27) に表示あり → 警告');
  const M = mkDraft('mmm');
  skuMap.run('pr_M1', 'mmm', 1, 0, sync);
  adRow('2026-09-22', 'cm', 'pr_m1', 'sku', { cost: 30 });
  ok(rec(M, { kind: 'stage', stage: 'running', campaign_types: ['auto'], happened_on: '2026-09-25', base_stage_event_id: 0 }).ok, 'M 9/25 に出稿 (表示は 9/22 だけ)');
  ok(/出稿を始めてから/.test(rowOf(M).warn || ''), '出稿開始より前の表示 (9/22) は数えない → 開始してからの表示 0 で警告');
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
