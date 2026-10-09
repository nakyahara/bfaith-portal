import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-compose-shoot-');
/**
 * AI の撮影判定 (画像制作の新フロー PR-C・2026-10-09)
 * 実行: node scripts/test-ph-lp-compose-shoot.mjs
 * 設計 = AI_reference『商品ハブ_画像制作の新フロー_設計_20261008.md』§3.2・§3.3 / スタッフ要望 ②
 *
 * ここで守りたいのは:
 *   ① 撮影判定は ⑦ の本文ではなく別の欄 (shoot_json)。構成の指示文 (スタッフと共有の正本) は変えない
 *   ② 形は厳しく見る。壊れていたら撮影判定だけ「AI の判定なし」にして、**構成は今どおり受け付ける**
 *   ③ 撮影判定を送らない古い実行役の結果も今どおり通る (再送の判定 = payload_hash も変わらない)
 *   ④ 読み口 latestShootJudgement は「いちばん新しい使える構成」の判定だけを返す (撮影指示書 PR-D が使う)
 *   ⑤ 画面は「AIのおすすめ」の札と理由を出すだけ。**撮影判定は自動で書き換えない** (人が押す)。
 *      「おすすめにする」は 3 択のボタンを押すのと同じ経路 (POST /shoot-mode・expected つき) を通る
 */
process.env.PH_SERVICE_TOKEN = 'test-token-lp-shoot';
process.env.PH_LP_COMPOSE_ENABLED = '1';
process.env.PH_LP_COMPOSE_DAILY_CAP = '200';

const fs = (await import('node:fs')).default;
const path = (await import('node:path')).default;
const { fileURLToPath } = await import('node:url');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const shootLib = await import('../apps/product-hub/lib/lp-shoot.js');
const pt = await import('../apps/product-hub/lib/prompt-templates.js');
const wf = await import('../apps/product-hub/lib/workflow.js');
const express = (await import('express')).default;
const { default: router, serviceApiRouter } = await import('../apps/product-hub/router.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const NAME = 'ハッカ油スプレー 100ml';
const OUT = compositionFor(NAME);   // 0枚目・1枚目・2枚目 の 3 枚 (lint を通る本文)
const blankCut = { cut: '', composition: '', props: '', background: '', tone: '', ng: '' };
/** 構成 (0〜2枚目) に合う撮影判定 */
const goodShoot = () => ({
  recommended: 'inhouse',
  reason: '2枚目 の写真 (使用シーン) がありません。卓上の簡単なカットなので社内撮影で足ります。',
  images: [
    { no: 2, needs_shoot: true, cut: '玄関でスプレーする手元', composition: '斜め上から手元と商品', props: '玄関マット', background: '白い床', tone: '自然光', ng: '顔を写さない' },
    { no: 0, needs_shoot: false, ...blankCut },
    { no: 1, needs_shoot: false, ...blankCut },
  ],
});

console.log('① 形の検査 (lib/lp-shoot.js・純粋関数)');
{
  const v = shootLib.validateShootJudgement(goodShoot(), { imageNos: [0, 1, 2] });
  ok(v.ok, '正しい形は通る');
  eq(v.value.images.map((x) => x.no), [0, 1, 2], '画像は番号順にそろえて保存する');
  eq(v.value.reason, goodShoot().reason, '理由は送られたまま保存する');
  eq(Object.keys(v.value.images[0]), ['no', 'needs_shoot', 'cut', 'composition', 'props', 'background', 'tone', 'ng'], '画像ごとの項目はこの順');
  const noneOk = shootLib.validateShootJudgement({ recommended: 'none', reason: 'r', images: [0, 1].map((no) => ({ no, needs_shoot: false, ...blankCut })) }, { imageNos: [0, 1] });
  ok(noneOk.ok, '撮影不要のおすすめ (全部 needs_shoot: false・cut 以下は "") は通る');

  const bad = (raw, why, nos = [0, 1, 2]) => {
    const r = shootLib.validateShootJudgement(raw, { imageNos: nos });
    ok(!r.ok && r.errors.length > 0, `🚨 通さない: ${why} (${r.ok ? '通ってしまった' : r.errors[0]})`);
  };
  bad(null, 'null');
  bad([goodShoot()], '配列');
  bad('{"recommended":"none"}', '文字列の JSON (オブジェクトで送る)');
  bad({ ...goodShoot(), summary: 'x' }, '知らないキー (上)');
  bad({ ...goodShoot(), recommended: 'Inhouse' }, 'recommended の大文字');
  bad({ ...goodShoot(), recommended: ' inhouse' }, 'recommended の前の空白 (trim して受けない)');
  bad({ ...goodShoot(), recommended: 'studio' }, '知らない recommended');
  bad({ ...goodShoot(), reason: '   ' }, '空の理由');
  bad({ ...goodShoot(), reason: 5 }, '理由が文字列でない');
  bad({ ...goodShoot(), reason: 'あ'.repeat(shootLib.SHOOT_REASON_MAX + 1) }, '長すぎる理由');
  bad({ ...goodShoot(), reason: 'a' + String.fromCharCode(0) + 'b' }, '理由に制御文字');
  bad({ ...goodShoot(), images: {} }, 'images が配列でない');
  bad({ ...goodShoot(), images: [] }, 'images が空');
  bad({ ...goodShoot(), images: Array.from({ length: 11 }, (_, i) => ({ no: i, needs_shoot: false })) }, 'images が多すぎる', Array.from({ length: 11 }, (_, i) => i));
  const withImage = (patch, i = 0) => { const g = goodShoot(); g.images[i] = { ...g.images[i], ...patch }; return g; };
  bad(withImage({ no: '2' }), 'no が文字列');
  bad(withImage({ no: 1.5 }), 'no が小数');
  bad(withImage({ no: 0 }), 'no が重複');
  // 🚨 欠け・前後の空白・食い違いを直して受けない (送られた値と保存する値を同じにする・Codex PR-C 名指し1 M)
  bad({ ...goodShoot(), reason: '  理由です  ' }, '理由の前後に空白 (trim して受けない)');
  bad({ ...goodShoot(), reason: '理由です\n' }, '理由の後ろに改行');
  { const g = goodShoot(); delete g.images[1].cut; bad(g, '撮影が要らない画像でも cut が欠けている (補わない)'); }
  { const g = goodShoot(); delete g.images[0].ng; bad(g, '撮影が要る画像の ng が欠けている'); }
  bad(withImage({ cut: ' 玄関の手元' }), 'カット名の前に空白');
  bad(withImage({ cut: '商品を撮影' }, 1), '🚨 撮影が要らない画像に撮影指示が書いてある (食い違い)');
  // 構成と照らさない使い方 (imageNos なし) でも、番号の型は見る (照らす側に頼らない)
  for (const [no, why] of [['2', '文字列'], [1.5, '小数'], [-1, '負'], [100, '大きすぎる'], [null, 'null']]) {
    const r = shootLib.validateShootJudgement(withImage({ no }, 2), {});
    ok(!r.ok, `🚨 構成と照らさなくても no の ${why} は通さない`);
  }
  bad(withImage({ needs_shoot: 'true' }), 'needs_shoot が文字列');
  bad(withImage({ extra: 1 }), '知らないキー (画像)');
  bad(withImage({ props: 3 }), '項目が文字列でない');
  bad(withImage({ cut: '' }), '撮影が要るのにカット名が無い');
  bad(withImage({ composition: '  ' }), '撮影が要るのに構図が無い');
  bad(withImage({ tone: 'x'.repeat(shootLib.SHOOT_FIELD_MAX + 1) }), '長すぎる項目');
  bad({ ...goodShoot(), recommended: 'none' }, '🚨 撮影不要のおすすめなのに撮影が要る画像がある (食い違い)');
  bad({ recommended: 'photographer', reason: 'r', images: [0, 1, 2].map((no) => ({ no, needs_shoot: false })) }, '🚨 カメラマン撮影のおすすめなのに撮影が要る画像が無い (食い違い)');
  bad({ ...goodShoot(), images: goodShoot().images.slice(0, 2) }, '🚨 構成の画像が足りない (1枚目 の判定が無い)');
  bad({ ...goodShoot(), images: [...goodShoot().images, { no: 7, needs_shoot: false }] }, '🚨 構成に無い画像の判定がある');
  bad({ ...goodShoot(), reason: 'x'.repeat(40_000) }, '大きすぎる');
  const circ = goodShoot(); circ.self = circ;
  let threw = false;
  try { bad(circ, '循環参照 (JSON にできない)'); } catch { threw = true; }
  ok(!threw, '🚨 JSON にできない値でも例外を出さない');

  const vc = shootLib.validateShootForComposition(goodShoot(), OUT);
  ok(vc.ok, '構成 (0〜2枚目) と照らして通る');
  const vx = shootLib.validateShootForComposition(goodShoot(), 'これは構成ではありません');
  ok(!vx.ok && /読めない/.test(vx.errors[0]), '構成を読めなければ照らせないので通さない');
  eq(shootLib.compositionImageNos(OUT), [0, 1, 2], '構成の画像番号は lp-tool の本番パーサーで読む');

  // 🚨 指示文は別の定数。構成の指示文 (スタッフの ChatGPT 定型文と共有の正本) には入っていない
  ok(!pt.PRODUCT_ANALYSIS_INSTRUCTION.includes('撮影判定') && !pt.PRODUCT_ANALYSIS_INSTRUCTION.includes('needs_shoot'),
    '🚨 構成の指示文 (PRODUCT_ANALYSIS_INSTRUCTION) は撮影判定を含まない (スタッフ版と同じまま)');
  const staff = pt.buildProductAnalysisPrompt({ name: NAME }, { product_info_text: 'x' }, '');
  ok(!staff.includes('needs_shoot'), 'スタッフの定型文に撮影判定の指示は混ざらない');
  ok(['recommended', 'reason', 'images', 'needs_shoot', ...shootLib.SHOOT_CUT_FIELDS].every((k) => shootLib.SHOOT_JUDGE_INSTRUCTION.includes(k)),
    '撮影判定の指示文は検査するキーを全部書いている (指示と検査がずれない)');
  ok(shootLib.SHOOT_JUDGE_INSTRUCTION.includes('⑦の本文には何も足さない'), '🚨 指示文が「⑦の本文に足さない」と言っている');
}

// ── 依頼を done まで進める道具 (サーバの lib を順に呼ぶ。実行役 (phlp) の通しは runner の試験) ──
let seq = 0;
const spec = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '## 出力形式\n'.repeat(5), sheetTitles: ['出力形式'], actor: 't@x' }).spec;
const mkDraft = (name = NAME) => {
  seq++;
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'test')`).run(`LP-SH-${seq}`, name).lastInsertRowid);
  db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`).run(id, '天然ハッカ油 100ml');
  db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, ?, '2026-09-30T00:00:00.000Z')`).run(id, `FILEIDSH${String(seq).padStart(4, '0')}`);
  return db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
};
/** 依頼 → claim → (画像を配った記録) → 予約。結果を出す直前まで */
function reserveFor(draft, { serve = true } = {}) {
  const fileId = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').get(draft.id).drive_file_id;
  const req = lp.requestJob(db, {
    draft, spec, productInfo: '天然ハッカ油 100ml', colorVariations: '', images: [{ file_id: fileId, modified_time: '2026-09-30T00:00:00.000Z' }],
    idempotencyKey: `shoot-key-${seq}-${Math.random().toString(36).slice(2, 8)}`, actor: 't@x',
  });
  if (!req.ok) throw new Error('requestJob: ' + req.error);
  const run = `run-sh-${req.job.id}`;
  const cl = lp.claimJob(db, { runnerRunId: run, maxImages: 16 });
  if (cl.job?.job_id !== req.job.id) throw new Error('claim が別の依頼を掴んだ: ' + JSON.stringify(cl));
  if (serve) lp.recordImageServed(db, req.job.id, { leaseToken: cl.job.lease_token, fileId, sha256: 'a'.repeat(64), bytes: 10 });
  const rv = lp.reserveGeneration(db, req.job.id, { leaseToken: cl.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION });
  if (!rv.ok) throw new Error('reserve: ' + rv.error);
  return { jobId: req.job.id, gid: rv.generation_id, packetHash: cl.job.packet_hash, run, leaseToken: cl.job.lease_token, packet: cl.job.packet };
}
const matchModel = (run) => lp.recordModelCheck(db, { runnerRunId: run, actualModels: ['claude-opus-5-5'] });
const jobRow = (id) => db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(id);
const accept = (r, extra = {}) => lp.submitResult(db, r.gid, { packetHash: r.packetHash, verdict: 'accepted', output: OUT, lint: { ok: true }, reviewRounds: 1, ...extra });

console.log('② packet に撮影判定の指示 (別の欄)・構成の指示文はそのまま');
{
  const d = mkDraft();
  const r = reserveFor(d);
  eq(r.packet.packet_version, 6, 'packet の版は 6 (PR-C2 で撮影判定の仕様書 shoot_spec を足した)');
  eq(r.packet.shoot_spec, null, '仕様書「新商品初動判定」を取り込む前は shoot_spec: null (PR-C の決まりのまま)');
  eq(r.packet.shoot_instruction, shootLib.SHOOT_JUDGE_INSTRUCTION, '🚨 claim で撮影判定の指示が届く (受付時に固定)');
  eq(r.packet.instruction, pt.PRODUCT_ANALYSIS_INSTRUCTION, '🚨 構成の指示文は正本のまま (くらべっこの前提を崩さない)');
  lp.submitResult(db, r.gid, { packetHash: r.packetHash, verdict: 'rejected', reason: '片付け' });
}

console.log('③ 正しい撮影判定 → 保存して、読み口 (latestShootJudgement) から読める');
let dOk, rOk;
{
  dOk = mkDraft();
  rOk = reserveFor(dOk);
  const res = accept(rOk, { shoot: goodShoot() });
  ok(res.ok && res.status === 'done', '構成は done');
  eq(res.shoot, { status: 'saved' }, '実行役に「保存した」と返す');
  const row = jobRow(rOk.jobId);
  eq(row.output_text, OUT, '🚨 ⑦の本文は送られたまま (撮影判定を足さない)');
  ok(JSON.parse(row.shoot_json).recommended === 'inhouse' && row.shoot_error === null, '撮影判定は別の列に保存');
  eq(lp.latestShootJudgement(db, dOk.id), null, '🚨 実モデルの確認が付くまでは出さない (構成の本文と同じ扱い)');
  matchModel(rOk.run);
  const j = lp.latestShootJudgement(db, dOk.id);
  ok(j && j.available && j.recommended === 'inhouse' && j.job_id === rOk.jobId, '確認が付いたら読める');
  eq(j.images.map((x) => [x.no, x.needs_shoot]), [[0, false], [1, false], [2, true]], '画像ごとの要否 (撮影指示書 PR-D の材料) も読める');
  eq(j.images[2].cut, '玄関でスプレーする手元', 'カット名も読める');
  const st = lp.jobStateFor(db, dOk.id);
  eq(st.shoot, { job_id: rOk.jobId, available: true, format: 1, recommended: 'inhouse', reason: goodShoot().reason,
    unbox: '', cut_count: 1, send_targets: '', warnings: [], missing: null },
    '画面の状態 (ポーリングの応答) には おすすめと理由と短い概要だけ (画像ごとの要否は出さない・PR-C の形は開封・送付が空)');
  ok(st.job.output_text === OUT, '構成の本文も今どおり出る');

  // 同じ結果の再送 = 保存済みを返す / 撮影判定だけ違う再送 = 別の結果なので断る
  const again = accept(rOk, { shoot: goodShoot() });
  ok(again.ok && again.already && again.shoot.status === 'saved', '同じ結果 (撮影判定も同じ) の再送は保存済みを返す');
  const diff = accept(rOk, { shoot: { ...goodShoot(), reason: '別の理由' } });
  eq(diff.code, 'already_finalized', '🚨 撮影判定だけ違う再送は上書きしない');
  const noShootRetry = accept(rOk);
  eq(noShootRetry.code, 'already_finalized', '撮影判定を落とした再送も別の結果として断る');
}

console.log('④ 壊れた撮影判定 → 構成は受け付け、撮影判定だけ「AI の判定なし」');
{
  const d = mkDraft();
  const r = reserveFor(d);
  const broken = { ...goodShoot(), recommended: 'none' };   // 食い違い
  const res = accept(r, { shoot: broken });
  ok(res.ok && res.status === 'done', '🚨 構成は done (撮影判定を巻き込んで落とさない)');
  ok(res.shoot.status === 'invalid' && /撮影が要る画像/.test(res.shoot.error), `実行役に理由を返す (${res.shoot.error})`);
  const row = jobRow(r.jobId);
  ok(row.shoot_json === null && /撮影が要る画像/.test(row.shoot_error), '保存は理由だけ');
  eq(row.output_text, OUT, '構成の本文は保存される');
  ok(JSON.parse(row.lint_json).ok === true && JSON.parse(row.lint_json).source === 'server', '🚨 lint の扱いは今どおり (サーバの結果)');
  matchModel(r.run);
  eq(lp.latestShootJudgement(db, d.id), { job_id: r.jobId, available: false, format: null, recommended: null, shooter: null, unbox: '',
    send_targets: '', purpose: '', finish: '', usage: '', reason: null, cuts: [], cut_count: 0, images: [], warnings: [], missing: 'invalid' },
    '読み口は「AI の判定なし (形が違った)」(項目はそろえて中身は空)');

  {
    // 🚨 大きすぎる判定どうしでも、中身が違えば別の結果 (「使えない」1 つに畳まない・Codex PR-C 名指し1 M)
    const dd = mkDraft();
    const rr = reserveFor(dd);
    const first = accept(rr, { shoot: { ...goodShoot(), reason: 'x'.repeat(50_000) } });
    ok(first.ok && first.shoot.status === 'invalid', '前提: 大きすぎる判定は invalid');
    ok(accept(rr, { shoot: { ...goodShoot(), reason: 'x'.repeat(50_000) } }).already === true, '同じ大きすぎる判定の再送は保存済みを返す');
    eq(accept(rr, { shoot: { ...goodShoot(), reason: 'y'.repeat(60_000) } }).code, 'already_finalized', '🚨 別の大きすぎる判定の再送は上書きしない');
  }
  {
    // 🚨 深い入れ子 (JSON.stringify は通るが再帰の canonicalJson は溢れる) でも 500 にせず、構成は done (Codex PR-C 名指し2 High)
    const deep = () => { const root = { ...goodShoot(), extra: {} }; let c = root.extra; for (let i = 0; i < 20_000; i++) { c.a = {}; c = c.a; } return root; };
    const dd = mkDraft();
    const rr = reserveFor(dd);
    let rs = null;
    try { rs = accept(rr, { shoot: deep() }); } catch (e) { rs = { thrown: String(e.message).slice(0, 80) }; }
    ok(rs?.ok && rs.status === 'done' && rs.shoot?.status === 'invalid', `🚨 深い入れ子の撮影判定でも例外にならず、構成は done (${JSON.stringify(rs?.thrown || rs?.shoot?.status)})`);
    ok(jobRow(rr.jobId).status === 'done' && db.prepare('SELECT status FROM ph_lp_compose_generations WHERE id = ?').get(rr.gid).status === 'accepted', '予約も確定している (reserved のまま残らない)');
    let again = null;
    try { again = accept(rr, { shoot: deep() }); } catch (e) { again = { thrown: String(e.message).slice(0, 80) }; }
    ok(again?.already === true, '同じ深い入れ子の再送は保存済みを返す');
    const deeper = deep(); let c = deeper.extra; while (c.a) c = c.a; c.b = 1;
    let other = null;
    try { other = accept(rr, { shoot: deeper }); } catch (e) { other = { thrown: String(e.message).slice(0, 80) }; }
    eq(other?.code, 'already_finalized', '🚨 中身の違う深い入れ子の再送は上書きしない (「使えない」1 つに畳まない・Codex PR-C 名指し3 M)');
    // 再帰しない直列化は、ふつうの値では canonicalJson と同じ
    const sample = { b: [1, { y: 'あ', x: null }], a: true, c: 'x"y' };
    eq(lp.canonicalJsonFlat(sample), lp.canonicalJson(sample), '再帰しない直列化はふつうの値で canonicalJson と同じ');
    const cyc = { a: 1 }; cyc.self = cyc;
    eq(lp.canonicalJsonFlat(cyc), null, '循環参照は null (例外にしない)');
  }
  for (const [label, shoot] of [
    ['大きすぎる', { ...goodShoot(), reason: 'x'.repeat(50_000) }],
    ['文字列', 'inhouse'],
    ['数', 3],
    ['配列', [1, 2]],
  ]) {
    const dd = mkDraft();
    const rr = reserveFor(dd);
    const rs = accept(rr, { shoot });
    ok(rs.ok && rs.status === 'done' && rs.shoot.status === 'invalid', `🚨 ${label} の撮影判定でも構成は done・撮影判定だけ invalid`);
  }
  const dc = mkDraft();
  const rc = reserveFor(dc);
  const circ = goodShoot(); circ.self = circ;
  let rcRes = null;
  try { rcRes = accept(rc, { shoot: circ }); } catch (e) { rcRes = { thrown: String(e.message) }; }
  ok(rcRes?.ok && rcRes.status === 'done' && rcRes.shoot.status === 'invalid', `🚨 JSON にできない撮影判定でも例外にならない (${JSON.stringify(rcRes?.thrown || rcRes?.shoot)})`);
}

console.log('⑤ 撮影判定を送らない古い実行役 → 今どおり通る');
{
  const d = mkDraft();
  const r = reserveFor(d);
  const res = accept(r);
  ok(res.ok && res.status === 'done', '構成は done');
  eq(res.shoot, { status: 'not_sent' }, '「送られていない」と返す');
  const gen = db.prepare('SELECT payload_hash FROM ph_lp_compose_generations WHERE id = ?').get(r.gid);
  const before = lp.sha256(lp.canonicalJson({ verdict: 'accepted', output: OUT, lint: JSON.stringify({ ok: true }), review_rounds: 1, reason: '' }));
  eq(gen.payload_hash, before, '🚨 撮影判定が無いときの payload_hash は この PR の前と同じ (古い実行役の再送が「別の結果」にならない)');
  ok(accept(r, { shoot: null }).already === true, 'null は「送らなかった」と同じ');
  matchModel(r.run);
  eq(lp.latestShootJudgement(db, d.id)?.missing, 'not_sent', '読み口は「AI の判定なし (送られていない)」');
}

console.log('⑥ 読み口は「いちばん新しい使える構成」だけ');
{
  // rejected (下書きつき) の撮影判定は保存するが、使える構成ではないので読み口には出さない
  const d = mkDraft();
  const r = reserveFor(d);
  const rj = lp.submitResult(db, r.gid, { packetHash: r.packetHash, verdict: 'rejected', output: OUT, reason: 'high が残った', shoot: goodShoot() });
  ok(rj.ok && rj.status === 'failed' && rj.shoot.status === 'saved', 'チェックを通らなかった構成にも撮影判定は残す');
  matchModel(r.run);
  eq(lp.latestShootJudgement(db, d.id), null, '🚨 rejected の構成は「使える構成」ではないので出さない (画像生成と同じ基準)');
  // 下書きを残さない rejected (画像を見ていない) には付けない
  const d2 = mkDraft();
  const r2 = reserveFor(d2, { serve: false });
  const rj2 = lp.submitResult(db, r2.gid, { packetHash: r2.packetHash, verdict: 'rejected', output: OUT, reason: 'x', shoot: goodShoot() });
  ok(rj2.ok && rj2.shoot.status === 'ignored' && jobRow(r2.jobId).shoot_json === null, '構成が残らない結果には撮影判定も付けない (ignored)');

  // 新しい依頼が動いている間は、前の構成の判定を出さない
  ok(lp.latestShootJudgement(db, dOk.id)?.available === true, '前提: 前の構成の判定がある');
  const fileId = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').get(dOk.id).drive_file_id;
  const again = lp.requestJob(db, {
    draft: dOk, spec, productInfo: '天然ハッカ油 100ml', colorVariations: '', images: [{ file_id: fileId }], idempotencyKey: 'shoot-key-again-1', actor: 't@x',
  });
  ok(again.ok, '作り直しを頼む');
  eq(lp.latestShootJudgement(db, dOk.id), null, '🚨 作り直しを頼んだら前の構成の判定は出さない (前の構成は「今の構成」ではない)');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(again.job.id);

  // DB を直接触られて構成と合わなくなった判定は出さない
  const d3 = mkDraft();
  const r3 = reserveFor(d3);
  accept(r3, { shoot: goodShoot() });
  matchModel(r3.run);
  ok(lp.latestShootJudgement(db, d3.id).available, '前提: 読める');
  const tampered = goodShoot(); tampered.images = tampered.images.slice(0, 2);
  db.prepare('UPDATE ph_lp_compose_jobs SET shoot_json = ? WHERE id = ?').run(JSON.stringify(tampered), r3.jobId);
  eq(lp.latestShootJudgement(db, d3.id)?.missing, 'invalid', '🚨 保存後に壊れた判定は「判定なし」 (読むときももう一度検査する)');
  db.prepare('UPDATE ph_lp_compose_jobs SET shoot_json = ? WHERE id = ?').run('{壊れた', r3.jobId);
  eq(lp.latestShootJudgement(db, d3.id)?.missing, 'invalid', 'JSON として読めなくても例外にならない');
  eq(lp.latestShootJudgement(db, 'abc'), null, '壊れた ID は null');
}

console.log('⑦ 出す前の検査 (lintForJob) — 撮影判定も同じ検査を何度でも受けられる');
{
  const d = mkDraft();
  const r = reserveFor(d);
  const plain = lp.lintForJob(db, r.jobId, { leaseToken: r.leaseToken, output: OUT });
  ok(plain.ok && plain.lint.ok && !('shoot' in plain), '撮影判定を渡さなければ今どおり (shoot は返さない)');
  const good = lp.lintForJob(db, r.jobId, { leaseToken: r.leaseToken, output: OUT, shoot: goodShoot() });
  eq(good.shoot, { ok: true, errors: [], warnings: [] }, '正しい撮影判定は ok');
  const badL = lp.lintForJob(db, r.jobId, { leaseToken: r.leaseToken, output: OUT, shoot: { ...goodShoot(), images: [] } });
  ok(badL.shoot.ok === false && badL.shoot.errors.length > 0, '壊れた撮影判定は何が悪いかを返す');
  eq(badL.lint, plain.lint, '🚨 撮影判定が通らなくても、構成の lint の結果は変わらない');
  lp.submitResult(db, r.gid, { packetHash: r.packetHash, verdict: 'rejected', reason: '片付け' });
}

console.log('⑧ 古い版 (4) の依頼は claim で止める (押し直し)');
{
  const d = mkDraft();
  const fileId = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').get(d.id).drive_file_id;
  const req = lp.requestJob(db, { draft: d, spec, productInfo: 'x', colorVariations: '', images: [{ file_id: fileId }], idempotencyKey: 'shoot-key-v4-1', actor: 't@x' });
  const p = JSON.parse(req.job.packet_json);
  delete p.shoot_instruction; p.packet_version = 4;
  db.prepare('UPDATE ph_lp_compose_jobs SET packet_json = ?, packet_hash = ?, packet_version = 4 WHERE id = ?').run(JSON.stringify(p), lp.sha256(lp.canonicalJson(p)), req.job.id);
  const cl = lp.claimJob(db, { runnerRunId: 'run-v4', maxImages: 16 });
  ok(cl.job === null && jobRow(req.job.id).error_code === 'packet_outdated', '🚨 撮影判定の指示が無い古い依頼は渡さない (packet_outdated)');
}

// ── HTTP: 本物の router と service-api ──
const app = express();
let session = { email: 'nakahara@x', role: 'admin' };
app.use((req, _res, next) => { req.session = session; next(); });
app.use('/apps/product-hub', router);
app.use('/apps/product-hub/service-api', serviceApiRouter);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;
const call = async (method, p, body, headers = {}) => {
  const res = await fetch(base + p, { method, headers: { 'Content-Type': 'application/json', ...headers }, body: body === undefined ? undefined : JSON.stringify(body) });
  let json = null; try { json = await res.json(); } catch { /* */ }
  return { status: res.status, json };
};
const svc = (method, p, body) => call(method, '/service-api' + p, body, { Authorization: 'Bearer test-token-lp-shoot' });

console.log('⑨ service-api の result / lint — shoot_json の欄');
let dHttp;
{
  dHttp = mkDraft();
  const r = reserveFor(dHttp);
  const lintRes = await svc('POST', `/lp-compose/jobs/${r.jobId}/lint`, { lease_token: r.leaseToken, output: OUT, shoot_json: { ...goodShoot(), recommended: 'none' } });
  ok(lintRes.status === 200 && lintRes.json.lint.ok === true && lintRes.json.shoot.ok === false, 'lint の口: 構成は通り、撮影判定は通らないと返す');
  const lintNo = await svc('POST', `/lp-compose/jobs/${r.jobId}/lint`, { lease_token: r.leaseToken, output: OUT });
  ok(lintNo.status === 200 && !('shoot' in lintNo.json), 'lint の口: 撮影判定を送らなければ今どおりの応答');
  const res = await svc('POST', `/lp-compose/generations/${r.gid}/result`, {
    packet_hash: r.packetHash, verdict: 'accepted', output: OUT, lint: { ok: true }, review_rounds: 1,
    shoot_json: { ...goodShoot(), reason: '2枚目 の写真がありません </script><script>alert(1)</script>' },
  });
  ok(res.status === 200 && res.json.status === 'done' && res.json.shoot.status === 'saved', `result の口: 撮影判定を保存 (${JSON.stringify(res.json?.shoot)})`);
  matchModel(r.run);
  const st = await call('GET', `/api/drafts/${dHttp.id}/lp-compose`);
  ok(st.json.shoot?.recommended === 'inhouse' && st.json.shoot.available === true, '画面の API (ポーリング) に おすすめが乗る');

  // 壊れた shoot_json でも構成は通る (HTTP でも)
  const d2 = mkDraft();
  const r2 = reserveFor(d2);
  const res2 = await svc('POST', `/lp-compose/generations/${r2.gid}/result`, {
    packet_hash: r2.packetHash, verdict: 'accepted', output: OUT, lint: { ok: true }, review_rounds: 1, shoot_json: { recommended: 'x' },
  });
  ok(res2.status === 200 && res2.json.status === 'done' && res2.json.shoot.status === 'invalid', '🚨 result の口: 壊れた撮影判定でも 200・構成は done');
  const d3 = mkDraft();
  const r3 = reserveFor(d3);
  const res3 = await svc('POST', `/lp-compose/generations/${r3.gid}/result`, {
    packet_hash: r3.packetHash, verdict: 'accepted', output: OUT, lint: { ok: true }, review_rounds: 1,
  });
  ok(res3.status === 200 && res3.json.status === 'done' && res3.json.shoot.status === 'not_sent', 'result の口: 送らない古い実行役も今どおり');
}

console.log('⑩ 詳細画面 — 撮影判定の箱におすすめの材料を埋める (本番の router で描く)');
{
  const html = await (await fetch(`${base}/detail/${dHttp.id}`)).text();
  const m = html.match(/<script type="application\/json" id="shoot-rec-json">([\s\S]*?)<\/script>/);
  ok(!!m, '撮影判定の箱に AI の判定 (JSON) を埋める');
  const emb = m ? JSON.parse(m[1]) : null;
  ok(emb && emb.recommended === 'inhouse' && emb.reason.includes('alert(1)'), '中身は おすすめと理由');
  ok(!html.includes('</script><script>alert(1)'), '🚨 理由に </script> があっても script が閉じない (< を潰す)');
  const box = html.slice(html.indexOf('id="shoot-mode-box"'), html.indexOf('</section>', html.indexOf('id="shoot-mode-box"')));
  ok(box.includes('id="shoot-rec"') && box.includes('id="shoot-rec-adopt"') && box.includes('id="shoot-rec-json"'), '🚨 おすすめの表示は撮影判定の箱 (#shoot-mode-box) の中だけ');
  eq((box.match(/data-shoot-rec-badge="/g) || []).length, 3, '札は 3 択のボタンそれぞれに (初めは隠す)');
  ok(/data-shoot-rec-badge="inhouse" hidden/.test(box), '札は JS が出す (サーバは隠して描く)');
  // まだ AI の構成が無い商品では null
  const dNo = mkDraft();
  const htmlNo = await (await fetch(`${base}/detail/${dNo.id}`)).text();
  ok(/<script type="application\/json" id="shoot-rec-json">null<\/script>/.test(htmlNo), '構成がまだ無ければ null');
  // インライン script が JS として読める (新しく足した塊で画面全体を壊していない)
  const blocks = [...html.matchAll(/<script(?![^>]*\stype=)[^>]*>([\s\S]*?)<\/script>/g)].map((x) => x[1]);
  let badJs = 0;
  for (const code of blocks) { try { new Function(code); } catch { badJs++; } }
  eq(badJs, 0, '🚨 詳細画面のインライン script がすべて JS として読める');
}

console.log('⑪ 権限 — 実際に押す役割 (画像登録者) で「おすすめにする」と同じ送り方が通る');
{
  const img = wf.createStaff({ name: '撮影判定 画像登録者 (PR-C)', kind: 'internal', portal_email: 'shoot-prc-img@b-faith.biz' });
  db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(img);
  const none = wf.createStaff({ name: '撮影判定 役割なし (PR-C)', kind: 'internal', portal_email: 'shoot-prc-none@b-faith.biz' });
  session = { email: 'shoot-prc-none@b-faith.biz', role: 'user' };
  const html = await (await fetch(`${base}/detail/${dHttp.id}`)).text();
  ok(html.includes('id="shoot-rec-json">{'), '役割が無くても AI のおすすめは見える (読むだけ)');
  const aiJob = lp.latestShootJudgement(db, dHttp.id).job_id;
  const rNo = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'inhouse', expected: null, ai_job_id: aiJob });
  eq(rNo.status, 403, '役割が無い人は「おすすめにする」(= /shoot-mode) で変えられない');
  session = { email: 'shoot-prc-img@b-faith.biz', role: 'user' };
  // 🚨 古いタブ: 画面が見ていたおすすめ (ai_job_id) が今の AI のおすすめと違えば 409 (Codex PR-C 名指し3 M)
  const ipOf = () => db.prepare('SELECT shoot_mode, material_status FROM draft_image_production WHERE draft_id = ?').get(dHttp.id) || {};
  const rOldJob = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'inhouse', expected: null, ai_job_id: aiJob - 1 });
  ok(rOldJob.status === 409 && /おすすめが新しく/.test(rOldJob.json?.error || '') && !ipOf().shoot_mode, `🚨 前の構成のおすすめ (別の job) は保存しない (${rOldJob.status})`);
  const rOtherMode = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'none', expected: null, ai_job_id: aiJob });
  ok(rOtherMode.status === 409 && !ipOf().shoot_mode, '🚨 「おすすめにする」なのにおすすめと違う判定は保存しない');
  const rBadId = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'inhouse', expected: null, ai_job_id: String(aiJob) });
  ok(rBadId.status === 400 && !ipOf().shoot_mode, 'ai_job_id は数だけ (文字列は 400)');
  const rImg = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'inhouse', expected: null, ai_job_id: aiJob });
  ok(rImg.status === 200 && rImg.json.shoot_mode === 'inhouse', '画像登録者 (管理者でない) は いまのおすすめを選べる');
  const rStale = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'none', expected: null });
  eq(rStale.status, 409, '🚨 古い画面 (未判定のつもり) からの送信は 409 (別タブとの食い違いは今どおり止まる)');
  const rHuman = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'photographer', expected: 'inhouse' });
  ok(rHuman.status === 200 && ipOf().shoot_mode === 'photographer', '3 択を直接押す (ai_job_id なし) なら AI と違う判定も今どおり選べる');
  session = { email: 'nakahara@x', role: 'admin' };
  eq(ipOf().shoot_mode, 'photographer', '判定は人が押したものだけ');
  // 作り直しを頼んだら (使える構成が無い) おすすめにするは通らない
  const fileId = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').get(dHttp.id).drive_file_id;
  const redo = lp.requestJob(db, { draft: dHttp, spec, productInfo: 'x', colorVariations: '', images: [{ file_id: fileId }], idempotencyKey: 'shoot-key-redo-http', actor: 't@x' });
  const rRedo = await call('POST', `/api/drafts/${dHttp.id}/shoot-mode`, { mode: 'inhouse', expected: 'photographer', ai_job_id: aiJob });
  ok(rRedo.status === 409 && ipOf().shoot_mode === 'photographer', '🚨 作り直しを頼んだ後の古いおすすめも保存しない');
  db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(redo.job.id);
  // AI のおすすめが出ても撮影判定は自動で書き換わらない
  const dAuto = mkDraft();
  const rA = reserveFor(dAuto);
  accept(rA, { shoot: { ...goodShoot() } });
  matchModel(rA.run);
  await call('GET', `/api/drafts/${dAuto.id}/lp-compose`);
  await fetch(`${base}/detail/${dAuto.id}`);
  const ipA = db.prepare('SELECT shoot_mode, material_status FROM draft_image_production WHERE draft_id = ?').get(dAuto.id);
  ok(ipA.shoot_mode === null && ipA.material_status === null, '🚨 AI の判定が付いても撮影判定・素材ステータスは自動で変わらない');
}

console.log('⑫ 画面の JS (@shoot-rec) — 偽の document で動かす');
{
  const HERE = path.dirname(fileURLToPath(import.meta.url));
  const src = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8');
  const cut = (a, b) => src.slice(src.indexOf(a), src.indexOf(b));
  const flowChunk = cut('/* @image-flow:start', '/* @image-flow:end */');
  const recChunk = cut('/* @shoot-rec:start', '/* @shoot-rec:end */');
  ok(recChunk.length > 200 && !recChunk.includes('<%'), '画面の JS: おすすめの部分を切り出せる (EJS の値を含まない)');
  ok(src.includes('if (window.phShootRec) window.phShootRec.update(s.shoot);'), '構成のポーリングの描画からおすすめを入れ直す');
  ok(/onShootModeChanged: function \(\) \{ shootRec\.refresh\(\); \}/.test(src), '撮影判定を押したら おすすめの文もそろえる');
  const F = new Function(flowChunk + recChunk + '\nreturn { initImageFlow, initShootRec, shootRecView };')();

  // 純粋関数: 決め方と文言
  const J = { job_id: 1, available: true, recommended: 'inhouse', reason: '2枚目 の写真がありません。', missing: null };
  eq(F.shootRecView(null, null).show, false, '構成が無ければ出さない');
  const v0 = F.shootRecView(J, null);
  ok(v0.show && v0.recommended === 'inhouse' && v0.adopt && v0.text.startsWith('AIのおすすめ: 社内撮影 — 2枚目 の写真がありません。') && /まだ判定していません/.test(v0.text),
    `未判定: おすすめ・理由・「おすすめにする」(${v0.text})`);
  const v1 = F.shootRecView(J, 'inhouse');
  ok(v1.show && !v1.adopt && !/いまの判定/.test(v1.text), 'おすすめと同じなら「おすすめにする」は出さない');
  const v2 = F.shootRecView(J, 'none');
  ok(v2.adopt && /いまの判定は「撮影不要」/.test(v2.text), '違う判定なら、いまの判定も書き、決めるのは人だと書く');
  const vNs = F.shootRecView({ job_id: 1, available: false, recommended: null, reason: null, missing: 'not_sent' }, null);
  ok(vNs.show && !vNs.recommended && !vNs.adopt && /撮影判定はありません/.test(vNs.text) && /前の仕組み/.test(vNs.text), '判定が無い (古い実行役) ときはそう書く');
  const vIv = F.shootRecView({ job_id: 1, available: false, recommended: null, reason: null, missing: 'invalid' }, null);
  ok(/形が正しくなかった/.test(vIv.text), '判定が壊れていたときはそう書く');
  ok(!F.shootRecView({ ...J, recommended: 'studio' }, null).recommended, '知らないおすすめは札を出さない');

  // 偽の DOM
  const pending = [];
  const fakeEl = (init = {}) => {
    const ls = {};
    const attrs = { ...(init.attrs || {}) };
    const el = {
      value: '', defaultValue: '', textContent: init.textContent ?? '', dataset: { ...(init.dataset || {}) }, hidden: !!init.hidden, disabled: false,
      innerHTMLSet: false,
      addEventListener: (t, fn) => { (ls[t] = ls[t] || []).push(fn); },
      fire: async (t) => { for (const fn of ls[t] || []) await fn(); },
      click() { if (el.disabled) return; const p = el.fire('click'); pending.push(p); return p; },
      getAttribute: (k) => (k in attrs ? attrs[k] : null),
      setAttribute: (k, v) => { attrs[k] = String(v); },
      classList: { set: new Set(), toggle(c, on) { if (on) this.set.add(c); else this.set.delete(c); } },
    };
    Object.defineProperty(el, 'innerHTML', { set() { el.innerHTMLSet = true; }, get() { return ''; } });
    return el;
  };
  // タイマーで後から送る (自動で選ぶ) 実装も拾えるよう、待ってから数える
  const settle = async () => { await new Promise((r) => setTimeout(r, 30)); while (pending.length) await pending.shift(); };
  const build = (current, judgement, rawJson) => {
    const els = {
      'shoot-mode-box': fakeEl({ dataset: { current: current || '' } }),
      'shoot-mode-msg': fakeEl(),
      'shoot-rec': fakeEl({ hidden: true }),
      'shoot-rec-text': fakeEl(),
      'shoot-rec-adopt': fakeEl({ hidden: true }),
      'shoot-rec-json': fakeEl({ textContent: rawJson !== undefined ? rawJson : JSON.stringify(judgement) }),
    };
    const modeBtns = ['none', 'inhouse', 'photographer'].map((m) => fakeEl({ dataset: { shootMode: m }, attrs: { 'aria-checked': m === current ? 'true' : 'false' } }));
    const badges = ['none', 'inhouse', 'photographer'].map((m) => fakeEl({ dataset: { shootRecBadge: m }, hidden: true }));
    const doc = {
      getElementById: (id) => els[id] || null,
      querySelectorAll: (q) => (q === '[data-shoot-mode]' ? modeBtns : q === '[data-shoot-rec-badge]' ? badges : []),
    };
    const posts = []; const reloads = [];
    let postMode = 'ok';
    const rec = F.initShootRec(doc);
    F.initImageFlow(doc, {
      base: '/apps/product-hub/api/drafts/9',
      post: async (url, body) => { posts.push([url, body]); return postMode === 'ok' ? { ok: true } : { ok: false, error: '409 ほかの人が先に変えました' }; },
      showAndReload: (json, m) => reloads.push([json, m]),
      clipboard: { writeText: () => Promise.resolve() }, confirm: () => true,
      onShootModeChanged: () => rec.refresh(),
    });
    return { els, modeBtns, badges, doc, posts, reloads, rec, setPost: (m) => { postMode = m; } };
  };

  const a = build(null, J);
  ok(a.els['shoot-rec'].hidden === false && a.els['shoot-rec-text'].textContent.includes('2枚目 の写真がありません'), '読み込み直後に おすすめと理由が出る');
  eq(a.badges.map((b) => b.hidden), [true, false, true], '🚨 札は おすすめ (社内撮影) のボタンにだけ');
  ok(a.els['shoot-rec-adopt'].hidden === false, '未判定なら「おすすめにする」を出す');
  await settle();
  eq(a.posts.length, 0, '🚨 出すだけで撮影判定は送らない (自動で書き換えない)');
  ok(!a.els['shoot-rec-text'].innerHTMLSet, '🚨 理由は textContent で入れる (AI の文を HTML にしない)');
  await a.els['shoot-rec-adopt'].click();
  await settle();
  eq(a.posts, [['/apps/product-hub/api/drafts/9/shoot-mode', { mode: 'inhouse', expected: null, ai_job_id: 1 }]],
    '🚨 「おすすめにする」= 社内撮影のボタンを押すのと同じ送り方 (expected つき・/shoot-mode) + どの構成のおすすめか (ai_job_id)');
  ok(a.els['shoot-mode-box'].dataset.current === 'inhouse' && a.modeBtns[1].getAttribute('aria-checked') === 'true', '保存できたら画面の判定も社内撮影');
  ok(a.els['shoot-rec-adopt'].hidden === true && !/まだ判定/.test(a.els['shoot-rec-text'].textContent), '選んだら「おすすめにする」は消え、文もそろう (読み直しを待たない)');
  eq(a.reloads.length, 1, '読み直しは今どおり 1 回');
  await a.els['shoot-rec-adopt'].click();
  await settle();
  eq(a.posts.length, 1, 'もう選んであれば押しても送らない');

  const b = build('none', J);
  ok(b.els['shoot-rec-adopt'].hidden === false && /いまの判定は「撮影不要」/.test(b.els['shoot-rec-text'].textContent), '人が違う判定にしていれば、それを書いて「おすすめにする」を出す');
  b.setPost('ng');
  await b.els['shoot-rec-adopt'].click();
  await settle();
  ok(b.posts.length === 1 && b.posts[0][1].expected === 'none' && b.els['shoot-mode-box'].dataset.current === 'none',
    '🚨 送れなかったら判定は変えない (画面も撮影不要のまま・expected は画面が見ていた判定)');
  ok(/保存できませんでした/.test(b.els['shoot-mode-msg'].textContent), '送れなかった理由を出す');
  ok(b.els['shoot-rec-adopt'].hidden === false, '送れなかったら「おすすめにする」は残る');
  // 「おすすめにする」の印は 1 回だけ。その後に 3 択を直接押したら ai_job_id は付かない (人の判定は AI と違ってよい)
  b.setPost('ok');
  await b.modeBtns[1].click();
  await settle();
  ok(b.posts.length === 2 && b.posts[1][1].mode === 'inhouse' && !('ai_job_id' in b.posts[1][1]),
    '🚨 「おすすめにする」が失敗した後に同じボタンを直接押しても ai_job_id は付かない (印は 1 回だけ)');

  // ポーリングで入れ直す
  const c = build(null, null);
  ok(c.els['shoot-rec'].hidden === true && c.badges.every((x) => x.hidden), '構成がまだ無ければ箱も札も出さない');
  c.rec.update(J);
  ok(c.els['shoot-rec'].hidden === false && c.badges[1].hidden === false, '構成ができたら (ポーリングの応答) おすすめが出る');
  await settle();
  eq(c.posts.length, 0, '🚨 構成ができても撮影判定は送らない');
  c.rec.update(undefined);
  ok(c.els['shoot-rec'].hidden === false, 'shoot を持たない古い応答では消さない');
  c.rec.update({ job_id: 2, available: false, recommended: null, reason: null, missing: 'not_sent' });
  ok(c.els['shoot-rec'].hidden === false && c.badges.every((x) => x.hidden) && c.els['shoot-rec-adopt'].hidden === true
    && c.els['shoot-rec'].classList.set.has('none'), '判定が無い構成なら札も「おすすめにする」も出さず、そう書く');
  c.rec.update(null);
  ok(c.els['shoot-rec'].hidden === true, '作り直しを頼んだら (使える構成が無い) 消す');
  let broken = null;
  try { broken = build(null, null, '{壊れた'); } catch (e) { broken = null; }
  ok(broken && broken.els['shoot-rec'].hidden === true, '埋め込みが壊れていても例外にならない (出さないだけ)');
  const fromEmbed = build(null, null, JSON.stringify(J));
  ok(fromEmbed.badges[1].hidden === false, '埋め込み (サーバが描いた JSON) から読む');
}

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
