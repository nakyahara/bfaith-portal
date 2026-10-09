import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor } from './fixtures/lp-compose/index.mjs';
const tmp = await temporaryTestDataDir(import.meta.url, 'test-lp-compose-shoot-spec-');
/**
 * AI の撮影判定を仕様書「新商品初動判定」に沿わせる (画像制作の新フロー PR-C2・2026-10-09)
 * 実行: node scripts/test-ph-lp-compose-shoot-spec.mjs
 *
 * ここで守りたいのは:
 *   ① 決まりをコードに書き写さない。仕様書 (ph_lp_specs の kind = initial_judge) を取り込んで、受付時に packet で固め、
 *      claim で hash を照らして実行役に渡す (LP制作システムの仕様書と同じ作法)
 *   ② 仕様書を取り込む前は PR-C の簡単な決まり (形 v1) のまま動く (取り込む前に壊れない)
 *   ③ 形 v2 (撮影依頼書連携データ) を厳しく見る: キー・型・列挙値・撮影判定とカットの食い違い・構成の画像と 1 対 1。
 *      運用ルール (カメラマンは 5 カット単位) は強制せず、警告だけ
 *   ④ v1 で保存済みの行は読み口 (latestShootJudgement) が v2 の形に寄せて返す。撮影指示書 (PR-D) が読む images の形は保つ
 *   ⑤ 古い実行役 (仕様書を落とせない phlp) には仕様書つきの依頼を掴ませない
 *   ⑥ 表の CHECK を広げる作り直しで、行・id・採番・追記専用のトリガー・一意索引を失わない
 *   ⑦ 画面: 撮影判定の箱に 開封要否・カット数・撮影用送付対象 を短く出す / 一覧の取り込みカードで種類を選べる
 *   ⑧ 撮影指示書 (PR-D): 項目名は CUT_FIELDS / SUMMARY_FIELDS と同じ。v2 なら AI のカット (LP に無いカットも) と概要をそのまま渡す。
 *      要撮影の正本 (編集版の shoot → AI の needs_shoot) と、元の画像を uid の a<番号> で引く作法は D のまま
 */
process.env.PH_SERVICE_TOKEN = 'test-token-lp-shoot-spec';
process.env.PH_LP_COMPOSE_ENABLED = '1';
process.env.PH_LP_COMPOSE_DAILY_CAP = '200';

const fs = (await import('node:fs')).default;
const path = (await import('node:path')).default;
const { spawn } = await import('node:child_process');
const { fileURLToPath } = await import('node:url');
const Database = (await import('better-sqlite3')).default;
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// master (PR-C まで) の ph_lp_specs の定義そのもの。作り直しの試験で「古い DB」を作るのに使う
const OLD_SPECS_SQL = `
    CREATE TABLE IF NOT EXISTS ph_lp_specs (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      kind              TEXT NOT NULL CHECK (kind IN ('product_analysis')),
      title             TEXT NOT NULL,
      body              TEXT NOT NULL,          -- 全タブをテキスト化したもの
      hash              TEXT NOT NULL,          -- body の sha256
      sheet_titles_json TEXT NOT NULL DEFAULT '[]',
      imported_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      imported_by       TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_ph_lp_specs_hash ON ph_lp_specs(kind, hash);
    CREATE INDEX IF NOT EXISTS idx_ph_lp_specs_kind ON ph_lp_specs(kind, id DESC);
    CREATE TRIGGER IF NOT EXISTS trg_ph_lp_specs_no_update BEFORE UPDATE ON ph_lp_specs
      BEGIN SELECT RAISE(ABORT, 'ph_lp_specs は追記専用です (更新は新しい行として入れてください)'); END;
    CREATE TRIGGER IF NOT EXISTS trg_ph_lp_specs_no_delete BEFORE DELETE ON ph_lp_specs
      BEGIN SELECT RAISE(ABORT, 'ph_lp_specs は追記専用です (job が版を参照しています)'); END;`;

// ── ⑥ 起動時の作り直し: 古い定義の表がある DB で initProductHubDB を呼ぶ (本番のデプロイと同じ順) ──
console.log('⑥ 起動時に ph_lp_specs の kind の CHECK を広げる (古い DB → 新しい定義)');
{
  const m = getMirrorDB();
  m.exec(OLD_SPECS_SQL);
  m.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('product_analysis', '旧1', 'b1', 'h1', 't')`).run();
  m.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('product_analysis', '旧2', 'b2', 'h2', 't')`).run();
  let threw = false;
  try { m.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('initial_judge', 'x', 'b', 'h', 't')`).run(); } catch { threw = true; }
  ok(threw, '前提: 古い定義では initial_judge を入れられない');
}
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
{
  const sql = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ph_lp_specs'").get().sql;
  ok(sql.includes("'initial_judge'"), '🚨 起動したら CHECK に initial_judge が入っている (initProductHubDB が作り直しを呼ぶ)');
  eq(db.prepare('SELECT id, title FROM ph_lp_specs ORDER BY id').all(), [{ id: 1, title: '旧1' }, { id: 2, title: '旧2' }], '🚨 行と id はそのまま (job が spec_id で指している)');
  const names = db.prepare("SELECT name FROM sqlite_master WHERE tbl_name = 'ph_lp_specs' AND type IN ('index','trigger') ORDER BY name").all().map((r) => r.name);
  ok(['idx_ph_lp_specs_kind', 'trg_ph_lp_specs_no_delete', 'trg_ph_lp_specs_no_update', 'uq_ph_lp_specs_hash'].every((n) => names.includes(n)), `🚨 索引とトリガーが作り直されている (${names.join(', ')})`);
}

// 作り直しそのもの (純粋に DB だけ。採番・FK・冪等・追記専用を見る)
{
  const mem = new Database(':memory:');
  mem.pragma('foreign_keys = ON');
  mem.exec(OLD_SPECS_SQL);
  mem.exec(`CREATE TABLE jobs (id INTEGER PRIMARY KEY, spec_id INTEGER NOT NULL REFERENCES ph_lp_specs(id))`);
  for (let i = 1; i <= 3; i++) mem.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('product_analysis', ?, ?, ?, 't')`).run('t' + i, 'b' + i, 'h' + i);
  mem.prepare('INSERT INTO jobs (id, spec_id) VALUES (1, 3)').run();
  // 消した行の採番 (3 より大きい id) を残しておく。作り直しで id が再利用されると、古い job が別の版を指す
  mem.prepare("UPDATE sqlite_sequence SET seq = 9 WHERE name = 'ph_lp_specs'").run();
  const r1 = dbmod.migrateLpSpecsKindCheck(mem);
  eq(r1, { migrated: true }, '古い定義なら作り直す');
  eq(mem.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'ph_lp_specs'").get().seq, 9, '🚨 採番の上限 (9) を保つ');
  const id = Number(mem.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('initial_judge', '新商品初動判定', 'b', 'h', 't')`).run().lastInsertRowid);
  eq(id, 10, '🚨 新しい行は 10 番から (消えた番号を使い回さない)');
  let bad = false;
  try { mem.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('simple_lp', 'x', 'b', 'h9', 't')`).run(); } catch { bad = true; }
  ok(bad, '知らない kind は今までどおり CHECK で落ちる');
  let upd = false, del = false, dup = false;
  try { mem.prepare("UPDATE ph_lp_specs SET body = 'x' WHERE id = 1").run(); } catch (e) { upd = /追記専用/.test(e.message); }
  try { mem.prepare('DELETE FROM ph_lp_specs WHERE id = 1').run(); } catch (e) { del = /追記専用/.test(e.message); }
  try { mem.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, imported_by) VALUES ('product_analysis', 'd', 'b', 'h1', 't')`).run(); } catch { dup = true; }
  ok(upd && del, '🚨 作り直した後も追記専用 (UPDATE・DELETE はトリガーが止める)');
  ok(dup, '🚨 作り直した後も (kind, hash) の一意索引が効く');
  eq(mem.pragma('foreign_key_check'), [], '外部キーの不整合なし (job の spec_id が版を指したまま)');
  eq(mem.pragma('foreign_keys', { simple: true }), 1, 'foreign_keys は元 (ON) に戻す');
  eq(dbmod.migrateLpSpecsKindCheck(mem), { migrated: false }, '二度目は何もしない (冪等)');
  mem.close();
}

const lp = await import('../apps/product-hub/lib/lp-compose.js');
const sh = await import('../apps/product-hub/lib/lp-shoot.js');
const sheet = await import('../apps/product-hub/lib/shoot-sheet.js');
const pt = await import('../apps/product-hub/lib/prompt-templates.js');
const wf = await import('../apps/product-hub/lib/workflow.js');
const express = (await import('express')).default;
const { default: router, serviceApiRouter } = await import('../apps/product-hub/router.js');

const NAME = 'ハッカ油スプレー 100ml';
const OUT = compositionFor(NAME);   // 0枚目・1枚目・2枚目
/** 仕様書の形 (v2) の正しい撮影判定 (社内撮影・2枚目に使う 1 カット + LP に無い 1 カット)。項目名は撮影指示書 (PR-D) の CUT_FIELDS と同じ */
const cutOf = (no, patch = {}) => ({
  no, priority: '必須', expression_type: '使用イメージ', variation: '代表1色', target: 'ハッカ油スプレー 100ml (1本)',
  content: '玄関でスプレーする手元', purpose: '使う場面を伝える', finish: '斜め上から手元と商品。ラベルが読めること',
  usage: '楽天 LP 2枚目', open_required: '不要', reference_theme: '玄関で使う手元', lp_image_nos: [2], notice: '顔を写さない', required_notice: '', ...patch,
});
const goodV2 = () => ({
  recommended: 'inhouse',
  conclusion: '2枚目の使用シーンの実写がありません。消耗品なので社内の簡易物撮りで足ります。',
  open_required: '不要', send_targets: 'ハッカ油スプレー 100ml (1本)', purpose: '使用シーンの実写を揃える',
  finish: '玄関で使っている手元が分かる明るい写真', usage: '楽天 LP・サムネイル',
  cuts: [cutOf(1), cutOf(2, { priority: '推奨', expression_type: '物撮り', content: 'ボトルを手に持ったサイズ感', reference_theme: '', notice: '', lp_image_nos: [] })],
  images: [{ no: 0, needs_shoot: false }, { no: 1, needs_shoot: false }, { no: 2, needs_shoot: true }],
});
const noneV2 = () => ({
  recommended: 'none', conclusion: '既存素材と図解で足ります。', open_required: '不要',
  send_targets: '', purpose: '', finish: '', usage: '', cuts: [],
  images: [0, 1, 2].map((no) => ({ no, needs_shoot: false })),
});
const blankCut = { cut: '', composition: '', props: '', background: '', tone: '', ng: '' };
const goodV1 = () => ({
  recommended: 'inhouse', reason: '2枚目 の写真がありません。',
  images: [
    { no: 0, needs_shoot: false, ...blankCut }, { no: 1, needs_shoot: false, ...blankCut },
    { no: 2, needs_shoot: true, cut: '玄関の手元', composition: '斜め上から', props: 'マット', background: '白い床', tone: '自然光', ng: '顔を写さない' },
  ],
});

console.log('③ 形 v2 の検査 (lib/lp-shoot.js・純粋関数)');
{
  const v = sh.validateShootJudgementV2(goodV2(), { imageNos: [0, 1, 2] });
  ok(v.ok, `正しい形 (社内撮影・LP に無いカットつき) は通る ${v.ok ? '' : v.errors.join(' / ')}`);
  eq(Object.keys(v.value), sh.SHOOT_V2_KEYS, '上のキーはこの順で保存する');
  eq(Object.keys(v.value.cuts[0]), sh.SHOOT_V2_CUT_FIELDS.map(([k]) => k), 'カットの項目は仕様書の並び (No〜参考イメージ + 使う LP 画像・注意・必ず出す表示)');
  eq(v.value.cuts[1].lp_image_nos, [], 'LP に無いカットは lp_image_nos: [] のまま');
  eq(v.value.cuts[1].reference_theme, '', '参考イメージは空でよい (仕様書: 参考画像が無ければ空欄)');
  ok(sh.validateShootJudgementV2(noneV2(), { imageNos: [0, 1, 2] }).ok, '追加撮影不要 (カットなし・概要は "") は通る');
  const shared = { ...goodV2(), images: [{ no: 0, needs_shoot: false }, { no: 1, needs_shoot: true }, { no: 2, needs_shoot: true }], cuts: [cutOf(1, { lp_image_nos: [1, 2] })] };
  ok(sh.validateShootJudgementV2(shared, { imageNos: [0, 1, 2] }).ok, '1 つのカットを 2 枚の LP 画像で使える (lp_image_nos が配列)');
  const photo = { ...goodV2(), recommended: 'photographer', cuts: [1, 2, 3, 4, 5].map((n) => cutOf(n, { lp_image_nos: n === 1 ? [2] : [] })) };
  ok(sh.validateShootJudgementV2(photo, { imageNos: [0, 1, 2] }).ok, 'カメラマン撮影 5 カットは通る');
  const photo7 = { ...photo, cuts: [1, 2, 3, 4, 5, 6, 7].map((n) => cutOf(n, { lp_image_nos: n === 1 ? [2] : [] })) };
  ok(sh.validateShootJudgementV2(photo7, { imageNos: [0, 1, 2] }).ok, '🚨 カメラマン撮影 7 カットも通る (5 カット単位は仕様書の運用ルール = 強制しない)');
  eq(sh.shootWarnings(sh.validateShootJudgementV2(photo7, { imageNos: [0, 1, 2] }).value), ['カメラマン撮影は 5 カット単位です (仕様書「新商品初動判定」) が、7 カットです'], '代わりに警告を出す');
  eq(sh.shootWarnings(sh.validateShootJudgementV2(photo, { imageNos: [0, 1, 2] }).value), [], '5 カットなら警告なし');
  eq(sh.shootWarnings(sh.validateShootJudgementV2(goodV2(), { imageNos: [0, 1, 2] }).value), [], '社内撮影には 5 カットの警告を出さない');

  const bad = (raw, why, nos = [0, 1, 2]) => {
    const r = sh.validateShootJudgementV2(raw, { imageNos: nos });
    ok(!r.ok && r.errors.length > 0, `🚨 通さない: ${why} (${r.ok ? '通ってしまった' : r.errors[0]})`);
  };
  const withCut = (patch, i = 0) => { const g = goodV2(); g.cuts[i] = { ...g.cuts[i], ...patch }; return g; };
  const without = (k) => { const g = goodV2(); delete g[k]; return g; };
  const withoutCut = (k) => { const g = goodV2(); delete g.cuts[0][k]; return g; };
  bad(null, 'null');
  bad({ ...goodV2(), shooter: '社内撮影' }, '知らないキー (撮影担当は撮影判定から決まるので受けない)');
  bad({ ...goodV2(), reason: '理由' }, '知らないキー (v2 の判定の結論は conclusion)');
  for (const k of ['open_required', 'send_targets', 'purpose', 'finish', 'usage', 'cuts', 'images', 'conclusion']) bad(without(k), `${k} が無い (欠けを補わない)`);
  bad({ ...goodV2(), recommended: '社内撮影' }, '撮影判定が日本語 (none / inhouse / photographer だけ)');
  bad({ ...goodV2(), open_required: 'いいえ' }, '開封要否が列挙値でない');
  bad({ ...goodV2(), open_required: '不要 ' }, '開封要否の後ろに空白');
  bad({ ...noneV2(), open_required: '必要' }, '🚨 追加撮影不要なのに開封が要る (食い違い・Codex PR-C2 名指し3)');
  bad({ ...goodV2(), open_required: '必要' }, '🚨 カットは全部 開封不要なのに全体が 必要 (食い違い)');
  {
    const mixed = { ...goodV2(), cuts: [cutOf(1, { open_required: '必要' }), cutOf(2, { lp_image_nos: [] })] };
    bad({ ...mixed, open_required: '必要' }, '🚨 カットの開封が混ざるのに全体が 必要 (一部必要 のはず)');
    ok(sh.validateShootJudgementV2({ ...mixed, open_required: '一部必要' }, { imageNos: [0, 1, 2] }).ok, 'カットの開封が混ざれば全体は 一部必要');
    ok(sh.validateShootJudgementV2({ ...mixed, cuts: mixed.cuts.map((c) => ({ ...c, open_required: '必要' })), open_required: '必要' }, { imageNos: [0, 1, 2] }).ok, '全部 必要 なら全体も 必要');
  }
  bad({ ...goodV2(), conclusion: '' }, '判定の結論が空');
  bad({ ...goodV2(), conclusion: 'あ'.repeat(sh.SHOOT_V2_CONCLUSION_MAX + 1) }, '判定の結論が長すぎる');
  bad({ ...goodV2(), send_targets: '' }, '🚨 撮影するのに撮影用送付対象が空');
  bad({ ...goodV2(), purpose: ' 目的' }, '撮影目的の前に空白 (trim して受けない)');
  bad({ ...goodV2(), finish: 'a' + String.fromCharCode(1) }, '完成イメージに制御文字');
  bad({ ...goodV2(), send_targets: '商品A\u0085商品B' }, '🚨 C1 制御文字 (U+0085 は改行に見える・Codex PR-C2 名指し4 L)');
  bad({ ...goodV2(), conclusion: '確認\u202Eです' }, '🚨 表示の向きを変える文字 (U+202E)');
  bad(withCut({ target: 'ブラック\u2066(1本)' }), '🚨 カットにも表示の向きを変える文字 (U+2066)');
  bad({ ...noneV2(), send_targets: 'ブラック (1本)' }, '🚨 追加撮影不要なのに撮影用送付対象がある (食い違い)');
  bad({ ...noneV2(), cuts: [cutOf(1, { lp_image_nos: [] })] }, '🚨 追加撮影不要なのに撮影カットがある (食い違い)');
  bad({ ...noneV2(), images: [{ no: 0, needs_shoot: false }, { no: 1, needs_shoot: false }, { no: 2, needs_shoot: true }] }, '🚨 追加撮影不要なのに撮影が要る画像がある');
  bad({ ...goodV2(), cuts: [] }, '🚨 社内撮影なのに撮影カットが無い');
  bad({ ...goodV2(), cuts: {} }, 'cuts が配列でない');
  bad({ ...goodV2(), cuts: Array.from({ length: sh.SHOOT_V2_MAX_CUTS + 1 }, (_, i) => cutOf(i + 1, { lp_image_nos: i === 0 ? [2] : [] })) }, 'カットが多すぎる');
  bad(withCut({ no: 2 }), 'カットの番号が 1 からの連番でない');
  bad(withCut({ no: '1' }), 'カットの番号が文字列');
  for (const [k] of sh.SHOOT_V2_CUT_FIELDS.slice(1)) bad(withoutCut(k), `カットの ${k} が無い`);
  {
    const r = sh.validateShootJudgementV2(withoutCut('lp_image_nos'), { imageNos: [0, 1, 2] });
    ok(!r.ok && r.errors.some((e) => /lp_image_nos .*がありません/.test(e)), `🚨 欠けは「無い」と言う ([] で補わない) (${r.errors?.[0]})`);
  }
  bad(withCut({ priority: '任意' }), '優先度が列挙値でない');
  bad(withCut({ expression_type: '物撮り+使用イメージ' }), '撮影表現タイプが仕様書の表記と違う (半角 +)');
  bad(withCut({ variation: '全色' }), '撮影対象バリエーションが列挙値でない');
  bad(withCut({ open_required: '一部必要' }), '🚨 カットの開封要否は 不要 / 必要 だけ (一部必要 は商品全体の値)');
  bad(withCut({ target: '' }), '撮影対象が空');
  bad(withCut({ content: '' }), '撮影内容が空');
  bad(withCut({ finish: 'x'.repeat(sh.SHOOT_FIELD_MAX + 1) }), '構図・完成イメージが長すぎる');
  bad(withCut({ required_notice: 3 }), '必ず出す表示が文字列でない');
  bad(withCut({ extra: 'x' }), 'カットに知らないキー');
  bad(withCut({ lp_image_nos: 2 }), 'lp_image_nos が配列でない');
  bad(withCut({ lp_image_nos: ['2'] }), 'lp_image_nos に文字列');
  bad(withCut({ lp_image_nos: [2, 2] }), 'lp_image_nos に重複');
  bad(withCut({ lp_image_nos: [2, 1] }), '🚨 カットを使う画像 (1枚目) が needs_shoot: false (食い違い)');
  bad(withCut({ lp_image_nos: [2, 7] }), 'カットを使う画像が images に無い');
  bad(withCut({ lp_image_nos: [] }), '🚨 needs_shoot: true の 2枚目 を使うカットが無い (撮影指示書から抜ける・Codex PR-C2 名指し1)');
  bad({ ...goodV2(), images: goodV2().images.slice(0, 2).concat([{ no: 2, needs_shoot: true, cut: 'x' }]) }, '🚨 images に撮影の中身を書いた (v2 は cuts に書く)');
  bad({ ...goodV2(), images: goodV2().images.slice(1) }, '🚨 構成の画像が足りない (0枚目 の判定が無い)');
  bad({ ...goodV2(), images: [...goodV2().images, { no: 5, needs_shoot: false }] }, '🚨 構成に無い画像の判定がある');
  bad({ ...goodV2(), conclusion: 'x'.repeat(sh.SHOOT_V2_RAW_MAX) }, '大きすぎる');
  const circ = goodV2(); circ.self = circ;
  let threw = false;
  try { bad(circ, '循環参照'); } catch { threw = true; }
  ok(!threw, '🚨 JSON にできない値でも例外を出さない');

  // 依頼の形と違う形は受けない (packet が決める)
  const asV1 = sh.validateShootForComposition(goodV1(), OUT, { format: 2 });
  ok(!asV1.ok && /新商品初動判定/.test(asV1.errors[0]), `🚨 仕様書を渡した依頼に v1 の形が来たら受けない (${asV1.errors?.[0]})`);
  const asV2 = sh.validateShootForComposition(goodV2(), OUT, { format: 1 });
  ok(!asV2.ok, '🚨 仕様書を渡していない依頼に v2 の形が来たら受けない');
  ok(sh.validateShootForComposition(goodV2(), OUT, { format: 2 }).ok && sh.validateShootForComposition(goodV1(), OUT, { format: 1 }).ok, 'それぞれ合う形なら通る');
  ok(sh.validateShootForComposition(goodV2(), OUT).ok && sh.validateShootForComposition(goodV1(), OUT).ok, "'auto' はどちらも見分けて通す");
  eq(sh.shootFormatOfPacket({ shoot_spec: { id: 1 } }), 2, 'packet に shoot_spec があれば 2');
  eq(sh.shootFormatOfPacket({ shoot_spec: null }), 1, '無ければ 1');

  // 指示文: 検査するキーと値を全部書いている (指示と検査がずれない) / 決まりは書き写していない
  const ins = sh.SHOOT_SPEC_INSTRUCTION;
  const allKeys = [...sh.SHOOT_V2_KEYS, ...sh.SHOOT_V2_CUT_FIELDS.map(([k]) => k), 'needs_shoot'];
  ok(allKeys.every((k) => ins.includes(k)), '指示文は検査するキーを全部書いている');
  const allValues = [...sh.SHOOT_OPEN_VALUES, ...sh.SHOOT_PRIORITY_VALUES, ...sh.SHOOT_EXPRESSION_VALUES, ...sh.SHOOT_VARIATION_VALUES, ...sh.SHOOT_RECOMMENDATIONS];
  ok(allValues.every((x) => ins.includes(`"${x}"`)), '指示文は列挙値を全部書いている');
  ok(ins.includes('shoot-spec-<ID>.md') && ins.includes('仕様書が正本') && ins.includes('offset'), '指示文: 仕様書のファイルを最後まで読む・仕様書が正本');
  ok(!/5 ?カット|最低5|革製品|消耗品/.test(ins), '🚨 指示文に仕様書の判定の決まり (5 カット・革製品・消耗品 …) を書き写していない');
  ok(ins.includes('⑦の本文には何も足さない'), '🚨 指示文が「⑦の本文に足さない」と言っている');
  ok(!pt.PRODUCT_ANALYSIS_INSTRUCTION.includes('新商品初動判定'), '🚨 構成の指示文 (スタッフと共有の正本) は変わらない');
  // 🚨 項目名は撮影指示書 (PR-D) の CUT_FIELDS / SUMMARY_FIELDS と同じ (違うのは lp_image_nos ↔ lp_image だけ)
  const v2CutKeys = sh.SHOOT_V2_CUT_FIELDS.map(([k]) => k).filter((k) => k !== 'no');
  eq(v2CutKeys.filter((k) => !sheet.CUT_FIELDS.includes(k)), ['lp_image_nos'], 'カットの項目は撮影指示書の CUT_FIELDS にある (lp_image_nos だけは番号の配列)');
  eq(sheet.CUT_FIELDS.filter((k) => !v2CutKeys.includes(k)), ['lp_image'], '撮影指示書の CUT_FIELDS は AI が全部埋める (lp_image は表示なので撮影指示書が作る)');
  const v2SumKeys = ['open_required', ...sh.SHOOT_V2_SUMMARY_FIELDS.map(([k]) => k), 'conclusion'];
  eq(sheet.SUMMARY_FIELDS.filter((k) => !v2SumKeys.includes(k)), ['judgement', 'shooter'], '概要は撮影判定・撮影担当 (撮影判定の箱の値) のほかを AI が埋める');
}

console.log('④ 読み口の形 (shootReadModel) — v1 を v2 に寄せる / 撮影指示書の形の summary・cuts');
{
  const m1 = sh.shootReadModel(sh.validateShootJudgement(goodV1(), { imageNos: [0, 1, 2] }).value);
  eq([m1.format, m1.recommended, m1.reason, m1.cut_count], [1, 'inhouse', '2枚目 の写真がありません。', 1], 'v1: 撮影判定と理由');
  eq(m1.summary, { judgement: '② 社内撮影', shooter: '社内撮影', open_required: '', send_targets: '', purpose: '', finish: '', usage: '', conclusion: '2枚目 の写真がありません。' },
    'v1: 概要 (撮影指示書の SUMMARY_FIELDS の形)。v1 に無い項目は空・撮影担当は撮影判定から');
  eq(m1.cuts[0], { no: 1, priority: '', expression_type: '', variation: '', target: '', content: '玄関の手元', purpose: '', finish: '斜め上から', usage: '', open_required: '', reference_theme: '', lp_image_nos: [2], notice: '顔を写さない', required_notice: '' },
    'v1: 撮影が要る画像を 1 カットずつにする (撮影内容 = cut・完成イメージ = composition・注意 = ng)');
  eq(m1.images[2], goodV1().images[2], '🚨 v1: images は保存したまま (props・background・tone・ng も残す)');
  eq(m1.warnings, [], 'v1 には警告を出さない');
  const m2 = sh.shootReadModel(sh.validateShootJudgementV2(goodV2(), { imageNos: [0, 1, 2] }).value);
  eq([m2.format, m2.reason, m2.cut_count], [2, goodV2().conclusion, 2], 'v2: 理由 = 判定の結論');
  eq(m2.summary, { judgement: '② 社内撮影', shooter: '社内撮影', open_required: '不要', send_targets: 'ハッカ油スプレー 100ml (1本)', purpose: '使用シーンの実写を揃える',
    finish: '玄関で使っている手元が分かる明るい写真', usage: '楽天 LP・サムネイル', conclusion: goodV2().conclusion }, 'v2: 概要');
  eq(m2.cuts, goodV2().cuts, 'v2: カットは送られたまま');
  eq(m2.images.map((im) => Object.keys(im)), [0, 1, 2].map(() => ['no', 'needs_shoot', 'cut', 'composition', 'props', 'background', 'tone', 'ng']), 'v2 でも images は PR-C の 8 項目の形');
  eq([m2.images[2].cut, m2.images[2].composition, m2.images[0].cut], ['玄関でスプレーする手元', '斜め上から手元と商品。ラベルが読めること', ''], 'v2: images にはその画像を使うカットの 撮影内容・完成イメージ');
  const mNone = sh.shootReadModel(sh.validateShootJudgementV2(noneV2(), { imageNos: [0, 1, 2] }).value);
  eq([mNone.summary.judgement, mNone.summary.shooter, mNone.cut_count], ['① 追加撮影不要', 'なし', 0], '追加撮影不要: 撮影担当「なし」・カット 0');
}

// ── 依頼を進める道具 ──
let seq = 0;
const spec = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '## 出力形式\n'.repeat(5), sheetTitles: ['出力形式'], actor: 't@x' }).spec;
const mkDraft = (name = NAME) => {
  seq++;
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'test')`).run(`LP-SS-${seq}`, name).lastInsertRowid);
  db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`).run(id, '天然ハッカ油 100ml');
  db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, 0, ?, '2026-09-30T00:00:00.000Z')`).run(id, `FILEIDSS${String(seq).padStart(4, '0')}`);
  return db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
};
const fileOf = (d) => db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').get(d.id).drive_file_id;
const request = (d, key = `ss-key-${seq}-${Math.random().toString(36).slice(2, 8)}`) => lp.requestJob(db, {
  draft: d, spec, productInfo: '天然ハッカ油 100ml', colorVariations: '', images: [{ file_id: fileOf(d), modified_time: '2026-09-30T00:00:00.000Z' }],
  idempotencyKey: key, actor: 't@x',
});
function reserveFor(d, { shootSpec = true } = {}) {
  const req = request(d);
  if (!req.ok) throw new Error('requestJob: ' + req.error);
  const run = `run-ss-${req.job.id}`;
  const cl = lp.claimJob(db, { runnerRunId: run, maxImages: 16, shootSpec });
  if (cl.job?.job_id !== req.job.id) throw new Error('claim が別の依頼を掴んだ: ' + JSON.stringify(cl).slice(0, 300));
  lp.recordImageServed(db, req.job.id, { leaseToken: cl.job.lease_token, fileId: fileOf(d), sha256: 'a'.repeat(64), bytes: 10 });
  const rv = lp.reserveGeneration(db, req.job.id, { leaseToken: cl.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION });
  if (!rv.ok) throw new Error('reserve: ' + rv.error);
  return { jobId: req.job.id, gid: rv.generation_id, packetHash: cl.job.packet_hash, run, leaseToken: cl.job.lease_token, job: cl.job };
}
const accept = (r, extra = {}) => lp.submitResult(db, r.gid, { packetHash: r.packetHash, verdict: 'accepted', output: OUT, lint: { ok: true }, reviewRounds: 1, ...extra });
const matchModel = (run) => lp.recordModelCheck(db, { runnerRunId: run, actualModels: ['claude-opus-5-5'] });
const jobRow = (id) => db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(id);
const cancel = (id) => db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'cancelled' WHERE id = ?`).run(id);

console.log('② 仕様書を取り込む前 — PR-C の簡単な決まり (v1) のまま動く');
let dV1, rV1;
{
  dV1 = mkDraft();
  rV1 = reserveFor(dV1, { shootSpec: false });
  eq([rV1.job.packet.packet_version, rV1.job.packet.shoot_spec, rV1.job.shoot_spec], [6, null, null], 'packet の版は 6・shoot_spec は null・claim にも仕様書は付かない');
  eq(rV1.job.packet.shoot_instruction, sh.SHOOT_JUDGE_INSTRUCTION, '🚨 指示は PR-C の簡単な決まり (取り込む前に壊れない)');
  const res = accept(rV1, { shoot: goodV1() });
  ok(res.ok && res.status === 'done' && res.shoot.status === 'saved', '古い実行役 (仕様書を知らない) でも v1 の判定を今どおり保存');
  matchModel(rV1.run);
  const j = lp.latestShootJudgement(db, dV1.id);
  ok(j.available && j.format === 1 && j.cut_count === 1 && j.cuts[0].lp_image_nos[0] === 2 && j.images[2].ng === '顔を写さない', 'v1 で保存した行も読み口は v2 の形に寄せる (images は元のまま)');
}

console.log('① 仕様書「新商品初動判定」を取り込む (追記専用・種類ごとの最新版)');
const JUDGE_BODY = '## 使い方\n新商品初動判定｜社内共有版\n\n## システム本文\n' + '判定ロジック。'.repeat(3000);
const judge = lp.importSpec(db, { kind: 'initial_judge', title: '新商品初動判定', body: JUDGE_BODY, sheetTitles: ['使い方', 'システム本文'], actor: 'admin@x' });
{
  ok(judge.ok && judge.created && judge.spec.kind === 'initial_judge', '取り込める (kind = initial_judge)');
  ok(lp.importSpec(db, { kind: 'initial_judge', title: '新商品初動判定', body: JUDGE_BODY, sheetTitles: ['使い方', 'システム本文'], actor: 'admin@x' }).created === false, '同じ中身なら版は増えない');
  eq(lp.latestSpec(db, 'product_analysis').id, spec.id, '🚨 LP制作システムの最新版は変わらない (種類ごとに別)');
  eq(lp.latestSpec(db, 'initial_judge').id, judge.spec.id, '新商品初動判定の最新版');
  eq(lp.importSpec(db, { kind: 'simple_lp', title: 't', body: 'b', actor: 'u' }).code, 'bad_kind', '知らない種類は断る');
  const asProduct = lp.requestJob(db, { draft: mkDraft(), spec: judge.spec, productInfo: 'x', colorVariations: '', images: [{ file_id: 'FILEIDSSX001' }], idempotencyKey: 'ss-key-asproduct', actor: 't' });
  eq(asProduct.code, 'bad_request', '🚨 新商品初動判定を LP制作システムの仕様書として依頼に使えない');
}

console.log('② 受付で仕様書を固め、claim で hash を照らして渡す');
let dV2, rV2;
{
  dV2 = mkDraft();
  const req = request(dV2, 'ss-key-v2-claim');
  const pk = JSON.parse(req.job.packet_json);
  eq(pk.shoot_spec, { id: judge.spec.id, kind: 'initial_judge', hash: judge.spec.hash }, '🚨 packet に仕様書の id・hash を固める (本文は複製しない)');
  eq(pk.shoot_instruction, sh.SHOOT_SPEC_INSTRUCTION, 'packet の指示は「仕様書で判定する」の方');
  eq(pk.instruction, pt.PRODUCT_ANALYSIS_INSTRUCTION, '🚨 構成の指示文は正本のまま (段階1 の測定を壊さない)');
  ok(JSON.stringify(pk).length < 40_000, `packet は仕様書の本文を持たないので小さいまま (${JSON.stringify(pk).length} 文字)`);
  // 古い phlp (shoot_spec と言わない) には掴ませない
  const old = lp.claimJob(db, { runnerRunId: 'run-old-phlp', maxImages: 16 });
  ok(old.ok && old.job === null && old.needs_shoot_spec === 1 && /install\.ps1/.test(old.error), `🚨 仕様書を落とせない古い phlp には掴ませず、理由を返す (${old.error})`);
  eq(jobRow(req.job.id).status, 'queued', '依頼は queued のまま (新しい phlp が拾う)');
  // 新しい版を取り込んでも、受付済みの依頼は受付時の版
  const judge2 = lp.importSpec(db, { kind: 'initial_judge', title: '新商品初動判定', body: JUDGE_BODY + '\n(Ver1.3.12)', sheetTitles: ['使い方', 'システム本文'], actor: 'admin@x' });
  ok(judge2.created, '前提: 新しい版を取り込んだ');
  const cl = lp.claimJob(db, { runnerRunId: 'run-ss-v2', maxImages: 16, shootSpec: true });
  eq(cl.job?.job_id, req.job.id, '新しい phlp は掴む');
  eq(cl.job.shoot_spec.id, judge.spec.id, '🚨 渡すのは受付時の版 (後から取り込んだ版ではない)');
  ok(cl.job.shoot_spec.body === JUDGE_BODY && cl.job.shoot_spec.hash === judge.spec.hash && cl.job.shoot_spec.kind === 'initial_judge', '本文の全文と hash が届く');
  eq(cl.job.spec.kind, 'product_analysis', 'LP制作システムの仕様書は今どおり spec で届く');
  lp.recordImageServed(db, req.job.id, { leaseToken: cl.job.lease_token, fileId: fileOf(dV2), sha256: 'a'.repeat(64), bytes: 10 });
  const rv = lp.reserveGeneration(db, req.job.id, { leaseToken: cl.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION });
  rV2 = { jobId: req.job.id, gid: rv.generation_id, packetHash: cl.job.packet_hash, run: 'run-ss-v2', leaseToken: cl.job.lease_token, job: cl.job };
  // 以降の依頼は新しい版 (judge2) を固める
  const dN = mkDraft();
  const reqN = request(dN, 'ss-key-v2-newer');
  eq(JSON.parse(reqN.job.packet_json).shoot_spec.id, judge2.spec.id, '新しい依頼は最新の版を固める');
  cancel(reqN.job.id);
}

console.log('② 渡す前の照合 — 受付時の仕様書と違えば渡さない (spec_changed)');
{
  const tamper = (mutate, why, { alsoJobRow = true, code = 'spec_changed' } = {}) => {
    const d = mkDraft();
    const req = request(d, `ss-key-tamper-${seq}`);
    const p = JSON.parse(req.job.packet_json);
    mutate(p);
    // packet の hash も作り直す (packet だけ見ていては気づけない書き換え)。alsoJobRow = job の列 (shoot_spec_id / hash) も合わせて書き換える
    db.prepare('UPDATE ph_lp_compose_jobs SET packet_json = ?, packet_hash = ? WHERE id = ?').run(JSON.stringify(p), lp.sha256(lp.canonicalJson(p)), req.job.id);
    if (alsoJobRow) db.prepare('UPDATE ph_lp_compose_jobs SET shoot_spec_id = ?, shoot_spec_hash = ? WHERE id = ?').run(p.shoot_spec.id, p.shoot_spec.hash, req.job.id);
    const cl = lp.claimJob(db, { runnerRunId: 'run-tamper', maxImages: 16, shootSpec: true });
    ok(cl.job === null && jobRow(req.job.id).error_code === code, `🚨 ${why} → 渡さずに ${code} (${jobRow(req.job.id).error_code})`);
  };
  eq([jobRow(rV2.jobId).shoot_spec_id, jobRow(rV2.jobId).shoot_spec_hash], [judge.spec.id, judge.spec.hash], '受付時の仕様書の版は job の列にも残す (LP制作システムの spec_id / spec_hash と同じ)');
  {
    const newest = lp.latestSpec(db, 'initial_judge');
    ok(newest.id !== judge.spec.id, '前提: 別の有効な版がある');
    tamper((p) => { p.shoot_spec.id = judge.spec.id; p.shoot_spec.hash = judge.spec.hash; }, '🚨 packet だけを別の有効な版に差し替えた (job の列と合わない・Codex PR-C2 名指し5)', { alsoJobRow: false, code: 'packet_tampered' });
  }
  tamper((p) => { p.shoot_spec.hash = 'f'.repeat(64); }, '仕様書の hash が版と違う');
  tamper((p) => { p.shoot_spec.id = spec.id; p.shoot_spec.hash = spec.hash; }, '仕様書の id が LP制作システムの版を指す (種類が違う)');
  tamper((p) => { p.shoot_spec.id = 99999; }, '仕様書の版が無い');
}

console.log('③ 結果: 仕様書の形 (v2) で保存 → 読み口・画面の状態');
{
  const lintGood = lp.lintForJob(db, rV2.jobId, { leaseToken: rV2.leaseToken, output: OUT, shoot: goodV2() });
  eq(lintGood.shoot, { ok: true, errors: [], warnings: [] }, '出す前の検査 (lint --shoot) は v2 の形で通る');
  const lintV1 = lp.lintForJob(db, rV2.jobId, { leaseToken: rV2.leaseToken, output: OUT, shoot: goodV1() });
  ok(lintV1.shoot.ok === false && /新商品初動判定/.test(lintV1.shoot.errors[0]), '🚨 仕様書を渡した依頼に v1 の形は通らない (lint でも分かる)');
  const res = accept(rV2, { shoot: goodV2() });
  ok(res.ok && res.status === 'done' && res.shoot.status === 'saved', `v2 の撮影判定を保存 (${JSON.stringify(res.shoot)})`);
  ok(accept(rV2, { shoot: goodV2() }).already === true, '同じ結果の再送は保存済みを返す');
  eq(jobRow(rV2.jobId).output_text, OUT, '🚨 ⑦の本文は送られたまま');
  matchModel(rV2.run);
  const j = lp.latestShootJudgement(db, dV2.id);
  ok(j.available && j.format === 2 && j.recommended === 'inhouse' && j.summary.shooter === '社内撮影' && j.summary.open_required === '不要' && j.cut_count === 2, '読み口は v2 の概要');
  eq(j.cuts.map((c) => [c.no, c.priority, c.lp_image_nos]), [[1, '必須', [2]], [2, '推奨', []]], 'カット (撮影指示書の材料)');
  eq(j.images[2].cut, '玄関でスプレーする手元', 'images (PR-D の cutsFromSlots が読む形) にも 2枚目のカット');
  const st = lp.jobStateFor(db, dV2.id);
  eq(st.shoot, { job_id: rV2.jobId, available: true, format: 2, recommended: 'inhouse', reason: goodV2().conclusion, open_required: '不要', cut_count: 2, send_targets: 'ハッカ油スプレー 100ml (1本)', warnings: [], missing: null },
    '画面の状態には 開封要否・カット数・撮影用送付対象 を足す (カットの中身は出さない)');
}

console.log('③ 結果: 依頼と違う形・壊れた形でも構成は受け付ける');
{
  const d = mkDraft();
  const r = reserveFor(d);
  const res = accept(r, { shoot: goodV1() });
  ok(res.ok && res.status === 'done' && res.shoot.status === 'invalid' && /新商品初動判定/.test(res.shoot.error), `🚨 v1 を送っても構成は done・撮影判定だけ invalid (${res.shoot.error})`);
  matchModel(r.run);
  eq(lp.latestShootJudgement(db, d.id).missing, 'invalid', '読み口は「AI の判定なし (形が違った)」');
  const d2 = mkDraft();
  const r2 = reserveFor(d2);
  const res2 = accept(r2, { shoot: { ...goodV2(), cuts: [] } });
  ok(res2.ok && res2.status === 'done' && res2.shoot.status === 'invalid', '撮影するのにカットが無い → 構成は done・撮影判定だけ invalid');
  const d3 = mkDraft();
  const r3 = reserveFor(d3);
  ok(accept(r3).shoot.status === 'not_sent' && jobRow(r3.jobId).status === 'done', '撮影判定を送らなくても今どおり done');
}

console.log('③ カメラマン撮影 7 カット → 保存して、警告を画面と lint に出す (強制しない)');
let dPhoto;
{
  dPhoto = mkDraft();
  const r = reserveFor(dPhoto);
  const photo7 = { ...goodV2(), recommended: 'photographer', send_targets: 'キャメル／ブラック／レッド（3色・各1本）', cuts: [1, 2, 3, 4, 5, 6, 7].map((n) => cutOf(n, { lp_image_nos: n === 1 ? [2] : [] })) };
  const ln = lp.lintForJob(db, r.jobId, { leaseToken: r.leaseToken, output: OUT, shoot: photo7 });
  ok(ln.shoot.ok === true && ln.shoot.warnings.length === 1 && /5 カット単位/.test(ln.shoot.warnings[0]), `lint は通り、警告を返す (${ln.shoot.warnings})`);
  ok(accept(r, { shoot: photo7 }).shoot.status === 'saved', '保存する (仕様書の運用ルールはサーバで強制しない)');
  matchModel(r.run);
  const st = lp.jobStateFor(db, dPhoto.id).shoot;
  ok(st.recommended === 'photographer' && st.cut_count === 7 && st.warnings.length === 1, '画面の状態に警告が乗る');
  // 🚨 読み口も依頼の形で見る: 仕様書の依頼の行を DB で v1 の形に替えても出さない
  const saved = jobRow(r.jobId).shoot_json;
  db.prepare('UPDATE ph_lp_compose_jobs SET shoot_json = ? WHERE id = ?').run(JSON.stringify(goodV1()), r.jobId);
  eq(lp.latestShootJudgement(db, dPhoto.id).missing, 'invalid', '🚨 仕様書の依頼に v1 の形が保存されていたら「判定なし」(読むときも依頼の形で見る)');
  db.prepare('UPDATE ph_lp_compose_jobs SET shoot_json = ? WHERE id = ?').run(saved, r.jobId);
}

console.log('⑧ 撮影指示書 (PR-D) — 仕様書の形 (v2) なら AI のカットと概要をそのまま渡す');
{
  // 純粋関数 (lib/shoot-sheet.js の cutsFromSlots)。D の試験と同じ作りの並び
  const blk = (mat) => ['## 画像の役割', '特長', '## 商品配置', 'ブロックの構図', '## 使用素材', mat, '## NG事項', 'ブロックのNG'].join('\n').split('\n');
  const slots = [
    { uid: 'a0', no: 0, name: 'サムネイル', lines: blk('提供された実物商品画像') },
    { uid: 'a1', no: 1, name: 'FV', lines: blk('提供された実物商品画像') },
    { uid: 'a2', no: 2, name: '成分', lines: blk('撮影: 手元') },
  ];
  const v = sh.validateShootJudgementV2({
    ...goodV2(),
    images: [{ no: 0, needs_shoot: false }, { no: 1, needs_shoot: true }, { no: 2, needs_shoot: true }],
    cuts: [
      cutOf(1, { content: 'LPに無い 手持ちサイズ', lp_image_nos: [] }),
      cutOf(2, { content: '2枚目の手元', lp_image_nos: [2] }),
      cutOf(3, { content: 'FVと2枚目で使う集合', lp_image_nos: [1, 2], required_notice: '実物への貼付不可' }),
    ],
  }, { imageNos: [0, 1, 2] });
  ok(v.ok, `前提: v2 の判定 ${v.ok ? '' : v.errors[0]}`);
  const J = sh.shootReadModel(v.value);
  const c = sheet.cutsFromSlots({ slots, hasEditShoot: false, aiImages: J.images, aiCuts: J.cuts });
  eq(c.map((x) => [x.content, x.lp_image]), [['FVと2枚目で使う集合', '1枚目｜FV・2枚目｜成分'], ['2枚目の手元', '2枚目｜成分'], ['LPに無い 手持ちサイズ', '']],
    '🚨 画像の並びで AI のカットを並べ、複数の画像で使うカットは 1 回だけ・LP に無いカットは最後に (落とさない)');
  const full = c[0];
  ok(full.priority === '必須' && full.expression_type === '使用イメージ' && full.variation === '代表1色' && full.target === 'ハッカ油スプレー 100ml (1本)'
    && full.purpose === '使う場面を伝える' && full.usage === '楽天 LP 2枚目' && full.open_required === '不要' && full.reference_theme === '玄関で使う手元' && full.notice === '顔を写さない',
  `仕様書の項目 (優先度・表現タイプ・バリエーション・撮影対象・目的・用途・開封・参考テーマ・注意) をそのまま渡す (${JSON.stringify(full).slice(0, 160)})`);
  ok(full.finish.includes('実物への貼付不可'), '必ず出す表示は撮影指示書の決まりどおり完成イメージに足される (D の normalizeCuts)');
  // 編集版: 並べ替え (a2 を 1枚目へ)・人が FV (a1) を「撮影不要」に・足した画像 (nNew) を要撮影に
  const edited = [
    { ...slots[0], shoot: false }, { ...slots[2], no: 1, shoot: true }, { ...slots[1], no: 2, shoot: false },
    { uid: 'nNew', no: 3, name: '使い方', lines: blk('提供された実物商品画像'), shoot: true },
  ];
  const ce = sheet.cutsFromSlots({ slots: edited, hasEditShoot: true, aiImages: J.images, aiCuts: J.cuts });
  eq(ce.map((x) => [x.content, x.lp_image]), [['2枚目の手元', '1枚目｜成分'], ['FVと2枚目で使う集合', '1枚目｜成分'], ['使い方', '3枚目｜使い方'], ['LPに無い 手持ちサイズ', '']],
    '🚨 要撮影は人の値 (編集版) が正本・元の画像 (uid a<番号>) で AI のカットを引く・足した画像はブロックから・表示は今の番号 (撮影不要にした FV は出さない)');
  const bothOn = sheet.cutsFromSlots({ slots: edited.map((x) => (x.uid === 'a1' ? { ...x, shoot: true } : x)), hasEditShoot: true, aiImages: J.images, aiCuts: J.cuts });
  ok(bothOn.some((x) => x.lp_image === '2枚目｜FV・1枚目｜成分'), '共有カットの両方の画像が要撮影なら両方を出す');
  const hashOf = (cs) => sheet.shootSheetMaterialHash({ productCode: 'X', productName: 'Y', shootMode: 'inhouse', folderUrl: '', summary: {}, cuts: cs });
  ok(hashOf(bothOn) !== hashOf(ce), '🚨 共有カットの片方の画像だけ撮影不要にしても材料の hash が変わる (「LP構成が変わりました」が出る・Codex PR-C2 名指し2 M)');
  const ceOff = sheet.cutsFromSlots({ slots: edited.map((x) => ({ ...x, shoot: x.uid === 'nNew' })), hasEditShoot: true, aiImages: J.images, aiCuts: J.cuts });
  eq(ceOff.map((x) => x.content), ['使い方', 'LPに無い 手持ちサイズ'], '人が「撮影不要」にした画像だけで使うカットは載せない (LP に無いカットは残す)');
  // v1 の判定なら今までどおり (D の対応づけ)
  const J1 = sh.shootReadModel(sh.validateShootJudgement(goodV1(), { imageNos: [0, 1, 2] }).value);
  const c1 = sheet.cutsFromSlots({ slots, hasEditShoot: false, aiImages: J1.images, aiCuts: null });
  ok(c1.length === 1 && c1[0].content === '玄関の手元' && c1[0].finish.startsWith('斜め上から') && c1[0].target === '', 'v1 の判定は今までどおり images から (撮影対象などは空欄)');

  // 本番の経路: shootSheetCutsFor (service) が v2 の判定から カット + 概要 を返す
  const svcMod = await import('../apps/product-hub/services/shoot-sheet-service.js');
  const m = svcMod.shootSheetCutsFor(db, dV2);
  eq(m.cuts.map((x) => [x.content, x.lp_image ? x.lp_image.split('｜')[0] : '']), [['玄関でスプレーする手元', '2枚目'], ['ボトルを手に持ったサイズ感', '']],
    '🚨 shootSheetCutsFor: AI のカット (LP に無いカットも) を撮影指示書の材料にする');
  eq(m.summary, lp.latestShootJudgement(db, dV2.id).summary, 'shootSheetCutsFor: 概要も AI の判定 (撮影指示書の SUMMARY_FIELDS の形)');
  const built = sheet.buildShootSheet({ productCode: 'X1', productName: NAME, shootMode: 'inhouse', summary: { ...m.summary, shooter: '社内撮影' }, cuts: m.cuts });
  const flat = built.sheets[0].rows.map((r) => r.join('｜'));
  ok(flat.includes('撮影用送付対象｜ハッカ油スプレー 100ml (1本)') && flat.includes('撮影対象｜ハッカ油スプレー 100ml (1本)') && flat.includes('カット2（推奨）'),
    `シートに 撮影用送付対象・撮影対象・社内撮影の推奨 が出る (${flat.slice(0, 12).join(' / ')})`);
  const mV1 = svcMod.shootSheetCutsFor(db, dV1);
  eq(mV1.summary, {}, 'v1 の判定の商品は概要を渡さない (今までどおり)');

  // 🚨 人が編集版で要撮影を変えたら、AI の概要 (送付対象など) は使わない (違う商品を撮影先へ送らない・Codex PR-C2 名指し4 M)
  ok(sheet.aiSummaryStillValid({ slots: edited, hasEditShoot: false, aiImages: J.images }), '編集版が無ければ AI の概要を使う');
  ok(sheet.aiSummaryStillValid({ slots: slots.map((x) => ({ ...x, shoot: x.uid !== 'a0' })).reverse(), hasEditShoot: true, aiImages: J.images }), '並べ替えただけ (要撮影は AI と同じ) なら使う');
  ok(!sheet.aiSummaryStillValid({ slots: edited, hasEditShoot: true, aiImages: J.images }), '要撮影を外した・足した画像を要撮影にした なら使わない');
  ok(!sheet.aiSummaryStillValid({ slots: slots.map((x) => ({ ...x, shoot: true })), hasEditShoot: true, aiImages: J.images }), 'AI が要らないとした画像を要撮影にしたなら使わない');
  ok(!sheet.aiSummaryStillValid({ slots: [...slots.map((x) => ({ ...x, shoot: x.uid !== 'a0' })), { uid: 'nNew', no: 3, name: '使い方', lines: [], shoot: true }], hasEditShoot: true, aiImages: J.images }),
    '🚨 AI と同じ画像に加えて、足した画像を要撮影にしたなら使わない (AI の概要はその画像を知らない)');
  const le = await import('../apps/product-hub/lib/lp-edit.js');
  const dE = mkDraft();
  const rE = reserveFor(dE);
  accept(rE, { shoot: goodV2() });
  matchModel(rE.run);
  const stE = le.editStateFor(db, dE, { canEdit: true });
  eq(stE.slots.map((x) => x.shoot), [false, false, true], '🚨 編集版がまだ無い構成の要撮影の初めの値は AI の判定 (全部「撮影不要」で始めない・Codex PR-C2 名指し5 M)');
  {
    // 画面の値をそのまま保存 (文字だけ直した) しても、AI のカットと概要が撮影指示書に残る
    const asIs = stE.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title + '!', copy: x.copy, body: x.body, shoot: x.shoot }));
    const sv0 = le.saveEdit(db, { draft: dE, baseJobId: stE.base_job_id, baseEditId: stE.base_edit_id, slots: asIs, actor: 'staff@x' });
    const after0 = svcMod.shootSheetCutsFor(db, dE);
    ok(sv0.ok && after0.cuts.some((x) => x.content === '玄関でスプレーする手元') && after0.summary.send_targets === 'ハッカ油スプレー 100ml (1本)',
      `🚨 画面の要撮影をそのまま保存しても AI のカット・概要は落ちない (${sv0.error || ''})`);
  }
  const send = (fn) => stE.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: fn(x.uid) }));
  const st1E = le.editStateFor(db, dE, { canEdit: true });
  const sv1 = le.saveEdit(db, { draft: dE, baseJobId: st1E.base_job_id, baseEditId: st1E.base_edit_id, slots: send((u) => u === 'a2').map((x) => ({ ...x, copy: x.copy + '。' })), actor: 'staff@x' });
  const after1 = svcMod.shootSheetCutsFor(db, dE);
  ok(sv1.ok && after1.summary.send_targets === 'ハッカ油スプレー 100ml (1本)', `編集版で要撮影が AI と同じなら AI の概要を使う (${sv1.error || ''})`);
  const st2 = le.editStateFor(db, dE, { canEdit: true });
  const sv2 = le.saveEdit(db, { draft: dE, baseJobId: st2.base_job_id, baseEditId: st2.base_edit_id, slots: send((u) => u === 'a1'), actor: 'staff@x' });
  const after2 = svcMod.shootSheetCutsFor(db, dE);
  ok(sv2.ok && /要確認/.test(after2.summary.send_targets) && !after2.summary.open_required && !after2.summary.conclusion,
    `🚨 人が要撮影を 2枚目 → 1枚目 に変えたら、AI の送付対象・開封要否・結論は使わず「要確認」 (${JSON.stringify(after2.summary).slice(0, 120)})`);
}

console.log('② PR-C の版 (5) の依頼は claim でそのまま受ける (デプロイで待っている依頼を押し直しにしない)');
{
  const d = mkDraft();
  const req = request(d, 'ss-key-v5');
  const p = JSON.parse(req.job.packet_json);
  delete p.shoot_spec; p.packet_version = 5; p.shoot_instruction = sh.SHOOT_JUDGE_INSTRUCTION;
  // 版 5 の行 (PR-C で作った依頼) は shoot_spec_id / hash の列が NULL
  db.prepare('UPDATE ph_lp_compose_jobs SET packet_json = ?, packet_hash = ?, packet_version = 5, shoot_spec_id = NULL, shoot_spec_hash = NULL WHERE id = ?').run(JSON.stringify(p), lp.sha256(lp.canonicalJson(p)), req.job.id);
  const old = lp.claimJob(db, { runnerRunId: 'run-v5-old', maxImages: 16 });
  eq(old.job?.job_id, req.job.id, '🚨 版 5 の依頼は古い phlp でも掴める (仕様書が無い = PR-C と同じ)');
  ok(old.job.shoot_spec === null && old.job.packet.shoot_instruction === sh.SHOOT_JUDGE_INSTRUCTION, '撮影判定は PR-C の決まり (v1)');
  const fileId = fileOf(d);
  lp.recordImageServed(db, req.job.id, { leaseToken: old.job.lease_token, fileId, sha256: 'a'.repeat(64), bytes: 10 });
  const rv = lp.reserveGeneration(db, req.job.id, { leaseToken: old.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION });
  const res = lp.submitResult(db, rv.generation_id, { packetHash: old.job.packet_hash, verdict: 'accepted', output: OUT, lint: { ok: true }, reviewRounds: 1, shoot: goodV1() });
  ok(res.ok && res.status === 'done' && res.shoot.status === 'saved', '版 5 の依頼の v1 の撮影判定を保存');
  const d4 = mkDraft();
  const r4 = request(d4, 'ss-key-v4');
  const p4 = JSON.parse(r4.job.packet_json);
  delete p4.shoot_spec; delete p4.shoot_instruction; p4.packet_version = 4;
  db.prepare('UPDATE ph_lp_compose_jobs SET packet_json = ?, packet_hash = ?, packet_version = 4, shoot_spec_id = NULL, shoot_spec_hash = NULL WHERE id = ?').run(JSON.stringify(p4), lp.sha256(lp.canonicalJson(p4)), r4.job.id);
  const c4 = lp.claimJob(db, { runnerRunId: 'run-v4', maxImages: 16, shootSpec: true });
  ok(c4.job === null && jobRow(r4.job.id).error_code === 'packet_outdated', '版 4 以下は今どおり packet_outdated');
}

console.log('⑤ queue — 古い phlp には、掴めない仕様書つきの依頼を数えない (毎分 Claude を空で起動しない)');
{
  const d = mkDraft();
  const req = request(d, 'ss-key-queue');
  const qOld = lp.queueSummary(db);
  const qNew = lp.queueSummary(db, Date.now(), { shootSpec: true });
  ok(qOld.claimable === 0 && qOld.waiting_runner_update === 1, `🚨 古い phlp の問い合わせ: claimable 0・waiting_runner_update 1 (${JSON.stringify([qOld.claimable, qOld.waiting_runner_update])})`);
  ok(qNew.claimable === 1 && qNew.waiting_runner_update === 0, '新しい phlp (shoot_spec=1) には数える');
  // 🚨 壊れた packet が並んでいても queue は落ちない (json_extract の例外で後ろの依頼まで止めない・Codex PR-C2 名指し3 M)
  const dB = mkDraft();
  const rB = request(dB, 'ss-key-queue-broken');
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = '{' WHERE id = ?`).run(rB.job.id);
  let qb = null;
  try { qb = lp.queueSummary(db); } catch (e) { qb = { thrown: e.message }; }
  ok(qb && !qb.thrown && qb.claimable === 1 && qb.waiting_runner_update === 1, `🚨 壊れた packet があっても queue は返る (壊れた行は claim が落とすので数える) (${JSON.stringify(qb).slice(0, 120)})`);
  const clB = lp.claimJob(db, { runnerRunId: 'run-broken', maxImages: 16 });
  ok(clB.job === null && jobRow(rB.job.id).error_code === 'packet_tampered' && jobRow(req.job.id).status === 'queued', '壊れた依頼は claim が落とし、仕様書つきの依頼は古い phlp に掴ませない (今どおり)');
  cancel(req.job.id);
}

console.log('⑤ miniPC の設定 — Claude は仕様書のファイルを書き換えられない');
{
  const HERE = path.dirname(fileURLToPath(import.meta.url));
  const settings = JSON.parse(fs.readFileSync(path.join(HERE, 'ph-nightly', 'settings.json'), 'utf8'));
  const deny = settings.permissions.deny;
  ok(['Write', 'Edit'].every((t) => deny.includes(`${t}(//C:/tools/ph-nightly/work/shoot-spec-*.md)`) && deny.includes(`${t}(//C:/tools/ph-nightly/work/spec-*.md)`)),
    '🚨 spec-*.md・shoot-spec-*.md は Write / Edit を deny (サーバは形しか見ないので、決まりを書き換えて判定させない)');
  ok(settings.permissions.allow.includes('Read'), '読むのは今どおりできる');
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
const svc = (method, p, body) => call(method, '/service-api' + p, body, { Authorization: 'Bearer test-token-lp-shoot-spec' });

console.log('① 取り込みの口 (POST /api/lp-specs?kind=initial_judge・admin だけ)');
let realChars = 0;
{
  const realPath = 'G:/共有ドライブ/AI_reference/システム設計/LP仕様書_snapshot/新商品初動判定_20261009.xlsx';
  let xlsx;
  if (fs.existsSync(realPath)) {
    xlsx = fs.readFileSync(realPath);
    console.log('  (実物の .xlsx を使う)');
  } else {
    const ExcelJS = (await import('exceljs')).default;
    const wb = new ExcelJS.Workbook();
    for (const t of ['使い方', 'システム本文', '入力テンプレート', '出力形式', '出力例', '更新履歴']) wb.addWorksheet(t).addRow(['区分', `${t} の本文 (新商品初動判定)`]);
    xlsx = Buffer.from(await wb.xlsx.writeBuffer());
    console.log('  (共有ドライブが無いので代わりの .xlsx を作った)');
  }
  const up = (q, role) => fetch(`${base}/api/lp-specs${q}`, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: xlsx }).then(async (r) => ({ status: r.status, json: await r.json() }));
  // 画像登録者 (撮影判定を押す役割) でも、仕様書は差し替えられない
  const imgStaff = wf.createStaff({ name: '初動判定 画像登録者', kind: 'internal', portal_email: 'ss-img@b-faith.biz' });
  db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(imgStaff);
  session = { email: 'ss-img@b-faith.biz', role: 'user' };
  eq((await up('?kind=initial_judge')).status, 403, '🚨 admin でなければ取り込めない (画像登録者でも)');
  session = { email: 'nakahara@x', role: 'admin' };
  const bad = await up('?kind=__proto__');
  ok(bad.status === 400 && bad.json.code === 'bad_kind', '知らない種類は 400 (bad_kind)');
  const r = await up('?kind=initial_judge&title=' + encodeURIComponent('新商品初動判定_20261009'));
  ok(r.status === 200 && r.json.ok && r.json.spec.title === '新商品初動判定_20261009', `取り込める (${r.json.spec?.chars} 文字・タブ ${r.json.spec?.sheet_titles?.join('・')})`);
  realChars = r.json.spec.chars;
  ok(realChars > 0 && realChars < lp.SPEC_BODY_MAX, `本文は上限 (${lp.SPEC_BODY_MAX} 文字) に収まる`);
  const noTitle = await fetch(`${base}/api/lp-specs?kind=initial_judge`, { method: 'GET' }).then((x) => x.json());
  eq(noTitle.spec.id, r.json.spec.id, 'GET /api/lp-specs?kind=initial_judge で今の版');
  eq(lp.latestSpec(db, 'product_analysis').id, spec.id, '🚨 LP制作システムの版は動かない');
  // 一覧画面 (admin) のカード: 種類を選べて、新商品初動判定の今の版が出る
  const html = await (await fetch(`${base}/list`)).text();
  ok(html.includes('id="lpspec-kind"') && html.includes('value="initial_judge"'), '一覧の取り込みカードに種類の選択がある');
  ok(html.includes(`新商品初動判定 (撮影判定): <strong>#${r.json.spec.id}</strong>`), '一覧の取り込みカードに新商品初動判定の今の版が出る');
}

console.log('⑤ service-api の claim — 仕様書つきの依頼は shoot_spec: true と言う実行役にだけ');
{
  const d = mkDraft();
  const req = request(d, 'ss-key-http-claim');
  const oldR = await svc('POST', '/lp-compose/claim', { runner_run_id: 'run-http-old', max_images: 16 });
  ok(oldR.status === 200 && oldR.json.job === null && oldR.json.needs_shoot_spec === 1, '🚨 shoot_spec を言わない実行役には掴ませない (needs_shoot_spec)');
  const strR = await svc('POST', '/lp-compose/claim', { runner_run_id: 'run-http-str', max_images: 16, shoot_spec: 'true' });
  ok(strR.json.job === null, 'shoot_spec は true (真偽値) だけ ("true" は言っていないのと同じ)');
  const newR = await svc('POST', '/lp-compose/claim', { runner_run_id: 'run-http-new', max_images: 16, shoot_spec: true });
  ok(newR.json.job?.job_id === req.job.id && newR.json.job.shoot_spec?.body?.length === realChars, `新しい実行役は掴み、仕様書の全文が届く (${newR.json.job?.shoot_spec?.body?.length} 文字)`);
  const size = JSON.stringify(newR.json).length;
  ok(size < 200_000, `claim の応答の大きさ (${size} 文字) — 仕様書は claim のときだけ載る`);
  await svc('POST', `/lp-compose/jobs/${req.job.id}/release`, { lease_token: newR.json.job.lease_token, reason: '片付け' });
  cancel(req.job.id);
}

console.log('⑦ 詳細画面 — 撮影判定の箱に v2 の概要を埋める');
{
  const html = await (await fetch(`${base}/detail/${dV2.id}`)).text();
  const m = html.match(/<script type="application\/json" id="shoot-rec-json">([\s\S]*?)<\/script>/);
  const emb = m ? JSON.parse(m[1]) : null;
  ok(emb && emb.format === 2 && emb.open_required === '不要' && emb.cut_count === 2 && emb.send_targets === 'ハッカ油スプレー 100ml (1本)', '埋め込みに 開封要否・カット数・撮影用送付対象');
  ok(emb && !('cuts' in emb) && !('images' in emb), 'カットの中身・画像ごとの要否は埋めない (撮影指示書 PR-D・構成の一覧 PR-B の役目)');
}

console.log('⑦ 画面の JS (@shoot-rec) — 偽の document で v2 の概要を出す');
{
  const HERE = path.dirname(fileURLToPath(import.meta.url));
  const src = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8');
  const cut = (a, b) => src.slice(src.indexOf(a), src.indexOf(b));
  const F = new Function(cut('/* @image-flow:start', '/* @image-flow:end */') + cut('/* @shoot-rec:start', '/* @shoot-rec:end */')
    + '\nreturn { initImageFlow, initShootRec, shootRecView };')();
  const J2 = lp.jobStateFor(db, dV2.id).shoot;
  const v = F.shootRecView(J2, null);
  ok(v.text.startsWith('AIのおすすめ: 社内撮影 — ' + goodV2().conclusion + '\n開封: 不要 ／ 撮影カット: 2 カット ／ 撮影用送付対象: ハッカ油スプレー 100ml (1本)'),
    `v2: 理由の次の行に 開封・カット数・送付対象 (${JSON.stringify(v.text)})`);
  const vP = F.shootRecView(lp.jobStateFor(db, dPhoto.id).shoot, 'photographer');
  ok(vP.text.includes('\n⚠ カメラマン撮影は 5 カット単位です') && !vP.adopt, '警告は ⚠ の行で出す');
  const vNone = F.shootRecView({ job_id: 9, available: true, format: 2, recommended: 'none', reason: '足ります。', open_required: '不要', cut_count: 0, send_targets: '', warnings: [], missing: null }, null);
  ok(vNone.text.includes('\n開封: 不要') && !vNone.text.includes('撮影カット'), '追加撮影不要ならカット数・送付対象は出さない');
  const J1 = lp.jobStateFor(db, dV1.id).shoot;
  const v1 = F.shootRecView(J1, null);
  ok(J1.format === 1 && !v1.text.includes('開封') && !v1.text.includes('撮影カット'), 'v1 (PR-C の形) には何も足さない (今どおり)');
  // 偽の DOM: 埋め込み → 表示 → ポーリングで入れ直し
  const fakeEl = (init = {}) => {
    const el = { textContent: init.textContent ?? '', dataset: { ...(init.dataset || {}) }, hidden: !!init.hidden, innerHTMLSet: false,
      addEventListener() {}, getAttribute: () => null, setAttribute() {}, classList: { toggle() {} } };
    Object.defineProperty(el, 'innerHTML', { set() { el.innerHTMLSet = true; }, get() { return ''; } });
    return el;
  };
  const evil = { ...J2, send_targets: '<img src=x onerror=alert(1)>' };
  const els = {
    'shoot-mode-box': fakeEl({ dataset: { current: '' } }), 'shoot-rec': fakeEl({ hidden: true }), 'shoot-rec-text': fakeEl(),
    'shoot-rec-adopt': fakeEl({ hidden: true }), 'shoot-rec-json': fakeEl({ textContent: JSON.stringify(J1) }),
  };
  const doc = { getElementById: (id) => els[id] || null, querySelectorAll: () => [] };
  const rec = F.initShootRec(doc);
  ok(!els['shoot-rec-text'].textContent.includes('開封'), '読み込み直後 (v1 の埋め込み) は今どおり');
  rec.update(evil);
  ok(els['shoot-rec-text'].textContent.includes('撮影用送付対象: <img src=x onerror=alert(1)>') && !els['shoot-rec-text'].innerHTMLSet,
    '🚨 ポーリングで v2 に入れ直す。AI の文は textContent で入れる (HTML にしない)');
}

console.log('⑦ 一覧の JS (@lpspec-upload) — 種類を選んで取り込む');
{
  const HERE = path.dirname(fileURLToPath(import.meta.url));
  const src = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'views', 'index.ejs'), 'utf8');
  const chunk = src.slice(src.indexOf('/* @lpspec-upload:start'), src.indexOf('/* @lpspec-upload:end */'));
  ok(chunk.length > 200 && !chunk.includes('<%'), '取り込みの部分を切り出せる (EJS の値を含まない)');
  const F = new Function(chunk + '\nreturn { initLpSpecUpload, lpSpecKindOf };')();
  const run = async (kindValue, { confirmOk = true, resp = { ok: true, created: true, spec: { id: 7, chars: 31419, sheet_titles: ['a', 'b'] } } } = {}) => {
    let click = null;
    const els = {
      'lpspec-upload-btn': { disabled: false, addEventListener: (t, fn) => { click = fn; } },
      'lpspec-result': { textContent: '' },
      'lpspec-file': { files: [{ name: 'upload.xlsx', arrayBuffer: async () => new ArrayBuffer(4) }] },
      'lpspec-kind': { value: kindValue },
    };
    const calls = { fetch: [], confirm: [], alert: [], reload: 0 };
    F.initLpSpecUpload({ getElementById: (id) => els[id] || null }, {
      fetch: async (u, o) => { calls.fetch.push([u, o.method, o.headers['Content-Type']]); return { json: async () => resp }; },
      confirm: (m) => { calls.confirm.push(m); return confirmOk; },
      alert: (m) => calls.alert.push(m), reloadSoon: () => { calls.reload++; },
    });
    await click();
    return { calls, out: els['lpspec-result'].textContent, btn: els['lpspec-upload-btn'] };
  };
  const a = await run('initial_judge');
  ok(a.calls.fetch.length === 1 && a.calls.fetch[0][0].startsWith('/apps/product-hub/api/lp-specs?kind=initial_judge&title='), `新商品初動判定を選ぶと kind=initial_judge で送る (${a.calls.fetch[0]?.[0]})`);
  ok(a.calls.confirm[0].includes('撮影判定の仕様書「新商品初動判定」') && /以降に作る全商品/.test(a.calls.confirm[0]), '🚨 確認に種類の名前を出す (取り違え防止)');
  ok(/新商品初動判定/.test(a.out) && a.calls.reload === 1 && a.btn.disabled === false, '取り込めたら種類の名前と版を出して読み直す');
  const b = await run('product_analysis');
  ok(b.calls.fetch[0][0].includes('kind=product_analysis') && /LP制作システム/.test(b.calls.confirm[0]), 'LP制作システムを選べば今までどおり');
  ok((await run('x')).calls.fetch[0][0].includes('kind=product_analysis'), '知らない値は LP制作システム (今までの既定)');
  const c = await run('initial_judge', { confirmOk: false });
  eq(c.calls.fetch.length, 0, '確認で止めたら送らない');
  const e = await run('initial_judge', { resp: { ok: false, error: 'admin だけです' } });
  ok(e.calls.alert[0] === 'admin だけです' && e.calls.reload === 0, '断られたら理由を出して読み直さない');
}

console.log('⑤ 実行役 (phlp) の通し — 仕様書のファイル・v2 の lint と result・clean');
{
  const HERE = path.dirname(fileURLToPath(import.meta.url));
  const PHLP = path.join(HERE, 'ph-nightly', 'phlp.mjs');
  const work = path.join(tmp || process.cwd(), 'work-ss');
  const state = path.join(tmp || process.cwd(), 'state-ss');
  fs.mkdirSync(work, { recursive: true });
  const phlp = (extraEnv, ...args) => new Promise((resolve) => {
    const child = spawn(process.execPath, [PHLP, ...args], {
      cwd: work,
      env: { ...process.env, PH_LP_BASE: `${base}/service-api`, PH_SERVICE_TOKEN: 'test-token-lp-shoot-spec', PH_LP_RETRIES: '1', PH_LP_STATE_DIR: state, ...extraEnv },
    });
    let so = '', se = '';
    child.stdout.on('data', (x) => { so += x; });
    child.stderr.on('data', (x) => { se += x; });
    child.on('close', (code) => { let json = null; try { json = JSON.parse(so); } catch { /* */ } resolve({ code, out: so, err: se, json }); });
  });
  const d = mkDraft();
  const req = request(d, 'ss-key-phlp');
  const id = String(req.job.id);
  const q = await phlp({}, 'queue');
  ok(q.code === 0 && q.json.queue.claimable === 1 && q.json.queue.waiting_runner_update === 0, '🚨 新しい phlp の queue は仕様書つきの依頼も数える (shoot_spec=1 と言う)');
  const qOld = await call('GET', '/service-api/lp-compose/queue', undefined, { Authorization: 'Bearer test-token-lp-shoot-spec' });
  ok(qOld.json.queue.claimable === 0 && qOld.json.queue.waiting_runner_update === 1, '言わない (古い phlp の) 問い合わせには数えない');
  const cl = await phlp({ PH_LP_RUN_ID: 'lpr-20261009-ss-phlp' }, 'claim', '--run', 'x');
  eq(cl.json?.job_id, req.job.id, '新しい phlp は仕様書つきの依頼を掴む (shoot_spec: true と言う)');
  const specFile = path.join(work, `shoot-spec-${id}.md`);
  ok(fs.existsSync(specFile) && fs.readFileSync(specFile, 'utf8').length === realChars, `🚨 仕様書の全文を shoot-spec-${id}.md に落とす`);
  eq(cl.json.shoot_spec, { id: lp.latestSpec(db, 'initial_judge').id, title: '新商品初動判定_20261009', file: `shoot-spec-${id}.md`, chars: realChars }, 'claim の出力は仕様書のファイル名と大きさだけ');
  const body = fs.readFileSync(specFile, 'utf8');
  ok(!cl.out.includes(body.slice(200, 260)) && cl.out.length < 30_000, `🚨 仕様書の本文は標準出力に出さない (Bash の出力が切れる・${cl.out.length} 文字)`);
  ok(cl.json.shoot_instruction === sh.SHOOT_SPEC_INSTRUCTION, '指示は「仕様書で判定する」の方');
  eq((await phlp({ PH_LP_MODEL: lp.DEFAULT_MODEL }, 'reserve', id)).code, 0, '予約');
  lp.recordImageServed(db, req.job.id, { leaseToken: jobRow(req.job.id).lease_token, fileId: fileOf(d), sha256: 'e'.repeat(64), bytes: 10 });
  fs.writeFileSync(path.join(work, `out-${id}.md`), OUT, 'utf8');
  fs.writeFileSync(path.join(work, `lint-${id}.json`), JSON.stringify({ ok: true }), 'utf8');
  fs.writeFileSync(path.join(work, `shoot-${id}.json`), JSON.stringify(goodV1()), 'utf8');
  const lBad = await phlp({}, 'lint', id, '--file', `out-${id}.md`, '--shoot', `shoot-${id}.json`);
  ok(lBad.code === 1 && lBad.json.shoot.ok === false, '🚨 v1 の形は lint --shoot で exit 1 (仕様書の形で書き直す)');
  // 整形した大きめの v2 (40 カット) も CLI の上限で止まらない
  const long = 'あ'.repeat(280);
  const big = { ...goodV2(), recommended: 'photographer', cuts: Array.from({ length: 40 }, (_, i) => cutOf(i + 1, { lp_image_nos: i === 0 ? [2] : [], content: long, finish: long, purpose: long })) };
  fs.writeFileSync(path.join(work, `shoot-${id}.json`), JSON.stringify(big, null, 2), 'utf8');
  const bigBytes = fs.statSync(path.join(work, `shoot-${id}.json`)).size;
  ok(bigBytes > 100_000 && JSON.stringify(big).length < sh.SHOOT_V2_RAW_MAX, `前提: 整形すると 10 万バイトを超えるが、サーバの上限 (文字) には収まる判定 (${bigBytes} バイト)`);
  const lBig = await phlp({}, 'lint', id, '--file', `out-${id}.md`, '--shoot', `shoot-${id}.json`);
  ok(lBig.code === 0 && lBig.json.shoot.ok === true, `🚨 整形した 40 カットの判定も CLI の上限で止まらずに送れる (${lBig.err.slice(0, 80)})`);
  fs.writeFileSync(path.join(work, `shoot-${id}.json`), JSON.stringify(goodV2(), null, 2), 'utf8');
  const lOk = await phlp({}, 'lint', id, '--file', `out-${id}.md`, '--shoot', `shoot-${id}.json`);
  ok(lOk.code === 0 && lOk.json.shoot.ok === true && Array.isArray(lOk.json.shoot.warnings), 'v2 の形は lint --shoot が通る (warnings も返る)');
  const res = await phlp({}, 'result', id, '--accepted', '--file', `out-${id}.md`, '--lint', `lint-${id}.json`, '--rounds', '1', '--shoot', `shoot-${id}.json`);
  ok(res.code === 0 && res.json.status === 'done' && res.json.shoot.status === 'saved', `result で v2 を保存 (${JSON.stringify(res.json?.shoot)})`);
  const cleaned = await phlp({}, 'clean', id);
  ok(cleaned.json.removed.includes(`shoot-spec-${id}.md`) && !fs.existsSync(specFile), 'clean が仕様書のファイルも消す');
}

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
