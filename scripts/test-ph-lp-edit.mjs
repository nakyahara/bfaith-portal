import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor, fixture, FIXTURES } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-edit-');
/**
 * LP 構成の確認・修正 — 画像制作の新フロー PR-B (apps/product-hub/lib/lp-edit.js・2026-10-09)
 * 実行: node scripts/test-ph-lp-edit.mjs
 *
 * 確かめたいのは:
 *   ① 書き戻しの往復 — 読む → そのまま書く → 元と 1 バイトも同じ。直したら 4 見出しの中身だけが変わり、lint とパーサを通る
 *   ② 並べ替え・追加・削除のあとの `# N枚目｜名前` の振り直し。構図・素材・NG はブロックのまま持ち越す
 *   ③ TOP と 1枚目 (FV) は位置固定・削除不可 / 知らない uid・重複・枚数・文字の形はサーバが断る
 *   ④ 保存は lint を通ったときだけ・古い版からの保存は 409・AI の出力は書き換えない・追記だけ
 *   ⑤ 画像生成 (lp-image) と lpc のコピーが「効いている構成」(編集版) を読む — 本番の経路で受付まで
 *   ⑥ 権限 (管理者・画像の役割の担当者だけ直せる)
 *   ⑦ 画面の JS を偽の document に載せて連続操作し、画面に出る値・送る本文を見る
 */
process.env.PH_SERVICE_TOKEN = 'test-token-lp-edit';
process.env.PH_LP_COMPOSE_ENABLED = '1';
const ENV = { PH_LP_IMAGE_ENABLED: '1', OPENAI_LP_IMAGE_API_KEY: 'sk-test-lp-edit', PH_LP_MONTHLY_BUDGET_JPY: '3000' };
for (const [k, v] of Object.entries(ENV)) process.env[k] = v;
delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;   // Drive には触らない

const fs = await import('node:fs');
const path = await import('node:path');
const { fileURLToPath } = await import('node:url');
const HERE = path.dirname(fileURLToPath(import.meta.url));

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const le = await import('../apps/product-hub/lib/lp-edit.js');
const li = await import('../apps/product-hub/lib/lp-image.js');
const { lintComposition } = await import('../apps/product-hub/lib/lp-lint.js');
const { parseConstructionDoc } = await import('../apps/product-hub/lib/lp-parser.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const NAME = 'ハッカ油スプレー';
const HAKKA = compositionFor(NAME);
const T0 = Date.parse('2026-10-09T03:00:00Z');

/** いまの構成 (cur) を lib の形で作る (uid = a0, a1 …・要撮影なし) */
const curOf = (text) => {
  const r = le.readComposition(text);
  if (!r.ok) throw new Error(r.error);
  return { ...r, slots: r.slots.map((s, i) => ({ ...s, uid: 'a' + i, shoot: false })) };
};
/** 画面が送る形 */
const sendOf = (cur) => cur.slots.map((s) => ({ uid: s.uid, role: s.role, title: s.title, copy: s.copy, body: s.body, shoot: s.shoot }));
const headingsOf = (text) => text.split('\n').filter((l) => /^#(?!#)\s*\d+\s*枚目/.test(l));
const lintOk = (text) => lintComposition(text, { productName: NAME });

console.log('① 読む (0枚目 = top・1枚目 = fv・ほか = point / 4 見出しの中身 / 「なし」は空欄)');
{
  const r = le.readComposition(HAKKA);
  ok(r.ok, '3 枚ものの fixture を読める');
  eq(r.slots.map((s) => s.kind), ['top', 'fv', 'point'], '種類');
  eq(r.slots.map((s) => s.name), ['サムネイル', 'FV', '使用シーン'], '見出しの名前');
  eq([r.slots[1].role, r.slots[1].title, r.slots[1].copy, r.slots[1].body],
    ['FV／商品理解', '夏のベタつく空気に、ひと吹き。', '天然ハッカ油スプレー 100ml', ''], 'FV の 役割・見出し・キャッチコピー・本文 (本文の「なし」は空欄)');
  eq([r.slots[0].title, r.slots[0].copy], ['', ''], 'TOP の見出し・キャッチコピーの「なし」も空欄で見せる');
  const r2 = le.readComposition(fixture(FIXTURES.specExample));
  ok(r2.ok && r2.slots.length === 2 && r2.slots[1].title === '指板のお手入れを、これ1本で。', '仕様書の出力例 (2 枚もの) も読める');
  ok(!le.readComposition(fixture(FIXTURES.legacyV21)).ok, 'V2.1 の旧形式 (# N枚目 の画像ブロックが lint の形でない) は直させない');
  ok(!le.readComposition('').ok, '空は読めない');
  // パーサの読みと切れ目が違う構成は直させない: 最後の画像の中に「共通NG」を含む見出し (パーサはそこで尻に切る)
  const odd = HAKKA.replace('## 生成後チェック\n使用場面の分かりやすさ', '## 共通NGに従う\nx\n\n## 生成後チェック\n使用場面の分かりやすさ');
  const ro = le.readComposition(odd);
  ok(!ro.ok && /パーサの読みと合いません/.test(ro.error), '🚨 パーサ (lp-tool の本番) とブロックの切れ目が合わない構成は直させない', ro.error);
}

console.log('② 往復 (読む → そのまま書く → 元と同じ)・直すと 4 見出しの中身だけ変わる');
let FIVE;
{
  for (const name of [FIXTURES.hakka3, FIXTURES.specExample]) {
    const text = name === FIXTURES.hakka3 ? HAKKA : fixture(name);
    const cur = curOf(text);
    const r = le.composeEdit(cur, sendOf(cur));
    ok(r.ok && r.text === text, `🚨 ${name}: 読んだまま書き戻すと 1 バイトも違わない`);
  }
  // CRLF の本文も読める (LF にそろえて書く)
  const crlf = curOf(HAKKA.replace(/\n/g, '\r\n'));
  ok(le.composeEdit(crlf, sendOf(crlf)).text === HAKKA, 'CRLF の本文も読める (書き戻しは LF)');

  const cur = curOf(HAKKA);
  const send = sendOf(cur);
  send[1].title = '夏の玄関に、ひと吹き。';
  send[1].body = '天然ハッカ油を使ったスプレーです。\n玄関や網戸に。';
  const r = le.composeEdit(cur, send);
  ok(r.ok, 'FV の見出しと本文を直せる');
  const a = HAKKA.split('\n'), b = r.text.split('\n');
  const changed = b.filter((l) => !a.includes(l));
  eq(changed, ['夏の玄関に、ひと吹き。', '天然ハッカ油を使ったスプレーです。', '玄関や網戸に。'], '増えた行は直した値だけ');
  ok(r.text.replace('夏の玄関に、ひと吹き。', '夏のベタつく空気に、ひと吹き。').replace('天然ハッカ油を使ったスプレーです。\n玄関や網戸に。', 'なし') === HAKKA,
    '🚨 直した 2 か所を戻すと元と同じ (共通の頭・尻・構図・素材・NG は 1 文字も変わらない)');
  const lint = lintOk(r.text);
  ok(lint.ok, 'lint を通る');
  const p = parseConstructionDoc(r.text);
  ok(p.images[1].mainCopy === '夏の玄関に、ひと吹き。' && p.images[1].body === '天然ハッカ油を使ったスプレーです。\n玄関や網戸に。',
    'lp-tool のパーサが直した見出し・本文を読む');
  // 空欄にすると「なし」と書く (⑦ の決まり)。読み直すと空欄
  const send2 = sendOf(cur); send2[1].copy = '   ';
  const r2 = le.composeEdit(cur, send2);
  ok(r2.ok && /## サブ見出し\nなし\n/.test(r2.text.split('# 1枚目')[1]) && le.readComposition(r2.text).slots[1].copy === '', '空欄は「なし」と書き、読むと空欄');

  // 追加 2 枚 → 5 枚もの (以降の試験の材料。AI の出力にもする)
  const send3 = [...sendOf(cur),
    { uid: 'nadd1', role: '成分', title: '天然ハッカ油', copy: '香りの元はハッカ油だけ', body: '', shoot: true },
    { uid: 'nadd2', role: 'よくある質問', title: 'こんなときに', copy: '', body: '玄関\n網戸\n車内', shoot: false }];
  const r3 = le.composeEdit(cur, send3);
  ok(r3.ok && r3.summary.added === 2, '画像を 2 枚足せる');
  eq(headingsOf(r3.text), ['# 0枚目｜サムネイル', '# 1枚目｜FV', '# 2枚目｜使用シーン', '# 3枚目｜成分', '# 4枚目｜よくある質問'], '足した画像は後ろに・見出しは役割から');
  const lint3 = lintOk(r3.text);
  ok(lint3.ok, '🚨 足した画像 (雛形) は lint を通る (15 見出し・必須項目)', JSON.stringify(lint3.errors));
  const w0 = lintOk(HAKKA).warnings.filter((w) => w.id === 0).length;
  const w3 = lint3.warnings.filter((w) => w.id === 0).length;
  ok(w3 === w0, `足した画像でパーサの警告 (取れなかった項目) が増えない (${w0} → ${w3})`);
  const p3 = parseConstructionDoc(r3.text);
  ok(p3.images[3].mainCopy === '天然ハッカ油' && p3.images[3].subCopy === '香りの元はハッカ油だけ' && p3.images[3].imageRole === '成分'
    && p3.images[4].body === '玄関\n網戸\n車内', 'パーサが足した画像の 4 項目を読む');
  ok(/## 使用素材\n提供された実物商品画像/.test(p3.images[3].rawBlockText) && /## NG事項\n共通NG事項に従う/.test(p3.images[3].rawBlockText), '雛形は構図・素材・NG を共通の決まりで埋める');
  FIVE = r3.text;
}

console.log('③ 並べ替え・削除のあとの番号・構図はブロックのまま持ち越す');
{
  const cur = curOf(FIVE);
  const s = sendOf(cur);
  // 2 ↔ 4 を入れ替え、3 を消す
  const r = le.composeEdit(cur, [s[0], s[1], s[4], s[2]]);
  ok(r.ok && r.summary.moved && r.summary.removed === 1, '並べ替え + 削除');
  eq(headingsOf(r.text), ['# 0枚目｜サムネイル', '# 1枚目｜FV', '# 2枚目｜よくある質問', '# 3枚目｜使用シーン'], '🚨 # N枚目 を並び順に振り直す');
  ok(lintOk(r.text).ok, 'lint を通る (番号が 0 からの連番)');
  const before = le.readComposition(FIVE).slots[2].lines.join('\n');
  const after = le.readComposition(r.text).slots[3].lines.join('\n');
  ok(before === after, '🚨 動かした画像 (使用シーン) のブロックの中身は 1 文字も変わらない (構図・素材・NG を持ち越す)');
  ok(!r.text.includes('# 共通NG事項\n') || r.text.endsWith(FIVE.slice(FIVE.indexOf('# 共通NG事項'))), '共通の尻 (共通NG・生成後チェック) は元のまま');
  ok(r.text.startsWith(FIVE.slice(0, FIVE.indexOf('# 0枚目'))), '共通の頭は元のまま');
  // 役割を変えると point の見出しの名前も変わる。TOP / FV の名前は変わらない
  const s2 = sendOf(cur); s2[2].role = '使い方｜玄関'; s2[0].role = '検索結果のサムネイル'; s2[1].role = 'ファーストビュー';
  const r2 = le.composeEdit(cur, s2);
  eq(headingsOf(r2.text).slice(0, 3), ['# 0枚目｜サムネイル', '# 1枚目｜FV', '# 2枚目｜使い方／玄関'], '役割を変えると point の名前も変わる (区切り記号は外す)・TOP / FV は固定');
  ok(lintOk(r2.text).ok, '役割を変えても lint を通る');
  // 最後の画像を真ん中へ (尻の前の空行の扱い)
  const r3 = le.composeEdit(cur, [s[0], s[1], s[4], s[2], s[3]]);
  ok(r3.ok && lintOk(r3.text).ok && headingsOf(r3.text)[2] === '# 2枚目｜よくある質問', '最後の画像を前に動かしても lint を通る');
  // 全部消すと TOP + FV の 2 枚
  const r4 = le.composeEdit(cur, [s[0], s[1]]);
  ok(r4.ok && lintOk(r4.text).ok && r4.summary.removed === 3, 'point を全部消しても lint を通る (2 枚)');
}

console.log('④ 決まり (TOP / FV は固定・知らない uid・枚数・文字の形)');
{
  const cur = curOf(FIVE);
  const s = sendOf(cur);
  const errOf = (send) => { const r = le.composeEdit(cur, send); return r.ok ? null : r.error; };
  ok(/TOP \(0枚目\) は位置が固定/.test(errOf([s[2], s[1], s[0], s[3], s[4]]) || ''), '🚨 TOP を動かせない');
  ok(/1枚目 \(FV\) は位置が固定/.test(errOf([s[0], s[2], s[3], s[4]]) || ''), '🚨 FV を消せない (位置もずらせない)');
  ok(/同じ画像が 2 回|位置が固定/.test(errOf([s[0], s[1], s[2], s[3], { ...s[1], uid: s[1].uid }]) || ''), 'FV をもう一度後ろに入れられない (重複か固定で断る)');
  ok(/今の構成にありません/.test(errOf([s[0], s[1], { ...s[2], uid: 'a9' }]) || ''), '🚨 今の構成に無い (n で始まらない) uid は断る (古いタブの削除済みの画像など)');
  ok(/同じ画像が 2 回/.test(errOf([s[0], s[1], s[2], s[2]]) || ''), '同じ画像を 2 回入れられない');
  const many = [...s, ...Array.from({ length: 6 }, (_, i) => ({ uid: 'nmany' + i, role: '特長', title: 'x' + i, copy: '', body: '', shoot: false }))];
  ok(/2〜10 枚/.test(errOf(many) || ''), `🚨 11 枚は断る (lint の上限 ${le.MAX_IMAGES} 枚)`);
  ok(le.composeEdit(cur, many.slice(0, 10)).ok, '10 枚までは通る');
  ok(/2〜10 枚/.test(errOf([s[0]]) || ''), '1 枚は断る');
  const bad = (patch, k = 2) => errOf(s.map((x, i) => (i === k ? { ...x, ...patch } : x))) || '';
  ok(/役割を入れてください/.test(bad({ role: '  ' })), '役割は空にできない');
  ok(/役割を入れてください/.test(bad({ role: 'なし' })), '役割を「なし」にもできない (⑦ の空の印)');
  ok(/行の先頭に # は使えません/.test(bad({ body: '玄関\n# 3枚目｜偽物' })), '🚨 行頭の # は断る (見出しと区別できなくなる)');
  ok(/行の先頭に # は使えません/.test(bad({ title: '#1位' })), '見出しの # も断る');
  ok(/見出しは 120 文字まで/.test(bad({ title: 'あ'.repeat(121) })), '長すぎる見出しは断る');
  ok(/制御文字/.test(bad({ copy: 'a\u0007b' })), '制御文字は断る');
  ok(/true \/ false/.test(bad({ shoot: 'yes' })), '要撮影は true / false だけ');
  ok(/文字で送って/.test(bad({ body: null })), '4 項目は文字で送る');
  ok(/uid\) の形が不正/.test(bad({ uid: '<x>' })), 'uid の形が違えば断る');
  ok(/画像の並び \(slots\) がありません/.test(le.composeEdit(cur, null).error), 'slots が無ければ断る');
  // AI が書いたまま触っていない項目は、長さや形を見ない (直していないものを理由に保存を止めない)
  const longAi = FIVE.replace('## 本文\n玄関\n網戸\n車内', '## 本文\n' + 'い'.repeat(1600));
  const curL = curOf(longAi);
  ok(le.composeEdit(curL, sendOf(curL)).ok, 'AI が書いた長い本文は、触らなければ通る');
  // 新しく足した画像は全部の項目を見る
  ok(/役割を入れてください/.test(errOf([...s, { uid: 'nnew', role: '', title: 'x', copy: '', body: '', shoot: false }]) || ''), '足した画像の役割も空にできない');
}

// ─── DB とサーバ ────────────────────────────────────────

const spec = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式'], actor: 't' }).spec;
const FOLDER = '1MtcKdnRZPf1iqKiNxMJ1ODDPJE3vX9JR';
let seqNo = 0;
/** 構成ができた (done・実モデル一致) 構成を 1 つ作る。draft を渡すとその商品に作り直す */
function compose(output, { draft = null, check = 'claude-opus-5-5' } = {}) {
  seqNo++;
  if (!draft) {
    const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url, image_priority, created_by) VALUES (?, ?, ?, ?, 't')`)
      .run('LPEDIT' + seqNo, NAME, 'https://drive.google.com/drive/folders/' + FOLDER, '自社商品（重要度：高）').lastInsertRowid);
    draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  }
  const images = [{ file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }];
  const r = lp.requestJob(db, { draft, spec, idempotencyKey: 'key-edit-' + seqNo, actor: 't', productInfo: 'ハッカ油', colorVariations: '', images, now: T0 });
  const c = lp.claimJob(db, { runnerRunId: 'run-edit-' + seqNo, maxImages: 16, now: T0 });
  if (!c.job || c.job.job_id !== r.job.id) throw new Error('claim mismatch');
  const g = lp.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: T0 });
  for (const im of JSON.parse(r.job.packet_json).images) {
    lp.recordImageServed(db, c.job.job_id, { leaseToken: c.job.lease_token, fileId: im.file_id, sha256: 'a'.repeat(64), bytes: 10, now: T0 });
  }
  const s = lp.submitResult(db, g.generation_id, { packetHash: c.job.packet_hash, verdict: 'accepted', output, lint: { ok: true }, reviewRounds: 1, now: T0 });
  if (s.status !== 'done') throw new Error('submit: ' + JSON.stringify(s));
  if (check) lp.recordModelCheck(db, { runnerRunId: 'run-edit-' + seqNo, actualModels: [check], now: T0 });
  return { draft, jobId: r.job.id };
}
const jobText = (id) => db.prepare('SELECT output_text FROM ph_lp_compose_jobs WHERE id = ?').get(id).output_text;
const editsOf = (draftId) => db.prepare('SELECT * FROM ph_lp_compose_edits WHERE draft_id = ? ORDER BY id').all(draftId);

console.log('⑤ 効いている構成・保存 (lint を通ったときだけ・追記だけ・409)');
{
  const A = compose(FIVE);
  const eff0 = le.effectiveComposeText(db, A.draft.id);
  ok(eff0 && eff0.job_id === A.jobId && eff0.edit_id === null && eff0.source === 'ai' && eff0.text === FIVE, '編集版が無ければ AI の本文が効いている');
  const st0 = le.editStateFor(db, A.draft, { canEdit: true });
  ok(st0.available && st0.base_edit_id === null && st0.slots.map((x) => x.uid).join() === 'a0,a1,a2,a3,a4', '画面の状態: uid は a0…・編集版なし');
  const send = st0.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));
  send[1].title = '夏の玄関に、ひと吹き。';
  send[3].shoot = true;
  const moved = [send[0], send[1], send[3], send[2], send[4]];
  const r1 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: null, slots: moved, actor: 'staff@x', now: T0 + 1000 });
  ok(r1.ok && r1.changed && Number.isInteger(r1.edit_id), '保存できる');
  ok(jobText(A.jobId) === FIVE, '🚨 AI の生の出力 (output_text) は書き換えない (段階1 の測定の材料)');
  const eff1 = le.effectiveComposeText(db, A.draft.id);
  ok(eff1.edit_id === r1.edit_id && eff1.source === 'edit' && eff1.text.includes('夏の玄関に、ひと吹き。') && eff1.edited_by === 'staff@x', '効いている構成 = 編集版');
  eq(headingsOf(eff1.text), ['# 0枚目｜サムネイル', '# 1枚目｜FV', '# 2枚目｜成分', '# 3枚目｜使用シーン', '# 4枚目｜よくある質問'], '並べ替えが入っている');
  const st1 = le.editStateFor(db, A.draft);
  ok(st1.slots.map((x) => x.uid).join() === 'a0,a1,a3,a2,a4' && st1.slots[2].shoot === true && st1.slots[3].shoot === false,
    '🚨 uid と要撮影は並べ替えても画像についていく (slots_json)');
  const meta = JSON.parse(editsOf(A.draft.id)[0].slots_json);
  ok(meta[2].shoot === true && meta[2].block.startsWith('# 2枚目｜成分') && /## 使用素材/.test(meta[2].block), 'slots_json に要撮影と元のブロックが残る (撮影指示書 PR-D が読む)');
  const ev = db.prepare(`SELECT detail, actor FROM draft_events WHERE draft_id = ? AND event = 'lp_compose_edited'`).all(A.draft.id);
  ok(ev.length === 1 && ev[0].actor === 'staff@x' && /並べ替え/.test(ev[0].detail) && /文字 1 枚/.test(ev[0].detail) && /撮影の要否 1 枚/.test(ev[0].detail), '操作履歴が残る', JSON.stringify(ev));

  // 同じ内容をもう一度 = 何もしない
  const same = st1.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));
  const r2 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: r1.edit_id, slots: same, actor: 'staff@x' });
  ok(r2.ok && r2.changed === false && editsOf(A.draft.id).length === 1, '同じ内容の保存は行を増やさない');
  // 🚨 古いタブ: 見ていた版 (編集版なし) から保存すると 409
  const r3 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: null, slots: send, actor: 'other@x' });
  ok(!r3.ok && r3.code === 'conflict' && editsOf(A.draft.id).length === 1, '🚨 古い版 (base_edit_id) からの保存は conflict・上書きしない');
  // lint に落ちる保存はしない (省略表現は lint の検査 10)
  const lintBad = same.map((x, i) => (i === 2 ? { ...x, body: '以下同様' } : x));
  const r4 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: r1.edit_id, slots: lintBad, actor: 'staff@x' });
  ok(!r4.ok && r4.code === 'lint' && /省略|以下同様/.test(r4.error) && editsOf(A.draft.id).length === 1, '🚨 lint に落ちる構成は保存しない (サーバの lint が正本)', r4.error);
  const r5 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: r1.edit_id, slots: same.slice(0, 1), actor: 'x' });
  ok(!r5.ok && r5.code === 'invalid', '形の悪い並びは invalid');
  ok(!le.saveEdit(db, { draft: A.draft, baseJobId: 'x', baseEditId: null, slots: same }).ok, 'base_job_id の形が違えば断る');
  // 追記だけ
  let threw = 0;
  try { db.prepare('UPDATE ph_lp_compose_edits SET output_text = ? WHERE id = ?').run('x', r1.edit_id); } catch { threw++; }
  try { db.prepare('DELETE FROM ph_lp_compose_edits WHERE id = ?').run(r1.edit_id); } catch { threw++; }
  ok(threw === 2, '🚨 編集版の表は追記だけ (書き換え・削除はトリガーで止まる)');

  // AI に作り直させた → 新しい構成が効く・古い構成への編集版は使わない・古い版からの保存は 409
  const B = compose(HAKKA, { draft: A.draft });
  const eff2 = le.effectiveComposeText(db, A.draft.id);
  ok(eff2.job_id === B.jobId && eff2.edit_id === null && eff2.text === HAKKA, '🚨 AI が作り直したら新しい構成 (古い構成への編集版は混ぜない)');
  const r6 = le.saveEdit(db, { draft: A.draft, baseJobId: A.jobId, baseEditId: r1.edit_id, slots: same, actor: 'staff@x' });
  ok(!r6.ok && r6.code === 'conflict' && /作り直されています/.test(r6.error), '🚨 作り直す前の画面からの保存は conflict');
  // 実モデルが一致しない (needs_review) 構成は効かない — その前の構成のまま
  compose(FIVE, { draft: A.draft, check: 'claude-sonnet-4' });
  ok(le.effectiveComposeText(db, A.draft.id).job_id === B.jobId, '実モデルが一致しない構成は「効いている構成」にしない');
  // 実モデルの確認がまだ付いていない done (確認待ち) も効かせない — 不一致と分かってから取り返せない
  compose(FIVE, { draft: A.draft, check: null });
  ok(le.effectiveComposeText(db, A.draft.id).job_id === B.jobId, '🚨 実モデルの確認待ち (model_check が無い done) の構成も「効いている構成」にしない');
  // 構成がまだ無い商品
  const none = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LPEDIT-NONE', 'x', 't')`).run().lastInsertRowid));
  ok(le.effectiveComposeText(db, none.id) === null && le.editStateFor(db, none).available === false, '構成がまだ無ければ出さない');
  ok(le.saveEdit(db, { draft: none, baseJobId: 1, baseEditId: null, slots: [] }).code === 'not_ready', '構成が無ければ保存できない');
}

console.log('⑥ 画像を作ったあとで構成が変わった印');
{
  const C = compose(FIVE);
  const insImg = (at) => db.prepare(`INSERT INTO ph_lp_image_jobs (draft_id, compose_job_id, idempotency_key, status, model, quality, size, created_at)
    VALUES (?, ?, ?, 'done', 'gpt-image-2.5-flare', 'medium', '1200x1200', ?)`).run(C.draft.id, C.jobId, 'k-' + at, new Date(at).toISOString());
  ok(le.editStateFor(db, C.draft).image_stale === false, '画像がまだ無ければ印は出さない');
  insImg(T0 + 10_000);
  ok(le.editStateFor(db, C.draft).image_stale === false, 'AI の構成のまま作った画像 = 変わっていない');
  const st = le.editStateFor(db, C.draft, { canEdit: true });
  const send = st.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));
  send[2].title = '直した見出し';
  le.saveEdit(db, { draft: C.draft, baseJobId: st.base_job_id, baseEditId: null, slots: send, actor: 't', now: T0 + 20_000 });
  ok(le.editStateFor(db, C.draft).image_stale === true, '🚨 画像を作ったあとで直すと「生成後に構成が変わりました」');
  insImg(T0 + 30_000);
  ok(le.editStateFor(db, C.draft).image_stale === false, '直した構成でもう一度作れば印は消える');
  const st2 = le.editStateFor(db, C.draft, { canEdit: true });
  const send2 = st2.slots.map((x, i) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: i === 3 ? !x.shoot : x.shoot }));
  le.saveEdit(db, { draft: C.draft, baseJobId: st2.base_job_id, baseEditId: st2.base_edit_id, slots: send2, actor: 't', now: T0 + 40_000 });
  ok(le.editStateFor(db, C.draft).image_stale === false, '要撮影の切り替えだけ (本文が同じ) なら画像は古くならない');
  compose(HAKKA, { draft: C.draft });
  ok(le.editStateFor(db, C.draft).image_stale === true, 'AI が構成を作り直したら、前の構成の画像は古い');
}

console.log('⑦ 画像生成 (lp-image) が編集版を読む');
{
  const D = compose(FIVE);
  // 参考画像なし (Drive に触らずに受付まで通すため。packet の画像だけ空にする)
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = json_set(packet_json, '$.images', json('[]')) WHERE id = ?`).run(D.jobId);
  const st = le.editStateFor(db, D.draft, { canEdit: true });
  const send = st.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));
  send[1].title = '編集版の見出し';
  const r = le.saveEdit(db, { draft: D.draft, baseJobId: st.base_job_id, baseEditId: null, slots: [send[0], send[1], send[4], send[2]], actor: 't' });
  ok(r.ok, '並べ替え・削除・見出しを直して保存');
  const cfg = li.lpImageConfig(ENV);
  ok(li.imageBlockReason(db, { draft: D.draft, folderId: FOLDER, env: ENV }) === null, '押せる');
  const st2 = li.imageStateFor(db, { draft: D.draft, folderId: FOLDER, env: ENV });
  eq(st2.planned_count, 4, '🚨 作る枚数は編集版の枚数 (5 → 4)');
  const rq = li.requestImageJob(db, { draft: D.draft, folderId: FOLDER, idempotencyKey: 'edit-img-0001', actor: 't', refTimes: {}, env: ENV });
  ok(rq.ok, '受け付ける (lib)');
  const ims = db.prepare('SELECT seq, no, name, prompt FROM ph_lp_images WHERE image_job_id = ? ORDER BY seq').all(rq.job.id);
  eq(ims.map((i) => `${i.no}:${i.name}`), ['0:サムネイル', '1:FV', '2:よくある質問', '3:使用シーン'], '🚨 画像は編集版の並び・名前で作る');
  ok(ims[1].prompt.includes('編集版の見出し') && !ims[1].prompt.includes('夏のベタつく空気に'), '🚨 prompt に直した見出しが入る (AI の見出しではない)');
  ok(!ims.some((i) => i.prompt.includes('香りの元はハッカ油だけ')), '消した画像 (成分) は作らない');
  ok(cfg.usable, '(設定は使える)');
  // 構成を作り直したら、新しい構成の AI の本文 (古い編集版は読まない)
  db.prepare(`UPDATE ph_lp_image_jobs SET status = 'done' WHERE id = ?`).run(rq.job.id);
  db.prepare(`UPDATE ph_lp_images SET status = 'done' WHERE image_job_id = ?`).run(rq.job.id);
  const E = compose(HAKKA, { draft: D.draft });
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = json_set(packet_json, '$.images', json('[]')) WHERE id = ?`).run(E.jobId);
  eq(li.imageStateFor(db, { draft: D.draft, folderId: FOLDER, env: ENV }).planned_count, 3, '作り直した構成は AI の本文 (3 枚)');

  // 作り直しが失敗した・作っている途中 → 人が直した前の構成から作れる (一覧と画像生成が同じ構成を見る・Codex PR-B 名指し H)
  const G = compose(FIVE);
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = json_set(packet_json, '$.images', json('[]')) WHERE id = ?`).run(G.jobId);
  const sg = le.editStateFor(db, G.draft, { canEdit: true });
  const sendG = sg.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));
  le.saveEdit(db, { draft: G.draft, baseJobId: G.jobId, baseEditId: null, slots: sendG.slice(0, 4), actor: 't' });
  const images = [{ file_id: 'FILEIDWHITE01', role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }];
  const rq2 = lp.requestJob(db, { draft: G.draft, spec, idempotencyKey: 'key-edit-fail-1', actor: 't', productInfo: 'ハッカ油', colorVariations: '', images, now: T0 });
  ok(rq2.ok && li.imageBlockReason(db, { draft: G.draft, folderId: FOLDER, env: ENV }) === null
    && li.imageStateFor(db, { draft: G.draft, folderId: FOLDER, env: ENV }).planned_count === 4, '🚨 AI が作り直している途中でも、直した構成 (4 枚) から作れる');
  const cl = lp.claimJob(db, { runnerRunId: 'run-edit-fail-1', maxImages: 16, now: T0 });
  lp.failJob(db, cl.job.job_id, { leaseToken: cl.job.lease_token, code: 'images_unavailable', message: 'x', now: T0 });
  ok(db.prepare('SELECT status FROM ph_lp_compose_jobs WHERE id = ?').get(rq2.job.id).status === 'failed', '(作り直しは失敗した)');
  ok(li.imageBlockReason(db, { draft: G.draft, folderId: FOLDER, env: ENV }) === null
    && li.imageStateFor(db, { draft: G.draft, folderId: FOLDER, env: ENV }).planned_count === 4, '🚨 作り直しが失敗しても、直した構成 (4 枚) から作れる');
  const rq3 = li.requestImageJob(db, { draft: G.draft, folderId: FOLDER, idempotencyKey: 'edit-img-fail1', actor: 't', refTimes: {}, env: ENV });
  ok(rq3.ok && rq3.job.compose_job_id === G.jobId, '受付は直した構成の依頼に付く');
}

// ─── 画面の口 (router) ────────────────────────────────────

const express = (await import('express')).default;
const { default: router } = await import('../apps/product-hub/router.js');
const app = express();
// 役割・メールはヘッダで切り替える (管理者でなく「実際に押す役割」でも試す)
app.use((req, _res, next) => { req.session = { email: req.get('X-Test-Email') || 'nakahara@x', role: req.get('X-Test-Role') || 'admin' }; next(); });
app.use('/apps/product-hub', router);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;
const AS_IMAGE = { 'X-Test-Role': 'user', 'X-Test-Email': 'image-staff@x' };
const AS_NOROLE = { 'X-Test-Role': 'user', 'X-Test-Email': 'norole-staff@x' };
const AS_STRANGER = { 'X-Test-Role': 'user', 'X-Test-Email': 'nobody@x' };
const imgStaff = Number(db.prepare(`INSERT INTO ph_staff (name, kind, portal_email) VALUES ('画像係', 'internal', 'image-staff@x')`).run().lastInsertRowid);
db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(imgStaff);
db.prepare(`INSERT INTO ph_staff (name, kind, portal_email) VALUES ('役割なし', 'internal', 'norole-staff@x')`).run();
const api = async (method, p, { body, headers = {} } = {}) => {
  const res = await fetch(base + p, { method, headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...headers }, body: body === undefined ? undefined : JSON.stringify(body) });
  const ct = res.headers.get('content-type') || '';
  return { status: res.status, json: ct.includes('json') ? await res.json() : null, text: ct.includes('json') ? null : await res.text() };
};
const sendFrom = (st) => st.slots.map((x) => ({ uid: x.uid, role: x.role, title: x.title, copy: x.copy, body: x.body, shoot: x.shoot }));

let R;   // 画面の試験でも使う商品
console.log('⑧ API (GET / PUT・権限・409・lint)');
{
  R = compose(FIVE);
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = json_set(packet_json, '$.images', json('[]')) WHERE id = ?`).run(R.jobId);
  const g0 = await api('GET', `/api/drafts/${R.draft.id}/lp-edit`);
  ok(g0.status === 200 && g0.json.available && g0.json.can_edit === true && g0.json.slots.length === 5 && g0.json.base_edit_id === null, 'GET: 管理者は直せる・5 枚');
  const gImg = await api('GET', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_IMAGE });
  ok(gImg.json.can_edit === true, 'GET: 画像の役割の担当者も直せる');
  const gNo = await api('GET', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_NOROLE });
  ok(gNo.status === 200 && gNo.json.can_edit === false && gNo.json.slots.length === 5, 'GET: 役割の無い担当者は見るだけ');
  const send = sendFrom(g0.json);
  send[2].title = '担当者が直した見出し';
  const pNo = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_NOROLE, body: { base_job_id: g0.json.base_job_id, base_edit_id: null, slots: send } });
  ok(pNo.status === 403 && editsOf(R.draft.id).length === 0, '🚨 PUT: 役割の無い担当者は 403・保存しない');
  const pSt = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_STRANGER, body: { base_job_id: g0.json.base_job_id, base_edit_id: null, slots: send } });
  ok(pSt.status === 403, '🚨 PUT: 担当者でない一般ユーザーは 403');
  const p1 = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_IMAGE, body: { base_job_id: g0.json.base_job_id, base_edit_id: null, slots: send } });
  ok(p1.status === 200 && p1.json.ok && p1.json.changed && p1.json.base_edit_id > 0 && p1.json.slots[2].title === '担当者が直した見出し', '🚨 PUT: 画像の役割の担当者が保存でき、読み直した状態が返る');
  ok(editsOf(R.draft.id)[0].edited_by === 'image-staff@x', '直した人が残る');
  const p2 = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: g0.json.base_job_id, base_edit_id: null, slots: send } });
  ok(p2.status === 409 && p2.json.code === 'conflict', '🚨 PUT: 古いタブ (見ていた版が古い) は 409');
  const bad = sendFrom(p1.json); bad[3].body = '必要枚数分同様';
  const p3 = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: p1.json.base_job_id, base_edit_id: p1.json.base_edit_id, slots: bad } });
  ok(p3.status === 400 && p3.json.code === 'lint' && Array.isArray(p3.json.errors), 'PUT: lint に落ちれば 400 (理由つき)');
  const p4 = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: p1.json.base_job_id, base_edit_id: p1.json.base_edit_id, slots: [p1.json.slots[1], p1.json.slots[0]] } });
  ok(p4.status === 400 && p4.json.code === 'invalid' && /固定/.test(p4.json.error), 'PUT: TOP / FV を動かすと 400');
  const p5 = await api('PUT', `/api/drafts/999999/lp-edit`, { body: {} });
  ok(p5.status === 404, '知らない商品は 404');
  const none = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LPEDIT-API-NONE', 'x', 't')`).run().lastInsertRowid);
  const p6 = await api('PUT', `/api/drafts/${none}/lp-edit`, { body: { base_job_id: 1, base_edit_id: null, slots: [] } });
  ok(p6.status === 409 && p6.json.code === 'not_ready', '構成がまだ無い商品は 409');
  ok((await api('GET', `/api/drafts/${none}/lp-edit`)).json.available === false, 'GET: 構成がまだ無ければ available=false');
  ok(editsOf(R.draft.id).length === 1, '失敗した保存は行を増やさない');

  // lpc (「AI が作った構成」の箱) の状態に、直した版が添えられる
  const lpc = await api('GET', `/api/drafts/${R.draft.id}/lp-compose`);
  ok(lpc.json.lp_edit && lpc.json.lp_edit.base_job_id === R.jobId && lpc.json.lp_edit.output_text.includes('担当者が直した見出し')
    && lpc.json.job.output_text === FIVE, '🚨 lp-compose の状態: 直した版を添える (AI の本文はそのまま)');
  // 画像生成の本番の口 (受付まで)
  const im = await api('GET', `/api/drafts/${R.draft.id}/lp-images`);
  ok(im.json.planned_count === 5 && im.json.blocked === null, '画像の状態: 編集版の枚数');
  const post = await api('POST', `/api/drafts/${R.draft.id}/lp-images`, { body: { idempotency_key: 'edit-route-0001' } });
  ok(post.status === 200 && post.json.ok, '画像を作る依頼を受け付ける (本番の経路)', JSON.stringify(post.json).slice(0, 200));
  const jobRow = db.prepare('SELECT id FROM ph_lp_image_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(R.draft.id);
  const prm = db.prepare('SELECT no, prompt FROM ph_lp_images WHERE image_job_id = ? ORDER BY seq').all(jobRow.id);
  ok(prm.length === 5 && prm[2].prompt.includes('担当者が直した見出し'), '🚨 受付で固定した prompt に、画面で直した見出しが入る');
  // 詳細画面
  const html = (await api('GET', `/detail/${R.draft.id}`, { headers: { Accept: 'text/html' } })).text;
  ok(html.includes('id="lpe"') && html.includes('id="lpe-json"') && html.includes(`data-url="/apps/product-hub/api/drafts/${R.draft.id}/lp-edit"`), '詳細画面に構成の一覧の置き場がある');
  const lpeAt = html.indexOf('id="lpe"');
  ok(lpeAt > html.indexOf('id="lpc"') && lpeAt < html.indexOf('id="shoot-mode-box"') && lpeAt > html.indexOf('id="ipf-step-lp"') && lpeAt < html.indexOf('id="ipf-step-shoot"'),
    '置き場は 2 (仮LP構成と撮影判定) の中・AI の構成の箱の下・撮影判定の上');
  const emb = JSON.parse(html.match(/<script type="application\/json" id="lpe-json">([\s\S]*?)<\/script>/)[1]);
  ok(emb.available && emb.slots.length === 5 && emb.can_edit === true, '最初の状態を埋め込む');
  const htmlNo = (await api('GET', `/detail/${R.draft.id}`, { headers: { Accept: 'text/html', ...AS_NOROLE } })).text;
  ok(JSON.parse(htmlNo.match(/id="lpe-json">([\s\S]*?)<\/script>/)[1]).can_edit === false, '役割の無い人の画面は見るだけ');
  const htmlNone = (await api('GET', `/detail/${none}`, { headers: { Accept: 'text/html' } })).text;
  ok(/<div id="lpe"[^>]*hidden>/.test(htmlNone), '構成がまだ無い商品は一覧を隠しておく');
  // AI の機能がオフなら出さない
  process.env.PH_LP_COMPOSE_ENABLED = '';
  const htmlOff = (await api('GET', `/detail/${R.draft.id}`, { headers: { Accept: 'text/html' } })).text;
  ok(!htmlOff.includes('id="lpe"'), 'AI の機能がオフなら一覧を出さない');
  process.env.PH_LP_COMPOSE_ENABLED = '1';
  // 🚨 AI と人の書いた文字に </script> が入っても画面が壊れない
  const xs = sendFrom((await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json);
  xs[2].body = '</script><img src=x onerror=alert(1)>';
  const cur = await api('GET', `/api/drafts/${R.draft.id}/lp-edit`);
  const px = await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: cur.json.base_job_id, base_edit_id: cur.json.base_edit_id, slots: xs } });
  ok(px.status === 200, '記号を含む本文も保存できる');
  const htmlX = (await api('GET', `/detail/${R.draft.id}`, { headers: { Accept: 'text/html' } })).text;
  const embX = htmlX.match(/id="lpe-json">([\s\S]*?)<\/script>/)[1];
  ok(!embX.includes('</script') && JSON.parse(embX).slots[2].body.includes('</script>'), '🚨 埋め込みの JSON で </script> を潰す (画面が途中で切れない)');
}

// ─── 画面の JS (偽の document) ─────────────────────────────

const detailSrc = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8').replace(/\r\n/g, '\n');
const chunk = detailSrc.slice(detailSrc.indexOf('/* @lp-edit:start'), detailSrc.indexOf('/* @lp-edit:end */'));
/**
 * 偽の document。root.innerHTML に入れた HTML から id の付いた要素を拾い直す (hidden / disabled / readonly も読む)。
 * 押せるのは描いたボタンだけ (disabled なら押せない) — 画面に出ていない操作を試験が勝手に呼ばない
 */
function makeDom(initial) {
  const reg = new Map();
  const ls = {};
  const root = {
    dataset: { url: `${base}/api/drafts/${R.draft.id}/lp-edit` }, hidden: false, _h: '',
    addEventListener: (t, fn) => { (ls[t] = ls[t] || []).push(fn); },
    set innerHTML(h) {
      this._h = h;
      for (const k of [...reg.keys()]) if (k !== 'lpe' && k !== 'lpe-json') reg.delete(k);
      for (const m of h.matchAll(/<(\w+)((?:[^>"]|"[^"]*")*?)\sid="([^"]+)"((?:[^>"]|"[^"]*")*)>/g)) {
        const attrs = ' ' + m[2] + ' ' + m[4] + ' ';
        reg.set(m[3], { id: m[3], hidden: /\shidden[\s>]/.test(attrs), disabled: /\sdisabled[\s>]/.test(attrs), readOnly: /\sreadonly[\s>]/.test(attrs),
          textContent: '', innerHTML: '', focused: 0, focus() { this.focused += 1; }, select() {} });
      }
    },
    get innerHTML() { return this._h; },
  };
  reg.set('lpe', root);
  reg.set('lpe-json', { textContent: JSON.stringify(initial) });
  const doc = { getElementById: (id) => reg.get(id) || null };
  const fire = (t, target) => { for (const fn of ls[t] || []) fn({ target, preventDefault() {} }); };
  /** 描いたボタンを押す。無い・disabled なら押さずに false */
  const click = (act, uid) => {
    const tags = [...root._h.matchAll(/<button\b([^>]*)>/g)].map((m) => m[1]);
    const tag = tags.find((a) => a.includes(`data-act="${act}"`) && (uid == null || a.includes(`data-uid="${uid}"`)));
    if (!tag) return false;
    // id のあるボタン (保存) は、描いた後に JS が disabled を切り替える — いまの要素の値を見る
    const idm = tag.match(/\sid="([^"]+)"/);
    if (idm ? reg.get(idm[1]).disabled : /\sdisabled\b/.test(tag)) return false;
    const btn = { dataset: { act, uid } };
    fire('click', { closest: () => btn });
    return true;
  };
  /** 欄に打つ。描いた欄が無い・読み取り専用なら打てない */
  const type = (uid, k, value) => {
    const el = reg.get(`lpe-f-${uid}-${k}`);
    if (!el || el.readOnly) return false;
    fire('input', { dataset: { uid, k }, value });
    return true;
  };
  const shown = (id) => { const el = reg.get(id); return !!el && !el.hidden; };
  return { doc, root, reg, click, type, shown, fire };
}
const settle = async (api0) => { for (let i = 0; i < 200 && api0.state().saving; i++) await new Promise((r) => setTimeout(r, 10)); await new Promise((r) => setTimeout(r, 30)); };
const realDeps = (extra = {}) => ({
  get: async (u) => (await fetch(u, { headers: { Accept: 'application/json' } })).json(),
  put: async (u, body) => { puts.push(body); return (await fetch(u, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json(); },
  confirm: () => confirmAnswer, onSaved: () => { savedEvents += 1; }, newUid: () => 'nui' + (++uidSeq), ...extra,
});
const puts = [];
let confirmAnswer = false, savedEvents = 0, uidSeq = 0;

console.log('⑨ 画面の JS: 一覧を描く・並べ替え・追加・削除・元に戻す・保存');
{
  ok(chunk.length > 1000 && !chunk.includes('<%'), '画面の JS を切り出せる (EJS の値を含まない)');
  const F = new Function(chunk + '\nreturn { initLpEdit, lpeAction, lpeHtml, lpeFromServer, lpeForSave, lpeDirty };')();
  const st = (await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json;
  const D = makeDom(st);
  const ui = F.initLpEdit(D.doc, realDeps());
  const H = () => D.root.innerHTML;
  ok(!D.root.hidden && H().includes('LP構成（5枚）') && H().includes('人が直した版'), '一覧を出す (枚数・人が直した版の印)');
  const [u0, u1, u2, u3, u4] = st.slots.map((s) => s.uid);
  ok(/<span class="lpe-no">TOP<\/span>/.test(H()) && /<span class="lpe-no">1枚目<\/span>/.test(H()) && /<span class="lpe-no">4枚目<\/span>/.test(H()), '番号は TOP / 1枚目 / … / 4枚目');
  ok(!D.click('del', u0) && !D.click('up', u1) && H().split('>固定</span>').length - 1 === 2, '🚨 TOP と 1枚目 (FV) には ↑↓× が無く「固定」');
  ok(!D.click('up', u2), '🚨 2枚目は上へ行けない (FV の上には行かない・ボタンが押せない)');
  ok(!D.click('down', u4), '最後の画像は下へ行けない');
  ok(!D.shown('lpe-dirty') && D.reg.get('lpe-save').disabled, '開いた直後は未保存の印なし・保存ボタンは押せない');
  // 文字を直す → 未保存の印・行の見出しが追随
  ok(D.type(u2, 'title', '画面で直した見出し'), '見出しを打てる');
  ok(D.shown('lpe-dirty') && !D.reg.get('lpe-save').disabled, '🚨 直すと「未保存の変更があります」・保存ボタンが押せる');
  ok(D.reg.get(`lpe-title-${u2}`).innerHTML === '<b>画面で直した見出し</b>', '行の見出しが打った値に変わる');
  D.type(u2, 'title', '<b>x</b>');
  ok(D.reg.get(`lpe-title-${u2}`).innerHTML === '<b>&lt;b&gt;x&lt;/b&gt;</b>', '🚨 打った文字は HTML にしない (エスケープ)');
  D.type(u2, 'title', '画面で直した見出し');
  // TOP の本文 → 20% の注意
  ok(!D.shown(`lpe-warn-${u0}`), 'TOP の本文が空なら注意は出ない');
  D.type(u0, 'body', '説明');
  ok(D.shown(`lpe-warn-${u0}`), '🚨 TOP に本文を入れると「20%を超えるおそれ」');
  D.type(u0, 'body', '');
  ok(!D.shown(`lpe-warn-${u0}`), '消すと注意も消える');
  // 並べ替え
  ok(D.click('down', u2), '2枚目を下へ');
  eq(ui.state().slots.map((s) => s.uid), [u0, u1, u3, u2, u4], '並びが入れ替わる');
  ok(/<span class="lpe-no">3枚目<\/span><span class="lpe-role" id="lpe-role-[^"]+">[^<]*<\/span><span class="lpe-title" id="lpe-title-[^"]+"><b>画面で直した見出し/.test(H()), '動かした画像は 3枚目 と出る (番号を振り直す)');
  ok(D.click('up', u2), '戻す (上へ)');
  ok(D.click('down', u3) && !D.click('down', u3), '3 を一番下へ (一番下ではもう下へ押せない)');
  eq(ui.state().slots.map((s) => s.uid), [u0, u1, u2, u4, u3], '並べ替えの連続');
  // 削除 → 元に戻す
  ok(D.click('del', u4), '削除');
  ok(H().includes('LP構成（4枚）') && /「元に戻す」|data-act="undo"/.test(H()) && H().includes('を削除しました (まだ保存していません)'), '🚨 削除すると「元に戻す」が出る');
  ok(D.click('undo'), '元に戻す');
  eq(ui.state().slots.map((s) => s.uid), [u0, u1, u2, u4, u3], '🚨 消した位置に戻る');
  ok(!H().includes('data-act="undo"'), '戻したら「元に戻す」は消える');
  // 追加
  ok(D.click('add'), '＋ 画像を追加');
  const added = ui.state().slots[5];
  ok(added.uid === 'nui1' && added.kind === 'point' && added.title === '新しい画像' && H().includes('LP構成（6枚）'), '後ろに新しい画像 (n で始まる uid)');
  ok(D.reg.get('lpe-f-nui1-title').focused === 1, '足した画像の見出しの欄に入る (すぐ打てる)');
  D.type('nui1', 'role', '比較');
  D.type('nui1', 'title', '他のスプレーとの違い');
  // 要撮影の切り替え
  ok(D.click('shoot', u3) && ui.state().slots[4].shoot === true && /data-act="shoot" data-uid="[^"]+"[^>]*>要撮影</.test(H()), '撮影不要 → 要撮影 (押すと切り替え)');
  // 上限
  ok(D.click('add') && D.click('add') && D.click('add') && D.click('add'), 'さらに 4 枚 (10 枚)');
  ok(H().includes('LP構成（10枚）') && !D.click('add') && H().includes('画像は 10 枚までです'), '🚨 10 枚 (lint の上限) で追加できない');
  D.fire('click', { closest: () => ({ dataset: { act: 'add' } }) });
  ok(ui.state().slots.length === 10 && /10 枚まで/.test(ui.state().msg), '🚨 押された知らせが来ても 11 枚目は足さない (理由を出す)');
  ok(D.click('del', 'nui5') && D.click('del', 'nui4') && D.click('del', 'nui3') && D.click('del', 'nui2'), '足しすぎた分を消す');
  ok(D.click('undo') && ui.state().slots.length === 7, '元に戻せるのは最後に消した 1 つ');
  ok(D.click('del', 'nui2'), 'もう一度消す');
  // 保存 (本物の API へ)
  ok(D.click('save'), '「LP構成を保存」');
  await settle(ui);
  const sent = puts[puts.length - 1];
  eq(sent.slots.map((s) => s.uid), [u0, u1, u2, u4, u3, 'nui1'], '🚨 送る並び (uid・並び順)');
  ok(sent.base_job_id === st.base_job_id && sent.base_edit_id === st.base_edit_id, '見ていた版を添えて送る');
  ok(sent.slots[2].title === '画面で直した見出し' && sent.slots[4].shoot === true && sent.slots[5].role === '比較' && sent.slots[0].body === '', '送る本文 (直した文字・要撮影・足した画像)');
  ok(ui.state().msg === 'LP構成を保存しました' && !D.shown('lpe-dirty') && savedEvents === 1, '🚨 保存できたら印が消え、上の箱に知らせる');
  const eff = le.effectiveComposeText(db, R.draft.id);
  eq(headingsOf(eff.text), ['# 0枚目｜サムネイル', '# 1枚目｜FV', '# 2枚目｜使用シーン', '# 3枚目｜よくある質問', '# 4枚目｜成分', '# 5枚目｜比較'], '🚨 サーバの構成が画面の並びになる');
  ok(lintOk(eff.text).ok && eff.text.includes('他のスプレーとの違い') && eff.text.includes('画面で直した見出し'), '保存した構成は lint を通る');
  ok(ui.state().base_edit_id === eff.edit_id && ui.state().slots[5].uid === 'nui1', '読み直した状態 (新しい版・足した画像の uid はそのまま)');
  // 続けて直す → 2 回目の保存は新しい版から
  D.type('nui1', 'copy', 'ここが違う');
  ok(D.click('save'), '続けてもう一度保存');
  await settle(ui);
  ok(ui.state().msg === 'LP構成を保存しました' && le.effectiveComposeText(db, R.draft.id).text.includes('ここが違う'), '2 回目の保存も通る (新しい版から送る)');

  // 🚨 ほかの人が先に保存 → 409 → 読み直しを促す・黙って上書きしない
  const other = (await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json;
  const os = sendFrom(other); os[1].title = 'ほかの人の見出し';
  await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: other.base_job_id, base_edit_id: other.base_edit_id, slots: os } });
  D.type(u2, 'body', 'こちらの本文');
  ok(D.click('save'), '古い版のまま保存を押す');
  await settle(ui);
  ok(/保存できませんでした: ほかの人/.test(ui.state().msg) && D.shown('lpe-dirty') && H().includes('data-act="reload"'), '🚨 409: 理由と「読み直す」を出し、直しは残す');
  ok(le.effectiveComposeText(db, R.draft.id).text.includes('ほかの人の見出し') && !le.effectiveComposeText(db, R.draft.id).text.includes('こちらの本文'), '🚨 ほかの人の保存を上書きしない');
  confirmAnswer = false;
  ok(D.click('reload') && ui.state().slots[2].body === 'こちらの本文', '読み直しで「捨てますか」に いいえ → 直しは残る');
  confirmAnswer = true;
  D.click('reload');
  await settle(ui);
  ok(ui.state().slots[1].title === 'ほかの人の見出し' && !D.shown('lpe-dirty') && ui.state().msg === '読み直しました', '「はい」で読み直す (ほかの人の版になる)');

  // 通信が切れたが実は保存できていた → 確かめて「保存しました」
  const st3 = ui.state();
  const D3 = makeDom((await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json);
  const ui3 = F.initLpEdit(D3.doc, realDeps({
    put: async (u, body) => { await fetch(u, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); throw new Error('net'); },
  }));
  D3.type(st3.slots[2].uid, 'copy', '切れても保存できていた');
  D3.click('save');
  await settle(ui3);
  ok(ui3.state().msg === 'LP構成を保存しました' && !D3.shown('lpe-dirty'), '🚨 応答が来なくても、サーバの構成が送ったものと同じなら保存できていたとみなす');
  // 通信が切れて保存もできていない → 失敗と出し、直しは残す
  const D4 = makeDom((await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json);
  const ui4 = F.initLpEdit(D4.doc, realDeps({ put: async () => { throw new Error('net'); } }));
  D4.type(st3.slots[2].uid, 'copy', '届かなかった');
  D4.click('save');
  await settle(ui4);
  ok(/通信できませんでした/.test(ui4.state().msg) && D4.shown('lpe-dirty') && ui4.state().slots[2].copy === '届かなかった', '届かなかったら失敗と出し、直しは残す');
  // lint に落ちる → 理由を出す
  const D5 = makeDom((await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json);
  const ui5 = F.initLpEdit(D5.doc, realDeps());
  D5.type(st3.slots[3].uid, 'body', '# 見出しのつもり');
  D5.click('save');
  await settle(ui5);
  ok(/行の先頭に # は使えません/.test(ui5.state().msg) && D5.shown('lpe-dirty'), 'サーバに断られた理由を出す');

  // 新しい AI の構成ができた → 直していなければ読み直す / 直していれば知らせるだけ
  const N = compose(HAKKA, { draft: R.draft });
  const D6 = makeDom((await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json);
  F.initLpEdit(D6.doc, realDeps());
  // (D6 はもう新しい構成。古い構成を見ている画面として ui5 を使う)
  D5.type(st3.slots[3].uid, 'body', '');
  await ui5.composeChanged(N.jobId);
  ok(ui5.state().newer === true && D5.root.innerHTML.includes('新しい構成を読み込む'), '🚨 直している途中に AI が作り直したら、捨てずに知らせる');
  const D7 = makeDom(st3.base_job_id ? { ...(await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json, base_job_id: st3.base_job_id } : {});
  const ui7 = F.initLpEdit(D7.doc, realDeps());
  await ui7.composeChanged(N.jobId);
  ok(ui7.state().base_job_id === N.jobId && ui7.state().slots.length === 3, '直していなければ新しい構成を読み込む');
  ok(D6.root.innerHTML.includes('AI が作ったまま'), '新しい構成は「AI が作ったまま」');

  // 見るだけの人
  const ro = (await api('GET', `/api/drafts/${R.draft.id}/lp-edit`, { headers: AS_NOROLE })).json;
  const D8 = makeDom(ro);
  const ui8 = F.initLpEdit(D8.doc, realDeps());
  ok(!D8.reg.get('lpe-save') && D8.root.innerHTML.includes('見るだけです'), '見るだけの人には保存ボタンを出さない');
  ok(!D8.click('add') && !D8.click('del', ro.slots[2].uid) && !D8.click('shoot', ro.slots[0].uid) && !D8.type(ro.slots[1].uid, 'title', 'x'), '🚨 見るだけの人は押せない・打てない');
  // ボタンが押せない形でも、押された知らせが来たら (古い描画・細工) 何もしない
  D8.fire('click', { closest: () => ({ dataset: { act: 'del', uid: ro.slots[2].uid } }) });
  D8.fire('click', { closest: () => ({ dataset: { act: 'add' } }) });
  D8.fire('input', { dataset: { uid: ro.slots[1].uid, k: 'title' }, value: 'x' });
  eq(ui8.state().slots.map((s) => s.title), ro.slots.map((s) => s.title), '🚨 何も変わらない (押された知らせが来ても)');
  // 構成がまだ無い
  const D9 = makeDom({ ok: true, available: false, can_edit: true, max_images: 10 });
  F.initLpEdit(D9.doc, realDeps());
  ok(D9.root.hidden === true && D9.root.innerHTML === '', '構成がまだ無ければ隠す');

  // 純粋関数: 押せない操作は黙って何もしない
  const S = F.lpeFromServer({ available: true, can_edit: true, max_images: 10, slots: [{ uid: 'a0', kind: 'top' }, { uid: 'a1', kind: 'fv' }, { uid: 'a2', kind: 'point' }] }, null);
  ok(!F.lpeAction(S, 'del', 'a0') && !F.lpeAction(S, 'del', 'a1') && !F.lpeAction(S, 'up', 'a2') && !F.lpeAction(S, 'down', 'a1') && !F.lpeAction(S, 'undo'),
    'lpeAction: TOP / FV を消す・FV の上へ・FV を動かす・戻すものが無い は何もしない');
  ok(F.lpeAction(S, 'del', 'a2') && S.slots.length === 2 && F.lpeAction(S, 'undo') && S.slots.map((s) => s.uid).join() === 'a0,a1,a2', 'lpeAction: 削除 → 元に戻す');
}

console.log('⑩ 「AI が作った構成」の箱 (lpc) — 直した版を出し、コピー・lp-tool にもそれを渡す');
{
  const lpcStart = detailSrc.indexOf('  (function initLpCompose() {');
  const lpcEnd = detailSrc.indexOf('  /* @lp-edit:start');
  const lpcSrc = detailSrc.slice(lpcStart, lpcEnd);
  ok(lpcStart > 0 && lpcEnd > lpcStart && !lpcSrc.includes('<%'), 'lpc の JS を切り出せる');
  const shownSrc = lpcSrc.slice(lpcSrc.indexOf('// lpc-shown:start'), lpcSrc.indexOf('// lpc-shown:end'));
  const lpcShown = new Function(shownSrc + '\nreturn lpcShown;')();
  const job = { id: 7, status: 'done', output_text: 'AI の本文' };
  eq(lpcShown({ job }).text, 'AI の本文', '直した版が無ければ AI の本文');
  const e = { base_job_id: 7, edit_id: 3, output_text: '直した本文', edited_by: 'a@x', edited_at: '2026-10-09T01:02:03Z' };
  const sh = lpcShown({ job, lp_edit: e });
  ok(sh.text === '直した本文' && sh.edited && sh.note.includes('2026-10-09 01:02') && sh.note.includes('a@x'), '直した版があればそちら (いつ・誰)');
  eq(lpcShown({ job: { ...job, id: 8 }, lp_edit: e }).text, 'AI の本文', '🚨 別の構成 (作り直す前) への直しは使わない');
  eq(lpcShown({ job: { ...job, status: 'running' }, lp_edit: e }), null, 'できていなければ出さない');

  // 箱の JS を丸ごと偽の document で動かす
  const els = new Map();
  const el = (id) => {
    if (!els.has(id)) {
      const ls = {};
      els.set(id, { id, value: '', textContent: '', hidden: false, disabled: false, dataset: {}, style: {}, href: '',
        addEventListener: (t, fn) => { (ls[t] = ls[t] || []).push(fn); }, fire: async (t) => { for (const fn of ls[t] || []) await fn({}); }, select() {} });
    }
    return els.get(id);
  };
  const st = (await api('GET', `/api/drafts/${R.draft.id}/lp-compose`)).json;   // いまは新しい構成 N (直していない)
  // 古い構成 (R) に直した版がある状態を作る: 新しい構成 N を直す
  const cur = (await api('GET', `/api/drafts/${R.draft.id}/lp-edit`)).json;
  const s2 = sendFrom(cur); s2[1].title = '箱に出る直した見出し';
  await api('PUT', `/api/drafts/${R.draft.id}/lp-edit`, { body: { base_job_id: cur.base_job_id, base_edit_id: cur.base_edit_id, slots: s2 } });
  el('lpc-json').textContent = JSON.stringify(st);   // 開いたときは直す前
  el('lpc').dataset = { draftId: String(R.draft.id), neCode: R.draft.ne_code, name: NAME };
  const docLs = {}; const dispatched = []; const clip = [];
  const fakeDoc = {
    getElementById: el, hidden: false,
    addEventListener: (t, fn) => { (docLs[t] = docLs[t] || []).push(fn); },
    dispatchEvent: (ev) => { dispatched.push(ev); for (const fn of docLs[ev.type] || []) fn(ev); },
  };
  class FakeEvent { constructor(type, init) { this.type = type; this.detail = init && init.detail; } }
  const fakeFetch = async (u) => ({ json: async () => (await api('GET', u.replace('/apps/product-hub', ''))).json });
  new Function('document', 'navigator', 'fetch', 'CustomEvent', 'window', lpcSrc)(fakeDoc, { clipboard: { writeText: async (t) => { clip.push(t); } } }, fakeFetch, FakeEvent, { crypto: null });
  ok(el('lpc-text').value === HAKKA && el('lpc-edited').hidden === true, '開いたとき (直す前) は AI の本文');
  const last = dispatched.filter((d) => d.type === 'ph:lp-compose-state').pop();
  ok(last && last.detail.done === true && last.detail.job_id === st.job.id, '下の一覧に「構成ができている・どの構成か」を知らせる');
  // 下の一覧で保存した → 箱も読み直す
  for (const fn of docLs['ph:lp-edit-saved'] || []) fn({});
  await new Promise((r) => setTimeout(r, 300));
  ok(el('lpc-text').value.includes('箱に出る直した見出し') && el('lpc-edited').hidden === false && /人が直した版/.test(el('lpc-result-title').textContent),
    '🚨 保存の知らせで箱が直した版に替わる');
  await el('lpc-copy').fire('click');
  ok(clip[clip.length - 1].includes('箱に出る直した見出し'), '🚨 「コピー」(lp-tool に貼る文) は直した版');
  await el('lpc-copy-ai').fire('click');
  ok(clip[clip.length - 1] === HAKKA, '「AI の初稿をコピー」は AI の生の出力 (測定用)');
  ok(/通っています/.test(el('lpc-lint').textContent), 'lint の行も直した版の結果');

  // 「画像を作る」の箱 (lpi) も、保存の知らせで作る枚数・取り置きを取り直す (Codex PR-B 名指し M)
  const lpiSrc = detailSrc.slice(detailSrc.indexOf('  (function initLpImages() {'), detailSrc.indexOf('  (function initLpCompose() {'));
  ok(lpiSrc.length > 500 && !lpiSrc.includes('<%'), '画像の箱の JS を切り出せる');
  const P = compose(FIVE);
  db.prepare(`UPDATE ph_lp_compose_jobs SET packet_json = json_set(packet_json, '$.images', json('[]')) WHERE id = ?`).run(P.jobId);
  els.clear();
  el('lpi').dataset = { draftId: String(P.draft.id) };
  el('lpi-json').textContent = JSON.stringify((await api('GET', `/api/drafts/${P.draft.id}/lp-images`)).json);
  const docLs2 = {};
  const doc2 = { getElementById: el, hidden: false, createElement: () => ({ style: {}, appendChild() {} }),
    addEventListener: (t, fn) => { (docLs2[t] = docLs2[t] || []).push(fn); } };
  new Function('document', 'fetch', 'window', lpiSrc)(doc2, fakeFetch, { crypto: null, confirm: () => true });
  ok(/5 枚/.test(el('lpi-btn').textContent), '開いたときは 5 枚');
  const cp = (await api('GET', `/api/drafts/${P.draft.id}/lp-edit`)).json;
  await api('PUT', `/api/drafts/${P.draft.id}/lp-edit`, { body: { base_job_id: cp.base_job_id, base_edit_id: null, slots: sendFrom(cp).slice(0, 3) } });
  for (const fn of docLs2['ph:lp-edit-saved'] || []) fn({});
  await new Promise((r) => setTimeout(r, 300));
  ok(/3 枚/.test(el('lpi-btn').textContent), '🚨 LP構成を保存したら「画像を作る」の枚数が直した構成 (3 枚) になる', el('lpi-btn').textContent);
}

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
