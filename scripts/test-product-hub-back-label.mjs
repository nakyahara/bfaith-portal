import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 🆕 商品登録 (product-hub) 側の「パッケージ裏面の写真」(2026-09-18 中原さん指示)
 *
 * 見るところ:
 *   ①入荷受付チェックで撮った写真が、正しいドラフトに結びつくか
 *     (写真は届いた子SKU で保存され、ドラフトは代表商品コードで作られる)
 *   ②他の商品の写真を覗けないか (配信 API の認可)
 *   ③工程ボードのバッジ (1クエリで全カードぶん)
 *   ④AI文字起こし: 写真を渡せるか / 読めない箇所を数えるか / **保存しない**か
 *
 * 実行: node scripts/test-product-hub-back-label.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ph-bl-test-'));
}

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();

const { getDB } = await import('../apps/product-hub/db.js');
const { createTables } = await import('../apps/inbound-check/db.js');
const bl = await import('../apps/inbound-check/back-label.js');
const svc = await import('../apps/product-hub/services/back-label-photos.js');
const ocr = await import('../apps/product-hub/services/back-label-ocr.js');

const db = getDB();
createTables(db);   // 入荷受付チェック側の表 (写真の正本) を用意する
const now = new Date().toISOString();

// ─── 種まき ───
// 代表商品コード parent (バリエーション) と、その子SKU 2つ。現物は子SKU で届く
const insProd = db.prepare(`INSERT INTO mirror_products
  (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?,?,?,'単品','取扱中','unknown',?,?)`);
insProd.run(1, 'parent', '親 (代表)', 'parent', now);
insProd.run(2, 'child-a', '子A (赤)', 'parent', now);
insProd.run(3, 'child-b', '子B (青)', 'parent', now);
insProd.run(4, 'solo', '単品商品', null, now);
insProd.run(5, 'other', 'よその商品', null, now);

const insDraft = db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'test')`);
const parentId = Number(insDraft.run('parent', '親 (代表)').lastInsertRowid);
const soloId = Number(insDraft.run('solo', '単品商品').lastInsertRowid);
const otherId = Number(insDraft.run('other', 'よその商品').lastInsertRowid);
const draftOf = (id) => db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);

const makeJpeg = (name) => {
  const dir = path.join(process.env.DATA_DIR, 'incoming');
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, name);
  // 中身に目印を入れて、どの写真が AI に渡ったか確かめられるようにする
  fs.writeFileSync(p, Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.from(name.padEnd(64, ' '))]));
  return p;
};
const shoot = (code, opId, file) => bl.addPhoto({
  codeKey: code, productId: code, productName: code, filePath: makeJpeg(file), operationId: opId, worker: 'テスト',
});

console.log('[1] 撮った写真がドラフトに結びつく');
{
  ok(svc.backLabelsAvailable(db) === true, '入荷受付チェックの表があれば使える');
  ok(svc.backLabelPhotosForDraft(db, draftOf(soloId)).length === 0, 'まだ撮っていなければ0枚');
  ok(shoot('solo', 'ph-solo0001', 's1.jpg').ok === true, '単品の写真を撮る');
  const solo = svc.backLabelPhotosForDraft(db, draftOf(soloId));
  ok(solo.length === 1 && solo[0].product_id === 'solo', '単品はそのまま結びつく');
  ok(solo[0].status === 'stored' && solo[0].drive_url === null, 'Drive へ送る前でも画面に出す (見られるので)');
}

console.log('[2] バリエーションは子SKU で撮っても代表のドラフトに出る');
{
  ok(shoot('child-a', 'ph-childa01', 'ca.jpg').ok === true, '子A の写真を撮る');
  ok(shoot('child-b', 'ph-childb01', 'cb.jpg').ok === true, '子B の写真を撮る');
  const p = svc.backLabelPhotosForDraft(db, draftOf(parentId));
  ok(p.length === 2, '代表のドラフトに子SKU 2枚とも出る (現物は子SKU で届く)');
  ok(p[0].created_at >= p[1].created_at, '新しい順に並ぶ');
  ok(p.every((x) => x.product_id !== 'parent'), 'どの子SKU を撮ったか分かる');
}

console.log('[3] 取込が覚えたコードからも引く (ph_ne_seen_codes)');
{
  db.prepare(`INSERT INTO ph_ne_seen_codes (code_key, ne_code, draft_id) VALUES ('seen-x', 'seen-x', ?)`).run(soloId);
  ok(shoot('seen-x', 'ph-seenx001', 'sx.jpg').ok === true, '自動取込が覚えたコードで撮る');
  ok(svc.backLabelPhotosForDraft(db, draftOf(soloId)).length === 2, '代表コードに寄る前の本コードでも結びつく');
}

console.log('[4] 他の商品の写真は覗けない (配信 API の認可)');
{
  const soloPhotos = svc.backLabelPhotosForDraft(db, draftOf(soloId));
  const parentPhotos = svc.backLabelPhotosForDraft(db, draftOf(parentId));
  ok(svc.photoBelongsToDraft(db, draftOf(soloId), soloPhotos[0].id) === true, '自分の写真は開ける');
  ok(svc.photoBelongsToDraft(db, draftOf(otherId), soloPhotos[0].id) === false, 'よその商品からは開けない');
  ok(svc.photoBelongsToDraft(db, draftOf(soloId), parentPhotos[0].id) === false, '代表の写真も別ドラフトからは開けない');
  ok(svc.photoBelongsToDraft(db, draftOf(soloId), 99999) === false, '存在しない id は開けない');
  ok(svc.photoBelongsToDraft(db, draftOf(soloId), 'abc') === false, '数字でない id は開けない');
}

console.log('[5] 消した写真・実体を失った写真は出さない');
{
  const before = svc.backLabelPhotosForDraft(db, draftOf(parentId));
  bl.deletePhoto(before[0].id, { actor: 'テスト' });
  ok(svc.backLabelPhotosForDraft(db, draftOf(parentId)).length === before.length - 1, '撮り直しで消した写真は出ない');
  ok(svc.photoBelongsToDraft(db, draftOf(parentId), before[0].id) === false, '消した写真は開けない');
  // 実体を失った写真 (入荷側が印を付けたもの) も出さない
  const rest = svc.backLabelPhotosForDraft(db, draftOf(parentId));
  db.prepare('UPDATE f_inbound_check_back_labels SET missing_file_at = ? WHERE id = ?').run(now, rest[0].id);
  ok(svc.backLabelPhotosForDraft(db, draftOf(parentId)).length === rest.length - 1, '見られない写真は出ない (壊れた画像を並べない)');
}

console.log('[6] 工程ボードのバッジ (1クエリで全カードぶん)');
{
  // [5] で親の写真は全部 (消した / 実体を失った) になったので、撮り直しておく
  ok(shoot('child-a', 'ph-childa02', 'ca2.jpg').ok === true, '子A を撮り直す');
  const m = svc.backLabelCountsByGroup(db);
  ok(m.get('parent') >= 1, '子SKU で撮った写真も代表コードで数える (カードは代表コード)');
  ok(m.get('solo') >= 1, '単品も数える');
  ok(!m.has('other'), '撮っていない商品は入らない');
  // ⭐バッジと詳細で食い違わせない (Codex PR2 #3)。取込履歴で solo に紐づけたコードは
  //   詳細に出る = バッジも solo に数える (seen-x 単独では数えない)
  ok(!m.has('seen-x'), '取込履歴で寄せたコードは、そのコードでは数えない');
  ok(m.get('solo') === svc.backLabelPhotosForDraft(db, draftOf(soloId)).length,
    'バッジの枚数と詳細に出る枚数が一致する');
}

console.log('[9] バリエーションから外した SKU の写真は親に混ざらない (Codex PR2 #1)');
{
  // child-b を親から外す = 単独ページになった SKU
  ok(shoot('child-b', 'ph-childb02', 'cb2.jpg').ok === true, '外す前に子B を撮っておく');
  const beforeCount = svc.backLabelPhotosForDraft(db, draftOf(parentId)).length;
  const bPhoto = svc.backLabelPhotosForDraft(db, draftOf(parentId)).find((x) => x.product_id === 'child-b');
  ok(!!bPhoto, '外す前は親に出る');
  db.prepare('INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, ?, ?)')
    .run(parentId, 'child-b', 'テスト');
  const after = svc.backLabelPhotosForDraft(db, draftOf(parentId));
  ok(after.length === beforeCount - 1 && after.every((x) => x.product_id !== 'child-b'),
    '外したら親の一覧から消える (外した SKU は単独ページなので親の写真ではない)');
  ok(svc.photoBelongsToDraft(db, draftOf(parentId), bPhoto.id) === false, '外した SKU の写真は親からは開けない');
  const m = svc.backLabelCountsByGroup(db);
  ok((m.get('child-b') || 0) >= 1, 'バッジも「外した SKU 自身」に付く (親には付かない)');
}

console.log('[10] 代表コードが変わった後の取込履歴は信じない (Codex PR2 #1)');
{
  // 取込履歴では old-draft に紐づいているが、いまの商品マスタでは parent の子になっているコード
  insProd.run(6, 'moved', '移った商品', 'parent', now);
  const oldId = Number(insDraft.run('old-draft', '昔のドラフト').lastInsertRowid);
  db.prepare(`INSERT INTO ph_ne_seen_codes (code_key, ne_code, draft_id) VALUES ('moved', 'moved', ?)`).run(oldId);
  ok(shoot('moved', 'ph-moved001', 'mv.jpg').ok === true, '移った商品を撮る');
  ok(svc.backLabelPhotosForDraft(db, draftOf(oldId)).every((x) => x.product_id !== 'moved'),
    '取込履歴が古くても、いまの所属と違えば出さない (別商品の写真を混ぜない)');
  ok(svc.backLabelPhotosForDraft(db, draftOf(parentId)).some((x) => x.product_id === 'moved'),
    'いまの代表コードのドラフトには出る');
  ok(svc.photoBelongsToDraft(db, draftOf(oldId), svc.backLabelPhotosForDraft(db, draftOf(parentId)).find((x) => x.product_id === 'moved').id) === false,
    '古いドラフトからは開けない');
}

console.log('[7] AI文字起こし');
{
  ok(ocr.backLabelOcrEnabled({}) === false, 'OPENAI_API_KEY が無ければ使えない (ボタンも出さない)');
  ok(ocr.backLabelOcrEnabled({ OPENAI_API_KEY: 'k' }) === true, 'あれば使える');
  let sent = null;
  const fakeFetch = async (url, opt) => {
    sent = JSON.parse(opt.body);
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: '【原材料】砂糖、' + ocr.UNREADABLE_MARK + '、食塩\n【内容量】100g' } }] }) };
  };
  const r = await ocr.transcribeBackLabel({
    images: [{ buffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg' }, { buffer: Buffer.from([0x89, 0x50]), mime: 'image/png' }],
    productName: 'テスト商品', env: { OPENAI_API_KEY: 'k' }, fetchImpl: fakeFetch,
  });
  ok(r.imageCount === 2, '2枚まとめて読ませる (裏面 + 側面の成分表)');
  ok(/【原材料】/.test(r.text), '読み取った文章を返す');
  ok(r.unreadable === 1, '読めなかった箇所を数える (人に確かめてもらうため)');
  const imgs = sent.messages[1].content.filter((c) => c.type === 'image_url');
  ok(imgs.length === 2 && imgs[0].image_url.url.startsWith('data:image/jpeg;base64,'), '写真を data URL で渡す');
  ok(imgs[1].image_url.url.startsWith('data:image/png;base64,'), '種類は写真ごとに正しく付ける');
  ok(/推測/.test(sent.messages[0].content) && sent.messages[0].content.includes(ocr.UNREADABLE_MARK),
    '「書かれていないことは書かない・読めない箇所は印」を指示している');
  ok(!/テスト商品/.test(sent.messages[0].content) && /テスト商品/.test(sent.messages[1].content[0].text),
    '商品名は参考情報として渡す (書き写す本文には入れさせない)');

  // 読み取った文章は**保存しない** (画面が受け取って人が確かめる)
  const ip = db.prepare('SELECT back_info_text FROM draft_image_production WHERE draft_id = ?').get(soloId);
  ok(!ip || !ip.back_info_text, '文字起こしだけでは裏面情報を書き換えない');
}

console.log('[8] AI文字起こしのエラーは画面に出せる日本語になる');
{
  const err = async (status, msg) => {
    try {
      await ocr.transcribeBackLabel({
        images: [{ buffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg' }],
        env: { OPENAI_API_KEY: 'k' },
        fetchImpl: async () => ({ ok: false, status, json: async () => ({ error: { message: msg } }) }),
      });
      return null;
    } catch (e) { return e.message; }
  };
  ok(/認証に失敗/.test(await err(401, 'bad key')), '401 は鍵の話だと分かる');
  ok(/混み合って/.test(await err(429, 'rate')), '429 は待てば直ると分かる');
  ok(/モデルが見つかりません/.test(await err(404, 'no model')), '404 はモデル名の話だと分かる');
  let thrown = null;
  try {
    await ocr.transcribeBackLabel({ images: [], env: { OPENAI_API_KEY: 'k' } });
  } catch (e) { thrown = e.message; }
  ok(/写真がありません/.test(thrown || ''), '写真が無ければ叩きに行かない');
  thrown = null;
  try {
    await ocr.transcribeBackLabel({ images: [{ buffer: Buffer.from([0xFF]), mime: 'image/jpeg' }], env: {} });
  } catch (e) { thrown = e.message; }
  ok(/未設定/.test(thrown || ''), '未設定なら叩きに行かない');
}

console.log('[11] AI文字起こし: 上限と途中切れ (Codex PR2 #2 / #6)');
{
  const big = { buffer: Buffer.alloc(ocr.MAX_IMAGE_BYTES + 1, 0xFF), mime: 'image/jpeg' };
  let thrown = null;
  try { await ocr.transcribeBackLabel({ images: [big], env: { OPENAI_API_KEY: 'k' } }); } catch (e) { thrown = e.message; }
  ok(/大きすぎ/.test(thrown || ''), '1枚が大きすぎれば叩きに行かない');
  const each = Math.floor(ocr.MAX_TOTAL_BYTES / 3) + 1024;
  const four = [0, 1, 2, 3].map(() => ({ buffer: Buffer.alloc(each, 0xFF), mime: 'image/jpeg' }));
  thrown = null;
  try { await ocr.transcribeBackLabel({ images: four, env: { OPENAI_API_KEY: 'k' } }); } catch (e) { thrown = e.message; }
  ok(/合計が大きすぎ/.test(thrown || ''), '合計が大きすぎれば叩きに行かない (メモリと費用の暴走を止める)');

  // 出力上限で途中で切れた応答は「切れた」と返す
  const cut = await ocr.transcribeBackLabel({
    images: [{ buffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg' }],
    env: { OPENAI_API_KEY: 'k' },
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ choices: [{ finish_reason: 'length', message: { content: '【原材料】砂糖、食' } }] }) }),
  });
  ok(cut.truncated === true, '途中で切れた応答は truncated で返す (完成した下書きに見せない)');
  const whole = await ocr.transcribeBackLabel({
    images: [{ buffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg' }],
    env: { OPENAI_API_KEY: 'k' },
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ choices: [{ finish_reason: 'stop', message: { content: '【原材料】砂糖' } }] }) }),
  });
  ok(whole.truncated === false, '最後まで書けた応答は truncated にしない');
}

console.log('[12] 写真に書かれた「指示」に従わせない (プロンプトインジェクション)');
{
  let sent = null;
  await ocr.transcribeBackLabel({
    images: [{ buffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg' }],
    env: { OPENAI_API_KEY: 'k' },
    fetchImpl: async (url, opt) => { sent = JSON.parse(opt.body);
      return { ok: true, status: 200, json: async () => ({ choices: [{ finish_reason: 'stop', message: { content: 'x' } }] }) }; },
  });
  ok(/指示として従わず/.test(sent.messages[0].content),
    '写真に写った指示らしき文は「印刷された文字」として書き写すだけ、と指示している');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} PASS ${pass} / FAIL ${fail}`);
process.exit(fail === 0 ? 0 : 1);
