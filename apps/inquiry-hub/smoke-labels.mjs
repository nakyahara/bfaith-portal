// 🏷️色付きラベル (labels.js + router /labels + 一覧絞込 + メールルール連携) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-labels.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-labels-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-labels-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { listLabels, createLabel, updateLabel, deleteLabel, setInquiryLabel, labelTextColor } = await import('./labels.js');
const { listInquiries, bulkUpdateInquiries } = await import('./queries.js');
const { addMailRule, evaluateMailRules, applyRuleToExistingMails, deleteMailRule, listMailRules } = await import('./mail-rules.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// テストデータ: 1店舗 + 問い合わせ3件 (新着2/完了1)
db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','テスト店','info@example.com')`).run();
const shopId = db.prepare('SELECT id FROM shops').get().id;
const mkInq = (ext, status, from = 'c@example.com') => db.prepare(`INSERT INTO inquiries
    (channel_type, shop_id, external_inquiry_id, subject, customer_identifier, internal_status, received_at, last_message_at)
  VALUES ('email', ?, ?, ?, ?, ?, '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')`)
  .run(shopId, ext, `件名 ${ext}`, from, status).lastInsertRowid;
const inqA = mkInq('t-a', 'open');
const inqB = mkInq('t-b', 'open');
const inqC = mkInq('t-c', 'done');

// ─── 1. CRUD + 色検証 ───
console.log('1. ラベルCRUD');
{
  const l1 = createLabel(' クレーム ', '#EF4444', 'tester');
  check('作成 (trim+色は小文字化)', l1.name === 'クレーム' && l1.color === '#ef4444');
  createLabel('電話', '#22c55e', 'tester');
  check('2件目も作成', listLabels().length === 2);

  let dup = null;
  try { createLabel('クレーム', '#000000'); } catch (e) { dup = e; }
  check('同名は作れない', dup !== null);
  let badColor = null;
  try { createLabel('別名', 'red'); } catch (e) { badColor = e; }
  check('不正な色 (#rrggbb以外) はthrow', badColor !== null);
  let empty = null;
  try { createLabel('  ', '#123456'); } catch (e) { empty = e; }
  check('空名はthrow', empty !== null);

  const l2 = listLabels().find(l => l.name === '電話');
  updateLabel(l2.id, { name: '電話対応', color: '#3b82f6', sortOrder: 5 });
  const sorted = listLabels();
  check('改名+色変更+並び順', sorted[0].name === '電話対応' && sorted[0].color === '#3b82f6');
  let badColor2 = null;
  try { updateLabel(l2.id, { color: 'javascript:alert(1)' }); } catch (e) { badColor2 = e; }
  check('更新でも色は厳格検証 (style注入防止)', badColor2 !== null);

  check('文字色: 明るい背景は黒', labelTextColor('#eab308') === '#1f2937');
  check('文字色: 暗い背景は白', labelTextColor('#ef4444') === '#fff');
}

// ─── 2. 割当・一覧絞込・一括操作 ───
console.log('2. 割当');
{
  const [tel, claim] = listLabels(); // sort順: 電話対応(5), クレーム(10)
  setInquiryLabel(inqA, claim.id, 'tester');
  setInquiryLabel(inqC, claim.id, 'tester');
  check('label_id が入る', db.prepare('SELECT label_id FROM inquiries WHERE id = ?').get(inqA).label_id === claim.id);

  const inbox = listInquiries({ view: 'inbox' });
  const rowA = inbox.rows.find(r => r.id === inqA);
  check('一覧行にラベル名+色が乗る', rowA.label_name === claim.name && rowA.label_color === claim.color, JSON.stringify(rowA));
  check('ラベル絞込 (ステータス問わず2件)', listInquiries({ view: 'all', label: String(claim.id) }).total === 2);
  check('ラベルなし絞込', listInquiries({ view: 'all', label: 'none' }).total === 1);

  setInquiryLabel(inqA, null, 'tester');
  check('null でラベルなしに戻せる', db.prepare('SELECT label_id FROM inquiries WHERE id = ?').get(inqA).label_id === null);
  const logs = db.prepare("SELECT * FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'label_change' ORDER BY id").all(inqA);
  check('操作ログが2件 (付与・解除)', logs.length === 2
    && JSON.parse(logs[0].after_json).label === claim.name
    && JSON.parse(logs[1].after_json).label === null, JSON.stringify(logs));
  let bad = null;
  try { setInquiryLabel(inqB, 999999, 'tester'); } catch (e) { bad = e; }
  check('存在しないラベルはthrow', bad !== null);

  // 一括操作でラベルをまとめて付与・解除
  const r1 = bulkUpdateInquiries([inqA, inqB], { labelId: tel.id }, { actorId: 'tester' });
  check('一括付与', r1.updated === 2
    && db.prepare('SELECT label_id FROM inquiries WHERE id = ?').get(inqB).label_id === tel.id);
  const r2 = bulkUpdateInquiries([inqA, inqB], { labelId: null }, { actorId: 'tester' });
  check('一括解除', r2.updated === 2);
}

// ─── 3. メールルール連携 (条件一致 → ラベル自動付与) ───
console.log('3. メールルール連携');
{
  const claim = listLabels().find(l => l.name === 'クレーム');
  // skip にラベルは指定できない / import はフォルダかラベルが必須
  let e1 = null;
  try { addMailRule({ conditions: [{ field: 'from', op: 'contains', value: 'x' }], action: 'skip', labelId: claim.id }); } catch (e) { e1 = e; }
  check('skip+ラベルは拒否', e1 !== null);
  let e2 = null;
  try { addMailRule({ conditions: [{ field: 'from', op: 'contains', value: 'x' }], action: 'import' }); } catch (e) { e2 = e; }
  check('import はフォルダかラベル必須', e2 !== null);
  check('import+ラベルのみはOK', addMailRule({
    name: 'クレームラベル', conditions: [{ field: 'subject', op: 'contains', value: '返金' }],
    action: 'import', labelId: claim.id, priority: 10,
  }).id > 0);

  const hit = evaluateMailRules({ from: 'a@b.com', subject: '返金してください', body: '' });
  check('評価結果に labelId が乗る', hit && hit.action === 'import' && hit.labelId === claim.id, JSON.stringify(hit));

  // 既存メールへの一括適用 (import+ラベル → ステータスは変えずラベルだけ付く)
  const t1 = mkInq('t-refund-1', 'open'); mkInq('t-refund-2', 'done');
  db.prepare("UPDATE inquiries SET subject = '返金希望' WHERE external_inquiry_id LIKE 't-refund-%'").run();
  const dry = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '返金' }],
    { matchMode: 'all', apply: false, action: 'import', labelId: claim.id });
  check('dry-run: 対象2件・変更なし', dry.matched === 2 && dry.labeled === 0
    && db.prepare('SELECT label_id FROM inquiries WHERE id = ?').get(t1).label_id === null, JSON.stringify(dry));
  const ap = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '返金' }],
    { matchMode: 'all', apply: true, action: 'import', labelId: claim.id });
  check('適用: 2件にラベル・ステータス不変', ap.labeled === 2 && ap.completed === 0
    && db.prepare('SELECT label_id, internal_status FROM inquiries WHERE id = ?').get(t1).label_id === claim.id
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(t1).internal_status === 'open', JSON.stringify(ap));
  const ap2 = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '返金' }],
    { matchMode: 'all', apply: true, action: 'import', labelId: claim.id });
  check('再適用は冪等 (0件)', ap2.matched === 0 && ap2.labeled === 0, JSON.stringify(ap2));

  // 「完了扱い+ラベル」の一括適用は、すでに完了済みのメールにもラベルを付ける
  // (2026-08-25 実運用: 完了済みだけが残っているとラベルが1件も付かなかった)
  const tDone = mkInq('t-refund-done', 'done');
  db.prepare("UPDATE inquiries SET subject = '返金希望2' WHERE id = ?").run(tDone);
  const apDone = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '返金希望2' }],
    { matchMode: 'all', apply: true, action: 'import_done', labelId: claim.id });
  check('完了済みメールにも一括適用でラベルが付く (完了化は0件)', apDone.labeled === 1 && apDone.completed === 0
    && db.prepare('SELECT label_id FROM inquiries WHERE id = ?').get(tDone).label_id === claim.id, JSON.stringify(apDone));

  // ラベル削除でルールのラベル参照も外れる (壊れたルールを残さない)
  const tmp = createLabel('一時', '#111111');
  const rid = addMailRule({ conditions: [{ field: 'from', op: 'contains', value: 'tmp' }], action: 'import', labelId: tmp.id }).id;
  const del = deleteLabel(tmp.id, 'tester');
  check('ラベル削除でルールから参照解除', del.rulesDetached === 1
    && listMailRules().find(r => r.id === rid).label_id === null, JSON.stringify(del));
  // ラベルが外れた import ルールは壊れたルール扱い (fail-open=通常取込)
  const probe = evaluateMailRules({ from: 'tmp@x.com', subject: '', body: '' });
  check('ラベル無しimportルールは評価から除外', !probe || probe.ruleId !== rid, JSON.stringify(probe));
  deleteMailRule(rid);
}

// ─── 4. HTTP (画面 + API) ───
console.log('4. HTTP');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/labels')).text();
  check('管理画面が出る', html.includes('新しいラベル名') && html.includes('色付きの目印'));
  check('色見本 (パレット) が出る', html.includes('class="swatch"'));

  const rNew = await jpost('/api/labels', { name: 'テスト作成', color: '#8b5cf6' });
  check('作成API', rNew.status === 200);
  const newId = (await rNew.json()).id;
  check('重複作成は400', (await jpost('/api/labels', { name: 'テスト作成', color: '#000000' })).status === 400);
  check('不正な色は400', (await jpost('/api/labels', { name: '色不正', color: 'red;x' })).status === 400);
  check('改名+色変更API', (await jpost(`/api/labels/${newId}`, { name: 'テスト改名', color: '#123456', sortOrder: 99 })).status === 200);

  const rAssign = await jpost(`/api/inquiries/${inqB}/label`, { label_id: newId });
  check('割当API', rAssign.status === 200 && (await rAssign.json()).label === 'テスト改名');
  check('不正な型は400', (await jpost(`/api/inquiries/${inqB}/label`, { label_id: 'abc' })).status === 400);
  check('存在しないIDは400', (await jpost(`/api/inquiries/${inqB}/label`, { label_id: 999999 })).status === 400);

  // 一覧: チップが色付きで出る + ラベル絞込select
  const list = await (await fetch(base + '/?view=all')).text();
  check('一覧: 状態の隣に「ラベル」列+色チップ (2026-08-25 メールディーラー同配置)',
    list.includes('<th>状態</th><th>ラベル</th>') && list.includes('class="lbl"') && list.includes('#123456'));
  check('一覧の絞込にラベルselect', list.includes('name="label"') && list.includes('ラベル: 全て'));

  // 詳細: select + h2チップ + メールルールのラベルselect
  const detail = await (await fetch(base + `/inquiries/${inqB}`)).text();
  check('詳細画面にラベルselect', detail.includes('labelSel'));
  check('詳細h2にチップ', detail.includes('class="lbl"'));
  check('今後の自動処理にラベルselect', detail.includes('mrLabel'));

  // メールルール手動追加API (labelId付き)
  const rRule = await jpost('/api/mail-rules', {
    name: 'API作成', priority: 40, matchMode: 'all', action: 'import', labelId: newId,
    conditions: [{ field: 'from', op: 'contains', value: 'label-api-test' }],
  });
  check('ルール追加API (labelId)', rRule.status === 200);
  const rulesPage = await (await fetch(base + '/mail-rules')).text();
  check('ルール一覧にラベルチップ+手動追加のラベルselect', rulesPage.includes('nLabel') && rulesPage.includes('テスト改名'));

  const rUnset = await jpost(`/api/inquiries/${inqB}/label`, { label_id: null });
  check('null でラベルなしに戻せる (API)', rUnset.status === 200 && (await rUnset.json()).label === null);
  const rDel = await jpost(`/api/labels/${newId}/delete`, {});
  check('削除API', rDel.status === 200);
  check('削除の再送も200 (冪等)', (await jpost(`/api/labels/${newId}/delete`, {})).status === 200);
  check('削除済みの改名は400', (await jpost(`/api/labels/${newId}`, { name: 'x' })).status === 400);

  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
