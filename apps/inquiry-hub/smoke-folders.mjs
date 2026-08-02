// 📁任意フォルダ (folders.js + router /folders + 一覧絞込) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-folders.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-folders-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-folders-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { listFolders, countUnfiled, createFolder, updateFolder, deleteFolder, setInquiryFolder } = await import('./folders.js');
const { listInquiries } = await import('./queries.js');
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
const mkInq = (ext, status) => db.prepare(`INSERT INTO inquiries
    (channel_type, shop_id, external_inquiry_id, subject, internal_status, received_at, last_message_at)
  VALUES ('email', ?, ?, ?, ?, '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')`)
  .run(shopId, ext, `件名 ${ext}`, status).lastInsertRowid;
const inqA = mkInq('t-a', 'open');
const inqB = mkInq('t-b', 'open');
const inqC = mkInq('t-c', 'done');

// ─── 1. CRUD ───
console.log('1. フォルダCRUD');
{
  const f1 = createFolder(' 返品・交換 ', 'tester');
  check('作成 (前後空白はtrim)', f1.name === '返品・交換');
  const f2 = createFolder('クレーム対応', 'tester');
  check('2件目も作成', listFolders().length === 2);

  let dup = null;
  try { createFolder('返品・交換'); } catch (e) { dup = e; }
  check('同名は作れない', dup !== null);

  let empty = null;
  try { createFolder('   '); } catch (e) { empty = e; }
  check('空名はthrow', empty !== null);

  let long = null;
  try { createFolder('あ'.repeat(41)); } catch (e) { long = e; }
  check('41文字はthrow (上限40)', long !== null);

  updateFolder(f2.id, { name: 'クレーム', sortOrder: 5 });
  const sorted = listFolders();
  check('改名+並び順が効く (sort_order昇順)', sorted[0].name === 'クレーム' && sorted[1].name === '返品・交換');

  let dupRename = null;
  try { updateFolder(f2.id, { name: '返品・交換' }); } catch (e) { dupRename = e; }
  check('改名で同名衝突もthrow', dupRename !== null);
}

// ─── 2. 割当と「受信トレイに残る」保証 ───
console.log('2. 割当');
{
  const [claim, ret] = listFolders();
  setInquiryFolder(inqA, claim.id, 'tester');
  setInquiryFolder(inqC, claim.id, 'tester');
  check('folder_id が入る', db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(inqA).folder_id === claim.id);
  check('未分類件数が減る', countUnfiled() === 1);

  const counts = listFolders({ withCounts: true });
  const c = counts.find(f => f.id === claim.id);
  check('件数: total=2 / 未対応=1 (完了は未対応に数えない)', c.total === 2 && c.open_count === 1, JSON.stringify(c));

  // ⭐ 中原さん確認事項: フォルダに入れても受信トレイからは消えない
  const inbox = listInquiries({ view: 'inbox' });
  check('フォルダに入れた未返信も受信トレイに残る', inbox.rows.some(r => r.id === inqA), JSON.stringify(inbox.rows.map(r => r.id)));
  check('一覧行にフォルダ名が乗る', inbox.rows.find(r => r.id === inqA).folder_name === claim.name);

  const inFolder = listInquiries({ view: 'all', folder: String(claim.id) });
  check('フォルダ絞込 (ステータス問わず2件)', inFolder.total === 2);
  const inFolderInbox = listInquiries({ view: 'inbox', folder: String(claim.id) });
  check('フォルダ×ビューはAND (新着のみ1件)', inFolderInbox.total === 1);
  check('未分類の絞込', listInquiries({ view: 'all', folder: 'none' }).total === 1);

  setInquiryFolder(inqA, null, 'tester');
  check('null で未分類に戻せる', db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(inqA).folder_id === null);
  const logs = db.prepare("SELECT * FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'folder_change' ORDER BY id").all(inqA);
  check('操作ログが2件 (割当・解除)', logs.length === 2
    && JSON.parse(logs[0].after_json).folder === claim.name
    && JSON.parse(logs[1].before_json).folder === claim.name
    && JSON.parse(logs[1].after_json).folder === null, JSON.stringify(logs));

  let badFolder = null;
  try { setInquiryFolder(inqB, 999999, 'tester'); } catch (e) { badFolder = e; }
  check('存在しないフォルダはthrow', badFolder !== null);
  setInquiryFolder(inqA, ret.id, 'tester');
}

// ─── 3. 削除 (中身は消えず未分類に戻る) ───
console.log('3. 削除');
{
  const ret = listFolders().find(f => f.name === '返品・交換');
  const r = deleteFolder(ret.id, 'tester');
  check('削除で中身を未分類へ (detached件数)', r.detached === 1, JSON.stringify(r));
  check('問い合わせ自体は残る', db.prepare('SELECT COUNT(*) AS c FROM inquiries').get().c === 3);
  check('一覧から消える (論理削除)', listFolders().every(f => f.name !== '返品・交換'));
  check('includeInactiveなら見える', listFolders({ includeInactive: true }).some(f => f.name === '返品・交換'));
  check('削除済みと同じ名前は作り直せる', createFolder('返品・交換').id > 0);

  const again = deleteFolder(ret.id, 'tester');
  check('削除済みの再削除は成功扱い (冪等)', again.alreadyDeleted === true && again.detached === 0);
  let missing = null;
  try { deleteFolder(999999, 'tester'); } catch (e) { missing = e; }
  check('存在しないIDの削除はthrow', missing !== null);
}

// ─── 4. HTTP (画面 + API) ───
console.log('4. HTTP');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/folders')).text();
  check('管理画面が出る', html.includes('新しいフォルダ名') && html.includes('未分類'));
  check('サイドバーにフォルダが並ぶ', html.includes('📁') && html.includes('クレーム'));
  check('「受信トレイにも残る」旨の説明がある', html.includes('受信トレイにも残ります'));

  const rNew = await jpost('/api/folders', { name: 'テスト作成' });
  check('作成API', rNew.status === 200);
  const newId = (await rNew.json()).id;
  check('重複作成は400', (await jpost('/api/folders', { name: 'テスト作成' })).status === 400);
  check('空名は400', (await jpost('/api/folders', { name: '' })).status === 400);

  check('改名API', (await jpost(`/api/folders/${newId}`, { name: 'テスト改名', sortOrder: 99 })).status === 200);

  const rAssign = await jpost(`/api/inquiries/${inqB}/folder`, { folder_id: newId });
  check('割当API', rAssign.status === 200 && (await rAssign.json()).folder === 'テスト改名');
  check('不正な型は400', (await jpost(`/api/inquiries/${inqB}/folder`, { folder_id: 'abc' })).status === 400);
  check('存在しないIDは400', (await jpost(`/api/inquiries/${inqB}/folder`, { folder_id: 999999 })).status === 400);
  const rUnset = await jpost(`/api/inquiries/${inqB}/folder`, { folder_id: null });
  check('null で未分類に戻せる (API)', rUnset.status === 200 && (await rUnset.json()).folder === null);

  const detail = await (await fetch(base + `/inquiries/${inqA}`)).text();
  check('詳細画面にフォルダselect', detail.includes('folderSel') && detail.includes('フォルダ'));

  const list = await (await fetch(base + '/?view=all&folder=' + newId)).text();
  check('一覧: フォルダビューの見出し', list.includes('このフォルダに入れた問い合わせ'));
  const listUnfiled = await (await fetch(base + '/?view=all&folder=none')).text();
  check('一覧: 未分類ビュー', listUnfiled.includes('フォルダに入れていない問い合わせ'));

  const rDel = await jpost(`/api/folders/${newId}/delete`, {});
  check('削除API', rDel.status === 200);
  check('削除の再送も200 (冪等)', (await jpost(`/api/folders/${newId}/delete`, {})).status === 200);
  check('削除済みの改名は400', (await jpost(`/api/folders/${newId}`, { name: 'x' })).status === 400);

  // 表示順の検証 (APIから直接壊れた値を送っても保存しない)
  check('表示順が小数は400', (await jpost(`/api/folders/${listFolders()[0].id}`, { sortOrder: 1.5 })).status === 400);
  check('表示順が範囲外は400', (await jpost(`/api/folders/${listFolders()[0].id}`, { sortOrder: -1 })).status === 400);
  // 大文字小文字・全角半角ちがいの同名も弾く
  await jpost('/api/folders', { name: 'Returns' });
  check('大文字小文字ちがいの同名は400', (await jpost('/api/folders', { name: 'returns' })).status === 400);
  check('全角/半角ちがいの同名も400 (NFKC正規化)', (await jpost('/api/folders', { name: 'Ｒｅｔｕｒｎｓ' })).status === 400);

  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
