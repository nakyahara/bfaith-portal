// 🔗クイックリンク (links.js + router /links + 一覧上部バー) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-links.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-links-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-links-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { listQuickLinks, createQuickLink, updateQuickLink, deleteQuickLink,
  normalizeLinkUrl, normalizeLinkIcon, MAX_ACTIVE_LINKS } = await import('./links.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── 1. URL・アイコンの検証 ───
console.log('1. 入力の検証');
{
  check('https はそのまま', normalizeLinkUrl('https://rmesse.rms.rakuten.co.jp/') === 'https://rmesse.rms.rakuten.co.jp/');
  check('スキーム無しは https:// を補う', normalizeLinkUrl('rmesse.rms.rakuten.co.jp/').startsWith('https://rmesse'));
  check('http も可', normalizeLinkUrl('http://example.com/x').startsWith('http://'));
  for (const bad of ['javascript:alert(1)', 'data:text/html,<script>', 'ftp://example.com', '   ']) {
    let e = null;
    try { normalizeLinkUrl(bad); } catch (err) { e = err; }
    check(`不正なURLは拒否: ${bad.slice(0, 20)}`, e !== null);
  }
  check('アイコン既定は🔗', normalizeLinkIcon('') === '🔗');
  check('絵文字はそのまま', normalizeLinkIcon('🛍️') === '🛍️');
  let eIcon = null;
  try { normalizeLinkIcon('あいうえおか'); } catch (e) { eIcon = e; }
  check('長すぎるアイコンは拒否', eIcon !== null);
  let eName = null;
  try { createQuickLink({ name: '  ', url: 'https://example.com' }); } catch (e) { eName = e; }
  check('空の名前は拒否', eName !== null);
}

// ─── 2. CRUD ───
console.log('2. CRUD');
{
  const l1 = createQuickLink({ name: ' ネクストエンジン ', url: 'https://main.next-engine.com/', icon: '📦' }, 'tester');
  check('作成 (前後空白はtrim)', l1.name === 'ネクストエンジン' && l1.icon === '📦');
  const l2 = createQuickLink({ name: 'ロジザード', url: 'zero.logizard.jp/' }, 'tester');
  check('スキーム補完+アイコン既定', l2.url.startsWith('https://zero.logizard.jp') && l2.icon === '🔗');
  check('一覧は表示順', listQuickLinks().map(l => l.name).join(',') === 'ネクストエンジン,ロジザード');

  updateQuickLink(l2.id, { name: 'ロジザードZERO', sortOrder: 5, icon: '🏬' });
  check('更新+並び替え', listQuickLinks()[0].name === 'ロジザードZERO' && listQuickLinks()[0].icon === '🏬');
  let eBadUpd = null;
  try { updateQuickLink(l2.id, { url: 'javascript:alert(1)' }); } catch (e) { eBadUpd = e; }
  check('更新でもURLは厳格検証', eBadUpd !== null
    && listQuickLinks().find(l => l.id === l2.id).url.startsWith('https://'));

  const d = deleteQuickLink(l2.id);
  check('削除 (論理削除)', d.name === 'ロジザードZERO' && listQuickLinks().length === 1);
  check('再削除は成功扱い (冪等)', deleteQuickLink(l2.id).alreadyDeleted === true);
  check('includeInactiveなら見える', listQuickLinks({ includeInactive: true }).length === 2);
  let eMissing = null;
  try { deleteQuickLink(999999); } catch (e) { eMissing = e; }
  check('存在しないIDはthrow', eMissing !== null);

  // 上限
  for (let i = listQuickLinks().length; i < MAX_ACTIVE_LINKS; i++) createQuickLink({ name: `L${i}`, url: `https://example.com/${i}` });
  let eMax = null;
  try { createQuickLink({ name: '溢れ', url: 'https://example.com/over' }); } catch (e) { eMax = e; }
  check(`上限${MAX_ACTIVE_LINKS}件を超えたら拒否`, eMax !== null);
  // 後片付け (上部バーのテストが読みやすいように既定+1件だけ残す)
  listQuickLinks().filter(l => /^L\d+$/.test(l.name)).forEach(l => deleteQuickLink(l.id));
}

// ─── 3. 既定リンクの投入 (テーブル新規作成時のみ) ───
console.log('3. 既定リンク');
{
  // 別DATA_DIRで、店舗を作った後にDBを作り直すと既定リンクが入る…のではなく、
  // 「テーブルが無いとき」に入る。ここでは投入ロジックの結果を直接確認する
  const dir2 = fs.mkdtempSync(path.join(baseDir, 'smoke-links-seed-'));
  const prev = process.env.DATA_DIR;
  process.env.DATA_DIR = dir2;
  const db2mod = await import('./db.js?seed=1');
  db2mod.initInquiryHubDB();
  const db2 = db2mod.getDB();
  check('店舗が無ければ既定リンクは入らない', db2.prepare('SELECT COUNT(*) AS c FROM inquiry_quick_links').get().c === 0);
  // 店舗を入れてテーブルを消す → 再初期化で既定が入る (本番の初回デプロイ相当)
  db2.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','373343')").run();
  db2.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('yahoo','Y店','b-faith01')").run();
  db2.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run();
  db2.exec('DROP TABLE inquiry_quick_links');
  db2mod.initInquiryHubDB();
  const seeded = db2mod.getDB().prepare('SELECT * FROM inquiry_quick_links ORDER BY sort_order').all();
  check('既定3件 (楽天R-Messe / Yahoo!ストア別URL / Gmail)', seeded.length === 3
    && seeded[0].url === 'https://rmesse.rms.rakuten.co.jp/'
    && seeded[1].url === 'https://pro.store.yahoo.co.jp/pro.b-faith01'
    && seeded[2].url === 'https://mail.google.com/', JSON.stringify(seeded.map(s => s.url)));
  // 2回目の初期化では増えない (既存テーブルには触らない)
  db2mod.initInquiryHubDB();
  check('再初期化しても重複投入しない', db2mod.getDB().prepare('SELECT COUNT(*) AS c FROM inquiry_quick_links').get().c === 3);
  // 全部消しても復活しない (画面での編集が正)
  db2mod.getDB().prepare('UPDATE inquiry_quick_links SET is_active = 0').run();
  db2mod.initInquiryHubDB();
  check('全削除後も復活しない', db2mod.getDB().prepare('SELECT COUNT(*) AS c FROM inquiry_quick_links WHERE is_active = 1').get().c === 0);
  db2mod.getDB().close();
  process.env.DATA_DIR = prev;
  fs.rmSync(dir2, { recursive: true, force: true });
}

// ─── 4. HTTP (画面 + API + 上部バー) ───
console.log('4. HTTP');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/links')).text();
  check('管理画面が出る', html.includes('リンクを追加') && html.includes('上部にそのまま並びます'));
  check('サイドバーに「リンクを作る・編集」', html.includes('/apps/inquiry-hub/links') && html.includes('リンクを作る・編集'));

  const rNew = await jpost('/api/links', { name: 'テスト追加', url: 'https://example.com/test', icon: '🧪' });
  check('追加API', rNew.status === 200);
  const newId = (await rNew.json()).id;
  check('不正URLは400', (await jpost('/api/links', { name: 'x', url: 'javascript:alert(1)' })).status === 400);
  check('空名は400', (await jpost('/api/links', { name: '', url: 'https://example.com' })).status === 400);
  check('更新API', (await jpost(`/api/links/${newId}`, { name: 'テスト更新', url: 'https://example.com/updated', icon: '🔧', sortOrder: 1 })).status === 200);

  // ⭐登録したリンクが一覧上部にそのまま出る (自動反映)
  const list = await (await fetch(base + '/?view=all')).text();
  check('一覧上部に登録したリンクが出る (アイコン+名前+新しいタブ)',
    list.includes('class="mall-links"') && list.includes('https://example.com/updated')
    && list.includes('テスト更新') && list.includes('target="_blank"'));
  check('既存のリンクも並ぶ', list.includes('ネクストエンジン'));

  const rDel = await jpost(`/api/links/${newId}/delete`, {});
  check('削除API', rDel.status === 200);
  check('削除の再送も200 (冪等)', (await jpost(`/api/links/${newId}/delete`, {})).status === 200);
  const list2 = await (await fetch(base + '/?view=all')).text();
  check('削除したリンクは上部から消える', !list2.includes('https://example.com/updated'));
  check('削除済みの更新は400', (await jpost(`/api/links/${newId}`, { name: 'x' })).status === 400);

  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
