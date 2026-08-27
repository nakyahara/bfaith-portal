/**
 * shohyo-links db.js のスモークテスト (一時 DATA_DIR に対して seed + CRUD を検証)
 * 実行: node apps/shohyo-links/tests/test-db.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-links-test-'));
process.env.DATA_DIR = tmp;

const { listLinks, createLink, updateLink, deleteLink, applyClassificationPatch } = await import('../db.js');

let failed = 0;
function check(label, cond) {
  console.log(`${cond ? 'OK ' : 'NG '} ${label}`);
  if (!cond) failed++;
}

// seed
const seeded = listLinks();
const seedFile = JSON.parse(fs.readFileSync(new URL('../seed/vendors.json', import.meta.url), 'utf8'));
check(`seed投入 (${seeded.length}件 = vendors.json ${seedFile.length}件)`, seeded.length === seedFile.length && seeded.length > 0);
check('seedの保存先パスに notion 前置詞が残っていない', seeded.every(r => !r.storage_path.includes('app.notion.com')));

// Phase 0: 取得方式の機械分類
const classification = JSON.parse(fs.readFileSync(new URL('../seed/classification.json', import.meta.url), 'utf8'));
const classifiedInSeed = classification.filter(c => c.fetch_method || c.fetch_note).length;
const classifiedInDb = seeded.filter(r => r.fetch_method || r.fetch_note).length;
check(`分類パッチ適用 (${classifiedInDb}件 = classification.json ${classifiedInSeed}件)`, classifiedInDb === classifiedInSeed && classifiedInDb > 0);
const efax = seeded.find(r => r.name.includes('EFAX'));
check('eFax は A (メールに添付の元データ)', efax?.fetch_method === 'A');
const rakutenMobile = seeded.find(r => r.name.includes('ﾗｸﾃﾝﾓﾊﾞｲﾙ'));
check('楽天モバイル は C (サイトからダウンロードの元データ)', rakutenMobile?.fetch_method === 'C');

// 手動修正はパッチで上書きされない (冪等性)
const manual = updateLink(efax.id, { fetch_method: 'E', fetch_note: '手動確認済み' });
applyClassificationPatch();
const after = listLinks().find(r => r.id === efax.id);
check('手動分類は再パッチで上書きされない', after.fetch_method === 'E' && after.fetch_note === '手動確認済み');
updateLink(efax.id, { fetch_method: 'A', fetch_note: efax.fetch_note }); // 戻す

// create
const created = createLink({ name: 'テスト支払い先', url: 'https://example.com/', card_type: 'テスト用', debit_day: '15' });
check('createLink が id を返す', Number.isInteger(created.id));
check('createLink の値が保存される', created.name === 'テスト支払い先' && created.debit_day === '15');

// name必須
let threw = false;
try { createLink({ url: 'https://example.com/' }); } catch (e) { threw = e.message === 'name_required'; }
check('name なし create は name_required', threw);

// update
const updated = updateLink(created.id, { receipt_source: 'サイトからダウンロード' });
check('updateLink が反映される', updated.receipt_source === 'サイトからダウンロード' && updated.name === 'テスト支払い先');
check('updateLink 存在しないid は null', updateLink(999999, { note: 'x' }) === null);

// delete
check('deleteLink 成功', deleteLink(created.id) === true);
check('deleteLink 二重削除は false', deleteLink(created.id) === false);
check('件数がseedに戻る', listLinks().length === seeded.length);

console.log(failed ? `\n${failed} 件失敗` : '\n全件パス');
process.exit(failed ? 1 : 0);
