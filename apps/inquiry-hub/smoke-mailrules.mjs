// 📧メールルール (mail-rules.js + router /mail-rules) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-mailrules.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-mailrules-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-mailrules-'));
process.env.DATA_DIR = workDir;
// 他のsmokeと同じDATA_DIRを共有して連続実行しても誤検知しないよう、開始時点の状態を記録
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { addMailRule, evaluateMailRules, importMailDealerRulesCsv, listMailRules } = await import('./mail-rules.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── 1. 評価エンジン ───
console.log('1. 評価エンジン');
{
  addMailRule({ name: '楽天自動配信', matchMode: 'all', priority: 10, action: 'skip',
    conditions: [{ field: 'from', op: 'contains', value: 'order-cancel@mail.rms.rakuten.co.jp' }] });
  addMailRule({ name: 'キャンセル完了系', matchMode: 'all', priority: 20, action: 'import_done',
    conditions: [{ field: 'subject', op: 'contains', value: 'キャンセルが確定' }] });
  addMailRule({ name: '複合any', matchMode: 'any', priority: 30, action: 'skip',
    conditions: [{ field: 'subject', op: 'starts_with', value: '【広告】' }, { field: 'from', op: 'ends_with', value: '@ads.example.com' }] });

  const m1 = evaluateMailRules({ from: 'Order-Cancel@mail.rms.rakuten.co.jp', subject: 'キャンセル' });
  check('from contains (大文字小文字無視) → skip', m1?.action === 'skip' && m1.ruleName === '楽天自動配信');
  const m2 = evaluateMailRules({ from: 'customer@example.com', subject: 'ご注文のキャンセルが確定しました' });
  check('subject contains → import_done', m2?.action === 'import_done');
  const m3 = evaluateMailRules({ from: 'x@ads.example.com', subject: 'こんにちは' });
  check('any: 片方一致でヒット', m3?.action === 'skip' && m3.ruleName === '複合any');
  check('どれにも当たらない → null (通常取込)', evaluateMailRules({ from: 'customer@gmail.com', subject: '商品について' }) === null);
  const first = evaluateMailRules({ from: 'order-cancel@mail.rms.rakuten.co.jp', subject: 'キャンセルが確定' });
  check('優先度昇順の先勝ち', first?.ruleName === '楽天自動配信');
  check('欠けたフィールドは空文字扱い', evaluateMailRules({ subject: '【広告】セール' })?.action === 'skip');

  let bad = null;
  try { addMailRule({ matchMode: 'all', action: 'skip', conditions: [{ field: 'x-header', op: 'contains', value: 'a' }] }); } catch (e) { bad = e; }
  check('不正フィールドはthrow', bad !== null);
  let bad2 = null;
  try { addMailRule({ matchMode: 'all', action: 'nuke', conditions: [{ field: 'from', op: 'contains', value: 'a' }] }); } catch (e) { bad2 = e; }
  check('不正アクションはthrow', bad2 !== null);

  // 壊れたルールがDBに紛れても評価は例外にならず fail-open (通常取込)。skip過剰適用もしない (Codex R1 high)
  for (const cj of ['[null]', '[1]', 'not-json', '[]', '[{"field":"x-header","op":"not_contains","value":"zz"}]']) {
    db.prepare(`INSERT INTO mail_rules (priority, name, match_mode, conditions_json, action) VALUES (1,'broken','all',?, 'skip')`).run(cj);
  }
  let evalOk = true, evalResult;
  try { evalResult = evaluateMailRules({ from: 'customer@gmail.com', subject: '商品について' }); } catch { evalOk = false; }
  check('壊れルール混入でも例外にならない', evalOk);
  check('壊れルールはスキップされ通常取込 (skip過剰適用なし)', evalResult === null);
  db.prepare('DELETE FROM mail_rules').run();
}

// ─── 2. メールディーラーCSV取込 ───
console.log('2. CSV取込');
{
  // 実CSVと同じヘッダー名 (列順は importer がヘッダー名で解決するため一部のみ)
  const header = ['条件ID', '優先度', '適用対象', '名称', '条件ブロックのand/or',
    '条件ブロック番号1', '条件のand/or1', '項目1', '文字列1', '範囲1',
    '条件ブロック番号2', '条件のand/or2', '項目2', '文字列2', '範囲2',
    '迷惑メール/ゴミ箱', '状態'].join(',');
  const rows = [
    // ゴミ箱 → skip
    '1001,5,0,広告除去,,1,,from,ads@example.com,1,,,,,,1,0',
    // 即削除 → skip (subject 2条件 and)
    '1002,6,0,,,1,,subject,メルマガ,1,1,and,subject,解除,1,2,0',
    // 対応完了 → import_done
    '1003,7,0,自動配信,,1,,from,no-reply@shop.example,1,,,,,,0,203',
    // フォルダ振り分けのみ → 対象外
    '1004,8,0,振り分けのみ,,1,,from,vendor@example.com,1,,,,,,0,0',
    // 未対応の範囲コード → unsupported
    '1005,9,0,未知範囲,,1,,from,x@example.com,9,,,,,,1,0',
    // and/or混在 → unsupported
    '1006,10,0,混在,,1,,from,a@example.com,1,1,or,subject,B,1,1,0'.replace(',1,1,or', ',1,and,from,mid@example.com,1,1,or').split(',').slice(0, 17).join(','),
  ];
  // 1006は手組みが崩れやすいので明示的に構築: 項目1=and + 項目2=or の混在
  rows[5] = '1006,10,0,混在,,1,and,from,a@example.com,1,1,or,subject,B,1,1,0';
  const csv = header + '\n' + rows.join('\n');

  const dry = importMailDealerRulesCsv(csv, { apply: false });
  check('dry-run: 集計 (skip2/done1/対象外1/不能2)',
    dry.toSkip === 2 && dry.toImportDone === 1 && dry.notTarget === 1 && dry.unsupported.length === 2,
    JSON.stringify(dry));
  check('dry-runではDBに入らない', listMailRules().length === 0);
  check('unsupportedに理由が付く', dry.unsupported.every(u => u.reason));

  const applied = importMailDealerRulesCsv(csv, { apply: true });
  check('apply: 新規3件', applied.applied === 3 && applied.updated === 0);
  const rules = listMailRules();
  check('external_key=maildealer:<条件ID> で保存', rules.every(r => /^maildealer:\d+$/.test(r.external_key)));
  check('2条件andが1ルールに', JSON.parse(rules.find(r => r.external_key === 'maildealer:1002').conditions_json).length === 2);
  check('評価も通る (skipルール)', evaluateMailRules({ from: 'ads@example.com' })?.action === 'skip');
  check('import_doneルールも機能', evaluateMailRules({ from: 'no-reply@shop.example' })?.action === 'import_done');

  const again = importMailDealerRulesCsv(csv, { apply: true });
  check('再取込は冪等 (全て更新扱い)', again.applied === 0 && again.updated === 3);
  check('件数は増えない', listMailRules().length === 3);

  let badCsv = null;
  try { importMailDealerRulesCsv('a,b,c\n1,2,3', { apply: false }); } catch (e) { badCsv = e; }
  check('ヘッダー不一致はthrow', badCsv !== null);
  const bomDry = importMailDealerRulesCsv('﻿' + csv, { apply: false });
  check('UTF-8 BOM付きCSVも解析できる', bomDry.toSkip === 2 && bomDry.toImportDone === 1);
}

// ─── 3. HTTP (画面+API) ───
console.log('3. HTTP');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/mail-rules')).text();
  check('画面: ルール一覧+取込+テストUI', html.includes('ルール一覧') && html.includes('csvFile') && html.includes('判定する'));
  check('画面: 移行ルールに出所表示', html.includes('メールディーラー移行'));
  check('ナビに📧タブ', html.includes('メールルール'));

  const rAdd = await jpost('/api/mail-rules', { name: '手動', matchMode: 'all', action: 'skip', priority: 1, conditions: [{ field: 'subject', op: 'contains', value: '広告' }] });
  check('手動追加API', rAdd.status === 200);
  const addedId = (await rAdd.json()).id;
  const rBadAdd = await jpost('/api/mail-rules', { matchMode: 'all', action: 'skip', priority: 1, conditions: [] });
  check('条件空は400', rBadAdd.status === 400);

  const rTest = await jpost('/api/mail-rules/test', { subject: 'これは広告です' });
  check('テストAPI: 一致ルールを返す', (await rTest.json()).match?.ruleId === addedId);
  const rTest2 = await jpost('/api/mail-rules/test', { subject: '通常の問い合わせ', from: 'c@example.com' });
  check('テストAPI: 不一致はnull', (await rTest2.json()).match === null);

  const rToggle = await jpost(`/api/mail-rules/${addedId}/toggle`, { active: false });
  check('無効化', rToggle.status === 200);
  check('無効ルールは評価されない', (await (await jpost('/api/mail-rules/test', { subject: 'これは広告です' })).json()).match === null);
  const rDel = await jpost(`/api/mail-rules/${addedId}/delete`, {});
  check('削除', rDel.status === 200);
  const rDel2 = await jpost(`/api/mail-rules/${addedId}/delete`, {});
  check('二重削除は404', rDel2.status === 404);

  server.close();
}

check('DBは一時サブディレクトリのみに作成', fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
