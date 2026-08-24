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

// ─── 4. 詳細画面からのルール作成 + 既存メールへの一括適用 (2026-07-25 中原さん要望) ───
console.log('4. 詳細画面からのルール作成');
{
  const { applyRuleToExistingMails } = await import('./mail-rules.js');
  const { toUtcIso } = await import('./db.js');
  const shopM = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run().lastInsertRowid;
  const shopR = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天','373343')").run().lastInsertRowid;
  const mk = (shopId, ch, ext, subject, from, status = 'open') => db.prepare(`INSERT INTO inquiries
      (channel_type, shop_id, external_inquiry_id, subject, customer_identifier, internal_status, is_unread, received_at, conversation_rev)
    VALUES (?,?,?,?,?,?,1,?,1)`).run(ch, shopId, ext, subject, from, status, toUtcIso(Date.now())).lastInsertRowid;

  const a1 = mk(shopM, 'email', 'm1', 'FBA商品を受領中です (FBA15GF962P2)', 'donotreply@amazon.com');
  const a2 = mk(shopM, 'email', 'm2', 'FBA商品が受領されました', 'donotreply@amazon.com');
  const other = mk(shopM, 'email', 'm3', '商品の在庫について', 'customer@gmail.com');
  const doneAlready = mk(shopM, 'email', 'm4', 'FBA通知', 'donotreply@amazon.com', 'done');
  const rakutenSame = mk(shopR, 'rakuten', 'r1', 'FBA商品を受領中です', 'donotreply@amazon.com');

  const cond = [{ field: 'from', op: 'equals', value: 'donotreply@amazon.com' }];
  const dry = applyRuleToExistingMails(cond, { apply: false });
  check('下見: 未完了のメールのみ数える (完了済み・他チャネルは除外)', dry.matched === 2 && dry.completed === 0, JSON.stringify(dry));
  check('下見では状態を変えない', db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(a1).internal_status === 'open');

  const applied = applyRuleToExistingMails(cond, { apply: true, actorId: 'tester' });
  check('適用: 2件を完了に', applied.completed === 2
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(a1).internal_status === 'done'
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(a2).internal_status === 'done');
  check('関係ない問い合わせは触らない', db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(other).internal_status === 'open');
  check('他チャネル (楽天) は対象外', db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(rakutenSame).internal_status === 'open');
  check('完了は削除しない (履歴に残る)', !!db.prepare('SELECT id FROM inquiries WHERE id = ?').get(a1));
  check('一括適用が操作ログに残る', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'status_change'").get(a1).c >= 1);

  // 件名条件・ドメイン条件
  const bySubject = applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '在庫' }], { apply: false });
  check('件名条件で絞れる', bySubject.matched === 1);
  const m5 = mk(shopM, 'email', 'm5', '請求のお知らせ', 'billing@amazon.co.jp');
  const byDomain = applyRuleToExistingMails([{ field: 'from', op: 'ends_with', value: '@amazon.co.jp' }], { apply: false });
  check('ドメイン条件で絞れる', byDomain.matched === 1);
  // LIKE特殊文字はリテラル扱い
  mk(shopM, 'email', 'm6', '100%OFFクーポン', 'promo@example.com');
  check('LIKE特殊文字はリテラル (%は全件マッチしない)',
    applyRuleToExistingMails([{ field: 'subject', op: 'contains', value: '100%O' }], { apply: false }).matched === 1);
  let badCond = null;
  try { applyRuleToExistingMails([{ field: 'from', op: 'contains', value: '' }], { apply: false }); } catch (e) { badCond = e; }
  check('空条件はthrow (全件マッチを防ぐ)', badCond !== null);
  // Reply-To/To/本文は inquiries に保存していないので既存適用は断る (差出人で代用すると誤爆する)
  const { canApplyToExisting } = await import('./mail-rules.js');
  check('canApplyToExisting: from/subject のみ true',
    canApplyToExisting([{ field: 'from', op: 'equals', value: 'a@b.c' }, { field: 'subject', op: 'contains', value: 'x' }]) === true
    && canApplyToExisting([{ field: 'reply_to', op: 'contains', value: 'x' }]) === false
    && canApplyToExisting([{ field: 'body', op: 'contains', value: 'x' }]) === false);
  let unsupported = null;
  try { applyRuleToExistingMails([{ field: 'reply_to', op: 'contains', value: 'x' }], { apply: false }); } catch (e) { unsupported = e; }
  check('非対応フィールドの一括適用はthrow', unsupported !== null && /対応していません/.test(unsupported.message));
  // 複合条件 (かつ/または) の絞り込み
  check('複合条件 かつ',
    applyRuleToExistingMails([{ field: 'from', op: 'ends_with', value: '@amazon.co.jp' }, { field: 'subject', op: 'contains', value: '請求' }],
      { matchMode: 'all', apply: false }).matched === 1);
  check('複合条件 または',
    applyRuleToExistingMails([{ field: 'from', op: 'equals', value: 'customer@gmail.com' }, { field: 'subject', op: 'contains', value: '請求' }],
      { matchMode: 'any', apply: false }).matched === 2);
}

// ─── 5. HTTP: 詳細画面のルール作成API ───
console.log('5. ルール作成API');
{
  const app2 = express();
  app2.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app2.listen(0, '127.0.0.1', () => r(s)); });
  const base2 = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;
  const jp = (p, data) => fetch(base2 + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const mailInq = db.prepare("SELECT id FROM inquiries WHERE external_inquiry_id = 'm3'").get().id;   // customer@gmail.com
  const rakutenInq = db.prepare("SELECT id FROM inquiries WHERE external_inquiry_id = 'r1'").get().id;

  const html = await (await fetch(`${base2}/inquiries/${mailInq}`)).text();
  check('メール詳細に「今後の自動処理」パネル (複合条件3行+かつ/または)', html.includes('今後の自動処理')
    && html.includes('id="mrField1"') && html.includes('id="mrField3"') && html.includes('id="mrMode"'));
  check('フォルダ・ラベル選択で扱いをimportへ自動切替するJSが載る (2026-08-20 実事故対応)',
    html.includes("['mrFolder', 'mrLabel'].forEach")
    && html.includes('振り分けだけする (新着のまま)」にしてください'));
  // 🔎 NEで受注検索 (2026-08-20 スタッフ要望): 注文番号が無い問い合わせにアドレスコピー+NE検索画面の導線
  check('メール詳細に「NEで受注検索」導線 (アドレスをdata属性で持つ)',
    html.includes('id="neMailSearch"') && html.includes('data-mail="customer@gmail.com"'));
  check('コピー→開くの順のクライアントJSが載る', html.includes("navigator.clipboard.writeText(neMailBtn.dataset.mail"));

  // 注文番号があるとNE直リンクが出るので検索導線は出さない
  db.prepare('UPDATE inquiries SET order_number = ? WHERE id = ?').run('123456-20260820-00001', rakutenInq);
  const htmlRk = await (await fetch(`${base2}/inquiries/${rakutenInq}`)).text();
  check('楽天詳細にはパネルを出さない', !htmlRk.includes('id="mrField1"'));
  check('注文番号あり: NE直リンクが出て検索導線は出さない',
    htmlRk.includes('kensaku_denpyo_no=123456-20260820-00001') && !htmlRk.includes('id="neMailSearch"'));
  // 顧客識別子がメールアドレスでない場合も出さない
  db.prepare('UPDATE inquiries SET order_number = NULL, customer_identifier = ? WHERE id = ?').run('member-9999', rakutenInq);
  const htmlRk2 = await (await fetch(`${base2}/inquiries/${rakutenInq}`)).text();
  check('識別子が@なし: 検索導線を出さない', !htmlRk2.includes('id="neMailSearch"'));

  const fromCond = [{ field: 'from', op: 'equals', value: 'customer@gmail.com' }];
  const rBad = await jp(`/api/inquiries/${rakutenInq}/mail-rule`, { conditions: fromCond, action: 'skip' });
  check('メール以外のチャネルは400', rBad.status === 400);
  const rBadAction = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: fromCond, action: 'nuke' });
  check('不正な扱いは400', rBadAction.status === 400);
  const rNoCond = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: [], action: 'skip' });
  check('条件が空は400', rNoCond.status === 400);
  const rBadField = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: [{ field: 'x-header', op: 'contains', value: 'a' }], action: 'skip' });
  check('不正なフィールドは400', rBadField.status === 400);

  const rDry = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: fromCond, action: 'skip', applyToExisting: true, dryRun: true });
  const dryJson = await rDry.json();
  check('dryRun: 件数だけ返しルールは作らない', dryJson.matched === 1 && dryJson.canApplyToExisting === true
    && listMailRules().every(r => !String(r.name || '').includes('customer@gmail.com')));

  // 複合条件 (メールディーラーと同じ「件名が○○ かつ Reply-Toが△△」。2026-07-25 中原さん要望)
  const multi = [
    { field: 'subject', op: 'contains', value: 'Your payment is on the way' },
    { field: 'reply_to', op: 'contains', value: 'no-reply@amazon.co.jp' },
  ];
  const rMultiDry = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: multi, matchMode: 'all', action: 'skip', applyToExisting: true, dryRun: true });
  const multiDry = await rMultiDry.json();
  check('複合条件: 説明文が「かつ」で連結される', multiDry.description.includes('かつ') && multiDry.description.includes('Reply-To'));
  check('Reply-To を含む条件は既存への一括適用に非対応と返す', multiDry.canApplyToExisting === false && multiDry.matched === 0);
  const rMulti = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: multi, matchMode: 'all', action: 'skip', applyToExisting: true });
  const multiMade = await rMulti.json();
  check('複合条件でもルールは作れる (既存適用は0件)', rMulti.status === 200 && multiMade.completed === 0);
  const multiRule = listMailRules().find(r => r.id === multiMade.id);
  check('複合条件が2件保存される', JSON.parse(multiRule.conditions_json).length === 2 && multiRule.match_mode === 'all');
  check('複合条件が評価に効く (両方一致でskip)',
    evaluateMailRules({ subject: 'Your payment is on the way', reply_to: 'no-reply@amazon.co.jp' })?.action === 'skip');
  check('片方だけでは一致しない (かつ)',
    evaluateMailRules({ subject: 'Your payment is on the way', reply_to: 'someone@example.com' }) === null);

  const rMake = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: fromCond, action: 'skip', applyToExisting: true });
  const made = await rMake.json();
  check('ルール作成+既存1件を完了に', rMake.status === 200 && made.completed === 1
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(mailInq).internal_status === 'done');
  const rule = listMailRules().find(r => r.id === made.id);
  check('作成されたルールが評価に効く', evaluateMailRules({ from: 'customer@gmail.com' })?.action === 'skip' && rule.action === 'skip');
  check('ルール名に条件が入る', String(rule.name).includes('customer@gmail.com'));

  // ─── 2026-08-20 実事故対応: skip+フォルダの拒否と、先勝ちルールに遮られる警告 ───
  const folder = db.prepare("INSERT INTO inquiry_folders (name) VALUES ('事故テスト')").run();
  const fid = Number(folder.lastInsertRowid);
  const rSkipFolder = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: [{ field: 'subject', op: 'contains', value: '在庫' }], action: 'skip', folderId: fid });
  check('skip+フォルダはサーバーでも400 (黙って捨てない)', rSkipFolder.status === 400
    && String((await rSkipFolder.json()).error).includes('フォルダは指定できません'));
  // 上の 'customer@gmail.com → skip' ルール (priority 50) が既にこのメールに当たる状態で、
  // 同じメールから import+フォルダのルールを作る → 作成はできるが shadowedBy で警告が返る
  const rShadow = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: [{ field: 'subject', op: 'contains', value: '在庫について' }], action: 'import', folderId: fid });
  const shadow = await rShadow.json();
  check('先勝ちルールに遮られる場合は shadowedBy を返す', rShadow.status === 200
    && shadow.shadowedBy && shadow.shadowedBy.id === made.id && Number.isInteger(shadow.shadowedBy.priority));
  // 遮るルールを無効化すれば新ルールが効く (shadowedBy無し)
  const { setMailRuleActive } = await import('./mail-rules.js');
  setMailRuleActive(made.id, false);
  setMailRuleActive(shadow.id, false);
  const rClear = await jp(`/api/inquiries/${mailInq}/mail-rule`, { conditions: [{ field: 'subject', op: 'contains', value: '商品の在庫' }], action: 'import', folderId: fid });
  const clear = await rClear.json();
  check('遮るルールが無ければ shadowedBy 無し', rClear.status === 200 && !clear.shadowedBy);

  srv.close();
}

// ─── 6. フォルダ自動振り分け (2026-08-17 中原さん要望: Gmail風「今後この送信元はこのフォルダへ」) ───
console.log('6. フォルダ自動振り分け');
{
  const { applyRuleToExistingMails } = await import('./mail-rules.js');
  const { toUtcIso } = await import('./db.js');
  db.prepare('DELETE FROM mail_rules').run();
  const folderId = Number(db.prepare("INSERT INTO inquiry_folders (name) VALUES ('Amazon通知')").run().lastInsertRowid);
  const shopM = db.prepare("SELECT id FROM shops WHERE channel_type = 'email' LIMIT 1").get().id;

  // 6a. addMailRule の検証
  const okRule = addMailRule({ name: 'Amazonへ', matchMode: 'all', priority: 5, action: 'import', folderId,
    conditions: [{ field: 'from', op: 'equals', value: 'folder-test@amazon.com' }] });
  check('importルール作成 (フォルダ付き)', Number.isInteger(Number(okRule.id)));
  let e1 = null;
  try { addMailRule({ matchMode: 'all', action: 'import', conditions: [{ field: 'from', op: 'contains', value: 'a' }] }); } catch (e) { e1 = e; }
  check('import + フォルダ・ラベル無しはthrow', e1 !== null && /フォルダかラベルの指定が必要/.test(e1.message));
  let e2 = null;
  try { addMailRule({ matchMode: 'all', action: 'skip', folderId, conditions: [{ field: 'from', op: 'contains', value: 'a' }] }); } catch (e) { e2 = e; }
  check('skip + フォルダ指定はthrow', e2 !== null);
  let e3 = null;
  try { addMailRule({ matchMode: 'all', action: 'import', folderId: 99999, conditions: [{ field: 'from', op: 'contains', value: 'a' }] }); } catch (e) { e3 = e; }
  check('存在しないフォルダはthrow', e3 !== null);
  const doneWithFolder = addMailRule({ name: '完了+フォルダ', matchMode: 'all', priority: 6, action: 'import_done', folderId,
    conditions: [{ field: 'from', op: 'equals', value: 'done-folder@amazon.com' }] });
  check('import_done + フォルダの組み合わせも作れる', Number.isInteger(Number(doneWithFolder.id)));

  // 6b. 評価が folderId を返す / 壊れルールは fail-open
  const ev = evaluateMailRules({ from: 'folder-test@amazon.com' });
  check('評価: action=import + folderId', ev?.action === 'import' && ev.folderId === folderId);
  db.prepare(`INSERT INTO mail_rules (priority, name, match_mode, conditions_json, action)
    VALUES (1,'broken-import','all','[{"field":"from","op":"equals","value":"broken@example.com"}]','import')`).run();
  check('import なのにフォルダ無しの壊れルールは評価されない (fail-open)', evaluateMailRules({ from: 'broken@example.com' }) === null);

  // 6c. gmail mapThread → initialFolderId (通常取込のまま) / import_done は完了+フォルダ
  const { mapThread } = await import('./sync/adapters/gmail.js');
  const th = (id, from) => ({ id, messages: [{ id: `${id}-m1`, internalDate: '1755300000000', payload: { headers: [
    { name: 'From', value: `送信元 <${from}>` }, { name: 'Subject', value: '通知' }] } }] });
  const mapped = mapThread(th('th-f1', 'folder-test@amazon.com'));
  check('mapThread: initialFolderId が付き通常取込のまま', mapped.initialFolderId === folderId && mapped.initialInternalStatus === undefined);
  const mappedDone = mapThread(th('th-f2', 'done-folder@amazon.com'));
  check('mapThread: import_done + フォルダは両方付く', mappedDone.initialFolderId === folderId && mappedDone.initialInternalStatus === 'done');

  // 6d. engine が新規作成時に folder_id を保存する (既存チケットは動かさない)
  const { runSync } = await import('./sync/engine.js');
  const shopF = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','振り分け店','folder-smoke@b-faith.biz')").run().lastInsertRowid;
  const mkAdapter = items => ({ channelType: 'email', fetchNew: async () => ({ inquiries: items }) });
  const r1 = await runSync(shopF, mkAdapter([mapped, mappedDone]));
  check('runSync 成功', r1.ok === true, JSON.stringify(r1));
  const created = db.prepare("SELECT * FROM inquiries WHERE shop_id = ? AND external_inquiry_id = 'th-f1'").get(shopF);
  check('新規作成でフォルダに入る (状態は新着)', created?.folder_id === folderId && created?.internal_status === 'open');
  const createdDone = db.prepare("SELECT * FROM inquiries WHERE shop_id = ? AND external_inquiry_id = 'th-f2'").get(shopF);
  check('import_done + フォルダ = 完了+フォルダ', createdDone?.folder_id === folderId && createdDone?.internal_status === 'done');
  // 既存チケットのフォルダは再同期で動かさない (手で移動した分を上書きしない)
  db.prepare('UPDATE inquiries SET folder_id = NULL WHERE id = ?').run(created.id);
  await runSync(shopF, mkAdapter([mapped]));
  check('再同期は既存のフォルダを動かさない', db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(created.id).folder_id === null);

  // 6e. 既存メールへの一括適用 (フォルダ入れ=ステータス不変・冪等)
  const mkMail = (ext, from, status = 'open') => db.prepare(`INSERT INTO inquiries
      (channel_type, shop_id, external_inquiry_id, subject, customer_identifier, internal_status, is_unread, received_at, conversation_rev)
    VALUES ('email',?,?,?,?,?,1,?,1)`).run(shopM, ext, 'フォルダ一括テスト', from, status, toUtcIso(Date.now())).lastInsertRowid;
  const f1 = mkMail('fb1', 'bulk-folder@example.com');
  const f2 = mkMail('fb2', 'bulk-folder@example.com', 'done');   // 完了済みもフォルダには入れる対象
  const cond = [{ field: 'from', op: 'equals', value: 'bulk-folder@example.com' }];
  const dryF = applyRuleToExistingMails(cond, { apply: false, action: 'import', folderId });
  check('下見 (フォルダ入れ): 完了済みも含めて数える', dryF.matched === 2, JSON.stringify(dryF));
  const appF = applyRuleToExistingMails(cond, { apply: true, action: 'import', folderId, actorId: 'tester' });
  check('適用: 2件をフォルダへ・ステータスは不変', appF.foldered === 2 && appF.completed === 0
    && db.prepare('SELECT folder_id, internal_status FROM inquiries WHERE id = ?').get(f1).internal_status === 'open'
    && db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(f1).folder_id === folderId
    && db.prepare('SELECT folder_id FROM inquiries WHERE id = ?').get(f2).folder_id === folderId);
  check('再適用は冪等 (0件)', applyRuleToExistingMails(cond, { apply: true, action: 'import', folderId, actorId: 'tester' }).foldered === 0);
  check('フォルダ変更が操作ログに残る', db.prepare(
    "SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id = ? AND action_type = 'folder_change'").get(f1).c >= 1);
  // import_done + フォルダ: 完了とフォルダ入れの両方
  const f3 = mkMail('fb3', 'bulk-done-folder@example.com');
  const appDF = applyRuleToExistingMails([{ field: 'from', op: 'equals', value: 'bulk-done-folder@example.com' }],
    { apply: true, action: 'import_done', folderId, actorId: 'tester' });
  check('import_done + フォルダの一括適用は完了+フォルダ両方', appDF.completed === 1 && appDF.foldered === 1
    && db.prepare('SELECT folder_id, internal_status FROM inquiries WHERE id = ?').get(f3).folder_id === folderId
    && db.prepare('SELECT internal_status FROM inquiries WHERE id = ?').get(f3).internal_status === 'done');
  db.prepare('DELETE FROM mail_rules').run();
}

check('DBは一時サブディレクトリのみに作成', fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
