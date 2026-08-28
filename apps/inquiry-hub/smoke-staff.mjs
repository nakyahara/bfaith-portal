// 👥担当者と権限マップ (staff.js + router /staff) + 📊実データ調査 (insights.js) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-staff.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-staff-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-staff-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB, BUILTIN_PERMISSIONS } = await import('./db.js');
const staff = await import('./staff.js');
const { collectInsights, clearInsightsCache, estimateCost, BLOCK_KEYWORDS, WMS_CUTOFFS, cutoffBucketIndex } = await import('./insights.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
// 再初期化を試すテストがあるので、そのたびに取り直す (古いハンドルは閉じられる)
let db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = fn => { try { fn(); return false; } catch { return true; } };

// ─── 1. 既定権限のシード ───
console.log('1. 既定権限');
{
  const all = staff.listPermissions({ includeInactive: true });
  check(`既定の${BUILTIN_PERMISSIONS.length}件が入る`, all.length === BUILTIN_PERMISSIONS.length, `実際=${all.length}`);
  check('D/E/S の3系統がそろう', ['decision', 'escalation', 'system'].every(k => all.some(p => p.kind === k)));
  check('キャンセル受付(D1)と返金実行(S4)が別権限', !!staff.getPermission('D1') && !!staff.getPermission('S4'));
  check('「止める」(S2a)と「解除する」(S2b)が別権限', !!staff.getPermission('S2a') && !!staff.getPermission('S2b'));
  check('既定はすべて is_builtin=1', all.every(p => p.is_builtin === 1));

  // ⚠️既定権限の名前・説明は変えられない (コードが D1/S4 の意味を前提に動くため)
  check('既定権限の名前は変更できない', throws(() => staff.updatePermission('D0', { name: '別の意味' })));
  check('既定権限の説明も変更できない', throws(() => staff.updatePermission('S4', { description: '在庫照会' })));

  // 社内メモは既定権限でも書けて、再初期化でも消えない
  staff.updatePermission('D0', { localNote: 'パートさんはここまで' });
  initInquiryHubDB();
  db = getDB();
  check('社内メモは既定権限にも書ける', staff.getPermission('D0').local_note === 'パートさんはここまで');
  check('再初期化しても既定権限の名前が壊れない', staff.getPermission('D0').name === '定型回答のみ');

  check('既定権限は削除できない', throws(() => staff.deletePermission('D0')));
  staff.setPermissionActive('E6', false);
  check('既定権限も無効にはできる', staff.getPermission('E6').is_active === 0);
  check('無効な権限は既定の一覧に出ない', !staff.listPermissions().some(p => p.code === 'E6'));
  staff.setPermissionActive('E6', true);
}

// ─── 2. 担当者のCRUD ───
console.log('2. 担当者');
let alice, bob;
{
  alice = staff.createStaff({ userKey: ' alice@b-faith.biz ', displayName: ' 中原 ', refundLimitYen: '3,000' }, 'tester');
  check('作成 (前後空白はtrim)', alice.userKey === 'alice@b-faith.biz' && alice.displayName === '中原');
  check('カンマ入りの金額を数値で保存', staff.getStaff(alice.id).refund_limit_yen === 3000);

  bob = staff.createStaff({ userKey: 'bob@b-faith.biz', displayName: '田中' }, 'tester');
  check('2人目も作成', staff.listStaff().length === 2);
  check('上限額は空でよい (null)', staff.getStaff(bob.id).refund_limit_yen === null);

  check('同じ担当者キーは作れない', throws(() => staff.createStaff({ userKey: 'alice@b-faith.biz', displayName: '別人' })));
  check('表示名が空はthrow', throws(() => staff.createStaff({ userKey: 'x@y.z', displayName: '  ' })));
  check('担当者キーが空はthrow', throws(() => staff.createStaff({ userKey: '', displayName: 'x' })));
  check('上限額が負はthrow', throws(() => staff.createStaff({ userKey: 'n@y.z', displayName: 'n', refundLimitYen: -1 })));
  check('上限額が小数はthrow', throws(() => staff.createStaff({ userKey: 'n2@y.z', displayName: 'n', refundLimitYen: '1.5' })));
}

// ─── 3. 権限の付与・剥奪 ───
console.log('3. 権限の付与');
{
  const r1 = staff.setStaffPermissions(alice.id, ['D0', 'D1', 'D2', 'S1', 'S2a'], 'tester');
  check('5件付与', r1.granted.length === 5 && r1.revoked.length === 0);

  // 差分だけを履歴に残す (変えていない権限のログで埋まらない)
  const r2 = staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1'], 'tester');
  check('差分だけ動く (剥奪2件・付与0件)', r2.granted.length === 0 && r2.revoked.length === 2, JSON.stringify(r2));

  const r3 = staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1'], 'tester');
  check('同じ内容の再送は何も動かない (冪等)', r3.granted.length === 0 && r3.revoked.length === 0);

  check('存在しない権限コードはthrow', throws(() => staff.setStaffPermissions(alice.id, ['D0', 'ZZ9'], 'tester')));
  check('throw後も元の権限が残っている (トランザクション)',
    staff.listStaff({ withPermissions: true }).find(s => s.id === alice.id).permissions.length === 3);

  const logs = staff.listPermissionLogs(50);
  check('履歴に付与と剥奪が残る', logs.some(l => l.action === 'grant') && logs.some(l => l.action === 'revoke'));
  check('履歴に実行者が残る', logs[0].actor === 'tester');
  check('履歴に権限名が残る (定義が消えても読める)', logs.every(l => l.permission_name));

  // ⚠️無効にした権限を、古い画面や直接APIから付け直せない (既存の付与は保持する)
  staff.setPermissionActive('E6', false);
  check('無効な権限は新規付与できない', throws(() => staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1', 'E6'], 'tester')));
  staff.setPermissionActive('E6', true);
  staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1', 'E6'], 'tester');
  staff.setPermissionActive('E6', false);
  const keep = staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1', 'E6'], 'tester');
  check('付与済みなら無効化後も保持できる (復帰用)', keep.granted.length === 0 && keep.revoked.length === 0);
  check('無効な権限では担当候補に出ない (fail-closed)', staff.findStaffWithPermissions(['E6']).length === 0);
  staff.setPermissionActive('E6', true);
  staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1'], 'tester');
}

// ─── 3b. 返金上限は fail-closed (空欄=無制限にしない) ───
console.log('3b. 返金上限の安全側');
{
  const withPerms = () => staff.listStaff({ withPermissions: true }).find(s => s.id === alice.id);
  staff.setStaffPermissions(alice.id, ['D0', 'D1', 'S1'], 'tester');
  check('D2が無ければ0円', staff.refundLimitOf(withPerms()).limit === 0);

  staff.setStaffPermissions(alice.id, ['D0', 'D1', 'D2', 'S1'], 'tester');
  staff.updateStaff(alice.id, { refundLimitYen: '' });
  const r = staff.refundLimitOf(withPerms());
  check('⭐D2ありでも上限未設定なら0円 (無制限にしない)', r.limit === 0 && r.hasD2 && r.needsLimit);

  staff.updateStaff(alice.id, { refundLimitYen: 3000 });
  const r2 = staff.refundLimitOf(withPerms());
  check('上限を入れれば効く', r2.limit === 3000 && r2.needsLimit === false);
}

// ─── 4. 権限で担当者を引く (トリアージのルーティングの土台) ───
console.log('4. 権限で引く');
{
  staff.setStaffPermissions(bob.id, ['D0', 'D1', 'D2', 'D3', 'S1', 'S2a', 'S2b', 'S4'], 'tester');
  check('D3+S4 を持つのは田中だけ', staff.findStaffWithPermissions(['D3', 'S4']).map(s => s.display_name).join() === '田中');
  check('D0+S1 は2人とも該当', staff.findStaffWithPermissions(['D0', 'S1']).length === 2);
  check('誰も持たない組み合わせは0件', staff.findStaffWithPermissions(['E3', 'E4']).length === 0);
  check('条件なしは全員', staff.findStaffWithPermissions([]).length === 2);
}

// ─── 5. まとめ保存 (画面の「保存」1回ぶん) ───
console.log('5. まとめ保存');
{
  const r = staff.saveStaffWithPermissions(alice.id,
    { displayName: '中原 大輔', refundLimitYen: 5000, permissions: ['D0', 'D1', 'D2', 'S1', 'S7'] }, 'tester');
  check('基本情報と権限が同時に入る',
    r.displayName === '中原 大輔' && staff.getStaff(alice.id).refund_limit_yen === 5000
    && r.permissions.granted.includes('S7'), JSON.stringify(r.permissions));

  // ⭐片方だけ保存される中途半端な状態を作らない
  const before = staff.listStaff({ withPermissions: true }).find(s => s.id === alice.id);
  const failed_ = throws(() => staff.saveStaffWithPermissions(alice.id,
    { displayName: '別名', permissions: ['D0', 'ZZ9'] }, 'tester'));
  const after = staff.listStaff({ withPermissions: true }).find(s => s.id === alice.id);
  check('権限が不正なら名前も変わらない (1トランザクション)',
    failed_ && after.display_name === before.display_name && after.permissions.length === before.permissions.length);
}

// ─── 6. 無効化 ───
console.log('6. 無効化');
{
  const c = staff.createStaff({ userKey: 'temp@b-faith.biz', displayName: '一時' }, 'tester');
  staff.setStaffPermissions(c.id, ['D0', 'S1'], 'tester');
  const r = staff.deactivateStaff(c.id, 'tester');
  check('無効化で権限も外れる', r.revoked === 2);
  check('一覧から消える', !staff.listStaff().some(s => s.id === c.id));
  check('includeInactive では残る', staff.listStaff({ includeInactive: true }).some(s => s.id === c.id));
  check('無効化の再送も成功 (冪等)', staff.deactivateStaff(c.id, 'tester').alreadyInactive === true);
  check('無効化後は同じキーで再登録できる', !!staff.createStaff({ userKey: 'temp@b-faith.biz', displayName: '一時2' }, 'tester'));
}

// ─── 7. 独自権限 ───
console.log('7. 独自権限');
{
  const p = staff.createPermission({ code: 'S9', kind: 'system', name: 'テスト権限', description: '説明' });
  check('追加できる', p.code === 'S9' && staff.getPermission('S9').is_builtin === 0);
  check('同じコードは作れない', throws(() => staff.createPermission({ code: 'S9', kind: 'system', name: 'x' })));
  check('記号入りのコードはthrow', throws(() => staff.createPermission({ code: 'S-9', kind: 'system', name: 'x' })));
  check('未知の種別はthrow', throws(() => staff.createPermission({ code: 'S10', kind: 'other', name: 'x' })));

  staff.setStaffPermissions(bob.id, ['D0', 'S9'], 'tester');
  const before = staff.listPermissionLogs(200).length;
  const del = staff.deletePermission('S9', 'tester');
  check('独自権限は削除できる', !staff.getPermission('S9'));
  check('削除で付与済みの行も消える',
    !staff.listStaff({ withPermissions: true }).find(s => s.id === bob.id).permissions.includes('S9'));
  // ⭐削除でも「誰がいつ外したか」を残す (追記のみの設計)
  check('削除時に持っていた人の剥奪ログが残る', del.revoked === 1 && staff.listPermissionLogs(200).length === before + 1);
  const delLog = staff.listPermissionLogs(200).find(l => l.permission_code === 'S9');
  check('権限定義が消えても履歴から名前が読める', delLog && delLog.permission_name === 'テスト権限', JSON.stringify(delLog));
}

// ─── 8. 実データ調査 (データが無くても落ちない) ───
console.log('8. 実データ調査');
{
  const empty = collectInsights({ days: 30, fresh: true });
  check('データ0件でも例外にならない', empty.totals.data.inquiries_all === 0);
  check('各項目は {data, error} で返る (失敗値を成功として扱わない)',
    empty.totals.error === null && Object.prototype.hasOwnProperty.call(empty.totals, 'data'));
  check('⭐締め窓は締めの回数と同じ3区分', empty.cutoffWindows.data.length === 3, String(empty.cutoffWindows.data.length));
  check('締め時刻は3回 (09:00/12:30/14:30)', WMS_CUTOFFS.map(c => c.label).join() === '09:00,12:30,14:30');
  check('キーワードは全件0で返る', empty.keywordHits.data.every(k => k.count === 0));

  // 締め窓の振り分けロジック単体 (JSTの分数 → どの締めに間に合わせるか)
  check('08:59 は 09:00の締め', cutoffBucketIndex(8 * 60 + 59) === 0);
  check('09:00 ちょうどは 12:30の締め', cutoffBucketIndex(9 * 60) === 1);
  check('12:29 は 12:30の締め', cutoffBucketIndex(12 * 60 + 29) === 1);
  check('12:30 ちょうどは 14:30の締め', cutoffBucketIndex(12 * 60 + 30) === 2);
  check('14:29 は 14:30の締め', cutoffBucketIndex(14 * 60 + 29) === 2);
  check('⭐14:30 ちょうどは翌朝09:00の締め (00:00〜09:00と同じ区分)', cutoffBucketIndex(14 * 60 + 30) === 0);
  check('23:59 も翌朝09:00の締め', cutoffBucketIndex(23 * 60 + 59) === 0);

  // テストデータ: 締め窓それぞれに1件ずつ入るように受信時刻を置く (UTC格納・JST=+9h)
  db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','テスト店','info@example.com')`).run();
  const shopId = db.prepare('SELECT id FROM shops').get().id;
  const today = new Date().toISOString().slice(0, 10);
  const mk = (ext, utcTime, body, order) => {
    const at = `${today}T${utcTime}:00Z`;
    const id = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
        internal_status, order_number, received_at, last_message_at)
      VALUES ('email', ?, ?, '件名', 'open', ?, ?, ?)`).run(shopId, ext, order, at, at).lastInsertRowid;
    db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
        is_incoming, message_body_text, received_at)
      VALUES (?, ?, 'customer', 1, ?, ?)`).run(id, `msg-${ext}`, body, at);
    return id;
  };
  // JST 07:00 (UTC22:00の前日) は面倒なので当日UTCで: JST = UTC+9
  mk('w1', '22:30', '注文をキャンセルしたいです', 'A-1');   // JST 07:30 → 09:00の締め
  mk('w2', '01:00', '住所を変更してください', 'A-2');       // JST 10:00 → 12:30の締め
  mk('w3', '04:00', 'お届け先を変えたい', null);            // JST 13:00 → 14:30の締め
  mk('w4', '08:00', 'キャンセルでお願いします', 'A-4');     // JST 17:00 → 翌朝
  mk('w5', '02:00', '商品の使い方を教えてください', 'A-5'); // キーワードに当たらない

  // ⚠️キャッシュがあるので取り直す
  const d = collectInsights({ days: 30, fresh: true });
  check('受信件数が数えられる', d.byChannel.data[0].c === 5, JSON.stringify(d.byChannel.data));
  const cut = d.cutoffWindows.data;
  check('⭐14:30以降と翌朝09:00までが同じ締めに合算される (2件)', cut[0].count === 2, JSON.stringify(cut.map(c => c.count)));
  check('09:00〜12:30 は1件', cut[1].count === 1);
  check('12:30〜14:30 は1件', cut[2].count === 1);
  check('キーワードに当たらない問い合わせは締め窓に入らない', cut.reduce((a, c) => a + c.count, 0) === 4);
  check('締め窓に問い合わせ数も出る (メッセージ数と別)', cut[0].inquiries === 2);
  check('キャンセルのキーワードが2件当たる', d.keywordHits.data.find(k => k.name === 'キャンセル').count === 2);
  check('キーワードに問い合わせ数も出る', d.keywordHits.data.find(k => k.name === 'キャンセル').inquiries === 2);
  check('注文番号の埋まり率が出る (4/5=80%)', d.orderNumberFill.data[0].pct === 80, JSON.stringify(d.orderNumberFill.data));
  check('本文の平均文字数が出る', d.bodyLength.data.avg_chars > 0);
  check('日別に集計される', d.byDay.data.length >= 1);

  // ⭐期間の境界: ISO文字列とSQLite形式の混在で取りこぼさないか (datetime()で包んでいるか)
  {
    const old = new Date(Date.now() - 40 * 86400000).toISOString().replace('T', ' ').slice(0, 19); // SQLite形式
    const id = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
        internal_status, received_at, last_message_at)
      VALUES ('email', ?, 'old-1', '件名', 'open', ?, ?)`).run(shopId, old, old).lastInsertRowid;
    db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
        is_incoming, message_body_text, received_at) VALUES (?, 'msg-old', 'customer', 1, 'キャンセルしたい', ?)`)
      .run(id, old);
    const d30 = collectInsights({ days: 30, fresh: true });
    const d90 = collectInsights({ days: 90, fresh: true });
    check('40日前のデータは30日集計に入らない', d30.byChannel.data[0].c === 5, JSON.stringify(d30.byChannel.data));
    check('90日集計には入る (書式が違っても拾える)', d90.byChannel.data[0].c === 6, JSON.stringify(d90.byChannel.data));
  }

  // キャッシュ (連打しても再集計しない)
  const c1 = collectInsights({ days: 30 });
  const c2 = collectInsights({ days: 30 });
  check('短時間の再呼び出しはキャッシュを返す', c1 === c2);
  clearInsightsCache();
  check('キャッシュを捨てれば取り直す', collectInsights({ days: 30 }) !== c1);

  const cost = estimateCost({ avgChars: 300, perDay: 80 });
  check('費用試算: 1件あたり1円未満', Number(cost.perCallJpy) > 0 && Number(cost.perCallJpy) < 1, cost.perCallJpy);
  check('費用試算: 月額が出る', cost.perMonthJpy > 0);
  check('キーワード定義が12種', BLOCK_KEYWORDS.length === 12);
}

// ─── 9. HTTP (画面とAPI) ───
console.log('9. 画面とAPI');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/staff')).text();
  check('権限画面が出る', html.includes('担当者と権限') && html.includes('どの権限が要るか'));
  check('3系統の見出しが出る', html.includes('決裁権限') && html.includes('エスカレーション') && html.includes('操作権限'));
  check('担当者カードが出る', html.includes('中原 大輔') && html.includes('田中'));
  check('権限のチェックボックスが出る', html.includes('class="p-chk"'));
  check('誰も持っていない権限に警告が出る', html.includes('誰も持っていません'));
  check('変更履歴が出る', html.includes('権限の変更履歴'));
  check('サイドバーに導線が出る', html.includes('担当者と権限'));

  const rNew = await jpost('/api/staff', { displayName: '新人', userKey: 'new@b-faith.biz' });
  check('担当者追加API', rNew.status === 200);
  const newId = (await rNew.json()).id;
  check('重複キーは400', (await jpost('/api/staff', { displayName: 'x', userKey: 'new@b-faith.biz' })).status === 400);
  check('表示名なしは400', (await jpost('/api/staff', { displayName: '', userKey: 'a@b.c' })).status === 400);

  const rSave = await jpost(`/api/staff/${newId}`, { displayName: '新人A', permissions: ['D0', 'S1'] });
  check('保存API (基本情報+権限)', rSave.status === 200 && (await rSave.json()).permissions.granted.length === 2);
  check('存在しない権限コードは400', (await jpost(`/api/staff/${newId}`, { permissions: ['NOPE'] })).status === 400);
  check('権限が配列でなければ無視 (基本情報だけ更新)', (await jpost(`/api/staff/${newId}`, { displayName: '新人B' })).status === 200);
  check('無効化API', (await jpost(`/api/staff/${newId}/deactivate`, {})).status === 200);
  check('無効化の再送も200 (冪等)', (await jpost(`/api/staff/${newId}/deactivate`, {})).status === 200);
  check('無効化後の保存は400', (await jpost(`/api/staff/${newId}`, { displayName: 'x' })).status === 400);

  check('権限追加API', (await jpost('/api/permissions', { code: 'D9', kind: 'decision', name: 'テスト決裁' })).status === 200);
  check('権限編集API', (await jpost('/api/permissions/D9', { name: 'テスト決裁2' })).status === 200);
  check('権限の有効/無効API', (await jpost('/api/permissions/D9/active', { active: false })).status === 200);
  check('active が boolean でなければ400', (await jpost('/api/permissions/D9/active', { active: 'no' })).status === 400);
  check('独自権限の削除API', (await jpost('/api/permissions/D9/delete', {})).status === 200);
  check('既定権限の削除は400', (await jpost('/api/permissions/D0/delete', {})).status === 400);
  check('存在しない権限の編集は400', (await jpost('/api/permissions/ZZZZ', { name: 'x' })).status === 400);

  const ins = await (await fetch(base + '/admin/insights')).text();
  check('調査画面が出る', ins.includes('実データ調査') && ins.includes('締めまでに何件さばく必要があるか'));
  check('締め時刻が画面に出る', ins.includes('09:00') && ins.includes('12:30') && ins.includes('14:30'));
  check('費用試算が出る', ins.includes('1ヶ月あたり'));
  check('注文番号の埋まり率が出る', ins.includes('注文番号がどれだけ埋まっているか'));
  check('期間の切替リンクが出る', ins.includes('?days=90'));
  const ins90 = await (await fetch(base + '/admin/insights?days=90')).text();
  check('期間を変えても出る', ins90.includes('直近 <b>90日</b>'));
  const insBad = await (await fetch(base + '/admin/insights?days=abc')).text();
  check('不正な期間は既定30日に丸める', insBad.includes('直近 <b>30日</b>'));

  const adm = await (await fetch(base + '/admin')).text();
  check('運用管理から調査画面への導線がある', adm.includes('/apps/inquiry-hub/admin/insights'));

  // ⭐管理者を限定したときのガード (env は毎回読むのでその場で切り替えて確認できる)
  {
    const rw = await (await fetch(base + '/staff')).text();
    check('env未設定のときは「誰でも変更できる」注意が出る', rw.includes('誰でもこの表を変更できます'));

    process.env.INQUIRY_HUB_PERMISSION_ADMINS = 'boss@b-faith.biz';
    check('管理者以外の担当者追加は403', (await jpost('/api/staff', { displayName: 'x', userKey: 'x@y.z' })).status === 403);
    check('管理者以外の保存は403', (await jpost(`/api/staff/${bob.id}`, { displayName: 'x' })).status === 403);
    check('管理者以外の無効化は403', (await jpost(`/api/staff/${bob.id}/deactivate`, {})).status === 403);
    check('管理者以外の権限追加は403', (await jpost('/api/permissions', { code: 'X1', kind: 'system', name: 'x' })).status === 403);
    check('管理者以外の権限編集は403', (await jpost('/api/permissions/D0', { localNote: 'x' })).status === 403);
    check('管理者以外の有効/無効は403', (await jpost('/api/permissions/D0/active', { active: false })).status === 403);
    check('管理者以外の権限削除は403', (await jpost('/api/permissions/D0/delete', {})).status === 403);
    check('403でも実データは変わっていない', staff.getStaff(bob.id).is_active === 1 && staff.getPermission('D0').is_active === 1);

    const ro = await (await fetch(base + '/staff')).text();
    check('閲覧のみの表示になる', ro.includes('閲覧のみです'));
    // ボタンや入力欄そのものが出ないことを見る (JS側には querySelector 用の文字列が残るので属性で判定)
    check('閲覧のみでは保存ボタンが出ない', !ro.includes('class="pri s-save"'));
    check('閲覧のみでは追加フォームが出ない', !ro.includes('id="newName"'));
    check('閲覧のみでは権限編集セクションが出ない', !ro.includes('id="npCode"'));
    check('閲覧のみでもチェックの状態は見える', ro.includes('class="p-chk"') && ro.includes('disabled'));

    delete process.env.INQUIRY_HUB_PERMISSION_ADMINS;
    check('env を外せばまた編集できる', (await jpost(`/api/staff/${bob.id}`, { displayName: '田中' })).status === 200);
  }

  server.close();
}

// ─── 10. inline script の健全性 (2026-08-27 の事故防止: 終了タグでHTMLが壊れないか) ───
console.log('10. HTMLの健全性');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json(), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  for (const p of ['/staff', '/admin/insights']) {
    const html = await (await fetch(base + p)).text();
    const open = (html.match(/<script/g) || []).length;
    const close = (html.match(/<\/script>/g) || []).length;
    check(`${p}: scriptの開始と終了が一致 (${open}/${close})`, open === close);
    const so = (html.match(/<style/g) || []).length;
    const sc = (html.match(/<\/style>/g) || []).length;
    check(`${p}: styleの開始と終了が一致 (${so}/${sc})`, so === sc);
  }
  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
