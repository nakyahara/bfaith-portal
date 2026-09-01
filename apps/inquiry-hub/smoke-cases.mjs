// 📦返品・交換案件 (return-cases.js + router /cases) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-cases.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-cases-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-cases-'));
process.env.DATA_DIR = workDir;

const { initInquiryHubDB, getDB } = await import('./db.js');
const rc = await import('./return-cases.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = fn => { try { fn(); return false; } catch { return true; } };
const errOf = fn => { try { fn(); return ''; } catch (e) { return String(e.message || e); } };

// ─── seed ───
db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','テスト店','info@example.com')`).run();
const shopId = db.prepare('SELECT id FROM shops').get().id;
const mkInquiry = (ext, { subject = '商品について', assignee = '田中', order = 'IH-260821-10428',
  product = '充電式ハンディファン', body = '使用中に電源が落ちます。不良かと思います。交換していただけますか。' } = {}) => {
  const at = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  const id = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
      internal_status, assigned_user_id, order_number, product_name, customer_name, received_at, last_message_at)
    VALUES ('email', ?, ?, ?, 'open', ?, ?, ?, '佐藤 美咲', ?, ?)`)
    .run(shopId, ext, subject, assignee, order, product, at, at).lastInsertRowid;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, is_incoming,
      message_body_text, received_at) VALUES (?,?,'customer',1,?,?)`).run(id, 'msg-' + ext, body, at);
  return id;
};

// ─── 1. 日付 (JST) ───
console.log('1. 日付ユーティリティ');
{
  // ⭐toISOString をそのまま使うと JST の日付が1日ずれる (feedback_jst_to_iso_string_trap)
  check('UTC 15:00Z は JST では翌日', rc.jstDate('2026-09-01T15:00:00Z') === '2026-09-02');
  check('UTC 00:00Z は JST では同日', rc.jstDate('2026-09-01T00:00:00Z') === '2026-09-01');
  check('JST日付 → UTC ISO', rc.jstDateToIso('2026-09-04') === '2026-09-04T00:00:00Z');
  check('不正な日付は null', rc.jstDateToIso('2026/09/04') === null && rc.jstDateToIso('') === null);
  // 2026-09-04 は金曜。1営業日後 = 月曜 (土日を飛ばす)
  const fri = new Date('2026-09-04T03:00:00Z');
  check('金曜の1営業日後は月曜', rc.businessDaysFromNow(1, fri) === '2026-09-07', rc.businessDaysFromNow(1, fri));
  check('金曜の0営業日後は金曜のまま', rc.businessDaysFromNow(0, fri) === '2026-09-04');
  const sat = new Date('2026-09-05T03:00:00Z');
  check('土曜の0営業日後は月曜に寄る', rc.businessDaysFromNow(0, sat) === '2026-09-07', rc.businessDaysFromNow(0, sat));
  check('期限前は超過0', rc.overdueDays('2026-09-10T00:00:00Z', new Date('2026-09-09T00:00:00Z')) === 0);
  check('期限なしは超過0', rc.overdueDays(null) === 0);
  // ⭐壊れた値が1行あるだけでボードが真っ白にならないこと (fail-soft)
  check('壊れた日時でも落ちない',
    rc.jstDate('こわれた') === '' && rc.overdueDays('こわれた') === 0 && rc.jstDate('+999999-01-01T00:00:00Z') === '');
  // ⭐当日は超過にしない (9:00 を過ぎただけで赤くしない。その日のうちにやればいい)
  check('期限当日の夕方はまだ超過0',
    rc.overdueDays('2026-09-09T00:00:00Z', new Date('2026-09-09T09:00:00Z')) === 0,
    String(rc.overdueDays('2026-09-09T00:00:00Z', new Date('2026-09-09T09:00:00Z'))));
  check('翌日になったら1日超過',
    rc.overdueDays('2026-09-09T00:00:00Z', new Date('2026-09-10T01:00:00Z')) === 1,
    String(rc.overdueDays('2026-09-09T00:00:00Z', new Date('2026-09-10T01:00:00Z'))));
  check('2日過ぎたら2日超過',
    rc.overdueDays('2026-09-01T00:00:00Z', new Date('2026-09-03T00:00:00Z')) === 2,
    String(rc.overdueDays('2026-09-01T00:00:00Z', new Date('2026-09-03T00:00:00Z'))));
}

// ─── 2. 案件になりそうかの検知 ───
console.log('2. 案件化の候補検知 (⭐候補を出すだけ・自動では案件にしない)');
{
  check('「交換」を拾う', rc.detectCaseKeywords('件名', '交換していただけますか').includes('交換'));
  check('「不良」を拾う', rc.detectCaseKeywords('初期不良の件', '').includes('初期不良'));
  check('「割れて」を拾う', rc.detectCaseKeywords('', '到着時に割れていました').includes('割れて'));
  check('関係ない問い合わせは拾わない', rc.detectCaseKeywords('送料について', '沖縄への送料はいくらですか').length === 0);
  check('引用部分は見ない', rc.detectCaseKeywords('お礼', 'ありがとうございました\n> 返品の件ですが').length === 0);
  // ⭐「返品できますか」という**質問**もキーワードには当たる。だから自動案件化はしない
  check('質問文もキーワードには当たる (だから人が押す設計)', rc.detectCaseKeywords('', '返品できますか？').length > 0);
}

// ─── 3. 案件の作成 ───
console.log('3. 案件の作成 (⭐必須入力は種別と次回確認日の2つだけ)');
let caseId, caseNo;
{
  const inq = mkInquiry('inq-1');
  check('種別が不正なら作れない', throws(() => rc.createCase({ inquiryId: inq, caseType: 'NOPE', nextActionDate: '2026-09-04' })));
  check('次回確認日がなければ作れない',
    errOf(() => rc.createCase({ inquiryId: inq, caseType: 'MANUFACTURER' })).includes('次回確認日'));
  const r = rc.createCase({ inquiryId: inq, caseType: 'MANUFACTURER', nextActionDate: '2026-09-04', actor: '田中' });
  caseId = r.id; caseNo = r.case_no;
  check('案件番号が RC-年-連番', /^RC-\d{4}-\d{4}$/.test(r.case_no), r.case_no);
  const c = rc.getCase(caseId);
  check('担当は問い合わせから自動で入る', c.assigned_user_id === '田中');
  check('注文番号・商品名を引き継ぐ', c.order_no === 'IH-260821-10428' && c.product_name === '充電式ハンディファン');
  check('最初は自社対応から始まる', c.waiting_on === 'SELF' && c.stage === 'RECEIVED');
  check('waiting_since が入る', !!c.waiting_since);
  const steps = rc.listSteps(caseId);
  check('メーカー対応の工程が11件できる', steps.length === 11, String(steps.length));
  check('要否未確定の工程がある (返送不要・直送の分かれ道)',
    steps.some(s => s.necessity_status === 'undecided'));
  check('待ち先に「メーカー」を持つ工程がある', steps.some(s => s.waiting_party === 'メーカー'));
  check('⭐外部を担当者にしない (assignee は全部社内)', steps.every(s => s.assignee_id === '田中'));
  check('問い合わせと多対多で紐づく (origin)',
    db.prepare("SELECT link_role FROM case_inquiries WHERE case_id=? AND inquiry_id=?").get(caseId, inq).link_role === 'origin');
  check('案件化の判断が保存される', rc.getTriage(inq)?.result === 'case_created');
  check('作成が履歴に残る', rc.listEvents(caseId).some(e => e.event_type === 'case_created'));

  // 種別ごとの工程数
  const i2 = mkInquiry('inq-2');
  const r2 = rc.createCase({ inquiryId: i2, caseType: 'RETURN_REFUND', nextActionDate: '2026-09-04', actor: '中村' });
  check('返品・返金は9工程', rc.listSteps(r2.id).length === 9, String(rc.listSteps(r2.id).length));
  check('連番が進む', r2.case_no !== caseNo);
  const i3 = mkInquiry('inq-3');
  const r3 = rc.createCase({ inquiryId: i3, caseType: 'EXCHANGE', nextActionDate: '2026-09-04', actor: '山本' });
  check('交換・代品は10工程', rc.listSteps(r3.id).length === 10, String(rc.listSteps(r3.id).length));
  check('返金工程は返品・返金にだけある',
    rc.listSteps(r2.id).some(s => s.step_type === 'execute_refund') &&
    !rc.listSteps(r3.id).some(s => s.step_type === 'execute_refund'));

  // 担当者がいない問い合わせからは作れない (⭐外部待ちでも社内担当は必ず要る)
  const i4 = mkInquiry('inq-4', { assignee: null });
  check('担当者が決まっていなければ作れない',
    errOf(() => rc.createCase({ inquiryId: i4, caseType: 'OTHER', nextActionDate: '2026-09-04' })).includes('担当者'));
  check('操作者がいれば担当に入る',
    rc.getCase(rc.createCase({ inquiryId: i4, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '小林' }).id)
      .assigned_user_id === '小林');
}

// ─── 4. 工程の操作 ───
console.log('4. 工程の操作');
{
  const steps = rc.listSteps(caseId);
  const s1 = steps[0];                       // 写真・症状・ロット番号を揃える (required)
  const undecided = steps.find(s => s.necessity_status === 'undecided');

  check('不正な操作は弾く', throws(() => rc.updateStep(caseId, s1.id, 'nope', { actor: '田中' })));
  check('他の案件の工程は動かせない', throws(() => rc.updateStep(caseId + 999, s1.id, 'complete', { actor: '田中' })));

  rc.updateStep(caseId, s1.id, 'complete', { actor: '田中' });
  const after = rc.listSteps(caseId).find(s => s.id === s1.id);
  check('完了にすると completed になる', after.progress_status === 'completed');
  check('完了者と完了日時が残る', after.completed_by === '田中' && !!after.completed_at);

  rc.updateStep(caseId, undecided.id, 'skip', { actor: '田中' });
  const skipped = rc.listSteps(caseId).find(s => s.id === undecided.id);
  check('対応不要にすると not_required', skipped.necessity_status === 'not_required');

  const undecided2 = rc.listSteps(caseId).find(s => s.necessity_status === 'undecided');
  rc.updateStep(caseId, undecided2.id, 'need', { actor: '田中' });
  check('必要にすると required + 未着手',
    rc.listSteps(caseId).find(s => s.id === undecided2.id).necessity_status === 'required');

  // ⭐工程を「回答・到着待ち」にすると案件の待ち先もそこへ寄る (二重入力を作らない)
  // 秒精度なので、同一秒での比較にならないよう起点を過去に戻してから確かめる
  db.prepare("UPDATE return_cases SET waiting_since = '2026-08-01T00:00:00Z' WHERE id = ?").run(caseId);
  const before = rc.getCase(caseId);
  const makerStep = rc.listSteps(caseId).find(s => s.step_type === 'wait_manufacturer_response');
  rc.updateStep(caseId, makerStep.id, 'wait', { actor: '田中', note: '技術部で調査中' });
  const c2 = rc.getCase(caseId);
  check('工程を待ちにすると案件がメーカー待ちになる', c2.waiting_on === 'SUPPLIER', c2.waiting_on);
  check('⭐待ち先が変わったので waiting_since も更新される', c2.waiting_since !== before.waiting_since);
  check('メモが残る', rc.listSteps(caseId).find(s => s.id === makerStep.id).note === '技術部で調査中');
  check('工程の変更が履歴に残る', rc.listEvents(caseId).some(e => e.event_type === 'step_changed'));

  // 戻せる (自動判定が誤ったときに人が直せる)
  rc.updateStep(caseId, s1.id, 'undo', { actor: '田中' });
  const undone = rc.listSteps(caseId).find(s => s.id === s1.id);
  check('戻すと未着手に戻り完了者も消える',
    undone.progress_status === 'not_started' && !undone.completed_at && !undone.completed_by);
  rc.updateStep(caseId, s1.id, 'complete', { actor: '田中' });
}

// ─── 5. 待ち先・返金の記録 ───
console.log('5. 待ち先と返金の記録');
{
  const c = rc.setWaiting(caseId, { waitingOn: 'CUSTOMER', actor: '田中' });
  check('⭐外部待ちにすると次回確認日が自動で入る (空欄を許さない)', !!c.next_action_at);
  const same = rc.getCase(caseId);
  const c2 = rc.setWaiting(caseId, { waitingOn: 'CUSTOMER', nextActionDate: '2026-09-30', actor: '田中' });
  check('待ち先が同じなら waiting_since は変わらない', c2.waiting_since === same.waiting_since);
  check('次回確認日は変えられる', rc.jstDate(c2.next_action_at) === '2026-09-30');
  check('不正な待ち先は弾く', throws(() => rc.setWaiting(caseId, { waitingOn: 'NOPE' })));
  check('担当は空にできない', throws(() => rc.setAssignee(caseId, '  ', '田中')));

  const r = rc.setRefund(caseId, { expected: '3,280', completed: '', ref: 'RK-123', actor: '田中' });
  check('カンマ付きの金額を受け付ける', r.refund_expected_amount === 3280);
  check('実績額は空のまま持てる', r.refund_completed_amount === null);
  check('マイナスの金額は弾く', throws(() => rc.setRefund(caseId, { expected: -1 })));
  check('返金の記録が履歴に残る', rc.listEvents(caseId).some(e => e.event_type === 'refund_recorded'));
  rc.setWaiting(caseId, { waitingOn: 'SUPPLIER', actor: '田中' });
}

// ─── 6. 完了ゲート (⭐ここが要) ───
console.log('6. 完了ゲート (条件付き fail-closed)');
{
  const blockers = rc.blockersOf(caseId);
  check('未処理の工程が残っている', blockers.total > 0);
  const r = rc.closeCase(caseId, { actor: '田中' });
  check('⭐必要な工程が残っていると完了できない', r.ok === false);
  check('何が残っているかを返す', r.blockers.steps.length > 0);
  check('要否未確定も完了を止める',
    rc.blockersOf(caseId).steps.some(s => s.necessity_status === 'undecided'));

  check('例外完了は理由がないと弾く', throws(() => rc.closeCase(caseId, { force: true, actor: '田中' })));
  check('例外完了は詳細メモがないと弾く',
    throws(() => rc.closeCase(caseId, { force: true, reasonCode: 'other', actor: '田中' })));

  // 別案件で「全部片付ければ完了できる」ことを確認する
  const inq = mkInquiry('inq-close');
  const c = rc.createCase({ inquiryId: inq, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '中村' });
  for (const s of rc.listSteps(c.id)) rc.updateStep(c.id, s.id, 'complete', { actor: '中村' });
  check('全部片付けば完了できる', rc.closeCase(c.id, { actor: '中村' }).ok === true);
  const closed = rc.getCase(c.id);
  check('完了すると待ち先が「完了」になる', closed.status === 'completed' && closed.waiting_on === 'NONE');
  check('完了日時と完了者が残る', !!closed.closed_at && closed.closed_by === '中村');
  check('⭐二重完了しない (already で返る)', rc.closeCase(c.id, { actor: '別人' }).already === true);
  check('完了した案件の工程は変更できない', throws(() => rc.updateStep(c.id, rc.listSteps(c.id)[0].id, 'undo', {})));
  check('完了した案件は待ち先も変えられない', throws(() => rc.setWaiting(c.id, { waitingOn: 'SELF' })));

  // 例外完了 (理由 + メモ)
  const inq2 = mkInquiry('inq-exc');
  const c2 = rc.createCase({ inquiryId: inq2, caseType: 'RETURN_REFUND', nextActionDate: '2026-09-04', actor: '小林' });
  const r2 = rc.closeCase(c2.id, { force: true, reasonCode: 'customer_unreachable', note: '3回連絡したが不通', actor: '小林' });
  check('理由とメモがあれば例外完了できる', r2.ok === true);
  check('例外の理由が残る', rc.getCase(c2.id).close_reason_code === 'customer_unreachable');
  check('⭐例外完了は履歴に別イベントで残る',
    rc.listEvents(c2.id).some(e => e.event_type === 'case_closed_exception'));
  check('未完了の工程が残っていたことも記録される', r2.blockers.total > 0);

  // 開け直せる
  rc.reopenCase(c2.id, '小林');
  check('開け直すと自社対応に戻る',
    rc.getCase(c2.id).status === 'active' && rc.getCase(c2.id).waiting_on === 'SELF');
  rc.closeCase(c2.id, { force: true, reasonCode: 'other', note: '再クローズ', actor: '小林' });
}

// ─── 6b. Codexレビューで塞いだ穴 (2026-09-01) ───
console.log('6b. 完了ゲートをすり抜ける経路を塞げているか');
{
  const inq = mkInquiry('inq-gate');
  const c = rc.createCase({ inquiryId: inq, caseType: 'RETURN_REFUND', nextActionDate: '2026-09-04', actor: '中村' });
  const refundStep = rc.listSteps(c.id).find(s => s.step_type === 'execute_refund');
  check('返金工程は最初から「必要」', refundStep.necessity_status === 'required');

  // ⭐High: 必要と決まっている工程を無理由で「対応不要」にできると、ゲートが無意味になる
  check('必要な工程は skip で外せない',
    errOf(() => rc.updateStep(c.id, refundStep.id, 'skip', { actor: '中村' })).includes('対応不要」にできません'));
  check('外すには理由が要る',
    errOf(() => rc.updateStep(c.id, refundStep.id, 'skip_required', { actor: '中村' })).includes('理由'));
  rc.updateStep(c.id, refundStep.id, 'skip_required', { reason: '顧客が返金を辞退したため', actor: '中村' });
  check('理由があれば外せる',
    rc.listSteps(c.id).find(s => s.id === refundStep.id).necessity_status === 'not_required');
  check('⭐外した記録は履歴に別イベントで残る',
    rc.listEvents(c.id).some(e => e.event_type === 'step_skipped_exception'));
  check('理由が工程のメモに残る',
    rc.listSteps(c.id).find(s => s.id === refundStep.id).note === '顧客が返金を辞退したため');

  // ⭐Medium: 「戻す」が実際には戻っていない (not_required のまま) 問題
  rc.updateStep(c.id, refundStep.id, 'undo', { actor: '中村' });
  const back = rc.listSteps(c.id).find(s => s.id === refundStep.id);
  check('⭐戻すと必要性も元に戻る (not_required のまま残らない)', back.necessity_status === 'required');
  check('戻すと未着手になる', back.progress_status === 'not_started');
  check('戻した工程はまた完了を止める',
    rc.blockersOf(c.id).steps.some(s => s.id === refundStep.id));

  // 要否未確定から「対応不要」にしたものを戻すと、要否未確定に戻る
  const undecided = rc.listSteps(c.id).find(s => s.necessity_status === 'undecided');
  rc.updateStep(c.id, undecided.id, 'skip', { actor: '中村' });
  rc.updateStep(c.id, undecided.id, 'undo', { actor: '中村' });
  check('要否未確定から外したものは要否未確定に戻る',
    rc.listSteps(c.id).find(s => s.id === undecided.id).necessity_status === 'undecided');
}

console.log('6b-2. 外した工程を戻すと「外す直前」に戻る (Codex R2)');
{
  const inq = mkInquiry('inq-undo2');
  const c = rc.createCase({ inquiryId: inq, caseType: 'RETURN_REFUND', nextActionDate: '2026-09-04', actor: '中村' });
  // テンプレートでは要否未確定 → 人が「必要」と決めた → 外した → 戻す
  const s = rc.listSteps(c.id).find(x => x.necessity_status === 'undecided');
  rc.updateStep(c.id, s.id, 'need', { actor: '中村' });
  rc.updateStep(c.id, s.id, 'skip_required', { reason: '顧客が返送を希望しないため', actor: '中村' });
  rc.updateStep(c.id, s.id, 'undo', { actor: '中村' });
  check('⭐人が「必要」と決めた判断は戻しても消えない (要否未確定まで巻き戻らない)',
    rc.listSteps(c.id).find(x => x.id === s.id).necessity_status === 'required',
    rc.listSteps(c.id).find(x => x.id === s.id).necessity_status);
  check('要否未確定の工程は skip_required では外せない (通常の「対応不要」を使う)',
    errOf(() => rc.updateStep(c.id, rc.listSteps(c.id).find(x => x.necessity_status === 'undecided').id,
      'skip_required', { reason: 'テスト', actor: '中村' })).includes('要否がまだ決まっていない'));
}

console.log('6c. 権限 (例外操作)');
{
  check('担当者マスタが空のうちは通す + 未整備と分かる',
    rc.canDoException('誰か').allowed === true && rc.canDoException('誰か').unmanaged === true);

  // 担当者を1人登録すると、権限を持つ人だけが例外操作できる
  const staff = await import('./staff.js');
  const s1 = staff.createStaff({ userKey: 'tanaka@example.com', displayName: '田中' }, 'smoke');
  staff.setStaffPermissions(s1.id, ['D0'], 'smoke');
  const s2 = staff.createStaff({ userKey: 'nakamura@example.com', displayName: '中村' }, 'smoke');
  staff.setStaffPermissions(s2.id, ['D3'], 'smoke');

  check('マスタ整備後は未整備フラグが下りる', rc.canDoException('田中').unmanaged === false);
  check('D3 を持たない人は例外操作できない', rc.canDoException('田中').allowed === false);
  check('D3 を持つ人は例外操作できる', rc.canDoException('中村').allowed === true);
  check('登録されていない人は例外操作できない', rc.canDoException('知らない人').allowed === false);
  check('⭐空白の有無で別人にしない', rc.canDoException('中 村').allowed === true);
  check('メールアドレスでも引ける', rc.canDoException('nakamura@example.com').allowed === true);

  const inq = mkInquiry('inq-perm');
  const c = rc.createCase({ inquiryId: inq, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '田中' });
  check('⭐権限のない人は例外完了できない',
    errOf(() => rc.closeCase(c.id, { force: true, reasonCode: 'other', note: 'テスト', actor: '田中' }))
      .includes('権限がありません'));
  check('権限のある人なら例外完了できる',
    rc.closeCase(c.id, { force: true, reasonCode: 'other', note: 'テスト', actor: '中村' }).ok === true);

  const inq2 = mkInquiry('inq-perm2');
  const c2 = rc.createCase({ inquiryId: inq2, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '田中' });
  const step = rc.listSteps(c2.id)[0];
  check('権限のない人は必要な工程を外せない',
    errOf(() => rc.updateStep(c2.id, step.id, 'skip_required', { reason: 'テスト', actor: '田中' }))
      .includes('権限がありません'));

  // ⭐同姓の別人に権限が化けないか (Codex R2)
  const s3 = staff.createStaff({ userKey: 'nakamura2@example.com', displayName: '中村' }, 'smoke');
  check('同じ表示名が2人いたら、表示名だけでは通さない', rc.canDoException('中村').allowed === false);
  check('メールアドレス (一意) なら引ける', rc.canDoException('nakamura@example.com').allowed === true);
  staff.deactivateStaff(s3.id, 'smoke');
  check('同名の片方を無効にすれば表示名でも引ける', rc.canDoException('中村').allowed === true);

  // ⭐「全員を無効化した」を未導入と取り違えない (取り違えると全員に例外権限が開く)
  staff.deactivateStaff(s1.id, 'smoke');
  staff.deactivateStaff(s2.id, 'smoke');
  check('⭐全員無効にしても未整備扱いにはならない', rc.canDoException('中村').unmanaged === false);
  check('⭐全員無効なら誰も例外操作できない (設定事故で権限が開かない)',
    rc.canDoException('中村').allowed === false && rc.canDoException('誰か').allowed === false);

  // 後続テストのために、一度も登録がない状態へ戻す (未導入の再現)
  db.prepare('DELETE FROM staff_permissions').run();
  db.prepare('DELETE FROM staff_members').run();
  check('登録を消せば未導入に戻る', rc.canDoException('誰か').unmanaged === true);
}

console.log('6d. 完了後の書き換え・重複作成・日付の検証');
{
  const inq = mkInquiry('inq-frozen');
  const c = rc.createCase({ inquiryId: inq, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '小林' });
  rc.closeCase(c.id, { force: true, reasonCode: 'other', note: '確認用', actor: '小林' });
  check('⭐完了した案件の担当は変えられない', throws(() => rc.setAssignee(c.id, '別人', '小林')));
  check('⭐完了した案件の返金額は書き換えられない', throws(() => rc.setRefund(c.id, { expected: 999, actor: '小林' })));
  rc.reopenCase(c.id, '小林');
  check('開け直せば直せる', rc.setRefund(c.id, { expected: 999, actor: '小林' }).refund_expected_amount === 999);

  // ⭐二重作成 (ボタンの二度押し・再送) を止める
  const inq2 = mkInquiry('inq-dup');
  rc.createCase({ inquiryId: inq2, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '小林' });
  let dupErr = null;
  try { rc.createCase({ inquiryId: inq2, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '小林' }); }
  catch (e) { dupErr = e; }
  check('同じ問い合わせに未完了案件があると止まる', dupErr?.code === 'DUPLICATE_CASE');
  check('止めるときは既存の案件番号を伝える', /^RC-/.test(dupErr?.caseNo || ''));
  check('明示すれば別案件として作れる (商品Aは返金・商品Bは代品)',
    !!rc.createCase({ inquiryId: inq2, caseType: 'EXCHANGE', nextActionDate: '2026-09-04',
      allowDuplicate: true, actor: '小林' }).case_no);

  // ⭐実在しない日付を弾く (APIを直に叩かれたときの入口)
  check('2026-02-31 は弾く', rc.jstDateToIso('2026-02-31') === null);
  check('2026-13-01 は弾く', rc.jstDateToIso('2026-13-01') === null);
  check('うるう年の2/29は通す', rc.jstDateToIso('2028-02-29') === '2028-02-29T00:00:00Z');
  check('平年の2/29は弾く', rc.jstDateToIso('2026-02-29') === null);

  // ⭐待ち先を変えたら期限も取り直す (前の待ち先の期限を引きずらない)
  const inq3 = mkInquiry('inq-due');
  const c3 = rc.createCase({ inquiryId: inq3, caseType: 'OTHER', nextActionDate: '2026-09-04', actor: '小林' });
  db.prepare("UPDATE return_cases SET waiting_on='CUSTOMER', next_action_at='2026-08-01T00:00:00Z' WHERE id=?").run(c3.id);
  const moved = rc.setWaiting(c3.id, { waitingOn: 'SUPPLIER', actor: '小林' });
  check('⭐待ち先が変わったら古い期限を引き継がない', moved.next_action_at > '2026-08-01T00:00:00Z');
  const keep = rc.setWaiting(c3.id, { waitingOn: 'SUPPLIER', nextActionDate: '2026-10-01', actor: '小林' });
  check('明示した日付はそのまま使う', rc.jstDate(keep.next_action_at) === '2026-10-01');
}

// ─── 7. ボード ───
console.log('7. ボード');
{
  const rows = rc.listBoardCases({ includeCompleted: false });
  check('未完了だけを出せる', rows.every(r => r.status === 'active'));
  check('次にやることが付く', rows.every(r => r.next_step_label || r.steps_total === 0));
  check('工程の進み具合が付く', rows.every(r => typeof r.steps_done === 'number' && r.steps_total > 0));
  check('滞留日数が付く', rows.every(r => typeof r.stagnant === 'number'));
  const withDone = rc.listBoardCases({ includeCompleted: true });
  check('完了も含められる', withDone.length > rows.length);
  check('担当で絞れる', rc.listBoardCases({ assignee: '田中' }).every(r => r.assigned_user_id === '田中'));
  check('種別で絞れる', rc.listBoardCases({ caseType: 'EXCHANGE' }).every(r => r.case_type === 'EXCHANGE'));

  const waitCols = rc.boardColumns('wait');
  check('誰待ちの列は7つ', waitCols.length === 7, String(waitCols.length));
  check('列に自社対応がある', waitCols.some(c => c.label === '自社対応'));
  const stageCols = rc.boardColumns('stage');
  check('工程の列は取下げを除く7つ', stageCols.length === 7, String(stageCols.length));
  check('⭐どちらのビューでも同じ案件が置ける (列は同じデータの見方を変えるだけ)',
    waitCols.length > 0 && stageCols.length > 0);

  const next = rc.nextStepOf(rc.listSteps(caseId));
  check('次にやること = 最初の必要かつ未完了の工程', !!next && next.necessity_status === 'required');
}

// ─── 8. 整合性チェック ───
console.log('8. 整合性チェック (カンバンが嘘をついていないか)');
{
  // 外部待ちなのに期限がない案件を作る (DBを直接いじって異常を再現)
  db.prepare("UPDATE return_cases SET next_action_at = NULL WHERE id = ?").run(caseId);
  const found = rc.findInconsistencies();
  check('外部待ちなのに期限なしを検出する', found.some(f => f.kind === 'no_due' && f.rows.length > 0));
  rc.setWaiting(caseId, { waitingOn: 'SUPPLIER', nextActionDate: '2026-09-10', actor: '田中' });
  check('直すと検出されなくなる',
    !rc.findInconsistencies().find(f => f.kind === 'no_due')?.rows.some(r => r.id === caseId));

  const d = rc.digestCounts();
  check('ダイジェストは0件でも数を返す (dead-man)', typeof d.total === 'number');
  check('期限超過・本日期限・期限なしに分かれる',
    Array.isArray(d.overdue) && Array.isArray(d.dueToday) && Array.isArray(d.noDue));
  check('待ち先ごとの件数が出る', typeof d.byWaiting.SUPPLIER === 'number');
}

// ─── 9. 画面とAPI ───
console.log('9. 画面とAPI');
{
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST',
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const board = await (await fetch(base + '/cases')).text();
  check('ボードが出る', board.includes('返品・交換案件'));
  check('3つのビューがある', board.includes('対応状況') && board.includes('処理工程') && board.includes('一覧'));
  check('既定は誰待ちの列', board.includes('メーカー・仕入先待ち') && board.includes('自社対応'));
  check('カードに次にやることが出る', board.includes('次にやること'));
  check('ドラッグしない理由が書いてある', board.includes('ドラッグ移動は入れていません'));
  check('問い合わせと案件は別完了だと書いてある', board.includes('問い合わせを完了にしても案件は残ります'));

  const stageView = await (await fetch(base + '/cases?view=stage')).text();
  check('工程ビューに切り替わる', stageView.includes('情報収集') && stageView.includes('返送・検品'));
  const listView = await (await fetch(base + '/cases?view=list')).text();
  check('一覧ビューは表で出る', listView.includes('<table') && listView.includes('案件番号'));

  const detail = await (await fetch(base + `/cases/${caseId}`)).text();
  check('案件詳細が出る', detail.includes(caseNo));
  check('次にやることが大きく出る', detail.includes('次にやること'));
  check('対応工程が出る', detail.includes('対応工程'));
  check('工程の内部コードは出すが画面用ラベルが主', detail.includes('メーカーの回答を確認'));
  check('返金の正はモールだと書いてある', detail.includes('返金の正データは各モールです'));
  check('関連する問い合わせが出る', detail.includes('関連する問い合わせ'));
  check('対応履歴が出る', detail.includes('対応履歴'));
  check('完了ボタンが出る', detail.includes('案件を完了'));
  check('存在しない案件は404', (await fetch(base + '/cases/999999')).status === 404);

  // 問い合わせ詳細の案件パネル
  const inqNew = mkInquiry('inq-panel');
  const inqPage = await (await fetch(base + `/inquiries/${inqNew}`)).text();
  check('候補バナーが出る', inqPage.includes('返品・交換の対応が残りそうです'));
  check('検出した言葉を見せる', inqPage.includes('交換'));
  check('案件化ボタンが出る', inqPage.includes('返品・交換案件として管理'));
  check('「今回は案件にしない」も出る', inqPage.includes('今回は案件にしない'));

  const noHit = mkInquiry('inq-nohit', { subject: '送料について', body: '沖縄への送料はいくらですか' });
  const noHitPage = await (await fetch(base + `/inquiries/${noHit}`)).text();
  check('関係ない問い合わせにはバナーを出さない', !noHitPage.includes('返品・交換の対応が残りそうです'));

  check('案件化API', (await jpost('/api/cases', { inquiryId: inqNew, caseType: 'EXCHANGE', nextActionDate: '2026-09-10' })).status === 200);
  const linked = await (await fetch(base + `/inquiries/${inqNew}`)).text();
  check('案件化後はパネルが案件表示に変わる', linked.includes('📦 返品・交換案件') && !linked.includes('対応が残りそうです'));
  check('問い合わせを完了にしても案件が残ると書いてある', linked.includes('返信が終わったことと、返金・代品が終わったことは別物'));

  check('種別なしの案件化は400', (await jpost('/api/cases', { inquiryId: inqNew, nextActionDate: '2026-09-10' })).status === 400);
  check('次回確認日なしの案件化は400', (await jpost('/api/cases', { inquiryId: inqNew, caseType: 'OTHER' })).status === 400);

  const stepId = rc.listSteps(caseId).find(s => s.necessity_status === 'required'
    && !['completed', 'exception'].includes(s.progress_status)).id;
  check('工程APIが動く', (await jpost(`/api/cases/${caseId}/steps/${stepId}`, { action: 'complete' })).status === 200);
  check('不正な操作は400', (await jpost(`/api/cases/${caseId}/steps/${stepId}`, { action: 'nope' })).status === 400);

  const closeRes = await (await jpost(`/api/cases/${caseId}/close`, {})).json();
  check('⭐完了APIは残りがあると ok:false で止める', closeRes.ok === false && closeRes.blockers.total > 0);
  check('止めるときは工程名を日本語で返す', closeRes.blockers.steps.every(s => s.label && !s.label.includes('_')));

  check('待ち先APIが動く',
    (await jpost(`/api/cases/${caseId}/waiting`, { waitingOn: 'CUSTOMER', nextActionDate: '2026-09-20' })).status === 200);
  check('返金APIが動く', (await jpost(`/api/cases/${caseId}/refund`, { expected: '1000' })).status === 200);
  check('「案件にしない」APIが動く', (await jpost(`/api/inquiries/${noHit}/no-case`, {})).status === 200);
  const declined = await (await fetch(base + `/inquiries/${noHit}`)).text();
  check('案件にしないと判断したら記録が出る', declined.includes('案件にしない'));
  check('やり直せる', (await jpost(`/api/inquiries/${noHit}/triage-reset`, {})).status === 200);

  // サイドバーの導線
  check('サイドバーに返品・交換案件が出る', board.includes('返品・交換案件</span>') || board.includes('nav-label">返品・交換案件'));

  await new Promise(r => server.close(r));
}

// ─── 10. 既存DBへの列追加 (Codex R2 High: リリースブロッカーだった) ───
// ⚠️ここで DATA_DIR を差し替えて別のDBを開くので、このセクションは必ず最後に置く
console.log('10. 既存DBに列が足りないときの移行');
{
  const Database = (await import('better-sqlite3')).default;
  const dir2 = fs.mkdtempSync(path.join(baseDir, 'smoke-cases-mig-'));
  // 先の版で作られた case_steps を再現する (template_necessity / necessity_before_skip だけが無い形)
  const oldDb = new Database(path.join(dir2, 'inquiry-hub.db'));
  oldDb.exec(`CREATE TABLE case_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL,
    step_type TEXT NOT NULL,
    necessity_status TEXT NOT NULL DEFAULT 'required',
    progress_status TEXT NOT NULL DEFAULT 'not_started',
    assignee_id TEXT, waiting_party TEXT, due_at TEXT, completed_at TEXT, completed_by TEXT,
    external_ref TEXT, note TEXT,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')))`);
  oldDb.prepare("INSERT INTO case_steps (case_id, step_type, necessity_status) VALUES (1,'wait_return_arrival','undecided')").run();
  oldDb.prepare("INSERT INTO case_steps (case_id, step_type, necessity_status) VALUES (1,'execute_refund','required')").run();
  oldDb.close();

  process.env.DATA_DIR = dir2;
  initInquiryHubDB();
  const db2 = getDB();
  const cols = db2.prepare('PRAGMA table_info(case_steps)').all().map(c => c.name);
  check('⭐既存DBにも template_necessity が足される (無いと案件作成が落ちる)', cols.includes('template_necessity'));
  check('⭐既存DBにも necessity_before_skip が足される', cols.includes('necessity_before_skip'));
  const rows = db2.prepare('SELECT step_type, template_necessity FROM case_steps ORDER BY id').all();
  check('既存の要否未確定はテンプレ値も要否未確定にする', rows[0].template_necessity === 'undecided');
  check('既存の必要な工程は required のまま', rows[1].template_necessity === 'required');
  // 移行後に案件を作れること (INSERT が落ちないこと) まで見る
  db2.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','移行テスト','mig@example.com')`).run();
  const sid = db2.prepare('SELECT id FROM shops').get().id;
  const at = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  const iid = db2.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
      internal_status, assigned_user_id, received_at) VALUES ('email',?,?,?,'open','田中',?)`)
    .run(sid, 'mig-1', '移行後の案件', at).lastInsertRowid;
  check('⭐移行後も案件を作れる',
    !!rc.createCase({ inquiryId: iid, caseType: 'RETURN_REFUND', nextActionDate: '2026-09-10', actor: '田中' }).case_no);
  try { fs.rmSync(dir2, { recursive: true, force: true }); } catch { /* 掃除できなくても結果は返す */ }
}

console.log(`\n${failed === 0 ? '✅' : '❌'} PASS ${passed} / FAIL ${failed}`);
try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* 掃除できなくても結果は返す */ }
process.exitCode = failed === 0 ? 0 : 1;
