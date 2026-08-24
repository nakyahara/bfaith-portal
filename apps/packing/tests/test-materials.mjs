/**
 * packing — 梱包資材の表示・現場登録 (materials.js) のテスト。
 * 要件 = AI_reference『梱包資材表示_要件定義_20260823.md』v1.7。
 *
 * 検証の要点 (Codex 4〜6巡目の必須項目):
 *   - combo_key 正規化 (合算・ソート・%エスケープ・不正明細)
 *   - 判定順 hidden → held → header → rule → candidates → unknown / AES 分岐 fail-closed
 *   - version CAS (ABA)・部分一意の同時登録・expected_delivery_code 文脈409
 *   - undo (1回だけ・期限・version一致・claim後は不可・pending取消)
 *   - outbox 遷移 (sweep/claim/失敗backoff/上限/stale回収/claim_token不一致の書き戻し無視/手動再送)
 *   - op_id 再現 (同一hash=同一応答・相違hash=409)・DDL CHECK・foreign_key_check
 *   - views の完了スナップショット固定
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-materials-test-'));
process.env.PACKING_MATERIAL_UNDO_SEC = '15';
process.env.PACKING_MATERIAL_UNDO_SECRET = 'test-secret';

const { initPickingDB } = await import('../../picking/db.js');
const { initPackingDB, getDB, utcNow } = await import('../db.js');
const {
  normalizeKeyText, comboKeyOf, resolveDelivery, judgeSlip, materialsForState,
  registerMaterial, undoMaterial, undoTokenFor, materialNotifyStep, manualResend,
  seedMaterialsData, onSlipCompleted, onSlipCompletionCleared, materialDailyCounts,
  classCandidates,
} = await import('../materials.js');
const { PackError } = await import('../service.js');

initPickingDB();
initPackingDB();
const db = getDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }
function throwsCode(fn, code) {
  try { fn(); } catch (e) {
    assert.ok(e instanceof PackError, `PackError 以外: ${e.message}`);
    assert.equal(e.code, code, `code ${e.code} (期待 ${code}): ${e.message}`);
    return e;
  }
  assert.fail(`エラーが出ない (期待 ${code})`);
}

// ─── seed (本番 seed と同じ構造の最小版) ───
seedMaterialsData({
  materials: [
    { code: 'nekopos_box', name: 'ネコポス段ボール', color: '#c9a227', sort_order: 10 },
    { code: 'tate_kraft_env', name: '縦長茶封筒', sort_order: 20 },
    { code: 'vinyl_s', name: '小サイズ白ビニール', sort_order: 40 },
    { code: 'box50', name: '50サイズ段ボール', sort_order: 80 },
    { code: 'letterpack500', name: 'レターパック500', sort_order: 200 },
    { code: 'old_env', name: '廃止予定封筒', sort_order: 500 },
  ],
  classes: [
    { class_value: 'ネコポス手動単品', sort_order: 10 },
    { class_value: 'AES《単品》', aes_kind: 'mail', sort_order: 20 },
    { class_value: 'AES《単品》【メール便以外】', aes_kind: 'other', sort_order: 30 },
    { class_value: 'AES《謎の新分類》', sort_order: 35 },          // aes_kind 未設定 → unknown
    { class_value: 'LINEギフト《単品、複数個を含む全て》', hide_card: 1, sort_order: 40 },
    { class_value: '50サイズ宅急便単品', sort_order: 50 },
  ],
  class_materials: [
    { class_value: 'ネコポス手動単品', codes: ['tate_kraft_env', 'vinyl_s', 'nekopos_box', 'old_env'] },
    { class_value: '50サイズ宅急便単品', codes: ['old_env'] },     // 後で old_env を無効化 → 有効候補ゼロ
  ],
  header_map: [
    { header_value: 'ヤマト(ネコポス)', base_delivery_code: 'nekopos' },
    { header_value: 'AES', base_delivery_code: 'aes' },
    { header_value: 'ヤマト宅急便【50サイズ専用】', base_delivery_code: 'takkyubin50' },
    { header_value: 'レターパック500', base_delivery_code: 'letterpack', material_code: 'letterpack500' },
  ],
}, 'test');

// ─── fixtures ───
let pickSeq = 0;
let packSeq = 0;
function makeBatch(cls, { slips = 1, status = 'packing' } = {}) {
  const now = utcNow();
  let pkBatchId = null;
  if (cls != null) {
    pkBatchId = ++pickSeq;
    db.prepare(`
      INSERT INTO pk_batches (id, tb_no, hikiate_class, work_date, composition,
        line_count, slip_count, total_qty, status, validity, csv_sha256, imported_by, created_at, updated_at)
      VALUES (?, ?, ?, '2026-08-24', '単品', 1, 1, 1, 'done', 'valid', 'sha', 'test', ?, ?)
    `).run(pkBatchId, `TB${pkBatchId}`, cls, now, now);
  }
  const id = ++packSeq;
  db.prepare(`
    INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty,
      pk_batch_id, match_status, status, worker, started_at, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, '2026-08-24', ?, ?, ?, ?, 'ok', ?, '検証者', ?, 'valid', ?, 'test', ?, ?)
  `).run(id, `KEY${id}`, `出荷_${id}`, slips, slips, slips, pkBatchId, status, now, `sha${id}`, now, now);
  for (let i = 1; i <= slips; i += 1) {
    db.prepare(`
      INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, mall, delivery_method, print_header1, status)
      VALUES (?, ?, ?, ?, 'テスト店', 'ネコポス 陸便 元払い 営業所止めなし', 'ヤマト(ネコポス)', 'pending')
    `).run(id, i, `NE${id}-${i}`, `SP${id}-${i}`);
  }
  return { id, pkBatchId };
}
function setSlip(batchId, seq, cols) {
  const sets = Object.keys(cols).map((k) => `${k}=?`).join(', ');
  db.prepare(`UPDATE pk_pack_slips SET ${sets} WHERE batch_id=? AND seq=?`).run(...Object.values(cols), batchId, seq);
}
function addLine(batchId, seq, sku, qty, name = '商品') {
  const slipId = db.prepare('SELECT id FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, seq).id;
  db.prepare('INSERT INTO pk_pack_lines (slip_id, sku, qty, product_name) VALUES (?, ?, ?, ?)')
    .run(slipId, sku, qty, name);
}
function slipWithLines(batchId, seq) {
  const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, seq);
  slip.lines = db.prepare(`
    SELECT l.sku, l.qty, l.product_name FROM pk_pack_lines l WHERE l.slip_id = ? ORDER BY l.id
  `).all(slip.id);
  return slip;
}
const judge = (batchId, seq, cls) => judgeSlip(db, { batchId, slip: slipWithLines(batchId, seq), hikiateClass: cls });

// ─── 1. 正規化・combo_key ───
t('normalizeKeyText: NFKC + 空白圧縮', () => {
  assert.equal(normalizeKeyText('　ヤマト（ネコポス） '), 'ヤマト(ネコポス)');
  assert.equal(normalizeKeyText('AES  《単品》'), 'AES 《単品》');
});
t('comboKeyOf: 合算・ソート・単品同形式', () => {
  assert.equal(comboKeyOf([{ sku: 'B-1', qty: 1 }, { sku: 'a-2', qty: 2 }, { sku: 'A-2 ', qty: 1 }]).comboKey,
    'v1|a-2*3|b-1*1');
  assert.equal(comboKeyOf([{ sku: 'oa-jon-6-5', qty: 2 }]).comboKey, 'v1|oa-jon-6-5*2');
});
t('comboKeyOf: 特殊文字はエスケープ・不正明細は null', () => {
  const k = comboKeyOf([{ sku: 'a|b*c%d', qty: 1 }]).comboKey;
  assert.ok(!k.slice(3).replace(/%[0-9A-F]{2}/g, '').includes('|') || k.split('|').length === 2, k);
  assert.equal(k, 'v1|a%7Cb%2Ac%25d*1');
  assert.equal(comboKeyOf([{ sku: '', qty: 1 }]), null);
  assert.equal(comboKeyOf([{ sku: 'a', qty: 0 }]), null);
  assert.equal(comboKeyOf([{ sku: 'a', qty: 1.5 }]), null);
  assert.equal(comboKeyOf([]), null);
});

// ─── 2. 配送種別の導出 ───
t('resolveDelivery: 既知ヘッダ・未知・空', () => {
  assert.equal(resolveDelivery(db, 'ヤマト(ネコポス)', null).code, 'nekopos');
  assert.equal(resolveDelivery(db, '謎のヘッダ', null).code, 'unknown');
  assert.equal(resolveDelivery(db, '', null).code, 'unknown');
  assert.equal(resolveDelivery(db, 'レターパック500', null).headerMaterial, 'letterpack500');
});
t('resolveDelivery: AES は分類マスタで分岐・未知は unknown (fail-closed)', () => {
  assert.equal(resolveDelivery(db, 'AES', 'AES《単品》').code, 'aes_mail');
  assert.equal(resolveDelivery(db, 'AES', 'AES《単品》【メール便以外】').code, 'aes_other');
  assert.equal(resolveDelivery(db, 'AES', 'AES《謎の新分類》').code, 'unknown');       // aes_kind 未設定
  assert.equal(resolveDelivery(db, 'AES', '未登録の分類').code, 'unknown');
  assert.equal(resolveDelivery(db, 'AES', null).code, 'unknown');
});

// ─── 3. 判定順 ───
const CLS = 'ネコポス手動単品';
t('judge: hidden 分類はカード非表示', () => {
  const b = makeBatch('LINEギフト《単品、複数個を含む全て》');
  addLine(b.id, 1, 'sku-a', 1);
  assert.equal(judge(b.id, 1, 'LINEギフト《単品、複数個を含む全て》').source, 'hidden');
});
t('judge: ④依頼中 (requested) は held・全判定より先', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-a', 1);
  setSlip(b.id, 1, { print_header1: 'レターパック500' });   // 伝票指定があっても held が勝つ
  db.prepare(`
    INSERT INTO pk_pack_ship_changes (batch_id, slip_seq, ne_slip_no, proposed_method, reason, requested_by, status, updated_at, created_at)
    VALUES (?, 1, 'NE', '宅急便60サイズ', '入らない', 'テスト', 'requested', ?, ?)
  `).run(b.id, utcNow(), utcNow());
  assert.equal(judge(b.id, 1, CLS).source, 'held');
});
t('judge: 伝票指定 (レターパック500) は header・登録不可', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-a', 1);
  setSlip(b.id, 1, { print_header1: 'レターパック500' });
  const c = judge(b.id, 1, CLS);
  assert.equal(c.source, 'header');
  assert.equal(c.material.code, 'letterpack500');
  assert.equal(c.canRegister, false);
});
t('judge: 未登録 + 分類候補 (active のみ・並び順)', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-a', 1);
  const c = judge(b.id, 1, CLS);
  assert.equal(c.source, 'candidates');
  assert.deepEqual(c.candidates.map((m) => m.code), ['tate_kraft_env', 'vinyl_s', 'nekopos_box', 'old_env']);
  assert.equal(c.canRegister, true);
});
t('judge: 配送種別 unknown は登録不可', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-a', 1);
  setSlip(b.id, 1, { print_header1: '謎のヘッダ' });
  const c = judge(b.id, 1, CLS);
  assert.equal(c.source, 'unknown');
  assert.equal(c.canRegister, false);
});
t('judge: 明細不正は line_invalid (候補は見えるが登録不可)', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-a', 0);
  const c = judge(b.id, 1, CLS);
  assert.equal(c.reason, 'line_invalid');
  assert.equal(c.canRegister, false);
  assert.ok(c.candidates.length > 0);
});

// ─── 4. 登録・変更・CAS ───
const bReg = makeBatch(CLS);
addLine(bReg.id, 1, 'sku-reg', 2);
const ctx0 = judge(bReg.id, 1, CLS);
let ev1;
t('register: 新規登録 → rule 化・undo token 付き', () => {
  ev1 = registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: ctx0.deliveryCode,
    opId: 'op-reg-1', worker: 'テスト',
  });
  assert.equal(ev1.action, 'register');
  assert.equal(ev1.rule.ruleVersion, 1);
  assert.ok(ev1.undoToken);
  const c = judge(bReg.id, 1, CLS);
  assert.equal(c.source, 'rule');
  assert.equal(c.material.code, 'nekopos_box');
});
t('register: 同一構成の別伝票にも同じルールが効く', () => {
  const b2 = makeBatch(CLS);
  addLine(b2.id, 1, 'SKU-REG', 2);   // 大文字でも同一キー
  assert.equal(judge(b2.id, 1, CLS).material.code, 'nekopos_box');
});
t('register: op_id 再送 (同一hash) は同一応答・相違hash は op_conflict', () => {
  const again = registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: ctx0.deliveryCode,
    opId: 'op-reg-1', worker: 'テスト',
  });
  assert.equal(again.eventId, ev1.eventId);
  assert.equal(again.undoToken, ev1.undoToken);
  throwsCode(() => registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: ctx0.deliveryCode,
    opId: 'op-reg-1', worker: 'テスト',
  }), 'op_conflict');
});
t('register: 未登録想定の二重登録は 409 conflict + 現在値', () => {
  const e = throwsCode(() => registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: ctx0.deliveryCode,
    opId: 'op-reg-2', worker: 'テスト',
  }), 'conflict');
  assert.equal(e.body.current.material.code, 'nekopos_box');
});
t('change: expected id+version の CAS・ABA も弾く', () => {
  const c = judge(bReg.id, 1, CLS);
  const ch = registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    expectedBefore: 'nekopos_box', opId: 'op-ch-1', worker: 'テスト',
  });
  assert.equal(ch.action, 'change');
  assert.equal(ch.rule.ruleVersion, 2);
  // 別端末が v2 を見て A に戻す (v3)。古い端末の v1 前提の変更は ABA でも 409
  const c2 = judge(bReg.id, 1, CLS);
  registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: c2.ruleId, expectedVersion: c2.ruleVersion, expectedDeliveryCode: c2.deliveryCode,
    opId: 'op-ch-2', worker: 'テスト2',
  });
  throwsCode(() => registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'tate_kraft_env',
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-ch-stale', worker: 'テスト',
  }), 'conflict');
});
t('change: 同じ資材なら noop (イベント無し)', () => {
  const c = judge(bReg.id, 1, CLS);
  const r = registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: c.material.code,
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-noop', worker: 'テスト',
  });
  assert.equal(r.noop, true);
  assert.equal(db.prepare("SELECT COUNT(*) c FROM pk_pack_material_events WHERE op_id='op-noop'").get().c, 0);
});
t('register: expected_delivery_code 不一致は 409 context_changed', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-ctx', 1);
  throwsCode(() => registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'teikeigai',   // 実際は nekopos
    opId: 'op-ctx', worker: 'テスト',
  }), 'context_changed');
});
t('register: held / hidden / header / unknown は拒否', () => {
  const bh = makeBatch(CLS);
  addLine(bh.id, 1, 'sku-h1', 1);
  db.prepare(`
    INSERT INTO pk_pack_ship_changes (batch_id, slip_seq, ne_slip_no, proposed_method, reason, requested_by, status, updated_at, created_at)
    VALUES (?, 1, 'NE', '宅急便60サイズ', '入らない', 'テスト', 'accepted', ?, ?)
  `).run(bh.id, utcNow(), utcNow());
  throwsCode(() => registerMaterial({
    batchId: bh.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'nekopos',
    opId: 'op-held', worker: 'テスト',
  }), 'ship_change_requested');
  const bg = makeBatch('LINEギフト《単品、複数個を含む全て》');
  addLine(bg.id, 1, 'sku-h2', 1);
  throwsCode(() => registerMaterial({
    batchId: bg.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'nekopos',
    opId: 'op-hidden', worker: 'テスト',
  }), 'hidden_class');
  const bl = makeBatch(CLS);
  addLine(bl.id, 1, 'sku-h3', 1);
  setSlip(bl.id, 1, { print_header1: 'レターパック500' });
  throwsCode(() => registerMaterial({
    batchId: bl.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'letterpack',
    opId: 'op-header', worker: 'テスト',
  }), 'header_fixed');
  const bu = makeBatch(CLS);
  addLine(bu.id, 1, 'sku-h4', 1);
  setSlip(bu.id, 1, { print_header1: '謎のヘッダ' });
  throwsCode(() => registerMaterial({
    batchId: bu.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'unknown',
    opId: 'op-unknown', worker: 'テスト',
  }), 'delivery_unknown');
});

// ─── 5. undo ───
t('undo: register の取り消しで rule は disabled (削除しない)', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-undo1', 1);
  const c = judge(b.id, 1, CLS);
  const r = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-u1', worker: 'テスト',
  });
  const u = undoMaterial({ opId: 'opu-u1', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト' });
  assert.equal(u.undone, true);
  assert.equal(db.prepare('SELECT status FROM pk_pack_material_rules WHERE id=?').get(r.rule.ruleId).status, 'disabled');
  assert.equal(judge(b.id, 1, CLS).source, 'candidates');   // 未登録に戻る
  // 2回目は不可 (UNIQUE target_event_id + undone_at)
  throwsCode(() => undoMaterial({ opId: 'opu-u1b', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト' }), 'already_undone');
  // 取消後の再登録は新しい行 (部分一意 WHERE active)
  const c2 = judge(b.id, 1, CLS);
  const r2 = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c2.deliveryCode,
    opId: 'op-u1c', worker: 'テスト',
  });
  assert.notEqual(r2.rule.ruleId, r.rule.ruleId);
});
t('undo: change の取り消しは before へ復元・pending 通知は cancelled', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-undo2', 1);
  let c = judge(b.id, 1, CLS);
  registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-u2a', worker: 'テスト',
  });
  c = judge(b.id, 1, CLS);
  const ch = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'tate_kraft_env',
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-u2b', worker: 'テスト',
  });
  assert.equal(db.prepare('SELECT notify_status FROM pk_pack_material_events WHERE id=?').get(ch.eventId).notify_status, 'pending');
  const u = undoMaterial({ opId: 'opu-u2', eventId: ch.eventId, undoToken: ch.undoToken, worker: 'テスト' });
  assert.equal(u.undone, true);
  assert.equal(judge(b.id, 1, CLS).material.code, 'vinyl_s');
  assert.equal(db.prepare('SELECT notify_status FROM pk_pack_material_events WHERE id=?').get(ch.eventId).notify_status, 'cancelled');
});
t('undo: その後の変更 (version 不一致) / 期限切れ / token 不正は拒否', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-undo3', 1);
  let c = judge(b.id, 1, CLS);
  const r = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-u3a', worker: 'テスト',
  });
  throwsCode(() => undoMaterial({ opId: 'opu-u3x', eventId: r.eventId, undoToken: 'bad-token', worker: 'テスト' }), 'bad_token');
  c = judge(b.id, 1, CLS);
  registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-u3b', worker: 'テスト2',
  });
  throwsCode(() => undoMaterial({ opId: 'opu-u3y', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト' }), 'undo_conflict');
  // 期限切れ (undo_expires_at を過去に)
  const b2 = makeBatch(CLS);
  addLine(b2.id, 1, 'sku-undo4', 1);
  const c2 = judge(b2.id, 1, CLS);
  const r2 = registerMaterial({
    batchId: b2.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c2.deliveryCode,
    opId: 'op-u4', worker: 'テスト',
  });
  db.prepare("UPDATE pk_pack_material_events SET undo_expires_at='2020-01-01T00:00:00Z' WHERE id=?").run(r2.eventId);
  throwsCode(() => undoMaterial({ opId: 'opu-u4', eventId: r2.eventId, undoToken: r2.undoToken, worker: 'テスト' }), 'undo_expired');
});

// ─── 6. outbox ───
function makeChangeEvent() {
  const b = makeBatch(CLS);
  addLine(b.id, 1, `sku-ob${b.id}`, 1);
  let c = judge(b.id, 1, CLS);
  registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: `op-ob-r${b.id}`, worker: 'テスト',
  });
  c = judge(b.id, 1, CLS);
  const ch = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: c.ruleId, expectedVersion: c.ruleVersion, expectedDeliveryCode: c.deliveryCode,
    opId: `op-ob-c${b.id}`, worker: 'テスト',
  });
  return ch.eventId;
}
const evRow = (id) => db.prepare('SELECT * FROM pk_pack_material_events WHERE id=?').get(id);
function makeDue(id) {
  db.prepare("UPDATE pk_pack_material_events SET notify_due_at='2026-01-01T00:00:00Z', next_attempt_at='2026-01-01T00:00:00Z' WHERE id=?").run(id);
}

await (async () => {
  const id = makeChangeEvent();
  t('outbox: 猶予前は claim されない', async () => {});
  const r0 = await materialNotifyStep(async () => true);
  assert.equal(r0.claimedRows, 0, '猶予中に claim された');
  makeDue(id);
  let sentText = null;
  const r1 = await materialNotifyStep(async (text) => { sentText = text; return true; });
  passed++; console.log('  ok: outbox: due 後に送信 → sent');
  assert.equal(r1.sent, 1);
  assert.equal(evRow(id).notify_status, 'sent');
  assert.ok(sentText.includes(`#${id}`), '本文に event ID');
  assert.ok(sentText.includes('vinyl_s') || sentText.includes('小サイズ白ビニール'), '本文に変更内容');

  // 送信後は undo 不可
  const undoAfterSent = evRow(id);
  throwsCode(() => undoMaterial({
    opId: `opu-sent${id}`, eventId: id, undoToken: undoTokenFor(id), worker: 'テスト',
  }), 'undo_conflict');
  passed++; console.log('  ok: outbox: sent 後の undo は 409');

  // 失敗 → pending + backoff + attempt 加算 → 上限で failed
  const id2 = makeChangeEvent();
  makeDue(id2);
  const rf = await materialNotifyStep(async () => { throw new Error('boom'); });
  assert.equal(rf.sent, 0);
  let row = evRow(id2);
  assert.equal(row.notify_status, 'pending');
  assert.equal(row.attempt_count, 1);
  assert.ok(row.next_attempt_at > utcNow(), 'backoff が未来');
  assert.ok(row.notify_error.includes('boom'));
  passed++; console.log('  ok: outbox: 失敗は pending + backoff + attempt加算');

  db.prepare('UPDATE pk_pack_material_events SET attempt_count=10 WHERE id=?').run(id2);
  makeDue(id2);
  await materialNotifyStep(async () => true);   // sweep が failed へ
  assert.equal(evRow(id2).notify_status, 'failed');
  passed++; console.log('  ok: outbox: 上限到達は sweep で failed (送信しない)');

  // 手動再送 = failed → pending・attempt リセット・新しい 48h 窓
  manualResend(id2, '管理者');
  row = evRow(id2);
  assert.equal(row.notify_status, 'pending');
  assert.equal(row.attempt_count, 0);
  assert.ok(row.resend_requested_at);
  const rr = await materialNotifyStep(async () => true);
  assert.equal(evRow(id2).notify_status, 'sent');
  assert.ok(rr.sent >= 1);
  passed++; console.log('  ok: outbox: 手動再送 → 送信');
  throwsCode(() => manualResend(id2, '管理者'), 'not_failed');

  // 48h 窓超過の sweep (created_at を過去に・resend なし)
  const id3 = makeChangeEvent();
  db.prepare("UPDATE pk_pack_material_events SET created_at='2020-01-01T00:00:00Z', notify_due_at='2020-01-01T00:00:15Z', next_attempt_at='2020-01-01T00:00:15Z' WHERE id=?").run(id3);
  await materialNotifyStep(async () => true);
  assert.equal(evRow(id3).notify_status, 'failed');
  passed++; console.log('  ok: outbox: 48h 超過は failed');

  // stale sending の回収 + 旧 claim_token の書き戻し無視
  const id4 = makeChangeEvent();
  makeDue(id4);
  db.prepare(`
    UPDATE pk_pack_material_events SET notify_status='sending', claim_token='old-token', claimed_at='2020-01-01T00:00:00Z', attempt_count=1
    WHERE id=?`).run(id4);
  await materialNotifyStep(null);   // sendFn 無し = 構成エラーでも回収は走る
  assert.equal(evRow(id4).notify_status, 'pending');
  assert.equal(evRow(id4).claim_token, null);
  const wrote = db.prepare(`
    UPDATE pk_pack_material_events SET notify_status='sent', notified_at=? WHERE id=? AND notify_status='sending' AND claim_token='old-token'
  `).run(utcNow(), id4);
  assert.equal(wrote.changes, 0, '旧 token の書き戻しが通ってしまった');
  passed++; console.log('  ok: outbox: stale 回収 + 旧 claim_token の書き戻し無視');

  // 構成エラー (sendFn null) では claim しない
  makeDue(id4);
  const rc = await materialNotifyStep(null);
  assert.equal(rc.configError, true);
  assert.equal(evRow(id4).notify_status, 'pending');
  passed++; console.log('  ok: outbox: webhook 未設定は claim しない');
})();

// ─── 7. views (表示観測ログ・完了スナップショット) ───
t('views: 表示で upsert・完了で固定・完了取消で解除', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-view', 1);
  const state = {
    batch: { id: b.id },
    slips: [slipWithLines(b.id, 1)],
  };
  const out = materialsForState(state, CLS);
  assert.equal(out[1].source, 'candidates');
  let v = db.prepare('SELECT * FROM pk_pack_material_views WHERE batch_id=? AND slip_seq=1').get(b.id);
  assert.equal(v.source, 'candidates');
  assert.equal(v.delivery_code, 'nekopos');
  onSlipCompleted(db, b.id, 1, utcNow());
  v = db.prepare('SELECT * FROM pk_pack_material_views WHERE batch_id=? AND slip_seq=1').get(b.id);
  assert.equal(v.completed_source, 'candidates');
  // 完了後にルール登録 → 再表示しても completed_* と判定列は固定
  const c = judge(b.id, 1, CLS);
  registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: `op-view${b.id}`, worker: 'テスト',
  });
  materialsForState(state, CLS);
  v = db.prepare('SELECT * FROM pk_pack_material_views WHERE batch_id=? AND slip_seq=1').get(b.id);
  assert.equal(v.source, 'candidates', '完了済み行の判定列が動いた');
  assert.equal(v.completed_source, 'candidates');
  onSlipCompletionCleared(db, b.id, 1);
  materialsForState(state, CLS);
  v = db.prepare('SELECT * FROM pk_pack_material_views WHERE batch_id=? AND slip_seq=1').get(b.id);
  assert.equal(v.source, 'rule', '完了解除後は判定列が更新される');
});

// ─── 8. 有効候補ゼロ・inactive ───
t('inactive 資材: 候補から除外・有効候補ゼロは no_candidates・ルールは⚠表示', () => {
  db.prepare("UPDATE pk_pack_materials SET is_active=0 WHERE code='old_env'").run();
  assert.deepEqual(classCandidates(db, CLS).map((m) => m.code), ['tate_kraft_env', 'vinyl_s', 'nekopos_box']);
  const b = makeBatch('50サイズ宅急便単品');
  addLine(b.id, 1, 'sku-z', 1);
  setSlip(b.id, 1, { print_header1: 'ヤマト宅急便【50サイズ専用】' });
  const c = judge(b.id, 1, '50サイズ宅急便単品');
  assert.equal(c.reason, 'no_candidates');           // 候補設定はあるが有効ゼロ
  assert.equal(c.canRegister, true);
  // inactive 資材は新規選択不可
  throwsCode(() => registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'old_env',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: 'takkyubin50',
    opId: 'op-inact', worker: 'テスト',
  }), 'bad_material');
});

// ─── 9. DDL CHECK・整合性 ───
t('DDL: change は notify none で保存できない / 非 change は pending で保存できない', () => {
  const base = {
    op_id: 'ddl-1', request_hash: 'h', rule_id: 1, rule_version: 1,
    combo_key: 'v1|a*1', delivery_code: 'nekopos', worker: 'w', created_at: utcNow(),
  };
  assert.throws(() => db.prepare(`
    INSERT INTO pk_pack_material_events (op_id, request_hash, action, rule_id, rule_version, combo_key, delivery_code,
      before_code, after_code, worker, created_at, undo_expires_at, notify_status)
    VALUES (@op_id, @request_hash, 'change', @rule_id, @rule_version, @combo_key, @delivery_code,
      'a', 'b', @worker, @created_at, @created_at, 'none')
  `).run(base), /CHECK/);
  assert.throws(() => db.prepare(`
    INSERT INTO pk_pack_material_events (op_id, request_hash, action, rule_id, rule_version, combo_key, delivery_code,
      after_code, worker, created_at, undo_expires_at, notify_status, notify_due_at, next_attempt_at)
    VALUES (@op_id, @request_hash, 'register', @rule_id, @rule_version, @combo_key, @delivery_code,
      'b', @worker, @created_at, @created_at, 'pending', @created_at, @created_at)
  `).run(base), /CHECK/);
});
t('DDL: 資材 code は2文字目以降の不正文字も拒否', () => {
  assert.throws(() => db.prepare(`
    INSERT INTO pk_pack_materials (code, name, created_at, updated_at) VALUES ('a漢字', 'x', ?, ?)
  `).run(utcNow(), utcNow()), /CHECK/);
});
t('op_id: 別 worker の同一 op_id は op_conflict (hash に worker を含む)', () => {
  throwsCode(() => registerMaterial({
    batchId: bReg.id, slipSeq: 1, materialCode: 'nekopos_box',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: ctx0.deliveryCode,
    opId: 'op-reg-1', worker: '別人',
  }), 'op_conflict');
});
t('undo: バッチ不一致の URL からは取り消せない', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-xbatch', 1);
  const c = judge(b.id, 1, CLS);
  const r = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-xb', worker: 'テスト',
  });
  throwsCode(() => undoMaterial({
    opId: 'opu-xb', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト', batchId: b.id + 999,
  }), 'event_not_found');
});
t('undo: 別バッチ URL では同一 op_id のリプレイも迂回できない (hash に batchId)', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-xb2', 1);
  const c = judge(b.id, 1, CLS);
  const r = registerMaterial({
    batchId: b.id, slipSeq: 1, materialCode: 'vinyl_s',
    expectedRuleId: null, expectedVersion: null, expectedDeliveryCode: c.deliveryCode,
    opId: 'op-xb2', worker: 'テスト',
  });
  const u = undoMaterial({ opId: 'opu-xb2', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト', batchId: b.id });
  assert.equal(u.undone, true);
  // 同じ op_id を別バッチの URL で再送 → hash 不一致で op_conflict (成功応答を再現しない)
  throwsCode(() => undoMaterial({
    opId: 'opu-xb2', eventId: r.eventId, undoToken: r.undoToken, worker: 'テスト', batchId: b.id + 999,
  }), 'op_conflict');
});
t('seed: 管理APIと同基準の検証 (分類名の長さ・候補件数)', () => {
  assert.throws(() => seedMaterialsData({ classes: [{ class_value: 'x'.repeat(121) }] }, 'test'), /分類名/);
  assert.throws(() => seedMaterialsData({
    class_materials: [{ class_value: 'ネコポス手動単品', codes: Array.from({ length: 51 }, (_, i) => `c${i}`) }],
  }, 'test'), /50件/);
  assert.throws(() => seedMaterialsData({ header_map: [{ header_value: '', base_delivery_code: 'nekopos' }] }, 'test'), /header/);
});
t('完了スナップショット: view 行が無くても完了時に生成される', () => {
  const b = makeBatch(CLS);
  addLine(b.id, 1, 'sku-noview', 1);
  onSlipCompleted(db, b.id, 1, utcNow());   // 一度も表示していない
  const v = db.prepare('SELECT * FROM pk_pack_material_views WHERE batch_id=? AND slip_seq=1').get(b.id);
  assert.ok(v, 'view 行が作られていない');
  assert.equal(v.completed_source, 'candidates');
});
t('foreign_key_check: 参照整合性エラーなし', () => {
  assert.deepEqual(db.pragma('foreign_key_check'), []);
});
t('daily counts: 本日分の集計', () => {
  const c = materialDailyCounts(new Date().toISOString().slice(0, 10));
  assert.ok(c.registered >= 1 && c.changed >= 1 && c.undone >= 1, JSON.stringify(c));
});

console.log(`\ntest-materials: ${passed} 件 pass`);
