/**
 * 配送ルール変更の承認フロー (rule-change.js) のテスト。
 *   node apps/packing-dispatch/test-rule-change.mjs
 * DATA_DIR を一時ディレクトリに向ける (本番mirrorには触れない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pd-rule-test-'));

const { initMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();
const { ensureSchema } = await import('./db.js');
const {
  createRuleChangeRequest, applyRuleChangeDecision, getRuleChangeOptions, decisionReplyText,
} = await import('./rule-change.js');

// ─── seed ───
const db = ensureSchema();
db.prepare(`INSERT OR IGNORE INTO pd_shipping_method (code, name_csv, carrier_id, ne_carrier_id, rank) VALUES
  ('nekopos', 'ネコポス', 'yamato', '1', 1)`).run();
db.prepare(`INSERT OR IGNORE INTO pd_shipping_method (code, name_csv, carrier_id, ne_carrier_id, rank) VALUES
  ('takyu60', '宅急便60サイズ', 'yamato', '1', 2)`).run();
db.prepare(`INSERT OR IGNORE INTO pd_shipping_method (code, name_csv, carrier_id, ne_carrier_id, rank) VALUES
  ('letterpack', 'レターパック', 'jp', '2', 3)`).run();
db.prepare(`INSERT OR IGNORE INTO pd_packing_machine (code, name_csv, sort_order) VALUES ('manual', '手動', 1)`).run();
db.prepare(`INSERT OR IGNORE INTO pd_packing_machine (code, name_csv, sort_order) VALUES ('pasline', 'PASLINE', 2)`).run();
// 既存の単品ルール帯 (hinokimat5l / rakuten / 1〜∞ = ネコポス手動)
db.prepare(`INSERT INTO pd_shipping_rule
  (sku_key, product_code, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code)
  VALUES ('hinokimat5l::::', 'hinokimat5l', 'rakuten', 1, NULL, 'nekopos', 'manual')`).run();

let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };
const expectErr = (fn, part) => {
  try { fn(); assert.fail('エラーになるはず'); }
  catch (e) { assert.ok(String(e.message).includes(part), `${part} を含むはず: ${e.message}`); }
};

t('options: モール・配送方法 (aes除く)・梱包機が返る', () => {
  const o = getRuleChangeOptions();
  assert.ok(o.mallGroups.includes('rakuten'));
  assert.ok(o.methods.some((m) => m.code === 'nekopos'));
  assert.ok(!o.methods.some((m) => m.code === 'aes'));
  assert.ok(o.machines.some((m) => m.code === 'manual'));
});

t('検証: 未知コード・梱包機×非ネコポス・不正モールは拒否', () => {
  const base = { kind: 'single', items: [{ sku: 'hinokimat5l', qty: 1 }], mallGroup: 'rakuten', qtyMin: 1, qtyMax: null, requestedBy: 't' };
  expectErr(() => createRuleChangeRequest({ ...base, shippingMethodCode: 'nope', packingMachineCode: 'manual' }), '未知の配送方法');
  expectErr(() => createRuleChangeRequest({ ...base, shippingMethodCode: 'takyu60', packingMachineCode: 'pasline' }), 'ネコポス');
  expectErr(() => createRuleChangeRequest({ ...base, mallGroup: 'qoo10', shippingMethodCode: 'takyu60', packingMachineCode: 'manual' }), 'モール');
});

t('単品: 申請→承認で既存帯の配送方法/梱包機が更新される', () => {
  const row = createRuleChangeRequest({
    kind: 'single', items: [{ sku: 'hinokimat5l', name: 'ひのきマット5L', qty: 1 }],
    mallGroup: 'rakuten', qtyMin: 1, qtyMax: null,
    shippingMethodCode: 'takyu60', packingMachineCode: 'manual',
    requestedBy: '星', context: '出荷_14 1527494',
  });
  assert.equal(row.status, 'requested');
  const done = applyRuleChangeDecision(row.id, 'approve', 'd.nakahara@b-faith.biz');
  assert.equal(done.status, 'approved');
  const rule = db.prepare(`SELECT * FROM pd_shipping_rule WHERE sku_key='hinokimat5l::::' AND mall_group='rakuten'`).get();
  assert.equal(rule.shipping_method_code, 'takyu60');
  assert.ok(decisionReplyText(done).includes('承認'));
  // 二重承認は already
  assert.equal(applyRuleChangeDecision(row.id, 'approve', 'x@b-faith.biz').already, true);
});

t('単品: 該当帯が無ければ failed (帯構成は壊さない)', () => {
  const row = createRuleChangeRequest({
    kind: 'single', items: [{ sku: 'hinokimat5l', qty: 3 }],
    mallGroup: 'rakuten', qtyMin: 3, qtyMax: 5,   // 帯 3〜5 は存在しない (1〜∞のみ)
    shippingMethodCode: 'takyu60', packingMachineCode: 'manual', requestedBy: '星',
  });
  const done = applyRuleChangeDecision(row.id, 'approve', 'a@b-faith.biz');
  assert.equal(done.status, 'failed');
  assert.ok(done.decide_note.includes('数量帯'));
});

t('アソート: 承認で saveAssortDecision (combo登録) される', () => {
  const row = createRuleChangeRequest({
    kind: 'assort',
    items: [{ sku: 'aaa', name: 'A', qty: 1 }, { sku: 'bbb', name: 'B', qty: 2 }],
    shippingMethodCode: 'takyu60', packingMachineCode: 'manual', requestedBy: '星',
  });
  const done = applyRuleChangeDecision(row.id, 'approve', 'a@b-faith.biz');
  assert.equal(done.status, 'approved');
  const dec = db.prepare('SELECT * FROM pd_assort_decision WHERE is_active=1 ORDER BY id DESC LIMIT 1').get();
  assert.equal(dec.shipping_method_code, 'takyu60');
  const items = db.prepare('SELECT * FROM pd_assort_decision_item WHERE decision_id=? ORDER BY sku_key').all(dec.id);
  assert.equal(items.length, 2);
});

t('アソート: レターパックは学習不可 → failed', () => {
  const row = createRuleChangeRequest({
    kind: 'assort', items: [{ sku: 'ccc', qty: 1 }, { sku: 'ddd', qty: 1 }],
    shippingMethodCode: 'letterpack', packingMachineCode: 'manual', requestedBy: '星',
  });
  const done = applyRuleChangeDecision(row.id, 'approve', 'a@b-faith.biz');
  assert.equal(done.status, 'failed');
  assert.ok(done.decide_note.includes('レターパック'));
});

t('却下: DBは変更されない', () => {
  const row = createRuleChangeRequest({
    kind: 'single', items: [{ sku: 'hinokimat5l', qty: 1 }],
    mallGroup: 'rakuten', qtyMin: 1, qtyMax: null,
    shippingMethodCode: 'nekopos', packingMachineCode: 'pasline', requestedBy: '星',
  });
  const done = applyRuleChangeDecision(row.id, 'reject', 'a@b-faith.biz');
  assert.equal(done.status, 'rejected');
  const rule = db.prepare(`SELECT * FROM pd_shipping_rule WHERE sku_key='hinokimat5l::::'`).get();
  assert.equal(rule.shipping_method_code, 'takyu60');   // 前のテストの値のまま
});

console.log(`test-rule-change: ${passed} 件 pass`);
