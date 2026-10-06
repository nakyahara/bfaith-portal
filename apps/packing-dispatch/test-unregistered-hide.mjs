import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 未登録一覧から外す / 元に戻す (pd_unregistered_hidden) のテスト。
 *   node apps/packing-dispatch/test-unregistered-hide.mjs
 * DATA_DIR を一時ディレクトリに向ける (本番mirrorには触れない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pd-unreg-hide-test-'));

const { initMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();
const { ensureSchema, productDiag } = await import('./db.js');
const { listUnregistered, listHiddenUnregistered, hideUnregistered, unhideUnregistered } = await import('./service.js');

// ─── seed ───
const db = ensureSchema();
const insP = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, セット構成品数, 原価状態, updated_at) VALUES (?,?,?,?,?,'OK','2026-10-06T00:00:00Z')`);
insP.run('typo-001', '間違えて作った商品', '単品', '取扱中', 0);
insP.run('TYPO-002 ', '間違えて作った商品2', '単品', '取扱中', 0);   // 大文字・末尾空白 (正規化して扱う)
insP.run('keep-001', '本当に未登録の商品', '単品', '取扱中', 0);
insP.run('ruled-001', 'ルール登録済み', '単品', '取扱中', 0);
db.prepare(`INSERT INTO pd_shipping_rule (sku_key, product_code, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code)
  VALUES ('ruled-001::::', 'ruled-001', 'rakuten', 1, NULL, 'nekopos', 'manual')`).run();

const codes = () => listUnregistered().map((r) => r.product_code.trim().toLowerCase()).sort();

let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };
const expectErr = (fn, part) => {
  try { fn(); assert.fail('エラーになるはず'); }
  catch (e) { assert.equal(e.code, 'VALIDATION'); assert.ok(String(e.message).includes(part), `${part} を含むはず: ${e.message}`); }
};

t('初期: ルール未登録の 3 件が出る (登録済みは出ない)', () => {
  assert.deepEqual(codes(), ['keep-001', 'typo-001', 'typo-002']);
  assert.equal(listHiddenUnregistered().length, 0);
});

t('1 件削除: 一覧から消え、削除したものに商品名・人つきで入る', () => {
  const r = hideUnregistered(['typo-001'], 'staff@example.com');
  assert.deepEqual(r, { requested: 1, hidden: 1, already: 0 });
  assert.deepEqual(codes(), ['keep-001', 'typo-002']);
  const h = listHiddenUnregistered();
  assert.equal(h.length, 1);
  assert.equal(h[0].product_code, 'typo-001');
  assert.equal(h[0].product_name, '間違えて作った商品');
  assert.equal(h[0].hidden_by, 'staff@example.com');
  assert.equal(h[0].still_unregistered, 1);
});

t('まとめて削除: 画面の表記ゆれ (大文字・空白) と重複・既に削除済みを吸収', () => {
  const r = hideUnregistered(['TYPO-002 ', 'typo-002', 'typo-001'], 'other@example.com');
  assert.deepEqual(r, { requested: 2, hidden: 1, already: 1 });
  assert.deepEqual(codes(), ['keep-001']);
  // 既に削除済みの typo-001 は外した人・日時を上書きしない
  assert.equal(listHiddenUnregistered().find((x) => x.product_code === 'typo-001').hidden_by, 'staff@example.com');
});

t('NE の商品マスタ・配送ルールは消えない (一覧から外すだけ)', () => {
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM mirror_products`).get().n, 4);
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM pd_shipping_rule`).get().n, 1);
});

t('商品診断に「一覧から外した」が出る', () => {
  assert.ok(productDiag('typo-001').hidden_from_unregistered);
  assert.equal(productDiag('keep-001').hidden_from_unregistered, null);
});

t('NE で取扱中止にしたら「一覧の対象外」と分かる', () => {
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中止' WHERE 商品コード='typo-001'`).run();
  assert.equal(listHiddenUnregistered().find((x) => x.product_code === 'typo-001').still_unregistered, 0);
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中' WHERE 商品コード='typo-001'`).run();
});

t('NE から消えても削除したものに商品名が残る', () => {
  db.prepare(`DELETE FROM mirror_products WHERE 商品コード='TYPO-002 '`).run();
  const x = listHiddenUnregistered().find((r) => r.product_code === 'typo-002');
  assert.equal(x.product_name, '間違えて作った商品2');
  assert.equal(x.still_unregistered, 0);
  insP.run('TYPO-002 ', '間違えて作った商品2', '単品', '取扱中', 0);
});

t('元に戻す: 一覧に戻る・まだ削除していないコードは数えない', () => {
  const r = unhideUnregistered(['typo-001', 'keep-001'], 'staff@example.com');
  assert.deepEqual(r, { requested: 2, restored: 1 });
  assert.deepEqual(codes(), ['keep-001', 'typo-001']);
  assert.deepEqual(listHiddenUnregistered().map((x) => x.product_code), ['typo-002']);
});

t('監査ログに削除・元に戻すが残る', () => {
  const acts = db.prepare(`SELECT action FROM pd_audit_log WHERE action LIKE 'unregistered_%' ORDER BY id`).all().map((r) => r.action);
  assert.deepEqual(acts, ['unregistered_hide', 'unregistered_hide', 'unregistered_unhide']);
});

t('検証: 配列でない・空・上限超えは 400 (何も変えない)', () => {
  const before = listHiddenUnregistered().length;
  expectErr(() => hideUnregistered('typo-001', 'x'), '配列');
  expectErr(() => hideUnregistered(undefined, 'x'), '配列');
  expectErr(() => hideUnregistered([], 'x'), '選ばれていません');
  expectErr(() => hideUnregistered(['  ', null], 'x'), '選ばれていません');
  expectErr(() => unhideUnregistered([], 'x'), '選ばれていません');
  expectErr(() => hideUnregistered(Array.from({ length: 2001 }, (_, i) => `c${i}`), 'x'), '2000');
  assert.equal(listHiddenUnregistered().length, before);
});

console.log(`\n${passed} passed`);
