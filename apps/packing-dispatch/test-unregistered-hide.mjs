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
insP.run('TYPO-002 ', '間違えて作った商品2', '単品', '取扱中', 0);   // 大文字・末尾空白
insP.run('ＦＷ－００１', '全角で作ったコード', '単品', '取扱中', 0);   // 全角 (SQLite の lower は全角を変えない)
insP.run('keep-001', '本当に未登録の商品', '単品', '取扱中', 0);
insP.run('ruled-001', 'ルール登録済み', '単品', '取扱中', 0);
insP.run('set-001', 'セット', 'セット', '取扱中', 2);
insP.run('stop-001', '取扱中止', '単品', '取扱中止', 0);
db.prepare(`INSERT INTO pd_shipping_rule (sku_key, product_code, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code)
  VALUES ('ruled-001::::', 'ruled-001', 'rakuten', 1, NULL, 'nekopos', 'manual')`).run();

const codes = () => listUnregistered().map((r) => r.product_code).sort();
const hiddenRows = (q) => listHiddenUnregistered(q).rows;
const hiddenOf = (code) => hiddenRows().find((x) => x.product_code === code);

let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };
const expectErr = (fn, part) => {
  try { fn(); assert.fail('エラーになるはず'); }
  catch (e) { assert.equal(e.code, 'VALIDATION'); assert.ok(String(e.message).includes(part), `${part} を含むはず: ${e.message}`); }
};

t('初期: ルール未登録の取扱中・単品 4 件が出る (登録済み・セット・取扱中止は出ない)', () => {
  assert.deepEqual(codes(), ['TYPO-002 ', 'keep-001', 'typo-001', 'ＦＷ－００１']);
  assert.deepEqual(listHiddenUnregistered(), { total: 0, rows: [] });
});

t('1 件削除: 一覧から消え、削除したものに商品名・人つきで入る', () => {
  const r = hideUnregistered(['typo-001'], 'staff@example.com');
  assert.deepEqual(r, { requested: 1, hidden: 1, already: 0, skipped: 0 });
  assert.deepEqual(codes(), ['TYPO-002 ', 'keep-001', 'ＦＷ－００１']);
  const h = hiddenOf('typo-001');
  assert.equal(h.product_name, '間違えて作った商品');
  assert.equal(h.hidden_by, 'staff@example.com');
  assert.equal(h.still_unregistered, 1);
});

t('まとめて削除: 画面の値そのまま (大文字・空白)・表記ゆれの重複・削除済みを吸収', () => {
  const r = hideUnregistered(['TYPO-002 ', 'typo-002', 'typo-001'], 'other@example.com');
  assert.deepEqual(r, { requested: 2, hidden: 1, already: 1, skipped: 0 });
  assert.deepEqual(codes(), ['keep-001', 'ＦＷ－００１']);
  assert.ok(hiddenOf('typo-002'));
  // 既に削除済みの typo-001 は外した人・日時を上書きしない
  assert.equal(hiddenOf('typo-001').hidden_by, 'staff@example.com');
});

t('全角コードも一覧から消える (Codex R1: NFKC と SQLite の正規化の食い違い)', () => {
  const r = hideUnregistered(['ＦＷ－００１'], 'staff@example.com');
  assert.equal(r.hidden, 1);
  assert.deepEqual(codes(), ['keep-001']);
  const h = hiddenOf('ＦＷ－００１');
  assert.equal(h.product_name, '全角で作ったコード');
  assert.equal(h.still_unregistered, 1);
  assert.ok(productDiag('ＦＷ－００１').hidden_from_unregistered);
});

t('一覧の対象でないコードは外さない (存在しない・ルール登録済み・セット・取扱中止) (Codex R1)', () => {
  const before = listHiddenUnregistered().total;
  const r = hideUnregistered(['nope-999', 'ruled-001', 'set-001', 'stop-001'], 'x');
  assert.deepEqual(r, { requested: 4, hidden: 0, already: 0, skipped: 4 });
  assert.equal(listHiddenUnregistered().total, before);
  // 後で本当に未登録になったら、ちゃんと一覧に出る
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中' WHERE 商品コード='stop-001'`).run();
  assert.ok(codes().includes('stop-001'));
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中止' WHERE 商品コード='stop-001'`).run();
});

t('NE の商品マスタ・配送ルールは消えない (一覧から外すだけ)', () => {
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM mirror_products`).get().n, 7);
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM pd_shipping_rule`).get().n, 1);
});

t('商品診断に「一覧から外した」が出る', () => {
  assert.ok(productDiag('typo-001').hidden_from_unregistered);
  assert.equal(productDiag('keep-001').hidden_from_unregistered, null);
});

t('いまの状態: 取扱中止・セットに変わった・ルール登録済みは「一覧の対象外」(本体と同じ判定) (Codex R1)', () => {
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中止' WHERE 商品コード='typo-001'`).run();
  assert.equal(hiddenOf('typo-001').still_unregistered, 0);
  db.prepare(`UPDATE mirror_products SET 取扱区分='取扱中' WHERE 商品コード='typo-001'`).run();
  db.prepare(`INSERT INTO mirror_set_components (セット商品コード, 構成商品コード, 数量, updated_at) VALUES ('typo-001', 'keep-001', 2, '2026-10-06T00:00:00Z')`).run();
  assert.equal(hiddenOf('typo-001').still_unregistered, 0);
  db.prepare(`DELETE FROM mirror_set_components WHERE セット商品コード='typo-001'`).run();
  assert.equal(hiddenOf('typo-001').still_unregistered, 1);
});

t('NE から消えても削除したものに商品名が残る', () => {
  db.prepare(`DELETE FROM mirror_products WHERE 商品コード='TYPO-002 '`).run();
  const x = hiddenOf('typo-002');
  assert.equal(x.product_name, '間違えて作った商品2');
  assert.equal(x.still_unregistered, 0);
  insP.run('TYPO-002 ', '間違えて作った商品2', '単品', '取扱中', 0);
});

t('削除したものを商品コード・商品名で絞れる (% や _ は文字として扱う)', () => {
  assert.deepEqual(hiddenRows('TYPO').map((x) => x.product_code).sort(), ['typo-001', 'typo-002']);
  assert.deepEqual(hiddenRows('全角').map((x) => x.product_code), ['ＦＷ－００１']);
  assert.equal(listHiddenUnregistered('%').total, 0);
  assert.equal(listHiddenUnregistered('_').total, 0);
});

t('元に戻す: 一覧に戻る・まだ削除していないコードは数えない', () => {
  const r = unhideUnregistered(['typo-001', 'keep-001'], 'staff@example.com');
  assert.deepEqual(r, { requested: 2, restored: 1 });
  assert.deepEqual(codes(), ['keep-001', 'typo-001']);
  assert.deepEqual(hiddenRows().map((x) => x.product_code).sort(), ['typo-002', 'ＦＷ－００１']);
  assert.equal(unhideUnregistered(['ＦＷ－００１'], 'x').restored, 1);
  assert.ok(codes().includes('ＦＷ－００１'));
});

t('監査ログに削除・元に戻すが残る', () => {
  const acts = db.prepare(`SELECT action FROM pd_audit_log WHERE action LIKE 'unregistered_%' ORDER BY id`).all().map((r) => r.action);
  assert.deepEqual(acts, ['unregistered_hide', 'unregistered_hide', 'unregistered_hide', 'unregistered_hide', 'unregistered_unhide', 'unregistered_unhide']);
});

t('500 件を超えても総件数は本当の数・表示は新しい 500 件', () => {
  const ins = db.prepare(`INSERT INTO pd_unregistered_hidden (product_code, product_name, hidden_by, hidden_at) VALUES (?,?,?,?)`);
  db.transaction(() => { for (let i = 0; i < 600; i++) ins.run(`bulk-${String(i).padStart(3, '0')}`, null, 'x', `2026-01-01T00:00:${String(i % 60).padStart(2, '0')}Z`); })();
  const h = listHiddenUnregistered();
  assert.equal(h.total, 601);
  assert.equal(h.rows.length, 500);
  assert.equal(h.rows[0].product_code, 'typo-002');   // 今日外したものが先頭
  assert.equal(listHiddenUnregistered('bulk-59').total, 10);   // bulk-590〜599   // 古くても検索で見つかる
});

t('検証: 配列でない・空・上限超えは 400 (何も変えない)', () => {
  const before = listHiddenUnregistered().total;
  expectErr(() => hideUnregistered('typo-001', 'x'), '配列');
  expectErr(() => hideUnregistered(undefined, 'x'), '配列');
  expectErr(() => hideUnregistered([], 'x'), '選ばれていません');
  expectErr(() => hideUnregistered(['  ', null, 3], 'x'), '選ばれていません');
  expectErr(() => unhideUnregistered([], 'x'), '選ばれていません');
  expectErr(() => hideUnregistered(Array.from({ length: 2001 }, (_, i) => `c${i}`), 'x'), '2000');
  assert.equal(listHiddenUnregistered().total, before);
});

console.log(`\n${passed} passed`);
