/**
 * warehouse — /service-api/ne-products/names の解決ロジック (resolveSingleNames) のテスト。
 *   node apps/warehouse/tests/test-ne-products-names.mjs
 *
 * 🚨検証の核心 (中原さん絶対ルール 2026-08-23): **セット商品の名前は決して返さない。必ず単品名。**
 *   - 単品コード → raw_ne_products の商品名
 *   - NEセット商品コード (raw_ne_set_products の親) → 構成単品へ展開。セット自身が raw_ne_products に
 *     「セット向けの名前」で載っていても、それは使わない
 *   - 展開に少しでも失敗したセット (構成名欠落・循環・深さ超過・不正構成行・不正数量) は
 *     部分結果を返さず unresolved に入れる (呼び出し側はCSV名へ戻さず「未解決」表示)
 *   - 入れ子セットは3回まで展開。同じ単品が複数経路で出たら数量合算
 *   - 大文字小文字の違い (COLLATE NOCASE)・親コードの前後空白・空白名の除外・1000件上限
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'wh-ne-names-test-'));   // db.js の import 副作用を隔離
const { resolveSingleNames } = await import('../ne-products-service.js');

const db = new Database(':memory:');
db.exec(`
  CREATE TABLE raw_ne_products (商品コード TEXT PRIMARY KEY, 商品名 TEXT, synced_at TEXT);
  CREATE TABLE raw_ne_set_products (
    セット商品コード TEXT, セット商品名 TEXT, 商品コード TEXT, 数量 INTEGER, synced_at TEXT,
    PRIMARY KEY (セット商品コード, 商品コード)
  );
`);
const insP = db.prepare('INSERT INTO raw_ne_products (商品コード, 商品名) VALUES (?, ?)');
const insS = db.prepare('INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, 商品コード, 数量) VALUES (?, ?, ?, ?)');
const SAGE = 'ホワイトセージ 浄化用 スプレー【50ml】_白プチ';
const RINREN = '凜恋 リンレン【ユズ&ネロリ トリートメント 詰め替え】400ml_厚紙封';
insP.run('sage-50', SAGE);
insP.run('rinren-400', RINREN);
insP.run('blank-name', '   ');
// セット: 自身も raw_ne_products に「セット向けの名前」で載っている (→ 絶対に使わない)
insP.run('sage-50-2', 'ホワイトセージ 50ml 2個セット【送料無料】');
insS.run('sage-50-2', 'ホワイトセージ 50ml 2個セット【送料無料】', 'sage-50', 2);
// 異種アソートセット
insP.run('gift-st', 'ギフトセット 凜恋+セージ');
insS.run('gift-st', 'ギフトセット 凜恋+セージ', 'rinren-400', 1);
insS.run('gift-st', 'ギフトセット 凜恋+セージ', 'sage-50', 1);
// 入れ子: セット of セット (2回展開)
insS.run('gift-st-x2', 'ギフト2組', 'gift-st', 2);
// 深さ境界: 3回展開で単品に到達 (OK) / 4回必要 (NG)
insS.run('d3', 'd3', 'd2', 1); insS.run('d2', 'd2', 'd1', 1); insS.run('d1', 'd1', 'sage-50', 1);
insS.run('d4', 'd4', 'd3', 1);
// 構成単品の名前が一部欠けるセット → 部分結果を返さない
insP.run('partial-set', 'パーシャルセット');
insS.run('partial-set', 'パーシャルセット', 'sage-50', 1);
insS.run('partial-set', 'パーシャルセット', 'no-such-sku', 1);
// 構成行が不正 (子コード空) しか無いセット。自身は raw_ne_products に載っている → 単品扱いしない
insP.run('broken-set', 'ブロークンセット【名前を返してはいけない】');
insS.run('broken-set', 'ブロークンセット', '', 1);
// 不正な数量 (0 / 負 / 小数 / NULL)
insP.run('qty0-set', 'QTY0セット'); insS.run('qty0-set', 'QTY0セット', 'sage-50', 0);
insP.run('qtyneg-set', 'QTYNEGセット'); insS.run('qtyneg-set', 'QTYNEGセット', 'sage-50', -1);
insP.run('qtydec-set', 'QTYDECセット'); insS.run('qtydec-set', 'QTYDECセット', 'sage-50', 1.5);
insP.run('qtynull-set', 'QTYNULLセット'); insS.run('qtynull-set', 'QTYNULLセット', 'sage-50', null);
// 循環: cyc-a → cyc-b → cyc-a (+正常枝)
insP.run('cyc-a', 'サイクルA【名前を返してはいけない】');
insS.run('cyc-a', 'サイクルA', 'cyc-b', 1);
insS.run('cyc-a', 'サイクルA', 'sage-50', 1);
insS.run('cyc-b', 'サイクルB', 'cyc-a', 1);
// 自己参照
insP.run('self-set', 'セルフ【名前を返してはいけない】');
insS.run('self-set', 'セルフ', 'self-set', 1);
// 親コードに前後空白 (DB側の汚れ)
insP.run('spaced-set', 'スペースセット【名前を返してはいけない】');
insS.run(' spaced-set ', 'スペースセット', 'sage-50', 3);
// 数量の桁あふれ: big-1 は最大安全整数で解決可、big-2 (×2) は入れ子の掛け算であふれる
insS.run('big-1', 'big1', 'sage-50', Number.MAX_SAFE_INTEGER);
insS.run('big-2', 'big2', 'big-1', 2);
// 文字列の数量 (TEXT として保存された汚れ) は不正
insS.run('qtystr-set', 'QTYSTRセット', 'sage-50', 'x1');

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

t('単品コードはマスタの商品名 (構成は付かない・unresolvedでもない)', () => {
  const r = resolveSingleNames(db, ['sage-50']);
  assert.equal(r.names['sage-50'], SAGE);
  assert.equal(r.components['sage-50'], undefined);
  assert.deepEqual(r.unresolved, []);
});

t('🚨セット商品は構成単品へ展開し、セット自身の名前は絶対に返さない', () => {
  const r = resolveSingleNames(db, ['sage-50-2']);
  assert.equal(r.names['sage-50-2'], SAGE);
  assert.ok(!r.names['sage-50-2'].includes('2個セット'));
  assert.deepEqual(r.components['sage-50-2'], [{ sku: 'sage-50', name: SAGE, qty: 2 }]);
});

t('異種アソートセットは単品名を " / " で連結', () => {
  const r = resolveSingleNames(db, ['gift-st']);
  assert.equal(r.names['gift-st'], `${RINREN} / ${SAGE}`);
  assert.equal(r.components['gift-st'].length, 2);
  assert.ok(!r.names['gift-st'].includes('ギフトセット'));
});

t('入れ子セットは展開して数量を掛け算', () => {
  const r = resolveSingleNames(db, ['gift-st-x2']);
  const byCode = Object.fromEntries(r.components['gift-st-x2'].map((c) => [c.sku, c.qty]));
  assert.deepEqual(byCode, { 'rinren-400': 2, 'sage-50': 2 });
  assert.ok(!r.names['gift-st-x2'].includes('ギフト'));
});

t('深さ境界: 3回展開で単品に到達すれば解決、4回必要なら unresolved', () => {
  const r = resolveSingleNames(db, ['d3', 'd4']);
  assert.equal(r.names['d3'], SAGE);
  assert.equal(r.names['d4'], undefined);
  assert.deepEqual(r.unresolved, ['d4']);
});

t('🚨構成単品の名前が一部でも欠けるセットは部分結果を返さず unresolved', () => {
  const r = resolveSingleNames(db, ['partial-set']);
  assert.equal(r.names['partial-set'], undefined);
  assert.deepEqual(r.unresolved, ['partial-set']);
});

t('🚨構成行が不正 (子コード空) しか無いセットも、自身のマスタ名へ落ちずに unresolved', () => {
  const r = resolveSingleNames(db, ['broken-set']);
  assert.equal(r.names['broken-set'], undefined);
  assert.deepEqual(r.unresolved, ['broken-set']);
});

t('不正な数量 (0 / 負 / 小数 / NULL / 文字列) のセットは unresolved', () => {
  const r = resolveSingleNames(db, ['qty0-set', 'qtyneg-set', 'qtydec-set', 'qtynull-set', 'qtystr-set']);
  assert.deepEqual(r.names, {});
  assert.deepEqual(r.unresolved.sort(), ['qty0-set', 'qtydec-set', 'qtyneg-set', 'qtynull-set', 'qtystr-set']);
});

t('数量の桁あふれ (入れ子の掛け算が安全整数を超える) は unresolved', () => {
  const r = resolveSingleNames(db, ['big-1', 'big-2']);
  assert.equal(r.components['big-1'][0].qty, Number.MAX_SAFE_INTEGER);
  assert.equal(r.names['big-2'], undefined);
  assert.deepEqual(r.unresolved, ['big-2']);
});

t('🚨循環 (A→B→A・正常枝つき) と自己参照は unresolved (部分結果なし・無限ループなし)', () => {
  const r = resolveSingleNames(db, ['cyc-a', 'cyc-b', 'self-set']);
  assert.deepEqual(r.names, {});
  assert.deepEqual(r.unresolved.sort(), ['cyc-a', 'cyc-b', 'self-set']);
});

t('DB側の親コードに前後空白があってもセットとして扱う (自身の名前へ落ちない)', () => {
  const r = resolveSingleNames(db, ['spaced-set']);
  assert.equal(r.names['spaced-set'], SAGE);
  assert.deepEqual(r.components['spaced-set'], [{ sku: 'sage-50', name: SAGE, qty: 3 }]);
});

t('大文字小文字の違いは無視し、応答キーは呼び出し側の表記 (同一コードの表記違いは先勝ち)', () => {
  const r = resolveSingleNames(db, ['SAGE-50', 'Sage-50-2', 'sage-50']);
  assert.equal(r.names['SAGE-50'], SAGE);
  assert.equal(r.names['Sage-50-2'], SAGE);
  assert.equal(r.names['sage-50'], undefined);   // 'SAGE-50' と同一視 (先勝ち)
});

t('空白だけの商品名・未登録の単品・空入力は返さない (unresolvedでもない → CSVの単品名へ)', () => {
  const r = resolveSingleNames(db, ['blank-name', 'unknown', '', null]);
  assert.deepEqual(r.names, {});
  assert.deepEqual(r.unresolved, []);
});

t('1000件上限 (1001件目は黙って無視 — 呼び出し側が1000件ずつ送る契約)', () => {
  const many = Array.from({ length: 1001 }, (_, i) => `x-${i}`);
  many[1000] = 'sage-50';
  const r = resolveSingleNames(db, many);
  assert.equal(r.names['sage-50'], undefined);
  const r2 = resolveSingleNames(db, [...many.slice(0, 999), 'sage-50']);
  assert.equal(r2.names['sage-50'], SAGE);
});

console.log(`test-ne-products-names: ${passed} 件 pass`);
