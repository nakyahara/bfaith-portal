import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-parent-expand.mjs — 代表商品コードからバリエーションへ広げる (2026-10-20 現場要望)
 *
 * 楽天RMS のように「商品ページ単位でまとめて」売価を直せるよう、代表商品コードを入れたら
 * ぶら下がる NEコードを全部並べる。ここで守りたいこと:
 *   ・NE の代表商品コード / 楽天の商品管理番号 / コードの形 のどれで当たっても子として拾う
 *   ・`-2` (2個セット) のような枝番や、セット商品は子にしない (色と同じ売価が入ってしまう)
 *   ・ふつうの NEコード入力 (単品で 代表商品コード = 自分) は今までどおり何も広げない
 *   ・広げた後のコード数で上限を見る (記録できても送れない履歴を作らない)
 *
 * 実行: node apps/price-update/test-parent-expand.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { expandParentCodes, buildTargets } from './resolve.js';
import { expandedLimitMessage } from './router.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'price-update-parent-'));

function makeDb(file, { withRep = true, withManage = true } = {}) {
  const db = new Database(path.join(tmpDir, file));
  db.exec(`CREATE TABLE mirror_products (
    商品コード TEXT PRIMARY KEY, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
    標準売価 REAL, 原価 REAL, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL,
    セット構成品数 INTEGER${withRep ? ', 代表商品コード TEXT' : ''})`);
  db.exec(`CREATE TABLE mirror_set_components (
    セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1,
    構成商品名 TEXT, 構成商品原価 REAL, updated_at TEXT NOT NULL, PRIMARY KEY (セット商品コード, 構成商品コード))`);
  db.exec(`CREATE TABLE mirror_rakuten_sku_map (rakuten_code TEXT PRIMARY KEY, ne_code TEXT NOT NULL, source TEXT NOT NULL,
    updated_at TEXT NOT NULL${withManage ? ', manage_number TEXT' : ''})`);
  db.exec(`CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code))`);
  return db;
}

function addProduct(db, code, { kind = '単品', rep = null, setCount = null } = {}) {
  const hasRep = db.prepare('PRAGMA table_info(mirror_products)').all().some((c) => c.name === '代表商品コード');
  const cols = '商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数'
    + (hasRep ? ', 代表商品コード' : '');
  const vals = [code, `商品 ${code}`, kind, '取扱中', 1000, 400, '確定', 100, '501', 'ネコポス', 0.1, setCount];
  if (hasRep) vals.push(rep);
  db.prepare(`INSERT INTO mirror_products (${cols}) VALUES (${vals.map(() => '?').join(',')})`).run(...vals);
}

const db = makeDb('mirror.db');
// 0726-001802: NE の代表商品コードで BK/BE、楽天の商品管理番号で GR (NE の代表は空のまま色を足した形)
addProduct(db, '0726-001802-bk', { rep: '0726-001802' });
addProduct(db, '0726-001802-be', { rep: '0726-001802' });
addProduct(db, '0726-001802-gr', { rep: null });
db.prepare('INSERT INTO mirror_rakuten_sku_map VALUES (?,?,?,?,?)').run('0726-001802-gr', '0726-001802-gr', 'am', 'now', '0726-001802');
db.prepare('INSERT INTO mirror_rakuten_sku_map VALUES (?,?,?,?,?)').run('0726-001802-bk', '0726-001802-bk', 'am', 'now', '0726-001802');
// 枝番が色でない (-2 = 2個セット) / セット商品は子にしない
addProduct(db, '0726-001802-2', { rep: null });
addProduct(db, '0726-001802-set', { kind: 'セット', rep: '0726-001802', setCount: 2 });
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('0726-001802-set', '0726-001802-bk', 1, null, 400, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('0726-001802-set', '0726-001802-be', 1, null, 400, 'now');
// コードの形だけで当たる (NE の代表も楽天の対応表も無い)
addProduct(db, 'xyz-100-wh', { rep: null });
addProduct(db, 'xyz-100-nv', { rep: null });
// ふつうの単品 (代表商品コード = 自分)
addProduct(db, 'solo-1', { rep: 'solo-1' });
// 親そのものも商品として登録されている (単品ページに色を足した形)
addProduct(db, 'kara-9', { rep: 'kara-9' });
addProduct(db, 'kara-9-rd', { rep: 'kara-9' });
// 楽天の対応表にだけある子 (商品マスタに無い)
db.prepare('INSERT INTO mirror_rakuten_sku_map VALUES (?,?,?,?,?)').run('ghost-1-bk', 'ghost-1-bk', 'am', 'now', 'ghost-1');
// LIKE のワイルドカード (_) を文字として扱うか
addProduct(db, 'abc-bk', { rep: null });

console.log('\n── 代表商品コードを広げる ──');
{
  const r = expandParentCodes(db, ['0726-001802']);
  eq(r.codes, ['0726-001802-be', '0726-001802-bk', '0726-001802-gr'], 'NE代表・楽天管理番号の両方から子を拾う (並びはコード順)');
  eq(r.expansions.length, 1, '展開は1件');
  eq(r.expansions[0].sources, ['ne', 'rms', 'code'], '手がかりの出所を返す (色の枝番はコードの形でも当たる)');
  ok(!r.codes.includes('0726-001802-2'), '-2 (英字の無い枝番) は子にしない');
  ok(!r.codes.includes('0726-001802-set'), 'セット商品は子にしない (色と同じ売価が入らないように)');
  eq(r.expansions[0].parentIsProduct, false, '親は商品マスタに無い');
}
{
  const r = expandParentCodes(db, ['XYZ-100']);
  eq(r.codes, ['xyz-100-nv', 'xyz-100-wh'], 'コードの形 (親コード-色) だけでも拾う・大文字小文字は問わない');
  eq(r.expansions[0].sources, ['code'], '出所はコードの形');
}
{
  const r = expandParentCodes(db, ['solo-1', 'abc-001']);
  eq(r.codes, ['solo-1', 'abc-001'], 'ふつうの NEコード・未知のコードは今までどおりそのまま');
  eq(r.expansions, [], '何も広げない');
}
{
  const r = expandParentCodes(db, ['kara-9']);
  eq(r.codes, ['kara-9', 'kara-9-rd'], '親も商品なら親も残す');
  eq(r.expansions[0].parentIsProduct, true, 'parentIsProduct');
}
{
  const r = expandParentCodes(db, ['ghost-1']);
  eq(r.codes, ['ghost-1-bk'], '楽天の対応表にだけある子も並べる (商品マスタに無いことは後で知らせる)');
  const t = buildTargets(db, r.codes);
  eq(t.unknownCodes, ['ghost-1-bk'], '商品マスタに無い子は unknownCodes に出る');
}
{
  const r = expandParentCodes(db, ['ab_']);
  eq(r.codes, ['ab_'], 'LIKE の _ はワイルドカードにしない (abc-bk を拾わない)');
}
{
  const r = expandParentCodes(db, ['solo-1', '0726-001802', '0726-001802-bk']);
  eq(r.codes, ['solo-1', '0726-001802-be', '0726-001802-bk', '0726-001802-gr'], '入力の並び順を保ち、重複は1つにする');
}

console.log('\n── 広げた後の引き当て ──');
{
  const { codes } = expandParentCodes(db, ['0726-001802']);
  const { targets } = buildTargets(db, codes);
  const singles = targets.filter((t) => t.rowKind === 'single').map((t) => t.neCode);
  const sets = targets.filter((t) => t.rowKind === 'set').map((t) => t.neCode);
  eq(singles, ['0726-001802-be', '0726-001802-bk', '0726-001802-gr'], '子はそれぞれ単品の行になる');
  eq(sets, ['0726-001802-set'], 'セットは今までどおり「含むセット」として別に出る (初期未選択)');
  ok(targets.every((t) => t.isSetProduct === (t.neCode === '0726-001802-set')), 'isSetProduct はセットだけ true');
}
{
  // ★セットのコードを直接入れると rowKind='single' になる。「まとめて入れる」から外す印は isSetProduct (Codex R1)
  const { targets } = buildTargets(db, ['0726-001802-bk', '0726-001802-set']);
  const set = targets.find((t) => t.neCode === '0726-001802-set');
  eq([set.rowKind, set.isSetProduct], ['single', true], '直接入れたセット: rowKind は single でも isSetProduct は true');
}

console.log('\n── 列が無い mirror でも落ちない ──');
{
  const db2 = makeDb('old.db', { withRep: false, withManage: false });
  addProduct(db2, 'old-1-bk');
  addProduct(db2, 'old-1-wh');
  const r = expandParentCodes(db2, ['old-1']);
  eq(r.codes, ['old-1-bk', 'old-1-wh'], '代表商品コード・manage_number 列が無ければコードの形だけで広げる');
  db2.close();
}

console.log('\n── 上限は広げた後の数で見る ──');
{
  const limits = { maxNeCodes: 3 };
  const x = [{ parent: 'p-1', children: ['p-1-a', 'p-1-b', 'p-1-c', 'p-1-d'], parentIsProduct: false }];
  const msg = expandedLimitMessage(['p-1-a', 'p-1-b', 'p-1-c', 'p-1-d'], x, limits);
  ok(msg && msg.includes('4 コード') && msg.includes('p-1 → 4 品') && msg.includes('分けて'), `上限超えは理由つきで止める: ${msg}`);
  eq(expandedLimitMessage(['a', 'b', 'c'], [], limits), null, '上限ちょうどは通す');
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
