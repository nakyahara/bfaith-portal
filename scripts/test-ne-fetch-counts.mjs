/**
 * test-ne-fetch-counts.mjs — NE の商品 / セット商品の取得の件数 (広げる道 PR-9。設計 v14 §3.6.3・§3.7・§5.2・R-g・§13 #20 の SQLite の側)
 *
 * 本物の fetchProducts / fetchSetProducts を、NE の API の mock (globalThis.fetch。test-ne-src.mjs と同じ形) に通して流す (raw 表へ直接 INSERT しない)。
 * 固定する契約:
 *   1 取得は設計 v13 §3.6.3 の鍵 (fetched_rows・write_attempts・stored_rows (DB で数えた今回の時刻の行)・dropped_no_code・dropped_missing_fields) と
 *     補助 (dropped_missing_detail・ページの数・各ページの行数・最後のページの行数) を、完了の印と **同じ取引で** sync_meta の ne_api_<kind>_fetch_counts に残す
 *   2 式 fetched = write_attempts + dropped_no_code + dropped_missing_fields か、重なり (write_attempts − stored_rows = 取得のコードが数えた重なり) が
 *     崩れたら完了の印を書かない (fail-closed)。商品 = 印も件数も無し / セット = 入れ替えは今までどおり・前の印も消す
 *   3 途中のページで失敗した回は、印も件数も書かない (商品 = 前の回の分も消えたまま / セット = 前の回の印と件数がそのまま = 前の回の集合のまま)
 *   4 最後のページが短い (R-g) は記録に残る (この PR では見抜かない = 照合の only_in_cdb の増えで見る)
 *   5 readNeFetchCounts は呼び手の読み取りの取引の中で読む = 別の接続が次の取得を書いても、印と件数が同じ回のまま
 *   6 読む時の不合格: 印が無い / 記録が無い / 壊れた JSON / 関係の崩れ / 別の回の記録。印を消す (CSV の取込など) と件数も消える
 *   7 今までの証跡 (complete_count・integrity) の形と値は変わらない
 *   8 取得中の印 (R12): 取得の始め (最初の API の呼び出しの前) に commit・完了の印と同じ取引で消す・途中で失敗した / 印を付けなかった回は残る (読む関数は fetch_in_progress)
 *   9 取得の版 (v14 §3.7・R14): ne-api.js → ne-fetch-counts.js → db.js の「パス + NUL + 中身 (CRLF → LF) + NUL」をつなげた sha256 を、
 *     取得の始めに 1 回だけ計算して件数の記録に入れる (途中でファイルが変わっても始めの値)。読む関数は両方の取得の版が同じときだけ返す
 *  10 落とした行は 1 行 1 分類 (商品 = コードが空 / セット = 親が空 → 親あり子が空)。数が読めないセットの行は落とさず 1 で書き、数だけ notes に残す
 *  11 空白だけのコードは今までどおり落とさずに書く (設計の表の「空白だけ」とは違う = 今の動きを変えない)
 * 使い方: node scripts/test-ne-fetch-counts.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ne-fetch-counts-'));
process.env.DATA_DIR = tmp;
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const warnings = [];
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = (...a) => warnings.push(a.join(' ')); try { return await fn(); } finally { console.log = l; console.warn = w; } };

// NE の API の mock (test-ne-src.mjs と同じ形)。pages を与えると、そのページの行をそのまま返す (短いページ・途中の失敗を作る)
const ne = { goods: [], setgoods: [], goodsPages: null, setPages: null, failAt: null, calls: [], onCall: null };
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
  const q = new URLSearchParams(opts.body);
  const offset = Number(q.get('offset')), limit = Number(q.get('limit'));
  const goods = u.endsWith('/api_v1_master_goods/search');
  ne.calls.push({ goods, offset, limit });
  if (ne.onCall) ne.onCall({ goods, offset });   // API と通信している最中 (SQLite の取引の外) の様子を見る
  if (ne.failAt && ne.failAt.goods === goods && ne.failAt.offset === offset) return { ok: true, status: 200, json: async () => ({ result: 'error', message: 'テストの途中の失敗' }) };
  const pages = goods ? ne.goodsPages : ne.setPages;
  const data = pages ? (pages[offset / limit] || []) : (goods ? ne.goods : ne.setgoods).slice(offset, offset + limit);
  return { ok: true, status: 200, json: async () => ({ result: 'success', data }) };
};
const { fetchProducts, fetchSetProducts } = await quietly(() => import('../apps/warehouse/ne-api.js'));
const { getDB, clearNeCompleteMarks } = await import('../apps/warehouse/db.js');
const { readNeFetchCounts, evalNeFetchCounts, checkNeFetchCounts, beginNeFetch, endNeFetch, NE_FETCH_COUNTS_KEY, NE_FETCH_IN_PROGRESS_KEY, NE_FETCH_SEAL_KEYS, NE_FETCH_MISSING_FIELDS, NE_FETCH_FINGERPRINT_FILES, computeFetchFingerprint } = await import('../apps/warehouse/ne-fetch-counts.js');
const db = () => getDB();
const meta = (k) => db().prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value ?? null;
const counts = (kind) => { const v = meta(NE_FETCH_COUNTS_KEY[kind]); return v == null ? null : JSON.parse(v); };
const setMeta = (k, v) => db().prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, 'x');
const nextSecond = () => new Promise((r) => setTimeout(r, 1100));   // 取得の時刻 (秒) を前の回と分ける
/** 照合と同じ: 読み取り専用の接続で BEGIN し、その中で読む */
const readInTx = (fn) => {
  const r = new Database(path.join(tmp, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try { r.exec('BEGIN'); try { return fn(r); } finally { r.exec('COMMIT'); } } finally { r.close(); }
};
const g = (id, extra = {}) => ({ goods_id: id, goods_name: `n-${id}`, ...extra });
const sg = (p, c, extra = {}) => ({ set_goods_id: p, set_goods_name: `s-${p}`, set_goods_selling_price: '100', set_goods_detail_goods_id: c, set_goods_detail_quantity: '1', ...extra });

await ta('[1] 商品: 空のコード・同じコードが 2 度 (ページの重なり)・小文字にして衝突・2 ページ → 件数と関係・ページを完了の印と一緒に残す', async () => {
  ne.goodsPages = null;
  ne.goods = [
    ...Array.from({ length: 998 }, (_, i) => g(`G${i}`)), g(''), g('ABC-1'),                 // 1 ページ目 1000 行 (空 1)
    g('G0'), g('G1'), g('G2'), g('abc-1'), { goods_name: 'goods_id の欄が無い' }, {}, g(null, { goods_name: null }),   // 欄が無い・全部の欄が無い・null = どれもコードが空 (1 行 1 分類)
    ...Array.from({ length: 243 }, (_, i) => g(`H${i}`)),   // 2 ページ目 250 行
  ];
  await quietly(fetchProducts);
  const c = counts('products');
  // 重なり = write_attempts − stored_rows = 4 (G0〜G2 が 2 度 = 3・ABC-1 と abc-1 が小文字にして衝突 = 1)
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.dropped_no_code, c.dropped_missing_fields, c.dropped_missing_detail],
    [1250, 1246, 1242, 4, 0, {}]);
  assert.deepEqual([c.page_limit, c.pages, c.page_rows, c.last_page_rows], [1000, 2, [1000, 250], 250]);
  // 印と同じ回 (時刻・通し番号)・DB の行の数 = stored_rows
  assert.equal(c.complete_at, meta('ne_api_products_complete_at'));
  assert.equal(String(c.complete_rev), meta('ne_api_products_complete_rev'));
  assert.equal(String(c.stored_rows), meta('ne_api_products_complete_count'));
  assert.equal(db().prepare('SELECT COUNT(*) AS n FROM raw_ne_products WHERE synced_at = ?').get(c.complete_at).n, 1242);
  assert.deepEqual(checkNeFetchCounts('products', c), []);
  // 取得の版 = 固定の順の各ファイルの「パス + NUL + LF にそろえた中身 + NUL」をつなげた sha256 (この試験の中で別に計算する)
  const crypto = await import('node:crypto');
  const expectFp = crypto.createHash('sha256').update(NE_FETCH_FINGERPRINT_FILES.map((f) => f + String.fromCharCode(0) + fs.readFileSync(path.join(repoRoot, f), 'utf8').split(String.fromCharCode(13, 10)).join(String.fromCharCode(10)) + String.fromCharCode(0)).join(''), 'utf8').digest('hex');
  assert.deepEqual([...NE_FETCH_FINGERPRINT_FILES], ['apps/warehouse/ne-api.js', 'apps/warehouse/ne-fetch-counts.js', 'apps/warehouse/db.js']);
  assert.equal(c.fetch_fingerprint, expectFp);
  assert.equal(computeFetchFingerprint(), expectFp);
  // 今までの証跡は形も値も今までどおり (written_rows = 書いた回数・dup_codes = 小文字のコード)
  const it = JSON.parse(meta('ne_api_products_integrity'));
  assert.deepEqual([it.fetched_rows, it.written_rows, it.dropped_no_code, it.distinct_codes, it.dup_code_count, it.dup_codes], [1250, 1246, 4, 1242, 4, ['g0', 'g1', 'g2', 'abc-1']]);
});

await ta('[2] 商品: ちょうど 1000 行 = 2 回目の呼び出しが 0 行で止まる (最後のページ 0) / 0 件の取得も記録する', async () => {
  await nextSecond();
  ne.goods = Array.from({ length: 1000 }, (_, i) => g(`K${i}`));
  await quietly(fetchProducts);
  let c = counts('products');
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.pages, c.page_rows, c.last_page_rows], [1000, 1000, 1000, 2, [1000, 0], 0]);
  await nextSecond();
  ne.goods = [];
  await quietly(fetchProducts);
  c = counts('products');
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.pages, c.page_rows, c.last_page_rows], [0, 0, 0, 1, [0], 0]);
  assert.ok(meta('ne_api_products_complete_at'));
});

await ta('[3] セット: 親が空・子が空・両方空・同じ親 × 子が 2 度・小文字にして衝突 → 落とした理由ごと・重なり・ページ (今までの証跡も同じ)', async () => {
  ne.setPages = null;
  ne.setgoods = [
    sg('S1', 'G1'), sg('S1', 'G2'), sg('S2', 'G3'),
    sg('', 'G4'),                        // 親が空
    sg('', ''),                          // 両方空 = 親が空に数える
    { set_goods_name: '親の欄が無い', set_goods_detail_goods_id: 'G5' },   // 欄の欠け = 親が空
    sg('S3', ''),                        // 子が空
    { set_goods_id: 'S4', set_goods_name: '子の欄が無い' },               // 欄の欠け = 子が空
    sg('S2', 'G3', { set_goods_detail_quantity: '2' }),                   // 同じ親 × 子 (同じ書き方)
    sg('SET-A', 'Up-1'), sg('set-a', 'up-1'),                             // 小文字にして衝突
    {},                                                                   // 全部の欄が無い = 親が空 (1 行 1 分類・要る欄の欠けには数えない)
    { set_goods_id: null, set_goods_detail_goods_id: null, set_goods_detail_quantity: null },   // 親も子も null = 親が空
    { set_goods_id: 'S5' },                                               // 親だけ (子・名前・売価・数量が無い) = 要る欄の欠け 1 つ
  ];
  await quietly(fetchSetProducts);
  const c = counts('setproducts');
  // コードが空 = 親が空 5 (子も空・全部の欄が無い行を含む) / 要る欄の欠け = 親はあるが子が空 3 / 重なり = 6 − 4 = 2 (S2 × G3 が 2 度・SET-A × Up-1 と set-a × up-1)
  //   1 行は 1 つの分類 = 5 + 3 + 6 = 14 = fetched (親子両方の欠けを二重に数えない)
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.dropped_no_code, c.dropped_missing_fields, c.dropped_missing_detail],
    [14, 6, 4, 5, 3, { set_goods_detail_goods_id: 3 }]);
  assert.deepEqual([c.pages, c.page_rows, c.last_page_rows], [1, [14], 14]);
  assert.equal(c.complete_at, meta('ne_api_setproducts_complete_at'));
  assert.equal(String(c.complete_rev), meta('ne_api_setproducts_complete_rev'));
  assert.equal(String(c.stored_rows), meta('ne_api_setproducts_complete_count'));
  const it = JSON.parse(meta('ne_api_setproducts_integrity'));
  assert.deepEqual([it.fetched_rows, it.valid_rows, it.dropped_missing_key, it.dropped_missing_parent, it.missing_child_parents, it.pair_dup_count],
    [14, 6, 8, 5, ['s3', 's4', 's5'], 2]);
  // 照合の封に渡す形 (設計 v13 §8-4b の products_fetch / set_fetch): 契約の 5 つの鍵だけ (重なりの鍵は無い = DB が write_attempts − stored_rows で数える)
  const r = readInTx((rdb) => readNeFetchCounts(rdb));
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.deepEqual(r.setproducts.seal, { fetched_rows: 14, write_attempts: 6, stored_rows: 4, dropped_no_code: 5, dropped_missing_fields: 3 });
  assert.deepEqual(r.products.seal, { fetched_rows: 0, write_attempts: 0, stored_rows: 0, dropped_no_code: 0, dropped_missing_fields: 0 });
  assert.equal(r.fetch_fingerprint, computeFetchFingerprint());   // 両方の取得の版が同じ = 照合が封へ渡す版
  assert.deepEqual(Object.keys(r.setproducts.seal), [...NE_FETCH_SEAL_KEYS]);
});

await ta('[4] 最後のページが短い (R-g): 2 ページ目が 600 行で止まる = 記録に残る (pages・page_rows)。この PR では見抜かない (照合の only_in_cdb で見る)', async () => {
  await nextSecond();
  ne.goodsPages = [Array.from({ length: 1000 }, (_, i) => g(`P${i}`)), Array.from({ length: 600 }, (_, i) => g(`Q${i}`)), Array.from({ length: 900 }, (_, i) => g(`R${i}`))];
  ne.calls = [];
  await quietly(fetchProducts);
  const c = counts('products');
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.pages, c.page_rows, c.last_page_rows], [1600, 1600, 1600, 2, [1000, 600], 600]);
  assert.equal(ne.calls.filter((x) => x.goods).length, 2);   // 3 ページ目は呼ばれない (今の止め方のまま)
  assert.equal(readInTx((rdb) => readNeFetchCounts(rdb)).products.ok, true);
  // セットも同じ
  ne.setPages = [Array.from({ length: 1000 }, (_, i) => sg(`T${i}`, 'C1')), Array.from({ length: 3 }, (_, i) => sg(`U${i}`, 'C1'))];
  await quietly(fetchSetProducts);
  const s = counts('setproducts');
  assert.deepEqual([s.fetched_rows, s.write_attempts, s.stored_rows, s.pages, s.page_rows, s.last_page_rows], [1003, 1003, 1003, 2, [1000, 3], 3]);
  ne.goodsPages = null; ne.setPages = null;
});

await ta('[5] 途中のページで失敗: 商品 = 印も件数も無い (前の回の分も消えたまま) / セット = 入れ替えない・前の回の印と件数がそのまま', async () => {
  await nextSecond();
  ne.goods = Array.from({ length: 1500 }, (_, i) => g(`F${i}`));
  ne.failAt = { goods: true, offset: 1000 };
  try { await assert.rejects(quietly(fetchProducts), /テストの途中の失敗/); } finally { ne.failAt = null; }
  for (const k of ['complete_at', 'complete_count', 'complete_rev', 'integrity', 'fetch_counts']) assert.equal(meta(`ne_api_products_${k}`), null, k);
  const pr = readInTx((rdb) => readNeFetchCounts(rdb)).products;   // 取得中の印が残る (途中で失敗した回)
  assert.deepEqual([pr.ok, pr.reason, pr.in_progress.kind, pr.in_progress.version], [false, 'fetch_in_progress', 'products', 'fc1']);
  const before = { at: meta('ne_api_setproducts_complete_at'), fc: meta(NE_FETCH_COUNTS_KEY.setproducts), rows: db().prepare('SELECT COUNT(*) AS n FROM raw_ne_set_products').get().n };
  ne.setgoods = Array.from({ length: 1500 }, (_, i) => sg(`V${i}`, 'C1'));
  ne.failAt = { goods: false, offset: 1000 };
  try { await assert.rejects(quietly(fetchSetProducts), /テストの途中の失敗/); } finally { ne.failAt = null; }
  assert.deepEqual({ at: meta('ne_api_setproducts_complete_at'), fc: meta(NE_FETCH_COUNTS_KEY.setproducts), rows: db().prepare('SELECT COUNT(*) AS n FROM raw_ne_set_products').get().n }, before);
  // 前の回の印と件数は書き換えない。ただし取得中の印が残る = 読む関数は fetch_in_progress (開く前のゲートが拒む)
  const sr = readInTx((rdb) => readNeFetchCounts(rdb)).setproducts;
  assert.deepEqual([sr.ok, sr.reason, sr.in_progress.kind], [false, 'fetch_in_progress', 'setproducts']);
  const ip = meta(NE_FETCH_IN_PROGRESS_KEY.setproducts);
  db().prepare('DELETE FROM sync_meta WHERE key = ?').run(NE_FETCH_IN_PROGRESS_KEY.setproducts);
  assert.equal(readInTx((rdb) => readNeFetchCounts(rdb)).setproducts.ok, true);   // 取得中の印を除けば、前の回の集合と前の回の件数 = 同じ回
  setMeta(NE_FETCH_IN_PROGRESS_KEY.setproducts, ip);
});

await ta('[6] fail-closed (商品): 同じ秒の 2 回目の取得で前の回の行が今回の時刻に残る = stored_rows > write_attempts (重なりが合わない) → 印も件数も書かない', async () => {
  // 時計を止める (now() が同じ秒を返す)。本番では 1 日 1 回なので起きないが、起きたら「この回の集合」が前の回の行で汚れる = 印を付けてはいけない
  await nextSecond();   // [5] の失敗した回の行 (同じ秒) と分ける
  const RealDate = Date;
  const fixed = RealDate.now();
  class FrozenDate extends RealDate { constructor(...a) { super(...(a.length ? a : [fixed])); } static now() { return fixed; } }
  globalThis.Date = FrozenDate;
  try {
    ne.goods = [g('W1'), g('W2'), g('W3')];
    await quietly(fetchProducts);
    assert.equal(counts('products').stored_rows, 3);
    ne.goods = [g('W1'), g('W2')];   // 同じ秒 = W3 の行も今回の時刻のまま
    warnings.length = 0;
    await quietly(fetchProducts);
  } finally { globalThis.Date = RealDate; }
  for (const k of ['complete_at', 'complete_count', 'complete_rev', 'integrity', 'fetch_counts']) assert.equal(meta(`ne_api_products_${k}`), null, k);
  assert.ok(warnings.some((w) => /取得の件数が合わない \(stored_gt_attempts, stored_ne_attempts_minus_duplicates\)/.test(w)), warnings.join('\n'));
  assert.ok(meta(NE_FETCH_IN_PROGRESS_KEY.products), '取得中の印は残る (印を付けなかった回)');
  // raw 表の書き込みは今までどおり (W1・W2 は今回の値で上書き済み)
  assert.equal(db().prepare("SELECT COUNT(*) AS n FROM raw_ne_products WHERE 商品コード IN ('w1', 'w2', 'w3')").get().n, 3);
});

await ta('[7] fail-closed (セット): 書いた行が DB で数えると足りない (trigger で 1 行の時刻を変える) → 入れ替えは今までどおり・印・件数・整合の証跡を消す', async () => {
  await nextSecond();
  ne.setgoods = [sg('Y1', 'C1'), sg('Y2', 'C1')];
  await quietly(fetchSetProducts);
  assert.ok(meta('ne_api_setproducts_complete_at'));
  db().exec("CREATE TRIGGER t_w9_shift AFTER INSERT ON raw_ne_set_products WHEN NEW.セット商品コード = 'boom' BEGIN UPDATE raw_ne_set_products SET synced_at = 'shifted' WHERE rowid = NEW.rowid; END");
  try {
    await nextSecond();
    ne.setgoods = [sg('Z1', 'C1'), sg('BOOM', 'C1')];
    warnings.length = 0;
    await quietly(fetchSetProducts);
  } finally { db().exec('DROP TRIGGER IF EXISTS t_w9_shift'); }
  for (const k of ['complete_at', 'complete_count', 'complete_rev', 'complete_parents', 'integrity', 'fetch_counts']) assert.equal(meta(`ne_api_setproducts_${k}`), null, k);
  assert.ok(warnings.some((w) => /セット商品の取得の件数が合わない/.test(w)), warnings.join('\n'));
  assert.deepEqual(db().prepare('SELECT セット商品コード AS p FROM raw_ne_set_products ORDER BY 1').all().map((r) => r.p), ['boom', 'z1']);   // 入れ替えは行う
  assert.equal(readInTx((rdb) => readNeFetchCounts(rdb)).setproducts.reason, 'fetch_in_progress');   // 取得中の印は残る (印を付けなかった回)
  db().prepare('DELETE FROM sync_meta WHERE key = ?').run(NE_FETCH_IN_PROGRESS_KEY.setproducts);
  assert.deepEqual(readInTx((rdb) => readNeFetchCounts(rdb)).setproducts, { ok: false, reason: 'no_mark' });
});

await ta('[8] 読む: 呼び手の読み取りの取引の中 = 別の接続が次の取得を書いても、印と件数は同じ回のまま / 取引の後は新しい回', async () => {
  await nextSecond();
  ne.goods = [g('A1'), g('A2')];
  ne.setgoods = [sg('AS', 'A1')];
  await quietly(fetchProducts); await quietly(fetchSetProducts);
  const first = counts('products');
  const r = new Database(path.join(tmp, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try {
    r.exec('BEGIN');
    const a = readNeFetchCounts(r);
    assert.equal(a.ok, true);
    // 読み取りの取引の間に、別の接続 (取込) が次の取得を最後まで書く (WAL = 書き込みは止まらない)
    await nextSecond();
    ne.goods = [g('A1'), g('A2'), g('A3')];
    await quietly(fetchProducts);
    const b = readNeFetchCounts(r);
    assert.deepEqual(b, a);
    assert.equal(b.products.counts.complete_at, first.complete_at);
    // 同じ取引で読む生の P の行の数 = 件数の stored_rows (照合が使う形)
    assert.equal(r.prepare('SELECT COUNT(*) AS n FROM raw_ne_products WHERE synced_at = ?').get(b.products.counts.complete_at).n, b.products.counts.stored_rows);
    r.exec('COMMIT');
    const c = readNeFetchCounts(r);
    assert.equal(c.ok, true);
    assert.notEqual(c.products.counts.complete_at, first.complete_at);
    assert.equal(c.products.counts.stored_rows, 3);
  } finally { r.close(); }
});

await ta('[9] 読む時の不合格: 記録が無い・壊れた JSON・関係の崩れ・別の回の記録・知らない版 / 印を消すと件数も消える (CSV の取込と同じ道)', async () => {
  const good = meta(NE_FETCH_COUNTS_KEY.products);
  const rd = () => readInTx((rdb) => readNeFetchCounts(rdb)).products;
  try {
    db().prepare('DELETE FROM sync_meta WHERE key = ?').run(NE_FETCH_COUNTS_KEY.products);
    assert.deepEqual(rd(), { ok: false, reason: 'no_record' });
    setMeta(NE_FETCH_COUNTS_KEY.products, '{壊れ');
    assert.deepEqual(rd(), { ok: false, reason: 'unreadable' });
    const g0 = JSON.parse(good);
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, write_attempts: g0.write_attempts + 1 }));
    assert.deepEqual(rd(), { ok: false, reason: 'invalid', problems: ['fetched_ne_attempts_plus_dropped'] });
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, stored_rows: g0.write_attempts + 1 }));
    assert.deepEqual(rd(), { ok: false, reason: 'invalid', problems: ['stored_gt_attempts'] });
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, complete_at: '2020-01-01 00:00:00' }));
    assert.deepEqual(rd(), { ok: false, reason: 'not_this_fetch' });
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, complete_rev: g0.complete_rev - 1 }));
    assert.deepEqual(rd(), { ok: false, reason: 'not_this_fetch' });
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, version: 'fc0' }));
    assert.equal(rd().reason, 'invalid');
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, fetch_fingerprint: null }));
    assert.deepEqual(rd(), { ok: false, reason: 'invalid', problems: ['fetch_fingerprint'] });
    // 商品とセットで取得の版が違う (朝の 2 つの取得が別のコード) = 各 kind は ok でも全体は ok でない・版を返さない
    setMeta(NE_FETCH_COUNTS_KEY.products, JSON.stringify({ ...g0, fetch_fingerprint: 'f'.repeat(64) }));
    const mm = readInTx((rdb) => readNeFetchCounts(rdb));
    assert.deepEqual([mm.products.ok, mm.setproducts.ok, mm.ok, mm.fetch_fingerprint, mm.fetch_fingerprint_mismatch], [true, true, false, null, true]);
  } finally { setMeta(NE_FETCH_COUNTS_KEY.products, good); }
  assert.equal(rd().ok, true);
  for (const kind of ['products', 'setproducts']) {
    assert.ok(meta(NE_FETCH_COUNTS_KEY[kind]), kind);
    db().transaction(() => clearNeCompleteMarks(kind))();
    assert.equal(meta(NE_FETCH_COUNTS_KEY[kind]), null, kind);
  }
  const r = readInTx((rdb) => readNeFetchCounts(rdb));
  assert.deepEqual([r.ok, r.products.reason, r.setproducts.reason], [false, 'no_mark', 'no_mark']);
  assert.deepEqual(evalNeFetchCounts(null).products, { ok: false, reason: 'no_mark' });
});

await ta('[10] checkNeFetchCounts: 式・重なり・内訳・ページ・形の崩れを 1 つずつ見つける (欄の欠け・余分・負・小数・文字・途中の短いページ・満杯の最後のページ)', async () => {
  const base = { version: 'fc1', kind: 'setproducts', complete_at: '2026-10-06 00:00:00', complete_rev: 5,
    fetched_rows: 1010, write_attempts: 1003, stored_rows: 1000, dropped_no_code: 4, dropped_missing_fields: 3, dropped_missing_detail: { set_goods_detail_goods_id: 3 },
    page_limit: 1000, pages: 2, page_rows: [1000, 10], last_page_rows: 10, fetch_fingerprint: 'a'.repeat(64), notes: { quantity_defaulted_rows: 2 } };
  assert.deepEqual(checkNeFetchCounts('setproducts', base), []);
  assert.deepEqual(checkNeFetchCounts('setproducts', base, { expectDuplicates: 3 }), []);
  assert.deepEqual(checkNeFetchCounts('setproducts', base, { expectDuplicates: 2 }), ['stored_ne_attempts_minus_duplicates']);   // 書く時だけ: コードが数えた重なりと DB が合わない
  const bad = (patch, want) => assert.deepEqual(checkNeFetchCounts('setproducts', { ...base, ...patch }), want, JSON.stringify(patch));
  bad({ fetched_rows: 1011, page_rows: [1000, 11], last_page_rows: 11 }, ['fetched_ne_attempts_plus_dropped']);
  bad({ stored_rows: 1004 }, ['stored_gt_attempts']);
  bad({ dropped_missing_detail: { set_goods_detail_goods_id: 2 } }, ['detail_sum_ne_missing_fields']);
  bad({ page_rows: [1000, 9] }, ['page_rows_sum_ne_fetched', 'last_page_rows']);
  bad({ pages: 3 }, ['pages_ne_page_rows', 'last_page_rows']);
  bad({ page_rows: [999, 11], last_page_rows: 11 }, ['page_not_full_before_last']);
  bad({ page_limit: 10 }, ['page_not_full_before_last', 'last_page_not_short']);
  bad({ pages: 0, page_rows: [], fetched_rows: 0, write_attempts: 0, stored_rows: 0, dropped_no_code: 0, dropped_missing_fields: 0, dropped_missing_detail: { set_goods_detail_goods_id: 0 }, notes: { quantity_defaulted_rows: 0 } }, ['no_page']);
  bad({ dropped_missing_detail: {} }, ['detail_keys:', 'not_count:detail.set_goods_detail_goods_id']);
  bad({ dropped_missing_detail: { set_goods_detail_goods_id: 3, other: 0 } }, ['detail_keys:other,set_goods_detail_goods_id']);
  bad({ dropped_missing_detail: null }, ['dropped_missing_detail']);
  bad({ stored_rows: -1 }, ['not_count:stored_rows']);
  bad({ stored_rows: 1000.5 }, ['not_count:stored_rows']);
  bad({ stored_rows: '1000' }, ['not_count:stored_rows']);
  bad({ write_attempts: undefined }, ['not_count:write_attempts']);
  bad({ complete_rev: -1 }, ['not_count:complete_rev']);
  bad({ complete_at: '2026-10-06T00:00:00Z' }, ['complete_at']);
  bad({ version: 'fc0' }, ['version:fc0']);
  bad({ fetch_fingerprint: 'A'.repeat(64) }, ['fetch_fingerprint']);
  bad({ fetch_fingerprint: 'a'.repeat(65) }, ['fetch_fingerprint']);
  bad({ fetch_fingerprint: undefined }, ['fetch_fingerprint']);
  bad({ notes: undefined }, ['notes']);
  bad({ notes: {} }, ['note_keys:', 'not_count:notes.quantity_defaulted_rows']);
  bad({ notes: { quantity_defaulted_rows: 2, x: 1 } }, ['note_keys:quantity_defaulted_rows,x']);
  bad({ notes: { quantity_defaulted_rows: -1 } }, ['not_count:notes.quantity_defaulted_rows']);
  bad({ notes: { quantity_defaulted_rows: 1004 } }, ['quantity_defaulted_gt_attempts']);
  bad({ kind: 'products' }, ['kind:products']);
  bad({ page_rows: [1000, -10] }, ['page_rows']);
  assert.deepEqual(checkNeFetchCounts('products', { ...base, kind: 'products' }), ['detail_keys:set_goods_detail_goods_id', 'note_keys:quantity_defaulted_rows']);   // 商品は欠けで落とす欄が無い
  assert.deepEqual(checkNeFetchCounts('other', base), ['unknown_kind:other']);
  assert.deepEqual(checkNeFetchCounts('products', null), ['not_object']);
  assert.deepEqual([...NE_FETCH_SEAL_KEYS], ['fetched_rows', 'write_attempts', 'stored_rows', 'dropped_no_code', 'dropped_missing_fields']);
  assert.deepEqual(NE_FETCH_MISSING_FIELDS, { products: [], setproducts: ['set_goods_detail_goods_id'] });
  assert.deepEqual(NE_FETCH_IN_PROGRESS_KEY, { products: 'ne_api_products_in_progress', setproducts: 'ne_api_setproducts_in_progress' });
});

await ta('[11] 取得中の印 (R12): API と通信している間ずっとある (セット = 書き込みの鍵を持たない間も)・完了の印と同じ取引で消える・途中で失敗したら残る・次に最後まで取れた回が消す / 自分の印だけ消す', async () => {
  // 別の接続 (ゲートと同じ読み方) で、API の呼び出しのたびに印を見る
  const seen = [];
  ne.onCall = ({ goods }) => { seen.push([goods ? 'products' : 'setproducts', readInTx((rdb) => readNeFetchCounts(rdb))[goods ? 'products' : 'setproducts'].reason]); };
  try {
    await nextSecond();
    ne.goods = Array.from({ length: 1500 }, (_, i) => g(`M${i}`));
    ne.setgoods = Array.from({ length: 2100 }, (_, i) => sg(`N${i}`, 'C1'));
    await quietly(fetchProducts); await quietly(fetchSetProducts);
    assert.deepEqual(seen, [['products', 'fetch_in_progress'], ['products', 'fetch_in_progress'], ['setproducts', 'fetch_in_progress'], ['setproducts', 'fetch_in_progress'], ['setproducts', 'fetch_in_progress']]);
    for (const k of ['products', 'setproducts']) assert.equal(meta(NE_FETCH_IN_PROGRESS_KEY[k]), null, k);   // 完了で消える
    const r = readInTx((rdb) => readNeFetchCounts(rdb));
    assert.equal(r.ok, true, JSON.stringify(r));
    // 途中で失敗 → 残る (中身 = その回の時刻)
    await nextSecond();
    ne.failAt = { goods: false, offset: 2000 };
    let failedTs = null;
    ne.onCall = ({ goods }) => { if (!goods) failedTs = JSON.parse(meta(NE_FETCH_IN_PROGRESS_KEY.setproducts)).started_at; };
    try { await assert.rejects(quietly(fetchSetProducts), /テストの途中の失敗/); } finally { ne.failAt = null; }
    const left = JSON.parse(meta(NE_FETCH_IN_PROGRESS_KEY.setproducts));
    assert.equal(left.started_at, failedTs);
    assert.ok(/^[0-9a-f-]{36}$/.test(left.run_id) && Number.isInteger(left.pid));
    // 次に最後まで取れた回が消す
    ne.onCall = null;
    await nextSecond();
    await quietly(fetchSetProducts);
    assert.equal(meta(NE_FETCH_IN_PROGRESS_KEY.setproducts), null);
    assert.equal(readInTx((rdb) => readNeFetchCounts(rdb)).ok, true);
  } finally { ne.onCall = null; }
  // 自分の印だけ消す: A が始まり、後から B が始まった (印を上書き) → A の完了は B の印を消さない
  const a = db().transaction(() => beginNeFetch(db(), 'setproducts', '2026-10-06 00:00:00'))();
  const b = db().transaction(() => beginNeFetch(db(), 'setproducts', '2026-10-06 00:00:00'))();
  assert.notEqual(a, b);
  assert.equal(db().transaction(() => endNeFetch(db(), 'setproducts', a))(), 0);
  assert.equal(meta(NE_FETCH_IN_PROGRESS_KEY.setproducts), b);
  assert.equal(db().transaction(() => endNeFetch(db(), 'setproducts', b))(), 1);
  assert.equal(meta(NE_FETCH_IN_PROGRESS_KEY.setproducts), null);
  // 読めない値も「取得中」とみなす
  assert.deepEqual(evalNeFetchCounts({ [NE_FETCH_IN_PROGRESS_KEY.products]: '{壊れ' }).products, { ok: false, reason: 'fetch_in_progress', in_progress: null });
  assert.throws(() => beginNeFetch(db(), 'other', 'x'), /知らない種類/);
});

await ta('[12] 空白だけのコードは今までどおり落とさずに書く (設計 v14 の分類の表の「空白だけ」とは違う = 今の動きを変えない)・式は合う', async () => {
  await nextSecond();
  ne.goods = [g('  '), g('OK1')];
  ne.setgoods = [sg(' ', 'OK1'), sg('SP1', ' ')];
  await quietly(fetchProducts); await quietly(fetchSetProducts);
  const c = counts('products'), s = counts('setproducts');
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.dropped_no_code], [2, 2, 2, 0]);
  assert.deepEqual([s.fetched_rows, s.write_attempts, s.stored_rows, s.dropped_no_code, s.dropped_missing_fields], [2, 2, 2, 0, 0]);
  assert.equal(db().prepare("SELECT COUNT(*) AS n FROM raw_ne_products WHERE 商品コード = '  ' AND synced_at = ?").get(c.complete_at).n, 1);
  assert.equal(readInTx((rdb) => readNeFetchCounts(rdb)).ok, true);
});

await ta('[13] セットの数が読めない行は落とさず 1 で書く (今までどおり)・数だけ notes に残す / 取得の途中で版の対象のファイルが変わっても、記録する版は始めに計算した値', async () => {
  await nextSecond();
  const fpBefore = computeFetchFingerprint();
  ne.setgoods = [sg('Q1', 'C1', { set_goods_detail_quantity: '' }), sg('Q1', 'C2', { set_goods_detail_quantity: 'abc' }), sg('Q1', 'C3', { set_goods_detail_quantity: '0' }),
    sg('Q1', 'C4', { set_goods_detail_quantity: null }), { set_goods_id: 'Q1', set_goods_name: 's-Q1', set_goods_detail_goods_id: 'C5' }, sg('Q1', 'C6', { set_goods_detail_quantity: '2' })];
  // API と通信している間に db.js の中身が変わった (git pull など) ことにする = 読み方だけを差し替える (本物のファイルは書き換えない)
  const realRead = fs.readFileSync;
  let changedFp = null;
  ne.onCall = () => {
    fs.readFileSync = (f, ...a) => { const v = realRead.call(fs, f, ...a); return String(f).replace(/\\/g, '/').endsWith('apps/warehouse/db.js') ? v + '\n// 変わった' : v; };
    changedFp = computeFetchFingerprint();
  };
  try { await quietly(fetchSetProducts); } finally { fs.readFileSync = realRead; ne.onCall = null; }
  const c = counts('setproducts');
  assert.deepEqual([c.fetched_rows, c.write_attempts, c.stored_rows, c.dropped_no_code, c.dropped_missing_fields, c.notes], [6, 6, 6, 0, 0, { quantity_defaulted_rows: 5 }]);
  assert.deepEqual(db().prepare("SELECT 商品コード AS c, 数量 AS q FROM raw_ne_set_products WHERE セット商品コード = 'q1' ORDER BY 1").all().map((r) => r.q), [1, 1, 1, 1, 1, 2]);
  assert.ok(changedFp && changedFp !== fpBefore, '途中で中身が変われば版も変わる');
  assert.equal(c.fetch_fingerprint, fpBefore);   // 記録は始めに計算した値
  assert.equal(computeFetchFingerprint(), fpBefore);   // 差し替えを戻せば元の版
});

globalThis.fetch = realFetch;
try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
