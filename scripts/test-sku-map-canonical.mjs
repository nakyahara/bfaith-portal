/**
 * test-sku-map-canonical.mjs — Amazon SKU の対応の「決まった並べ方」とハッシュ (lib/sku-map-canonical.js・PR ⑦-0・16 §7 M7)
 *
 * 固定する契約 (Company DB の写し・miniPC・Render の 3 か所で同じハッシュになる土台):
 *   [1] 固定の入力 → 固定の文字列と固定のハッシュ (ここが落ちたら並べ方が変わった = 版を上げる)
 *   [2] 行の並び・鍵の順番に依らない。並びは UTF-8 のバイト順 (JS の UTF-16 の順ではない)
 *   [3] NULL は null・空文字・'null' の文字と区別する。列が無い (undefined) は投げる
 *   [4] 文字は変えない (NFC / NFD は別・前後の空白も数える)。対になっていないサロゲートは投げる
 *   [5] 数は安全な整数だけ ('2' の文字・1.5 は投げる。-0 は 0・2n は 2)
 *   [6] 時刻: 決まった形 (UTC・ミリ秒・Z) だけ受ける。作る側は toCanonicalTimestamp で JST / UTC / Date を同じ形に。時差の無い文字は投げる。この PC の TZ に依らない
 *   [7] 同じ鍵が 2 行 = 投げる
 *   [8] 世代: bigint > 0 だけ・数で比べる ('10' > '9')・文字で返す
 *   [9] 受ける決まり (validateSkuMap)
 *   [10] 形の変換: miniPC の行 / Render に送る形 → 決まった形。行き来してもハッシュが同じ
 * 使い方: node scripts/test-sku-map-canonical.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {
  SKU_MAP_CANON_FORMAT, serializeSkuMap, skuMapDigest, buildSkuMapGeneration, validateSkuMap,
  toCanonicalTimestamp, isCanonicalTimestamp, parseSkuMapGeneration, formatSkuMapGeneration,
  fromMiniPcRows, toMirrorWireRows, fromMirrorWireRows, SKU_MAP_GENERATION_MAX,
  CANON_TS_RE, SKU_MAP_EDGE_SPACE_CHARS, skuMapKeyProblem,
} from '../lib/sku-map-canonical.js';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const code = (c) => (e) => e.code === c;

const T1 = '2026-05-01T00:00:00.000Z', T2 = '2026-09-30T12:34:56.789Z';
const GOLDEN = () => ({
  master: [
    { seller_sku: 'pr_b-001', name: 'テスト B', created_at: T1, updated_at: T2 },
    { seller_sku: 'a-001', name: 'Aセット "特" \\ 😀\t改行\n', created_at: T1, updated_at: T1 },
  ],
  components: [
    { seller_sku: 'pr_b-001', ne_code: 'ne-002', quantity: 2, sort_order: 1, created_at: T1, updated_at: T2 },
    { seller_sku: 'pr_b-001', ne_code: 'ne-001', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 },
    { seller_sku: 'a-001', ne_code: 'zz9', quantity: 3, sort_order: 0, created_at: T2, updated_at: T2 },
  ],
});
const GOLDEN_TEXT = [
  'sku-map-canon-v1',
  'master 2',
  '["a-001","Aセット \\"特\\" \\\\ 😀\\t改行\\n","2026-05-01T00:00:00.000Z","2026-05-01T00:00:00.000Z"]',
  '["pr_b-001","テスト B","2026-05-01T00:00:00.000Z","2026-09-30T12:34:56.789Z"]',
  'components 3',
  '["a-001","zz9",3,0,"2026-09-30T12:34:56.789Z","2026-09-30T12:34:56.789Z"]',
  '["pr_b-001","ne-001",1,0,"2026-05-01T00:00:00.000Z","2026-05-01T00:00:00.000Z"]',
  '["pr_b-001","ne-002",2,1,"2026-05-01T00:00:00.000Z","2026-09-30T12:34:56.789Z"]',
  '',
].join('\n');
const GOLDEN_HASH = 'b25258775c769767afda2fed6870f949f5e89af470ca7d9155ebf1f82b9f49fb';
const hashOf = (canon) => skuMapDigest(canon).content_hash;

t('[1] 固定の入力 → 固定の文字列と固定のハッシュ (UTF-8 の sha256)', () => {
  const text = serializeSkuMap(GOLDEN());
  assert.equal(text, GOLDEN_TEXT);
  assert.equal(crypto.createHash('sha256').update(Buffer.from(GOLDEN_TEXT, 'utf8')).digest('hex'), GOLDEN_HASH);
  assert.deepEqual(skuMapDigest(GOLDEN()), { format: SKU_MAP_CANON_FORMAT, content_hash: GOLDEN_HASH, master_rows: 2, component_rows: 3 });
  assert.equal(SKU_MAP_CANON_FORMAT, 'sku-map-canon-v1');
});

t('[2] 並びと鍵の順番に依らない・並びは UTF-8 のバイト順', () => {
  const g = GOLDEN();
  const shuffled = {
    master: [g.master[1], g.master[0]].map((r) => Object.fromEntries(Object.entries(r).reverse())),
    components: [g.components[2], g.components[0], g.components[1]].map((r) => Object.fromEntries(Object.entries(r).reverse())),
  };
  assert.equal(hashOf(shuffled), GOLDEN_HASH);
  // 余計な列は数えない
  assert.equal(hashOf({ master: g.master.map((r) => ({ ...r, created_by: 'x', synced_at: 'y' })), components: g.components }), GOLDEN_HASH);
  // 'x～' (U+FF5E = EF BD 9E) と 'x😀' (U+1F600 = F0 9F 98 80): UTF-16 では 😀 (D83D) が先、UTF-8 では ～ が先
  const keys = ['x😀', 'x～'];
  assert.deepEqual([...keys].sort(), ['x😀', 'x～']);   // JS の並べ方 (UTF-16) とは違うことの確認
  const text = serializeSkuMap({
    master: keys.map((k) => ({ seller_sku: k, name: 'n', created_at: T1, updated_at: T1 })),
    components: keys.map((k) => ({ seller_sku: k, ne_code: 'c', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 })),
  });
  assert.ok(text.indexOf('"x～"') < text.indexOf('"x😀"'), text);
  // 構成は (seller_sku, ne_code) の順 (sort_order の順ではない)
  const lines = serializeSkuMap(GOLDEN()).split('\n');
  assert.ok(lines.indexOf(GOLDEN_TEXT.split('\n')[6]) < lines.indexOf(GOLDEN_TEXT.split('\n')[7]));
});

t('[3] NULL・空文字・"null" はどれも別。列が無いのは投げる', () => {
  const withName = (name) => ({ ...GOLDEN(), master: [{ ...GOLDEN().master[0], name }, GOLDEN().master[1]] });
  const hs = [null, '', 'null'].map((n) => hashOf(withName(n)));
  assert.equal(new Set(hs).size, 3);
  assert.ok(!hs.includes(GOLDEN_HASH));
  assert.match(serializeSkuMap(withName(null)), /\["pr_b-001",null,"2026-05-01T00:00:00.000Z"/);
  assert.match(serializeSkuMap(withName('null')), /\["pr_b-001","null",/);
  // 時刻・数の NULL も null と書く (受ける決まりでは弾くが、並べ方としては決まっている)
  const g = GOLDEN(); g.components[0].updated_at = null; g.components[0].quantity = null;
  assert.match(serializeSkuMap(g), /\["pr_b-001","ne-002",null,1,"2026-05-01T00:00:00.000Z",null\]/);
  // 列が無い (undefined) は null と区別して投げる
  const miss = GOLDEN(); delete miss.master[0].updated_at;
  assert.throws(() => serializeSkuMap(miss), code('SKU_MAP_MISSING_COLUMN'));
  // 鍵は NULL にできない
  const nk = GOLDEN(); nk.components[0].ne_code = null;
  assert.throws(() => serializeSkuMap(nk), code('SKU_MAP_BAD_VALUE'));
});

t('[4] 文字は変えない: NFC と NFD は別・前後の空白も数える・絵文字は通す・対でないサロゲートは投げる', () => {
  const nfc = '\u304C', nfd = '\u304B\u3099';   // が (1 文字) と か + 濁点 (2 文字)
  assert.notEqual(nfc, nfd); assert.equal(nfc.normalize('NFC'), nfd.normalize('NFC'));
  const withName = (name) => ({ ...GOLDEN(), master: [{ ...GOLDEN().master[0], name }, GOLDEN().master[1]] });
  assert.notEqual(hashOf(withName(nfc)), hashOf(withName(nfd)));
  assert.notEqual(hashOf(withName('テスト B ')), GOLDEN_HASH);
  assert.notEqual(hashOf(withName('テスト　B')), GOLDEN_HASH);   // 全角の空白
  assert.throws(() => serializeSkuMap(withName('bad\uD800')), code('SKU_MAP_BAD_VALUE'));
  const lone = GOLDEN(); lone.components[0].ne_code = 'x\uDC00';
  assert.throws(() => serializeSkuMap(lone), code('SKU_MAP_BAD_VALUE'));
  // 文字でない名前 (数) は投げる
  assert.throws(() => serializeSkuMap(withName(123)), code('SKU_MAP_BAD_VALUE'));
});

t('[5] 数は安全な整数だけ', () => {
  const withQty = (q) => { const g = GOLDEN(); g.components[0].quantity = q; return g; };
  assert.throws(() => serializeSkuMap(withQty('2')), code('SKU_MAP_BAD_VALUE'));
  assert.throws(() => serializeSkuMap(withQty(1.5)), code('SKU_MAP_BAD_VALUE'));
  assert.throws(() => serializeSkuMap(withQty(Number.MAX_SAFE_INTEGER + 1)), code('SKU_MAP_BAD_VALUE'));
  assert.throws(() => serializeSkuMap(withQty(true)), code('SKU_MAP_BAD_VALUE'));
  assert.throws(() => serializeSkuMap(withQty(2n ** 60n)), code('SKU_MAP_BAD_VALUE'));
  assert.equal(hashOf(withQty(2n)), GOLDEN_HASH);
  const z = GOLDEN(); z.components[1].sort_order = -0;
  assert.equal(hashOf(z), GOLDEN_HASH);
});

t('[6] 時刻: 決まった形だけ受ける・JST / UTC / Date / マイクロ秒を同じ形に・時差なしは投げる・TZ に依らない', () => {
  const cases = [
    ['2026-10-01T09:00:00+09:00', '2026-10-01T00:00:00.000Z'],
    ['2026-10-01T09:00:00.123+0900', '2026-10-01T00:00:00.123Z'],
    ['2026-10-01 09:00:00.123456+09', '2026-10-01T00:00:00.123Z'],   // PostgreSQL の timestamptz の文字 (JST の session)。ミリ秒未満は切り捨て
    ['2026-10-01 00:00:00.999999+00', '2026-10-01T00:00:00.999Z'],   // 繰り上げない
    ['2026-09-30T23:30:00-05:30', '2026-10-01T05:00:00.000Z'],
    ['2026-10-01T00:00:00Z', '2026-10-01T00:00:00.000Z'],
    ['2026-10-01T00:00:00.5z', '2026-10-01T00:00:00.500Z'],
    ['2026-01-01T08:59:59.999+09:00', '2025-12-31T23:59:59.999Z'],   // 年をまたぐ
  ];
  const run = () => cases.map(([i]) => toCanonicalTimestamp(i));
  const before = process.env.TZ;
  try {
    process.env.TZ = 'Asia/Tokyo'; const jst = run();
    process.env.TZ = 'America/Los_Angeles'; const la = run();
    process.env.TZ = 'UTC'; const utc = run();
    assert.deepEqual(jst, cases.map(([, o]) => o));
    assert.deepEqual(la, jst); assert.deepEqual(utc, jst);
  } finally { if (before === undefined) delete process.env.TZ; else process.env.TZ = before; }
  assert.equal(toCanonicalTimestamp(new Date(Date.UTC(2026, 9, 1, 0, 0, 0, 7))), '2026-10-01T00:00:00.007Z');
  for (const bad of ['2026-10-01 09:00:00', '2026-10-01T09:00:00', '2026-02-30T00:00:00Z', '2026-10-01T24:00:00Z', '2026-10-01T00:00:00+25:00', '0099-01-01T00:00:00Z', '', 'x', 20261001]) {
    assert.throws(() => toCanonicalTimestamp(bad), code('SKU_MAP_BAD_TIMESTAMP'), String(bad));
  }
  assert.throws(() => toCanonicalTimestamp(new Date('x')), code('SKU_MAP_BAD_TIMESTAMP'));
  // 並べ方は決まった形だけ (黙って直さない = 時刻の書き方が変わったら気づける)
  for (const bad of ['2026-10-01T00:00:00Z', '2026-10-01T09:00:00.000+09:00', '2026-10-01 00:00:00.000Z', '2026-02-30T00:00:00.000Z']) {
    assert.equal(isCanonicalTimestamp(bad), false, bad);
    const g = GOLDEN(); g.master[0].created_at = bad;
    assert.throws(() => serializeSkuMap(g), code('SKU_MAP_BAD_TIMESTAMP'), bad);
  }
  // JST の書き方で作った行も、toCanonicalTimestamp を通せば UTC の書き方と同じハッシュ
  const jstRows = GOLDEN();
  jstRows.master[0].created_at = toCanonicalTimestamp('2026-05-01 09:00:00+09');
  jstRows.master[0].updated_at = toCanonicalTimestamp('2026-09-30T21:34:56.789+09:00');
  assert.equal(hashOf(jstRows), GOLDEN_HASH);
});

t('[6b] isCanonicalTimestamp は Date で作り直して同じか (new Date(v).toISOString() === v) と同じ答え (網の目で照らす)', () => {
  const ref = (v) => { if (typeof v !== 'string' || !CANON_TS_RE.test(v)) return false; const d = new Date(v); return !Number.isNaN(d.getTime()) && d.toISOString() === v; };
  const p2 = (n) => String(n).padStart(2, '0');
  let n = 0, trues = 0;
  for (const y of ['0000', '0004', '0099', '0100', '0400', '1900', '1970', '2000', '2023', '2024', '2026', '2100', '9999']) {
    for (let mo = 0; mo <= 13; mo++) {
      for (let d = 0; d <= 32; d++) {
        for (const [h, mi, s] of [[0, 0, 0], [23, 59, 59], [24, 0, 0], [12, 60, 0], [12, 0, 60], [99, 99, 99]]) {
          const v = `${y}-${p2(mo)}-${p2(d)}T${p2(h)}:${p2(mi)}:${p2(s)}.${String((d * 37) % 1000).padStart(3, '0')}Z`;
          assert.equal(isCanonicalTimestamp(v), ref(v), v);
          n++; if (ref(v)) trues++;
        }
      }
    }
  }
  assert.ok(n > 30000 && trues > 4000, `${n} / ${trues}`);
  for (const v of ['2026-10-01T00:00:00.000z', ' 2026-10-01T00:00:00.000Z', '2026-10-01T00:00:00.000Z ', '2026-1-01T00:00:00.000Z', null, 20261001, '']) {
    assert.equal(isCanonicalTimestamp(v), ref(v), String(v));
  }
});

t('[7] 同じ鍵が 2 行 = 投げる', () => {
  const g = GOLDEN(); g.master.push({ ...g.master[0], name: '別名' });
  assert.throws(() => serializeSkuMap(g), code('SKU_MAP_DUPLICATE_KEY'));
  const c = GOLDEN(); c.components.push({ ...c.components[0], sort_order: 2 });
  assert.throws(() => serializeSkuMap(c), code('SKU_MAP_DUPLICATE_KEY'));
});

t('[8] 世代: bigint > 0 だけ・数で比べる・文字で返す', () => {
  assert.equal(parseSkuMapGeneration('10'), 10n);
  assert.equal(parseSkuMapGeneration(10), 10n);
  assert.ok(parseSkuMapGeneration('10') > parseSkuMapGeneration('9'));   // 文字で比べると '10' < '9'
  assert.ok('10' < '9');
  assert.equal(parseSkuMapGeneration('9223372036854775807'), SKU_MAP_GENERATION_MAX);
  for (const bad of ['0', 0, -1, '-1', '01', '1e3', 1.5, '1.0', ' 1', '9223372036854775808', 2 ** 53, Number.MAX_SAFE_INTEGER + 2, null, undefined, true, {}, [], '']) {
    assert.equal(parseSkuMapGeneration(bad), null, String(bad));
  }
  assert.equal(formatSkuMapGeneration(12n), '12');
  assert.equal(formatSkuMapGeneration('9007199254740993'), '9007199254740993');   // 2^53 を超えても桁が落ちない
  assert.throws(() => formatSkuMapGeneration('0'), code('SKU_MAP_BAD_GENERATION'));
  const g = buildSkuMapGeneration({ generation: 7, ...GOLDEN() });
  assert.deepEqual(g, { format: SKU_MAP_CANON_FORMAT, generation: '7', content_hash: GOLDEN_HASH, master_rows: 2, component_rows: 3 });
});

t('[9] 受ける決まり (validateSkuMap)', () => {
  assert.deepEqual(validateSkuMap(GOLDEN()), []);
  const probs = (mut) => { const g = GOLDEN(); mut(g); return validateSkuMap(g).map((x) => `${x.where}: ${x.problem}`).join(' | '); };
  assert.match(probs((g) => { g.master = []; g.components = []; }), /0 件/);
  assert.match(probs((g) => { g.master[0].seller_sku = 'PR_B-001'; g.components.forEach((c) => { if (c.seller_sku === 'pr_b-001') c.seller_sku = 'PR_B-001'; }); }), /大文字/);
  assert.match(probs((g) => { g.components[0].ne_code = ' ne-002'; }), /前後に空白/);
  assert.match(probs((g) => { g.components[0].ne_code = 'ne\u0000x'; }), /制御文字/);
  assert.match(probs((g) => { g.master[0].name = '  '; }), /名前が空/);
  assert.match(probs((g) => { g.master[0].name = null; }), /名前が空/);
  assert.match(probs((g) => { g.master[0].updated_at = '2026-10-01T00:00:00Z'; }), /時刻が決まった形でない/);
  assert.match(probs((g) => { g.components[0].created_at = null; }), /時刻が決まった形でない/);
  assert.match(probs((g) => { g.components[0].quantity = 0; }), /数量/);
  assert.match(probs((g) => { g.components[0].quantity = '2'; }), /数量/);
  assert.match(probs((g) => { g.components[0].sort_order = 2; }), /0\.\.1 でない/);   // 隙間
  assert.match(probs((g) => { g.components[0].sort_order = 0; }), /0\.\.1 でない/);   // 重なり
  assert.match(probs((g) => { g.components.push({ ...g.components[2], seller_sku: 'nobody' }); }), /親に無い SKU/);
  assert.match(probs((g) => { g.components = g.components.filter((c) => c.seller_sku !== 'a-001'); }), /構成が 0 行/);
  assert.match(probs((g) => { g.master.push({ ...g.master[0] }); }), /同じ SKU が 2 行/);
  assert.match(probs((g) => { g.components.push({ ...g.components[0], sort_order: 2 }); }), /同じ \(SKU, NE コード\)/);
  assert.match(probs((g) => { g.master[0] = 'x'; }), /オブジェクトでない/);
  assert.equal(validateSkuMap({ master: null, components: [] }).length, 1);
  // pr_ の SKU は特別扱いしない (16 §3 #10)
  assert.deepEqual(validateSkuMap({ master: [{ seller_sku: 'pr_x', name: 'x', created_at: T1, updated_at: T1 }], components: [{ seller_sku: 'pr_x', ne_code: 'pr_x', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 }] }), []);
});

t('[9b] 空白の決まりは固定の集合 (= JS の trim が削る文字と同じ・SQLite の trim より厳しい)。中の空白・全角の大文字は通す', () => {
  const ch = (cp) => String.fromCodePoint(cp);
  // 固定の集合 = 今の JS の trim が削る文字 (BMP 全部で照らす。緩めていない・実行環境で動かない)
  const trimmed = [];
  for (let cp = 0; cp <= 0xffff; cp++) if (!(cp >= 0xd800 && cp <= 0xdfff) && ch(cp).trim() === '') trimmed.push(cp);
  assert.deepEqual([...SKU_MAP_EDGE_SPACE_CHARS].map((c) => c.codePointAt(0)), trimmed);
  const edge = [0x09, 0x0a, 0x0d, 0x20, 0xa0, 0x2028, 0x3000, 0xfeff];
  for (const cp of edge) {
    assert.ok(skuMapKeyProblem(`abc${ch(cp)}`), `末尾 U+${cp.toString(16)}`);
    assert.ok(skuMapKeyProblem(`${ch(cp)}abc`), `先頭 U+${cp.toString(16)}`);
  }
  // 中の全角の空白・NBSP は通す (鍵 = core.norm_code(鍵) までは求めない。正規化の重なりは切替前の片付け)
  assert.equal(skuMapKeyProblem(`ab${ch(0x3000)}c`), null);
  assert.equal(skuMapKeyProblem(`ab${ch(0xa0)}c`), null);
  // 中の TAB は制御文字として断る (前からの決まり)
  assert.match(skuMapKeyProblem('ab\tc'), /制御文字/);
  // 大文字は ASCII だけ (SQLite の lower() と同じ範囲)。全角の Ａ・É は通す
  assert.match(skuMapKeyProblem('abC'), /大文字/);
  assert.equal(skuMapKeyProblem(`ab${ch(0xff21)}`), null);
  assert.equal(skuMapKeyProblem(`ab${ch(0xc9)}`), null);
  assert.match(skuMapKeyProblem('x'.repeat(256)), /255/);
  assert.equal(skuMapKeyProblem('x'.repeat(255)), null);
  // 名前: 空白だけ (全角の空白だけも) は空。中の TAB・改行は通す
  const probs = (mut) => { const g = GOLDEN(); mut(g); return validateSkuMap(g).map((x) => `${x.where}: ${x.problem}`).join(' | '); };
  assert.match(probs((g) => { g.master[0].name = ch(0x3000).repeat(2); }), /名前が空/);
  assert.match(probs((g) => { g.master[0].name = ` ${ch(0xa0)}\t`; }), /名前が空/);
  assert.equal(probs((g) => { g.master[0].name = `a\tb${ch(0x3000)}`; }), '');
  assert.match(probs((g) => { g.components[0].ne_code = `ne-002${ch(0x3000)}`; }), /前後に空白/);
});

t('[10] 形の変換: miniPC の行 / Render に送る形 ↔ 決まった形 (行き来してもハッシュが同じ)', () => {
  const g = GOLDEN();
  const mini = fromMiniPcRows({
    masterRows: g.master.map((m) => ({ seller_sku: m.seller_sku, 商品名: m.name, created_at: m.created_at, updated_at: m.updated_at, created_by: 'a', updated_by: 'b' })),
    componentRows: g.components.map((c) => ({ seller_sku: c.seller_sku, ne_code: c.ne_code, 数量: c.quantity, sort_order: c.sort_order, created_at: c.created_at, updated_at: c.updated_at })),
  });
  assert.equal(hashOf(mini), GOLDEN_HASH);
  const wire = toMirrorWireRows(g);
  assert.deepEqual(Object.keys(wire.sku_resolved[0]).sort(), ['component_created_at', 'component_updated_at', 'ne_code', 'quantity', 'seller_sku', 'sort_order', 'source', 'source_updated_at', '商品名'].sort());
  assert.equal(wire.sku_resolved[0].商品名, 'テスト B'); assert.equal(wire.sku_resolved[0].source_updated_at, T2);
  const back = fromMirrorWireRows(wire);
  assert.deepEqual(back.issues, []);
  assert.equal(hashOf(back), GOLDEN_HASH);
  // 親から写しただけの列が親と違えば問題にする (ハッシュに入らない列を黙って受けない)
  const bad = toMirrorWireRows(g);
  bad.sku_resolved[0].商品名 = '別の名前'; bad.sku_resolved[1].source_updated_at = T1; bad.sku_resolved[2].source = 'auto';
  const issues = fromMirrorWireRows(bad).issues.map((x) => x.where);
  assert.deepEqual(issues, ['sku_resolved[0].商品名', 'sku_resolved[1].source_updated_at', 'sku_resolved[2].source']);
  // 古い送り手の 数量 は世代つきでは受けない (quantity だけ)
  const old = toMirrorWireRows(g); old.sku_resolved[0].数量 = old.sku_resolved[0].quantity; delete old.sku_resolved[0].quantity;
  assert.match(validateSkuMap(fromMirrorWireRows(old)).map((x) => x.problem).join(), /数量/);
});

console.log(`\n${passed} 件 PASS`);
