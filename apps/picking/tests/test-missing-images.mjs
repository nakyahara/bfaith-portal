/**
 * test-missing-images.mjs — 「画像が出ない商品」一覧 (2026-08-31 中原さん依頼) の検証。
 *
 * 守りたいこと:
 *   - 写真が1枚も出ない商品を漏れなく出す (取得失敗・未取得も含む)
 *   - バリエーション商品で「そのSKU自身の写真が無く別バリエーションの写真が出る」ものを別枠で出す
 *   - 楽天の商品管理番号を出す (キャッシュ済みを優先し、無ければ mirror から今解決する)
 *   - 期間・再ピックバッチの扱いが速さ統計と同じ
 * 実行: node apps/picking/tests/test-missing-images.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-missing-img-'));
process.env.RAKUTEN_SHOP_SLUG = 'b-faith';

const { initPickingDB, getDB, jstToday } = await import('../db.js');
const { initMirrorDB, getMirrorDB } = await import('../../warehouse-mirror/db.js');
const { listMissingImages, missingImagesCsv, rakutenItemUrl, clearMirrorMapsCache, requestForceRefresh, isImageQueueBusy } = await import('../images.js');

initPickingDB();
initMirrorDB();
const db = getDB();
const mdb = getMirrorDB();

let passed = 0;
const t = (name, fn) => {
  const out = fn();
  if (out && typeof out.then === 'function') { pending.push(out.then(() => { passed++; console.log(`  ok: ${name}`); })); return; }
  passed++; console.log(`  ok: ${name}`);
};
const pending = [];

const today = jstToday();
const shift = (d, n) => { const x = new Date(`${d}T00:00:00Z`); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
const now = new Date().toISOString().slice(0, 19) + 'Z';

// ─── ピッキングバッチと明細 ───
let batchSeq = 0;
function mkBatch(workDate, { origin = 'csv', validity = 'valid' } = {}) {
  const id = ++batchSeq;
  db.prepare(`INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition,
      line_count, slip_count, total_qty, status, validity, worker, origin, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, 'AES《単品》', '出荷_1', ?, '単品', 0, 0, 0, 'done', ?, '星', ?, ?, 'test', ?, ?)`)
    .run(id, `TB${id}`, workDate, validity, origin, `sha${id}`, now, now);
  return id;
}
let lineSeq = 0;
function addLine(batchId, sku, qty = 1, name = null) {
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty, status)
    VALUES (?, ?, '001-001-01', 'P3FA', ?, ?, NULL, ?, 'done')`).run(batchId, ++lineSeq, sku, name, qty);
}
const cacheImg = (sku, { mn = null, variant = null, white = null, top = null, status = 'ok' } = {}) =>
  db.prepare(`INSERT INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)`).run(sku, mn, white, top, variant, status, now);

const b1 = mkBatch(today);
// ① 画像あり (単品・共通写真のみ → 共有が無いのでバリエーション扱いにしない)
addLine(b1, 'ok-single', 2, '写真ありの単品');
cacheImg('ok-single', { mn: 'ok-single', white: 'https://image.rakuten.co.jp/b-faith/cabinet/a.jpg' });
// ② 楽天に画像が無い (not_found・管理番号も引けない)
addLine(b1, 'nf-noitem', 3, '楽天に無い商品');
cacheImg('nf-noitem', { status: 'not_found' });
// ③ 取得に失敗 (error・管理番号あり)
addLine(b1, 'err-sku', 1, '取得失敗の商品');
cacheImg('err-sku', { mn: 'err-sku', status: 'error' });
// ④ まだ取得していない (キャッシュ行なし)
addLine(b1, 'uncached-sku', 5, '未取得の商品');
// ⑤ バリエーション: 同じ管理番号を3SKUで共有。うち2つは自分の写真なし
for (const [sku, variant] of [['kofunneil-0639', 'https://image.rakuten.co.jp/b-faith/cabinet/v639.jpg'],
  ['kofunneil-0776', null], ['kofunneil-0801', null]]) {
  addLine(b1, sku, 1, `胡粉ネイル ${sku}`);
  cacheImg(sku, { mn: 'kofunneil', variant, white: 'https://image.rakuten.co.jp/b-faith/cabinet/kofun.jpg' });
}
// ⑥ 期間外 (40日前) と ⑦ 再ピックバッチ・無効バッチ → どれも出ない
addLine(mkBatch(shift(today, -40)), 'old-sku', 1, '期間外');
addLine(mkBatch(today, { origin: 'repick' }), 'repick-sku', 1, '再ピック');
addLine(mkBatch(today, { validity: 'invalid' }), 'invalid-sku', 1, '差し替え前');

// mirror: nf-noitem は楽天コードだけあって商品ページは無い / uncached-sku は今なら管理番号が引ける
mdb.prepare("INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, 'test', ?)")
  .run('nf-noitem-v2', 'nf-noitem', now);
mdb.prepare("INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, 'test', ?)")
  .run('uncached-sku', 'uncached-sku', now);
const insItem = mdb.prepare(`INSERT INTO mirror_rakuten_item_daily (date_jst, item_manage_number, source_run_id, source_row_hash, synced_at) VALUES (?, ?, 'test', ?, ?)`);
for (const mn of ['uncached-sku', 'kofunneil', 'err-sku', 'ok-single']) insItem.run(today, mn, `h-${mn}`, now);

const r = listMissingImages({ until: today, days: 30 });
const bySku = new Map(r.missing.map((x) => [x.sku, x]));

t('写真が1枚も出ない商品を出す (楽天に無い・取得失敗・未取得)', () => {
  assert.deepEqual([...bySku.keys()].sort(), ['err-sku', 'nf-noitem', 'uncached-sku']);
  assert.equal(bySku.get('nf-noitem').status, 'not_found');
  assert.equal(bySku.get('err-sku').status, 'error');
  assert.equal(bySku.get('uncached-sku').status, 'uncached', 'キャッシュ行が無いものも出る');
});

t('明細数の多い順・数量と最終ピッキング日つき', () => {
  // どれも1明細なので同数 → SKU 昇順で安定させる
  assert.deepEqual(r.missing.map((x) => x.sku), ['err-sku', 'nf-noitem', 'uncached-sku']);
  assert.equal(bySku.get('uncached-sku').qty, 5);
  assert.equal(bySku.get('nf-noitem').lastDate, today);
  assert.equal(bySku.get('nf-noitem').name, '楽天に無い商品', '商品名はロジザード名 (pk_lines)');
});

t('楽天の商品管理番号: キャッシュ済みを優先し、無ければ mirror から今解決する', () => {
  assert.equal(bySku.get('err-sku').manageNumber, 'err-sku', 'キャッシュ済み');
  assert.equal(bySku.get('uncached-sku').manageNumber, 'uncached-sku', 'mirror から解決');
  assert.equal(bySku.get('uncached-sku').itemUrl, 'https://item.rakuten.co.jp/b-faith/uncached-sku/');
  assert.equal(bySku.get('nf-noitem').manageNumber, null, '楽天に商品ページが無ければ null');
  assert.equal(bySku.get('nf-noitem').itemUrl, null);
  assert.equal(bySku.get('nf-noitem').rakutenCode, 'nf-noitem-v2', '楽天SKUコードは分かるので手がかりとして出す');
});

t('再取得で直る可能性のフラグ (error/未取得/後から出品された)', () => {
  assert.equal(bySku.get('err-sku').retryable, true);
  assert.equal(bySku.get('uncached-sku').retryable, true);
  assert.equal(bySku.get('nf-noitem').retryable, false, '管理番号が引けないものは何度取り直しても同じ');
});

t('バリエーション: 同じ商品ページを共有していて自分の写真が無いものだけ出す', () => {
  assert.deepEqual(r.variantMissing.map((x) => x.sku).sort(), ['kofunneil-0776', 'kofunneil-0801']);
  const x = r.variantMissing.find((v) => v.sku === 'kofunneil-0776');
  assert.equal(x.sharedBy, 3, '同じ管理番号を共有するSKU数');
  assert.equal(x.manageNumber, 'kofunneil');
  assert.equal(x.itemUrl, 'https://item.rakuten.co.jp/b-faith/kofunneil/');
  assert.ok(x.imageUrl.endsWith('/kofun.jpg'), 'いま出ている写真 (商品共通) も見せる');
  assert.ok(!r.variantMissing.some((v) => v.sku === 'kofunneil-0639'), '自分の写真があるものは出さない');
  assert.ok(!r.variantMissing.some((v) => v.sku === 'ok-single'), '単品 (共有なし) は共通写真でも問題なし');
});

t('期間外・再ピック・無効バッチは対象外 (速さ統計と同じ)', () => {
  const all = [...r.missing, ...r.variantMissing].map((x) => x.sku);
  for (const sku of ['old-sku', 'repick-sku', 'invalid-sku']) assert.ok(!all.includes(sku), sku);
  assert.equal(r.summary.skus, 7, '期間内の有効バッチのSKUだけ数える (ok-single + 画像なし3 + 胡粉3)');
});

t('サマリ', () => {
  assert.equal(r.summary.missing, 3);
  assert.equal(r.summary.variantMissing, 2);
  assert.deepEqual(r.summary.byStatus, { not_found: 1, error: 1, uncached: 1 });
  assert.equal(r.summary.mirrorAvailable, true);
  assert.equal(r.since, shift(today, -29));
});

t('CSV: BOM つき・両区分・管理番号とURL入り', () => {
  const csv = missingImagesCsv(r);
  assert.ok(csv.startsWith('﻿'), 'Excel 用の BOM');
  const lines = csv.trim().split('\r\n');
  assert.equal(lines.length, 1 + 3 + 2);
  assert.ok(lines.some((l) => l.startsWith('画像なし,nf-noitem,')));
  assert.ok(lines.some((l) => l.includes('バリエーション画像なし,kofunneil-0776,') && l.includes('item.rakuten.co.jp/b-faith/kofunneil/')));
  assert.ok(!/,\s*undefined\s*,/.test(csv), 'undefined を書かない');
});

t('カンマ・引用符を含む商品名をエスケープする', () => {
  const csv = missingImagesCsv({
    missing: [{ sku: 'x', name: 'A,B "C"', statusLabel: 's', manageNumber: null, rakutenCode: null, itemUrl: null, lines: 1, qty: 1, lastDate: today }],
    variantMissing: [],
  });
  assert.ok(csv.includes('"A,B ""C"""'));
});

t('rakutenItemUrl: 管理番号なしは null・店舗スラッグは env', () => {
  assert.equal(rakutenItemUrl(null), null);
  assert.equal(rakutenItemUrl('abc'), 'https://item.rakuten.co.jp/b-faith/abc/');
});

t('期間を絞ると対象も減る', () => {
  const r7 = listMissingImages({ until: shift(today, -1), days: 7 });
  assert.equal(r7.summary.skus, 0, '前日までの窓には今日の明細が入らない');
  assert.equal(r7.missing.length, 0);
});

// ─── Codex R1: mirror 補完後の管理番号でバリエーション判定 / 兄弟が期間外でも共有 ───
{
  // キャッシュには管理番号が入っていない (画像取得時は mirror に商品が無かった) が、
  // 今なら mirror から解決できて、別SKUと同じ商品ページを共有している
  const bLate = mkBatch(today);
  addLine(bLate, 'late-a', 1, '後から出品A');
  cacheImg('late-a', { mn: null, white: 'https://image.rakuten.co.jp/b-faith/cabinet/late.jpg' });
  addLine(bLate, 'late-b', 1, '後から出品B');
  cacheImg('late-b', { mn: null, white: 'https://image.rakuten.co.jp/b-faith/cabinet/late.jpg' });
  for (const sku of ['late-a', 'late-b']) {
    mdb.prepare("INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, 'test', ?)")
      .run(sku, sku, now);
  }
  insItem.run(today, 'late', 'h-late', now);   // late-a / late-b → ハイフン削りで 'late' に解決
  // 兄弟SKUが期間内に無いケース: sibling-old はキャッシュにだけ居る (今日の明細は sibling-new のみ)
  addLine(bLate, 'sibling-new', 1, '兄弟が期間外');
  cacheImg('sibling-new', { mn: 'sibling', white: 'https://image.rakuten.co.jp/b-faith/cabinet/sib.jpg' });
  cacheImg('sibling-old', { mn: 'sibling', variant: 'https://image.rakuten.co.jp/b-faith/cabinet/sib-old.jpg' });

  clearMirrorMapsCache();
  const r2 = listMissingImages({ until: today, days: 30 });
  const v2 = new Map(r2.variantMissing.map((x) => [x.sku, x]));

  t('mirror で補完した管理番号でもバリエーション判定する (Codex R1 High)', () => {
    assert.ok(v2.has('late-a') && v2.has('late-b'), 'キャッシュに管理番号が無くても共有を検出');
    assert.equal(v2.get('late-a').manageNumber, 'late');
    assert.equal(v2.get('late-a').sharedBy, 2);
  });

  t('兄弟SKUがこの期間に流れていなくてもバリエーション商品として扱う', () => {
    assert.ok(v2.has('sibling-new'), '共有数の母集団は画像キャッシュ全体 (商品構造は期間で変わらない)');
    assert.equal(v2.get('sibling-new').sharedBy, 2);
  });

  t('再取得の対象数をサマリに出す (管理番号があり直る可能性のあるものだけ)', () => {
    assert.equal(typeof r2.summary.retryable, 'number');
    assert.ok(r2.summary.retryable >= 1);
    // nf-noitem は管理番号が引けないので対象外
    assert.ok(!r2.missing.some((x) => x.sku === 'nf-noitem' && x.retryable && x.manageNumber));
  });
}

t('CSV: Excel の数式として評価される先頭文字を無害化する (Codex R1)', () => {
  const csv = missingImagesCsv({
    missing: [
      { sku: '=cmd', name: '=1+1', statusLabel: '@x', manageNumber: '-mn', rakutenCode: '+rc', itemUrl: null, lines: 1, qty: 1, lastDate: today },
    ],
    variantMissing: [],
  });
  const line = csv.trim().split('\r\n')[1];
  assert.ok(line.startsWith("画像なし,'=cmd,"), `SKU が無害化される: ${line}`);
  assert.ok(line.includes("'=1+1"), '商品名');
  assert.ok(line.includes("'@x"), '状態');
  assert.ok(line.includes("'-mn"), '管理番号');
  assert.ok(line.includes("'+rc"), '楽天SKUコード');
});

t('強制再取得: 対象ゼロでも落ちない・実行中は null (呼び出し側が 409)', async () => {
  assert.equal(isImageQueueBusy(), false);
  const empty = await requestForceRefresh([], 'test');
  assert.equal(empty.requested, 0);
});

t('強制再取得: 解決に失敗したら例外を返す (キュー混雑=null と区別する — Codex R2)', async () => {
  const { ensureImagesFor } = await import('../images.js');
  await assert.rejects(
    () => ensureImagesFor(['boom-sku'], { force: true, loadMaps: () => { throw new Error('mirror down'); } }),
    /mirror down/);
  // requestForceRefresh 経由でも握り潰さない
  const { queueEnsureImages } = await import('../images.js');
  let caught = null;
  await queueEnsureImages(['boom-sku2'], 'test', {
    force: true,
    loadMaps: () => { throw new Error('mirror down 2'); },
    onError: (e) => { caught = e; },
  });
  assert.match(String(caught?.message), /mirror down 2/);
});

t('商品名は作業日の新しい方を採る (過去日を後から取り込んでも上書きされない — Codex R2)', () => {
  const bNew = mkBatch(today);
  addLine(bNew, 'name-test', 1, '新しい名前');
  const bOld = mkBatch(shift(today, -3));           // 後から取り込んだ過去日 (rowid は大きい)
  addLine(bOld, 'name-test', 1, '古い名前');
  clearMirrorMapsCache();
  const rr = listMissingImages({ until: today, days: 30 });
  const row = rr.missing.find((x) => x.sku === 'name-test');
  assert.equal(row.name, '新しい名前');
  assert.equal(row.lastDate, today);
  assert.equal(row.lines, 2, '件数は期間内の全明細');
});

// ─── 楽天 W/AM/AL の別名 (2026-09-01 実測: 画像なし407件中124件が AL を掴んで誤判定) ───
{
  const bAl = mkBatch(today);
  addLine(bAl, 'waterbowl-m-wh', 1, 'ヘルスウォーター ボウル M 白');
  cacheImg('waterbowl-m-wh', { status: 'not_found' });   // キャッシュ時は管理番号を引けなかった
  // 同じ ne_code に3行 (W=商品番号 / AM=連携SKU / AL=連番)。AL だけでは商品管理番号に届かない
  const insMap = mdb.prepare("INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?, ?, ?, ?)");
  insMap.run('waterbowl-m-wh', 'waterbowl-m-wh', 'am', now);
  insMap.run('394', 'waterbowl-m-wh', 'al', now);        // 後から入る = 旧実装ではこれが勝っていた
  insItem.run(today, 'waterbowl-m', 'h-wb', now);
  clearMirrorMapsCache();
  const r3 = listMissingImages({ until: today, days: 30 });
  const row = r3.missing.find((x) => x.sku === 'waterbowl-m-wh');

  t('AL (連番) が最後に入っていても、AM/W から商品管理番号を解決する', () => {
    assert.equal(row.manageNumber, 'waterbowl-m', '旧実装は 394 を掴んで null になっていた');
    assert.equal(row.itemUrl, 'https://item.rakuten.co.jp/b-faith/waterbowl-m/');
    assert.equal(row.retryable, true, 'キャッシュ時に引けず今は引ける → 再取得で直る可能性');
  });

  t('楽天SKUコード欄には分かっているコードを全部並べる (連番だけ見せない)', () => {
    assert.ok(row.rakutenCode.includes('waterbowl-m-wh'), 'AM');
    assert.ok(row.rakutenCode.includes('394'), 'AL');
    assert.match(row.rakutenCode, / \/ /, '区切りは " / "');
  });
}

// ─── 画面テンプレート (EJS) の描画 + インラインJSの構文 ───
// (2026-08-25 事故: EJS内のJS文字列に実改行が混入し、画面のボタンが全部無反応になった)
{
  const ejs = (await import('ejs')).default;
  const vm = await import('node:vm');
  const { fileURLToPath } = await import('node:url');
  const viewPath = new URL('../views/admin_missing_images.ejs', import.meta.url);
  const src = fs.readFileSync(viewPath, 'utf8');
  const cases = [
    ['データあり', r],
    ['空 (該当なし)', { since: today, until: today, days: 30, summary: { skus: 0, missing: 0, variantMissing: 0, byStatus: {}, mirrorAvailable: true }, missing: [], variantMissing: [] }],
    ['mirror無し', { ...r, summary: { ...r.summary, mirrorAvailable: false } }],
  ];
  for (const [label, result] of cases) {
    let html = null;
    try {
      html = ejs.render(src, { title: 't', username: 'u', displayName: 'd', isAdmin: true, result, retried: null },
        { filename: fileURLToPath(viewPath) });
    } catch (e) { throw new Error(`${label}: 描画失敗 ${e.message}`); }
    const js = html.split('<script>').pop().split('</script>')[0];
    try { new vm.Script(js); } catch (e) { throw new Error(`${label}: インラインJSが構文エラー ${e.message}`); }
    assert.equal((html.match(/<script>/g) || []).length, (html.match(/<\/script>/g) || []).length, `${label}: scriptタグ数一致`);
    assert.ok(!/onclick="[^"]*"[^ >]/.test(html), `${label}: onclick 属性の引用符が壊れていない`);
    passed++;
    console.log(`  ok: 画面 (${label})`);
  }
  const html = ejs.render(src, { title: 't', username: 'u', displayName: 'd', isAdmin: true, result: r, retried: null },
    { filename: fileURLToPath(viewPath) });
  t('画面に商品管理番号と楽天商品ページのリンクが出る', () => {
    assert.ok(html.includes('https://item.rakuten.co.jp/b-faith/kofunneil/'), '商品ページURL');
    assert.ok(html.includes('>uncached-sku</a>'), '解決した管理番号がリンクになる');
    assert.ok(html.includes('nf-noitem'), '管理番号が無い商品も一覧に出る');
    assert.ok(html.includes('missing-images.csv?until='), 'CSV リンク');
  });
}

await Promise.all(pending);
db.close();
try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 後始末失敗は無視 */ }
console.log(`\ntest-missing-images: ${passed} 件 pass`);
