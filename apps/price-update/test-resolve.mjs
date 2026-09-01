/**
 * test-resolve.mjs — 引き当て・セット展開・ライブ価格・監査記録の検証 (要件 F1/F2/F6)
 *
 * mirror を模した一時 SQLite に対して、実コードをそのまま通す。
 * モールAPI (楽天 details-bulk / Yahoo getItemDetail) だけ差し替える。
 *
 * ここで守りたいこと:
 *   ・マスタに載っているだけの行を confirmed にしない (更新できるのはライブ確認が取れた行だけ)
 *   ・楽天の文字列価格 ("1000") を整数にする — しないと M2 の楽観ロックが全件 conflict になる
 *   ・セット原価は構成品が1つでも欠けたら null (欠けた分を 0 円で埋めない)
 *   ・監査は追記のみで、状態はイベントから導出される
 *
 * 実行: node apps/price-update/test-resolve.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { buildTargets, findSetsContaining, setCostOf, listingUrl, parentCodeOf, normCode } from './resolve.js';
import { toIntPrice, fetchRakutenPrices, fetchYahooPrices as fetchYahooPricesRaw, _resetCodeMapCache } from './live-price.js';
// 既存ケースは「候補=そのコードだけ」で呼ぶ
const fetchYahooPrices = (codes, deps) => (Array.isArray(codes) && typeof codes[0] === 'string'
  ? fetchYahooPricesRaw(codes.map((c) => ({ key: c, candidates: [c] })), deps)
  : fetchYahooPricesRaw(codes, deps));
import { createTables, insertRun, appendEvent, currentStates, getRun, listRuns } from './db.js';
import { buildPreviewRows, evaluateRows, parseCodes, parseStrictPrice } from './router.js';
import { rakutenShippingLabel, yahooPostageLabel } from './shipping-labels.js';
import { loadShippingRates, resolveMallShippingCost, familyOf } from './shipping-cost.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'price-update-test-'));
const db = new Database(path.join(tmpDir, 'mirror.db'));

// ── mirror 相当の表を用意 (本番と同じ列名) ──
db.exec(`CREATE TABLE mirror_products (
  商品コード TEXT PRIMARY KEY, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
  標準売価 REAL, 原価 REAL, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, セット構成品数 INTEGER)`);
db.exec(`CREATE TABLE mirror_set_components (
  セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1,
  構成商品名 TEXT, 構成商品原価 REAL, updated_at TEXT NOT NULL,
  PRIMARY KEY (セット商品コード, 構成商品コード))`);
db.exec(`CREATE TABLE mirror_rakuten_sku_map (rakuten_code TEXT PRIMARY KEY, ne_code TEXT NOT NULL, source TEXT NOT NULL, updated_at TEXT NOT NULL, manage_number TEXT)`);
db.exec(`CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code))`);
db.exec(`CREATE TABLE mirror_amazon_price_snapshot_daily (date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT, my_price REAL, buybox_price REAL, fetched_at TEXT, PRIMARY KEY (date_jst, seller_sku))`);
db.exec(`CREATE TABLE dim_mall (mall_key TEXT PRIMARY KEY, label TEXT, display_order INTEGER, is_channel INTEGER, in_daily_summary INTEGER, tax_included INTEGER, fee_rate_approx REAL)`);
createTables(db);

const insProduct = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
insProduct.run('abc-001', 'テスト商品A', '単品', '取扱中', 1200, 500, '確定', 100, '501', 'ネコポス', 0.1, null);
insProduct.run('abc-002', 'テスト商品B', '単品', '取扱中', 900, 300, '確定', 100, '501', 'ネコポス', 0.1, null);
insProduct.run('abc-set', 'A+Bセット', 'セット', '取扱中', 2000, null, '確定', 150, '501', 'ネコポス', 0.1, 2);
insProduct.run('abc-set2', '原価欠けセット', 'セット', '取扱中', 2500, null, '未確定', 150, '501', 'ネコポス', 0.1, 2);
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set', 'abc-001', 2, 'テスト商品A', 500, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set', 'abc-002', 1, 'テスト商品B', 300, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set2', 'abc-001', 1, 'テスト商品A', 500, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set2', 'zzz-999', 1, '原価未登録品', null, 'now');
db.prepare('INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?,?,?,?)').run('abc-001', 'abc-001', 'am', 'now');
db.prepare('INSERT INTO mirror_sku_resolved VALUES (?,?,?,?)').run('AMZ-ABC-001', 'abc-001', 1, 'master');
db.prepare('INSERT INTO mirror_amazon_price_snapshot_daily VALUES (?,?,?,?,?,?)').run('2026-08-27', 'amz-abc-001', 'B00TEST', 1480, 1450, '2026-08-27T03:00:00Z');
for (const [k, fee] of [['rakuten', 0.12], ['yahoo', 0.10], ['amazon', 0.15], ['aupay', 0.10], ['qoo10', 0.10]]) {
  db.prepare('INSERT INTO dim_mall VALUES (?,?,?,?,?,?,?)').run(k, k, 1, 0, 1, 1, fee);
}

console.log('\n── 入力のパース ──');
{
  eq(parseCodes('abc-001\nabc-002, abc-001  abc-003'), ['abc-001', 'abc-002', 'abc-003'], '改行・カンマ・空白区切り + 重複除去');
  eq(parseCodes(''), [], '空入力は空配列');
}

console.log('\n── 新売価の厳格パース (勝手に切り捨てない) ──');
{
  eq(parseStrictPrice('1200'), { ok: true, value: 1200 }, '整数文字列');
  eq(parseStrictPrice(1200), { ok: true, value: 1200 }, '数値');
  eq(parseStrictPrice(''), { ok: true, value: null }, '空は未入力');
  eq(parseStrictPrice(null), { ok: true, value: null }, 'null は未入力');
  ok(!parseStrictPrice('1200.9').ok, '★小数は 1200 に切り捨てず拒否 (入力と監査の値がずれる)');
  ok(!parseStrictPrice(1200.9).ok, '数値の小数も拒否');
  ok(!parseStrictPrice('1e3').ok, '指数表記は拒否');
  ok(!parseStrictPrice('1,200').ok, 'カンマ入りは拒否');
  ok(!parseStrictPrice('-100').ok, '負数は拒否');
  ok(!parseStrictPrice({}).ok, 'オブジェクトは拒否');
}

console.log('\n── セットの逆引きと原価再計算 ──');
{
  const sets = findSetsContaining(db, ['abc-001']);
  eq(sets.map((s) => s.setCode).sort(), ['abc-set', 'abc-set2'], 'abc-001 を含むセットが両方出る');
  eq(setCostOf(db, 'abc-set').cost, 500 * 2 + 300, 'セット原価 = Σ(構成品原価 × 数量)');
  const missing = setCostOf(db, 'abc-set2');
  eq(missing.cost, null, '構成品の原価が欠けたら null (0円で埋めない)');
  eq(missing.missing, ['zzz-999'], '欠けている構成品を返す');
  const overridden = setCostOf(db, 'abc-set', new Map([['abc-001', 600]]));
  eq(overridden.cost, 600 * 2 + 300, '新原価を入れたらその分だけ差し替えて再計算');
}

console.log('\n── 引き当て (mirror 由来はすべて rule 以下) ──');
{
  const { targets, unknownCodes } = buildTargets(db, ['abc-001', 'nope-999']);
  eq(unknownCodes, ['nope-999'], '商品マスタに無いコードを返す');
  const single = targets.find((t) => t.neCode === 'abc-001');
  eq(single.rowKind, 'single', '単品行');
  const set = targets.find((t) => t.neCode === 'abc-set');
  ok(!!set && set.rowKind === 'set', 'セットも一覧に入る');
  eq(set.cost, 1300, 'セット行の原価は構成から再計算した値');
  const malls = [...new Set(single.listings.map((l) => l.mall))].sort();
  eq(malls, ['amazon', 'aupay', 'qoo10', 'rakuten', 'yahoo'].filter((m) => malls.includes(m)), 'モールごとに候補が出る');
  ok(single.listings.every((l) => l.confidence !== 'confirmed'), '★mirror 由来だけでは confirmed にしない');
  const yahoo = single.listings.filter((l) => l.mall === 'yahoo');
  ok(yahoo.some((l) => l.listingCode === 'abc-001'), 'Yahoo は「出品コード=NEコード」規則で候補を出す');
  // mirror_yahoo_sku_map / mirror_aupay_sku_map / mirror_qoo10_items が無い環境でも落ちない
  ok(true, 'sku_map 表が無くても例外にならない (fail-soft)');
}

console.log('\n── URL ──');
{
  eq(listingUrl('rakuten', 'abc-001'), 'https://item.rakuten.co.jp/b-faith/abc-001/', '楽天商品ページ');
  eq(listingUrl('yahoo', 'abc-001'), 'https://store.shopping.yahoo.co.jp/b-faith01/abc-001.html', 'Yahoo商品ページ');
  eq(listingUrl('amazon', 'AMZ', { asin: 'B00TEST' }), 'https://www.amazon.co.jp/dp/B00TEST', 'Amazonは ASIN 由来');
  eq(listingUrl('amazon', 'AMZ'), null, 'ASIN が無ければ Amazon URL は作らない');
}

console.log('\n── 楽天の文字列価格 (M0実測) ──');
{
  eq(toIntPrice('1000'), 1000, '文字列 "1000" は 1000');
  eq(toIntPrice(1000), 1000, '数値もそのまま');
  eq(toIntPrice('1,000'), null, 'カンマ入りは読めない扱い');
  eq(toIntPrice('1000abc'), null, '末尾ゴミは読めない扱い');
  eq(toIntPrice(''), null, '空は null');
  eq(toIntPrice(null), null, 'null は null');
  eq(toIntPrice(1000.0000000001), null, '★整数でない数値は丸めずに null (監査値と実価格をずらさない)');
  eq(toIntPrice(1000.5), null, '小数は null');
}

console.log('\n── 楽天カラバリ: AM/AL/W は同じSKUの別名 → 1行にまとめる (2026-08-30 実機で判明) ──');
{
  // 実データと同じ形: ne_code=0726-001802-bk に対し al=360 / am=0726-001802-bk / w=0726-001802
  db.prepare('INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
    .run('0726-001802-bk', '合皮補修シート ブラック', '単品', '取扱中', 577, 210, '確定', 182, '103', '定形外規格内（50g以内）', 0.1, null);
  for (const [rc, src] of [['360', 'al'], ['0726-001802-bk', 'am'], ['0726-001802', 'w']]) {
    // manage_number 無し = 列が足される前に同期された行。W 行から管理番号にたどる旧経路が生きていること
    db.prepare('INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at) VALUES (?,?,?,?)').run(rc, '0726-001802-bk', src, 'now');
  }
  const { targets } = buildTargets(db, ['0726-001802-bk']);
  const rak = targets[0].listings.filter((l) => l.mall === 'rakuten');
  eq(rak.length, 1, '★別名3つでも楽天の行は1つ');
  eq(rak[0].listingCode, '0726-001802', '表示は W (商品番号 = 管理番号)');
  eq(rak[0].aliases.sort(), ['0726-001802', '0726-001802-bk', '360'], '別名は候補として保持');

  // 実物と同じ 12 variant の商品。別名 360 / 0726-001802-BK のどちらでも同じ variant を指す
  const variants = {};
  const colors = ['BK', 'CL', 'WH', 'CM', 'DB', 'OW', 'BE', 'BG', 'GR'];
  colors.forEach((c, i) => { variants[String(360 + i)] = { merchantDefinedSkuId: `0726-001802-${c}`, standardPrice: '577' }; });
  for (const c of ['MG', 'BR', 'NV']) variants[`0726-001802-${c}`] = { merchantDefinedSkuId: `0726-001802-${c}`, standardPrice: '577' };

  _resetCodeMapCache();
  const deps = {
    fetchAllItemCodes: async () => ({ '0726-001802': '0726-001802' }),
    fetchItemDetailsBulkDetailed: async (mns) => ({
      items: mns.includes('0726-001802')
        ? [{ manageNumber: '0726-001802', itemNumber: '0726-001802', title: '合皮補修シート', variants }]
        : [],
      failed: [],
    }),
  };
  const prices = await fetchRakutenPrices([{ key: rak[0].listingCode, aliases: rak[0].aliases }], deps);
  const p = prices.get('0726-001802');
  eq([p.found, p.price, p.skuCode, p.manageNumber], [true, 577, '360', '0726-001802'],
    '★12SKU の中から別名で variant 360 を特定できる');

  // 別名がどの variant にも当たらない場合は確定させない (取り違え防止)
  _resetCodeMapCache();
  const miss = await fetchRakutenPrices([{ key: 'zzz-999', aliases: ['zzz-999'] }], {
    ...deps,
    fetchAllItemCodes: async () => ({ 'zzz-999': '0726-001802' }),
  });
  eq(miss.get('zzz-999').found, false, '別名がどのSKUにも当たらなければ未確定');
  ok(/[0-9]+ SKU/.test(miss.get('zzz-999').reason), '理由に SKU 数を書く: ' + miss.get('zzz-999').reason);
}

console.log('\n── ★カラバリ: W 行を持たない色 (BE) でも manage_number から商品ページへ届く (2026-09-01) ──');
{
  // 実データと同じ形: 0726-001802-be は am / al の 2 行だけ (W 行は BK が持っている)。
  // 列が足される前はここで manageNumber=366 として楽天に問い合わせて「見つかりません」になっていた
  db.prepare('INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
    .run('0726-001802-be', '合皮補修シート ベージュ', '単品', '取扱中止', 577, 210, '確定', 182, '103', '定形外規格内（50g以内）', 0.1, null);
  const ins = db.prepare('INSERT INTO mirror_rakuten_sku_map (rakuten_code, ne_code, source, updated_at, manage_number) VALUES (?,?,?,?,?)');
  ins.run('0726-001802-be', '0726-001802-be', 'am', 'now', '0726-001802');
  ins.run('366', '0726-001802-be', 'al', 'now', '0726-001802');

  const { targets } = buildTargets(db, ['0726-001802-be']);
  const rak = targets[0].listings.filter((l) => l.mall === 'rakuten');
  eq(rak.length, 1, '楽天の行は 1 つ');
  eq(rak[0].manageNumber, '0726-001802', '★W 行が無くても商品管理番号が分かる');
  eq(rak[0].listingCode, '0726-001802', '表示も商品管理番号');
  eq(rak[0].aliases.sort(), ['0726-001802-be', '366'], '別名は AM / AL');

  const variants = {};
  ['BK', 'CL', 'WH', 'CM', 'DB', 'OW', 'BE', 'BG', 'GR'].forEach((c, i) => {
    variants[String(360 + i)] = { merchantDefinedSkuId: `0726-001802-${c}`, standardPrice: '577' };
  });
  let allCodesCalled = 0;
  let asked = [];
  _resetCodeMapCache();
  const deps = {
    fetchAllItemCodes: async () => { allCodesCalled++; return {}; },
    fetchItemDetailsBulkDetailed: async (mns) => {
      asked = mns;
      return {
        items: mns.includes('0726-001802') ? [{ manageNumber: '0726-001802', itemNumber: '0726-001802', title: '合皮補修シート', variants }] : [],
        failed: mns.filter((m) => m !== '0726-001802').map((m) => ({ manageNumber: m, reason: 'Not found' })),
      };
    },
  };
  const target = { key: rak[0].listingCode, aliases: rak[0].aliases, manageNumber: rak[0].manageNumber, manageNumbers: rak[0].manageNumbers };
  const prices = await fetchRakutenPrices([target], deps);
  const p = prices.get('0726-001802');
  eq(asked, ['0726-001802'], '★問い合わせるのは商品管理番号 (366 を管理番号として投げない)');
  eq([p.found, p.price, p.skuCode, p.manageNumber], [true, 577, '366', '0726-001802'], '★BE の variant 366 = 577円 が取れる');
  eq(allCodesCalled, 0, '対応表で管理番号が決まる時は all-codes を取りに行かない');

  // ★対応表を作った後に SKU が差し替わった疑い: AL 366 は当たるが、その variant の AM が対応表に無い (Codex R2)
  const swapped = { ...variants, 366: { merchantDefinedSkuId: 'other-product-xx', standardPrice: '980' } };
  const depsSwapped = { ...deps, fetchItemDetailsBulkDetailed: async () => ({
    items: [{ manageNumber: '0726-001802', itemNumber: '0726-001802', title: 'T', variants: swapped }], failed: [] }) };
  const sw = (await fetchRakutenPrices([target], depsSwapped)).get('0726-001802');
  eq(sw.found, false, '★AL は当たるが AM が別物 → 別商品の SKU の疑いで確定しない');
  ok(/other-product-xx/.test(sw.reason) && /差し替わった/.test(sw.reason), '理由に実際の AM と疑いを書く: ' + sw.reason);
  // AM が空の variant (単品。0726-001588 の実物と同じ形) は検査の対象外
  const depsNoAm = { ...deps, fetchItemDetailsBulkDetailed: async () => ({
    items: [{ manageNumber: '0726-001588', variants: { '0726-001588': { standardPrice: '1280' } } }], failed: [] }) };
  const single = (await fetchRakutenPrices([{ key: '0726-001588', aliases: ['0726-001588'], manageNumber: '0726-001588' }], depsNoAm)).get('0726-001588');
  eq([single.found, single.price], [true, 1280], 'AM が無い単品は通る');

  // ★同じプレビューに BK と BE を並べても、片方がもう片方を上書きしない (行キーは NE コード単位)
  const both = await fetchRakutenPrices([
    { key: '0726-001802', rowKey: 'bk|rakuten', aliases: ['0726-001802-bk', '360', '0726-001802'], manageNumber: '0726-001802' },
    { key: '0726-001802', rowKey: 'be|rakuten', aliases: ['0726-001802-be', '366'], manageNumber: '0726-001802' },
  ], deps);
  eq([both.get('bk|rakuten')?.skuCode, both.get('be|rakuten')?.skuCode], ['360', '366'], '★BK=360 / BE=366 をそれぞれ返す (衝突しない)');
  eq(asked, ['0726-001802'], '同じ商品は 1 回だけ問い合わせる');

  // ★同じ NE コードが 2 つの楽天商品に紐づいていたら、どちらか決めずに未確定
  ins.run('dup-x', 'dup-001', 'am', 'now', 'page-a');
  ins.run('dup-y', 'dup-001', 'al', 'now', 'page-b');
  db.prepare('INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
    .run('dup-001', '二重出品', '単品', '取扱中', 1000, 400, '確定', 182, '103', '定形外規格内（50g以内）', 0.1, null);
  const dup = buildTargets(db, ['dup-001']).targets[0].listings.find((l) => l.mall === 'rakuten');
  eq([dup.manageNumber, dup.manageNumbers.sort()], [null, ['page-a', 'page-b']], '複数の商品管理番号 → 1 つに決めない');
  const dupPrice = await fetchRakutenPrices([{ key: dup.listingCode, aliases: dup.aliases, manageNumber: dup.manageNumber, manageNumbers: dup.manageNumbers }], deps);
  const dp = dupPrice.get(normCode(dup.listingCode));
  eq(dp.found, false, '★複数の楽天商品に紐づく NE コードは確定しない');
  ok(/page-a.*page-b|page-b.*page-a/.test(dp.reason), '理由に両方の商品管理番号を書く: ' + dp.reason);
}

console.log('\n── Yahoo カラバリ: 親の商品コード + 個別商品コード (2026-08-30 中原さん確認) ──');
{
  // 実際の登録: item_code=0726-001802 のページに sub_code=0726-001802-BK がある。
  // NEコード (0726-001802-bk) そのままでは 400 になるので、親コードも候補に入れて拾う
  eq(parentCodeOf('0726-001802-bk'), '0726-001802', 'カラー枝番を落として親コードを作る');
  eq(parentCodeOf('0726-001163'), null, '★枝番でない数字までは剥がさない (別商品を掴まないため)');
  eq(parentCodeOf('0726-001163-2'), null, '★2個セットの -2 も剥がさない');

  const calls = [];
  const yahooDeps = {
    fetchYahooItemDetail: async (c) => {
      calls.push(c);
      if (c === '0726-001802') {
        return {
          ok: true, ItemCode: '0726-001802', Name: '合皮補修シート', Price: 577,
          SubCodes: [{ SubCode: '0726-001802-BK', Price: null }, { SubCode: '0726-001802-CL', Price: null }],
        };
      }
      const e = new Error('[yahoo-proxy:get-item-detail] HTTP 400');
      throw e;
    },
  };
  const r = await fetchYahooPricesRaw(
    [{ key: '0726-001802-bk', candidates: ['0726-001802-bk', '0726-001802'] }], yahooDeps);
  const got = r.get('0726-001802-bk');
  eq([got.found, got.price, got.skuCode, got.itemCode], [true, 577, '0726-001802-BK', '0726-001802'],
    '★親コードで当てて、個別商品コードの価格 (継承なら商品価格) を採る');
  eq(calls, ['0726-001802-bk', '0726-001802'], 'NEコード → 親コードの順に試す');

  // 親ページに自分の個別商品コードが無ければ確定させない (別商品を掴まない)
  const wrong = await fetchYahooPricesRaw(
    [{ key: '0726-001802-zz', candidates: ['0726-001802-zz', '0726-001802'] }], yahooDeps);
  eq(wrong.get('0726-001802-zz').found, false, '親ページに自分のコードが無ければ未確定');
}

console.log('\n── 発送方法: モール側の設定を抜き出す (売価差の理由になるため) ──');
{
  _resetCodeMapCache();
  const rk = await fetchRakutenPrices([{ key: 'ship-001', aliases: ['ship-001'] }], {
    fetchAllItemCodes: async () => ({ 'ship-001': 'ship-001' }),
    fetchItemDetailsBulkDetailed: async () => ({
      items: [{
        manageNumber: 'ship-001', title: 'T',
        variants: {
          'ship-001': {
            standardPrice: '577', normalDeliveryDateId: 4,
            shipping: { shippingMethodGroup: '1', postageIncluded: true, singleItemShipping: 0, okihaiSetting: true },
          },
        },
      }],
      failed: [],
    }),
  });
  eq(rk.get('ship-001').shipping, { methodGroup: '1', postageIncluded: true, singleItemShipping: 0, deliveryDateId: 4 },
    '楽天: variant の shipping (配送方法セット番号・送料込みか) を返す');

  const yh = await fetchYahooPricesRaw([{ key: 'ship-y', candidates: ['ship-y'] }], {
    fetchYahooItemDetail: async (c) => ({
      ok: true, ItemCode: c, Name: 'Y', Price: 698, SubCodes: [],
      Delivery: '2', PostageSet: '1', ShipWeight: '50',
    }),
  });
  eq(yh.get('ship-y').shipping, { delivery: '2', postageSet: '1', shipWeight: '50' },
    'Yahoo: Delivery / PostageSet / ShipWeight を返す');

  const none = await fetchYahooPricesRaw([{ key: 'ship-z', candidates: ['ship-z'] }], {
    fetchYahooItemDetail: async (c) => ({ ok: true, ItemCode: c, Name: 'Y', Price: 100, SubCodes: [] }),
  });
  eq(none.get('ship-z').shipping, null, '発送情報が無ければ null (空の項目を作らない)');
}

console.log('\n── モール別の配送関係費 (既存の送料マスタを参照) ──');
{
  db.exec(`CREATE TABLE mirror_shipping_rates (
    shipping_code TEXT PRIMARY KEY, 大分類区分 TEXT, 運送会社 TEXT, 小分類区分名称 TEXT NOT NULL,
    梱包サイズ TEXT, 最大重量 TEXT, 追跡有無 TEXT, 送料 REAL, 出荷作業料 REAL,
    想定梱包資材費 REAL, 想定人件費 REAL, 配送関係費合計 REAL, 備考 TEXT,
    source_run_id TEXT, source_row_hash TEXT, synced_at TEXT)`);
  const insRate = db.prepare('INSERT INTO mirror_shipping_rates (shipping_code, 運送会社, 小分類区分名称, 最大重量, 送料, 配送関係費合計) VALUES (?,?,?,?,?,?)');
  insRate.run('103', '日本郵便', '定形外規格内（50g以内）', '50', 140, 182);
  insRate.run('501', 'ヤマト運輸', 'ネコポス', '1000', 198, 237);
  insRate.run('1201', '日本郵便', 'ゆうパケットパフ', '1000', 374, 424);

  const rates = loadShippingRates(db);
  ok(rates.available, '送料マスタを読める');

  // ネコポス = 名前が完全一致 → マスタの配送関係費合計
  eq(resolveMallShippingCost(rates, { mallMethodName: 'ネコポス', neShippingCode: '103', neShippingCost: 182 }),
    { cost: 237, source: 'mall', label: 'ネコポス', exact: true },
    '★Yahoo=ネコポスなら 237円 (商品マスタの182円ではなく)');

  // 楽天「定形外」はまとめた呼び方 → 商品マスタの段 (定形外規格内50g=182円) を使う
  eq(resolveMallShippingCost(rates, { mallMethodName: '定形外', neShippingCode: '103', neShippingCost: 182 }),
    { cost: 182, source: 'product', label: '定形外規格内（50g以内）', exact: false },
    '楽天=定形外 は商品マスタの段を使う (重さの段が決まらないため)');

  // ★モールが「定形外」で商品マスタが「定形内」= 同じ郵便定形の系統。
  //   モールの名前は設定セットの名前で、実際の段は商品マスタが正 (中原さん 8/31) → 不明にしない
  insRate.run('102', '日本郵便', '定形内（50g以内）', '50', 110, 146);
  const rates2 = loadShippingRates(db);
  eq(resolveMallShippingCost(rates2, { mallMethodName: '定形外', neShippingCode: '102', neShippingCost: 146 }),
    { cost: 146, source: 'product', label: '定形内（50g以内）', exact: false },
    '★楽天=定形外 × 商品マスタ=定形内 は同系統なので警告を出さない');

  // 系統も違う → 不明。商品マスタの値に戻すが「不明」と言う
  const unknown = resolveMallShippingCost(rates, { mallMethodName: '佐川急便', neShippingCode: '103', neShippingCost: 182 });
  eq([unknown.cost, unknown.source], [182, 'unknown'], '★決められない時は不明と言う (近い名前に寄せない)');
  eq(familyOf('定形外規格内（50g以内）'), '郵便定形', '定形系はまとめて1つの系統');
  eq(familyOf('クロネコ宅急便'), 'ヤマト宅急便', '宅急便系も1つの系統');
  eq(familyOf('よく分からない配送'), null, '分類できない名前は同系統とみなさない');

  eq(resolveMallShippingCost(rates, { mallMethodName: null, neShippingCode: '1201', neShippingCost: 424 }).cost,
    424, 'モール側が分からない行は商品マスタの値');

  // 送料マスタがまだ無い環境でも落ちない
  const empty = { rows: [], byCode: new Map(), byName: new Map(), available: false };
  eq(resolveMallShippingCost(empty, { mallMethodName: 'ネコポス', neShippingCode: '103', neShippingCost: 182 }).cost,
    182, 'マスタが無ければ商品マスタの値 (fail-soft)');
}

console.log('\n── ★行データまで配送関係費が届いているか (配線の抜けを検知する) ──');
{
  // 単体では正しく引けていても、行に載せ忘れると画面は商品マスタの値のまま。
  // 2026-08-31 に実際その抜けがあった (mirror には 25 行あるのに Yahoo が 182円 のままだった)
  db.prepare('INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 送料コード, 配送方法, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
    .run('ship-nk', 'ネコポス確認用', '単品', '取扱中', 700, 210, '確定', 182, '103', '定形外規格内（50g以内）', 0.1, null);
  const { rows } = await buildPreviewRows(db, ['ship-nk'], new Map(), {
    fetchAllItemCodes: async () => ({}),
    fetchItemDetailsBulkDetailed: async () => ({ items: [], failed: [] }),
    fetchYahooItemDetail: async (c) => ({
      ok: true, ItemCode: c, Name: 'Y', Price: 698, SubCodes: [],
      Delivery: '1', PostageSet: '6', ShipWeight: null,   // 6 = ネコポス
    }),
  });
  const y = rows.find((r) => r.mall === 'yahoo');
  eq([y.shipping, y.shippingSource, y.shippingLabel], [237, 'mall', 'ネコポス'],
    '★Yahoo行の shipping が ネコポス 237円 になっている (商品マスタの182円ではない)');
  eq(y.productShipping, 182, '商品マスタの送料も参考として持つ');
  // 粗利もその送料で計算されていること
  const ev = evaluateRows([{ ...y, newPrice: 800, selected: true }])[0].evaluation;
  // 800 - 231(原価税込) - 80(手数料10%) - 237(ネコポス) = 252
  eq(ev.estimate.gross, 800 - 231 - 80 - 237, '★粗利もネコポスの送料で計算される');
}

console.log('\n── 配送方法の番号 → 名前 ──');
{
  eq(rakutenShippingLabel(1), '1 (定形外)', '楽天 1 = 定形外');
  eq(rakutenShippingLabel('9'), '9 (ゆうパケットパフ)', '文字列の番号も引ける');
  eq(rakutenShippingLabel(99), '99', '★表に無い番号は番号だけ (勝手に名前を当てない)');
  eq(rakutenShippingLabel(null), null, '未設定は null');
  eq(yahooPostageLabel(6), '6 (ネコポス)', 'Yahoo 6 = ネコポス');
  eq(yahooPostageLabel(12), '12 (ゆうパケットパフ)', 'Yahoo 12 = ゆうパケットパフ');
  eq(yahooPostageLabel(99), '99', '★Yahoo も表に無い番号は番号だけ');
}

console.log('\n── 引き当てできないモールは行を消さず「未解決」で出す (要件 F1) ──');
{
  const { targets } = buildTargets(db, ['abc-002']);   // 楽天/Amazon の map を持たない商品
  const malls = targets[0].listings.map((l) => l.mall).sort();
  eq(malls, ['amazon', 'aupay', 'qoo10', 'rakuten', 'yahoo'], '5モールすべての行が出る');
  const rak = targets[0].listings.find((l) => l.mall === 'rakuten');
  eq(rak.confidence, 'unresolved', '楽天は未解決');
  eq(rak.listingCode, null, '出品コードは空');
  ok(rak.source.includes('出品が無いとは限りません'), '「未出品」と言い切らない');
}

console.log('\n── ライブ価格の取得 (モールAPIは差し替え) ──');
{
  _resetCodeMapCache();
  const deps = {
    fetchAllItemCodes: async () => ({ 'abc-001': 'MN-ABC-001' }),
    fetchItemDetailsBulkDetailed: async (mns) => ({
      items: mns.includes('MN-ABC-001') ? [{
        manageNumber: 'MN-ABC-001', title: '楽天のテスト商品',
        variants: { 'sku-a': { standardPrice: '1000', merchantDefinedSkuId: 'abc-001' } },
      }] : [],
      failed: mns.filter((m) => m !== 'MN-ABC-001').map((m) => ({ manageNumber: m, reason: 'not found' })),
    }),
  };
  const prices = await fetchRakutenPrices(
    [{ key: 'abc-001', aliases: ['abc-001'] }, { key: 'ghost-001', aliases: ['ghost-001'] }], deps);
  eq(prices.get('abc-001').price, 1000, '楽天のライブ価格が整数で取れる');
  eq(prices.get('abc-001').skuCode, 'sku-a', 'どの SKU かも分かる');
  eq(prices.get('ghost-001').found, false, '見つからない出品は found=false');

  const yahoo = await fetchYahooPrices(['abc-001', 'old-vps'], {
    fetchYahooItemDetail: async (c) => (c === 'abc-001'
      ? { ok: true, ItemCode: c, Name: 'Yahooのテスト商品', Price: 1180, SubCodes: [] }
      : { ok: true, ItemCode: c, Name: '未デプロイ時', Price: undefined }),
  });
  eq(yahoo.get('abc-001').price, 1180, 'Yahoo のライブ価格');
  eq(yahoo.get('old-vps').found, false, '★Price が返らない (VPS未デプロイ) 時は取得不可として扱う');
  ok(yahoo.get('old-vps').reason.includes('未デプロイ'), '理由に未デプロイの可能性を書く');
}

console.log('\n── Yahoo: 応答の取り違えと SKU別価格 (fail-closed) ──');
{
  // ★別商品の応答が返ってきたら確定させない (「価格が返った」= 実在確認 ではない)
  const mismatched = await fetchYahooPrices(['abc-001'], {
    fetchYahooItemDetail: async () => ({ ok: true, ItemCode: 'other-999', Name: '別商品', Price: 500 }),
  });
  eq(mismatched.get('abc-001').found, false, '応答の ItemCode が違えば取得不可');
  ok(mismatched.get('abc-001').reason.includes('含まれていません'), '理由に「その商品に無い」と書く');

  const notOk = await fetchYahooPrices(['abc-001'], {
    fetchYahooItemDetail: async (c) => ({ ok: false, ItemCode: c, Price: 500 }),
  });
  eq(notOk.get('abc-001').found, false, 'ok=false は取得不可');

  // SKU別価格があるのに、要求コードがどのサブコードでもない → どのSKUの価格か決められない
  const parentOfVariants = await fetchYahooPrices(['item-v'], {
    fetchYahooItemDetail: async (c) => ({
      ok: true, ItemCode: c, Name: 'バリ商品', Price: 2000,
      SubCodes: [{ SubCode: 'item-v-a', Price: 2100 }, { SubCode: 'item-v-b', Price: 2200 }],
    }),
  });
  eq(parentOfVariants.get('item-v').found, false, '★SKU別価格がある商品の親コードは未確定 (親価格で値付けさせない)');
  ok(parentOfVariants.get('item-v').reason.includes('SKU別価格'), '理由に SKU別価格と書く');

  // 要求コードがサブコードと一致 → そのサブコードの価格を使う
  const sub = await fetchYahooPrices(['item-v-b'], {
    fetchYahooItemDetail: async (c) => ({
      ok: true, ItemCode: c, Name: 'バリ商品', Price: 2000,
      SubCodes: [{ SubCode: 'item-v-a', Price: 2100 }, { SubCode: 'item-v-b', Price: 2200 }],
    }),
  });
  eq([sub.get('item-v-b').price, sub.get('item-v-b').skuCode], [2200, 'item-v-b'], 'サブコード一致ならその価格');

  // サブコードで問い合わせて応答の ItemCode が親商品になる仕様でも引き当てられる
  const subViaParent = await fetchYahooPrices(['item-v-a'], {
    fetchYahooItemDetail: async () => ({
      ok: true, ItemCode: 'item-v', Name: 'バリ商品', Price: 2000,
      SubCodes: [{ SubCode: 'item-v-a', Price: 2100 }, { SubCode: 'item-v-b', Price: 2200 }],
    }),
  });
  eq([subViaParent.get('item-v-a').price, subViaParent.get('item-v-a').skuCode], [2100, 'item-v-a'],
    '★応答が親 ItemCode でも、サブコード一致なら引き当てる');

  // 応答に要求コードがどこにも無い (ItemCode も SubCodes も違う) → 取り違えとして拒否
  const wrongItem = await fetchYahooPrices(['item-x'], {
    fetchYahooItemDetail: async () => ({
      ok: true, ItemCode: 'item-v', Name: '別商品', Price: 2000,
      SubCodes: [{ SubCode: 'item-v-a', Price: 2100 }],
    }),
  });
  eq(wrongItem.get('item-x').found, false, '要求コードが応答のどこにも無ければ拒否');

  // サブコードはあるが価格を持たない (商品価格を継承する運用) → 商品価格でよい
  const inherit = await fetchYahooPrices(['item-w'], {
    fetchYahooItemDetail: async (c) => ({
      ok: true, ItemCode: c, Name: '継承', Price: 1500,
      SubCodes: [{ SubCode: 'item-w-a', Price: null }],
    }),
  });
  eq(inherit.get('item-w').price, 1500, 'サブコードが価格を持たなければ商品価格を使う');
}

console.log('\n── プレビュー行の組み立て (confirmed への昇格) ──');
let previewRows = null;
{
  _resetCodeMapCache();
  const deps = {
    fetchAllItemCodes: async () => ({ 'abc-001': 'MN-ABC-001' }),
    fetchItemDetailsBulkDetailed: async () => ({
      items: [{ manageNumber: 'MN-ABC-001', title: 'T', variants: { 'sku-a': { standardPrice: '1000', merchantDefinedSkuId: 'abc-001' } } }],
      failed: [],
    }),
    fetchYahooItemDetail: async (c) => ({ ok: true, ItemCode: c, Name: 'Y', Price: 1180, SubCodes: [] }),
  };
  const { rows, notices } = await buildPreviewRows(db, ['abc-001'], new Map(), deps);
  previewRows = rows;
  eq(notices, [], '通知なし');
  const rak = rows.find((r) => r.mall === 'rakuten');
  eq([rak.price, rak.confidence, rak.skuCode], [1000, 'confirmed', 'sku-a'], '★ライブ価格が取れた楽天行だけ confirmed に昇格');
  const yah = rows.find((r) => r.mall === 'yahoo');
  eq([yah.price, yah.confidence], [1180, 'confirmed'], 'Yahoo も同様');
  const amz = rows.find((r) => r.mall === 'amazon');
  eq([amz.price, amz.manual], [1480, true], 'Amazon はスナップショット表示 + 手動扱い');
  ok(amz.note.includes('更新対象外'), 'Amazon は更新対象外と明記');
  ok(rows.filter((r) => r.mall === 'aupay' || r.mall === 'qoo10').every((r) => r.manual), 'auPAY / Qoo10 は手動行');
  eq(rak.feeRate, 0.12, '手数料率は dim_mall から');

  const evaluated = evaluateRows(rows.map((r) => (r.mall === 'rakuten' ? { ...r, newPrice: 1200, selected: true } : r)));
  const ev = evaluated.find((r) => r.mall === 'rakuten').evaluation;
  ok(ev.canUpdate, '確定行に妥当な新売価を入れれば更新可 (M2 で実際に送る)');
  ok(evaluated.find((r) => r.mall === 'amazon').evaluation === null, '手動行にはガード評価を付けない');
}

console.log('\n── 監査記録 (append-only・状態はイベントから導出) ──');
{
  const chosen = previewRows.filter((r) => r.mall === 'rakuten' || r.manual);
  const runId = insertRun(db, {
    createdBy: 'tester@example.com',
    neCodes: ['abc-001'],
    limits: { maxNeCodes: 20 },
    operations: chosen.map((r) => ({
      mall: r.mall, neCode: r.neCode, rowKind: r.rowKind, listingCode: r.listingCode, skuCode: r.skuCode,
      confidence: r.confidence, expectedCurrentPrice: r.price, newPrice: r.mall === 'rakuten' ? 1200 : null,
      cost: r.cost, taxRate: r.taxRate, shipping: r.shipping, feeRate: r.feeRate,
      initialState: r.manual ? 'manual_required' : 'previewed',
    })),
  });
  const run = getRun(db, runId);
  ok(!!run, 'run が保存される');
  eq(run.operations.length, chosen.length, '行数が一致');
  const manualOp = run.operations.find((o) => o.initial_state === 'manual_required');
  eq(manualOp.state, 'manual_required', '初期状態は initial_state');

  appendEvent(db, runId, { operationId: manualOp.operation_id, actor: 'tester@example.com', event: 'manual_done' });
  eq(getRun(db, runId).operations.find((o) => o.operation_id === manualOp.operation_id).state, 'manual_done',
    'イベント追記で状態が変わる');
  appendEvent(db, runId, { operationId: manualOp.operation_id, actor: 'tester@example.com', event: 'manual_required' });
  eq(getRun(db, runId).operations.find((o) => o.operation_id === manualOp.operation_id).state, 'manual_required',
    '取り消しも追記で表す (行の書き換えをしない)');

  const ops = db.prepare('SELECT COUNT(*) n FROM pu_operations WHERE run_id = ?').get(runId).n;
  eq(ops, chosen.length, '★pu_operations の行数は増減しない (append-only)');
  eq(db.prepare('SELECT COUNT(*) n FROM pu_events WHERE run_id = ?').get(runId).n, 3, 'イベントは run_created + 2件');
  ok(listRuns(db).some((r) => r.run_id === runId), '一覧に出る');

  // 状態イベント以外の event 名では状態が動かない (未知のイベントで状態を壊さない)
  appendEvent(db, runId, { operationId: manualOp.operation_id, actor: 't', event: 'note_added' });
  eq(currentStates(db, runId).get(manualOp.operation_id), 'manual_required', '未知のイベントは状態を変えない');

  // ★append-only は DB 側で強制されている (規約だけにしない)
  const throws = (fn, label) => {
    try { fn(); ok(false, label + ' — 通ってしまった'); } catch (e) { ok(/追記のみ|属していません/.test(e.message), `${label} — ${e.message.slice(0, 60)}`); }
  };
  throws(() => db.prepare('UPDATE pu_operations SET new_price = 1 WHERE run_id = ?').run(runId), 'pu_operations の UPDATE は拒否される');
  throws(() => db.prepare('DELETE FROM pu_operations WHERE run_id = ?').run(runId), 'pu_operations の DELETE は拒否される');
  throws(() => db.prepare('UPDATE pu_runs SET note = ? WHERE run_id = ?').run('改ざん', runId), 'pu_runs の UPDATE は拒否される');
  throws(() => db.prepare('DELETE FROM pu_events WHERE run_id = ?').run(runId), 'pu_events の DELETE は拒否される');
  throws(() => appendEvent(db, runId, { operationId: 'puo-not-in-this-run', actor: 't', event: 'manual_done' }),
    '他 run の operation にイベントは付けられない');
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
