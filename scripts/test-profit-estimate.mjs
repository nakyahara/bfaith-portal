/**
 * test-profit-estimate.mjs — 1 個あたりの利益の計算 (lib/profit-estimate.js) の単体の試験 (10/6 中原さん「利益の計算も入れてほしい」)
 *
 * 確かめること:
 *   1 式 = 売価 − round(売価 × 10%) − 原価 × (1 + 税率) − 配送料 (手で計算した数と同じ)・利益率 = 利益 / 売価
 *   2 原価なし・売価なし (空・0 円) = 計算しない (0 円として計算しない)・理由の文
 *   3 税率 8% / 税率なし (10% として・知らせ)・送料なし (0 円として・知らせ)
 *   4 マイナス (赤字) も計算する・見せ方は「−」
 *   5 price-update の estimateGross は同じ関数 (export し直し)・手数料の率は 1 か所 (PLATFORM_FEE_RATES)
 *   6 import の無いファイル (ブラウザにそのまま配る) = import / require を書いていない
 *   7 内訳の 1 行の等式が必ず成り立つ (税込原価が .5 になる 10%・小数の送料・8% の 2 桁) = 左の式の数 = 右の数・利益 = その四捨五入 (Codex #1632 R1 Low)
 * 使い方: node scripts/test-profit-estimate.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';

const PF = await import('../lib/profit-estimate.js');
const PU = await import('../apps/price-update/pricing.js');

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

t('[1] 式: 売価 1,000・原価 100・税率 10%・送料 520 = 1000 − 100 − 110 − 520 = 270 円 (27.0%)・内訳の 1 行', () => {
  const r = PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.1, shipping: 520 });
  assert.equal(r.ok, true);
  assert.equal(r.profit, 270);
  assert.equal(r.fee, 100);
  assert.equal(r.costInclTax, 110);
  assert.ok(Math.abs(r.rate - 0.27) < 1e-9);
  assert.deepEqual(r.notes, []);
  assert.equal(PF.fmtProfitRate(r.rate), '27.0%');
  assert.equal(PF.profitLine(r), '売価 1,000 − 手数料 100 (10%) − 税込原価 110 (100 × 1.10) − 配送料 520 = 利益 270 円');
  // 手数料は 1 円未満を四捨五入 (price-update と同じ)・利益は円で四捨五入
  const r2 = PF.masterProfit({ price: 1234, cost: 333, taxRate: 0.1, shipping: 210.4 });
  assert.equal(r2.fee, 123);
  assert.equal(r2.profit, Math.round(1234 - 123 - 333 * 1.1 - 210.4));
  // 文字の数 (DB の numeric の文字・画面の欄) も読む
  assert.equal(PF.masterProfit({ price: '1000', cost: '100', taxRate: '0.1', shipping: '520' }).profit, 270);
});

t('[2] 原価なし・売価なし = 計算しない (0 円として計算しない)・理由', () => {
  const noCost = PF.masterProfit({ price: 1000, cost: null, taxRate: 0.1, shipping: 520 });
  assert.equal(noCost.ok, false);
  assert.deepEqual(noCost.missing, ['cost']);
  assert.equal(noCost.reason, '原価が未登録なので計算できません');
  assert.equal(noCost.profit, undefined, '利益の数を返さない');
  for (const blank of [undefined, '']) assert.equal(PF.masterProfit({ price: 1000, cost: blank, taxRate: 0.1, shipping: 520 }).ok, false, `原価 ${JSON.stringify(blank)}`);
  // 原価 0 円は「入っている」(上書きの 0 円) = 計算する
  assert.equal(PF.masterProfit({ price: 1000, cost: 0, taxRate: 0.1, shipping: 520 }).profit, 380);
  const noPrice = PF.masterProfit({ price: null, cost: 100, taxRate: 0.1, shipping: 520 });
  assert.deepEqual([noPrice.ok, noPrice.missing, noPrice.reason], [false, ['price'], '標準売価が未入力なので計算できません']);
  assert.equal(PF.masterProfit({ price: 0, cost: 100, taxRate: 0.1, shipping: 520 }).ok, false, '売価 0 円 = 利益率が出せない = 計算しない');
  const both = PF.masterProfit({ price: null, cost: null, taxRate: 0.1, shipping: 520 });
  assert.equal(both.reason, '標準売価が未入力・原価が未登録なので計算できません');
  assert.equal(PF.profitLine(noCost), '');
  // 元の関数も原価なし = gross null (price-update の今までの契約)
  assert.equal(PF.estimateGross({ price: 1000, cost: null, taxRate: 0.1, feeRate: 0.1, shipping: 0 }).gross, null);
});

t('[3] 税率 8%・税率なし (10% として)・送料なし (0 円として) = 計算するが知らせる', () => {
  const r8 = PF.masterProfit({ price: 1000, cost: 200, taxRate: 0.08, shipping: 520 });
  assert.equal(r8.profit, 1000 - 100 - 216 - 520);
  assert.equal(r8.costInclTax, 216);
  assert.match(PF.profitLine(r8), /税込原価 216 \(200 × 1\.08\)/);
  const noTax = PF.masterProfit({ price: 1000, cost: 200, taxRate: null, shipping: 520 });
  assert.equal(noTax.profit, 1000 - 100 - 220 - 520);
  assert.deepEqual(noTax.notes, ['tax_default']);
  assert.deepEqual(PF.profitNoteWords(noTax.notes), ['税率が未入力なので 10% として計算']);
  const noShip = PF.masterProfit({ price: 1000, cost: 200, taxRate: 0.1, shipping: null });
  assert.equal(noShip.profit, 1000 - 100 - 220);
  assert.deepEqual(noShip.notes, ['shipping_zero']);
  assert.match(PF.profitNoteWords(noShip.notes)[0], /送料が未入力なので配送料 0 円/);
});

t('[4] マイナス (赤字) も計算する・見せ方は「−」', () => {
  const r = PF.masterProfit({ price: 1000, cost: 400, taxRate: 0.08, shipping: 520 });
  assert.equal(r.profit, 1000 - 100 - 432 - 520);
  assert.ok(r.profit < 0 && r.rate < 0);
  assert.equal(PF.fmtProfitYen(r.profit), '−52');
  assert.equal(PF.fmtProfitRate(r.rate), '−5.2%');
  assert.equal(PF.fmtProfitYen(12345), '12,345');
  assert.match(PF.profitLine(r), /= 利益 −52 円$/);
});

t('[5] price-update と同じ関数・手数料の率は 1 か所', () => {
  assert.equal(PU.estimateGross, PF.estimateGross, 'pricing.js の estimateGross = lib の関数そのもの');
  assert.equal(PU.DEFAULT_TAX_RATE, PF.DEFAULT_TAX_RATE);
  assert.equal(PF.PLATFORM_FEE_RATES.standard, 0.10);
  assert.ok(Object.isFrozen(PF.PLATFORM_FEE_RATES));
  // masterProfit の中身は estimateGross (率を渡すと同じ数)
  const e = PF.estimateGross({ price: 1980, cost: 700, taxRate: 0.08, feeRate: 0.1, shipping: 210 });
  const m = PF.masterProfit({ price: 1980, cost: 700, taxRate: 0.08, shipping: 210 });
  assert.equal(m.profit, Math.round(e.gross)); assert.equal(m.fee, e.fee); assert.equal(m.rate, e.rate);
  // 率を変えれば変わる (モールごとに変える日の入口)
  assert.equal(PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.1, shipping: 0, feeRate: 0.15 }).fee, 150);
});

t('[6] ブラウザにそのまま配るファイル = import / require を書いていない', () => {
  const src = fs.readFileSync(new URL('../lib/profit-estimate.js', import.meta.url), 'utf8');
  assert.ok(!/^\s*import\s/m.test(src) && !/\brequire\(/.test(src) && !/\bprocess\./.test(src), 'Node だけのものを使っていない');
});

t('[7] 内訳の 1 行の等式が成り立つ: 税込原価 16.5 (原価 15 × 1.10)・小数の送料 210.4・8% の 35.64 / 総当たりで左の式 = 右の数・利益 = 四捨五入 (整数の手計算と同じ)', () => {
  // Codex の例: 前は「1,000 − 100 − 17 − 0 = 利益 884 円」(左辺は 883)
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 15, taxRate: 0.1, shipping: 0 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 16.5 (15 × 1.10) − 配送料 0 = 883.5 → 利益 884 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.08, shipping: 210.4 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 108 (100 × 1.08) − 配送料 210.4 = 581.6 → 利益 582 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 33, taxRate: 0.08, shipping: 0 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 35.64 (33 × 1.08) − 配送料 0 = 864.36 → 利益 864 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.1, shipping: 520 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 110 (100 × 1.10) − 配送料 520 = 利益 270 円', '割り切れるときは → を出さない');
  const n = (x) => Number(x.replace(/,/g, '').replace('−', '-'));
  let checked = 0;
  for (const price of [980, 1000, 1234, 2980]) for (const cost of Array.from({ length: 400 }, (_, i) => i * 5 + 1).concat([15, 25, 45, 3000])) for (const taxRate of [0.08, 0.1, null]) for (const shipping of [0, 210.4, 520, 99.5, null]) {
    const r = PF.masterProfit({ price, cost, taxRate, shipping });
    const line = PF.profitLine(r);
    const m = /^売価 ([\d,]+) − 手数料 ([\d,]+) \(10%\) − 税込原価 ([\d,.]+) \([\d,]+ × 1\.(?:08|10)\) − 配送料 ([\d,.]+) = (?:(−?[\d,.]+) → )?利益 (−?[\d,]+) 円(?: \(四捨五入\))?$/.exec(line);
    assert.ok(m, line);
    const left = Math.round((n(m[1]) - n(m[2]) - n(m[3]) - n(m[4])) * 100) / 100;
    const right = m[5] == null ? n(m[6]) : n(m[5]);
    assert.equal(left, right, `左の式 = 右の数: ${line}`);
    assert.equal(Math.round(right) + 0, n(m[6]), `利益 = 四捨五入: ${line}`);
    // 整数の手計算 (銭まで): 売価・手数料は円・原価 × (100 + 税率%) は銭・送料は銭
    const sen = price * 100 - Math.round(price * 0.1) * 100 - cost * Math.round(100 + (taxRate ?? 0.1) * 100) - Math.round((shipping ?? 0) * 100);
    assert.equal(r.profit, Math.round(sen / 100) + 0, `手計算と同じ: ${line}`);
    checked++;
  }
  assert.ok(checked > 20000);
});

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
