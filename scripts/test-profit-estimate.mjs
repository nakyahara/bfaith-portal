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
 *   9 四捨五入は 0.5 を 0 から遠い方へ (−51.5 → −52 円・−3.75 → −3.8%)・CSV の利益率も画面と同じ丸め・低い粗利率の境目の警告は 2 桁 (Codex #1632 R3)・10% ちょうどは低いと言わない (R4)
 *   8 マスタの入力と価格改定の画面の円・% が同じ (Codex #1632 R2 M: 売価 1,367・原価 645・10%・送料 520・手数料 10% = 0.5 円が浮動小数で 0.4999… → 前は価格改定だけ 0 円)
 *   7 内訳の 1 行の等式が必ず成り立つ (税込原価が .5 になる 10%・小数の送料・8% の 2 桁) = 左の式の数 = 右の数・利益 = その四捨五入 (Codex #1632 R1 Low)
 * 使い方: node scripts/test-profit-estimate.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const PF = await import('../lib/profit-estimate.js');
const PU = await import('../apps/price-update/pricing.js');

let passed = 0;
async function t(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

await t('[1] 式: 売価 1,000・原価 100・税率 10%・送料 520 = 1000 − 100 − 110 − 520 = 270 円 (27.0%)・内訳の 1 行', () => {
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

await t('[2] 原価なし・売価なし = 計算しない (0 円として計算しない)・理由', () => {
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

await t('[3] 税率 8%・税率なし (10% として)・送料なし (0 円として) = 計算するが知らせる', () => {
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

await t('[4] マイナス (赤字) も計算する・見せ方は「−」', () => {
  const r = PF.masterProfit({ price: 1000, cost: 400, taxRate: 0.08, shipping: 520 });
  assert.equal(r.profit, 1000 - 100 - 432 - 520);
  assert.ok(r.profit < 0 && r.rate < 0);
  assert.equal(PF.fmtProfitYen(r.profit), '−52');
  assert.equal(PF.fmtProfitRate(r.rate), '−5.2%');
  assert.equal(PF.fmtProfitYen(12345), '12,345');
  assert.match(PF.profitLine(r), /= 利益 −52 円$/);
});

await t('[5] price-update と同じ関数・手数料の率は 1 か所', () => {
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

await t('[6] ブラウザにそのまま配るファイル = import / require を書いていない', () => {
  const src = fs.readFileSync(new URL('../lib/profit-estimate.js', import.meta.url), 'utf8');
  assert.ok(!/^\s*import\s/m.test(src) && !/\brequire\(/.test(src) && !/\bprocess\./.test(src), 'Node だけのものを使っていない');
});

await t('[7] 内訳の 1 行の等式が成り立つ: 税込原価 16.5 (原価 15 × 1.10)・小数の送料 210.4・8% の 35.64 / 総当たりで左の式 = 右の数・利益 = 四捨五入 (整数の手計算と同じ)', () => {
  // Codex の例: 前は「1,000 − 100 − 17 − 0 = 利益 884 円」(左辺は 883)
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 15, taxRate: 0.1, shipping: 0 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 16.5 (15 × 1.10) − 配送料 0 = 883.5 → 利益 884 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.08, shipping: 210.4 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 108 (100 × 1.08) − 配送料 210.4 = 581.6 → 利益 582 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 33, taxRate: 0.08, shipping: 0 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 35.64 (33 × 1.08) − 配送料 0 = 864.36 → 利益 864 円 (四捨五入)');
  assert.equal(PF.profitLine(PF.masterProfit({ price: 1000, cost: 100, taxRate: 0.1, shipping: 520 })), '売価 1,000 − 手数料 100 (10%) − 税込原価 110 (100 × 1.10) − 配送料 520 = 利益 270 円', '割り切れるときは → を出さない');
  const n = (x) => Number(x.replace(/,/g, '').replace('−', '-'));
  const half = (v) => Math.sign(v) * Math.round(Math.abs(v)) + 0;   // 四捨五入 (0.5 は 0 から遠い方へ)
  let checked = 0;
  let nNeg = 0;
  for (const price of [980, 1000, 1234, 2980, 500]) for (const cost of Array.from({ length: 400 }, (_, i) => i * 5 + 1).concat([15, 25, 45, 365, 3000])) for (const taxRate of [0.08, 0.1, null]) for (const shipping of [0, 210.4, 520, 99.5, null]) {
    const r = PF.masterProfit({ price, cost, taxRate, shipping });
    const line = PF.profitLine(r);
    const m = /^売価 ([\d,]+) − 手数料 ([\d,]+) \(10%\) − 税込原価 ([\d,.]+) \([\d,]+ × 1\.(?:08|10)\) − 配送料 ([\d,.]+) = (?:(−?[\d,.]+) → )?利益 (−?[\d,]+) 円(?: \(四捨五入\))?$/.exec(line);
    assert.ok(m, line);
    const left = Math.round((n(m[1]) - n(m[2]) - n(m[3]) - n(m[4])) * 100) / 100;
    const right = m[5] == null ? n(m[6]) : n(m[5]);
    assert.equal(left, right, `左の式 = 右の数: ${line}`);
    assert.equal(half(right), n(m[6]), `利益 = 四捨五入: ${line}`);
    // 整数の手計算 (銭まで): 売価・手数料は円・原価 × (100 + 税率%) は銭・送料は銭
    const sen = price * 100 - Math.round(price * 0.1) * 100 - cost * Math.round(100 + (taxRate ?? 0.1) * 100) - Math.round((shipping ?? 0) * 100);
    assert.equal(r.profit, half(sen / 100), `手計算と同じ: ${line}`);
    checked++;
    if (/= −[\d,]+\.5 → /.test(line)) nNeg++;
  }
  assert.ok(checked > 20000);
  assert.ok(nNeg > 50, `負の .5 の行を見た (${nNeg})`);
});

{
  // 価格改定の画面 (views/index.ejs の画面の JS) の粗利の欄を、本物の関数で描く (サーバーの evaluateRow の答えを JSON で渡すのと同じ)
  const tpl = fs.readFileSync(new URL('../apps/price-update/views/index.ejs', import.meta.url), 'utf8');
  const yenSrc = /^ {2}const yen = .*;$/m.exec(tpl)[0];
  const cellSrc = /^ {2}function grossCell\(r, ev\) \{[\s\S]*?^ {2}\}$/m.exec(tpl)[0];
  assert.ok(!/<%/.test(yenSrc + cellSrc), 'EJS のタグを含まない部分だけ');
  const ctx = vm.createContext({});
  vm.runInContext(`${yenSrc}\n${cellSrc}\nthis.grossCell = grossCell;`, ctx);
  const puCell = (row) => {
    const ev = JSON.parse(JSON.stringify(PU.evaluateRow({ mall: 'rakuten', confidence: 'confirmed', currentPrice: row.price, newPrice: row.price, cost: row.cost, taxRate: row.taxRate, shipping: row.shipping, feeRate: 0.1 })));
    const html = ctx.grossCell({ newPrice: row.price, shippingSource: 'known' }, ev);
    const m = /<div>(-?[\d,]+) 円<\/div><div class="pu-note">(-?[\d.]+)%<\/div>/.exec(html);
    assert.ok(m, html);
    return { yen: Number(m[1].replace(/,/g, '')), pct: Number(m[2]), ev };
  };
  const R = await import('../apps/master-edit/read.mjs');
  const meCell = (row) => {
    const r = R.rowProfit({ standard_price: row.price, cost: row.cost, tax_rate: row.taxRate, shipping_cost: row.shipping });   // 一覧・CSV・画面の利益
    return { yen: Number(PF.fmtProfitYen(r.profit).replace('−', '-').replace(/,/g, '')), pct: Number(PF.fmtProfitRate(r.rate).replace('−', '-').replace('%', '')), r };
  };
  await t('[8] マスタの入力と価格改定の画面の利益の円・% が同じ (Codex の再現値 1,367・645・10%・520 = 両方 1 円)・生の gross は変えない・総当たり', () => {
    const codex = { price: 1367, cost: 645, taxRate: 0.1, shipping: 520 };
    const raw = PF.estimateGross({ ...codex, feeRate: 0.1 }).gross;
    assert.ok(raw > 0.49 && raw < 0.5, `浮動小数では 0.5 に届かない (${raw})`);
    assert.equal(PF.grossYen(raw), 1, '銭まで丸めてから円 = 1 円');
    const pu = puCell(codex); const me = meCell(codex);
    assert.equal(pu.ev.estimate.gross, raw, '価格改定の生の gross はそのまま (判定はこれ)');
    assert.deepEqual([pu.yen, me.yen], [1, 1], '両方の画面が 1 円');
    assert.equal(pu.pct, me.pct);
    assert.equal(PF.masterProfit(codex).profit, 1);
    assert.equal(PF.grossYen(-0.4), 0); assert.ok(!Object.is(PF.grossYen(-0.4), -0), '−0 は 0');
    assert.equal(PF.grossYen(null), null); assert.equal(PF.ratePct1(null), null);
    assert.equal(PF.ratePct1(0.1235), 12.4); assert.ok(!Object.is(PF.ratePct1(-0.00001), -0));
    let n = 0;
    for (const price of [980, 1367, 1980, 2500]) for (let cost = 1; cost <= 1500; cost += 7) for (const taxRate of [0.08, 0.1]) for (const shipping of [0, 210.4, 520]) {
      const row = { price, cost, taxRate, shipping };
      const a = puCell(row); const b = meCell(row);
      assert.deepEqual([a.yen, a.pct], [b.yen, b.pct], JSON.stringify(row));
      n++;
    }
    assert.ok(n > 4000);
  });
  await t('[9] 四捨五入は 0.5 を 0 から遠い方へ (Codex #1632 R3 M): −51.5 → −52 円 (両方の画面・内訳)・−3.75% → −3.8% / CSV の利益率 = 画面 (R3 L) / 低い粗利率の境目の警告は 2 桁 (R3 L)', async () => {
    // 売価 500・原価 365・税率 10%・送料 100・手数料 10% = 500 − 50 − 401.5 − 100 = −51.5
    const neg = { price: 500, cost: 365, taxRate: 0.1, shipping: 100 };
    const m = PF.masterProfit(neg);
    assert.deepEqual([m.gross, m.profit], [-51.5, -52]);
    assert.equal(PF.profitLine(m), '売価 500 − 手数料 50 (10%) − 税込原価 401.5 (365 × 1.10) − 配送料 100 = −51.5 → 利益 −52 円 (四捨五入)');
    const pu = puCell(neg); const me = meCell(neg);
    assert.deepEqual([pu.yen, me.yen], [-52, -52], '両方の画面が −52 円');
    assert.equal(pu.pct, me.pct);
    assert.deepEqual([PF.grossYen(-0.5), PF.grossYen(0.5), PF.grossYen(-2.5), PF.roundHalf(-2.5), PF.roundHalf(2.5)], [-1, 1, -3, -3, 3]);
    assert.deepEqual([PF.ratePct1(-0.0375), PF.ratePct1(0.0375), PF.fmtProfitRate(-0.0375)], [-3.8, 3.8, '−3.8%']);
    assert.ok(!Object.is(PF.roundHalf(-0.4), -0), '−0 は 0');
    // CSV の利益率 = 画面 (売価 504・原価 21・税率 10%・送料 210.4 = 43.75% → 画面 43.8%・前の CSV は 43.7%)
    const { buildListCsv } = await import('../apps/master-edit/list-csv.mjs');
    const rows = [];
    const want = new Map();
    for (const price of [504, 980, 1367, 500]) for (let cost = 1; cost <= 600; cost += 4) for (const taxRate of [0.08, 0.1]) for (const shipping of [0, 210.4, 100]) {
      const r = R.rowProfit({ standard_price: price, cost, tax_rate: taxRate, shipping_cost: shipping });
      const code = `c${rows.length}`;
      rows.push({ code, kind: 'single', name: code, state: 'available', reg_state: 'none', flags: [], sales: null, standard_price: price, cost, tax_rate: taxRate,
        profit: r.ok ? r.profit : null, profit_rate: r.ok ? r.rate : null });
      want.set(code, [PF.fmtProfitYen(r.profit).replace('−', '-').replace(/,/g, ''), PF.fmtProfitRate(r.rate).replace('−', '-').replace('%', '')]);
    }
    rows.unshift({ code: 'codex', kind: 'single', name: 'codex', state: 'available', reg_state: 'none', flags: [], sales: null, ...(() => { const r = R.rowProfit({ standard_price: 504, cost: 21, tax_rate: 0.1, shipping_cost: 210.4 }); return { profit: r.profit, profit_rate: r.rate }; })() });
    want.set('codex', [String(PF.masterProfit({ price: 504, cost: 21, taxRate: 0.1, shipping: 210.4 }).profit), '43.8']);
    const csv = buildListCsv({ rows }, {}, { nowMs: Date.UTC(2030, 0, 10, 3) }).replace(/^\ufeff/, '').trimEnd().split('\r\n').map((line) => [...line.matchAll(/"((?:[^"]|"")*)"(?:,|$)/g)].map((x) => x[1]));
    const hi = csv[0].findIndex((h) => h.startsWith('利益 (1 個あたり')); const ri = csv[0].indexOf('利益率 (%)');
    let nEdge = 0;
    for (const line of csv.slice(1)) {
      assert.deepEqual([Number(line[hi]), Number(line[ri])], want.get(line[0]).map(Number), `CSV = 画面の数 ${line[0]} (${line[ri]} / ${want.get(line[0])[1]})`);
      if (/5$/.test(String(Math.round(Math.abs((rows.find((x) => x.code === line[0]).profit_rate ?? 0) * 1e5))))) nEdge++;
    }
    assert.equal(csv.find((x) => x[0] === 'codex')[ri], '43.8');
    assert.ok(nEdge > 10, `.x5 の境目の利益率を見た (${nEdge})`);
    // 低い粗利率の境目: 売価 500・原価 278・税率 8%・送料 100 = 生 9.952% (判定 = 低い・欄 = 10.0%) → 警告は 2 桁 9.95%
    const ev = (row) => PU.evaluateRow({ mall: 'rakuten', confidence: 'confirmed', currentPrice: row.price, newPrice: row.price, cost: row.cost, taxRate: row.taxRate, shipping: row.shipping, feeRate: 0.1 });
    const edge = ev({ price: 500, cost: 278, taxRate: 0.08, shipping: 100 });
    assert.ok(edge.estimate.rate < 0.1 && edge.estimate.ratePct === 10, '生は 10% 未満・欄は 10.0');
    assert.ok(edge.warns.includes('粗利率が低いです (概算 9.95%)'), JSON.stringify(edge.warns));
    // 10% ちょうど (売価 504・原価 276・税率 10%・送料 100 = 504 − 50 − 303.6 − 100 = 50.4 = 10%。浮動小数では 0.09999999999999995) = 低いと言わない (Codex #1632 R4 L)
    const just = ev({ price: 504, cost: 276, taxRate: 0.1, shipping: 100 });
    assert.ok(just.estimate.rate < 0.1, `生の率は 10% に届かない (${just.estimate.rate})`);
    assert.equal(just.estimate.ratePct, 10);
    assert.ok(!just.warns.some((w) => w.startsWith('粗利率が低い')), JSON.stringify(just.warns));
    // 10% ちょうどの総当たり (売価 × 10% = 利益 になる組): どれも低いと言わない
    let nJust = 0;
    for (let price = 300; price <= 3000; price++) for (const taxRate of [0.08, 0.1]) for (const shipping of [0, 100, 520]) {
      // 利益 = 売価の 10% ちょうどになる原価 (銭で解く): 売価 − 手数料 − 原価 × (1 + 税率) − 送料 = 売価 × 10%
      const k = Math.round(100 + taxRate * 100);
      const left = price * 100 - Math.round(price * 0.1) * 100 - shipping * 100 - price * 10;
      if (left <= 0 || left % k !== 0) continue;
      const cost = left / k;
      nJust++;
      assert.ok(!ev({ price, cost, taxRate, shipping }).warns.some((w) => w.startsWith('粗利率が低い')), `10% ちょうど ${price}・${cost}・${taxRate}・${shipping}`);
    }
    assert.ok(nJust >= 5, `10% ちょうどの組を見た (${nJust})`);
    const low = ev({ price: 1000, cost: 300, taxRate: 0.1, shipping: 520 });   // 1000 − 100 − 330 − 520 = 50 = 5.0%
    assert.ok(low.warns.includes('粗利率が低いです (概算 5.0%)'), '境目でなければ 1 桁');
    const fine = ev({ price: 1000, cost: 100, taxRate: 0.1, shipping: 520 });   // 27%
    assert.ok(!fine.warns.some((w) => w.startsWith('粗利率が低い')), '判定は今までどおり (10% 以上は出さない)');
    // 境目の総当たり: 判定が低い行の警告の数は必ず 10 未満
    for (let cost = 250; cost <= 300; cost++) for (const price of [480, 500, 520]) {
      const e = ev({ price, cost, taxRate: 0.08, shipping: 100 });
      const w = e.warns.find((x) => x.startsWith('粗利率が低いです'));
      assert.equal(!!w, PF.roundHalf(e.estimate.rate, 12) < 0.1, '判定 = 率 (12 桁に丸めた) < 10%');
      if (w) assert.ok(Number(/概算 (-?[\d.]+)%/.exec(w)[1]) < 10, `低いと言うときの数は 10 未満: ${w}`);
      if (w) assert.ok(!/10\.00?%/.test(w), w);
    }
  });
}

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
