/**
 * test-view.mjs — 画面の描画テスト
 *
 * 🚨 memory の教訓 (feedback_screen_test_green_but_verifies_nothing):
 *    EJS を描画するだけのテストは「router が値を渡し忘れている」のを検知しない。
 *    ここでは「描画できること」に加えて、**タブが実際に出ること**と
 *    **XSS 対策 (inline onclick を使っていないこと)** を固定する。
 *
 * 実行: node apps/expected-profit/test-view.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import ejs from 'ejs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const VIEW = path.join(__dirname, '../../views/profit-analysis.ejs');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const render = (locals) => ejs.render(fs.readFileSync(VIEW, 'utf8'), locals, { filename: VIEW });

// router.js の res.render と同じ locals を使う
// (ここがズレると「描画は通るが本番で落ちる」テストになる)
const baseLocals = {
  title: '商品収益性ダッシュボード',
  username: 'test@example.com',
  displayName: 'テスト',
  featureFlagEnabled: false,
};

console.log('描画');

let html;
t('EJS が描画できる', () => {
  html = render(baseLocals);
  assert.ok(html.length > 1000);
});

t('[!] 「想定利益 (単品)」タブが出る', () => {
  assert.ok(html.includes('想定利益 (単品)'), 'サブタブが無い');
  assert.ok(html.includes("data-view=\"expected\""), 'data-view が無い');
});

t('[!] 前提 (含んでいない費用) が画面に書かれている', () => {
  // 「これだけ手元に残る」と誤解させないための表示 (§2 / §9.1)
  assert.ok(html.includes('広告費'), '未計上費目の説明が無い');
  assert.ok(html.includes('ポイント原資'));
  assert.ok(html.includes('長期保管手数料'), 'FBA の未計上費目が無い');
});

t('[!] Amazon の手数料が「見積」だと書かれている', () => {
  assert.ok(html.includes('見積'), '見積である旨が無い');
  assert.ok(html.includes('確定請求額ではありません'));
});

t('[!] FBA と自社出荷を混ぜない旨が書かれている', () => {
  assert.ok(html.includes('同じ順位表に混ぜていません'));
});

t('[!] 標準シナリオが書かれている', () => {
  assert.ok(html.includes('1注文・1個・同梱なし'));
});

console.log('\nXSS 対策 (§9.5)');

t('[!] 想定利益タブのイベントを inline onclick で書いていない', () => {
  // 品番に ' が含まれると inline onclick は壊れる。addEventListener で結ぶこと
  const section = html.slice(html.indexOf('function renderExpectedProfit'), html.indexOf('function bindExpectedProfitEvents'));
  assert.ok(!/onclick="/.test(section), 'renderExpectedProfit が inline onclick を出している');
  assert.ok(!/onchange="/.test(section), 'renderExpectedProfit が inline onchange を出している');
});

t('[!] 動的な文字列を escapeHtml に通している', () => {
  const section = html.slice(html.indexOf('function renderExpectedProfit'), html.indexOf('function bindExpectedProfitEvents'));
  // 商品名・出品コードは必ず escape する
  assert.ok(section.includes('escapeHtml(r.mall_item_key)'));
  assert.ok(section.includes("escapeHtml(r.product_name || '')"));
});

t('addEventListener で結んでいる', () => {
  assert.ok(html.includes("addEventListener('change'"));
  assert.ok(html.includes("addEventListener('click'"));
});

console.log('\nタブBのフラグ (既存の Dark Launch を壊していない)');

t('flag OFF ではタブB が出ない', () => {
  const off = render({ ...baseLocals, featureFlagEnabled: false });
  assert.ok(!off.includes('在庫整理・撤退判断支援'), 'flag OFF なのにタブB が出ている');
});

t('flag ON ではタブB が出て、想定利益タブも共存する', () => {
  const on = render({ ...baseLocals, featureFlagEnabled: true });
  assert.ok(on.includes('在庫整理・撤退判断支援'));
  assert.ok(on.includes('想定利益 (単品)'));
});


console.log('');
console.log('経費の内訳の表 (中原さん 2026-09-08「すべての経費と配送方法をちゃんと表示」)');

// 🚨 画面の表はブラウザ側の JS が文字列で組み立てている。EJS を描くだけでは中身を見られない。
//    想定利益タブの JS を丸ごと切り出して**実際に動かし**、列の数と colspan が合っているか、
//    どの配送方法を使ったかが出ているかを確かめる
//    (colspan の数え間違いは目で見ないと分からず、表が1列ずれる)
function makeScreen() {
  const start = html.indexOf('// ─── 想定利益 (単品販売シナリオ) ───');
  const end = html.indexOf('async function loadData()');
  assert.ok(start > 0 && end > start, '画面から想定利益タブの JS を切り出せない');
  const src = html.slice(start, end);
  const container = { innerHTML: '' };
  const sandbox = {
    escapeHtml: (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
    MALL_FEE_RATES_LABEL: { amazon: 'Amazon', rakuten: '楽天' },
    fetchJson: async () => ({}),
    document: {
      getElementById: (id) => (id === 'table-container' ? container : { value: '', addEventListener() {} }),
      querySelector: () => null,
    },
  };
  const api = new Function(...Object.keys(sandbox),
    src + '; return { render: renderExpectedProfit, detail: epDetailHtml, cols: EP_COLS, setState: (o) => Object.assign(epState, o) };'
  )(...Object.values(sandbox));
  return { api, container };
}

const PUBLISHED = {
  built_at: '2026-09-08T00:00:00Z', published_at: '2026-09-08T07:34:00Z',
  generation_id: 'g1', seq: 23, malls_included: ['amazon', 'rakuten'], malls_degraded: [],
};

function renderTable(rows, scope) {
  const { api, container } = makeScreen();
  api.setState({
    rows, total: rows.length, scope, published: PUBLISHED,
    summary: { total: rows.length, ok: rows.length, rankEligible: rows.length, expiredNow: 0 },
  });
  api.render();
  return container.innerHTML;
}

const cells = (tr) => (tr.match(/<t[dh][^>]*>/g) || []).map(tag => {
  const m = tag.match(/colspan="(\d+)"/);
  return m ? Number(m[1]) : 1;
}).reduce((a, b) => a + b, 0);

const sampleRow = (over = {}) => ({
  mall: 'amazon', mall_item_key: 'sku1', product_name: '商品', fulfillment: 'FBM',
  price_ex_tax: 900, price_incl_tax: 990, postage_revenue_ex_tax: 0,
  cost_ex_tax: 300, unit_quantity: 1,
  shipping_method: 'ネコポス', shipping_code: '501',
  shipping_rate_name: 'ネコポス', shipping_rate_category: 'メール便', shipping_group: null,
  shipping_fee_ex_tax: 180, shipping_work_ex_tax: 20,
  shipping_material_ex_tax: 10, shipping_labor_ex_tax: 9, shipping_total_ex_tax: 219,
  fba_fee_ex_tax: null, referral_fee_ex_tax: 89, closing_fee_ex_tax: 0,
  per_item_fee_ex_tax: 0, fee_total_ex_tax: 89, fee_rate_display: 0.1,
  expected_profit: 292, expected_margin_rate: 0.324, shipping_revenue_status: 'included',
  cost_method: 'single', ne_code: 'ne001',
  rank_eligible_now: 1, rank_exclusion_reason_now: null, incomplete_reason: null,
  ...over,
});

const dataRows = (out) => (out.match(/<tr[^>]*>[\s\S]*?<\/tr>/g) || []);

t('[!] 自社配送の行の列数が見出しと一致する (colspan の数え間違いを防ぐ)', () => {
  const { api } = makeScreen();
  const out = renderTable([sampleRow()], 'self_v1');
  const trs = dataRows(out);
  assert.ok(trs.length >= 3, `見出し2行 + データ1行のはず (実際 ${trs.length})`);
  const group = cells(trs[0]);
  const head = cells(trs[1]);
  const body = cells(trs[2]);
  assert.equal(head, api.cols.length, `見出しの数が列定義と合わない (${head} vs ${api.cols.length})`);
  assert.equal(group, head, `まとまりの見出しが合わない (${group} vs ${head})`);
  assert.equal(body, head, `データ行が合わない (${body} vs ${head})`);
});

t('[!] FBA の行も列数が一致する (Amazon が配送する行は colspan でまとめている)', () => {
  const out = renderTable([sampleRow({
    fulfillment: 'FBA', shipping_method: null, shipping_code: null,
    shipping_rate_name: null, shipping_rate_category: null,
    shipping_fee_ex_tax: null, shipping_work_ex_tax: null,
    shipping_material_ex_tax: null, shipping_labor_ex_tax: null, shipping_total_ex_tax: null,
    fba_fee_ex_tax: 462,
  })], 'fba_v1');
  const trs = dataRows(out);
  assert.equal(cells(trs[2]), cells(trs[1]), 'FBA 行の列数が見出しと合わない');
});

t('[!] どの配送方法を使ったかが表に出る (送料マスタの区分名 + コード)', () => {
  const out = renderTable([sampleRow({ shipping_rate_name: '宅急便 60サイズ', shipping_code: '702' })], 'self_v1');
  assert.ok(out.includes('宅急便 60サイズ'), '使った配送区分の名前が出ていない');
  assert.ok(out.includes('>702<'), '送料区分コードが出ていない');
});

t('[!] 区分名が無いときだけ NE 登録の配送方法に落とす (空欄にしない)', () => {
  const out = renderTable([sampleRow({ shipping_rate_name: null, shipping_method: 'ゆうパケット' })], 'self_v1');
  assert.ok(out.includes('ゆうパケット'), 'NE 登録の配送方法にも落ちていない');
});

t('[!] 結果 (想定利益・利益率) は右に貼り付ける — 横スクロールで答えが消えない', () => {
  // 24列を横に並べた結果、いちばん見たい利益率が画面の外に出ていた (2026-09-08 作り直しの発端)
  const out = renderTable([sampleRow()], 'self_v1');
  assert.ok(out.includes('ep-stick-r2'), '想定利益が貼り付けられていない');
  assert.ok(out.includes('ep-stick-r'), '想定利益率が貼り付けられていない');
  assert.ok(out.includes('ep-stick-l'), '商品名が貼り付けられていない');
});

t('[!] 経費の内訳が1つずつ出る (合計だけにしない)', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  for (const [name, v] of [['送料', '180'], ['出荷作業料', '20'], ['梱包資材費', '10'], ['人件費', '9'],
    ['販売手数料', '89']]) {
    assert.ok(out.includes('>' + v + '<'), `${name} (${v}) が出ていない`);
  }
});

t('見出しに経費の名前が全部ある', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  for (const h of ['使った配送', '送料', '出荷作業料', '梱包資材費', '人件費',
    'FBA配送代行', '販売手数料', '成約料', '基本成約料', '手数料 合計', '想定利益', '想定利益率']) {
    assert.ok(out.includes('>' + h + '<'), `見出し「${h}」が無い`);
  }
});

t('[!] 値が無いことと 0 円を見分けられる', () => {
  const out = renderTable([sampleRow({ closing_fee_ex_tax: 0, per_item_fee_ex_tax: null })], 'self_v1');
  assert.ok(out.includes('—'), '値が無い欄に — が出ていない');
});

console.log('');
console.log('1行ぶんの内訳 (行を押すと開く)');

t('[!] 「この計算で使った配送」を言葉で書く (中原さん 2026-09-08)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({ shipping_rate_name: 'ネコポス', shipping_code: '501', shipping_rate_category: 'メール便' }));
  assert.ok(out.includes('この計算で使った配送'));
  assert.ok(out.includes('ネコポス'));
  assert.ok(out.includes('送料コード 501'));
  assert.ok(out.includes('メール便'));
});

t('[!] モール側の配送パターンも出す (送料込み判断の根拠 §16-13)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({ shipping_group: 'ネコポスマケプレプライム設定' }));
  assert.ok(out.includes('ネコポスマケプレプライム設定'));
});

t('[!] FBA は「Amazon が配送」と書き、自社の送料を出さない', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({
    fulfillment: 'FBA', shipping_rate_name: null, shipping_total_ex_tax: null, fba_fee_ex_tax: 462,
  }));
  assert.ok(out.includes('Amazon が配送'));
  assert.ok(!out.includes('配送関係費 合計'), 'FBA なのに自社の配送費を出している');
});

t('[!] 送料区分が未登録なら、そう書く (勝手に埋めない)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({ shipping_rate_name: null, shipping_method: null, shipping_code: null }));
  assert.ok(out.includes('送料区分が未登録'), '未登録であることが書かれていない');
});

t('[!] 内訳の列数も表と合っている (colspan)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow());
  assert.ok(out.includes('colspan="' + api.cols.length + '"'), '内訳の colspan が列数と合わない');
});

console.log('');
console.log('ヘッダ (2026-09-08: style.css に無いクラスを使っていて崩れていた)');

t('[!] ポータル共通のヘッダを使っている', () => {
  assert.ok(html.includes('class="portal-header"'), 'portal-header を使っていない');
  assert.ok(!html.includes('class="top-nav"'), 'style.css に定義の無い top-nav が残っている');
});

t('[!] 想定利益タブでは実績タブの操作と数字を隠す', () => {
  // 実績の KPI (売上合計など) が残ると、想定利益の集計だと誤読される
  assert.ok(html.includes("document.getElementById('pd-filters-actual')"));
  assert.ok(html.includes("document.getElementById('summary-cards')"));
  assert.ok(html.includes("document.getElementById('pd-tab-a-actions')"));
});

console.log('');
console.log('タブ間の混線 (Codex 2巡目)');

t('[!] 想定利益タブは実績タブの絞り込み (#filter-mall) に書き込まない', () => {
  // 書き戻していたので、想定利益でモールを変えると実績タブの絞り込みまで黙って変わっていた
  const block = html.slice(html.indexOf('// ─── 想定利益 (単品販売シナリオ) ───'), html.indexOf('async function loadData()'));
  assert.ok(!/getElementById\('filter-mall'\)\.value\s*=/.test(block), '#filter-mall へ書き戻している');
  assert.ok(block.includes('epState.mall'), '想定利益タブが自分のモール状態を持っていない');
});

t('[!] 読み込み中にタブを切り替えたら、遅れて返った応答で上書きしない', () => {
  for (const fn of ['loadExpectedProfit', 'loadData', 'loadTrendData']) {
    const i = html.indexOf('function ' + fn + '(');
    assert.ok(i > 0, fn + ' が無い');
    // 🚨 関数の切れ目で止める。固定長で切ると隣の関数のガードを拾ってしまい、
    //    ガードを外しても PASS する試験になる (最初に書いたときそうなっていた)
    const rest = html.slice(i + 10);
    const nextFn = rest.search(/\n {4}(async )?function /);
    const body = nextFn > 0 ? rest.slice(0, nextFn) : rest;
    // 応答を反映する経路の数だけガードが要る (成功と失敗の両方)
    const guards = (body.match(/stillMine\(/g) || []).length;
    assert.ok(guards >= 2, fn + ' の取り違え防止のガードが足りない (' + guards + ')');
  }
});

t('[!] 貼り付けた結果2列は、幅とオフセットを同じ変数から出す', () => {
  // right: 104px / min-width: 104px と別々に書くと、中身が広がったとき左の列が右の列に重なる
  assert.ok(html.includes('--ep-r-col'), '幅の変数が無い');
  assert.ok(/right:\s*var\(--ep-r-col\)/.test(html), 'オフセットが変数から出ていない');
  assert.ok(/max-width:\s*var\(--ep-r-col\)/.test(html), '幅が固定されていない');
});

t('[!] 取り違えの確認は、共有している行データを書き換える前に行う', () => {
  // currentData は表の描画と CSV が共有している。先に入れ替えると、別タブを見ているのに
  // 並び替えた瞬間そこへ古い行が出る (Codex 3巡目)
  const i = html.indexOf('async function loadData(');
  const rest = html.slice(i + 10);
  const nextFn = rest.search(/\n {4}(async )?function /);
  const body = nextFn > 0 ? rest.slice(0, nextFn) : rest;
  const guard = body.indexOf('stillMine(');
  const mutate = body.indexOf('currentData =');
  assert.ok(guard > 0 && mutate > 0, 'loadData の中身が読めない');
  assert.ok(guard < mutate, 'currentData をガードより先に書き換えている');
});

console.log(`\n${passed} 件 PASS`);
