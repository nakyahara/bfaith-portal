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
import vm from 'node:vm';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import ejs from 'ejs';
// 🚨 状態の一覧は正本 (easyship-rates.js) から取る。ここに写すと足し忘れを検出できない
import { EASYSHIP_STATUSES } from './easyship-rates.js';
// 🚨 取扱中を表す値も正本 (query.js) から取る。画面に文字列を写しているので、
//    片方だけ変えると「取扱中なのに止まって見える」行ができる
import { HANDLING_ACTIVE } from './query.js';
// 「売価を変えて試算」の式は商品ハブの正本 (profit.js) と突き合わせる。画面に写した式だけを見ても、ずれに気づけない
import { computeProfit as phComputeProfit, TAKE_RATE as PH_TAKE_RATE } from '../product-hub/lib/profit.js';
// 個数を持つモールの正本。画面の EP_QTY_MALLS と突き合わせる (片方だけ変えると個数不明の出品を 1 個で試算する)
import { skuMapHasQuantity } from './load-inputs.js';
// 送料の税率の正本。画面は税抜で保存された送料を税込に戻して表示するので、率がずれると嘘の税込が出る
import { SERVICE_TAX_RATE, shippingCostExTax } from './calc.js';

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
  assert.ok(html.includes('自社出荷と同じ順位表には混ぜません'));
});

t('[!] 標準シナリオが書かれている', () => {
  assert.ok(html.includes('1 注文・1 個・同梱なし'));
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
  // 商品名・出品コードは必ず escape する。出品コードは押すとコピー (epItemKeyHtml の中で escape する)
  assert.ok(section.includes('epItemKeyHtml(r)'), 'テープ行 / 24 列の表が出品コードをコピー用にしていない');
  assert.ok(!section.includes('escapeHtml(r.mall_item_key)'), '押してもコピーできない素の出品コードが残っている');
  assert.ok(section.includes("escapeHtml(r.product_name || '')"));
});

console.log('\n商品コードを押すとコピー (2026-09-14 中原さん指示)');

/**
 * 画面の <script> から helper を切り出して実際に動かす。
 * 🚨 文字列が含まれるかだけを見ても、capture で止めていない (= コードを押すと行が開閉する) のは検知できない
 */
//   clipboard: 'ok' = 書ける / 'reject' = 拒否される / 'none' = API が無い (http で開いた等)
//   execOk:    予備経路 (execCommand) が成功するか
//   selection: window.getSelection() が返すもの (null = 何も選んでいない)
function loadCopyHelpers({ clipboard = 'ok', execOk = true, selection = null } = {}) {
  const start = html.indexOf('const HTML_ESCAPE_MAP');
  const end = html.indexOf('function formatNumber');
  assert.ok(start > 0 && end > start, 'helper の位置が見つからない');
  const listeners = [];
  const written = [];
  const execCalls = [];
  const toast = { hidden: true, textContent: '', ng: false, setAttribute() {} };
  toast.classList = { toggle: (c, on) => { if (c === 'ng') toast.ng = !!on; } };
  const navigator = clipboard === 'none' ? {} : {
    clipboard: {
      writeText: (s) => {
        written.push(s);
        return clipboard === 'reject' ? Promise.reject(new Error('denied')) : Promise.resolve();
      },
    },
  };
  const ctx = {
    document: {
      addEventListener: (type, fn, capture) => listeners.push({ type, fn, capture }),
      getElementById: () => toast,
      createElement: () => ({ value: '', style: {}, setAttribute() {}, select() {}, remove() {} }),
      body: { appendChild() {} },
      execCommand: (cmd) => { execCalls.push(cmd); return execOk; },
    },
    window: { isSecureContext: true, getSelection: () => selection },
    navigator,
    setTimeout: () => 0,
    clearTimeout: () => {},
  };
  vm.createContext(ctx);
  vm.runInContext(html.slice(start, end), ctx);
  return { ctx, listeners, written, execCalls, toast };
}

/** 画面のクリック受け (capture) にコードの押下を渡す。止めたかどうかを返す */
function clickCode(h, code) {
  const click = h.listeners.find(l => l.type === 'click');
  const el = { dataset: { copy: code } };
  let stopped = false;
  click.fn({ target: { closest: sel => (sel === '.copy-code' ? el : null) },
    stopPropagation: () => { stopped = true; }, preventDefault: () => {} });
  return { el, stopped };
}
const settle = () => new Promise(r => setTimeout(r, 0));

t('[!] 商品コードを押すとコピーされ、その行は開閉しない', () => {
  const { listeners, written } = loadCopyHelpers();
  const click = listeners.find(l => l.type === 'click');
  assert.ok(click, 'クリックを受けていない');
  assert.equal(click.capture, true, 'capture で受けないと、先に行の開閉が走る');
  let stopped = false;
  const el = { dataset: { copy: 'oscare3' } };
  click.fn({ target: { closest: sel => (sel === '.copy-code' ? el : null) },
    stopPropagation: () => { stopped = true; }, preventDefault: () => {} });
  assert.ok(stopped, 'コードを押したのが行の開閉まで伝わってしまう');
  assert.deepEqual(written, ['oscare3']);
  // コード以外を押したときは止めない (行を開く操作を奪わない)
  let stopped2 = false;
  click.fn({ target: { closest: () => null }, stopPropagation: () => { stopped2 = true; }, preventDefault: () => {} });
  assert.ok(!stopped2, 'コード以外の押下まで止めている');
  assert.equal(written.length, 1);
});

t('[!] 楽天の出品コードは 商品管理番号 と SKU管理番号 を別々にコピーできる', () => {
  const { ctx } = loadCopyHelpers();
  const rk = ctx.epItemKeyHtml({ mall: 'rakuten', mall_item_key: 'oscare3/oscare2' });
  assert.ok(rk.includes('data-copy="oscare3"') && rk.includes('data-copy="oscare2"'), rk);
  // Yahoo も「商品コード/SubCode」なので分ける (2026-09-19)
  const yh = ctx.epItemKeyHtml({ mall: 'yahoo', mall_item_key: 'footraku/footraku-LB-M' });
  assert.ok(yh.includes('data-copy="footraku"') && yh.includes('data-copy="footraku-LB-M"'), yh);
  // SubCode を持たない Yahoo の出品はそのまま 1 つ
  const yh1 = ctx.epItemKeyHtml({ mall: 'yahoo', mall_item_key: 'ae-amber50' });
  assert.equal((yh1.match(/data-copy=/g) || []).length, 1, yh1);
  // Amazon の出品者 SKU は / を含みうるので分けない
  const az = ctx.epItemKeyHtml({ mall: 'amazon', mall_item_key: 'ab/cd' });
  assert.equal((az.match(/data-copy=/g) || []).length, 1, az);
  assert.ok(az.includes('data-copy="ab/cd"'), az);
  // 値は属性にも本文にも escape して入れる
  const x = ctx.copyCodeHtml('a"<b>\'');
  assert.ok(!x.includes('<b>') && !x.includes('a"<'), x);
  assert.equal(ctx.copyCodeHtml(null), '');
  assert.equal(ctx.copyCodeHtml(''), '');
});

t('addEventListener で結んでいる', () => {
  // 🚨 flag ON (= 本番と同じ) で見る。change を結んでいるのはタブB 側なので、
  //    flag OFF の描画だけを見て「結んでいない」と読むのは誤検知
  const on = render({ ...baseLocals, featureFlagEnabled: true });
  assert.ok(on.includes("addEventListener('change'"));
  assert.ok(on.includes("addEventListener('click'"));
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
  // 🚨 作り直した画面 (2026-09-09) は差し替え前にフォーカスの居場所を見る。
  //    contains / querySelector が無いと、ここから下の試験が全部 DOM 不足で落ちる
  const container = { innerHTML: '', contains: () => false, querySelector: () => null };
  // 商品コードを押すとコピーの helper は想定利益タブの外 (共通 helper) にあるので、画面の実物を渡す
  const copy = loadCopyHelpers().ctx;
  const sandbox = {
    escapeHtml: (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
    copyCodeHtml: copy.copyCodeHtml,
    epItemKeyHtml: copy.epItemKeyHtml,
    MALL_FEE_RATES_LABEL: { amazon: 'Amazon', rakuten: '楽天' },
    fetchJson: async () => ({}),
    document: {
      getElementById: (id) => (id === 'table-container' ? container : { value: '', addEventListener() {} }),
      querySelector: () => null,
      // 🚨 作り直した画面 (2026-09-09) は描画の中で querySelectorAll を呼ぶ。
      //    stub に無いと、ここから下の試験が全部「DOM が無い」で落ちる
      querySelectorAll: () => [],
      activeElement: null, body: {},
    },
  };
  const api = new Function(...Object.keys(sandbox),
    src + '; return { render: renderExpectedProfit, detail: epDetailHtml, cols: EP_COLS,'
      + ' serviceTaxRate: EP_SERVICE_TAX_RATE, setState: (o) => Object.assign(epState, o) };'
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
    // 🚨 列の検算をしたいので「全列で照合」の側にする (既定はテープ表示)
    layout: 'table',
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
console.log('配送関係費の内訳 (2026-09-19 中原さん「配送の細かい内訳もクリックしたら見れるように」)');

// 🚨 発端: 0726-001886 の「配送関係費 合計 189」を Easy Ship の送料だと読まれた。
//    実際は 送料 150 + 資材 23 + 人件費 16。合計しか出していないと、合計が送料だと読まれる

/**
 * 内訳の 1 行ずつを {費目名, 金額, 税込の添え書き} に分解する。
 * 🚨 ラベルと金額を HTML 全体から別々に探すと、**入れ替わっていても試験が通る** (Codex R1 P2)。
 *    行の中で組にして取り出し、組のまま突き合わせる
 */
const subRows = (out) => (out.match(/<div class="ep-calc-row sub">[\s\S]*?<\/div>/g) || []).map((row) => {
  const label = row.match(/<span class="ep-calc-label">([\s\S]*?)<\/span><span class="ep-calc-val">/);
  const val = row.match(/<span class="ep-calc-val">([\s\S]*?)<\/span><\/div>/);
  assert.ok(label && val, '内訳の行の形が変わっている: ' + row);
  const incl = label[1].match(/<span class="ep-calc-incl">([\s\S]*?)<\/span>/);
  return {
    label: label[1].replace(/<span class="ep-calc-incl">[\s\S]*?<\/span>/, '').replace(/<[^>]*>/g, '').trim(),
    value: val[1].replace(/<[^>]*>/g, '').trim(),
    incl: incl ? incl[1].trim() : null,
  };
});

t('[!] 合計だけでなく、費目が1つずつ出る (ラベルと金額を組で照合する)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow());
  assert.ok(out.includes('配送関係費 合計'), '合計が消えている');
  // 🚨 組で比べる。ラベルと金額を別々に探すと、入れ替わっていても通ってしまう
  assert.deepEqual(subRows(out).map(({ label, value }) => [label, value]),
    [['送料', '180'], ['出荷作業料', '20'], ['梱包資材費', '10'], ['人件費', '9']]);
});

t('[!] 費目が入れ替わったら落ちる (この試験自体が入れ替わりを見張れているか)', () => {
  // 上の試験が「通るだけ」になっていないことを、ここで実際に入れ替えて確かめる
  const swapped = [['送料', '180'], ['出荷作業料', '10'], ['梱包資材費', '20'], ['人件費', '9']];
  const { api } = makeScreen();
  const actual = subRows(api.detail(sampleRow())).map(({ label, value }) => [label, value]);
  assert.notDeepEqual(actual, swapped, '資材費と作業料が入れ替わっても気づけない試験になっている');
});

t('[!] 送料には税込の元値も添える (Amazon・ヤマトの料金表は税込。150 と 165 を突き合わせられる)', () => {
  const { api } = makeScreen();
  const rows = subRows(api.detail(sampleRow({ shipping_fee_ex_tax: 150 })));
  const fee = rows.find(x => x.label === '送料');
  assert.equal(fee.value, '150');
  // 🚨 部分一致 (/165/) だと 1165 でも通る。添え書きごと完全一致で固定する (Codex R2)
  assert.equal(fee.incl, '料金表では税込 165', '送料の行に税込の元値が添っていない');
});

t('[!] 税込を添えるのは送料だけ (作業料・資材費・人件費は税抜のまま保存されている)', () => {
  const { api } = makeScreen();
  const withIncl = subRows(api.detail(sampleRow())).filter(x => x.incl != null).map(x => x.label);
  assert.deepEqual(withIncl, ['送料'], '送料以外にも税込を添えている');
});

t('[!] 画面の税率が正本 (calc.js SERVICE_TAX_RATE) と同じ', () => {
  // ここがずれると「料金表では税込 ○○」が嘘になり、料金表と突き合わせた人が誤って表を直す
  assert.equal(makeScreen().api.serviceTaxRate, SERVICE_TAX_RATE);
});

t('[!] 添えた税込が、送料マスタの元の金額に戻る (正本の割り戻しと往復させる)', () => {
  // 画面の表示だけを見ても「本当に元の 198 円に戻るか」は分からない。
  // 正本 (shippingCostExTax) が税抜にした値を渡し、画面が税込へ戻せることを往復で確かめる
  const rate = { 送料: 198, 出荷作業料: 0, 想定梱包資材費: 23, 想定人件費: 16 };
  const s = shippingCostExTax(rate);
  assert.ok(s.ok, '正本の割り戻しが失敗している');
  const { api } = makeScreen();
  const out = api.detail(sampleRow({ shipping_fee_ex_tax: s.fee, shipping_total_ex_tax: s.total }));
  assert.ok(out.includes('税込 ' + rate.送料), `税込 ${rate.送料} に戻っていない`);
});

t('[!] Easy Ship は「送料だけ差し替え」と書く (4費目すべてが Easy Ship 料金だと読ませない)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({ easyship_status: 'easyship', easyship_size_code: 'MAIL', easyship_region: '関東' }));
  assert.ok(out.includes('送料だけ Easy Ship の料金に差し替え'), '差し替えの範囲が書かれていない');
  assert.ok(out.includes('送料コード 501'), 'ほかの費目の出所 (自社の送料区分) が書かれていない');
});

t('[!] Easy Ship でない行には差し替えの注記を出さない', () => {
  const { api } = makeScreen();
  for (const st of [null, 'not_registered', 'inactive']) {
    const out = api.detail(sampleRow({ easyship_status: st }));
    assert.ok(!out.includes('送料だけ Easy Ship'), `easyship_status=${st} なのに差し替えの注記が出ている`);
  }
});

t('[!] 送料区分が未登録の行は、費目を0円で並べない (取れていないことを0円に見せない)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({
    shipping_rate_name: null, shipping_method: null, shipping_code: null,
    shipping_fee_ex_tax: null, shipping_work_ex_tax: null,
    shipping_material_ex_tax: null, shipping_labor_ex_tax: null, shipping_total_ex_tax: null,
  }));
  assert.equal(subRows(out).length, 0, '値が無いのに内訳の行を作っている');
});

t('[!] 一部の費目だけ無いときは、その行だけ — にする (0 円と見分けられる)', () => {
  const { api } = makeScreen();
  const rows = subRows(api.detail(sampleRow({ shipping_work_ex_tax: null, shipping_material_ex_tax: 0 })));
  assert.deepEqual(rows.map(({ label, value }) => [label, value]),
    [['送料', '180'], ['出荷作業料', '—'], ['梱包資材費', '0'], ['人件費', '9']]);
});

t('[!] 全部 0 円でも内訳は出す (0 は取れている値。出さないと合計 0 の理由が読めない)', () => {
  const { api } = makeScreen();
  const rows = subRows(api.detail(sampleRow({
    shipping_fee_ex_tax: 0, shipping_work_ex_tax: 0,
    shipping_material_ex_tax: 0, shipping_labor_ex_tax: 0, shipping_total_ex_tax: 0,
  })));
  assert.deepEqual(rows.map(({ label, value }) => [label, value]),
    [['送料', '0'], ['出荷作業料', '0'], ['梱包資材費', '0'], ['人件費', '0']]);
  assert.equal(rows.find(x => x.label === '送料').incl, '料金表では税込 0');
});

t('[!] 注記に入る NE の値も escape する (配送方法名に仕込まれても素の HTML にしない)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({
    easyship_status: 'easyship',
    shipping_method: '<img src=x onerror=alert(1)>', shipping_code: '"><script>alert(2)</script>',
  }));
  assert.ok(!out.includes('<img src=x'), '配送方法名が素の HTML で出ている');
  assert.ok(!out.includes('<script>alert(2)'), '送料コードが素の HTML で出ている');
  assert.ok(out.includes('&lt;img src=x'), 'escape した形でも出ていない (注記そのものが消えている)');
});

t('[!] FBA は配送関係費の内訳を出さない (自社の配送費はかからない)', () => {
  const { api } = makeScreen();
  const out = api.detail(sampleRow({
    fulfillment: 'FBA', shipping_rate_name: null, shipping_total_ex_tax: null, fba_fee_ex_tax: 462,
  }));
  assert.ok(!out.includes('ep-calc-row sub'), 'FBA なのに自社の配送費の内訳を出している');
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

console.log('');
console.log('想定赤字モニター (2026-09-09 作り直し)');

t('4 つの山のボタンが出る (押して絞り込める)', () => {
  for (const key of ['actionable', 'breakeven', 'unknown', 'allowed']) {
    assert.ok(html.includes(`data-ep-pile="${key}"`) || html.includes(`key: '${key}'`),
      `${key} の山が無い`);
  }
});

t('[!] 許容登録のダイアログは feature flag の外に置く (常時 present)', () => {
  // タブ B の flag が OFF の描画でもダイアログが要る (想定利益タブは flag と無関係)
  assert.ok(html.includes('id="ep-allow-dlg"'), 'ダイアログが出ていない');
  for (const id of ['ep-allow-cap', 'ep-allow-until', 'ep-allow-reason', 'ep-allow-by']) {
    assert.ok(html.includes(`id="${id}"`), `${id} が無い`);
  }
});

t('[!] 期限の入力は date で、上限と一緒に必須と書いてある', () => {
  const i = html.indexOf('id="ep-allow-until"');
  assert.ok(i > 0);
  const around = html.slice(i - 400, i + 200);
  assert.ok(/type="date"/.test(html.slice(i - 60, i + 60)), '期限が date 入力ではない');
  assert.ok(around.includes('無期限にはできません'), '無期限が作れないことが書いていない');
});

t('[!] 新しい部分で inline onclick を使っていない (品番の \' で壊れる)', () => {
  const i = html.indexOf('想定赤字モニター (2026-09-09 作り直し)');
  const block = html.slice(i);
  assert.ok(!/onclick="[^"]*ep[A-Z]/.test(block), 'inline onclick が残っている');
});

t('[!] ヘッダは .portal-header を使う (.top-nav は style.css に存在しない)', () => {
  assert.ok(html.includes('class="portal-header"'));
  assert.ok(!/\.top-nav\s*\{/.test(html), '存在しないクラスにスタイルを当てている');
});

t('フォントは画面ぜんぶメイリオ (中原さん指定)', () => {
  assert.ok(/font-family:\s*"Meiryo"/.test(html), 'メイリオが指定されていない');
});

t('[!] 明るい地の前提だった直書き色が残っていない (ダークで読めなくなる)', () => {
  // :root のトークン定義は除いて調べる
  const rootEnd = html.indexOf('}', html.indexOf(':root {'));
  const rest = html.slice(rootEnd, html.indexOf('</style>'));
  const bad = ['#fff', '#ffffff', '#f9fafb', '#f3f4f6', '#e5e7eb', '#dc2626', '#1e3a8a']
    .filter(c => rest.toLowerCase().includes(c));
  assert.deepEqual(bad, [], `直書きの明色が残っている: ${bad.join(', ')}`);
});

t('[!] 判定できない件数を隠さない (0 件になるまで「赤字なし」と書かない)', () => {
  assert.ok(html.includes('この件数が残るうちは「赤字なし」とは言えません'));
  assert.ok(html.includes('「赤字なし」とはまだ言えません'), '要対応 0 件のときの文言が無い');
});

t('[!] 内訳の列は消していない (全列で照合に切り替えられる)', () => {
  // 🚨 列数を文言に埋め込まない。列を足すたびに文言と実物がずれる
  assert.ok(html.includes('全列で照合'));
  assert.ok(html.includes('function epFullTableHtml'));
});

console.log('');
console.log('Codex レビュー 2 巡目の指摘');

t('[!] 黒字も含めて全出品を見る入口がある (山だけだと照合・CSV から黒字が消える)', () => {
  assert.ok(html.includes('data-ep-pile="all"'), '全出品の入口が無い');
  assert.ok(html.includes('黒字も含めて全出品を見る'));
});

t('[!] 保存中はダイアログを閉じさせない (先の保存が今のダイアログを閉じる)', () => {
  assert.ok(html.includes('epAllowSaving'), '保存中フラグが無い');
  assert.ok(/dlg\.addEventListener\('cancel'/.test(html), 'Escape での閉鎖を止めていない');
  assert.ok(html.includes('epRevoking'), '取り消しに処理中ガードが無い');
  assert.ok(html.includes('seq !== epAllowSeq'), '応答をどの操作のものか照合していない');
});

t('[!] 再描画のあとフォーカスを戻す (キーボードだけで操作する人が居場所を失う)', () => {
  assert.ok(html.includes('function epFocusSelector'), 'フォーカス復元が無い');
  assert.ok(html.includes('back.focus()'));
});

t('[!] 押しても何も起きないボタンを置かない', () => {
  assert.ok(!html.includes('足りない情報を登録する'), '動かないボタンが残っている');
  assert.ok(html.includes('EP_FIX_HINT'), '次に何をすればいいかの案内が無い');
});

t('[!] 欠損の「—」を罫線色にしない (ダークでほぼ消える)', () => {
  assert.ok(/\.ep-none \{ color: var\(--muted\)/.test(html), '.ep-none が読めない色のまま');
  assert.ok(!/rgba\(255,255,255,\.7\)/.test(html), '明るいホバー背景が残っている');
});

t('[!] ダイアログに名前とエラーの通知がある', () => {
  assert.ok(html.includes('aria-labelledby="ep-allow-heading"'));
  assert.ok(html.includes('id="ep-allow-err" role="alert"'));
  assert.ok((html.match(/aria-required="true"/g) || []).length >= 4, '必須項目に aria-required が足りない');
});

t('展開した内訳を aria-controls で結ぶ', () => {
  assert.ok(html.includes("setAttribute('aria-controls', 'ep-open-panel')"));
  assert.ok(html.includes('id="ep-open-panel"'));
});

console.log('');
console.log('Codex レビュー 3 巡目の指摘');

t('[!] 反対側の件数を取れなかったとき、前の「要対応 0」を残さない', () => {
  assert.ok(html.includes('scopeCountFailed'), '失敗を 0 件と区別していない');
  assert.ok(html.includes('件数を取れませんでした'));
  assert.ok(html.includes('delete epState.scopeCounts[otherScope]'), '再取得時に未確認へ戻していない');
});

t('[!] 読み込みで DOM を差し替える前にフォーカスの戻り先を覚える', () => {
  const i = html.indexOf('async function loadExpectedProfit()');
  const j = html.indexOf("c.innerHTML = '<div class=\"loading\">", i);
  assert.ok(i > 0 && j > i);
  assert.ok(html.slice(i, j).includes('epPendingFocus = epFocusSelector'),
    '読み込み表示に差し替えたあとでは、押したボタンはもう無い');
});

t('[!] 待っている間に人が別の場所へ移っていたらフォーカスを奪わない', () => {
  assert.ok(html.includes('const mayFocus = document.activeElement === document.body'));
});

t('[!] 閉じた行に aria-controls を残さない (別の行の内訳を指す)', () => {
  assert.ok(html.includes("b.removeAttribute('aria-controls')"));
  // 再読み込みで開いたまま復元する経路でも付ける
  assert.ok(html.includes("(open ? ' aria-controls=\"ep-open-panel\"' : '')"));
});

console.log('');
console.log('在庫数・取扱区分 (2026-09-09 中原さん指示)');

// 一覧 (テープ表示) 側。既定の表示はこちらなので、ここに出ていないと「出していない」に等しい
function renderTape(rows, over = {}) {
  const { api, container } = makeScreen();
  const { summary: sumOver, ...stateOver } = over;
  api.setState({
    layout: 'tape',
    rows, total: rows.length, scope: 'self_v1', published: PUBLISHED,
    summary: { total: rows.length, ok: rows.length, rankEligible: rows.length, expiredNow: 0, ...sumOver },
    ...stateOver,
  });
  api.render();
  return container.innerHTML;
}

// 在庫セルの中身だけを取り出す (画面のどこかに 0 があるだけで通る試験にしない)
const stockCell = (out) => {
  const m = out.match(/<span class="ep-cell-label">在庫[\s\S]*?<\/span><span class="ep-cell-val[^"]*">([\s\S]*?)<\/span>/);
  return m ? m[1] : null;
};

// 取扱区分のバッジ (在庫セルのラベルの中) だけを取り出す。
// 🚨 画面全体を includes で見ない。2026-09-10 に絞り込みの <option value="stopped"> を足したら、
//    「取扱中の行に stopped が付いていないこと」を見ていた試験が**素通り**した。
//    行のバッジを名指しで取れば、画面のどこか別の場所の文字列では通らない
const handlingBadge = (out) => {
  const m = out.match(/<span class="ep-cell-label">在庫\s*<span class="(how[^"]*)">([^<]*)<\/span>/);
  return m ? { cls: m[1], text: m[2] } : null;
};

const stocked = (over = {}) => sampleRow({
  handling_class: '取扱中', stock_qty: 12, stock_allocated_qty: 3,
  monitor_state: 'unallowed', built_at: '2026-09-08T00:00:00Z', ...over,
});

t('[!] 一覧に在庫数と取扱区分が出る', () => {
  // 🚨 画面のどこかに「在庫」の 2 文字があるだけでは通さない。**在庫のセルの中身**を見る
  const out = renderTape([stocked()]);
  assert.equal(stockCell(out), '12', '一覧の在庫セルに在庫数が出ていない');
  assert.equal(handlingBadge(out)?.text, '取扱中', '行のバッジに取扱区分が出ていない');
});

t('[!] 取扱中でないものが一覧で見分けられる', () => {
  const on = handlingBadge(renderTape([stocked()]));
  const off = handlingBadge(renderTape([stocked({ handling_class: '取扱終了' })]));
  assert.equal(off?.text, '取扱終了', '取扱区分が出ていない');
  assert.match(off.cls, /stopped/, '取扱中でないことが見た目で分からない');
  assert.doesNotMatch(on.cls, /stopped/, '取扱中まで止まっている扱いになっている');
});

t('[!] 在庫が分からない行を「0」と書かない', () => {
  const zero = stockCell(renderTape([stocked({ stock_qty: 0, stock_allocated_qty: 0 })]));
  const unknown = renderTape([stocked({ stock_qty: null, stock_allocated_qty: null })]);
  assert.equal(zero, '0', '在庫0 が 0 と出ていない');
  assert.ok(!/\d/.test(stockCell(unknown) || ''), `在庫が分からない行に数字が出ている: ${stockCell(unknown)}`);
  assert.ok(unknown.includes('分かりません'), '分からないことが書かれていない');
});

t('[!] FBA の行には「FBA 倉庫の在庫ではない」と書く', () => {
  const fba = renderTape([stocked({ fulfillment: 'FBA' })]);
  assert.ok(fba.includes('FBA 倉庫の在庫は含みません'), 'FBA 在庫と誤読されるまま出している');
  const fbm = renderTape([stocked({ fulfillment: 'FBM' })]);
  assert.ok(!fbm.includes('FBA 倉庫の在庫は含みません'), '自社出荷の行にまで FBA の注記が出ている');
});

t('[!] 引当と「出せる在庫」も読める (在庫数だけでは出荷できる数が分からない)', () => {
  const out = renderTape([stocked()]);
  assert.ok(out.includes('引当 3 個'), '引当数が読めない');
  assert.ok(out.includes('出せる 9 個'), '出せる在庫が出ていない');
});

t('[!] 引当が分からなければ「出せる在庫」を出さない (0 で埋めない)', () => {
  const out = renderTape([stocked({ stock_allocated_qty: null })]);
  assert.ok(out.includes('在庫 12 個'), '在庫数まで消えている');
  assert.ok(!out.includes('出せる'), '引当が分からないのに出せる在庫を出している');
});

t('[!] 全列で照合の表にも 取扱区分・在庫数・引当数 の列がある', () => {
  const out = renderTable([stocked()], 'self_v1');
  for (const h of ['取扱区分', '在庫数', '引当数']) {
    assert.ok(out.includes('>' + h + '<'), `見出し「${h}」が無い`);
  }
  assert.ok(out.includes('在庫・取扱'), 'まとまりの見出しが無い');
});

t('[!] 在庫と取扱区分は計算に入っていない、と画面に書いてある', () => {
  // 「在庫を見て利益を出している」と誤解されると、数字の意味が変わってしまう
  assert.ok(html.includes('在庫数・取扱区分は計算に入っていません'), '計算に入っていない旨が無い');
  assert.ok(html.includes('自社倉庫のぶんだけ'), '自社倉庫ぶんであることが書かれていない');
});

t('[!] 在庫がいつ時点の値かを書く (日中に動くので「いまの在庫」と読まれる)', () => {
  const out = renderTape([stocked()]);
  assert.ok(/\d+\/\d+ \d+:\d+ 時点/.test(out), '在庫がいつ時点かが画面に無い');
});

t('[!] 行を開いた内訳にも在庫と取扱区分が出る (根拠を見る層)', () => {
  const { api } = makeScreen();
  const out = api.detail(stocked({ handling_class: '取扱終了' }));
  assert.ok(/在庫（\d+\/\d+ \d+:\d+ 時点）/.test(out), '在庫の as-of が内訳に無い');
  assert.ok(out.includes('在庫 12 個'), '内訳に在庫数が無い');
  assert.ok(out.includes('取扱終了'), '内訳に取扱区分が無い');
  assert.ok(out.includes('もう扱っていない出品です'), '取扱終了の意味が書かれていない');
});

console.log('');
console.log('長い商品名で一覧が崩れる (2026-09-10 中原さん報告)');

// 🚨 実際に画面で起きた形。行そのものが <button> なので中身は全部 <span> で書いてある。
//    素の <span> は行内要素なので overflow / text-overflow が効かず、長い商品名が「…」で
//    切れずに右の 売価・原価・在庫 の列に重なって出ていた。**display:block が要る**
const cssRule = (name) => {
  const m = html.match(new RegExp('\\.' + name + '\\s*\\{([^}]*)\\}'));
  return m ? m[1] : null;
};

// 🚨 4 つそろって初めて切れる。1 つでも欠けるとはみ出すので 4 つとも見る
//    (Codex R1: overflow / nowrap を消しても通る試験だった)
const assertClipped = (name) => {
  const rule = cssRule(name);
  assert.ok(rule, `.${name} の定義が無い`);
  assert.match(rule, /display:\s*block/,
    `.${name} が行内要素のまま。overflow / text-overflow が効かず、長い文字列が右の列に重なる`);
  assert.match(rule, /overflow:\s*hidden/, `.${name} にはみ出しを隠す指定が無い`);
  assert.match(rule, /text-overflow:\s*ellipsis/, `.${name} に「…」で切る指定が無い`);
  assert.match(rule, /white-space:\s*nowrap/, `.${name} に折り返さない指定が無い (折り返すと行が伸びる)`);
};

t('[!] 一覧の商品名は「…」で切れる (行内要素のままだと隣の列に重なる)', () => {
  assertClipped('ep-tape-name');
});

t('[!] 除外理由の行も同じ (長い理由がはみ出す)', () => {
  assertClipped('ep-tape-why');
});

t('[!] 商品名を切るには親に min-width:0 が要る (grid のセルは中身より狭くならない)', () => {
  assert.match(cssRule('ep-tape-main'), /min-width:\s*0/);
});

console.log('');
console.log('モール・取扱・在庫での絞り込み (2026-09-10 中原さん指示)');

const selectOptions = (out, id) => {
  const m = out.match(new RegExp('<select id="' + id + '">([\\s\\S]*?)</select>'));
  if (!m) return null;
  return [...m[1].matchAll(/<option value="([^"]*)"/g)].map(x => x[1]);
};

t('[!] モール・取扱・在庫の 3 つで絞り込める', () => {
  const out = renderTape([stocked()]);
  assert.ok(selectOptions(out, 'ep-mall'), 'モールの絞り込みが無い');
  assert.ok(selectOptions(out, 'ep-handling'), '取扱の絞り込みが無い');
  assert.ok(selectOptions(out, 'ep-stock'), '在庫の絞り込みが無い');
});

t('[!] 選べる値はサーバが受け付ける値と同じ (選べるのに効かない項目を作らない)', () => {
  const out = renderTape([stocked()]);
  // '' = 全部 (絞り込まない)。残りは query.js の HANDLING_FILTERS / STOCK_FILTERS の名前
  assert.deepEqual(selectOptions(out, 'ep-handling'), ['', 'active', 'stopped', 'unknown']);
  assert.deepEqual(selectOptions(out, 'ep-stock'), ['', 'in_stock', 'none', 'unknown']);
});

t('[!] 在庫「なし」の説明が実際の判定と合っている (0 個以下。負の在庫もここに入る)', () => {
  // 🚨 Codex R1。query.js の none は「0 以下」。画面に「0 個」と書くと嘘になる
  const out = renderTape([stocked()]);
  const m = out.match(/<option value="none"[^>]*>([^<]*)<\/option>/);
  assert.match(m[1], /0 個以下/, `在庫「なし」の説明が判定と合っていない: ${m && m[1]}`);
});

t('[!] 既定は「全部」(開いた瞬間に何かが隠れていない)', () => {
  const out = renderTape([stocked()]);
  const selected = (id) => {
    const m = out.match(new RegExp('<select id="' + id + '">([\\s\\S]*?)</select>'));
    const s = m[1].match(/<option value="([^"]*)" selected>/);
    return s ? s[1] : null;
  };
  assert.equal(selected('ep-handling'), '');
  assert.equal(selected('ep-stock'), '');
});

t('[!] 絞り込んだら見出しにそう書く (何件の N が何を指すか分からなくなる)', () => {
  const out = renderTape([stocked()], { handling: 'active', stock: 'in_stock' });
  assert.match(out, /取扱 取扱中/, '見出しに取扱の絞り込みが出ていない');
  assert.match(out, /在庫 あり/, '見出しに在庫の絞り込みが出ていない');
  assert.match(out, /上の件数は[^<]*取扱[^<]*で絞り込む前/, '山の件数が絞り込み前だと書いていない');
});

t('[!] 絞り込んでいなければ「絞り込む前」の断りは出さない (いつも出ていると読まれない)', () => {
  assert.doesNotMatch(renderTape([stocked()]), /絞り込む前/);
});

const EMPTY_GEN = { total: 9021, handlingKnown: 0, stockKnown: 0 };

t('[!] 在庫がどこにも入っていなければ、0 件を「該当なし」と読ませない', () => {
  // 2026-09-09 の夜に実際に起きた形 (夜間バッチが古い版で動いて列が入らなかった)
  const out = renderTape([], { stock: 'in_stock', summary: EMPTY_GEN });
  assert.match(out, /9,021 件すべてで[^<]*在庫数[^<]*が空なので、この絞り込みは必ず 0 件になります/,
    '「空です」と書いていない。絞り込みが 0 件になったのを該当なしと読んでしまう');
  assert.match(out, /「該当なし」ではありません/);
});

t('[!] その出荷区分に出品が 1 件も無い夜を「空です」と言わない (Codex R1)', () => {
  // 🚨 集計は選んでいる出荷区分のぶん。0 件の区分では handlingKnown も 0 になるが、
  //    それは「入っていない」ではなく「数える相手が居ない」
  const out = renderTape([], { stock: 'in_stock', summary: { total: 0, handlingKnown: 0, stockKnown: 0 } });
  assert.doesNotMatch(out, /空なので/, '出品 0 件を「空です」と言い切っている');
});

t('[!] 値が入っている世代には「空です」を出さない', () => {
  const out = renderTape([stocked()],
    { stock: 'in_stock', summary: { total: 9021, handlingKnown: 9021, stockKnown: 9021 } });
  assert.doesNotMatch(out, /空なので/);
});

t('[!] 「分からない」を選んでいるときは断らない (値が空でも行は出る)', () => {
  // 🚨 Codex R2。unknown は**空の行を選ぶ**絞り込み。0 件にならないので警告は筋違い
  const out = renderTape([stocked()], { stock: 'unknown', summary: EMPTY_GEN });
  assert.doesNotMatch(out, /空なので/, '「分からない」を選んでいるのに 0 件になると断っている');
});

t('[!] 絞り込んでいない側の列が空でも断らない (Codex R2)', () => {
  // 取扱で絞っていて、その 0 件が本当に「該当なし」のとき、在庫の空を持ち出さない
  const out = renderTape([], { handling: 'stopped', summary: { total: 9021, handlingKnown: 9021, stockKnown: 0 } });
  assert.doesNotMatch(out, /空なので/, '選んでいない在庫の欠損で「該当なしではない」と言っている');
});

t('[!] 断るのは、選んでいる絞り込みが要求している列だけ', () => {
  const out = renderTape([], { handling: 'active', summary: EMPTY_GEN });
  assert.match(out, /取扱区分が空なので/);
  assert.doesNotMatch(out, /在庫数が空なので/, '選んでいない在庫まで持ち出している');
});

t('[!] 画面の「取扱中」は正本 (query.js) と同じ文字列', () => {
  // 片方だけ変えると、取扱中の行が「もう扱っていない」色で出る
  assert.ok(html.includes(`=== '${HANDLING_ACTIVE}'`),
    `画面が ${HANDLING_ACTIVE} を判定していない (query.js の HANDLING_ACTIVE とズレている)`);
});

t('[!] CSV も画面と同じ絞り込みで出す (絞る前の行が混ざった CSV を配らない)', () => {
  const m = html.match(/function epQuery\(extra\)[\s\S]*?\n    \}/);
  assert.ok(m, 'epQuery が見つからない');
  assert.match(m[0], /q\.set\('handling'/, 'epQuery に取扱の絞り込みが入っていない');
  assert.match(m[0], /q\.set\('stock'/, 'epQuery に在庫の絞り込みが入っていない');
  assert.match(html, /expected-profit\.csv\?' \+ epQuery\(\)/, 'CSV が epQuery を使っていない');
});

console.log('');
console.log('Amazon の自社出荷: Easy Ship か自己配送か (2026-09-09 中原さん指示)');

const esRowFor = (over = {}) => stocked({
  mall: 'amazon', fulfillment: 'FBM', shipping_rate_name: 'Amazon Easy Ship サイズ60',
  shipping_rate_category: 'Easy Ship', easyship_status: 'easyship',
  easyship_size_code: 'SIZE_60', easyship_region: '関東', ...over,
});

t('[!] Easy Ship で計算した行は、一覧でそう分かる', () => {
  const out = renderTape([esRowFor()]);
  assert.ok(out.includes('Easy Ship'), '一覧に Easy Ship と出ていない');
  assert.ok(out.includes('Amazon Easy Ship サイズ60'), '使った配送が Easy Ship の区分名になっていない');
});

t('[!] 自己配送とみなした行は、一覧で見分けられる', () => {
  // 🚨 これが見えないと「登録漏れで違う送料のまま」に気づけない (中原さん指示の条件)
  const out = renderTape([esRowFor({
    easyship_status: 'not_registered', shipping_rate_name: 'ネコポス',
    shipping_rate_category: 'メール便', easyship_size_code: null, easyship_region: null,
  })]);
  assert.ok(out.includes('自己配送とみなし'), '自己配送とみなしたことが一覧に出ていない');
  assert.ok(out.includes('ep-chip-warn'), '目立つ形になっていない');
});

t('Easy Ship で計算できた行は警告の色にしない', () => {
  assert.ok(!renderTape([esRowFor()]).includes('ep-chip-warn'), 'Easy Ship なのに警告扱い');
});

t('[!] 行を開くと、何をすれば直るかまで書いてある', () => {
  const { api } = makeScreen();
  const out = api.detail(esRowFor({ easyship_status: 'not_registered' }));
  assert.ok(out.includes('Amazon の配送'), '内訳に Amazon の配送が無い');
  assert.ok(out.includes('/apps/easy-ship'), '登録先が書かれていない');
  assert.ok(out.includes('自己配送とみなして'), '何をしたかが書かれていない');
});

t('[!] Easy Ship の行には、どのサイズ・どの宛先で計算したかを書く', () => {
  const { api } = makeScreen();
  const out = api.detail(esRowFor());
  assert.ok(out.includes('SIZE_60'), 'サイズ区分が無い');
  assert.ok(out.includes('関東'), '宛先地域が無い');
  assert.ok(out.includes('実際の請求額ではありません'), '想定であることが書かれていない');
});

t('[!] サイズが読めなかった行は、近いサイズに寄せていないと書く', () => {
  const { api } = makeScreen();
  const out = api.detail(esRowFor({ easyship_status: 'size_unmapped', easyship_size_code: 'SIZE_70' }));
  assert.ok(out.includes('SIZE_70'), 'どのコードが読めなかったか出ていない');
  assert.ok(out.includes('近いサイズには寄せていません'), '寄せていないことが書かれていない');
});

t('Easy Ship の状態を持たない行 (楽天・FBA) には何も出さない', () => {
  const { api } = makeScreen();
  const out = api.detail(stocked({ easyship_status: null }));
  assert.ok(!out.includes('Amazon の配送'), '関係ない行にまで出ている');
});

t('[!] Easy Ship の状態は全部、日本語の言葉と説明を持つ (Codex P2)', () => {
  // 🚨 状態を足して言葉を足し忘れると、内部の英語がそのまま画面に出て、
  //    「見えるようにしておく」という条件 (中原さん指示) を満たせなくなる。
  //    一覧を正本にして、画面がそれを網羅していることを突き合わせる
  const { api } = makeScreen();
  for (const status of EASYSHIP_STATUSES) {
    const row = esRowFor({ easyship_status: status });
    const tape = renderTape([row]);
    assert.ok(!new RegExp('>\\s*' + status + '\\s*<').test(tape),
      `一覧に内部の英語 ${status} がそのまま出ている`);
    const detail = api.detail(row);
    assert.ok(detail.includes('Amazon の配送'), `${status} の内訳に Amazon の配送が無い`);
    assert.ok(!detail.includes(status), `${status} の説明が無く、内部の英語が出ている`);
  }
});

t('[!] 全列で照合の表に「Amazonの配送」の列がある', () => {
  const out = renderTable([esRowFor()], 'self_v1');
  assert.ok(out.includes('>Amazonの配送<'), '見出しが無い');
});

t('[!] 前提の欄に、Easy Ship の扱いと「請求額ではない」旨が書いてある', () => {
  assert.ok(html.includes('Amazon の自社出荷は Easy Ship 料金で計算します'), '扱いが書かれていない');
  assert.ok(html.includes('自己配送とみなして'), '未登録の扱いが書かれていない');
  assert.ok(html.includes('注文履歴でしか分かりません'), '請求額ではないことが書かれていない');
});

console.log('\n商品コードを押すとコピー: 描画と失敗時 (Codex R1)');

// 🚨 helper を直接呼ぶだけの試験は「テープ行からコードの表示を消した」「予備経路が常に失敗」を
//    壊しても通っていた (Codex R1 がメモリ上で両方を施して 89 件 PASS を確認)。
//    実際に描いた行にコピー用のコードが出ていること、失敗の経路ごとの結末を固定する
t('[!] テープ行と 24 列の表の両方で、出品コードが押すとコピーになっている', () => {
  const row = { ...esRowFor(), mall: 'rakuten', mall_item_key: 'oscare3/oscare2', fulfillment: 'self' };
  for (const [name, out] of [['テープ行', renderTape([row])], ['24 列の表', renderTable([row], 'self_v1')]]) {
    assert.ok(out.includes('class="copy-code" data-copy="oscare3"'), `${name} に商品管理番号のコピーが無い`);
    assert.ok(out.includes('class="copy-code" data-copy="oscare2"'), `${name} に SKU管理番号のコピーが無い`);
  }
  const detail = makeScreen().api.detail({ ...row, ne_code: 'ne-001' });
  assert.ok(detail.includes('class="copy-code" data-copy="ne-001"'), '行を開いた内訳の NE品番がコピーできない');
});

async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

await ta('[!] clipboard に書けたら「コピーしました」', async () => {
  const h = loadCopyHelpers();
  clickCode(h, 'oscare3');
  await settle();
  assert.deepEqual(h.written, ['oscare3']);
  assert.equal(h.execCalls.length, 0, '書けたのに予備経路まで動いている');
  assert.equal(h.toast.textContent, '「oscare3」をコピーしました');
  assert.equal(h.toast.ng, false);
  assert.equal(h.toast.hidden, false);
});

await ta('[!] clipboard が拒否したら予備経路でコピーする', async () => {
  const h = loadCopyHelpers({ clipboard: 'reject' });
  clickCode(h, 'oscare3');
  await settle();
  assert.deepEqual(h.execCalls, ['copy'], '予備経路に落ちていない');
  assert.equal(h.toast.textContent, '「oscare3」をコピーしました');
});

await ta('[!] clipboard API が無ければ予備経路でコピーする', async () => {
  const h = loadCopyHelpers({ clipboard: 'none' });
  clickCode(h, 'oscare3');
  await settle();
  assert.deepEqual(h.execCalls, ['copy']);
  assert.equal(h.toast.textContent, '「oscare3」をコピーしました');
});

await ta('[!] 予備経路まで失敗したら「コピーできませんでした」と出す (無言にしない)', async () => {
  const h = loadCopyHelpers({ clipboard: 'reject', execOk: false });
  clickCode(h, 'oscare3');
  await settle();
  assert.equal(h.toast.textContent, 'コピーできませんでした');
  assert.equal(h.toast.ng, true, '失敗なのに成功の色のまま');
});

await ta('[!] 別の場所に文字の選択が残っていてもコピーする (Codex R1)', async () => {
  const elsewhere = { isCollapsed: false, rangeCount: 1, getRangeAt: () => ({ intersectsNode: () => false }) };
  const h = loadCopyHelpers({ selection: elsewhere });
  const { stopped } = clickCode(h, 'oscare3');
  await settle();
  assert.ok(stopped);
  assert.deepEqual(h.written, ['oscare3'], '無関係な選択でコピーを止めている');
  assert.equal(h.toast.textContent, '「oscare3」をコピーしました');
});

await ta('[!] 押したコードそのものを選んでいるときは、手でのコピーを優先する', async () => {
  const onCode = { isCollapsed: false, rangeCount: 1,
    getRangeAt: () => ({ intersectsNode: (n) => n && n.dataset && n.dataset.copy === 'oscare3' }) };
  const h = loadCopyHelpers({ selection: onCode });
  const { stopped } = clickCode(h, 'oscare3');
  await settle();
  assert.ok(stopped, '選択中でも行の開閉は止める');
  assert.deepEqual(h.written, []);
  assert.equal(h.toast.textContent, '');
});

console.log('\n売価を変えて試算 (2026-09-14 中原さん指示)');

/** 画面の phProfit を切り出す (商品ハブの式と同じ数字になるかを見る) */
function loadPhProfit() {
  const i = html.indexOf('function phProfit(');
  const j = html.indexOf('\n    function ', i + 10);
  assert.ok(i > 0 && j > i, '画面に phProfit が無い');
  return new Function(html.slice(i, j) + '\nreturn phProfit;')();
}

t('[!] 試算の式は商品ハブ (product-hub/lib/profit.js) と同じ数字を出す', () => {
  const ph = loadPhProfit();
  const cases = [[1280, 660, 10, 237], [7480, 6300, 8, 945], [9800, 6300, 8, 237], [500, 900, 10, 300], [9999, 1, 10, 0], [1, 0, 8, 0]];
  for (const [price, cost, tax, ship] of cases) {
    const want = phComputeProfit({ price, costExTax: cost, taxPercent: tax, shippingCost: ship });
    const got = ph(price, cost, tax, ship, PH_TAKE_RATE);
    const label = JSON.stringify({ price, cost, tax, ship });
    assert.equal(Math.round(got.profit), want.profit, '利益額が違う ' + label);
    assert.equal(Math.round(got.costIncTax), want.costIncTax, '税込原価が違う ' + label);
    // 画面は商品ハブの画面と同じ toFixed(1)。サーバ側の丸めとは境界で 0.1 ずれうる
    assert.ok(Math.abs(Number(got.margin.toFixed(1)) - want.marginPct) <= 0.1, '利益率が違う ' + label);
  }
  // 商品ハブ smoke.mjs と同じ例 (1280円 / 原価660 / 税10% / 送料237 → 189円 / 14.8%)
  const ex = ph(1280, 660, 10, 237, PH_TAKE_RATE);
  assert.equal(Math.round(ex.profit), 189);
  assert.equal(ex.margin.toFixed(1), '14.8');
});

t('[!] 行を開いた内訳に試算の枠が出る (NE品番がある行だけ)', () => {
  const row = { ...esRowFor(), mall: 'rakuten', mall_item_key: 'drycricket200-3/normal-inventory',
    ne_code: 'drycricket200-3', price_incl_tax: 7480 };
  const d = makeScreen().api.detail(row);
  assert.ok(d.includes('data-pcalc-ne="drycricket200-3"'), '試算の枠が無い');
  assert.ok(d.includes('data-pcalc-price="7480"'), '今の売価が初期値に入っていない');
  assert.ok(d.includes('商品ハブの基本情報と同じ式'), '想定利益と式が違うことが書かれていない');
  const none = makeScreen().api.detail({ ...row, ne_code: null });
  assert.ok(!none.includes('data-pcalc-ne'), 'NE品番が無いのに読み込もうとしている');
  assert.ok(none.includes('NE品番に紐づいていないので計算できません'));
});

t('[!] まとめ買い SKU は原価を個数ぶんにする (1 個ぶんで試算すると利益が過大に出る)', () => {
  const d = makeScreen().api.detail({ ...esRowFor(), mall: 'amazon', ne_code: 'abc', unit_quantity: 3 });
  assert.ok(d.includes('data-pcalc-qty="3"'));
});

/**
 * 画面の関数を名前で切り出して動かす (試算の値の決め方を、画面の操作と切り離して確かめる)。
 * 切り出すのはこの画面自身の <script> だけ (外から来た文字列は入らない)
 */
function epFn(names, deps = {}) {
  const src = names.map((n) => {
    const i = html.indexOf('function ' + n + '(');
    const j = html.indexOf('\n    function ', i + 10);
    assert.ok(i > 0 && j > i, '画面に ' + n + ' が無い');
    return html.slice(i, j);
  }).join('\n');
  return new Function(...Object.keys(deps), src + '\nreturn {' + names.join(', ') + '};')(...Object.values(deps));
}
const pcData = (over = {}) => ({
  found: true, ne_code: 'drycricket200-3', cost_ex_tax: 6300, shipping_cost: 945, shipping_method: '宅急便100',
  tax_percent: 8, tax_source: 'ne', take_rate: PH_TAKE_RATE,
  // 🚨 NE の登録値を先頭に置かない (「いつも先頭を選ぶ」壊れ方を検知するため)
  ship_choices: [{ method: 'ネコポス', cost: 237 }, { method: '宅急便100', cost: 945, isCurrent: true }, { method: '宅急便120', cost: 1100 }],
  ...over,
});

t('[!] 試算: まとめ買いは原価を個数ぶんにして、商品ハブの式と同じ利益額になる (Codex R1 P2)', () => {
  const { epPriceCalcModel: model, phProfit: ph } = epFn(['phProfit', 'epPriceCalcModel']);
  const m3 = model(pcData(), '3');
  assert.equal(m3.ok, true);
  assert.equal(m3.cost, 18900, '原価が個数ぶんになっていない');
  const p = ph(30000, m3.cost, m3.tax, m3.ship, m3.take);
  const want = phComputeProfit({ price: 30000, costExTax: 18900, taxPercent: 8, shippingCost: 945 });
  assert.equal(Math.round(p.profit), want.profit);
  assert.equal(model(pcData(), '1').cost, 6300);
  assert.equal(model(pcData(), undefined).cost, 6300, '個数の印が無い枠は 1 個');
});

t('[!] 試算: 開いた時点は NE の登録送料が選ばれている (先頭でなくても) (Codex R1 P2)', () => {
  const { epPriceCalcModel: model } = epFn(['phProfit', 'epPriceCalcModel']);
  const m = model(pcData(), '1');
  assert.equal(m.initialIndex, 1, 'NE の登録値ではなく先頭が選ばれている');
  assert.equal(m.ship, 945);
  const noCurrent = model(pcData({ ship_choices: [{ method: 'ネコポス', cost: 237 }, { method: '宅急便120', cost: 1100 }] }), '1');
  assert.equal(noCurrent.initialIndex, 0);
  const noChoices = model(pcData({ ship_choices: [] }), '1');
  assert.equal(noChoices.initialIndex, -1);
  assert.equal(noChoices.ship, 945, '選択肢が無いときは NE の送料で計算する');
});

t('[!] 試算: Amazon で個数が分からない出品は計算しない (Codex R1 P1)', () => {
  const { epPriceCalcModel: model } = epFn(['phProfit', 'epPriceCalcModel']);
  const m = model(pcData(), 'unknown');
  assert.equal(m.ok, false);
  assert.ok(m.message.includes('何個入りか分からない'), m.message);
  // 描画側: Amazon × 個数不明 → unknown / 楽天 × 個数なし → 1 個
  const az = makeScreen().api.detail({ ...esRowFor(), mall: 'amazon', ne_code: 'abc', unit_quantity: null });
  assert.ok(az.includes('data-pcalc-qty="unknown"'), 'Amazon の個数不明を 1 個として試算しようとしている');
  const rk = makeScreen().api.detail({ ...esRowFor(), mall: 'rakuten', ne_code: 'abc', unit_quantity: null });
  assert.ok(rk.includes('data-pcalc-qty="1"'), '楽天 (個数を持たない) が計算されない');
});

t('[!] 試算: 個数を持つモールの一覧が想定利益の正本 (skuMapHasQuantity) と同じ', () => {
  const m = html.match(/const EP_QTY_MALLS = (\[[^\]]*\]);/);
  assert.ok(m, '画面に EP_QTY_MALLS が無い');
  const list = JSON.parse(m[1].replace(/'/g, '"'));
  for (const mall of ['amazon', 'rakuten', 'yahoo', 'aupay', 'qoo10', 'linegift', 'mercari']) {
    assert.equal(list.includes(mall), skuMapHasQuantity(mall), mall + ' の扱いが想定利益と違う');
  }
});

t('試算: 品番・原価・送料が無いときは理由を出して計算しない', () => {
  const { epPriceCalcModel: model } = epFn(['phProfit', 'epPriceCalcModel']);
  assert.equal(model({ found: false, reason: 'not_found' }, '1').ok, false);
  assert.ok(model({ found: false, reason: 'ambiguous' }, '1').message.includes('大文字小文字'));
  assert.ok(model(pcData({ cost_ex_tax: null }), '1').message.includes('原価が無い'));
  assert.ok(model(pcData({ shipping_cost: null }), '1').message.includes('送料'));
  assert.equal(model(pcData({ cost_ex_tax: 0 }), '1').ok, true, '原価 0 は 0 として計算する (商品ハブと同じ)');
});

t('[!] 試算: 読み込み中に閉じた枠へ、あとから来た応答を書き込まない (Codex R1 P2)', () => {
  const { epRenderPriceCalc } = epFn(['phProfit', 'epPriceCalcModel', 'epRenderPriceCalc'],
    { escapeHtml: (s) => String(s), epNum: (n) => String(n) });
  const closed = { isConnected: false, innerHTML: 'old', dataset: { pcalcQty: '1' } };
  epRenderPriceCalc(closed, pcData());
  assert.equal(closed.innerHTML, 'old', '閉じた枠を書き換えた');
  const unknown = { isConnected: true, innerHTML: '', dataset: { pcalcQty: 'unknown' } };
  epRenderPriceCalc(unknown, pcData());
  assert.ok(unknown.innerHTML.includes('何個入りか分からない'), '計算しない理由が出ていない');
});

t('[!] 内訳を開くどの経路でも試算を始める (テープ行 / 24 列の表 / 再読み込みで開いたまま)', () => {
  assert.ok(/tape\.insertAdjacentHTML\('afterend', epOpenHtml\(row\)\);\s*epInitPriceCalcs\(tape\.nextElementSibling\);/.test(html), 'テープ行');
  assert.ok(/tr\.insertAdjacentHTML\('afterend', epDetailHtml\(row\)\);\s*epInitPriceCalcs\(tr\.nextElementSibling\);/.test(html), '24 列の表');
  assert.ok(html.includes('epInitPriceCalcs(document);'), '再読み込みで開いたまま');
});

console.log(`\n${passed} 件 PASS`);
