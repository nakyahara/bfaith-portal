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
// 🚨 状態の一覧は正本 (easyship-rates.js) から取る。ここに写すと足し忘れを検出できない
import { EASYSHIP_STATUSES } from './easyship-rates.js';
// 🚨 取扱中を表す値も正本 (query.js) から取る。画面に文字列を写しているので、
//    片方だけ変えると「取扱中なのに止まって見える」行ができる
import { HANDLING_ACTIVE } from './query.js';

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
  // 商品名・出品コードは必ず escape する
  assert.ok(section.includes('escapeHtml(r.mall_item_key)'));
  assert.ok(section.includes("escapeHtml(r.product_name || '')"));
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
  const sandbox = {
    escapeHtml: (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
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

t('[!] 一覧の商品名は「…」で切れる (行内要素のままだと隣の列に重なる)', () => {
  const rule = cssRule('ep-tape-name');
  assert.ok(rule, '.ep-tape-name の定義が無い');
  assert.match(rule, /text-overflow:\s*ellipsis/, '「…」で切る指定が無い');
  assert.match(rule, /display:\s*block/,
    '.ep-tape-name が行内要素のまま。overflow / text-overflow が効かず、長い商品名が右の列に重なる');
});

t('[!] 除外理由の行も同じ (長い理由がはみ出す)', () => {
  const rule = cssRule('ep-tape-why');
  assert.match(rule, /text-overflow:\s*ellipsis/);
  assert.match(rule, /display:\s*block/, '.ep-tape-why も行内要素のままでは切れない');
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

t('[!] 世代に在庫・取扱が入っていなければ、0 件を「該当なし」と読ませない', () => {
  // 2026-09-09 の夜に実際に起きた形 (夜間バッチが古い版で動いて列が入らなかった)
  const out = renderTape([], { stock: 'in_stock', summary: { total: 0, handlingKnown: 0, stockKnown: 0 } });
  assert.match(out, /この世代には[^<]*在庫数[^<]*が 1 件も入っていません/,
    '「入っていない」と書いていない。絞り込みが 0 件になったのを該当なしと読んでしまう');
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

console.log(`\n${passed} 件 PASS`);
