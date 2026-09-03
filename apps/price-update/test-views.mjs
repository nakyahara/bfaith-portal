/**
 * test-views.mjs — 画面テンプレートが壊れていないかの smoke
 *
 * EJS の構文エラーや、inline script の中に script 終了タグ相当の文字列が混ざる事故
 * (HTML パーサがそこで閉じてしまい、JS がページに丸見えになる — product-links #944 の再発防止) を
 * デプロイ前に落とす。
 *
 * 実行: node apps/price-update/test-views.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ejs from 'ejs';
import { toJst, TO_JST_CLIENT_SRC } from './format.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));

// 画面が使うヘルパー。router.js が render のたびに渡しているものと同じ。
// ここで一括して足し、テスト側の呼び出しごとに書かなくて済むようにする。
// ★これで足すので「router が渡し忘れた」事故はテンプレのテストでは出ない → 末尾で別に見る
const VIEW_HELPERS = { toJst, toJstClientSrc: TO_JST_CLIENT_SRC };
const _ejsRender = ejs.render.bind(ejs);
ejs.render = (tpl, data, opts) => _ejsRender(tpl, { ...VIEW_HELPERS, ...data }, opts);
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

const OPEN_TAG = '<' + 'script';
const CLOSE_TAG = '<' + '/script' + '>';

function checkTagBalance(html, name) {
  const opens = (html.match(new RegExp(OPEN_TAG, 'gi')) || []).length;
  const closes = (html.match(new RegExp(CLOSE_TAG, 'gi')) || []).length;
  ok(opens === closes, `${name}: script の開始/終了タグ数が一致 (開始 ${opens} / 終了 ${closes})`);
}

const cases = [
  ['index.ejs', {
    title: '価格一括改定', displayName: 'テスト', isAdmin: true,
    limits: { maxNeCodes: 20, maxSkuRows: 100, maxRowsPerNeCode: 50 },
    runs: [{ run_id: 'pur-x', created_at: '2026-08-28T01:02:03.000Z', created_by: 't@example.com', kind: 'normal', note: 'メモ', op_count: 3 }],
  }],
  ['index.ejs', {
    title: '価格一括改定', displayName: 'テスト', isAdmin: false,
    limits: { maxNeCodes: 20, maxSkuRows: 100, maxRowsPerNeCode: 50 }, runs: [],
  }],
  ['run.ejs', {
    title: '履歴', displayName: 'テスト', isAdmin: false,
    run: {
      run_id: 'pur-x', created_at: '2026-08-28T01:02:03.000Z', created_by: 't@example.com',
      kind: 'normal', note: null, neCodes: ['abc-001'], limits: {},
      operations: [
        {
          operation_id: 'puo-1', mall: 'rakuten', ne_code: 'abc-001', row_kind: 'single',
          product_name: 'テスト商品', listing_code: 'abc-001', sku_code: 'sku-a', confidence: 'confirmed',
          price_source: '楽天RMS (ライブ)', expected_current_price: 1000, new_price: 1200,
          initial_state: 'previewed', state: 'previewed',
          guard_json: JSON.stringify({ blocks: [], warns: ['粗利率が低いです'] }), product_url: 'https://example.com',
        },
        {
          operation_id: 'puo-2', mall: 'amazon', ne_code: 'abc-001', row_kind: 'single',
          product_name: 'テスト商品', listing_code: 'AMZ-1', sku_code: null, confidence: 'rule',
          price_source: 'snapshot', expected_current_price: null, new_price: null,
          initial_state: 'manual_required', state: 'manual_done', guard_json: null, product_url: null,
        },
      ],
      events: [{ at: '2026-08-28T01:02:03.000Z', actor: 't@example.com', event: 'run_created', operation_id: null, detail_json: '{}' }],
    },
  }],
];

for (const [name, data] of cases) {
  const file = path.join(HERE, 'views', name);
  let html = null;
  try {
    html = ejs.render(fs.readFileSync(file, 'utf8'), data, { filename: file });
    ok(true, `${name}: レンダリングできる`);
  } catch (e) {
    ok(false, `${name}: レンダリング失敗 — ${e.message}`);
    continue;
  }
  checkTagBalance(html, name);
  ok(!html.includes('<%'), `${name}: 未処理の EJS タグが残っていない`);
}

// テンプレート原文にも「JS の中に終了タグ」が無いこと (コメント内でも HTML パーサは閉じる)
for (const name of ['index.ejs', 'run.ejs']) {
  const src = fs.readFileSync(path.join(HERE, 'views', name), 'utf8');
  const inScript = src.split(OPEN_TAG).slice(1).map((s) => s.split(CLOSE_TAG)[0]);
  ok(inScript.every((s) => !s.includes(CLOSE_TAG)), `${name}: inline script の中に終了タグ文字列が無い`);
}

// 実行 (M2) の画面: 状態が日本語で出ること・実行カードがあること
{
  const file = path.join(HERE, 'views', 'run.ejs');
  const html = ejs.render(fs.readFileSync(file, 'utf8'), {
    title: '履歴', displayName: 'テスト', isAdmin: false,
    run: {
      run_id: 'pur-m2', created_at: '2026-09-01T01:02:03.000Z', created_by: 't@example.com',
      kind: 'normal', note: null, neCodes: ['abc-001'], limits: {},
      operations: ['confirmed', 'noop', 'conflict', 'unknown', 'skipped', 'blocked'].map((st, i) => ({
        operation_id: 'puo-' + i, mall: 'rakuten', ne_code: 'abc-001', row_kind: 'single',
        product_name: 'テスト商品', listing_code: 'abc-001', sku_code: 'sku-' + i, confidence: 'confirmed',
        price_source: '楽天RMS (ライブ)', expected_current_price: 1000, new_price: 1200,
        initial_state: 'previewed', state: st, guard_json: null, product_url: null,
      })),
      events: [{ at: '2026-09-01T01:02:03.000Z', actor: 't@example.com', event: 'confirmed', operation_id: 'puo-0', detail_json: '{}' }],
    },
  }, { filename: file });
  ok(html.includes('更新済み (確認ずみ)') && html.includes('結果が不明'), '★M2 の状態が日本語で出る (confirmed / unknown)');
  ok(!/>confirmed</.test(html) && !/>unknown</.test(html), '生の状態名がそのまま出ていない');
  ok(html.includes('実行する') && html.includes('exec-btn'), '実行カード (確認欄+ボタン) がある');
  ok(html.includes('自動では戻せません') || html.includes('価格を実際に書き換えます'), '取り消せないことを画面で伝えている');
  checkTagBalance(html, 'run.ejs (M2)');
}

// 値に細工があっても画面から抜け出せないこと (run_id は script に直書きしない)
{
  const file = path.join(HERE, 'views', 'run.ejs');
  const evil = 'pur-x' + CLOSE_TAG + OPEN_TAG + '>alert(1)' + CLOSE_TAG + '"><img src=x onerror=alert(1)>';
  const html = ejs.render(fs.readFileSync(file, 'utf8'), {
    title: '履歴', displayName: evil, isAdmin: false,
    run: {
      run_id: evil, created_at: '2026-09-01T01:02:03.000Z', created_by: evil,
      kind: 'normal', note: evil, neCodes: [evil], limits: {},
      operations: [{
        operation_id: evil, mall: 'rakuten', ne_code: evil, row_kind: 'single',
        product_name: evil, listing_code: evil, sku_code: evil, confidence: 'confirmed',
        price_source: evil, expected_current_price: 1000, new_price: 1200,
        initial_state: 'previewed', state: 'previewed', guard_json: null, product_url: null,
      }],
      events: [{ at: '2026-09-01T01:02:03.000Z', actor: evil, event: 'run_created', operation_id: evil, detail_json: evil }],
    },
  }, { filename: file });
  checkTagBalance(html, 'run.ejs (細工つき)');
  ok(!html.includes('<img src=x'), '★細工した文字列がそのまま HTML として出ていない');
  ok(!html.includes('alert(1)' + CLOSE_TAG), 'script を抜け出せていない');
}

// 出品ページのリンクは http(s) だけ
{
  const file = path.join(HERE, 'views', 'run.ejs');
  const render = (url) => ejs.render(fs.readFileSync(file, 'utf8'), {
    title: '履歴', displayName: 'テスト', isAdmin: false,
    run: {
      run_id: 'pur-url', created_at: '2026-09-01T01:02:03.000Z', created_by: 't@example.com',
      kind: 'normal', note: null, neCodes: ['abc-001'], limits: {},
      operations: [{
        operation_id: 'puo-1', mall: 'rakuten', ne_code: 'abc-001', row_kind: 'single',
        product_name: '商品', listing_code: 'abc-001', sku_code: 'sku-a', confidence: 'confirmed',
        price_source: '楽天RMS (ライブ)', expected_current_price: 1000, new_price: 1200,
        initial_state: 'previewed', state: 'previewed', guard_json: null, product_url: url,
      }],
      events: [],
    },
  }, { filename: file });
  const LINK = 'target=' + String.fromCharCode(34) + '_blank' + String.fromCharCode(34);
  ok(render('https://item.rakuten.co.jp/x/y/').includes(LINK), '正しい URL はリンクになる');
  ok(!render('javascript:alert(1)').includes(LINK), '★javascript: の URL はリンクにしない');
  ok(!render('javascript:alert(1)').includes('javascript:'), 'href に入れない');
  ok(!render('  JavaScript:alert(1)').includes(LINK), '前後の空白・大文字でもすり抜けない');
}

// 復旧 run: 戻せなかった行を画面に出す (黙って消さない)
{
  const file = path.join(HERE, 'views', 'run.ejs');
  const render = (events) => ejs.render(fs.readFileSync(file, 'utf8'), {
    title: '履歴', displayName: 'テスト', isAdmin: false,
    run: {
      run_id: 'pur-rec', created_at: '2026-09-01T01:02:03.000Z', created_by: 't@example.com',
      kind: 'recovery', source_run_id: 'pur-src', note: null, neCodes: ['abc-001'], limits: {},
      operations: [{
        operation_id: 'puo-1', mall: 'rakuten', ne_code: 'abc-001', row_kind: 'single',
        product_name: '商品', listing_code: 'mn-1', sku_code: 'sku-a', confidence: 'confirmed',
        price_source: '楽天RMS (ライブ)', expected_current_price: 578, new_price: 577,
        initial_state: 'previewed', state: 'previewed', guard_json: null, product_url: null,
      }],
      events,
    },
  }, { filename: file });

  const withMiss = render([{ at: '2026-09-01T01:02:03.000Z', actor: 't@example.com', event: 'recovery_incomplete',
    operation_id: null, detail_json: JSON.stringify({ notRestored: [
      { operationId: 'puo-x', neCode: 'abc-002', mall: 'rakuten', listingCode: 'mn-GONE', skuCode: 'sku-z',
        reason: 'いま引き当て直すと同じ出品が見つかりません' }] }) }]);
  ok(withMiss.includes('戻せなかった行が 1 行'), '★戻せなかった行を画面の先頭で知らせる');
  ok(withMiss.includes('mn-GONE') && withMiss.includes('abc-002'), 'どの出品が戻っていないか出す');
  ok(withMiss.includes('復旧 run') && withMiss.includes('pur-src'), '復旧 run だと分かり、元の履歴へ行ける');

  const clean = render([]);
  const HEAD = '戻せなかった行が ';
  ok(!clean.includes(HEAD), '戻せなかった行が無ければ出さない');
  // 壊れた記録でもレンダリングが落ちない
  const broken = render([{ at: '2026-09-01T01:02:03.000Z', actor: 't', event: 'recovery_incomplete', operation_id: null, detail_json: '{壊れ' }]);
  ok(!broken.includes(HEAD), '壊れた記録は無視して画面を出す');
  checkTagBalance(withMiss, 'run.ejs (復旧)');
}

// ★実行ボタンの文言をモール名で決め打ちしない (Yahoo の履歴で「楽天へ送る」と出ていた)
{
  const src = fs.readFileSync(path.join(HERE, 'views', 'run.ejs'), 'utf8');
  // 画面に直接書かれた固定文言 (テンプレート側) にモール名が無いこと
  const template = src.split(OPEN_TAG)[0];
  ok(!/楽天へ送る|Yahooへ送る/.test(template), '★ボタンの文言にモール名を直書きしない');
  ok(/exec-btn'\)\.textContent/.test(src), '送るモールから文言を作っている');
  ok(/sendMalls/.test(src) && /g\.enabled/.test(src), '★実際に送るモール (kill switch が有効なもの) だけを名前に出す');
}

console.log('\n── 「1色のつもりが全色に効く」を画面から消さない ──');
{
  const idx = fs.readFileSync(path.join(HERE, 'views', 'index.ejs'), 'utf8');
  const run = fs.readFileSync(path.join(HERE, 'views', 'run.ejs'), 'utf8');

  ok(/sharedNote/.test(idx), '引き当て画面が sharedNote を出す');
  ok(/pu-warn/.test(idx), '注意書き用のスタイルがある (ふつうの注記と見分けがつく)');
  ok(/matchedSubCode/.test(idx), '★当たった色の個別商品コードを見せる (送り先とは別物)');

  ok(/exec-warn/.test(run), '送るボタンの手前に注意書きの枠がある');
  ok(/info.warnings/.test(run), '実行前チェックの warnings を使っている');
  ok(/pu-warn/.test(run), '履歴の一覧にも注意書きのスタイルがある');
  // ★注意書きは「ブロック理由が無いとき」だけ出す、という書き方をしていないこと。
  //   ブロックされた行こそ、なぜ止まったか以外の事実も見えていないといけない
  ok(!run.includes(String.fromCharCode(125) + " else if (guard && guard.warns"),
    '★ブロック理由があっても注意書きを消さない (else if にしない)');
}

console.log('\n── 日時は JST で出す (DB は UTC) ──');
{
  // 2026-09-02T10:40:00Z = 日本時間 2026-09-02 19:40。
  // ★9時間ずれたまま出していたため、中原さんが run を探して取り違えかけた (2026-09-03)
  const file = path.join(HERE, 'views', 'index.ejs');
  const mkRuns = (at) => ({
    title: '価格一括改定', displayName: 'テスト', isAdmin: false,
    limits: { maxNeCodes: 20, maxSkuRows: 100, maxRowsPerNeCode: 50 },
    runs: [{ run_id: 'pur-x', created_at: at, created_by: 't@example.com', kind: 'normal', note: null, op_count: 1 }],
  });
  const html = ejs.render(fs.readFileSync(file, 'utf8'), mkRuns('2026-09-02T10:40:00.000Z'), { filename: file });
  ok(html.includes('2026-09-02 19:40'), '一覧: UTC 10:40 が JST 19:40 で出る');
  ok(!html.includes('2026-09-02 10:40'), '一覧: UTC のままの表記が残っていない');
  ok(html.includes('日時 (JST)'), '一覧: 見出しに JST と書いてある');

  // 日をまたぐ側 (いちばん取り違えやすい): UTC 15:30 = 翌日 00:30 JST
  const html2 = ejs.render(fs.readFileSync(file, 'utf8'), mkRuns('2026-09-02T15:30:00.000Z'), { filename: file });
  ok(html2.includes('2026-09-03 00:30'), '★一覧: 日をまたぐ時刻も正しく翌日になる');

  const rfile = path.join(HERE, 'views', 'run.ejs');
  const rhtml = ejs.render(fs.readFileSync(rfile, 'utf8'), {
    title: '履歴', displayName: 'テスト', isAdmin: false,
    run: {
      run_id: 'pur-z', created_at: '2026-09-02T10:40:00.000Z', created_by: 't@example.com',
      kind: 'normal', note: null, neCodes: ['abc-001'], limits: {}, operations: [],
      events: [{ at: '2026-09-02T15:30:00.000Z', actor: 't@example.com', event: 'run_created', operation_id: null, detail_json: '{}' }],
    },
  }, { filename: rfile });
  ok(rhtml.includes('2026-09-02 19:40:00'), '詳細: 見出しの日時が JST (秒まで)');
  ok(rhtml.includes('2026-09-03 00:30:00'), '詳細: イベントの日時も JST');
  ok(/function toJst/.test(rhtml), '★ブラウザ側にも同じ toJst が入っている (claim の表示で使う)');
}

console.log('\n── router が画面へヘルパーを渡している ──');
{
  // ★上の shim で足しているので、渡し忘れはテンプレのテストでは出ない。ここで直接見る
  const router = fs.readFileSync(path.join(HERE, 'router.js'), 'utf8');
  ok(router.includes("from './format.js'"), 'router が format.js を読み込んでいる');
  const renders = router.split('res.render(view(').slice(1);
  ok(renders.length === 2, `router の画面 render は 2 箇所 (実際 ${renders.length})`);
  ok(renders.every((r) => /\btoJst\b/.test(r.slice(0, 400))), '★どの画面にも toJst を渡している');
  ok(router.includes('toJstClientSrc'), 'run.ejs にブラウザ用のソースも渡している');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
