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

const HERE = path.dirname(fileURLToPath(import.meta.url));
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

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
