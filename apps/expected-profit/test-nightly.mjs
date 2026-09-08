/**
 * test-nightly.mjs — 夜間ジョブの通し 受入試験 (§8 / §10.1 障害)
 *
 * API も HTTP も叩かない。deps 差し替えで一本通す。
 * 実行: node apps/expected-profit/test-nightly.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-n-'));
process.env.SP_API_MARKETPLACE_ID = 'A1VC38T7YXB528';
process.env.SP_API_SELLER_ID = 'S1';

const { initExpectedProfitDB } = await import('./db.js');
const { pingUrl, runNightly, deadlineOf, feeTargetsFrom } = await import('./nightly.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

console.log('全体終了期限 (§8.4)');

t('[!] 23:30 に始まったら期限は翌日の 06:00 JST', () => {
  const start = new Date('2026-09-07T14:30:00Z');       // JST 23:30
  const d = deadlineOf(start);
  assert.equal(d.toISOString(), '2026-09-07T21:00:00.000Z');  // JST 翌06:00
  assert.ok(d > start);
});

t('期限は必ず未来になる', () => {
  const d = deadlineOf(new Date('2026-09-07T22:00:00Z'));
  assert.ok(d > new Date('2026-09-07T22:00:00Z'));
});

console.log('\n手数料の見積対象');

t('[!] Amazon の FBA/FBM だけを対象にする', () => {
  const rows = [
    { mall: 'amazon', fetch_status: 'ok', fulfillment: 'FBA', mall_item_key: 'a', mall_item_ref: 'B1', price_incl_tax: 1000, points: 0, postage_revenue_incl_tax: 0 },
    { mall: 'amazon', fetch_status: 'ok', fulfillment: null, mall_item_key: 'b', mall_item_ref: 'B2', price_incl_tax: 1000, points: 0 },
    { mall: 'rakuten', fetch_status: 'ok', fulfillment: 'self', mall_item_key: 'c', price_incl_tax: 1000, points: 0 },
  ];
  const targets = feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' });
  assert.equal(targets.length, 1);
  assert.equal(targets[0].seller_sku, 'a');
});

t('[!] 送料を算定基礎に渡す (送料込みなら0)', () => {
  const rows = [
    { mall: 'amazon', fetch_status: 'ok', fulfillment: 'FBM', mall_item_key: 'a', mall_item_ref: 'B1', price_incl_tax: 1198, points: 0, postage_revenue_incl_tax: 230 },
  ];
  const targets = feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' });
  assert.equal(targets[0].in_shipping, 230);
});

t('価格が取れなかった行は対象にしない', () => {
  const rows = [{ mall: 'amazon', fetch_status: 'not_found', fulfillment: 'FBA', mall_item_key: 'a', price_incl_tax: null, points: 0 }];
  assert.equal(feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' }).length, 0);
});

console.log('\n通し (deps 差し替え)');

const db = initExpectedProfitDB();

const warehouseDb = {
  prepare(sql) {
    return {
      all: () => {
        if (sql.includes('m_products')) return [{
          商品コード: 'ne001', 商品名: 'テスト商品', 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE',
          消費税率: 0.1, 税区分: 'STANDARD_10', 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3, 取扱区分: '取扱中',
        }];
        if (sql.includes('shipping_rates')) return [{
          shipping_code: '501', 大分類区分: 'ネコポス', 小分類区分名称: 'ネコポス',
          送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9, 配送関係費合計: 237,
        }];
        if (sql.includes('v_sku_resolved')) return [{ seller_sku: 'sku1', ne_code: 'ne001' }];
        if (sql.includes('f_rakuten_sku_map')) return [{ rakuten_code: 'sku1', ne_code: 'ne001' }];
        return [];
      },
      get: () => ({ v: '2026-09-07 00:00:00' }),
    };
  },
  pragma() {},
  close() {},
};

const rakutenPage = async () => ({
  results: [{ item: { manageNumber: 'item1', variants: {
    sku1: { standardPrice: '1100', payment: { taxIncluded: true, taxRate: '0.1' }, shipping: { postageIncluded: true }, merchantDefinedSkuId: 'sku1' },
  } } }],
  nextCursorMark: null,
});

// 🚨 pastDeadline() は **実時計**を見る (夜間バッチは何時間も動くので、これが正しい)。
//    固定日時の期限を渡すと、その日を過ぎた翌日から試験が落ちる = 書いた日にしか通らない試験になる
//    (実際に翌日 落ちた)。期限そのものを見たい試験だけが、明示的に過去/未来を渡す
const FUTURE_DEADLINE = () => new Date(Date.now() + 6 * 60 * 60 * 1000);

await ta('[!] 一本通ると世代ができて公開される', async () => {
  const published = [];
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async (b) => { published.push(b); return { ok: true }; },
      getPublished: async () => ({ generation_id: published[0]?.generation_id, seq: published[0]?.seq }),
    },
    log: () => {},
  });
  assert.equal(r.ok, true, r.error);
  assert.ok(r.generationId);
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(r.generationId);
  assert.equal(gen.remote_status, 'published');
  assert.equal(gen.local_status, 'validated');
});

await ta('[!] 1モールの取得が失敗しても、他モールで世代を作る (fail-soft)', async () => {
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['amazon', 'rakuten'], skipFees: true,
    fetchDeps: {
      amazon: { getActiveListingsReport: async () => { throw new Error('SP-API 500'); } },
      rakuten: { searchPage: rakutenPage },
    },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async () => ({ ok: true }),
      getPublished: async () => null,   // 確認できない
    },
    log: () => {},
  });
  const amazonStep = r.steps.find(s => s.step === 'fetch:amazon');
  const buildStep = r.steps.find(s => s.step === 'build');
  assert.equal(amazonStep.ok, false);          // Amazon は失敗
  assert.equal(buildStep.ok, true);            // でも世代は作る
  assert.ok(buildStep.rowCount > 0);
});

await ta('[!] 読み戻しで確認できなければ ok にしない', async () => {
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async () => ({ ok: true }),
      getPublished: async () => ({ generation_id: '別の世代', seq: 9999 }),
    },
    log: () => {},
  });
  assert.equal(r.ok, undefined ?? false);
  assert.equal(r.error, 'publish_not_confirmed');
});

await ta('[!] 検証で拒否された世代は転送しない', async () => {
  let sent = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['rakuten'], skipFees: true,
    // 列挙が空 → 世代0行 → 検証で拒否
    fetchDeps: { rakuten: { searchPage: async () => ({ results: [], nextCursorMark: null }) } },
    publishDeps: {
      postChunk: async () => { sent = true; return { ok: true }; },
      postPublish: async () => ({ ok: true }),
      getPublished: async () => null,
    },
    log: () => {},
  });
  assert.equal(sent, false);
  assert.equal(r.error, 'validation_failed');
});

await ta('--skip-publish なら世代だけ作って転送しない', async () => {
  let sent = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['rakuten'], skipFees: true, skipPublish: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: { postChunk: async () => { sent = true; return { ok: true }; } },
    log: () => {},
  });
  assert.equal(r.ok, true);
  assert.equal(r.skippedPublish, true);
  assert.equal(sent, false);
});

await ta('[!] 期限を過ぎたら新しい取得を始めない (全工程に伝播している)', async () => {
  let fetched = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    deadline: new Date('2000-01-01T00:00:00Z'),   // 既に過ぎている
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: async () => { fetched = true; return { results: [], nextCursorMark: null }; } } },
    publishDeps: { postChunk: async () => ({ ok: true }), postPublish: async () => ({ ok: true }), getPublished: async () => null },
    log: () => {},
  });
  assert.equal(fetched, false, '期限後なのに取得を始めた');
  assert.equal(r.error, 'deadline_exceeded');
  const step = r.steps.find(s => s.step === 'fetch:rakuten');
  assert.equal(step.error, 'deadline_exceeded');
});

await ta('[!] 処理の途中で期限を跨いだら、そこで取得を止める (ページごとに見る)', async () => {
  // 1ページ目は期限内、2ページ目で期限を越える
  let page = 0;
  const deadline = new Date(Date.now() + 40);
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline,
    malls: ['rakuten'], skipFees: true, skipPublish: true,
    fetchDeps: { rakuten: { searchPage: async () => {
      page++;
      await new Promise(res => setTimeout(res, 30));   // 1ページ 30ms
      return { results: [{ item: { manageNumber: `p${page}`, variants: {
        v: { standardPrice: '1100', payment: { taxIncluded: true }, shipping: { postageIncluded: true } },
      } } }], nextCursorMark: `c${page}` };
    } } },
    log: () => {},
  });
  const step = r.steps.find(s => s.step === 'fetch:rakuten');
  assert.ok(page <= 3, `期限後もページを取り続けた (${page}ページ)`);
  assert.equal(step.deadlineHit, true, '期限で打ち切ったことが記録されていない');
  assert.equal(step.status, 'partial');
});

await ta('[!] 転送の途中で期限を跨いだら、そこで止めて公開しない', async () => {
  // 世代を作ってから、期限切れの deadline で転送する
  const built = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['rakuten'], skipFees: true, skipPublish: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    log: () => {},
  });
  const { publishToRender } = await import('./publish.js');
  let sent = 0;
  const r = await publishToRender(db, built.generationId, {
    chunkSize: 1,
    deadline: new Date(Date.now() - 1000),   // 既に過ぎている
    postChunk: async () => { sent++; return { ok: true }; },
    postPublish: async () => { throw new Error('publish を呼んではいけない'); },
    getPublished: async () => null,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, 'deadline_exceeded');
  assert.equal(sent, 0);
});


// Amazon の出品レポート1行 (実物と同じ日本語ヘッダ)
const amazonListing = () => ({
  '出品者SKU': 'sku-log', '商品ID': 'B001', '価格': '1980',
  'フルフィルメント・チャンネル': 'AMAZON_JP', 'ステータス': 'Active', 'ポイント': '0',
});

console.log('');
console.log('手数料の再利用が見えるか (SP-API は 0.5 req/s。静かに全件取り直させない)');

await ta('[!] 取り直したら理由の内訳を必ずログに出す', () => {
  // 🚨 これが無いと「なぜ再利用が効かなかったか」を翌朝に追えない
  const logs = [];
  return runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['amazon'], skipPublish: true,
    fetchDeps: { amazon: { getActiveListingsReport: async () => ({ listings: [amazonListing()] }) } },
    feeDeps: {
      sleepMs: 0,
      callFeesApi: async (body) => body.map(b => ({
        Status: 'Success',
        FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier, SellerId: 'S1' },
        FeesEstimate: {
          TotalFeesEstimate: { CurrencyCode: 'JPY', Amount: 546 },
          // 🚨 FBA の出品なので FBAFees を返す。無いと missing_fba_fee になり再利用されない (正しい挙動)
          FeeDetailList: [
            { FeeType: 'ReferralFee', FeeAmount: { Amount: 84 }, FinalFee: { Amount: 84 }, FeePromotion: { Amount: 0 } },
            { FeeType: 'FBAFees', FeeAmount: { Amount: 462 }, FinalFee: { Amount: 462 }, FeePromotion: { Amount: 0 } },
          ],
        },
      })),
    },
    log: (m) => logs.push(String(m)),
  }).then(() => {
    const line = logs.find(l => l.includes('手数料:'));
    assert.ok(line, '手数料の行が無い');
    assert.match(line, /理由=/, `理由の内訳が出ていない: ${line}`);
    assert.match(line, /missing/, `初回は「見積が無い」のはず: ${line}`);
  });
});

await ta('[!] 取り直しが 0 件なら理由は出さない (毎晩ノイズにしない)', () => {
  const logs = [];
  return runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['amazon'], skipPublish: true,
    fetchDeps: { amazon: { getActiveListingsReport: async () => ({ listings: [amazonListing()] }) } },
    feeDeps: { sleepMs: 0, callFeesApi: async () => { throw new Error('叩かせない'); } },
    log: (m) => logs.push(String(m)),
  }).then(() => {
    const line = logs.find(l => l.includes('手数料:'));
    assert.ok(line);
    assert.ok(line.includes('再利用1'), `再利用されていない: ${line}`);
    assert.ok(!line.includes('理由='), `取り直し0なのに理由が出ている: ${line}`);
  });
});


await ta('[!] 失敗して待ち中の対象の数もログに出す (増え続けたら値かカタログの問題)', async () => {
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const failApi = async (body) => body.map(b => ({
    Status: 'ClientError',
    FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier },
    Error: { Message: 'There is an client-side error.' },
  }));
  const runOnce = (logs) => runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['amazon'], skipPublish: true,
    fetchDeps: { amazon: { getActiveListingsReport: async () => ({ listings: [amazonListing()] }) } },
    feeDeps: { sleepMs: 0, callFeesApi: failApi, now: () => new Date('2026-09-08T00:00:00Z') },
    log: (m) => logs.push(String(m)),
  });

  const first = [];
  await runOnce(first);
  assert.match(first.find(l => l.includes('手数料:')), /失敗1/, '1晩目は失敗として数える');

  const second = [];
  await runOnce(second);
  const line = second.find(l => l.includes('手数料:'));
  assert.match(line, /失敗待ち1/, `待ち中の数が出ていない: ${line}`);
  assert.ok(!/失敗1/.test(line.replace(/失敗待ち1/, '')), `待ち中なのに叩いている: ${line}`);
  db.exec('DELETE FROM amazon_fee_failure');
});


await ta('[!] 再利用が効いていない警告は「キャッシュに当たらなかった数」で出す', async () => {
  // 🚨 異常判定は失敗待ちを除く**前**の数、表示は除いた**後**の数、だと
  //    「0/4 件を取り直そうとした」という意味の通らない警告になる (Codex R9-6)
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const listing = (sku, price) => ({
    '出品者SKU': sku, '商品ID': 'B00000000' + sku.slice(-1), '価格': String(price),
    'フルフィルメント・チャンネル': 'DEFAULT', 'ステータス': 'Active', 'ポイント': '0',
  });
  const listings = [listing('okA', 1000), listing('bad1', 1100), listing('bad2', 1200), listing('bad3', 1300)];
  // okA だけ成功、bad* は失敗する API
  const api = async (body) => body.map(b => {
    const sku = String(b.FeesEstimateRequest.Identifier).split('|')[0];
    if (sku === 'okA') {
      return {
        Status: 'Success',
        FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier, SellerId: 'S1' },
        FeesEstimate: {
          TotalFeesEstimate: { CurrencyCode: 'JPY', Amount: 84 },
          FeeDetailList: [{ FeeType: 'ReferralFee', FeeAmount: { Amount: 84 }, FinalFee: { Amount: 84 }, FeePromotion: { Amount: 0 } }],
        },
      };
    }
    return {
      Status: 'ClientError',
      FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier },
      Error: { Message: 'bad' },
    };
  });
  const run = (logs) => runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'), deadline: FUTURE_DEADLINE(),
    malls: ['amazon'], skipPublish: true,
    fetchDeps: { amazon: { getActiveListingsReport: async () => ({ listings }) } },
    feeDeps: { sleepMs: 0, callFeesApi: api, now: () => new Date('2026-09-08T00:00:00Z') },
    log: (m) => logs.push(String(m)),
  });

  await run([]);                       // 1晩目: okA が保存され、bad* は失敗記録に入る
  const logs = [];
  await run(logs);                     // 2晩目: bad* は失敗待ち → 取り直し候補は 0 になる
  const warn = logs.find(l => l.includes('再利用が効いていない'));
  assert.ok(warn, `警告が出ていない: ${JSON.stringify(logs)}`);
  assert.match(warn, /3\/4 件がキャッシュに当たらなかった/, `母集団が混ざっている: ${warn}`);
  assert.ok(!/^.*0\/4/.test(warn), `取り直し候補の数 (0) を出してはいけない: ${warn}`);
  db.exec('DELETE FROM amazon_fee_failure');
});


console.log('');
console.log('監視への報告 (Codex R15: 失敗が成功として記録されていた)');

t('[!] status はクエリで送る (body だと受け口が読まず ok 扱いになる)', () => {
  // 🚨 受け口 (apps/jobs-monitor/router.js) は req.query.status しか見ない。
  //    body に入れると省略扱い → 既定の 'ok' になり、**公開失敗が成功として記録される**
  const u = pingUrl('expected-profit-nightly', 'fail', 'RENDER_MIRROR_URL not configured',
    { JOBS_MONITOR_URL: 'https://portal.test' });
  assert.ok(u.includes('status=fail'), `status がクエリに無い: ${u}`);
  assert.ok(u.includes('note='), `note がクエリに無い: ${u}`);
  assert.ok(u.startsWith('https://portal.test/apps/jobs-monitor/ping/expected-profit-nightly?'), u);
});

t('[!] JOBS_MONITOR_URL に末尾のパスが付いていても origin だけを使う', () => {
  const u = pingUrl('job1', 'ok', null, { JOBS_MONITOR_URL: 'https://portal.test/apps/mirror' });
  assert.ok(u.startsWith('https://portal.test/apps/jobs-monitor/ping/job1?'), u);
});

t('note は200文字まで (受け口の上限に合わせる)', () => {
  const u = pingUrl('job1', 'fail', 'あ'.repeat(500), { JOBS_MONITOR_URL: 'https://portal.test' });
  const note = new URL(u).searchParams.get('note');
  assert.equal(note.length, 200);
});

t('URL が無ければ空 (報告しない)', () => {
  assert.equal(pingUrl('job1', 'ok', null, {}), '');
  assert.equal(pingUrl('job1', 'ok', null, { JOBS_MONITOR_URL: 'ごみ' }), '');
  assert.equal(pingUrl('job1', 'ok', null, { JOBS_MONITOR_URL: 'file:///tmp/x' }), '');
});

t('ジョブIDはURLに入れられる形に逃がす', () => {
  const u = pingUrl('a/b c', 'ok', null, { JOBS_MONITOR_URL: 'https://portal.test' });
  assert.ok(u.includes('/ping/a%2Fb%20c?'), u);
});

console.log('');
console.log('監視への報告 (ランナーとの終了コードの約束)');

// 🚨 ランナー (run-expected-profit-nightly.ps1) は終了コードで「もう報告したか」を受け取る。
//    0=ok / 3=失敗だが報告済み / 1=失敗して報告もできていない。
//    ここが崩れると、ランナーが重ねて fail ping を打ち、具体的な理由を汎用文言で上書きする
const PAST_DEADLINE = () => new Date(Date.now() - 1000);

await ta('[!] 報告できたら reported=true (ランナーは重ねて打たない)', async () => {
  const calls = [];
  const origFetch = globalThis.fetch;
  process.env.JOBS_MONITOR_URL = 'https://portal.test';
  process.env.JOBS_MONITOR_TOKEN = 'tok';
  globalThis.fetch = async (url) => { calls.push(String(url)); return { ok: true, status: 200 }; };
  try {
    const r = await runNightly({ db, warehouseDb, deadline: PAST_DEADLINE(), malls: [], skipFees: true, log: () => {} });
    assert.equal(r.ok, false);
    assert.equal(r.reported, true, '報告したのに reported が立っていない');
    assert.ok(calls[0] && calls[0].includes('status=fail'), calls[0]);
  } finally {
    globalThis.fetch = origFetch;
    delete process.env.JOBS_MONITOR_URL;
    delete process.env.JOBS_MONITOR_TOKEN;
  }
});

await ta('[!] 受け口に断られたら reported=false (ランナーが代わりに打つ)', async () => {
  const origFetch = globalThis.fetch;
  process.env.JOBS_MONITOR_URL = 'https://portal.test';
  process.env.JOBS_MONITOR_TOKEN = 'tok';
  globalThis.fetch = async () => ({ ok: false, status: 401 });
  try {
    const r = await runNightly({ db, warehouseDb, deadline: PAST_DEADLINE(), malls: [], skipFees: true, log: () => {} });
    assert.equal(r.reported, false, '401 で弾かれたのに報告済みにしている');
  } finally {
    globalThis.fetch = origFetch;
    delete process.env.JOBS_MONITOR_URL;
    delete process.env.JOBS_MONITOR_TOKEN;
  }
});

await ta('[!] 監視の設定が無いときも reported=false', async () => {
  const r = await runNightly({ db, warehouseDb, deadline: PAST_DEADLINE(), malls: [], skipFees: true, log: () => {} });
  assert.equal(r.reported, false);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
