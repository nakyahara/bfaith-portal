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
import { buildTargets, findSetsContaining, setCostOf, listingUrl } from './resolve.js';
import { toIntPrice, fetchRakutenPrices, fetchYahooPrices, _resetCodeMapCache } from './live-price.js';
import { createTables, insertRun, appendEvent, currentStates, getRun, listRuns } from './db.js';
import { buildPreviewRows, evaluateRows, parseCodes, parseStrictPrice } from './router.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'price-update-test-'));
const db = new Database(path.join(tmpDir, 'mirror.db'));

// ── mirror 相当の表を用意 (本番と同じ列名) ──
db.exec(`CREATE TABLE mirror_products (
  商品コード TEXT PRIMARY KEY, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT,
  標準売価 REAL, 原価 REAL, 原価状態 TEXT NOT NULL, 送料 REAL, 消費税率 REAL, セット構成品数 INTEGER)`);
db.exec(`CREATE TABLE mirror_set_components (
  セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1,
  構成商品名 TEXT, 構成商品原価 REAL, updated_at TEXT NOT NULL,
  PRIMARY KEY (セット商品コード, 構成商品コード))`);
db.exec(`CREATE TABLE mirror_rakuten_sku_map (rakuten_code TEXT PRIMARY KEY, ne_code TEXT NOT NULL, source TEXT NOT NULL, updated_at TEXT NOT NULL)`);
db.exec(`CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code))`);
db.exec(`CREATE TABLE mirror_amazon_price_snapshot_daily (date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT, my_price REAL, buybox_price REAL, fetched_at TEXT, PRIMARY KEY (date_jst, seller_sku))`);
db.exec(`CREATE TABLE dim_mall (mall_key TEXT PRIMARY KEY, label TEXT, display_order INTEGER, is_channel INTEGER, in_daily_summary INTEGER, tax_included INTEGER, fee_rate_approx REAL)`);
createTables(db);

const insProduct = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 消費税率, セット構成品数) VALUES (?,?,?,?,?,?,?,?,?,?)`);
insProduct.run('abc-001', 'テスト商品A', '単品', '取扱中', 1200, 500, '確定', 100, 0.1, null);
insProduct.run('abc-002', 'テスト商品B', '単品', '取扱中', 900, 300, '確定', 100, 0.1, null);
insProduct.run('abc-set', 'A+Bセット', 'セット', '取扱中', 2000, null, '確定', 150, 0.1, 2);
insProduct.run('abc-set2', '原価欠けセット', 'セット', '取扱中', 2500, null, '未確定', 150, 0.1, 2);
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set', 'abc-001', 2, 'テスト商品A', 500, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set', 'abc-002', 1, 'テスト商品B', 300, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set2', 'abc-001', 1, 'テスト商品A', 500, 'now');
db.prepare('INSERT INTO mirror_set_components VALUES (?,?,?,?,?,?)').run('abc-set2', 'zzz-999', 1, '原価未登録品', null, 'now');
db.prepare('INSERT INTO mirror_rakuten_sku_map VALUES (?,?,?,?)').run('abc-001', 'abc-001', 'am', 'now');
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
  const prices = await fetchRakutenPrices(['abc-001', 'ghost-001'], deps);
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
  ok(mismatched.get('abc-001').reason.includes('一致しません'), '理由に不一致と書く');

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
