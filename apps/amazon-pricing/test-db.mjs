/**
 * test-db.mjs — ap_* 表・追記のみの強制・方針の履歴・判定 run・読み取りモデルの検証。
 *
 * 実行: node apps/amazon-pricing/test-db.mjs
 */
import Database from 'better-sqlite3';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createTables, savePolicy, getPolicy, listPolicyEvents, addReview, reviewStats, evaluationsOfRun, evaluationsForSku, runForSnapshot, listRuns, normalizePolicyPatch } from './db.js';
import { loadListings, loadListing, priceHistory, mirrorTablesAvailable, REQUIRED_MIRROR_TABLES } from './read-model.js';
import { runEvaluation } from './evaluate.js';
import { RULE_VERSION } from './engine.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const throws = (fn, needle, label) => {
  try { fn(); ok(false, `${label} — 例外が出なかった`); }
  catch (e) { ok(String(e.message).includes(needle), `${label} — ${e.message}`); }
};

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ap-test-'));
const db = new Database(path.join(tmp, 'mirror.db'));
db.pragma('foreign_keys = ON');

// ─── mirror 表の最小フィクスチャ (本番の DDL から必要な列だけ) ───
db.exec(`CREATE TABLE mirror_amazon_sku_fees (seller_sku TEXT PRIMARY KEY, asin TEXT, fulfillment_channel TEXT,
  referral_fee REAL, referral_fee_rate REAL, fba_fee REAL, variable_closing_fee REAL, per_item_fee REAL, total_fee REAL, price_used REAL, fetched_at TEXT NOT NULL)`);
db.exec(`CREATE TABLE mirror_amazon_price_snapshot_daily (date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT NOT NULL DEFAULT '', channel TEXT,
  my_price REAL, buybox_price REAL, buybox_is_mine INTEGER, fetched_at TEXT, source_run_id TEXT NOT NULL, source_row_hash TEXT NOT NULL, synced_at TEXT NOT NULL, PRIMARY KEY (date_jst, seller_sku))`);
db.exec(`CREATE TABLE mirror_sku_resolved (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, quantity INTEGER NOT NULL, source TEXT NOT NULL, 商品名 TEXT, source_updated_at TEXT, sort_order INTEGER NOT NULL DEFAULT 0, synced_at TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code))`);
db.exec(`CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL, 送料 REAL, 消費税率 REAL, updated_at TEXT NOT NULL)`);
db.exec(`CREATE TABLE mirror_amazon_finance_sku_daily (date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin_norm TEXT NOT NULL DEFAULT '', units_net_sold REAL NOT NULL DEFAULT 0, sales_principal_jpy REAL NOT NULL DEFAULT 0, cost_status TEXT NOT NULL, source_run_id TEXT NOT NULL, source_row_hash TEXT NOT NULL, synced_at TEXT NOT NULL, PRIMARY KEY (date_jst, seller_sku, asin_norm))`);

const T = '2026-09-07T00:00:00.000Z';
const fees = db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee_rate, fba_fee, per_item_fee, variable_closing_fee, fetched_at) VALUES (?,?,?,?,?,?,?,?)`);
fees.run('PR_FBA1 ', 'B000FBA1', 'FBA', 0.10, 400, 0, 0, T);      // ★大文字 + 末尾空白 (突合の片側正規化を確かめる)
fees.run('pr_set1', 'B000SET1', 'FBA', 0.10, 500, 0, 0, T);        // セット (構成品 2 点、片方の原価が無い)
fees.run('pr_fbm1', 'B000FBM1', 'FBM', 0.15, null, 0, 0, T);       // 自己発送
fees.run('pr_noprice', 'B000NOPR', 'FBA', 0.10, 300, 0, 0, T);     // 価格スナップショット無し
const snap = db.prepare(`INSERT INTO mirror_amazon_price_snapshot_daily (date_jst, seller_sku, asin, my_price, buybox_price, buybox_is_mine, source_run_id, source_row_hash, synced_at) VALUES (?,?,?,?,?,?,?,?,?)`);
snap.run('2026-09-06', 'PR_FBA1 ', 'B000FBA1', 2100, 1950, 0, 'r', 'h', T);
snap.run('2026-09-07', 'PR_FBA1 ', 'B000FBA1', 2000, 1900, 0, 'r', 'h', T);
snap.run('2026-09-07', 'pr_set1', 'B000SET1', 3000, 2500, 0, 'r', 'h', T);
snap.run('2026-09-07', 'pr_fbm1', 'B000FBM1', 1500, 1400, 1, 'r', 'h', T);
const res = db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, synced_at) VALUES (?,?,?,?,?,?)`);
res.run('pr_fba1', 'ne-a', 1, 'master', 'A商品', T);
res.run('pr_set1', 'ne-a', 2, 'master', 'A商品', T);
res.run('pr_set1', 'ne-b', 1, 'master', 'B商品 (原価なし)', T);
res.run('pr_fbm1', 'ne-c', 1, 'master', 'C商品', T);
const prod = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価, 原価状態, 送料, 消費税率, updated_at) VALUES (?,?,?,?,?,?,?,?)`);
prod.run('ne-a', 'A商品', '単品', 1000, 'COMPLETE', 200, 0.10, T);      // 税込 1100
prod.run('ne-b', 'B商品', '単品', null, 'MISSING', 200, 0.10, T);
prod.run('ne-c', 'C商品', '単品', 500, 'OVERRIDDEN', 300, 0.08, T);    // 税込 540、送料 300
const fin = db.prepare(`INSERT INTO mirror_amazon_finance_sku_daily (date_jst, seller_sku, units_net_sold, sales_principal_jpy, cost_status, source_run_id, source_row_hash, synced_at) VALUES (?,?,?,?,?,?,?,?)`);
fin.run('2026-09-01', 'pr_fba1', 3, 6000, 'complete', 'r', 'h1', T);
fin.run('2026-08-20', 'PR_FBA1', 2, 4000, 'complete', 'r', 'h2', T);
fin.run('2026-07-01', 'pr_fba1', 9, 18000, 'complete', 'r', 'h3', T);   // 30 日より前 → 数えない

console.log('\n── 表の作成と AI 用 view ──');
{
  createTables(db);
  const names = new Set(db.prepare(`SELECT name FROM sqlite_master`).all().map((r) => r.name));
  for (const t of ['ap_policies', 'ap_policy_events', 'ap_evaluation_runs', 'ap_evaluations', 'ap_evaluation_reviews']) ok(names.has(t), `表 ${t} がある`);
  ok(names.has('v_ap_listing_360'), 'view v_ap_listing_360 がある (mirror 表がそろっている)');
  ok(mirrorTablesAvailable(db).ok, `必要な mirror 表 ${REQUIRED_MIRROR_TABLES.length} 個がそろっている`);
  createTables(db);
  ok(true, 'createTables は 2 回呼んでも落ちない (冪等)');

  // 表が無い DB では view を作らない (無い表を参照する view は以後の DDL を全部壊す)
  const empty = new Database(':memory:');
  createTables(empty);
  ok(!empty.prepare(`SELECT name FROM sqlite_master WHERE name='v_ap_listing_360'`).get(), 'mirror 表が無い DB では view を作らない');
  ok(!mirrorTablesAvailable(empty).ok, '  → mirrorTablesAvailable も false');
  empty.exec('CREATE TABLE zzz (a)');
  ok(true, '  → その後の CREATE TABLE が通る (壊れた view が無い)');
  empty.close();
}

console.log('\n── 読み取りモデル (1 出品 1 行) ──');
{
  const rows = loadListings(db);
  ok(rows.length === 4, `出品 4 行 (fees が母集合)。実際 ${rows.length}`);
  const fba = rows.find((r) => r.seller_sku === 'PR_FBA1 ');
  ok(fba && fba.my_price === 2000 && fba.buybox_price === 1900, '最新日 (2026-09-07) の価格が付く');
  ok(fba.snapshot_date_jst === '2026-09-07', '  snapshot_date_jst');
  ok(fba.cost_incl_tax === 1100, `大文字・空白つき SKU でも原価が突合する (1100、1/100 円に丸め)。実際 ${fba.cost_incl_tax}`);
  ok(fba.units_30d === 5, `30 日販売は大文字小文字を無視して合算、30 日より前は数えない (5)。実際 ${fba.units_30d}`);
  ok(fba.ne_name === 'A商品' && fba.ne_code === 'ne-a', '商品名・NE コードが付く');
  const set = rows.find((r) => r.seller_sku === 'pr_set1');
  ok(set.cost_incl_tax === null, '★構成品に原価の無いものがあるセットは原価 null (部分合計を出さない)');
  ok(set.cost_missing_parts === 1 && set.parts === 2, '  原価不明 1 / 構成 2');
  const fbm = rows.find((r) => r.seller_sku === 'pr_fbm1');
  ok(fbm.cost_incl_tax === 540 && fbm.ship_cost === 300, 'OVERRIDDEN も原価ありとして扱う・送料が付く');
  const np = rows.find((r) => r.seller_sku === 'pr_noprice');
  ok(np.my_price === null && np.cost_incl_tax === null && np.ne_code === null, '突合できない SKU は null のまま (落とさない)');
  ok(loadListing(db, 'pr_fbm1')?.seller_sku === 'pr_fbm1' && loadListing(db, 'nope') === null, 'loadListing');
  const hist = priceHistory(db, 'PR_FBA1 ', 90);
  ok(hist.length === 2 && hist[0].date_jst === '2026-09-07', '価格の推移 (新しい順)');
  const viaView = db.prepare(`SELECT COUNT(*) c FROM v_ap_listing_360`).get().c;
  ok(viaView === 4, 'view からも同じ 4 行が読める (AI はこれを読む)');
  // Codex R4 Medium: 小数の原価でも view (AI が読む) と loadListings (画面・判定) が同じ値になる (SQL 側で ROUND 2 桁)
  prod.run('ne-frac', 'F商品', '単品', 1000.01, 'COMPLETE', 200, 0.10, T);   // 1000.01 × 1.10 = 1100.011 → 1100.01
  res.run('pr_frac', 'ne-frac', 1, 'master', 'F商品', T);
  fees.run('pr_frac', 'B000FRAC', 'FBA', 0.10, 400, 0, 0, T);
  const fromJs = loadListing(db, 'pr_frac').cost_incl_tax;
  const fromView = db.prepare(`SELECT cost_incl_tax FROM v_ap_listing_360 WHERE seller_sku = 'pr_frac'`).get().cost_incl_tax;
  ok(fromJs === 1100.01 && fromView === 1100.01, `小数の原価: 画面/判定 ${fromJs} = view ${fromView} = 1100.01`);
  db.prepare(`DELETE FROM mirror_amazon_sku_fees WHERE seller_sku = 'pr_frac'`).run();
}

console.log('\n── 方針の保存と履歴 ──');
{
  const r1 = savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'buybox', floor_price: '1,800', ceiling_price: 2500, min_margin_rate: '12' }, actorId: 'a@example.com', reasonCode: 'initial' });
  ok(r1.changed.length === 4 && !r1.changed.includes('note') && !r1.changed.includes('offset_jpy'), `初回は既定値と違う 4 列だけ残す (メモ (なし)→(なし) は残さない)。実際 ${JSON.stringify(r1.changed)}`);
  const p = getPolicy(db, 'PR_FBA1 ');
  ok(p.mode === 'buybox' && p.floor_price === 1800 && p.ceiling_price === 2500 && p.min_margin_rate === 0.12, '保存された値 (カンマ除去・% → 小数)');
  const ev1 = listPolicyEvents(db, { sku: 'PR_FBA1 ' });
  ok(ev1.length === 4 && ev1.every((e) => e.old_value === null && e.change_group === r1.changeGroup), '履歴 4 行・old は null・同じ change_group');
  const blank = savePolicy(db, { sku: 'pr_fbm1', patch: {}, actorId: 'a@example.com', reasonCode: 'initial' });
  ok(blank.changed.length === 1 && blank.changed[0] === 'mode' && getPolicy(db, 'pr_fbm1')?.mode === 'off', '何も入れずに初回保存 → mode の 1 行だけ残る (設定したことは見える)');

  const r2 = savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'buybox', floor_price: 1800, ceiling_price: 2500, min_margin_rate: 12, floor_price: 1900 }, actorId: 'b@example.com', reasonCode: 'cost_change', reasonText: '仕入値上げ' });
  ok(r2.changed.length === 1 && r2.changed[0] === 'floor_price', '変わった列だけ履歴に残る (floor_price)');
  const last = listPolicyEvents(db, { sku: 'PR_FBA1 ' })[0];
  ok(last.old_value === '1800' && last.new_value === '1900' && last.reason_text === '仕入値上げ' && last.actor_id === 'b@example.com', '  前後の値・理由・誰が');

  const r3 = savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'buybox', floor_price: 1900, ceiling_price: 2500, min_margin_rate: '12%' }, actorId: 'b@example.com', reasonCode: 'margin' });
  ok(r3.changed.length === 0 && listPolicyEvents(db, { sku: 'PR_FBA1 ' }).length === 5, '同じ内容を送り直しても履歴は増えない');

  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { floor_price: 0 }, actorId: 'x', reasonCode: 'mistake' }), '0 は入れられません', 'ストッパー 0 は拒否 (旧ツールの「0 = 下限なし」を二度と作らない)');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { ceiling_price: 1000 }, actorId: 'x', reasonCode: 'mistake' }), '赤字ストッパー以上', '高値 < 赤字 は拒否 (片方だけ変えた時も)');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'off' }, actorId: 'x', reasonCode: 'other' }), '理由を書いて', '「その他」は理由の文章が必須');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'off' }, actorId: 'x', reasonCode: 'nope' }), '変更理由を選んで', '知らない理由コードは拒否');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'auto' }, actorId: 'x', reasonCode: 'stop' }), '追従モードの値が不正', '知らないモードは拒否');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { floor_price: 12.5 }, actorId: 'x', reasonCode: 'stop' }), '整数円', '小数は拒否 (勝手に丸めない)');
  const n = normalizePolicyPatch({ min_margin_rate: '95' });
  ok(n.errors.length > 0, '最低粗利率 95% は拒否');
  // Codex R3 Medium: 境界では常に % (0.5 = 0.5%。1 未満を比率と解釈しない)
  ok(normalizePolicyPatch({ min_margin_rate: '0.5' }).patch.min_margin_rate === 0.005, '0.5 は 0.5% (= 0.005) として受ける');
  ok(normalizePolicyPatch({ min_margin_rate: '12' }).patch.min_margin_rate === 0.12, '12 は 12%');
  ok(normalizePolicyPatch({ min_margin_rate: 89.9 }).errors.length === 0 && normalizePolicyPatch({ min_margin_rate: 90 }).errors.length > 0, '89.9% は通り 90% は拒否');
}

console.log('\n── 追記のみ (トリガ) ──');
{
  throws(() => db.prepare(`UPDATE ap_policy_events SET new_value = '1' WHERE event_id = 1`).run(), 'UPDATE 禁止', '履歴の UPDATE は落ちる');
  throws(() => db.prepare(`DELETE FROM ap_policy_events WHERE event_id = 1`).run(), 'DELETE 禁止', '履歴の DELETE は落ちる');
  throws(() => db.prepare(`INSERT OR REPLACE INTO ap_policy_events (event_id, seller_sku, at, actor_type, actor_id, field, reason_code, change_group)
    VALUES (1, 'x', 'y', 'human', 'z', 'mode', 'initial', 'g')`).run(), '置き換え禁止', 'INSERT OR REPLACE で既存行を置き換えられない');
  // ★Codex R1 High: recursive_triggers を ON にしていない別接続 (SQL コンソール等) でも REPLACE は止まる
  const other = new Database(path.join(tmp, 'mirror.db'));
  const before = other.prepare(`SELECT new_value FROM ap_policy_events WHERE event_id = 1`).get().new_value;
  throws(() => other.prepare(`INSERT OR REPLACE INTO ap_policy_events (event_id, seller_sku, at, actor_type, actor_id, field, new_value, reason_code, change_group)
    VALUES (1, 'x', 'y', 'human', 'z', 'mode', 'HACKED', 'initial', 'g')`).run(), '置き換え禁止', '別接続 (recursive_triggers OFF) からの INSERT OR REPLACE も落ちる');
  ok(other.prepare(`SELECT new_value FROM ap_policy_events WHERE event_id = 1`).get().new_value === before, '  行は元のまま');
  throws(() => other.prepare(`UPDATE ap_policies SET floor_price = 1 WHERE seller_sku = 'PR_FBA1 '`).run(), '変更履歴', '履歴を伴わない ap_policies の直接 UPDATE は落ちる');
  throws(() => other.prepare(`INSERT INTO ap_policies (seller_sku, mode, updated_at, updated_by) VALUES ('ghost', 'buybox', 'now', 'x')`).run(), '変更履歴', '履歴を伴わない ap_policies の直接 INSERT は落ちる');
  throws(() => other.prepare(`DELETE FROM ap_policies WHERE seller_sku = 'PR_FBA1 '`).run(), '削除できません', 'ap_policies の DELETE は落ちる');
  // Codex R2 High 2: 既存イベントと同じ時刻を指定した INSERT OR REPLACE (別接続) も落ちる
  const cur = other.prepare(`SELECT * FROM ap_policies WHERE seller_sku = 'PR_FBA1 '`).get();
  throws(() => other.prepare(`INSERT OR REPLACE INTO ap_policies (seller_sku, mode, floor_price, updated_at, updated_by) VALUES ('PR_FBA1 ', 'off', 1, ?, ?)`).run(cur.updated_at, cur.updated_by), '置き換えも不可', '既存 SKU への INSERT OR REPLACE (同じ時刻) は落ちる');
  ok(other.prepare(`SELECT floor_price FROM ap_policies WHERE seller_sku = 'PR_FBA1 '`).get().floor_price === cur.floor_price, '  行は元のまま');
  // Codex R2 Medium 1: 「同時刻に何か 1 件」では足りない。note の履歴だけ書いて mode を直接変える経路は落ちる
  const at2 = new Date(Date.parse(cur.updated_at) + 5000).toISOString();
  other.prepare(`INSERT INTO ap_policy_events (seller_sku, at, actor_type, actor_id, field, old_value, new_value, reason_code, change_group) VALUES ('PR_FBA1 ', ?, 'human', ?, 'note', NULL, 'x', 'other', 'g2')`).run(at2, cur.updated_by);
  throws(() => other.prepare(`UPDATE ap_policies SET mode = 'off', note = 'x', updated_at = ? WHERE seller_sku = 'PR_FBA1 '`).run(at2), '変更内容', 'note の履歴だけで mode も変える UPDATE は落ちる');
  other.prepare(`UPDATE ap_policies SET note = 'x', updated_at = ? WHERE seller_sku = 'PR_FBA1 '`).run(at2);
  ok(other.prepare(`SELECT note FROM ap_policies WHERE seller_sku = 'PR_FBA1 '`).get().note === 'x', '  履歴どおりの UPDATE (note だけ) は通る');
  other.close();
  // 上の直接 UPDATE のあとでも savePolicy は正しく動く (updated_at が前より進む)
  const r4 = savePolicy(db, { sku: 'PR_FBA1 ', patch: { note: null }, actorId: 'a@example.com', reasonCode: 'mistake' });
  ok(r4.changed.length === 1 && r4.changed[0] === 'note' && getPolicy(db, 'PR_FBA1 ').note === null, 'savePolicy で note を消す (履歴 1 行)');
  throws(() => savePolicy(db, { sku: 'PR_FBA1 ', patch: { mode: 'off' }, actorId: 'x', reasonCode: 'stop', reasonText: 'あ'.repeat(301) }), '300 文字', '理由のメモ 301 文字は拒否 (サーバ側でも上限)');
}

console.log('\n── 判定 run (シャドー) ──');
{
  const r = runEvaluation(db, { trigger: 'test', actorId: 't' });
  ok(!r.skipped && r.run.status === 'success' && r.run.listings_total === 4, `run 成功・4 出品。実際 ${JSON.stringify({ skipped: r.skipped, status: r.run?.status, n: r.run?.listings_total })}`);
  ok(r.run.snapshot_date_jst === '2026-09-07' && r.run.rule_version === RULE_VERSION, '  価格データの日付とルール版が付く');
  const evals = evaluationsOfRun(db, r.run.run_id);
  ok(evals.length === 4, '  判定 4 行');
  const fba = evals.find((e) => e.seller_sku === 'PR_FBA1 ');
  // 原価 1100 + FBA 400 = 1500 / (1 − 0.10 − 0.12) = 1923.07 → 1924。人のストッパー 1900 < 1924 → 実効 1924。カート 1900 → 1924 で止まる
  ok(fba.action === 'lower' && fba.proposed_price === 1924 && fba.reason_code === 'FLOOR_CLAMP', `  方針 buybox の行はカートに向かうが計算した下限 1924 で止まる。実際 ${fba.action} ${fba.proposed_price} ${fba.reason_code}`);
  const inputs = JSON.parse(fba.inputs_json);
  ok(inputs.policy?.mode === 'buybox' && inputs.cost_incl_tax === 1100 && inputs.snapshot_date_jst === '2026-09-07' && inputs.sources.cost, '  inputs_json に方針・原価・日付・出どころが残る');
  const set = evals.find((e) => e.seller_sku === 'pr_set1');
  ok(set.action === 'keep' && set.reason_code === 'OFF' && set.flags.includes('COST_UNKNOWN'), '  方針の無い行は off (維持) + 旗 COST_UNKNOWN');
  const np = evals.find((e) => e.seller_sku === 'pr_noprice');
  ok(np.action === 'hold' && np.reason_code === 'NO_MY_PRICE', '  価格の無い行は方針より先に「価格が取れていない」で保留 (データの穴として見える)');
  // 方針あり = PR_FBA1 (buybox) と pr_fbm1 (空で保存 = off) の 2 件 → 方針なし 2
  ok(r.summary.by_action.keep === 2 && r.summary.by_action.hold === 1 && r.summary.by_action.lower === 1 && r.summary.no_policy === 2, `  summary: ${JSON.stringify(r.summary.by_action)} / 方針なし ${r.summary.no_policy}`);

  const again = runEvaluation(db, { trigger: 'test', actorId: 't' });
  ok(again.skipped && again.run.run_id === r.run.run_id, '同じ入力 (同じ日・同じ同期状態) には 2 本目を作らない (skip)');
  // Codex R5 Medium: 同じ日でも同期が進んで入力が変わったら (行が増えた・同期時刻が進んだ) 作り直す
  db.prepare(`UPDATE mirror_amazon_price_snapshot_daily SET my_price = 2050, synced_at = '2026-09-07T05:00:00.000Z' WHERE date_jst = '2026-09-07' AND seller_sku = 'PR_FBA1 '`).run();
  const afterSync = runEvaluation(db, { trigger: 'test', actorId: 't' });
  ok(!afterSync.skipped && afterSync.run.run_id !== r.run.run_id, '同じ日でも同期が進めば (synced_at が変わる) 新しい run を作る');
  ok(afterSync.run.input_fingerprint !== r.run.input_fingerprint && afterSync.run.snapshot_date_jst === '2026-09-07', '  指紋が違い、日付は同じ');
  ok(evaluationsOfRun(db, afterSync.run.run_id).find((e) => e.seller_sku === 'PR_FBA1 ').current_price === 2050, '  新しい価格で判定している');
  const forced = runEvaluation(db, { trigger: 'test', actorId: 't', force: true });
  ok(!forced.skipped && forced.run.run_id !== r.run.run_id, 'force なら作り直す');
  ok(runForSnapshot(db, '2026-09-07', RULE_VERSION).run_id === forced.run.run_id, '  最新の成功 run が返る');
  ok(listRuns(db).length === 3, '  run は 3 本 (最初・同期後・force)');

  // autonomy_level は 0 以外を入れられない (実行段階が無いことを表で示す)
  throws(() => db.prepare(`INSERT INTO ap_evaluations (run_id, seller_sku, evaluated_at, rule_version, autonomy_level, action, reason_code, reason_text, inputs_json)
    VALUES (?, 'x', ?, ?, 1, 'keep', 'OFF', 'x', '{}')`).run(forced.run.run_id, T, RULE_VERSION), 'CHECK', '★autonomy_level = 1 (人承認後に実行) は CHECK で拒否');
  throws(() => db.prepare(`UPDATE ap_evaluations SET proposed_price = 1 WHERE decision_id = ?`).run(fba.decision_id), 'UPDATE 禁止', '判定の UPDATE は落ちる');
  // Codex R2 High 2: UNIQUE(run_id, seller_sku) 経由の INSERT OR REPLACE (主キー省略・別接続) も落ちる
  {
    const other2 = new Database(path.join(tmp, 'mirror.db'));
    throws(() => other2.prepare(`INSERT OR REPLACE INTO ap_evaluations (run_id, seller_sku, evaluated_at, rule_version, action, reason_code, reason_text, inputs_json)
      VALUES (?, 'PR_FBA1 ', ?, ?, 'keep', 'OFF', 'replaced', '{}')`).run(r.run.run_id, T, RULE_VERSION), '置き換え禁止', 'UNIQUE 経由の INSERT OR REPLACE は落ちる');
    ok(other2.prepare(`SELECT reason_text FROM ap_evaluations WHERE decision_id = ?`).get(fba.decision_id).reason_text !== 'replaced', '  行は元のまま');
    other2.close();
  }
  throws(() => db.prepare(`DELETE FROM ap_evaluation_runs WHERE run_id = ?`).run(r.run.run_id), '削除できません', 'run の DELETE は落ちる');
  throws(() => db.prepare(`UPDATE ap_evaluation_runs SET trigger = 'manual' WHERE run_id = ?`).run(r.run.run_id), '終了記録', '終わった run の書き換えは落ちる');
  // Codex R1 Medium: running の run でも「終了の記録」以外は書き換えられない
  db.prepare(`INSERT INTO ap_evaluation_runs (run_id, started_at, trigger, actor_id, snapshot_date_jst, input_fingerprint, rule_version, status) VALUES ('apr-running', ?, 'test', 't', '2026-09-07', 'fp', ?, 'running')`).run(T, RULE_VERSION);
  throws(() => db.prepare(`UPDATE ap_evaluation_runs SET snapshot_date_jst = '2099-01-01' WHERE run_id = 'apr-running'`).run(), '終了記録', 'running の run の snapshot を変える UPDATE は落ちる');
  throws(() => db.prepare(`UPDATE ap_evaluation_runs SET status = 'running', listings_total = 999 WHERE run_id = 'apr-running'`).run(), '終了記録', 'running → running は落ちる');
  throws(() => db.prepare(`INSERT OR REPLACE INTO ap_evaluation_runs (run_id, started_at, trigger, actor_id, input_fingerprint, rule_version, status) VALUES ('apr-running', ?, 'manual', 'x', 'fp', ?, 'success')`).run(T, RULE_VERSION), '置き換え禁止', 'run の INSERT OR REPLACE は落ちる');
  db.prepare(`UPDATE ap_evaluation_runs SET status = 'failed', finished_at = ?, error = 'test' WHERE run_id = 'apr-running'`).run(T);
  ok(db.prepare(`SELECT status FROM ap_evaluation_runs WHERE run_id = 'apr-running'`).get().status === 'failed', '  running → failed (終了の記録) は通る');

  // Codex R1 Medium: 判定の保存に失敗しても run は failed として残る (ロールバックで消えない)
  db.exec(`CREATE TRIGGER tmp_boom BEFORE INSERT ON ap_evaluations BEGIN SELECT RAISE(ABORT, 'boom'); END`);
  const failedRun = runEvaluation(db, { trigger: 'test', actorId: 't', force: true });
  db.exec('DROP TRIGGER tmp_boom');
  ok(failedRun.error && /boom/.test(failedRun.error) && failedRun.run?.status === 'failed', `保存に失敗した run は failed として残り、error が返る (${failedRun.run?.status} / ${failedRun.error})`);
  ok(db.prepare(`SELECT COUNT(*) c FROM ap_evaluation_runs WHERE status = 'failed'`).get().c === 2, '  failed の run が DB に残っている');
  ok(db.prepare(`SELECT COUNT(*) c FROM ap_evaluations WHERE run_id = ?`).get(failedRun.run.run_id).c === 0, '  失敗した run の判定行は残らない (savepoint で巻き戻し)');
  ok(runForSnapshot(db, '2026-09-07', RULE_VERSION).run_id === forced.run.run_id, '  最新の成功 run は変わらない');
}

console.log('\n── 採点 (人のフィードバック) ──');
{
  const run = runForSnapshot(db, '2026-09-07', RULE_VERSION);
  const evals = evaluationsOfRun(db, run.run_id);
  const target = evals.find((e) => e.seller_sku === 'PR_FBA1 ');
  addReview(db, { decisionId: target.decision_id, reviewerId: 'a@example.com', verdict: 'disagree', comment: '下限が高すぎる' });
  addReview(db, { decisionId: target.decision_id, reviewerId: 'a@example.com', verdict: 'agree' });
  const after = evaluationsOfRun(db, run.run_id).find((e) => e.seller_sku === 'PR_FBA1 ');
  ok(after.review_verdict === 'agree' && after.review_by === 'a@example.com', '最後の採点が「現在の見方」になる (追記のみ)');
  const stats = reviewStats(db, run.run_id);
  ok(stats.agree === 1 && stats.disagree === 0, `集計は最後の採点だけを数える: ${JSON.stringify(stats)}`);
  ok(db.prepare(`SELECT COUNT(*) c FROM ap_evaluation_reviews`).get().c === 2, '採点の行は 2 本とも残る');
  throws(() => addReview(db, { decisionId: 999999, reviewerId: 'x', verdict: 'agree' }), 'その判定はありません', '無い判定には採点できない');
  throws(() => addReview(db, { decisionId: target.decision_id, reviewerId: 'x', verdict: 'great' }), '採点の値が不正', '知らない採点値は拒否');
  throws(() => addReview(db, { decisionId: target.decision_id, reviewerId: 'x', verdict: 'agree', comment: 'あ'.repeat(501) }), '500 文字', 'ひとこと 501 文字は拒否 (黙って切らない)');
  throws(() => db.prepare(`DELETE FROM ap_evaluation_reviews`).run(), 'DELETE 禁止', '採点の DELETE は落ちる');
  const forSku = evaluationsForSku(db, 'PR_FBA1 ');
  ok(forSku.length === 3 && forSku[0].decision_id > forSku[1].decision_id, 'SKU 別の判定履歴 (新しい順・3 run ぶん)');
}

db.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows の WAL 残りは無視 */ }
console.log(`\n${failed === 0 ? '🎉 ALL PASS' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
