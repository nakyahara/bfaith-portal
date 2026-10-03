#!/usr/bin/env node
/**
 * test-amazon-finance-read.mjs — Amazon の財務の共通の読み口 (lib/amazon-finance-read.js・F4-1) の試験
 *
 *   1. consumer ごとの profile が閉じた一覧のとおり・全部 legacy (今の写し) を読む
 *   2. 一覧に無い consumer・宣言していない dataset は例外 (黙って既定の表を返さない)
 *   3. 壊れた profile の組 (cdb を選ぶ・consumer が足りない / 余計・知らない dataset・空・間接の読み手の行き先が無い) は止まる
 *   4. env を読まない (AMAZON_FINANCE_READ_PROFILE / AMAZON_FINANCE_READ_SOURCE を入れても legacy のまま) = F4-1 では切り替えられない
 *   5. 読み手のソースに旧い写しの表の名前を直に書いていない (読み口・書き込み側・miniPC・試験を除く)。
 *      読み口の呼び出しの consumer は一覧にあり、一覧の consumer は必ずどこかで使われている
 *
 * 値が変わらないこと (before/after の応答の一致) は scripts/test-amazon-finance-read-parity.mjs。
 * 実行: node scripts/test-amazon-finance-read.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const LIB = path.join(REPO, 'lib/amazon-finance-read.js');
const R = await import(pathToFileURL(LIB).href);

let pass = 0, fail = 0;
const ok = (cond, name, extra) => {
  if (cond) { pass++; console.log(`  ✅ ${name}`); } else { fail++; console.log(`  ❌ ${name}${extra === undefined ? '' : ` ${JSON.stringify(extra)}`}`); }
};
const throws = (fn, re) => { try { fn(); return false; } catch (e) { return re.test(String(e.message)); } };

const FIN = 'mirror_amazon_finance_sku_daily';
const FEES = 'mirror_amazon_account_fees_monthly';

console.log('\n── 1. consumer ごとの profile (閉じた一覧・全部 legacy) ──');
// 🚨 consumer を足す / 消すときはここも直す (= レビューで必ず目に入る)
const EXPECTED = {
  'amazon-dashboard:settled-boundary': { finance_daily: FIN },
  'amazon-dashboard:overview': { finance_daily: FIN },
  'amazon-dashboard:account-fees': { account_fees: FEES },
  'amazon-dashboard:trend': { finance_daily: FIN },
  'amazon-dashboard:waterfall': { finance_daily: FIN },
  'amazon-dashboard:sku-profit': { finance_daily: FIN },
  'amazon-dashboard:ads': { finance_daily: FIN },
  'amazon-dashboard:bestsellers': { finance_daily: FIN },
  'amazon-dashboard:diagnosis': { finance_daily: FIN },
  'margin-alert': { finance_daily: FIN },
  'amazon-pricing': { finance_daily: FIN },
  'supplier-sales': { finance_daily: FIN },
  'site-products': { finance_daily: FIN },
  'mall-finance-unified-view': { finance_daily: FIN },
};
{
  ok(JSON.stringify(R.SOURCES) === JSON.stringify(['legacy']), 'source は legacy だけ (cdb の道は無い)', R.SOURCES);
  ok(JSON.stringify(Object.keys(R.PROFILE_SETS)) === JSON.stringify(['legacy']), 'profile の組は legacy だけ', Object.keys(R.PROFILE_SETS));
  ok(R.ACTIVE_PROFILE_SET === 'legacy', '使う組 = legacy (固定)');
  const desc = R.describeProfile();
  ok(JSON.stringify(Object.keys(desc).sort()) === JSON.stringify(Object.keys(EXPECTED).sort()), `consumer の一覧 (${Object.keys(EXPECTED).length} 個) が決めたとおり`, Object.keys(desc));
  for (const [c, ds] of Object.entries(EXPECTED)) {
    const got = desc[c] || {};
    const same = JSON.stringify(Object.keys(got).sort()) === JSON.stringify(Object.keys(ds).sort())
      && Object.entries(ds).every(([d, t]) => got[d]?.source === 'legacy' && got[d]?.table === t);
    ok(same, `${c} → ${Object.entries(ds).map(([d, t]) => `${d}=${t}`).join(', ')} (legacy)`, got);
    for (const [d, t] of Object.entries(ds)) ok(R.amazonFinanceTable(c, d) === t, `  amazonFinanceTable('${c}', '${d}') = ${t}`);
  }
  ok(JSON.stringify(R.INDIRECT_CONSUMERS) === JSON.stringify({ 'purchase-orders': 'mall-finance-unified-view', 'ai-insights': 'mall-finance-unified-view' }), '間接の読み手 = purchase-orders・ai-insights (統合の view を通す)', R.INDIRECT_CONSUMERS);
  ok(Object.isFrozen(R.PROFILE_SETS) && Object.isFrozen(R.PROFILE_SETS.legacy) && Object.isFrozen(R.PROFILE_SETS.legacy['supplier-sales']), 'profile は凍結 (実行中に書き換えられない)');
  const before = R.financeDailyTable('supplier-sales');
  try { R.PROFILE_SETS.legacy['supplier-sales'].finance_daily = 'cdb'; } catch { /* strict mode では TypeError */ }
  ok(R.financeDailyTable('supplier-sales') === before, '書き換えようとしても変わらない');
}

console.log('\n── 2. 一覧に無い consumer・宣言していない dataset は例外 ──');
{
  ok(throws(() => R.financeDailyTable('nope'), /一覧に無い/), '知らない consumer は例外');
  ok(throws(() => R.financeDailyTable(undefined), /一覧に無い/), 'consumer を渡し忘れると例外');
  ok(throws(() => R.financeDailyTable('purchase-orders'), /一覧に無い/), '間接の読み手 (purchase-orders) は表の名前をもらえない (view を通す)');
  ok(throws(() => R.accountFeesTable('supplier-sales'), /宣言していない/), '宣言していない dataset (supplier-sales の月の手数料) は例外');
  ok(throws(() => R.financeDailyTable('amazon-dashboard:account-fees'), /宣言していない/), 'account-fees の consumer は日次の財務を読めない');
  ok(throws(() => R.amazonFinanceTable('supplier-sales', 'nope'), /宣言していない/), '知らない dataset は例外');
  ok(throws(() => R.financeDailyTable('__proto__'), /一覧に無い/), "'__proto__' を consumer にしても通らない");
}

console.log('\n── 3. 壊れた profile の組は止まる ──');
{
  const legacy = R.PROFILE_SETS.legacy;
  const clone = () => JSON.parse(JSON.stringify(legacy));
  const check = (name, mutate, re) => {
    const set = clone(); mutate(set);
    ok(throws(() => R.validateProfileSet('x', { x: set }), re), name);
  };
  ok(JSON.stringify(R.validateProfileSet('legacy')) === JSON.stringify(Object.keys(EXPECTED).sort()), 'legacy の組は通る');
  check('cdb を選ぶと止まる (F4-1 では選べない)', (s) => { s['supplier-sales'].finance_daily = 'cdb'; }, /選べない/);
  check('cdb:finance_only のような値も止まる', (s) => { s['amazon-pricing'].finance_daily = 'cdb:finance_only'; }, /選べない/);
  check('consumer が 1 つ足りないと止まる', (s) => { delete s['site-products']; }, /足りない: site-products/);
  check('一覧に無い consumer があると止まる', (s) => { s['new-reader'] = { finance_daily: 'legacy' }; }, /余計: new-reader/);
  check('知らない dataset があると止まる', (s) => { s['amazon-dashboard:trend'].profit = 'legacy'; }, /知らない dataset 'profit'/);
  check('何も読まない consumer は止まる', (s) => { s['margin-alert'] = {}; }, /何も読まない/);
  ok(throws(() => R.validateProfileSet('cdb'), /'cdb' は無い/), '無い組の名前は止まる');
  ok(throws(() => R.validateProfileSet('toString'), /'toString' は無い/), "'toString' のような名前も止まる");
}

console.log('\n── 4. env を読まない (F4-1 では切り替えられない) ──');
{
  const src = fs.readFileSync(LIB, 'utf8');
  ok(!/process\s*\.\s*env/.test(src), '読み口のソースに process.env が無い');
  const r = spawnSync(process.execPath, ['--input-type=module', '-e',
    `const m = await import(${JSON.stringify(pathToFileURL(LIB).href)}); console.log(JSON.stringify([m.ACTIVE_PROFILE_SET, m.financeDailyTable('supplier-sales'), m.accountFeesTable('amazon-dashboard:account-fees')]));`],
  { env: { ...process.env, AMAZON_FINANCE_READ_PROFILE: 'cdb', AMAZON_FINANCE_READ_SOURCE: 'cdb' }, encoding: 'utf8' });
  ok(r.status === 0 && r.stdout.trim() === JSON.stringify(['legacy', FIN, FEES]), 'env に cdb を入れても legacy のまま', { status: r.status, out: r.stdout, err: r.stderr.slice(0, 300) });
}

console.log('\n── 5. 読み手が旧い写しの表の名前を直に書いていない ──');
{
  // コメント (/* */・行の // ・SQL の --) を除いてから探す
  const strip = (s) => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[\s;,(){}])\/\/.*$/gm, '$1').replace(/--.*$/gm, '');
  const files = [];
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) { if (!['node_modules', '.git', 'fixtures'].includes(e.name)) walk(p); } else if (/\.(m?js|cjs|ejs)$/.test(e.name)) files.push(p);
    }
  };
  for (const d of ['apps', 'lib', 'mcp', 'tools', 'scripts']) if (fs.existsSync(path.join(REPO, d))) walk(path.join(REPO, d));
  files.push(path.join(REPO, 'server.js'));
  const rel = (f) => path.relative(REPO, f).split(path.sep).join('/');
  // 直に書いてよい所 = 読み口・写しへの書き込み (受け口・表の DDL)・miniPC の送り手・試験
  const allowed = (r) => r === 'lib/amazon-finance-read.js'
    || r === 'apps/warehouse-mirror/db.js' || r === 'apps/warehouse-mirror/router.js'
    || r.startsWith('apps/warehouse/')
    || /(^|\/)test-[^/]*$/.test(r) || /smoke[^/]*$/.test(r);
  const LITERAL = /mirror_amazon_finance_sku_daily|mirror_amazon_account_fees_monthly/;
  const offenders = [];
  const calls = [];
  for (const f of files) {
    const r = rel(f);
    const src = fs.readFileSync(f, 'utf8');
    const code = strip(src);
    if (LITERAL.test(code) && !allowed(r)) offenders.push(r);
    if (r !== 'lib/amazon-finance-read.js' && !/(^|\/)test-[^/]*$/.test(r)) {
      for (const m of code.matchAll(/\b(financeDailyTable|accountFeesTable)\(\s*'([^']+)'\s*\)/g)) calls.push({ file: r, fn: m[1], consumer: m[2] });
      for (const m of code.matchAll(/\bconsumer:\s*'([^']+)'/g)) calls.push({ file: r, fn: 'opts.consumer', consumer: m[1] });
    }
  }
  ok(offenders.length === 0, `読み口を通さずに旧い写しの表の名前を書いている所 = 0 (${files.length} 本を見た)`, offenders);

  // 統合の view の Amazon の枝は読み口を通す (db.js は DDL があるので丸ごとは許しているが、view の中は別に見る)
  const db = fs.readFileSync(path.join(REPO, 'apps/warehouse-mirror/db.js'), 'utf8');
  const start = db.indexOf('CREATE VIEW v_mall_finance_daily_unified AS');
  const branch = start >= 0 ? db.slice(start, db.indexOf('UNION ALL', start)) : '';
  ok(branch.includes("financeDailyTable('mall-finance-unified-view')") && !LITERAL.test(branch), "統合の view の Amazon の枝 = financeDailyTable('mall-finance-unified-view')");

  // 呼び出しの consumer は一覧にある・dataset を宣言している
  const bad = calls.filter((c) => {
    const p = R.PROFILE_SETS.legacy[c.consumer];
    if (!p) return true;
    if (c.fn === 'financeDailyTable' || c.fn === 'opts.consumer') return !('finance_daily' in p);
    return !('account_fees' in p);
  });
  ok(calls.length >= 15 && bad.length === 0, `読み口の呼び出し ${calls.length} か所の consumer が全部一覧にある`, bad);
  // 一覧の consumer は必ずどこかで使われている (使われない profile を残さない)
  const used = new Set(calls.map((c) => c.consumer));
  // settledBySku(db, from, to, 'amazon-dashboard:xxx') のように引数で渡す所も数える
  for (const f of files.filter((x) => rel(x) === 'apps/amazon-dashboard/queries.js')) {
    for (const m of strip(fs.readFileSync(f, 'utf8')).matchAll(/'(amazon-dashboard:[a-z-]+)'/g)) used.add(m[1]);
  }
  const unused = Object.keys(EXPECTED).filter((c) => !used.has(c));
  ok(unused.length === 0, '一覧の consumer は全部どこかで使われている', unused);
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件 OK / ${fail} 件 NG`);
process.exitCode = fail ? 1 : 0;
