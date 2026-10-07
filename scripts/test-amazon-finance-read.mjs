#!/usr/bin/env node
/**
 * test-amazon-finance-read.mjs — Amazon の財務の共通の読み口 (lib/amazon-finance-read.js・F4-1) の試験
 *
 *   1. consumer ごとの profile が閉じた一覧のとおり・全部 legacy (今の写し) を読む
 *   2. 一覧に無い consumer・宣言していない dataset は例外 (黙って既定の表を返さない)
 *   3. 壊れた profile の組 (cdb を選ぶ・consumer が足りない / 余計・知らない dataset・空・間接の読み手の行き先が無い) は止まる
 *      (consumer ごとの dataset の集合も閉じている = 差し替え・足し・抜けは止まる)
 *   4. env を読まない (AMAZON_FINANCE_READ_PROFILE / AMAZON_FINANCE_READ_SOURCE を入れても legacy のまま) = F4-1 では切り替えられない
 *   5. 旧い写しの表の名前を直に書いてよいのは「書き込み側の決まった使い方」(DDL・索引・受け口の INSERT・同期の登録・表の作り直し) の行だけ。
 *      ファイル丸ごとは許さない。試験のファイルを除くのは本番から import されていないときだけ。読む形 (FROM / JOIN) を足すと落ちる
 *   6. どの関数がどの consumer で読むかの表 (用途ごとの割り当て) と、実際の呼び出しが合う。間違えた割り当ては落ちる
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
  check('consumer が 1 つ足りないと止まる', (s) => { delete s['amazon-pricing']; }, /足りない: amazon-pricing/);
  check('一覧に無い consumer があると止まる', (s) => { s['new-reader'] = { finance_daily: 'legacy' }; }, /余計: new-reader/);
  check('知らない dataset があると止まる', (s) => { s['amazon-dashboard:trend'].profit = 'legacy'; }, /知らない dataset 'profit'/);
  check('何も読まない consumer は止まる (dataset の抜け)', (s) => { s['margin-alert'] = {}; }, /何も読まない/);
  // consumer ごとの dataset の集合も閉じている (Codex #1599 R1 M1)
  check('dataset の差し替えは止まる (supplier-sales を月の手数料に)', (s) => { s['supplier-sales'] = { account_fees: 'legacy' }; }, /'supplier-sales' の dataset が閉じた一覧と違う/);
  check('dataset の足しは止まる (supplier-sales に月の手数料も)', (s) => { s['supplier-sales'].account_fees = 'legacy'; }, /'supplier-sales' の dataset が閉じた一覧と違う/);
  check('dataset の差し替えは止まる (月の手数料の consumer を日次の財務に)', (s) => { s['amazon-dashboard:account-fees'] = { finance_daily: 'legacy' }; }, /'amazon-dashboard:account-fees' の dataset が閉じた一覧と違う/);
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
  // 子のプロセスが起動できない (EPERM など) ときは stdout / stderr が無い = 原因 (r.error) を出して落ちる (Codex #1599 R1 Low)
  ok(!r.error && r.status === 0 && String(r.stdout || '').trim() === JSON.stringify(['legacy', FIN, FEES]), 'env に cdb を入れても legacy のまま',
    { status: r.status, error: r.error ? String(r.error.message || r.error) : null, out: String(r.stdout || '').slice(0, 300), err: String(r.stderr || '').slice(0, 300) });
}

// ─────────── ソースを読む試験の道具 ───────────
// コメント (/* */・行の // ・SQL の --) を除く (行の数は保つ = 行の番号がずれない)
const strip = (s) => s.replace(/\r\n/g, '\n').replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' ')).replace(/(^|[\s;,(){}])\/\/.*$/gm, '$1').replace(/--.*$/gm, '');
const rel = (f) => path.relative(REPO, f).split(path.sep).join('/');
const files = [];
{
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) { if (!['node_modules', '.git', 'fixtures'].includes(e.name)) walk(p); } else if (/\.(m?js|cjs|ejs)$/.test(e.name)) files.push(p);
    }
  };
  for (const d of ['apps', 'lib', 'mcp', 'tools', 'scripts']) if (fs.existsSync(path.join(REPO, d))) walk(path.join(REPO, d));
  files.push(path.join(REPO, 'server.js'));
}
const isTestName = (r) => /(^|\/)test-[^/]*$/.test(r) || /(^|\/)[^/]*smoke[^/]*$/.test(r);
const SRC = new Map(files.map((f) => [rel(f), strip(fs.readFileSync(f, 'utf8'))]));

console.log('\n── 5. 旧い写しの表の名前を直に書いてよいのは「書き込み側の決まった使い方」だけ (Codex #1599 R1 M3) ──');
const LITERAL = /mirror_amazon_finance_sku_daily|mirror_amazon_account_fees_monthly/;
const T = '(mirror_amazon_finance_sku_daily|mirror_amazon_account_fees_monthly)';
// ファイル × 行の形で許す (ファイル丸ごとは許さない)。読む (SELECT … FROM / JOIN) 形は 1 つだけ = 月の手数料の表の作り直しの写し
const WRITE_SIDE_ALLOW = [
  { file: 'lib/amazon-finance-read.js', re: new RegExp(`^\\s*(finance_daily|account_fees): '${T}',$`), why: '読み口の legacy の表の名前' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*db\.exec\(`CREATE TABLE IF NOT EXISTS mirror_amazon_finance_sku_daily \($/, why: '表の DDL' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*const have = new Set\(db\.prepare\(`PRAGMA table_info\(mirror_amazon_finance_sku_daily\)`\)/, why: '列の有無 (列を足す前)' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*if \(!have\.has\('\w+'\)\) db\.exec\(`ALTER TABLE mirror_amazon_finance_sku_daily ADD COLUMN \w+ [^`]*`\);$/, why: '列を足す' },
  { file: 'apps/warehouse-mirror/db.js', re: new RegExp(`^\\s*db\\.exec\\('CREATE INDEX IF NOT EXISTS \\w+ ON ${T}\\([^']*\\)'\\);$`), why: '索引' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*const maafmCur = db\.prepare\(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mirror_amazon_account_fees_monthly'`\)\.get\(\);$/, why: '表の定義を見る (作り直しの要否)' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*const maafmDeps = maafmCur \? db\.prepare\(`SELECT type, name FROM sqlite_master WHERE type IN \('view', 'trigger'\) AND sql LIKE '%mirror_amazon_account_fees_monthly%'`\)/, why: '依存する view / trigger を見る' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*console\.(error|log)\(['`]\[warehouse-mirror\] [^`']*mirror_amazon_account_fees_monthly/, why: 'ログの文字' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*db\.exec\(MAAFM_SQL\('mirror_amazon_account_fees_monthly(_new)?'\)\);$/, why: '表の DDL (作り直し)' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*db\.exec\(`INSERT INTO mirror_amazon_account_fees_monthly_new \(\$\{cols\}\) SELECT \$\{cols\} FROM mirror_amazon_account_fees_monthly`\);$/, why: '作り直しの写し (旧 → _new・同じ表)', readsOk: true },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*db\.exec\('DROP TABLE mirror_amazon_account_fees_monthly'\);$/, why: '作り直し' },
  { file: 'apps/warehouse-mirror/db.js', re: /^\s*db\.exec\('ALTER TABLE mirror_amazon_account_fees_monthly_new RENAME TO mirror_amazon_account_fees_monthly'\);$/, why: '作り直し' },
  { file: 'apps/warehouse-mirror/router.js', re: new RegExp(`^\\s*INSERT OR REPLACE INTO ${T} \\($`), why: '受け口 (写しへの書き込み)' },
  { file: 'apps/warehouse-mirror/router.js', re: new RegExp(`^\\s*mirror_table: '${T}',$`), why: '同期の entity の登録 (書き込み先)' },
  { file: 'apps/warehouse/db.js', re: /^\s*'f_amazon_account_fees_monthly_v1', 'mirror_amazon_account_fees_monthly',$/, why: 'miniPC の同期の契約 (送り先の表)' },
];
const READS_OLD = new RegExp(`\\b(FROM|JOIN)\\s+${T}\\b`, 'i');
/** 1 ファイルの違反の一覧 (許しに当たらない行・許しに当たっても読む形の行) */
function literalViolations(file, code, hits) {
  const out = [];
  code.split('\n').forEach((raw, i) => {
    const line = raw.replace(/\s+$/, '');   // 行の終わりの空白 (コメントを除いた跡) は見ない
    if (!LITERAL.test(line)) return;
    const a = WRITE_SIDE_ALLOW.find((x) => x.file === file && x.re.test(line));
    if (!a) { out.push(`${file}:${i + 1}: ${line.trim().slice(0, 120)}`); return; }
    if (READS_OLD.test(line) && !a.readsOk) { out.push(`${file}:${i + 1} (読む形): ${line.trim().slice(0, 120)}`); return; }
    if (hits) hits.add(a);
  });
  return out;
}
{
  // 試験のファイルを除いてよいのは「本番のコードから import されていない」ときだけ
  const importedByProd = new Set();
  for (const [r, code] of SRC) {
    if (isTestName(r)) continue;
    for (const m of code.matchAll(/(?:\bfrom\s*|\bimport\s*\(\s*|\brequire\s*\(\s*)['"](\.{1,2}\/[^'"]+)['"]/g)) {
      importedByProd.add(rel(path.resolve(path.dirname(path.join(REPO, r)), m[1])));
    }
  }
  const testImportedByProd = [...importedByProd].filter((r) => isTestName(r) && SRC.has(r));
  ok(testImportedByProd.length === 0, '本番のコードから import されている試験のファイルが無い (= 除いた試験は読み手になれない)', testImportedByProd);

  const hits = new Set();
  const viol = [];
  for (const [r, code] of SRC) {
    if (isTestName(r) && !importedByProd.has(r)) continue;
    viol.push(...literalViolations(r, code, hits));
  }
  ok(viol.length === 0, `旧い写しの表の名前は書き込み側の決まった ${WRITE_SIDE_ALLOW.length} 形だけ (${SRC.size} 本を見た)`, viol);
  const stale = WRITE_SIDE_ALLOW.filter((a) => !hits.has(a)).map((a) => `${a.file}: ${a.why}`);
  ok(stale.length === 0, '許しは全部使われている (古い許しを残さない)', stale);

  // 書き込み側のファイルに読む形を足すと落ちる (検査そのものの試験)
  const inject = [
    ['apps/warehouse-mirror/db.js', "  const n = db.prepare('SELECT COUNT(*) AS n FROM mirror_amazon_finance_sku_daily').get().n;"],
    ['apps/warehouse-mirror/db.js', '      FROM mirror_amazon_finance_sku_daily'],
    ['apps/warehouse-mirror/db.js', '    db.exec(`INSERT INTO mirror_amazon_account_fees_monthly_new (${cols}) SELECT ${cols} FROM mirror_amazon_finance_sku_daily`);'],
    ['apps/warehouse-mirror/router.js', '    LEFT JOIN mirror_amazon_account_fees_monthly f ON f.date_jst = x.date_jst'],
    ['apps/warehouse-mirror/router.js', "    const t = 'mirror_amazon_finance_sku_daily';"],
    ['apps/warehouse/db.js', '  SELECT MAX(date_jst) FROM mirror_amazon_account_fees_monthly'],
    ['lib/amazon-finance-read.js', "    cdb_daily: 'mirror_amazon_finance_sku_daily',"],
    ['apps/amazon-dashboard/queries.js', '    FROM mirror_amazon_finance_sku_daily'],
  ];
  for (const [file, line] of inject) {
    const code = `${SRC.get(file) || ''}\n${line}\n`;
    ok(literalViolations(file, code).length === 1, `足すと落ちる: ${file} に「${line.trim().slice(0, 70)}」`);
  }
}

console.log('\n── 6. どの関数がどの consumer で読むか (用途ごとの割り当てを固定・Codex #1599 R1 M2) ──');
// 読み口を呼ぶ所を出てくる順に「種類:consumer」で並べた期待の表。🚨 読み手を足す・consumer を変えるときはここも直す
//   financeDailyTable / accountFeesTable = 表の名前をもらう所 / settledBySku = 共通の SKU 集計に consumer を渡す所
//   default = getSkuProfit の既定 / opts.consumer = getSkuProfit を consumer つきで呼ぶ所 / $consumer = 引数で受けた consumer をそのまま渡す所
const D = (x) => `amazon-dashboard:${x}`;
const ASSIGN = {
  'apps/amazon-dashboard/queries.js': {
    settledSummary: [`financeDailyTable:${D('overview')}`],                                   // 概要のタイル
    getAccountFees: [`accountFeesTable:${D('account-fees')}`],                                // 月の手数料の表
    accountFeesCostForMonth: [`accountFeesTable:${D('account-fees')}`],                       // 月のタイルの最終利益
    getTrend: [`financeDailyTable:${D('trend')}`],                                            // 傾向
    settledBySku: ['financeDailyTable:$consumer'],                                            // 共通の SKU 集計 (呼び手の consumer)
    getWaterfall: [`financeDailyTable:${D('waterfall')}`, `settledBySku:${D('waterfall')}`],  // 滝の合計 / SKU 指定の広告の割り振り
    getSkuProfit: [`default:${D('sku-profit')}`, 'settledBySku:$consumer', 'financeDailyTable:$consumer'],   // SKU の利益 (画面 / margin-alert)
    lastSettledDate: [`financeDailyTable:${D('settled-boundary')}`],                          // 決済のそろった日
    getAdsAnalysis: [`settledBySku:${D('ads')}`, `financeDailyTable:${D('ads')}`],            // 広告の SKU / 月の TACoS
    getBestsellers: [`settledBySku:${D('bestsellers')}`, `settledBySku:${D('bestsellers')}`, `financeDailyTable:${D('bestsellers')}`],   // 今期 / 前期 / スパーク
    getDiagnosis: [`settledBySku:${D('diagnosis')}`, `settledBySku:${D('diagnosis')}`, `settledBySku:${D('diagnosis')}`, `settledBySku:${D('diagnosis')}`,
      `financeDailyTable:${D('diagnosis')}`, `financeDailyTable:${D('diagnosis')}`],          // 前月 / 前々月 / 赤字の月 / 広告垂れ流し / 30 日の売上 / 価格ミス
  },
  'apps/profit-analysis/margin-alert-job.js': { collectMarginRows: ['opts.consumer:margin-alert'] },
  'apps/amazon-pricing/read-model.js': { '*': ['financeDailyTable:amazon-pricing'] },          // FINANCE_DAILY (360 行・必要な表・指紋・鮮度が使う)
  'apps/amazon-pricing/engine.js': { describeInputs: ['financeDailyTable:amazon-pricing'] },   // 判定の監査の出どころ
  'apps/supplier-sales/aggregate.js': { '*': ['financeDailyTable:supplier-sales'] },           // AMAZON_FINANCE_DAILY (4 つの SQL が使う)
  'apps/warehouse-mirror/db.js': { createTables: ['financeDailyTable:mall-finance-unified-view'] },
};
/** 読み口を呼ぶ所を出てくる順に (位置, 「種類:consumer」) */
function readCalls(code) {
  const res = [];
  const lit = (q, id) => (q !== undefined ? q : `$${id}`);
  for (const m of code.matchAll(/\b(financeDailyTable|accountFeesTable)\(\s*(?:'([^']*)'|([A-Za-z_$][\w$]*))\s*\)/g)) res.push([m.index, `${m[1]}:${lit(m[2], m[3])}`]);
  for (const m of code.matchAll(/(?<!function\s)\bsettledBySku\(((?:[^()]|\([^()]*\))*)\)/g)) {
    const last = m[1].split(',').pop().trim();
    const q = /^'([^']*)'$/.exec(last);
    res.push([m.index, `settledBySku:${q ? q[1] : `$${last}`}`]);
  }
  for (const m of code.matchAll(/\bopts\.consumer\s*\?\?\s*'([^']*)'/g)) res.push([m.index, `default:${m[1]}`]);
  for (const m of code.matchAll(/\bconsumer:\s*'([^']*)'/g)) res.push([m.index, `opts.consumer:${m[1]}`]);
  return res.sort((a, b) => a[0] - b[0]);
}
/** 関数の本体の [始まり, 終わり) (引数の既定値の {} を飛ばして本体の { から括弧を数える) */
function functionRange(code, name) {
  const m = new RegExp(`(^|\\n)[ \\t]*(?:export\\s+)?(?:async\\s+)?function\\s+${name}\\s*\\(`).exec(code);
  if (!m) return null;
  let i = m.index + m[0].length, depth = 1;
  while (i < code.length && depth) { if (code[i] === '(') depth++; else if (code[i] === ')') depth--; i++; }
  const open = code.indexOf('{', i);
  depth = 0;
  for (let j = open; j < code.length; j++) {
    if (code[j] === '{') depth++;
    else if (code[j] === '}' && --depth === 0) return [open, j + 1];
  }
  return null;
}
function assignmentProblems(file, code, expect) {
  const problems = [];
  const calls = readCalls(code);
  const claimed = new Set();
  for (const [fn, want] of Object.entries(expect)) {
    let inFn;
    if (fn === '*') inFn = calls;
    else {
      const r = functionRange(code, fn);
      if (!r) { problems.push(`${file}: 関数 ${fn} が見つからない`); continue; }
      if (/\n[ \t]*(?:export\s+)?function\s/.test(code.slice(r[0], r[1]))) { problems.push(`${file}: ${fn} の本体を取り出せない (括弧の数え違い)`); continue; }
      inFn = calls.filter(([p]) => p >= r[0] && p < r[1]);
    }
    for (const c of inFn) claimed.add(c);
    const got = inFn.map(([, t]) => t);
    if (JSON.stringify(got) !== JSON.stringify(want)) problems.push(`${file} ${fn}: 期待 ${JSON.stringify(want)} / 実際 ${JSON.stringify(got)}`);
  }
  const stray = calls.filter((c) => !claimed.has(c)).map(([, t]) => t);
  if (stray.length) problems.push(`${file}: 表に無い所で読み口を呼んでいる ${JSON.stringify(stray)}`);
  return problems;
}
{
  // 読み口を import している本番のファイル = 表のファイル (読み手を足したら表に足さないと落ちる)
  //   + getSkuProfit に consumer を渡して読むファイル (margin-alert-job.js は読み口を import せずに opts.consumer で読む)
  const readers = [...SRC].filter(([r, code]) => !isTestName(r) && r !== 'lib/amazon-finance-read.js'
    && (/amazon-finance-read\.js['"]/.test(code) || /\bconsumer:\s*'/.test(code))).map(([r]) => r).sort();
  ok(JSON.stringify(readers) === JSON.stringify(Object.keys(ASSIGN).sort()), `読み口を使う本番のファイル ${readers.length} 本 = 割り当ての表のファイル`, readers);
  const problems = Object.entries(ASSIGN).flatMap(([file, expect]) => assignmentProblems(file, SRC.get(file) || '', expect));
  ok(problems.length === 0, '関数ごとの consumer の割り当てが表のとおり', problems);
  // 表の consumer は全部一覧にあり、一覧の consumer は全部どこかに割り当てられている。dataset も合う
  const tokens = Object.values(ASSIGN).flatMap((e) => Object.values(e).flat());
  const bad = tokens.filter((t) => {
    const [kind, ...rest] = t.split(':'); const c = rest.join(':');
    if (c.startsWith('$')) return false;
    const p = R.PROFILE_SETS.legacy[c];
    if (!p) return true;
    return kind === 'accountFeesTable' ? !('account_fees' in p) : !('finance_daily' in p);
  });
  ok(bad.length === 0, '表の consumer は全部一覧にあり、読む dataset を宣言している', bad);
  const assigned = new Set(tokens.map((t) => t.split(':').slice(1).join(':')));
  const unused = Object.keys(EXPECTED).filter((c) => !assigned.has(c));
  ok(unused.length === 0, '一覧の consumer は全部どこかの関数に割り当てられている', unused);
  // getSkuProfit が受ける consumer は 2 つだけ
  const q = SRC.get('apps/amazon-dashboard/queries.js') || '';
  ok(/const SKU_PROFIT_CONSUMERS = \['amazon-dashboard:sku-profit', 'margin-alert'\];/.test(q), "getSkuProfit が受ける consumer = 'amazon-dashboard:sku-profit'・'margin-alert' だけ");

  // 検査そのものの試験: 割り当てを 1 つ間違えると落ちる (diagnosis の settledBySku を ads に)
  const wrong = q.replace("settledBySku(db, adFrom, today, 'amazon-dashboard:diagnosis')", "settledBySku(db, adFrom, today, 'amazon-dashboard:ads')");
  ok(wrong !== q && assignmentProblems('apps/amazon-dashboard/queries.js', wrong, ASSIGN['apps/amazon-dashboard/queries.js']).some((p) => p.includes('getDiagnosis')),
    '割り当てを間違えると落ちる (getDiagnosis の settledBySku を amazon-dashboard:ads にした)');
  const moved = q.replace("FROM ${financeDailyTable('amazon-dashboard:trend')}", "FROM ${financeDailyTable('amazon-dashboard:overview')}");
  ok(moved !== q && assignmentProblems('apps/amazon-dashboard/queries.js', moved, ASSIGN['apps/amazon-dashboard/queries.js']).length > 0, '割り当てを間違えると落ちる (getTrend を overview にした)');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件 OK / ${fail} 件 NG`);
process.exitCode = fail ? 1 : 0;
