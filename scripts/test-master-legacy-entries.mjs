/**
 * test-master-legacy-entries.mjs — 古い入口の一覧 (config/master-legacy-entries.mjs) が、コードの書き込みの口を取りこぼしていないかを数える
 * (Company DB構想 14 §9 v2 M2「ルートを数える試験」・§10 契約 v3 H1・PR #1565 Codex R1 H4)
 *
 *   1. apps/・lib/・scripts/ の全部のコードを「関数・ルートのかたまり」に分け、マスタの表・列・ファイルに書くかたまりを見つける。
 *      さらに**関数の呼び出しをたどる** (ファイルをまたぐ: 読み込んだ名前・同じファイルで作った名前だけ)。書く関数を呼ぶかたまりも「書く」。
 *      書くルートは LEGACY_ENTRIES (閉じる) か LEGACY_EXEMPT (写しの口 = guard つき) に載っていなければ落とす = 新しい書き込みの口を黙って足せない
 *   2. 一覧のルート・画面がコードにある (古い行が無い)・閉じない口の理由の種類を確かめる (写し = guard・閉じ済み = router.use・手 = コード無し)
 *   3. ルートでない書き込みの口 (CLI = process.argv・定期実行 = cron.schedule) も一覧 (cli / job) か NON_ENTRY_WRITERS (人の入口ではない・理由つき) に載っている
 *   4. csv-import.js の mode は全部、閉じる か 止めない に分けてあり、閉じる mode は本当に書き・止める mode は書かない
 *   5. 門が実際に掛かっている: app ごとに masterLegacyGate・時間のかかる取込は legacyRecheck・CLI は legacyCliGate を 2 回・job は段階を読む・
 *      route_part の handler は res.locals.masterLegacyWrite を見る・楽天の出品の payload はいつも税率の決め方を渡す
 *
 * 使い方: node scripts/test-master-legacy-entries.mjs [--show] (DB もネットも使わない。ファイルを読むだけ)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { LEGACY_ENTRIES, LEGACY_EXEMPT, CLI_KEEP_MODES, MASTER_WRITE_TARGETS, WHEN_FROZEN } from '../config/master-legacy-entries.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SHOW = process.argv.includes('--show');
let failed = 0, passed = 0;
const ok = (cond, label, detail = '') => { if (cond) { passed++; console.log(`  ✓ ${label}`); } else { failed++; console.log(`  ✗ ${label}${detail ? `\n      ${detail}` : ''}`); } };

/** ルートでも一覧の CLI / job でもないのにマスタに書く口 (人の入口ではない・理由つき)。ここに足すときは理由を書く */
const NON_ENTRY_WRITERS = {
  'apps/warehouse/rebuild-m-products.js': 'm_products の毎朝の作り直し (上書き表と NE から導く。人の入口ではない。切替後は ④ の写し)',
  'apps/warehouse/record-m-products-history.js': 'm_products の履歴の記録 (作り直しの後)',
};

// ─── ファイルを集める ───
function walk(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (ent.name === 'node_modules' || ent.name.startsWith('.')) continue;
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else if (/\.(m?js|cjs)$/.test(ent.name)) out.push(p);
  }
  return out;
}
const rel = (p) => path.relative(ROOT, p).split(path.sep).join('/');
const isTestFile = (r) => /(^|\/)(test-[^/]*|[^/]*\.test\.m?js|[^/]*smoke[^/]*)$/.test(r) || /\/tests?\//.test(r);
const files = [...walk(path.join(ROOT, 'apps')), ...walk(path.join(ROOT, 'lib')), ...walk(path.join(ROOT, 'scripts'))]
  .map((p) => ({ abs: p, rel: rel(p) })).filter((f) => !isTestFile(f.rel));
const text = new Map(files.map((f) => [f.rel, fs.readFileSync(f.abs, 'utf8').replace(/\r\n/g, '\n')]));   // CRLF のファイルも同じ形で読む

// ─── かたまりに分ける (関数の定義・ルートの定義で区切る) ───
const ROUTE_RE = /\b([A-Za-z_$][\w$]*)\.(get|post|put|patch|delete|all)\(\s*(['"`])([^'"`]+)\3/;
// 関数の区切りは行の頭 (字下げなし) だけ = ルートの中の const で切らない
const FN_RE = /^(?:export\s+)?(?:async\s+)?function\s*\*?\s*([A-Za-z_$][\w$]*)\s*\(|^(?:export\s+)?const\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>|^(?:export\s+)?const\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s+)?function\b/;
function blocksOf(fileRel) {
  const lines = text.get(fileRel).split('\n');
  const starts = [];
  lines.forEach((l, i) => {
    const r = l.match(ROUTE_RE);
    if (r && /router|app|api|^r$/i.test(r[1]) && r[4].startsWith('/')) { starts.push({ i, route: { method: r[2].toUpperCase(), path: r[4] } }); return; }
    const f = l.match(FN_RE);
    if (f) starts.push({ i, name: f[1] || f[2] || f[3] });
  });
  const out = [{ file: fileRel, name: null, route: null, top: true, text: lines.slice(0, starts[0]?.i ?? lines.length).join('\n') }];
  starts.forEach((s, k) => out.push({ file: fileRel, name: s.name || null, route: s.route || null, text: lines.slice(s.i, k + 1 < starts.length ? starts[k + 1].i : lines.length).join('\n') }));
  return out;
}
const blocks = files.flatMap((f) => blocksOf(f.rel));

// ─── 書く印 ───
const SQL_WRITE = new RegExp(`(?:INSERT(?:\\s+OR\\s+\\w+)?\\s+INTO|UPDATE|DELETE\\s+FROM|REPLACE\\s+INTO)\\s+["\`]?(?:${MASTER_WRITE_TARGETS.tables.join('|')})\\b`, 'i');
const COLUMN_RULES = MASTER_WRITE_TARGETS.columns.map((c) => ({
  ...c,
  sql: new RegExp(`(?:INSERT(?:\\s+OR\\s+\\w+)?\\s+INTO|UPDATE)\\s+${c.table}\\b[\\s\\S]{0,400}?\\b${c.column}\\b`, 'i'),
}));
/** そのかたまりが直接書くか (理由を返す) */
function writesDirectly(b) {
  if (SQL_WRITE.test(b.text)) return 'SQL (表)';
  for (const c of COLUMN_RULES) {
    if (c.writers.some((w) => b.name === w)) continue;   // 汎用の書き手そのもの (呼ぶ側で列を見る)
    if (c.sql.test(b.text)) return `SQL (${c.id})`;
    for (const w of c.writers) {
      for (const m of b.text.matchAll(new RegExp(`(?<![.\\w$])${w}\\(([\\s\\S]*?)\\);`, 'g'))) {
        // 中身をその場で書く ({ ... }) = その列の文字があるときだけ / 変数をそのまま渡す = その列も入りうる (Notion の取込の rec.yahoo など)
        if (m[1].includes('{') ? new RegExp(`\\b${c.column}\\b`).test(m[1]) : true) return `${w}(…${c.column}…)`;
      }
    }
  }
  for (const f of MASTER_WRITE_TARGETS.files) if (b.text.includes(f.match) && /writeFileSync|writeFile\(/.test(b.text)) return `ファイル (${f.id})`;
  for (const d of MASTER_WRITE_TARGETS.dynamic) if (b.file === d.file && d.match.some((s) => b.text.includes(s))) return `表の名前を変数で (${d.id})`;
  for (const n of MASTER_WRITE_TARGETS.new_products) if (n.match.test(b.text)) return `新商品を作る (${n.id})`;
  return null;
}
/** ファイルが読み込んだ名前 → { from: 読み込んだファイル, name: 元の名前 } (相対の import だけ) */
const importCache = new Map();
function importsOf(fileRel) {
  if (importCache.has(fileRel)) return importCache.get(fileRel);
  const map = new Map();
  for (const m of text.get(fileRel).matchAll(/import\s*\{([^}]*)\}\s*from\s*['"]([^'"]+)['"]/g)) {
    if (!m[2].startsWith('.')) continue;
    let from = path.posix.normalize(path.posix.join(path.posix.dirname(fileRel), m[2]));
    if (!text.has(from)) for (const ext of ['.js', '.mjs']) if (text.has(from + ext)) { from += ext; break; }
    if (!text.has(from)) continue;
    for (const part of m[1].split(',')) {
      const [orig, alias] = part.trim().split(/\s+as\s+/);
      if (orig) map.set((alias || orig).trim(), { from, name: orig.trim() });
    }
  }
  importCache.set(fileRel, map);
  return map;
}
/** name( の呼び出しが、どのファイルのどの関数か ("ファイル#関数名"。分からなければ null = 同じ名前の別の関数と取り違えない) */
function resolveCall(fileRel, name) {
  if (blocks.some((x) => x.file === fileRel && x.name === name)) return `${fileRel}#${name}`;
  const im = importsOf(fileRel).get(name);
  return im ? `${im.from}#${im.name}` : null;
}
// 関数の呼び出しをたどって、書く関数を広げる (動かなくなるまで)。鍵 = "ファイル#関数名"
const writerWhy = new Map();
for (const b of blocks) { const w = writesDirectly(b); if (w && b.name) writerWhy.set(`${b.file}#${b.name}`, `${b.file}: ${w}`); }
function callsWriter(b) {
  const names = new Set([...writerWhy.keys()].map((k) => k.split('#')[1]));
  for (const name of names) {
    if (name === b.name) continue;
    if (!new RegExp(`(?<![.\\w$])${name}\\(`).test(b.text)) continue;
    const key = resolveCall(b.file, name);
    if (key && writerWhy.has(key)) return `${name}() ← ${writerWhy.get(key)}`;
  }
  return null;
}
for (let round = 0; round < 10; round++) {
  let grew = false;
  for (const b of blocks) {
    if (!b.name || writerWhy.has(`${b.file}#${b.name}`)) continue;
    const w = callsWriter(b);
    if (w) { writerWhy.set(`${b.file}#${b.name}`, `${b.file}: ${w}`); grew = true; }
  }
  if (!grew) break;
}
const whyOf = (b) => writesDirectly(b) || callsWriter(b);

console.log('── 1. マスタの表・列・ファイルに書くルートは、全部一覧にある (関数の呼び出しをたどる) ──');
const listedKey = (e) => `${e.file} ${e.method} ${e.path}`;
const entryKeys = new Set(LEGACY_ENTRIES.filter((e) => e.file && e.method && e.path).map(listedKey));
const exemptKeys = new Set(LEGACY_EXEMPT.filter((e) => e.file && e.method && e.path).map(listedKey));
const routeIndex = new Set();
const flagged = [];
/**
 * GET も見る (Codex #1565 R2 Low 4: GET を丸ごと飛ばすと、読むだけに見えて外に出す口を見落とす)。
 * GET で数えるのは: マスタの表・列・ファイルに書く (POST と同じ決め方) / NE のマスタを書き換えるファイルを作る (MASTER_WRITE_TARGETS.exports)
 */
const exportOf = (b) => {
  for (const x of MASTER_WRITE_TARGETS.exports) if (x.match.test(b.text)) return `外への出口 (${x.id})`;
  return null;
};
const flaggedGets = [];
for (const b of blocks) {
  if (!b.route) continue;
  const key = `${b.file} ${b.route.method} ${b.route.path}`;
  routeIndex.add(key);
  const why = b.route.method === 'GET' ? (whyOf(b) || exportOf(b)) : whyOf(b);
  if (why) { flagged.push({ key, why }); if (b.route.method === 'GET') flaggedGets.push({ key, why }); }
}
if (SHOW) for (const x of flagged) console.log(`    · ${x.key} ← ${x.why}`);
const missing = flagged.filter((x) => !entryKeys.has(x.key) && !exemptKeys.has(x.key));
ok(flagged.length >= 40, `マスタに書くルートを見つけた (${flagged.length} 本) = 探し方が効いている`);
ok(missing.length === 0, 'マスタに書くルートは全部 LEGACY_ENTRIES か LEGACY_EXEMPT にある', missing.map((x) => `${x.key} ← ${x.why}`).join('\n      '));
// 関数をまたいで見つける力 (Codex R1: 同じファイルの文字だけでは Notion の取込を見落とした)
const mustFind = ['apps/product-hub/router.js POST /api/notion-import', 'apps/product-hub/router.js POST /api/register-codes', 'apps/profit-calculator/router.js POST /api/suppliers', 'apps/supplier-sales/router.js POST /api/supplier-name'];
ok(mustFind.every((k) => flagged.some((x) => x.key === k)), '別のファイルの関数越しに書くルートも見つける (Notion の取込・NE のコードから登録・仕入れ先 JSON・売れ筋共有の表示名)',
  mustFind.filter((k) => !flagged.some((x) => x.key === k)).join(', '));
// GET の外への出口も決まりで見つける (一覧に手で足したものに頼らない)
ok(flaggedGets.some((x) => x.key === 'apps/profit-calculator/router.js GET /api/products/csv/ne'), 'GET でも、NE のマスタ取込の CSV を作る口 (profit-calculator の /api/products/csv/ne) を決まりで見つける',
  flaggedGets.map((x) => `${x.key} ← ${x.why}`).join('\n      ') || '(GET は 1 つも見つからない)');
if (SHOW) for (const x of flaggedGets) console.log(`    · GET ${x.key} ← ${x.why}`);

console.log('── 2. 一覧がコードと合う・閉じない口の理由を確かめる ──');
const stale = [...LEGACY_ENTRIES, ...LEGACY_EXEMPT].filter((e) => e.file && e.method && e.path).filter((e) => !routeIndex.has(listedKey(e)));
ok(stale.length === 0, '一覧のルート・画面がコードにある (古い行が無い)', stale.map((e) => `${e.id} → ${listedKey(e)}`).join('\n      '));
ok(LEGACY_ENTRIES.every((e) => WHEN_FROZEN[e.when_frozen]), '閉じる入口は全部 when_frozen (閉じたときの動き) を持つ');
// 閉じる route・route_field・route_part は本当に書く (書かないものを載せて数をごまかさない)
const notWriting = LEGACY_ENTRIES.filter((e) => ['route', 'route_field', 'route_part'].includes(e.kind)).filter((e) => !flagged.some((x) => x.key === listedKey(e)));
ok(notWriting.length === 0, '閉じる API は本当にマスタに書く (見つけた書き込みの口と一致)', notWriting.map((e) => e.id).join(', '));
for (const e of LEGACY_EXEMPT) {
  if (e.kind === 'replication') {
    const src = text.get(e.file) || '';
    const def = src.split('\n').find((l) => l.includes(`.${e.method.toLowerCase()}('${e.path}'`));
    ok(!!def && def.includes(e.guard), `写しの口 ${e.id}: ルートの定義に ${e.guard} (人の入口ではない)`, def || 'ルートが無い');
  } else if (e.kind === 'already_closed') {
    const src = text.get(e.file) || '';
    const useAt = src.indexOf(`router.use(${e.guard})`);
    const firstWrite = src.search(/router\.(post|put|patch|delete)\(/);
    ok(useAt >= 0 && (firstWrite < 0 || useAt < firstWrite), `閉じ済み ${e.id}: router.use(${e.guard}) が全部の書き込みのルートより前`);
  } else if (e.kind === 'manual') {
    ok(!e.file && !e.method && !e.path, `手の入口 ${e.id}: コードを持たない (切替の証拠 manual_entries_stopped に載せる)`);
  } else if (e.kind === 'company_db_outbox') {
    // ⑤-3b のマージで見つけた: ボードを開いたときの新しい登録の知らせの取り込み = 新しい道。MASTER_EDIT_OPEN = 1 の Render だけ (guard が書き手にある)
    const src = text.get(e.writer_file) || '';
    ok(e.method === 'GET' && src.includes(e.guard), `新しい道の口 ${e.id}: ${e.writer_file} は ${e.guard} のときだけ取り込む`);
  } else if (e.kind === 'seed_on_read') {
    const src = text.get(e.writer_file) || '';
    ok(e.method === 'GET' && src.includes(e.guard) && src.includes('existsSync('), `初期データだけの口 ${e.id}: ${e.writer_file} は無いときだけ ${e.guard} を書く`);
  }
}

console.log('── 3. ルートでない書き込みの口 (CLI・定期実行) も一覧にある ──');
{
  const cliFiles = new Set(LEGACY_ENTRIES.filter((e) => e.kind === 'cli').map((e) => e.file));
  const jobFiles = new Set(LEGACY_ENTRIES.filter((e) => e.kind === 'job').map((e) => e.file));
  const unlisted = [];
  for (const f of files) {
    const src = text.get(f.rel);
    const isCli = /process\.argv/.test(src) && /\bisMain\b|^main\(\)|^await main\(\)|^\s*main\(\)\.catch/m.test(src);
    const isJob = /cron\.schedule\(/.test(src);
    if (!isCli && !isJob) continue;
    const writerBlock = blocks.find((b) => b.file === f.rel && !b.route && whyOf(b));
    if (!writerBlock) continue;
    if (cliFiles.has(f.rel) || jobFiles.has(f.rel) || NON_ENTRY_WRITERS[f.rel]) continue;
    unlisted.push(`${f.rel} ← ${whyOf(writerBlock)}`);
  }
  ok(unlisted.length === 0, 'マスタに書く CLI・定期実行は全部 cli / job の入口か、人の入口ではない書き手 (理由つき)', unlisted.join('\n      '));
  // 許す一覧に古い行を残さない (本当にマスタに書くファイルだけ)
  for (const f of Object.keys(NON_ENTRY_WRITERS)) ok(text.has(f) && blocks.some((b) => b.file === f && whyOf(b)), `人の入口ではない書き手 ${f} は本当にマスタに書く (古い行ではない)`);
}

console.log('── 4. csv-import.js の mode は全部分けてある ──');
{
  const src = text.get('apps/warehouse/csv-import.js');
  const block = src.slice(src.indexOf('const handlers = {'), src.indexOf('};', src.indexOf('const handlers = {')));
  const modes = [...block.matchAll(/^\s{4}(\w+):\s*\(\)/gm)].map((m) => m[1]);
  const closed = new Set(LEGACY_ENTRIES.filter((e) => e.kind === 'cli' && e.file === 'apps/warehouse/csv-import.js').map((e) => e.mode));
  const keep = new Set(Object.keys(CLI_KEEP_MODES['apps/warehouse/csv-import.js'] || {}));
  ok(modes.length >= 7, `csv-import.js の mode を読めた (${modes.join(', ')})`);
  ok(modes.every((m) => closed.has(m) || keep.has(m)), 'mode は全部「閉じる」か「止めない」', modes.filter((m) => !closed.has(m) && !keep.has(m)).join(', '));
  ok([...closed].every((m) => !keep.has(m)), '「閉じる」と「止めない」が重ならない');
  for (const m of modes) {
    const fn = (block.match(new RegExp(`^\\s{4}${m}:\\s*\\(\\)\\s*=>\\s*(\\w+)\\(`, 'm')) || [])[1];
    if (!fn) continue;
    const body = blocks.find((b) => b.file === 'apps/warehouse/csv-import.js' && b.name === fn);
    const writes = !!(body && whyOf(body));
    ok(closed.has(m) ? writes : !writes, `csv-import.js ${m} (${fn}) は ${closed.has(m) ? '閉じる = マスタに書く' : '止めない = マスタに書かない'}`);
  }
}

console.log('── 5. 門が実際に掛かっている ──');
{
  const apps = [...new Set(LEGACY_ENTRIES.filter((e) => e.app).map((e) => e.app))];
  for (const app of apps) {
    const routerFile = [...new Set(LEGACY_ENTRIES.filter((e) => e.app === app).map((e) => e.file))].find((f) => /router\.m?js$/.test(f));
    ok((text.get(routerFile) || '').includes(`router.use(masterLegacyGate('${app}'))`), `${app}: ${routerFile} が router.use(masterLegacyGate('${app}'))`);
  }
  ok((text.get('apps/warehouse/router.js') || '').includes('mountSkuMasterApi(router)'), 'SKU マスタの API は warehouse の router (門の後ろ) に載る');
  for (const e of LEGACY_ENTRIES.filter((x) => x.recheck && x.kind === 'route')) {
    const def = (text.get(e.file) || '').split('\n').find((l) => l.includes(`router.${e.method.toLowerCase()}('${e.path}'`));
    ok(!!def && def.includes(`legacyRecheck('${e.id}')`), `${e.id}: ファイルを受け取った後・書く前にもう一度読む (legacyRecheck)`, def || 'ルートが無い');
  }
  for (const e of LEGACY_ENTRIES.filter((x) => x.kind === 'cli')) {
    const src = text.get(e.file) || '';
    const q = e.mode === '*' ? `'${e.id}'` : 'legacyEntry.id';
    const first = src.includes(`legacyCliGate(${q})`);
    const locked = src.includes(`runWithLegacyCliLock(${q},`);
    ok(first && locked, `${e.id}: mode が分かったらすぐ門を通し (legacyCliGate)、書くところは段階の鍵を持って読み直す (runWithLegacyCliLock)`);
  }
  for (const e of LEGACY_ENTRIES.filter((x) => x.kind === 'job')) {
    const src = text.get(e.file) || '';
    // 門の共通の包み (runLegacyJob = 毎回段階を読む・終わるまで書きかけに数える。Codex #1565 R2 Medium 2) を通し、流さなかったら丸ごと止める
    ok(src.includes(`await runLegacyJob('${e.id}', `) && /if \(!job\.ran\) \{[\s\S]{0,600}?return;/.test(src), `${e.id}: 門の共通の包み (runLegacyJob) で毎回段階を読み・書きかけに数え、閉じていれば丸ごと止める (ログを残して return)`);
  }
  for (const e of LEGACY_ENTRIES.filter((x) => x.kind === 'route_part')) {
    const b = blocks.find((x) => x.route && x.file === e.file && x.route.method === e.method && x.route.path === e.path);
    ok(!!b && /res\.locals\.masterLegacyWrite\?\.writable === true/.test(b.text), `${e.id}: handler が res.locals.masterLegacyWrite.writable のときだけマスタの部分を書く`);
  }
  // 楽天の出品の payload は、いつも税率の決め方 (切替前 = legacy / 閉じた後 = Company DB) を渡す (R1 H5)
  const calls = [];
  for (const f of files) for (const m of text.get(f.rel).matchAll(/buildItemPayload\(([^)]*)\)/g)) if (!/^\s*db,\s*draftId,\s*\{\s*tax\s*=\s*null\s*\}\s*=\s*\{\}\s*$/.test(m[1])) calls.push(`${f.rel}: ${m[1]}`);
  // { tax: … } か { tax } (直前に const tax = await resolveListingTax(…) がある) の形
  const passesTax = (c) => /\{\s*tax:/.test(c) || (/\{\s*tax\s*\}/.test(c) && /const tax = await resolveListingTax\(/.test(text.get(c.split(':')[0])));
  ok(calls.length >= 2 && calls.every(passesTax), `楽天の出品の payload を作る呼び出しは全部 { tax } を渡す (${calls.length} か所)`, calls.filter((c) => !passesTax(c)).join('\n      '));
}

console.log(`\n${failed ? '❌' : '✅'} ${passed} 件 OK / ${failed} 件 NG`);
if (failed) process.exitCode = 1;
