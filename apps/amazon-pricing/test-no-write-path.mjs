/**
 * test-no-write-path.mjs — 「Amazon へ書き込む経路が無い」ことを機械的に確かめる。
 *
 * ★このアプリの安全は kill switch ではなく「書き込むコードが存在しない」ことで担保している。
 *   env を入れても、API を直叩きしても、価格は変わらない。
 *   将来、実行段階 (M3) を足すときは、このテストを**意図して**書き換えることになる
 *   (= レビューで必ず目に入る)。
 *
 * 見るもの:
 *   1. apps/amazon-pricing/ の全ファイルに、SP-API の書き込み関数・miniPC の書き込み口・外部呼び出し (fetch) が無い
 *   2. 旧ツール (profit-calculator) に残っていた価格書き込みの口 (update-price / worker 起動) が消えている
 *   3. 旧ワーカー (price-scheduler.js / price-engine.js) が存在しない
 *
 * 実行: node apps/amazon-pricing/test-no-write-path.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '../..');
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// 単語を分けて書いてあるのは、このテスト自身が自分の禁止語に引っかからないようにするため
const j = (...parts) => parts.join('');
const FORBIDDEN = [
  j('update', 'Price'),                 // profit-calculator/sp-api.js の価格更新
  j('patch', 'ListingsItem'),           // SP-API Listings Items API の書き込み operation
  j('patch', 'Listing('),               // miniPC 経由の出品 patch
  j('put', 'ListingsItem'),
  j('create', 'Feed'),                  // Feeds API
  j('service-api/', 'research'),        // miniPC の SP-API 中継 (書き込みも含む)
  j('call', 'MiniPC'),
  j('amazon-', 'sp-api'),               // SP-API クライアント本体
  j('sp-api', '.js'),                   // 旧ツールの SP-API モジュール
  j('WAREHOUSE', '_URL'),
  j('Selling', 'Partner'),
  j('fetch', '('),                      // 外部への呼び出しそのもの (サーバ側コードに限る)
  j('client-', 'sqs'),                  // 旧ワーカーの通知受信
  j('purchasable', '_offer'),           // 価格属性の patch 本文
];

function walk(dir, out = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, out);
    else if (/\.(js|mjs|ejs)$/.test(e.name) && !/^test-/.test(e.name)) out.push(p);
  }
  return out;
}

console.log('\n── 1. apps/amazon-pricing/ に書き込み経路が無い ──');
{
  const files = walk(HERE);
  ok(files.length >= 8, `対象ファイル ${files.length} 本`);
  for (const f of files) {
    const src = fs.readFileSync(f, 'utf8');
    const rel = path.relative(ROOT, f).replace(/\\/g, '/');
    const isView = f.endsWith('.ejs');
    for (const word of FORBIDDEN) {
      // 画面のブラウザ側 JS は自分のサーバ (/apps/amazon-pricing/api/...) にしか fetch しない。それは許す
      if (isView && word === j('fetch', '(')) {
        const bad = (src.match(/fetch\((['"])(?!\/apps\/amazon-pricing\/api\/)/g) || []).length;
        ok(bad === 0, `${rel}: fetch は自分の API 以外を呼ばない`);
        continue;
      }
      ok(!src.includes(word), `${rel}: "${word}" が無い`);
    }
  }
  // import 先も自分のアプリ内 + 決まった読み取り専用モジュールだけ
  const ALLOWED_IMPORTS = new Set(['express', 'path', 'url', 'node:fs', 'node:path', 'node:url', 'node:os', 'better-sqlite3',
    '../warehouse-mirror/db.js', '../price-update/format.js']);
  for (const f of files.filter((x) => x.endsWith('.js'))) {
    const src = fs.readFileSync(f, 'utf8');
    const rel = path.relative(ROOT, f).replace(/\\/g, '/');
    const imports = [...src.matchAll(/^import\s[^'"]*['"]([^'"]+)['"]/gm)].map((m) => m[1]);
    const outside = imports.filter((i) => !i.startsWith('./') && !ALLOWED_IMPORTS.has(i));
    ok(outside.length === 0, `${rel}: import は許可リスト内だけ (${outside.join(', ') || 'OK'})`);
  }
}

console.log('\n── 2. 旧ツールの書き込み口が消えている ──');
{
  const router = fs.readFileSync(path.join(ROOT, 'apps/profit-calculator/router.js'), 'utf8');
  ok(!router.includes(j("router.post('/api/amazon/", "update-price'")), 'profit-calculator: POST /api/amazon/update-price が無い');
  ok(!router.includes(j("'/api/price-revision/", "worker/start'")), 'profit-calculator: POST /api/price-revision/worker/start が無い');
  ok(!router.includes(j('start', 'PriceWorker')), 'profit-calculator: ワーカー起動の import が無い');
  ok(!router.includes(j("callMiniPC('/", "price'")), 'profit-calculator: miniPC の /price を呼ぶ関数が無い');
  const server = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  // コメントで名前を挙げるのは構わない。import 文 (= 実行される) が無いことを見る
  ok(!new RegExp(`^import[^\\n]*${j('price-', 'scheduler')}`, 'm').test(server), 'server.js: price-scheduler を import していない');
  ok(!new RegExp(`^\\s*${j('start', 'PriceWorker')}\\(`, 'm').test(server), 'server.js: ワーカーを起動していない');
  ok(server.includes("requireAppAccess('amazon-pricing')"), 'server.js: 新アプリは画面権限つきでマウント');
}

console.log('\n── 3. 旧ワーカーのファイルが無い ──');
{
  for (const f of ['apps/profit-calculator/price-scheduler.js', 'apps/profit-calculator/price-engine.js']) {
    ok(!fs.existsSync(path.join(ROOT, f)), `${f} が存在しない`);
  }
}

console.log(`\n${failed === 0 ? '🎉 ALL PASS — Amazon へ書き込む経路はありません' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
