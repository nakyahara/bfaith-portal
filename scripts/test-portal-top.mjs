/**
 * test-portal-top.mjs — ポータルトップ (検索・10 分類・グループカード) の smoke。
 *
 *   A. registry: 書き方の抜けがない / 権限のキー (id) とパスが 1 本も変わっていない
 *   B. 組み立て: 管理者・iPad の現場・一部のモールだけ見える人・warehouse variant で、
 *      見えてよいものだけが、正しい形 (グループ or 普通のカード) で出る
 *   C. 描画: dashboard.ejs を dashboardLocals() の値で本当に描画する
 *      (server.js の GET / と同じ関数で値を作るので、テンプレートへの渡し忘れもここで分かる)
 *   D. 検索: ブラウザ側 (public/js/portal-top.js) とサーバー側の正規化が一致する
 *   E. 実物に近いルート: express に static と GET / を載せて本当に叩く
 *
 * 実行: node scripts/test-portal-top.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import ejs from 'ejs';
import express from 'express';
import { apps, externalLinks, categories } from '../lib/portal-apps.js';
import { validateRegistry, buildDashboard, dashboardLocals, normalizeForSearch, SUMMARY_MAX } from '../lib/portal-dashboard.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const VIEW = path.join(ROOT, 'views', 'dashboard.ejs');
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// ── A. registry ──
const problems = validateRegistry();
ok(problems.length === 0, `registry の書き方に抜けがない${problems.length ? '\n   ' + problems.join('\n   ') : ''}`);

// 2026-09-13 時点 (origin/master deeb862e) のアプリ id とパス。権限 (allowedApps) のキーなので変えてはいけない
const EXPECTED_IDS = [
  'product-hub', 'product-links', 'linegift-sync', 'mercari-sync', 'rakuten-yahoo-sync', 'aes-pdf-sorter',
  'ranking-checker', 'profit-calculator', 'fba-replenishment', 'warehouse', 'amazon-accounting',
  'amazon-usa-accounting', 'rakuten-accounting', 'aupay-accounting', 'yahoo-accounting', 'qoo10-accounting',
  'linegift-accounting', 'fba-profitability', 'mercari-accounting', 'profit-analysis', 'amazon-dashboard',
  'rakuten-analytics', 'yahoo-analytics', 'aupay-analytics', 'qoo10-analytics', 'biz-ops-overview',
  'product-management-list', 'cross-sell-finder', 'inbound-info', 'inbound-check', 'iroha-work', 'fba-box',
  'staff', 'giftset-assembly', 'sales-analytics-linegift', 'shipping-log', 'shipping-work', 'picking',
  'easy-ship', 'select-set', 'packing-dispatch', 'mgmt-accounting', 'shohyo-links', 'inventory-monthly',
  'exec-dashboard', 'product-scout', 'mis-shipment', 'purchase-orders', 'price-update', 'amazon-pricing',
  'inquiry-hub', 'ai-insights', 'supplier-sales', 'postage',
];
const SPECIAL_PATHS = {
  'profit-calculator': '/apps/profit-calculator/',
  'inbound-check': '/apps/inbound-check/',
  'iroha-work': '/apps/iroha-work/',
  'fba-box': '/apps/fba-box/admin',
  'staff': '/apps/staff/',
  'product-scout': '/apps/product-scout/keywords',
  'amazon-pricing': '/apps/amazon-pricing/',
};
const ids = apps.map(a => a.id);
ok(ids.length === 54 && new Set(ids).size === 54, `アプリは 54 本で id の重複なし (実際 ${ids.length} 本)`);
const missing = EXPECTED_IDS.filter(id => !ids.includes(id));
const extra = ids.filter(id => !EXPECTED_IDS.includes(id));
ok(missing.length === 0 && extra.length === 0, `権限 id が変わっていない${missing.length ? ' 消えた: ' + missing : ''}${extra.length ? ' 増えた: ' + extra : ''}`);
const pathDiff = apps.filter(a => a.path !== (SPECIAL_PATHS[a.id] || `/apps/${a.id}`)).map(a => `${a.id}=${a.path}`);
ok(pathDiff.length === 0, `パスが変わっていない${pathDiff.length ? ' ' + pathDiff : ''}`);
ok(apps.every(a => typeof a.description === 'string' && a.description.length > 0), '元の長い説明 (description) を開発メモとして残している');
ok(apps.every(a => [...a.summary].length <= SUMMARY_MAX), `一行説明はすべて ${SUMMARY_MAX} 字以内`);
ok(categories.some(c => c.id === 'data'), "分類 id 'data' が残っている (warehouse variant が参照)");
ok(externalLinks.find(x => x.id === 'x-order-conditions')?.status === 'archived', '発注条件参照ツールはしまった (archived)');

// ── B. 組み立て ──
const cardIds = model => model.sections.flatMap(s => s.cards.flatMap(c => c.kind === 'group' ? c.chips.map(ch => ch.id) : [c.id]));

const admin = buildDashboard({ allowedApps: '*', variant: 'render' });
ok(admin.sections.length === 10, `管理者: 10 分類 (実際 ${admin.sections.length})`);
ok(admin.totalCards === 43, `管理者: トップのカードは 43 枚 (実際 ${admin.totalCards})`);
const maxPerSec = Math.max(...admin.sections.map(s => s.visibleCount));
ok(maxPerSec <= 8, `管理者: 1 分類 8 枚まで (最大 ${maxPerSec})`);
ok(admin.showSearch && admin.hasArchived, '管理者: 検索窓と「しまったアプリも探す」が出る');
const adminIds = cardIds(admin);
ok(EXPECTED_IDS.every(id => adminIds.filter(x => x === id).length === 1), '管理者: 54 本すべてが 1 回ずつ (カードかグループのボタンで) 出る');
const groups = admin.sections.flatMap(s => s.cards.filter(c => c.kind === 'group'));
ok(groups.length === 2 && groups.find(g => g.id === 'mall-analytics')?.chips.length === 6 && groups.find(g => g.id === 'mall-accounting')?.chips.length === 8,
  '管理者: モール別分析 (6) とモール別 売上集計 (8) がグループカードになる');
const archivedCards = admin.sections.flatMap(s => s.cards.filter(c => c.archived));
ok(archivedCards.length === 1 && archivedCards[0].id === 'x-order-conditions', '管理者: しまったカードは発注条件参照ツールだけ');

const ipad = buildDashboard({ allowedApps: ['iroha-work', 'inbound-check'], variant: 'render' });
ok(ipad.totalCards === 2 && !ipad.showSearch, 'iPad (2 本だけ): カード 2 枚・検索窓は出さない');
ok(JSON.stringify(cardIds(ipad).sort()) === JSON.stringify(['inbound-check', 'iroha-work']), 'iPad: 権限のあるアプリだけ');
ok(ipad.sections.every(s => s.cards.every(c => !c.external)), 'iPad: 外部リンクは出ない (管理者だけ)');

const oneMall = buildDashboard({ allowedApps: ['rakuten-analytics'], variant: 'render' });
const oneCard = oneMall.sections[0]?.cards[0];
ok(oneMall.totalCards === 1 && oneCard?.kind === 'app' && oneCard.id === 'rakuten-analytics' && oneCard.name === '楽天分析ツール',
  'モールが 1 本だけ見える人: グループにせず普通のカード (楽天分析ツール)');

const twoMalls = buildDashboard({ allowedApps: ['rakuten-analytics', 'yahoo-analytics', 'purchase-orders'], variant: 'render' });
const g2 = twoMalls.sections.flatMap(s => s.cards).find(c => c.kind === 'group');
ok(g2 && g2.chips.map(c => c.id).join(',') === 'rakuten-analytics,yahoo-analytics', 'モールが 2 本見える人: グループのボタンは見られる 2 本だけ');
ok(JSON.stringify(cardIds(twoMalls).sort()) === JSON.stringify(['purchase-orders', 'rakuten-analytics', 'yahoo-analytics']), '一部の権限: 余計なアプリが混ざらない');

const nothing = buildDashboard({ allowedApps: [], variant: 'render' });
ok(nothing.sections.length === 0 && nothing.totalCards === 0, '権限なし: 何も出ない');

const whAdmin = buildDashboard({ allowedApps: '*', variant: 'warehouse' });
ok(whAdmin.totalCards === 1 && whAdmin.sections[0].cards[0].href === '/apps/warehouse/register' && whAdmin.sections[0].name === 'マスタ・設定',
  'warehouse variant (管理者): 「マスタ登録」だけ');
ok(buildDashboard({ allowedApps: ['warehouse'], variant: 'warehouse' }).totalCards === 1, 'warehouse variant: warehouse 権限があれば「マスタ登録」');
ok(buildDashboard({ allowedApps: ['picking'], variant: 'warehouse' }).totalCards === 0, 'warehouse variant: warehouse 権限がなければ何も出ない');

// ── C. 描画 ──
const session = (allowedApps, role = 'user') => ({ allowedApps, role, email: 'someone@b-faith.biz', displayName: 'テスト' });
const render = locals => ejs.renderFile(VIEW, locals);
const hrefs = html => [...html.matchAll(/href="([^"]+)"/g)].map(m => m[1]);

const adminHtml = await render(dashboardLocals({ session: session('*', 'admin'), variant: 'render' }));
ok(adminHtml.includes('id="top-q"') && adminHtml.includes('/js/portal-top.js'), '描画 (管理者): 検索窓とスクリプトがある');
ok(adminHtml.includes('id="top-arch"') && /data-archived hidden/.test(adminHtml), '描画 (管理者): しまったカードは最初から隠れている');
ok(!adminHtml.includes('稼働中'), '描画: 「稼働中」バッジはもう出さない');
ok((adminHtml.match(/class="top-nav-item/g) || []).length === 10, '描画 (管理者): 分類ナビが 10 個');
ok(adminHtml.includes('href="/admin/users"'), '描画 (管理者): ユーザー管理のリンク');
ok(EXPECTED_IDS.every(id => hrefs(adminHtml).includes(SPECIAL_PATHS[id] || `/apps/${id}`)), '描画 (管理者): 54 本すべてのリンクがある');
ok(/target="_blank" rel="noopener"/.test(adminHtml), '描画: 外部リンクは新しいタブ');

const ipadHtml = await render(dashboardLocals({ session: session(['iroha-work', 'inbound-check']), variant: 'render' }));
const ipadAppLinks = hrefs(ipadHtml).filter(h => h.startsWith('/apps/'));
ok(JSON.stringify(ipadAppLinks.sort()) === JSON.stringify(['/apps/inbound-check/', '/apps/iroha-work/']), '描画 (iPad): アプリへのリンクは権限のある 2 本だけ');
ok(!ipadHtml.includes('id="top-q"') && !ipadHtml.includes('portal-top.js') && !ipadHtml.includes('class="top-nav"'), '描画 (iPad): 検索窓・分類ナビ・スクリプトは出さない');
ok(!ipadHtml.includes('/admin/users') && !ipadHtml.includes('script.google.com'), '描画 (iPad): 管理者リンクも外部リンクも出ない');

const noneHtml = await render(dashboardLocals({ session: session([]), variant: 'render' }));
ok(noneHtml.includes('使えるアプリがまだありません'), '描画 (権限なし): 案内が出る');

const whHtml = await render(dashboardLocals({ session: session('*', 'admin'), variant: 'warehouse' }));
ok(whHtml.includes('/apps/warehouse/register') && hrefs(whHtml).filter(h => h.startsWith('/apps/')).length === 1, '描画 (warehouse variant): マスタ登録だけ');

// ── D. 検索の正規化 ──
const code = fs.readFileSync(path.join(ROOT, 'public', 'js', 'portal-top.js'), 'utf8');
const sandbox = { window: {}, document: { getElementById: () => null } };
vm.runInNewContext(code, sandbox);
const clientNormalize = sandbox.window.__portalTopNormalize;
const samples = ['プライスター', 'ﾌﾟﾗｲｽﾀｰ', 'ＡＢＣ　ｄｅｆ', '楽天 分析', 'Qoo10', 'ＦＢＡ納品', 'メールディーラー', 'au PAY', 'ヴァ・ヵヶ'];
ok(typeof clientNormalize === 'function' && samples.every(s => clientNormalize(s) === normalizeForSearch(s)), '検索: ブラウザ側とサーバー側の正規化が一致');
const findCard = id => admin.sections.flatMap(s => s.cards).find(c => c.id === id);
ok(findCard('amazon-pricing').search.includes(normalizeForSearch('ﾌﾟﾗｲｽﾀｰ')), '検索: 「ﾌﾟﾗｲｽﾀｰ」(半角カナ) で Amazon 価格管理に当たる');
ok(findCard('inquiry-hub').search.includes(normalizeForSearch('めーるでぃーらー')), '検索: 旧名「めーるでぃーらー」で問い合わせ管理に当たる');
ok(findCard('purchase-orders').search.includes(normalizeForSearch('発注対象商品シート')), '検索: 旧名「発注対象商品シート」で発注補助に当たる');
const mallAnalytics = findCard('mall-analytics');
ok(!mallAnalytics.search.includes(normalizeForSearch('楽天')) && mallAnalytics.chips.find(c => c.id === 'rakuten-analytics').search.includes(normalizeForSearch('楽天')),
  '検索: モール名はグループ本体ではなくボタン側で当てる (「楽天 分析」で楽天のボタンだけ光る)');

// ── E. 実物に近いルート ──
const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(ROOT, 'views'));
app.use(express.static(path.join(ROOT, 'public')));
app.get('/', (req, res) => res.render('dashboard', dashboardLocals({ session: session('*', 'admin'), variant: 'render' })));
const server = await new Promise(resolve => { const s = app.listen(0, () => resolve(s)); });
const base = `http://127.0.0.1:${server.address().port}`;
try {
  const top = await fetch(base + '/');
  const js = await fetch(base + '/js/portal-top.js');
  ok(top.status === 200 && (await top.text()).includes('モール別分析'), 'ルート: GET / が 200 でトップを返す');
  ok(js.status === 200 && (await js.text()).includes('__portalTopNormalize'), 'ルート: /js/portal-top.js が配信される');
} finally {
  // fetch の keep-alive 接続を閉じ切ってから終わる (閉じている途中で process.exit すると
  // Windows で libuv が UV_HANDLE_CLOSING の assertion で落ちる)
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}

console.log(failed ? `\n❌ ${failed} 件失敗` : '\n✅ すべて OK');
process.exitCode = failed ? 1 : 0;
