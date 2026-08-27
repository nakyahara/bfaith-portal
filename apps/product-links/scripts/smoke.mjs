/**
 * 商品リンク台帳 smoke (DB 分離・外部 API 非接続)。
 * 実行: DATA_DIR=/tmp/pl-smoke node apps/product-links/scripts/smoke.mjs
 * 検証: URL 正規化 / upsert 冪等・多由来 / product-hub 同期 (追加・変更・空・削除) / primary 一意 / 検索 (NFKC・セット継承) / EJS 描画
 */
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import ejs from 'ejs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
if (!process.env.DATA_DIR) { console.error('FATAL: DATA_DIR を指定してください'); process.exit(1); }
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });

let failed = 0;
const check = (name, cond, detail = '') => { if (cond) console.log(`  OK  ${name}`); else { failed++; console.error(`  NG  ${name} ${detail}`); } };

const pl = await import('../db.js');
const { analyzeUrl, normalizeCode, normalizeText, upsertLink, detachStaleSources, setPrimary, searchProducts, linksByCodes, invalidateCatalogCache, updateLinkMeta, softDeleteLink } = pl;
const { syncDraftLinks, reconcileAll } = await import('../sync.js');
const ph = await import('../../product-hub/db.js');

console.log('--- URL 正規化');
const c1 = analyzeUrl('https://www.canva.com/design/DAF1abc_XYZ/aBcDeFgHiJ/edit?utm=1');
const c2 = analyzeUrl('https://www.canva.com/design/DAF1abc_XYZ/OtherToken99/view');
check('canva DESIGN_ID 抽出', c1?.provider === 'canva' && c1.external_id === 'DAF1abc_XYZ' && c1.link_type_hint === 'canva');
check('canva TOKEN/edit/view 違いは同一', c1.normalized_url === c2.normalized_url);
check('canva templates は同一化しない', analyzeUrl('https://www.canva.com/templates/abc/')?.provider === null);
const d1 = analyzeUrl('https://drive.google.com/drive/folders/1wMgG-MvVdun7-y89gvGovv8Y7x8ICAd4?usp=sharing');
const d2 = analyzeUrl('https://drive.google.com/drive/u/0/folders/1wMgG-MvVdun7-y89gvGovv8Y7x8ICAd4');
check('drive folder fileId', d1?.external_id === '1wMgG-MvVdun7-y89gvGovv8Y7x8ICAd4' && d1.link_type_hint === 'drive_folder');
check('drive URL 表記違いは同一', d1.normalized_url === d2.normalized_url);
check('drive file → drive_file', analyzeUrl('https://drive.google.com/file/d/1AbC_dEf-123456789012345/view')?.link_type_hint === 'drive_file');
check('gdoc → gdoc', analyzeUrl('https://docs.google.com/document/d/1AbC_dEf-123456789012345/edit')?.link_type_hint === 'gdoc');
check('other 末尾スラッシュ・fragment', analyzeUrl('HTTPS://Example.com/x/#top')?.normalized_url === 'https://example.com/x');
check('normalizeCode 全角空白・大文字', normalizeCode('　ABC-01 ') === 'abc-01');
check('normalizeText NFKC', normalizeText('ＳＰＥＡＲＭＩＮＴ　３０ｍｌ') === 'spearmint 30ml');

console.log('--- DB');
const { initMirrorDB } = await import('../../warehouse-mirror/db.js');
initMirrorDB(); // 本番では server.js が起動時に実行する (smoke では明示)
const db = ph.initProductHubDB();
pl.initProductLinksDB();
pl.initProductLinksDB();
check('init 冪等', !!db.prepare("SELECT 1 FROM sqlite_master WHERE name = 'ph_product_links'").get());
// mirror_products / mirror_set_components を最小で用意 (warehouse-mirror の initMirror が無い環境でも検索を試せるように)
db.exec(`CREATE TABLE IF NOT EXISTS mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, セット構成品数 INTEGER, updated_at TEXT NOT NULL DEFAULT '')`);
db.exec(`CREATE TABLE IF NOT EXISTS mirror_set_components (セット商品コード TEXT NOT NULL, 構成商品コード TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, 構成商品名 TEXT, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY (セット商品コード, 構成商品コード))`);
db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, 'n/a', '')`).run('spearmint30', 'スペアミント オイル 【30ml】', '単品', '取扱中');
db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, 'n/a', '')`).run('mint3set', 'ミント3本セット', 'セット', '取扱中');
db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, 'n/a', '')`).run('nolink01', 'リンク無し商品', '単品', '取扱中');
db.prepare(`INSERT INTO mirror_set_components (セット商品コード, 構成商品コード, 数量, 構成商品名, updated_at) VALUES (?, ?, ?, ?, '')`).run('mint3set', 'spearmint30', 3, 'スペアミント オイル 【30ml】');

const canvaA = 'https://www.canva.com/design/DAFaaaa111/tok1/edit';
const folderA = 'https://drive.google.com/drive/folders/1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
const r1 = upsertLink(db, { neCode: 'SPEARMINT30', linkType: 'canva', url: canvaA, source: 'manual', sourceEntityId: 'tester' });
const r2 = upsertLink(db, { neCode: 'spearmint30', linkType: 'canva', url: canvaA.replace('tok1/edit', 'tok2/view'), source: 'notion_image', sourceEntityId: 'page1' });
check('upsert 冪等 (同一 Canva は同じ行)', r1.created && !r2.created && r1.id === r2.id);
const srcs = db.prepare('SELECT source_system FROM ph_product_link_sources WHERE link_id = ? ORDER BY 1').all(r1.id).map((r) => r.source_system);
check('多由来 (manual + notion_image)', srcs.join(',') === 'manual,notion_image', srcs.join(','));

console.log('--- product-hub 同期');
const ins = db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url) VALUES (?, ?, ?)`).run('spearmint30', 'スペアミント', folderA);
const draftId = Number(ins.lastInsertRowid);
ph.upsertImageProduction(db, draftId, { canva_url: canvaA });
let s = syncDraftLinks(db, draftId);
check('sync: folder + canva', s.ok && s.upserted === 2, JSON.stringify(s));
let links = linksByCodes(db, ['spearmint30']).get('spearmint30');
check('canva 行に product_hub 由来が追加 (行は増えない)', links.filter((l) => l.link_type === 'canva').length === 1 && links.find((l) => l.link_type === 'canva').sources.includes('product_hub'));
check('drive_folder 行', links.some((l) => l.link_type === 'drive_folder' && l.external_id === '1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'));
s = syncDraftLinks(db, draftId);
check('sync 冪等', s.ok && s.detached === 0 && linksByCodes(db, ['spearmint30']).get('spearmint30').length === 2);
// フォルダを変える → 旧フォルダ行は由来が無くなるので論理削除、新フォルダ行が入る
const folderB = 'https://drive.google.com/drive/folders/1BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB';
db.prepare('UPDATE product_drafts SET drive_folder_url = ? WHERE id = ?').run(folderB, draftId);
s = syncDraftLinks(db, draftId);
links = linksByCodes(db, ['spearmint30']).get('spearmint30');
check('フォルダ変更: 旧は削除・新が生きる', s.detached === 1 && links.filter((l) => l.link_type === 'drive_folder').length === 1 && links.find((l) => l.link_type === 'drive_folder').external_id === '1BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB');
check('旧フォルダ行は deleted_at', db.prepare("SELECT deleted_at FROM ph_product_links WHERE external_id = '1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'").get()?.deleted_at != null);
// Canva を空にする → manual/notion 由来が残るので行は生きたまま、product_hub 由来だけ detached
ph.upsertImageProduction(db, draftId, { canva_url: null });
s = syncDraftLinks(db, draftId);
const canvaRow = linksByCodes(db, ['spearmint30']).get('spearmint30').find((l) => l.link_type === 'canva');
check('Canva 空: 他由来があるので行は残る・product_hub 由来だけ外れる', canvaRow && !canvaRow.sources.includes('product_hub') && canvaRow.sources.includes('manual'));
// ドラフト削除 → reconcile で由来外し
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(draftId);
const rc = reconcileAll(db);
check('reconcile: 消えたドラフトの由来を外す', rc.ok && rc.gone === 1 && !db.prepare("SELECT 1 FROM ph_product_links WHERE external_id = '1BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' AND deleted_at IS NULL").get());

console.log('--- primary / meta');
const p1 = upsertLink(db, { neCode: 'spearmint30', linkType: 'canva', purpose: 'top_image', url: 'https://www.canva.com/design/DAFtop00001/x/edit', source: 'manual', sourceEntityId: 't' });
const p2 = upsertLink(db, { neCode: 'spearmint30', linkType: 'canva', purpose: 'top_image', url: 'https://www.canva.com/design/DAFtop00002/x/edit', source: 'manual', sourceEntityId: 't' });
setPrimary(db, p1.id, true);
setPrimary(db, p2.id, true);
const prim = db.prepare("SELECT id FROM ph_product_links WHERE ne_code = 'spearmint30' AND purpose = 'top_image' AND is_primary = 1 AND deleted_at IS NULL").all();
check('primary は用途ごとに 1 件 (後勝ち)', prim.length === 1 && prim[0].id === p2.id);
updateLinkMeta(db, p2.id, { purpose: 'detail_image' });
check('用途変更で primary が外れる', db.prepare('SELECT is_primary, purpose FROM ph_product_links WHERE id = ?').get(p2.id).is_primary === 0);
check('drive_folder は purpose を持たない', db.prepare("SELECT purpose FROM ph_product_links WHERE link_type = 'drive_folder' AND purpose IS NOT NULL").all().length === 0);
let threw = false;
try { upsertLink(db, { neCode: 'x', linkType: 'canva', purpose: 'bogus', url: 'https://a.b/c', source: 'manual' }); } catch (e) { threw = e.code === 'VALIDATION'; }
check('不正 purpose は拒否 (VALIDATION)', threw);
threw = false;
const np = upsertLink(db, { neCode: 'spearmint30', linkType: 'canva', url: 'https://www.canva.com/design/DAFnopurpose/x/edit', source: 'manual', sourceEntityId: 't' });
try { setPrimary(db, np.id, true); } catch (e) { threw = e.code === 'VALIDATION'; }
check('用途なしは primary にできない', threw && db.prepare('SELECT is_primary FROM ph_product_links WHERE id = ?').get(np.id).is_primary === 0);
// 同じ DESIGN_ID を種類違いで入れても 1 行 (同一判定は 商品×正規化URL)
const dup = upsertLink(db, { neCode: 'spearmint30', linkType: 'other', url: 'https://www.canva.com/design/DAFnopurpose/zz/view', source: 'manual', sourceEntityId: 't' });
check('種類違いでも同じ URL は同じ行', !dup.created && dup.id === np.id && db.prepare('SELECT link_type FROM ph_product_links WHERE id = ?').get(np.id).link_type === 'canva');
// 人が削除した行を自動同期が復活させない
const ins2 = db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url) VALUES (?, ?, ?)`).run('nolink01', 'リンク無し商品', folderA);
const draft2 = Number(ins2.lastInsertRowid);
syncDraftLinks(db, draft2);
const autoRow = linksByCodes(db, ['nolink01']).get('nolink01')[0];
softDeleteLink(db, autoRow.id, 'tester');
const s2 = syncDraftLinks(db, draft2);
check('削除済みは自動同期で復活しない (由来だけ記録)', s2.ok && !linksByCodes(db, ['nolink01']) .get('nolink01') && db.prepare("SELECT deleted_at FROM ph_product_links WHERE id = ?").get(autoRow.id).deleted_at != null);
check('削除済み行に product_hub 由来が再付与されている', db.prepare("SELECT detached_at FROM ph_product_link_sources WHERE link_id = ? AND source_system = 'product_hub'").get(autoRow.id)?.detached_at === null);
const re = upsertLink(db, { neCode: 'nolink01', linkType: 'drive_folder', url: folderA, source: 'manual', sourceEntityId: 't' });
check('人が入れ直すと復活 (同じ id)', re.id === autoRow.id && db.prepare('SELECT deleted_at FROM ph_product_links WHERE id = ?').get(re.id).deleted_at === null);
softDeleteLink(db, autoRow.id, 'tester');
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(draft2);
// PATCH は原子的: 不正 purpose なら primary も変わらない
const { patchLink } = pl;
threw = false;
try { patchLink(db, p2.id, { is_primary: true, purpose: 'bogus' }); } catch (e) { threw = e.code === 'VALIDATION'; }
check('patchLink 原子性 (400 なら primary も戻る)', threw && db.prepare('SELECT is_primary FROM ph_product_links WHERE id = ?').get(p2.id).is_primary === 0);
patchLink(db, np.id, { purpose: 'a_plus', is_primary: true });
check('patchLink 用途+primary 同時指定 → 新しい用途で primary', db.prepare('SELECT purpose, is_primary FROM ph_product_links WHERE id = ?').get(np.id).is_primary === 1);
patchLink(db, np.id, { purpose: 'variation', is_primary: true });
const npRow = db.prepare('SELECT purpose, is_primary FROM ph_product_links WHERE id = ?').get(np.id);
check('patchLink 用途変更+primary → 変更後の用途で primary のまま', npRow.purpose === 'variation' && npRow.is_primary === 1);
// 不正 URL は台帳に入らない (DB 層ゲート)
for (const bad of ['javascript:alert(1)', 'http://%', 'https://user:pw@example.com/x', 'ftp://example.com/a']) {
  let rejected = false;
  try { upsertLink(db, { neCode: 'spearmint30', linkType: 'other', url: bad, source: 'manual', sourceEntityId: 't' }); } catch (e) { rejected = e.code === 'VALIDATION'; }
  check(`不正 URL 拒否: ${bad}`, rejected);
}
// 自動同期も不正 URL を写さない
const ins3 = db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url) VALUES (?, ?, ?)`).run('nolink01', 'x', 'javascript:alert(1)');
const s3 = syncDraftLinks(db, Number(ins3.lastInsertRowid));
check('自動同期: 不正 URL は写さない', s3.ok && s3.upserted === 0);
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(Number(ins3.lastInsertRowid));
// strict は例外を投げる (存在しない draft でも tableExists は通るので、不正な source で試す代わりに db を壊さず検証: 用途なし primary を strict 経路で)
threw = false;
try { syncDraftLinks({ prepare: () => { throw new Error('boom'); } }, 1, { strict: true }); } catch { threw = true; }
check('strict=true は例外を投げる', threw);
check('strict 既定は握る', syncDraftLinks({ prepare: () => { throw new Error('boom'); } }, 1).ok === false);
// セット構成の product-hub 補完は複数構成品を全部拾う
db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, 'n/a', '')`).run('phset01', 'ハブ派生セット', 'セット', '取扱中');
const insSet = db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES (?, ?)`).run('phset01', 'ハブ派生セット');
db.prepare('INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, ?, ?)').run(Number(insSet.lastInsertRowid), 'spearmint30', 2, 0);
db.prepare('INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, ?, ?)').run(Number(insSet.lastInsertRowid), 'nolink01', 1, 1);
check('draft_set_members 補完は全構成品', (pl.membersOf(db, ['phset01']).get('phset01') || []).length === 2);
check('mirror にある構成は mirror が正 (補完しない)', (pl.membersOf(db, ['mint3set']).get('mint3set') || []).length === 1);

console.log('--- 検索');
invalidateCatalogCache();
let res = searchProducts(db, { q: 'ｽﾍﾟｱﾐﾝﾄ' });
check('NFKC (半角カナ→全角) で商品名ヒット', res.rows.some((r) => r.code === 'spearmint30'), JSON.stringify(res.rows.map((r) => r.code)));
res = searchProducts(db, { q: 'SPEAR' });
check('コード前方一致 (大文字)', res.rows.some((r) => r.code === 'spearmint30'));
res = searchProducts(db, { q: 'ミント3本' });
const setRow = res.rows.find((r) => r.code === 'mint3set');
check('セット行に構成単品のリンクが参照で付く', setRow?.is_set && setRow.members.length === 1 && setRow.members[0].code === 'spearmint30' && setRow.members[0].links.length >= 1);
check('セット自身のリンクは無い (継承コピーしない)', setRow.links.length === 0);
res = searchProducts(db, { q: '', onlyMissing: true });
check('リンク未登録フィルタ', res.rows.some((r) => r.code === 'nolink01') && !res.rows.some((r) => r.code === 'spearmint30'));
res = searchProducts(db, { q: 'スペアミント オイル' });
check('空白区切り AND', res.rows.some((r) => r.code === 'spearmint30'));
softDeleteLink(db, p1.id, 'tester');
check('論理削除後は検索に出ない', !linksByCodes(db, ['spearmint30']).get('spearmint30').some((l) => l.id === p1.id));

console.log('--- EJS 描画');
const html = await ejs.renderFile(path.join(__dirname, '..', 'views', 'index.ejs'), {
  title: '商品リンク台帳', displayName: 'tester', canEdit: true, isAdmin: true, q: 'ミント', onlyMissing: false, kind: '',
  result: searchProducts(db, { q: 'ミント' }), stats: pl.stats(db),
  linkTypes: pl.LINK_TYPES, linkTypeLabels: pl.LINK_TYPE_LABELS, purposes: pl.PURPOSES, purposeLabels: pl.PURPOSE_LABELS, sourceLabels: pl.SOURCE_LABELS,
});
check('index.ejs 描画', html.includes('商品リンク台帳') && html.includes('mint3set') && html.includes('構成商品から参照'));
const html2 = await ejs.renderFile(path.join(__dirname, '..', 'views', 'index.ejs'), {
  title: '商品リンク台帳', displayName: 'viewer', canEdit: false, isAdmin: false, q: '', onlyMissing: false, kind: '',
  result: searchProducts(db, { q: '' }), stats: pl.stats(db),
  linkTypes: pl.LINK_TYPES, linkTypeLabels: pl.LINK_TYPE_LABELS, purposes: pl.PURPOSES, purposeLabels: pl.PURPOSE_LABELS, sourceLabels: pl.SOURCE_LABELS,
});
check('閲覧のみ描画 (追加ボタン無し)', !html2.includes('class="btn btn-sm add-toggle"') && !html2.includes('class="addbox"'));

console.log(failed === 0 ? '\nALL PASS' : `\n${failed} FAILED`);
process.exitCode = failed === 0 ? 0 : 1;
