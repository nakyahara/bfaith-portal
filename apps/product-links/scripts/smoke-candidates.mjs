/**
 * 商品リンク台帳 PR2 smoke — 候補表 (Drive 走査 / Notion / CSV → accept) を外部 API 非接続で検証。
 * 実行: DATA_DIR=/tmp/pl2-smoke node apps/product-links/scripts/smoke-candidates.mjs
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

const { initMirrorDB } = await import('../../warehouse-mirror/db.js');
initMirrorDB();
const ph = await import('../../product-hub/db.js');
const pl = await import('../db.js');
const cand = await import('../candidates.js');
const src = await import('../import-sources.js');
const { syncDraftLinks } = await import('../sync.js');
const db = ph.initProductHubDB();
pl.initProductLinksDB();

const insP = db.prepare(`INSERT OR IGNORE INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, 'n/a', '')`);
insP.run('spearmint30', 'スペアミント オイル 【30ml】', '単品', '取扱中');
insP.run('spearmint100', 'スペアミント オイル 【100ml】', '単品', '取扱中');
insP.run('okuwa07set', 'オオクワガタペア セット⑦', 'セット', '取扱中');
insP.run('bikerepairs', 'バイクの補修シート 黒', '単品', '取扱中');
insP.run('uniq', 'ユニーク', '単品', '取扱中');
pl.invalidateCatalogCache();

console.log('--- フォルダ名 → コード');
check('code_name', cand.codeFromFolderName('spearmint30_スペアミント オイル 【30ml】') === 'spearmint30');
check('全角空白混入・大文字', cand.codeFromFolderName('　OKUWA07SET_オオクワガタペア セット⑦　') === 'okuwa07set');
check('旧コード形式', cand.codeFromFolderName('0726-001802_合皮補修シート 【グレー】') === '0726-001802');
check('アンダースコア無し', cand.codeFromFolderName('bikerepairs バイク') === 'bikerepairs');
const catalog = pl.loadCatalog(db);
check('exact', cand.inferCode(catalog, 'spearmint30').confidence === 'exact');
check('prefix 一意', cand.inferCode(catalog, 'uni').inferred === 'uniq' && cand.inferCode(catalog, 'uni').confidence === 'prefix');
check('prefix 複数 → none', cand.inferCode(catalog, 'spearmint').confidence === 'none');
check('none', cand.inferCode(catalog, '0726-001802').confidence === 'none');

console.log('--- Drive 走査 (モック)');
// product-hub に spearmint30 のフォルダが既にある → 走査で同じ fileId が来たら duplicate で自動処理
const d = db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url) VALUES (?, ?, ?)`).run('spearmint30', 'x', 'https://drive.google.com/drive/folders/1SPEARMINT30FOLDERIDXXXXXXXXXX');
syncDraftLinks(db, Number(d.lastInsertRowid));
const folders = [
  { folder_id: '1SPEARMINT30FOLDERIDXXXXXXXXXX', name: 'spearmint30_スペアミント オイル 【30ml】' },
  { folder_id: '1OKUWA07SETFOLDERIDXXXXXXXXXXX', name: 'okuwa07set_オオクワガタペア セット⑦　' },
  { folder_id: '1OLDCODEFOLDERIDXXXXXXXXXXXXXX', name: '0726-001802_合皮補修シート 【グレー】' },
  { folder_id: '1UNIQPREFIXFOLDERIDXXXXXXXXXXX', name: 'uni_ユニーク' },
];
let r = await src.scanDriveProductFolders(db, { actor: 'tester', list: async () => folders });
check('走査: 4件 → 新規3 / 重複1', r.folders === 4 && r.inserted === 3 && r.duplicate === 1, JSON.stringify(r));
check('重複は既存行に drive_scan 由来が付く', !!db.prepare("SELECT 1 FROM ph_product_link_sources s JOIN ph_product_links l ON l.id = s.link_id WHERE l.external_id = '1SPEARMINT30FOLDERIDXXXXXXXXXX' AND s.source_system = 'drive_scan'").get());
r = await src.scanDriveProductFolders(db, { actor: 'tester', list: async () => folders });
check('再走査は二重に候補化しない', r.inserted === 0 && r.skipped >= 3, JSON.stringify(r));
let pending = cand.listCandidates(db, { resolution: 'pending' });
check('未処理 3 件・exact が先頭', pending.length === 3 && pending[0].confidence === 'exact' && pending[0].inferred_ne_code === 'okuwa07set');
check('inferred_name が付く', pending[0].inferred_name.includes('オオクワガタ'));

console.log('--- accept / reject');
const old = pending.find((c) => c.raw_code === '0726-001802');
let threw = false;
try { cand.acceptCandidate(db, old.id, { actor: 'tester' }); } catch (e) { threw = e.code === 'VALIDATION'; }
check('コード不明の候補はそのままでは採用できない', threw);
threw = false;
try { cand.acceptCandidate(db, old.id, { neCode: 'nonexist', actor: 'tester' }); } catch (e) { threw = e.code === 'VALIDATION'; }
check('マスタに無いコードは拒否', threw);
const acc = cand.acceptCandidate(db, old.id, { neCode: 'BIKEREPAIRS', actor: 'tester' });
check('人がコードを直して採用 → 台帳に入る', acc.ne_code === 'bikerepairs' && acc.created && pl.linksByCodes(db, ['bikerepairs']).get('bikerepairs')?.[0]?.link_type === 'drive_folder');
check('採用済みの由来 = drive_scan', pl.getLink(db, acc.link_id).sources.includes('drive_scan'));
threw = false;
try { cand.acceptCandidate(db, old.id, { neCode: 'bikerepairs', actor: 'tester' }); } catch (e) { threw = e.code === 'VALIDATION'; }
check('二重採用は拒否', threw);
const pre = cand.listCandidates(db).find((c) => c.raw_code === 'uni');
check('reject', cand.rejectCandidate(db, pre.id, { actor: 'tester' }) && !cand.rejectCandidate(db, pre.id, { actor: 'tester' }));
const exactIds = cand.listCandidates(db).filter((c) => c.confidence === 'exact').map((c) => c.id);
const all = cand.acceptExactByIds(db, { ids: [...exactIds, old.id, 999999], actor: 'tester' });
check('完全一致まとめて採用 (id 列挙・処理済み/不明 id はスキップ)', all.accepted === 1 && all.failed === 0 && all.skipped === 2 && pl.linksByCodes(db, ['okuwa07set']).get('okuwa07set')?.length === 1, JSON.stringify(all));
// 却下済み候補と同じリンクが後から本表に入っても、再走査で由来が足されない (Codex PR2 R1 High)
const rejectedC = cand.listCandidates(db, { resolution: 'rejected' })[0];
pl.upsertLink(db, { neCode: 'uniq', linkType: 'drive_folder', url: rejectedC.raw_url, source: 'manual', sourceEntityId: 't' });
r = await src.scanDriveProductFolders(db, { actor: 'tester', list: async () => folders });
const uniqLink = pl.linksByCodes(db, ['uniq']).get('uniq')[0];
check('再走査: 却下済みは触らない (drive_scan 由来が付かない)', r.inserted === 0 && r.duplicate === 0 && !uniqLink.sources.includes('drive_scan'));
const counts = cand.candidateCounts(db);
check('counts', counts.pending === 0 && counts.accepted === 2 && counts.rejected === 1 && counts.duplicate === 1, JSON.stringify(counts));

console.log('--- Notion 画像DB (モック)');
const page = (id, code, name, canva, drive) => ({ id, properties: {
  '商品コード': { type: 'rich_text', rich_text: [{ plain_text: code }] },
  'Name': { type: 'title', title: [{ plain_text: name }] },
  'Canva': { type: 'url', url: canva }, 'グーグルドライブURL': { type: 'url', url: drive },
} });
const pages = [
  page('p1', 'spearmint100', 'スペアミント100', 'https://www.canva.com/design/DAFsp100/tok/edit', 'https://drive.google.com/drive/folders/1SP100FOLDERIDXXXXXXXXXXXXXXXX'),
  page('p2', 'unknowncode', '不明', 'https://www.canva.com/design/DAFunknown/tok/edit', null),
  page('p3', 'spearmint30', 'x', null, 'https://drive.google.com/drive/folders/1SPEARMINT30FOLDERIDXXXXXXXXXX'),
  page('p4', '', 'コード無し', 'https://www.canva.com/design/DAFnocode/tok/edit', null),
];
r = await src.importNotionImageDb(db, { actor: 'tester', query: async () => ({ pages }), config: () => ({ databaseId: 'x' }) });
check('Notion: canva2+drive1 → 新規3 / 重複1 (product-hub 既存フォルダ)', r.pages === 4 && r.inserted === 3 && r.duplicate === 1, JSON.stringify(r));
pending = cand.listCandidates(db, { resolution: 'pending', source: 'notion_image' });
check('exact 2 (spearmint100 canva+drive)・none 1', pending.filter((c) => c.confidence === 'exact').length === 2 && pending.filter((c) => c.confidence === 'none').length === 1);
r = await src.importNotionImageDb(db, { actor: 'tester', query: async () => ({ pages }), config: () => ({ databaseId: 'x' }) });
check('Notion 再実行は二重化しない', r.inserted === 0);

console.log('--- CSV');
const items = cand.parseCsvItems('ne_code,url,link_type,purpose,label\nspearmint30,"https://www.canva.com/design/DAFcsv1/t/edit",canva,a_plus,"A+ 1枚目"\nbad,\nnonexist,https://example.com/x,,,\n');
check('parse 2 行 (url 空は除外)', items.length === 2 && items[0].purpose === 'a_plus' && items[0].label === 'A+ 1枚目');
const csv = cand.addCandidates(db, { batchId: cand.newBatchId('csv'), source: 'csv', items, actor: 'tester' });
check('CSV 候補化', csv.inserted === 2, JSON.stringify(csv));
const csvExact = cand.listCandidates(db, { source: 'csv' }).find((c) => c.raw_code === 'spearmint30');
cand.acceptCandidate(db, csvExact.id, { actor: 'tester' });
const csvLink = pl.linksByCodes(db, ['spearmint30']).get('spearmint30').find((l) => l.external_id === 'DAFcsv1');
check('CSV 採用 → purpose/label 引き継ぎ', csvLink && csvLink.purpose === 'a_plus' && csvLink.label === 'A+ 1枚目');
check('タブ区切り', cand.parseCsvItems('商品コード\turl\nabc\thttps://a.b/c').length === 1);
threw = false;
try { cand.parseCsvItems('foo,bar\n1,2'); } catch (e) { threw = e.code === 'VALIDATION'; }
check('ヘッダ不正は VALIDATION', threw);
check('不正 URL はスキップ', cand.addCandidates(db, { batchId: 'b', source: 'csv', items: [{ raw_code: 'spearmint30', url: 'javascript:alert(1)' }] }).skipped === 1);
// external_id が取れない URL も (provider='url', 正規化URL) で二重化しない
const o1 = cand.addCandidates(db, { batchId: 'b', source: 'csv', items: [{ raw_code: 'uniq', url: 'https://example.com/page/' }] });
const o2 = cand.addCandidates(db, { batchId: 'b', source: 'csv', items: [{ raw_code: 'uniq', url: 'HTTPS://EXAMPLE.com/page#x' }] });
check('種別不明 URL の重複判定 (正規化)', o1.inserted === 1 && o2.inserted === 0 && o2.skipped === 1, JSON.stringify([o1, o2]));

console.log('--- EJS');
const locals = {
  title: 't', displayName: 'admin', candidates: cand.listCandidates(db, { resolution: 'all' }), counts: cand.candidateCounts(db),
  resolution: 'all', source: null, driveFolderId: src.driveFolderId(),
  purposes: pl.PURPOSES, purposeLabels: pl.PURPOSE_LABELS, linkTypeLabels: pl.LINK_TYPE_LABELS, sourceLabels: pl.SOURCE_LABELS,
};
const html = await ejs.renderFile(path.join(__dirname, '..', 'views', 'admin.ejs'), locals);
check('admin.ejs 描画', html.includes('取込 (admin)') && html.includes('okuwa07set'));
check('script 開始/終了タグ数一致', (html.match(/<script>/g) || []).length === (html.match(/<\/script>/g) || []).length);
const html2 = await ejs.renderFile(path.join(__dirname, '..', 'views', 'index.ejs'), {
  title: 't', displayName: 'd', canEdit: true, isAdmin: true, q: '', onlyMissing: false, kind: '',
  result: pl.searchProducts(db, { q: '' }), stats: pl.stats(db),
  linkTypes: pl.LINK_TYPES, linkTypeLabels: pl.LINK_TYPE_LABELS, purposes: pl.PURPOSES, purposeLabels: pl.PURPOSE_LABELS, sourceLabels: pl.SOURCE_LABELS,
});
check('index.ejs に取込リンク (admin)', html2.includes('/apps/product-links/admin') && (html2.match(/<script>/g) || []).length === (html2.match(/<\/script>/g) || []).length);

console.log(failed === 0 ? '\nALL PASS' : `\n${failed} FAILED`);
process.exitCode = failed === 0 ? 0 : 1;
