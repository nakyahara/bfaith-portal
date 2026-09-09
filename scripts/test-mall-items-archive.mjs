/**
 * test-mall-items-archive.mjs — scripts/mall-items/archive-items.mjs のローカルテスト
 * 使い方: node scripts/test-mall-items-archive.mjs (repo ルートで。一時ディレクトリで完結、rclone は呼ばない)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import {
  archiveItems, buildText, contentHashOf, listItemSnapshots, resolveOffsiteRemote, rcloneArgs, offsiteSync,
} from './mall-items/archive-items.mjs';

let failures = 0;
function check(name, cond, extra = '') { if (cond) console.log(`PASS: ${name}`); else { failures++; console.log(`FAIL: ${name} ${extra}`); } }
const T = fs.mkdtempSync(path.join(os.tmpdir(), 'mall-items-test-'));
const dest = path.join(T, 'history');
const quiet = () => {};
const manifest = (mall) => {
  const mf = path.join(dest, mall, 'manifest.jsonl');
  return fs.existsSync(mf) ? fs.readFileSync(mf, 'utf-8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l)) : [];
};
const noTmp = (mall) => {
  const root = path.join(dest, mall);
  if (!fs.existsSync(root)) return true;
  if (fs.readdirSync(root).some((f) => f.startsWith('.work-'))) return false;
  return listItemSnapshots(dest, mall).every((s) => !fs.readdirSync(path.dirname(s.file)).some((f) => f.endsWith('.tmp')));
};
const base = { shopId: 'S1@A1VC38T7YXB528', source: 'merchant_listings_all_data', dest, noOffsite: true, log: quiet, now: new Date('2026-09-09T14:35:10Z') };
const tsv = 'seller-sku\tasin1\tprice\tstatus\r\nsku-b\tB000B\t1200\tActive\r\nsku-a\tB000A\t980\tActive\r\n';

// ── T1: TSV を保存 (原文のまま、ファイル名は取得時刻 JST + run_id) ──
const r1 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_001', fetchedAt: '2026-09-09T14:31:05Z', format: 'tsv', payload: tsv, meta: { complete: true, enum_status: 'ok', api_version: 'reports/2021-06-30' } });
check('T1 archived', r1.action === 'archived' && r1.code === 'archived' && r1.offsite === 'skipped', JSON.stringify(r1));
check('T1 ファイル名 = amazon/2026/09/items_amazon_20260909_233105_r_001.tsv.gz', r1.relFile === 'amazon/2026/09/items_amazon_20260909_233105_r_001.tsv.gz', r1.relFile);
check('T1 gzip を解くと原文と同じ (CRLF も保持)', zlib.gunzipSync(fs.readFileSync(r1.file)).toString('utf-8') === tsv);
check('T1 items = rows = 2 (ヘッダ除く)', r1.items === 2 && r1.rows === 2, `${r1.items}/${r1.rows}`);
const m1 = manifest('amazon');
check('T1 manifest に 1 行 (complete / enum_status / api_version / sha256 / content_hash / snapshot_at +09:00)',
  m1.length === 1 && m1[0].complete === true && m1[0].enum_status === 'ok' && m1[0].api_version === 'reports/2021-06-30'
  && /^[0-9a-f]{64}$/.test(m1[0].sha256) && /^[0-9a-f]{64}$/.test(m1[0].content_hash)
  && m1[0].snapshot_at === '2026-09-09T23:31:05+09:00' && m1[0].same_as_previous === false && m1[0].run_id === 'r_001', JSON.stringify(m1));
check('T1 tmp / work が残らない', noTmp('amazon'));

// ── T2: 同じ内容を別の順序で取っても content_hash は同じ → same_as_previous=true。ファイルは別 (毎晩保存) ──
const tsvReordered = 'seller-sku\tasin1\tprice\tstatus\nsku-a\tB000A\t980\tActive\nsku-b\tB000B\t1200\tActive\n';
const r2 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_002', fetchedAt: '2026-09-10T14:31:05Z', format: 'tsv', payload: tsvReordered, meta: { complete: true } });
check('T2 変化が無くても保存する (別ファイル)', r2.code === 'archived' && r2.relFile !== r1.relFile, r2.relFile);
check('T2 行順・改行コードが違っても content_hash は同じ = same_as_previous', r2.contentHash === r1.contentHash && r2.sameAsPrevious === true, `${r2.contentHash} vs ${r1.contentHash}`);
check('T2 sha256 は違う (原文は別)', r2.sha256 !== r1.sha256);
check('T2 manifest 2 行目 same_as_previous=true', manifest('amazon')[1].same_as_previous === true);

// ── T3: 内容が変わったら same_as_previous=false ──
const r3 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_003', fetchedAt: '2026-09-11T14:31:05Z', format: 'tsv', payload: tsv.replace('980', '990'), meta: { complete: true } });
check('T3 価格が変わったら same_as_previous=false', r3.sameAsPrevious === false);

// ── T4: 0 件は保存しない (取れなかったを無くなったと読み替えない) ──
const r4 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_004', fetchedAt: '2026-09-12T14:31:05Z', format: 'tsv', payload: 'seller-sku\tasin1\n' });
check('T4 ヘッダだけの TSV は skipped code=empty', r4.action === 'skipped' && r4.code === 'empty', JSON.stringify(r4));
const r4b = await archiveItems({ ...base, mall: 'rakuten', shopId: '1', source: 'rms_items_search', runId: 'r_004b', fetchedAt: '2026-09-12T14:31:05Z', format: 'ndjson', payload: [] });
check('T4 空配列も skipped code=empty', r4b.action === 'skipped' && r4b.code === 'empty');
check('T4 manifest は増えない', manifest('amazon').length === 3 && manifest('rakuten').length === 0);

// ── T5: 楽天 NDJSON (manageNumber 順に並べる・部分取得は complete=false で記録) ──
const items = [
  { item: { manageNumber: 'zzz-002', variants: { 'zzz-002': { standardPrice: '1080' } } } },
  { item: { manageNumber: 'aaa-001', variants: { 'aaa-001': { standardPrice: '500' } } } },
];
const r5 = await archiveItems({ ...base, mall: 'rakuten', shopId: '1', source: 'rms_items_search', runId: 'r_005', fetchedAt: '2026-09-09T14:40:00Z', format: 'ndjson', payload: items, sortKey: (r) => r?.item?.manageNumber, meta: { complete: false, enum_status: 'partial', pages: 100, truncated: true, deadline_hit: false } });
check('T5 archived (部分取得も証拠として保存)', r5.action === 'archived' && r5.complete === false, JSON.stringify(r5));
const lines5 = zlib.gunzipSync(fs.readFileSync(r5.file)).toString('utf-8').trim().split('\n');
check('T5 1 要素 1 行、manageNumber 順', lines5.length === 2 && JSON.parse(lines5[0]).item.manageNumber === 'aaa-001' && JSON.parse(lines5[1]).item.manageNumber === 'zzz-002');
const m5 = manifest('rakuten')[0];
check('T5 manifest に complete=false / pages / truncated / enum_status', m5.complete === false && m5.pages === 100 && m5.truncated === true && m5.enum_status === 'partial', JSON.stringify(m5));

// ── T6: 同名 gz が既にある (クラッシュ後の再実行) → 同内容は exists_same、別内容は COLLISION ──
const r6a = await archiveItems({ ...base, mall: 'rakuten', shopId: '1', source: 'rms_items_search', runId: 'r_005', fetchedAt: '2026-09-09T14:40:00Z', format: 'ndjson', payload: [...items].reverse(), sortKey: (r) => r?.item?.manageNumber });
check('T6a 同名同内容は exists_same (manifest は増えない)', r6a.code === 'exists_same' && manifest('rakuten').length === 1, JSON.stringify(r6a));
check('T6a 戻り値は manifest の記録を正とする (今回の引数で complete=true に戻らない)', r6a.complete === false && r6a.sameAsPrevious === false && r6a.items === 2 && r6a.sha256 === manifest('rakuten')[0].sha256, JSON.stringify(r6a));
let threw = null;
try {
  await archiveItems({ ...base, mall: 'rakuten', shopId: '1', source: 'rms_items_search', runId: 'r_005', fetchedAt: '2026-09-09T14:40:00Z', format: 'ndjson', payload: [items[0]] });
} catch (e) { threw = e; }
check('T6b 同名別内容は COLLISION で throw (上書きしない)', threw && threw.code === 'COLLISION', threw && threw.message);
check('T6 衝突後も work / tmp が残らない', noTmp('rakuten'));

// ── T7: 引数不正 ──
for (const [name, bad] of [
  ['知らない mall', { mall: 'mercari' }],
  ['知らない format', { format: 'csv' }],
  ['shopId 無し', { shopId: '' }],
  ['fetchedAt が読めない', { fetchedAt: 'yesterday' }],
  ['tsv に配列', { payload: [] }],
]) {
  let e = null;
  try { await archiveItems({ ...base, mall: 'amazon', runId: 'r_007', fetchedAt: '2026-09-09T14:31:05Z', format: 'tsv', payload: tsv, ...bad }); } catch (x) { e = x; }
  check(`T7 ${name} は BAD_ARGS`, e && e.code === 'BAD_ARGS', e && e.message);
}

// ── T8: dry-run は書かない ──
const before = listItemSnapshots(dest, 'amazon').length;
const r8 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_008', fetchedAt: '2026-09-13T14:31:05Z', format: 'tsv', payload: tsv, dryRun: true });
check('T8 dry-run はファイルも manifest も作らない', r8.dryRun === true && listItemSnapshots(dest, 'amazon').length === before && manifest('amazon').length === 3);

// ── T9: manifest の末尾が壊れていても追記できる (書き込み途中のクラッシュ) ──
{
  const mf = path.join(dest, 'amazon', 'manifest.jsonl');
  fs.appendFileSync(mf, '{"archived_at":"2026-09-1');
  const r9 = await archiveItems({ ...base, mall: 'amazon', runId: 'r_009', fetchedAt: '2026-09-14T14:31:05Z', format: 'tsv', payload: tsv });
  const recs = manifest('amazon');
  check('T9 壊れた末尾を切り詰めて追記 (4 行すべて JSON として読める)', r9.code === 'archived' && recs.length === 4 && recs[3].run_id === 'r_009', String(recs.length));
}

// ── T10: 純関数 ──
check('T10 buildText(tsv) は末尾改行を足すだけ', buildText('tsv', 'a\tb\n1\t2').text === 'a\tb\n1\t2\n');
check('T10 contentHashOf は CRLF/LF と行順の違いを吸収', contentHashOf('tsv', 'h\r\nb\r\na\r\n') === contentHashOf('tsv', 'h\na\nb\n'));
check('T10 contentHashOf はヘッダを固定 (ヘッダとデータの入れ替えは別内容)', contentHashOf('tsv', 'h\na\n') !== contentHashOf('tsv', 'a\nh\n'));
check('T10 listItemSnapshots は日付順', listItemSnapshots(dest, 'amazon').map((s) => s.runId).join(',') === 'r_001,r_002,r_003,r_009');
check('T10 resolveOffsiteRemote: 明示 > BACKUP の最終要素置換 > null',
  resolveOffsiteRemote({ MALL_ITEMS_RCLONE_REMOTE: 'x:y/z' }) === 'x:y/z'
  && resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup/warehouse/' }) === 'gdrive:bfaith-backup/mall-items-history'
  && resolveOffsiteRemote({ BACKUP_RCLONE_REMOTE: 'gdrive:bfaith-backup' }) === null
  && resolveOffsiteRemote({}) === null);

// ── T11: offsite は分離して呼ぶ (夜間処理が公開のあとに残り時間で)。rclone は呼ばない ──
{
  const args = rcloneArgs('D:/data/mall-items-history', 'gdrive:bfaith-backup/mall-items-history', { rcloneConfig: 'C:/x/rclone.conf', timeoutMs: 90_000 });
  check('T11 rcloneArgs: --config / copy / include 2 種 / --max-duration は持ち時間の 10 秒手前',
    args[0] === '--config' && args[1] === 'C:/x/rclone.conf' && args[2] === 'copy'
    && args.includes('/*/*/*/items_*.gz') && args.includes('/*/manifest.jsonl')
    && args[args.indexOf('--max-duration') + 1] === '80s', args.join(' '));
  check('T11 rcloneArgs: 持ち時間が短くても --max-duration は 10s を下回らない', rcloneArgs('d', 'r', { timeoutMs: 5_000 }).includes('10s'));
  const s1 = offsiteSync({ dest, env: {}, log: quiet });
  check('T11 offsiteSync: remote 未設定は skipped (rclone を呼ばない)', s1.status === 'skipped' && /remote/.test(s1.reason), JSON.stringify(s1));
  const s2 = offsiteSync({ dest: path.join(T, 'nope'), env: { MALL_ITEMS_RCLONE_REMOTE: 'x:y/z' }, log: quiet });
  check('T11 offsiteSync: 履歴フォルダが無ければ skipped', s2.status === 'skipped' && /無い/.test(s2.reason), JSON.stringify(s2));
}

fs.rmSync(T, { recursive: true, force: true });
console.log(failures ? `\n${failures} FAILED` : '\nALL PASS');
process.exit(failures ? 1 : 0);
